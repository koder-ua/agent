import ssl
import zlib
import http
import time
import logging
import asyncio
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp

from . import rpc
from .common import USER_NAME
from .rpc import BlockType


logger = logging.getLogger("rpc")


exc_list = [BaseException, SystemExit, KeyboardInterrupt, GeneratorExit, Exception, StopIteration,
            BufferError, ArithmeticError, FloatingPointError, OverflowError, ZeroDivisionError, AssertionError,
            AttributeError, EnvironmentError, IOError, OSError, EOFError, ImportError, LookupError, IndexError,
            KeyError, MemoryError, NameError, UnboundLocalError, ReferenceError, RuntimeError, NotImplementedError,
            SystemError, TypeError, ValueError, UnicodeError, UnicodeDecodeError, UnicodeEncodeError,
            UnicodeTranslateError]


exc_map = {exc.__name__: exc for exc in exc_list}


async def process_rpc_results(resp: aiohttp.ClientResponse, allow_streamed: bool = False):
    if resp.status != http.HTTPStatus.OK:
        raise RPCServerFailure(f"Server failed with code {resp.status}")

    name, data, kwargs = await rpc.deserialize(resp.content, allow_streamed=allow_streamed)
    assert kwargs == {}
    assert len(data) == 1
    if name == rpc.CALL_FAILED:
        exc_cls_name, message, tb = data[0]
        exc = exc_map.get(exc_cls_name, Exception)(message)
        raise exc from Exception("RPC server traceback:\n" + tb)
    else:
        assert name == rpc.CALL_SUCCEEDED
        return data[0]


class RPCServerFailure(Exception): pass


class AsyncRPCClient:
    def __init__(self, url: str, ssl_cert_file: Optional[Union[str, Path]], access_key: str,
                 user: str = USER_NAME, max_retry: int = 3, retry_timeout: int = 5) -> None:
        self._rpc_url = url + '/rpc'
        self._ping_url = url + '/ping'
        self._access_key = access_key
        self._user = user
        self._request_in_progress = False
        self._http_conn = aiohttp.ClientSession()
        self._connected = False
        self._max_retry = max_retry
        self._retry_timeout = retry_timeout

        if ssl_cert_file:
            self._ssl = ssl.create_default_context(cadata=open(ssl_cert_file).read())
        else:
            self._ssl = None

        self._auth = aiohttp.BasicAuth(login=user, password=self._access_key)
        self._post_params = {"url": self._rpc_url, "ssl": self._ssl, "auth": self._auth,
                             "verify_ssl": self._ssl is not None}

    @property
    def streamed(self) -> 'StreamedProxy':
        return StreamedProxy(self)

    async def wait_ready(self, timeout: float = 30, period: float = 0.1):
        etime = time.time() + timeout
        wtime = timeout

        async def ping():
            async with self._http_conn.get(self._ping_url, ssl=self._ssl, verify_ssl=self._ssl is not None) as resp:
                pass

        while wtime > 0:
            try:
                await asyncio.wait_for(ping(), wtime)
                break
            except aiohttp.ClientConnectionError:
                await asyncio.sleep(period)
            wtime = etime - time.time()
        else:
            raise aiohttp.ClientConnectionError("Can't connect to {}".format(self._rpc_url))

    async def __aenter__(self) -> 'AsyncRPCClient':
        await self._http_conn.__aenter__()
        self._connected = True
        return self

    async def __aexit__(self, x, y, z):
        assert self._connected
        await self._http_conn.__aexit__(x, y, z)
        self._connected = False

    def _prepare_call(self, name, args, kwargs) -> Tuple[float, Any]:
        assert self._connected
        assert not self._request_in_progress, "Can't share connection between requests"
        timeout = kwargs.pop("_call_timeout", None)
        return timeout, rpc.serialize(name, args, kwargs)

    async def __call__(self, name: str, args: List, kwargs: Dict[str, Any]):
        _, data = self._prepare_call(name, args, kwargs)
        async with self._http_conn.post(**self._post_params, data=data) as resp:
            return await process_rpc_results(resp)

    def _call_streamed(self, name: str, args: List, kwargs: Dict[str, Any]):
        _, data = self._prepare_call(name, args, kwargs)
        return StreamedCall(self._http_conn, self._post_params, data)

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self, name, streamed=False)

    async def run(self, cmd: Union[List[str], str], timeout: int = 60, input_data: bytes = None,
                  log: bool = True, merge_err: bool = False,
                  *, node_name: str, compress: bool = True) -> Tuple[bytes, bytes]:
        if log:
            logger.debug("%s: %s", node_name, cmd)

        code, out, err = self.cli.run_cmd(cmd, timeout=timeout, input_data=input_data, merge_err=merge_err)

        if merge_err:
            assert err is None

        if compress:
            out = zlib.decompress(out)
            err = zlib.decompress(err)

        if code == 0:
            return out, err
        else:
            raise subprocess.CalledProcessError(code, cmd, output=out + err)


class StreamedProxy:
    def __init__(self, rpc: AsyncRPCClient) -> None:
        self.rpc = rpc

    def __getattr__(self, name):
        return RPCModuleProxy(self.rpc, name, streamed=True)


class StreamedCall:
    def __init__(self, http_conn: aiohttp.ClientSession, params: Dict[str, Any], data: Any) -> None:
        self.http_conn = http_conn
        self.params = params
        self.data = data
        self.req = http_conn.post(**self.params, data=data)

    async def __aenter__(self):
        resp = await self.req.__aenter__()
        return await process_rpc_results(resp, allow_streamed=True)

    async def __aexit__(self, x, y, z):
        return await self.req.__aexit__(x, y, z)


class RPCModuleProxy:
    def __init__(self, rpc: AsyncRPCClient, mod_name: str, streamed: bool) -> None:
        self.rpc = rpc
        self.mod_name = mod_name
        self.streamed = streamed

    def __getattr__(self, name) -> 'RPCFuncProxy':
        return RPCFuncProxy(self.rpc, mod_name=self.mod_name, func_name=name, streamed=self.streamed)


class RPCFuncProxy:

    def __init__(self, rpc: AsyncRPCClient, mod_name: str, func_name: str, streamed: bool):
        self.rpc = rpc
        self.mod_name = mod_name
        self.streamed = streamed
        self.name = f"{mod_name}::{func_name}"

    def __call__(self, *args, **kwargs):
        if self.streamed:
            return self.rpc._call_streamed(self.name, list(args), kwargs)
        else:
            return self.rpc(self.name, list(args), kwargs)  # awaitable


class ConnectionClosed(Exception):
    pass
