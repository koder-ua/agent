import ssl
import traceback
import zlib
import http
import time
import logging
import asyncio
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, AsyncIterator, BinaryIO

import aiohttp

from . import rpc
from .common import USER_NAME, DEFAULT_PORT
from .utils import CMDResult, CmdType
from .rpc import BlockType


logger = logging.getLogger("rpc")


exc_list = [BaseException, SystemExit, KeyboardInterrupt, GeneratorExit, Exception, StopIteration,
            BufferError, ArithmeticError, FloatingPointError, OverflowError, ZeroDivisionError, AssertionError,
            AttributeError, EnvironmentError, IOError, OSError, EOFError, ImportError, LookupError, IndexError,
            KeyError, MemoryError, NameError, UnboundLocalError, ReferenceError, RuntimeError, NotImplementedError,
            SystemError, TypeError, ValueError, UnicodeError, UnicodeDecodeError, UnicodeEncodeError,
            UnicodeTranslateError, subprocess.CalledProcessError]


exc_map = {exc.__name__: exc for exc in exc_list}


async def process_rpc_results(resp: aiohttp.ClientResponse, allow_streamed: bool = False) -> Any:
    if resp.status != http.HTTPStatus.OK:
        raise RPCServerFailure(f"Server failed with code {resp.status}")

    name, data, kwargs = await rpc.deserialize(resp.content, allow_streamed=allow_streamed)
    assert kwargs == {}
    assert len(data) == 1
    if name == rpc.CALL_FAILED:
        exc_cls_name, message, tb = data[0]
        if exc_cls_name == 'TypeError':
            traceback.print_stack()
            print(tb)
        print(exc_cls_name, message)
        exc = exc_map.get(exc_cls_name, Exception)(message)
        raise exc from Exception("RPC server traceback:\n" + tb)
    else:
        assert name == rpc.CALL_SUCCEEDED
        return data[0]


class RPCServerFailure(Exception): pass


class AsyncRPCClient:
    def __init__(self, hostname_or_url: str,
                 ssl_cert_file: Optional[Union[str, Path]],
                 access_key: str,
                 user: str = USER_NAME,
                 max_retry: int = 3,
                 retry_timeout: int = 5,
                 port: int = DEFAULT_PORT) -> None:

        if hostname_or_url.startswith("http://") or hostname_or_url.startswith("https://"):
            self._rpc_url = hostname_or_url
            if hostname_or_url.endswith('/'):
                self._ping_url = hostname_or_url.rsplit("/", 2)[0] + '/ping'
            else:
                self._ping_url = hostname_or_url.rsplit("/", 1)[0] + '/ping'
        else:
            self._rpc_url = f"https://{hostname_or_url}:{port}/rpc"
            self._ping_url = f"https://{hostname_or_url}:{port}/ping"

        self._access_key = access_key
        self._user = user
        self._request_in_progress = False
        self._http_conn = aiohttp.ClientSession()
        self._connected = False
        self._max_retry = max_retry
        self._retry_timeout = retry_timeout

        if ssl_cert_file:
            self._ssl: Optional[ssl.SSLContext] = ssl.create_default_context(cadata=open(ssl_cert_file).read())
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
            raise aiohttp.ClientConnectionError(f"Can't connect to {self._rpc_url}")

    async def __aenter__(self) -> 'AsyncRPCClient':
        await self._http_conn.__aenter__()
        self._connected = True
        return self

    async def __aexit__(self, x, y, z) -> None:
        assert self._connected
        await self._http_conn.__aexit__(x, y, z)
        self._connected = False

    def _prepare_call(self, name, args, kwargs) -> Tuple[float, Any]:
        assert self._connected
        assert not self._request_in_progress, "Can't share connection between requests"
        timeout = kwargs.pop("_call_timeout", None)
        return timeout, rpc.serialize(name, args, kwargs)

    async def __call__(self, name: str, args: List, kwargs: Dict[str, Any]) -> Any:
        _, data = self._prepare_call(name, args, kwargs)
        async with self._http_conn.post(**self._post_params, data=data) as resp:
            return await process_rpc_results(resp)

    def _call_streamed(self, name: str, args: List, kwargs: Dict[str, Any]) -> 'StreamedCall':
        _, data = self._prepare_call(name, args, kwargs)
        return StreamedCall(self._http_conn, self._post_params, data)

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self, name, streamed=False)

    async def save_file(self, path: Union[str, Path], fd: BinaryIO, compress: bool = True):
        async for chunk in self.iter_file(path, compress=compress):
            fd.write(chunk)

    async def iter_file(self, path: Union[str, Path], compress: bool = True) -> AsyncIterator[bytes]:
        async with self.streamed.fs.get_file(path, compress=compress) as stream:
            async for tp, chunk in stream:
                assert tp == BlockType.binary
                yield chunk

    async def get_file(self, path: Union[str, Path], compress: bool = True) -> bytes:
        return b"".join([chunk async for chunk in self.iter_file(path, compress=compress)])

    async def run(self, cmd: CmdType, timeout: int = 60, input_data: bytes = None,
                  log: bool = True, merge_err: bool = False,
                  *, node_name: str, compress: bool = True) -> CMDResult:
        if log:
            logger.debug("%s: %s", node_name, cmd)

        code, out, err = await self.cli.run_cmd(cmd if isinstance(cmd, str) else [str(i) for i in cmd],
                                                timeout=timeout, input_data=input_data, merge_err=merge_err)

        if merge_err:
            assert err is None

        if compress:
            out = zlib.decompress(out)
            err = None if err is None else zlib.decompress(err)

        return CMDResult(cmd, out, err, code)


class StreamedProxy:
    def __init__(self, rpc: AsyncRPCClient) -> None:
        self.rpc = rpc

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self.rpc, name, streamed=True)


class StreamedCall:
    def __init__(self, http_conn: aiohttp.ClientSession, params: Dict[str, Any], data: Any) -> None:
        self.http_conn = http_conn
        self.params = params
        self.data = data
        self.req = http_conn.post(**self.params, data=data)

    async def __aenter__(self) -> 'StreamedCall':
        resp = await self.req.__aenter__()
        return await process_rpc_results(resp, allow_streamed=True)

    async def __aexit__(self, x, y, z) -> None:
        return await self.req.__aexit__(x, y, z)


class RPCModuleProxy:
    def __init__(self, rpc: AsyncRPCClient, mod_name: str, streamed: bool) -> None:
        self.rpc = rpc
        self.mod_name = mod_name
        self.streamed = streamed

    def __getattr__(self, name) -> 'RPCFuncProxy':
        return RPCFuncProxy(self.rpc, mod_name=self.mod_name, func_name=name, streamed=self.streamed)


class RPCFuncProxy:

    def __init__(self, rpc: AsyncRPCClient, mod_name: str, func_name: str, streamed: bool) -> None:
        self.rpc = rpc
        self.mod_name = mod_name
        self.streamed = streamed
        self.name = f"{mod_name}::{func_name}"

    def __call__(self, *args, **kwargs) -> Any:
        if self.streamed:
            return self.rpc._call_streamed(self.name, list(args), kwargs)
        else:
            return self.rpc(self.name, list(args), kwargs)  # awaitable


class ConnectionClosed(Exception):
    pass
