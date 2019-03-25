import asyncio
import time

import ssl
import http
import aiohttp
from typing import Any, Dict, List, Optional, Tuple

from . import rpc
from .server import USER_NAME


exc_list = [BaseException, SystemExit, KeyboardInterrupt, GeneratorExit, Exception, StopIteration,
            BufferError, ArithmeticError, FloatingPointError, OverflowError, ZeroDivisionError, AssertionError,
            AttributeError, EnvironmentError, IOError, OSError, EOFError, ImportError, LookupError, IndexError,
            KeyError, MemoryError, NameError, UnboundLocalError, ReferenceError, RuntimeError, NotImplementedError,
            SystemError, TypeError, ValueError, UnicodeError, UnicodeDecodeError, UnicodeEncodeError,
            UnicodeTranslateError]


exc_map = {exc.__name__: exc for exc in exc_list}


async def process_rpc_results(resp: aiohttp.ClientResponse, allow_streamed: bool = False):
    assert resp.status == http.HTTPStatus.OK, str(resp.status)
    name, data, kwargs = await rpc.deserialize(resp.content, allow_streamed=allow_streamed)
    assert kwargs == {}
    assert len(data) == 1
    if name == rpc.CALL_FAILED:
        exc_cls_name, message, tb = data[0]
        exc = exc_map.get(exc_cls_name, Exception)(message)
        raise exc from Exception("RPC server traceback:\n" + tb)
    else:
        return data[0]


class AsyncRPCClient(object):
    def __init__(self, url: str, ssl_cert_file: Optional[str], access_key: str, user: str = USER_NAME) -> None:
        self.rpc_url = url + '/rpc'
        self.ping_url = url + '/ping'
        self.access_key = access_key
        self.user = user
        self.request_in_progress = False
        self.http_conn = aiohttp.ClientSession()
        self.connected = False

        if ssl_cert_file:
            self.ssl = ssl.create_default_context(cafile=ssl_cert_file)
        else:
            self.ssl = None

        self.auth = aiohttp.BasicAuth(login=user, password=self.access_key)
        self.post_params = {"url": self.rpc_url, "ssl": self.ssl, "auth": self.auth}

    @property
    def streamed(self) -> 'StreamedProxy':
        return StreamedProxy(self)

    async def wait_ready(self, timeout: float = 30, period: float = 0.1):
        etime = time.time() - timeout
        wtime = timeout
        while wtime > 0:
            try:
                await asyncio.wait_for(self.__ping__(), wtime)
                break
            except aiohttp.ClientConnectionError:
                await asyncio.sleep(period)
            wtime = etime - time.time()

    async def __ping__(self):
        async with self.http_conn.get(self.ping_url, ssl=self.ssl) as resp:
            pass

    async def __aenter__(self) -> 'AsyncRPCClient':
        await self.http_conn.__aenter__()
        self.connected = True
        return self

    async def __aexit__(self, x, y, z):
        assert self.connected
        await self.http_conn.__aexit__(x, y, z)
        self.connected = False

    def __prepare_call__(self, name, args, kwargs) -> Tuple[float, Any]:
        assert self.connected
        assert not self.request_in_progress, "Can't share connection between requests"
        timeout = kwargs.pop("_call_timeout", None)
        return timeout, rpc.serialize(name, args, kwargs)

    async def __call__(self, name: str, args: List, kwargs: Dict[str, Any]):
        _, data = self.__prepare_call__(name, args, kwargs)
        async with self.http_conn.post(**self.post_params, data=data) as resp:
            return await process_rpc_results(resp)

    def __call_streamed__(self, name: str, args: List, kwargs: Dict[str, Any]):
        _, data = self.__prepare_call__(name, args, kwargs)
        return StreamedCall(self.http_conn, self.post_params, data)

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self, name)


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


class RPCFuncProxy:
    def __init__(self, rpc: AsyncRPCClient, mod_name: str, func_name: str, streamed: bool = False) -> None:
        self.rpc = rpc
        self.name = mod_name + "::" + func_name
        self.streamed = streamed

    def __call__(self, *args, **kwargs):
        if self.streamed:
            return self.rpc.__call_streamed__(self.name, list(args), kwargs)  # async withable
        else:
            return self.rpc(self.name, list(args), kwargs)  # awaitable


class RPCModuleProxy:
    def __init__(self, rpc: AsyncRPCClient, name: str, streamed: bool = False) -> None:
        self.rpc = rpc
        self.name = name
        self.streamed = streamed

    def __getattr__(self, name) -> 'RPCFuncProxy':
        return RPCFuncProxy(self.rpc, self.name, name, self.streamed)

