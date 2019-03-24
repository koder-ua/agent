from typing import Any, Dict, List

import aiohttp
from . import rpc
from .server import USER_NAME


exc_list = [BaseException, SystemExit, KeyboardInterrupt, GeneratorExit, Exception, StopIteration,
            BufferError, ArithmeticError, FloatingPointError, OverflowError, ZeroDivisionError, AssertionError,
            AttributeError, EnvironmentError, IOError, OSError, EOFError, ImportError, LookupError, IndexError,
            KeyError, MemoryError, NameError, UnboundLocalError, ReferenceError, RuntimeError, NotImplementedError,
            SystemError, TypeError, ValueError, UnicodeError, UnicodeDecodeError, UnicodeEncodeError,
            UnicodeTranslateError]


exc_map = {exc.__name__: exc for exc in exc_list}


class AsyncRPCClient(object):
    def __init__(self, url: str, access_key: str, user: str = USER_NAME) -> None:
        self.url = url
        self.access_key = access_key
        self.user = user
        self.request_in_progress = False
        self.http_conn = aiohttp.ClientSession()
        self.connected = False

    async def connect(self):
        await self.http_conn.__aenter__()
        self.connected = True

    async def close(self, x, y, z):
        assert self.connected
        await self.http_conn.__aexit__(x, y, z)
        self.connected = False

    async def __call__(self, name: str, args: List, kwargs: Dict[str, Any]):
        assert self.connected
        assert not self.request_in_progress, "Can't share connection between requests"
        timeout = kwargs.pop("_call_timeout", None)

        async with self.http_conn.post(self.url, data=rpc.serialize(name, args, kwargs)) as resp:
            name, data, kwargs = await rpc.deserialize(resp.content, resp, resp.content_length)
            assert kwargs == {}
            assert len(data) == 1
            if name == rpc.CALL_FAILED:
                exc_cls_name, message, tb = data[0]
                raise exc_map.get(exc_cls_name, Exception)("{}\n\nOriginal traceback:\n{}".format(message, tb))
            else:
                return data[0]

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self, name)


class RPCFuncProxy:
    def __init__(self, rpc: AsyncRPCClient, mod_name: str, func_name: str) -> None:
        self.rpc = rpc
        self.name = mod_name + "::" + func_name

    def __call__(self, *args, **kwargs):
        return self.rpc(self.name, list(args), kwargs)


class RPCModuleProxy:
    def __init__(self, rpc: AsyncRPCClient, name: str) -> None:
        self.rpc = rpc
        self.name = name

    def __getattr__(self, name) -> 'RPCFuncProxy':
        return RPCFuncProxy(self.rpc, self.name, name)



async def connect(addr, api_key: str, key_file=None, cert_file=None, conn_timeout=5, timeout=None):
    conn = AsyncRPCClient("https://{}/rpc", api_key)
    await conn.sys.ping()
    return conn
