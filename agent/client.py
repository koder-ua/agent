import os
import re
import ssl
import traceback
import zlib
import http
from contextlib import asynccontextmanager
from io import BytesIO
import time
import logging
import asyncio
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, AsyncIterator, BinaryIO, NamedTuple, cast, Iterable

import aiohttp
from koder_utils import CMDResult, CmdType, IAsyncNode, AnyPath

from . import USER_NAME, DEFAULT_PORT
from .plugins import (IReadableAsync, ChunkedFile, ZlibStreamDecompressor, DEFAULT_HTTP_CHUNK, ZlibStreamCompressor,
                      HistoricCollectionConfig)
from .rpc import BlockType, deserialize, CALL_FAILED, CALL_SUCCEEDED, serialize


logger = logging.getLogger("conn")


exc_list = [BaseException, SystemExit, KeyboardInterrupt, GeneratorExit, Exception, StopIteration,
            BufferError, ArithmeticError, FloatingPointError, OverflowError, ZeroDivisionError, AssertionError,
            AttributeError, EnvironmentError, IOError, OSError, EOFError, ImportError, LookupError, IndexError,
            KeyError, MemoryError, NameError, UnboundLocalError, ReferenceError, RuntimeError, NotImplementedError,
            SystemError, TypeError, ValueError, UnicodeError, UnicodeDecodeError, UnicodeEncodeError,
            FileNotFoundError, UnicodeTranslateError, subprocess.CalledProcessError, subprocess.TimeoutExpired]


exc_map = {exc.__name__: exc for exc in exc_list}


async def process_rpc_results(resp: aiohttp.ClientResponse, allow_streamed: bool = False) -> Any:
    if resp.status != http.HTTPStatus.OK:
        raise RPCServerFailure(f"Server failed with code {resp.status}")

    name, data, kwargs = await deserialize(resp.content.iter_chunked(DEFAULT_HTTP_CHUNK), allow_streamed=allow_streamed)
    assert kwargs == {}
    assert len(data) == 1
    if name == CALL_FAILED:
        exc_cls_name, args, tb = data[0]
        if exc_cls_name == 'TypeError':
            traceback.print_stack()
        exc = exc_map.get(exc_cls_name, Exception)(*args)
        raise exc from Exception("RPC server traceback:\n" + tb)
    else:
        assert name == CALL_SUCCEEDED
        return data[0]


class RPCServerFailure(Exception):
    pass


class AsyncRPCClient:
    def __init__(self, hostname_or_url: str,
                 ssl_cert_file: Optional[Union[str, Path]],
                 api_key: str,
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
            self._rpc_url = f"https://{hostname_or_url}:{port}/conn"
            self._ping_url = f"https://{hostname_or_url}:{port}/ping"

        self._api_key = api_key
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

        self._auth = aiohttp.BasicAuth(login=user, password=self._api_key)
        self._post_params = {"url": self._rpc_url, "ssl": self._ssl, "auth": self._auth,
                             "verify_ssl": self._ssl is not None}

    @property
    def streamed(self) -> 'StreamedProxy':
        return StreamedProxy(self)

    async def wait_ready(self, timeout: float = 30, period: float = 0.1):
        end_time = time.time() + timeout
        wait_time = timeout

        async def ping():
            async with self._http_conn.get(self._ping_url, ssl=self._ssl, verify_ssl=self._ssl is not None):
                pass

        while wait_time > 0:
            try:
                await asyncio.wait_for(ping(), wait_time)
                break
            except aiohttp.ClientConnectionError:
                await asyncio.sleep(period)
            wait_time = end_time - time.time()
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
        return timeout, serialize(name, args, kwargs)

    async def __call__(self, name: str, args: List, kwargs: Dict[str, Any]) -> Any:
        _, data = self._prepare_call(name, args, kwargs)
        async with self._http_conn.post(**self._post_params, data=data) as resp:
            return await process_rpc_results(resp)

    def _call_streamed(self, name: str, args: List, kwargs: Dict[str, Any]) -> 'StreamedCall':
        _, data = self._prepare_call(name, args, kwargs)
        return StreamedCall(self._http_conn, self._post_params, data)

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self, name, streamed=False)


class StreamedProxy:
    def __init__(self, rpc_conn: AsyncRPCClient) -> None:
        self.rpc_conn = rpc_conn

    def __getattr__(self, name) -> 'RPCModuleProxy':
        return RPCModuleProxy(self.rpc_conn, name, streamed=True)


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
    def __init__(self, rpc_conn: AsyncRPCClient, mod_name: str, streamed: bool) -> None:
        self.rpc_conn = rpc_conn
        self.mod_name = mod_name
        self.streamed = streamed

    def __getattr__(self, name) -> 'RPCFuncProxy':
        return RPCFuncProxy(self.rpc_conn, mod_name=self.mod_name, func_name=name, streamed=self.streamed)


class RPCFuncProxy:

    def __init__(self, rpc_conn: AsyncRPCClient, mod_name: str, func_name: str, streamed: bool) -> None:
        self.rpc_conn = rpc_conn
        self.mod_name = mod_name
        self.streamed = streamed
        self.name = f"{mod_name}::{func_name}"

    def __call__(self, *args, **kwargs) -> Any:
        if self.streamed:
            return self.rpc_conn._call_streamed(self.name, list(args), kwargs)
        else:
            return self.rpc_conn(self.name, list(args), kwargs)  # awaitable


class ConnectionClosed(Exception):
    pass


def to_streamed_content(content: Union[bytes, BinaryIO, IReadableAsync]) -> IReadableAsync:
    if isinstance(content, bytes):
        return ChunkedFile(BytesIO(content))
    elif isinstance(content, IReadableAsync):
        return content
    else:
        return ChunkedFile(content)


def compress_proxy():
    pass


class StatRes(NamedTuple):
    mode: int
    ino: int
    dev: int
    nlink: int
    uid: int
    gid: int
    size: int
    atime: int
    mtime: int
    ctime: int


class ReadFileLike:
    pass


class WriteFileLike(IReadableAsync):
    def __init__(self) -> None:
        self._q = asyncio.Queue(maxsize=1)
        self._closed = False

    def close(self) -> None:
        self._q.put(None)

    async def readany(self) -> bytes:
        if self._closed:
            return b''

        data = await self._q.get()
        if data is None:
            assert self._closed
            return b''
        else:
            return data

    async def write(self, data: bytes) -> None:
        await self._q.put(data)


class IReadableAsyncFromStream(IReadableAsync):
    def __init__(self, chunked_aiter: AsyncIterator[Tuple[BlockType, bytes]],
                 expected_type: BlockType = BlockType.binary) -> None:
        self.chunked_aiter = chunked_aiter
        self.expected_type = expected_type

    async def readany(self) -> bytes:
        async for tp, data in self.chunked_aiter:
            assert tp == self.expected_type
            return data
        return b''


class IAgentRPCNode(IAsyncNode):
    def __init__(self, conn_addr: str, conn: AsyncRPCClient) -> None:
        self.conn_addr = conn_addr
        self.conn = conn

    def __str__(self) -> str:
        return f"RPC({self.conn_addr})"

    async def read(self, path: AnyPath, compress: bool = True) -> bytes:
        return b"".join([chunk async for chunk in self.iter_file(str(path), compress=compress)])

    async def tail_file(self, path: AnyPath, size: int) -> AsyncIterator[bytes]:
        async with self.conn.streamed.fs.tail(str(path), size) as block_iter:
            async for tp, chunk in block_iter:
                assert tp == BlockType.binary
                yield chunk

    async def iter_file(self, path: AnyPath, compress: bool = True) -> AsyncIterator[bytes]:
        async with self.conn.streamed.fs.get_file(str(path), compress=compress) as block_iter:
            if compress:
                async for chunk in ZlibStreamDecompressor(IReadableAsyncFromStream(block_iter)):
                    yield chunk
            else:
                async for tp, chunk in block_iter:
                    assert tp == BlockType.binary
                    yield chunk

    async def write(self, path: AnyPath, content: Union[BinaryIO, bytes, IReadableAsync], compress: bool = True):
        stream = to_streamed_content(content)
        if compress:
            stream = ZlibStreamCompressor(stream)
        await self.conn.fs.write_file(str(path), stream, compress=compress)

    async def write_tmp(self, content: Union[BinaryIO, bytes, IReadableAsync], compress: bool = True) -> Path:
        stream = to_streamed_content(content)
        if compress:
            stream = ZlibStreamCompressor(stream)
        return Path(await self.conn.fs.write_file(None, stream, compress=compress))

    async def stat(self, path: AnyPath) -> os.stat_result:
        return cast(os.stat_result, StatRes(*(await self.conn.fs.stat(str(path)))))

    async def run(self, cmd: CmdType, input_data: Union[bytes, None, BinaryIO] = None,
                  merge_err: bool = True, timeout: float = 60, output_to_devnull: bool = False,
                  term_timeout: float = 1, env: Dict[str, str] = None,
                  compress: bool = True) -> CMDResult:

        assert isinstance(input_data, bytes) or input_data is None
        code, out, err = await self.conn.cli.run_cmd(cmd if isinstance(cmd, str) else [str(i) for i in cmd],
                                                     term_timeout=term_timeout,
                                                     timeout=timeout, input_data=input_data, merge_err=merge_err,
                                                     env=env, compress=compress)

        if merge_err:
            assert err is None

        if compress:
            out = zlib.decompress(out)
            err = None if err is None else zlib.decompress(err)

        return CMDResult(cmd, out, err, code)

    async def disconnect(self) -> None:
        pass

    async def __aenter__(self) -> 'IAsyncNode':
        await self.conn.__aenter__()
        return self

    async def __aexit__(self, x, y, z) -> bool:
        return await self.conn.__aexit__(x, y, z)

    async def exists(self, fname: AnyPath) -> bool:
        return await self.conn.fs.file_exists(str(fname))

    async def iterdir(self, path: AnyPath) -> Iterable[Path]:
        return map(Path, await self.conn.fs.iterdir(str(path)))

    async def open(self, path: AnyPath, mode: str = "wb", compress: bool = True) -> Union[ReadFileLike, WriteFileLike]:
        if mode == "wb":
            flike = WriteFileLike()
            asyncio.create_task(self.write(path, flike, compress=compress))
            return flike
        raise ValueError(f"Unsupported mode {mode}")


class ConnectionPool:
    def __init__(self, certificates: Dict[str, Any], max_conn_per_node: int, **extra_kwargs) -> None:

        self.free_conn: Dict[str, List[IAgentRPCNode]] = {}
        self.conn_per_node: Dict[str, int] = {}
        self.max_conn_per_node = max_conn_per_node
        self.certificates = certificates
        self.extra_kwargs = extra_kwargs
        self.opened = False

    async def get_conn(self, conn_addr: str) -> IAgentRPCNode:
        assert self.opened, "Pool is not opened"
        while True:
            free_cons = self.free_conn.setdefault(conn_addr, [])
            if free_cons:
                return free_cons.pop()

            if self.conn_per_node.setdefault(conn_addr, 0) < self.max_conn_per_node:
                self.conn_per_node[conn_addr] += 1
                new_conn = await self._rpc_connect(conn_addr)
                return new_conn

            await asyncio.sleep(0.5)

    def release_conn(self, conn_addr: str, conn: IAgentRPCNode) -> None:
        assert self.opened, "Pool is not opened"
        return self.free_conn[conn_addr].append(conn)

    async def _rpc_connect(self, conn_addr: str) -> IAgentRPCNode:
        """Connect to nodes and fill Node object with basic node info: ips and hostname"""

        rpc = AsyncRPCClient(conn_addr,
                             ssl_cert_file=self.certificates[conn_addr],
                             **self.extra_kwargs)

        await rpc.__aenter__()
        return IAgentRPCNode(conn_addr, rpc)

    async def __aenter__(self) -> 'ConnectionPool':
        assert not self.opened, "Pool already opened"
        self.opened = True
        return self

    async def __aexit__(self, x, y, z) -> None:
        assert self.opened, "Pool is not opened"
        for addr, conns in self.free_conn.items():
            assert len(conns) == self.conn_per_node[addr]
            for conn in conns:
                await conn.__aexit__(x, y, z)
            self.conn_per_node[addr] = 0
        self.free_conn = {}
        self.opened = False

    @asynccontextmanager
    async def connection(self, conn_addr: str) -> AsyncIterator[IAgentRPCNode]:
        conn = await self.get_conn(conn_addr)
        try:
            yield conn
        finally:
            self.release_conn(conn_addr, conn)


async def get_sock_count(conn: IAgentRPCNode, pid: int) -> int:
    return await conn.conn.fs.count_sockets_for_process(pid)


async def get_device_for_file(conn: IAgentRPCNode, fname: str) -> Tuple[str, str]:
    """Find storage device, on which file is located"""

    dev = (await conn.conn.fs.get_dev_for_file(fname)).decode()
    assert dev.startswith('/dev'), "{!r} is not starts with /dev".format(dev)
    root_dev = dev = dev.strip()
    rr = re.match('^(/dev/[shv]d.*?)\\d+', root_dev)
    if rr:
        root_dev = rr.group(1)
    return root_dev, dev

