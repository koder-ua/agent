import abc
import zlib
from dataclasses import field, dataclass
from enum import IntEnum
from typing import Callable, AsyncIterable, BinaryIO, Any


def validate_name(name: str):
    assert not name.startswith("_")
    assert name != 'streamed'


exposed = {}
exposed_async = {}


def expose_func(module: str, func: Callable):
    validate_name(module)
    validate_name(func.__name__)
    exposed[module + "::" + func.__name__] = func
    return func


def expose_func_async(module: str, func: Callable):
    validate_name(module)
    validate_name(func.__name__)
    exposed_async[module + "::" + func.__name__] = func
    return func


DEFAULT_FILE_CHUNK = 1 << 20
DEFAULT_HTTP_CHUNK = 1 << 16
DEFAULT_COMPRESSOR_CHUNK = DEFAULT_HTTP_CHUNK


class BlockType(IntEnum):
    json = 1
    binary = 2


class IReadableAsync(AsyncIterable[bytes]):
    @abc.abstractmethod
    async def readany(self) -> bytes:
        pass

    def __aiter__(self) -> 'IReadableAsync':
        return self

    async def __anext__(self) -> bytes:
        res = await self.readany()
        if not res:
            raise StopAsyncIteration()
        return res


@dataclass
class ChunkedFile(IReadableAsync):
    fd: BinaryIO
    chunk: int = DEFAULT_FILE_CHUNK
    closed: bool = field(default=False, init=False)

    async def readany(self) -> bytes:
        if self.closed:
            return b""

        data = self.fd.read(self.chunk)
        if not data:
            self.closed = True
        return data


@dataclass
class ZlibStreamCompressor(IReadableAsync):
    fd: IReadableAsync
    min_chunk: int = DEFAULT_COMPRESSOR_CHUNK
    compressor: Any = field(default_factory=zlib.compressobj, init=False)
    eof: bool = field(default=False, init=False)

    async def readany(self) -> bytes:
        if self.eof:
            return b''

        curr = b''
        async for chunk in self.fd:
            assert chunk
            curr += self.compressor.compress(chunk)
            if len(curr) >= self.min_chunk:
                return curr

        self.eof = True
        return curr + self.compressor.flush()


@dataclass
class ZlibStreamDecompressor(IReadableAsync):
    fd: IReadableAsync
    min_chunk: int = DEFAULT_COMPRESSOR_CHUNK
    decompressor: Any = field(default_factory=zlib.decompressobj, init=False)
    eof: bool = field(default=False, init=False)

    async def readany(self) -> bytes:
        if self.eof:
            return b''

        curr = b''
        async for chunk in self.fd:
            assert chunk
            curr += self.decompressor.decompress(chunk)
            if len(curr) >= self.min_chunk:
                return curr

        self.eof = True
        return curr + self.decompressor.flush()


from . import ceph, cli, fs, system
