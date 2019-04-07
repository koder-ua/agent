import abc
import inspect
import zlib
from dataclasses import field, dataclass
from enum import IntEnum
from typing import Callable, AsyncIterable, BinaryIO, Any, Optional


def validate_name(name: str):
    assert not name.startswith("_")
    assert name != 'streamed'


exposed = {}
exposed_async = {}


def expose_func(module: str, func: Callable):
    if inspect.iscoroutinefunction(func):
        validate_name(module)
        validate_name(func.__name__)
        exposed_async[module + "::" + func.__name__] = func
    else:
        validate_name(module)
        validate_name(func.__name__)
        exposed[module + "::" + func.__name__] = func
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
    till_offset: Optional[int] = None
    close_at_the_end: bool = False

    def done(self):
        if self.close_at_the_end:
            self.fd.close()
        self.closed = True

    async def readany(self) -> bytes:
        if self.closed:
            return b""

        if self.till_offset:
            offset = self.fd.tell()
            if offset >= self.till_offset:
                self.done()
                return b""
            max_read = min(offset - self.till_offset, self.chunk)
        else:
            max_read = self.chunk

        data = self.fd.read(max_read)
        if not data:
            self.done()
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
