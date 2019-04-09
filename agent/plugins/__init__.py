import abc
import inspect
import os
import zlib
from dataclasses import field, dataclass
from enum import IntEnum
from typing import Callable, AsyncIterable, BinaryIO, Any, Optional, TypeVar, Dict, Type, Generic
from concurrent.futures._base import CancelledError, TimeoutError as AsyncTimeoutError


DEFAULT_ENVIRON = dict(os.environ.items())


NO_VAR_MARK = DEFAULT_ENVIRON.get("AGENT_NO_VAR_MARK", "<<empty>>")


# return settings for default system python
if 'ORIGIN_PYTHONHOME' in DEFAULT_ENVIRON:
    if DEFAULT_ENVIRON['ORIGIN_PYTHONHOME'].strip() == NO_VAR_MARK:
        del DEFAULT_ENVIRON['PYTHONHOME']
    else:
        DEFAULT_ENVIRON['PYTHONHOME'] = DEFAULT_ENVIRON['ORIGIN_PYTHONHOME']


if 'ORIGIN_PYTHONPATH' in DEFAULT_ENVIRON:
    if DEFAULT_ENVIRON['ORIGIN_PYTHONPATH'].strip() == NO_VAR_MARK:
        del DEFAULT_ENVIRON['PYTHONPATH']
    else:
        DEFAULT_ENVIRON['PYTHONPATH'] = DEFAULT_ENVIRON['ORIGIN_PYTHONPATH']


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


T = TypeVar('T')


@dataclass
class RPCClass(Generic[T]):
    pack: Callable[[T], Dict[str, Any]]
    unpack: Callable[[Dict[str, Any]], T]


exposed_classes: Dict[str, RPCClass] = {}


def default_pack(val: Any) -> Dict[str, Any]:
    return val.__dict__


class Tmp: pass


def default_unpack(tp: T) -> Callable[[Dict[str, Any]], T]:
    def unpack_closure(attrs: Dict[str, Any]) -> T:
        t = Tmp()
        t.__class__ = tp
        t.__dict__.update(attrs)
        return t
    return unpack_closure


def expose_type(tp: Type[T],
                pack: Callable[[T], Dict[str, Any]] = None,
                unpack: Callable[[Dict[str, Any]], T] = None) -> Type[T]:
    if pack is None:
        if hasattr(tp, "__to_json__"):
            pack = tp.__json_reduce__
        else:
            pack = default_pack
    if unpack is None:
        if hasattr(tp, "__from_json__"):
            unpack = tp.__from_json__
        else:
            unpack = default_unpack(tp)
    exposed_classes[f"{tp.__module__}::{tp.__name__}"] = RPCClass(pack, unpack)
    return tp


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
from .ceph import HistoricCollectionConfig
