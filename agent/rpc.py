import abc
import base64
import hashlib
import json
import struct
import zlib
from enum import Enum

from async_generator import async_generator, yield_

from typing import Any, Dict, List, Tuple, Optional, Callable, NamedTuple
from typing.io import BinaryIO

exposed = {}
exposed_async = {}

EOD_MARKER = b'\x00'
CUSTOM_TYPE_KEY = '__custom__type_658aaae5-6216-4fe0-8483-d51cf21a6ba5'
STREAM_TYPE = 'binary_stream'
BYTES_TYPE = 'bytes'

DEFAULT_CHUNK = 1 << 20

# use string instead of int to unify call and return code
CALL_FAILED = 'fail'
CALL_SUCCEEDED = 'success'


def validate_name(name: str):
    assert not name.startswith("_")
    assert name != 'streamed'


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


# async stream classes

class RPCStreamError(Exception):
    pass


class IReadableAsync:
    @abc.abstractmethod
    async def readany(self) -> bytes:
        pass


class ChunkedFile(IReadableAsync):
    def __init__(self, fd: BinaryIO, chunk: int = DEFAULT_CHUNK) -> None:
        self.fd = fd
        self.chunk = chunk

    async def readany(self) -> bytes:
        if self.fd.closed:
            return b""

        data = self.fd.read(self.chunk)
        if not data:
            self.fd.close()
        return data


class ZlibStreamCompressor(IReadableAsync):

    def __init__(self, fd: IReadableAsync) -> None:
        self.fd = fd
        self.compressor = zlib.compressobj()
        self.eof = False

    async def readany(self) -> bytes:
        if self.eof:
            return b''

        res = b''
        while not res:
            data = await self.fd.readany()
            if data == b'':
                self.eof = True
                return res + self.compressor.flush()
            res += self.compressor.compress(data)
        return res


def prepare_for_json(args: List, kwargs: Dict) -> Tuple[Dict[str, Any], Optional[IReadableAsync]]:
    streams = []
    p_args = do_prepare_for_json(args, streams)
    p_kwargs = do_prepare_for_json(kwargs, streams)
    return {'args': p_args, 'kwargs': p_kwargs}, None if streams == [] else streams[0]


def do_prepare_for_json(val: Any, streams: List[IReadableAsync]) -> Any:
    vt = type(val)
    if vt in (int, float, str, bool) or val is None:
        return val

    if vt is bytes:
        return {CUSTOM_TYPE_KEY: BYTES_TYPE, 'val': base64.b64encode(val).decode('ascii')}

    if vt is list or vt is tuple:
        return [do_prepare_for_json(i, streams) for i in val]

    if vt is dict:
        assert all(isinstance(key, str) for key in val)
        assert CUSTOM_TYPE_KEY not in val, "Can't use {:!r} as key serializable dict".format(CUSTOM_TYPE_KEY)
        return {key: do_prepare_for_json(value, streams) for key, value in val.items()}

    if isinstance(val, IReadableAsync):
        assert len(streams) == 0, "Params can only contains single stream"
        streams.append(val)
        return {CUSTOM_TYPE_KEY: STREAM_TYPE}

    raise TypeError("Can't serialize value of type {}".format(vt))


def unpack_from_json(data: Dict[str, Any], block_aiter) -> Tuple[str, List, Dict, bool]:
    args = data.pop('args')
    assert type(args) is list

    name = data.pop('name')
    assert isinstance(name, str)

    kwargs = data.pop('kwargs')
    assert type(kwargs) is dict
    assert all(isinstance(key, str) for key in kwargs)

    streams = [block_aiter]
    args = do_unpack_from_json(args, streams)
    kwargs = do_unpack_from_json(kwargs, streams)
    return name, args, kwargs, streams == []


def do_unpack_from_json(val: Any, streams: List) -> Any:
    vt = type(val)
    if vt in (int, float, str, bool) or val is None:
        return val

    if vt is list:
        return [do_unpack_from_json(i, streams) for i in val]

    if vt is dict:
        cctype = val.get(CUSTOM_TYPE_KEY)
        if cctype is None:
            assert all(isinstance(key, str) for key in val)
            return {key: do_unpack_from_json(value, streams) for key, value in val.items()}
        elif cctype == STREAM_TYPE:
            assert streams
            return streams.pop()
        elif cctype == BYTES_TYPE:
            return base64.b64decode(val['val'].encode('ascii'))

    raise TypeError("Can't deserialize value of type {}".format(vt))


class BlockType(Enum):
    json = 1
    binary = 2


block_header_struct = struct.Struct("!BL")
hash_factory = hashlib.md5
hash_digest_size = hash_factory().digest_size
block_header_size = block_header_struct.size + hash_digest_size


def check_digest(block_data: bytes, expected_digest: bytes):
    hashobj = hash_factory()
    hashobj.update(block_data)

    if hashobj.digest() != expected_digest:
        raise RPCStreamError("Checksum failed")


def make_block_header(tp: BlockType, data: bytes) -> bytes:
    hashobj = hash_factory()
    tp_and_size = block_header_struct.pack(tp.value, len(data))
    hashobj.update(tp_and_size)
    hashobj.update(data)
    return hashobj.digest() + tp_and_size


class Block(NamedTuple):
    tp: BlockType
    header_size: int
    data_size: int
    hash: bytes
    raw_header: bytes


def parse_block_header(header: bytes) -> Block:
    assert len(header) == block_header_size
    tp, size = block_header_struct.unpack(header[hash_digest_size:])
    return Block(BlockType(tp), block_header_size, size, header[:hash_digest_size],
                 raw_header=header[hash_digest_size:])


def try_parse_block_header(buffer: bytes) -> Tuple[Optional[BinaryIO], bytes]:
    if len(buffer) < block_header_size:
        return None, buffer

    return parse_block_header(buffer[:block_header_size]), buffer[block_header_size:]


@async_generator
async def yield_blocks(data_stream):
    buffer = b""
    block: Optional[Block] = None

    while True:
        new_chunk = await data_stream.readany()
        buffer += new_chunk

        # while we have enought data to produce new blocks
        while True:
            if block is None:
                block, buffer = try_parse_block_header(buffer)

            # if not enought data - exit
            if block is None or len(buffer) < block.data_size:
                break

            block_data = buffer[:block.data_size]
            check_digest(block.raw_header + block_data, block.hash)
            await yield_((block.tp, block_data))

            buffer = buffer[block.data_size:]
            block = None

        # if not enought data and no new data - exit
        if not new_chunk:
            break

    if block is not None:
        raise RPCStreamError("Stream ends before all data transferred")

    if buffer != b'':
        raise RPCStreamError(f"Stream ends before all data transferred: {buffer}")


@async_generator
async def serialize(name: str, args: List, kwargs: Dict[str, Any]):
    args, maybe_stream = prepare_for_json(args, kwargs)
    args['name'] = name
    serialized_args = json.dumps(args).encode("utf8")
    await yield_(make_block_header(BlockType.json, serialized_args) + serialized_args)
    if maybe_stream is not None:
        while True:
            data = await maybe_stream.readany()
            assert isinstance(data, bytes), "Stream must yield bytes type, not {}".format(type(data))
            if data == b'':
                break
            await yield_(make_block_header(BlockType.binary, data) + data)


async def deserialize(data_stream, allow_streamed: bool = False) -> Tuple[str, List, Dict]:
    """
    Unpack request from aiohttp.StreamReader or compatible stream
    """

    blocks_iter = yield_blocks(data_stream)
    tp, data = await blocks_iter.__anext__()
    if tp != BlockType.json:
        raise RPCStreamError("Get block type of {} instead of json".format(tp.name))

    js_data = json.loads(data.decode('utf8'))
    name, args, kwargs, use_stream = unpack_from_json(js_data, blocks_iter)

    if use_stream:
        if not allow_streamed:
            raise ValueError("Streaming not allowed for this call")
    else:
        try:
            await blocks_iter.__anext__()
        except StopAsyncIteration:
            pass
        else:
            raise RPCStreamError("Extra data after end of message")

    return name, args, kwargs
