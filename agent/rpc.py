import abc
import base64
import json
import zlib

from async_generator import async_generator, yield_

from typing import Any, Dict, List, Tuple, Optional, Callable
from typing.io import BinaryIO

exposed = {}
exposed_async = {}

EOD_MARKER = b'\x00'
CUSTOM_TYPE_KEY = '__custom__type_658aaae5-6216-4fe0-8483-d51cf21a6ba5'
STREAM_TYPE = 'binary_stream'
BYTES_TYPE = 'bytes'

DEFAULT_CHUNK = 1 << 20
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


class StreamReaderProxy(IReadableAsync):
    def __init__(self, data: bytes, reader: IReadableAsync) -> None:
        self.data = data
        self.reader = reader

    async def readany(self) -> bytes:
        if self.data:
            res = self.data
            self.data = b""
        else:
            res = await self.reader.readany()
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


def unpack_from_json(data: Dict[str, Any], stream: IReadableAsync) -> Tuple[str, List, Dict, bool]:
    args = data.pop('args')
    assert type(args) is list

    name = data.pop('name')
    assert isinstance(name, str)

    kwargs = data.pop('kwargs')
    assert type(kwargs) is dict
    assert all(isinstance(key, str) for key in kwargs)

    streams = [stream]
    args = do_unpack_from_json(args, streams)
    kwargs = do_unpack_from_json(kwargs, streams)
    return name, args, kwargs, streams == []


def do_unpack_from_json(val: Any, streams: List[IReadableAsync]) -> Any:
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


@async_generator
async def serialize(name: str, args: List, kwargs: Dict[str, Any]):
    args, maybe_stream = prepare_for_json(args, kwargs)
    args['name'] = name
    serialized_args = json.dumps(args).encode("utf8")
    assert EOD_MARKER not in serialized_args
    await yield_(serialized_args + EOD_MARKER)
    if maybe_stream is not None:
        while True:
            data = await maybe_stream.readany()
            assert isinstance(data, bytes), "Stream must yield bytes type, not {}".format(type(data))
            if data == b'':
                break
            await yield_(data)


async def deserialize(data_stream, allow_streamed: bool = False):
    """
    Unpack request from aiohttp.StreamReader or compatible stream
    """
    data = b""
    eof_found = False
    while True:
        new_chunk = await data_stream.readany()
        data += new_chunk
        if not new_chunk:
            eof_found = True

        eof_offset = data.find(EOD_MARKER)
        if eof_offset >= 0:
            break

        if not new_chunk:
            assert new_chunk, "Stream closed, but no EOD_MARKER found\n%r" % (data,)

    js_data = json.loads(data[:eof_offset].decode('utf8'))
    stream = StreamReaderProxy(data[eof_offset + 1:], data_stream)
    name, args, kwargs, use_stream = unpack_from_json(js_data, stream)

    if use_stream:
        if not allow_streamed:
            raise ValueError("Streaming not allowed for this call")
    else:
        assert eof_offset + 1 == len(data), "{} != {}".format(eof_offset + 1, len(data))
        if not eof_found:
            new_chunk = await data_stream.readany()
            assert new_chunk == b'', "Extra data after the end of json part, not consumed"

    return name, args, kwargs

