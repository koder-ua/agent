import abc
import json

from typing import Any, Dict, List, Tuple, Iterator, Optional, Callable

exposed = {}
exposed_async = {}

EOD_MARKER = b'\x00'
CUSTOM_TYPE_KEY = '__custom__type_658aaae5-6216-4fe0-8483-d51cf21a6ba5'
STREAM_TYPE = 'binary_stream'
BYTES_TYPE = 'bytes'

DEFAULT_CHUNK = 1 << 20
CALL_FAILED = 'fail'
CALL_SUCCEEDED = 'success'


def expose_func(module: str, func: Callable):
    exposed[module + "::" + func.__name__] = func
    return func

def expose_func_async(module: str, func: Callable):
    exposed_async[module + "::" + func.__name__] = func
    return func


class IClousable(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def close(self):
        pass


class IReadable(IClousable):
    @abc.abstractmethod
    async def read_chunk(self):
        pass


class FDIReadable(IReadable):
    def __init__(self, fd, chunk_size: int = DEFAULT_CHUNK) -> None:
        self.chunk_size = chunk_size
        self.fd = fd

    async def read_chunk(self):
        return self.fd.read(self.chunk_size)

    def close(self):
        self.fd.close()


class StreamPayload:
    def __init__(self, fd: IReadable) -> None:
        self.fd = fd

    def close(self):
        self.fd.close()


class StreamReaderProxy(IReadable):
    def __init__(self, data: bytes, reader: IReadable, req: IClousable) -> None:
        self.data = data
        self.reader = reader
        self.req = req

    async def read_chunk(self) -> bytes:
        if self.data:
            res = self.data
            self.data = b""
        else:
            res = self.reader.read_chunk()
        return res

    async def close(self):
        await self.req.close()


def prepare(args: List, kwargs: Dict) -> Tuple[Dict[str, Any], Optional[StreamPayload]]:
    streams = []
    p_args = do_prepare(args, streams)
    p_kwargs = do_prepare(kwargs, streams)
    return {'args': p_args, 'kwargs': p_kwargs}, None if streams == [] else streams[0]


def do_prepare(val: Any, streams: List[StreamPayload]) -> Any:
    vt = type(val)
    if vt in (int, float, str, bool) or val is None:
        return val

    if vt is bytes:
        return {CUSTOM_TYPE_KEY: BYTES_TYPE, 'val': val.encode("utf8")}

    if vt is list:
        return [do_prepare(i, streams) for i in val]

    if vt is dict:
        assert all(isinstance(key, str) for key in val)
        assert CUSTOM_TYPE_KEY not in val, "Can't use {:!r} as key serializable dict".format(CUSTOM_TYPE_KEY)
        return {key: do_prepare(value, streams) for key, value in val.items()}

    if vt is StreamPayload:
        assert len(streams) == 0, "Params can only contains single stream"
        streams.append(val)
        return {CUSTOM_TYPE_KEY: STREAM_TYPE}

    raise TypeError("Can't serialize value of type {}".format(vt))


def unprepare(data: Dict[str, Any], stream: StreamReaderProxy) -> Tuple[str, List, Dict, bool]:
    args = data.pop('args')
    assert type(args) is list

    name = data.pop('name')
    assert isinstance(name, str)

    kwargs = data.pop('kwargs')
    assert type(kwargs) is dict
    assert all(isinstance(key, str) for key in kwargs)

    streams = [stream]
    args = do_unprepare(args, streams)
    kwargs = do_unprepare(kwargs, streams)
    return name, args, kwargs, streams == []


def do_unprepare(val: Any, streams: List[StreamReaderProxy]) -> Any:
    vt = type(val)
    if vt in (int, float, str, bool) or val is None:
        return val

    if vt is list:
        return [do_unprepare(i, streams) for i in val]

    if vt is dict:
        cctype = vt.get(CUSTOM_TYPE_KEY)
        if cctype is None:
            assert all(isinstance(key, str) for key in val)
            return {key: do_unprepare(value, streams) for key, value in val.items()}
        elif cctype == STREAM_TYPE:
            assert streams
            return streams.pop()
        elif cctype == BYTES_TYPE:
            return vt['val'].decode("utf8")

    raise TypeError("Can't deserialize value of type {}".format(vt))


def serialize(name: str, args: List, kwargs: Dict[str, Any], read_chunk: int = DEFAULT_CHUNK) -> Iterator[bytes]:
    args, maybe_stream = prepare(args, kwargs)
    args['name'] = name
    serialized_args = json.dumps(args).encode("utf8")
    assert EOD_MARKER not in serialized_args
    yield serialized_args + EOD_MARKER
    if maybe_stream is not None:
        while True:
            data = maybe_stream.fd.read_chunk()
            assert isinstance(data, bytes), "Stream must yield bytes type, not {}".format(type(data))
            if data == b'':
                break
            yield data


async def deserialize(data_stream, req, content_length: int, chunk: int = DEFAULT_CHUNK):
    # read header
    data = b""
    eof_offset = 0
    while True:
        data += (await data_stream.read(chunk))
        eof_offset = data.find(EOD_MARKER)
        if eof_offset >= 0:
            break

    js_data = json.loads(data[:eof_offset].decode('utf8'))
    stream = StreamReaderProxy(data[eof_offset:], data_stream, req)
    name, args, kwargs, use_stream = unprepare(js_data, stream)

    if not use_stream:
        await stream.close()
        assert eof_offset == len(data)
        assert content_length == eof_offset

    return name, args, kwargs

