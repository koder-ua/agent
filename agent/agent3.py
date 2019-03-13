import re
import os
import asyncio

try:
    import ssl
except ImportError:
    ssl = None

import sys
import time
import socket
import struct
import hashlib
import tempfile
import argparse
import threading
import subprocess
from pprint import pprint
from io import BytesIO
from typing import Any, Sequence, Dict, Tuple


from .agent import PACK_MAPPING, serialize_exception, UNPACK_MAPPING


def serialize(obj: Any) -> bytes:
    try:
        code, func, _ = PACK_MAPPING[type(obj)]
    except KeyError:
        if isinstance(obj, Exception):
            return PACK_MAPPING[Exception][0] + serialize_exception(obj)
        raise ValueError("Can't serialize {0!r}".format(obj))
    return code + func(obj)


def deserialize(stream):
    if isinstance(stream, bytes):
        stream = BytesIO(stream)

    try:
        func = UNPACK_MAPPING[stream.read(1)]
    except KeyError as exc:
        raise ValueError("Can't deserialize typecode {0!r}".format(exc))

    return func(stream)


class ConnectionClosed(Exception):
    pass


class AsyncioTransport:
    MAX_TIMEOUT = 60 * 60 * 24 * 365
    size_s = struct.Struct("!I")
    min_blob_size = 64

    def __init__(self,
                 reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter,
                 remote_addr: Tuple[str, int],
                 timeout: float = 5) -> None:
        self.reader = reader
        self.writer = writer
        self.remote_addr = remote_addr
        self.timeout = self.MAX_TIMEOUT if timeout is None else timeout

    def close(self):
        self.writer.close()

    def get_timeout(self, timeout: float = None) -> float:
        return self.timeout if timeout is None else timeout

    async def send_message(self, name: str, args: Sequence[Any], kwargs: Dict[str, Any], timeout: float = None):
        message_s = serialize([name, args, kwargs])
        message_s = self.size_s.pack(len(message_s)) + message_s
        md5 = hashlib.md5()
        md5.update(message_s)
        self.writer.write(message_s + md5.digest())
        await asyncio.wait_for(self.writer.drain(), self.get_timeout(timeout))

    async def recv_message(self, timeout: float = None) -> Tuple[str, Sequence[Any], Dict[str, Any]]:
        md5 = hashlib.md5()
        etime = time.time() + self.get_timeout(timeout)
        data_sz_s = await asyncio.wait_for(self.reader.read(self.size_s.size), etime - time.time())
        md5.update(data_sz_s)
        data_sz, = self.size_s.unpack(data_sz_s)
        data_s = await asyncio.wait_for(self.reader.read(data_sz), etime - time.time())
        md5.update(data_s)
        name, args, kwargs = deserialize(data_s)
        digest = md5.digest()
        assert (await asyncio.wait_for(self.reader.read(len(digest)), etime - time.time())) == digest
        return name, args, kwargs


# -------------------------------   API  -------------------------------------------------------------------------------

exc_list = [BaseException, SystemExit, KeyboardInterrupt, GeneratorExit, Exception, StopIteration,
            BufferError, ArithmeticError, FloatingPointError, OverflowError, ZeroDivisionError, AssertionError,
            AttributeError, EnvironmentError, IOError, OSError, EOFError, ImportError, LookupError, IndexError,
            KeyError, MemoryError, NameError, UnboundLocalError, ReferenceError, RuntimeError, NotImplementedError,
            SystemError, TypeError, ValueError, UnicodeError, UnicodeDecodeError, UnicodeEncodeError,
            UnicodeTranslateError]


exc_map = {exc.__name__: exc for exc in exc_list}


class SimpleRPCClient(object):
    def __init__(self, transport, name=None, lock=None):
        self._tr = transport
        self._name = name
        if lock is None:
            self._rpc_lock = threading.Lock()
        else:
            self._rpc_lock = lock

    async def __call__(self, *args, **kwargs):
        assert self._tr is not None, "Connection already closed"

        if self._name is None:
            raise ValueError("Can't call empty name")

        if '_send_timeout' in kwargs:
            send_timeout = kwargs.pop("_send_timeout")
        else:
            send_timeout = None

        if '_recv_timeout' in kwargs:
            recv_timeout = kwargs.pop("_recv_timeout")
        else:
            recv_timeout = None

        with self._rpc_lock:
            await self._tr.send_message(self._name, args, kwargs, timeout=send_timeout)
            name, (res, tb, exc_cls_name), kwargs = await self._tr.recv_message(timeout=recv_timeout)

        assert name is None
        assert kwargs == {}

        if not exc_cls_name:
            return res

        exc_msg = res.decode("utf8")
        if tb is not None:
            exc_msg += "\nOriginal traceback:\n" + tb.decode("utf8")

        if exc_cls_name in exc_map:
            raise exc_map[exc_cls_name](exc_msg)

        raise RuntimeError(exc_msg)

    def disconnect(self):
        assert self._tr is not None, "Connection already closed"

        self._tr.close()
        self._tr = None

    def __getattr__(self, name):
        assert self._tr is not None, "Connection already closed"

        if self._name is not None:
            name = self._name + '.' + name
        return self.__class__(self._tr, name, lock=self._rpc_lock)

    def __enter__(self):
        assert self._tr is not None, "Connection already closed"
        return self

    def __exit__(self, x, y, z):
        self.disconnect()


async def connect(loop: asyncio.AbstractEventLoop,
                  addr: Tuple[str, int],
                  cert_file: str = None,
                  conn_timeout: float = 5,
                  timeout: float = None) -> SimpleRPCClient:
    if cert_file:
        sc = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=cert_file)
    else:
        sc = None
    reader, writer = await asyncio.wait_for(await asyncio.open_connection(*addr, loop=loop, ssl=sc), conn_timeout)
    return SimpleRPCClient(AsyncioTransport(reader, writer, addr, timeout=timeout))


# ----------------------------------------------  CLI hendlers ---------------------------------------------------------


def parse_type(val):
    if re.match(r"-?\d+", val):
        return int(val)
    return val


async def client_main(loop, opts):
    rpc = await connect(loop, addr=opts.server_addr.split(":"), cert_file=opts.cert_file)

    with rpc:
        for part in opts.name.split("."):
            rpc = getattr(rpc, part)

        pprint(await rpc(*map(parse_type, opts.params)))


def make_cert_and_key(key_file=None, cert_file=None,
                      subj="/C=NN/ST=Some/L=Some/O=Ceph-monitor/OU=Ceph-monitor/CN=mirantis.com"):

    if key_file is None:
        key_file = tempfile.mktemp()

    if cert_file is None:
        cert_file = tempfile.mktemp()

    os.close(os.open(key_file, os.O_WRONLY | os.O_CREAT, 0o600))
    os.close(os.open(cert_file, os.O_WRONLY | os.O_CREAT, 0o600))

    subprocess.check_call("openssl genrsa 1024 2>/dev/null > " + key_file, shell=True)
    subprocess.check_call('openssl req -new -x509 -nodes -sha1 -days 365 ' +
                          '-key "{0}" -subj "{1}" > {2} 2>/dev/null'.format(key_file, subj, cert_file),
                          shell=True)

    return key_file, cert_file


def parse_args(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')

    parser.add_argument("-k", "--key-file", default=None)
    parser.add_argument("-c", "--cert-file", default=None)

    client_parser = subparsers.add_parser('call', help='send cmd to server')
    client_parser.add_argument("-s", "--server-addr", required=True)
    client_parser.add_argument("name")
    client_parser.add_argument("params", nargs="*")
    client_parser.set_defaults(subparser_name="call")

    keygen_parser = subparsers.add_parser('gen_keys', help='Generate keys')
    keygen_parser.add_argument("--subj",
                               default="/C=NN/ST=Some/L=Some/O=Ceph-monitor/OU=Ceph-monitor/CN=mirantis.com")
    keygen_parser.set_defaults(subparser_name="keygen")

    return parser.parse_args(argv[1:])


def main(argv):
    opts = parse_args(argv)

    if opts.subparser_name == 'call':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(client_main(loop, opts))
        loop.close()
    elif opts.subparser_name == 'keygen':
        key, cert = make_cert_and_key(cert_file=opts.key_file,
                                      key_file=opts.cert_file,
                                      subj=opts.subj)
        print("key={0}\ncert={1}".format(key, cert))
    else:
        sys.stderr.write("Unknown cmd\n")

    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
