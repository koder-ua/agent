#!/usr/bin/env python2
from __future__ import print_function

import re
import os

try:
    import ssl
except ImportError:
    ssl = None

import sys
import json
import time
import pickle
import select
import socket
import struct
import hashlib
import logging
import tempfile
import argparse
import traceback
import functools
import threading
import subprocess
from pprint import pprint, pformat


IS_PYTHON3 = (sys.version_info >= (3,))

if IS_PYTHON3:
    import queue
    from io import BytesIO as BIO
    BytesType = bytes
    Unicode = str
else:
    import Queue as queue
    from StringIO import StringIO as BIO
    BytesType = str
    Unicode = unicode

# ----------------------------- LOGGING --------------------------------------------------------------------------------

logger = logging.getLogger('agent')


def setup_logger(opts):
    if opts.log_config:
        if IS_PYTHON3:
            logging.config.fileConfig(opts.log_config)
        else:
            raise ValueError("Python 2.X doesn't support logger config")
    else:
        level = getattr(logging, opts.log_level)
        logger.setLevel(level)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

# ----------------------------- LOAD PLUGINS ---------------------------------------------------------------------------


RPC_TEMPO_MOD = "__rpc_temporary_module__"


if not IS_PYTHON3:
    import imp

    def load_py(path):
        mod = imp.load_source(RPC_TEMPO_MOD, path)
        del sys.modules[RPC_TEMPO_MOD]
        return mod

elif sys.version_info < (3, 4):
    from importlib.machinery import SourceFileLoader

    def load_py(path):
        mod = SourceFileLoader(RPC_TEMPO_MOD, path).load_module()
        del sys.modules[RPC_TEMPO_MOD]
        return mod
else:
    import importlib.util

    def load_py(path):
        spec = importlib.util.spec_from_file_location(RPC_TEMPO_MOD, path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        del sys.modules[RPC_TEMPO_MOD]
        return mod


# ---------------------------------------------  TRANSPORT PROTO -------------------------------------------------------
# ----------------------------------------------- SERIALIZATION --------------------------------------------------------

IntStruct = struct.Struct("!q")
FltStruct = struct.Struct("!d")


def serialize_int(obj):
    return IntStruct.pack(obj)


def deserialize_int(stream):
    return IntStruct.unpack(stream.read(IntStruct.size))[0]


def serialize_float(obj):
    return FltStruct.pack(obj)


def deserialize_float(stream):
    return FltStruct.unpack(stream.read(FltStruct.size))[0]


def serialize_bytes(obj):
    return serialize_int(len(obj)) + obj


def deserialize_bytes(stream):
    return stream.read(deserialize_int(stream))


def serialize_unicode(obj):
    obj_b = obj.encode("utf8")
    return serialize_int(len(obj_b)) + obj_b


def deserialize_unicode(stream):
    return stream.read(deserialize_int(stream)).decode("utf8")


def serialize_bool(obj):
    return b't' if obj else b'f'


def deserialize_bool(stream):
    return stream.read(1) == b't'


def serialize_none(obj):
    return b""


def deserialize_none(stream):
    return None


def serialize_list(obj):
    return serialize_int(len(obj)) + b"".join(map(serialize, obj))


def deserialize_list(stream):
    return [deserialize(stream) for i in range(deserialize_int(stream))]


def serialize_tuple(obj):
    return serialize_int(len(obj)) + b"".join(map(serialize, obj))


def deserialize_tuple(stream):
    return tuple(deserialize(stream) for i in range(deserialize_int(stream)))


def serialize_set(obj):
    return serialize_int(len(obj)) + \
        b"".join(map(serialize, obj))


def deserialize_set(stream):
    return set(deserialize(stream) for i in range(deserialize_int(stream)))


def serialize_dict(obj):
    return serialize_int(len(obj)) + b"".join(map(serialize, obj.keys())) + b"".join(map(serialize, obj.values()))


def deserialize_dict(stream):
    num = deserialize_int(stream)
    keys = [deserialize(stream) for _ in range(num)]
    values = [deserialize(stream) for _ in range(num)]
    return dict(zip(keys, values))


def serialize_exception(obj):
    sobj = pickle.dumps(obj)
    return serialize_int(len(sobj)) + sobj


def deserialize_exception(stream):
    ln = deserialize_int(stream)
    return pickle.loads(stream.read(ln))


PACK_MAPPING = {
    int: (b"i", serialize_int, deserialize_int),
    float: (b"f", serialize_float, deserialize_float),
    BytesType: (b"b", serialize_bytes, deserialize_bytes),
    Unicode: (b"u", serialize_unicode, deserialize_unicode),
    bool: (b"l", serialize_bool, deserialize_bool),
    type(None): (b"n", serialize_none, deserialize_none),
    tuple: (b"T", serialize_tuple, deserialize_tuple),
    list: (b"L", serialize_list, deserialize_list),
    set: (b"S", serialize_set, deserialize_set),
    dict: (b"D", serialize_dict, deserialize_dict),
    Exception: (b"e", serialize_exception, deserialize_exception)
}


UNPACK_MAPPING = dict((code, unpack_func) for code, _, unpack_func in PACK_MAPPING.values())


def serialize(obj):
    try:
        code, func, _ = PACK_MAPPING[type(obj)]
    except KeyError:
        if isinstance(obj, Exception):
            return PACK_MAPPING[Exception][0] + serialize_exception(obj)
        raise ValueError("Can't serialize {0!r}".format(obj))
    return code + func(obj)


def deserialize(stream):
    if isinstance(stream, BytesType):
        stream = BIO(stream)

    try:
        func = UNPACK_MAPPING[stream.read(1)]
    except KeyError as exc:
        raise ValueError("Can't deserialize typecode {0!r}".format(exc))

    return func(stream)


class ConnectionClosed(Exception):
    pass


# TODO: reconnect on error

class Transport(object):
    TIMEOUT = 30
    size_s = struct.Struct("!I")
    min_blob_size = 64

    def __init__(self, sock, timeout=None):
        self.sock = sock
        self.sock.settimeout(self.TIMEOUT if timeout is None else timeout)

    @property
    def remote_addr(self):
        return self.sock.getpeername()

    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        self.sock = None

    def send_message(self, name, args, kwargs, timeout=None):
        message_s = serialize([name, args, kwargs])
        message_s = self.size_s.pack(len(message_s)) + message_s
        md5 = hashlib.md5()
        md5.update(message_s)
        self.send(message_s, timeout)
        self.send(md5.digest())

    def recv_message(self, timeout=None):
        md5 = hashlib.md5()

        while True:
            try:
                data_sz_s = self.recv(self.size_s.size, timeout)
                break
            except socket.timeout:
                if timeout is not None:
                    raise

        md5.update(data_sz_s)
        data_sz, = self.size_s.unpack(data_sz_s)
        data_s = self.recv(data_sz)
        md5.update(data_s)
        name, args, kwargs = deserialize(data_s)
        digest = md5.digest()
        assert self.recv(len(digest)) == digest
        return name, args, kwargs

    def recv(self, size, timeout=None):
        data = b""
        etime = None if timeout is None else (time.time() + timeout)

        while size != len(data):
            if etime:
                wtime = etime - time.time()
                if wtime < 0:
                    raise socket.timeout("Receive timeout")
                r, _, _ = select.select([self.sock], [], [], wtime)
                if not r:
                    raise socket.timeout("Receive timeout")

            ndata = self.sock.recv(size - len(data))
            if not ndata:
                raise ConnectionClosed(str(self.sock.getpeername()))

            data += ndata

        return data

    def send(self, data, timeout=None):
        if not timeout:
            self.sock.sendall(data)
            return

        etime = time.time() + timeout
        while data:
            tleft = etime - time.time()
            if tleft < 0:
                raise socket.timeout("Send timeout")

            _, w, _ = select.select([], [self.sock], [], tleft)

            if not w:
                raise socket.timeout("Send timeout")

            send_bytes = self.sock.send(data)
            data = data[send_bytes:]


# ----------------------------------  Thread pool ----------------------------------------------------------------------


def worker(iq, oq):
    while True:
        id, func, arg = iq.get()

        if id is None:
            return

        try:
            oq.put((id, func(arg), None, None))
        except Exception as exc:
            tb = traceback.format_exc()
            if getattr(func, 'noraise', False):
                logger.error("Exception during %s: %s.\n%s", func.__name__, exc, tb)
            oq.put((id, str(exc), tb, type(exc).__name__))


class Pool(object):
    def __init__(self, size=32):
        self.tasks_q = queue.Queue()
        self.results_q = queue.Queue()
        self.threads = [threading.Thread(target=worker, args=(self.tasks_q, self.results_q))
                        for _ in range(size)]

        for th in self.threads:
            th.daemon = True
            th.start()

    def map(self, func, vals):
        for pos, val in enumerate(vals):
            self.tasks_q.put((pos, func, val))

        res = []
        while len(res) < len(vals):
            res.append(self.results_q.get())

        return [(msg, tb, exc_cls_name) for _, msg, tb, exc_cls_name in sorted(res)]

    def stop(self):
        for _ in self.threads:
            self.tasks_q.put((None, None, None))

        for th in self.threads:
            th.join()

    def __enter__(self):
        return self

    def __exit__(self, x, y, z):
        self.stop()


# ----------------------------------  agent_module ---------------------------------------------------------------------


class Promote(Exception):
    def __init__(self, msg, tb, cls_name):
        Exception.__init__(self, msg)
        self.tb = tb
        self.cls_name = cls_name


if IS_PYTHON3:
    def tostr(vl):
        if isinstance(vl, bytes):
            return vl.decode('utf8', errors='replace')
        return vl
else:
    def tostr(vl):
        if isinstance(vl, unicode):
            return vl.encode('utf8')
        return vl

def noraise(func):
    func.noraise = True
    return func


class AgentModule:
    pass


agent_module = AgentModule()
agent_module.Pool = Pool
agent_module.noraise = noraise
agent_module.queue = queue
agent_module.BIO = BIO
agent_module.tostr = tostr
agent_module.IS_PYTHON3 = IS_PYTHON3
agent_module.Promote = Promote


sys.modules['agent_module'] = agent_module


class CommonUtils:
    @staticmethod
    def follow_symlink(fname):
        while os.path.islink(fname):
            dev_link_next = os.readlink(fname)
            dev_link_next = os.path.join(os.path.dirname(fname), dev_link_next)
            fname = os.path.abspath(dev_link_next)
        return tostr(fname)


sys.modules['rpc_common'] = CommonUtils

# ----------------------------------  Utils ----------------------------------------------------------------------------


def val_to_str(val, max_str_len=20):
    if isinstance(val, BytesType):
        if len(val) > max_str_len - 2:
            return repr(str(val[:max_str_len - 5]) + "...")
        return repr(val)
    elif isinstance(val, Unicode):
        return val_to_str(val.encode("utf8"), max_str_len)
    elif isinstance(val, list):
        return "[...] * {0}".format(len(val))
    elif isinstance(val, tuple):
        return "(...,) * {0}".format(len(val))
    elif isinstance(val, dict):
        return "{{... => ...}} * {0}".format(len(val))
    elif isinstance(val, set):
        return "{{...}} * {0}".format(len(val))
    else:
        res = repr(val)
        if len(res) > max_str_len:
            res = res[:max_str_len - 3] + "..."
        return res


def format_func_call(name, args, kwargs):
    params = list(map(val_to_str, args))
    params += ["{0}={1}".format(var_name, val_to_str(val)) for var_name, val in sorted(kwargs.items())]
    return "{0}({1})".format(name, ", ".join(params))


def rpc_worker(transport, call_map, thread_id, rpc_queue):
    exit_requested = False
    try:
        while True:
            name, args, kwargs = transport.recv_message()

            if name == "DEBUG":
                logger.info("DEBUG info requested: \n%s", pformat(call_map))
                res = None, None, None
            elif name not in call_map:
                msg = "Name {0!r} is not found in RPC map".format(name)
                res = msg, None, 'NameError'
                logger.error(msg)
            else:
                func = call_map[name]
                fcall = format_func_call(name, args, kwargs)
                try:
                    res = func(*args, **kwargs), None, None
                except Promote as exc:
                    res = exc.message, exc.tb, exc.cls_name
                except Exception as exc:
                    tb = traceback.format_exc()
                    if getattr(func, 'noraise', False):
                        logger.warning("Request %s failed: %s.\n%s", fcall, exc, tb)
                    res = str(exc), tb, type(exc).__name__
                else:
                    logger.debug("%s => %s", fcall, val_to_str(res[0    ]))

            transport.send_message(None, res, {})
    except ConnectionClosed:
        logger.info("Connection to client %s closed", transport.remote_addr)
    except SystemExit:
        transport.send_message(None, [None, None, None], {})
        exit_requested = True
    except Exception:
        logger.exception("During processing data from %s", transport.remote_addr)
    finally:
        transport.close()
    rpc_queue.put((thread_id, not exit_requested))


def find_rpc_funcs(mod_name, dct):
    rpc_prefix = "rpc_"
    rpc_prefix_len = len(rpc_prefix)
    for name, val in dct.items():
        if name.startswith(rpc_prefix):
            # fix globals
            val.func_globals.update(dct)
            yield (mod_name + "." + name[rpc_prefix_len:]), val


def verify_server_options(opts):
    if opts.key_file and not opts.cert_file:
        logger.error("Must pass cert file with key file")
        return False

    if not re.match(r".*:\d+", opts.listen_addr):
        logger.error("Wrong listen addr %r", opts.listen_addr)
        return False

    if ':' not in opts.listen_addr:
        logger.error("Wrong host:port - %r", opts.listen_addr)
        return False

    host, port_s = opts.listen_addr.split(":")
    try:
        port = int(port_s)
    except ValueError:
        logger.error("port is not integer %r", port_s)
        return False

    if 0 > port or port >= 2 ** 16:
        logger.error("Wrong port %s", port)
        return False

    try:
        socket.gethostbyname(host)
    except socket.gaierror:
        logger.error("Can't resolve host %r in listen addr", host)
        return False

    return True


# ----------------------------------  Daemonization --------------------------------------------------------------------


class Daemonizator(object):
    def __init__(self, working_dir, stdout, stderr):
        self.working_dir = working_dir
        self.stdout = stdout
        self.stderr = stderr
        self.wpipe = None

    def two_fork(self):
        try:
            pid = os.fork()
            if pid > 0:
                # return to parent
                return False
        except OSError as e:
            sys.stderr.write("fork #1 failed: {0} ({1})\n".format(e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.setsid()

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from 1st children parent
                # use os._exit to aviod calling atexit functions
                os._exit(0)
        except OSError as e:
            sys.stderr.write("fork #2 failed: {0} ({1})\n".format(e.errno, e.strerror))
            sys.exit(1)

        os.chdir(self.working_dir)
        os.umask(0)
        return True

    def redirect_streams(self):
        # redirect standard file descriptors
        mode = os.O_CREAT | os.O_APPEND | os.O_WRONLY
        if self.stdout == self.stderr:
            stdout_fd = stderr_fd = os.open(self.stdout, mode)
        else:
            stdout_fd = os.open(self.stdout, mode)
            stderr_fd = os.open(self.stderr, mode)

        sys.stdout.flush()
        sys.stderr.flush()

        os.close(sys.stdin.fileno())
        os.close(sys.stdout.fileno())
        os.close(sys.stderr.fileno())

        os.dup2(stdout_fd, sys.stdout.fileno())
        os.dup2(stderr_fd, sys.stderr.fileno())

    def daemonize(self):
        rpipe, self.wpipe = os.pipe()

        if not self.two_fork():
            os.close(self.wpipe)

            data = b""

            while True:
                ndata = os.read(rpipe, 1024)
                if not ndata:
                    break
                data += ndata

            os.close(rpipe)
            try:
                return False, json.loads(data.decode("utf8"))
            except Exception as err:
                return False, {"err": str(err)}

        os.close(rpipe)
        self.redirect_streams()
        return True, None

    def daemon_ready(self, server_data):
        os.write(self.wpipe, json.dumps(server_data).encode("utf8"))
        os.close(self.wpipe)

    def exit_parent(self):
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)


# ---------------------------------  PLUGINS  --------------------------------------------------------------------------

PLUGIN_MODULES = {}
MODULES = []


def load_plugin(fname):
    logger.info("Loading plugin from file %r", fname)
    mod = load_py(fname)
    PLUGIN_MODULES[mod.mod_name] = (mod, mod.__version__)
    methonds = dict(find_rpc_funcs(mod.mod_name, mod.__dict__))
    logger.info("Module %s with methods %s loader", mod.mod_name, ",".join(methonds.keys()))
    return methonds


def load_module(call_map, module_name, module_version, module_content):
    lc = {}
    logger.info("Loading plugin %r", module_name)

    exec(compile(module_content, module_name, 'exec'), globals(), lc)
    new_methods = find_rpc_funcs(module_name, lc)

    call_map.update(new_methods)
    logger.info("New module %s with methods %s loaded", module_name, ",".join(name for name, _ in new_methods))
    PLUGIN_MODULES[module_name] = (lc, module_version)
    logger.info(repr(lc))


def load_module_file(call_map, module_name, module_version, module_content):
    lc = {}
    logger.info("Loading plugin %r", module_name)

    fd, path = tempfile.mkstemp()
    os.write(fd, module_content)
    os.close(fd)
    mod = load_py(path)
    os.unlink(path)

    new_methods = find_rpc_funcs(module_name, mod.__dict__)
    call_map.update(new_methods)

    logger.info("New module %s with methods %s loaded", module_name, ",".join(name for name, _ in new_methods))

    PLUGIN_MODULES[module_name] = (mod.__dict__, module_version)
    MODULES.append(mod)
    logger.info(repr(lc))


def get_calls_info(call_map):
    return list(x.decode("utf8") for x in sorted(list(call_map)))


def list_modules():
    res = []
    for name, (_, version) in PLUGIN_MODULES.items():
        res.append((name, version))
    return res


def stop_server():
    logger.info("Stop requested. Exiting")
    raise SystemExit()


def flush_logs():
    for handler in logger.handlers:
        handler.flush()

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except:
            pass


LOG_FILE = None


def get_logs():
    if LOG_FILE:
        return open(LOG_FILE).read()
    return None


# -------------------------------- RPC SERVER --------------------------------------------------------------------------

def get_log_file(opts):
    if opts.stdout_file is not None:
        return opts.stdout_file

    try:
        return os.devnull
    except AttributeError:
        return '/dev/null'


def get_call_map(opts):
    call_map = {}

    for fname in opts.plugin:
        call_map.update(load_plugin(fname))

    call_map['server.stop'] = stop_server
    call_map['server.rpc_info'] = functools.partial(get_calls_info, call_map)
    call_map['server.load_module'] = functools.partial(load_module_file, call_map)
    call_map['server.list_modules'] = list_modules
    call_map['server.flush_logs'] = flush_logs
    call_map['server.get_logs'] = get_logs

    call_map['sys.time'] = time.time

    return call_map


def server_main(opts):
    global LOG_FILE
    if opts.daemon:
        LOG_FILE = get_log_file(opts)
        daemonizator = Daemonizator(opts.working_dir, LOG_FILE, LOG_FILE)
        is_daemon, daemon_data = daemonizator.daemonize()
        if not is_daemon:
            settings = json.dumps(daemon_data)
            if opts.show_settings == '-':
                print(settings)
            elif opts.show_settings:
                with open(opts.show_settings, "w") as sett_fd:
                    sett_fd.write(settings)
            daemonizator.exit_parent()
    elif opts.show_settings or opts.stdout_file:
        logger.warning("--show-settings and --stdout-file options ignored for non-daemon mode")

    logger.info("Start listening on %s", opts.listen_addr)
    srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        call_map = get_call_map(opts)
        srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host, port = opts.listen_addr.split(":")
        srv_sock.bind((host, int(port)))
        srv_sock.listen(3)

        # signal to parent process, that prepartion done
        if opts.daemon:
            server_data = {"daemon_pid": os.getpid(), "addr": "{}:{}".format(*srv_sock.getsockname())}
            daemonizator.daemon_ready(server_data)

        logger.info("Ready")
        logger.info("Call map = %s", pformat(call_map))

        thread_id = 0
        active_threads = {}
        last_connection_at = time.time()
        res_queue = queue.Queue()
        done = False

        while True:
            # if there a results
            while len(active_threads) >= opts.max_connections or not res_queue.empty():
                th_id, res = res_queue.get()
                if not res:
                    done = True
                active_threads.pop(th_id).join()

            if done:
                break

            r, _, _ = select.select([srv_sock], [], [], 0.01)
            if not r:
                conn_timeout = time.time() - last_connection_at
                if opts.timeout <= conn_timeout and not active_threads:
                    logger.error("No active client for %s. Exiting", conn_timeout)
                    return
                continue

            last_connection_at = time.time()

            sock, fromaddr = srv_sock.accept()
            logger.info("Get connection from %s", fromaddr)

            if opts.key_file:
                sock = ssl.wrap_socket(sock, server_side=True, certfile=opts.cert_file, keyfile=opts.key_file)

            th = threading.Thread(target=rpc_worker, args=(Transport(sock), call_map, thread_id, res_queue))
            th.daemon = True
            th.start()
            active_threads[thread_id] = th
            thread_id += 1
    except Exception:
        logger.exception("")
    finally:
        srv_sock.close()


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

    def __call__(self, *args, **kwargs):
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
            self._tr.send_message(self._name, args, kwargs, timeout=send_timeout)
            name, (res, tb, exc_cls_name), kwargs = self._tr.recv_message(timeout=recv_timeout)

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


def connect(addr, key_file=None, cert_file=None, conn_timeout=5, timeout=None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host, port = addr

    sock.settimeout(conn_timeout)
    sock.connect((host, int(port)))
    sock.settimeout(None)

    if key_file:
        sock = ssl.wrap_socket(sock,
                               server_side=True,
                               certfile=cert_file,
                               keyfile=key_file)

    return SimpleRPCClient(Transport(sock, timeout=timeout))


# ----------------------------------------------  CLI hendlers ---------------------------------------------------------


def parse_type(val):
    if re.match(r"-?\d+", val):
        return int(val)
    return val


def client_main(opts):
    rpc = connect(
        addr=opts.server_addr.split(":"),
        key_file=opts.key_file,
        cert_file=opts.cert_file
    )

    with rpc:
        for part in opts.name.split("."):
            rpc = getattr(rpc, part)

        pprint(rpc(*map(parse_type, opts.params)))


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
    parser.add_argument("--log-level", default="DEBUG",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--log-config", default=None)

    server_parser = subparsers.add_parser('server', help='start a serving daemon')
    server_parser.add_argument("-l", "--listen-addr", required=True)
    server_parser.add_argument("-m", "--max-connections", default=16)

    server_parser.add_argument("-p", "--plugin", action='append', default=[])
    server_parser.add_argument("--timeout", type=int, default=300,
                               help="exit if have no successfull connection in this timeout")
    server_parser.add_argument("-d", "--daemon", action="store_true", help="became a daemon")
    server_parser.add_argument("--stdout-file", default=None)
    server_parser.add_argument("-s", "--show-settings", default=None, nargs='?', const='-',
                               help="dump settings dict after daemonization, not used in other cases")
    server_parser.add_argument("--working-dir", default="/tmp",
                               help="cd to this directory after daemonization, not used in other cases")
    server_parser.add_argument("--id", default="", help="Used only to find a process")
    server_parser.set_defaults(subparser_name="server")

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

    setup_logger(opts)

    if opts.subparser_name == 'server':
        if verify_server_options(opts):
            return server_main(opts)
    elif opts.subparser_name == 'call':
        return client_main(opts)
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
