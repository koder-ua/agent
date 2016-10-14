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
import select
import pickle
import socket
import struct
import hashlib
import logging
import tempfile
import argparse
import functools
import threading
import subprocess
from pprint import pprint, pformat

try:
    import Queue as queue
except ImportError:
    import queue


# ----------------------------- LOGGING --------------------------------------------------------------------------------

logger = logging.getLogger('agent')


def setup_logger(opts):
    if opts.log_config:
        logging.config.fileConfig(opts.log_config)
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


if sys.version_info < (3, 0):
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


# ----------------------------- PROC CONTROL ---------------------------------------------------------------------------


class Proc(object):
    "Background process class"

    STDOUT = 0
    STDERR = 1
    EXIT_CODE = 2
    term_timeout = 1
    kill_timeout = 1

    RUNNING = 0
    TERM_SEND = 1
    KILL_SEND = 2

    def __init__(self, cmd, timeout, input_data=None, merge_out=True):
        self.input_data = input_data
        self.cmd = cmd
        self.timeout = timeout
        self.merge_out = merge_out

        self.proc = None
        self.end_time = None
        self.input_file = None
        self.output_q = None
        self.state = None

    def spawn(self):
        if self.input_data:
            self.input_file = tempfile.TemporaryFile(prefix="inp_data")

        if self.merge_out:
            stderr = subprocess.STDOUT
        else:
            stderr = subprocess.PIPE

        self.proc = subprocess.Popen(self.cmd,
                                     shell=isinstance(self.cmd, basestring),
                                     stdout=subprocess.PIPE,
                                     stderr=stderr,
                                     stdin=self.input_file)
        self.state = self.RUNNING
        self.output_q = queue.Queue()

        if self.timeout:
            self.end_time = time.time() + self.timeout

        if self.input_data:
            self.input_file.write(self.input_data)
            self.input_file.close()

        watch_th = threading.Thread(target=self.watch_proc_th)
        watch_th.daemon = True
        watch_th.start()

    def on_timeout(self):
        if self.state == self.RUNNING:
            self.term()
            self.end_time = time.time() + self.term_timeout
        elif self.state == self.TERM_SEND:
            self.kill()
            self.end_time = time.time() + self.kill_timeout
        else:
            assert self.state == self.KILL_SEND
            raise RuntimeError("Can't kill process")

    def watch_proc_th(self):
        if self.merge_out:
            all_pipes = [self.proc.stdout]
        else:
            all_pipes = [self.proc.stdout, self.proc.stderr]

        # set non-blocking

        while all_pipes:
            if self.end_time is not None:
                timeout = self.end_time - time.time()
            else:
                timeout = None

            r, _, e = select.select(all_pipes, [], all_pipes, timeout)
            if e != []:
                pass

            if not r:
                self.on_timeout()

            for pipe in r:
                data = pipe.read(1024)
                if len(data) == 0:
                    all_pipes.remove(pipe)
                    pipe.close()
                code = self.STDOUT if pipe is self.proc.stdout else self.STDERR
                self.output_q.put((code, data))

        if self.end_time is not None:
            self.proc.wait()
        else:
            self.proc.poll()
            while self.proc.returncode is None:
                while time.time() < self.end_time:
                    if self.proc.poll() is not None:
                        break
                    time.sleep(0.1)
                self.timeout()

        self.output_q.put((self.EXIT_CODE, self.proc.returncode))

    def get_updates(self):
        stdout_data = ""
        stderr_data = ""
        code = None
        while not self.output_q.empty():
            code, data = self.output_q.get()
            if code == self.STDOUT:
                stdout_data += data
            elif code == self.STDERR:
                stderr_data += data
            else:
                code = data
        return code, stdout_data, stderr_data

    def term(self):
        self.proc.terminate()

    def kill(self):
        self.proc.kill()


# ---------------------------------------------  TRANSPORT PROTO -------------------------------------------------------


class RawData(object):
    def __init__(self, data=None, idx=None):
        self.data = data
        self.idx = idx


class ConnectionClosed(Exception):
    pass


class Transport(object):
    TIMEOUT = 30
    size_s = struct.Struct("!I")
    min_blob_size = 64

    def __init__(self, sock, timeout=None):
        self.sock = sock
        if timeout is None:
            self.sock.settimeout(self.TIMEOUT)
        else:
            self.sock.settimeout(timeout)

    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
        self.sock = None

    def send_message(self, name, args, kwargs, timeout=None):
        blobs = []
        new_args = []
        for obj in args:
            if isinstance(obj, (str, bytes)) and len(obj) >= self.min_blob_size:
                robj = RawData(None, len(blobs))
                blobs.append(obj)
                obj = robj
            new_args.append(obj)

        new_kwargs = {}
        for param_name, obj in kwargs.items():
            if isinstance(obj, (str, bytes)) and len(obj) >= self.min_blob_size:
                robj = RawData(None, len(blobs))
                blobs.append(obj)
                obj = robj
            new_kwargs[param_name] = obj

        message = [map(len, blobs), name, tuple(new_args), new_kwargs]
        message_s = pickle.dumps(message)
        message_s = self.size_s.pack(len(message_s)) + message_s

        md5 = hashlib.md5()
        md5.update(message_s)

        self.send(message_s, timeout)

        for blob in blobs:
            md5.update(blob)
            self.send(blob)

        self.send(md5.digest())

    def recv_message(self, timeout=None):
        md5 = hashlib.md5()

        data_sz_s = self.recv(self.size_s.size, timeout)
        md5.update(data_sz_s)
        data_sz, = self.size_s.unpack(data_sz_s)

        data_s = self.recv(data_sz)
        md5.update(data_s)
        blobs_lens, name, args, kwargs = pickle.loads(data_s)

        blobs = []
        for clen in blobs_lens:
            blobs.append(self.recv(clen))
            md5.update(blobs[-1])
        digest = md5.digest()

        new_args = []
        for obj in args:
            if isinstance(obj, RawData):
                obj = blobs[obj.idx]
            new_args.append(obj)

        new_kwargs = {}
        for param_name, obj in kwargs.items():
            if isinstance(obj, RawData):
                obj = blobs[obj.idx]
            new_kwargs[param_name] = obj

        exp_digest = self.recv(len(digest))
        assert exp_digest == digest

        return name, tuple(new_args), new_kwargs

    def recv(self, size, timeout=None):
        data = b""

        if timeout:
            r, _, _ = select.select([self.sock], [], [], timeout)
            if not r:
                raise socket.timeout("Recv start timeout")

        while size != len(data):
            ndata = self.sock.recv(size - len(data))
            if not ndata:
                raise ConnectionClosed()
            data += ndata

        return data

    def send(self, data, timeout=None):
        if not timeout:
            self.sock.sendall(data)
            return

        etime = time.time() + timeout
        while True:
            tleft = etime - time.time()
            _, w, _ = select.select([], [self.sock], [], tleft)
            if not w:
                raise socket.timeout("Send timeout")

            send_bytes = self.sock.send(data)
            data = data[send_bytes:]


# -----------------------------------------------------


def val_to_str(val, max_str_len=20):
    if isinstance(val, basestring):
        if len(val) > max_str_len:
            return repr(str(val[:max_str_len - 3]) + "...")
        return repr(val)
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
    params += ["{0}={1}".format(var_name, val_to_str(val))
               for var_name, val in sorted(kwargs.items())]
    return "{0}({1})".format(name, ", ".join(params))


def rpc_master(transport, call_map, thid, queue):
    res = True
    try:
        while True:
            name, args, kwargs = transport.recv_message()
            logger.info("RPC request " + format_func_call(name, args, kwargs))

            if name not in call_map:
                res = False, NameError(name)
                logger.error("Unknown name {0!r}".format(name))
            else:
                try:
                    res = True, call_map[name](*args, **kwargs)
                except Exception as exc:
                    res = False, exc

            logger.info("Done, sending responce: " + val_to_str(res[1]))
            transport.send_message(None, res, {})
    except ConnectionClosed:
        logger.info("Connection closed")
    except SystemExit:
        transport.send_message(None, [True, None], {})
        res = False
    except Exception:
        logger.exception("During processing client")
    finally:
        transport.close()
    queue.put((thid, res))


def find_rpc_funcs(dct):
    rpc_prefix = "rpc_"
    rpc_prefix_len = len(rpc_prefix)
    mod_name = dct['RPC_MODULE']
    for name, val in dct.items():
        if name.startswith(rpc_prefix):
            yield (mod_name + "." + name[rpc_prefix_len:]), val


def load_plugin(fname):
    mod = load_py(fname)
    del mod.__dict__['__builtins__']
    return dict(find_rpc_funcs(mod.__dict__))


def validate_server_options(opts):
    if opts.key_file and not opts.cert_file:
        logger.error("Must pass cert file with key file")
        return False

    if not re.match(r".*:\d+", opts.listen_addr):
        logger.error("Wrong listen addr")
        return False

    host, port = opts.listen_addr.split(":")
    port = int(port)

    if port > (2 ** 16 - 1):
        logger.error("Wrong port")
        return False

    try:
        socket.gethostbyname(host)
    except socket.gaierror:
        logger.error("Can't resolve host in listen addr")
        return False

    return True


# ----------------------------------  Daemonization ---------------------------------------------------------------------

class Daemonizator(object):
    def __init__(self, working_dir="/tmp", stdout=None, stderr=None):
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
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
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
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        os.chdir(self.working_dir)
        os.umask(0)
        return True

    def redirect_streams(self):
        # redirect standard file descriptors
        mode = os.O_CREAT | os.O_APPEND
        if self.stdout == self.stderr:
            out_fd = err_fd = os.open(self.stdout, mode)
        else:
            out_fd = os.open(self.stdout, mode)
            err_fd = os.open(self.stderr, mode)

        sys.stdout.flush()
        sys.stderr.flush()

        os.close(sys.stdin.fileno())
        os.close(sys.stdout.fileno())
        os.close(sys.stderr.fileno())

        os.dup2(out_fd, sys.stdout.fileno())
        os.dup2(err_fd, sys.stderr.fileno())

    def daemonize(self):
        rpipe, self.wpipe = os.pipe()

        is_daemon = self.two_fork()

        if not is_daemon:
            os.close(self.wpipe)
            # read untill stream would be closed
            sz = struct.calcsize("I") + 1
            (dpid,) = struct.unpack("I", os.read(rpipe, sz))
            os.close(rpipe)
            return dpid

        os.close(rpipe)
        self.redirect_streams()
        os.write(self.wpipe, struct.pack("I", os.getpid()))
        return

    def daemon_ready(self):
        os.close(self.wpipe)

    def exit_parent(self):
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)


# ---------------------------------  BUILD-IN RPC METHODS  -------------------------------------------------------------


procs_lock = threading.Lock()
proc_id = 0
procs = {}


def spawn(cmd, timeout=None, input_data=None):
    global proc_id

    proc = Proc(cmd, timeout, input_data)
    proc.spawn()

    with procs_lock:
        curr_id = proc_id
        proc_id += 1
        procs[curr_id] = proc

    return curr_id


def get_updates(proc_id):
    with procs_lock:
        proc = procs[proc_id]

    ecode, d_out, d_err = proc.get_updates()

    if ecode is not None:
        with procs_lock:
            del procs[proc_id]
    return ecode, d_out, d_err


def load_module(call_map, module_name, module_content):
    lc = {}
    eval(compile(module_content, module_name, 'exec'), globals(), lc)
    call_map.update(find_rpc_funcs(lc))
    logger.info("New module {!r} with methods {} loaded".format(
        module_name, ",".join(lc.keys())))


def load_module_fc(call_map, module_path):
    new_methods = load_plugin(module_path)
    call_map.update(new_methods)
    logger.info("New module from {!r} with methods {} loaded".format(
        module_path, ",".join(new_methods.keys())))


def get_calls_info(call_map):
    return list(sorted(list(call_map)))


def stop_server():
    logger.info("Stop requested. Exiting")
    raise SystemExit()


def get_file(path):
    return open(path, "rb").read()


def store_file(path, content):
    with open(path, "wb") as fd:
        fd.write(content)


def file_exists(path):
    return os.path.exists(path)

# -------------------------------- RPC SERVER --------------------------------------------------------------------------


def get_log_file(opts):
    if opts.stdout_file is None:
        if hasattr(os, 'devnull'):
            return os.devnull
        else:
            return '/dev/null'
    else:
        return opts.stdout_file


def get_call_map(opts):
    call_map = {}

    for fname in opts.plugin:
        call_map.update(load_plugin(fname))

    call_map['server.stop'] = stop_server
    call_map['server.rpc_info'] = functools.partial(get_calls_info, call_map)
    call_map['server.load_module'] = functools.partial(load_module, call_map)
    call_map['server.load_module_fl'] = functools.partial(load_module, call_map)

    call_map['cli.spawn'] = spawn
    call_map['cli.get_updates'] = get_updates

    call_map['fs.get_file'] = get_file
    call_map['fs.store_file'] = store_file
    call_map['fs.file_exists'] = file_exists

    return call_map


def server_main(opts):
    if opts.daemon:
        log_file = get_log_file(opts)
        daemonizator = Daemonizator(opts.working_dir, log_file, log_file)
        daemon_pid = daemonizator.daemonize()
        if daemon_pid is not None:
            settings = json.dumps({"daemon_pid": daemon_pid})
            if opts.show_settings == '-':
                print(settings)
            elif opts.show_settings:
                with open(opts.show_settings, "w") as sett_fd:
                    sett_fd.write(settings)
            daemonizator.exit_parent()
    elif opts.show_settings:
        logger.warning("--show-settings option ignored for non-daemon mode")

    logger.info("Start listening on {0}".format(opts.listen_addr))
    srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        call_map = get_call_map(opts)

        srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        host, port = opts.listen_addr.split(":")
        srv_sock.bind((host, int(port)))
        srv_sock.listen(3)

        # signal to parent process, that prepartion done
        if opts.daemon:
            daemonizator.daemon_ready()

        logger.info("Ready")
        logger.debug(pformat(call_map.keys()))

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
                    logger.info("Communication timeout. Exiting")
                    return
                continue

            last_connection_at = time.time()

            sock, fromaddr = srv_sock.accept()
            logger.info("Get connection from {0}".format(fromaddr))
            if opts.key_file:
                sock = ssl.wrap_socket(sock,
                                       server_side=True,
                                       certfile=opts.cert_file,
                                       keyfile=opts.key_file)

            th = threading.Thread(target=rpc_master,
                                  args=(Transport(sock), call_map, thread_id, res_queue))
            th.daemon = True
            th.start()
            active_threads[thread_id] = th
            thread_id += 1
    except Exception:
        logger.exception("")
    finally:
        srv_sock.close()


# -------------------------------   API  -------------------------------------------------------------------------------


class SimpleRPCClient(object):
    def __init__(self, transport, name=None):
        self._tr = transport
        self._name = name

    def __call__(self, *args, **kwargs):
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

        self._tr.send_message(self._name, args, kwargs, timeout=send_timeout)
        name, (ok, res), kwargs = self._tr.recv_message(timeout=recv_timeout)
        assert name is None
        assert kwargs == {}

        if ok:
            return res

        assert isinstance(res, Exception)
        raise res

    def __getattr__(self, name):
        if self._name is not None:
            name = self._name + '.' + name
        return self.__class__(self._tr, name)

    def __enter__(self):
        return self

    def __exit__(self, x, y, z):
        self._tr.close()
        self._tr = None


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
        if validate_server_options(opts):
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
