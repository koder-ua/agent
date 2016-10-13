import os
import time
import json
import signal
import socket
import shutil
import pickle
import tempfile
import threading
import contextlib
import subprocess
from concurrent.futures import ThreadPoolExecutor

try:
    import Queue as queue
except ImportError:
    import queue

import pytest

from agent import agent
import plugin1

# ------------------    HELPERS    -------------------------------------------


class Settings(object):
    pass


def spawn_rpc(transport, call_map):
    th = threading.Thread(target=agent.rpc_master,
                          args=(transport, call_map,
                                0, queue.Queue()))
    th.daemon = True
    th.start()
    return th


@pytest.fixture()
def settings():
    sett = Settings()
    sett.log_file = '/dev/null'
    sett.daemon = False
    sett.listen_addr = "localhost:6666"
    sett.working_dir = '/tmp'
    sett.timeout = 300
    return sett


class SocketMock(object):
    def __init__(self):
        self.buffer = ""
        self.recv_offset = 0

    def settimeout(self, tout):
        pass

    def recv(self, size):
        if len(self.buffer) - self.recv_offset >= size:
            frm = self.recv_offset
            self.recv_offset += size
            return self.buffer[frm:self.recv_offset]
        raise socket.timeout("timed out")

    def sendall(self, data):
        self.buffer += data


@pytest.fixture()
def two_tr():
    s1, s2 = socket.socketpair()
    return agent.Transport(s1), agent.Transport(s2)


def setup_module(module):
    class LSett(object):
        log_config = None
        log_level = "DEBUG"

    agent.setup_logger(LSett)


def exit_func():
    raise SystemExit()


def pong(*args, **kwargs):
    return args, kwargs


def plen(*args, **kwargs):
    return len(args) + len(kwargs)


def add(x, y):
    return x + y


def raise_exc(param):
    raise Exception(param)


@contextlib.contextmanager
def setup_rpc(rpc_map=None):
    tr1, tr2 = two_tr()
    rpc = agent.SimpleRPCClient(tr1)

    if rpc_map is None:
        rpc_map = {
            'raise_exc': raise_exc,
            'add': add,
            'pong': pong,
            'plen': plen,
        }

    rpc_map['_exit_'] = exit_func

    th = spawn_rpc(tr2, rpc_map)

    try:
        yield rpc
    finally:
        rpc._exit_()
        th.join()


DEFAULT_BIND_ADDR = 'localhost:6677'


@contextlib.contextmanager
def spawn_server(**params):
    with tempfile.NamedTemporaryFile(prefix="agent_test.") as fileobj:
        log_name = fileobj.name

        default_params = {
            'listen-addr': DEFAULT_BIND_ADDR,
            'daemon': None,
            'stdout-file': log_name,
            'show-settings': None
        }

        if agent.__file__.endswith(".pyc"):
            agent_path = agent.__file__[:-1]
        else:
            agent_path = agent.__file__

        default_params.update(params)

        cli_params = " ".join(
            ("'--" + k.replace("_", '-') + ("=" + v if v else "") + "'")
            for k, v in default_params.items()
        )

        agent_cmd = "python {} server {}".format(agent_path, cli_params)
        jsettings = subprocess.check_output(agent_cmd, shell=True)
        asett = json.loads(jsettings)

        try:
            addr = default_params['listen-addr'].split(":")
            rpc = agent.connect(addr,
                                default_params.get('key-file'),
                                default_params.get('cert-file'),
                                timeout=1)
        except Exception:
            os.kill(asett['pid'], signal.SIGKILL)
            print(open(log_name).read())
            raise

        try:
            with rpc:
                try:
                    yield rpc
                finally:
                    try:
                        rpc.server.stop()
                    except Exception as exc:
                        print("Failed to stop server: {!s}".format(exc))
        finally:
            print(open(log_name).read())
            shutil.copyfile(log_name, "/tmp/last.log")

# ------------------    TESTS    ---------------------------------------------


def test_transport_simple(two_tr):
    tr1, tr2 = two_tr

    name = "xxxx"
    args = (1, 2, [3, 4])
    kwargs = {"a": 12, "b": [12, "3"]}

    tr1.send_message(name, args, kwargs)
    name1, args1, kwargs1 = tr2.recv_message()

    assert name == name1
    assert args == args1
    assert kwargs == kwargs1

    for i in range(8):
        tr1.send_message(name, args, kwargs)

    for i in range(8):
        name1, args1, kwargs1 = tr2.recv_message()

        assert name == name1
        assert args == args1
        assert kwargs == kwargs1


def test_transport_blob():
    sock = SocketMock()
    tr = agent.Transport(sock)

    data = "\x00" * 1000000
    name = "xxxx"
    args = (data, 2, [3, 4])
    kwargs = {"a": data, "b": [12, "3"]}

    tr.send_message(name, args, kwargs)
    s1 = len(sock.buffer)
    name1, args1, kwargs1 = tr.recv_message()

    assert name1 == name
    assert args1 == args
    assert kwargs1 == kwargs
    assert s1 <= len(data) * 2.2
    assert s1 <= len(pickle.dumps(data)) * 2


def test_rpc_simple():
    with setup_rpc() as rpc:
        assert ((1, 2), {}) == rpc.pong(1, 2)
        assert ((None,), {}) == rpc.pong(None)
        assert (({1: 2, 3: 4, 5: [1]},), {}) == rpc.pong({1: 2, 3: 4, 5: [1]})
        assert (tuple(), {"a": 12, 'x': "23"}) == rpc.pong(a=12, x="23")

        with pytest.raises(Exception) as exc:
            rpc.raise_exc("12")

        assert '12' in str(exc)


def test_rpc_errors():
    with setup_rpc() as rpc:
        with pytest.raises(NameError):
            rpc.unknown_func()

        with pytest.raises(NameError):
            rpc.unknown_namespace.unknown_func(12)

        assert 3 == rpc.add(1, 2)

        with pytest.raises(TypeError):
            rpc.add(1, 2, 3)

        with pytest.raises(TypeError):
            rpc.add(1, "2")

        with pytest.raises(TypeError):
            rpc.add(1)

        with pytest.raises(TypeError):
            rpc.add(1, t=12)


def test_rpc_namespaces(two_tr):
    rpc_map = {
        'sys.raise_exc': raise_exc,
        'x.y.pong': pong,
        'x.y.z.pong': plen
    }
    with setup_rpc(rpc_map) as rpc:
        assert ((1, 2), {}) == rpc.x.y.pong(1, 2)
        assert 2 == rpc.x.y.z.pong(1, 2)

        with pytest.raises(Exception):
            rpc.sys.raise_exc("12")


def check_ls_in_folder(rpc, folder):
    ls_id = rpc.cli.spawn("ls -1a {}".format(folder))
    out = ""
    code = None
    while code is None:
        code, new_out, err = rpc.cli.get_updates(ls_id)
        out += new_out
        assert not err
        time.sleep(0.1)

    assert not err

    files = os.listdir(folder) + ["..", '.', '']
    diff = set(out.split("\n")).symmetric_difference(set(files))
    assert not diff


def test_real_server_simple():
    expected_methods = {'cli.spawn', 'cli.get_updates',
                        'server.rpc_info', 'server.stop'}
    with spawn_server() as rpc:
        procs = rpc.server.rpc_info()
        assert expected_methods.issubset(set(procs))
        check_ls_in_folder(rpc, "/etc")


def test_real_server_long_run():
    max_seq = 3
    with spawn_server() as rpc:
        sleep_id = rpc.cli.spawn("for id in `seq 1 {}` ; do echo $id ; sleep 1 ; done".format(max_seq))
        out = ""
        time.sleep(1)
        code, new_out, err = rpc.cli.get_updates(sleep_id)
        assert not err
        assert code is None
        out += new_out
        time.sleep(max_seq)

        code, new_out, err = rpc.cli.get_updates(sleep_id)
        assert not err
        assert code == 0
        output = [int(i.strip()) for i in (out + new_out).split() if i.strip()]
        output.sort()
        assert list(range(1, max_seq + 1)) == output


def test_real_server_many_procs():
    num_proc = 100

    with spawn_server() as rpc:
        id_pids = {idx: rpc.cli.spawn("sleep 2; echo {}".format(idx))
                   for idx in range(num_proc)}

        for counter in range(4):
            new_ids = []
            for idx, proc_id in id_pids.items():
                code, out, err = rpc.cli.get_updates(proc_id)
                if code is not None:
                    assert idx == int(out.strip())
                    new_ids.append(idx)
                    assert code == 0
                    assert not err

            for idx in new_ids:
                del id_pids[idx]

            if not id_pids:
                break

            time.sleep(1)
        else:
            assert False, "Timeout"


def load_thread(addr, folder, count):
    with agent.connect(addr) as rpc:
        for i in range(count):
            check_ls_in_folder(rpc, folder)


def test_real_server_concurrent():
    folders = []
    count = 32

    for name in os.listdir("/usr"):
        fpath = os.path.join("/usr", name)
        if os.path.isdir(fpath):
            folders.append(fpath)

    max_conn = min(len(folders), 32)
    host, port = DEFAULT_BIND_ADDR.split(":")

    with spawn_server(max_connections="33"):
        with ThreadPoolExecutor(max_conn) as executor:
            futures = [executor.submit(load_thread, (host, port), folder, count)
                       for folder in folders]

            for fut in futures:
                fut.result()


def test_real_server_plugins():
    plugin_path = plugin1.__file__.replace(".pyc", ".py")
    with spawn_server(plugin=plugin_path) as rpc:
        assert rpc.pl1.add(1, 2) == 3
