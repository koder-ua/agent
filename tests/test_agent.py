import os
import time
import json
import signal
import socket
import pickle
import tempfile
import threading
import contextlib
import subprocess

import pytest

import agent


# ------------------    HELPERS    -------------------------------------------


class Settings(object):
    pass


def spawn_rpc(transport, call_map):
    th = threading.Thread(target=agent.rpc_master, args=(transport, call_map))
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


@contextlib.contextmanager
def spawn_server(**params):
    fileobj = tempfile.NamedTemporaryFile(prefix="agent_test.")
    log_name = fileobj.name

    default_params = {
        'listen-addr': 'localhost:6677',
        'daemon': None,
        'log-file': log_name,
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
                            default_params.get('cert-file'))
    except Exception:
        os.kill(asett['pid'], signal.SIGKILL)
        print(open(log_name).read())
        os.unlink(log_name)
        raise

    with rpc:
        try:
            yield rpc
        except Exception:
            print(open(log_name).read())
            raise
        finally:
            try:
                rpc.server.stop()
            except Exception as exc:
                print("Failed to stop server: {!s}".format(exc))
                pass
        os.unlink(log_name)

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


def test_rpc_simple(two_tr):
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


def test_real_server_simple():
    expected_methods = {'cli.spawn', 'cli.get_updates',
                        'server.rpc_info', 'server.stop'}
    with spawn_server() as rpc:
        procs = rpc.server.rpc_info()
        assert expected_methods.issubset(set(procs))

        ls_id = rpc.cli.spawn("ls -1a /etc")
        out = ""
        code = None
        err = ""
        while code is None:
            code, new_out, new_err = rpc.cli.get_updates(ls_id)
            out += new_out
            err += new_err
            time.sleep(0.1)

        assert not err

        files = os.listdir("/etc") + ["..", '.', '']
        diff = set(out.split("\n")).symmetric_difference(set(files))
        assert not diff
