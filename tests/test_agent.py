import asyncio
import json
import os
import subprocess
import tempfile
from io import BytesIO
import distutils.spawn
from typing import Iterator, Tuple, Dict, Any, List

import pytest
from pathlib import Path

from agent import AsyncRPCClient, IAgentRPCNode, ConnectionPool


# ------------------    HELPERS    -------------------------------------------


test_addr = "localhost"
path = Path(__file__).parent
test_cert_path = path / "test_cert.crt"
test_key_path = path / "test_key.key"
api_key = (path / "api_key.raw").open().read()


@pytest.fixture
async def rpc_node():
    conn = AsyncRPCClient(test_addr, test_cert_path, api_key)
    return IAgentRPCNode(test_addr, conn)


@pytest.fixture
async def conn_pool_32():
    return ConnectionPool(access_key=api_key, certificates={test_addr: test_cert_path}, max_conn_per_node=32)


@pytest.fixture
async def conn_pool_2():
    return ConnectionPool(access_key=api_key, certificates={test_addr: test_cert_path}, max_conn_per_node=2)


@pytest.mark.asyncio
async def test_ping(rpc_node: IAgentRPCNode):
    async with rpc_node:
        assert 'pong' == (await rpc_node.conn.sys.ping('pong'))


@pytest.mark.asyncio
async def test_read(rpc_node: IAgentRPCNode):
    async with rpc_node:
        expected_data = test_cert_path.open("rb").read()

        data = await rpc_node.read(test_cert_path, compress=False)
        assert data == expected_data

        data = await rpc_node.read(test_cert_path, compress=True)
        assert data == expected_data

        data = await rpc_node.read(test_cert_path)
        assert data == expected_data

        with test_cert_path.open('rb') as fd:
            async for block in rpc_node.iter_file(test_cert_path, compress=True):
                assert fd.read(len(block)) == block
            assert fd.read() == b''

        with test_cert_path.open('rb') as fd:
            async for block in rpc_node.iter_file(test_cert_path, compress=False):
                assert fd.read(len(block)) == block
            assert fd.read() == b''

        with test_cert_path.open('rb') as fd:
            async for block in rpc_node.iter_file(test_cert_path):
                assert fd.read(len(block)) == block
            assert fd.read() == b''


@pytest.mark.asyncio
async def test_read_large(rpc_node: IAgentRPCNode):
    fname = "/home/koder/Downloads/ops.tar.gz"
    async with rpc_node:
        with open(fname, 'rb') as fd:
            async for chunk in rpc_node.iter_file(fname, compress=False):
                assert fd.read(len(chunk)) == chunk

            assert fd.read() == b''


@pytest.mark.asyncio
async def test_write(rpc_node: IAgentRPCNode):
    data = b'-' * 100_000
    async with rpc_node:
        with tempfile.NamedTemporaryFile() as fl:
            await rpc_node.write(fl.name, data, compress=False)
            assert data == fl.file.read()

        assert not Path(fl.name).exists()

        with tempfile.NamedTemporaryFile() as fl:
            await rpc_node.write(fl.name, data, compress=True)
            assert data == fl.file.read()

        assert not Path(fl.name).exists()

        with tempfile.NamedTemporaryFile() as fl:
            await rpc_node.write(fl.name, data)
            assert data == fl.file.read()

        assert not Path(fl.name).exists()

        with tempfile.NamedTemporaryFile() as src:
            src.file.write(data)
            src.file.seek(0, os.SEEK_SET)
            with tempfile.NamedTemporaryFile() as dst:
                await rpc_node.write(dst.name, src.file)
                assert data == dst.file.read()

        assert not Path(dst.name).exists()
        assert not Path(src.name).exists()

        with tempfile.NamedTemporaryFile() as dst:
            await rpc_node.write(dst.name, BytesIO(data))
            assert data == dst.file.read()

        assert not Path(dst.name).exists()


@pytest.mark.asyncio
async def test_write_temp(rpc_node: IAgentRPCNode):
    data = b'-' * 100_000
    tmpdirlist = os.listdir('/tmp')

    async with rpc_node:
        fpath = await rpc_node.write_tmp(data, compress=False)

    assert fpath.open('rb').read() == data
    assert str(fpath.parent) == '/tmp'
    assert fpath.name not in tmpdirlist
    fpath.unlink()


@pytest.mark.asyncio
async def test_write_large(rpc_node: IAgentRPCNode):
    fname = "/home/koder/Downloads/ops.tar.gz"
    async with rpc_node:
        with open(fname, 'rb') as src:
            with tempfile.NamedTemporaryFile() as dst:
                await rpc_node.write(dst.name, src, compress=False)

                chunk = 1 << 20
                src.seek(0, os.SEEK_SET)

                while True:
                    data = src.read(chunk)
                    assert data == dst.file.read(chunk)
                    if not data:
                        break


@pytest.mark.asyncio
async def test_fs_utils(rpc_node: IAgentRPCNode):
    exists = '/'
    not_exists = '/this_folder_does_not_exists'
    assert Path(exists).exists()
    assert not Path(not_exists).exists()

    async with rpc_node:
        assert await rpc_node.exists(exists)
        assert not (await rpc_node.exists(not_exists))

        assert await rpc_node.exists(Path(exists))
        assert not (await rpc_node.exists(Path(not_exists)))

        assert list(await rpc_node.stat(exists)) == list(os.stat(exists))
        assert sorted(list(await rpc_node.iterdir(exists))) == sorted(list(Path(exists).iterdir()))

        assert list(await rpc_node.stat(exists)) == list(os.stat(exists))
        assert sorted(list(await rpc_node.iterdir(exists))) == sorted(list(Path(exists).iterdir()))


@pytest.mark.asyncio
async def test_lsdir_parallel(conn_pool_32: ConnectionPool):
    folders = ['/usr/bin', '/usr/lib', '/usr/local/lib', '/bin', '/sbin', '/run', '/var/lib', '/var/run',
               '/etc', '/boot', '/lib']

    async with conn_pool_32:
        async def loader(path: str, loops: int = 100):
            expected = sorted(list(Path(path).iterdir()))
            async with conn_pool_32.connection(test_addr) as conn:
                for _ in range(loops):
                    assert sorted(list(await conn.iterdir(path))) == expected

        await asyncio.gather(*map(loader, folders))


@pytest.mark.asyncio
async def test_lsdir_parallel_max2(conn_pool_2: ConnectionPool):
    folders = ['/usr/bin', '/usr/lib', '/usr/local/lib', '/bin', '/bin']

    curr_count = 0
    max_count = 0

    async with conn_pool_2:
        async def loader(path: str, loops: int = 10):
            expected = sorted(list(Path(path).iterdir()))
            async with conn_pool_2.connection(test_addr) as conn:
                nonlocal max_count
                nonlocal curr_count
                curr_count += 1
                max_count = max(max_count, curr_count)
                for _ in range(loops):
                    assert sorted(list(await conn.iterdir(path))) == expected
                curr_count -= 1

        await asyncio.gather(*map(loader, folders))

    assert max_count == 2


@pytest.mark.asyncio
async def test_run(rpc_node: IAgentRPCNode):
    expected_res = subprocess.run('ls -1 /', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert expected_res.returncode == 0

    async with rpc_node:
        for cmd in ['ls -1 /', ['ls', '-1', '/'], ['ls', '-1', Path('/')]]:
            ps_result = await rpc_node.run(cmd)
            assert ps_result.returncode == 0
            ps_result.check_returncode()
            assert ps_result.stderr_b is None
            assert ps_result.stdout_b.decode() == ps_result.stdout
            assert ps_result.args == cmd
            assert expected_res.stdout.decode() == ps_result.stdout

        ps_result = await rpc_node.run("ls -1 /", compress=False)
        assert ps_result.returncode == 0
        ps_result.check_returncode()
        assert ps_result.stderr_b is None
        assert ps_result.stdout_b.decode() == ps_result.stdout
        assert ps_result.args == "ls -1 /"
        assert expected_res.stdout.decode() == ps_result.stdout


@pytest.mark.asyncio
async def test_run_issues(rpc_node: IAgentRPCNode):
    not_existed_exe = 'this-cmd-does-not-exists-1241414515415'
    fails_with_code_1 = 'exit 1'
    async with rpc_node:
        with pytest.raises(FileNotFoundError):
            await rpc_node.run([not_existed_exe])

        ps_result = await rpc_node.run(not_existed_exe)
        assert ps_result.returncode != 0

        ps_result = await rpc_node.run(fails_with_code_1)
        assert ps_result.returncode == 1


@pytest.mark.asyncio
async def test_run_input(rpc_node: IAgentRPCNode):
    expected_res = subprocess.run('ls -1 /', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert expected_res.returncode == 0
    cmd = 'ls -1 /'

    async with rpc_node:
        for proc in ["bash", ["bash"]]:
            ps_result = await rpc_node.run(proc, input_data=cmd.encode())
            assert ps_result.returncode == 0
            assert ps_result.stderr_b is None
            assert ps_result.args == proc
            assert expected_res.stdout.decode() == ps_result.stdout


@pytest.mark.asyncio
async def test_run_timeout(rpc_node: IAgentRPCNode):
    async with rpc_node:
        for proc in [["sleep", "3600"], "sleep 3600"]:
            coro = rpc_node.run(proc, timeout=0.5, term_timeout=0.1)
            done, not_done = await asyncio.wait([coro], timeout=2)
            assert done
            task, = done
            with pytest.raises(subprocess.TimeoutExpired):
                await task


@pytest.mark.asyncio
async def test_python27(rpc_node: IAgentRPCNode):
    async with rpc_node:
        for proc in [["python2.7", "-c", "import sys; print sys.version_info"],
                     "python2.7 -c 'import sys; print sys.version_info'"]:

            ps_result = await rpc_node.run(proc)
            assert ps_result.returncode == 0
            assert ps_result.stderr_b is None
            assert ps_result.args == proc
            assert "sys.version_info(major=2, minor=7, " in ps_result.stdout


@pytest.mark.asyncio
async def test_run_environ(rpc_node: IAgentRPCNode):
    var_name = "TEST_VAR"
    var_val = "TEST_VALUE"
    async with rpc_node:
        env = await rpc_node.conn.cli.environ()

        assert var_name not in env
        assert 'PATH' in env

        for proc in [["python2.7", "-c", "import os; print os.environ['TEST_VAR']"]]:
            ps_result = await rpc_node.run(proc, merge_err=False)
            assert ps_result.returncode != 0
            assert "KeyError: 'TEST_VAR'" in ps_result.stderr_b.decode()

        new_env = env.copy()
        new_env[var_name] = var_val
        for proc in [["python2.7", "-c", "import os; print os.environ['TEST_VAR']"], "echo $TEST_VAR"]:
            ps_result = await rpc_node.run(proc, env=new_env, merge_err=False)
            assert ps_result.returncode == 0
            assert var_val == ps_result.stdout.strip()


def get_mounts() -> Dict[str, Tuple[str, str]]:
    lsblk = json.loads(subprocess.check_output("lsblk --json", shell=True))

    def iter_mounts(curr: List[Dict[str, Any]], parent_device: str = None) -> Iterator[Tuple[str, str, str]]:
        for dev_info in curr:
            name = parent_device if parent_device else dev_info["name"]
            if dev_info.get("mountpoint") is not None:
                yield name, dev_info["name"], dev_info["mountpoint"]

            if 'children' in dev_info:
                yield from iter_mounts(dev_info['children'], name)

    return {mp: ('/dev/' + dev,  '/dev/' + partition) for dev, partition, mp in iter_mounts(lsblk["blockdevices"])}


def find_mount_point(path: Path) -> Path:
    path = path.resolve()
    while not os.path.ismount(path):
        assert str(path) != '/'
        path = path.parent
    return path


@pytest.mark.asyncio
async def test_other_fs(rpc_node: IAgentRPCNode):
    mounts = get_mounts()

    async with rpc_node:
        with tempfile.NamedTemporaryFile() as fl:
            expected_device, expected_partition = mounts[str(find_mount_point(Path(fl.name)))]
            device, partition = await rpc_node.conn.fs.get_dev_and_partition(fl.name)
            assert Path(device).is_block_device()
            assert Path(partition).is_block_device()
            assert expected_partition == partition
            assert expected_device == device

            assert len(await rpc_node.conn.fs.find_pids_for_cmd('zsh', find_binary=True)) > 0
            assert (await rpc_node.conn.fs.count_sockets_for_process(os.getpid())) > 0

            for dev_name in (await rpc_node.conn.fs.get_block_devs_info()):
                assert Path(f"/dev/{dev_name}").is_block_device()
