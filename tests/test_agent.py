import asyncio
import os
import tempfile
from io import BytesIO

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