import os
import zlib
import asyncio
import threading
import subprocess
import contextlib


from agent.client import AsyncRPCClient, BlockType
from agent.server import get_key_enc


# ------------------    HELPERS    -------------------------------------------

test_addr = "localhost:55887"
test_cert = os.path.join(os.path.dirname(__file__), "test_cert.crt")
test_key = os.path.join(os.path.dirname(__file__), "test_key.key")


@contextlib.contextmanager
def spawn_rpc():
    key, enc_key = get_key_enc()
    proc = []

    def closure():
        cmd = f"python -m agent.server server --cert {test_cert} --key {test_key} --api-key-val {enc_key} --addr {test_addr}"
        proc.append(subprocess.Popen(cmd.split()))

    th = threading.Thread(target=closure, daemon=True)
    th.start()

    try:
        yield key
    finally:
        if proc:
            try:
                proc[0].kill()
            except OSError:
                pass


ALL_FUNCS = []


def test(func):
    ALL_FUNCS.append(func)
    return func


@test
async def test_ping(conn):
    await conn.sys.ping('pong')


@test
async def test_file_transfer(conn):
    fname = os.path.abspath(test_cert)
    async with conn.streamed.fs.get_file(fname, compress=False) as stream:
        data = b""
        async for tp, chunk in stream:
            assert tp == BlockType.binary
            data += chunk
        dt = open(fname, 'rb').read()
        assert data == dt, f"{len(data)} != {len(dt)}"

    async with conn.streamed.fs.get_file(fname, compress=True) as stream:
        data = b""
        async for tp, chunk in stream:
            assert tp == BlockType.binary
            data += chunk
        assert zlib.decompress(data) == open(fname, 'rb').read()


@test
async def test_large_file_transfer(conn):
    fname = "/home/koder/Downloads/ops.tar.gz"
    with open(fname, 'rb') as fd:
        async with conn.streamed.fs.get_file(fname, compress=False) as stream:
            async for tp, chunk in stream:
                assert tp == BlockType.binary
                assert fd.read(len(chunk)) == chunk

        assert fd.read() == b''


def main():
    async def test_runner():
        with spawn_rpc() as key:
            async with AsyncRPCClient(url="https://" + test_addr, ssl_cert_file=test_cert, access_key=key) as conn:
                await conn.wait_ready(10.0)
                for func in ALL_FUNCS:
                    print("Running", func.__name__, "... ", end="")
                    await func(conn)
                    print("OK")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_runner())
    loop.close()

    return 0


if __name__ == "__main__":
    exit(main())