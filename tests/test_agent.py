import os
import time
import atexit
import asyncio
import threading
import subprocess


from aiohttp import ClientConnectionError


from agent.client import AsyncRPCClient
from agent.server import get_key_enc


# ------------------    HELPERS    -------------------------------------------

test_addr = "localhost:55887"
test_cert = os.path.join(os.path.dirname(__file__), "test_cert.crt")
test_key = os.path.join(os.path.dirname(__file__), "test_key.key")


def spawn_rpc():
    key, enc_key = get_key_enc()

    def closure():
        cmd = "python -m agent.server server --cert {} --key {} --api-key-val {} {}"
        cmd = cmd.format(test_cert, test_key, enc_key, test_addr)
        print(cmd)
        proc = subprocess.Popen(cmd.split())
        atexit.register(proc.kill)

    th = threading.Thread(target=closure, daemon=True)
    th.start()
    return key


async def test():
    # key = spawn_rpc()

    # wait for server to start
    key = "82c4f12be7c307c3a52dfe7c254408bfb1dd5cbf63c421310da0e0dec46d1aeb4a4136b71f400ac8171aded591874f18a1731" + \
        "aa35a98914ffc8a650118af30d5::2888D3730D2D291FA73B0B446F808BC5"

    async with AsyncRPCClient(url="https://" + test_addr, ssl_cert_file=test_cert, access_key=key) as conn:
        await conn.wait_ready(1.0)
        print(await conn.sys.ping('pong'))
        get_fl = conn.streamed.fs.get_file
        async with get_fl(os.path.abspath(test_cert), compress=False) as stream:
            while True:
                data = await stream.readany()
                if data == b'':
                    break
                print(data.decode("utf8"))

loop = asyncio.get_event_loop()
loop.run_until_complete(test())
loop.close()
