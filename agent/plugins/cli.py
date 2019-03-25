from __future__ import print_function

import zlib
import time
import signal
import asyncio
import logging
import functools
import subprocess
from typing import List, Optional


from .. import rpc


expose = functools.partial(rpc.expose_func, "fs")
expose_async = functools.partial(rpc.expose_func_async, "fs")

logger = logging.getLogger("agent.cli")


all_procs = []
last_killall_requested = 0
last_killall_sig = None  # type: Optional[int]


async def async_check_output(cmd: List[str], timeout: int = 30) -> bytes:
    proc = await asyncio.create_subprocess_exec(*cmd,
                                                timeout=timeout,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.STDOUT)
    data = await proc.communicate()
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd=cmd, output=data[0])
    return data[0]


# TODO: make this streaming data from process to caller
# TODO: how to pass exit code back in this case?
@expose_async
async def run_cmd(cmd: List[str], timeout: int = None, input_data: bytes = None):
    logger.info("CMD start requested: %s", cmd)
    t = time.time()
    proc = await asyncio.create_subprocess_exec(*cmd,
                                                timeout=timeout,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.STDOUT,
                                                stdin=None if input_data else asyncio.subprocess.PIPE)
    # there a race between creating of process and killing all processes, fix it
    if t < last_killall_requested:
        assert last_killall_sig is not None
        proc.send_signal(last_killall_sig)
    else:
        all_procs.append(proc)

    data = await proc.stdout.communicate(input_data)
    return [proc.returncode, zlib.compress(data[0])]


@expose
def killall(signal_num: int = signal.SIGKILL):
    logger.info("Signal %s is requested for all procs", signal)

    if signal_num in (signal.SIGKILL, signal.SIGTERM):
        global last_killall_requested
        global last_killall_sig
        last_killall_requested = time.time()
        last_killall_sig = signal_num

    for proc in all_procs:
        try:
            proc.kill(signal_num)
        except:
            pass
