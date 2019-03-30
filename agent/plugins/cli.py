import zlib
import time
import signal
import asyncio
import logging
import functools
from typing import Optional, Tuple, List

from .. import rpc
from .. import utils

expose = functools.partial(rpc.expose_func, "fs")
expose_async = functools.partial(rpc.expose_func_async, "fs")

logger = logging.getLogger("agent.cli")


all_procs: List[asyncio.subprocess.Process] = []
last_killall_requested: int = 0
last_killall_sig: Optional[int] = None


# TODO: make this streaming data from process to caller
# TODO: how to pass exit code back in this case?
@expose_async
async def run_cmd(cmd: utils.CmdType,
                  timeout: int = None,
                  input_data: bytes = None,
                  compress: bool = True,
                  merge_err: bool = False,
                  output_to_devnull: bool = False,
                  term_timeout: int = 1) -> Tuple[int, bytes, bytes]:

    start_time = time.time()
    proc, input_data = await utils.start_proc(cmd, input_data, merge_err, output_to_devnull)

    # there a race between creating of process and killing all processes, fix it
    if start_time < last_killall_requested:
        assert last_killall_sig is not None
        proc.send_signal(last_killall_sig)

    all_procs.append(proc)

    _, out, err = await utils.run_proc_timeout(cmd, proc, timeout=timeout,
                                               input_data=input_data, term_timeout=term_timeout)

    if compress:
        out = zlib.compress(out)
        if err is not None:
            err = zlib.compress(err)

    return proc.returncode, out, err


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
            proc.send_signal(signal_num)
        except:
            pass
