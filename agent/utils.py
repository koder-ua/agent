import asyncio
import logging
import subprocess
from pathlib import Path
from typing.io import BinaryIO
from typing import Union, Sequence, NamedTuple, Optional, Tuple

logger = logging.getLogger("agent")


CmdType = Union[str, Sequence[Union[str, bytes, Path]]]


class CMDResult(NamedTuple):
    cmd: CmdType
    out_b: bytes
    err_b: Optional[bytes]

    @property
    def out(self) -> str:
        return self.out_b.decode("utf8")


async def run_proc_timeout(cmd: CmdType,
                           proc: asyncio.subprocess.Process,
                           timeout: int,
                           input_data: Optional[bytes],
                           term_timeout: int) -> CMDResult:

    done, not_done = await asyncio.wait({proc.communicate(input=input_data)}, timeout=timeout - 2 * term_timeout)
    if not_done:
        proc.terminate()
        done, not_done = await asyncio.wait({proc.communicate()}, timeout=term_timeout)
        if not_done:
            proc.kill()
            proc_fut = proc.communicate()
            done, not_done = await asyncio.wait(proc_fut, timeout=term_timeout)

            if not_done:
                raise RuntimeError(f"Can't kill process {proc.pid} of {cmd}")

        proc_fut, = done
        out, err = await proc_fut
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout, output=out, stderr=err)

    proc_fut, = done
    out, err = await proc_fut

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(returncode=proc.returncode,
                                            cmd=cmd, output=out, stderr=err)

    return CMDResult(cmd, out, err)


async def start_proc(cmd: CmdType,
                     input_data: Union[bytes, None, BinaryIO] = None,
                     merge_err: bool = True,
                     output_to_devnull: bool = False) -> Tuple[asyncio.subprocess.Process, Optional[bytes]]:

    if isinstance(input_data, bytes):
        stdin = asyncio.subprocess.PIPE
    elif input_data is None:
        stdin = None
    else:
        stdin = input_data
        input_data = None

    if output_to_devnull:
        stderr = asyncio.subprocess.DEVNULL
        stdout = asyncio.subprocess.DEVNULL
    else:
        stderr = asyncio.subprocess.STDOUT if merge_err else asyncio.subprocess.PIPE
        stdout = asyncio.subprocess.PIPE

    if isinstance(cmd, str):
        func = asyncio.create_subprocess_shell
        cmd = [cmd]
    else:
        func = asyncio.create_subprocess_exec
        cmd = [str(arg) for arg in cmd]

    return (await func(*cmd, stdout=stdout, stderr=stderr, stdin=stdin)), input_data


async def run(cmd: CmdType,
              input_data: Union[bytes, None, BinaryIO] = None,
              merge_err: bool = True,
              timeout: int = 60,
              output_to_devnull: bool = False,
              term_timeout: int = 1) -> CMDResult:

    proc, input_data = await start_proc(cmd, input_data, merge_err, output_to_devnull)
    res = await run_proc_timeout(cmd, proc, timeout=timeout, input_data=input_data, term_timeout=term_timeout)

    if merge_err:
        assert res.err_b is None

    return res

