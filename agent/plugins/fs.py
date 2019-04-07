import errno
import os
import json
import stat
import zlib
import shutil
import os.path
import logging
import tempfile
import functools
import subprocess
import distutils.spawn
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple, AsyncGenerator, AsyncIterable

from koder_utils import run_stdout

from . import expose_func, expose_func_async, IReadableAsync, ChunkedFile, ZlibStreamCompressor, BlockType


expose = functools.partial(expose_func, "fs")
expose_async = functools.partial(expose_func_async, "fs")


MAX_FILE_SIZE = 8 * (1 << 20)


logger = logging.getLogger("agent.fs")


@expose
def expanduser(path: str) -> str:
    return os.path.expanduser(path)


@expose
def iterdir(path: str) -> List[str]:
    return list(map(str, Path(path).iterdir()))


@expose
def get_file(path: str, compress: bool = True) -> IReadableAsync:
    fd = ChunkedFile(open(path, "rb"))
    return ZlibStreamCompressor(fd) if compress else fd


@expose
def get_file_no_stream(path: str, compress: bool = True) -> IReadableAsync:
    if os.stat(path).st_size > MAX_FILE_SIZE:
        raise ValueError("File to large for single-shot stransfer")
    fc = open(path, "rb").read()
    return zlib.compress(fc) if compress else fc


@expose
async def file_stat(path: str) -> Dict[str, Any]:
    fstat = os.stat(path)

    if stat.S_ISBLK(fstat.st_mode):
        with open(path, 'rb') as fd:
            fd.seek(os.SEEK_END, 0)
            size = fd.tell()
    else:
        size = fstat.st_size

    return {"size": size}


@expose_async
async def write_file(path: Optional[str], content: AsyncIterable[Tuple[BlockType, bytes]],
                     compress: bool = False) -> str:
    if path is None:
        fd, path = tempfile.mkstemp()
        os.close(fd)

    unzipobj = zlib.decompressobj() if compress else None

    with open(path, "wb") as fd:
        async for btype, data in content:
            assert btype == BlockType.binary

            if compress:
                data = unzipobj.decompress(data)
            fd.write(data)

        if compress:
            fd.write(unzipobj.flush())

    return path


@expose
def file_exists(path: str) -> bool:
    return os.path.exists(path)


@expose
def rmtree(path: str):
    shutil.rmtree(path)


@expose
def makedirs(path: str):
    os.makedirs(path)


@expose
def unlink(path: str):
    os.unlink(path)


@expose_async
async def which(name: str) -> Optional[str]:
    try:
        return await run_stdout(["which", name])
    except subprocess.CalledProcessError:
        return None


@expose_async
async def get_dev_for_file(fname: str) -> str:
    dev_link = (await run_stdout(["df", fname])).strip().split("\n")[1].split()[0]
    return str(Path(fname if dev_link == 'udev' else dev_link).resolve())


def fall_down(node: Dict, root: str, res_dict: Dict[str, str]):
    if 'mountpoint' in node and node['mountpoint']:
        res_dict[node['mountpoint']] = root

    for ch_node in node.get('children', []):
        fall_down(ch_node, root, res_dict)


async def get_mountpoint_to_dev_mapping() -> Dict[str, str]:
    lsblk = json.loads(await run_stdout(["lsblk", '-a', '--json']))
    res = {}
    for node in lsblk['blockdevices']:
        fall_down(node, node['name'], res)
    return res


def follow_symlink(fname: str) -> str:
    while os.path.islink(fname):
        dev_link_next = os.readlink(fname)
        dev_link_next = os.path.join(os.path.dirname(fname), dev_link_next)
        fname = os.path.abspath(dev_link_next)
    return fname


@expose_async
async def get_dev_for_file2(fname: str) -> str:
    fname = os.path.abspath(fname)
    fname = follow_symlink(fname)
    mp_map = await get_mountpoint_to_dev_mapping()

    while True:
        if fname in mp_map:
            return '/dev/' + mp_map[fname]
        assert fname != '/', f"Can't found dev for {fname}"
        fname = os.path.dirname(fname)
        fname = follow_symlink(fname)


@expose
def binarys_exists(names: List[str]) -> List[str]:
    return [distutils.spawn.find_executable(name) is not None for name in names]


@expose
def find_pids_for_cmd(bname: str) -> List[int]:
    bin_path = distutils.spawn.find_executable(bname)

    if not bin_path:
        raise NameError(f"Can't found binary path for {bname!r}")

    res = []
    for name in os.listdir('/proc'):
        if name.isdigit() and os.path.isdir(f'/proc/{name}'):
            exe = f'/proc/{name}/exe'
            if os.path.exists(exe) and os.path.islink(exe) and bin_path == os.readlink(exe):
                res.append(int(name))

    return res


@expose
def get_block_devs_info(filter_virtual: bool = True) -> Dict[str, Tuple[bool, str]]:
    res = {}
    for name in os.listdir("/sys/block"):
        rot_fl = f"/sys/block/{name}/queue/rotational"
        sched_fl = f"/sys/block/{name}/queue/scheduler"
        if os.path.isfile(rot_fl) and os.path.isfile(sched_fl):
            if filter_virtual and follow_symlink(f'/sys/block/{name}').startswith('/sys/devices/virtual'):
                continue
            res[name] = (
                open(rot_fl).read().strip() == 1,
                open(sched_fl).read()
            )
    return res


@expose
def stat(path: str) -> List[int]:
    return list(os.stat(path))


@expose
def stat_all(paths: List[str]) -> List[List[int]]:
    return list(map(stat, paths))


@expose
def count_sockets_for_process(pid: int) -> int:
    count = 0
    for fd in os.listdir('/proc/{0}/fd'.format(pid)):
        try:
            if stat.S_ISSOCK(os.stat('/proc/{0}/fd/{1}'.format(pid, fd)).st_mode):
                count += 1
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

    return count
