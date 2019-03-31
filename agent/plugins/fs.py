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
from typing import List, Optional, Dict, Any, Tuple

from .. import rpc
from .. import utils

MAX_FILE_SIZE = 8 * (1 << 20)

expose = functools.partial(rpc.expose_func, "fs")
expose_async = functools.partial(rpc.expose_func_async, "fs")


logger = logging.getLogger("agent.fs")


@expose
def expanduser(path: str) -> str:
    return os.path.expanduser(path)


@expose
def listdir(path: str) -> List[str]:
    return os.listdir(path)


@expose
def get_file(path: str, compress: bool = True) -> rpc.IReadableAsync:
    fd = rpc.ChunkedFile(open(path, "rb"))
    return rpc.ZlibStreamCompressor(fd) if compress else fd


@expose
def get_file_no_stream(path: str, compress: bool = True) -> rpc.IReadableAsync:
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
async def store_file(path: str, content: rpc.IReadableAsync, compressed: bool = False) -> str:
    if path is None:
        path = tempfile.mkstemp()

    unzlib = zlib.decompressobj() if compressed else None

    with open(path, "wb") as fd:
        while True:
            data = await content.readany()

            if compressed:
                data = unzlib.decompress(data)

            if data == b'':
                if compressed:
                    fd.write(unzlib.flush())
                break
            else:
                fd.write(data)

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
        return (await utils.run(["which", name])).stdout
    except subprocess.CalledProcessError:
        return None


@expose_async
async def get_dev_for_file(fname: str):
    out = (await utils.run(["df", fname])).stdout
    dev_link = out.strip().split("\n")[1].split()[0]

    if dev_link == 'udev':
        dev_link = fname

    dev_link = os.path.abspath(dev_link)
    while os.path.islink(dev_link):
        dev_link_next = os.readlink(dev_link)
        dev_link_next = os.path.join(os.path.dirname(dev_link), dev_link_next)
        dev_link = os.path.abspath(dev_link_next)

    return dev_link


def fall_down(node: Dict, root: str, res_dict: Dict[str, str]):
    if 'mountpoint' in node and node['mountpoint']:
        res_dict[node['mountpoint']] = root

    for ch_node in node.get('children', []):
        fall_down(ch_node, root, res_dict)


async def get_mountpoint_to_dev_mapping() -> Dict[str, str]:
    lsblk = json.loads((await utils.run(["lsblk", '-a', '--json']))).stdout
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

    logger.debug(f"Find pids for binary {bname} = {res}")

    return res


@expose
def get_block_devs_info(filter_virtual : bool = True) -> Dict[str, Tuple[bool, str]]:
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
