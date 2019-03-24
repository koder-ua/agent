from typing import cast, List, Optional, Dict, Any

import functools
import os
import json
import stat
import zlib
import shutil
import os.path
import logging
import tempfile
import subprocess
import distutils.spawn

from typing.io import BinaryIO


from .. import rpc
from .cli import async_check_output


expose = functools.partial(rpc.expose_func, "fs")
expose_async = functools.partial(rpc.expose_func_async, "fs")


logger = logging.getLogger("agent.fs")


@expose
def expanduser(path: str) -> str:
    return os.path.expanduser(path)


@expose
def listdir(path: str) -> List[str]:
    return os.listdir(path)


class ZlibCompressor(rpc.IReadable):

    def __init__(self, fd: BinaryIO, chunk: int = rpc.DEFAULT_CHUNK) -> None:
        self.fd = fd
        self.chunk = chunk
        self.compressor = zlib.compressobj()
        self.eof = True

    async def read_chunk(self):
        if self.eof:
            return b''

        res = b''
        while not res:
            data = self.fd.read(self.chunk)
            if data == b'':
                self.eof = True
                return res + self.compressor.flush()
            res += self.compressor.compress(data)

    def close(self):
        self.fd.close()


@expose
def get_file(path: str, compress: bool = True) -> rpc.StreamPayload:
    fd = open(path, "rb")
    return rpc.StreamPayload(cast(rpc.IReadable, ZlibCompressor(fd) if compress else fd))


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
async def store_file(path: str, content: rpc.StreamPayload, compressed: bool = False) -> str:
    if path is None:
        path = tempfile.mkstemp()

    unzlib = zlib.decompressobj() if compressed else None

    with open(path, "wb") as fd:
        while True:
            data = await content.fd.read_chunk()

            if compressed:
                data = unzlib.decompress(data)

            if data == b'':
                if compressed:
                    fd.write(unzlib.flush())
                break

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
        return (await async_check_output(["which", name])).decode("utf8")
    except subprocess.CalledProcessError:
        return None


@expose_async
async def get_dev_for_file(fname: str):
    out = (await async_check_output(["df", fname])).decode("utf8")
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
    lsblk = json.loads((await async_check_output(["lsblk", '-a', '--json']))).decode("utf8")
    res = {}
    for node in lsblk['blockdevices']:
        fall_down(node, node['name'], res)
    return res


@expose_async
async def get_dev_for_file2(fname: str) -> str:
    fname = os.path.abspath(fname)
    fname = follow_symlink(fname)
    mp_map = await get_mountpoint_to_dev_mapping()

    while True:
        if fname in mp_map:
            return '/dev/' + mp_map[fname]
        assert fname != '/', "Can't found dev for {0}".format(fname)
        fname = os.path.dirname(fname)
        fname = follow_symlink(fname)


@expose
def binarys_exists(names: List[str]) -> List[str]:
    return [distutils.spawn.find_executable(name) is not None for name in names]
