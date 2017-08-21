import os
import json
import stat
import zlib
import shutil
import os.path
import logging
import tempfile
import subprocess

from agent_module import noraise, tostr

mod_name = "fs"
__version__ = (0, 1)


logger = logging.getLogger("agent.fs")


@noraise
def rpc_expanduser(path):
    return os.path.expanduser(path)


@noraise
def rpc_listdir(path):
    return os.listdir(path)


@noraise
def rpc_get_file(path, compress=True):
    data = open(path, "rb").read()
    if compress:
        data = zlib.compress(data)
    return data


@noraise
def rpc_file_stat(path):
    fstat = os.stat(path)

    if stat.S_ISBLK(fstat.st_mode):
        with open(path, 'rb') as fd:
            fd.seek(os.SEEK_END, 0)
            size = fd.tell()
    else:
        size = fstat.st_size

    return {"size": size}


@noraise
def rpc_store_file(path, content, compress=False):
    if path is None:
        path = tempfile.mkstemp()

    if compress:
        content = zlib.decompress(content)

    with open(path, "wb") as fd:
        fd.write(content)

    return path


@noraise
def rpc_file_exists(path):
    return os.path.exists(path)


@noraise
def rpc_rmtree(path):
    shutil.rmtree(path)


@noraise
def rpc_makedirs(path):
    os.makedirs(path)


@noraise
def rpc_unlink(path):
    os.unlink(path)


@noraise
def rpc_which(name):
    try:
        return subprocess.check_output(["which", name])
    except subprocess.CalledProcessError:
        return None


@noraise
def rpc_get_dev_for_file(fname):
    fname = tostr(fname)
    out = subprocess.check_output(["df", fname])
    dev_link = out.strip().split("\n")[1].split()[0]

    if dev_link == 'udev':
        dev_link = fname

    dev_link = os.path.abspath(dev_link)
    while os.path.islink(dev_link):
        dev_link_next = os.readlink(dev_link)
        dev_link_next = os.path.join(os.path.dirname(dev_link), dev_link_next)
        dev_link = os.path.abspath(dev_link_next)

    return tostr(dev_link)


def follow_symlink(fname):
    while os.path.islink(fname):
        dev_link_next = os.readlink(fname)
        dev_link_next = os.path.join(os.path.dirname(fname), dev_link_next)
        fname = os.path.abspath(dev_link_next)
    return tostr(fname)


def fall_down(node, root, res_dict):
    if 'mountpoint' in node and node['mountpoint']:
        res_dict[node['mountpoint']] = root

    for ch_node in node.get('children', []):
        fall_down(ch_node, root, res_dict)


def get_mountpoint_to_dev_mapping():
    lsblk = json.loads(subprocess.check_output(["lsblk", '-a', '--json']))
    res = {}
    for node in lsblk['blockdevices']:
        fall_down(node, node['name'], res)
    return res


@noraise
def rpc_get_dev_for_file2(fname):
    fname = os.path.abspath(tostr(fname))
    fname = follow_symlink(fname)
    mp_map = get_mountpoint_to_dev_mapping()

    while True:
        if fname in mp_map:
            return '/dev/' + mp_map[fname]
        assert fname != '/', "Can't found dev for {0}".format(fname)
        fname = os.path.dirname(fname)
        fname = follow_symlink(fname)
