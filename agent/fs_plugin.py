import os
import stat
import zlib
import shutil
import os.path
import tempfile
import subprocess


from agent_module import noraise

mod_name = "fs"
__version__ = (0, 1)


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
def rpc_get_dev_for_file(fname):
    out = subprocess.check_output(["df", fname])
    dev_link = out.strip().split("\n")[1].split()[0]

    if dev_link == 'udev':
        dev_link = fname

    dev_link = os.path.abspath(dev_link)
    while os.path.islink(dev_link):
        dev_link_next = os.readlink(dev_link)
        dev_link_next = os.path.join(os.path.dirname(dev_link), dev_link_next)
        dev_link = os.path.abspath(dev_link_next)
    return dev_link
