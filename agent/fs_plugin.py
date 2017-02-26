import os
import stat
import zlib
import shutil
import os.path
import tempfile


mod_name = "fs"
__version__ = (0, 1)


def rpc_expanduser(path):
    return os.path.expanduser(path)


def rpc_listdir(path):
    return os.listdir(path)


def rpc_get_file(path, compress=True):
    data = open(path, "rb").read()
    if compress:
        data = zlib.compress(data)
    return data


def rpc_file_stat(path):
    fstat = os.stat(path)

    if stat.S_ISBLK(fstat.st_mode):
        with open(path, 'rb') as fd:
            fd.seek(os.SEEK_END, 0)
            size = fd.tell()
    else:
        size = fstat.st_size

    return {"size": size}


def rpc_store_file(path, content, compress=False):
    if path is None:
        path = tempfile.mkstemp()

    if compress:
        content = zlib.decompress(content)

    with open(path, "wb") as fd:
        fd.write(content)

    return path


def rpc_file_exists(path):
    return os.path.exists(path)


def rpc_rmtree(path):
    shutil.rmtree(path)


def rpc_makedirs(path):
    os.makedirs(path)


def rpc_unlink(path):
    os.unlink(path)
