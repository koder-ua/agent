import os.path


mod_name = "fs"
__version__ = (0, 1)


def rpc_get_file(path):
    return open(path, "rb").read()


def rpc_store_file(path, content):
    with open(path, "wb") as fd:
        fd.write(content)


def rpc_file_exists(path):
    return os.path.exists(path)
