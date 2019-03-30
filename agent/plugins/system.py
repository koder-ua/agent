import logging
import functools


from .. import rpc


expose = functools.partial(rpc.expose_func, "sys")
expose_async = functools.partial(rpc.expose_func_async, "sys")


logger = logging.getLogger("agent.fs")


@expose
def ping(data: str = "") -> str:
    return data


@expose
def test(*args, **kwargs):
    return [args, kwargs]