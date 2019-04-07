import logging
import functools

from . import expose_func, expose_func_async


expose = functools.partial(expose_func, "sys")
expose_async = functools.partial(expose_func_async, "sys")


logger = logging.getLogger("agent.fs")


@expose
def ping(data: str = "") -> str:
    return data


@expose
def test(*args, **kwargs):
    return [args, kwargs]


@expose
def get_logs() -> str:
    return ""
