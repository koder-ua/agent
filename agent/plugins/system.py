import logging
import functools

from . import expose_func


expose = functools.partial(expose_func, "sys")


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
