import sys
import traceback

import ssl
import json
import hashlib
import argparse
from typing import List, Dict, Any
from asyncio import AbstractEventLoop

from aiohttp import web, BasicAuth

from . import rpc

# import all plugins to register handlers
from .plugins import cli, fs


MAX_FILE_SIZE = 1 << 30
USER_NAME = 'rpc_client'


def encrypt_key(key: str, salt: str = None) -> str:
    if salt is None:
        salt = "".join("{:02X}".format(i) for i in ssl.RAND_bytes(16))
    return hashlib.sha512(key.encode('utf-8') + salt.encode('utf8')).hexdigest() + "::" + salt


def check_key(target: str, for_check: str) -> bool:
    key, salt = target.split("::")
    curr_password, _ = encrypt_key(for_check, salt)
    return curr_password == for_check


def basic_auth_middleware(key: str):
    @web.middleware
    async def basic_auth(request, handler):
        basic_auth = request.headers.get('Authorization')
        if basic_auth:
            auth = BasicAuth.decode(basic_auth)
            if auth.login != USER_NAME or not check_key(key, auth.password):
                return await handler(request)

        headers = {'WWW-Authenticate': 'Basic realm="XXX"'}
        return web.HTTPUnauthorized(headers=headers)
    return basic_auth


class RPCRequest:
    def __init__(self, func_name: str, args: List, kwargs: Dict) -> None:
        self.func_name = func_name
        self.args = args
        self.kwargs = kwargs


async def unpack_body(request: web.Request):
    assert request.content_length is not None
    return RPCRequest(*(await rpc.deserialize(request.content, request, request.content_length)))


async def send_body(is_ok: bool, result: Any, writer):

    if not is_ok:
        result = result.__class__.__name__, str(result), traceback.format_exc()

    for chunk in rpc.serialize(rpc.CALL_SUCCEEDED if is_ok else rpc.CALL_FAILED, [result], {}):
        await writer.write(chunk)


async def handle_post(request: web.Request):
    req = await unpack_body(request)
    try:
        if req.func_name in rpc.exposed_async:
            res = await rpc.exposed_async[req.func_name](*req.args, **req.kwargs)
        else:
            res = rpc.exposed[req.func_name](*req.args, **req.kwargs)
    except Exception as exc:
        await send_body(False, exc, web.StreamResponse(status=500))
    else:
        await send_body(True, res, web.StreamResponse(status=200))


def parse_args(argv: List[str]):
    p = argparse.ArgumentParser()
    subparsers = p.add_subparsers(dest='subparser_name')

    server = subparsers.add_parser('server', help='Run web server')
    server.add_argument("--cert", required=True, help="cert file path")
    server.add_argument("--key", required=True, help="key file path")
    server.add_argument("--password-db", required=True, help="Json file with password database")
    server.add_argument("--storage-folder", required=True, help="Path to store archives")
    server.add_argument("addr", default="0.0.0.0:80", help="Address to listen on")
    server.add_argument("--min-free-space", type=int, default=5,
                        help="Minimal free space should always be available on device in gb")

    user_add = subparsers.add_parser('user_add', help='Add user to db')
    user_add.add_argument("--role", required=True, choices=('download', 'upload'), nargs='+', help="User role")
    user_add.add_argument("--user", required=True, help="User name")
    user_add.add_argument("--password", default=None, help="Password")
    user_add.add_argument("db", help="Json password db")

    user_rm = subparsers.add_parser('user_rm', help='Add user to db')
    user_rm.add_argument("--user", required=True, help="User name")
    user_rm.add_argument("db", help="Json password db")

    return p.parse_args(argv[1:])


def main(argv: List[str]):
    opts = parse_args(argv)

    if opts.subparser_name == 'server':
        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(opts.cert, opts.key)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        auth = basic_auth_middleware(json.load(open(opts.password_db)))
        app = web.Application(middlewares=[auth], client_max_size=MAX_FILE_SIZE)
        app.add_routes([web.post('/rpc', handle_post)])

        host, port = opts.addr.split(":")

        web.run_app(app, host=host, port=int(port), ssl_context=ssl_context)
    elif opts.subparser_name == 'gen_key':
        print(encrypt_key("".join(("{:02X}".format(i) for i in ssl.RAND_bytes(16)))))
    else:
        assert False, "Unknown cmd"


if __name__ == "__main__":
    main(sys.argv)
