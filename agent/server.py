import ssl
import sys
import http
import hashlib
import argparse
import traceback
from typing import List, Dict, Any, Tuple

from aiohttp import web, BasicAuth

from . import rpc

# import all plugins to register handlers
from .plugins import cli, fs, system


MAX_FILE_SIZE = 1 << 30
USER_NAME = 'rpc_client'


def encrypt_key(key: str, salt: str = None) -> str:
    if salt is None:
        salt = "".join("{:02X}".format(i) for i in ssl.RAND_bytes(16))
    return hashlib.sha512(key.encode('utf-8') + salt.encode('utf8')).hexdigest() + "::" + salt


def check_key(target: str, for_check: str) -> bool:
    key, salt = target.split("::")
    curr_password = encrypt_key(for_check, salt)
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


async def send_body(is_ok: bool, result: Any, writer):

    if not is_ok:
        result = result.__class__.__name__, str(result), traceback.format_exc()

    async for chunk in rpc.serialize(rpc.CALL_SUCCEEDED if is_ok else rpc.CALL_FAILED, [result], {}):
        await writer.write(chunk)


async def handle_post(request: web.Request):
    req = RPCRequest(*(await rpc.deserialize(request.content)))
    responce = web.StreamResponse(status=http.HTTPStatus.OK)
    await responce.prepare(request)
    try:
        if req.func_name in rpc.exposed_async:
            res = await rpc.exposed_async[req.func_name](*req.args, **req.kwargs)
        else:
            res = rpc.exposed[req.func_name](*req.args, **req.kwargs)
    except Exception as exc:
        await send_body(False, exc, responce)
    else:
        await send_body(True, res, responce)
    return responce


async def handle_ping(request: web.Request):
    responce = web.Response(status=http.HTTPStatus.OK)
    await responce.prepare(request)
    return responce


def get_key_enc() -> Tuple[str, str]:
    key = "".join(("{:02X}".format(i) for i in ssl.RAND_bytes(16)))
    return key, encrypt_key(key)


def start_rpc_server(addr: str, ssl_cert: str, ssl_key: str, api_key: str):
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(ssl_cert, ssl_key)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    auth = basic_auth_middleware(api_key)
    app = web.Application(middlewares=[auth], client_max_size=MAX_FILE_SIZE)
    app.add_routes([web.post('/rpc', handle_post)])
    app.add_routes([web.get('/ping', handle_ping)])

    host, port = addr.split(":")

    web.run_app(app, host=host, port=int(port), ssl_context=ssl_context)


def parse_args(argv: List[str]):
    p = argparse.ArgumentParser()
    subparsers = p.add_subparsers(dest='subparser_name')

    server = subparsers.add_parser('server', help='Run web server')
    server.add_argument("--cert", required=True, help="cert file path")
    server.add_argument("--key", required=True, help="key file path")
    server.add_argument("--api-key", default=None, help="Json file api key")
    server.add_argument("--api-key-val", help="Json file api key")
    server.add_argument("addr", default="0.0.0.0:55443", help="Address to listen on")

    subparsers.add_parser('gen_key', help='Generate new key')

    return p.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    if opts.subparser_name == 'server':
        if opts.api_key is None:
            api_key = opts.api_key_val
        else:
            api_key = open(opts.key_file).read()
        start_rpc_server(opts.addr, opts.cert, opts.key, api_key)
    elif opts.subparser_name == 'gen_key':
        print("Key={}\nenc_key={}".format(*get_key_enc()))
    else:
        assert False, "Unknown cmd"
    return 0


if __name__ == "__main__":
    main(sys.argv)
