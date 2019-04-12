import ssl
import sys
import http
import argparse
import traceback
import subprocess
import logging.config
from pathlib import Path
from typing import List, Dict, Any

from aiohttp import web, BasicAuth

from .plugins import DEFAULT_HTTP_CHUNK, exposed, exposed_async, CONFIG_OBJ, on_server_startup, on_server_shutdown
from .common import USER_NAME, MAX_FILE_SIZE, get_config, get_key_enc, encrypt_key, AgentConfig, config_logging
from . import rpc


logger = logging.getLogger("agent")


def check_key(target: str, for_check: str) -> bool:
    key, salt = target.split("::")
    curr_password = encrypt_key(for_check, salt)
    return curr_password == for_check


def basic_auth_middleware(key: str):
    @web.middleware
    async def basic_auth(request, handler):
        if request.path == '/ping':
            return await handler(request)

        auth_info = request.headers.get('Authorization')
        if auth_info:
            auth = BasicAuth.decode(auth_info)
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
        if isinstance(result, subprocess.TimeoutExpired):
            args = (result.cmd, result.timeout, result.stdout, result.stderr)
        elif isinstance(result, subprocess.CalledProcessError):
            args = (result.returncode, result.cmd, result.stdout, result.stderr)
        else:
            args = result.args

        result = result.__class__.__name__, args, traceback.format_exc()
    async for chunk in rpc.serialize(rpc.CALL_SUCCEEDED if is_ok else rpc.CALL_FAILED, [result], {}):
        await writer.write(chunk)


async def handle_post(request: web.Request):
    try:
        req = RPCRequest(*(await rpc.deserialize(request.content.iter_chunked(DEFAULT_HTTP_CHUNK),
                         allow_streamed=True)))
        responce = web.StreamResponse(status=http.HTTPStatus.OK, headers={'Content-Encoding': 'identity'})
        await responce.prepare(request)
        try:
            if req.func_name in exposed_async:
                res = await exposed_async[req.func_name](*req.args, **req.kwargs)
            else:
                res = exposed[req.func_name](*req.args, **req.kwargs)
        except Exception as exc:
            await send_body(False, exc, responce)
        else:
            await send_body(True, res, responce)
        return responce
    except:
        logger.exception("During send body")
        raise


async def handle_ping(request: web.Request):
    responce = web.Response(status=http.HTTPStatus.OK)
    await responce.prepare(request)
    return responce


def start_rpc_server(cfg: AgentConfig):
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    host = cfg.listen_ip
    port = cfg.server_port

    ssl_context.load_cert_chain(str(cfg.ssl_cert), str(cfg.ssl_key))
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    auth = basic_auth_middleware(cfg.api_key_enc.open().read())
    app = web.Application(middlewares=[auth], client_max_size=MAX_FILE_SIZE)
    app.add_routes([web.post('/conn', handle_post)])
    app.add_routes([web.get('/ping', handle_ping)])

    for func in on_server_startup:
        app.on_startup.append(func)

    for func in on_server_shutdown:
        app.on_cleanup.append(func)

    web.run_app(app, host=host, port=int(port), ssl_context=ssl_context)


def parse_args(argv: List[str]):
    p = argparse.ArgumentParser()
    subparsers = p.add_subparsers(dest='subparser_name')
    server = subparsers.add_parser('server', help='Run web server')
    server.add_argument("--config", required=True, help="Config file path")
    subparsers.add_parser('gen_key', help='Generate new key')
    return p.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    cfg = get_config(Path(opts.config))
    config_logging(cfg)

    if opts.subparser_name == 'server':
        # share config with plugins
        CONFIG_OBJ[0] = cfg
        start_rpc_server(cfg)
    elif opts.subparser_name == 'gen_key':
        key, enc_key = get_key_enc()
        print(f"Key={key}\nenc_key={enc_key}")
    else:
        assert False, f"Unknown cmd {opts.subparser_name}"
    return 0


if __name__ == "__main__":
    main(sys.argv)
