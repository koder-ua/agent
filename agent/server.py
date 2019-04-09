import ssl
import sys
import http
import argparse
import traceback
import subprocess
import logging.config
from typing import List, Dict, Any

from aiohttp import web, BasicAuth

from .plugins import DEFAULT_HTTP_CHUNK, exposed, exposed_async
from . import rpc, USER_NAME, MAX_FILE_SIZE, DEFAULT_PORT
from .ssh_deploy import get_key_enc, encrypt_key


def check_key(target: str, for_check: str) -> bool:
    key, salt = target.split("::")
    curr_password = encrypt_key(for_check, salt)
    return curr_password == for_check


def basic_auth_middleware(key: str):
    @web.middleware
    async def basic_auth(request, handler):
        if request.path == '/ping':
            return await handler(request)

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
        if isinstance(result, subprocess.TimeoutExpired):
            args = (result.cmd, result.timeout, result.stdout, result.stderr)
        else:
            args = result.args

        result = result.__class__.__name__, args, traceback.format_exc()

    async for chunk in rpc.serialize(rpc.CALL_SUCCEEDED if is_ok else rpc.CALL_FAILED, [result], {}):
        await writer.write(chunk)


async def handle_post(request: web.Request):
    req = RPCRequest(*(await rpc.deserialize(request.content.iter_chunked(DEFAULT_HTTP_CHUNK), allow_streamed=True)))
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


async def handle_ping(request: web.Request):
    responce = web.Response(status=http.HTTPStatus.OK)
    await responce.prepare(request)
    return responce


def start_rpc_server(addr: str, ssl_cert: str, ssl_key: str, api_key: str):
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(ssl_cert, ssl_key)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    auth = basic_auth_middleware(api_key)
    app = web.Application(middlewares=[auth], client_max_size=MAX_FILE_SIZE)
    app.add_routes([web.post('/conn', handle_post)])
    app.add_routes([web.get('/ping', handle_ping)])

    host, port = addr.split(":")

    web.run_app(app, host=host, port=int(port), ssl_context=ssl_context)


def parse_args(argv: List[str]):
    p = argparse.ArgumentParser()
    subparsers = p.add_subparsers(dest='subparser_name')

    server = subparsers.add_parser('server', help='Run web server')
    server.add_argument("--cert", required=True, help="cert file path")
    server.add_argument("--key", required=True, help="key file path")
    server.add_argument("--api-key", default=None, help="api key file")
    server.add_argument("--api-key-val", help="Json file api key")
    server.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO",
                        help="Console log level")
    server.add_argument("--persistent-log", action='store_true', help="Log to /var/log/mira_agent.log as well")
    server.add_argument("--persistent-level",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], default="INFO",
                        help="Persistent log level")
    server.add_argument("--addr", default=f"0.0.0.0:{DEFAULT_PORT}", help="Address to listen on")

    subparsers.add_parser('gen_key', help='Generate new key')

    return p.parse_args(argv[1:])


log_config = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "stream": "ext://sys.stdout"
        },
        "persistent": {
            "level": "INFO",
            "class": "logging.FileHandler",
            "formatter": "simple",
            "filename": "/var/log/mira_agent.log"
        },
    },
    "loggers": {
        "cmd":     {"level": "DEBUG", "handlers": ["console"]},
        "storage": {"level": "DEBUG", "handlers": ["console"]},
        "rpc":     {"level": "DEBUG", "handlers": ["console"]},
        "cephlib": {"level": "DEBUG", "handlers": ["console"]},
        "agent":   {"level": "DEBUG", "handlers": ["console"]},
    }
}


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    if opts.subparser_name == 'server':

        if not opts.persistent_log:
            del log_config['handlers']['persistent']
        else:
            log_config['handlers']['persistent']['level'] = opts.persistent_level
            for lcfg in log_config['loggers'].values():
                lcfg['handlers'].append('persistent')

        log_config['handlers']['console']['level'] = opts.log_level
        logging.config.dictConfig(log_config)

        api_key = opts.api_key_val if opts.api_key is None else open(opts.api_key).read()
        start_rpc_server(opts.addr, opts.cert, opts.key, api_key)
    elif opts.subparser_name == 'gen_key':
        key, enc_key = get_key_enc()
        print(f"Key={key}\nenc_key={enc_key}")
    else:
        assert False, f"Unknown cmd {opts.subparser_name}"
    return 0


if __name__ == "__main__":
    main(sys.argv)
