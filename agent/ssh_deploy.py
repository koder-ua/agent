import re
import os
import ssl
import sys
import uuid
import os.path
import asyncio
import getpass
import hashlib
import argparse
import subprocess
from pathlib import Path
from typing import List, Tuple, Any

import aiohttp

from koder_utils import SSH, read_inventory, make_secure, make_cert_and_key

from .client import AsyncRPCClient, RPCServerFailure


SERVICE_NAME = "mirantis_agent.service"
AGENT_DATA_PATH = Path("/var/mirantis/agent")
LOCAL_INSTALL_PATH = Path(__file__).resolve().parent.parent


async def stop(nodes: List[SSH]):
    await asyncio.gather(*[node.run(["sudo", "systemctl", "stop", SERVICE_NAME]) for node in nodes])


async def disable(nodes: List[SSH]):
    await asyncio.gather(*[node.run(["sudo", "systemctl", "disable", SERVICE_NAME]) for node in nodes])


async def enable(nodes: List[SSH]):
    await asyncio.gather(*[node.run(["sudo", "systemctl", "enable", SERVICE_NAME]) for node in nodes])


async def start(nodes: List[SSH]):
    await asyncio.gather(*[node.run(["sudo", "systemctl", "start", SERVICE_NAME]) for node in nodes])


async def status(nodes: List[SSH], certs_folder: Path):

    key_path = certs_folder / 'agent_api.key'
    if not key_path.is_file():
        print(f"Can't find key file at {key_path}")
        return

    access_key = key_path.open().read()

    async def check_node(node: SSH) -> Tuple[str, bool]:
        ssl_cert_file = certs_folder / f'agent_server.{node.node}.cert'
        client = AsyncRPCClient(node.node,
                                ssl_cert_file=ssl_cert_file,
                                access_key=access_key)

        try:
            async with client:
                await client.sys.ping(_call_timeout=5)
            return node.node, True
        except aiohttp.ClientConnectionError:
            return node.node, False
        except RPCServerFailure:
            return node.node, True

    max_node_name_len = max(len(node.node) for node in nodes)
    for node_name, res in sorted(await asyncio.gather(*map(check_node, nodes))):
        print("{0:>{1}} {2:>8}".format(node_name, max_node_name_len, "RUN" if res else "NOT RUN"))


async def remove(nodes: List[SSH], target_folder: str = "/opt/mirantis"):
    target_path = Path(target_folder)

    try:
        await disable(nodes)
    except subprocess.SubprocessError:
        pass

    try:
        await stop(nodes)
    except subprocess.SubprocessError:
        pass

    async def runner(node: SSH):
        try:
            agent_folder: Path = target_path / "agent"
            service_target = Path("/lib/systemd/system") / SERVICE_NAME

            await node.run(["sudo", "rm", "--force", service_target])
            await node.run(["sudo", "systemctl", "daemon-reload"])

            for folder in (agent_folder, AGENT_DATA_PATH):
                assert re.match(r"/[a-zA-Z0-9-_]+/mirantis/agent$", str(folder)), \
                    f"{folder} not match re of allowed to rm path"
                await node.run(["sudo", "rm", "--preserve-root", "--recursive", "--force", folder])
        except Exception as exc:
            exc.node = node  # type: ignore
            raise

    for val in await asyncio.gather(*map(runner, nodes), return_exceptions=True):
        if val is not None:
            assert isinstance(val, Exception)
            print(f"Failed on node {getattr(getattr(val, 'node', None), 'node', None)} with message: {val!s}")


def get_key_enc() -> Tuple[str, str]:
    key = "".join((f"{i:02X}" for i in ssl.RAND_bytes(16)))
    return key, encrypt_key(key)


def encrypt_key(key: str, salt: str = None) -> str:
    if salt is None:
        salt = "".join(f"{i:02X}" for i in ssl.RAND_bytes(16))
    return hashlib.sha512(key.encode('utf-8') + salt.encode('utf8')).hexdigest() + "::" + salt


async def deploy(nodes: List[SSH], arch_file: str, max_parallel_uploads: int, target_folder: Path,
                 certs_folder: Path):

    upload_semaphore = asyncio.Semaphore(max_parallel_uploads if max_parallel_uploads else len(nodes))

    certs_folder.mkdir(mode=0o770, parents=True, exist_ok=True)

    api_key_file = certs_folder / "agent_api.key"
    api_enc_key_file = certs_folder / "agent_api_key.enc"

    make_secure(api_key_file, api_enc_key_file)
    api_key, api_enc_key = get_key_enc()

    with api_key_file.open('w') as fd:
        fd.write(api_key)

    with api_enc_key_file.open('w') as fd:
        fd.write(api_enc_key)

    async def runner(node: SSH):
        node_certs_folder = target_folder / "certs"

        await node.run(["sudo", "mkdir", "--parents", target_folder])
        await node.run(["sudo", "mkdir", "--parents", AGENT_DATA_PATH])
        assert arch_file.endswith(".tar.gz")

        temp_arch_file = f"/tmp/mirantis_agent_{uuid.uuid1()!s}.tar.gz"

        async with upload_semaphore:
            await node.copy(arch_file, temp_arch_file)

        await node.run(["sudo", "tar", "--extract", "--directory=" + str(target_folder), "--file", temp_arch_file])
        await node.run(["sudo", "chown", "--recursive", "root.root", target_folder])
        await node.run(["sudo", "chmod", "--recursive", "o-w", target_folder])
        await node.run(["sudo", "mkdir", "--parents", str(node_certs_folder)])

        await node.run(["sudo", "mkdir", "--parents", str(node_certs_folder)])

        ssl_cert_file = certs_folder / f"agent_server.{node.node}.cert"
        ssl_key_file = certs_folder / f"agent_server.{node.node}.key"
        make_secure(ssl_cert_file, ssl_key_file)

        await make_cert_and_key(ssl_key_file, ssl_cert_file,
                                f"/C=NN/ST=Some/L=Some/O=agent/OU=agent/CN={node.node}")

        target_cert_path = str(node_certs_folder / "ssl_cert.cert")
        target_key_path = str(node_certs_folder / "ssl_cert.key")
        target_api_key_path = str(node_certs_folder / "api.key")

        await node.run(["sudo", "tee", target_cert_path], input_data=ssl_cert_file.open("rb").read())
        await node.run(["sudo", "tee", target_key_path], input_data=ssl_key_file.open("rb").read())
        await node.run(["sudo", "tee", target_api_key_path], input_data=api_enc_key.encode("utf8"))

        await node.run(["rm", temp_arch_file])

        service_content = (LOCAL_INSTALL_PATH / (SERVICE_NAME + "_template")).open().read()

        service_content = service_content.replace("{INSTALL}", str(target_folder)) \
            .replace("{CERT_PATH}", target_cert_path) \
            .replace("{KEY_PATH}", target_key_path) \
            .replace("{API_KEY_PATH}", target_api_key_path)

        await node.run(["sudo", "tee", f"/lib/systemd/system/{SERVICE_NAME}"], input_data=service_content.encode())
        await node.run(["sudo", "systemctl", "daemon-reload"])

    await asyncio.gather(*map(runner, nodes))
    await enable(nodes)
    await start(nodes)


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest='subparser_name')

    deploy_parser = subparsers.add_parser('install', help='Deploy')
    deploy_parser.add_argument("--max-parallel-uploads", default=0, type=int,
                               help="Max parallel archive uploads to target nodes (default: %(default)s)")
    deploy_parser.add_argument("--arch", metavar='ARCH_FILE',
                               default=str(LOCAL_INSTALL_PATH / 'distribution.tar.gz'),
                               help="Path to file with agent archive (default: %(default)s)")
    deploy_parser.add_argument("--target", metavar='TARGET_FOLDER',
                               default="/opt/mirantis/agent",
                               help="Path to deploy agent to on target nodes (default: %(default)s)")

    status_parser = subparsers.add_parser('status', help='Show daemons statuses')
    stop_parser = subparsers.add_parser('stop', help='Stop daemons')
    start_parser = subparsers.add_parser('start', help='Start daemons')
    remove_parser = subparsers.add_parser('uninstall', help='Remove service')

    for sbp in (deploy_parser, start_parser, stop_parser, status_parser, remove_parser):
        sbp.add_argument("inventory", metavar='FILE',
                         help="Path to file with list of ssh ip/names of ceph nodes")
        sbp.add_argument("--ssh-user", metavar='SSH_USER',
                         default=getpass.getuser(),
                         help="SSH user, (default: %(default)s)")
        sbp.add_argument("--certs-folder", metavar='DIR',
                         default=str(LOCAL_INSTALL_PATH / "agent_client_keys"),
                         help="Folder to store/read API keys and certificates, (default: %(default)s)")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    inventory = read_inventory(opts.inventory)
    nodes = [SSH(name_or_ip, ssh_user=opts.ssh_user) for name_or_ip in inventory]

    if opts.subparser_name == 'install':
        clear_arch = False

        try:
            asyncio.run(deploy(nodes, opts.arch,
                               max_parallel_uploads=opts.max_parallel_uploads,
                               target_folder=Path(opts.target),
                               certs_folder=Path(opts.certs_folder)))
        finally:
            if clear_arch:
                os.unlink(opts.arch)
    elif opts.subparser_name == 'status':
        asyncio.run(status(nodes, certs_folder=Path(opts.certs_folder)))
    elif opts.subparser_name == 'start':
        asyncio.run(start(nodes))
    elif opts.subparser_name == 'uninstall':
        asyncio.run(remove(nodes))
    else:
        assert opts.subparser_name == 'stop', f"Unknown command {opts.subparser_name}"
        asyncio.run(stop(nodes))
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
