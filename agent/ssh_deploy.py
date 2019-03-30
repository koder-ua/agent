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
import tempfile
import subprocess
import urllib.request
from pathlib import Path
from typing.io import BinaryIO
from typing import List, Union, NamedTuple, Tuple, Any

import aiohttp

from . import utils


SERVICE_NAME = "mirantis_agent.service"
AGENT_DATA_PATH = "/var/mirantis/agent"
INSTALL_PATH = Path(__file__).resolve().parent.parent


DEFAULT_PORT = 55443

DEFAULT_OPTS = ("-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "ConnectionAttempts=3",
                "-o", "ConnectTimeout=10",
                "-o", "LogLevel=ERROR")


class CMDResult(NamedTuple):
    cmd: Union[str, List[str]]
    out_b: bytes
    err_b: bytes

    def out(self) -> str:
        return self.out_b.decode("utf8")


class SSH:
    def __init__(self, node: str, ssh_user: str, ssh_opts: Tuple[str, ...] = DEFAULT_OPTS) -> None:
        self.node = node
        self.ssh_opts = list(ssh_opts)
        self.ssh_user = ssh_user
        self.cmd_prefix = ["ssh"] + self.ssh_opts + [self.ssh_user + "@" + self.node] + ['--']
        self.cmd_prefix_s = " ".join(self.cmd_prefix) + " "

    async def run(self, cmd: Union[str, List[str]], *args, **kwargs) -> CMDResult:
        cmd = (self.cmd_prefix if isinstance(cmd, list) else self.cmd_prefix_s) + cmd
        return await utils.run(cmd, *args, **kwargs)

    async def scp(self, source: str, target: str, timeout: int = 60):
        cmd = ["scp", *self.ssh_opts, source, f"{self.ssh_user}@{self.node}:{target}"]
        await utils.run(cmd, timeout=timeout)


def make_secure(*files: Path):
    for fl in files:
        if fl.exists():
            fl.unlink()
        os.close(os.open(str(fl), os.O_WRONLY | os.O_CREAT, 0o600))


async def make_cert_and_key(key_file: Path, cert_file: Path, subj: str):
    await utils.run(f"openssl genrsa 1024 2>/dev/null > {key_file}")
    cmd = f'openssl req -new -x509 -nodes -sha1 -days 365 -key "{key_file}" -subj "{subj}" > {cert_file} 2>/dev/null'
    await utils.run(cmd)


def read_inventory(path: str) -> List[str]:
    names = [name_or_ip.strip() for name_or_ip in open(path)]
    return [name_or_ip for name_or_ip in names if name_or_ip and not name_or_ip.startswith("#")]


async def stop(nodes: List[SSH]):
    await asyncio.gather(*[node.run(f"sudo systemctl stop {SERVICE_NAME}") for node in nodes])


async def disable(nodes: List[SSH]):
    await asyncio.gather(*[node.run(f"sudo systemctl disable {SERVICE_NAME}") for node in nodes])


async def start(nodes: List[SSH]):
    await asyncio.gather(*[node.run(f"sudo systemctl start {SERVICE_NAME}") for node in nodes])


async def status(nodes: List[SSH]):
    from .client import AsyncRPCClient, RPCServerFailure
    access_key = (INSTALL_PATH / 'agent_client_keys/agent_api.key').open().read()

    async def check_node(node: SSH) -> Tuple[str, bool]:
        ssl_cert_file = INSTALL_PATH / 'agent_client_keys/agent_server.{node.node}.cert'
        client = AsyncRPCClient(f"https://{node.node}:{DEFAULT_PORT}",
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
            agent_folder = target_path / "agent"
            service_target = Path("/lib/systemd/system") / SERVICE_NAME

            await node.run(["sudo", "rm", "--force", service_target])
            await node.run(["sudo", "systemctl", "daemon-reload"])

            for folder in (agent_folder, AGENT_DATA_PATH):
                assert re.match(r"/[a-zA-Z0-9-_]+/mirantis/agent$", str(folder)), \
                    f"{folder} not match re of allowed to rm path"
                await node.run(["sudo", "rm", "--preserve-root", "--recursive", "--force", folder])
        except Exception as exc:
            exc.node = node
            raise

    for val in await asyncio.gather(*map(runner, nodes), return_exceptions=True):
        if val is not None:
            assert isinstance(val, Exception)
            print(f"Failed on node {val.node.node} with message: {val!s}")


def get_key_enc() -> Tuple[str, str]:
    key = "".join(("{:02X}".format(i) for i in ssl.RAND_bytes(16)))
    return key, encrypt_key(key)


def encrypt_key(key: str, salt: str = None) -> str:
    if salt is None:
        salt = "".join("{:02X}".format(i) for i in ssl.RAND_bytes(16))
    return hashlib.sha512(key.encode('utf-8') + salt.encode('utf8')).hexdigest() + "::" + salt


async def deploy(nodes: List[SSH], arch_file: str, max_parallel_uploads: int, target_folder: str,
                 local_target: str):
    target_path = Path(target_folder)

    upload_semaphore = asyncio.Semaphore(max_parallel_uploads if max_parallel_uploads else len(nodes))

    local_dir = Path(local_target)
    local_dir.mkdir(mode=0o770, parents=True, exist_ok=True)

    api_key_file = local_dir / "agent_api.key"
    api_enc_key_file = local_dir / "agent_api_key.enc"

    make_secure(api_key_file, api_enc_key_file)
    api_key, api_enc_key = get_key_enc()

    with open(api_key_file, 'w') as fd:
        fd.write(api_key)

    with open(api_enc_key_file, 'w') as fd:
        fd.write(api_enc_key)

    async def runner(node: SSH):
        agent_folder = target_path / "agent"
        certs_folder = agent_folder / "certs"
        service_file = agent_folder / SERVICE_NAME
        service_target = "/lib/systemd/system"

        await node.run(["sudo", "mkdir", "--parents", agent_folder])
        await node.run(["sudo", "mkdir", "--parents", AGENT_DATA_PATH])
        assert arch_file.endswith(".tar.gz")

        temp_arch_file = f"/tmp/mirantis_agent_{uuid.uuid1()!s}.tar.gz"

        async with upload_semaphore:
            await node.scp(arch_file, temp_arch_file)

        await node.run(["sudo", "tar", "--extract", "--directory=" + str(agent_folder), "--file", temp_arch_file])
        await node.run(["sudo", "chown", "--recursive", "root.root", agent_folder])
        await node.run(["sudo", "chmod", "--recursive", "o-w", agent_folder])
        await node.run(["sudo", "mkdir", "--parents", str(certs_folder)])

        await node.run(["sudo", "mkdir", "--parents", str(certs_folder)])

        ssl_cert_file = local_dir / f"agent_server.{node.node}.cert"
        ssl_key_file = local_dir / f"agent_server.{node.node}.key"
        make_secure(ssl_cert_file, ssl_key_file)

        await make_cert_and_key(ssl_key_file, ssl_cert_file,
                                f"/C=NN/ST=Some/L=Some/O=agent/OU=agent/CN={node.node}")

        ssl_cert = ssl_cert_file.open("rb").read()
        await node.run(["sudo", "tee", str(certs_folder / "ssl_cert.cert")], input_data=ssl_cert)

        ssl_key = ssl_key_file.open("rb").read()
        await node.run(["sudo", "tee", str(certs_folder / "ssl_cert.key")], input_data=ssl_key)

        await node.run(["sudo", "tee", str(certs_folder / "api.key")], input_data=api_enc_key.encode("utf8"))

        await node.run(["rm", temp_arch_file])
        await node.run(["sudo", "ln", "--symbolic", service_file, service_target])
        await node.run(["sudo", "systemctl", "daemon-reload"])

    await asyncio.gather(*map(runner, nodes))
    await start(nodes)
    await asyncio.sleep(1)
    await status(nodes)


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest='subparser_name')

    deploy = subparsers.add_parser('deploy', help='Deploy')
    deploy.add_argument("--max-parallel-uploads", default=0, type=int,
                        help="Max parallel archive uploads to target nodes (default: %(default)s)")
    deploy.add_argument("--arch", metavar='ARCH_FILE',
                        default=str(INSTALL_PATH / 'distribution.tar.gz'),
                        help="Path to file with agent archive (default: %(default)s)")
    deploy.add_argument("--target", metavar='TARGET_FOLDER',
                        default="/opt/mirantis",
                        help="Path to deploy agent to on target nodes (default: %(default)s)")
    deploy.add_argument("--local-target", metavar='CERT_FILE_FOLDER',
                        default=str(INSTALL_PATH / "agent_client_keys"),
                        help="Folder to store agent certificates & api key (default: %(default)s)")

    status = subparsers.add_parser('status', help='Show daemons statuses')
    stop = subparsers.add_parser('stop', help='Stop daemons')
    start = subparsers.add_parser('start', help='Start daemons')
    remove = subparsers.add_parser('remove', help='Remove service')

    for sbp in (deploy, start, stop, status, remove):
        sbp.add_argument("inventory", metavar='FILE',
                         help="Path to file with list of ssh ip/names of ceph nodes")
        sbp.add_argument("--ssh-user", metavar='SSH_USER',
                         default=getpass.getuser(),
                         help="Path to file with agent archive")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    inventory = read_inventory(opts.inventory)
    nodes = [SSH(name_or_ip, ssh_user=opts.ssh_user) for name_or_ip in inventory]

    if opts.subparser_name == 'deploy':
        clear_arch = False
        if opts.arch.startswith("http://") or opts.arch.startswith("https://"):
            fd, arch = tempfile.mkstemp(prefix="agent_arch_", suffix=".tar.gz")
            os.close(fd)
            urllib.request.urlretrieve(opts.arch, arch)
            clear_arch = True
        else:
            arch = opts.arch

        try:
            asyncio.run(deploy(nodes, arch, max_parallel_uploads=opts.max_parallel_uploads,
                               target_folder=opts.target,
                               local_target=opts.local_target))
        finally:
            if clear_arch:
                os.unlink(arch)
    elif opts.subparser_name == 'status':
        asyncio.run(status(nodes))
    elif opts.subparser_name == 'start':
        asyncio.run(start(nodes))
    elif opts.subparser_name == 'remove':
        asyncio.run(remove(nodes))
    else:
        assert opts.subparser_name == 'stop', f"Unknown command {opts.subparser_name}"
        asyncio.run(stop(nodes))
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
