import logging
import re
import sys
import uuid
import asyncio
import getpass
import argparse
import subprocess
from pathlib import Path
from typing import List, Any


from koder_utils import SSH, read_inventory, make_secure, make_cert_and_key, rpc_map, b2ssize

from . import AsyncRPCClient, get_connection_pool_cfg
from .common import get_key_enc, get_config, get_config_default_path, AgentConfig, config_logging


logger = logging.getLogger("ctl")
LOCAL_INSTALL_PATH = Path(__file__).resolve().parent.parent
SERVICE_FILE_DIR = Path("/lib/systemd/system")


# --------------- SSH BASED CONTROLS FUNCTIONS -------------------------------------------------------------------------


async def stop(service: str, nodes: List[SSH]) -> None:
    logger.info(f"Stopping service {service} on nodes {' '.join(node.node for node in nodes)}")
    await asyncio.gather(*[node.run(["sudo", "systemctl", "stop", service]) for node in nodes])


async def disable(service: str, nodes: List[SSH]) -> None:
    logger.info(f"Disabling service {service} on nodes {' '.join(node.node for node in nodes)}")
    await asyncio.gather(*[node.run(["sudo", "systemctl", "disable", service]) for node in nodes])


async def enable(service: str, nodes: List[SSH]) -> None:
    logger.info(f"Enabling service {service} on nodes {' '.join(node.node for node in nodes)}")
    await asyncio.gather(*[node.run(["sudo", "systemctl", "enable", service]) for node in nodes])


async def start(service: str, nodes: List[SSH]) -> None:
    logger.info(f"Starting service {service} on nodes {' '.join(node.node for node in nodes)}")
    await asyncio.gather(*[node.run(["sudo", "systemctl", "start", service]) for node in nodes])


async def remove(cfg: Any, nodes: List[SSH]):
    logger.info(f"Removing rpc_agent from nodes {' '.join(node.node for node in nodes)}")

    try:
        await disable(cfg.server.service, nodes)
    except subprocess.SubprocessError:
        pass

    try:
        await stop(cfg.server.service, nodes)
    except subprocess.SubprocessError:
        pass

    async def runner(node: SSH) -> None:
        agent_folder = Path(cfg.root)
        service_target = SERVICE_FILE_DIR / cfg.server.service

        await node.run(["sudo", "rm", "--force", str(service_target)])
        await node.run(["sudo", "systemctl", "daemon-reload"])

        for folder in (agent_folder, cfg.server.storage):
            assert re.match(r"/[a-zA-Z0-9-_]+/rpc_agent$", str(folder)), \
                f"{folder} not match re of allowed to rm path"
            await node.run(["sudo", "rm", "--preserve-root", "--recursive", "--force", str(folder)])

    for node, val in zip(nodes, await asyncio.gather(*map(runner, nodes), return_exceptions=True)):
        if val is not None:
            assert isinstance(val, Exception)
            logger.error(f"Failed on node {node} with message: {val!s}")


async def deploy(cfg: AgentConfig, nodes: List[SSH], max_parallel_uploads: int, inventory: List[str]):
    logger.info(f"Start deploying on nodes: {' '.join(inventory)}")

    upload_semaphore = asyncio.Semaphore(max_parallel_uploads if max_parallel_uploads else len(nodes))

    if max_parallel_uploads:
        logger.debug(f"Max uploads is set to {max_parallel_uploads}")

    cfg.secrets.mkdir(mode=0o770, parents=True, exist_ok=True)

    make_secure(cfg.api_key, cfg.api_key_enc)
    api_key, api_enc_key = get_key_enc()

    with cfg.api_key.open('w') as fd:
        fd.write(api_key)

    with cfg.api_key_enc.open('w') as fd:
        fd.write(api_enc_key)

    logger.debug(f"Api keys generated")

    async def runner(node: SSH):
        logger.debug(f"Start deploying node {node.node}")

        await node.run(["sudo", "mkdir", "--parents", cfg.root])
        await node.run(["sudo", "mkdir", "--parents", cfg.storage])
        assert str(cfg.arch_file).endswith(".tar.gz")

        temp_arch_file = f"/tmp/rpc_agent_{uuid.uuid1()!s}.tar.gz"

        logger.debug(f"Copying {b2ssize(cfg.arch_file.stat().st_size)} bytes of archive to {node.node}")
        async with upload_semaphore:
            await node.copy(cfg.arch_file, temp_arch_file)

        logger.debug(f"Untaring and makeing dirs on {node.node}")
        await node.run(["sudo", "tar", "--extract", "--directory=" + str(cfg.root), "--file", temp_arch_file])
        await node.run(["sudo", "chown", "--recursive", "root.root", cfg.root])
        await node.run(["sudo", "chmod", "--recursive", "o-w", cfg.root])
        await node.run(["sudo", "mkdir", "--parents", cfg.secrets])

        logger.debug(f"Generating certs for {node.node}")
        ssl_cert_file = Path(str(cfg.ssl_cert_templ).replace("[node]", node.node))
        ssl_key_file = cfg.secrets / f'key.{node.node}.tempo'
        make_secure(ssl_cert_file, ssl_key_file)

        await make_cert_and_key(ssl_key_file, ssl_cert_file,
                                f"/C=NN/ST=Some/L=Some/O=agent/OU=agent/CN={node.node}")

        logger.debug(f"Copying certs and keys to {node.node}")
        await node.run(["sudo", "tee", cfg.ssl_cert], input_data=ssl_cert_file.open("rb").read())
        await node.run(["sudo", "tee", cfg.ssl_key], input_data=ssl_key_file.open("rb").read())
        await node.run(["sudo", "tee", cfg.api_key_enc], input_data=api_enc_key.encode("utf8"))
        ssl_key_file.unlink()
        await node.run(["rm", temp_arch_file])

        logger.debug(f"Copying service file to {node.node}")
        service_content = cfg.service.open().read()
        service_content = service_content.replace("{INSTALL}", str(cfg.root))
        service_content = service_content.replace("{CONFIG_PATH}", str(cfg.config))

        await node.run(["sudo", "tee", f"/lib/systemd/system/{cfg.service_name}"],
                       input_data=service_content.encode())
        await node.run(["sudo", "systemctl", "daemon-reload"])
        logger.debug(f"Done with {node.node}")

    await asyncio.gather(*map(runner, nodes))
    await enable(cfg.service_name, nodes)
    await start(cfg.service_name, nodes)

    if not cfg.inventory.exists() and inventory:
        with cfg.inventory.open("w") as fd:
            fd.write("\n".join(inventory))


# --------------- RPC BASED CONTROLS FUNCTIONS -------------------------------------------------------------------------


async def check_node(conn: AsyncRPCClient) -> bool:
    return await conn.conn.sys.ping(_call_timeout=5)


async def status(cfg: Any, nodes: List[str]) -> None:
    async with get_connection_pool_cfg(cfg) as pool:
        max_node_name_len = max(map(len, nodes))
        async for node_name, res in rpc_map(pool, check_node, nodes):
            logger.info("{0:>{1}} {2:>8}".format(node_name, max_node_name_len, "RUN" if res else "NOT RUN"))


def parse_args(argv: List[str]) -> Any:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest='subparser_name')

    deploy_parser = subparsers.add_parser('install', help='Deploy agent on nodes from inventory')
    deploy_parser.add_argument("--max-parallel-uploads", default=0, type=int,
                               help="Max parallel archive uploads to target nodes (default: %(default)s)")
    deploy_parser.add_argument("--target", metavar='TARGET_FOLDER',
                               default=LOCAL_INSTALL_PATH,
                               help="Path to deploy agent to on target nodes (default: %(default)s)")

    stop_parser = subparsers.add_parser('stop', help='Stop daemons')
    start_parser = subparsers.add_parser('start', help='Start daemons')
    remove_parser = subparsers.add_parser('uninstall', help='Remove service')

    for sbp in (deploy_parser, start_parser, stop_parser, remove_parser):
        sbp.add_argument("--ssh-user", metavar='SSH_USER',
                         default=getpass.getuser(),
                         help="SSH user, (default: %(default)s)")

    status_parser = subparsers.add_parser('status', help='Show daemons statuses')
    for sbp in (deploy_parser, start_parser, stop_parser, status_parser, remove_parser):
        sbp.add_argument("--inventory", metavar='INVENTORY_FILE', default=None,
                         help="Path to file with list of ssh ip/names of ceph nodes")
        sbp.add_argument("--config", metavar='CONFIG_FILE', default=get_config_default_path(),
                         help="Config file path (default: %(default)s)")

    return parser.parse_args(argv[1:])


def main(argv: List[str]) -> int:
    opts = parse_args(argv)
    cfg = get_config(opts.config)
    config_logging(cfg, no_persistent=True)

    if opts.inventory:
        inventory = read_inventory(opts.inventory)
    else:
        inventory = read_inventory(str(cfg.inventory))

    if opts.subparser_name == 'status':
        asyncio.run(status(cfg, inventory))
        return 0

    nodes = [SSH(name_or_ip, ssh_user=opts.ssh_user) for name_or_ip in inventory]
    if opts.subparser_name == 'install':
        asyncio.run(deploy(cfg, nodes, max_parallel_uploads=opts.max_parallel_uploads, inventory=inventory))
    elif opts.subparser_name == 'start':
        asyncio.run(start(cfg.service_name, nodes))
    elif opts.subparser_name == 'uninstall':
        asyncio.run(remove(cfg.service_name, nodes))
    elif opts.subparser_name == 'uninstall':
        asyncio.run(stop(cfg.service_name, nodes))
    else:
        assert False, f"Unknown command {opts.subparser_name}"
    return 0


if __name__ == "__main__":
    exit(main(sys.argv))
