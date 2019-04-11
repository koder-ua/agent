import configparser
import json
import ssl
import hashlib
import logging.config
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple, Dict, Any, Optional

from koder_utils import RAttredDict

MAX_FILE_SIZE = 1 << 30
USER_NAME = 'rpc_client'
AGENT_INSTALL_PATH = Path(__file__).parent.parent

@dataclass
class AgentConfig:
    root: Path
    secrets: Path
    log_config: Path
    server_port: int
    log_level: str
    config: Path
    cmd_timeout: int
    storage: Path
    persistent_log: Optional[Path]
    persistent_log_level: Optional[str]
    listen_ip: str
    service_name: str
    service: Path
    ssl_cert: Path
    ssl_key: Path
    api_key_enc: Path
    historic_ops: Path
    inventory: Path
    api_key: Path
    ssl_cert_templ: Path
    max_conn: int
    arch_file: Path
    raw: configparser.ConfigParser
    rraw: Any


def get_config_default_path() -> Path:
    return AGENT_INSTALL_PATH / 'config.cfg'


def get_config(path: Path = None) -> AgentConfig:
    cfg = configparser.ConfigParser()
    if path:
        cfg.read_file(path.open())
    else:
        cfg.read(get_config_default_path())

    rcfg = RAttredDict(cfg)

    common = rcfg.common
    server = rcfg.server

    path_formatters: Dict[str, str] = {}
    for name, val in [('root', common.root), ('secrets', common.secrets), ('storage', server.storage)]:
        path_formatters[name] = val.format(**path_formatters)

    def mkpath(val: str) -> Path:
        return Path(val.format(**path_formatters))

    if getattr(server, "persistent_log", None):
        persistent_log = mkpath(server.persistent_log)
        persistent_log_level = server.persistent_log_level
    else:
        persistent_log = None
        persistent_log_level = None

    return AgentConfig(
        root=Path(path_formatters['root']),
        secrets=Path(path_formatters['secrets']),

        log_config=mkpath(common.log_config),
        server_port=int(common.server_port),
        log_level=common.log_level,
        config=mkpath(common.config),
        cmd_timeout=int(common.cmd_timeout),

        storage=mkpath(server.storage),
        persistent_log=persistent_log,
        persistent_log_level=persistent_log_level,
        listen_ip=server.listen_ip,
        service_name=server.service.rsplit("/", 1)[1],
        service=mkpath(server.service),
        ssl_cert=mkpath(server.ssl_cert),
        ssl_key=mkpath(server.ssl_key),
        api_key_enc=mkpath(server.api_key_enc),
        historic_ops=mkpath(server.historic_ops),

        inventory=mkpath(rcfg.client.inventory),
        api_key=mkpath(rcfg.client.api_key),
        ssl_cert_templ=mkpath(rcfg.client.ssl_cert_templ),
        max_conn=int(rcfg.client.max_conn),

        arch_file=mkpath(rcfg.deploy.arch_file),

        raw=cfg,
        rraw=rcfg
    )


def get_key_enc() -> Tuple[str, str]:
    key = "".join((f"{i:02X}" for i in ssl.RAND_bytes(16)))
    return key, encrypt_key(key)


def encrypt_key(key: str, salt: str = None) -> str:
    if salt is None:
        salt = "".join(f"{i:02X}" for i in ssl.RAND_bytes(16))
    return hashlib.sha512(key.encode('utf-8') + salt.encode('utf8')).hexdigest() + "::" + salt


def get_certificates(cert_name_template: Path) -> Dict[str, Path]:
    certificates: Dict[str, Path] = {}

    certs_folder = cert_name_template.parent
    certs_glob = cert_name_template.name

    if not certs_folder.is_dir():
        raise RuntimeError(f"Can't find cert folder at {certs_folder}")

    before_node, after_node = certs_glob.split("[node]")

    for file in certs_folder.glob(certs_glob.replace('[node]', '*')):
        node_name = file.name[len(before_node): -len(after_node)]
        certificates[node_name] = file

    return certificates


def config_logging(cfg: AgentConfig, no_persistent: bool = False):
    log_config = json.load(cfg.log_config.open())

    if not cfg.persistent_log or no_persistent:
        del log_config['handlers']['persistent']
    else:
        log_config['handlers']['persistent']['level'] = cfg.persistent_log_level
        log_config['handlers']['persistent']['filename'] = str(cfg.persistent_log)
        for lcfg in log_config['loggers'].values():
            lcfg['handlers'].append('persistent')

    log_config['handlers']['console']['level'] = cfg.log_level
    logging.config.dictConfig(log_config)

