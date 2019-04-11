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


def get_key_enc() -> Tuple[str, str]:
    key = "".join((f"{i:02X}" for i in ssl.RAND_bytes(16)))
    return key, encrypt_key(key)


def encrypt_key(key: str, salt: str = None) -> str:
    if salt is None:
        salt = "".join(f"{i:02X}" for i in ssl.RAND_bytes(16))
    return hashlib.sha512(key.encode('utf-8') + salt.encode('utf8')).hexdigest() + "::" + salt


def get_certificates(certs_folder: Path, cert_name_template: str) -> Dict[str, Path]:
    certificates: Dict[str, Path] = {}

    if not certs_folder.is_dir():
        raise RuntimeError(f"Can't find cert folder at {certs_folder}")

    before_node, after_node = cert_name_template.split("[node]")

    for file in certs_folder.glob(cert_name_template.replace('[node]', '*')):
        node_name = file.name[len(before_node): -len(after_node)]
        certificates[node_name] = file

    return certificates


def get_config_default_path() -> Path:
    return AGENT_INSTALL_PATH / 'config.cfg'


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
    ssl_cert_templ: str
    max_conn: int
    arch_file: Path
    raw: configparser.ConfigParser
    rraw: Any


def get_config(path: Path = None) -> AgentConfig:
    cfg = configparser.ConfigParser()
    if path:
        cfg.read_file(path.open())
    else:
        cfg.read(get_config_default_path())

    rcfg = RAttredDict(cfg)

    path_formatters: Dict[str, str] = {}
    for name, val in [('root', rcfg.common.root), ('secrets', rcfg.common.secrets), ('storage', rcfg.server.secrets)]:
        path_formatters[name] = val.format(**path_formatters)

    def mkpath(val: str) -> Path:
        return Path(val.format(**path_formatters))

    if getattr(rcfg.server, "persistent_log", None):
        persistent_log = mkpath(rcfg.server.persistent_log)
        persistent_log_level = rcfg.server.persistent_log_level
    else:
        persistent_log = None
        persistent_log_level = None

    return AgentConfig(
        root=Path(path_formatters['root']),
        secrets=Path(path_formatters['secrets']),
        log_config=mkpath(rcfg.common.log_config),
        server_port=int(rcfg.common.server_port),
        log_level=rcfg.common.log_level,
        config=mkpath(rcfg.common.config),
        cmd_timeout=int(rcfg.common.cmd_timeout),
        storage=mkpath(rcfg.server.storage),
        persistent_log=persistent_log,
        persistent_log_level=persistent_log_level,
        listen_ip=rcfg.server.listen_ip,
        service_name=rcfg.server.service,
        service=mkpath(rcfg.server.service),
        ssl_cert=mkpath(rcfg.server.ssl_cert),
        ssl_key=mkpath(rcfg.server.ssl_key),
        api_key_enc=mkpath(rcfg.server.api_key_enc),
        historic_ops=mkpath(rcfg.server.historic_ops),
        inventory=mkpath(rcfg.client.inventory),
        api_key=mkpath(rcfg.client.api_key),
        ssl_cert_templ=rcfg.client.ssl_cert_templ.format(**path_formatters),
        max_conn=int(rcfg.client.max_conn),
        arch_file=mkpath(rcfg.deploy.arch_file),
        raw=cfg,
        rraw=rcfg
    )


def config_logging(cfg: Any):
    log_config = json.load(cfg.log_config.open())

    if not cfg.persistent_log:
        del log_config['handlers']['persistent']
    else:
        log_config['handlers']['persistent']['level'] = cfg.persistent_level
        log_config['handlers']['persistent']['filename'] = str(cfg.persistent_log)
        for lcfg in log_config['loggers'].values():
            lcfg['handlers'].append('persistent')

    log_config['handlers']['console']['level'] = cfg.log_level
    logging.config.dictConfig(log_config)