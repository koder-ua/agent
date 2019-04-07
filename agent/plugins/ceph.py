import os
import re
import sys
import json
import time
import glob
import gzip
import socket
import pprint
import asyncio
import os.path
import logging
import datetime
import functools
import subprocess
import collections
from typing import Iterator, TextIO, List, Dict, Tuple, Any, Callable, Set, cast, BinaryIO, Iterable, Coroutine
from functools import partial
from collections import defaultdict

from koder_utils import run_stdout, open_to_append

from cephlib.commands import get_local_osds, set_size_duration, get_historic
from cephlib.historic_ops import RecId, CephOp, ParseResult, RecordFile, get_packer, HEADER_LAST_NAME, HEADER_LAST
from cephlib.classes import CephHealth

from . import expose_func, expose_func_async


expose = functools.partial(expose_func, "ceph")
expose_async = functools.partial(expose_func_async, "ceph")

logger = logging.getLogger("agent.ceph")


def iter_ceph_logs_fd() -> Iterator[TextIO]:
    all_files = []
    for name in glob.glob("/var/log/ceph/ceph.log*"):
        if name == '/var/log/ceph/ceph.log':
            all_files.append((0, open(name, 'r')))
        else:
            rr = re.match(r"/var/log/ceph/ceph\.log\.(\d+)\.gz$", name)
            if rr:
                all_files.append((-int(rr.group(1)), gzip.open(name, mode='rt', encoding='utf8')))

    for _, fd in sorted(all_files):
        yield fd


def iter_log_messages(fd: TextIO) -> Iterator[Tuple[float, CephHealth]]:
    for ln in fd:
        msg = None
        dt, tm, service_name, service_id, addr, uid, _, src, level, message = ln.split(" ", 9)
        if 'overall HEALTH_OK' in message or 'Cluster is now healthy' in message:
            msg = CephHealth.HEALTH_OK
        elif message == 'scrub mismatch':
            msg = CephHealth.SCRUB_MISSMATCH
        elif 'clock skew' in message:
            msg = CephHealth.CLOCK_SKEW
        elif 'marked down' in message:
            msg = CephHealth.OSD_DOWN
        elif 'Reduced data availability' in message:
            msg = CephHealth.REDUCED_AVAIL
        elif 'Degraded data redundancy' in message:
            msg = CephHealth.DEGRADED
        elif 'no active mgr' in message:
            msg = CephHealth.NO_ACTIVE_MGR
        elif "slow requests" in message and "included below" in message:
            msg = CephHealth.SLOW_REQUESTS
        elif 'calling monitor election' in message:
            msg = CephHealth.MON_ELECTION

        if msg is not None:
            y_month_day = dt.split("-")
            h_m_s = tm.split('.')[0].split(":")
            date = datetime.datetime(*map(int, y_month_day + h_m_s))
            yield time.mktime(date.timetuple()), msg


def almost_sorted_ceph_log_messages(sort_buffer_size: int) -> Iterator[Tuple[float, CephHealth]]:
    all_messages: List[Tuple[float, CephHealth]] = []
    for fd in iter_ceph_logs_fd():
        for message in iter_log_messages(fd):
            all_messages.append(message)
            if len(all_messages) > sort_buffer_size:
                all_messages.sort()
                yield all_messages[:sort_buffer_size // 2]
                del all_messages[:sort_buffer_size // 2]
    yield all_messages


@expose
def find_issues_in_ceph_log(max_lines: int = 100000, max_issues: int = 100) -> str:
    errs_warns = []
    for idx, ln in enumerate(open("/var/log/ceph/ceph.log")):
        if idx == max_lines:
            break
        if 'cluster [ERR]' in ln or "cluster [WRN]" in ln:
            errs_warns.append(ln)
            if len(errs_warns) == max_issues:
                break
    return "".join(errs_warns[-max_lines:])


@expose
def analyze_ceph_logs_for_issues(sort_buffer_size: int = 10000) \
        -> Tuple[Dict[str, int], List[Tuple[bool, float, float]]]:

    error_per_type = collections.Counter()
    status_ranges = []
    currently_healthy = None
    region_started_at = None

    utc = None
    for all_messages in almost_sorted_ceph_log_messages(sort_buffer_size):
        for utc, mess_id in all_messages:
            if region_started_at is None:
                region_started_at = utc
                currently_healthy = mess_id == CephHealth.HEALTH_OK
                continue

            if mess_id != CephHealth.HEALTH_OK:
                error_per_type[mess_id] += 1
                if currently_healthy:
                    status_ranges.append((True, region_started_at, utc))
                    region_started_at = utc
                    currently_healthy = False
            elif not currently_healthy:
                status_ranges.append((False, region_started_at, utc))
                region_started_at = utc
                currently_healthy = True

    if utc and utc != region_started_at:
        status_ranges.append((currently_healthy, region_started_at, utc))

    return {key.name: val for key, val in error_per_type.items()}, status_ranges


class NoPoolFound(Exception):
    pass


RADOS_DF = 'rados df -f json'
PG_DUMP = 'ceph pg dump -f json 2>/dev/null'
CEPH_DF = 'ceph df -f json'
CEPH_S = 'ceph -s -f json'


FileRec = Tuple[RecId, Any]
BinaryFileRec = Tuple[RecId, bytes]
BinInfoFunc = Callable[[], Coroutine[Any, Any, Iterable[FileRec]]]


async def dump_cluster_info(commands: List[str], timeout: float = 15) -> List[FileRec]:
    """
    make a message with provided cmd outputs
    """
    output = {'time': int(time.time())}
    for cmd in commands:
        try:
            output[cmd] = json.loads(await run_stdout(cmd, timeout=timeout))
        except subprocess.SubprocessError:
            pass
    if output:
        return [(RecId.cluster_info, output)]

    return []


class CephDumper:
    def __init__(self, osd_ids: Set[int], size: int, duration: int, cmd_tout: int = 15, min_diration: int = 0,
                 dump_unparsed_headers: bool = False) -> None:
        self.osd_ids = osd_ids
        self.not_inited_osd = osd_ids.copy()
        self.pools_map: Dict[int, Tuple[str, int]] = {}
        self.pools_map_no_name: Dict[int, int] = {}
        self.size = size
        self.duration = duration
        self.cmd_tout = cmd_tout
        self.min_duration = min_diration
        self.last_time_ops: Dict[int, Set[str]] = defaultdict(set)
        self.first_cycle = True
        self.dump_unparsed_headers = dump_unparsed_headers

    async def reload_pools(self) -> bool:
        data = json.loads(await run_stdout("ceph osd lspools -f json", timeout=self.cmd_tout))

        new_pools_map = {}
        for idx, pool in enumerate(sorted(data, key=lambda x: x['poolname'])):
            new_pools_map[pool['poolnum']] = (pool['poolname'], idx)

        if new_pools_map != self.pools_map:
            self.pools_map = new_pools_map
            self.pools_map_no_name = {num: idx for num, (_, idx) in new_pools_map.items()}
            return True
        return False

    async def dump_historic(self) -> List[FileRec]:
        try:
            if self.not_inited_osd:
                self.not_inited_osd = await set_size_duration(self.not_inited_osd, self.size, self.duration,
                                                              timeout=self.cmd_tout)

            ctime = int(time.time())
            osd_ops: Dict[int, str] = {}

            for osd_id in self.osd_ids:
                if osd_id not in self.not_inited_osd:
                    try:
                        osd_ops[osd_id] = await get_historic(osd_id)
                        # data = get_historic_fast(osd_id)
                    except (subprocess.CalledProcessError, OSError):
                        self.not_inited_osd.add(osd_id)
                        continue

            result: List[FileRec] = []
            if await self.reload_pools():
                if self.first_cycle:
                    result.append((RecId.pools, self.pools_map))
                else:
                    # pools updated - skip this cycle, as different ops may came from pools before and after update
                    return []

            for osd_id, data in osd_ops.items():
                try:
                    parsed = json.loads(data)
                except Exception:
                    raise Exception(repr(data))

                if self.size != parsed['size'] or self.duration != parsed['duration']:
                    self.not_inited_osd.add(osd_id)
                    continue

                ops = []
                for op in parsed['ops']:
                    if self.min_duration and int(op.get('duration') * 1000) < self.min_duration:
                        continue
                    try:
                        parse_res, ceph_op = CephOp.parse_op(op)
                        if ceph_op:
                            ops.append(ceph_op)
                        elif parse_res == ParseResult.unknown:
                            logger.info("UNKNOWN: %s", op['description'])
                    except Exception:
                        parse_res = ParseResult.failed

                    if parse_res == ParseResult.failed:
                        logger.exception("Failed to parse op: {}".format(pprint.pformat(op)))

                ops = [op for op in ops if op.tp is not None and op.description not in self.last_time_ops[osd_id]]
                if self.min_duration:
                    ops = [op for op in ops if op.duration >= self.min_duration]
                self.last_time_ops[osd_id] = {op.description for op in ops}

                for op in ops:
                    assert op.pack_pool_id is None
                    op.pack_pool_id = self.pools_map_no_name[op.pool_id]

                result.append((RecId.ops, (osd_id, ctime, ops)))

            return result
        except Exception:
            logger.exception("In dump_historic")


def dict2str_helper(dct: Dict[str, Any], prefix: str) -> List[str]:
    res = []
    for k, v in dct.items():
        assert isinstance(k, str)
        if isinstance(v, dict):
            res.extend(dict2str_helper(v, prefix + k + "::"))
        else:
            res.append("{}{} {}".format(prefix, k, v))
    return res


def dict2str(dct: Dict[str, Any]) -> str:
    return "\n".join(dict2str_helper(dct, ""))


class DumpLoop:
    sigusr_requested_handlers = ["cluster_info", "pg_dump"]

    def __init__(self, opts: Any, osd_ids: Set[int], fd: RecordFile,
                 checker: Callable[[int], bool]) -> None:
        self.opts = opts
        self.osd_ids = osd_ids
        self.fd = fd
        self.packer = get_packer(opts.packer)
        self.checker = checker

        # name => (func, timout, next_call)
        self.handlers: Dict[str, Tuple[BinInfoFunc, float, float]] = {}
        self.fill_handlers()

        self.running_handlers: Set[str] = set()
        self.server = None
        self.status: Dict[str, Any] = {'last_handler_run_at': {}}

    async def close(self):
        if self.server:
            self.server.close()
            await asyncio.create_task(self.server.wait_closed())
            self.server = None

    async def handle_conn(self, reader: asyncio.StreamReader, writer):
        addr = writer.get_extra_info('peername')
        logger.debug("Get new conn from %s", addr)
        cmd = (await reader.readline()).decode('utf8').strip()
        logger.debug("Get cmd %s from %s", cmd, addr)
        if cmd == 'info':
            data = self.status.copy()
            data.update({"error": 0, "message": "Success"})
            writer.write(json.dumps(data).encode('utf8'))
            await writer.drain()
            logger.debug("Successfully send respond to %s", addr)
        else:
            logger.warning("Unknown cmd %s from %s", cmd, addr)
            data = json.dumps({"error": 1, "message": "Unknown cmd {!r}".format(cmd)})
            writer.write(data.encode('utf8'))
            await writer.drain()

        writer.close()

    async def start(self):
        for name in self.handlers:
            self.start_handler(name, repeat=True)

    async def check_loop(self):
        while True:
            await asyncio.sleep(60)
            if not self.checker(self.fd.tell()):
                break

    def fill_handlers(self) -> None:
        ctime = time.time()
        if self.opts.record_cluster != 0:
            func = partial(dump_cluster_info, (RADOS_DF, CEPH_DF, CEPH_S), self.opts.timeout)
            self.handlers["cluster_info"] = func, self.opts.record_cluster, ctime

        if self.opts.record_pg_dump != 0:
            func = partial(dump_cluster_info, (PG_DUMP,), self.opts.timeout)
            self.handlers["pg_dump"] = func, self.opts.record_pg_dump, ctime

        dumper = CephDumper(self.osd_ids, self.opts.size, self.opts.duration,
                            self.opts.timeout, self.opts.min_duration, self.opts.dump_unparsed_headers)
        self.handlers["historic"] = dumper.dump_historic, self.opts.duration, ctime

    def start_handler(self, name: str, repeat: bool = False):
        asyncio.create_task(self.run_handler(name, repeat))

    async def run_handler(self, name: str, repeat: bool = False, one_short_wait: int = 60):
        run_handler = True
        if name in self.running_handlers:
            if repeat:
                run_handler = False
            else:
                for i in range(one_short_wait):
                    await asyncio.sleep(1.0)
                    if name not in self.running_handlers:
                        run_handler = True
                        break

        if run_handler:
            handler, _, _ = self.handlers[name]
            self.running_handlers.add(name)
            data = await handler()
            self.running_handlers.remove(name)
            self.status['last_handler_run_at'][name] = int(time.time())

            for rec_id, packed in self.packer.pack_iter(data):
                logger.debug("Handler %s provides %s bytes of data of type %s", name, len(packed), rec_id)
                self.fd.write_record(rec_id, packed)

        if repeat:
            handler, tout, next_time = self.handlers[name]
            next_time += tout
            curr_time = time.time()
            sleep_time = next_time - curr_time

            if sleep_time <= 0:
                delta = (int(-sleep_time / tout) + 1) * tout
                next_time += delta
                sleep_time += delta

            assert sleep_time > 0
            self.handlers[name] = handler, tout, next_time
            asyncio.get_event_loop().call_later(sleep_time, self.start_handler, name, True)

    def sigusr1_handler(self) -> None:
        logger.info("Get SIGUSR1, will dump data")
        for name in self.sigusr_requested_handlers:
            if name in self.handlers:
                self.start_handler(name)


async def record_to_file(opts: Any) -> int:
    logger.info("Start recording with opts = %s", " ".join(sys.argv))
    params = {'packer': opts.packer, 'cmd': sys.argv,
              'node': [socket.gethostname(), socket.getfqdn()],
              'time': time.time(),
              'date': str(datetime.datetime.now()),
              'tz_offset': time.localtime().tm_gmtoff}

    checker = make_checker(max_file_sz_mb=opts.record_file_size_max_mb,
                           max_record_hours=opts.record_max_hours,
                           min_disk_free_gb=opts.min_free_disk_space_gb,
                           record_file_path=opts.output_file)
    try:
        osd_ids = await get_local_osds()
        logger.info("osds = %s", osd_ids)

        with cast(BinaryIO, open_to_append(opts.output_file, True)) as os_fd:
            os_fd.seek(0, os.SEEK_SET)
            fd = RecordFile(os_fd, pack_each=opts.compress_each * 1024)
            header = fd.seek_to_last_valid_record()

            if header is None:
                os_fd.seek(0, os.SEEK_SET)
                os_fd.write(HEADER_LAST)
            else:
                assert header == HEADER_LAST, "Can only append to file with {} version".format(HEADER_LAST_NAME)

            fd.write_record(RecId.params, json.dumps(params).encode("utf8"))

            dl = DumpLoop(opts, osd_ids, fd, checker)
            await dl.start()
            try:
                while True:
                    await asyncio.sleep(0.1)
            finally:
                # await dl.close()
                raise

    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as exc:
        logger.exception("During recording")
        if isinstance(exc, OSError):
            return exc.errno
        return 1
    return 0


def make_checker(max_file_sz_mb: int,
                 max_record_hours: int,
                 min_disk_free_gb: int,
                 record_file_path: str) -> Callable[[int], bool]:

    record_stop_time = time.time() + max_record_hours * 60 * 60
    max_file_size_bts = max_file_sz_mb * 2 ** 20
    min_disk_free_bts = min_disk_free_gb * 2 ** 30

    def checker(file_size: int) -> bool:
        disk_free = os.statvfs(record_file_path).f_bfree
        if disk_free <= min_disk_free_bts:
            logger.warning("Stop recording due to disk free space %sGiB less then minimal %sGiB",
                           disk_free // 2 ** 30, min_disk_free_gb)
            return False

        if time.time() >= record_stop_time:
            logger.warning("Stop recording due record time expired")
            return False

        if file_size >= max_file_size_bts:
            logger.warning("Stop recording due to record file too large %sMiB, while %sMiB is a limit",
                           file_size // 2 ** 20, max_file_sz_mb)
            return False
        return True

    return checker


@expose
def start_historic_collection():
    pass


@expose
def stop_historic_collection():
    pass


@expose
def get_historic_collection_status():
    pass


@expose
def get_collected_historic_data():
    pass
