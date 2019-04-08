import os
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path

import time
import pprint
import asyncio
import os.path
import logging
import functools
import subprocess
import collections
from typing import Iterator, List, Dict, Tuple, Any, Callable, Set, Iterable, Coroutine, Optional, AsyncIterator, \
    Generic, TypeVar
from collections import defaultdict

from cephlib import (RecId, CephCLI, CephOp, ParseResult, RecordFile, CephHealth, iter_log_messages, iter_ceph_logs_fd,
                     CephRelease, OpRec, IPacker, get_historic_packer, get_ceph_version)

from . import expose_func, IReadableAsync, ChunkedFile

expose = functools.partial(expose_func, "ceph")

logger = logging.getLogger("agent.ceph")


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


FileRec = Tuple[RecId, Any]
BinaryFileRec = Tuple[RecId, bytes]
BinInfoFunc = Callable[[], Coroutine[Any, Any, Iterable[FileRec]]]

GiB = 1 << 30
MiB = 1 << 20
DEFAULT_MAX_REC_FILE_SIZE = GiB
DEFAULT_MIN_DEVICE_FREE = 50 * GiB
DEFAULT_SIZE = 20
DEFAULT_DURATION = 600


@dataclass
class CollectionConfig:
    osd_ids: Set[int] = field(default_factory=set)
    size: int = DEFAULT_SIZE
    duration: Optional[int] = None
    min_duration: Optional[int] = 50
    dump_unparsed_headers: bool = False
    pg_dump_timeout: Optional[int] = None
    extra_cmd: List[str] = field(default_factory=list)
    extra_dump_timeout: Optional[int] = None
    max_record_file: int = DEFAULT_MAX_REC_FILE_SIZE
    min_device_free: int = DEFAULT_MIN_DEVICE_FREE
    collection_end_time: Optional[float] = None
    packer_name: str = 'compact'


class LoopCmd(IntEnum):
    reconfig = 1
    exit = 2


Msg = TypeVar('Msg')

# just try for fun, can really use
class TypedQueue(Generic[Msg]):
    def __init__(self):
        self.q = asyncio.Queue()

    def put_nowait(self, val: Msg):
        self.q.put_nowait(val)

    async def get(self) -> Msg:
        return await self.q.get()

    def task_done(self) -> None:
        self.q.task_done()

    async def join(self) -> None:
        await self.q.join()


class CephHistoricDumper:
    def __init__(self, release: CephRelease, record_file_path: Path, cmd_timeout: int = 50) -> None:
        self.not_inited_osd: Set[int] = set()
        self.pools_map: Dict[int, Tuple[str, int]] = {}
        self.pools_map_no_name: Dict[int, int] = {}
        self.last_time_ops: Dict[int, Set[str]] = defaultdict(set)
        self.cli = CephCLI(node=None, extra_params=[], timeout=cmd_timeout, release=release)
        self.collection_config = CollectionConfig(osd_ids=set())
        self.packer: Optional[IPacker] = None

        self.record_file_path = record_file_path
        if not self.record_file_path.exists():
            with self.record_file_path.open("wb"):
                pass

        self.record_fd = self.record_file_path.open("r+b")
        self.record_file = RecordFile(self.record_fd)
        if self.record_file.prepare_for_append(truncate_invalid=True):
            logger.error(f"Records file broken at offset {self.record_file.tell()}, truncated to last valid record")
        self.update_lock = asyncio.Lock()

        self.historic_config_q: Optional[asyncio.Queue] = None
        self.pgdump_config_q: Optional[asyncio.Queue] = None
        self.cluster_info_config_q: Optional[asyncio.Queue] = None

    def start_bg(self):
        asyncio.create_task(self.loop("historic_config_q", "duration", self.historic_cycle, self.historic_disabled))
        asyncio.create_task(self.loop("pgdump_config_q", "pg_dump_timeout", self.pg_dump_cycle, None))
        asyncio.create_task(self.loop("cluster_info_config_q", "extra_dump_timeout", self.cluster_info_cycle, None))

    def get_free_space(self) -> int:
        return os.statvfs(str(self.record_file_path)).f_bfree

    def check_recording_allowed(self) -> bool:
        disk_free = self.get_free_space()
        if disk_free <= self.collection_config.min_device_free:
            logger.warning("Stop recording due to disk free space %sMiB less then minimal %sMiB",
                           disk_free // MiB, self.collection_config.min_device_free // MiB)
            return False

        if time.time() >= self.collection_config.collection_end_time:
            logger.warning("Stop recording due record time expired")
            return False

        if self.record_file.tell() >= self.collection_config.max_record_file:
            logger.warning("Stop recording due to record file too large %sMiB, while %sMiB is a limit",
                           self.record_file.tell() // MiB, self.collection_config.max_record_file // MiB)
            return False
        return True

    def get_collection_config(self) -> Dict[str, Any]:
        res = self.collection_config.__dict__.copy()
        res['file_size'] = self.record_file.tell()
        res['free_space'] = self.get_free_space()
        res['osd_ids'] = list(res['osd_ids'])
        return res

    async def disable_record(self):
        async with self.update_lock:
            self.collection_config.pg_dump_timeout = None
            self.collection_config.extra_dump_timeout = None
            self.collection_config.historic_enabled = False

        await self.send_reconfig()

    async def send_reconfig(self):
        assert self.pgdump_config_q
        assert self.historic_config_q
        assert self.cluster_info_config_q

        self.pgdump_config_q.put_nowait(LoopCmd.reconfig)
        self.historic_config_q.put_nowait(LoopCmd.reconfig)
        self.cluster_info_config_q.put_nowait(LoopCmd.reconfig)

        ready, not_ready = await asyncio.wait([self.pgdump_config_q.join(),
                                               self.historic_config_q.join(),
                                               self.cluster_info_config_q.join()], timeout=10)
        assert not not_ready, "Can't reconfigure some loops!"

    async def enable_record(self,
                            osd_ids: Optional[Iterable[int]],
                            size: Optional[int],
                            duration: Optional[int],
                            min_duration: Optional[int] = 50,
                            dump_unparsed_headers: bool = False,
                            pg_dump_timeout: Optional[int] = None,
                            extra_cmd: List[str] = None,
                            extra_dump_timeout: Optional[int] = None,
                            max_record_file: int = DEFAULT_MAX_REC_FILE_SIZE,
                            min_device_free: int = DEFAULT_MIN_DEVICE_FREE,
                            max_collect_for: int = 24 * 60 * 60,
                            packer_name: str = 'compact'):

        if duration is not None:
            assert size is not None

        if osd_ids is None:
            osd_ids = await self.cli.get_local_osds()

        async with self.update_lock:
            self.collection_config.osd_ids = set(osd_ids)
            self.collection_config.size = size
            self.collection_config.duration = duration
            self.collection_config.min_duration = min_duration
            self.collection_config.dump_unparsed_headers = dump_unparsed_headers
            self.collection_config.pg_dump_timeout = pg_dump_timeout
            self.collection_config.extra_cmd = extra_cmd if extra_cmd else []
            self.collection_config.extra_dump_timeout = extra_dump_timeout
            self.collection_config.historic_enabled = True
            self.collection_config.max_record_file = max_record_file
            self.collection_config.min_device_free = min_device_free
            self.collection_config.collection_end_time = time.time() + max_collect_for
            self.collection_config.packer = packer_name

            cfg = self.collection_config.__dict__.copy()
            cfg['osd_ids'] = list(osd_ids)
            self.packer = get_historic_packer(packer_name)
            rec = self.packer.pack_record(RecId.params, cfg)
            if rec:
                self.record_file.write_record(*rec)

        await self.send_reconfig()

    async def dump_cluster_info(self) -> Optional[FileRec]:
        """
        make a message with provided cmd outputs
        """
        output = {'time': int(time.time())}

        for cmd in self.collection_config.extra_cmd:
            try:
                output[cmd] = await self.cli.run_no_ceph(cmd)
            except subprocess.SubprocessError as exc:
                logger.error("Cmd failed: %s", exc)

        return (RecId.cluster_info, output) if len(output) > 1 else None

    async def reload_pools(self) -> Optional[FileRec]:
        pools = await self.cli.get_pools()

        new_pools_map: Dict[int, Tuple[str, int]] = {}
        for idx, (pool_id, pool_name) in enumerate(sorted(pools.items())):
            new_pools_map[pool_id] = pool_name, idx

        if new_pools_map != self.pools_map:
            self.pools_map = new_pools_map
            self.pools_map_no_name = {num: idx for num, (_, idx) in new_pools_map.items()}
            return RecId.pools, self.pools_map
        return None

    async def dump_historic(self) -> AsyncIterator[FileRec]:
        try:
            ctime = time.time()
            curr_not_inited = self.not_inited_osd
            self.not_inited_osd = set()
            for osd_id in curr_not_inited:
                if not await self.cli.set_history_size_duration(osd_id,
                                                                self.collection_config.size,
                                                                self.collection_config.duration):
                    self.not_inited_osd.add(osd_id)

            new_rec = await self.reload_pools()
            if new_rec:
                # pools updated - skip this cycle, as different ops may came from pools before and after update
                yield new_rec
            else:
                for osd_id in self.collection_config.osd_ids.difference(self.not_inited_osd):
                    try:
                        parsed = await self.cli.get_historic(osd_id)
                    except (subprocess.CalledProcessError, OSError):
                        self.not_inited_osd.add(osd_id)
                        continue

                    if self.collection_config.size != parsed['size'] or \
                            self.collection_config.duration != parsed['duration']:
                        self.not_inited_osd.add(osd_id)
                        continue

                    ops = []

                    for op in self.parse_historic_records(parsed['ops']):
                        if op.tp is not None and op.description not in self.last_time_ops[osd_id]:
                            assert op.pack_pool_id is None
                            op.pack_pool_id = self.pools_map_no_name[op.pool_id]
                            ops.append(op)

                    self.last_time_ops[osd_id] = {op.description for op in ops}
                    yield (RecId.ops, (osd_id, ctime, ops))
        except Exception:
            logger.exception("In dump_historic")

    def parse_historic_records(self, ops: List[OpRec]) -> Iterator[CephOp]:
        for raw_op in ops:
            if self.collection_config.min_duration and \
                    int(raw_op.get('duration') * 1000) < self.collection_config.min_duration:
                continue
            try:
                parse_res, ceph_op = CephOp.parse_op(raw_op)
                if ceph_op:
                    yield ceph_op
                elif parse_res == ParseResult.unknown:
                    logger.debug(f"Unknown ceph op: {raw_op['description']}")
            except Exception as exc:
                logger.debug(f"Failed to parse op: {exc}\n{pprint.pformat(raw_op)}")

    async def loop(self, q_attr: str, timeout_attr: str, cycle_func, stop_func):
        q = asyncio.Queue()
        setattr(self, q_attr, q)
        try:
            running = False
            wait_timeout: Optional[float] = None
            next_run: Optional[float] = None

            while True:
                next_run_at = (next_run - time.time()) if next_run else None
                done, _ = await asyncio.wait([q.get()], timeout=next_run_at)

                # config changed
                if done:
                    fut, = done
                    cmd: LoopCmd = await fut
                    assert cmd == LoopCmd.reconfig

                    new_timeout = getattr(self.collection_config, timeout_attr)

                    # if stopped and enabled
                    if new_timeout and not running:
                        running = True
                        next_run = time.time()
                        wait_timeout = new_timeout

                    # if running and disables
                    if not new_timeout and running:
                        if stop_func:
                            await stop_func()
                        running = False
                        next_run = None
                        wait_timeout = None

                    # if timeout updated
                    if wait_timeout != new_timeout:
                        assert running
                        wait_timeout = new_timeout
                        next_run = time.time()

                    q.task_done()

                if running and next_run <= time.time():
                    assert wait_timeout
                    await cycle_func()
                    next_run = time.time() + wait_timeout

                if not running:
                    assert next_run is None
        except:
            import traceback
            traceback.print_exc()
        finally:
            setattr(self, q_attr, None)

    async def historic_cycle(self):
        logger.info("Start dump historic")
        async for rec_id, data in self.dump_historic():
            rec = self.packer.pack_record(rec_id, data)
            if rec:
                self.record_file.write_record(*rec, flush=False)
        self.record_file.flush()

    async def historic_disabled(self):
        for osd_id in self.collection_config.osd_ids:
            await self.cli.set_history_size_duration(osd_id, DEFAULT_SIZE, DEFAULT_DURATION)

    async def pg_dump_cycle(self):
        logger.debug("Run pg dump")
        data = (await self.cli.run_json_raw("pg dump")).strip()
        if data.startswith("dumped all"):
            data = data.replace("dumped all", "", 1).lstrip()
        rec = self.packer.pack_record(RecId.pgdump, data)
        if rec:
            self.record_file.write_record(*rec)

    async def cluster_info_cycle(self):
        logger.debug("Run cluster info: %s", self.collection_config.extra_cmd)
        rec = self.packer.pack_record(*(await self.dump_cluster_info()))
        if rec:
            self.record_file.write_record(*rec)


dumper: Optional[CephHistoricDumper] = None


@expose
async def start_historic_collection(record_file_path: str, *args, cmd_timeout: int = 50, **kwargs):
    try:
        global dumper
        if dumper:
            assert dumper.record_file_path == Path(record_file_path)
        else:
            version = await get_ceph_version()
            dumper = CephHistoricDumper(version.release, Path(record_file_path), cmd_timeout=cmd_timeout)
            dumper.start_bg()
        await dumper.enable_record(*args, **kwargs)
    except:
        import traceback
        traceback.print_exc()
        raise


@expose
async def stop_historic_collection():
    try:
        global dumper
        assert dumper
        await dumper.stop_collection()
    except:
        import traceback
        traceback.print_exc()
        raise


@expose
def get_historic_collection_status() -> Dict[str, Any]:
    try:
        assert dumper
        return dumper.get_collection_config()
    except:
        import traceback
        traceback.print_exc()
        raise


@expose
def get_collected_historic_data(offset: int, size: int) -> IReadableAsync:
    try:
        assert dumper
        rfd = dumper.record_file_path.open("rb")
        rfd.seek(offset)
        return ChunkedFile(rfd, close_at_the_end=True, till_offset=offset + size)
    except:
        import traceback
        traceback.print_exc()
        raise

