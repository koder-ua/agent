import re
import time
import glob
import gzip
import logging
import datetime
import functools
import collections
from enum import Enum
from typing import Tuple, List, Dict, Iterator, TextIO

from .. import rpc


expose = functools.partial(rpc.expose_func, "ceph")
expose_async = functools.partial(rpc.expose_func_async, "ceph")

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


class CephHealth(Enum):
    HEALTH_OK = 0
    SCRUB_MISSMATCH = 1
    CLOCK_SKEW = 2
    OSD_DOWN = 3
    REDUCED_AVAIL = 4
    DEGRADED = 5
    NO_ACTIVE_MGR = 6
    SLOW_REQUESTS = 7
    MON_ELECTION = 8


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
def find_issues_in_ceph_log(max_lines: int = 10000) -> str:
    errs_warns = []
    for ln in open("/var/log/ceph/ceph.log"):
        if 'cluster [ERR]' in ln or "cluster [WRN]" in ln:
            errs_warns.append(ln)
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
