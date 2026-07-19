#!/usr/bin/env python3
"""Publish a bounded, secret-free view of NAS runtime operations.

The legacy watcher owns archive lanes, but raw WAL capture and one-off CAR
acquisition jobs may deliberately run outside that process tree. This sidecar
observes those processes without publishing command lines, endpoints, tokens,
or filesystem paths.
"""

import argparse
from dataclasses import dataclass
import fcntl
import json
import math
import os
from pathlib import Path
import re
import signal
import time


MIB = 1024 * 1024
SLOTS_PER_EPOCH = 432_000
MAX_PROCESSES = 20
stop_requested = False


@dataclass(frozen=True)
class ProcessSample:
    pid: int
    ppid: int
    start_ticks: int
    start_unix_secs: int
    name: str
    args: tuple
    rchar: int
    read_bytes: int
    write_bytes: int
    cpu_ticks: int
    rss_bytes: int


def nonnegative_rate(current, previous, elapsed):
    if previous is None or elapsed <= 0 or current < previous:
        return None
    return (current - previous) / elapsed


def bounded_rate(value):
    if value is None or not math.isfinite(value) or value < 0:
        return None
    return round(value, 3)


def sanitize_name(value):
    cleaned = "".join(character for character in value if character.isprintable()).strip()
    return (cleaned or "unknown")[:64]


def parse_proc_stat(value):
    closing = value.rfind(")")
    if closing < 0:
        raise ValueError("invalid proc stat")
    fields = value[closing + 2 :].split()
    if len(fields) < 20:
        raise ValueError("short proc stat")
    return {
        "ppid": int(fields[1]),
        "cpu_ticks": int(fields[11]) + int(fields[12]),
        "start_ticks": int(fields[19]),
    }


def parse_proc_io(value):
    result = {}
    for line in value.splitlines():
        key, separator, raw = line.partition(":")
        if separator:
            result[key] = int(raw.strip())
    return result


def boot_time(proc_root):
    for line in (proc_root / "stat").read_text(encoding="utf-8").splitlines():
        if line.startswith("btime "):
            return int(line.split()[1])
    raise ValueError("/proc/stat has no boot time")


def process_args(path):
    raw = path.read_bytes()
    return tuple(
        item.decode("utf-8", errors="replace")
        for item in raw.split(b"\0")
        if item
    )


def process_sample(proc_root, pid, boot_unix_secs, clock_ticks, page_size):
    root = proc_root / str(pid)
    stat = parse_proc_stat((root / "stat").read_text(encoding="utf-8"))
    counters = parse_proc_io((root / "io").read_text(encoding="utf-8"))
    resident_pages = int((root / "statm").read_text(encoding="utf-8").split()[1])
    return ProcessSample(
        pid=pid,
        ppid=stat["ppid"],
        start_ticks=stat["start_ticks"],
        start_unix_secs=boot_unix_secs + stat["start_ticks"] // clock_ticks,
        name=sanitize_name((root / "comm").read_text(encoding="utf-8")),
        args=process_args(root / "cmdline"),
        rchar=max(0, counters.get("rchar", 0)),
        read_bytes=max(0, counters.get("read_bytes", 0)),
        write_bytes=max(0, counters.get("write_bytes", 0)),
        cpu_ticks=max(0, stat["cpu_ticks"]),
        rss_bytes=max(0, resident_pages * page_size),
    )


def collect_processes(proc_root):
    clock_ticks = os.sysconf("SC_CLK_TCK")
    page_size = os.sysconf("SC_PAGE_SIZE")
    boot_unix_secs = boot_time(proc_root)
    samples = {}
    inaccessible = 0
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if entry.stat().st_uid != os.getuid():
                continue
        except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
            continue
        pid = int(entry.name)
        try:
            sample = process_sample(proc_root, pid, boot_unix_secs, clock_ticks, page_size)
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError, OSError):
            inaccessible += 1
            continue
        samples[pid] = sample
    return samples, inaccessible


def command_has(sample, executable, subcommand=None):
    if not sample.args:
        return False
    if Path(sample.args[0]).name != executable:
        return False
    return subcommand is None or subcommand in sample.args[1:]


def option_value(args, option):
    for index, item in enumerate(args):
        if item == option and index + 1 < len(args):
            return args[index + 1]
        if item.startswith(option + "="):
            return item[len(option) + 1 :]
    return None


def read_last_json_line(path, maximum_bytes=64 * 1024):
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        end = handle.tell()
        if end <= 0:
            raise ValueError("empty journal")
        start = max(0, end - maximum_bytes)
        handle.seek(start)
        payload = handle.read(end - start)
    lines = [line for line in payload.splitlines() if line.strip()]
    if not lines:
        raise ValueError("journal has no complete record")
    return json.loads(lines[-1])


def previous_sample(previous, sample):
    candidate = previous.get(sample.pid)
    if candidate is None or candidate.start_ticks != sample.start_ticks:
        return None
    return candidate


def live_capture_status(samples, previous, elapsed, now_unix_secs):
    candidates = [
        sample for sample in samples.values()
        if command_has(sample, "hivezilla", "record-grpc-raw")
        or command_has(sample, "blockzilla-live-producer", "record-grpc-raw")
    ]
    candidates.sort(key=lambda sample: (sample.start_ticks, sample.pid), reverse=True)
    for sample in candidates:
        output_dir = option_value(sample.args, "--output-dir")
        if not output_dir:
            continue
        journal = Path(output_dir) / "raw-blocks.jsonl"
        try:
            record = read_last_json_line(journal)
            slot = int(record["slot"])
            epoch = int(record.get("epoch", slot // SLOTS_PER_EPOCH))
            frame_id = int(record.get("frame_id", 0))
            modified = int(journal.stat().st_mtime)
        except (FileNotFoundError, KeyError, TypeError, ValueError, OSError, json.JSONDecodeError):
            continue
        if slot < 0 or epoch != slot // SLOTS_PER_EPOCH or frame_id < 0:
            continue
        old = previous_sample(previous, sample)
        write_rate = nonnegative_rate(
            sample.write_bytes,
            old.write_bytes if old else None,
            elapsed,
        )
        return {
            "state": "capturing" if now_unix_secs <= modified + 30 else "stalled",
            "mode": "raw_wal",
            "source": "grpc",
            "pid": sample.pid,
            "epoch": epoch,
            "last_slot": slot,
            "blocks_written": frame_id + 1,
            "bytes_written": sample.write_bytes,
            "write_mib_per_sec": bounded_rate(
                None if write_rate is None else write_rate / MIB
            ),
            "started_unix_secs": sample.start_unix_secs,
            "updated_unix_secs": min(now_unix_secs, modified),
        }
    return None


EPOCH_CAR_RE = re.compile(r"epoch-([0-9]+)[.]car(?:[.]part)?$")


def open_car_part(proc_root, pid):
    fd_root = proc_root / str(pid) / "fd"
    for descriptor in fd_root.iterdir():
        try:
            target = Path(os.readlink(descriptor))
        except (FileNotFoundError, PermissionError, OSError):
            continue
        match = EPOCH_CAR_RE.search(target.name)
        if match and target.name.endswith(".part"):
            return target, int(match.group(1))
    return None, None


def checksum_jobs(proc_root, samples, previous, elapsed, now_unix_secs):
    jobs = []
    for sample in samples.values():
        if not command_has(sample, "sha256sum") or "--check" not in sample.args:
            continue
        try:
            source, epoch = open_car_part(proc_root, sample.pid)
            if source is None or epoch is None:
                continue
            total = source.stat().st_size
        except (FileNotFoundError, PermissionError, OSError):
            continue
        done = min(total, sample.rchar)
        old = previous_sample(previous, sample)
        bytes_per_sec = nonnegative_rate(done, min(total, old.rchar) if old else None, elapsed)
        read_mib_per_sec = bounded_rate(
            None if bytes_per_sec is None else bytes_per_sec / MIB
        )
        eta_secs = None
        if bytes_per_sec is not None and bytes_per_sec > 0:
            eta_secs = max(0, round((total - done) / bytes_per_sec))
        jobs.append({
            "id": "car-verify-epoch-{}".format(epoch),
            "kind": "car_verify",
            "epoch": epoch,
            "phase": "checksum",
            "state": "running",
            "pid": sample.pid,
            "bytes_done": done,
            "bytes_total": total,
            "progress_pct": round(done * 100 / total, 3) if total > 0 else None,
            "read_mib_per_sec": read_mib_per_sec,
            "write_mib_per_sec": 0.0,
            "eta_secs": eta_secs,
            "rss_bytes": sample.rss_bytes,
            "started_unix_secs": sample.start_unix_secs,
            "updated_unix_secs": now_unix_secs,
        })
    return jobs


def expected_size_from_parent(samples, sample, source):
    parent = samples.get(sample.ppid)
    if parent is None:
        return None
    for index, item in enumerate(parent.args[:-1]):
        if item == str(source) and parent.args[index + 1].isdigit():
            return int(parent.args[index + 1])
    return None


def download_jobs(samples, previous, elapsed, now_unix_secs):
    jobs = []
    for sample in samples.values():
        if not command_has(sample, "aria2c"):
            continue
        output = option_value(sample.args, "--out")
        directory = option_value(sample.args, "--dir")
        if not output:
            continue
        source = Path(directory or ".") / output
        match = EPOCH_CAR_RE.search(source.name)
        if not match:
            continue
        epoch = int(match.group(1))
        try:
            done = source.stat().st_size
        except (FileNotFoundError, PermissionError, OSError):
            done = 0
        total = expected_size_from_parent(samples, sample, source)
        old = previous_sample(previous, sample)
        bytes_per_sec = nonnegative_rate(
            sample.write_bytes,
            old.write_bytes if old else None,
            elapsed,
        )
        progress = None
        eta_secs = None
        if total is not None and total > 0:
            done = min(done, total)
            progress = round(done * 100 / total, 3)
            if bytes_per_sec is not None and bytes_per_sec > 0:
                eta_secs = max(0, round((total - done) / bytes_per_sec))
        jobs.append({
            "id": "car-download-epoch-{}".format(epoch),
            "kind": "car_download",
            "epoch": epoch,
            "phase": "download",
            "state": "running",
            "pid": sample.pid,
            "bytes_done": done,
            "bytes_total": total,
            "progress_pct": progress,
            "read_mib_per_sec": 0.0,
            "write_mib_per_sec": bounded_rate(
                None if bytes_per_sec is None else bytes_per_sec / MIB
            ),
            "eta_secs": eta_secs,
            "rss_bytes": sample.rss_bytes,
            "started_unix_secs": sample.start_unix_secs,
            "updated_unix_secs": now_unix_secs,
        })
    return jobs


def is_blockzilla_owned(sample):
    private_command = " ".join(sample.args).lower()
    return (
        "blockzilla" in private_command
        or "hivezilla" in private_command
        or "publish-runtime-operations.py" in private_command
    )


def process_io_status(samples, previous, elapsed, inaccessible, now_unix_secs):
    entries = []
    clock_ticks = os.sysconf("SC_CLK_TCK")
    for sample in samples.values():
        if is_blockzilla_owned(sample):
            continue
        old = previous_sample(previous, sample)
        read_rate = nonnegative_rate(
            sample.read_bytes,
            old.read_bytes if old else None,
            elapsed,
        )
        write_rate = nonnegative_rate(
            sample.write_bytes,
            old.write_bytes if old else None,
            elapsed,
        )
        cpu_rate = nonnegative_rate(
            sample.cpu_ticks,
            old.cpu_ticks if old else None,
            elapsed,
        )
        read_mib = bounded_rate(None if read_rate is None else read_rate / MIB)
        write_mib = bounded_rate(None if write_rate is None else write_rate / MIB)
        if read_mib is None or write_mib is None or read_mib + write_mib <= 0:
            continue
        entries.append({
            "id": "{}-{}".format(sample.pid, sample.start_ticks),
            "pid": sample.pid,
            "name": sample.name,
            "read_mib_per_sec": read_mib,
            "write_mib_per_sec": write_mib,
            "cpu_percent": bounded_rate(
                None if cpu_rate is None else cpu_rate * 100 / clock_ticks
            ),
            "rss_bytes": sample.rss_bytes,
            "blockzilla_owned": False,
        })
    entries.sort(
        key=lambda entry: (
            -(entry["read_mib_per_sec"] + entry["write_mib_per_sec"]),
            entry["name"],
            entry["pid"],
        )
    )
    active_count = len(entries)
    truncated = active_count > MAX_PROCESSES
    entries = entries[:MAX_PROCESSES]
    return {
        "state": "ready" if previous else "collecting",
        "sampled_unix_secs": now_unix_secs,
        "sample_window_secs": round(elapsed, 3) if previous and elapsed > 0 else None,
        "active_count": active_count,
        "inaccessible_count": inaccessible,
        "truncated": truncated,
        "processes": entries,
        "message": None if previous else "Collecting the first process-counter interval",
    }


def build_status(proc_root, samples, previous, elapsed, inaccessible, now_unix_secs):
    jobs = checksum_jobs(proc_root, samples, previous, elapsed, now_unix_secs)
    jobs.extend(download_jobs(samples, previous, elapsed, now_unix_secs))
    jobs.sort(key=lambda job: (job["epoch"], job["kind"], job["id"]))
    return {
        "schema_version": 1,
        "updated_unix_secs": now_unix_secs,
        "live_capture": live_capture_status(samples, previous, elapsed, now_unix_secs),
        "jobs": jobs,
        "process_io": process_io_status(
            samples, previous, elapsed, inaccessible, now_unix_secs
        ),
    }


def publish(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(".{}.{}.tmp".format(path.name, os.getpid()))
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, separators=(",", ":"), sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o644)
    os.replace(temporary, path)


def request_stop(_signum, _frame):
    global stop_requested
    stop_requested = True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"))
    parser.add_argument("--interval-secs", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval_secs <= 0:
        parser.error("--interval-secs must be positive")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = args.output.parent / ".runtime-operations-publisher.lock"
    with lock_path.open("w", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("runtime operations publisher is already running")
        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        previous = {}
        previous_at = None
        while not stop_requested:
            now = int(time.time())
            samples, inaccessible = collect_processes(args.proc_root)
            elapsed = 0 if previous_at is None else max(0, now - previous_at)
            publish(
                args.output,
                build_status(
                    args.proc_root,
                    samples,
                    previous,
                    elapsed,
                    inaccessible,
                    now,
                ),
            )
            previous = samples
            previous_at = now
            if args.once:
                break
            time.sleep(args.interval_secs)


if __name__ == "__main__":
    main()
