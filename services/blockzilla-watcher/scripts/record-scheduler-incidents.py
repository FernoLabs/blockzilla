#!/usr/bin/env python3
"""Record bounded scheduler incidents with pre-transition process context.

The recorder is intentionally separate from the scheduler and maintenance
workers. It observes their atomic status/control files and never signals a
process. Private incidents contain process identity and resource samples; use
publish-scheduler-activity.py to create the redacted public feed.
"""

from __future__ import annotations

import argparse
from collections import deque
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import time


MIB = 1024 * 1024
MAX_JSON_BYTES = 8 * MIB
MAX_EVENT_BYTES = 256 * 1024
MAX_PRIVATE_LOG_BYTES = 10 * MIB
MEMORY_PAUSE_KIB = 2_621_440
LOAD_PAUSE = 10.0
BACKFILL_STATES = {
    "starting",
    "waiting_for_resources",
    "running",
    "paused_for_resources",
    "failed",
    "complete",
}
JOB_PATTERN = re.compile(r"[a-z0-9_]+:[0-9]+")
IO_REASON_PATTERN = re.compile(
    r"IO PSI full avg10 ([0-9]+(?:\.[0-9]+)?) reached pause threshold ([0-9]+(?:\.[0-9]+)?)"
)
LOAD_REASON_PATTERN = re.compile(
    r"load average ([0-9]+(?:\.[0-9]+)?) reached CPU load ceiling ([0-9]+(?:\.[0-9]+)?)"
)
AUTO_PAUSE_PATTERN = re.compile(r"auto-paused ([a-z0-9_]+:[0-9]+): (.+)")
AUTO_RESUME_PATTERN = re.compile(r"auto-resumed ([a-z0-9_]+:[0-9]+): .+")
stop_requested = False


def safe_number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number >= 0 else None


def safe_int(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def safe_process_name(value):
    if not isinstance(value, str):
        return "process"
    clean = "".join(character if character.isalnum() or character in "._:+-" else "_" for character in value.strip())
    return clean[:64] or "process"


def read_json(path, max_bytes=MAX_JSON_BYTES):
    try:
        size = path.stat().st_size
        if size <= 0 or size > max_bytes:
            return None
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else None
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError):
        return None


def atomic_json(path, value, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(".{}.{}.tmp".format(path.name, os.getpid()))
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
    if len(encoded) > MAX_EVENT_BYTES:
        raise ValueError("scheduler incident state exceeds its size limit")
    with temporary.open("wb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, mode)
    os.replace(temporary, path)


def append_jsonl(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
    if len(encoded) > MAX_EVENT_BYTES:
        raise ValueError("scheduler incident exceeds its size limit")
    if path.exists() and path.stat().st_size + len(encoded) > MAX_PRIVATE_LOG_BYTES:
        rotated = path.with_name(path.name + ".1")
        try:
            rotated.unlink()
        except FileNotFoundError:
            pass
        os.replace(path, rotated)
        os.chmod(rotated, 0o600)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.chmod(path, 0o600)


def parse_proc_stat(value):
    closing = value.rfind(")")
    opening = value.find("(")
    if opening <= 0 or closing <= opening:
        raise ValueError("invalid /proc stat")
    fields = value[closing + 2 :].split()
    if len(fields) < 22:
        raise ValueError("short /proc stat")
    return {
        "state": fields[0],
        "ppid": int(fields[1]),
        "cpu_ticks": int(fields[11]) + int(fields[12]),
        "start_ticks": int(fields[19]),
        "rss_pages": max(0, int(fields[21])),
    }


def parse_proc_io(value):
    counters = {}
    for raw_line in value.splitlines():
        if ":" not in raw_line:
            continue
        key, raw_value = raw_line.split(":", 1)
        if key in {"read_bytes", "write_bytes"}:
            counters[key] = max(0, int(raw_value.strip()))
    if set(counters) != {"read_bytes", "write_bytes"}:
        raise ValueError("incomplete /proc io")
    return counters


def collect_process_counters(proc_root=Path("/proc")):
    counters = {}
    inaccessible = 0
    for process_dir in proc_root.iterdir():
        if not process_dir.name.isdigit():
            continue
        pid = int(process_dir.name)
        try:
            parsed = parse_proc_stat((process_dir / "stat").read_text(encoding="utf-8"))
            name = safe_process_name((process_dir / "comm").read_text(encoding="utf-8"))
            uid = process_dir.stat().st_uid
        except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError):
            continue
        io_counters = None
        try:
            io_counters = parse_proc_io((process_dir / "io").read_text(encoding="utf-8"))
        except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError):
            inaccessible += 1
        identity = "{}:{}".format(pid, parsed["start_ticks"])
        counters[identity] = {
            "pid": pid,
            "name": name,
            "uid": uid,
            **parsed,
            "read_bytes": None if io_counters is None else io_counters["read_bytes"],
            "write_bytes": None if io_counters is None else io_counters["write_bytes"],
        }
    return counters, inaccessible


def process_rates(previous, current, elapsed_secs, clock_ticks, page_size, inaccessible_count, limit=20):
    elapsed = max(0.001, float(elapsed_secs))
    processes = []
    blocked = []
    for identity, value in current.items():
        prior = previous.get(identity)
        read_rate = None
        write_rate = None
        cpu_percent = None
        if prior is not None:
            if value["read_bytes"] is not None and prior["read_bytes"] is not None:
                read_rate = max(0, value["read_bytes"] - prior["read_bytes"]) / elapsed / MIB
            if value["write_bytes"] is not None and prior["write_bytes"] is not None:
                write_rate = max(0, value["write_bytes"] - prior["write_bytes"]) / elapsed / MIB
            cpu_delta = max(0, value["cpu_ticks"] - prior["cpu_ticks"])
            cpu_percent = cpu_delta * 100.0 / max(1, clock_ticks) / elapsed
        item = {
            "pid": value["pid"],
            "start_ticks": value["start_ticks"],
            "name": value["name"],
            "uid": value["uid"],
            "state": value["state"],
            "read_mib_per_sec": None if read_rate is None else round(read_rate, 3),
            "write_mib_per_sec": None if write_rate is None else round(write_rate, 3),
            "cpu_percent": None if cpu_percent is None else round(cpu_percent, 1),
            "rss_bytes": value["rss_pages"] * page_size,
        }
        if value["state"] == "D":
            blocked.append(item)
        if (read_rate or 0) > 0 or (write_rate or 0) > 0:
            processes.append(item)
    processes.sort(
        key=lambda item: (item["read_mib_per_sec"] or 0) + (item["write_mib_per_sec"] or 0),
        reverse=True,
    )
    blocked.sort(
        key=lambda item: (item["read_mib_per_sec"] or 0) + (item["write_mib_per_sec"] or 0),
        reverse=True,
    )
    return {
        "sample_window_secs": round(elapsed, 3),
        "active_count": len(processes),
        "inaccessible_count": inaccessible_count,
        "truncated": len(processes) > limit or len(blocked) > limit,
        "top_processes": processes[:limit],
        "blocked_processes": blocked[:limit],
    }


def proc_value(path, prefix):
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            if raw_line.startswith(prefix):
                return raw_line[len(prefix) :].strip().split()[0]
    except (FileNotFoundError, OSError, ValueError):
        return None
    return None


def host_metrics(backfill, proc_root=Path("/proc")):
    resources = backfill.get("resources") if isinstance(backfill, dict) else None
    resources = resources if isinstance(resources, dict) else {}
    memory_kib = safe_int(resources.get("mem_available_kib"))
    if memory_kib is None:
        raw_memory = proc_value(proc_root / "meminfo", "MemAvailable:")
        memory_kib = int(raw_memory) if raw_memory and raw_memory.isdigit() else None
    io_full = safe_number(resources.get("io_full_avg10"))
    if io_full is None:
        try:
            for line in (proc_root / "pressure" / "io").read_text(encoding="utf-8").splitlines():
                if not line.startswith("full "):
                    continue
                for field in line.split():
                    if field.startswith("avg10="):
                        io_full = safe_number(float(field.split("=", 1)[1]))
        except (FileNotFoundError, OSError, ValueError):
            pass
    load1 = safe_number(resources.get("load1"))
    if load1 is None:
        try:
            load1 = safe_number(float((proc_root / "loadavg").read_text(encoding="utf-8").split()[0]))
        except (FileNotFoundError, OSError, ValueError, IndexError):
            pass
    return {
        "memory_available_bytes": None if memory_kib is None else memory_kib * 1024,
        "io_full_avg10": io_full,
        "io_pause_threshold": safe_number(resources.get("io_pause_full_avg10")),
        "load1": load1,
        "load_pause_threshold": LOAD_PAUSE,
        "finalizer_active": resources.get("finalizer_active") if isinstance(resources.get("finalizer_active"), bool) else None,
        "live_capture_age_secs": safe_int(resources.get("live_capture_age_seconds")),
        "live_capture_max_stale_secs": safe_int(resources.get("live_capture_max_stale_seconds")),
        "live_capture_healthy": resources.get("live_capture_healthy") if isinstance(resources.get("live_capture_healthy"), bool) else None,
    }


def classify_backfill_pause(metrics):
    reasons = []
    memory_bytes = safe_int(metrics.get("memory_available_bytes"))
    if memory_bytes is not None and memory_bytes < MEMORY_PAUSE_KIB * 1024:
        reasons.append({"code": "memory_pressure", "observed": memory_bytes, "threshold": MEMORY_PAUSE_KIB * 1024, "unit": "bytes"})
    io_full = safe_number(metrics.get("io_full_avg10"))
    io_threshold = safe_number(metrics.get("io_pause_threshold"))
    if io_full is not None and io_threshold is not None and io_full >= io_threshold:
        reasons.append({"code": "io_pressure", "observed": io_full, "threshold": io_threshold, "unit": "percent"})
    load1 = safe_number(metrics.get("load1"))
    if load1 is not None and load1 >= LOAD_PAUSE:
        reasons.append({"code": "load_pressure", "observed": load1, "threshold": LOAD_PAUSE, "unit": "load"})
    if metrics.get("finalizer_active") is True:
        reasons.append({"code": "finalizer_active"})
    if metrics.get("live_capture_healthy") is False:
        reasons.append({
            "code": "live_capture_stale",
            "observed": safe_int(metrics.get("live_capture_age_secs")),
            "threshold": safe_int(metrics.get("live_capture_max_stale_secs")),
            "unit": "seconds",
        })
    return reasons or [{"code": "transient_resource_guard"}]


def reason_from_control_target(target):
    io_match = IO_REASON_PATTERN.search(target)
    if io_match:
        return [{"code": "io_pressure", "observed": float(io_match.group(1)), "threshold": float(io_match.group(2)), "unit": "percent"}]
    load_match = LOAD_REASON_PATTERN.search(target)
    if load_match:
        return [{"code": "load_pressure", "observed": float(load_match.group(1)), "threshold": float(load_match.group(2)), "unit": "load"}]
    return [{"code": "resource_guard"}]


def event_id(at_unix_secs, component, kind, subject):
    value = "{}\0{}\0{}\0{}".format(at_unix_secs, component, kind, subject or "")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def make_event(at_unix_secs, component, kind, subject, epoch, actor, reasons, metrics, context, prelude):
    return {
        "schema_version": 1,
        "id": event_id(at_unix_secs, component, kind, subject),
        "at_unix_secs": at_unix_secs,
        "component": component,
        "kind": kind,
        "subject": subject,
        "epoch": epoch,
        "actor": actor,
        "reasons": reasons,
        "metrics": metrics,
        "process_context": context,
        "prelude": list(prelude),
    }


def control_event(value, lease, metrics, context, prelude):
    if not isinstance(value, dict):
        return None
    at = safe_int(value.get("at_unix_secs"))
    action = value.get("action")
    target = value.get("target")
    if at is None or not isinstance(action, str) or not isinstance(target, str):
        return None
    lease_active = isinstance(lease, dict) and lease.get("state") == "active"
    lease_jobs = set(lease.get("jobs", [])) if lease_active and isinstance(lease.get("jobs"), list) else set()
    if action in {"pause", "resume"} and (target == "scheduler" or JOB_PATTERN.fullmatch(target)):
        kind = "paused" if action == "pause" else "resumed"
        actor = "priority_lease" if lease_active and (target == "scheduler" or target in lease_jobs) else "operator"
        reasons = [{"code": "priority_lease" if actor == "priority_lease" else "operator_request"}]
        return make_event(
            at,
            "archive_scheduler",
            kind,
            target,
            None,
            actor,
            reasons,
            metrics,
            context if kind == "paused" else None,
            prelude if kind == "paused" else [],
        )
    if action != "legacy_adaptive":
        return None
    pause_match = AUTO_PAUSE_PATTERN.fullmatch(target)
    if pause_match:
        job = pause_match.group(1)
        return make_event(
            at,
            "archive_scheduler",
            "paused",
            job,
            None,
            "automatic",
            reason_from_control_target(pause_match.group(2)),
            metrics,
            context,
            prelude,
        )
    resume_match = AUTO_RESUME_PATTERN.fullmatch(target)
    if resume_match:
        return make_event(
            at,
            "archive_scheduler",
            "resumed",
            resume_match.group(1),
            None,
            "automatic",
            [{"code": "resources_recovered"}],
            metrics,
            None,
            [],
        )
    return None


def read_new_jsonl(path, inode, offset):
    try:
        stat_result = path.stat()
        next_inode = stat_result.st_ino
        if inode != next_inode or offset > stat_result.st_size:
            offset = 0
        values = []
        with path.open("rb") as handle:
            handle.seek(offset)
            for raw_line in handle:
                if len(raw_line) > MAX_EVENT_BYTES:
                    continue
                try:
                    value = json.loads(raw_line)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue
                if isinstance(value, dict):
                    values.append(value)
            return values, next_inode, handle.tell()
    except (FileNotFoundError, OSError):
        return [], inode, offset


def request_stop(_signum, _frame):
    global stop_requested
    stop_requested = True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill-status", type=Path, required=True)
    parser.add_argument("--control-events", type=Path, required=True)
    parser.add_argument("--priority-lease", type=Path)
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--events-output", type=Path, required=True)
    parser.add_argument("--interval-secs", type=float, default=5.0)
    parser.add_argument("--ring-secs", type=float, default=30.0)
    parser.add_argument("--process-limit", type=int, default=20)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval_secs <= 0 or args.ring_secs < args.interval_secs or args.process_limit <= 0:
        parser.error("sampling values must be positive and ring-secs must cover one interval")

    args.state_file.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(args.state_file.parent, 0o700)
    lock_path = args.state_file.parent / "recorder.lock"
    with lock_path.open("w", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("scheduler incident recorder is already running")

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        checkpoint = read_json(args.state_file, MAX_EVENT_BYTES) or {}
        previous_backfill_state = checkpoint.get("backfill_state")
        control_inode = safe_int(checkpoint.get("control_inode"))
        control_offset = safe_int(checkpoint.get("control_offset")) or 0
        if control_inode is None:
            try:
                control_stat = args.control_events.stat()
                control_inode = control_stat.st_ino
                control_offset = control_stat.st_size
            except FileNotFoundError:
                control_inode = 0
                control_offset = 0

        clock_ticks = os.sysconf("SC_CLK_TCK")
        page_size = os.sysconf("SC_PAGE_SIZE")
        ring_size = max(2, int(math.ceil(args.ring_secs / args.interval_secs)) + 1)
        prelude = deque(maxlen=ring_size)
        process_contexts = deque(maxlen=ring_size)
        prior_processes = {}
        prior_monotonic = time.monotonic()

        while not stop_requested:
            sample_started = time.monotonic()
            now = int(time.time())
            backfill = read_json(args.backfill_status, MAX_EVENT_BYTES) or {}
            lease = read_json(args.priority_lease, MAX_EVENT_BYTES) if args.priority_lease else None
            current_processes, inaccessible = collect_process_counters()
            elapsed = max(0.001, sample_started - prior_monotonic)
            context = process_rates(
                prior_processes,
                current_processes,
                elapsed,
                clock_ticks,
                page_size,
                inaccessible,
                args.process_limit,
            )
            metrics = host_metrics(backfill)
            backfill_state = backfill.get("state") if backfill.get("state") in BACKFILL_STATES else None
            epoch = safe_int((backfill.get("current") or {}).get("epoch")) if isinstance(backfill.get("current"), dict) else None
            sample_summary = {"at_unix_secs": now, "backfill_state": backfill_state, **metrics}
            prelude.append(sample_summary)
            process_contexts.append(context)

            if previous_backfill_state != backfill_state and backfill_state is not None:
                at = safe_int(backfill.get("updated_unix_seconds")) or now
                if backfill_state == "paused_for_resources":
                    incident = make_event(
                        at,
                        "block_time_gap_backfill",
                        "paused",
                        "backfill",
                        epoch,
                        "automatic",
                        classify_backfill_pause(metrics),
                        metrics,
                        context,
                        list(prelude)[-4:],
                    )
                    append_jsonl(args.events_output, incident)
                elif previous_backfill_state == "paused_for_resources" and backfill_state == "running":
                    incident = make_event(
                        at,
                        "block_time_gap_backfill",
                        "resumed",
                        "backfill",
                        epoch,
                        "automatic",
                        [{"code": "resources_recovered"}],
                        metrics,
                        None,
                        [],
                    )
                    append_jsonl(args.events_output, incident)
                elif backfill_state in {"failed", "complete"}:
                    incident = make_event(
                        at,
                        "block_time_gap_backfill",
                        "failed" if backfill_state == "failed" else "completed",
                        "backfill",
                        epoch,
                        "automatic",
                        [{"code": "extractor_failed" if backfill_state == "failed" else "backfill_complete"}],
                        metrics,
                        context if backfill_state == "failed" else None,
                        list(prelude)[-4:] if backfill_state == "failed" else [],
                    )
                    append_jsonl(args.events_output, incident)
                previous_backfill_state = backfill_state

            control_values, control_inode, control_offset = read_new_jsonl(
                args.control_events, control_inode, control_offset
            )
            for value in control_values:
                incident = control_event(value, lease, metrics, context, list(prelude)[-4:])
                if incident is not None:
                    append_jsonl(args.events_output, incident)

            atomic_json(
                args.state_file,
                {
                    "schema_version": 1,
                    "updated_unix_secs": now,
                    "backfill_state": previous_backfill_state,
                    "control_inode": control_inode,
                    "control_offset": control_offset,
                    "process_accessible_count": len(current_processes) - inaccessible,
                    "process_inaccessible_count": inaccessible,
                },
            )
            prior_processes = current_processes
            prior_monotonic = sample_started
            if args.once:
                break
            remaining = args.interval_secs - (time.monotonic() - sample_started)
            if remaining > 0:
                time.sleep(remaining)


if __name__ == "__main__":
    main()
