#!/usr/bin/env python3
"""Report shared-disk capacity. Never delete data or control services.

Keep the reviewed snapshot_budget.py beside this script in the same release.
Run every 30 seconds as a service, or use --once for a finite sample.
"""

import argparse
from collections import deque
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import statistics
import sys
import tempfile
import time


SCHEMA = "blockzilla-native-capacity-v1"
INTERVAL_SECONDS = 30
STALE_AFTER_SECONDS = 90
WINDOW_SECONDS = 600
MIN_OBSERVATION_SECONDS = 60
MAX_SAMPLES = 64
MAX_JSON_BYTES = 64 * 1024
SOURCE_KEYS = {"genesis_hash", "node_identity", "agave_revision", "session_id"}


def load_snapshot_helper():
    path = Path(__file__).resolve().with_name("snapshot_budget.py")
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError("snapshot_helper_not_regular")
    body = path.read_bytes()
    name = "_capacity_snapshot_budget_" + hashlib.sha256(body).hexdigest()
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("snapshot_helper_unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    # Execute the exact hashed bytes without creating a cache beside the input.
    exec(compile(body, str(path), "exec"), module.__dict__)
    return module, hashlib.sha256(body).hexdigest()


def read_json(path):
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError("report_not_regular")
    with path.open("rb") as stream:
        body = stream.read(MAX_JSON_BYTES + 1)
    if len(body) > MAX_JSON_BYTES:
        raise ValueError("report_too_large")
    return json.loads(body)


def read_source(root):
    value = read_json(root / "config/native-active-source.json")
    if (not isinstance(value, dict) or set(value) != SOURCE_KEYS
            or any(not isinstance(item, str) for item in value.values())
            or any(not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", value[key])
                   for key in ("genesis_hash", "node_identity"))
            or not re.fullmatch(r"[0-9a-f]{40}", value["agave_revision"])
            or not re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{1,16}", value["session_id"])):
        raise ValueError("invalid_active_source")
    return value


def runtime_health(path, source, now_ms):
    """Optionally read the independent live-priority report, without RPC calls."""
    if path is None:
        return {"configured": False}
    try:
        value = read_json(path)
        updated = value["updated_unix_ms"]
        if type(updated) is not int or not 0 <= now_ms - updated <= STALE_AFTER_SECONDS * 1000:
            raise ValueError("stale_runtime_health")
        if value["source"] not in (source, source["session_id"]) or type(value["healthy"]) is not bool:
            raise ValueError("invalid_runtime_health")
        return {"configured": True, "state": "HEALTHY" if value["healthy"] else "UNHEALTHY",
                "healthy": value["healthy"], "age_ms": now_ms - updated}
    except (OSError, ValueError, KeyError, TypeError):
        return {"configured": True, "state": "UNAVAILABLE", "healthy": False}


class Forecast:
    """Use a finite monotonic window; a source or filesystem change resets it."""

    def __init__(self):
        self.identity = None
        self.samples = deque(maxlen=MAX_SAMPLES)

    def reset(self):
        self.identity = None
        self.samples.clear()

    def observe(self, identity, now, free_bytes, archive_bytes, headroom_bytes, reserve_headroom_bytes):
        if (identity != self.identity or (self.samples and now <= self.samples[-1][0])):
            self.reset()
            self.identity = identity
        self.samples.append((now, free_bytes, free_bytes + archive_bytes))
        while self.samples and now - self.samples[0][0] > WINDOW_SECONDS:
            self.samples.popleft()
        span = now - self.samples[0][0]
        ready = len(self.samples) >= 3 and span >= MIN_OBSERVATION_SECONDS
        result = {
            "forecast_ready": ready, "forecast_window_seconds": WINDOW_SECONDS,
            "forecast_min_observation_seconds": MIN_OBSERVATION_SECONDS,
            "forecast_observed_seconds": round(span, 3), "forecast_samples": len(self.samples),
            "net_disk_growth_bytes_per_second": None,
            "non_archive_growth_bytes_per_second": None,
            "estimated_seconds_to_capture_reserve": None,
            "estimated_seconds_to_snapshot_headroom_exhaustion": None,
            "forecast_is_guarantee": False,
        }
        if not ready:
            return result
        net, non_archive = [], []
        samples = list(self.samples)
        for i, left in enumerate(samples):
            for right in samples[i + 1:]:
                elapsed = right[0] - left[0]
                if elapsed >= MIN_OBSERVATION_SECONDS:
                    net.append((left[1] - right[1]) / elapsed)
                    non_archive.append((left[2] - right[2]) / elapsed)
        # Medians reduce the effect of a single statvfs or archive-allocation jump.
        net_rate, non_archive_rate = statistics.median(net), statistics.median(non_archive)
        result.update(net_disk_growth_bytes_per_second=round(net_rate, 3),
                      non_archive_growth_bytes_per_second=round(non_archive_rate, 3))
        if net_rate > 0:
            result["estimated_seconds_to_capture_reserve"] = round(max(0, reserve_headroom_bytes) / net_rate, 3)
        if non_archive_rate > 0:
            result["estimated_seconds_to_snapshot_headroom_exhaustion"] = round(
                max(0, headroom_bytes) / non_archive_rate, 3)
        return result


def collect(root, snapshots, helper_sha256, forecast, *, monotonic=None, now_ms=None,
            runtime_health_report=None):
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    monotonic = time.monotonic() if monotonic is None else monotonic
    source = read_source(root)
    directory = root / "snapshots"
    if root.stat().st_dev != directory.stat().st_dev:
        raise ValueError("snapshot_filesystem_differs_from_capture")
    records = snapshots.inventory_snapshots(directory)
    usage = os.statvfs(root)
    free_bytes = usage.f_bavail * usage.f_frsize
    total_bytes = usage.f_blocks * usage.f_frsize
    budget = snapshots.snapshot_plan(records, free_bytes)
    if read_source(root) != source:
        raise ValueError("active_source_changed_during_sample")
    known_archives = [record for record in records if record.kind in ("full", "incremental", "active_temporary")]
    archive_bytes = sum(record.allocated_bytes for record in known_archives)
    snapshot_headroom = free_bytes - budget["capture_reserve_bytes"] - budget["next_archive_budget_bytes"]
    recovery_headroom = free_bytes - budget["recovery_minimum_free_bytes"] - budget["next_archive_budget_bytes"]
    critical, low = snapshot_headroom < 0, recovery_headroom < 0
    value = {
        "schema": SCHEMA, "state": "RUNNING", "capacity_state": "CRITICAL" if critical else "WARNING" if low else "OK",
        "updated_unix_ms": now_ms, "sample_interval_seconds": INTERVAL_SECONDS,
        "stale_after_seconds": STALE_AFTER_SECONDS, "source": source,
        "free_disk_bytes": free_bytes, "total_disk_bytes": total_bytes,
        "capture_reserve_bytes": budget["capture_reserve_bytes"],
        "recovery_minimum_free_bytes": budget["recovery_minimum_free_bytes"],
        "next_archive_budget_bytes": budget["next_archive_budget_bytes"],
        "snapshot_headroom_bytes": snapshot_headroom, "recovery_headroom_bytes": recovery_headroom,
        "snapshot_capture_reserve_at_risk": critical, "snapshot_headroom_low": low,
        "snapshot_archive_allocated_bytes": archive_bytes,
        "active_temporary_archive_count": sum(record.kind == "active_temporary" for record in records),
        "snapshot_helper_sha256": helper_sha256,
        "runtime_health": runtime_health(runtime_health_report, source, now_ms),
        "read_only_inputs": True, "service_operations": [], "deleted_files": [],
        "limits": ["Archive allocation is a write-budget observation, not a size bound.",
                   "Forecasts use recent measured growth; future writes and retirement can change it.",
                   "Non-archive growth excludes immediate completed and temporary snapshot archives only.",
                   "This collector cannot stop disk growth or prevent a capture failure."],
    }
    identity = (tuple(sorted(source.items())), root.stat().st_dev, total_bytes)
    value.update(forecast.observe(identity, monotonic, free_bytes, archive_bytes,
                                 snapshot_headroom, free_bytes - budget["capture_reserve_bytes"]))
    return value


def sample(root, snapshots, helper_sha256, forecast, **kwargs):
    try:
        return collect(root, snapshots, helper_sha256, forecast, **kwargs)
    except (OSError, ValueError, KeyError, TypeError, OverflowError):
        forecast.reset()
        return failure("capacity_sample_unavailable", kwargs.get("now_ms"))


def failure(code, now_ms=None):
    return {"schema": SCHEMA, "state": "FAILED", "error": code,
            "updated_unix_ms": int(time.time() * 1000) if now_ms is None else now_ms,
            "sample_interval_seconds": INTERVAL_SECONDS, "stale_after_seconds": STALE_AFTER_SECONDS,
            "read_only_inputs": True, "service_operations": [], "deleted_files": []}


def save(report, value):
    report.mkdir(mode=0o700, parents=True, exist_ok=True)
    if report.is_symlink() or not report.is_dir():
        raise ValueError("invalid_report_directory")
    name = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=report, prefix=".STATUS-", delete=False) as stream:
            name = stream.name
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, report / "STATUS.json")
        name = None
        descriptor = os.open(report, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if name is not None:
            Path(name).unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--runtime-health-report", type=Path,
                        help="optional independent live-priority STATUS.json; never controls capacity results")
    parser.add_argument("--once", action="store_true", help="save and print one sample")
    args = parser.parse_args(argv)
    os.umask(0o077)
    root = args.root.resolve(strict=True)
    try:
        snapshots, digest = load_snapshot_helper()
    except (OSError, ValueError, ImportError, SyntaxError):
        value = failure("snapshot_helper_unavailable")
        save(args.report, value)
        if args.once:
            print(json.dumps(value, sort_keys=True))
        return 1
    forecast = Forecast()
    while True:
        value = sample(root, snapshots, digest, forecast, runtime_health_report=args.runtime_health_report)
        save(args.report, value)
        if args.once:
            print(json.dumps(value, sort_keys=True, allow_nan=False))
            return 0 if value["state"] == "RUNNING" else 1
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
