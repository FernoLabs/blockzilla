#!/usr/bin/env python3
"""Publish a bounded, secret-free view of the NAS block-time-gap backfill."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import sys
import time


MAX_SOURCE_BYTES = 64 * 1024
STATES = {
    "starting",
    "waiting_for_resources",
    "running",
    "paused_for_resources",
    "failed",
    "complete",
}
stop_requested = False


def nonnegative_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("{} must be a non-negative integer".format(name))
    return value


def nonnegative_number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError("{} must be a non-negative number".format(name))
    return value


def nullable_nonnegative_int(value, name):
    return None if value is None else nonnegative_int(value, name)


def safe_error(value):
    if value is None:
        return None
    if isinstance(value, str) and re.fullmatch(r"epoch [0-9]+ extractor exited [0-9]+", value):
        return value
    return "Extractor failed"


def public_status(private):
    if not isinstance(private, dict) or private.get("schema_version") != 1:
        raise ValueError("unsupported private status schema")
    state = private.get("state")
    if state not in STATES:
        raise ValueError("unsupported backfill state")

    backfill = private.get("backfill")
    current = private.get("current")
    resources = private.get("resources")
    if not isinstance(backfill, dict) or not isinstance(current, dict) or not isinstance(resources, dict):
        raise ValueError("missing private status object")

    epochs_done = nonnegative_int(backfill.get("epochs_done"), "epochs_done")
    epochs_total = nonnegative_int(backfill.get("epochs_total"), "epochs_total")
    source_bytes_done = nonnegative_int(
        backfill.get("overall_source_bytes_done"), "overall_source_bytes_done"
    )
    source_bytes_total = nonnegative_int(backfill.get("source_bytes_total"), "source_bytes_total")
    if epochs_done > epochs_total or source_bytes_done > source_bytes_total:
        raise ValueError("backfill counters exceed their totals")

    progress_state = current.get("progress_state")
    if progress_state is not None and not isinstance(progress_state, str):
        raise ValueError("progress_state must be a string or null")
    eta_reliable = backfill.get("eta_reliable")
    if not isinstance(eta_reliable, bool):
        raise ValueError("eta_reliable must be a boolean")

    return {
        "schema_version": 1,
        "state": state,
        "started_unix_secs": nonnegative_int(
            private.get("started_unix_seconds"), "started_unix_seconds"
        ),
        "updated_unix_secs": nonnegative_int(
            private.get("updated_unix_seconds"), "updated_unix_seconds"
        ),
        "backfill": {
            "epochs_done": epochs_done,
            "epochs_total": epochs_total,
            "source_bytes_done": source_bytes_done,
            "source_bytes_total": source_bytes_total,
            "throughput_bytes_per_sec": nonnegative_number(
                backfill.get("wall_throughput_bytes_per_second"),
                "wall_throughput_bytes_per_second",
            ),
            "eta_secs": nullable_nonnegative_int(backfill.get("eta_seconds"), "eta_seconds"),
            "eta_reliable": eta_reliable,
        },
        "current": {
            "epoch": nullable_nonnegative_int(current.get("epoch"), "current.epoch"),
            "progress_state": progress_state,
        },
        "paused_secs": nonnegative_int(resources.get("paused_seconds"), "paused_seconds"),
        "last_error": safe_error(private.get("last_error")),
    }


def read_status(path):
    size = path.stat().st_size
    if size <= 0 or size > MAX_SOURCE_BYTES:
        raise ValueError("private status size is outside the allowed range")
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


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
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval-secs", type=float, default=15.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.interval_secs <= 0:
        parser.error("--interval-secs must be positive")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = args.output.parent / ".block-time-gap-backfill-publisher.lock"
    with lock_path.open("w", encoding="utf-8") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("block-time-gap status publisher is already running")

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        while not stop_requested:
            try:
                publish(args.output, public_status(read_status(args.source)))
            except Exception as error:
                print("status publish failed: {}".format(error), file=sys.stderr, flush=True)
                if args.once:
                    raise
            if args.once:
                break
            time.sleep(args.interval_secs)


if __name__ == "__main__":
    main()
