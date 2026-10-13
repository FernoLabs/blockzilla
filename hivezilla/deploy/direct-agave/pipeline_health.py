#!/usr/bin/env python3
"""Read capture health from durable state. This tool does not change source data."""
import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import time
import urllib.request


def read_json(path, limit=1024 * 1024):
    with path.open("rb") as source:
        data = source.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f"JSON file exceeds limit: {path}")
    return json.loads(data)


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as target:
            json.dump(value, target, separators=(",", ":"))
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def rpc(url, method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params or []}).encode()
    request = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as response:
        value = json.load(response)
    if "error" in value:
        raise ValueError(f"{method}: {value['error']}")
    return value["result"]


def latest_session(root):
    # Session names contain a fixed-width timestamp, followed by the validator PID.
    sessions = sorted(path for path in root.iterdir()
                      if path.is_dir() and (path / "SESSION.json").is_file())
    if not sessions:
        raise ValueError("No direct Agave capture session is available")
    return sessions[-1]


def inspect_capture(session):
    import fcntl
    failure = read_json(session / "FAILURE.json")
    meta = read_json(session / "SESSION.json")
    with closing(sqlite3.connect((session / "events.sqlite").as_uri() + "?mode=ro",
                                 uri=True, timeout=2)) as connection:
        state, durable, error = connection.execute(
            "SELECT state,durable_sequence,error FROM status WHERE id=1").fetchone()
        newest = connection.execute(
            "SELECT max(slot) FROM events WHERE kind='rooted'").fetchone()[0]
        columns = {row[1] for row in connection.execute("PRAGMA table_info(status)")}
        if "latest_rooted_slot" in columns:
            high = connection.execute("SELECT latest_rooted_slot FROM status WHERE id=1").fetchone()[0]
            newest = max(newest or 0, high or 0) or None
    live = False
    with (session / "LIVE.lock").open("r") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            live = True
    problems = []
    if failure.get("state") == "FAILED" or state == "FAILED":
        problems.append(failure.get("error") or error or "Capture failed")
    if state == "CAPTURING" and not live:
        problems.append("Capture writer stopped without a clean close")
    retirement_failure = session / "RETIREMENT-FAILED.json"
    if retirement_failure.exists():
        retirement = read_json(retirement_failure)
        problems.append("Source retirement stopped: " + str(retirement.get("error", retirement)))
    sizes = sum(path.stat().st_size for path in session.iterdir() if path.is_file())
    config = meta["config"]
    return {"session_id": meta["session_id"], "path": str(session), "state": state,
            "node_identity": config["node_identity"], "genesis_hash": config["genesis_hash"],
            "validator_pid": int(meta["session_id"].rsplit("-", 1)[1], 16),
            "writer_live": live, "durable_sequence": durable,
            "latest_rooted_slot": newest, "journal_bytes": sizes,
            "journal_limit_bytes": config["max_journal_bytes"],
            "reserve_disk_bytes": config["reserve_disk_bytes"],
            "previous_unclean_sessions": meta.get("previous_unclean_sessions", []),
            "raw_shred_completeness": False}, problems


def report(args):
    now = int(time.time() * 1000)
    value = {"schema": "hivezilla-direct-pipeline-health-v1", "updated_unix_ms": now,
             "state": "HEALTHY", "problems": [], "warnings": [],
             "canonical": False, "full_epoch": False}
    try:
        value["validator"] = {"health": rpc(args.rpc, "getHealth"),
                              "finalized_slot": rpc(args.rpc, "getSlot", [{"commitment": "finalized"}]),
                              "identity": rpc(args.rpc, "getIdentity")["identity"],
                              "genesis_hash": rpc(args.rpc, "getGenesisHash")}
    except Exception as error:
        value["problems"].append(f"Validator health check failed: {error}")
    try:
        session = latest_session(args.capture_root)
        capture, problems = inspect_capture(session)
        value["capture"] = capture
        value["problems"].extend(problems)
        validator = value.get("validator", {})
        if validator and (validator["identity"] != capture["node_identity"]
                          or validator["genesis_hash"] != capture["genesis_hash"]):
            value["problems"].append("Capture identity or genesis differs from the local validator")
        if capture["state"] == "CLOSED":
            value["warnings"].append("Direct capture is closed; live capture is not active")
        if capture["journal_bytes"] >= capture["journal_limit_bytes"] * 0.8:
            value["warnings"].append("Capture journal uses at least 80 percent of its storage limit")
        if capture["previous_unclean_sessions"]:
            value["warnings"].append("An earlier session ended without a clean close; earlier coverage can have gaps")
        output = args.output_root / capture["session_id"]
        worker = read_json(output / "STATUS.json")
        value["worker"] = worker
        if worker.get("state") == "FAILED":
            value["problems"].append(worker.get("error", "Compact worker failed"))
        compact_slot = worker.get("last_compact_slot")
        if compact_slot is not None:
            capture["latest_rooted_slot"] = max(capture["latest_rooted_slot"] or 0, compact_slot)
        root_slot = capture["latest_rooted_slot"]
        if compact_slot is not None and root_slot is not None:
            value["compact_lag_slots"] = max(0, root_slot - compact_slot)
            if value["compact_lag_slots"] > 256:
                value["warnings"].append("Compact output is more than 256 rooted slots behind capture")
        age = max(0, now / 1000 - (output / "STATUS.json").stat().st_mtime)
        value["worker_status_age_seconds"] = int(age)
        if capture["state"] == "CAPTURING" and age > 120:
            value["problems"].append("Compact worker status has not changed for more than 120 seconds")
        free = shutil.disk_usage(args.capture_root).free
        value["free_disk_bytes"] = free
        if free < capture["reserve_disk_bytes"] + 20 * 1024**3:
            value["problems"].append("Free disk space is near the capture reserve")
    except Exception as error:
        value["problems"].append(f"Direct pipeline check failed: {error}")
    try:
        previous = read_json(args.report)
        if previous.get("capture", {}).get("session_id") == value.get("capture", {}).get("session_id"):
            old = previous["capture"]
            current = value["capture"]
            if current["durable_sequence"] > old["durable_sequence"]:
                value["last_durable_change_unix_ms"] = now
            else:
                value["last_durable_change_unix_ms"] = previous.get("last_durable_change_unix_ms", now)
            if current["state"] == "CAPTURING" and now - value["last_durable_change_unix_ms"] > 30000:
                value["problems"].append("No capture records became durable for more than 30 seconds")
        else:
            value["last_durable_change_unix_ms"] = now
    except (FileNotFoundError, KeyError, ValueError):
        value["last_durable_change_unix_ms"] = now
    if args.shred_root:
        try:
            current = read_json(args.shred_root / "CURRENT.json")
            status = read_json(Path(current["path"]) / "STATUS.json")
            value["shreds"] = status
            age = now - status["updated_unix_ms"]
            if status.get("state") != "ACTIVE" or age > 30000:
                value["warnings"].append("Raw shred capture is not current; no complete shred coverage claim")
        except Exception as error:
            value["warnings"].append(f"Raw shred status is unavailable: {error}")
    if value["problems"]:
        value["state"] = "FAILED"
    elif value["warnings"]:
        value["state"] = "WARNING"
    atomic_json(args.report, value)
    print(json.dumps(value, separators=(",", ":")))
    return 1 if value["problems"] else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--shred-root", type=Path)
    parser.add_argument("--rpc", default="http://127.0.0.1:8899")
    raise SystemExit(report(parser.parse_args()))
