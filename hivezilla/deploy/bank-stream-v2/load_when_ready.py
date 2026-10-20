#!/usr/bin/env python3
"""Load live capture after catch-up, without stopping Agave for capture storage faults."""
import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import subprocess
import time
import urllib.request

GIB = 1024**3
STORAGE_MARGIN = 100 * GIB
SESSION_RE = re.compile(r"[0-9a-f]{32}-[0-9a-f]{1,16}\Z")
BOOTSTRAP_SECONDS = 1700
PROGRESS_SECONDS = 30


def read_json(path, limit=1024**2):
    if not stat.S_ISREG(path.lstat().st_mode):
        raise RuntimeError("invalid_metadata_file")
    with path.open("rb") as stream:
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise RuntimeError("metadata_too_large")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise RuntimeError("invalid_metadata_object")
    return value


def atomic_report(path, value):
    path.parent.mkdir(exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.chmod(0o600)
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def rpc(url, method, params):
    request = urllib.request.Request(url, json.dumps({"jsonrpc": "2.0", "id": 1,
        "method": method, "params": params}).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=8) as response:
        body = response.read(65537)
    if len(body) > 65536:
        raise RuntimeError("rpc_reply_too_large")
    data = json.loads(body)
    if not isinstance(data, dict) or "error" in data:
        raise RuntimeError("rpc_not_ready")
    return data["result"]


def verify_host(binary, expected_pid=None):
    pid = int(subprocess.check_output(["systemctl", "show", "hivezilla-validator.service",
        "-p", "MainPID", "--value"], text=True))
    if pid <= 0 or (expected_pid is not None and pid != expected_pid):
        raise RuntimeError("validator_pid_changed")
    if (Path("/proc") / str(pid) / "exe").resolve(strict=True) != binary.resolve(strict=True):
        raise RuntimeError("running_host_differs_from_tested_host")
    return pid


def capture_inventory(root):
    sessions = set()
    with os.scandir(root) as entries:
        for count, entry in enumerate(entries, 1):
            if count > 1024:
                raise RuntimeError("capture_inventory_limit")
            if SESSION_RE.fullmatch(entry.name) and entry.is_dir(follow_symlinks=False):
                sessions.add(entry.name)
    return sessions


def writer_present(session, pid, proc_root=Path("/proc")):
    stamp = (session / "LIVE.lock").lstat()
    if not stat.S_ISREG(stamp.st_mode):
        raise RuntimeError("invalid_capture_writer_lock")
    expected = (os.major(stamp.st_dev), os.minor(stamp.st_dev), stamp.st_ino)
    with (proc_root / "locks").open("rb") as stream:
        body = stream.read(1024**2 + 1)
    if len(body) > 1024**2:
        raise RuntimeError("lock_inventory_too_large")
    for line in body.decode("ascii").splitlines():
        fields = line.split()
        if len(fields) >= 8 and fields[1:4] == ["FLOCK", "ADVISORY", "WRITE"] and fields[4] == str(pid):
            device = fields[5].split(":")
            if len(device) == 3 and (int(device[0], 16), int(device[1], 16), int(device[2])) == expected:
                return True
    return False


def journal_status(session):
    if read_json(session / "FAILURE.json", 8192).get("state") == "FAILED":
        raise RuntimeError("new_capture_failed")
    with closing(sqlite3.connect((session / "events.sqlite").resolve().as_uri() + "?mode=ro",
                               uri=True, timeout=0.5)) as db:
        db.execute("PRAGMA query_only=ON")
        deadline = time.monotonic() + 0.5
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        row = db.execute("SELECT state,durable_sequence,latest_rooted_slot FROM status WHERE id=1").fetchone()
    if row is None or row[0] != "CAPTURING":
        raise RuntimeError("new_capture_not_running")
    if any(type(value) is not int or not 0 <= value <= 2**53 - 1 for value in row[1:]):
        raise RuntimeError("invalid_capture_counters")
    return row[1:]


def wait_for_progress(session, binary, pid):
    deadline = time.monotonic() + PROGRESS_SECONDS
    baseline = None
    while time.monotonic() < deadline:
        verify_host(binary, pid)
        current = journal_status(session)
        if not writer_present(session, pid):
            raise RuntimeError("new_capture_writer_missing")
        if baseline is None:
            baseline = current
        elif any(value < previous for value, previous in zip(current, baseline)):
            raise RuntimeError("capture_counter_regressed")
        elif all(value > previous for value, previous in zip(current, baseline)):
            return current
        time.sleep(0.5)
    raise RuntimeError("new_capture_progress_not_observed")


def run_loader(root, binary, config_path):
    config = read_json(config_path)
    reserve = config["reserve_disk_bytes"]
    if type(reserve) is not int or not 0 <= reserve <= 2**53 - 1:
        raise RuntimeError("invalid_capture_reserve")
    capture_root = Path(config["output_root"]).resolve(strict=True)
    if not capture_root.is_dir():
        raise RuntimeError("invalid_capture_root")
    pid = verify_host(binary)
    report = root / "reports/bank-v2-native-load.json"
    base = {"validator_binary": str(binary), "plugin_config": str(config_path), "validator_pid": pid,
            "host_executable_verified": True, "started_unix_ms": int(time.time() * 1000)}

    def record(state, **details):
        atomic_report(report, {**base, "state": state, "updated_unix_ms": int(time.time() * 1000), **details})

    def storage_blocked():
        verify_host(binary, pid)
        available = shutil.disk_usage(capture_root).free
        if available < reserve + STORAGE_MARGIN:
            record("CAPTURE_BLOCKED_STORAGE", free_disk_bytes=available,
                   capture_reserve_bytes=reserve, storage_margin_bytes=STORAGE_MARGIN)
            return True
        return False

    # Replace a previous boot's LOADED receipt before collecting current readiness.
    record("WAITING_FOR_LIVE_CAPTURE")
    if storage_blocked():
        return 0
    deadline = time.monotonic() + BOOTSTRAP_SECONDS
    network_at, network_slot = 0, None
    while time.monotonic() < deadline:
        if storage_blocked():
            return 0
        try:
            if rpc("http://127.0.0.1:8899", "getHealth", []) != "ok":
                raise RuntimeError("node_unhealthy")
            local = rpc("http://127.0.0.1:8899", "getSlot", [{"commitment": "processed"}])
            if network_slot is None or time.monotonic() - network_at > 5:
                network_slot = rpc("https://api.mainnet-beta.solana.com", "getSlot", [{"commitment": "finalized"}])
                network_at = time.monotonic()
            if any(type(slot) is not int or slot < 0 for slot in (local, network_slot)):
                raise RuntimeError("invalid_rpc_slot")
            if local < network_slot - 16:
                raise RuntimeError("node_catching_up")
            break
        except (OSError, ValueError, RuntimeError, KeyError):
            print("Waiting for live capture: RPC is not ready", flush=True)
            time.sleep(2)
    else:
        record("CAPTURE_NOT_READY", reason="validator_did_not_reach_live_health")
        return 1

    before = capture_inventory(capture_root)
    # RPC catch-up and directory reads can take time. Recheck immediately before load.
    if storage_blocked():
        return 0
    try:
        result = subprocess.run([str(binary), "--ledger", str(root / "ledger"), "plugin", "load", str(config_path)],
                                capture_output=True, text=True, timeout=30)
        new = capture_inventory(capture_root) - before
        if len(new) != 1:
            raise RuntimeError("native_load_did_not_create_one_session")
        session_path = capture_root / new.pop()
        session = read_json(session_path / "SESSION.json")
        if session.get("session_id") != session_path.name or session.get("bank_observations_v2") is not True:
            raise RuntimeError("invalid_new_capture_identity")
        base.update(session_id=session_path.name, agave_revision=session["agave_revision"])
        if result.returncode:
            raise RuntimeError("native_plugin_load_failed")
        durable, rooted = wait_for_progress(session_path, binary, pid)
        verify_host(binary, pid)
        record("LOADED", local_processed_slot=local, network_finalized_slot=network_slot,
               durable_sequence=durable, latest_rooted_slot=rooted, writer_lock_verified=True,
               loaded_unix_ms=int(time.time() * 1000))
        print("Native plugin loaded after validator catch-up and durable capture progress", flush=True)
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, sqlite3.Error, subprocess.SubprocessError) as error:
        # Capture readiness is separate from validator readiness. Keep Agave running.
        reason = str(error) if type(error) is RuntimeError else "new_capture_check_failed"
        record("CAPTURE_FAILED", reason=reason, operator_recovery_required=True)
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    raise SystemExit(run_loader(args.root, args.binary, args.config))
