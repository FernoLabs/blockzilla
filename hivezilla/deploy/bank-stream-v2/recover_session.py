#!/usr/bin/env python3
"""Recover a failed capture or adopt an explicitly tested revision. Do not restart Agave."""
import argparse
from contextlib import closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sqlite3
import stat
import subprocess
import time

NAMES = ["hivezilla-direct-worker", "hivezilla-bank-v2-producer",
         "hivezilla-bank-v2-origin", "hivezilla-stream-origin",
         "hivezilla-stream-relay", "hivezilla-bank-v2-r2-publisher",
         "hivezilla-native-cold-storage-20261012-live"]
ACTIVE_SOURCE = "config/native-active-source.json"
SOURCE_DROPIN = "z" * 96 + "-native-source.conf"
GIB = 1024**3
MAX_SAFE_INTEGER = 2**53 - 1


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".new")
    with open(temp, "w") as f:
        f.write(value)
        f.flush()
        os.fsync(f.fileno())
    temp.chmod(0o600)
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def json_write(path, value):
    write(path, json.dumps(value, indent=2) + "\n")


def exec_args(text):
    lines = [s.removeprefix("ExecStart=") for s in text.splitlines()
             if s.startswith("ExecStart=") and s != "ExecStart="]
    if not lines:
        raise RuntimeError("missing_exec_start")
    return shlex.split(lines[-1])


def replace_option(args, name, value):
    args = args.copy()
    if args.count(name) != 1:
        raise RuntimeError("missing_or_repeated_option")
    i = args.index(name)
    if i + 1 == len(args) or args[i + 1].startswith("--"):
        raise RuntimeError("missing_option_value")
    args[i + 1] = str(value)
    return args


def set_option(args, name, value):
    if name in args:
        return replace_option(args, name, value)
    return [*args, name, str(value)]


def storage_command(args, root, session):
    """Bind every storage checkpoint to the same new source as the feed."""
    paths = {
        "--output": root / "compact/direct-agave-processed-live" / session,
        "--bank-output": root / "compact/bank-v2-live" / session,
        "--capture": root / "captures/direct-agave-live" / session,
        "--publisher-state": root / "reports/native-processed-r2-bank-v2" / session,
        "--v1-spool": root / "stream-relay-spool-bank-v2" / session,
        "--bank-origin-spool": root / "bank-v2-origin-spool" / session,
    }
    for option, path in paths.items():
        args = replace_option(args, option, path)
    # Do not change the credential file, minimum local age, byte checks or limits.
    return args


def replacement_commands(commands, root, session, expected, worker_binary=None, builder_binary=None):
    """Preflight the complete switch without creating files or stopping units."""
    processed = root / "compact/direct-agave-processed-live" / session
    banks = root / "compact/bank-v2-live" / session
    origin_spool = root / "bank-v2-origin-spool" / session
    v1_spool = root / "stream-relay-spool-bank-v2" / session
    v2_spool = root / "bank-v2-relay-spool" / session
    publisher_state = root / "reports/native-processed-r2-bank-v2" / session
    producer = commands["hivezilla-bank-v2-producer"].copy()
    if len(producer) != 5 or producer[1] != "build-confirmed" or producer[-1] != "--follow":
        raise RuntimeError("unexpected_producer_command")
    producer[2:4] = [str(processed), str(banks)]
    if worker_binary is not None:
        producer[0] = str(worker_binary)
    bank_origin = replace_option(commands["hivezilla-bank-v2-origin"], "--output", banks)
    bank_origin = replace_option(bank_origin, "--spool", origin_spool)
    v1_origin = replace_option(commands["hivezilla-stream-origin"], "--output", processed)
    relay = replace_option(commands["hivezilla-stream-relay"], "--spool", v1_spool)
    relay = replace_option(relay, "--banks-spool", v2_spool)
    relay = replace_option(relay, "--banks-source-file", expected)
    publisher = replace_option(commands["hivezilla-bank-v2-r2-publisher"], "--output", processed)
    publisher = replace_option(publisher, "--state", publisher_state)
    storage = storage_command(commands["hivezilla-native-cold-storage-20261012-live"], root, session)
    replacements = [
        ("hivezilla-bank-v2-producer", producer, [banks, processed / "feed.sqlite-shm"]),
        ("hivezilla-bank-v2-origin", bank_origin, [origin_spool, banks / "feed.sqlite-shm"]),
        ("hivezilla-stream-origin", v1_origin, [processed / "feed.sqlite-shm"]),
        ("hivezilla-stream-relay", relay, [v1_spool, v2_spool]),
        ("hivezilla-bank-v2-r2-publisher", publisher, [publisher_state, processed / "feed.sqlite-shm"]),
        ("hivezilla-native-cold-storage-20261012-live", storage,
         [processed, banks / "feed.sqlite-shm", root / "captures/direct-agave-live" / session / "events.sqlite-shm"]),
    ]
    if worker_binary is not None:
        worker = replace_option(commands["hivezilla-direct-worker"], "--engine", worker_binary)
        if builder_binary is not None:
            worker = replace_option(worker, "--blockzilla", builder_binary)
        replacements.insert(0, ("hivezilla-direct-worker", worker, None))
    for _, args, writable in replacements:
        override_text(args, writable)  # Validate command quoting before a reload.
    return replacements


def check_source_override_order(unit, text):
    for line in text.splitlines():
        if line.startswith("# ") and unit + ".service.d/" in line and line.endswith(".conf"):
            name = Path(line[2:]).name
            if name != SOURCE_DROPIN and name >= SOURCE_DROPIN:
                raise RuntimeError("source_override_does_not_sort_last")


def tail_plan(output, state, source):
    """Count the closed feed tail without changing its checkpoint or intent."""
    checkpoint = json.loads((state / "CHECKPOINT.json").read_text())
    if checkpoint["session"] != source["session_id"] or Path(checkpoint["source"]).resolve() != output.resolve():
        raise RuntimeError("old_publisher_source_mismatch")
    session, position, _ = checkpoint["cursor"].split(":")
    if session != source["session_id"] or not position.startswith("p") or not position[1:].isdigit():
        raise RuntimeError("old_publisher_cursor_mismatch")
    after = int(position[1:])
    sequence = checkpoint["sequence"]
    if type(sequence) is not int or sequence < 0:
        raise RuntimeError("invalid_old_publisher_sequence")
    with closing(sqlite3.connect((output / "feed.sqlite").resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("PRAGMA query_only=ON")
        maximum = db.execute("SELECT max(sequence) FROM feed_events").fetchone()[0]
        if after > (maximum or 0):
            raise RuntimeError("old_publisher_checkpoint_beyond_source")
        count, first, last = db.execute(
            "SELECT count(*),min(sequence),max(sequence) FROM feed_events WHERE sequence>?", (after,)).fetchone()
    if count and (first != after + 1 or last != after + count):
        raise RuntimeError("old_publisher_tail_gap")
    return {"events": count, "checkpoint_sequence": sequence,
            "expected_sequence": sequence + count, "last_position": last if count else after}


def finite_drain_command(args, output, state, tail):
    if type(tail["events"]) is not int or tail["events"] <= 0:
        raise RuntimeError("no_publisher_tail_to_drain")
    args = replace_option(replace_option(args, "--output", output), "--state", state)
    return set_option(args, "--max-chunks", tail["events"])


def drain_closed_source(command_file):
    """Recount after every process restart; a partial commit reduces the bound."""
    if command_file.is_symlink() or command_file.stat().st_size > 65536:
        raise RuntimeError("invalid_drain_command_file")
    args = json.loads(command_file.read_text())
    if (not isinstance(args, list) or not all(isinstance(arg, str) for arg in args)
            or len(args) < 3 or args[1] != "publish" or args.count("--processed") != 1):
        raise RuntimeError("invalid_processed_drain_command")
    systemd_command(args)  # Apply the same argument checks as the saved unit.
    if args.count("--output") != 1 or args.count("--state") != 1:
        raise RuntimeError("invalid_drain_source_options")
    output = Path(args[args.index("--output") + 1]).resolve(strict=True)
    state = Path(args[args.index("--state") + 1]).resolve(strict=True)
    with open(state / "DRAIN.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source = source_tuple(json.loads((output / "FEED.json").read_text()))
        feed_state = json.loads((output / "STATUS.json").read_text())["state"]
        if feed_state == "DRAINED":
            record = json.loads((command_file.parent / "RECOVERY.json").read_text())
            root = Path(record["root"]).resolve(strict=True)
            upgrade = record.get("revision_upgrade")
            if (not upgrade or source != record["old_source"]
                    or source["agave_revision"] != upgrade["old_revision"]
                    or output != root / "compact/direct-agave-processed-live" / source["session_id"]
                    or state != root / "reports/native-processed-r2-bank-v2" / source["session_id"]
                    or terminal_upgrade_capture(root, source) != "CLOSED"):
                raise RuntimeError("drain_source_is_not_closed_failed")
            terminal_upgrade_feed(root, source, "CLOSED")
        elif feed_state != "FAILED":
            raise RuntimeError("drain_source_is_not_closed_failed")
        tail = tail_plan(output, state, source)
        if tail["events"]:
            subprocess.run(finite_drain_command(args, output, state, tail), check=True)
        final = tail_plan(output, state, source)
        if final["events"] or final["checkpoint_sequence"] != tail["expected_sequence"]:
            raise RuntimeError("publisher_tail_not_complete")
        intent = state / "INTENT.json"
        if intent.exists():
            raw = json.loads(intent.read_text())
            commits = raw.get("commits", [raw])
            checkpoint = json.loads((state / "CHECKPOINT.json").read_text())
            if (not commits or commits[-1].get("sequence") != checkpoint["sequence"]
                    or commits[-1].get("cursor") != checkpoint["cursor"]):
                raise RuntimeError("publisher_tail_pending_intent")
            # A committed stale intent is kept for the publisher's own recovery.
        json_write(state / "DRAIN-RECEIPT.json", {
            "state": "COMPLETE", "source": source, "checkpoint_sequence": final["checkpoint_sequence"],
            "tail_position": final["last_position"], "updated_unix_ms": int(time.time() * 1000),
            "remote_head_checked_by_publisher": bool(tail["events"]),
        })


def systemd_command(args):
    # systemd quoting is not shell quoting. Reject control characters and escapes
    # so a saved command cannot add a new directive or expand a specifier.
    if any(any(c in a for c in "\n\r\0%$") for a in args):
        raise RuntimeError("unsafe_systemd_argument")
    return " ".join(json.dumps(a) for a in args)


def override_text(args, writable=None, extra=""):
    text = "[Service]\nExecStart=\nExecStart=" + systemd_command(args) + "\n"
    if writable is not None:
        text += "ReadWritePaths=\nReadWritePaths=" + systemd_command([str(p) for p in writable]) + "\n"
    return text + extra


def install(report, path, text):
    saved = report / "saved" / str(path).lstrip("/")
    if path.exists() and not saved.exists():
        # Installed unit metadata can be root-owned with mode 0600. Preserve its
        # exact text through the same privileged read used for unit discovery.
        previous = subprocess.check_output(["sudo", "cat", "--", str(path)], text=True)
        write(saved, previous)
    new = report / "new" / str(path).lstrip("/")
    write(new, text)
    # systemd configuration is public metadata. A root umask of 077 must not
    # make a newly created drop-in directory unreadable to the operating user.
    run("sudo", "install", "-d", "-m", "755", str(path.parent))
    run("sudo", "install", "-m", "644", str(new), str(path))


def wait(check, seconds, error):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.5)
    raise RuntimeError(error)


def source_tuple(header):
    return {k: header[k] for k in ["genesis_hash", "node_identity", "agave_revision", "session_id"]}


def recovery_capacity(root, now_ms=None):
    """Require fresh snapshot headroom when the capacity collector is installed."""
    current_free = shutil.disk_usage(root).free
    report = root / "reports/native-capacity"
    try:
        mode = report.lstat().st_mode
    except FileNotFoundError:
        if current_free < 500 * GIB:
            raise RuntimeError("less_than_100_GiB_above_capture_reserve")
        return {"mode": "legacy", "free_disk_bytes": current_free, "required_free_bytes": 500 * GIB}
    if not stat.S_ISDIR(mode):
        raise RuntimeError("recovery_capacity_report_directory_invalid")

    def read_object(path):
        if not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError("invalid_capacity_metadata")
        with path.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError("capacity_metadata_too_large")
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError("invalid_capacity_object")
        return value

    try:
        pin = read_object(root / ACTIVE_SOURCE)
        if (set(pin) != {"genesis_hash", "node_identity", "agave_revision", "session_id"}
                or not all(isinstance(value, str) for value in pin.values())
                or any(not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", pin[key])
                       for key in ("genesis_hash", "node_identity"))
                or not re.fullmatch(r"[0-9a-f]{40}", pin["agave_revision"])
                or not re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{1,16}", pin["session_id"])):
            raise ValueError("invalid_capacity_source_pin")
        data = read_object(report / "STATUS.json")
        if (data.get("schema") != "blockzilla-native-capacity-v1" or data.get("state") != "RUNNING"
                or data.get("source") != pin):
            raise ValueError("capacity_report_not_current")
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        stamp = data["updated_unix_ms"]
        if type(stamp) is not int or not 0 <= now_ms - stamp <= 90000:
            raise ValueError("capacity_report_not_fresh")
        names = ("free_disk_bytes", "capture_reserve_bytes", "recovery_minimum_free_bytes", "next_archive_budget_bytes")
        if any(type(data.get(name)) is not int or not 0 <= data[name] <= MAX_SAFE_INTEGER for name in names):
            raise ValueError("invalid_capacity_budget")
        free, reserve, minimum, archive = (data[name] for name in names)
        if reserve != 400 * GIB or minimum < 500 * GIB or archive < 128 * GIB:
            raise ValueError("unsafe_capacity_budget")
        snapshot_headroom, recovery_headroom = free - reserve - archive, free - minimum - archive
        for name, expected in (("snapshot_headroom_bytes", snapshot_headroom),
                               ("recovery_headroom_bytes", recovery_headroom)):
            if type(data.get(name)) is not int or data[name] != expected:
                raise ValueError("invalid_capacity_math")
        for name, expected in (("snapshot_capture_reserve_at_risk", snapshot_headroom < 0),
                               ("snapshot_headroom_low", recovery_headroom < 0)):
            if type(data.get(name)) is not bool or data[name] != expected:
                raise ValueError("invalid_capacity_math")
        expected_state = "CRITICAL" if snapshot_headroom < 0 else "WARNING" if recovery_headroom < 0 else "OK"
        if data.get("capacity_state") != expected_state or read_object(root / ACTIVE_SOURCE) != pin:
            raise ValueError("invalid_capacity_state_or_source_change")
        required = minimum + archive
        if required > MAX_SAFE_INTEGER:
            raise ValueError("invalid_capacity_threshold")
    except (OSError, ValueError, KeyError, TypeError):
        raise RuntimeError("recovery_capacity_report_invalid_or_stale") from None
    # Use fresh filesystem space, not the collector's earlier free-space sample.
    current_free = shutil.disk_usage(root).free
    if current_free < required:
        raise RuntimeError("insufficient_recovery_snapshot_headroom")
    return {"mode": "capacity_report", "source": pin, "report_updated_unix_ms": stamp,
            "free_disk_bytes": current_free, "required_free_bytes": required}


def existing_capture(root, session_id, old, pid, expected_revision=None):
    """Adopt only an explicitly named live source from the running host."""
    if not re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{1,16}", session_id):
        raise RuntimeError("invalid_existing_capture_session")
    if int(session_id.rsplit("-", 1)[1], 16) != int(pid) or session_id == old["session_id"]:
        raise RuntimeError("existing_capture_host_mismatch")
    session = root / "captures/direct-agave-live" / session_id
    if session.is_symlink() or session.resolve(strict=True).parent != (root / "captures/direct-agave-live").resolve():
        raise RuntimeError("unsafe_existing_capture")
    meta = json.loads((session / "SESSION.json").read_text())
    if (meta.get("session_id") != session_id
            or meta.get("agave_revision") != (expected_revision or old["agave_revision"])
            or meta.get("bank_observations_v2") is not True
            or any(meta["config"].get(k) != old[k] for k in ["genesis_hash", "node_identity"])):
        raise RuntimeError("existing_capture_source_mismatch")
    if ((session / "CLOSED.json").exists()
            or json.loads((session / "FAILURE.json").read_text()).get("state") == "FAILED"):
        raise RuntimeError("existing_capture_failed_or_closed")
    with closing(sqlite3.connect((session / "events.sqlite").as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("PRAGMA query_only=ON")
        status = db.execute("SELECT state,durable_sequence FROM status WHERE id=1").fetchone()
    if not status or status[0] != "CAPTURING" or status[1] <= 0:
        raise RuntimeError("existing_capture_not_durable")
    with (session / "LIVE.lock").open("r") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return session
    raise RuntimeError("existing_capture_writer_missing")


def revision_upgrade_options(session_id, old_revision, new_revision, worker_binary, worker_sha256,
                             builder_binary=None, builder_sha256=None):
    values = (old_revision, new_revision, worker_binary, worker_sha256, builder_binary, builder_sha256)
    if not any(value is not None for value in values):
        return None
    if (not session_id or not all(value is not None for value in values)
            or any(not re.fullmatch(r"[0-9a-f]{40}", value) for value in (old_revision, new_revision))
            or old_revision == new_revision
            or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in (worker_sha256, builder_sha256))):
        raise RuntimeError("revision_upgrade_requires_session_revisions_and_tested_artifacts")
    return {"old_revision": old_revision, "new_revision": new_revision,
            "worker_binary": str(worker_binary), "worker_sha256": worker_sha256,
            "builder_binary": str(builder_binary), "builder_sha256": builder_sha256}


def verify_upgrade_worker(root, upgrade):
    worker = verify_upgrade_artifact(root, upgrade["worker_binary"], upgrade["worker_sha256"], "worker")
    verify_upgrade_artifact(root, upgrade["builder_binary"], upgrade["builder_sha256"], "builder")
    return worker


def verify_upgrade_artifact(root, path, digest, label):
    binary = Path(path)
    if (not binary.is_absolute() or binary.is_symlink() or not binary.is_file()
            or binary.resolve(strict=True) != binary
            or not binary.is_relative_to(root / "releases") or not os.access(binary, os.X_OK)
            or hashlib.sha256(binary.read_bytes()).hexdigest() != digest):
        raise RuntimeError("upgrade_" + label + "_artifact_mismatch")
    return binary


def verify_upgrade_load(root, pid, session_id, revision, binary):
    receipt = json.loads((root / "reports/bank-v2-native-load.json").read_text())
    if (receipt.get("state") != "LOADED" or receipt.get("validator_pid") != int(pid)
            or receipt.get("session_id") != session_id or receipt.get("agave_revision") != revision
            or receipt.get("host_executable_verified") is not True
            or receipt.get("writer_lock_verified") is not True
            or Path(receipt.get("validator_binary", "")).resolve() != binary):
        raise RuntimeError("upgrade_native_load_receipt_mismatch")


def terminal_upgrade_capture(root, source):
    """Require exact old-source evidence and a released writer before an upgrade."""
    capture = root / "captures/direct-agave-live" / source["session_id"]
    if capture.is_symlink() or capture.resolve(strict=True).parent != (root / "captures/direct-agave-live").resolve():
        raise RuntimeError("unsafe_old_upgrade_capture")
    meta = json.loads((capture / "SESSION.json").read_text())
    actual = {"session_id": meta.get("session_id"), "agave_revision": meta.get("agave_revision"),
              **{key: meta["config"].get(key) for key in ("genesis_hash", "node_identity")}}
    if actual != source or meta.get("bank_observations_v2") is not True:
        raise RuntimeError("old_upgrade_capture_source_mismatch")
    with (capture / "LIVE.lock").open("r") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("old_upgrade_capture_writer_is_live") from None
        if json.loads((capture / "FAILURE.json").read_text()).get("state") == "FAILED":
            return "FAILED"
        closed = json.loads((capture / "CLOSED.json").read_text())
        wal = capture / "events.sqlite-wal"
        if wal.exists() and wal.stat().st_size:
            raise RuntimeError("old_upgrade_capture_has_uncheckpointed_wal")
        # A clean close has checkpointed and released the writer. Immutable mode
        # avoids creating SQLite lock files inside a read-only drain service.
        with closing(sqlite3.connect((capture / "events.sqlite").as_uri() + "?mode=ro&immutable=1", uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            status = db.execute("SELECT state,durable_sequence FROM status WHERE id=1").fetchone()
        if (not status or status[0] != "CLOSED" or closed.get("state") != "CLOSED"
                or closed.get("durable_sequence") != status[1]):
            raise RuntimeError("old_upgrade_capture_is_not_terminal")
    return "CLOSED"


def terminal_upgrade_feed(root, source, capture_state):
    output = root / "compact/direct-agave-processed-live" / source["session_id"]
    status = json.loads((output / "STATUS.json").read_text())
    if (source_tuple(json.loads((output / "FEED.json").read_text())) != source
            or status.get("state") != ("DRAINED" if capture_state == "CLOSED" else "FAILED")):
        raise RuntimeError("old_upgrade_processed_feed_is_not_terminal")
    if capture_state == "CLOSED":
        closed = json.loads((root / "captures/direct-agave-live" / source["session_id"] / "CLOSED.json").read_text())
        with (output / "WORKER.lock").open("r") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("old_upgrade_processed_worker_is_live") from None
            with closing(sqlite3.connect((output / "feed.sqlite").as_uri() + "?mode=ro", uri=True)) as db:
                db.execute("PRAGMA query_only=ON")
                row = db.execute("SELECT source_sequence,event_sequence FROM feed_state WHERE id=1").fetchone()
                dirty = db.execute("SELECT count(*) FROM banks WHERE dirty=1").fetchone()[0]
            if (not row or row[0] != closed["durable_sequence"] or dirty != 0
                    or status.get("source_sequence") != row[0] or status.get("event_sequence") != row[1]):
                raise RuntimeError("old_upgrade_processed_feed_is_not_drained")


def recover(root, report, session_id=None, expected_old_revision=None, expected_new_revision=None,
            worker_binary=None, worker_sha256=None, builder_binary=None, builder_sha256=None):
    upgrade = revision_upgrade_options(session_id, expected_old_revision, expected_new_revision,
                                       worker_binary, worker_sha256, builder_binary, builder_sha256)
    root = root.resolve(strict=True)
    new_worker = verify_upgrade_worker(root, upgrade) if upgrade else None
    new_builder = Path(upgrade["builder_binary"]) if upgrade else None
    recovery_capacity(root)
    report.mkdir(parents=True, exist_ok=False, mode=0o700)
    current_relay = exec_args(run("sudo", "systemctl", "cat", "hivezilla-stream-relay.service"))
    if current_relay.count("--banks-source-file") != 1:
        raise RuntimeError("missing_current_source_pin")
    old = json.loads(Path(current_relay[current_relay.index("--banks-source-file") + 1]).read_text())
    old = source_tuple(old)
    if (not all(isinstance(value, str) for value in old.values())
            or not re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{1,16}", old["session_id"])):
        raise RuntimeError("invalid_old_source")
    active_source = root / ACTIVE_SOURCE
    if active_source.exists() and json.loads(active_source.read_text()) != source_tuple(old):
        raise RuntimeError("active_source_pin_mismatch")
    old_capture = root / "captures/direct-agave-live" / old["session_id"]
    if upgrade:
        if old["agave_revision"] != upgrade["old_revision"]:
            raise RuntimeError("upgrade_old_revision_mismatch")
        old_terminal = terminal_upgrade_capture(root, old)
        terminal_upgrade_feed(root, old, old_terminal)
    else:
        failed = json.loads((old_capture / "FAILURE.json").read_text())
        if failed.get("state") != "FAILED":
            raise RuntimeError("old_capture_is_not_failed")
    units = {name: run("sudo", "systemctl", "cat", name + ".service") for name in NAMES}
    for name, text in units.items():
        if upgrade or name != "hivezilla-direct-worker":
            check_source_override_order(name, text)
    commands = {name: exec_args(text) for name, text in units.items()}
    replacement_commands(commands, root, old["session_id"], report / "expected-source.json", new_worker, new_builder)
    if upgrade and Path(commands["hivezilla-direct-worker"][
            commands["hivezilla-direct-worker"].index("--engine") + 1]).resolve() == new_worker:
        raise RuntimeError("upgrade_worker_path_is_not_new")
    if upgrade and Path(commands["hivezilla-direct-worker"][
            commands["hivezilla-direct-worker"].index("--blockzilla") + 1]).resolve() == new_builder:
        raise RuntimeError("upgrade_builder_path_is_not_new")
    old_processed = root / "compact/direct-agave-processed-live" / old["session_id"]
    old_state = root / "reports/native-processed-r2-bank-v2" / old["session_id"]
    # Preflight storage arguments before a plugin reload makes a new source.
    old_storage = commands["hivezilla-native-cold-storage-20261012-live"]
    if storage_command(old_storage, root, old["session_id"]) != old_storage:
        raise RuntimeError("old_cold_storage_source_mismatch")
    for name, text in units.items():
        write(report / (name + ".previous-unit"), text + "\n")
    config = root / "config/direct-agave.json"
    cfg = json.loads(config.read_text())
    if not cfg.get("bank_v2") or cfg["reserve_disk_bytes"] != 400 * 1024**3:
        raise RuntimeError("unexpected_native_config")
    shutil.copy2(config, report / "previous-plugin-config.json")
    pid = run("systemctl", "show", "hivezilla-validator", "-p", "MainPID", "--value")
    if not int(pid):
        raise RuntimeError("validator_not_running")
    adopted = (existing_capture(root, session_id, old, pid, upgrade["new_revision"]) if upgrade else
               existing_capture(root, session_id, old, pid) if session_id else None)
    host = Path("/proc") / pid / "exe"
    binary = host.resolve(strict=True)
    plugin = Path(cfg["libpath"])
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in [binary, plugin]}
    if upgrade:
        verify_upgrade_load(root, pid, session_id, upgrade["new_revision"], binary)
        hashes[str(new_worker)] = upgrade["worker_sha256"]
        hashes[str(new_builder)] = upgrade["builder_sha256"]
    record = {"schema": "blockzilla-native-session-recovery-v1", "state": "PREPARED",
              "root": str(root),
              "old_source": old, "validator_pid": int(pid), "artifacts": hashes,
              "old_capture_preserved": True, "gap_rebuilt": False,
              "adopt_existing_capture": bool(adopted),
              "started_unix_ms": int(time.time() * 1000)}
    if adopted:
        record["session_id"] = session_id
    if not upgrade:
        record["old_failure_sha256"] = hashlib.sha256((old_capture / "FAILURE.json").read_bytes()).hexdigest()
        record["old_session_sha256"] = hashlib.sha256((old_capture / "SESSION.json").read_bytes()).hexdigest()
    if upgrade:
        record.update(revision_upgrade=upgrade, old_capture_state=old_terminal,
                      session_id=session_id)
    json_write(report / "RECOVERY.json", record)
    before = {p.name for p in (root / "captures/direct-agave-live").iterdir() if p.is_dir()}
    recovery_capacity(root)
    run("sudo", "systemctl", "stop", "hivezilla-direct-worker", "hivezilla-bank-v2-producer",
        "hivezilla-bank-v2-r2-publisher")
    tail = tail_plan(old_processed, old_state, old)
    record["old_publisher_tail"] = tail
    json_write(report / "RECOVERY.json", record)
    if adopted is None:
        run(str(binary), "--ledger", str(root / "ledger"), "plugin", "reload",
            "hivezilla-direct-agave-v1", str(config))

    def capture_ready():
        paths = [p for p in (root / "captures/direct-agave-live").iterdir()
                 if p.is_dir() and p.name not in before and (p / "events.sqlite").is_file()
                 and (p / "SESSION.json").is_file()]
        if len(paths) > 1:
            raise RuntimeError("multiple_new_capture_sessions")
        if not paths:
            return None
        meta = json.loads((paths[0] / "SESSION.json").read_text())
        if meta["agave_revision"] != old["agave_revision"] or not meta["bank_observations_v2"]:
            raise RuntimeError("new_native_source_revision_mismatch")
        return paths[0]

    session = adopted or wait(capture_ready, 30, "new_capture_did_not_start")
    prepare_source_switch(root, report, record, commands, pid, session)


def prepare_source_switch(root, report, record, commands, pid, session):
    """Resume an adopted source without loading a second native capture."""
    old = record["old_source"]
    upgrade = record.get("revision_upgrade")
    new_worker = verify_upgrade_worker(root, upgrade) if upgrade else None
    new_builder = Path(upgrade["builder_binary"]) if upgrade else None
    tail = record["old_publisher_tail"]
    old_processed = root / "compact/direct-agave-processed-live" / old["session_id"]
    old_state = root / "reports/native-processed-r2-bank-v2" / old["session_id"]
    record.update(state="CAPTURE_STARTED", session_id=session.name)
    json_write(report / "RECOVERY.json", record)
    if upgrade:
        worker_args = replace_option(commands["hivezilla-direct-worker"], "--engine", new_worker)
        worker_args = replace_option(worker_args, "--blockzilla", new_builder)
        install(report, Path("/etc/systemd/system/hivezilla-direct-worker.service.d") / SOURCE_DROPIN,
                override_text(worker_args))
        run("sudo", "systemctl", "daemon-reload")
    run("sudo", "systemctl", "reset-failed", "hivezilla-direct-worker")
    run("sudo", "systemctl", "start", "hivezilla-direct-worker")
    processed = root / "compact/direct-agave-processed-live" / session.name

    def payload_ready():
        p = processed / "STATUS.json"
        if not p.exists():
            return False
        status = json.loads(p.read_text())
        if status["state"] == "FAILED":
            raise RuntimeError("new_processed_worker_failed")
        return status.get("event_sequence", 0) >= 32

    wait(payload_ready, 120, "new_processed_feed_not_ready")
    source = source_tuple(json.loads((processed / "FEED.json").read_text()))
    expected_identity = dict(old, agave_revision=upgrade["new_revision"]) if upgrade else old
    if {k: v for k, v in source.items() if k != "session_id"} != {k: v for k, v in expected_identity.items() if k != "session_id"}:
        raise RuntimeError("new_source_identity_changed")
    if source["session_id"] != session.name:
        raise RuntimeError("new_source_session_mismatch")
    expected = report / "expected-source.json"
    json_write(expected, source)
    banks = root / "compact/bank-v2-live" / session.name
    origin_spool = root / "bank-v2-origin-spool" / session.name
    v1_spool = root / "stream-relay-spool-bank-v2" / session.name
    v2_spool = root / "bank-v2-relay-spool" / session.name
    publisher_state = root / "reports/native-processed-r2-bank-v2" / session.name
    paths = [banks, origin_spool, v1_spool, v2_spool, publisher_state]
    planned = [str(path) for path in paths]
    path_error = "upgrade" if upgrade else "recovery"
    if "switch_paths" not in record:
        if any(path.exists() or path.is_symlink() for path in paths):
            raise RuntimeError(path_error + "_switch_path_already_exists")
        record["switch_paths"] = planned
        json_write(report / "RECOVERY.json", record)
    if record["switch_paths"] != planned:
        raise RuntimeError(path_error + "_saved_switch_paths_mismatch")
    for path in paths:
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise RuntimeError("unsafe_" + path_error + "_switch_path")
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    replacements = replacement_commands(commands, root, session.name, expected, new_worker, new_builder)
    # Keep a separate publisher for the failed session. It has a distinct source
    # and durable checkpoint; it must not write the new session's cloud head.
    drain_name = "hivezilla-bank-v2-r2-drain-" + old["session_id"] if tail["events"] else None
    if drain_name:
        drain = finite_drain_command(commands["hivezilla-bank-v2-r2-publisher"], old_processed, old_state, tail)
        drain_file = report / "old-publisher-command.json"
        write(drain_file, json.dumps(drain, indent=2) + "\n")
        drain_runner = report / "drain_closed_source.py"
        shutil.copy2(Path(__file__), drain_runner)
        drain_runner.chmod(0o600)
        drain_unit = ("[Unit]\nAfter=network-online.target\n[Service]\nUser=ubuntu\nGroup=ubuntu\nUMask=0077\n"
                  "ExecStart=" + systemd_command(["/usr/bin/python3", str(drain_runner),
                      "--drain-command-file", str(drain_file)]) + "\nRestart=on-failure\nRestartSec=30\n"
                  "MemoryMax=2G\nCPUQuota=200%\nNice=10\nIOWeight=10\nProtectSystem=strict\n"
                  "ProtectHome=read-only\nReadWritePaths=" + str(old_state) + " " + str(old_processed / "feed.sqlite-shm") +
                  "\n[Install]\nWantedBy=multi-user.target\n")
        install(report, Path("/etc/systemd/system") / (drain_name + ".service"), drain_unit)
    record.update(state="SWITCH_PREPARED", source=source, processed_output=str(processed),
                  bank_output=str(banks), origin_spool=str(origin_spool), relay_spool=str(v2_spool),
                  source_file=str(expected), old_source_drain_unit=drain_name)
    # Save the old retention command. Its data must stay recoverable after the
    # live unit changes source; this patch does not start another background drain.
    write(report / "old-cold-storage-command.json", json.dumps(
        commands["hivezilla-native-cold-storage-20261012-live"], indent=2) + "\n")
    record["old_cold_storage_resume_required"] = True
    json_write(report / "RECOVERY.json", record)
    finish_switch(root, report, record, commands, pid)


def finish_switch(root, report, record, commands, pid):
    source = record["source"]
    session = source["session_id"]
    expected = Path(record["source_file"])
    if json.loads(expected.read_text()) != source:
        raise RuntimeError("recovery_source_file_mismatch")
    processed = root / "compact/direct-agave-processed-live" / session
    banks = root / "compact/bank-v2-live" / session
    publisher_state = root / "reports/native-processed-r2-bank-v2" / session
    upgrade = record.get("revision_upgrade")
    worker_binary = verify_upgrade_worker(root, upgrade) if upgrade else None
    builder_binary = Path(upgrade["builder_binary"]) if upgrade else None
    replacements = replacement_commands(commands, root, session, expected, worker_binary, builder_binary)
    for name, _, _ in replacements:
        check_source_override_order(name, run("sudo", "systemctl", "cat", name + ".service"))
    stop_names = ["hivezilla-stream-relay", "hivezilla-stream-origin",
                  "hivezilla-bank-v2-origin", "hivezilla-bank-v2-r2-publisher",
                  "hivezilla-native-cold-storage-20261012-live", "hivezilla-bank-v2-producer"]
    if upgrade:
        stop_names.append("hivezilla-direct-worker")
    run("sudo", "systemctl", "stop", *stop_names)
    for name, args, writable in replacements:
        install(report, Path("/etc/systemd/system") / (name + ".service.d") / SOURCE_DROPIN,
                override_text(args, writable))
    run("sudo", "systemctl", "daemon-reload")
    run("sudo", "systemctl", "reset-failed", *[n for n, _, _ in replacements])
    if upgrade:
        run("sudo", "systemctl", "start", "hivezilla-direct-worker")
    run("sudo", "systemctl", "start", "hivezilla-bank-v2-producer")
    wait(lambda: (banks / "FEED.json").exists() and (banks / "feed.sqlite-shm").exists(), 30,
         "new_bank_feed_not_initialized")
    run("sudo", "systemctl", "start", "hivezilla-stream-origin", "hivezilla-bank-v2-origin")
    time.sleep(2)
    run("sudo", "systemctl", "start", "hivezilla-stream-relay", "hivezilla-bank-v2-r2-publisher")
    wait(lambda: (publisher_state / "CHECKPOINT.json").exists(), 30, "new_publisher_not_initialized")
    run("sudo", "systemctl", "start", "hivezilla-native-cold-storage-20261012-live")
    wait(lambda: (processed / "cold-storage/STATUS.json").exists(), 120, "new_cold_storage_not_initialized")
    if record.get("old_source_drain_unit"):
        run("sudo", "systemctl", "start", record["old_source_drain_unit"])
    if run("systemctl", "show", "hivezilla-validator", "-p", "MainPID", "--value") != pid:
        raise RuntimeError("validator_pid_changed")
    # Read the actual process arguments, not just unit definitions. A running
    # process can still use the old command after daemon-reload.
    for name, args, _ in replacements:
        process = run("systemctl", "show", name, "-p", "MainPID", "--value")
        actual = (Path("/proc") / process / "cmdline").read_bytes().decode().rstrip("\0").split("\0")
        if actual != args:
            raise RuntimeError("running_service_command_mismatch")
    # The readers prefer this atomic pin. Do not overwrite the dated recovery
    # record, and do not select a merely newer capture as the active source.
    json_write(root / ACTIVE_SOURCE, source)
    record["active_source_file"] = str(root / ACTIVE_SOURCE)
    record.update(state="RECOVERED_PENDING_PUBLIC_CHECK", validator_pid_unchanged=True,
                  free_disk_bytes=shutil.disk_usage(root).free, updated_unix_ms=int(time.time() * 1000))
    json_write(report / "RECOVERY.json", record)
    print(json.dumps({"state": record["state"], "validator_pid_unchanged": True,
                      "old_capture_preserved": True, "gap_rebuilt": False}))


def resume_switch(root, report):
    root, report = root.resolve(strict=True), report.resolve(strict=True)
    record = json.loads((report / "RECOVERY.json").read_text())
    upgrade = record.get("revision_upgrade")
    preparing = record["state"] in ("PREPARED", "CAPTURE_STARTED") and bool(record.get("session_id"))
    if not preparing and record["state"] not in ("SWITCH_PREPARED", "RECOVERED_PENDING_PUBLIC_CHECK"):
        raise RuntimeError("recovery_switch_is_not_prepared")
    if Path(record.get("root", "")) != root:
        raise RuntimeError("recovery_saved_root_mismatch")
    recovery_capacity(root)
    pid = run("systemctl", "show", "hivezilla-validator", "-p", "MainPID", "--value")
    if int(pid) != record["validator_pid"]:
        raise RuntimeError("validator_pid_changed_before_resume")
    if upgrade:
        expected = dict(record["old_source"], agave_revision=upgrade["new_revision"],
                        session_id=record["session_id"])
        source = record.get("source", expected)
        revision_upgrade_options(source["session_id"], upgrade["old_revision"],
                                 upgrade["new_revision"], upgrade["worker_binary"], upgrade["worker_sha256"],
                                 upgrade["builder_binary"], upgrade["builder_sha256"])
        if (record["old_source"]["agave_revision"] != upgrade["old_revision"]
                or source != expected or Path(record["root"]) != root):
            raise RuntimeError("upgrade_saved_source_mismatch")
        verify_upgrade_worker(root, upgrade)
        binary = (Path("/proc") / pid / "exe").resolve(strict=True)
        verify_upgrade_load(root, pid, source["session_id"], upgrade["new_revision"], binary)
        for path, digest in record["artifacts"].items():
            if hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest:
                raise RuntimeError("upgrade_runtime_artifact_changed")
        capture = existing_capture(root, source["session_id"], record["old_source"], pid,
                                   upgrade["new_revision"])
    else:
        old = record["old_source"]
        source = record.get("source", dict(old, session_id=record["session_id"]))
        if source != dict(old, session_id=record["session_id"]):
            raise RuntimeError("recovery_saved_source_mismatch")
        saved_config = report / "previous-plugin-config.json"
        if (root / "config/direct-agave.json").read_bytes() != saved_config.read_bytes():
            raise RuntimeError("recovery_plugin_configuration_changed")
        plugin = Path(json.loads(saved_config.read_text())["libpath"])
        binary = (Path("/proc") / pid / "exe").resolve(strict=True)
        if not {str(binary), str(plugin)}.issubset(record["artifacts"]):
            raise RuntimeError("recovery_runtime_artifact_path_mismatch")
        for path, digest in record["artifacts"].items():
            if hashlib.sha256(Path(path).read_bytes()).hexdigest() != digest:
                raise RuntimeError("recovery_runtime_artifact_changed")
        old_capture = root / "captures/direct-agave-live" / old["session_id"]
        for name, key in (("FAILURE.json", "old_failure_sha256"), ("SESSION.json", "old_session_sha256")):
            if hashlib.sha256((old_capture / name).read_bytes()).hexdigest() != record[key]:
                raise RuntimeError("recovery_old_capture_evidence_changed")
        if terminal_upgrade_capture(root, old) != "FAILED":
            raise RuntimeError("old_capture_is_not_failed")
        capture = existing_capture(root, source["session_id"], old, pid)
    recovery_capacity(root)
    commands = {name: exec_args((report / (name + ".previous-unit")).read_text()) for name in NAMES}
    if preparing:
        old = record["old_source"]
        if upgrade:
            if terminal_upgrade_capture(root, old) != record["old_capture_state"]:
                raise RuntimeError("old_upgrade_capture_state_changed")
            terminal_upgrade_feed(root, old, record["old_capture_state"])
        else:
            terminal_upgrade_feed(root, old, "FAILED")
        for name in NAMES:
            check_source_override_order(name, run("sudo", "systemctl", "cat", name + ".service"))
        run("sudo", "systemctl", "stop", "hivezilla-direct-worker", "hivezilla-bank-v2-producer",
            "hivezilla-bank-v2-r2-publisher")
        record["old_publisher_tail"] = tail_plan(
            root / "compact/direct-agave-processed-live" / old["session_id"],
            root / "reports/native-processed-r2-bank-v2" / old["session_id"], old)
        json_write(report / "RECOVERY.json", record)
        prepare_source_switch(root, report, record, commands, pid, capture)
    else:
        finish_switch(root, report, record, commands, pid)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path)
    p.add_argument("--report", type=Path)
    p.add_argument("--drain-command-file", type=Path)
    p.add_argument("--session-id", help="explicit healthy capture to adopt after a validator restart")
    p.add_argument("--expected-old-revision", help="full prior host revision for an explicit revision upgrade")
    p.add_argument("--expected-new-revision", help="full tested new host revision; requires an existing capture")
    p.add_argument("--worker-binary", type=Path, help="tested new-revision worker under ROOT/releases")
    p.add_argument("--worker-sha256", help="SHA-256 of the tested new worker; also used for the v2 producer")
    p.add_argument("--builder-binary", type=Path, help="tested new-revision compact builder under ROOT/releases")
    p.add_argument("--builder-sha256", help="SHA-256 of the tested new compact builder")
    p.add_argument("--resume-switch", action="store_true", help="complete a saved switch without reloading capture")
    a = p.parse_args()
    upgrade_options = (a.expected_old_revision, a.expected_new_revision, a.worker_binary, a.worker_sha256,
                       a.builder_binary, a.builder_sha256)
    if a.drain_command_file:
        if a.root or a.report or a.session_id or a.resume_switch or any(upgrade_options):
            p.error("drain mode cannot use recovery options")
        drain_closed_source(a.drain_command_file)
    elif a.root and a.report:
        if a.resume_switch:
            if a.session_id or any(upgrade_options):
                p.error("--resume-switch uses the source in the saved report")
            resume_switch(a.root, a.report)
        else:
            recover(a.root, a.report, a.session_id, a.expected_old_revision, a.expected_new_revision,
                    a.worker_binary, a.worker_sha256, a.builder_binary, a.builder_sha256)
    else:
        p.error("recovery requires --root and --report")
