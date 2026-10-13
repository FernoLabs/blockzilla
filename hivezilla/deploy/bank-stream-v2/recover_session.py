#!/usr/bin/env python3
"""Recover a failed capture with an explicit new source. Do not restart Agave."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import sqlite3
import subprocess
import time

NAMES = ["hivezilla-direct-worker", "hivezilla-bank-v2-producer",
         "hivezilla-bank-v2-origin", "hivezilla-stream-origin",
         "hivezilla-stream-relay", "hivezilla-bank-v2-r2-publisher"]


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


def systemd_command(args):
    # systemd quoting is not shell quoting. Reject control characters and escapes
    # so a saved command cannot add a new directive or expand a specifier.
    if any(any(c in a for c in "\n\r\0%$") for a in args):
        raise RuntimeError("unsafe_systemd_argument")
    return " ".join(json.dumps(a) for a in args)


def override_text(args, writable=None, extra=""):
    text = "[Service]\nExecStart=\nExecStart=" + systemd_command(args) + "\n"
    if writable is not None:
        text += "ReadWritePaths=\nReadWritePaths=" + " ".join(str(p) for p in writable) + "\n"
    return text + extra


def install(report, path, text):
    saved = report / "saved" / str(path).lstrip("/")
    if path.exists() and not saved.exists():
        saved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, saved)
    new = report / "new" / str(path).lstrip("/")
    write(new, text)
    run("sudo", "mkdir", "-p", str(path.parent))
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


def recover(root, report):
    root = root.resolve(strict=True)
    if shutil.disk_usage(root).free < 500 * 1024**3:
        raise RuntimeError("less_than_100_GiB_above_capture_reserve")
    report.mkdir(parents=True, exist_ok=False, mode=0o700)
    current_relay = exec_args(run("systemctl", "cat", "hivezilla-stream-relay.service"))
    if current_relay.count("--banks-source-file") != 1:
        raise RuntimeError("missing_current_source_pin")
    old = json.loads(Path(current_relay[current_relay.index("--banks-source-file") + 1]).read_text())
    old_capture = root / "captures/direct-agave-live" / old["session_id"]
    failed = json.loads((old_capture / "FAILURE.json").read_text())
    if failed.get("state") != "FAILED":
        raise RuntimeError("old_capture_is_not_failed")
    units = {name: run("systemctl", "cat", name + ".service") for name in NAMES}
    commands = {name: exec_args(text) for name, text in units.items()}
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
    host = Path("/proc") / pid / "exe"
    binary = host.resolve(strict=True)
    plugin = Path(cfg["libpath"])
    hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in [binary, plugin]}
    record = {"schema": "blockzilla-native-session-recovery-v1", "state": "PREPARED",
              "old_source": old, "validator_pid": int(pid), "artifacts": hashes,
              "old_capture_preserved": True, "gap_rebuilt": False,
              "started_unix_ms": int(time.time() * 1000)}
    json_write(report / "RECOVERY.json", record)
    before = {p.name for p in (root / "captures/direct-agave-live").iterdir() if p.is_dir()}
    run("sudo", "systemctl", "stop", "hivezilla-direct-worker", "hivezilla-bank-v2-producer")
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

    session = wait(capture_ready, 30, "new_capture_did_not_start")
    record.update(state="CAPTURE_STARTED", session_id=session.name)
    json_write(report / "RECOVERY.json", record)
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
    if {k: v for k, v in source.items() if k != "session_id"} != {k: v for k, v in old.items() if k != "session_id"}:
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
    for p in [banks, origin_spool, v1_spool, v2_spool, publisher_state]:
        p.mkdir(parents=True, exist_ok=False, mode=0o700)
    producer = commands["hivezilla-bank-v2-producer"].copy()
    if producer[1] != "build-confirmed" or len(producer) != 5 or producer[-1] != "--follow":
        raise RuntimeError("unexpected_producer_command")
    producer[2:4] = [str(processed), str(banks)]
    bank_origin = replace_option(commands["hivezilla-bank-v2-origin"], "--output", banks)
    bank_origin = replace_option(bank_origin, "--spool", origin_spool)
    v1_origin = replace_option(commands["hivezilla-stream-origin"], "--output", processed)
    relay = replace_option(commands["hivezilla-stream-relay"], "--spool", v1_spool)
    relay = replace_option(relay, "--banks-spool", v2_spool)
    relay = replace_option(relay, "--banks-source-file", expected)
    publisher = replace_option(commands["hivezilla-bank-v2-r2-publisher"], "--output", processed)
    publisher = replace_option(publisher, "--state", publisher_state)
    replacements = [
        ("hivezilla-bank-v2-producer", producer, [banks, processed / "feed.sqlite-shm"]),
        ("hivezilla-bank-v2-origin", bank_origin, [origin_spool, banks / "feed.sqlite-shm"]),
        ("hivezilla-stream-origin", v1_origin, [processed / "feed.sqlite-shm"]),
        ("hivezilla-stream-relay", relay, [v1_spool, v2_spool]),
        ("hivezilla-bank-v2-r2-publisher", publisher, [publisher_state, processed / "feed.sqlite-shm"]),
    ]
    for name, args, writable in replacements:
        install(report, Path("/etc/systemd/system") / (name + ".service.d/zzzzzzzzzz-recovery-20261011.conf"),
                override_text(args, writable))
    # Keep a separate publisher for the failed session. It has a distinct source
    # and durable checkpoint; it must not write the new session's cloud head.
    drain_name = "hivezilla-bank-v2-r2-drain-20261011"
    drain = commands["hivezilla-bank-v2-r2-publisher"]
    old_processed = root / "compact/direct-agave-processed-live" / old["session_id"]
    old_state = root / "reports/native-processed-r2-bank-v2" / old["session_id"]
    old_state.mkdir(parents=True, exist_ok=True, mode=0o700)
    drain = replace_option(replace_option(drain, "--output", old_processed), "--state", old_state)
    drain_unit = ("[Unit]\nAfter=network-online.target\n[Service]\nUser=ubuntu\nGroup=ubuntu\nUMask=0077\n"
                  "ExecStart=" + systemd_command(drain) + "\nRestart=on-failure\nRestartSec=30\n"
                  "MemoryMax=2G\nCPUQuota=200%\nNice=10\nIOWeight=10\nProtectSystem=strict\n"
                  "ProtectHome=read-only\nReadWritePaths=" + str(old_state) + " " + str(old_processed / "feed.sqlite-shm") +
                  "\n[Install]\nWantedBy=multi-user.target\n")
    install(report, Path("/etc/systemd/system") / (drain_name + ".service"), drain_unit)
    record.update(state="SWITCH_PREPARED", source=source, processed_output=str(processed),
                  bank_output=str(banks), origin_spool=str(origin_spool), relay_spool=str(v2_spool),
                  source_file=str(expected), old_source_drain_unit=drain_name)
    json_write(report / "RECOVERY.json", record)
    run("sudo", "systemctl", "stop", "hivezilla-stream-relay", "hivezilla-stream-origin",
        "hivezilla-bank-v2-origin", "hivezilla-bank-v2-r2-publisher")
    run("sudo", "systemctl", "daemon-reload")
    run("sudo", "systemctl", "reset-failed", *[n for n, _, _ in replacements])
    run("sudo", "systemctl", "start", "hivezilla-bank-v2-producer")
    wait(lambda: (banks / "FEED.json").exists() and (banks / "feed.sqlite-shm").exists(), 30,
         "new_bank_feed_not_initialized")
    run("sudo", "systemctl", "start", "hivezilla-stream-origin", "hivezilla-bank-v2-origin")
    time.sleep(2)
    run("sudo", "systemctl", "start", "hivezilla-stream-relay", "hivezilla-bank-v2-r2-publisher", drain_name)
    run("sudo", "systemctl", "enable", drain_name)
    if run("systemctl", "show", "hivezilla-validator", "-p", "MainPID", "--value") != pid:
        raise RuntimeError("validator_pid_changed")
    # Read the actual process arguments, not just unit definitions. A running
    # process can still use the old command after daemon-reload.
    for name, args, _ in replacements:
        process = run("systemctl", "show", name, "-p", "MainPID", "--value")
        actual = (Path("/proc") / process / "cmdline").read_bytes().decode().rstrip("\0").split("\0")
        if actual != args:
            raise RuntimeError("running_service_command_mismatch")
    record.update(state="RECOVERED_PENDING_PUBLIC_CHECK", validator_pid_unchanged=True,
                  free_disk_bytes=shutil.disk_usage(root).free, updated_unix_ms=int(time.time() * 1000))
    json_write(report / "RECOVERY.json", record)
    print(json.dumps({"state": record["state"], "validator_pid_unchanged": True,
                      "old_capture_preserved": True, "gap_rebuilt": False}))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--report", type=Path, required=True)
    a = p.parse_args()
    recover(a.root, a.report)
