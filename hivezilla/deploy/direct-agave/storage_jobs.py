#!/usr/bin/env python3
"""Inspect explicitly approved storage jobs; optionally retry transient failures."""
import argparse
from contextlib import ExitStack, closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import stat
import subprocess
import time

SOURCE_KEYS = {"genesis_hash", "node_identity", "agave_revision", "session_id"}
SCHEMA = "blockzilla-storage-jobs-registry-v1"
STATUS_SCHEMA = "blockzilla-storage-jobs-status-v1"
MAX_JOBS = 16
TRANSIENT_TYPES = {"EndpointConnectionError", "ConnectionClosedError", "ConnectTimeoutError", "ReadTimeoutError",
                   "BundleTransferTimeout"}
TRANSIENT_HTTP = {408, 429, 500, 502, 503, 504}
LIVE_UNIT = "hivezilla-native-cold-storage-20261012-live.service"
SHOW_PROPERTIES = "ActiveState,SubState,MainPID,Result,InvocationID,Restart,FreezerState"
VALUE_OPTIONS = {"--output", "--bank-output", "--capture", "--publisher-state", "--credentials-file",
                 "--v1-spool", "--bank-origin-spool", "--keep-seconds", "--batch-banks",
                 "--parallel-bundles", "--max-batches", "--max-archived-banks"}
FLAGS = {"--retire", "--follow"}


def read_json(path, maximum=256 * 1024):
    path = Path(path)
    if not stat.S_ISREG(path.lstat().st_mode):
        raise RuntimeError("invalid_metadata_file")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise RuntimeError("metadata_too_large")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise RuntimeError("invalid_metadata_object")
    return value


def atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".new")
    with temporary.open("w") as stream:
        json.dump(value, stream, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    temporary.chmod(0o600)
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def command_sha256(args):
    return hashlib.sha256(json.dumps(args, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while body := stream.read(1024**2):
            digest.update(body)
    return digest.hexdigest()


def source_tuple(value):
    return {key: value[key] for key in SOURCE_KEYS}


def active_source(root):
    value = read_json(root / "config/native-active-source.json")
    if (set(value) != SOURCE_KEYS or not all(isinstance(item, str) for item in value.values())
            or not re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{1,16}", value["session_id"])
            or not re.fullmatch(r"[0-9a-f]{40}", value["agave_revision"])):
        raise RuntimeError("invalid_active_storage_source")
    return value


def positive(value):
    return type(value) is int and value > 0


def command_options(args):
    result = {}
    index = 3
    while index < len(args):
        name = args[index]
        if name in result or name not in VALUE_OPTIONS | FLAGS:
            raise RuntimeError("invalid_storage_command")
        if name in FLAGS:
            result[name] = True
        else:
            index += 1
            if index == len(args) or args[index].startswith("--"):
                raise RuntimeError("invalid_storage_command")
            result[name] = args[index]
        index += 1
    return result


def validate_job(root, job):
    if not isinstance(job, dict):
        raise RuntimeError("invalid_storage_job")
    required = {"unit", "source", "output", "max_archived_banks", "namespace", "helper", "helper_sha256",
                "expected_command", "expected_command_sha256", "allow_restart", "max_retries", "retry_delay_seconds"}
    if set(job) != required:
        raise RuntimeError("invalid_storage_job_fields")
    if not re.fullmatch(r"hivezilla-native-cold-storage-[0-9]{8}-(?:source-[0-9a-f]{8}|old|live)\.service", job["unit"]):
        raise RuntimeError("invalid_storage_unit")
    source = job["source"]
    if (not isinstance(source, dict) or set(source) != SOURCE_KEYS
            or not all(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9-]{1,128}", value) for value in source.values())
            or not re.fullmatch(r"[0-9a-f]{32}-[0-9a-f]{1,16}", source["session_id"])
            or not re.fullmatch(r"[0-9a-f]{40}", source["agave_revision"])):
        raise RuntimeError("invalid_storage_source")
    continuous = job["max_archived_banks"] is None
    if (continuous and job["unit"] != LIVE_UNIT
            or job["unit"] == LIVE_UNIT and source != active_source(root)):
        raise RuntimeError("uncapped_storage_scope_rejected")
    if ((not continuous and (not positive(job["max_archived_banks"]) or job["max_archived_banks"] > 2**53 - 1))
            or type(job["allow_restart"]) is not bool
            or type(job["max_retries"]) is not int or not 0 <= job["max_retries"] <= 3
            or type(job["retry_delay_seconds"]) is not int or not 60 <= job["retry_delay_seconds"] <= 900):
        raise RuntimeError("invalid_storage_job_limits")
    for field in ("namespace", "helper_sha256", "expected_command_sha256"):
        if not isinstance(job[field], str) or not re.fullmatch(r"[0-9a-f]{64}", job[field]):
            raise RuntimeError("invalid_storage_job_digest")
    helper = Path(job["helper"])
    if not helper.is_absolute() or not helper.is_relative_to(root / "releases") or helper.name != "cold_storage.py":
        raise RuntimeError("invalid_storage_helper")
    args = job["expected_command"]
    if (not isinstance(args, list) or not 3 <= len(args) <= 64
            or not all(isinstance(arg, str) and arg and not any(c in arg for c in "\0\r\n%$") for arg in args)
            or args[:3] != ["/usr/bin/python3", str(helper), "archive"]
            or command_sha256(args) != job["expected_command_sha256"]):
        raise RuntimeError("invalid_storage_command")
    options = command_options(args)
    session = source["session_id"]
    paths = {"--output": root / "compact/direct-agave-processed-live" / session,
             "--bank-output": root / "compact/bank-v2-live" / session,
             "--capture": root / "captures/direct-agave-live" / session,
             "--publisher-state": root / "reports/native-processed-r2-bank-v2" / session,
             "--v1-spool": root / "stream-relay-spool-bank-v2" / session,
             "--bank-origin-spool": root / "bank-v2-origin-spool" / session}
    if (job["output"] != str(paths["--output"])
            or any(options.get(flag) != str(path) for flag, path in paths.items())
            or options.get("--credentials-file") != str(root / "config/native-cold-storage.env")
            or not all(options.get(flag) is True for flag in FLAGS)
            or ("--max-archived-banks" in options if continuous
                else options.get("--max-archived-banks") != str(job["max_archived_banks"]))):
        raise RuntimeError("storage_command_scope_mismatch")
    for flag, default, minimum, maximum in [("--keep-seconds", "7200", 7200, 2**31),
            ("--batch-banks", "64", 1, 128), ("--max-batches", "1", 1, 2**31)]:
        value = options.get(flag, default)
        if not value.isdigit() or not minimum <= int(value) <= maximum:
            raise RuntimeError("invalid_storage_command_limit")
    if options.get("--parallel-bundles", "2") not in {"1", "2", "4"}:
        raise RuntimeError("invalid_storage_command_limit")
    return job


def load_registry(path):
    path = Path(path)
    if not stat.S_ISREG(path.lstat().st_mode):
        raise RuntimeError("invalid_storage_registry_file")
    with path.open("rb") as stream:
        raw = stream.read(256 * 1024 + 1)
    if len(raw) > 256 * 1024:
        raise RuntimeError("storage_registry_too_large")
    registry = json.loads(raw)
    if not isinstance(registry, dict):
        raise RuntimeError("invalid_storage_registry")
    if Path(path).stat().st_mode & 0o022:
        raise RuntimeError("writable_storage_registry")
    if set(registry) != {"schema", "root", "jobs"} or registry["schema"] != SCHEMA:
        raise RuntimeError("invalid_storage_registry")
    root = Path(registry["root"])
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise RuntimeError("invalid_storage_root")
    if not isinstance(registry["jobs"], list) or not 1 <= len(registry["jobs"]) <= MAX_JOBS:
        raise RuntimeError("invalid_storage_jobs_count")
    jobs = [validate_job(root, job) for job in registry["jobs"]]
    if len({job["unit"] for job in jobs}) != len(jobs) or len({job["output"] for job in jobs}) != len(jobs):
        raise RuntimeError("duplicate_storage_job")
    return root, jobs, hashlib.sha256(raw).hexdigest()


def run(*args):
    return subprocess.check_output(args, text=True, timeout=15).strip()


def unit_command(text):
    lines = [line[len("ExecStart="):] for line in text.splitlines()
             if line.startswith("ExecStart=") and line != "ExecStart="]
    if not lines:
        raise RuntimeError("missing_storage_command")
    return shlex.split(lines[-1])


def inspect_job(root, job, now_ms=None, runner=run, proc_root=Path("/proc")):
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    result = {"unit": job["unit"], "source": job["source"], "state": "UNAVAILABLE", "issues": [],
              "max_archived_banks": job["max_archived_banks"], "retryable": False}
    issues = result["issues"]
    try:
        helper = Path(job["helper"])
        if (helper.is_symlink() or helper.resolve(strict=True) != helper or not helper.is_file()
                or file_sha256(helper) != job["helper_sha256"]):
            raise RuntimeError("storage_helper_changed")
        actual = unit_command(runner("sudo", "systemctl", "cat", job["unit"]))
        if actual != job["expected_command"] or command_sha256(actual) != job["expected_command_sha256"]:
            raise RuntimeError("storage_command_changed")
        show = dict(line.split("=", 1) for line in runner("systemctl", "show", job["unit"],
                    "-p", SHOW_PROPERTIES).splitlines() if "=" in line)
        if show.get("Restart") != "no":
            raise RuntimeError("storage_uncontrolled_restart")
        result.update(active_state=show["ActiveState"], invocation_id=show.get("InvocationID", ""),
                      frozen=show.get("FreezerState") == "frozen")
        pid = int(show["MainPID"])
        if show["ActiveState"] == "active":
            if pid <= 0 or (proc_root / str(pid) / "cmdline").read_bytes().decode().rstrip("\0").split("\0") != actual:
                raise RuntimeError("storage_running_command_changed")
        output = Path(job["output"])
        if output.is_symlink() or output.resolve(strict=True) != output:
            raise RuntimeError("storage_output_changed")
        source = job["source"]
        if job["max_archived_banks"] is None and source != active_source(root):
            raise RuntimeError("live_storage_source_changed")
        if source_tuple(read_json(output / "FEED.json")) != source:
            raise RuntimeError("storage_source_changed")
        options = command_options(actual)
        bank_header = read_json(Path(options["--bank-output"]) / "FEED.json")
        native_header = read_json(Path(options["--capture"]) / "SESSION.json")
        native_source = {"session_id": native_header["session_id"], "agave_revision": native_header["agave_revision"],
                         **{key: native_header["config"][key] for key in ("genesis_hash", "node_identity")}}
        if (source_tuple(bank_header) != source or bank_header["payload_root"] != str(output)
                or native_source != source):
            raise RuntimeError("storage_source_changed")
        path = output / "cold-storage/archive.sqlite"
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
            db.execute("PRAGMA query_only=ON")
            metadata = json.loads(db.execute("SELECT value FROM meta WHERE id=1").fetchone()[0])
            if metadata != {"source": source, "namespace": job["namespace"]}:
                raise RuntimeError("storage_archive_binding_changed")
            indexes = {row[1] for row in db.execute("PRAGMA index_list(archives)")}
            if "archives_retired" not in indexes:
                raise RuntimeError("storage_archive_count_index_missing")
            archived, retired = db.execute("SELECT count(*),coalesce(sum(retired),0) FROM archives").fetchone()
        maximum = job["max_archived_banks"]
        result.update(archived_banks=archived, retired_banks=retired,
                      remaining_banks=None if maximum is None else max(0, maximum - archived))
        if maximum is not None and archived > maximum:
            raise RuntimeError("storage_absolute_cap_exceeded")
        if maximum is not None and archived == maximum and retired == archived:
            result["state"] = "COMPLETE"
            return result
        status = read_json(output / "cold-storage/STATUS.json")
        updated = status["updated_unix_ms"]
        if type(updated) is not int or updated > now_ms + 5000:
            raise RuntimeError("invalid_storage_status_time")
        result.update(status_age_ms=max(0, now_ms - updated), storage_progress=status.get("storage_progress"),
                      last_archive_progress_unix_ms=status.get("last_archive_progress_unix_ms"))
        if show["ActiveState"] == "active":
            result["state"] = "PAUSED" if result["frozen"] else "RUNNING"
            if status.get("state") == "FAILED":
                issues.append("storage_job_failed")
            elif not result["frozen"] and result["status_age_ms"] > 90000:
                issues.append("storage_job_stale")
            if status.get("storage_progress") == "STALLED" and not result["frozen"]:
                issues.append("storage_job_stalled")
            return result
        result["state"] = "FAILED" if show["ActiveState"] == "failed" else "STOPPED"
        issues.append("storage_job_failed" if result["state"] == "FAILED" else "storage_job_stopped")
        failure = read_json(output / "cold-storage/LAST-FAILURE.json")
        invocation = show.get("InvocationID", "")
        if (failure.get("state") != "FAILED" or failure.get("error") != "archive_operation_failed"
                or failure.get("source") != source or not re.fullmatch(r"[0-9a-f]{32}", invocation)
                or failure.get("invocation_id") != invocation
                or type(failure.get("updated_unix_ms")) is not int or failure["updated_unix_ms"] > now_ms + 5000):
            raise RuntimeError("storage_failure_evidence_mismatch")
        kind, http_status = failure.get("error_type"), failure.get("http_status")
        protocol = kind == "ResponseStreamingError" and failure.get("network_error_type") == "ProtocolError"
        transient = kind in TRANSIENT_TYPES or protocol or (kind == "ClientError" and type(http_status) is int
                                                           and http_status in TRANSIENT_HTTP)
        result.update(error_type=kind, http_status=http_status, failure_unix_ms=failure["updated_unix_ms"],
                      network_error_type="ProtocolError" if protocol else None,
                      retryable=show["ActiveState"] == "failed" and transient)
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, RuntimeError, subprocess.SubprocessError):
        issues.append("storage_job_unavailable")
        result["retryable"] = False
    return result


def job_key(job):
    return hashlib.sha256(json.dumps(job, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def retry_job(root, job, snapshot, state, save_state, now_ms, runner=run, inspector=inspect_job):
    if not job["allow_restart"]:
        return
    entry = state.setdefault("jobs", {}).setdefault(job_key(job), {"attempts": 0})
    if type(entry.get("attempts")) is not int or not 0 <= entry["attempts"] <= 3:
        snapshot["issues"].append("storage_job_retry_state_invalid")
        snapshot["retryable"] = False
        return
    archived = snapshot.get("archived_banks")
    if type(archived) is int:
        if archived < entry.get("last_observed_archived_banks", archived):
            snapshot["issues"].append("storage_job_archive_counter_regressed")
            snapshot["retryable"] = False
            return
        if archived != entry.get("last_observed_archived_banks"):
            entry["last_observed_archived_banks"] = archived
            save_state(state)
    healthy = snapshot["state"] == "RUNNING" and not snapshot["issues"]
    if healthy:
        archived = snapshot["archived_banks"]
        if "healthy_since_unix_ms" not in entry:
            entry.update(healthy_since_unix_ms=now_ms, healthy_start_archived_banks=archived)
            save_state(state)
        elif now_ms - entry["healthy_since_unix_ms"] >= 60000 and archived > entry["healthy_start_archived_banks"]:
            if entry["attempts"]:
                entry.update(attempts=0, last_reset_unix_ms=now_ms)
                save_state(state)
        return
    if "healthy_since_unix_ms" in entry:
        entry.pop("healthy_since_unix_ms")
        entry.pop("healthy_start_archived_banks", None)
        save_state(state)
    if not snapshot["retryable"]:
        return
    invocation = snapshot["invocation_id"]
    if entry.get("failure_invocation") != invocation:
        entry.update(failure_invocation=invocation,
                     next_retry_unix_ms=now_ms + min(900, job["retry_delay_seconds"] * 2 ** entry["attempts"]) * 1000)
        save_state(state)
    snapshot["retry_attempts"] = entry["attempts"]
    snapshot["next_retry_unix_ms"] = entry["next_retry_unix_ms"]
    if entry["attempts"] >= job["max_retries"]:
        snapshot["issues"].append("storage_job_retry_exhausted")
        return
    if now_ms < entry["next_retry_unix_ms"] or entry.get("attempted_invocation") == invocation:
        return
    # Recheck command, artifact, source, cap and failure immediately before start.
    fresh = inspector(root, job, now_ms=now_ms, runner=runner)
    if not fresh["retryable"] or fresh.get("invocation_id") != invocation:
        snapshot["issues"].append("storage_job_changed_before_retry")
        return
    entry.update(attempts=entry["attempts"] + 1, attempted_invocation=invocation, last_retry_unix_ms=now_ms)
    save_state(state)  # Reserve the attempt durably before either service action.
    try:
        runner("sudo", "systemctl", "reset-failed", job["unit"])
        runner("sudo", "systemctl", "start", job["unit"])
        snapshot.update(state="RETRY_STARTED", retry_attempts=entry["attempts"])
    except (OSError, subprocess.SubprocessError):
        snapshot["issues"].append("storage_job_retry_start_failed")


def collect(root, jobs, state=None, save_state=None, supervise=False, now_ms=None, runner=run, registry_sha256=None):
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    current = active_source(root)
    rows = []
    for job in jobs:
        snapshot = inspect_job(root, job, now_ms=now_ms, runner=runner)
        if supervise:
            retry_job(root, job, snapshot, state, save_state, now_ms, runner=runner)
        rows.append(snapshot)
    issues = sorted({issue for row in rows for issue in row["issues"]})[:40]
    return {"schema": STATUS_SCHEMA, "updated_unix_ms": now_ms,
            "active_source": current, "registry_sha256": registry_sha256,
            "state": "DEGRADED" if issues else "HEALTHY", "issues": issues, "jobs": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path)
    parser.add_argument("--supervise", action="store_true")
    parser.add_argument("--follow", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=30)
    args = parser.parse_args()
    if args.supervise and args.report_dir is None:
        parser.error("supervision requires a persistent --report-dir")
    if not 5 <= args.interval_seconds <= 60:
        parser.error("interval must be between 5 and 60 seconds")
    with ExitStack() as resources:
        lock = None
        state = {"schema": "blockzilla-storage-jobs-retries-v1", "jobs": {}}
        if args.report_dir:
            args.report_dir.mkdir(parents=True, exist_ok=True)
        if args.supervise:
            lock = (args.report_dir / "LOCK").open("a")
            resources.callback(lock.close)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            state_path = args.report_dir / "RETRIES.json"
            if state_path.exists():
                state = read_json(state_path)
                if state.get("schema") != "blockzilla-storage-jobs-retries-v1" or not isinstance(state.get("jobs"), dict):
                    raise RuntimeError("invalid_storage_retry_state")
        def save_state(value):
            atomic(args.report_dir / "RETRIES.json", value)
        try:
            while True:
                try:
                    root, jobs, registry_digest = load_registry(args.registry)
                    status = collect(root, jobs, state, save_state, args.supervise, registry_sha256=registry_digest)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError):
                    status = {"schema": STATUS_SCHEMA, "updated_unix_ms": int(time.time() * 1000),
                              "state": "DEGRADED", "issues": ["storage_jobs_registry_invalid"], "jobs": []}
                if args.report_dir:
                    atomic(args.report_dir / "STATUS.json", status)
                if not args.follow:
                    print(json.dumps(status))
                    return
                time.sleep(args.interval_seconds)
        finally:
            if lock is not None:
                lock.close()


if __name__ == "__main__":
    main()
