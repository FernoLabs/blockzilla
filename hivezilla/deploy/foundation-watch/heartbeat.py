#!/usr/bin/env python3
"""Read Foundation telemetry and send an independent machine heartbeat.

Only the sender report is written. Capture, compact files, SQLite rows and
systemd units are never changed. A failed telemetry read still produces a
machine-alive heartbeat with fixed error codes.
"""
import argparse
from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

# Resolve the pinned sibling module even when an operator imports this sender
# by file path, without adding unrelated directories to the Python search path.
_archive_spec = importlib.util.spec_from_file_location("foundation_archive_status", Path(__file__).with_name("archive_status.py"))
archive_status = importlib.util.module_from_spec(_archive_spec)
_archive_spec.loader.exec_module(archive_status)

SCHEMA = "blockzilla-foundation-heartbeat-v1"
ACCEPTED_SCHEMA = "blockzilla-foundation-heartbeat-accepted-v1"
ROOT = Path("/home/ubuntu/live/hivezilla-validator-20260930")
RESERVE_BYTES = 400 * 1024**3
MAX_JSON_BYTES = 256 * 1024
MAX_REPLY_BYTES = 4096
MAX_SAFE_INTEGER = 2**53 - 1
SESSION_RE = re.compile(r"[0-9a-f]{32}-[0-9a-f]{1,16}\Z")
BOOT_RE = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")
STATE_RE = re.compile(r"[A-Z_]{1,40}\Z")
SERVICES = {
    "hivezilla-validator.service": "validator",
    "hivezilla-direct-worker.service": "worker",
    "hivezilla-bank-v2-producer.service": "producer",
    "hivezilla-bank-v2-r2-publisher.service": "publisher",
    "hivezilla-native-cold-storage-20261012-live.service": "cold_storage",
    "hivezilla-live-priority-20261012.service": "priority",
}
SOURCE_BOUND_SERVICES = {"worker", "producer", "publisher", "cold_storage"}
ACTIVE_STATES = {"active", "inactive", "failed", "activating", "deactivating", "reloading"}


class SafeError(Exception):
    """An error with a fixed code, without a URL, token or response body."""


def integer(value, *, nullable=False):
    if nullable and value is None:
        return None
    if type(value) is not int or not 0 <= value <= MAX_SAFE_INTEGER:
        raise SafeError("invalid_integer")
    return value


def state(value):
    if not isinstance(value, str) or not STATE_RE.fullmatch(value):
        raise SafeError("invalid_state")
    return value


def read_json(path, limit=MAX_JSON_BYTES):
    with path.open("rb") as stream:
        body = stream.read(limit + 1)
    if len(body) > limit:
        raise SafeError("file_too_large")
    value = json.loads(body)
    if not isinstance(value, dict):
        raise SafeError("invalid_object")
    return value


def read_capture(path):
    # mode=ro prevents row changes and prevents accidental database creation.
    # query_only is a second guard. With SQLite >=3.22, existing readable WAL
    # and SHM files permit access under the unit's read-only filesystem mount.
    # Missing sidecar files must fail collection, never create them in source.
    # Do not use immutable=1: capture is live and committed WAL rows matter.
    uri = path.resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=0.5)) as db:
        db.execute("PRAGMA query_only=ON")
        deadline = time.monotonic() + 0.5
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        columns = {row[1] for row in db.execute("PRAGMA table_info(status)")}
        timestamp = "updated_unix_ms" if "updated_unix_ms" in columns else "NULL"
        row = db.execute("SELECT state,durable_sequence,latest_rooted_slot," + timestamp
                         + " FROM status WHERE id=1").fetchone()
        if row is None:
            raise SafeError("missing_capture_status")
        return {"state": state(row[0]), "durable_sequence": integer(row[1]),
                "latest_rooted_slot": integer(row[2]),
                "updated_unix_ms": integer(row[3], nullable=True)}


def read_service(unit):
    result = subprocess.run(
        ["systemctl", "show", unit, "--property=ActiveState", "--property=ExecStart", "--no-pager"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=1.5, check=True)
    if len(result.stdout) > 16384:
        raise SafeError("service_reply_too_large")
    fields = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    active = fields.get("ActiveState")
    if active not in ACTIVE_STATES:
        raise SafeError("invalid_service_state")
    return active, fields.get("ExecStart", "")


def recognized_worker_launcher(command, root):
    """Accept only the deployed processed launcher with the expected roots.

    Its unit has no literal session: the launcher selects the latest capture.
    This narrow exception relies on the independent source inventory, FEED
    identity and compact freshness checks. Other generic commands must fail.
    """
    if "argv[]=" in command:
        if command.count("argv[]=") != 1:
            return False
        match = re.search(r"(?:^|;\s+)argv\[\]=(.*?)(?:\s+;|$)", command)
        if not match:
            return False
        command = match[1]
    try:
        argv = shlex.split(command)
        expected_script = root / "deploy/processed-stream-20261010/worker_launcher.py"
        if len(argv) != 11 or argv[:2] != ["/usr/bin/python3", str(expected_script)]:
            return False
        options = {}
        index = 2
        processed = False
        while index < len(argv):
            flag = argv[index]
            if flag == "--processed" and not processed:
                processed = True
                index += 1
                continue
            if flag not in {"--capture-root", "--output-root", "--engine", "--blockzilla"} or flag in options:
                return False
            options[flag] = argv[index + 1]
            index += 2
        if (not processed or options.get("--capture-root") != str(root / "captures/direct-agave-live")
                or options.get("--output-root") != str(root / "compact/direct-agave-processed-live")):
            return False
        for flag, binary in (("--engine", "hivezilla-direct-agave"), ("--blockzilla", "blockzilla")):
            parts = Path(options[flag]).relative_to(root / "releases").parts
            if len(parts) != 2 or parts[1] != binary or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,95}", parts[0]):
                return False
        return True
    except (ValueError, IndexError, KeyError):
        return False


def inspect_sources(capture_root, selected, proc_root):
    """Find a changed source without taking a writer lock or changing a file.

    The fixed-width session prefix is the capture creation time in nanoseconds.
    Kernel FLOCK records identify a live writer by the lock file's device and
    inode. An old CAPTURING row alone is insufficient after a process crash.
    """
    with (proc_root / "locks").open("rb") as stream:
        body = stream.read(1024 * 1024 + 1)
    if len(body) > 1024 * 1024:
        raise SafeError("lock_inventory_too_large")
    locked = set()
    for line in body.decode("ascii").splitlines():
        fields = line.split()
        if len(fields) >= 8 and fields[1:4] == ["FLOCK", "ADVISORY", "WRITE"]:
            device = fields[5].split(":")
            if len(device) == 3:
                locked.add((int(device[0], 16), int(device[1], 16), int(device[2])))
    deadline = time.monotonic() + 2
    newest = selected
    active = set()
    with os.scandir(capture_root) as entries:
        for count, entry in enumerate(entries, 1):
            if count > 1024 or time.monotonic() > deadline:
                raise SafeError("capture_inventory_limit")
            if not SESSION_RE.fullmatch(entry.name) or not entry.is_dir(follow_symlinks=False):
                continue
            path = Path(entry.path)
            if read_json(path / "SESSION.json").get("session_id") != entry.name:
                raise SafeError("capture_inventory_identity_invalid")
            if int(entry.name[:32], 16) > int(newest[:32], 16):
                newest = entry.name
            lock_path = path / "LIVE.lock"
            if lock_path.exists():
                stamp = lock_path.stat()
                key = (os.major(stamp.st_dev), os.minor(stamp.st_dev), stamp.st_ino)
                if key in locked:
                    active.add(entry.name)
    errors = set()
    if newest != selected:
        errors.add("source_newer_capture")
    if active - {selected}:
        errors.add("source_active_capture_mismatch")
    if selected not in active and active:
        # An active writer has priority over the old recovery record. A newer
        # failed session still produces an explicit mismatch warning above.
        binding_session = max(active, key=lambda value: int(value[:32], 16))
    else:
        binding_session = newest
    return binding_session, errors


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        fp.close()
        raise SafeError("http_redirect_rejected")


def direct_opener():
    # Do not inherit a proxy or forward Authorization to a redirect target.
    return urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())


def read_rpc():
    request = urllib.request.Request(
        "http://127.0.0.1:8899", method="POST", headers={"Content-Type": "application/json"},
        data=json.dumps([
            {"jsonrpc": "2.0", "id": 1, "method": "getHealth"},
            {"jsonrpc": "2.0", "id": 2, "method": "getSlot", "params": [{"commitment": "finalized"}]},
        ], separators=(",", ":")).encode())
    try:
        with direct_opener().open(request, timeout=2) as reply:
            if reply.status != 200:
                raise SafeError("rpc_http_error")
            body = reply.read(MAX_REPLY_BYTES + 1)
    except urllib.error.HTTPError as error:
        error.close()
        raise SafeError("rpc_http_error") from None
    if len(body) > MAX_REPLY_BYTES:
        raise SafeError("rpc_reply_too_large")
    values = json.loads(body)
    if not isinstance(values, list) or len(values) != 2:
        raise SafeError("invalid_rpc_reply")
    by_id = {item.get("id"): item for item in values if isinstance(item, dict)}
    if len(by_id) != 2 or set(by_id) != {1, 2}:
        raise SafeError("invalid_rpc_reply")
    healthy = by_id[1].get("result") == "ok"
    finalized = integer(by_id[2].get("result"), nullable=True)
    if finalized is None:
        raise SafeError("invalid_rpc_slot")
    return {"healthy": healthy, "finalized_slot": finalized}


def stale(timestamp, now, limit_ms):
    return timestamp is None or timestamp > now + 30000 or now - timestamp > limit_ms


def sample(root=ROOT, *, now_ms=None, proc_root=Path("/proc"), service_reader=read_service,
           rpc_reader=read_rpc, disk_reader=shutil.disk_usage):
    now = integer(now_ms if now_ms is not None else int(time.time() * 1000))
    result = {
        "schema": SCHEMA, "machine_id": "foundation", "sampled_unix_ms": now,
        "monotonic_ms": integer(time.monotonic_ns() // 1000000),
        "boot_id": None, "free_disk_bytes": None, "capture_reserve_bytes": RESERVE_BYTES,
        "mem_available_bytes": None,
        "capture": {"session": None, "state": None, "durable_sequence": None,
                    "latest_rooted_slot": None, "updated_unix_ms": None},
        "compact": {"state": None, "last_compact_slot": None, "event_sequence": None,
                    "source_sequence": None, "updated_unix_ms": None},
        "publisher": {"state": None, "sequence": None, "updated_unix_ms": None},
        "cold_storage": {"state": None, "retired_banks": None, "updated_unix_ms": None},
        "services": {unit: "unknown" for unit in SERVICES},
        "rpc": {"healthy": None, "finalized_slot": None}, "errors": [],
    }
    errors = set()
    try:
        with (proc_root / "sys/kernel/random/boot_id").open("rb") as stream:
            value = stream.read(129).decode("ascii").strip()
        if len(value) > 128:
            raise SafeError("boot_id_too_large")
        if not BOOT_RE.fullmatch(value):
            raise SafeError("invalid_boot_id")
        result["boot_id"] = value
    except Exception:
        errors.add("boot_id_unavailable")
    try:
        result["free_disk_bytes"] = integer(disk_reader(root).free)
    except Exception:
        errors.add("disk_unavailable")
    try:
        # /proc/meminfo is a bounded kernel file; never send its full contents.
        with (proc_root / "meminfo").open("rb") as stream:
            text = stream.read(16385)
        if len(text) > 16384:
            raise SafeError("meminfo_too_large")
        match = re.search(rb"^MemAvailable:\s+([0-9]+) kB$", text, re.MULTILINE)
        if not match:
            raise SafeError("mem_available_missing")
        result["mem_available_bytes"] = integer(int(match[1]) * 1024)
    except Exception:
        errors.add("memory_unavailable")

    session = None
    try:
        recovery = read_json(root / "reports/bank-v2-recovery-20261012/RECOVERY.json")
        value = recovery["source"]["session_id"]
        if not isinstance(value, str) or not SESSION_RE.fullmatch(value):
            raise SafeError("source_invalid")
        session = value
        result["capture"]["session"] = session
    except Exception:
        errors.add("source_invalid")

    binding_session = session
    if session:
        try:
            binding_session, source_errors = inspect_sources(root / "captures/direct-agave-live", session, proc_root)
            errors.update(source_errors)
        except Exception:
            errors.add("capture_inventory_unavailable")
        capture_path = root / "captures/direct-agave-live" / session
        output = root / "compact/direct-agave-processed-live" / session
        publisher_path = root / "reports/native-processed-r2-bank-v2" / session
        try:
            meta = read_json(capture_path / "SESSION.json")
            if meta.get("session_id") != session:
                errors.add("capture_source_mismatch")
            result["capture"].update(read_capture(capture_path / "events.sqlite"))
            stamp = result["capture"]["updated_unix_ms"]
            if stamp is None:
                paths = [capture_path / "events.sqlite", capture_path / "events.sqlite-wal"]
                stamp = max(int(path.stat().st_mtime_ns // 1000000) for path in paths if path.exists())
            if stale(stamp, now, 120000):
                errors.add("capture_stale")
        except Exception:
            errors.add("capture_unavailable")
        try:
            if read_json(output / "FEED.json").get("session_id") != session:
                errors.add("compact_source_mismatch")
            data = read_json(output / "STATUS.json")
            result["compact"] = {"state": state(data["state"]),
                                 **{key: integer(data[key], nullable=key == "last_compact_slot")
                                    for key in ("last_compact_slot", "event_sequence", "source_sequence", "updated_unix_ms")}}
            if stale(result["compact"]["updated_unix_ms"], now, 120000):
                errors.add("compact_stale")
        except Exception:
            errors.add("compact_unavailable")
        try:
            data = read_json(publisher_path / "STATUS.json")
            checkpoint = read_json(publisher_path / "CHECKPOINT.json")
            expected_prefix = "native-stream/processed/direct-agave-live-v1/" + session
            if (checkpoint.get("session") != session or checkpoint.get("source") != str(output)
                    or checkpoint.get("prefix") != expected_prefix):
                errors.add("publisher_source_mismatch")
            result["publisher"] = {"state": state(data["state"]), "sequence": integer(checkpoint["sequence"]),
                                   "updated_unix_ms": integer(data["updated_unix_ms"])}
            if stale(result["publisher"]["updated_unix_ms"], now, 120000):
                errors.add("publisher_stale")
        except Exception:
            errors.add("publisher_unavailable")
        try:
            data = read_json(output / "cold-storage/STATUS.json")
            result["cold_storage"] = {"state": state(data["state"]),
                                      "retired_banks": integer(data["retired_banks"]),
                                      "updated_unix_ms": integer(data["updated_unix_ms"])}
            if stale(result["cold_storage"]["updated_unix_ms"], now, 300000):
                errors.add("cold_storage_stale")
        except Exception:
            errors.add("cold_storage_unavailable")

    for unit, label in SERVICES.items():
        try:
            active, command = service_reader(unit)
            if active not in ACTIVE_STATES:
                raise SafeError("invalid_service_state")
            result["services"][unit] = active
            if active != "active":
                errors.add("service_" + label + "_inactive")
            if binding_session and label in SOURCE_BOUND_SERVICES:
                bound_sessions = set(re.findall(r"[0-9a-f]{32}-[0-9a-f]+", command))
                launcher_matches = label == "worker" and recognized_worker_launcher(command, root)
                if (bound_sessions and bound_sessions != {binding_session}
                        or not bound_sessions and not launcher_matches
                        or launcher_matches and binding_session != session):
                    errors.add("service_" + label + "_source_mismatch")
        except Exception:
            errors.add("service_" + label + "_unavailable")
    try:
        result["rpc"] = rpc_reader()
        if type(result["rpc"]["healthy"]) is not bool:
            raise SafeError("invalid_rpc_health")
        integer(result["rpc"]["finalized_slot"])
        if not result["rpc"]["healthy"]:
            errors.add("rpc_unhealthy")
    except Exception:
        result["rpc"] = {"healthy": None, "finalized_slot": None}
        errors.add("rpc_unavailable")
    result["errors"] = sorted(errors)
    # Archive status is optional. Its collection failure must not suppress a
    # machine heartbeat or change the independent live-pipeline checks.
    try:
        archive = archive_status.collect(root, now, service_reader)
        if archive is not None:
            result["archive"] = archive
    except Exception:
        pass
    return result


def endpoint(value):
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment
                or parsed.path not in ("", "/", "/heartbeat") or port not in (None, 443)
                or any(c.isspace() or ord(c) < 32 for c in value)):
            raise ValueError()
    except (TypeError, ValueError):
        raise SafeError("watch_url_invalid") from None
    return urllib.parse.urlunsplit(("https", parsed.netloc, "/heartbeat", "", ""))


def send(payload, url, token, *, opener=None):
    url = endpoint(url)
    if (not isinstance(token, str) or not 32 <= len(token) <= 512
            or not re.fullmatch(r"[A-Za-z0-9_.~-]+", token)):
        raise SafeError("watch_token_invalid")
    if payload.get("schema") != SCHEMA or payload.get("machine_id") != "foundation":
        raise SafeError("heartbeat_schema_invalid")
    stamp = integer(payload.get("sampled_unix_ms"))
    integer(payload.get("monotonic_ms"))
    body = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
    if len(body) > 65536:
        raise SafeError("heartbeat_too_large")
    request = urllib.request.Request(url, method="POST", data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json",
                 "Authorization": "Bearer " + token, "User-Agent": "Blockzilla-Foundation-Watch/1.0"})
    try:
        with (opener or direct_opener()).open(request, timeout=5) as reply:
            if reply.status not in (200, 202):
                raise SafeError("watch_http_error")
            answer = reply.read(MAX_REPLY_BYTES + 1)
            if len(answer) > MAX_REPLY_BYTES:
                raise SafeError("watch_reply_too_large")
            accepted = json.loads(answer)
            if (not isinstance(accepted, dict) or accepted.get("accepted") is not True
                    or accepted.get("schema") != ACCEPTED_SCHEMA
                    or type(accepted.get("sampled_unix_ms")) is not int
                    or accepted["sampled_unix_ms"] != stamp):
                raise SafeError("watch_reply_invalid")
    except SafeError:
        raise
    except urllib.error.HTTPError as error:
        error.close()
        raise SafeError("watch_send_failed") from None
    except (urllib.error.URLError, OSError, ValueError):
        raise SafeError("watch_send_failed") from None


def save_report(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", prefix=".sender-", dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            os.chmod(temporary, 0o600)
            json.dump(value, stream, separators=(",", ":"), allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def run_once(payload, report, url, token, *, sender=send):
    delivery = {"schema": "blockzilla-foundation-heartbeat-sender-v1", "state": "SENT",
                "sampled_unix_ms": payload["sampled_unix_ms"], "errors": payload["errors"],
                "send_error": None}
    try:
        sender(payload, url, token)
    except SafeError as error:
        delivery["state"] = "FAILED"
        delivery["send_error"] = error.args[0]
    except Exception:
        delivery["state"] = "FAILED"
        delivery["send_error"] = "watch_send_failed"
    save_report(report, delivery)
    return 0 if delivery["state"] == "SENT" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--report", type=Path, default=ROOT / "reports/foundation-watch/STATUS.json")
    parser.add_argument("--sample-only", action="store_true", help="Print telemetry; do not send or write a report.")
    args = parser.parse_args()
    payload = sample(args.root)
    if args.sample_only:
        print(json.dumps(payload, separators=(",", ":"), allow_nan=False))
        return 0
    try:
        return run_once(payload, args.report, os.environ.get("FOUNDATION_WATCH_URL"),
                        os.environ.get("FOUNDATION_WATCH_TOKEN"))
    except Exception:
        # Never include exception text: it can contain credentials or a remote body.
        print("heartbeat_report_failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
