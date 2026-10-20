#!/usr/bin/env python3
"""Archive exact rooted compact files and custody to R2 before local retirement."""
import argparse
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import selectors
import shlex
import shutil
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import threading
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack, closing

MAX_FILE = 256 * 1024**2
MAX_BUNDLE = 512 * 1024**2
RESTORE_RESERVE = 400 * 1024**3
RESTORE_MARGIN = 1024**3
STORAGE_STALL_SECONDS = 900
SCAN_PAGE_PAUSE_SECONDS = 0.1
BUNDLE_TRANSFER_SECONDS = 300
TRANSFER_REPORT_LIMIT = 4096
TRANSFER_SCHEMA = "blockzilla-bundle-transfer-v1"
RECOVERY_SCHEMA = "blockzilla-native-cold-recovery-v1"
RECOVERY_MARKER_SCHEMA = "blockzilla-native-cold-recovery-initialized-v1"
RECOVERY_DELAYS = (30, 60, 120)
RECOVERY_PROBE_SECONDS = 900
SOURCE_KEYS = ("genesis_hash", "node_identity", "agave_revision", "session_id")


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(1024**2):
            h.update(b)
    return h.hexdigest()


def atomic(path, value):
    temp = path.with_name(path.name + ".new")
    with open(temp, "w") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())
    temp.chmod(0o600)
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def checked(root, path):
    path = Path(path)
    rel = path.relative_to(root)
    if not rel.parts or any(p in ("..", ".") for p in rel.parts):
        raise RuntimeError("unsafe_archive_path")
    current = root
    for part in rel.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError("symlink_archive_path")
    return path


def identity(path):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_size > MAX_FILE:
        raise RuntimeError("invalid_archive_file")
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns]


def read_json(path):
    if path.is_symlink() or path.stat().st_size > 1024**2:
        raise RuntimeError("metadata_too_large")
    return json.loads(path.read_text())


def replay_base(path, source, bank=False):
    if path.is_symlink() or path.stat().st_size > 64 * 1024**2:
        raise RuntimeError("invalid_replay_checkpoint")
    cp = json.loads(path.read_text())
    if bank and cp["source"] != source:
        raise RuntimeError("replay_checkpoint_source_mismatch")
    session, position, _ = cp["base"].split(":")
    if session != source["session_id"] or not position.startswith("p"):
        raise RuntimeError("replay_checkpoint_source_mismatch")
    return int(position[1:])


def streaming_protocol_error(error):
    """Recognize only botocore's typed wrapper for a broken response stream."""
    try:
        from botocore.exceptions import ResponseStreamingError
        from urllib3.exceptions import ProtocolError
    except ImportError:
        return False
    details = getattr(error, "kwargs", None)
    return (isinstance(error, ResponseStreamingError) and isinstance(details, dict)
            and isinstance(details.get("error"), ProtocolError))


class BundleTransferTimeout(RuntimeError):
    pass


class BundleTransferError(RuntimeError):
    def __init__(self, evidence=None):
        super().__init__("bundle_transfer_failed")
        self.evidence = evidence


def failure_fields(error):
    if isinstance(error, BundleTransferError) and error.evidence is not None:
        return error.evidence.copy()
    response = getattr(error, "response", None)
    metadata = response.get("ResponseMetadata") if isinstance(response, dict) else None
    status = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
    kind = type(error).__name__
    return dict(error_type=kind if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", kind) else "BundleTransferError",
                http_status=status if type(status) is int and 100 <= status <= 599 else None,
                network_error_type="ProtocolError" if streaming_protocol_error(error) else None)


def transient_evidence(fields):
    return (fields["error_type"] in {"EndpointConnectionError", "ConnectionClosedError", "ConnectTimeoutError",
                                    "ReadTimeoutError", "BundleTransferTimeout"}
            and fields["http_status"] is None and fields["network_error_type"] is None
            or fields["error_type"] == "ResponseStreamingError" and fields["network_error_type"] == "ProtocolError"
            and fields["http_status"] is None
            or fields["error_type"] == "ClientError" and fields["network_error_type"] is None
            and fields["http_status"] in {408, 429, 500, 502, 503, 504})


def transient_transfer(error):
    return transient_evidence(failure_fields(error))


class RecoveryStateError(RuntimeError):
    pass


class RecoveryBlocked(RuntimeError):
    pass


class RecoveryCircuit:
    """One source-wide budget, owned by the archive thread and durable in meta3.

    Meta4 is an initialization anchor. Losing either row cannot create a new
    budget. A reservation stays spent when a process dies before child launch.
    """
    def __init__(self, db, source, namespace, maximum, now_ms=None):
        self.db = db
        self.persisted = True
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        self.binding = dict(source=source, namespace=namespace, max_archived_banks=maximum)
        rows = dict(db.execute("SELECT id,value FROM meta WHERE id IN (3,4)"))
        marker = dict(schema=RECOVERY_MARKER_SCHEMA, binding=self.binding)
        if not rows:
            self.value = dict(schema=RECOVERY_SCHEMA, binding=self.binding, state="NORMAL", burst_retries_used=0,
                outage_since_unix_ms=None, next_attempt_unix_ms=None, reservation=None, last_error=None,
                last_failure_unix_ms=None, healthy_since_unix_ms=None, healthy_start_archived_banks=None)
            db.execute("INSERT INTO meta(id,value) VALUES(4,?)", (json.dumps(marker, sort_keys=True),))
            self.save()
        else:
            try:
                if set(rows) != {3, 4} or len(rows[4]) > 16384 or strict_json(rows[4]) != marker or len(rows[3]) > 16384:
                    raise ValueError()
                self.value = strict_json(rows[3])
                self.validate()
            except (ValueError, TypeError, KeyError):
                raise RecoveryStateError("invalid_recovery_state") from None
        if self.value["state"] == "BLOCKED":
            raise RecoveryBlocked("storage_recovery_blocked")
        if self.value["reservation"] is not None:
            self.value.update(reservation=None, healthy_since_unix_ms=None, healthy_start_archived_banks=None,
                              state="OPEN" if self.value["burst_retries_used"] == 3 else "BACKOFF")
            self.save()
        elif self.value["state"] == "RECOVERING":
            # A stopped process is not sixty seconds of observed healthy work.
            self.value.update(healthy_since_unix_ms=now_ms, healthy_start_archived_banks=self.archived())
            self.save()

    def validate(self):
        v = self.value
        fields = {"schema", "binding", "state", "burst_retries_used", "outage_since_unix_ms",
                  "next_attempt_unix_ms", "reservation", "last_error", "healthy_since_unix_ms",
                  "healthy_start_archived_banks", "last_failure_unix_ms"}
        if (not isinstance(v, dict) or set(v) != fields or v["schema"] != RECOVERY_SCHEMA
                or v["binding"] != self.binding or v["state"] not in {"NORMAL", "BACKOFF", "OPEN", "RECOVERING", "BLOCKED"}
                or type(v["burst_retries_used"]) is not int or not 0 <= v["burst_retries_used"] <= 3):
            raise ValueError()
        for key in ("outage_since_unix_ms", "next_attempt_unix_ms", "healthy_since_unix_ms", "healthy_start_archived_banks", "last_failure_unix_ms"):
            if v[key] is not None and (type(v[key]) is not int or not 0 <= v[key] <= 2**63 - 1):
                raise ValueError()
        error = v["last_error"]
        if error is not None and (not isinstance(error, dict) or set(error) != {"error_type", "http_status", "network_error_type"}
                or not isinstance(error["error_type"], str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", error["error_type"])
                or error["http_status"] is not None and (type(error["http_status"]) is not int or not 100 <= error["http_status"] <= 599)
                or error["network_error_type"] not in (None, "ProtocolError")
                or error["network_error_type"] is not None and error["error_type"] != "ResponseStreamingError"):
            raise ValueError()
        reservation = v["reservation"]
        if reservation is not None and (not isinstance(reservation, dict) or set(reservation) != {"kind", "reserved_unix_ms"}
                or reservation["kind"] not in {"retry", "probe"} or type(reservation["reserved_unix_ms"]) is not int
                or not 0 <= reservation["reserved_unix_ms"] <= 2**63 - 1):
            raise ValueError()
        if v["state"] == "NORMAL":
            if v["burst_retries_used"] or any(v[k] is not None for k in fields - {"schema", "binding", "state", "burst_retries_used"}):
                raise ValueError()
        elif v["state"] == "BLOCKED":
            if error is None or reservation is not None or v["healthy_since_unix_ms"] is not None or v["healthy_start_archived_banks"] is not None:
                raise ValueError()
        else:
            if (error is None or not transient_evidence(error) or v["outage_since_unix_ms"] is None
                    or v["last_failure_unix_ms"] is None or v["next_attempt_unix_ms"] is None
                    or not v["outage_since_unix_ms"] <= v["last_failure_unix_ms"] <= v["next_attempt_unix_ms"]):
                raise ValueError()
            delay = RECOVERY_DELAYS[v["burst_retries_used"]] if v["burst_retries_used"] < 3 else RECOVERY_PROBE_SECONDS
            if v["next_attempt_unix_ms"] < v["last_failure_unix_ms"] + delay * 1000:
                raise ValueError()
            if v["state"] == "OPEN" and v["burst_retries_used"] != 3:
                raise ValueError()
            if v["state"] == "RECOVERING" and (v["healthy_since_unix_ms"] is None
                    or v["healthy_start_archived_banks"] is None or reservation is not None
                    or v["burst_retries_used"] == 0 or v["healthy_since_unix_ms"] < v["last_failure_unix_ms"]):
                raise ValueError()
            if v["state"] in {"BACKOFF", "OPEN"} and (v["healthy_since_unix_ms"] is not None
                    or v["healthy_start_archived_banks"] is not None):
                raise ValueError()
            if v["state"] == "BACKOFF" and v["burst_retries_used"] == 3 and reservation is None:
                raise ValueError()
            if reservation is not None:
                used = v["burst_retries_used"]
                if (reservation["reserved_unix_ms"] < v["outage_since_unix_ms"]
                        or reservation["kind"] == "probe" and (used != 3 or v["state"] != "OPEN")
                        or reservation["kind"] == "retry" and (not 1 <= used <= 3 or v["state"] != "BACKOFF")):
                    raise ValueError()
                delay = RECOVERY_DELAYS[used] if used < 3 else RECOVERY_PROBE_SECONDS
                if v["next_attempt_unix_ms"] < reservation["reserved_unix_ms"] + delay * 1000:
                    raise ValueError()

    def save(self):
        self.persisted = False
        self.validate()
        body = json.dumps(self.value, separators=(",", ":"), sort_keys=True)
        if len(body) > 16384:
            raise RecoveryStateError("recovery_state_too_large")
        self.db.execute("INSERT OR REPLACE INTO meta(id,value) VALUES(3,?)", (body,))
        self.db.commit()
        self.persisted = True

    def archived(self):
        return self.db.execute("SELECT count(*) FROM archives").fetchone()[0]

    def fresh_allowed(self):
        return self.value["state"] in {"NORMAL", "RECOVERING"}

    def due(self, now_ms):
        return (self.value["state"] in {"BACKOFF", "OPEN", "RECOVERING"}
                and self.value["reservation"] is None and now_ms >= self.value["next_attempt_unix_ms"])

    def reserve(self, now_ms):
        if not self.due(now_ms):
            return None
        v = self.value
        if v["burst_retries_used"] < 3:
            v["burst_retries_used"] += 1
            kind = "retry"
            delay = RECOVERY_DELAYS[v["burst_retries_used"]] if v["burst_retries_used"] < 3 else RECOVERY_PROBE_SECONDS
            v["state"] = "BACKOFF"
        else:
            kind, delay, v["state"] = "probe", RECOVERY_PROBE_SECONDS, "OPEN"
        v.update(reservation=dict(kind=kind, reserved_unix_ms=now_ms), next_attempt_unix_ms=now_ms + delay * 1000,
                 healthy_since_unix_ms=None, healthy_start_archived_banks=None)
        self.save()  # No child starts before this reservation commits.
        return kind

    def failed(self, error, submitted_ms, kind, now_ms):
        v = self.value
        if not transient_transfer(error):
            self.block(error)
            raise error
        outage = v["outage_since_unix_ms"]
        if kind is not None:
            v["reservation"] = None
        used = v["burst_retries_used"]
        delay = RECOVERY_DELAYS[used] if used < 3 else RECOVERY_PROBE_SECONDS
        active_retry = v["reservation"] is not None and v["reservation"]["kind"] == "retry"
        v.update(state="BACKOFF" if used < 3 or active_retry else "OPEN", last_error=failure_fields(error),
                 last_failure_unix_ms=now_ms,
                 outage_since_unix_ms=min(outage if outage is not None else submitted_ms, submitted_ms),
                 next_attempt_unix_ms=max(v["next_attempt_unix_ms"] or 0, now_ms + delay * 1000),
                 healthy_since_unix_ms=None, healthy_start_archived_banks=None)
        self.save()

    def succeeded(self, kind, now_ms):
        if kind is not None:
            self.value.update(state="RECOVERING", reservation=None, healthy_since_unix_ms=now_ms,
                              healthy_start_archived_banks=self.archived())
            self.save()

    def tick(self, now_ms, failed_pending):
        v = self.value
        if (v["state"] == "RECOVERING" and not failed_pending and v["reservation"] is None
                and now_ms - v["healthy_since_unix_ms"] >= 60000
                and self.archived() > v["healthy_start_archived_banks"]):
            v.update(state="NORMAL", burst_retries_used=0, outage_since_unix_ms=None,
                     next_attempt_unix_ms=None, reservation=None, last_error=None,
                     last_failure_unix_ms=None, healthy_since_unix_ms=None, healthy_start_archived_banks=None)
            self.save()

    def block(self, error):
        if self.value["state"] == "BLOCKED" and self.persisted:
            return
        self.db.rollback()  # Never commit a partial archive transaction with the latch.
        if self.value["state"] != "BLOCKED":
            self.value.update(state="BLOCKED", reservation=None, last_error=failure_fields(error),
                              healthy_since_unix_ms=None, healthy_start_archived_banks=None)
        self.save()

    def status(self):
        return {key: self.value[key] for key in ("schema", "state", "burst_retries_used", "outage_since_unix_ms",
            "next_attempt_unix_ms", "reservation", "last_error", "last_failure_unix_ms", "healthy_since_unix_ms", "healthy_start_archived_banks")}


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate_transfer_field")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs)


def transfer_process(command, request, timeout=BUNDLE_TRANSFER_SECONDS):
    """Bound all child work and output; kill and reap before returning failure."""
    deadline = time.monotonic() + timeout
    raw = json.dumps(request, separators=(",", ":")).encode()
    if len(raw) > TRANSFER_REPORT_LIMIT:
        raise BundleTransferError()
    reader, writer = os.pipe()  # EOF makes the child exit if this parent dies.
    process = None
    try:
        with tempfile.TemporaryFile() as input_file:
            input_file.write(raw); input_file.seek(0)
            process = subprocess.Popen([*command, "--parent-watch-fd", str(reader)],
                stdin=input_file, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, pass_fds=(reader,))
        os.close(reader); reader = None
        output = bytearray()
        with selectors.DefaultSelector() as poll:
            poll.register(process.stdout, selectors.EVENT_READ)
            while poll.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BundleTransferTimeout("bundle_transfer_deadline")
                for key, _ in poll.select(remaining):
                    body = os.read(key.fd, TRANSFER_REPORT_LIMIT + 1 - len(output))
                    if not body:
                        poll.unregister(key.fileobj)
                    else:
                        output.extend(body)
                        if len(output) > TRANSFER_REPORT_LIMIT:
                            raise BundleTransferError()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise BundleTransferTimeout("bundle_transfer_deadline")
        try:
            code = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            raise BundleTransferTimeout("bundle_transfer_deadline") from None
        try:
            value = strict_json(output)
        except (ValueError, UnicodeError):
            raise BundleTransferError() from None
        return code, value
    finally:
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()
        if reader is not None:
            os.close(reader)
        os.close(writer)


def bundle_stamp(path):
    info = path.lstat()
    if not path.is_absolute() or not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= MAX_BUNDLE:
        raise BundleTransferError()
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def validate_transfer_result(code, value, expected):
    if code == 0 and isinstance(value, dict) and value == expected and type(value.get("bytes")) is int:
        return
    if (code == 1 and isinstance(value, dict)
            and set(value) == {"schema", "state", "error_type", "http_status", "network_error_type"}
            and value["schema"] == TRANSFER_SCHEMA and value["state"] == "FAILED"
            and isinstance(value["error_type"], str)
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", value["error_type"])
            and (value["http_status"] is None or type(value["http_status"]) is int and 100 <= value["http_status"] <= 599)
            and value["network_error_type"] in (None, "ProtocolError")
            and (value["network_error_type"] is None or value["error_type"] == "ResponseStreamingError")):
        raise BundleTransferError({key: value[key] for key in ("error_type", "http_status", "network_error_type")})
    raise BundleTransferError()


class R2:
    def __init__(self, credentials):
        import boto3
        from botocore.config import Config
        self.credentials_file = credentials.resolve(strict=True)
        self.helper = Path(__file__).resolve(strict=True)
        self.helper_sha256 = digest(self.helper)
        values = {}
        for line in credentials.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            k, v = line.split("=", 1)
            parts = shlex.split(v)
            if len(parts) != 1 or k in values:
                raise RuntimeError("invalid_credentials_file")
            values[k] = parts[0]
        endpoint = values.get("R2_S3_ENDPOINT", values.get("R2_ENDPOINT", values.get("AWS_ENDPOINT_URL_S3")))
        self.bucket = values.get("R2_BUCKET", values.get("S3_BUCKET"))
        if not endpoint or not self.bucket or not endpoint.startswith("https://"):
            raise RuntimeError("missing_r2_destination")
        self.namespace = hashlib.sha256((endpoint + "\0" + self.bucket).encode()).hexdigest()
        self.client = boto3.client("s3", endpoint_url=endpoint,
            region_name=values.get("R2_S3_REGION", values.get("R2_REGION", "auto")),
            aws_access_key_id=values.get("R2_ACCESS_KEY_ID", values.get("AWS_ACCESS_KEY_ID")),
            aws_secret_access_key=values.get("R2_SECRET_ACCESS_KEY", values.get("AWS_SECRET_ACCESS_KEY")),
            config=Config(connect_timeout=10, read_timeout=60, retries={"max_attempts": 3},
                          request_checksum_calculation="when_required", response_checksum_validation="when_required"))

    def put_verified(self, key, path, sha):
        path = path.resolve(strict=True)
        stamp = bundle_stamp(path)
        if digest(self.helper) != self.helper_sha256:
            raise BundleTransferError()
        expected = dict(schema=TRANSFER_SCHEMA, state="VERIFIED", namespace=self.namespace,
                        key=key, sha256=sha, bytes=stamp[2], nonce=os.urandom(16).hex(),
                        helper_sha256=self.helper_sha256)
        request = dict(expected, path=str(path))
        code, value = transfer_process([sys.executable, str(self.helper), "_transfer",
                                       "--credentials-file", str(self.credentials_file)], request)
        validate_transfer_result(code, value, expected)
        if bundle_stamp(path) != stamp:
            raise BundleTransferError()

    def _put_verified_in_process(self, key, path, sha):
        from botocore.exceptions import ClientError, ConnectionClosedError, ReadTimeoutError, ResponseStreamingError
        from urllib3.exceptions import ReadTimeoutError as URLLib3ReadTimeoutError
        try:
            with open(path, "rb") as f:
                self.client.put_object(Bucket=self.bucket, Key=key, Body=f,
                    ContentLength=path.stat().st_size, IfNoneMatch="*", Metadata={"sha256": sha})
        except ClientError as e:
            if e.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
                raise
        for attempt in range(3):
            # A failed stream has no verified prefix. Start each whole GET at
            # byte zero with a fresh count/hash, and close it before backoff.
            h, count = hashlib.sha256(), 0
            try:
                response = self.client.get_object(Bucket=self.bucket, Key=key)
                with closing(response["Body"]) as body:
                    while b := body.read(1024**2):
                        count += len(b)
                        if count > MAX_BUNDLE:
                            raise RuntimeError("remote_bundle_too_large")
                        h.update(b)
            except (ReadTimeoutError, ConnectionClosedError, URLLib3ReadTimeoutError, ResponseStreamingError) as error:
                if isinstance(error, ResponseStreamingError) and not streaming_protocol_error(error):
                    raise
                if attempt == 2:
                    raise
                time.sleep(attempt + 1)
                continue
            if count != path.stat().st_size or h.hexdigest() != sha:
                raise RuntimeError("remote_bundle_readback_mismatch")
            return

    def download(self, key, destination, sha, before_write):
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        count = 0
        with response["Body"] as body, open(destination, "xb") as f:
            while b := body.read(1024**2):
                count += len(b)
                if count > MAX_BUNDLE:
                    raise RuntimeError("remote_bundle_too_large")
                before_write(len(b))
                f.write(b)
            f.flush()
            os.fsync(f.fileno())
        if digest(destination) != sha:
            raise RuntimeError("remote_bundle_readback_mismatch")


def transfer_child(credentials, watch_fd):
    # This process has no database, source-retirement or restore authority.
    # The pipe is held open only by its parent; SIGTERM/SIGKILL also closes it.
    if not stat.S_ISFIFO(os.fstat(watch_fd).st_mode):
        raise BundleTransferError()
    def parent_watch():
        try:
            os.read(watch_fd, 1)
        finally:
            os._exit(70)
    threading.Thread(target=parent_watch, daemon=True).start()
    try:
        raw = sys.stdin.buffer.read(TRANSFER_REPORT_LIMIT + 1)
        request = strict_json(raw)
        fields = {"schema", "state", "namespace", "key", "sha256", "bytes", "nonce", "helper_sha256", "path"}
        if (len(raw) > TRANSFER_REPORT_LIMIT or not isinstance(request, dict) or set(request) != fields
                or request["schema"] != TRANSFER_SCHEMA or request["state"] != "VERIFIED"
                or any(not isinstance(request[name], str) or not re.fullmatch(r"[0-9a-f]{64}", request[name])
                       for name in ("namespace", "sha256", "helper_sha256"))
                or not isinstance(request["nonce"], str) or not re.fullmatch(r"[0-9a-f]{32}", request["nonce"])
                or not isinstance(request["key"], str) or len(request["key"]) > 1024
                or not request["key"].startswith("native-custody/processed-v1/")
                or not request["key"].endswith("/" + request["sha256"] + ".tar")
                or type(request["bytes"]) is not int or not 0 < request["bytes"] <= MAX_BUNDLE
                or not isinstance(request["path"], str)):
            raise BundleTransferError()
        path = Path(request["path"])
        stamp = bundle_stamp(path)
        if (path.resolve(strict=True) != path or stamp[2] != request["bytes"]
                or digest(Path(__file__).resolve(strict=True)) != request["helper_sha256"]
                or digest(path) != request["sha256"]):
            raise BundleTransferError()
        remote = R2(credentials)
        if remote.namespace != request["namespace"]:
            raise BundleTransferError()
        remote._put_verified_in_process(request["key"], path, request["sha256"])
        if bundle_stamp(path) != stamp:
            raise BundleTransferError()
        result = {key: value for key, value in request.items() if key != "path"}
        code = 0
    except Exception as error:
        result = dict(schema=TRANSFER_SCHEMA, state="FAILED", **failure_fields(error))
        code = 1
    # Only this small fixed-shape record crosses the process boundary.
    print(json.dumps(result, separators=(",", ":")), flush=True)
    return code


def database(state, source, namespace):
    state.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(state / "archive.sqlite", timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("CREATE TABLE IF NOT EXISTS meta(id INTEGER PRIMARY KEY,value TEXT);"
                     "CREATE TABLE IF NOT EXISTS archives(slot INTEGER,bank INTEGER,record TEXT,retired INTEGER DEFAULT 0,PRIMARY KEY(slot,bank));"
                     "CREATE INDEX IF NOT EXISTS archives_retired ON archives(retired);")
    expected = json.dumps({"source": source, "namespace": namespace}, sort_keys=True)
    db.execute("INSERT OR IGNORE INTO meta VALUES(1,?)", (expected,))
    if db.execute("SELECT value FROM meta WHERE id=1").fetchone()[0] != expected:
        db.close()
        raise RuntimeError("archive_source_or_destination_changed")
    db.commit()
    return db


def candidates(feed, after, scan_after=0, scan_through=2**63 - 1, scan_bank=2**63 - 1):
    # Match the durable cloud index in SQLite. Do not rescan every already
    # archived bank for each small bundle as retained history grows.
    return feed.execute("SELECT a.slot,a.bank,a.artifact,a.marker_sha,a.custody_path,a.custody_sha,"
        "(SELECT max(e.sequence) FROM feed_events e WHERE e.slot=a.slot AND e.bank=a.bank) "
        "FROM archive_catalog a JOIN banks b ON b.slot=a.slot AND b.bank=a.bank "
        "WHERE b.state='rooted' AND b.dirty=0 AND a.retirement_enqueued=1 "
        "AND (a.slot,a.bank)>(?,?) AND a.slot<=? "
        "AND (SELECT max(e.sequence) FROM feed_events e WHERE e.slot=a.slot AND e.bank=a.bank)<? "
        "AND NOT EXISTS(SELECT 1 FROM cold.archives c WHERE c.slot=a.slot AND c.bank=a.bank) "
        "ORDER BY a.slot,a.bank LIMIT 128", (scan_after, scan_bank, scan_through, after)).fetchall()


def build_bank(root, row, source):
    slot, bank, artifact, marker_sha, proof_path, proof_sha, _ = row
    artifact, proof_path = checked(root, artifact), checked(root, proof_path)
    marker_path = artifact / "DIRECT-AGAVE-COMPACT.v1.json"
    marker, proof = read_json(marker_path), read_json(proof_path)
    if digest(marker_path) != marker_sha or digest(proof_path) != proof_sha:
        raise RuntimeError("source_marker_changed")
    if any(marker.get(k) != source[k] for k in SOURCE_KEYS) or proof["session_id"] != source["session_id"]:
        raise RuntimeError("source_identity_mismatch")
    if proof["state"] != "COMPLETE" or not proof["finalized"] or proof["payload_marker_sha256"] != marker_sha:
        raise RuntimeError("custody_not_complete")
    if Path(proof["payload_marker_path"]) != marker_path:
        raise RuntimeError("custody_payload_path_mismatch")
    for b in [marker["produced_blocks"][0], proof["produced_blocks"][0]]:
        if b["slot"] != slot or b["bank_id"] != bank:
            raise RuntimeError("custody_bank_mismatch")
    files = []
    for base, header, name in [(artifact, marker, "DIRECT-AGAVE-COMPACT.v1.json"),
                               (proof_path.parent, proof, proof_path.name)]:
        entries = header["files"] + [{"path": name, "bytes": (base / name).stat().st_size,
                                      "sha256": digest(base / name), "keep": True}]
        for entry in entries:
            if len(PurePosixPath(entry["path"]).parts) != 1 or entry["path"] in (".", ".."):
                raise RuntimeError("unsafe_manifest_name")
            path = checked(root, base / entry["path"])
            before = identity(path)
            sha = digest(path)
            if before != identity(path) or before[2] != entry["bytes"] or sha != entry["sha256"]:
                raise RuntimeError("source_file_changed")
            files.append({"path": str(path.relative_to(root)), "bytes": before[2],
                          "sha256": sha, "stamp": before, "keep": entry.get("keep", False)})
    return {"slot": slot, "bank": bank, "artifact": str(artifact.relative_to(root)),
            "custody": str(proof_path.parent.relative_to(root)), "source": source,
            "through_sequence": proof["through_sequence"], "files": files}


def pack(root, records, path):
    manifest = json.dumps({"schema": "blockzilla-native-custody-bundle-v1", "banks": records}).encode()
    if sum(f["bytes"] for r in records for f in r["files"]) + len(manifest) + 1024**2 > MAX_BUNDLE:
        raise RuntimeError("bundle_too_large")
    with tarfile.open(path, "w") as tar:
        info = tarfile.TarInfo("BUNDLE.json")
        info.size = len(manifest)
        info.mode = 0o600
        tar.addfile(info, io.BytesIO(manifest))
        for r in records:
            for entry in r["files"]:
                p = checked(root, root / entry["path"])
                if identity(p) != entry["stamp"]:
                    raise RuntimeError("source_changed_before_bundle")
                info = tarfile.TarInfo(entry["path"])
                info.size, info.mode = entry["bytes"], 0o600
                with open(p, "rb") as f:
                    tar.addfile(info, f)
                if identity(p) != entry["stamp"]:
                    raise RuntimeError("source_changed_during_bundle")
    with open(path, "rb") as f:
        os.fsync(f.fileno())


def retire(root, db, record):
    # Preserve small native markers. The full bytes and their file hashes are in
    # the verified remote bundle. Restore can recreate exact original paths.
    note = {k: record[k] for k in ["source", "slot", "bank", "key", "bundle_sha256", "namespace", "files"]}
    note["schema"] = "blockzilla-native-cold-copy-v1"
    for folder in [record["artifact"], record["custody"]]:
        atomic(checked(root, root / folder) / "REMOTE-CUSTODY.json", note)
    removed = 0
    changed_directories = set()
    for entry in record["files"]:
        if entry["keep"]:
            continue
        path = checked(root, root / entry["path"])
        if not path.exists():
            continue
        if digest(path) != entry["sha256"]:
            raise RuntimeError("source_changed_before_retirement")
        # A newly restored copy has another inode. Leave it available to its reader.
        if identity(path) != entry["stamp"]:
            continue
        removed += path.stat().st_blocks * 512
        path.unlink()
        changed_directories.add(path.parent)
    # One directory sync makes all of this bank's unlinks durable. Commit the
    # retired flag only after both artifact/custody directories are synced.
    for parent in changed_directories:
        fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    db.execute("UPDATE archives SET retired=1 WHERE slot=? AND bank=?", (record["slot"], record["bank"]))
    db.commit()
    return removed


def upload_bundle(root, remote, records, path):
    pack(root, records, path)
    sha = digest(path)
    key = "native-custody/processed-v1/" + records[0]["source"]["session_id"] + "/" + sha + ".tar"
    remote.put_verified(key, path, sha)
    return sha, key


def commit_bundle(root, db, remote, records, path, sha, key, retire_files):
    for record in records:
        record.update(key=key, bundle_sha256=sha, namespace=remote.namespace)
        db.execute("INSERT INTO archives(slot,bank,record) VALUES(?,?,?)",
                   (record["slot"], record["bank"], json.dumps(record)))
    # Save progress in the same transaction as the verified recovery index.
    # Existing archives without this field have an unknown commit time.
    db.execute("INSERT OR REPLACE INTO meta(id,value) VALUES(2,?)", (str(int(time.time() * 1000)),))
    db.commit()  # Recovery index commits strictly after full remote readback.
    freed = sum(retire(root, db, r) for r in records) if retire_files else 0
    path.unlink()
    return freed


def archive_group(root, db, remote, records, state, retire_files):
    path = state / "bundle.tar"
    sha, key = upload_bundle(root, remote, records, path)
    return commit_bundle(root, db, remote, records, path, sha, key, retire_files)


class Transfers:
    """Only network/packing runs in threads. SQLite and retirement have one owner.

    The client is created before threads start, per the Boto3 client guidance:
    https://docs.aws.amazon.com/boto3/latest/guide/clients.html
    """
    def __init__(self, root, db, remote, state, retire_files, limit, recovery=None, clock=None):
        self.root, self.db, self.remote, self.state = root, db, remote, state
        self.retire_files, self.limit = retire_files, limit
        self.pool = ThreadPoolExecutor(max_workers=limit)
        self.pending = {}
        self.recovery = recovery
        self.clock = clock or (lambda: int(time.time() * 1000))

    def can_submit(self):
        if len(self.pending) >= self.limit:
            return False
        return (self.recovery is None or self.recovery.fresh_allowed()
                or not self.pending and self.recovery.due(self.clock()))

    def pending_banks(self):
        return {(r['slot'], r['bank']) for data in self.pending.values() for r in data['records']}

    def submit(self, records):
        if not self.can_submit():
            return False
        kind = None
        if self.recovery is not None and not self.recovery.fresh_allowed():
            kind = self.recovery.reserve(self.clock())
            if kind is None:
                return False
        used = {data['number'] for data in self.pending.values()}
        number = next(n for n in range(self.limit) if n not in used)
        path = self.state / f'bundle-{number}.tar'
        future = self.pool.submit(upload_bundle, self.root, self.remote, records, path)
        self.pending[future] = dict(number=number, path=path, records=records, started=time.monotonic(),
            submitted_unix_ms=self.clock(), retry_wait=False, attempt_kind=kind)
        return True

    def collect(self, block=False):
        if block and self.pending:
            active = [future for future, data in self.pending.items() if not data['retry_wait']]
            if active:
                wait(active, timeout=1, return_when=FIRST_COMPLETED)
            else:
                time.sleep(1)  # Backoff must not block STATUS writes or spin the CPU.
        committed = []
        for future in list(self.pending):
            data = self.pending[future]
            if data['retry_wait'] or not future.done():
                continue
            try:
                sha, key = future.result()  # Failure has no SQLite or deletion side effects.
                freed = commit_bundle(self.root, self.db, self.remote, data['records'], data['path'],
                                      sha, key, self.retire_files)
                if self.recovery is not None:
                    self.recovery.succeeded(data['attempt_kind'], self.clock())
            except Exception as error:
                if self.recovery is None:
                    raise
                self.recovery.failed(error, data['submitted_unix_ms'], data['attempt_kind'], self.clock())
                data.update(retry_wait=True, attempt_kind=None)
                continue
            committed.append(dict(banks=len(data['records']), freed=freed,
                                  seconds=time.monotonic() - data['started']))
            del self.pending[future]
        if self.recovery is not None:
            for future, data in list(self.pending.items()):
                if data['retry_wait'] and self.recovery.due(self.clock()):
                    kind = self.recovery.reserve(self.clock())
                    if kind is not None:
                        replacement = self.pool.submit(upload_bundle, self.root, self.remote, data['records'], data['path'])
                        del self.pending[future]
                        data.update(retry_wait=False, attempt_kind=kind)
                        self.pending[replacement] = data
                    break
            self.recovery.tick(self.clock(), any(data['retry_wait'] for data in self.pending.values()))
        return committed

    def close(self):
        self.pool.shutdown(wait=True)


def progress_status(transfers, last_progress_ms, now_ms):
    pending = list(transfers.pending.values())
    eligible_since = min((d['submitted_unix_ms'] for d in pending), default=None)
    recovery = getattr(transfers, 'recovery', None)
    outage = recovery.value['outage_since_unix_ms'] if recovery is not None else None
    if outage is not None:
        eligible_since = min(eligible_since, outage) if eligible_since is not None else outage
    oldest_slot = min((r['slot'] for d in pending for r in d['records']), default=None)
    if pending or outage is not None:
        last_activity = max(eligible_since, last_progress_ms or eligible_since)
        progress = ('STALLED' if now_ms - last_activity >= STORAGE_STALL_SECONDS * 1000
                    else 'TRANSFERRING')
    elif last_progress_ms is not None and now_ms - last_progress_ms < STORAGE_STALL_SECONDS * 1000:
        progress = 'ADVANCING'
    else:
        progress = 'WAITING'
    return dict(storage_progress=progress, progress_stall_seconds=STORAGE_STALL_SECONDS,
                last_archive_progress_unix_ms=last_progress_ms,
                eligible_wait_since_unix_ms=eligible_since, oldest_eligible_pending_slot=oldest_slot)


def archive_allowance(archived_banks, pending_banks, maximum):
    """Count durable banks and all reserved transfers against one absolute cap."""
    if maximum is None:
        return None
    if type(maximum) is not int or maximum < 1:
        raise RuntimeError("invalid_absolute_archive_limit")
    return max(0, maximum - archived_banks - len(pending_banks))


def safe_failure_trace(error):
    # Exception messages, locals and source lines can contain credentials or
    # signed request URLs. Keep only the real stack locations and error type.
    kind = "".join(c for c in type(error).__name__ if c.isalnum() or c == "_")[:64]
    frames = list(traceback.walk_tb(error.__traceback__))[-24:]
    lines = ["cold_storage_archive_failed: " + kind]
    for frame, line in frames:
        lines.append(f"  File {frame.f_code.co_filename[:512]}, line {line}, in {frame.f_code.co_name[:128]}")
    return kind, "\n".join(lines)[-8192:]


def record_archive_failure(context, error):
    kind, trace = safe_failure_trace(error)
    evidence = failure_fields(error)
    kind = evidence["error_type"]
    print(trace, file=sys.stderr, flush=True)
    # A second invocation that fails to acquire LOCK must not mark a healthy
    # owner FAILED. This context is set only after exclusive lock acquisition.
    if not context:
        return
    state = context["state"]
    previous = {}
    try:
        previous = read_json(state / "STATUS.json")
        if not isinstance(previous, dict):
            previous = {}
    except (OSError, ValueError, RuntimeError):
        pass
    try:
        path = state / "archive.sqlite"
        if path.exists():
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
                db.execute("PRAGMA query_only=ON")
                count, retired = db.execute("SELECT count(*),coalesce(sum(retired),0) FROM archives").fetchone()
                previous.update(archived_banks=count, retired_banks=retired)
                progress = db.execute("SELECT value FROM meta WHERE id=2").fetchone()
                if progress:
                    previous["last_archive_progress_unix_ms"] = int(progress[0])
    except (OSError, ValueError, sqlite3.Error):
        pass
    previous.update(schema="blockzilla-native-cold-storage-status-v1", state="FAILED",
                    storage_progress="FAILED", error="archive_operation_failed", error_type=kind,
                    failure_traceback=trace, updated_unix_ms=int(time.time() * 1000))
    previous["http_status"] = evidence["http_status"]
    previous["network_error_type"] = evidence["network_error_type"]
    if context.get("recovery") is not None:
        previous["recovery"] = context["recovery"].status()
        previous["recovery_state_persisted"] = (getattr(context["recovery"], "persisted", False)
                                                and not context.get("recovery_latch_write_failed", False))
    if "source" in context:
        previous["source"] = context["source"]
    invocation = os.environ.get("INVOCATION_ID", "")
    if len(invocation) == 32 and all(c in "0123456789abcdef" for c in invocation):
        previous["invocation_id"] = invocation
    try:
        atomic(state / "STATUS.json", previous)
        atomic(state / "LAST-FAILURE.json", previous)
    except (OSError, ValueError):
        print("cold_storage_failed_status_write_failed", file=sys.stderr, flush=True)


def restore_space(root, reserve, margin, additional, disk_usage):
    if disk_usage(root).free < reserve + margin + additional:
        raise RuntimeError("restore_disk_reserve_reached")


def restore_member(tar, note, requested):
    members = {}
    for member in tar:
        name = PurePosixPath(member.name)
        if (member.name in members or len(members) >= 8193
                or name.is_absolute() or not name.parts
                or any(p in (".", "..") for p in name.parts) or str(name) != member.name
                or not member.isfile() or member.size > MAX_FILE):
            raise RuntimeError("invalid_archive_members")
        members[member.name] = member
    manifest = members.get("BUNDLE.json")
    if manifest is None or manifest.size > 1024**2:
        raise RuntimeError("invalid_bundle_manifest")
    with tar.extractfile(manifest) as f:
        bundle = json.load(f)
    banks = bundle.get("banks")
    if (bundle.get("schema") != "blockzilla-native-custody-bundle-v1"
            or not isinstance(banks, list) or not 1 <= len(banks) <= 128):
        raise RuntimeError("invalid_bundle_manifest")
    if any(not isinstance(b, dict) for b in banks):
        raise RuntimeError("invalid_bundle_manifest")
    matching = [b for b in banks if (b.get("slot"), b.get("bank")) == (note["slot"], note["bank"])]
    if (len(matching) != 1 or any(b.get("source") != note["source"] for b in banks)
            or matching[0].get("files") != note["files"]):
        raise RuntimeError("bundle_receipt_mismatch")
    for entry in note["files"]:
        member = members.get(entry["path"])
        if member is None or member.size != entry["bytes"]:
            raise RuntimeError("archive_member_mismatch")
    return members[requested]


def restore(root, requested, state, remote, *, reserve_bytes=RESTORE_RESERVE,
            margin_bytes=RESTORE_MARGIN, lock_path=None, disk_usage=None):
    if reserve_bytes < 0 or margin_bytes < 0:
        raise RuntimeError("invalid_restore_reserve")
    disk_usage = shutil.disk_usage if disk_usage is None else disk_usage
    requested = checked(root, requested)
    parent = requested.parent
    note = read_json(parent / "REMOTE-CUSTODY.json")
    if note["schema"] != "blockzilla-native-cold-copy-v1" or note["namespace"] != remote.namespace:
        raise RuntimeError("invalid_cold_copy_source")
    header = read_json(root / "FEED.json")
    if any(note["source"].get(k) != header.get(k) for k in SOURCE_KEYS):
        raise RuntimeError("restore_source_mismatch")
    if not note["key"].startswith("native-custody/processed-v1/" + note["source"]["session_id"] + "/"):
        raise RuntimeError("invalid_cold_copy_key")
    relative = str(requested.relative_to(root))
    entries = {f["path"]: f for f in note["files"]}
    if len(entries) != len(note["files"]):
        raise RuntimeError("duplicate_cold_copy_files")
    if relative not in entries:
        raise RuntimeError("requested_path_not_archived")
    for entry in entries.values():
        checked(root, root / entry["path"])
        if type(entry["bytes"]) is not int or not 0 <= entry["bytes"] <= MAX_FILE:
            raise RuntimeError("invalid_cold_copy_file_size")
    entry = entries[relative]
    state = checked(root, state)
    # All sessions under the same processed-output parent share admission. The
    # lock stays held through transfer, extraction and publication; no reader's
    # existing file is evicted. Separate output parents must set a common lock.
    lock_path = Path(lock_path) if lock_path is not None else root.parent / ".native-cold-restore.lock"
    if not lock_path.is_absolute():
        raise RuntimeError("unsafe_restore_lock")
    lock_path = lock_path.parent.resolve(strict=True) / lock_path.name
    lock_path = checked(Path(lock_path.anchor), lock_path)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a") as lock:
        if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
            raise RuntimeError("unsafe_restore_lock")
        fcntl.flock(lock, fcntl.LOCK_EX)
        if requested.exists():
            before = identity(requested)
            if (before[2] != entry["bytes"] or digest(requested) != entry["sha256"]
                    or before != identity(requested)):
                raise RuntimeError("restore_conflicts_with_local_file")
            return
        restore_space(root, reserve_bytes, margin_bytes, MAX_BUNDLE + entry["bytes"], disk_usage)
        state.mkdir(parents=True, exist_ok=True)
        if state.stat().st_dev != root.stat().st_dev or parent.stat().st_dev != root.stat().st_dev:
            raise RuntimeError("restore_filesystem_mismatch")
        # Each call owns a unique directory. Failed or partial downloads cannot
        # remove another process's temporary file, including after PID reuse.
        with tempfile.TemporaryDirectory(prefix="restore-", dir=state) as work:
            temporary, stage = Path(work) / "bundle.tar", Path(work) / "requested-file"
            remote.download(note["key"], temporary, note["bundle_sha256"],
                            lambda n: restore_space(root, reserve_bytes, margin_bytes,
                                                    entry["bytes"] + n, disk_usage))
            with tarfile.open(temporary, "r:") as tar:
                member = restore_member(tar, note, relative)
                with tar.extractfile(member) as source, open(stage, "xb") as f:
                    while b := source.read(1024**2):
                        restore_space(root, reserve_bytes, margin_bytes, len(b), disk_usage)
                        f.write(b)
                    f.flush()
                    os.fsync(f.fileno())
            if digest(stage) != entry["sha256"]:
                raise RuntimeError("archive_member_digest_mismatch")
            stage.chmod(0o600)
            # Hard-link publication never overwrites another reader's copy.
            try:
                os.link(stage, requested)
            except FileExistsError:
                if digest(requested) != entry["sha256"]:
                    raise RuntimeError("restore_conflicts_with_local_file")
            fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)


def archive(a):
    context = {}
    with ExitStack() as resources:
        try:
            return archive_open(a, resources, context)
        except Exception as error:
            try:
                if context.get("recovery") is not None:
                    context["recovery"].block(error)
            except Exception:
                context["recovery_latch_write_failed"] = True
                print("cold_storage_recovery_latch_write_failed", file=sys.stderr, flush=True)
            record_archive_failure(context, error)
            raise RuntimeError("cold_storage_archive_failed; inspect LAST-FAILURE.json") from None


def archive_open(a, resources, failure_context=None):
    root = a.output.resolve(strict=True)
    header = read_json(root / "FEED.json")
    source = {k: header[k] for k in SOURCE_KEYS}
    bank_header = read_json(a.bank_output / "FEED.json")
    if any(bank_header[k] != source[k] for k in SOURCE_KEYS) or Path(bank_header["payload_root"]).resolve() != root:
        raise RuntimeError("bank_source_mismatch")
    state = root / "cold-storage"
    state.mkdir(parents=True, exist_ok=True)
    lock = open(state / "LOCK", "a")
    resources.callback(lock.close)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if failure_context is not None:
        failure_context.update(state=state, source=source)
    remote = R2(a.credentials_file)
    db = database(state, source, remote.namespace)
    resources.callback(db.close)
    recovery = RecoveryCircuit(db, source, remote.namespace, getattr(a, "max_archived_banks", None))
    if failure_context is not None:
        failure_context["recovery"] = recovery
    feed = sqlite3.connect("file:" + str(root / "feed.sqlite") + "?mode=ro", uri=True, timeout=30)
    resources.callback(feed.close)
    feed.execute("ATTACH DATABASE ? AS cold", ("file:" + str(state / "archive.sqlite") + "?mode=ro",))
    banks = sqlite3.connect("file:" + str(a.bank_output / "feed.sqlite") + "?mode=ro", uri=True, timeout=30)
    resources.callback(banks.close)
    capture = sqlite3.connect("file:" + str(a.capture / "events.sqlite") + "?mode=ro", uri=True, timeout=30)
    resources.callback(capture.close)
    retired, freed, batches, scan_after, scan_bank = 0, 0, 0, 0, -1
    scan_through = None
    sweeps_started, sweeps_completed, scan_pages, scan_rows = 0, 0, 0, 0
    last_candidate_seen, last_candidate_eligible = None, None
    last_skips = dict(frame_replay=0, pending=0, archived=0, retention=0, bank_replay=0, writer_ack=0)
    saved_progress = db.execute("SELECT value FROM meta WHERE id=2").fetchone()
    last_progress = int(saved_progress[0]) if saved_progress else None
    transfers = Transfers(root, db, remote, state, a.retire, a.parallel_bundles, recovery=recovery)
    resources.callback(transfers.close)
    submitted_once = False
    batch_seconds, last_batch_banks = None, 0
    max_archived = getattr(a, "max_archived_banks", None)
    archive_allowance(0, set(), max_archived)
    while True:
        for done in transfers.collect(block=bool(transfers.pending)):
            freed += done['freed']
            retired += done['banks']
            batches += 1
            batch_seconds, last_batch_banks = done['seconds'], done['banks']
            last_progress = int(db.execute("SELECT value FROM meta WHERE id=2").fetchone()[0])
        if a.retire:
            for raw, in db.execute("SELECT record FROM archives WHERE retired=0").fetchall():
                freed += retire(root, db, json.loads(raw))
        archived = db.execute("SELECT count(*) FROM archives").fetchone()[0]
        allowance = archive_allowance(archived, transfers.pending_banks(), max_archived)
        absolute_done = max_archived is not None and archived >= max_archived
        cp = read_json(a.publisher_state / "CHECKPOINT.json")
        if Path(cp["source"]).resolve() != root or cp["session"] != source["session_id"]:
            raise RuntimeError("frame_publisher_source_mismatch")
        session, position, _ = cp["cursor"].split(":")
        if session != source["session_id"] or not position.startswith("p"):
            raise RuntimeError("frame_publisher_cursor_mismatch")
        after = min(int(position[1:]), banks.execute("SELECT processed_sequence FROM feed_state WHERE id=1").fetchone()[0])
        after = min(after, replay_base(a.v1_spool / "CHECKPOINT.json", source))
        bank_base = replay_base(a.bank_origin_spool / "CHECKPOINT.json", source, bank=True)
        bounded_done = bool(a.max_batches and batches + len(transfers.pending) >= a.max_batches)
        can_submit = (transfers.can_submit() and not bounded_done and allowance != 0
                      and (a.follow or not submitted_once))
        if can_submit and scan_through is None:
            # Freeze each sweep's end. A growing live tail must not prevent a
            # revisit of banks skipped for age, replay or native writer ACK.
            # archive_pending_retirement(retirement_enqueued,slot) bounds this
            # lookup. Pages and individual gate queries remain bounded/indexed.
            scan_through = feed.execute("SELECT max(slot) FROM archive_catalog "
                                        "WHERE retirement_enqueued=1").fetchone()[0] or 0
            scan_after, scan_bank = 0, -1
            sweeps_started += 1
        rows = candidates(feed, after, scan_after, scan_through, scan_bank) if can_submit else []
        sweep_completed = can_submit and not rows
        if can_submit and not rows:
            sweeps_completed += 1
            scan_through = None
        if rows:
            scan_pages += 1
            last_candidate_seen = int(time.time() * 1000)
        records = []
        group_bytes = 0
        skips = dict(frame_replay=0, pending=0, archived=0, retention=0, bank_replay=0, writer_ack=0)
        pending_banks = transfers.pending_banks()
        for row in rows:
            previous_scan = (scan_after, scan_bank)
            scan_after, scan_bank = row[:2]
            scan_rows += 1
            if row[6] is None or row[6] >= after:
                skips['frame_replay'] += 1
                continue
            if row[:2] in pending_banks:
                skips['pending'] += 1
                continue
            if db.execute("SELECT 1 FROM archives WHERE slot=? AND bank=?", row[:2]).fetchone():
                skips['archived'] += 1
                continue
            marker = Path(row[2]) / "DIRECT-AGAVE-COMPACT.v1.json"
            if time.time() - marker.stat().st_mtime < a.keep_seconds:
                skips['retention'] += 1
                continue
            last_bank_event = banks.execute("SELECT max(sequence) FROM feed_events WHERE artifact=?", (row[2],)).fetchone()[0]
            if last_bank_event is None or last_bank_event >= bank_base:
                skips['bank_replay'] += 1
                continue
            record = build_bank(root, row, source)
            if capture.execute("SELECT 1 FROM events WHERE slot=? AND bank=? AND sequence<=? LIMIT 1",
                               (row[0], row[1], record["through_sequence"])).fetchone():
                skips['writer_ack'] += 1
                continue  # Wait for native writer acknowledgement, never delete its proof first.
            last_candidate_eligible = int(time.time() * 1000)
            record_bytes = sum(f["bytes"] for f in record["files"])
            if group_bytes + record_bytes + 1024**2 > MAX_BUNDLE:
                if not records:
                    raise RuntimeError("single_bank_exceeds_bundle_bound")
                scan_after, scan_bank = previous_scan
                break
            records.append(record)
            group_bytes += record_bytes
            if len(records) >= a.batch_banks or allowance is not None and len(records) >= allowance:
                break
        if records:
            transfers.submit(records)
            submitted_once = True
        if rows:
            last_skips = skips
        status = {"schema": "blockzilla-native-cold-storage-status-v1",
                  "state": "COMPLETE" if absolute_done and not transfers.pending else "RUNNING",
                  "archived_banks": archived,
                  "retired_banks": db.execute("SELECT count(*) FROM archives WHERE retired=1").fetchone()[0],
                  "freed_bytes_this_run": freed, "keep_seconds": a.keep_seconds,
                  "batch_bank_limit": a.batch_banks,
                  "parallel_bundle_limit": a.parallel_bundles, "pending_bundles": len(transfers.pending),
                  "last_batch_banks": last_batch_banks, "last_batch_seconds": batch_seconds,
                  "scan_sweeps_started": sweeps_started, "scan_sweeps_completed": sweeps_completed,
                  "scan_pages_this_run": scan_pages, "scan_rows_this_run": scan_rows,
                  "scan_after_slot": scan_after, "scan_after_bank": scan_bank, "scan_through_slot": scan_through,
                  "last_candidate_seen_unix_ms": last_candidate_seen,
                  "last_candidate_eligible_unix_ms": last_candidate_eligible,
                  "last_scan_skip_counts": last_skips,
                  "full_remote_readback_required": True, "updated_unix_ms": int(time.time() * 1000)}
        if max_archived is not None:
            status.update(max_archived_banks=max_archived,
                          archive_allowance_banks=archive_allowance(archived, transfers.pending_banks(), max_archived))
            if absolute_done and not transfers.pending:
                status["stop_reason"] = "max_archived_banks"
        status.update(progress_status(transfers, last_progress, status['updated_unix_ms']))
        status['recovery'] = recovery.status()
        atomic(state / "STATUS.json", status)
        if not transfers.pending and (absolute_done or recovery.fresh_allowed()
                and ((a.max_batches and batches >= a.max_batches) or not a.follow)):
            print(json.dumps(status))
            return
        if not records and not transfers.pending:
            # Continue a finite sweep without a 15-second wait per blocked page.
            # Yield between pages; wait normally after the entire sweep ends.
            time.sleep(1 if not recovery.fresh_allowed() else 15 if sweep_completed else SCAN_PAGE_PAUSE_SECONDS)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_transfer":
        child = argparse.ArgumentParser()
        child.add_argument("--credentials-file", type=Path, required=True)
        child.add_argument("--parent-watch-fd", type=int, required=True)
        options = child.parse_args(sys.argv[2:])
        os.umask(0o077)
        raise SystemExit(transfer_child(options.credentials_file, options.parent_watch_fd))
    p = argparse.ArgumentParser(description=__doc__)
    modes = p.add_subparsers(dest="mode", required=True)
    put = modes.add_parser("archive")
    for name in ["output", "bank-output", "capture", "publisher-state", "credentials-file", "v1-spool", "bank-origin-spool"]:
        put.add_argument("--" + name, type=Path, required=True)
    put.add_argument("--keep-seconds", type=int, default=7200)
    put.add_argument("--retire", action="store_true")
    put.add_argument("--follow", action="store_true")
    put.add_argument("--max-batches", type=int)
    put.add_argument("--max-archived-banks", type=int,
                     help="absolute durable archive count for this source, including earlier invocations")
    put.add_argument("--batch-banks", type=int, choices=range(1, 129), default=64)
    put.add_argument("--parallel-bundles", type=int, choices=[1, 2, 4], default=2)
    get = modes.add_parser("restore")
    get.add_argument("--output", type=Path, required=True)
    get.add_argument("--path", type=Path, required=True)
    get.add_argument("--credentials-file", type=Path,
                     default=Path(os.environ.get("HIVEZILLA_NATIVE_ARCHIVE_CREDS", "/missing-credentials")))
    get.add_argument("--reserve-bytes", type=int, default=RESTORE_RESERVE)
    get.add_argument("--margin-bytes", type=int, default=RESTORE_MARGIN)
    get.add_argument("--restore-lock", type=Path,
                     help="shared admission lock; default is in the processed-output parent")
    a = p.parse_args()
    os.umask(0o077)
    if a.mode == "archive":
        if (a.keep_seconds < 7200 or a.max_batches is not None and a.max_batches < 1
                or a.max_archived_banks is not None and a.max_archived_banks < 1):
            p.error("retain at least two hours; archive limits must be positive")
        archive(a)
    else:
        if a.reserve_bytes < 0 or a.margin_bytes < 0:
            p.error("restore reserve and margin must not be negative")
        root = a.output.resolve(strict=True)
        restore(root, a.path, root / "cold-storage", R2(a.credentials_file),
                reserve_bytes=a.reserve_bytes, margin_bytes=a.margin_bytes, lock_path=a.restore_lock)
