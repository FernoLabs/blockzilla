#!/usr/bin/env python3
"""Aggregate local shred telemetry into a bounded, public-safe status document.

The collector reads two independent local sources:

* shred-reader's loopback-only ``/metrics`` JSON endpoint; and
* Hivezilla's atomically published post-fsync shred status file.

Only fixed counters and state enums are selected into the public document. Source
URLs, filesystem paths, process or journal identities, peer addresses, and
arbitrary error text are never copied into the output.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import os
from pathlib import Path
import stat
import sys
import threading
import time
from urllib.parse import SplitResult, urlsplit


STATUS_PATH = "/api/v1/sidecars/shred-ingest/status.json"
HEALTH_PATH = "/healthz"
MAX_SOURCE_JSON_BYTES = 64 * 1024
MAX_PUBLIC_JSON_BYTES = 64 * 1024
MAX_SAFE_INTEGER = (1 << 53) - 1
MAX_HTTP_REQUESTS = 32
REQUEST_HEADER_TIMEOUT_SECS = 5.0
HIVEZILLA_STATES = {"waiting", "receiving", "stalled", "stopped"}
SOURCE_ERRORS = (
    OSError,
    UnicodeError,
    ValueError,
    TypeError,
    HTTPException,
    OverflowError,
    RecursionError,
)


@dataclass(frozen=True)
class ReceiverEndpoint:
    host: str
    port: int
    path: str = "/metrics"


@dataclass(frozen=True)
class StatusConfig:
    hivezilla_status_file: Path
    receiver_endpoint: ReceiverEndpoint
    output_file: Path | None = None
    cors_origin: str | None = None
    interval_secs: int = 5
    receiver_timeout_secs: float = 2.0
    hivezilla_stale_after_secs: int = 20
    tvu_active_after_secs: int = 30
    max_future_skew_secs: int = 5


def _object_without_duplicate_keys(pairs):
    value = {}
    for key, child in pairs:
        if key in value:
            raise ValueError("JSON contains a duplicate object key")
        value[key] = child
    return value


def _decode_json(raw: bytes):
    if not raw or len(raw) > MAX_SOURCE_JSON_BYTES:
        raise ValueError("source JSON has an invalid size")
    return json.loads(
        raw.decode("utf-8", errors="strict"),
        object_pairs_hook=_object_without_duplicate_keys,
    )


def read_bounded_regular(path: Path, maximum: int = MAX_SOURCE_JSON_BYTES) -> bytes:
    """Read one regular file without following its final symlink component."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("status input is not a regular file")
        if info.st_size <= 0 or info.st_size > maximum:
            raise ValueError("status input has an invalid size")
        chunks = []
        remaining = maximum + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if not payload or len(payload) > maximum:
            raise ValueError("status input has an invalid size")
        return payload
    finally:
        os.close(descriptor)


def _record(value, name: str):
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _integer(
    value,
    name: str,
    *,
    nullable: bool = False,
    positive: bool = False,
    maximum: int = MAX_SAFE_INTEGER,
):
    if nullable and value is None:
        return None
    lower = 1 if positive else 0
    if type(value) is not int or value < lower or value > maximum:
        qualifier = "positive" if positive else "non-negative"
        raise ValueError(f"{name} must be a {qualifier} bounded integer")
    return value


def _required(source: dict, key: str, name: str):
    if key not in source:
        raise ValueError(f"{name}.{key} is required")
    return source[key]


def parse_hivezilla_status(value, now_unix_secs: int, config: StatusConfig):
    source = _record(value, "Hivezilla status")
    if source.get("schema_version") != 1:
        raise ValueError("unsupported Hivezilla status schema")
    state = _required(source, "state", "Hivezilla status")
    if type(state) is not str or state not in HIVEZILLA_STATES:
        raise ValueError("unsupported Hivezilla recorder state")

    updated = _integer(
        _required(source, "updated_unix_secs", "Hivezilla status"),
        "updated_unix_secs",
        positive=True,
    )
    started = _integer(
        _required(source, "started_unix_secs", "Hivezilla status"),
        "started_unix_secs",
        positive=True,
    )
    last_durable = _integer(
        _required(source, "last_durable_unix_secs", "Hivezilla status"),
        "last_durable_unix_secs",
        nullable=True,
        positive=True,
    )
    accepted = _integer(
        _required(source, "accepted_total", "Hivezilla status"),
        "accepted_total",
    )
    invalid = _integer(
        _required(source, "invalid_total", "Hivezilla status"),
        "invalid_total",
    )
    payload_bytes = _integer(
        _required(source, "bytes_total", "Hivezilla status"),
        "bytes_total",
    )
    durable_sequence = _integer(
        _required(source, "durable_through_sequence", "Hivezilla status"),
        "durable_through_sequence",
        nullable=True,
    )
    latest_slot = _integer(
        _required(source, "latest_slot", "Hivezilla status"),
        "latest_slot",
        nullable=True,
    )
    shred_version = _integer(
        _required(source, "shred_version", "Hivezilla status"),
        "shred_version",
        nullable=True,
        maximum=(1 << 16) - 1,
    )
    spool_bytes = _integer(
        _required(source, "spool_bytes", "Hivezilla status"),
        "spool_bytes",
    )
    spool_max_bytes = _integer(
        _required(source, "spool_max_bytes", "Hivezilla status"),
        "spool_max_bytes",
        positive=True,
    )
    filesystem_free = _integer(
        _required(source, "filesystem_free_bytes", "Hivezilla status"),
        "filesystem_free_bytes",
    )
    filesystem_total = _integer(
        _required(source, "filesystem_total_bytes", "Hivezilla status"),
        "filesystem_total_bytes",
        positive=True,
    )
    reserve_free = _integer(
        _required(source, "reserve_free_bytes", "Hivezilla status"),
        "reserve_free_bytes",
    )

    if started > updated:
        raise ValueError("Hivezilla start timestamp is after its update")
    if last_durable is not None and last_durable > updated:
        raise ValueError("Hivezilla durable timestamp is after its update")
    if updated > now_unix_secs + config.max_future_skew_secs:
        raise ValueError("Hivezilla update timestamp is in the future")
    if spool_bytes > spool_max_bytes:
        raise ValueError("Hivezilla spool usage exceeds its configured maximum")
    if filesystem_free > filesystem_total or reserve_free > filesystem_total:
        raise ValueError("Hivezilla filesystem counters are contradictory")

    durable_tail = (durable_sequence, latest_slot, shred_version)
    has_durable_tail = any(child is not None for child in durable_tail)
    complete_durable_tail = all(child is not None for child in durable_tail)
    if has_durable_tail and not complete_durable_tail:
        raise ValueError("Hivezilla durable evidence is incomplete")
    if last_durable is not None and not complete_durable_tail:
        raise ValueError("Hivezilla durable timestamp lacks a durable tail")
    # Session counters restart with the process, while the durable sequence and
    # recovered tail survive. A recovered journal can therefore have a tail but
    # no last-durable wall-clock timestamp until this process fsyncs its first shred.
    if accepted > 0 and (not complete_durable_tail or last_durable is None):
        raise ValueError("Hivezilla accepted count lacks durable evidence")
    if accepted == 0 and payload_bytes != 0:
        raise ValueError("Hivezilla payload bytes exist without accepted shreds")

    fresh = (
        updated <= now_unix_secs + config.max_future_skew_secs
        and now_unix_secs <= updated + config.hivezilla_stale_after_secs
    )
    return {
        "availability": "available",
        "status_fresh": fresh,
        "state": state,
        "updated_unix_secs": updated,
        "started_unix_secs": started,
        "last_durable_unix_secs": last_durable,
        "accepted_total": accepted,
        "invalid_total": invalid,
        "bytes_total": payload_bytes,
        "durable_through_sequence": durable_sequence,
        "latest_slot": latest_slot,
        "shred_version": shred_version,
        "spool_bytes": spool_bytes,
        "spool_max_bytes": spool_max_bytes,
        "filesystem_free_bytes": filesystem_free,
        "filesystem_total_bytes": filesystem_total,
        "reserve_free_bytes": reserve_free,
    }


def unavailable_hivezilla_status():
    return {
        "availability": "unavailable",
        "status_fresh": False,
        "state": "unavailable",
        "updated_unix_secs": None,
        "started_unix_secs": None,
        "last_durable_unix_secs": None,
        "accepted_total": None,
        "invalid_total": None,
        "bytes_total": None,
        "durable_through_sequence": None,
        "latest_slot": None,
        "shred_version": None,
        "spool_bytes": None,
        "spool_max_bytes": None,
        "filesystem_free_bytes": None,
        "filesystem_total_bytes": None,
        "reserve_free_bytes": None,
    }


def parse_receiver_metrics(value, sampled_unix_secs: int, config: StatusConfig):
    source = _record(value, "shred-reader metrics")

    def counter(key: str, *, maximum: int = MAX_SAFE_INTEGER):
        return _integer(
            _required(source, key, "shred-reader metrics"),
            f"shred-reader metrics.{key}",
            maximum=maximum,
        )

    uptime = counter("uptime_seconds")
    known_peers = counter("gossip_peers")
    recent_peers = counter("recent_gossip_peers")
    tvu_peers = counter("tvu_peers")
    shred_version = counter("shred_version", maximum=(1 << 16) - 1)
    packets = counter("packets_total")
    received_bytes = counter("bytes_total")
    parsed = counter("parsed_total")
    invalid = counter("invalid_total")
    mismatched = counter("version_mismatch_total")
    unique = counter("unique_total")
    duplicates = counter("duplicates_total")
    data = counter("data_total")
    code = counter("code_total")
    targets = counter("forward_targets", maximum=32)
    forwarded = counter("forwarded_datagrams_total")
    forward_errors = counter("forward_errors_total")
    latest_slot_raw = counter("latest_slot")
    seconds_since_packet = _integer(
        _required(source, "seconds_since_last_packet", "shred-reader metrics"),
        "shred-reader metrics.seconds_since_last_packet",
        nullable=True,
    )

    if recent_peers > known_peers:
        raise ValueError("recent gossip peer count exceeds known peer count")
    if targets == 0 and (forwarded != 0 or forward_errors != 0):
        raise ValueError("forwarding counters exist without a forwarding target")
    attempts = forwarded + forward_errors
    if attempts > MAX_SAFE_INTEGER:
        raise ValueError("forwarding attempt count exceeds the public integer limit")

    gossip_state = "observed" if recent_peers > 0 else "waiting"
    if packets == 0 or seconds_since_packet is None:
        tvu_state = "waiting"
    elif seconds_since_packet <= config.tvu_active_after_secs:
        tvu_state = "receiving"
    else:
        tvu_state = "idle"

    if targets == 0:
        forwarding_state = "disabled"
    elif forwarded > 0:
        forwarding_state = "sending"
    elif forward_errors > 0:
        # Cumulative errors cannot prove a current fault. Use this state only
        # when the process has not recorded a single successful send.
        forwarding_state = "errors"
    else:
        forwarding_state = "waiting"

    return {
        "gossip": {
            "state": gossip_state,
            "recent_peer_count": recent_peers,
            "known_peer_count": known_peers,
            "tvu_peer_count": tvu_peers,
            "shred_version": shred_version,
            "receiver_uptime_secs": uptime,
            "updated_unix_secs": sampled_unix_secs,
        },
        "tvu": {
            "state": tvu_state,
            "packets_total": packets,
            "bytes_total": received_bytes,
            "parsed_total": parsed,
            "invalid_total": invalid,
            "version_mismatch_total": mismatched,
            "unique_total": unique,
            "duplicates_total": duplicates,
            "data_total": data,
            "code_total": code,
            "latest_slot": latest_slot_raw if parsed > 0 else None,
            "seconds_since_last_packet": seconds_since_packet,
            "updated_unix_secs": sampled_unix_secs,
        },
        "forwarding": {
            "state": forwarding_state,
            "target_count": targets,
            "attempts_total": attempts,
            "successful_datagrams_total": forwarded,
            "errors_total": forward_errors,
            "updated_unix_secs": sampled_unix_secs,
        },
    }


def unavailable_receiver_status():
    return {
        "gossip": {
            "state": "unavailable",
            "recent_peer_count": None,
            "known_peer_count": None,
            "tvu_peer_count": None,
            "shred_version": None,
            "receiver_uptime_secs": None,
            "updated_unix_secs": None,
        },
        "tvu": {
            "state": "unavailable",
            "packets_total": None,
            "bytes_total": None,
            "parsed_total": None,
            "invalid_total": None,
            "version_mismatch_total": None,
            "unique_total": None,
            "duplicates_total": None,
            "data_total": None,
            "code_total": None,
            "latest_slot": None,
            "seconds_since_last_packet": None,
            "updated_unix_secs": None,
        },
        "forwarding": {
            "state": "unavailable",
            "target_count": None,
            "attempts_total": None,
            "successful_datagrams_total": None,
            "errors_total": None,
            "updated_unix_secs": None,
        },
    }


def fetch_receiver_metrics(config: StatusConfig):
    endpoint = config.receiver_endpoint
    try:
        address = ipaddress.ip_address(endpoint.host)
    except ValueError as error:
        raise ValueError("receiver metrics host is not an IP literal") from error
    if (
        not address.is_loopback
        or endpoint.path != "/metrics"
        or not 1 <= endpoint.port <= 65535
    ):
        raise ValueError("receiver metrics endpoint is not an exact loopback /metrics URL")
    connection = HTTPConnection(
        endpoint.host,
        endpoint.port,
        timeout=config.receiver_timeout_secs,
    )
    try:
        connection.request(
            "GET",
            endpoint.path,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "Connection": "close",
            },
        )
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError("shred-reader metrics returned a non-success status")
        content_type = response.getheader("Content-Type", "")
        if content_type.partition(";")[0].strip().lower() != "application/json":
            raise ValueError("shred-reader metrics returned a non-JSON response")
        content_length = response.getheader("Content-Length")
        if content_length is not None:
            try:
                declared = int(content_length)
            except ValueError as error:
                raise ValueError("shred-reader metrics has an invalid content length") from error
            if declared < 0 or declared > MAX_SOURCE_JSON_BYTES:
                raise ValueError("shred-reader metrics exceeds its byte limit")
        raw = response.read(MAX_SOURCE_JSON_BYTES + 1)
        if len(raw) > MAX_SOURCE_JSON_BYTES:
            raise ValueError("shred-reader metrics exceeds its byte limit")
        return _decode_json(raw)
    finally:
        connection.close()


def read_hivezilla_status(config: StatusConfig):
    return _decode_json(read_bounded_regular(config.hivezilla_status_file))


def build_status(
    config: StatusConfig,
    *,
    now_unix_secs: int | None = None,
    receiver_loader=None,
    hivezilla_loader=None,
):
    generated = int(time.time()) if now_unix_secs is None else now_unix_secs
    if generated <= 0 or generated > MAX_SAFE_INTEGER:
        raise ValueError("generated timestamp is outside the public integer range")

    receiver_loader = receiver_loader or (lambda: fetch_receiver_metrics(config))
    hivezilla_loader = hivezilla_loader or (lambda: read_hivezilla_status(config))

    try:
        receiver = parse_receiver_metrics(receiver_loader(), generated, config)
    except SOURCE_ERRORS:
        receiver = unavailable_receiver_status()

    try:
        hivezilla = parse_hivezilla_status(hivezilla_loader(), generated, config)
    except SOURCE_ERRORS:
        hivezilla = unavailable_hivezilla_status()

    return {
        "schema_version": 1,
        "updated_unix_secs": generated,
        "gossip": receiver["gossip"],
        "tvu": receiver["tvu"],
        "forwarding": receiver["forwarding"],
        "hivezilla": hivezilla,
    }


def encode_public_status(value) -> bytes:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    if not encoded or len(encoded) > MAX_PUBLIC_JSON_BYTES:
        raise ValueError("public shred status exceeds its byte limit")
    return encoded


def write_atomic_public(path: Path, payload: bytes) -> None:
    """Atomically replace a public snapshot inside one already-existing directory."""

    if not payload or len(payload) > MAX_PUBLIC_JSON_BYTES:
        raise ValueError("public shred status has an invalid size")
    file_name = path.name
    if not file_name or file_name in {".", ".."}:
        raise ValueError("public snapshot path has no safe file name")

    directory_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    directory_flags |= getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory = os.open(path.parent, directory_flags)
    temporary_name = f".{file_name}.{os.getpid()}-{time.monotonic_ns()}.tmp"
    try:
        directory_info = os.fstat(directory)
        if not stat.S_ISDIR(directory_info.st_mode):
            raise ValueError("public snapshot parent is not a directory")
        try:
            destination_info = os.stat(file_name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            destination_info = None
        if destination_info is not None and not stat.S_ISREG(destination_info.st_mode):
            raise ValueError("public snapshot destination is not a regular file")

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(temporary_name, flags, 0o644, dir_fd=directory)
        try:
            os.fchmod(descriptor, 0o644)
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            try:
                os.close(descriptor)
            except OSError:
                pass
            raise
        os.replace(
            temporary_name,
            file_name,
            src_dir_fd=directory,
            dst_dir_fd=directory,
        )
        os.fsync(directory)
    finally:
        try:
            os.unlink(temporary_name, dir_fd=directory)
        except FileNotFoundError:
            pass
        os.close(directory)


def validate_distinct_files(source: Path, output: Path | None) -> None:
    if output is None:
        return
    if source == output:
        raise ValueError("public output file must differ from the Hivezilla input file")
    try:
        if os.path.samefile(source, output):
            raise ValueError("public output file aliases the Hivezilla input file")
    except FileNotFoundError:
        pass


class StatusCache:
    def __init__(self, config: StatusConfig):
        self.config = config
        self._lock = threading.Lock()
        self._encoded: bytes | None = None
        self._health: bytes | None = None
        self._refresh_failed = True
        self._stopped = threading.Event()

    def refresh(self) -> None:
        try:
            status = build_status(self.config)
            encoded = encode_public_status(status)
            health = encode_public_status(
                {
                    "ok": True,
                    "updated_unix_secs": status["updated_unix_secs"],
                    "receiver": (
                        "unavailable"
                        if status["gossip"]["state"] == "unavailable"
                        else "available"
                    ),
                    "hivezilla": status["hivezilla"]["availability"],
                }
            )
            if self.config.output_file is not None:
                write_atomic_public(self.config.output_file, encoded)
        except Exception:
            # Never log exception text: paths, URLs, or source diagnostics may be embedded in it.
            with self._lock:
                self._refresh_failed = True
            raise
        with self._lock:
            self._encoded = encoded
            self._health = health
            self._refresh_failed = False

    def status(self) -> bytes | None:
        with self._lock:
            return self._encoded

    def health(self) -> tuple[bool, bytes]:
        with self._lock:
            healthy = self._encoded is not None and not self._refresh_failed
            if healthy and self._health is not None:
                return True, self._health
        return False, b'{"ok":false}'

    def run(self) -> None:
        while not self._stopped.wait(self.config.interval_secs):
            try:
                self.refresh()
            except Exception as error:
                print(
                    f"shred_status refresh_failed type={type(error).__name__}",
                    file=sys.stderr,
                    flush=True,
                )

    def stop(self) -> None:
        self._stopped.set()


class ShredStatusHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "shred-status"
    sys_version = ""
    cache: StatusCache | None = None
    cors_origin: str | None = None

    def do_GET(self):
        self._serve(send_body=True)

    def do_HEAD(self):
        self._serve(send_body=False)

    def do_OPTIONS(self):
        if not self._request_target_is_safe(send_body=True):
            return
        if not self._origin_is_allowed():
            self._json_response(403, b'{"error":"origin not allowed"}', True)
            return
        if urlsplit(self.path).path not in {HEALTH_PATH, STATUS_PATH}:
            self._json_response(404, b'{"error":"not found"}', True)
            return
        requested_method = self.headers.get("Access-Control-Request-Method")
        if requested_method is not None and requested_method not in {"GET", "HEAD"}:
            self._json_response(405, b'{"error":"method not allowed"}', True)
            return
        requested_headers = self.headers.get("Access-Control-Request-Headers", "")
        headers = {
            item.strip().casefold()
            for item in requested_headers.split(",")
            if item.strip()
        }
        if not headers.issubset({"accept", "cache-control"}):
            self._json_response(403, b'{"error":"request header not allowed"}', True)
            return
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.send_header("Allow", "GET, HEAD, OPTIONS")
        self._security_headers()
        self._cors_headers(preflight=True)
        self.end_headers()

    def do_POST(self):
        self._method_not_allowed()

    do_PUT = do_POST
    do_PATCH = do_POST
    do_DELETE = do_POST
    do_CONNECT = do_POST
    do_TRACE = do_POST
    do_COPY = do_POST
    do_LOCK = do_POST
    do_MKCOL = do_POST
    do_MOVE = do_POST
    do_PROPFIND = do_POST
    do_UNLOCK = do_POST

    def _method_not_allowed(self):
        if not self._origin_is_allowed():
            self._json_response(403, b'{"error":"origin not allowed"}', True)
            return
        self._json_response(
            405,
            b'{"error":"read-only status service"}',
            True,
            extra_headers=(("Allow", "GET, HEAD, OPTIONS"),),
        )

    def _serve(self, send_body: bool):
        if not self._request_target_is_safe(send_body=send_body):
            return
        if not self._origin_is_allowed():
            self._json_response(403, b'{"error":"origin not allowed"}', send_body)
            return
        path = urlsplit(self.path).path
        if path == HEALTH_PATH:
            if self.cache is None:
                self._json_response(503, b'{"ok":false}', send_body)
                return
            healthy, body = self.cache.health()
            self._json_response(200 if healthy else 503, body, send_body)
            return
        if path == STATUS_PATH:
            body = None if self.cache is None else self.cache.status()
            if body is None:
                self._json_response(503, b'{"error":"status unavailable"}', send_body)
                return
            self._json_response(200, body, send_body)
            return
        self._json_response(404, b'{"error":"not found"}', send_body)

    def _request_target_is_safe(self, *, send_body: bool) -> bool:
        target = urlsplit(self.path)
        if (
            target.scheme
            or target.netloc
            or target.query
            or target.fragment
            or not target.path.startswith("/")
        ):
            self._json_response(400, b'{"error":"invalid request target"}', send_body)
            return False
        return True

    def _origin_is_allowed(self) -> bool:
        origins = self.headers.get_all("Origin", failobj=[]) or []
        if not origins:
            return True
        return len(origins) == 1 and self.cors_origin is not None and origins[0] == self.cors_origin

    def _cors_headers(self, *, preflight: bool = False):
        origins = self.headers.get_all("Origin", failobj=[]) or []
        if (
            self.cors_origin is not None
            and len(origins) == 1
            and origins[0] == self.cors_origin
        ):
            self.send_header("Access-Control-Allow-Origin", self.cors_origin)
            self.send_header("Vary", "Origin")
            if preflight:
                self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Accept, Cache-Control")
                self.send_header("Access-Control-Max-Age", "600")

    def _security_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")

    def _json_response(
        self,
        status_code: int,
        body: bytes,
        send_body: bool,
        *,
        extra_headers=(),
    ):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for key, value in extra_headers:
            self.send_header(key, value)
        self._security_headers()
        self._cors_headers()
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def log_message(self, _format, *_args):
        return


class ShredStatusServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 32

    def __init__(
        self,
        server_address,
        handler_class,
        *,
        max_requests: int = MAX_HTTP_REQUESTS,
        client_timeout_secs: float = REQUEST_HEADER_TIMEOUT_SECS,
    ):
        if max_requests <= 0 or client_timeout_secs <= 0:
            raise ValueError("HTTP limits must be positive")
        self._request_slots = threading.BoundedSemaphore(max_requests)
        self._client_timeout_secs = client_timeout_secs
        super().__init__(server_address, handler_class)

    def get_request(self):
        request, client_address = super().get_request()
        request.settimeout(self._client_timeout_secs)
        return request, client_address

    def process_request(self, request, client_address):
        if not self._request_slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._request_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


def parse_listener(value: str):
    parsed = urlsplit("//" + value)
    try:
        port = parsed.port
    except ValueError as error:
        raise argparse.ArgumentTypeError("listen address has an invalid port") from error
    if (
        parsed.username is not None
        or parsed.password is not None
        or parsed.hostname is None
        or port is None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise argparse.ArgumentTypeError("listen address must be an explicit IPv4 host and port")
    if parsed.hostname == "localhost":
        host = "127.0.0.1"
    else:
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError as error:
            raise argparse.ArgumentTypeError("listen host must be an IPv4 literal") from error
        if not isinstance(address, ipaddress.IPv4Address):
            raise argparse.ArgumentTypeError("listen host must be IPv4")
        if not (address.is_loopback or address.is_private or address.is_unspecified):
            raise argparse.ArgumentTypeError(
                "listen host must be loopback, private, or the bridge wildcard"
            )
        host = str(address)
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("listen port must be between 1 and 65535")
    return host, port


def parse_receiver_endpoint(value: str):
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as error:
        raise argparse.ArgumentTypeError("receiver metrics URL has an invalid port") from error
    if (
        parsed.scheme != "http"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.hostname is None
        or port is None
        or parsed.path != "/metrics"
        or parsed.query
        or parsed.fragment
    ):
        raise argparse.ArgumentTypeError(
            "receiver metrics URL must be an explicit loopback http://IP:port/metrics URL"
        )
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError as error:
        raise argparse.ArgumentTypeError("receiver metrics host must be a loopback IP literal") from error
    if not address.is_loopback:
        raise argparse.ArgumentTypeError("receiver metrics host must be loopback")
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("receiver metrics port must be between 1 and 65535")
    return ReceiverEndpoint(str(address), port)


def parse_cors_origin(value: str):
    if value == "*":
        raise argparse.ArgumentTypeError("CORS origin cannot be a wildcard")
    parsed: SplitResult = urlsplit(value)
    try:
        parsed_port = parsed.port
    except ValueError as error:
        raise argparse.ArgumentTypeError("CORS origin has an invalid port") from error
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or any(character in value for character in "\r\n")
    ):
        raise argparse.ArgumentTypeError("CORS origin must be one exact HTTP(S) origin")
    if parsed_port is not None and not 1 <= parsed_port <= 65535:
        raise argparse.ArgumentTypeError("CORS origin port must be between 1 and 65535")
    return value


def absolute_path(value: str):
    path = Path(value)
    if not path.is_absolute() or path == Path("/"):
        raise argparse.ArgumentTypeError("path must be absolute and non-root")
    return path


def positive_integer(value: str):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def positive_float(value: str):
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be positive") from error
    if not 0 < parsed <= 60:
        raise argparse.ArgumentTypeError("value must be positive and at most 60 seconds")
    return parsed


def _environment(name: str, fallback=None):
    value = os.environ.get(name)
    return fallback if value is None or value == "" else value


def argument_parser():
    hivezilla_default = _environment("SHRED_STATUS_HIVEZILLA_FILE")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--listen",
        type=parse_listener,
        default=_environment("SHRED_STATUS_LISTEN", "127.0.0.1:8790"),
        help="HTTP listen address; loopback, private IPv4, and 0.0.0.0 are accepted",
    )
    parser.add_argument(
        "--hivezilla-status-file",
        type=absolute_path,
        default=hivezilla_default,
        required=hivezilla_default is None,
        help="absolute path to Hivezilla's atomic private status JSON",
    )
    parser.add_argument(
        "--receiver-metrics-url",
        type=parse_receiver_endpoint,
        default=_environment(
            "SHRED_STATUS_RECEIVER_METRICS_URL",
            "http://127.0.0.1:19090/metrics",
        ),
        help="exact loopback shred-reader /metrics URL",
    )
    parser.add_argument(
        "--output-file",
        type=absolute_path,
        default=_environment("SHRED_STATUS_OUTPUT_FILE"),
        help="optional absolute path for an atomic sanitized public JSON snapshot",
    )
    parser.add_argument(
        "--cors-origin",
        type=parse_cors_origin,
        default=_environment("SHRED_STATUS_CORS_ORIGIN"),
        help="optional exact browser Origin allowed to read the HTTP endpoints",
    )
    parser.add_argument(
        "--interval-secs",
        type=positive_integer,
        default=_environment("SHRED_STATUS_INTERVAL_SECS", "5"),
        help="source refresh and public snapshot interval",
    )
    parser.add_argument(
        "--receiver-timeout-secs",
        type=positive_float,
        default=_environment("SHRED_STATUS_RECEIVER_TIMEOUT_SECS", "2"),
        help="loopback metrics request timeout",
    )
    parser.add_argument(
        "--hivezilla-stale-after-secs",
        type=positive_integer,
        default=_environment("SHRED_STATUS_HIVEZILLA_STALE_AFTER_SECS", "20"),
        help="maximum age of a fresh Hivezilla heartbeat",
    )
    parser.add_argument(
        "--tvu-active-after-secs",
        type=positive_integer,
        default=_environment("SHRED_STATUS_TVU_ACTIVE_AFTER_SECS", "30"),
        help="packet age still labeled as actively receiving",
    )
    return parser


def main(argv=None):
    parser = argument_parser()
    args = parser.parse_args(argv)
    config = StatusConfig(
        hivezilla_status_file=args.hivezilla_status_file,
        receiver_endpoint=args.receiver_metrics_url,
        output_file=args.output_file,
        cors_origin=args.cors_origin,
        interval_secs=args.interval_secs,
        receiver_timeout_secs=args.receiver_timeout_secs,
        hivezilla_stale_after_secs=args.hivezilla_stale_after_secs,
        tvu_active_after_secs=args.tvu_active_after_secs,
    )
    try:
        validate_distinct_files(config.hivezilla_status_file, config.output_file)
        cache = StatusCache(config)
        cache.refresh()
    except (OSError, ValueError) as error:
        parser.error(f"invalid status configuration or output: {type(error).__name__}")

    worker = threading.Thread(target=cache.run, name="shred-status-refresh", daemon=True)
    worker.start()
    handler = type(
        "ConfiguredShredStatusHandler",
        (ShredStatusHandler,),
        {"cache": cache, "cors_origin": config.cors_origin},
    )
    server = ShredStatusServer(args.listen, handler)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        cache.stop()
        worker.join(timeout=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
