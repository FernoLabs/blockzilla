#!/usr/bin/env python3
"""Serve the watcher through a loopback proxy with public-safe telemetry."""

import argparse
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import re
import sys
import threading
from urllib.parse import urlsplit


MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_INGEST_JSON_BYTES = 1024 * 1024
MAX_SSE_LINE_BYTES = 4 * 1024 * 1024
MAX_PUBLIC_GAPS = 32
MAX_PUBLIC_INCIDENTS = 32
MAX_SAFE_INTEGER = (1 << 53) - 1
COPY_REQUEST_HEADERS = {
    "accept",
    "cache-control",
    "if-modified-since",
    "if-none-match",
    "range",
    "user-agent",
}
HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
SAFE_RESPONSE_HEADERS = {
    "accept-ranges",
    "cache-control",
    "content-encoding",
    "content-language",
    "content-length",
    "content-range",
    "etag",
    "expires",
    "last-modified",
    "vary",
}
PATH_KEY_SUFFIXES = ("_dir", "_path", "_root")
SENSITIVE_PUBLIC_KEYS = {
    "access_key",
    "access_token",
    "api_key",
    "authorization",
    "client_secret",
    "credential",
    "credentials",
    "endpoint",
    "password",
    "passwd",
    "private_key",
    "proxy_authorization",
    "refresh_token",
    "secret",
    "session_key",
    "token",
}
SENSITIVE_PUBLIC_KEY_SUFFIXES = (
    "_access_key",
    "_access_token",
    "_api_key",
    "_authorization",
    "_client_secret",
    "_credential",
    "_credentials",
    "_password",
    "_private_key",
    "_refresh_token",
    "_secret",
    "_session_key",
    "_token",
)
AUTHORIZATION_TOKEN = re.compile(
    r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+"
)
CREDENTIALED_URL = re.compile(
    r"(?i)\b([a-z][a-z0-9+.-]*://)[^/\s?#]+@"
)
QUERY_CREDENTIAL = re.compile(
    r"(?i)([?&](?:access[_-]?key|access[_-]?token|api[_-]?key|"
    r"client[_-]?secret|credential|password|private[_-]?key|"
    r"refresh[_-]?token|secret|token)=)[^&#\s]*"
)
INLINE_CREDENTIAL = re.compile(
    r"(?i)\b(access[_-]?key|access[_-]?token|api[_-]?key|"
    r"client[_-]?secret|credential|password|private[_-]?key|"
    r"refresh[_-]?token|secret|token)\s*[:=]\s*[^\s,;&#]+"
)
SAFE_SSE_EVENTS = {b"resync", b"snapshot", b"snapshot_patch"}
PRIVATE_LISTENER_NETWORKS = tuple(
    ipaddress.IPv4Network(cidr) for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
PUBLIC_API_GETS = {
    "/api/v1/events",
    "/api/v1/sidecars/block-time-gaps/status.json",
    "/api/v1/sidecars/ingest-pipeline/status.json",
    "/api/v1/sidecars/runtime-operations/status.json",
    "/api/v1/status",
}
OVERALL_STATES = {"healthy", "degraded", "failed", "unknown"}
UPSTREAM_STATES = {"connected", "reconnecting", "stalled", "unavailable"}
RECORDER_STATES = {"recording", "paused", "stalled", "unavailable"}
REPLICATION_STATES = {"caught_up", "syncing", "stalled", "unavailable"}
INDEXER_STATES = {"indexing", "caught_up", "stalled", "unavailable"}
OBJECT_STORE_STATES = {"standby", "uploading", "degraded", "unavailable"}
FALLBACK_STATES = {"capturing", "standby", "stalled", "unavailable"}
GAP_COVERAGE = {"raw", "normalized", "rpc_recoverable", "unproven"}
INCIDENT_SEVERITIES = {"warning", "error", "critical"}
PUBLIC_INCIDENT_IDS = {
    "grpc_stale",
    "upstream_access_blocked",
    "replay_gap",
    "resume_coverage",
    "replay_recovery_failed",
    "disk_space",
    "disk_check_failed",
    "volume_invalid",
    "cache_rotation_failed",
    "generation_rotation_failed",
    "generation_backlog",
    "object_store",
    "receiver_ack_stale",
}
ABSOLUTE_PRIVATE_PATH = re.compile(
    r"(?<![A-Za-z0-9:])/(?:Applications|Users|bin|boot|data|dev|etc|home|lib|lib64|media|mnt|opt|proc|root|run|sbin|srv|storage|sys|tmp|usr|var|volume[0-9]*)"
    r"(?:/[^\s,;:()\[\]{}]+)*"
)


def safe_basename(value):
    if not isinstance(value, str):
        return value
    return value.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]


def is_sensitive_public_key(value):
    if not isinstance(value, str):
        return False
    separated = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", value)
    normalized = re.sub(r"[^a-z0-9]+", "_", separated.casefold()).strip("_")
    compact = normalized.replace("_", "")
    if normalized in SENSITIVE_PUBLIC_KEYS or compact in {
        "accesskey",
        "accesstoken",
        "apikey",
        "authorization",
        "clientsecret",
        "credential",
        "credentials",
        "endpoint",
        "password",
        "passwd",
        "privatekey",
        "proxyauthorization",
        "refreshtoken",
        "secret",
        "sessionkey",
        "token",
    }:
        return True
    if set(normalized.split("_")).intersection({
        "authorization",
        "credential",
        "credentials",
        "password",
        "passwd",
        "secret",
        "token",
    }):
        return True
    return normalized.endswith(SENSITIVE_PUBLIC_KEY_SUFFIXES)


def sanitize_public_string(value):
    public = ABSOLUTE_PRIVATE_PATH.sub("<redacted-path>", value)
    public = AUTHORIZATION_TOKEN.sub(
        lambda match: "{} <redacted>".format(match.group(1)), public
    )
    public = CREDENTIALED_URL.sub(r"\1<redacted>@", public)
    public = QUERY_CREDENTIAL.sub(r"\1<redacted>", public)
    public = INLINE_CREDENTIAL.sub(
        lambda match: "{}=<redacted>".format(match.group(1)), public
    )
    return public


def sanitize_public_value(value, key=None):
    if isinstance(value, dict):
        return {
            child_key: sanitize_public_value(child_value, child_key)
            for child_key, child_value in value.items()
            if not is_sensitive_public_key(child_key)
        }
    if isinstance(value, list):
        return [sanitize_public_value(item) for item in value]
    if isinstance(value, str):
        if key == "path" or (isinstance(key, str) and key.endswith(PATH_KEY_SUFFIXES)):
            return sanitize_public_string(safe_basename(value))
        return sanitize_public_string(value)
    return value


def public_json_bytes(raw):
    if len(raw) > MAX_JSON_BYTES:
        raise ValueError("upstream JSON exceeds the public proxy limit")
    value = json.loads(raw)
    public = sanitize_public_value(value)
    encoded = json.dumps(public, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if ABSOLUTE_PRIVATE_PATH.search(encoded.decode("utf-8")):
        raise ValueError("public JSON still contains an absolute private path")
    return encoded


def public_ingest_json_bytes(raw):
    if len(raw) > MAX_INGEST_JSON_BYTES:
        raise ValueError("ingest status exceeds the public proxy limit")
    value = json.loads(raw, object_pairs_hook=_object_without_duplicate_keys)
    selected = _public_ingest_status(value)
    return json.dumps(selected, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _object_without_duplicate_keys(pairs):
    value = {}
    for key, child in pairs:
        if key in value:
            raise ValueError("ingest status contains a duplicate object key")
        value[key] = child
    return value


def _required_object(value, name):
    if not isinstance(value, dict):
        raise ValueError("{} must be an object".format(name))
    return value


def _integer(value, name, *, positive=False, nullable=False):
    if nullable and value is None:
        return None
    if type(value) is not int or value < (1 if positive else 0) or value > MAX_SAFE_INTEGER:
        qualifier = "a positive" if positive else "a non-negative"
        raise ValueError("{} must be {} safe integer".format(name, qualifier))
    return value


def _enum(value, allowed, name):
    if type(value) is not str or value not in allowed:
        raise ValueError("{} has an unsupported value".format(name))
    return value


def _stage(value, name, fields):
    source = _required_object(value, name)
    selected = {}
    for field, validator in fields.items():
        if field not in source:
            raise ValueError("{}.{} is required".format(name, field))
        selected[field] = validator(source[field])
    return selected


def _public_ingest_status(value):
    root = _required_object(value, "ingest status")
    required = {
        "schema_version",
        "updated_unix_secs",
        "overall_state",
        "upstream",
        "recorder",
        "replication",
        "indexer",
        "object_store",
        "fallback",
        "gaps",
        "gaps_truncated",
        "incidents",
    }
    missing = required.difference(root)
    if missing:
        raise ValueError("ingest status is missing required fields")
    if type(root["schema_version"]) is not int or root["schema_version"] != 1:
        raise ValueError("unsupported ingest status schema")

    selected = {
        "schema_version": 1,
        "updated_unix_secs": _integer(
            root["updated_unix_secs"], "updated_unix_secs", positive=True
        ),
        "overall_state": _enum(root["overall_state"], OVERALL_STATES, "overall_state"),
        "upstream": _stage(root["upstream"], "upstream", {
            "state": lambda child: _enum(child, UPSTREAM_STATES, "upstream.state"),
            "updated_unix_secs": lambda child: _integer(
                child, "upstream.updated_unix_secs", positive=True, nullable=True
            ),
            "reconnects_1h": lambda child: _integer(
                child, "upstream.reconnects_1h", nullable=True
            ),
        }),
        "recorder": _stage(root["recorder"], "recorder", {
            "state": lambda child: _enum(child, RECORDER_STATES, "recorder.state"),
            "durable_slot": lambda child: _integer(child, "recorder.durable_slot", nullable=True),
            "updated_unix_secs": lambda child: _integer(
                child, "recorder.updated_unix_secs", positive=True, nullable=True
            ),
            "active_bytes": lambda child: _integer(child, "recorder.active_bytes"),
            "sealed_generations": lambda child: _integer(child, "recorder.sealed_generations"),
            "unacknowledged_bytes": lambda child: _integer(child, "recorder.unacknowledged_bytes"),
            "disk_free_bytes": lambda child: _integer(child, "recorder.disk_free_bytes", nullable=True),
            "disk_total_bytes": lambda child: _integer(child, "recorder.disk_total_bytes", nullable=True),
        }),
        "replication": _stage(root["replication"], "replication", {
            "state": lambda child: _enum(child, REPLICATION_STATES, "replication.state"),
            "ack_through_sequence": lambda child: _integer(
                child, "replication.ack_through_sequence", nullable=True
            ),
            "ack_slot": lambda child: _integer(child, "replication.ack_slot", nullable=True),
            "updated_unix_secs": lambda child: _integer(
                child, "replication.updated_unix_secs", positive=True, nullable=True
            ),
            "lag_records": lambda child: _integer(child, "replication.lag_records", nullable=True),
        }),
        "indexer": _stage(root["indexer"], "indexer", {
            "state": lambda child: _enum(child, INDEXER_STATES, "indexer.state"),
            "last_slot": lambda child: _integer(child, "indexer.last_slot", nullable=True),
            "updated_unix_secs": lambda child: _integer(
                child, "indexer.updated_unix_secs", positive=True, nullable=True
            ),
            "lag_slots": lambda child: _integer(child, "indexer.lag_slots", nullable=True),
        }),
        "object_store": _stage(root["object_store"], "object_store", {
            "state": lambda child: _enum(child, OBJECT_STORE_STATES, "object_store.state"),
            "provider": lambda child: _enum(child, {"r2"}, "object_store.provider"),
            "committed_bytes": lambda child: _integer(
                child, "object_store.committed_bytes", nullable=True
            ),
            "pending_bytes": lambda child: _integer(child, "object_store.pending_bytes"),
            "updated_unix_secs": lambda child: _integer(
                child, "object_store.updated_unix_secs", positive=True, nullable=True
            ),
        }),
        "fallback": _stage(root["fallback"], "fallback", {
            "state": lambda child: _enum(child, FALLBACK_STATES, "fallback.state"),
            "last_slot": lambda child: _integer(child, "fallback.last_slot", nullable=True),
            "updated_unix_secs": lambda child: _integer(
                child, "fallback.updated_unix_secs", positive=True, nullable=True
            ),
            "lag_slots": lambda child: _integer(child, "fallback.lag_slots", nullable=True),
        }),
    }

    free = selected["recorder"]["disk_free_bytes"]
    total = selected["recorder"]["disk_total_bytes"]
    if free is not None and total is not None and free > total:
        raise ValueError("recorder.disk_free_bytes exceeds total capacity")
    selected["gaps"] = _public_gaps(root["gaps"])
    if type(root["gaps_truncated"]) is not bool:
        raise ValueError("gaps_truncated must be boolean")
    selected["gaps_truncated"] = root["gaps_truncated"]
    selected["incidents"] = _public_incidents(root["incidents"])
    _validate_ingest_consistency(selected)
    return selected


def _validate_ingest_consistency(status):
    updated = status["updated_unix_secs"]
    future_limit = updated + 5
    if status["gaps_truncated"] and len(status["gaps"]) != MAX_PUBLIC_GAPS:
        raise ValueError("truncated gap feed must fill the public gap window")
    stage_times = (
        status["upstream"]["updated_unix_secs"],
        status["recorder"]["updated_unix_secs"],
        status["replication"]["updated_unix_secs"],
        status["indexer"]["updated_unix_secs"],
        status["object_store"]["updated_unix_secs"],
        status["fallback"]["updated_unix_secs"],
    )
    if any(timestamp is not None and timestamp > future_limit for timestamp in stage_times):
        raise ValueError("stage timestamp is ahead of the status sample")

    upstream = status["upstream"]
    if (upstream["state"] == "unavailable") != (upstream["updated_unix_secs"] is None):
        raise ValueError("upstream state contradicts its timestamp")

    recorder = status["recorder"]
    recorder_evidence = (
        recorder["durable_slot"],
        recorder["updated_unix_secs"],
        recorder["disk_free_bytes"],
        recorder["disk_total_bytes"],
    )
    if recorder["state"] == "unavailable":
        if any(item is not None for item in recorder_evidence):
            raise ValueError("unavailable recorder includes live evidence")
    elif any(item is None for item in recorder_evidence):
        raise ValueError("available recorder is missing live evidence")

    replication = status["replication"]
    replication_evidence = (
        replication["ack_through_sequence"],
        replication["ack_slot"],
        replication["updated_unix_secs"],
        replication["lag_records"],
    )
    if replication["state"] == "unavailable":
        if any(item is not None for item in replication_evidence):
            raise ValueError("unavailable replication includes live evidence")
    else:
        if (
            replication["ack_through_sequence"] is None
            or replication["updated_unix_secs"] is None
            or replication["lag_records"] is None
        ):
            raise ValueError("available replication is missing live evidence")
        if (replication["lag_records"] == 0) == (replication["ack_slot"] is None):
            raise ValueError("replication ACK slot contradicts record lag")
        if replication["state"] == "caught_up" and replication["lag_records"] != 0:
            raise ValueError("caught-up replication has record lag")
        if replication["state"] == "syncing" and replication["lag_records"] == 0:
            raise ValueError("syncing replication has no record lag")

    indexer = status["indexer"]
    indexer_evidence = (
        indexer["last_slot"], indexer["updated_unix_secs"], indexer["lag_slots"]
    )
    if indexer["state"] == "unavailable":
        if any(item is not None for item in indexer_evidence):
            raise ValueError("unavailable indexer includes live evidence")
    elif any(item is None for item in indexer_evidence):
        raise ValueError("available indexer is missing live evidence")
    if indexer["state"] == "caught_up" and indexer["lag_slots"] != 0:
        raise ValueError("caught-up indexer has slot lag")

    object_store = status["object_store"]
    if object_store["state"] == "unavailable":
        if not (
            object_store["committed_bytes"] is None
            and object_store["pending_bytes"] == 0
            and object_store["updated_unix_secs"] is None
        ):
            raise ValueError("unavailable object store includes live evidence")
    else:
        if object_store["updated_unix_secs"] is None:
            raise ValueError("available object store is missing a timestamp")
        if object_store["state"] == "standby" and object_store["pending_bytes"] != 0:
            raise ValueError("standby object store has pending uploads")
        if object_store["state"] == "uploading" and object_store["pending_bytes"] == 0:
            raise ValueError("uploading object store has no pending uploads")

    fallback = status["fallback"]
    fallback_evidence = (
        fallback["last_slot"], fallback["updated_unix_secs"], fallback["lag_slots"]
    )
    if fallback["state"] == "unavailable":
        if any(item is not None for item in fallback_evidence):
            raise ValueError("unavailable fallback includes live evidence")
    elif fallback["state"] == "capturing" and any(
        item is None for item in fallback_evidence
    ):
        raise ValueError("capturing fallback is missing live evidence")
    elif any(item is None for item in fallback_evidence) and any(
        item is not None for item in fallback_evidence
    ):
        raise ValueError("fallback evidence is incomplete")

    if any(
        incident["started_unix_secs"] > future_limit
        or (
            incident["resolved_unix_secs"] is not None
            and incident["resolved_unix_secs"] > future_limit
        )
        for incident in status["incidents"]
    ):
        raise ValueError("incident timestamp is ahead of the status sample")
    active = [
        incident for incident in status["incidents"]
        if incident["resolved_unix_secs"] is None
    ]
    has_failure = any(
        incident["severity"] in {"error", "critical"} for incident in active
    ) or any(gap["coverage"] == "unproven" for gap in status["gaps"])
    has_warning = any(
        incident["severity"] == "warning" for incident in active
    ) or any(gap["coverage"] != "raw" for gap in status["gaps"])
    if has_failure and status["overall_state"] != "failed":
        raise ValueError("overall state hides a failed continuity signal")
    if has_warning and status["overall_state"] == "healthy":
        raise ValueError("overall state hides a degraded continuity signal")


def _public_gaps(value):
    if not isinstance(value, list) or len(value) > MAX_PUBLIC_GAPS:
        raise ValueError("gaps must be a bounded array")
    selected = []
    ranges = set()
    for index, child in enumerate(value):
        name = "gaps[{}]".format(index)
        gap = _stage(child, name, {
            "from_slot": lambda item: _integer(item, name + ".from_slot"),
            "to_slot": lambda item: _integer(item, name + ".to_slot"),
            "produced_blocks": lambda item: _integer(
                item, name + ".produced_blocks", nullable=True
            ),
            "coverage": lambda item: _enum(item, GAP_COVERAGE, name + ".coverage"),
        })
        if gap["to_slot"] < gap["from_slot"]:
            raise ValueError("{} has a reversed slot range".format(name))
        key = (gap["from_slot"], gap["to_slot"])
        if key in ranges:
            raise ValueError("gap ranges must be unique")
        ranges.add(key)
        selected.append(gap)
    return selected


def _public_incidents(value):
    if not isinstance(value, list) or len(value) > MAX_PUBLIC_INCIDENTS:
        raise ValueError("incidents must be a bounded array")
    selected = []
    seen = set()
    for index, child in enumerate(value):
        name = "incidents[{}]".format(index)
        incident = _stage(child, name, {
            "id": lambda item: _enum(item, PUBLIC_INCIDENT_IDS, name + ".id"),
            "severity": lambda item: _enum(item, INCIDENT_SEVERITIES, name + ".severity"),
            "started_unix_secs": lambda item: _integer(
                item, name + ".started_unix_secs", positive=True
            ),
            "resolved_unix_secs": lambda item: _integer(
                item, name + ".resolved_unix_secs", positive=True, nullable=True
            ),
        })
        if incident["id"] in seen:
            raise ValueError("incident ids must be unique")
        if (
            incident["resolved_unix_secs"] is not None
            and incident["resolved_unix_secs"] < incident["started_unix_secs"]
        ):
            raise ValueError("{} resolves before it starts".format(name))
        seen.add(incident["id"])
        selected.append(incident)
    return selected


def public_sse_line(line):
    newline = b"\n" if line.endswith(b"\n") else b""
    content = line[:-1] if newline else line
    if content.endswith(b"\r"):
        content = content[:-1]
    if b"\r" in content or b"\n" in content:
        return None
    if not content:
        return newline
    if content.startswith(b":"):
        # Preserve the heartbeat without forwarding an internal comment value.
        return b""
    if b":" in content:
        field, raw = content.split(b":", 1)
        if raw.startswith(b" "):
            raw = raw[1:]
    else:
        field, raw = content, b""

    if field == b"data":
        if not raw:
            return None
        try:
            return b"data:" + public_json_bytes(raw) + newline
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
            return None
    if field == b"event":
        if raw not in SAFE_SSE_EVENTS:
            return None
        return b"event:" + raw + newline
    if field in {b"id", b"retry"}:
        if not raw.isdigit() or len(raw) > 20:
            return None
        number = int(raw)
        if number > MAX_SAFE_INTEGER:
            return None
        return field + b":" + str(number).encode("ascii") + newline
    return None


class PublicWatcherProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    upstream_host = "127.0.0.1"
    upstream_port = 8786
    ingest_upstream_host = "127.0.0.1"
    ingest_upstream_port = 8790
    upstream_timeout_secs = 60.0

    def do_GET(self):
        self.proxy(send_body=True)

    def do_HEAD(self):
        self.proxy(send_body=False)

    def do_POST(self):
        self.send_error(405, "read-only watcher")

    do_DELETE = do_POST
    do_PATCH = do_POST
    do_PUT = do_POST

    def proxy(self, send_body):
        parsed = urlsplit(self.path)
        if parsed.scheme or parsed.netloc or not parsed.path.startswith("/"):
            self.send_error(400, "invalid request target")
            return
        if parsed.path.startswith("/api/") and parsed.path not in PUBLIC_API_GETS:
            self.send_error(404, "unknown public watcher endpoint")
            return
        target = parsed.path
        if parsed.query:
            target += "?" + parsed.query
        is_ingest_path = parsed.path == "/api/v1/sidecars/ingest-pipeline/status.json"

        request_headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() in COPY_REQUEST_HEADERS
        }
        request_headers["Accept-Encoding"] = "identity"
        upstream_host = (
            self.ingest_upstream_host if is_ingest_path else self.upstream_host
        )
        upstream_port = (
            self.ingest_upstream_port if is_ingest_path else self.upstream_port
        )
        connection = HTTPConnection(
            upstream_host, upstream_port, timeout=self.upstream_timeout_secs
        )
        response_started = False
        try:
            connection.request(self.command, target, headers=request_headers)
            upstream = connection.getresponse()
            content_type = upstream.getheader("Content-Type", "")
            media_type = content_type.partition(";")[0].strip().lower()
            is_json = (
                media_type == "application/json"
                and upstream.status not in {204, 304}
            )

            if upstream.status >= 400:
                self.send_public_error(send_body)
                response_started = True
                return

            # This endpoint crosses a trust boundary. Never accept a different
            # content type whose bytes could contain internal diagnostics or secrets.
            if is_ingest_path:
                if upstream.status != 200:
                    self.send_public_error(send_body)
                    response_started = True
                    return
                if not is_json:
                    raise ValueError("ingest endpoint returned an unsafe content type")
                if send_body:
                    raw = upstream.read(MAX_INGEST_JSON_BYTES + 1)
                    body = public_ingest_json_bytes(raw)
                else:
                    body = b""
                self.send_response(200)
                response_started = True
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                if send_body:
                    self.wfile.write(body)
                return

            is_api_path = parsed.path.startswith("/api/")
            is_sse_path = parsed.path == "/api/v1/events"
            if is_sse_path and media_type != "text/event-stream":
                raise ValueError("event endpoint returned an unsafe content type")
            if is_api_path and not is_sse_path and not is_json:
                raise ValueError("public API endpoint returned an unsafe content type")
            is_sse = is_sse_path

            if is_json and send_body:
                raw = upstream.read(MAX_JSON_BYTES + 1)
                body = public_json_bytes(raw)
                self.send_response(upstream.status)
                response_started = True
                self.copy_response_headers(upstream.getheaders(), transformed=True)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(upstream.status)
            response_started = True
            self.copy_response_headers(
                upstream.getheaders(), transformed=is_sse or is_json
            )
            if is_sse:
                self.close_connection = True
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("X-Accel-Buffering", "no")
                self.send_header("Connection", "close")
            elif is_json:
                self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            if not send_body:
                return

            if is_sse:
                while True:
                    line = upstream.readline(MAX_SSE_LINE_BYTES + 1)
                    if not line:
                        break
                    if len(line) > MAX_SSE_LINE_BYTES:
                        break
                    public_line = public_sse_line(line)
                    if public_line is None:
                        break
                    if not public_line:
                        continue
                    self.wfile.write(public_line)
                    self.wfile.flush()
                return

            while True:
                chunk = upstream.read(64 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionError, OSError, ValueError):
            if not response_started and not self.wfile.closed:
                try:
                    self.send_public_error(send_body)
                except (BrokenPipeError, ConnectionError, OSError):
                    pass
        finally:
            connection.close()

    def send_public_error(self, send_body):
        body = b'{"error":"watcher upstream unavailable"}'
        self.send_response(502)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body) if send_body else 0))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def copy_response_headers(self, headers, transformed):
        for key, value in headers:
            lower = key.lower()
            if lower in HOP_BY_HOP_HEADERS:
                continue
            if lower != "content-type" and lower not in SAFE_RESPONSE_HEADERS:
                continue
            if transformed and lower in {"content-encoding", "content-length", "etag"}:
                continue
            if lower == "content-type" and transformed:
                continue
            self.send_header(key, value)

    def log_message(self, message, *args):
        status = next(
            (
                code
                for value in args
                if str(value).isdigit()
                for code in [int(value)]
                if 400 <= code <= 599
            ),
            None,
        )
        if status is None:
            return
        try:
            print("watcher proxy: HTTP {}".format(status), file=sys.stderr, flush=True)
        except OSError:
            pass


class PublicWatcherProxyServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self,
        server_address,
        handler_class,
        max_requests=64,
        client_timeout_secs=10.0,
    ):
        if client_timeout_secs <= 0:
            raise ValueError("client timeout must be positive")
        self.request_slots = threading.BoundedSemaphore(max_requests)
        self.client_timeout_secs = client_timeout_secs
        super().__init__(server_address, handler_class)

    def get_request(self):
        request, client_address = super().get_request()
        request.settimeout(self.client_timeout_secs)
        return request, client_address

    def process_request(self, request, client_address):
        if not self.request_slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self.request_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.request_slots.release()


def network_address(value, flag, allow_private):
    parsed = urlsplit("//" + value)
    if parsed.port is None or parsed.hostname is None:
        raise argparse.ArgumentTypeError("{} must include an explicit host and port".format(flag))
    if parsed.hostname == "localhost":
        return parsed.hostname, parsed.port
    try:
        address = ipaddress.IPv4Address(parsed.hostname)
    except ipaddress.AddressValueError as error:
        raise argparse.ArgumentTypeError(
            "{} must use localhost or an explicit IPv4 address".format(flag)
        ) from error
    private_listener = allow_private and any(
        address in network for network in PRIVATE_LISTENER_NETWORKS
    )
    allowed = address.is_loopback or private_listener
    if address.is_unspecified or not allowed:
        scope = "loopback or private" if allow_private else "loopback"
        raise argparse.ArgumentTypeError(
            "{} must use an explicit {} address".format(flag, scope)
        )
    return parsed.hostname, parsed.port


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="127.0.0.1:8787")
    parser.add_argument("--upstream", default="127.0.0.1:8786")
    parser.add_argument("--ingest-upstream", default="127.0.0.1:8790")
    parser.add_argument("--upstream-timeout-secs", type=float, default=60.0)
    args = parser.parse_args()
    if args.upstream_timeout_secs <= 0:
        parser.error("--upstream-timeout-secs must be positive")
    listen = network_address(args.listen, "--listen", allow_private=True)
    upstream = network_address(args.upstream, "--upstream", allow_private=False)
    ingest_upstream = network_address(
        args.ingest_upstream, "--ingest-upstream", allow_private=False
    )
    if listen in {upstream, ingest_upstream}:
        parser.error("--listen must differ from both upstream addresses")
    if upstream == ingest_upstream:
        parser.error("--upstream and --ingest-upstream must differ")

    PublicWatcherProxyHandler.upstream_host = upstream[0]
    PublicWatcherProxyHandler.upstream_port = upstream[1]
    PublicWatcherProxyHandler.ingest_upstream_host = ingest_upstream[0]
    PublicWatcherProxyHandler.ingest_upstream_port = ingest_upstream[1]
    PublicWatcherProxyHandler.upstream_timeout_secs = args.upstream_timeout_secs
    server = PublicWatcherProxyServer(listen, PublicWatcherProxyHandler)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
