#!/usr/bin/env python3
"""Publish a bounded, secret-free view of the durable live-ingest pipeline."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import threading
import time
from urllib.parse import urlsplit


STATUS_PATH = "/api/v1/sidecars/ingest-pipeline/status.json"
MAX_JSON_BYTES = 1024 * 1024
MAX_TAIL_BYTES = 2 * 1024 * 1024
MAX_GAPS = 32
MAX_INCIDENTS = 32
MAX_TREE_ENTRIES = 4096
MAX_SAFE_INTEGER = (1 << 53) - 1
MAX_REPLAY_RESUME_HEADROOM_SLOTS = 10_000
DEFAULT_DISK_CRITICAL_FREE_BYTES = 21_474_836_480
DEFAULT_DISK_WARNING_FREE_BYTES = 32_212_254_720
PRIVATE_NETWORKS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
SAFE_INCIDENT_ID = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
ACK_JOURNAL_ID = re.compile(r"^[0-9a-f]{32}$")
ALERT_HEADING = re.compile(br"^Blockzilla backup - (WARNING|ERROR|CRITICAL)$")
INCIDENT_SEVERITY_RANK = {"warning": 1, "error": 2, "critical": 3}
GAP_COVERAGE_RANK = {
    "unproven": 0,
    "rpc_recoverable": 1,
    "normalized": 2,
    "raw": 3,
}
INCIDENT_ID_MAP = {
    "recorder_restarting": "grpc_stale",
    "grpc_stale": "grpc_stale",
    "upstream_access_blocked": "upstream_access_blocked",
    "provider_replay_gap": "replay_gap",
    "resume_coverage": "resume_coverage",
    "replay_recovery_failed": "replay_recovery_failed",
    "disk_space": "disk_space",
    "disk_warning": "disk_space",
    "disk_critical": "disk_space",
    "disk_check_failed": "disk_check_failed",
    "volume_invalid": "volume_invalid",
    "cache_rotation_failed": "cache_rotation_failed",
    "generation_rotation_failed": "generation_rotation_failed",
    "generation_backlog": "generation_backlog",
    "primary_sync_stale": "receiver_ack_stale",
    "generation_upload_failed": "object_store",
    "r2_usage_check_failed": "object_store",
    "r2_usage": "object_store",
    "b2_usage_check_failed": "object_store",
    "b2_usage": "object_store",
    "b2_usage_warning": "object_store",
    "b2_usage_critical": "object_store",
}


@dataclass(frozen=True)
class StatusConfig:
    cache_root: Path
    ack_status_file: Path
    known_gaps_file: Path | None = None
    capture_stale_after_secs: int = 60
    ack_stale_after_secs: int = 120
    disk_critical_free_bytes: int = DEFAULT_DISK_CRITICAL_FREE_BYTES
    disk_warning_free_bytes: int = DEFAULT_DISK_WARNING_FREE_BYTES


def _safe_regular_bytes(path: Path, maximum: int) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("status input is not a regular file")
        if info.st_size > maximum:
            raise ValueError("status input exceeds its byte limit")
        chunks = []
        remaining = maximum + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > maximum:
            raise ValueError("status input exceeds its byte limit")
        return data, info
    finally:
        os.close(descriptor)


def _safe_json(path: Path, maximum: int = MAX_JSON_BYTES):
    data, info = _safe_regular_bytes(path, maximum)
    return json.loads(data), info


def _safe_tail_json(path: Path) -> tuple[dict, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size <= 0:
            raise ValueError("durable journal is empty or not regular")
        start = max(0, info.st_size - MAX_TAIL_BYTES)
        os.lseek(descriptor, start, os.SEEK_SET)
        data = b""
        while len(data) <= MAX_TAIL_BYTES:
            chunk = os.read(descriptor, min(64 * 1024, MAX_TAIL_BYTES + 1 - len(data)))
            if not chunk:
                break
            data += chunk
        if len(data) > MAX_TAIL_BYTES:
            raise ValueError("durable journal tail exceeds its byte limit")
        if start > 0:
            first_newline = data.find(b"\n")
            if first_newline < 0:
                raise ValueError("durable journal has no complete bounded JSON row")
            # The bounded read can begin inside a row. Only that leading
            # fragment is uncommitted from this reader's point of view.
            data = data[first_newline + 1:]

        final_newline = data.rfind(b"\n")
        if final_newline < 0:
            raise ValueError("durable journal has no newline-committed JSON row")
        # A concurrently appended final fragment is not committed until its
        # newline arrives. Ignore only that fragment; the last complete row
        # must itself decode and validate instead of falling back indefinitely.
        committed = data[:final_newline]
        final_row = committed.rsplit(b"\n", 1)[-1]
        if not final_row.strip():
            raise ValueError("durable journal final committed row is empty")
        try:
            value = json.loads(final_row)
        except json.JSONDecodeError as error:
            raise ValueError("durable journal final committed row is invalid JSON") from error
        if not isinstance(value, dict):
            raise ValueError("durable journal final committed row is not an object")
        return value, info
    finally:
        os.close(descriptor)


def _non_negative_int(value):
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool)
        and 0 <= value <= MAX_SAFE_INTEGER
        else None
    )


def _positive_int(value):
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool)
        and 0 < value <= MAX_SAFE_INTEGER
        else None
    )


def _bounded_nonempty_string(value, maximum: int = 128):
    return value if isinstance(value, str) and 0 < len(value) <= maximum else None


def _journal_id_hex(value):
    if not isinstance(value, list) or len(value) != 16:
        return None
    if any(
        not isinstance(byte, int) or isinstance(byte, bool) or byte < 0 or byte > 255
        for byte in value
    ):
        return None
    return bytes(value).hex()


def _bounded_tree_bytes(root: Path) -> int:
    if root.is_symlink():
        raise ValueError("status tree root is a symlink")
    total = 0
    seen = 0
    pending = [root]
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                seen += 1
                if seen > MAX_TREE_ENTRIES:
                    raise ValueError("status tree exceeds its entry limit")
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise ValueError("status tree contains a symlink")
                if stat.S_ISDIR(info.st_mode):
                    pending.append(Path(entry.path))
                elif stat.S_ISREG(info.st_mode):
                    total += info.st_size
                else:
                    raise ValueError("status tree contains a special file")
    return total


def _safe_directories(root: Path, maximum: int = 64) -> list[Path]:
    if root.is_symlink():
        raise ValueError("status directory is a symlink")
    if not root.exists():
        return []
    directories = []
    with os.scandir(root) as entries:
        for entry in entries:
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                raise ValueError("status directory contains a symlink")
            if stat.S_ISDIR(info.st_mode):
                directories.append(Path(entry.path))
            if len(directories) > maximum:
                raise ValueError("status directory exceeds its entry limit")
    return sorted(directories, key=lambda value: value.name)


@contextmanager
def _rotation_lock(cache_root: Path):
    path = cache_root / ".rotation.lock"
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("rotation lock is not a regular file")
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


class CaptureProgressTracker:
    """Keep WAL freshness tied to logical committed-row progress, not file touches."""

    def __init__(self):
        self._lock = threading.Lock()
        self._stream = None
        self._sequence = None
        self._updated = None

    def observe(self, stream, sequence: int, evidence_updated: int) -> int:
        stream_key = (
            stream["cluster_id"],
            stream["origin_node_id"],
            stream["source_id"],
            stream["journal_id"],
        )
        with self._lock:
            if self._stream == stream_key and self._sequence is not None:
                if sequence < self._sequence:
                    raise ValueError("durable journal logical sequence regressed")
                if sequence == self._sequence:
                    return self._updated
            self._stream = stream_key
            self._sequence = sequence
            self._updated = evidence_updated
            return evidence_updated


def _read_capture(
    cache_root: Path,
    now: int,
    stale_after: int,
    progress_tracker: CaptureProgressTracker | None = None,
):
    active = cache_root / "active"
    with _rotation_lock(cache_root):
        identity, _ = _safe_json(active / "identity.json")
        tail, tail_info = _safe_tail_json(active / "raw-blocks.jsonl")
        if not isinstance(identity, dict) or identity.get("schema_version") != 1:
            raise ValueError("capture identity has an unsupported schema")
        if tail.get("schema_version") != 1:
            raise ValueError("durable journal tail has an unsupported schema")
        frame = _non_negative_int(tail.get("frame_id"))
        slot = _non_negative_int(tail.get("slot"))
        cluster_id = _bounded_nonempty_string(identity.get("cluster_id"))
        origin_node_id = _bounded_nonempty_string(identity.get("origin_node_id"))
        source_id = _bounded_nonempty_string(identity.get("source_id"))
        physical_journal_id = _journal_id_hex(identity.get("journal_id"))
        replication_journal_value = identity.get("replication_journal_id")
        sequence_base_value = identity.get("replication_sequence_base")
        if (replication_journal_value is None) != (sequence_base_value is None):
            raise ValueError("capture logical replication identity is incomplete")
        logical_journal_id = (
            physical_journal_id
            if replication_journal_value is None
            else _journal_id_hex(replication_journal_value)
        )
        sequence_base = (
            0 if sequence_base_value is None else _non_negative_int(sequence_base_value)
        )
        if frame is None or slot is None:
            raise ValueError("durable journal tail has invalid counters")
        if (
            cluster_id is None or origin_node_id is None or source_id is None or
            physical_journal_id is None or logical_journal_id is None or sequence_base is None
        ):
            raise ValueError("capture identity has invalid fields")
        sequence = sequence_base + frame
        if sequence > MAX_SAFE_INTEGER:
            raise ValueError("durable journal logical sequence exceeds the public integer limit")
        active_bytes = _bounded_tree_bytes(active)
        sealed = _safe_directories(cache_root / "sealed")
        sealed_bytes = sum(_bounded_tree_bytes(generation) for generation in sealed)
        stream = {
            "cluster_id": cluster_id,
            "origin_node_id": origin_node_id,
            "source_id": source_id,
            "journal_id": logical_journal_id,
        }
    updated = int(tail_info.st_mtime)
    if updated > now + 5:
        raise ValueError("durable journal timestamp is in the future")
    if progress_tracker is not None:
        updated = progress_tracker.observe(stream, sequence, updated)
    age = max(0, now - updated)
    state = "recording" if age <= stale_after else "stalled"
    return {
        "state": state,
        "slot": slot,
        "sequence": sequence,
        "updated": updated,
        "active_bytes": active_bytes,
        "sealed_count": len(sealed),
        "sealed_bytes": sealed_bytes,
        "stream": stream,
    }


def _read_ack(path: Path):
    value, _ = _safe_json(path)
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise ValueError("ACK status has an unsupported schema")
    cluster_id = _bounded_nonempty_string(value.get("cluster_id"))
    origin_node_id = _bounded_nonempty_string(value.get("origin_node_id"))
    source_id = _bounded_nonempty_string(value.get("source_id"))
    journal_id = value.get("journal_id")
    sequence = _non_negative_int(value.get("through_sequence"))
    updated = _positive_int(value.get("updated_unix_secs"))
    if (
        cluster_id is None or origin_node_id is None or source_id is None or
        not isinstance(journal_id, str) or ACK_JOURNAL_ID.fullmatch(journal_id) is None or
        sequence is None or updated is None
    ):
        raise ValueError("ACK status fields are invalid")
    return {
        "sequence": sequence,
        "updated": updated,
        "stream": {
            "cluster_id": cluster_id,
            "origin_node_id": origin_node_id,
            "source_id": source_id,
            "journal_id": journal_id,
        },
    }


def _ack_for_capture(ack, capture, now: int, stale_after: int):
    if ack["stream"] != capture["stream"]:
        raise ValueError("ACK status belongs to a different replication stream")
    if ack["sequence"] > capture["sequence"]:
        raise ValueError("ACK status is ahead of the durable capture")
    lag = capture["sequence"] - ack["sequence"]
    updated = ack["updated"]
    if updated > now + 5:
        raise ValueError("ACK status timestamp is in the future")
    age = max(0, now - updated)
    state = "stalled" if age > stale_after else "caught_up" if lag == 0 else "syncing"
    return {**ack, "state": state, "lag": lag}


def _gap_from_event(value):
    if not isinstance(value, dict):
        raise ValueError("replay gap record is not an object")
    schema = value.get("schema_version")
    anchor = _non_negative_int(value.get("anchor_slot"))
    requested = _non_negative_int(value.get("requested_slot"))
    if type(schema) is not int:
        raise ValueError("replay gap record has an unsupported schema")
    if schema == 1:
        resume = _non_negative_int(value.get("available_slot"))
        if (
            anchor is None or requested is None or resume is None or
            requested < anchor or resume <= requested
        ):
            raise ValueError("replay gap schema 1 fields are invalid")
    elif schema == 2:
        provider = _non_negative_int(value.get("provider_available_slot"))
        resume = _non_negative_int(value.get("selected_resume_slot"))
        if (
            anchor is None or requested is None or provider is None or resume is None or
            requested < anchor or provider <= requested or resume < provider or
            resume - provider > MAX_REPLAY_RESUME_HEADROOM_SLOTS
        ):
            raise ValueError("replay gap schema 2 fields are invalid")
    else:
        raise ValueError("replay gap record has an unsupported schema")

    # The anchor is the last locally durable slot. The recorder resumes at the
    # selected slot, so everything strictly between those two points remains
    # unproven until a stronger, separately supplied coverage source replaces
    # this audit evidence.
    if resume == anchor + 1:
        return None
    if resume <= anchor:
        # This is implied by the schema relations above today, but retaining an
        # explicit boundary check keeps the interval construction fail-closed
        # if a future recorder schema changes those relations.
        raise ValueError("replay gap resume slot does not follow its anchor")
    return {
        "from_slot": anchor + 1,
        "to_slot": resume - 1,
        "produced_blocks": None,
        "coverage": "unproven",
    }


def _read_gaps(config: StatusConfig):
    gaps = {}

    def merge(gap):
        key = (gap["from_slot"], gap["to_slot"])
        previous = gaps.get(key)
        if previous is None:
            gaps[key] = gap
            return
        if GAP_COVERAGE_RANK[gap["coverage"]] > GAP_COVERAGE_RANK[previous["coverage"]]:
            gaps[key] = gap
        elif previous["produced_blocks"] is None and gap["produced_blocks"] is not None:
            gaps[key] = {**previous, "produced_blocks": gap["produced_blocks"]}

    if config.known_gaps_file is not None:
        if config.known_gaps_file.is_symlink():
            raise ValueError("known gap file is a symlink")
        if not config.known_gaps_file.is_file():
            raise ValueError("configured known gap file is missing or not regular")
        value, _ = _safe_json(config.known_gaps_file)
        if not isinstance(value, list) or len(value) > MAX_TREE_ENTRIES:
            raise ValueError("known gap file is invalid or unbounded")
        for gap in value:
            if not isinstance(gap, dict):
                raise ValueError("known gap entry is invalid")
            start = _non_negative_int(gap.get("from_slot"))
            end = _non_negative_int(gap.get("to_slot"))
            blocks = gap.get("produced_blocks")
            coverage = gap.get("coverage")
            if (
                start is None or end is None or end < start or
                (blocks is not None and _non_negative_int(blocks) is None) or
                not isinstance(coverage, str) or
                coverage not in {"raw", "normalized", "rpc_recoverable", "unproven"}
            ):
                raise ValueError("known gap entry has invalid fields")
            merge({
                "from_slot": start,
                "to_slot": end,
                "produced_blocks": blocks,
                "coverage": coverage,
            })

    generation_roots = [config.cache_root / "active"]
    generation_roots.extend(_safe_directories(config.cache_root / "sealed"))
    # Generation-local copies authorize recorder recovery, but ACK-covered
    # generations are eventually retired. The monitoring registry is the
    # immutable audit copy intended to keep those gaps visible afterward.
    replay_directories = [config.cache_root / "monitoring" / "replay-gaps"]
    replay_directories.extend(generation / "replay-gaps" for generation in generation_roots)
    scanned = 0
    for directory in replay_directories:
        if directory.is_symlink():
            raise ValueError("replay gap directory is a symlink")
        if not directory.exists():
            continue
        with os.scandir(directory) as entries:
            for entry in entries:
                scanned += 1
                if scanned > MAX_TREE_ENTRIES:
                    raise ValueError("replay gap tree exceeds its entry limit")
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise ValueError("replay gap directory contains a symlink")
                if not entry.name.endswith(".json"):
                    continue
                if not stat.S_ISREG(info.st_mode):
                    raise ValueError("replay gap entry is not a regular file")
                event, _ = _safe_json(Path(entry.path))
                gap = _gap_from_event(event)
                if gap is not None:
                    merge(gap)
    ordered = [gaps[key] for key in sorted(gaps)]
    has_unproven = any(gap["coverage"] == "unproven" for gap in ordered)
    has_partial = any(
        gap["coverage"] in {"normalized", "rpc_recoverable"} for gap in ordered
    )
    return ordered[:MAX_GAPS], len(ordered) > MAX_GAPS, has_unproven, has_partial


def _alert_severity(active_path: Path):
    data, _ = _safe_regular_bytes(active_path, 4096)
    lines = data.splitlines()
    first_line = lines[0] if lines else b""
    match = ALERT_HEADING.fullmatch(first_line)
    if match is None:
        raise ValueError("incident active state has an invalid heading")
    return match.group(1).decode("ascii").lower()


def _alert_started(directory: Path, raw_id: str, fallback: int):
    delivered = directory / f"{raw_id}.delivered"
    if delivered.is_symlink():
        raise ValueError("incident delivery state is a symlink")
    if not delivered.exists():
        return max(1, fallback)
    data, _ = _safe_regular_bytes(delivered, 64)
    parts = data.strip().split()
    if len(parts) != 2 or parts[1] not in {b"WARNING", b"ERROR", b"CRITICAL"}:
        raise ValueError("incident delivery state is invalid")
    try:
        started = int(parts[0])
    except ValueError as error:
        raise ValueError("incident delivery timestamp is invalid") from error
    if started <= 0:
        raise ValueError("incident delivery timestamp is invalid")
    return started


def _read_incidents(cache_root: Path):
    directory = cache_root / "monitoring" / "telegram-alerts"
    if directory.is_symlink():
        raise ValueError("incident directory is a symlink")
    if not directory.exists():
        return []
    incidents = {}
    scanned = 0
    with os.scandir(directory) as entries:
        for entry in entries:
            scanned += 1
            if scanned > MAX_TREE_ENTRIES:
                raise ValueError("incident directory exceeds its entry limit")
            info = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(info.st_mode):
                raise ValueError("incident directory contains a symlink")
            if not stat.S_ISREG(info.st_mode) or not entry.name.endswith(".active"):
                continue
            raw_id = entry.name.removesuffix(".active")
            incident_id = INCIDENT_ID_MAP.get(raw_id)
            if incident_id is None or not SAFE_INCIDENT_ID.fullmatch(incident_id):
                continue
            severity = _alert_severity(Path(entry.path))
            incident = {
                "id": incident_id,
                "severity": severity,
                "started_unix_secs": _alert_started(directory, raw_id, int(info.st_mtime)),
                "resolved_unix_secs": None,
            }
            previous = incidents.get(incident_id)
            if previous is not None:
                previous["started_unix_secs"] = min(
                    previous["started_unix_secs"], incident["started_unix_secs"]
                )
                if (
                    INCIDENT_SEVERITY_RANK[severity] >
                    INCIDENT_SEVERITY_RANK[previous["severity"]]
                ):
                    previous["severity"] = severity
            elif len(incidents) < MAX_INCIDENTS:
                incidents[incident_id] = incident
    return sorted(incidents.values(), key=lambda item: (item["started_unix_secs"], item["id"]))


def build_status(
    config: StatusConfig,
    now: int | None = None,
    progress_tracker: CaptureProgressTracker | None = None,
):
    generated = int(time.time()) if now is None else now
    if config.cache_root.is_symlink() or not config.cache_root.is_dir():
        raise ValueError("cache root must be a real directory")
    critical_free = _non_negative_int(config.disk_critical_free_bytes)
    warning_free = _non_negative_int(config.disk_warning_free_bytes)
    if critical_free is None or warning_free is None or warning_free <= critical_free:
        raise ValueError("disk warning threshold must exceed the critical threshold")
    capture = _read_capture(
        config.cache_root,
        generated,
        config.capture_stale_after_secs,
        progress_tracker,
    )
    raw_ack = _read_ack(config.ack_status_file)
    if raw_ack["stream"] != capture["stream"] or raw_ack["sequence"] > capture["sequence"]:
        # Rotation and ACK publication are independent atomic operations. One
        # bounded reread prevents a harmless boundary race from dropping the
        # public sample, while a persistent mismatch still fails closed.
        capture = _read_capture(
            config.cache_root,
            generated,
            config.capture_stale_after_secs,
            progress_tracker,
        )
    ack = _ack_for_capture(raw_ack, capture, generated, config.ack_stale_after_secs)
    disk = os.statvfs(config.cache_root)
    free_bytes = disk.f_bavail * disk.f_frsize
    total_bytes = disk.f_blocks * disk.f_frsize
    gaps, gaps_truncated, has_unproven_gap, has_partial_gap = _read_gaps(config)
    incidents = _read_incidents(config.cache_root)
    incident_failure = any(
        incident["severity"] in {"error", "critical"} for incident in incidents
    )
    disk_critical = free_bytes < critical_free
    disk_warning = free_bytes < warning_free
    has_error = (
        capture["state"] == "stalled" or ack["state"] == "stalled" or
        incident_failure or disk_critical or has_unproven_gap
    )
    has_warning = (
        ack["state"] == "syncing" or bool(incidents) or disk_warning or has_partial_gap
    )
    overall = "failed" if has_error else "degraded" if has_warning else "healthy"
    unacknowledged_bytes = capture["sealed_bytes"]
    if ack["lag"] > 0:
        # The handoff journal does not keep a per-logical-sequence byte prefix.
        # Report the whole active generation as a conservative upper bound.
        unacknowledged_bytes += capture["active_bytes"]
    return {
        "schema_version": 1,
        "updated_unix_secs": generated,
        "overall_state": overall,
        "upstream": {
            # The legacy enum name means only that the durable WAL tail is
            # fresh. This publisher has no direct Triton socket telemetry.
            "state": "connected" if capture["state"] == "recording" else "stalled",
            "updated_unix_secs": capture["updated"],
            "reconnects_1h": None,
        },
        "recorder": {
            "state": capture["state"],
            "durable_slot": capture["slot"],
            "updated_unix_secs": capture["updated"],
            "active_bytes": capture["active_bytes"],
            "sealed_generations": capture["sealed_count"],
            "unacknowledged_bytes": unacknowledged_bytes,
            "disk_free_bytes": free_bytes,
            "disk_total_bytes": total_bytes,
        },
        "replication": {
            "state": ack["state"],
            "ack_through_sequence": ack["sequence"],
            "ack_slot": capture["slot"] if ack["lag"] == 0 else None,
            "updated_unix_secs": ack["updated"],
            "lag_records": ack["lag"],
        },
        "indexer": {
            "state": "unavailable",
            "last_slot": None,
            "updated_unix_secs": None,
            "lag_slots": None,
        },
        "object_store": {
            # No durable, stream-bound R2 watermark is currently available.
            "provider": "r2",
            "state": "unavailable",
            "committed_bytes": None,
            "pending_bytes": 0,
            "updated_unix_secs": None,
        },
        "fallback": {
            "state": "unavailable",
            "last_slot": None,
            "updated_unix_secs": None,
            "lag_slots": None,
        },
        "gaps": gaps,
        "gaps_truncated": gaps_truncated,
        "incidents": incidents,
    }


class StatusCache:
    def __init__(self, config: StatusConfig, interval_secs: int):
        self.config = config
        self.interval_secs = interval_secs
        self._lock = threading.Lock()
        self._encoded = None
        self._refresh_failed = True
        self._stopped = threading.Event()
        self._capture_progress = CaptureProgressTracker()

    def refresh(self):
        try:
            encoded = json.dumps(
                build_status(self.config, progress_tracker=self._capture_progress),
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
        except (
            OSError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            OverflowError,
            RecursionError,
        ):
            self._mark_refresh_failed()
            raise
        with self._lock:
            self._encoded = encoded
            self._refresh_failed = False

    def get(self):
        with self._lock:
            return self._encoded

    def healthy(self):
        with self._lock:
            return self._encoded is not None and not self._refresh_failed

    def _mark_refresh_failed(self):
        with self._lock:
            self._refresh_failed = True

    def run(self):
        while not self._stopped.is_set():
            try:
                self.refresh()
            except (
                OSError,
                TypeError,
                ValueError,
                json.JSONDecodeError,
                OverflowError,
                RecursionError,
            ):
                # Preserve the last valid sample. Its timestamp will age and the
                # client will fail visibly stale instead of accepting guesses.
                self._mark_refresh_failed()
            self._stopped.wait(self.interval_secs)

    def stop(self):
        self._stopped.set()


class IngestStatusHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    cache = None

    def do_GET(self):
        self._serve(send_body=True)

    def do_HEAD(self):
        self._serve(send_body=False)

    def do_POST(self):
        self.send_error(405, "read-only status service")

    do_DELETE = do_POST
    do_PATCH = do_POST
    do_PUT = do_POST

    def _serve(self, send_body):
        target = urlsplit(self.path)
        if target.scheme or target.netloc or target.query:
            self.send_error(400, "invalid request target")
            return
        if target.path == "/healthz":
            if self.cache is None or not self.cache.healthy():
                self._response(b'{"ok":false}', send_body, status=503)
                return
            self._response(b'{"ok":true}', send_body)
            return
        if target.path != STATUS_PATH:
            self.send_error(404, "unknown status endpoint")
            return
        body = self.cache.get()
        if body is None:
            self.send_error(503, "status snapshot unavailable")
            return
        self._response(body, send_body)

    def _response(self, body, send_body, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if send_body:
            self.wfile.write(body)

    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        super().end_headers()

    def log_message(self, _format, *_args):
        return


class IngestStatusServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 32

    def __init__(self, server_address, handler_class, max_requests=32):
        self._request_slots = threading.BoundedSemaphore(max_requests)
        super().__init__(server_address, handler_class)

    def get_request(self):
        request, client_address = super().get_request()
        request.settimeout(5.0)
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


def listener(value):
    parsed = urlsplit("//" + value)
    if parsed.hostname is None or parsed.port is None:
        raise argparse.ArgumentTypeError("--listen must include an explicit host and port")
    if parsed.hostname == "localhost":
        return parsed.hostname, parsed.port
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError as error:
        raise argparse.ArgumentTypeError("--listen must use localhost or an explicit IP address") from error
    if address.is_unspecified or not (address.is_loopback or any(address in network for network in PRIVATE_NETWORKS)):
        raise argparse.ArgumentTypeError("--listen must use a loopback or private address")
    return str(address), parsed.port


def positive_seconds(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def non_negative_bytes(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a non-negative integer") from error
    if parsed < 0 or parsed > MAX_SAFE_INTEGER:
        raise argparse.ArgumentTypeError("value must be a non-negative safe integer")
    return parsed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen", type=listener, default=listener("127.0.0.1:8790"))
    parser.add_argument("--cache-root", type=Path, required=True)
    parser.add_argument("--ack-status-file", type=Path, required=True)
    parser.add_argument("--known-gaps-file", type=Path)
    parser.add_argument("--interval-secs", type=positive_seconds, default=10)
    parser.add_argument("--capture-stale-after-secs", type=positive_seconds, default=60)
    parser.add_argument("--ack-stale-after-secs", type=positive_seconds, default=120)
    parser.add_argument(
        "--disk-critical-free-bytes",
        type=non_negative_bytes,
        default=DEFAULT_DISK_CRITICAL_FREE_BYTES,
    )
    parser.add_argument(
        "--disk-warning-free-bytes",
        type=non_negative_bytes,
        default=DEFAULT_DISK_WARNING_FREE_BYTES,
    )
    args = parser.parse_args(argv)
    if args.disk_warning_free_bytes <= args.disk_critical_free_bytes:
        parser.error("--disk-warning-free-bytes must exceed --disk-critical-free-bytes")
    config = StatusConfig(
        cache_root=args.cache_root,
        ack_status_file=args.ack_status_file,
        known_gaps_file=args.known_gaps_file,
        capture_stale_after_secs=args.capture_stale_after_secs,
        ack_stale_after_secs=args.ack_stale_after_secs,
        disk_critical_free_bytes=args.disk_critical_free_bytes,
        disk_warning_free_bytes=args.disk_warning_free_bytes,
    )
    cache = StatusCache(config, args.interval_secs)
    cache.refresh()
    worker = threading.Thread(target=cache.run, name="ingest-status-refresh", daemon=True)
    worker.start()
    handler = type("ConfiguredIngestStatusHandler", (IngestStatusHandler,), {"cache": cache})
    server = IngestStatusServer(args.listen, handler)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        cache.stop()
        worker.join(timeout=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
