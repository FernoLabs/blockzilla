#!/usr/bin/env python3
"""Fail-closed cleanup for non-production entries in the NAS live scan root.

This tool never deletes data.  It only moves a deliberately small allow-list of
debug, benchmark, sample, and failed-recovery directories to a same-filesystem
quarantine outside the live root.  Production captures and repair bundles are
inventory-only.

Apply mode publishes one self-contained archive directory with an atomic JSON
receipt.  A hidden transaction directory and its atomic transaction journal are
left behind if the process or host dies before publication, making interrupted
moves visible and recoverable.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import socket
import stat
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator


TOOL_VERSION = 1
REPAIR_MARKER = "REPAIR-REQUIRED.json"
MAX_REPAIR_MARKER_BYTES = 16 * 1024 * 1024
MAX_REPAIR_MARKERS = 256
MAX_REPAIR_SOURCES = 64
MAX_RECEIPT_INPUTS = 512
DEFAULT_MIN_AGE_SECONDS = 3600
DEFAULT_MAX_CANDIDATE_ENTRIES = 100_000

SAFE_CANDIDATE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("failed_recovery", re.compile(r"^\.recovery-failed(?:-.+)?$")),
    ("benchmark", re.compile(r"^codex-bench(?:-.+)?$")),
    ("samples", re.compile(r"^codex-samples(?:-.+)?$")),
)

# The presence of any of these paths means that a directory is capture-shaped,
# even if somebody gave it a debug-looking name.  Such a directory is retained.
PRODUCTION_SIGNALS = (
    "producer-layout.json",
    REPAIR_MARKER,
    "blocks/live-no-registry-blocks.bin",
    "blocks/live-pre-hot-blocks.bin",
    "blocks/grpc-raw-blocks.bin",
    "index/block-index.bin",
    "index/blockhash_registry.bin",
    "journal/grpc-blocks.jsonl",
    "journal/progress.json",
    "poh/poh.wincode",
)


class CleanupError(RuntimeError):
    """An unsafe or inconsistent cleanup request."""


@dataclass(frozen=True)
class TreeSnapshot:
    entries: int
    regular_files: int
    directories: int
    symlinks: int
    logical_bytes: int
    allocated_bytes: int
    newest_mtime_ns: int
    root_device: int
    root_inode: int
    metadata_sha256: str
    inode_keys: frozenset[tuple[int, int]]

    def receipt_value(self) -> dict[str, int | str]:
        return {
            "entries": self.entries,
            "regular_files": self.regular_files,
            "directories": self.directories,
            "symlinks": self.symlinks,
            "logical_bytes": self.logical_bytes,
            "allocated_bytes": self.allocated_bytes,
            "newest_mtime_ns": self.newest_mtime_ns,
            "root_device": self.root_device,
            "root_inode": self.root_inode,
            "metadata_sha256": self.metadata_sha256,
        }


@dataclass(frozen=True)
class RepairMarker:
    path: Path
    epoch: int
    state: str
    sha256: str
    bytes: int
    protected_paths: tuple[Path, ...]
    source_capture_paths: tuple[Path, ...]

    def receipt_value(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "epoch": self.epoch,
            "state": self.state,
            "sha256": self.sha256,
            "bytes": self.bytes,
            "source_capture_paths": [str(path) for path in self.source_capture_paths],
            "protected_path_count": len(self.protected_paths),
        }


@dataclass(frozen=True)
class Candidate:
    name: str
    path: Path
    category: str
    production_signals: tuple[str, ...]
    protected_by: tuple[Path, ...]
    snapshot: TreeSnapshot

    @property
    def eligible(self) -> bool:
        return not self.production_signals and not self.protected_by


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def default_run_id() -> str:
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"live-root-cleanup-{timestamp}-{uuid.uuid4().hex[:8]}"


def validate_run_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
        raise argparse.ArgumentTypeError(
            "run id must contain 1..128 ASCII letters, digits, '.', '_' or '-'"
        )
    return value


def validate_entry_name(value: str) -> str:
    if (
        value in {"", ".", ".."}
        or os.path.isabs(value)
        or Path(value).name != value
        or "/" in value
        or "\x00" in value
    ):
        raise argparse.ArgumentTypeError("candidate must be one direct-child directory name")
    return value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Quarantine only known non-production folders outside the NAS live scan root; "
            "never delete or relocate captures"
        )
    )
    parser.add_argument("--live-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--mode", choices=("dry-run", "apply"), default="dry-run")
    parser.add_argument("--run-id", type=validate_run_id, default=default_run_id())
    parser.add_argument(
        "--candidate",
        action="append",
        type=validate_entry_name,
        help=(
            "direct-child name to consider; repeat as needed. If omitted, all existing "
            "directories matching the narrow built-in non-production allow-list are considered"
        ),
    )
    parser.add_argument(
        "--min-age-seconds",
        type=int,
        default=DEFAULT_MIN_AGE_SECONDS,
        help="refuse a candidate if any entry was modified more recently (default: 3600)",
    )
    parser.add_argument(
        "--max-candidate-entries",
        type=int,
        default=DEFAULT_MAX_CANDIDATE_ENTRIES,
        help="bound the inode/open-file audit for each candidate",
    )
    return parser.parse_args(argv)


def resolve_directory(path: Path, label: str) -> Path:
    try:
        resolved = path.expanduser().resolve(strict=True)
    except (FileNotFoundError, RuntimeError, OSError) as error:
        raise CleanupError(f"{label} is unavailable: {path}: {error}") from error
    if not resolved.is_dir():
        raise CleanupError(f"{label} is not a directory: {resolved}")
    if stat.S_ISLNK(os.lstat(path.expanduser()).st_mode):
        raise CleanupError(f"{label} must not be a symlink: {path}")
    return resolved


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def classify_non_production_name(name: str) -> str | None:
    for category, pattern in SAFE_CANDIDATE_PATTERNS:
        if pattern.fullmatch(name):
            return category
    return None


def production_signals(path: Path) -> tuple[str, ...]:
    return tuple(signal for signal in PRODUCTION_SIGNALS if (path / signal).exists())


def walk_no_follow(root: Path) -> Iterator[tuple[Path, os.stat_result]]:
    """Yield a bounded caller-owned tree without following symlinked directories."""

    try:
        yield root, root.lstat()
    except OSError as error:
        raise CleanupError(f"stat candidate root {root}: {error}") from error

    def onerror(error: OSError) -> None:
        raise CleanupError(f"walk candidate {root}: {error}")

    for directory, directories, files in os.walk(
        root, topdown=True, followlinks=False, onerror=onerror
    ):
        directories.sort()
        files.sort()
        directory_path = Path(directory)
        for name in [*directories, *files]:
            path = directory_path / name
            try:
                yield path, path.lstat()
            except OSError as error:
                raise CleanupError(f"stat candidate entry {path}: {error}") from error


def snapshot_tree(root: Path, max_entries: int) -> TreeSnapshot:
    if max_entries <= 0:
        raise CleanupError("max-candidate-entries must be positive")
    digest = hashlib.sha256()
    entries = regular_files = directories = symlinks = 0
    logical_bytes = allocated_bytes = newest_mtime_ns = 0
    root_stat: os.stat_result | None = None
    inode_keys: set[tuple[int, int]] = set()

    for path, metadata in walk_no_follow(root):
        entries += 1
        if entries > max_entries:
            raise CleanupError(
                f"candidate {root} exceeds the {max_entries}-entry audit bound"
            )
        if root_stat is None:
            root_stat = metadata
            if not stat.S_ISDIR(metadata.st_mode):
                raise CleanupError(f"candidate is not a directory: {root}")
        if metadata.st_dev != root_stat.st_dev:
            raise CleanupError(
                f"candidate {root} crosses a filesystem or mount at {path}; refusing rename"
            )

        mode = metadata.st_mode
        if stat.S_ISREG(mode):
            regular_files += 1
            logical_bytes += metadata.st_size
        elif stat.S_ISDIR(mode):
            directories += 1
        elif stat.S_ISLNK(mode):
            symlinks += 1
        else:
            raise CleanupError(
                f"candidate {root} contains a socket/device/FIFO at {path}; refusing rename"
            )
        allocated_bytes += getattr(metadata, "st_blocks", 0) * 512
        newest_mtime_ns = max(newest_mtime_ns, metadata.st_mtime_ns)
        inode_keys.add((metadata.st_dev, metadata.st_ino))
        relative = b"." if path == root else os.fsencode(path.relative_to(root))
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        for value in (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_mode,
            metadata.st_nlink,
            metadata.st_uid,
            metadata.st_gid,
            metadata.st_size,
            getattr(metadata, "st_blocks", 0),
            metadata.st_mtime_ns,
        ):
            digest.update(int(value).to_bytes(16, "big", signed=False))

    if root_stat is None:  # pragma: no cover - the root is yielded first
        raise CleanupError(f"candidate disappeared while scanning: {root}")
    return TreeSnapshot(
        entries=entries,
        regular_files=regular_files,
        directories=directories,
        symlinks=symlinks,
        logical_bytes=logical_bytes,
        allocated_bytes=allocated_bytes,
        newest_mtime_ns=newest_mtime_ns,
        root_device=root_stat.st_dev,
        root_inode=root_stat.st_ino,
        metadata_sha256=digest.hexdigest(),
        inode_keys=frozenset(inode_keys),
    )


def reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CleanupError(f"repair marker contains duplicate JSON key {key!r}")
        result[key] = value
    return result


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def canonical_reference_path(value: str) -> Path:
    """Resolve existing symlink prefixes while allowing a missing final path."""

    return Path(os.path.realpath(os.path.normpath(value)))


def absolute_paths_in_json(value: Any) -> Iterator[Path]:
    """Find absolute path strings without retaining thousands of relative RPC paths."""

    if isinstance(value, dict):
        for child in value.values():
            yield from absolute_paths_in_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from absolute_paths_in_json(child)
    elif isinstance(value, str) and os.path.isabs(value):
        yield canonical_reference_path(value)


def read_repair_marker(marker_path: Path) -> RepairMarker:
    try:
        metadata = marker_path.lstat()
    except OSError as error:
        raise CleanupError(f"stat repair marker {marker_path}: {error}") from error
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise CleanupError(f"repair marker must be a regular non-symlink file: {marker_path}")
    if metadata.st_size <= 0 or metadata.st_size > MAX_REPAIR_MARKER_BYTES:
        raise CleanupError(
            f"repair marker {marker_path} has {metadata.st_size} bytes; expected "
            f"1..={MAX_REPAIR_MARKER_BYTES}"
        )
    try:
        payload = marker_path.read_bytes()
    except OSError as error:
        raise CleanupError(f"read repair marker {marker_path}: {error}") from error
    if len(payload) != metadata.st_size:
        raise CleanupError(f"repair marker changed while reading: {marker_path}")
    try:
        document = json.loads(payload, object_pairs_hook=reject_duplicate_json_keys)
    except (json.JSONDecodeError, UnicodeDecodeError, RecursionError) as error:
        raise CleanupError(f"decode repair marker {marker_path}: {error}") from error
    try:
        after = marker_path.lstat()
    except OSError as error:
        raise CleanupError(f"restat repair marker {marker_path}: {error}") from error
    if (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) != (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
    ):
        raise CleanupError(f"repair marker changed while reading: {marker_path}")
    if not isinstance(document, dict):
        raise CleanupError(f"repair marker is not a JSON object: {marker_path}")

    version = document.get("version")
    epoch = document.get("epoch")
    state = document.get("state")
    publication_ready = document.get("publication_ready")
    sources = document.get("block_sources")
    receipts = document.get("capture_and_completion_receipts", [])
    if not isinstance(version, int) or isinstance(version, bool) or version <= 0:
        raise CleanupError(f"repair marker has invalid version: {marker_path}")
    if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 0:
        raise CleanupError(f"repair marker has invalid epoch: {marker_path}")
    if not isinstance(state, str) or not state:
        raise CleanupError(f"repair marker has invalid state: {marker_path}")
    if publication_ready is not False:
        raise CleanupError(
            f"REPAIR-REQUIRED marker must declare publication_ready=false: {marker_path}"
        )
    if not isinstance(sources, list) or not (1 <= len(sources) <= MAX_REPAIR_SOURCES):
        raise CleanupError(
            f"repair marker {marker_path} must contain 1..={MAX_REPAIR_SOURCES} block sources"
        )
    if not isinstance(receipts, list) or len(receipts) > MAX_RECEIPT_INPUTS:
        raise CleanupError(
            f"repair marker {marker_path} has invalid or excessive input receipts"
        )

    source_paths: list[Path] = []
    for index, source in enumerate(sources):
        if not isinstance(source, dict):
            raise CleanupError(f"repair marker source {index} is not an object: {marker_path}")
        original = source.get("original_capture_dir")
        selected = source.get("selected_blocks")
        if not isinstance(original, str) or not os.path.isabs(original):
            raise CleanupError(
                f"repair marker source {index} has no absolute original_capture_dir: {marker_path}"
            )
        if not isinstance(selected, int) or isinstance(selected, bool) or selected <= 0:
            raise CleanupError(
                f"repair marker source {index} has invalid selected_blocks: {marker_path}"
            )
        source_paths.append(canonical_reference_path(original))
    if len(set(source_paths)) != len(source_paths):
        raise CleanupError(f"repair marker repeats a block source: {marker_path}")
    for source_path in source_paths:
        try:
            source_metadata = source_path.stat()
        except OSError as error:
            raise CleanupError(
                f"repair marker source is unavailable; no cleanup is allowed: "
                f"{source_path}: {error}"
            ) from error
        if not stat.S_ISDIR(source_metadata.st_mode):
            raise CleanupError(
                f"repair marker source is not a directory: {source_path}"
            )

    # The bundle itself is always protected.  Absolute paths anywhere in the
    # bounded marker include sealed receipts and source files.  Relative RPC
    # paths are contained by the already protected bundle and need not become
    # 14,000 receipt rows.
    protected = {marker_path.parent, *source_paths, *absolute_paths_in_json(document)}
    return RepairMarker(
        path=marker_path,
        epoch=epoch,
        state=state,
        sha256=sha256_bytes(payload),
        bytes=len(payload),
        protected_paths=tuple(sorted(protected, key=os.fspath)),
        source_capture_paths=tuple(source_paths),
    )


def discover_repair_markers(live_root: Path) -> tuple[RepairMarker, ...]:
    live_root = live_root.resolve(strict=True)
    found: list[Path] = []

    def onerror(error: OSError) -> None:
        raise CleanupError(f"walk live root for repair markers: {error}")

    for directory, directories, files in os.walk(
        live_root, topdown=True, followlinks=False, onerror=onerror
    ):
        directories.sort()
        files.sort()
        if REPAIR_MARKER in files:
            found.append(Path(directory) / REPAIR_MARKER)
            if len(found) > MAX_REPAIR_MARKERS:
                raise CleanupError(
                    f"live root has more than {MAX_REPAIR_MARKERS} repair markers"
                )
    return tuple(read_repair_marker(path) for path in found)


def overlapping_markers(path: Path, markers: Iterable[RepairMarker]) -> tuple[Path, ...]:
    references: set[Path] = set()
    for marker in markers:
        for protected in marker.protected_paths:
            if is_relative_to(protected, path) or is_relative_to(path, protected):
                references.add(marker.path)
                break
    return tuple(sorted(references, key=os.fspath))


def candidate_names(live_root: Path, requested: list[str] | None) -> list[str]:
    if requested:
        duplicates = sorted({name for name in requested if requested.count(name) > 1})
        if duplicates:
            raise CleanupError(f"duplicate --candidate values: {', '.join(duplicates)}")
        return sorted(requested)
    names: list[str] = []
    try:
        entries = list(live_root.iterdir())
    except OSError as error:
        raise CleanupError(f"read live root {live_root}: {error}") from error
    for path in entries:
        if classify_non_production_name(path.name) is not None:
            names.append(path.name)
    return sorted(names)


def inspect_candidates(
    live_root: Path,
    names: Iterable[str],
    markers: Iterable[RepairMarker],
    max_entries: int,
) -> tuple[Candidate, ...]:
    candidates: list[Candidate] = []
    for name in names:
        category = classify_non_production_name(name)
        if category is None:
            raise CleanupError(
                f"candidate {name!r} is not in the narrow non-production allow-list; "
                "production capture relocation is intentionally unsupported"
            )
        path = live_root / name
        try:
            metadata = path.lstat()
        except FileNotFoundError as error:
            raise CleanupError(f"requested candidate is missing: {path}") from error
        except OSError as error:
            raise CleanupError(f"stat requested candidate {path}: {error}") from error
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise CleanupError(f"candidate must be a real direct-child directory: {path}")
        path = path.resolve(strict=True)
        candidates.append(
            Candidate(
                name=name,
                path=path,
                category=category,
                production_signals=production_signals(path),
                protected_by=overlapping_markers(path, markers),
                snapshot=snapshot_tree(path, max_entries),
            )
        )
    return tuple(candidates)


def parent_process_ids() -> set[int]:
    result = {os.getpid()}
    pid = os.getpid()
    while pid > 1:
        try:
            text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            tail = text[text.rfind(")") + 2 :].split()
            pid = int(tail[1])
        except (FileNotFoundError, PermissionError, ValueError, IndexError, OSError):
            break
        if pid in result:
            break
        result.add(pid)
    return result


def path_points_into(path_text: str, roots: Iterable[Path]) -> bool:
    if path_text.endswith(" (deleted)"):
        path_text = path_text[: -len(" (deleted)")]
    if not os.path.isabs(path_text):
        return False
    normalized = Path(os.path.normpath(path_text))
    return any(is_relative_to(normalized, root) for root in roots)


def proc_quiescence_scan(candidates: Iterable[Candidate]) -> dict[str, Any]:
    candidates = tuple(candidates)
    if not candidates:
        return {
            "method": "linux_proc_fd_inode_cmdline_maps_cwd_exe",
            "candidates": 0,
            "global_scope_complete": True,
            "matches": [],
        }
    proc_root = Path("/proc")
    if not proc_root.is_dir():
        raise CleanupError("apply safety audit requires Linux /proc")
    roots = tuple(candidate.path for candidate in candidates)
    target_inodes: dict[tuple[int, int], str] = {}
    for candidate in candidates:
        for inode in candidate.snapshot.inode_keys:
            target_inodes[inode] = str(candidate.path)
    root_bytes = tuple(os.fsencode(str(root)) for root in roots)
    ignored = parent_process_ids()
    denied = 0
    processes_readable = 0
    fd_links_readable = 0
    matches: list[dict[str, Any]] = []

    try:
        process_paths = sorted(
            (path for path in proc_root.iterdir() if path.name.isdigit()),
            key=lambda path: int(path.name),
        )
    except OSError as error:
        raise CleanupError(f"enumerate /proc: {error}") from error
    for process_path in process_paths:
        pid = int(process_path.name)
        if pid in ignored:
            continue
        try:
            command_line = (process_path / "cmdline").read_bytes()
            processes_readable += 1
            if command_line and any(root in command_line for root in root_bytes):
                matches.append({"pid": pid, "kind": "cmdline"})
        except FileNotFoundError:
            continue
        except PermissionError:
            denied += 1
        except OSError as error:
            if error.errno not in {2, 3}:
                denied += 1

        try:
            memory_maps = (process_path / "maps").read_bytes()
            if memory_maps and any(root in memory_maps for root in root_bytes):
                matches.append({"pid": pid, "kind": "memory_map"})
        except FileNotFoundError:
            continue
        except PermissionError:
            denied += 1
        except OSError as error:
            if error.errno not in {2, 3}:
                denied += 1

        for link_name in ("cwd", "exe"):
            link = process_path / link_name
            try:
                linked = os.readlink(link)
                metadata = link.stat()
            except FileNotFoundError:
                continue
            except PermissionError:
                denied += 1
                continue
            except OSError as error:
                if error.errno not in {2, 3}:
                    denied += 1
                continue
            matched = target_inodes.get((metadata.st_dev, metadata.st_ino))
            if matched is not None or path_points_into(linked, roots):
                matches.append(
                    {"pid": pid, "kind": link_name, "path": matched or linked}
                )

        fd_dir = process_path / "fd"
        try:
            descriptors = list(fd_dir.iterdir())
        except FileNotFoundError:
            continue
        except PermissionError:
            denied += 1
            continue
        except OSError as error:
            if error.errno not in {2, 3}:
                denied += 1
            continue
        for descriptor in descriptors:
            try:
                metadata = descriptor.stat()
                linked = os.readlink(descriptor)
                fd_links_readable += 1
            except FileNotFoundError:
                continue
            except PermissionError:
                denied += 1
                continue
            except OSError as error:
                if error.errno not in {2, 3}:
                    denied += 1
                continue
            matched = target_inodes.get((metadata.st_dev, metadata.st_ino))
            if matched is not None or path_points_into(linked, roots):
                matches.append(
                    {
                        "pid": pid,
                        "kind": "fd",
                        "fd": descriptor.name,
                        "path": matched or linked,
                    }
                )

    report = {
        "method": "linux_proc_fd_inode_cmdline_maps_cwd_exe",
        "candidates": len(candidates),
        "processes_readable": processes_readable,
        "fd_links_readable": fd_links_readable,
        "permission_denied_or_unreadable": denied,
        "global_scope_complete": denied == 0,
        "matches": matches,
    }
    if matches:
        raise CleanupError(f"candidate directories are active/open: {matches}")
    if denied:
        raise CleanupError(
            f"global /proc audit was incomplete ({denied} denied/unreadable entries); rerun as root"
        )
    return report


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_json_replace_atomic(path: Path, value: Any) -> None:
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def write_json_no_replace_atomic(path: Path, value: Any) -> None:
    if path.exists():
        raise CleanupError(f"receipt already exists: {path}")
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, 0o600)
        # A hard link publishes a fully fsynced file without overwriting a racing
        # receipt.  Both paths are in the same receipt directory/filesystem.
        os.link(temporary, path)
        temporary.unlink()
        fsync_directory(path.parent)
    except BaseException:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def marker_signature(markers: Iterable[RepairMarker]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(marker.path), marker.sha256) for marker in markers))


def candidate_receipt_value(
    candidate: Candidate, destination: Path | None, decision: str, reason: str
) -> dict[str, Any]:
    return {
        "name": candidate.name,
        "source": str(candidate.path),
        "destination": None if destination is None else str(destination),
        "category": candidate.category,
        "decision": decision,
        "reason": reason,
        "production_signals": list(candidate.production_signals),
        "protected_by_repair_markers": [str(path) for path in candidate.protected_by],
        "snapshot": candidate.snapshot.receipt_value(),
    }


def inventory_live_root(live_root: Path, markers: Iterable[RepairMarker]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        entries = sorted(live_root.iterdir(), key=lambda path: path.name)
    except OSError as error:
        raise CleanupError(f"inventory live root {live_root}: {error}") from error
    for path in entries:
        try:
            metadata = path.lstat()
        except OSError as error:
            raise CleanupError(f"stat live-root entry {path}: {error}") from error
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            kind = "non_directory"
        else:
            category = classify_non_production_name(path.name)
            signals = production_signals(path)
            protected = overlapping_markers(path, markers)
            if protected:
                kind = "retained_repair_bundle_or_source"
            elif signals:
                kind = "retained_production_capture"
            elif category:
                kind = f"quarantine_candidate_{category}"
            else:
                kind = "retained_unclassified"
        epoch_matches = sorted({int(value) for value in re.findall(r"epoch-([0-9]+)", path.name)})
        rows.append(
            {
                "name": path.name,
                "kind": kind,
                "epoch_hints": epoch_matches,
            }
        )
    return rows


def assert_candidate_age(candidate: Candidate, min_age_seconds: int) -> None:
    if min_age_seconds < 0:
        raise CleanupError("min-age-seconds must be non-negative")
    now_ns = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1_000_000_000)
    age_ns = max(0, now_ns - candidate.snapshot.newest_mtime_ns)
    minimum_ns = min_age_seconds * 1_000_000_000
    if age_ns < minimum_ns:
        raise CleanupError(
            f"candidate {candidate.path} changed {age_ns / 1_000_000_000:.1f}s ago; "
            f"minimum age is {min_age_seconds}s"
        )


def build_base_receipt(
    *,
    args: argparse.Namespace,
    live_root: Path,
    archive_root: Path,
    markers: tuple[RepairMarker, ...],
    candidates: tuple[Candidate, ...],
    inventory: list[dict[str, Any]],
    started_at: str,
) -> dict[str, Any]:
    return {
        "version": TOOL_VERSION,
        "tool": "nas-live-root-cleanup",
        "mode": args.mode,
        "state": "planned" if args.mode == "dry-run" else "applying",
        "run_id": args.run_id,
        "started_at_utc": started_at,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "uid": os.getuid(),
        "live_root": str(live_root),
        "archive_root": str(archive_root),
        "same_filesystem": live_root.stat().st_dev == archive_root.stat().st_dev,
        "policy": {
            "deletes_files": False,
            "moves_production_captures": False,
            "candidate_name_allow_list": [
                pattern.pattern for _, pattern in SAFE_CANDIDATE_PATTERNS
            ],
            "min_age_seconds": args.min_age_seconds,
            "max_candidate_entries": args.max_candidate_entries,
            "repair_references_are_retained": True,
        },
        "repair_markers": [marker.receipt_value() for marker in markers],
        "inventory": inventory,
        "candidates": [
            candidate_receipt_value(
                candidate,
                None,
                "blocked" if not candidate.eligible else "planned_quarantine",
                (
                    "repair manifest references this path"
                    if candidate.protected_by
                    else "capture-shaped production artifacts found"
                    if candidate.production_signals
                    else "narrow non-production allow-list match"
                ),
            )
            for candidate in candidates
        ],
    }


def revalidate(
    *,
    live_root: Path,
    original_markers: tuple[RepairMarker, ...],
    original_candidates: tuple[Candidate, ...],
    max_entries: int,
    min_age_seconds: int,
) -> tuple[tuple[RepairMarker, ...], tuple[Candidate, ...], dict[str, Any]]:
    markers = discover_repair_markers(live_root)
    if marker_signature(markers) != marker_signature(original_markers):
        raise CleanupError("repair marker set changed after planning")
    candidates = inspect_candidates(
        live_root,
        [candidate.name for candidate in original_candidates],
        markers,
        max_entries,
    )
    for before, after in zip(original_candidates, candidates, strict=True):
        if before.snapshot != after.snapshot:
            raise CleanupError(f"candidate changed after planning: {before.path}")
        if not after.eligible:
            raise CleanupError(f"candidate became protected or capture-shaped: {after.path}")
        assert_candidate_age(after, min_age_seconds)
    proc_report = proc_quiescence_scan(candidates)
    return markers, candidates, proc_report


def apply_cleanup(
    *,
    args: argparse.Namespace,
    live_root: Path,
    archive_root: Path,
    markers: tuple[RepairMarker, ...],
    candidates: tuple[Candidate, ...],
    base_receipt: dict[str, Any],
) -> Path:
    if not candidates:
        raise CleanupError("no quarantine candidates selected")
    blocked = [candidate for candidate in candidates if not candidate.eligible]
    if blocked:
        details = "; ".join(
            f"{candidate.name}: signals={list(candidate.production_signals)} "
            f"repair_markers={[str(path) for path in candidate.protected_by]}"
            for candidate in blocked
        )
        raise CleanupError(f"one or more requested candidates are protected: {details}")

    final = archive_root / args.run_id
    staging = archive_root / f".{args.run_id}.in-progress-{uuid.uuid4().hex}"
    if final.exists():
        raise CleanupError(f"archive destination already exists: {final}")
    if staging.exists():  # practically impossible, but keep publication no-replace
        raise CleanupError(f"transaction staging already exists: {staging}")
    staging.mkdir(mode=0o700)
    items = staging / "items"
    items.mkdir(mode=0o700)
    fsync_directory(staging)
    fsync_directory(archive_root)

    transaction_path = staging / "transaction.json"
    transaction: dict[str, Any] = {
        "version": TOOL_VERSION,
        "tool": "nas-live-root-cleanup",
        "state": "preflight",
        "run_id": args.run_id,
        "live_root": str(live_root),
        "intended_final_path": str(final),
        "moved": [],
        "remaining": [candidate.name for candidate in candidates],
        "rollback": [],
    }
    write_json_replace_atomic(transaction_path, transaction)

    moved: list[tuple[Candidate, Path]] = []
    published = False
    try:
        _, candidates, proc_report = revalidate(
            live_root=live_root,
            original_markers=markers,
            original_candidates=candidates,
            max_entries=args.max_candidate_entries,
            min_age_seconds=args.min_age_seconds,
        )
        transaction["state"] = "moving"
        transaction["proc_quiescence"] = proc_report
        write_json_replace_atomic(transaction_path, transaction)

        for candidate in candidates:
            destination = items / candidate.name
            if destination.exists():
                raise CleanupError(f"staging destination already exists: {destination}")
            os.rename(candidate.path, destination)
            fsync_directory(live_root)
            fsync_directory(items)
            moved.append((candidate, destination))
            transaction["moved"].append(candidate.name)
            transaction["remaining"].remove(candidate.name)
            write_json_replace_atomic(transaction_path, transaction)

            after = snapshot_tree(destination, args.max_candidate_entries)
            if after != candidate.snapshot:
                raise CleanupError(
                    f"candidate metadata changed across rename: {candidate.name}"
                )

        receipt = dict(base_receipt)
        receipt.update(
            {
                "state": "applied",
                "completed_at_utc": utc_now(),
                "archive_path": str(final),
                "receipt_path": str(final / "receipt.json"),
                "proc_quiescence": proc_report,
                "candidates": [
                    candidate_receipt_value(
                        candidate,
                        final / "items" / candidate.name,
                        "quarantined",
                        "same-filesystem atomic rename; no data deleted",
                    )
                    for candidate in candidates
                ],
            }
        )
        transaction["state"] = "ready_to_publish"
        write_json_replace_atomic(transaction_path, transaction)
        write_json_no_replace_atomic(staging / "receipt.json", receipt)
        fsync_directory(items)
        fsync_directory(staging)
        if final.exists():
            raise CleanupError(f"archive destination appeared during apply: {final}")
        os.rename(staging, final)
        published = True
        fsync_directory(archive_root)
        return final / "receipt.json"
    except BaseException as original_error:
        if published:
            raise CleanupError(
                f"cleanup was published at {final}, but final directory sync failed: "
                f"{original_error}; inspect the atomic receipt before retrying"
            ) from original_error
        rollback_errors: list[str] = []
        for candidate, destination in reversed(moved):
            try:
                if candidate.path.exists():
                    raise CleanupError(
                        f"cannot roll back {candidate.name}: source path was recreated"
                    )
                os.rename(destination, candidate.path)
                fsync_directory(items)
                fsync_directory(live_root)
                transaction["rollback"].append(candidate.name)
            except BaseException as rollback_error:
                rollback_errors.append(f"{candidate.name}: {rollback_error}")
        transaction["error"] = str(original_error)
        try:
            (staging / "receipt.json").unlink()
        except FileNotFoundError:
            pass
        except OSError as receipt_error:
            rollback_errors.append(f"remove unpublished receipt: {receipt_error}")
        transaction["state"] = "rolled_back" if not rollback_errors else "rollback_failed"
        transaction["rollback_errors"] = rollback_errors
        try:
            write_json_replace_atomic(transaction_path, transaction)
        except BaseException as journal_error:
            rollback_errors.append(f"transaction journal: {journal_error}")
        if not rollback_errors:
            # Keep the visible transaction journal as evidence, but no candidate
            # remains in it.  It can be removed after operator review.
            fsync_directory(staging)
        suffix = f"; rollback errors: {rollback_errors}" if rollback_errors else ""
        raise CleanupError(f"cleanup apply failed: {original_error}{suffix}") from original_error


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.min_age_seconds < 0:
        raise CleanupError("min-age-seconds must be non-negative")
    if args.max_candidate_entries <= 0:
        raise CleanupError("max-candidate-entries must be positive")
    live_root = resolve_directory(args.live_root, "live root")
    archive_root = resolve_directory(args.archive_root, "archive root")
    if is_relative_to(archive_root, live_root) or is_relative_to(live_root, archive_root):
        raise CleanupError("live root and archive root must not contain one another")
    if live_root.stat().st_dev != archive_root.stat().st_dev:
        raise CleanupError(
            "archive root is on a different filesystem; quarantine must use atomic rename"
        )

    started_at = utc_now()
    markers = discover_repair_markers(live_root)
    names = candidate_names(live_root, args.candidate)
    candidates = inspect_candidates(
        live_root, names, markers, args.max_candidate_entries
    )
    inventory = inventory_live_root(live_root, markers)
    base_receipt = build_base_receipt(
        args=args,
        live_root=live_root,
        archive_root=archive_root,
        markers=markers,
        candidates=candidates,
        inventory=inventory,
        started_at=started_at,
    )

    if args.mode == "dry-run":
        # A dry run still performs the same strict open-FD/global-process audit.
        for candidate in candidates:
            assert_candidate_age(candidate, args.min_age_seconds)
        blocked = [candidate for candidate in candidates if not candidate.eligible]
        proc_report = proc_quiescence_scan(
            candidate for candidate in candidates if candidate.eligible
        )
        receipt = dict(base_receipt)
        receipt.update(
            {
                "state": "dry_run_blocked" if blocked else "dry_run_safe",
                "completed_at_utc": utc_now(),
                "proc_quiescence": proc_report,
            }
        )
        receipt_path = archive_root / f"dry-run-{args.run_id}.receipt.json"
        write_json_no_replace_atomic(receipt_path, receipt)
    else:
        receipt_path = apply_cleanup(
            args=args,
            live_root=live_root,
            archive_root=archive_root,
            markers=markers,
            candidates=candidates,
            base_receipt=base_receipt,
        )

    print(
        json.dumps(
            {
                "mode": args.mode,
                "receipt": str(receipt_path),
                "candidates": len(candidates),
                "eligible": sum(candidate.eligible for candidate in candidates),
                "logical_bytes": sum(
                    candidate.snapshot.logical_bytes
                    for candidate in candidates
                    if candidate.eligible
                ),
                "allocated_bytes": sum(
                    candidate.snapshot.allocated_bytes
                    for candidate in candidates
                    if candidate.eligible
                ),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
