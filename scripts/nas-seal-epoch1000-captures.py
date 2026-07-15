#!/usr/bin/env python3
"""Create atomic, auditable capture-seal receipts for epoch repair.

The receipts are point-in-time evidence. Capture writers must remain stopped until
the Rust repair command publishes (or fails); the Rust command performs its own
end-of-run fingerprint validation.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import hashlib
import json
import os
import shutil
import socket
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ELIGIBLE_RUN_NAMES = ("hot-run.bin",)


@dataclass(frozen=True)
class CaptureSpec:
    name: str
    capture_dir: Path
    selected_blocks: int
    max_slot: int
    journal: Path
    pubkey_run_dir: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Atomically seal stopped capture slices for prepare-epoch-repair"
    )
    parser.add_argument("--epoch", type=int, required=True)
    parser.add_argument("--coverage-receipt", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--capture",
        action="append",
        nargs=6,
        metavar=("NAME", "DIR", "BLOCKS", "MAX_SLOT", "JOURNAL", "PUBKEY_RUNS"),
        required=True,
        help="repeat once per capture slice",
    )
    parser.add_argument(
        "--allow-incomplete-proc-scan",
        action="store_true",
        help="publish while recording inaccessible /proc processes; use only with a separate fuser check",
    )
    return parser.parse_args()


def resolve_existing(path: Path, *, directory: bool = False) -> Path:
    resolved = path.expanduser().resolve(strict=True)
    if directory and not resolved.is_dir():
        raise RuntimeError(f"not a directory: {resolved}")
    if not directory and not resolved.is_file():
        raise RuntimeError(f"not a regular file: {resolved}")
    return resolved


def fingerprint(path: Path) -> dict[str, int | str]:
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 0:
        raise RuntimeError(f"input must be a non-empty regular file: {path}")
    return {
        "path": str(path),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "bytes": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
        "mode": stat.S_IMODE(metadata.st_mode),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=1024 * 1024) as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def eligible_run_files(directory: Path) -> list[Path]:
    files = []
    for path in directory.iterdir():
        if not path.is_file():
            continue
        if path.name in ELIGIBLE_RUN_NAMES or (
            path.name.startswith("run-") and path.name.endswith(".bin")
        ):
            files.append(path.resolve(strict=True))
    files.sort(key=lambda value: value.name)
    if not files:
        raise RuntimeError(f"no eligible pubkey-run files in {directory}")
    return files


def run_summary(files: Iterable[Path]) -> dict[str, int | str]:
    digest = hashlib.sha256()
    count = 0
    total_bytes = 0
    for path in files:
        row = fingerprint(path)
        canonical = {
            "name": path.name,
            "device": row["device"],
            "inode": row["inode"],
            "bytes": row["bytes"],
            "mtime_ns": row["mtime_ns"],
        }
        digest.update(
            json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
        )
        digest.update(b"\n")
        count += 1
        total_bytes += int(row["bytes"])
    return {
        "eligible_files": count,
        "total_bytes": total_bytes,
        "metadata_rows_sha256": digest.hexdigest(),
    }


def directory_fingerprint(path: Path) -> dict[str, int | str]:
    metadata = path.stat()
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError(f"not a directory: {path}")
    return {
        "path": str(path),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "mtime_ns": metadata.st_mtime_ns,
        "mode": stat.S_IMODE(metadata.st_mode),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
    }


def parent_processes() -> set[int]:
    result = {os.getpid()}
    pid = os.getpid()
    while pid > 1:
        try:
            text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
            tail = text[text.rfind(")") + 2 :].split()
            pid = int(tail[1])
        except (FileNotFoundError, PermissionError, ValueError, IndexError):
            break
        if pid in result:
            break
        result.add(pid)
    return result


def proc_quiescence_scan(
    files: Iterable[Path], path_patterns: Iterable[Path]
) -> dict[str, int | bool | str]:
    target_inodes = {
        (path.stat().st_dev, path.stat().st_ino): str(path) for path in files
    }
    patterns = [os.fsencode(str(path)) for path in path_patterns]
    ignored = parent_processes()
    process_dirs_readable = 0
    process_dirs_denied = 0
    fd_links_readable = 0
    fd_links_denied = 0
    command_lines_readable = 0
    command_lines_denied = 0
    open_matches: list[dict[str, int | str]] = []
    argv_matches: list[dict[str, int | str]] = []

    for proc_dir_text in glob.glob("/proc/[0-9]*"):
        proc_dir = Path(proc_dir_text)
        try:
            pid = int(proc_dir.name)
        except ValueError:
            continue
        if pid in ignored:
            continue
        try:
            command_line = (proc_dir / "cmdline").read_bytes()
            command_lines_readable += 1
            if command_line and any(pattern in command_line for pattern in patterns):
                argv_matches.append({"pid": pid, "uid": proc_dir.stat().st_uid})
        except FileNotFoundError:
            continue
        except PermissionError:
            command_lines_denied += 1

        fd_dir = proc_dir / "fd"
        try:
            fd_names = list(fd_dir.iterdir())
            process_dirs_readable += 1
        except FileNotFoundError:
            continue
        except PermissionError:
            process_dirs_denied += 1
            continue
        for fd_path in fd_names:
            try:
                metadata = fd_path.stat()
                fd_links_readable += 1
            except FileNotFoundError:
                continue
            except PermissionError:
                fd_links_denied += 1
                continue
            matched = target_inodes.get((metadata.st_dev, metadata.st_ino))
            if matched is not None:
                open_matches.append({"pid": pid, "fd": fd_path.name, "path": matched})

    if open_matches:
        raise RuntimeError(f"capture inputs have open file descriptors: {open_matches}")
    if argv_matches:
        raise RuntimeError(f"capture paths occur in active process arguments: {argv_matches}")
    return {
        "method": "proc_fd_device_inode_and_cmdline",
        "process_dirs_readable": process_dirs_readable,
        "process_dirs_denied_or_raced": process_dirs_denied,
        "fd_links_readable": fd_links_readable,
        "fd_links_denied": fd_links_denied,
        "command_lines_readable": command_lines_readable,
        "command_lines_denied": command_lines_denied,
        "global_scope_complete": process_dirs_denied == 0
        and fd_links_denied == 0
        and command_lines_denied == 0,
    }


def fuser_quiescence(files: list[Path]) -> dict[str, int | str | bool]:
    fuser = shutil.which("fuser")
    if fuser is None:
        raise RuntimeError("fuser is unavailable")
    batches = 0
    for start in range(0, len(files), 64):
        batch = files[start : start + 64]
        result = subprocess.run(
            [fuser, "-s", *map(str, batch)],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            raise RuntimeError("fuser found an active user of at least one capture input")
        if result.returncode != 1:
            raise RuntimeError(f"fuser failed with exit code {result.returncode}")
        batches += 1
    return {
        "method": "fuser_silent_batches",
        "binary": fuser,
        "files_checked": len(files),
        "batches": batches,
        "no_users_found": True,
    }


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def stable_fingerprints(files: list[Path]) -> list[dict[str, int | str]]:
    return [fingerprint(path) for path in files]


def main() -> int:
    args = parse_args()
    if args.epoch < 0:
        raise RuntimeError("epoch must be non-negative")
    coverage = resolve_existing(args.coverage_receipt)
    output_dir = args.output_dir.expanduser().absolute()
    output_parent = output_dir.parent.resolve(strict=True)
    if output_dir.exists():
        raise RuntimeError(f"receipt output already exists: {output_dir}")

    captures: list[CaptureSpec] = []
    names: set[str] = set()
    for raw in args.capture:
        name, capture_dir, blocks, max_slot, journal, pubkey_runs = raw
        if name in names or not name.replace("-", "_").isalnum():
            raise RuntimeError(f"invalid or duplicate capture name: {name}")
        names.add(name)
        selected_blocks = int(blocks)
        selected_max_slot = int(max_slot)
        if selected_blocks <= 0 or selected_max_slot < 0:
            raise RuntimeError(f"invalid capture count/cutoff for {name}")
        captures.append(
            CaptureSpec(
                name=name,
                capture_dir=resolve_existing(Path(capture_dir), directory=True),
                selected_blocks=selected_blocks,
                max_slot=selected_max_slot,
                journal=resolve_existing(Path(journal)),
                pubkey_run_dir=resolve_existing(Path(pubkey_runs), directory=True),
            )
        )

    capture_files: dict[str, list[Path]] = {}
    pubkey_files: dict[str, list[Path]] = {}
    all_files = [coverage]
    path_patterns = [coverage]
    for capture in captures:
        files = [
            resolve_existing(capture.capture_dir / "blocks/live-no-registry-blocks.bin"),
            resolve_existing(capture.capture_dir / "index/block-index.bin"),
            resolve_existing(capture.capture_dir / "poh/poh.wincode"),
            resolve_existing(capture.capture_dir / "index/blockhash_registry.bin"),
            capture.journal,
        ]
        runs = eligible_run_files(capture.pubkey_run_dir)
        capture_files[capture.name] = files
        pubkey_files[capture.name] = runs
        all_files.extend(files)
        all_files.extend(runs)
        path_patterns.extend(
            [capture.capture_dir, capture.journal, capture.pubkey_run_dir]
        )

    before = stable_fingerprints(all_files)
    run_directories_before = {
        capture.name: directory_fingerprint(capture.pubkey_run_dir)
        for capture in captures
    }
    coverage_sha256 = sha256_file(coverage)
    first_proc = proc_quiescence_scan(all_files, path_patterns)
    if not first_proc["global_scope_complete"] and not args.allow_incomplete_proc_scan:
        raise RuntimeError(
            "global /proc scan is incomplete; rerun as root or explicitly pass "
            "--allow-incomplete-proc-scan"
        )
    first_fuser = fuser_quiescence(all_files)
    sealed_at = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except (FileNotFoundError, PermissionError):
        boot_id = "unavailable"

    receipts: dict[str, dict[str, object]] = {}
    for capture in captures:
        receipts[capture.name] = {
            "version": 1,
            "state": f"audited_sealed_for_epoch{args.epoch}_repair",
            "epoch": args.epoch,
            "capture": {
                "name": capture.name,
                "path": str(capture.capture_dir),
                "selected_blocks": capture.selected_blocks,
                "max_slot": capture.max_slot,
                "journal": str(capture.journal),
                "pubkey_run_dir": str(capture.pubkey_run_dir),
            },
            "tracked_files": [fingerprint(path) for path in capture_files[capture.name]],
            "pubkey_runs": run_summary(pubkey_files[capture.name]),
            "pubkey_run_directory": run_directories_before[capture.name],
            "coverage_receipt": {
                **fingerprint(coverage),
                "sha256": coverage_sha256,
            },
            "sealed_at_utc": sealed_at,
            "host": socket.gethostname(),
            "boot_id": boot_id,
            "creator_uid": os.getuid(),
            "quiescence": {
                "proc": first_proc,
                "fuser": first_fuser,
                "scope_override_used": not first_proc["global_scope_complete"],
                "point_in_time_only": True,
                "writers_must_remain_stopped_until_repair_publication": True,
            },
        }

    staging = output_parent / f".{output_dir.name}.stage-{os.getpid()}"
    if staging.exists():
        raise RuntimeError(f"stale receipt staging directory exists: {staging}")
    run_owner = output_parent.stat()
    try:
        staging.mkdir(mode=0o700)
        if os.getuid() == 0:
            os.chown(staging, run_owner.st_uid, run_owner.st_gid)
        for name, receipt in receipts.items():
            target = staging / f"{name}.json"
            descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                json.dump(receipt, output, indent=2, sort_keys=True)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.chmod(target, 0o600)
            if os.getuid() == 0:
                os.chown(target, run_owner.st_uid, run_owner.st_gid)
        fsync_directory(staging)

        second_proc = proc_quiescence_scan(all_files, path_patterns)
        if not second_proc["global_scope_complete"] and not args.allow_incomplete_proc_scan:
            raise RuntimeError("global /proc scan became incomplete before publication")
        fuser_quiescence(all_files)
        after = stable_fingerprints(all_files)
        if before != after:
            raise RuntimeError("capture input fingerprints changed during receipt creation")
        for capture in captures:
            if directory_fingerprint(capture.pubkey_run_dir) != run_directories_before[capture.name]:
                raise RuntimeError(
                    f"pubkey-run directory metadata changed during receipt creation: {capture.pubkey_run_dir}"
                )
            if eligible_run_files(capture.pubkey_run_dir) != pubkey_files[capture.name]:
                raise RuntimeError(
                    f"pubkey-run directory contents changed during receipt creation: {capture.pubkey_run_dir}"
                )
        if sha256_file(coverage) != coverage_sha256:
            raise RuntimeError("coverage receipt changed during receipt creation")

        os.rename(staging, output_dir)
        fsync_directory(output_parent)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise

    report = {
        "output_dir": str(output_dir),
        "receipts": [str(output_dir / f"{capture.name}.json") for capture in captures],
        "coverage_sha256": coverage_sha256,
        "proc_global_scope_complete": bool(first_proc["global_scope_complete"]),
        "proc_scope_override_used": not bool(first_proc["global_scope_complete"]),
        "files_checked": len(all_files),
    }
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
