#!/usr/bin/env python3
"""Read-only Agave snapshot inventory, retention plan and disk budget.

This tool has no deletion or service-control operation. File names identify
completed archives; this tool does not verify their contents. Retirement needs
a separate approved action with fresh file identity and open-file checks.
"""

import argparse
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import stat


GIB = 1024**3
DEFAULT_NEXT_ARCHIVE_BYTES = 128 * GIB
FULL_FLAG = "--maximum-full-snapshots-to-retain"
INCREMENTAL_FLAG = "--maximum-incremental-snapshots-to-retain"
BOUNDED_RETENTION_ARGS = (FULL_FLAG, "1", INCREMENTAL_FLAG, "1")
HASH = r"[1-9A-HJ-NP-Za-km-z]{32,44}"
FORMAT = r"tar\.(?:zst|lz4)"
FULL_NAME = re.compile(rf"snapshot-(\d+)-{HASH}\.{FORMAT}\Z")
INCREMENTAL_NAME = re.compile(rf"incremental-snapshot-(\d+)-(\d+)-{HASH}\.{FORMAT}\Z")
TEMP_NAME = re.compile(rf"tmp-snapshot-archive-\d+\.{FORMAT}\Z")


def bounded_snapshot_args(argv, help_text):
    """Return a proposed argv with explicit 1/1 retention, after help checks.

    Use help from the exact binary that will start. Agave applies retention
    after archive completion, so these flags do not create write headroom.
    """
    for flag in (FULL_FLAG, INCREMENTAL_FLAG):
        if not re.search(rf"(?m)^\s*{re.escape(flag)}\s+<NUMBER>\s*$", help_text):
            raise ValueError(f"pinned binary help does not support {flag}")
    if "--no-snapshots" in argv or "--no-incremental-snapshots" in argv:
        raise ValueError("snapshot generation differs from the 1/1 retention policy")
    result = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        option, equal, value = argument.partition("=")
        if option in (FULL_FLAG, INCREMENTAL_FLAG):
            if not equal:
                index += 1
                if index >= len(argv):
                    raise ValueError(f"missing value for {option}")
                value = argv[index]
            if not value.isdigit() or int(value) < 1:
                raise ValueError(f"invalid retention value for {option}")
        else:
            result.append(argument)
        index += 1
    return result + list(BOUNDED_RETENTION_ARGS)


@dataclass(frozen=True)
class SnapshotFile:
    name: str
    kind: str
    slot: int | None
    base_slot: int | None
    bytes: int
    allocated_bytes: int
    device: int
    inode: int
    mtime_ns: int


def inventory_snapshots(directory):
    """Read immediate entries only. Preserve symlinks, directories and unknowns."""
    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("snapshot directory must be a directory, with no final symlink")
    records = []
    for path in sorted(directory.iterdir(), key=lambda item: item.name):
        info = path.lstat()
        kind, slot, base_slot = "other", None, None
        if stat.S_ISREG(info.st_mode):
            if info.st_size > 0 and (match := FULL_NAME.fullmatch(path.name)):
                kind, slot = "full", int(match[1])
            elif info.st_size > 0 and (match := INCREMENTAL_NAME.fullmatch(path.name)):
                base_slot, slot = int(match[1]), int(match[2])
                if slot > base_slot:
                    kind = "incremental"
            elif TEMP_NAME.fullmatch(path.name):
                kind = "active_temporary"
        records.append(SnapshotFile(
            name=path.name, kind=kind, slot=slot, base_slot=base_slot,
            bytes=info.st_size, allocated_bytes=info.st_blocks * 512,
            device=info.st_dev, inode=info.st_ino, mtime_ns=info.st_mtime_ns))
    return records


def snapshot_plan(records, free_bytes, capture_reserve_bytes=400 * GIB,
                  recovery_minimum_free_bytes=500 * GIB, next_archive_bytes=None):
    """Plan retention by slot, with all equal newest identities retained.

    One full and one matching incremental are the target. Ambiguous equal-slot
    archives stay retained. Temporary and unknown paths are never candidates.
    The next-write estimate is an observed floor, not a future size guarantee.
    """
    numeric_inputs = [free_bytes, capture_reserve_bytes, recovery_minimum_free_bytes]
    if next_archive_bytes is not None:
        numeric_inputs.append(next_archive_bytes)
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 0
           for value in numeric_inputs):
        raise ValueError("disk budgets must be nonnegative integer bytes")
    if recovery_minimum_free_bytes < capture_reserve_bytes:
        raise ValueError("recovery minimum must cover the capture reserve")
    if next_archive_bytes == 0:
        raise ValueError("next archive budget must be greater than zero")
    records = sorted(records, key=lambda item: item.name)
    if len({record.name for record in records}) != len(records):
        raise ValueError("snapshot inventory contains duplicate path names")
    if any(record.bytes < 0 or record.allocated_bytes < 0 for record in records):
        raise ValueError("snapshot inventory contains invalid file sizes")
    fulls = [record for record in records if record.kind == "full"]
    full_slot = max((record.slot for record in fulls), default=None)
    newest_fulls = [record for record in fulls if record.slot == full_slot]
    matching = [record for record in records
                if record.kind == "incremental" and record.base_slot == full_slot]
    incremental_slot = max((record.slot for record in matching), default=None)
    newest_incrementals = [record for record in matching if record.slot == incremental_slot]
    protected = {record.name for record in newest_fulls + newest_incrementals}
    candidates = [record for record in records
                  if full_slot is not None and record.kind in ("full", "incremental")
                  and record.name not in protected]
    candidate_names = {record.name for record in candidates}
    retained = [record for record in records if record.name not in candidate_names]
    observed_write_floor = max((record.allocated_bytes for record in records
                               if record.kind in ("full", "incremental", "active_temporary")),
                              default=0)
    configured_write_budget = (DEFAULT_NEXT_ARCHIVE_BYTES if next_archive_bytes is None
                               else next_archive_bytes)
    write_budget = max(observed_write_floor, configured_write_budget)
    release_bytes = sum(record.allocated_bytes for record in candidates)
    projected_free = free_bytes + release_bytes
    pressure_threshold = capture_reserve_bytes + write_budget
    recovery_with_write_threshold = recovery_minimum_free_bytes + write_budget
    warnings = ["Next archive size can exceed the observed write budget.",
                "Archive names do not verify archive contents or restart recovery.",
                "Retirement needs approval and fresh file identity and open-file checks."]
    if len(newest_fulls) > 1 or len(newest_incrementals) > 1:
        warnings.append("Equal newest slots have multiple archive identities; all are retained.")
    if not fulls:
        warnings.append("No completed full archive was found; no retirement is proposed.")
    return {
        "schema": "blockzilla-agave-snapshot-budget-v1",
        "read_only": True,
        "plan_state": "REVIEW_REQUIRED" if fulls else "BLOCKED_NO_COMPLETED_FULL",
        "archive_contents_verified": False,
        "bounded_retention_args": list(BOUNDED_RETENTION_ARGS),
        "latest_full_slot": full_slot,
        "latest_matching_incremental_slot": incremental_slot,
        "retained": [asdict(record) for record in retained],
        "retirement_candidates_require_approval": [asdict(record) for record in candidates],
        "candidate_release_allocated_bytes": release_bytes,
        "current_free_bytes": free_bytes,
        "projected_free_after_candidate_retirement_bytes": projected_free,
        "capture_reserve_bytes": capture_reserve_bytes,
        "recovery_minimum_free_bytes": recovery_minimum_free_bytes,
        "observed_next_archive_floor_bytes": observed_write_floor,
        "configured_next_archive_floor_bytes": configured_write_budget,
        "next_archive_budget_bytes": write_budget,
        "pressure_threshold_free_bytes": pressure_threshold,
        "pressure_before_next_archive": free_bytes < pressure_threshold,
        "recovery_with_next_archive_threshold_free_bytes": recovery_with_write_threshold,
        "startup_disk_gate_passed": free_bytes >= recovery_with_write_threshold,
        "projected_recovery_minimum_met": bool(fulls) and projected_free >= recovery_minimum_free_bytes,
        "projected_recovery_with_next_archive_budget_met": bool(fulls) and projected_free >= recovery_with_write_threshold,
        "warnings": warnings,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshots", type=Path, required=True)
    parser.add_argument("--free-bytes", type=int,
                        help="Use a recorded free-space sample instead of statvfs")
    parser.add_argument("--check-start", action="store_true",
                        help="Check current free space for validator startup; exit 75 if budget is insufficient")
    parser.add_argument("--capture-reserve-bytes", type=int, default=400 * GIB)
    parser.add_argument("--recovery-minimum-free-bytes", type=int, default=500 * GIB)
    parser.add_argument("--next-archive-bytes", type=int,
                        help="Configured next-archive floor, default 128 GiB; cannot lower the observed floor")
    args = parser.parse_args(argv)
    if args.check_start and args.free_bytes is not None:
        parser.error("--check-start requires current statvfs space; do not use --free-bytes")
    try:
        records = inventory_snapshots(args.snapshots)
        if args.free_bytes is None:
            usage = os.statvfs(args.snapshots)
            free_bytes = usage.f_bavail * usage.f_frsize
        else:
            free_bytes = args.free_bytes
        result = snapshot_plan(records, free_bytes, args.capture_reserve_bytes,
                               args.recovery_minimum_free_bytes, args.next_archive_bytes)
    except (OSError, ValueError) as error:
        print(json.dumps({"schema": "blockzilla-agave-snapshot-budget-v1", "read_only": True,
                          "startup_disk_gate_passed": False, "error": str(error)}))
        return 75 if args.check_start else 1
    result["directory"] = str(args.snapshots.absolute())
    result["free_space_source"] = "statvfs" if args.free_bytes is None else "recorded_sample"
    result["check_start"] = args.check_start
    print(json.dumps(result, indent=2, sort_keys=True))
    return 75 if args.check_start and not result["startup_disk_gate_passed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
