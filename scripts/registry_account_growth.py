#!/usr/bin/env python3
"""Compute per-epoch account counts and exact first-seen growth from registry.bin.

The registry files are large, so exact growth is computed with disk buckets:

1. scan registry.bin files once and write fixed-size bucket records;
2. reduce one bucket at a time in memory to find each account's first epoch;
3. write a CSV with per-epoch active accounts, new accounts, and global unique.

The fast metadata path only reads file sizes. The exact path can be resumed by
re-running with the same --work-dir.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import struct
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Iterable


SLOTS_PER_EPOCH = 432_000
KEY_LEN = 32
EPOCH_LEN = 4
RECORD_LEN = KEY_LEN + EPOCH_LEN


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-root", required=True, type=Path)
    parser.add_argument("--out-csv", required=True, type=Path)
    parser.add_argument("--work-dir", type=Path)
    parser.add_argument("--start-epoch", type=int, default=0)
    parser.add_argument("--end-epoch", type=int)
    parser.add_argument("--exact", action="store_true", help="Compute exact growth with disk buckets.")
    parser.add_argument("--bucket-bits", type=int, default=13, help="2^N buckets for exact mode.")
    parser.add_argument("--buffer-mib", type=int, default=1, help="Per-bucket write buffer size.")
    parser.add_argument("--open-files", type=int, default=128, help="LRU open bucket files.")
    parser.add_argument(
        "--logs-root",
        type=Path,
        action="append",
        default=[],
        help="Optional run/log root used to extract tx counts from epoch logs.",
    )
    parser.add_argument("--force-bucketize", action="store_true")
    parser.add_argument("--force-reduce", action="store_true")
    return parser.parse_args()


def epoch_from_dir(path: Path) -> int | None:
    match = re.fullmatch(r"epoch-(\d+)", path.name)
    return int(match.group(1)) if match else None


def iter_epochs(root: Path, start: int, end: int | None) -> list[tuple[int, Path]]:
    rows: list[tuple[int, Path]] = []
    for epoch_dir in root.glob("epoch-*"):
        epoch = epoch_from_dir(epoch_dir)
        if epoch is None or epoch < start or (end is not None and epoch > end):
            continue
        registry = epoch_dir / "registry.bin"
        if registry.is_file() and registry.stat().st_size > 0:
            rows.append((epoch, registry))
    return sorted(rows)


def count_registry(path: Path) -> int:
    size = path.stat().st_size
    if size % KEY_LEN:
        raise ValueError(f"{path} size {size} is not divisible by {KEY_LEN}")
    return size // KEY_LEN


def count_blockhashes(epoch_dir: Path) -> int:
    path = epoch_dir / "blockhash_registry.bin"
    if not path.is_file():
        return 0
    size = path.stat().st_size
    if size % KEY_LEN:
        return 0
    return size // KEY_LEN


def parse_tx_counts(log_roots: Iterable[Path]) -> dict[int, int]:
    tx_counts: dict[int, int] = {}
    pattern = re.compile(r"blocks=(\d+)\s+txs=(\d+)")
    for root in log_roots:
        if not root.exists():
            continue
        for log in root.rglob("epoch-*.log"):
            match_epoch = re.search(r"epoch-(\d+)", log.name)
            if not match_epoch:
                continue
            epoch = int(match_epoch.group(1))
            try:
                text = log.read_text(errors="replace")
            except OSError:
                continue
            matches = list(pattern.finditer(text))
            if matches:
                tx_counts[epoch] = int(matches[-1].group(2))
    return tx_counts


class BucketWriter:
    def __init__(self, bucket_dir: Path, bucket_count: int, buffer_limit: int, max_open: int):
        self.bucket_dir = bucket_dir
        self.bucket_count = bucket_count
        self.buffer_limit = buffer_limit
        self.max_open = max_open
        self.buffers: dict[int, bytearray] = {}
        self.files: OrderedDict[int, object] = OrderedDict()
        bucket_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, bucket: int) -> Path:
        shard = bucket >> 8
        path = self.bucket_dir / f"{shard:02x}"
        path.mkdir(exist_ok=True)
        width = max(1, ((self.bucket_count - 1).bit_length() + 3) // 4)
        return path / f"bucket-{bucket:0{width}x}.bin"

    def _file(self, bucket: int):
        handle = self.files.get(bucket)
        if handle is not None:
            self.files.move_to_end(bucket)
            return handle
        if len(self.files) >= self.max_open:
            old_bucket, old_handle = self.files.popitem(last=False)
            old_handle.close()
        handle = self._path(bucket).open("ab")
        self.files[bucket] = handle
        return handle

    def add(self, bucket: int, record: bytes) -> None:
        buf = self.buffers.setdefault(bucket, bytearray())
        buf.extend(record)
        if len(buf) >= self.buffer_limit:
            self.flush_bucket(bucket)

    def flush_bucket(self, bucket: int) -> None:
        buf = self.buffers.get(bucket)
        if not buf:
            return
        self._file(bucket).write(buf)
        buf.clear()

    def close(self) -> None:
        for bucket in list(self.buffers):
            self.flush_bucket(bucket)
        for handle in self.files.values():
            handle.close()
        self.files.clear()


def bucket_for_key(key: bytes, bucket_count: int) -> int:
    # Pubkeys are already high-entropy. Use the first bytes; avoid hashing billions of keys.
    value = int.from_bytes(key[:4], "little")
    return value & (bucket_count - 1)


def bucketize(rows: list[tuple[int, Path]], work_dir: Path, bucket_bits: int, buffer_mib: int, open_files: int) -> None:
    bucket_count = 1 << bucket_bits
    bucket_dir = work_dir / "buckets"
    done = work_dir / "bucketize.done"
    if done.exists():
        return
    if bucket_dir.exists():
        for path in bucket_dir.rglob("bucket-*.bin"):
            path.unlink()
    writer = BucketWriter(bucket_dir, bucket_count, buffer_mib << 20, open_files)
    record_count = 0
    try:
        for epoch, registry in rows:
            print(f"bucketize epoch={epoch} registry={registry}", file=sys.stderr, flush=True)
            with registry.open("rb", buffering=64 << 20) as handle:
                while True:
                    chunk = handle.read(64 << 20)
                    if not chunk:
                        break
                    if len(chunk) % KEY_LEN:
                        raise ValueError(f"{registry} read non-key-aligned chunk")
                    packed_epoch = struct.pack("<I", epoch)
                    for offset in range(0, len(chunk), KEY_LEN):
                        key = chunk[offset : offset + KEY_LEN]
                        writer.add(bucket_for_key(key, bucket_count), key + packed_epoch)
                        record_count += 1
        writer.close()
    finally:
        writer.close()
    (work_dir / "bucketize.meta.json").write_text(
        json.dumps({"bucket_bits": bucket_bits, "bucket_count": bucket_count, "records": record_count}, indent=2)
    )
    done.write_text("ok\n")


def reduce_buckets(work_dir: Path, force: bool = False) -> dict[int, int]:
    out = work_dir / "new_accounts_by_epoch.json"
    if out.exists() and not force:
        return {int(k): int(v) for k, v in json.loads(out.read_text()).items()}

    new_by_epoch: dict[int, int] = {}
    bucket_paths = sorted((work_dir / "buckets").rglob("bucket-*.bin"))
    for idx, path in enumerate(bucket_paths, 1):
        size = path.stat().st_size
        if size % RECORD_LEN:
            raise ValueError(f"{path} size {size} is not divisible by {RECORD_LEN}")
        print(f"reduce {idx}/{len(bucket_paths)} {path} records={size // RECORD_LEN}", file=sys.stderr, flush=True)
        first_seen: dict[bytes, int] = {}
        with path.open("rb", buffering=64 << 20) as handle:
            while True:
                chunk = handle.read(RECORD_LEN * 256_000)
                if not chunk:
                    break
                if len(chunk) % RECORD_LEN:
                    raise ValueError(f"{path} read non-record-aligned chunk")
                for offset in range(0, len(chunk), RECORD_LEN):
                    key = chunk[offset : offset + KEY_LEN]
                    epoch = struct.unpack_from("<I", chunk, offset + KEY_LEN)[0]
                    previous = first_seen.get(key)
                    if previous is None or epoch < previous:
                        first_seen[key] = epoch
        for epoch in first_seen.values():
            new_by_epoch[epoch] = new_by_epoch.get(epoch, 0) + 1
    out.write_text(json.dumps(dict(sorted(new_by_epoch.items())), indent=2))
    return new_by_epoch


def write_csv(
    out_csv: Path,
    rows: list[tuple[int, Path]],
    new_by_epoch: dict[int, int] | None,
    tx_counts: dict[int, int],
) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cumulative = 0
    with out_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "epoch",
                "first_slot",
                "last_slot",
                "account_count",
                "active_account_count",
                "tx_count",
                "blockhash_count",
                "unique_account_growth",
                "global_unique_accounts",
                "new_account_ratio",
                "active_to_global_unique_ratio",
                "registry_path",
            ],
        )
        writer.writeheader()
        for epoch, registry in rows:
            growth = ""
            global_unique = ""
            new_account_ratio = ""
            active_to_global_unique_ratio = ""
            account_count = count_registry(registry)
            if new_by_epoch is not None:
                growth_value = new_by_epoch.get(epoch, 0)
                cumulative += growth_value
                growth = growth_value
                global_unique = cumulative
                if account_count:
                    new_account_ratio = f"{growth_value / account_count:.12g}"
                if cumulative:
                    active_to_global_unique_ratio = f"{account_count / cumulative:.12g}"
            writer.writerow(
                {
                    "epoch": epoch,
                    "first_slot": epoch * SLOTS_PER_EPOCH,
                    "last_slot": epoch * SLOTS_PER_EPOCH + SLOTS_PER_EPOCH - 1,
                    "account_count": account_count,
                    "active_account_count": account_count,
                    "tx_count": tx_counts.get(epoch, ""),
                    "blockhash_count": count_blockhashes(registry.parent),
                    "unique_account_growth": growth,
                    "global_unique_accounts": global_unique,
                    "new_account_ratio": new_account_ratio,
                    "active_to_global_unique_ratio": active_to_global_unique_ratio,
                    "registry_path": str(registry),
                }
            )


def main() -> int:
    args = parse_args()
    if args.exact and args.work_dir is None:
        print("--exact requires --work-dir", file=sys.stderr)
        return 2
    rows = iter_epochs(args.registry_root, args.start_epoch, args.end_epoch)
    if not rows:
        print(f"no registry.bin files found under {args.registry_root}", file=sys.stderr)
        return 1
    tx_counts = parse_tx_counts(args.logs_root)
    new_by_epoch = None
    if args.exact:
        work_dir = args.work_dir
        work_dir.mkdir(parents=True, exist_ok=True)
        if args.force_bucketize:
            done = work_dir / "bucketize.done"
            if done.exists():
                done.unlink()
        bucketize(rows, work_dir, args.bucket_bits, args.buffer_mib, args.open_files)
        new_by_epoch = reduce_buckets(work_dir, force=args.force_reduce)
    write_csv(args.out_csv, rows, new_by_epoch, tx_counts)
    print(f"wrote {args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
