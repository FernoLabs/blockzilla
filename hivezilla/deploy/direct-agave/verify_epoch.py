#!/usr/bin/env python3
"""Read-only native epoch continuity and PoH check, with a separate durable result DB."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time


def require(condition, message):
    if not condition:
        raise ValueError(message)


def atomic_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def bounded_json(path):
    require(path.is_file() and not path.is_symlink(), f"invalid receipt: {path}")
    require(path.stat().st_size <= 16 << 20, f"receipt too large: {path}")
    raw = path.read_bytes()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def inventory(output, epoch, state):
    start, end = epoch * 432000, (epoch + 1) * 432000
    database = sqlite3.connect(f"file:{output}/committed.sqlite?mode=ro", uri=True)
    database.execute("PRAGMA query_only=ON")
    grouped = {}
    for slot, artifact in database.execute(
        "SELECT slot,artifact FROM commits WHERE slot>=? AND slot<? ORDER BY slot", (start, end)
    ):
        grouped.setdefault(artifact, []).append(slot)
    require(grouped, "epoch has no committed blocks")
    jobs = []
    previous = None
    first = None
    totals = dict(blocks=0, transactions=0, signatures=0, entries=0, skipped_slots=0)
    gaps = 0
    started = time.monotonic()
    last_progress = started
    for name, slots in grouped.items():
        artifact = Path(name)
        require(artifact.parent == output / f"epoch-{epoch}" and artifact.resolve() == artifact,
                f"artifact outside pinned epoch: {artifact}")
        receipt, receipt_sha = bounded_json(artifact / "DIRECT-AGAVE-NORMALIZED.v1.json")
        marker, marker_sha = bounded_json(artifact / "DIRECT-AGAVE-COMPACT.v1.json")
        require(receipt["schema"] == "hivezilla-direct-agave-normalized-v1" and
                marker["schema"] == "hivezilla-direct-agave-compact-v1" and
                marker["state"] == "COMPLETE" and receipt["epoch"] == epoch and
                receipt["finalized"] is True and marker["finalized"] is True and
                receipt["canonical"] is False and marker["canonical"] is False and
                marker["publication_ready"] is False, f"invalid native commit: {name}")
        for key in ("session_id", "agave_revision", "genesis_hash", "node_identity", "epoch",
                    "start_slot", "end_slot_exclusive", "source_profile"):
            require(receipt[key] == marker[key], f"receipt/marker mismatch: {key}: {name}")
        require(receipt["session_id"] == output.name and
                receipt["source_profile"] == "agave-bank-callbacks-v1" and
                marker["source_receipt_sha256"] == receipt_sha, f"native source mismatch: {name}")
        blocks = receipt["blocks"]
        require(blocks == marker["produced_blocks"] and len(blocks) == marker["blocks"] and
                [b["slot"] for b in blocks] == slots, f"commit inventory mismatch: {name}")
        require(blocks[0]["slot"] == receipt["start_slot"] and
                blocks[-1]["slot"] + 1 == receipt["end_slot_exclusive"], f"range mismatch: {name}")
        registry = (artifact / "blockhash_registry.bin").read_bytes()
        require(len(registry) == 32 * len(blocks), f"registry length mismatch: {name}")
        tail = (artifact / "prev_blockhash_tail.bin").read_bytes()
        require(tail and len(tail) % 40 == 0 and
                tail[-40:-8] == bytes(blocks[0]["parent_blockhash"]) and
                int.from_bytes(tail[-8:], "little") == blocks[0]["parent_slot"],
                f"predecessor tail mismatch: {name}")
        chunk = dict(blocks=len(blocks), transactions=0, signatures=0, entries=0)
        for position, block in enumerate(blocks):
            require(block["block_id"] == position and start <= block["slot"] < end and
                    block["parent_slot"] < block["slot"] and block["entries"] > 0 and
                    registry[position * 32:(position + 1) * 32] == bytes(block["blockhash"]),
                    f"block identity mismatch: {name}")
            if previous is not None:
                require(block["parent_slot"] == previous["slot"] and
                        block["parent_blockhash"] == previous["blockhash"],
                        f"parent chain gap at {block['slot']}")
                skipped = block["slot"] - previous["slot"] - 1
                require(skipped >= 0, "non-increasing slots")
                totals["skipped_slots"] += skipped
                gaps += int(skipped > 0)
            else:
                first = block
            previous = block
            for key in ("transactions", "signatures", "entries"):
                chunk[key] += block[key]
        for key in chunk:
            require(marker[key] == chunk[key], f"marker count mismatch: {key}: {name}")
            totals[key] += chunk[key]
        jobs.append(dict(artifact=name, receipt_sha=receipt_sha, marker_sha=marker_sha, **chunk))
        if time.monotonic() - last_progress >= 5:
            atomic_json(state / "STATUS.json", dict(state="CHECKING_CONTINUITY", epoch=epoch,
                        chunks_checked=len(jobs), chunks_total=len(grouped), **totals))
            last_progress = time.monotonic()
    boundaries = []
    for slot, expected_hash in ((first["parent_slot"], first["parent_blockhash"]),):
        row = database.execute("SELECT artifact FROM commits WHERE slot=?", (slot,)).fetchone()
        require(row is not None, "epoch predecessor is not retained")
        receipt, _ = bounded_json(Path(row[0]) / "DIRECT-AGAVE-NORMALIZED.v1.json")
        block = next(b for b in receipt["blocks"] if b["slot"] == slot)
        require(block["blockhash"] == expected_hash, "epoch predecessor hash mismatch")
        boundaries.append(dict(kind="predecessor", slot=slot, matched=True))
    row = database.execute("SELECT slot,artifact FROM commits WHERE slot>=? ORDER BY slot LIMIT 1", (end,)).fetchone()
    require(row is not None, "epoch successor is not yet retained")
    receipt, _ = bounded_json(Path(row[1]) / "DIRECT-AGAVE-NORMALIZED.v1.json")
    successor = next(b for b in receipt["blocks"] if b["slot"] == row[0])
    require(successor["parent_slot"] == previous["slot"] and
            successor["parent_blockhash"] == previous["blockhash"], "epoch successor hash mismatch")
    boundaries.append(dict(kind="successor", slot=successor["slot"], matched=True))
    database.close()
    report = dict(schema="hivezilla-native-epoch-continuity-v1", state="PASSED", epoch=epoch,
                  source=str(output), first_slot=first["slot"], last_slot=previous["slot"],
                  chunks=len(jobs), gap_ranges=gaps, boundaries=boundaries,
                  canonical=False, independent_consensus_proof=False,
                  independent_archive_coverage_confirmed=False,
                  elapsed_seconds=time.monotonic() - started, **totals)
    atomic_json(state / "CONTINUITY.json", report)
    with (state / "JOBS.jsonl").open("w") as stream:
        for job in jobs:
            stream.write(json.dumps(job, separators=(",", ":")) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    return jobs, report


def verify_chunk(job, verifier, timeout):
    root = Path(job["artifact"])
    for filename, expected in (("DIRECT-AGAVE-NORMALIZED.v1.json", job["receipt_sha"]),
                               ("DIRECT-AGAVE-COMPACT.v1.json", job["marker_sha"])):
        require(bounded_json(root / filename)[1] == expected, f"pinned receipt changed: {root}")
    names = ("archive-v2-blocks.index", "archive-v2-blocks.zstd", "signatures.bin",
             "blockhash_registry.bin", "prev_blockhash_tail.bin", "available-poh.wincode")
    stamps = {name: (root / name).stat() for name in names}
    result = subprocess.run([str(verifier), "verify-archive-v2-poh", str(root),
                             "--native-agave", "--threads", "1"],
                            capture_output=True, text=True, timeout=timeout)
    require(result.returncode == 0, f"PoH failed: {root}: {result.stderr[-12000:]}")
    report = json.loads(result.stdout)
    require(report["complete_source"] is True and report["external_blockhash_blocks"] == 0 and
            report["source_profile"] == "agave-bank-callbacks-v1" and
            report["blocks_verified"] == job["blocks"] and
            report["entries_verified"] == job["entries"] and
            report["transactions_consumed"] == job["transactions"] and
            report["signatures_hashed"] == job["signatures"], f"incomplete PoH check: {root}")
    for name, before in stamps.items():
        after = (root / name).stat()
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns),
                f"verification input changed: {root / name}")
    return job, report


def run(args):
    state, output = args.state.resolve(), args.output.resolve()
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    require(not state.is_relative_to(output), "verification state must be outside compact output")
    # One process owns the result database. Locks are separate from the source reader.
    import fcntl
    lock = (state / ".verify.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    jobs, continuity = inventory(output, args.epoch, state)
    if args.continuity_only:
        atomic_json(state / "STATUS.json", dict(continuity, state="CONTINUITY_PASSED", poh_verification="not-run"))
        print(json.dumps(continuity), flush=True)
        return
    database = sqlite3.connect(state / "RESULTS.sqlite")
    database.execute("PRAGMA journal_mode=WAL")
    database.execute("PRAGMA synchronous=FULL")
    database.execute("CREATE TABLE IF NOT EXISTS results(artifact TEXT PRIMARY KEY,receipt_sha TEXT NOT NULL,marker_sha TEXT NOT NULL,report TEXT NOT NULL)")
    done = {row[0]: row[1:] for row in database.execute("SELECT artifact,receipt_sha,marker_sha,report FROM results")}
    totals = dict(blocks_verified=0, entries_verified=0, transactions_consumed=0,
                  signatures_hashed=0, hashes_recomputed=0, compressed_bytes_read=0)
    pending = []
    completed = 0
    for job in jobs:
        cached = done.get(job["artifact"])
        if cached is None:
            pending.append(job)
        else:
            require(cached[:2] == (job["receipt_sha"], job["marker_sha"]), "resume source changed")
            report = json.loads(cached[2])
            for key in totals:
                totals[key] += report[key]
            completed += 1
    resumed = dict(totals)
    started = time.monotonic()
    last_progress = started
    status = dict(schema="hivezilla-native-epoch-poh-verification-v1", epoch=args.epoch,
                  source=str(output), verifier=str(args.verifier.resolve()), workers=args.workers,
                  chunks_total=len(jobs), blocks_total=continuity["blocks"],
                  entries_total=continuity["entries"], canonical=False,
                  independent_consensus_proof=False, independent_archive_coverage_confirmed=False,
                  ed25519_signature_verification="not-run", tick_schedule_verification="not-run",
                  continuity_receipt=str(state / "CONTINUITY.json"),
                  resumed_chunks=completed, state="VERIFYING_POH")
    def progress(final=False):
        elapsed = time.monotonic() - started
        fresh = totals["blocks_verified"] - resumed["blocks_verified"]
        rate = fresh / elapsed if elapsed else 0
        status.update(chunks_verified=completed, elapsed_seconds=elapsed,
                      blocks_per_second=rate, **totals)
        status["estimated_remaining_seconds"] = (continuity["blocks"] - totals["blocks_verified"]) / rate if rate else None
        atomic_json(state / "STATUS.json", status)
        if final:
            atomic_json(state / "RECEIPT.json", status)
        print(json.dumps(status, separators=(",", ":")), flush=True)
    progress()
    iterator = iter(pending)
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
    futures = set()
    try:
        for _ in range(args.workers * 2):
            job = next(iterator, None)
            if job is not None:
                futures.add(executor.submit(verify_chunk, job, args.verifier.resolve(), args.chunk_timeout))
        while futures:
            ready, futures = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in ready:
                job, report = future.result()
                database.execute("INSERT INTO results VALUES(?,?,?,?)", (job["artifact"], job["receipt_sha"], job["marker_sha"], json.dumps(report)))
                for key in totals:
                    totals[key] += report[key]
                completed += 1
                job = next(iterator, None)
                if job is not None:
                    futures.add(executor.submit(verify_chunk, job, args.verifier.resolve(), args.chunk_timeout))
            if time.monotonic() - last_progress >= 5:
                database.commit()
                progress()
                last_progress = time.monotonic()
        require(completed == len(jobs) and totals["blocks_verified"] == continuity["blocks"] and
                totals["entries_verified"] == continuity["entries"], "epoch verification is incomplete")
        database.commit()
        status["state"] = "PASSED"
        progress(final=True)
    except BaseException as error:
        for future in futures:
            future.cancel()
        database.commit()
        status.update(state="FAILED", error=str(error))
        progress()
        raise
    finally:
        executor.shutdown(wait=True, cancel_futures=True)
        database.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--epoch", type=int, required=True)
    parser.add_argument("--verifier", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--chunk-timeout", type=int, default=180)
    parser.add_argument("--continuity-only", action="store_true")
    args = parser.parse_args()
    require(0 <= args.epoch < (1 << 64) // 432000 and 1 <= args.workers <= 64,
            "invalid epoch or worker limit")
    try:
        run(args)
    except BaseException as error:
        args.state.mkdir(parents=True, exist_ok=True)
        atomic_json(args.state / "FAILURE.json", dict(state="FAILED", error=str(error)))
        raise
