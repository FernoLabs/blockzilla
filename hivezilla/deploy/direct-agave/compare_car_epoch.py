#!/usr/bin/env python3
"""Compare sealed native block identities and signature bytes with a CAR import.

This does not compare execution metadata or verify Ed25519 signatures. It never
changes either compact source. The native export is bound to a completed PoH
receipt and to the receipt hashes recorded by that verification job.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import struct
import time

HEADER = struct.Struct("<8sQQ")
RECORD = struct.Struct("<QQ32s32sQQQ32s")
INDEX_HEADER = struct.Struct("<8sHHQQiI")
INDEX_ROW = struct.Struct("<IQQIIIQQI")
HASH_HEADER = struct.Struct("<8sHHQ")
HASH_ROW = struct.Struct("<Q32sq")
MAGIC = b"BZNCCMP1"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as out:
        json.dump(value, out, indent=2)
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
    temporary.replace(path)


def stamp(path):
    s = path.stat()
    return (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)


def checked_bytes(path, limit):
    require(path.is_file() and not path.is_symlink(), f"invalid file: {path}")
    require(path.stat().st_size <= limit, f"file too large: {path}")
    return path.read_bytes()


def signature_digest(stream, count):
    remaining = count * 64
    digest = hashlib.sha256()
    while remaining:
        data = stream.read(min(remaining, 1 << 20))
        require(data, "signature file truncated")
        remaining -= len(data)
        digest.update(data)
    return digest.digest()


def read_index(path):
    data = checked_bytes(path, 32 << 20)
    require(len(data) >= INDEX_HEADER.size, "index header truncated")
    magic, version, reserved, count, blob_bytes, level, flags = INDEX_HEADER.unpack_from(data)
    require((magic, version, reserved) == (b"BZV2HIX1", 1, 0) and flags == 0,
            "unsupported index format")
    require(len(data) == INDEX_HEADER.size + count * INDEX_ROW.size, "index length mismatch")
    offset = txs = sigs = 0
    for i in range(count):
        row = INDEX_ROW.unpack_from(data, INDEX_HEADER.size + i * INDEX_ROW.size)
        block_id, slot, at, compressed, raw, transactions, first_tx, first_sig, signatures = row
        require((block_id, at, first_tx, first_sig) == (i, offset, txs, sigs),
                f"noncontiguous index row {i}")
        require(compressed > 0 and raw > 0 and at + compressed <= blob_bytes,
                f"invalid frame bounds at {slot}")
        offset += compressed
        txs += transactions
        sigs += signatures
    require(offset == blob_bytes, "unindexed compact bytes")
    return data, count, blob_bytes, txs, sigs


def export_native(output, verification, state, epoch):
    output, verification, state = output.resolve(), verification.resolve(), state.resolve()
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    require(not state.is_relative_to(output), "export must be outside native output")
    proof_raw = checked_bytes(verification / "RECEIPT.json", 1 << 20)
    proof = json.loads(proof_raw)
    require(proof["state"] == "PASSED" and proof["epoch"] == epoch and
            Path(proof["source"]).resolve() == output and
            proof["blocks_total"] == proof["blocks_verified"] and
            proof["chunks_total"] == proof["chunks_verified"] and
            proof["entries_total"] == proof["entries_verified"], "incomplete or unrelated PoH receipt")
    db = sqlite3.connect(f"file:{output}/committed.sqlite?mode=ro", uri=True)
    db.execute("PRAGMA query_only=ON")
    committed = dict(db.execute("SELECT slot,artifact FROM commits WHERE slot>=? AND slot<?",
                               (epoch * 432000, (epoch + 1) * 432000)))
    db.close()
    require(len(committed) == proof["blocks_verified"], "native inventory differs from PoH receipt")
    path = state / "native-blocks.bin"
    temporary = state / "native-blocks.bin.part"
    totals = dict(blocks=0, transactions=0, signatures=0, entries=0, chunks=0)
    digest = hashlib.sha256()
    previous = None
    last_progress = started = time.monotonic()
    with temporary.open("xb") as out, (verification / "JOBS.jsonl").open() as jobs:
        header = HEADER.pack(MAGIC, epoch, len(committed))
        out.write(header)
        digest.update(header)
        for line in jobs:
            job = json.loads(line)
            artifact = Path(job["artifact"])
            require(artifact.parent == output / f"epoch-{epoch}" and artifact.resolve() == artifact,
                    "native artifact outside pinned epoch")
            raw = checked_bytes(artifact / "DIRECT-AGAVE-NORMALIZED.v1.json", 16 << 20)
            require(hashlib.sha256(raw).hexdigest() == job["receipt_sha"], "native receipt changed after PoH")
            marker = checked_bytes(artifact / "DIRECT-AGAVE-COMPACT.v1.json", 16 << 20)
            require(hashlib.sha256(marker).hexdigest() == job["marker_sha"], "compact marker changed after PoH")
            normalized = json.loads(raw)
            blocks = normalized["blocks"]
            index, count, blob_bytes, txs, sigs = read_index(artifact / "archive-v2-blocks.index")
            require(count == len(blocks) == job["blocks"] and
                    txs == job["transactions"] and sigs == job["signatures"], "native index totals mismatch")
            require((artifact / "archive-v2-blocks.zstd").stat().st_size == blob_bytes, "native blob size mismatch")
            registry = checked_bytes(artifact / "blockhash_registry.bin", 32 * 32)
            require(len(registry) == count * 32, "native blockhash registry length mismatch")
            signature_path = artifact / "signatures.bin"
            before = stamp(signature_path)
            require(before[2] == sigs * 64, "native signature file length mismatch")
            with signature_path.open("rb") as signatures:
                for i, block in enumerate(blocks):
                    row = INDEX_ROW.unpack_from(index, INDEX_HEADER.size + i * INDEX_ROW.size)
                    slot, parent = block["slot"], block["parent_slot"]
                    blockhash, parent_hash = bytes(block["blockhash"]), bytes(block["parent_blockhash"])
                    require(committed.pop(slot, None) == str(artifact), "native commit/source mismatch")
                    require((row[0], row[1], row[5], row[8]) ==
                            (i, slot, block["transactions"], block["signatures"]), "native row/receipt mismatch")
                    require(registry[i * 32:(i + 1) * 32] == blockhash, "native registry/receipt mismatch")
                    require(parent < slot and (previous is None or (parent, parent_hash) == previous),
                            "native parent chain mismatch")
                    record = RECORD.pack(slot, parent, blockhash, parent_hash, block["transactions"],
                                         block["signatures"], block["entries"],
                                         signature_digest(signatures, block["signatures"]))
                    out.write(record)
                    digest.update(record)
                    previous = (slot, blockhash)
                    for key in ("transactions", "signatures", "entries"):
                        totals[key] += block[key]
                    totals["blocks"] += 1
                require(not signatures.read(1), "native signature file trailing bytes")
            require(stamp(signature_path) == before, "native signature file changed during export")
            totals["chunks"] += 1
            if time.monotonic() - last_progress >= 5:
                save(state / "STATUS.json", dict(state="EXPORTING_NATIVE", epoch=epoch, **totals))
                last_progress = time.monotonic()
        require(not committed, "native commits missing from export")
        for key, proof_key in [("transactions", "transactions_consumed"), ("signatures", "signatures_hashed"),
                               ("entries", "entries_verified"), ("chunks", "chunks_verified")]:
            require(totals[key] == proof[proof_key], f"native export proof total mismatch: {key}")
        out.flush()
        os.fsync(out.fileno())
    require(not path.exists(), "native export already exists")
    temporary.rename(path)
    receipt = dict(schema="hivezilla-native-car-export-v1", state="COMPLETE", epoch=epoch,
                   source=str(output), inventory=str(path), inventory_sha256=digest.hexdigest(),
                   poh_receipt=proof, poh_receipt_sha256=hashlib.sha256(proof_raw).hexdigest(),
                   elapsed_seconds=time.monotonic() - started, **totals)
    save(state / "EXPORT.json", receipt)
    save(state / "STATUS.json", receipt)
    return receipt


def read_hash_rows(path):
    data = checked_bytes(path, 32 << 20)
    require(len(data) >= HASH_HEADER.size, "hash index header truncated")
    magic, version, row_size, count = HASH_HEADER.unpack_from(data)
    require((magic, version, row_size) == (b"BZBHIX3!", 3, HASH_ROW.size) and count > 0 and
            len(data) == HASH_HEADER.size + count * HASH_ROW.size, "invalid hash index")
    return data, count


def export_car(archive, predecessor, source, state):
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata = json.loads(checked_bytes(source / "source.json", 1 << 20))
    imported = json.loads(checked_bytes(archive / "blockzilla-car-import-receipt.json", 1 << 20))
    epoch = metadata["epoch"]
    require(imported["epoch"] == epoch, "CAR import epoch mismatch")
    for name, binding in metadata["source_records"].items():
        raw = checked_bytes(source / name, 10 << 20)
        require(len(raw) == binding["bytes"] and hashlib.sha256(raw).hexdigest() == binding["sha256"],
                f"published source record changed: {name}")
    slots = [int(line) for line in (source / f"{epoch}.slots.txt").read_text().splitlines()]
    slots = [s for s in slots if epoch * 432000 <= s < (epoch + 1) * 432000]
    require(slots and len(slots) == metadata["num_slots"] and all(a < b for a, b in zip(slots, slots[1:])),
            "invalid CAR slot list")
    index, count, blob_bytes, transactions, signature_count = read_index(archive / "archive-v2-blocks.index")
    hashes, hash_count = read_hash_rows(archive / "blockhash_index_v3.bin")
    require(count == hash_count == len(slots) and transactions == metadata["num_transactions"],
            "CAR index/source total mismatch")
    require((archive / "archive-v2-blocks.zstd").stat().st_size == blob_bytes, "CAR compact blob size mismatch")
    previous_hashes, previous_count = read_hash_rows(predecessor / "blockhash_index_v3.bin")
    parent_slot, parent_hash, _ = HASH_ROW.unpack_from(previous_hashes, HASH_HEADER.size + (previous_count - 1) * HASH_ROW.size)
    signature_path = archive / "signatures.bin"
    before = stamp(signature_path)
    require(before[2] == signature_count * 64, "CAR signature file length mismatch")
    temporary = state / "car-blocks.bin.part"
    path = state / "car-blocks.bin"
    digest = hashlib.sha256()
    last_progress = started = time.monotonic()
    with temporary.open("xb") as out, signature_path.open("rb") as signatures:
        header = HEADER.pack(MAGIC, epoch, count)
        out.write(header)
        digest.update(header)
        for i, slot in enumerate(slots):
            row = INDEX_ROW.unpack_from(index, INDEX_HEADER.size + i * INDEX_ROW.size)
            hash_slot, blockhash, _ = HASH_ROW.unpack_from(hashes, HASH_HEADER.size + i * HASH_ROW.size)
            require(row[1] == hash_slot == slot and parent_slot < slot, "CAR source slot/parent mismatch")
            record = RECORD.pack(slot, parent_slot, blockhash, parent_hash, row[5], row[8], 0,
                                 signature_digest(signatures, row[8]))
            out.write(record)
            digest.update(record)
            parent_slot, parent_hash = slot, blockhash
            if time.monotonic() - last_progress >= 5:
                save(state / "STATUS.json", dict(state="EXPORTING_CAR", epoch=epoch,
                                                blocks_checked=i + 1, blocks_total=count))
                last_progress = time.monotonic()
        require(not signatures.read(1), "CAR signature file trailing bytes")
        out.flush()
        os.fsync(out.fileno())
    require(stamp(signature_path) == before, "CAR signature file changed during export")
    require(not path.exists(), "CAR export already exists")
    temporary.rename(path)
    result = dict(schema="hivezilla-car-comparison-export-v1", state="COMPLETE", epoch=epoch,
                  inventory=str(path), inventory_sha256=digest.hexdigest(), blocks=count,
                  transactions=transactions, signatures=signature_count, signature_bytes=before[2],
                  source_cid=metadata["epoch_cid"], source_records=metadata["source_records"],
                  import_receipt=imported, elapsed_seconds=time.monotonic() - started,
                  includes_private_native_data=False, entry_counts_in_inventory=False)
    save(state / "EXPORT-CAR.json", result)
    save(state / "STATUS.json", result)
    return result


def compare_inventories(native_export, car_export, state):
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    native_receipt = json.loads(checked_bytes(native_export / "EXPORT.json", 1 << 20))
    car_receipt = json.loads(checked_bytes(car_export / "EXPORT-CAR.json", 1 << 20))
    require(native_receipt["state"] == car_receipt["state"] == "COMPLETE" and
            native_receipt["schema"] == "hivezilla-native-car-export-v1" and
            car_receipt["schema"] == "hivezilla-car-comparison-export-v1", "incomplete comparison export")
    for name in ("epoch", "blocks", "transactions", "signatures"):
        require(native_receipt[name] == car_receipt[name], f"CAR/native total mismatch: {name}")
    epoch, count = native_receipt["epoch"], native_receipt["blocks"]
    proof = native_receipt["poh_receipt"]
    require(proof["state"] == "PASSED" and proof["blocks_verified"] == count and
            proof["transactions_consumed"] == native_receipt["transactions"] and
            proof["signatures_hashed"] == native_receipt["signatures"], "native PoH/export mismatch")
    fields = ["slot", "parent_slot", "blockhash", "parent_blockhash", "transactions",
              "signature_count", "signature_bytes_and_order"]
    native_path, car_path = native_export / "native-blocks.bin", car_export / "car-blocks.bin"
    before = (stamp(native_path), stamp(car_path))
    require(before[0][2] == before[1][2] == HEADER.size + count * RECORD.size, "inventory length mismatch")
    nd, cd = hashlib.sha256(), hashlib.sha256()
    mismatches = []
    mismatch_count = 0
    with native_path.open("rb") as native, car_path.open("rb") as car:
        nh, ch = native.read(HEADER.size), car.read(HEADER.size)
        require(nh == ch == HEADER.pack(MAGIC, epoch, count), "inventory header mismatch")
        nd.update(nh)
        cd.update(ch)
        for _ in range(count):
            nb, cb = native.read(RECORD.size), car.read(RECORD.size)
            nd.update(nb)
            cd.update(cb)
            n, c = RECORD.unpack(nb), RECORD.unpack(cb)
            require(c[6] == 0, "unexpected CAR entry-count field")
            failed = [name for name, at in zip(fields, (0, 1, 2, 3, 4, 5, 7)) if n[at] != c[at]]
            if failed:
                mismatch_count += 1
                if len(mismatches) < 20:
                    mismatches.append(dict(slot=c[0], fields=failed))
        require(not native.read(1) and not car.read(1), "inventory trailing bytes")
    require(nd.hexdigest() == native_receipt["inventory_sha256"] and
            cd.hexdigest() == car_receipt["inventory_sha256"], "inventory digest mismatch")
    require((stamp(native_path), stamp(car_path)) == before, "comparison input changed")
    result = dict(schema="hivezilla-native-car-comparison-v1", state="PASSED" if not mismatch_count else "FAILED",
                  epoch=epoch, native_source=native_receipt["source"], car_source_cid=car_receipt["source_cid"],
                  car_source_records=car_receipt["source_records"], blocks_checked=count,
                  transactions=native_receipt["transactions"], signatures=native_receipt["signatures"],
                  signature_bytes_checked=car_receipt["signature_bytes"], mismatch_blocks=mismatch_count,
                  mismatches=mismatches, fields_checked=fields, native_poh=proof,
                  execution_metadata_comparison="not-run", ed25519_signature_verification="not-run",
                  whole_car_sha256_verification="not-run", independent_consensus_proof=False,
                  native_publication_flags_changed=False)
    save(state / "RECEIPT.json", result)
    save(state / "STATUS.json", result)
    return result


def compare(export, archive, predecessor, source, state):
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    receipt = json.loads(checked_bytes(export / "EXPORT.json", 1 << 20))
    require(receipt["state"] == "COMPLETE", "native export not complete")
    epoch = receipt["epoch"]
    metadata = json.loads(checked_bytes(source / "source.json", 1 << 20))
    import_receipt = json.loads(checked_bytes(archive / "blockzilla-car-import-receipt.json", 1 << 20))
    require(metadata["epoch"] == import_receipt["epoch"] == epoch, "CAR epoch mismatch")
    for name, binding in metadata["source_records"].items():
        raw = checked_bytes(source / name, 10 << 20)
        require(len(raw) == binding["bytes"] and hashlib.sha256(raw).hexdigest() == binding["sha256"],
                f"published source record changed: {name}")
    slots = [int(line) for line in (source / f"{epoch}.slots.txt").read_text().splitlines()]
    slots = [s for s in slots if epoch * 432000 <= s < (epoch + 1) * 432000]
    require(len(slots) == metadata["num_slots"] and all(a < b for a, b in zip(slots, slots[1:])),
            "invalid published CAR slot list")
    index, count, blob_bytes, transactions, signature_count = read_index(archive / "archive-v2-blocks.index")
    hashes, hash_count = read_hash_rows(archive / "blockhash_index_v3.bin")
    require(count == hash_count == len(slots) == receipt["blocks"], "CAR/native block count mismatch")
    require(transactions == metadata["num_transactions"] == receipt["transactions"], "transaction total mismatch")
    require(signature_count == receipt["signatures"], "signature total mismatch")
    require((archive / "archive-v2-blocks.zstd").stat().st_size == blob_bytes, "CAR compact blob size mismatch")
    previous_hashes, previous_count = read_hash_rows(predecessor / "blockhash_index_v3.bin")
    parent_slot, parent_hash, _ = HASH_ROW.unpack_from(previous_hashes, HASH_HEADER.size + (previous_count - 1) * HASH_ROW.size)
    signature_path = archive / "signatures.bin"
    before = stamp(signature_path)
    require(before[2] == signature_count * 64, "CAR signature file length mismatch")
    native_path = export / "native-blocks.bin"
    native_before = stamp(native_path)
    require(native_before[2] == HEADER.size + count * RECORD.size, "native export length mismatch")
    digest = hashlib.sha256()
    mismatches = []
    mismatch_count = checked = consumed = 0
    last_progress = started = time.monotonic()
    with native_path.open("rb") as native, signature_path.open("rb") as signatures:
        header = native.read(HEADER.size)
        require(HEADER.unpack(header) == (MAGIC, epoch, count), "native export header mismatch")
        digest.update(header)
        for i, slot in enumerate(slots):
            raw = native.read(RECORD.size)
            require(len(raw) == RECORD.size, "native export truncated")
            digest.update(raw)
            n_slot, n_parent, n_hash, n_parent_hash, n_txs, n_sigs, n_entries, n_sig_digest = RECORD.unpack(raw)
            row = INDEX_ROW.unpack_from(index, INDEX_HEADER.size + i * INDEX_ROW.size)
            hash_slot, blockhash, _ = HASH_ROW.unpack_from(hashes, HASH_HEADER.size + i * HASH_ROW.size)
            require(row[1] == hash_slot == slot, "CAR index/published slot mismatch")
            fields = {"slot": n_slot == slot, "parent_slot": n_parent == parent_slot,
                      "blockhash": n_hash == blockhash, "parent_blockhash": n_parent_hash == parent_hash,
                      "transactions": n_txs == row[5], "signature_count": n_sigs == row[8],
                      "signature_bytes_and_order": n_sig_digest == signature_digest(signatures, row[8])}
            failed = [name for name, matches in fields.items() if not matches]
            if failed:
                mismatch_count += 1
                if len(mismatches) < 20:
                    mismatches.append(dict(slot=slot, fields=failed))
            checked += 1
            consumed += row[8] * 64
            parent_slot, parent_hash = slot, blockhash
            if time.monotonic() - last_progress >= 5:
                save(state / "STATUS.json", dict(state="COMPARING", epoch=epoch, blocks_checked=checked,
                     blocks_total=count, signature_bytes_checked=consumed, mismatch_blocks=mismatch_count))
                last_progress = time.monotonic()
        require(not native.read(1) and not signatures.read(1), "comparison input has trailing bytes")
    require(digest.hexdigest() == receipt["inventory_sha256"], "native export digest mismatch")
    require(stamp(native_path) == native_before and stamp(signature_path) == before, "comparison input changed")
    result = dict(schema="hivezilla-native-car-comparison-v1", state="PASSED" if not mismatch_count else "FAILED",
                  epoch=epoch, native_source=receipt["source"], car_archive=str(archive),
                  car_source_cid=metadata["epoch_cid"], car_source_records=metadata["source_records"],
                  blocks_checked=checked, transactions=transactions, signatures=signature_count,
                  signature_bytes_checked=consumed, mismatch_blocks=mismatch_count, mismatches=mismatches,
                  native_poh=receipt["poh_receipt"], elapsed_seconds=time.monotonic() - started,
                  fields_checked=list(fields), execution_metadata_comparison="not-run",
                  ed25519_signature_verification="not-run", whole_car_sha256_verification="not-run",
                  independent_consensus_proof=False, native_publication_flags_changed=False)
    save(state / "RECEIPT.json", result)
    save(state / "STATUS.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("export")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--verification", type=Path, required=True)
    p.add_argument("--epoch", type=int, required=True)
    p.add_argument("--state", type=Path, required=True)
    p = commands.add_parser("compare")
    for name in ("export", "archive", "predecessor", "source", "state"):
        p.add_argument("--" + name, type=Path, required=True)
    p = commands.add_parser("export-car")
    for name in ("archive", "predecessor", "source", "state"):
        p.add_argument("--" + name, type=Path, required=True)
    p = commands.add_parser("compare-inventories")
    for name in ("native-export", "car-export", "state"):
        p.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "export":
            result = export_native(args.output, args.verification, args.state, args.epoch)
        elif args.command == "export-car":
            result = export_car(args.archive, args.predecessor, args.source, args.state)
        elif args.command == "compare-inventories":
            result = compare_inventories(args.native_export, args.car_export, args.state)
        else:
            result = compare(args.export, args.archive, args.predecessor, args.source, args.state)
        print(json.dumps(result), flush=True)
        return 0 if result["state"] in ("COMPLETE", "PASSED") else 1
    except Exception as error:
        args.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        save(args.state / "STATUS.json", dict(state="FAILED", error=str(error)))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
