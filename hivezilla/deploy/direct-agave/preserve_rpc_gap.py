#!/usr/bin/env python3
"""Preserve finalized local RPC blocks between two native capture sessions.

This is recovery evidence, not a native event journal or compact archive.
It does not restore entry boundaries or establish complete raw shred coverage.
"""
import concurrent.futures
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
import shutil
import time
import urllib.request


class RpcError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(f"RPC error {code}")


def atomic_json(path, value):
    stage = path.with_suffix(path.suffix + ".tmp")
    with stage.open("w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(stage, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def rpc(method, params):
    data = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method,
                       "params": params}).encode()
    for attempt in range(3):
        try:
            request = urllib.request.Request("http://127.0.0.1:8899", data,
                                             {"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
            value = json.loads(raw)
            if value.get("error") is not None:
                raise RpcError(value["error"].get("code"))
            if value.get("result") is None:
                raise RpcError(-32004)
            return raw, value["result"]
        except RpcError as error:
            # Deep finalized history is not repaired by waiting. Delaying every
            # pruned block lets the ledger prune later recoverable blocks too.
            if error.code != -32005 or attempt == 2:
                raise
            time.sleep(1)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(1)


def base58(data):
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    n = int.from_bytes(bytes(data), "big")
    out = ""
    while n:
        n, remainder = divmod(n, 58)
        out = alphabet[remainder] + out
    leading = len(data) - len(bytes(data).lstrip(b"\0"))
    return "1" * leading + out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root")
    parser.add_argument("old_session")
    parser.add_argument("new_session")
    parser.add_argument("destination")
    parser.add_argument("--allow-pruned-prefix", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--workers", type=int, choices=range(1, 33), default=4)
    args = parser.parse_args()
    root, old_session, new_session, destination = args.root, args.old_session, args.new_session, args.destination
    root, destination = Path(root), Path(destination)
    destination.mkdir(parents=True, exist_ok=args.resume)
    old = json.loads((root / "compact/direct-agave-live" / old_session / "CURSOR.json").read_text())
    new = json.loads((root / "compact/direct-agave-live" / new_session / "CURSOR.json").read_text())
    start, end = old["last_slot"] + 1, new["first_slot"] - 1
    if start > end:
        raise RuntimeError("No gap between these sessions")
    identity = rpc("getIdentity", [])[1]["identity"]
    genesis = rpc("getGenesisHash", [])[1]
    if identity != "J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ" or genesis != "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d":
        raise RuntimeError("Unexpected local validator")
    available_start = rpc("getFirstAvailableBlock", [])[1]
    prior = json.loads((destination / "RECEIPT.json").read_text()) if args.resume else None
    if prior is not None and any(prior.get(k) != v for k, v in {
        "schema": "hivezilla-local-rpc-gap-backup-v1", "identity": identity,
        "genesis_hash": genesis, "old_session": old_session, "new_session": new_session,
        "first_slot": start, "last_slot": end}.items()):
        raise RuntimeError("Resume receipt belongs to another source or gap")
    if prior is not None:
        atomic_json(destination / ("RECEIPT-before-resume-" + str(time.time_ns()) + ".json"), prior)
    backup_start = prior["backup_first_slot"] if prior else max(start, available_start) if args.allow_pruned_prefix else start
    if backup_start > end:
        raise RuntimeError("The whole gap is outside local RPC retention")
    slots = prior["produced_slots"] if prior else rpc("getBlocks", [backup_start, end, {"commitment": "finalized"}])[1]
    if slots != sorted(set(slots)) or any(s < backup_start or s > end for s in slots):
        raise RuntimeError("Invalid finalized slot list")
    options = {"commitment": "finalized", "encoding": "base64",
               "transactionDetails": "full", "rewards": True,
               "maxSupportedTransactionVersion": 1}
    receipt = {"schema": "hivezilla-local-rpc-gap-backup-v1", "state": "SAVING",
               "started_unix": int(time.time()), "rpc": "http://127.0.0.1:8899",
               "identity": identity, "genesis_hash": genesis,
               "old_session": old_session, "new_session": new_session,
               "first_slot": start, "last_slot": end,
               "backup_first_slot": backup_start,
               "unavailable_rpc_prefix": [start, backup_start - 1] if backup_start > start else None,
               "slot_span": end - start + 1, "produced_slots": slots,
               "skipped_slots": sorted(set(range(backup_start, end + 1)) - set(slots)),
               "native_capture_complete": False, "compact_gap_rebuilt": False,
               "raw_shred_coverage_verified": False,
               "entry_boundaries_preserved": False, "blocks": [], "errors": []}
    atomic_json(destination / "RECEIPT.json", receipt)

    def block_record(slot, path, raw, compressed, value):
        txs = value["transactions"]
        if any(t.get("meta") is None or not isinstance(t.get("transaction"), list)
               or t["transaction"][1] != "base64" for t in txs):
            raise RuntimeError("Block has absent metadata or wire transaction")
        return {"slot": slot, "path": path.name, "parent_slot": value["parentSlot"],
                "blockhash": value["blockhash"], "previous_blockhash": value["previousBlockhash"],
                "transactions": len(txs), "rewards": len(value.get("rewards") or []),
                "rpc_bytes": len(raw), "gzip_bytes": len(compressed),
                "rpc_sha256": hashlib.sha256(raw).hexdigest(),
                "gzip_sha256": hashlib.sha256(compressed).hexdigest()}

    prior_blocks = {b["slot"]: b for b in prior.get("blocks", [])} if prior else {}

    def save(slot):
        path = destination / (str(slot) + ".rpc.json.gz")
        if args.resume and path.exists():
            compressed = path.read_bytes()
            raw = gzip.decompress(compressed)
            value = json.loads(raw)
            if value.get("error") or value.get("result") is None:
                raise RuntimeError("Invalid saved RPC response")
            row = block_record(slot, path, raw, compressed, value["result"])
            if slot in prior_blocks and row != prior_blocks[slot]:
                raise RuntimeError("Saved block differs from its prior receipt")
            return row
        if slot < available_start:
            raise RuntimeError("Missing saved block is now outside local RPC retention")
        if shutil.disk_usage(destination).free < 700 * 1024**3:
            raise RuntimeError("Recovery backup stopped at its 700 GiB free-disk reserve")
        raw, value = rpc("getBlock", [slot, options])
        compressed = gzip.compress(raw, compresslevel=1, mtime=0)
        row = block_record(slot, path, raw, compressed, value)
        stage = path.with_name(path.name + ".stage-" + str(os.getpid()))
        with stage.open("xb") as f:
            f.write(compressed)
            f.flush()
            os.fsync(f.fileno())
        # Publish without replacing an existing saved block.
        os.link(stage, path)
        stage.unlink()
        return row

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        # Fetch still available missing blocks before auditing the saved files.
        # The original finalized slot list remains the required coverage set.
        order = sorted(slots, key=lambda s: ((destination / (str(s) + ".rpc.json.gz")).exists(), s < available_start, s))
        futures = {pool.submit(save, slot): slot for slot in order}
        for future in concurrent.futures.as_completed(futures):
            try:
                receipt["blocks"].append(future.result())
            except Exception as error:
                receipt["errors"].append({"slot": futures[future], "error": str(error)})
            if (len(receipt["blocks"]) + len(receipt["errors"])) % 32 == 0:
                receipt["updated_unix"] = int(time.time())
                # Keep the live status small; rewriting the complete growing manifest
                # every 32 blocks would cause excess disk traffic during a long backup.
                atomic_json(destination / "STATUS.json", {
                    "state": "SAVING", "updated_unix": receipt["updated_unix"],
                    "blocks_saved": len(receipt["blocks"]), "blocks_expected": len(slots),
                    "errors": len(receipt["errors"]),
                    "last_saved_slot": max((b["slot"] for b in receipt["blocks"]), default=None),
                })
                print(json.dumps({"saved": len(receipt["blocks"]), "errors": len(receipt["errors"])}), flush=True)
    receipt["blocks"].sort(key=lambda b: b["slot"])
    previous_slot, previous_hash = old["last_slot"], base58(old["last_hash"])
    if backup_start > start and receipt["blocks"]:
        previous_slot = receipt["blocks"][0]["parent_slot"]
        previous_hash = receipt["blocks"][0]["previous_blockhash"]
    chain_ok = len(receipt["blocks"]) == len(slots) and not receipt["errors"]
    for block in receipt["blocks"]:
        chain_ok = chain_ok and block["parent_slot"] == previous_slot and block["previous_blockhash"] == previous_hash
        previous_slot, previous_hash = block["slot"], block["blockhash"]
    try:
        boundary_options = dict(options, transactionDetails="none")
        following = rpc("getBlock", [new["first_slot"], boundary_options])[1]
        chain_ok = chain_ok and following["parentSlot"] == previous_slot and following["previousBlockhash"] == previous_hash
        receipt["following_block"] = {k: following[k] for k in ["parentSlot", "previousBlockhash", "blockhash"]}
    except Exception as error:
        receipt["errors"].append({"slot": new["first_slot"], "error": str(error)})
        chain_ok = False
    receipt.update(state=("SAVED_PARTIAL" if backup_start > start else "SAVED") if chain_ok else "INCOMPLETE",
                   retained_chain_matches_following_block=chain_ok,
                   chain_matches_boundaries=chain_ok and backup_start == start,
                   completed_unix=int(time.time()), blocks_saved=len(receipt["blocks"]),
                   transactions_saved=sum(b["transactions"] for b in receipt["blocks"]),
                   gzip_bytes=sum(b["gzip_bytes"] for b in receipt["blocks"]))
    atomic_json(destination / "RECEIPT.json", receipt)
    atomic_json(destination / "STATUS.json", {k: receipt[k] for k in
                ["state", "completed_unix", "blocks_saved", "transactions_saved", "gzip_bytes"]})
    print(json.dumps({k: v for k, v in receipt.items() if k not in ["blocks", "produced_slots"]}), flush=True)
    return 0 if chain_ok else 1


if __name__ == "__main__":
    sys.exit(main())
