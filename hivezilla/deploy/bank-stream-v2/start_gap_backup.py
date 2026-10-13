#!/usr/bin/env python3
"""Save available finalized RPC gap blocks, with separate recovery provenance."""
import argparse
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import time


def bytes32(value):
    if isinstance(value, list) and len(value) == 32 and all(isinstance(v, int) and 0 <= v <= 255 for v in value):
        return value
    if isinstance(value, str):
        alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        number = 0
        for char in value:
            number = number * 58 + alphabet.index(char)
        raw = b"\0" * (len(value) - len(value.lstrip("1"))) + number.to_bytes((number.bit_length() + 7) // 8, "big")
        if len(raw) == 32:
            return list(raw)
    raise RuntimeError("invalid_boundary_hash")


def bank(output, last=False):
    with sqlite3.connect("file:" + str(output / "feed.sqlite") + "?mode=ro", uri=True) as db:
        row = db.execute("SELECT event_json FROM feed_events WHERE kind='root' ORDER BY sequence " +
                         ("DESC" if last else "ASC") + " LIMIT 1").fetchone()
    if not row:
        raise RuntimeError("rooted_boundary_not_available")
    return json.loads(row[0])["bank"]


def start(root, report, backup_script):
    spec = importlib.util.spec_from_file_location("preserve_rpc_gap", backup_script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    record = json.loads((report / "RECOVERY.json").read_text())
    if record["state"] != "RECOVERED_PENDING_PUBLIC_CHECK":
        raise RuntimeError("unexpected_recovery_state")
    old = record["old_source"]["session_id"]
    new = record["source"]["session_id"]
    a = bank(root / "compact/direct-agave-processed-live" / old, last=True)
    b = bank(Path(record["processed_output"]))
    options = {"commitment": "finalized", "transactionDetails": "none", "maxSupportedTransactionVersion": 1}
    for boundary in [b]:
        _, block = module.rpc("getBlock", [boundary["slot"], options])
        if block["blockhash"] != module.base58(bytes32(boundary["blockhash"])):
            raise RuntimeError("new_root_boundary_rpc_mismatch")
    available = module.rpc("getFirstAvailableBlock", [])[1]
    old_rpc_match = None
    if available <= a["slot"]:
        _, block = module.rpc("getBlock", [a["slot"], options])
        old_rpc_match = block["blockhash"] == module.base58(bytes32(a["blockhash"]))
        if not old_rpc_match:
            raise RuntimeError("old_root_boundary_rpc_mismatch")
    layout = report / "gap-source-boundaries"
    layout.mkdir(exist_ok=False, mode=0o700)
    for session, cursor in [(old, {"last_slot": a["slot"], "last_hash": bytes32(a["blockhash"])}),
                            (new, {"first_slot": b["slot"]})]:
        p = layout / "compact/direct-agave-live" / session
        p.mkdir(parents=True)
        module.atomic_json(p / "CURSOR.json", cursor)
    first, last = a["slot"] + 1, b["slot"] - 1
    summary = {"schema": "blockzilla-gap-recovery-boundaries-v1", "state": "BACKUP_STARTING",
               "old_last_root_slot": a["slot"], "new_first_root_slot": b["slot"],
               "first_gap_slot": first, "last_gap_slot": last, "gap_slot_span": last - first + 1,
               "first_available_rpc_slot": available, "old_boundary_rpc_matches": old_rpc_match,
               "new_boundary_rpc_matches": True,
               "unavailable_rpc_prefix": [first, min(last, available - 1)] if available > first else None,
               "boundary_layout_is_native_output": False,
               "compact_gap_rebuilt": False, "entry_boundaries_preserved": False,
               "raw_shred_coverage_verified": False, "checked_unix_ms": int(time.time() * 1000)}
    module.atomic_json(report / "GAP-BOUNDARIES.json", summary)
    destination = report / "rpc-gap-blocks"
    args = ["sudo", "systemd-run", "--unit=hivezilla-native-gap-backup-20261011",
            "--property=User=ubuntu", "--property=Group=ubuntu", "--property=UMask=0077",
            "--property=MemoryMax=4G", "--property=CPUQuota=800%", "--property=RuntimeMaxSec=7200",
            "--property=StandardOutput=append:" + str(report / "gap-backup.log"),
            "--property=StandardError=append:" + str(report / "gap-backup.log"),
            "/usr/bin/python3", str(backup_script), str(layout), old, new, str(destination),
            "--allow-pruned-prefix"]
    subprocess.run(args, check=True)
    print(json.dumps(summary))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--report", type=Path, required=True)
    p.add_argument("--backup-script", type=Path, required=True)
    a = p.parse_args()
    start(a.root, a.report, a.backup_script)
