#!/usr/bin/env python3
"""Finite live fan-out test; never changes capture or validator services."""
import argparse
import concurrent.futures
import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.parse
from transport import req, frame


def main():
    a = argparse.ArgumentParser()
    a.add_argument("--url", default="http://127.0.0.1:8871")
    a.add_argument("--token-file", type=Path, required=True)
    a.add_argument("--report", type=Path, required=True)
    args = a.parse_args()
    token = args.token_file.read_text().strip()
    def head():
        with req(args.url + "/v1/head", token=token) as r:
            return json.load(r)
    def stats():
        text = subprocess.check_output(["systemctl", "show", "hivezilla-stream-origin.service", "hivezilla-stream-relay.service",
                                        "--property=Id,MainPID,CPUUsageNSec,MemoryCurrent,MemoryPeak,NRestarts"], text=True)
        return text
    before = head()
    cursor = before["latest_cursor"]
    def client(_):
        rows = []
        started = time.monotonic()
        with req(args.url + "/v1/blocks?after=" + urllib.parse.quote(cursor), token=token) as r:
            for _ in range(8):
                wire, meta = frame(r)
                rows.append({"sha256": hashlib.sha256(wire).hexdigest(), "bytes": len(wire),
                             "last_slot": meta["marker"]["end_slot_exclusive"] - 1,
                             "blocks": meta["marker"]["blocks"]})
        return {"seconds": time.monotonic() - started, "chunks": rows}
    stats_before = stats()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        consumers = list(pool.map(client, range(10)))
    assert all(c["chunks"] == consumers[0]["chunks"] for c in consumers)
    after = head()
    assert after["state"] == "LIVE" and not after["stale"]
    result = {"schema": "hivezilla-stream-live-test-v1", "state": "PASS", "clients": 10,
              "identical_chunks_per_client": 8, "head_before": before, "head_after": after,
              "consumers": consumers, "service_stats_before": stats_before, "service_stats_after": stats(),
              "same_machine_pilot": True, "external_capacity_benchmark": False,
              "validator_changed": False, "block_crypto_verification": "not performed",
              "wire_integrity": "per-object and full-frame SHA-256"}
    args.report.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"state": "PASS", "clients": 10, "same_chunks": 8,
                      "upstream_chunks_during_test": after["received_chunks"] - before["received_chunks"],
                      "slowest_client_seconds": max(c["seconds"] for c in consumers)}))


if __name__ == "__main__":
    main()
