#!/usr/bin/env python3
"""Build a benchmark prefix plan from a separately accepted Compact V2 index.

Accept a raw BZV2HIX1 index or a BZHTTPC1 HTTP cache file. The required payload
hash must come from independent evidence, not from the benchmark being checked.
The script performs no network requests and does not overwrite an output.
"""

import argparse
import hashlib
import json
from pathlib import Path
import struct


def require(condition, message):
    if not condition:
        raise ValueError(message)


def extract(args):
    source = args.index.read_bytes()
    payload = source
    provenance = {"source_path": str(args.index.resolve()),
                  "source_sha256": hashlib.sha256(source).hexdigest()}
    if source[:8] == b"BZHTTPC1":
        require(len(source) >= 64, "short HTTP cache header")
        version, fixed, header, size, epoch, base_len, object_len, etag_len = \
            struct.unpack_from("<HHIQQHHH", source, 8)
        require(version == 1 and fixed == 64, "unsupported HTTP cache header")
        require(source[38:64] == bytes(26), "nonzero HTTP cache reserved bytes")
        require(header == 64 + base_len + object_len + etag_len,
                "HTTP cache field lengths differ")
        require(len(source) == header + size, "HTTP cache payload length differs")
        require(epoch == args.epoch, "HTTP cache epoch differs")
        cursor = 64
        for key, length in (("base_url", base_len), ("object", object_len),
                            ("strong_etag", etag_len)):
            provenance[key] = source[cursor:cursor + length].decode("utf-8")
            cursor += length
        require(provenance["object"] == "archive-v2-blocks.index",
                "HTTP cache object differs")
        payload = source[header:]

    index_hash = hashlib.sha256(payload).hexdigest()
    require(index_hash == args.expected_index_sha256.lower(),
            "index payload does not match the independently accepted hash")
    require(len(payload) >= 36 and payload[:8] == b"BZV2HIX1",
            "short or invalid Compact V2 index header")
    version, reserved, row_count, blob_bytes, level, flags = \
        struct.unpack_from("<HHQQiI", payload, 8)
    require(version == 1 and reserved == 0 and flags & ~3 == 0,
            "unsupported Compact V2 index header")
    require(len(payload) == 36 + row_count * 52, "index row length differs")
    require(0 < args.blocks <= row_count, "block count is outside the index")

    rows = []
    tx_ordinal = signature_ordinal = offset = 0
    previous_slot = args.start_slot - 1
    for number, row in enumerate(struct.iter_unpack("<IQQIIIQQI", payload[36:])):
        block_id, slot, block_offset, compressed_len, _, tx_count, first_tx, \
            first_signature, signature_count = row
        require(block_id == number and slot > previous_slot,
                f"invalid block id or slot order at row {number}")
        require(block_offset == offset and first_tx == tx_ordinal
                and first_signature == signature_ordinal,
                f"invalid cumulative index field at row {number}")
        if number < args.blocks:
            rows.append([slot, tx_count])
        previous_slot = slot
        offset += compressed_len
        tx_ordinal += tx_count
        signature_ordinal += signature_count
    require(offset == blob_bytes, "index does not cover its declared blob")
    require(rows[0][0] == args.start_slot, "index does not start at requested slot")
    provenance.update(index_sha256=index_hash, index_bytes=len(payload),
                      index_block_count=row_count, index_transactions=tx_ordinal,
                      index_level=level, index_flags=flags,
                      accepted_index_sha256=args.expected_index_sha256.lower())
    return {"epoch": args.epoch, "start_slot": args.start_slot,
            "end_slot_exclusive": rows[-1][0] + 1,
            "block_transaction_rows": rows, "source_index": provenance}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--epoch", type=int, required=True)
    parser.add_argument("--start-slot", type=int, required=True)
    parser.add_argument("--blocks", type=int, required=True)
    parser.add_argument("--expected-index-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = extract(args)
    with args.output.open("x") as output:
        json.dump(plan, output, separators=(",", ":"))
        output.write("\n")
    rows = plan["block_transaction_rows"]
    print(json.dumps({"output": str(args.output), "blocks": len(rows),
                      "transactions": sum(row[1] for row in rows),
                      "end_slot_exclusive": plan["end_slot_exclusive"],
                      "index_sha256": plan["source_index"]["index_sha256"]}))


if __name__ == "__main__":
    main()
