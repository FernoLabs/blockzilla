#!/usr/bin/env python3
"""Compare two saved JSON-RPC response corpora."""

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left-rows", required=True)
    parser.add_argument("--right-rows", required=True)
    parser.add_argument("--left-label", default="left")
    parser.add_argument("--right-label", default="right")
    parser.add_argument("--out-dir", required=True)
    return parser.parse_args()


def load_rows(path):
    rows = {}
    with Path(path).open() as file:
        for line in file:
            row = json.loads(line)
            rows[(int(row["slot"]), row["call"])] = row
    return rows


def read_body_bytes(path):
    path = Path(path)
    if not path.exists():
        gz_path = Path(str(path) + ".gz")
        if gz_path.exists():
            path = gz_path
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as file:
            return file.read()
    return path.read_bytes()


def read_body(path):
    return json.loads(read_body_bytes(path))


def rpc_payload(value):
    if not isinstance(value, dict):
        return value
    if "error" in value:
        return {"error": value.get("error")}
    return value.get("result")


def first_diff(left, right, path="$"):
    if type(left) is not type(right):
        return path, f"type left={type(left).__name__} right={type(right).__name__}"
    if isinstance(left, dict):
        left_keys = set(left)
        right_keys = set(right)
        if left_keys != right_keys:
            missing = sorted(right_keys - left_keys)
            extra = sorted(left_keys - right_keys)
            if missing:
                return f"{path}.{missing[0]}", "missing in left"
            return f"{path}.{extra[0]}", "extra in left"
        for key in sorted(left_keys):
            diff_path, diff = first_diff(left[key], right[key], f"{path}.{key}")
            if diff_path:
                return diff_path, diff
        return "", ""
    if isinstance(left, list):
        if len(left) != len(right):
            return path, f"list length left={len(left)} right={len(right)}"
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            diff_path, diff = first_diff(left_item, right_item, f"{path}[{index}]")
            if diff_path:
                return diff_path, diff
        return "", ""
    if left != right:
        return path, f"left={left!r} right={right!r}"[:500]
    return "", ""


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    left_rows = load_rows(args.left_rows)
    right_rows = load_rows(args.right_rows)
    common = sorted(set(left_rows) & set(right_rows))
    missing_left = sorted(set(right_rows) - set(left_rows))
    missing_right = sorted(set(left_rows) - set(right_rows))

    mismatch_path = out_dir / "mismatches.jsonl"
    matched = 0
    mismatched = 0
    diff_paths = Counter()
    errors = Counter()

    with mismatch_path.open("w") as mismatch_file:
        for index, key in enumerate(common, 1):
            left_row = left_rows[key]
            right_row = right_rows[key]
            try:
                left_bytes = read_body_bytes(left_row["body_path"])
                right_bytes = read_body_bytes(right_row["body_path"])
                if left_bytes == right_bytes:
                    diff_path = ""
                    diff = ""
                else:
                    left = rpc_payload(json.loads(left_bytes))
                    right = rpc_payload(json.loads(right_bytes))
                    diff_path, diff = first_diff(left, right)
            except Exception as err:  # noqa: BLE001 - diagnostic artifact
                diff_path = "$"
                diff = f"compare error: {err!r}"
                errors[type(err).__name__] += 1

            if diff_path:
                mismatched += 1
                diff_paths[diff_path] += 1
                mismatch_file.write(
                    json.dumps(
                        {
                            "slot": key[0],
                            "call": key[1],
                            "diff_path": diff_path,
                            "diff": diff,
                            "left_body_path": left_row["body_path"],
                            "right_body_path": right_row["body_path"],
                            "left_rpc": left_row.get("rpc"),
                            "right_rpc": right_row.get("rpc"),
                        },
                        sort_keys=True,
                    )
                    + "\n"
                )
            else:
                matched += 1
            if index % 500 == 0:
                print(
                    f"compared={index}/{len(common)} "
                    f"matched={matched} mismatched={mismatched}",
                    flush=True,
                )

    summary = {
        "left_label": args.left_label,
        "right_label": args.right_label,
        "common": len(common),
        "matched": matched,
        "mismatched": mismatched,
        "missing_left": len(missing_left),
        "missing_right": len(missing_right),
        "diff_paths": dict(diff_paths),
        "compare_errors": dict(errors),
        "artifacts": {"mismatches": str(mismatch_path)},
    }
    summary_path = out_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
