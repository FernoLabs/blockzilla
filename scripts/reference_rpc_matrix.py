#!/usr/bin/env python3
"""Build a reusable Solana RPC reference corpus.

The runner is intentionally boring:

- request bodies and response bodies are stored for every provider
- endpoint URLs are never written to artifacts, only endpoint labels
- completed calls are reused on the next run
- timing, wire bytes, retries, and first-diff paths are recorded in JSONL
"""

import argparse
import concurrent.futures
import gzip
import hashlib
import json
import os
import random
import statistics
import subprocess
import tempfile
import time
from collections import Counter
from pathlib import Path


SLOTS_PER_EPOCH = 432_000
DEFAULT_ENCODINGS = ("json", "jsonParsed", "base58", "base64")
DEFAULT_TRANSACTION_DETAILS = ("full", "accounts", "signatures", "none")
DEFAULT_REWARDS = (False, True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a resumable Solana getBlock reference matrix."
    )
    parser.add_argument("--out-dir", required=True)
    parser.add_argument(
        "--plan-file",
        help="JSON plan from this script, or an object/list containing slot rows.",
    )
    parser.add_argument(
        "--epochs",
        default="0,100,200,300,400,500,600,700,800,900,920",
        help="Comma/space separated epoch list used when --plan-file is omitted.",
    )
    parser.add_argument("--samples-per-epoch", type=int, default=100)
    parser.add_argument("--seed", type=int, default=4242)
    parser.add_argument(
        "--endpoint",
        action="append",
        required=True,
        help=(
            "Endpoint as label=url or label=@ENV_VAR. The URL is used for requests "
            "but never persisted in output artifacts."
        ),
    )
    parser.add_argument(
        "--primary",
        default="worker",
        help="Endpoint label used as the left side for diffing. Default: worker.",
    )
    parser.add_argument(
        "--encodings",
        default=",".join(DEFAULT_ENCODINGS),
        help="Comma separated getBlock encodings.",
    )
    parser.add_argument(
        "--transaction-details",
        default=",".join(DEFAULT_TRANSACTION_DETAILS),
        help="Comma separated transactionDetails values.",
    )
    parser.add_argument(
        "--rewards",
        default="false,true",
        help="Comma separated reward flags: false,true.",
    )
    parser.add_argument("--commitment", default="finalized")
    parser.add_argument("--max-supported-transaction-version", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--rate-limit-retries", type=int, default=4)
    parser.add_argument("--retry-sleep", type=float, default=2.0)
    parser.add_argument("--retry-backoff", type=float, default=2.0)
    parser.add_argument(
        "--compressed",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Ask curl for HTTP compression. Bodies saved by curl are decoded.",
    )
    parser.add_argument(
        "--store-compressed-bodies",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Store decoded response bodies as .body.gz artifacts to save disk.",
    )
    parser.add_argument("--only-endpoint", action="append", default=[])
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def split_csv(value):
    return [part.strip() for part in value.replace(" ", ",").split(",") if part.strip()]


def parse_bool(value):
    lowered = value.lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise SystemExit(f"invalid boolean value {value!r}")


def parse_endpoint(value):
    if "=" not in value:
        raise SystemExit("--endpoint must be label=url or label=@ENV_VAR")
    label, url = value.split("=", 1)
    label = label.strip()
    if not label:
        raise SystemExit("endpoint label must not be empty")
    if url.startswith("@"):
        env_name = url[1:]
        url = os.environ.get(env_name)
        if not url:
            raise SystemExit(f"environment variable {env_name} is not set")
    if not url.startswith(("http://", "https://")):
        raise SystemExit(f"endpoint {label!r} is not an HTTP URL")
    return label, url


def parse_epochs(value):
    epochs = []
    for part in split_csv(value):
        epoch = int(part)
        if epoch < 0:
            raise SystemExit(f"invalid negative epoch {epoch}")
        epochs.append(epoch)
    return epochs


def build_plan(epochs, samples_per_epoch, seed):
    rng = random.Random(seed)
    rows = []
    for epoch in epochs:
        base = epoch * SLOTS_PER_EPOCH
        rows.append(
            {
                "epoch": epoch,
                "slot": base,
                "kind": "boundary",
                "sample_index": 0,
            }
        )
        population = range(base + 1, base + SLOTS_PER_EPOCH)
        count = min(samples_per_epoch, SLOTS_PER_EPOCH - 1)
        for sample_index, slot in enumerate(sorted(rng.sample(population, count)), 1):
            rows.append(
                {
                    "epoch": epoch,
                    "slot": slot,
                    "kind": "random",
                    "sample_index": sample_index,
                }
            )
    return rows


def read_plan(path):
    value = json.loads(Path(path).read_text())
    if isinstance(value, dict):
        if "slots" in value:
            return value["slots"]
        if "plan" in value:
            return value["plan"]
    if isinstance(value, list):
        return value
    raise SystemExit(f"unrecognized plan shape in {path}")


def matrix(encodings, transaction_details, rewards):
    calls = [{"method": "getBlockTime", "call": "getBlockTime", "params": {}}]
    for encoding in encodings:
        for details in transaction_details:
            for reward in rewards:
                calls.append(
                    {
                        "method": "getBlock",
                        "call": (
                            f"getBlock:encoding={encoding}:"
                            f"details={details}:rewards={str(reward).lower()}"
                        ),
                        "params": {
                            "encoding": encoding,
                            "transactionDetails": details,
                            "rewards": reward,
                        },
                    }
                )
    return calls


def request_payload(slot, call, args):
    if call["method"] == "getBlockTime":
        return {"jsonrpc": "2.0", "id": 1, "method": "getBlockTime", "params": [slot]}
    config = {
        "commitment": args.commitment,
        "encoding": call["params"]["encoding"],
        "transactionDetails": call["params"]["transactionDetails"],
        "maxSupportedTransactionVersion": args.max_supported_transaction_version,
        "rewards": call["params"]["rewards"],
    }
    return {"jsonrpc": "2.0", "id": 1, "method": "getBlock", "params": [slot, config]}


def request_id(row, call):
    key = {
        "epoch": row["epoch"],
        "slot": row["slot"],
        "kind": row.get("kind", ""),
        "sample_index": row.get("sample_index", 0),
        "call": call["call"],
    }
    digest = hashlib.sha256(json.dumps(key, sort_keys=True).encode()).hexdigest()[:16]
    safe_call = (
        call["call"]
        .replace(":", "_")
        .replace("=", "-")
        .replace("+", "plus")
        .replace("/", "_")
    )
    return f"e{row['epoch']}-s{row['slot']}-{safe_call}-{digest}"


def endpoint_dir(out_dir, label):
    path = Path(out_dir) / "responses" / label
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_body_bytes(path):
    path = Path(path)
    if path.exists():
        if path.suffix == ".gz":
            with gzip.open(path, "rb") as file:
                return file.read()
        return path.read_bytes()
    gz_path = Path(str(path) + ".gz")
    if gz_path.exists():
        with gzip.open(gz_path, "rb") as file:
            return file.read()
    return b""


def perform_request(url, payload, args):
    data = json.dumps(payload, separators=(",", ":")).encode()
    total_started = time.perf_counter()
    attempts = 0
    rate_limit_events = 0
    transient_events = 0
    retry_sleep_s = 0.0
    final = None

    for attempt in range(args.rate_limit_retries + 1):
        attempts += 1
        with tempfile.NamedTemporaryFile(prefix="rpc-matrix-body-", delete=False) as body_file:
            body_path = body_file.name
        with tempfile.NamedTemporaryFile(prefix="rpc-matrix-headers-", delete=False) as header_file:
            header_path = header_file.name

        cmd = [
            "curl",
            "-sS",
            "--max-time",
            str(args.timeout),
            "-D",
            header_path,
            "-o",
            body_path,
            "-w",
            "%{http_code}\t%{size_download}\t%{time_total}",
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
        ]
        if args.compressed:
            cmd.append("--compressed")
        cmd.extend(["--data-binary", "@-", url])

        completed = subprocess.run(cmd, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        body = Path(body_path).read_bytes() if Path(body_path).exists() else b""
        headers = Path(header_path).read_bytes() if Path(header_path).exists() else b""
        Path(body_path).unlink(missing_ok=True)
        Path(header_path).unlink(missing_ok=True)

        parts = completed.stdout.decode(errors="replace").strip().split("\t")
        http = parts[0] if len(parts) > 0 and parts[0] else "000"
        wire_bytes = int(float(parts[1])) if len(parts) > 1 and parts[1] else len(body)
        attempt_elapsed_s = float(parts[2]) if len(parts) > 2 and parts[2] else 0.0
        parsed = None
        parse_error = ""
        try:
            parsed = json.loads(body) if body else None
        except Exception as err:  # noqa: BLE001 - diagnostic artifact
            parse_error = repr(err)

        error_text = " ".join(
            (
                completed.stderr.decode(errors="replace"),
                parse_error,
                json.dumps(parsed.get("error")) if isinstance(parsed, dict) and parsed.get("error") else "",
            )
        ).lower()
        rate_limited = (
            http == "429"
            or "too many requests" in error_text
            or "rate limit" in error_text
            or "ratelimit" in error_text
        )
        transient = (
            http in {"000", "502", "503", "504"}
            or completed.returncode != 0
            or "operation timed out" in error_text
            or bool(parse_error)
        )
        final = {
            "http": http,
            "curl_returncode": completed.returncode,
            "body": body,
            "headers": headers,
            "json": parsed,
            "parse_error": parse_error,
            "stderr": completed.stderr.decode(errors="replace"),
            "wire_bytes": wire_bytes,
            "attempt_elapsed_s": attempt_elapsed_s,
            "rate_limited": rate_limited,
            "transient": transient,
        }

        if not (rate_limited or transient) or attempt >= args.rate_limit_retries:
            break
        if rate_limited:
            rate_limit_events += 1
        else:
            transient_events += 1
        sleep_s = args.retry_sleep * (args.retry_backoff**attempt)
        time.sleep(sleep_s)
        retry_sleep_s += sleep_s

    final["elapsed_s"] = time.perf_counter() - total_started
    final["attempts"] = attempts
    final["rate_limit_events"] = rate_limit_events
    final["transient_events"] = transient_events
    final["retry_sleep_s"] = retry_sleep_s
    return final


def run_one(endpoint, row, call, args):
    label, url = endpoint
    out = endpoint_dir(args.out_dir, label)
    rid = request_id(row, call)
    body_path = out / (
        f"{rid}.body.gz" if args.store_compressed_bodies else f"{rid}.body"
    )
    headers_path = out / f"{rid}.headers"
    request_path = out / f"{rid}.request.json"
    meta_path = out / f"{rid}.meta.json"

    payload = request_payload(row["slot"], call, args)
    request_record = {
        "epoch": row["epoch"],
        "slot": row["slot"],
        "kind": row.get("kind", ""),
        "sample_index": row.get("sample_index", 0),
        "method": call["method"],
        "call": call["call"],
        "payload": payload,
    }

    if meta_path.exists() and body_path.exists():
        meta = json.loads(meta_path.read_text())
        meta["cache_hit"] = True
        return meta

    result = perform_request(url, payload, args)
    if args.store_compressed_bodies:
        body_path.write_bytes(gzip.compress(result["body"], compresslevel=1))
    else:
        body_path.write_bytes(result["body"])
    headers_path.write_bytes(result["headers"])
    request_path.write_text(json.dumps(request_record, indent=2, sort_keys=True) + "\n")
    meta = {
        **{key: value for key, value in request_record.items() if key != "payload"},
        "endpoint": label,
        "cache_hit": False,
        "http": result["http"],
        "rpc": "ok"
        if isinstance(result["json"], dict) and "error" not in result["json"]
        else "error",
        "curl_returncode": result["curl_returncode"],
        "elapsed_s": result["elapsed_s"],
        "final_attempt_elapsed_s": result["attempt_elapsed_s"],
        "body_bytes": len(result["body"]),
        "wire_bytes": result["wire_bytes"],
        "attempts": result["attempts"],
        "rate_limit_events": result["rate_limit_events"],
        "transient_events": result["transient_events"],
        "retry_sleep_s": result["retry_sleep_s"],
        "body_path": str(body_path),
        "headers_path": str(headers_path),
        "request_path": str(request_path),
        "parse_error": result["parse_error"],
        "stderr": result["stderr"][:1000],
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    return meta


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


def add_diffs(rows, primary):
    body_by_key = {}
    for row in rows:
        try:
            body_by_key[(row["endpoint"], row["slot"], row["call"])] = json.loads(
                read_body_bytes(row["body_path"])
            )
        except Exception:  # noqa: BLE001 - diagnostic artifact
            body_by_key[(row["endpoint"], row["slot"], row["call"])] = None

    for row in rows:
        if row["endpoint"] == primary:
            row["ok_vs_primary"] = ""
            row["diff_path_vs_primary"] = ""
            row["diff_vs_primary"] = ""
            continue
        left = body_by_key.get((primary, row["slot"], row["call"]))
        right = body_by_key.get((row["endpoint"], row["slot"], row["call"]))
        diff_path, diff = first_diff(rpc_payload(left), rpc_payload(right))
        row["ok_vs_primary"] = diff_path == ""
        row["diff_path_vs_primary"] = diff_path
        row["diff_vs_primary"] = diff


def percentile(values, pct):
    if not values:
        return 0.0
    values = sorted(values)
    index = max(0, min(len(values) - 1, int(len(values) * pct + 0.999999) - 1))
    return values[index]


def summarize(rows):
    times = [float(row["elapsed_s"]) for row in rows]
    return {
        "attempted": len(rows),
        "ok_http_rpc": sum(1 for row in rows if row["http"] == "200" and row["rpc"] == "ok"),
        "errors": sum(1 for row in rows if not (row["http"] == "200" and row["rpc"] == "ok")),
        "ok_vs_primary": sum(1 for row in rows if row.get("ok_vs_primary") is True),
        "mismatch_vs_primary": sum(1 for row in rows if row.get("ok_vs_primary") is False),
        "http": dict(Counter(row["http"] for row in rows)),
        "rpc": dict(Counter(row["rpc"] for row in rows)),
        "avg_s": sum(times) / len(times) if times else 0.0,
        "p50_s": statistics.median(times) if times else 0.0,
        "p90_s": percentile(times, 0.90),
        "p95_s": percentile(times, 0.95),
        "p99_s": percentile(times, 0.99),
        "max_s": max(times) if times else 0.0,
        "body_bytes": sum(int(row["body_bytes"]) for row in rows),
        "wire_bytes": sum(int(row["wire_bytes"]) for row in rows),
        "attempts": sum(int(row["attempts"]) for row in rows),
        "rate_limit_events": sum(int(row["rate_limit_events"]) for row in rows),
        "transient_events": sum(int(row["transient_events"]) for row in rows),
        "retry_sleep_s": sum(float(row["retry_sleep_s"]) for row in rows),
        "diff_paths_vs_primary": dict(
            Counter(row["diff_path_vs_primary"] for row in rows if row.get("diff_path_vs_primary"))
        ),
    }


def write_outputs(out_dir, rows, labels, plan, calls, args):
    if len(labels) > 1 and args.primary in labels:
        add_diffs(rows, args.primary)
    else:
        for row in rows:
            row["ok_vs_primary"] = ""
            row["diff_path_vs_primary"] = ""
            row["diff_vs_primary"] = ""
    out = Path(out_dir)
    rows_path = out / "rows.jsonl"
    with rows_path.open("w") as file:
        for row in rows:
            file.write(json.dumps(row, sort_keys=True) + "\n")

    summary = {
        "plan": {
            "slot_count": len(plan),
            "calls_per_slot": len(calls),
            "logical_calls_per_endpoint": len(plan) * len(calls),
            "endpoint_labels": labels,
            "primary": args.primary,
            "encodings": split_csv(args.encodings),
            "transactionDetails": split_csv(args.transaction_details),
            "rewards": [parse_bool(value) for value in split_csv(args.rewards)],
        },
        "artifacts": {
            "rows": str(rows_path),
            "responses_dir": str(out / "responses"),
        },
        "endpoints": {},
    }
    for label in labels:
        endpoint_rows = [row for row in rows if row["endpoint"] == label]
        summary["endpoints"][label] = {
            "global": summarize(endpoint_rows),
            "by_call": {},
        }
        for call in calls:
            call_rows = [row for row in endpoint_rows if row["call"] == call["call"]]
            summary["endpoints"][label]["by_call"][call["call"]] = summarize(call_rows)

    summary_path = out / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def main():
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    endpoints = [parse_endpoint(value) for value in args.endpoint]
    if args.only_endpoint:
        allowed = set(args.only_endpoint)
        endpoints = [endpoint for endpoint in endpoints if endpoint[0] in allowed]
    labels = [label for label, _url in endpoints]
    if args.primary not in labels and not args.only_endpoint:
        raise SystemExit(f"primary endpoint {args.primary!r} is not in endpoint list")

    plan = read_plan(args.plan_file) if args.plan_file else build_plan(
        parse_epochs(args.epochs), args.samples_per_epoch, args.seed
    )
    calls = matrix(
        split_csv(args.encodings),
        split_csv(args.transaction_details),
        [parse_bool(value) for value in split_csv(args.rewards)],
    )
    plan_record = {
        "slots_per_epoch": SLOTS_PER_EPOCH,
        "slot_count": len(plan),
        "calls_per_slot": len(calls),
        "logical_calls_per_endpoint": len(plan) * len(calls),
        "total_logical_calls": len(plan) * len(calls) * len(endpoints),
        "slots": plan,
        "calls": calls,
    }
    (out_dir / "plan.json").write_text(json.dumps(plan_record, indent=2, sort_keys=True) + "\n")
    (out_dir / "endpoints.json").write_text(
        json.dumps({"labels": labels, "primary": args.primary}, indent=2, sort_keys=True) + "\n"
    )

    print(
        json.dumps(
            {
                "out_dir": str(out_dir),
                "slot_count": len(plan),
                "calls_per_slot": len(calls),
                "labels": labels,
                "total_logical_calls": len(plan) * len(calls) * len(endpoints),
            },
            sort_keys=True,
        )
    )
    if args.dry_run:
        return

    rows = []
    for endpoint in endpoints:
        label = endpoint[0]
        jobs = [(endpoint, row, call, args) for row in plan for call in calls]
        print(f"endpoint={label} jobs={len(jobs)}", flush=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
            futures = [pool.submit(run_one, *job) for job in jobs]
            for index, future in enumerate(concurrent.futures.as_completed(futures), 1):
                row = future.result()
                rows.append(row)
                if index % 50 == 0 or index == len(futures):
                    print(
                        f"endpoint={label} completed={index}/{len(futures)} "
                        f"last_call={row['call']} slot={row['slot']} "
                        f"http={row['http']} rpc={row['rpc']} "
                        f"elapsed={float(row['elapsed_s']):.3f}s",
                        flush=True,
                    )
        write_outputs(out_dir, rows, labels, plan, calls, args)

    summary = write_outputs(out_dir, rows, labels, plan, calls, args)
    print(json.dumps(summary["endpoints"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
