#!/usr/bin/env python3
import argparse
import concurrent.futures
import csv
import json
import os
import random
import re
import statistics
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path


SLOTS_PER_EPOCH = 432_000
RAW_STRIDE = 12
V2_STRIDE = 44
DEFAULT_ENDPOINT = "https://worker.example.com/"
PROFILE_FIELDS = [
    "profile_total_ms",
    "profile_slot_index_ms",
    "profile_old_faithful_download_ms",
    "profile_old_faithful_bytes",
    "profile_block_range_len",
    "profile_previous_blockhash_ms",
    "profile_parent_slot_index_ms",
    "profile_parent_block_download_ms",
    "profile_parent_block_download_bytes",
    "profile_render_total_ms",
    "profile_render_parse_zstd_ms",
    "profile_render_write_transactions_ms",
    "profile_render_write_transaction_json_ms",
    "profile_render_write_meta_json_ms",
    "profile_render_output_bytes",
    "profile_render_transactions",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark getBlock over epoch boundary slots plus random present slots "
            "and emit per-epoch/global reports."
        )
    )
    parser.add_argument(
        "epochs",
        nargs="*",
        help=(
            "Epoch specs: 10, 10-20, 10,50,100, or 'available'. "
            "Default: available."
        ),
    )
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument(
        "--slot-index-dir",
        default=os.environ.get("SLOT_INDEX_DIR", "/srv/blockzilla/blockzilla/slot-index"),
    )
    parser.add_argument("--plan-file", help="Read epoch/slot plan TSV instead of scanning indexes.")
    parser.add_argument("--samples-per-epoch", type=int, default=100)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--require-v2",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Only use epoch-N-slot-ranges-v2.raw indexes. "
            "Default: enabled so benchmarks avoid legacy indexes without previous blockhash."
        ),
    )
    parser.add_argument("--prefer-v2", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--transport", choices=("curl", "urllib"), default="curl")
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Append profile=1 and capture Worker X-OF-Profile timings in request TSV rows.",
    )
    parser.add_argument(
        "--compressed",
        action="store_true",
        help="Use curl --compressed to request compressed HTTP responses and decode them locally.",
    )
    parser.add_argument(
        "--rate-limit-retries",
        type=int,
        default=0,
        help=(
            "Retry a logical request after HTTP 429 or JSON-RPC rate-limit errors. "
            "Use this for external RPC providers; leave at 0 for Worker timing."
        ),
    )
    parser.add_argument(
        "--rate-limit-sleep",
        type=float,
        default=1.0,
        help="Initial sleep before retrying a rate-limited request.",
    )
    parser.add_argument(
        "--rate-limit-backoff",
        type=float,
        default=1.0,
        help="Multiplier applied to --rate-limit-sleep after each rate-limit retry.",
    )
    parser.add_argument(
        "--rate-limit-max-sleep",
        type=float,
        default=0.0,
        help="Optional cap for retry sleep; 0 means uncapped.",
    )
    parser.add_argument(
        "--transient-retries",
        type=int,
        default=0,
        help="Retry transient HTTP/RPC failures such as 502, 503, 504, and upstream 503.",
    )
    parser.add_argument(
        "--transient-sleep",
        type=float,
        default=1.0,
        help="Initial sleep before retrying a transient failure.",
    )
    parser.add_argument(
        "--transient-backoff",
        type=float,
        default=1.0,
        help="Multiplier applied to --transient-sleep after each transient retry.",
    )
    parser.add_argument(
        "--transient-max-sleep",
        type=float,
        default=0.0,
        help="Optional cap for transient retry sleep; 0 means uncapped.",
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="Extra HTTP header, repeatable, e.g. --header 'Authorization: Bearer ...'.",
    )
    parser.add_argument("--commitment", default="finalized")
    parser.add_argument("--encoding", default="json")
    parser.add_argument(
        "--transaction-details",
        default="none",
        choices=("full", "accounts", "signatures", "none"),
    )
    parser.add_argument("--max-supported-transaction-version", type=int, default=0)
    parser.add_argument("--rewards", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--prefix", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--progress-every", type=int, default=100)
    return parser.parse_args()


def expand_epoch_specs(specs, slot_index_dir, require_v2):
    if not specs:
        specs = ["available"]

    out = []
    seen = set()
    for spec in specs:
        for part in spec.replace(",", " ").split():
            if not part:
                continue
            if part == "available":
                values = available_epochs(slot_index_dir, require_v2)
            elif "-" in part:
                start_s, end_s = part.split("-", 1)
                start = int(start_s)
                end = int(end_s)
                step = 1 if end >= start else -1
                values = range(start, end + step, step)
            else:
                values = [int(part)]
            for epoch in values:
                if epoch < 0:
                    raise SystemExit(f"invalid negative epoch {epoch}")
                if epoch not in seen:
                    seen.add(epoch)
                    out.append(epoch)
    return out


def available_epochs(slot_index_dir, require_v2):
    epochs = set()
    if require_v2:
        pattern = re.compile(r"epoch-(\d+)-slot-ranges-v2\.raw$")
    else:
        pattern = re.compile(r"epoch-(\d+)-slot-ranges(?:-v2)?\.raw$")
    for name in os.listdir(slot_index_dir):
        match = pattern.fullmatch(name)
        if match:
            epochs.add(int(match.group(1)))
    return sorted(epochs)


def index_path(slot_index_dir, epoch, prefer_v2, require_v2):
    raw = Path(slot_index_dir) / f"epoch-{epoch}-slot-ranges.raw"
    v2 = Path(slot_index_dir) / f"epoch-{epoch}-slot-ranges-v2.raw"
    if require_v2:
        if v2.is_file():
            return v2, V2_STRIDE
        raise FileNotFoundError(
            f"missing v2 slot index for epoch {epoch} under {slot_index_dir}"
        )
    if prefer_v2 and v2.is_file():
        return v2, V2_STRIDE
    if raw.is_file():
        return raw, RAW_STRIDE
    if v2.is_file():
        return v2, V2_STRIDE
    raise FileNotFoundError(f"missing slot index for epoch {epoch} under {slot_index_dir}")


def present_slots(epoch, path, stride):
    data = path.read_bytes()
    expected = SLOTS_PER_EPOCH * stride
    if len(data) != expected:
        raise SystemExit(f"{path} has {len(data)} bytes, expected {expected}")

    slots = []
    for slot_in_epoch in range(SLOTS_PER_EPOCH):
        off = slot_in_epoch * stride
        length = struct.unpack_from("<I", data, off + 8)[0]
        if length:
            slots.append(epoch * SLOTS_PER_EPOCH + slot_in_epoch)
    return slots


def build_plan(args):
    rng = random.Random(args.seed)
    rows = []
    for epoch in expand_epoch_specs(args.epochs, args.slot_index_dir, args.require_v2):
        path, stride = index_path(
            args.slot_index_dir, epoch, args.prefer_v2, args.require_v2
        )
        slots = present_slots(epoch, path, stride)
        if not slots:
            print(f"epoch={epoch}: no present slots in {path}", file=sys.stderr)
            continue

        first = slots[0]
        last = slots[-1]
        rows.append(
            {
                "epoch": epoch,
                "slot": first,
                "kind": "bound_first",
                "sample_index": 0,
                "present_slots": len(slots),
                "index_path": str(path),
            }
        )

        interior = slots[1:-1] if len(slots) > 2 else []
        count = min(args.samples_per_epoch, len(interior))
        for sample_index, slot in enumerate(sorted(rng.sample(interior, count)), 1):
            rows.append(
                {
                    "epoch": epoch,
                    "slot": slot,
                    "kind": "random",
                    "sample_index": sample_index,
                    "present_slots": len(slots),
                    "index_path": str(path),
                }
            )

        if last != first:
            rows.append(
                {
                    "epoch": epoch,
                    "slot": last,
                    "kind": "bound_last",
                    "sample_index": 0,
                    "present_slots": len(slots),
                    "index_path": str(path),
                }
            )

        print(
            f"epoch={epoch} present={len(slots)} samples={count} "
            f"first={first} last={last} index={path}",
            file=sys.stderr,
        )
    return rows


def read_plan(path, require_v2):
    with open(path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = []
        for row in reader:
            index = row.get("index_path", "")
            if require_v2 and not index.endswith("-slot-ranges-v2.raw"):
                raise SystemExit(
                    f"{path}: plan row for epoch {row.get('epoch')} slot {row.get('slot')} "
                    "does not use a v2 slot index"
                )
            rows.append(
                {
                    "epoch": int(row["epoch"]),
                    "slot": int(row["slot"]),
                    "kind": row["kind"],
                    "sample_index": int(row.get("sample_index") or 0),
                    "present_slots": int(row.get("present_slots") or 0),
                    "index_path": index,
                }
            )
    return rows


def write_plan(path, rows):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            delimiter="\t",
            fieldnames=[
                "epoch",
                "slot",
                "kind",
                "sample_index",
                "present_slots",
                "index_path",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def request_slot(args, row):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [
            row["slot"],
            {
                "commitment": args.commitment,
                "encoding": args.encoding,
                "transactionDetails": args.transaction_details,
                "maxSupportedTransactionVersion": args.max_supported_transaction_version,
                "rewards": args.rewards,
            },
        ],
    }
    data = json.dumps(payload, separators=(",", ":")).encode()
    logical_started = time.perf_counter()
    rate_limit_sleep_s = 0.0
    rate_limit_response_elapsed_s = 0.0
    rate_limit_events = 0
    transient_response_elapsed_s = 0.0
    transient_sleep_s = 0.0
    transient_events = 0
    final = None
    max_attempts = max(args.rate_limit_retries, args.transient_retries) + 1

    attempts = 0
    for attempt_index in range(max_attempts):
        result = request_slot_once(args, row, data)
        final = result
        attempts = attempt_index + 1

        retry_kind = retryable_kind(result)
        if retry_kind is None:
            break

        if retry_kind == "rate_limit":
            rate_limit_events += 1
            rate_limit_response_elapsed_s += float(result["elapsed_s"])
            retries_remaining = rate_limit_events <= args.rate_limit_retries
            sleep_s = retry_sleep(
                args.rate_limit_sleep,
                args.rate_limit_backoff,
                args.rate_limit_max_sleep,
                rate_limit_events - 1,
            )
        else:
            transient_events += 1
            transient_response_elapsed_s += float(result["elapsed_s"])
            retries_remaining = transient_events <= args.transient_retries
            sleep_s = retry_sleep(
                args.transient_sleep,
                args.transient_backoff,
                args.transient_max_sleep,
                transient_events - 1,
            )

        if not retries_remaining:
            break

        if sleep_s > 0:
            time.sleep(sleep_s)
            if retry_kind == "rate_limit":
                rate_limit_sleep_s += sleep_s
            else:
                transient_sleep_s += sleep_s

    elapsed_s = time.perf_counter() - logical_started
    final["final_attempt_elapsed_s"] = final["elapsed_s"]
    final["elapsed_s"] = elapsed_s
    final["attempts"] = attempts
    final["rate_limited"] = 1 if rate_limit_events else 0
    final["rate_limit_events"] = rate_limit_events
    final["rate_limit_retries"] = max(0, attempts - 1) if rate_limit_events else 0
    final["rate_limit_response_elapsed_s"] = rate_limit_response_elapsed_s
    final["rate_limit_sleep_s"] = rate_limit_sleep_s
    final["rate_limit_waste_s"] = rate_limit_response_elapsed_s + rate_limit_sleep_s
    final["transient_retried"] = 1 if transient_events else 0
    final["transient_events"] = transient_events
    final["transient_retries"] = max(0, attempts - 1) if transient_events else 0
    final["transient_response_elapsed_s"] = transient_response_elapsed_s
    final["transient_sleep_s"] = transient_sleep_s
    final["transient_waste_s"] = transient_response_elapsed_s + transient_sleep_s
    return final


def retry_sleep(base_sleep, backoff, max_sleep, retry_index):
    sleep_s = base_sleep * (backoff ** retry_index)
    if max_sleep > 0:
        sleep_s = min(sleep_s, max_sleep)
    return sleep_s


def retryable_kind(result):
    if is_rate_limited(result):
        return "rate_limit"
    if is_transient_failure(result):
        return "transient"
    return None


def is_rate_limited(result):
    if str(result.get("http")) == "429":
        return True

    error = str(result.get("error") or "").lower()
    return (
        '"code":429' in error
        or "'code': 429" in error
        or "too many requests" in error
        or "rate limit" in error
        or "rate-limit" in error
        or "ratelimit" in error
    )


def is_transient_failure(result):
    if str(result.get("http")) in {"502", "503", "504"}:
        return True

    error = str(result.get("error") or "").lower()
    return (
        "returned http 502" in error
        or "returned http 503" in error
        or "returned http 504" in error
        or "bad gateway" in error
        or "service unavailable" in error
        or "gateway timeout" in error
        or "operation timed out" in error
    )


def request_slot_once(args, row, data):
    if args.transport == "curl":
        return request_slot_curl(args, row, data)
    return request_slot_urllib(args, row, data)


def request_slot_curl(args, row, data):
    body_path = None
    headers_path = None
    started = time.perf_counter()
    http = "000"
    body = b""
    profile = None
    error = ""
    elapsed = 0.0
    wire_bytes = 0
    try:
        with tempfile.NamedTemporaryFile(prefix="rpc-bench-body-", delete=False) as body_file:
            body_path = body_file.name
        if args.profile:
            with tempfile.NamedTemporaryFile(
                prefix="rpc-bench-headers-", delete=False
            ) as headers_file:
                headers_path = headers_file.name
        cmd = [
            "curl",
            "-sS",
            "--max-time",
            str(args.timeout),
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
        if headers_path:
            cmd.extend(["-D", headers_path])
        for header in args.header:
            cmd.extend(["-H", header])
        cmd.extend(["--data-binary", "@-", request_endpoint(args)])
        completed = subprocess.run(
            cmd,
            input=data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        metrics = completed.stdout.decode(errors="replace").strip().split("\t")
        if len(metrics) == 3:
            http, size, total = metrics
            try:
                wire_bytes = int(float(size))
            except ValueError:
                wire_bytes = 0
            try:
                elapsed = float(total)
            except ValueError:
                elapsed = time.perf_counter() - started
        else:
            elapsed = time.perf_counter() - started
        if completed.returncode != 0:
            error = completed.stderr.decode(errors="replace").strip()
        if body_path:
            with open(body_path, "rb") as f:
                body = f.read()
        if headers_path:
            profile = parse_profile_headers(Path(headers_path).read_text(errors="replace"))
    except Exception as exc:
        elapsed = time.perf_counter() - started
        error = repr(exc)
    finally:
        if body_path:
            try:
                os.unlink(body_path)
            except OSError:
                pass
        if headers_path:
            try:
                os.unlink(headers_path)
            except OSError:
                pass

    if elapsed == 0.0:
        elapsed = time.perf_counter() - started
    return finish_result(args, row, http, body, elapsed, error, profile=profile, wire_bytes=wire_bytes)


def request_slot_urllib(args, row, data):
    req = urllib.request.Request(
        request_endpoint(args),
        data=data,
        headers=urllib_headers(args.header),
        method="POST",
    )

    started = time.perf_counter()
    http = "000"
    body = b""
    profile = None
    error = ""
    try:
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:
            http = str(resp.status)
            profile = parse_profile_value(resp.headers.get("x-of-profile"))
            body = resp.read()
    except urllib.error.HTTPError as exc:
        http = str(exc.code)
        body = exc.read()
        error = str(exc)
    except Exception as exc:
        error = repr(exc)
    elapsed = time.perf_counter() - started

    rpc = "unknown"
    if body:
        try:
            parsed = json.loads(body)
            rpc = "error" if "error" in parsed else "ok"
            if "error" in parsed and not error:
                error = json.dumps(parsed["error"], separators=(",", ":"))
        except Exception as exc:
            rpc = "invalid_json"
            if not error:
                error = repr(exc)
    elif not error:
        rpc = "empty"

    return finish_result(args, row, http, body, elapsed, error, rpc=rpc, profile=profile)


def finish_result(args, row, http, body, elapsed, error, rpc=None, profile=None, wire_bytes=None):
    if rpc is None:
        rpc = "unknown"
        if body:
            try:
                parsed = json.loads(body)
                rpc = "error" if "error" in parsed else "ok"
                if "error" in parsed and not error:
                    error = json.dumps(parsed["error"], separators=(",", ":"))
            except Exception as exc:
                rpc = "invalid_json"
                if not error:
                    error = repr(exc)
        elif not error:
            rpc = "empty"

    result = {
        "endpoint": args.endpoint,
        "epoch": row["epoch"],
        "slot": row["slot"],
        "kind": row["kind"],
        "sample_index": row["sample_index"],
        "http": http,
        "rpc": rpc,
        "bytes": len(body),
        "wire_bytes": len(body) if wire_bytes is None else wire_bytes,
        "elapsed_s": elapsed,
        "error": one_line(error),
    }
    result.update(profile_fields(profile))
    return result


def request_endpoint(args):
    if not args.profile:
        return args.endpoint

    parsed = urllib.parse.urlsplit(args.endpoint)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    filtered = [(key, value) for key, value in query if key != "profile"]
    filtered.append(("profile", "1"))
    return urllib.parse.urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            urllib.parse.urlencode(filtered),
            parsed.fragment,
        )
    )


def parse_profile_headers(headers):
    value = None
    for line in headers.splitlines():
        if line.lower().startswith("x-of-profile:"):
            value = line.split(":", 1)[1].strip()
    return parse_profile_value(value)


def parse_profile_value(value):
    if not value:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def profile_fields(profile):
    fields = {key: "" for key in PROFILE_FIELDS}
    if not isinstance(profile, dict):
        return fields

    fetch = profile.get("fetch") if isinstance(profile.get("fetch"), dict) else {}
    render = profile.get("render") if isinstance(profile.get("render"), dict) else {}
    values = {
        "profile_total_ms": profile.get("totalMs"),
        "profile_slot_index_ms": fetch.get("slotIndexMs"),
        "profile_old_faithful_download_ms": fetch.get("oldFaithfulDownloadMs"),
        "profile_old_faithful_bytes": fetch.get("oldFaithfulBytes"),
        "profile_block_range_len": fetch.get("blockRangeLen"),
        "profile_previous_blockhash_ms": fetch.get("previousBlockhashMs"),
        "profile_parent_slot_index_ms": fetch.get("parentSlotIndexMs"),
        "profile_parent_block_download_ms": fetch.get("parentBlockDownloadMs"),
        "profile_parent_block_download_bytes": fetch.get("parentBlockDownloadBytes"),
        "profile_render_total_ms": render.get("totalMs"),
        "profile_render_parse_zstd_ms": render.get("parseZstdAndDecodeTransactionsMs"),
        "profile_render_write_transactions_ms": render.get("writeTransactionsMs"),
        "profile_render_write_transaction_json_ms": render.get("writeTransactionJsonMs"),
        "profile_render_write_meta_json_ms": render.get("writeMetaJsonMs"),
        "profile_render_output_bytes": render.get("outputBytes"),
        "profile_render_transactions": render.get("transactions"),
    }
    for key, value in values.items():
        if value is not None:
            fields[key] = value
    return fields


def one_line(value):
    return str(value).replace("\t", " ").replace("\n", " ").replace("\r", " ")


def urllib_headers(extra_headers):
    headers = {"Content-Type": "application/json"}
    for header in extra_headers:
        if ":" not in header:
            continue
        name, value = header.split(":", 1)
        headers[name.strip()] = value.strip()
    return headers


def percentile(sorted_values, pct):
    if not sorted_values:
        return 0.0
    index = max(0, min(len(sorted_values) - 1, int(len(sorted_values) * pct + 0.999999) - 1))
    return sorted_values[index]


def summarize_rows(rows):
    if not rows:
        return {
            "attempted": 0,
            "ok": 0,
            "errors": 0,
            "bytes": 0,
            "wire_bytes": 0,
            "attempts": 0,
            "request_elapsed_sum_s": 0.0,
            "rate_limited": 0,
            "rate_limit_events": 0,
            "rate_limit_retries": 0,
            "rate_limit_response_elapsed_s": 0.0,
            "rate_limit_sleep_s": 0.0,
            "rate_limit_waste_s": 0.0,
            "transient_retried": 0,
            "transient_events": 0,
            "transient_retries": 0,
            "transient_response_elapsed_s": 0.0,
            "transient_sleep_s": 0.0,
            "transient_waste_s": 0.0,
            "min_s": 0.0,
            "p50_s": 0.0,
            "p90_s": 0.0,
            "p95_s": 0.0,
            "p99_s": 0.0,
            "max_s": 0.0,
            "avg_s": 0.0,
            "http": "",
            "rpc": "",
        }
    times = sorted(float(row["elapsed_s"]) for row in rows)
    ok = sum(1 for row in rows if row["http"] == "200" and row["rpc"] == "ok")
    http_counts = Counter(row["http"] for row in rows)
    rpc_counts = Counter(row["rpc"] for row in rows)
    request_elapsed_sum_s = sum(times)
    return {
        "attempted": len(rows),
        "ok": ok,
        "errors": len(rows) - ok,
        "bytes": sum(int(row["bytes"]) for row in rows),
        "wire_bytes": sum(int(row.get("wire_bytes", row["bytes"])) for row in rows),
        "attempts": sum(int(row.get("attempts", 1)) for row in rows),
        "request_elapsed_sum_s": request_elapsed_sum_s,
        "rate_limited": sum(int(row.get("rate_limited", 0)) for row in rows),
        "rate_limit_events": sum(int(row.get("rate_limit_events", 0)) for row in rows),
        "rate_limit_retries": sum(int(row.get("rate_limit_retries", 0)) for row in rows),
        "rate_limit_response_elapsed_s": sum(
            float(row.get("rate_limit_response_elapsed_s", 0.0)) for row in rows
        ),
        "rate_limit_sleep_s": sum(float(row.get("rate_limit_sleep_s", 0.0)) for row in rows),
        "rate_limit_waste_s": sum(float(row.get("rate_limit_waste_s", 0.0)) for row in rows),
        "transient_retried": sum(int(row.get("transient_retried", 0)) for row in rows),
        "transient_events": sum(int(row.get("transient_events", 0)) for row in rows),
        "transient_retries": sum(int(row.get("transient_retries", 0)) for row in rows),
        "transient_response_elapsed_s": sum(
            float(row.get("transient_response_elapsed_s", 0.0)) for row in rows
        ),
        "transient_sleep_s": sum(float(row.get("transient_sleep_s", 0.0)) for row in rows),
        "transient_waste_s": sum(float(row.get("transient_waste_s", 0.0)) for row in rows),
        "min_s": times[0],
        "p50_s": statistics.median(times),
        "p90_s": percentile(times, 0.90),
        "p95_s": percentile(times, 0.95),
        "p99_s": percentile(times, 0.99),
        "max_s": times[-1],
        "avg_s": sum(times) / len(times),
        "http": ",".join(f"{key}:{value}" for key, value in sorted(http_counts.items())),
        "rpc": ",".join(f"{key}:{value}" for key, value in sorted(rpc_counts.items())),
    }


def write_request_rows(path, rows):
    fields = [
        "endpoint",
        "epoch",
        "slot",
        "kind",
        "sample_index",
        "http",
        "rpc",
        "bytes",
        "wire_bytes",
        "elapsed_s",
        "final_attempt_elapsed_s",
        "attempts",
        "rate_limited",
        "rate_limit_events",
        "rate_limit_retries",
        "rate_limit_response_elapsed_s",
        "rate_limit_sleep_s",
        "rate_limit_waste_s",
        "transient_retried",
        "transient_events",
        "transient_retries",
        "transient_response_elapsed_s",
        "transient_sleep_s",
        "transient_waste_s",
        *PROFILE_FIELDS,
        "error",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=fields)
        writer.writeheader()
        for row in rows:
            out = dict(row)
            out["elapsed_s"] = f"{row['elapsed_s']:.6f}"
            out["final_attempt_elapsed_s"] = f"{row['final_attempt_elapsed_s']:.6f}"
            out["rate_limit_response_elapsed_s"] = f"{row['rate_limit_response_elapsed_s']:.6f}"
            out["rate_limit_sleep_s"] = f"{row['rate_limit_sleep_s']:.6f}"
            out["rate_limit_waste_s"] = f"{row['rate_limit_waste_s']:.6f}"
            out["transient_response_elapsed_s"] = f"{row['transient_response_elapsed_s']:.6f}"
            out["transient_sleep_s"] = f"{row['transient_sleep_s']:.6f}"
            out["transient_waste_s"] = f"{row['transient_waste_s']:.6f}"
            for key in PROFILE_FIELDS:
                if isinstance(out.get(key), float):
                    out[key] = f"{out[key]:.3f}"
            writer.writerow(out)


def write_summary_tsv(path, summaries):
    fields = [
        "epoch",
        "attempted",
        "ok",
        "errors",
        "bytes",
        "wire_bytes",
        "attempts",
        "request_elapsed_sum_s",
        "rate_limited",
        "rate_limit_events",
        "rate_limit_retries",
        "rate_limit_response_elapsed_s",
        "rate_limit_sleep_s",
        "rate_limit_waste_s",
        "transient_retried",
        "transient_events",
        "transient_retries",
        "transient_response_elapsed_s",
        "transient_sleep_s",
        "transient_waste_s",
        "min_s",
        "p50_s",
        "p90_s",
        "p95_s",
        "p99_s",
        "max_s",
        "avg_s",
        "http",
        "rpc",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=fields)
        writer.writeheader()
        for epoch, summary in summaries:
            row = {"epoch": epoch, **summary}
            for key in [
                "request_elapsed_sum_s",
                "rate_limit_response_elapsed_s",
                "rate_limit_sleep_s",
                "rate_limit_waste_s",
                "transient_response_elapsed_s",
                "transient_sleep_s",
                "transient_waste_s",
                "min_s",
                "p50_s",
                "p90_s",
                "p95_s",
                "p99_s",
                "max_s",
                "avg_s",
            ]:
                row[key] = f"{row[key]:.6f}"
            writer.writerow(row)


def write_global_json(path, summary, args, plan_count, epochs, wall_s):
    body = {
        "endpoint": args.endpoint,
        "epochs": epochs,
        "epoch_count": len(epochs),
        "planned_requests": plan_count,
        "transactionDetails": args.transaction_details,
        "rewards": args.rewards,
        "requireV2": args.require_v2,
        "preferV2": args.prefer_v2,
        "profile": args.profile,
        "compressed": args.compressed,
        "concurrency": args.concurrency,
        "timeout": args.timeout,
        "wall_s": wall_s,
        "rateLimit": {
            "retries": args.rate_limit_retries,
            "sleep_s": args.rate_limit_sleep,
            "backoff": args.rate_limit_backoff,
            "max_sleep_s": args.rate_limit_max_sleep,
        },
        "transientRetry": {
            "retries": args.transient_retries,
            "sleep_s": args.transient_sleep,
            "backoff": args.transient_backoff,
            "max_sleep_s": args.transient_max_sleep,
        },
        "summary": summary,
    }
    path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")


def main():
    args = parse_args()
    if args.samples_per_epoch < 0:
        raise SystemExit("--samples-per-epoch must be >= 0")
    if args.concurrency < 1:
        raise SystemExit("--concurrency must be >= 1")
    if args.rate_limit_retries < 0:
        raise SystemExit("--rate-limit-retries must be >= 0")
    if args.rate_limit_sleep < 0:
        raise SystemExit("--rate-limit-sleep must be >= 0")
    if args.rate_limit_backoff < 0:
        raise SystemExit("--rate-limit-backoff must be >= 0")
    if args.rate_limit_max_sleep < 0:
        raise SystemExit("--rate-limit-max-sleep must be >= 0")
    if args.transient_retries < 0:
        raise SystemExit("--transient-retries must be >= 0")
    if args.transient_sleep < 0:
        raise SystemExit("--transient-sleep must be >= 0")
    if args.transient_backoff < 0:
        raise SystemExit("--transient-backoff must be >= 0")
    if args.transient_max_sleep < 0:
        raise SystemExit("--transient-max-sleep must be >= 0")
    if args.compressed and args.transport != "curl":
        raise SystemExit("--compressed requires --transport curl")

    if args.output_dir is None:
        stamp = time.strftime("%Y%m%dT%H%M%S")
        args.output_dir = f"/tmp/bench-rpc-epoch-report-{stamp}"
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = args.prefix or "worker-getblock"

    plan_rows = (
        read_plan(args.plan_file, args.require_v2) if args.plan_file else build_plan(args)
    )
    plan_path = output_dir / f"{prefix}-plan.tsv"
    write_plan(plan_path, plan_rows)
    epochs = sorted({row["epoch"] for row in plan_rows})
    print(
        f"plan={plan_path} epochs={len(epochs)} requests={len(plan_rows)}",
        file=sys.stderr,
    )

    if args.dry_run:
        return 0

    request_path = output_dir / f"{prefix}-requests.tsv"
    per_epoch_path = output_dir / f"{prefix}-per-epoch.tsv"
    global_path = output_dir / f"{prefix}-global.json"

    results = []
    completed = 0
    run_started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(request_slot, args, row) for row in plan_rows]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            if args.progress_every and completed % args.progress_every == 0:
                print(f"completed={completed}/{len(plan_rows)}", file=sys.stderr)
    wall_s = time.perf_counter() - run_started

    results.sort(key=lambda row: (int(row["epoch"]), int(row["slot"]), row["kind"]))
    write_request_rows(request_path, results)

    grouped = defaultdict(list)
    for row in results:
        grouped[int(row["epoch"])].append(row)

    per_epoch = [(epoch, summarize_rows(grouped[epoch])) for epoch in sorted(grouped)]
    write_summary_tsv(per_epoch_path, per_epoch)
    global_summary = summarize_rows(results)
    write_global_json(global_path, global_summary, args, len(plan_rows), epochs, wall_s)

    print(f"requests={request_path}", file=sys.stderr)
    print(f"per_epoch={per_epoch_path}", file=sys.stderr)
    print(f"global={global_path}", file=sys.stderr)
    print(
        "global "
        f"attempted={global_summary['attempted']} ok={global_summary['ok']} "
        f"errors={global_summary['errors']} avg_s={global_summary['avg_s']:.3f} "
        f"p50_s={global_summary['p50_s']:.3f} p90_s={global_summary['p90_s']:.3f} "
        f"p95_s={global_summary['p95_s']:.3f} max_s={global_summary['max_s']:.3f} "
        f"bytes={global_summary['bytes']} wall_s={wall_s:.3f} "
        f"rate_limit_events={global_summary['rate_limit_events']} "
        f"rate_limit_waste_s={global_summary['rate_limit_waste_s']:.3f} "
        f"transient_events={global_summary['transient_events']} "
        f"transient_waste_s={global_summary['transient_waste_s']:.3f}",
        file=sys.stderr,
    )
    return 0 if global_summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
