#!/usr/bin/env python3
"""Compare saved local V2/V3 example runs without changing inputs or results.

Exit 0 requires a complete current run, exact output parity, comparable recorded
input/settings metadata, and no elapsed-time investigation flag. Exit 1 means
the report needs attention; exit 2 means invalid command-line arguments.
"""
import argparse
import csv
import json
import math
from pathlib import Path


FORMATS = ("compact-v2", "indexer-v3")
EPOCHS = tuple(range(0, 1001, 100))
WORKLOADS = ("slot-hours", "usdc", "pumpfun", "firewatch")
COUNTS = ("blocks", "transactions", "recorded_inner_instructions")
OUTPUT_FIELDS = ("output_schema", "output_rows", "output_bytes", "output_complete",
                 "indeterminate_transactions", "coverage_sha256")
TIMES = ("total_s", "scan_s", "wall_s")


def read_json(path):
    try:
        return json.loads(path.read_text()), None
    except (OSError, ValueError) as error:
        return None, "{}: {}".format(path, error)


def key(job):
    return (job["format"], job["mode"], int(job["epoch"]), job["workload"])


def job_path(root, identity):
    fmt, mode, epoch, workload = identity
    return root / "jobs" / fmt / mode / ("epoch-{}".format(epoch)) / workload / "result.json"


def scalar(value):
    # Runner fields are strings, while hand-saved JSON can use scalar types.
    return str(value).lower() if isinstance(value, bool) else str(value)


def finite(value, positive=False):
    number = float(value)
    if not math.isfinite(number) or number < 0 or (positive and number == 0):
        raise ValueError("expected a finite {}number".format("positive " if positive else "nonnegative "))
    return number


def nonnegative_int(value):
    if isinstance(value, bool):
        raise ValueError("boolean is not a count")
    number = int(value)
    if number < 0 or str(number) != str(value):
        raise ValueError("expected a nonnegative integer")
    return number


def resolved_output(root, result):
    path = Path(result["output_path"])
    return path if path.is_absolute() else root / path


def same_bytes(left, right):
    # Outputs only. No archive hashing, no full-output memory allocation.
    if left.stat().st_size != right.stat().st_size:
        return False
    with left.open("rb") as a, right.open("rb") as b:
        while True:
            chunk = a.read(4 * 1024 * 1024)
            if chunk != b.read(len(chunk)):
                return False
            if not chunk:
                return True


def validate_result(root, identity, result):
    errors = []
    if not isinstance(result, dict):
        return ["missing or invalid result object"]
    try:
        if key(result) != identity:
            errors.append("result identity differs from selected job")
        if result.get("status") != "PASS" or result.get("exit_code", 0) != 0:
            errors.append("reader result did not pass")
        for field in ("blocks", "transactions"):
            nonnegative_int(result[field])
        if identity[-1] == "slot-hours":
            nonnegative_int(result["recorded_inner_instructions"])
            buckets = result["buckets"]
            if not isinstance(buckets, list) or not buckets:
                raise ValueError("empty or invalid count buckets")
            for field in COUNTS:
                if sum(nonnegative_int(bucket[field]) for bucket in buckets) != nonnegative_int(result[field]):
                    errors.append("bucket sum differs from " + field)
            for bucket in buckets:
                for field in ("approximate_hour", "start_slot", "end_slot_exclusive"):
                    nonnegative_int(bucket[field])
        else:
            for field in OUTPUT_FIELDS:
                if result.get(field) is None:
                    raise ValueError("missing " + field)
            for field in ("output_rows", "output_bytes", "indeterminate_transactions"):
                nonnegative_int(result[field])
            if scalar(result["output_complete"]) not in ("true", "false"):
                raise ValueError("invalid output_complete")
            if not result["output_schema"] or not result["coverage_sha256"]:
                raise ValueError("empty output schema or coverage digest")
            path = resolved_output(root, result)
            if not path.is_file():
                errors.append("output file is unavailable: " + str(path))
            elif path.stat().st_size != int(result["output_bytes"]):
                errors.append("output length differs from saved result")
    except (KeyError, TypeError, ValueError, OSError) as error:
        errors.append("invalid saved result: " + str(error))
    return errors


def normalized_buckets(buckets):
    return [{field: scalar(value) for field, value in bucket.items()} for bucket in buckets]


def correctness(left_root, right_root, identity, left, right):
    fields = COUNTS if identity[-1] == "slot-hours" else ("blocks", "transactions") + OUTPUT_FIELDS
    differences = [field for field in fields if scalar(left[field]) != scalar(right[field])]
    if identity[-1] == "slot-hours":
        if normalized_buckets(left["buckets"]) != normalized_buckets(right["buckets"]):
            differences.append("buckets")
    elif not same_bytes(resolved_output(left_root, left), resolved_output(right_root, right)):
        differences.append("output_bytes_content")
    return differences


def metadata(root):
    values = {}
    for name in ("inventory", "run", "host", "plan", "status"):
        values[name], values[name + "_error"] = read_json(root / (name + ".json"))
    return values


def inventory_for(data, identity):
    fmt, mode, epoch, _ = identity
    if not isinstance(data, list):
        raise ValueError("missing inventory")
    selected = {}
    for row in data:
        if not isinstance(row, dict):
            raise ValueError("invalid inventory object")
        if (row.get("format"), row.get("source"), row.get("epoch")) != (fmt, mode, epoch):
            continue
        name = row["object"]
        if name in selected:
            raise ValueError("duplicate inventory object: " + name)
        # Size alone does not establish comparable source metadata.
        selected[name] = (nonnegative_int(row["size_bytes"]), nonnegative_int(row["mtime_ns"]))
    if not selected:
        raise ValueError("no selected local inventory objects")
    return selected


def comparison_context(before, current, identity):
    reasons = []
    try:
        left = inventory_for(before["inventory"], identity)
        right = inventory_for(current["inventory"], identity)
        if left != right:
            reasons.append("source object set, size, or modification time changed")
    except (KeyError, TypeError, ValueError) as error:
        reasons.append("source metadata unavailable or invalid: " + str(error))
    for section, fields in (("run", ("threads",)), ("host", ("host", "logical_cpus"))):
        for field in fields:
            left = before[section].get(field) if isinstance(before.get(section), dict) else None
            right = current[section].get(field) if isinstance(current.get(section), dict) else None
            if left is None or right is None or scalar(left) != scalar(right):
                reasons.append("{} {} changed or is missing".format(section, field))
    if identity[-1] == "firewatch":
        left = before["run"].get("wallet") if isinstance(before.get("run"), dict) else None
        right = current["run"].get("wallet") if isinstance(current.get("run"), dict) else None
        if left is None or right is None or left != right:
            reasons.append("wallet changed or is missing")
    return reasons


def current_run_errors(data, expected):
    errors = []
    try:
        jobs = data["plan"]
        if not isinstance(jobs, list):
            raise ValueError("missing plan")
        keys = [key(job) for job in jobs]
        if len(keys) != len(set(keys)) or set(keys) != set(expected):
            errors.append("current plan differs from expected scope or contains duplicate jobs")
        status = data["status"]
        if not isinstance(status, dict) or status.get("state") != "PASS":
            errors.append("current runner has not completed with PASS")
        elif status.get("completed") != len(expected) or status.get("total") != len(expected):
            errors.append("current completed/total counters differ from expected scope")
        if not expected:
            errors.append("empty expected scope")
    except (KeyError, TypeError, ValueError) as error:
        errors.append("invalid current run metadata: " + str(error))
    return errors


def sample_resources(root, result):
    """Optional sampled values; absent counters stay absent, never become zero."""
    attempt = Path(result.get("attempt", ""))
    if not str(result.get("attempt", "")):
        return {}
    if not attempt.is_absolute():
        attempt = root / attempt
    output = {}
    first, last = None, None
    try:
        with (attempt / "resources.jsonl").open() as source:
            for line in source:
                row = json.loads(line)
                if "rss_bytes" in row:
                    output["sampled_max_rss_bytes"] = max(output.get("sampled_max_rss_bytes", 0), finite(row["rss_bytes"]))
                if "time" in row:
                    if first is None:
                        first = row
                    last = row
        if first and last and finite(last["time"]) > finite(first["time"]):
            seconds = float(last["time"]) - float(first["time"])
            for counter, metric, multiplier in (("cpu_seconds", "sampled_cpu_percent", 100),
                                                 ("process_read_bytes", "sampled_storage_read_mb_s", 1e-6)):
                if counter in first and counter in last:
                    delta = finite(last[counter]) - finite(first[counter])
                    if delta >= 0:
                        output[metric] = delta / seconds * multiplier
    except (OSError, ValueError, TypeError, KeyError):
        output["sample_error"] = "resource log unavailable or invalid"
    return output


def compare_job(identity, baseline_root, current_root, before_meta, current_meta, threshold):
    fmt, mode, epoch, workload = identity
    row = dict(format=fmt, mode=mode, epoch=epoch, workload=workload,
               correctness="UNVERIFIED", comparability="INCOMPARABLE", performance="UNVERIFIED",
               issues=[], timing={}, baseline_root=str(baseline_root))
    current, current_error = read_json(job_path(current_root, identity))
    baseline, baseline_error = read_json(job_path(baseline_root, identity))
    current_errors = validate_result(current_root, identity, current)
    if current_error or current_errors:
        row["state"] = "INCOMPLETE_CURRENT"
        row["issues"] += ([current_error] if current_error else []) + current_errors
        return row
    baseline_errors = validate_result(baseline_root, identity, baseline)
    if baseline_error or baseline_errors:
        row["state"] = "MISSING_BASELINE"
        row["issues"] += ([baseline_error] if baseline_error else []) + baseline_errors
        return row
    try:
        differences = correctness(baseline_root, current_root, identity, baseline, current)
    except OSError as error:
        row["state"] = "INCOMPARABLE"
        row["issues"].append("could not compare output bytes: " + str(error))
        return row
    row["correctness"] = "MISMATCH" if differences else "MATCH"
    row["issues"] += ["different " + field for field in differences]
    reasons = comparison_context(before_meta, current_meta, identity)
    row["comparability"] = "INCOMPARABLE" if reasons else "RECORDED_METADATA_MATCH"
    row["issues"] += reasons
    flagged = []
    for metric in TIMES:
        try:
            left, right = finite(baseline[metric], True), finite(current[metric], True)
            ratio = right / left
            row["timing"][metric] = dict(baseline=left, current=right, ratio=ratio,
                                         change_percent=(ratio - 1) * 100)
            if ratio > 1 + threshold / 100:
                flagged.append(metric)
        except (KeyError, ValueError, TypeError) as error:
            row["comparability"] = "INCOMPARABLE"
            row["issues"].append("{} unavailable or invalid: {}".format(metric, error))
    for field in ("scan_source_bytes", "scan_local_read_bytes", "total_tps", "decoded_transactions",
                  "decoded_scan_tps", "effective_workers", "skipped_failed_transactions"):
        row[field] = dict(baseline=baseline.get(field), current=current.get(field))
    row["baseline_resources"] = sample_resources(baseline_root, baseline)
    row["current_resources"] = sample_resources(current_root, current)
    row["slowdown_metrics"] = flagged
    row["performance"] = "INVESTIGATE" if flagged else "WITHIN_THRESHOLD"
    if row["comparability"] == "INCOMPARABLE":
        row["performance"] = "INCOMPARABLE"
    row["state"] = ("CORRECTNESS_MISMATCH" if differences else
                    "INCOMPARABLE" if row["comparability"] == "INCOMPARABLE" else
                    "PERFORMANCE_FLAG" if flagged else "PASS")
    return row


def compare_runs(baseline_v2, baseline_v3, current, epochs=EPOCHS, workloads=WORKLOADS, threshold=10.0):
    expected = [(fmt, "local", epoch, workload) for fmt in FORMATS for epoch in epochs for workload in workloads]
    roots = {"compact-v2": baseline_v2, "indexer-v3": baseline_v3}
    before = {fmt: metadata(root) for fmt, root in roots.items()}
    current_meta = metadata(current)
    errors = current_run_errors(current_meta, expected)
    rows = [compare_job(identity, roots[identity[0]], current, before[identity[0]], current_meta, threshold)
            for identity in expected]
    counts = {}
    for row in rows:
        counts[row["state"]] = counts.get(row["state"], 0) + 1
    return dict(state="PASS" if not errors and rows and all(row["state"] == "PASS" for row in rows) else "NEEDS_ATTENTION",
                expected_jobs=len(expected), run_errors=errors, job_states=counts,
                current_root=str(current), baseline_roots={fmt: str(root) for fmt, root in roots.items()},
                elapsed_investigation_threshold_percent=threshold,
                source_identity_policy="Equal recorded object names, sizes, and modification times are supporting metadata, not content identity proof. No archive files are read.",
                measurement_limits="Single passes with uncontrolled OS cache. A timing flag needs investigation and is not statistical regression proof. Resource samples are not exact peak memory or final storage throughput.",
                rows=rows)


def write_report(output_dir, report):
    output_dir.mkdir(parents=False, exist_ok=False)
    (output_dir / "comparison.json").write_text(json.dumps(report, indent=2) + "\n")
    columns = ("format", "mode", "epoch", "workload", "state", "correctness", "comparability", "performance")
    columns += tuple(metric + suffix for metric in TIMES for suffix in ("_baseline", "_current", "_ratio", "_change_percent"))
    columns += ("issues",)
    with (output_dir / "comparison.tsv").open("w", newline="") as target:
        writer = csv.DictWriter(target, columns, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for row in report["rows"]:
            flat = dict(row, issues="; ".join(row["issues"]))
            for metric, values in row["timing"].items():
                flat.update({metric + "_" + name: value for name, value in values.items()})
            writer.writerow(flat)
    lines = ["# Archive example comparison", "", "Result: **{}**.".format(report["state"]), "",
             "Expected jobs: {}. Job states: {}.".format(report["expected_jobs"], json.dumps(report["job_states"], sort_keys=True)), "",
             "Elapsed-time investigation threshold: {}%.".format(report["elapsed_investigation_threshold_percent"]), "",
             report["source_identity_policy"], "", report["measurement_limits"], ""]
    lines += ["- " + error for error in report["run_errors"]]
    lines += ["", "See comparison.tsv for each job and comparison.json for complete evidence.", ""]
    (output_dir / "README.md").write_text("\n".join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-v2", required=True, type=Path)
    parser.add_argument("--baseline-v3", required=True, type=Path)
    parser.add_argument("--current", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path, help="new directory; existing directories are refused")
    parser.add_argument("--epochs", default=",".join(map(str, EPOCHS)))
    parser.add_argument("--workloads", default=",".join(WORKLOADS))
    parser.add_argument("--slowdown-percent", type=float, default=10.0)
    args = parser.parse_args(argv)
    try:
        epochs = tuple(int(value) for value in args.epochs.split(","))
        workloads = tuple(args.workloads.split(","))
        if not epochs or len(set(epochs)) != len(epochs) or not set(epochs) <= set(EPOCHS):
            raise ValueError("epochs must be distinct members of the sample set")
        if not workloads or len(set(workloads)) != len(workloads) or not set(workloads) <= set(WORKLOADS):
            raise ValueError("workloads must be distinct known workloads")
        finite(args.slowdown_percent)
        if args.output_dir.exists() or not args.output_dir.parent.is_dir():
            raise ValueError("output directory must be new and its parent must exist")
    except ValueError as error:
        parser.error(str(error))
    report = compare_runs(args.baseline_v2, args.baseline_v3, args.current, epochs, workloads, args.slowdown_percent)
    write_report(args.output_dir, report)
    print("{}: {} expected jobs; report {}".format(report["state"], report["expected_jobs"], args.output_dir))
    return 0 if report["state"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
