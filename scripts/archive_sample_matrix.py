#!/usr/bin/env python3
"""One full read per example/source/epoch. No archive hashing or cache warming."""
import argparse
import concurrent.futures
import csv
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import time
import urllib.error
import urllib.request

EPOCHS = (0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000)
FORMATS = ("compact-v2", "indexer-v3", "car")
WORKLOADS = ("slot-hours", "usdc", "pumpfun", "firewatch")
ORIGIN = "https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev"
WALLET = "5LikTUsx695BHRipWoRrn6YmTQEcPrvbR8YaHxdSRQo8"
BASE = ("archive-v2-meta.wincode", "registry.bin", "registry.mphf", "signatures.bin",
        "blockhash_registry.bin", "vote_hash_registry.bin", "poh.wincode", "shredding.wincode")
PLANES = ("blocks.index", "transaction-directory.wincode", "messages.wincode",
          "loaded-addresses.wincode", "inner-instructions.wincode", "logs.wincode",
          "token-balances.wincode", "balances.wincode", "outcomes.wincode",
          "transaction-rewards.wincode", "raw-metadata-fallbacks.wincode", "block-rewards.wincode",
          "account-postings-adaptive-v3.pages", "account-postings-adaptive-v3.control",
          "account-postings-adaptive-v3.coverage")
FILES = {"compact-v2": BASE + ("archive-v2-blocks.index", "archive-v2-blocks.zstd"),
         "indexer-v3": BASE + tuple("archive-v2-standalone-" + n for n in PLANES)}
# Count an old tail if it is present. Do not require or produce one.
OPTIONAL = ("prev_blockhash_tail.bin",)
METRICS = ("blocks", "transactions", "recorded_inner_instructions", "setup_s", "scan_s", "total_s",
           "scan_tps", "total_tps", "scan_blocks_s", "total_blocks_s", "bound_source_size_bytes",
           "stored_archive_bytes", "scan_source_bytes", "scan_source_mb_s", "setup_network_bytes",
           "scan_network_bytes", "total_network_bytes", "scan_network_mb_s", "total_network_mb_s",
           "scan_local_read_bytes", "scan_local_read_mb_s", "scan_cache_read_bytes",
           "requested_blocks", "candidate_blocks", "skipped_blocks", "decoded_blocks",
           "requested_transactions", "candidate_transactions", "skipped_transactions",
           "decoded_transactions", "decoded_scan_tps", "effective_workers", "max_active_workers",
           "output_rows", "output_bytes", "output_complete", "indeterminate_transactions", "coverage_sha256",
           "skipped_failed_transactions",
           "pipeline_read_s", "pipeline_input_wait_s", "pipeline_buffer_wait_s", "pipeline_decode_project_wall_s",
           "pipeline_worker_decode_sum_s", "pipeline_worker_projection_sum_s", "pipeline_consume_s",
           "pipeline_projection_buffer_wait_s", "pipeline_result_send_wait_s", "pipeline_signature_read_s",
           "pipeline_signature_assign_s", "pipeline_publish_s")


def save(path, data):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n")
    temporary.replace(path)


def table(path, columns, rows):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w", newline="") as output:
        writer = csv.DictWriter(output, columns, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def fields(line):
    return dict(re.findall(r"(?:^|\s)([A-Za-z0-9_]+)=([^\s]+)", line))


def binary(fmt, workload):
    return "read-car" if (fmt, workload) == ("car", "slot-hours") else "read-{}-{}".format("archive-v3" if fmt == "indexer-v3" else fmt, workload)


def plan(args):
    modes = ("local", "network") if args.mode == "both" else (args.mode,)
    return [dict(format=f, mode=m, epoch=e, workload=w, binary=binary(f, w))
            for f in getattr(args, "formats", FORMATS) for m in modes for e in getattr(args, "epochs", EPOCHS) for w in args.workloads
            if not (getattr(args, "car_count_only", False) and f == "car" and w != "slot-hours")]


def job_key(job):
    return "{format}/{mode}/epoch-{epoch}/{workload}".format(**job)


def object_names(fmt, epoch):
    if fmt == "car":
        return ("epoch-{}.car".format(epoch), "epoch-{}-slot-ranges.raw".format(epoch))
    return FILES[fmt]


def inventory(args):
    """Inspect publication object lengths only; never read archive payloads here."""
    rows = []
    if args.archive_root:
        for fmt in getattr(args, "formats", FORMATS):
            for epoch in getattr(args, "epochs", EPOCHS):
                for name in object_names(fmt, epoch) + (() if fmt == "car" else OPTIONAL):
                    path = args.archive_root / fmt / str(epoch) / name
                    if name in OPTIONAL and not path.exists():
                        continue
                    stat = path.stat()
                    if not path.is_file():
                        raise ValueError("not a file: {}".format(path))
                    rows.append(dict(format=fmt, epoch=epoch, object=name, size_bytes=stat.st_size,
                                     mtime_ns=stat.st_mtime_ns, source="local"))
    if args.mode in ("network", "both"):
        keys = [(f, e, n) for f in getattr(args, "formats", FORMATS) for e in getattr(args, "epochs", EPOCHS)
                for n in object_names(f, e) + (() if f == "car" else OPTIONAL)]

        def head(key):
            fmt, epoch, name = key
            url = "{}/{}/{}/{}".format(args.origin.rstrip("/"), fmt, epoch, name)
            try:
                request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "blockzilla-sample-preflight/1"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    return dict(format=fmt, epoch=epoch, object=name, size_bytes=int(response.headers["Content-Length"]),
                                etag=response.headers.get("ETag", ""), source="network")
            except urllib.error.HTTPError as error:
                if error.code == 404 and name in OPTIONAL:
                    return None
                raise ValueError("HEAD {} returned HTTP {}".format(url, error.code)) from error
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            rows.extend(row for row in pool.map(head, keys) if row is not None)
    return rows


def size_mismatches(rows):
    local = {(r["format"], r["epoch"], r["object"]): r["size_bytes"] for r in rows if r["source"] == "local"}
    return [dict(row, local_size_bytes=local.get((row["format"], row["epoch"], row["object"])))
            for row in rows if row["source"] == "network" and row["object"] not in OPTIONAL
            and local.get((row["format"], row["epoch"], row["object"])) != row["size_bytes"]]


def sample_process(pid):
    """Linux process I/O is physical storage I/O, NOT SDK logical or HTTP bytes."""
    row = {"time": time.time(), "load_1m": os.getloadavg()[0]}
    try:
        proc = Path("/proc") / str(pid)
        if proc.exists():
            stat = (proc / "stat").read_text().rsplit(")", 1)[1].split()
            row["cpu_seconds"] = (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK")
            row["rss_bytes"] = int(stat[21]) * os.sysconf("SC_PAGE_SIZE")
            for line in (proc / "io").read_text().splitlines():
                key, value = line.split(":")
                if key in ("read_bytes", "write_bytes", "rchar", "wchar"):
                    row["process_" + key] = int(value)
            net = Path("/proc/net/dev").read_text().splitlines()[2:]
            row["host_network_rx_bytes"] = sum(int(line.split(":")[1].split()[0])
                                               for line in net if line.split(":")[0].strip() != "lo")
        else:
            values = subprocess.check_output(["ps", "-o", "%cpu=,rss=", "-p", str(pid)], text=True).split()
            if len(values) == 2:
                row.update(cpu_percent=float(values[0]), rss_bytes=int(values[1]) * 1024)
    except (OSError, ValueError, subprocess.SubprocessError):
        pass  # Unavailable is not zero.
    return row


def parse_result(job, stdout):
    lines = stdout.read_text().splitlines()
    summaries = [fields(line) for line in lines if line.startswith("format=" + job["format"] + " ")]
    if len(summaries) != 1:
        raise ValueError("expected one reader summary")
    raw = summaries[0]
    if raw.get("epoch") != str(job["epoch"]):
        raise ValueError("reader returned a different epoch")
    result = dict(raw)
    for line in lines:
        details = fields(line)
        if "skipped_failed_transactions" in details:
            result["skipped_failed_transactions"] = details["skipped_failed_transactions"]
    result["blocks"] = raw.get("blocks", raw.get("requested_blocks"))
    result["transactions"] = raw.get("transactions", raw.get("requested_transactions"))
    result["scan_source_bytes"] = raw.get("scan_logical_read_bytes", raw.get("source_read_bytes"))
    result["scan_source_mb_s"] = raw.get("scan_logical_read_mb_s", raw.get("scan_source_mb_s"))
    result["indeterminate_transactions"] = raw.get("indeterminate_transactions", raw.get("coverage_indeterminate_transactions"))
    for key in ("blocks", "transactions", "setup_s", "scan_s", "total_s", "scan_tps", "total_tps",
                "bound_source_size_bytes", "scan_source_bytes", "scan_source_mb_s", "setup_network_bytes",
                "scan_network_bytes", "total_network_bytes"):
        value = float(result[key])
        if not math.isfinite(value) or value < 0:
            raise ValueError("invalid metric: " + key)
    for phase in ("scan", "total"):
        result[phase + "_blocks_s"] = int(result["blocks"]) / max(float(raw[phase + "_s"]), 0.000001)
    if job["format"] == "car":
        result["scan_local_read_bytes"] = result["scan_source_bytes"] if job["mode"] == "local" else 0
        result["scan_local_read_mb_s"] = result["scan_source_mb_s"] if job["mode"] == "local" else 0
        result["scan_cache_read_bytes"] = raw.get("scan_cache_bytes")
    if job["workload"] == "slot-hours":
        result["buckets"] = [fields(line) for line in lines if line.startswith("approximate_hour=")]
        if not result["buckets"] or not raw.get("recorded_inner_instructions"):
            raise ValueError("missing hour buckets or instruction count")
        for key in ("blocks", "transactions", "recorded_inner_instructions"):
            if sum(int(bucket[key]) for bucket in result["buckets"]) != int(result[key]):
                raise ValueError("bucket sum mismatch: " + key)
    else:
        for key in ("output_schema", "output_rows", "output_bytes", "output_complete", "indeterminate_transactions", "coverage_sha256"):
            if result.get(key) is None:
                raise ValueError("missing workload metric: " + key)
    for line in lines:
        if line.startswith("pipeline="):
            for key, value in fields(line).items():
                if key in METRICS:
                    result[key] = value
    result["raw_metrics"] = raw
    return result


def same_bytes(left, right):
    # Compare application outputs only, outside reader timings. No epoch hashes.
    if left.stat().st_size != right.stat().st_size:
        return False
    with left.open("rb") as a, right.open("rb") as b:
        while True:
            chunk = a.read(4 * 1024 * 1024)
            if chunk != b.read(len(chunk)):
                return False
            if not chunk:
                return True


def check_parity(job, result, results):
    peers = [r for r in results if r["epoch"] == job["epoch"] and r["workload"] == job["workload"] and r["status"] == "PASS"]
    if not peers:
        return "PENDING"
    baseline = peers[0]
    keys = ("buckets", "blocks", "transactions", "recorded_inner_instructions") if job["workload"] == "slot-hours" else (
        "output_schema", "output_rows", "output_bytes", "output_complete", "indeterminate_transactions", "coverage_sha256")
    if any(result.get(key) != baseline.get(key) for key in keys):
        return "MISMATCH"
    if job["workload"] != "slot-hours" and not same_bytes(Path(result["output_path"]), Path(baseline["output_path"])):
        return "MISMATCH"
    return "MATCH"


def run_one(args, job, sizes, results):
    root = args.results_root / "jobs" / job_key(job)
    root.mkdir(parents=True, exist_ok=True)
    result_file = root / "result.json"
    if result_file.exists():
        previous = json.loads(result_file.read_text())
        if previous["status"] == "PASS":
            if job["workload"] != "slot-hours":
                path = Path(previous["output_path"])
                if not path.is_file() or path.stat().st_size != int(previous["output_bytes"]):
                    raise ValueError("completed output is missing or changed: " + str(path))
            previous["parity"] = check_parity(job, previous, results)
            if previous["parity"] == "MISMATCH":
                raise ValueError("completed output no longer matches: " + job_key(job))
            print("skip " + job_key(job), flush=True)
            return previous
    attempt = root / ("attempt-{:03}".format(len(list(root.glob("attempt-*"))) + 1))
    attempt.mkdir()  # Preserve failed outputs. Retries get a fresh network cache.
    command = [str(args.bin_dir / job["binary"]), "--epoch", str(job["epoch"])]
    command += ["--archive-root", str(args.archive_root)] if job["mode"] == "local" else ["--origin", args.origin]
    if job["format"] != "car":
        command += ["--threads", str(args.threads)]
        if job["mode"] == "network":
            command += ["--cache-root", str(attempt / "cache")]
    if job["workload"] != "slot-hours":
        command += ["--output", str(attempt / "output.bin")]
    if job["workload"] == "firewatch":
        command += ["--wallet", args.wallet]
    save(attempt / "command.json", command)
    print("start " + job_key(job), flush=True)
    started = time.monotonic()
    result = dict(job, status="FAIL", attempt=str(attempt), started_at=time.time())
    process = None
    try:
        with (attempt / "stdout.log").open("w") as out, (attempt / "stderr.log").open("w") as err, \
                (attempt / "resources.jsonl").open("w") as resources:
            process = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            log_offset = 0
            def follow():
                nonlocal log_offset
                with (attempt / "stderr.log").open() as log:
                    log.seek(log_offset)
                    for line in log:
                        print("[{}] {}".format(job_key(job), line.rstrip()), flush=True)
                    log_offset = log.tell()
            prior = None
            while True:
                follow()
                sample = sample_process(process.pid)
                if prior:
                    dt = max(sample["time"] - prior["time"], 0.001)
                    for counter, rate in (("process_read_bytes", "physical_disk_mb_s"), ("host_network_rx_bytes", "host_rx_mb_s"), ("cpu_seconds", "cpu_percent")):
                        if counter in sample and counter in prior:
                            sample[rate] = (sample[counter] - prior[counter]) / dt * (100 if rate == "cpu_percent" else 1e-6)
                    print("resource {} elapsed_s={:.0f} {}".format(job_key(job), time.monotonic() - started,
                          " ".join("{}={:.1f}".format(k, sample[k]) for k in ("cpu_percent", "rss_bytes", "physical_disk_mb_s", "host_rx_mb_s") if k in sample)), flush=True)
                resources.write(json.dumps(sample) + "\n")
                resources.flush()
                prior = sample
                try:
                    exit_code = process.wait(timeout=args.interval)
                    break
                except subprocess.TimeoutExpired:
                    pass
            follow()
        result["wall_s"] = time.monotonic() - started
        result["exit_code"] = exit_code
        if exit_code:
            raise ValueError("reader exit {}".format(exit_code))
        result.update(parse_result(job, attempt / "stdout.log"))
        result.update(job)
        result["stored_archive_bytes"] = sizes[(job["mode"], job["format"], job["epoch"])]
        if job["workload"] != "slot-hours":
            result["output_path"] = str(attempt / "output.bin")
            if Path(result["output_path"]).stat().st_size != int(result["output_bytes"]):
                raise ValueError("output length differs from reader report")
        result["parity"] = check_parity(job, result, results)
        result["status"] = "FAIL" if result["parity"] == "MISMATCH" else "PASS"
    except BaseException as error:
        if process is not None and process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        result["error"] = str(error)
        save(result_file, result)
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
    save(result_file, result)
    print("{} {} total_s={} tps={} read_mb_s={} network_mb_s={} parity={} error={}".format(
        result["status"], job_key(job), result.get("total_s", "?"), result.get("total_tps", "?"),
        result.get("scan_source_mb_s", "?"), result.get("total_network_mb_s", "?"), result.get("parity", "?"), result.get("error", "none")), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("local", "network", "both"), default="both")
    parser.add_argument("--formats", default=",".join(FORMATS), help="comma-separated archive formats; default: all three")
    parser.add_argument("--archive-root", type=Path)
    parser.add_argument("--origin", default=ORIGIN)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=12)
    parser.add_argument("--wallet", default=WALLET)
    parser.add_argument("--workloads", default=",".join(WORKLOADS), help="comma-separated; default: all four")
    parser.add_argument("--epochs", default=",".join(map(str, EPOCHS)), help="comma-separated sample epochs; each is read in full")
    parser.add_argument("--car-count-only", action="store_true", help="run all selected V2/V3 examples but only count for CAR")
    parser.add_argument("--interval", type=float, default=10, help="resource log interval in seconds")
    parser.add_argument("--check-only", action="store_true", help="check files, binaries and HTTP HEADs; do not start readers")
    parser.add_argument("--stop-on-error", action="store_true", help="stop after a reader error; retain all results")
    args = parser.parse_args()
    args.formats = args.formats.split(",")
    if len(set(args.formats)) != len(args.formats) or not set(args.formats) <= set(FORMATS):
        parser.error("invalid format selection")
    args.formats = [fmt for fmt in FORMATS if fmt in args.formats]
    args.workloads = args.workloads.split(",")
    try:
        args.epochs = [int(epoch) for epoch in args.epochs.split(",")]
    except ValueError:
        parser.error("epochs must be comma-separated integers")
    if len(set(args.epochs)) != len(args.epochs) or not set(args.epochs) <= set(EPOCHS):
        parser.error("epochs must be distinct members of the sample set")
    if len(set(args.workloads)) != len(args.workloads) or not set(args.workloads) <= set(WORKLOADS):
        parser.error("invalid workload selection")
    if args.threads < 1 or not math.isfinite(args.interval) or args.interval < 1:
        parser.error("threads and interval must be positive")
    if args.mode in ("local", "both") and not args.archive_root:
        parser.error("local reads need --archive-root")
    args.bin_dir, args.results_root = args.bin_dir.resolve(), args.results_root.resolve()
    if args.archive_root:
        args.archive_root = args.archive_root.resolve()
    args.results_root.mkdir(parents=True, exist_ok=True)
    lock = (args.results_root / ".runner-lock").open("w")
    with lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return execute(args, parser)


def execute(args, parser):
    def stop(*_):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, stop)
    jobs = plan(args)
    if not jobs:
        parser.error("format and workload filters select no jobs")
    builds = {}
    for name in sorted({j["binary"] for j in jobs}):
        path = args.bin_dir / name
        if not os.access(path, os.X_OK):
            parser.error("missing executable: " + str(path))
        builds[name] = hashlib.sha256(path.read_bytes()).hexdigest()  # Executables only.
    config = {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items() if k != "check_only"}
    config.update(binary_sha256=builds, runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    config_path = args.results_root / "run.json"
    if config_path.exists() and json.loads(config_path.read_text()) != config:
        parser.error("run configuration changed; use a new results directory")
    save(config_path, config)
    save(args.results_root / "plan.json", jobs)
    save(args.results_root / "host.json", dict(host=platform.node(), platform=platform.platform(), logical_cpus=os.cpu_count(),
         python=platform.python_version(), cache_policy="fresh per attempt; OS and CDN caches uncontrolled",
         free_result_bytes=os.statvfs(args.results_root).f_bavail * os.statvfs(args.results_root).f_frsize))
    print("preflight: {} jobs; file lengths and HTTP HEADs only".format(len(jobs)), flush=True)
    objects = inventory(args)
    old_inventory = args.results_root / "inventory.json"
    if old_inventory.exists() and json.loads(old_inventory.read_text()) != objects:
        parser.error("source inventory changed; use a new results directory")
    save(old_inventory, objects)
    mismatches = size_mismatches(objects) if args.mode == "both" else []
    save(args.results_root / "size-mismatches.json", mismatches)
    if mismatches:
        save(args.results_root / "status.json", dict(state="PREFLIGHT_FAILED", size_mismatches=len(mismatches)))
        for row in mismatches:
            print("SIZE_MISMATCH {format}/{epoch}/{object} local={local_size_bytes} public={size_bytes}".format(**row), flush=True)
        return 1
    sizes = {}
    for row in objects:
        key = (row["source"], row["format"], row["epoch"])
        sizes[key] = sizes.get(key, 0) + row["size_bytes"]
    table(args.results_root / "archive-sizes.tsv", ("mode", "format", "epoch", "stored_archive_bytes"),
          [dict(mode=m, format=f, epoch=e, stored_archive_bytes=n) for (m, f, e), n in sizes.items()])
    if args.check_only:
        save(args.results_root / "status.json", dict(state="READY", completed=0, total=len(jobs)))
        print("READY: {} jobs; no reader was started".format(len(jobs)), flush=True)
        return 0
    results = []
    try:
        for job in jobs:
            save(args.results_root / "status.json", dict(state="RUNNING", current=job_key(job), completed=len(results), total=len(jobs)))
            results.append(run_one(args, job, sizes, results))
            table(args.results_root / "summary.tsv", ("format", "mode", "epoch", "workload", "status", "parity", "error", "wall_s") + METRICS, results)
            if args.stop_on_error and results[-1]["status"] != "PASS":
                save(args.results_root / "status.json", dict(state="FAIL", completed=len(results), total=len(jobs), current=job_key(job)))
                return 1
    except KeyboardInterrupt:
        save(args.results_root / "status.json", dict(state="INTERRUPTED", completed=len(results), total=len(jobs)))
        raise
    except Exception:
        save(args.results_root / "status.json", dict(state="FAIL", completed=len(results), total=len(jobs)))
        raise
    parity = []
    for epoch, workload in dict.fromkeys((j["epoch"], j["workload"]) for j in jobs):
        group = [r for r in results if r["epoch"] == epoch and r["workload"] == workload]
        expected = sum(j["epoch"] == epoch and j["workload"] == workload for j in jobs)
        passed = len(group) == expected and all(r["status"] == "PASS" for r in group)
        parity.append(dict(epoch=epoch, workload=workload, status="MATCH" if passed else "FAIL_OR_INCOMPLETE"))
    table(args.results_root / "parity.tsv", ("epoch", "workload", "status"), parity)
    passed = all(row["status"] == "MATCH" for row in parity)
    save(args.results_root / "status.json", dict(state="PASS" if passed else "FAIL", completed=len(results), total=len(jobs)))
    return 0 if passed else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Stopped. Partial output and logs are kept.", flush=True)
        raise SystemExit(130)
