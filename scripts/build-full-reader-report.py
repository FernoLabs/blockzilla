#!/usr/bin/env python3
"""Build the final reader report with accepted V2, V3, and CAR results."""

import hashlib
import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/blockzilla-full-reader-report-matplotlib")
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, FuncFormatter


ROOT = Path(__file__).resolve().parents[1]
BENCHMARKS = ROOT / "docs" / "benchmarks"
ARTIFACTS = BENCHMARKS / "artifacts"
SOURCE = ARTIFACTS / "full-v2-v3-reader-20260911" / "results.json"
OUTPUT = SOURCE.parent
REPORT = BENCHMARKS / "full-v2-v3-reader-20260911.md"

FORMATS = ["compact-v2", "indexer-v3"]
MODES = ["local", "network"]
SERIES = [
    ("compact-v2", "local"),
    ("compact-v2", "network"),
    ("indexer-v3", "local"),
    ("indexer-v3", "network"),
    ("car", "local"),
]
STORED_FORMATS = ["compact-v2", "indexer-v3", "car"]
WORKLOADS = ["slot-hours", "usdc", "pumpfun", "user-program-index"]
NAMES = {"compact-v2": "V2", "indexer-v3": "V3", "car": "CAR"}
MODE_NAMES = {"local": "Disk", "network": "Network"}
WORKLOAD_NAMES = {
    "slot-hours": "Count / CPI",
    "usdc": "USDC",
    "pumpfun": "Pump.fun",
    "user-program-index": "User program index",
}
COLORS = {"compact-v2": "#0072B2", "indexer-v3": "#D55E00", "car": "#4D4D4D"}
ELAPSED_TICKS = (0.1, 1, 10, 60, 600, 3_600, 21_600)


def save_chart(fig, stem):
    metadata = {"Creator": "Blockzilla reader benchmark", "Date": None}
    fig.savefig(OUTPUT / f"{stem}.png", dpi=180, facecolor="white", metadata=metadata)
    fig.savefig(OUTPUT / f"{stem}.svg", facecolor="white", metadata=metadata)
    svg = OUTPUT / f"{stem}.svg"
    svg.write_text("\n".join(line.rstrip() for line in svg.read_text().splitlines()) + "\n")
    plt.close(fig)


def duration(seconds):
    seconds = float(seconds)
    if seconds >= 3600:
        return f"{seconds / 3600:.2f} h"
    if seconds >= 60:
        return f"{seconds / 60:.1f} min"
    return f"{seconds:.1f} s"


def precise_duration(seconds):
    seconds = float(seconds)
    if seconds >= 3_600:
        hours = int(seconds // 3_600)
        minutes = int((seconds % 3_600) // 60)
        return f"{hours}h {minutes}m"
    if seconds >= 60:
        minutes = int(seconds // 60)
        return f"{minutes}m {seconds % 60:.1f}s"
    return f"{seconds:.1f}s"


def rate(value):
    value = float(value)
    if value >= 1e9:
        return f"{value / 1e9:.2f}B"
    if value >= 1e6:
        return f"{value / 1e6:.2f}M"
    if value >= 1e3:
        return f"{value / 1e3:.1f}k"
    return f"{value:.0f}"


def elapsed_tick(value, _position):
    """Format a seconds value as a compact, readable duration."""
    if value < 1:
        return f"{value * 1_000:g} ms"
    if value < 60:
        return f"{value:g} s"
    if value < 3_600:
        minutes, seconds = divmod(round(value), 60)
        return f"{minutes}m {seconds}s" if seconds else f"{minutes} min"
    hours, remainder = divmod(round(value), 3_600)
    minutes = remainder // 60
    return f"{hours}h {minutes}m" if minutes else f"{hours} h"


raw = SOURCE.read_bytes()
data = json.loads(raw)
rows = data["cases"]
car_rows = data["car_file_cases"]
car_network_rows = data["car_network_cases"]
stored_sizes = data["comparable_stored_sizes"]
assert data["schema"] == "blockzilla-full-v2-v3-reader-report-v1"
assert len(rows) == 176
assert all(row["status"] == "PASS" for row in rows)
keys = [(row["format"], row["mode"], int(row["epoch"]), row["workload"]) for row in rows]
assert len(set(keys)) == len(keys)
assert set(key[0] for key in keys) == set(FORMATS)
assert set(key[1] for key in keys) == set(MODES)
assert set(key[2] for key in keys) == set(range(0, 1001, 100))
assert set(key[3] for key in keys) == set(WORKLOADS)
assert len(car_rows) == 44
assert all(row["format"] == "car" and row["mode"] == "local" for row in car_rows)
assert all(row["status"] == "PASS" and row["parity"] == "MATCH" for row in car_rows)
assert len(car_network_rows) == 4
assert all(
    row["format"] == "car"
    and row["mode"] == "network"
    and int(row["epoch"]) == 900
    and row["status"] == "PASS"
    for row in car_network_rows
)
assert {row["workload"] for row in car_network_rows} == set(WORKLOADS)
assert data["stored_sizes"]["car"]["300"] == 508_343_057_180
assert stored_sizes["car"]["300"] == 206_326_307_867

lookup = {
    (row["format"], row["mode"], int(row["epoch"]), row["workload"]): row
    for row in rows + car_rows + car_network_rows
}
epochs = list(range(0, 1001, 100))

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "text.color": "#1c2735",
        "svg.hashsalt": "blockzilla-reader-report",
        "svg.fonttype": "none",
    }
)


def line_chart(metric, stem, title, subtitle, ylabel, log=False, tick_formatter=None):
    fig, axes = plt.subplots(2, 2, figsize=(14, 9.5))
    fig.subplots_adjust(left=.085, right=.98, top=.79, bottom=.10, hspace=.48, wspace=.24)
    fig.text(.04, .96, title, fontsize=22, fontweight="bold")
    fig.text(.04, .915, subtitle, fontsize=11, color="#526170")
    for ax, workload in zip(axes.flat, WORKLOADS):
        for fmt, mode in SERIES:
            values = [float(lookup[(fmt, mode, epoch, workload)][metric]) for epoch in epochs]
            ax.plot(
                epochs,
                values,
                color=COLORS[fmt],
                linestyle="-" if mode == "local" else "--",
                marker="o",
                markersize=3,
                linewidth=1.8,
                label=f"{NAMES[fmt]} {MODE_NAMES[mode].lower()}",
            )
        network_car = lookup[("car", "network", 900, workload)]
        ax.plot(
            [900],
            [float(network_car[metric])],
            color=COLORS["car"],
            linestyle="none",
            marker="D",
            markerfacecolor="white",
            markeredgewidth=1.8,
            markersize=6,
            label="CAR network · epoch 900",
        )
        ax.set_title(WORKLOAD_NAMES[workload], loc="left", fontweight="bold")
        ax.set_xticks(epochs[::2])
        ax.set_xlabel("Epoch")
        ax.set_ylabel(ylabel)
        if log:
            ax.set_yscale("log")
        if tick_formatter:
            lower, upper = ax.get_ylim()
            ax.yaxis.set_major_locator(
                FixedLocator([tick for tick in ELAPSED_TICKS if lower <= tick <= upper])
            )
            ax.yaxis.set_major_formatter(FuncFormatter(tick_formatter))
        ax.grid(color="#e5e9ee", linewidth=.7)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.tick_params(length=0)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, ncol=3, frameon=False, loc="upper left", bbox_to_anchor=(.035, .89))
    save_chart(fig, stem)


line_chart(
    "total_s",
    "completion-time",
    "Reader completion time",
    "All sample epochs · lower is better · CAR network is available for epoch 900",
    "Elapsed time · log scale",
    log=True,
    tick_formatter=elapsed_tick,
)
line_chart(
    "total_tps",
    "covered-tps",
    "Reader covered throughput",
    "All sample epochs · higher is better · CAR network is available for epoch 900",
    "Covered transactions/s · log scale",
    log=True,
)
line_chart(
    "scan_source_mb_s",
    "logical-read-speed",
    "Logical source read speed",
    "Higher means more logical bytes per scan second · CAR network is available for epoch 900",
    "Logical MB/s · log scale",
    log=True,
)

fig, ax = plt.subplots(figsize=(12, 5.4))
fig.subplots_adjust(left=.085, right=.98, top=.78, bottom=.18)
fig.text(.04, .94, "Stored archive size", fontsize=22, fontweight="bold")
fig.text(
    .04,
    .885,
    "Compressed archive size · CAR epoch 300 uses its measured zstd level-3 size",
    fontsize=11,
    color="#526170",
)
width = 24
for index, fmt in enumerate(STORED_FORMATS):
    values = [stored_sizes[fmt][str(epoch)] / 1e9 for epoch in epochs]
    positions = [epoch + (index - 1) * width for epoch in epochs]
    ax.bar(positions, values, width=width, color=COLORS[fmt], label=NAMES[fmt])
ax.set_xticks(epochs)
ax.set_xlabel("Epoch")
ax.set_ylabel("Stored GB")
ax.legend(frameon=False, ncol=3, loc="upper left")
ax.grid(axis="y", color="#e5e9ee", linewidth=.7)
ax.set_axisbelow(True)
for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(length=0)
save_chart(fig, "stored-size")

aggregate = []
for workload in WORKLOADS:
    for fmt, mode in SERIES:
        selected = [lookup[(fmt, mode, epoch, workload)] for epoch in epochs]
        total_s = sum(float(row["total_s"]) for row in selected)
        transactions = sum(int(row["transactions"]) for row in selected)
        scan_s = sum(float(row["scan_s"]) for row in selected)
        source_bytes = sum(int(row["scan_source_bytes"]) for row in selected)
        aggregate.append(
            {
                "workload": workload,
                "format": fmt,
                "mode": mode,
                "total_s": total_s,
                "total_tps": transactions / total_s,
                "scan_source_mb_s": source_bytes / 1e6 / scan_s,
            }
        )

totals = []
for fmt, mode in SERIES:
    selected = [
        row for row in rows + car_rows if row["format"] == fmt and row["mode"] == mode
    ]
    total_s = sum(float(row["total_s"]) for row in selected)
    total_tx = sum(int(row["transactions"]) for row in selected)
    total_scan_s = sum(float(row["scan_s"]) for row in selected)
    total_source = sum(int(row["scan_source_bytes"]) for row in selected)
    totals.append(
        {
            "format": fmt,
            "mode": mode,
            "total_s": total_s,
            "total_tps": total_tx / total_s,
            "scan_source_mb_s": total_source / 1e6 / total_scan_s,
            "stored_gb": sum(stored_sizes[fmt].values()) / 1e9,
        }
    )

equal = data["car_jetstreamer_equal_output"]
equal_parameters = equal["parameters"]
equal_reader_lookup = {row["reader"]: row for row in equal["readers"]}
assert equal_parameters["requested_workers"] == 12
assert equal_parameters["timed_runs_per_reader"] == 2
assert equal_parameters["concurrent_readers"] == 1
total_lookup = {(row["format"], row["mode"]): row for row in totals}
disk_speedup = total_lookup[("compact-v2", "local")]["total_s"] / total_lookup[("indexer-v3", "local")]["total_s"]
network_speedup = total_lookup[("compact-v2", "network")]["total_s"] / total_lookup[("indexer-v3", "network")]["total_s"]
disk_reduction = (1 - 1 / disk_speedup) * 100
network_reduction = (1 - 1 / network_speedup) * 100
v3_wins = {}
for mode in MODES:
    v3_wins[mode] = sum(
        float(lookup[("indexer-v3", mode, epoch, workload)]["total_s"])
        < float(lookup[("compact-v2", mode, epoch, workload)]["total_s"])
        for epoch in epochs
        for workload in WORKLOADS
    )
lines = [
    "# Archive reader performance",
    "",
    "Updated **11 September 2026**. The main test covers all 11 sample epochs, four examples, disk and network input, and both V2 and V3. All 176 cases passed after the stale public epoch 300 index was replaced. The graphs also include 44 accepted CAR disk cases and four accepted CAR network cases for epoch 900.",
    "",
    "## Whole test at a glance",
    "",
    "The time column is the sum for all four examples over all 11 epochs. TPS is total covered transactions divided by that time. Stored size is the sum of the 11 compressed archives. CAR network has only epoch 900 data, so its results are in a separate table.",
    "",
    "| Reader | Input | Total time | Covered TPS | Logical MB/s | Stored size |",
    "|---|---|---:|---:|---:|---:|",
]
for row in totals:
    lines.append(
        f"| {NAMES[row['format']]} | {MODE_NAMES[row['mode']]} | {duration(row['total_s'])} | {rate(row['total_tps'])} | {row['scan_source_mb_s']:,.1f} | {row['stored_gb']:,.1f} GB |"
    )
lines += [
    "",
    f"Across the complete matrix, V3 used {disk_reduction:.1f}% less time than V2 from disk and {network_reduction:.1f}% less time over the network. V3 finished first in {v3_wins['local']} of 44 disk cases and {v3_wins['network']} of 44 network cases. The gain depends strongly on the example because V3 can skip most data for some indexed queries.",
    "",
    "The CAR disk line uses outer zstd for ten epochs. Epoch 300 uses raw CAR. These CAR measurements are two to three days older than the V2/V3 measurements, so small differences can include host and cache variation.",
    "",
    "![Completion time](artifacts/full-v2-v3-reader-20260911/completion-time.png)",
    "",
    "![Covered TPS](artifacts/full-v2-v3-reader-20260911/covered-tps.png)",
    "",
    "![Logical read speed](artifacts/full-v2-v3-reader-20260911/logical-read-speed.png)",
    "",
    "![Stored size](artifacts/full-v2-v3-reader-20260911/stored-size.png)",
    "",
    "The storage graph uses zstd CAR for all epochs. For epoch 300, it uses the measured 206.3 GB zstd level-3 CAR plus its 5.2 MB slot index. The timed CAR disk test used the 508.3 GB raw input, so this adjustment changes only the storage comparison.",
    "",
    "## Workload totals",
    "",
    "Each row combines the 11 epochs. Completion time is the clearest speed comparison. Logical MB/s shows how much source data the reader moves during the scan.",
    "",
    "| Example | Reader | Input | Time | Covered TPS | Logical MB/s |",
    "|---|---|---|---:|---:|---:|",
]
for workload in WORKLOADS:
    for row in [item for item in aggregate if item["workload"] == workload]:
        lines.append(
            f"| {WORKLOAD_NAMES[workload]} | {NAMES[row['format']]} | {MODE_NAMES[row['mode']]} | {duration(row['total_s'])} | {rate(row['total_tps'])} | {row['scan_source_mb_s']:,.1f} |"
        )
lines += [
    "",
    "## CAR disk and network at epoch 900",
    "",
    "These four network tests read the complete raw CAR object. The disk tests read the outer-zstd CAR object. The output and counters match, but the input size is different. The result therefore includes both network transfer and source encoding effects.",
    "",
    "| Example | Disk time | Network time | Disk TPS | Network TPS | Network / disk time |",
    "|---|---:|---:|---:|---:|---:|",
]
for workload in WORKLOADS:
    disk = lookup[("car", "local", 900, workload)]
    network = lookup[("car", "network", 900, workload)]
    lines.append(
        f"| {WORKLOAD_NAMES[workload]} | {duration(disk['total_s'])} | {duration(network['total_s'])} | {rate(disk['total_tps'])} | {rate(network['total_tps'])} | {float(network['total_s']) / float(disk['total_s']):.2f}× |"
    )
lines += [
    "",
    f"The disk source is {lookup[('car', 'local', 900, 'slot-hours')]['stored_archive_bytes'] / 1e9:.1f} GB. The network source is {lookup[('car', 'network', 900, 'slot-hours')]['stored_archive_bytes'] / 1e9:.1f} GB.",
    "",
    "## CAR reader and Jetstreamer",
    "",
    f"This separate epoch 900 network reference gives both readers the same {equal['blocks']:,} blocks and {equal['transactions']:,} transactions. They read the same raw CAR object through the same gateway. They produce the same {equal['output_bytes'] / 1e9:.3f} GB output file, with the same SHA-256 hash.",
    "",
    "| Parameter | CAR reader | Jetstreamer |",
    "|---|---|---|",
    f"| Version | Blockzilla CAR reader; binary `{equal_reader_lookup['car']['binary_sha256'][:12]}` | Jetstreamer {equal_parameters['jetstreamer_version']}; binary `{equal_reader_lookup['jetstreamer']['binary_sha256'][:12]}` |",
    f"| Processing workers | {equal_parameters['car_decode_workers']} decode workers | {equal_parameters['jetstreamer_firehose_workers']} firehose workers on a {equal_parameters['jetstreamer_tokio_runtime_threads']}-thread Tokio runtime |",
    f"| HTTP input | {equal_parameters['car_http_workers']} closed-range workers; {equal_parameters['car_http_chunk_bytes'] / 2**20:.0f} MiB chunks; {equal_parameters['car_http_window_chunks']}-chunk window | Long HTTP/1.1 ranges from each worker offset to the CAR end; {equal_parameters['jetstreamer_reader_buffer_bytes'] / 2**20:.0f} MiB reader buffer |",
    f"| Allocator | {equal_parameters['allocator']} | {equal_parameters['allocator']} |",
    f"| Timed runs | {equal_parameters['timed_runs_per_reader']}, fresh process for each run | {equal_parameters['timed_runs_per_reader']}, fresh process for each run |",
    f"| Signature verification | {'Enabled' if equal_parameters['signature_verification'] else 'Disabled'} | {'Enabled' if equal_parameters['signature_verification'] else 'Disabled'} |",
    "",
    "The runner first checks 64 blocks. It then runs CAR, Jetstreamer, Jetstreamer, and CAR, with one active process at a time. Timing includes decode, canonical output ordering, file writes, and final file sync. The final byte and SHA-256 checks occur after the timer stops.",
    "",
    "| Reader | Individual times | Mean time | TPS | Mean CPU time | Mean CPU use | Peak memory |",
    "|---|---:|---:|---:|---:|---:|---:|",
]
for row in equal["readers"]:
    lines.append(
        f"| {row['name']} | {' / '.join(precise_duration(value) for value in row['run_total_seconds'])} | {precise_duration(row['mean_total_seconds'])} | {rate(row['total_tps'])} | {precise_duration(row['mean_cpu_seconds'])} | {row['mean_cpu_cores']:.2f} cores | {row['peak_rss_mib_min']:.0f}–{row['peak_rss_mib_max']:.0f} MiB |"
    )
lines += [
    "",
    f"The CAR reader is {equal['readers'][1]['mean_total_seconds'] / equal['readers'][0]['mean_total_seconds']:.2f} times faster by mean wall time. The requested worker count alone does not show actual CPU use: CAR used about {equal['readers'][0]['mean_cpu_cores']:.2f} CPU cores on average, while Jetstreamer used about {equal['readers'][1]['mean_cpu_cores']:.2f}. The two CAR times also vary more than the two Jetstreamer times, so this small sample is a reference result rather than a stable production limit.",
    "",
    "Both paths decode transactions and status metadata, classify votes and failures, compute message hashes, and decode rewards. Jetstreamer also creates native Solana metadata objects and uses asynchronous callbacks. The CAR reader retains borrowed transaction fields and protobuf status metadata. These internal costs remain in the result even though the exported bytes match exactly.",
    "",
    "This CAR comparison is a full decode adapter test. The V2/V3 examples have different output work and can use indexes, so their TPS values are not directly equal to this CAR test.",
    "",
    "[Exact CAR and Jetstreamer receipts](artifacts/car-jetstreamer-common-output-20260909.json)",
    "",
    "## Design notes",
    "",
    "CAR keeps the original data model and needs broad decoding. Outer zstd reduces storage and can be decompressed as the reader scans. V2 uses compressed block frames, compact account IDs, shared registries, and borrowed decoding. V3 separates fields into files and adds reverse indexes. It can skip unrelated blocks and avoid downloading unused data.",
    "",
    "The network reader downloads sealed signature data once, keeps large registries in a local cache, merges adjacent HTTP ranges, and processes downloads while worker threads decode earlier data. These changes reduce HTTP calls and allocation work. Disk can still win when a query reads much of an epoch because the network path has request latency and transfer limits.",
    "",
    "The NAS ran another CPU compaction job during part of this test. SSD traffic was not shared, but some CPU speed can be lower than an idle-machine result. No correction was applied.",
    "",
    "## Acceptance method",
    "",
    "The final data set contains only passing case records. V2 disk and network results come from two completed groups. V3 disk and 40 network results come from the full V3 batch. Its final batch check detected the epoch 300 repair because the repair occurred while the last epoch 1000 case ran. A comparison of all 526 before and after inventory entries found one change: the ETag of the epoch 300 block index. The size and all other entries stayed equal. The four original epoch 300 failures were removed, and a clean four-case run against the repaired inventory passed. Output hashes and counters match across V2, V3, disk, and network for every epoch and example. The separate CAR disk records have passing parity for all 44 cases. The three CAR network example receipts passed and match their disk output. The CAR count case is in the accepted network set, and its block, transaction, instruction, and CPI counters match disk.",
    "",
    f"[Source data](artifacts/full-v2-v3-reader-20260911/results.json) · SHA-256 `{hashlib.sha256(raw).hexdigest()}`",
    "",
]
REPORT.write_text("\n".join(lines))
print(f"Wrote {REPORT} and charts in {OUTPUT}")
