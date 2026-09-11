#!/usr/bin/env python3
"""Build the final V2/V3 reader report from a normalized result artifact."""

import hashlib
import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/blockzilla-full-reader-report-matplotlib")
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter


ROOT = Path(__file__).resolve().parents[1]
BENCHMARKS = ROOT / "docs" / "benchmarks"
ARTIFACTS = BENCHMARKS / "artifacts"
SOURCE = ARTIFACTS / "full-v2-v3-reader-20260911" / "results.json"
OUTPUT = SOURCE.parent
REPORT = BENCHMARKS / "full-v2-v3-reader-20260911.md"

FORMATS = ["compact-v2", "indexer-v3"]
MODES = ["local", "network"]
WORKLOADS = ["slot-hours", "usdc", "pumpfun", "user-program-index"]
NAMES = {"compact-v2": "V2", "indexer-v3": "V3"}
MODE_NAMES = {"local": "Disk", "network": "Network"}
WORKLOAD_NAMES = {
    "slot-hours": "Count / CPI",
    "usdc": "USDC",
    "pumpfun": "Pump.fun",
    "user-program-index": "User program index",
}
COLORS = {"compact-v2": "#0072B2", "indexer-v3": "#D55E00"}


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


def rate(value):
    value = float(value)
    if value >= 1e9:
        return f"{value / 1e9:.2f}B"
    if value >= 1e6:
        return f"{value / 1e6:.2f}M"
    if value >= 1e3:
        return f"{value / 1e3:.1f}k"
    return f"{value:.0f}"


raw = SOURCE.read_bytes()
data = json.loads(raw)
rows = data["cases"]
assert data["schema"] == "blockzilla-full-v2-v3-reader-report-v1"
assert len(rows) == 176
assert all(row["status"] == "PASS" for row in rows)
keys = [(row["format"], row["mode"], int(row["epoch"]), row["workload"]) for row in rows]
assert len(set(keys)) == len(keys)
assert set(key[0] for key in keys) == set(FORMATS)
assert set(key[1] for key in keys) == set(MODES)
assert set(key[2] for key in keys) == set(range(0, 1001, 100))
assert set(key[3] for key in keys) == set(WORKLOADS)

lookup = {(row["format"], row["mode"], int(row["epoch"]), row["workload"]): row for row in rows}
epochs = list(range(0, 1001, 100))

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "text.color": "#1c2735",
        "svg.fonttype": "none",
    }
)


def line_chart(metric, stem, title, subtitle, ylabel, log=False):
    fig, axes = plt.subplots(2, 2, figsize=(14, 9.5))
    fig.subplots_adjust(left=.085, right=.98, top=.84, bottom=.12, hspace=.42, wspace=.24)
    fig.text(.04, .96, title, fontsize=22, fontweight="bold")
    fig.text(.04, .915, subtitle, fontsize=11, color="#526170")
    for ax, workload in zip(axes.flat, WORKLOADS):
        for fmt in FORMATS:
            for mode in MODES:
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
        ax.set_title(WORKLOAD_NAMES[workload], loc="left", fontweight="bold")
        ax.set_xticks(epochs[::2])
        ax.set_xlabel("Epoch")
        ax.set_ylabel(ylabel)
        if log:
            ax.set_yscale("log")
        ax.grid(color="#e5e9ee", linewidth=.7)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_visible(False)
        ax.tick_params(length=0)
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, ncol=4, frameon=False, loc="upper left", bbox_to_anchor=(.035, .89))
    save_chart(fig, stem)


line_chart(
    "total_s",
    "completion-time",
    "Reader completion time",
    "All sample epochs · lower is better · total time includes setup, scan, and output",
    "Seconds · log scale",
    log=True,
)
line_chart(
    "total_tps",
    "covered-tps",
    "Reader covered throughput",
    "All sample epochs · higher is better · index skips count as covered transactions",
    "Covered transactions/s · log scale",
    log=True,
)
line_chart(
    "scan_source_mb_s",
    "logical-read-speed",
    "Logical source read speed",
    "Higher means more logical bytes per scan second; it does not always mean a faster query",
    "Logical MB/s · log scale",
    log=True,
)

fig, ax = plt.subplots(figsize=(12, 5.4))
fig.subplots_adjust(left=.085, right=.98, top=.78, bottom=.18)
fig.text(.04, .94, "Stored archive size", fontsize=22, fontweight="bold")
fig.text(.04, .885, "Complete format size for each sample epoch", fontsize=11, color="#526170")
width = 34
for index, fmt in enumerate(FORMATS):
    values = [data["stored_sizes"][fmt][str(epoch)] / 1e9 for epoch in epochs]
    positions = [epoch + (index - .5) * width for epoch in epochs]
    ax.bar(positions, values, width=width, color=COLORS[fmt], label=NAMES[fmt])
ax.set_xticks(epochs)
ax.set_xlabel("Epoch")
ax.set_ylabel("Stored GB")
ax.legend(frameon=False, ncol=2, loc="upper left")
ax.grid(axis="y", color="#e5e9ee", linewidth=.7)
ax.set_axisbelow(True)
for spine in ax.spines.values():
    spine.set_visible(False)
ax.tick_params(length=0)
save_chart(fig, "stored-size")

aggregate = []
for workload in WORKLOADS:
    for fmt in FORMATS:
        for mode in MODES:
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
for fmt in FORMATS:
    for mode in MODES:
        selected = [row for row in rows if row["format"] == fmt and row["mode"] == mode]
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
                "stored_gb": sum(data["stored_sizes"][fmt].values()) / 1e9,
            }
        )

equal = data["car_jetstreamer_equal_output"]
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
    "# V2 and V3 reader performance",
    "",
    "Updated **11 September 2026**. The main test covers all 11 sample epochs, four examples, disk and network input, and both V2 and V3. All 176 cases passed after the stale public epoch 300 index was replaced.",
    "",
    "## Whole test at a glance",
    "",
    "The time column is the sum for all four examples over all 11 epochs. TPS is total covered transactions divided by that time. Stored size is the sum of the 11 complete archives and repeats for disk and network.",
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
    "![Completion time](artifacts/full-v2-v3-reader-20260911/completion-time.png)",
    "",
    "![Covered TPS](artifacts/full-v2-v3-reader-20260911/covered-tps.png)",
    "",
    "![Logical read speed](artifacts/full-v2-v3-reader-20260911/logical-read-speed.png)",
    "",
    "![Stored size](artifacts/full-v2-v3-reader-20260911/stored-size.png)",
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
    "## CAR reader and Jetstreamer",
    "",
    f"This separate epoch 900 network reference gives both readers the same {equal['blocks']:,} blocks. They produce the same {equal['output_bytes'] / 1e9:.3f} GB output file, with the same SHA-256 hash.",
    "",
    "| Reader | Mean time | TPS | Peak memory |",
    "|---|---:|---:|---:|",
]
for row in equal["readers"]:
    lines.append(
        f"| {row['name']} | {row['mean_total_seconds']:.1f} s | {rate(row['total_tps'])} | {row['peak_rss_mib_min']:.0f}–{row['peak_rss_mib_max']:.0f} MiB |"
    )
lines += [
    "",
    "This CAR comparison is a full decode adapter test. The V2/V3 examples have different output work and can use indexes, so their TPS values are not directly equal to this CAR test.",
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
    "The final data set contains only passing case records. V2 disk and network results come from two completed groups. V3 disk and 40 network results come from the full V3 batch. Its final batch check detected the epoch 300 repair because the repair occurred while the last epoch 1000 case ran. A comparison of all 526 before and after inventory entries found one change: the ETag of the epoch 300 block index. The size and all other entries stayed equal. The four original epoch 300 failures were removed, and a clean four-case run against the repaired inventory passed. Output hashes and counters match across V2, V3, disk, and network for every epoch and example.",
    "",
    f"[Source data](artifacts/full-v2-v3-reader-20260911/results.json) · SHA-256 `{hashlib.sha256(raw).hexdigest()}`",
    "",
]
REPORT.write_text("\n".join(lines))
print(f"Wrote {REPORT} and charts in {OUTPUT}")
