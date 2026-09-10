#!/usr/bin/env python3
"""Build the compact reader comparison from verified benchmark artifacts."""

import hashlib
import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", "/tmp/blockzilla-reader-report-matplotlib")
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / "docs" / "benchmarks" / "artifacts"
OUTPUT = ARTIFACTS / "tiny-reader-report-20260909"
OUTPUT.mkdir(exist_ok=True)
EXAMPLE_SOURCE = ARTIFACTS / "reader-speed-summary-20260909" / "data.json"
EQUAL_SOURCE = ARTIFACTS / "car-jetstreamer-common-output-20260909.json"


def load(path):
    raw = path.read_bytes()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def save_chart(fig, path):
    metadata = {"Creator": "Blockzilla reader benchmark", "Date": None}
    fig.savefig(path, dpi=180, facecolor="white", metadata=metadata)
    if path.suffix == ".svg":
        path.write_text(
            "\n".join(line.rstrip() for line in path.read_text().splitlines()) + "\n"
        )


examples, examples_sha = load(EXAMPLE_SOURCE)
equal, equal_sha = load(EQUAL_SOURCE)
assert len(examples["cases"]) == 24 and not examples["missing_cases"]
assert equal["state"] == "PASS" and equal["independently_rehashed"]

workloads = ["slot-hours", "usdc", "pumpfun", "user-program-index"]
workload_names = ["Count / CPI", "USDC", "Pump.fun", "Program index"]
formats = ["compact-v2", "indexer-v3", "car"]
format_names = {"compact-v2": "V2", "indexer-v3": "V3", "car": "CAR"}
colors = {"compact-v2": "#265eaa", "indexer-v3": "#177765", "car": "#b65312", "jetstreamer": "#7553a6"}
lookup = {
    (row["mode"], row["format"], row["workload"]): row
    for row in examples["cases"]
}
network = []
for workload in workloads:
    for fmt in formats:
        row = lookup[("network", fmt, workload)]
        network.append(
            {
                "workload": workload,
                "format": fmt,
                "total_seconds": float(row["total_s"]),
                "total_tps": float(row["total_tps"]),
            }
        )

full_equal = [row for row in equal["cases"] if not row["smoke"]]
assert len(full_equal) == 4
digests = {row["output_sha256"] for row in full_equal}
sizes = {row["output_bytes"] for row in full_equal}
assert len(digests) == len(sizes) == 1
equal_summary = []
for reader in ["car", "jetstreamer"]:
    rows = [row for row in full_equal if row["reader"] == reader]
    assert len(rows) == 2 and all(row["state"] == "PASS" for row in rows)
    seconds = sum(row["total_seconds"] for row in rows) / len(rows)
    equal_summary.append(
        {
            "reader": reader,
            "mean_total_seconds": seconds,
            "total_tps": 8_925_832 / seconds,
            "output_bytes": rows[0]["output_bytes"],
            "peak_rss_mib_min": min(row["peak_rss_mib"] for row in rows),
            "peak_rss_mib_max": max(row["peak_rss_mib"] for row in rows),
        }
    )

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "text.color": "#1c2735",
        "svg.fonttype": "none",
    }
)
fig, axes = plt.subplots(1, 2, figsize=(14, 5.8), gridspec_kw={"width_ratios": [1.75, 1]})
fig.subplots_adjust(left=.07, right=.98, top=.77, bottom=.2, wspace=.3)
fig.text(.04, .94, "Reader completion time", fontsize=22, fontweight="bold")
fig.text(.04, .885, "Shorter bars are faster · epoch 900 · network input", fontsize=12)

ax = axes[0]
width = .23
for format_index, fmt in enumerate(formats):
    values = [
        next(row["total_seconds"] for row in network if row["format"] == fmt and row["workload"] == workload) / 60
        for workload in workloads
    ]
    positions = [index + (format_index - 1) * width for index in range(len(workloads))]
    bars = ax.bar(positions, values, width, color=colors[fmt], label=format_names[fmt])
    for bar, value in zip(bars, values):
        label = f"{value:.1f}m" if value >= 1 else f"{value * 60:.0f}s"
        ax.annotate(label, (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    xytext=(0, 4), textcoords="offset points", ha="center", va="bottom", fontsize=8)
ax.set_title("Same full-epoch example output", loc="left", fontweight="bold", pad=12)
ax.set_ylabel("Total time (minutes)")
ax.set_xticks(range(len(workloads)), workload_names)
ax.set_ylim(0, max(bar.get_height() for bar in ax.patches) * 1.2)
ax.legend(frameon=False, ncol=3, loc="upper left")

ax = axes[1]
values = [row["mean_total_seconds"] for row in equal_summary]
labels = ["CAR", "Jetstreamer"]
bars = ax.bar(labels, values, width=.56, color=[colors["car"], colors["jetstreamer"]])
for bar, value in zip(bars, values):
    ax.annotate(f"{value:.1f}s", (bar.get_x() + bar.get_width() / 2, value),
                xytext=(0, 5), textcoords="offset points", ha="center", va="bottom", fontsize=10)
ax.set_title("Same 2.300 GB byte output", loc="left", fontweight="bold", pad=12)
ax.set_ylabel("Mean total time (seconds)")
ax.set_ylim(0, max(values) * 1.2)

for ax in axes:
    ax.grid(axis="y", color="#e5e9ee", linewidth=.7)
    ax.set_axisbelow(True)
    ax.tick_params(axis="both", length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)

fig.text(.04, .08, "Left: total time includes setup and example output. Program index uses each format's index when available.", fontsize=9, color="#526170")
fig.text(.04, .045, "Right: 8,192 blocks, 8,925,832 transactions, 12 workers, mimalloc; output files matched byte for byte.", fontsize=9, color="#526170")
for extension in ["png", "svg"]:
    save_chart(fig, OUTPUT / f"completion-time.{extension}")
plt.close(fig)

result = {
    "schema": "blockzilla-tiny-reader-report-v1",
    "source_sha256": {
        str(EXAMPLE_SOURCE.relative_to(ARTIFACTS)): examples_sha,
        str(EQUAL_SOURCE.relative_to(ARTIFACTS)): equal_sha,
    },
    "network_full_epoch": network,
    "equal_output": {
        "blocks": 8192,
        "transactions": 8_925_832,
        "output_sha256": next(iter(digests)),
        "output_bytes": next(iter(sizes)),
        "readers": equal_summary,
    },
}
(OUTPUT / "data.json").write_text(json.dumps(result, indent=2) + "\n")
print(f"Wrote compact chart and data to {OUTPUT}")
