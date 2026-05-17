#!/usr/bin/env python3
import argparse
import csv
import json
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Compare two bench_rpc_epoch_report.py outputs.")
    parser.add_argument("--left-label", required=True)
    parser.add_argument("--left-dir", required=True)
    parser.add_argument("--left-prefix", required=True)
    parser.add_argument("--right-label", required=True)
    parser.add_argument("--right-dir", required=True)
    parser.add_argument("--right-prefix", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--prefix", default="rpc-compare")
    return parser.parse_args()


def read_per_epoch(path):
    out = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            epoch = int(row["epoch"])
            out[epoch] = row
    return out


def read_global(path):
    return json.loads(path.read_text())


def as_float(row, key):
    return float(row[key])


def ratio(left, right, key):
    right_value = as_float(right, key)
    if right_value == 0:
        return 0.0
    return as_float(left, key) / right_value


def fmt(value):
    return f"{value:.6f}"


def summary_float(summary, key):
    return float(summary.get(key, 0.0))


def summary_int(summary, key):
    return int(summary.get(key, 0))


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    left_dir = Path(args.left_dir)
    right_dir = Path(args.right_dir)
    left_epoch = read_per_epoch(left_dir / f"{args.left_prefix}-per-epoch.tsv")
    right_epoch = read_per_epoch(right_dir / f"{args.right_prefix}-per-epoch.tsv")
    left_global = read_global(left_dir / f"{args.left_prefix}-global.json")
    right_global = read_global(right_dir / f"{args.right_prefix}-global.json")

    epochs = sorted(set(left_epoch) & set(right_epoch))
    compare_tsv = output_dir / f"{args.prefix}-per-epoch.tsv"
    fields = [
        "epoch",
        f"{args.left_label}_ok",
        f"{args.right_label}_ok",
        f"{args.left_label}_avg_s",
        f"{args.right_label}_avg_s",
        "avg_ratio_left_over_right",
        f"{args.left_label}_p95_s",
        f"{args.right_label}_p95_s",
        "p95_ratio_left_over_right",
        f"{args.left_label}_max_s",
        f"{args.right_label}_max_s",
        "max_ratio_left_over_right",
        f"{args.left_label}_bytes",
        f"{args.right_label}_bytes",
    ]
    with open(compare_tsv, "w", newline="") as f:
        writer = csv.DictWriter(f, delimiter="\t", fieldnames=fields)
        writer.writeheader()
        for epoch in epochs:
            left = left_epoch[epoch]
            right = right_epoch[epoch]
            writer.writerow(
                {
                    "epoch": epoch,
                    f"{args.left_label}_ok": left["ok"],
                    f"{args.right_label}_ok": right["ok"],
                    f"{args.left_label}_avg_s": left["avg_s"],
                    f"{args.right_label}_avg_s": right["avg_s"],
                    "avg_ratio_left_over_right": fmt(ratio(left, right, "avg_s")),
                    f"{args.left_label}_p95_s": left["p95_s"],
                    f"{args.right_label}_p95_s": right["p95_s"],
                    "p95_ratio_left_over_right": fmt(ratio(left, right, "p95_s")),
                    f"{args.left_label}_max_s": left["max_s"],
                    f"{args.right_label}_max_s": right["max_s"],
                    "max_ratio_left_over_right": fmt(ratio(left, right, "max_s")),
                    f"{args.left_label}_bytes": left["bytes"],
                    f"{args.right_label}_bytes": right["bytes"],
                }
            )

    left_summary = left_global["summary"]
    right_summary = right_global["summary"]
    compare_json = output_dir / f"{args.prefix}-global.json"
    body = {
        "left_label": args.left_label,
        "right_label": args.right_label,
        "left": left_global,
        "right": right_global,
        "ratios_left_over_right": {
            "avg_s": left_summary["avg_s"] / right_summary["avg_s"]
            if right_summary["avg_s"]
            else 0,
            "p50_s": left_summary["p50_s"] / right_summary["p50_s"]
            if right_summary["p50_s"]
            else 0,
            "p95_s": left_summary["p95_s"] / right_summary["p95_s"]
            if right_summary["p95_s"]
            else 0,
            "max_s": left_summary["max_s"] / right_summary["max_s"]
            if right_summary["max_s"]
            else 0,
        },
    }
    compare_json.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")

    markdown = output_dir / f"{args.prefix}.md"
    if (
        left_summary["ok"] == left_summary["attempted"]
        and right_summary["ok"] == right_summary["attempted"]
    ):
        verdict = (
            f"Fastest average latency: "
            f"`{args.left_label if left_summary['avg_s'] < right_summary['avg_s'] else args.right_label}`."
        )
    else:
        verdict = (
            "No latency winner: at least one endpoint returned errors. "
            "Latency rows include failed responses."
        )
    markdown.write_text(
        "\n".join(
            [
                "# RPC Epoch Benchmark Comparison",
                "",
                f"Compared `{args.left_label}` against `{args.right_label}` using the same slot plan.",
                "",
                "| metric | "
                + args.left_label
                + " | "
                + args.right_label
                + " | left/right |",
                "| --- | ---: | ---: | ---: |",
                f"| attempted | {left_summary['attempted']} | {right_summary['attempted']} | |",
                f"| ok | {left_summary['ok']} | {right_summary['ok']} | |",
                f"| errors | {left_summary['errors']} | {right_summary['errors']} | |",
                f"| avg_s | {left_summary['avg_s']:.3f} | {right_summary['avg_s']:.3f} | {body['ratios_left_over_right']['avg_s']:.3f} |",
                f"| p50_s | {left_summary['p50_s']:.3f} | {right_summary['p50_s']:.3f} | {body['ratios_left_over_right']['p50_s']:.3f} |",
                f"| p95_s | {left_summary['p95_s']:.3f} | {right_summary['p95_s']:.3f} | {body['ratios_left_over_right']['p95_s']:.3f} |",
                f"| max_s | {left_summary['max_s']:.3f} | {right_summary['max_s']:.3f} | {body['ratios_left_over_right']['max_s']:.3f} |",
                f"| bytes | {left_summary['bytes']} | {right_summary['bytes']} | |",
                f"| wall_s | {left_global.get('wall_s', 0.0):.3f} | {right_global.get('wall_s', 0.0):.3f} | |",
                f"| request_elapsed_sum_s | {summary_float(left_summary, 'request_elapsed_sum_s'):.3f} | {summary_float(right_summary, 'request_elapsed_sum_s'):.3f} | |",
                f"| attempts | {summary_int(left_summary, 'attempts')} | {summary_int(right_summary, 'attempts')} | |",
                f"| rate_limit_events | {summary_int(left_summary, 'rate_limit_events')} | {summary_int(right_summary, 'rate_limit_events')} | |",
                f"| rate_limit_retries | {summary_int(left_summary, 'rate_limit_retries')} | {summary_int(right_summary, 'rate_limit_retries')} | |",
                f"| rate_limit_response_elapsed_s | {summary_float(left_summary, 'rate_limit_response_elapsed_s'):.3f} | {summary_float(right_summary, 'rate_limit_response_elapsed_s'):.3f} | |",
                f"| rate_limit_sleep_s | {summary_float(left_summary, 'rate_limit_sleep_s'):.3f} | {summary_float(right_summary, 'rate_limit_sleep_s'):.3f} | |",
                f"| rate_limit_waste_s | {summary_float(left_summary, 'rate_limit_waste_s'):.3f} | {summary_float(right_summary, 'rate_limit_waste_s'):.3f} | |",
                "",
                verdict,
                "",
                f"Per-epoch TSV: `{compare_tsv}`",
                f"Global JSON: `{compare_json}`",
                "",
            ]
        )
    )

    print(f"per_epoch={compare_tsv}")
    print(f"global={compare_json}")
    print(f"markdown={markdown}")


if __name__ == "__main__":
    raise SystemExit(main())
