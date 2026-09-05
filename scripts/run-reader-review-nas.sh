#!/usr/bin/env bash
# Epoch-300 count comparison, followed by the full local example matrix.
set -euo pipefail
package=${1:?usage: run-reader-review-nas.sh PACKAGE ARCHIVE_ROOT RESULTS_ROOT}
archive=${2:?archive root is required}
results=${3:?new results root is required}
mkdir -p "$results"
exec 9>"$results/sequence.lock"
flock -n 9 || { echo 'This sequence is already running.' >&2; exit 1; }
common=(--mode local --threads 12 --archive-root "$archive" --bin-dir "$package/bin")
runner=(python3 -u "$package/archive_sample_matrix.py")
echo 'PREFLIGHT full matrix: local file sizes only'
"${runner[@]}" "${common[@]}" --results-root "$results/full" --check-only
echo 'PHASE epoch-300: count/slot-hours, V2 then V3 then CAR'
"${runner[@]}" "${common[@]}" --epochs 300 --workloads slot-hours --results-root "$results/epoch-300"
echo 'PHASE full: four examples, all 11 epochs, V2 then V3 then CAR'
"${runner[@]}" "${common[@]}" --results-root "$results/full"
echo 'COMPLETE'
