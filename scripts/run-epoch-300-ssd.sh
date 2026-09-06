#!/usr/bin/env bash
# Copy the full epoch first. Then run the same nine examples as the HDD comparison.
set -euo pipefail
package=${1:?usage: run-epoch-300-ssd.sh PACKAGE}
source=/volume1/blockzilla/benchmark-results/sample-reader-package-20260904-final/archive
share=/volume2/blockzilla-bench
archive=$share/archive
results=$share/results/epoch-300-20260905
[[ $(findmnt -n -o TARGET -T "$share") == /volume2 ]] || { echo 'SSD volume is not mounted'; exit 1; }
[[ $(findmnt -n -o FSTYPE -T "$share") == btrfs ]] || { echo 'Unexpected SSD filesystem'; exit 1; }
[[ -w "$share" ]] || { echo 'SSD shared folder is not writable'; exit 1; }
mkdir -p "$archive" "$results"
exec 9>"$results/copy-and-run.lock"
flock -n 9 || { echo 'This SSD sequence is already running'; exit 1; }
date -Iseconds
echo 'COPY epoch 300: V2, V3, CAR; preserve shared hard links; no archive hash pass'
rsync -rtHL --relative --info=progress2 --no-inc-recursive \
  "$source/./compact-v2/300/" "$source/./indexer-v3/300/" "$source/./car/300/" "$archive/"
echo 'COPY COMPLETE: flush SSD writes before starting readers'
sync -f "$archive"
date -Iseconds
echo 'BENCHMARK: four V2 examples, four V3 examples, CAR count only; local disk; same reader binaries'
python3 -u "$package/archive_sample_matrix.py" --mode local --threads 12 --epochs 300 --car-count-only \
  --archive-root "$archive" --bin-dir /volume1/blockzilla/benchmark-results/blockzilla-reader-review-20260905-final/bin \
  --results-root "$results"
date -Iseconds
echo 'COMPLETE: nine tests; results and resource logs are kept on the SSD'
