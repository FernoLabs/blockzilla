#!/usr/bin/env bash
# Network trial first; copy the sample set; then read all V2, all V3, CAR count.
set -euo pipefail
control=/volume2/blockzilla-bench/control/all-samples-20260905
source=/volume1/blockzilla/benchmark-results/sample-reader-package-20260904-final/archive
archive=/volume2/blockzilla-bench/archive
bins=/volume1/blockzilla/benchmark-results/blockzilla-reader-review-20260905-final/bin
network_results=/volume2/blockzilla-bench/results/epoch-300-network-20260905
local_results=/volume1/blockzilla/benchmark-results/all-samples-ssd-input-20260905
phase=initialization
trap 'echo "FAILED phase=$phase line=$LINENO time=$(date -Iseconds)" >&2' ERR
[[ $(findmnt -n -o TARGET -T "$control") == /volume2 ]]
[[ $(findmnt -n -o FSTYPE -T "$control") == btrfs ]]
[[ $(cat /sys/block/md2/md/level) == raid0 ]]
mkdir -p "$network_results" "$local_results"
exec 8>"$control/sequence.lock"
flock -n 8 || { echo 'This sequence is already running'; exit 1; }

phase=network_epoch_300
echo "PHASE $phase $(date -Iseconds)"
# Fresh per-attempt HTTP caches and dump outputs use SSD for this WAN trial.
# The full local run below keeps outputs on HDD, as in the local baseline.
python3 -u "$control/archive_sample_matrix.py" --mode network --epochs 300 --car-count-only \
  --threads 12 --bin-dir "$bins" --results-root "$network_results"

phase=copy_all_samples
echo "PHASE $phase $(date -Iseconds)"
python3 - "$source" "$archive" <<'PY'
import os, sys
from pathlib import Path
source, target = map(Path, sys.argv[1:])
groups = {}
for epoch in range(0, 1001, 100):
    for fmt in ('compact-v2', 'indexer-v3', 'car'):
        for path in (source / fmt / str(epoch)).iterdir():
            assert path.is_file(), f'Unexpected input: {path}'
            st = path.stat()
            key = st.st_dev, st.st_ino
            entry = groups.setdefault(key, {'bytes': st.st_size, 'present': False})
            dest = target / path.relative_to(source)
            if dest.is_file():
                ds = dest.stat()
                entry['present'] |= ds.st_size == st.st_size and int(ds.st_mtime) == int(st.st_mtime)
required = sum(g['bytes'] for g in groups.values() if not g['present'])
fs = os.statvfs(target)
free = fs.f_bavail * fs.f_frsize
reserve = 128 * 1024**3
print(f'COPY_SPACE required_bytes={required} free_bytes={free} reserve_bytes={reserve}', flush=True)
assert free >= required + reserve, 'Not enough SSD space; copy and full run were not started'
PY
copy_sources=()
for format in compact-v2 indexer-v3 car; do
  for epoch in 0 100 200 300 400 500 600 700 800 900 1000; do
    copy_sources+=("$source/./$format/$epoch/")
  done
done
# One invocation preserves hard links shared between formats and reuses epoch 300.
# No archive checksum pass, no deletions, and no reader runs during this copy.
rsync -rtHL --relative --info=progress2 --stats --no-inc-recursive "${copy_sources[@]}" "$archive/"
sync -f "$archive"
echo "COPY_ALL_COMPLETE $(date -Iseconds)"

phase=local_all_samples
echo "PHASE $phase $(date -Iseconds)"
python3 -u "$control/archive_sample_matrix.py" --mode local --car-count-only --threads 12 \
  --archive-root "$archive" --bin-dir "$bins" --results-root "$local_results"
echo "FULL_RUN_COMPLETE $(date -Iseconds)"
