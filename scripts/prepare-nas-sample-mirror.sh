#!/usr/bin/env bash
# Create path aliases only. Do not copy, hash, repair, or replace archive files.
set -euo pipefail
if [[ $# != 1 ]]; then
  printf 'usage: %s NEW_ARCHIVE_ROOT\n' "$0" >&2
  exit 2
fi
root=$1
script_dir=$(cd "$(dirname "$0")" && pwd)
mkdir "$root"
mkdir "$root/compact-v2" "$root/indexer-v3" "$root/car"
link_set() {
  python3 - "$script_dir" "$1" "$2" "$3" "$4" <<'PY'
import os
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
from archive_sample_matrix import object_names, OPTIONAL
source, destination, fmt, epoch = Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], int(sys.argv[5])
destination.mkdir()
for name in object_names(fmt, epoch) + (() if fmt == 'car' else OPTIONAL):
    path = source / name
    if name in OPTIONAL and not path.exists():
        continue
    if path.is_symlink() or not path.is_file():
        raise ValueError('required regular source file missing: ' + str(path))
    os.link(path, destination / name)
PY
}
for epoch in 0 100 200 300 400 500 600 700 800 900 1000; do
  case "$epoch" in
    0|1000) v2="/volume1/blockzilla/archive/epoch-$epoch" ;;
    900) v2=/volume1/blockzilla/archive-metadata-normalization/staging/epoch-900-current-typed-errors-v1-20260828T124710CEST ;;
    *) v2="/volume1/blockzilla/archive-metadata-normalization/staging/sample-epochs-100-800-current-typed-errors-v1-20260831/epoch-$epoch" ;;
  esac
  v3="/volume1/blockzilla/scheduler-state/archive-samples-v1-upload-20260831/all-epoch-v3-current-reader-r1/archive/indexer-v3/$epoch"
  case "$epoch" in
    200|300|400|500|600|700|800)
      v3="/volume1/blockzilla/scheduler-state/archive-samples-v1-v3-current-rebuild-20260901/epoch-$epoch/current-reader-gate/archive/indexer-v3/$epoch"
      ;;
  esac
  [[ -d "$v2" && -d "$v3" ]] || { printf 'missing source for epoch %s\n' "$epoch" >&2; exit 1; }
  link_set "$v2" "$root/compact-v2/$epoch" compact-v2 "$epoch"
  link_set "$v3" "$root/indexer-v3/$epoch" indexer-v3 "$epoch"
  if [[ "$epoch" == 0 ]]; then
    mkdir "$root/car/0"
    ln /volume1/blockzilla/old-faithful/epoch-0.car "$root/car/0/epoch-0.car"
    ln /volume1/blockzilla/old-faithful/slot-index/epoch-0-slot-ranges.raw "$root/car/0/epoch-0-slot-ranges.raw"
  else
    link_set "/volume1/blockzilla/old-faithful/benchmark-retained/epoch-$epoch" "$root/car/$epoch" car "$epoch"
  fi
done
printf 'Mirror paths prepared: %s\nRun the matrix with --check-only before the full run.\n' "$root"
