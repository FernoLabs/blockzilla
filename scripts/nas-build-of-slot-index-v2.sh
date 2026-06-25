#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 ]]; then
  cat <<'EOF'
Usage:
  nas-build-of-slot-index-v2.sh <start-epoch> [end-epoch]

Environment:
  INDEXES_DIR       Old Faithful compact-index cache.
                    Default: /volume1/blockzilla-of-indexes
  SLOT_INDEX_DIR    Output directory for v1/v2 slot indexes.
                    Default: /volume1/blockzilla-slot-index
  ARCHIVE_V2_DIR    Root with epoch-N/blockhash_registry.bin.
                    Default: /home/ach/dev/blockzilla-v2
  CARS_DIR          Optional local plain CAR dir for header reads.
  OVERWRITE_V2=1    Rebuild existing epoch-N-slot-ranges-v2.raw.
  PULL_V1_RAW=1     Pull existing epoch-N-slot-ranges.raw from R2 first.
  PUSH_V2_R2=1      Push v2 outputs after build.
  SLOT_INDEX_REMOTE R2 remote, default r2:blockzilla/slot-index
EOF
  exit 0
fi

START_EPOCH="$1"
END_EPOCH="${2:-$START_EPOCH}"
INDEXES_DIR="${INDEXES_DIR:-/volume1/blockzilla-of-indexes}"
SLOT_INDEX_DIR="${SLOT_INDEX_DIR:-/volume1/blockzilla-slot-index}"
ARCHIVE_V2_DIR="${ARCHIVE_V2_DIR:-/home/ach/dev/blockzilla-v2}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SLOT_RANGE_DIR="$SCRIPT_DIR/../crates/old-faithful/slot-ranges"

mkdir -p "$INDEXES_DIR" "$SLOT_INDEX_DIR"

if [[ "${PULL_V1_RAW:-0}" == "1" ]]; then
  SLOT_INDEX_INCLUDE='epoch-*-slot-ranges.raw' \
    "$SLOT_RANGE_DIR/sync-slot-index-r2.sh" pull "$SLOT_INDEX_DIR" "${SLOT_INDEX_REMOTE:-r2:blockzilla/slot-index}"
fi

env_args=(
  INDEXES_DIR="$INDEXES_DIR"
  SLOT_INDEX_DIR="$SLOT_INDEX_DIR"
  ARCHIVE_V2_DIR="$ARCHIVE_V2_DIR"
  OVERWRITE_V2="${OVERWRITE_V2:-0}"
)

if [[ -n "${CARS_DIR:-}" ]]; then
  env_args+=(CARS_DIR="$CARS_DIR")
fi

env "${env_args[@]}" "$SLOT_RANGE_DIR/build-slot-index-range.sh" "$START_EPOCH" "$END_EPOCH"

if [[ "${PUSH_V2_R2:-0}" == "1" ]]; then
  SLOT_INDEX_INCLUDE='epoch-*-slot-ranges-v2.raw' \
    "$SLOT_RANGE_DIR/sync-slot-index-r2.sh" push "$SLOT_INDEX_DIR" "${SLOT_INDEX_REMOTE:-r2:blockzilla/slot-index-v2}"
fi
