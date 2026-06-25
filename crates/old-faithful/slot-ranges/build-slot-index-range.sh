#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 ]]; then
  cat <<'EOF'
Usage:
  build-slot-index-range.sh <start-epoch> [end-epoch]

Environment:
  INDEXES_DIR       Compact index cache directory. Default: ./indexes
  SLOT_INDEX_DIR    Output directory for epoch-*-slot-ranges.raw. Default: ./out
  CARS_DIR          Optional local plain CAR directory for header reads.
  ARCHIVE_V2_DIR    Optional Archive V2 sidecar root with epoch-N/
                    blockhash_registry.bin. archive-v2-blocks.index is used
                    when present, otherwise non-empty raw slot ranges define
                    the blockhash order.
  OVERWRITE_V2=1    Rebuild epoch-*-slot-ranges-v2.raw while reusing existing
                    epoch-*-slot-ranges.raw when present.
  OVERWRITE=1       Rebuild epoch-*-slot-ranges.raw too, useful when compact
                    index slot order is needed for v2 blockhash alignment.
  SYNC_R2_AFTER=1   Push SLOT_INDEX_DIR to r2:blockzilla/slot-index after build.

This downloads missing Old Faithful compact index files, builds slot-range
offset files, and can optionally upload the result to the Cloudflare R2 mirror.
EOF
  exit 0
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

START_EPOCH="$1"
END_EPOCH="${2:-$START_EPOCH}"
INDEXES_DIR="${INDEXES_DIR:-indexes}"
SLOT_INDEX_DIR="${SLOT_INDEX_DIR:-out}"

"$SCRIPT_DIR/dl-indexes.sh" "$START_EPOCH" "$END_EPOCH" "$INDEXES_DIR"

args=(
  "--start-epoch" "$START_EPOCH"
  "--end-epoch" "$END_EPOCH"
  "--indexes-dir" "$INDEXES_DIR"
  "--output-dir" "$SLOT_INDEX_DIR"
)

if [[ -n "${CARS_DIR:-}" ]]; then
  args+=("--cars-dir" "$CARS_DIR")
fi

if [[ -n "${ARCHIVE_V2_DIR:-}" ]]; then
  args+=("--archive-v2-dir" "$ARCHIVE_V2_DIR")
fi

if [[ "${OVERWRITE_V2:-0}" == "1" ]]; then
  args+=("--overwrite-v2")
fi

if [[ "${OVERWRITE:-0}" == "1" ]]; then
  args+=("--overwrite")
fi

cd "$REPO_ROOT"
"${CARGO_BIN:-cargo}" run --release -p of-slot-ranges --bin of-slot-ranges -- "${args[@]}"

if [[ "${SYNC_R2_AFTER:-0}" == "1" ]]; then
  "$SCRIPT_DIR/sync-slot-index-r2.sh" push "$SLOT_INDEX_DIR"
fi
