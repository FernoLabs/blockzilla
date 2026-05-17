#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 ]]; then
  cat <<'EOF'
Usage:
  build-car-slot-indexes.sh <start-epoch> [end-epoch]

Environment:
  CARS_DIR          Directory containing epoch-N.car or epoch-N.car.zst.
                    Default: ./epochs
  SLOT_INDEX_DIR    Output directory for epoch-*-slot-ranges*.raw.
                    Default: ./out
  BLOCKHASH_DIR     Root for epoch-N/blockhash_registry.bin.
                    Default: $SLOT_INDEX_DIR/blockhash-registry
  JOBS              Number of CAR epochs to scan concurrently. Default: 4
  PREFER_ZST=1      Prefer epoch-N.car.zst when both raw and zstd exist.
  OVERWRITE=1       Replace existing raw/registry/v2 files.
  SCAN_ONLY=1       Only build raw ranges + blockhash registries; skip v2 stitch.

The CAR scan is parallel per epoch. It writes raw slot ranges plus
blockhash_registry.bin. Unless SCAN_ONLY=1, a second fast pass stitches
epoch-N-slot-ranges-v2.raw from those registries so each epoch gets the
previous epoch's last blockhash.
EOF
  exit 0
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

START_EPOCH="$1"
END_EPOCH="${2:-$START_EPOCH}"
CARS_DIR="${CARS_DIR:-epochs}"
SLOT_INDEX_DIR="${SLOT_INDEX_DIR:-out}"
BLOCKHASH_DIR="${BLOCKHASH_DIR:-$SLOT_INDEX_DIR/blockhash-registry}"
JOBS="${JOBS:-4}"

scan_args=(
  "--start-epoch" "$START_EPOCH"
  "--end-epoch" "$END_EPOCH"
  "--output-dir" "$SLOT_INDEX_DIR"
  "--blockhash-dir" "$BLOCKHASH_DIR"
  "--seed-blockhash-dir" "$BLOCKHASH_DIR"
  "--jobs" "$JOBS"
  "--no-v2"
)

if [[ "${PREFER_ZST:-0}" == "1" ]]; then
  scan_args+=("--prefer-zst")
fi

if [[ "${OVERWRITE:-0}" == "1" ]]; then
  scan_args+=("--overwrite")
fi

cd "$REPO_ROOT"
cargo run --release -p of-slot-ranges --bin of-car-slot-index -- "${scan_args[@]}" "$CARS_DIR"

if [[ "${SCAN_ONLY:-0}" != "1" ]]; then
  v2_args=(
    "--start-epoch" "$START_EPOCH"
    "--end-epoch" "$END_EPOCH"
    "--indexes-dir" "/tmp/of-slot-ranges-unused-indexes"
    "--output-dir" "$SLOT_INDEX_DIR"
    "--archive-v2-dir" "$BLOCKHASH_DIR"
    "--overwrite-v2"
  )
  cargo run --release -p of-slot-ranges --bin of-slot-ranges -- "${v2_args[@]}"
fi
