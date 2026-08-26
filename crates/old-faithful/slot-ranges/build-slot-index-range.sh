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
  BLOCKHASH_DIR     Root with epoch-N/blockhash_registry.bin. Ordered slots
                    come only from the Old Faithful slot-to-CID index.
                    For START_EPOCH > 0, it must also contain epoch-(N-1).
  ARCHIVE_V2_DIR    Optional legacy/audit mode. Do not set it with
                    BLOCKHASH_DIR.
  SEED_PREVIOUS_BLOCKHASH
                    Optional base58 seed for epoch 0 when its registry is not
                    prefixed with the mainnet genesis hash.
  OVERWRITE_V2=1    Rebuild epoch-*-slot-ranges-v2.raw while reusing existing
                    epoch-*-slot-ranges.raw when present.
  OVERWRITE=1       Rebuild epoch-*-slot-ranges.raw too, useful when compact
                    index slot order is needed for v2 blockhash alignment.
  SYNC_R2_AFTER=1   Upload v2 to r2:blockzilla/slot-index-v2 after strict
                    validation when a blockhash source is set.

This downloads missing Old Faithful compact index files, builds slot-range
offset files, and can optionally upload the result to the Cloudflare R2 mirror.
When every raw range file already exists, it downloads only slot-to-CID indexes
unless DOWNLOAD_CID_INDEX=1 is set.
EOF
  exit 0
fi

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"

START_EPOCH="$1"
END_EPOCH="${2:-$START_EPOCH}"
INDEXES_DIR="${INDEXES_DIR:-indexes}"
SLOT_INDEX_DIR="${SLOT_INDEX_DIR:-out}"

if [[ -n "${BLOCKHASH_DIR:-}" && -n "${ARCHIVE_V2_DIR:-}" ]]; then
  echo "BLOCKHASH_DIR and ARCHIVE_V2_DIR are mutually exclusive" >&2
  exit 2
fi

download_cid_default=0
if [[ "${OVERWRITE:-0}" == "1" ]]; then
  download_cid_default=1
else
  for ((epoch=START_EPOCH; epoch<=END_EPOCH; epoch++)); do
    if [[ ! -s "$SLOT_INDEX_DIR/epoch-$epoch-slot-ranges.raw" ]]; then
      download_cid_default=1
      break
    fi
  done
fi
DOWNLOAD_CID_INDEX="${DOWNLOAD_CID_INDEX:-$download_cid_default}" \
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

if [[ -n "${BLOCKHASH_DIR:-}" ]]; then
  args+=("--blockhash-dir" "$BLOCKHASH_DIR")
elif [[ -n "${ARCHIVE_V2_DIR:-}" ]]; then
  args+=("--archive-v2-dir" "$ARCHIVE_V2_DIR")
fi

if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
  args+=("--seed-previous-blockhash" "$SEED_PREVIOUS_BLOCKHASH")
fi

if [[ "${OVERWRITE_V2:-0}" == "1" ]]; then
  args+=("--overwrite-v2")
fi

if [[ "${OVERWRITE:-0}" == "1" ]]; then
  args+=("--overwrite")
fi

cd "$REPO_ROOT"
"${CARGO_BIN:-cargo}" run --release -p of-slot-ranges --bin of-slot-ranges -- "${args[@]}"

if [[ -n "${BLOCKHASH_DIR:-}" ]]; then
  validator_seed_args=()
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    validator_seed_args+=("--seed-previous-blockhash" "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "${CARGO_BIN:-cargo}" run --release -p of-slot-ranges \
    --bin of-validate-slot-index-v2 -- \
    "$SLOT_INDEX_DIR" "$BLOCKHASH_DIR" \
    --indexes-dir "$INDEXES_DIR" \
    --start-epoch "$START_EPOCH" \
    --end-epoch "$END_EPOCH" \
    "${validator_seed_args[@]}"
elif [[ -n "${ARCHIVE_V2_DIR:-}" ]]; then
  validator_seed_args=()
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    validator_seed_args+=("--seed-previous-blockhash" "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "${CARGO_BIN:-cargo}" run --release -p of-slot-ranges \
    --bin of-validate-slot-index-v2 -- \
    "$SLOT_INDEX_DIR" "$ARCHIVE_V2_DIR" \
    --archive-v2 \
    --start-epoch "$START_EPOCH" \
    --end-epoch "$END_EPOCH" \
    "${validator_seed_args[@]}"
fi

if [[ "${SYNC_R2_AFTER:-0}" == "1" ]]; then
  if [[ -n "${BLOCKHASH_DIR:-}" ]]; then
    SLOT_INDEX_V2_VALIDATE_BIN="${SLOT_INDEX_V2_VALIDATE_BIN:-$REPO_ROOT/target/release/of-validate-slot-index-v2}" \
      "$SCRIPT_DIR/sync-slot-index-r2.sh" push-v2 \
      "$SLOT_INDEX_DIR" "$INDEXES_DIR" "$BLOCKHASH_DIR"
  elif [[ -n "${ARCHIVE_V2_DIR:-}" ]]; then
    SLOT_INDEX_V2_VALIDATE_BIN="${SLOT_INDEX_V2_VALIDATE_BIN:-$REPO_ROOT/target/release/of-validate-slot-index-v2}" \
      "$SCRIPT_DIR/sync-slot-index-r2.sh" push-v2-archive \
      "$SLOT_INDEX_DIR" "$ARCHIVE_V2_DIR"
  else
    "$SCRIPT_DIR/sync-slot-index-r2.sh" push "$SLOT_INDEX_DIR"
  fi
fi
