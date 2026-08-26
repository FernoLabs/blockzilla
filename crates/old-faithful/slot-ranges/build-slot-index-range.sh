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
  OLD_FAITHFUL_BASE_URL
                    Optional compact-index and plain-CAR mirror base URL.
  BLOCKHASH_DIR     Root with epoch-N/blockhash_registry.bin. Ordered slots
                    come from SLOT_LIST_DIR when set. Otherwise, they come
                    from the Old Faithful slot-to-CID index.
                    For START_EPOCH > 0, it must also contain epoch-(N-1).
  SLOT_LIST_DIR     Optional local directory with epoch-N.slots.txt or
                    N.slots.txt. Requires BLOCKHASH_DIR and existing raw range
                    files. This path does not use compact indexes, CID indexes,
                    CAR files, or Archive V2.
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

Without SLOT_LIST_DIR, this downloads missing Old Faithful compact index files,
builds slot-range offset files, and can optionally upload the result to the
Cloudflare R2 mirror. SLOT_LIST_DIR rebuilds v2 directly from existing raw
ranges, public Old Faithful slot lists, and blockhash registries.
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
if [[ -n "${SLOT_LIST_DIR:-}" && -z "${BLOCKHASH_DIR:-}" ]]; then
  echo "SLOT_LIST_DIR requires BLOCKHASH_DIR" >&2
  exit 2
fi
if [[ -n "${SLOT_LIST_DIR:-}" && "${OVERWRITE:-0}" == "1" ]]; then
  echo "SLOT_LIST_DIR requires reused raw files; do not set OVERWRITE=1" >&2
  exit 2
fi

download_cid_default=0
reuse_all_raw=1
if [[ -n "${SLOT_LIST_DIR:-}" ]]; then
  for ((epoch=START_EPOCH; epoch<=END_EPOCH; epoch++)); do
    if [[ ! -s "$SLOT_INDEX_DIR/epoch-$epoch-slot-ranges.raw" ]]; then
      echo "SLOT_LIST_DIR requires existing raw file $SLOT_INDEX_DIR/epoch-$epoch-slot-ranges.raw" >&2
      exit 2
    fi
  done
elif [[ "${OVERWRITE:-0}" == "1" ]]; then
  download_cid_default=1
  reuse_all_raw=0
else
  for ((epoch=START_EPOCH; epoch<=END_EPOCH; epoch++)); do
    if [[ ! -s "$SLOT_INDEX_DIR/epoch-$epoch-slot-ranges.raw" ]]; then
      download_cid_default=1
      reuse_all_raw=0
      break
    fi
  done
fi
if [[ -z "${SLOT_LIST_DIR:-}" ]]; then
  DOWNLOAD_CID_INDEX="${DOWNLOAD_CID_INDEX:-$download_cid_default}" \
    "$SCRIPT_DIR/dl-indexes.sh" "$START_EPOCH" "$END_EPOCH" "$INDEXES_DIR"
fi

args=(
  "--start-epoch" "$START_EPOCH"
  "--end-epoch" "$END_EPOCH"
  "--output-dir" "$SLOT_INDEX_DIR"
)

if [[ -n "${SLOT_LIST_DIR:-}" ]]; then
  args+=("--slot-list-dir" "$SLOT_LIST_DIR")
else
  args+=("--indexes-dir" "$INDEXES_DIR")
fi

if [[ -z "${SLOT_LIST_DIR:-}" && -n "${CARS_DIR:-}" ]]; then
  args+=("--cars-dir" "$CARS_DIR")
fi

if [[ -z "${SLOT_LIST_DIR:-}" && -n "${OLD_FAITHFUL_BASE_URL:-}" ]]; then
  args+=("--base-url" "$OLD_FAITHFUL_BASE_URL")
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
  if [[ -n "${SLOT_LIST_DIR:-}" ]]; then
    validator_args=(
      "$SLOT_INDEX_DIR" "$BLOCKHASH_DIR"
      --v2-authoritative
      --start-epoch "$START_EPOCH"
      --end-epoch "$END_EPOCH"
    )
  else
    validator_args=(
      "$SLOT_INDEX_DIR" "$BLOCKHASH_DIR"
      --indexes-dir "$INDEXES_DIR"
      --start-epoch "$START_EPOCH"
      --end-epoch "$END_EPOCH"
    )
    if [[ -n "${CARS_DIR:-}" ]]; then
      validator_args+=("--cars-dir" "$CARS_DIR")
    fi
    if [[ -n "${OLD_FAITHFUL_BASE_URL:-}" ]]; then
      validator_args+=("--base-url" "$OLD_FAITHFUL_BASE_URL")
    fi
    if [[ "$reuse_all_raw" == "1" ]]; then
      validator_args+=("--reuse-raw")
    fi
  fi
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    validator_args+=("--seed-previous-blockhash" "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "${CARGO_BIN:-cargo}" run --release -p of-slot-ranges \
    --bin of-validate-slot-index-v2 -- "${validator_args[@]}"
elif [[ -n "${ARCHIVE_V2_DIR:-}" ]]; then
  validator_args=(
    "$SLOT_INDEX_DIR" "$ARCHIVE_V2_DIR"
    --archive-v2
    --start-epoch "$START_EPOCH"
    --end-epoch "$END_EPOCH"
  )
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    validator_args+=("--seed-previous-blockhash" "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "${CARGO_BIN:-cargo}" run --release -p of-slot-ranges \
    --bin of-validate-slot-index-v2 -- "${validator_args[@]}"
fi

if [[ "${SYNC_R2_AFTER:-0}" == "1" ]]; then
  if [[ -n "${BLOCKHASH_DIR:-}" ]]; then
    SEED_PREVIOUS_BLOCKHASH="${SEED_PREVIOUS_BLOCKHASH:-}" \
      SLOT_INDEX_V2_VALIDATE_BIN="${SLOT_INDEX_V2_VALIDATE_BIN:-$REPO_ROOT/target/release/of-validate-slot-index-v2}" \
      SLOT_INDEX_START_EPOCH="$START_EPOCH" \
      SLOT_INDEX_END_EPOCH="$END_EPOCH" \
      "$SCRIPT_DIR/sync-slot-index-r2.sh" push-v2-authoritative \
      "$SLOT_INDEX_DIR" "$BLOCKHASH_DIR"
  elif [[ -n "${ARCHIVE_V2_DIR:-}" ]]; then
    SLOT_INDEX_V2_VALIDATE_BIN="${SLOT_INDEX_V2_VALIDATE_BIN:-$REPO_ROOT/target/release/of-validate-slot-index-v2}" \
      "$SCRIPT_DIR/sync-slot-index-r2.sh" push-v2-archive \
      "$SLOT_INDEX_DIR" "$ARCHIVE_V2_DIR"
  else
    "$SCRIPT_DIR/sync-slot-index-r2.sh" push "$SLOT_INDEX_DIR"
  fi
fi
