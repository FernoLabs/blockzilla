#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-pull}"
LOCAL_DIR="${2:-${SLOT_INDEX_DIR:-out}}"
INCLUDE="${SLOT_INDEX_INCLUDE:-epoch-*-slot-ranges.raw}"
V2_INCLUDE="${SLOT_INDEX_V2_INCLUDE:-epoch-*-slot-ranges-v2.raw}"
TRANSFERS="${RCLONE_TRANSFERS:-16}"
CHECKERS="${RCLONE_CHECKERS:-32}"
VALIDATOR="${SLOT_INDEX_VALIDATE_BIN:-of-validate-slot-index}"
V2_VALIDATOR="${SLOT_INDEX_V2_VALIDATE_BIN:-of-validate-slot-index-v2}"
V2_START_EPOCH="${SLOT_INDEX_START_EPOCH:-}"
V2_END_EPOCH="${SLOT_INDEX_END_EPOCH:-}"

if [[ -n "$V2_START_EPOCH" || -n "$V2_END_EPOCH" ]]; then
  if [[ ! "$V2_START_EPOCH" =~ ^[0-9]+$ || ! "$V2_END_EPOCH" =~ ^[0-9]+$ ]]; then
    echo "SLOT_INDEX_START_EPOCH and SLOT_INDEX_END_EPOCH must both be unsigned integers" >&2
    exit 2
  fi
  if (( 10#$V2_START_EPOCH > 10#$V2_END_EPOCH )); then
    echo "SLOT_INDEX_START_EPOCH is greater than SLOT_INDEX_END_EPOCH" >&2
    exit 2
  fi
fi

usage() {
  cat <<'EOF'
Usage:
  sync-slot-index-r2.sh pull [local-dir] [remote]
  sync-slot-index-r2.sh push [local-dir] [remote]
  sync-slot-index-r2.sh validate [local-dir]
  sync-slot-index-r2.sh push-v2-authoritative [local-dir] [blockhash-dir] [remote]
  sync-slot-index-r2.sh validate-v2-authoritative [local-dir] [blockhash-dir]
  sync-slot-index-r2.sh push-v2 [local-dir] [indexes-dir] [blockhash-dir] [remote]
  sync-slot-index-r2.sh validate-v2 [local-dir] [indexes-dir] [blockhash-dir]
  sync-slot-index-r2.sh push-v2-archive [local-dir] [archive-v2-sidecar-dir] [remote]
  sync-slot-index-r2.sh push-v2-registry-only [local-dir] [direct-car-registry-dir] [remote]

Defaults:
  local-dir: $SLOT_INDEX_DIR or ./out
  raw remote: $SLOT_INDEX_REMOTE or r2:blockzilla/slot-index
  v2 remote:  $SLOT_INDEX_V2_REMOTE or r2:blockzilla/slot-index-v2
  SEED_PREVIOUS_BLOCKHASH: Optional base58 epoch-0 genesis seed for an
                           unprefixed direct-CAR registry.
  CARS_DIR: Optional local plain CAR root for duplicate-CID proof reads.
  OLD_FAITHFUL_BASE_URL: Optional remote compact-index and CAR mirror.
  SLOT_INDEX_V2_REUSE_RAW=1: Skip the all-CID canonical rebuild for trusted,
                             reused raw indexes. Normal v2 mode only.
  SLOT_INDEX_START_EPOCH / SLOT_INDEX_END_EPOCH:
                             Validate and upload only this inclusive range.

Raw modes copy only epoch-*-slot-ranges.raw files. push-v2-authoritative is the
production v2 path. It validates only the v2 files and blockhash registries.
push-v2 is an explicit CID-backed audit. Archive and direct-CAR modes are also
explicit.
Pull uses --ignore-existing so historical offsets are reused and only missing
epochs are downloaded.
EOF
}

validate_slot_index() {
  local root="$1"
  "$VALIDATOR" "$root"
}

validate_slot_index_v2() {
  local root="$1"
  local indexes_root="$2"
  local registry_root="$3"
  local args=("$root" "$registry_root" --indexes-dir "$indexes_root")
  if [[ -n "$V2_START_EPOCH" ]]; then
    args+=(--start-epoch "$V2_START_EPOCH" --end-epoch "$V2_END_EPOCH")
  fi
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    args+=(--seed-previous-blockhash "$SEED_PREVIOUS_BLOCKHASH")
  fi
  if [[ -n "${CARS_DIR:-}" ]]; then
    args+=(--cars-dir "$CARS_DIR")
  fi
  if [[ -n "${OLD_FAITHFUL_BASE_URL:-}" ]]; then
    args+=(--base-url "$OLD_FAITHFUL_BASE_URL")
  fi
  if [[ "${SLOT_INDEX_V2_REUSE_RAW:-0}" == "1" ]]; then
    args+=(--reuse-raw)
  fi
  "$V2_VALIDATOR" "${args[@]}"
}

validate_slot_index_v2_authoritative() {
  local root="$1"
  local registry_root="$2"
  local args=("$root" "$registry_root" --v2-authoritative)
  if [[ -n "$V2_START_EPOCH" ]]; then
    args+=(--start-epoch "$V2_START_EPOCH" --end-epoch "$V2_END_EPOCH")
  fi
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    args+=(--seed-previous-blockhash "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "$V2_VALIDATOR" "${args[@]}"
}

validate_slot_index_v2_archive() {
  local root="$1"
  local sidecar_root="$2"
  local args=("$root" "$sidecar_root" --archive-v2)
  if [[ -n "$V2_START_EPOCH" ]]; then
    args+=(--start-epoch "$V2_START_EPOCH" --end-epoch "$V2_END_EPOCH")
  fi
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    args+=(--seed-previous-blockhash "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "$V2_VALIDATOR" "${args[@]}"
}

validate_slot_index_v2_registry_only() {
  local root="$1"
  local registry_root="$2"
  local args=("$root" "$registry_root" --registry-only)
  if [[ -n "$V2_START_EPOCH" ]]; then
    args+=(--start-epoch "$V2_START_EPOCH" --end-epoch "$V2_END_EPOCH")
  fi
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    args+=(--seed-previous-blockhash "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "$V2_VALIDATOR" "${args[@]}"
}

upload_slot_index_v2() {
  local root="$1"
  local remote="$2"
  local include_args=()
  if [[ -n "$V2_START_EPOCH" ]]; then
    local epoch
    for ((epoch=10#$V2_START_EPOCH; epoch<=10#$V2_END_EPOCH; epoch++)); do
      include_args+=(--include "epoch-$epoch-slot-ranges-v2.raw")
    done
  else
    include_args+=(--include "$V2_INCLUDE")
  fi
  rclone copy "$root" "$remote" \
    "${include_args[@]}" \
    --immutable \
    --transfers "$TRANSFERS" \
    --checkers "$CHECKERS" \
    --stats 10s \
    --stats-one-line
  rclone check "$root" "$remote" \
    "${include_args[@]}" \
    --one-way \
    --checkers "$CHECKERS"
}

case "$MODE" in
  pull)
    REMOTE="${3:-${SLOT_INDEX_REMOTE:-r2:blockzilla/slot-index}}"
    mkdir -p "$LOCAL_DIR"
    rclone copy "$REMOTE" "$LOCAL_DIR" \
      --include "$INCLUDE" \
      --ignore-existing \
      --transfers "$TRANSFERS" \
      --checkers "$CHECKERS" \
      --stats 10s \
      --stats-one-line
    validate_slot_index "$LOCAL_DIR"
    ;;
  push)
    REMOTE="${3:-${SLOT_INDEX_REMOTE:-r2:blockzilla/slot-index}}"
    validate_slot_index "$LOCAL_DIR"
    rclone copy "$LOCAL_DIR" "$REMOTE" \
      --include "$INCLUDE" \
      --transfers "$TRANSFERS" \
      --checkers "$CHECKERS" \
      --stats 10s \
      --stats-one-line
    ;;
  push-v2-authoritative)
    REGISTRY_ROOT="${3:-${BLOCKHASH_DIR:-$LOCAL_DIR/blockhash-registry}}"
    REMOTE="${4:-${SLOT_INDEX_V2_REMOTE:-r2:blockzilla/slot-index-v2}}"
    validate_slot_index_v2_authoritative "$LOCAL_DIR" "$REGISTRY_ROOT"
    upload_slot_index_v2 "$LOCAL_DIR" "$REMOTE"
    ;;
  push-v2)
    INDEXES_ROOT="${3:-${INDEXES_DIR:-indexes}}"
    REGISTRY_ROOT="${4:-${BLOCKHASH_DIR:-$LOCAL_DIR/blockhash-registry}}"
    REMOTE="${5:-${SLOT_INDEX_V2_REMOTE:-r2:blockzilla/slot-index-v2}}"
    validate_slot_index_v2 "$LOCAL_DIR" "$INDEXES_ROOT" "$REGISTRY_ROOT"
    upload_slot_index_v2 "$LOCAL_DIR" "$REMOTE"
    ;;
  push-v2-archive)
    SIDECAR_ROOT="${3:-${ARCHIVE_V2_DIR:-$LOCAL_DIR/archive-v2}}"
    REMOTE="${4:-${SLOT_INDEX_V2_REMOTE:-r2:blockzilla/slot-index-v2}}"
    validate_slot_index_v2_archive "$LOCAL_DIR" "$SIDECAR_ROOT"
    upload_slot_index_v2 "$LOCAL_DIR" "$REMOTE"
    ;;
  push-v2-registry-only)
    REGISTRY_ROOT="${3:-${BLOCKHASH_DIR:-$LOCAL_DIR/blockhash-registry}}"
    REMOTE="${4:-${SLOT_INDEX_V2_REMOTE:-r2:blockzilla/slot-index-v2}}"
    validate_slot_index_v2_registry_only "$LOCAL_DIR" "$REGISTRY_ROOT"
    upload_slot_index_v2 "$LOCAL_DIR" "$REMOTE"
    ;;
  validate)
    validate_slot_index "$LOCAL_DIR"
    ;;
  validate-v2-authoritative)
    REGISTRY_ROOT="${3:-${BLOCKHASH_DIR:-$LOCAL_DIR/blockhash-registry}}"
    validate_slot_index_v2_authoritative "$LOCAL_DIR" "$REGISTRY_ROOT"
    ;;
  validate-v2)
    INDEXES_ROOT="${3:-${INDEXES_DIR:-indexes}}"
    REGISTRY_ROOT="${4:-${BLOCKHASH_DIR:-$LOCAL_DIR/blockhash-registry}}"
    validate_slot_index_v2 "$LOCAL_DIR" "$INDEXES_ROOT" "$REGISTRY_ROOT"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
