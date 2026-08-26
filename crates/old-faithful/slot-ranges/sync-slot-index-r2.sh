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

usage() {
  cat <<'EOF'
Usage:
  sync-slot-index-r2.sh pull [local-dir] [remote]
  sync-slot-index-r2.sh push [local-dir] [remote]
  sync-slot-index-r2.sh validate [local-dir]
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

Raw modes copy only epoch-*-slot-ranges.raw files. push-v2 validates ordered
slots from Old Faithful slot-to-CID indexes, CAR ranges from raw indexes, and
hashes from blockhash registries. Archive and direct-CAR modes are explicit.
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
  local seed_args=()
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    seed_args+=(--seed-previous-blockhash "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "$V2_VALIDATOR" "$root" "$registry_root" --indexes-dir "$indexes_root" "${seed_args[@]}"
}

validate_slot_index_v2_archive() {
  local root="$1"
  local sidecar_root="$2"
  local seed_args=()
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    seed_args+=(--seed-previous-blockhash "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "$V2_VALIDATOR" "$root" "$sidecar_root" --archive-v2 "${seed_args[@]}"
}

validate_slot_index_v2_registry_only() {
  local root="$1"
  local registry_root="$2"
  local seed_args=()
  if [[ -n "${SEED_PREVIOUS_BLOCKHASH:-}" ]]; then
    seed_args+=(--seed-previous-blockhash "$SEED_PREVIOUS_BLOCKHASH")
  fi
  "$V2_VALIDATOR" "$root" "$registry_root" --registry-only "${seed_args[@]}"
}

upload_slot_index_v2() {
  local root="$1"
  local remote="$2"
  rclone copy "$root" "$remote" \
    --include "$V2_INCLUDE" \
    --immutable \
    --transfers "$TRANSFERS" \
    --checkers "$CHECKERS" \
    --stats 10s \
    --stats-one-line
  rclone check "$root" "$remote" \
    --include "$V2_INCLUDE" \
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
