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
  sync-slot-index-r2.sh push-v2 [local-dir] [blockhash-dir] [remote]
  sync-slot-index-r2.sh validate-v2 [local-dir] [blockhash-dir]

Defaults:
  local-dir: $SLOT_INDEX_DIR or ./out
  raw remote: $SLOT_INDEX_REMOTE or r2:blockzilla/slot-index
  v2 remote:  $SLOT_INDEX_V2_REMOTE or r2:blockzilla/slot-index-v2

Raw modes copy only epoch-*-slot-ranges.raw files. push-v2 copies only
epoch-*-slot-ranges-v2.raw files after strict validation against the raw files
and blockhash registries. Pull uses --ignore-existing so historical offsets
are reused and only missing epochs are downloaded.
EOF
}

validate_slot_index() {
  local root="$1"
  "$VALIDATOR" "$root"
}

validate_slot_index_v2() {
  local root="$1"
  local blockhash_root="$2"
  "$V2_VALIDATOR" "$root" "$blockhash_root"
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
    BLOCKHASH_ROOT="${3:-${BLOCKHASH_DIR:-$LOCAL_DIR/blockhash-registry}}"
    REMOTE="${4:-${SLOT_INDEX_V2_REMOTE:-r2:blockzilla/slot-index-v2}}"
    validate_slot_index_v2 "$LOCAL_DIR" "$BLOCKHASH_ROOT"
    rclone copy "$LOCAL_DIR" "$REMOTE" \
      --include "$V2_INCLUDE" \
      --immutable \
      --transfers "$TRANSFERS" \
      --checkers "$CHECKERS" \
      --stats 10s \
      --stats-one-line
    rclone check "$LOCAL_DIR" "$REMOTE" \
      --include "$V2_INCLUDE" \
      --one-way \
      --checkers "$CHECKERS"
    ;;
  validate)
    validate_slot_index "$LOCAL_DIR"
    ;;
  validate-v2)
    BLOCKHASH_ROOT="${3:-${BLOCKHASH_DIR:-$LOCAL_DIR/blockhash-registry}}"
    validate_slot_index_v2 "$LOCAL_DIR" "$BLOCKHASH_ROOT"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
