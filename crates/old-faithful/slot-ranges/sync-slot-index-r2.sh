#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-pull}"
LOCAL_DIR="${2:-${SLOT_INDEX_DIR:-out}}"
REMOTE="${3:-${SLOT_INDEX_REMOTE:-r2:blockzilla/slot-index}}"
INCLUDE="${SLOT_INDEX_INCLUDE:-epoch-*-slot-ranges.raw}"
TRANSFERS="${RCLONE_TRANSFERS:-16}"
CHECKERS="${RCLONE_CHECKERS:-32}"
VALIDATOR="${SLOT_INDEX_VALIDATE_BIN:-of-validate-slot-index}"

usage() {
  cat <<'EOF'
Usage:
  sync-slot-index-r2.sh pull [local-dir] [remote]
  sync-slot-index-r2.sh push [local-dir] [remote]
  sync-slot-index-r2.sh validate [local-dir]

Defaults:
  local-dir: $SLOT_INDEX_DIR or ./out
  remote:    $SLOT_INDEX_REMOTE or r2:blockzilla/slot-index

The script copies only epoch-*-slot-ranges.raw files. Pull uses
--ignore-existing so historical offsets are reused and only missing epochs are
downloaded.
EOF
}

validate_slot_index() {
  local root="$1"
  "$VALIDATOR" "$root"
}

case "$MODE" in
  pull)
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
    validate_slot_index "$LOCAL_DIR"
    rclone copy "$LOCAL_DIR" "$REMOTE" \
      --include "$INCLUDE" \
      --transfers "$TRANSFERS" \
      --checkers "$CHECKERS" \
      --stats 10s \
      --stats-one-line
    ;;
  validate)
    validate_slot_index "$LOCAL_DIR"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
