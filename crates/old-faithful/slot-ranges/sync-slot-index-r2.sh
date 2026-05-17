#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-pull}"
LOCAL_DIR="${2:-${SLOT_INDEX_DIR:-out}}"
REMOTE="${3:-${SLOT_INDEX_REMOTE:-r2:blockzilla/slot-index}}"
INCLUDE="${SLOT_INDEX_INCLUDE:-epoch-*-slot-ranges.raw}"
TRANSFERS="${RCLONE_TRANSFERS:-16}"
CHECKERS="${RCLONE_CHECKERS:-32}"

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
  python3 - "$root" <<'PY'
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
expected_size = 432_000 * 12

if not root.exists():
    print(f"missing directory: {root}", file=sys.stderr)
    sys.exit(1)

valid = []
bad = []
for path in root.glob("epoch-*-slot-ranges.raw"):
    match = re.fullmatch(r"epoch-(\d+)-slot-ranges\.raw", path.name)
    if not match:
        continue
    epoch = int(match.group(1))
    size = path.stat().st_size
    if size == expected_size:
        valid.append(epoch)
    else:
        bad.append((epoch, size))

valid.sort()
present = set(valid)
missing_between = []
if valid:
    missing_between = [epoch for epoch in range(valid[0], valid[-1] + 1) if epoch not in present]

print(f"slot_index_dir={root}")
print(f"valid_count={len(valid)}")
print(f"first_last={(valid[0], valid[-1]) if valid else None}")
print(f"bad_count={len(bad)}")
print(f"missing_between_count={len(missing_between)}")

if bad:
    print(f"bad_files={bad[:20]}", file=sys.stderr)
if missing_between:
    print(f"missing_between={missing_between[:50]}", file=sys.stderr)

if bad:
    sys.exit(2)
PY
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
