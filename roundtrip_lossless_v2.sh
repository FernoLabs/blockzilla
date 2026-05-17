#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  ./roundtrip_lossless_v2.sh <input.car|input.car.zst> <work_dir> [roundtrip.car]

Examples:
  ./roundtrip_lossless_v2.sh epochs/epoch-0.car.zst /tmp/epoch-0-test
  ./roundtrip_lossless_v2.sh /data/epoch-123.car /tmp/epoch-123-test /tmp/epoch-123-test/out.car
EOF
  exit 1
}

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log() {
  echo "$(timestamp) $*"
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: missing required command '$1'" >&2
    exit 1
  fi
}

if [[ $# -lt 2 || $# -gt 3 ]]; then
  usage
fi

INPUT="$1"
WORK_DIR="$2"
ROUNDTRIP_CAR="${3:-${WORK_DIR}/roundtrip.car}"
BUNDLE_DIR="${WORK_DIR}/lossless-v2"

if [[ ! -f "$INPUT" ]]; then
  echo "Error: input file not found: $INPUT" >&2
  exit 1
fi

need_cmd cargo
need_cmd shasum
need_cmd cmp

mkdir -p "$WORK_DIR"
rm -rf "$BUNDLE_DIR"
mkdir -p "$BUNDLE_DIR"
rm -f "$ROUNDTRIP_CAR"

case "$INPUT" in
  *.car)
    SOURCE_CAR="$INPUT"
    ;;
  *.car.zst)
    need_cmd zstd
    INPUT_CAR="${INPUT%.zst}"
    WORK_CAR="${WORK_DIR}/$(basename "${INPUT%.zst}")"

    if [[ -f "$INPUT_CAR" ]]; then
      SOURCE_CAR="$INPUT_CAR"
      log "reusing existing source CAR -> $SOURCE_CAR"
    elif [[ -f "$WORK_CAR" ]]; then
      SOURCE_CAR="$WORK_CAR"
      log "reusing existing decompressed CAR -> $SOURCE_CAR"
    else
      SOURCE_CAR="$WORK_CAR"
      log "decompressing source CAR for byte-for-byte comparison -> $SOURCE_CAR"
      zstd -d -k -f "$INPUT" -o "$SOURCE_CAR"
    fi
    ;;
  *)
    echo "Error: input must end with .car or .car.zst" >&2
    exit 1
    ;;
esac

log "building lossless-v2 bundle from $INPUT"
cargo run --release -p of-archive-importer -- \
  build-lossless-v2 "$INPUT" "$BUNDLE_DIR"

log "extracting CAR to $ROUNDTRIP_CAR"
cargo run --release -p of-archive-importer -- \
  extract-lossless-v2 "$BUNDLE_DIR" "$ROUNDTRIP_CAR"

SOURCE_HASH="$(shasum -a 256 "$SOURCE_CAR" | awk '{print $1}')"
ROUNDTRIP_HASH="$(shasum -a 256 "$ROUNDTRIP_CAR" | awk '{print $1}')"

echo
echo "source:    $SOURCE_HASH  $SOURCE_CAR"
echo "roundtrip: $ROUNDTRIP_HASH  $ROUNDTRIP_CAR"

if cmp -s "$SOURCE_CAR" "$ROUNDTRIP_CAR"; then
  echo "MATCH"
else
  echo "DIFF"
  exit 2
fi
