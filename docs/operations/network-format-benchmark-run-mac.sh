#!/usr/bin/env bash
set -euo pipefail

# This runner is intentionally limited to epoch 0. Epoch 0 is the only approved
# sample that is currently present in the public benchmark bucket.
EPOCH="${1:-0}"
WALLET="${2:-11111111111111111111111111111111}"
ORIGIN="${3:-https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev}"
RESULT_DIR="${4:-$PWD/benchmark-results/mac-network-epoch-0-r2}"
CAR_PATH="${5:-/private/tmp/blockzilla-network-benchmark-inputs-r1/epoch-0.car}"
COMPARATOR_BIN="${6:-/private/tmp/blockzilla-index-archive-fast/target/release/archive-three-format-compare}"
JETSTREAMER_BIN="${7:-/private/tmp/jetstreamer-semantic-target/release/jetstreamer-semantic-compare}"
JETSTREAMER_IMAGE="${8:-jetstreamer-build-env-r1:latest}"

if [ "$EPOCH" -ne 0 ]; then
  echo "unsupported_epoch: only epoch 0 is present in the public benchmark bucket" >&2
  exit 2
fi

SLOT_END_INCLUSIVE=1023
EXPECTED_CAR_BYTES=4286945461
CAR_BASE="$ORIGIN/car"
COMPACT_BASE="$ORIGIN/compact-v2"
V3_BASE="$ORIGIN/indexer-v3/"
SCRATCH="$RESULT_DIR/comparator-scratch"
V3_CACHE_DIR="$RESULT_DIR/v3-cache"

test ! -e "$RESULT_DIR"
test -f "$CAR_PATH"
test "$(stat -f %z "$CAR_PATH")" -eq "$EXPECTED_CAR_BYTES"
test -x "$COMPARATOR_BIN"
test -x "$JETSTREAMER_BIN"
docker image inspect "$JETSTREAMER_IMAGE" >/dev/null

mkdir -m 700 -p "$RESULT_DIR"
mkdir -m 700 -p "$SCRATCH"
mkdir -m 700 -p "$V3_CACHE_DIR"

python3 -c 'import socket, sys, urllib.parse; host=urllib.parse.urlparse(sys.argv[1]).hostname; print(host, socket.gethostbyname(host))' "$ORIGIN"

"$COMPARATOR_BIN" --help > "$RESULT_DIR/comparator-help.txt"
docker run --rm --platform linux/amd64 \
  -v "$JETSTREAMER_BIN:/usr/local/bin/jetstreamer-semantic-compare:ro" \
  "$JETSTREAMER_IMAGE" \
  /usr/local/bin/jetstreamer-semantic-compare --help \
  > "$RESULT_DIR/jetstreamer-help.txt"

curl -fsS -I "$CAR_BASE/$EPOCH/epoch-$EPOCH.car" > "$RESULT_DIR/car-head.txt"
curl -fsS -I "$CAR_BASE/$EPOCH/epoch-$EPOCH-slot-ranges.raw" > "$RESULT_DIR/car-index-head.txt"
curl -fsS -D "$RESULT_DIR/compact-range-head.txt" -o /dev/null \
  -H 'Range: bytes=0-0' \
  "$COMPACT_BASE/v1/epochs/$EPOCH/files/archive-v2-blocks.zstd"
curl -fsS -D "$RESULT_DIR/v3-range-head.txt" -o /dev/null \
  -H 'Range: bytes=0-0' \
  "$V3_BASE/v1/epochs/$EPOCH/files/archive-v2-standalone-messages.wincode"

/usr/bin/time -lp docker run --rm --platform linux/amd64 \
  -v "$JETSTREAMER_BIN:/usr/local/bin/jetstreamer-semantic-compare:ro" \
  "$JETSTREAMER_IMAGE" \
  /usr/local/bin/jetstreamer-semantic-compare \
  --epoch "$EPOCH" \
  --end-slot-inclusive "$SLOT_END_INCLUSIVE" \
  --wallet "$WALLET" \
  --download-threads 4 \
  --buffer-window-bytes 268435456 \
  --http-base "$CAR_BASE/" \
  --index-base "$CAR_BASE/" \
  --max-transactions 5000000 \
  --max-records 10000000 \
  --sample-limit 20 \
  > "$RESULT_DIR/jetstreamer-slots-0-1023.json" \
  2> "$RESULT_DIR/jetstreamer-slots-0-1023.log"

run_comparator() {
  local label="$1"
  /usr/bin/time -lp "$COMPARATOR_BIN" \
    --car "$CAR_PATH" \
    --trusted-car \
    --compact-v2-base-url "$COMPACT_BASE" \
    --v3-base-url "$V3_BASE" \
    --v3-cache-root "$V3_CACHE_DIR" \
    --epoch "$EPOCH" \
    --slots-per-epoch 432000 \
    --wallet "$WALLET" \
    --scratch "$SCRATCH" \
    --mismatch-limit 20 \
    --max-blocks 1024 \
    > "$RESULT_DIR/comparator-$label.json" \
    2> "$RESULT_DIR/comparator-$label.log"
}

run_comparator cold
run_comparator warm

echo "benchmark_complete: $RESULT_DIR"
