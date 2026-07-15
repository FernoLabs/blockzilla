#!/bin/bash
set -uo pipefail

# Keep a crash-recoverable, compressed copy of live Yellowstone block events on macOS.
# The Rust recorder owns the durable cursor; this wrapper only handles reconnects and
# pauses before the filesystem reserve is exhausted.

umask 077
export LC_ALL=C

REPO=${BLOCKZILLA_REPO:-/Users/augustin/Developement/ferno/blockzilla-v1}
BIN=${BLOCKZILLA_RAW_BIN:-/Users/augustin/.local/bin/blockzilla-live-producer-raw-bridge}
OUTPUT_DIR=${BLOCKZILLA_RAW_OUTPUT_DIR:-/Users/augustin/blockzilla-live-bridge/raw-spool/current}
INITIAL_FROM_SLOT=${BLOCKZILLA_RAW_FROM_SLOT:-}
MIN_FREE_BYTES=${BLOCKZILLA_RAW_MIN_FREE_BYTES:-21474836480}
MAX_BLOCKS=${BLOCKZILLA_RAW_MAX_BLOCKS:-1000000}
TIMEOUT_SECS=${BLOCKZILLA_RAW_TIMEOUT_SECS:-86400}
RESTART_DELAY_SECS=${BLOCKZILLA_RAW_RESTART_DELAY_SECS:-5}
LOW_DISK_RECHECK_SECS=${BLOCKZILLA_RAW_LOW_DISK_RECHECK_SECS:-60}
ORIGIN_NODE_ID=${BLOCKZILLA_RAW_ORIGIN_NODE_ID:-mac-bridge}
SOURCE_ID=${BLOCKZILLA_RAW_SOURCE_ID:-grpc-raw-primary}
LOCAL_INTERFACE=${BLOCKZILLA_GRPC_LOCAL_INTERFACE:-}

if [[ -f "$REPO/personal/live-producer.env" ]]; then
  # shellcheck source=/dev/null
  source "$REPO/personal/live-producer.env"
fi

: "${BLOCKZILLA_GRPC_ENDPOINT:?set BLOCKZILLA_GRPC_ENDPOINT in personal/live-producer.env}"
: "${BLOCKZILLA_GRPC_X_TOKEN:?set BLOCKZILLA_GRPC_X_TOKEN in personal/live-producer.env}"

if [[ ! -x "$BIN" ]]; then
  echo "missing executable: $BIN" >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR"

if [[ -z ${BLOCKZILLA_GRPC_LOCAL_ADDRESS:-} && -n "$LOCAL_INTERFACE" ]]; then
  if local_address=$(/usr/sbin/ipconfig getifaddr "$LOCAL_INTERFACE" 2>/dev/null); then
    export BLOCKZILLA_GRPC_LOCAL_ADDRESS=$local_address
    echo "$(date -u +%FT%TZ) raw_recorder grpc_local_address=$local_address interface=$LOCAL_INTERFACE" >&2
  else
    echo "$(date -u +%FT%TZ) raw_recorder grpc_local_address_unavailable interface=$LOCAL_INTERFACE using_default_route" >&2
  fi
fi

timestamp() {
  date -u +%FT%TZ
}

available_bytes() {
  local available_kib
  available_kib=$(df -Pk "$OUTPUT_DIR" | awk 'NR == 2 { print $4 }')
  if [[ ! "$available_kib" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  printf '%s\n' "$((available_kib * 1024))"
}

child_pid=
terminate() {
  if [[ -n "$child_pid" ]]; then
    kill -TERM "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  exit 0
}
trap terminate INT TERM HUP

while true; do
  free_bytes=$(available_bytes) || {
    echo "$(timestamp) raw_recorder disk_check_failed output=$OUTPUT_DIR" >&2
    sleep "$RESTART_DELAY_SECS"
    continue
  }
  if (( free_bytes < MIN_FREE_BYTES )); then
    echo "$(timestamp) raw_recorder paused_low_disk available_bytes=$free_bytes reserve_bytes=$MIN_FREE_BYTES" >&2
    sleep "$LOW_DISK_RECHECK_SECS"
    continue
  fi

  args=(
    record-grpc-raw
    --endpoint "$BLOCKZILLA_GRPC_ENDPOINT"
    --output-dir "$OUTPUT_DIR"
    --max-blocks "$MAX_BLOCKS"
    --timeout-secs "$TIMEOUT_SECS"
    --compression-level 1
    --segment-target-bytes 268435456
    --max-record-bytes 134217728
    --min-free-bytes "$MIN_FREE_BYTES"
    --origin-node-id "$ORIGIN_NODE_ID"
    --source-id "$SOURCE_ID"
  )
  if [[ -n "$INITIAL_FROM_SLOT" ]]; then
    args+=(--from-slot "$INITIAL_FROM_SLOT")
  fi

  echo "$(timestamp) raw_recorder starting output=$OUTPUT_DIR free_bytes=$free_bytes" >&2
  "$BIN" "${args[@]}" &
  child_pid=$!
  wait "$child_pid"
  status=$?
  child_pid=
  echo "$(timestamp) raw_recorder exited status=$status; retrying in ${RESTART_DELAY_SECS}s" >&2
  sleep "$RESTART_DELAY_SECS"
done
