#!/bin/sh
set -eu

# Reconnect supervisor for the raw-only Yellowstone recorder. The Rust process owns
# the durable cursor. This wrapper owns only disk admission, restart backoff, and a
# metadata-only health signal suitable for a concurrently appended journal.

umask 077
export LC_ALL=C

BIN=${BLOCKZILLA_RAW_BIN:-/usr/local/bin/blockzilla-live-producer}
OUTPUT_DIR=${BLOCKZILLA_RAW_OUTPUT_DIR:-/data/grpc-raw}
INITIAL_FROM_SLOT=${BLOCKZILLA_RAW_FROM_SLOT:-}
MIN_FREE_BYTES=${BLOCKZILLA_RAW_MIN_FREE_BYTES:-21474836480}
MAX_BLOCKS=${BLOCKZILLA_RAW_MAX_BLOCKS:-1000000000}
TIMEOUT_SECS=${BLOCKZILLA_RAW_TIMEOUT_SECS:-31536000}
RESTART_DELAY_SECS=${BLOCKZILLA_RAW_RESTART_DELAY_SECS:-5}
LOW_DISK_RECHECK_SECS=${BLOCKZILLA_RAW_LOW_DISK_RECHECK_SECS:-60}
COMPRESSION_LEVEL=${BLOCKZILLA_RAW_COMPRESSION_LEVEL:-1}
SEGMENT_TARGET_BYTES=${BLOCKZILLA_RAW_SEGMENT_TARGET_BYTES:-268435456}
MAX_RECORD_BYTES=${BLOCKZILLA_RAW_MAX_RECORD_BYTES:-134217728}
CLUSTER_ID=${BLOCKZILLA_RAW_CLUSTER_ID:-solana-mainnet}
ORIGIN_NODE_ID=${BLOCKZILLA_RAW_ORIGIN_NODE_ID:-hetzner-dokploy}
SOURCE_ID=${BLOCKZILLA_RAW_SOURCE_ID:-grpc-raw-hetzner}

STATE_DIR=${BLOCKZILLA_RAW_STATE_DIR:-/tmp}
STATE_FILE=$STATE_DIR/blockzilla-raw-recorder.state
STARTED_FILE=$STATE_DIR/blockzilla-raw-recorder.started
JOURNAL_FILE=$OUTPUT_DIR/raw-blocks.jsonl

# These defaults keep a quiet connection alive without enabling adaptive HTTP/2
# windows. The endpoint may still choose an uncompressed response.
export BLOCKZILLA_GRPC_ACCEPT_COMPRESSION=${BLOCKZILLA_GRPC_ACCEPT_COMPRESSION:-zstd}
export BLOCKZILLA_GRPC_HTTP2_ADAPTIVE_WINDOW=${BLOCKZILLA_GRPC_HTTP2_ADAPTIVE_WINDOW:-false}
export BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS=${BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS:-30}
export BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS=${BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS:-10}
export BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE=${BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE:-false}

timestamp() {
  date -u +%FT%TZ
}

require_uint() {
  variable_name=$1
  variable_value=$2
  case "$variable_value" in
    ''|*[!0-9]*)
      echo "invalid unsigned integer in $variable_name" >&2
      exit 2
      ;;
  esac
}

write_state() {
  state_value=$1
  state_tmp=$STATE_FILE.$$
  printf '%s %s\n' "$state_value" "$(date +%s)" > "$state_tmp"
  mv -f "$state_tmp" "$STATE_FILE"
}

healthcheck() {
  stale_after=${BLOCKZILLA_RAW_STALE_AFTER_SECS:-180}
  startup_grace=${BLOCKZILLA_RAW_STARTUP_GRACE_SECS:-300}
  require_uint BLOCKZILLA_RAW_STALE_AFTER_SECS "$stale_after"
  require_uint BLOCKZILLA_RAW_STARTUP_GRACE_SECS "$startup_grace"

  if [ -r "$STATE_FILE" ]; then
    IFS=' ' read -r recorder_state _state_time < "$STATE_FILE" || recorder_state=unknown
    if [ "$recorder_state" = low_disk ] || [ "$recorder_state" = stopping ]; then
      echo "raw recorder state is $recorder_state" >&2
      return 1
    fi
  fi

  now=$(date +%s)
  if [ -s "$JOURNAL_FILE" ]; then
    modified=$(stat -c %Y "$JOURNAL_FILE") || return 1
    if [ "$modified" -gt "$now" ]; then
      age=0
    else
      age=$((now - modified))
    fi
    if [ "$age" -le "$stale_after" ]; then
      return 0
    fi
    echo "raw journal is stale: age_seconds=$age limit_seconds=$stale_after" >&2
    return 1
  fi

  if [ ! -e "$STARTED_FILE" ]; then
    echo "raw recorder startup marker is missing" >&2
    return 1
  fi
  started=$(stat -c %Y "$STARTED_FILE") || return 1
  if [ "$started" -gt "$now" ]; then
    startup_age=0
  else
    startup_age=$((now - started))
  fi
  if [ "$startup_age" -le "$startup_grace" ]; then
    return 0
  fi
  echo "raw journal was not created within startup grace: age_seconds=$startup_age" >&2
  return 1
}

if [ "${1:-}" = --healthcheck ]; then
  healthcheck
  exit $?
fi

if [ "$#" -ne 0 ]; then
  echo "usage: $0 [--healthcheck]" >&2
  exit 2
fi

: "${BLOCKZILLA_GRPC_ENDPOINT:?set BLOCKZILLA_GRPC_ENDPOINT}"
: "${BLOCKZILLA_GRPC_X_TOKEN:?set BLOCKZILLA_GRPC_X_TOKEN}"

for numeric_setting in \
  "BLOCKZILLA_RAW_MIN_FREE_BYTES:$MIN_FREE_BYTES" \
  "BLOCKZILLA_RAW_MAX_BLOCKS:$MAX_BLOCKS" \
  "BLOCKZILLA_RAW_TIMEOUT_SECS:$TIMEOUT_SECS" \
  "BLOCKZILLA_RAW_RESTART_DELAY_SECS:$RESTART_DELAY_SECS" \
  "BLOCKZILLA_RAW_LOW_DISK_RECHECK_SECS:$LOW_DISK_RECHECK_SECS" \
  "BLOCKZILLA_RAW_SEGMENT_TARGET_BYTES:$SEGMENT_TARGET_BYTES" \
  "BLOCKZILLA_RAW_MAX_RECORD_BYTES:$MAX_RECORD_BYTES"
do
  setting_name=${numeric_setting%%:*}
  setting_value=${numeric_setting#*:}
  require_uint "$setting_name" "$setting_value"
done
if [ -n "$INITIAL_FROM_SLOT" ]; then
  require_uint BLOCKZILLA_RAW_FROM_SLOT "$INITIAL_FROM_SLOT"
fi
compression_level_digits=$COMPRESSION_LEVEL
case "$compression_level_digits" in
  -*) compression_level_digits=${compression_level_digits#-} ;;
esac
case "$compression_level_digits" in
  ''|*[!0-9]*)
    echo "invalid integer in BLOCKZILLA_RAW_COMPRESSION_LEVEL" >&2
    exit 2
    ;;
esac

if [ ! -x "$BIN" ]; then
  echo "missing executable: $BIN" >&2
  exit 2
fi
if [ "$(uname -m)" = x86_64 ] \
  && ! grep -qE '(^|[[:space:]])aes([[:space:]]|$)' /proc/cpuinfo
then
  echo "x86_64 host does not expose the AES CPU feature required by gxhash" >&2
  exit 2
fi

mkdir -p "$OUTPUT_DIR" "$STATE_DIR"
: > "$STARTED_FILE"
write_state starting

available_bytes() {
  available_kib=$(df -Pk "$OUTPUT_DIR" | awk 'NR == 2 { print $4 }')
  case "$available_kib" in
    ''|*[!0-9]*) return 1 ;;
  esac
  printf '%s\n' "$((available_kib * 1024))"
}

child_pid=
terminate() {
  write_state stopping
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  exit 0
}
trap terminate INT TERM HUP

while :; do
  if ! free_bytes=$(available_bytes); then
    write_state disk_check_failed
    echo "$(timestamp) raw_recorder disk_check_failed output=$OUTPUT_DIR" >&2
    sleep "$RESTART_DELAY_SECS"
    continue
  fi
  if [ "$free_bytes" -lt "$MIN_FREE_BYTES" ]; then
    write_state low_disk
    echo "$(timestamp) raw_recorder paused_low_disk available_bytes=$free_bytes reserve_bytes=$MIN_FREE_BYTES" >&2
    sleep "$LOW_DISK_RECHECK_SECS"
    continue
  fi

  set -- \
    record-grpc-raw \
    --endpoint "$BLOCKZILLA_GRPC_ENDPOINT" \
    --output-dir "$OUTPUT_DIR" \
    --max-blocks "$MAX_BLOCKS" \
    --timeout-secs "$TIMEOUT_SECS" \
    --compression-level "$COMPRESSION_LEVEL" \
    --segment-target-bytes "$SEGMENT_TARGET_BYTES" \
    --max-record-bytes "$MAX_RECORD_BYTES" \
    --min-free-bytes "$MIN_FREE_BYTES" \
    --cluster-id "$CLUSTER_ID" \
    --origin-node-id "$ORIGIN_NODE_ID" \
    --source-id "$SOURCE_ID"
  if [ -n "$INITIAL_FROM_SLOT" ]; then
    set -- "$@" --from-slot "$INITIAL_FROM_SLOT"
  fi

  write_state running
  echo "$(timestamp) raw_recorder starting output=$OUTPUT_DIR free_bytes=$free_bytes" >&2
  "$BIN" "$@" &
  child_pid=$!
  if wait "$child_pid"; then
    status=0
  else
    status=$?
  fi
  child_pid=
  write_state backoff
  echo "$(timestamp) raw_recorder exited status=$status; retrying in ${RESTART_DELAY_SECS}s" >&2
  sleep "$RESTART_DELAY_SECS"
done
