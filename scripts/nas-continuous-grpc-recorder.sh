#!/usr/bin/env bash
set -euo pipefail

# Durable, epoch-agnostic safety spool for the NAS live indexer. This process
# deliberately never uses --stop-at-epoch-boundary: archive inspection and
# epoch packaging must not be able to interrupt gRPC consumption.

REPO=${BLOCKZILLA_REPO:-/volume1/@home/ach/dev/blockzilla-v1-registry-mphf-20260616}
BIN=${BLOCKZILLA_RAW_BIN:-$REPO/target/release/blockzilla-live-producer-raw-continuous}
OUTPUT_DIR=${BLOCKZILLA_RAW_OUTPUT_DIR:-/volume1/@home/ach/dev/blockzilla-grpc-raw-continuous}
LOG_ROOT=${BLOCKZILLA_RAW_LOG_ROOT:-$REPO/logs/raw-continuous}
STATE_ROOT=${BLOCKZILLA_RAW_STATE_ROOT:-$REPO/state/raw-continuous}
INITIAL_FROM_SLOT=${BLOCKZILLA_RAW_FROM_SLOT:-}
MAX_BLOCKS=${BLOCKZILLA_RAW_MAX_BLOCKS:-1000000000}
TIMEOUT_SECS=${BLOCKZILLA_RAW_TIMEOUT_SECS:-31536000}
IDLE_TIMEOUT_SECS=${BLOCKZILLA_RAW_IDLE_TIMEOUT_SECS:-180}
RESTART_DELAY_SECS=${BLOCKZILLA_RAW_RESTART_DELAY_SECS:-1}
MIN_FREE_BYTES=${BLOCKZILLA_RAW_MIN_FREE_BYTES:-2199023255552}
COMPRESSION_LEVEL=${BLOCKZILLA_RAW_COMPRESSION_LEVEL:-1}
SEGMENT_TARGET_BYTES=${BLOCKZILLA_RAW_SEGMENT_TARGET_BYTES:-268435456}
MAX_RECORD_BYTES=${BLOCKZILLA_RAW_MAX_RECORD_BYTES:-134217728}
REQUIRE_COMPLETE_POH=${BLOCKZILLA_RAW_REQUIRE_COMPLETE_POH:-0}

mkdir -p "$OUTPUT_DIR" "$LOG_ROOT" "$STATE_ROOT"

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

exec 9> "$STATE_ROOT/supervisor.lock"
if ! flock -n 9; then
  echo "continuous raw recorder supervisor is already running" >&2
  exit 2
fi

child_pid=
stop_child() {
  if [[ -n "$child_pid" ]]; then
    kill -TERM "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  exit 0
}
trap stop_child TERM INT

ts() {
  date -u +%FT%TZ
}

while true; do
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  report="$LOG_ROOT/attempt-$stamp-report.json"
  log_file="$LOG_ROOT/attempt-$stamp.log"
  from_slot_args=()
  if [[ ! -s "$OUTPUT_DIR/raw-blocks.jsonl" && -n "$INITIAL_FROM_SLOT" ]]; then
    from_slot_args=(--from-slot "$INITIAL_FROM_SLOT")
  fi
  poh_args=()
  if [[ "$REQUIRE_COMPLETE_POH" == "1" ]]; then
    poh_args=(--require-complete-poh)
  fi

  printf '%s recorder_start output_dir=%s report=%s\n' "$(ts)" "$OUTPUT_DIR" "$report"
  set +e
  "$BIN" record-grpc-raw \
    --endpoint "$BLOCKZILLA_GRPC_ENDPOINT" \
    --output-dir "$OUTPUT_DIR" \
    "${from_slot_args[@]}" \
    --resume-coverage-warning-file "$OUTPUT_DIR/.monitoring/resume-coverage-warning.json" \
    --max-blocks "$MAX_BLOCKS" \
    --timeout-secs "$TIMEOUT_SECS" \
    --idle-timeout-secs "$IDLE_TIMEOUT_SECS" \
    --compression-level "$COMPRESSION_LEVEL" \
    --segment-target-bytes "$SEGMENT_TARGET_BYTES" \
    --max-record-bytes "$MAX_RECORD_BYTES" \
    --min-free-bytes "$MIN_FREE_BYTES" \
    --cluster-id solana-mainnet \
    --origin-node-id blockzilla-nas-primary \
    --source-id grpc-raw-nas-continuous \
    "${poh_args[@]+"${poh_args[@]}"}" \
    > "$report" 2> "$log_file" &
  child_pid=$!
  wait "$child_pid"
  rc=$?
  child_pid=
  set -e
  printf '%s recorder_stopped rc=%s output_dir=%s log=%s retry_secs=%s\n' \
    "$(ts)" "$rc" "$OUTPUT_DIR" "$log_file" "$RESTART_DELAY_SECS"
  sleep "$RESTART_DELAY_SECS"
done
