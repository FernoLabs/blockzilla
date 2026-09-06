#!/bin/sh
set -eu

# Continuously copy only the Blockzilla receiver's locally fsynced prefix into
# a separate standard raw-gRPC generation. The canonical receiver tree is
# mounted read-only and this process has no cleanup path.

umask 077
export LC_ALL=C

SOURCE_ROOT=${BLOCKZILLA_RECEIVER_BRIDGE_SOURCE_ROOT:-/receiver-data/blockzilla-ingest-primary/replication-receiver}
HIVEZILLA_BIN=${HIVEZILLA_BIN:-/usr/local/bin/hivezilla}
OUTPUT_DIR=${BLOCKZILLA_RECEIVER_BRIDGE_OUTPUT_DIR:-/derived/raw-current}
CLUSTER_ID=${BLOCKZILLA_RECEIVER_BRIDGE_CLUSTER_ID:-solana-mainnet}
ORIGIN_NODE_ID=${BLOCKZILLA_RECEIVER_BRIDGE_ORIGIN_NODE_ID:-source-node-01}
SOURCE_ID=${BLOCKZILLA_RECEIVER_BRIDGE_SOURCE_ID:-grpc-raw-source-backup}
JOURNAL_ID=${BLOCKZILLA_RECEIVER_BRIDGE_JOURNAL_ID:-}
MAX_RECORDS=${BLOCKZILLA_RECEIVER_BRIDGE_MAX_RECORDS:-128}
MAX_UNCOMPRESSED_RECORD_BYTES=${BLOCKZILLA_RECEIVER_BRIDGE_MAX_UNCOMPRESSED_RECORD_BYTES:-134217728}
MAX_OUTPUT_BYTES=${BLOCKZILLA_RECEIVER_BRIDGE_MAX_OUTPUT_BYTES:-8796093022208}
MIN_FREE_BYTES=${BLOCKZILLA_RECEIVER_BRIDGE_MIN_FREE_BYTES:-6597069766656}
POLL_INTERVAL_SECS=${BLOCKZILLA_RECEIVER_BRIDGE_POLL_INTERVAL_SECS:-2}
FAILURE_RETRY_SECS=${BLOCKZILLA_RECEIVER_BRIDGE_FAILURE_RETRY_SECS:-60}
ERROR_DIRECTORY=${BLOCKZILLA_RECEIVER_BRIDGE_ERROR_DIRECTORY:-/tmp}

die() {
  printf '%s\n' "receiver-bridge: $*" >&2
  exit 1
}

require_stable_id() {
  label=$1
  value=$2
  case "$value" in
    ''|[!A-Za-z0-9]*|*[!A-Za-z0-9_.-]*) die "$label is invalid" ;;
  esac
  [ "${#value}" -le 64 ] || die "$label is too long"
}

[ "$(id -u)" = 10001 ] || die "must run as dedicated UID 10001"
require_stable_id "cluster id" "$CLUSTER_ID"
require_stable_id "origin node id" "$ORIGIN_NODE_ID"
require_stable_id "source id" "$SOURCE_ID"
case "$JOURNAL_ID" in
  *[!0-9A-Fa-f]*|'') die "journal id must be exactly 32 hexadecimal characters" ;;
esac
[ "${#JOURNAL_ID}" -eq 32 ] || die "journal id must be exactly 32 hexadecimal characters"
case "$MAX_RECORDS" in ''|*[!0-9]*) die "max records must be a positive integer" ;; esac
case "$MAX_UNCOMPRESSED_RECORD_BYTES" in ''|*[!0-9]*) die "max uncompressed record bytes must be a positive integer" ;; esac
case "$MAX_OUTPUT_BYTES" in ''|*[!0-9]*) die "max output bytes must be a positive integer" ;; esac
case "$MIN_FREE_BYTES" in ''|*[!0-9]*) die "min free bytes must be a non-negative integer" ;; esac
case "$POLL_INTERVAL_SECS" in ''|*[!0-9]*) die "poll interval must be a positive integer" ;; esac
case "$FAILURE_RETRY_SECS" in ''|*[!0-9]*) die "failure retry must be a positive integer" ;; esac
[ "$MAX_RECORDS" -gt 0 ] && [ "$MAX_RECORDS" -le 1000000 ] || \
  die "max records must be between 1 and 1000000"
[ "$MAX_UNCOMPRESSED_RECORD_BYTES" -gt 0 ] || die "max uncompressed record bytes must be greater than zero"
[ "$MAX_OUTPUT_BYTES" -gt 0 ] || die "max output bytes must be greater than zero"
[ "$POLL_INTERVAL_SECS" -gt 0 ] || die "poll interval must be greater than zero"
[ "$FAILURE_RETRY_SECS" -gt 0 ] || die "failure retry must be greater than zero"
[ -d "$SOURCE_ROOT" ] && [ ! -L "$SOURCE_ROOT" ] && [ -r "$SOURCE_ROOT" ] || \
  die "receiver source root must be a readable non-symlink directory"
output_parent=${OUTPUT_DIR%/*}
if [ "$output_parent" = "$OUTPUT_DIR" ]; then
  output_parent=.
fi
[ -d "$output_parent" ] && [ ! -L "$output_parent" ] && [ -w "$output_parent" ] || \
  die "receiver bridge output parent must be a writable non-symlink directory"
[ -d "$ERROR_DIRECTORY" ] && [ ! -L "$ERROR_DIRECTORY" ] && [ -w "$ERROR_DIRECTORY" ] || \
  die "receiver bridge error directory must be a writable non-symlink directory"

stop_requested=false
trap 'stop_requested=true' TERM INT HUP
current_error=$ERROR_DIRECTORY/receiver-bridge-current-error.$$
last_error=$ERROR_DIRECTORY/receiver-bridge-last-error.$$
trap 'rm -f "$current_error" "$last_error"' EXIT

while [ "$stop_requested" = false ]; do
  if "$HIVEZILLA_BIN" bridge-receiver-grpc-raw \
    --receiver-spool-root "$SOURCE_ROOT" \
    --output-dir "$OUTPUT_DIR" \
    --cluster-id "$CLUSTER_ID" \
    --origin-node-id "$ORIGIN_NODE_ID" \
    --source-id "$SOURCE_ID" \
    --journal-id "$JOURNAL_ID" \
    --max-records "$MAX_RECORDS" \
    --max-uncompressed-record-bytes "$MAX_UNCOMPRESSED_RECORD_BYTES" \
    --max-output-bytes "$MAX_OUTPUT_BYTES" \
    --min-free-bytes "$MIN_FREE_BYTES" \
    >/dev/null 2>"$current_error"
  then
    rm -f "$current_error" "$last_error"
    [ "$stop_requested" = true ] || sleep "$POLL_INTERVAL_SECS"
  else
    if [ ! -s "$current_error" ]; then
      printf '%s\n' "receiver bridge bounded pass failed" >"$current_error"
    fi
    if [ ! -f "$last_error" ] || ! cmp -s "$current_error" "$last_error"; then
      cat "$current_error" >&2
      printf '%s\n' "receiver-bridge: pass failed; source remains read-only; retrying after backoff" >&2
      cp "$current_error" "$last_error"
    fi
    [ "$stop_requested" = true ] || sleep "$FAILURE_RETRY_SECS"
  fi
done
