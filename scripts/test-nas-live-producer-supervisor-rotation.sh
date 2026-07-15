#!/usr/bin/env bash
set -euo pipefail

if [[ "${BLOCKZILLA_SUPERVISOR_FAKE_BIN:-0}" == "1" ]]; then
  command_name=${1:?missing fake command}
  shift
  case "$command_name" in
    capture-grpc)
      archive_dir=
      while (($#)); do
        case "$1" in
          --archive-dir)
            archive_dir=$2
            shift 2
            ;;
          *) shift ;;
        esac
      done
      mkdir -p "$archive_dir/journal" "$archive_dir/blocks" "$archive_dir/poh" "$archive_dir/index"
      count=0
      if [[ -s "$BLOCKZILLA_SUPERVISOR_TEST_ROOT/capture-count" ]]; then
        count=$(<"$BLOCKZILLA_SUPERVISOR_TEST_ROOT/capture-count")
      fi
      count=$((count + 1))
      printf '%s\n' "$count" > "$BLOCKZILLA_SUPERVISOR_TEST_ROOT/capture-count"
      if [[ "$count" -eq 1 ]]; then
        printf '{"slot":9,"block_id":0}\n' > "$archive_dir/journal/grpc-blocks.jsonl"
        printf '{"first_slot":9,"last_slot":9,"blocks_written":1,"stopped_at_epoch_boundary":true}\n'
        exit 0
      fi
      date +%s%N > "$BLOCKZILLA_SUPERVISOR_TEST_ROOT/second-capture-started"
      sleep 30
      ;;
    inspect-capture)
      date +%s%N > "$BLOCKZILLA_SUPERVISOR_TEST_ROOT/inspect-started"
      sleep 3
      printf '{"first_slot":9,"last_slot":9,"block_frames":1,"poh_frames":1}\n'
      date +%s%N > "$BLOCKZILLA_SUPERVISOR_TEST_ROOT/inspect-finished"
      ;;
    *)
      printf 'unexpected fake command: %s\n' "$command_name" >&2
      exit 2
      ;;
  esac
  exit 0
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
test_root=$(mktemp -d)
supervisor_pid=

cleanup() {
  if [[ -n "$supervisor_pid" ]]; then
    pkill -TERM -P "$supervisor_pid" 2>/dev/null || true
    kill -TERM "$supervisor_pid" 2>/dev/null || true
    wait "$supervisor_pid" 2>/dev/null || true
  fi
  rm -rf "$test_root"
}
trap cleanup EXIT

mkdir -p "$test_root/live" "$test_root/logs"
BLOCKZILLA_SUPERVISOR_FAKE_BIN=1 \
BLOCKZILLA_SUPERVISOR_TEST_ROOT="$test_root" \
BLOCKZILLA_REPO="$test_root" \
BLOCKZILLA_LIVE_ROOT="$test_root/live" \
BLOCKZILLA_LIVE_LOG_ROOT="$test_root/logs" \
BLOCKZILLA_LIVE_BIN="$script_dir/test-nas-live-producer-supervisor-rotation.sh" \
BLOCKZILLA_LIVE_FROM_SLOT=9 \
BLOCKZILLA_SLOTS_PER_EPOCH=10 \
BLOCKZILLA_GRPC_ENDPOINT=https://example.invalid \
BLOCKZILLA_GRPC_X_TOKEN=test-only \
  "$script_dir/nas-live-producer-supervisor.sh" > "$test_root/supervisor.log" 2>&1 &
supervisor_pid=$!

deadline=$((SECONDS + 5))
while [[ ! -f "$test_root/second-capture-started" ]] && ((SECONDS < deadline)); do
  sleep 0.05
done

if [[ ! -f "$test_root/second-capture-started" ]]; then
  cat "$test_root/supervisor.log" >&2
  echo "next capture did not start" >&2
  exit 1
fi
if [[ -f "$test_root/inspect-started" ]]; then
  cat "$test_root/supervisor.log" >&2
  echo "inspection started before the next capture" >&2
  exit 1
fi
grep -q 'capture_finalize_started' "$test_root/supervisor.log"
echo "supervisor opened the next capture without waiting for inspection"
