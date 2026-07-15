#!/usr/bin/env bash
set -euo pipefail

# Capture Blockzilla live epochs from a Yellowstone gRPC feed.
#
# Required environment:
#   BLOCKZILLA_GRPC_ENDPOINT   Yellowstone gRPC endpoint, e.g. https://...
#   BLOCKZILLA_GRPC_X_TOKEN    x-token metadata value for the endpoint
#
# Optional environment:
#   BLOCKZILLA_STARTUP_RPC_URL JSON-RPC endpoint for startup epoch sync
#   BLOCKZILLA_LIVE_ROOT       output root for live captures
#   BLOCKZILLA_REPO            NAS repo path
#   BLOCKZILLA_LIVE_PUBKEY_INDEX_MODE
#                             pubkey index mode; default runs (bounded memory)
#   BLOCKZILLA_LIVE_PUBKEY_HOT_REGISTRY
#                             optional previous epoch registry.bin for hot-cache counts
#   BLOCKZILLA_LIVE_PUBKEY_HOT_COUNT
#                             number of previous-registry keys to keep hot; default 1000
#   BLOCKZILLA_LIVE_FROM_SLOT
#                             optional first slot for a new, empty capture

REPO=${BLOCKZILLA_REPO:-/home/ach/dev/blockzilla-v1-registry-mphf-20260616}
LIVE_ROOT=${BLOCKZILLA_LIVE_ROOT:-/volume1/@home/ach/dev/blockzilla-live}
LOG_ROOT=${BLOCKZILLA_LIVE_LOG_ROOT:-$REPO/logs/live-producer}
BIN=${BLOCKZILLA_LIVE_BIN:-$REPO/target/release/blockzilla-live-producer}
SLOTS_PER_EPOCH=${BLOCKZILLA_SLOTS_PER_EPOCH:-432000}
MAX_BLOCKS=${BLOCKZILLA_LIVE_MAX_BLOCKS:-1000000}
CAPTURE_TIMEOUT_SECS=${BLOCKZILLA_LIVE_CAPTURE_TIMEOUT_SECS:-86400}
RESTART_DELAY_SECS=${BLOCKZILLA_LIVE_RESTART_DELAY_SECS:-30}
REPLAY_UNAVAILABLE_MARGIN_SLOTS=${BLOCKZILLA_LIVE_REPLAY_UNAVAILABLE_MARGIN_SLOTS:-1000}
RAW_BLOCK_STORAGE=${BLOCKZILLA_LIVE_RAW_BLOCK_STORAGE:-failure}
PUBKEY_INDEX_MODE=${BLOCKZILLA_LIVE_PUBKEY_INDEX_MODE:-runs}
PUBKEY_HOT_REGISTRY=${BLOCKZILLA_LIVE_PUBKEY_HOT_REGISTRY:-}
PUBKEY_HOT_COUNT=${BLOCKZILLA_LIVE_PUBKEY_HOT_COUNT:-1000}
INITIAL_FROM_SLOT=${BLOCKZILLA_LIVE_FROM_SLOT:-}
INITIAL_CAPTURE_DIR=${BLOCKZILLA_LIVE_CAPTURE_DIR:-}
AUTO_PACKAGE_CLOSED_EPOCH=${BLOCKZILLA_LIVE_AUTO_PACKAGE_CLOSED_EPOCH:-0}
FINALIZE_HANDOFF_DELAY_SECS=${BLOCKZILLA_LIVE_FINALIZE_HANDOFF_DELAY_SECS:-10}

mkdir -p "$LIVE_ROOT" "$LOG_ROOT"

if [[ -f "$REPO/personal/live-producer.env" ]]; then
  # shellcheck source=/dev/null
  source "$REPO/personal/live-producer.env"
fi
if [[ -f "$REPO/personal/rpc-providers.env" ]]; then
  # shellcheck source=/dev/null
  source "$REPO/personal/rpc-providers.env"
fi

: "${BLOCKZILLA_GRPC_ENDPOINT:?set BLOCKZILLA_GRPC_ENDPOINT in personal/live-producer.env}"
: "${BLOCKZILLA_GRPC_X_TOKEN:?set BLOCKZILLA_GRPC_X_TOKEN in personal/live-producer.env}"

if [[ ! -x "$BIN" ]]; then
  echo "missing executable: $BIN" >&2
  exit 2
fi

ts() {
  date -u +%FT%TZ
}

json_field() {
  python3 - "$1" "$2" <<'PY'
import json, sys
path, key = sys.argv[1], sys.argv[2]
with open(path) as f:
    data = json.load(f)
value = data
for part in key.split("."):
    value = value.get(part) if isinstance(value, dict) else None
print("" if value is None else value)
PY
}

log() {
  printf '%s %s\n' "$(ts)" "$*"
}

write_capture_progress() {
  local archive_dir=$1
  local state=$2
  local pid=${3:-}
  python3 - "$archive_dir/progress.json" "$archive_dir/journal/grpc-blocks.jsonl" \
    "$state" "$pid" <<'PY'
import json
import os
import sys
import time

path, journal_path, state, pid_text = sys.argv[1:]
first_slot = None
last_slot = None
blocks_done = 0
try:
    with open(journal_path, "rb") as journal:
        for line in journal:
            try:
                row = json.loads(line)
            except Exception:
                continue
            slot = row.get("slot")
            if not isinstance(slot, int) and isinstance(row.get("block"), dict):
                slot = row["block"].get("slot")
            if isinstance(slot, int):
                first_slot = slot if first_slot is None else min(first_slot, slot)
                last_slot = slot
            block_id = row.get("block_id")
            if isinstance(block_id, int):
                blocks_done = max(blocks_done, block_id + 1)
except FileNotFoundError:
    pass

payload = {
    "schema_version": 1,
    "phase": "capture",
    "state": state,
    "blocks_done": blocks_done,
    "first_slot": first_slot,
    "last_slot": last_slot,
    "updated_unix_secs": int(time.time()),
}
if pid_text:
    payload["pid"] = int(pid_text)
temp = path + ".tmp"
with open(temp, "w") as output:
    json.dump(payload, output, sort_keys=True)
    output.write("\n")
    output.flush()
    os.fsync(output.fileno())
os.replace(temp, path)
PY
}

latest_capture_slot() {
  local archive_dir=$1
  local journal="$archive_dir/journal/grpc-blocks.jsonl"
  if [[ ! -s "$journal" ]]; then
    return 1
  fi
  python3 - "$journal" <<'PY'
import json, sys
last = None
with open(sys.argv[1], "rb") as f:
    for line in f:
        try:
            row = json.loads(line)
        except Exception:
            continue
        slot = row.get("slot")
        if not isinstance(slot, int) and isinstance(row.get("block"), dict):
            slot = row["block"].get("slot")
        if isinstance(slot, int):
            last = slot
if last is None:
    raise SystemExit(1)
print(last)
PY
}

min_resume_slot_file() {
  printf '%s/.capture-min-resume-slot' "$1"
}

read_min_resume_slot() {
  local file
  file=$(min_resume_slot_file "$1")
  if [[ -s "$file" ]]; then
    head -1 "$file"
  fi
}

record_unavailable_gap() {
  local archive_dir=$1
  local wanted_slot=$2
  local available_slot=$3
  local resume_slot=$((available_slot + REPLAY_UNAVAILABLE_MARGIN_SLOTS))
  local gap_file="$archive_dir/live-gap-unavailable.tsv"
  if (( available_slot <= wanted_slot )); then
    return 0
  fi
  if [[ ! -s "$gap_file" ]]; then
    printf 'start_slot\tend_slot\tresume_slot\treason\trecorded_at\n' > "$gap_file"
  fi
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$wanted_slot" "$((resume_slot - 1))" "$resume_slot" \
    "grpc_replay_unavailable_past_valid_range" "$(ts)" >> "$gap_file"
  printf '%s\n' "$resume_slot" > "$(min_resume_slot_file "$archive_dir")"
  log "capture_gap_recorded archive_dir=$archive_dir start_slot=$wanted_slot end_slot=$((resume_slot - 1)) resume_slot=$resume_slot last_available_slot=$available_slot margin_slots=$REPLAY_UNAVAILABLE_MARGIN_SLOTS gap_file=$gap_file"
}

last_available_slot_from_log() {
  local log_file=$1
  python3 - "$log_file" <<'PY'
import re, sys
text = open(sys.argv[1], errors="replace").read()
matches = re.findall(r"last available:\s*([0-9]+)", text)
if not matches:
    raise SystemExit(1)
print(matches[-1])
PY
}

sync_epoch() {
  local rpc_url=${BLOCKZILLA_STARTUP_RPC_URL:-${BLOCKZILLA_TRITON_RPC_URL:-}}
  if [[ -z "$rpc_url" ]]; then
    log "startup_epoch_sync skipped: no BLOCKZILLA_STARTUP_RPC_URL/BLOCKZILLA_TRITON_RPC_URL"
    return 0
  fi
  local out="$LOG_ROOT/startup-epoch-$(date -u +%Y%m%dT%H%M%SZ).json"
  "$BIN" sync-rpc-epoch \
    --rpc-url "$rpc_url" \
    --rpc-rate-limit-per-sec "${BLOCKZILLA_RPC_RATE_LIMIT_PER_SEC:-2}" \
    > "$out"
  log "startup_epoch_sync report=$out epoch=$(json_field "$out" epoch) absolute_slot=$(json_field "$out" absolute_slot)"
}

finalize_capture() {
  local archive_dir=$1
  local inspect_json=$2
  local first_slot last_slot blocks poh epoch package_state
  first_slot=$(json_field "$inspect_json" first_slot)
  last_slot=$(json_field "$inspect_json" last_slot)
  blocks=$(json_field "$inspect_json" block_frames)
  poh=$(json_field "$inspect_json" poh_frames)
  epoch=$((first_slot / SLOTS_PER_EPOCH))
  package_state=repair_gate
  if [[ "$AUTO_PACKAGE_CLOSED_EPOCH" == "1" ]]; then
    printf 'epoch=%s\nverified_at=%s\npolicy=BLOCKZILLA_LIVE_AUTO_PACKAGE_CLOSED_EPOCH\n' \
      "$epoch" "$(ts)" > "$archive_dir/READY-TO-PACKAGE"
    package_state=ready
  fi
  python3 - "$archive_dir/live-package-plan.json" "$archive_dir" "$inspect_json" \
    "$epoch" "$first_slot" "$last_slot" "$blocks" "$poh" "$package_state" "$(ts)" <<'PY'
import json, os, sys

path, archive_dir, inspect_json, epoch, first_slot, last_slot, blocks, poh, state, created_at = sys.argv[1:]
payload = {
    "schema_version": 1,
    "source_kind": "live_capture",
    "state": state,
    "epoch": int(epoch),
    "first_slot": int(first_slot),
    "last_slot": int(last_slot),
    "blocks": int(blocks),
    "poh_records": int(poh),
    "capture_dir": archive_dir,
    "inspect_report": inspect_json,
    "ready_marker": os.path.join(archive_dir, "READY-TO-PACKAGE"),
    "created_at": created_at,
}

temp = path + ".tmp"
with open(temp, "w") as f:
    json.dump(payload, f, sort_keys=True)
    f.write("\n")
    f.flush()
    os.fsync(f.fileno())
os.replace(temp, path)
PY
  log "capture_closed archive_dir=$archive_dir epoch=$epoch first_slot=$first_slot last_slot=$last_slot blocks=$blocks poh_records=$poh package_state=$package_state"
  cat > "$archive_dir/FINALIZE-NEXT.md" <<EOF
# Live Capture Finalization Needed

Captured by \`nas-live-producer-supervisor.sh\` and stopped at an epoch boundary.

- inspect report: \`$inspect_json\`
- block frames: \`$blocks\`
- PoH records: \`$poh\`
- pipeline state: \`$package_state\`
- package plan: \`$archive_dir/live-package-plan.json\`

The current live producer writes append-friendly block frames plus sidecars:
\`blocks/live-no-registry-blocks.bin\`, \`poh/poh.wincode\`, and \`index/*\`.

Before treating this as final compressed Blockzilla archive output, add or run
a finalizer that converts the live capture into the canonical hot-block archive
layout and builds the registry-backed compact files.

The compaction pipeline will display this capture in \`repair_gate\` until
\`$archive_dir/READY-TO-PACKAGE\` exists. Set
\`BLOCKZILLA_LIVE_AUTO_PACKAGE_CLOSED_EPOCH=1\` only when the capture policy
already guarantees all required repairs and sidecars.
EOF
}

finalize_closed_capture() {
  local archive_dir=$1
  local inspect_json=$2
  local rc

  # Inspection can take minutes (or block in kernel I/O). It must never sit in
  # the gRPC ingest handoff between two epochs. The capture is closed and
  # immutable at this point, so it is safe to inspect in a background worker
  # while the supervisor immediately opens the next epoch subscription.
  sleep "$FINALIZE_HANDOFF_DELAY_SECS"
  set +e
  "$BIN" inspect-capture --archive-dir "$archive_dir" > "$inspect_json"
  rc=$?
  set -e
  if [[ "$rc" -ne 0 ]]; then
    log "capture_inspect_failed rc=$rc archive_dir=$archive_dir inspect=$inspect_json"
    return "$rc"
  fi

  finalize_capture "$archive_dir" "$inspect_json"
}

main() {
  sync_epoch || true

  local next_from_slot=${INITIAL_FROM_SLOT:-}
  local first_capture_dir=$INITIAL_CAPTURE_DIR

  while true; do
    local stamp archive_dir inspect_json capture_epoch
    stamp=$(date -u +%Y%m%dT%H%M%SZ)
    if [[ -n "$first_capture_dir" ]]; then
      archive_dir=$first_capture_dir
      first_capture_dir=
    elif [[ -n "$next_from_slot" ]]; then
      capture_epoch=$((next_from_slot / SLOTS_PER_EPOCH))
      archive_dir=$LIVE_ROOT/epoch-$capture_epoch-capture-$stamp
    else
      archive_dir=$LIVE_ROOT/capture-$stamp
    fi
    inspect_json="$LOG_ROOT/$(basename "$archive_dir")-inspect.json"

    mkdir -p "$archive_dir"

    local attempt=0
    while true; do
      local attempt_stamp log_file report_json from_slot last_slot
      attempt=$((attempt + 1))
      attempt_stamp=$(date -u +%Y%m%dT%H%M%SZ)
      log_file="$LOG_ROOT/$(basename "$archive_dir")-attempt-$attempt_stamp.log"
      report_json="$LOG_ROOT/$(basename "$archive_dir")-attempt-$attempt_stamp-report.json"

      from_slot=()
      if last_slot=$(latest_capture_slot "$archive_dir" 2>/dev/null); then
        local wanted_slot resume_floor resume_slot
        wanted_slot=$((last_slot + 1))
        resume_slot=$wanted_slot
        if resume_floor=$(read_min_resume_slot "$archive_dir" 2>/dev/null); then
          if [[ "$resume_floor" =~ ^[0-9]+$ ]] && (( resume_floor > resume_slot )); then
            resume_slot=$resume_floor
          fi
        fi
        from_slot=(--from-slot "$resume_slot")
        log "capture_resume archive_dir=$archive_dir last_slot=$last_slot wanted_slot=$wanted_slot from_slot=$resume_slot attempt=$attempt"
      elif [[ -n "$next_from_slot" ]]; then
        local empty_resume_slot recorded_resume_floor
        empty_resume_slot=$next_from_slot
        if recorded_resume_floor=$(read_min_resume_slot "$archive_dir" 2>/dev/null); then
          if [[ "$recorded_resume_floor" =~ ^[0-9]+$ ]] && (( recorded_resume_floor > empty_resume_slot )); then
            empty_resume_slot=$recorded_resume_floor
          fi
        fi
        from_slot=(--from-slot "$empty_resume_slot")
        log "capture_start_from_slot archive_dir=$archive_dir requested_from_slot=$next_from_slot from_slot=$empty_resume_slot endpoint=$BLOCKZILLA_GRPC_ENDPOINT attempt=$attempt"
      else
        log "capture_start archive_dir=$archive_dir endpoint=$BLOCKZILLA_GRPC_ENDPOINT attempt=$attempt"
      fi

      set +e
      pubkey_hot_args=()
      if [[ -n "$PUBKEY_HOT_REGISTRY" ]]; then
        pubkey_hot_args=(--pubkey-hot-registry "$PUBKEY_HOT_REGISTRY" --pubkey-hot-count "$PUBKEY_HOT_COUNT")
      fi
      "$BIN" capture-grpc \
        --endpoint "$BLOCKZILLA_GRPC_ENDPOINT" \
        --archive-dir "$archive_dir" \
        "${from_slot[@]}" \
        --max-blocks "$MAX_BLOCKS" \
        --timeout-secs "$CAPTURE_TIMEOUT_SECS" \
        --slots-per-epoch "$SLOTS_PER_EPOCH" \
        --raw-block-storage "$RAW_BLOCK_STORAGE" \
        --pubkey-index-mode "$PUBKEY_INDEX_MODE" \
        "${pubkey_hot_args[@]+"${pubkey_hot_args[@]}"}" \
        --stop-at-epoch-boundary \
        > "$report_json" 2> "$log_file" &
      local capture_pid=$!
      write_capture_progress "$archive_dir" capturing "$capture_pid"
      wait "$capture_pid"
      local rc=$?
      if [[ "$rc" -eq 0 ]]; then
        write_capture_progress "$archive_dir" closed
      else
        write_capture_progress "$archive_dir" stopped
      fi
      set -e
      if [[ "$rc" -eq 0 ]]; then
        log "capture_completed rc=0 archive_dir=$archive_dir report=$report_json log=$log_file"
        break
      fi

      local available_slot
      if available_slot=$(last_available_slot_from_log "$log_file" 2>/dev/null); then
        if [[ ${#from_slot[@]} -eq 2 ]]; then
          record_unavailable_gap "$archive_dir" "${from_slot[1]}" "$available_slot"
        fi
      fi
      log "capture_failed_retrying rc=$rc archive_dir=$archive_dir report=$report_json log=$log_file delay_secs=$RESTART_DELAY_SECS"
      sleep "$RESTART_DELAY_SECS"
    done

    # Use the capture report to compute the resume slot before starting any
    # potentially slow inspection. This keeps the reconnect gap bounded to the
    # process handoff instead of the full inspection/finalization duration.
    last_slot=$(json_field "$report_json" last_slot)
    if [[ ! "$last_slot" =~ ^[0-9]+$ ]]; then
      log "capture_report_missing_last_slot archive_dir=$archive_dir report=$report_json"
      return 1
    fi
    next_from_slot=$((last_slot + 1))

    local finalize_log finalize_pid
    finalize_log="$LOG_ROOT/$(basename "$archive_dir")-finalize-$(date -u +%Y%m%dT%H%M%SZ).log"
    finalize_closed_capture "$archive_dir" "$inspect_json" > "$finalize_log" 2>&1 &
    finalize_pid=$!
    log "capture_finalize_started pid=$finalize_pid archive_dir=$archive_dir inspect=$inspect_json log=$finalize_log next_from_slot=$next_from_slot"

    if [[ "${BLOCKZILLA_LIVE_ONCE:-0}" == "1" ]]; then
      log "live_once=true stopping_after_one_capture"
      break
    fi
  done
}

main "$@"
