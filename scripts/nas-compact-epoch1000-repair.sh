#!/usr/bin/env bash
set -euo pipefail

# Resumable, low-priority epoch-1000 repair materialization, degraded hot
# compaction, and block-access build. Nothing in the live repair view is
# removed by this runner.

BIN=${BLOCKZILLA_REPAIR_BIN:?set BLOCKZILLA_REPAIR_BIN to the deployed repair binary}
REPAIR_VIEW=${BLOCKZILLA_REPAIR_VIEW:-/volume1/@home/ach/dev/blockzilla-live/epoch-1000-union-repair-view-20260713}
STATE_ROOT=${BLOCKZILLA_STATE_ROOT:-/volume1/@home/ach/dev/blockzilla-pipeline/state/nas-pipeline-v2}
MATERIALIZED=${BLOCKZILLA_REPAIR_MATERIALIZED:-$STATE_ROOT/live-repair-materialized/epoch-1000}
HOT_OUTPUT=${BLOCKZILLA_REPAIR_HOT_OUTPUT:-/volume1/@home/ach/dev/blockzilla-v2/epoch-1000}
RUN_ROOT=${BLOCKZILLA_REPAIR_RUN_ROOT:-$STATE_ROOT/live-repair-runs/epoch-1000}

IO_PAUSE_FULL_AVG10=${BLOCKZILLA_REPAIR_IO_PAUSE_FULL_AVG10:-50}
IO_RESUME_FULL_AVG10=${BLOCKZILLA_REPAIR_IO_RESUME_FULL_AVG10:-25}
MEMORY_PAUSE_MIB=${BLOCKZILLA_REPAIR_MEMORY_PAUSE_MIB:-1024}
MEMORY_RESUME_MIB=${BLOCKZILLA_REPAIR_MEMORY_RESUME_MIB:-1536}
HIGH_SAMPLES_TO_PAUSE=${BLOCKZILLA_REPAIR_HIGH_SAMPLES_TO_PAUSE:-3}
LOW_SAMPLES_TO_RESUME=${BLOCKZILLA_REPAIR_LOW_SAMPLES_TO_RESUME:-6}
SAMPLE_SECS=${BLOCKZILLA_REPAIR_SAMPLE_SECS:-10}

mkdir -p "$RUN_ROOT" "$(dirname "$MATERIALIZED")" "$(dirname "$HOT_OUTPUT")"
exec 9>"$RUN_ROOT/runner.lock"
if ! flock -n 9; then
  printf 'epoch-1000 repair runner is already active\n' >&2
  exit 75
fi
exec >>"$RUN_ROOT/runner.log" 2>&1

log() {
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$*"
}

io_full_avg10() {
  awk '/^full / {for (i = 1; i <= NF; i++) if ($i ~ /^avg10=/) {sub(/^avg10=/, "", $i); print $i; exit}}' /proc/pressure/io
}

memory_available_mib() {
  awk '/^MemAvailable:/ {printf "%d\n", $2 / 1024; exit}' /proc/meminfo
}

float_ge() {
  awk -v value="$1" -v threshold="$2" 'BEGIN {exit !(value >= threshold)}'
}

float_le() {
  awk -v value="$1" -v threshold="$2" 'BEGIN {exit !(value <= threshold)}'
}

CHILD_PID=
CHILD_PAUSED=0

forward_term() {
  if [[ -n "${CHILD_PID:-}" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
    kill -CONT "$CHILD_PID" 2>/dev/null || true
    kill -TERM "$CHILD_PID" 2>/dev/null || true
  fi
}
trap forward_term INT TERM EXIT

run_guarded() {
  local label=$1
  shift
  local high_samples=0 low_samples=0 io_full available exit_status

  log "phase_start label=$label"
  /usr/bin/ionice -c 2 -n 6 /usr/bin/nice -n 10 \
    env MALLOC_ARENA_MAX=2 RAYON_NUM_THREADS=2 "$@" &
  CHILD_PID=$!
  CHILD_PAUSED=0
  printf '%s\n' "$CHILD_PID" >"$RUN_ROOT/$label.pid"

  while kill -0 "$CHILD_PID" 2>/dev/null; do
    io_full=$(io_full_avg10)
    available=$(memory_available_mib)
    if [[ "$CHILD_PAUSED" == 0 ]]; then
      if float_ge "$io_full" "$IO_PAUSE_FULL_AVG10" || (( available <= MEMORY_PAUSE_MIB )); then
        high_samples=$((high_samples + 1))
      else
        high_samples=0
      fi
      if (( high_samples >= HIGH_SAMPLES_TO_PAUSE )); then
        kill -STOP "$CHILD_PID"
        CHILD_PAUSED=1
        low_samples=0
        log "phase_paused label=$label pid=$CHILD_PID io_full_avg10=$io_full memory_available_mib=$available"
      fi
    else
      if float_le "$io_full" "$IO_RESUME_FULL_AVG10" && (( available >= MEMORY_RESUME_MIB )); then
        low_samples=$((low_samples + 1))
      else
        low_samples=0
      fi
      if (( low_samples >= LOW_SAMPLES_TO_RESUME )); then
        kill -CONT "$CHILD_PID"
        CHILD_PAUSED=0
        high_samples=0
        log "phase_resumed label=$label pid=$CHILD_PID io_full_avg10=$io_full memory_available_mib=$available"
      fi
    fi
    sleep "$SAMPLE_SECS"
  done

  set +e
  wait "$CHILD_PID"
  exit_status=$?
  set -e
  CHILD_PID=
  CHILD_PAUSED=0
  rm -f "$RUN_ROOT/$label.pid"
  if (( exit_status != 0 )); then
    log "phase_failed label=$label exit_status=$exit_status"
    return "$exit_status"
  fi
  log "phase_complete label=$label"
}

main() {
  [[ -x "$BIN" ]]
  [[ -f "$REPAIR_VIEW/REPAIR-REQUIRED.json" ]]

  run_guarded materialize \
    "$BIN" materialize-archive-v2-live-repair \
    "$REPAIR_VIEW" "$MATERIALIZED" \
    --max-rpc-json-mib 32 \
    --checkpoint-every 256 \
    --pubkey-run-max-keys 250000

  local hot_stage hot_progress
  hot_stage="$(dirname "$HOT_OUTPUT")/.$(basename "$HOT_OUTPUT").repair-hot-stage"
  hot_progress="$hot_stage/repair/hot-progress.json"
  run_guarded compact \
    env BLOCKZILLA_PROGRESS_FILE="$hot_progress" \
    "$BIN" build-archive-v2-degraded-hot-blocks-from-repair \
    "$MATERIALIZED" "$HOT_OUTPUT" --level 1

  run_guarded block-access \
    "$BIN" build-archive-v2-repair-block-access \
    "$REPAIR_VIEW" "$HOT_OUTPUT"

  [[ -f "$HOT_OUTPUT/REPAIR-COMPACTED.json" ]]
  [[ -f "$HOT_OUTPUT/archive-v2-block-access.wincode" ]]
  [[ -f "$HOT_OUTPUT/archive-v2-block-access.index" ]]
  [[ -f "$HOT_OUTPUT/archive-v2-get-block.index" ]]
  [[ -f "$HOT_OUTPUT/prev_blockhash_tail.bin" ]]
  [[ ! -e "$HOT_OUTPUT/READY" ]]
  [[ ! -e "$HOT_OUTPUT/poh.wincode" ]]
  [[ ! -e "$HOT_OUTPUT/shredding.wincode" ]]
  jq -e '
    .version == 1 and
    .state == "degraded_hot_archive_missing_poh_and_shredding" and
    .canonical == false and
    .publication_ready == false and
    .block_archive_ready == true and
    .block_access_ready == true and
    .files.block_access == "archive-v2-block-access.wincode" and
    .files.block_access_index == "archive-v2-block-access.index" and
    .files.get_block_index == "archive-v2-get-block.index" and
    .files.previous_blockhash_tail == "prev_blockhash_tail.bin" and
    .poh_coverage.missing_records > 0 and
    .poh_coverage.record_ids_have_explicit_gaps == true and
    .shredding_coverage.missing_records == .produced_blocks and
    .shredding_coverage.canonical_sidecar_emitted == false
  ' "$HOT_OUTPUT/REPAIR-COMPACTED.json" >/dev/null
  log "repair_archive_and_block_access_complete output=$HOT_OUTPUT"
}

main "$@"
