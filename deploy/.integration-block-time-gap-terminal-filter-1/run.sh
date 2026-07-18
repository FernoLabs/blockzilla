#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

BIN=${BLOCKZILLA_BIN:-/volume1/@home/ach/dev/blockzilla-pipeline/releases/blockzilla-gap-archive-v2-2026.07.18-1/bin/blockzilla}
STATE_DIR=${STATE_DIR:-/volume1/@home/ach/dev/blockzilla-pipeline/state/block-time-gap-archive-v2-v1}
MANIFEST=${MANIFEST:-$STATE_DIR/run-manifest.tsv}
PIPELINE_STATUS=${PIPELINE_STATUS:-/volume1/@home/ach/dev/blockzilla-pipeline/state/nas-pipeline-v2/status.json}
ARCHIVE_ROOT=${ARCHIVE_ROOT:-/volume1/@home/ach/dev/blockzilla-v2}
IO_RESUME_FULL_AVG10=${IO_RESUME_FULL_AVG10:-10.0}
IO_PAUSE_FULL_AVG10=${IO_PAUSE_FULL_AVG10:-80.0}
LIVE_CAPTURE_MAX_STALE_SECONDS=${LIVE_CAPTURE_MAX_STALE_SECONDS:-60}
# Keep the backfill below live capture at the OS scheduler level. The workflow-level
# priority lease removes historical competitors; it does not promote this process.
CHILD_NICE=${CHILD_NICE:-19}
CHILD_IO_PRIORITY=${CHILD_IO_PRIORITY:-7}

mkdir -p "$STATE_DIR/logs" "$STATE_DIR/progress" "$STATE_DIR/receipts"
exec 9>"$STATE_DIR/runner.lock"
if ! flock -n 9; then
  echo "block-time gap archive backfill is already running" >&2
  exit 75
fi

test -x "$BIN"
test -s "$MANIFEST"
test -s "$PIPELINE_STATUS"
awk -v resume="$IO_RESUME_FULL_AVG10" -v pause="$IO_PAUSE_FULL_AVG10" \
  'BEGIN {exit !(resume >= 0 && resume < pause && pause <= 100)}'
case "$CHILD_NICE" in
  ''|*[!0-9]*) echo "CHILD_NICE must be an integer from 0 to 19" >&2; exit 64 ;;
esac
case "$CHILD_IO_PRIORITY" in
  ''|*[!0-9]*) echo "CHILD_IO_PRIORITY must be an integer from 0 to 7" >&2; exit 64 ;;
esac
test "$CHILD_NICE" -le 19
test "$CHILD_IO_PRIORITY" -le 7
case "$LIVE_CAPTURE_MAX_STALE_SECONDS" in
  ''|*[!0-9]*) echo "LIVE_CAPTURE_MAX_STALE_SECONDS must be a positive integer" >&2; exit 64 ;;
esac
test "$LIVE_CAPTURE_MAX_STALE_SECONDS" -gt 0

RUN_STARTED_UNIX=$(date +%s)
BACKFILL_STARTED_UNIX=$RUN_STARTED_UNIX
CARRY_ELAPSED_SECONDS=0
PAUSED_SECONDS=0
if test -s "$STATE_DIR/status.json" && jq -e '.schema_version == 1' "$STATE_DIR/status.json" >/dev/null 2>&1; then
  previous_started=$(jq -r '.started_unix_seconds // 0' "$STATE_DIR/status.json")
  previous_elapsed=$(jq -r '.backfill.wall_elapsed_seconds // empty' "$STATE_DIR/status.json")
  previous_paused=$(jq -r '.resources.paused_seconds // 0' "$STATE_DIR/status.json")
  if test "$previous_started" -gt 0 && test "$previous_started" -le "$RUN_STARTED_UNIX"; then
    BACKFILL_STARTED_UNIX=$previous_started
    if test -n "$previous_elapsed"; then
      CARRY_ELAPSED_SECONDS=$previous_elapsed
    else
      CARRY_ELAPSED_SECONDS=$((RUN_STARTED_UNIX - previous_started))
    fi
  fi
  PAUSED_SECONDS=$previous_paused
fi
TOTAL_EPOCHS=$(wc -l < "$MANIFEST" | tr -d ' ')
TOTAL_BYTES=$(awk -F '\t' '{sum += $3} END {printf "%.0f", sum}' "$MANIFEST")
DONE_EPOCHS=0
DONE_BYTES=0
MEASURED_BYTES=0
MEASURED_SECONDS=0
CURRENT_EPOCH=
CURRENT_PATH=
CURRENT_SOURCE_BYTES=0
CURRENT_PID=
CURRENT_PROGRESS=
LAST_ERROR=
LIVE_CAPTURE_PID=
LIVE_CAPTURE_LAST_WRITE_BYTES=0
LIVE_CAPTURE_LAST_ADVANCE_UNIX=$(date +%s)
LIVE_CAPTURE_AGE_SECONDS=0
LIVE_CAPTURE_HEALTHY=false

for receipt in "$STATE_DIR"/receipts/epoch-*.json; do
  test -e "$receipt" || continue
  DONE_EPOCHS=$((DONE_EPOCHS + 1))
  receipt_bytes=$(jq -r '.source_bytes // 0' "$receipt")
  DONE_BYTES=$((DONE_BYTES + receipt_bytes))
  receipt_seconds=$(jq -r '.duration_seconds // 0' "$receipt")
  if test "$receipt_bytes" -gt 0 && test "$receipt_seconds" -gt 0; then
    MEASURED_BYTES=$((MEASURED_BYTES + receipt_bytes))
    MEASURED_SECONDS=$((MEASURED_SECONDS + receipt_seconds))
  fi
done

live_capture_values() {
  local candidate_pid candidate_cmd current_pid current_write now
  current_pid=
  while read -r candidate_pid; do
    test -n "$candidate_pid" || continue
    test -r "/proc/$candidate_pid/cmdline" || continue
    candidate_cmd=$(tr '\0' ' ' < "/proc/$candidate_pid/cmdline" 2>/dev/null || true)
    if [[ "$candidate_cmd " == *"blockzilla-live-producer capture-grpc "* \
      || "$candidate_cmd " == *"blockzilla-live-producer record-grpc-raw "* ]]; then
      current_pid=$candidate_pid
      break
    fi
  done < <(pgrep -f blockzilla-live-producer 2>/dev/null || true)

  now=$(date +%s)
  if test -z "$current_pid" || test ! -r "/proc/$current_pid/io"; then
    LIVE_CAPTURE_PID=
    LIVE_CAPTURE_AGE_SECONDS=$((now - LIVE_CAPTURE_LAST_ADVANCE_UNIX))
    LIVE_CAPTURE_HEALTHY=false
    return
  fi
  current_write=$(awk '/^write_bytes:/ {print $2}' "/proc/$current_pid/io" 2>/dev/null || echo 0)
  if test "$current_pid" != "$LIVE_CAPTURE_PID" || test "$current_write" -gt "$LIVE_CAPTURE_LAST_WRITE_BYTES"; then
    LIVE_CAPTURE_LAST_ADVANCE_UNIX=$now
  fi
  LIVE_CAPTURE_PID=$current_pid
  LIVE_CAPTURE_LAST_WRITE_BYTES=$current_write
  LIVE_CAPTURE_AGE_SECONDS=$((now - LIVE_CAPTURE_LAST_ADVANCE_UNIX))
  if test "$LIVE_CAPTURE_AGE_SECONDS" -le "$LIVE_CAPTURE_MAX_STALE_SECONDS"; then
    LIVE_CAPTURE_HEALTHY=true
  else
    LIVE_CAPTURE_HEALTHY=false
  fi
}

resource_values() {
  MEM_AVAILABLE_KIB=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)
  IO_FULL_AVG10=$(awk '/^full / {for (i=1;i<=NF;i++) if ($i ~ /^avg10=/) {split($i,a,"="); print a[2]}}' /proc/pressure/io)
  LOAD1=$(awk '{print $1}' /proc/loadavg)
  FINALIZER_ACTIVE=false
  while read -r candidate_pid; do
    test -n "$candidate_pid" || continue
    candidate_cmd=$(tr '\0' ' ' < "/proc/$candidate_pid/cmdline" 2>/dev/null || true)
    if [[ " $candidate_cmd " == *" finalize-archive-v2-first-seen "* ]]; then
      FINALIZER_ACTIVE=true
      break
    fi
  done < <(pgrep -x blockzilla 2>/dev/null || true)
  live_capture_values
}

admission_healthy() {
  resource_values
  awk -v mem="$MEM_AVAILABLE_KIB" -v io="$IO_FULL_AVG10" -v load="$LOAD1" \
    -v io_resume="$IO_RESUME_FULL_AVG10" \
    'BEGIN {exit !(mem >= 3145728 && io <= io_resume && load <= 8.0)}' \
    && test "$FINALIZER_ACTIVE" = false \
    && test "$LIVE_CAPTURE_HEALTHY" = true
}

critical_pressure() {
  resource_values
  awk -v mem="$MEM_AVAILABLE_KIB" -v io="$IO_FULL_AVG10" -v load="$LOAD1" \
    -v io_pause="$IO_PAUSE_FULL_AVG10" \
    'BEGIN {exit !(mem < 2621440 || io >= io_pause || load >= 10.0)}' \
    || test "$FINALIZER_ACTIVE" = true \
    || test "$LIVE_CAPTURE_HEALTHY" != true
}

publish_status() {
  local state=$1
  local now elapsed wall_elapsed current_done overall_done rate eta progress_state
  now=$(date +%s)
  elapsed=$((now - RUN_STARTED_UNIX))
  wall_elapsed=$((CARRY_ELAPSED_SECONDS + elapsed))
  current_done=0
  progress_state=null
  if test -n "$CURRENT_PROGRESS" && test -s "$CURRENT_PROGRESS"; then
    current_done=$(jq -r '.source_bytes_done // 0' "$CURRENT_PROGRESS" 2>/dev/null || echo 0)
    progress_state=$(jq -r '.state // "unknown"' "$CURRENT_PROGRESS" 2>/dev/null || echo unknown)
  fi
  overall_done=$((DONE_BYTES + current_done))
  # Existing sidecars must not be treated as bytes generated instantly at runner start.
  # Use only completed scans with recorded durations for the throughput and ETA.
  rate=$(awk -v bytes="$MEASURED_BYTES" -v seconds="$MEASURED_SECONDS" 'BEGIN {if (seconds > 0) printf "%.3f", bytes/seconds; else print 0}')
  eta=$(awk -v done="$overall_done" -v total="$TOTAL_BYTES" -v bytes="$MEASURED_BYTES" -v seconds="$MEASURED_SECONDS" 'BEGIN {if (bytes > 0 && seconds > 0 && total >= done) printf "%.0f", (total-done)*seconds/bytes; else print -1}')
  resource_values
  jq -n \
    --arg state "$state" \
    --argjson started "$BACKFILL_STARTED_UNIX" \
    --argjson runner_started "$RUN_STARTED_UNIX" \
    --argjson updated "$now" \
    --argjson wall_elapsed "$wall_elapsed" \
    --argjson total_epochs "$TOTAL_EPOCHS" \
    --argjson done_epochs "$DONE_EPOCHS" \
    --argjson total_bytes "$TOTAL_BYTES" \
    --argjson done_bytes "$DONE_BYTES" \
    --argjson current_bytes_done "$current_done" \
    --argjson overall_bytes_done "$overall_done" \
    --argjson throughput_bps "$rate" \
    --argjson eta_seconds "$eta" \
    --argjson measured_bytes "$MEASURED_BYTES" \
    --argjson measured_seconds "$MEASURED_SECONDS" \
    --arg current_epoch "$CURRENT_EPOCH" \
    --arg current_path "$CURRENT_PATH" \
    --arg current_pid "$CURRENT_PID" \
    --arg current_progress_state "$progress_state" \
    --argjson mem_available_kib "$MEM_AVAILABLE_KIB" \
    --argjson io_full_avg10 "$IO_FULL_AVG10" \
    --argjson load1 "$LOAD1" \
    --argjson finalizer_active "$FINALIZER_ACTIVE" \
    --argjson paused_seconds "$PAUSED_SECONDS" \
    --argjson io_resume_full_avg10 "$IO_RESUME_FULL_AVG10" \
    --argjson io_pause_full_avg10 "$IO_PAUSE_FULL_AVG10" \
    --argjson child_nice "$CHILD_NICE" \
    --argjson child_io_priority "$CHILD_IO_PRIORITY" \
    --argjson live_capture_max_stale_seconds "$LIVE_CAPTURE_MAX_STALE_SECONDS" \
    --argjson live_capture_age_seconds "$LIVE_CAPTURE_AGE_SECONDS" \
    --argjson live_capture_healthy "$LIVE_CAPTURE_HEALTHY" \
    --arg live_capture_pid "$LIVE_CAPTURE_PID" \
    --arg last_error "$LAST_ERROR" \
    '{schema_version:1,state:$state,started_unix_seconds:$started,runner_started_unix_seconds:$runner_started,updated_unix_seconds:$updated,backfill:{epochs_done:$done_epochs,epochs_total:$total_epochs,source_bytes_done:$done_bytes,current_source_bytes_done:$current_bytes_done,overall_source_bytes_done:$overall_bytes_done,source_bytes_total:$total_bytes,wall_elapsed_seconds:$wall_elapsed,measured_source_bytes:$measured_bytes,measured_duration_seconds:$measured_seconds,wall_throughput_bytes_per_second:$throughput_bps,eta_seconds:(if $eta_seconds >= 0 then $eta_seconds else null end),eta_reliable:($measured_seconds >= 300)},current:{epoch:(if $current_epoch == "" then null else ($current_epoch|tonumber) end),path:(if $current_path == "" then null else $current_path end),pid:(if $current_pid == "" then null else ($current_pid|tonumber) end),progress_state:(if $current_progress_state == "null" then null else $current_progress_state end)},resources:{mem_available_kib:$mem_available_kib,io_full_avg10:$io_full_avg10,load1:$load1,finalizer_active:$finalizer_active,paused_seconds:$paused_seconds,io_resume_full_avg10:$io_resume_full_avg10,io_pause_full_avg10:$io_pause_full_avg10,child_nice:$child_nice,child_io_priority:$child_io_priority,live_capture_max_stale_seconds:$live_capture_max_stale_seconds,live_capture_age_seconds:$live_capture_age_seconds,live_capture_healthy:$live_capture_healthy,live_capture_pid:(if $live_capture_pid == "" then null else ($live_capture_pid|tonumber) end)},last_error:(if $last_error == "" then null else $last_error end)}' \
    > "$STATE_DIR/.status.json.tmp"
  mv "$STATE_DIR/.status.json.tmp" "$STATE_DIR/status.json"
}

event() {
  local kind=$1
  local message=$2
  jq -cn --argjson at "$(date +%s)" --arg kind "$kind" --arg epoch "$CURRENT_EPOCH" --arg message "$message" \
    '{at_unix_seconds:$at,kind:$kind,epoch:(if $epoch == "" then null else ($epoch|tonumber) end),message:$message}' \
    >> "$STATE_DIR/events.jsonl"
}

verify_child_identity() {
  local pid=$1
  local expected_start=$2
  test -d "/proc/$pid"
  test "$(readlink -f "/proc/$pid/exe")" = "$(readlink -f "$BIN")"
  test "$(awk '{print $22}' "/proc/$pid/stat")" = "$expected_start"
}

terminal_archive_eligible() {
  local epoch=$1 archive_dir=$2 required
  case "$archive_dir" in
    "$ARCHIVE_ROOT/epoch-$epoch") ;;
    *) return 1 ;;
  esac
  for required in \
    "$archive_dir/archive-v2-meta.wincode" \
    "$archive_dir/archive-v2-blocks.zstd" \
    "$archive_dir/archive-v2-blocks.index"; do
    test -s "$required" || return 1
  done
  jq -e --argjson epoch "$epoch" --arg output "$archive_dir" '
    .epochs[]
    | select(.epoch == $epoch and .state == "complete" and .output_path == $output)
    | select(any(.artifacts[]; .kind == "metadata" and .state == "present" and (.bytes // 0) > 0))
    | select(any(.artifacts[]; .kind == "blocks" and .state == "present" and (.bytes // 0) > 0))
    | select(any(.artifacts[]; .kind == "block_index" and .state == "present" and (.bytes // 0) > 0))
  ' "$PIPELINE_STATUS" >/dev/null
}

wait_for_admission() {
  while ! admission_healthy; do
    publish_status waiting_for_resources
    sleep 30
  done
}

publish_status starting
event runner_started "manifest_epochs=$TOTAL_EPOCHS manifest_bytes=$TOTAL_BYTES child_nice=$CHILD_NICE child_io_priority=$CHILD_IO_PRIORITY io_resume_full_avg10=$IO_RESUME_FULL_AVG10 io_pause_full_avg10=$IO_PAUSE_FULL_AVG10 live_capture_max_stale_seconds=$LIVE_CAPTURE_MAX_STALE_SECONDS"

while IFS=$'\t' read -r epoch archive_dir source_bytes; do
  test -n "$epoch"
  CURRENT_EPOCH=$epoch
  CURRENT_PATH=$archive_dir
  CURRENT_SOURCE_BYTES=$source_bytes
  CURRENT_PROGRESS="$STATE_DIR/progress/epoch-$epoch.json"
  sidecar="$archive_dir/block-time-gaps.bin"
  receipt="$STATE_DIR/receipts/epoch-$epoch.json"
  log="$STATE_DIR/logs/epoch-$epoch.log"

  if test -s "$receipt"; then
    continue
  fi
  if test -e "$sidecar"; then
    "$BIN" verify-block-time-gaps "$sidecar" --epoch "$epoch" >> "$log" 2>&1
    jq -n --argjson epoch "$epoch" --argjson source_bytes "$source_bytes" --arg result existing_valid \
      '{epoch:$epoch,source_bytes:$source_bytes,result:$result,completed_unix_seconds:now|floor}' > "$receipt.tmp"
    mv "$receipt.tmp" "$receipt"
    DONE_EPOCHS=$((DONE_EPOCHS + 1))
    DONE_BYTES=$((DONE_BYTES + source_bytes))
    event epoch_skipped "existing valid sidecar"
    continue
  fi

  if ! terminal_archive_eligible "$epoch" "$archive_dir"; then
    LAST_ERROR="epoch $epoch is no longer a terminal-complete Archive V2 reader core"
    event epoch_ineligible "$LAST_ERROR"
    publish_status failed
    exit 65
  fi
  wait_for_admission
  if ! terminal_archive_eligible "$epoch" "$archive_dir"; then
    LAST_ERROR="epoch $epoch changed while waiting for resource admission"
    event epoch_ineligible "$LAST_ERROR"
    publish_status failed
    exit 65
  fi
  publish_status running
  event epoch_started "source_bytes=$source_bytes"
  epoch_started=$(date +%s)
  : > "$log"
  nice -n "$CHILD_NICE" ionice -c2 -n "$CHILD_IO_PRIORITY" "$BIN" build-block-time-gaps "$archive_dir" \
    --epoch "$epoch" --source archive --progress-json "$CURRENT_PROGRESS" >> "$log" 2>&1 &
  CURRENT_PID=$!

  for _ in $(seq 1 20); do
    test -d "/proc/$CURRENT_PID" || break
    if test "$(readlink -f "/proc/$CURRENT_PID/exe" 2>/dev/null || true)" = "$(readlink -f "$BIN")"; then
      break
    fi
    sleep 0.1
  done
  CHILD_START=$(awk '{print $22}' "/proc/$CURRENT_PID/stat")
  verify_child_identity "$CURRENT_PID" "$CHILD_START"

  while kill -0 "$CURRENT_PID" 2>/dev/null; do
    sleep 15
    if ! kill -0 "$CURRENT_PID" 2>/dev/null; then
      break
    fi
    if critical_pressure; then
      verify_child_identity "$CURRENT_PID" "$CHILD_START"
      kill -STOP "$CURRENT_PID"
      pause_started=$(date +%s)
      event epoch_paused "critical resource pressure"
      publish_status paused_for_resources
      healthy_checks=0
      while test "$healthy_checks" -lt 2; do
        sleep 30
        if admission_healthy; then
          healthy_checks=$((healthy_checks + 1))
        else
          healthy_checks=0
        fi
        publish_status paused_for_resources
      done
      verify_child_identity "$CURRENT_PID" "$CHILD_START"
      kill -CONT "$CURRENT_PID"
      pause_ended=$(date +%s)
      PAUSED_SECONDS=$((PAUSED_SECONDS + pause_ended - pause_started))
      event epoch_resumed "resources healthy for 60 seconds"
    fi
    publish_status running
  done

  if wait "$CURRENT_PID"; then
    rc=0
  else
    rc=$?
  fi
  CURRENT_PID=
  epoch_finished=$(date +%s)
  if test "$rc" -ne 0; then
    LAST_ERROR="epoch $epoch extractor exited $rc"
    event epoch_failed "$LAST_ERROR"
    publish_status failed
    exit "$rc"
  fi
  "$BIN" verify-block-time-gaps "$sidecar" --epoch "$epoch" >> "$log" 2>&1
  jq -n --argjson epoch "$epoch" --argjson source_bytes "$source_bytes" \
    --argjson started "$epoch_started" --argjson finished "$epoch_finished" --arg result generated \
    '{epoch:$epoch,source_bytes:$source_bytes,result:$result,started_unix_seconds:$started,completed_unix_seconds:$finished,duration_seconds:($finished-$started)}' \
    > "$receipt.tmp"
  mv "$receipt.tmp" "$receipt"
  DONE_EPOCHS=$((DONE_EPOCHS + 1))
  DONE_BYTES=$((DONE_BYTES + source_bytes))
  duration_seconds=$((epoch_finished - epoch_started))
  if test "$duration_seconds" -gt 0; then
    MEASURED_BYTES=$((MEASURED_BYTES + source_bytes))
    MEASURED_SECONDS=$((MEASURED_SECONDS + duration_seconds))
  fi
  event epoch_completed "duration_seconds=$((epoch_finished - epoch_started))"
  CURRENT_EPOCH=
  CURRENT_PATH=
  CURRENT_SOURCE_BYTES=0
  CURRENT_PROGRESS=
  publish_status running
done < "$MANIFEST"

CURRENT_EPOCH=
CURRENT_PATH=
CURRENT_SOURCE_BYTES=0
CURRENT_PROGRESS=
event runner_completed "all manifest epochs complete"
publish_status complete
