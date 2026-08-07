#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

BIN=${BLOCKZILLA_BIN:-/volume1/@home/ach/dev/blockzilla-pipeline/releases/blockzilla-gap-archive-v2-2026.07.18-1/bin/blockzilla}
STATE_DIR=${STATE_DIR:-/volume1/@home/ach/dev/blockzilla-pipeline/state/block-time-gap-archive-v2-v1}
MANIFEST=${MANIFEST:-$STATE_DIR/run-manifest.tsv}
PIPELINE_STATUS=${PIPELINE_STATUS:-/volume1/@home/ach/dev/blockzilla-pipeline/state/nas-pipeline-v2/status.json}
ARCHIVE_ROOT=${ARCHIVE_ROOT:-/volume1/@home/ach/dev/blockzilla-v2}
WORKERS=${WORKERS:-1}
STATUS_INTERVAL_SECONDS=${STATUS_INTERVAL_SECONDS:-15}
IO_RESUME_FULL_AVG10=${IO_RESUME_FULL_AVG10:-10.0}
IO_PAUSE_FULL_AVG10=${IO_PAUSE_FULL_AVG10:-80.0}
LIVE_CAPTURE_MAX_STALE_SECONDS=${LIVE_CAPTURE_MAX_STALE_SECONDS:-60}
# Keep the backfill below live capture at the OS scheduler level. The workflow-level
# priority lease removes historical competitors; it does not promote this process.
CHILD_NICE=${CHILD_NICE:-19}
CHILD_IO_PRIORITY=${CHILD_IO_PRIORITY:-7}

WORKER_DIR="$STATE_DIR/workers"
CLAIM_DIR="$STATE_DIR/claims"
PAUSE_FILE="$STATE_DIR/supervisor.pause"
PAUSED_IDENTITIES="$STATE_DIR/paused-children.tsv"
MANIFEST_INDEX="$STATE_DIR/run-manifest-index.json"

mkdir -p "$STATE_DIR/logs" "$STATE_DIR/progress" "$STATE_DIR/receipts" "$WORKER_DIR" "$CLAIM_DIR"
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
case "$WORKERS" in
  ''|*[!0-9]*) echo "WORKERS must be an integer from 1 to 32" >&2; exit 64 ;;
esac
test "$WORKERS" -ge 1
test "$WORKERS" -le 32
case "$STATUS_INTERVAL_SECONDS" in
  ''|*[!0-9]*) echo "STATUS_INTERVAL_SECONDS must be a positive integer" >&2; exit 64 ;;
esac
test "$STATUS_INTERVAL_SECONDS" -gt 0
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

# Claims and worker snapshots are ephemeral. Receipts are the durable source of
# truth, so a supervised restart can safely discard these only while holding the
# singleton runner lock.
rm -f "$WORKER_DIR"/worker-*.json "$PAUSE_FILE" "$PAUSED_IDENTITIES"
for stale_claim in "$CLAIM_DIR"/epoch-*; do
  test -d "$stale_claim" || continue
  rmdir "$stale_claim"
done

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
jq -Rn '
  [inputs
    | split("\t")
    | if length != 3 then error("invalid manifest row") else . end
    | {epoch:(.[0]|tonumber),path:.[1],source_bytes:(.[2]|tonumber)}]
' "$MANIFEST" > "$MANIFEST_INDEX.tmp"
jq -e --argjson expected_count "$TOTAL_EPOCHS" --argjson expected_bytes "$TOTAL_BYTES" '
  length == $expected_count
  and (map(.epoch) | unique | length) == length
  and (map(.path) | unique | length) == length
  and all(.[]; .epoch >= 0 and .source_bytes > 0)
  and ((map(.source_bytes) | add) == $expected_bytes)
' "$MANIFEST_INDEX.tmp" >/dev/null
mv "$MANIFEST_INDEX.tmp" "$MANIFEST_INDEX"
DONE_EPOCHS=0
DONE_BYTES=0
MEASURED_BYTES=0
MEASURED_SECONDS=0
RECEIPT_CACHE_SIGNATURE=
LAST_ERROR=
LIVE_CAPTURE_PID=
LIVE_CAPTURE_LAST_WRITE_BYTES=0
LIVE_CAPTURE_LAST_ADVANCE_UNIX=$(date +%s)
LIVE_CAPTURE_AGE_SECONDS=0
LIVE_CAPTURE_HEALTHY=false
PRESSURE_PAUSED=false
PRESSURE_PAUSE_STARTED=0
HEALTHY_CHECKS=0
PRESSURE_REASON=

recompute_receipts() {
  local -a receipts
  local receipt_count receipt_dir_stamp signature
  receipts=("$STATE_DIR"/receipts/epoch-*.json)
  if ! test -e "${receipts[0]}"; then
    signature=0
    test "$signature" != "$RECEIPT_CACHE_SIGNATURE" || return 0
    DONE_EPOCHS=0
    DONE_BYTES=0
    MEASURED_BYTES=0
    MEASURED_SECONDS=0
    RECEIPT_CACHE_SIGNATURE=$signature
    return 0
  fi
  receipt_count=${#receipts[@]}
  receipt_dir_stamp=$(stat -c '%Y:%Z' "$STATE_DIR/receipts")
  signature="$receipt_count:$receipt_dir_stamp"
  test "$signature" != "$RECEIPT_CACHE_SIGNATURE" || return 0
  IFS=$'\t' read -r DONE_EPOCHS DONE_BYTES MEASURED_BYTES MEASURED_SECONDS < <(
    jq -nr --slurpfile manifest "$MANIFEST_INDEX" '
      $manifest[0] as $rows
      | reduce (inputs | {value:.,filename:input_filename}) as $entry
          ({count:0,bytes:0,measured_bytes:0,measured_seconds:0,seen:{}};
            $entry.value as $receipt
            | ($entry.filename | capture("epoch-(?<epoch>[0-9]+)\\.json$").epoch | tonumber) as $filename_epoch
            | ($rows | map(select(.epoch == $receipt.epoch))) as $matches
            | if (($receipt.epoch | type) != "number")
                or ($filename_epoch != $receipt.epoch)
                or ($matches | length) != 1
                or ($matches[0].source_bytes != $receipt.source_bytes)
                or (.seen[($receipt.epoch|tostring)] // false)
              then error("invalid or duplicate block-time gap receipt: " + $entry.filename)
              else
                .seen[($receipt.epoch|tostring)] = true
                | .count += 1
                | .bytes += $receipt.source_bytes
                | if ($receipt.source_bytes > 0 and ($receipt.duration_seconds // 0) > 0) then
                    .measured_bytes += $receipt.source_bytes
                    | .measured_seconds += $receipt.duration_seconds
                  else . end
              end)
      | [.count,.bytes,.measured_bytes,.measured_seconds]
      | @tsv
    ' "${receipts[@]}"
  )
  RECEIPT_CACHE_SIGNATURE=$signature
}

live_capture_values() {
  local candidate_pid candidate_cmd current_pid current_write now wanted_subcommand
  current_pid=
  current_write=
  # Prefer the canonical compact capture, then accept the raw safety WAL. A
  # container-owned process may be visible to pgrep while its /proc I/O counters
  # are unreadable to this service; skip it instead of declaring live capture
  # unhealthy while another healthy producer is available.
  for wanted_subcommand in capture-grpc record-grpc-raw; do
    while read -r candidate_pid; do
      test -n "$candidate_pid" || continue
      test -r "/proc/$candidate_pid/cmdline" || continue
      candidate_cmd=$(tr '\0' ' ' < "/proc/$candidate_pid/cmdline" 2>/dev/null || true)
      [[ "$candidate_cmd " == *"blockzilla-live-producer $wanted_subcommand "* ]] || continue
      if ! current_write=$(awk '/^write_bytes:/ {print $2}' "/proc/$candidate_pid/io" 2>/dev/null); then
        continue
      fi
      case "$current_write" in
        ''|*[!0-9]*) continue ;;
      esac
      current_pid=$candidate_pid
      break 2
    done < <(pgrep -f blockzilla-live-producer 2>/dev/null || true)
  done

  now=$(date +%s)
  if test -z "$current_pid"; then
    LIVE_CAPTURE_PID=
    LIVE_CAPTURE_AGE_SECONDS=$((now - LIVE_CAPTURE_LAST_ADVANCE_UNIX))
    LIVE_CAPTURE_HEALTHY=false
    return
  fi
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
  local candidate_pid candidate_cmd
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
  test ! -e "$PAUSE_FILE" \
    && awk -v mem="$MEM_AVAILABLE_KIB" -v io="$IO_FULL_AVG10" -v load="$LOAD1" \
      -v io_resume="$IO_RESUME_FULL_AVG10" \
      'BEGIN {exit !(mem >= 3145728 && io <= io_resume && load <= 8.0)}' \
    && test "$FINALIZER_ACTIVE" = false \
    && test "$LIVE_CAPTURE_HEALTHY" = true
}

resume_healthy() {
  resource_values
  awk -v mem="$MEM_AVAILABLE_KIB" -v io="$IO_FULL_AVG10" -v load="$LOAD1" \
    -v io_resume="$IO_RESUME_FULL_AVG10" \
    'BEGIN {exit !(mem >= 3145728 && io <= io_resume && load <= 8.0)}' \
    && test "$FINALIZER_ACTIVE" = false \
    && test "$LIVE_CAPTURE_HEALTHY" = true
}

critical_pressure() {
  resource_values
  PRESSURE_REASON=
  if awk -v value="$MEM_AVAILABLE_KIB" 'BEGIN {exit !(value < 2621440)}'; then
    PRESSURE_REASON=memory
  elif awk -v value="$IO_FULL_AVG10" -v threshold="$IO_PAUSE_FULL_AVG10" 'BEGIN {exit !(value >= threshold)}'; then
    PRESSURE_REASON=io
  elif awk -v value="$LOAD1" 'BEGIN {exit !(value >= 10.0)}'; then
    PRESSURE_REASON=load
  elif test "$FINALIZER_ACTIVE" = true; then
    PRESSURE_REASON=archive_finalizer
  elif test "$LIVE_CAPTURE_HEALTHY" != true; then
    PRESSURE_REASON=live_capture
  fi
  test -n "$PRESSURE_REASON"
}

event() {
  local kind=$1 lane=$2 epoch=$3 message=$4
  (
    exec 7>>"$STATE_DIR/events.lock"
    flock 7
    jq -cn --argjson at "$(date +%s)" --arg kind "$kind" --arg lane "$lane" \
      --arg epoch "$epoch" --arg message "$message" \
      '{at_unix_seconds:$at,kind:$kind,lane:(if $lane == "" then null else ($lane|tonumber) end),epoch:(if $epoch == "" then null else ($epoch|tonumber) end),message:$message}' \
      >> "$STATE_DIR/events.jsonl"
  )
}

write_worker_state() {
  local lane=$1 state=$2 epoch=$3 path=$4 pid=$5 start_ticks=$6
  local progress=$7 source_bytes=$8 started=$9 error=${10}
  local target="$WORKER_DIR/worker-$lane.json"
  jq -n \
    --argjson lane "$lane" --arg state "$state" --arg epoch "$epoch" \
    --arg path "$path" --arg pid "$pid" --arg start_ticks "$start_ticks" \
    --arg progress "$progress" --arg source_bytes "$source_bytes" \
    --arg started "$started" --arg error "$error" --argjson updated "$(date +%s)" \
    '{lane:$lane,state:$state,epoch:(if $epoch == "" then null else ($epoch|tonumber) end),path:(if $path == "" then null else $path end),pid:(if $pid == "" then null else ($pid|tonumber) end),start_ticks:(if $start_ticks == "" then null else ($start_ticks|tonumber) end),progress_path:(if $progress == "" then null else $progress end),source_bytes:(if $source_bytes == "" then 0 else ($source_bytes|tonumber) end),started_unix_seconds:(if $started == "" then null else ($started|tonumber) end),updated_unix_seconds:$updated,error:(if $error == "" then null else $error end)}' \
    > "$target.tmp.$$"
  mv "$target.tmp.$$" "$target"
}

verify_child_identity() {
  local pid=$1 expected_start=$2
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

worker_loop() (
  local lane=$1 epoch archive_dir source_bytes sidecar receipt log claim
  local epoch_started epoch_finished duration_seconds rc
  local worker_error=
  WORKER_CHILD_PID=
  WORKER_CHILD_START=
  WORKER_CLAIM=
  WORKER_EPOCH=
  WORKER_PATH=
  WORKER_PROGRESS=
  WORKER_SOURCE_BYTES=
  WORKER_STARTED=

  # Only the top-level supervisor may keep the singleton lock. Extractor
  # children also receive /dev/null rather than the manifest stream.
  exec 9>&-

  worker_on_exit() {
    local exit_rc=$1
    trap - EXIT TERM INT
    set +e
    if test -n "$WORKER_CHILD_PID" && verify_child_identity "$WORKER_CHILD_PID" "$WORKER_CHILD_START"; then
      kill -CONT "$WORKER_CHILD_PID" 2>/dev/null
      kill -TERM "$WORKER_CHILD_PID" 2>/dev/null
      for _ in $(seq 1 40); do
        kill -0 "$WORKER_CHILD_PID" 2>/dev/null || break
        sleep 0.25
      done
      if verify_child_identity "$WORKER_CHILD_PID" "$WORKER_CHILD_START" 2>/dev/null; then
        kill -KILL "$WORKER_CHILD_PID" 2>/dev/null
      fi
      wait "$WORKER_CHILD_PID" 2>/dev/null
    fi
    if test -n "$WORKER_CLAIM"; then
      rmdir "$WORKER_CLAIM" 2>/dev/null
    fi
    if test "$exit_rc" -ne 0; then
      if test "$exit_rc" -eq 143 || test "$exit_rc" -eq 130; then
        write_worker_state "$lane" stopped "$WORKER_EPOCH" "$WORKER_PATH" "" "" "$WORKER_PROGRESS" "$WORKER_SOURCE_BYTES" "$WORKER_STARTED" "worker stopped"
      else
        write_worker_state "$lane" failed "$WORKER_EPOCH" "$WORKER_PATH" "" "" "$WORKER_PROGRESS" "$WORKER_SOURCE_BYTES" "$WORKER_STARTED" "${worker_error:-worker exited $exit_rc}"
      fi
    fi
    exit "$exit_rc"
  }
  trap 'worker_on_exit $?' EXIT
  trap 'exit 143' TERM
  trap 'exit 130' INT

  write_worker_state "$lane" idle "" "" "" "" "" "" "" ""
  while IFS=$'\t' read -r epoch archive_dir source_bytes <&8; do
    test -n "$epoch"
    receipt="$STATE_DIR/receipts/epoch-$epoch.json"
    test -s "$receipt" && continue
    claim="$CLAIM_DIR/epoch-$epoch"
    if ! mkdir "$claim" 2>/dev/null; then
      continue
    fi
    WORKER_CLAIM=$claim
    WORKER_EPOCH=$epoch
    WORKER_PATH=$archive_dir
    WORKER_PROGRESS="$STATE_DIR/progress/epoch-$epoch.json"
    WORKER_SOURCE_BYTES=$source_bytes
    WORKER_STARTED=
    sidecar="$archive_dir/block-time-gaps.bin"
    log="$STATE_DIR/logs/epoch-$epoch.log"

    # Another lane may have completed between the first receipt check and claim.
    if test -s "$receipt"; then
      rmdir "$claim"
      WORKER_CLAIM=
      continue
    fi
    if test -e "$sidecar"; then
      "$BIN" verify-block-time-gaps "$sidecar" --epoch "$epoch" >> "$log" 2>&1
      jq -n --argjson epoch "$epoch" --argjson source_bytes "$source_bytes" --arg result existing_valid \
        '{epoch:$epoch,source_bytes:$source_bytes,result:$result,completed_unix_seconds:now|floor}' > "$receipt.tmp.$$"
      mv "$receipt.tmp.$$" "$receipt"
      event epoch_skipped "$lane" "$epoch" "existing valid sidecar"
      rmdir "$claim"
      WORKER_CLAIM=
      continue
    fi
    if ! terminal_archive_eligible "$epoch" "$archive_dir"; then
      worker_error="epoch $epoch is no longer a terminal-complete Archive V2 reader core"
      event epoch_ineligible "$lane" "$epoch" "$worker_error"
      return 65
    fi

    while ! admission_healthy; do
      write_worker_state "$lane" waiting_for_resources "$epoch" "$archive_dir" "" "" "$WORKER_PROGRESS" "$source_bytes" "" ""
      sleep 30
    done
    if ! terminal_archive_eligible "$epoch" "$archive_dir"; then
      worker_error="epoch $epoch changed while waiting for resource admission"
      event epoch_ineligible "$lane" "$epoch" "$worker_error"
      return 65
    fi

    epoch_started=$(date +%s)
    WORKER_STARTED=$epoch_started
    rm -f "$WORKER_PROGRESS"
    # BASHPID keeps retry logs distinct even if a supervised restart launches
    # the same epoch and lane again within the same second.
    log="$STATE_DIR/logs/epoch-$epoch-attempt-$epoch_started-lane-$lane-pid-$BASHPID.log"
    : > "$log"
    write_worker_state "$lane" starting "$epoch" "$archive_dir" "" "" "$WORKER_PROGRESS" "$source_bytes" "$epoch_started" ""
    event epoch_started "$lane" "$epoch" "source_bytes=$source_bytes"
    nice -n "$CHILD_NICE" ionice -c2 -n "$CHILD_IO_PRIORITY" "$BIN" build-block-time-gaps "$archive_dir" \
      --epoch "$epoch" --source archive --progress-json "$WORKER_PROGRESS" \
      </dev/null 8<&- >> "$log" 2>&1 &
    WORKER_CHILD_PID=$!

    for _ in $(seq 1 20); do
      test -d "/proc/$WORKER_CHILD_PID" || break
      if test "$(readlink -f "/proc/$WORKER_CHILD_PID/exe" 2>/dev/null || true)" = "$(readlink -f "$BIN")"; then
        break
      fi
      sleep 0.1
    done
    WORKER_CHILD_START=$(awk '{print $22}' "/proc/$WORKER_CHILD_PID/stat")
    verify_child_identity "$WORKER_CHILD_PID" "$WORKER_CHILD_START"
    write_worker_state "$lane" running "$epoch" "$archive_dir" "$WORKER_CHILD_PID" "$WORKER_CHILD_START" "$WORKER_PROGRESS" "$source_bytes" "$epoch_started" ""

    if wait "$WORKER_CHILD_PID"; then
      rc=0
    else
      rc=$?
    fi
    WORKER_CHILD_PID=
    WORKER_CHILD_START=
    epoch_finished=$(date +%s)
    if test "$rc" -ne 0; then
      worker_error="epoch $epoch extractor exited $rc"
      event epoch_failed "$lane" "$epoch" "$worker_error"
      return "$rc"
    fi
    "$BIN" verify-block-time-gaps "$sidecar" --epoch "$epoch" >> "$log" 2>&1
    jq -n --argjson epoch "$epoch" --argjson source_bytes "$source_bytes" \
      --argjson started "$epoch_started" --argjson finished "$epoch_finished" --argjson lane "$lane" --arg result generated \
      '{epoch:$epoch,source_bytes:$source_bytes,result:$result,lane:$lane,started_unix_seconds:$started,completed_unix_seconds:$finished,duration_seconds:($finished-$started)}' \
      > "$receipt.tmp.$$"
    mv "$receipt.tmp.$$" "$receipt"
    duration_seconds=$((epoch_finished - epoch_started))
    event epoch_completed "$lane" "$epoch" "duration_seconds=$duration_seconds"
    rmdir "$claim"
    WORKER_CLAIM=
    WORKER_EPOCH=
    WORKER_PATH=
    WORKER_PROGRESS=
    WORKER_SOURCE_BYTES=
    WORKER_STARTED=
    write_worker_state "$lane" idle "" "" "" "" "" "" "" ""
  done 8< "$MANIFEST"

  write_worker_state "$lane" complete "" "" "" "" "" "" "" ""
  trap - EXIT TERM INT
  exit 0
)

publish_status() {
  local state=$1 now elapsed wall_elapsed overall_done rate eta eta_reliable
  local lane worker_file worker_state worker_epoch worker_progress worker_pid receipt
  local current_done current_elapsed worker_done worker_elapsed worker_rate
  local aggregate_current_rate active_workers rated_workers first_epoch first_path first_pid first_progress_state
  local workers_jsonl workers_json progress_state candidate_priority first_priority

  recompute_receipts
  now=$(date +%s)
  elapsed=$((now - RUN_STARTED_UNIX))
  wall_elapsed=$((CARRY_ELAPSED_SECONDS + elapsed))
  current_done=0
  aggregate_current_rate=0
  active_workers=0
  rated_workers=0
  first_epoch=
  first_path=
  first_pid=
  first_progress_state=null
  first_priority=0
  workers_jsonl="$STATE_DIR/.workers-status.jsonl.tmp.$$"
  : > "$workers_jsonl"

  for lane in $(seq 1 "$WORKERS"); do
    worker_file="$WORKER_DIR/worker-$lane.json"
    if ! test -s "$worker_file"; then
      jq -cn --argjson lane "$lane" '{lane:$lane,state:"starting",epoch:null,path:null,pid:null,start_ticks:null,progress_path:null,source_bytes:0,started_unix_seconds:null,updated_unix_seconds:null,error:null,source_bytes_done:0,progress_state:null,throughput_bytes_per_second:0}' >> "$workers_jsonl"
      continue
    fi
    worker_state=$(jq -r '.state' "$worker_file")
    worker_epoch=$(jq -r '.epoch // empty' "$worker_file")
    worker_pid=$(jq -r '.pid // empty' "$worker_file")
    worker_progress=$(jq -r '.progress_path // empty' "$worker_file")
    worker_done=0
    worker_elapsed=0
    progress_state=null
    if test -n "$worker_epoch"; then
      receipt="$STATE_DIR/receipts/epoch-$worker_epoch.json"
      if test "$worker_state" = running && test ! -s "$receipt" && test -n "$worker_progress" && test -s "$worker_progress"; then
        worker_done=$(jq -r '.source_bytes_done // 0' "$worker_progress" 2>/dev/null || echo 0)
        worker_elapsed=$(jq -r '.elapsed_seconds // 0' "$worker_progress" 2>/dev/null || echo 0)
        progress_state=$(jq -r '.state // "unknown"' "$worker_progress" 2>/dev/null || echo unknown)
      fi
    fi
    worker_rate=$(awk -v bytes="$worker_done" -v seconds="$worker_elapsed" 'BEGIN {if (bytes > 0 && seconds >= 10) printf "%.3f", bytes/seconds; else print 0}')
    if awk -v lane_rate="$worker_rate" 'BEGIN {exit !(lane_rate > 0)}'; then
      rated_workers=$((rated_workers + 1))
    fi
    current_done=$((current_done + worker_done))
    aggregate_current_rate=$(awk -v total="$aggregate_current_rate" -v lane_rate="$worker_rate" 'BEGIN {printf "%.3f", total+lane_rate}')
    if test "$worker_state" = running || test "$worker_state" = starting; then
      active_workers=$((active_workers + 1))
    fi
    candidate_priority=0
    if test "$worker_state" = running && test -n "$worker_pid"; then
      candidate_priority=2
    elif test -n "$worker_epoch"; then
      candidate_priority=1
    fi
    if test "$candidate_priority" -gt "$first_priority"; then
      first_priority=$candidate_priority
      first_epoch=$worker_epoch
      first_path=$(jq -r '.path // empty' "$worker_file")
      first_pid=$worker_pid
      first_progress_state=$progress_state
    fi
    jq -c --argjson source_bytes_done "$worker_done" --arg progress_state "$progress_state" \
      --argjson throughput_bps "$worker_rate" \
      '. + {source_bytes_done:$source_bytes_done,progress_state:(if $progress_state == "null" then null else $progress_state end),throughput_bytes_per_second:$throughput_bps}' \
      "$worker_file" >> "$workers_jsonl"
  done
  workers_json=$(jq -s . "$workers_jsonl")
  rm -f "$workers_jsonl"

  overall_done=$((DONE_BYTES + current_done))
  if test "$overall_done" -gt "$TOTAL_BYTES"; then
    overall_done=$TOTAL_BYTES
  fi
  if awk -v current="$aggregate_current_rate" 'BEGIN {exit !(current > 0)}'; then
    rate=$aggregate_current_rate
    if test "$rated_workers" -eq "$active_workers" && test "$active_workers" -eq "$WORKERS"; then
      eta_reliable=true
    else
      eta_reliable=false
    fi
  else
    rate=$(awk -v bytes="$MEASURED_BYTES" -v seconds="$MEASURED_SECONDS" 'BEGIN {if (seconds > 0) printf "%.3f", bytes/seconds; else print 0}')
    if test "$WORKERS" -eq 1 && test "$MEASURED_SECONDS" -ge 300; then
      eta_reliable=true
    else
      eta_reliable=false
    fi
  fi
  eta=$(awk -v done="$overall_done" -v total="$TOTAL_BYTES" -v bytes_per_second="$rate" 'BEGIN {if (bytes_per_second > 0 && total >= done) printf "%.0f", (total-done)/bytes_per_second; else print -1}')
  resource_values

  jq -n \
    --arg state "$state" \
    --argjson started "$BACKFILL_STARTED_UNIX" \
    --argjson runner_started "$RUN_STARTED_UNIX" \
    --argjson updated "$now" \
    --argjson wall_elapsed "$wall_elapsed" \
    --argjson workers_configured "$WORKERS" \
    --argjson active_workers "$active_workers" \
    --argjson total_epochs "$TOTAL_EPOCHS" \
    --argjson done_epochs "$DONE_EPOCHS" \
    --argjson total_bytes "$TOTAL_BYTES" \
    --argjson done_bytes "$DONE_BYTES" \
    --argjson current_bytes_done "$current_done" \
    --argjson overall_bytes_done "$overall_done" \
    --argjson throughput_bps "$rate" \
    --argjson eta_seconds "$eta" \
    --argjson eta_reliable "$eta_reliable" \
    --argjson measured_bytes "$MEASURED_BYTES" \
    --argjson measured_seconds "$MEASURED_SECONDS" \
    --arg current_epoch "$first_epoch" \
    --arg current_path "$first_path" \
    --arg current_pid "$first_pid" \
    --arg current_progress_state "$first_progress_state" \
    --argjson workers "$workers_json" \
    --argjson mem_available_kib "$MEM_AVAILABLE_KIB" \
    --argjson io_full_avg10 "$IO_FULL_AVG10" \
    --argjson load1 "$LOAD1" \
    --argjson finalizer_active "$FINALIZER_ACTIVE" \
    --argjson pressure_paused "$PRESSURE_PAUSED" \
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
    '{schema_version:1,state:$state,started_unix_seconds:$started,runner_started_unix_seconds:$runner_started,updated_unix_seconds:$updated,backfill:{epochs_done:$done_epochs,epochs_total:$total_epochs,workers_configured:$workers_configured,active_workers:$active_workers,source_bytes_done:$done_bytes,current_source_bytes_done:$current_bytes_done,overall_source_bytes_done:$overall_bytes_done,source_bytes_total:$total_bytes,wall_elapsed_seconds:$wall_elapsed,measured_source_bytes:$measured_bytes,measured_duration_seconds:$measured_seconds,wall_throughput_bytes_per_second:$throughput_bps,eta_seconds:(if $eta_seconds >= 0 then $eta_seconds else null end),eta_reliable:$eta_reliable},current:{epoch:(if $current_epoch == "" then null else ($current_epoch|tonumber) end),path:(if $current_path == "" then null else $current_path end),pid:(if $current_pid == "" then null else ($current_pid|tonumber) end),progress_state:(if $current_progress_state == "null" then null else $current_progress_state end)},workers:$workers,resources:{mem_available_kib:$mem_available_kib,io_full_avg10:$io_full_avg10,load1:$load1,finalizer_active:$finalizer_active,pressure_pause_active:$pressure_paused,paused_seconds:$paused_seconds,io_resume_full_avg10:$io_resume_full_avg10,io_pause_full_avg10:$io_pause_full_avg10,child_nice:$child_nice,child_io_priority:$child_io_priority,live_capture_max_stale_seconds:$live_capture_max_stale_seconds,live_capture_age_seconds:$live_capture_age_seconds,live_capture_healthy:$live_capture_healthy,live_capture_pid:(if $live_capture_pid == "" then null else ($live_capture_pid|tonumber) end)},last_error:(if $last_error == "" then null else $last_error end)}' \
    > "$STATE_DIR/.status.json.tmp"
  mv "$STATE_DIR/.status.json.tmp" "$STATE_DIR/status.json"
}

pause_active_children() {
  local lane worker_file pid start_ticks epoch snapshot_tmp
  snapshot_tmp="$PAUSED_IDENTITIES.tmp.$$"
  : > "$snapshot_tmp"
  touch "$PAUSE_FILE"
  if test -s "$PAUSED_IDENTITIES"; then
    while IFS=$'\t' read -r lane pid start_ticks epoch; do
      if verify_child_identity "$pid" "$start_ticks" 2>/dev/null; then
        printf '%s\t%s\t%s\t%s\n' "$lane" "$pid" "$start_ticks" "$epoch" >> "$snapshot_tmp"
      fi
    done < "$PAUSED_IDENTITIES"
  fi
  for lane in $(seq 1 "$WORKERS"); do
    worker_file="$WORKER_DIR/worker-$lane.json"
    test -s "$worker_file" || continue
    pid=$(jq -r '.pid // empty' "$worker_file")
    start_ticks=$(jq -r '.start_ticks // empty' "$worker_file")
    epoch=$(jq -r '.epoch // empty' "$worker_file")
    test -n "$pid" && test -n "$start_ticks" || continue
    if verify_child_identity "$pid" "$start_ticks" 2>/dev/null; then
      if kill -STOP "$pid" 2>/dev/null; then
        if ! awk -F '\t' -v wanted_pid="$pid" -v wanted_start="$start_ticks" \
          '$2 == wanted_pid && $3 == wanted_start {found=1} END {exit !found}' "$snapshot_tmp"; then
          printf '%s\t%s\t%s\t%s\n' "$lane" "$pid" "$start_ticks" "$epoch" >> "$snapshot_tmp"
        fi
      fi
    fi
  done
  mv "$snapshot_tmp" "$PAUSED_IDENTITIES"
}

resume_paused_children() {
  local lane pid start_ticks epoch
  if test -s "$PAUSED_IDENTITIES"; then
    while IFS=$'\t' read -r lane pid start_ticks epoch; do
      if verify_child_identity "$pid" "$start_ticks" 2>/dev/null; then
        kill -CONT "$pid" 2>/dev/null || true
      fi
    done < "$PAUSED_IDENTITIES"
  fi
  rm -f "$PAUSED_IDENTITIES"
}

active_snapshot() {
  local lane file epoch pid
  local snapshot=
  for lane in $(seq 1 "$WORKERS"); do
    file="$WORKER_DIR/worker-$lane.json"
    test -s "$file" || continue
    epoch=$(jq -r '.epoch // empty' "$file")
    pid=$(jq -r '.pid // empty' "$file")
    test -n "$pid" || continue
    snapshot="${snapshot}lane=$lane,epoch=$epoch,pid=$pid;"
  done
  printf '%s' "$snapshot"
}

declare -a WORKER_PIDS
declare -a WORKER_REAPED
SUPERVISOR_WORKERS_STARTED=false
SUPERVISOR_CLEANED=false

terminate_workers() {
  local lane pid start_ticks file
  touch "$PAUSE_FILE"
  resume_paused_children
  for lane in $(seq 1 "$WORKERS"); do
    pid=${WORKER_PIDS[$lane]:-}
    test -n "$pid" || continue
    kill -TERM "$pid" 2>/dev/null || true
  done
  for _ in $(seq 1 40); do
    local alive=false
    for lane in $(seq 1 "$WORKERS"); do
      pid=${WORKER_PIDS[$lane]:-}
      if test -n "$pid" && kill -0 "$pid" 2>/dev/null; then alive=true; fi
    done
    test "$alive" = false && break
    sleep 0.25
  done
  for lane in $(seq 1 "$WORKERS"); do
    pid=${WORKER_PIDS[$lane]:-}
    if test -n "$pid" && kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
    file="$WORKER_DIR/worker-$lane.json"
    if test -s "$file"; then
      pid=$(jq -r '.pid // empty' "$file")
      start_ticks=$(jq -r '.start_ticks // empty' "$file")
      if test -n "$pid" && test -n "$start_ticks" && verify_child_identity "$pid" "$start_ticks" 2>/dev/null; then
        kill -CONT "$pid" 2>/dev/null || true
        kill -KILL "$pid" 2>/dev/null || true
      fi
    fi
  done
  for lane in $(seq 1 "$WORKERS"); do
    pid=${WORKER_PIDS[$lane]:-}
    test -n "$pid" || continue
    wait "$pid" 2>/dev/null || true
  done
}

supervisor_on_exit() {
  local exit_rc=$1
  trap - EXIT TERM INT
  if test "$SUPERVISOR_WORKERS_STARTED" = true && test "$SUPERVISOR_CLEANED" = false; then
    set +e
    test -n "$LAST_ERROR" || LAST_ERROR="supervisor exited unexpectedly with status $exit_rc"
    event runner_failed "" "" "$LAST_ERROR"
    terminate_workers
    if test "$PRESSURE_PAUSED" = true && test "$PRESSURE_PAUSE_STARTED" -gt 0; then
      PAUSED_SECONDS=$((PAUSED_SECONDS + $(date +%s) - PRESSURE_PAUSE_STARTED))
    fi
    PRESSURE_PAUSED=false
    publish_status failed
    rm -f "$PAUSE_FILE"
  fi
  exit "$exit_rc"
}
trap 'supervisor_on_exit $?' EXIT

shutdown_supervisor() {
  trap - TERM INT
  LAST_ERROR="supervisor stopped"
  event runner_stopping "" "" "signal received"
  terminate_workers
  if test "$PRESSURE_PAUSED" = true && test "$PRESSURE_PAUSE_STARTED" -gt 0; then
    PAUSED_SECONDS=$((PAUSED_SECONDS + $(date +%s) - PRESSURE_PAUSE_STARTED))
  fi
  PRESSURE_PAUSED=false
  publish_status stopped
  rm -f "$PAUSE_FILE"
  SUPERVISOR_CLEANED=true
  exit 143
}
trap shutdown_supervisor TERM INT

recompute_receipts
for lane in $(seq 1 "$WORKERS"); do
  write_worker_state "$lane" starting "" "" "" "" "" "" "" ""
done
publish_status starting
event runner_started "" "" "manifest_epochs=$TOTAL_EPOCHS manifest_bytes=$TOTAL_BYTES workers=$WORKERS child_nice=$CHILD_NICE child_io_priority=$CHILD_IO_PRIORITY io_resume_full_avg10=$IO_RESUME_FULL_AVG10 io_pause_full_avg10=$IO_PAUSE_FULL_AVG10 live_capture_max_stale_seconds=$LIVE_CAPTURE_MAX_STALE_SECONDS"

SUPERVISOR_WORKERS_STARTED=true
for lane in $(seq 1 "$WORKERS"); do
  worker_loop "$lane" &
  WORKER_PIDS[$lane]=$!
  WORKER_REAPED[$lane]=false
done

while true; do
  alive_workers=0
  failed_rc=0
  failed_lane=
  for lane in $(seq 1 "$WORKERS"); do
    test "${WORKER_REAPED[$lane]}" = false || continue
    pid=${WORKER_PIDS[$lane]}
    if kill -0 "$pid" 2>/dev/null; then
      alive_workers=$((alive_workers + 1))
      continue
    fi
    if wait "$pid"; then rc=0; else rc=$?; fi
    WORKER_REAPED[$lane]=true
    if test "$rc" -ne 0 && test "$failed_rc" -eq 0; then
      failed_rc=$rc
      failed_lane=$lane
    fi
  done

  if test "$failed_rc" -ne 0; then
    LAST_ERROR="worker $failed_lane exited $failed_rc"
    worker_file="$WORKER_DIR/worker-$failed_lane.json"
    if test -s "$worker_file"; then
      worker_error=$(jq -r '.error // empty' "$worker_file")
      test -z "$worker_error" || LAST_ERROR="$worker_error"
    fi
    event runner_failed "$failed_lane" "" "$LAST_ERROR"
    terminate_workers
    PRESSURE_PAUSED=false
    publish_status failed
    rm -f "$PAUSE_FILE"
    SUPERVISOR_CLEANED=true
    exit "$failed_rc"
  fi

  if test "$alive_workers" -eq 0; then
    recompute_receipts
    if test "$DONE_EPOCHS" -ne "$TOTAL_EPOCHS"; then
      LAST_ERROR="workers exited with $DONE_EPOCHS of $TOTAL_EPOCHS receipts"
      event runner_failed "" "" "$LAST_ERROR"
      publish_status failed
      SUPERVISOR_CLEANED=true
      exit 66
    fi
    event runner_completed "" "" "all manifest epochs complete"
    publish_status complete
    rm -f "$PAUSE_FILE"
    SUPERVISOR_CLEANED=true
    exit 0
  fi

  if critical_pressure; then
    HEALTHY_CHECKS=0
    if test "$PRESSURE_PAUSED" = false; then
      PRESSURE_PAUSED=true
      PRESSURE_PAUSE_STARTED=$(date +%s)
      event workers_paused "" "" "reason=$PRESSURE_REASON mem_available_kib=$MEM_AVAILABLE_KIB io_full_avg10=$IO_FULL_AVG10 load1=$LOAD1 finalizer_active=$FINALIZER_ACTIVE live_capture_healthy=$LIVE_CAPTURE_HEALTHY live_capture_age_seconds=$LIVE_CAPTURE_AGE_SECONDS active=$(active_snapshot)"
    fi
    # Reconcile on every sample: a worker may have crossed admission just before
    # the pause sentinel was created but not yet published its child identity.
    pause_active_children
  elif test "$PRESSURE_PAUSED" = true; then
    pause_active_children
    if resume_healthy; then
      HEALTHY_CHECKS=$((HEALTHY_CHECKS + 1))
    else
      HEALTHY_CHECKS=0
    fi
    if test "$HEALTHY_CHECKS" -ge 4; then
      resume_paused_children
      rm -f "$PAUSE_FILE"
      PAUSED_SECONDS=$((PAUSED_SECONDS + $(date +%s) - PRESSURE_PAUSE_STARTED))
      PRESSURE_PAUSED=false
      PRESSURE_PAUSE_STARTED=0
      HEALTHY_CHECKS=0
      event workers_resumed "" "" "resources healthy for four checks"
    fi
  fi

  if test "$PRESSURE_PAUSED" = true; then
    publish_status paused_for_resources
  else
    publish_status running
  fi
  sleep "$STATUS_INTERVAL_SECONDS"
done
