#!/usr/bin/env bash
set -euo pipefail

REPO=${BLOCKZILLA_REPO:-/home/ach/dev/blockzilla-v1-registry-mphf-20260616}
CAR_ROOT=${BLOCKZILLA_CAR_ROOT:-/volume1/blockzilla}
ARCHIVE_ROOT=${BLOCKZILLA_ARCHIVE_ROOT:-/volume1/@home/ach/dev/blockzilla-v2}
BIN=${BLOCKZILLA_BIN:-$REPO/target/release/blockzilla.lowmem-final-20260712T2155}
FINALIZER_LOCK=${BLOCKZILLA_FINALIZER_LOCK:-/tmp/blockzilla-first-seen-finalizer.lock}
STATE_DIR=${BLOCKZILLA_QUEUE_STATE_DIR:-}
MIN_AVAILABLE_KIB=${BLOCKZILLA_MIN_AVAILABLE_KIB:-2097152}
MIN_SWAP_FREE_KIB=${BLOCKZILLA_MIN_SWAP_FREE_KIB:-2097152}
MIN_VOLUME_FREE_KIB=${BLOCKZILLA_MIN_VOLUME_FREE_KIB:-21474836480}

if [[ -z "$STATE_DIR" ]]; then
  STATE_DIR=$(cat "$REPO/logs/compact-firstseen-manual-current")
fi

QUEUE_A=$STATE_DIR/queue-a.txt
QUEUE_B=$STATE_DIR/queue-b.txt
QUEUE_C=$STATE_DIR/queue-c.txt
QUEUE_D=$STATE_DIR/queue-d.txt
RUN_LOG=$STATE_DIR/runner-4lane.log
QUARANTINE_ROOT=$ARCHIVE_ROOT/.pipeline-quarantine/queued-$(date -u +%Y%m%dT%H%M%SZ)

log() {
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$RUN_LOG"
}

die() {
  log "ERROR $*"
  exit 1
}

file_nonempty() {
  [[ -s "$1" ]]
}

archive_complete() {
  local epoch=$1 dir=$ARCHIVE_ROOT/epoch-$1 file
  local required=(
    archive-v2-meta.wincode
    archive-v2-blocks.zstd
    archive-v2-blocks.index
    signatures.bin
    poh.wincode
    shredding.wincode
    registry.bin
    registry_counts.bin
    registry.mphf
    registry-first-seen.manifest
    blockhash_registry.bin
  )
  for file in "${required[@]}"; do
    file_nonempty "$dir/$file" || return 1
  done
  [[ ! -e "$dir/archive-v2-first-seen-scan-complete.v1" ]]
}

scan_marker_exists() {
  file_nonempty "$ARCHIVE_ROOT/epoch-$1/archive-v2-first-seen-scan-complete.v1"
}

input_for_epoch() {
  local epoch=$1
  if file_nonempty "$CAR_ROOT/epoch-$epoch.car.zst"; then
    printf '%s\n' "$CAR_ROOT/epoch-$epoch.car.zst"
  elif file_nonempty "$CAR_ROOT/epoch-$epoch.car"; then
    printf '%s\n' "$CAR_ROOT/epoch-$epoch.car"
  else
    return 1
  fi
}

pid_matches_epoch() {
  local pid=$1 epoch=$2 cmdline
  [[ -r "/proc/$pid/cmdline" ]] || return 1
  cmdline=$(tr '\0' ' ' <"/proc/$pid/cmdline")
  [[ "$cmdline" == *"build-archive-v2-hot-blocks"* ]] &&
    [[ "$cmdline" == *"epoch-$epoch"* ]] &&
    [[ "$cmdline" == *"$ARCHIVE_ROOT/epoch-$epoch"* ]]
}

wait_for_scan() {
  local epoch=$1 pid=${2:-} log_path=$STATE_DIR/epoch-$1.log state
  if archive_complete "$epoch" || scan_marker_exists "$epoch"; then
    return 0
  fi
  [[ -n "$pid" ]] || die "epoch=$epoch has neither a scan process nor a durable marker"

  while kill -0 "$pid" 2>/dev/null; do
    state=$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ')
    [[ "$state" == Z* ]] && break
    pid_matches_epoch "$pid" "$epoch" || die "epoch=$epoch pid=$pid no longer matches the expected scan"
    sleep 30
  done
  wait "$pid" 2>/dev/null || true

  if ! scan_marker_exists "$epoch" && ! archive_complete "$epoch"; then
    log "scan_failed epoch=$epoch pid=$pid log=$log_path"
    tail -40 "$log_path" 2>/dev/null | tee -a "$RUN_LOG" || true
    die "epoch=$epoch scan exited without a durable completion marker"
  fi
  log "scan_complete epoch=$epoch pid=$pid"
}

wait_for_resources() {
  local available swap_free volume_free
  while true; do
    available=$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)
    swap_free=$(awk '/^SwapFree:/{print $2}' /proc/meminfo)
    volume_free=$(df -Pk "$ARCHIVE_ROOT" | awk 'NR==2{print $4}')
    if (( available >= MIN_AVAILABLE_KIB &&
          swap_free >= MIN_SWAP_FREE_KIB &&
          volume_free >= MIN_VOLUME_FREE_KIB )) &&
       pgrep -f 'blockzilla-live-producer capture-grpc' >/dev/null; then
      log "resources_ready available_kib=$available swap_free_kib=$swap_free volume_free_kib=$volume_free"
      return 0
    fi
    log "resources_wait available_kib=$available swap_free_kib=$swap_free volume_free_kib=$volume_free"
    sleep 300
  done
}

quarantine_incomplete_output() {
  local epoch=$1 output=$ARCHIVE_ROOT/epoch-$1 destination stamp
  [[ -d "$output" ]] || return 0
  if [[ -z "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    return 0
  fi
  archive_complete "$epoch" && return 0
  scan_marker_exists "$epoch" && return 0

  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  mkdir -p "$QUARANTINE_ROOT"
  destination=$QUARANTINE_ROOT/epoch-$epoch-$stamp
  mv "$output" "$destination"
  printf 'Quarantined by two-lane runner before a fresh epoch %s scan at %s.\n' \
    "$epoch" "$stamp" >"$destination/PIPELINE-QUARANTINE.txt"
  log "output_quarantined epoch=$epoch destination=$destination"
}

launch_scan() {
  local epoch=$1 input previous_epoch previous_input previous_registry previous_blockhash
  local output log_path pid
  local command
  input=$(input_for_epoch "$epoch") || die "epoch=$epoch has no raw CAR input"
  previous_epoch=$((epoch - 1))
  previous_input=$(input_for_epoch "$previous_epoch" || true)
  previous_registry=$ARCHIVE_ROOT/epoch-$previous_epoch/registry.bin
  previous_blockhash=$ARCHIVE_ROOT/epoch-$previous_epoch/blockhash_registry.bin
  file_nonempty "$previous_registry" || die "epoch=$epoch previous registry is missing: $previous_registry"
  if [[ -z "$previous_input" ]]; then
    file_nonempty "$previous_blockhash" || {
      die "epoch=$epoch has neither a previous CAR nor a valid previous blockhash sidecar"
    }
  fi
  output=$ARCHIVE_ROOT/epoch-$epoch
  log_path=$STATE_DIR/epoch-$epoch.log

  quarantine_incomplete_output "$epoch"
  mkdir -p "$output"
  log "scan_start epoch=$epoch input=$input previous=${previous_input:-sidecars:$previous_blockhash} seed=$previous_registry output=$output"
  command=(
    "$BIN" build-archive-v2-hot-blocks
    "$input" "$output"
    --level 1
    --first-seen-registry
    --first-seen-seed-registry "$previous_registry"
    --first-seen-seed-keys 65536
    --first-seen-registry-capacity 34000000
    --first-seen-decode-workers 4
    --car-zstd-prefetch-mib 4
    --first-seen-scan-only
    --first-seen-finalizer-lock "$FINALIZER_LOCK"
  )
  if [[ -n "$previous_input" ]]; then
    command+=(--previous-car "$previous_input")
  fi
  "${command[@]}" >"$log_path" 2>&1 &
  pid=$!
  printf '%s\n' "$pid" >"$STATE_DIR/epoch-$epoch.pid"
  log "scan_pid epoch=$epoch pid=$pid log=$log_path"
  LAST_SCAN_PID=$pid
}

finalize_epoch() {
  local epoch=$1 output=$ARCHIVE_ROOT/epoch-$1 log_path=$STATE_DIR/epoch-$1-finalize.log
  if archive_complete "$epoch"; then
    log "finalize_skip_complete epoch=$epoch"
    return 0
  fi
  scan_marker_exists "$epoch" || die "epoch=$epoch cannot finalize without a durable scan marker"
  log "finalize_start epoch=$epoch output=$output"
  if ! "$BIN" finalize-archive-v2-first-seen \
      "$output" --finalizer-lock "$FINALIZER_LOCK" \
      >"$log_path" 2>&1; then
    log "finalize_failed epoch=$epoch log=$log_path"
    tail -40 "$log_path" 2>/dev/null | tee -a "$RUN_LOG" || true
    die "epoch=$epoch finalizer failed"
  fi
  archive_complete "$epoch" || die "epoch=$epoch finalizer exited without complete required artifacts"
  log "finalize_complete epoch=$epoch log=$log_path"
}

queue_head() {
  awk 'NF{print $1; exit}' "$1"
}

dequeue() {
  local queue=$1 expected=$2 actual tmp
  actual=$(queue_head "$queue")
  [[ "$actual" == "$expected" ]] || die "queue=$queue expected head=$expected actual=$actual"
  tmp=$queue.tmp
  awk 'BEGIN{removed=0} NF && !removed {removed=1; next} {print}' "$queue" >"$tmp"
  mv "$tmp" "$queue"
  log "queue_advanced queue=$(basename "$queue") completed_epoch=$expected"
}

prune_completed_heads() {
  local queue=$1 epoch
  while [[ -s "$queue" ]]; do
    epoch=$(queue_head "$queue")
    archive_complete "$epoch" || break
    log "queue_skip_already_complete queue=$(basename "$queue") epoch=$epoch"
    dequeue "$queue" "$epoch"
  done
}

initialize_queues() {
  if [[ ! -e "$QUEUE_C" || ! -e "$QUEUE_D" ]]; then
    {
      seq 277 279
      seq 281 299
      seq 601 699
    } >"$QUEUE_A"
    {
      printf '301\n'
      seq 303 399
      seq 701 799
    } >"$QUEUE_B"
    {
      seq 402 499
      seq 863 899
    } >"$QUEUE_C"
    seq 502 599 >"$QUEUE_D"
  fi
}

finish_batch() {
  local epochs=("${1:-}" "${3:-}" "${5:-}" "${7:-}")
  local pids=("${2:-}" "${4:-}" "${6:-}" "${8:-}")
  local index
  for index in 0 1 2 3; do
    [[ -z "${epochs[$index]}" ]] || wait_for_scan "${epochs[$index]}" "${pids[$index]}"
  done
  for index in 0 1 2 3; do
    [[ -z "${epochs[$index]}" ]] || finalize_epoch "${epochs[$index]}"
  done
}

main() {
  local epoch_a epoch_b epoch_c epoch_d pid_a pid_b pid_c pid_d
  local epoch_queue epoch queue
  mkdir -p "$STATE_DIR"
  initialize_queues
  log "runner_start lanes=4 state=$STATE_DIR binary=$BIN lock=$FINALIZER_LOCK"

  if [[ -n "${INITIAL_A_EPOCH:-}" || -n "${INITIAL_B_EPOCH:-}" ||
        -n "${INITIAL_C_EPOCH:-}" || -n "${INITIAL_D_EPOCH:-}" ]]; then
    epoch_a=${INITIAL_A_EPOCH:-$(queue_head "$QUEUE_A")}
    epoch_b=${INITIAL_B_EPOCH:-$(queue_head "$QUEUE_B")}
    epoch_c=${INITIAL_C_EPOCH:-$(queue_head "$QUEUE_C")}
    epoch_d=${INITIAL_D_EPOCH:-$(queue_head "$QUEUE_D")}
    pid_a=${INITIAL_A_PID:-}
    pid_b=${INITIAL_B_PID:-}
    pid_c=${INITIAL_C_PID:-}
    pid_d=${INITIAL_D_PID:-}

    if [[ -n "$epoch_a" && -z "$pid_a" ]] &&
       ! archive_complete "$epoch_a" && ! scan_marker_exists "$epoch_a"; then
      launch_scan "$epoch_a"
      pid_a=$LAST_SCAN_PID
    fi
    if [[ -n "$epoch_b" && -z "$pid_b" ]] &&
       ! archive_complete "$epoch_b" && ! scan_marker_exists "$epoch_b"; then
      launch_scan "$epoch_b"
      pid_b=$LAST_SCAN_PID
    fi
    if [[ -n "$epoch_c" && -z "$pid_c" ]] &&
       ! archive_complete "$epoch_c" && ! scan_marker_exists "$epoch_c"; then
      launch_scan "$epoch_c"
      pid_c=$LAST_SCAN_PID
    fi
    if [[ -n "$epoch_d" && -z "$pid_d" ]] &&
       ! archive_complete "$epoch_d" && ! scan_marker_exists "$epoch_d"; then
      launch_scan "$epoch_d"
      pid_d=$LAST_SCAN_PID
    fi

    finish_batch \
      "$epoch_a" "$pid_a" "$epoch_b" "$pid_b" \
      "$epoch_c" "$pid_c" "$epoch_d" "$pid_d"
    for epoch_queue in \
      "$epoch_a:$QUEUE_A" "$epoch_b:$QUEUE_B" \
      "$epoch_c:$QUEUE_C" "$epoch_d:$QUEUE_D"; do
      epoch=${epoch_queue%%:*}
      queue=${epoch_queue#*:}
      if [[ -n "$epoch" && "$(queue_head "$queue")" == "$epoch" ]] &&
         archive_complete "$epoch"; then
        dequeue "$queue" "$epoch"
      fi
    done
  fi

  while true; do
    prune_completed_heads "$QUEUE_A"
    prune_completed_heads "$QUEUE_B"
    prune_completed_heads "$QUEUE_C"
    prune_completed_heads "$QUEUE_D"
    epoch_a=$(queue_head "$QUEUE_A")
    epoch_b=$(queue_head "$QUEUE_B")
    epoch_c=$(queue_head "$QUEUE_C")
    epoch_d=$(queue_head "$QUEUE_D")
    if [[ -z "$epoch_a" && -z "$epoch_b" && -z "$epoch_c" && -z "$epoch_d" ]]; then
      log "all_queues_complete"
      return 0
    fi

    wait_for_resources
    pid_a=
    pid_b=
    pid_c=
    pid_d=
    if [[ -n "$epoch_a" ]]; then
      if ! archive_complete "$epoch_a" && ! scan_marker_exists "$epoch_a"; then
        launch_scan "$epoch_a"
        pid_a=$LAST_SCAN_PID
      fi
    fi
    if [[ -n "$epoch_b" ]]; then
      if ! archive_complete "$epoch_b" && ! scan_marker_exists "$epoch_b"; then
        launch_scan "$epoch_b"
        pid_b=$LAST_SCAN_PID
      fi
    fi
    if [[ -n "$epoch_c" ]]; then
      if ! archive_complete "$epoch_c" && ! scan_marker_exists "$epoch_c"; then
        launch_scan "$epoch_c"
        pid_c=$LAST_SCAN_PID
      fi
    fi
    if [[ -n "$epoch_d" ]]; then
      if ! archive_complete "$epoch_d" && ! scan_marker_exists "$epoch_d"; then
        launch_scan "$epoch_d"
        pid_d=$LAST_SCAN_PID
      fi
    fi

    finish_batch \
      "$epoch_a" "$pid_a" "$epoch_b" "$pid_b" \
      "$epoch_c" "$pid_c" "$epoch_d" "$pid_d"
    [[ -z "$epoch_a" ]] || dequeue "$QUEUE_A" "$epoch_a"
    [[ -z "$epoch_b" ]] || dequeue "$QUEUE_B" "$epoch_b"
    [[ -z "$epoch_c" ]] || dequeue "$QUEUE_C" "$epoch_c"
    [[ -z "$epoch_d" ]] || dequeue "$QUEUE_D" "$epoch_d"
  done
}

trap 'log "runner_stopped signal"; exit 143' INT TERM
main "$@"
