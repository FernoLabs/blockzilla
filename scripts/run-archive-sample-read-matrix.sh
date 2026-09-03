#!/usr/bin/env bash
set -uo pipefail

readonly DEFAULT_ORIGIN="https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev"
readonly -a EPOCHS=(0 100 200 300 400 500 600 700 800 900 1000)
readonly -a FORMATS=(compact-v2 indexer-v3 car)

mode=""
archive_root=""
origin="$DEFAULT_ORIGIN"
bin_dir=""
results_root=""
threads=12

usage() {
  printf '%s\n' \
    "usage:" \
    "  $0 --mode local --archive-root DIR --bin-dir DIR --results-root DIR [--threads N]" \
    "  $0 --mode network --bin-dir DIR --results-root DIR [--origin URL] [--threads N]" \
    "" \
    "Runs one complete read for Compact V2, Indexer V3, and CAR on epochs" \
    "0, 100, 200, ..., 1000, in that order. Completed jobs are not repeated."
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      mode=${2:-}
      shift 2
      ;;
    --archive-root)
      archive_root=${2:-}
      shift 2
      ;;
    --origin)
      origin=${2:-}
      shift 2
      ;;
    --bin-dir)
      bin_dir=${2:-}
      shift 2
      ;;
    --results-root)
      results_root=${2:-}
      shift 2
      ;;
    --threads)
      threads=${2:-}
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown option: %s\n\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$mode" != local && "$mode" != network ]]; then
  printf '%s\n' '--mode must be local or network' >&2
  exit 2
fi
if [[ -z "$bin_dir" || -z "$results_root" ]]; then
  printf '%s\n' '--bin-dir and --results-root are required' >&2
  exit 2
fi
if [[ "$mode" == local && -z "$archive_root" ]]; then
  printf '%s\n' '--archive-root is required in local mode' >&2
  exit 2
fi
if [[ ! "$threads" =~ ^[1-9][0-9]*$ ]]; then
  printf '%s\n' '--threads must be a positive integer' >&2
  exit 2
fi

bin_dir=$(cd "$bin_dir" 2>/dev/null && pwd) || {
  printf 'binary directory does not exist: %s\n' "$bin_dir" >&2
  exit 2
}
if [[ "$mode" == local ]]; then
  archive_root=$(cd "$archive_root" 2>/dev/null && pwd) || {
    printf 'archive root does not exist: %s\n' "$archive_root" >&2
    exit 2
  }
fi
mkdir -p "$results_root"
results_root=$(cd "$results_root" && pwd)

readonly CAR_READER="$bin_dir/read-car"
readonly V2_READER="$bin_dir/read-compact-v2-slot-hours"
readonly V3_READER="$bin_dir/read-indexer-v3-slot-hours"
for reader in "$CAR_READER" "$V2_READER" "$V3_READER"; do
  if [[ ! -x "$reader" ]]; then
    printf 'reader is missing or is not executable: %s\n' "$reader" >&2
    exit 2
  fi
done

lock="$results_root/.runner-lock"
if ! mkdir "$lock" 2>/dev/null; then
  printf 'another runner may be active; lock exists: %s\n' "$lock" >&2
  exit 2
fi
cleanup() {
  rmdir "$lock" 2>/dev/null || true
}
trap cleanup EXIT

readonly jobs_root="$results_root/jobs"
readonly cache_root="$results_root/cache"
readonly summary="$results_root/summary.tsv"
readonly parity="$results_root/parity.tsv"
readonly run_status="$results_root/status.tsv"
mkdir -p "$jobs_root" "$cache_root"

field() {
  local name=$1
  awk -v name="$name" '{
    for (i = 1; i <= NF; i++) {
      split($i, pair, "=")
      if (pair[1] == name) {
        sub("^[^=]*=", "", $i)
        print $i
        exit
      }
    }
  }'
}

headline_for() {
  local format=$1 file=$2
  awk -v prefix="format=$format " 'index($0, prefix) == 1 { print; exit }' "$file"
}

reader_for() {
  case "$1" in
    car) printf '%s\n' "$CAR_READER" ;;
    compact-v2) printf '%s\n' "$V2_READER" ;;
    indexer-v3) printf '%s\n' "$V3_READER" ;;
    *) return 1 ;;
  esac
}

write_summary() {
  local epoch format row
  printf 'format\tepoch\tmode\tblocks\ttransactions\trecorded_inner_instructions\tsetup_seconds\tscan_seconds\ttotal_seconds\tscan_tps\ttotal_tps\tbound_source_size_bytes\tscan_source_bytes\tscan_source_mb_s\tscan_network_bytes\tscan_network_mb_s\tscan_local_bytes\tscan_local_mb_s\tstatus\n' > "$summary.tmp"
  for epoch in "${EPOCHS[@]}"; do
    for format in "${FORMATS[@]}"; do
      row="$jobs_root/$format/epoch-$epoch/result.tsv"
      [[ -f "$row" ]] && cat "$row" >> "$summary.tmp"
    done
  done
  mv "$summary.tmp" "$summary"
}

completed_jobs() {
  local count=0 epoch format
  for epoch in "${EPOCHS[@]}"; do
    for format in "${FORMATS[@]}"; do
      [[ -f "$jobs_root/$format/epoch-$epoch/PASS" ]] && count=$((count + 1))
    done
  done
  printf '%s\n' "$count"
}

write_status() {
  local state=$1 current_format=${2:--} current_epoch=${3:--}
  printf 'state\tcompleted\ttotal\tcurrent_format\tcurrent_epoch\tupdated_at\n%s\t%s\t33\t%s\t%s\t%s\n' \
    "$state" "$(completed_jobs)" "$current_format" "$current_epoch" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    > "$run_status.tmp"
  mv "$run_status.tmp" "$run_status"
}

run_one() {
  local format=$1 epoch=$2 job_root reader headline job_cache
  local tmp_stdout tmp_stderr started ended wall_seconds exit_code
  local blocks transactions inner setup_seconds scan_seconds total_seconds
  local scan_tps total_tps bound_bytes source_bytes source_mb_s
  local network_bytes network_mb_s local_bytes local_mb_s value
  local -a source_args command
  job_root="$jobs_root/$format/epoch-$epoch"
  if [[ -f "$job_root/PASS" ]]; then
    printf 'skip format=%s epoch=%s status=PASS\n' "$format" "$epoch"
    return 0
  fi
  mkdir -p "$job_root"
  reader=$(reader_for "$format") || return 1
  source_args=()
  if [[ "$mode" == local ]]; then
    source_args=(--archive-root "$archive_root")
  elif [[ "$format" == car ]]; then
    source_args=(--origin "$origin")
  else
    job_cache="$cache_root/$format/epoch-$epoch"
    mkdir -p "$job_cache"
    source_args=(--origin "$origin" --cache-root "$job_cache")
  fi
  command=("$reader" --epoch "$epoch" "${source_args[@]}")
  if [[ "$format" != car ]]; then
    command+=(--threads "$threads")
  fi

  write_status RUNNING "$format" "$epoch"
  printf 'start format=%s epoch=%s mode=%s time=%s\n' \
    "$format" "$epoch" "$mode" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  tmp_stdout="$job_root/stdout.tmp.$$"
  tmp_stderr="$job_root/stderr.tmp.$$"
  started=$(date +%s)
  if "${command[@]}" > "$tmp_stdout" 2> "$tmp_stderr"; then
    exit_code=0
  else
    exit_code=$?
  fi
  ended=$(date +%s)
  wall_seconds=$((ended - started))
  mv "$tmp_stdout" "$job_root/stdout.log"
  mv "$tmp_stderr" "$job_root/stderr.log"

  if [[ "$exit_code" -ne 0 ]]; then
    printf 'FAIL\tformat=%s\tepoch=%s\texit_code=%s\twall_seconds=%s\n' \
      "$format" "$epoch" "$exit_code" "$wall_seconds" > "$job_root/status.tsv"
    printf 'fail format=%s epoch=%s exit=%s\n' "$format" "$epoch" "$exit_code" >&2
    return 1
  fi

  headline=$(headline_for "$format" "$job_root/stdout.log")
  if [[ -z "$headline" ]]; then
    printf 'FAIL\tformat=%s\tepoch=%s\treason=missing_summary_line\n' \
      "$format" "$epoch" > "$job_root/status.tsv"
    printf 'fail format=%s epoch=%s reason=missing_summary_line\n' "$format" "$epoch" >&2
    return 1
  fi

  blocks=$(printf '%s\n' "$headline" | field blocks)
  transactions=$(printf '%s\n' "$headline" | field transactions)
  inner=$(printf '%s\n' "$headline" | field recorded_inner_instructions)
  setup_seconds=$(printf '%s\n' "$headline" | field setup_s)
  scan_seconds=$(printf '%s\n' "$headline" | field scan_s)
  total_seconds=$(printf '%s\n' "$headline" | field total_s)
  scan_tps=$(printf '%s\n' "$headline" | field scan_tps)
  total_tps=$(printf '%s\n' "$headline" | field total_tps)
  bound_bytes=$(printf '%s\n' "$headline" | field bound_source_size_bytes)

  if [[ "$format" == car ]]; then
    source_bytes=$(printf '%s\n' "$headline" | field source_read_bytes)
    source_mb_s=$(printf '%s\n' "$headline" | field scan_source_mb_s)
    network_bytes=$(printf '%s\n' "$headline" | field scan_network_bytes)
    network_mb_s=$(printf '%s\n' "$headline" | field scan_network_mb_s)
    local_bytes=$([[ "$mode" == local ]] && printf '%s' "$source_bytes" || printf '0')
    local_mb_s=$([[ "$mode" == local ]] && printf '%s' "$source_mb_s" || printf '0')
  else
    source_bytes=$(printf '%s\n' "$headline" | field scan_logical_read_bytes)
    source_mb_s=$(printf '%s\n' "$headline" | field scan_logical_read_mb_s)
    network_bytes=$(printf '%s\n' "$headline" | field scan_network_bytes)
    network_mb_s=$(printf '%s\n' "$headline" | field scan_network_mb_s)
    local_bytes=$(printf '%s\n' "$headline" | field scan_local_read_bytes)
    local_mb_s=$(printf '%s\n' "$headline" | field scan_local_read_mb_s)
  fi
  network_bytes=${network_bytes:-0}
  network_mb_s=${network_mb_s:-0}
  local_bytes=${local_bytes:-0}
  local_mb_s=${local_mb_s:-0}

  for value in "$blocks" "$transactions" "$inner" "$setup_seconds" "$scan_seconds" \
    "$total_seconds" "$scan_tps" "$total_tps" "$bound_bytes" "$source_bytes" "$source_mb_s"; do
    if [[ -z "$value" ]]; then
      printf 'FAIL\tformat=%s\tepoch=%s\treason=incomplete_summary_line\n' \
        "$format" "$epoch" > "$job_root/status.tsv"
      printf 'fail format=%s epoch=%s reason=incomplete_summary_line\n' "$format" "$epoch" >&2
      return 1
    fi
  done

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\tPASS\n' \
    "$format" "$epoch" "$mode" "$blocks" "$transactions" "$inner" \
    "$setup_seconds" "$scan_seconds" "$total_seconds" "$scan_tps" "$total_tps" \
    "$bound_bytes" "$source_bytes" "$source_mb_s" "$network_bytes" "$network_mb_s" \
    "$local_bytes" "$local_mb_s" > "$job_root/result.tsv"
  printf 'PASS\tformat=%s\tepoch=%s\twall_seconds=%s\n' \
    "$format" "$epoch" "$wall_seconds" > "$job_root/status.tsv"
  touch "$job_root/PASS"
  write_summary
  printf 'pass format=%s epoch=%s blocks=%s transactions=%s total_s=%s total_tps=%s read_mb_s=%s\n' \
    "$format" "$epoch" "$blocks" "$transactions" "$total_seconds" "$total_tps" "$source_mb_s"
}

write_parity() {
  local mismatches=0 epoch car_row v2_row v3_row status
  local car_blocks car_tx car_inner v2_blocks v2_tx v2_inner
  local v3_blocks v3_tx v3_inner
  printf 'epoch\tcar_blocks\tv2_blocks\tv3_blocks\tcar_transactions\tv2_transactions\tv3_transactions\tcar_recorded_inner\tv2_recorded_inner\tv3_recorded_inner\tstatus\n' > "$parity.tmp"
  for epoch in "${EPOCHS[@]}"; do
    car_row="$jobs_root/car/epoch-$epoch/result.tsv"
    v2_row="$jobs_root/compact-v2/epoch-$epoch/result.tsv"
    v3_row="$jobs_root/indexer-v3/epoch-$epoch/result.tsv"
    if [[ ! -f "$car_row" || ! -f "$v2_row" || ! -f "$v3_row" ]]; then
      printf '%s\t-\t-\t-\t-\t-\t-\t-\t-\t-\tINCOMPLETE\n' "$epoch" >> "$parity.tmp"
      mismatches=$((mismatches + 1))
      continue
    fi
    IFS=$'\t' read -r _ _ _ car_blocks car_tx car_inner _ < "$car_row"
    IFS=$'\t' read -r _ _ _ v2_blocks v2_tx v2_inner _ < "$v2_row"
    IFS=$'\t' read -r _ _ _ v3_blocks v3_tx v3_inner _ < "$v3_row"
    status=PASS
    if [[ "$car_blocks" != "$v2_blocks" || "$v2_blocks" != "$v3_blocks" || \
          "$car_tx" != "$v2_tx" || "$v2_tx" != "$v3_tx" || \
          "$car_inner" != "$v2_inner" || "$v2_inner" != "$v3_inner" ]]; then
      status=MISMATCH
      mismatches=$((mismatches + 1))
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$epoch" "$car_blocks" "$v2_blocks" "$v3_blocks" \
      "$car_tx" "$v2_tx" "$v3_tx" "$car_inner" "$v2_inner" "$v3_inner" \
      "$status" >> "$parity.tmp"
  done
  mv "$parity.tmp" "$parity"
  [[ "$mismatches" -eq 0 ]]
}

write_summary
write_status RUNNING
failures=0
for format in "${FORMATS[@]}"; do
  for epoch in "${EPOCHS[@]}"; do
    run_one "$format" "$epoch" || failures=$((failures + 1))
  done
done
write_summary
if ! write_parity; then
  failures=$((failures + 1))
fi

if [[ "$failures" -eq 0 ]]; then
  write_status PASS
  printf 'matrix complete: %s\n' "$summary"
  exit 0
fi
write_status FAIL
printf 'matrix finished with %s failed jobs or parity checks; see %s\n' \
  "$failures" "$results_root" >&2
exit 1
