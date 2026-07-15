#!/usr/bin/env bash
set -euo pipefail

package_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
expected_platform=$(<"$package_root/BUILD_PLATFORM")
actual_platform="$(uname -s)-$(uname -m)"
if [[ "$actual_platform" != "$expected_platform" ]]; then
  echo "package platform $expected_platform does not match this host $actual_platform" >&2
  exit 5
fi
env_file=${HIVEZILLA_ENV_FILE:-$package_root/etc/nas-pipeline.env}
if [[ ! -f "$env_file" ]]; then
  env_file="$package_root/etc/nas-pipeline.env.example"
  echo "warning: using example configuration $env_file" >&2
fi
set -a
# shellcheck source=/dev/null
source "$env_file"
set +a

if [[ -n "${HIVEZILLA_ARGS:-}" ]]; then
  read -r -a pipeline_args <<< "$HIVEZILLA_ARGS"
else
  : "${HIVEZILLA_BIND:=127.0.0.1:8787}"
  : "${BLOCKZILLA_BIN:=$package_root/bin/blockzilla}"
  : "${BLOCKZILLA_CAR_ROOT:?set BLOCKZILLA_CAR_ROOT in $env_file}"
  : "${BLOCKZILLA_ARCHIVE_ROOT:?set BLOCKZILLA_ARCHIVE_ROOT in $env_file}"
  : "${BLOCKZILLA_LIVE_ROOT:?set BLOCKZILLA_LIVE_ROOT in $env_file}"
  : "${HIVEZILLA_STATE_ROOT:?set HIVEZILLA_STATE_ROOT in $env_file}"
  : "${HIVEZILLA_SCAN_CONCURRENCY:=2}"
  : "${HIVEZILLA_LEGACY_COMPACT_CONCURRENCY:=0}"
  : "${HIVEZILLA_LEGACY_COMPACT_FINALIZER_OVERLAP:=0}"
  : "${HIVEZILLA_LEGACY_COMPACT_CPU_CORES_PER_WORKER:=1}"
  : "${HIVEZILLA_LEGACY_COMPACT_CPU_BUDGET_CORES:=12}"
  : "${HIVEZILLA_LEGACY_COMPACT_IO_MIB_PER_SEC_PER_WORKER:=120}"
  : "${HIVEZILLA_LEGACY_COMPACT_IO_BUDGET_MIB_PER_SEC:=0}"
  : "${HIVEZILLA_LEGACY_COMPACT_AUTO_PAUSE:=1}"
  : "${HIVEZILLA_LEGACY_COMPACT_MIN_RUNNING:=2}"
  : "${HIVEZILLA_LEGACY_COMPACT_MEMORY_GUARD_MIB:=512}"
  : "${HIVEZILLA_LEGACY_COMPACT_IO_PAUSE_FULL_AVG10:=40}"
  : "${HIVEZILLA_LEGACY_COMPACT_IO_RESUME_FULL_AVG10:=25}"
  : "${HIVEZILLA_LEGACY_COMPACT_PAUSE_COOLDOWN_SECS:=60}"
  : "${HIVEZILLA_SCAN_MEMORY_MIB:=1024}"
  : "${HIVEZILLA_FINALIZER_MEMORY_MIB:=512}"
  : "${HIVEZILLA_MEMORY_RESERVE_MIB:=1536}"
  : "${HIVEZILLA_DISK_RESERVE_GIB:=256}"
  : "${HIVEZILLA_COMPRESSION_LEVEL:=1}"
  : "${HIVEZILLA_POLL_INTERVAL_SECS:=5}"
  : "${HIVEZILLA_FINALIZER_LOCK:=/tmp/blockzilla-first-seen-finalizer.lock}"
  : "${HIVEZILLA_NO_ACCESS:=0}"
  : "${HIVEZILLA_DOWNLOAD_CONCURRENCY:=1}"
  : "${HIVEZILLA_PREFLIGHT_CAR:=1}"

  pipeline_args=(
    --bind "$HIVEZILLA_BIND"
    --blockzilla-bin "$BLOCKZILLA_BIN"
    --car-root "$BLOCKZILLA_CAR_ROOT"
    --archive-root "$BLOCKZILLA_ARCHIVE_ROOT"
    --live-root "$BLOCKZILLA_LIVE_ROOT"
    --state-root "$HIVEZILLA_STATE_ROOT"
    --scan-concurrency "$HIVEZILLA_SCAN_CONCURRENCY"
    --legacy-compact-concurrency "$HIVEZILLA_LEGACY_COMPACT_CONCURRENCY"
    --legacy-compact-finalizer-overlap "$HIVEZILLA_LEGACY_COMPACT_FINALIZER_OVERLAP"
    --legacy-compact-cpu-cores-per-worker "$HIVEZILLA_LEGACY_COMPACT_CPU_CORES_PER_WORKER"
    --legacy-compact-cpu-budget-cores "$HIVEZILLA_LEGACY_COMPACT_CPU_BUDGET_CORES"
    --legacy-compact-io-mib-per-sec-per-worker "$HIVEZILLA_LEGACY_COMPACT_IO_MIB_PER_SEC_PER_WORKER"
    --legacy-compact-io-budget-mib-per-sec "$HIVEZILLA_LEGACY_COMPACT_IO_BUDGET_MIB_PER_SEC"
    --legacy-compact-min-running "$HIVEZILLA_LEGACY_COMPACT_MIN_RUNNING"
    --legacy-compact-memory-guard-mib "$HIVEZILLA_LEGACY_COMPACT_MEMORY_GUARD_MIB"
    --legacy-compact-io-pause-full-avg10 "$HIVEZILLA_LEGACY_COMPACT_IO_PAUSE_FULL_AVG10"
    --legacy-compact-io-resume-full-avg10 "$HIVEZILLA_LEGACY_COMPACT_IO_RESUME_FULL_AVG10"
    --legacy-compact-pause-cooldown-secs "$HIVEZILLA_LEGACY_COMPACT_PAUSE_COOLDOWN_SECS"
    --scan-memory-mib "$HIVEZILLA_SCAN_MEMORY_MIB"
    --finalizer-memory-mib "$HIVEZILLA_FINALIZER_MEMORY_MIB"
    --memory-reserve-mib "$HIVEZILLA_MEMORY_RESERVE_MIB"
    --disk-reserve-gib "$HIVEZILLA_DISK_RESERVE_GIB"
    --level "$HIVEZILLA_COMPRESSION_LEVEL"
    --poll-interval-secs "$HIVEZILLA_POLL_INTERVAL_SECS"
    --finalizer-lock "$HIVEZILLA_FINALIZER_LOCK"
    --download-concurrency "$HIVEZILLA_DOWNLOAD_CONCURRENCY"
    --ui-dir "$package_root/ui"
  )
  if [[ -n "${REPAIR_BLOCKZILLA_BIN:-}" ]]; then
    pipeline_args+=(--repair-blockzilla-bin "$REPAIR_BLOCKZILLA_BIN")
  fi
  if [[ "$HIVEZILLA_NO_ACCESS" == "1" ]]; then
    pipeline_args+=(--no-access)
  fi
  if [[ "$HIVEZILLA_LEGACY_COMPACT_AUTO_PAUSE" == "1" ]]; then
    pipeline_args+=(--legacy-compact-auto-pause)
  fi
  if [[ "$HIVEZILLA_PREFLIGHT_CAR" == "1" ]]; then
    pipeline_args+=(--preflight-car)
  fi
  if [[ -n "${HIVEZILLA_CAR_SOURCE_URL_TEMPLATE:-}" ]]; then
    if [[ -z "${HIVEZILLA_START_EPOCH:-}" || -z "${HIVEZILLA_END_EPOCH:-}" ]]; then
      echo "HIVEZILLA_CAR_SOURCE_URL_TEMPLATE requires explicit start/end epoch bounds" >&2
      exit 2
    fi
    pipeline_args+=(--car-source-url-template "$HIVEZILLA_CAR_SOURCE_URL_TEMPLATE")
  fi
  if [[ -n "${HIVEZILLA_START_EPOCH:-}" ]]; then
    pipeline_args+=(--start-epoch "$HIVEZILLA_START_EPOCH")
  fi
  if [[ -n "${HIVEZILLA_END_EPOCH:-}" ]]; then
    pipeline_args+=(--end-epoch "$HIVEZILLA_END_EPOCH")
  fi
  if [[ -n "${HIVEZILLA_PRIORITY_EPOCH_START:-}" ]]; then
    pipeline_args+=(--priority-epoch-start "$HIVEZILLA_PRIORITY_EPOCH_START")
  fi
  if [[ -n "${HIVEZILLA_PRIORITY_EPOCH_END:-}" ]]; then
    pipeline_args+=(--priority-epoch-end "$HIVEZILLA_PRIORITY_EPOCH_END")
  fi
  if [[ "${HIVEZILLA_EXECUTE:-0}" == "1" ]]; then
    pipeline_args+=(--execute)
  fi
fi

# Extra command-line flags are appended without eval. Use them only for options
# not already emitted above: clap rejects duplicate singleton options such as
# --bind and --start-epoch. Set those values in the environment file instead.
pipeline_args+=("$@")
exec "$package_root/bin/hivezilla" pipeline "${pipeline_args[@]}"
