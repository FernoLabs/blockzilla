#!/usr/bin/env bash
set -euo pipefail

package_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
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
  : "${HIVEZILLA_SCAN_CONCURRENCY:=4}"
  : "${HIVEZILLA_SCAN_MEMORY_MIB:=800}"
  : "${HIVEZILLA_MEMORY_RESERVE_MIB:=256}"
  : "${HIVEZILLA_DISK_RESERVE_GIB:=256}"
  : "${HIVEZILLA_COMPRESSION_LEVEL:=1}"
  : "${HIVEZILLA_POLL_INTERVAL_SECS:=5}"
  : "${HIVEZILLA_FINALIZER_LOCK:=/tmp/blockzilla-first-seen-finalizer.lock}"
  : "${HIVEZILLA_NO_ACCESS:=0}"

  pipeline_args=(
    --bind "$HIVEZILLA_BIND"
    --blockzilla-bin "$BLOCKZILLA_BIN"
    --car-root "$BLOCKZILLA_CAR_ROOT"
    --archive-root "$BLOCKZILLA_ARCHIVE_ROOT"
    --live-root "$BLOCKZILLA_LIVE_ROOT"
    --state-root "$HIVEZILLA_STATE_ROOT"
    --scan-concurrency "$HIVEZILLA_SCAN_CONCURRENCY"
    --scan-memory-mib "$HIVEZILLA_SCAN_MEMORY_MIB"
    --memory-reserve-mib "$HIVEZILLA_MEMORY_RESERVE_MIB"
    --disk-reserve-gib "$HIVEZILLA_DISK_RESERVE_GIB"
    --level "$HIVEZILLA_COMPRESSION_LEVEL"
    --poll-interval-secs "$HIVEZILLA_POLL_INTERVAL_SECS"
    --finalizer-lock "$HIVEZILLA_FINALIZER_LOCK"
    --ui-dir "$package_root/ui"
  )
  if [[ "$HIVEZILLA_NO_ACCESS" == "1" ]]; then
    pipeline_args+=(--no-access)
  fi
  if [[ -n "${HIVEZILLA_START_EPOCH:-}" ]]; then
    pipeline_args+=(--start-epoch "$HIVEZILLA_START_EPOCH")
  fi
  if [[ -n "${HIVEZILLA_END_EPOCH:-}" ]]; then
    pipeline_args+=(--end-epoch "$HIVEZILLA_END_EPOCH")
  fi
  if [[ "${HIVEZILLA_EXECUTE:-0}" == "1" ]]; then
    pipeline_args+=(--execute)
  fi
fi

# Extra command-line flags are appended without eval. They are useful for a
# one-off bind address or epoch bound while keeping the env file unchanged.
pipeline_args+=("$@")
exec "$package_root/bin/hivezilla" pipeline "${pipeline_args[@]}"
