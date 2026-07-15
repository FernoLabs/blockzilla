#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/package-nas-pipeline.sh [VERSION]

Build the Hivezilla static UI plus release compactor, live-producer, and
Hivezilla binaries, then assemble:

  dist/nas-pipeline/blockzilla-nas-pipeline-VERSION/
  dist/nas-pipeline/blockzilla-nas-pipeline-VERSION.tar.gz
  dist/nas-pipeline/blockzilla-nas-pipeline-VERSION.tar.gz.sha256

Environment:
  BLOCKZILLA_PIPELINE_VERSION  Version when VERSION is omitted.
  BLOCKZILLA_DIST_ROOT         Output root (default: dist/nas-pipeline).
  CARGO_TARGET_DIR             Cargo target directory (default: target).
  BLOCKZILLA_SKIP_BUILD        Set to 1 to package prebuilt binaries.
  BLOCKZILLA_BINARY_DIR        Directory containing prebuilt binaries when
                               BLOCKZILLA_SKIP_BUILD=1.
  BLOCKZILLA_BUILD_PLATFORM    Platform label for a prebuilt package.
  BLOCKZILLA_OMIT_LIVE_PRODUCER
                               Set to 1 for a controller/compactor-only package.
  SOURCE_DATE_EPOCH            Stable build stamp; GNU tar also normalizes archive metadata.

The script refuses to replace an existing versioned directory or tarball.
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(cd -- "$script_dir/.." && pwd -P)
ui_root="$repo_root/crates/hivezilla/ui"
target_dir=${CARGO_TARGET_DIR:-$repo_root/target}
dist_root=${BLOCKZILLA_DIST_ROOT:-$repo_root/dist/nas-pipeline}
case "$target_dir" in
  /*) ;;
  *) target_dir="$repo_root/$target_dir" ;;
esac
case "$dist_root" in
  /*) ;;
  *) dist_root="$repo_root/$dist_root" ;;
esac
export CARGO_TARGET_DIR="$target_dir"

for command_name in cargo npm tar; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "missing required command: $command_name" >&2
    exit 2
  fi
done

if [[ ! -f "$ui_root/package-lock.json" ]]; then
  echo "missing locked UI dependencies: $ui_root/package-lock.json" >&2
  exit 2
fi

version=${1:-${BLOCKZILLA_PIPELINE_VERSION:-}}
if [[ -z "$version" ]]; then
  git_revision=$(git -C "$repo_root" rev-parse --short=12 HEAD 2>/dev/null || printf 'unknown')
  version="${git_revision}-$(date -u +%Y%m%dT%H%M%SZ)"
fi
version=${version//[^A-Za-z0-9._-]/-}
if [[ -z "$version" || "$version" == "." || "$version" == ".." ]]; then
  echo "invalid package version" >&2
  exit 2
fi

package_name="blockzilla-nas-pipeline-$version"
package_dir="$dist_root/$package_name"
tarball="$dist_root/$package_name.tar.gz"
tarball_checksum="$tarball.sha256"
if [[ -e "$package_dir" || -e "$tarball" || -e "$tarball_checksum" ]]; then
  echo "refusing to replace existing package: $package_name" >&2
  exit 3
fi

staging_root=$(mktemp -d "${TMPDIR:-/tmp}/blockzilla-nas-pipeline.XXXXXXXX")
cleanup() {
  rm -rf -- "$staging_root"
}
trap cleanup EXIT INT TERM
staging_package="$staging_root/$package_name"

echo "building Hivezilla static UI"
(
  cd "$ui_root"
  npm ci
  npm run check
  npm run build
)
if [[ ! -s "$ui_root/build/index.html" ]]; then
  echo "UI build did not produce build/index.html; configure the Svelte static adapter" >&2
  exit 4
fi

if [[ "${BLOCKZILLA_SKIP_BUILD:-0}" == "1" ]]; then
  binary_dir=${BLOCKZILLA_BINARY_DIR:?set BLOCKZILLA_BINARY_DIR when BLOCKZILLA_SKIP_BUILD=1}
  case "$binary_dir" in
    /*) ;;
    *) binary_dir="$repo_root/$binary_dir" ;;
  esac
  echo "packaging prebuilt binaries from $binary_dir"
else
  echo "building Blockzilla compactor release binary"
  cargo build \
    --locked \
    --release \
    --manifest-path "$repo_root/Cargo.toml" \
    -p blockzilla \
    --bin blockzilla

  echo "building Blockzilla live-producer release binary"
  cargo build \
    --locked \
    --release \
    --manifest-path "$repo_root/Cargo.toml" \
    -p blockzilla-live-producer \
    --bin blockzilla-live-producer

  echo "building Hivezilla release binary"
  cargo build \
    --locked \
    --release \
    --manifest-path "$repo_root/Cargo.toml" \
    -p blockzilla-hivezilla \
    --bin hivezilla
  binary_dir="$target_dir/release"
fi

blockzilla_bin="$binary_dir/blockzilla"
live_producer_bin="$binary_dir/blockzilla-live-producer"
hivezilla_bin="$binary_dir/hivezilla"
required_binaries=("$blockzilla_bin" "$hivezilla_bin")
if [[ "${BLOCKZILLA_OMIT_LIVE_PRODUCER:-0}" != "1" ]]; then
  required_binaries+=("$live_producer_bin")
fi
for binary in "${required_binaries[@]}"; do
  if [[ ! -x "$binary" ]]; then
    echo "release build did not produce executable: $binary" >&2
    exit 4
  fi
done

mkdir -p \
  "$staging_package/bin" \
  "$staging_package/ui" \
  "$staging_package/etc" \
  "$staging_package/docs"
cp "$blockzilla_bin" "$staging_package/bin/blockzilla"
cp "$hivezilla_bin" "$staging_package/bin/hivezilla"
if [[ "${BLOCKZILLA_OMIT_LIVE_PRODUCER:-0}" != "1" ]]; then
  cp "$live_producer_bin" "$staging_package/bin/blockzilla-live-producer"
fi
cp -R "$ui_root/build/." "$staging_package/ui/"
cp "$repo_root/docs/nas-compaction-pipeline.md" "$staging_package/docs/"

# These release binaries are native to the build host. Record the platform so
# the wrapper fails closed instead of attempting to execute an incompatible
# package (for example, macOS/arm64 binaries on the Linux/x86_64 NAS).
printf '%s\n' "${BLOCKZILLA_BUILD_PLATFORM:-$(uname -s)-$(uname -m)}" > "$staging_package/BUILD_PLATFORM"

cat > "$staging_package/etc/nas-pipeline.env.example" <<'EOF'
# Copy this file to nas-pipeline.env and adjust paths for the NAS.
HIVEZILLA_BIND=127.0.0.1:8787
# Omit to use the compactor shipped in this package. Set an absolute path only
# for an intentional binary override/rollback.
# BLOCKZILLA_BIN=/absolute/path/to/blockzilla
# Optional separate binary used only for repair materialization/compaction.
# REPAIR_BLOCKZILLA_BIN=/absolute/path/to/blockzilla-repair
BLOCKZILLA_CAR_ROOT=/volume1/blockzilla
BLOCKZILLA_ARCHIVE_ROOT=/volume1/@home/ach/dev/blockzilla-v2
BLOCKZILLA_LIVE_ROOT=/volume1/@home/ach/dev/blockzilla-live
HIVEZILLA_STATE_ROOT=/home/ach/dev/blockzilla-pipeline/state/nas-pipeline-v2
HIVEZILLA_SCAN_CONCURRENCY=2
# Zero means there is no numeric lane ceiling. The controller probes one lane
# at a time and stops only at measured CPU/load, I/O, memory, disk, configured
# archive-device throughput, or marginal-throughput guards. Live ingest health
# is independent telemetry and never pauses historical compaction.
HIVEZILLA_LEGACY_COMPACT_CONCURRENCY=0
# Zero also keeps finalizer overlap adaptive and uncapped. A positive value is
# retained only as an explicit rollback/compatibility ceiling.
HIVEZILLA_LEGACY_COMPACT_FINALIZER_OVERLAP=0
HIVEZILLA_LEGACY_COMPACT_CPU_CORES_PER_WORKER=1
HIVEZILLA_LEGACY_COMPACT_CPU_BUDGET_CORES=12
HIVEZILLA_LEGACY_COMPACT_IO_MIB_PER_SEC_PER_WORKER=120
HIVEZILLA_LEGACY_COMPACT_IO_BUDGET_MIB_PER_SEC=0
# Bootstrap two useful lanes when safe, then keep probing upward while aggregate
# useful throughput improves. A stopped worker retains RSS; pausing arrests
# growth but does not free memory, so paused lanes stay in memory admission.
HIVEZILLA_LEGACY_COMPACT_AUTO_PAUSE=1
HIVEZILLA_LEGACY_COMPACT_MIN_RUNNING=2
HIVEZILLA_LEGACY_COMPACT_MEMORY_GUARD_MIB=512
HIVEZILLA_LEGACY_COMPACT_IO_PAUSE_FULL_AVG10=40
HIVEZILLA_LEGACY_COMPACT_IO_RESUME_FULL_AVG10=25
HIVEZILLA_LEGACY_COMPACT_PAUSE_COOLDOWN_SECS=60
HIVEZILLA_SCAN_MEMORY_MIB=1024
HIVEZILLA_FINALIZER_MEMORY_MIB=512
HIVEZILLA_MEMORY_RESERVE_MIB=1536
HIVEZILLA_DISK_RESERVE_GIB=256
HIVEZILLA_COMPRESSION_LEVEL=1
HIVEZILLA_POLL_INTERVAL_SECS=5
HIVEZILLA_FINALIZER_LOCK=/tmp/blockzilla-first-seen-finalizer.lock
HIVEZILLA_NO_ACCESS=0
HIVEZILLA_DOWNLOAD_CONCURRENCY=1
HIVEZILLA_PREFLIGHT_CAR=1

# Optional bounded CAR acquisition. Leave unset by default. Enabling this
# requires explicit HIVEZILLA_START_EPOCH and HIVEZILLA_END_EPOCH bounds, and
# each download is structurally preflighted before its atomic canonical rename.
# The rendered URL must end in .car or .car.zst before any query string.
# HIVEZILLA_CAR_SOURCE_URL_TEMPLATE=https://example.invalid/epoch-{epoch}.car

# Required for scheduler/job control requests. Use a long random value and keep
# the real environment file private. Read-only status/events remain separate.
# HIVEZILLA_CONTROL_TOKEN=replace-with-a-long-random-token

# Optional inclusive inventory/scheduling bounds.
# HIVEZILLA_START_EPOCH=0
# HIVEZILLA_END_EPOCH=1001

# Optional work-conserving historical priority band. Runnable epochs in this
# band are considered newest-first, then scheduling falls back to normal order.
# HIVEZILLA_PRIORITY_EPOCH_START=863
# HIVEZILLA_PRIORITY_EPOCH_END=899

# The wrapper intentionally monitors only. Set exactly 1 after reviewing the
# generated state and queues to allow it to launch compaction/finalizer jobs.
HIVEZILLA_EXECUTE=0

# Optional complete replacement for the default arguments after "pipeline".
# This is split on whitespace without eval; use the wrapper's command-line
# arguments when a value needs spaces.
# HIVEZILLA_ARGS=--bind 127.0.0.1:8787 --scan-concurrency 2
EOF

cat > "$staging_package/run-nas-pipeline.sh" <<'EOF'
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
EOF
chmod 0755 "$staging_package/run-nas-pipeline.sh"

printf '%s\n' "$version" > "$staging_package/VERSION"
if [[ -n "${SOURCE_DATE_EPOCH:-}" ]]; then
  if [[ ! "$SOURCE_DATE_EPOCH" =~ ^[0-9]+$ ]]; then
    echo "SOURCE_DATE_EPOCH must be an unsigned integer" >&2
    exit 2
  fi
  printf 'unix:%s\n' "$SOURCE_DATE_EPOCH" > "$staging_package/BUILT_AT"
else
  printf '%s\n' "$(date -u +%FT%TZ)" > "$staging_package/BUILT_AT"
fi
if git -C "$repo_root" rev-parse HEAD > "$staging_package/GIT_REVISION" 2>/dev/null; then
  :
else
  printf 'unknown\n' > "$staging_package/GIT_REVISION"
fi

(
  cd "$staging_package"
  if command -v sha256sum >/dev/null 2>&1; then
    while IFS= read -r file; do
      sha256sum "$file"
    done < <(
      { find bin ui etc docs -type f -print; printf '%s\n' run-nas-pipeline.sh VERSION BUILT_AT GIT_REVISION BUILD_PLATFORM; } \
        | LC_ALL=C sort
    ) > SHA256SUMS
  elif command -v shasum >/dev/null 2>&1; then
    while IFS= read -r file; do
      shasum -a 256 "$file"
    done < <(
      { find bin ui etc docs -type f -print; printf '%s\n' run-nas-pipeline.sh VERSION BUILT_AT GIT_REVISION BUILD_PLATFORM; } \
        | LC_ALL=C sort
    ) > SHA256SUMS
  else
    echo "missing sha256sum or shasum" >&2
    exit 2
  fi
)

mkdir -p "$dist_root"
mv "$staging_package" "$package_dir"

tar_args=(-czf "$tarball" -C "$dist_root" "$package_name")
if [[ -n "${SOURCE_DATE_EPOCH:-}" ]] && tar --version 2>/dev/null | grep -q 'GNU tar'; then
  tar_args=(
    --sort=name
    "--mtime=@$SOURCE_DATE_EPOCH"
    --owner=0
    --group=0
    --numeric-owner
    -czf "$tarball"
    -C "$dist_root"
    "$package_name"
  )
fi
tar "${tar_args[@]}"

(
  cd "$dist_root"
  tarball_name=$(basename "$tarball")
  checksum_name=$(basename "$tarball_checksum")
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$tarball_name" > "$checksum_name"
  else
    shasum -a 256 "$tarball_name" > "$checksum_name"
  fi
)

echo "package directory: $package_dir"
echo "package tarball:   $tarball"
echo "tarball checksum:  $tarball_checksum"
echo "monitor-only by default; set HIVEZILLA_EXECUTE=1 only after review"
