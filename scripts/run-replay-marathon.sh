#!/usr/bin/env bash
set -euo pipefail

program_name="${0##*/}"
readonly metadata_schema='blockzilla-replay-marathon-v1'
readonly default_duration='6h'
readonly default_pidstat_interval='60'
readonly termination_grace_seconds='30'
original_args=("$@")
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
script_path="$script_dir/${BASH_SOURCE[0]##*/}"

workspace=''
binary=''
archive_root=''
anchor_checkpoint=''
anchor_sha256=''
completed_epoch=''
start_epoch=''
end_epoch=''
run_dir=''
duration="$default_duration"
duration_seconds=''
cpu_set='none'
nice_level='0'
sample_diffs='0'
sample_accounts='0'
pidstat_interval="$default_pidstat_interval"
dry_run=0

generation_paths=()
generation_probe_outputs=()
generation_manifest_paths=()
generation_manifest_sha256s=()
generation_digests=()
generation_registry_sha256s=()
generation_ids=()
expected_cluster=''
expected_slots_per_epoch=''

replay_pid=''
replay_job_pid=''
replay_process_group=0
log_stream_pid=''
pidstat_pid=''
pidstat_job_pid=''
fifo_path=''
stop_reason='replay-exit'
requested_signal=''
termination_deadline=0

usage() {
  cat <<'EOF'
Run a bounded, checkpointed Compact replay continuation with durable logs.

Usage:
  scripts/run-replay-marathon.sh \
    --workspace /absolute/path/to/blockzilla-v1 \
    --binary /absolute/path/to/blockzilla-replay-poc \
    --archive-root /absolute/path/to/compact-epochs \
    --anchor-checkpoint /absolute/path/to/epoch-N.chk \
    --anchor-sha256 HEX \
    --completed-epoch N \
    --start-epoch N+1 \
    --end-epoch M \
    --run-dir /absolute/path/to/new-run-directory [options]

Required:
  --workspace DIR            Explicit repository checkout used as the cwd.
  --binary FILE              Executable blockzilla-replay-poc binary.
  --archive-root DIR         Contains epoch-N Compact generation directories.
  --anchor-checkpoint FILE   Frozen checkpoint at --completed-epoch.
  --anchor-sha256 HEX        Trusted whole-file SHA-256 for the anchor.
  --completed-epoch N        Exact Compact generation bound by the anchor.
  --start-epoch N            First successor; must equal completed epoch + 1.
  --end-epoch N              Last successor, inclusive.
  --run-dir DIR              New directory. Existing paths are refused.

Runtime controls:
  --duration TIME            Wall limit: integer seconds or Ns, Nm, Nh.
                             Use 0 for no limit. Default: 6h.
  --cpu-set LIST             Linux taskset CPU list, for example 2 or 2-3,8.
                             Default: none (no affinity wrapper).
  --nice N                   nice adjustment from -20 through 19. Default: 0.
  --sample-diffs N           Replay diagnostic diff sample count. Default: 0.
  --sample-accounts N        Final account sample count. Default: 0.
  --pidstat-interval N       Resource sample interval in seconds. Default: 60.

Validation-only modes:
  --dry-run                  Validate inputs and print the exact replay plan.
                             It creates no run directory and launches no replay.
  --self-test                Run isolated parser/hash safety checks. No replay.
  -h, --help                 Show this help.

The runner validates every Compact generation with `probe-compact` before it
starts. It copies the binary and authenticated anchor into the unique run
directory and makes those copies read-only. The mutable checkpoint output is a
different run-local file and is atomically refreshed by the replay binary only
at exhausted generation boundaries.
EOF
}

die() {
  printf '%s: %s\n' "$program_name" "$*" >&2
  exit 1
}

utc_now() {
  date -u '+%Y-%m-%dT%H:%M:%SZ'
}

supervisor_log() {
  local message="$*"
  local rendered
  rendered="$(utc_now) $message"
  printf '%s\n' "$rendered"
  if [[ -n "$run_dir" && -d "$run_dir" ]]; then
    printf '%s\n' "$rendered" >> "$run_dir/supervisor.log"
  fi
}

require_option_value() {
  local option="$1"
  local value="${2-}"
  [[ -n "$value" ]] || die "$option requires a nonempty value"
}

is_safe_unsigned() {
  local value="$1"
  [[ "$value" =~ ^(0|[1-9][0-9]{0,9})$ ]] && (( 10#$value <= 2147483647 ))
}

is_safe_count() {
  is_safe_unsigned "$1"
}

duration_to_seconds() {
  local value="$1"
  local number suffix multiplier
  if [[ "$value" =~ ^(0|[1-9][0-9]{0,9})([smh]?)$ ]]; then
    number="${BASH_REMATCH[1]}"
    suffix="${BASH_REMATCH[2]}"
  else
    return 1
  fi
  case "$suffix" in
    ''|s) multiplier=1 ;;
    m) multiplier=60 ;;
    h) multiplier=3600 ;;
    *) return 1 ;;
  esac
  if (( 10#$number > 2147483647 / multiplier )); then
    return 1
  fi
  printf '%s' "$((10#$number * multiplier))"
}

is_safe_cpu_set() {
  local value="$1"
  [[ "$value" == 'none' || "$value" =~ ^[0-9]+([,-][0-9]+)*$ ]]
}

validate_path_text() {
  local option="$1"
  local value="$2"
  [[ "$value" == /* ]] || die "$option must be an absolute path"
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* && "$value" != *$'\t'* ]] \
    || die "$option must not contain tabs or line breaks"
}

canonical_directory() {
  local option="$1"
  local value="$2"
  validate_path_text "$option" "$value"
  [[ ! -L "$value" && -d "$value" ]] || die "$option must be a real directory, not a symlink: $value"
  (cd -- "$value" && pwd -P)
}

canonical_regular_file() {
  local option="$1"
  local value="$2"
  local parent base canonical_parent
  validate_path_text "$option" "$value"
  [[ ! -L "$value" && -f "$value" && -s "$value" ]] \
    || die "$option must be a nonempty regular file, not a symlink: $value"
  parent="${value%/*}"
  base="${value##*/}"
  [[ -n "$parent" && -n "$base" ]] || die "$option is not a valid file path: $value"
  canonical_parent="$(cd -- "$parent" && pwd -P)"
  printf '%s/%s' "$canonical_parent" "$base"
}

canonical_new_directory() {
  local option="$1"
  local value="$2"
  local parent base canonical_parent
  validate_path_text "$option" "$value"
  [[ ! -e "$value" && ! -L "$value" ]] || die "$option already exists; refusing to reuse it: $value"
  parent="${value%/*}"
  base="${value##*/}"
  [[ -n "$parent" && -n "$base" && "$base" != '.' && "$base" != '..' ]] \
    || die "$option is not a safe new directory path: $value"
  [[ ! -L "$parent" && -d "$parent" ]] || die "$option parent must be a real existing directory: $parent"
  canonical_parent="$(cd -- "$parent" && pwd -P)"
  [[ -w "$canonical_parent" ]] || die "$option parent is not writable: $canonical_parent"
  printf '%s/%s' "$canonical_parent" "$base"
}

sha256_file() {
  local path="$1"
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -- "$path" | awk '{print $1}'
  elif command -v sha256sum >/dev/null 2>&1; then
    sha256sum -- "$path" | awk '{print $1}'
  else
    die 'shasum or sha256sum is required'
  fi
}

lowercase_sha256() {
  local value="$1"
  value="$(printf '%s' "$value" | tr 'A-F' 'a-f')"
  [[ "$value" =~ ^[0-9a-f]{64}$ ]] || die '--anchor-sha256 must be exactly 64 hexadecimal characters'
  printf '%s' "$value"
}

shell_quote_command() {
  local item
  for item in "$@"; do
    printf '%q ' "$item"
  done
  printf '\n'
}

run_self_test() {
  local temporary hash
  [[ "$(duration_to_seconds 6h)" == '21600' ]] || die 'self-test: 6h duration parse failed'
  [[ "$(duration_to_seconds 90m)" == '5400' ]] || die 'self-test: 90m duration parse failed'
  [[ "$(duration_to_seconds 0)" == '0' ]] || die 'self-test: zero duration parse failed'
  if duration_to_seconds '1h30m' >/dev/null 2>&1; then
    die 'self-test: malformed duration was accepted'
  fi
  is_safe_cpu_set '2-3,8' || die 'self-test: valid CPU set was rejected'
  if is_safe_cpu_set '2;id'; then
    die 'self-test: unsafe CPU set was accepted'
  fi
  is_safe_count '0' || die 'self-test: zero count was rejected'
  if is_safe_count '-1'; then
    die 'self-test: negative count was accepted'
  fi
  temporary="$(mktemp -d "${TMPDIR:-/tmp}/blockzilla-replay-marathon-self-test.XXXXXX")"
  trap 'rm -rf -- "$temporary"' EXIT
  printf 'abc' > "$temporary/hash-input"
  hash="$(sha256_file "$temporary/hash-input")"
  [[ "$hash" == 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad' ]] \
    || die 'self-test: SHA-256 helper returned the wrong digest'
  mkdir -- "$temporary/existing-run"
  [[ -e "$temporary/existing-run" ]] || die 'self-test: unique-run fixture failed'
  rm -rf -- "$temporary"
  trap - EXIT
  printf '%s: PASS (duration, count, CPU-set, SHA-256, and unique-run guards; replay not launched)\n' "$program_name"
}

if (( $# == 1 )) && [[ "$1" == '--self-test' ]]; then
  run_self_test
  exit 0
fi

while (( $# > 0 )); do
  case "$1" in
    --workspace)
      require_option_value "$1" "${2-}"
      workspace="$2"
      shift 2
      ;;
    --binary)
      require_option_value "$1" "${2-}"
      binary="$2"
      shift 2
      ;;
    --archive-root)
      require_option_value "$1" "${2-}"
      archive_root="$2"
      shift 2
      ;;
    --anchor-checkpoint)
      require_option_value "$1" "${2-}"
      anchor_checkpoint="$2"
      shift 2
      ;;
    --anchor-sha256)
      require_option_value "$1" "${2-}"
      anchor_sha256="$2"
      shift 2
      ;;
    --completed-epoch)
      require_option_value "$1" "${2-}"
      completed_epoch="$2"
      shift 2
      ;;
    --start-epoch)
      require_option_value "$1" "${2-}"
      start_epoch="$2"
      shift 2
      ;;
    --end-epoch)
      require_option_value "$1" "${2-}"
      end_epoch="$2"
      shift 2
      ;;
    --run-dir)
      require_option_value "$1" "${2-}"
      run_dir="$2"
      shift 2
      ;;
    --duration)
      require_option_value "$1" "${2-}"
      duration="$2"
      shift 2
      ;;
    --cpu-set)
      require_option_value "$1" "${2-}"
      cpu_set="$2"
      shift 2
      ;;
    --nice)
      require_option_value "$1" "${2-}"
      nice_level="$2"
      shift 2
      ;;
    --sample-diffs)
      require_option_value "$1" "${2-}"
      sample_diffs="$2"
      shift 2
      ;;
    --sample-accounts)
      require_option_value "$1" "${2-}"
      sample_accounts="$2"
      shift 2
      ;;
    --pidstat-interval)
      require_option_value "$1" "${2-}"
      pidstat_interval="$2"
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --self-test)
      die '--self-test must be used by itself'
      ;;
    --)
      shift
      (( $# == 0 )) || die "unexpected positional arguments: $*"
      ;;
    *)
      die "unknown argument: $1 (use --help)"
      ;;
  esac
done

[[ -n "$workspace" ]] || die '--workspace is required'
[[ -n "$binary" ]] || die '--binary is required'
[[ -n "$archive_root" ]] || die '--archive-root is required'
[[ -n "$anchor_checkpoint" ]] || die '--anchor-checkpoint is required'
[[ -n "$anchor_sha256" ]] || die '--anchor-sha256 is required'
[[ -n "$completed_epoch" ]] || die '--completed-epoch is required'
[[ -n "$start_epoch" ]] || die '--start-epoch is required'
[[ -n "$end_epoch" ]] || die '--end-epoch is required'
[[ -n "$run_dir" ]] || die '--run-dir is required'

workspace="$(canonical_directory '--workspace' "$workspace")"
binary="$(canonical_regular_file '--binary' "$binary")"
archive_root="$(canonical_directory '--archive-root' "$archive_root")"
anchor_checkpoint="$(canonical_regular_file '--anchor-checkpoint' "$anchor_checkpoint")"
run_dir="$(canonical_new_directory '--run-dir' "$run_dir")"
anchor_sha256="$(lowercase_sha256 "$anchor_sha256")"

[[ -f "$workspace/Cargo.toml" && -f "$workspace/crates/blockzilla-replay/Cargo.toml" ]] \
  || die '--workspace does not look like the Blockzilla replay checkout'
[[ -x "$binary" ]] || die "--binary is not executable: $binary"

is_safe_unsigned "$completed_epoch" || die '--completed-epoch must be a nonnegative integer no larger than 2147483647'
is_safe_unsigned "$start_epoch" || die '--start-epoch must be a nonnegative integer no larger than 2147483647'
is_safe_unsigned "$end_epoch" || die '--end-epoch must be a nonnegative integer no larger than 2147483647'
completed_epoch=$((10#$completed_epoch))
start_epoch=$((10#$start_epoch))
end_epoch=$((10#$end_epoch))
(( completed_epoch < 2147483647 )) || die '--completed-epoch is too large to have a successor'
(( start_epoch == completed_epoch + 1 )) \
  || die "--start-epoch must equal --completed-epoch + 1 ($((completed_epoch + 1)))"
(( end_epoch >= start_epoch )) || die '--end-epoch must be at least --start-epoch'
(( end_epoch - start_epoch + 1 <= 4096 )) || die 'refusing a range larger than 4096 generations'

duration_seconds="$(duration_to_seconds "$duration")" \
  || die '--duration must be an integer number of seconds or use one s, m, or h suffix'
is_safe_cpu_set "$cpu_set" || die '--cpu-set must be none or a taskset list such as 2 or 2-3,8'
[[ "$nice_level" =~ ^-?(0|[1-9][0-9]?)$ ]] || die '--nice must be an integer from -20 through 19'
nice_level=$((10#$nice_level))
(( nice_level >= -20 && nice_level <= 19 )) || die '--nice must be an integer from -20 through 19'
is_safe_count "$sample_diffs" || die '--sample-diffs must be a nonnegative integer no larger than 2147483647'
is_safe_count "$sample_accounts" || die '--sample-accounts must be a nonnegative integer no larger than 2147483647'
is_safe_count "$pidstat_interval" || die '--pidstat-interval must be a positive integer no larger than 2147483647'
sample_diffs=$((10#$sample_diffs))
sample_accounts=$((10#$sample_accounts))
pidstat_interval=$((10#$pidstat_interval))
(( pidstat_interval > 0 )) || die '--pidstat-interval must be greater than zero'

command -v awk >/dev/null 2>&1 || die 'awk is required'
command -v mkfifo >/dev/null 2>&1 || die 'mkfifo is required'
command -v nice >/dev/null 2>&1 || die 'nice is required'
command -v pidstat >/dev/null 2>&1 || die 'pidstat is required (install the sysstat package)'
command -v stdbuf >/dev/null 2>&1 || die 'stdbuf is required for line-buffered replay logs'
if [[ "$cpu_set" != 'none' ]]; then
  command -v taskset >/dev/null 2>&1 || die '--cpu-set requires taskset (normally provided by util-linux)'
fi
setsid_wait_supported=0
if command -v setsid >/dev/null 2>&1; then
  setsid_help="$(setsid --help 2>&1 || true)"
  if [[ "$setsid_help" == *'--wait'* ]]; then
    setsid_wait_supported=1
  fi
fi

actual_anchor_sha256="$(sha256_file "$anchor_checkpoint")"
[[ "$actual_anchor_sha256" == "$anchor_sha256" ]] \
  || die "anchor checkpoint SHA-256 mismatch: expected $anchor_sha256, found $actual_anchor_sha256"
binary_sha256="$(sha256_file "$binary")"

validate_generation() {
  local expected_epoch="$1"
  local path="$2"
  local manifest="$path/archive-v2-generation.json"
  local output identity_line digest_line
  local actual_cluster actual_epoch generation_id slots_per_epoch generation_digest registry_sha256
  local manifest_sha256

  [[ ! -L "$path" && -d "$path" ]] || die "Compact epoch $expected_epoch is not a real directory: $path"
  [[ ! -L "$manifest" && -f "$manifest" && -s "$manifest" ]] \
    || die "Compact epoch $expected_epoch has no regular sealed manifest: $manifest"
  manifest_sha256="$(sha256_file "$manifest")"

  if ! output="$("$binary" probe-compact "$path" --max-slots 0 --sample-transactions 0 2>&1)"; then
    printf '%s\n' "$output" >&2
    die "probe-compact rejected epoch $expected_epoch: $path"
  fi
  identity_line="$(printf '%s\n' "$output" | awk '/^cluster=/{print; exit}')"
  digest_line="$(printf '%s\n' "$output" | awk '/^generation_digest=/{print; exit}')"
  if [[ "$identity_line" =~ ^cluster=([^[:space:]]+)[[:space:]]epoch=([0-9]+)[[:space:]]generation_id=([^[:space:]]+)[[:space:]]slots_per_epoch=([0-9]+)$ ]]; then
    actual_cluster="${BASH_REMATCH[1]}"
    actual_epoch="${BASH_REMATCH[2]}"
    generation_id="${BASH_REMATCH[3]}"
    slots_per_epoch="${BASH_REMATCH[4]}"
  else
    die "could not parse probe identity for epoch $expected_epoch: $identity_line"
  fi
  if [[ "$digest_line" =~ ^generation_digest=([0-9a-f]{64})[[:space:]]registry_sha256=([0-9a-f]{64})$ ]]; then
    generation_digest="${BASH_REMATCH[1]}"
    registry_sha256="${BASH_REMATCH[2]}"
  else
    die "could not parse probe digests for epoch $expected_epoch: $digest_line"
  fi
  [[ "$actual_epoch" == "$expected_epoch" ]] \
    || die "generation path $path declares epoch $actual_epoch, expected $expected_epoch"
  if [[ -z "$expected_cluster" ]]; then
    expected_cluster="$actual_cluster"
    expected_slots_per_epoch="$slots_per_epoch"
  else
    [[ "$actual_cluster" == "$expected_cluster" ]] \
      || die "epoch $expected_epoch cluster $actual_cluster differs from $expected_cluster"
    [[ "$slots_per_epoch" == "$expected_slots_per_epoch" ]] \
      || die "epoch $expected_epoch slots_per_epoch $slots_per_epoch differs from $expected_slots_per_epoch"
  fi
  generation_paths+=("$path")
  generation_probe_outputs+=("$output")
  generation_manifest_paths+=("$manifest")
  generation_manifest_sha256s+=("$manifest_sha256")
  generation_digests+=("$generation_digest")
  generation_registry_sha256s+=("$registry_sha256")
  generation_ids+=("$generation_id")
}

completed_generation="$archive_root/epoch-$completed_epoch"
validate_generation "$completed_epoch" "$completed_generation"
epoch="$start_epoch"
while (( epoch <= end_epoch )); do
  validate_generation "$epoch" "$archive_root/epoch-$epoch"
  epoch=$((epoch + 1))
done

successor_paths=("${generation_paths[@]:1}")
planned_anchor="$run_dir/anchor.checkpoint"
planned_binary="$run_dir/replay-binary"
checkpoint_out="$run_dir/checkpoint.latest.chk"
replay_command=(
  "$planned_binary"
  resume-compact-chain
  --checkpoint "$planned_anchor"
  --expected-checkpoint-sha256 "$anchor_sha256"
  --completed-generation "$completed_generation"
  "${successor_paths[@]}"
  --sample-diffs "$sample_diffs"
  --sample-accounts "$sample_accounts"
  --checkpoint-out "$checkpoint_out"
  --generation-metrics
)
launch_command=()
if (( nice_level != 0 )); then
  launch_command+=(nice -n "$nice_level")
fi
if [[ "$cpu_set" != 'none' ]]; then
  launch_command+=(taskset -c "$cpu_set")
fi
launch_command+=(stdbuf -oL -eL "${replay_command[@]}")
if (( setsid_wait_supported )); then
  launch_command=(setsid --wait "${launch_command[@]}")
fi

if (( dry_run )); then
  printf 'validation=passed\n'
  printf 'mode=dry-run replay_launched=false run_directory_created=false\n'
  printf 'workspace=%s\n' "$workspace"
  printf 'binary=%s sha256=%s\n' "$binary" "$binary_sha256"
  printf 'anchor=%s sha256=%s completed_epoch=%s\n' "$anchor_checkpoint" "$anchor_sha256" "$completed_epoch"
  printf 'cluster=%s slots_per_epoch=%s successor_generations=%s range=%s..%s\n' \
    "$expected_cluster" "$expected_slots_per_epoch" "${#successor_paths[@]}" "$start_epoch" "$end_epoch"
  printf 'duration=%s duration_seconds=%s cpu_set=%s nice=%s pidstat_interval=%s\n' \
    "$duration" "$duration_seconds" "$cpu_set" "$nice_level" "$pidstat_interval"
  printf 'planned_run_dir=%s\n' "$run_dir"
  printf 'planned_command='
  shell_quote_command "${launch_command[@]}"
  exit 0
fi

run_parent="${run_dir%/*}"
lock_file="$run_parent/.blockzilla-replay-marathon.lock"
lock_backend=''
if [[ ! -e "$lock_file" && ! -L "$lock_file" ]]; then
  # noclobber makes concurrent first creation harmless; the winner and loser
  # subsequently open the same inode for the advisory lock.
  (set -o noclobber; : > "$lock_file") 2>/dev/null || true
fi
[[ ! -L "$lock_file" && -f "$lock_file" ]] \
  || die "advisory lock path must be a regular file, not a symlink: $lock_file"
if command -v flock >/dev/null 2>&1; then
  exec 9>> "$lock_file"
  flock -n 9 || die "another replay marathon holds the advisory lock: $lock_file"
  lock_backend='flock'
elif command -v lockf >/dev/null 2>&1; then
  if [[ "${BLOCKZILLA_REPLAY_MARATHON_LOCKED_PATH-}" != "$lock_file" ]]; then
    bash_path="$(command -v bash)"
    exec env BLOCKZILLA_REPLAY_MARATHON_LOCKED_PATH="$lock_file" \
      lockf -kn "$lock_file" "$bash_path" "$script_path" "${original_args[@]}"
  fi
  lock_backend='lockf'
else
  die 'flock or lockf is required for the run-parent advisory lock'
fi

# Recheck under the advisory lock and use mkdir as the atomic uniqueness gate.
[[ ! -e "$run_dir" && ! -L "$run_dir" ]] || die "run directory appeared concurrently: $run_dir"
mkdir -- "$run_dir"
chmod 0700 "$run_dir"
supervisor_log "run directory created: $run_dir"

cleanup_processes() {
  local exit_status=$?
  if [[ -n "$replay_pid" ]] && kill -0 "$replay_pid" 2>/dev/null; then
    if (( replay_process_group )); then
      kill -s TERM -- "-$replay_pid" 2>/dev/null || true
    else
      kill -s TERM -- "$replay_pid" 2>/dev/null || true
    fi
  fi
  if [[ -n "$pidstat_pid" ]]; then
    kill -s TERM -- "$pidstat_pid" 2>/dev/null || true
  fi
  if [[ -n "$pidstat_job_pid" ]]; then
    kill -s TERM -- "$pidstat_job_pid" 2>/dev/null || true
  fi
  if [[ -n "$replay_job_pid" ]]; then
    kill -s TERM -- "$replay_job_pid" 2>/dev/null || true
  fi
  if [[ -n "$log_stream_pid" ]]; then
    kill -s TERM -- "$log_stream_pid" 2>/dev/null || true
  fi
  if [[ -n "$fifo_path" && -p "$fifo_path" ]]; then
    rm -f -- "$fifo_path"
  fi
  return "$exit_status"
}
trap cleanup_processes EXIT

cp -- "$anchor_checkpoint" "$planned_anchor"
cp -- "$binary" "$planned_binary"
chmod 0400 "$planned_anchor"
chmod 0500 "$planned_binary"
[[ "$(sha256_file "$planned_anchor")" == "$anchor_sha256" ]] \
  || die 'run-local anchor copy failed its SHA-256 check'
[[ "$(sha256_file "$planned_binary")" == "$binary_sha256" ]] \
  || die 'run-local binary copy failed its SHA-256 check'
[[ "$(sha256_file "$anchor_checkpoint")" == "$anchor_sha256" ]] \
  || die 'source anchor changed while the run-local immutable copy was created'
[[ "$(sha256_file "$binary")" == "$binary_sha256" ]] \
  || die 'source binary changed while the run-local immutable copy was created'

started_utc="$(utc_now)"
started_epoch_seconds="$(date '+%s')"
hostname_value="$(hostname 2>/dev/null || printf 'unknown')"
uname_value="$(uname -a 2>/dev/null || printf 'unknown')"
git_commit='unavailable'
git_tracked_dirty='unknown'
if command -v git >/dev/null 2>&1 && git -C "$workspace" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git_commit="$(git -C "$workspace" rev-parse HEAD 2>/dev/null || printf 'unavailable')"
  if [[ -n "$(git -C "$workspace" status --porcelain --untracked-files=no 2>/dev/null)" ]]; then
    git_tracked_dirty='true'
  else
    git_tracked_dirty='false'
  fi
fi
script_sha256="$(sha256_file "$script_path")"

{
  printf 'schema\t%s\n' "$metadata_schema"
  printf 'started_utc\t%s\n' "$started_utc"
  printf 'supervisor_pid\t%s\n' "$$"
  printf 'hostname\t%s\n' "$hostname_value"
  printf 'uname\t%s\n' "$uname_value"
  printf 'workspace\t%s\n' "$workspace"
  printf 'git_commit\t%s\n' "$git_commit"
  printf 'git_tracked_dirty\t%s\n' "$git_tracked_dirty"
  printf 'script_sha256\t%s\n' "$script_sha256"
  printf 'binary_source\t%s\n' "$binary"
  printf 'binary_snapshot\t%s\n' "$planned_binary"
  printf 'binary_sha256\t%s\n' "$binary_sha256"
  printf 'archive_root\t%s\n' "$archive_root"
  printf 'cluster\t%s\n' "$expected_cluster"
  printf 'slots_per_epoch\t%s\n' "$expected_slots_per_epoch"
  printf 'anchor_source\t%s\n' "$anchor_checkpoint"
  printf 'anchor_snapshot\t%s\n' "$planned_anchor"
  printf 'anchor_sha256\t%s\n' "$anchor_sha256"
  printf 'completed_epoch\t%s\n' "$completed_epoch"
  printf 'start_epoch\t%s\n' "$start_epoch"
  printf 'end_epoch\t%s\n' "$end_epoch"
  printf 'duration\t%s\n' "$duration"
  printf 'duration_seconds\t%s\n' "$duration_seconds"
  printf 'cpu_set\t%s\n' "$cpu_set"
  printf 'nice\t%s\n' "$nice_level"
  printf 'sample_diffs\t%s\n' "$sample_diffs"
  printf 'sample_accounts\t%s\n' "$sample_accounts"
  printf 'pidstat_interval_seconds\t%s\n' "$pidstat_interval"
  printf 'advisory_lock\t%s\n' "$lock_file"
  printf 'advisory_lock_backend\t%s\n' "$lock_backend"
  printf 'checkpoint_output\t%s\n' "$checkpoint_out"
} > "$run_dir/run-metadata.tsv"
chmod 0400 "$run_dir/run-metadata.tsv"

{
  printf 'kind\tepoch\tpath\tsha256\tgeneration_digest\tregistry_sha256\tidentity\n'
  printf 'binary\t-\t%s\t%s\t-\t-\treplay-binary\n' "$binary" "$binary_sha256"
  printf 'anchor\t%s\t%s\t%s\t-\t-\tfrozen-checkpoint\n' "$completed_epoch" "$anchor_checkpoint" "$anchor_sha256"
  index=0
  while (( index < ${#generation_paths[@]} )); do
    printf 'generation\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$((completed_epoch + index))" \
      "${generation_manifest_paths[$index]}" \
      "${generation_manifest_sha256s[$index]}" \
      "${generation_digests[$index]}" \
      "${generation_registry_sha256s[$index]}" \
      "${generation_ids[$index]}"
    index=$((index + 1))
  done
} > "$run_dir/input-hashes.tsv"
chmod 0400 "$run_dir/input-hashes.tsv"

{
  index=0
  for output in "${generation_probe_outputs[@]}"; do
    printf 'probe_index=%s path=%s\n' "$index" "${generation_paths[$index]}"
    printf '%s\n' "$output"
    index=$((index + 1))
  done
} > "$run_dir/generation-probes.log"
chmod 0400 "$run_dir/generation-probes.log"

shell_quote_command "${launch_command[@]}" > "$run_dir/command.txt"
chmod 0400 "$run_dir/command.txt"

fifo_path="$run_dir/.replay-log.fifo"
mkfifo -m 0600 "$fifo_path"
: > "$run_dir/replay.log"
: > "$run_dir/generation-metrics.log"
: > "$run_dir/checkpoint-events.log"
: > "$run_dir/resource-pidstat.log"
replay_pid_file="$run_dir/.replay.pid"
replay_status_file="$run_dir/.replay.exit-status"
log_stream_status_file="$run_dir/.log-stream.exit-status"
pidstat_pid_file="$run_dir/.pidstat.pid"
pidstat_status_file="$run_dir/.pidstat.exit-status"

stream_replay_log() {
  local line
  exec 3>> "$run_dir/replay.log"
  exec 4>> "$run_dir/generation-metrics.log"
  exec 5>> "$run_dir/checkpoint-events.log"
  while IFS= read -r line || [[ -n "$line" ]]; do
    printf '%s\n' "$line"
    printf '%s\n' "$line" >&3
    case "$line" in
      generation_metrics\ *)
        printf '%s\n' "$line" >&4
        ;;
      checkpoint_published\ *|checkpoint_source\ *)
        printf '%s\n' "$line" >&5
        ;;
    esac
  done < "$fifo_path"
}

stream_replay_log_job() {
  local status
  set +e
  stream_replay_log
  status=$?
  printf '%s\n' "$status" > "$log_stream_status_file.tmp"
  mv -- "$log_stream_status_file.tmp" "$log_stream_status_file"
  return 0
}

stream_replay_log_job &
log_stream_pid=$!

cd -- "$workspace"
runtime_launch=()
if (( nice_level != 0 )); then
  runtime_launch+=(nice -n "$nice_level")
fi
if [[ "$cpu_set" != 'none' ]]; then
  runtime_launch+=(taskset -c "$cpu_set")
fi
runtime_launch+=(stdbuf -oL -eL "${replay_command[@]}")
if (( setsid_wait_supported )); then
  replay_process_group=1
fi

run_replay_job() {
  local child status
  set +e
  if (( replay_process_group )); then
    setsid --wait "${runtime_launch[@]}" > "$fifo_path" 2>&1 &
  else
    "${runtime_launch[@]}" > "$fifo_path" 2>&1 &
  fi
  child=$!
  printf '%s\n' "$child" > "$replay_pid_file.tmp"
  mv -- "$replay_pid_file.tmp" "$replay_pid_file"
  wait "$child"
  status=$?
  printf '%s\n' "$status" > "$replay_status_file.tmp"
  mv -- "$replay_status_file.tmp" "$replay_status_file"
  return 0
}

run_replay_job &
replay_job_pid=$!
ready_attempts=0
while [[ ! -f "$replay_pid_file" ]]; do
  kill -0 "$replay_job_pid" 2>/dev/null || die 'replay launcher exited before publishing its PID'
  ready_attempts=$((ready_attempts + 1))
  (( ready_attempts <= 100 )) || die 'replay launcher did not publish its PID within ten seconds'
  sleep 0.1
done
replay_pid="$(awk 'NR == 1 {print; exit}' "$replay_pid_file")"
[[ "$replay_pid" =~ ^[1-9][0-9]*$ ]] || die "replay launcher published an invalid PID: $replay_pid"

run_pidstat_job() {
  local child status
  set +e
  stdbuf -oL -eL pidstat -h -u -r -d -w -p "$replay_pid" "$pidstat_interval" \
    > "$run_dir/resource-pidstat.log" 2>&1 &
  child=$!
  printf '%s\n' "$child" > "$pidstat_pid_file.tmp"
  mv -- "$pidstat_pid_file.tmp" "$pidstat_pid_file"
  wait "$child"
  status=$?
  printf '%s\n' "$status" > "$pidstat_status_file.tmp"
  mv -- "$pidstat_status_file.tmp" "$pidstat_status_file"
  return 0
}

run_pidstat_job &
pidstat_job_pid=$!
ready_attempts=0
while [[ ! -f "$pidstat_pid_file" ]]; do
  kill -0 "$pidstat_job_pid" 2>/dev/null || die 'pidstat launcher exited before publishing its PID'
  ready_attempts=$((ready_attempts + 1))
  (( ready_attempts <= 100 )) || die 'pidstat launcher did not publish its PID within ten seconds'
  sleep 0.1
done
pidstat_pid="$(awk 'NR == 1 {print; exit}' "$pidstat_pid_file")"
[[ "$pidstat_pid" =~ ^[1-9][0-9]*$ ]] || die "pidstat launcher published an invalid PID: $pidstat_pid"

{
  printf 'role\tpid\n'
  printf 'supervisor\t%s\n' "$$"
  printf 'replay\t%s\n' "$replay_pid"
  printf 'replay_launcher\t%s\n' "$replay_job_pid"
  printf 'log_stream\t%s\n' "$log_stream_pid"
  printf 'pidstat\t%s\n' "$pidstat_pid"
  printf 'pidstat_launcher\t%s\n' "$pidstat_job_pid"
  printf 'replay_separate_process_group\t%s\n' "$replay_process_group"
} > "$run_dir/pids.tsv"
chmod 0400 "$run_dir/pids.tsv"

supervisor_log "replay started pid=$replay_pid epochs=$start_epoch..$end_epoch duration_seconds=$duration_seconds"

send_replay_signal() {
  local signal_name="$1"
  if (( replay_process_group )); then
    kill -s "$signal_name" -- "-$replay_pid" 2>/dev/null || true
  else
    kill -s "$signal_name" -- "$replay_pid" 2>/dev/null || true
  fi
}

request_stop() {
  local reason="$1"
  local signal_name="$2"
  local now
  if [[ "$stop_reason" == 'replay-exit' ]]; then
    stop_reason="$reason"
    requested_signal="$signal_name"
    now="$(date '+%s')"
    termination_deadline=$((now + termination_grace_seconds))
    supervisor_log "requesting replay stop reason=$reason signal=$signal_name grace_seconds=$termination_grace_seconds"
    send_replay_signal "$signal_name"
  fi
}

trap 'request_stop signal-HUP HUP' HUP
trap 'request_stop signal-INT INT' INT
trap 'request_stop signal-TERM TERM' TERM

log_stream_ended_observed=0
pidstat_ended_observed=0
while [[ ! -f "$replay_status_file" ]]; do
  now_seconds="$(date '+%s')"
  if [[ -f "$log_stream_status_file" ]]; then
    if (( log_stream_ended_observed )) && [[ "$stop_reason" == 'replay-exit' ]]; then
      request_stop 'log-stream-failure' TERM
    fi
    log_stream_ended_observed=1
  fi
  if [[ -f "$pidstat_status_file" ]]; then
    if (( pidstat_ended_observed )) && [[ "$stop_reason" == 'replay-exit' ]]; then
      request_stop 'pidstat-failure' TERM
    fi
    pidstat_ended_observed=1
  fi
  if [[ "$stop_reason" == 'replay-exit' ]] && (( duration_seconds > 0 )) \
    && (( now_seconds - started_epoch_seconds >= duration_seconds )); then
    request_stop 'duration-limit' TERM
  elif [[ "$stop_reason" != 'replay-exit' ]] && (( now_seconds >= termination_deadline )); then
    supervisor_log 'replay did not stop during the grace period; sending KILL'
    send_replay_signal KILL
    termination_deadline=$((now_seconds + 86400))
  fi
  sleep 1 || true
done

replay_exit_status="$(awk 'NR == 1 {print; exit}' "$replay_status_file")"
[[ "$replay_exit_status" =~ ^[0-9]+$ ]] || die "replay launcher published an invalid exit status: $replay_exit_status"
set +e
wait "$replay_job_pid"
replay_job_exit_status=$?
set -e
replay_job_pid=''

kill -s TERM -- "$pidstat_pid" 2>/dev/null || true
monitor_wait_attempts=0
while [[ ! -f "$pidstat_status_file" ]] && (( monitor_wait_attempts < 50 )); do
  monitor_wait_attempts=$((monitor_wait_attempts + 1))
  sleep 0.1
done
if [[ ! -f "$pidstat_status_file" ]]; then
  kill -s KILL -- "$pidstat_pid" 2>/dev/null || true
  monitor_wait_attempts=0
  while [[ ! -f "$pidstat_status_file" ]] && (( monitor_wait_attempts < 20 )); do
    monitor_wait_attempts=$((monitor_wait_attempts + 1))
    sleep 0.1
  done
fi

log_wait_attempts=0
while [[ ! -f "$log_stream_status_file" ]] && (( log_wait_attempts < 50 )); do
  log_wait_attempts=$((log_wait_attempts + 1))
  sleep 0.1
done
if [[ ! -f "$log_stream_status_file" ]]; then
  kill -s TERM -- "$log_stream_pid" 2>/dev/null || true
fi

set +e
wait "$pidstat_job_pid"
pidstat_job_exit_status=$?
wait "$log_stream_pid"
log_stream_job_exit_status=$?
set -e
pidstat_exit_status="$(awk 'NR == 1 {print; exit}' "$pidstat_status_file")"
[[ "$pidstat_exit_status" =~ ^[0-9]+$ ]] || pidstat_exit_status='invalid'
log_stream_exit_status="$(awk 'NR == 1 {print; exit}' "$log_stream_status_file" 2>/dev/null)"
[[ "$log_stream_exit_status" =~ ^[0-9]+$ ]] || log_stream_exit_status='invalid'
pidstat_pid=''
pidstat_job_pid=''
log_stream_pid=''
rm -f -- "$fifo_path"
fifo_path=''

ended_utc="$(utc_now)"
ended_epoch_seconds="$(date '+%s')"
elapsed_seconds=$((ended_epoch_seconds - started_epoch_seconds))
resource_log_bytes="$(wc -c < "$run_dir/resource-pidstat.log" | tr -d '[:space:]')"

last_metrics_line="$(awk '/^generation_metrics / && /checkpoint_published=true/{line=$0} END{print line}' "$run_dir/generation-metrics.log")"
last_completed_epoch=''
last_completed_generation_digest=''
if [[ "$last_metrics_line" =~ ^generation_metrics[[:space:]]epoch=([0-9]+).*generation_digest=([0-9a-f]{64}).*checkpoint_published=true ]]; then
  last_completed_epoch="${BASH_REMATCH[1]}"
  last_completed_generation_digest="${BASH_REMATCH[2]}"
fi
generation_metrics_count="$(awk '/^generation_metrics /{count++} END{print count+0}' "$run_dir/generation-metrics.log")"

final_checkpoint_sha256='absent'
final_checkpoint_size='0'
checkpoint_boundary_status='none-published-this-run'
resume_checkpoint="$planned_anchor"
resume_checkpoint_sha256="$anchor_sha256"
resume_completed_epoch="$completed_epoch"
if [[ -e "$checkpoint_out" || -L "$checkpoint_out" ]]; then
  if [[ -L "$checkpoint_out" || ! -f "$checkpoint_out" || ! -s "$checkpoint_out" ]]; then
    checkpoint_boundary_status='invalid-output-object'
  else
    final_checkpoint_sha256="$(sha256_file "$checkpoint_out")"
    final_checkpoint_size="$(wc -c < "$checkpoint_out" | tr -d '[:space:]')"
    chmod 0400 "$checkpoint_out"
    if [[ -n "$last_completed_epoch" ]]; then
      checkpoint_boundary_status='bound-by-flushed-generation-metrics'
      resume_checkpoint="$checkpoint_out"
      resume_checkpoint_sha256="$final_checkpoint_sha256"
      resume_completed_epoch="$last_completed_epoch"
    else
      checkpoint_boundary_status='output-present-without-flushed-boundary-metrics'
    fi
  fi
fi

inputs_unchanged='true'
[[ "$(sha256_file "$anchor_checkpoint")" == "$anchor_sha256" ]] || inputs_unchanged='false'
[[ "$(sha256_file "$binary")" == "$binary_sha256" ]] || inputs_unchanged='false'
index=0
while (( index < ${#generation_manifest_paths[@]} )); do
  manifest_path="${generation_manifest_paths[$index]}"
  manifest_sha="${generation_manifest_sha256s[$index]}"
  if [[ ! -f "$manifest_path" || -L "$manifest_path" || "$(sha256_file "$manifest_path")" != "$manifest_sha" ]]; then
    inputs_unchanged='false'
  fi
  index=$((index + 1))
done

wrapper_exit_status="$replay_exit_status"
case "$stop_reason" in
  duration-limit) wrapper_exit_status=124 ;;
  signal-HUP) wrapper_exit_status=129 ;;
  signal-INT) wrapper_exit_status=130 ;;
  signal-TERM) wrapper_exit_status=143 ;;
  log-stream-failure|pidstat-failure) wrapper_exit_status=70 ;;
esac
if [[ "$checkpoint_boundary_status" == 'invalid-output-object' || "$inputs_unchanged" != 'true' ]]; then
  wrapper_exit_status=70
fi
if [[ "$log_stream_exit_status" != '0' ]]; then
  wrapper_exit_status=70
fi
if (( elapsed_seconds >= pidstat_interval )) && [[ "$resource_log_bytes" == '0' ]]; then
  wrapper_exit_status=70
fi

{
  printf 'schema\t%s\n' "$metadata_schema"
  printf 'started_utc\t%s\n' "$started_utc"
  printf 'ended_utc\t%s\n' "$ended_utc"
  printf 'elapsed_seconds\t%s\n' "$elapsed_seconds"
  printf 'stop_reason\t%s\n' "$stop_reason"
  printf 'forwarded_signal\t%s\n' "${requested_signal:-none}"
  printf 'replay_exit_status\t%s\n' "$replay_exit_status"
  printf 'replay_launcher_exit_status\t%s\n' "$replay_job_exit_status"
  printf 'wrapper_exit_status\t%s\n' "$wrapper_exit_status"
  printf 'pidstat_exit_status\t%s\n' "$pidstat_exit_status"
  printf 'pidstat_launcher_exit_status\t%s\n' "$pidstat_job_exit_status"
  printf 'resource_pidstat_log_bytes\t%s\n' "$resource_log_bytes"
  printf 'log_stream_exit_status\t%s\n' "$log_stream_exit_status"
  printf 'log_stream_launcher_exit_status\t%s\n' "$log_stream_job_exit_status"
  printf 'generation_metrics_count\t%s\n' "$generation_metrics_count"
  printf 'last_completed_epoch_this_run\t%s\n' "${last_completed_epoch:-none}"
  printf 'last_completed_generation_digest\t%s\n' "${last_completed_generation_digest:-none}"
  printf 'checkpoint_boundary_status\t%s\n' "$checkpoint_boundary_status"
  printf 'final_checkpoint_path\t%s\n' "$checkpoint_out"
  printf 'final_checkpoint_size\t%s\n' "$final_checkpoint_size"
  printf 'final_checkpoint_sha256\t%s\n' "$final_checkpoint_sha256"
  printf 'inputs_unchanged_after_run\t%s\n' "$inputs_unchanged"
  printf 'resume_checkpoint\t%s\n' "$resume_checkpoint"
  printf 'resume_checkpoint_sha256\t%s\n' "$resume_checkpoint_sha256"
  printf 'resume_completed_epoch\t%s\n' "$resume_completed_epoch"
} > "$run_dir/exit-status.tsv.tmp"
mv -- "$run_dir/exit-status.tsv.tmp" "$run_dir/exit-status.tsv"
chmod 0400 "$run_dir/exit-status.tsv"

{
  printf 'checkpoint=%q\n' "$resume_checkpoint"
  printf 'checkpoint_sha256=%q\n' "$resume_checkpoint_sha256"
  printf 'completed_epoch=%q\n' "$resume_completed_epoch"
} > "$run_dir/resume.env"
chmod 0400 "$run_dir/resume.env"

supervisor_log "replay finished replay_status=$replay_exit_status wrapper_status=$wrapper_exit_status stop_reason=$stop_reason"
supervisor_log "resume checkpoint=$resume_checkpoint completed_epoch=$resume_completed_epoch sha256=$resume_checkpoint_sha256"

trap - HUP INT TERM
exit "$wrapper_exit_status"
