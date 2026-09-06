#!/usr/bin/env bash
set -euo pipefail

program_name="${0##*/}"

readonly default_genesis_bin='/private/tmp/mainnet-genesis/genesis.bin'
readonly mainnet_genesis_sha256='45296998a6f8e2a784db5d9f95e18fc23f70441a1039446801089879b08c7ef0'
readonly mainnet_genesis_size=132347
readonly sync_marker_name='.blockzilla-compact-sync-v1'
readonly manifest_name='archive-v2-generation.json'

destination=''
source_root=''
epoch_0_source=''
epoch_1_source=''
epoch_selection='both'
genesis_bin="$default_genesis_bin"
readonly genesis_sha256="$mainnet_genesis_sha256"
readonly cluster_id='mainnet-beta'
readonly slots_per_epoch=432000
epoch_0_generation_id='epoch-0-replay-compact'
epoch_1_generation_id='epoch-1-replay-compact'
transport='auto'
ssh_port='22'
rsync_bin="${RSYNC_BIN:-rsync}"
scp_bin="${SCP_BIN:-scp}"
ssh_bin="${SSH_BIN:-ssh}"
cargo_bin="${CARGO_BIN:-cargo}"

usage() {
  cat <<'EOF'
Fetch the replay-minimal Blockzilla Compact generations for epochs 0 and 1.

Usage:
  scripts/sync-replay-compact.sh --destination DIR [options]

Required:
  --destination DIR              Local root. Epochs are stored in epoch-0/epoch-1.
  --source-root SSH_SOURCE       Parent SSH/rsync source containing epoch-0 and
                                 epoch-1 directories.

Source selection:
  --epoch-0-source SSH_SOURCE    Override the complete epoch-0 source directory.
  --epoch-1-source SSH_SOURCE    Override the complete epoch-1 source directory.
  --epoch 0|1|both               Fetch one epoch or both. Default: both.
  --transport auto|rsync|scp     Transfer method. Default: auto (try rsync,
                                 then legacy-protocol scp if rsync fails).
  --ssh-port PORT                Valid SSH port used by both transports.
                                 Default: 22.

Genesis and manifest identity:
  --genesis-bin FILE             Exact local genesis.bin copied into epoch 0.
                                 Default: /private/tmp/mainnet-genesis/genesis.bin
  --epoch-0-generation-id ID     Default: epoch-0-replay-compact.
  --epoch-1-generation-id ID     Default: epoch-1-replay-compact.

  -h, --help                     Show this help.

The transfer is resumable at file boundaries. It requests only the exact
Compact files needed by replay, never downloads a source manifest, and never
uses deletion flags. Each file is downloaded to its own partial path and moved
atomically into place only after its pinned size is verified. A destination
epoch directory is created with a source-binding marker; a nonempty unmarked
directory is rejected. Existing completion manifests are never overwritten.
After all selected epochs pass local checks, the Archive V2 gateway validates
the structure, hashes the files, and atomically creates each manifest.
EOF
}

die() {
  printf '%s: %s\n' "$program_name" "$*" >&2
  exit 1
}

require_option_value() {
  local option="$1"
  local value="${2-}"
  [[ -n "$value" ]] || die "$option requires a nonempty value"
}

while (( $# > 0 )); do
  case "$1" in
    --destination)
      require_option_value "$1" "${2-}"
      destination="$2"
      shift 2
      ;;
    --source-root)
      require_option_value "$1" "${2-}"
      source_root="$2"
      shift 2
      ;;
    --epoch-0-source)
      require_option_value "$1" "${2-}"
      epoch_0_source="$2"
      shift 2
      ;;
    --epoch-1-source)
      require_option_value "$1" "${2-}"
      epoch_1_source="$2"
      shift 2
      ;;
    --epoch)
      require_option_value "$1" "${2-}"
      epoch_selection="$2"
      shift 2
      ;;
    --transport)
      require_option_value "$1" "${2-}"
      transport="$2"
      shift 2
      ;;
    --ssh-port)
      require_option_value "$1" "${2-}"
      ssh_port="$2"
      shift 2
      ;;
    --genesis-bin)
      require_option_value "$1" "${2-}"
      genesis_bin="$2"
      shift 2
      ;;
    --epoch-0-generation-id)
      require_option_value "$1" "${2-}"
      epoch_0_generation_id="$2"
      shift 2
      ;;
    --epoch-1-generation-id)
      require_option_value "$1" "${2-}"
      epoch_1_generation_id="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
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

[[ -n "$destination" ]] || die '--destination is required'
[[ "$destination" != '/' ]] || die 'refusing to use / as the destination'
case "$epoch_selection" in
  0|1|both) ;;
  *) die '--epoch must be 0, 1, or both' ;;
esac
case "$transport" in
  auto|rsync|scp) ;;
  *) die '--transport must be auto, rsync, or scp' ;;
esac
[[ "$ssh_port" =~ ^[0-9]+$ && ${#ssh_port} -le 5 ]] \
  || die '--ssh-port must be an integer from 1 through 65535'
ssh_port=$((10#$ssh_port))
(( ssh_port >= 1 && ssh_port <= 65535 )) || die '--ssh-port must be an integer from 1 through 65535'
[[ -n "$source_root" ]] || die '--source-root is required'

validate_single_line() {
  local name="$1"
  local value="$2"
  [[ -n "$value" ]] || die "$name must not be empty"
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "$name must be one line"
}

validate_single_line '--source-root' "$source_root"
validate_single_line '--epoch-0-generation-id' "$epoch_0_generation_id"
validate_single_line '--epoch-1-generation-id' "$epoch_1_generation_id"

join_remote_source() {
  local root="$1"
  local child="$2"
  printf '%s/%s' "${root%/}" "$child"
}

if [[ -z "$epoch_0_source" ]]; then
  epoch_0_source="$(join_remote_source "$source_root" 'epoch-0')"
fi
if [[ -z "$epoch_1_source" ]]; then
  epoch_1_source="$(join_remote_source "$source_root" 'epoch-1')"
fi

validate_remote_source() {
  local option="$1"
  local source="$2"
  local host
  local remote_path

  validate_single_line "$option" "$source"
  [[ "$source" == *:* ]] || die "$option must be an SSH source such as user@host:/absolute/path"
  host="${source%%:*}"
  remote_path="${source#*:}"
  [[ "$host" =~ ^([A-Za-z0-9._-]+@)?[A-Za-z0-9._-]+$ ]] || die "$option has an unsafe SSH host"
  [[ "$remote_path" =~ ^/[A-Za-z0-9._/@+-]+$ ]] || die "$option has an unsafe absolute remote path"
}

validate_remote_source '--epoch-0-source' "$epoch_0_source"
validate_remote_source '--epoch-1-source' "$epoch_1_source"

if [[ "$transport" == 'auto' || "$transport" == 'rsync' ]]; then
  command -v "$rsync_bin" >/dev/null 2>&1 || die "rsync command not found: $rsync_bin"
  command -v "$ssh_bin" >/dev/null 2>&1 || die "ssh command not found: $ssh_bin"
fi
if [[ "$transport" == 'auto' || "$transport" == 'scp' ]]; then
  command -v "$scp_bin" >/dev/null 2>&1 || die "scp command not found: $scp_bin"
fi
command -v "$cargo_bin" >/dev/null 2>&1 || die "cargo command not found: $cargo_bin"
command -v cmp >/dev/null 2>&1 || die 'cmp is required'

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

file_size() {
  local path="$1"
  wc -c < "$path" | tr -d '[:space:]'
}

verify_regular_nonempty_file() {
  local path="$1"
  [[ ! -L "$path" ]] || die "refusing symlink: $path"
  [[ -f "$path" ]] || die "required regular file is missing: $path"
  [[ -s "$path" ]] || die "required file is empty: $path"
}

path_exists() {
  [[ -e "$1" || -L "$1" ]]
}

verify_exact_size() {
  local path="$1"
  local expected="$2"
  local actual
  actual="$(file_size "$path")"
  [[ "$actual" == "$expected" ]] || die "unexpected size for $path: expected $expected bytes, found $actual"
}

epoch_is_selected() {
  local epoch="$1"
  [[ "$epoch_selection" == 'both' || "$epoch_selection" == "$epoch" ]]
}

genesis_actual_sha256=''
if epoch_is_selected 0; then
  verify_regular_nonempty_file "$genesis_bin"
  [[ "$genesis_sha256" =~ ^[0-9a-f]{64}$ ]] || die 'configured genesis SHA-256 must be 64 lowercase hexadecimal characters'
  genesis_actual_sha256="$(sha256_file "$genesis_bin")"
  [[ "$genesis_actual_sha256" == "$genesis_sha256" ]] || die "genesis SHA-256 mismatch: expected $genesis_sha256, found $genesis_actual_sha256"
  verify_exact_size "$genesis_bin" "$mainnet_genesis_size"
fi

if path_exists "$destination"; then
  [[ ! -L "$destination" && -d "$destination" ]] || die "destination must be a real directory, not a symlink: $destination"
else
  mkdir -p -- "$destination"
fi

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "$script_dir/.." && pwd -P)"
genesis_temporary=''

cleanup() {
  if [[ -n "$genesis_temporary" ]]; then
    rm -f -- "$genesis_temporary"
  fi
}
trap cleanup EXIT

epoch_dir() {
  printf '%s/epoch-%s' "${destination%/}" "$1"
}

source_for_epoch() {
  case "$1" in
    0) printf '%s' "$epoch_0_source" ;;
    1) printf '%s' "$epoch_1_source" ;;
    *) die "internal error: unsupported epoch $1" ;;
  esac
}

generation_id_for_epoch() {
  case "$1" in
    0) printf '%s' "$epoch_0_generation_id" ;;
    1) printf '%s' "$epoch_1_generation_id" ;;
    *) die "internal error: unsupported epoch $1" ;;
  esac
}

expected_marker() {
  local epoch="$1"
  local source="$2"
  local generation_id="$3"
  local epoch_genesis_sha256='none'
  if [[ "$epoch" == 0 ]]; then
    epoch_genesis_sha256="$genesis_actual_sha256"
  fi
  printf '%s\n' \
    'format=blockzilla-replay-compact-sync-v1' \
    "epoch=$epoch" \
    "source=$source" \
    "cluster_id=$cluster_id" \
    "slots_per_epoch=$slots_per_epoch" \
    "generation_id=$generation_id" \
    "genesis_sha256=$epoch_genesis_sha256" \
    "ssh_port=$ssh_port"
}

expected_legacy_port_22_marker() {
  local epoch="$1"
  local source="$2"
  local generation_id="$3"
  local epoch_genesis_sha256='none'
  if [[ "$epoch" == 0 ]]; then
    epoch_genesis_sha256="$genesis_actual_sha256"
  fi
  printf '%s\n' \
    'format=blockzilla-replay-compact-sync-v1' \
    "epoch=$epoch" \
    "source=$source" \
    "cluster_id=$cluster_id" \
    "slots_per_epoch=$slots_per_epoch" \
    "generation_id=$generation_id" \
    "genesis_sha256=$epoch_genesis_sha256"
}

prepare_epoch_directory() {
  local epoch="$1"
  local directory
  local source
  local generation_id
  local marker
  local expected
  local legacy_port_22
  local actual
  local first_entry

  directory="$(epoch_dir "$epoch")"
  source="$(source_for_epoch "$epoch")"
  generation_id="$(generation_id_for_epoch "$epoch")"
  marker="$directory/$sync_marker_name"
  expected="$(expected_marker "$epoch" "$source" "$generation_id")"
  legacy_port_22="$(expected_legacy_port_22_marker "$epoch" "$source" "$generation_id")"

  if path_exists "$directory"; then
    [[ ! -L "$directory" && -d "$directory" ]] || die "epoch destination must be a real directory: $directory"
  else
    mkdir -- "$directory"
  fi
  ! path_exists "$directory/$manifest_name" || die "refusing to overwrite existing manifest: $directory/$manifest_name"

  if path_exists "$marker"; then
    [[ ! -L "$marker" && -f "$marker" ]] || die "invalid sync marker: $marker"
    actual="$(<"$marker")"
    if [[ "$actual" != "$expected" ]]; then
      # Markers written by the first sync-helper version did not record the
      # port. They are compatible only with that version's fixed port 22.
      [[ "$ssh_port" == 22 && "$actual" == "$legacy_port_22" ]] \
        || die "sync marker does not match this invocation: $marker"
    fi
  else
    first_entry="$(find "$directory" -mindepth 1 -maxdepth 1 -print -quit)"
    [[ -z "$first_entry" ]] || die "refusing nonempty unmarked epoch directory: $directory"
    (umask 077 && printf '%s\n' "$expected" > "$marker")
  fi
}

for epoch in 0 1; do
  if epoch_is_selected "$epoch"; then
    prepare_epoch_directory "$epoch"
  fi
done

copy_genesis() {
  local directory
  local target
  local temporary

  directory="$(epoch_dir 0)"
  target="$directory/genesis.bin"
  if path_exists "$target"; then
    [[ ! -L "$target" && -f "$target" ]] || die "invalid existing genesis target: $target"
    cmp -s -- "$genesis_bin" "$target" || die "existing epoch-0 genesis differs from $genesis_bin"
    return
  fi

  temporary="$directory/.genesis.bin.partial.$$"
  ! path_exists "$temporary" || die "temporary genesis path already exists: $temporary"
  genesis_temporary="$temporary"
  cp -p -- "$genesis_bin" "$temporary"
  cmp -s -- "$genesis_bin" "$temporary" || die 'copied genesis verification failed'
  mv -n -- "$temporary" "$target"
  cmp -s -- "$genesis_bin" "$target" || die "epoch-0 genesis target changed concurrently: $target"
  genesis_temporary=''
}

sync_one_file() {
  local epoch="$1"
  local name="$2"
  local source
  local directory
  local target
  local partial_directory
  local partial
  local expected_size
  local remote

  source="$(source_for_epoch "$epoch")"
  directory="$(epoch_dir "$epoch")"
  target="$directory/$name"
  partial_directory="$directory/.blockzilla-rsync-partial"
  partial="$partial_directory/$name.partial"
  expected_size="$(expected_size_for_file "$epoch" "$name")"
  remote="$source/$name"

  if path_exists "$target"; then
    [[ ! -L "$target" && -f "$target" ]] || die "invalid existing transfer target: $target"
    verify_exact_size "$target" "$expected_size"
    printf 'reuse epoch=%s file=%s bytes=%s\n' "$epoch" "$name" "$expected_size"
    return
  fi

  if path_exists "$partial_directory"; then
    [[ ! -L "$partial_directory" && -d "$partial_directory" ]] \
      || die "invalid partial-transfer directory: $partial_directory"
  else
    mkdir -- "$partial_directory"
  fi
  if path_exists "$partial"; then
    [[ ! -L "$partial" && -f "$partial" ]] || die "invalid partial transfer: $partial"
  fi

  printf 'sync epoch=%s file=%s transport=%s port=%s\n' "$epoch" "$name" "$transport" "$ssh_port"
  case "$transport" in
    rsync)
      transfer_with_rsync "$remote" "$partial"
      ;;
    scp)
      transfer_with_scp "$remote" "$partial"
      ;;
    auto)
      if transfer_with_rsync "$remote" "$partial"; then
        :
      else
        printf 'rsync failed; retrying file with legacy scp: epoch=%s file=%s\n' \
          "$epoch" "$name" >&2
        transfer_with_scp "$remote" "$partial"
      fi
      ;;
  esac

  verify_regular_nonempty_file "$partial"
  verify_exact_size "$partial" "$expected_size"
  mv -n -- "$partial" "$target"
  [[ ! -e "$partial" ]] || die "transfer target appeared concurrently: $target"
  verify_regular_nonempty_file "$target"
  verify_exact_size "$target" "$expected_size"
}

transfer_with_rsync() {
  local remote="$1"
  local partial="$2"
  "$rsync_bin" \
    -rt \
    --partial \
    --progress \
    -e "$ssh_bin -p $ssh_port" \
    -- \
    "$remote" \
    "$partial"
}

transfer_with_scp() {
  local remote="$1"
  local partial="$2"
  # -O forces the legacy SCP protocol. The NAS accepts it even when its rsync
  # daemon cannot interoperate with the macOS-provided openrsync client.
  "$scp_bin" -O -P "$ssh_port" -- "$remote" "$partial"
}

expected_size_for_file() {
  local epoch="$1"
  local name="$2"
  case "$epoch:$name" in
    0:archive-v2-blocks.zstd) printf '%s' 74044326 ;;
    0:archive-v2-blocks.index) printf '%s' 22440532 ;;
    0:archive-v2-meta.wincode) printf '%s' 99588 ;;
    0:registry.bin) printf '%s' 14336 ;;
    0:blockhash_registry.bin) printf '%s' 13809568 ;;
    1:archive-v2-blocks.zstd) printf '%s' 200625731 ;;
    1:archive-v2-blocks.index) printf '%s' 22386920 ;;
    1:archive-v2-meta.wincode) printf '%s' 47 ;;
    1:registry.bin) printf '%s' 6752 ;;
    1:blockhash_registry.bin) printf '%s' 13776544 ;;
    1:prev_blockhash_tail.bin) printf '%s' 12000 ;;
    *) die "internal error: no pinned size for epoch $epoch file $name" ;;
  esac
}

readonly -a epoch_0_files=(
  'archive-v2-blocks.zstd'
  'archive-v2-blocks.index'
  'archive-v2-meta.wincode'
  'registry.bin'
  'blockhash_registry.bin'
)
readonly -a epoch_1_files=(
  'archive-v2-blocks.zstd'
  'archive-v2-blocks.index'
  'archive-v2-meta.wincode'
  'registry.bin'
  'blockhash_registry.bin'
  'prev_blockhash_tail.bin'
)

if epoch_is_selected 0; then
  copy_genesis
  for name in "${epoch_0_files[@]}"; do
    sync_one_file 0 "$name"
  done
fi
if epoch_is_selected 1; then
  for name in "${epoch_1_files[@]}"; do
    sync_one_file 1 "$name"
  done
fi

verify_allowed_entries() {
  local epoch="$1"
  local directory
  local entry
  local name

  directory="$(epoch_dir "$epoch")"
  while IFS= read -r -d '' entry; do
    name="${entry##*/}"
    case "$name" in
      "$sync_marker_name")
        [[ ! -L "$entry" && -f "$entry" ]] || die "invalid sync marker: $entry"
        ;;
      .blockzilla-rsync-partial)
        [[ ! -L "$entry" && -d "$entry" ]] || die "invalid partial-transfer directory: $entry"
        ;;
      archive-v2-blocks.zstd|archive-v2-blocks.index|archive-v2-meta.wincode|registry.bin|blockhash_registry.bin)
        ;;
      genesis.bin)
        [[ "$epoch" == 0 ]] || die "epoch 1 must not contain genesis.bin: $entry"
        ;;
      prev_blockhash_tail.bin)
        [[ "$epoch" == 1 ]] || die "epoch 0 must not contain prev_blockhash_tail.bin: $entry"
        ;;
      *)
        die "unexpected entry in replay-minimal epoch directory: $entry"
        ;;
    esac
  done < <(find "$directory" -mindepth 1 -maxdepth 1 -print0)
}

read_hot_index_row_count() {
  local path="$1"
  local prefix
  local -a bytes
  local byte
  local multiplier=1
  local value=0

  prefix="$(od -An -v -tx1 -N 12 -- "$path" | tr -d '[:space:]')"
  [[ "$prefix" == '425a56324849583101000000' ]] || die "invalid Archive V2 hot-index header: $path"
  bytes=( $(od -An -v -tu1 -j 12 -N 8 -- "$path") )
  (( ${#bytes[@]} == 8 )) || die "truncated Archive V2 hot-index row count: $path"
  for byte in "${bytes[@]}"; do
    value=$((value + byte * multiplier))
    multiplier=$((multiplier * 256))
  done
  printf '%s' "$value"
}

verify_hot_index_rows() {
  local path="$1"
  local expected="$2"
  local actual
  actual="$(read_hot_index_row_count "$path")"
  [[ "$actual" == "$expected" ]] || die "unexpected row count for $path: expected $expected, found $actual"
}

verify_epoch_files() {
  local epoch="$1"
  local directory
  local name

  directory="$(epoch_dir "$epoch")"
  verify_allowed_entries "$epoch"
  if [[ "$epoch" == 0 ]]; then
    for name in "${epoch_0_files[@]}" genesis.bin; do
      verify_regular_nonempty_file "$directory/$name"
    done
    verify_exact_size "$directory/archive-v2-blocks.zstd" 74044326
    verify_exact_size "$directory/archive-v2-blocks.index" 22440532
    verify_exact_size "$directory/archive-v2-meta.wincode" 99588
    verify_exact_size "$directory/registry.bin" 14336
    verify_exact_size "$directory/blockhash_registry.bin" 13809568
    verify_exact_size "$directory/genesis.bin" "$mainnet_genesis_size"
    verify_hot_index_rows "$directory/archive-v2-blocks.index" 431548
  else
    for name in "${epoch_1_files[@]}"; do
      verify_regular_nonempty_file "$directory/$name"
    done
    ! path_exists "$directory/genesis.bin" || die "epoch 1 must not contain genesis.bin: $directory/genesis.bin"
    verify_exact_size "$directory/archive-v2-blocks.zstd" 200625731
    verify_exact_size "$directory/archive-v2-blocks.index" 22386920
    verify_exact_size "$directory/archive-v2-meta.wincode" 47
    verify_exact_size "$directory/registry.bin" 6752
    verify_exact_size "$directory/blockhash_registry.bin" 13776544
    verify_exact_size "$directory/prev_blockhash_tail.bin" 12000
    verify_hot_index_rows "$directory/archive-v2-blocks.index" 430517
  fi
}

for epoch in 0 1; do
  if epoch_is_selected "$epoch"; then
    verify_epoch_files "$epoch"
  fi
done

readonly message_schema_marker='archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker'

# Mainnet epochs 0 and 1 were built before the current message grammar. State
# that in the generation rather than inferring it from content hashes: readers
# verify this 87-byte file directly. It is also listed in the manifest, so a
# reader that still resolves the marker through the manifest honours it too.
write_message_schema_marker() {
  local directory="$1"
  local source_marker="$repo_root/crates/blockzilla-replay/assets/$message_schema_marker"
  verify_regular_nonempty_file "$source_marker"
  if [[ ! -e "$directory/$message_schema_marker" ]]; then
    cp -- "$source_marker" "$directory/$message_schema_marker"
  fi
  verify_regular_nonempty_file "$directory/$message_schema_marker"
}

generate_manifest() {
  local epoch="$1"
  local directory
  local generation_id
  local manifest

  directory="$(epoch_dir "$epoch")"
  generation_id="$(generation_id_for_epoch "$epoch")"
  manifest="$directory/$manifest_name"
  ! path_exists "$manifest" || die "refusing to overwrite existing manifest: $manifest"

  write_message_schema_marker "$directory"
  printf 'validate and manifest epoch=%s directory=%s\n' "$epoch" "$directory"
  if [[ "$epoch" == 0 ]]; then
    (
      cd -- "$repo_root"
      "$cargo_bin" run --release --locked -p blockzilla-archive-gateway -- \
        generate-manifest \
        --archive-dir "$directory" \
        --cluster-id "$cluster_id" \
        --epoch 0 \
        --generation-id "$generation_id" \
        --slots-per-epoch "$slots_per_epoch" \
        --file blockhash_registry.bin \
        --file "$message_schema_marker"
    )
  else
    (
      cd -- "$repo_root"
      "$cargo_bin" run --release --locked -p blockzilla-archive-gateway -- \
        generate-manifest \
        --archive-dir "$directory" \
        --cluster-id "$cluster_id" \
        --epoch 1 \
        --generation-id "$generation_id" \
        --slots-per-epoch "$slots_per_epoch" \
        --file blockhash_registry.bin \
        --file prev_blockhash_tail.bin \
        --file "$message_schema_marker"
    )
  fi
  verify_regular_nonempty_file "$manifest"
}

for epoch in 0 1; do
  if epoch_is_selected "$epoch"; then
    generate_manifest "$epoch"
  fi
done

printf 'Compact replay input is ready under %s\n' "$destination"
