#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
sync_script="$script_dir/sync-replay-compact.sh"
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/blockzilla-compact-sync-test.XXXXXX")"
trap 'rm -rf -- "$temporary_root"' EXIT

remote_root="$temporary_root/remote"
destination="$temporary_root/local"
fake_bin="$temporary_root/bin"
genesis="$temporary_root/genesis.bin"
mkdir -p -- "$remote_root/epoch-0" "$remote_root/epoch-1" "$fake_bin"

epoch_0_files=(
  archive-v2-blocks.zstd
  archive-v2-blocks.index
  archive-v2-meta.wincode
  registry.bin
  blockhash_registry.bin
)
epoch_1_files=(
  archive-v2-blocks.zstd
  archive-v2-blocks.index
  archive-v2-meta.wincode
  registry.bin
  blockhash_registry.bin
  prev_blockhash_tail.bin
)

for name in "${epoch_0_files[@]}"; do
  : > "$remote_root/epoch-0/$name"
done
for name in "${epoch_1_files[@]}"; do
  : > "$remote_root/epoch-1/$name"
done
truncate -s 74044326 "$remote_root/epoch-0/archive-v2-blocks.zstd"
truncate -s 22440532 "$remote_root/epoch-0/archive-v2-blocks.index"
truncate -s 99588 "$remote_root/epoch-0/archive-v2-meta.wincode"
truncate -s 14336 "$remote_root/epoch-0/registry.bin"
truncate -s 13809568 "$remote_root/epoch-0/blockhash_registry.bin"
printf 'BZV2HIX1\001\000\000\000\274\225\006\000\000\000\000\000' \
  | dd of="$remote_root/epoch-0/archive-v2-blocks.index" bs=1 conv=notrunc 2>/dev/null

truncate -s 200625731 "$remote_root/epoch-1/archive-v2-blocks.zstd"
truncate -s 22386920 "$remote_root/epoch-1/archive-v2-blocks.index"
truncate -s 47 "$remote_root/epoch-1/archive-v2-meta.wincode"
truncate -s 6752 "$remote_root/epoch-1/registry.bin"
truncate -s 13776544 "$remote_root/epoch-1/blockhash_registry.bin"
truncate -s 12000 "$remote_root/epoch-1/prev_blockhash_tail.bin"
printf 'BZV2HIX1\001\000\000\000\265\221\006\000\000\000\000\000' \
  | dd of="$remote_root/epoch-1/archive-v2-blocks.index" bs=1 conv=notrunc 2>/dev/null
printf 'source manifest must not be copied\n' > "$remote_root/epoch-0/archive-v2-generation.json"
for decoy in epoch.car shredding.wincode poh.wincode rpc.json; do
  printf 'must not be copied: %s\n' "$decoy" > "$remote_root/epoch-0/$decoy"
  printf 'must not be copied: %s\n' "$decoy" > "$remote_root/epoch-1/$decoy"
done
printf 'exact genesis fixture\n' > "$genesis"
truncate -s 132347 "$genesis"

cat > "$fake_bin/rsync" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
while (( $# > 2 )); do
  case "$1" in
    -rt|--partial|--progress|--)
      ;;
    -e)
      shift
      [[ "$1" == *' -p '* ]] || {
        printf 'rsync remote shell did not include a port: %s\n' "$1" >&2
        exit 2
      }
      ;;
    *)
      printf 'unsupported rsync argument in helper: %s\n' "$1" >&2
      exit 2
      ;;
  esac
  shift
done
source_path="${1#*:}"
if [[ -n "${FAKE_RSYNC_ALWAYS_FAIL:-}" ]]; then
  printf 'rsync: invalid path: simulated openrsync incompatibility\n' >&2
  exit 23
fi
if [[ -n "${FAKE_RSYNC_FAIL_ON_NAME:-}" \
  && "${source_path##*/}" == "$FAKE_RSYNC_FAIL_ON_NAME" \
  && ! -e "${FAKE_RSYNC_FAILURE_MARKER:?}" ]]; then
  printf 'partial' > "$2"
  : > "$FAKE_RSYNC_FAILURE_MARKER"
  exit 23
fi
cp -p -- "$source_path" "$2"
EOF
chmod +x "$fake_bin/rsync"

cat > "$fake_bin/scp" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
legacy=0
port=''
while (( $# > 2 )); do
  case "$1" in
    -O)
      legacy=1
      shift
      ;;
    -P)
      port="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      printf 'unsupported scp argument in helper: %s\n' "$1" >&2
      exit 2
      ;;
  esac
done
[[ "$legacy" == 1 && -n "$port" && $# == 2 ]]
if [[ -n "${FAKE_SCP_EXPECT_PORT:-}" && "$port" != "$FAKE_SCP_EXPECT_PORT" ]]; then
  printf 'unexpected scp port: expected %s, found %s\n' "$FAKE_SCP_EXPECT_PORT" "$port" >&2
  exit 2
fi
source_path="${1#*:}"
if [[ -n "${FAKE_SCP_LOG:-}" ]]; then
  printf '%s %s\n' "$port" "${source_path##*/}" >> "$FAKE_SCP_LOG"
fi
if [[ -n "${FAKE_SCP_FAIL_ON_NAME:-}" \
  && "${source_path##*/}" == "$FAKE_SCP_FAIL_ON_NAME" \
  && ! -e "${FAKE_SCP_FAILURE_MARKER:?}" ]]; then
  printf 'partial-scp' > "$2"
  : > "$FAKE_SCP_FAILURE_MARKER"
  exit 1
fi
if [[ -n "${FAKE_SCP_SHORT_ON_NAME:-}" \
  && "${source_path##*/}" == "$FAKE_SCP_SHORT_ON_NAME" ]]; then
  printf 'short-success' > "$2"
  exit 0
fi
cp -p -- "$source_path" "$2"
EOF
chmod +x "$fake_bin/scp"

cat > "$fake_bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
archive_dir=''
epoch=''
saw_blockhash=0
saw_tail=0
while (( $# > 0 )); do
  case "$1" in
    --archive-dir)
      archive_dir="$2"
      shift 2
      ;;
    --epoch)
      epoch="$2"
      shift 2
      ;;
    --file)
      case "$2" in
        blockhash_registry.bin) saw_blockhash=1 ;;
        prev_blockhash_tail.bin) saw_tail=1 ;;
      esac
      shift 2
      ;;
    *) shift ;;
  esac
done
[[ -n "$archive_dir" && -n "$epoch" && "$saw_blockhash" == 1 ]]
if [[ "$epoch" == 0 ]]; then
  [[ -f "$archive_dir/genesis.bin" && "$saw_tail" == 0 ]]
else
  [[ ! -e "$archive_dir/genesis.bin" && "$saw_tail" == 1 ]]
fi
printf '{"created_by":"fake-gateway","epoch":%s}\n' "$epoch" > "$archive_dir/archive-v2-generation.json"
EOF
chmod +x "$fake_bin/cargo"

cat > "$fake_bin/shasum" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '45296998a6f8e2a784db5d9f95e18fc23f70441a1039446801089879b08c7ef0  %s\n' "${!#}"
EOF
chmod +x "$fake_bin/shasum"

export RSYNC_BIN="$fake_bin/rsync"
export SCP_BIN="$fake_bin/scp"
export CARGO_BIN="$fake_bin/cargo"

PATH="$fake_bin:$PATH" \
  "$sync_script" \
  --destination "$destination" \
  --source-root "fixture@nas:$remote_root" \
  --genesis-bin "$genesis"

for name in "${epoch_0_files[@]}" genesis.bin archive-v2-generation.json; do
  [[ -f "$destination/epoch-0/$name" ]]
done
for name in "${epoch_1_files[@]}" archive-v2-generation.json; do
  [[ -f "$destination/epoch-1/$name" ]]
done
[[ ! -e "$destination/epoch-1/genesis.bin" ]]
grep -q 'fake-gateway' "$destination/epoch-0/archive-v2-generation.json"
grep -q '^ssh_port=22$' "$destination/epoch-0/.blockzilla-compact-sync-v1"
for decoy in epoch.car shredding.wincode poh.wincode rpc.json; do
  [[ ! -e "$destination/epoch-0/$decoy" ]]
  [[ ! -e "$destination/epoch-1/$decoy" ]]
done

if PATH="$fake_bin:$PATH" \
  "$sync_script" \
  --destination "$destination" \
  --source-root "fixture@nas:$remote_root" \
  --genesis-bin "$genesis" >"$temporary_root/retry.out" 2>"$temporary_root/retry.err"; then
  printf 'expected retry with an existing manifest to fail\n' >&2
  exit 1
fi
grep -q 'refusing to overwrite existing manifest' "$temporary_root/retry.err"

resume_destination="$temporary_root/resume-local"
failure_marker="$temporary_root/failed-once"
if FAKE_RSYNC_FAIL_ON_NAME='archive-v2-meta.wincode' \
  FAKE_RSYNC_FAILURE_MARKER="$failure_marker" \
  "$sync_script" \
  --destination "$resume_destination" \
  --source-root "fixture@nas:$remote_root" \
  --epoch 1 \
  --transport rsync >"$temporary_root/interrupted.out" 2>"$temporary_root/interrupted.err"; then
  printf 'expected the injected interrupted transfer to fail\n' >&2
  exit 1
fi
[[ -f "$resume_destination/epoch-1/.blockzilla-compact-sync-v1" ]]
[[ ! -e "$resume_destination/epoch-1/archive-v2-generation.json" ]]
[[ ! -e "$resume_destination/epoch-1/archive-v2-meta.wincode" ]]
[[ -f "$resume_destination/epoch-1/.blockzilla-rsync-partial/archive-v2-meta.wincode.partial" ]]

"$sync_script" \
  --destination "$resume_destination" \
  --source-root "fixture@nas:$remote_root" \
  --epoch 1 \
  --transport rsync
grep -q 'fake-gateway' "$resume_destination/epoch-1/archive-v2-generation.json"

auto_destination="$temporary_root/auto-local"
scp_log="$temporary_root/scp.log"
FAKE_RSYNC_ALWAYS_FAIL=1 \
FAKE_SCP_EXPECT_PORT=35022 \
FAKE_SCP_LOG="$scp_log" \
  "$sync_script" \
  --destination "$auto_destination" \
  --source-root "fixture@nas:$remote_root" \
  --epoch 1 \
  --ssh-port 35022
grep -q 'fake-gateway' "$auto_destination/epoch-1/archive-v2-generation.json"
[[ "$(wc -l < "$scp_log" | tr -d '[:space:]')" == 6 ]]
grep -q '^35022 archive-v2-blocks.zstd$' "$scp_log"

scp_resume_destination="$temporary_root/scp-resume-local"
scp_failure_marker="$temporary_root/scp-failed-once"
# A failed run from the original helper may already have its port-22 marker.
# It remains safely resumable on port 22, but cannot be reused for another
# endpoint port.
mkdir -p -- "$scp_resume_destination/epoch-1"
printf '%s\n' \
  'format=blockzilla-replay-compact-sync-v1' \
  'epoch=1' \
  "source=fixture@nas:$remote_root/epoch-1" \
  'cluster_id=mainnet-beta' \
  'slots_per_epoch=432000' \
  'generation_id=epoch-1-replay-compact' \
  'genesis_sha256=none' \
  > "$scp_resume_destination/epoch-1/.blockzilla-compact-sync-v1"
if FAKE_SCP_FAIL_ON_NAME='archive-v2-meta.wincode' \
  FAKE_SCP_FAILURE_MARKER="$scp_failure_marker" \
  "$sync_script" \
  --destination "$scp_resume_destination" \
  --source-root "fixture@nas:$remote_root" \
  --epoch 1 \
  --transport scp >"$temporary_root/scp-interrupted.out" 2>"$temporary_root/scp-interrupted.err"; then
  printf 'expected the injected interrupted scp transfer to fail\n' >&2
  exit 1
fi
[[ ! -e "$scp_resume_destination/epoch-1/archive-v2-meta.wincode" ]]
[[ -f "$scp_resume_destination/epoch-1/.blockzilla-rsync-partial/archive-v2-meta.wincode.partial" ]]

if "$sync_script" \
  --destination "$scp_resume_destination" \
  --source-root "fixture@nas:$remote_root" \
  --epoch 1 \
  --transport scp \
  --ssh-port 23 >"$temporary_root/port-mismatch.out" 2>"$temporary_root/port-mismatch.err"; then
  printf 'expected a legacy port-22 marker to reject another port\n' >&2
  exit 1
fi
grep -q 'sync marker does not match this invocation' "$temporary_root/port-mismatch.err"

"$sync_script" \
  --destination "$scp_resume_destination" \
  --source-root "fixture@nas:$remote_root" \
  --epoch 1 \
  --transport scp
grep -q 'fake-gateway' "$scp_resume_destination/epoch-1/archive-v2-generation.json"

short_destination="$temporary_root/short-local"
if FAKE_SCP_SHORT_ON_NAME='archive-v2-meta.wincode' \
  "$sync_script" \
  --destination "$short_destination" \
  --source-root "fixture@nas:$remote_root" \
  --epoch 1 \
  --transport scp >"$temporary_root/short.out" 2>"$temporary_root/short.err"; then
  printf 'expected a successful but short transfer to fail size validation\n' >&2
  exit 1
fi
grep -q 'unexpected size' "$temporary_root/short.err"
[[ ! -e "$short_destination/epoch-1/archive-v2-meta.wincode" ]]
[[ -f "$short_destination/epoch-1/.blockzilla-rsync-partial/archive-v2-meta.wincode.partial" ]]
[[ ! -e "$short_destination/epoch-1/archive-v2-generation.json" ]]

printf '\000' \
  | dd of="$remote_root/epoch-1/archive-v2-blocks.index" bs=1 seek=12 conv=notrunc 2>/dev/null
bad_rows_destination="$temporary_root/bad-rows-local"
if "$sync_script" \
  --destination "$bad_rows_destination" \
  --source-root "fixture@nas:$remote_root" \
  --epoch 1 >"$temporary_root/bad-rows.out" 2>"$temporary_root/bad-rows.err"; then
  printf 'expected the wrong index row count to fail\n' >&2
  exit 1
fi
grep -q 'unexpected row count' "$temporary_root/bad-rows.err"
[[ ! -e "$bad_rows_destination/epoch-1/archive-v2-generation.json" ]]

if "$sync_script" \
  --destination "$temporary_root/unsafe-source-local" \
  --source-root 'fixture@nas:/unsafe path' \
  --epoch 1 >"$temporary_root/unsafe-source.out" 2>"$temporary_root/unsafe-source.err"; then
  printf 'expected unsafe SSH source syntax to fail\n' >&2
  exit 1
fi
grep -q 'unsafe absolute remote path' "$temporary_root/unsafe-source.err"

for invalid_port in 0 65536 invalid; do
  if "$sync_script" \
    --destination "$temporary_root/invalid-port-local" \
    --epoch 1 \
    --ssh-port "$invalid_port" \
    >"$temporary_root/invalid-port.out" \
    2>"$temporary_root/invalid-port.err"; then
    printf 'expected invalid SSH port to fail: %s\n' "$invalid_port" >&2
    exit 1
  fi
  grep -q '\--ssh-port must be an integer from 1 through 65535' "$temporary_root/invalid-port.err"
done

if "$sync_script" \
  --destination "$temporary_root/invalid-transport-local" \
  --epoch 1 \
  --transport ftp \
  >"$temporary_root/invalid-transport.out" \
  2>"$temporary_root/invalid-transport.err"; then
  printf 'expected invalid transport to fail\n' >&2
  exit 1
fi
grep -q '\--transport must be auto, rsync, or scp' "$temporary_root/invalid-transport.err"

for forbidden_option in --genesis-sha256 --cluster-id --slots-per-epoch --car --rpc-url; do
  if "$sync_script" \
    --destination "$temporary_root/forbidden-option-local" \
    --epoch 1 \
    "$forbidden_option" forbidden \
    >"$temporary_root/forbidden-option.out" \
    2>"$temporary_root/forbidden-option.err"; then
    printf 'expected forbidden option to fail: %s\n' "$forbidden_option" >&2
    exit 1
  fi
  grep -q 'unknown argument' "$temporary_root/forbidden-option.err"
done

if "$sync_script" \
  --destination "$temporary_root/missing-source-local" \
  --epoch 1 \
  >"$temporary_root/missing-source.out" \
  2>"$temporary_root/missing-source.err"; then
  printf 'expected a missing source root to fail\n' >&2
  exit 1
fi
grep -q '\--source-root is required' "$temporary_root/missing-source.err"

printf 'sync-replay-compact self-test passed\n'
