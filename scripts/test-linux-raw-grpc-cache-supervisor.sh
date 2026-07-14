#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
supervisor=$repo_root/scripts/linux-raw-grpc-recorder.sh
fixture_root=$(mktemp -d "${TMPDIR:-/tmp}/blockzilla-cache-test.XXXXXX")
trap 'rm -rf "$fixture_root"' EXIT

export BLOCKZILLA_RAW_CACHE_MODE=b2-generations
export BLOCKZILLA_TELEGRAM_ENABLED=false

# Load definitions without entering command dispatch or the main loop.
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"

CACHE_ROOT=$fixture_root/cache
ACTIVE_GENERATION_DIR=$CACHE_ROOT/active
SEALED_GENERATION_DIR=$CACHE_ROOT/sealed
GENERATION_RECEIPT_DIR=$CACHE_ROOT/receipts
GENERATION_MONITORING_DIR=$CACHE_ROOT/monitoring
GENERATION_ROTATION_MARKER=$CACHE_ROOT/.rotation
REPLAY_RECOVERY_FILE=$GENERATION_MONITORING_DIR/replay-recovery-floor.json
REPLAY_GAP_DIR=$ACTIVE_GENERATION_DIR/replay-gaps
ALERT_STATE_DIR=$CACHE_ROOT/alert-state
JOURNAL_FILE=$ACTIVE_GENERATION_DIR/raw-blocks.jsonl
MAX_RECORD_BYTES=1024
CLUSTER_ID=solana-mainnet
ORIGIN_NODE_ID=hetzner-test
SOURCE_ID=grpc-raw-test
GENERATION_REMOTE_PREFIX=grpc-raw/v1
GENERATION_PYTHON_BIN=${PYTHON_BIN:-python3}

# Tests run on macOS and Linux; durability ordering is tested independently from
# the platform-specific sync(1) spelling used in production.
sync_path() { :; }
cache_real_directory() {
  [ ! -L "$1" ] && [ -d "$1" ]
}
journal_size() {
  if [ -s "$JOURNAL_FILE" ]; then
    wc -c < "$JOURNAL_FILE" | tr -d ' '
  else
    printf '%s\n' 0
  fi
}

fake_bin=$fixture_root/fake-producer
cat > "$fake_bin" <<'SH'
#!/bin/sh
set -eu
command_name=$1
shift
case "$command_name" in
  verify-grpc-raw-poh)
    output_dir=
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --output-dir) output_dir=$2; shift 2 ;;
        *) shift ;;
      esac
    done
    slot=$(cat "$output_dir/slot")
    printf '{\n  "records_verified": 1,\n  "last_slot": %s\n}\n' "$slot"
    ;;
  seed-grpc-raw-generation)
    source_dir=
    target_dir=
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --source-dir) source_dir=$2; shift 2 ;;
        --target-dir) target_dir=$2; shift 2 ;;
        *) shift ;;
      esac
    done
    mkdir "$target_dir"
    slot=$(cat "$source_dir/slot")
    cp "$source_dir/slot" "$target_dir/slot"
    printf '%s\n' successor > "$target_dir/role"
    printf '%s\n' seed > "$target_dir/raw-blocks.jsonl"
    printf '{\n  "seeded_slot": %s\n}\n' "$slot"
    ;;
  *) exit 64 ;;
esac
SH
chmod +x "$fake_bin"
BIN=$fake_bin

reset_rotation_fixture() {
  rm -rf "$CACHE_ROOT"
  mkdir -p "$ACTIVE_GENERATION_DIR" "$SEALED_GENERATION_DIR" \
    "$GENERATION_RECEIPT_DIR" "$GENERATION_MONITORING_DIR" "$ALERT_STATE_DIR"
  printf '%s\n' 123 > "$ACTIVE_GENERATION_DIR/slot"
  printf '%s\n' old > "$ACTIVE_GENERATION_DIR/role"
  printf '%s\n' old-old-old > "$ACTIVE_GENERATION_DIR/raw-blocks.jsonl"
  printf '%s\n' durable-event > "$GENERATION_MONITORING_DIR/resume-event"
  printf '%s\n' incident > "$(alert_file grpc_stale active)"
  printf '%s\n' 99999 > "$(alert_file grpc_stale journal_size)"
  printf '%s\n' incident > "$(alert_file recorder_restarting active)"
  printf '%s\n' 99999 > "$(alert_file recorder_restarting journal_size)"
  REPLAY_RECOVERY_ANCHOR_SLOT=
  REPLAY_RECOVERY_REQUESTED_SLOT=
  REPLAY_MIN_RESUME_SLOT=
  REPLAY_VERIFIED_ANCHOR_SLOT=
  REPLAY_SKIP_RETIRE_VERIFY_ONCE=false
  unset BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT || true
}

assert_rotated() {
  generation_id=slot-00000000000000000123
  test "$(cat "$ACTIVE_GENERATION_DIR/role")" = successor
  test "$(cat "$SEALED_GENERATION_DIR/$generation_id/role")" = old
  test ! -e "$GENERATION_ROTATION_MARKER"
  test "$(cat "$GENERATION_MONITORING_DIR/resume-event")" = durable-event
  successor_journal_size=$(stat -c %s "$JOURNAL_FILE" 2>/dev/null || stat -f %z "$JOURNAL_FILE")
  test "$(cat "$(alert_file grpc_stale journal_size)")" = "$successor_journal_size"
  test "$(cat "$(alert_file recorder_restarting journal_size)")" = "$successor_journal_size"
}

write_replay_report() {
  replay_fixture_path=$1
  replay_fixture_anchor=$2
  replay_fixture_requested=$3
  replay_fixture_available=$4
  replay_fixture_seen=${5:-0}
  replay_fixture_written=${6:-0}
  printf '{\n  "frames_seen": %s,\n  "frames_written": %s,\n  "effective_from_slot": %s,\n  "resume_overlap_slot": %s,\n  "replay_unavailable": true,\n  "replay_unavailable_requested_slot": %s,\n  "replay_available_slot": %s\n}\n' \
    "$replay_fixture_seen" "$replay_fixture_written" \
    "$replay_fixture_requested" "$replay_fixture_anchor" \
    "$replay_fixture_requested" "$replay_fixture_available" \
    > "$replay_fixture_path"
}

# Interrupted Rust seed creation leaves a hidden sibling of its .next-slot
# target. Recovery removes only names that exactly match Rust's validated
# generation-id/PID/nanosecond format.
reset_rotation_fixture
seed_generation_id=slot-00000000000000000123
valid_seed_temp=$CACHE_ROOT/..next-$seed_generation_id.seed-1234-567890.tmp
ignored_seed_lookalike=$CACHE_ROOT/..next-$seed_generation_id.seed-notrust.tmp
mkdir -p "$valid_seed_temp/nested" "$ignored_seed_lookalike"
printf '%s\n' partial > "$valid_seed_temp/nested/frame"
recover_rotation_transaction
test ! -e "$valid_seed_temp"
test -d "$ignored_seed_lookalike"

# A malformed name that reaches the seed-temp glob fails closed and is retained.
reset_rotation_fixture
malformed_seed_temp=$CACHE_ROOT/..next-$seed_generation_id.seed-not-a-number-567890.tmp
mkdir "$malformed_seed_temp"
if recover_rotation_transaction; then
  echo "malformed orphan seed temp was accepted" >&2
  exit 1
fi
test -d "$malformed_seed_temp"

# A matching regular file is never recursively removed.
reset_rotation_fixture
file_seed_temp=$CACHE_ROOT/..next-$seed_generation_id.seed-1234-567890.tmp
printf '%s\n' keep > "$file_seed_temp"
if recover_rotation_transaction; then
  echo "regular-file orphan seed temp was accepted" >&2
  exit 1
fi
test -f "$file_seed_temp"
test "$(cat "$file_seed_temp")" = keep

# A matching symlink is rejected without touching its target.
reset_rotation_fixture
seed_symlink_target=$fixture_root/seed-symlink-target
symlink_seed_temp=$CACHE_ROOT/..next-$seed_generation_id.seed-1234-567890.tmp
mkdir "$seed_symlink_target"
printf '%s\n' keep > "$seed_symlink_target/frame"
ln -s "$seed_symlink_target" "$symlink_seed_temp"
if recover_rotation_transaction; then
  echo "symlink orphan seed temp was accepted" >&2
  exit 1
fi
test -L "$symlink_seed_temp"
test "$(cat "$seed_symlink_target/frame")" = keep

# Normal ordering: seed first, publish active successor, expose sealed old last.
reset_rotation_fixture
rotate_active_generation
assert_rotated

# Crash after hiding the old generation: uploader sees no sealed generation.
reset_rotation_fixture
BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT=after_old_hidden
if rotate_active_generation; then
  echo "rotation failpoint after_old_hidden did not fire" >&2
  exit 1
fi
test ! -e "$ACTIVE_GENERATION_DIR"
test -z "$(find "$SEALED_GENERATION_DIR" -mindepth 1 -maxdepth 1 -print -quit)"
marker_seed_temp=$CACHE_ROOT/..next-slot-00000000000000000123.seed-4321-987654.tmp
mkdir "$marker_seed_temp"
unset BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT
recover_rotation_transaction
test ! -e "$marker_seed_temp"
assert_rotated

# Crash after successor publication: old remains hidden until recovery.
reset_rotation_fixture
BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT=after_successor_active
if rotate_active_generation; then
  echo "rotation failpoint after_successor_active did not fire" >&2
  exit 1
fi
test "$(cat "$ACTIVE_GENERATION_DIR/role")" = successor
test -z "$(find "$SEALED_GENERATION_DIR" -mindepth 1 -maxdepth 1 -print -quit)"
unset BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT
recover_rotation_transaction
assert_rotated

# Crash after sealed publication: marker recovery completes without data loss.
reset_rotation_fixture
BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT=after_sealed_visible
if rotate_active_generation; then
  echo "rotation failpoint after_sealed_visible did not fire" >&2
  exit 1
fi
test -e "$GENERATION_ROTATION_MARKER"
test -d "$ACTIVE_GENERATION_DIR"
test -d "$SEALED_GENERATION_DIR/slot-00000000000000000123"
set +e
upload_one_generation
rotation_upload_status=$?
set -e
test "$rotation_upload_status" -eq 75
test -d "$SEALED_GENERATION_DIR/slot-00000000000000000123"
unset BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT
recover_rotation_transaction
assert_rotated

# Upload/delete gate: failure or an invalid receipt always retains the sealed data.
reset_rotation_fixture
rm -rf "$ACTIVE_GENERATION_DIR"
upload_id=slot-00000000000000000200
upload_dir=$SEALED_GENERATION_DIR/$upload_id
mkdir "$upload_dir"
printf '%s\n' payload > "$upload_dir/payload"
credentials=$fixture_root/backblaze-credentials
printf '%s\n' fixture > "$credentials"
GENERATION_CREDENTIALS_FILE=$credentials

mock_uploader=$fixture_root/mock-uploader
cat > "$mock_uploader" <<'SH'
#!/bin/sh
set -eu
shift
generation_dir=$1
remote_prefix=$2
receipt=$3
shift 3
generation_id=
predecessor=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --generation-id) generation_id=$2; shift 2 ;;
    --predecessor-manifest-sha256) predecessor=$2; shift 2 ;;
    *) shift ;;
  esac
done
case "${MOCK_UPLOAD_RESULT:-failure}" in
  failure) exit 1 ;;
  invalid)
    printf '%s\n' '{}' > "$receipt"
    exit 0
    ;;
  valid)
    manifest_hash=$(printf 'a%.0s' $(seq 1 64))
    commit_hash=$(printf 'b%.0s' $(seq 1 64))
    if [ -n "$predecessor" ]; then
      predecessor_json=$(printf ',"predecessor_manifest_sha256":"%s"' "$predecessor")
    else
      predecessor_json=
    fi
    printf '{"schema_version":1,"generation_id":"%s","remote_prefix":"%s","manifest_key":"%s/manifest.json","manifest_sha256":"%s","manifest_version_id":"version-manifest","commit_key":"%s/_COMMITTED","commit_sha256":"%s","commit_version_id":"version-commit","file_count":1,"total_bytes":8,"verified_unix_secs":1%s}\n' \
      "$generation_id" "$remote_prefix" "$remote_prefix" "$manifest_hash" \
      "$remote_prefix" "$commit_hash" "$predecessor_json" > "$receipt"
    ;;
esac
SH
chmod +x "$mock_uploader"
GENERATION_UPLOADER_BIN=$mock_uploader

MOCK_UPLOAD_RESULT=failure
export MOCK_UPLOAD_RESULT
if upload_one_generation; then
  echo "failed uploader was accepted" >&2
  exit 1
fi
test -d "$upload_dir"

MOCK_UPLOAD_RESULT=invalid
export MOCK_UPLOAD_RESULT
if upload_one_generation; then
  echo "invalid receipt was accepted" >&2
  exit 1
fi
test -d "$upload_dir"

MOCK_UPLOAD_RESULT=valid
export MOCK_UPLOAD_RESULT
upload_one_generation
test ! -e "$upload_dir"
test -s "$GENERATION_RECEIPT_DIR/$upload_id.json"
test -s "$GENERATION_RECEIPT_DIR/.chain"

# A validated replay-window failure first creates immutable generation evidence,
# then advances the durable monitoring pointer without rotating or deleting data.
reset_rotation_fixture
replay_report=$fixture_root/replay-report.json
write_replay_report "$replay_report" 123 123 200
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
test "$REPLAY_RECOVERY_ANCHOR_SLOT" -eq 123
test "$REPLAY_MIN_RESUME_SLOT" -eq 200
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200.json"
test "$(cat "$ACTIVE_GENERATION_DIR/role")" = old
test -z "$(find "$SEALED_GENERATION_DIR" -mindepth 1 -maxdepth 1 -print -quit)"

# A crash after the immutable record but before the pointer is idempotent.
rm "$REPLAY_RECOVERY_FILE"
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
test "$REPLAY_MIN_RESUME_SLOT" -eq 200

# If the provider floor advances again before a successful append, it must be
# monotonic, preserve the original anchor, and add another immutable record.
write_replay_report "$replay_report" 123 200 250
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
test "$REPLAY_RECOVERY_ANCHOR_SLOT" -eq 123
test "$REPLAY_MIN_RESUME_SLOT" -eq 250
test -f "$REPLAY_GAP_DIR/replay-gap-123-200-250.json"

# Regressions, mismatched anchors, mid-stream reports, and modified immutable
# records all fail closed without changing the last accepted floor.
write_replay_report "$replay_report" 123 250 240
if persist_replay_recovery_from_report "$replay_report"; then
  echo "replay floor regression was accepted" >&2
  exit 1
fi
write_replay_report "$replay_report" 124 250 300
if persist_replay_recovery_from_report "$replay_report"; then
  echo "replay anchor mismatch was accepted" >&2
  exit 1
fi
write_replay_report "$replay_report" 123 250 300 1 0
if persist_replay_recovery_from_report "$replay_report"; then
  echo "mid-stream replay recovery was accepted" >&2
  exit 1
fi
write_replay_report "$replay_report" 123 250 300
printf '%s\n' '  "replay_unavailable": false' >> "$replay_report"
if persist_replay_recovery_from_report "$replay_report"; then
  echo "duplicate replay status field was accepted" >&2
  exit 1
fi
load_replay_recovery_floor
test "$REPLAY_MIN_RESUME_SLOT" -eq 250

# Once verified durable progress reaches the floor, retire only the mutable
# pointer. A later independent provider gap can then establish a new anchor.
printf '%s\n' 200 > "$ACTIVE_GENERATION_DIR/slot"
if retire_replay_recovery_floor_if_advanced; then
  echo "replay marker detached from active tail was accepted" >&2
  exit 1
fi
test -f "$REPLAY_RECOVERY_FILE"
printf '%s\n' 260 > "$ACTIVE_GENERATION_DIR/slot"
retire_replay_recovery_floor_if_advanced
test ! -e "$REPLAY_RECOVERY_FILE"
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200.json"
test -f "$REPLAY_GAP_DIR/replay-gap-123-200-250.json"
write_replay_report "$replay_report" 260 260 300
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
test "$REPLAY_RECOVERY_ANCHOR_SLOT" -eq 260
test "$REPLAY_MIN_RESUME_SLOT" -eq 300

reset_rotation_fixture
mkdir "$REPLAY_GAP_DIR"
printf '%s\n' tampered > "$REPLAY_GAP_DIR/replay-gap-123-123-200.json"
write_replay_report "$replay_report" 123 123 200
if persist_replay_recovery_from_report "$replay_report"; then
  echo "modified immutable replay record was accepted" >&2
  exit 1
fi
test ! -e "$REPLAY_RECOVERY_FILE"

# A syntactically canonical pointer has no authority without its exact
# immutable generation record.
reset_rotation_fixture
printf '{"anchor_slot":123,"cluster_id":"%s","minimum_resume_slot":200,"origin_node_id":"%s","requested_slot":123,"schema_version":1,"source_id":"%s"}\n' \
  "$CLUSTER_ID" "$ORIGIN_NODE_ID" "$SOURCE_ID" > "$REPLAY_RECOVERY_FILE"
if load_replay_recovery_floor; then
  echo "replay pointer without immutable evidence was accepted" >&2
  exit 1
fi

# A pointer symlink is never followed or overwritten.
reset_rotation_fixture
replay_pointer_target=$fixture_root/replay-pointer-target
printf '%s\n' keep > "$replay_pointer_target"
ln -s "$replay_pointer_target" "$REPLAY_RECOVERY_FILE"
if load_replay_recovery_floor; then
  echo "replay recovery symlink was accepted" >&2
  exit 1
fi
test "$(cat "$replay_pointer_target")" = keep

# If generation-cap preflight stops before the first floor block is appended,
# rotation duplicates the exact immutable evidence into the seeded successor so
# the still-pending marker remains authoritative.
reset_rotation_fixture
write_replay_report "$replay_report" 123 123 200
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
rotate_active_generation
load_replay_recovery_floor
test "$REPLAY_MIN_RESUME_SLOT" -eq 200
test -f "$ACTIVE_GENERATION_DIR/replay-gaps/replay-gap-123-123-200.json"
test -f "$SEALED_GENERATION_DIR/slot-00000000000000000123/replay-gaps/replay-gap-123-123-200.json"

# If a floor block is already durable, retire the pointer before rotation. The
# evidence stays in the old sealed generation and no stale authority is needed
# in the exact-tail successor.
reset_rotation_fixture
write_replay_report "$replay_report" 123 123 200
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
printf '%s\n' 200 > "$ACTIVE_GENERATION_DIR/slot"
retire_replay_recovery_floor_if_advanced
test ! -e "$REPLAY_RECOVERY_FILE"
rotate_active_generation
load_replay_recovery_floor
test -z "$REPLAY_MIN_RESUME_SLOT"
test -f "$SEALED_GENERATION_DIR/slot-00000000000000000200/replay-gaps/replay-gap-123-123-200.json"
test ! -e "$ACTIVE_GENERATION_DIR/replay-gaps/replay-gap-123-123-200.json"

printf '%s\n' "linux raw recorder bounded-cache tests: ok"
