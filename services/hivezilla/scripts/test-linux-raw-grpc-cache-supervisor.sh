#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
supervisor=$repo_root/scripts/linux-raw-grpc-recorder.sh
fixture_root=$(mktemp -d "${TMPDIR:-/tmp}/blockzilla-cache-test.XXXXXX")
trap 'rm -rf "$fixture_root"' EXIT

export BLOCKZILLA_RAW_CACHE_MODE=b2-generations
export BLOCKZILLA_RAW_OBJECT_STORE=backblaze
export BLOCKZILLA_TELEGRAM_ENABLED=false

# Provider selection resolves R2 through generic aliases and keeps the existing
# Backblaze variables as the default-provider compatibility path.
r2_config=$(
  BLOCKZILLA_RAW_OBJECT_STORE=r2 \
  BLOCKZILLA_RAW_OBJECT_STORE_CREDENTIALS_FILE=/run/secrets/test-r2 \
  BLOCKZILLA_RAW_OBJECT_STORE_REMOTE_PREFIX=r2-live/v1 \
  BLOCKZILLA_B2_CREDENTIALS_FILE=/should/not/use-b2-creds \
  BLOCKZILLA_B2_REMOTE_PREFIX=should/not/use-b2-prefix \
    sh -s -- "$supervisor" <<'SH'
supervisor=$1
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
validate_object_store
printf '%s|%s|%s|%s\n' "$OBJECT_STORE" "$OBJECT_STORE_HUMAN_NAME" \
  "$GENERATION_CREDENTIALS_FILE" "$GENERATION_REMOTE_PREFIX"
SH
)
test "$r2_config" = 'r2|Cloudflare R2|/run/secrets/test-r2|r2-live/v1'

r2_default_config=$(
  BLOCKZILLA_RAW_OBJECT_STORE=r2 \
    sh -s -- "$supervisor" <<'SH'
supervisor=$1
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
printf '%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
  "$GENERATION_CREDENTIALS_FILE" "$GENERATION_REMOTE_PREFIX" \
  "$R2_USAGE_ALERT_ENABLED" "$R2_RETENTION_ENABLED" \
  "$R2_RETENTION_TRIGGER_BYTES" "$R2_RETENTION_TARGET_BYTES" \
  "$R2_RETENTION_MINIMUM_AGE_SECS" \
  "$R2_RETENTION_MINIMUM_RETAINED_GENERATIONS" \
  "$R2_RETENTION_CHECK_INTERVAL_SECS"
SH
)
test "$r2_default_config" = '/run/secrets/r2_credentials|live-grpc-backup/v1|true|false|950000000000|900000000000|86400|2|3600'

backblaze_legacy_config=$(
  BLOCKZILLA_B2_CREDENTIALS_FILE=/run/secrets/legacy-b2 \
  BLOCKZILLA_B2_REMOTE_PREFIX=legacy-grpc/v1 \
    sh -s -- "$supervisor" <<'SH'
supervisor=$1
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
printf '%s|%s|%s\n' "$OBJECT_STORE" "$GENERATION_CREDENTIALS_FILE" \
  "$GENERATION_REMOTE_PREFIX"
SH
)
test "$backblaze_legacy_config" = 'backblaze|/run/secrets/legacy-b2|legacy-grpc/v1'

# A NAS-only bounded cache gets the same crash-safe generation and replay-gap
# machinery without enabling any object-store worker.
local_generation_config=$(
  BLOCKZILLA_RAW_CACHE_MODE=local-generations \
  BLOCKZILLA_RAW_OBJECT_STORE=none \
    sh -s -- "$supervisor" <<'SH'
supervisor=$1
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
validate_data_paths
validate_object_store
printf '%s|%s|%s|%s|%s\n' \
  "$CACHE_MODE" "$OBJECT_STORE" "$OUTPUT_DIR" "$ALERT_STATE_DIR" \
  "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
SH
)
test "$local_generation_config" = \
  'local-generations|none|/data/grpc-cache/active|/data/grpc-cache/monitoring/telegram-alerts|/data/grpc-cache/monitoring/resume-coverage-warning.json'

if BLOCKZILLA_RAW_CACHE_MODE=b2-generations \
  BLOCKZILLA_RAW_OBJECT_STORE=none \
  sh -s -- "$supervisor" <<'SH'
supervisor=$1
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
validate_object_store
SH
then
  echo "object-store mode accepted the local-only provider" >&2
  exit 1
fi
if BLOCKZILLA_RAW_CACHE_MODE=local-generations \
  BLOCKZILLA_RAW_OBJECT_STORE=backblaze \
  sh -s -- "$supervisor" <<'SH'
supervisor=$1
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
validate_object_store
SH
then
  echo "local-only mode accepted an object-store provider" >&2
  exit 1
fi

if BLOCKZILLA_RAW_OBJECT_STORE=unknown sh -s -- "$supervisor" <<'SH'
supervisor=$1
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
validate_object_store
SH
then
  echo "unknown raw object store was accepted" >&2
  exit 1
fi
if BLOCKZILLA_RAW_OBJECT_STORE=r2 \
  BLOCKZILLA_RAW_OBJECT_STORE_REMOTE_PREFIX=../unsafe \
  sh -s -- "$supervisor" <<'SH'
supervisor=$1
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
validate_object_store
SH
then
  echo "unsafe R2 object prefix was accepted" >&2
  exit 1
fi

# Retention limits are validated as one coherent 1 TB rolling-window policy.
for invalid_retention_case in trigger_not_above_target trigger_over_budget \
  zero_target one_generation
do
  case "$invalid_retention_case" in
    trigger_not_above_target)
      invalid_retention_env='R2_RETENTION_TRIGGER_BYTES=900000000000 R2_RETENTION_TARGET_BYTES=900000000000'
      ;;
    trigger_over_budget)
      invalid_retention_env='R2_RETENTION_TRIGGER_BYTES=1000000000001'
      ;;
    zero_target)
      invalid_retention_env='R2_RETENTION_TARGET_BYTES=0'
      ;;
    one_generation)
      invalid_retention_env='R2_RETENTION_MINIMUM_RETAINED_GENERATIONS=1'
      ;;
  esac
  if sh -s -- "$supervisor" "$invalid_retention_env" <<'SH'
supervisor=$1
invalid_retention_env=$2
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"
OBJECT_STORE=r2
CACHE_MODE=b2-generations
eval "$invalid_retention_env"
validate_r2_retention_config
SH
  then
    echo "invalid R2 retention case was accepted: $invalid_retention_case" >&2
    exit 1
  fi
done

# Load definitions without entering command dispatch or the main loop.
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"

# The recorder uses adaptive HTTP/2 flow control and a tolerant transport ACK
# deadline, then reconnects immediately after a session has proved durable
# progress. Zero-progress sessions retain the longer restart delay.
test "$BLOCKZILLA_GRPC_CONNECT_TIMEOUT_SECS" -eq 10
test "$BLOCKZILLA_GRPC_HTTP2_ADAPTIVE_WINDOW" = true
test "$BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS" -eq 30
test "$RESTART_DELAY_SECS" -eq 5
test "$PROGRESS_RESTART_DELAY_SECS" -eq 0
test "$REPLAY_RESUME_HEADROOM_SLOTS" -eq 100
# Deployment manifests are intentionally not part of this reusable helper
# suite. Verify the defaults and the argument wiring in the supervisor itself.
grep -Fq \
  '"BLOCKZILLA_RAW_PROGRESS_RESTART_DELAY_SECS:$PROGRESS_RESTART_DELAY_SECS"' \
  "$supervisor"
if (require_uint BLOCKZILLA_RAW_PROGRESS_RESTART_DELAY_SECS invalid) 2>/dev/null; then
  echo "invalid progress reconnect delay was accepted" >&2
  exit 1
fi

# The relay handoff is opt-in. Its token remains a file argument and is never
# read into or printed by the supervisor shell.
test -z "$RAW_RELAY_BIND"
test "$RAW_RELAY_X_TOKEN_FILE" = /run/secrets/grpc_relay_x_token
test "$RAW_RELAY_MAX_RECORDS" -eq 128
test "$RAW_RELAY_MAX_ENCODED_BYTES" -eq 134217728
test "$RAW_RELAY_MAX_CLIENTS" -eq 4
relay_child=$fixture_root/mock-relay-child
cat > "$relay_child" <<'SH'
#!/bin/sh
set -eu
printf '%s\n' "$@" > "${MOCK_RELAY_ARG_LOG:?}"
SH
chmod +x "$relay_child"
relay_args=$fixture_root/relay-args
relay_report=$fixture_root/relay-report
relay_token=$fixture_root/relay-token
printf '%s\n' 'relay-secret-must-not-appear' > "$relay_token"
BIN=$relay_child
CHILD_REPORT_FILE=$relay_report
MOCK_RELAY_ARG_LOG=$relay_args
export MOCK_RELAY_ARG_LOG
RAW_RELAY_BIND=
RAW_RELAY_X_TOKEN_FILE=$fixture_root/missing-is-allowed-while-disabled
validate_raw_relay_config
start_raw_recorder_child record-grpc-raw --endpoint fixture
wait "$RAW_RECORDER_CHILD_PID"
relay_expected=$fixture_root/relay-expected
printf '%s\n' record-grpc-raw --endpoint fixture > "$relay_expected"
if ! cmp -s "$relay_expected" "$relay_args"; then
  echo "empty relay bind changed recorder arguments" >&2
  exit 1
fi

RAW_RELAY_BIND=127.0.0.1:50051
RAW_RELAY_X_TOKEN_FILE=$relay_token
RAW_RELAY_MAX_RECORDS=128
RAW_RELAY_MAX_ENCODED_BYTES=134217728
RAW_RELAY_MAX_CLIENTS=4
validate_raw_relay_config
start_raw_recorder_child record-grpc-raw --endpoint fixture
wait "$RAW_RECORDER_CHILD_PID"
printf '%s\n' \
  record-grpc-raw --endpoint fixture \
  --relay-bind 127.0.0.1:50051 \
  --relay-x-token-file "$relay_token" \
  --relay-max-records 128 \
  --relay-max-encoded-bytes 134217728 \
  --relay-max-clients 4 > "$relay_expected"
cmp -s "$relay_expected" "$relay_args"
if grep -q 'relay-secret-must-not-appear' "$relay_args" "$relay_report"; then
  echo "relay token content escaped into process output" >&2
  exit 1
fi

RAW_RELAY_X_TOKEN_FILE=$fixture_root/missing-relay-token
if validate_raw_relay_config >/dev/null 2>&1; then
  echo "enabled relay accepted a missing token file" >&2
  exit 1
fi
: > "$fixture_root/empty-relay-token"
RAW_RELAY_X_TOKEN_FILE=$fixture_root/empty-relay-token
if validate_raw_relay_config >/dev/null 2>&1; then
  echo "enabled relay accepted an empty token file" >&2
  exit 1
fi
ln -s "$relay_token" "$fixture_root/symlink-relay-token"
RAW_RELAY_X_TOKEN_FILE=$fixture_root/symlink-relay-token
if validate_raw_relay_config >/dev/null 2>&1; then
  echo "enabled relay accepted a symlink token file" >&2
  exit 1
fi
RAW_RELAY_X_TOKEN_FILE=$relay_token
RAW_RELAY_MAX_CLIENTS=0
if validate_raw_relay_config >/dev/null 2>&1; then
  echo "enabled relay accepted a zero client limit" >&2
  exit 1
fi
RAW_RELAY_BIND=
RAW_RELAY_X_TOKEN_FILE=/run/secrets/grpc_relay_x_token
RAW_RELAY_MAX_CLIENTS=4
unset MOCK_RELAY_ARG_LOG

CACHE_ROOT=$fixture_root/cache
ACTIVE_GENERATION_DIR=$CACHE_ROOT/active
SEALED_GENERATION_DIR=$CACHE_ROOT/sealed
GENERATION_RECEIPT_DIR=$CACHE_ROOT/receipts
GENERATION_MONITORING_DIR=$CACHE_ROOT/monitoring
GENERATION_ROTATION_MARKER=$CACHE_ROOT/.rotation
GENERATION_ROTATION_LOCK=$CACHE_ROOT/.rotation.lock
GENERATION_RETENTION_LOCK=$CACHE_ROOT/.retention.lock
B2_USAGE_REPORT_FILE=$GENERATION_MONITORING_DIR/b2-account-usage.json
REPLAY_RECOVERY_FILE=$GENERATION_MONITORING_DIR/replay-recovery-floor.json
REPLAY_GAP_DIR=$ACTIVE_GENERATION_DIR/replay-gaps
ALERT_STATE_DIR=$CACHE_ROOT/alert-state
JOURNAL_FILE=$ACTIVE_GENERATION_DIR/raw-blocks.jsonl
MAX_RECORD_BYTES=1024
CLUSTER_ID=solana-mainnet
ORIGIN_NODE_ID=source-node-test
SOURCE_ID=grpc-raw-test
GENERATION_REMOTE_PREFIX=grpc-raw/v1
GENERATION_PYTHON_BIN=${PYTHON_BIN:-python3}
REPLAY_RESUME_HEADROOM_SLOTS=0
B2_CAP_RETRY_SECS=3600

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

# Known provider caps must move both background workers off their hot retry
# loops. Unknown failures retain each worker's ordinary interval.
for capacity_status in 20 21 22; do
  test "$(backblaze_retry_seconds "$capacity_status" 60)" -eq 3600
done
test "$(backblaze_retry_seconds 1 60)" -eq 60
test "$(backblaze_retry_seconds 75 3600)" -eq 3600

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
    if [ -e "$output_dir/fail-verify" ]; then
      exit 1
    fi
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
  notify-supervisor)
    printf '%s %s %s\n' "$1" "$BLOCKZILLA_SUPERVISOR_NOTIFY_FILE" \
      "$BLOCKZILLA_SUPERVISOR_NOTIFY_TOKEN" >> "$SUPERVISOR_NOTIFY_LOG"
    ;;
  *) exit 64 ;;
esac
SH
chmod +x "$fake_bin"
BIN=$fake_bin

SUPERVISOR_NOTIFY_LOG=$fixture_root/supervisor-notify.log
export SUPERVISOR_NOTIFY_LOG
unset BLOCKZILLA_SUPERVISOR_NOTIFY_FILE BLOCKZILLA_SUPERVISOR_NOTIFY_TOKEN
notify_parent_supervisor ready
test ! -e "$SUPERVISOR_NOTIFY_LOG"
export BLOCKZILLA_SUPERVISOR_NOTIFY_FILE=$fixture_root/notify.json
export BLOCKZILLA_SUPERVISOR_NOTIFY_TOKEN=attempt-token
notify_parent_supervisor ready
grep -Fxq "ready $BLOCKZILLA_SUPERVISOR_NOTIFY_FILE attempt-token" \
  "$SUPERVISOR_NOTIFY_LOG"
unset BLOCKZILLA_SUPERVISOR_NOTIFY_FILE BLOCKZILLA_SUPERVISOR_NOTIFY_TOKEN

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
  REPLAY_RECOVERY_SCHEMA_VERSION=
  REPLAY_PROVIDER_AVAILABLE_SLOT=
  REPLAY_MIN_RESUME_SLOT=
  REPLAY_GAP_PROVIDER_AVAILABLE_SLOT=
  REPLAY_GAP_SELECTED_RESUME_SLOT=
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

# Local-only generations retain the same immutable replay-gap authorization as
# uploaded generations. Object storage is not required to escape a stale-floor
# restart loop safely.
reset_rotation_fixture
local_replay_report=$fixture_root/local-replay-report.json
write_replay_report "$local_replay_report" 123 123 200
CACHE_MODE=local-generations
persist_replay_recovery_from_report "$local_replay_report"
load_replay_recovery_floor
test "$REPLAY_MIN_RESUME_SLOT" -eq 200
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200-200.json"
CACHE_MODE=b2-generations

# Rotation never follows a lock-file symlink or mutates its target.
reset_rotation_fixture
rotation_lock_target=$fixture_root/rotation-lock-target
printf '%s\n' keep > "$rotation_lock_target"
ln -s "$rotation_lock_target" "$GENERATION_ROTATION_LOCK"
if recover_rotation_transaction; then
  echo "rotation accepted a symlink lock file" >&2
  exit 1
fi
test "$(cat "$rotation_lock_target")" = keep

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

# Crash recovery must not expose a hidden predecessor until its full content
# audit succeeds. The generation ID is a monotonic sequence and intentionally
# need not equal the predecessor's tail slot after a fork or repeated slot.
reset_rotation_fixture
BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT=after_successor_active
if rotate_active_generation; then
  echo "rotation audit-gate fixture did not stop after successor publication" >&2
  exit 1
fi
hidden_old=$CACHE_ROOT/.sealed-slot-00000000000000000123
touch "$hidden_old/fail-verify"
unset BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT
if recover_rotation_transaction; then
  echo "rotation recovery published a predecessor that failed its content audit" >&2
  exit 1
fi
test -d "$hidden_old"
test -z "$(find "$SEALED_GENERATION_DIR" -mindepth 1 -maxdepth 1 -print -quit)"
rm -f "$hidden_old/fail-verify"
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
first_upload_id=$upload_id
first_upload_dir=$upload_dir
mkdir "$upload_dir"
printf '%s\n' payload > "$upload_dir/payload"
credentials=$fixture_root/backblaze-credentials
printf '%s\n' fixture > "$credentials"
GENERATION_CREDENTIALS_FILE=$credentials

mock_uploader=$fixture_root/mock-uploader
cat > "$mock_uploader" <<'SH'
#!/bin/sh
set -eu
command_name=$1
shift
if [ -n "${MOCK_UPLOADER_CALL_LOG:-}" ]; then
  {
    printf 'command=%s\n' "$command_name"
    for mock_arg in "$@"; do
      printf 'arg=%s\n' "$mock_arg"
    done
  } >> "$MOCK_UPLOADER_CALL_LOG"
fi
if [ "$command_name" = b2-account-usage ]; then
  case "${MOCK_USAGE_RESULT:-failure}" in
    failure) exit 1 ;;
    status-*) exit "${MOCK_USAGE_RESULT#status-}" ;;
    valid)
      printf '%s\n' \
        '{"schema_version":1,"scope":"account","scope_complete":true,"total_stored_bytes":42}'
      exit 0
      ;;
    *) exit 64 ;;
  esac
fi
if [ "$command_name" = r2-retention ]; then
  receipt_directory=$1
  remote_prefix=$2
  shift 2
  retention_mode=dry-run
  target_bytes=
  minimum_age_secs=
  minimum_retained_generations=
  maximum_generation_slot=
  provider=
  credentials_file=
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --target-bytes) target_bytes=$2; shift 2 ;;
      --minimum-age-secs) minimum_age_secs=$2; shift 2 ;;
      --maximum-generation-slot) maximum_generation_slot=$2; shift 2 ;;
      --minimum-retained-generations)
        minimum_retained_generations=$2
        shift 2
        ;;
      --apply) retention_mode=apply; shift ;;
      --provider) provider=$2; shift 2 ;;
      --credentials-file) credentials_file=$2; shift 2 ;;
      *) exit 64 ;;
    esac
  done
  test -n "$receipt_directory"
  test "$provider" = r2
  test -n "$credentials_file"
  case "$target_bytes:$minimum_age_secs:$minimum_retained_generations:$maximum_generation_slot" in
    *[!0-9:]*) exit 64 ;;
  esac
  if [ "$retention_mode" = apply ]; then
    case "${MOCK_R2_RETENTION_APPLY_RESULT:-valid}" in
      failure) exit 1 ;;
      invalid) printf '%s\n' '{}'; exit 0 ;;
      valid) ;;
      *) exit 64 ;;
    esac
  else
    case "${MOCK_R2_RETENTION_DRY_RUN_RESULT:-valid}" in
      failure) exit 1 ;;
      invalid) printf '%s\n' '{}'; exit 0 ;;
      valid) ;;
      *) exit 64 ;;
    esac
  fi
  active_file=${MOCK_R2_ACTIVE_BYTES_FILE:?}
  before=$(cat "$active_file")
  case "$before" in ''|*[!0-9]*) exit 64 ;; esac
  before_count=${MOCK_R2_ACTIVE_GENERATIONS:-4}
  after=$before
  after_count=$before_count
  selected_bytes=0
  selected_ids='[]'
  if [ "$retention_mode" = apply ] && [ "$before" -gt "$target_bytes" ]; then
    after=${MOCK_R2_POST_APPLY_BYTES:-$target_bytes}
    case "$after" in ''|*[!0-9]*) exit 64 ;; esac
    [ "$after" -le "$before" ] || exit 64
    selected_bytes=$((before - after))
    after_count=$((before_count - 2))
    selected_ids='["slot-00000000000000000001","slot-00000000000000000002"]'
    printf '%s\n' "$after" > "$active_file"
  fi
  if [ "$after" -le "$target_bytes" ]; then
    target_satisfied=true
  else
    target_satisfied=false
  fi
  printf '{"schema_version":1,"storage_provider":"r2","mode":"%s","remote_prefix":"%s","target_bytes":%s,"maximum_generation_slot":%s,"minimum_age_secs":%s,"minimum_retained_generations":%s,"retained_payload_bytes_before":%s,"retained_payload_bytes_after":%s,"retained_generation_count_before":%s,"retained_generation_count_after":%s,"selected_payload_bytes":%s,"selected_generation_ids":%s,"target_satisfied":%s}\n' \
    "$retention_mode" "$remote_prefix" "$target_bytes" \
    "$maximum_generation_slot" "$minimum_age_secs" \
    "$minimum_retained_generations" "$before" \
    "$after" "$before_count" "$after_count" "$selected_bytes" \
    "$selected_ids" "$target_satisfied"
  exit 0
fi
[ "$command_name" = upload-generation ] || exit 64
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
  status-*)
    status=${MOCK_UPLOAD_RESULT#status-}
    case "$status" in ''|*[!0-9]*) exit 64 ;; esac
    exit "$status"
    ;;
  invalid)
    printf '%s\n' '{}' > "$receipt"
    exit 0
    ;;
  valid)
    generation_number=${generation_id#slot-}
    manifest_hash=$(printf '%064s' "$generation_number" | tr ' ' 0)
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
mock_uploader_calls=$fixture_root/uploader-calls
: > "$mock_uploader_calls"
MOCK_UPLOADER_CALL_LOG=$mock_uploader_calls
export MOCK_UPLOADER_CALL_LOG

# The upload path never follows a retention-lock symlink or mutates its target.
retention_lock_target=$fixture_root/retention-lock-target
printf '%s\n' keep > "$retention_lock_target"
ln -s "$retention_lock_target" "$GENERATION_RETENTION_LOCK"
retention_lock_calls_before=$(grep -c '^command=upload-generation$' \
  "$mock_uploader_calls" || true)
if upload_one_generation; then
  echo "upload accepted a symlink retention lock" >&2
  exit 1
fi
test "$(cat "$retention_lock_target")" = keep
test "$(grep -c '^command=upload-generation$' "$mock_uploader_calls" || true)" \
  -eq "$retention_lock_calls_before"
rm -f "$GENERATION_RETENTION_LOCK"

# An existing lock must already be private. The uploader never repairs or
# silently trusts a group/world-accessible coordination inode.
: > "$GENERATION_RETENTION_LOCK"
chmod 0644 "$GENERATION_RETENTION_LOCK"
if upload_one_generation; then
  echo "upload accepted a non-private retention lock" >&2
  exit 1
fi
test "$(grep -c '^command=upload-generation$' "$mock_uploader_calls" || true)" \
  -eq "$retention_lock_calls_before"
rm -f "$GENERATION_RETENTION_LOCK"

# Selection and upload wait behind the same exclusive cache-root lock that the
# Rust ACK-GC uses while renaming or deleting sealed generations.
MOCK_UPLOAD_RESULT=failure
export MOCK_UPLOAD_RESULT
retention_holder_ready=$fixture_root/retention-holder-ready
"$GENERATION_PYTHON_BIN" - "$GENERATION_RETENTION_LOCK" \
  "$retention_holder_ready" <<'PY' &
import fcntl
import os
import sys
import time

descriptor = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
fcntl.flock(descriptor, fcntl.LOCK_EX)
with open(sys.argv[2], "w", encoding="ascii") as marker:
    marker.write("ready\n")
time.sleep(1)
os.close(descriptor)
PY
retention_holder_pid=$!
for _ in $(seq 1 100); do
  [ -s "$retention_holder_ready" ] && break
  sleep 0.01
done
test -s "$retention_holder_ready"
retention_blocked_started=$fixture_root/retention-blocked-started
(
  printf '%s\n' started > "$retention_blocked_started"
  upload_one_generation
) &
retention_blocked_pid=$!
for _ in $(seq 1 100); do
  [ -s "$retention_blocked_started" ] && break
  sleep 0.01
done
test -s "$retention_blocked_started"
sleep 0.2
test "$(grep -c '^command=upload-generation$' "$mock_uploader_calls" || true)" \
  -eq "$retention_lock_calls_before"
wait "$retention_holder_pid"
set +e
wait "$retention_blocked_pid"
retention_blocked_status=$?
set -e
test "$retention_blocked_status" -eq 1
test "$(grep -c '^command=upload-generation$' "$mock_uploader_calls")" \
  -eq $((retention_lock_calls_before + 1))

# The account-usage worker must retain the uploader's typed capacity status so
# it selects the same slow backoff as the generation worker.
for expected_usage_status in 20 21 22; do
  MOCK_USAGE_RESULT=status-$expected_usage_status
  export MOCK_USAGE_RESULT
  set +e
  run_b2_usage_scan
  observed_usage_status=$?
  set -e
  test "$observed_usage_status" -eq "$expected_usage_status"
done
MOCK_USAGE_RESULT=failure
export MOCK_USAGE_RESULT
set +e
run_b2_usage_scan
observed_usage_status=$?
set -e
test "$observed_usage_status" -eq 1
MOCK_USAGE_RESULT=valid
export MOCK_USAGE_RESULT
run_b2_usage_scan
test "$B2_USAGE_BYTES" -eq 42
test -s "$B2_USAGE_REPORT_FILE"
unset MOCK_USAGE_RESULT

MOCK_UPLOAD_RESULT=failure
export MOCK_UPLOAD_RESULT
if upload_one_generation; then
  echo "failed uploader was accepted" >&2
  exit 1
fi
test -d "$upload_dir"
rm -f "$GENERATION_RECEIPT_DIR/$upload_id.json"

MOCK_UPLOAD_RESULT=invalid
export MOCK_UPLOAD_RESULT
if upload_one_generation; then
  echo "invalid receipt was accepted" >&2
  exit 1
fi
test -d "$upload_dir"
rm -f "$GENERATION_RECEIPT_DIR/$upload_id.json"

# Typed Backblaze capacity failures survive the shell boundary exactly. An
# unknown uploader failure remains the generic status and never inherits a cap
# explanation merely because it may also have originated from HTTP 403.
for expected_upload_status in 20 21 22; do
  MOCK_UPLOAD_RESULT=status-$expected_upload_status
  export MOCK_UPLOAD_RESULT
  set +e
  upload_one_generation
  observed_upload_status=$?
  set -e
  test "$observed_upload_status" -eq "$expected_upload_status"
  test -d "$upload_dir"
done
MOCK_UPLOAD_RESULT=status-37
export MOCK_UPLOAD_RESULT
set +e
upload_one_generation
observed_upload_status=$?
set -e
test "$observed_upload_status" -eq 1
test -d "$upload_dir"

MOCK_UPLOAD_RESULT=valid
export MOCK_UPLOAD_RESULT
upload_calls_before=$(grep -c '^command=upload-generation$' "$mock_uploader_calls" || true)
upload_one_generation
test -d "$upload_dir"
test -s "$GENERATION_RECEIPT_DIR/$upload_id.json"
test -s "$GENERATION_RECEIPT_DIR/.chain"
test "$(sealed_generation_count)" -eq 0
test "$(retained_sealed_generation_count)" -eq 1
load_upload_chain
test "$UPLOAD_CHAIN_ID" = "$upload_id"
test "${#UPLOAD_CHAIN_HASH}" -eq 64
first_manifest_hash=$(printf '%064s' "${upload_id#slot-}" | tr ' ' 0)
test "$UPLOAD_CHAIN_HASH" = "$first_manifest_hash"
upload_calls_after=$(grep -c '^command=upload-generation$' "$mock_uploader_calls")
test "$upload_calls_after" -eq $((upload_calls_before + 1))

# Restart discovery skips a locally retained generation whose receipt is
# already reachable from the durable chain head.
upload_one_generation
test -d "$upload_dir"
test "$(grep -c '^command=upload-generation$' "$mock_uploader_calls")" \
  -eq "$upload_calls_after"

# The next generation reloads the persisted 64-hex chain and binds its receipt
# to the predecessor while retaining both local copies.
second_upload_id=slot-00000000000000000201
second_upload_dir=$SEALED_GENERATION_DIR/$second_upload_id
mkdir "$second_upload_dir"
printf '%s\n' payload-2 > "$second_upload_dir/payload"
upload_one_generation
test -d "$second_upload_dir"
test -s "$GENERATION_RECEIPT_DIR/$second_upload_id.json"
load_upload_chain
test "$UPLOAD_CHAIN_ID" = "$second_upload_id"
second_manifest_hash=$(printf '%064s' "${second_upload_id#slot-}" | tr ' ' 0)
test "$UPLOAD_CHAIN_HASH" = "$second_manifest_hash"
test "$("$GENERATION_PYTHON_BIN" -c 'import json,sys; print(json.load(open(sys.argv[1]))["predecessor_manifest_sha256"])' \
  "$GENERATION_RECEIPT_DIR/$second_upload_id.json")" = "$first_manifest_hash"

# Pressure draining is strictly FIFO even if directories were created in the
# opposite order. Fixed-width generation IDs make lexical and slot order equal.
fifo_newer_id=slot-00000000000000000300
fifo_older_id=slot-00000000000000000250
mkdir "$SEALED_GENERATION_DIR/$fifo_newer_id"
printf '%s\n' newer > "$SEALED_GENERATION_DIR/$fifo_newer_id/payload"
mkdir "$SEALED_GENERATION_DIR/$fifo_older_id"
printf '%s\n' older > "$SEALED_GENERATION_DIR/$fifo_older_id/payload"
upload_one_generation
test -d "$SEALED_GENERATION_DIR/$fifo_older_id"
test -d "$SEALED_GENERATION_DIR/$fifo_newer_id"

# Disk pressure cannot evict even object-store-committed local generations
# until Blockzilla provides a verified durable ACK. Uncommitted and off-chain
# directories are retained as well.
uncommitted_older_id=slot-00000000000000000150
mkdir "$SEALED_GENERATION_DIR/$uncommitted_older_id"
printf '%s\n' uncommitted > "$SEALED_GENERATION_DIR/$uncommitted_older_id/payload"
printf '%s\n' '{}' > "$GENERATION_RECEIPT_DIR/$uncommitted_older_id.json"
# Simulate a crash after a valid remote receipt was written for the newer
# generation but before `.chain` publication. Valid-but-off-chain is still not
# committed and therefore has no deletion authority.
load_upload_chain
offchain_prefix=$(generation_remote_prefix "$fifo_newer_id")
"$GENERATION_UPLOADER_BIN" upload-generation \
  "$SEALED_GENERATION_DIR/$fifo_newer_id" "$offchain_prefix" \
  "$GENERATION_RECEIPT_DIR/$fifo_newer_id.json" \
  --generation-id "$fifo_newer_id" \
  --credentials-file "$GENERATION_CREDENTIALS_FILE" \
  --predecessor-manifest-sha256 "$UPLOAD_CHAIN_HASH"
validate_generation_receipt "$GENERATION_RECEIPT_DIR/$fifo_newer_id.json" \
  "$fifo_newer_id" "$offchain_prefix" "$UPLOAD_CHAIN_HASH"
load_generation_retention_state "$fifo_newer_id"
test "$TARGET_GENERATION_COMMITTED" = 0
generation_spill_active=true
set +e
evict_first_committed_generation
blocked_eviction_status=$?
set -e
test "$blocked_eviction_status" -eq "$BLOCKZILLA_ACK_UNAVAILABLE_STATUS"
test "$GENERATION_EVICTION_PERFORMED" = false
test -z "$GENERATION_EVICTED_ID"
test "$GENERATION_EVICTION_BLOCKED_REASON" = blockzilla_ack_unavailable
test -d "$first_upload_dir"
test -d "$second_upload_dir"
test -d "$SEALED_GENERATION_DIR/$fifo_older_id"
test -d "$SEALED_GENERATION_DIR/$uncommitted_older_id"
test -d "$SEALED_GENERATION_DIR/$fifo_newer_id"

# R2 uses the same receipt-verification path and retains its local copy, but
# never calls the Backblaze account-usage command. Generic credentials and
# prefix are passed unchanged to upload-generation.
saved_object_store=$OBJECT_STORE
saved_object_store_human_name=$OBJECT_STORE_HUMAN_NAME
saved_generation_credentials_file=$GENERATION_CREDENTIALS_FILE
saved_generation_remote_prefix=$GENERATION_REMOTE_PREFIX
reset_rotation_fixture
rm -rf "$ACTIVE_GENERATION_DIR"
OBJECT_STORE=r2
OBJECT_STORE_HUMAN_NAME='Cloudflare R2'
GENERATION_REMOTE_PREFIX=r2-live/v1
test "$(object_store_retry_seconds 21 60)" -eq 60
r2_credentials=$fixture_root/r2-credentials
printf '%s\n' r2-fixture > "$r2_credentials"
GENERATION_CREDENTIALS_FILE=$r2_credentials
mock_uploader_calls=$fixture_root/r2-uploader-calls
: > "$mock_uploader_calls"
MOCK_UPLOADER_CALL_LOG=$mock_uploader_calls
export MOCK_UPLOADER_CALL_LOG

set +e
run_b2_usage_scan
r2_b2_usage_status=$?
set -e
test "$r2_b2_usage_status" -eq 78
test ! -s "$mock_uploader_calls"
b2_usage_worker_pid=
start_b2_usage_worker
test -z "$b2_usage_worker_pid"
R2_USAGE_ALERT_ENABLED=false
R2_RETENTION_ENABLED=false
r2_usage_worker_pid=
start_r2_usage_worker
test -z "$r2_usage_worker_pid"
R2_USAGE_ALERT_ENABLED=true
R2_RETENTION_ENABLED=true
r2_active_bytes=$fixture_root/r2-active-bytes
printf '%s\n' 0 > "$r2_active_bytes"
MOCK_R2_ACTIVE_BYTES_FILE=$r2_active_bytes
export MOCK_R2_ACTIVE_BYTES_FILE

# A new R2 provider ledger starts at a trustworthy zero without inventing a
# synthetic receipt chain. The remote verifier is not invoked until the first
# real generation publishes both its receipt and `.chain`.
r2_retention_calls_before=$(grep -c '^command=r2-retention$' \
  "$mock_uploader_calls" || true)
test "$(r2_receipt_ledger_bytes)" -eq 0
test "$(grep -c '^command=r2-retention$' "$mock_uploader_calls" || true)" \
  -eq "$r2_retention_calls_before"

# Missing-chain state is zero only when the receipt directory is truly empty.
# A partial receipt or any other interrupted publication artifact fails closed.
printf '%s\n' '{' > \
  "$GENERATION_RECEIPT_DIR/slot-00000000000000000499.json"
if r2_receipt_ledger_bytes >/dev/null 2>&1; then
  echo "R2 usage accepted a receipt without a chain head" >&2
  exit 1
fi
rm -f "$GENERATION_RECEIPT_DIR/slot-00000000000000000499.json"
printf '%s\n' interrupted > "$GENERATION_RECEIPT_DIR/.chain.tmp"
if r2_receipt_ledger_bytes >/dev/null 2>&1; then
  echo "R2 usage accepted a non-empty ledger without a chain head" >&2
  exit 1
fi
rm -f "$GENERATION_RECEIPT_DIR/.chain.tmp"

r2_upload_id=slot-00000000000000000400
r2_upload_dir=$SEALED_GENERATION_DIR/$r2_upload_id
mkdir "$r2_upload_dir"
printf '%s\n' r2-payload > "$r2_upload_dir/payload"
MOCK_UPLOAD_RESULT=invalid
export MOCK_UPLOAD_RESULT
if upload_one_generation; then
  echo "R2 invalid receipt was accepted" >&2
  exit 1
fi
test -d "$r2_upload_dir"
rm -f "$GENERATION_RECEIPT_DIR/$r2_upload_id.json"
r2_expected_prefix=r2-live/v1/solana-mainnet/source-node-test/$r2_upload_id
grep -Fxq 'command=upload-generation' "$mock_uploader_calls"
grep -Fxq "arg=$r2_expected_prefix" "$mock_uploader_calls"
grep -Fxq "arg=$r2_credentials" "$mock_uploader_calls"
if grep -q '^command=b2-account-usage$' "$mock_uploader_calls"; then
  echo "R2 path invoked Backblaze account usage" >&2
  exit 1
fi

MOCK_UPLOAD_RESULT=valid
export MOCK_UPLOAD_RESULT
upload_one_generation
test -d "$r2_upload_dir"
test -s "$GENERATION_RECEIPT_DIR/$r2_upload_id.json"
printf '%s\n' 8 > "$r2_active_bytes"
test "$(r2_receipt_ledger_bytes)" -eq 8

# Healthy local headroom keeps a newly sealed generation on source host without eagerly copying it to
# R2. Once the spill watermark is crossed, the worker uploads the oldest generation but still
# retains the verified local directory because object-store proof is not a Blockzilla ACK.
r2_healthy_id=slot-00000000000000000401
r2_healthy_dir=$SEALED_GENERATION_DIR/$r2_healthy_id
mkdir "$r2_healthy_dir"
printf '%s\n' r2-healthy > "$r2_healthy_dir/payload"
r2_calls_before=$(grep -c '^command=upload-generation$' "$mock_uploader_calls")
(
  MIN_FREE_BYTES=100
  MAX_GENERATION_BYTES=80
  GENERATION_SPILL_START_PERCENT=25
  GENERATION_SPILL_RECOVERY_PERCENT=35
  validate_data_volume() { return 0; }
  available_bytes() { printf '%s\n' 500; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  sleep() { exit 0; }
  generation_upload_worker
)
test -d "$r2_upload_dir"
test -d "$r2_healthy_dir"
test "$(grep -c '^command=upload-generation$' "$mock_uploader_calls")" \
  -eq "$r2_calls_before"
test ! -e "$(alert_file generation_backlog active)"
(
  MIN_FREE_BYTES=100
  MAX_GENERATION_BYTES=80
  GENERATION_SPILL_START_PERCENT=25
  GENERATION_SPILL_RECOVERY_PERCENT=35
  validate_data_volume() { return 0; }
  available_bytes() { printf '%s\n' 200; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  sleep() { exit 0; }
  generation_upload_worker
)
test "$(grep -c '^command=upload-generation$' "$mock_uploader_calls")" \
  -eq $((r2_calls_before + 1))
test -d "$r2_healthy_dir"
test -s "$GENERATION_RECEIPT_DIR/$r2_healthy_id.json"
discard_alert generation_backlog

# Returning to healthy headroom does not re-upload either retained copy.
r2_calls_after=$(grep -c '^command=upload-generation$' "$mock_uploader_calls")
(
  MIN_FREE_BYTES=100
  MAX_GENERATION_BYTES=80
  GENERATION_SPILL_START_PERCENT=25
  GENERATION_SPILL_RECOVERY_PERCENT=35
  validate_data_volume() { return 0; }
  available_bytes() { printf '%s\n' 500; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  sleep() { exit 0; }
  generation_upload_worker
)
test "$(grep -c '^command=upload-generation$' "$mock_uploader_calls")" \
  -eq "$r2_calls_after"

# R2 accounting is anchor-aware because the uploader owns `.chain` and
# `.r2-retention-anchor.json` validation. The shell always supplies a maximum
# generation slot of zero and never exposes --apply until signed generation
# acknowledgements exist.
R2_RETENTION_TRIGGER_BYTES=100
R2_RETENTION_TARGET_BYTES=80
R2_RETENTION_MINIMUM_AGE_SECS=86400
R2_RETENTION_MINIMUM_RETAINED_GENERATIONS=2
R2_RETENTION_CHECK_INTERVAL_SECS=3600
printf '%s\n' 90 > "$r2_active_bytes"
retention_calls_before=$(grep -c '^command=r2-retention$' "$mock_uploader_calls" || true)
(
  validate_data_volume() { return 0; }
  sleep() { exit 0; }
  r2_usage_worker
)
test "$(grep -c '^command=r2-retention$' "$mock_uploader_calls")" \
  -eq $((retention_calls_before + 1))
test "$(cat "$r2_active_bytes")" -eq 90
grep -Fxq 'arg=--target-bytes' "$mock_uploader_calls"
grep -Fxq "arg=$R2_RETENTION_SCAN_TARGET_BYTES" "$mock_uploader_calls"
grep -Fxq 'arg=--maximum-generation-slot' "$mock_uploader_calls"
grep -Fxq 'arg=0' "$mock_uploader_calls"
grep -Fxq 'arg=--minimum-age-secs' "$mock_uploader_calls"
grep -Fxq "arg=$R2_RETENTION_MINIMUM_AGE_SECS" "$mock_uploader_calls"
grep -Fxq 'arg=--minimum-retained-generations' "$mock_uploader_calls"
grep -Fxq "arg=$R2_RETENTION_MINIMUM_RETAINED_GENERATIONS" "$mock_uploader_calls"
grep -Fxq 'arg=--provider' "$mock_uploader_calls"
grep -Fxq 'arg=r2' "$mock_uploader_calls"
if grep -Fxq 'arg=--apply' "$mock_uploader_calls"; then
  echo "unsigned R2 accounting invoked destructive retention" >&2
  exit 1
fi

# Crossing the 950->900 policy boundary still cannot authorize deletion. The
# active total is retained and one accounting pass is made; a direct apply
# request is rejected before the uploader is invoked.
printf '%s\n' 120 > "$r2_active_bytes"
retention_calls_before=$(grep -c '^command=r2-retention$' "$mock_uploader_calls")
(
  validate_data_volume() { return 0; }
  sleep() { exit 0; }
  r2_usage_worker
)
test "$(grep -c '^command=r2-retention$' "$mock_uploader_calls")" \
  -eq $((retention_calls_before + 1))
test "$(cat "$r2_active_bytes")" -eq 120
set +e
run_r2_retention_command apply "$R2_RETENTION_TARGET_BYTES"
blocked_apply_status=$?
set -e
test "$blocked_apply_status" -eq 78
test "$(grep -c '^command=r2-retention$' "$mock_uploader_calls")" \
  -eq $((retention_calls_before + 1))
test "$(grep -c '^arg=--apply$' "$mock_uploader_calls" || true)" -eq 0

# Backblaze and invalid/anchor-rejected dry runs never fall through to apply.
retention_calls_before=$(grep -c '^command=r2-retention$' "$mock_uploader_calls")
OBJECT_STORE=backblaze
r2_usage_worker
test "$(grep -c '^command=r2-retention$' "$mock_uploader_calls")" \
  -eq "$retention_calls_before"
OBJECT_STORE=r2
MOCK_R2_RETENTION_DRY_RUN_RESULT=invalid
export MOCK_R2_RETENTION_DRY_RUN_RESULT
(
  validate_data_volume() { return 0; }
  sleep() { exit 0; }
  r2_usage_worker
)
test "$(grep -c '^command=r2-retention$' "$mock_uploader_calls")" \
  -eq $((retention_calls_before + 1))
test "$(grep -c '^arg=--apply$' "$mock_uploader_calls" || true)" -eq 0
unset MOCK_R2_RETENTION_DRY_RUN_RESULT

MOCK_R2_RETENTION_DRY_RUN_RESULT=invalid
export MOCK_R2_RETENTION_DRY_RUN_RESULT
if r2_receipt_ledger_bytes >/dev/null 2>&1; then
  echo "R2 retention accounting accepted an invalid ledger result" >&2
  exit 1
fi
unset MOCK_R2_RETENTION_DRY_RUN_RESULT
unset MOCK_UPLOADER_CALL_LOG MOCK_UPLOAD_RESULT MOCK_R2_ACTIVE_BYTES_FILE
OBJECT_STORE=$saved_object_store
OBJECT_STORE_HUMAN_NAME=$saved_object_store_human_name
GENERATION_CREDENTIALS_FILE=$saved_generation_credentials_file
GENERATION_REMOTE_PREFIX=$saved_generation_remote_prefix

# Legacy replay evidence remains readable during the schema-2 rollout.
reset_rotation_fixture
publish_replay_gap_record 123 123 200
publish_replay_recovery_floor 123 123 200
load_replay_recovery_floor
test "$REPLAY_MIN_RESUME_SLOT" -eq 200
test -z "$REPLAY_PROVIDER_AVAILABLE_SLOT"
test "$REPLAY_RECOVERY_SCHEMA_VERSION" -eq 1
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200.json"

# The next provider failure upgrades only the new link in the chain to v2.
REPLAY_RESUME_HEADROOM_SLOTS=32
replay_report=$fixture_root/replay-report.json
write_replay_report "$replay_report" 123 200 250
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
test "$REPLAY_RECOVERY_SCHEMA_VERSION" -eq 2
test "$REPLAY_PROVIDER_AVAILABLE_SLOT" -eq 250
test "$REPLAY_MIN_RESUME_SLOT" -eq 282
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200.json"
test -f "$REPLAY_GAP_DIR/replay-gap-123-200-250-282.json"

# A configured cushion is distinguished from the provider-advertised floor in
# both immutable evidence and the mutable resume pointer.
reset_rotation_fixture
REPLAY_RESUME_HEADROOM_SLOTS=32
replay_report=$fixture_root/replay-report.json
write_replay_report "$replay_report" 123 123 200
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
test "$REPLAY_PROVIDER_AVAILABLE_SLOT" -eq 200
test "$REPLAY_MIN_RESUME_SLOT" -eq 232
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200-232.json"
grep -q '"provider_available_slot":200' \
  "$REPLAY_GAP_DIR/replay-gap-123-123-200-232.json"
grep -q '"selected_resume_slot":232' \
  "$REPLAY_GAP_DIR/replay-gap-123-123-200-232.json"
REPLAY_RESUME_HEADROOM_SLOTS=7
load_replay_recovery_floor
test "$REPLAY_MIN_RESUME_SLOT" -eq 232
printf '%s\n' 231 > "$ACTIVE_GENERATION_DIR/slot"
if retire_replay_recovery_floor_if_advanced; then
  echo "replay floor retired before selected resume slot" >&2
  exit 1
fi
test -f "$REPLAY_RECOVERY_FILE"
printf '%s\n' 232 > "$ACTIVE_GENERATION_DIR/slot"
retire_replay_recovery_floor_if_advanced
test ! -e "$REPLAY_RECOVERY_FILE"
REPLAY_RESUME_HEADROOM_SLOTS=0

# Headroom overflow fails before creating either immutable or mutable evidence.
reset_rotation_fixture
REPLAY_RESUME_HEADROOM_SLOTS=100
write_replay_report "$replay_report" \
  999999999999999900 999999999999999900 999999999999999950
if persist_replay_recovery_from_report "$replay_report"; then
  echo "replay resume headroom overflow was accepted" >&2
  exit 1
fi
test ! -e "$REPLAY_RECOVERY_FILE"
test ! -e "$REPLAY_GAP_DIR"
REPLAY_RESUME_HEADROOM_SLOTS=0

# Distinct provider facts cannot collide even when they select the same slot,
# and a pointer with tampered P or S never detaches from immutable evidence.
reset_rotation_fixture
publish_replay_gap_record 123 123 232 200
publish_replay_gap_record 123 123 232 201
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200-232.json"
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-201-232.json"
publish_replay_recovery_floor 123 123 232 200
cp "$REPLAY_RECOVERY_FILE" "$REPLAY_RECOVERY_FILE.valid"
sed 's/"provider_available_slot":200/"provider_available_slot":202/' \
  "$REPLAY_RECOVERY_FILE.valid" > "$REPLAY_RECOVERY_FILE"
if load_replay_recovery_floor; then
  echo "tampered provider replay floor was accepted" >&2
  exit 1
fi
sed 's/"selected_resume_slot":232/"selected_resume_slot":233/' \
  "$REPLAY_RECOVERY_FILE.valid" > "$REPLAY_RECOVERY_FILE"
if load_replay_recovery_floor; then
  echo "tampered selected replay slot was accepted" >&2
  exit 1
fi
rm -f "$REPLAY_RECOVERY_FILE.valid"

# Even canonical, mutually matching v2 evidence cannot authorize a resume
# cushion larger than the recorder's absolute 10,000-slot safety bound.
reset_rotation_fixture
mkdir "$REPLAY_GAP_DIR"
oversized_gap_payload=$(replay_gap_payload 123 123 10201 200)
printf '%s\n' "$oversized_gap_payload" > \
  "$REPLAY_GAP_DIR/replay-gap-123-123-200-10201.json"
printf \
  '{"anchor_slot":123,"cluster_id":"%s","origin_node_id":"%s","provider_available_slot":200,"requested_slot":123,"schema_version":2,"selected_resume_slot":10201,"source_id":"%s"}\n' \
  "$CLUSTER_ID" "$ORIGIN_NODE_ID" "$SOURCE_ID" > "$REPLAY_RECOVERY_FILE"
if validate_replay_gap_record 123 123 10201 200; then
  echo "oversized immutable replay cushion was accepted" >&2
  exit 1
fi
if load_replay_recovery_floor; then
  echo "oversized persisted replay cushion was accepted" >&2
  exit 1
fi

# A validated replay-window failure first creates immutable generation evidence,
# then advances the durable monitoring pointer without rotating or deleting data.
reset_rotation_fixture
replay_report=$fixture_root/replay-report.json
write_replay_report "$replay_report" 123 123 200
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
test "$REPLAY_RECOVERY_ANCHOR_SLOT" -eq 123
test "$REPLAY_PROVIDER_AVAILABLE_SLOT" -eq 200
test "$REPLAY_MIN_RESUME_SLOT" -eq 200
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200-200.json"
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
test -f "$REPLAY_GAP_DIR/replay-gap-123-200-250-250.json"

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
test -f "$REPLAY_GAP_DIR/replay-gap-123-123-200-200.json"
test -f "$REPLAY_GAP_DIR/replay-gap-123-200-250-250.json"
write_replay_report "$replay_report" 260 260 300
persist_replay_recovery_from_report "$replay_report"
load_replay_recovery_floor
test "$REPLAY_RECOVERY_ANCHOR_SLOT" -eq 260
test "$REPLAY_MIN_RESUME_SLOT" -eq 300

reset_rotation_fixture
mkdir "$REPLAY_GAP_DIR"
printf '%s\n' tampered > "$REPLAY_GAP_DIR/replay-gap-123-123-200-200.json"
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
test -f "$ACTIVE_GENERATION_DIR/replay-gaps/replay-gap-123-123-200-200.json"
test -f "$SEALED_GENERATION_DIR/slot-00000000000000000123/replay-gaps/replay-gap-123-123-200-200.json"

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
test -f "$SEALED_GENERATION_DIR/slot-00000000000000000200/replay-gaps/replay-gap-123-123-200-200.json"
test ! -e "$ACTIVE_GENERATION_DIR/replay-gaps/replay-gap-123-123-200-200.json"

printf '%s\n' "linux raw recorder bounded-cache tests: ok"
