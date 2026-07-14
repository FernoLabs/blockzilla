#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
supervisor=$repo_root/scripts/linux-raw-grpc-recorder.sh
fixture_root=$(mktemp -d "${TMPDIR:-/tmp}/blockzilla-alert-test.XXXXXX")
trap 'rm -rf "$fixture_root"' EXIT

export BLOCKZILLA_RAW_OUTPUT_DIR=$fixture_root/output
export BLOCKZILLA_RAW_STATE_DIR=$fixture_root/state
export BLOCKZILLA_TELEGRAM_ENABLED=true
export BLOCKZILLA_TELEGRAM_BOT_TOKEN_FILE=$repo_root/deploy/dokploy/telegram-bot-token.example
export BLOCKZILLA_TELEGRAM_CHAT_ID=-1001234567890
export BLOCKZILLA_TELEGRAM_CURL_BIN=/usr/bin/true
export BLOCKZILLA_TELEGRAM_ALERT_COOLDOWN_SECS=900

# Load definitions without entering the supervisor's command dispatch or main loop.
eval "$(sed '/^if \[ "${1:-}" = --healthcheck \]; then/,$d' "$supervisor")"

mkdir -p "$ALERT_STATE_DIR" "$RESUME_COVERAGE_EVENT_DIR"

make_event() {
  requested_slot=$1
  first_slot=$2
  observed_slot=$3
  event_id=$("$GENERATION_PYTHON_BIN" - \
    "$requested_slot" "$first_slot" "$observed_slot" <<'PY'
import hashlib
import struct
import sys

slots = tuple(int(value) for value in sys.argv[1:])
print(hashlib.sha256(
    b"blockzilla-grpc-resume-coverage-warning-v1" + struct.pack("<QQQ", *slots)
).hexdigest())
PY
  )
  printf '{"event_id":"%s","schema_version":1,"requested_overlap_slot":%s,"first_delivered_slot":%s,"observed_later_slot":%s,"written_unix_secs":123}\n' \
    "$event_id" "$requested_slot" "$first_slot" "$observed_slot" \
    > "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
}

# A delivered durable event gets an event-specific marker before it is removed.
make_event 100 104 104
event_a=$event_id
monitor_resume_coverage_alert
test "$ALERT_DELIVERY_RESULT" = sent
test ! -e "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_a"
grep -q '^Problem: Upstream gRPC data gap detected$' \
  "$(alert_file resume_coverage active)"
grep -q 'Impact: Hetzner already had slot 100. The provider resumed at slot 104' \
  "$(alert_file resume_coverage active)"
grep -q '^Action:' "$(alert_file resume_coverage active)"

# A crash-replayed copy of the same delivered event is removed without another send.
make_event 100 104 104
monitor_resume_coverage_alert
test ! -e "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_a"

# A non-canonical delivery marker cannot bypass the pending event.
make_event 104 108 108
printf '%s\ngarbage\n' "$event_a" > "$RESUME_COVERAGE_DELIVERED_FILE"
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
printf '%s\n' "$event_a" > "$RESUME_COVERAGE_DELIVERED_FILE"
rm -f "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

# A different event inside the global cooldown is coalesced into the already-sent
# incident and removed so it cannot block durable recording.
make_event 104 108 108
event_b=$event_id
monitor_resume_coverage_alert
test "$ALERT_DELIVERY_RESULT" = suppressed
test ! -e "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_a"

# Corrupt, forged, duplicate-key, symlinked, and oversized events remain in place.
printf '{\n' > "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

forged_id=$(printf 'f%.0s' {1..64})
printf '{"event_id":"%s","schema_version":1,"requested_overlap_slot":108,"first_delivered_slot":112,"observed_later_slot":112,"written_unix_secs":123}\n' \
  "$forged_id" > "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

make_event 108 112 112
duplicate_event_id=$event_id
printf '{"event_id":"%s","schema_version":1,"schema_version":1,"requested_overlap_slot":108,"first_delivered_slot":112,"observed_later_slot":112,"written_unix_secs":123}\n' \
  "$duplicate_event_id" > "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

printf '{"event_id":"%s","schema_version":1,"requested_overlap_slot":108,"first_delivered_slot":112,"observed_later_slot":112,"written_unix_secs":123,"extra":NaN}\n' \
  "$duplicate_event_id" > "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

printf '\357\273\277{"event_id":"%s","schema_version":1,"requested_overlap_slot":108,"first_delivered_slot":112,"observed_later_slot":112,"written_unix_secs":123}\n' \
  "$duplicate_event_id" > "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

printf '{"event_id":"%s","schema_version":1,"requested_overlap_slot":108,"first_delivered_slot":112,"observed_later_slot":112,"written_unix_secs":123,"extra":0}\n' \
  "$duplicate_event_id" > "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

rm -f "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
printf 'target\n' > "$fixture_root/symlink-target"
ln -s "$fixture_root/symlink-target" "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
monitor_resume_coverage_alert
test -L "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
rm -f "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

dd if=/dev/zero of="$ACTIVE_RESUME_COVERAGE_EVENT_FILE" bs=4097 count=1 \
  >/dev/null 2>&1
monitor_resume_coverage_alert
test "$(wc -c < "$ACTIVE_RESUME_COVERAGE_EVENT_FILE" | tr -d ' ')" = 4097
rm -f "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

# A successful send whose durable delivery marker cannot be written stays pending.
make_event 112 116 116
event_marker_failure=$event_id
discard_alert resume_coverage
saved_delivery_file=$RESUME_COVERAGE_DELIVERED_FILE
RESUME_COVERAGE_DELIVERED_FILE=$fixture_root/blocked-delivery-marker
mkdir "$RESUME_COVERAGE_DELIVERED_FILE.$$"
monitor_resume_coverage_alert
test "$ALERT_DELIVERY_RESULT" = sent
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test ! -e "$RESUME_COVERAGE_DELIVERED_FILE"
rm -rf "$RESUME_COVERAGE_DELIVERED_FILE.$$"
RESUME_COVERAGE_DELIVERED_FILE=$saved_delivery_file
rm -f "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

# Failed delivery leaves the exact durable event pending for a later pass.
make_event 116 120 120
event_c=$event_id
discard_alert resume_coverage
TELEGRAM_CURL_BIN=/usr/bin/false
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_a"

# A one-shot incident that failed to send is retried on a later monitor pass.
discard_alert recorder_restarting
raise_alert recorder_restarting ERROR "fixture restart"
test -s "$(alert_file recorder_restarting active)"
test ! -e "$(alert_file recorder_restarting delivered)"
TELEGRAM_CURL_BIN=/usr/bin/true
retry_pending_alerts
test -s "$(alert_file recorder_restarting active)"
test -s "$(alert_file recorder_restarting delivered)"

# The incident remains active until recovery, which is sent after the incident.
clear_alert recorder_restarting "fixture recovered"
test ! -e "$(alert_file recorder_restarting active)"
test ! -e "$(alert_file recorder_restarting delivered)"

# A failed replay-floor persistence attempt must remain one open incident while
# retries run without an authoritative marker. It recovers only after a trusted
# floor existed and passed the next loop's checks.
raise_alert replay_recovery_failed CRITICAL "fixture replay failure"
REPLAY_MIN_RESUME_SLOT=
clear_replay_recovery_alert_if_floor_was_authoritative false
test -e "$(alert_file replay_recovery_failed active)"
test -e "$(alert_file replay_recovery_failed delivered)"
REPLAY_MIN_RESUME_SLOT=200
clear_replay_recovery_alert_if_floor_was_authoritative true
test ! -e "$(alert_file replay_recovery_failed active)"
test ! -e "$(alert_file replay_recovery_failed delivered)"
test "$(alert_title replay_recovery_failed)" = "Provider-gap recovery paused"

# Stopping the monitor must interrupt its interval immediately. Otherwise an
# already-finished replay probe waits 30 seconds while the provider floor moves.
MONITOR_INTERVAL_SECS=30
sleep 30 &
quick_stop_child_pid=$!
start_child_monitor "$quick_stop_child_pid"
sleep 1
quick_stop_started=$(date +%s)
kill -TERM "$monitor_pid"
wait "$monitor_pid"
quick_stop_elapsed=$(($(date +%s) - quick_stop_started))
test "$quick_stop_elapsed" -le 2
kill -TERM "$quick_stop_child_pid" 2>/dev/null || true
wait "$quick_stop_child_pid" 2>/dev/null || true
monitor_pid=

# Disk incidents recover only after their configured hysteresis margin.
MIN_FREE_BYTES=100
DISK_WARN_FREE_BYTES=200
DISK_CRITICAL_RECOVERY_BYTES=110
DISK_WARNING_RECOVERY_BYTES=210
update_disk_alerts 50
test -e "$(alert_file disk_space active)"
update_disk_alerts 105
test -e "$(alert_file disk_space active)"
update_disk_alerts 150
test -e "$(alert_file disk_space active)"
update_disk_alerts 205
test -e "$(alert_file disk_space active)"
update_disk_alerts 220
test ! -e "$(alert_file disk_space active)"

# Backblaze usage counts the whole account and escalates independently near the
# decimal 10 GB allowance. Recovery requires the configured hysteresis margin.
B2_USAGE_ALLOWANCE_BYTES=250
B2_USAGE_WARNING_BYTES=150
B2_USAGE_CRITICAL_BYTES=200
B2_USAGE_WARNING_RECOVERY_BYTES=130
b2_usage_send_count=0
telegram_send() {
  b2_usage_send_count=$((b2_usage_send_count + 1))
  return 0
}
update_b2_usage_alerts 160
test "$b2_usage_send_count" -eq 1
test -e "$(alert_file b2_usage active)"
load_alert_delivery_state "$(alert_file b2_usage delivered)"
test "$ALERT_DELIVERED_LEVEL" = WARNING
update_b2_usage_alerts 210
test "$b2_usage_send_count" -eq 2
test -e "$(alert_file b2_usage active)"
load_alert_delivery_state "$(alert_file b2_usage delivered)"
test "$ALERT_DELIVERED_LEVEL" = CRITICAL
update_b2_usage_alerts 185
test "$b2_usage_send_count" -eq 2
test -e "$(alert_file b2_usage active)"
update_b2_usage_alerts 175
test "$b2_usage_send_count" -eq 2
test -e "$(alert_file b2_usage active)"
update_b2_usage_alerts 125
test "$b2_usage_send_count" -eq 3
test ! -e "$(alert_file b2_usage active)"

usage_report=$fixture_root/b2-account-usage.json
printf '%s\n' '{"schema_version":1,"scope_complete":true,"total_stored_bytes":123}' > "$usage_report"
test "$(b2_usage_report_bytes "$usage_report")" = 123
printf '%s\n' '{"schema_version":1,"scope_complete":false,"total_stored_bytes":123}' > "$usage_report"
if b2_usage_report_bytes "$usage_report" >/dev/null 2>&1; then
  echo "incomplete Backblaze usage report was accepted" >&2
  exit 1
fi

# A steady incident sends exactly one opening even after the old reminder
# interval. A severity escalation and the eventual recovery each send once.
telegram_send_count=0
telegram_send() {
  telegram_send_count=$((telegram_send_count + 1))
  return 0
}
test "$(human_bytes 402268160)" = "383.6 MiB"
test "$(human_decimal_bytes 1206880746)" = "1.2 GB"
discard_alert generation_backlog
raise_alert generation_backlog WARNING \
  "Cause: fixture backlog\nImpact: fixture impact\nAction: fixture action"
test "$telegram_send_count" -eq 1
printf '0 WARNING\n' > "$(alert_file generation_backlog delivered)"
raise_alert generation_backlog WARNING \
  "Cause: same fixture backlog\nImpact: still active\nAction: fixture action"
test "$telegram_send_count" -eq 1
raise_alert generation_backlog ERROR \
  "Cause: fixture upload failure\nImpact: still active\nAction: fixture action"
test "$telegram_send_count" -eq 2
raise_alert generation_backlog ERROR \
  "Cause: same fixture upload failure\nImpact: still active\nAction: fixture action"
test "$telegram_send_count" -eq 2
clear_alert generation_backlog "Fixture pipeline recovered."
test "$telegram_send_count" -eq 3

# A quick fail/recover/fail flap is silent, but a CRITICAL escalation bypasses
# the reopen debounce immediately.
raise_alert generation_backlog WARNING \
  "Cause: fixture flap\nImpact: fixture impact\nAction: fixture action"
test "$telegram_send_count" -eq 3
test -e "$(alert_file generation_backlog silent)"
raise_alert generation_backlog CRITICAL \
  "Cause: fixture critical flap\nImpact: capture paused\nAction: fixture action"
test "$telegram_send_count" -eq 4
test ! -e "$(alert_file generation_backlog silent)"
clear_alert generation_backlog "Fixture critical flap recovered."
test "$telegram_send_count" -eq 5

# A non-escalating quick flap is silent in both directions. If the reopened
# incident remains active past the debounce, it is promoted to one new opening.
raise_alert generation_backlog ERROR \
  "Cause: fixture quiet flap\nImpact: fixture impact\nAction: fixture action"
test "$telegram_send_count" -eq 5
test -e "$(alert_file generation_backlog silent)"
clear_alert generation_backlog "Fixture flap recovered."
test "$telegram_send_count" -eq 5
raise_alert generation_backlog ERROR \
  "Cause: persistent fixture flap\nImpact: fixture impact\nAction: fixture action"
test "$telegram_send_count" -eq 5
printf '0\n' > "$(alert_file generation_backlog closed)"
raise_alert generation_backlog ERROR \
  "Cause: persistent fixture flap\nImpact: fixture impact\nAction: fixture action"
test "$telegram_send_count" -eq 6

# Corrupt or partially persisted delivery state cannot suppress escalation.
printf '%s\n' 'corrupt-state' > "$(alert_file generation_backlog delivered)"
raise_alert generation_backlog CRITICAL \
  "Cause: fixture state repair\nImpact: capture paused\nAction: fixture action"
test "$telegram_send_count" -eq 7
discard_alert generation_backlog

# The uploader and disk monitor can report the same pipeline incident at the
# same instant. Per-key serialization must produce one opening and, later, one
# escalation without corrupting the atomic delivery marker. The deliberately
# slow sender makes the duplicate-send race deterministic without the lock.
concurrent_send_log=$fixture_root/concurrent-telegram-sends
concurrent_start_file=$fixture_root/concurrent-telegram-start
: > "$concurrent_send_log"
telegram_send() {
  sleep 1
  printf '%s\n' sent >> "$concurrent_send_log"
  return 0
}

# A worker crash cannot strand the incident lock. The kernel releases its
# descriptor and the next monitor pass can deliver normally.
concurrent_crash_ready=$fixture_root/concurrent-lock-crash-ready
(
  acquire_alert_lock generation_backlog
  : > "$concurrent_crash_ready"
  while :; do :; done
) &
concurrent_crash_pid=$!
while [ ! -e "$concurrent_crash_ready" ]; do
  sleep 0.01
done
kill -KILL "$concurrent_crash_pid"
wait "$concurrent_crash_pid" 2>/dev/null || true
raise_alert generation_backlog WARNING \
  "Cause: fixture after lock-owner crash\nImpact: fixture impact\nAction: fixture action"
test "$(wc -l < "$concurrent_send_log" | tr -d ' ')" -eq 1
test -f "$ALERT_STATE_DIR/.generation_backlog.lock"
test ! -L "$ALERT_STATE_DIR/.generation_backlog.lock"
discard_alert generation_backlog
: > "$concurrent_send_log"

for concurrent_worker in 1 2; do
  (
    while [ ! -e "$concurrent_start_file" ]; do
      sleep 0.01
    done
    raise_alert generation_backlog ERROR \
      "Cause: concurrent fixture failure\nImpact: fixture impact\nAction: fixture action"
  ) &
  eval "concurrent_pid_$concurrent_worker=$!"
done
: > "$concurrent_start_file"
wait "$concurrent_pid_1"
wait "$concurrent_pid_2"
test "$(wc -l < "$concurrent_send_log" | tr -d ' ')" -eq 1
load_alert_delivery_state "$(alert_file generation_backlog delivered)"
test "$ALERT_DELIVERED_LEVEL" = ERROR
test -f "$ALERT_STATE_DIR/.generation_backlog.lock"

rm -f "$concurrent_start_file"
for concurrent_worker in 1 2; do
  (
    while [ ! -e "$concurrent_start_file" ]; do
      sleep 0.01
    done
    raise_alert generation_backlog CRITICAL \
      "Cause: concurrent fixture escalation\nImpact: capture paused\nAction: fixture action"
  ) &
  eval "concurrent_pid_$concurrent_worker=$!"
done
: > "$concurrent_start_file"
wait "$concurrent_pid_1"
wait "$concurrent_pid_2"
test "$(wc -l < "$concurrent_send_log" | tr -d ' ')" -eq 2
load_alert_delivery_state "$(alert_file generation_backlog delivered)"
test "$ALERT_DELIVERED_LEVEL" = CRITICAL
test -f "$ALERT_STATE_DIR/.generation_backlog.lock"
discard_alert generation_backlog

telegram_send() {
  telegram_send_count=$((telegram_send_count + 1))
  return 0
}

# In bounded-cache mode the upload worker owns one correlated pipeline
# incident. A known sealed backlog suppresses derivative disk alerts.
saved_cache_mode=$CACHE_MODE
saved_sealed_generation_dir=$SEALED_GENERATION_DIR
saved_generation_backlog_warn_count=$GENERATION_BACKLOG_WARN_COUNT
CACHE_MODE=b2-generations
SEALED_GENERATION_DIR=$fixture_root/correlation-sealed
GENERATION_BACKLOG_WARN_COUNT=2
mkdir -p \
  "$SEALED_GENERATION_DIR/slot-00000000000000000001" \
  "$SEALED_GENERATION_DIR/slot-00000000000000000002"
discard_alert disk_space
correlation_send_count=$telegram_send_count
update_disk_alerts 50
test "$telegram_send_count" -eq $((correlation_send_count + 1))
test ! -e "$(alert_file disk_space active)"
test -e "$(alert_file generation_backlog active)"
grep -q ' CRITICAL$' "$(alert_file generation_backlog delivered)"

# A recovered uploader cannot close the correlated incident until both the
# backlog and the warning-level headroom (including hysteresis) are healthy.
discard_alert generation_backlog
raise_alert generation_backlog ERROR \
  "Cause: fixture upload block\nImpact: fixture impact\nAction: fixture action"
test -e "$(alert_file generation_backlog active)"
(
  validate_data_volume() { return 0; }
  upload_one_generation() { return 0; }
  sealed_generation_count() { printf '%s\n' 1; }
  available_bytes() { printf '%s\n' 205; }
  sleep() { exit 0; }
  generation_upload_worker
)
test -e "$(alert_file generation_backlog active)"
(
  validate_data_volume() { return 0; }
  upload_one_generation() { return 0; }
  sealed_generation_count() { printf '%s\n' 1; }
  available_bytes() { printf '%s\n' 220; }
  sleep() { exit 0; }
  generation_upload_worker
)
test ! -e "$(alert_file generation_backlog active)"
CACHE_MODE=$saved_cache_mode
SEALED_GENERATION_DIR=$saved_sealed_generation_dir
GENERATION_BACKLOG_WARN_COUNT=$saved_generation_backlog_warn_count

# Disabling Telegram must not disable the runtime fail-closed volume guard.
TELEGRAM_ENABLED=false
MONITOR_INTERVAL_SECS=1
VOLUME_MARKER=/data/.blockzilla-test-missing-$$
sleep 30 &
guarded_pid=$!
start_child_monitor "$guarded_pid"
wait "$monitor_pid"
if wait "$guarded_pid" 2>/dev/null; then
  echo "runtime volume guard did not stop its child" >&2
  exit 1
fi
if kill -0 "$guarded_pid" 2>/dev/null; then
  echo "runtime volume guard left its child running" >&2
  exit 1
fi
monitor_pid=

# Alert bookkeeping failure is best-effort and cannot terminate the caller.
rm -rf "$ALERT_STATE_DIR"
printf 'not-a-directory\n' > "$ALERT_STATE_DIR"
raise_alert state_failure ERROR "fixture state failure"

printf '%s\n' "linux raw recorder alert tests: ok"
