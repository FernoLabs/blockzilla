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

assert_simple_storage_message() {
  simple_message_file=$1
  test "$(wc -l < "$simple_message_file" | tr -d ' ')" -le 5
  test "$(wc -c < "$simple_message_file" | tr -d ' ')" -le 420
  if grep -Eq 'KiB|MiB|GiB|sealed generation|spill|watermark|durable|WAL|cursor|allowance bytes' \
    "$simple_message_file"
  then
    echo "storage alert contains internal jargon" >&2
    exit 1
  fi
}

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
grep -q '^Blockzilla backup - WARNING$' \
  "$(alert_file resume_coverage active)"
grep -q '^gRPC reconnect not verified$' \
  "$(alert_file resume_coverage active)"
grep -q '^Status: The provider did not replay saved slot 100 after reconnect\.$' \
  "$(alert_file resume_coverage active)"
grep -q '^Data: It later sent slot 104, so coverage between them could not be verified\.$' \
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

# Reconnect and missing-slot incidents close only when recording moves forward.
# Their clear messages must not claim that uncertain or missing data was fixed.
saved_telegram_send_definition=$(declare -f telegram_send)
focused_alert_message=$fixture_root/focused-alert-message
focused_alert_send_count=0
telegram_send() {
  focused_alert_send_count=$((focused_alert_send_count + 1))
  printf '%s\n' "$1" > "$focused_alert_message"
  return 0
}
discard_alert resume_coverage
raise_alert resume_coverage WARNING \
  "Status: The provider did not replay saved slot 100 after reconnect.
Data: It later sent slot 104, so coverage between them could not be verified.
Action: Compare that range with another source and repair any gaps."
grep -q '^Blockzilla backup - WARNING$' "$focused_alert_message"
grep -q '^gRPC reconnect not verified$' "$focused_alert_message"
grep -q '^Data: It later sent slot 104, so coverage between them could not be verified\.$' \
  "$focused_alert_message"
test "$(wc -l < "$focused_alert_message" | tr -d ' ')" -le 5
focused_alert_count_before_retire=$focused_alert_send_count
retire_alert resume_coverage
test "$focused_alert_send_count" -eq "$focused_alert_count_before_retire"
test ! -e "$(alert_file resume_coverage active)"
test -e "$(alert_file resume_coverage closed)"

# A confirmed provider-history gap uses the stronger missing-slot wording even
# though its clear message only means that recording resumed.
discard_alert provider_replay_gap
raise_alert provider_replay_gap WARNING \
  "Status: This backup is missing slots 101-103.
Data: Earlier saved blocks are safe; this range is not in the backup.
Action: Repair the missing range from another source if needed."
clear_alert provider_replay_gap \
  "Status: New gRPC blocks are being saved again.
Data: The previously reported missing slots are still missing.
Action: Repair that slot range from another source if needed."
grep -q '^Blockzilla backup - RECORDING RESUMED$' "$focused_alert_message"
grep -q '^Some gRPC slots are missing$' "$focused_alert_message"
grep -q '^Data: The previously reported missing slots are still missing\.$' \
  "$focused_alert_message"
test "$(wc -l < "$focused_alert_message" | tr -d ' ')" -le 5

# The first append after a planned provider-history recovery also emits an
# overlap warning. It belongs to the stronger active provider-gap incident and
# must not create a second opening/recovery pair.
discard_alert resume_coverage
discard_alert provider_replay_gap
make_event 200 204 204
raise_alert provider_replay_gap WARNING \
  "Status: This backup is missing slots 201-203.
Data: Earlier saved blocks are safe; this range is not in the backup.
Action: Repair the missing range from another source if needed."
focused_alert_count_before_correlation=$focused_alert_send_count
monitor_resume_coverage_alert
test "$ALERT_DELIVERY_RESULT" = suppressed
test ! -e "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test ! -e "$(alert_file resume_coverage active)"
test -e "$(alert_file provider_replay_gap active)"
test "$focused_alert_send_count" -eq "$focused_alert_count_before_correlation"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_a"
discard_alert provider_replay_gap

# An incident created by the previous release has no journal-growth floor.
# Bootstrap it at the current size and require a later append before recovery.
discard_alert resume_coverage
raise_alert resume_coverage WARNING \
  "Status: The provider did not replay saved slot 300 after reconnect.
Data: It later sent slot 304, so coverage between them could not be verified.
Action: Compare that range with another source and repair any gaps."
rm -f "$(alert_file resume_coverage journal_size)"
mkdir -p "$OUTPUT_DIR"
printf '%s\n' first-record > "$JOURNAL_FILE"
saved_journal_size_definition=$(declare -f journal_size)
journal_size() {
  if [ -f "$JOURNAL_FILE" ]; then
    wc -c < "$JOURNAL_FILE" | tr -d ' '
  else
    printf '%s\n' 0
  fi
}
clear_alert_after_journal_growth resume_coverage \
  "Status: New gRPC blocks are being saved again.
Data: The earlier reconnect range is still unverified.
Action: Compare that range with another source if needed." \
  silent
test -e "$(alert_file resume_coverage active)"
test -s "$(alert_file resume_coverage journal_size)"
focused_alert_count_before_silent_close=$focused_alert_send_count
printf '%s\n' second-record >> "$JOURNAL_FILE"
clear_alert_after_journal_growth resume_coverage \
  "Status: New gRPC blocks are being saved again.
Data: The earlier reconnect range is still unverified.
Action: Compare that range with another source if needed." \
  silent
test ! -e "$(alert_file resume_coverage active)"
test -e "$(alert_file resume_coverage closed)"
test "$focused_alert_send_count" -eq "$focused_alert_count_before_silent_close"

# Silent retirement never drops an opening that Telegram has not accepted.
discard_alert resume_coverage
printf '%s\n' retry-base > "$JOURNAL_FILE"
remember_alert_journal_floor resume_coverage
saved_focused_sender_definition=$(declare -f telegram_send)
telegram_send() { return 1; }
raise_alert resume_coverage WARNING \
  "Status: The provider did not replay saved slot 400 after reconnect.
Data: It later sent slot 404, so coverage between them could not be verified.
Action: Compare that range with another source and repair any gaps."
test -e "$(alert_file resume_coverage active)"
test ! -e "$(alert_file resume_coverage delivered)"
printf '%s\n' retry-growth >> "$JOURNAL_FILE"
clear_alert_after_journal_growth resume_coverage \
  "Status: New gRPC blocks are being saved again.
Data: The earlier reconnect range is still unverified.
Action: Compare that range with another source if needed." \
  silent
test -e "$(alert_file resume_coverage active)"
test ! -e "$(alert_file resume_coverage delivered)"
eval "$saved_focused_sender_definition"
focused_alert_count_before_retry=$focused_alert_send_count
clear_alert_after_journal_growth resume_coverage \
  "Status: New gRPC blocks are being saved again.
Data: The earlier reconnect range is still unverified.
Action: Compare that range with another source if needed." \
  silent
test ! -e "$(alert_file resume_coverage active)"
test "$focused_alert_send_count" -eq "$((focused_alert_count_before_retry + 1))"
eval "$saved_journal_size_definition"
rm -f "$JOURNAL_FILE"

# Pending incident files from the previous message layout remain retryable
# during a rolling deployment.
discard_alert recorder_restarting
printf '%s\n' \
  'Blockzilla backup alert' \
  'Problem: Recorder stopped unexpectedly' \
  'Severity: ERROR' \
  'Node: fixture' \
  'Time (UTC): fixture' \
  '' \
  'fixture old-format incident' \
  > "$(alert_file recorder_restarting active)"
retry_pending_alert recorder_restarting
load_alert_delivery_state "$(alert_file recorder_restarting delivered)"
test "$ALERT_DELIVERED_LEVEL" = ERROR
grep -q '^Severity: ERROR$' "$focused_alert_message"
discard_alert recorder_restarting

# Local hard-floor incidents written by the previous release are still
# recognized so the recovery path cannot strand them.
discard_alert generation_backlog
printf '%s\n' \
  'Cause: The disk-first Hetzner cache reached its safety floor before Backblaze spill cleanup recovered headroom.' \
  > "$(alert_file generation_backlog active)"
generation_backlog_local_incident_active
discard_alert generation_backlog
eval "$saved_telegram_send_definition"

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
test "$(alert_title replay_recovery_failed)" = "gRPC recovery is paused"

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

# Generic disk openings and recoveries obey the same five-line, decimal-unit
# contract as the Backblaze-specific messages.
saved_telegram_send_definition=$(declare -f telegram_send)
disk_contract_message=$fixture_root/disk-contract-message
telegram_send() {
  printf '%s\n' "$1" > "$disk_contract_message"
  return 0
}
saved_disk_min_free=$MIN_FREE_BYTES
saved_disk_warn_free=$DISK_WARN_FREE_BYTES
saved_disk_critical_recovery=$DISK_CRITICAL_RECOVERY_BYTES
saved_disk_warning_recovery=$DISK_WARNING_RECOVERY_BYTES
MIN_FREE_BYTES=400000000
DISK_WARN_FREE_BYTES=800000000
DISK_CRITICAL_RECOVERY_BYTES=450000000
DISK_WARNING_RECOVERY_BYTES=850000000
discard_alert disk_space
update_disk_alerts 350000000
grep -q '^Blockzilla backup - CRITICAL$' "$disk_contract_message"
grep -q '^Storage: 350 MB free; backup needs 400 MB\.$' \
  "$disk_contract_message"
assert_simple_storage_message "$disk_contract_message"
update_disk_alerts 900000000
grep -q '^Blockzilla backup - RECOVERED$' "$disk_contract_message"
grep -q '^Resolved: Backup disk is low$' "$disk_contract_message"
grep -q '^Storage: 900 MB free\.$' "$disk_contract_message"
assert_simple_storage_message "$disk_contract_message"
MIN_FREE_BYTES=$saved_disk_min_free
DISK_WARN_FREE_BYTES=$saved_disk_warn_free
DISK_CRITICAL_RECOVERY_BYTES=$saved_disk_critical_recovery
DISK_WARNING_RECOVERY_BYTES=$saved_disk_warning_recovery
eval "$saved_telegram_send_definition"

# Backblaze usage counts the whole account and uses simple GB copy near the
# decimal 10 GB limit. Recovery requires the configured hysteresis margin.
B2_USAGE_ALLOWANCE_BYTES=10000000000
B2_USAGE_WARNING_BYTES=8000000000
B2_USAGE_CRITICAL_BYTES=9500000000
B2_USAGE_WARNING_RECOVERY_BYTES=7500000000
b2_usage_send_count=0
b2_usage_last_message=$fixture_root/b2-usage-last-message
telegram_send() {
  b2_usage_send_count=$((b2_usage_send_count + 1))
  printf '%s\n' "$1" > "$b2_usage_last_message"
  return 0
}
update_b2_usage_alerts 8100000000
test "$b2_usage_send_count" -eq 1
test -e "$(alert_file b2_usage active)"
grep -q '^Storage: 8\.1 GB used of 10 GB\.$' "$b2_usage_last_message"
assert_simple_storage_message "$b2_usage_last_message"
load_alert_delivery_state "$(alert_file b2_usage delivered)"
test "$ALERT_DELIVERED_LEVEL" = WARNING
update_b2_usage_alerts 9600000000
test "$b2_usage_send_count" -eq 2
test -e "$(alert_file b2_usage active)"
grep -q '^Storage: 9\.6 GB used of 10 GB\.$' "$b2_usage_last_message"
assert_simple_storage_message "$b2_usage_last_message"
load_alert_delivery_state "$(alert_file b2_usage delivered)"
test "$ALERT_DELIVERED_LEVEL" = CRITICAL
update_b2_usage_alerts 9200000000
test "$b2_usage_send_count" -eq 2
test -e "$(alert_file b2_usage active)"
update_b2_usage_alerts 8500000000
test "$b2_usage_send_count" -eq 2
test -e "$(alert_file b2_usage active)"
update_b2_usage_alerts 7400000000
test "$b2_usage_send_count" -eq 3
test ! -e "$(alert_file b2_usage active)"
grep -q '^Blockzilla backup - RECOVERED$' "$b2_usage_last_message"
grep -q '^Resolved: Backblaze storage is filling up$' "$b2_usage_last_message"
grep -q '^Storage: 7\.4 GB used of 10 GB\.$' "$b2_usage_last_message"
assert_simple_storage_message "$b2_usage_last_message"

# A cap/API outage already represented by the correlated pipeline incident
# must not fan out into a second Backblaze-usage-check opening and recovery.
discard_alert generation_backlog
discard_alert b2_usage_check_failed
b2_usage_scan_failed_alert
test -e "$(alert_file b2_usage_check_failed active)"
raise_alert generation_backlog ERROR \
  "Cause: fixture provider cap\nImpact: fixture impact\nAction: fixture action"
b2_usage_scan_failed_alert
test ! -e "$(alert_file b2_usage_check_failed active)"
b2_usage_scan_recovered_alert
test ! -e "$(alert_file b2_usage_check_failed active)"
discard_alert generation_backlog
b2_usage_scan_failed_alert
test -e "$(alert_file b2_usage_check_failed active)"
b2_usage_scan_recovered_alert
test ! -e "$(alert_file b2_usage_check_failed active)"

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
test "$(human_decimal_bytes 402427904)" = "402 MB"
test "$(human_decimal_bytes 1207959552)" = "1.2 GB"
test "$(human_decimal_bytes 3221225472)" = "3.2 GB"
test "$(human_decimal_bytes 10000000000)" = "10 GB"
test "$(human_decimal_bytes 999999999)" = "1 GB"
test "$(human_decimal_bytes 999999)" = "<1 MB"
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

# In disk-first cache mode a sealed backlog is healthy local retention. B2 is
# untouched above the low watermark, then FIFO draining stays active until the
# high watermark. Reserve arithmetic wins over percentages when it is larger.
saved_cache_mode=$CACHE_MODE
saved_sealed_generation_dir=$SEALED_GENERATION_DIR
saved_generation_backlog_warn_count=$GENERATION_BACKLOG_WARN_COUNT
saved_min_free_bytes=$MIN_FREE_BYTES
saved_max_generation_bytes=$MAX_GENERATION_BYTES
saved_spill_start_percent=$GENERATION_SPILL_START_PERCENT
saved_spill_recovery_percent=$GENERATION_SPILL_RECOVERY_PERCENT
CACHE_MODE=b2-generations
SEALED_GENERATION_DIR=$fixture_root/correlation-sealed
GENERATION_BACKLOG_WARN_COUNT=2
MIN_FREE_BYTES=100
MAX_GENERATION_BYTES=80
GENERATION_SPILL_START_PERCENT=25
GENERATION_SPILL_RECOVERY_PERCENT=35
mkdir -p \
  "$SEALED_GENERATION_DIR/slot-00000000000000000001" \
  "$SEALED_GENERATION_DIR/slot-00000000000000000002"

generation_spill_thresholds 1000
test "$GENERATION_SPILL_START_BYTES" -eq 250
test "$GENERATION_SPILL_RECOVERY_BYTES" -eq 350
MIN_FREE_BYTES=402653184
MAX_GENERATION_BYTES=402653184
generation_spill_thresholds 3221225472
test "$GENERATION_SPILL_START_BYTES" -eq 805306368
test "$GENERATION_SPILL_RECOVERY_BYTES" -eq 1207959552
MIN_FREE_BYTES=100
MAX_GENERATION_BYTES=80
generation_spill_active=false
update_generation_spill_state 249 1000
test "$generation_spill_active" = true
update_generation_spill_state 349 1000
test "$generation_spill_active" = true
update_generation_spill_state 350 1000
test "$generation_spill_active" = false

# A large intentional backlog with healthy headroom neither uploads nor alerts.
healthy_upload_log=$fixture_root/healthy-spill-uploads
: > "$healthy_upload_log"
discard_alert generation_backlog
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() { printf '%s\n' 500; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() { printf '%s\n' upload >> "$healthy_upload_log"; }
  sleep() { exit 0; }
  generation_upload_worker
)
test ! -s "$healthy_upload_log"
test ! -e "$(alert_file generation_backlog active)"

# Once pressure starts, successful uploads run without a 60-second delay until
# the high watermark is reached. The free-space sequence models two deletions.
drain_upload_log=$fixture_root/drain-spill-uploads
drain_space_log=$fixture_root/drain-spill-space-calls
: > "$drain_upload_log"
: > "$drain_space_log"
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  available_bytes() {
    drain_call_count=$(wc -l < "$drain_space_log" | tr -d ' ')
    printf '%s\n' call >> "$drain_space_log"
    case "$drain_call_count" in
      0) printf '%s\n' 200 ;;
      1|2) printf '%s\n' 250 ;;
      *) printf '%s\n' 400 ;;
    esac
  }
  upload_one_generation() { printf '%s\n' upload >> "$drain_upload_log"; }
  sleep() { exit 0; }
  generation_upload_worker
)
test "$(wc -l < "$drain_upload_log" | tr -d ' ')" -eq 2
test ! -e "$(alert_file generation_backlog active)"

# A rotation transaction defers spill cleanup with status 75 and must not spin.
deferred_upload_log=$fixture_root/deferred-spill-uploads
: > "$deferred_upload_log"
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() { printf '%s\n' 200; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() {
    printf '%s\n' upload >> "$deferred_upload_log"
    return 75
  }
  sleep() { exit 0; }
  generation_upload_worker
)
test "$(wc -l < "$deferred_upload_log" | tr -d ' ')" -eq 1

# A nominally successful spill that changes neither queue depth nor free space
# is a real pressure-path failure, not healthy local retention.
no_progress_upload_log=$fixture_root/no-progress-spill-uploads
: > "$no_progress_upload_log"
discard_alert generation_backlog
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() { printf '%s\n' 200; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() {
    printf '%s\n' upload >> "$no_progress_upload_log"
    return 0
  }
  sleep() { exit 0; }
  generation_upload_worker
)
test "$(wc -l < "$no_progress_upload_log" | tr -d ' ')" -eq 1
grep -q 'Backblaze upload finished, but Hetzner space did not increase' \
  "$(alert_file generation_backlog active)"
grep -q ' ERROR$' "$(alert_file generation_backlog delivered)"
discard_alert generation_backlog

# Typed capacity statuses produce precise operator actions. Unknown failures
# remain generic and cannot be promoted from an untyped provider response.
generation_upload_failure_details 20
case "$pipeline_cause $pipeline_action" in
  *Backblaze\ download\ limit\ reached*Raise\ the\ Backblaze\ download\ limit*) ;;
  *) echo "download-cap action was not specific" >&2; exit 1 ;;
esac
generation_upload_failure_details 21
case "$pipeline_cause $pipeline_action" in
  *Backblaze\ Class\ C\ limit\ reached*Raise\ the\ Backblaze\ Class\ C\ limit*) ;;
  *) echo "Class-C action was not specific" >&2; exit 1 ;;
esac
generation_upload_failure_details 22
case "$pipeline_cause $pipeline_action" in
  *Backblaze\ storage\ limit\ reached*Increase\ the\ Backblaze\ storage\ limit*) ;;
  *) echo "storage-cap action was not specific" >&2; exit 1 ;;
esac
generation_upload_failure_details 37
case "$pipeline_cause $pipeline_action" in
  *download\ limit\ reached*|*Class\ C\ limit\ reached*|*storage\ limit\ reached*)
    echo "generic upload failure inherited a typed cap message" >&2
    exit 1
    ;;
esac

# The live Class-C + hard-floor case is five plain lines with decimal MB/GB,
# no raw byte values, and no storage implementation jargon.
production_spill_message=$fixture_root/production-spill-message
discard_alert generation_backlog
(
  MIN_FREE_BYTES=402653184
  MAX_GENERATION_BYTES=402653184
  GENERATION_SPILL_START_PERCENT=25
  GENERATION_SPILL_RECOVERY_PERCENT=35
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 6; }
  available_bytes() { printf '%s\n' 402427904; }
  filesystem_capacity_bytes() { printf '%s\n' 3221225472; }
  upload_one_generation() { return 21; }
  telegram_send() { printf '%s\n' "$1" > "$production_spill_message"; }
  sleep() { exit 0; }
  generation_upload_worker
)
grep -q '^Blockzilla backup - CRITICAL$' "$production_spill_message"
grep -q '^Status: Backblaze Class C limit reached\. Backup is paused; saved data is safe\.$' \
  "$production_spill_message"
grep -q '^Storage: 402 MB free\. 6 backup batches waiting locally\.$' \
  "$production_spill_message"
grep -q '^Action: Raise the Backblaze Class C limit or wait for the daily reset\.$' \
  "$production_spill_message"
assert_simple_storage_message "$production_spill_message"
if grep -Eq '402427904|402653184|3221225472' "$production_spill_message"; then
  echo "storage alert exposed raw byte values" >&2
  exit 1
fi
discard_alert generation_backlog

# One correlated incident opens once, escalates only at the hard floor, and
# sends one recovery after high-water headroom returns.
spill_send_log=$fixture_root/spill-telegram-sends
: > "$spill_send_log"
telegram_send() { printf '%s\n' sent >> "$spill_send_log"; }
discard_alert generation_backlog
run_failed_spill() {
  failed_spill_free=$1
  (
    validate_data_volume() { return 0; }
    sealed_generation_count() { printf '%s\n' 3; }
    available_bytes() { printf '%s\n' "$failed_spill_free"; }
    filesystem_capacity_bytes() { printf '%s\n' 1000; }
    upload_one_generation() { return 21; }
    sleep() { exit 0; }
    generation_upload_worker
  )
}
run_failed_spill 200
test "$(wc -l < "$spill_send_log" | tr -d ' ')" -eq 1
grep -q 'Backblaze Class C limit reached' \
  "$(alert_file generation_backlog active)"
test ! -e "$(alert_file generation_upload_failed active)"
run_failed_spill 200
test "$(wc -l < "$spill_send_log" | tr -d ' ')" -eq 1
run_failed_spill 50
test "$(wc -l < "$spill_send_log" | tr -d ' ')" -eq 2
grep -q ' CRITICAL$' "$(alert_file generation_backlog delivered)"
run_failed_spill 50
test "$(wc -l < "$spill_send_log" | tr -d ' ')" -eq 2
# Headroom alone is not proof that a remote cap recovered; do not send a false
# recovery without a successful exact upload (unless no sealed backlog remains).
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() { printf '%s\n' 400; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() { return 99; }
  sleep() { exit 0; }
  generation_upload_worker
)
test "$(wc -l < "$spill_send_log" | tr -d ' ')" -eq 2
test -e "$(alert_file generation_backlog active)"
recovery_space_log=$fixture_root/recovery-spill-space-calls
: > "$recovery_space_log"
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() {
    recovery_call_count=$(wc -l < "$recovery_space_log" | tr -d ' ')
    printf '%s\n' call >> "$recovery_space_log"
    if [ "$recovery_call_count" -eq 0 ]; then
      printf '%s\n' 200
    else
      printf '%s\n' 400
    fi
  }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() { return 0; }
  sleep() { exit 0; }
  generation_upload_worker
)
test "$(wc -l < "$spill_send_log" | tr -d ' ')" -eq 3
test ! -e "$(alert_file generation_backlog active)"

# If any queue, free-space, filesystem-capacity, or spill-policy input cannot
# be inspected, the worker performs no upload/delete action and opens one
# comprehensible fail-closed incident.
failed_gate_upload_log=$fixture_root/failed-gate-uploads
: > "$failed_gate_upload_log"
discard_alert generation_backlog
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() { printf '%s\n' 100; }
  filesystem_capacity_bytes() { return 1; }
  upload_one_generation() { printf '%s\n' upload >> "$failed_gate_upload_log"; }
  sleep() { exit 0; }
  generation_upload_worker
)
test ! -s "$failed_gate_upload_log"
grep -q 'Cannot read Hetzner disk size' \
  "$(alert_file generation_backlog active)"
grep -q ' ERROR$' "$(alert_file generation_backlog delivered)"
discard_alert generation_backlog
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() { return 1; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() { printf '%s\n' upload >> "$failed_gate_upload_log"; }
  sleep() { exit 0; }
  generation_upload_worker
)
test ! -s "$failed_gate_upload_log"
grep -q 'Cannot read Hetzner free space' \
  "$(alert_file generation_backlog active)"
discard_alert generation_backlog
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() { printf '%s\n' 100; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  update_generation_spill_state() { return 1; }
  upload_one_generation() { printf '%s\n' upload >> "$failed_gate_upload_log"; }
  sleep() { exit 0; }
  generation_upload_worker
)
test ! -s "$failed_gate_upload_log"
grep -q 'Storage safety check failed' \
  "$(alert_file generation_backlog active)"
discard_alert generation_backlog
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { return 1; }
  available_bytes() { printf '%s\n' 100; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() { printf '%s\n' upload >> "$failed_gate_upload_log"; }
  sleep() { exit 0; }
  generation_upload_worker
)
test ! -s "$failed_gate_upload_log"
grep -q 'Cannot read local backup files' \
  "$(alert_file generation_backlog active)"

# Once every gate reads cleanly and headroom is above the high watermark, only
# a gate-origin incident recovers even while healthy local generations remain.
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() { printf '%s\n' 500; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() { printf '%s\n' upload >> "$failed_gate_upload_log"; }
  sleep() { exit 0; }
  generation_upload_worker
)
test ! -s "$failed_gate_upload_log"
test ! -e "$(alert_file generation_backlog active)"

# A measurement failure after a verified uploader return is also visible; the
# worker cannot infer cleanup progress from a stale pre-upload snapshot.
post_upload_space_calls=$fixture_root/post-upload-space-calls
: > "$post_upload_space_calls"
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 3; }
  available_bytes() {
    post_upload_call_count=$(wc -l < "$post_upload_space_calls" | tr -d ' ')
    printf '%s\n' call >> "$post_upload_space_calls"
    if [ "$post_upload_call_count" -eq 0 ]; then
      printf '%s\n' 200
    else
      return 1
    fi
  }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() { return 0; }
  sleep() { exit 0; }
  generation_upload_worker
)
grep -q 'Cannot read Hetzner free space' \
  "$(alert_file generation_backlog active)"
discard_alert generation_backlog

# The spill worker gets one hard-floor monitor pass to report the exact remote
# cause. A typed active incident is preserved while that worker is alive.
discard_alert disk_space
discard_alert generation_backlog
update_disk_alerts 200
test ! -e "$(alert_file disk_space active)"
test ! -e "$(alert_file generation_backlog active)"
sleep 30 &
upload_worker_pid=$!
update_disk_alerts 50
test ! -e "$(alert_file disk_space active)"
test ! -e "$(alert_file generation_backlog active)"
test -e "$(alert_file generation_backlog handoff)"
raise_alert generation_backlog ERROR \
  "Cause: fixture typed Class C incident
Impact: fixture impact
Action: fixture action"
update_disk_alerts 50
grep -q 'fixture typed Class C incident' \
  "$(alert_file generation_backlog active)"
kill -TERM "$upload_worker_pid" 2>/dev/null || true
wait "$upload_worker_pid" 2>/dev/null || true
upload_worker_pid=
discard_alert generation_backlog

# If the worker is dead, the disk monitor immediately owns the same-key
# fail-safe CRITICAL instead of allowing capture to pause silently.
update_disk_alerts 50
test ! -e "$(alert_file disk_space active)"
test -e "$(alert_file generation_backlog active)"
grep -q ' CRITICAL$' "$(alert_file generation_backlog delivered)"

# Restoring local headroom above the recovery watermark proves a generic local
# hard-floor incident recovered, even with retained generations and no B2
# upload. Remote-cap incidents have a different durable cause and cannot be
# closed by this path.
hard_floor_recovery_upload_log=$fixture_root/hard-floor-recovery-uploads
: > "$hard_floor_recovery_upload_log"
(
  validate_data_volume() { return 0; }
  sealed_generation_count() { printf '%s\n' 2; }
  available_bytes() { printf '%s\n' 500; }
  filesystem_capacity_bytes() { printf '%s\n' 1000; }
  upload_one_generation() {
    printf '%s\n' upload >> "$hard_floor_recovery_upload_log"
  }
  sleep() { exit 0; }
  generation_upload_worker
)
test ! -s "$hard_floor_recovery_upload_log"
test ! -e "$(alert_file generation_backlog active)"

# Re-open the dead-worker fallback before checking the zero-backlog wording.
update_disk_alerts 50
test -e "$(alert_file generation_backlog active)"
rmdir \
  "$SEALED_GENERATION_DIR/slot-00000000000000000001" \
  "$SEALED_GENERATION_DIR/slot-00000000000000000002"
update_disk_alerts 50
test -e "$(alert_file generation_backlog active)"
grep -q ' CRITICAL$' "$(alert_file generation_backlog delivered)"
discard_alert generation_backlog
CACHE_MODE=$saved_cache_mode
SEALED_GENERATION_DIR=$saved_sealed_generation_dir
GENERATION_BACKLOG_WARN_COUNT=$saved_generation_backlog_warn_count
MIN_FREE_BYTES=$saved_min_free_bytes
MAX_GENERATION_BYTES=$saved_max_generation_bytes
GENERATION_SPILL_START_PERCENT=$saved_spill_start_percent
GENERATION_SPILL_RECOVERY_PERCENT=$saved_spill_recovery_percent

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
