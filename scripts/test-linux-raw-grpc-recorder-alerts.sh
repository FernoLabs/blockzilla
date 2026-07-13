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
  event_id=$1
  printf '{"event_id":"%s","schema_version":1,"requested_overlap_slot":100,"first_delivered_slot":104,"observed_later_slot":104,"written_unix_secs":123}\n' \
    "$event_id" > "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
}

event_a=$(printf 'a%.0s' {1..64})
event_b=$(printf 'b%.0s' {1..64})
event_c=$(printf 'c%.0s' {1..64})

# A delivered durable event gets an event-specific marker before it is removed.
make_event "$event_a"
monitor_resume_coverage_alert
test ! -e "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_a"

# A different event inside the global cooldown must remain pending, not be mistaken for A.
make_event "$event_b"
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_a"

# Once the cooldown expires, B is marked and removed.
printf '0\n' > "$(alert_file resume_coverage last)"
monitor_resume_coverage_alert
test ! -e "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_b"

# Failed delivery leaves the exact durable event pending for a later pass.
make_event "$event_c"
printf '0\n' > "$(alert_file resume_coverage last)"
TELEGRAM_CURL_BIN=/usr/bin/false
monitor_resume_coverage_alert
test -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"
test "$(cat "$RESUME_COVERAGE_DELIVERED_FILE")" = "$event_b"

# A one-shot incident that failed to send is retried on a later monitor pass.
rm -f "$(alert_file recorder_restarting active)" "$(alert_file recorder_restarting last)"
raise_alert recorder_restarting ERROR "fixture restart"
test -s "$(alert_file recorder_restarting active)"
test ! -e "$(alert_file recorder_restarting last)"
TELEGRAM_CURL_BIN=/usr/bin/true
retry_pending_alerts
test -s "$(alert_file recorder_restarting active)"
test -s "$(alert_file recorder_restarting last)"

# The incident remains active until recovery, which is sent after the incident.
clear_alert recorder_restarting "fixture recovered"
test ! -e "$(alert_file recorder_restarting active)"
test ! -e "$(alert_file recorder_restarting last)"

# Disk incidents recover only after their configured hysteresis margin.
MIN_FREE_BYTES=100
DISK_WARN_FREE_BYTES=200
DISK_CRITICAL_RECOVERY_BYTES=110
DISK_WARNING_RECOVERY_BYTES=210
update_disk_alerts 50
test -e "$(alert_file disk_critical active)"
update_disk_alerts 105
test -e "$(alert_file disk_critical active)"
update_disk_alerts 150
test ! -e "$(alert_file disk_critical active)"
test -e "$(alert_file disk_warning active)"
update_disk_alerts 205
test -e "$(alert_file disk_warning active)"
update_disk_alerts 220
test ! -e "$(alert_file disk_warning active)"

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
