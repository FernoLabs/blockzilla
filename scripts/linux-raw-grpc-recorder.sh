#!/bin/sh
set -eu

# Reconnect supervisor for the raw-only Yellowstone recorder. The Rust process owns
# the durable cursor. This wrapper owns disk admission, restart backoff, outbound
# alerts, and a metadata-only health signal suitable for a concurrently appended
# journal.

umask 077
export LC_ALL=C

BIN=${BLOCKZILLA_RAW_BIN:-/usr/local/bin/blockzilla-live-producer}
OUTPUT_DIR=${BLOCKZILLA_RAW_OUTPUT_DIR:-/data/grpc-raw}
INITIAL_FROM_SLOT=${BLOCKZILLA_RAW_FROM_SLOT:-}
MIN_FREE_BYTES=${BLOCKZILLA_RAW_MIN_FREE_BYTES:-21474836480}
MAX_BLOCKS=${BLOCKZILLA_RAW_MAX_BLOCKS:-1000000000}
TIMEOUT_SECS=${BLOCKZILLA_RAW_TIMEOUT_SECS:-31536000}
IDLE_TIMEOUT_SECS=${BLOCKZILLA_RAW_IDLE_TIMEOUT_SECS:-180}
RESTART_DELAY_SECS=${BLOCKZILLA_RAW_RESTART_DELAY_SECS:-5}
LOW_DISK_RECHECK_SECS=${BLOCKZILLA_RAW_LOW_DISK_RECHECK_SECS:-60}
COMPRESSION_LEVEL=${BLOCKZILLA_RAW_COMPRESSION_LEVEL:-1}
SEGMENT_TARGET_BYTES=${BLOCKZILLA_RAW_SEGMENT_TARGET_BYTES:-268435456}
MAX_RECORD_BYTES=${BLOCKZILLA_RAW_MAX_RECORD_BYTES:-134217728}
REQUIRE_COMPLETE_POH=${BLOCKZILLA_RAW_REQUIRE_COMPLETE_POH:-true}
CLUSTER_ID=${BLOCKZILLA_RAW_CLUSTER_ID:-solana-mainnet}
ORIGIN_NODE_ID=${BLOCKZILLA_RAW_ORIGIN_NODE_ID:-hetzner-dokploy-01}
SOURCE_ID=${BLOCKZILLA_RAW_SOURCE_ID:-grpc-raw-hetzner-backup}

# The legacy mode keeps one ever-growing dedicated-volume spool. The bounded
# cache mode stores one live generation at a stable path and exposes immutable
# sealed generations only after a seeded successor has become active.
CACHE_MODE=${BLOCKZILLA_RAW_CACHE_MODE:-legacy}
CACHE_ROOT=${BLOCKZILLA_RAW_CACHE_ROOT:-/data/grpc-cache}
MAX_GENERATION_BYTES=${BLOCKZILLA_RAW_MAX_GENERATION_BYTES:-402653184}
GENERATION_BACKLOG_WARN_COUNT=${BLOCKZILLA_RAW_GENERATION_BACKLOG_WARN_COUNT:-2}
GENERATION_UPLOAD_RETRY_SECS=${BLOCKZILLA_RAW_GENERATION_UPLOAD_RETRY_SECS:-60}
GENERATION_SPILL_START_PERCENT=${BLOCKZILLA_RAW_B2_SPILL_START_PERCENT:-25}
GENERATION_SPILL_RECOVERY_PERCENT=${BLOCKZILLA_RAW_B2_SPILL_RECOVERY_PERCENT:-35}
REPLAY_RESUME_HEADROOM_SLOTS=${BLOCKZILLA_RAW_REPLAY_RESUME_HEADROOM_SLOTS:-100}
MAX_REPLAY_RESUME_HEADROOM_SLOTS=10000
GENERATION_UPLOADER_BIN=${BLOCKZILLA_RAW_GENERATION_UPLOADER_BIN:-/usr/local/bin/blockzilla-s3-upload}
GENERATION_CREDENTIALS_FILE=${BLOCKZILLA_B2_CREDENTIALS_FILE:-/run/secrets/backblaze_credentials}
GENERATION_REMOTE_PREFIX=${BLOCKZILLA_B2_REMOTE_PREFIX:-grpc-raw/v1}
GENERATION_PYTHON_BIN=${BLOCKZILLA_RAW_GENERATION_PYTHON_BIN:-python3}
B2_USAGE_ALERT_ENABLED=${BLOCKZILLA_B2_USAGE_ALERT_ENABLED:-false}
B2_USAGE_ALLOWANCE_BYTES=${BLOCKZILLA_B2_USAGE_ALLOWANCE_BYTES:-10000000000}
B2_USAGE_WARNING_BYTES=${BLOCKZILLA_B2_USAGE_WARNING_BYTES:-8000000000}
B2_USAGE_CRITICAL_BYTES=${BLOCKZILLA_B2_USAGE_CRITICAL_BYTES:-9500000000}
B2_USAGE_RECOVERY_HYSTERESIS_BYTES=${BLOCKZILLA_B2_USAGE_RECOVERY_HYSTERESIS_BYTES:-500000000}
B2_USAGE_CHECK_INTERVAL_SECS=${BLOCKZILLA_B2_USAGE_CHECK_INTERVAL_SECS:-300}
B2_USAGE_OVER_LIMIT_CHECK_INTERVAL_SECS=${BLOCKZILLA_B2_USAGE_OVER_LIMIT_CHECK_INTERVAL_SECS:-21600}

ACTIVE_GENERATION_DIR=$CACHE_ROOT/active
SEALED_GENERATION_DIR=$CACHE_ROOT/sealed
GENERATION_RECEIPT_DIR=${BLOCKZILLA_RAW_CACHE_RECEIPT_DIR:-$CACHE_ROOT/receipts}
GENERATION_MONITORING_DIR=$CACHE_ROOT/monitoring
GENERATION_ROTATION_MARKER=$CACHE_ROOT/.rotation
B2_USAGE_REPORT_FILE=$GENERATION_MONITORING_DIR/b2-account-usage.json
REPLAY_RECOVERY_FILE=$GENERATION_MONITORING_DIR/replay-recovery-floor.json
REPLAY_GAP_DIR=$ACTIVE_GENERATION_DIR/replay-gaps
if [ "$CACHE_MODE" = b2-generations ]; then
  OUTPUT_DIR=$ACTIVE_GENERATION_DIR
fi

# Telegram is deliberately outbound-only. The token is read from a file and fed
# to curl through standard input, never through curl's argument vector.
TELEGRAM_ENABLED=${BLOCKZILLA_TELEGRAM_ENABLED:-false}
TELEGRAM_BOT_TOKEN_FILE=${BLOCKZILLA_TELEGRAM_BOT_TOKEN_FILE:-/run/secrets/telegram_bot_token}
TELEGRAM_CHAT_ID=${BLOCKZILLA_TELEGRAM_CHAT_ID:-}
TELEGRAM_MESSAGE_THREAD_ID=${BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID:-}
TELEGRAM_ALERT_COOLDOWN_SECS=${BLOCKZILLA_TELEGRAM_ALERT_COOLDOWN_SECS:-900}
TELEGRAM_CURL_BIN=${BLOCKZILLA_TELEGRAM_CURL_BIN:-curl}
DISK_WARN_FREE_BYTES=${BLOCKZILLA_RAW_DISK_WARN_FREE_BYTES:-32212254720}
DISK_RECOVERY_HYSTERESIS_BYTES=${BLOCKZILLA_RAW_DISK_RECOVERY_HYSTERESIS_BYTES:-1073741824}
MONITOR_INTERVAL_SECS=${BLOCKZILLA_RAW_MONITOR_INTERVAL_SECS:-30}
PRIMARY_SYNC_HEARTBEAT_FILE=${BLOCKZILLA_PRIMARY_SYNC_HEARTBEAT_FILE:-}
PRIMARY_SYNC_STALE_AFTER_SECS=${BLOCKZILLA_PRIMARY_SYNC_STALE_AFTER_SECS:-600}
RAW_STALE_AFTER_SECS=${BLOCKZILLA_RAW_STALE_AFTER_SECS:-180}
STARTUP_GRACE_SECS=${BLOCKZILLA_RAW_STARTUP_GRACE_SECS:-300}

STATE_DIR=${BLOCKZILLA_RAW_STATE_DIR:-/tmp}
STATE_FILE=$STATE_DIR/blockzilla-raw-recorder.state
STARTED_FILE=$STATE_DIR/blockzilla-raw-recorder.started
JOURNAL_FILE=$OUTPUT_DIR/raw-blocks.jsonl
VOLUME_MARKER=${BLOCKZILLA_RAW_VOLUME_MARKER:-/data/.blockzilla-raw-volume}
if [ "$CACHE_MODE" = b2-generations ]; then
  # Incident state must survive container rebuilds. Otherwise every deploy or
  # crash forgets which incidents were already delivered and Telegram receives
  # the same opening alerts again.
  ALERT_STATE_DIR=$GENERATION_MONITORING_DIR/telegram-alerts
else
  ALERT_STATE_DIR=$STATE_DIR/blockzilla-raw-alerts
fi
CHILD_REPORT_FILE=$STATE_DIR/blockzilla-raw-recorder.child.json
if [ "$CACHE_MODE" = b2-generations ]; then
  RESUME_COVERAGE_EVENT_DIR=$GENERATION_MONITORING_DIR
else
  RESUME_COVERAGE_EVENT_DIR=$OUTPUT_DIR/.monitoring
fi
RESUME_COVERAGE_EVENT_FILE=$RESUME_COVERAGE_EVENT_DIR/resume-coverage-warning.json
RESUME_COVERAGE_DELIVERED_FILE=$RESUME_COVERAGE_EVENT_DIR/resume-coverage-warning.delivered
ACTIVE_RESUME_COVERAGE_EVENT_FILE=$RESUME_COVERAGE_EVENT_FILE

validate_data_paths() {
  case "$CACHE_MODE" in
    legacy|b2-generations) ;;
    *)
      echo "BLOCKZILLA_RAW_CACHE_MODE must be legacy or b2-generations" >&2
      return 1
      ;;
  esac
  if [ "$CACHE_MODE" = b2-generations ] \
    && [ "$CACHE_ROOT" != /data/grpc-cache ]
  then
    echo "bounded cache root must be /data/grpc-cache" >&2
    return 1
  fi
  if [ "$CACHE_MODE" = b2-generations ] \
    && [ "$VOLUME_MARKER" != /data/.blockzilla-raw-volume ]
  then
    echo "bounded cache marker must be /data/.blockzilla-raw-volume" >&2
    return 1
  fi
  case "$OUTPUT_DIR" in
    /data|/data/*) ;;
    *)
      echo "BLOCKZILLA_RAW_OUTPUT_DIR must be /data or a child of /data" >&2
      return 1
      ;;
  esac
  case "$OUTPUT_DIR/" in
    */../*|*/./*)
      echo "BLOCKZILLA_RAW_OUTPUT_DIR must not contain . or .. path components" >&2
      return 1
      ;;
  esac
  case "$VOLUME_MARKER" in
    /data/*) ;;
    *)
      echo "BLOCKZILLA_RAW_VOLUME_MARKER must be a child of /data" >&2
      return 1
      ;;
  esac
  case "$VOLUME_MARKER/" in
    */../*|*/./*)
      echo "BLOCKZILLA_RAW_VOLUME_MARKER must not contain . or .. path components" >&2
      return 1
      ;;
  esac
}

validate_data_volume() {
  volume_require_output=${1:-false}
  if [ ! -d /data ]; then
    echo "raw recorder data mount is missing" >&2
    return 1
  fi
  if [ -L "$VOLUME_MARKER" ] || [ ! -f "$VOLUME_MARKER" ] || [ ! -r "$VOLUME_MARKER" ]; then
    echo "raw recorder volume marker is missing, unreadable, or a symlink" >&2
    return 1
  fi

  volume_data_real=$(readlink -f /data) || return 1
  volume_marker_real=$(readlink -f "$VOLUME_MARKER") || return 1
  case "$volume_marker_real" in
    "$volume_data_real"/*) ;;
    *)
      echo "raw recorder volume marker resolves outside /data" >&2
      return 1
      ;;
  esac

  volume_data_device=$(stat -c %d /data) || return 1
  volume_marker_device=$(stat -c %d "$VOLUME_MARKER") || return 1
  if [ "$volume_marker_device" != "$volume_data_device" ]; then
    echo "raw recorder volume marker is on a different filesystem from /data" >&2
    return 1
  fi
  IFS= read -r volume_marker_value < "$VOLUME_MARKER" || return 1
  if [ "$CACHE_MODE" = b2-generations ]; then
    if [ "$volume_marker_value" != blockzilla-raw-cache-v1 ]; then
      echo "raw cache marker does not contain the stable cache identity" >&2
      return 1
    fi
  else
    case "$volume_marker_value" in
      ''|*[!0-9]*)
        echo "raw recorder volume marker does not contain a filesystem device id" >&2
        return 1
        ;;
    esac
    if [ "$volume_marker_value" != "$volume_data_device" ]; then
      echo "raw recorder filesystem device differs from its fail-closed marker" >&2
      return 1
    fi
  fi

  if [ -e "$OUTPUT_DIR" ]; then
    if [ ! -d "$OUTPUT_DIR" ]; then
      echo "raw recorder output path is not a directory" >&2
      return 1
    fi
    volume_output_real=$(readlink -f "$OUTPUT_DIR") || return 1
    case "$volume_output_real" in
      "$volume_data_real"|"$volume_data_real"/*) ;;
      *)
        echo "raw recorder output directory resolves outside /data" >&2
        return 1
        ;;
    esac
    volume_output_device=$(stat -c %d "$OUTPUT_DIR") || return 1
    if [ "$volume_output_device" != "$volume_data_device" ]; then
      echo "raw recorder output directory is on a different filesystem from /data" >&2
      return 1
    fi
  elif [ "$volume_require_output" = true ]; then
    echo "raw recorder output directory is missing" >&2
    return 1
  fi
}

# These defaults keep a quiet connection alive without enabling adaptive HTTP/2
# windows. The endpoint may still choose an uncompressed response.
export BLOCKZILLA_GRPC_ACCEPT_COMPRESSION=${BLOCKZILLA_GRPC_ACCEPT_COMPRESSION:-zstd}
export BLOCKZILLA_GRPC_HTTP2_ADAPTIVE_WINDOW=${BLOCKZILLA_GRPC_HTTP2_ADAPTIVE_WINDOW:-false}
export BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS=${BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS:-30}
export BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS=${BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS:-10}
export BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE=${BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE:-false}

timestamp() {
  date -u +%FT%TZ
}

require_uint() {
  variable_name=$1
  variable_value=$2
  case "$variable_value" in
    ''|*[!0-9]*)
      echo "invalid unsigned integer in $variable_name" >&2
      exit 2
      ;;
  esac
}

write_state() {
  state_value=$1
  state_tmp=$STATE_FILE.$$
  if ! printf '%s %s\n' "$state_value" "$(date +%s)" > "$state_tmp"; then
    echo "$(timestamp) recorder_state write_failed" >&2
    rm -f "$state_tmp" 2>/dev/null || true
    return 0
  fi
  if ! mv -f "$state_tmp" "$STATE_FILE"; then
    echo "$(timestamp) recorder_state publish_failed" >&2
    rm -f "$state_tmp" 2>/dev/null || true
  fi
  return 0
}

validate_telegram_config() {
  case "$TELEGRAM_ENABLED" in
    true) ;;
    false) return 0 ;;
    *)
      echo "BLOCKZILLA_TELEGRAM_ENABLED must be true or false" >&2
      return 1
      ;;
  esac

  if [ -L "$TELEGRAM_BOT_TOKEN_FILE" ] \
    || [ ! -f "$TELEGRAM_BOT_TOKEN_FILE" ] \
    || [ ! -r "$TELEGRAM_BOT_TOKEN_FILE" ]
  then
    echo "Telegram bot-token file is missing, unreadable, or a symlink" >&2
    return 1
  fi
  case "$TELEGRAM_CHAT_ID" in
    -*) telegram_chat_digits=${TELEGRAM_CHAT_ID#-} ;;
    @*)
      telegram_chat_name=${TELEGRAM_CHAT_ID#@}
      case "$telegram_chat_name" in
        ''|*[!A-Za-z0-9_]*)
          echo "invalid Telegram channel username" >&2
          return 1
          ;;
      esac
      telegram_chat_digits=
      ;;
    *) telegram_chat_digits=$TELEGRAM_CHAT_ID ;;
  esac
  if [ -n "$telegram_chat_digits" ]; then
    case "$telegram_chat_digits" in
      *[!0-9]*)
        echo "BLOCKZILLA_TELEGRAM_CHAT_ID must be numeric or an @channel username" >&2
        return 1
        ;;
    esac
  elif [ "${TELEGRAM_CHAT_ID#@}" = "$TELEGRAM_CHAT_ID" ]; then
    echo "BLOCKZILLA_TELEGRAM_CHAT_ID is required" >&2
    return 1
  fi
  case "$TELEGRAM_MESSAGE_THREAD_ID" in
    ''|*[!0-9]*)
      if [ -n "$TELEGRAM_MESSAGE_THREAD_ID" ]; then
        echo "BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID must be an unsigned integer" >&2
        return 1
      fi
      ;;
  esac
  if ! command -v "$TELEGRAM_CURL_BIN" >/dev/null 2>&1; then
    echo "Telegram curl executable is missing" >&2
    return 1
  fi
}

load_telegram_token() {
  telegram_token=
  if ! validate_telegram_config; then
    return 1
  fi
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 1
  fi
  if LC_ALL=C grep -q "$(printf '\r')" "$TELEGRAM_BOT_TOKEN_FILE"; then
    echo "Telegram bot-token file contains a carriage return" >&2
    return 1
  fi
  telegram_token_bytes=$(wc -c < "$TELEGRAM_BOT_TOKEN_FILE" | tr -d ' ')
  case "$telegram_token_bytes" in
    ''|*[!0-9]*|0)
      echo "Telegram bot-token file is empty or unreadable" >&2
      return 1
      ;;
  esac
  if [ "$telegram_token_bytes" -gt 256 ]; then
    echo "Telegram bot-token file is unexpectedly large" >&2
    return 1
  fi
  telegram_newlines=$(tr -cd '\n' < "$TELEGRAM_BOT_TOKEN_FILE" | wc -c | tr -d ' ')
  case "$telegram_newlines" in
    0|1) ;;
    *)
      echo "Telegram bot-token file must contain exactly one line" >&2
      return 1
      ;;
  esac
  telegram_token=$(sed -n '1p' "$TELEGRAM_BOT_TOKEN_FILE")
  telegram_rest=$(sed -n '2,$p' "$TELEGRAM_BOT_TOKEN_FILE")
  if [ -n "$telegram_rest" ]; then
    telegram_token=
    echo "Telegram bot-token file must contain exactly one line" >&2
    return 1
  fi
  case "$telegram_token" in
    *:*) ;;
    *)
      telegram_token=
      echo "Telegram bot token has an invalid shape" >&2
      return 1
      ;;
  esac
  telegram_bot_id=${telegram_token%%:*}
  telegram_bot_secret=${telegram_token#*:}
  case "$telegram_bot_id" in
    ''|*[!0-9]*)
      telegram_token=
      echo "Telegram bot token has an invalid bot ID" >&2
      return 1
      ;;
  esac
  case "$telegram_bot_secret" in
    ''|*[!A-Za-z0-9_-]*|*:*)
      telegram_token=
      echo "Telegram bot token has an invalid secret" >&2
      return 1
      ;;
  esac
}

telegram_send() {
  telegram_body=$1
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 0
  fi
  if ! load_telegram_token; then
    return 1
  fi

  if [ -n "$TELEGRAM_MESSAGE_THREAD_ID" ]; then
    if ! printf 'url = "https://api.telegram.org/bot%s/sendMessage"\n' "$telegram_token" \
      | "$TELEGRAM_CURL_BIN" --config - --proto '=https' --tlsv1.2 \
        --silent --fail --output /dev/null --connect-timeout 2 --max-time 3 \
        --request POST --data-urlencode "chat_id=$TELEGRAM_CHAT_ID" \
        --data-urlencode "message_thread_id=$TELEGRAM_MESSAGE_THREAD_ID" \
        --data-urlencode "text=$telegram_body" \
        --data-urlencode "disable_web_page_preview=true"
    then
      telegram_token=
      return 1
    fi
  else
    if ! printf 'url = "https://api.telegram.org/bot%s/sendMessage"\n' "$telegram_token" \
      | "$TELEGRAM_CURL_BIN" --config - --proto '=https' --tlsv1.2 \
        --silent --fail --output /dev/null --connect-timeout 2 --max-time 3 \
        --request POST --data-urlencode "chat_id=$TELEGRAM_CHAT_ID" \
        --data-urlencode "text=$telegram_body" \
        --data-urlencode "disable_web_page_preview=true"
    then
      telegram_token=
      return 1
    fi
  fi
  telegram_token=
  return 0
}

alert_file() {
  printf '%s/%s.%s\n' "$ALERT_STATE_DIR" "$1" "$2"
}

alert_title() {
  case "$1" in
    recorder_restarting) printf '%s\n' 'Recorder restarting' ;;
    grpc_stale) printf '%s\n' 'No new gRPC blocks' ;;
    volume_invalid) printf '%s\n' 'Backup disk unavailable' ;;
    disk_check_failed) printf '%s\n' 'Cannot check backup disk' ;;
    disk_space) printf '%s\n' 'Backup disk is low' ;;
    disk_critical) printf '%s\n' 'Backup disk critically low' ;;
    disk_warning) printf '%s\n' 'Backup disk running low' ;;
    b2_usage_check_failed) printf '%s\n' 'Cannot check Backblaze storage' ;;
    b2_usage) printf '%s\n' 'Backblaze storage is filling up' ;;
    b2_usage_warning) printf '%s\n' 'Backblaze archive near 10 GB' ;;
    b2_usage_critical) printf '%s\n' 'Backblaze archive almost full' ;;
    primary_sync_stale) printf '%s\n' 'Blockzilla confirmation missing' ;;
    generation_rotation_failed) printf '%s\n' 'Local backup is paused' ;;
    replay_recovery_failed) printf '%s\n' 'gRPC recovery is paused' ;;
    provider_replay_gap) printf '%s\n' 'Some gRPC slots are missing' ;;
    resume_coverage) printf '%s\n' 'gRPC reconnect not verified' ;;
    generation_upload_failed) printf '%s\n' 'Backblaze upload failed' ;;
    generation_backlog) printf '%s\n' 'Backup storage problem' ;;
    *) printf '%s\n' "$1" | tr '_' ' ' ;;
  esac
}

human_decimal_bytes() {
  human_value=$1
  case "$human_value" in
    ''|*[!0-9]*) return 1 ;;
  esac
  if [ "$human_value" -ge 1000000000 ]; then
    human_tenths=$(((human_value + 50000000) / 100000000))
    if [ $((human_tenths % 10)) -eq 0 ]; then
      printf '%s GB' "$((human_tenths / 10))"
    else
      printf '%s.%s GB' "$((human_tenths / 10))" "$((human_tenths % 10))"
    fi
  elif [ "$human_value" -ge 1000000 ]; then
    human_megabytes=$(((human_value + 500000) / 1000000))
    if [ "$human_megabytes" -ge 1000 ]; then
      printf '%s' '1 GB'
    else
      printf '%s MB' "$human_megabytes"
    fi
  else
    printf '%s' '<1 MB'
  fi
}

human_duration() {
  duration_seconds=$1
  case "$duration_seconds" in
    ''|*[!0-9]*) return 1 ;;
  esac
  if [ "$duration_seconds" -lt 120 ]; then
    printf '%s' '1 minute'
  elif [ "$duration_seconds" -lt 3600 ]; then
    printf '%s minutes' "$(((duration_seconds + 30) / 60))"
  elif [ "$duration_seconds" -lt 7200 ]; then
    printf '%s' '1 hour'
  else
    printf '%s hours' "$(((duration_seconds + 1800) / 3600))"
  fi
}

discard_alert() {
  discard_key=$1
  rm -f \
    "$(alert_file "$discard_key" active)" \
    "$(alert_file "$discard_key" delivered)" \
    "$(alert_file "$discard_key" last)" \
    "$(alert_file "$discard_key" level)" \
    "$(alert_file "$discard_key" silent)" \
    "$(alert_file "$discard_key" closed)" \
    "$(alert_file "$discard_key" handoff)" \
    "$(alert_file "$discard_key" journal_size)" \
    2>/dev/null || true
}

alert_level_rank() {
  case "$1" in
    WARNING) printf '%s\n' 1 ;;
    ERROR) printf '%s\n' 2 ;;
    CRITICAL) printf '%s\n' 3 ;;
    *) return 1 ;;
  esac
}

write_alert_delivery_time() {
  delivery_key=$1
  delivery_last=$2
  delivery_now=$3
  delivery_tmp=$delivery_last.$$
  if ! printf '%s\n' "$delivery_now" > "$delivery_tmp"; then
    echo "$(timestamp) telegram_alert cooldown_write_failed key=$delivery_key" >&2
    rm -f "$delivery_tmp" 2>/dev/null || true
    return 1
  fi
  if ! mv -f "$delivery_tmp" "$delivery_last"; then
    echo "$(timestamp) telegram_alert cooldown_publish_failed key=$delivery_key" >&2
    rm -f "$delivery_tmp" 2>/dev/null || true
    return 1
  fi
  return 0
}

load_alert_delivery_state() {
  delivery_state_file=$1
  ALERT_DELIVERED_AT=
  ALERT_DELIVERED_LEVEL=
  if [ -L "$delivery_state_file" ] \
    || [ ! -f "$delivery_state_file" ] \
    || [ ! -r "$delivery_state_file" ]
  then
    return 1
  fi
  delivery_state_bytes=$(wc -c < "$delivery_state_file" | tr -d ' ')
  delivery_state_lines=$(wc -l < "$delivery_state_file" | tr -d ' ')
  case "$delivery_state_bytes" in ''|*[!0-9]*) return 1 ;; esac
  case "$delivery_state_lines" in ''|*[!0-9]*) return 1 ;; esac
  if [ "$delivery_state_bytes" -gt 64 ] || [ "$delivery_state_lines" -ne 1 ]; then
    return 1
  fi
  IFS=' ' read -r ALERT_DELIVERED_AT ALERT_DELIVERED_LEVEL delivery_state_extra \
    < "$delivery_state_file" || return 1
  case "$ALERT_DELIVERED_AT" in
    ''|*[!0-9]*) return 1 ;;
  esac
  [ -z "$delivery_state_extra" ] || return 1
  alert_level_rank "$ALERT_DELIVERED_LEVEL" >/dev/null 2>&1 || return 1
}

write_alert_delivery_state() {
  delivery_key=$1
  delivery_state_file=$2
  delivery_now=$3
  delivery_level=$4
  case "$delivery_now" in
    ''|*[!0-9]*) return 1 ;;
  esac
  alert_level_rank "$delivery_level" >/dev/null || return 1
  delivery_state_tmp=$delivery_state_file.$$
  if ! printf '%s %s\n' "$delivery_now" "$delivery_level" \
    > "$delivery_state_tmp"
  then
    echo "$(timestamp) telegram_alert delivery_state_write_failed key=$delivery_key" >&2
    rm -f "$delivery_state_tmp" 2>/dev/null || true
    return 1
  fi
  if ! mv -f "$delivery_state_tmp" "$delivery_state_file"; then
    echo "$(timestamp) telegram_alert delivery_state_publish_failed key=$delivery_key" >&2
    rm -f "$delivery_state_tmp" 2>/dev/null || true
    return 1
  fi
}

alert_flock_lock_fd() {
  if command -v flock >/dev/null 2>&1; then
    flock -w 20 9
    return $?
  fi
  # macOS does not ship the flock CLI, but Python exposes the same
  # kernel-released advisory lock. The inherited descriptor keeps the lock
  # owned by this shell after the helper exits.
  "$GENERATION_PYTHON_BIN" - 9 20 <<'PY'
import fcntl
import sys
import time

fd = int(sys.argv[1])
deadline = time.monotonic() + float(sys.argv[2])
while True:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        break
    except BlockingIOError:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SystemExit(1)
        time.sleep(min(0.05, remaining))
PY
}

alert_flock_unlock_fd() {
  if command -v flock >/dev/null 2>&1; then
    flock -u 9
    return $?
  fi
  "$GENERATION_PYTHON_BIN" - 9 <<'PY'
import fcntl
import sys

fcntl.flock(int(sys.argv[1]), fcntl.LOCK_UN)
PY
}

acquire_alert_lock() {
  lock_key=$1
  case "$lock_key" in
    ''|*[!A-Za-z0-9_.-]*) return 1 ;;
  esac
  ALERT_LOCK_FILE=$ALERT_STATE_DIR/.$lock_key.lock
  if [ -L "$ALERT_LOCK_FILE" ] \
    || { [ -e "$ALERT_LOCK_FILE" ] && [ ! -f "$ALERT_LOCK_FILE" ]; }
  then
    return 1
  fi
  if ! exec 9>> "$ALERT_LOCK_FILE"; then
    ALERT_LOCK_FILE=
    return 1
  fi
  if ! alert_flock_lock_fd; then
    exec 9>&-
    ALERT_LOCK_FILE=
    return 1
  fi
  return 0
}

release_alert_lock() {
  [ -n "${ALERT_LOCK_FILE:-}" ] || return 0
  alert_flock_unlock_fd 2>/dev/null || true
  exec 9>&-
  ALERT_LOCK_FILE=
}

raise_alert_locked() {
  alert_key=$1
  alert_level=$2
  alert_message=$3
  ALERT_DELIVERY_RESULT=disabled
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 0
  fi
  mkdir -p "$ALERT_STATE_DIR" 2>/dev/null || return 0
  alert_active=$(alert_file "$alert_key" active)
  alert_delivery_file=$(alert_file "$alert_key" delivered)
  alert_silent_file=$(alert_file "$alert_key" silent)
  alert_closed_file=$(alert_file "$alert_key" closed)
  alert_heading=$(alert_title "$alert_key")
  # Telegram already shows the send time, and this deployment has one backup
  # node. Keep the useful information visible without a metadata preamble.
  alert_text=$(printf 'Blockzilla backup - %s\n%s\n%s' \
    "$alert_level" "$alert_heading" "$alert_message")
  if ! printf '%s\n' "$alert_text" > "$alert_active"; then
    ALERT_DELIVERY_RESULT=failed
    echo "$(timestamp) telegram_alert state_write_failed key=$alert_key" >&2
    return 0
  fi
  alert_now=$(date +%s)
  alert_previous=
  alert_previous_level=
  if load_alert_delivery_state "$alert_delivery_file"; then
    alert_previous=$ALERT_DELIVERED_AT
    alert_previous_level=$ALERT_DELIVERED_LEVEL
  elif [ -e "$alert_delivery_file" ] || [ -L "$alert_delivery_file" ]; then
    # Corrupt state must not suppress a real incident forever.
    rm -f "$alert_delivery_file" 2>/dev/null || true
  fi
  alert_closed=
  if [ -r "$alert_closed_file" ]; then
    IFS= read -r alert_closed < "$alert_closed_file" || alert_closed=
    case "$alert_closed" in
      ''|*[!0-9]*) alert_closed= ;;
    esac
  fi
  if [ -e "$alert_silent_file" ]; then
    if [ -n "$alert_closed" ] \
      && [ "$alert_now" -ge "$alert_closed" ] \
      && [ $((alert_now - alert_closed)) -lt "$TELEGRAM_ALERT_COOLDOWN_SECS" ]
    then
      alert_previous_rank=$(alert_level_rank "$alert_previous_level" 2>/dev/null \
        || printf '%s\n' 0)
      alert_current_rank=$(alert_level_rank "$alert_level" 2>/dev/null \
        || printf '%s\n' 0)
      if [ "$alert_current_rank" -le "$alert_previous_rank" ]; then
        ALERT_DELIVERY_RESULT=suppressed
        return 0
      fi
      # A silent reopen that becomes more severe is announced immediately.
      rm -f "$alert_silent_file" "$alert_closed_file" 2>/dev/null || true
      alert_closed=
    else
      rm -f "$alert_silent_file" "$alert_delivery_file" \
        "$alert_closed_file" 2>/dev/null || true
      alert_previous=
      alert_previous_level=
      alert_closed=
    fi
  fi
  if [ -z "$alert_previous" ] \
    && [ -n "$alert_closed" ] \
    && [ "$alert_now" -ge "$alert_closed" ] \
    && [ $((alert_now - alert_closed)) -lt "$TELEGRAM_ALERT_COOLDOWN_SECS" ]
  then
    # A just-recovered incident that flaps open again is tracked but stays
    # silent. If it remains open beyond the debounce, a later monitor pass
    # promotes it to a normal opening alert.
    printf '%s\n' "$alert_closed" > "$alert_silent_file" 2>/dev/null || true
    write_alert_delivery_state "$alert_key" "$alert_delivery_file" \
      "$alert_now" "$alert_level" || true
    ALERT_DELIVERY_RESULT=suppressed
    return 0
  elif [ -n "$alert_closed" ]; then
    rm -f "$alert_closed_file" 2>/dev/null || true
  fi
  # Normal monitor passes refresh the on-disk incident details but never send
  # reminders. One opening plus one recovery is enough; failed opening sends
  # remain pending and are retried by retry_pending_alerts().
  if [ -n "$alert_previous" ]; then
    alert_previous_rank=$(alert_level_rank "$alert_previous_level" 2>/dev/null \
      || printf '%s\n' 0)
    alert_current_rank=$(alert_level_rank "$alert_level" 2>/dev/null \
      || printf '%s\n' 0)
    if [ "$alert_current_rank" -le "$alert_previous_rank" ]; then
      ALERT_DELIVERY_RESULT=suppressed
      return 0
    fi
  fi
  if ! telegram_send "$alert_text"; then
    ALERT_DELIVERY_RESULT=failed
    echo "$(timestamp) telegram_alert delivery_failed key=$alert_key" >&2
    return 0
  fi
  ALERT_DELIVERY_RESULT=sent
  write_alert_delivery_state "$alert_key" "$alert_delivery_file" \
    "$alert_now" "$alert_level" || true
  rm -f "$(alert_file "$alert_key" last)" \
    "$(alert_file "$alert_key" level)" 2>/dev/null || true
  rm -f "$alert_silent_file" "$alert_closed_file" 2>/dev/null || true
  return 0
}

raise_alert() {
  lock_raise_key=$1
  ALERT_DELIVERY_RESULT=failed
  mkdir -p "$ALERT_STATE_DIR" 2>/dev/null || return 0
  if ! acquire_alert_lock "$lock_raise_key"; then
    echo "$(timestamp) telegram_alert state_lock_unavailable key=$lock_raise_key" >&2
    return 0
  fi
  raise_alert_locked "$@"
  lock_raise_status=$?
  release_alert_lock
  return "$lock_raise_status"
}

raise_alert_once() {
  once_key=$1
  once_level=$2
  once_message=$3
  raise_alert "$once_key" "$once_level" "$once_message"
}

retry_pending_alert_locked() {
  retry_key=$1
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 0
  fi
  retry_active=$(alert_file "$retry_key" active)
  retry_delivery_file=$(alert_file "$retry_key" delivered)
  retry_silent_file=$(alert_file "$retry_key" silent)
  if [ ! -e "$retry_active" ] || [ -e "$retry_silent_file" ]
  then
    return 0
  fi
  if load_alert_delivery_state "$retry_delivery_file"; then
    return 0
  fi
  if [ -e "$retry_delivery_file" ] || [ -L "$retry_delivery_file" ]; then
    rm -f "$retry_delivery_file" 2>/dev/null || true
  fi
  if [ -L "$retry_active" ] || [ ! -f "$retry_active" ] || [ ! -r "$retry_active" ]; then
    echo "$(timestamp) telegram_alert pending_incident_unreadable key=$retry_key" >&2
    return 0
  fi
  retry_bytes=$(wc -c < "$retry_active" | tr -d ' ')
  case "$retry_bytes" in
    ''|*[!0-9]*|0)
      echo "$(timestamp) telegram_alert pending_incident_invalid key=$retry_key" >&2
      return 0
      ;;
  esac
  if [ "$retry_bytes" -gt 4096 ]; then
    echo "$(timestamp) telegram_alert pending_incident_oversize key=$retry_key" >&2
    return 0
  fi
  retry_text=$(sed -n '1,100p' "$retry_active")
  retry_level=$(sed -n '1s/^Blockzilla backup - //p' "$retry_active")
  if ! alert_level_rank "$retry_level" >/dev/null 2>&1; then
    # Accept incident files written by the previous message layout during a
    # rolling deployment.
    retry_level=$(sed -n 's/^Severity: //p' "$retry_active" | head -n 1)
  fi
  alert_level_rank "$retry_level" >/dev/null 2>&1 || return 0
  if ! telegram_send "$retry_text"; then
    echo "$(timestamp) telegram_alert delivery_failed key=$retry_key" >&2
    return 0
  fi
  retry_now=$(date +%s)
  write_alert_delivery_state "$retry_key" "$retry_delivery_file" \
    "$retry_now" "$retry_level" || true
  return 0
}

retry_pending_alert() {
  lock_retry_key=$1
  mkdir -p "$ALERT_STATE_DIR" 2>/dev/null || return 0
  if ! acquire_alert_lock "$lock_retry_key"; then
    echo "$(timestamp) telegram_alert state_lock_unavailable key=$lock_retry_key" >&2
    return 0
  fi
  retry_pending_alert_locked "$@"
  lock_retry_status=$?
  release_alert_lock
  return "$lock_retry_status"
}

retry_pending_alerts() {
  for retry_key in \
    recorder_restarting \
    grpc_stale \
    volume_invalid \
    disk_check_failed \
    disk_space \
    b2_usage_check_failed \
    b2_usage \
    primary_sync_stale \
    generation_rotation_failed \
    replay_recovery_failed \
    provider_replay_gap \
    generation_backlog
  do
    retry_pending_alert "$retry_key"
  done
  return 0
}

clear_alert_locked() {
  alert_key=$1
  alert_message=$2
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 0
  fi
  alert_active=$(alert_file "$alert_key" active)
  alert_delivery_file=$(alert_file "$alert_key" delivered)
  alert_silent_file=$(alert_file "$alert_key" silent)
  alert_closed_file=$(alert_file "$alert_key" closed)
  if [ ! -e "$alert_active" ]; then
    return 0
  fi
  if [ -e "$alert_silent_file" ]; then
    alert_closed_now=$(date +%s)
    write_alert_delivery_time "$alert_key" "$alert_closed_file" \
      "$alert_closed_now" || true
    rm -f "$alert_active" "$alert_delivery_file" "$alert_silent_file" \
      "$(alert_file "$alert_key" last)" \
      "$(alert_file "$alert_key" level)" \
      "$(alert_file "$alert_key" handoff)" 2>/dev/null || true
    return 0
  fi
  retry_pending_alert_locked "$alert_key"
  if ! load_alert_delivery_state "$alert_delivery_file"; then
    return 0
  fi
  alert_heading=$(alert_title "$alert_key")
  case "$alert_key" in
    provider_replay_gap)
      alert_recovery_state='RECORDING RESUMED'
      alert_recovery_heading=$alert_heading
      ;;
    *)
      alert_recovery_state=RECOVERED
      alert_recovery_heading="Resolved: $alert_heading"
      ;;
  esac
  alert_text=$(printf 'Blockzilla backup - %s\n%s\n%s' \
    "$alert_recovery_state" "$alert_recovery_heading" "$alert_message")
  if ! telegram_send "$alert_text"; then
    echo "$(timestamp) telegram_recovery delivery_failed key=$alert_key" >&2
    return 0
  fi
  alert_closed_now=$(date +%s)
  write_alert_delivery_time "$alert_key" "$alert_closed_file" \
    "$alert_closed_now" || true
  if ! rm -f "$alert_active" "$alert_delivery_file" "$alert_silent_file" \
    "$(alert_file "$alert_key" last)" \
    "$(alert_file "$alert_key" level)" \
    "$(alert_file "$alert_key" handoff)"
  then
    echo "$(timestamp) telegram_recovery state_remove_failed key=$alert_key" >&2
  fi
  return 0
}

clear_alert() {
  lock_clear_key=$1
  mkdir -p "$ALERT_STATE_DIR" 2>/dev/null || return 0
  if ! acquire_alert_lock "$lock_clear_key"; then
    echo "$(timestamp) telegram_recovery state_lock_unavailable key=$lock_clear_key" >&2
    return 0
  fi
  clear_alert_locked "$@"
  lock_clear_status=$?
  release_alert_lock
  return "$lock_clear_status"
}

retire_alert_locked() {
  retire_key=$1
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 0
  fi
  retire_active=$(alert_file "$retire_key" active)
  [ -e "$retire_active" ] || return 0
  retire_delivery=$(alert_file "$retire_key" delivered)
  retry_pending_alert_locked "$retire_key"
  if ! load_alert_delivery_state "$retire_delivery"; then
    # Never hide an opening that Telegram has not accepted yet. The active
    # incident and journal floor remain so a later pass can retry delivery.
    return 0
  fi
  retire_closed=$(alert_file "$retire_key" closed)
  retire_now=$(date +%s)
  if ! write_alert_delivery_time "$retire_key" "$retire_closed" "$retire_now"; then
    return 0
  fi
  if ! rm -f "$retire_active" \
    "$retire_delivery" \
    "$(alert_file "$retire_key" silent)" \
    "$(alert_file "$retire_key" last)" \
    "$(alert_file "$retire_key" level)" \
    "$(alert_file "$retire_key" handoff)" \
    "$(alert_file "$retire_key" journal_size)"
  then
    echo "$(timestamp) telegram_alert silent_close_failed key=$retire_key" >&2
  fi
  return 0
}

retire_alert() {
  lock_retire_key=$1
  mkdir -p "$ALERT_STATE_DIR" 2>/dev/null || return 0
  if ! acquire_alert_lock "$lock_retire_key"; then
    echo "$(timestamp) telegram_alert state_lock_unavailable key=$lock_retire_key" >&2
    return 0
  fi
  retire_alert_locked "$lock_retire_key"
  lock_retire_status=$?
  release_alert_lock
  return "$lock_retire_status"
}

journal_size() {
  if [ -s "$JOURNAL_FILE" ]; then
    stat -c %s "$JOURNAL_FILE"
  else
    printf '%s\n' 0
  fi
}

remember_alert_journal_floor() {
  floor_key=$1
  floor_active=$(alert_file "$floor_key" active)
  floor_file=$(alert_file "$floor_key" journal_size)
  if [ ! -e "$floor_active" ] || [ ! -r "$floor_file" ]; then
    floor_size=$(journal_size 2>/dev/null || printf '%s\n' 0)
    floor_tmp=$floor_file.$$
    if ! printf '%s\n' "$floor_size" > "$floor_tmp"; then
      echo "$(timestamp) telegram_alert journal_floor_write_failed key=$floor_key" >&2
      rm -f "$floor_tmp" 2>/dev/null || true
      return 0
    fi
    if ! mv -f "$floor_tmp" "$floor_file"; then
      echo "$(timestamp) telegram_alert journal_floor_publish_failed key=$floor_key" >&2
      rm -f "$floor_tmp" 2>/dev/null || true
    fi
  fi
  return 0
}

reset_alert_journal_floor() {
  reset_key=$1
  reset_active=$(alert_file "$reset_key" active)
  [ -e "$reset_active" ] || return 0
  reset_floor_file=$(alert_file "$reset_key" journal_size)
  reset_floor_size=$(journal_size 2>/dev/null || printf '%s\n' 0)
  reset_floor_tmp=$reset_floor_file.$$
  if ! printf '%s\n' "$reset_floor_size" > "$reset_floor_tmp" \
    || ! mv -f "$reset_floor_tmp" "$reset_floor_file"
  then
    echo "$(timestamp) telegram_alert journal_floor_reset_failed key=$reset_key" >&2
    rm -f "$reset_floor_tmp" 2>/dev/null || true
  fi
  return 0
}

reset_rotated_journal_incident_floors() {
  reset_alert_journal_floor grpc_stale
  reset_alert_journal_floor recorder_restarting
  reset_alert_journal_floor provider_replay_gap
  reset_alert_journal_floor resume_coverage
}

clear_alert_after_journal_growth() {
  growth_key=$1
  growth_message=$2
  growth_recovery_mode=${3:-notify}
  growth_active=$(alert_file "$growth_key" active)
  growth_floor_file=$(alert_file "$growth_key" journal_size)
  if [ ! -e "$growth_active" ]; then
    return 0
  fi
  if [ -L "$growth_floor_file" ] || [ ! -r "$growth_floor_file" ]; then
    # Releases before the journal-growth recovery model could leave a
    # delivered incident without a floor. Bootstrap at the current size and
    # require one later append before announcing that recording resumed.
    reset_alert_journal_floor "$growth_key"
    return 0
  fi
  if ! IFS= read -r growth_floor < "$growth_floor_file"; then
    reset_alert_journal_floor "$growth_key"
    return 0
  fi
  growth_size=$(journal_size 2>/dev/null || printf '%s\n' 0)
  case "$growth_floor" in
    ''|*[!0-9]*) reset_alert_journal_floor "$growth_key"; return 0 ;;
  esac
  case "$growth_size" in
    ''|*[!0-9]*) return 0 ;;
  esac
  if [ "$growth_size" -gt "$growth_floor" ]; then
    if [ "$growth_recovery_mode" = silent ]; then
      retire_alert "$growth_key"
    else
      clear_alert "$growth_key" "$growth_message"
    fi
    if [ ! -e "$growth_active" ]; then
      rm -f "$growth_floor_file" 2>/dev/null || \
        echo "$(timestamp) telegram_recovery journal_floor_remove_failed key=$growth_key" >&2
    fi
  fi
}

clear_replay_recovery_alert_if_floor_was_authoritative() {
  replay_floor_was_authoritative=$1
  if [ "$replay_floor_was_authoritative" = true ]; then
    clear_alert replay_recovery_failed \
      "Status: gRPC recovery is ready and backup will restart automatically.
Data: Everything saved earlier is safe.
Action: None."
  fi
}

raise_recorder_restart_alert() {
  restart_message=$1
  remember_alert_journal_floor recorder_restarting
  raise_alert recorder_restarting ERROR \
    "Status: The recorder stopped and is restarting automatically.
Data: Everything already saved is safe.
Action: Check the recorder logs only if this keeps happening."
}

file_age_seconds() {
  age_path=$1
  age_now=$(date +%s)
  age_modified=$(stat -c %Y "$age_path") || return 1
  if [ "$age_modified" -gt "$age_now" ]; then
    printf '%s\n' 0
  else
    printf '%s\n' $((age_now - age_modified))
  fi
}

monitor_disk_alerts() {
  if ! monitor_free_bytes=$(available_bytes); then
    raise_alert disk_check_failed ERROR \
      "Status: Hetzner free space is unavailable. Backup safety cannot be confirmed.
Storage: Unknown.
Action: Check the Hetzner disk."
    return 0
  fi
  clear_alert disk_check_failed \
    "Status: Hetzner disk checks are working again.
Storage: $(human_decimal_bytes "$monitor_free_bytes") free.
Action: None."
  update_disk_alerts "$monitor_free_bytes"
}

update_disk_alerts() {
  disk_free_bytes=$1
  if [ "$CACHE_MODE" = b2-generations ]; then
    pipeline_active=$(alert_file generation_backlog active)
    pipeline_backlog=
    pipeline_backlog=$(sealed_generation_count 2>/dev/null) || pipeline_backlog=
    if [ -n "$pipeline_backlog" ]; then
      pipeline_disk_context="$pipeline_backlog backup batches are waiting locally."
    else
      pipeline_disk_context="The waiting backup count is unavailable."
    fi
    if [ "$disk_free_bytes" -lt "$MIN_FREE_BYTES" ]; then
      pipeline_worker_running=false
      if [ -n "${upload_worker_pid:-}" ] \
        && kill -0 "$upload_worker_pid" 2>/dev/null
      then
        pipeline_worker_running=true
      fi
      pipeline_handoff=$(alert_file generation_backlog handoff)
      if [ "$pipeline_worker_running" = true ] \
        && [ -e "$pipeline_active" ]
      then
        # A live worker already owns a typed or gate-specific incident and will
        # escalate it after observing this floor. Preserve that exact cause.
        rm -f "$pipeline_handoff" 2>/dev/null || true
        return 0
      fi
      if [ "$pipeline_worker_running" = true ] \
        && [ ! -e "$pipeline_handoff" ]
      then
        # On the first floor observation, give the live worker one monitor pass
        # to report an exact provider status. If it hangs, the durable handoff
        # marker makes the next pass fall through to the generic fail-safe.
        : > "$pipeline_handoff" 2>/dev/null || true
        return 0
      fi
      # This is the fail-safe path when the independent spill worker is dead,
      # stuck past one handoff pass, or has not opened its own incident.
      raise_alert generation_backlog CRITICAL \
        "Status: Hetzner disk is full. Backup is paused; saved data is safe.
Storage: $(human_decimal_bytes "$disk_free_bytes") free; backup needs $(human_decimal_bytes "$MIN_FREE_BYTES"). $pipeline_disk_context
Action: Restore Backblaze uploads or add Hetzner disk space. Automatic retry is on."
      return 0
    fi
    rm -f "$(alert_file generation_backlog handoff)" 2>/dev/null || true
    # Sealed generations are intentional local retention above the floor. The
    # spill worker owns remote-cap/API failures and closes that same incident at
    # its high-water mark; the disk monitor must not turn a healthy backlog into
    # periodic warning/recovery noise.
    if [ -e "$pipeline_active" ]; then
      return 0
    fi
    return 0
  fi

  disk_space_active=$(alert_file disk_space active)
  if [ "$disk_free_bytes" -lt "$MIN_FREE_BYTES" ]; then
    raise_alert disk_space CRITICAL \
      "Status: Backup is paused; saved data is safe.
Storage: $(human_decimal_bytes "$disk_free_bytes") free; backup needs $(human_decimal_bytes "$MIN_FREE_BYTES").
Action: Free Hetzner disk space or restore Backblaze uploads. Automatic retry is on."
  elif [ "$disk_free_bytes" -lt "$DISK_WARN_FREE_BYTES" ]; then
    raise_alert disk_space WARNING \
      "Status: The Hetzner disk is getting low, but it still has room for recording.
Storage: $(human_decimal_bytes "$disk_free_bytes") free; warning level is $(human_decimal_bytes "$DISK_WARN_FREE_BYTES").
Action: Restore Backblaze uploads or add disk space."
  elif [ -e "$disk_space_active" ] \
    && [ "$disk_free_bytes" -lt "$DISK_WARNING_RECOVERY_BYTES" ]
  then
    : # Keep one incident active until the full warning hysteresis is reached.
  else
    clear_alert disk_space \
      "Status: Backup disk space is healthy again.
Storage: $(human_decimal_bytes "$disk_free_bytes") free.
Action: None."
  fi
}

monitor_feed_alerts() {
  # Consume a reconnect warning before closing a provider-history incident.
  # The first post-gap append deliberately produces both signals; keeping the
  # provider incident active here lets the warning coalesce into it.
  monitor_resume_coverage_alert
  if [ -s "$JOURNAL_FILE" ]; then
    monitor_age=$(file_age_seconds "$JOURNAL_FILE") || return 0
    if [ "$monitor_age" -gt "$RAW_STALE_AFTER_SECS" ]; then
      remember_alert_journal_floor grpc_stale
      raise_alert grpc_stale ERROR \
        "Status: No new gRPC block was saved for $(human_duration "$monitor_age").
Data: Everything saved earlier is safe.
Action: Check the gRPC provider or connection."
    else
      clear_alert_after_journal_growth grpc_stale \
        "Status: New gRPC blocks are being saved again.
Data: New blocks are being saved; everything saved earlier is safe.
Action: None."
    fi
    clear_alert_after_journal_growth recorder_restarting \
      "Status: The recorder restarted and new blocks are being saved.
Data: Backup is running again.
Action: None."
    clear_alert_after_journal_growth provider_replay_gap \
      "Status: New gRPC blocks are being saved again.
Data: The previously reported missing slots are still missing.
Action: Repair that slot range from another source if needed."
    clear_alert_after_journal_growth resume_coverage \
      "Status: New gRPC blocks are being saved again.
Data: The earlier reconnect range is still unverified.
Action: Compare that range with another source if needed." \
      silent
  elif [ -e "$STARTED_FILE" ]; then
    monitor_start_age=$(file_age_seconds "$STARTED_FILE") || return 0
    if [ "$monitor_start_age" -gt "$STARTUP_GRACE_SECS" ]; then
      remember_alert_journal_floor grpc_stale
      raise_alert grpc_stale ERROR \
        "Status: No gRPC block was saved after startup.
Data: No new backup data is arriving.
Action: Check the gRPC provider or connection."
    fi
  fi

  monitor_primary_sync_alert
}

monitor_resume_coverage_alert() {
  if [ "$TELEGRAM_ENABLED" != true ] \
    || [ -z "${ACTIVE_RESUME_COVERAGE_EVENT_FILE:-}" ]
  then
    return 0
  fi
  if ! load_resume_coverage_event; then
    return 0
  fi
  delivered_event_id=
  if [ -e "$RESUME_COVERAGE_DELIVERED_FILE" ] \
    || [ -L "$RESUME_COVERAGE_DELIVERED_FILE" ]
  then
    if ! load_resume_coverage_delivery; then
      return 0
    fi
  fi
  if [ "${delivered_event_id:-}" = "$resume_event_id" ]; then
    remove_resume_coverage_event
    return 0
  fi
  provider_gap_active=$(alert_file provider_replay_gap active)
  if [ ! -L "$provider_gap_active" ] && [ -f "$provider_gap_active" ]; then
    # A provider-history recovery deliberately cannot replay its old anchor.
    # That expected overlap warning is already covered by the stronger active
    # missing-slot incident, so consuming it avoids a second alert lifecycle.
    ALERT_DELIVERY_RESULT=suppressed
    remove_resume_coverage_event
    return 0
  fi
  if [ -n "$resume_requested_slot" ] && [ -n "$resume_observed_slot" ]; then
    resume_gap_detail="Status: The provider did not replay saved slot $resume_requested_slot after reconnect.
Data: It later sent slot $resume_observed_slot, so coverage between them could not be verified.
Action: Compare that range with another source and repair any gaps."
  else
    resume_gap_detail="Status: The provider did not replay the last saved slot after reconnect.
Data: Reconnect coverage could not be verified; earlier saved blocks are safe.
Action: Compare that range with another source and repair any gaps."
  fi
  remember_alert_journal_floor resume_coverage
  raise_alert resume_coverage WARNING \
    "$resume_gap_detail"
  # Suppression means this alert key already belongs to a delivered active
  # incident. Coalesce the durable event without claiming that this exact event
  # was sent; failed sends stay pending.
  case "${ALERT_DELIVERY_RESULT:-}" in
    sent)
      if write_resume_coverage_delivery "$resume_event_id"; then
        remove_resume_coverage_event
      fi
      ;;
    suppressed) remove_resume_coverage_event ;;
  esac
}

load_resume_coverage_event() {
  resume_event_id=
  resume_requested_slot=
  resume_first_slot=
  resume_observed_slot=
  if [ -L "$ACTIVE_RESUME_COVERAGE_EVENT_FILE" ] \
    || [ ! -f "$ACTIVE_RESUME_COVERAGE_EVENT_FILE" ] \
    || [ ! -r "$ACTIVE_RESUME_COVERAGE_EVENT_FILE" ] \
    || [ ! -s "$ACTIVE_RESUME_COVERAGE_EVENT_FILE" ]
  then
    return 1
  fi
  resume_event_bytes=$(wc -c < "$ACTIVE_RESUME_COVERAGE_EVENT_FILE" | tr -d ' ')
  case "$resume_event_bytes" in
    ''|*[!0-9]*|0) return 1 ;;
  esac
  if [ "$resume_event_bytes" -gt 4096 ]; then
    return 1
  fi
  if ! resume_event_fields=$("$GENERATION_PYTHON_BIN" - \
    "$ACTIVE_RESUME_COVERAGE_EVENT_FILE" 2>/dev/null <<'PY'
import hashlib
import json
import os
import stat
import struct
import sys

path = sys.argv[1]
if not hasattr(os, "O_NOFOLLOW"):
    raise SystemExit(1)
flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
fd = os.open(path, flags)
try:
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= 4096:
        raise ValueError("invalid event file")
    payload = bytearray()
    while len(payload) <= before.st_size:
        chunk = os.read(fd, before.st_size + 1 - len(payload))
        if not chunk:
            break
        payload.extend(chunk)
    after = os.fstat(fd)
finally:
    os.close(fd)
if (
    len(payload) != before.st_size
    or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
):
    raise ValueError("event changed while reading")

def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value

def invalid_constant(value):
    raise ValueError(f"invalid JSON constant: {value}")

event = json.loads(
    payload.decode("utf-8"),
    object_pairs_hook=unique_object,
    parse_constant=invalid_constant,
)
if (
    not isinstance(event, dict)
    or type(event.get("schema_version")) is not int
    or event["schema_version"] != 1
):
    raise ValueError("invalid event schema")
expected_fields = {
    "event_id",
    "schema_version",
    "requested_overlap_slot",
    "first_delivered_slot",
    "observed_later_slot",
    "written_unix_secs",
}
if set(event) != expected_fields:
    raise ValueError("unexpected event fields")
numeric_fields = (
    "requested_overlap_slot",
    "first_delivered_slot",
    "observed_later_slot",
    "written_unix_secs",
)
for field in numeric_fields:
    value = event.get(field)
    if type(value) is not int or not 0 <= value <= (1 << 64) - 1:
        raise ValueError("invalid numeric field")
requested = event["requested_overlap_slot"]
first = event["first_delivered_slot"]
observed = event["observed_later_slot"]
if requested >= observed:
    raise ValueError("event does not advance")
event_id = event.get("event_id")
if (
    not isinstance(event_id, str)
    or len(event_id) != 64
    or any(character not in "0123456789abcdef" for character in event_id)
):
    raise ValueError("invalid event ID")
expected = hashlib.sha256(
    b"blockzilla-grpc-resume-coverage-warning-v1"
    + struct.pack("<QQQ", requested, first, observed)
).hexdigest()
if event_id != expected:
    raise ValueError("event ID mismatch")
print(f"{event_id}:{requested}:{first}:{observed}")
PY
  ); then
    return 1
  fi
  resume_event_id=${resume_event_fields%%:*}
  resume_event_remainder=${resume_event_fields#*:}
  resume_requested_slot=${resume_event_remainder%%:*}
  resume_event_remainder=${resume_event_remainder#*:}
  resume_first_slot=${resume_event_remainder%%:*}
  resume_observed_slot=${resume_event_remainder#*:}
  case "$resume_event_id:$resume_requested_slot:$resume_first_slot:$resume_observed_slot" in
    *[!0-9a-f:]*) return 1 ;;
  esac
  [ "${#resume_event_id}" -eq 64 ] \
    && [ -n "$resume_requested_slot" ] \
    && [ -n "$resume_first_slot" ] \
    && [ -n "$resume_observed_slot" ]
}

resume_coverage_event_valid() {
  load_resume_coverage_event >/dev/null 2>&1
}

load_resume_coverage_delivery() {
  delivered_event_id=
  if [ -L "$RESUME_COVERAGE_DELIVERED_FILE" ] \
    || [ ! -f "$RESUME_COVERAGE_DELIVERED_FILE" ] \
    || [ ! -r "$RESUME_COVERAGE_DELIVERED_FILE" ]
  then
    return 1
  fi
  delivered_event_bytes=$(wc -c < "$RESUME_COVERAGE_DELIVERED_FILE" | tr -d ' ')
  if [ "$delivered_event_bytes" != 65 ]; then
    return 1
  fi
  IFS= read -r delivered_event_id < "$RESUME_COVERAGE_DELIVERED_FILE" \
    || delivered_event_id=
  if [ "${#delivered_event_id}" -ne 64 ]; then
    delivered_event_id=
    return 1
  fi
  case "$delivered_event_id" in
    *[!0-9a-f]*) delivered_event_id=; return 1 ;;
  esac
  return 0
}

write_resume_coverage_delivery() {
  delivered_id=$1
  delivered_tmp=$RESUME_COVERAGE_DELIVERED_FILE.$$
  if ! printf '%s\n' "$delivered_id" > "$delivered_tmp"; then
    echo "$(timestamp) resume_coverage_event delivery_marker_write_failed" >&2
    rm -f "$delivered_tmp" 2>/dev/null || true
    return 1
  fi
  if ! mv -f "$delivered_tmp" "$RESUME_COVERAGE_DELIVERED_FILE"; then
    echo "$(timestamp) resume_coverage_event delivery_marker_publish_failed" >&2
    rm -f "$delivered_tmp" 2>/dev/null || true
    return 1
  fi
  if ! sync -f "$RESUME_COVERAGE_EVENT_DIR" 2>/dev/null; then
    echo "$(timestamp) resume_coverage_event delivery_marker_sync_failed" >&2
    return 1
  fi
  return 0
}

remove_resume_coverage_event() {
  if ! rm -f "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"; then
    echo "$(timestamp) resume_coverage_event remove_failed" >&2
    return 0
  fi
  sync -f "$RESUME_COVERAGE_EVENT_DIR" 2>/dev/null || \
    echo "$(timestamp) resume_coverage_event directory_sync_failed" >&2
  return 0
}

monitor_primary_sync_alert() {
  if [ "$TELEGRAM_ENABLED" != true ] || [ -z "$PRIMARY_SYNC_HEARTBEAT_FILE" ]; then
    return 0
  fi
  heartbeat_seen_file=$(alert_file primary_sync_stale seen)
  if [ -L "$PRIMARY_SYNC_HEARTBEAT_FILE" ] \
    || [ ! -f "$PRIMARY_SYNC_HEARTBEAT_FILE" ] \
    || [ ! -r "$PRIMARY_SYNC_HEARTBEAT_FILE" ]
  then
    heartbeat_should_alert=false
    if [ -e "$heartbeat_seen_file" ]; then
      heartbeat_should_alert=true
    elif heartbeat_start_age=$(file_age_seconds "$STARTED_FILE" 2>/dev/null) \
      && [ "$heartbeat_start_age" -gt "$PRIMARY_SYNC_STALE_AFTER_SECS" ]
    then
      heartbeat_should_alert=true
    fi
    if [ "$heartbeat_should_alert" = true ]; then
      raise_alert primary_sync_stale WARNING \
        "Status: Blockzilla confirmation is missing.
Data: Nothing is deleted because of this alert.
Action: Check the Blockzilla connection."
    fi
    return 0
  fi

  if ! : > "$heartbeat_seen_file"; then
    echo "$(timestamp) telegram_alert heartbeat_state_write_failed" >&2
  fi
  heartbeat_age=$(file_age_seconds "$PRIMARY_SYNC_HEARTBEAT_FILE") || heartbeat_age=$PRIMARY_SYNC_STALE_AFTER_SECS
  if [ "$heartbeat_age" -gt "$PRIMARY_SYNC_STALE_AFTER_SECS" ]; then
    raise_alert primary_sync_stale WARNING \
      "Status: No Blockzilla confirmation for $(human_duration "$heartbeat_age").
Data: Nothing is deleted because of this alert.
Action: Check the Blockzilla connection."
  else
    clear_alert primary_sync_stale \
      "Status: Blockzilla confirmations are arriving again.
Data: Hetzner backup remains safe.
Action: None."
  fi
}

monitor_child() {
  monitored_pid=$1
  monitor_sleep_pid=
  monitor_journal_identity=
  if [ -e "$JOURNAL_FILE" ]; then
    monitor_journal_identity=$(stat -c '%d:%i' "$JOURNAL_FILE" 2>/dev/null || true)
  fi
  trap '
    if [ -n "$monitor_sleep_pid" ]; then
      kill -TERM "$monitor_sleep_pid" 2>/dev/null || true
      wait "$monitor_sleep_pid" 2>/dev/null || true
    fi
    exit 0
  ' INT TERM HUP
  while kill -0 "$monitored_pid" 2>/dev/null; do
    # Waiting on an explicit child makes TERM interruptible in dash. A foreground
    # sleep defers the trap until its full interval, which can turn an immediate
    # provider replay response into a 30-second reconnect delay.
    sleep "$MONITOR_INTERVAL_SECS" &
    monitor_sleep_pid=$!
    if ! wait "$monitor_sleep_pid" 2>/dev/null; then
      exit 0
    fi
    monitor_sleep_pid=
    if ! kill -0 "$monitored_pid" 2>/dev/null; then
      break
    fi
    if validate_data_volume true >/dev/null 2>&1; then
      current_journal_identity=
      if [ -e "$JOURNAL_FILE" ]; then
        current_journal_identity=$(stat -c '%d:%i' "$JOURNAL_FILE" 2>/dev/null || true)
      fi
      if [ -n "$current_journal_identity" ] \
        && [ -n "$monitor_journal_identity" ] \
        && [ "$current_journal_identity" != "$monitor_journal_identity" ]
      then
        # In-process rollover replaces active/raw-blocks.jsonl while the child
        # and Yellowstone stream stay alive. Incident progress floors belong to
        # the old inode and must restart at the seeded successor's size.
        reset_rotated_journal_incident_floors
      fi
      if [ -n "$current_journal_identity" ]; then
        monitor_journal_identity=$current_journal_identity
      fi
      retry_pending_alerts
      clear_alert volume_invalid \
        "Status: The Hetzner backup disk is available again.
Data: Backup can continue.
Action: None."
      monitor_disk_alerts
      monitor_feed_alerts
    else
      kill -TERM "$monitored_pid" 2>/dev/null || true
      raise_alert volume_invalid CRITICAL \
        "Status: The Hetzner backup disk is unavailable. Backup is paused.
Data: Nothing was deleted.
Action: Check the Hetzner volume mount."
      break
    fi
  done
}

start_child_monitor() {
  monitor_child "$1" &
  monitor_pid=$!
}

sync_path() {
  sync -f "$1" 2>/dev/null
}

cache_real_directory() {
  cache_path=$1
  if [ -L "$cache_path" ] || [ ! -d "$cache_path" ]; then
    echo "cache path is not a real directory: $cache_path" >&2
    return 1
  fi
  cache_real=$(readlink -f "$cache_path") || return 1
  cache_root_real=$(readlink -f "$CACHE_ROOT") || return 1
  case "$cache_real" in
    "$cache_root_real"|"$cache_root_real"/*) ;;
    *)
      echo "cache path resolves outside cache root: $cache_path" >&2
      return 1
      ;;
  esac
  cache_device=$(stat -c %d "$cache_path") || return 1
  data_device=$(stat -c %d /data) || return 1
  if [ "$cache_device" != "$data_device" ]; then
    echo "cache path is not on the recorder filesystem: $cache_path" >&2
    return 1
  fi
}

ensure_cache_child_directory() {
  cache_child=$1
  if [ -e "$cache_child" ]; then
    cache_real_directory "$cache_child"
    return $?
  fi
  mkdir "$cache_child" || return 1
  sync_path "$CACHE_ROOT" || return 1
  cache_real_directory "$cache_child"
}

safe_replay_identity() {
  [ -n "$CLUSTER_ID" ] && [ -n "$ORIGIN_NODE_ID" ] && [ -n "$SOURCE_ID" ] \
    || return 1
  case "$CLUSTER_ID:$ORIGIN_NODE_ID:$SOURCE_ID" in
    *[!A-Za-z0-9._:-]*) return 1 ;;
  esac
}

valid_replay_shell_uint() {
  replay_uint_value=$1
  case "$replay_uint_value" in
    ''|*[!0-9]*) return 1 ;;
    0|[1-9]|[1-9][0-9]*) ;;
    *) return 1 ;;
  esac
  # POSIX test arithmetic is signed. This bound remains many orders of
  # magnitude above any plausible Solana slot while avoiding overflow.
  [ "${#replay_uint_value}" -le 18 ]
}

load_replay_recovery_floor() {
  REPLAY_RECOVERY_ANCHOR_SLOT=
  REPLAY_RECOVERY_REQUESTED_SLOT=
  REPLAY_RECOVERY_SCHEMA_VERSION=
  REPLAY_PROVIDER_AVAILABLE_SLOT=
  REPLAY_MIN_RESUME_SLOT=
  if [ ! -e "$REPLAY_RECOVERY_FILE" ] && [ ! -L "$REPLAY_RECOVERY_FILE" ]; then
    return 0
  fi
  if ! safe_replay_identity \
    || [ -L "$REPLAY_RECOVERY_FILE" ] \
    || [ ! -f "$REPLAY_RECOVERY_FILE" ] \
    || [ ! -r "$REPLAY_RECOVERY_FILE" ]
  then
    return 1
  fi
  replay_recovery_bytes=$(wc -c < "$REPLAY_RECOVERY_FILE" | tr -d ' ')
  replay_recovery_lines=$(wc -l < "$REPLAY_RECOVERY_FILE" | tr -d ' ')
  valid_replay_shell_uint "$replay_recovery_bytes" || return 1
  valid_replay_shell_uint "$replay_recovery_lines" || return 1
  [ "$replay_recovery_bytes" -le 1024 ] \
    && [ "$replay_recovery_lines" -eq 1 ] || return 1
  replay_recovery_actual=$(sed -n '1p' "$REPLAY_RECOVERY_FILE")
  replay_recovery_anchor=$(printf '%s\n' "$replay_recovery_actual" | sed -n \
    's/^{"anchor_slot":\([0-9][0-9]*\),"cluster_id":.*$/\1/p')
  replay_recovery_requested=$(printf '%s\n' "$replay_recovery_actual" | sed -n \
    's/^.*"requested_slot":\([0-9][0-9]*\),"schema_version":.*$/\1/p')
  replay_recovery_schema=$(printf '%s\n' "$replay_recovery_actual" | sed -n \
    's/^.*"schema_version":\([0-9][0-9]*\),.*"source_id":.*$/\1/p')
  valid_replay_shell_uint "$replay_recovery_anchor" || return 1
  valid_replay_shell_uint "$replay_recovery_requested" || return 1
  valid_replay_shell_uint "$replay_recovery_schema" || return 1
  case "$replay_recovery_schema" in
    1)
      replay_recovery_floor=$(printf '%s\n' "$replay_recovery_actual" | sed -n \
        's/^.*"minimum_resume_slot":\([0-9][0-9]*\),"origin_node_id":.*$/\1/p')
      valid_replay_shell_uint "$replay_recovery_floor" || return 1
      replay_recovery_expected=$(printf \
        '{"anchor_slot":%s,"cluster_id":"%s","minimum_resume_slot":%s,"origin_node_id":"%s","requested_slot":%s,"schema_version":1,"source_id":"%s"}' \
        "$replay_recovery_anchor" "$CLUSTER_ID" "$replay_recovery_floor" \
        "$ORIGIN_NODE_ID" "$replay_recovery_requested" "$SOURCE_ID")
      [ "$replay_recovery_floor" -gt "$replay_recovery_requested" ] || return 1
      ;;
    2)
      replay_recovery_provider=$(printf '%s\n' "$replay_recovery_actual" | sed -n \
        's/^.*"provider_available_slot":\([0-9][0-9]*\),"requested_slot":.*$/\1/p')
      replay_recovery_floor=$(printf '%s\n' "$replay_recovery_actual" | sed -n \
        's/^.*"selected_resume_slot":\([0-9][0-9]*\),"source_id":.*$/\1/p')
      valid_replay_shell_uint "$replay_recovery_provider" || return 1
      valid_replay_shell_uint "$replay_recovery_floor" || return 1
      replay_recovery_expected=$(printf \
        '{"anchor_slot":%s,"cluster_id":"%s","origin_node_id":"%s","provider_available_slot":%s,"requested_slot":%s,"schema_version":2,"selected_resume_slot":%s,"source_id":"%s"}' \
        "$replay_recovery_anchor" "$CLUSTER_ID" "$ORIGIN_NODE_ID" \
        "$replay_recovery_provider" "$replay_recovery_requested" \
        "$replay_recovery_floor" "$SOURCE_ID")
      [ "$replay_recovery_provider" -gt "$replay_recovery_requested" ] \
        && [ "$replay_recovery_floor" -ge "$replay_recovery_provider" ] \
        && [ "$((replay_recovery_floor - replay_recovery_provider))" \
          -le "$MAX_REPLAY_RESUME_HEADROOM_SLOTS" ] || return 1
      REPLAY_PROVIDER_AVAILABLE_SLOT=$replay_recovery_provider
      ;;
    *) return 1 ;;
  esac
  [ "$replay_recovery_actual" = "$replay_recovery_expected" ] || return 1
  [ "$replay_recovery_requested" -ge "$replay_recovery_anchor" ] || return 1
  validate_replay_gap_record "$replay_recovery_anchor" \
    "$replay_recovery_requested" "$replay_recovery_floor" \
    "$REPLAY_PROVIDER_AVAILABLE_SLOT" || return 1
  REPLAY_RECOVERY_ANCHOR_SLOT=$replay_recovery_anchor
  REPLAY_RECOVERY_REQUESTED_SLOT=$replay_recovery_requested
  REPLAY_RECOVERY_SCHEMA_VERSION=$replay_recovery_schema
  REPLAY_MIN_RESUME_SLOT=$replay_recovery_floor
}

strict_replay_report_fields() {
  replay_report_path=$1
  "$GENERATION_PYTHON_BIN" - "$replay_report_path" <<'PY'
import json
import os
import stat
import sys


def reject_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


try:
    path = sys.argv[1]
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size < 1 or metadata.st_size > 65536:
            raise ValueError("invalid report file")
        payload = os.read(descriptor, 65537)
        if len(payload) != metadata.st_size:
            raise ValueError("report changed while reading")
    finally:
        os.close(descriptor)
    report = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    if not isinstance(report, dict) or report.get("replay_unavailable") is not True:
        raise ValueError("not a replay-unavailable report")
    names = (
        "resume_overlap_slot",
        "replay_unavailable_requested_slot",
        "replay_available_slot",
        "effective_from_slot",
        "frames_seen",
        "frames_written",
    )
    values = []
    for name in names:
        value = report.get(name)
        if type(value) is not int or value < 0 or value >= 10**18:
            raise ValueError(f"invalid {name}")
        values.append(value)
    print(" ".join(str(value) for value in values))
except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
PY
}

replay_gap_payload() {
  replay_payload_anchor=$1
  replay_payload_requested=$2
  replay_payload_resume=$3
  replay_payload_provider=${4:-}
  if [ -z "$replay_payload_provider" ]; then
    printf \
      '{"anchor_slot":%s,"available_slot":%s,"cluster_id":"%s","origin_node_id":"%s","requested_slot":%s,"schema_version":1,"source_id":"%s"}' \
      "$replay_payload_anchor" "$replay_payload_resume" "$CLUSTER_ID" \
      "$ORIGIN_NODE_ID" "$replay_payload_requested" "$SOURCE_ID"
  else
    printf \
      '{"anchor_slot":%s,"cluster_id":"%s","origin_node_id":"%s","provider_available_slot":%s,"requested_slot":%s,"schema_version":2,"selected_resume_slot":%s,"source_id":"%s"}' \
      "$replay_payload_anchor" "$CLUSTER_ID" "$ORIGIN_NODE_ID" \
      "$replay_payload_provider" "$replay_payload_requested" \
      "$replay_payload_resume" "$SOURCE_ID"
  fi
}

validate_replay_gap_record_in_generation() {
  replay_validate_generation=$1
  replay_validate_anchor=$2
  replay_validate_requested=$3
  replay_validate_resume=$4
  replay_validate_provider=${5:-}
  valid_replay_shell_uint "$replay_validate_anchor" || return 1
  valid_replay_shell_uint "$replay_validate_requested" || return 1
  valid_replay_shell_uint "$replay_validate_resume" || return 1
  [ "$replay_validate_requested" -ge "$replay_validate_anchor" ] || return 1
  if [ -n "$replay_validate_provider" ]; then
    valid_replay_shell_uint "$replay_validate_provider" || return 1
    [ "$replay_validate_provider" -gt "$replay_validate_requested" ] \
      && [ "$replay_validate_resume" -ge "$replay_validate_provider" ] \
      && [ "$((replay_validate_resume - replay_validate_provider))" \
        -le "$MAX_REPLAY_RESUME_HEADROOM_SLOTS" ] || return 1
  else
    [ "$replay_validate_resume" -gt "$replay_validate_requested" ] || return 1
  fi
  cache_real_directory "$replay_validate_generation" || return 1
  replay_validate_gap_dir=$replay_validate_generation/replay-gaps
  cache_real_directory "$replay_validate_gap_dir" || return 1
  if [ -n "$replay_validate_provider" ]; then
    replay_validate_path=$replay_validate_gap_dir/replay-gap-$replay_validate_anchor-$replay_validate_requested-$replay_validate_provider-$replay_validate_resume.json
  else
    replay_validate_path=$replay_validate_gap_dir/replay-gap-$replay_validate_anchor-$replay_validate_requested-$replay_validate_resume.json
  fi
  if [ -L "$replay_validate_path" ] \
    || [ ! -f "$replay_validate_path" ] \
    || [ ! -r "$replay_validate_path" ]
  then
    return 1
  fi
  replay_validate_bytes=$(wc -c < "$replay_validate_path" | tr -d ' ') || return 1
  valid_replay_shell_uint "$replay_validate_bytes" || return 1
  [ "$replay_validate_bytes" -le 1024 ] || return 1
  replay_validate_payload=$(replay_gap_payload "$replay_validate_anchor" \
    "$replay_validate_requested" "$replay_validate_resume" \
    "$replay_validate_provider") || return 1
  [ "$(wc -l < "$replay_validate_path" | tr -d ' ')" -eq 1 ] \
    && [ "$(sed -n '1p' "$replay_validate_path")" = "$replay_validate_payload" ]
}

validate_replay_gap_record() {
  validate_replay_gap_record_in_generation "$ACTIVE_GENERATION_DIR" "$@"
}

publish_replay_gap_record_to_generation() {
  replay_gap_generation=$1
  replay_gap_anchor=$2
  replay_gap_requested=$3
  replay_gap_resume=$4
  replay_gap_provider=${5:-}
  REPLAY_PERSIST_FAILURE_STAGE=gap_generation_validate
  cache_real_directory "$replay_gap_generation" || return 1
  replay_target_gap_dir=$replay_gap_generation/replay-gaps
  if [ -e "$replay_target_gap_dir" ]; then
    REPLAY_PERSIST_FAILURE_STAGE=gap_directory_validate
    cache_real_directory "$replay_target_gap_dir" || return 1
  else
    REPLAY_PERSIST_FAILURE_STAGE=gap_directory_create
    mkdir "$replay_target_gap_dir" || return 1
    REPLAY_PERSIST_FAILURE_STAGE=gap_generation_sync
    sync_path "$replay_gap_generation" || return 1
    REPLAY_PERSIST_FAILURE_STAGE=gap_directory_validate
    cache_real_directory "$replay_target_gap_dir" || return 1
  fi
  if [ -n "$replay_gap_provider" ]; then
    replay_gap_path=$replay_target_gap_dir/replay-gap-$replay_gap_anchor-$replay_gap_requested-$replay_gap_provider-$replay_gap_resume.json
  else
    replay_gap_path=$replay_target_gap_dir/replay-gap-$replay_gap_anchor-$replay_gap_requested-$replay_gap_resume.json
  fi
  REPLAY_PERSIST_FAILURE_STAGE=gap_payload
  replay_gap_payload_value=$(replay_gap_payload "$replay_gap_anchor" \
    "$replay_gap_requested" "$replay_gap_resume" \
    "$replay_gap_provider") || return 1
  if [ -e "$replay_gap_path" ] || [ -L "$replay_gap_path" ]; then
    REPLAY_PERSIST_FAILURE_STAGE=gap_existing_validate
    validate_replay_gap_record_in_generation "$replay_gap_generation" \
      "$replay_gap_anchor" "$replay_gap_requested" \
      "$replay_gap_resume" "$replay_gap_provider" || return 1
    REPLAY_PERSIST_FAILURE_STAGE=gap_directory_sync
    sync_path "$replay_target_gap_dir" || return 1
    return 0
  fi
  # Keep interrupted temporary writes outside the active generation so an
  # unrelated later rotation can never manifest a half-written audit file.
  replay_gap_tmp=$GENERATION_MONITORING_DIR/.replay-gap.$$.tmp
  rm -f "$replay_gap_tmp"
  REPLAY_PERSIST_FAILURE_STAGE=gap_temp_write
  if ! printf '%s\n' "$replay_gap_payload_value" > "$replay_gap_tmp"; then
    rm -f "$replay_gap_tmp" 2>/dev/null || true
    return 1
  fi
  REPLAY_PERSIST_FAILURE_STAGE=gap_temp_sync
  if ! sync_path "$replay_gap_tmp"; then
    rm -f "$replay_gap_tmp" 2>/dev/null || true
    return 1
  fi
  REPLAY_PERSIST_FAILURE_STAGE=gap_publish
  if ! mv "$replay_gap_tmp" "$replay_gap_path"; then
    rm -f "$replay_gap_tmp" 2>/dev/null || true
    return 1
  fi
  REPLAY_PERSIST_FAILURE_STAGE=gap_directory_sync
  sync_path "$replay_target_gap_dir"
}

publish_replay_gap_record() {
  publish_replay_gap_record_to_generation "$ACTIVE_GENERATION_DIR" "$@"
}

publish_replay_recovery_floor() {
  replay_floor_anchor=$1
  replay_floor_requested=$2
  replay_floor_value=$3
  replay_floor_provider=${4:-}
  REPLAY_PERSIST_FAILURE_STAGE=floor_evidence_validate
  validate_replay_gap_record "$replay_floor_anchor" \
    "$replay_floor_requested" "$replay_floor_value" \
    "$replay_floor_provider" || return 1
  REPLAY_PERSIST_FAILURE_STAGE=floor_payload
  if [ -z "$replay_floor_provider" ]; then
    replay_floor_payload=$(printf \
      '{"anchor_slot":%s,"cluster_id":"%s","minimum_resume_slot":%s,"origin_node_id":"%s","requested_slot":%s,"schema_version":1,"source_id":"%s"}' \
      "$replay_floor_anchor" "$CLUSTER_ID" "$replay_floor_value" \
      "$ORIGIN_NODE_ID" "$replay_floor_requested" "$SOURCE_ID")
  else
    replay_floor_payload=$(printf \
      '{"anchor_slot":%s,"cluster_id":"%s","origin_node_id":"%s","provider_available_slot":%s,"requested_slot":%s,"schema_version":2,"selected_resume_slot":%s,"source_id":"%s"}' \
      "$replay_floor_anchor" "$CLUSTER_ID" "$ORIGIN_NODE_ID" \
      "$replay_floor_provider" "$replay_floor_requested" \
      "$replay_floor_value" "$SOURCE_ID")
  fi
  replay_floor_tmp=$REPLAY_RECOVERY_FILE.$$
  rm -f "$replay_floor_tmp"
  REPLAY_PERSIST_FAILURE_STAGE=floor_temp_write
  if ! printf '%s\n' "$replay_floor_payload" > "$replay_floor_tmp"; then
    rm -f "$replay_floor_tmp" 2>/dev/null || true
    return 1
  fi
  REPLAY_PERSIST_FAILURE_STAGE=floor_temp_sync
  if ! sync_path "$replay_floor_tmp"; then
    rm -f "$replay_floor_tmp" 2>/dev/null || true
    return 1
  fi
  REPLAY_PERSIST_FAILURE_STAGE=floor_publish
  if ! mv -f "$replay_floor_tmp" "$REPLAY_RECOVERY_FILE"; then
    rm -f "$replay_floor_tmp" 2>/dev/null || true
    return 1
  fi
  REPLAY_PERSIST_FAILURE_STAGE=floor_directory_sync
  sync_path "$GENERATION_MONITORING_DIR"
}

verified_active_generation_last_slot() {
  replay_verify_report=$CACHE_ROOT/.replay-verify.$$.json
  rm -f "$replay_verify_report"
  if ! verify_generation "$ACTIVE_GENERATION_DIR" "$replay_verify_report"; then
    echo "$(timestamp) raw_recorder replay_active_verify_failed reason=verify_command" >&2
    rm -f "$replay_verify_report"
    return 1
  fi
  replay_verified_slot=$(sed -n \
    's/^[[:space:]]*"last_slot":[[:space:]]*\([0-9][0-9]*\),*$/\1/p' \
    "$replay_verify_report")
  rm -f "$replay_verify_report"
  if ! valid_replay_shell_uint "$replay_verified_slot"; then
    echo "$(timestamp) raw_recorder replay_active_verify_failed reason=last_slot_parse" >&2
    return 1
  fi
  printf '%s\n' "$replay_verified_slot"
}

reconcile_replay_recovery_with_verified_slot() {
  replay_reconcile_last_slot=$1
  valid_replay_shell_uint "$replay_reconcile_last_slot" || return 1
  REPLAY_RECOVERY_NEEDS_CARRY=false
  if [ -z "$REPLAY_MIN_RESUME_SLOT" ]; then
    REPLAY_VERIFIED_ANCHOR_SLOT=
    return 0
  fi
  if [ "$replay_reconcile_last_slot" -lt "$REPLAY_MIN_RESUME_SLOT" ]; then
    [ "$replay_reconcile_last_slot" -eq "$REPLAY_RECOVERY_ANCHOR_SLOT" ] || return 1
    REPLAY_RECOVERY_NEEDS_CARRY=true
    REPLAY_VERIFIED_ANCHOR_SLOT=$replay_reconcile_last_slot
    return 0
  fi
  [ "$replay_reconcile_last_slot" -gt "$REPLAY_RECOVERY_ANCHOR_SLOT" ] || return 1
  if ! rm -f "$REPLAY_RECOVERY_FILE" \
    || ! sync_path "$GENERATION_MONITORING_DIR"
  then
    return 1
  fi
  REPLAY_RECOVERY_ANCHOR_SLOT=
  REPLAY_RECOVERY_REQUESTED_SLOT=
  REPLAY_RECOVERY_SCHEMA_VERSION=
  REPLAY_PROVIDER_AVAILABLE_SLOT=
  REPLAY_MIN_RESUME_SLOT=
  REPLAY_VERIFIED_ANCHOR_SLOT=
  clear_alert provider_replay_gap \
    "Status: New gRPC blocks are being saved again.
Data: The previously reported missing slots are still missing.
Action: Repair that slot range from another source if needed."
  echo "$(timestamp) raw_recorder replay_floor_retired durable_last_slot=$replay_reconcile_last_slot" >&2
}

retire_replay_recovery_floor_if_advanced() {
  if [ -z "$REPLAY_MIN_RESUME_SLOT" ]; then
    REPLAY_VERIFIED_ANCHOR_SLOT=
    return 0
  fi
  replay_retire_last_slot=$(verified_active_generation_last_slot) || return 1
  reconcile_replay_recovery_with_verified_slot "$replay_retire_last_slot"
}

persist_replay_recovery_from_report() {
  replay_report=$1
  REPLAY_PERSIST_FAILURE_STAGE=report_preconditions
  if [ "$CACHE_MODE" != b2-generations ] \
    || [ -L "$replay_report" ] \
    || [ ! -f "$replay_report" ] \
    || [ ! -r "$replay_report" ]
  then
    return 1
  fi
  REPLAY_PERSIST_FAILURE_STAGE=report_size
  replay_report_bytes=$(wc -c < "$replay_report" | tr -d ' ')
  valid_replay_shell_uint "$replay_report_bytes" || return 1
  [ "$replay_report_bytes" -gt 0 ] || return 1
  [ "$replay_report_bytes" -le 65536 ] || return 1
  REPLAY_PERSIST_FAILURE_STAGE=report_json
  replay_fields=$(strict_replay_report_fields "$replay_report") || return 1
  # The strict parser emits exactly six bounded decimal fields.
  REPLAY_PERSIST_FAILURE_STAGE=report_fields
  set -- $replay_fields
  [ "$#" -eq 6 ] || return 1
  replay_anchor=$1
  replay_requested=$2
  replay_available=$3
  replay_effective=$4
  replay_frames_seen=$5
  replay_frames_written=$6
  for replay_field in "$@"; do
    valid_replay_shell_uint "$replay_field" || return 1
  done
  [ "$replay_frames_seen" -eq 0 ] \
    && [ "$replay_frames_written" -eq 0 ] \
    && [ "$replay_effective" -eq "$replay_requested" ] \
    && [ "$replay_available" -gt "$replay_requested" ] \
    && [ "$replay_requested" -ge "$replay_anchor" ] || return 1
  # The provider attests P as its oldest replayable slot. Select S=P+H locally
  # so a new handshake can finish before P moves again, and persist P and S as
  # distinct facts before the recorder is allowed to request S.
  REPLAY_PERSIST_FAILURE_STAGE=resume_headroom
  replay_resume_limit=$((999999999999999999 - REPLAY_RESUME_HEADROOM_SLOTS))
  [ "$replay_available" -le "$replay_resume_limit" ] || return 1
  replay_resume=$((replay_available + REPLAY_RESUME_HEADROOM_SLOTS))
  [ "$replay_resume" -ge "$replay_available" ] || return 1

  REPLAY_PERSIST_FAILURE_STAGE=existing_floor
  load_replay_recovery_floor || return 1
  REPLAY_PERSIST_FAILURE_STAGE=floor_relation
  if [ -n "$REPLAY_MIN_RESUME_SLOT" ]; then
    [ "$REPLAY_RECOVERY_ANCHOR_SLOT" -eq "$replay_anchor" ] \
      && [ "$REPLAY_MIN_RESUME_SLOT" -eq "$replay_requested" ] || return 1
  else
    [ "$replay_anchor" -eq "$replay_requested" ] || return 1
  fi
  if [ "${REPLAY_VERIFIED_ANCHOR_SLOT:-}" != "$replay_anchor" ]; then
    REPLAY_PERSIST_FAILURE_STAGE=active_verify
    replay_verified_last_slot=$(verified_active_generation_last_slot) || return 1
    REPLAY_PERSIST_FAILURE_STAGE=anchor_relation
    [ "$replay_verified_last_slot" -eq "$replay_anchor" ] || return 1
    REPLAY_VERIFIED_ANCHOR_SLOT=$replay_anchor
  fi

  # The immutable generation record is durable before the mutable resume pointer.
  # A crash between them merely causes this exact idempotent record to be reused.
  REPLAY_PERSIST_FAILURE_STAGE=gap_record
  publish_replay_gap_record \
    "$replay_anchor" "$replay_requested" "$replay_resume" \
    "$replay_available" || return 1
  REPLAY_PERSIST_FAILURE_STAGE=floor_pointer
  publish_replay_recovery_floor \
    "$replay_anchor" "$replay_requested" "$replay_resume" \
    "$replay_available" || return 1
  REPLAY_GAP_ANCHOR_SLOT=$replay_anchor
  REPLAY_GAP_REQUESTED_SLOT=$replay_requested
  REPLAY_GAP_PROVIDER_AVAILABLE_SLOT=$replay_available
  REPLAY_GAP_SELECTED_RESUME_SLOT=$replay_resume
  REPLAY_SKIP_RETIRE_VERIFY_ONCE=true
  REPLAY_PERSIST_FAILURE_STAGE=
}

valid_generation_id() {
  generation_id_value=$1
  case "$generation_id_value" in
    slot-[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) return 0 ;;
    *) return 1 ;;
  esac
}

generation_id_from_verify_report() {
  verify_report=$1
  verified_last_slot=$(sed -n \
    's/^[[:space:]]*"last_slot":[[:space:]]*\([0-9][0-9]*\),*$/\1/p' \
    "$verify_report" | tail -n 1)
  case "$verified_last_slot" in
    ''|*[!0-9]*) return 1 ;;
  esac
  padded_slot=$(printf '%020d' "$verified_last_slot") || return 1
  GENERATION_ID=slot-$padded_slot
  valid_generation_id "$GENERATION_ID"
}

write_rotation_marker() {
  marker_generation_id=$1
  valid_generation_id "$marker_generation_id" || return 1
  marker_tmp=$GENERATION_ROTATION_MARKER.$$
  printf '%s\n' "$marker_generation_id" > "$marker_tmp" || return 1
  sync_path "$marker_tmp" || return 1
  mv -f "$marker_tmp" "$GENERATION_ROTATION_MARKER" || return 1
  sync_path "$CACHE_ROOT"
}

read_rotation_marker() {
  if [ -L "$GENERATION_ROTATION_MARKER" ] \
    || [ ! -f "$GENERATION_ROTATION_MARKER" ] \
    || [ ! -r "$GENERATION_ROTATION_MARKER" ]
  then
    return 1
  fi
  marker_bytes=$(wc -c < "$GENERATION_ROTATION_MARKER" | tr -d ' ')
  case "$marker_bytes" in
    ''|*[!0-9]*|0) return 1 ;;
  esac
  [ "$marker_bytes" -le 128 ] || return 1
  IFS= read -r ROTATION_GENERATION_ID < "$GENERATION_ROTATION_MARKER" || return 1
  valid_generation_id "$ROTATION_GENERATION_ID"
}

rotation_failpoint() {
  [ "${BLOCKZILLA_RAW_TEST_FAIL_ROTATION_AT:-}" != "$1" ]
}

complete_rotation_transaction() {
  rotation_id=$1
  valid_generation_id "$rotation_id" || return 1
  rotation_next=$CACHE_ROOT/.next-$rotation_id
  rotation_old=$CACHE_ROOT/.sealed-$rotation_id
  rotation_target=$SEALED_GENERATION_DIR/$rotation_id

  if [ -e "$rotation_target" ]; then
    if [ ! -d "$ACTIVE_GENERATION_DIR" ] \
      || [ -e "$rotation_next" ] \
      || [ -e "$rotation_old" ]
    then
      echo "inconsistent completed cache rotation for $rotation_id" >&2
      return 1
    fi
  else
    if [ ! -e "$rotation_old" ]; then
      if [ ! -d "$ACTIVE_GENERATION_DIR" ] \
        || [ -L "$ACTIVE_GENERATION_DIR" ] \
        || [ ! -d "$rotation_next" ] \
        || [ -L "$rotation_next" ]
      then
        echo "cache rotation lacks its old or successor generation for $rotation_id" >&2
        return 1
      fi
      mv "$ACTIVE_GENERATION_DIR" "$rotation_old" || return 1
      sync_path "$CACHE_ROOT" || return 1
      rotation_failpoint after_old_hidden || return 1
    fi

    if [ ! -e "$ACTIVE_GENERATION_DIR" ]; then
      if [ ! -d "$rotation_old" ] \
        || [ -L "$rotation_old" ] \
        || [ ! -d "$rotation_next" ] \
        || [ -L "$rotation_next" ]
      then
        echo "cache rotation cannot publish its successor for $rotation_id" >&2
        return 1
      fi
      mv "$rotation_next" "$ACTIVE_GENERATION_DIR" || return 1
      sync_path "$CACHE_ROOT" || return 1
      rotation_failpoint after_successor_active || return 1
    elif [ -e "$rotation_next" ]; then
      echo "cache rotation has both active and pending successor for $rotation_id" >&2
      return 1
    fi

    if [ ! -d "$rotation_old" ] || [ -L "$rotation_old" ]; then
      echo "cache rotation old generation is invalid for $rotation_id" >&2
      return 1
    fi
    # Hot rotation publishes the successor before auditing the stopped
    # predecessor, so the Yellowstone stream can remain open. Keep the old
    # directory hidden from the uploader until a complete WAL/protobuf/PoH
    # replay proves the exact generation ID. Repeating this audit during crash
    # recovery is intentional and fail-closed.
    rotation_verify_report=$CACHE_ROOT/.rotation-recovery-verify.$$.json
    rm -f "$rotation_verify_report"
    if ! verify_generation "$rotation_old" "$rotation_verify_report" \
      || ! generation_id_from_verify_report "$rotation_verify_report" \
      || [ "$GENERATION_ID" != "$rotation_id" ]
    then
      rm -f "$rotation_verify_report"
      echo "cache rotation predecessor failed its publication audit for $rotation_id" >&2
      return 1
    fi
    rm -f "$rotation_verify_report"
    # This is the first point at which the uploader can discover the sealed
    # generation. The active successor is already durable and visible.
    mv "$rotation_old" "$rotation_target" || return 1
    sync_path "$SEALED_GENERATION_DIR" || return 1
    sync_path "$CACHE_ROOT" || return 1
    rotation_failpoint after_sealed_visible || return 1
  fi

  # The successor starts with one copied durable row, so its journal size is
  # smaller than the old generation. Reset active incident floors to that new
  # inode/size so the next append can produce the recovery transition.
  reset_rotated_journal_incident_floors
  rm -f "$GENERATION_ROTATION_MARKER" || return 1
  sync_path "$CACHE_ROOT"
}

cleanup_orphan_generation_seed_temps() {
  orphan_seed_removed=false
  for orphan_seed in "$CACHE_ROOT"/..next-slot-*.seed-*-*.tmp; do
    # A broken symlink does not satisfy -e, so test -L before treating an
    # unmatched glob as empty. Seed targets are always real directories.
    if [ ! -e "$orphan_seed" ] && [ ! -L "$orphan_seed" ]; then
      continue
    fi
    orphan_seed_name=${orphan_seed##*/}
    orphan_seed_body=${orphan_seed_name#..next-}
    orphan_seed_generation_id=${orphan_seed_body%%.seed-*}
    orphan_seed_prefix=..next-$orphan_seed_generation_id.seed-
    orphan_seed_suffix=${orphan_seed_name#"$orphan_seed_prefix"}
    case "$orphan_seed_suffix" in
      *.tmp) orphan_seed_numbers=${orphan_seed_suffix%.tmp} ;;
      *) orphan_seed_numbers= ;;
    esac
    orphan_seed_pid=${orphan_seed_numbers%%-*}
    orphan_seed_unique=${orphan_seed_numbers#*-}
    if ! valid_generation_id "$orphan_seed_generation_id" \
      || [ "$orphan_seed_suffix" = "$orphan_seed_name" ] \
      || [ "$orphan_seed_unique" = "$orphan_seed_numbers" ]
    then
      echo "invalid orphan generation seed name: $orphan_seed" >&2
      return 1
    fi
    case "$orphan_seed_pid" in
      ''|*[!0-9]*)
        echo "invalid orphan generation seed name: $orphan_seed" >&2
        return 1
        ;;
    esac
    case "$orphan_seed_unique" in
      ''|*[!0-9]*)
        echo "invalid orphan generation seed name: $orphan_seed" >&2
        return 1
        ;;
    esac
    if [ -L "$orphan_seed" ] || [ ! -d "$orphan_seed" ]; then
      echo "invalid orphan generation seed directory: $orphan_seed" >&2
      return 1
    fi
    rm -rf "$orphan_seed" || return 1
    orphan_seed_removed=true
  done
  if [ "$orphan_seed_removed" = true ]; then
    sync_path "$CACHE_ROOT" || return 1
  fi
}

recover_rotation_transaction() {
  cleanup_orphan_generation_seed_temps || return 1
  if [ ! -e "$GENERATION_ROTATION_MARKER" ]; then
    for orphan_next in "$CACHE_ROOT"/.next-slot-*; do
      [ -e "$orphan_next" ] || continue
      if [ -L "$orphan_next" ] || [ ! -d "$orphan_next" ]; then
        echo "invalid orphan cache successor: $orphan_next" >&2
        return 1
      fi
      rm -rf "$orphan_next" || return 1
    done
    for orphan_old in "$CACHE_ROOT"/.sealed-slot-*; do
      [ -e "$orphan_old" ] || continue
      echo "hidden sealed generation lacks a rotation marker: $orphan_old" >&2
      return 1
    done
    sync_path "$CACHE_ROOT" || return 1
    return 0
  fi
  read_rotation_marker || {
    echo "cache rotation marker is invalid" >&2
    return 1
  }
  complete_rotation_transaction "$ROTATION_GENERATION_ID"
}

verify_generation() {
  generation_dir=$1
  generation_report=$2
  "$BIN" verify-grpc-raw-poh \
    --output-dir "$generation_dir" \
    --max-record-bytes "$MAX_RECORD_BYTES" \
    --min-records 1 > "$generation_report"
}

rotate_active_generation() {
  verify_report=$CACHE_ROOT/.rotation-verify.$$.json
  seed_report=$CACHE_ROOT/.rotation-seed.$$.json
  rm -f "$verify_report" "$seed_report"
  if ! verify_generation "$ACTIVE_GENERATION_DIR" "$verify_report" \
    || ! generation_id_from_verify_report "$verify_report"
  then
    rm -f "$verify_report" "$seed_report"
    return 1
  fi
  if ! load_replay_recovery_floor \
    || ! reconcile_replay_recovery_with_verified_slot "$verified_last_slot"
  then
    echo "provider replay recovery state is inconsistent with the verified generation" >&2
    rm -f "$verify_report" "$seed_report"
    return 1
  fi
  rotation_id=$GENERATION_ID
  rotation_next=$CACHE_ROOT/.next-$rotation_id
  rotation_old=$CACHE_ROOT/.sealed-$rotation_id
  rotation_target=$SEALED_GENERATION_DIR/$rotation_id
  if [ -e "$rotation_next" ] || [ -e "$rotation_old" ] \
    || [ -e "$rotation_target" ] \
    || [ -e "$GENERATION_RECEIPT_DIR/$rotation_id.json" ] \
    || [ -e "$GENERATION_ROTATION_MARKER" ]
  then
    echo "cache generation rotation target already exists for $rotation_id" >&2
    rm -f "$verify_report" "$seed_report"
    return 1
  fi
  if ! "$BIN" seed-grpc-raw-generation \
    --source-dir "$ACTIVE_GENERATION_DIR" \
    --target-dir "$rotation_next" \
    --max-record-bytes "$MAX_RECORD_BYTES" > "$seed_report"
  then
    rm -f "$verify_report" "$seed_report"
    rm -rf "$rotation_next"
    return 1
  fi
  seeded_slot=$(sed -n \
    's/^[[:space:]]*"seeded_slot":[[:space:]]*\([0-9][0-9]*\),*$/\1/p' \
    "$seed_report" | tail -n 1)
  if [ "$seeded_slot" != "$verified_last_slot" ] \
    || ! verify_generation "$rotation_next" "$seed_report.verified"
  then
    echo "seeded cache generation does not preserve the verified durable tail" >&2
    rm -f "$verify_report" "$seed_report" "$seed_report.verified"
    rm -rf "$rotation_next"
    return 1
  fi
  if [ "$REPLAY_RECOVERY_NEEDS_CARRY" = true ] \
    && ! publish_replay_gap_record_to_generation "$rotation_next" \
      "$REPLAY_RECOVERY_ANCHOR_SLOT" "$REPLAY_RECOVERY_REQUESTED_SLOT" \
      "$REPLAY_MIN_RESUME_SLOT" "$REPLAY_PROVIDER_AVAILABLE_SLOT"
  then
    echo "seeded cache generation could not retain provider replay-gap evidence" >&2
    rm -f "$verify_report" "$seed_report" "$seed_report.verified"
    rm -rf "$rotation_next"
    return 1
  fi
  rm -f "$verify_report" "$seed_report" "$seed_report.verified"
  write_rotation_marker "$rotation_id" || return 1
  complete_rotation_transaction "$rotation_id"
}

prepare_cache_layout() {
  if [ -e "$CACHE_ROOT" ]; then
    if [ -L "$CACHE_ROOT" ] || [ ! -d "$CACHE_ROOT" ]; then
      echo "raw cache root is not a real directory" >&2
      return 1
    fi
  else
    mkdir "$CACHE_ROOT" || return 1
    sync_path /data || return 1
  fi
  cache_real_directory "$CACHE_ROOT" || return 1
  if [ "$GENERATION_RECEIPT_DIR" != "$CACHE_ROOT/receipts" ]; then
    echo "BLOCKZILLA_RAW_CACHE_RECEIPT_DIR must be $CACHE_ROOT/receipts" >&2
    return 1
  fi
  ensure_cache_child_directory "$SEALED_GENERATION_DIR" || return 1
  ensure_cache_child_directory "$GENERATION_RECEIPT_DIR" || return 1
  ensure_cache_child_directory "$GENERATION_MONITORING_DIR" || return 1
  ensure_cache_child_directory "$ALERT_STATE_DIR" || return 1
  recover_rotation_transaction || return 1
  if [ ! -e "$ACTIVE_GENERATION_DIR" ]; then
    mkdir "$ACTIVE_GENERATION_DIR" || return 1
    sync_path "$CACHE_ROOT" || return 1
  fi
  cache_real_directory "$ACTIVE_GENERATION_DIR" || return 1
}

normalized_generation_base_prefix() {
  normalized_prefix=$GENERATION_REMOTE_PREFIX
  while [ "${normalized_prefix%/}" != "$normalized_prefix" ]; do
    normalized_prefix=${normalized_prefix%/}
  done
  case "$normalized_prefix" in
    ''|.|..|/*|*//*|*/../*|../*|*/..|*/.|*'/./'*|./*) return 1 ;;
  esac
  printf '%s\n' "$normalized_prefix"
}

generation_remote_prefix() {
  prefix_generation_id=$1
  valid_generation_id "$prefix_generation_id" || return 1
  prefix_base=$(normalized_generation_base_prefix) || return 1
  case "$CLUSTER_ID:$ORIGIN_NODE_ID" in
    *[!A-Za-z0-9._:-]*) return 1 ;;
  esac
  printf '%s/%s/%s/%s\n' \
    "$prefix_base" "$CLUSTER_ID" "$ORIGIN_NODE_ID" "$prefix_generation_id"
}

valid_sha256_hex() {
  sha256_value=$1
  [ "${#sha256_value}" -eq 64 ] || return 1
  case "$sha256_value" in
    *[!0-9a-f]*) return 1 ;;
  esac
}

load_upload_chain() {
  UPLOAD_CHAIN_ID=
  UPLOAD_CHAIN_HASH=
  chain_file=$GENERATION_RECEIPT_DIR/.chain
  [ -e "$chain_file" ] || return 0
  if [ -L "$chain_file" ] || [ ! -f "$chain_file" ] || [ ! -r "$chain_file" ]; then
    return 1
  fi
  IFS=' ' read -r UPLOAD_CHAIN_ID UPLOAD_CHAIN_HASH chain_extra < "$chain_file" || return 1
  valid_generation_id "$UPLOAD_CHAIN_ID" || return 1
  valid_sha256_hex "$UPLOAD_CHAIN_HASH" || return 1
  [ -z "${chain_extra:-}" ]
}

validate_generation_receipt() {
  receipt_path=$1
  receipt_generation_id=$2
  receipt_remote_prefix=$3
  receipt_expected_predecessor=$4
  if [ -L "$receipt_path" ] || [ ! -f "$receipt_path" ] || [ ! -r "$receipt_path" ]; then
    return 1
  fi
  receipt_bytes=$(wc -c < "$receipt_path" | tr -d ' ')
  case "$receipt_bytes" in
    ''|*[!0-9]*|0) return 1 ;;
  esac
  [ "$receipt_bytes" -le 1048576 ] || return 1
  "$GENERATION_PYTHON_BIN" - "$receipt_path" "$receipt_generation_id" \
    "$receipt_remote_prefix" "$receipt_expected_predecessor" <<'PY'
import json
import re
import sys

try:
    path, generation_id, prefix, expected_predecessor = sys.argv[1:]
    with open(path, "r", encoding="utf-8") as stream:
        receipt = json.load(stream)
    hex64 = re.compile(r"[0-9a-f]{64}").fullmatch
    def version_id(value):
        return (
            type(value) is str
            and 0 < len(value.encode("utf-8")) <= 1024
            and not any(ord(character) < 0x20 or ord(character) == 0x7f for character in value)
        )
    assert type(receipt.get("schema_version")) is int and receipt["schema_version"] == 1
    assert receipt.get("generation_id") == generation_id
    assert receipt.get("remote_prefix") == prefix
    assert receipt.get("manifest_key") == prefix + "/manifest.json"
    assert receipt.get("commit_key") == prefix + "/_COMMITTED"
    assert hex64(receipt.get("manifest_sha256", ""))
    assert hex64(receipt.get("commit_sha256", ""))
    assert version_id(receipt.get("manifest_version_id"))
    assert version_id(receipt.get("commit_version_id"))
    assert type(receipt.get("file_count")) is int and receipt["file_count"] >= 1
    assert type(receipt.get("total_bytes")) is int and receipt["total_bytes"] > 0
    assert type(receipt.get("verified_unix_secs")) is int and receipt["verified_unix_secs"] > 0
    predecessor = receipt.get("predecessor_manifest_sha256")
    if expected_predecessor == "-":
        assert predecessor is None
    elif expected_predecessor != "*":
        assert predecessor == expected_predecessor
    if predecessor is not None:
        assert hex64(predecessor)
except (AssertionError, KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
PY
}

receipt_manifest_hash() {
  "$GENERATION_PYTHON_BIN" - "$1" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as stream:
    print(json.load(stream)["manifest_sha256"])
PY
}

publish_upload_chain() {
  chain_generation_id=$1
  chain_manifest_hash=$2
  chain_file=$GENERATION_RECEIPT_DIR/.chain
  chain_tmp=$chain_file.$$
  printf '%s %s\n' "$chain_generation_id" "$chain_manifest_hash" > "$chain_tmp" || return 1
  sync_path "$chain_tmp" || return 1
  mv -f "$chain_tmp" "$chain_file" || return 1
  sync_path "$GENERATION_RECEIPT_DIR"
}

finalize_uploaded_generation() {
  finalized_dir=$1
  finalized_id=$2
  finalized_prefix=$3
  finalized_receipt=$4
  load_upload_chain || return 1
  if [ "$UPLOAD_CHAIN_ID" = "$finalized_id" ]; then
    expected_predecessor=*
  elif [ -n "$UPLOAD_CHAIN_HASH" ]; then
    expected_predecessor=$UPLOAD_CHAIN_HASH
  else
    expected_predecessor=-
  fi
  validate_generation_receipt "$finalized_receipt" "$finalized_id" \
    "$finalized_prefix" "$expected_predecessor" || return 1
  finalized_hash=$(receipt_manifest_hash "$finalized_receipt") || return 1
  if [ "$UPLOAD_CHAIN_ID" = "$finalized_id" ]; then
    [ "$UPLOAD_CHAIN_HASH" = "$finalized_hash" ] || return 1
  else
    publish_upload_chain "$finalized_id" "$finalized_hash" || return 1
  fi
  case "$finalized_dir" in
    "$SEALED_GENERATION_DIR"/slot-*) ;;
    *) return 1 ;;
  esac
  [ -d "$finalized_dir" ] && [ ! -L "$finalized_dir" ] || return 1
  rm -rf "$finalized_dir" || return 1
  sync_path "$SEALED_GENERATION_DIR"
}

first_sealed_generation() {
  FIRST_SEALED_GENERATION=
  for sealed_candidate in "$SEALED_GENERATION_DIR"/slot-*; do
    [ -e "$sealed_candidate" ] || continue
    sealed_id=${sealed_candidate##*/}
    valid_generation_id "$sealed_id" || return 1
    if [ -L "$sealed_candidate" ] || [ ! -d "$sealed_candidate" ]; then
      return 1
    fi
    FIRST_SEALED_GENERATION=$sealed_candidate
    return 0
  done
  return 0
}

sealed_generation_count() {
  sealed_count=0
  for sealed_candidate in "$SEALED_GENERATION_DIR"/slot-*; do
    [ -e "$sealed_candidate" ] || continue
    [ -d "$sealed_candidate" ] && [ ! -L "$sealed_candidate" ] || return 1
    sealed_count=$((sealed_count + 1))
  done
  printf '%s\n' "$sealed_count"
}

percentage_bytes() {
  percentage_total=$1
  percentage_value=$2
  case "$percentage_total" in ''|*[!0-9]*) return 1 ;; esac
  case "$percentage_value" in ''|*[!0-9]*) return 1 ;; esac
  [ "$percentage_total" -gt 0 ] || return 1
  [ "$percentage_value" -le 100 ] || return 1
  # Divide before multiplying so a large filesystem cannot overflow merely
  # while calculating a percentage.
  printf '%s\n' "$((
    (percentage_total / 100) * percentage_value
    + ((percentage_total % 100) * percentage_value / 100)
  ))"
}

generation_spill_thresholds() {
  spill_total_bytes=$1
  spill_start_ratio=$(percentage_bytes "$spill_total_bytes" \
    "$GENERATION_SPILL_START_PERCENT") || return 1
  spill_recovery_ratio=$(percentage_bytes "$spill_total_bytes" \
    "$GENERATION_SPILL_RECOVERY_PERCENT") || return 1
  spill_start_reserve=$((MIN_FREE_BYTES + MAX_GENERATION_BYTES))
  spill_recovery_reserve=$((MIN_FREE_BYTES + (2 * MAX_GENERATION_BYTES)))
  if [ "$spill_start_reserve" -le "$MIN_FREE_BYTES" ] \
    || [ "$spill_recovery_reserve" -le "$spill_start_reserve" ]
  then
    return 1
  fi
  GENERATION_SPILL_START_BYTES=$spill_start_ratio
  if [ "$spill_start_reserve" -gt "$GENERATION_SPILL_START_BYTES" ]; then
    GENERATION_SPILL_START_BYTES=$spill_start_reserve
  fi
  GENERATION_SPILL_RECOVERY_BYTES=$spill_recovery_ratio
  if [ "$spill_recovery_reserve" -gt "$GENERATION_SPILL_RECOVERY_BYTES" ]; then
    GENERATION_SPILL_RECOVERY_BYTES=$spill_recovery_reserve
  fi
  [ "$GENERATION_SPILL_RECOVERY_BYTES" \
    -gt "$GENERATION_SPILL_START_BYTES" ] || return 1
}

update_generation_spill_state() {
  spill_free_bytes=$1
  spill_total_bytes=$2
  case "$spill_free_bytes" in ''|*[!0-9]*) return 1 ;; esac
  case "$spill_total_bytes" in ''|*[!0-9]*) return 1 ;; esac
  generation_spill_thresholds "$spill_total_bytes" || return 1
  if [ "$generation_spill_active" = true ]; then
    if [ "$spill_free_bytes" -ge "$GENERATION_SPILL_RECOVERY_BYTES" ]; then
      generation_spill_active=false
    fi
  elif [ "$spill_free_bytes" -lt "$GENERATION_SPILL_START_BYTES" ]; then
    generation_spill_active=true
  fi
}

generation_upload_failure_details() {
  failure_status=$1
  case "$failure_status" in
    20)
      pipeline_cause="Status: Backblaze download limit reached."
      pipeline_action="Action: Raise the Backblaze download limit or wait for the daily reset."
      ;;
    21)
      pipeline_cause="Status: Backblaze Class C limit reached."
      pipeline_action="Action: Raise the Backblaze Class C limit or wait for the daily reset."
      ;;
    22)
      pipeline_cause="Status: Backblaze storage limit reached."
      pipeline_action="Action: Increase the Backblaze storage limit."
      ;;
    *)
      pipeline_cause="Status: Backblaze upload failed."
      pipeline_action="Action: Check Backblaze and the recorder logs. Automatic retry is on."
      ;;
  esac
}

upload_one_generation() {
  # A rotation marker means the old generation may be visible but the rename
  # transaction is not yet committed. Recovery owns it until the marker clears.
  [ ! -e "$GENERATION_ROTATION_MARKER" ] || return 75
  first_sealed_generation || return 1
  [ -n "$FIRST_SEALED_GENERATION" ] || return 0
  upload_dir=$FIRST_SEALED_GENERATION
  upload_id=${upload_dir##*/}
  upload_prefix=$(generation_remote_prefix "$upload_id") || return 1
  upload_receipt=$GENERATION_RECEIPT_DIR/$upload_id.json
  load_upload_chain || return 1
  if [ "$UPLOAD_CHAIN_ID" = "$upload_id" ]; then
    existing_expected=*
  elif [ -n "$UPLOAD_CHAIN_HASH" ]; then
    existing_expected=$UPLOAD_CHAIN_HASH
  else
    existing_expected=-
  fi
  if validate_generation_receipt "$upload_receipt" "$upload_id" \
    "$upload_prefix" "$existing_expected" 2>/dev/null
  then
    finalize_uploaded_generation "$upload_dir" "$upload_id" \
      "$upload_prefix" "$upload_receipt"
    return $?
  fi
  if [ -L "$GENERATION_CREDENTIALS_FILE" ] \
    || [ ! -f "$GENERATION_CREDENTIALS_FILE" ] \
    || [ ! -r "$GENERATION_CREDENTIALS_FILE" ] \
    || [ ! -x "$GENERATION_UPLOADER_BIN" ]
  then
    return 1
  fi
  set -- upload-generation "$upload_dir" "$upload_prefix" "$upload_receipt" \
    --generation-id "$upload_id" \
    --credentials-file "$GENERATION_CREDENTIALS_FILE"
  if [ -n "$UPLOAD_CHAIN_HASH" ]; then
    set -- "$@" --predecessor-manifest-sha256 "$UPLOAD_CHAIN_HASH"
  fi
  "$GENERATION_UPLOADER_BIN" "$@" &
  generation_uploader_pid=$!
  if wait "$generation_uploader_pid"; then
    generation_uploader_status=0
  else
    generation_uploader_status=$?
  fi
  generation_uploader_pid=
  if [ "$generation_uploader_status" -ne 0 ]; then
    # Preserve the uploader's small, allowlisted capacity statuses so the one
    # correlated pipeline incident can name the exact operator action. Unknown
    # failures remain generic and are never inferred from an untyped HTTP 403.
    case "$generation_uploader_status" in
      20|21|22) return "$generation_uploader_status" ;;
      *) return 1 ;;
    esac
  fi
  finalize_uploaded_generation "$upload_dir" "$upload_id" \
    "$upload_prefix" "$upload_receipt"
}

terminate_generation_upload_worker() {
  if [ -n "${generation_uploader_pid:-}" ]; then
    kill -TERM "$generation_uploader_pid" 2>/dev/null || true
    wait "$generation_uploader_pid" 2>/dev/null || true
  fi
  exit 0
}

raise_generation_spill_gate_alert() {
  spill_gate_failure=$1
  spill_gate_backlog=$2
  spill_gate_free=$3
  spill_gate_level=ERROR
  case "$spill_gate_failure" in
    queue)
      spill_gate_status="Status: Cannot read local backup files. Backblaze upload is paused; saved data is safe."
      ;;
    free_space)
      spill_gate_status="Status: Cannot read Hetzner free space. Backblaze upload is paused; saved data is safe."
      ;;
    capacity)
      spill_gate_status="Status: Cannot read Hetzner disk size. Backblaze upload is paused; saved data is safe."
      ;;
    policy)
      spill_gate_status="Status: Storage safety check failed. Backblaze upload is paused; saved data is safe."
      ;;
    *)
      spill_gate_status="Status: Storage safety check failed. Backblaze upload is paused; saved data is safe."
      ;;
  esac
  case "$spill_gate_backlog" in
    ''|*[!0-9]*)
      spill_gate_backlog_detail="Waiting backup count unavailable."
      ;;
    *)
      spill_gate_backlog_detail="$spill_gate_backlog backup batches waiting locally."
      ;;
  esac
  case "$spill_gate_free" in
    ''|*[!0-9]*)
      spill_gate_space_detail="Storage: Free space unavailable. $spill_gate_backlog_detail"
      ;;
    *)
      spill_gate_space_detail="Storage: $(human_decimal_bytes "$spill_gate_free") free. $spill_gate_backlog_detail"
      if [ "$spill_gate_free" -lt "$MIN_FREE_BYTES" ]; then
        spill_gate_level=CRITICAL
        spill_gate_space_detail="$spill_gate_space_detail Backup recording is paused."
      fi
      ;;
  esac
  rm -f "$(alert_file generation_backlog handoff)" 2>/dev/null || true
  raise_alert generation_backlog "$spill_gate_level" \
    "$spill_gate_status
$spill_gate_space_detail
Action: Check the Hetzner disk and recorder service. Automatic retry is on."
}

generation_backlog_local_incident_active() {
  gate_incident_file=$(alert_file generation_backlog active)
  if [ -L "$gate_incident_file" ] \
    || [ ! -f "$gate_incident_file" ] \
    || [ ! -r "$gate_incident_file" ]
  then
    return 1
  fi
  gate_incident_bytes=$(wc -c < "$gate_incident_file" | tr -d ' ')
  case "$gate_incident_bytes" in
    ''|*[!0-9]*|0) return 1 ;;
  esac
  [ "$gate_incident_bytes" -le 4096 ] || return 1
  gate_incident_marker=$(sed -n '/^Status: /p' "$gate_incident_file" | head -n 1)
  case "$gate_incident_marker" in
    'Status: Cannot read local backup files. Backblaze upload is paused; saved data is safe.'|\
    'Status: Cannot read Hetzner free space. Backblaze upload is paused; saved data is safe.'|\
    'Status: Cannot read Hetzner disk size. Backblaze upload is paused; saved data is safe.'|\
    'Status: Storage safety check failed. Backblaze upload is paused; saved data is safe.'|\
    'Status: Cannot read local backup files. Upload cleanup is paused; saved data is safe.'|\
    'Status: Cannot read Hetzner free space. Upload cleanup is paused; saved data is safe.'|\
    'Status: Cannot read Hetzner disk size. Upload cleanup is paused; saved data is safe.'|\
    'Status: Storage safety check failed. Upload cleanup is paused; saved data is safe.'|\
    'Status: Hetzner disk is full. Backup is paused; saved data is safe.')
      return 0
      ;;
  esac
  # Accept the exact previous copy during a rolling deployment.
  gate_incident_marker=$(sed -n '/^Cause: /p' "$gate_incident_file" | head -n 1)
  case "$gate_incident_marker" in
    'Cause: Hetzner could not inspect the sealed-generation queue safely.'|\
    'Cause: Hetzner could not measure free space on the recorder filesystem.'|\
    'Cause: Hetzner could not measure the recorder filesystem capacity.'|\
    'Cause: Hetzner could not validate the disk spill watermarks from the measured filesystem values.'|\
    'Cause: The disk-first Hetzner cache reached its safety floor before Backblaze spill cleanup recovered headroom.')
      return 0
      ;;
    *) return 1 ;;
  esac
}

generation_upload_worker() {
  generation_uploader_pid=
  generation_spill_active=false
  trap terminate_generation_upload_worker INT TERM HUP
  while :; do
    if validate_data_volume true >/dev/null 2>&1 \
      && [ -d "$SEALED_GENERATION_DIR" ]
    then
      generation_upload_failed=false
      generation_retry_immediately=false
      generation_space_valid=false
      generation_free_bytes=
      generation_total_bytes=
      generation_backlog=
      generation_gate_failure=
      generation_policy_ready=false
      if ! generation_backlog=$(sealed_generation_count 2>/dev/null); then
        generation_gate_failure=queue
      elif ! generation_free_bytes=$(available_bytes 2>/dev/null); then
        generation_gate_failure=free_space
      elif ! generation_total_bytes=$(filesystem_capacity_bytes 2>/dev/null); then
        generation_gate_failure=capacity
      elif ! update_generation_spill_state "$generation_free_bytes" \
        "$generation_total_bytes"
      then
        generation_gate_failure=policy
      else
        generation_policy_ready=true
      fi
      if [ "$generation_policy_ready" = true ]; then
        generation_space_valid=true
        generation_backlog_before=$generation_backlog
        generation_free_before=$generation_free_bytes
        generation_upload_attempted=false
        generation_upload_succeeded=false
        upload_status=
        if [ "$generation_spill_active" = true ] \
          && [ "$generation_backlog" -gt 0 ]
        then
          generation_upload_attempted=true
          if upload_one_generation; then
            upload_status=0
            generation_upload_succeeded=true
          else
            upload_status=$?
            if [ "$upload_status" -ne 75 ]; then
              generation_upload_failed=true
              generation_upload_failure_details "$upload_status"
            fi
          fi
        fi

        # Refresh both values after a verified upload and local removal. The
        # worker drains FIFO without an artificial retry delay until the high
        # watermark is restored; normal local retention does not touch B2.
        generation_refresh_failure=
        if ! generation_backlog=$(sealed_generation_count 2>/dev/null); then
          generation_refresh_failure=queue
        elif ! generation_free_bytes=$(available_bytes 2>/dev/null); then
          generation_refresh_failure=free_space
        elif ! generation_total_bytes=$(filesystem_capacity_bytes 2>/dev/null); then
          generation_refresh_failure=capacity
        elif ! update_generation_spill_state "$generation_free_bytes" \
          "$generation_total_bytes"
        then
          generation_refresh_failure=policy
        else
          generation_space_valid=true
        fi
        if [ -n "$generation_refresh_failure" ]; then
          generation_space_valid=false
        fi

        if [ "$generation_upload_failed" = true ]; then
          pipeline_level=ERROR
          if [ "$generation_space_valid" != true ]; then
            pipeline_level=CRITICAL
            pipeline_capture="Backup status cannot be confirmed; saved data is safe."
          elif [ "$generation_free_bytes" -lt "$MIN_FREE_BYTES" ]; then
            pipeline_level=CRITICAL
            pipeline_capture="Backup is paused; saved data is safe."
          else
            pipeline_capture="Hetzner still has room for recording; saved data is safe."
          fi
          case "$generation_backlog" in
            ''|*[!0-9]*) generation_batch_detail="Waiting backup count unavailable." ;;
            *) generation_batch_detail="$generation_backlog backup batches waiting locally." ;;
          esac
          if [ "$generation_space_valid" = true ]; then
            generation_free_detail="Storage: $(human_decimal_bytes "$generation_free_bytes") free. $generation_batch_detail"
          else
            generation_free_detail="Storage: Free space unavailable. $generation_batch_detail"
          fi
          rm -f "$(alert_file generation_backlog handoff)" 2>/dev/null || true
          raise_alert generation_backlog "$pipeline_level" \
            "$pipeline_cause $pipeline_capture
$generation_free_detail
$pipeline_action"
        elif [ -n "$generation_refresh_failure" ]; then
          raise_generation_spill_gate_alert "$generation_refresh_failure" \
            "$generation_backlog" "$generation_free_bytes"
        elif [ "$generation_space_valid" = true ] \
          && [ "$generation_spill_active" = false ] \
          && [ "$generation_free_bytes" -ge "$GENERATION_SPILL_RECOVERY_BYTES" ] \
          && generation_backlog_local_incident_active
        then
          clear_alert generation_backlog \
            "Status: Backup storage checks are working again.
Storage: $(human_decimal_bytes "$generation_free_bytes") free. Saved data is safe.
Action: None."
        elif [ "$generation_space_valid" = true ] \
          && [ "$generation_spill_active" = false ] \
          && [ "$generation_free_bytes" -ge "$GENERATION_SPILL_RECOVERY_BYTES" ] \
          && { [ "$generation_upload_succeeded" = true ] \
            || [ "$generation_backlog" -eq 0 ]; }
        then
          clear_alert generation_backlog \
            "Status: Backblaze uploads are working and backup recording can continue.
Storage: $(human_decimal_bytes "$generation_free_bytes") free on Hetzner.
Action: None."
        elif [ "$generation_space_valid" = true ] \
          && [ "$generation_spill_active" = true ] \
          && [ "$generation_backlog" -gt 0 ] \
          && [ "$generation_upload_attempted" = true ] \
          && [ "$upload_status" -eq 0 ] \
          && { [ "$generation_backlog" -lt "$generation_backlog_before" ] \
            || [ "$generation_free_bytes" -gt "$generation_free_before" ]; }
        then
          generation_retry_immediately=true
        elif [ "$generation_space_valid" = true ] \
          && [ "$generation_spill_active" = true ] \
          && [ "$generation_backlog" -gt 0 ] \
          && [ "$generation_upload_attempted" = true ] \
          && [ "$upload_status" -eq 0 ]
        then
          pipeline_level=ERROR
          pipeline_capture="Hetzner still has room for recording; saved data is safe."
          if [ "$generation_free_bytes" -lt "$MIN_FREE_BYTES" ]; then
            pipeline_level=CRITICAL
            pipeline_capture="Backup is paused; saved data is safe."
          fi
          rm -f "$(alert_file generation_backlog handoff)" 2>/dev/null || true
          raise_alert generation_backlog "$pipeline_level" \
            "Status: Backblaze upload finished, but Hetzner space did not increase. $pipeline_capture
Storage: $(human_decimal_bytes "$generation_free_bytes") free; $generation_backlog backup batches waiting locally.
Action: Check the recorder logs. Automatic retry is on."
        fi
      else
        raise_generation_spill_gate_alert "$generation_gate_failure" \
          "$generation_backlog" "$generation_free_bytes"
      fi
      # This older derivative incident was replaced by generation_backlog so a
      # single remote failure cannot fan out into upload, backlog, and disk
      # notifications.
      discard_alert generation_upload_failed
      if [ "$generation_retry_immediately" = true ]; then
        continue
      fi
    fi
    sleep "$GENERATION_UPLOAD_RETRY_SECS"
  done
}

start_generation_upload_worker() {
  rm -f "$(alert_file generation_backlog handoff)" 2>/dev/null || true
  generation_upload_worker &
  upload_worker_pid=$!
}

b2_usage_report_bytes() {
  usage_report=$1
  if [ -L "$usage_report" ] \
    || [ ! -f "$usage_report" ] \
    || [ ! -r "$usage_report" ]
  then
    return 1
  fi
  usage_report_size=$(wc -c < "$usage_report" | tr -d ' ')
  case "$usage_report_size" in
    ''|*[!0-9]*|0) return 1 ;;
  esac
  [ "$usage_report_size" -le 65536 ] || return 1
  "$GENERATION_PYTHON_BIN" - "$usage_report" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    report = json.load(handle)
if not isinstance(report, dict):
    raise SystemExit("usage report must be an object")
if report.get("schema_version") != 1 or report.get("scope_complete") is not True:
    raise SystemExit("usage report is incomplete or has an unsupported schema")
stored_bytes = report.get("total_stored_bytes")
if isinstance(stored_bytes, bool) or not isinstance(stored_bytes, int) or stored_bytes < 0:
    raise SystemExit("usage report has invalid total_stored_bytes")
print(stored_bytes)
PY
}

run_b2_usage_scan() {
  if [ -L "$GENERATION_CREDENTIALS_FILE" ] \
    || [ ! -f "$GENERATION_CREDENTIALS_FILE" ] \
    || [ ! -r "$GENERATION_CREDENTIALS_FILE" ] \
    || [ ! -x "$GENERATION_UPLOADER_BIN" ]
  then
    return 1
  fi
  usage_tmp=$B2_USAGE_REPORT_FILE.$$.tmp
  rm -f "$usage_tmp" 2>/dev/null || return 1
  "$GENERATION_UPLOADER_BIN" b2-account-usage \
    --credentials-file "$GENERATION_CREDENTIALS_FILE" > "$usage_tmp" &
  b2_usage_query_pid=$!
  if wait "$b2_usage_query_pid"; then
    usage_status=0
  else
    usage_status=$?
  fi
  b2_usage_query_pid=
  if [ "$usage_status" -ne 0 ]; then
    rm -f "$usage_tmp" 2>/dev/null || true
    return 1
  fi
  if ! B2_USAGE_BYTES=$(b2_usage_report_bytes "$usage_tmp"); then
    rm -f "$usage_tmp" 2>/dev/null || true
    return 1
  fi
  if ! chmod 0600 "$usage_tmp" \
    || ! sync_path "$usage_tmp" \
    || ! mv -f "$usage_tmp" "$B2_USAGE_REPORT_FILE" \
    || ! sync_path "$GENERATION_MONITORING_DIR"
  then
    rm -f "$usage_tmp" 2>/dev/null || true
    return 1
  fi
  return 0
}

update_b2_usage_alerts() {
  usage_bytes=$1
  usage_active=$(alert_file b2_usage active)

  # Retire the two-key implementation silently. One account-capacity problem
  # must escalate and recover as one incident, not as warning/critical pairs.
  discard_alert b2_usage_warning
  discard_alert b2_usage_critical

  if [ "$usage_bytes" -ge "$B2_USAGE_CRITICAL_BYTES" ]; then
    raise_alert_once b2_usage CRITICAL \
      "Status: Backblaze is almost full. Existing data is safe.
Storage: $(human_decimal_bytes "$usage_bytes") used of $(human_decimal_bytes "$B2_USAGE_ALLOWANCE_BYTES").
Action: Increase the Backblaze storage limit now."
  elif [ "$usage_bytes" -ge "$B2_USAGE_WARNING_BYTES" ]; then
    raise_alert_once b2_usage WARNING \
      "Status: Backblaze storage is filling up. Existing data is safe.
Storage: $(human_decimal_bytes "$usage_bytes") used of $(human_decimal_bytes "$B2_USAGE_ALLOWANCE_BYTES").
Action: Increase the storage limit before it reaches $(human_decimal_bytes "$B2_USAGE_ALLOWANCE_BYTES")."
  elif [ -e "$usage_active" ] \
    && [ "$usage_bytes" -lt "$B2_USAGE_WARNING_RECOVERY_BYTES" ]
  then
    clear_alert b2_usage \
      "Status: Backblaze storage is below the alert level again.
Storage: $(human_decimal_bytes "$usage_bytes") used of $(human_decimal_bytes "$B2_USAGE_ALLOWANCE_BYTES").
Action: None."
  fi
}

terminate_b2_usage_worker() {
  if [ -n "${b2_usage_query_pid:-}" ]; then
    kill -TERM "$b2_usage_query_pid" 2>/dev/null || true
    wait "$b2_usage_query_pid" 2>/dev/null || true
  fi
  exit 0
}

b2_usage_scan_failed_alert() {
  if [ -e "$(alert_file generation_backlog active)" ]; then
    # A provider/API failure already belongs to the correlated backup-pipeline
    # incident. Do not fan the same cap or outage out into a second storage-
    # measurement opening (and later a second recovery).
    discard_alert b2_usage_check_failed
    return 0
  fi
  raise_alert_once b2_usage_check_failed ERROR \
    "Status: Backblaze storage usage is unavailable.
Storage: Unknown.
Action: Check Backblaze if this remains active."
}

b2_usage_scan_recovered_alert() {
  recovered_usage_bytes=${1:-}
  if [ -e "$(alert_file generation_backlog active)" ]; then
    discard_alert b2_usage_check_failed
    return 0
  fi
  case "$recovered_usage_bytes" in
    ''|*[!0-9]*) recovered_usage_detail='Storage: Current usage is available.' ;;
    *) recovered_usage_detail="Storage: $(human_decimal_bytes "$recovered_usage_bytes") used of $(human_decimal_bytes "$B2_USAGE_ALLOWANCE_BYTES")." ;;
  esac
  clear_alert b2_usage_check_failed \
    "Status: Backblaze storage checks are working again.
$recovered_usage_detail
Action: None."
}

b2_usage_worker() {
  b2_usage_query_pid=
  trap terminate_b2_usage_worker INT TERM HUP
  while :; do
    usage_sleep=$B2_USAGE_CHECK_INTERVAL_SECS
    if ! validate_data_volume true >/dev/null 2>&1; then
      : # The dedicated volume incident is reported by the main monitor.
    elif run_b2_usage_scan; then
      b2_usage_scan_recovered_alert "$B2_USAGE_BYTES"
      update_b2_usage_alerts "$B2_USAGE_BYTES"
      echo "$(timestamp) b2_usage stored_bytes=$B2_USAGE_BYTES allowance_bytes=$B2_USAGE_ALLOWANCE_BYTES" >&2
      if [ "$B2_USAGE_BYTES" -ge "$B2_USAGE_CRITICAL_BYTES" ]; then
        usage_sleep=$B2_USAGE_OVER_LIMIT_CHECK_INTERVAL_SECS
      fi
    else
      b2_usage_scan_failed_alert
    fi
    sleep "$usage_sleep"
  done
}

start_b2_usage_worker() {
  b2_usage_worker &
  b2_usage_worker_pid=$!
}

healthcheck() {
  stale_after=${BLOCKZILLA_RAW_STALE_AFTER_SECS:-180}
  startup_grace=${BLOCKZILLA_RAW_STARTUP_GRACE_SECS:-300}
  require_uint BLOCKZILLA_RAW_STALE_AFTER_SECS "$stale_after"
  require_uint BLOCKZILLA_RAW_STARTUP_GRACE_SECS "$startup_grace"

  validate_data_paths || return 1
  validate_data_volume true || return 1

  if [ -r "$STATE_FILE" ]; then
    IFS=' ' read -r recorder_state _state_time < "$STATE_FILE" || recorder_state=unknown
    if [ "$recorder_state" = low_disk ] \
      || [ "$recorder_state" = disk_check_failed ] \
      || [ "$recorder_state" = cache_rotation_failed ] \
      || [ "$recorder_state" = replay_recovery_failed ] \
      || [ "$recorder_state" = stopping ]
    then
      echo "raw recorder state is $recorder_state" >&2
      return 1
    fi
  fi

  now=$(date +%s)
  if [ -s "$JOURNAL_FILE" ]; then
    modified=$(stat -c %Y "$JOURNAL_FILE") || return 1
    if [ "$modified" -gt "$now" ]; then
      age=0
    else
      age=$((now - modified))
    fi
    if [ "$age" -le "$stale_after" ]; then
      return 0
    fi
    echo "raw journal is stale: age_seconds=$age limit_seconds=$stale_after" >&2
    return 1
  fi

  if [ ! -e "$STARTED_FILE" ]; then
    echo "raw recorder startup marker is missing" >&2
    return 1
  fi
  started=$(stat -c %Y "$STARTED_FILE") || return 1
  if [ "$started" -gt "$now" ]; then
    startup_age=0
  else
    startup_age=$((now - started))
  fi
  if [ "$startup_age" -le "$startup_grace" ]; then
    return 0
  fi
  echo "raw journal was not created within startup grace: age_seconds=$startup_age" >&2
  return 1
}

if [ "${1:-}" = --healthcheck ]; then
  healthcheck
  exit $?
fi

if [ "${1:-}" = --telegram-test ]; then
  if [ "$#" -ne 1 ] || [ "$TELEGRAM_ENABLED" != true ]; then
    echo "Telegram alerts must be enabled for --telegram-test" >&2
    exit 2
  fi
  if ! validate_telegram_config; then
    exit 2
  fi
  test_text=$(printf 'Blockzilla backup - TEST\nTelegram alerts are working\nStatus: Test message received.\nAction: None.')
  if ! telegram_send "$test_text"; then
    echo "Telegram test delivery failed" >&2
    exit 1
  fi
  echo "Telegram test alert delivered" >&2
  exit 0
fi

if [ "$#" -ne 0 ]; then
  echo "usage: $0 [--healthcheck|--telegram-test]" >&2
  exit 2
fi

if ! validate_telegram_config; then
  exit 2
fi
if [ "$TELEGRAM_ENABLED" = true ]; then
  if ! load_telegram_token; then
    exit 2
  fi
  telegram_token=
fi

: "${BLOCKZILLA_GRPC_ENDPOINT:?set BLOCKZILLA_GRPC_ENDPOINT}"
if [ -z "${BLOCKZILLA_GRPC_X_TOKEN:-}" ] && [ -z "${BLOCKZILLA_GRPC_X_TOKEN_FILE:-}" ]; then
  echo "set BLOCKZILLA_GRPC_X_TOKEN_FILE (preferred) or BLOCKZILLA_GRPC_X_TOKEN" >&2
  exit 2
fi
if [ -n "${BLOCKZILLA_GRPC_X_TOKEN_FILE:-}" ]; then
  if [ -L "$BLOCKZILLA_GRPC_X_TOKEN_FILE" ] \
    || [ ! -f "$BLOCKZILLA_GRPC_X_TOKEN_FILE" ] \
    || [ ! -r "$BLOCKZILLA_GRPC_X_TOKEN_FILE" ]
  then
    echo "gRPC x-token file is missing, unreadable, or a symlink" >&2
    exit 2
  fi
fi

if ! validate_data_paths; then
  exit 2
fi
case "$REQUIRE_COMPLETE_POH" in
  true|false) ;;
  *)
    echo "BLOCKZILLA_RAW_REQUIRE_COMPLETE_POH must be true or false" >&2
    exit 2
    ;;
esac
case "$B2_USAGE_ALERT_ENABLED" in
  true|false) ;;
  *)
    echo "BLOCKZILLA_B2_USAGE_ALERT_ENABLED must be true or false" >&2
    exit 2
    ;;
esac
if [ "$B2_USAGE_ALERT_ENABLED" = true ] && [ "$CACHE_MODE" != b2-generations ]; then
  echo "Backblaze usage alerts require BLOCKZILLA_RAW_CACHE_MODE=b2-generations" >&2
  exit 2
fi

for numeric_setting in \
  "BLOCKZILLA_RAW_MIN_FREE_BYTES:$MIN_FREE_BYTES" \
  "BLOCKZILLA_RAW_MAX_BLOCKS:$MAX_BLOCKS" \
  "BLOCKZILLA_RAW_TIMEOUT_SECS:$TIMEOUT_SECS" \
  "BLOCKZILLA_RAW_IDLE_TIMEOUT_SECS:$IDLE_TIMEOUT_SECS" \
  "BLOCKZILLA_RAW_RESTART_DELAY_SECS:$RESTART_DELAY_SECS" \
  "BLOCKZILLA_RAW_LOW_DISK_RECHECK_SECS:$LOW_DISK_RECHECK_SECS" \
  "BLOCKZILLA_RAW_SEGMENT_TARGET_BYTES:$SEGMENT_TARGET_BYTES" \
  "BLOCKZILLA_RAW_MAX_RECORD_BYTES:$MAX_RECORD_BYTES" \
  "BLOCKZILLA_RAW_MAX_GENERATION_BYTES:$MAX_GENERATION_BYTES" \
  "BLOCKZILLA_RAW_GENERATION_BACKLOG_WARN_COUNT:$GENERATION_BACKLOG_WARN_COUNT" \
  "BLOCKZILLA_RAW_GENERATION_UPLOAD_RETRY_SECS:$GENERATION_UPLOAD_RETRY_SECS" \
  "BLOCKZILLA_RAW_B2_SPILL_START_PERCENT:$GENERATION_SPILL_START_PERCENT" \
  "BLOCKZILLA_RAW_B2_SPILL_RECOVERY_PERCENT:$GENERATION_SPILL_RECOVERY_PERCENT" \
  "BLOCKZILLA_RAW_REPLAY_RESUME_HEADROOM_SLOTS:$REPLAY_RESUME_HEADROOM_SLOTS" \
  "BLOCKZILLA_B2_USAGE_ALLOWANCE_BYTES:$B2_USAGE_ALLOWANCE_BYTES" \
  "BLOCKZILLA_B2_USAGE_WARNING_BYTES:$B2_USAGE_WARNING_BYTES" \
  "BLOCKZILLA_B2_USAGE_CRITICAL_BYTES:$B2_USAGE_CRITICAL_BYTES" \
  "BLOCKZILLA_B2_USAGE_RECOVERY_HYSTERESIS_BYTES:$B2_USAGE_RECOVERY_HYSTERESIS_BYTES" \
  "BLOCKZILLA_B2_USAGE_CHECK_INTERVAL_SECS:$B2_USAGE_CHECK_INTERVAL_SECS" \
  "BLOCKZILLA_B2_USAGE_OVER_LIMIT_CHECK_INTERVAL_SECS:$B2_USAGE_OVER_LIMIT_CHECK_INTERVAL_SECS" \
  "BLOCKZILLA_TELEGRAM_ALERT_COOLDOWN_SECS:$TELEGRAM_ALERT_COOLDOWN_SECS" \
  "BLOCKZILLA_RAW_DISK_WARN_FREE_BYTES:$DISK_WARN_FREE_BYTES" \
  "BLOCKZILLA_RAW_DISK_RECOVERY_HYSTERESIS_BYTES:$DISK_RECOVERY_HYSTERESIS_BYTES" \
  "BLOCKZILLA_RAW_MONITOR_INTERVAL_SECS:$MONITOR_INTERVAL_SECS" \
  "BLOCKZILLA_RAW_STALE_AFTER_SECS:$RAW_STALE_AFTER_SECS" \
  "BLOCKZILLA_RAW_STARTUP_GRACE_SECS:$STARTUP_GRACE_SECS" \
  "BLOCKZILLA_PRIMARY_SYNC_STALE_AFTER_SECS:$PRIMARY_SYNC_STALE_AFTER_SECS"
do
  setting_name=${numeric_setting%%:*}
  setting_value=${numeric_setting#*:}
  require_uint "$setting_name" "$setting_value"
done
if [ -n "$INITIAL_FROM_SLOT" ]; then
  require_uint BLOCKZILLA_RAW_FROM_SLOT "$INITIAL_FROM_SLOT"
fi
if [ "$MONITOR_INTERVAL_SECS" -eq 0 ]; then
  echo "BLOCKZILLA_RAW_MONITOR_INTERVAL_SECS must be non-zero" >&2
  exit 2
fi
if [ "$B2_USAGE_WARNING_BYTES" -eq 0 ] \
  || [ "$B2_USAGE_WARNING_BYTES" -ge "$B2_USAGE_CRITICAL_BYTES" ] \
  || [ "$B2_USAGE_CRITICAL_BYTES" -gt "$B2_USAGE_ALLOWANCE_BYTES" ] \
  || [ "$B2_USAGE_RECOVERY_HYSTERESIS_BYTES" -eq 0 ] \
  || [ "$B2_USAGE_RECOVERY_HYSTERESIS_BYTES" -ge "$B2_USAGE_WARNING_BYTES" ] \
  || [ "$B2_USAGE_CHECK_INTERVAL_SECS" -eq 0 ] \
  || [ "$B2_USAGE_OVER_LIMIT_CHECK_INTERVAL_SECS" -eq 0 ]
then
  echo "Backblaze usage thresholds, hysteresis, and intervals are invalid" >&2
  exit 2
fi
B2_USAGE_WARNING_RECOVERY_BYTES=$((B2_USAGE_WARNING_BYTES - B2_USAGE_RECOVERY_HYSTERESIS_BYTES))
if [ "$CACHE_MODE" = b2-generations ]; then
  if ! valid_replay_shell_uint "$REPLAY_RESUME_HEADROOM_SLOTS" \
    || [ "$REPLAY_RESUME_HEADROOM_SLOTS" \
      -gt "$MAX_REPLAY_RESUME_HEADROOM_SLOTS" ]
  then
    echo "BLOCKZILLA_RAW_REPLAY_RESUME_HEADROOM_SLOTS must be between 0 and 10000" >&2
    exit 2
  fi
  if [ "$MAX_GENERATION_BYTES" -le "$MAX_RECORD_BYTES" ]; then
    echo "BLOCKZILLA_RAW_MAX_GENERATION_BYTES must exceed BLOCKZILLA_RAW_MAX_RECORD_BYTES" >&2
    exit 2
  fi
  if [ "$GENERATION_BACKLOG_WARN_COUNT" -eq 0 ] \
    || [ "$GENERATION_UPLOAD_RETRY_SECS" -eq 0 ]
  then
    echo "generation backlog threshold and upload retry must be non-zero" >&2
    exit 2
  fi
  if [ "$GENERATION_SPILL_START_PERCENT" -eq 0 ] \
    || [ "$GENERATION_SPILL_START_PERCENT" -ge 100 ] \
    || [ "$GENERATION_SPILL_RECOVERY_PERCENT" \
      -le "$GENERATION_SPILL_START_PERCENT" ] \
    || [ "$GENERATION_SPILL_RECOVERY_PERCENT" -gt 100 ]
  then
    echo "Backblaze disk-spill percentages are invalid" >&2
    exit 2
  fi
  if ! generation_spill_thresholds 107374182400; then
    echo "Backblaze disk-spill reserve arithmetic is invalid" >&2
    exit 2
  fi
  if [ "$GENERATION_RECEIPT_DIR" != /data/grpc-cache/receipts ]; then
    echo "BLOCKZILLA_RAW_CACHE_RECEIPT_DIR must be /data/grpc-cache/receipts" >&2
    exit 2
  fi
  if ! safe_replay_identity; then
    echo "cache upload cluster, origin, and source IDs must be non-empty and use safe path characters" >&2
    exit 2
  fi
  if ! normalized_generation_base_prefix >/dev/null; then
    echo "BLOCKZILLA_B2_REMOTE_PREFIX is not a safe relative object prefix" >&2
    exit 2
  fi
  if ! command -v "$GENERATION_PYTHON_BIN" >/dev/null 2>&1; then
    echo "generation receipt validator is missing" >&2
    exit 2
  fi
fi
if [ "$DISK_WARN_FREE_BYTES" -le "$MIN_FREE_BYTES" ]; then
  echo "BLOCKZILLA_RAW_DISK_WARN_FREE_BYTES must exceed BLOCKZILLA_RAW_MIN_FREE_BYTES" >&2
  exit 2
fi
if [ "$DISK_RECOVERY_HYSTERESIS_BYTES" -eq 0 ]; then
  echo "BLOCKZILLA_RAW_DISK_RECOVERY_HYSTERESIS_BYTES must be non-zero" >&2
  exit 2
fi
DISK_CRITICAL_RECOVERY_BYTES=$((MIN_FREE_BYTES + DISK_RECOVERY_HYSTERESIS_BYTES))
DISK_WARNING_RECOVERY_BYTES=$((DISK_WARN_FREE_BYTES + DISK_RECOVERY_HYSTERESIS_BYTES))
if [ "$DISK_CRITICAL_RECOVERY_BYTES" -le "$MIN_FREE_BYTES" ] \
  || [ "$DISK_WARNING_RECOVERY_BYTES" -le "$DISK_WARN_FREE_BYTES" ]
then
  echo "disk recovery threshold overflow" >&2
  exit 2
fi
if [ -n "$PRIMARY_SYNC_HEARTBEAT_FILE" ]; then
  case "$PRIMARY_SYNC_HEARTBEAT_FILE" in
    /data/*) ;;
    *)
      echo "BLOCKZILLA_PRIMARY_SYNC_HEARTBEAT_FILE must be a child of /data" >&2
      exit 2
      ;;
  esac
  case "$PRIMARY_SYNC_HEARTBEAT_FILE/" in
    */../*|*/./*)
      echo "BLOCKZILLA_PRIMARY_SYNC_HEARTBEAT_FILE must not contain . or .. path components" >&2
      exit 2
      ;;
  esac
fi
compression_level_digits=$COMPRESSION_LEVEL
case "$compression_level_digits" in
  -*) compression_level_digits=${compression_level_digits#-} ;;
esac
case "$compression_level_digits" in
  ''|*[!0-9]*)
    echo "invalid integer in BLOCKZILLA_RAW_COMPRESSION_LEVEL" >&2
    exit 2
    ;;
esac

if [ ! -x "$BIN" ]; then
  echo "missing executable: $BIN" >&2
  exit 2
fi
if [ "$(uname -m)" = x86_64 ] \
  && ! grep -qE '(^|[[:space:]])aes([[:space:]]|$)' /proc/cpuinfo
then
  echo "x86_64 host does not expose the AES CPU feature required by gxhash" >&2
  exit 2
fi

mkdir -p "$STATE_DIR"
if [ "$TELEGRAM_ENABLED" = true ]; then
  if ! mkdir -p "$ALERT_STATE_DIR"; then
    echo "$(timestamp) telegram_alert state_directory_unavailable" >&2
  fi
fi
: > "$STARTED_FILE"
write_state starting

available_bytes() {
  available_kib=$(df -Pk "$OUTPUT_DIR" | awk 'NR == 2 { print $4 }')
  case "$available_kib" in
    ''|*[!0-9]*) return 1 ;;
  esac
  printf '%s\n' "$((available_kib * 1024))"
}

filesystem_capacity_bytes() {
  capacity_kib=$(df -Pk "$OUTPUT_DIR" | awk 'NR == 2 { print $2 }')
  case "$capacity_kib" in
    ''|*[!0-9]*|0) return 1 ;;
  esac
  capacity_bytes=$((capacity_kib * 1024))
  [ "$capacity_bytes" -gt "$capacity_kib" ] || return 1
  printf '%s\n' "$capacity_bytes"
}

child_pid=
monitor_pid=
upload_worker_pid=
b2_usage_worker_pid=
REPLAY_VERIFIED_ANCHOR_SLOT=
REPLAY_SKIP_RETIRE_VERIFY_ONCE=false
REPLAY_PERSIST_FAILURE_STAGE=not_attempted
terminate() {
  if [ -n "$b2_usage_worker_pid" ]; then
    kill -TERM "$b2_usage_worker_pid" 2>/dev/null || true
  fi
  if [ -n "$upload_worker_pid" ]; then
    kill -TERM "$upload_worker_pid" 2>/dev/null || true
  fi
  if [ -n "$monitor_pid" ]; then
    kill -TERM "$monitor_pid" 2>/dev/null || true
  fi
  if [ -n "$child_pid" ]; then
    kill -TERM "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  if [ -n "$monitor_pid" ]; then
    wait "$monitor_pid" 2>/dev/null || true
  fi
  if [ -n "$upload_worker_pid" ]; then
    wait "$upload_worker_pid" 2>/dev/null || true
  fi
  if [ -n "$b2_usage_worker_pid" ]; then
    wait "$b2_usage_worker_pid" 2>/dev/null || true
  fi
  write_state stopping
  exit 0
}
trap terminate INT TERM HUP

while :; do
  REPLAY_MIN_RESUME_SLOT=
  monitor_primary_sync_alert
  if ! validate_data_volume; then
    write_state volume_invalid
    raise_alert volume_invalid CRITICAL \
      "Status: The Hetzner backup disk is unavailable. Backup is paused.
Data: Nothing was deleted.
Action: Check the Hetzner volume mount."
    echo "$(timestamp) raw_recorder paused_invalid_volume output=$OUTPUT_DIR" >&2
    sleep "$LOW_DISK_RECHECK_SECS"
    continue
  fi
  if [ "$CACHE_MODE" = b2-generations ]; then
    if ! prepare_cache_layout; then
      write_state cache_rotation_failed
      raise_alert generation_rotation_failed CRITICAL \
        "Status: The local backup file could not rotate. Backup is paused.
Data: Everything already saved is safe.
Action: Check the recorder logs. Automatic retry is on."
      echo "$(timestamp) raw_recorder paused_cache_recovery_failed" >&2
      sleep "$LOW_DISK_RECHECK_SECS"
      continue
    fi
    clear_alert generation_rotation_failed \
      "Status: Local backup files are rotating normally again.
Data: Backup can continue.
Action: None."
    if ! load_replay_recovery_floor; then
      write_state replay_recovery_failed
      raise_alert replay_recovery_failed CRITICAL \
        "Status: gRPC recovery check failed. Backup is paused.
Data: Local and Backblaze data are safe.
Action: Automatic retry is on."
      echo "$(timestamp) raw_recorder paused_replay_recovery_marker_invalid" >&2
      sleep "$LOW_DISK_RECHECK_SECS"
      continue
    fi
    replay_floor_was_authoritative=false
    if [ -n "$REPLAY_MIN_RESUME_SLOT" ]; then
      replay_floor_was_authoritative=true
    fi
    replay_floor_ready=true
    if [ "$REPLAY_SKIP_RETIRE_VERIFY_ONCE" = true ]; then
      REPLAY_SKIP_RETIRE_VERIFY_ONCE=false
      if [ -z "$REPLAY_MIN_RESUME_SLOT" ] \
        || [ "$REPLAY_VERIFIED_ANCHOR_SLOT" != "$REPLAY_RECOVERY_ANCHOR_SLOT" ]
      then
        replay_floor_ready=false
      fi
    elif ! retire_replay_recovery_floor_if_advanced; then
      replay_floor_ready=false
    fi
    if [ "$replay_floor_ready" != true ]; then
      write_state replay_recovery_failed
      raise_alert replay_recovery_failed CRITICAL \
        "Status: A local backup file could not be checked. Backup is paused.
Data: Nothing was deleted.
Action: Automatic retry is on."
      echo "$(timestamp) raw_recorder paused_replay_floor_retirement_failed" >&2
      sleep "$LOW_DISK_RECHECK_SECS"
      continue
    fi
    # Do not announce recovery merely because a retry starts. The incident is
    # resolved only after its trusted floor marker exists durably.
    clear_replay_recovery_alert_if_floor_was_authoritative \
      "$replay_floor_was_authoritative"
    if [ -n "$REPLAY_MIN_RESUME_SLOT" ]; then
      remember_alert_journal_floor provider_replay_gap
      if [ -n "$REPLAY_PROVIDER_AVAILABLE_SLOT" ]; then
        replay_cushion_detail=
        if [ "$REPLAY_MIN_RESUME_SLOT" -gt "$REPLAY_PROVIDER_AVAILABLE_SLOT" ]; then
          replay_cushion_detail="Reconnect policy: slots $REPLAY_PROVIDER_AVAILABLE_SLOT through $((REPLAY_MIN_RESUME_SLOT - 1)) were deliberately bypassed so the next handshake can outrun the moving replay floor. The provider did not report this cushion as unavailable, and the choice is retained in the audit record."
        fi
        raise_alert provider_replay_gap WARNING \
          "Status: This backup is missing slots $((REPLAY_RECOVERY_ANCHOR_SLOT + 1))-$((REPLAY_MIN_RESUME_SLOT - 1)).
Data: Earlier saved blocks are safe; this range is not in the backup.
Action: Backup will continue at slot $REPLAY_MIN_RESUME_SLOT. Repair the missing range if needed."
      else
        raise_alert provider_replay_gap WARNING \
          "Status: This backup is missing slots $((REPLAY_RECOVERY_ANCHOR_SLOT + 1))-$((REPLAY_MIN_RESUME_SLOT - 1)).
Data: Earlier saved blocks are safe; this range is not in the backup.
Action: Backup will continue at slot $REPLAY_MIN_RESUME_SLOT. Repair the missing range if needed."
      fi
    fi
    if [ -z "$upload_worker_pid" ] \
      || ! kill -0 "$upload_worker_pid" 2>/dev/null
    then
      if [ -n "$upload_worker_pid" ]; then
        wait "$upload_worker_pid" 2>/dev/null || true
      fi
      start_generation_upload_worker
    fi
    if [ "$B2_USAGE_ALERT_ENABLED" = true ] \
      && { [ -z "$b2_usage_worker_pid" ] \
        || ! kill -0 "$b2_usage_worker_pid" 2>/dev/null; }
    then
      if [ -n "$b2_usage_worker_pid" ]; then
        wait "$b2_usage_worker_pid" 2>/dev/null || true
      fi
      start_b2_usage_worker
    fi
  elif ! mkdir -p "$OUTPUT_DIR"; then
    write_state volume_invalid
    raise_alert volume_invalid CRITICAL \
      "Status: The Hetzner backup folder is unavailable. Backup is paused.
Data: Nothing was deleted.
Action: Check the Hetzner volume mount."
    echo "$(timestamp) raw_recorder paused_invalid_output output=$OUTPUT_DIR" >&2
    sleep "$LOW_DISK_RECHECK_SECS"
    continue
  fi
  if ! validate_data_volume true; then
    write_state volume_invalid
    raise_alert volume_invalid CRITICAL \
      "Status: The Hetzner backup folder is unavailable. Backup is paused.
Data: Nothing was deleted.
Action: Check the Hetzner volume mount."
    echo "$(timestamp) raw_recorder paused_invalid_output output=$OUTPUT_DIR" >&2
    sleep "$LOW_DISK_RECHECK_SECS"
    continue
  fi
  clear_alert volume_invalid \
    "Status: The Hetzner backup disk is available again.
Data: Backup can continue.
Action: None."
  # A durable gap event must keep retrying even when disk admission prevents a
  # recorder child (and its background monitor) from starting.
  monitor_resume_coverage_alert
  if ! free_bytes=$(available_bytes); then
    write_state disk_check_failed
    raise_alert disk_check_failed ERROR \
      "Status: Hetzner free space is unavailable. Backup safety cannot be confirmed.
Storage: Unknown.
Action: Check the Hetzner disk."
    echo "$(timestamp) raw_recorder disk_check_failed output=$OUTPUT_DIR" >&2
    sleep "$RESTART_DELAY_SECS"
    continue
  fi
  clear_alert disk_check_failed \
    "Status: Hetzner disk checks are working again.
Storage: $(human_decimal_bytes "$free_bytes") free.
Action: None."
  update_disk_alerts "$free_bytes"
  if [ "$free_bytes" -lt "$MIN_FREE_BYTES" ]; then
    write_state low_disk
    echo "$(timestamp) raw_recorder paused_low_disk available_bytes=$free_bytes reserve_bytes=$MIN_FREE_BYTES" >&2
    sleep "$LOW_DISK_RECHECK_SECS"
    continue
  fi

  set -- \
    record-grpc-raw \
    --endpoint "$BLOCKZILLA_GRPC_ENDPOINT" \
    --output-dir "$OUTPUT_DIR" \
    --max-blocks "$MAX_BLOCKS" \
    --timeout-secs "$TIMEOUT_SECS" \
    --idle-timeout-secs "$IDLE_TIMEOUT_SECS" \
    --compression-level "$COMPRESSION_LEVEL" \
    --segment-target-bytes "$SEGMENT_TARGET_BYTES" \
    --max-record-bytes "$MAX_RECORD_BYTES" \
    --min-free-bytes "$MIN_FREE_BYTES" \
    --cluster-id "$CLUSTER_ID" \
    --origin-node-id "$ORIGIN_NODE_ID" \
    --source-id "$SOURCE_ID"
  if [ "$REQUIRE_COMPLETE_POH" = true ]; then
    set -- "$@" --require-complete-poh
  fi
  if [ "$CACHE_MODE" = b2-generations ]; then
    set -- "$@" \
      --max-generation-bytes "$MAX_GENERATION_BYTES" \
      --hot-generation-root "$CACHE_ROOT"
  fi
  if [ -n "$INITIAL_FROM_SLOT" ]; then
    set -- "$@" --from-slot "$INITIAL_FROM_SLOT"
  fi
  if [ -n "$REPLAY_MIN_RESUME_SLOT" ]; then
    set -- "$@" --min-resume-slot "$REPLAY_MIN_RESUME_SLOT"
  fi
  set -- "$@" --resume-coverage-warning-file "$ACTIVE_RESUME_COVERAGE_EVENT_FILE"

  write_state running
  echo "$(timestamp) raw_recorder starting output=$OUTPUT_DIR free_bytes=$free_bytes" >&2
  : > "$CHILD_REPORT_FILE"
  "$BIN" "$@" > "$CHILD_REPORT_FILE" &
  child_pid=$!
  # The monitor owns the runtime volume guard as well as optional alerts, so it
  # must remain active when Telegram delivery is temporarily disabled.
  start_child_monitor "$child_pid"
  if wait "$child_pid"; then
    status=0
  else
    status=$?
  fi
  if [ -n "$monitor_pid" ]; then
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
    monitor_pid=
  fi
  child_pid=
  monitor_resume_coverage_alert

  exit_reason=process_error
  if [ "$status" -eq 0 ]; then
    if grep -q '"replay_unavailable": true' "$CHILD_REPORT_FILE"; then
      exit_reason=provider_replay_unavailable
    elif grep -q '"stopped_generation_full": true' "$CHILD_REPORT_FILE"; then
      exit_reason=generation_byte_limit
    elif grep -q '"idle_timed_out": true' "$CHILD_REPORT_FILE"; then
      exit_reason=durable_block_idle_timeout
    elif grep -q '"stream_ended": true' "$CHILD_REPORT_FILE"; then
      exit_reason=grpc_stream_ended
    elif grep -q '"timed_out": true' "$CHILD_REPORT_FILE"; then
      exit_reason=total_timeout
    elif grep -q '"stopped_low_disk": true' "$CHILD_REPORT_FILE"; then
      exit_reason=low_disk_floor
    elif grep -q '"stopped_at_epoch_boundary": true' "$CHILD_REPORT_FILE"; then
      exit_reason=epoch_boundary
    else
      exit_reason=clean_exit
    fi
  fi
  last_slot=$(sed -n 's/^[[:space:]]*"last_slot": \([0-9][0-9]*\),*$/\1/p' "$CHILD_REPORT_FILE" | tail -n 1)
  [ -n "$last_slot" ] || last_slot=unknown
  if [ -s "$JOURNAL_FILE" ]; then
    journal_age=$(file_age_seconds "$JOURNAL_FILE" 2>/dev/null || printf '%s\n' unknown)
  else
    journal_age=unknown
  fi
  if grep -q '"resume_coverage_warning_publication_failed": true' \
    "$CHILD_REPORT_FILE"
  then
    raise_alert resume_coverage WARNING \
      "Status: The provider did not replay the saved slot, and the warning report could not be saved. Backup is paused.
Data: Everything saved earlier is safe.
Action: Check the Hetzner monitoring folder."
  fi
  if [ "$CACHE_MODE" = b2-generations ] \
    && [ "$status" -eq 0 ] \
    && [ "$exit_reason" = provider_replay_unavailable ]
  then
    if [ -s "$CHILD_REPORT_FILE" ]; then
      sed -n '1,200p' "$CHILD_REPORT_FILE"
    fi
    if persist_replay_recovery_from_report "$CHILD_REPORT_FILE"; then
      clear_alert replay_recovery_failed \
        "Status: gRPC recovery is ready. Backup will restart at slot $REPLAY_GAP_SELECTED_RESUME_SLOT.
Data: Everything saved earlier is safe.
Action: None."
      remember_alert_journal_floor provider_replay_gap
      replay_cushion_detail=
      if [ "$REPLAY_GAP_SELECTED_RESUME_SLOT" -gt "$REPLAY_GAP_PROVIDER_AVAILABLE_SLOT" ]; then
        replay_cushion_detail="Reconnect policy: slots $REPLAY_GAP_PROVIDER_AVAILABLE_SLOT through $((REPLAY_GAP_SELECTED_RESUME_SLOT - 1)) were deliberately bypassed so the next handshake can outrun the moving replay floor. The provider did not report this cushion as unavailable, and the choice is retained in the audit record."
      fi
      raise_alert provider_replay_gap WARNING \
        "Status: This backup is missing slots $((REPLAY_GAP_ANCHOR_SLOT + 1))-$((REPLAY_GAP_SELECTED_RESUME_SLOT - 1)).
Data: Earlier saved blocks are safe; this range is not in the backup.
Action: Backup will continue at slot $REPLAY_GAP_SELECTED_RESUME_SLOT. Repair the missing range if needed."
      write_state running
      echo "$(timestamp) raw_recorder replay_gap_recorded anchor_slot=$REPLAY_GAP_ANCHOR_SLOT requested_slot=$REPLAY_GAP_REQUESTED_SLOT provider_floor=$REPLAY_GAP_PROVIDER_AVAILABLE_SLOT selected_resume_slot=$REPLAY_GAP_SELECTED_RESUME_SLOT headroom_slots=$REPLAY_RESUME_HEADROOM_SLOTS; resuming capture" >&2
      continue
    fi
    write_state replay_recovery_failed
    raise_alert replay_recovery_failed CRITICAL \
      "Status: The gRPC provider no longer has the requested old slots. Backup is paused.
Data: Local and Backblaze data are safe.
Action: Automatic recovery is retrying."
    echo "$(timestamp) raw_recorder replay_recovery_persist_failed stage=${REPLAY_PERSIST_FAILURE_STAGE:-unknown}; retrying in ${RESTART_DELAY_SECS}s" >&2
    sleep "$RESTART_DELAY_SECS"
    continue
  fi
  if [ "$CACHE_MODE" = b2-generations ] \
    && [ "$status" -eq 0 ] \
    && [ "$exit_reason" = generation_byte_limit ]
  then
    if [ -s "$CHILD_REPORT_FILE" ]; then
      sed -n '1,200p' "$CHILD_REPORT_FILE"
    fi
    if rotate_active_generation; then
      clear_alert generation_rotation_failed \
        "Status: Local backup files are rotating normally again.
Data: Backup can continue.
Action: None."
      write_state running
      echo "$(timestamp) raw_recorder generation_rotated; resuming capture" >&2
      continue
    fi
    write_state cache_rotation_failed
    raise_alert generation_rotation_failed CRITICAL \
      "Status: The local backup file could not rotate. Backup is paused.
Data: Everything already saved is safe.
Action: Check the recorder logs. Automatic retry is on."
    echo "$(timestamp) raw_recorder generation_rotation_failed; retrying in ${RESTART_DELAY_SECS}s" >&2
    sleep "$RESTART_DELAY_SECS"
    continue
  fi
  if ! validate_data_volume true >/dev/null 2>&1; then
    raise_alert volume_invalid CRITICAL \
      "Status: The Hetzner backup disk is unavailable. Backup is paused.
Data: Nothing was deleted.
Action: Check the Hetzner volume mount."
  elif [ "$exit_reason" = low_disk_floor ]; then
    if current_free_bytes=$(available_bytes) && [ "$current_free_bytes" -lt "$MIN_FREE_BYTES" ]; then
      # The disk monitor and child-exit path share the same incident key. A
      # single low-space stop must not fan out into separate disk and pipeline
      # notifications.
      update_disk_alerts "$current_free_bytes"
    else
      raise_recorder_restart_alert \
        "Recorder exited after a low-disk stop; status=$status last_slot=$last_slot journal_age_seconds=$journal_age."
    fi
  elif [ "$exit_reason" = durable_block_idle_timeout ] \
    && [ -e "$(alert_file grpc_stale active)" ]
  then
    : # The stale-journal incident already owns the alert and recovery transition.
  else
    raise_recorder_restart_alert \
      "Recorder exited; status=$status reason=$exit_reason last_slot=$last_slot journal_age_seconds=$journal_age. Restarting after ${RESTART_DELAY_SECS}s."
  fi
  if [ -s "$CHILD_REPORT_FILE" ]; then
    sed -n '1,200p' "$CHILD_REPORT_FILE"
  fi
  write_state backoff
  echo "$(timestamp) raw_recorder exited status=$status reason=$exit_reason; retrying in ${RESTART_DELAY_SECS}s" >&2
  sleep "$RESTART_DELAY_SECS"
done
