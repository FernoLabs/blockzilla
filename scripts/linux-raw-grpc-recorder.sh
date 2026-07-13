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
ALERT_STATE_DIR=$STATE_DIR/blockzilla-raw-alerts
CHILD_REPORT_FILE=$STATE_DIR/blockzilla-raw-recorder.child.json
RESUME_COVERAGE_EVENT_DIR=$OUTPUT_DIR/.monitoring
RESUME_COVERAGE_EVENT_FILE=$RESUME_COVERAGE_EVENT_DIR/resume-coverage-warning.json
RESUME_COVERAGE_DELIVERED_FILE=$RESUME_COVERAGE_EVENT_DIR/resume-coverage-warning.delivered
ACTIVE_RESUME_COVERAGE_EVENT_FILE=$RESUME_COVERAGE_EVENT_FILE

validate_data_paths() {
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
  IFS= read -r volume_expected_device < "$VOLUME_MARKER" || return 1
  case "$volume_expected_device" in
    ''|*[!0-9]*)
      echo "raw recorder volume marker does not contain a filesystem device id" >&2
      return 1
      ;;
  esac
  if [ "$volume_expected_device" != "$volume_data_device" ]; then
    echo "raw recorder filesystem device differs from its fail-closed marker" >&2
    return 1
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

raise_alert() {
  alert_key=$1
  alert_level=$2
  alert_message=$3
  ALERT_DELIVERY_RESULT=disabled
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 0
  fi
  mkdir -p "$ALERT_STATE_DIR" 2>/dev/null || return 0
  alert_active=$(alert_file "$alert_key" active)
  alert_last=$(alert_file "$alert_key" last)
  alert_text=$(printf '%s BLOCKZILLA %s\nnode=%s source=%s utc=%s\n%s' \
    "$alert_level" "$alert_key" "$ORIGIN_NODE_ID" "$SOURCE_ID" "$(timestamp)" "$alert_message")
  if ! printf '%s\n' "$alert_text" > "$alert_active"; then
    ALERT_DELIVERY_RESULT=failed
    echo "$(timestamp) telegram_alert state_write_failed key=$alert_key" >&2
    return 0
  fi
  alert_now=$(date +%s)
  alert_previous=0
  if [ -r "$alert_last" ]; then
    IFS= read -r alert_previous < "$alert_last" || alert_previous=0
    case "$alert_previous" in
      ''|*[!0-9]*) alert_previous=0 ;;
    esac
  fi
  if [ "$alert_now" -ge "$alert_previous" ] \
    && [ $((alert_now - alert_previous)) -lt "$TELEGRAM_ALERT_COOLDOWN_SECS" ]
  then
    ALERT_DELIVERY_RESULT=suppressed
    return 0
  fi
  if ! telegram_send "$alert_text"; then
    ALERT_DELIVERY_RESULT=failed
    echo "$(timestamp) telegram_alert delivery_failed key=$alert_key" >&2
    return 0
  fi
  ALERT_DELIVERY_RESULT=sent
  write_alert_delivery_time "$alert_key" "$alert_last" "$alert_now" || true
  return 0
}

retry_pending_alert() {
  retry_key=$1
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 0
  fi
  retry_active=$(alert_file "$retry_key" active)
  retry_last=$(alert_file "$retry_key" last)
  if [ ! -e "$retry_active" ] || [ -r "$retry_last" ]; then
    return 0
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
  if ! telegram_send "$retry_text"; then
    echo "$(timestamp) telegram_alert delivery_failed key=$retry_key" >&2
    return 0
  fi
  retry_now=$(date +%s)
  write_alert_delivery_time "$retry_key" "$retry_last" "$retry_now" || true
  return 0
}

retry_pending_alerts() {
  for retry_key in \
    recorder_restarting \
    grpc_stale \
    volume_invalid \
    disk_check_failed \
    disk_critical \
    disk_warning \
    primary_sync_stale
  do
    retry_pending_alert "$retry_key"
  done
  return 0
}

clear_alert() {
  alert_key=$1
  alert_message=$2
  if [ "$TELEGRAM_ENABLED" != true ]; then
    return 0
  fi
  alert_active=$(alert_file "$alert_key" active)
  alert_last=$(alert_file "$alert_key" last)
  if [ ! -e "$alert_active" ]; then
    return 0
  fi
  retry_pending_alert "$alert_key"
  if [ ! -r "$alert_last" ]; then
    return 0
  fi
  alert_text=$(printf 'RECOVERED BLOCKZILLA %s\nnode=%s source=%s utc=%s\n%s' \
    "$alert_key" "$ORIGIN_NODE_ID" "$SOURCE_ID" "$(timestamp)" "$alert_message")
  if ! telegram_send "$alert_text"; then
    echo "$(timestamp) telegram_recovery delivery_failed key=$alert_key" >&2
    return 0
  fi
  if ! rm -f "$alert_active" "$alert_last"; then
    echo "$(timestamp) telegram_recovery state_remove_failed key=$alert_key" >&2
  fi
  return 0
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

clear_alert_after_journal_growth() {
  growth_key=$1
  growth_message=$2
  growth_active=$(alert_file "$growth_key" active)
  growth_floor_file=$(alert_file "$growth_key" journal_size)
  if [ ! -e "$growth_active" ] || [ ! -r "$growth_floor_file" ]; then
    return 0
  fi
  IFS= read -r growth_floor < "$growth_floor_file" || return 0
  growth_size=$(journal_size 2>/dev/null || printf '%s\n' 0)
  case "$growth_floor" in
    ''|*[!0-9]*) return 0 ;;
  esac
  case "$growth_size" in
    ''|*[!0-9]*) return 0 ;;
  esac
  if [ "$growth_size" -gt "$growth_floor" ]; then
    clear_alert "$growth_key" "$growth_message"
    if [ ! -e "$growth_active" ]; then
      rm -f "$growth_floor_file" 2>/dev/null || \
        echo "$(timestamp) telegram_recovery journal_floor_remove_failed key=$growth_key" >&2
    fi
  fi
}

raise_recorder_restart_alert() {
  restart_message=$1
  remember_alert_journal_floor recorder_restarting
  raise_alert recorder_restarting ERROR "$restart_message"
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
    raise_alert disk_check_failed ERROR "Unable to read free space for the recorder volume."
    return 0
  fi
  clear_alert disk_check_failed "Filesystem free-space checks are working again."
  update_disk_alerts "$monitor_free_bytes"
}

update_disk_alerts() {
  disk_free_bytes=$1
  disk_critical_active=$(alert_file disk_critical active)
  disk_warning_active=$(alert_file disk_warning active)
  if [ "$disk_free_bytes" -lt "$MIN_FREE_BYTES" ]; then
    raise_alert disk_critical CRITICAL \
      "Free bytes=$disk_free_bytes, below hard floor=$MIN_FREE_BYTES; durable capture will pause."
  elif [ -e "$disk_critical_active" ] \
    && [ "$disk_free_bytes" -lt "$DISK_CRITICAL_RECOVERY_BYTES" ]
  then
    : # Keep the critical incident active until the recovery margin is reached.
  elif [ "$disk_free_bytes" -lt "$DISK_WARN_FREE_BYTES" ]; then
    clear_alert disk_critical \
      "Free space recovered above the hard floor plus its hysteresis margin."
    raise_alert disk_warning WARNING \
      "Free bytes=$disk_free_bytes, below warning threshold=$DISK_WARN_FREE_BYTES. No automatic WAL deletion is enabled."
  elif [ -e "$disk_warning_active" ] \
    && [ "$disk_free_bytes" -lt "$DISK_WARNING_RECOVERY_BYTES" ]
  then
    clear_alert disk_critical "Free space recovered above the critical threshold."
  else
    clear_alert disk_critical "Free space recovered above the critical threshold."
    clear_alert disk_warning \
      "Free space recovered above the warning threshold plus its hysteresis margin."
  fi
}

monitor_feed_alerts() {
  if [ -s "$JOURNAL_FILE" ]; then
    monitor_age=$(file_age_seconds "$JOURNAL_FILE") || return 0
    if [ "$monitor_age" -gt "$RAW_STALE_AFTER_SECS" ]; then
      remember_alert_journal_floor grpc_stale
      raise_alert grpc_stale ERROR \
        "No durable block append for ${monitor_age}s (limit=${RAW_STALE_AFTER_SECS}s)."
    else
      clear_alert_after_journal_growth grpc_stale \
        "A new durable raw-journal record was appended after the stale-feed incident."
    fi
    clear_alert_after_journal_growth recorder_restarting \
      "A new durable block was appended after restart."
  elif [ -e "$STARTED_FILE" ]; then
    monitor_start_age=$(file_age_seconds "$STARTED_FILE") || return 0
    if [ "$monitor_start_age" -gt "$STARTUP_GRACE_SECS" ]; then
      remember_alert_journal_floor grpc_stale
      raise_alert grpc_stale ERROR \
        "No durable raw journal was created within startup grace=${STARTUP_GRACE_SECS}s."
    fi
  fi

  monitor_primary_sync_alert
  monitor_resume_coverage_alert
}

monitor_resume_coverage_alert() {
  if [ "$TELEGRAM_ENABLED" != true ] \
    || [ -z "${ACTIVE_RESUME_COVERAGE_EVENT_FILE:-}" ]
  then
    return 0
  fi
  if ! resume_event_id=$(resume_coverage_event_id); then
    return 0
  fi
  delivered_event_id=
  if [ -r "$RESUME_COVERAGE_DELIVERED_FILE" ] \
    && [ ! -L "$RESUME_COVERAGE_DELIVERED_FILE" ]
  then
    IFS= read -r delivered_event_id < "$RESUME_COVERAGE_DELIVERED_FILE" || delivered_event_id=
  fi
  if [ "$delivered_event_id" = "$resume_event_id" ]; then
    remove_resume_coverage_event
    return 0
  fi
  raise_alert resume_coverage WARNING \
    "Yellowstone skipped the inclusively requested durable resume slot; audit and repair the uncovered range."
  if [ "${ALERT_DELIVERY_RESULT:-}" = sent ]; then
    if write_resume_coverage_delivery "$resume_event_id"; then
      remove_resume_coverage_event
    fi
  fi
}

resume_coverage_event_id() {
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
  parsed_event_id=$(sed -n \
    's/.*"event_id":"\([0-9a-f][0-9a-f]*\)".*/\1/p' \
    "$ACTIVE_RESUME_COVERAGE_EVENT_FILE")
  if [ "${#parsed_event_id}" -ne 64 ]; then
    return 1
  fi
  case "$parsed_event_id" in
    *[!0-9a-f]*) return 1 ;;
  esac
  printf '%s\n' "$parsed_event_id"
}

resume_coverage_event_valid() {
  resume_coverage_event_id >/dev/null 2>&1
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
        "Configured primary-sync heartbeat is missing or unreadable. The sync/ACK protocol is not deletion authority."
    fi
    return 0
  fi

  if ! : > "$heartbeat_seen_file"; then
    echo "$(timestamp) telegram_alert heartbeat_state_write_failed" >&2
  fi
  heartbeat_age=$(file_age_seconds "$PRIMARY_SYNC_HEARTBEAT_FILE") || heartbeat_age=$PRIMARY_SYNC_STALE_AFTER_SECS
  if [ "$heartbeat_age" -gt "$PRIMARY_SYNC_STALE_AFTER_SECS" ]; then
    raise_alert primary_sync_stale WARNING \
      "Primary-sync heartbeat age=${heartbeat_age}s exceeds limit=${PRIMARY_SYNC_STALE_AFTER_SECS}s."
  else
    clear_alert primary_sync_stale "The primary-sync heartbeat is fresh again."
  fi
}

monitor_child() {
  monitored_pid=$1
  trap 'exit 0' INT TERM HUP
  while kill -0 "$monitored_pid" 2>/dev/null; do
    sleep "$MONITOR_INTERVAL_SECS"
    if ! kill -0 "$monitored_pid" 2>/dev/null; then
      break
    fi
    if validate_data_volume true >/dev/null 2>&1; then
      retry_pending_alerts
      clear_alert volume_invalid "The dedicated recorder volume is valid again."
      monitor_disk_alerts
      monitor_feed_alerts
    else
      kill -TERM "$monitored_pid" 2>/dev/null || true
      raise_alert volume_invalid CRITICAL \
        "The dedicated recorder volume or fail-closed marker became invalid; stopping capture."
      break
    fi
  done
}

start_child_monitor() {
  monitor_child "$1" &
  monitor_pid=$!
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
  test_text=$(printf 'TEST BLOCKZILLA telegram\nnode=%s source=%s utc=%s\nOutbound alert delivery is working.' \
    "$ORIGIN_NODE_ID" "$SOURCE_ID" "$(timestamp)")
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

for numeric_setting in \
  "BLOCKZILLA_RAW_MIN_FREE_BYTES:$MIN_FREE_BYTES" \
  "BLOCKZILLA_RAW_MAX_BLOCKS:$MAX_BLOCKS" \
  "BLOCKZILLA_RAW_TIMEOUT_SECS:$TIMEOUT_SECS" \
  "BLOCKZILLA_RAW_IDLE_TIMEOUT_SECS:$IDLE_TIMEOUT_SECS" \
  "BLOCKZILLA_RAW_RESTART_DELAY_SECS:$RESTART_DELAY_SECS" \
  "BLOCKZILLA_RAW_LOW_DISK_RECHECK_SECS:$LOW_DISK_RECHECK_SECS" \
  "BLOCKZILLA_RAW_SEGMENT_TARGET_BYTES:$SEGMENT_TARGET_BYTES" \
  "BLOCKZILLA_RAW_MAX_RECORD_BYTES:$MAX_RECORD_BYTES" \
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

child_pid=
monitor_pid=
terminate() {
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
  write_state stopping
  exit 0
}
trap terminate INT TERM HUP

while :; do
  monitor_primary_sync_alert
  if ! validate_data_volume; then
    write_state volume_invalid
    raise_alert volume_invalid CRITICAL \
      "The dedicated recorder volume or fail-closed marker is invalid; capture is paused."
    echo "$(timestamp) raw_recorder paused_invalid_volume output=$OUTPUT_DIR" >&2
    sleep "$LOW_DISK_RECHECK_SECS"
    continue
  fi
  if ! mkdir -p "$OUTPUT_DIR" || ! validate_data_volume true; then
    write_state volume_invalid
    raise_alert volume_invalid CRITICAL \
      "The recorder output directory could not be created safely on the dedicated volume; capture is paused."
    echo "$(timestamp) raw_recorder paused_invalid_output output=$OUTPUT_DIR" >&2
    sleep "$LOW_DISK_RECHECK_SECS"
    continue
  fi
  clear_alert volume_invalid "The dedicated recorder volume is valid again."
  # A durable gap event must keep retrying even when disk admission prevents a
  # recorder child (and its background monitor) from starting.
  monitor_resume_coverage_alert
  if ! free_bytes=$(available_bytes); then
    write_state disk_check_failed
    raise_alert disk_check_failed ERROR "Unable to read free space for the recorder volume."
    echo "$(timestamp) raw_recorder disk_check_failed output=$OUTPUT_DIR" >&2
    sleep "$RESTART_DELAY_SECS"
    continue
  fi
  clear_alert disk_check_failed "Filesystem free-space checks are working again."
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
  if [ -n "$INITIAL_FROM_SLOT" ]; then
    set -- "$@" --from-slot "$INITIAL_FROM_SLOT"
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
    if grep -q '"idle_timed_out": true' "$CHILD_REPORT_FILE"; then
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
  if grep -q '"resume_coverage_warning": true' "$CHILD_REPORT_FILE" \
    && ! resume_coverage_event_valid
  then
    raise_alert resume_coverage WARNING \
      "The provider did not deliver the inclusive resume slot; audit slot coverage before relying on this backup."
  fi
  if ! validate_data_volume true >/dev/null 2>&1; then
    raise_alert volume_invalid CRITICAL \
      "The dedicated recorder volume or fail-closed marker is invalid; capture is paused."
  elif [ "$exit_reason" = low_disk_floor ]; then
    if current_free_bytes=$(available_bytes) && [ "$current_free_bytes" -lt "$MIN_FREE_BYTES" ]; then
      raise_alert disk_critical CRITICAL \
        "Recorder stopped at the disk floor. Free bytes=$current_free_bytes, floor=$MIN_FREE_BYTES."
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
