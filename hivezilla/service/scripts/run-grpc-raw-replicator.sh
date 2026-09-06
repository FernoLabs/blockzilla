#!/bin/sh
set -eu

# File-backed Compose secrets retain their host ownership and mode. Copy only
# the client private key into this container's private tmpfs so the replication
# client always consumes a UID-only runtime copy with mode 0600.

umask 077
export LC_ALL=C

CONFIG_FILE=${BLOCKZILLA_REPLICATION_CONFIG_FILE:-/run/secrets/ingest_replica_config}
HIVEZILLA_BIN=${HIVEZILLA_BIN:-/usr/local/bin/hivezilla}
CACHE_ROOT=${BLOCKZILLA_REPLICATION_CACHE_ROOT:-/data/grpc-cache}
SOURCE_PRIVATE_KEY=${BLOCKZILLA_REPLICATION_PRIVATE_KEY_SOURCE:-/run/secrets/source_replica_private_key}
RUNTIME_PRIVATE_DIRECTORY=${BLOCKZILLA_REPLICATION_RUNTIME_PRIVATE_DIRECTORY:-/tmp/blockzilla-replicator}
RUNTIME_PRIVATE_KEY=${RUNTIME_PRIVATE_DIRECTORY}/client-private-key.pem
POLL_INTERVAL_MS=${BLOCKZILLA_REPLICATION_POLL_INTERVAL_MS:-1000}
ACK_STATUS_FILE=${BLOCKZILLA_REPLICATION_ACK_STATUS_FILE:-/data/replication-control/receiver-ack-status.json}

die() {
  printf '%s\n' "raw-replicator: $*" >&2
  exit 1
}

require_regular_readable_file() {
  label=$1
  path=$2
  [ -f "$path" ] && [ ! -L "$path" ] && [ -r "$path" ] || \
    die "$label must be a readable regular non-symlink file"
}

[ "$(id -u)" = 10001 ] || die "must run as dedicated UID 10001"
case "$POLL_INTERVAL_MS" in
  ''|*[!0-9]*) die "BLOCKZILLA_REPLICATION_POLL_INTERVAL_MS must be a positive integer" ;;
esac
[ "$POLL_INTERVAL_MS" -gt 0 ] || \
  die "BLOCKZILLA_REPLICATION_POLL_INTERVAL_MS must be greater than zero"

require_regular_readable_file "replication config" "$CONFIG_FILE"
require_regular_readable_file "replication client private key" "$SOURCE_PRIVATE_KEY"
[ -d "$CACHE_ROOT" ] && [ ! -L "$CACHE_ROOT" ] || \
  die "replication cache root must be a non-symlink directory"

status_directory=${ACK_STATUS_FILE%/*}
mkdir -p "$status_directory"
[ -d "$status_directory" ] && [ ! -L "$status_directory" ] && [ -w "$status_directory" ] || \
  die "replication ACK status directory must be writable and non-symlinked"

mkdir -p "$RUNTIME_PRIVATE_DIRECTORY"
[ -d "$RUNTIME_PRIVATE_DIRECTORY" ] && [ ! -L "$RUNTIME_PRIVATE_DIRECTORY" ] || \
  die "runtime private-key directory is unsafe"
chmod 0700 "$RUNTIME_PRIVATE_DIRECTORY"
rm -f "$RUNTIME_PRIVATE_KEY"
install -m 0600 "$SOURCE_PRIVATE_KEY" "$RUNTIME_PRIVATE_KEY"

exec "$HIVEZILLA_BIN" replicate-grpc-raw \
  --config "$CONFIG_FILE" \
  --cache-root "$CACHE_ROOT" \
  --ack-status-file "$ACK_STATUS_FILE" \
  --poll-interval-ms "$POLL_INTERVAL_MS"
