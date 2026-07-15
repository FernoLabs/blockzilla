#!/bin/sh
set -eu

# Docker Compose file secrets are normally mounted read-only with mode 0444.
# The replication client deliberately rejects a group/world-readable private
# key, so copy only that key into this container's private tmpfs before exec.

umask 077
export LC_ALL=C

CONFIG_FILE=${BLOCKZILLA_REPLICATION_CONFIG_FILE:-/run/secrets/ingest_replica_config}
CACHE_ROOT=${BLOCKZILLA_REPLICATION_CACHE_ROOT:-/data/grpc-cache}
SOURCE_PRIVATE_KEY=${BLOCKZILLA_REPLICATION_PRIVATE_KEY_SOURCE:-/run/secrets/hetzner_replica_private_key}
RUNTIME_PRIVATE_KEY=/tmp/blockzilla-replicator/client-private-key.pem
POLL_INTERVAL_MS=${BLOCKZILLA_REPLICATION_POLL_INTERVAL_MS:-1000}

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

case "$POLL_INTERVAL_MS" in
  ''|*[!0-9]*) die "BLOCKZILLA_REPLICATION_POLL_INTERVAL_MS must be a positive integer" ;;
esac
[ "$POLL_INTERVAL_MS" -gt 0 ] || \
  die "BLOCKZILLA_REPLICATION_POLL_INTERVAL_MS must be greater than zero"

require_regular_readable_file "replication config" "$CONFIG_FILE"
require_regular_readable_file "replication client private key" "$SOURCE_PRIVATE_KEY"
[ -d "$CACHE_ROOT" ] && [ ! -L "$CACHE_ROOT" ] || \
  die "replication cache root must be a non-symlink directory"

key_directory=${RUNTIME_PRIVATE_KEY%/*}
mkdir -p "$key_directory"
[ -d "$key_directory" ] && [ ! -L "$key_directory" ] || \
  die "runtime private-key directory is unsafe"
chmod 0700 "$key_directory"
rm -f "$RUNTIME_PRIVATE_KEY"
install -m 0600 "$SOURCE_PRIVATE_KEY" "$RUNTIME_PRIVATE_KEY"

exec /usr/local/bin/blockzilla-live-producer replicate-grpc-raw \
  --config "$CONFIG_FILE" \
  --cache-root "$CACHE_ROOT" \
  --poll-interval-ms "$POLL_INTERVAL_MS"
