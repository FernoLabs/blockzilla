#!/bin/sh
set -eu

# File-backed Compose secrets retain their host ownership and mode. Copy only
# the TLS and receipt-signing private keys into this container's private tmpfs
# so the receiver always consumes UID-only runtime copies with mode 0600.

umask 077
export LC_ALL=C

CONFIG_FILE=${BLOCKZILLA_RECEIVER_CONFIG_FILE:-/run/secrets/ingest_primary_config}
HIVEZILLA_BIN=${HIVEZILLA_BIN:-/usr/local/bin/hivezilla}
SOURCE_SERVER_PRIVATE_KEY=${BLOCKZILLA_RECEIVER_SERVER_PRIVATE_KEY_SOURCE:-/run/secrets/blockzilla_primary_private_key}
SOURCE_RECEIPT_SIGNING_KEY=${BLOCKZILLA_RECEIVER_RECEIPT_SIGNING_KEY_SOURCE:-/run/secrets/blockzilla_receipt_signing_key}
RUNTIME_PRIVATE_DIRECTORY=${BLOCKZILLA_RECEIVER_RUNTIME_PRIVATE_DIRECTORY:-/tmp/blockzilla-receiver}
RUNTIME_SERVER_PRIVATE_KEY=${RUNTIME_PRIVATE_DIRECTORY}/server-private-key.pem
RUNTIME_RECEIPT_SIGNING_KEY=${RUNTIME_PRIVATE_DIRECTORY}/receipt-signing-key.pem
CONTROL_DIRECTORY=${BLOCKZILLA_RECEIVER_CONTROL_DIRECTORY:-/data/replication-control}

die() {
  printf '%s\n' "raw-receiver: $*" >&2
  exit 1
}

require_regular_readable_file() {
  label=$1
  path=$2
  [ -f "$path" ] && [ ! -L "$path" ] && [ -r "$path" ] || \
    die "$label must be a readable regular non-symlink file"
}

[ "$(id -u)" = 10001 ] || die "must run as dedicated UID 10001"
require_regular_readable_file "receiver config" "$CONFIG_FILE"
require_regular_readable_file "server private key" "$SOURCE_SERVER_PRIVATE_KEY"
require_regular_readable_file "receipt signing key" "$SOURCE_RECEIPT_SIGNING_KEY"
[ -d /data ] && [ ! -L /data ] && [ -w /data ] || \
  die "/data must be a writable non-symlink directory"

mkdir -p "$CONTROL_DIRECTORY"
[ -d "$CONTROL_DIRECTORY" ] && [ ! -L "$CONTROL_DIRECTORY" ] || \
  die "replication control directory is unsafe"
chmod 0700 "$CONTROL_DIRECTORY"

mkdir -p "$RUNTIME_PRIVATE_DIRECTORY"
[ -d "$RUNTIME_PRIVATE_DIRECTORY" ] && [ ! -L "$RUNTIME_PRIVATE_DIRECTORY" ] || \
  die "runtime private-key directory is unsafe"
chmod 0700 "$RUNTIME_PRIVATE_DIRECTORY"

rm -f "$RUNTIME_SERVER_PRIVATE_KEY" "$RUNTIME_RECEIPT_SIGNING_KEY"
install -m 0600 "$SOURCE_SERVER_PRIVATE_KEY" "$RUNTIME_SERVER_PRIVATE_KEY"
install -m 0600 "$SOURCE_RECEIPT_SIGNING_KEY" "$RUNTIME_RECEIPT_SIGNING_KEY"

exec "$HIVEZILLA_BIN" serve-ingest-receiver \
  --config "$CONFIG_FILE"
