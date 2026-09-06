#!/bin/sh
set -eu

# The outbound client uses one purpose-built mTLS identity for both the public
# source host source and the NAS-local durable receiver. Stage only its private key
# in tmpfs and leave all trust/config inputs read-only.

umask 077
export LC_ALL=C

CONFIG_FILE=${BLOCKZILLA_PULL_CLIENT_CONFIG_FILE:-/run/secrets/grpc_pull_client_config}
HIVEZILLA_BIN=${HIVEZILLA_BIN:-/usr/local/bin/hivezilla}
SOURCE_CLIENT_CERTIFICATE=${BLOCKZILLA_PULL_CLIENT_CERTIFICATE_FILE:-/run/secrets/blockzilla_pull_client_certificate}
SOURCE_CLIENT_PRIVATE_KEY=${BLOCKZILLA_PULL_CLIENT_PRIVATE_KEY_SOURCE:-/run/secrets/blockzilla_pull_client_private_key}
SOURCE_REPLICATION_CA=${BLOCKZILLA_PULL_CLIENT_CA_FILE:-/run/secrets/replication_ca}
SOURCE_RECEIPT_PUBLIC_KEY=${BLOCKZILLA_PULL_CLIENT_RECEIPT_PUBLIC_KEY_FILE:-/run/secrets/blockzilla_receipt_public_key}
RUNTIME_PRIVATE_DIRECTORY=${BLOCKZILLA_PULL_CLIENT_RUNTIME_PRIVATE_DIRECTORY:-/tmp/blockzilla-pull-client}
RUNTIME_CLIENT_PRIVATE_KEY=${RUNTIME_PRIVATE_DIRECTORY}/client-private-key.pem
POLL_INTERVAL_MS=${BLOCKZILLA_PULL_CLIENT_POLL_INTERVAL_MS:-1000}
PROTOCOL=${BLOCKZILLA_PULL_CLIENT_PROTOCOL:-v1}

die() {
  # Do not echo file paths, endpoints, credentials, config content, or remote
  # status strings into container logs.
  printf '%s\n' "raw-pull-client: $1" >&2
  exit 1
}

require_regular_readable_file() {
  label=$1
  path=$2
  [ -f "$path" ] && [ ! -L "$path" ] && [ -r "$path" ] || \
    die "$label is unavailable or unsafe"
}

case "$POLL_INTERVAL_MS" in
  ''|*[!0-9]*) die "poll interval must be a positive integer" ;;
esac
[ "$POLL_INTERVAL_MS" -gt 0 ] || die "poll interval must be greater than zero"
case "$PROTOCOL" in
  v1|v2) ;;
  *) die "protocol must be v1 or v2" ;;
esac

[ "$(id -u)" = 10001 ] || die "must run as dedicated UID 10001"
require_regular_readable_file "pull client config" "$CONFIG_FILE"
require_regular_readable_file "client certificate" "$SOURCE_CLIENT_CERTIFICATE"
require_regular_readable_file "client private key" "$SOURCE_CLIENT_PRIVATE_KEY"
require_regular_readable_file "replication CA" "$SOURCE_REPLICATION_CA"
require_regular_readable_file "receipt public key" "$SOURCE_RECEIPT_PUBLIC_KEY"

mkdir -p "$RUNTIME_PRIVATE_DIRECTORY"
[ -d "$RUNTIME_PRIVATE_DIRECTORY" ] && [ ! -L "$RUNTIME_PRIVATE_DIRECTORY" ] || \
  die "runtime private-key directory is unsafe"
chmod 0700 "$RUNTIME_PRIVATE_DIRECTORY"

rm -f "$RUNTIME_CLIENT_PRIVATE_KEY"
if ! install -m 0600 "$SOURCE_CLIENT_PRIVATE_KEY" "$RUNTIME_CLIENT_PRIVATE_KEY" 2>/dev/null; then
  die "could not stage the client private key"
fi
require_regular_readable_file "runtime client private key" "$RUNTIME_CLIENT_PRIVATE_KEY"
[ "$(stat -c '%a' "$RUNTIME_CLIENT_PRIVATE_KEY" 2>/dev/null)" = 600 ] || \
  die "runtime client private key has an unsafe mode"

exec "$HIVEZILLA_BIN" pull-grpc-raw \
  --config "$CONFIG_FILE" \
  --poll-interval-ms "$POLL_INTERVAL_MS" \
  --protocol "$PROTOCOL"
