#!/bin/sh
set -eu

# Compose file-backed secrets keep their host ownership and mode. Stage only
# the TLS server private key in this container's UID-only tmpfs; every public
# trust/config input remains mounted read-only at its stable path.

umask 077
export LC_ALL=C

CONFIG_FILE=${BLOCKZILLA_PULL_SOURCE_CONFIG_FILE:-/run/secrets/grpc_pull_source_config}
SOURCE_SERVER_CERTIFICATE=${BLOCKZILLA_PULL_SOURCE_CERTIFICATE_FILE:-/run/secrets/hetzner_pull_source_certificate}
SOURCE_SERVER_PRIVATE_KEY=${BLOCKZILLA_PULL_SOURCE_PRIVATE_KEY_SOURCE:-/run/secrets/hetzner_pull_source_private_key}
SOURCE_CLIENT_CA=${BLOCKZILLA_PULL_SOURCE_CLIENT_CA_FILE:-/run/secrets/pull_client_ca}
SOURCE_ALLOWED_NODES=${BLOCKZILLA_PULL_SOURCE_ALLOWLIST_FILE:-/run/secrets/pull_allowed_nodes}
SOURCE_RECEIPT_PUBLIC_KEY=${BLOCKZILLA_PULL_SOURCE_RECEIPT_PUBLIC_KEY_FILE:-/run/secrets/blockzilla_receipt_public_key}
RUNTIME_PRIVATE_DIRECTORY=/tmp/blockzilla-pull-source
RUNTIME_SERVER_PRIVATE_KEY=${RUNTIME_PRIVATE_DIRECTORY}/server-private-key.pem

die() {
  # Keep startup failures useful without echoing a path, endpoint, certificate,
  # key, config content, or remote status supplied by an untrusted input.
  printf '%s\n' "raw-pull-source: $1" >&2
  exit 1
}

require_regular_readable_file() {
  label=$1
  path=$2
  [ -f "$path" ] && [ ! -L "$path" ] && [ -r "$path" ] || \
    die "$label is unavailable or unsafe"
}

[ "$(id -u)" = 10001 ] || die "must run as dedicated UID 10001"
require_regular_readable_file "source config" "$CONFIG_FILE"
require_regular_readable_file "server certificate" "$SOURCE_SERVER_CERTIFICATE"
require_regular_readable_file "server private key" "$SOURCE_SERVER_PRIVATE_KEY"
require_regular_readable_file "client CA" "$SOURCE_CLIENT_CA"
require_regular_readable_file "client allowlist" "$SOURCE_ALLOWED_NODES"
require_regular_readable_file "receipt public key" "$SOURCE_RECEIPT_PUBLIC_KEY"
[ -d /source-data ] && [ ! -L /source-data ] || \
  die "source data directory is unavailable or unsafe"
[ -d /source-data/grpc-cache ] && [ ! -L /source-data/grpc-cache ] \
  && [ -r /source-data/grpc-cache ] && [ -x /source-data/grpc-cache ] || \
  die "source cache directory is unavailable or unsafe"
# `test -w` checks mode bits and can report writable even for a Docker read-only
# volume mount. Require the actual mount and every nested mount to carry `ro`.
awk '
  $5 == "/source-data" {
    source_seen = 1
    if ($6 ~ /(^|,)ro(,|$)/) source_ro = 1
  }
  index($5, "/source-data/") == 1 && $6 !~ /(^|,)ro(,|$)/ {
    nested_rw = 1
  }
  END { exit !(source_seen && source_ro && !nested_rw) }
' /proc/self/mountinfo || die "source cache mount is not read-only"
require_regular_readable_file \
  "source rotation lock" /source-data/grpc-cache/.rotation.lock
[ -d /control ] && [ ! -L /control ] \
  && [ -w /control ] && [ -x /control ] || \
  die "durable control directory is unavailable or unsafe"

mkdir -p "$RUNTIME_PRIVATE_DIRECTORY"
[ -d "$RUNTIME_PRIVATE_DIRECTORY" ] && [ ! -L "$RUNTIME_PRIVATE_DIRECTORY" ] || \
  die "runtime private-key directory is unsafe"
chmod 0700 "$RUNTIME_PRIVATE_DIRECTORY"

rm -f "$RUNTIME_SERVER_PRIVATE_KEY"
if ! install -m 0600 "$SOURCE_SERVER_PRIVATE_KEY" "$RUNTIME_SERVER_PRIVATE_KEY" 2>/dev/null; then
  die "could not stage the server private key"
fi
require_regular_readable_file "runtime server private key" "$RUNTIME_SERVER_PRIVATE_KEY"
[ "$(stat -c '%a' "$RUNTIME_SERVER_PRIVATE_KEY" 2>/dev/null)" = 600 ] || \
  die "runtime server private key has an unsafe mode"

exec /usr/local/bin/blockzilla-live-producer serve-grpc-raw-pull-source \
  --config "$CONFIG_FILE"
