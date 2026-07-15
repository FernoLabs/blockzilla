#!/bin/sh
set -eu

# Add the reverse Blockzilla -> Hetzner pull identities to an existing offline replication CA.
# The existing CA and Blockzilla receipt key remain stable. This helper refuses to overwrite its
# output and never prints private-key material.

umask 077
export LC_ALL=C

die() {
  printf '%s\n' "pull-pki: $*" >&2
  exit 1
}

require_stable_id() {
  label=$1
  value=$2
  case "$value" in
    ''|[!A-Za-z0-9]*|*[!A-Za-z0-9_.-]*) die "$label must use the stable identifier grammar" ;;
  esac
  [ "${#value}" -le 64 ] || die "$label must contain at most 64 ASCII characters"
}

require_dns_name() {
  label=$1
  value=$2
  case "$value" in
    ''|[!A-Za-z0-9]*|*[!A-Za-z0-9.-]*|*..*|*[!A-Za-z0-9]) \
      die "$label must be a plain DNS name" ;;
  esac
  [ "${#value}" -le 253 ] || die "$label is too long"
  remaining=$value
  while :; do
    case "$remaining" in
      *.*)
        dns_label=${remaining%%.*}
        remaining=${remaining#*.}
        ;;
      *)
        dns_label=$remaining
        remaining=
        ;;
    esac
    case "$dns_label" in
      ''|-*|*-|*[!A-Za-z0-9-]*) die "$label contains an invalid DNS label" ;;
    esac
    [ "${#dns_label}" -le 63 ] || die "$label contains an overlong DNS label"
    [ -n "$remaining" ] || break
  done
}

require_regular_file() {
  label=$1
  path=$2
  [ -f "$path" ] && [ ! -L "$path" ] && [ -r "$path" ] || \
    die "$label must be a readable regular non-symlink file"
}

[ "$#" -eq 2 ] || die "usage: $0 EXISTING_PKI_DIRECTORY OUTPUT_DIRECTORY"
command -v openssl >/dev/null 2>&1 || die "openssl is required"
command -v od >/dev/null 2>&1 || die "od is required"

EXISTING_DIRECTORY=$1
OUTPUT_DIRECTORY=$2
CLUSTER_ID=${BLOCKZILLA_CLUSTER_ID:-solana-mainnet}
ORIGIN_NODE_ID=${BLOCKZILLA_ORIGIN_NODE_ID:-hetzner-dokploy-01}
SOURCE_ID=${BLOCKZILLA_SOURCE_ID:-grpc-raw-hetzner-backup}
SOURCE_SERVER_NAME=${BLOCKZILLA_PULL_SOURCE_SERVER_NAME:-blockzilla-hetzner-source}
PULL_CLIENT_ID=${BLOCKZILLA_PULL_CLIENT_ID:-blockzilla-pull-client}
LEAF_DAYS=${BLOCKZILLA_REPLICATION_LEAF_DAYS:-825}

require_stable_id BLOCKZILLA_CLUSTER_ID "$CLUSTER_ID"
require_stable_id BLOCKZILLA_ORIGIN_NODE_ID "$ORIGIN_NODE_ID"
require_stable_id BLOCKZILLA_SOURCE_ID "$SOURCE_ID"
require_stable_id BLOCKZILLA_PULL_CLIENT_ID "$PULL_CLIENT_ID"
require_dns_name BLOCKZILLA_PULL_SOURCE_SERVER_NAME "$SOURCE_SERVER_NAME"
case "$LEAF_DAYS" in
  ''|*[!0-9]*) die "BLOCKZILLA_REPLICATION_LEAF_DAYS must be a positive integer" ;;
esac
[ "$LEAF_DAYS" -gt 0 ] || die "BLOCKZILLA_REPLICATION_LEAF_DAYS must be greater than zero"

[ -d "$EXISTING_DIRECTORY" ] && [ ! -L "$EXISTING_DIRECTORY" ] || \
  die "existing PKI directory must be a non-symlink directory"
CA_CERTIFICATE=$EXISTING_DIRECTORY/offline/replication-ca.crt
CA_PRIVATE_KEY=$EXISTING_DIRECTORY/offline/replication-ca.key
RECEIPT_PUBLIC_KEY=$EXISTING_DIRECTORY/hetzner/blockzilla-receipt.pub
require_regular_file "replication CA certificate" "$CA_CERTIFICATE"
require_regular_file "replication CA private key" "$CA_PRIVATE_KEY"
require_regular_file "Blockzilla receipt public key" "$RECEIPT_PUBLIC_KEY"

ca_certificate_public=$(openssl x509 -in "$CA_CERTIFICATE" -pubkey -noout | openssl dgst -sha256)
ca_private_public=$(openssl pkey -in "$CA_PRIVATE_KEY" -pubout | openssl dgst -sha256)
[ "$ca_certificate_public" = "$ca_private_public" ] || \
  die "replication CA certificate and private key do not match"

[ ! -e "$OUTPUT_DIRECTORY" ] && [ ! -L "$OUTPUT_DIRECTORY" ] || \
  die "output directory already exists; refusing to overwrite it"
output_parent=${OUTPUT_DIRECTORY%/*}
if [ "$output_parent" = "$OUTPUT_DIRECTORY" ]; then
  output_parent=.
fi
output_name=${OUTPUT_DIRECTORY##*/}
[ -n "$output_name" ] || die "output directory must not end with a slash"
[ -d "$output_parent" ] && [ ! -L "$output_parent" ] || \
  die "output parent must be an existing non-symlink directory"

stage=$(mktemp -d "${output_parent}/.${output_name}.tmp.XXXXXX")
cleanup() {
  rm -rf "$stage"
}
trap cleanup 0 HUP INT TERM
mkdir -m 0700 "$stage/hetzner" "$stage/blockzilla" "$stage/.work"
work=$stage/.work

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 \
  -out "$stage/hetzner/hetzner-pull-source.key" >/dev/null 2>&1
openssl req -new -sha256 \
  -key "$stage/hetzner/hetzner-pull-source.key" \
  -subj "/CN=$SOURCE_SERVER_NAME" \
  -out "$work/source-server.csr" >/dev/null 2>&1
printf '%s\n' \
  'basicConstraints=critical,CA:FALSE' \
  'keyUsage=critical,digitalSignature,keyEncipherment' \
  'extendedKeyUsage=serverAuth' \
  "subjectAltName=DNS:$SOURCE_SERVER_NAME" \
  'subjectKeyIdentifier=hash' \
  'authorityKeyIdentifier=keyid,issuer' >"$work/source-server.ext"
server_serial=$(openssl rand -hex 16)
openssl x509 -req -sha256 \
  -in "$work/source-server.csr" \
  -CA "$CA_CERTIFICATE" \
  -CAkey "$CA_PRIVATE_KEY" \
  -set_serial "0x$server_serial" \
  -days "$LEAF_DAYS" \
  -extfile "$work/source-server.ext" \
  -out "$stage/hetzner/hetzner-pull-source.crt" >/dev/null 2>&1

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 \
  -out "$stage/blockzilla/blockzilla-pull-client.key" >/dev/null 2>&1
openssl req -new -sha256 \
  -key "$stage/blockzilla/blockzilla-pull-client.key" \
  -subj "/CN=$PULL_CLIENT_ID" \
  -out "$work/pull-client.csr" >/dev/null 2>&1
printf '%s\n' \
  'basicConstraints=critical,CA:FALSE' \
  'keyUsage=critical,digitalSignature,keyEncipherment' \
  'extendedKeyUsage=clientAuth' \
  'subjectKeyIdentifier=hash' \
  'authorityKeyIdentifier=keyid,issuer' >"$work/pull-client.ext"
client_serial=$(openssl rand -hex 16)
openssl x509 -req -sha256 \
  -in "$work/pull-client.csr" \
  -CA "$CA_CERTIFICATE" \
  -CAkey "$CA_PRIVATE_KEY" \
  -set_serial "0x$client_serial" \
  -days "$LEAF_DAYS" \
  -extfile "$work/pull-client.ext" \
  -out "$stage/blockzilla/blockzilla-pull-client.crt" >/dev/null 2>&1

client_fingerprint=$(
  openssl x509 -in "$stage/blockzilla/blockzilla-pull-client.crt" -outform DER |
    openssl dgst -sha256 -binary |
    od -An -tx1 |
    tr -d ' \n'
)
case "$client_fingerprint" in
  *[!0-9a-f]*|'') die "could not derive the lowercase pull-client fingerprint" ;;
esac
[ "${#client_fingerprint}" -eq 64 ] || die "pull-client fingerprint has the wrong length"

printf '%s\n' \
  '{' \
  '  "schema_version": 1,' \
  '  "clients": [' \
  '    {' \
  "      \"certificate_sha256\": \"$client_fingerprint\"," \
  '      "identities": [' \
  '        {' \
  "          \"cluster_id\": \"$CLUSTER_ID\"," \
  "          \"origin_node_id\": \"$ORIGIN_NODE_ID\"," \
  "          \"source_id\": \"$SOURCE_ID\"" \
  '        }' \
  '      ]' \
  '    }' \
  '  ]' \
  '}' >"$stage/hetzner/pull-allowed-nodes.json"
cp "$stage/hetzner/pull-allowed-nodes.json" "$stage/blockzilla/pull-allowed-nodes.json"
cp "$CA_CERTIFICATE" "$stage/hetzner/pull-client-ca.crt"
cp "$CA_CERTIFICATE" "$stage/blockzilla/hetzner-pull-source-ca.crt"
cp "$RECEIPT_PUBLIC_KEY" "$stage/hetzner/blockzilla-receipt.pub"
cp "$RECEIPT_PUBLIC_KEY" "$stage/blockzilla/blockzilla-receipt.pub"

openssl verify -CAfile "$CA_CERTIFICATE" \
  -purpose sslserver "$stage/hetzner/hetzner-pull-source.crt" >/dev/null
openssl verify -CAfile "$CA_CERTIFICATE" \
  -purpose sslclient "$stage/blockzilla/blockzilla-pull-client.crt" >/dev/null
openssl pkey -in "$stage/hetzner/hetzner-pull-source.key" -check -noout >/dev/null
openssl pkey -in "$stage/blockzilla/blockzilla-pull-client.key" -check -noout >/dev/null
openssl pkey -pubin -in "$stage/hetzner/blockzilla-receipt.pub" -noout >/dev/null

rm -rf "$work"
chmod 0600 \
  "$stage/hetzner/hetzner-pull-source.key" \
  "$stage/blockzilla/blockzilla-pull-client.key"
chmod 0644 \
  "$stage/hetzner/hetzner-pull-source.crt" \
  "$stage/hetzner/pull-client-ca.crt" \
  "$stage/hetzner/pull-allowed-nodes.json" \
  "$stage/hetzner/blockzilla-receipt.pub" \
  "$stage/blockzilla/blockzilla-pull-client.crt" \
  "$stage/blockzilla/hetzner-pull-source-ca.crt" \
  "$stage/blockzilla/pull-allowed-nodes.json" \
  "$stage/blockzilla/blockzilla-receipt.pub"

mv "$stage" "$OUTPUT_DIRECTORY"
trap - 0 HUP INT TERM
printf '%s\n' "Created split reverse-pull trust material in $OUTPUT_DIRECTORY"
printf '%s\n' "Pull client certificate SHA-256: $client_fingerprint"
