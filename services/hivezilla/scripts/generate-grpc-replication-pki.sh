#!/bin/sh
set -eu

# Offline bootstrap helper for the source host -> Blockzilla raw replication trust
# bundle. It refuses to overwrite an existing destination and never prints key
# material. Run it on a trusted machine, then distribute only each host's
# subdirectory and retain offline/ separately.

umask 077
export LC_ALL=C

die() {
  printf '%s\n' "replication-pki: $*" >&2
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
  value=$1
  case "$value" in
    ''|[!A-Za-z0-9]*|*[!A-Za-z0-9.-]*|*..*|*[!A-Za-z0-9]) \
      die "BLOCKZILLA_PRIMARY_SERVER_NAME must be a plain DNS name" ;;
  esac
  [ "${#value}" -le 253 ] || die "BLOCKZILLA_PRIMARY_SERVER_NAME is too long"
  dns_remaining=$value
  while :; do
    case "$dns_remaining" in
      *.*)
        dns_label=${dns_remaining%%.*}
        dns_remaining=${dns_remaining#*.}
        ;;
      *)
        dns_label=$dns_remaining
        dns_remaining=
        ;;
    esac
    case "$dns_label" in
      ''|-*|*-|*[!A-Za-z0-9-]*) \
        die "BLOCKZILLA_PRIMARY_SERVER_NAME contains an invalid DNS label" ;;
    esac
    [ "${#dns_label}" -le 63 ] || \
      die "BLOCKZILLA_PRIMARY_SERVER_NAME contains an overlong DNS label"
    [ -n "$dns_remaining" ] || break
  done
}

[ "$#" -eq 1 ] || die "usage: $0 OUTPUT_DIRECTORY"
command -v openssl >/dev/null 2>&1 || die "openssl is required"
command -v od >/dev/null 2>&1 || die "od is required"
openssl genpkey -algorithm ED25519 -out /dev/null >/dev/null 2>&1 || \
  die "OpenSSL with Ed25519 support is required"

OUTPUT_DIRECTORY=$1
CLUSTER_ID=${BLOCKZILLA_CLUSTER_ID:-solana-mainnet}
REPLICA_NODE_ID=${BLOCKZILLA_REPLICA_NODE_ID:-source-node-01}
SOURCE_ID=${BLOCKZILLA_SOURCE_ID:-grpc-raw-source-backup}
PRIMARY_SERVER_NAME=${BLOCKZILLA_PRIMARY_SERVER_NAME:-blockzilla-primary}
RECEIPT_KEY_ID=${BLOCKZILLA_RECEIPT_KEY_ID:-receipt-current}
CA_DAYS=${BLOCKZILLA_REPLICATION_CA_DAYS:-3650}
LEAF_DAYS=${BLOCKZILLA_REPLICATION_LEAF_DAYS:-825}

require_stable_id "BLOCKZILLA_CLUSTER_ID" "$CLUSTER_ID"
require_stable_id "BLOCKZILLA_REPLICA_NODE_ID" "$REPLICA_NODE_ID"
require_stable_id "BLOCKZILLA_SOURCE_ID" "$SOURCE_ID"
require_stable_id "BLOCKZILLA_RECEIPT_KEY_ID" "$RECEIPT_KEY_ID"
require_dns_name "$PRIMARY_SERVER_NAME"
case "$CA_DAYS" in ''|*[!0-9]*) die "BLOCKZILLA_REPLICATION_CA_DAYS must be a positive integer" ;; esac
case "$LEAF_DAYS" in ''|*[!0-9]*) die "BLOCKZILLA_REPLICATION_LEAF_DAYS must be a positive integer" ;; esac
[ "$CA_DAYS" -gt 0 ] || die "BLOCKZILLA_REPLICATION_CA_DAYS must be greater than zero"
[ "$LEAF_DAYS" -gt 0 ] || die "BLOCKZILLA_REPLICATION_LEAF_DAYS must be greater than zero"

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

mkdir -m 0700 "$stage/offline" "$stage/blockzilla" "$stage/source-node" "$stage/.work"
work=$stage/.work

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 \
  -out "$stage/offline/replication-ca.key" >/dev/null 2>&1
openssl req -new -x509 -sha256 \
  -key "$stage/offline/replication-ca.key" \
  -subj "/CN=Blockzilla raw replication CA" \
  -days "$CA_DAYS" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "subjectKeyIdentifier=hash" \
  -out "$stage/offline/replication-ca.crt" >/dev/null 2>&1

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 \
  -out "$stage/blockzilla/blockzilla-primary.key" >/dev/null 2>&1
openssl req -new -sha256 \
  -key "$stage/blockzilla/blockzilla-primary.key" \
  -subj "/CN=$PRIMARY_SERVER_NAME" \
  -out "$work/server.csr" >/dev/null 2>&1
printf '%s\n' \
  'basicConstraints=critical,CA:FALSE' \
  'keyUsage=critical,digitalSignature,keyEncipherment' \
  'extendedKeyUsage=serverAuth' \
  "subjectAltName=DNS:$PRIMARY_SERVER_NAME" \
  'subjectKeyIdentifier=hash' \
  'authorityKeyIdentifier=keyid,issuer' >"$work/server.ext"
server_serial=$(openssl rand -hex 16)
openssl x509 -req -sha256 \
  -in "$work/server.csr" \
  -CA "$stage/offline/replication-ca.crt" \
  -CAkey "$stage/offline/replication-ca.key" \
  -set_serial "0x$server_serial" \
  -days "$LEAF_DAYS" \
  -extfile "$work/server.ext" \
  -out "$stage/blockzilla/blockzilla-primary.crt" >/dev/null 2>&1

openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 \
  -out "$stage/source-node/source-node-replica.key" >/dev/null 2>&1
openssl req -new -sha256 \
  -key "$stage/source-node/source-node-replica.key" \
  -subj "/CN=$REPLICA_NODE_ID" \
  -out "$work/client.csr" >/dev/null 2>&1
printf '%s\n' \
  'basicConstraints=critical,CA:FALSE' \
  'keyUsage=critical,digitalSignature,keyEncipherment' \
  'extendedKeyUsage=clientAuth' \
  'subjectKeyIdentifier=hash' \
  'authorityKeyIdentifier=keyid,issuer' >"$work/client.ext"
client_serial=$(openssl rand -hex 16)
openssl x509 -req -sha256 \
  -in "$work/client.csr" \
  -CA "$stage/offline/replication-ca.crt" \
  -CAkey "$stage/offline/replication-ca.key" \
  -set_serial "0x$client_serial" \
  -days "$LEAF_DAYS" \
  -extfile "$work/client.ext" \
  -out "$stage/source-node/source-node-replica.crt" >/dev/null 2>&1

openssl genpkey -algorithm ED25519 \
  -out "$stage/blockzilla/blockzilla-receipt.key" >/dev/null 2>&1
openssl pkey \
  -in "$stage/blockzilla/blockzilla-receipt.key" \
  -pubout \
  -out "$stage/source-node/blockzilla-receipt.pub" >/dev/null 2>&1

cp "$stage/offline/replication-ca.crt" "$stage/blockzilla/replica-ca.crt"
cp "$stage/offline/replication-ca.crt" "$stage/source-node/blockzilla-primary-ca.crt"
printf '%s\n' "$RECEIPT_KEY_ID" >"$stage/blockzilla/receipt-key-id.txt"
printf '%s\n' "$RECEIPT_KEY_ID" >"$stage/source-node/receipt-key-id.txt"

client_fingerprint=$(
  openssl x509 -in "$stage/source-node/source-node-replica.crt" -outform DER |
    openssl dgst -sha256 -binary |
    od -An -tx1 |
    tr -d ' \n'
)
case "$client_fingerprint" in
  *[!0-9a-f]*|'') die "could not derive the lowercase client certificate fingerprint" ;;
esac
[ "${#client_fingerprint}" -eq 64 ] || die "client certificate fingerprint has the wrong length"

printf '%s\n' \
  '{' \
  '  "schema_version": 1,' \
  '  "clients": [' \
  '    {' \
  "      \"certificate_sha256\": \"$client_fingerprint\"," \
  '      "identities": [' \
  '        {' \
  "          \"cluster_id\": \"$CLUSTER_ID\"," \
  "          \"origin_node_id\": \"$REPLICA_NODE_ID\"," \
  "          \"source_id\": \"$SOURCE_ID\"" \
  '        }' \
  '      ]' \
  '    }' \
  '  ]' \
  '}' >"$stage/blockzilla/allowed-nodes.json"

openssl verify -CAfile "$stage/offline/replication-ca.crt" \
  -purpose sslserver "$stage/blockzilla/blockzilla-primary.crt" >/dev/null
openssl verify -CAfile "$stage/offline/replication-ca.crt" \
  -purpose sslclient "$stage/source-node/source-node-replica.crt" >/dev/null
openssl pkey -in "$stage/blockzilla/blockzilla-receipt.key" -check -noout >/dev/null
openssl pkey -pubin -in "$stage/source-node/blockzilla-receipt.pub" -noout >/dev/null

rm -rf "$work"
chmod 0600 \
  "$stage/offline/replication-ca.key" \
  "$stage/blockzilla/blockzilla-primary.key" \
  "$stage/blockzilla/blockzilla-receipt.key" \
  "$stage/source-node/source-node-replica.key"
chmod 0644 \
  "$stage/offline/replication-ca.crt" \
  "$stage/blockzilla/blockzilla-primary.crt" \
  "$stage/blockzilla/replica-ca.crt" \
  "$stage/blockzilla/allowed-nodes.json" \
  "$stage/blockzilla/receipt-key-id.txt" \
  "$stage/source-node/blockzilla-primary-ca.crt" \
  "$stage/source-node/source-node-replica.crt" \
  "$stage/source-node/blockzilla-receipt.pub" \
  "$stage/source-node/receipt-key-id.txt"

mv "$stage" "$OUTPUT_DIRECTORY"
trap - 0 HUP INT TERM
printf '%s\n' "Created a split replication trust bundle in $OUTPUT_DIRECTORY"
printf '%s\n' "Client certificate SHA-256: $client_fingerprint"
