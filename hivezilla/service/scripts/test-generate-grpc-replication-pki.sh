#!/bin/sh
set -eu

SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TEMPORARY=$(mktemp -d)
trap 'rm -rf "$TEMPORARY"' EXIT HUP INT TERM

if ! openssl genpkey -algorithm ED25519 \
  -out "$TEMPORARY/ed25519-probe.key" >/dev/null 2>&1
then
  printf '%s\n' "raw gRPC replication PKI test skipped: OpenSSL lacks Ed25519"
  exit 0
fi

bundle=$TEMPORARY/replication-pki
pull_bundle=$TEMPORARY/pull-pki
"$SCRIPT_DIRECTORY/generate-grpc-replication-pki.sh" "$bundle" >/dev/null
"$SCRIPT_DIRECTORY/generate-grpc-pull-pki.sh" "$bundle" "$pull_bundle" >/dev/null

[ "$(cat "$bundle/blockzilla/receipt-key-id.txt")" = receipt-current ]
[ "$(cat "$bundle/source-node/receipt-key-id.txt")" = receipt-current ]
test -s "$bundle/blockzilla/blockzilla-primary.crt"
test -s "$bundle/blockzilla/blockzilla-primary.key"
test -s "$bundle/blockzilla/blockzilla-receipt.key"
test -s "$bundle/source-node/source-node-replica.crt"
test -s "$bundle/source-node/source-node-replica.key"
test -s "$bundle/source-node/blockzilla-receipt.pub"
test -s "$bundle/blockzilla/allowed-nodes.json"

openssl verify -CAfile "$bundle/offline/replication-ca.crt" \
  -purpose sslserver "$bundle/blockzilla/blockzilla-primary.crt" >/dev/null
openssl verify -CAfile "$bundle/offline/replication-ca.crt" \
  -purpose sslclient "$bundle/source-node/source-node-replica.crt" >/dev/null
grep -Fq '"origin_node_id": "source-node-01"' \
  "$bundle/blockzilla/allowed-nodes.json"
grep -Fq '"source_id": "grpc-raw-source-backup"' \
  "$bundle/blockzilla/allowed-nodes.json"
test -s "$pull_bundle/source-node/source-node-pull-source.crt"
test -s "$pull_bundle/source-node/source-node-pull-source.key"
test -s "$pull_bundle/source-node/pull-client-ca.crt"
test -s "$pull_bundle/source-node/pull-allowed-nodes.json"
test -s "$pull_bundle/blockzilla/blockzilla-pull-client.crt"
test -s "$pull_bundle/blockzilla/blockzilla-pull-client.key"
test -s "$pull_bundle/blockzilla/source-node-pull-source-ca.crt"
openssl verify -CAfile "$bundle/offline/replication-ca.crt" \
  -purpose sslserver "$pull_bundle/source-node/source-node-pull-source.crt" >/dev/null
openssl verify -CAfile "$bundle/offline/replication-ca.crt" \
  -purpose sslclient "$pull_bundle/blockzilla/blockzilla-pull-client.crt" >/dev/null

if "$SCRIPT_DIRECTORY/generate-grpc-replication-pki.sh" "$bundle" >/dev/null 2>&1; then
  echo "PKI generator overwrote an existing bundle" >&2
  exit 1
fi
if "$SCRIPT_DIRECTORY/generate-grpc-pull-pki.sh" \
  "$bundle" "$pull_bundle" >/dev/null 2>&1
then
  echo "pull PKI generator overwrote an existing bundle" >&2
  exit 1
fi

printf '%s\n' "raw gRPC replication PKI test passed"
