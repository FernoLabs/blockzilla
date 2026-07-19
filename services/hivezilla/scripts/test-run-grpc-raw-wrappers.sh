#!/bin/sh
set -eu

SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TEMPORARY=$(mktemp -d)
trap 'rm -rf "$TEMPORARY"' EXIT HUP INT TERM

runtime_directory=$TEMPORARY/runtime
cache_root=$TEMPORARY/cache
control_directory=$TEMPORARY/control
config_file=$TEMPORARY/replica.json
source_key=$TEMPORARY/source-private-key.pem
mock_hivezilla=$TEMPORARY/hivezilla
called_file=$TEMPORARY/called
bridge_called_file=$TEMPORARY/bridge-called
mock_bin_directory=$TEMPORARY/bin

mkdir -p "$cache_root" "$mock_bin_directory"
printf '%s\n' test-private-key >"$source_key"
chmod 0600 "$source_key"
printf '{"client_private_key_file":"%s/client-private-key.pem"}\n' \
  "$runtime_directory" >"$config_file"

cat >"$mock_hivezilla" <<'MOCK'
#!/bin/sh
set -eu
command_name=$1
shift
case "$command_name" in
  replicate-grpc-raw)
    [ "$1" = --config ]
    config_file=$2
    [ -f "$config_file" ]
    [ -f "$BLOCKZILLA_TEST_RUNTIME_KEY" ]
    mode=$(stat -f '%Lp' "$BLOCKZILLA_TEST_RUNTIME_KEY" 2>/dev/null || \
      stat -c '%a' "$BLOCKZILLA_TEST_RUNTIME_KEY")
    [ "$mode" = 600 ]
    grep -F "\"client_private_key_file\":\"$BLOCKZILLA_TEST_RUNTIME_KEY\"" \
      "$config_file" >/dev/null
    printf '%s\n' called >"$BLOCKZILLA_TEST_CALLED_FILE"
    ;;
  bridge-receiver-grpc-raw)
    printf '%s\n' called >"$BLOCKZILLA_TEST_BRIDGE_CALLED_FILE"
    ;;
  *) exit 64 ;;
esac
MOCK
chmod 0700 "$mock_hivezilla"
cat >"$mock_bin_directory/id" <<'MOCK'
#!/bin/sh
[ "$1" = -u ] && printf '%s\n' 10001
MOCK
chmod 0700 "$mock_bin_directory/id"

PATH=$mock_bin_directory:$PATH \
BLOCKZILLA_REPLICATION_CONFIG_FILE=$config_file \
BLOCKZILLA_REPLICATION_CACHE_ROOT=$cache_root \
BLOCKZILLA_REPLICATION_PRIVATE_KEY_SOURCE=$source_key \
BLOCKZILLA_REPLICATION_RUNTIME_PRIVATE_DIRECTORY=$runtime_directory \
BLOCKZILLA_REPLICATION_ACK_STATUS_FILE=$control_directory/ack.json \
BLOCKZILLA_TEST_RUNTIME_KEY=$runtime_directory/client-private-key.pem \
BLOCKZILLA_TEST_CALLED_FILE=$called_file \
HIVEZILLA_BIN=$mock_hivezilla \
  "$SCRIPT_DIRECTORY/run-grpc-raw-replicator.sh"

[ -f "$called_file" ]

bridge_source=$TEMPORARY/receiver
bridge_output_parent=$TEMPORARY/derived
mkdir -p "$bridge_source" "$bridge_output_parent"
PATH=$mock_bin_directory:$PATH \
BLOCKZILLA_RECEIVER_BRIDGE_SOURCE_ROOT=$bridge_source \
BLOCKZILLA_RECEIVER_BRIDGE_OUTPUT_DIR=$bridge_output_parent/raw-current \
BLOCKZILLA_RECEIVER_BRIDGE_JOURNAL_ID=00112233445566778899aabbccddeeff \
BLOCKZILLA_RECEIVER_BRIDGE_POLL_INTERVAL_SECS=1 \
BLOCKZILLA_RECEIVER_BRIDGE_ERROR_DIRECTORY=$TEMPORARY \
BLOCKZILLA_TEST_BRIDGE_CALLED_FILE=$bridge_called_file \
HIVEZILLA_BIN=$mock_hivezilla \
  "$SCRIPT_DIRECTORY/run-grpc-receiver-bridge.sh" &
bridge_pid=$!
attempt=0
while [ ! -f "$bridge_called_file" ] && [ "$attempt" -lt 30 ]; do
  sleep 0.1
  attempt=$((attempt + 1))
done
kill -TERM "$bridge_pid" 2>/dev/null || true
wait "$bridge_pid" 2>/dev/null || true
[ -f "$bridge_called_file" ]

printf '%s\n' "raw gRPC wrapper staging test passed"
