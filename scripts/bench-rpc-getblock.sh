#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage:
  bench-rpc-getblock.sh [endpoint] [slot...]

Examples:
  scripts/bench-rpc-getblock.sh 378967388
  scripts/bench-rpc-getblock.sh https://your-worker.example 378967388
  ROUNDS=5 TRANSACTION_DETAILS=none scripts/bench-rpc-getblock.sh https://api.mainnet-beta.solana.com 378967388

Environment:
  ROUNDS                         Requests per slot. Default: 3
  COMMITMENT                     Default: finalized
  ENCODING                       Default: json
  TRANSACTION_DETAILS            full, signatures, none. Default: full
  REWARDS                        true or false. Default: false
  MAX_SUPPORTED_TX_VERSION       Default: 0
  REQUEST_DELAY                  Seconds to sleep after each request. Default: 0
  CURL_EXTRA                     Extra curl flags, e.g. '--compressed'
  BLOCKZILLA_WORKER_URL          Default endpoint when no endpoint argument is given
EOF
  exit 0
fi

endpoint="${BLOCKZILLA_WORKER_URL:-}"
if [[ "${1:-}" =~ ^https?:// ]]; then
  endpoint="$1"
  shift
fi
if [[ -z "$endpoint" ]]; then
  echo "endpoint argument or BLOCKZILLA_WORKER_URL is required" >&2
  exit 2
fi

slots=("$@")
if [[ ${#slots[@]} -eq 0 ]]; then
  slots=(378967388)
fi

rounds="${ROUNDS:-3}"
commitment="${COMMITMENT:-finalized}"
encoding="${ENCODING:-json}"
transaction_details="${TRANSACTION_DETAILS:-full}"
rewards="${REWARDS:-false}"
max_supported_tx_version="${MAX_SUPPORTED_TX_VERSION:-0}"
request_delay="${REQUEST_DELAY:-0}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

printf "endpoint\tslot\tround\thttp\trpc\tbytes\tdns_s\tconnect_s\ttls_s\tttfb_s\ttotal_s\n"

for slot in "${slots[@]}"; do
  for round in $(seq 1 "$rounds"); do
    body="$tmpdir/body-$slot-$round.json"
    payload="$tmpdir/payload-$slot-$round.json"
    cat >"$payload" <<EOF
{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[${slot},{"commitment":"${commitment}","encoding":"${encoding}","transactionDetails":"${transaction_details}","maxSupportedTransactionVersion":${max_supported_tx_version},"rewards":${rewards}}]}
EOF

    # shellcheck disable=SC2086
    metrics="$(curl -sS ${CURL_EXTRA:-} -o "$body" \
      -w "%{http_code}\t%{size_download}\t%{time_namelookup}\t%{time_connect}\t%{time_appconnect}\t%{time_starttransfer}\t%{time_total}" \
      -X POST \
      -H "Content-Type: application/json" \
      --data-binary @"$payload" \
      "$endpoint")"

    IFS=$'\t' read -r http_code bytes dns connect tls ttfb total <<<"$metrics"
    rpc_status="ok"
    if grep -q '"error"' "$body"; then
      rpc_status="error"
    fi

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
      "$endpoint" \
      "$slot" \
      "$round" \
      "$http_code" \
      "$rpc_status" \
      "$bytes" \
      "$dns" \
      "$connect" \
      "$tls" \
      "$ttfb" \
      "$total"

    if [[ "$request_delay" != "0" ]]; then
      sleep "$request_delay"
    fi
  done
done
