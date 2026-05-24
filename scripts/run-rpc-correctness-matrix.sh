#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out_root="${OUT_DIR:-target/rpc-bench/correctness-matrix-${stamp}}"
slot_index_dir="${SLOT_INDEX_DIR:-/srv/blockzilla/blockzilla/slot-index}"
epochs="${EPOCHS:-available}"
slots="${SLOTS:-}"
samples_per_epoch="${SAMPLES_PER_EPOCH:-100}"
slot_concurrency="${SLOT_CONCURRENCY:-2}"
primary="${PRIMARY:-helius}"
max_diffs_per_call="${MAX_DIFFS_PER_CALL:-256}"

blockzilla_url="${BLOCKZILLA_WORKER_URL:-https://worker.example.com}"
of_worker_url="${OF_WORKER_URL:-}"
helius_url="${HELIUS_RPC_URL:-}"
triton_url="${TRITON_RPC_URL:-}"
mainnet_url="${MAINNET_BETA_RPC_URL:-https://api.mainnet-beta.solana.com}"

if [[ -z "$helius_url" ]]; then
  echo "HELIUS_RPC_URL must be set" >&2
  exit 2
fi
if [[ -z "$triton_url" ]]; then
  echo "TRITON_RPC_URL must be set" >&2
  exit 2
fi

endpoint_args=(
  --endpoint "blockzilla=@BZ_MATRIX_BLOCKZILLA_URL"
  --endpoint "helius=@BZ_MATRIX_HELIUS_URL"
  --endpoint "triton=@BZ_MATRIX_TRITON_URL"
  --endpoint "mainnet=@BZ_MATRIX_MAINNET_URL"
)
if [[ -n "$of_worker_url" ]]; then
  endpoint_args+=(--endpoint "of=@BZ_MATRIX_OF_WORKER_URL")
fi
export BZ_MATRIX_BLOCKZILLA_URL="$blockzilla_url"
export BZ_MATRIX_HELIUS_URL="$helius_url"
export BZ_MATRIX_TRITON_URL="$triton_url"
export BZ_MATRIX_MAINNET_URL="$mainnet_url"
export BZ_MATRIX_OF_WORKER_URL="$of_worker_url"

default_cases=(
  "full:json:false"
  "full:jsonParsed:false"
  "full:base64:false"
  "full:base58:false"
  "accounts:json:false"
  "accounts:jsonParsed:false"
  "signatures:json:false"
  "none:json:false"
  "full:json:true"
)

case_list="${CASES:-}"
if [[ -n "$case_list" ]]; then
  IFS=',' read -r -a cases <<< "$case_list"
else
  cases=("${default_cases[@]}")
fi

slot_args=()
epoch_args=()
if [[ -n "$slots" ]]; then
  for slot in ${slots//,/ }; do
    slot_args+=(--slot "$slot")
  done
else
  for epoch in ${epochs//,/ }; do
    epoch_args+=("$epoch")
  done
fi

mkdir -p "$out_root"
echo "out_root=${out_root}"
echo "primary=${primary} epochs=${epochs} slots=${slots:-none} samples_per_epoch=${samples_per_epoch} cases=${#cases[@]}"

cargo build -p blockzilla-get-block-worker --bin rpc-correctness-check
runner="${RUNNER:-target/debug/rpc-correctness-check}"

for case_spec in "${cases[@]}"; do
  IFS=':' read -r details encoding rewards <<< "$case_spec"
  if [[ -z "${details:-}" || -z "${encoding:-}" || -z "${rewards:-}" ]]; then
    echo "invalid case '${case_spec}', expected transactionDetails:encoding:rewards" >&2
    exit 2
  fi
  case_name="${details}-${encoding}-rewards-${rewards}"
  case_out="${out_root}/${case_name}"
  mkdir -p "$case_out"
  echo "case=${case_name}"
  cmd=(
    "$runner"
    "${endpoint_args[@]}"
    --primary "$primary"
    --slot-index-dir "$slot_index_dir"
    --samples-per-epoch "$samples_per_epoch"
    --slot-concurrency "$slot_concurrency"
    --transaction-details "$details"
    --encoding "$encoding"
    --rewards="$rewards"
    --max-diffs-per-call "$max_diffs_per_call"
    --output-dir "$case_out"
    --prefix "$case_name"
  )
  if [[ -n "$slots" ]]; then
    cmd+=("${slot_args[@]}")
  else
    cmd+=("${epoch_args[@]}")
  fi
  "${cmd[@]}"
done

echo "completed=${out_root}"
