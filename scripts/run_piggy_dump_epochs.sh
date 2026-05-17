#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_piggy_dump_epochs.sh [options]
#
# Options:
#   --start-at EPOCH       First epoch to scan. Defaults to Piggy deploy epoch 869.
#   --end-at EPOCH         Last epoch to scan. Defaults to newest epoch CAR in --car-dir.
#   --last-days DAYS       Scan roughly the last N days, ending at --end-at/newest epoch.
#   --car-dir DIR          Directory containing epoch-N.car or epoch-N.car.zst.
#   --out-dir DIR          Output root. Defaults to ./piggy-dump/epochs-START-END.
#   --start-slot SLOT      First slot to scan. Defaults to first Piggy tx slot 375549954.
#
# Backwards-compatible positional form still works:
#   scripts/run_piggy_dump_epochs.sh <start_epoch> <end_epoch> [car_dir] [out_dir] [start_slot]
#
# Output is split per epoch:
#   OUT/epoch-N/transactions.bin
#   OUT/epoch-N/signature.index.tsv
#   OUT/epoch-N/user.index.tsv
#   OUT/epoch-N/account.index.tsv
#   OUT/epoch-N/mint.index.tsv
#   OUT/epoch-N/program.index.tsv
#   OUT/epoch-N/meta.json

DEPLOY_EPOCH=869
DEPLOY_SLOT=375549954
SECONDS_PER_DAY=86400
SLOTS_PER_EPOCH=432000
MS_PER_SLOT=400

START_EPOCH=""
END_EPOCH=""
LAST_DAYS=""
CAR_DIR="./epochs"
OUT_DIR=""
START_SLOT="${DEPLOY_SLOT}"

if [[ $# -gt 0 && "${1}" != --* ]]; then
  START_EPOCH="${1:?start epoch required}"
  END_EPOCH="${2:?end epoch required}"
  CAR_DIR="${3:-./epochs}"
  OUT_DIR="${4:-}"
  START_SLOT="${5:-${DEPLOY_SLOT}}"
else
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --start-at)
        START_EPOCH="${2:?--start-at requires an epoch}"
        shift 2
        ;;
      --end-at)
        END_EPOCH="${2:?--end-at requires an epoch}"
        shift 2
        ;;
      --last-days)
        LAST_DAYS="${2:?--last-days requires a day count}"
        shift 2
        ;;
      --car-dir)
        CAR_DIR="${2:?--car-dir requires a directory}"
        shift 2
        ;;
      --out-dir)
        OUT_DIR="${2:?--out-dir requires a directory}"
        shift 2
        ;;
      --start-slot)
        START_SLOT="${2:?--start-slot requires a slot}"
        shift 2
        ;;
      -h|--help)
        cat <<'EOF'
Usage:
  scripts/run_piggy_dump_epochs.sh [options]

Options:
  --start-at EPOCH       First epoch to scan. Defaults to Piggy deploy epoch 869.
  --end-at EPOCH         Last epoch to scan. Defaults to newest epoch CAR in --car-dir.
  --last-days DAYS       Scan roughly the last N days, ending at --end-at/newest epoch.
  --car-dir DIR          Directory containing epoch-N.car or epoch-N.car.zst.
  --out-dir DIR          Output root. Defaults to ./piggy-dump/epochs-START-END.
  --start-slot SLOT      First slot to scan. Defaults to first Piggy tx slot 375549954.

Backwards-compatible positional form still works:
  scripts/run_piggy_dump_epochs.sh <start_epoch> <end_epoch> [car_dir] [out_dir] [start_slot]

Output is split per epoch:
  OUT/epoch-N/transactions.bin
  OUT/epoch-N/signature.index.tsv
  OUT/epoch-N/user.index.tsv
  OUT/epoch-N/account.index.tsv
  OUT/epoch-N/mint.index.tsv
  OUT/epoch-N/program.index.tsv
  OUT/epoch-N/meta.json
EOF
        exit 0
        ;;
      *)
        echo "unknown argument: $1" >&2
        exit 2
        ;;
    esac
  done
fi

discover_latest_epoch() {
  find "${CAR_DIR}" -maxdepth 1 -type f \( -name 'epoch-*.car' -o -name 'epoch-*.car.zst' \) \
    | sed -E 's#^.*/epoch-([0-9]+)\.car(\.zst)?$#\1#' \
    | sort -n \
    | tail -1
}

if [[ -z "${END_EPOCH}" ]]; then
  END_EPOCH="$(discover_latest_epoch)"
  if [[ -z "${END_EPOCH}" ]]; then
    echo "could not discover newest epoch in ${CAR_DIR}; pass --end-at" >&2
    exit 2
  fi
fi

if [[ -n "${LAST_DAYS}" ]]; then
  # Solana epochs are roughly 432k slots. At 400ms/slot that is almost 2 days.
  epochs_back=$(( (LAST_DAYS * SECONDS_PER_DAY * 1000 + SLOTS_PER_EPOCH * MS_PER_SLOT - 1) / (SLOTS_PER_EPOCH * MS_PER_SLOT) ))
  START_EPOCH=$(( END_EPOCH - epochs_back + 1 ))
elif [[ -z "${START_EPOCH}" ]]; then
  START_EPOCH="${DEPLOY_EPOCH}"
fi

if (( START_EPOCH < DEPLOY_EPOCH )); then
  START_EPOCH="${DEPLOY_EPOCH}"
fi

if [[ -z "${OUT_DIR}" ]]; then
  OUT_DIR="./piggy-dump/epochs-${START_EPOCH}-${END_EPOCH}"
fi

BIN="${BIN:-./target/release/of-token-index}"

mkdir -p "${OUT_DIR}"

if [[ ! -x "${BIN}" ]] || ! "${BIN}" --help 2>/dev/null | grep -q 'piggy-dump'; then
  cargo build --release -p of-token-index
fi

SUMMARY="${OUT_DIR}/summary.tsv"
STATE_DIR="${OUT_DIR}/_state"
SEED_USERS="${STATE_DIR}/users.txt"
SEED_TOKEN_ACCOUNTS="${STATE_DIR}/token-accounts.txt"
mkdir -p "${STATE_DIR}"
touch "${SEED_USERS}" "${SEED_TOKEN_ACCOUNTS}"
: >"${SUMMARY}"
printf "epoch\tcar\tdumped_txs\tdump_bytes\ttracked_users\ttracked_token_accounts\tsignature_rows\tuser_rows\taccount_rows\tmint_rows\tprogram_rows\n" >>"${SUMMARY}"

echo "piggy-dump range: epoch ${START_EPOCH}..${END_EPOCH}, start_slot=${START_SLOT}, car_dir=${CAR_DIR}, out_dir=${OUT_DIR}" >&2

for epoch in $(seq "${START_EPOCH}" "${END_EPOCH}"); do
  zst="${CAR_DIR}/epoch-${epoch}.car.zst"
  raw="${CAR_DIR}/epoch-${epoch}.car"

  if [[ -f "${zst}" ]]; then
    car="${zst}"
  elif [[ -f "${raw}" ]]; then
    car="${raw}"
  else
    printf "%s\tMISSING\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" "${epoch}" >>"${SUMMARY}"
    echo "epoch-${epoch}: missing ${zst} or ${raw}, skipping" >&2
    continue
  fi

  epoch_out="${OUT_DIR}/epoch-${epoch}"
  mkdir -p "${epoch_out}"

  echo "epoch-${epoch}: dumping ${car} -> ${epoch_out}" >&2
  "${BIN}" piggy-dump \
    --car "${car}" \
    --out-dir "${epoch_out}" \
    --start-slot "${START_SLOT}" \
    --seed-user-file "${SEED_USERS}" \
    --seed-token-account-file "${SEED_TOKEN_ACCOUNTS}"

  dumped="$(jq -r '.dumped_transactions' "${epoch_out}/meta.json")"
  bytes="$(jq -r '.dump_bytes' "${epoch_out}/meta.json")"
  users="$(jq -r '.discovered_users' "${epoch_out}/meta.json")"
  token_accounts="$(jq -r '.discovered_token_accounts' "${epoch_out}/meta.json")"
  signature_rows="$(jq -r '.signature_index_rows' "${epoch_out}/meta.json")"
  user_rows="$(jq -r '.user_index_rows' "${epoch_out}/meta.json")"
  account_rows="$(jq -r '.account_index_rows' "${epoch_out}/meta.json")"
  mint_rows="$(jq -r '.mint_index_rows' "${epoch_out}/meta.json")"
  program_rows="$(jq -r '.program_index_rows' "${epoch_out}/meta.json")"

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "${epoch}" "${car}" "${dumped}" "${bytes}" "${users}" "${token_accounts}" \
    "${signature_rows}" "${user_rows}" "${account_rows}" "${mint_rows}" "${program_rows}" >>"${SUMMARY}"
  echo "epoch-${epoch}: dumped=${dumped}, tracked_users=${users}, tracked_token_accounts=${token_accounts}, dump_bytes=${bytes}" >&2
  cp "${epoch_out}/discovered-users.txt" "${SEED_USERS}"
  cp "${epoch_out}/discovered-token-accounts.txt" "${SEED_TOKEN_ACCOUNTS}"
done

echo "summary: ${SUMMARY}"
