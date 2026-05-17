#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_piggy_scan_epochs.sh <start_epoch> <end_epoch> [car_dir] [out_dir]
#
# The scanner prefers epoch-N.car.zst when present, then epoch-N.car.

START_EPOCH="${1:?start epoch required}"
END_EPOCH="${2:?end epoch required}"
CAR_DIR="${3:-./epochs}"
OUT_DIR="${4:-./piggy-analysis/epochs-${START_EPOCH}-${END_EPOCH}}"

BIN="${BIN:-./target/release/of-token-index}"

mkdir -p "${OUT_DIR}"

if [[ ! -x "${BIN}" ]]; then
  cargo build --release -p of-token-index
fi

SUMMARY="${OUT_DIR}/summary.tsv"
COMBINED="${OUT_DIR}/piggy-events-${START_EPOCH}-${END_EPOCH}.jsonl"

: >"${SUMMARY}"
: >"${COMBINED}"
printf "epoch\tcar\tevents\n" >>"${SUMMARY}"

for epoch in $(seq "${START_EPOCH}" "${END_EPOCH}"); do
  zst="${CAR_DIR}/epoch-${epoch}.car.zst"
  raw="${CAR_DIR}/epoch-${epoch}.car"

  if [[ -f "${zst}" ]]; then
    car="${zst}"
  elif [[ -f "${raw}" ]]; then
    car="${raw}"
  else
    printf "%s\tMISSING\t0\n" "${epoch}" >>"${SUMMARY}"
    echo "epoch-${epoch}: missing ${zst} or ${raw}, skipping" >&2
    continue
  fi

  out="${OUT_DIR}/piggy-events-epoch-${epoch}.jsonl"
  echo "epoch-${epoch}: scanning ${car} -> ${out}" >&2
  "${BIN}" piggy-scan --car "${car}" --out "${out}"

  events="$(wc -l <"${out}" | tr -d ' ')"
  printf "%s\t%s\t%s\n" "${epoch}" "${car}" "${events}" >>"${SUMMARY}"
  cat "${out}" >>"${COMBINED}"
done

echo "summary: ${SUMMARY}"
echo "combined: ${COMBINED}"
