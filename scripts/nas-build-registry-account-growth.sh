#!/usr/bin/env bash
set -euo pipefail

REGISTRY_ROOT="${REGISTRY_ROOT:-/home/ach/dev/blockzilla-v2}"
OUT_CSV="${OUT_CSV:-/home/ach/dev/archive-v2-runs/registry-account-growth/epoch-account-growth.csv}"
WORK_DIR="${WORK_DIR:-/home/ach/dev/archive-v2-runs/registry-account-growth/work}"
START_EPOCH="${START_EPOCH:-0}"
END_EPOCH="${END_EPOCH:-}"
BUCKET_BITS="${BUCKET_BITS:-13}"
EXACT="${EXACT:-0}"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

args=(
  "$REPO_ROOT/scripts/registry_account_growth.py"
  --registry-root "$REGISTRY_ROOT"
  --out-csv "$OUT_CSV"
  --start-epoch "$START_EPOCH"
  --logs-root "/home/ach/dev/archive-v2-runs"
)

if [[ -n "$END_EPOCH" ]]; then
  args+=(--end-epoch "$END_EPOCH")
fi

if [[ "$EXACT" == "1" ]]; then
  args+=(--exact --work-dir "$WORK_DIR" --bucket-bits "$BUCKET_BITS")
fi

python3 "${args[@]}"

