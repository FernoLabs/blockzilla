#!/usr/bin/env bash
set -euo pipefail

BIN="${BIN:-./target/release/blockzilla}"
CAR_DIR="${CAR_DIR:-/volume1/blockzilla}"
OUT_ROOT="${OUT_ROOT:-/volume1/blockzilla-v2}"
LOG_ROOT="${LOG_ROOT:-/home/ach/dev/archive-v2-runs}"
RUN_ID="${RUN_ID:-$(date +%Y%m%dT%H%M%S%z)}"
RUN_DIR="${RUN_DIR:-$LOG_ROOT/hot-v2-shredding-sidecar-batch-$RUN_ID}"
EPOCHS="${EPOCHS:-10 50 100 200 300 400 500 600 700}"
LEVEL="${LEVEL:-1}"

mkdir -p "$OUT_ROOT" "$RUN_DIR"

if [[ ! -x "$BIN" ]]; then
  cargo build --release -p blockzilla --bin blockzilla
fi

echo "run_id=$RUN_ID"
echo "binary=$BIN"
echo "car_dir=$CAR_DIR"
echo "out_root=$OUT_ROOT"
echo "run_dir=$RUN_DIR"
echo "epochs=$EPOCHS"
echo "level=$LEVEL"
date -Is

for epoch in $EPOCHS; do
  input="$CAR_DIR/epoch-$epoch.car.zst"
  output="$OUT_ROOT/epoch-$epoch"
  epoch_log="$RUN_DIR/epoch-$epoch.log"
  marker="$output/.complete-hot-v2-shredding-sidecar-v2"

  echo
  echo "===== epoch $epoch ====="
  date -Is

  if [[ ! -s "$input" ]]; then
    echo "missing input $input; skipping"
    continue
  fi

  if [[ -e "$marker" && -s "$output/archive-v2-blocks.index" && -s "$output/shredding.wincode" ]]; then
    echo "already complete: $output"
    continue
  fi

  mkdir -p "$output"
  cmd=(
    "$BIN"
    build-archive-v2-hot-blocks
    "$input"
    "$output"
    --level "$LEVEL"
  )

  prev_epoch=$((epoch - 1))
  previous_car="$CAR_DIR/epoch-$prev_epoch.car.zst"
  if [[ -s "$previous_car" ]]; then
    cmd+=(--previous-car "$previous_car")
  else
    echo "previous CAR missing for epoch $epoch: $previous_car"
  fi

  printf 'command:'
  printf ' %q' "${cmd[@]}"
  printf '\n'

  "${cmd[@]}" 2>&1 | tee "$epoch_log"

  if [[ ! -s "$output/archive-v2-blocks.index" || ! -s "$output/shredding.wincode" ]]; then
    echo "epoch $epoch finished without expected hot index/shredding sidecar" >&2
    exit 1
  fi

  date -Is > "$marker"
  du -sh "$output"
  ls -lh \
    "$output/archive-v2-blocks.zstd" \
    "$output/archive-v2-blocks.index" \
    "$output/registry.bin" \
    "$output/signatures.bin" \
    "$output/poh.wincode" \
    "$output/shredding.wincode" \
    "$output/vote_hash_registry.bin" || true
done

echo
echo "batch complete"
date -Is
