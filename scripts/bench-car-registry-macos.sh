#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <epoch.car|epoch.car.zst> [output-dir]" >&2
  echo "env: MAX_BLOCKS=10000 INITIAL_CAPACITY=8000000 HH_CAPS='32768 262144 1048576'" >&2
  exit 2
fi

car_path="$1"
output_dir="${2:-target/bench-car-registry-macos}"
initial_capacity="${INITIAL_CAPACITY:-8000000}"
hh_caps="${HH_CAPS:-32768 262144 1048576}"

mkdir -p "$output_dir"
cargo build --release -p blockzilla --bin blockzilla

max_blocks_args=()
if [[ -n "${MAX_BLOCKS:-}" ]]; then
  max_blocks_args=(--max-blocks "$MAX_BLOCKS")
fi

run_one() {
  local label="$1"
  shift
  echo "== $label =="
  /usr/bin/time -l target/release/blockzilla bench-car-registry \
    "$car_path" \
    --initial-capacity "$initial_capacity" \
    "${max_blocks_args[@]}" \
    "$@" 2>&1 | tee "$output_dir/$label.log"
}

run_one exact-old --strategy exact-old
run_one exact-stream --strategy exact-stream

for cap in $hh_caps; do
  run_one "unique-space-saving-$cap" \
    --strategy unique-space-saving \
    --heavy-hitter-capacity "$cap"
done
