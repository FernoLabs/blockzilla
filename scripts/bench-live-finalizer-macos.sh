#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  cat >&2 <<'USAGE'
usage: scripts/bench-live-finalizer-macos.sh <live-capture-dir> [output-dir]

Benchmarks the local live-capture finalizer path on macOS.

env:
  MAX_BLOCKS=50000          Limit hot-write rows for iteration; unset for full capture.
  LEVEL=1                   Per-block zstd level.
  REGISTRY_MODES="counts runs scan"
                            Registry sources to compare. Use "counts runs touches scan auto".
  RUN_REUSE=1               Re-run counts output to measure hot-write with registry reused.
  FLAMEGRAPH=0              Also run cargo-flamegraph for counts-reuse, if installed.
  RUSTFLAGS_EXTRA=""        Extra RUSTFLAGS appended after frame pointers.

Notes:
  - counts uses <capture>/index/pubkey-counts.bin and should avoid the registry scan.
  - runs uses <capture>/index/pubkey-runs/*.bin and should avoid the raw touch sort.
  - touches uses <capture>/index/pubkey-touches.bin and externally sorts raw touches.
  - scan decodes live-no-registry-blocks.bin to rebuild the registry.
  - output sidecars are hard-linked when possible, so sample runs do not copy full PoH.
USAGE
  exit 2
fi

capture_dir="$1"
output_root="${2:-target/bench-live-finalizer-macos}"
max_blocks="${MAX_BLOCKS:-}"
level="${LEVEL:-1}"
registry_modes="${REGISTRY_MODES:-counts runs scan}"
run_reuse="${RUN_REUSE:-1}"
flamegraph="${FLAMEGRAPH:-0}"

bin="target/release/blockzilla"
mkdir -p "$output_root"

if [[ ! -s "$capture_dir/blocks/live-no-registry-blocks.bin" ]]; then
  echo "missing live block file: $capture_dir/blocks/live-no-registry-blocks.bin" >&2
  exit 1
fi
if [[ ! -s "$capture_dir/index/blockhash_registry.bin" ]]; then
  echo "missing blockhash registry: $capture_dir/index/blockhash_registry.bin" >&2
  exit 1
fi
if [[ ! -s "$capture_dir/poh/poh.wincode" ]]; then
  echo "missing PoH sidecar: $capture_dir/poh/poh.wincode" >&2
  exit 1
fi

max_blocks_args=()
if [[ -n "$max_blocks" ]]; then
  max_blocks_args=(--max-blocks "$max_blocks")
fi

export CARGO_PROFILE_RELEASE_DEBUG="${CARGO_PROFILE_RELEASE_DEBUG:-true}"
export RUSTFLAGS="-C force-frame-pointers=yes ${RUSTFLAGS_EXTRA:-}"

echo "== build =="
cargo build --release -p blockzilla --bin blockzilla

run_one() {
  local label="$1"
  shift
  local out_dir="$output_root/$label"
  local log="$output_root/$label.log"
  rm -rf "$out_dir"
  mkdir -p "$out_dir"
  echo "== $label =="
  /usr/bin/time -l "$bin" \
    build-archive-v2-hot-blocks-from-live \
    "$capture_dir" \
    "$out_dir" \
    --level "$level" \
    "${max_blocks_args[@]}" \
    "$@" 2>&1 | tee "$log"
}

for mode in $registry_modes; do
  run_one "registry-$mode" --registry-source "$mode"
done

if [[ "$run_reuse" == "1" ]]; then
  reuse_dir="$output_root/registry-counts"
  if [[ ! -d "$reuse_dir" ]]; then
    echo "counts output missing; running it first for reuse benchmark" >&2
    run_one "registry-counts" --registry-source counts
  fi
  echo "== counts-reuse-hot-write =="
  /usr/bin/time -l "$bin" \
    build-archive-v2-hot-blocks-from-live \
    "$capture_dir" \
    "$reuse_dir" \
    --level "$level" \
    "${max_blocks_args[@]}" \
    --registry-source counts 2>&1 | tee "$output_root/counts-reuse-hot-write.log"
fi

if [[ "$flamegraph" == "1" ]]; then
  if cargo flamegraph --help >/dev/null 2>&1; then
    fg_dir="$output_root/flamegraph-counts-reuse"
    mkdir -p "$fg_dir"
    echo "== flamegraph counts-reuse-hot-write =="
    cargo flamegraph -p blockzilla --bin blockzilla \
      --output "$fg_dir/flamegraph.svg" \
      -- build-archive-v2-hot-blocks-from-live \
      "$capture_dir" \
      "$output_root/registry-counts" \
      --level "$level" \
      "${max_blocks_args[@]}" \
      --registry-source counts
  else
    echo "cargo-flamegraph is not installed; install with: cargo install flamegraph" >&2
  fi
fi

echo "== summary logs =="
ls -1 "$output_root"/*.log
