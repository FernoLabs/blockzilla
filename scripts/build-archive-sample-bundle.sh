#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 1 ]]; then
  printf 'usage: %s NEW_PACKAGE_DIRECTORY\n' "$0" >&2
  exit 2
fi
repo=$(cd "$(dirname "$0")/.." && pwd)
mkdir "$1"
bundle=$(cd "$1" && pwd)
cd "$repo"
target=${CARGO_TARGET_DIR:-$repo/target}
bins=(read-car read-car-usdc read-car-pumpfun read-car-firewatch)
for format in compact-v2 archive-v3; do
  for workload in slot-hours usdc pumpfun firewatch; do
    bins+=("read-$format-$workload")
  done
done
args=()
for bin in "${bins[@]}"; do args+=(--bin "$bin"); done
env -u CPATH -u C_INCLUDE_PATH -u CPLUS_INCLUDE_PATH -u LIBRARY_PATH -u SDKROOT \
  RUSTFLAGS='-C target-feature=+aes,+sse2' cargo build --release --locked \
  --target x86_64-unknown-linux-musl \
  -p blockzilla-read-car -p blockzilla-read-compact-v2 -p blockzilla-read-archive-v3 "${args[@]}"
mkdir "$bundle/bin"
for bin in "${bins[@]}"; do cp "$target/x86_64-unknown-linux-musl/release/$bin" "$bundle/bin/"; done
cp scripts/run-archive-sample-read-matrix.sh scripts/archive_sample_matrix.py \
  scripts/prepare-nas-sample-mirror.sh scripts/build-archive-sample-bundle.sh \
  scripts/test_archive_sample_matrix.py "$bundle/"
chmod +x "$bundle/"*.sh
git rev-parse HEAD > "$bundle/source-revision.txt"
git diff --binary HEAD > "$bundle/source-changes.patch"
# Include untracked runner sources as well as the patch of tracked files.
cp examples/workloads/src/progress.rs "$bundle/progress.rs"
cp crates/compact-v2/blockzilla-compact-v2-reader/src/query_keys.rs "$bundle/query_keys.rs"
cp crates/compact-v2/blockzilla-compact-v2-reader/src/count_projection.rs "$bundle/count_projection.rs"
cp crates/blockzilla-model/src/projection_pool.rs "$bundle/projection_pool.rs"
cp -R bench/reader-profile "$bundle/reader-profile-source"
cp docs/benchmarks/sample-reader-matrix.md "$bundle/README.md"
rustc --version > "$bundle/compiler.txt"
printf 'Built 12 Linux readers in %s\nNo benchmark was started.\n' "$bundle"
