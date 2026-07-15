# Live finalizer benchmark notes

Goal: make `build-archive-v2-hot-blocks-from-live` faster than CAR hot compaction by avoiding work the live capture already did.

## Current bottleneck hypothesis

- Registry scan should not decode every `live-no-registry-blocks.bin` frame when live capture already writes `index/pubkey-counts.bin` or sorted `index/pubkey-runs/*.bin`.
- Epoch-boundary sidecar transfer should not copy large PoH/blockhash files when hard links are possible.
- Hot write still has unavoidable work: replace raw pubkeys with registry ids, build hot block rows, write signatures, serialize each hot block, zstd-compress each block, and write the block/index/meta files.

## Patch under test

- `build-archive-v2-hot-blocks-from-live --registry-source counts|runs|touches|scan|auto`
  - `counts`: build `registry.bin` and `registry_counts.bin` from `index/pubkey-counts.bin`.
  - `runs`: build by merging raw sorted `(pubkey[32], count:u32)` run files from `index/pubkey-runs/`.
  - `touches`: build by external sorting raw 32-byte touches from `index/pubkey-touches.bin`.
  - `scan`: old behavior, scan/decode live blocks to count pubkeys.
  - `auto`: prefer bounded runs, then counts, then touches, fallback to scan.
- Live PoH and blockhash sidecars now use hard-link-first/copy-fallback.
- Live hot write logs timing totals for optimize, hot block build internals, serialization, zstd, writes, and zstd buffer growth.
- The managed path runs registry merge, MPHF build, and hot rewrite as separate
  processes. Live stream buffers are 8 MiB, pubkey-run cursors are 64 KiB, and
  the controller admits each stage against `MemAvailable` before launch.

## Local run

```bash
MAX_BLOCKS=50000 \
REGISTRY_MODES="counts scan" \
RUN_REUSE=1 \
scripts/bench-live-finalizer-macos.sh /path/to/live-capture target/bench-live-finalizer-macos
```

To compare the low-memory paths:

```bash
MAX_BLOCKS=50000 \
REGISTRY_MODES="runs touches scan" \
RUN_REUSE=0 \
scripts/bench-live-finalizer-macos.sh /path/to/live-capture target/bench-live-finalizer-macos
```

Top-K registry overlap, useful for deciding how effective
`--pubkey-hot-registry ... --pubkey-hot-count 1000` will be:

```bash
rustc -O scripts/registry_topk_overlap.rs -o target/registry_topk_overlap
target/registry_topk_overlap --registry-root /path/to/blockzilla-v2 --top-k 1000
```

Optional flamegraph:

```bash
cargo install flamegraph
MAX_BLOCKS=50000 FLAMEGRAPH=1 \
scripts/bench-live-finalizer-macos.sh /path/to/live-capture target/bench-live-finalizer-macos
```

Use a local SSD sample first. Do not benchmark directly over SSHFS unless the goal is to measure network/filesystem latency.
