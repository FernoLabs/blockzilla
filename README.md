# Blockzilla

Blockzilla is Ferno's Solana archive tooling. The current project focus is Archive V2: a compact, indexed, block-oriented replacement for the old `compact.bin` experiments.

This repository contains:

- CAR readers and Old Faithful compatibility helpers.
- Archive V2 builders, repackers, analyzers, and read benchmarks.
- Token instruction/event dumpers used to benchmark real index workloads such as USDC.
- Cloudflare worker and slot-index helpers for serving historical `getBlock`.

## Current Benchmark Snapshot

The latest lean benchmark report is in
[docs/blockzilla-lean-storage-read-getblock-report-current-20260524.md](docs/blockzilla-lean-storage-read-getblock-report-current-20260524.md).

Headline results from the current-code snapshot:

- Epoch 920 token instruction scan: **1,025 blocks/s**, **1.47M tx/s** on one thread.
- Epoch 920 Pump.fun scan: **1,017 blocks/s** on one thread, scaling to **3,959 blocks/s** and **5.69M tx/s** on eight threads.
- Epoch 10 reader comparison: Blockzilla hot blocks read at **84.3k blocks/s** for token scanning versus **17.4k blocks/s** from CAR and **26.7k blocks/s** from CAR.zst.
- `getBlock` 132-slot sample: Blockzilla averaged **483 ms** for `full`, **228 ms** for `signatures`, **274 ms** for `accounts`, and **1,030 ms** for `full + rewards`.
- `blockBin` averaged **217 ms** without materialization and **389 ms** with materialization.

## Current CLI

Use the top-level `blockzilla` binary for new work:

```bash
cargo run --release -p blockzilla -- --help
cargo run --release -p blockzilla -- build-archive-v2-hot-blocks \
  /srv/blockzilla/blockzilla/epoch-800.car.zst \
  /home/blockzilla/dev/blockzilla-v2/epoch-800 \
  --level 1
```

The old `of-archive-importer` binary still exists for compatibility with older scripts, but new Archive V2 and token-benchmark commands should live under `blockzilla`.

## Archive V2 Layout

The current production-shaped epoch directory is:

```text
epoch-N/
  archive-v2-blocks.zstd        # one independent zstd frame per block
  archive-v2-blocks.index       # external block offset/size table
  archive-v2-meta.wincode       # archive-level records

  registry.bin                  # epoch pubkey registry
  registry_counts.bin           # usage counts for registry ordering
  blockhash_registry.bin         # PoH blockhash registry
  prev_blockhash_tail.bin        # recent blockhash seed handoff
  vote_hash_registry.bin         # vote-program hash sidecar

  signatures.bin
  poh.wincode
  shredding.wincode
```

Alternative benchmark layouts are also supported:

- `archive-v2-blocks.wincode`: raw uncompressed block blobs with the same index shape.
- `archive-v2-blocks.wincode.zst`: whole-file zstd over the raw block stream.
- CAR/CAR.ZST inputs for baseline read and token-index comparisons.

We will keep adding measured compression ratios and read speeds as the benchmark matrix stabilizes.

## Common Commands

Build a hot-block Archive V2 epoch:

```bash
cargo run --release -p blockzilla -- build-archive-v2-hot-blocks \
  /srv/blockzilla/blockzilla/epoch-900.car.zst \
  /home/blockzilla/dev/blockzilla-v2/epoch-900 \
  --level 1
```

Repack block-zstd to raw blocks plus whole-file zstd:

```bash
cargo run --release -p blockzilla -- repack-archive-v2-hot-blocks-raw \
  /home/blockzilla/dev/blockzilla-v2/epoch-900/archive-v2-blocks.zstd \
  /home/blockzilla/dev/blockzilla-v2/epoch-900-raw \
  --index /home/blockzilla/dev/blockzilla-v2/epoch-900/archive-v2-blocks.index \
  --level 1
```

Benchmark block reads:

```bash
cargo run --release -p blockzilla -- bench-archive-v2-hot-blocks \
  /home/blockzilla/dev/blockzilla-v2/epoch-900/archive-v2-blocks.zstd \
  --workers 4

cargo run --release -p blockzilla -- bench-archive-v2-hot-blocks-raw \
  /home/blockzilla/dev/blockzilla-v2/epoch-900-raw/archive-v2-blocks.wincode \
  --workers 8
```

Run the token-instruction dump benchmark:

```bash
cargo run --release -p blockzilla -- dump-token-instructions \
  /home/blockzilla/dev/blockzilla-v2/epoch-900/archive-v2-blocks.zstd \
  /home/blockzilla/dev/token-dumps/epoch-900-hot-zstd-w4 \
  --registry /home/blockzilla/dev/blockzilla-v2/epoch-900/registry.bin \
  --workers 4 \
  --no-output
```

For CAR inputs, pass the matching Blockzilla registry so event rows can use the same account ids:

```bash
cargo run --release -p blockzilla -- dump-usdc-token-events \
  /srv/blockzilla/blockzilla/epoch-900.car \
  /home/blockzilla/dev/token-dumps/epoch-900-car \
  --format car \
  --registry /home/blockzilla/dev/blockzilla-v2/epoch-900/registry.bin
```

## Target Machine

The current benchmark target is a UGREEN NASync DXP8800 class machine with:

- 8 x Seagate Exos ST28000NM000C 28 TB HDDs.
- 2 x Crucial P310 4 TB PCIe Gen4 NVMe SSDs used as cache.
- 8 GB RAM, so builders must stay conservative with resident memory and avoid unnecessary concurrent disk-heavy jobs.

This hardware shape matters: sequential throughput is good, memory is tight, and benchmark jobs should usually serialize full-epoch repacks/builds unless we are explicitly testing disk contention.

## Development Notes

- `crates/blockzilla-format` owns the Archive V2 data types, sidecar names, registry helpers, and readers/writers that should be shared by all binaries.
- `crates/old-faithful/car-reader` owns CAR/CAR.ZST parsing and Old Faithful quirks.
- `blockzilla` owns user-facing Archive V2 build, repack, analysis, and token benchmark commands.
- `crates/old-faithful/*` should stay focused on Old Faithful compatibility, slot indexes, and worker support.

Legacy `compact.bin` analyzers still exist as `analyze-compact` and `dump-compact-log-strings`, but they are not part of the current production path.
