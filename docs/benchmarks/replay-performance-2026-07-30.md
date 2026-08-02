# Replay performance baseline — 2026-07-30

This baseline measures Blockzilla Compact Archive V2 replay and the in-process
canonical Bank account table. It does not use CAR input.

## Test host

- NAS CPU: Intel Core i5-1235U, x86-64, 10 cores / 12 threads
- OS: Linux 6.12.30+
- Replay pinned to logical CPU 2
- Rust 1.96 release build with `RUSTFLAGS=-Ctarget-cpu=native`
- Replay binary SHA-256:
  `a4bb63a71b9379e65b50c7729dc73d3a30cda26f2f87c00d518f42bfb892b671`
- Account benchmark binary SHA-256:
  `fcc5b7c6776ddc84ea646a5d2b947987aa86912ed89eec95f3ed8e80b4b60d01`

## End-to-end Compact replay

`blocks/s` counts present Compact block rows, not empty ledger slots. Archive
throughput is the sum of compressed `blocks.bin` frame lengths divided by
Compact visit time; index, registry, manifest, and genesis sidecars are
excluded.

| Range | Live Bank accounts | Present blocks/s | Transactions/s | Compressed payload GB/s | Measurement |
|---|---:|---:|---:|---:|---|
| Epoch 0 | 441 → 449 | 241,016 | 963,329 | 0.041353 | New generation telemetry |
| Epoch 1 | 449 → 637 | 47,396 | 1,381,483 | 0.022087 | New generation telemetry |
| Epoch 74 | about 32,249 → 41,281 | about 149 | about 20,755 | about 0.000695 | Reconstructed from the prior full-run elapsed time |
| Epoch 75 | 41,281 → 48,321 | about 142 | about 20,500 | about 0.000679 | Reconstructed from the prior full-run elapsed time |
| Epoch 76, first 20,000 blocks | 48,321 → 48,470 | about 108 | about 15,583 | about 0.000544 | Median replay time with the 23.058 s checkpoint restore excluded |

Epoch 0 and 1 are vote-heavy launch-era workloads and are not representative
of later BPF-heavy epochs. The low 0.0005–0.0007 GB/s rates in the later runs
show that NAS sequential bandwidth is not currently saturated.

## Account-table cardinality sweep

The synthetic workload used 128-byte account data and five timed rounds. Setup,
workload construction, canonical hashing, and batch staging were excluded from
timed regions. Every mutation workload was reversed and the canonical pre/post
SHA-256 was identical.

| Live accounts | Lookup hit ns/op | Lookup M/s | Direct update ns/op | Direct update M/s | Batch commit M writes/s |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 6.7 | 148.93 | 9.3 | 107.36 | 21.10 |
| 10,000 | 14.5 | 69.09 | 16.4 | 61.03 | 20.51 |
| 54,339 | 51.2 | 19.52 | 54.1 | 18.49 | 22.33 |
| 100,000 | 65.8 | 15.19 | 71.6 | 13.97 | 22.40 |
| 250,000 | 79.6 | 12.56 | 91.6 | 10.91 | 21.94 |
| 500,000 | 87.1 | 11.48 | 102.9 | 9.71 | 21.73 |
| 1,000,000 | 92.1 | 10.86 | 119.4 | 8.38 | 21.63 |

All isolated store-operation regions reported zero allocations. The direct
update workload uses random existing keys and captures the cache-locality cost
of registry growth. The batch workload repeatedly publishes a pre-staged
256-write hot set, so it is a publication ceiling rather than a random-key
scaling result.

At the current 54,339-account state size, the raw table can perform roughly
950 random lookups for every transaction sustained by the epoch-75 replay.
Hash lookup and in-place mutation are therefore not the present bottleneck.
Likely remaining costs are Compact decode, instruction dispatch/BPF execution,
transaction overlay construction, account-data cloning, and cumulative diff
tracking.

## Telemetry contract

Pass `--generation-metrics` to `replay-compact-chain` or
`resume-compact-chain`. Every completely consumed sealed generation emits one
machine-parseable `generation_metrics` line containing:

- present blocks/s, transactions/s, instructions/s, and compressed payload GB/s;
- Compact decode/visit, replay, checkpoint encode/publish/hash, and wall time;
- live Bank-account and cumulative changed-key cardinality at both boundaries;
- committed/failed transaction and instruction deltas; and
- generic atomic account-batch counts, write categories, and commit time.

Generic batch timing does not include direct Vote writes, Compact balance
reconciliation, or Bank-owned sysvar writes. Metrics are optional and do not
change the runtime profile or checkpoint bytes. The disabled generic path does
not read clocks or invoke callbacks.
