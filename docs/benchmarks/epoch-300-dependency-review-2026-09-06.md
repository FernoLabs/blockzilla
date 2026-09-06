# Epoch 300 dependency retest — 6 September 2026

The bounded V2 comparison passed all 24 cases and 60 measured iterations after the dependency update. Exact workload counters, coverage, source-read totals, worker participation, and resource bounds matched the frozen rolling-pipeline build. None of the six median elapsed comparisons reached the 10% investigation threshold. These short runs do not establish a general speed improvement.

Pump.fun made more Rust allocation requests in both worker settings. This cost is recorded below; the result is not a claim that allocations stayed unchanged.

## Scope and method

The test used the first 2,048 blocks of epoch 300, starting at block ordinal zero. USDC, Pump.fun, and indexed USDC each ran with one and twelve workers. Each process had one warmup, followed by three timing iterations or two separate allocation iterations. All 24 processes ran sequentially. Pair order was baseline then candidate at one worker and candidate then baseline at twelve workers. Their combined process wall time was 102.24 seconds.

The baseline was the exact [rolling-pipeline build](epoch-300-rolling-pipeline-2026-09-06.md), run again during this comparison. Both binaries used Rust 1.98.1, release mode, `x86_64-unknown-linux-musl`, and `-C target-feature=+aes,+sse2`. Both used the same immediate atomic allocation counters. Allocation-mode times include counter overhead and are excluded from speed comparisons.

The [dependency review](../reference/dependency-review-2026-09-06.md) records the version changes and compatibility checks. The candidate was built after the full workspace test command exited successfully: 3,568 passed, none failed, one ignored, across 128 test harnesses. Source snapshots before and after the Linux build differed only in documentation.

## Timing

Times below are seconds: median, then minimum–maximum. A negative change means a lower observed median. The three short, warm samples per case do not support a full-epoch performance claim.

| Workload | Workers | Prior build, seconds | Updated build, seconds | Median change |
| --- | ---: | ---: | ---: | ---: |
| USDC | 1 | 1.837906 (1.784385–1.845800) | 1.793072 (1.754612–1.800911) | -2.44% |
| USDC | 12 | 0.501321 (0.489867–0.504680) | 0.495962 (0.487793–0.497382) | -1.07% |
| Pump.fun | 1 | 2.050579 (2.046249–2.081277) | 2.035041 (2.010945–2.046279) | -0.76% |
| Pump.fun | 12 | 0.531310 (0.530463–0.537801) | 0.524272 (0.519519–0.532220) | -1.32% |
| Indexed USDC | 1 | 1.898150 (1.855529–1.901230) | 1.820100 (1.777370–1.825448) | -4.11% |
| Indexed USDC | 12 | 0.512130 (0.509850–0.520700) | 0.499565 (0.497821–0.514288) | -2.45% |

## Rust allocation requests

These are medians from two separately instrumented iterations. Calls include successful allocation, zeroed-allocation, and reallocation requests. Requested bytes are cumulative; they are neither live memory nor peak RSS. Allocations inside C zstd are not counted. The full ranges and size-bucket checks are retained in the [JSON results](epoch-300-dependency-review-2026-09-06.json).

| Workload | Workers | Prior calls | Updated calls | Call change | Requested-byte change |
| --- | ---: | ---: | ---: | ---: | ---: |
| USDC | 1 | 431 | 429.5 | -0.35% | -0.28% |
| USDC | 12 | 3,413.5 | 3,506.5 | +2.72% | -0.37% |
| Pump.fun | 1 | 302,878 | 307,652.5 | +1.58% | +0.31% |
| Pump.fun | 12 | 402,264 | 417,416 | +3.77% | +4.26% |
| Indexed USDC | 1 | 345 | 339 | -1.74% | -0.18% |
| Indexed USDC | 12 | 1,608.5 | 1,672 | +3.95% | +1.57% |

Pump.fun call ranges did not overlap: 302,383–303,373 versus 305,643–309,662 at one worker, and 399,990–404,538 versus 414,947–419,885 at twelve workers. Its twelve-worker requested-byte ranges also did not overlap: 415,390,876–420,236,092 versus 434,756,903–436,488,942 bytes. The one-worker byte ranges overlapped. The twelve-worker USDC and indexed-USDC call and byte ranges overlapped. Rolling worker assignment can vary buffer growth, but this test does not establish the cause of the Pump.fun increase.

## Correctness and source checks

Every measured iteration read 2,048 blocks and 3,274,503 transactions. All workloads omitted the same 554,678 known failed transactions and reported complete coverage with zero indeterminate transactions. Exact historical counter and coverage strings matched in every case.

| Check | USDC and indexed USDC | Pump.fun |
| --- | ---: | ---: |
| Matching transactions | 22,505 | 0 |
| Selected pre / post balance rows | 55,273 / 55,534 | — |
| Logical source bytes | 654,045,903 | 221,283,307 |
| Logical source calls | 69 | 54 |
| Decoded instructions | 0 | 3,253,320 |

The canonical USDC sink counted 110,807 rows and 15,069,796 bytes. The indexed sink counted the same rows, 7,756,566 data bytes, and 6,279 dictionary entries using 376,816 bytes. No unavailable token-balance or mint evidence was reported. These are sink counters; this profiler run did not write and compare complete output files. Pump.fun had no matches, so it did not test the cost of writing matched transactions.

All 60 measurements used the requested active worker count. Observed in-flight peaks were eight blocks, 20,034 transactions, and 20,914,414 declared bytes at one worker. At twelve workers they were 96 blocks, at most 131,072 transactions, and 50,493,562 declared bytes. These satisfy the block, transaction, and 64 MiB normal declared-byte windows. Aggregate peaks do not prove the timing of large-block isolation; deterministic local pipeline tests cover that rule.

The folder scan found all eleven sample epochs under CAR, V2, and V3. V2 epoch 300 kept the same eleven filenames, sizes, and modification times as the saved baseline. Device/inode and nanosecond change/modification records were also unchanged across this run. The two small control-file hashes matched:

- `archive-v2-blocks.index`: `78570dbf0ce8e183ebbe895a1096f20802d1fcd1504971110803b39d9ffbab80`.
- `archive-v2-meta.wincode`: `36fbc15de3d4f6b169f8769c170cf1c1b2a2d74e118dcfa503755b12038c85d4`.

This is file-stat continuity plus control-file hash evidence. It is not a new content hash of all payload or registry files. The completed raw `epoch-300.car` was present at 508,337,873,180 bytes; this V2 retest did not scan it or repeat its earlier decompressor verification. No archive file was changed.

## Host conditions and limits

No copy or compression process appeared in 62 host samples. One-minute load ranged from 0.12 to 2.67. Available memory ranged from 3,051,540 to 3,643,860 KiB. The largest sampled process RSS was 557,804 KiB (about 545 MiB); sampling can miss a true peak. CPU pressure `some avg10` reached 7.85%, I/O pressure reached 0.50%, and sampled memory pressure remained zero. Logical source bytes are not physical disk-read bytes.

No cache was flushed, and host load was not controlled. A 10% elapsed increase is an investigation threshold, not a statistical regression test. This comparison covers V2 prefixes only. It does not repeat full-epoch output-byte checks, validate V3 performance, or measure a raw-CAR baseline. The earlier full-epoch report remains evidence for its own recorded source and binary hashes.

## Provenance

| Item | SHA-256 |
| --- | --- |
| Baseline profiler | `d6f838c87a689198d7cbc425da48665a03422b358e491967806cbdbcb79f067b` |
| Updated profiler | `843beafbfca4ed00754fb6264431e5ce1d0644b3cf3574d6250e2b46de11be76` |
| Allocation-counter source | `ff6c5d7acb061e74a2f530473c8fd8150fc6ba26cc12606cca5375bc76744442` |
| Candidate Cargo.lock | `3aebe00fe33070a00f89a58d35f5aae51da321526651c10eff75ff794295d734` |

The [JSON results](epoch-300-dependency-review-2026-09-06.json) retain exact timing/allocation ranges, source records, checks, build identities, and evidence hashes. The NAS control and result directories are `epoch300-dependency-review-20260906T200823Z` under `/volume2/blockzilla-bench/control/` and `/volume2/blockzilla-bench/results/`. The NAS received only the required profiler and control files. The full source patch, source manifests, and build/test logs remain in the local package at `target/nas-validation/epoch300-dependency-review-20260906T200823Z/`. The reduced bundle records their relevant digests; it is hash-verified, not signed.
