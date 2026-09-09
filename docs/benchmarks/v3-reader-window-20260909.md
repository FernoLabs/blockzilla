# V3 reader: concurrent network input — 9 September 2026

The V3 network reader now uses concurrent input workers and reusable semantic-plane and signature buffers. All 12 NAS comparison cases passed. Count scans improved by 13.05×, transaction identity scans by 3.27×, and the single USDC comparison by 3.45×. The new setting is retained.

These are epoch 900 prefix tests, not full-epoch measurements. Count tests read 32,768 blocks; application tests read 8,192 blocks. The old and new settings used the same release executable, with 12 decode workers and fresh caches. No profiler or allocation counter was enabled.

## Transaction rate

The table uses total transactions divided by combined scan time. Startup is excluded. USDC TPS counts transactions scanned, not matching transactions or output rows. Count and transaction identities have two runs per setting; USDC has one.

| Workload | Previous TPS | New TPS | Scan speed gain |
|---|---:|---:|---:|
| count | 200,372 | 2,615,330 | 13.05× |
| transactions | 485,486 | 1,585,553 | 3.27× |
| usdc | 172,228 | 593,686 | 3.45× |

## Read rate and memory

MB/s uses source bytes divided by scan time, with decimal megabytes. Source bytes can include registry reads. Peak RSS is kernel-measured process memory, including setup and all application state. It is not the input-buffer budget.

| Workload | Previous MB/s | New MB/s | Previous peak RSS MiB | New peak RSS MiB |
|---|---:|---:|---:|---:|
| count | 12.55 | 163.80 | 173.9–176.5 | 399.4–399.6 |
| transactions | 46.56 | 152.07 | 202.4–203.1 | 407.5–409.1 |
| usdc | 24.02 | 82.80 | 1849.2 | 1849.1 |

## Every test

Tests ran in the listed order. Total time is the reader-reported setup plus scan time. CPU time is kernel process CPU time, including setup. GET counts cover the scan only.

| Case | Scan s | Setup s | Total s | Scan TPS | MB/s | GETs | CPU s | Peak RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 00-local-count-window | 4.066 | 0.153 | 4.219 | 8,773,328 | 549.50 | 0 | 39.658 | 170.65 |
| 01-local-count-legacy | 3.971 | 0.151 | 4.122 | 8,984,204 | 562.70 | 0 | 40.191 | 170.53 |
| 02-network-count-legacy | 175.356 | 7.325 | 182.681 | 203,427 | 12.74 | 1280 | 40.525 | 173.89 |
| 03-network-count-window | 13.802 | 4.916 | 18.717 | 2,584,636 | 161.88 | 335 | 42.479 | 399.45 |
| 04-network-count-window | 13.478 | 4.710 | 18.188 | 2,646,762 | 165.77 | 335 | 42.335 | 399.63 |
| 05-network-count-legacy | 180.704 | 5.611 | 186.315 | 197,407 | 12.36 | 1280 | 40.396 | 176.47 |
| 06-network-transactions-legacy | 18.871 | 5.909 | 24.781 | 472,983 | 45.36 | 192 | 10.622 | 202.36 |
| 07-network-transactions-window | 4.488 | 5.635 | 10.123 | 1,988,883 | 190.76 | 78 | 10.554 | 409.11 |
| 08-network-transactions-window | 6.771 | 5.521 | 12.292 | 1,318,226 | 126.43 | 78 | 10.578 | 407.47 |
| 09-network-transactions-legacy | 17.899 | 5.984 | 23.883 | 498,668 | 47.83 | 192 | 10.451 | 203.11 |
| 10-network-usdc-legacy | 51.826 | 5.403 | 57.228 | 172,228 | 24.02 | 283 | 13.745 | 1849.23 |
| 11-network-usdc-window | 15.035 | 5.906 | 20.941 | 593,686 | 82.80 | 71 | 14.157 | 1849.13 |

Including setup, the changes were:

| Workload | Previous total TPS | New total TPS | Gain including setup |
|---|---:|---:|---:|
| count | 193,347 | 1,933,181 | 10.00× |
| transactions | 366,836 | 796,423 | 2.17× |
| usdc | 155,968 | 426,236 | 2.73× |

## What changed

The previous V3 input path used one producer, at most 128 blocks per group, a 16 MiB group target, and a 64 MiB input budget. The new path requests up to eight input workers, targets 32 MiB across selected planes and signatures, and reserves at most 256 MiB across reusable input slots. The planner can reduce worker count so all per-plane and signature capacities fit. Groups contain at most 8,192 adjacent selected blocks. No format change or new slot index is required.

Each loader retains one input slot. It fills the slot again only after all job and semantic references are released. Both the semantic-plane vectors and the signature vector are reused. Plane and signature reads within a group remain sequential; different groups load concurrently. The coordinator delivers groups in order. Four-block decode jobs and the existing decode/output admission limits remain unchanged. Sparse gaps are not fetched. Sparse and oversized fallback jobs keep their separate limits.

The count tests used seven input workers and reserved 250,169,262 bytes (238.6 MiB). The legacy input-capacity receipt reports its credit ceiling; the new receipt reports fixed slot capacity. Count GETs fell from 1,280 to 335 for exactly 2,234,237,327 source bytes. Transaction GETs fell from 192 to 78. Both workloads preserve exact source-byte totals. The combination of larger groups and several concurrent reads explains the measured gain; this comparison does not isolate the contribution of each change.

Count CPU time rose only from about 40.5 to 42.4 seconds per process while scan wall time fell from 175–181 to 13.5–13.8 seconds. Transaction CPU time stayed near 10.5 seconds. This supports input scheduling as the main cause of this improvement. It does not prove that all remaining time is network waiting.

## Limits and next measurements

New transaction scans varied from 4.49 to 6.77 seconds. Remote cache state and unrelated NAS activity were not controlled. Only one benchmark process ran at a time. The small sample does not establish a full-epoch or long-run speed guarantee.

The new count read rate is about 164 MB/s. This remains below local count reads at 550–563 MB/s and the earlier 547–558 MB/s download-only result. The latter used sixteen 64 MiB requests, a different region, and discarded bytes without decoding or ordered delivery. These workloads are not equivalent. More concurrency within each plane group and HTTP body-adapter costs remain possible follow-up measurements; no HTTP/2 or compression gain is claimed here.

The 256 MiB limit covers reserved input vectors, not total process RSS. The block index, group-plan metadata, HTTP buffers, registry state, decoded frames, output, and fallback jobs have separate costs. Group-level allocations and HTTP copies remain. The pointer-reuse test supports reuse of large input vectors; these timing runs did not measure total allocation counts.

The [V2 comparison](v2-reader-window-20260909.md) used the same count and transaction prefixes at an earlier time. Its indexed USDC workload differs from this standard V3 USDC workload, so do not compare their USDC TPS as equivalent work.

## Verification

- All 12 receipt files match their SHA-256 hashes. All case checks passed. Both V3 and retained V2 epoch 900 archive inventories stayed unchanged (size, modification time, inode, and device). This inventory check is not a full archive content hash.
- Count: 32,768 blocks and 35,672,161 transactions, with matching instruction and inner-instruction totals across local and network runs.
- Transaction identities: 8,192 blocks and 8,925,832 transactions. All four network outputs match 714,066,608 bytes and SHA-256 `54531e48c946cb66c0c148d7339fdbb11511cffaaec9bc9c5a33f650a10d3685`.
- USDC: both runs match the complete workload report, 174,958 matching transactions, 341,094 failed transactions skipped, 906,065 output rows, 123,224,884 output bytes, and source-byte totals. The sink report does not hash the full USDC output contents.
- Every NAS case stayed within its input capacity and decoded-job limits: at most 24 assigned jobs, 100,000 declared transactions, and 256 MiB declared decoded bytes; the projected-block bound remains 96 at 12 decode workers.
- The focused test compares local, previous remote, and concurrent remote paths for dense and sparse selections. It delays the first directory request until a later request starts, checks buffer address reuse, verifies worker reduction under a 24 KiB budget, and covers input failure and sink cancellation.
- The final reader passes 142 library tests and three public API tests. The HTTP fixture requires a loopback socket; it passed in the authorized full test run. The V3 example and benchmark tool compile. Strict Clippy still reports six pre-existing `chunks_exact` warnings, with none in the new input module.
- The frozen benchmark executable predates the final explicit signature-capacity guard and stronger small-budget test. These changes preserve the default schedule and valid data path. The artifact records both build-time and final source hashes.
- The controller and its readers stopped after completion. The broad benchmark and Jetstreamer remain deferred.

## Reproduce

Build with `cargo build --release -p blockzilla-reader-profile`. Use one process at a time and a fresh cache directory for each network attempt.

```sh
./target/release/blockzilla-reader-profile --format v3 --epoch 900 \
  --workload count --blocks 32768 --workers 12 --iterations 1 --warmups 0 \
  --origin https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev \
  --cache-root /tmp/v3-window-count
```

Add `--v3-legacy-input` and another empty cache directory for the previous schedule. For application tests, use `--workload transactions --blocks 8192` or `--workload usdc --blocks 8192`. The SDK exposes `set_network_input_config`; `None` selects the previous schedule.

Timed executable SHA-256: `b7714fd8db5b0bfc6d849b7c7073d183d036583aa3f5d440e5bc8e44e78890b5`.

[Raw results, build configuration, and verification](artifacts/v3-reader-window-20260909.json). [Input design](../design/v3-concurrent-input.md).
