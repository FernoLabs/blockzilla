# V2 reader: concurrent network input — 9 September 2026

The V2 network query reader now downloads larger adjacent ranges with eight input workers. It reuses shared compressed buffers within a 256 MiB capacity limit. Decode jobs use slices of these buffers. The local read schedule and decoded-output limits remain unchanged. The new network setting is enabled by `CompactV2ParallelScanConfig::new`; set `network_input` to `None` to use the previous schedule.

On the NAS, all 12 comparison cases passed their checks. Count scans were 2.38× faster, transaction identity scans were 1.78× faster, and one indexed USDC scan was 1.78× faster. These are epoch 900 prefix tests, not full-epoch measurements. The change is retained. V3 is outside this comparison.

## Scan throughput

Rates below use total work divided by combined scan time for each group. They do not use the arithmetic mean of individual rates. MB/s uses decimal megabytes. Startup and sidecar downloads are excluded here. TPS for USDC means transactions scanned, not matching transactions or output rows.

| Network workload | Blocks per run | Runs per setting | Previous TPS | New TPS | Speed gain | Previous MB/s | New MB/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| count | 32,768 | 2 | 503,044 | 1,195,942 | 2.38× | 62.99 | 149.75 |
| transactions | 8,192 | 2 | 366,398 | 651,085 | 1.78× | 70.34 | 125.00 |
| usdc | 8,192 | 1 | 561,363 | 1,001,213 | 1.78× | 126.41 | 225.46 |

The count result varied: new scans took 22.16–37.49 s, compared with 67.92–73.91 s before. New transaction scans took 13.18–14.24 s, compared with 21.93–26.79 s before. The small sample does not establish a long-run service rate.

## Every test

Tests ran in the order below. Total time is the reader-reported setup plus scan time. CPU is kernel-reported process CPU time, including setup. RSS is kernel peak process memory; it includes registries, decoded data, output, and HTTP memory as well as input buffers. GET counts cover the scan only.

| Case | Scan s | Setup s | Total s | Scan TPS | Scan MB/s | GETs | CPU s | Peak RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 00-local-count-window | 5.023 | 0.035 | 5.058 | 7,101,927 | 889.27 | 0 | 54.879 | 77.92 |
| 01-local-count-legacy | 4.919 | 0.028 | 4.947 | 7,252,635 | 908.14 | 0 | 54.614 | 77.98 |
| 02-network-count-legacy | 73.906 | 23.832 | 97.738 | 482,672 | 60.44 | 550 | 57.628 | 85.11 |
| 03-network-count-window | 22.164 | 16.079 | 38.245 | 1,609,459 | 201.53 | 148 | 63.298 | 306.57 |
| 04-network-count-window | 37.491 | 21.469 | 58.960 | 951,479 | 119.14 | 148 | 62.780 | 303.81 |
| 05-network-count-legacy | 67.920 | 20.410 | 88.330 | 525,210 | 65.76 | 550 | 57.289 | 82.96 |
| 06-network-transactions-legacy | 21.932 | 16.993 | 38.925 | 406,981 | 78.13 | 156 | 14.678 | 136.65 |
| 07-network-transactions-window | 14.235 | 13.995 | 28.230 | 627,016 | 120.38 | 55 | 14.035 | 356.91 |
| 08-network-transactions-window | 13.183 | 21.925 | 35.108 | 677,076 | 129.99 | 55 | 14.248 | 353.37 |
| 09-network-transactions-legacy | 26.790 | 18.279 | 45.069 | 333,175 | 63.96 | 156 | 14.521 | 136.30 |
| 10-network-usdc-legacy | 15.900 | 18.355 | 34.256 | 561,363 | 126.41 | 142 | 18.379 | 960.62 |
| 11-network-usdc-window | 8.915 | 13.827 | 22.742 | 1,001,213 | 225.46 | 41 | 19.809 | 1174.28 |

Including setup, combined count throughput rose from 383,431 to 733,959 TPS (1.91×). Transaction throughput rose from 212,536 to 281,847 TPS (1.33×). USDC throughput rose from 260,563 to 392,479 TPS (1.51×). Each network attempt used a fresh private cache and downloaded 912,008,526 setup bytes. Setup took 13.8–23.8 s. Remote cache conditions were not controlled.

## Why the reader improved

The slot index already has the offsets and lengths needed for exact block reads. The previous download plan was coupled to small decode batches. The new plan joins adjacent compressed ranges up to a 32 MiB target, without crossing gaps or splitting frames. Count GETs fell from 550 to 148 for the same 4,466,690,123 bytes. Transaction GETs fell from 156 to 55 for the same 1,713,623,710 bytes. USDC GETs fell from 142 to 41 for the same 2,009,994,660 source bytes; that total includes registry reads.

The fixed input pool includes free, downloading, queued, and decoder-held vectors. A buffer returns to the pool when its last consumer releases it. The measured reserved compressed capacity stayed below 256 MiB. Decode admission stayed within 96 blocks, 131,072 transactions, and 64 MiB of declared uncompressed input for every NAS case. Up to 12 decode workers ran. This is borrowed decoding within the SDK; the current blocking HTTP body adapter still copies into caller buffers.

Count peak RSS rose from 83–85 MiB to 304–307 MiB. Transaction peak RSS rose from 136–137 MiB to 353–357 MiB. USDC rose from 961 MiB to 1,174 MiB; its registry and application state add memory beyond the input pool. The 256 MiB limit is not a whole-process memory limit. Local count runs used about 78 MiB in both modes and measured 7.10 versus 7.25 million TPS; one run each cannot establish a local speed change.

## What still limits speed

New count scans spent 18.14 and 33.48 s waiting for input out of 22.16 and 37.49 s. Download delivery remains the main measured wait. CPU use alone does not explain this gap: total count process CPU time rose from about 57 s to 63 s while wall time fell.

For transaction identities, signature reads took 11.64 and 11.32 s out of 14.24 and 13.18 s. These reads occur in the ordered publisher in `compact_query.rs`, after block projection. The larger block input window does not make signature reads concurrent. A bounded signature-prefetch path is the next direct target. The scan-stage clocks overlap; do not add them to derive total time.

The [download-only test](v2-download-only-20260909.md) reached 547–558 MB/s with sixteen 64 MiB requests over a different short region. It discarded input immediately. This reader uses eight smaller ranges, retains input for consumers, decodes it, and publishes ordered results. The download-only number is a reference, not an equivalent workload. This change keeps the existing HTTP client and protocol. No HTTP/2 or compression benefit is claimed.

## Method and correctness

- NAS: `Blockzilla-00`, epoch 900, 12 decode workers, release Linux musl binary. One test process at a time. No allocation counters or CPU profiler in timing runs.
- Count: first 32,768 blocks, 35,672,161 transactions; local/new/previous comparisons match block, transaction, instruction, and inner-instruction counts.
- Transaction identities: first 8,192 blocks, 8,925,832 transactions. All four runs match the 714,066,608 output bytes and SHA-256 `54531e48c946cb66c0c148d7339fdbb11511cffaaec9bc9c5a33f650a10d3685`.
- Indexed USDC: first 8,192 blocks. Both runs match the report, 174,958 matching transactions, 341,094 failed transactions skipped, 906,065 output rows, 63,424,626 output bytes, and 36,670 dictionary rows / 2,200,276 dictionary bytes. This diagnostic sink checks report totals; it does not hash full USDC output contents.
- Source-byte totals match within each workload. All 12 result-file SHA-256 checks passed. Archive size, modification time, inode, and device inventories stayed unchanged. This inventory check is not a full archive content hash.
- No scan transport retries were recorded. All test processes and the controller stopped after completion. The old broad benchmark and Jetstreamer remain deferred.
- The delayed-first-read test also covers later input failure, sink cancellation, selected subranges, buffer limits, ordered callbacks, worker joins, and a decoded block larger than the normal admission budget.

After timing, final review added an early rejection for a range target larger than its configured buffer budget, an assertion for that case, and formatting changes. The default timed path is unchanged. All 234 V2 library tests pass. The reader-profile tool and V2 example compile. Strict Clippy still fails on 13 existing warnings; the new validation-style warning was fixed.

## Reproduce

Build the diagnostic tool with `cargo build --release -p blockzilla-reader-profile`. Use an empty cache directory for each network attempt. For the count comparison:

```sh
./target/release/blockzilla-reader-profile --format v2 --epoch 900 \
  --workload count --blocks 32768 --workers 12 --iterations 1 --warmups 0 \
  --origin https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev \
  --cache-root /tmp/v2-window-count
```

Add `--v2-legacy-input` and select another empty cache directory for the previous schedule. For transaction identities, use `--workload transactions --blocks 8192`. For indexed USDC, use `--workload usdc --blocks 8192 --indexed-usdc`. Run one process at a time.

Timed executable SHA-256: `43c3572853a27c7544bf1a433dc56e790b86d63c0e7c31eb35a4db1d7660275b`.

[Raw results, configuration, build hashes, and verification](artifacts/v2-reader-window-20260909.json). [Input-window design](../design/v2-concurrent-input.md).
