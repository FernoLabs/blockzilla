# Epoch 900 network reader pilot

> **9 September completed results:** all 16 runs passed. V3 improved; the V2 candidate was rejected and removed. See the [full results and comparison limits](network-reader-comparison-20260909.md).

> **Follow-up diagnosis:** a detailed mirror run reproduced an incomplete HTTP body.
> A separate allocator test found substantial overhead in our Jetstreamer build.
> See the [diagnosis and limits](network-reader-diagnosis-20260908.md); original baseline measurements remain unchanged.

> **Selected baseline:** use the [epoch 900 network reference](network-reference-baseline-20260908.md)
> for future reader work. Jetstreamer and SDK workloads have separate acceptance rules.

> **8 September, Triton comparison:** the same Jetstreamer executable and 12 workers
> passed the 8,192-block test from Triton: 8,925,832 transactions in 471.0 seconds
> (18,951 TPS), with no reported firehose error. This is an accepted prefix,
> not a full-epoch result. See the [Triton report](jetstreamer-triton-20260908.md).

> **8 September, Jetstreamer retry:** the new 8,192-block check stopped after
> 346.4 seconds on an HTTP response-body read error. It completed 6,020 blocks
> and 6,546,976 callbacks; the full epoch was not started. No new Jetstreamer
> speed result is accepted. See the [retry report](jetstreamer-epoch900-retry-20260908.md).


Updated on 8 September 2026. The local matrix and all nine SDK network cases
passed acceptance. Both Jetstreamer correctness checks passed; its full run
failed. The selected network scope remains epoch 900.

| Reader | Epoch | Network work |
| --- | --- | --- |
| Compact V2 | 900 | Count, USDC balances, Pump.fun, user-program-index |
| Indexer V3 prototype | 900 | The same four workloads |
| CAR | 900 | One transaction and recorded-CPI count |
| Jetstreamer | 900 | Two 512-block correctness checks, then one full decoded transaction count |

This is nine SDK cases and three Jetstreamer cases. CAR USDC, Pump.fun and
user-program-index network scans are deferred. No other network epochs are
selected. Removing those three CAR scans removes about 1.58 TB of planned
raw CAR reads. One full CAR count plus one full Jetstreamer count reads about
1.05 TB, before proofs, indexes and retries. These volumes do not include
V2/V3 traffic and do not predict elapsed time.

The baseline ran local tests first, then V2/V3 network cases, CAR count and
Jetstreamer sequentially. Exact outputs and sources were checked. Keep each
attempt and its cache, and compare exact outputs after timed reads stop. Use
the existing frozen package as the initial baseline. Its old `firewatch` ID
maps to the current user-program-index workload name. Do not change that
package, its recorded source hashes or old result IDs.

Use separate result directories for the V2/V3 phase and the CAR count phase.
The current local control's `network-epoch900-plan.json` records both exact
commands under `sdk_network.phases`. The previous twelve-case network command
is archived and must not be launched.

After the pilot, compare setup, scan and total time, HTTP body bytes, cache
reads, CPU use, active workers and input waits. Frozen V2/V3 output does not
include HTTP request counts or request latency. V2 stage timings can help
separate input waits, decoding and sink work. Add small, targeted measurements
if the existing evidence cannot identify the cause of a delay.

Keep the first results as the baseline. Test any relevant reader improvement
with a new recorded build and repeat the affected epoch900 cases. Require
exact output parity and a measured benefit before expanding the network run.
The next report will cover the local matrix, this pilot, and the network
reader findings. A full network sample run remains deferred.

The [early report](early-reader-results-20260907T0526Z.md) retains its fixed
measurement snapshot and original planned case list, with a dated scope note.
The [runner guide](sample-reader-matrix.md) describes the available metrics and
input rules. CAR network input is raw CAR; local epoch900 input is streamed
zstd. Jetstreamer performs different decode and validation work from the SDK
CPI count. Keep those limits and the reported concurrent NAS CPU load visible.

## Accepted baseline — 8 September 2026

All nine SDK cases passed final acceptance against the accepted local outputs.
The checks include exact ordered count buckets or exact application output,
coverage, source inventories, command settings and frozen binary hashes.
The full local matrix also passed all 132 cases. These network results use
12 requested processing workers and fresh private cache directories.

### Transaction rate

The rate below is query coverage divided by total elapsed time, including setup.
For a selective query, covered transactions can include blocks skipped by the
index. This is not always the number of transactions decoded each second.

| Reader | Workload | Total seconds | Covered TPS |
| --- | --- | ---: | ---: |
| V2 | Count | 2,515.88 | 189,209 |
| V2 | USDC | 2,431.19 | 195,800 |
| V2 | Pump.fun | 2,151.12 | 221,292 |
| V2 | User-program-index | 1,962.24 | 242,594 |
| V3 prototype | Count | 4,315.46 | 110,307 |
| V3 prototype | USDC | 3,545.64 | 134,257 |
| V3 prototype | Pump.fun | 6,605.37 | 72,067 |
| V3 prototype | User-program-index | 14.98 | 31,775,595 |
| CAR | Count | 2,552.40 | 186,501 |

V3 user-program-index selected only 10 blocks and decoded 11,412 transactions.
Its decode rate during the scan was 2,607.55 TPS. Its high coverage rate is an
index benefit, not a claim that it decoded 31.8 million transactions per second.
V3 Pump.fun selected 426,475 blocks and decoded 470,452,521 transactions;
its scan decode rate was 71,460.88 TPS.

### HTTP transfer rate

Decimal GB and MB/s below measure response-body bytes consumed by the reader,
including setup and any counted partial retry bodies. They exclude HTTP headers.
These are separate from logical read volume, local cache reads and host traffic.

| Reader | Workload | HTTP body GB | Total HTTP MB/s |
| --- | --- | ---: | ---: |
| V2 | Count | 60.811 | 24.17 |
| V2 | USDC | 60.819 | 25.02 |
| V2 | Pump.fun | 93.198 | 43.33 |
| V2 | User-program-index | 60.819 | 30.99 |
| V3 prototype | Count | 29.143 | 6.75 |
| V3 prototype | USDC | 21.019 | 5.93 |
| V3 prototype | Pump.fun | 61.197 | 9.26 |
| V3 prototype | User-program-index | 0.451 | 30.13 |
| CAR | Count | 527.051 | 206.49 |

CAR uses raw CAR over HTTP. The local epoch-900 CAR test used outer zstd.
These transport sizes must not be treated as the same stored representation.
The V3 results are for the measured standalone prototype. The operator reported
a concurrent NAS compaction job; the CPU effect was not measured, and no timing
correction has been applied.

### Jetstreamer reference

Both 512-block correctness checks passed: 545,576 transactions with exact block
parity, using one worker and 12 workers. The full 12-worker epoch-900 attempt
failed after 1,549.322 seconds with 12 reported block-read timeouts. It completed
24,415 blocks and 27,111,371 transaction callbacks before it stopped. Its report
is marked invalid. No full-epoch Jetstreamer TPS is accepted.

The strict benchmark adapter stops on a reported upstream error. Jetstreamer
itself can restart the stream. The timeout cause is not yet known. The reference
also performs more transaction decoding work than the SDK count projection.
Do not use the small correctness checks or failed partial run as a speed ranking.

### Reader changes from the review

V2 input is serial while decode work runs in parallel. V3 input requests stop
at small decode-job boundaries. The proposed fix is a shared download window
with a fixed byte limit and several active requests, supplied by each format's
index planner. It will preserve selected ranges, borrowed buffers and output
order. See the [source review and implementation plan](../design/network-reader-window.md).

The current local V2/V3 examples now expose existing request, retry and cache
counters. The download-window change is not implemented, and no improved speed
has been measured. Keep this accepted baseline for the next candidate build.

Evidence: [compact accepted metrics and Jetstreamer failure receipt](artifacts/network-reader-epoch900-20260908.json).
The full local acceptance and network acceptance receipts remain in
`target/nas-validation/all-samples-signer-fix-20260907T001310Z/`.
