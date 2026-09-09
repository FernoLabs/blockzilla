# Reader benchmark update — 9 September 2026

**The new network input schedule improves V2 and V3. CAR buffer reuse has no
confirmed speed gain and remains disabled by default.** All 34 cases in these
focused comparisons passed their workload checks. The broad full-epoch suite
remains stopped; the results below cover epoch 900 prefixes.

## Transaction rate

Count tests use 32,768 blocks; transaction and USDC tests use 8,192. Rates use
total transactions divided by combined scan time, excluding setup and sidecar
downloads. Count and identity scans have two network runs per setting; USDC
has one. USDC TPS counts scanned transactions, not matching transactions.

| Format / workload | Previous TPS | New TPS | Speed gain |
|---|---:|---:|---:|
| V2 / count | 503,044 | 1,195,942 | 2.38× |
| V3 / count | 200,372 | 2,615,330 | 13.05× |
| V2 / transaction identities | 366,398 | 651,085 | 1.78× |
| V3 / transaction identities | 485,486 | 1,585,553 | 3.27× |
| V2 / indexed USDC | 561,363 | 1,001,213 | 1.78× |
| V3 / standard USDC | 172,228 | 593,686 | 3.45× |

The two USDC paths do different work and must not be ranked directly.

## Read rate

Source MB/s uses decimal megabytes divided by scan time. It can include registry
reads and is not physical disk throughput.

| Format / workload | Previous MB/s | New MB/s |
|---|---:|---:|
| V2 / count | 62.99 | 149.75 |
| V3 / count | 12.55 | 163.80 |
| V2 / transaction identities | 70.34 | 125.00 |
| V3 / transaction identities | 46.56 | 152.07 |
| V2 / indexed USDC | 126.41 | 225.46 |
| V3 / standard USDC | 24.02 | 82.80 |

Larger adjacent reads and concurrent downloads keep the decoders supplied.
Both readers reuse input buffers within a 256 MiB budget. Count peak process
memory rose from about **85 to 307 MiB for V2**, and **176 to 400 MiB for V3**.
The input budget is not a whole-process memory limit.

## CAR result

CAR already downloaded concurrent 32 MiB ranges. Over 8,192 blocks, four-worker
buffer reuse reduced large allocations from **292–295 to eight**, but scan speed
fell from **178,149 to 145,545 TPS**. Received speed fell from **196.56 to
161.13 MB/s**. A shorter comparison also showed lower speed. Peak memory did
not fall. Eight-worker reuse reached 196,723 pooled TPS, but varied widely.
The existing default is retained.

CAR fully decodes transactions and metadata, so its TPS is not equivalent to
the V2/V3 identity scans. Network conditions were not controlled. These short
runs do not establish full-epoch rates. Jetstreamer was not rerun.

Detailed results and verification: [V2](v2-reader-window-20260909.md),
[V3](v3-reader-window-20260909.md), [CAR](car-reader-window-20260909.md).
The [download-only test](v2-download-only-20260909.md) reached 547–558 MB/s with
larger requests and no decoding; it is a separate workload.

## Earlier full-epoch count results

These results predate the new input schedule. Each case covers 476,026,811
transactions on epoch 900, with one run per setting. TPS includes setup.

| Source / format | Previous TPS | Measured TPS |
|---|---:|---:|
| Local / V2 | 5,882,574 | 6,292,584 |
| Network / V2 | 189,209 | 474,605 |
| Local / V3 | 7,238,017 | 7,799,617 |
| Network / V3 | 110,307 | 215,835 |

The full application suite is incomplete. Historical receipts and the frozen
plan remain under `target/nas-validation/full-readers-20260909T080240Z`.
[Earlier full-suite report](all-samples-reader-comparison-2026-09.md).
