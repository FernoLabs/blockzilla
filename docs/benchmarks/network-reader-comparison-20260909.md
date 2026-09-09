# Network reader results — 9 September 2026

All 16 runs passed on the NAS: eight old/new V2/V3 cases and eight CAR/Jetstreamer cases. Each covered the first 8,192 canonical blocks of epoch 900, with 8,925,832 transactions and 12 requested workers. Each case ran in a fresh process, with one active reader at a time.

**Keep the V3 change. Remove the V2 experiment.** V3 reduced mean scan time by 28.1% and mean total time by 24.6%. The V2 experiment increased mean total time by 9.5%, with substantial run-to-run variation and no memory improvement. Its changes have been removed from the working tree. The measured candidate source and executable are retained with the receipts.

## V2 and V3: transaction identities

These cases serialize and hash transaction identities, then discard the output. They do not perform the full CAR/Jetstreamer decode workload below. Every run matched the accepted output hash `54531e48c946cb66c0c148d7339fdbb11511cffaaec9bc9c5a33f650a10d3685`.

Each format ran old/new/new/old. The table uses mean elapsed time; TPS is the identical transaction count divided by that mean. MB/s uses decimal megabytes of exact response-body input. Setup is separate from scan time.

| Format / version | Setup (s) | Scan (s) | Total (s) | Scan TPS | Total TPS |
| --- | ---: | ---: | ---: | ---: | ---: |
| V2 Previous | 15.301 | 16.363 | 31.664 | 545,474 | 281,888 |
| V2 Candidate | 15.620 | 19.057 | 34.677 | 468,379 | 257,400 |
| V3 Previous | 4.366 | 23.009 | 27.375 | 387,933 | 326,061 |
| V3 Candidate | 4.084 | 16.554 | 20.638 | 539,206 | 432,504 |

| Format / version | Scan input (MB) | Mean scan rate (MB/s) | Scan GETs | Peak RSS range (MiB) |
| --- | ---: | ---: | ---: | ---: |
| V2 Previous | 1,713.6 | 104.72 | 156 | 137.5–138.0 |
| V2 Candidate | 1,713.6 | 89.92 | 174 | 137.3–141.5 |
| V3 Previous | 856.1 | 37.21 | 2,176 | 196.1–197.4 |
| V3 Candidate | 856.1 | 51.72 | 192 | 201.7–201.8 |

V3 shares signature windows across adjacent decode jobs. It now reads 64 signature windows instead of 2,048 small windows for this range. Together with 128 semantic-plane requests, that gives 192 GETs instead of 2,176: **91.2% fewer requests with exactly the same 856,096,236 input bytes**. Its 64 MiB shared input budget includes signatures. Mean kernel peak RSS increased by about 5 MiB; this is a speed improvement, not a memory reduction.

The V2 experiment overlapped signature input with block processing, but split one 32 MiB signature window into two 16 MiB windows to preserve the normal buffer budget. Requests increased from 156 to 174. One candidate scan took 22.37 s; the other took 15.75 s, versus 15.31 s and 17.42 s for the prior reader. Network variation prevents a firm claim that overlap itself is the cause, but these results do not justify adopting the candidate. V2 keeps its prior signature reader and existing concurrent block input.

All V2/V3 cases reported zero incomplete-body and server-error retries. Each had a fresh sidecar cache. The first old V2 process reported about 22.5 MB of storage input; the other V2 cases reported 64 KiB or zero. These whole-process storage counters include setup and executable/cache reads; they are not proof of equal operating-system cache state.

## CAR and Jetstreamer: full decode comparison

These are ordinary timing builds, with no HTTP trace patch or profiler. Both allocator variants retain the same underlying decoder versions. The Jetstreamer reference is pinned to 0.7.0 (`cffaf3d891b3cbe45a46dd963d6d3571b2aa1a24`). The mimalloc variant changes the global allocator only; it keeps the original reader dependencies and callback logic.

| Source | Reader | Allocator | Elapsed (s) | TPS | Peak RSS (MiB) |
| --- | --- | --- | ---: | ---: | ---: |
| Triton | Our CAR probe | system | 97.035 | 91,986 | 354.6 |
| Triton | Our CAR probe | mimalloc | 43.725 | 204,136 | 441.0 |
| Triton | Jetstreamer | system | 468.939 | 19,034 | 59.2 |
| Triton | Jetstreamer | mimalloc | 140.891 | 63,353 | 281.6 |
| Our gateway | Our CAR probe | system | 103.364 | 86,353 | 352.7 |
| Our gateway | Our CAR probe | mimalloc | 45.397 | 196,616 | 402.1 |
| Our gateway | Jetstreamer | system | 471.038 | 18,949 | 58.3 |
| Our gateway | Jetstreamer | mimalloc | 140.574 | 63,496 | 268.5 |

CAR elapsed includes its measured HEAD setup and full decode scan, including HTTP teardown. Jetstreamer elapsed is its full firehose call, including index acquisition. Plan loading and report writing are outside these intervals. CAR HEAD setup was 0.07–0.15 s.

With mimalloc, our CAR probe took **45.40 s on our gateway**, versus **140.57 s for Jetstreamer**: about **3.10×** the end-to-end throughput in this test. On Triton the factor was about **3.22×**. This is a broader decode-path comparison, not proof that a single parsing function is three times faster.

Both paths decode transactions and status metadata, classify votes and failed status, compute message hashes, and decode rewards. Our probe retains borrowed transaction fields and protobuf status metadata; Jetstreamer also converts metadata and rewards into Solana native types and dispatches asynchronous callbacks. Our probe hashes the encoded message directly; Jetstreamer serializes the decoded message before hashing. These differences account for real application costs but must be stated when comparing rates.

All eight cases matched the full ordered block counts, **6,533,434 simple votes**, and **341,094 failed transactions**. All four CAR cases also matched each other's per-block signature/message-hash/status digests. The original Jetstreamer adapter does not emit these digests, so this does not prove complete decoded-field parity between libraries. No signature verification was enabled.

Our CAR probe uses more memory than Jetstreamer. On our gateway, mimalloc raised its kernel peak RSS from 352.7 to 402.1 MiB, while process CPU time fell from 1,102.2 to 69.8 CPU-seconds for the complete process. Jetstreamer also benefited from mimalloc: 471.04 to 140.57 s, about 3.35× throughput. The old musl-system-allocator reference must not be presented as Jetstreamer's best available performance.

The mimalloc setting is optional in the benchmark adapter. This work does not change the global allocator of the public SDK or user applications. The bounded 12-worker full-decode probe is also a benchmark adapter; its throughput is not a measured promise for every public CAR query API.

| CAR source / allocator | Received HTTP body (MB) | Body rate over setup + scan (MB/s) | GETs | Body retries |
| --- | ---: | ---: | ---: | ---: |
| triton-car-system | 9,932.1 | 102.36 | 296 | 0 |
| triton-car-mimalloc | 9,797.9 | 224.08 | 292 | 0 |
| mirror-car-system | 9,932.1 | 96.09 | 296 | 0 |
| mirror-car-mimalloc | 9,797.9 | 215.83 | 292 | 0 |

The CAR body counter includes read-ahead beyond the last consumed block. The different allocator builds finish at different points in that read-ahead window. It is transport throughput, not useful decoded payload throughput. Jetstreamer has no exact body-byte counter in these ordinary builds; no accepted MB/s value is derived from host network traffic. No local raw CAR file was created or decompressed.

## What happened to the earlier failure?

The earlier diagnostic established an incomplete HTTP response body before CAR decoding. It did not identify the component that closed the connection. In the later transport-only test, long responses from our gateway and Triton, plus bounded gateway ranges, each delivered the same 600 MiB with identical hashes. Each transfer deliberately lasted about 400 seconds. The full Jetstreamer mirror retries above also passed.

The fault therefore did not recur in this round. There is no evidence here for a fixed five-minute timeout or a corrupt transaction at the previously failing slot. A transient network, edge, origin or client transport failure remains possible. Our new CAR retries protect against bounded incomplete responses and passed injected-failure tests, but no retry was exercised in these successful live runs. Do not claim that they repaired the gateway or established the original root cause.

## Validation and scope

The transport test, benchmark sources and results were collected and hash-checked. Source length/ETag checks remained stable around the runs. Our gateway requires a strong ETag; Triton CAR access is explicitly operator-trusted because it has no strong ETag. The range data and output checks are not a whole-CAR cryptographic identity proof.

These results cover one 8,192-block prefix, not the full epoch. V2/V3 used two repetitions per version. CAR used one case per source, reader and allocator. More repetitions are needed before treating the CAR ratios as a stable production guarantee. No new flame graph was captured in this round; CPU and memory are operating-system resource measurements.

After removal of the V2 experiment, all 534 tests in the three reader suites
passed again: 233 V2, 142 V3 and 159 CAR. The final Linux release builds also
passed. Both rebuilt CAR executables are byte-identical to the measured ones.

The final source keeps CAR retry handling and V3 signature grouping, and removes the V2 prefetch experiment. The measured changes and reports are packaged together. The rejected V2 experiment is excluded.

## Individual V2/V3 runs

| Case | Setup (s) | Scan (s) | Total (s) | Scan TPS |
| --- | ---: | ---: | ---: | ---: |
| v2-a1 | 15.022 | 15.307 | 30.330 | 583,107 |
| v2-b1 | 11.621 | 22.368 | 33.989 | 399,043 |
| v2-b2 | 19.619 | 15.746 | 35.365 | 566,878 |
| v2-a2 | 15.579 | 17.420 | 32.999 | 512,404 |
| v3-a1 | 4.984 | 20.983 | 25.966 | 425,388 |
| v3-b1 | 3.589 | 17.276 | 20.865 | 516,652 |
| v3-b2 | 4.579 | 15.831 | 20.410 | 563,819 |
| v3-a2 | 3.748 | 25.035 | 28.783 | 356,540 |

[Exact results and receipt hashes](artifacts/network-reader-comparison-20260909.json). [Transport-only control](artifacts/car-transport-isolation-20260909.json). [CAR probe contract and usage](../../bench/reader-profile/README-car-decode.md). [Original baseline](network-reference-baseline-20260908.md).
