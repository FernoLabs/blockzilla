# Local-cache full-epoch test: MEASURED_UNVERIFIED

On 10 October 2026, the owner approved removal of the old epoch-1000 CAR input
from Foundation to make space for live capture. The 767,389,334,224-byte CAR is
no longer in that cache. Benchmark reports, receipts, cache metadata, and slot
ranges remain. The input hashes and measurements below describe the original
test. See the
[cleanup receipt](../../hivezilla/stream-relay/evidence/processed-stream-20261010/EPOCH1000-CAR-CLEANUP.json).

**State: `MEASURED_UNVERIFIED`.** At the user's request, these runs skip the
independent full export check, output SHA-256, and byte comparison. The controller
checked reader receipts, canonical block and transaction counts, source identity,
and output file sizes. These outputs have no confirmed byte parity.

The warm CAR 64-worker repeat took **109.624 seconds** for the full epoch.
The fastest measured Jetstreamer run used 128 workers and took **124.455
seconds**. CAR had **13.53% higher transaction throughput** and used **46.6%
less reader CPU time** in these two runs. These are individual measured runs;
they do not establish statistical confidence or a general reader ranking.

**The first CAR 64-worker run was affected by different input conditions.**
It took 156.623 seconds, with far more host disk reads during the run. Its aggregate
HTTP-read time was 2,631 worker-seconds, compared with 134 in the repeat. CAR
with 128 decode workers took 111.896 seconds, so it did not beat the warm
64-worker repeat. Use **64 parsers, 64 decode workers, and eight HTTP clients**
for this local setup for now. The data does not show a benefit from 128 decode
workers. All five completed measurements remain below, including the first
slow CAR run.

These local results are separate from the
[accepted earlier public-source full-epoch test](triton-manycore-full-epoch-20260930.md)
and the [dynamic prefix and failed public-source attempts](triton-dynamic-scheduling-20260930.md).

## Input and checks

The input is the local copy of Triton's epoch-1000 CAR. The canonical plan covers
slots `[432000000, 432432000)`, with **431,781 blocks and 547,899,266
transactions**. The independently checked Compact V2 index supplied the plan.

The downloader completed a full local-file hash pass. It checked every durable
download-job hash, the complete raw slot-index hash, and all 17 CAR samples from
the dataset manifest. The completed cache contains:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `epoch-1000.car` | 767,389,334,224 | `3c9727378e617f5cba8b5e206bce8fc6ae5df34d3a4408eeb69aea4d62ac7218` |
| `epoch-1000-slot-ranges.raw` | 5,184,000 | `6b0e72ff321996c23ef3fcd53a02d812141484ec4eea559f45d67113be1b89e3` |

This verification identifies the completed local copy. The remote CAR had no
strong ETag, so it does not prove that the original remote object was unchanged
between all download requests.

Both files are read-only. The server on `http://127.0.0.1:44133/` served only these
two files during the tests. It pins the completed manifest and file identities, checks those
identities before and during transfers, and supplies strong ETags from the local
hashes. It trusts the downloader's full verification and does not repeat that
hash pass at startup. The controller checks the server PID and process start
time, source script, manifest, and file identities. The complete raw-index hash,
CAR length, and all 17 samples matched before and after each completed series.

All five completed reader receipts reported the complete plan, 302,828,458
votes, and 66,856,874 failed transactions. Each output file had **175,420,540,252 bytes**,
matching its receipt. These are receipt and file-size checks. The controller did
not inspect every exported record, hash the outputs, or compare their bytes.
The saved cases are `MEASURED`; the series acceptance is `UNVERIFIED`, not `PASS`.

## Host and reader settings

The host is `ubuntu@213.239.141.61`: AMD EPYC 9555P, 64 physical cores,
128 hardware threads, about 1.1 TiB RAM, and NVMe storage. The saved host record
reports Linux 7.0.0-15. Both readers use the native release builds and mimalloc
from the recorded source archives. Only one reader runs at a time, in a fresh
process. The same local server stays running across cases.

All three CAR runs used 64 parsers, eight independent HTTP clients,
128 MiB range chunks, and a 256 MiB dynamic job target. They used owned metadata
and `--legacy-http-buffers`. The configured HTTP body window was 8 GiB.
Each completed all 2,849 jobs and all 431,781 parsed and decoded blocks. The
64-worker case used 64 decode workers and a shared raw-block pool and queue of
192 entries each. The 128-worker case used 128 decode workers and 256 entries
each. Parser count, client count, chunk size, and job size stayed the same.

Jetstreamer used 64 and 128 workers with the unchanged executable built from official Git
revision `048d2cd25af949679c56347440b7cf5a9a900063`, still labelled version 0.7.0.
A saved upstream check at 13:56:48 UTC confirmed that revision was still the
latest Git head. Its local environment was `JETSTREAMER_SPAWN_PENDING=24`,
`JETSTREAMER_SPAWN_GRACE_SECS=30`, and `JETSTREAMER_RECYCLE_PCT=50`. The earlier
public-source runs used **1/30/50**, so those runs do not hold every setting
constant with this test.

The repeat CAR 64-worker case used the same settings and executable as the
first CAR 64-worker case. The order was CAR 64, Jetstreamer 64, CAR 128,
Jetstreamer 128, then CAR 64 again. There was no randomized or reverse-order
repeat of each pair.

## First measurements

Total reader time includes source setup, decoding, spool writes, ordered export,
and final file sync. Scan time includes reading, decoding, and spool writes.
Download and full-CAR warmup are outside reader timing. All times below are in
seconds. Throughput uses the receipt transaction count divided by total time;
it does not imply independently verified output contents.

| Case | Total | Scan | Total minus scan | Transactions/s |
| --- | ---: | ---: | ---: | ---: |
| CAR P64/W64/C8 | 156.623065 | 79.899229 | 76.723836 | 3,498,202.930 |
| Jetstreamer W64 | 131.765793 | 76.632120 | 55.133673 | 4,158,129.753 |
| CAR P64/W128/C8 | 111.895780 | 54.876393 | 57.019388 | 4,896,514.105 |
| Jetstreamer W128 | 124.455140 | 68.535552 | 55.919587 | 4,402,383.604 |
| CAR P64/W64/C8, repeat | 109.623578 | 52.678584 | 56.944994 | 4,998,005.701 |

For the first 64-worker pair, the total difference is 24.857272 seconds. The
scan difference is 3.267109 seconds. The remaining difference is 21.590163 seconds, or 86.9% of the total
gap. `Total minus scan` also includes setup and other work around the scan; it
is not a separately instrumented export timer. The code performs the ordered
export and final sync after the scan, so this points to that phase as the main
part of the observed gap. It does not isolate the effect of storage, cache
state, or export write order.

Jetstreamer's total throughput was 1.189 times CAR's in the 64-worker pair.
In the 128-worker pair, CAR's total throughput was 1.112 times Jetstreamer's.
CAR's scan was 13.659160 seconds shorter; its time outside the scan was
1.099800 seconds longer. Thus the 128-worker result differs from the 64-worker
result both during and after the scan. CAR's 64-worker repeat took 52.678584
seconds in the scan and 56.944994 seconds outside it. Its total time was
2.272203 seconds below the CAR 128-worker run. This supports a cache and storage
effect in the first comparison. It does not show a repeatable advantage for
128 CAR decode workers.

| Case | Reader CPU seconds | Server CPU seconds | Combined CPU seconds | Reader mean cores | Combined mean cores |
| --- | ---: | ---: | ---: | ---: | ---: |
| CAR P64/W64/C8 | 3,471.473078 | 123.720000 | 3,595.193078 | 20.877 | 21.621 |
| Jetstreamer W64 | 4,799.784294 | 75.050000 | 4,874.834294 | 34.238 | 34.774 |
| CAR P64/W128/C8 | 3,424.542786 | 45.030000 | 3,469.572786 | 28.107 | 28.476 |
| Jetstreamer W128 | 6,503.843542 | 73.380000 | 6,577.223542 | 49.040 | 49.593 |
| CAR P64/W64/C8, repeat | 3,471.050983 | 44.460000 | 3,515.510983 | 29.038 | 29.410 |

| Case | Whole-process seconds | Reader peak RSS, MiB | Sampled reader peak threads | Server RSS before/after, MiB | Server lifetime peak RSS, MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| CAR P64/W64/C8 | 166.282758 | 12,754.863 | 137 | 24.246 / 24.246 | 28.039 |
| Jetstreamer W64 | 140.187727 | 2,088.660 | 65 | 24.246 / 24.246 | 28.328 |
| CAR P64/W128/C8 | 121.841547 | 14,095.176 | 201 | 24.246 / 24.246 | 28.430 |
| Jetstreamer W128 | 132.623455 | 3,809.902 | 129 | 24.246 / 24.246 | 29.543 |
| CAR P64/W64/C8, repeat | 119.535323 | 12,822.203 | 137 | 24.246 / 24.246 | 29.770 |

CPU seconds include all threads in each measured process. Mean cores use
whole-process time, which includes startup and shutdown and exceeds receipt
total time. Server CPU is sampled separately around each reader process.
The server peak RSS covers its lifetime, not only that case. Thread samples can
miss short peaks. Process RSS does not include the host's full file cache.

All three CAR runs reported zero incomplete-body, request-send, or server-error retries.
Both Jetstreamer cases reported zero permitted body retries, zero connection
recycles, and zero operation timeouts. They reported 348 and 316 work steals at
64 and 128 workers, respectively. The server's cumulative snapshot recorded
1,710 transport errors, with only HTTP 200 and 206 response counts and no
cache-identity failure. That counter does not classify each cause. It is
different from the readers' retry counters.

The saved server analysis uses intervals of about two seconds wholly inside
each reader's recorded process interval. It covers 98.6% to 98.9% of process
time. These maxima are interval averages, not instantaneous peaks. They cover
parts of setup and export as well as the scan.

| Case | Maximum sampled server CPU, cores | Maximum sampled send rate, GB/s | Transport errors in sampled intervals |
| --- | ---: | ---: | ---: |
| CAR P64/W64/C8 | 7.885 | 15.522 | 0 |
| Jetstreamer W64 | 1.091 | 10.430 | 770 |
| CAR P64/W128/C8 | 0.926 | 15.582 | 0 |
| Jetstreamer W128 | 1.152 | 11.656 | 880 |
| CAR P64/W64/C8, repeat | 0.931 | 15.678 | 0 |

GB/s uses decimal bytes. The sampled error counts do not cover the entire
server lifetime or prove an error cause. The direct CPU deltas in the earlier
table are the full reader-interval measurements. In the CAR repeat, the server
used 0.372 cores on average. This does not indicate broad server CPU saturation,
but it does not exclude a limit in one server thread or in the storage path.
The server stopped cleanly after the tests.

## Cache and storage limits

Before each reader, the controller read the complete 767,389,334,224-byte CAR
with a 32 MiB buffer and checked file identity. The system caches were not dropped.
This is a comparison after the same cache-warming procedure. It is neither a
cold-cache test nor proof that all input and spool pages stayed in RAM.

| Case | Full-CAR warmup, seconds | Host NVMe read bytes | Host NVMe write bytes |
| --- | ---: | ---: | ---: |
| CAR P64/W64/C8 | 37.803 | 355,111,161,856 | 350,869,323,776 |
| Jetstreamer W64 | 105.331 | 59,479,896,064 | 350,867,357,696 |
| CAR P64/W128/C8 | 45.090 | 71,106,560 | 350,864,416,768 |
| Jetstreamer W128 | 42.359 | 2,166,784 | 350,875,095,040 |
| CAR P64/W64/C8, repeat | 40.278 | 3,448,832 | 350,858,825,728 |

The disk values are changes in the host `nvme0n1` counters during each measured
process interval, after warmup. These are host counters, not direct attribution
to a reader or to CAR input. Disk and partition
counters overlap and must not be added together. Buffered writes and cache
eviction can shift physical I/O between phases. This difference in disk reads
limits any claim that the timing gap is caused only by decoding or scheduling.
CAR's aggregate HTTP-read time was 2,631.184 worker-seconds in its first
64-worker run, 126.077 in its 128-worker run, and 134.469 in its 64-worker
repeat. These overlapping worker times are not
elapsed scan time. Together with the disk counters, they show that input
conditions changed. The repeat used the same CAR worker settings as the first
run and completed sooner than the 128-worker case. Thus the initial speed
increase cannot be assigned to more decode workers. No run proves that every
input page stayed in RAM for its entire duration.

## Evidence

The [local-cache evidence artifact](artifacts/triton-local-car-full-epoch-20260930.json)
is the saved-file snapshot collected at **2026-09-30 14:30:38 UTC**. It contains
all five receipts, final controller states, and server analysis, not the CAR
or export bytes.
The completed series are retained under
`/home/ubuntu/benchmarks/zilla-triton-20260930/` in
`results-dynamic-local-full-epoch-1000-64-r1` and
`results-dynamic-local-full-epoch-1000-128-r1`, with the completed repeat in
`results-dynamic-local-full-epoch-1000-car64-r2`. All three series are
`MEASURED_UNVERIFIED`.

| Evidence | SHA-256 |
| --- | --- |
| Evidence snapshot used here | `af754b0055a69cc6ad8b035319ad01469e1d27750e0b19adecb780e8f27b5344` |
| Completed local cache manifest | `bd36a672d10d7005b0ae1c83f5250ac3fc45a53fc6c998c6a29456ed3228320e` |
| 64-worker controller status | `e2022e3ab36b57f13cedec471c780ed9b2fa0fe9dc77a9423553f98d5496da48` |
| CAR 64-worker receipt | `d0996f803b3fa1ae182d8a931b31968335e66437996ca9324de7ceb49bfe64f2` |
| Jetstreamer 64-worker receipt | `561f60558d40c7cce522255b43ad5360aaba54ac0d0eef1360bcbda6a9d1bfb2` |
| 128-worker controller status | `9fca662b13935df20a7097b2ba354d85cd1d43a829cec1aab861be831e69180b` |
| CAR 128-worker receipt | `39906b4ad289a5a0b372b042394a13b3e83fc4cd33c1696569e14b231c626773` |
| Jetstreamer 128-worker receipt | `63aa6a579466d63026b7c7de1d57b1cdb2572988f71542c3b26c8c44c054b064` |
| CAR 64-worker repeat controller status | `050b63a8976e8ee94f78bfb82ee31a854cbe71da5e418495977d5d32dcab2c66` |
| CAR 64-worker repeat receipt | `b50cd820c07df4174b41011a5f0a4ebe20a36c700f591244a593af0c5b6f5ede` |
| Saved server analysis | `16acda32d4bf8f7bda92ec206974427c2e0ed5425c168abc10f2e833c1f1898a` |
| CAR executable | `caf5fcc29442143364916d2fe94b6c11e227cf141a0e4b8d8d9ff9f92ef208d8` |
| Jetstreamer executable | `4bfe1e0121013accb7855d049ba0bae2ab960c4f5f9ba0aae9a40bec98a5b0a2` |
| Performance controller | `6f5bbd860ac9d3d09a664db926f802baad3a99c7c6aa60f41846d5528eb2b5e7` |

Source archives and their verification records are retained separately from
current working files. No hash from an earlier verified export is assigned to
these new unverified output files.
