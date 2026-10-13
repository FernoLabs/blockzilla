# Memory export full-epoch test: six measurements and one failed attempt

Zilla's optional memory export mode reduced mean full-epoch time from **111.599
to 101.582 seconds**, or **8.98%**, across two runs per mode. It used about
**190–193 GiB peak process memory**, compared with 12.56–12.69 GiB for disk mode,
and **29.5% more reader CPU time** on average. Keep disk mode as the default;
memory mode is an option when this time saving justifies its resource cost.
These few runs do not establish statistical confidence or a general speed claim.

**Final states:** the original four-case series is `FAILED` because its first
Jetstreamer memory attempt timed out with incomplete coverage. Its other three
cases completed as `MEASURED`. A separate series retried Jetstreamer with unchanged
settings and repeated Zilla in reverse order; all three cases are `MEASURED`, and
that series is `MEASURED_UNVERIFIED`. All six completed outputs remain unverified
at the byte level, as requested. The failed attempt remains in the evidence.

This experiment changes the **benchmark application's export storage**, not the
production SDK. Both Zilla and Jetstreamer use the same new export mode. Both
read the same local CAR copy through the same local HTTP server. This is not a
comparison of direct Triton network throughput.

## Reason for the experiment

The [previous local full-epoch test](triton-local-car-full-epoch-20260930.md)
used a disk spool for both readers. Completed blocks were first written in arrival
order. After decoding, the exporter read those blocks and wrote the final file in
canonical slot order. The warm Zilla 64-worker run took 109.624 seconds, including
52.679 seconds in the scan. The remaining 56.945 seconds included setup, coverage
checks, ordered export, and final file sync. It was not a separate export timer.

The final output contained 175,420,540,252 bytes. Host NVMe counters recorded
about 350.9 GB of writes during that run, consistent with the disk spool and final
output. Those host counters are not direct file-specific measurements. The shared
spool also held one lock during each block write, which could make decode workers
wait. The earlier tests did not isolate the cost of this lock.

The hypothesis is that this host can use its memory to remove the disk scratch
write and read. The final canonical output and file sync remain part of the work.
The same change is applied to both readers so that the output cost remains common.

## Shared export change

`bench/car-export/shared.rs` keeps disk export as the default. The new explicit
memory mode reserves a block's slot and full payload size under a short lock.
It then uses fallible allocation and copies the payload outside that lock. The
reservation includes copies that are still in progress, so concurrent workers
cannot exceed the configured payload limit. A duplicate slot, invalid block, or
limit failure returns an error; it does not silently omit a block.

The memory cases used **192 GiB**, or 206,158,430,208 bytes, as the payload
limit. This limit excludes the callers' block buffers, map entries, allocator
overhead, the final writer buffer, and the operating system's file cache. It is
not a limit on total process or host memory. Before a memory case, the controller
checks Linux `MemAvailable` against the configured payload limit plus a 16 GiB
overhead allowance. That check does not guarantee that all input pages remain
cached during the test.

Finalization still checks block coverage and transaction counts, writes the same
`car-common-export-v1` bytes in canonical slot and transaction order, flushes the
writer, and calls `sync_all()` on the final file. It releases stored payloads as
they are written. Memory mode creates no disk spool. Its receipt records the mode,
payload limit, peak reserved payload bytes, and zero disk-spool bytes. Disk mode
records its payload bytes written to the spool. Both readers now record a separate
`export_finish_seconds` value as well as total and scan time.

All export work and final file sync stay inside total reader time. The memory mode
does not disable transaction decoding, metadata decoding, exported fields, votes,
failed transactions, or output persistence. Neither mode enables signature
verification. The upstream Jetstreamer library is unchanged.

## Input and case order

The source was the previously completed local copy of Triton's epoch-1000 CAR.
The canonical plan covers slots `[432000000, 432432000)`, with **431,781 blocks
and 547,899,266 transactions**. Each completed export contained
**175,420,540,252 bytes**.

| Local source | Bytes | SHA-256 |
| --- | ---: | --- |
| CAR | 767,389,334,224 | `3c9727378e617f5cba8b5e206bce8fc6ae5df34d3a4408eeb69aea4d62ac7218` |
| Raw slot index | 5,184,000 | `6b0e72ff321996c23ef3fcd53a02d812141484ec4eea559f45d67113be1b89e3` |

The host is `ubuntu@213.239.141.61`: AMD EPYC 9555P, 64 physical cores,
128 hardware threads, about 1.1 TiB RAM, and NVMe storage. Only one reader ran
at a time, in a fresh process. Both readers used `http://127.0.0.1:34299/`.
Before each reader, the controller read the full CAR with a 32 MiB buffer and
checked file identity. This warmup was outside the reader timer. Caches were not
dropped. The same procedure does not prove identical cache residency, so host
disk counters remain necessary when results are compared.

The full-epoch cases ran in this order. After the original four cases, a separate
series retried Jetstreamer memory mode with unchanged settings, then repeated
Zilla in the opposite order to check the effect of run order:

| Order | Reader | Export mode | Parser workers | Decode/processing workers |
| ---: | --- | --- | ---: | ---: |
| 1 | Zilla | Disk | 64 | 64 |
| 2 | Zilla | Memory, 192 GiB payload limit | 64 | 64 |
| 3 | Jetstreamer | Memory, 192 GiB payload limit | — | 128 |
| 4 | Jetstreamer | Disk | — | 128 |
| 5 | Jetstreamer, retry | Memory, 192 GiB payload limit | — | 128 |
| 6 | Zilla, repeat | Memory, 192 GiB payload limit | 64 | 64 |
| 7 | Zilla, repeat | Disk | 64 | 64 |

All Zilla cases use dynamic scheduling, eight HTTP clients, 128 MiB range
chunks, a 256 MiB job target, owned metadata, and `--legacy-http-buffers`.
All Jetstreamer cases use `JETSTREAMER_SPAWN_PENDING=24`,
`JETSTREAMER_SPAWN_GRACE_SECS=30`, and `JETSTREAMER_RECYCLE_PCT=50`, with
`JETSTREAMER_FORCE_LEGACY_INDEX` unset. These are the same local startup settings
as the previous local comparison. This order is not randomized and does not by
itself establish statistical confidence.

Jetstreamer remains pinned to official Git revision
`048d2cd25af949679c56347440b7cf5a9a900063`, labelled package version 0.7.0. The
upstream check at **2026-09-30 15:19:07 UTC** reported the same latest upstream
head. Changes are confined to the shared benchmark exporter, reader adapters,
measurement controller, tests, and test instructions.

## Checks and verification limit

Five standalone shared-export tests passed. Small fixtures check exact disk and
memory output equality, including reverse and concurrent block arrival, omitted
and present empty blocks, concurrent payload-limit crossing, duplicate and invalid
block rejection, invalid plans, payload release, and absence of a memory-mode
spool file. These tests check the new code and schema on small inputs. They do not
prove byte equality of the full-epoch outputs.

The version-2 native build also passed **35 CAR tests, 10 Jetstreamer adapter
tests, and 25 controller tests**. Both native release builds completed. These
counts describe code tests, not full-epoch output verification.

At the user's request, the full-epoch performance runs use
`--skip-output-verification`. The controller still checks reader receipts,
canonical block and transaction counts, source identity, output sizes, and memory
mode receipt fields. It skips independent full-record verification, output
SHA-256, and full byte comparison. A completed case is labelled `MEASURED`.
A series can be labelled `MEASURED_UNVERIFIED`, with acceptance `UNVERIFIED`,
only if every requested case completes its checks. The original series remains
`FAILED` because of its incomplete Jetstreamer memory attempt. Skipping output
verification does not permit incomplete coverage or relaxed error handling.
An earlier output hash must not be assigned to these new outputs.

Before and after both series, the source checks matched the complete raw-index
hash, CAR length and ETag, and all 17 CAR samples. Every memory attempt passed
the memory preflight. The lowest recorded `MemAvailable` was 1,193,378,381,824
bytes, above the required 223,338,299,392 bytes. This is an available-memory check,
not proof of complete cache residency.

## Measurements

All six completed cases reported 431,781 blocks, 547,899,266 transactions,
302,828,458 votes, and 66,856,874 failed transactions. Each final output contained
175,420,540,252 bytes. These are receipt and size checks; the output bytes have
not been compared.

All durations below are in seconds. The separate export finish timer includes
coverage checks, canonical output, flush, and final file sync. Scan time includes
the block spool work, whether the spool uses disk or memory.

| Order and case | State | Total | Scan | Export finish | Transactions/s |
| --- | --- | ---: | ---: | ---: | ---: |
| 1. Zilla disk | `MEASURED` | 113.986155 | 55.110555 | 58.782192 | 4,806,717.690 |
| 2. Zilla memory | `MEASURED` | 105.725081 | 56.964339 | 48.668084 | 5,182,301.670 |
| 3. Jetstreamer memory | `FAILED`, incomplete | — | — | — | — |
| 4. Jetstreamer disk | `MEASURED` | 123.476450 | 67.472720 | 55.935230 | 4,437,277.445 |
| 5. Jetstreamer memory, retry | `MEASURED` | 130.622861 | 81.502377 | 48.164115 | 4,194,512.816 |
| 6. Zilla memory, repeat | `MEASURED` | 97.439617 | 45.105438 | 52.241097 | 5,622,962.008 |
| 7. Zilla disk, repeat | `MEASURED` | 109.211777 | 52.260849 | 56.857631 | 5,016,851.499 |

The first Jetstreamer memory attempt reported **415,578 blocks and 525,790,907
transactions**, fewer than the required full epoch. Worker 11 timed out while
waiting for `read_raw_header` at slot 432039256. The existing error policy rejects
this operation timeout because it is not one of the permitted response-body
errors. The receipt also reported that block rows differed from the canonical
plan, one upstream operation timeout, and 77 upstream connection recycles.
`export_bytes` was null. This attempt has no completed full-export timing and
must not be included in throughput comparisons. Its retry used the same
binary, worker count, export limit, and error policy; the failure remains in the
record.

| Completed case | Reader CPU seconds | Server CPU seconds | Reader mean cores | Peak RSS, MiB | Peak sampled threads |
| --- | ---: | ---: | ---: | ---: | ---: |
| Zilla disk | 3,434.680827 | 43.81 | 27.710 | 12,998.988 | 137 |
| Zilla memory | 5,123.170661 | 58.92 | 46.234 | 194,617.383 | 137 |
| Jetstreamer disk | 6,353.089094 | 76.51 | 48.227 | 3,834.594 | 129 |
| Jetstreamer memory, retry | 7,546.644279 | 81.67 | 56.598 | 210,184.195 | 129 |
| Zilla memory, repeat | 3,808.823909 | 56.74 | 37.250 | 197,986.785 | 137 |
| Zilla disk, repeat | 3,462.193214 | 46.65 | 29.056 | 12,866.465 | 137 |

Reader CPU includes all reader threads. Reader mean cores use whole-process time,
including startup and shutdown, rather than receipt total time. Server CPU was
measured separately around each reader process. RSS excludes the host's full
file cache. Samples can miss short thread-count peaks.

| Case | Peak reserved memory payload, bytes | Disk spool payload, bytes | Host NVMe reads, bytes | Host NVMe writes, bytes | Warmup, seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Zilla disk | 0 | 175,410,177,492 | 1,001,295,872 | 350,864,461,824 | 37.916947 |
| Zilla memory | 175,410,177,492 | 0 | 991,428,608 | 175,449,022,464 | 47.002935 |
| Jetstreamer disk | 0 | 175,410,177,492 | 1,384,448 | 350,868,639,744 | 46.315255 |
| Jetstreamer memory, retry | 175,410,177,492 | 0 | 196,575,232 | 175,442,829,312 | 39.963699 |
| Zilla memory, repeat | 175,410,177,492 | 0 | 8,343,552 | 175,442,198,528 | 40.139333 |
| Zilla disk, repeat | 0 | 175,410,177,492 | 29,018,624 | 350,868,743,168 | 46.186356 |

Every completed memory case retained 175,410,177,492 payload bytes, below the
192 GiB limit. Final output is larger because it includes the file and block
headers. Memory mode cut measured host NVMe writes from about 350.9 GB to
175.4 GB. Process RSS also includes other allocations and can exceed the payload
limit without violating it.

The completed Jetstreamer memory retry took longer than its disk case, despite
about half as many host disk writes. Its final export phase was shorter, but
its scan phase was longer. It also used more CPU and much more memory. These
results do not support a general speed gain from this export mode.

Host disk values cover `nvme0n1` during the reader process. They are not direct
file-specific counters. Partition counters overlap and must not be added.
The common warmup procedure does not guarantee identical cache residency.

All four completed Zilla cases reported zero incomplete-body, request-send, and
server-error retries. Jetstreamer disk reported zero permitted body retries,
operation timeouts, and connection recycles. The successful Jetstreamer memory
retry reported zero permitted body retries and operation timeouts, but **32
connection recycles**. The failed first attempt reported 77 recycles and one
operation timeout. These differences remain part of the evidence; the error
policy was not weakened to obtain a completed run.

## Interpretation and next step

Across the two Zilla measurements in each mode, mean total time was 111.598966
seconds for disk and 101.582349 seconds for memory. This is **8.98% less elapsed
time**, or **9.86% higher throughput** when the same total transaction count is
divided by the total measured time for each mode. Mean throughput by that method
is 4.910 million versus 5.394 million transactions per second. This is a summary
of the four observed runs, not a statistical confidence statement.

Zilla's mean export finish time fell from 57.819912 to 50.454590 seconds. Mean
scan time fell from 53.685702 to 51.034889 seconds, but the two memory scans
differed substantially: 56.964339 and 45.105438 seconds. System CPU also differed:
1,567.916 seconds in the first memory run and 331.396 in its repeat. The cause of
that variation was not isolated. Both memory runs completed sooner than their
disk counterpart, including the reverse-order pair, but run order, caching, and
system activity still limit precise attribution.

Mean reader CPU increased from 3,448.437020 to 4,465.997285 seconds, or **29.5%**.
Peak Zilla RSS increased from 12.56–12.69 GiB to 190.06–193.35 GiB. The completed
Jetstreamer memory retry took **5.79% more total time** and **18.8% more CPU**
than its disk case. Thus memory export is an optional tradeoff for this Zilla
benchmark on this host, not a broadly better default for both readers.

Keep disk export as the default. The next proposed design is to write completed
blocks in canonical order while decoding continues, with a bounded memory buffer
and a disk spill path. It must guarantee admission and progress for the earliest
missing block; otherwise later blocks can fill the buffer and prevent the block
needed to drain it from reaching the writer. This design is **not implemented or
measured** here. It could avoid both a full-epoch memory allocation and part of
the serial final-copy phase, but that remains a hypothesis.

## Evidence

The version-2 source archive is pinned by SHA-256
`e6ae737a791a6481da091562c0b37e17128e8fb4cd810377b8cc7801635ed2f8`.
The deployment source record is saved in the artifact as
`control/zilla-local-memory-source-verification-v2.json`. It records these
source hashes before the native build:

| Source | SHA-256 |
| --- | --- |
| Shared export | `da1f7f0ae28143bf0bd3ef5a6af38092e445eef38b4a07a87d783787667f0d6d` |
| Zilla adapter | `1ca9c5ae11ed195f3a3f105bbd8937b022b2f0980e37e8e1baa0cb08f61a059c` |
| Jetstreamer adapter | `c6b64d3823e125ad89469a1395c11e0d222fec3574d3ef0b3c590eba671b0e35` |
| Controller | `66e24102a92ed6f3db538fe151fec7c85820a0607deef382d856b68ab1b50205` |

The [final evidence artifact](artifacts/triton-memory-export-full-epoch-20260930.json)
contains **986,700 bytes** and was collected at **2026-09-30 15:41:38 UTC**. It
retains both final controller states, all seven attempt receipts and logs,
versioned source and build records, source checks, and cleanup evidence. All
collected file snapshots were stable; no expected files were missing and no read
errors were reported. The artifact does not contain CAR or output bytes and does
not create a new output acceptance result.

| Evidence | SHA-256 |
| --- | --- |
| Final evidence artifact | `50542c1bbfbe735836a4780150086ec718c2ebfa48acbef6bad796a71c4aab24` |
| Version-2 native build record | `f9530a09fd1b5e6c4e02089401df53796cb5542483ed3b367c5ea08e7b890ed1` |
| Version-2 native test log | `59fa665e5183f1e37b24a593ec1fc56d1a8fc4ce95b68b2fe2c9f0e613d9b469` |
| Zilla executable | `c4a0a52382fd012f77eac3e99a2a1304f516a1ab95a0e3a0de2c1bc178752755` |
| Jetstreamer executable | `5e104463c3efa357e408c132eae7a28ec26f079b3d424413ce0deea829c27569` |
| Original series status, `FAILED` | `61c3306155d2118318e30af12344e8519f7c2fa7e9d03c41ff52c8b02d03e403` |
| Retry/repeat series status, `MEASURED_UNVERIFIED` | `70d64c161578dbdc4ab269e604d86d3444e4389be878cca908eaa509a5638297` |
| Failed Jetstreamer memory receipt | `b2656fddcfeaa8c50f5b3d9382c81fd6f79c188e11ea05dab1afce1f3f704e8f` |
| Cleanup record | `10576c15ae9b390796cc32ee0545c4748a6ab379c7b3b3a2415d985b504a9b8c` |

The two result directories remain under
`/home/ubuntu/benchmarks/zilla-triton-20260930/` as
`results-dynamic-local-memory-epoch1000-r1` and
`results-dynamic-local-memory-epoch1000-r2`. The local cache and output files were
retained. The server stopped with no server-state error; its process and the
known controllers were absent at cleanup. Free disk space was 3,536,775,528,448
bytes. Its cumulative transport-error counter was 2,605, with only HTTP 200 and
206 responses; that counter does not identify each error's cause and is separate
from reader retries. The earlier benchmark reports remain unchanged and retain
their own evidence and limits.
