# Triton CAR dynamic scheduling

All four initial cases passed exact output checks. Dynamic CAR completed this
32,768-block prefix in 30.501642542 seconds. Static CAR with one HTTP client
took 38.739670011 seconds. This is one sample per case, in fixed order.
The later full-epoch comparison from the public source remains incomplete
because source requests failed. The user selected a local copy for the next
comparison. Five local measurements are complete; see the separate
[local-cache full-epoch report](triton-local-car-full-epoch-20260930.md).
The [earlier full-epoch result](triton-manycore-full-epoch-20260930.md) remains
separate and unchanged.

## Method and code

The host is `ubuntu@213.239.141.61`, with an AMD EPYC 9555P, 64 physical cores,
128 hardware threads, about 1.1 TiB RAM, and NVMe storage. The readers use
Rust 1.98.0 release builds, `-C target-cpu=native`, and mimalloc.
Only one reader ran at a time. The order was static CAR with one client,
static CAR with eight clients, dynamic CAR with eight clients, then Jetstreamer.
Each case used a fresh process. No repeat mean or variance is available.
For the accepted initial series, the 27 CAR Rust tests passed locally and in the
native release build on the server. All 38 Python control tests passed at that
stage. Later code changes and failed attempts are recorded separately below.

The benchmark CAR probe now supports dynamic job allocation. Parsers claim
indexed byte ranges that end at complete block boundaries. Decode workers take
parsed blocks from a shared bounded queue. The raw-block pool and queue capacity
are `decode workers + 2 * parser workers`. Parser and decode counts are separate
in dynamic mode. Static mode retains fixed parser ranges and its existing decode
allocation. Both modes can use multiple independent HTTP clients. Client count
changes connection pools, not the maximum number of parser reads.

All CAR cases used 64 parsers, 64 decode workers, 32 MiB HTTP chunks, owned
metadata, and the same existing buffer-allocation mode. The dynamic target was
256 MiB per job. It produced 239 jobs and a 192-block pool. Each HTTP client
checks the same index, CAR header sample, and object lengths during setup.
The [controller instructions](../../bench/car-export/README-dynamic.md) describe
the command and acceptance checks. Production SDK defaults are unchanged.

Jetstreamer used 64 workers at official Git revision
`048d2cd25af949679c56347440b7cf5a9a900063`, the latest Git head checked for this
test; its package version is still 0.7.0. It used the same frozen executable as
the accepted earlier full comparison. The environment was
`JETSTREAMER_SPAWN_PENDING=1`, `JETSTREAMER_SPAWN_GRACE_SECS=30`, and
`JETSTREAMER_RECYCLE_PCT=50`. The explicit `body-transport` policy retained the
same two exact allowed response-body errors, empty callback-state guards, and
shared limit of two errors per worker and slot. Other errors remained fatal.

## Exact input and output checks

The input was Triton's epoch-1000 CAR at
`https://files.old-faithful.net/1000/epoch-1000.car`. The independent Compact V2
index supplied the canonical plan. The selected prefix covers slots
`[432000000, 432032784)`: **32,768 blocks and 44,966,602 transactions**.
Its full source index was checked against a current complete download, as
recorded in the dataset manifest. Its SHA-256 is
`e65650edc7f486766ccaa9dbe575c876e3844a1e580165b309fe8619eb28ad3a`.

Before and after the four cases, the controller checked the complete 5,184,000-byte
Triton raw slot index, the 767,389,334,224-byte CAR object length, and all 17
64 KiB CAR samples in the manifest. All checks matched. The raw slot-index
SHA-256 is `6b0e72ff321996c23ef3fcd53a02d812141484ec4eea559f45d67113be1b89e3`.
There was no strong CAR ETag. These checks do not prove complete CAR immutability
or pin every HTTP response to one object version.

Each reader matched every canonical row. The native verifier checked every
export record, ordering, lengths, flags, and counts. Each export contained
22,982,484 votes and 6,483,254 failed transactions. All **14,558,449,492 bytes**
matched, with SHA-256
`6f5bf889712d1766a8a676a6fb45a7a75624103fd58db527a2e03048e2fda90a`.
The export includes slot, transaction index, first signature, message hash,
vote/failure flags, fee, and pre/post lamport balances. Equality covers these
fields, not every possible metadata field. Signature verification was off.

## Initial measurements

Total time includes source setup, decoding, spool writes, ordered export, and
final file sync. Scan time is shown separately. Source checks, native output
verification, SHA-256, and complete byte comparison are outside reader timing.
Throughput is transaction count divided by total time, rounded to three decimals.

| Case | Total seconds | Scan seconds | Transactions/s |
| --- | ---: | ---: | ---: |
| Static CAR, 64 parsers / 64 decoders / 1 client | 38.739670011 | 33.222923722 | 1,160,737.869 |
| Static CAR, 64 parsers / 64 decoders / 8 clients | 43.832240765 | 36.208410743 | 1,025,879.609 |
| Dynamic CAR, 64 parsers / 64 decoders / 8 clients | 30.501642542 | 22.821485844 | 1,474,235.426 |
| Jetstreamer, 64 workers | 67.954116884 | 62.852640337 | 661,720.056 |

| Case | Process CPU seconds | Mean CPU cores | Peak RSS, MiB |
| --- | ---: | ---: | ---: |
| Static CAR, 1 client | 237.360444 | 6.0025 | 4944.359375 |
| Static CAR, 8 clients | 272.834312 | 6.1109 | 4980.81640625 |
| Dynamic CAR, 8 clients | 282.219276 | 9.0035 | 5195.83203125 |
| Jetstreamer, 64 workers | 496.500310 | 7.2290 | 1815.921875 |

Mean CPU cores are CPU seconds divided by whole-process time, which also includes
process startup and shutdown. Host network and disk counters can include other
activity. Buffered writes can move physical disk activity outside a measured
interval. Thread and process-I/O samples occur every five seconds and can miss
peaks or the final interval.

Static CAR with one client recovered eight incomplete bodies and four
request-send errors. Static CAR with eight clients recovered five and two,
respectively. Dynamic CAR recovered 23 incomplete bodies and no request-send
errors. Jetstreamer recovered 36 reported body errors while reading blocks,
recorded one connection recycle and 94 work steals, and had no operation
timeouts. All retries remained in elapsed time. CAR and Jetstreamer count
different retry units; these counts are not a direct reliability comparison.

## Interpretation and source limit

Dynamic CAR had 1.270 times the total throughput of static CAR with one client
in these samples. The extra clients alone did not improve the static case.
Client setup took 0.339690677 seconds for static C1, 2.504932856 for static C8,
and 2.608739853 for dynamic C8.

Dynamic parser work totalled 1,228.449228114 worker-wall seconds, including
1,205.600411542 seconds in HTTP reads: 98.1%. Decoders recorded 184.373761147
seconds of work and 1,215.310687848 seconds of queue wait. Their active share was
13.2%. The maximum queue depth observed after enqueue was 3 of 192. These
measurements support testing input concurrency before adding decode workers.
They do not establish a physical network limit. Stage times are sums of worker
wall time, not CPU seconds.

The later tuning set attempted dynamic P64/W64/C1, P128/W64/C8, P128/W64/C16,
and P128/W64/C8 with 128 MiB jobs, plus Jetstreamer with 128 workers. P, W, and C
mean parser, decode-worker, and HTTP-client counts. The smaller jobs test the
final-job delay: 239 jobs permit only about two rounds at 128 parsers.
Jetstreamer's 30-second spawn grace, retries, and connection recycle can affect
this short-prefix result. It must not be used to predict a full-epoch ranking.

Three tuning starts failed during source checks, before any reader ran:
two connection resets and then HTTP 429. These are failed preflights, not reader
measurements. They remain separate from the accepted initial series. The later
controller permits three complete source-check attempts only for connection
resets or incomplete bodies; it still rejects HTTP 429 and any content mismatch.

After a five-minute pause, tuning attempt `r4` passed its source checks before
starting readers. All five reader cases then failed:

| Case | Recorded failure |
| --- | --- |
| Dynamic P64/W64/C1, 256 MiB jobs | A CAR range GET returned HTTP 429 after reading had started. The failed process ended after 22.125997083 seconds. |
| Dynamic P128/W64/C8, 256 MiB jobs | The initial CAR HEAD request returned HTTP 429. |
| Dynamic P128/W64/C16, 256 MiB jobs | The initial CAR HEAD request returned HTTP 429. |
| Dynamic P128/W64/C8, 128 MiB jobs | The initial CAR HEAD request returned HTTP 429. |
| Jetstreamer, 128 workers | The stream closed before the slot range was complete. The receipt contained zero blocks and zero transactions. It did not identify HTTP 429. |

The final source checks also passed and matched the checks before `r4`.
Successful identity checks do not prove that the endpoint can sustain the reader
load. All five samples remain invalid; the controller retained `FAILED` and
produced no accepted summary. The Jetstreamer error is not evidence of HTTP 429
for that reader. No throughput comparison uses these failed samples.

Reader and verifier executable hashes were unchanged between the accepted
initial series and `r4`. The only controller change added the bounded complete
source-check retry described above. No stable variance, full-epoch speed gain,
or general reader ranking is claimed here.

## Resumed remote attempts

The user then requested a full-epoch test. Three further full-epoch runs and one
prefix run failed. The full plan contains **431,781 blocks and 547,899,266
transactions**, over slots `[432000000, 432432000)`. Each run passed all source
checks before its readers started. Each final source check failed with HTTP 429,
so no run has an accepted summary or complete verified output comparison.

| Run directory | Requested cases and recorded failures |
| --- | --- |
| `results-dynamic-full-epoch-1000-r1` | Dynamic P64/W64/C8, 32 MiB chunks, failed on a CAR range HTTP 429 after 32.165762143 receipt seconds. Jetstreamer 64 then reported an early closed stream with zero blocks. Jetstreamer 128 reported `Unknown CID version` after 22,226 blocks and 29,713,453 transactions. |
| `results-dynamic-full-epoch-1000-car32-r1` | Dynamic P32/W64/C8, 32 MiB chunks, failed on a CAR range HTTP 429 after 243.216760708 receipt seconds. The pipeline had decoded 185,407 blocks. |
| `results-dynamic-full-epoch-1000-jet64-r1` | A separate Jetstreamer 64 attempt reported `Unknown CID version` after 15,172 blocks and 19,568,671 transactions. Its failed receipt time was 22.454383773 seconds. |
| `results-dynamic-chunk128-prefix-r1` | Dynamic P64/W64/C8 used 128 MiB chunks for the same 32,768-block prefix. It failed on HTTP 429 after 24.511740154 receipt seconds, with 32,755 blocks decoded and 236 of 239 jobs complete. |

The CAR HTTP body counters were 85,872,886,678 bytes for the full P64 attempt,
355,311,679,156 bytes for full P32, and 64,358,990,746 bytes for the 128 MiB
prefix attempt. These counters include source setup and repeated body bytes.
They are not counts of distinct verified input bytes. The times and partial
counts above describe failures; they are not accepted throughput measurements.

The Jetstreamer receipts do not identify HTTP 429 as the cause of their reader
errors. The final source checks did receive HTTP 429. That does not prove that
HTTP 429 caused either `Unknown CID version` error. These errors remained fatal.

The first three resumed runs used the original dynamic CAR executable. The
fourth used a new benchmark executable that permits indexed HTTP chunks up to
128 MiB. The default remains 32 MiB, and the ordered SDK path still rejects
chunks above 32 MiB. The full 29-test CAR probe suite passed after this change.
The larger chunks did not produce a completed prefix result. Jetstreamer and
the verifier remained unchanged across these four runs.

## Local-cache comparison: complete, output verification skipped

The local tests used one copy of the same epoch-1000 CAR and raw slot
index. The copy was a separate preparation step. Its download time was not
included in reader throughput. A resumable downloader writes disjoint ranges,
checks each response length and range, retains durable SHA-256 checkpoints, and
uses one shared pause for new requests after HTTP 429. A final full-file read
checks all completed job hashes and computes each local file hash. The raw-index
hash and all 17 CAR samples must also match the accepted dataset manifest.

The downloader publishes its completed manifest only after those checks and
after making both files read-only. Local hashes identify the downloaded bytes.
They do not prove that the remote CAR, which has no strong ETag, was unchanged
throughout the download.

Both readers accessed the same files through a server bound only to
`127.0.0.1`. It serves only the two named epoch files. It uses the completed
manifest's hashes as strong ETags and checks file identity before and during
transfers. It rejects a changed file. The server trusts the downloader's
completed verification; it does not hash the full CAR again at startup.
Its real HTTP tests cover byte ranges, conditional requests, concurrent reads,
invalid paths, file changes during transfer, and bounded transfer fallback.

Before each fresh reader process, the controller read the complete local
CAR with a bounded buffer. This preparation is outside reader timing and gives
each reader the same cache-warming method. It does not guarantee that every CAR
page stays in RAM. Readers ran sequentially. The server stayed running
across cases, and its CPU use was measured separately around each reader.
The report shows reader CPU, server CPU, combined CPU, and separate memory
measurements. The server's peak memory value covers its full lifetime.

The local tests are now complete and are documented in the separate
[local-cache full-epoch report](triton-local-car-full-epoch-20260930.md), with
[saved evidence](artifacts/triton-local-car-full-epoch-20260930.json). The user
later asked to skip full output verification for these performance runs.
Their five measurements are therefore `MEASURED_UNVERIFIED`: canonical counts,
receipts, source checks, and output sizes matched, but the outputs have no
independent full-record check, output SHA-256, or confirmed byte parity. These
results do not change the acceptance status of the public-source tests above.

## Evidence identity

The accepted server result is
`/home/ubuntu/benchmarks/zilla-triton-20260930/results-dynamic-initial-32768`.
The [compact evidence](artifacts/triton-dynamic-scheduling-20260930.json)
includes all four accepted case results, receipt metrics, source checks,
settings, commands, verification counters, and original status hashes. It also
preserves the three failed preflights and all five failed `r4` cases, including
available receipts and error logs. The four resumed run directories above are
retained under `/private/tmp/zilla-dynamic-evidence` in the local evidence copy.
Their status hashes are listed below. Repeated host CPU details are compacted.

| Evidence | SHA-256 |
| --- | --- |
| Initial status JSON | `3d051ab72b9f383203edc95988ee06ba5cfc35ff14722b36d5021ff16ac44826` |
| Initial CAR executable | `8358a4ae3c69cc1dc9065d94491b3997ac234f89179dc0d8b75b63ca013136fe` |
| Frozen Jetstreamer executable | `4bfe1e0121013accb7855d049ba0bae2ab960c4f5f9ba0aae9a40bec98a5b0a2` |
| Native verifier | `14813476682bd575c5d7b3b024d45bed7d55a3d8748920e102a2c4fbe2cabb3e` |
| Initial controller | `ffdba4a6a30dbf32724e93240cfa0b9000de32da30f2becc707ea1fe85f9d894` |
| Prefix plan | `8aa7a4a8f1c16fe35e9c220787c2db85af6d23ef2bb281fbd39eedbec5d04db1` |
| Dataset manifest | `f14fe03d29983ee8dbe000d842ee292d6bca83f65e3608050596e1762593e7db` |
| Frozen source archive, `dynamic-benchmark-source.tar.gz` | `6548661cf0861c2be32e5adc9bf62a5a1868f69112cd530ca46cf60a8d4dfb21` |
| Failed tuning `r4` status JSON | `b4e422d7f4368ffac74968fb3d28f6be6598400ea311910f5cfbd5c6f87ad4dd` |
| Failed full-epoch `r1` status JSON | `13361ad29e77a4331d84cdcaef87e4ed873c57288e87815747647fc3dc258004` |
| Failed full-epoch CAR P32 status JSON | `f5e9d91e1dc0c6175d2e65b51ef2b84db21d9270651b6022739a62be63d2ecde` |
| Failed separate full-epoch Jetstreamer 64 status JSON | `a8b7460e9ee13ebcd54119387a8ea3383e781d97f58b7c56a8439a2c540c5206` |
| Failed 128 MiB prefix status JSON | `c33b8559bf42d1208efd298fef2b255d8a8ed06afdcf13639b7a7bcf5b289184` |
| CAR executable for the 128 MiB prefix attempt | `2a3e34a31839a8278bdbce351628a342654981b396647906ae72f22b26850fee` |
