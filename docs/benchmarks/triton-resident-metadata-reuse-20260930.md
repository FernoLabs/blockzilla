# Resident CAR decode: metadata allocation reuse

Metadata allocation reuse reduced mean scan time from **33.398 to 32.527
seconds** in the alternating P32/W128 comparison. This is **2.61% less elapsed
time** and **2.68% higher throughput**, from **16.405 to 16.844 million
transactions/s**. Mean scan CPU time fell by **3.24%**. Both candidate runs were
faster than both same-series baselines. Two runs per variant do not establish
statistical confidence or a general reader ranking.

The baseline is the selected A2 + B build from the
[previous code test](triton-resident-decode-optimization-20260930.md). The
candidate adds metadata log-string and token-balance reuse. The new library
APIs require explicit use; existing APIs are unchanged. The measured gain
applies to this benchmark after it adopts those APIs, not automatically to
every SDK caller. The completed profile shows lower CPU sample shares in the
selected allocator functions. The final evidence and cleanup audit are complete.

## Baseline and workload

The frozen baseline executable is `opt-floor-bin-v1/car-decode-reference`, with
SHA-256
`34485a67e2fa0f28f9d394300d36d05e3507cfd775108b4101af14ad7b31759a`.
It includes capacity-floor buffer selection in the transaction decoder and
the one-update receipt hash in the benchmark. Both remain part of the baseline
for this metadata test.

The host is `ubuntu@213.239.141.61`, with 64 physical cores and 128 hardware
threads. Each timed comparison uses **32 parser workers and 128 decode workers**, dynamic
scheduling, 128 MiB range copies, and a 256 MiB job target. The allocator and
native diagnostic build settings match across measured variants.

The input is the complete local epoch-1000 CAR: **431,781 canonical blocks and
547,899,266 transactions**. Its size is 767,389,334,224 bytes, with recorded
SHA-256
`3c9727378e617f5cba8b5e206bce8fc6ae5df34d3a4408eeb69aea4d62ac7218`.
The reader maps the full CAR and raw index read-only and locks both mappings
with `mlock`. File-identity and residency checks must pass before and after
each scan. Indexed ranges are copied into parser buffers.

The workload has **no HTTP input and no transaction export**. It retains full
transaction, owned metadata, and rewards decoding, vote classification,
message hashes, and receipt digests. The scan timer includes input range
copies, parsing, decode and hash work, and worker start and join. Mapping,
preload, residency checks, and control handshakes remain outside that timer.
Stage sampling and perf are off for the elapsed-time comparison.

These times describe the resident decode pipeline. They are not Triton network
throughput and do not change the earlier Jetstreamer or export comparisons.

## Implementation

The change adds `TransactionStatusMetaReuse`,
`decode_transaction_status_meta_from_frame_reusing`, and
`decode_transaction_status_meta_into_reusing` in the shared reader library.
The benchmark's owned decoder calls these APIs to keep selected allocations
between decoded transactions. The original APIs remain unchanged. Reuse targets
**log-message `String` values and `TokenBalance` objects**. Fields stay owned,
and the decoder still processes all fields.

The cache limit is **1 MiB per decode worker**, with at most **512 log
strings and 128 token-balance objects** retained. At 128 workers, the byte limits
allow at most 128 MiB of retained cache. This is a cache limit, not a bound on
the full reader's RSS or the size of a currently decoded metadata object. The
limit includes the actual capacity of both pool vectors and all string buffers
they retain. Objects that do not fit are dropped. The cache does not grow beyond
its reserved pool capacity.

The reuse path clears previous contents, then uses prost's string and message
merge functions for the pooled fields. Other fields use the original generated
merge operation. Tests check decoded values, repeated-field order, merge
behavior, and errors against the existing owned decoder. Optional
`UiTokenAmount` values are still dropped normally; their nested strings are not
pooled. The required transaction and receipt hash work remains unchanged.

This change has two parts: reusable metadata storage in the shared library,
and use of its API by the benchmark. A measured benefit in this benchmark does
not by itself establish a benefit for every library caller or input.

## Validation and build identity

The native build passed **22 metadata-decoder tests, including seven new test
groups, and 45 reader tests**. The tests compare the reuse path with the existing
owned decoder. They cover repeated fields, duplicate-field merge behavior,
empty and absent fields, malformed input, recovery after decode errors, and
large and small metadata objects. Cache tests cover item limits and the
retained-byte limit.

The measured candidate is `opt-meta-bin-v1/car-decode-reference`, with SHA-256
`e83dd4313e6ca248173f9e95cbd6584d33e31e6fd8e48cf9d623558d0005604c`.
It uses mimalloc and `release-debug`, with Rust flags
`-C target-cpu=native -C force-frame-pointers=yes -C force-unwind-tables=yes`
and C/C++ flags `-fno-omit-frame-pointer -g`.

| Frozen candidate source | SHA-256 |
| --- | --- |
| `metadata_decoder.rs` | `9c1a56e287d7a4b2e15d8d26453960082a0b0803872fe2391f69e5ff3b9e9252` |
| `metadata_decoder/owned_reuse.rs` | `8242ea2aa1ba477109f84b9d7ca4284ca85819cdbcf0b60c17acbcfcad30b551` |
| `versioned_transaction.rs` | `0ce89e96944713e7488c0b7c2d163f54145c8f526c07467b53e7457636131dce` |
| `car-decode-reference.rs` | `fdf4e4bf20c98cbcddddb8a394b51d5c08c4c606a2ed24ef3f123b37582a91ba` |

Full-output verification remains **skipped**, as requested. Small decoder
tests do not constitute full-epoch output parity. Completed scans must still
retain source identity, the complete canonical block and transaction counts,
expected vote and failed-transaction totals, no reader errors, and resident
input checks. Report states must retain this distinction.

The elapsed-time comparison used alternating baseline and candidate runs
at P32/W128. Each result identifies its frozen executable. Build records
and frozen source proofs define the source-to-binary mapping; a checkout hash
recorded at run time does not necessarily describe a reused baseline binary.
The candidate build and all five unprofiled scans completed successfully.

## Measurements

The fresh baseline ran before the alternating series. It is retained below
but excluded from the primary means. Each case is `MEASURED`; the containing
series are `MEASURED_UNVERIFIED`. CPU values are process CPU seconds measured
around the scan, not process-lifetime CPU time.

| Case, in execution order | Scan seconds | Million transactions/s | Scan CPU seconds | Scan minor faults |
| --- | ---: | ---: | ---: | ---: |
| Fresh A2 + B baseline | 33.376934 | 16.415506 | 4,134.455882 | 24,571 |
| 01 baseline | 33.392150 | 16.408026 | 4,141.019268 | 25,711 |
| 02 metadata reuse | 32.540158 | 16.837634 | 4,008.147335 | 23,278 |
| 03 baseline | 33.404698 | 16.401863 | 4,142.366311 | 23,994 |
| 04 metadata reuse | 32.514680 | 16.850828 | 4,006.991313 | 26,842 |

All five scans decoded 431,781 canonical blocks and 547,899,266 transactions.
They recorded the expected 302,828,458 votes and 66,856,874 failed transactions.
All CAR and index pages were locked and resident before and after each scan.
Each scan recorded zero major faults and zero host NVMe read bytes during the
scan. Minor faults can still occur for working allocations.

| Same-series mean | Scan seconds | Million transactions/s | Scan CPU seconds | Mean CPU equivalents |
| --- | ---: | ---: | ---: | ---: |
| A2 + B baseline | 33.398424 | 16.404944 | 4,141.692790 | 124.01 |
| Metadata reuse | 32.527419 | 16.844228 | 4,007.569324 | 123.21 |

Throughput is total decoded transactions divided by total scan time for the
two runs of each variant. The **2.61% elapsed-time reduction**, **2.68%
throughput gain**, and **3.24% CPU-time reduction** use only the alternating
series. The fresh baseline and earlier optimization results are excluded.
CPU equivalents are aggregate CPU seconds divided by elapsed seconds; they
are not physical core counts.

The candidate improves this resident workload while preserving full owned
decode and the required hash work. These measurements do not directly count
allocations or memory bandwidth. The two-run samples limit confidence in the
precise gain. They also do not establish full-epoch output parity or a gain
for callers that continue to use the original library APIs.

## Follow-up CPU profile

The metadata-reuse executable completed a separate **P64/W64** profile in
**39.510687 seconds**. The capture used `cpu-clock` at 99 Hz with frame-pointer
call chains, gated around the resident scan. It contains **319,104 samples**,
with **zero lost samples reported**. Folded weights are CPU-clock nanoseconds,
not sample counts; their total is 3,223,272,695,040 ns.

Compare the [metadata-reuse CPU-time flamegraph](artifacts/triton-resident-metadata-reuse-20260930-cpu-time.svg)
with the [previous A2 + B flamegraph](artifacts/triton-resident-decode-optimization-20260930-cpu-time.svg).
Both use P64/W64. The comparison baseline already includes the transaction
buffer-selection and receipt-hash changes. These profiles do not describe the
CPU distribution at the P32/W128 configuration used for the speed measurements.
The [profile analysis](artifacts/triton-resident-metadata-reuse-20260930-profile-analysis.json)
records the weighted results and their limits.

| Selected allocator function | A2 + B self share | Metadata-reuse self share |
| --- | ---: | ---: |
| `_mi_page_malloc_zero` | 2.66% | 1.60% |
| `mi_free` | 1.62% | 0.92% |
| `mi_theap_malloc_aligned` | 1.45% | 0.75% |
| `RawVecInner::finish_grow` | 1.26% | 0.52% |

These values are CPU sample shares for selected self functions. They are not
allocation counts, and they do not include every allocation cost. The lower
shares support the reuse change, but they do not measure its exact contribution
to the P32/W128 elapsed-time gain.

The largest resolved self functions remain BLAKE3 AVX-512 compression at
15.61%, Zstd sequence decompression at 9.56%, the CAR block-reader inner closure
at 5.42%, and UTF-8 validation at 4.73%. UTF-8's share increased from 4.18% in
the prior profile. A lower total CPU cost, changed code layout, and inline
attribution can change these percentages; that increase alone does not
establish a UTF-8 regression. The complete saved
[self-function report](artifacts/triton-resident-metadata-reuse-20260930-perf-self.txt)
provides the individual symbols.

**12.84% of weighted leaf samples remain unresolved**. Zero lost samples does
not establish complete symbol resolution or stack unwinding. Profile conversion
completed successfully with no conversion failure in this round. The optional
`UiTokenAmount` strings remain outside the reuse cache; no further reuse change
or measurement is included in this test.

## Evidence and final choice

The [combined evidence](artifacts/triton-resident-metadata-reuse-20260930.json)
is **2,709,148 bytes**, with SHA-256
`b08fcbaa40ee46d10b346783d90496c300947fe323ec26ced46d9748975b93d7`.
It retains the four alternating cases, fresh baseline, completed profile,
native tests, baseline and candidate build records, source proofs, conversion
record, and final audit. Its state is `MEASURED_UNVERIFIED`; full-output
verification remains skipped.

The final audit covers **six completed scans: five unprofiled and one profiled**.
All source pages stayed locked and resident, source identities stayed unchanged,
and every scan recorded zero major faults and zero host NVMe read bytes. All
owned reader and perf processes had exited. Their page locks were released;
the host retained 8,620 KiB of other locked memory. The selected local source
matches its frozen proof.

The [compressed folded stacks](artifacts/triton-resident-metadata-reuse-20260930-cpu.folded.gz),
flamegraph, self-function report, and profile analysis are saved with the
combined evidence. Raw profile data remain on the benchmark host with recorded
hashes.

The selected change retains the new reuse APIs and their use in the benchmark's
owned decoder. Existing library APIs remain unchanged. The measured benefit is
an additional resident-decode gain over A2 + B; it does not change the earlier
network, export, or Jetstreamer results.
