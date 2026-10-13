# Resident CAR decode: buffer selection and receipt hashing

The selected **A2 + B** change decoded the full resident epoch in a mean of
**33.385 seconds**, against **33.937 seconds** for its two adjacent P32/W128
baseline runs. It used **1.62% less elapsed time** and **2.13% less CPU time**.
Throughput increased from **16.145 to 16.411 million transactions/s**, a **1.65%
gain**. Both candidate runs were faster than both adjacent baselines. Two runs
per variant do not establish statistical confidence or a general reader ranking.

A2 improves reusable-buffer selection in the shared transaction decoder. B
combines receipt hash updates in the benchmark application. Both changes are
selected; the complete measurement cannot be assigned to either change alone.
The follow-up CPU profile shows a lower share of samples in buffer selection.
The final evidence and cleanup audit are complete.

This test follows the
[resident decode profile](triton-resident-decode-profile-20260930.md). It keeps
the selected **32 parser workers and 128 decode workers** for each variant.
The earlier P64/W64 worker baseline is not the baseline for these code changes.
The test compares Zilla variants on the same local resident input. It does not
compare Zilla with Jetstreamer or measure Triton network throughput.

## Workload and timing

The host is `ubuntu@213.239.141.61`, with 64 physical cores and 128 hardware
threads. Every completed case must decode epoch 1000: **431,781 canonical
blocks and 547,899,266 transactions**. The CAR is 767,389,334,224 bytes, with
recorded SHA-256
`3c9727378e617f5cba8b5e206bce8fc6ae5df34d3a4408eeb69aea4d62ac7218`.

Each reader maps the full CAR and raw index read-only, locks the mappings with
`mlock`, and checks residency before and after the scan. Source identities and
the canonical plan must stay unchanged. The reader copies indexed ranges into
parser buffers. There is no HTTP input, no export, and no separate input-pinning
helper. The configuration uses dynamic scheduling, 128 MiB range copies, a
256 MiB job target, and the same allocator and native diagnostic build settings.

`scan_seconds` includes range copies, CAR parsing, full transaction decoding,
owned metadata decoding, rewards decoding, vote classification, message hashes,
receipt digests, and worker start and join. It excludes source mapping and
preload, residency walks, and profiler handshakes. Stage sampling and perf are
off for the timed code comparisons. The required decode and hash work stays in
each variant.

The comparison runner records the binary hash for each scan. It runs variants
in a mixed order with baseline repeats. This reduces dependence on one first
run, but the small number of repeats does not establish statistical confidence.
CPU seconds and page faults must be assessed with elapsed time. A small time
gain alone does not prove lower CPU cost or lower memory bandwidth use.

## Changes under test

| Variant | Change | Scope |
| --- | --- | --- |
| Baseline | Original byte-buffer selection and five receipt updates | Existing resident diagnostic reader |
| A1, `pool` | One-pass byte-buffer selection, with a fast path for an exact fit at the last index | Shared `of-car-reader` transaction decoder |
| A1 + B, `combined` | A1 plus one 114-byte receipt update per transaction | Shared decoder plus benchmark receipt code |
| A2 + B, `floor` | Capacity-floor selection with reverse search, plus B | Shared decoder plus benchmark receipt code; selected |

The shared decoder selects reusable byte buffers for instruction and lookup
data. The original rule selects the smallest capacity that fits the requested
length. Equal capacities select the highest pool index. If no capacity fits,
it selects the largest available capacity, with the same tie rule. A1 keeps
that rule and avoids the separate fallback scan. It does not reduce the copied
bytes or change retention limits, decoded values, errors, or reuse statistics.

A2 records a lower bound on the capacities accepted by the byte pools. It
searches from the highest index and stops when it finds the smallest possible
fit. The bound can stay below the current minimum after removals. That can
reduce the chance of an early exit but must not change the selected buffer.
Tests cover external recycled buffers, decode errors, pool trimming, and
rejected buffers because they can affect this bound.

B changes only the benchmark receipt calculation in
`bench/reader-profile/src/bin/car-decode-reference.rs`. It builds the same
114 bytes on the stack: slot and index as little-endian 64-bit integers, the
64-byte signature, the 32-byte message hash, then vote and failure bytes. One
BLAKE3 update replaces five updates. Byte order, record order, hash mode, and
finalization stay unchanged. Message hashing is unchanged. If stage sampling
is enabled, the receipt timer includes record construction and the update.

B removes four update calls per transaction, or 2,191,597,064 calls across this
epoch. That is a call-count change, not a measured CPU saving. The earlier
profile assigned 5.565% of sampled transaction-stage wall time to receipt
hashing. That share is not a CPU percentage or a full-scan percentage. Any gain
from B belongs to this benchmark application; it is not a production SDK
receipt-path gain.

## Build history and checks

Build 1 failed at the offline library test with exit code 101. Build 2 completed
its commands but produced a stale combined executable: its hash was identical
to A1's executable despite the changed receipt source. The source and binary
check caught this before measurement. Build 3 rebuilt the combined variant,
and **no scan from the stale combined executable is used**. The raw build-2
record says `PASS` because its commands exited successfully; that state does
not validate its combined executable. Both earlier build records are retained.
Build 4 produced the measured A2 + B executable.

Each measured variant uses a frozen executable and recorded source hashes.
The original receipt source was saved before B was applied, so A1 could be
built without the receipt change. The builds use `release-debug`, mimalloc,
native CPU optimization, frame pointers, and unwind tables. Rust build flags
are `-C target-cpu=native -C force-frame-pointers=yes -C force-unwind-tables=yes`;
the C and C++ flags are `-fno-omit-frame-pointer -g`.

The runner's `result.source_sha256` records the remote checkout at run time.
It is **not necessarily the source used to build the frozen executable**.
In particular, baseline runs reuse the original executable after candidate
sources have been installed. The build records and each variant's frozen
source proof define the source-to-binary mapping.

| Measured executable | SHA-256 |
| --- | --- |
| Baseline, resident diagnostic v2 | `187706e0d2b13210a871d771a4f0d1af2e2a726bf34c94913cf577a119f91d9b` |
| A1, pool v1 | `494ea8a2711f41f76186c7ec9fd3bef9c80e57b9676443b09be25cb973d9371f` |
| A1 + B, combined v2 | `76194405e87d806f94ecc734ec0138ce67b9c6276328aed5b8c85f3c38ea62d6` |
| A2 + B, floor v1 | `34485a67e2fa0f28f9d394300d36d05e3507cfd775108b4101af14ad7b31759a` |

The candidate proofs identify these changed-file hashes:

| Source variant | File | SHA-256 |
| --- | --- | --- |
| Original receipt code, used by A1 | `car-decode-reference.rs` | `40572c223e9835f323704ef9d64258ccbf8e1644428e925d95afea8173f8bc17` |
| B, used by both combined candidates | `car-decode-reference.rs` | `75d8ff0229ce5e772b6ea3c90baa2c4ff5e55b1d78e4a9d95faae6bad1d324d5` |
| A1 | `versioned_transaction.rs` | `4f8434ad19a35c40f19a97f3d79136b021919d30ad6574b60d3518f7b588031c` |
| A2 | `versioned_transaction.rs` | `0ce89e96944713e7488c0b7c2d163f54145c8f526c07467b53e7457636131dce` |

Small tests compare buffer selection with the original algorithm over ordered
pools, duplicate capacities, empty pools, exact fits, and insufficient
capacities. They also check pool contents, allocation reuse, growth counts, and
retention limits. A2 adds tests for its capacity bound. Receipt tests compare
the one-update form with the original five-update form, including all flag
combinations, integer boundaries, BLAKE3 block and chunk boundaries, the empty
stream, and every prefix of a 1,025-record stream. The final selected source
passed **12 native transaction-decoder tests and 45 native reader tests**, with
zero failures. The installed source matches the frozen A2 + B source hashes.

Full-output verification remains **skipped**, as requested. These small code
tests do not constitute a full-epoch output comparison. Completed measurements
still require source identity, full canonical counts, expected vote and failed
transaction totals, no reader errors, and the resident-input checks. A result
may be `MEASURED_UNVERIFIED` when these checks pass; it must not be described as
full-output parity.

## Measurements

All eleven timed scans completed as `MEASURED`. Their containing series are
`MEASURED_UNVERIFIED`. The table preserves execution order, including the fresh
baseline taken before the two comparison series. CPU values are reader process
CPU seconds measured around the scan, not process-lifetime CPU time.

| Series and case | Variant | Scan seconds | Million transactions/s | Scan CPU seconds |
| --- | --- | ---: | ---: | ---: |
| Fresh baseline | Baseline | 33.880915 | 16.171325 | 4,229.674364 |
| Series 1, 01 | A1 | 33.724863 | 16.246153 | 4,189.436498 |
| Series 1, 02 | A1 + B | 33.639979 | 16.287147 | 4,175.348377 |
| Series 1, 03 | Baseline | 33.909988 | 16.157460 | 4,231.766854 |
| Series 1, 04 | A1 + B | 33.626617 | 16.293618 | 4,171.651723 |
| Series 1, 05 | A1 | 33.751468 | 16.233346 | 4,188.662815 |
| Series 1, 06 | Baseline | 34.106811 | 16.064219 | 4,258.440427 |
| Series 2, 01 | A2 + B | 33.384978 | 16.411551 | 4,140.435141 |
| Series 2, 02 | Baseline | 33.946840 | 16.139920 | 4,236.626516 |
| Series 2, 03 | A2 + B | 33.385575 | 16.411257 | 4,143.028063 |
| Series 2, 04 | Baseline | 33.926551 | 16.149572 | 4,227.354353 |

All eleven scans recorded the required 431,781 blocks, 547,899,266 transactions,
302,828,458 votes, and 66,856,874 failed transactions. Source pages were all
resident before and after each scan, with successful locks. Each scan recorded
zero major faults and zero host NVMe read bytes during the scan. Minor faults
remain possible for working allocations. These checks do not establish
full-output parity.

Each mean below uses the two runs of that variant in its own series. Throughput
is total transactions divided by total scan time. The fresh baseline and the
earlier worker-tuning runs are excluded from these comparison means.

| Series and variant | Mean scan seconds | Million transactions/s | Mean scan CPU seconds | Less elapsed time than same-series baseline | Less CPU time |
| --- | ---: | ---: | ---: | ---: | ---: |
| Series 1 baseline | 34.008399 | 16.110704 | 4,245.103640 | — | — |
| Series 1 A1 | 33.738165 | 16.239747 | 4,189.049657 | 0.79% | 1.32% |
| Series 1 A1 + B | 33.633298 | 16.290382 | 4,173.500050 | 1.10% | 1.69% |
| Series 2 baseline | 33.936695 | 16.144744 | 4,231.990435 | — | — |
| Series 2 A2 + B | 33.385277 | 16.411404 | 4,141.731602 | 1.62% | 2.13% |

Series 1 A1 gained 0.80% throughput; A1 + B gained 1.12%. The small difference
between them is consistent with a small extra benefit from receipt batching,
but two runs do not isolate that effect precisely. B was not tested alone.
Series 2 A2 + B gained **1.65% throughput** against its adjacent baselines.
Mean CPU equivalents fell from 124.70 to 124.06 of the 128 logical CPUs. This
shows a small CPU-cost reduction; it does not establish a memory-bandwidth
limit or an allocation-rate reduction.

The selected source retains A2 in the shared decoder and B in the benchmark.
It preserves full decode, buffer-selection rules, and receipt bytes. This is
a modest improvement to the already tuned resident pipeline. It must not be
combined with the earlier worker-count gain to claim an independently measured
code gain, or applied to the earlier export and Jetstreamer results.

## Follow-up CPU profile

The selected A2 + B executable completed a separate **P64/W64** CPU profile in
40.301255 seconds. The capture used `cpu-clock` at 99 Hz with frame-pointer call
chains, gated around the resident scan. It contains **324,197 samples**, with
**zero lost samples reported**. Folded weights are CPU-clock nanoseconds, not
sample counts. Their total is 3,274,717,138,970 ns.

Compare the [A2 + B CPU-time flamegraph](artifacts/triton-resident-decode-optimization-20260930-cpu-time.svg)
with the [original CPU-time flamegraph](artifacts/triton-resident-decode-20260930-r2-cpu-time.svg).
Both profiles use P64/W64. They do not describe the distribution of CPU work at
the P32/W128 configuration used for the speed comparison. The
[profile analysis](artifacts/triton-resident-decode-optimization-20260930-profile-analysis.json)
contains the weighted comparison and its limits.

| Function | Original inclusive share | A2 + B inclusive share |
| --- | ---: | ---: |
| `take_byte_buffer` | 4.936932% | 2.858139% |
| `copy_bytes_reusing` | 5.275759% | 3.308482% |
| New `byte_buffer_index` helper | Not present | 2.452213% |
| `retained_capacity_bytes` | 1.464108% | 1.242454% |

These inclusive shares overlap. The helper is part of the buffer-selection
path, so its share must not be added to its callers' shares. The lower observed
shares support the buffer-selection change, but they do not isolate its exact
contribution to the P32/W128 elapsed-time gain. Inline expansion and changed
code layout also affect attribution.

The largest resolved self functions remain BLAKE3 AVX-512 compression at
15.69%, Zstd sequence decompression at 9.65%, the CAR block-reader inner closure
at 5.37%, and UTF-8 validation at 4.18%. A higher percentage can result from a
lower total CPU cost; it does not by itself show a regression in that function.
**12.39% of weighted leaf samples remain unresolved**. Zero lost samples does
not establish complete symbol resolution or stack unwinding.

Metadata parsing and allocation remain useful next targets. The saved
[self-function report](artifacts/triton-resident-decode-optimization-20260930-perf-self.txt)
includes protobuf string merging, repeated token-balance messages, and allocator
functions. The next candidate is reuse of nested metadata allocations, while
preserving all decoded fields and errors. This is a proposed next change; no
additional metadata implementation or speed measurement is included here.

The first conversion failed because this perf version does not support
`--show-event-period`. Conversion then succeeded with explicit event fields
and inline symbols. The final hash read required `sudo` for the root-owned
`perf.data`. The failed command and permission log remain in the evidence.
The saved trace was reused; no scan was repeated for these conversion steps.

## Evidence and cleanup

The [combined evidence](artifacts/triton-resident-decode-optimization-20260930.json)
is **5,078,628 bytes**, with SHA-256
`4e8284f1221a6cb3c95aeb8c19fc470ad2077085954959b49e2a5abf5c18c041`.
It retains builds 1–4, the original baseline build, the final native test log,
the fresh baseline, all six series-1 scans, all four series-2 scans, and the
completed profile. The evidence state is `MEASURED_UNVERIFIED`; full-output
verification remains skipped.

The final audit covers **12 completed scans: 11 unprofiled and one profiled**.
All source pages stayed locked and resident, source identities were unchanged,
and each scan recorded zero major faults and zero host NVMe read bytes. All
owned reader and perf processes had exited. Their page locks were released;
the host retained only 8,620 KiB of other locked memory. The selected local
source matches its frozen proof.

The [compressed folded stacks](artifacts/triton-resident-decode-optimization-20260930-cpu.folded.gz),
flamegraph, self-function report, and profile analysis are saved with the main
evidence. Raw perf data remain on the benchmark host with recorded hashes.
No result here changes the earlier Jetstreamer comparison or establishes a
network performance ranking.
