# Resident CAR decode profile: measured CPU costs and worker tuning

Two resident decode runs with **32 parser workers and 128 decode workers**
averaged **33.937 seconds**, or **16.14 million transactions/s**. Three unprofiled
P64/W64 baselines averaged **41.501 seconds** and 13.20 million transactions/s.
The tuned runs used **18.23% less elapsed time**, delivered **22.29% higher
throughput**, and used **25.94% more CPU time**. Use P32/W128 for this resident
decode workload; the change is worker allocation, not a decoder algorithm change.
The available repetitions do not establish statistical confidence or a general
reader ranking.

The evidence points to decode work as the main constraint at P64/W64. Decoders
used about 79% of measured worker CPU, while parsers spent substantial time
waiting for free buffers. Sampled stages identify metadata processing as a large
cost, and CPU stacks identify hashing, decompression, and copying among the largest
individual functions. These findings guide the next code work; they do not show
that any field or validation can safely be omitted.

The first series remains **`FAILED`** because two profiler attempts stopped at
the start handshake. The corrected profiling series and worker-tuning series are
**`MEASURED_UNVERIFIED`**. The confirmation series has the same measured state.
Ten scans completed; two failed attempts remain in the record. Code tests passed:
**43 native reader tests and 11 runner tests**.

The task is to identify the limit in Zilla's full decode pipeline on
`ubuntu@213.239.141.61`. The host has 64 physical cores and 128 hardware threads.
The epoch-1000 input contains **431,781 canonical blocks and 547,899,266
transactions**. Its CAR size is 767,389,334,224 bytes, with recorded SHA-256
`3c9727378e617f5cba8b5e206bce8fc6ae5df34d3a4408eeb69aea4d62ac7218`.

This workload reads local memory and performs full transaction, owned metadata,
and rewards decoding, plus message and receipt hashes. It has **no HTTP input
and no transaction export**. It is a separate workload from the earlier
[memory export test](triton-memory-export-full-epoch-20260930.md). Its times must
not be presented as a gain over that test or as direct Triton network throughput.

## Resident input

The reader opens the complete CAR and raw slot index as read-only mappings and
locks both mappings with `mlock`. It checks file identities against the completed
cache manifest. The runner also checks the full raw-index hash and canonical
plan. It trusts the earlier completed CAR hash; it does not repeat a full CAR
hash pass for these runs.

The mappings remain locked inside the reader for the run. Full `mincore` walks
check residency before and after the scan. A lock failure, changed file identity,
or nonresident page prevents a successful resident-input receipt. There is no
separate pinning helper and no second full anonymous copy of the CAR. Successful
`mlock` keeps mapped pages resident until unlock; `mincore` separately records the
page residency checks.
[Linux mlock documentation](https://man7.org/linux/man-pages/man2/mlock.2.html),
[Linux mincore documentation](https://man7.org/linux/man-pages/man2/mincore.2.html).

A new indexed resident-range transport feeds the existing dynamic worker
pipeline. It copies selected ranges from the locked mapping into parser buffers;
this is not a zero-copy decode path. The starting configuration uses **64 parser
workers and 64 decode workers**, 128 MiB range copies, and a 256 MiB job target.
The resident transport uses one shared mapping, with the compatibility setting
`--http-clients 1`; it makes no HTTP requests.

## Timing and instrumentation

The diagnostic runner is `bench/car-export/run_resident_profile.py`. The reader
first completes mapping, locking, and its initial residency checks, then signals
that resident input is ready. The runner enables the selected profiler through
its control FIFO and acknowledges the start gate before the scan begins. It
disables collection when the scan completes. Profiler command and acknowledgement
times are retained.

`scan_seconds` includes memory range copies, CAR parsing, complete transaction,
metadata and rewards decode, hashes, worker startup, and worker joins. It excludes
mapping and preload, residency walks, and waits for the profiler handshake.
Profile collection is gated around that scan rather than the full process.
The reader's scan timer and profiler enable interval have distinct boundaries;
their recorded timestamps must remain separate.

The optional stage run samples approximately **one transaction in 1,024** for
transaction stages. It records sample counts and the configured interval. Block
stages are measured for every block. The diagnostic data separates transaction
deserialization, metadata decompression and deserialization, vote classification,
message hashing, receipt hashing, recycling, and block work. It also records
worker and process CPU, queue waits, and scan page faults. Sampled stage durations
are wall times, not CPU seconds. Worker durations overlap and cannot be added to
obtain elapsed time.

The initial sequence was baseline, sampled stages, CPU stacks, hardware counters,
and baseline repeat. The failed profiler cases were then repeated. The worker
test used P32/W64, P32/W96, and P32/W128, followed by a third P64/W64 baseline.
Only parser and decode worker counts were selected differently in that test.
The derived buffer budgets changed with those counts: parser range buffers fell
from 8 GiB to 4 GiB, and the raw-block pool remained `decoders + 2 × parsers`.
Range-copy size and job size stayed unchanged. Stage timers and perf were off
for the worker tests and the subsequent P32/W128 confirmation.

All measured cases use the **same diagnostic executable**, allocator, and input.
The unprofiled baselines describe this binary, not the older release executable.
The P64/W64 profiled scans took 41.413 and 41.426 seconds, near the unprofiled
41.429–41.613-second range. The available repetitions do not isolate a precise
profiler overhead. The CPU profile describes **P64/W64**, not the tuned P32/W128
configuration.

## Build and profiler scope

The version-2 native diagnostic build passed. It retains debug symbols and native
CPU optimization and enables frame pointers. Its executable SHA-256 is
`187706e0d2b13210a871d771a4f0d1af2e2a726bf34c94913cf577a119f91d9b`.
The frozen build settings and versioned runner hashes remain in the source and
controller records. Frame pointers can change code generation; using the same binary for
all cases avoids assigning that difference to profiler overhead.

The CPU-stack case uses **`cpu-clock` at 99 Hz with frame-pointer call chains**.
It samples CPU work, including permitted kernel work, rather than blocked wall
time. Collection attaches to the controlled reader process after preload and
starts disabled until the FIFO gate opens. Symbol resolution, sample counts,
and lost-record diagnostics were checked; the limitations are reported below.
The saved launch command is the authority for the final event and settings.
[Linux perf record documentation](https://man7.org/linux/man-pages/man1/perf-record.1.html).

The separate counter case requests task clock, context switches, CPU migrations,
page faults, major faults, and a grouped cycles/instructions pair. Raw values,
unsupported-event messages, and event running times are retained. IPC and cache
behavior cannot by themselves prove a bottleneck. This first counter set does
not measure DRAM bandwidth.
[Linux perf stat documentation](https://man7.org/linux/man-pages/man1/perf-stat.1.html).

The flamegraph conversion uses **inferno 0.11.21**. Raw perf data and folded
stacks are retained with the SVG. A flamegraph describes the distribution of collected CPU
samples; queue wait time requires separate timing evidence.
[Inferno source](https://github.com/jonhoo/inferno).

The runner launches controlled child processes through `sudo -n` and gives the
reader an unlimited process memlock limit with `prlimit`. It records exact child
PIDs, executable identities, and process start times; cleanup targets only those
owned processes. Global perf settings are unchanged. The resident reader closes
its mappings at exit, which releases the page locks.

## Checks and limits

The user's request to skip full-output verification remains in effect. No
transaction export is produced in this workload. Canonical block and transaction
counts, source identity, decode errors, and the known vote/failure totals remain
required. A CPU profile does not establish full-output byte parity.

The process maps and touches the full source, so its RSS includes shared mapped
CAR pages. Report mapped and locked source bytes separately from working buffers;
RSS cannot be compared directly with the earlier HTTP reader's RSS. Locked source
pages remove source-page eviction as an input variable, but they do not remove
range copies, allocation, cache effects within the CPU, or memory bandwidth limits.

Perf samples measure active CPU work. Stage timers can include scheduling delays.
Per-worker CPU, queue waits, scan faults, and hardware counters must be read
together. Failed, incomplete, or unsupported diagnostics must remain visible and
must not become successful performance samples.

## Measurements

The three unprofiled P64/W64 scans took **41.460212, 41.613306, and 41.428667
seconds**. Their mean was **41.500729 seconds**, or **13.20 million transactions
per second** using total transactions divided by total scan time. All rates below
describe the resident decode pipeline with no HTTP or export.

| Case, in execution order | State | Scan seconds | Transactions/s | Scan CPU seconds | Scan minor faults | Scan major faults |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Baseline P64/W64, first | `MEASURED` | 41.460212 | 13,215,061.720 | 3,359.214172 | 30,954 | 0 |
| Sampled stages P64/W64 | `MEASURED` | 41.501555 | 13,201,897.302 | 3,360.245286 | 21,625 | 0 |
| CPU stacks, first attempt | `FAILED` before scan | — | — | — | — | — |
| Hardware counters, first attempt | `FAILED` before scan | — | — | — | — | — |
| Baseline P64/W64, repeat | `MEASURED` | 41.613306 | 13,166,444.003 | 3,362.740158 | 19,913 | 0 |
| CPU stacks P64/W64, corrected runner | `MEASURED` | 41.413175 | 13,230,071.548 | 3,352.988467 | 20,354 | 0 |
| Hardware counters P64/W64, corrected runner | `MEASURED` | 41.426142 | 13,225,930.399 | 3,354.263899 | 18,413 | 0 |
| Worker test P32/W64 | `MEASURED` | 41.473892 | 13,210,702.869 | 3,325.279492 | 17,681 | 0 |
| Worker test P32/W96 | `MEASURED` | 34.860966 | 15,716,697.671 | 4,059.142194 | 21,579 | 0 |
| Worker test P32/W128 | `MEASURED` | 33.921265 | 16,152,088.484 | 4,223.883660 | 21,972 | 0 |
| Baseline P64/W64, third | `MEASURED` | 41.428667 | 13,225,124.099 | 3,353.919603 | 18,309 | 0 |
| Confirmation P32/W128 | `MEASURED` | 33.951778 | 16,137,572.336 | 4,235.677593 | 24,031 | 0 |

All ten completed scans decoded the complete canonical plan and retained the
known vote and failed-transaction counts. Before and after every completed scan,
all **187,350,912 CAR pages** and **1,266 index pages** were resident, with
4,096-byte pages. The mappings stayed locked. Each reader made zero HTTP requests.
Its source counters, including the header read during setup, recorded
767,372,677,966 copied bytes through 8,546 range reads. The range-buffer
allowance was 8 GiB for P64 cases and 4 GiB for P32 cases. Minor faults can still
occur for working allocations even when source pages remain locked.

The first baseline used **717.990 parser CPU seconds** and **2,641.059 decoder
CPU seconds**. Decoders therefore used **78.6% of measured worker CPU time**.
The repeat was similar: 711.665 parser and 2,650.916 decoder CPU seconds.
The first baseline's summed parser queue wait was 1,917.415 worker-wall seconds;
decoder queue wait was only 3.727. The repeat recorded 1,934.203 and 4.623,
respectively. These queue times overlap across workers and are not elapsed time
or CPU time. They indicate that the current parser stage keeps the decode stage
supplied; they do not by themselves prove whether decode instructions, CPU cache,
or memory bandwidth is the limiting resource.

The stage run took 41.501555 seconds, within the baseline range. This small
sample does not isolate a precise instrumentation overhead. It timed **534,665
transactions** from the 547,899,266 decoded transactions. The following fractions
use only the sum of the seven sampled transaction-stage wall times as their
denominator. They are **not CPU percentages or full-scan percentages**.

| Sampled transaction stage | Raw sampled wall seconds | Share of sampled transaction-stage time |
| --- | ---: | ---: |
| Metadata deserialization and reset | 0.910546959 | 33.569% |
| Metadata decompression | 0.711898073 | 26.246% |
| Message hashing | 0.451398291 | 16.642% |
| Transaction deserialization | 0.391573410 | 14.436% |
| Receipt digest | 0.150938675 | 5.565% |
| Transaction recycling | 0.074284699 | 2.739% |
| Vote classification | 0.021795495 | 0.804% |

Metadata decompression and deserialization form the largest combined share in
these stage samples. The percentages above do not justify skipping any
metadata fields or changing the full-decode workload. All-block timers separately
recorded 0.504356 worker-wall seconds for rewards and 0.291092 for the final block
digest; those values are excluded from the sampled-transaction denominator.

## CPU profile

The corrected P64/W64 capture contains **329,962 samples**, with **zero lost
samples reported**. It used `cpu-clock` at 99 Hz. Each folded weight represents
CPU-clock event periods in nanoseconds, not a sample count. The total folded
weight is 3,332,949,461,620 ns, or 3,332.949 sampled CPU seconds. Dividing by the
10,101,010 ns period gives the recorded sample count.

Use the [corrected CPU-time flamegraph](artifacts/triton-resident-decode-20260930-r2-cpu-time.svg).
Its title and tooltips now describe CPU time. The correction did not change stack
geometry or event weights; the original remote SVG remains preserved.
The [profile analysis](artifacts/triton-resident-decode-20260930-r2-profile-analysis.json)
records hashes, conversion details, and interpretation limits.

The five largest resolved self functions in the saved perf report are:

| Function | Rounded self share |
| --- | ---: |
| BLAKE3 AVX-512 compression, `_blake3_compress_in_place_avx512` | 15.25% |
| Zstd sequence decompression, `ZSTD_decompressSequences_default.constprop.0` | 9.45% |
| CAR ordered block-reader inner closure | 5.29% |
| Transaction buffer copying, `copy_bytes_reusing` | 5.27% |
| UTF-8 validation, `core::str::converts::from_utf8` | 4.04% |

These are self shares, not inclusive call-tree shares or complete stage costs.
Allocation and protobuf functions also appear in the saved report. Their work can
belong to metadata or other callers, so those categories cannot simply be added
to inclusive stage shares.

**12.44% of weighted leaf samples remain unresolved** in the collapsed stacks:
10.59% appear as a libc module placeholder and 1.85% as an executable placeholder.
Zero lost samples does not prove complete symbol resolution or stack unwinding.
Expanded inline calls and semicolons inside Rust type names also affect encoded
stack depth; that depth is not a physical frame-pointer depth measurement.

The separate P64/W64 hardware-counter capture recorded:

| Counter | Value |
| --- | ---: |
| Task clock | 3,353,054.38 ms |
| Context switches | 440,903 |
| CPU migrations | 12,800 |
| Page faults | 18,413 |
| Major faults | 0 |
| CPU cycles | 12,448,141,101,233 |
| Instructions | 24,548,321,672,278 |
| Instructions per cycle | 1.972047 |

The [saved perf output](artifacts/triton-resident-decode-20260930-r2-perf-stat.csv)
reports **100.00% event running time** for each counter.
This gives no evidence of counter multiplexing in that run. It is an aggregate
P64/W64 result, not a P32/W128 counter result. IPC alone does not identify a
bottleneck, and these events do not measure memory bandwidth. Counter collection
and the reader's own CPU measurements have slightly different boundaries.

## Worker balance and next code targets

Reducing parsers from 64 to 32 while keeping 64 decoders left scan time near
41.5 seconds. Adding decode workers then reduced elapsed time in the completed
test. CPU cost increased as more work ran in parallel:

| Configuration | Parser CPU seconds | Decoder CPU seconds | Mean CPU equivalents | Parser wait, worker-wall seconds | Decoder wait, worker-wall seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| P64/W64, first baseline | 717.990 | 2,641.059 | 81.023 | 1,917.415 | 3.727 |
| P32/W64 | 679.762 | 2,645.406 | 80.178 | 645.450 | 3.374 |
| P32/W96 | 737.389 | 3,321.598 | 116.438 | 373.283 | 9.225 |
| P32/W128 | 733.802 | 3,489.908 | 124.520 | 29.263 | 300.472 |
| P32/W128, confirmation | 732.022 | 3,503.467 | 124.756 | 20.968 | 311.500 |

CPU equivalents are aggregate CPU seconds divided by scan seconds, not physical
core counts. The two P32/W128 runs averaged about 124.6 of the machine's 128 logical CPUs.
Its decoder queue wait increased and parser wait fell, which shows that the
balance between stages changed. This does not prove memory-bandwidth saturation.

The two P32/W128 scans took 33.921265 and 33.951778 seconds. Their mean of
33.936521 seconds is **18.23% below** the three-baseline mean, with **22.29% higher
throughput**. Mean scan CPU increased from 3,358.624644 to 4,229.780627 seconds,
or **25.94%**. Moving from P32/W96 to the first P32/W128 run added only **2.77%
throughput** for **4.06% more CPU time** in those individual runs. Keep these
measurements separate from the P64/W64 CPU profile and from the earlier
output-export comparison.

The next code targets are metadata/protobuf allocation and copying, reusable
transaction buffers, and the hashing path. The measurements justify examining
these paths while preserving full decode and the required hash results. No
decoder algorithm optimization was applied in this test; the measured change is
worker allocation. Any later code change needs its own identical-workload test.

## Failed attempts and source history

The initial stack and counter attempts failed because the runner rejected perf's
acknowledgement format, which included a NUL byte. The reader was still waiting
at the start gate, so these attempts have no completed scan result. Both failed
attempts and their controlled cleanup remain in the first series. The runner
handling was corrected, and its 11 tests passed. The second series completed the
stack and counter captures with the same reader binary. The original series
remains `FAILED`; later successful diagnostics do not erase those attempts.

After measurement, explanatory receipt strings and the converter's event-unit
title were corrected. Those are label changes, not new decode measurements.
The measured version-2 reader binary and its recorded hash remain unchanged.
The final source-label check passed all 43 native reader tests. The updated
converter built successfully and processed the actual retained `perf.script`.
Its folded output matched the original exactly; its corrected event-unit labels
were checked. These source checks and proofs are separate from the frozen
measurement build.

## Evidence

The saved controller records used here are
`/private/tmp/zilla-resident-r1-status.json`,
`/private/tmp/zilla-resident-r2-status.json`, and
`/private/tmp/zilla-resident-r3-status.json`, with confirmation in
`/private/tmp/zilla-resident-r4-status.json`. The first state is `FAILED`; the other
three are `MEASURED_UNVERIFIED`. The corrected series retain controller hash
`b586bcb8cadd1f0031d5152c377e4246e657f4887149014df666e4dfc34414d0`.

The [combined evidence artifact](artifacts/triton-resident-decode-profile-20260930.json)
was created at **2026-09-30 17:34:55 UTC**. It contains **2,329,819 bytes**, with
SHA-256 `7ae5794d84425900fc93f11e74dc120e6f9713ca3257829bc97ef6a42be70fd6`.
It retains all four series, the ten completed scans and two failed attempts,
build and source proofs, stage data, raw counter output, CPU-profile analysis,
and final cleanup. Its summary is `MEASURED_UNVERIFIED`; the first series remains
`FAILED` within that evidence.

The final audit found all owned reader and perf processes absent or stopped.
The test's mappings and page locks were released. Host `Mlocked` remained at
8,620 KiB from other system use, rather than the test's approximately 767 GB.
The cache identities were unchanged. Source files and raw profiling data remain
available for review. The profile files establish measured CPU activity, not
full-output byte parity or performance through Triton's network endpoint.
