# V2 USDC: one metadata pass

This report records the metadata fix before indexed output and the rolling
pipeline. See the later [paired pipeline benchmark](epoch-300-rolling-pipeline-2026-09-06.md)
for the current scheduling result. The validation and measurements below apply
to the builds named in this report.

Status: reader patch verified. Outputs match. Elapsed time improved, but the
10% performance threshold against the old reader was not met.

## Change

The fix is in `blockzilla-compact-v2-reader`. The USDC example already asks
for execution status and recorded token balances together. The reader now
returns both from the token-balance metadata traversal and reuses the parsed
rows when it resolves the requested mint. It no longer runs a separate
metadata count traversal first for this request.

The combined traversal still validates the complete record. It returns status
and CPI presence for the existing error, inner-instruction and loaded-address
flag checks. Omitted failed balances are consumed and validated without being
retained. Buffer clearing prevents stale rows after failures or empty records.
The public token-projection method keeps its signature and retains failed
balances when status filtering is not requested. The example, output format,
V3 split-token path, Pump.fun path and count path are unchanged.

## Validation

All 388 selected reader, workload and example tests passed, with all relevant
features enabled. Five new regression tests plus an extended historical-field
test cover combined projection, both metadata schemas, padded historical
integers, status-disabled requests, incorrect flags, damaged retained and
discarded metadata, buffer reuse, and unknown status. The first test attempt
could not open a local HTTP listener in the sandbox; the rerun with local
test-server access passed. Workspace formatting also passes.

The full workspace run passed 3,523 tests, with one ignored and no failures
(129 test harnesses). The 388 selected tests overlap that total. Independent
code review found no correctness blocker.

## NAS retest

The epoch-300 sequence is old / patched / patched / old. Each scan uses the
same local SSD archive, SSD output, 12 workers and 3 GiB shared-registry cap.
Every output is retained. Exact output comparison occurs after the timed
scans. The previous unpatched-refactor pair (109.547 seconds on average)
remains a historical reference; the old baseline is measured again now.

- Control: `/volume2/blockzilla-bench/control/epoch300-usdc-single-pass-20260906T154800Z/`.
- Results: `/volume2/blockzilla-bench/results/epoch300-usdc-single-pass-20260906T154800Z/`.
- Source base: `2672d42714da7560672b7a99b420c8315eb733e2` plus the saved patch.
- Full source patch SHA-256: `fc23ac64eb6a5a1e7da103bf6219f2716ce6a94e0f1440d1d2cf9b1a491c927d`.
- Patched binary SHA-256: `2e3c61c71f2c48e6940014fc6cfe68824fbc95eac04d90f77f9818b43eff082c`.
- Compiler: Rust 1.98.1, Linux x86-64 musl, release, `-C target-feature=+aes,+sse2`.

The source patch includes the user's existing model error-message edit, which
this fix does not modify. Binary and source-patch hashes were verified on the
NAS before launch. The compressor was already stopped; the retest observes
its process state and CPU counter and sends it no signal.

Acceptance requires matching output bytes, report fields and recorded input
and host settings, a patched mean within 10% of the fresh old baseline, and
less than 10% spread within each pair. These are investigation thresholds,
not statistical confidence intervals. This retest covers epoch 300 USDC;
it does not restart the full matrix.


## Results

| Version | Full epoch runs (s) | Mean (s) |
| --- | --- | ---: |
| Old reader, fresh baseline | 73.683, 78.701 | 76.192 |
| Refactor before fix, earlier pair | 108.649, 110.445 | 109.547 |
| Refactor with one metadata pass | 88.338, 89.690 | 89.014 |

The patch reduces elapsed time by 18.7% against the earlier refactor pair.
It remains 16.8% above the fresh old baseline. Pair spread is 6.81% for the old
reader and 1.53% for the patch. The result status is `NEEDS_ATTENTION` because
the performance threshold was not met, even though all correctness checks pass.

All four new outputs match the saved reference byte for byte: 30,224,439 rows,
4,110,523,748 bytes, unchanged schema, totals and coverage. Each scan reports
408,989 blocks, 724,730,034 transactions and 111,179,816 omitted known failures.
All 12 workers were used, with 12 active at once. The shared registry contains
432,762,560 bytes. The compressor stayed stopped and its CPU counter remained
1,154.54 seconds; no signal was sent to it.

Separate bounded timings used the first 2,048 blocks, one warmup and three
measured iterations, with output serialization to a discard sink. Counters,
source reads and coverage match across versions. These diagnostic counters do
not replace the full output byte checks above.

| Workers | Old median (s) | Before fix (s) | One pass (s) |
| ---: | ---: | ---: | ---: |
| 1 | 1.554 | 2.183 | 1.849 |
| 12 | 0.510 | 0.644 | 0.555 |

The remaining gap is in projection. The old reader skipped flagged failures
before validating messages and metadata. The patched reader keeps those
checks. There is no second USDC metadata traversal left. This does not prove
that the remaining parsing cost cannot be reduced.

An attribute-only inline experiment in a separate source copy produced
byte-identical executable ELF sections, including the entire `.text` section.
It was not applied or timed on the NAS.

Correction after the indexed-output retest: the allocation counter below can
miss worker counts because it flushes on thread exit. These historical allocation
totals remain provisional and must not support exact old/new comparisons.
The timing and output results above remain valid. See
[the corrected allocation measurements](epoch-300-indexed-usdc-and-allocation-retest-2026-09-06.md).

Separate allocation measurements over 3,274,503 transactions report 11,836
old versus 11,815 one-pass allocation calls at one worker, and 30,091 versus
30,080 at 12 workers. These uncorrected totals do not establish an allocation regression.
These counts include scan setup and output-buffer growth, but not C zstd
allocations. Requested bytes include the complete registry load; they are not
live memory or peak RSS. Allocation-instrumented timings are excluded above.

Local raw evidence is under
`target/nas-validation/epoch300-usdc-single-pass-20260906T154800Z/` in
`results-metadata/`, `profile-results/`, and `profile-allocations/`. Subsequent
allocation changes require a separate build and measurement. These numbers
identify only the source patch and binary hashes recorded above.
