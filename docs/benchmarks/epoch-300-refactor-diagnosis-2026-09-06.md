# Epoch 300: refactor performance diagnosis

This report measures the refactor before the USDC correction. The subsequent
[reader fix and retest](epoch-300-usdc-single-pass-fix-2026-09-06.md) are recorded
separately.

Later correction: the allocation totals in this historical report use a
thread-exit counter that can miss worker counts. Those totals and percentage
comparisons remain provisional. Timing and output checks remain valid. See
[the corrected allocation retest](epoch-300-indexed-usdc-and-allocation-retest-2026-09-06.md).

Status: complete. V2 has a measured projection slowdown; the V3 epoch-300
pair is within the 10% investigation threshold. All checked outputs match.
The full 88-job matrix is stopped and its automatic follow-up is paused.
All prior outputs remain available. Reader source was not changed during
this diagnosis.

## Experiment

NAS: `ach@192.168.1.46:22`, host `Blockzilla-00`. Local SSD archive inputs and
outputs, 12 workers, one reader process at a time. Epoch 300 has 408,989 blocks
and 724,730,034 transactions; these two workloads omit details for 111,179,816
known failed transactions while keeping scan totals and coverage explicit.

The saved baseline V2 binaries are compared with the refactor binaries in
old/new/new/old order for each of USDC and Pump.fun. No cache flush, archive
rewrite, or archive hash pass is performed. The compressor was already stopped
when this task checked its process state. Its state and CPU counter are
recorded throughout the focused sequence; this task sends it no signals.

- Control: `/volume2/blockzilla-bench/control/epoch300-focus-20260906T150500Z/`.
- Results: `/volume2/blockzilla-bench/results/epoch300-focus-20260906T150500Z/`.
- Baseline binaries: `/volume2/blockzilla-bench/control/v2-patched-20260906/bin/`.
- Refactor binaries: `/volume2/blockzilla-bench/control/refactor-v2-v3-2672d427-20260906T140000Z/bin/`.

Baseline hashes match the earlier run. Baseline source is `b5499e69` plus the
saved patch with SHA-256
`d3f10d2a7cc49fcbbeb34c0af8b8871a04cdca5f7051d10e1fac74a6ab08cb90`.
Refactor source is `2672d427` plus the pre-existing model error-message edit.
The source patch and binary hashes remain beside the builds.

Exact output comparisons ran after the timed sequence. All eight runs match
the baseline byte for byte, including schemas, totals and coverage. Recorded
input metadata, host and worker settings also match.

## Measured V2 result

| Workload | Old runs (s) | New runs (s) | Old mean (s) | New mean (s) | Extra elapsed time |
|---|---|---|---:|---:|---:|
| USDC | 73.872, 78.877 | 108.649, 110.445 | 76.374 | 109.547 | +43.4% |
| Pump.fun | 125.765, 125.521 | 143.938, 143.804 | 125.643 | 143.871 | +14.5% |

These are means of two full-epoch runs per version. They establish a repeated
slowdown on this input, not a statistical confidence interval or a result for
all sample epochs. The output checks establish unchanged output for these
samples, not equivalence of the old reader's validation guarantees.

## Source findings

The reader-fix integration in `2703ed38` added status and row-flag consistency
checks. The old V2 reader could trust `HAS_ERROR` and omit a transaction before
decoding its message or metadata. The current reader validates the stored
record first. This prevents a wrong flag from suppressing a successful
transaction or admitting an invalid failed transaction.

USDC now traverses successful metadata twice: the status-only branch calls
`CompactV2MetadataProjector::count`, and token-balance projection then calls
`project_token_balances_reusing`. Both traverse the complete metadata grammar.
The token-balance visitor already parses status and CPI presence, but discards
those values instead of returning them to the caller.

Pump.fun projects messages and metadata for failed transactions before it
omits their details. This can allocate message keys, instruction lists, CPI
groups, and loaded-address vectors that the workload will not consume.

Source locations:

- `crates/compact-v2/blockzilla-compact-v2-reader/src/compact_query.rs`, status-only metadata branch near line 1014, full projection near line 1051, failed-detail return near line 1134, token-balance projection near line 1298.
- `crates/compact-v2/blockzilla-compact-v2-reader/src/metadata_projection.rs`, token-balance visitor near line 306.

## Safe optimization direction

1. USDC: return status and CPI presence from the existing token-balance
   traversal, validate flags from those values, and avoid the separate count
   traversal. For omitted failures, consume and validate token fields without
   retaining their balances.
2. Pump.fun: use a flagged failure only as a hint to select message/metadata
   count projection. Validate the entire record and all relevant flags before
   returning the reduced transaction header. A contradictory flag must cause
   an error.
3. Preserve unknown status, failure instruction indexes, historical source
   decoding, bounds and registry checks, trailing-byte rejection, all scan
   totals, and output order.

Restoring the old unchecked early return is not a valid fix. Existing tests
cover both flag disagreements, damaged failed metadata, preserved failure
indexes, and unknown-status coverage. Required validation can still have a
cost after redundant work is removed.

## Bounded diagnostics

Separate diagnostic binaries use the unchanged profile harness from the exact
baseline and current source snapshots. Both were built with Rust 1.98.1 for
Linux x86-64 musl, release mode, `-C target-feature=+aes,+sse2`.

The profile read the first 2,048 blocks of epoch 300 with one and
twelve workers. It discards output-file writes, uses one warmup and three
timed iterations, and measures Rust allocations in a separate run. USDC uses
a 3,072 MiB registry cap; Pump.fun uses 1,024 MiB, matching their examples.
The profile checks counters and coverage between iterations, not output
content. Allocation totals do not measure live memory or C zstd allocations.

The bounded range contains 3,274,503 transactions, including 554,678 known
failures (16.94%). Pump.fun has no matches in this range or in the full
epoch-300 example; its output is the 44-byte header. Its timing measures a
scan with no matching events, not sustained event-output throughput.

All 16 timing/allocation datasets pass checks for matching transaction,
instruction and source-byte totals, schemas, counters and coverage. The
diagnostic binaries use the same compiler, so these comparisons also remove
a compiler-version difference as an explanation for the measured source gap.

| Workload | Workers | Old elapsed (s) | New elapsed (s) | Old projection sum (s) | New projection sum (s) |
|---|---:|---:|---:|---:|---:|
| USDC | 1 | 1.608746 | 2.344047 | 0.811040 | 1.526165 |
| USDC | 12 | 0.519652 | 0.655055 | 1.916867 | 3.193317 |
| Pump.fun | 1 | 1.834124 | 2.220863 | 1.218458 | 1.596201 |
| Pump.fun | 12 | 0.591157 | 0.691901 | 4.481581 | 5.712636 |

For one worker, the added projection time is approximately 97% of the added
elapsed time in both workloads. Read time is about 31–32 ms and changes
little. Decompression time changes by about 16–18 ms. Stage measurements
overlap and their medians are independent; this percentage is a diagnostic
comparison, not an exact additive accounting identity.

The separate one-worker allocation run reports 11,825 versus 11,849 Rust
allocation calls for USDC, but 361,591 versus 613,441 for Pump.fun (+69.7%).
Pump.fun requested allocation bytes rise from 239.3 to 323.5 MB (+35.2%).
This supports repeated parsing as the USDC cost and additional retained
projection objects as part of the Pump.fun cost. These counters alone do
not assign every allocation to a specific call site.

Raw local diagnostics and the generated JSON/Markdown summary are under
`target/nas-validation/epoch300-focus-20260906T150500Z/`. On the NAS they are
under the focused results directory's `diagnostics/timing` and
`diagnostics/allocations` subdirectories.

## Larger slowdown in the interrupted run

The saved epoch-600 Pump.fun records show 153.10 seconds before and 399.99
seconds after. Summed worker projection time rose from 1,253.29 to 4,179.52
seconds. Producer read time stayed at 37.55 versus 37.91 seconds; decompression
worker time fell from 262.67 to 248.16 seconds. Sampled peak resident memory
also did not rise (393.5 versus 376.0 MB). These records point to projection
cost, not a larger archive read or larger resident-memory footprint. They do
not replace a controlled repeat of epoch 600, which is outside this focused
epoch-300 experiment.

## V3 epoch-300 check

The old and new V3 examples each ran once on epoch 300 after the bounded V2
diagnostics ended. They used the same local SSD inputs, 12 workers, and the
same wallet. This checks the frozen standalone V3 prototype. It does not
measure the canonical V3 catalog/ledger converter output.

| Workload | Old (s) | New (s) | Change in elapsed time |
|---|---:|---:|---:|
| Slot-hour count | 55.966 | 59.757 | +6.8% |
| USDC | 74.871 | 75.835 | +1.3% |
| Pump.fun | 0.199 | 0.188 | -5.6% |
| Firewatch | 0.148 | 0.153 | +3.5% |

All four comparisons pass exact output/count checks and recorded input,
host, worker and wallet checks. The two short indexed queries are dominated
by startup at this scale; their percentages are not reliable speedup claims.
This is one old/new pair, not a repeated measurement across all epochs.

Both the full V2 pairs and subsequent diagnostics/V3 phase recorded the
compressor in `T (stopped)` state. Its cumulative CPU time stayed at 1,154.54
seconds throughout. Thus compression did not run during these measurements.
Other system services and OS caches were not controlled.

## Conclusion

The data and source review agree: extra V2 projection work explains the
measured slowdown. USDC has redundant metadata traversal. Pump.fun builds
objects for failed transactions that it then discards. The bounded allocation
and stage measurements support these mechanisms; they do not isolate an exact
per-function cost or prove how much a future optimization will recover.

Keep the status/integrity checks and remove repeated parsing and unnecessary
retained objects. Re-run the same paired checks after that change. Full-matrix
performance approval remains open; this focused diagnosis is complete.

Machine-readable evidence: `epoch-300-refactor-2026-09-06-summary.json` beside
this report. Full small logs are saved locally under
`target/nas-validation/epoch300-focus-20260906T150500Z/results-metadata/`.
All large outputs remain on the NAS.
