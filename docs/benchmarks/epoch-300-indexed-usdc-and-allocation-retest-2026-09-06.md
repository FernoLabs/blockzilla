# Epoch 300 V2: indexed output and allocation retest

This is the pre-pipeline benchmark. The later
[rolling-pipeline report](epoch-300-rolling-pipeline-2026-09-06.md) records the
current scheduling implementation and its paired performance comparison.
The output and corrected allocation evidence below remains valid for these builds.

Status: complete. All 18 full-output checks pass. Normal USDC output averages
88.636 seconds; indexed output averages 90.808 seconds. Indexed data and its
dictionary use 47.97% less space. The USDC speed gap to the historical old reader
remains. At the end of this run, the NAS had no active benchmark process and
was released for the user's copy.

## Full scans

All runs use the same local epoch 300 archive on `Blockzilla-00`, 12 workers,
and local output files. The order is canonical / indexed / indexed / canonical,
then one Pump.fun confirmation. Runs are serial. No archive is changed and no
cache is flushed. Output comparison starts only after all timed scans finish.

| Run | Scan (s) | Reported total (s) | Process wall (s) |
| --- | ---: | ---: | ---: |
| Canonical USDC 1 | 86.165 | 86.200 | 86.209 |
| Indexed USDC 1 | 90.256 | 90.291 | 90.314 |
| Indexed USDC 2 | 91.278 | 91.324 | 91.382 |
| Canonical USDC 2 | 91.051 | 91.071 | 91.083 |
| Pump.fun | 133.339 | 133.363 | 133.379 |

The canonical mean is 88.635730 seconds. The indexed mean is 90.807932 seconds,
2.45% more. Repeat spread, measured as maximum/minimum minus one, is 5.65% for
canonical and 1.14% for indexed. One Pump.fun run cannot establish repeat spread.

| Historical reference | Reference total (s) | Current total change |
| --- | ---: | ---: |
| USDC old reader, earlier fresh pair | 76.192 | +16.33% |
| USDC refactor before one-pass fix | 109.547 | −19.09% |
| USDC one-pass fix | 89.014 | −0.42% |
| Pump.fun old reader | 125.643 | +6.14% |
| Pump.fun refactor before allocation fix | 143.871 | −7.30% |

These references were measured earlier, not interleaved with this run. They
show that USDC has not recovered the old speed. The additional allocation work
does not show a material full-USDC time reduction beyond the one-pass fix.
The earlier 10% USDC threshold is still not met against its saved baseline.
This result does not clear that performance concern for merge.

Indexed output includes stream hashing, two output files, and synchronization.
Canonical output does not have the same hash/synchronization cost. These are
application timings, not isolated measurements of registry lookup cost.
Expansion is excluded. V3 was not rerun in this focused V2 experiment.

## Output verification

Both canonical files match each other and the saved old output byte for byte.
Both indexed files expand through the checked CLI to those exact bytes. Each
expander exits successfully after validating completion, source scope, file
lengths, row counts, hashes, and coverage. Indexed data, dictionary, and source
metadata also match across repeats. All 18 verifier checks pass.

Every full USDC scan reports:

- 408,989 blocks and 724,730,034 transactions.
- 111,179,816 omitted known failed transactions.
- 6,253,225 matching transactions and 30,224,439 balance rows.
- 15,091,684 pre rows and 15,132,755 post rows.
- Complete coverage, zero indeterminate transactions, and zero unavailable
  token-balance or mint reports.

Canonical output is 4,110,523,748 bytes. Its SHA-256, also produced by both
expanded outputs, is
`c307d97f8999ad4cc7a6ad753007c4d6cf4e5a874cfba5da81bd43700ef1d1a2`.

Indexed data is 2,115,710,806 bytes. Its dictionary contains 384,291 first-observed
source references and uses 23,057,536 bytes. Together they use 2,138,768,342 bytes,
47.968% less than canonical, excluding the small JSON sidecars. Dictionary
entries include the token account, mint, owner, and program references required
by the output; this is not a count of newly created token accounts. No SQLite
database is created.

The two checked expansions take 9.37 and 9.72 seconds, including streaming byte
comparison and hashing by the verifier. Those are verification costs, not pure
expander benchmarks. The verifier does not create another multi-GB output file.

Pump.fun output matches the old reference. Epoch 300 has no matching Pump.fun
events, so its 44-byte output tests scan/filter behavior, not event-write speed.

## Workers and host conditions

Every full scan reports 12 effective workers and 12 active workers at peak,
with groups of at most 48 blocks. Sampled process CPU use averages about nine
cores. Peak concurrency does not mean continuous use of all twelve cores.

The USDC scans read the same 44,118,946,554 logical bytes in 11,268 calls and
share one 432,762,560-byte registry. Sampled peak resident memory is about
589–590 MiB for canonical, 597–598 MiB for indexed, and 366 MiB for Pump.fun.
Two-second samples can miss peaks and the final part of process CPU use.

All 258 host samples show compressor PID 1784546 stopped at 913.87 CPU seconds
and parent PID 1629771 stopped at 2.03 CPU seconds. No signals were sent to them.
There is some CPU and memory pressure; more than 2.69 GiB remains available.
Host pressure is system-wide and can include the reader itself. This is not a
claim that the host has no background activity. The older observer in the first
16 profile cases checks an exited compressor PID; the full-run observer uses
the current PIDs above.

## Bounded timings

Each timing case covers the first 2,048 blocks: 3,274,503 transactions, one
warmup, and three measured iterations. Output is serialized to a discard sink.
These runs have allocation counting disabled. Counters, coverage, source bytes,
and calls match across versions and worker counts.

| Workload / workers | One-pass median (s) | Current median (s) |
| --- | ---: | ---: |
| Canonical USDC / 1 | 1.828280 | 1.800052 |
| Canonical USDC / 12 | 0.546417 | 0.543420 |
| Pump.fun / 1 | 2.112349 | 2.106681 |
| Pump.fun / 12 | 0.680281 | 0.598717 |

Current indexed USDC medians are 1.833762 seconds at one worker and 0.550966
seconds at twelve. They do not show a speed gain over current canonical output.
All four indexed profile reports match canonical USDC counters and coverage:
110,807 balances and 6,279 dictionary rows for this bounded sample.

## Corrected allocation measurements

Review found a measurement defect: dropping a Rayon pool signals shutdown but
does not join all worker OS threads. The old profiler flushes thread-local
counts on thread exit. Its snapshot can miss a late flush or include that flush
in the next measurement. Earlier allocation totals, including old baselines
and the first pass of this experiment, are provisional. The earlier progress
claims of 89%/36% reductions against the one-pass binary are withdrawn.

The profiler now records each allocation immediately in atomic counters. This
changes the diagnostic executable only. It does not change the timed reader
executables. Three targeted tests pass, including live-worker visibility and
isolation between measurement windows. A separate Linux profiler build was
verified on the NAS. The six cases below use one warmup and two measured
allocation iterations. Every workload counter and coverage report matches the earlier
timing run; all allocation bucket sums pass.

| Workload / workers | Allocation calls, two runs | Median requested bytes |
| --- | --- | ---: |
| Canonical USDC / 1 | 575, 577 | 490,447,913 |
| Canonical USDC / 12 | 3,150, 3,314 | 666,545,676 |
| Indexed USDC / 1 | 341, 341 | 490,849,241 |
| Indexed USDC / 12 | 1,563, 1,648 | 671,399,901 |
| Pump.fun / 1 | 381,943, 368,005 | 245,577,434 |
| Pump.fun / 12 | 439,282, 439,977 | 472,263,221 |

Within this corrected measurement, indexed USDC has 40.8% fewer allocation
calls at one worker and 50.3% fewer at twelve. Requested bytes increase by
0.08% and 0.73%; fewer calls do not imply less retained memory or faster scans.
Buffer growth and scheduling can change allocation totals between scans.
These counts include setup and the shared registry. They are not live memory,
exclude native C zstd allocations, and are not zero. Timings with the atomic
counter enabled include counter contention and are excluded from speed claims.
No corrected old-reader allocation binary was built, so a verified allocation
reduction against that baseline is not claimed.

## Reproduction and evidence

The timed source is revision `2672d42714da7560672b7a99b420c8315eb733e2` plus patch
`3cc0437742a26a1c62c1ad1261089eaa258866e1bab47865b359bf341b954d19`.
It includes the user's existing model error-message edit without changing it.
All 1,034 source-file hashes match the saved build at launch. Workspace tests
pass: 3,549 passed, one ignored, no failures. Linux x86-64 musl release builds
use Rust 1.98.1 and `-C target-feature=+aes,+sse2`.

The corrected profiler is the same source with only `allocation.rs` changed;
its full patch is
`5bab3536fffe4896ed3fd6db942d6068fe094bc0868476cdc2426c991d9eafff`
and binary SHA-256 is
`460edcaad3bfe9d7ad1b17d2d80ae1c46c44db7c8fd512620783d5cc314298e9`.
The targeted profiler tests pass after that change. No full reader scan needs
to be repeated for this profiler-only correction.

NAS control/result directories are below `/volume2/blockzilla-bench/` with tags:

- `epoch300-allocation-review-20260906T161500Z` for timed scans and output checks.
- `epoch300-allocation-counter-check-20260906T172000Z` for corrected allocation checks.

Full output files remain on the NAS. Small evidence files are saved locally in
`target/nas-validation/epoch300-final-results-20260906/`. Build packages, source
patches, hashes, exact commands, test logs, observers, and the output verifier
are retained under `target/nas-validation/`. The summary is also saved in
`epoch-300-indexed-usdc-and-allocation-retest-2026-09-06.json` beside this report.
The full 88-job matrix and its automation remain stopped. Nothing was merged
or pushed.
