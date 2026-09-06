# Refactor V2/V3 NAS performance check

Status: the full matrix was stopped at the user's request after 29 completed
jobs. Focused epoch-300 tests are complete: V2 USDC is 43.4% slower and
Pump.fun is 14.5% slower in paired runs; all checked outputs match. V3 is
within the 10% investigation threshold on epoch 300. See
[the focused diagnosis](epoch-300-refactor-diagnosis-2026-09-06.md).

## Build and run settings

Source revision: `2672d42714da7560672b7a99b420c8315eb733e2`. The pre-existing
model error-message edit is included in the build and saved as a source patch
beside the binaries. Compiler: Rust 1.98.1. Target: Linux x86-64 musl, release
profile, `-C target-feature=+aes,+sse2`, matching the prior target and flags.
All eight binaries built successfully and their transfer hashes match.

The run selects the four dedicated examples (slot-hour count, USDC recorded
balances, Pump.fun, and Firewatch) for epochs 0, 100, ..., 1000. It uses local
SSD inputs and outputs, 12 reader workers, the same Firewatch wallet, and one
example process at a time. It reads the frozen standalone V3 prototype; it
does not benchmark the canonical V3 converter output.

All 383 input objects match the baseline file names, sizes, and modification
times. This is a metadata comparison, not an archive content hash. No archive
payload was read during preflight. The 12 runner tests passed on the NAS.

## Baselines

- V2: `/volume2/blockzilla-bench/results/all-v2-ssd-patched-20260906/`.
- V3: `/volume2/blockzilla-bench/results/all-v3-car-local-statusfix-20260906/`.

Each baseline has all 44 required format jobs passing. The V3 directory later
stopped in its CAR phase; that unrelated CAR failure is outside this comparison.
The baseline reader times sum to approximately 2 hours 47 minutes. Output
comparison adds time outside the reader measurements. OS caches are uncontrolled.

The output versions are `BZUSDC02` and `BZPUMP02`. Count and workload outputs
must match their same-version baseline, including unknown-status coverage.
Epoch 0 incomplete source metadata remains explicit and is not a reader error.

## NAS locations

- Build control: `/volume2/blockzilla-bench/control/refactor-v2-v3-2672d427-20260906T140000Z/`.
- First results: `/volume2/blockzilla-bench/results/refactor-v2-v3-2672d427-20260906T140000Z/`.
- Restart control: `/volume2/blockzilla-bench/control/refactor-v2-v3-2672d427-20260906T141000Z/`.
- Restart results: `/volume2/blockzilla-bench/results/refactor-v2-v3-2672d427-20260906T141000Z/`.

The first run was interrupted after 11 completed jobs. The user confirmed a
large copy had been active and requested a restart. All first-run outputs and
partial output remain in place. The restart uses the same binaries.

A separate `zstd -3 -T12` process was still compressing the retained epoch-800
CAR into the SSD archive-zstd directory when the restart began. Its load can
affect the timings. It was not stopped by this benchmark task.

At an earlier check, 17 of 88 jobs had completed without a reader error. This
does not establish output parity or performance parity. For example, epoch-300
V2 Firewatch took 117.57 seconds versus 117.63 seconds in the baseline, while
V2 USDC took 112.14 seconds versus 76.45 seconds. The concurrent compression
was still active; these are preliminary, uncontrolled measurements.

The task follow-up `verify-nas-v2-and-v3-performance` is now paused because the
user changed the scope to epoch 300. The full runner state is `INTERRUPTED`.

## Focused epoch-300 diagnosis

Control: `/volume2/blockzilla-bench/control/epoch300-focus-20260906T150500Z/`.
Results: `/volume2/blockzilla-bench/results/epoch300-focus-20260906T150500Z/`.

The focused sequence uses the exact saved V2 baseline binaries and the new
binaries, one reader at a time, with 12 workers. It runs old/new/new/old for
USDC, then old/new/new/old for Pump.fun. Output comparisons run after the
timed sequence. No source semantics have been changed for this experiment.

The old binary hashes match their baseline run record. Its saved source patch
has SHA-256 `d3f10d2a7cc49fcbbeb34c0af8b8871a04cdca5f7051d10e1fac74a6ab08cb90`
and applies to source base `b5499e696e79d9ca8c9c55e2c6960361d90e4f62`.

At focused-test preparation, the compressor's actual process state was
`T (stopped)`. This task did not pause it and will not resume it without
authorization. The earlier full-run `compression_active` field only checked
process existence; it does not prove that compression was consuming CPU.
The focused run records process state and cumulative CPU time instead.

Source review identifies two candidate costs: USDC traverses successful
metadata once for full status/flag validation and again for token balances;
Pump.fun projects failed-transaction messages and metadata before omitting
them. The old reader trusted row flags and skipped these transactions before
validation. These checks protect correctness and must not simply be removed.

## Original full-matrix comparison

This comparison was not run because the user stopped the matrix. It must not
be reported as complete. The focused epoch-300 comparison is recorded in the
separate diagnosis linked above.

The saved-run comparison tool checks exact count buckets and application
output bytes, as well as totals, schemas, completeness, unknown-status coverage,
recorded input metadata, host, and worker settings. It records elapsed ratios
and sampled resource metrics. A slowdown above 10% is an investigation flag,
not statistical proof of a regression. It cannot pass an incomplete run.

```sh
python3 compare_archive_sample_runs.py \
  --baseline-v2 /volume2/blockzilla-bench/results/all-v2-ssd-patched-20260906 \
  --baseline-v3 /volume2/blockzilla-bench/results/all-v3-car-local-statusfix-20260906 \
  --current /volume2/blockzilla-bench/results/refactor-v2-v3-2672d427-20260906T141000Z \
  --output-dir /volume2/blockzilla-bench/results/refactor-v2-v3-2672d427-20260906T141000Z-comparison
```

Run this after the reader sequence completes so comparison I/O does not
compete with measured scans. The tool writes JSON, TSV, and a short report;
all baseline inputs and outputs remain unchanged. Its 18 tests pass locally.
