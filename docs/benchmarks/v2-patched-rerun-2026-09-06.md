# Patched V2 SSD rerun — 6 September 2026

## Scope

Run the four separate V2 examples once on each sample epoch: 0, 100, 200, 300, 400, 500, 600, 700, 800, 900 and 1000. Use local SSD input and SSD output, twelve workers, and one example process at a time. No CAR/V3/network run, archive conversion, upload, or archive hash pass is part of this rerun.

NAS package: `/volume2/blockzilla-bench/control/v2-patched-20260906/`.
Results: `/volume2/blockzilla-bench/results/all-v2-ssd-patched-20260906/`.
Progress log: `/volume2/blockzilla-bench/control/v2-patched-20260906/run.log`.

## Changes

- The V2 USDC example permits one shared registry up to 3 GiB, instead of falling back to tiny per-worker caches above 1 GiB. The SDK's general default is unchanged. All eleven sample registries must fit before launch.
- USDC and Pump.fun exclude known failed transactions. The V2 SDK can use typed row status flags to omit their instruction/balance projection and selected signatures. Transaction headers and scan totals remain present. Unknown status is not silently skipped and remains a coverage issue.
- USDC status selection retains its lightweight message-count/token-balance projection path for non-failed transactions; it does not enable full instruction projection.
- The output schemas and magic values are versioned because the filtering changes their meaning: `BZUSDC02` and `BZPUMP02`. These are example output changes, not archive-format changes. Both reports include `skipped_failed_transactions`.
- FireWatch already excludes failed transactions from its reached-program list; its behavior remains unchanged. The count example still counts all transactions and recorded inner instructions.
- The CAR borrowed-array decoder rejects indefinite-length arrays and wrong CBOR types before iteration. This closes the reproduced `next` bypass. No CID lookup table or per-transaction allocation was added.

The shared workload layer also makes the failure-filter semantics available when CAR/V3 examples are next rebuilt. The new early row-flag projection optimization is implemented for V2; other adapters may still project details before the workload rejects a failure.

## Interpretation

This remains the recorded-balance USDC workload, not the instruction-derived account tracker. Do not present its speed as a benchmark of account discovery.

USDC/Pump.fun output and timing now reflect a changed filter. Compare elapsed time, transaction scan rate and read volume, but do not claim a pure reader speedup from that comparison alone. Count and FireWatch retain their old workload definitions. Old output files stay intact.

The runner stops on an execution error. It records `output_complete` separately; old source metadata can still make epoch 0 extraction incomplete. A V2-only parity value of PENDING means no other format was compared. Runner PASS is execution success, not proof of complete historical metadata.

Acceptance for this launch: focused reader/workload regressions pass, four Linux binaries build, all 44 input jobs pass the file-presence preflight, and the new runner starts on NAS. Full-run completion and final performance results are recorded later.

## Build and checks

The four Linux x86-64 musl release binaries built successfully with `+aes,+sse2`, matching the previous build flags. Source base: `b5499e696e79d9ca8c9c55e2c6960361d90e4f62`; the uncommitted tracked changes are saved as `source-changes.patch` in the NAS package. Executable hashes and the runner hash are saved in the run's `run.json`. The old binaries are unchanged.

Passed: 32 Compact V2 query tests, 19 shared workload tests, three CAR node tests (including rejection of unsupported next arrays), and 11 matrix-runner tests. All 44 jobs passed file/executable preflight. No archive bytes were scanned by that preflight. The largest sample registry is epoch 700 at 2,171,852,096 bytes, below the 3 GiB cap.

## Launch confirmed

Runner PID 1110902 started successfully. At the first check, six of 44 jobs had completed without execution errors, and epoch 100 Pump.fun was running. All four epoch 0 jobs completed; its count totals and all slot-hour buckets match the previous V2 result exactly. Epoch 100 count and USDC also completed.

Epoch 0 USDC and Pump.fun still explicitly report unknown/incomplete metadata coverage and zero skipped failures. These are not reader errors, and they were not silently classified as successful transactions. Do not interpret the runner's PASS label as complete source metadata.
