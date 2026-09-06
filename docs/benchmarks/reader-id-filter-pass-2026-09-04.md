# Reader ID filter pass — 4 September 2026

This is a focused SDK change. It is not a claim that all allocation or throughput
work is complete. No archive data, scheduler, or compactor was changed.

## Changes

The 12 binaries remain separate: count, USDC, Pump.fun, and FireWatch, each for
CAR, Compact V2, and Indexer V3. Local and network inputs use the same projection
code. Jetstreamer is not part of this build.

- V2 and V3 bind filter keys to epoch-local registry IDs before the scan. Workers
  share the bound IDs. V3 candidate lookup and transaction filtering reuse the
  same binding. A raw-key fallback record compares its stored key directly.
- Count requests omit program keys, account lists, signatures, and status.
  They retain all instruction coordinates and coverage. No public-key registry
  data is read by a V2 or V3 count scan.
- Pump.fun compares program IDs before materializing a selected program key.
  Non-matching instructions retain their coordinates but have no program key.
- USDC compares mint IDs before resolving output fields. Selected balance rows
  still need their owner and token-program keys to preserve the output schema.
  The shared dense registry policy remains available for this output work.
- FireWatch compares signer IDs first. It materializes program keys only for
  successful transactions signed by the target wallet. The fixed output still
  contains real wallet and program public keys.
- CAR borrows static and loaded key lanes instead of copying and joining them
  for each transaction. CAR already stores full keys; it has no compact registry
  to resolve. Optional base58 metadata keys decode into a fixed buffer.
- Common small V2/V3 messages keep up to eight key references and four instruction
  views inline. Large messages still work. This removes two heap allocations for
  common small messages; instruction payloads remain borrowed when not requested.
- Filtered V2 balance output no longer reserves space for all non-matching rows.

`registry.mphf` is used when present. If this optional index is absent, binding
uses one bounded pass over `registry.bin`, before workers start. If the complete
registry is already in memory, binding reuses it. This fallback adds setup I/O;
it is not repeated per transaction. A damaged present index is an error, not an
excuse to silently change the filter.

## API change

`ResolvedInstruction.program_id` is now `Option<[u8; 32]>`. `None` means that the
request did not select the key. A zero key is never used as a missing-value marker,
because it is a valid System Program key. Existing full projections return `Some`.
Token tracking and durable token replay reject missing required program identities.
Existing serialized token replay digests are unchanged for full projections.

Examples use `without_instruction_programs`, `with_instruction_programs_for`, or
`with_required_signer`. These choices are in the SDK, not new CLI options.

## Checks

The focused suites passed: query SDK (73), workload output (17), V2 projection
(29), V3 projection (46), and CAR projection (16). These include malformed input,
coverage, loaded addresses, CPI, output order, and signature reconstruction.

New checks make V2 registry reads fail and confirm that count scans still pass
with one and twelve workers. They also check that a parallel program filter binds
once. V3 checks confirm zero key expansion for count and program filters, including
an absent key, and reuse of the binding after candidate lookup.

Release package:
`/private/tmp/sample-reader-package-20260904-id-filter-final`.

NAS destination:
`/volume1/blockzilla/benchmark-results/sample-reader-package-20260904-id-filter-final`.

The old benchmark package and its results are not overwritten. The epoch 0 check
passed all eight V2/V3 jobs and the CAR count job. Counts and all completed V3
workload outputs matched V2. The remaining CAR smoke jobs were stopped at the
user's request to start the full run at once.

The old full runner (1064224) and short check runner (1276478) were stopped with
SIGTERM. Their logs and completed results remain intact. The new full runner is
1281842. It uses 12 V2/V3 workers, all 11 epochs, all four workloads, disk and public
network inputs: 264 jobs, in V2 then V3 then CAR order. CAR retains its existing
single-thread query adapter. New logs and results are under the NAS destination's
`runner.log` and `results/`. No archive payloads were changed or uploaded.

Epoch 0 is a correctness check, not a representative throughput measurement.

## Remaining work

The common query projection still builds transaction and instruction vectors.
Counting is not yet a fully borrowed summary callback. V2 also retains its
per-worker-wave merge barrier. Those are separate performance changes.

Do not use the old benchmark timings as measurements of this new build. Record a
fresh run before stating a speed gain. Epoch 0 alone does not test modern USDC or
Pump.fun activity; the SDK fixtures cover positive target matches and CPI.
