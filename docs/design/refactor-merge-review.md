# Refactor merge review

Review date: 2026-09-06. Target: `main`. Integration branch: `refactor`.

## Included branches

- Remote `main` at `ffffcbe1` was already an ancestor of the refactor.
- Local `main` at `545dfa0b` has the same example cleanup as the refactor's
  `b109fa40`. The merge records both histories.
- `codex/sample-archive-benchmark` at `c086be87` adds the reader and example
  fixes in `1d949be4`, plus their recorded audits and benchmark results.
- The refactor retains the canonical/historical decoder separation, descriptor
  integrity checks, ordered-worker fix, and canonical V3 reader extraction.

The sample branch's untracked NAS compression script is outside this merge.
The captured benchmark files describe their original runs. They are not test
results for the integrated refactor.

## Integration corrections

Known failed transactions can omit unrequested details while retaining their
headers and transaction counts. Unknown status stays explicit. Count-only
requests still include every transaction and recorded instruction.

The review corrected four gaps in the incoming fixes:

1. Compact V2 checks decoded metadata and row flags before it reports success
   or omits a known failure. The error flag alone cannot suppress a record.
2. Disabling execution status also disables failure-detail omission. Direct
   or deserialized requests with contradictory options fail validation.
3. CAR checks the source failure index against the outer instruction count
   before it emits a reduced-detail header.
4. A CAR-only count benchmark reports only scheduled workloads. Empty filter
   combinations fail before inventory or reader work starts.

CAR, V2, and V3 examples now report their skipped-failure counts consistently.
The V3 status-only path validates the complete outcome plane. CAR rejects
unsupported DataFrame array shapes before borrowed iteration. The V2 USDC
example permits one shared registry of up to 3 GiB; the reader default is
unchanged.

## Compatibility and limits

USDC and Pump.fun examples exclude known failures. Their output identifiers
are now `BZUSDC02` and `BZPUMP02`; compare them with outputs from the same
version. Unknown execution status still produces explicit incomplete coverage.
The count workload retains its prior meaning.

The folder and package renames are intentional and recorded in
[workspace-layout.json](workspace-layout.json). Storage object names and routes
are unchanged. SPYX market indexes can require a rebuild after their code
fingerprint changes.

Canonical V3 HTTP/common-model support and the remaining legacy-consumer
migration are future work, as listed in the
[workspace plan](workspace-restructure.md). The parked archive-token-events
example already had an invalid SDK dependency on remote main; its unsupported
status is now explicit. These are separate from integration correctness.

Local checks do not establish full-epoch workload parity or production
performance. Linux CI and Worker release builds must run in their target
environments before release.

## Validation and merge decision

The focused reader/model/workload run passed 553 tests. The benchmark runner
passed 12 tests, including the combined CAR/count filter case. Formatting,
script syntax, local documentation links, and the Archive V2 wire-boundary
check pass. The package mapping still includes 46 packages, 165 targets, and
105 binaries.

The full integrated workspace test run passed **3,715 tests**, with no
failures and one ignored manual release-mode benchmark, across 140 harnesses.
Normal binary builds, the all-target workspace check, optional contributor
and repair-tool builds, and the Old Faithful WebAssembly build all pass.
All 27 imported benchmark data/profile files remain byte-identical to the
sample branch, including the TSV empty fields.

**Decision: ready to merge after Linux CI passes.** No unresolved code conflict
or known integration regression remains from this review. The merge is saved
on `refactor`; `main` has not been changed. The user's pre-existing model
error-message edit remains uncommitted and unchanged.
