# User-program-index naming audit

Checked on 7 September 2026. The retained indexer package is
`blockzilla-user-program-index`. The shared reader workload is
`user-program-index`. The earlier package rename did not cover the examples,
monitor, operational tools, and report labels. Those current names are now
updated.

## Current names

| Area | Current name |
| --- | --- |
| Indexer package and standalone command | `blockzilla-user-program-index` |
| Dump command | `blockzilla-dump user-program-index` |
| CAR example | `read-car-user-program-index` |
| Compact V2 example | `read-compact-v2-user-program-index` |
| Archive V3 example | `read-archive-v3-user-program-index` |
| Shared Rust workload | `UserProgramIndexSink`, `UserProgramIndexReport`, `user_program_index_scan_request` |
| Benchmark and profile workload | `user-program-index` |
| Wire-profile tools | `user-program-index-wire-profile-audit`, `user-program-index-wire-profile-audit-batch`, `user-program-index-wire-profile-marker-transition` |
| Controller source, still parked | `indexer/blockzilla-user-program-index/src/bin/user-program-index-controller.rs` |
| Controller configuration | `BLOCKZILLA_USER_PROGRAM_INDEX_*` |
| Monitor option | `--user-program-index-status-file` |
| Monitor environment | `BLOCKZILLA_MONITOR_USER_PROGRAM_INDEX_STATUS_FILE` |
| Operations scripts | `scripts/*user-program-index*.sh` |
| Report and chart labels | User-program index |

All 44 workspace packages and 162 Cargo targets were checked. No active
package or target uses the retired FireWatch or Firebase indexer name. The
workspace layout records the old names only as migration inputs.

Use the new binary names when building a new package. Old Rust workload names
and old example binary targets are removed. The matrix runner accepts
`firewatch` as a legacy selection alias and records new jobs with the current
name. Selecting both names is an error. The comparison tool reads old or new
saved result paths, preserves wallet checks, and rejects ambiguous duplicate
results. An old run must resume with its own frozen runner and configuration.

## Names retained for compatibility

These old strings are intentional. Removing them requires a separate, explicit
format or state migration.

- The wallet/program output keeps binary magic `BZFWAL01` and schema
  `blockzilla-example-firewatch-wallet-program/v1`. Output bytes, wallet
  selection, success filtering, and query semantics remain unchanged.
- Audit hash domains, manifest kinds, generation IDs, receipts, scratch names,
  and ownership tags must still identify existing data and processes.
- The scheduler keeps the `firewatch-index` state directory and its controller
  lock. Batch scripts keep the established shared lock and check both old and
  current service names, so two versions cannot silently write at once.
- Monitor schema-v3 JSON keeps `firewatch_index_*` wire fields and lane IDs.
  Rust names and display labels use the current name. Summary parsing also
  accepts current aliases and rejects duplicate old/new fields.
- Old controller and monitor environment variables remain fallback inputs.
  Current configuration takes precedence. The old monitor command option is
  a hidden alias. Process recovery can identify an exact old process identity.
- Historical executable paths remain valid only where an immutable audit
  manifest or existing process requires them. No old package is restored.

The [indexer guide](../../indexer/blockzilla-user-program-index/README.md#naming-and-compatibility)
and [monitor guide](../../blockzilla/monitor/README.md) describe these interfaces.

## Saved measurements and external applications

The active NAS benchmark uses a frozen package built before this naming
change. Its executable names, raw result IDs, output schema, source hashes,
and saved paths remain unchanged. New display labels map that recorded
`firewatch` workload to `user-program-index`. No measured value is changed.
The working tree now differs from the source snapshot of the timed package;
the benchmark provenance must refer to that frozen snapshot.

Dated reviews can retain source paths, quotes, and commands from their reviewed
revisions. Migration tables also retain old input names. These do not describe
additional current crates.

FireWatch in the [external integration guide](../guides/firewatch-local-archive-indexing.md)
is the separate application in `ferno-watcher`. Its parser, database, and
identity cache are outside this rename. The guide and its references now state
this distinction.

## Validation

The affected reader examples, profile tool, indexer, and operational targets
pass Cargo checks with the optional developer tools enabled. The main
Blockzilla command also passes its Cargo check. The existing
workload and indexer library tests pass: 23 and 105 tests. The monitor passes
114 tests, including compatibility input and status checks. The batch auditor
passes 31 tests, including exact old/current process identity checks. The matrix and
comparison suites pass 44 tests, including six focused rename regressions.
Shell syntax and service-guard checks pass. Formatting checks pass.

An independent review checked all current binary paths and found unchanged
workload behavior. Another review checked 486 local Markdown links, 18
fragments, 13 target renames, and nine target-feature mappings without errors.
The report builders confirm that the frozen source JSON is unchanged.
