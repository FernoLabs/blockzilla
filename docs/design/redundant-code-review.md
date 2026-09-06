# Repository redundant-code review

Review date: 2026-09-06. Branch: `refactor`. Baseline: `2703ed38`.

The review covers the workspace package graph, Rust modules, command targets,
examples, benchmarks, operational scripts, and current documentation. It also
checks tracked web source fingerprints and package boundaries; it does not
claim a Svelte behavior review. A content scan examined 719 tracked source and
configuration files over 160 bytes and found no identical whole files.
The important overlap is partial copies and inactive implementations.

## Changes applied

### One user-program index package

`blockzilla-firebase-indexer` is removed. The retained package is
[`blockzilla-user-program-index`](../../indexer/blockzilla-user-program-index/README.md).
Firebase's library already forwarded its index API to this package. Six old
implementation files were therefore inactive copies: `build`, `decode`,
`dense_accumulator`, `format`, `query`, and `signer_rank`. Their removal deletes
10,703 lines without removing an active implementation. The complete cleanup
reduces Rust source and test code by 13,129 lines.

The primary command is now `blockzilla-user-program-index`. Its subcommands,
options, and stored index layout are unchanged. The package also retains all
eight supported operational and measurement commands. `cli` enables the
primary command; `developer-tools` enables the operational commands and their
library modules. The default library does not add these optional dependencies.
Unused V3 reader re-exports and dependencies are removed.

The cgroup helper is linked once through the library, instead of compiled
again through a binary-local source path. Its required interface is public
because Rust binaries and libraries are separate crates. The monitor and
manual launch guard recognize the renamed binary and existing deployments.
Immutable audit manifests can retain the old pinned executable path; new
manifests can use the new name. Path order and binary hash checks remain.

The already parked Firewatch controller and its design notes move with the
indexer. It is still excluded from builds because its manifest API needs a
separate update. It is not counted as a supported binary.

### Put decoding in the reader layer

The historical Compact V2 field decoder now lives in
[`source_decode.rs`](../../crates/compact-v2/blockzilla-compact-v2-reader/src/source_decode.rs).
The user-program index retains its public `decode` import as a small re-export.
The V3 prototype uses the reader directly. No format reader depends on an
indexer application. This move retains the decoder body and its tests,
including the explicit historical integer policy and partial-read limits.

### Retire the unused replay codec

`blockzilla-replay-format` is removed. No external caller used its slot,
transaction, message, or instruction codec. The two existing callers of its
PoH helpers now use the V2 reader's `poh` module. The hashing algorithms,
bounds, error text, and two helper tests are retained.

Payload format 8 remains reserved and unsupported. The replay runtime and V3
wire format are unchanged. The design remains available; this removal does
not claim that V3 is an equivalent replay format. See the
[retirement record](replay-format-retirement.md).

### Share example report code

The V2 and V3 examples had identical `ExampleReport` definitions and three
implementations. They now use
[`examples/workloads/src/report.rs`](../../examples/workloads/src/report.rs).
Both packages retain their public trait exports. Output fields, field order,
workload names, and output versions are unchanged.

## Remaining consolidation candidates

The line counts below describe repeated nonblank runs, not safe deletion
amounts. Similar code can enforce different input and trust requirements.

| Priority | Code | Finding and next step |
| --- | --- | --- |
| 1 | [Legacy reader](../../crates/compat/blockzilla-read-sdk-legacy/src/lib.rs) and [current V2 reader](../../crates/compact-v2/blockzilla-compact-v2-reader/src/lib.rs) | The legacy package contains about 19,464 Rust lines, with overlap in reading, manifests, projection, and sources. CLI, gateway, replay runtime, SPYX, token dump, and operational audit tools still use it. Move their publication authority, inventory, lock, wire-profile, and selective-read behavior to supported APIs before removing it. |
| 2 | [V3 source reconstruction](../../crates/archive-v3/blockzilla-archive-v3-convert/src/source_v2.rs) and [V2 signed messages](../../crates/compact-v2/blockzilla-compact-v2-reader/src/signed_message.rs) | Serialization and instruction reconstruction overlap. Candidate ownership and errors differ. Make the reader own the implementation, adapt converter inputs, and verify reconstructed signed bytes before deletion. |
| 3 | [Trusted normalizer](../../blockzilla/cli/src/archive_v2/trusted_metadata_normalize.rs) and [publication normalizer](../../blockzilla/cli/src/bin/archive_v2_normalize_metadata.rs) | About 554 repeated lines in geometry, parsing, and row checks. Extract private helpers; retain the separate trust and publication checks. |
| 4 | [Token transaction dump](../../indexer/blockzilla-token-transaction-dump/src/lib.rs) | Report paths repeat about 225–235 lines of manifest, registry, and stream validation. Share admission and streaming helpers while retaining each report's final audit and output. |
| 5 | [Edge correctness tools](../../edgezilla/get-block/Cargo.toml) | RPC correctness and epoch benchmarks repeat about 377 lines of endpoint and sample planning. Share a test-tool helper module. This does not justify merging the edge services. |
| 6 | [CLI](../../blockzilla/cli/Cargo.toml) and [Hivezilla fixture benchmarks](../../hivezilla/service/Cargo.toml) | The benchmark-only `SpaceSavingPubkeyTracker` repeats about 120 lines. Extract it only within measurement tooling. |
| 7 | [V2 edge service](../../edgezilla/get-block/README.md) and [Old Faithful edge service](../../edgezilla/of-get-block/README.md) | About 128 repeated formatting lines. Shared formatting may be useful after a field-level RPC comparison; source and response contracts differ. |

These are identified follow-up work, not completed migrations. The legacy
reader and signed-message reconstruction are the highest-value next steps.

## Code retained for a reason

| Area | Decision |
| --- | --- |
| Archive families | Keep CAR, Compact V2, canonical V3, and the frozen standalone prototype. They read different stored formats. V3 remains the intended V2 replacement. |
| Old Faithful slot index | Keep `of-slot-ranges` with its reader. The edge reader uses its index. |
| Byte sources | Keep the source contract, local source, HTTP source, and cache. They have separate functions. Current V2 HTTP/cache files are re-exports, not copied implementations. |
| Parsers and live format | Keep log classification, structured program logs, DEX decoding, and shared live block types. They have active callers and distinct contracts. |
| Indexer applications | Keep dump database, token extraction, balance audit, SPYX query, and experimental token API. Their inputs and outputs differ. |
| Product groups | Keep Blockzilla tools, Hivezilla ingestion, Edgezilla services, and replay runtime separate. Similar archive access does not make the products duplicates. |
| Sample gateways | Keep both current gateways. Their public URLs and object-key mappings differ. |
| Reader/converter wrappers | Keep compatibility exports that forward to one implementation and keep converter-backed reader tests. They do not contain a second decoder. |
| Parked code | Retain the controller, unsupported archive-token-events example, and two benchmark attic binaries. They contain unique work or support recorded measurements. Port or retire them explicitly. |

No additional orphan Rust modules were found beyond the documented parked
code. Captured benchmark and operational evidence is unchanged.

## Validation

The package mapping verifies **44 packages, 163 targets, and all 105 binaries**.
The two removed targets are library targets. The primary indexer binary is
renamed; operational binaries retain their names and gain explicit features.
The target checker verifies each declared removal and feature change; its six
regression tests pass.

The final workspace run, with operational tools enabled, passed **3,697 tests**
across 138 harnesses, with no failures and one ignored manual benchmark. The
lower count reflects the removed unused codec tests and duplicate cgroup test
compilation; retained hash tests run from the reader. The new executable-path
regression test is included.

Workspace all-target checks and Blockzilla contributor/repair-tool checks
pass. The user-program library also compiles without command features, and
the renamed command runs with `cli` alone. Formatting, the Archive V2 wire
boundary, shell syntax, and 57 current local documentation links pass. The
shared decoder and report code match their prior implementations. Third-party
lockfile versions and checksums are unchanged.

The initial sandbox run could not bind loopback sockets. The final run used
local socket access. An existing Hivezilla 40 ms timeout test failed under
concurrent compilation load, then passed alone and in the full run with four
test workers. No production code or timeout was changed to bypass this test.

**Decision: local cleanup checks pass; merge after Linux CI passes.** Linux CI
and target Worker release builds remain required before release. `main` is
unchanged. The pre-existing model error-message edit is unchanged and remains
outside this cleanup.
