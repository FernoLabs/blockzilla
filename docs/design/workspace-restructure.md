# Workspace structure

Updated 2026-09-06 on branch `refactor`. The layout is applied: 44 workspace
packages, 163 targets, and 105 binaries. The former CAR SDK is merged into
`of-car-reader`. No binary was deleted by the folder move.

## Layout

Group code by subject. Shared formats and readers stay in `crates/`. Indexer
applications and runtime experiments have their own top-level groups.

```text
crates/
  blockzilla-model/                 canonical blocks and ordering contract
  blockzilla-primitives/            keys, string table, LEB128 framing
  blockzilla-live-format/           shared live and candidate formats
  source/
    blockzilla-source/
    blockzilla-source-local/
    blockzilla-source-http/
    blockzilla-source-cache/
  solana-codec/
    solana-shred-codec/
  parser/
    blockzilla-log-parser/
    blockzilla-program-logs/
    blockzilla-dex-parser/
  old-faithful/
    of-car-reader/                 CAR types, reader, and archive opening
    of-slot-ranges/                slot-index builder, repair, and validation
  compact-v2/
    blockzilla-registry/
    blockzilla-compact/
    blockzilla-archive-v2/
    blockzilla-compact-v2-reader/
  archive-v3/
    blockzilla-archive-v3/
    blockzilla-archive-v3-reader/
    blockzilla-archive-v3-convert/
  compat/
    blockzilla-read-sdk-legacy/

indexer/
  blockzilla-user-program-index/
  blockzilla-spyx-query/
  blockzilla-token-transaction-dump/
  blockzilla-token-balance-audit/
  blockzilla-dump/

runtime/
  blockzilla-replay/

blockzilla/
  cli/  archive-gateway/  monitor/
hivezilla/
  service/  protocol/  object-store/  ledger-compat/  gossip-compat/
edgezilla/
  get-block/  of-get-block/  archive-samples/  r2-gateway/
examples/
  workloads/  read-car/  read-compact-v2/  read-archive-v3/  token-api/
bench/
  reader-profile/  archive-v3-measure/
web/  docs/  scripts/
```

The complete package mapping is in [workspace-layout.json](workspace-layout.json).
It includes unchanged packages, explicit renames, and the removed CAR interface
package, the merged Firebase package, and the retired replay codec. The excluded `examples/archive-token-events` source is also retained.

## Decisions

- **Archive V3 replaces V2 in the long term.** Index Archive is the former
  working name for the canonical V3 format, with `catalog/blocks.wincode`
  and `ledger/transactions.wincode`. The converter writes this format.
  The frozen standalone Indexer V3 prototype is a different format: it uses
  `archive-v2-standalone-blocks.index` and separate standalone payload files.
  The folder move changes neither format's stored bytes, filenames, or routes.
- **One reader per format is the target.** The CAR archive interface is now
  `of_car_reader::archive`, behind the opt-in `archive` feature. The V2
  interface is `blockzilla_compact_v2_reader::archive`. The dedicated V3
  reader owns `CanonicalReader` for canonical local files and retains the
  prototype's `IndexerV3Archive` API. These APIs open
  different layouts. The converter retains compatibility exports of the local
  reader; read applications use the reader crate directly.
- **Keep the shared model.** `blockzilla-model` provides common block types,
  ordering checks, and sinks used by the examples. The per-format readers
  retain their own optimized operations.
- **Keep transport independent.** Local files, HTTP ranges, and the HTTP
  object cache use the same byte-source contract. Source crates do not
  depend on archive formats. The cache stores whole selected objects.
- **Keep primitives shared.** Log parsers and multiple formats need the key,
  string-table, and framing types. They do not belong inside one format.
- **Keep Old Faithful tools together.** `of-slot-ranges` retains all eight
  binaries and its reading API beside the CAR types and reader.
- **Keep index names accurate.** `blockzilla-user-program-index` already
  implements signer-to-program relations. Token extraction and balance
  verification are separate tools; they are not renamed as index products.
- **Keep only existing Solana codecs.** Shred is the shared local codec.
  Geyser types come from upstream. A shared RPC renderer requires a separate
  field-level comparison. JSON key order alone is not a correctness defect.
- **Keep live format shared.** Both the CLI and Hivezilla use it. Ledger and
  gossip compatibility code stay beside Hivezilla.
- **Retire the unused replay codec.** Its PoH hashing helpers now live in
  the V2 reader. Payload format 8 stays reserved; the replay runtime is
  unchanged. See the [retirement record](replay-format-retirement.md).
- **One user-program index package.** The Firebase command and eight supported
  operational tools now live in `blockzilla-user-program-index`. Its six
  inactive implementation copies and unused reader re-exports are removed.
  The command uses the `cli` feature; operational tools use `developer-tools`.

## Completed work

The working-tree snapshot, branch merge, source extraction, V2 interface merge,
format split, and migration off `blockzilla-format` are committed. The format
split includes explicit feature forwarding and `unexpected_cfgs = "deny"`.

The CAR interface is merged into its reader. The frozen Indexer V3 prototype
engine is extracted from Firebase. The canonical V3 converter is callable through
`blockzilla archive-v3 convert`, alongside the five V3 index commands. The
standalone converter remains as a compatibility entry point to the same code.
`ia-read` is now an advanced example that reads canonical converter candidates
through the dedicated reader's `CanonicalReader`. The reader also owns common
header validation and bounded decompression. Pure account geometry is shared
by readers and writers in the V3 format crate. Converter-backed tests retain
the writer-to-reader checks without a reader-to-converter dependency.
The standard `read-archive-v3-*` workloads still use
the prototype reader. The account-scan and prefix-comparison
commands live in the benchmark package. Their executable names are unchanged.

The legacy reader has moved into `compat/`. That is a path change, not proof
that its consumers have migrated. Package and folder names now match this
layout. The model error-message edit that existed before this work is retained.

The review also recovered the bounded borrowed decoder and the canonical LEB128
guard lost during earlier changes. Historical source readers use an explicit
tolerant integer mode to preserve the supported padded source fields. Both
modes retain integer bounds and write the same canonical bytes. Canonical
rewrite, PoH, shredding, and native V3 object validation remain strict.
Integrity and signature checks now use the
admitted source descriptor for both trusted and published readers. Parallel
decoding keeps each block independently available to a worker. Test fixtures
now supply the current source bindings and distinguish general CAR reconstruction
from the ordered reader's physical-order contract.

## Remaining migration work

1. Add the shared model and byte-source interfaces to the canonical local
   reader in `blockzilla-archive-v3-reader`. Keep the prototype API explicit
   during the transition. Gate this work with small V2-to-V3 fixtures that check reader output, metadata coverage,
   signatures, and hashes, plus cross-format checks through `blockzilla-model`.
   A reader move alone does not add HTTP support or common-model projection.
2. Migrate the six remaining legacy-reader consumers: the CLI, archive gateway,
   token dumper, user-program operational tools, replay tools, and SPYX query tools. Preserve
   selective metadata, signer projection, compact logs, and batch behavior.
3. Consolidate signed-message reconstruction between the V2 reader and V3
   converter. The historical source decoder is now shared by the V2 reader,
   V3 prototype, and user-program index. No format reader depends on an indexer.
4. Verify current archive message and metadata schemas, then finish the V2
   freeze. Only then remove legacy readers and wire-profile migration code.
   Moving a directory does not make it safe to delete.
5. Restore the parked Firewatch controller and the excluded archive-token-events
   example against supported readers. Its old SDK-boundary test is not
   currently part of workspace CI.
6. Complete cross-format output and performance checks on the archive corpus.
   Local fixture tests and successful compilation do not prove production
   archive compatibility or release readiness.

## Redundant-code review

The later [repository audit](redundant-code-review.md) records the indexer
merge, shared decoder and report extraction, replay-codec retirement, and
remaining consolidation work. These changes retain all supported binaries.

## Main and reader-fix integration

The [merge review](refactor-merge-review.md) records the later integration of
local main and the sample-reader fixes, including the current test results and
output-version changes. The checks below describe the completed structural
refactor before that integration.

## Verification

The final local workspace run passed **3,703 tests**, with no failures and one
ignored manual release-mode encoder benchmark. It completed 140 test harnesses.
The canonical reader extraction also passed its isolated format, reader, and
converter tests (101, 138, and 121). The final historical-source checks passed
67 Archive V2 and 118 legacy-reader tests. The ordered-reader scheduling fix
passed 100 repeated runs of the 12-worker contention test.

Normal binary and all-target workspace builds pass, as do the optional
contributor builds. The metadata repair and three shred-reconstruction suites
pass (3, 8, 12, and 22 tests). The Old Faithful reader passes its WebAssembly
build with `compact-index,zstd-wasm` and no default features.

The command-line fixture check built an Archive V2 archive from the included
CAR sample, then read it successfully: one block and 4,208 transactions.
The earlier folder checks also passed the four Hivezilla shell suites, 58 shell
syntax checks, nine archive-sample Python tests, 28 ingest-status Python tests,
and the replay helper self-tests. Local Markdown links were checked after the
moves and the reader documentation update.

These results are from local macOS checks. They do not replace Linux CI,
Worker release builds, or the production corpus checks listed above.

For the structural refactor, formatting and the Archive V2 wire-boundary check
passed, and the package mapping accounted for 46 packages, 165 targets, and
105 binaries. The later consolidation reduced this to 44 packages and 163
targets, with all 105 binaries retained. Third-party
lockfile versions and checksums are unchanged. Captured SPYX process evidence
is unchanged, and the pre-existing model error-message edit remains uncommitted.

Save Cargo metadata before and after changes. Verify the full mapping with:

```sh
python3 scripts/check-workspace-targets.py before.json after.json \
  --layout docs/design/workspace-layout.json
```

The checker accounts for package merges, explicit target removal, feature
changes, and package renames. It
checks every destination, target name, target kind, and required feature. It
rejects missing and extra targets. Target identity is not sufficient by itself:

- Run workspace and isolated-package builds to catch missing feature forwarding.
- Run the affected reader, converter, CLI, and repair tests.
- Verify embedded file paths and paths based on `CARGO_MANIFEST_DIR`.
- Check CI, launch scripts, sample-runner commands, and local documentation links.
- Preserve captured evidence and external storage paths.

SPYX hashes parser source files, including `market_builder.rs` itself. Changing
its embedded source paths changes that implementation fingerprint. Existing
market data remains on disk, but opening it with a new binary can require a
rebuild under the existing fingerprint check. No automatic rebuild or deletion
is part of this refactor.
