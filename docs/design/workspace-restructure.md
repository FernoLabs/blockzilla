# Workspace structure

Updated 2026-09-06 on branch `refactor`. The layout is applied: 46 workspace
packages, 165 targets, and 105 binaries. The former CAR SDK is merged into
`of-car-reader`. No binary was deleted by the folder move.

## Layout

Group code by subject. Shared formats and readers stay in `crates/`. Indexer
applications and runtime experiments have their own top-level groups.

```text
crates/
  blockzilla-model/                 canonical blocks and ordering contract
  blockzilla-primitives/            keys, string table, LEB128 framing
  blockzilla-live-format/           shared live and candidate formats
  blockzilla-replay-format/         reserved replay format; removal candidate
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
  blockzilla-firebase-indexer/

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
package. The excluded `examples/archive-token-events` source is also retained.

## Decisions

- **Archive V3 replaces V2 in the long term.** Index Archive is the former
  working name for V3. V3 is an archive format, not an indexer product.
  Both versions remain supported during the transition. Existing archive
  filenames, format identifiers, routes, and stored bytes are unchanged.
- **One reader per format.** The CAR archive interface is now
  `of_car_reader::archive`, behind the opt-in `archive` feature. The V2
  interface is `blockzilla_compact_v2_reader::archive`. The V3 engine now
  belongs to `blockzilla-archive-v3-reader`; Firebase re-exports its old API.
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
- **Do not enable or delete the reserved replay format during this move.**
  Its [removal review](../../crates/blockzilla-replay-format/REMOVAL-CANDIDATE.md)
  remains separate from runtime experiments and the folder changes.

## Completed work

The working-tree snapshot, branch merge, source extraction, V2 interface merge,
format split, and migration off `blockzilla-format` are committed. The format
split includes explicit feature forwarding and `unexpected_cfgs = "deny"`.

The CAR interface is merged into its reader. The V3 engine is extracted from
Firebase. The V3 converter is callable through
`blockzilla archive-v3 convert`, alongside the five V3 index commands. The
standalone converter remains as a compatibility entry point to the same code.
`ia-read` is now an advanced example. The account-scan and prefix-comparison
commands live in the benchmark package. Their executable names are unchanged.

The legacy reader has moved into `compat/`. That is a path change, not proof
that its consumers have migrated. Package and folder names now match this
layout. The model error-message edit that existed before this work is retained.

## Remaining migration work

1. Migrate the six remaining legacy-reader consumers: the CLI, archive gateway,
   token dumper, Firebase tools, replay tools, and SPYX query tools. Preserve
   selective metadata, signer projection, compact logs, and batch behavior.
2. Extract shared record projection and decoding from the V2 reader and
   user-program index. V3 still depends on these implementations. Removing
   Firebase ownership does not remove those separate dependencies.
3. Verify current archive message and metadata schemas, then finish the V2
   freeze. Only then remove legacy readers and wire-profile migration code.
   Moving a directory does not make it safe to delete.
4. Restore the parked Firebase controller and the excluded archive-token-events
   example against supported readers. Its old SDK-boundary test is not
   currently part of workspace CI.
5. Complete cross-format output and performance checks on the archive corpus.
   Local fixture tests and successful compilation do not prove production
   archive compatibility or release readiness.

## Verification

Save Cargo metadata before and after changes. Verify the full mapping with:

```sh
python3 scripts/check-workspace-targets.py before.json after.json \
  --layout docs/design/workspace-layout.json
```

The checker accounts for declared package renames and CAR package removal. It
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
