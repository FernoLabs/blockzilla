# Archive formats and dedicated readers

Status: workspace structure and reader entry points checked, 2026-09-06.

Select the reader by the stored layout. Canonical Archive V3 is the format
intended to replace V2. The frozen standalone Indexer V3 prototype has a
different layout and reader; the two are not interchangeable.

| Format | Reader | Example package |
|---|---|---|
| Old Faithful CAR | [`of-car-reader`](../../crates/old-faithful/of-car-reader/Readme.md), with the `archive` feature | [`read-car`](../../examples/read-car/README.md) |
| Compact V2 | [`blockzilla-compact-v2-reader`](../../crates/compact-v2/blockzilla-compact-v2-reader/README.md) | [`read-compact-v2`](../../examples/read-compact-v2/README.md) |
| Frozen standalone Indexer V3 prototype | [`blockzilla-archive-v3-reader`](../../crates/archive-v3/blockzilla-archive-v3-reader/README.md), `IndexerV3Archive` | Standard `read-archive-v3-*` workloads in [`read-archive-v3`](../../examples/read-archive-v3/README.md) |
| Canonical Archive V3 converter output | [`blockzilla_archive_v3_reader::CanonicalReader`](../../crates/archive-v3/blockzilla-archive-v3-reader/src/canonical.rs), local files only | [`ia-read`](../../examples/read-archive-v3/src/bin/ia-read.rs) in the same example package |

Use `of_car_reader::archive::CarArchive`,
`blockzilla_compact_v2_reader::CompactV2Archive`, or
`blockzilla_archive_v3_reader::IndexerV3Archive` for CAR, Compact V2, or the
frozen prototype respectively. Enable the Compact V2 reader's `http` feature
for its high-level archive API, including local opening. These APIs hide object paths, HTTP ranges, cache rules,
source checks, and wire decoding.

These three readers publish the same ordered views from
[`blockzilla-model`](../../crates/blockzilla-model/README.md). This
lets the three small binaries use the same application rules without a
run-time format switch. The canonical `CanonicalReader` instead returns
format-native blocks and transactions. It does not yet implement the common
`ArchiveInstructionSource` interface or HTTP access.

`IndexerV3Archive` requires `archive-v2-standalone-blocks.index` and the
standalone payload files. It cannot open canonical converter output, which
uses `catalog/blocks.wincode`, `ledger/transactions.wincode`, and the other
objects in the [canonical V3 format](../../crates/archive-v3/blockzilla-archive-v3/README.md).

Use these two references:

- [Sample bucket layout and design choices](archive-sample-layout-and-design.md)
- [Detailed reader behavior](archive-reader-formats.md)

## Simple start

The CAR, V2, and prototype workload programs default to epoch 900 and scan the
complete epoch. During public-source staging, use the matching local folder:

```console
cargo run --release --locked -p blockzilla-read-car \
  --bin read-car-usdc -- \
  --archive-root archive

cargo run --release --locked -p blockzilla-read-compact-v2 \
  --bin read-compact-v2-usdc -- \
  --archive-root archive

cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin read-archive-v3-usdc -- \
  --archive-root archive
```

Replace the binary name with the Pump.fun or FireWatch program in the same
package. Read the package guide for output names and the FireWatch wallet
argument.

When the public sample is active, omit `--archive-root archive`. All three
examples use the same configured origin and the same
`/<format>/<epoch>/<file-name>` layout.

The frozen prototype uses `indexer-v3` in its storage and HTTP paths, for example
`archive/indexer-v3/900/` and `/indexer-v3/900/<file-name>`.

Read one slot from a canonical V3 converter candidate with:

```console
cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin ia-read -- <candidate-directory> <slot>
```

Add `--full` to read the runtime effect columns. This local example uses
`blockzilla_archive_v3_reader::CanonicalReader`.

## Design summary

- CAR preserves the independent source graph. Its slot-to-offset index gives
  direct ordered ranges. Local opening supports `.car.zst` when completed raw
  `.car` is absent; compressed scans follow decoded offsets sequentially.
- Compact V2 stores compressed row-oriented blocks and shared registries. It
  uses a bounded rolling worker window for ordered scans. The optional indexed
  USDC output keeps numeric references and a source-scoped discovery dictionary.
- The frozen Indexer V3 prototype separates semantic planes and adds adaptive
  reverse lookup. A sparse query can reject blocks before it reads their payloads.
- Canonical Archive V3 uses a fixed-address block catalog, a merged transaction
  stream, dictionaries, runtime effect files, and separate indexes and sidecars.

The CAR, V2, and prototype reader paths do not require an archive publication
manifest, an archive payload hash, a partial hash, or an epoch seal. Network
sources use fixed object names, exact lengths, and strong ETags. Local sources
use pinned files.
Application-output hashes check result parity only.

See the [rolling pipeline](../design/reader-pipeline-rolling-window.md) and
[indexed USDC contract](usdc-indexed-balances-v1.md) for the execution and
output rules. The existing canonical `BZUSDC02` output is unchanged.

The dedicated V3 reader owns `CanonicalReader`. The remaining V3 migration
must add the shared model and byte-source interfaces for canonical output.
Use small V2-to-V3 conversion fixtures and common-model output comparisons as
gates before applications migrate. Reader extraction alone does not establish
cross-format parity or production archive compatibility.
