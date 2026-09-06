# Archive formats and dedicated readers

Status: workspace structure and reader entry points checked, 2026-09-06.

Blockzilla has one reader crate and one small example package for each
stored archive format. Start with the package for the format that you want to
read. The reader hides object paths, HTTP ranges, cache rules, source checks, and
wire decoding.

| Format | Reader | Example package |
|---|---|---|
| Old Faithful CAR | [`of-car-reader`](../../crates/old-faithful/of-car-reader/Readme.md), with the `archive` feature | [`read-car`](../../examples/read-car/README.md) |
| Compact V2 | [`blockzilla-compact-v2-reader`](../../crates/compact-v2/blockzilla-compact-v2-reader/README.md) | [`read-compact-v2`](../../examples/read-compact-v2/README.md) |
| Archive V3 | [`blockzilla-archive-v3-reader`](../../crates/archive-v3/blockzilla-archive-v3-reader/README.md) | [`read-archive-v3`](../../examples/read-archive-v3/README.md) |

Use `of_car_reader::archive::CarArchive`,
`blockzilla_compact_v2_reader::CompactV2Archive`, or
`blockzilla_archive_v3_reader::IndexerV3Archive` for the selected format. Enable
the Compact V2 reader's `http` feature for network access. Archive V3 keeps the
`IndexerV3` prefix on its public Rust types for compatibility.

The readers publish the same ordered views from
[`blockzilla-model`](../../crates/blockzilla-model/README.md). This
lets the three small binaries use the same application rules without a
run-time format switch.

Use these two references:

- [Sample bucket layout and design choices](archive-sample-layout-and-design.md)
- [Detailed reader behavior](archive-reader-formats.md)

## Simple start

The beginner workload programs default to epoch 900 and scan the complete
epoch. During public-source staging, use the matching local folder:

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

Archive V3 still uses `indexer-v3` in its storage and HTTP paths, for example
`archive/indexer-v3/900/` and `/indexer-v3/900/<file-name>`.

## Design summary

- CAR preserves the independent source graph. Its slot-to-offset index gives
  direct ordered ranges.
- Compact V2 stores compressed row-oriented blocks and shared registries. It
  is small and efficient for a complete ordered scan.
- Archive V3 separates semantic planes and adds adaptive reverse lookup. A
  sparse query can reject blocks before it reads their payloads.

These reader paths do not require an archive publication manifest, an archive
payload hash, a partial hash, or an epoch seal. Network sources use fixed
object names, exact lengths, and strong ETags. Local sources use pinned files.
Application-output hashes check result parity only.
