# Archive formats and dedicated read SDKs

Status: implemented product guide, 2026-08-31.

Blockzilla has one dedicated read SDK and one small example package for each
stored archive format. Start with the package for the format that you want to
read. The SDK hides object paths, HTTP ranges, cache rules, source checks, and
wire decoding.

| Format | SDK | Example package |
|---|---|---|
| Old Faithful CAR | [`blockzilla-car-read-sdk`](../../crates/blockzilla-car-read-sdk/README.md) | [`read-car`](../../examples/read-car/README.md) |
| Compact V2 | [`blockzilla-compact-v2-read-sdk`](../../crates/blockzilla-compact-v2-read-sdk/README.md) | [`read-compact-v2`](../../examples/read-compact-v2/README.md) |
| Indexer V3 | [`blockzilla-indexer-v3-read-sdk`](../../crates/blockzilla-indexer-v3-read-sdk/README.md) | [`read-indexer-v3`](../../examples/read-indexer-v3/README.md) |

The SDKs publish the same ordered views from
[`blockzilla-query-sdk`](../../crates/blockzilla-query-sdk/README.md). This
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

cargo run --release --locked -p blockzilla-read-indexer-v3 \
  --bin read-indexer-v3-usdc -- \
  --archive-root archive
```

Replace the binary name with the Pump.fun or FireWatch program in the same
package. Read the package guide for output names and the FireWatch wallet
argument.

When the public sample is active, omit `--archive-root archive`. All three
examples use the same configured origin and the same
`/<format>/<epoch>/<file-name>` layout.

## Design summary

- CAR preserves the independent source graph. Its slot-to-offset index gives
  direct ordered ranges.
- Compact V2 stores compressed row-oriented blocks and shared registries. It
  is small and efficient for a complete ordered scan.
- Indexer V3 separates semantic planes and adds adaptive reverse lookup. A
  sparse query can reject blocks before it reads their payloads.

These reader paths do not require an archive publication manifest, an archive
payload hash, a partial hash, or an epoch seal. Network sources use fixed
object names, exact lengths, and strong ETags. Local sources use pinned files.
Application-output hashes check result parity only.
