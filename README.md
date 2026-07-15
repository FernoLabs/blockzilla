# Blockzilla

Blockzilla is an open-source toolkit for building, reading, validating, and serving compact Solana historical archives.

The project is centered on **Archive V2**: an indexed, block-oriented format designed for efficient sequential scans, direct per-slot reads, and derived indexes without running a validator.

> [!IMPORTANT]
> Archive V2 is under active development. The format and command-line interfaces may change before a 1.0 release.

## What is included

- `blockzilla`: the main Archive V2 builder, inspector, repair, and benchmark CLI.
- `blockzilla-format`: shared archive records, indexes, registries, and codecs.
- `of-car-reader`: streaming readers for Old Faithful CAR and CAR.ZST inputs.
- `of-slot-ranges`: compact slot-to-CAR-range index tooling.
- `blockzilla-live-producer`: live-feed capture and archive-production primitives.
- `blockzilla-get-block-worker`: a read-only `getBlock` serving implementation.
- `blockzilla-token-api`: an example derived index and token-data API.

Operational control planes, machine-specific deployment configuration, generated release bundles, and incident reports are intentionally maintained outside the public reference tree.

## Quick start

Install a current stable Rust toolchain, then build the workspace:

```bash
git clone https://github.com/FernoLabs/blockzilla.git
cd blockzilla
cargo build --workspace
cargo run -p blockzilla --bin blockzilla -- --help
```

Build a one-block Archive V2 sample from the included CAR fixture:

```bash
cargo run --release -p blockzilla --bin blockzilla -- \
  build-archive-v2-hot-blocks \
  crates/old-faithful/car-reader/benches/fixtures/epoch-157-biggest.car \
  /tmp/blockzilla-smoke \
  --max-blocks 1 \
  --no-access
```

For a full epoch, replace the fixture with an Old Faithful `.car` or `.car.zst` file and remove `--max-blocks`.

## Archive V2 shape

```text
epoch-N/
  archive-v2-blocks.zstd
  archive-v2-blocks.index
  archive-v2-meta.wincode
  registry.bin
  registry_counts.bin
  blockhash_registry.bin
  signatures.bin
  poh.wincode
  shredding.wincode
```

Independent compressed block frames support direct indexed reads while registries and sidecars avoid repeating high-cardinality values in every block.

## Documentation

- [Documentation index](docs/README.md)
- [Archive V2 hot-block format](docs/reference/archive-v2-hot-block-format.md)
- [Why verifiable historical archives matter](docs/design/horizon-problem-statement.md)
- [Live archive producer](docs/guides/live-archive-producer.md)
- [Redundant live ingest design](docs/design/live-ingest-redundancy.md)
- [Current benchmark snapshot](docs/benchmarks/archive-v2-storage-read-getblock-2026-05-24.md)

## Development

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked
```

See [ROADMAP.md](ROADMAP.md) for the public priorities.

## License

Blockzilla is available under the [MIT License](LICENSE).
