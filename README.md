# Blockzilla

Blockzilla is a Rust toolkit for turning Solana history into compact, indexed
archive files for local streaming, scanning, filtering, and historical reads.

The main product is the `blockzilla` CLI and the Blockzilla archive format. Both are
pre-1.0, so pin the Git revision used to produce an archive.

## Components

| Path | Status | Role |
| --- | --- | --- |
| [`blockzilla/`](blockzilla/README.md) | Active | Builds and reads Blockzilla archives and includes an experimental finite-work scheduler. |
| [`services/`](services/README.md) | Prototype / experimental | Hivezilla durable capture/replication and read-only Edgezilla implementations. |
| [`apps/`](apps/README.md) | Experimental | Watcher UI and other deployable frontend tooling. |
| [`crates/`](crates/) | Libraries | Shared formats, parsers, and Old Faithful readers. |
| [`examples/token-api/`](examples/token-api/README.md) | Example | Builds a small derived index and local API. |
| [`docs/`](docs/README.md) | Reference | Implemented formats, architecture, research, and historical results. |
| [`scripts/`](scripts/README.md) | Developer tools | Benchmarks and correctness utilities. |

The shortest working newcomer path is:

```text
Old Faithful CAR -> Blockzilla -> local archive files
```

The target system adds independent Hivezilla capture and raw custody,
Blockzilla scheduling and catalog authority, fenced Hivezilla archive workers,
cloud replication, Edgezilla serving, and local indexer streaming. See the
[system schema](docs/architecture/full-system-schema.md) for that direction and
the [Hivezilla implementation assessment](docs/architecture/hivezilla-v1-implementation-plan.md)
for the code-to-target gap and ordered build gates.

## Quick start: build and read the fixture

The repository pins Rust 1.96.0. Clone it and inspect the CLI:

```bash
git clone https://github.com/FernoLabs/blockzilla.git
cd blockzilla
cargo run --locked -p blockzilla -- --help
```

Build one block from the included fixture and read it back:

```bash
OUT="$(mktemp -d)"

cargo run --locked -p blockzilla -- \
  build-archive-v2-hot-blocks \
  crates/old-faithful/car-reader/benches/fixtures/epoch-157-biggest.car \
  "$OUT" \
  --max-blocks 1 \
  --no-access

cargo run --locked -p blockzilla -- \
  bench-archive-v2-hot-blocks \
  "$OUT/archive-v2-blocks.zstd" \
  --workers 1 \
  --chunk-size 1
```

The fixture is only a smoke test. Full epochs can require hundreds of gigabytes
across source, output, and temporary files; use a fresh output directory and
keep the source archive immutable.

## Development

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked
```

Read the [documentation index](docs/README.md) and the
[security policy](SECURITY.md) before working on formats, deployments, or
credential-handling code.

Blockzilla is available under the [MIT License](LICENSE).
