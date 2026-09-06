# Blockzilla

Blockzilla is a Rust toolkit for turning Solana history into compact, indexed
archive files for local streaming, scanning, filtering, and historical reads.

The main product is the `blockzilla` CLI and the Blockzilla archive format. Both are
pre-1.0, so pin the Git revision used to produce an archive.

## Components

| Path | Role |
| --- | --- |
| [`blockzilla/`](blockzilla/README.md) | CLI, archive gateway, and monitor. |
| [`hivezilla/`](hivezilla/README.md) | Live capture, replication, protocol, and storage. |
| [`edgezilla/`](edgezilla/README.md) | Independently deployed Cloudflare Workers. |
| [`crates/`](crates) | Archive formats, readers, byte sources, and parsers. |
| [`indexer/`](indexer/README.md) | Index builders, queries, extraction, and audits. |
| [`runtime/`](runtime/README.md) | Experimental transaction replay. |
| [`examples/`](examples) | Archive reader examples and canonical V3 candidate checks. |
| [`bench/`](bench) | Reader and conversion measurements. |
| [`web/`](web/spyx-explorer/README.md) | Web applications. |
| [`docs/`](docs/README.md) | Format specifications, architecture, and operating guides. |
| [`scripts/`](scripts/README.md) | Build, benchmark, and operational helpers. |

Archive V3 is the canonical format intended to replace Archive V2. Its converter
candidates can be read locally with `ia-read`. The `IndexerV3Archive` API and
standard `read-archive-v3-*` workloads still read the frozen standalone prototype;
they cannot read canonical converter output. The dedicated V3 reader crate now
owns `CanonicalReader`, used by `ia-read` and the converter's index builders.
The common reader interface and HTTP support for canonical V3 remain to be
added, with cross-format fixture checks. See the
[workspace structure](docs/design/workspace-restructure.md) for crate locations
and the remaining migration work.

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
  crates/old-faithful/of-car-reader/benches/fixtures/epoch-157-biggest.car \
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

Read the [documentation index](docs/README.md) before working on formats,
deployments, or credential-handling code. Cross-cutting future work and product
decisions are tracked in the [project backlog](TODO.md).

Blockzilla is available under the [MIT License](LICENSE).
