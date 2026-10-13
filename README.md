# Blockzilla

Read Solana history from a compact archive on Cloudflare. The reader runs on
your machine, downloads only the bytes it needs, and gives your code ordered
blocks and transactions. You need no validator and no RPC node.

```bash
git clone --depth 1 https://github.com/FernoLabs/blockzilla.git && cd blockzilla
cargo install --locked --path blockzilla/cli --bin blockzilla --debug
blockzilla stream --epoch 200 --max-blocks 5
```

Start with the [quick start](docs/start/quick-start.md), then the
[user guide](docs/start/overview.md): [Rust callbacks](docs/start/callbacks.md),
[token exports](docs/start/token-dumper.md), a
[token SQLite index](docs/start/token-indexer.md),
[program dumps](docs/start/program-dumper.md), and the
[live stream](docs/start/live-stream.md) (private beta). For storage details,
see [file formats](docs/advanced/formats.md).

This repository also contains the tools that build the archive. The CLI and the
archive format are pre-1.0, so pin the Git revision that produced an archive.
The sections below are for contributors.

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

For current reader use, start with the [format guide](docs/reference/archive-formats-and-read-sdk.md)
and the [CAR](examples/read-car/README.md), [V2](examples/read-compact-v2/README.md),
or [V3](examples/read-archive-v3/README.md) examples. V2 uses a bounded rolling
pipeline and can write optional [indexed USDC balances](docs/reference/usdc-indexed-balances-v1.md)
with a source-scoped public-key dictionary. The
[rolling-pipeline comparison](docs/benchmarks/epoch-300-rolling-pipeline-2026-09-06.md)
records full-epoch output checks for its frozen build. The later
[dependency retest](docs/benchmarks/epoch-300-dependency-review-2026-09-06.md)
checks V2 prefixes after the dependency update.

The user read path is:

```text
Blockzilla archive on Cloudflare -> Blockzilla reader -> application or database
```

The Cloudflare archive serves Compact V2 sample epochs. The live stream is in
private beta. A crates.io release and a complete historical catalog are later
milestones. Start with the [user guide](docs/start/overview.md); archive
production and fixture checks below are for contributors.

## Quick start: build and read the fixture

Use a Rust toolchain that meets the workspace `rust-version` in
[`Cargo.toml`](Cargo.toml). Clone the repository and inspect the CLI:

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
