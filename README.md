# Blockzilla

Blockzilla is an open-source toolkit for building, reading, validating, and
serving compact Solana historical archives without running a validator.

The primary product is the `blockzilla` command-line application and its
**Archive V2** format. Archive V2 stores independently compressed blocks with
indexes and shared sidecars so readers can scan an epoch or seek directly to a
slot.

> [!IMPORTANT]
> Blockzilla is pre-1.0. Archive V2 and the command-line interface can still
> change between commits. Pin the revision used to create an archive.

## What works today

The supported newcomer path is deliberately small:

```text
Old Faithful CAR or CAR.ZST
            |
            v
      Blockzilla CLI
            |
            v
   local Archive V2 directory
            |
            v
 local readers, benchmarks, and indexer experiments
```

`blockzilla` can preflight a CAR, build a local Archive V2 directory, create
read sidecars, and read/decode the result. That path is exercised by the fixture
workflow below.

## Product status

| Product | Status | Current boundary |
| --- | --- | --- |
| [Blockzilla](blockzilla/README.md) | Active, primary product | Builds and reads local Archive V2 archives from CAR/CAR.ZST inputs. Repair and analysis commands are available but more specialized. |
| [Hivezilla](hivezilla/README.md) | Prototype | Contains gRPC capture, durable-spool, inspection, and repair experiments. Multi-provider failover and shred capture are not production-ready. |
| [Edgezilla](edgezilla/README.md) | Experimental | Contains read-only Cloudflare Workers for Blockzilla Archive V2 and direct Old Faithful CAR compatibility. Replication is not implemented here yet. |
| [`examples/token-api`](examples/token-api/README.md) | Experimental example | Builds a derived token index from Archive V2 and serves a small local API. |

The target system uses multiple Hivezilla instances to retain network input,
Blockzilla as the storage and compaction authority, and Edgezilla to serve a
replicated archive. Shred ingestion, scheduling, cloud replication, and the
proposed `blockzilla sync` / `blockzilla stream` indexer workflow are planned,
not part of the working quick start.

## Quick start: build and read the fixture

Prerequisites are Git and `rustup`. The repository pins Rust 1.96.0 in
[`rust-toolchain.toml`](rust-toolchain.toml), and `rustup` selects it
automatically. Clone the repository and inspect the CLI:

```bash
git clone https://github.com/FernoLabs/blockzilla.git
cd blockzilla
cargo run --locked -p blockzilla -- --help
```

Build one block from the included 2.5 MiB CAR fixture into a fresh temporary
directory:

```bash
OUT="$(mktemp -d)"

cargo run --locked -p blockzilla -- \
  build-archive-v2-hot-blocks \
  crates/old-faithful/car-reader/benches/fixtures/epoch-157-biggest.car \
  "$OUT" \
  --max-blocks 1 \
  --no-access
```

Inspect the required files, then perform an indexed read and full block decode:

```bash
test -s "$OUT/archive-v2-blocks.zstd"
test -s "$OUT/archive-v2-blocks.index"
test -s "$OUT/archive-v2-meta.wincode"
find "$OUT" -maxdepth 1 -type f -print | sort

cargo run --locked -p blockzilla -- \
  bench-archive-v2-hot-blocks \
  "$OUT/archive-v2-blocks.zstd" \
  --workers 1 \
  --chunk-size 1
```

A successful run creates the compressed block file and index plus metadata,
pubkey and blockhash registries, signatures, vote hashes, PoH, and shredding
sidecars. `--no-access` intentionally omits the two larger getBlock-access
sidecars from this smoke test.

> [!WARNING]
> The bundled fixture is a smoke test, not a sizing model. A real Solana epoch
> can require hundreds of gigabytes across its input, output, and temporary
> data. Use a fresh output directory on storage with ample free space, keep the
> source archive immutable, and use `--release` for full-epoch work.

## Repository map

| Path | Purpose |
| --- | --- |
| [`blockzilla/`](blockzilla/README.md) | Main CLI and Archive V2 build/read operations. |
| [`hivezilla/`](hivezilla/README.md) | Live-input prototype and `hivezilla` executable. |
| [`edgezilla/`](edgezilla/README.md) | Cloud replica and read-only serving experiments. |
| [`crates/blockzilla-format/`](crates/blockzilla-format/README.md) | Shared Archive V2 records, codecs, indexes, and registries. |
| [`crates/blockzilla-log-parser/`](crates/blockzilla-log-parser/README.md) | Runtime log parsing for compact log representations. |
| `crates/old-faithful/` | CAR readers and slot-range index utilities. |
| [`examples/token-api/`](examples/token-api/README.md) | Example derived index and local API. |
| [`docs/`](docs/README.md) | Architecture, format reference, design notes, guides, and curated benchmarks. |
| [`scripts/`](scripts/README.md) | Developer benchmark and correctness utilities; not product entry points. |

## Development

Run the workspace checks before submitting a change:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked
```

Start with the [documentation index](docs/README.md), then see the
[roadmap](ROADMAP.md) for planned work. Report vulnerabilities privately as
described in the [security policy](SECURITY.md).

## License

Blockzilla is available under the [MIT License](LICENSE).
