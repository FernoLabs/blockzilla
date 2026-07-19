# Blockzilla CLI

`blockzilla` is the main product in this repository. It converts Solana CAR or
CAR.ZST files into indexed Archive V2 directories and reads them back.

The local CAR-to-Archive V2 path works today. The CLI and format are pre-1.0;
pin the producing Git revision. The CLI can finalize captured live input, but
continuous production ingestion, scheduling, replication, and indexer
streaming remain roadmap work.

## Try it

The repository [quick start](../README.md#quick-start-build-and-read-the-fixture)
builds and reads a one-block fixture without network access.

```bash
cargo run --locked -p blockzilla -- --help
```

The main commands are:

| Command | Purpose |
| --- | --- |
| `preflight-car` | Verify that a CAR can be streamed to clean EOF. |
| `build-archive-v2-hot-blocks` | Build compressed blocks, indexes, metadata, and serving sidecars. |
| `bench-archive-v2-hot-blocks` | Read and decode indexed blocks. |

Other commands are advanced repair, analysis, migration, or benchmark tools;
use `--help` as the authoritative command reference.

## Archive format

An Archive V2 directory contains compressed blocks plus indexes, registries,
metadata, and optional serving sidecars. The implemented layout is documented
in the [Archive V2 format reference](../docs/reference/archive-v2-hot-block-format.md)
and shared types live in [`blockzilla-format`](../crates/blockzilla-format/README.md).

Use a fresh output directory, keep source CAR files immutable, and budget ample
space for full epochs. See the [roadmap](../ROADMAP.md) for planned product work
and the [security policy](../SECURITY.md) for private reporting.
