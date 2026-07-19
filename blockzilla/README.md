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
| `build-block-time-gaps` | Build a sparse local slot-gap and block-time sidecar without RPC. |
| `build-archive-v2-hot-blocks` | Build compressed blocks, indexes, metadata, and serving sidecars. |
| `bench-archive-v2-hot-blocks` | Read and decode indexed blocks. |

Other commands are advanced repair, analysis, migration, or benchmark tools;
use `--help` as the authoritative command reference.

### Block-time gap sidecars

For an Archive V2 epoch that contains `blockhash_index_v3.bin`, build a
`block-time-gaps.bin` sidecar entirely from local archive data:

```bash
cargo run --locked -p blockzilla -- build-block-time-gaps \
  /archive-v2/epoch-842 --epoch 842
```

The sidecar stores each run of absent slots together with the archived block
time immediately before and after it. It also preserves consecutive-slot time
discontinuities above the recorded whole-second threshold. These are archive
observations, not an automatic claim that Solana was unavailable: a slot gap
can also reveal incomplete archive input. The header retains the source
SHA-256, threshold, and first/last block so adjacent epoch results can be joined
safely.

Complete `build-archive-v2-hot-blocks` runs now publish
`blockhash_index_v3.bin` and `block-time-gaps.bin` by default during their
existing CAR pass. This includes the normal, first-seen, and PreHot builders.
Bounded `--max-blocks` runs are intentionally partial and publish neither file.
`build-blockhash-registry` also creates or repairs the gap sidecar whenever it
publishes or reuses a complete V3 index.
Deferred first-seen scans written by the new builder persist this requirement;
pre-upgrade scan candidates remain finalizable as an explicit legacy exception.

## Archive format

An Archive V2 directory contains compressed blocks plus indexes, registries,
metadata, and optional serving sidecars. The implemented layout is documented
in the [Archive V2 format reference](../docs/reference/archive-v2-hot-block-format.md)
and shared types live in [`blockzilla-format`](../crates/blockzilla-format/README.md).

Use a fresh output directory, keep source CAR files immutable, and budget ample
space for full epochs. See the [roadmap](../ROADMAP.md) for planned product work
and the [security policy](../SECURITY.md) for private reporting.
