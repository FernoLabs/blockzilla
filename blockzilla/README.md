# Blockzilla CLI

## Purpose

`blockzilla` is the main product in this repository. It turns Solana historical
inputs into Archive V2 directories and provides the commands used to inspect,
read, repair, and derive data from those archives.

## Status

The CAR/CAR.ZST to local Archive V2 path is usable today and is the public
starting point. The CLI is pre-1.0: advanced repair, analysis, and benchmark
commands remain subject to change.

Blockzilla does not yet run the target NAS scheduler, consume gRPC or shreds as
a standalone daemon, replicate an archive to cloud storage, or provide the
planned `sync` and `stream` commands.

## Supported newcomer path

| Task | Command |
| --- | --- |
| Verify a CAR can be streamed to clean EOF | `preflight-car` |
| Build the indexed hot-block Archive V2 representation | `build-archive-v2-hot-blocks` |
| Read and decode every indexed hot block | `bench-archive-v2-hot-blocks` |
| Build getBlock access data | `build-archive-v2-block-access` |
| Build the direct per-slot serving index | `build-archive-v2-get-block-index` |

The CLI also contains no-registry builders, repackers, live-capture finalizers,
repair materializers, analyzers, data dumps, and focused benchmarks. These are
maintainer and research surfaces rather than a stable public API. The current
command list and flags are authoritative:

```bash
cargo run --locked -p blockzilla -- --help
cargo run --locked -p blockzilla -- \
  build-archive-v2-hot-blocks --help
```

The repository-level [quick start](../README.md#quick-start-build-and-read-the-fixture)
builds and reads a one-block fixture without network access.

## Archive output

A hot-block build writes one directory containing:

- `archive-v2-blocks.zstd` and `archive-v2-blocks.index` for indexed block
  reads;
- `archive-v2-meta.wincode` for archive metadata;
- `registry.bin`, `registry_counts.bin`, and `registry.mphf` for pubkeys;
- blockhash, signature, vote-hash, PoH, and shredding sidecars; and
- optional block-access and getBlock indexes for serving.

The implemented constants and record types live in
[`crates/blockzilla-format`](../crates/blockzilla-format/README.md). See the
[Archive V2 format reference](../docs/reference/archive-v2-hot-block-format.md)
before writing another reader.

## Source map

| Path | Responsibility |
| --- | --- |
| `src/main.rs` | CLI definition and command dispatch. |
| `src/archive_v2.rs` | Archive V2 builders, readers, indexes, repackers, and benchmarks. |
| `src/archive_v2/repair.rs` | Repair-specific validation and materialization. |
| `src/car_preflight.rs` | Bounded-memory CAR structural preflight. |
| `src/first_seen_finalization.rs` | Deferred first-seen registry finalization. |
| `src/pre_hot.rs` | Typed pre-hot spool used by staged builds. |
| `src/token_events.rs` | Derived token instruction/event extraction. |
| `src/bin/` | Opt-in contributor diagnostics, benchmarks, and one-shot migrations. |

`blockzilla` is the only default executable. Contributor binaries are disabled
unless the caller explicitly enables `diagnostic-tools`, `benchmark-tools`, or
`migration-tools`; `developer-tools` enables all three. They are not supported
product commands. For example:

```bash
cargo run --locked -p blockzilla --features diagnostic-tools \
  --bin car-log-audit -- --help
```

## Validation

```bash
cargo fmt --all -- --check
cargo check --locked -p blockzilla --all-targets
cargo test --locked -p blockzilla --all-targets
```

## Safety and security

Use a fresh output directory. Builders create or replace named files inside the
destination, and full-epoch work can consume hundreds of gigabytes. Keep source
CAR files immutable, retain the producing Git revision with each archive, and
test destructive operational workflows on copies.

Do not place provider credentials, production paths, incident data, or archive
artifacts in this repository. Report vulnerabilities through the
[security policy](../SECURITY.md).
