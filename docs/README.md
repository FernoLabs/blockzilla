# Blockzilla documentation

This directory separates implemented interfaces from proposed architecture and
historical design work. For a first successful run, start with the repository
[fixture quick start](../README.md#quick-start-build-and-read-the-fixture), not
the target-system documents.

## Product documentation

- [Blockzilla CLI](../blockzilla/README.md): the working CAR-to-Archive V2 path,
  command groups, code map, and validation.
- [Hivezilla](../hivezilla/README.md): the live-input prototype and its current
  safety boundary.
- [Edgezilla](../edgezilla/README.md): the experimental cloud-replica and
  read-only serving boundary.
- [Token API example](../examples/token-api/README.md): an experimental derived
  index and local HTTP API.
- [Developer scripts](../scripts/README.md): benchmark and correctness utilities
  with network and credential requirements.

## Architecture proposals

- [Full system schema](architecture/full-system-schema.md) is the concise target
  flow across network inputs, Hivezilla, Blockzilla, cloud storage, Edgezilla,
  and local indexers.
- [System overview](architecture/system-overview.md) explains product ownership,
  current repository boundaries, and the proposed end state.
- [Local sync and indexing](architecture/local-streaming.md) proposes how a local
  indexer could seed, checkpoint, and follow compacted archives. Its `sync` and
  `stream` commands are not implemented yet.

Architecture documents describe direction unless they explicitly identify an
implemented command or format.

## Implemented reference

- [Archive V2 hot-block format](reference/archive-v2-hot-block-format.md)
  documents the files and records implemented by `blockzilla-format` and the
  Blockzilla builders.

Pin a Git revision when implementing a reader: Archive V2 is still pre-1.0.

## Design and historical context

- [Horizon problem statement](design/horizon-problem-statement.md) describes the
  historical-verification problem Blockzilla addresses.
- [Live-ingest redundancy](design/live-ingest-redundancy.md) records durability,
  deduplication, and receipt invariants. It includes work not yet wired into a
  production Hivezilla service.
- [Archive V2 evolution](design/archive-v2-evolution.md) collects non-normative
  sidecar, lookup-index, metadata, and publication-manifest ideas.
- [Log compression](design/log-compression.md) is a research question about
  generalized, byte-preserving runtime-log templates.
- [Live archive producer guide](guides/live-archive-producer.md) preserves the
  earlier prototype design. Use the product READMEs for current ownership and
  status.

## Benchmarks

- [Archive V2 storage and read snapshot](benchmarks/archive-v2-storage-read-getblock-2026-05-24.md)
  is a curated historical measurement. Results are specific to the stated
  corpus, commit, build profile, and hardware; they are not a service-level
  guarantee.

Machine-specific runbooks, provider credentials, incident reports, raw
benchmark output, and production deployment configuration do not belong in this
public reference documentation. See the repository [security policy](../SECURITY.md)
for private vulnerability reporting.
