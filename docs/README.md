# Blockzilla documentation

Start with the repository [fixture quick start](../README.md#quick-start-build-and-read-the-fixture).
It is the shortest working path through the project.

Current guides: [Blockzilla CLI](../blockzilla/README.md),
[supporting services](../services/README.md), the
[token API example](../examples/token-api/README.md), and
[developer scripts](../scripts/README.md).

## Implemented reference

- [Archive V2 hot-block format](reference/archive-v2-hot-block-format.md)
  documents the files and records implemented by `blockzilla-format` and the
  Blockzilla builders.

Archive V2 is pre-1.0. Pin the Git revision used to produce and read an archive.

## Proposed architecture

- [Full system schema](architecture/full-system-schema.md): concise target flow
  from network input to storage, edge serving, and local indexers.
- [System overview](architecture/system-overview.md): product ownership and the
  proposed end state.
- [Local sync and indexing](architecture/local-streaming.md): proposed local
  indexer seeding and streaming; its commands are not implemented yet.

## Research and history

- [Horizon problem statement](design/horizon-problem-statement.md)
- [Live-ingest redundancy](design/live-ingest-redundancy.md)
- [Archive V2 evolution](design/archive-v2-evolution.md)
- [Log compression](design/log-compression.md)
- [Earlier live-producer design](guides/live-archive-producer.md)
- [Archive V2 benchmark snapshot](benchmarks/archive-v2-storage-read-getblock-2026-05-24.md)

These documents preserve ideas and measurements; they are not all implemented
or current. Machine-specific runbooks, credentials, incidents, raw benchmark
output, and production deployment configuration do not belong here. Use the
repository [security policy](../SECURITY.md) for private reporting.
