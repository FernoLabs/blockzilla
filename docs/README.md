# Blockzilla documentation

The documentation is organized around the public Archive V2 path.

## Reference

- [Archive V2 hot-block format](reference/archive-v2-hot-block-format.md) defines the indexed block representation and its sidecars.

## Design

- [Horizon problem statement](design/horizon-problem-statement.md) describes the historical-verification problem Blockzilla addresses.
- [Redundant live ingest](design/live-ingest-redundancy.md) records the durability and replication invariants for live capture.
- [Log compression](design/log-compression.md) explains the compact runtime-log representation.

## Guides

- [Live archive producer](guides/live-archive-producer.md) introduces capture sources, repair sources, and epoch production.

## Benchmarks

- [Archive V2 storage and read snapshot](benchmarks/archive-v2-storage-read-getblock-2026-05-24.md) is the current curated benchmark report. Treat results as hardware- and corpus-specific.

Machine-specific runbooks, incident reports, and raw benchmark output are not part of this reference documentation.
