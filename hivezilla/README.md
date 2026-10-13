# Hivezilla

[The service](service/README.md) captures live input and retains recoverable data.
`protocol/` defines its messages. `object-store/` provides storage support.
`ledger-compat/` and `gossip-compat/` contain the Agave compatibility code used
by Hivezilla. Shared Solana codecs live in `../crates/solana-codec/`.

The [direct Agave engine](direct-agave/README.md) is the current capture and
compact path on the Ubuntu validator host. It receives native execution, entry,
metadata, and rooted-bank callbacks, commits a local source journal, and builds
Compact V2 without gRPC. A separate receive hook records raw shreds. See the
[production service notes](deploy/direct-agave/README.md) for paths, storage
limits, health checks, and recovery. R2 upload and canonical shred reconciliation
are not connected to this deployment.

The earlier [live relaunch review](../docs/architecture/hivezilla-live-relaunch.md)
defines the first release with one Hetzner reader, one NAS consumer, gRPC and
shred input, and R2 recovery. Blockzilla owns reconciliation and compaction.
Other projects share the live feed and use Blockzilla for finalized block backfill.
The design preserves independent reader identities for the future hive.
It separates the existing code from the work required before launch.
