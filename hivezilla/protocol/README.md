# hivezilla-protocol

Dependency-light canonical primitives shared by Hivezilla V1 custody producers
and consumers.

The crate freezes the persisted value layer for:

- stream identities, manifests, lineage, centralized-registry snapshots, and
  exact registry heads;
- `RecordV1` prefix chains, gap evidence, sealed journal segments, and
  node-private overflow metadata and keys;
- HiveSync cutover/range/chunk semantics, cumulative ACKs, accepted-ACK receipt
  chains, and source-retirement checkpoints; and
- self-describing terminal raw objects, physical-copy receipts, canonical
  range indexes, durability-policy bindings, and protected-prefix checkpoints.

It deliberately contains no filesystem or database engine, provider client,
network server, membership protocol, source adapter, scheduler, processor, or
compactor. Provider attestation/readback and durable transaction ordering remain
trusted-runtime responsibilities; constructing a value alone never authorizes
an ACK or deletion. Cursor-validation hooks likewise do not replace the source
journal/index lookup required before ACK acceptance or garbage collection.
Generated HiveSync and public-exit protobuf bindings live in
`hivezilla/service`, while compaction/catalog values live in
`blockzilla-compaction-protocol`.

The normative wire contract is
[`docs/design/hivezilla-record-and-sync-protocol.md`](../../docs/design/hivezilla-record-and-sync-protocol.md).
