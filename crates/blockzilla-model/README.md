# blockzilla-model

Start with the product guide,
[`Archive formats and dedicated readers`](../../docs/reference/archive-formats-and-read-sdk.md).
It shows format choice, the current reader entry points, the common scan API,
source checks, and reader file layouts.

`blockzilla-model` defines the common, ordered block and transaction stream used by
CAR, Compact V2, and the frozen standalone Indexer V3 reader.

The new canonical Archive V3 reader does not yet implement this stream API.
Its current local entry point is `blockzilla_archive_v3_convert::canonical_reader::CanonicalReader`.

The crate contains no archive decoder. Each format adapter must validate its
source and convert it to the canonical types in this crate. Applications can
then use one callback API without importing format-specific transaction types.

The block callback includes empty block rows. This gives applications a safe
checkpoint boundary. A short transaction callback is also available for simple
queries.

The first application is the implemented instruction-derived classic USDC
event processor. It does not use pre-token or post-token balance observations.
A restart-safe SQLite store is available in `blockzilla-dump`. The CAR,
Compact V2, and standalone Indexer V3 adapters are implemented. Each reader provides the
common types and scan interface. The old `archive-token-events` network command
is excluded from the workspace until its source setup is migrated to these
readers. See its [porting note](../../examples/archive-token-events/PORT-REQUIRED.md).

See:

- [`Archive formats and dedicated readers`](../../docs/reference/archive-formats-and-read-sdk.md)
- [`Archive Instruction Stream V1`](../../docs/reference/archive-instruction-stream-v1.md)
- [`Archive reader formats`](../../docs/reference/archive-reader-formats.md)
- [`USDC token event ledger V1`](../../docs/reference/usdc-token-event-ledger-v1.md)
