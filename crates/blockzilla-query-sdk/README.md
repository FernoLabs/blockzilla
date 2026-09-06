# blockzilla-query-sdk

Start with the product guide,
[`Archive formats and the read SDK`](../../docs/reference/archive-formats-and-read-sdk.md).
It shows format choice, the implemented `NetworkEpoch` facade, the common scan
API, source trust, and reader file layouts.

`blockzilla-query-sdk` defines the common, ordered block and transaction stream used by
CAR, Compact V2, and Indexer V3 readers.

The crate contains no archive decoder. Each format adapter must validate its
source and convert it to the canonical types in this crate. Applications can
then use one callback API without importing format-specific transaction types.

The block callback includes empty block rows. This gives applications a safe
checkpoint boundary. A short transaction callback is also available for simple
queries.

The first application is the implemented instruction-derived classic USDC
event processor. It does not use pre-token or post-token balance observations.
A restart-safe SQLite store is available in `blockzilla-dump`. The CAR,
Compact V2, and Indexer V3 adapters are implemented. The bounded three-format
network command uses the same token scan driver and SQLite sink for all three
formats.

See:

- [`Archive formats and the read SDK`](../../docs/reference/archive-formats-and-read-sdk.md)
- [`Archive Instruction Stream V1`](../../docs/reference/archive-instruction-stream-v1.md)
- [`Archive reader formats`](../../docs/reference/archive-reader-formats.md)
- [`USDC token event ledger V1`](../../docs/reference/usdc-token-event-ledger-v1.md)
