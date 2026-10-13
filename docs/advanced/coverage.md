# Data and coverage

Blockzilla never reports missing data as empty data. This page explains how a
gap looks in the API, and what a successful read proves.

## Data states

| State | Meaning |
| --- | --- |
| Present | The archive has the fact. |
| Known empty | The archive proves there is nothing, for example no inner calls. |
| Not recorded | The original source did not record this field. |
| Unknown | The reader cannot establish the fact. |
| Not requested | Your request left this field out. |
| Backfilled | The fact came from a later repair, not from the original source. |

An empty instruction list in a request without instructions is "not
requested". It does not mean the transaction had no instructions.

## Execution status

`ExecutionStatus` is `Succeeded`, `Failed`, or `Unknown(reason)`.

- A failed transaction is part of the ledger. Its instructions did not change
  state. Do not apply them as successful changes.
- `Unknown` is not success. Keep it in your output, or let the scan stop.

By default a scan stops when it meets data that the request requires but the
archive does not have. The `allow_*` options on `ScanRequest` accept a gap
explicitly. The scan receipt then counts the affected transactions. Save that
choice with your output.

## What a successful read proves

| Result | Proves | Does not prove |
| --- | --- | --- |
| A bounded read succeeds | Those blocks were read and decoded. | That the epoch is complete. |
| A full-epoch scan succeeds | Every block in the archive file was read. | That the archive has every block Solana produced. |

Checking that an epoch has every produced block and every skipped slot is a
separate step. Keep these facts separate: produced blocks, skipped slots,
missing ranges, and the source of each fact.

## Live data

The live stream comes from Blockzilla's own validator capture. Its blocks carry
`canonical=false`: a capture gap is possible and is not yet repaired
automatically. Treat live data as a development source until this flag changes.

## Source identity

Every archive has an identity, and every source has a verification level:

| Level | Meaning |
| --- | --- |
| `ObjectSetBound` | File names, URLs, lengths, and strong ETags bind the source. The public archive uses this level. |
| `OperatorTrusted` | An operator accepts the local files. `--archive-dir` reads use this level. |
| `InternalBindingOnly` | Only internal links bind the files. |
| `Unverified` | No verification. |

Default requests accept the first two levels. A weaker source needs an explicit
request option.

A local read checks the file set and its structure, but it does not hash the
whole archive. Its `--source-id` is a label, not a content hash.

## Restart positions

Save these values with your output:

- the source identity and its verification level,
- the next block number,
- the coverage issues of the range you committed.

Commit the output and the next block number together, at a block boundary. On
restart, reject a changed source identity.

Each tool has its own position format. They are not interchangeable:

| Tool | Position |
| --- | --- |
| Rust reader | Your saved block number |
| Live SDK | Chunk cursor, saved after `batch.ack()` |
| Token index | Cursor stored in its SQLite database |
| Program dumper | Block checkpoint stored in its SQLite database |

## Exact formats

Use the format crate of the same Git revision to decode. Choose one schema per
source when you admit it. Do not guess a schema for each transaction.

See the [common SDK guide](../guides/blockzilla-query-sdk.md) and the
[reader behavior reference](../reference/archive-reader-formats.md) for the full
rules.
