# Blockzilla query SDK guide

Status: implementation guide, 2026-08-28. The source-neutral contract, the
three reference adapters, the instruction-only classic Token processor, the
restart-safe SQLite sink, and the bounded network command are implemented.
Run-specific network results are not part of this guide.

This guide defines the intended application experience. It does not present a
format adapter as ready when it is not ready.

## Goal

An application should need these steps:

1. Open one archive source.
2. Check its identity and verification level.
3. Select a full epoch or a bounded block range.
4. Give the reader one callback or sink.
5. Read the scan receipt and coverage counts.

The application should not need to manage:

- HTTP ranges;
- CAR nodes;
- Compact V2 registry IDs;
- Indexer V3 plane locators;
- message-version joins;
- loaded-address resolution;
- CPI metadata joins;
- decoder scratch buffers.

The format adapter owns this work.

## What exists now

The [`blockzilla-query-sdk`](../../crates/blockzilla-query-sdk/README.md)
contains the source-neutral models and sink direction.

The format-specific readers also exist:

- [`of-car-reader`](../../crates/old-faithful/car-reader/Readme.md) for CAR;
- [`blockzilla-read-sdk`](../../crates/blockzilla-read-sdk/README.md) for
  Compact V2;
- the current local and HTTPS Indexer V3 readers, described in the
  [Indexer V3 section of the product guide](../reference/archive-formats-and-read-sdk.md#indexer-first-standalone-v3).

The three adapters are implemented. The network example at
[`examples/archive-token-events`](../../examples/archive-token-events/README.md)
selects the format only during source setup. All three paths then use the same
token scan driver and SQLite sink.

The network example records these exact SDK trust levels:

| Format | Trust level | Meaning |
|---|---|---|
| CAR | `operator-trusted` | The operator accepts the CAR object and canonical slot plan. The adapter binds the URL, exact length, strong ETag, and slot-plan digest. |
| Compact V2 | `published-manifest` | A completed generation manifest binds the published generation. |
| Indexer V3 | `internal-binding-only` | Internal object bindings and an explicit candidate binding are present, but there is no publication manifest. |

The V3 path explicitly accepts its weaker trust level. The output keeps that
level. Output parity does not change it.

Selecting CAR through the facade is the application's explicit operator-trust
decision. CAR remains `operator-trusted`; its strong ETag and slot-plan digest
do not make it publication-verified.

## Common read model

All adapters must produce the same model from
[Archive Instruction Stream V1](../reference/archive-instruction-stream-v1.md).

An application receives:

- source identity and verification strength;
- block identity, including epoch, block ordinal, and slot;
- transactions in canonical transaction-index order;
- exact instruction, execution, and CPI coverage states;
- an optional primary signature;
- required signer public keys in message order;
- resolved outer and inner instructions;
- exact program keys, ordered account keys, instruction-data coverage, and
  coordinates;
- a scan receipt with source work and coverage counts.

The normal callback uses borrowed views. The reader can reuse its decode
buffers after the callback returns. An application must copy data that it
keeps.

## Public API

`ArchiveInstructionSource` is the format-neutral source trait. Its
`scan_ordered` method accepts a `ScanRequest` and a mutable `BlockSink`.

`BlockSink` receives one `BlockView` for each source block, including an empty
block. `BlockView::transaction_views` returns ordered `TransactionView`
values.

`ArchiveInstructionSourceExt` adds `for_each_block` and
`for_each_transaction`. These helpers also work with a runtime-selected
`dyn ArchiveInstructionSource`.

This example operates on any implemented adapter. For network sources,
`blockzilla-archive-sdk::NetworkEpoch` opens CAR, Compact V2, or Indexer V3 and
returns the same runtime `ArchiveSource` type.

```rust,no_run
use blockzilla_query_sdk::{
    ArchiveInstructionSource, ArchiveInstructionSourceExt, Result,
    ScanReceipt, ScanRequest,
};

fn inspect(
    source: &mut dyn ArchiveInstructionSource,
    request: &ScanRequest,
) -> Result<ScanReceipt> {
    let identity = source.identity();
    println!(
        "format={} epoch={} blocks={} verification={:?}",
        identity.format,
        identity.epoch,
        identity.block_count,
        identity.verification,
    );

    source.for_each_block(request, |block| {
        println!(
            "slot={} block={} transactions={}",
            block.header.slot,
            block.header.block_ordinal,
            block.transactions.len(),
        );

        for transaction in block.transaction_views() {
            println!(
                "tx={} signers={} instructions={}",
                transaction.header.tx_index,
                transaction.required_signers.len(),
                transaction.instructions.len(),
            );
        }
        Ok(())
    })
}
```

The caller prepares `request` before it calls this format-neutral function. A
request for Indexer V3 must explicitly call `allow_unverified_source`; the
function must not weaken source policy by itself.

For a small query that does not need empty-block checkpoints, use
`for_each_transaction`. The returned `ScanReceipt` still counts every scanned
block.

By default, a request requires exact data for every instruction. A query can
instead select bytes only for specified program IDs. The default still stops
if selected bytes are not exact. A durable audit tool can call
`allow_incomplete_instruction_data` to deliver that instruction with explicit
coverage instead. The USDC event tool uses this mode for the classic SPL Token
program. It saves a coverage issue when selected Token bytes are not exact. It
does not stop for an unrelated ambiguous vote instruction.

## Source selection

Source setup has one explicit choice at the edge of the application:

- CAR path or stream;
- Compact V2 generation directory or gateway URL;
- Indexer V3, also called the indexer-first candidate, by directory or HTTPS
  base URL.

After setup, the query logic receives a common source interface. It must not
match on the archive format.

The reference adapters are:

- `CarInstructionSource<R>` for a local or network `Read` stream;
- `CompactV2InstructionSource<S>` for a local or HTTP range source;
- `IndexerV3InstructionSource` for a local or shared range source.

`blockzilla-archive-sdk::NetworkEpoch` is the common network selector. It
admits the published Compact V2 epoch, creates the canonical block plan, and
opens one requested format as an `ArchiveSource`. A direct local tool can
construct one reference adapter instead. Application query logic does not
match on format-specific types after setup.

The `NetworkEpoch` facade supports `mainnet-beta` and derives the exact first
slot from Solana's warm-up-aware epoch schedule. `NetworkEpochOptions` changes
only the local-fixture transport policy. A custom cluster must construct the
direct format adapters with an explicit first slot. The facade also checks that
Indexer V3 block rows are a dense prefix of the Compact V2 canonical slot plan.
A requested range must fit in that prefix.

## Verification policy

Keep the default fail-closed.

A published Compact V2 generation can use its manifest and generation digest.
An explicit unpublished Compact V2 fixture can instead be `operator-trusted`.
A CAR can be an explicitly operator-trusted source, whether it is local or
remote. Current Indexer V3 has strict internal bindings, but it has no
publication manifest. It must keep the
internal-only status.

A caller can explicitly accept a weaker source for a benchmark or migration
test. The scan receipt and output metadata must keep that choice.

Do not label semantic parity as publication verification. See
[Archive reader formats](../reference/archive-reader-formats.md).

## Full and bounded scans

A full request scans all blocks in the selected epoch source.

A bounded request has a first block ordinal and a nonzero block count. The
adapter must not publish a block outside that logical range. A sequential
adapter such as CAR can read earlier source rows to reach the first requested
block. A physical range request can also include format framing bytes. The
receipt must state physical source work separately.

For a deterministic application database, commit only at a completed block
boundary. Store the next block ordinal with the application rows in one sink
transaction.

Empty blocks must reach the sink boundary. Otherwise, an application cannot
make an exact block-universe comparison or checkpoint after an empty final
block.

## Query sink

Use a sink when the query has state, batching, or restart rules. Use a closure
for a small stateless scan.

A sink can:

- count programs;
- select Pump.fun events;
- build a wallet-to-program index;
- build the instruction-only USDC event ledger;
- write SQLite or another database;
- make a deterministic parity digest.

The public `BlockSink` receives one complete block at a time. This keeps empty
blocks, gives one natural checkpoint boundary, and reduces callback overhead.
The `BlockView` contains ordered transactions. Each `TransactionView` contains
ordered resolved instructions.

The SQLite USDC writer is one application sink. It must not become a required
part of the archive reader.

## Error and coverage handling

Archive errors and sink errors are different.

An archive error means that the source or adapter cannot produce the required
canonical record. A sink error means that the application could not process a
valid record.

Do not turn these states into no-match results:

- missing metadata needed for CPI;
- raw transaction or metadata fallback;
- invalid account reference;
- unsupported target instruction;
- unverified source when verification is required;
- non-contiguous history when continuous state is required.

The caller can select a less strict policy, but the receipt must count the
accepted coverage gaps.

## USDC event example

The USDC processor uses the common instruction stream. It does not use
pre-token or post-token balance observations. The result is an
instruction-event ledger. It is not an observed balance ledger. The
three-format command writes one isolated database and report folder for CAR,
Compact V2, and Indexer V3. It then compares their canonical ledger rows.

It performs these steps for each block:

1. Visit transactions in canonical order.
2. Visit each outer instruction and its CPI list in canonical order.
3. Decode classic SPL Token instructions.
4. Update temporary or applied token-account lifetime state.
5. Publish target events and exact delta legs.
6. Commit the event batch and next block checkpoint together.

The processor can send the same event batch to SQLite, another database, or a
test digest. See
[USDC token event ledger V1](../reference/usdc-token-event-ledger-v1.md).

Run the public network example with one command:

```bash
cargo run --locked -p blockzilla-archive-token-events -- \
  network \
  --origin https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev \
  --epoch 0 \
  --max-blocks 1024 \
  --output-root /private/tmp/blockzilla-token-events-e0
```

The output root must be an absolute private path. The command accepts exactly
these sample epochs: `0`, `100`, `200`, `300`, `400`, `500`, `600`, `700`,
`800`, `900`, and `1000`. It has a hard limit of 1,024 canonical block rows
for one run. This limit belongs to the demo, not to the SDK.

The command keeps each archive in its own folder:

```text
<output-root>/archive-cache/origin-.../compact-v2/
<output-root>/archive-cache/origin-.../indexer-v3/
<output-root>/car/epoch-N/
<output-root>/compact-v2/epoch-N/
<output-root>/indexer-v3/epoch-N/
<output-root>/comparison/epoch-N/
```

Each format result folder has its own SQLite database and report. Compact V2
and Indexer V3 have separate cache trees under `archive-cache`. The comparison
command audits each database in read-only mode. It then merge-compares full
token-event, coverage, tracker, and ledger-control rows. It resolves
database-local key IDs to the raw 32-byte public keys before it compares the
rows.

The database keeps one SHA-256 digest for each complete canonical
`BlockView`. The comparison checks these digests. It does not keep a second
full source projection. Thus, it reports full-row source-projection parity as
`not-proved-full-row`.

Epoch 0 is only a structural network example. The current epoch-0 Compact V2
and Indexer V3 samples have limited metadata, and USDC is absent from this
range. Do not use an empty epoch-0 result as a throughput result or a
semantic-completeness result.

## Network receipts

Do not use one byte count for all network work. A format adapter report should
separate:

- identity HEAD requests;
- range GET requests;
- network response-body bytes;
- local cache bytes;
- compressed stored bytes;
- decoded bytes delivered to the application;
- cold and warm cache state.

`ScanReceipt` keeps cross-format work and coverage totals. Its optional I/O
receipt keeps source bytes, decoded bytes, and persistent-cache reads. A
format-specific report can add exact HEAD, GET, cold-cache, and warm-cache
details.

`ArchiveSource::io_snapshot` is a point-in-time value. After the scan, consume
the source with `ArchiveSource::finish_io` so background CAR workers stop before
the final total is recorded. Indexer V3 caches its bounded block index and
required registry. Its large transaction directory, optional signatures, and
semantic payload planes stay as pinned, uncached range reads and remain in the
network counters.

## Ease-of-use target

Jetstreamer gives a simple sequential network stream. The Blockzilla API must
have a similarly small normal path: one source, one range, and one callback.

Blockzilla also needs explicit archive properties that a live-style stream
does not give by itself:

- immutable source identity;
- bounded historical range;
- exact instruction coordinates;
- CPI and decode coverage;
- deterministic restart position;
- format-independent parity.

These checks must not force each application to understand the archive
format. They belong in the adapter and common publisher.

## Validation status

The format adapters have corruption, coverage, ordering, range, and sink-stop
fixtures. The facade also tests the warm-up-aware first slot and the V3 dense
slot-plan prefix. The CAR HTTPS stream has local protocol-failure fixtures. The
SQLite writer has restart, rollback, path, schema, digest, and tracker-state
fixtures.

The public network example has a hard 1,024-block limit. It is a
correctness and SDK demonstration. It is not a full-epoch throughput tool.
The report separates setup, scan, request, byte, cache, coverage, and database
facts. A later benchmark can make a performance claim only after the three
formats produce the same application rows for the same range.
