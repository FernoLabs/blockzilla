# Blockzilla query SDK guide

Status: implementation guide, reviewed 2026-09-06. The shared model, three
format adapters, classic Token processor, and SQLite sink are implemented.
The old `NetworkEpoch` facade is removed. Its archive-token-events command is
parked and excluded from the workspace until it is ported to the dedicated
readers. Use the current reader examples linked below.

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

[`blockzilla-model`](../../crates/blockzilla-model/README.md) contains the
source-neutral models, source trait, scan requests, and sink interfaces.

The format-specific readers also exist:

- [`of-car-reader`](../../crates/old-faithful/of-car-reader/Readme.md) for CAR;
- [`blockzilla-compact-v2-reader`](../../crates/compact-v2/blockzilla-compact-v2-reader/README.md) for
  Compact V2;
- [`blockzilla-archive-v3-reader`](../../crates/archive-v3/blockzilla-archive-v3-reader/README.md),
  whose `IndexerV3Archive` adapter reads the frozen standalone prototype.

The small [CAR](../../examples/read-car/README.md),
[V2](../../examples/read-compact-v2/README.md), and
[prototype V3](../../examples/read-archive-v3/README.md) examples use the same
workload rules. Canonical Archive V3 is the replacement format under migration.
Its local `CanonicalReader` returns format-native data; common-model and HTTP
support are still pending. See the [reader entry points](../reference/archive-formats-and-read-sdk.md).

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

This example operates on any implemented `ArchiveInstructionSource` adapter.
Open the concrete reader before passing it to this function.

```rust,no_run
use blockzilla_model::{
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

The caller prepares `request` before it calls this format-neutral function.
Only a source admitted as `InternalBindingOnly` or `Unverified` needs an
explicit `allow_unverified_source` request. Inspect the actual source identity;
do not infer its verification level from the format name.

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
- Compact V2 epoch directory or HTTPS origin;
- the frozen Indexer V3 prototype by directory or HTTPS origin.

After setup, the query logic receives a common source interface. It must not
match on the archive format.

The high-level entry points are:

- `of_car_reader::archive::CarArchive`, with the `archive` feature;
- `blockzilla_compact_v2_reader::CompactV2Archive`, with the `http` feature
  for this high-level API, including its local constructor;
- `blockzilla_archive_v3_reader::IndexerV3Archive` for the prototype.

The lower-level `CarInstructionSource`, `CompactV2InstructionSource`, and
`IndexerV3InstructionSource` remain available for callers that already own
the required source and descriptor. The reader guides define their admission
rules. Sample paths use fixed 432,000-slot archive windows. Direct adapters
carry explicit first-slot and block-plan geometry; this is not a general
warm-up-aware Solana epoch selector.

The local CAR reader selects a completed `.car` first, then `.car.zst` if raw
CAR is absent. It ignores `.partial` files. A compressed CAR is decoded
sequentially; index offsets refer to decoded CAR bytes.

## Verification policy

Keep the default fail-closed.

The common model has four levels:

| Level | Meaning |
|---|---|
| `ObjectSetBound` | The admitted object set is pinned to its source identities. Network readers use exact object names, URLs, lengths, and strong ETags. |
| `OperatorTrusted` | The caller explicitly accepts the descriptor and its object bindings. This is used for local archive opening and the Old Faithful transport policy. |
| `InternalBindingOnly` | Internal cross-file bindings exist, but the source lacks stronger admission evidence. |
| `Unverified` | No accepted source verification is asserted. |

Default requests accept the first two levels. The high-level sample readers
do not require a publication manifest or a whole-archive hash. The lower-level
V2 published-generation API separately supports manifest and content checks.
Keep the admission rules of the API actually used.

`SourceIdentity.binding` can be a descriptive operator or candidate ID. Some
local or object-set descriptors synthesize a `GenerationBinding` digest; it
must not be presented as a verified registry-content hash. Scope registry IDs
to the pinned source and registry, and record the verification strength.

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

## Token events and balance outputs

The USDC processor uses the common instruction stream. It does not use
pre-token or post-token balance observations. The result is an
instruction-event ledger. It is not an observed balance ledger. The processor
and SQLite writer live in [`blockzilla-dump`](../../indexer/blockzilla-dump/README.md).

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

The old three-format [archive-token-events example](../../examples/archive-token-events/README.md)
is parked. Its [port plan](../../examples/archive-token-events/PORT-REQUIRED.md)
records the work needed to restore the command. Its historical command lines
are not current workspace entry points.

The supported `read-*-usdc` examples instead write recorded pre/post token
balance observations through the shared `UsdcBalanceSink`. They retain the
`BZUSDC02` binary format. The optional V2 `read-compact-v2-usdc-indexed`
command writes compact numeric references plus one source-scoped public-key
dictionary. It resolves a registry reference only at first discovery; the
actual token account, local account index, and owner remain separate fields.
This mode uses `IndexedTokenSink` rather than allocating a resolved canonical
balance for every row.

The checked expander recreates `BZUSDC02` from the indexed data, dictionary,
source sidecar, and completion sidecar. It checks scope, lengths, counts, and
streaming output-file hashes. It does not authenticate the selected source
metadata against the original registry. See the
[indexed output contract](../reference/usdc-indexed-balances-v1.md).

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

The concrete archive readers' `io_snapshot` methods return point-in-time values.
After the scan, consume the source with `finish_io` so background CAR workers
stop before the final total is recorded. The prototype V3 reader caches its bounded block index and
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
fixtures. CAR HTTPS has local protocol-failure fixtures. The SQLite writer has
restart, rollback, path, schema, digest, and tracker-state fixtures. The V2
[rolling pipeline](../design/reader-pipeline-rolling-window.md) additionally
tests bounded admission, incremental ordered publication, and thread shutdown
after source, worker, or sink failure.

The [epoch 300 rolling-pipeline report](../benchmarks/epoch-300-rolling-pipeline-2026-09-06.md)
records the latest paired V2 NAS comparison. It checks exact USDC, expanded
indexed-USDC, and Pump.fun output bytes outside timed scans. These results
apply to the named input and builds; they do not establish canonical Archive
V3 compatibility or performance on all epochs.
