# Archive Instruction Stream V1

Status: implemented common contract, 2026-08-28. The contract code, its unit
tests, and the CAR, Compact V2, and Indexer V3 reference adapters exist.

Archive Instruction Stream V1 is the source-neutral boundary between archive
readers and application logic. CAR, Compact V2, and Indexer V3 must produce
the same ordered records.

The contract is in
[`blockzilla-model`](../../crates/blockzilla-model/README.md). The
crate contains the common models, request gates, order validation, source
identity, scan receipts, and sink boundary. It does not contain an archive
decoder.

## Purpose

An application must be able to write one instruction query. It must not need
to know:

- how CAR nodes form a block;
- how Compact V2 registry IDs resolve;
- how Indexer V3 planes and directories join;
- which HTTP ranges a reader uses;
- which Wincode or protobuf type a source stores.

The format adapter owns those tasks. It validates its source, resolves all
instruction account references, and publishes canonical records.

## Non-goals

The stream does not interpret SPL Token, Pump.fun, or another application
protocol.

It does not create a SQLite database. SQLite is one possible application sink.

It does not make an unverified source verified. Source identity and coverage
stay part of the result.

It does not make a sparse epoch set continuous.

## Source identity

Each source has one identity before a scan starts. The identity contains:

- archive format: CAR, Compact V2, or Indexer V3;
- a user-visible source label;
- an optional cluster identity;
- epoch;
- the exact first slot in the epoch;
- slots per epoch;
- exact block-row count;
- verification strength;
- a stable binding when one is available.

The first slot is explicit. An adapter must not calculate it as
`epoch * slots_per_epoch`, because a cluster can use a warm-up epoch schedule.

The verification strengths distinguish a published manifest, an
operator-trusted source, an internal binding, and an unverified source.

The default request accepts a published source or an explicitly trusted
input. The input can be local or remote. It rejects an internal-only or
unverified source. A caller must
make an explicit request to use a weaker source. See
[Archive reader formats](archive-reader-formats.md) for the current format
limits.

## Scan request

A scan can cover the full source or one contiguous block range. A bounded
range uses a first block ordinal and a nonzero block count.

The request has fail-closed gates for:

- source verification;
- complete outer instructions and resolved accounts;
- complete CPI coverage;
- known transaction execution status.

The request also selects the instruction programs that require exact data
bytes. The choices are all programs, a specific program list, or no program
data. Program and account identities stay available in all three modes.

An adapter must check these gates before it publishes application data.

## Canonical block and transaction order

An adapter publishes blocks in increasing block ordinal. It publishes
transactions in canonical `tx_index` order inside each block.

Storage order is not a substitute for canonical transaction order. If a
format stores rows in another order, its adapter must reorder them with a
strict memory bound.

Each block header contains:

- epoch;
- block ordinal;
- slot;

Each transaction header contains:

- canonical transaction index;
- execution status;
- the failed outer-instruction index when the source records an exact failed
  boundary;
- outer-instruction coverage;
- CPI coverage.

The primary signature is optional. A source that has no signature sidecar can
still publish the instruction stream, but it must not invent a signature.

Each transaction also contains the required signer public keys in message
order. This supports a wallet-to-program query without a second message
decoder.

Empty blocks are canonical records. An adapter must publish them. This gives a
sink an exact block universe and a checkpoint after an empty final block.

## Canonical instruction order

For each transaction, publish every outer instruction in message order. Place
the recorded inner instruction list for an outer instruction immediately
after that outer instruction.

The order is:

1. outer instruction 0;
2. its inner instructions 0 through N;
3. outer instruction 1;
4. its inner instructions 0 through N;
5. continue to the end of the message.

Each instruction coordinate contains:

- a zero-based position in the flattened canonical stream;
- the zero-based outer instruction index;
- an optional zero-based inner instruction index;
- an optional runtime stack height.

An outer instruction has no inner index and no stack height. An inner
instruction keeps the exact stack height when the source records it. A source
that does not record it uses `None`.

Inner groups must have unique outer indexes. Inner indexes in one group must
be contiguous and must keep source invocation order.

## Resolved instruction

Each canonical instruction contains:

- its coordinate;
- the 32-byte program public key;
- an ordered list of 32-byte account public keys;
- instruction-data coverage and, when exact, the instruction-data bytes.

The account list contains only the accounts of that instruction. It does not
include the program unless the source instruction also lists it as an
account.

The adapter must resolve static and loaded message accounts before it
publishes an instruction. An invalid account or program reference is not an
empty account. It is an error or an explicit coverage issue.

Instruction data is `exact`, `not-requested`, or `unknown` with a reason. A
non-exact instruction has no data bytes. This rule prevents an application
from using partial bytes as an exact instruction. It also lets a token query
require exact SPL Token bytes without reading signature sidecars only to
resolve an unrelated historical vote encoding.

Data selection and scan strictness are separate. A request first selects the
programs whose data the adapter must reconstruct. The strict default stops if
selected data is not exact. An audit sink can allow incomplete selected data.
In that mode, the adapter publishes `unknown` coverage and no bytes, and the
sink records the gap. It must not publish `not-requested` for a selected
instruction.

Outer-instruction coverage is either complete or unknown with a reason. An
empty instruction vector with complete coverage means that the transaction
has no outer instructions. It does not mean that instruction data is missing.

## Execution status

The transaction status has three states:

- `succeeded`;
- `failed`;
- `unknown`, with a coverage reason.

`succeeded` means that all transaction state changes committed. `failed`
means that no transaction state change committed.

The instruction stream can contain outer message instructions from a failed
transaction. Their presence in the message does not prove that runtime invoked
all of them. Inner instructions come from the recorded CPI data and are
observed invocations that later rolled back with the transaction.

When the source records an exact failed outer-instruction index, the adapter
publishes it. An outer instruction before that index was invoked and rolled
back. The instruction at the index has unknown invocation completion. An
outer instruction after the index was not invoked. An adapter must fail if
the boundary conflicts with the transaction status or recorded CPI facts.

An application that needs invocation-attempt facts must use the detailed
transaction error and CPI evidence. It must not treat every outer message
instruction in a failed transaction as invoked.

## CPI coverage

CPI coverage has these states:

- `complete`;
- `not-recorded`;
- `unknown`, with a coverage reason.

`not-recorded` is a source fact. It is not the same as a complete empty inner
instruction list.

The standard instruction query requires complete CPI coverage. An
application can explicitly accept incomplete coverage, but the scan receipt
must count affected transactions.

## Coverage reasons

The common model includes reasons for:

- absent metadata;
- raw transaction fallback;
- raw metadata fallback;
- invalid account or program references;
- ambiguous or unavailable instruction data;
- unsupported instructions;
- an unverified source;
- non-contiguous history;
- another explicit reason.

An adapter must not convert one of these conditions to a no-match result.

## Sink boundary

The public source trait is `ArchiveInstructionSource`. Its main method has this
boundary:

```rust,ignore
fn scan_ordered(
    &mut self,
    request: &ScanRequest,
    sink: &mut dyn BlockSink,
) -> Result<ScanReceipt>;
```

The sink implements `BlockSink`. Its callback receives one `BlockView` at a
time, including empty blocks. Each `BlockView` supplies ordered
`TransactionView` values.

`ArchiveInstructionSourceExt` supplies two short helpers:

- `for_each_block` for a closure over `BlockView`;
- `for_each_transaction` for a closure over `TransactionView`.

Use `for_each_block` or `BlockSink` when the application needs an exact block
checkpoint. The transaction helper still counts empty blocks in its
`ScanReceipt`, but it does not make a transaction callback for them.

The source owns temporary decode storage. A sink must copy data that it keeps
after a callback returns.

The sink can:

- filter instructions;
- build token events;
- write SQLite or another database;
- calculate a deterministic comparison digest;
- stop with an application error.

Archive errors and sink errors stay separate. An application error must not be
reported as a corrupt archive.

The public API must keep the normal use small. A caller should select one
source, define one request, and give one callback or sink. It should not need
to manage HTTP ranges, sidecar joins, registry IDs, or decoder scratch.

## Scan receipt

A successful scan returns exact counts for:

- blocks;
- transactions;
- instructions;
- transactions with incomplete outer-instruction coverage;
- transactions with incomplete CPI coverage;
- transactions with unknown execution status.

The receipt also counts instructions with incomplete data coverage.

The receipt also has optional I/O counters for source read calls, source read
bytes, decoded bytes, cache read calls, and cache read bytes. An adapter uses
`None` when it cannot measure a counter exactly.

An adapter must count logical source work consistently. A format-specific
report can add cold-cache, warm-cache, and physical HTTP details. It must not
change the common count meanings.

## Adapter rules

Each adapter must:

1. validate source identity before the first output record;
2. enforce the requested block range;
3. publish canonical transaction order;
4. publish empty block rows;
5. resolve every published public key;
6. publish required signer keys in message order;
7. keep exact instruction account order and data bytes;
8. preserve outer and inner coordinates;
9. preserve stack height when available;
10. publish exact instruction, execution, and CPI coverage;
11. use bounded allocation and input lengths;
12. return exact scan counts.

The adapter must fail if it cannot maintain these rules. It must not guess a
schema or silently omit an invalid record.

The common `OrderedBlockPublisher` applies the source identity, requested
range, block and slot order, dense transaction indexes, and coverage gates
before a `BlockSink` receives a block. Format adapters should use this checked
publisher instead of making separate order rules.

## Conformance tests

The CAR, Compact V2, and Indexer V3 adapters must pass the same fixture suite.
The suite must include:

- Legacy, V0, and supported V1 messages;
- loaded writable and readonly accounts;
- no inner instructions;
- more than one inner group;
- nested CPI stack heights;
- failed and unknown transaction status;
- raw and absent metadata;
- invalid account and program references;
- zero-instruction and high-instruction-count transactions;
- a bounded block range;
- a sink error during a scan.

For the same source facts, all adapters must produce the same canonical record
digest. A parity result does not replace each adapter's source-verification
check.
