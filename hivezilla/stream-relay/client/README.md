# Simple full-feed SDK

This first SDK receives the complete compact feed and filters transactions
locally. It runs the same Rust decoder in Node.js and browsers through
`decoder.wasm`. There are no JavaScript package dependencies or generated
Wasm bindings. This package is private and has not been published to npm.

The separate `./consensus` entry point reads immutable retained Alpenglow footer
facts with `decoder-consensus-v1.wasm`. It preserves leader nanoseconds and
certificate target slots as decimal strings. It checks the complete fact digest
and caller-supplied source/bank/execution binding. Certificates remain
`captured_not_verified`; this API does not set a bank's finality state. Read
the [format and delivery plan](../../../docs/architecture/alpenglow-consensus-facts.md).

Start with [Live stream](../../../docs/start/live-stream.md) for connection and
transport examples, or [Program dumper](../../../docs/start/program-dumper.md)
for a program watch list. The separate [token indexer](../../../docs/start/token-indexer.md)
provides a SQLite writer for finalized history. It rejects the processed live
feed until it can undo invalidated banks.

The current live source is the native, operator-trusted processed projection.
`canonical=false` and `publicationReady=false` remain visible. The decoder
does not verify Solana signatures, reconstruct exact signed wire bytes, expand
all compact logs into RPC strings, or certify a complete epoch.

```js
import { createClient } from './sdk.mjs';

const client = createClient({ url: STREAM_URL, apiKey: API_KEY });
for await (const batch of client.stream({
  programs: [YOUR_PROGRAM_ADDRESS],
  after: savedCursor,
  includeDetails: true,
})) {
  // Your application must store candidate banks, select roots, and undo
  // invalidations. Commit the application changes and cursor together.
  await applyBankEventAndSaveCursor(batch.session, batch.event, batch.blocks, batch.cursor);
  await batch.ack();
}
```

`programs` matches top-level **instruction references** or recorded inner/CPI
program calls. A top-level instruction reference does not prove that execution
reached that instruction. `accounts` matches static and resolved loaded message
accounts. Program and account watch lists combine with OR. The empty watch lists
select every transaction. Keys are addresses, not cached epoch integer IDs.
The decoder resolves each chunk's dictionary, so changing IDs cannot change the
watched address. `successfulOnly` is false by default; true keeps only known
successful execution results.

Small transaction records are returned by default: first signature (transaction
ID), all signatures, success, and program references. `success=null` means the
execution metadata is unavailable. `innerCoverage` distinguishes recorded inner
metadata from unavailable inner metadata. `includeDetails=true` also returns
resolved `accountKeys`, the compact `message`, and compact `metadata`. These
objects retain the Rust compact schemas; they are not Solana RPC JSON. Integers
outside JavaScript's exact range are decimal strings. Resolve nested compact
pubkeys with `batch.resolvePubkey(reference)`. Its dictionary scope is the chunk
digest, not a persistent epoch-wide registry.

Filtering and small output records do **not** reduce network traffic in this
version. Every block is received. Expansion is limited to selected transactions.
One Wasm instance keeps reusable allocator memory; a high-water mark after a large
request is not returned to the host. Keep few batches alive in application code.
Each block has a 64 MiB decompression limit; each decoded output has a 32 MiB limit.
Unsupported/raw fallback schemas or inconsistent fields fail explicitly.

Call `ack()` only after durable application processing. Even batches with no
matches need an ACK. Checkpoint storage completes before the transport advances.
Use the saved cursor in a new client to resume. Delivery can repeat after a crash;
deduplicate using source session, bank ID, blockhash, and transaction index/signature. A cursor outside
the server's retained window raises an error; archive recovery is not connected.

`mode: 'binary'` is the default for direct connections. Use `mode: 'sse'` on
`stream.blockzilla.dev`; the tunnel can buffer a long binary response. SSE receives small chunk notices and
fetches the same full compact binary chunks. The current SSE endpoint does not
send an inline list of transaction signatures. A future receipt endpoint can
send slot, blockhash, finality, signatures, and results. The index feed with
selective GET requests is a separate future source, behind the same application
interface. CDN caching remains disabled in the pilot.

Node loads the Wasm file next to `sdk.mjs`. A browser serves `sdk.mjs`, `index.mjs`,
and `decoder.wasm` from its application origin. The relay must allow that exact
origin. API keys stay in request headers. Never ship a privileged relay key in
a public application bundle; provision an appropriate consumer credential.

`example.mjs` reads a key from a file and saves a checkpoint with file and
directory synchronization. Replace its console output with durable processing
before acknowledging. It connects to an existing endpoint; it does not create
a public tunnel or restart a validator.

```sh
node example.mjs STREAM_URL KEY_FILE CHECKPOINT_FILE PROGRAM_ADDRESS
node --test test.mjs sdk-test.mjs
```

Tests cover decoder buffer ownership, bounds, dictionary resolution, and the
existing transport. `sdk-live-test.mjs` checks Node and real Chrome, both HTTP
modes, local program/account filters, checkpoint restart, and stable memory on
repeated reads. `../tests/sdk_rpc_oracle.py` compares every transaction ID,
signature, result, and program reference with the local validator's finalized
`getBlock` response. It also compares the selected program/account transaction
sets. Receipts are under `../evidence/`.

The processed profile is `native-processed-compact-v1`. `batch.kind` is `block`,
`root`, or `invalidate`. A block contains one bank, with `bankId` and
`finality: 'processed'`. Controls have zero transactions and no dictionaries.
They reference the existing bank. Roots do not establish complete archive
coverage. The resume cursor is an opaque event position, not a slot or blockhash.
