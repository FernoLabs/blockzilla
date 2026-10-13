# Live stream

Follow complete processed Solana banks before finality. A bank can later be invalidated. The relay sends the same complete compact
blocks to every client. Your client decodes them and keeps only the
transactions you select.

> **Private beta.** The stream is at `https://stream.blockzilla.dev` and needs a
> consumer API key. Ask the Blockzilla team for a key. The JavaScript SDK is not
> yet on npm, so you use it from a clone of this repository.

`blockzilla stream --epoch ...` reads finished epochs from the archive. It does
not follow new blocks. Use this page for new blocks.

## Open a live demo

Open the free [live demo](https://stream.blockzilla.dev/demo) to see complete SSE messages
arrive in your browser. You can pause and resume. The view keeps the latest 100
messages. The [blockhash feed](https://stream.blockzilla.dev/v1/blockhashes)
sends the same data as SSE events with JSON block identities. No API key is required. It sends
processed blocks and later `root` or `invalidate` records. A processed block can
change. This feed contains no transactions or compact block payloads.
Each block's JSON contains only `slot`, `blockhash`, and `parent_blockhash`.
Root and invalidation JSON contain only `slot` and `blockhash`. The event name
identifies the kind; the SSE ID supplies the resume position. These values are
not repeated in JSON. The demo shows all three SSE lines.

To use it in a browser application:

```js
const events = new EventSource('https://stream.blockzilla.dev/v1/blockhashes');
for (const type of ['block', 'root', 'invalidate']) {
  events.addEventListener(type, event => {
    console.log(event.lastEventId, type, JSON.parse(event.data));
  });
}
events.addEventListener('gap', event => {
  events.close();
  console.error(JSON.parse(event.data));
});
```

EventSource sends its last event ID when it reconnects. You can also use
`?after=CURSOR`. An expired cursor requires recovery; it does not silently start
from the head. This small public view uses a separate connection limit.

For a private demo, open this URL with your URL-encoded key:

```text
https://stream.blockzilla.dev/v1/events?api_key=YOUR_KEY
```

This feed sends compact chunk notices. Each chunk URL still needs your key.
Keys in URLs can appear in browser history and access logs. Use the
`Authorization: Bearer KEY` header in applications.

## Before you start

- Node.js 22 or later.
- A clone of this repository. See [Quick start](quick-start.md#1-install-the-cli).
- Your consumer key in a private file outside the repository:

```bash
mkdir -p ~/.config/blockzilla
nano ~/.config/blockzilla/stream.key   # paste the key, then save
chmod 600 ~/.config/blockzilla/stream.key
```

## Print live transactions

From the repository root:

```bash
BLOCKZILLA_STREAM_MODE=sse node hivezilla/stream-relay/client/example.mjs \
  https://stream.blockzilla.dev ~/.config/blockzilla/stream.key ./stream.cursor
```

Each output line is a transaction or a small `root` or `invalidate` record:

```json
{"slot":…,"blockhash":"…","signature":"…","success":true,"topLevelPrograms":["…"],"innerPrograms":["…"]}
```

The first run starts at the newest event. Resume uses `SESSION:pSEQUENCE:SHA256`. Treat this value as opaque. A slot or blockhash is not a resume position. The example saves its position in
`./stream.cursor`; the next run continues from there. Stop with Ctrl+C.

To print only one program, add its address as the last argument. This example
selects Pump.fun:

```bash
BLOCKZILLA_STREAM_MODE=sse node hivezilla/stream-relay/client/example.mjs \
  https://stream.blockzilla.dev ~/.config/blockzilla/stream.key ./pumpfun.cursor \
  6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
```

Always set `BLOCKZILLA_STREAM_MODE=sse` for `stream.blockzilla.dev`. See
[Binary or SSE](#binary-or-sse).

## Use the SDK in your application

Import the SDK from your clone. Keep `sdk.mjs`, `index.mjs`, and `decoder.wasm`
in the same folder.

```js
import { createClient } from './hivezilla/stream-relay/client/sdk.mjs';

const client = createClient({
  url: 'https://stream.blockzilla.dev',
  apiKey: process.env.BLOCKZILLA_STREAM_KEY,
});

for await (const batch of client.stream({
  after: savedCursor,          // undefined on the first run
  mode: 'sse',
  programs: [PROGRAM_ADDRESS], // optional local filter
})) {
  // Your handler stores candidates, selects roots, or undoes invalidations.
  // Commit the application changes and this cursor in one transaction.
  await applyBankEventAndSaveCursor(batch.session, batch.event, batch.blocks, batch.cursor);
  await batch.ack();               // then advance the cursor
}
```

`savedCursor` and the bank handler belong to your application. A root record selects one bank. It is not a complete archive coverage statement.

- Call `batch.ack()` only after your data is stored. The SDK then calls
  `onCheckpoint` with the new cursor.
- Acknowledge every batch, also a batch with no matching transactions.
  Otherwise the position does not advance.
- After a crash, the SDK can send the last batch again. Make the bank handler safe
  to repeat: use the source session, bank ID, blockhash, and transaction index as a unique key. Two banks can have the same slot.

In a browser, serve the three SDK files from your own site. The relay accepts
one configured browser origin, so ask the Blockzilla team to add yours. Do not
put a key in a public web page.

## Filter on your machine

```js
client.stream({
  programs: [PROGRAM_ADDRESS],
  accounts: [ACCOUNT_ADDRESS],
  successfulOnly: true,
  includeDetails: true,
});
```

| Option | Effect |
| --- | --- |
| `programs` | Keep transactions that call one of these programs, at top level or in a recorded inner call (CPI). |
| `accounts` | Keep transactions whose message uses one of these accounts. |
| `successfulOnly` | Drop failed transactions. Default: `false`. |
| `includeDetails` | Add account keys, message, and metadata to each record. |

`programs` and `accounts` combine with OR. With no filter, you get all
transactions.

Filters run after the full feed arrives. They reduce the work in your
application, not your network traffic.

## Returned data

| Value | Meaning |
| --- | --- |
| `batch.blocks` | Blocks in order, including blocks with no matching transactions |
| `batch.cursor` | The resume position after this batch |
| `block.slot`, `block.blockhash`, `block.parentSlot` | Block identity |
| `block.transactionCount` | Transactions in the block, before filtering |
| `block.transactions` | Your selected transactions |
| `tx.index`, `tx.signature` | Position in the block and transaction signature |
| `tx.success` | `true`, `false`, or `null` when the result is not known |
| `tx.topLevelPrograms`, `tx.innerPrograms` | Programs called directly and through CPI |
| `tx.innerCoverage` | Whether inner-call data was recorded for this transaction |
| `tx.accountKeys`, `tx.message`, `tx.metadata` | Extra data when `includeDetails` is `true` |

Details use Blockzilla's compact schema, not Solana RPC JSON. Large integers can
be decimal strings. Use `batch.resolvePubkey(reference)` to turn a key reference
into an address.

If `tx.innerCoverage` says inner data is missing, an empty `innerPrograms` list
does not mean that no CPI happened.

## Binary or SSE

| Mode | How data arrives |
| --- | --- |
| `sse` | Small events announce each new chunk. The SDK downloads each chunk with an HTTP GET. Use this mode with `stream.blockzilla.dev`. |
| `binary` | One long HTTP response carries all chunks. This is the SDK default. Cloudflare can buffer it, so use it only on a direct connection to a relay. |

Both modes return the same blocks.

## Stop and resume

The cursor identifies one exact chunk. Save it and pass it as `after` to
continue.

The relay keeps only recent chunks: a 128 MiB window by default, which is about
two minutes of blocks. If your client is offline for longer, its cursor expires
and the SDK stops with a replay error. The JavaScript SDK cannot yet fill that
gap from the archive.

If you need to recover after long stops, use the
[token indexer](token-indexer.md). It can fill a gap from Blockzilla history,
when that history is published.

## Connect from Rust

The [Rust client probe](../../hivezilla/stream-relay/examples/rust-consumer/README.md)
connects without Node.js. From the repository root:

```bash
cargo run --locked \
  --manifest-path hivezilla/stream-relay/examples/rust-consumer/Cargo.toml \
  -- https://stream.blockzilla.dev ~/.config/blockzilla/stream.key 3
```

It prints three chunk summaries and stops. It is a connection example: it does
not save data, retry, or resume by itself. For an application, decode frames
with `hivezilla_stream::frame::decode`, then transactions with
`blockzilla_stream_decoder::decode`. These crates are not yet on crates.io.

## Limits

- **Finalized blocks only.** Processed and confirmed blocks are not available.
- **Full feed for every client.** About 0.5 MB per block reaches your machine,
  whatever filter you use.
- **Development data source.** Blocks come from Blockzilla's own validator
  capture. The feed marks them `canonical=false`: a gap in capture is possible
  and is not yet repaired automatically. See [Data and coverage](../advanced/coverage.md).
- **No CDN cache yet.** Every chunk comes from the relay.

Reference: [SDK](../../hivezilla/stream-relay/client/README.md) and
[transport and HTTP routes](../../hivezilla/stream-relay/README.md).

## Processed bank records

The profile is `native-processed-compact-v1`. A `block` event supplies one immutable
compact bank. `event.bank` has the node-local bank ID, slot, blockhash, parent slot,
and parent blockhash. A `root` or `invalidate` event supplies the same bank identity
and its `payload_sha256`, with no block payload. Finalization does not replay,
compact, or send the full bank a second time. Transport retries can repeat an event.

Save the event sequence and application changes in one transaction. Keep candidate
state separate from rooted history, and remove candidate effects on `invalidate`.
Bank IDs have meaning only within this producer and source session. Parent links
can identify forks; there is no single increasing slot sequence in the processed feed.

The current SQLite token indexer accepts finalized history only. It rejects this
feed because fork undo is not implemented. The stream source still has
`canonical=false`, `publication_ready=false`, and `evidence_included=false`.
Recorded logs can be truncated by Agave. The feed is not a complete RPC rendering.

A source session change or an expired position stops the current client. Do not
silently start at the head. Automatic archive backfill and full-epoch coverage are
not established for this processed feed.
