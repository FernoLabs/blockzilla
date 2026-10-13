# Processed compact bank stream

The owner changed the target on 10 October 2026: send complete processed blocks
once and reuse their compact payloads for finalized history. There will be no
second full block delivery at finalization. Bank invalidations and resume need
small control records and event positions. This profile is implemented and was
installed on Foundation on 10 October. The public endpoint serves this one feed.
See the [stream design](../../docs/architecture/hivezilla-compact-stream-relay.md#current-target-compact-once-stream-at-processed).

A separate Rust origin reads committed native compact output. One upstream
connection supplies a Rust relay. The relay sends the same immutable bytes to
all consumers. It does not decode or filter transactions.

This is a **development projection feed**, not a canonical archive contract.
It preserves `canonical=false`, `publication_ready=false`, processed commitment,
operator identity, genesis, Agave revision, bank IDs, parent links, and the complete
native compact commit marker. The payload contains every known compact projection
file and reader dependency, including registries, signatures, index, available
PoH, metadata, and the normalization receipt. It excludes `source-events.json[.zst]`.
That source evidence remains on the validator machine. Compact fields must not be
presented as full native execution evidence. A portable Rust/Wasm semantic decoder
and simple JavaScript SDK are now in `decoder/` and `client/sdk.mjs`. They receive
the full feed and apply local program/account filters. Full RPC rendering and the
supported public field-coverage contract remain separate work. See
[`client/README.md`](client/README.md).

Start with the [live stream user guide](../../docs/start/live-stream.md).
The [program dumper](../../docs/start/program-dumper.md) shows local program
selection. The [token indexer](../../docs/start/token-indexer.md) supplies a
SQLite consumer for finalized history. It rejects the processed live feed until
application undo is implemented.

Since 5 October 2026, the API-key protected relay is also accessible at
**`https://stream.blockzilla.dev`** through the existing Foundation Cloudflare
Tunnel. Use SSE notices with finite binary chunk GETs on that route. SSH and WARP
are not required for consumers. A [Rust client probe](examples/rust-consumer/README.md)
uses the existing frame admission and native decoder directly. It is a bounded
example, not a durable subscription client. The public route does not add R2
backfill, CDN caching, a second relay machine, or independent finality evidence.

## Wire and HTTP contract

Each processed block frame contains one complete bank. A `root` or `invalidate`
frame contains no binary objects. Rooting selects the same compact payload for
the archive catalog; it does not run the converter or send the block again.
The origin adds no batch wait. Maximum payload: 16 MiB. Maximum JSON: 64 KiB.
Oversized batches stop the feed with a visible error; they are not skipped.

```
8 bytes  ASCII BZSTRM01
4 bytes  JSON length, unsigned little endian
4 bytes  payload length, unsigned little endian
N bytes  UTF-8 JSON envelope
M bytes  concatenated compact objects
```

The envelope has schema `blockzilla-stream-envelope-v1`, profile
`native-processed-compact-v1`, a typed `event`, `evidence_included=false`, the marker,
and ordered objects with path, offset, byte length, and SHA-256. Object views
use those offsets within the payload. A chunk ID is SHA-256 of the complete
frame. It is independent of HTTP read boundaries and current worker timestamps.
The cursor is `SESSION:pEVENT_SEQUENCE:CHUNK_SHA256`. A bootstrap or retention
anchor uses `SESSION:pEVENT_SEQUENCE:-`. Only the exact returned anchor is valid.
Bank IDs are local to the node and source session. Banks at one slot can compete.
Each bank has its own parent link. Event positions must be consecutive. Legacy
finalized files remain readable with their old slot cursor and profile.

| GET route | Response |
| --- | --- |
| `/v1/head` | Current cursor, earliest permitted `after`, source status, stale flag, byte budget and counters |
| `/v1/blocks?after=CURSOR` | Open binary response of complete frames |
| `/v1/events?after=CURSOR` | UTF-8 SSE chunk notices with cursor IDs and binary URLs; status events every idle 10 seconds |
| `/v1/chunks/SHA256` | One finite binary frame, identical to the binary stream bytes |
| `/v1/blockhashes?after=CURSOR` | Optional free SSE feed with JSON block identities and root/invalidation records; no transaction payloads |
| `/demo` | Free browser view of the blockhash feed, with pause/resume and the latest 100 JSON messages |

An omitted `after` explicitly selects live delivery from the head at connection
open. For replay, use `earliest_after` or a saved exact cursor. SSE also accepts
`Last-Event-ID`; conflicting header/query values return 400. A cursor outside
retention returns **410**, with current head information. A slow stream whose
cursor expires is terminated; SSE emits a `gap` event first. No route silently
changes an expired cursor to the current head. Root and invalidation records can
refer to earlier slots. Capture-session or source-identity changes stop intake.

Full block requests accept an `Authorization: Bearer KEY` header. Keys are read
from files at startup. A relay started with `--allow-query-api-key` also accepts
`?api_key=KEY` on private read routes. This permits a browser demo at
`https://stream.blockzilla.dev/v1/events?api_key=YOUR_KEY`. Encode the key as a
URL query value. A conflicting header and URL key, or duplicate URL keys,
are rejected. Notice URLs never contain the key; a manual chunk GET needs its
own header or URL key. URL keys can appear in browser history and access logs.
Use the header in applications. The mTLS origin accepts headers only.

With `--public-blockhashes`, the relay also serves `/v1/blockhashes` without a
key. A `block` event's JSON contains only `slot`, base58 `blockhash`, and
`parent_blockhash`. A `root` or `invalidate` event's JSON contains only `slot`
and `blockhash`. The SSE `event:` line identifies the event kind once. The
`id:` line supplies the exact resume position; neither field is repeated in JSON.
A `block` is processed and can later be invalidated. A `root` selects an existing
bank; it does not send another full block. `status` and `gap` events report idle
state and replay/source failures. This view requires the processed source profile.
It follows the same exact resume rules as the private feed and starts live when
no cursor is supplied. It does not provide archive backfill.
The `/demo` page displays complete `id:`, `event:`, and `data:` lines without
removing JSON fields. It keeps the latest 100 messages and supports pause/resume.

The public view uses wildcard CORS and a separate request budget, default eight
open requests (`--public-max-requests`). All metadata readers share one encoded
notice per event. Slow readers do not retain full block bytes. The private routes
still require a key and accept only one configured browser origin. Both demo
options are off by default and are available on the relay only.

All current responses use `no-store` and `Referrer-Policy: no-referrer`; authenticated
CDN chunk caching is future work. HTTP/1.1 and HTTP/2 work over TLS. The code
refuses non-loopback cleartext binds and remote cleartext upstream links.

## Bounds and recovery

The processed origin uses a read-only SQLite connection and the atomic event
commit in `feed.sqlite` as its publication fence. Pending files cannot publish
themselves. The database, WAL, and
compact files stay read-only. SQLite can update its shared lock metadata in
`feed.sqlite-shm`. Origin retention is a memory window;
origin restart can expire older relay cursors. Archive backfill is not connected.

The relay fsyncs a frame and checkpoint before admitting it. Its checkpoint
lists the retained frames and exact resume position. A process lock rejects
two writers for one spool. Its byte window is finite, default 128 MiB. The
relay can replay that window after restart. Clients share immutable `Bytes`
buffers. They do not receive a private copy of the whole ring.

Memory consists of the ring, one ingest/build stage, and at most one maximum
frame held by each active response, plus bounded HTTP/TLS buffers. Budget for
`ring + max_requests * MAX_FRAME + ingest overhead`; do not equate the ring
budget with total RSS. Default 32 requests and 64 connections are bounded.
There are at most 32 HTTP/2 streams per connection and a 64 KiB send buffer.
Handshakes/header reads have a five-second deadline. Connections close after
five minutes, including a stalled client, and reconnect from the saved cursor.
Origin/relay service resource limits provide an additional host bound.

Origin failure and protocol/digest/identity errors latch `FAILED`. Network
interruptions retry after 1–30 seconds from the durable cursor. The relay head
becomes stale after 30 seconds without intake; this measures intake freshness,
not time since Solana finalization. Counters are process-local. Restart restores
position and replay data, not lifetime metric totals. No deletion of native
source or compact output depends on a relay ACK.

TLS on an origin requires a client CA. Use a dedicated relay CA and only issue
approved relay certificates. The relay validates the server name and CA and
uses a client identity. Early data is disabled. To revoke credentials immediately,
replace the key/CA and restart only the affected stream service; this closes its
existing connections. Dynamic revocation without restart is not implemented.

## Run

Build and test on the remote machine. Do not run a local Cargo build on the
operator Mac. The crate has an independent lock file and no validator ABI.

```sh
cargo test --locked
cargo build --release --locked

hivezilla-stream origin --output /path/to/compact/SESSION --start-slot SLOT \
  --bind 127.0.0.1:8870 --token-file /protected/origin.key \
  --tls-cert /protected/origin.crt --tls-key /protected/origin.pem \
  --client-ca /protected/relay-ca.crt --max-requests 4

hivezilla-stream relay --upstream https://ORIGIN:8870 \
  --upstream-token-file /protected/origin.key --upstream-ca /protected/origin-ca.crt \
  --upstream-identity /protected/relay-identity.pem \
  --spool /path/to/relay-spool --bind 127.0.0.1:8871 \
  --token-file /protected/client.key
```

Public relay binding additionally needs `--tls-cert` and `--tls-key`. The current
machine test is separate from a second-machine deployment and Cloudflare routing.
No existing gRPC or tunnel routing is changed by compiling this crate.

## Browser and Node.js

The dependency-free ES module is `client/index.mjs`. It provides binary HTTP
and SSE transport, byte bounds, file/frame digests, cancellation, parent-chain
checks, reconnect, and explicit durable checkpoints. It returns raw compact
objects and block identities. It **does not yet decode compact transactions**.
Use fetch-based SSE so the key stays in the Authorization header.

```js
import { streamChunks } from './client/index.mjs';

for await (const chunk of streamChunks({
  url: 'https://relay.example',
  apiKey,
  after: savedCursor,          // omit only to start at the current head
  mode: 'binary',             // or 'sse'
  signal: abortController.signal,
  onCheckpoint: async cursor => saveCursorDurably(cursor),
  onStatus: status => reportStreamStatus(status),
})) {
  await processCompactObjects(chunk.objects, chunk.blocks);
  await chunk.ack();          // after processing is durable; required before next item
}
```

Authentication failures, expired cursors, conflicting parents, corrupt chunks,
and missing dependencies require explicit recovery. They do not silently retry
from a different position. The application controls its own processing and
checkpoint storage. Frame SHA-256 is an integrity check, not Solana signature
or PoH verification. The browser module requires a secure context with Web Crypto.

The origin can use `--tail-slots 256` for a bounded recent startup instead of
`--start-slot`. Source session selection remains explicit. Startup recovery
reclaims only this spool's orphan frames and temporary files under its process
lock; it does not touch native source or compact output.

Run JavaScript tests with `node --test client/test.mjs`. Rust tests include the
commit fence, dependency corruption, skip/gap distinction, spool restart,
conflicting content, replay expiry, and transport access constraints.

## Tested local pilot, 4 October 2026

Both services run on the Foundation machine, on loopback ports 8870 and 8871.
Eleven Rust tests, nine JavaScript tests, and ten process-level checks passed.
Ten live consumers received identical bytes for eight chunks, through one
upstream feed. Node.js 26.8.1 and real Chrome 154.0.8037.97 received the same five
live chunks with both transport modes. Receipts are in `evidence/`.

The eight-chunk live sample contained 12 blocks and 4,824,713 bytes per consumer
in about 3.09 seconds: roughly 12.5 Mbit/s. A separate 20-second sample with no
deliberate fan-out load used 0.70 percent of one core for the origin and 0.47
percent for the relay, with about 166 MB and 332 MB of service memory. This
includes file cache. These observations do not prove external network capacity.

The shared Rust/Wasm semantic decoder is now implemented for browsers and Node.
The separate Rust token indexer can backfill from published Blockzilla history
or retained native compact files. Archive recovery is not connected to the relay
or JavaScript SDK. Public routing, a second-machine deployment, QUIC, certificate
renewal, automatic session changes, and CDN caching remain unfinished. The pilot
uses 30-day dedicated certificates and startup-loaded API keys. See the native
deployment record for exact paths and service bounds.

## Foundation R2 publication and stream repair, 5 October 2026

The source reader stopped with a SQLite read-only error while the worker kept
writing compact output. The repaired source retries transient SQLite errors
from the same position. The source and native-history consumer units allow
writes only to the audit database's shared lock file. Their database connections
remain read-only. The origin and relay were restarted. The validator and compact
worker kept their PIDs. The old failed relay spool remains in place; the new
relay has a separate spool. Saved consumer positions still require history
recovery when they fall outside the new replay window.

The Rust `publish` command now saves the same finite stream frames to Foundation
R2. Its production unit is `deploy/hivezilla-native-stream-publisher.service`.
It starts at slot **453600000**, the start of epoch **1050**, and continues across
epoch boundaries in the selected capture session. A validator session change
requires an explicit new source selection.

- Bucket: `solana-archive-data`.
- Prefix: `native-stream/direct-agave-live-v1/SESSION`.
- Immutable frames: `chunks/LAST_SLOT-SHA256.frame`.
- Immutable ordered receipts: `commits/SEQUENCE_20_DIGITS.json`.
- Catalog head: `HEAD.json`; namespace ownership: `OWNER.json`.
- Local state: `ROOT/reports/native-stream-r2-live-20261005`.
- Credentials: private `ROOT/config/foundation-r2/native-stream.env`.
- Publisher release: `ROOT/releases/hivezilla-stream-r2-20261005-r2`.

Each receipt links the previous cursor to the new cursor. The publisher checks
parent slots, block hashes, source identity, and the native durable commit fence.
It saves an upload intent before network writes. Up to 64 upload workers handle
at most 64 frames and 64 MiB per batch. Only a complete batch can advance the
catalog head and local checkpoint. Restart completes the same pending batch.
Readers must use only the receipt sequences admitted by `HEAD.json`; a pending
batch can have objects and receipts present before the head advances.
An existing conflicting object or a parent gap stops publication. A separate
state lock and remote ownership record prevent another state from using the
same namespace. The processed publisher has a four-core CPU limit and a 2 GiB
memory limit. Status records separate source-read time from upload/commit time.
The earlier finalized publisher retains its old limits and namespace.

Conditional object creation, remote lengths, and uploader digest metadata check
payload admission. Small receipts and the head have exact read-back checks.
The digest metadata is not a provider-attested checksum. Routine publication
does not download every payload again. Source compact files remain on disk.
The source profile keeps `canonical=false`, `publication_ready=false`, and
`evidence_included=false`. Full epoch coverage still needs separate evidence.

Sixteen Rust checks, nine JavaScript transport checks, and ten process-level
checks passed. The parallel R2 test saved 84 blocks in 18.61 seconds, about
eight times the serial test's byte rate. This is a short capacity sample.
The earlier test objects remain under `native-stream/direct-agave-v1/SESSION`.
The old native tar snapshot backup stays disabled. The CAR archive upload uses
its separate prefix and service.

R2 publication now supplies durable objects and an ordered catalog. A public
R2 read route and automatic R2 backfill in the SDK remain unfinished. The live
HTTP/SSE relay is still on the Foundation machine. Its separate Hetzner host
requires access details.

## Opt-in native confirmed bank stream, 10 October 2026

The separate `native-bank-compact-v2` profile adds `/v2/banks/head`,
`/v2/banks/events?after=CURSOR`, and `/v2/banks/chunks/SHA256`. These routes require
backend Bearer headers. URL credentials remain a v1 demo option only.

The complete requested contract is [the v2 build specification](../../docs/blockzilla-confirmed-stream-spec.md).
The exact native host patch, build/load record, source identity, replay rules,
rollback procedure, and consumer handoff are in
[the native v2 guide](../deploy/bank-stream-v2/README.md).
The [Rust consumer](examples/rust-consumer/src/bin/bank-consumer.rs) commits
payloads, small controls, and the cursor in SQLite. The
[separate JavaScript SDK](client/BANKS-V2.md) uses strict Rust Wasm admission.
Both preserve failed transactions and match exact payload digests. Delayed
factual status messages remove readiness while replay continues. They do not
renew the confirmed tuple. Source, protocol, and explicit replay gaps stop the
client. Native processed and finalized v1 admission stays separate.

Provider tests do not certify the Ferno wallet/send adapter, Alpenglow footer
coverage, an uninterrupted epoch, or a second relay machine. Use the recorded
live test results for actual replay duration, throughput, and delay.
