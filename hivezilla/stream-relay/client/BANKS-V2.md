# Opt-in native bank client

Use `./banks.mjs` on your backend with `decoder-banks-v2.wasm`. This module uses
separate Rust decoder exports. It does not change v1 admission. Keep the Bearer
key in a server secret or private file. Do not put it in a URL or browser setting.

```js
import { createBankClient } from './banks.mjs';
const client = createBankClient({
  url: 'https://stream.blockzilla.dev/',
  apiKey: privateServerKey,
  source: operatorSuppliedSource, // exact genesis, node, Agave revision, session
});
for await (const frame of client.stream({
  after: databaseCursor,
  onBootstrap: async ({ after, payload, control }) => {
    await saveBootstrapDependency(after, payload, control);
  },
})) {
  // One database transaction: payload/dependency + control + application + cursor.
  await commitBankEventAndCursor(frame);
  await frame.ack();
}
```

Supply a saved `after` cursor to resume. With no cursor, the client explicitly
joins at the current head. A source change or replay gap stops the iterator. It
never resets a saved position. Your database must pin the complete source tuple.
Network failures retry the last acknowledged position. An application commit
failure stops the iterator; reopen it from the database checkpoint.

A block is a complete processed candidate. Match confirmation by bank identity,
original marker digest, and full payload frame digest. Failed transactions remain
present with a non-null error. Controls contain no second compact block. A missing
control dependency is fetched at the exact relative chunk path. The bootstrap
head resolves its exact control and full payload before selection. Save that
payload in your database if later controls will need its transaction records.

`client.readyHash()` returns a native confirmed/finalized tuple only when the
client has reached the provider head and health is current. It throws on stale
health, a processed-only source, invalidated confirmed state, or excessive age.
Repeated status or an older root does not renew the selected tuple's monotonic
age. This is a blockhash selection helper, not a wallet or send implementation.
Use the tuple's `context_slot` as the send floor. Keep the signed blockhash and
expiry bound to the same transaction.

The decoder checks strict v2 JSON fields, required nulls, object layout/digests,
signatures, loaded addresses, outcomes, and native block height. The client also
checks source, SSE start boundary, notice ID, sequence, byte length, slot bounds,
relative URL, and full-frame SHA before application. Credentialed GETs do not
follow redirects. Metadata proofs have a 16,384-bank hard limit and are pruned by
height. Transaction payload storage and application rollback belong to the
consumer. Do not insert processed blocks into finalized history.

Build Wasm and export the synthetic current compact fixtures on Foundation with
`hivezilla/deploy/bank-stream-v2/build_client.py`. Run `node --test test.mjs
sdk-test.mjs banks-test.mjs` in this directory. The fixtures are not live mainnet
data and do not prove native plugin compatibility; the separate native load
receipt covers that boundary. The package remains private and is not on npm.

A delayed health sample can still be valid evidence of its original sample. It
must not stop replay. Readiness checks its current age and uses the stricter
five-second source age policy, in addition to the 15-second monotonic selection
limit. A delayed or repeated old sample cannot make a hash ready. Product client
headers identify this backend to the public edge; no API key enters that header.


Finite reads run in parallel, with a bound of four. Notices, decoded results,
application effects, and acknowledgements keep event order. Closing the iterator
cancels its pending reads. Wasm decode runs on the JavaScript thread; the bound
is not a claim that one thread can render all transaction details at every live
rate. Use the Rust consumer when you need the measured full-detail throughput.
A worker pool for Node/browser decode remains a possible client improvement.

The Foundation public probe on 10 October decoded 80 events and resumed exactly.
It preserved execution errors but recorded zero hash-ready observations with
full-detail decode and durable per-frame file writes. This finite check does
not certify that the JavaScript path keeps pace with live publication. The
Rust public collector passed the separate 11-minute test. See
`hivezilla/deploy/bank-stream-v2/ACCEPTANCE.md` for actual delay and capacity.
