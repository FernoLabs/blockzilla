# Rust client probe

This example connects directly through HTTPS. It receives SSE chunk notices,
fetches finite binary chunks, validates their frame and object digests with
`hivezilla_stream::frame`, then uses the shared Rust compact decoder.
It does not need SSH, Node.js, or Wasm.

Use Rust 1.98 or later and the current source checkout. The crates are not
published. Build from this directory:

```sh
RUSTFLAGS='-C target-cpu=native' cargo run --locked -- https://stream.blockzilla.dev /private/path/stream-api.key 3
```

The optional fourth argument is the complete `AFTER_CURSOR` from an earlier
output record. Treat the cursor as opaque. With no cursor, the probe starts after
the current head. For processed data it checks consecutive event positions and stable source identity. Each bank retains its own parent link. Two banks can have the same slot. A production application must also check its first block against its
saved block identity, and store data and cursor in one durable transaction.

This is a bounded probe, not a durable indexer. It prints chunk counts and source
flags. It does not save records, retry, or backfill. HTTP 410, gap events, source
failure, or malformed data stop the probe. Network errors retain the last printed
cursor; a caller can reconnect from that cursor while the relay still retains it.
Never infer exactly-once processing from console output.

SSE carries notices, not full blocks or a signature list. Each GET supplies the
same complete compact data. Filtering is local. Add program/account filters to
`blockzilla_stream_decoder::Options`; use `include_details` for compact message
and metadata fields. Decoder output is JSON bytes with the compact SDK schema.
It is not full RPC JSON or a complete account-state feed.

The source remains operator-trusted processed bank data with `canonical=false` and
`publicationReady=false`. Archive backfill is not connected to this example.
Keep the consumer key private; it is a request header, not a URL parameter.

The finalized pilot was tested on 5 October with eight chunks, 12 blocks, and
12,817 transactions. The public processed feed was tested on 10 October with
16 events, including payload-free roots. One earlier probe stopped on a
connection reset; a new probe passed. This is not a sustained capacity test.
Three parser/URL tests cover SSE boundaries and URL admission. Network failures
stop this probe. A durable application must retry from its saved cursor.

See [the live stream guide](../../../../docs/start/live-stream.md).

The processed profile is `native-processed-compact-v1`. The probe prints the typed
`event` field. `block` includes one complete compact bank; `root` and `invalidate`
have no transaction payload. A root selects the existing bank for history. It does
not send the bank again. Save candidate data by source session, bank ID, and blockhash,
and undo effects on invalidation. Store the full opaque event cursor with your data.
The probe itself does not implement application undo or durable recovery.


## Opt-in native bank v2

The additional `bank-consumer` binary uses the separate v2 contract. Build it on
Foundation with `cargo +1.98.0 build --release --bin bank-consumer`. The original
binary remains the default for existing `cargo run` commands.

```sh
bank-consumer https://stream.blockzilla.dev/ KEY_FILE EXPECTED_SOURCE_JSON ledger.sqlite 1000
```

Keep the key in a private backend file. Get the exact four-field source JSON from
the operator. The source is pinned in SQLite. Events, payload references, decoded
transaction data, and the cursor commit together. A restart resumes that cursor.
SSE transport and chunk GET network failures retry at the last commit. A failed
source, digest, dependency, or protocol check stops this probe. Restart uses the
same database. An HTTP/SSE gap requires explicit
reconciliation, not a reset. Controls fetch a missing dependency once and promote
only its exact bank, original marker digest, full-frame digest, and native height.
Processed data never supplies a confirmed blockhash. The public Rust `ClientState`
checks source age and monotonic selection age.

This is an append-only collector. Application rollback, token account state,
wallet watches, and finalized history commits remain consumer work.


The v2 example has four active finite GETs and twelve ordered jobs. Native decode
runs in bounded blocking jobs; SQLite applies the results in event order. It
retries wrapped response-body resets and 429/5xx without changing the saved
cursor. Source, identity, digest, and replay admission errors stop it. The public
probe restarts this example with the same database and checks consecutive event
positions without duplicates. See `hivezilla/deploy/bank-stream-v2/ACCEPTANCE.md`
for the provider deployment and measured limits.
