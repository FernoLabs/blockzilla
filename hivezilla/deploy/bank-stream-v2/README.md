# Native bank stream v2

This is a separate backend profile: `native-bank-compact-v2`.
The v1 profiles, public demo, and finalized history reader remain separate.
The full requested contract is in [the build specification](../../../docs/blockzilla-confirmed-stream-spec.md).

## Build and native boundary

All Cargo commands run on Foundation. The native host and plugin use Rust
1.97.1 and the same patched Agave interface. The relay, portable decoder, and
consumer use Rust 1.98. The interface adds `notify_bank_state_v2`; an old host
must not load the new plugin. Install the host and plugin as one release.

The patch reads bank ID, slot, parent slot/hash, blockhash, block height, and
`get_blockhash_last_valid_block_height(last_blockhash)` at the frozen and actual
optimistic confirmation boundaries. Agave's existing exact parent walk supplies
ancestor callbacks. A frozen callback does not establish confirmation.

The plugin writes the bounded durable native journal. Discovery stores native
confirmation and exact control sequence/time before compact construction. The
existing Tower completeness gate checks dense transactions, entries, block
metadata, and the last entry hash. Tower does not emit the Alpenglow footer;
the real writer test sends confirmation before complete entry/block data. This
release does not certify Alpenglow completeness.

`build-confirmed PROCESSED OUTPUT --follow` reuses each original processed
payload. It does not compact or replay it again. Original marker bytes determine
`payload_sha256`. The immutable v2 frame has its own digest. Small controls
reference that exact frame. Missing native expiry or ancestry is a visible
failure. The first native root establishes the partial capture boundary; it
does not claim coverage before capture.

The isolated native load test loads the actual shared library, calls the patched
host notifier with a real Bank, and checks both durable journal observations.
Its slot-17 bank has height 1 and expiry 151. This is a test genesis, not live
mainnet evidence. The normal and ignored native tests also use the real compact
writer to check early confirmation, native expiry, and restart reuse.

## Read routes

All v2 routes require an `Authorization: Bearer` header. URL keys are rejected.

- `GET /v2/banks/head` returns one atomic confirmed head and replay boundary.
- `GET /v2/banks/events?after=CURSOR` returns SSE notices, status, and gaps.
- `GET /v2/banks/chunks/SHA256` returns a finite `BZSTRM01` v2 frame.

The accepted position is returned in `X-Blockzilla-Start-Cursor` before the body.
The first SSE record is a full status. Further status records occur at least
once per five seconds, including during traffic. A chunk ID equals its notice
cursor. Status and gap records have no ID. Invalid or conflicting positions
return 400. Changed or expired sources return 410 with `{error,head}`.

The origin has one additional private mTLS route, `/v2/banks/state`, for bounded
relay ancestry bootstrap. It is not installed on the public relay.

Each retained control pins its full payload. Spool files and the checkpoint are
synced before publication. Replay is limited by bytes; the installer sets 4 GiB
for each v2 spool and leaves the v1 budget separate. The checkpoint has a
64 MiB bound. At most eight staged events share one synced commit. The relay
flush timer is 10 ms; no staged head or event is visible before that commit. Check measured replay duration in the
operator receipt. The live test retained about 32.4 minutes in the 4 GiB budget
and accepted its original start cursor after collection and verification.

Source time remains the callback time. An older root does not replace or refresh
the selected head. Capture or publication failure removes live readiness. The
client checks the source tuple, accepted boundary, notice, frame digest, native
expiry, and monotonic observation age. It never gets a usable hash from a
processed block alone.

## Deployment and recovery

`install.py` accepts only passed host, native, and transport receipts. It saves
the units and plugin config, copies immutable artifacts, and restarts the
validator for the native interface change. `load_when_ready.py` is an `ExecStartPost` hook: it waits for
local health and checks the local slot against public finalized state before
loading capture. Historical startup replay is outside the live source session.
The same hook runs after later validator restarts.

A restart creates a new native source session. This is an explicit source gap.
Keep the old session, spools, reports, and R2 publication checkpoint. The old
publisher can finish its source. New publication has a separate service and
checkpoint under the existing processed namespace.

The installation record is `ROOT/reports/bank-stream-v2-20261010/DEPLOYMENT.json`.
Its `saved` and `changes` directories identify each replaced file. For rollback,
stop the new v2 services and worker, restore these files and the saved plugin
config, reload systemd, restart the old host, then start the old worker after its
fresh old-interface capture session exists. Restore public origin/relay only
with their matching source session. Do not silently assign old cursors to a new
session. Source data is not deleted by this installer.

Local compact/evidence retirement after R2 upload remains unfinished. The disk
reserve and bounded native journal can stop capture before storage exhaustion.
A full uninterrupted epoch, complete replay from genesis, and a second relay
machine are not certified by this stream release.

## Consumer handoff

Use `blockzilla_stream_decoder::decode_banks` for v2. The v1 decoder entry points
still reject v2. Failed transactions retain a non-null `err`, signatures retain
their order, and loaded account keys are resolved with the recorded dictionary.
The decoded block height must equal the native event height.

[The Rust example](../../stream-relay/examples/rust-consumer/src/bin/bank-consumer.rs)
uses a private backend header and an operator-pinned source JSON file:

```sh
bank-consumer https://stream.blockzilla.dev/ KEY_FILE EXPECTED_SOURCE_JSON ledger.sqlite 1000
```

It stores full decoded payloads once in `payloads`. It stores small events and
their cursor in one SQLite transaction. A restart resumes that cursor. A gap
requires application reconciliation; it never silently starts at the head.
The example is an append-only source collector, not a token balance adapter or
wallet confirmation service. The event limit is for finite probes.

Use a control's `payload_frame_sha256` to join its exact payload. Do not join by
slot alone. For signature watches, preserve failed transactions and require a
confirmed/root control for the exact bank. Apply invalidation to descendants by
recorded parent links; stop readiness if confirmed state is invalidated.

Ferno still needs its opt-in adapter and wallet/send acceptance suite. Do not
claim click-to-wallet or send-to-confirm measurements from provider tests.


## Operator checks added during installation

Systemd reads drop-in files in file-name order. Use `zz-bank-v2.conf` after the
existing `shred-hook.conf`. Reset both `ExecStart` and `ExecStartPost`. Parse the
old quoted arguments before removing the startup plugin config. The loader
checks `/proc/PID/exe` against the immutable tested host before loading anything.
Its receipt identifies the new journal session and exact host revision. An old
host can load this library and still omit the new callback; a successful load
alone is insufficient evidence.

The first installation exposed both errors above. The startup and old-host
journals were excluded, kept, and marked in the deployment receipt. The current
installation must pass the native tuple match check before it is called live.
`install.py --resume` uses the saved configuration and native load receipt. It
does not create another validator release or silently select a different source.
A finite `probe_live.py` records route checks, native tuple match counts, Rust
consumer timing, resource samples, and retained replay duration on Foundation.
Private ledger records stay on that machine. Only test summaries are copied here.

The separate [JavaScript v2 client](../../stream-relay/client/BANKS-V2.md) uses
strict Rust Wasm admission and a bounded bank metadata cache. It requires an
acknowledgement after durable application. V1 JavaScript admission is unchanged.


A transport restart uses the existing checkpoint without another ancestry
bootstrap. Only an empty spool obtains the private source bootstrap. Transient
bootstrap I/O retries with a 1–10 second delay. Source, digest, dependency, and
replay failures remain terminal. A server-local `SOURCE-ERROR.json` records a
terminal reason. It is not a public route. The deployment tests include delayed
origin startup and resume when the bootstrap route is absent.

The Rust consumer has four bounded parallel GETs and twelve ordered decode jobs. It applies results
in event order and commits application data plus cursor in one SQLite transaction.
The JavaScript client has four bounded parallel GETs and ordered acknowledgement.
Its Wasm decode remains on one JavaScript thread. A slow application can lose
readiness and can reach the finite replay boundary; it does not skip events.

Use `upgrade_transport.py` for a tested transport release. It preserves the
native process, source, and spool paths and saves the replaced override. The
installer's initial failed runs remain in the server deployment record. The
successful native load used a new validator process; later transport changes
have not restarted that process. The long soak and final handoff are recorded
in [the acceptance record](ACCEPTANCE.md).
