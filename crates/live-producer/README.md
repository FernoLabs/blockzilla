# blockzilla-live-producer

Deployable app boundary for building Blockzilla archives from live feeds.

The shared archive-facing types live in `blockzilla-format`; this crate owns
process concerns:

- CLI and deployment configuration
- live source adapter selection
- output directory layout
- future gRPC, Fumarole, LaserStream, DoubleZero, shred stream, RPC, and CAR
  repair workers

Current commands:

```bash
cargo run -p blockzilla-live-producer -- init --archive-dir blockzilla-live
cargo run -p blockzilla-live-producer -- plan --source own-grpc
cargo run -p blockzilla-live-producer -- run --dry-run --source own-grpc
```

The non-dry-run `run` command intentionally fails until a real source adapter is
implemented. That keeps deployments from silently running without ingesting
blocks.

## Redundant ingest configuration

The durable multi-source foundation has a separate versioned configuration. It
supports multiple gRPC/UDP/QUIC shred inputs, primary/replica roles, per-source
secret references, bounded event/byte queues, reconnect controls, mTLS material,
and disk-spool limits. Literal secret values are not representable.

Validate a primary or replica configuration without resolving or printing its
credentials:

```bash
cargo run -p blockzilla-live-producer -- validate-ingest-config \
  --config crates/live-producer/config/ingest-primary.example.json

cargo run -p blockzilla-live-producer -- validate-ingest-config \
  --config crates/live-producer/config/ingest-replica.example.json
```

The command prints only a redacted operational summary. `mode: "slave"` is
accepted as a deprecated input alias and normalizes to `replica`.

The implementation also provides source-independent block/entry/shred
identities, deterministic duplicate/conflict/fork classification, a segmented
raw spool that recomputes content digests, syncs each committed checksummed
frame, validates sealed segments, and truncates only incomplete crash tails. A
bounded mTLS receiver signs cumulative ACKs only after its exact raw WAL state is
durable. The Hetzner sender verifies and fsyncs each signed ACK before advancing
its local cursor; storage rotations keep unique physical WAL identities under
one stable logical replication sequence. Both WALs enforce one writer and
become fail-stop after ambiguous I/O errors.

Run the locally implemented receiver and raw-WAL sender with:

```bash
cargo run -p blockzilla-live-producer -- serve-ingest-receiver \
  --config crates/live-producer/config/ingest-primary.example.json

cargo run -p blockzilla-live-producer -- replicate-grpc-raw \
  --config crates/live-producer/config/ingest-replica.example.json \
  --cache-root /data/grpc-cache
```

On Blockzilla, copy only the receiver's locally fsynced prefix into a separate standard raw-gRPC
generation for the existing PoH verifier and epoch materializer:

```bash
cargo run --release -p blockzilla-live-producer -- bridge-receiver-grpc-raw \
  --receiver-spool-root /volume1/blockzilla/receiver-spool \
  --output-dir /volume1/blockzilla/receiver-derived/raw-current \
  --cluster-id solana-mainnet \
  --origin-node-id HETZNER_ORIGIN_ID \
  --source-id grpc-raw-hetzner-backup \
  --journal-id 32_HEX_CHARACTERS
```

The receiver stays live and its WAL is opened read-only. Bridge, indexing, and compaction progress
are separate from the signed ACK that authorizes Hetzner cleanup. See
[`docs/receiver-wal-bridge.md`](../../docs/receiver-wal-bridge.md) for restart semantics and the
cutover checklist.

A network response alone cannot make replica data eligible for garbage
collection: the exact signed ACK must first be verified and synced into the
local cumulative-ACK WAL. `replicate-grpc-raw` then retires covered sealed
generations oldest-first through a fsynced intent. It publishes an exact
retained-boundary anchor when the successor continues the same logical stream;
an independently replayable legacy base-zero successor needs no anchor. It
never accepts an upload receipt, heartbeat, or byte watermark as deletion
authority. The Hetzner Compose sender is available behind an inactive
`replication` profile, but the production receiver/private route and key
material are not deployed yet. The current `capture-grpc` process must not be
replaced until the route, replay, archive-writer recovery, and power-loss
rollout tests are complete. See
[`docs/live-ingest-redundancy.md`](../../docs/live-ingest-redundancy.md) for the
protocol and rollout sequence.

## Epoch boundaries

The live builder uses Old Faithful-compatible boundaries by default:

```text
epoch = slot / 432000
epoch_start_slot = epoch * 432000
```

Watch finalized gRPC slot/block-meta updates and stop when an epoch boundary is
observed:

```bash
BLOCKZILLA_GRPC_X_TOKEN=... \
  cargo run -p blockzilla-live-producer -- watch-epochs-grpc \
  --endpoint https://example.mainnet.rpcpool.com \
  --startup-rpc-url https://example.mainnet.rpcpool.com \
  --max-boundaries 1
```

This is the control-plane event the archive writer should use to close the
current live epoch, flush per-epoch indexes, and start the next epoch directory.
When `--startup-rpc-url` is provided, the watcher first calls `getEpochInfo`,
records the current absolute slot, epoch slot index, computed boundary slots,
and whether the RPC epoch model matches the fixed Old Faithful `432000`
slots-per-epoch layout.

For a standalone startup check:

```bash
cargo run -p blockzilla-live-producer -- sync-rpc-epoch \
  --rpc-url https://api.mainnet-beta.solana.com \
  --rpc-rate-limit-per-sec 5
```

RPC commands accept client pacing and server rate-limit handling flags:

- `--rpc-rate-limit-per-sec N` spaces requests locally; `0` disables pacing.
- `--rpc-follow-rate-limit true` retries after HTTP `429`, `Retry-After`, or
  JSON-RPC rate-limit errors.
- `--rpc-rate-limit-retries N` caps server-driven retry attempts.
- `--rpc-rate-limit-base-delay-ms N` sets fallback exponential backoff when the
  server does not provide `Retry-After`.

You can also compute a missing-slot plan for an epoch from already-observed
slots/ranges:

```bash
cargo run -p blockzilla-live-producer -- plan-epoch-backfill \
  --epoch 900 \
  --observed-range 388800000-389231999
```

## gRPC probe

Use `BLOCKZILLA_GRPC_X_TOKEN` for the Yellowstone `x-token` metadata value:

```bash
BLOCKZILLA_GRPC_X_TOKEN=... \
  cargo run -p blockzilla-live-producer -- probe-grpc \
  --endpoint https://example.mainnet.rpcpool.com
```

The probe connects, reads basic unary status, then subscribes to block metadata
and entry updates. It is intentionally light enough to verify PoH entry
availability without streaming full transaction payloads.

### gRPC transport tuning

All transport settings are optional. An unset setting leaves tonic's current
default unchanged, except response compression, whose explicit/default `none`
value preserves the existing uncompressed behavior. Invalid values abort before
the connection is opened.

- `BLOCKZILLA_GRPC_ACCEPT_COMPRESSION=none|gzip|zstd` enables exactly one
  response decompressor; it defaults to `none`.
- `BLOCKZILLA_GRPC_HTTP2_ADAPTIVE_WINDOW=true|false` controls HTTP/2 adaptive
  flow-control windows.
- `BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS=N` sets the HTTP/2 ping
  interval to a positive integer number of seconds.
- `BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS=N` sets the ping timeout to a
  positive integer number of seconds.
- `BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE=true|false` controls whether
  pings continue without active streams.
- `BLOCKZILLA_GRPC_LOCAL_ADDRESS=IP` binds the client socket to a bare IPv4 or
  IPv6 source address. This can select a physical interface for this process
  without changing system routes; the address must already belong to the host.

Values are exact and case-sensitive; whitespace, hostnames, CIDR notation,
ports, zero durations, and alternative Boolean spellings are rejected.

## gRPC capture

For a low-CPU outage bridge that records events without archive conversion or
derived sidecars, use `record-grpc-raw`:

```bash
BLOCKZILLA_GRPC_X_TOKEN=... \
  cargo run --release -p blockzilla-live-producer -- record-grpc-raw \
  --endpoint https://example.mainnet.rpcpool.com \
  --output-dir /path/with/free-space/mac-bridge \
  --from-slot 432621000 \
  --min-free-bytes 17179869184 \
  --idle-timeout-secs 180 \
  --require-complete-poh

cargo run --release -p blockzilla-live-producer -- inspect-grpc-raw \
  --output-dir /path/with/free-space/mac-bridge \
  --verify-payloads
cargo run --release -p blockzilla-live-producer -- verify-grpc-raw-poh \
  --output-dir /path/with/free-space/mac-bridge
```

The same process can opt into a private Yellowstone-compatible relay without
opening another upstream connection:

```bash
cargo run --release -p blockzilla-live-producer -- record-grpc-raw \
  --endpoint https://example.mainnet.rpcpool.com \
  --output-dir /path/with/free-space/active \
  --relay-bind 0.0.0.0:10001 \
  --relay-x-token-file /run/secrets/grpc_relay_x_token \
  --relay-max-records 128 \
  --relay-max-encoded-bytes 134217728 \
  --relay-max-clients 4
```

`--relay-bind` and `--relay-x-token-file` must be supplied together; omitting
both disables the relay. The downstream token file is independent of
`BLOCKZILLA_GRPC_X_TOKEN`. The listener is reserved before the upstream
transport opens, its bounded ring is preloaded from the verified active WAL,
and an update is published only after both the WAL and handoff journal fsyncs
succeed. `from_slot` is limited to the retained ring and zstd responses are
enabled for clients that advertise support.

Each confirmed block update is retained as one independently decompressible
zstd level-1 record in the checksummed segmented WAL. The WAL is synced before
`raw-blocks.jsonl` advances; restart recovers an incomplete WAL tail,
reconciles the WAL/journal crash window, validates the authoritative tail,
continues frame IDs,
and requests the last durable slot inclusively. The exact prior block is skipped
after overlap is observed; a later first delivery emits a coverage warning.
`--resume-coverage-warning-file PATH` also atomically publishes a small
secret-free JSON event as soon as that warning is detected, allowing a
long-running supervisor to alert without waiting for recorder exit. `PATH` is
restricted to `OUTPUT_DIR/.monitoring/resume-coverage-warning.json`; the event
and directory are synced before the recorder may append the later block, and a
different pending event is never overwritten. The journal's durable tail always
controls resume after the first retained block; `--from-slot` is only a bootstrap
hint for an empty spool. The optional `--idle-timeout-secs` watchdog exits a
transport that has not durably appended a block within the configured interval
so an outer supervisor can reconnect. The journal records slot, parent,
WAL segment/offset/length, compressed and raw
lengths, blockhash, and the SHA-256 of the uncompressed protobuf envelope.
`--min-free-bytes` defaults to 16 GiB and stops cleanly before appending when
free space falls below that reserve; zero disables the guard.

The compressed payload is the full `SubscribeUpdate` represented by the
compiled Yellowstone schema, including its filters and creation timestamp,
rather than only the nested block. As with all prost/tonic clients, wire fields
unknown to the compiled protobuf schema cannot be preserved. Run this command
under a restart supervisor for transport failures or normal stream termination.
For a continuous bridge, omit `--stop-at-epoch-boundary`.

`--require-complete-poh` rejects a block before the WAL durability boundary
unless its embedded entries are non-empty, contiguous, correctly partition the
block's transactions, contain 32-byte hashes, and terminate at the block's
blockhash. These are structural reconstructability checks; they do not
cryptographically re-execute the PoH chain or prove arbitrary intermediate
hashes. `verify-grpc-raw-poh` locks and audits a stopped/snapshotted WAL, requires
at least one record by default, checks WAL/journal tail parity, and enforces the
same entry invariants. Use `--min-records 0` only for an intentional empty-spool
diagnostic. Both it and payload-validating `inspect-grpc-raw` require a stopped
recorder or filesystem snapshot/copy for a consistent full-spool result.

Capture live block updates with transactions and entries into the producer
layout:

```bash
BLOCKZILLA_GRPC_X_TOKEN=... \
  cargo run -p blockzilla-live-producer -- capture-grpc \
  --endpoint https://example.mainnet.rpcpool.com \
  --archive-dir blockzilla-live \
  --max-blocks 1000000 \
  --pubkey-index-mode runs \
  --pubkey-hot-registry blockzilla-v2/epoch-N-1/registry.bin \
  --pubkey-hot-count 1000 \
  --stop-at-epoch-boundary

cargo run -p blockzilla-live-producer -- inspect-capture \
  --archive-dir blockzilla-live
```

`capture-grpc` keeps Yellowstone's bidirectional request side open and answers application-level
pings. `--connect-timeout-secs` and `--subscribe-timeout-secs` bound setup, while
`--idle-timeout-secs` measures time since the last fully flushed block append; ping-only traffic
does not reset it. Before returning a non-boundary report, the capture flushes its block artifacts,
indexes, derived pubkey state, and journal. JSON reports use `outcome: "retryable"` with a stable
`retry_reason`; `permission_denied` and `unauthenticated` additionally set `action_required: true`
so a supervisor can alert once and use a longer retry delay. Only `outcome: "epoch_boundary"` with
`stopped_at_epoch_boundary: true` closes a capture.

The capture writes wincode/LEB128 framed records:

- `blocks/live-no-registry-blocks.bin`
- `blocks/grpc-raw-blocks.bin` (raw protobuf block payloads in wincode frames)
- `poh/poh.wincode`
- `index/block-index.bin`
- `index/blockhash_registry.bin`
- `index/signatures.bin`
- `index/signature-index.bin`
- `index/pubkey-counts.bin`
- `index/pubkey-touches.bin` when `--pubkey-index-mode touches` or
  `counts-and-touches` is used
- `index/pubkey-runs/*.bin` when `--pubkey-index-mode runs` or
  `counts-and-runs` is used. These are raw sorted `(pubkey[32], count:u32)`
  records. If `--pubkey-hot-registry` is supplied, `hot-run.bin` holds exact
  counts for the first `--pubkey-hot-count` pubkeys from that previous registry.
- `journal/grpc-blocks.jsonl`

Each journal row records the OF-compatible `epoch` and `epoch_slot_index`. With
`--stop-at-epoch-boundary`, capture stops before writing the first block from the
next epoch so an orchestrator can flush/finalize the previous epoch cleanly.

For fast low-memory capture, prefer `--pubkey-index-mode runs`. Capture
sorts/dedupes pubkeys per block, spills bounded sorted count chunks, and avoids
keeping the full live count map in memory. The optional hot-registry cache keeps
exact counts for the previous epoch's top accounts in a tiny in-memory map, which
reduces run volume without changing final registry correctness. The hot-block
finalizer can then build `registry.bin` by merging sorted runs:

```bash
cargo run --release -p blockzilla --bin blockzilla -- build-archive-v2-hot-blocks-from-live \
  blockzilla-live \
  blockzilla-v2/epoch-N \
  --registry-source runs
```

`--pubkey-index-mode touches` remains useful as a simpler comparison mode: it
appends every raw 32-byte pubkey touch and lets the finalizer perform the whole
external sort later with `--registry-source touches`.

## RPC getBlock backfill

Regular RPC is a degraded repair lane: it can recover block metadata,
transactions, transaction status, rewards, and signatures, but not full PoH
entries or shred boundary metadata. The raw payloads are therefore stored under
`repair/rpc-get-block` and should stay marked as repair data until CAR/live PoH
sidecars fill the missing fields.

```bash
cargo run -p blockzilla-live-producer -- backfill-rpc \
  --rpc-url https://api.mainnet-beta.solana.com \
  --archive-dir blockzilla-live \
  --start-slot 418176000 \
  --end-slot 418176010 \
  --rpc-rate-limit-per-sec 5
```

## Fixture benchmark

Replay a captured `blocks/grpc-raw-blocks.bin` fixture without touching the live
endpoint:

```bash
cargo run --release -p blockzilla-live-producer -- bench-fixture \
  --archive-dir blockzilla-v1/live-grpc-100 \
  --iterations 2 \
  --hash-backend all \
  --write-mode none
```

Approximate registry-order experiments can be layered onto the same run. This
keeps exact counts as ground truth, but also tracks a bounded SpaceSaving
heavy-hitter head and reports its exact varint-ID byte cost:

```bash
cargo run --release -p blockzilla-live-producer -- bench-fixture \
  --archive-dir blockzilla-v1/live-grpc-100 \
  --iterations 1 \
  --hash-backend gxhash-u32 \
  --heavy-hitter-capacity 32768,262144 \
  --write-mode none
```

Use `--write-mode archive --block-write-strategy all` to compare the allocating
wincode writer against the reusable scratch-buffer writer. Temporary output
defaults to `target/blockzilla-live-producer-bench`.
