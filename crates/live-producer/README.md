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

The initial implementation also provides source-independent block/entry/shred
identities, deterministic duplicate/conflict/fork classification, a segmented
raw spool that recomputes content digests, syncs each committed checksummed
frame, validates sealed segments, and truncates only incomplete crash tails, and
a checksummed receipt WAL. Both WALs enforce one writer and become fail-stop
after ambiguous I/O errors. A network response alone cannot make replica data
eligible for garbage collection: the exact signed receipt must first be verified
and synced into the local receipt WAL. There is intentionally no segment unlink
API until a sealed-segment ACK manifest can prove every frame is eligible.

These primitives are not wired into the production socket adapters yet. The
current `capture-grpc` process must not be replaced until replay, archive-writer
recovery, and power-loss fault tests are complete. See
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

## Atomic multi-capture epoch repair view

`prepare-epoch-repair` consolidates audited slices without copying their large
normalized block files. It hard-links each block source and prebuilt bounded
pubkey runs, merges the small index/PoH/blockhash/journal streams in slot order,
deduplicates compatible overlap, rewrites retained PoH records to produced
ordinals (with explicit gaps for RPC-only blocks), writes every live and RPC
blockhash in produced order, and hard-links deeply validated RPC `getBlock`
sidecars. The output directory is renamed into place only after exact counts,
the produced parent chain, source fingerprints, hard-link identity, and durable
input receipts all pass. A failure removes staging and never creates the target
directory.

For the audited epoch-1000 cutover, the command shape is:

```bash
blockzilla-live-producer prepare-epoch-repair \
  --capture-dir "$EARLY_CAPTURE" \
  --capture-blocks 153234 \
  --capture-max-slot 432167486 \
  --capture-pubkey-runs "$EARLY_CAPTURE/index/pubkey-runs" \
  --capture-journal "$EARLY_CAPTURE/journal/grpc-blocks.jsonl" \
  --capture-sealed-marker "$EARLY_CAPTURE/CAPTURE-SEALED.json" \
  --capture-dir "$LATE_CAPTURE" \
  --capture-blocks 264316 \
  --capture-max-slot 432431999 \
  --capture-pubkey-runs "$EPOCH1000_FINALIZER_VIEW/index/pubkey-runs" \
  --capture-journal "$LATE_CAPTURE/journal/grpc-blocks.jsonl" \
  --capture-sealed-marker "$LATE_CAPTURE/CAPTURE-SEALED.json" \
  --rpc-repair-dir "$RPC_EPOCH1000_DIR" \
  --rpc-complete-marker "$RPC_RUN_COMPLETE" \
  --output-dir "$EPOCH1000_REPAIR_VIEW" \
  --epoch 1000 \
  --expected-live-blocks 417550 \
  --expected-rpc-blocks 14231 \
  --expected-duplicate-live-blocks 0
```

Every source must be stopped and immutable before its sealed marker is created;
the RPC completion marker must be created only after all writers have exited.
The command snapshots and later revalidates size, mtime, device/inode, and a
SHA-256 digest for bounded inputs. The default RPC JSON ceiling is 32 MiB; each
base64 payload must be canonical and decode as a Solana wire transaction.

All capture/run/RPC paths and the output must be on the same filesystem because
hard-link failure is fatal; there is deliberately no silent hundreds-of-GiB
copy fallback. The output and hidden staging path may neither contain nor be
nested in any input path. The result contains `REPAIR-REQUIRED.json`, never
`READY`, and deliberately does not publish canonical `poh/poh.wincode`,
`index/blockhash_registry.bin`, or `index/pubkey-runs` paths. Instead it contains
`repair/available-poh.wincode`, `repair/produced-blockhashes.bin`, and
`repair/live-pubkey-runs`.

RPC-only blocks remain sidecars with explicit missing-PoH and missing-shredding
status. Hard-linked normalized frames retain source-local current and previous
blockhash IDs. The current hot finalizer cannot consume this bundle: a future
repair-aware materializer must remap both IDs, insert the RPC blocks, and rebuild
canonical sidecars. Reused pubkey runs cover live blocks only, so RPC pubkeys
must also be extracted before registry construction. After the repair view is
verified, its hard-linked normalized blocks, runs, and RPC files no longer
depend on the corresponding original directory entries. Raw protobuf, source
journals, non-selected tail indexes/PoH, and shred inputs are outside that
cleanup guarantee and must be retained separately. The linked block blob can
physically include tail bytes, but the repair plan does not index or guarantee
those tail records.

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

# The recorder must be stopped, rotated, or represented by a filesystem snapshot.
cargo run --release -p blockzilla-live-producer -- materialize-grpc-raw \
  --input-dir /path/with/free-space/mac-bridge \
  --archive-dir /path/to/live/epoch-1002-raw-slice \
  --epoch 1002
```

Each confirmed block update is retained as one independently decompressible
zstd level-1 record in the checksummed segmented WAL. The WAL is synced before
`raw-blocks.jsonl` advances; restart recovers an incomplete WAL tail,
reconciles the WAL/journal crash window, validates the authoritative tail,
continues frame IDs, and requests the last durable slot inclusively. The exact
prior block is skipped after overlap is observed; a later first delivery emits a
coverage warning. `--resume-coverage-warning-file PATH` also atomically publishes
a small secret-free JSON event as soon as that warning is detected, allowing a
long-running supervisor to alert without waiting for recorder exit. `PATH` is
restricted to `OUTPUT_DIR/.monitoring/resume-coverage-warning.json`; the event
and directory are synced before the recorder may append the later block, and a
different pending event is never overwritten. The journal's
durable tail always controls resume after the
first retained block; `--from-slot` is only a bootstrap hint for an empty spool. The optional
`--idle-timeout-secs` watchdog exits a transport that has not durably appended a
block within the configured interval so an outer supervisor can reconnect. The
journal records slot, parent, WAL segment/offset/length, compressed and raw
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

`materialize-grpc-raw` is the scheduler-facing, network-free conversion boundary.
It holds the raw WAL's exclusive writer lock throughout a bounded-memory replay,
selects the requested epoch rows, builds pubkey runs and the other capture
sidecars in a fresh sibling `.raw-materializing` directory, validates and syncs
them, then publishes the capture with one rename. An active recorder is rejected
before staging is created. The source WAL is opened read-only and is never
deleted; the published receipt binds the generated artifacts to SHA-256 hashes
of the source identity and handoff journal.

A stopped spool proves only a stable committed snapshot, not a complete epoch.
The command therefore creates neither `READY-TO-PACKAGE` nor `FINALIZE-NEXT.md`.
Hivezilla must keep the materialized slice repair-gated until a separate epoch
boundary, slot-coverage, and missing-field audit grants approval. If conversion
fails after its fresh staging directory was created, that directory is retained
for inspection; remove only that deterministic staging sibling after verifying
its ownership before retrying. Never remove the source WAL as retry cleanup.

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
