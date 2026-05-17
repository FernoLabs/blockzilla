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

## gRPC capture

Capture live block updates with transactions and entries into the producer
layout:

```bash
BLOCKZILLA_GRPC_X_TOKEN=... \
  cargo run -p blockzilla-live-producer -- capture-grpc \
  --endpoint https://example.mainnet.rpcpool.com \
  --archive-dir blockzilla-live \
  --max-blocks 1000000 \
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
- `journal/grpc-blocks.jsonl`

Each journal row records the OF-compatible `epoch` and `epoch_slot_index`. With
`--stop-at-epoch-boundary`, capture stops before writing the first block from the
next epoch so an orchestrator can flush/finalize the previous epoch cleanly.

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

Use `--write-mode archive --block-write-strategy all` to compare the allocating
wincode writer against the reusable scratch-buffer writer. Temporary output
defaults to `target/blockzilla-live-producer-bench`.
