# Hivezilla

## Purpose

Hivezilla is the live-input boundary in the target Blockzilla architecture. Its
job is to retain network data durably so a Blockzilla storage server can recover
after an outage instead of depending on one uninterrupted stream.

The Cargo package and executable are both named `hivezilla`.

## Status

Hivezilla is a prototype, not a production ingestion service. The repository
contains working gRPC probes and capture paths, a checksummed raw spool,
inspection and materialization commands, configuration validation, RPC repair
helpers, and deterministic ingest primitives.

The following target behavior is not complete:

- automatic failover across independently operated Hivezilla instances;
- a production shred-stream adapter;
- automatic delivery acknowledgement and retention cleanup after Blockzilla has
  committed data;
- a stable compact network-block protocol; and
- production orchestration, alerting, and upgrade procedures.

The generic `run` command only supports `--dry-run`; a non-dry run intentionally
fails until a real source adapter owns that path.

## Build and inspect

```bash
cargo build --locked -p hivezilla
cargo run --locked -p hivezilla -- --help
```

Two useful offline checks require no provider credentials:

```bash
cargo run --locked -p hivezilla -- \
  validate-ingest-config \
  --config hivezilla/config/ingest-primary.example.json

cargo run --locked -p hivezilla -- \
  plan-epoch-backfill \
  --epoch 900 \
  --observed-range 388800000-389231999
```

The example configurations contain secret *references*, never literal secret
values. Use command-specific help before running a network or disk-writing
command:

```bash
cargo run --locked -p hivezilla -- probe-grpc --help
cargo run --locked -p hivezilla -- record-grpc-raw --help
cargo run --locked -p hivezilla -- capture-grpc --help
```

## Command groups

| Area | Commands | Status |
| --- | --- | --- |
| Local planning | `init`, `plan`, `run --dry-run`, `plan-epoch-backfill` | Prototype, safe to explore offline. |
| Configuration | `validate-ingest-config` | Implemented validation with redacted summaries. |
| gRPC observation | `probe-grpc`, `watch-epochs-grpc` | Network-dependent prototype. |
| Raw retention | `record-grpc-raw`, `inspect-grpc-raw`, `verify-grpc-raw-poh`, `materialize-grpc-raw` | Implemented prototype; requires careful disk and lifecycle management. |
| Live capture | `capture-grpc`, `inspect-capture` | Prototype input for Blockzilla's live finalizer. |
| Repair | `sync-rpc-epoch`, `backfill-rpc`, `prepare-epoch-repair` and backfill helpers | Advanced repair tooling, not the default path. |
| Benchmarking | `bench-fixture` | Offline developer benchmark. |

## Source map

| Path | Responsibility |
| --- | --- |
| `src/main.rs` | CLI definition and dispatch. |
| `src/app.rs`, `src/config.rs`, `src/source.rs` | Application plan, configuration, and source selection. |
| `src/grpc.rs` | Yellowstone gRPC probes and normalized capture. |
| `src/grpc_raw.rs` | Raw gRPC WAL recording, inspection, verification, and materialization. |
| `src/ingest/` | Multi-source configuration, deduplication, replication receipts, and spool primitives. |
| `src/repair.rs`, `src/rpc.rs` | Repair planning and regular RPC fallback. |
| `config/` | Secret-free example configurations. |

The durability design and its unresolved rollout work are documented in
[`docs/design/live-ingest-redundancy.md`](../docs/design/live-ingest-redundancy.md).

## Validation

```bash
cargo fmt --all -- --check
cargo check --locked -p hivezilla --all-targets
cargo test --locked -p hivezilla --all-targets
```

## Safety and security

Live capture is a networked, long-running, disk-writing workload. Use a test
endpoint first, configure a free-space reserve, keep raw spools until downstream
commit is independently verified, and supervise every prototype process.

Keep Yellowstone tokens and RPC credentials outside configuration files and
shell history. Never commit captures, journals, provider URLs containing keys,
or incident artifacts. See the repository [security policy](../SECURITY.md).
