# Hivezilla

Hivezilla is the live-input boundary for Blockzilla. It captures Solana network
data and retains a recoverable raw copy so storage outages do not create gaps.

Hivezilla is under active development. The repository currently includes:

- Yellowstone gRPC observation, capture, and durable raw recording;
- a checksummed, segmented spool with inspection, replay, repair, and
  materialization tools;
- mTLS receiver, push replication, pull replication, and receiver-to-spool
  bridging; and
- bounded supervision, disk admission, rotation, retention, and monitoring
  helpers.

Canonical multi-source selection, automatic multi-instance failover, and shred
capture remain planned. The generic `run` command still supports `--dry-run`
only; use the explicit commands below for implemented data paths.

## Try it

Inspect the CLI and validate the secret-free example configuration:

```bash
cargo run --locked -p hivezilla -- --help

cargo run --locked -p hivezilla -- \
  validate-ingest-config \
  --config services/hivezilla/config/ingest-primary.example.json
```

The main command groups are:

| Area | Commands |
| --- | --- |
| Observe | `probe-grpc`, `watch-epochs-grpc` |
| Retain | `record-grpc-raw`, `inspect-grpc-raw`, `verify-grpc-raw-poh`, `materialize-grpc-raw` |
| Replicate | `serve-ingest-receiver`, `replicate-grpc-raw`, `pull-grpc-raw`, `serve-grpc-raw-pull-source`, `bridge-receiver-grpc-raw` |
| Capture | `capture-grpc`, `inspect-capture` |
| Repair | `sync-rpc-epoch`, `backfill-rpc`, `prepare-epoch-repair` |
| Operate | `supervise`, `notify-supervisor` |

Use command-specific `--help` before starting a networked or disk-writing task.
Examples reference credentials through environment variables or files; do not
place secret values in them.

The `scripts/` directory contains portable launch, PKI, object-storage, and
monitoring helpers. They intentionally contain no deployment manifest or real
host topology. See [scripts/README.md](scripts/README.md) before using them.

`record-grpc-raw` performs bounded live recovery: it validates the handoff
journal tail, that row's exact WAL frame, and the active WAL segment. It does
not rescan sealed historical segments on reconnect. After stopping or rotating
a raw capture, the Blockzilla maintenance task must run the full offline audit
before materialization or retention cleanup:

```bash
hivezilla inspect-grpc-raw \
  --output-dir /path/to/stopped-or-snapshotted-capture \
  --verify-payloads
```

The full audit holds the writer lock and checks all sealed WAL segments, every
handoff row, payload checksums, and protobuf decoding. Run it only against a
stopped capture or an immutable filesystem snapshot.

Long-lived source processes can use Hivezilla's portable supervisor instead of
depending on systemd or Docker for restart policy:

```bash
hivezilla supervise \
  --name mainnet-grpc-a \
  --state-dir /var/lib/blockzilla/supervisor/mainnet-grpc-a \
  --restart on-failure \
  --restart-burst 4 \
  --restart-window-secs 120 \
  -- \
  hivezilla record-grpc-raw [source arguments]
```

Rapid failures are fenced as `crash_loop`; they are never retried forever.
Optional tokenized readiness and heartbeat notifications are described in the
[portable-supervisor design](../../docs/design/portable-supervisor.md).

Replication advances cleanup authority only after the receiver has durably
stored the exact prefix and the source has durably recorded the matching signed
acknowledgement. Object-store upload receipts alone never authorize deletion.

The durability model and unfinished work are described in the
[live-ingest design](../../docs/design/live-ingest-redundancy.md). Keep raw
spools until downstream storage is independently verified, and never commit
provider URLs, tokens, captures, journals, or incident artifacts. See the
repository [security policy](../../SECURITY.md).
