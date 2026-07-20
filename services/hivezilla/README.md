# Hivezilla

Hivezilla is the live-input boundary for Blockzilla. It captures Solana network
data and retains a recoverable raw copy so storage outages do not create gaps.

Hivezilla is under active development. The repository currently includes:

- Yellowstone gRPC observation, capture, and durable raw recording;
- byte-for-byte Solana shred UDP recording into the common durable ingress spool;
- a checksummed, segmented spool with inspection, replay, repair, and
  materialization tools;
- mTLS receiver, push replication, pull replication, and receiver-to-spool
  bridging; and
- bounded supervision, disk admission, rotation, retention, and monitoring
  helpers.

Canonical multi-source selection, automatic multi-instance failover, and shred
reconstruction remain planned. The generic `run` command still supports `--dry-run`
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
| Retain | `record-grpc-raw`, `record-shred-udp`, `inspect-grpc-raw`, `verify-grpc-raw-poh`, `materialize-grpc-raw` |
| Replicate | `serve-ingest-receiver`, `replicate-grpc-raw`, `pull-grpc-raw`, `serve-grpc-raw-pull-source`, `bridge-receiver-grpc-raw` |
| Capture | `capture-grpc`, `inspect-capture` |
| Repair | `sync-rpc-epoch`, `backfill-rpc`, `prepare-epoch-repair` |
| Operate | `supervise`, `notify-supervisor` |

Use command-specific `--help` before starting a networked or disk-writing task.
Examples reference credentials through environment variables or files; do not
place secret values in them.

The first shred adapter is intentionally narrow: `record-shred-udp` selects one
enabled `shred_udp` source from the schema-v2 ingest config, parses the stable
Solana common shred header, and syncs the exact datagram through `SpoolWriter`
before accepting the next observation. Transport duplicates are preserved.
Raw unauthenticated UDP should remain on loopback or a trusted private network;
the adapter rejects configured authentication until a versioned authenticated
envelope exists.

```bash
hivezilla record-shred-udp \
  --config services/hivezilla/config/ingest-shred-udp.example.json \
  --source-id shred-reader-loopback \
  --journal-id 0123456789abcdef0123456789abcdef \
  --status-file /var/lib/hivezilla-status/recorder.json
```

Keep the journal id stable with the spool volume across restarts. Generate a
new id whenever a new physical journal is intentionally created. The optional
status file is replaced atomically every five seconds and after a clean stop.
It contains post-`fsync` counters, the durable sequence, freshness timestamps,
and storage capacity only; it must live outside the quota-accounted spool.

`scripts/shred_status_server.py` can combine that file with a shred-reader
loopback metrics endpoint. It selects a fixed, public-safe schema and can write
an atomic snapshot for a separate read-only web container. The supplied
`docker-compose.hivezilla-shred.dokploy.yml` uses this split so the metrics
source stays on host loopback, the collector mounts recorder status read-only,
and the public container can expose only the separate sanitized JSON volume.

This deployment is a bounded raw shred recorder, not block reconstruction or
indexing. Its example spool fails closed at 20 GiB and has no deletion policy;
capacity or a verified downstream replication/retention path must be added for
indefinite operation.

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
