# Hivezilla

Hivezilla is the live-input boundary for Blockzilla. It captures Solana network
data and retains a recoverable raw copy so storage outages do not create gaps.

Hivezilla is currently a prototype. It includes:

- Yellowstone gRPC observation and capture;
- a checksummed raw spool with inspection and materialization tools; and
- configuration validation and RPC repair helpers.

Multi-instance failover, shred capture, downstream commit acknowledgement, and
automatic retention cleanup are not production-ready. The generic `run`
command supports `--dry-run` only.

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
| Capture | `capture-grpc`, `inspect-capture` |
| Repair | `sync-rpc-epoch`, `backfill-rpc`, `prepare-epoch-repair` |

Use command-specific `--help` before starting a networked or disk-writing task.
Examples reference credentials through environment variables or files; do not
place secret values in them.

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

The durability model and unfinished work are described in the
[live-ingest design](../../docs/design/live-ingest-redundancy.md). Keep raw spools
until downstream storage is independently verified, and never commit provider
URLs, tokens, captures, journals, or incident artifacts. See the repository
[security policy](../../SECURITY.md).
