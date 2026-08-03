# Hivezilla

Hivezilla is the live-input boundary for Blockzilla. Its V1 objective is to
preserve every admitted Solana record without silent loss; when configured
durability cannot be maintained, capture must backpressure or fail explicitly
and alert. Packets lost before admission and destruction of the sole pre-ACK
source copy remain explicit residual risks.
The code-backed [convergence architecture](../../docs/architecture/hivezilla-convergence.md)
and draft [record and sync protocol](../../docs/design/hivezilla-record-and-sync-protocol.md)
describe how the current source-specific paths converge into one hive.
The [implementation assessment](../../docs/architecture/hivezilla-v1-implementation-plan.md)
maps those contracts to the existing modules and ordered build gates.

Hivezilla is under active development. The repository currently includes:

- Yellowstone gRPC observation, capture, and durable raw recording;
- finalized Helius block recording through a tiny root WebSocket plus RPC catch-up;
- byte-for-byte Solana shred UDP recording into the common durable ingress spool;
- a checksummed, segmented spool with inspection, replay, repair, and
  materialization tools;
- mTLS receiver, push replication, pull replication, and receiver-to-spool
  bridging; and
- bounded supervision, disk admission, rotation, retention, and monitoring
  helpers.

Canonical multi-source selection and a continuous rooted shred-to-block
promotion path remain planned. Dynamic source failover, peer membership, and
peer custody transfer are deferred beyond V1. The generic `run` command still
supports `--dry-run` only; use the explicit commands below for implemented data
paths.

The minimal V1 target is also not implemented end to end yet. Each source node
owns a local logical spool plus its own private temporary cloud-overflow
namespace. A sealed upload with a provider-verified end-to-end checksum (or
verified read-back) that is catalogued durably may replace its local copy under
pressure, but only the one configured terminal raw consumer's cumulative
exact-prefix ACK makes the data eligible for cleanup. The source must durably
record that accepted ACK and its receipt-bound retirement checkpoint before
deleting from either tier. That consumer writes self-describing exact objects to enough
independently verified physical targets to satisfy the immutable durability
policy, plus a rebuildable range/copy index, before issuing its one logical ACK.
On
reconnect, it starts live at an atomic cutover `T` while separately budgeted
session-fenced idempotent range fetches download `[C, T)` from local disk or
cloud.

The current Yellowstone relay is an internal, source-specific operator tool;
it is not a public subscription boundary. The proposed isolated
[Hivezilla Exit protocol](../../docs/design/hivezilla-public-exit-protocol.md)
serves named exact raw-shred and derived block-observation streams with bounded
replay. It does not compact archives or allow subscribers to affect capture,
custody, or retention. The proposed
[node-role schema](../../docs/architecture/hivezilla-node-roles.md) separates
capture, terminal custody/download, processing, public exit, compact workers,
archive reads, and operations. Blockzilla remains the scheduler and canonical
catalog authority; a fenced Hivezilla compact worker performs the physical
Archive V2 build and upload.

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
| Retain | `record-grpc-raw`, `record-helius-blocks`, `record-shred-udp`, `inspect-grpc-raw`, `verify-grpc-raw-poh`, `verify-grpc-raw-ledger-shadow`, `materialize-grpc-raw` |
| Replicate | `serve-ingest-receiver`, `replicate-grpc-raw`, `pull-grpc-raw`, `serve-grpc-raw-pull-source`, `serve-shred-spool-pull-source`, `bridge-receiver-grpc-raw` |
| Capture | `capture-grpc`, `inspect-capture` |
| Repair | `sync-rpc-epoch`, `backfill-rpc`, `prepare-epoch-repair` |
| Operate | `serve-shred-reader`, `serve-shred-status`, `supervise`, `notify-supervisor` |

Use command-specific `--help` before starting a networked or disk-writing task.
Examples reference credentials through environment variables or files; do not
place secret values in them.

### Offline ledger projection shadow

`verify-grpc-raw-ledger-shadow` audits the raw WAL and projects every block in
memory into the unpromoted ledger-candidate shape:

```bash
cargo run --locked -p hivezilla -- \
  verify-grpc-raw-ledger-shadow \
  --output-dir /path/to/stopped-or-snapshotted-grpc-raw \
  --min-records 1
```

Success requires `candidates_projected == records_scanned`. Projection results
are reported as deterministic counters for candidates, signed transactions,
signatures, signed-message bytes, PoH entries, and any incomplete WAL tail. The
command does not write an artifact, publish a candidate, claim finality, advance
an ACK, or authorize spool deletion. The existing PoH verifier and its report
remain unchanged.

The pinned Yellowstone 12.4 schema cannot retain Agave V1's transaction-config
field. The shadow therefore requires every `versioned = true` transaction's
fee-payer signature to prove the reconstructed V0 bytes and fails closed on a
V1 or otherwise ambiguous row. This is an alerting/cutover blocker, not a way to
recover the field already dropped by known-schema raw capture.

### Helius finalized block recorder

Helius does not support Solana's unstable `blockSubscribe`. Hivezilla therefore
uses `rootSubscribe` only as a low-volume wake-up signal, fetches finalized
blocks with `getBlock`, and uses `getBlocks` for batched catch-up and skipped-slot
confirmation. The API key is read from a file and is never included in command
output, cursor state, or stored blocks.

Run a bounded canary before starting a supervised recorder:

```bash
cargo run --locked -p hivezilla -- \
  record-helius-blocks \
  --api-key-file /path/to/helius_api.txt \
  --output-dir /path/to/blockzilla-helius-raw \
  --max-blocks 10 \
  --timeout-secs 120
```

Each block is atomically published as a zstd-compressed raw `getBlock` JSON
document under `blocks/epoch-N/shard-N/`. A synced coverage journal records both
produced and skipped finalized slots before `cursor.json` advances. Reusing the
same output directory resumes from that cursor and reuses already published
block files after an interrupted batch.

The report includes observed HTTP/WebSocket bytes and an estimated Helius credit
count. Normal live-head operation costs approximately one standard RPC call per
finalized slot; reconnects, catch-up range queries, and skipped-slot checks add a
small overhead. These RPC blocks contain transactions and rewards but no Solana
PoH entries. They are a transaction/block backup source, not an entry-complete
replacement for the Yellowstone raw spool.

### Shred UDP recorder

`record-shred-udp` selects one enabled `shred_udp` source from a schema-v2
ingest config, parses the stable Solana common shred header, and syncs the exact
datagram through `SpoolWriter` before accepting the next observation. Transport
duplicates are preserved. Raw unauthenticated UDP must stay on loopback or a
trusted private network; authenticated envelopes are not implemented yet.

IPv4 multicast sources may select their local interface by concrete address or
by a bounded interface name such as `doublezero1`. DoubleZero's IBRL routing and
its direct Edge multicast feed are separate operating modes; see
the [DoubleZero section in the live archive producer guide](../../docs/guides/live-archive-producer.md#doublezero--shred-stream)
before
connecting either mode.

```bash
hivezilla record-shred-udp \
  --config services/hivezilla/config/ingest-shred-udp.example.json \
  --source-id shred-reader-loopback \
  --journal-id 0123456789abcdef0123456789abcdef \
  --status-file /var/lib/hivezilla-status/recorder.json
```

Keep the journal ID stable with the spool volume across restarts; create a new
one only for a deliberately new physical journal. The optional status file is
atomically replaced every five seconds and contains only post-`fsync` counters,
durable sequence, freshness, and storage capacity. It must live outside the
quota-accounted spool.

`hivezilla serve-shred-status` combines that file with a shred-reader loopback
metrics endpoint and writes an atomic, public-safe snapshot:

```bash
hivezilla serve-shred-status \
  --listen 127.0.0.1:18790 \
  --hivezilla-status-file /recorder-status/recorder.json \
  --receiver-metrics-url http://127.0.0.1:19090/metrics \
  --output-file /public-status/public.json \
  --cors-origin https://watcher.blockzilla.dev
```

The supplied `docker-compose.hivezilla-shred.dokploy.yml` keeps metrics on host
loopback, mounts recorder status read-only into the Rust collector, and exposes
only the sanitized JSON through a separate read-only web container.

`shred-reader` itself is now merged into the Hivezilla binary; use
`hivezilla serve-shred-reader` (with environment variables described in
`services/hivezilla/config/shred-reader.env.example`) to run the same gossip/TVU runtime without
a separate service process.

This is a bounded raw shred recorder, not block reconstruction or indexing. Its
example spool fails closed at 20 GiB, and the recorder itself never deletes.
The raw NAS replication path is documented in
[the live ingest and storage design note](../../docs/architecture/live-ingest-and-storage.md):
the NAS opens an mTLS pull connection to Hetzner and writes the same compressed
shred records to its own durable spool. In that legacy deployment, after the
exact NAS **staging ACK** crosses the source's cumulative-ACK WAL fsync boundary,
the pull source may retire only sealed segments strictly before the retained
ACK-anchor segment. This is not a V1 custody ACK and must not enable V1 GC. An
unsigned, unpersisted, mismatched, or merely visible ACK never enables deletion.

The `scripts/` directory contains portable launch, PKI, object-storage, and
monitoring helpers. They intentionally contain no deployment manifest or real
host topology. See [scripts/README.md](scripts/README.md) before using them.

`record-grpc-raw` currently performs bounded source-process recovery: it
validates the handoff journal tail, that row's exact WAL frame, and the active
WAL segment. It does not rescan sealed historical segments on reconnect. After
stopping or rotating a raw capture, an operator must run the full offline audit
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

The implemented pre-V1 replication path can retire source segments after the
receiver stores a prefix and the source persists its signed acknowledgement.
That acknowledgement is a **legacy staging receipt**, not a V1 custody ACK;
its GC path must be disabled before the source joins a V1 deployment. In the V1
target, a provider-verified end-to-end checksum (or verified read-back) plus the
durable range catalog may authorize eviction of that local copy, but it never
retires the logical data. Only an ACK from the configured terminal raw dataset
makes covered data eligible, after verified permanent exact-raw objects satisfy
the independent-target policy and their rebuildable range/copy index is
durable. The source then fsyncs the accepted-ACK receipt and its receipt-bound
retirement checkpoint before deleting local or overflow copies.
Derived blocks, compact jobs, Archive V2 commits, and public subscribers cannot
send that ACK.

The durability model and unfinished work are described in the
[live-ingest design](../../docs/design/live-ingest-redundancy.md). Keep raw
spools until downstream storage is independently verified, and never commit
provider URLs, tokens, captures, journals, or incident artifacts. See the
repository [security policy](../../SECURITY.md).
