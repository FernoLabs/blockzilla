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
The [live relaunch review](../../docs/architecture/hivezilla-live-relaunch.md)
sets the current target: one Hetzner reader, one NAS custody consumer, shared
live subscriptions for other projects, and Blockzilla reconciliation,
compaction, and finalized block backfill on the NAS.

The [local validator execution test](EXECUTION-CANARY.md) uses a separate
current-schema stream with finalized blocks from a self-operated validator.
It retains raw execution evidence and builds a verified Compact V2 test
archive. Its output is explicitly noncanonical. See the
[validator operations guide](../../docs/architecture/hivezilla-live-canary-operations.md)
for the bounded node test and local control commands.
The [validator dashboard](../dashboard/README.md) is a loopback-only Topcoat
page for the canary report and validator metrics.
The planned [Hivezilla-owned Geyser plugin](../../docs/architecture/hivezilla-geyser-plugin-v1.md)
will first run beside Yellowstone and write a separate durable local stream.

Hivezilla is under active development. The repository currently includes:

- Yellowstone gRPC observation, capture, and durable raw recording;
- finalized Helius block recording through a tiny root WebSocket plus RPC catch-up;
- byte-for-byte Solana shred UDP recording into the common durable ingress spool;
- a checksummed, segmented spool with inspection, replay, repair, and
  materialization tools;
- mTLS receiver, push replication, pull replication, and receiver-to-spool
  bridging;
- bounded supervision, disk admission, rotation, retention, and monitoring
  helpers; and
- a durable V1 overflow range catalog, an R2 backend, and explicit commands
  for verified upload and recovery of sealed segments, including bounded
  conversion of committed gRPC journals. Automatic spill and NAS recovery
  remain pending.

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
archive reads, and operations. The relaunch puts reconciliation and Compact V2
building in Blockzilla on the NAS. Hivezilla may prepare data tied to its source.
Ordinary subscribers use Blockzilla for finalized block backfill; Compact V2
cannot replace exact raw-event replay. These subscription and backfill runtimes
remain to be implemented.

## Try it

Inspect the CLI and validate the secret-free example configuration:

```bash
cargo run --locked -p hivezilla -- --help

cargo run --locked -p hivezilla -- \
  validate-ingest-config \
  --config hivezilla/service/config/ingest-primary.example.json
```

The main command groups are:

| Area | Commands |
| --- | --- |
| Observe | `probe-grpc`, `watch-epochs-grpc` |
| Retain | `record-grpc-raw`, `record-helius-blocks`, `record-shred-udp`, `inspect-grpc-raw`, `verify-grpc-raw-poh`, `verify-grpc-raw-ledger-shadow`, `materialize-grpc-raw` |
| Replicate | `serve-ingest-receiver`, `replicate-grpc-raw`, `pull-grpc-raw`, `serve-grpc-raw-pull-source`, `serve-shred-spool-pull-source`, `bridge-receiver-grpc-raw` |
| Capture | `capture-grpc`, `inspect-capture` |
| Repair | `sync-rpc-epoch`, `backfill-rpc`, `prepare-epoch-repair` |
| Overflow | `spill-grpc-raw-overflow`, `upload-overflow-segment`, `recover-overflow-segment`, `inspect-overflow-index` |
| Operate | `serve-shred-reader`, `serve-shred-status`, `supervise`, `notify-supervisor` |

Use command-specific `--help` before starting a networked or disk-writing task.
Examples reference credentials through environment variables or files; do not
place secret values in them.

### Overflow catalog inspection

Read an existing V1 overflow catalog without object-store access:

```bash
cargo run --locked -p hivezilla -- \
  inspect-overflow-index --state-dir /path/to/overflow-state
```

JSON output lists each reader and stream, pending ranges, verified ranges and
bytes, and the contiguous verified record prefix. The prefix stops at a gap or
an unverified upload. It is not a NAS ACK, a current remote-availability check,
or proof of complete blocks. Inspection does not initialize missing state.

The catalog persists upload intents before remote writes and verifies the
stored bytes before it records a receipt. Recovery rechecks object identity,
length, version, checksum, and the complete record chain. The library supports
bounded range pages for restart recovery and refuses overlapping or conflicting
ranges. It cannot delete data or advance a consumer ACK. Source journal
adapters and the continuous spill/recovery loop are not wired to this catalog
yet.

### Bounded R2 overflow transfer

Upload one **sealed HIVESEG1 segment**, then recover its exact bytes into a new
local file. These commands do not accept legacy recorder WAL files. They use
the existing uploader's R2 settings file or environment variables.

```bash
cargo run --locked -p hivezilla -- \
  upload-overflow-segment \
  --state-dir /path/to/overflow-state \
  --credentials-file /path/to/r2.env \
  --remote-prefix hivezilla/hetzner-1 \
  --reader-id hetzner-1 \
  --segment /path/to/sealed-segment.hseg

cargo run --locked -p hivezilla -- \
  recover-overflow-segment \
  --state-dir /path/to/overflow-state \
  --credentials-file /path/to/r2.env \
  --remote-prefix hivezilla/hetzner-1 \
  --reader-id hetzner-1 \
  --stream-id STREAM_ID_FROM_UPLOAD_REPORT \
  --sequence RECORD_SEQUENCE_FROM_UPLOAD_REPORT \
  --output /existing/directory/recovered.hseg
```

The upload report includes `stream_id`, `first_sequence`, and `next_sequence`.
Choose a record sequence in that interval. Recovery requires the existing
catalog; it does not discover objects by listing the bucket. This is a manual
transfer path, not the NAS live consumer or a deployed spill service.

The backend reuses the signed S3 client and conditional writes documented in
[R2 S3 compatibility](https://developers.cloudflare.com/r2/api/s3/api/).
It does a full SHA-256 read-back before the catalog records verification.
User-supplied checksum metadata and ETags do not replace that check. Transfers
are limited to 256 MiB per segment by default and can use a smaller
`--max-segment-bytes` value. The limit bounds encoded bytes, not total process
memory; decoding and upload buffers also use memory. HTTP retries share one
immutable body buffer. Network I/O runs in a blocking worker.

The catalog namespace binds the endpoint, bucket, and prefix. Credentials may
rotate without changing it. A lost upload reply leaves a pending intent; rerun
the same upload to verify the existing object without replacing it. Failed
uploads preserve the source file. Failed recovery does not publish a partial
output; successful recovery syncs the new file and its directory. Existing
output files are never replaced. These commands perform no ACK or deletion.

### Committed gRPC journal to R2

`spill-grpc-raw-overflow` reads committed records from the live recorder. It
keeps each compressed payload unchanged and maps each logical source sequence
to the same V1 record sequence. It does not sort, deduplicate, or select blocks.
Blockzilla owns those decisions.

```bash
cargo run --locked -p hivezilla -- \
  spill-grpc-raw-overflow \
  --state-dir /path/to/overflow-state \
  --credentials-file /path/to/r2.env \
  --remote-prefix hivezilla/hetzner-1 \
  --reader-id hetzner-1 \
  --manifest /path/to/trusted-stream.manifest \
  --cache-root /path/to/grpc-hot-cache \
  --max-records-per-segment 256 \
  --max-segments 4
```

Use `--source-dir` instead of `--cache-root` for one generation. The cache-root
path follows validated physical rotations within the manifest's logical source
stream. Copied boundary records are not repeated. Other logical streams need
their own manifests and spill passes.

The manifest must be a canonical binary `StreamManifestV1` from trusted stream
configuration. Use V1 payload format **1/version 1** for this adapter; the
legacy recorder's format number **2** means gRPC, while V1 format **2** means
raw shreds. Build the producer descriptor with
`overflow::grpc::GrpcRawOverflowDescriptor::new(source, max_decoded_bytes)`
and its `encode()` method. Bind the actual logical `ReplicationStreamId`,
cluster genesis, R2 namespace, gap stream, and terminal durability policy in
the manifest. This command does not create a policy or a stream registry.
Manifest provisioning remains a deployment requirement. Do not use example
identities or a placeholder manifest hash for production.

The descriptor pins the Yellowstone 12.7.0 protobuf schema and the admitted
field set. The adapter checks exact protobuf decode/encode equality, block
identity, hashes, and one complete zstd frame. Unknown fields, unsupported
transaction config, extra zstd frames, or corruption stop conversion. Stored
compressed bytes are never recompressed; the legacy recorder did not persist
its original compression settings. The manifest is uploaded and verified
before any segment.

One pass handles at most four segments by default, with at most 256 records
per segment. Both count limits can be set to 1..=1024. Encoded segments use the
R2 byte limit above. Decoded input is limited to 512 MiB per segment, with a
per-record limit in the immutable producer descriptor of at most 128 MiB.
These limits do not represent peak process memory.

Rerun the command to resume from the catalog. An unfinished upload retains its
original segment boundary and checksum, even when the journal grows or the
record-count setting changes. A missing or changed source range stops the
pass. An incomplete handoff tail waits for the writer to commit it. The JSON
report includes `next_sequence`, `verified_records`, and
`incomplete_handoff_tail_bytes`. `reached_snapshot_end` refers only to the
selected generation snapshots; a live writer can append immediately afterward.

Source journals remain intact. This command supplies a bounded gRPC spill
pass, not an automatic spill controller, a shred adapter, or NAS reconciliation.
Disable legacy source cleanup when using this V1 path until the V1 custody
policy and recovery process are in place.

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

Yellowstone 12.7 retains Agave V1's transaction-config field. The current raw
stream rejects that field before encoding and at decode/recovery boundaries;
payload format 2 and identity schema 1 keep their existing supported block
schema. V1 capture requires a separate stream schema. The ledger shadow also
rejects a present config. For config-absent versioned messages, it still requires
the fee-payer signature to prove the reconstructed V0 bytes. Older 12.4 captures
cannot recover a field that their known-schema decoder already dropped.

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
  --config hivezilla/service/config/ingest-shred-udp.example.json \
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
`hivezilla/service/config/shred-reader.env.example`) to run the same gossip/TVU runtime without
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
provider URLs, tokens, captures, journals, or incident artifacts.
