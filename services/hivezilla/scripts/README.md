# Hivezilla operational helpers

These helpers package the durable raw-capture primitives without encoding a
specific server, provider account, or deployment system.

They implement the current generation-based recorder and replication path.
They do not yet implement the proposed V1 live-first/two-lane catch-up or the
complete node-owned cloud-overflow lifecycle.

| Helper | Purpose |
| --- | --- |
| `linux-raw-grpc-recorder.sh` | Supervise bounded raw recording, generation rotation, optional object-store spill, safe cleanup, and alerts |
| `run-grpc-raw-receiver.sh` | Start the mTLS durable receiver |
| `run-grpc-raw-replicator.sh` | Push sealed and active raw generations to a receiver |
| `run-grpc-raw-pull-source.sh` | Expose a durable source through the bounded pull protocol |
| `run-grpc-raw-pull-client.sh` | Pull source-selected batches into the local receiver |
| `run-grpc-receiver-bridge.sh` | Copy a receiver's durable prefix into a standard raw generation without mutating the receiver tree |
| `generate-grpc-replication-pki.sh` | Create an offline CA and push-replication identities |
| `generate-grpc-pull-pki.sh` | Add pull identities to an existing replication trust bundle |
| `s3_multipart_upload.py` | Upload and verify bounded generations in an S3-compatible store; includes provider-specific retention support |
| `pull_ack_telegram_monitor.py` | Alert when signed receiver acknowledgements stop advancing |
| `ingest_status_server.py` | Serve a bounded, secret-free capture and signed-ACK status snapshot for the watcher UI |

The production shred-status collector is now the Rust command
`hivezilla serve-shred-status`. `shred_status_server.py` is frozen and retained
only as a differential test oracle until the first live Rust-container rollout
is verified; no Docker or Compose entrypoint executes it.

The launch wrappers expect a dedicated UID and file-backed secrets. Override
their documented `BLOCKZILLA_*` environment variables for your deployment; do
not modify the scripts to add real endpoints or credentials. Private keys are
staged into process-private runtime directories and must never be committed.
Runtime configuration must reference the staged copy, not its read-only source:

| Wrapper | Source secret | Runtime configuration path |
| --- | --- | --- |
| receiver | `BLOCKZILLA_RECEIVER_SERVER_PRIVATE_KEY_SOURCE` | `/tmp/blockzilla-receiver/server-private-key.pem` |
| receiver | `BLOCKZILLA_RECEIVER_RECEIPT_SIGNING_KEY_SOURCE` | `/tmp/blockzilla-receiver/receipt-signing-key.pem` |
| push replicator | `BLOCKZILLA_REPLICATION_PRIVATE_KEY_SOURCE` | `/tmp/blockzilla-replicator/client-private-key.pem` |
| pull source | `BLOCKZILLA_PULL_SOURCE_PRIVATE_KEY_SOURCE` | `/tmp/blockzilla-pull-source/server-private-key.pem` |
| pull client | `BLOCKZILLA_PULL_CLIENT_PRIVATE_KEY_SOURCE` | `/tmp/blockzilla-pull-client/client-private-key.pem` |

The two files under `config/` already use these receiver and push-replicator
runtime paths. Pull configurations are deployment-specific, but must use the
corresponding paths above. `HIVEZILLA_BIN` may select a local build for testing;
production wrappers default to `/usr/local/bin/hivezilla`.
The `*_RUNTIME_PRIVATE_DIRECTORY` overrides exist for isolated tests; a
production config must name the same directory selected by its wrapper.

The replication PKI helper produces host-specific bundles. Mount or copy these
artifacts under the names used by the example configs:

| Generated artifact | Runtime input/config name |
| --- | --- |
| `blockzilla/blockzilla-primary.crt` | receiver `/etc/blockzilla/tls/primary.crt` |
| `blockzilla/blockzilla-primary.key` | receiver source secret `/run/secrets/blockzilla_primary_private_key` |
| `blockzilla/replica-ca.crt` | receiver `/etc/blockzilla/tls/replica-ca.crt` |
| `blockzilla/allowed-nodes.json` | receiver `/etc/blockzilla/replication/allowed-nodes.json` |
| `blockzilla/blockzilla-receipt.key` | receiver source secret `/run/secrets/blockzilla_receipt_signing_key` |
| `source-node/blockzilla-primary-ca.crt` | replica `/run/secrets/blockzilla-primary-ca.crt` |
| `source-node/source-node-replica.crt` | replica `/run/secrets/source-node-replica.crt` |
| `source-node/source-node-replica.key` | replica source secret `/run/secrets/source_replica_private_key` |
| `source-node/blockzilla-receipt.pub` | replica `/run/secrets/receipt-current.pub` trusted key |

The default generated receipt key id is `receipt-current`. Keep an older public
key as `receipt-previous` only during an intentional key-rotation window.
The pull PKI helper extends that trust bundle with the reverse path:

| Generated pull artifact | Wrapper input |
| --- | --- |
| `source-node/source-node-pull-source.crt` | `BLOCKZILLA_PULL_SOURCE_CERTIFICATE_FILE` |
| `source-node/source-node-pull-source.key` | `BLOCKZILLA_PULL_SOURCE_PRIVATE_KEY_SOURCE` |
| `source-node/pull-client-ca.crt` | `BLOCKZILLA_PULL_SOURCE_CLIENT_CA_FILE` |
| `source-node/pull-allowed-nodes.json` | `BLOCKZILLA_PULL_SOURCE_ALLOWLIST_FILE` |
| `source-node/blockzilla-receipt.pub` | `BLOCKZILLA_PULL_SOURCE_RECEIPT_PUBLIC_KEY_FILE` |
| `blockzilla/blockzilla-pull-client.crt` | `BLOCKZILLA_PULL_CLIENT_CERTIFICATE_FILE` |
| `blockzilla/blockzilla-pull-client.key` | `BLOCKZILLA_PULL_CLIENT_PRIVATE_KEY_SOURCE` |
| `blockzilla/source-node-pull-source-ca.crt` | `BLOCKZILLA_PULL_CLIENT_CA_FILE` |
| `blockzilla/blockzilla-receipt.pub` | `BLOCKZILLA_PULL_CLIENT_RECEIPT_PUBLIC_KEY_FILE` |

Generate PKI outside the repository checkout with an OpenSSL build that
supports Ed25519 (OpenSSL 1.1.1 or newer). Private `*.key` and `*.pem` files and
the conventional `replication-pki/` and `pull-pki/` output directories are
ignored defensively, but they should still live in a dedicated secret store.

When `linux-raw-grpc-recorder.sh` runs beneath `hivezilla supervise`, it sends
the supervisor's authenticated `ready` notification only after validating its
state and writing its startup marker. The recorder exits if that notification
fails, allowing the bounded supervisor policy to retry or fence the crash loop.

Build both native operational binaries before running the shell suites. The
object-store and shell tests use local fixtures only:

```bash
python3 -m pip install -r services/hivezilla/scripts/requirements.txt
python3 services/hivezilla/scripts/test_s3_multipart_upload.py
python3 services/hivezilla/scripts/test_pull_ack_telegram_monitor.py
python3 services/hivezilla/scripts/test_ingest_status_server.py
python3 services/hivezilla/scripts/test_shred_status_server.py
bash services/hivezilla/scripts/test-linux-raw-grpc-cache-supervisor.sh
bash services/hivezilla/scripts/test-linux-raw-grpc-recorder-alerts.sh
bash services/hivezilla/scripts/test-run-grpc-raw-wrappers.sh
bash services/hivezilla/scripts/test-generate-grpc-replication-pki.sh
```

Start ACK alerting with explicit paths or the equivalent historical
`BLOCKZILLA_PULL_ACK_*` and `BLOCKZILLA_TELEGRAM_*` environment variables:

```bash
hivezilla monitor-pull-ack-telegram \
  --ack-status-file /control/pull-ack-status.json \
  --state-file /alert-state/pull-ack-alert.json \
  --token-file /run/secrets/telegram_bot_token \
  --chat-id -100123456 \
  --stale-after-secs 300 \
  --startup-grace-secs 300 \
  --interval-secs 30
```

The alert-state directory must be owned by the monitor UID and must not be
group/world writable. A durable `opening` or `recovery` phase is intentionally
treated as delivered after restart: if shutdown or a network timeout makes the
Telegram result unknowable, the monitor suppresses a duplicate notification.

Review every filesystem limit, TLS identity, retention threshold, and cleanup
policy before operating against real data. The current helper's generation
cleanup requires a verified durable receiver acknowledgement; upload success
alone does not trigger that cleanup.

The V1 target makes a narrower distinction. Every source Hivezilla has its own
private temporary cloud-overflow bucket or namespace. After a sealed segment is
uploaded with a provider-verified end-to-end checksum (or verified read-back)
and recorded in a durable local catalog, disk pressure may evict that local
copy. The logical source record is not retired:
the cloud object must remain available until the one configured terminal raw
consumer writes verified exact objects plus a durable range index to its
separate permanent raw dataset and cumulatively ACKs the exact contiguous
prefix. Once the source persists that ACK and its retirement anchor, it may
delete covered copies from both local disk and cloud.

The target reconnect path also is not implemented by these wrappers. It chooses
an atomic cutover `T`, resumes live delivery at `T`, and runs separately
budgeted stateless range fetches over `[C, T)` from local disk or cloud. Bulk
ranges may arrive out of order, but only one
contiguous exact-byte ACK advances cleanup. Blockzilla schedules archive work
and owns the canonical catalog; a separately fenced Hivezilla compact worker
builds and uploads Archive V2 objects. Neither archive progress nor a public
subscriber can acknowledge raw custody.

## Read-only ingest status

`ingest_status_server.py` reads only the raw cache and the monitoring copy of
the signed receiver ACK. It selects public counters into a cached JSON document
and never publishes endpoints, identities, journal IDs, block hashes, object
keys, receipt hashes, alert text, tokens, command lines, or paths. It has no
mutation endpoint and does not need the Docker socket or any secret mount.
Replay-gap evidence is also read from the recorder's persistent
`monitoring/replay-gaps` registry, so an ACK-covered generation can be retired
without making a known continuity loss disappear from the dashboard.
The checked-in mainnet seed records the already-audited
`433728271`–`433731796` window as RPC-recoverable. When `--known-gaps-file` is
set, a missing or non-regular file fails the publisher health check instead of
silently reporting false continuity.

Run it behind an authenticated same-origin reverse proxy. Bind it to loopback
or an explicit private address; wildcard and public listeners are rejected:

```bash
python3 services/hivezilla/scripts/ingest_status_server.py \
  --listen 127.0.0.1:8790 \
  --cache-root /path/to/read-only/grpc-cache \
  --ack-status-file /path/to/read-only/pull-ack-status.json \
  --known-gaps-file services/hivezilla/config/known-ingest-gaps.mainnet.json \
  --disk-critical-free-bytes 402653184 \
  --disk-warning-free-bytes 805306368
```

By default, missing WAL progress fails red after 60 seconds and a stale signed
ACK fails red after 120 seconds. Both thresholds are explicit CLI options for
deployments with different operational tolerances. Disk thresholds must match
the recorder deployment: the values above are the current roughly 403 MB
critical and 805 MB warning gates for the 3 GB Hetzner volume. The publisher's
conservative 20/30 GiB defaults match the standalone recorder defaults and
must be overridden for that small volume.

The watcher reads
`/api/v1/sidecars/ingest-pipeline/status.json`. Route that one path to this
service, for example with the public watcher's dedicated loopback
`--ingest-upstream 127.0.0.1:8790`; keep every management and raw-data path
unreachable. The ACK proves
durable receiver storage, not indexing. Indexer and NAS fallback fields remain
`unavailable` in this source-side snapshot and the watcher fills them only from
its separate, fresh NAS telemetry.
