# Hivezilla operational helpers

These helpers package the durable raw-capture primitives without encoding a
specific server, provider account, or deployment system.

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

The object-store helper requires Python 3.11 or newer and the dependency in
`requirements.txt`. Its tests and all shell tests use local fixtures only:

```bash
python3 -m pip install -r services/hivezilla/scripts/requirements.txt
python3 services/hivezilla/scripts/test_s3_multipart_upload.py
python3 services/hivezilla/scripts/test_pull_ack_telegram_monitor.py
bash services/hivezilla/scripts/test-linux-raw-grpc-cache-supervisor.sh
bash services/hivezilla/scripts/test-linux-raw-grpc-recorder-alerts.sh
bash services/hivezilla/scripts/test-run-grpc-raw-wrappers.sh
bash services/hivezilla/scripts/test-generate-grpc-replication-pki.sh
```

Review every filesystem limit, TLS identity, retention threshold, and cleanup
policy before operating against real data. Upload success is not permission to
delete a source generation: cleanup additionally requires a verified durable
receiver acknowledgement.
