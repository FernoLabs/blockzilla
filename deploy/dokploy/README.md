# Sole Yellowstone recorder and relay on Dokploy

## Direct pull-source canary

Deploy `docker-compose.dokploy-pull-source.yml` as a **new Dokploy Compose
application** using `pull-source.env.example`. It attaches the existing external
`blockzilla-live-raw-data-external` volume read-only at `/source-data` and keeps
its ACK WAL in the separate stable read/write `blockzilla-live-pull-control`
volume mounted at `/control`. It publishes direct Tonic mTLS on host TCP
`10443` to container TCP `9443`. It has no Triton, Telegram, R2, or Backblaze
secrets and cannot open another paid Yellowstone subscription. Do not add a
Dokploy domain or a TLS-terminating reverse proxy.

The separate application is the canary target because creating it does not
recreate or restart the existing recorder. Its source config must:

- bind `0.0.0.0:9443` and read `/source-data/grpc-cache`;
- persist ACK state at `/control/pull-cumulative-ack.wal`;
- publish the non-authoritative Telegram-monitor snapshot at
  `/control/pull-ack-status.json` only after that ACK WAL is synced;
- select at most one event per batch;
- trust only the generated pull-client certificate fingerprint and the
  Blockzilla receipt public key; and
- keep generation GC disabled until the direct pull, receiver fsync, signed ACK,
  ACK-WAL fsync, restart, and exact replay proof all pass.

Build the source image off-host and preload it on Hetzner. Set
`BLOCKZILLA_PULL_SOURCE_IMAGE_ID` to the exact `sha256:...` content address
reported by `docker image inspect --format '{{.Id}}'` on both machines. The
standalone production Compose accepts that content address rather than a
mutable tag, has no `build` stanza, and uses `pull_policy: never`, so a
deployment cannot consume the small server's Docker filesystem or silently
substitute another image.

Pre-create the external `blockzilla-live-pull-control` volume in a trusted
operator step. The `pull-control-init` one-shot service creates no network and
mounts only that volume; it sets its root to UID/GID `10001:10001`, mode `0700`,
before the source starts. Because the volume is external, deleting the Dokploy
application or running `down -v` cannot delete its ACK WAL. The source wrapper
then fails closed unless `/source-data` is an actual read-only mount, its cache
and pre-created lock are readable, and `/control` is writable. Never delete or
replace the control volume while its binary ACK WAL is retention authority.

Before the first canary start, run the recorder from the same reviewed image (or
an explicitly compatible build) so its rotations take the exclusive side of
the source's shared lock. Verify that the recorder created the mode-`0600`,
UID/GID-`10001:10001` `/data/grpc-cache/.rotation.lock`. Never manufacture that
file to make an older, non-locking writer look compatible, and never give the
public source a read/write recorder mount to initialize it.

Create these six Dokploy File mounts for the new application. The server private
key is copied at startup into a UID-10001-owned mode-0600 tmpfs file. Copy
`grpc-pull-source.json.example` for the config and make its trusted receipt key
id match the NAS receiver's signing key id.

| Dokploy file name | Container secret |
| --- | --- |
| `grpc-pull-source.json` | `/run/secrets/grpc_pull_source_config` |
| `hetzner-pull-source.crt` | `/run/secrets/hetzner_pull_source_certificate` |
| `hetzner-pull-source.key` | `/run/secrets/hetzner_pull_source_private_key` |
| `pull-client-ca.crt` | `/run/secrets/pull_client_ca` |
| `pull-allowed-nodes.json` | `/run/secrets/pull_allowed_nodes` |
| `blockzilla-receipt.pub` | `/run/secrets/blockzilla_receipt_public_key` |

The integrated `pull-source` profile in `docker-compose.dokploy.yml` is only a
future consolidation option. Keep the recorder application's
`COMPOSE_PROFILES` empty during the canary.

This deployment is the single paid Yellowstone source for Blockzilla. Hetzner
records every confirmed full-block envelope into a durable WAL, can copy
bounded immutable generations to Cloudflare R2 under local pressure, and serves the same durably
accepted stream to authenticated downstream consumers. Blockzilla indexers and
other products must subscribe to the Hetzner relay; they must not open their own
Triton full-block subscriptions.

The first relay API remains Yellowstone-compatible and emits the exact full
block envelope. The compacter consumes that stream once and produces the
Blockzilla compact representation as a versioned derivative. The compact
archive is not the recovery source: the raw WAL and R2 generations remain able
to rebuild it after a schema change.

The relay remains private and token-authenticated. Do not expose an anonymous
Yellowstone endpoint. Until the relay command and private network route have
passed the fan-out/replay tests, keep all duplicate paid consumers disabled and
do not top up Triton.

The Hetzner host uses a hard-capped, preallocated **disk-first cache**. The
current conservative reservation is 3 GiB; the provisioner also accepts a
larger explicit size when the host has enough free space above its protected
root reserve. Every accepted block and handoff row are fsynced locally before
the block enters the bounded RAM fan-out ring. A normal generation is capped at
384 MiB and follows this lifecycle:

1. The Rust recorder durably appends to `/data/grpc-cache/active`.
2. At the generation byte limit, the Rust recorder keeps the same Yellowstone
   subscription open, seeds a fresh generation with the exact last durable
   compressed block as frame zero, and atomically publishes it as `active`
   under a synced rotation marker.
3. The stopped predecessor remains hidden from the uploader until a complete
   WAL, journal, protobuf, and required-PoH audit succeeds. The already-received
   cap-crossing block is then fsynced as successor frame one before polling the
   stream again, and only the audited predecessor becomes visible under
   `sealed/`.
4. An audited sealed generation may be copied to R2 as an overflow safety copy.
   The verified generation remains local. A successful R2 upload and receipt do
   not prove that Blockzilla consumed the data and do not authorize local
   eviction.
5. The uploader writes generation files to the dedicated R2 prefix, then
   `manifest.json`, then `_COMMITTED` last. Every single PUT is conditional on
   the key being absent and binds Content-MD5 plus SHA-256 metadata. Exact HEAD
   verification detects either an identical retry or an immutable-key
   collision; R2 bucket versioning is not assumed.
6. A synced local upload receipt pins the manifest and commit hashes/ETags and is
   published only after all remote verification succeeds. It proves the R2 copy,
   not Blockzilla synchronization. Local or remote deletion additionally
   requires a valid signed Blockzilla durable ACK for the exact generation.
   The signed receiver protocol and crash-recoverable local ACK cleanup are
   implemented and locally tested. The standalone Hetzner pull source serves
   the same durable WAL to a Blockzilla-initiated mTLS client. Until its full
   canary passes and GC is explicitly enabled in source configuration,
   production local cleanup and all remote cleanup remain locked and capture
   pauses at the hard local floor.

Routine R2 publication performs conditional PUT plus metadata HEAD, but no
payload GET. Re-downloading every generation during upload previously consumed
about twice the ingest volume and exhausted Backblaze's separate daily download
cap. Payloads are downloaded and fully rehashed only during replay/restore.

At the measured 5–6 GiB/hour input rate, the current 3 GiB reservation is only
30–36 minutes of theoretical storage. The spill watermarks can trigger R2
safety-copy work, but an R2 commit alone must not reclaim local space. Before
signed-ACK GC is deployed, a sustained Blockzilla outage therefore ends at the
hard floor: the recorder keeps every durable generation, alerts once, and
pauses without polling another block.

R2 retention is separate from the configured host limit. The dedicated
`blockzilla-live-grpc` bucket has a self-imposed 1 TB safety budget; R2 does not
enforce that value as a hard bucket capacity. Its role is overflow and
disaster recovery, not a six-day history service for every subscriber. The
receipt-ledger monitor warns at 800 GB and becomes critical at 950 GB. That
ledger is a lower bound: interrupted/orphan objects require a separate
paginated bucket reconciliation before production retention is enabled. At the
critical watermark capture pauses if signed ACKs do not expose enough safe
cleanup; byte pressure never evicts unacknowledged data.

Cleanup may remove only whole, verified, oldest generations beneath this
recorder's exact prefix after a valid signed Blockzilla durable ACK binds their
exact observation chain, manifest, and predecessor. `_COMMITTED` is removed
first so an interrupted remote prune cannot advertise a partial generation. A
heartbeat, unsigned slot, upload receipt, disk watermark, or R2 watermark can
alert but can never delete backup data.

## Current scope

This recorder resumes from its own last durable slot inclusively and checks the
exact overlap block. It is the only component permitted to use the paid Triton
token. The authenticated live relay, bounded mTLS raw receiver, signed
cumulative ACK, Hetzner pull source, and outbound Blockzilla pull client are
implemented for local testing. The production direct path and receiver-spool
consumer are not yet proven. The historical Yellowstone reader is also still
missing. Until the pull path is activated, Blockzilla must remain stopped
rather than silently starting a second paid full-block subscription.

The current production recorder/backup layer:

- it can alert when the Yellowstone feed stalls, the recorder restarts, the
  cache is invalid, a pressure-triggered R2 spill fails, or the hard
  local floor is reached;
- it cannot yet serve a downstream cursor older than the local live ring;
  the target recovery endpoint is Blockzilla's durable raw history, with R2 used
  only to restore a primary backlog;
- it can create and verify R2 upload receipts, but those receipts have no local
  or remote deletion authority;
- Blockzilla's durable raw receiver and outbound pull client are not yet proven
  against the standalone Dokploy source, so no production worker consumes the
  new durable ACK WAL. Production local eviction and remote pruning stay locked
  during this proof.

The next deployment step starts the standalone source with GC disabled, then
starts the NAS receiver and outbound `pull-client` profile. This changes no
WAL-first hot-path process.
Blockzilla fsyncs the exact raw WAL and cursor, then signs a cumulative
sequence/digest-chain receipt. Hetzner verifies and fsyncs that ACK before a
whole generation becomes eligible for local cleanup. R2 remote pruning remains
disabled until it has its own ACK-bound manifest/apply path. The bounded RAM ring
remains fan-out only and may evict entries freely because their WAL records are
already durable.

The optional primary-sync monitor remains disabled until the source publishes a
stable post-fsync status snapshot. It never treats a generic ping, connection
state, or mtime-only approximation as synchronization.
The single-source gateway and durable receipt design is
documented in
[`docs/live-grpc-single-source.md`](../../docs/live-grpc-single-source.md); a
generic ping is not treated as synchronization or deletion authority.

## Host cache

Run [`provision-3gb-cache.sh`](provision-3gb-cache.sh) as root on the selected
Dokploy server. The legacy filename is retained for compatibility; the size is
configurable. It is idempotent and fail-closed:

```sh
sudo ./deploy/dokploy/provision-3gb-cache.sh

# Example for a new 8 GiB cache; this does not resize an existing image.
sudo BLOCKZILLA_CACHE_BYTES=8589934592 \
  ./deploy/dokploy/provision-3gb-cache.sh
```

The script:

- requires at least the requested cache size plus the configured 4 GiB root
  reserve before first provisioning;
- defaults to a non-sparse 3,221,225,472-byte ext4 image and accepts a larger
  `BLOCKZILLA_CACHE_BYTES` value at
  `/var/lib/blockzilla/raw-cache.ext4`;
- persists its loop mount at `/mnt/blockzilla-raw` in `/etc/fstab`;
- uses `nosuid,nodev,noexec,noatime` and zero ext4 reserved-block percentage;
- keeps the mount root and fail-closed marker root-owned while granting runtime
  UID/GID `10001` access only to `grpc-cache/`;
- creates or validates the external Docker bind volume
  `blockzilla-live-raw-data-external`.

The container accepts the stable cache marker only in
`BLOCKZILLA_RAW_CACHE_MODE=b2-generations`. Legacy dedicated-volume mode keeps
the original numeric device marker validation. If the loop mount is missing,
the marker is absent from the underlying host directory and capture cannot
silently fall back to the root filesystem.

An existing image is never silently resized: a different requested size fails
closed and requires an explicit stopped-service migration. Build the recorder
image before reserving the cache file when the server root disk is tight.
Docker build layers live under Docker's data root, outside this cache.

## Dokploy configuration

Create a Compose application from this repository and select
`docker-compose.dokploy.yml`. Copy the non-secret settings from
`.env.example` into Dokploy's Environment panel.

Create the five recorder Compose **File** mounts under **Advanced → Mounts**:

| Dokploy file name | Local example | Container secret |
| --- | --- | --- |
| `yellowstone-x-token` | `yellowstone-x-token.example` | `/run/secrets/grpc_x_token` |
| `yellowstone-relay-x-token` | a new downstream-only token | `/run/secrets/grpc_relay_x_token` |
| `telegram-bot-token` | `telegram-bot-token.example` | `/run/secrets/telegram_bot_token` |
| `cloudflare-r2.env` | `cloudflare-r2-credentials.example` | `/run/secrets/r2_credentials` |
| `backblaze-blockzilla.env` | `backblaze-credentials.example` | `/run/secrets/backblaze_credentials` |

Keep these host-file settings:

```dotenv
BLOCKZILLA_GRPC_X_TOKEN_HOST_FILE=../files/yellowstone-x-token
BLOCKZILLA_RAW_RELAY_X_TOKEN_HOST_FILE=../files/yellowstone-relay-x-token
BLOCKZILLA_TELEGRAM_BOT_TOKEN_HOST_FILE=../files/telegram-bot-token
BLOCKZILLA_R2_CREDENTIALS_HOST_FILE=../files/cloudflare-r2.env
BLOCKZILLA_B2_CREDENTIALS_HOST_FILE=../files/backblaze-blockzilla.env
```

Credential files are parsed as literal allowlisted `KEY=value` data and are
never sourced as shell. The R2 token should have Object Read & Write access only
to the `blockzilla-live-grpc` bucket; application code is further confined to
`live-grpc-backup/v1`. The ordinary uploader never deletes committed objects.
Retention remains dry-run-only until the locally implemented signed Blockzilla
ACK verifier is wired into the production GC path. Its eventual apply path may
delete only ACK-covered, validated whole generations below that prefix.

The bounded-cache settings are:

```dotenv
BLOCKZILLA_RAW_CACHE_MODE=b2-generations
BLOCKZILLA_RAW_CACHE_ROOT=/data/grpc-cache
BLOCKZILLA_RAW_OUTPUT_DIR=/data/grpc-cache/active
BLOCKZILLA_RAW_MAX_GENERATION_BYTES=402653184
BLOCKZILLA_RAW_SEGMENT_TARGET_BYTES=67108864
BLOCKZILLA_RAW_MAX_RECORD_BYTES=134217728
BLOCKZILLA_RAW_MIN_FREE_BYTES=402653184
BLOCKZILLA_RAW_B2_SPILL_START_PERCENT=25
BLOCKZILLA_RAW_B2_SPILL_RECOVERY_PERCENT=35
BLOCKZILLA_RAW_DISK_WARN_FREE_BYTES=805306368
BLOCKZILLA_RAW_DISK_RECOVERY_HYSTERESIS_BYTES=134217728
BLOCKZILLA_RAW_REPLAY_RESUME_HEADROOM_SLOTS=100
BLOCKZILLA_RAW_OBJECT_STORE=r2
BLOCKZILLA_RAW_OBJECT_STORE_CREDENTIALS_FILE=/run/secrets/r2_credentials
BLOCKZILLA_RAW_OBJECT_STORE_REMOTE_PREFIX=live-grpc-backup/v1
BLOCKZILLA_R2_USAGE_ALERT_ENABLED=true
BLOCKZILLA_R2_USAGE_WARNING_BYTES=800000000000
BLOCKZILLA_R2_USAGE_CRITICAL_BYTES=950000000000
BLOCKZILLA_R2_USAGE_RECOVERY_HYSTERESIS_BYTES=50000000000
BLOCKZILLA_R2_USAGE_CHECK_INTERVAL_SECS=3600
BLOCKZILLA_R2_RETENTION_ENABLED=false
BLOCKZILLA_R2_RETENTION_TRIGGER_BYTES=950000000000
BLOCKZILLA_R2_RETENTION_TARGET_BYTES=900000000000
BLOCKZILLA_R2_RETENTION_MINIMUM_AGE_SECS=86400
BLOCKZILLA_R2_RETENTION_MINIMUM_RETAINED_GENERATIONS=2
BLOCKZILLA_R2_RETENTION_CHECK_INTERVAL_SECS=3600
```

The retention trigger/target values currently drive planning and alerts only.
Setting `BLOCKZILLA_R2_RETENTION_ENABLED=true` must not enable deletion while the
signed ACK-to-R2 manifest/prune apply path is absent.

### Private downstream relay contract

The Rust relay is opt-in. An empty `BLOCKZILLA_RAW_RELAY_BIND` means the
supervisor must omit every relay argument. A non-empty bind maps one-for-one to
this exact `record-grpc-raw` CLI contract:

```text
--relay-bind "$BLOCKZILLA_RAW_RELAY_BIND"
--relay-x-token-file "$BLOCKZILLA_RAW_RELAY_X_TOKEN_FILE"
--relay-max-records "$BLOCKZILLA_RAW_RELAY_MAX_RECORDS"
--relay-max-encoded-bytes "$BLOCKZILLA_RAW_RELAY_MAX_ENCODED_BYTES"
--relay-max-clients "$BLOCKZILLA_RAW_RELAY_MAX_CLIENTS"
```

For transient same-network consumers only, enable it with
`BLOCKZILLA_RAW_RELAY_BIND=0.0.0.0:10001`. Consumers on the private Compose
network connect to `http://raw-recorder:10001`; no host port is published. The
downstream token comes only from `/run/secrets/grpc_relay_x_token`. It must be a
non-empty regular file (not a symlink), at most 4096 bytes, with visible ASCII
and at most one trailing LF/CRLF. The paid upstream token and its environment
variable are never a fallback.

Defaults retain at most 128 exact full-block updates and 128 MiB for at most
four clients. The byte cap must remain at least as large as
`BLOCKZILLA_RAW_MAX_RECORD_BYTES`, so every accepted WAL record is publishable.
The relay preloads that bounded tail from the verified active WAL,
then assigns a process-local sequence beginning at zero. It enables zstd
responses when a client advertises zstd support. `from_slot` works only inside
the retained in-memory ring; an older request fails explicitly so a consumer
can switch to Blockzilla's historical raw service. R2 is a restore source for
Blockzilla, not a normal subscriber endpoint. A block becomes visible only after
both its WAL frame and handoff row have been fsynced, and relay publication
failure stops the recorder.

The durable Blockzilla path is the standalone `pull-source` plus the NAS
`pull-client`, not this bounded RAM relay. Blockzilla connects to
`https://188.245.147.127:10443` using TLS name
`blockzilla-hetzner-source`; the source never connects inbound to the NAS.
The client forwards each exact batch to the local receiver, waits for its
fsynced signed ACK, and only then submits that ACK to Hetzner. The source
persists the verified ACK before answering. A read, disconnect, generic ping,
R2 receipt, or unsigned slot cannot advance retention.

Keep source GC disabled during the canary. The read-only recorder mount makes
this a filesystem capability boundary, not merely a config flag. Once restart
and replay tests prove the exact durable chain, enabling local oldest-first
cleanup requires a separate reviewed Compose revision that changes the recorder
mount to read/write together with the explicit `gc.enabled=true` config change.
Do not make either change through an environment toggle. R2 cleanup remains
disabled.

The R2 usage check is deliberately hourly and follows only the validated local
receipt chain in `blockzilla-live-grpc`; it does not itself authorize deletion.

The replay headroom lets a reconnect outrun an upstream replay floor that moves
during the TLS/gRPC handshake. The immutable schema-2 gap record distinguishes
the provider-confirmed expired range from these deliberately bypassed 100 slots.

Set `BLOCKZILLA_TELEGRAM_CHAT_ID` and keep Telegram enabled. For a forum topic,
also set `BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID`. Leave
`BLOCKZILLA_PRIMARY_SYNC_HEARTBEAT_FILE` empty until the direct-path canary has
produced `/control/pull-ack-status.json`. In a later reviewed revision, mount
the same control volume read-only into the recorder at `/data/pull-control` and
point its monitor at `/data/pull-control/pull-ack-status.json`. The JSON snapshot
is for alerts only; the binary ACK WAL remains the sole cleanup authority.

`BLOCKZILLA_RAW_FROM_SLOT` is bootstrap-only. If it is empty, a fresh cache
starts from the provider's live position. Once a durable row exists, the
recorder always requests that row's slot inclusively, skips the exact overlap,
and publishes a durable warning if the provider begins later.

## First deployment checks

After deployment, run the outbound Telegram test in the Dokploy terminal:

```sh
/usr/local/bin/blockzilla-raw-recorder --telegram-test
```

Then verify the mount, marker, journal progress, and bounded filesystem:

```sh
df -h /data
test "$(cat /data/.blockzilla-raw-volume)" = blockzilla-raw-cache-v1
stat /data/grpc-cache/active/raw-blocks.jsonl
find /data/grpc-cache/sealed -mindepth 1 -maxdepth 1 -type d -print
find /data/grpc-cache/receipts -maxdepth 1 -name 'slot-*.json' -print
```

Wait for at least one complete rotation. A healthy local-only check requires:

- the active journal continues growing after rotation from its seeded overlap;
- each sealed generation remains local; an optional R2 receipt proves only that
  its safety copy, manifest, and commit hashes/ETags were verified;
- crossing the spill-start watermark may copy a generation to R2 but must not
  remove its local directory without a signed Blockzilla ACK;
- while the pull source/client proof is incomplete or GC is disabled, pressure reaches the
  hard floor and pauses capture rather than deleting a local or remote
  generation;
- the remote prefix is
  `live-grpc-backup/v1/<cluster>/<origin>/slot-<20-digit-slot>/`;
- after signed ACK cleanup is deployed, a valid ACK can return free space above
  the recovery watermark; before then no cleanup-based recovery is expected;
- `/tmp/blockzilla-raw-recorder.state` inside the recorder says `running`, the
  healthcheck passes, and no upload/backlog incident is active. A live PID with
  no new fsynced handoff row is `stalled`, not healthy.

Do not retire another recorder based only on a healthy badge. Compare at least
1,000 overlapping `(slot, blockhash)` records and their ordered PoH entry
hashes/counts first.

## Verification and recovery

The active writer owns an exclusive WAL lock. Run structural verification only
against a stopped container, a sealed generation, or a filesystem snapshot:

```sh
/usr/local/bin/blockzilla-live-producer inspect-grpc-raw \
  --output-dir /data/grpc-cache/sealed/slot-00000000000000000000 \
  --verify-payloads

/usr/local/bin/blockzilla-live-producer verify-grpc-raw-poh \
  --output-dir /data/grpc-cache/sealed/slot-00000000000000000000 \
  --min-records 1
```

The second command requires non-empty embedded entries, exact
entry/transaction counts, contiguous indexes, matching slots and transaction
ranges, 32-byte entry hashes, and a final entry hash equal to the blockhash.
This proves the inputs needed to reconstruct the PoH sidecar, not continuous
slot coverage or cryptographic execution of PoH.

Rotation uses a synced marker plus hidden `.next-*` and `.sealed-*` directories.
After a crash, startup completes the recorded rename transaction before capture
or upload resumes. The uploader cannot discover an old generation until its
seeded successor is already active.

## Telegram alerts

The notifier is outbound-only and uses the Telegram Bot API over HTTPS. It sends
one opening per incident, one severity escalation, and at most one recovery;
steady incidents do not generate reminders. A reconnect-verification warning closes
silently because recording never stopped. Incident state is durable across
container rebuilds. The default 900-second setting debounces a quick reopen,
while a failure that stays open beyond that window is announced. Failed
deliveries remain pending for retry.

Messages are intentionally short: at most five useful lines with a status,
storage in decimal `MB`/`GB`/`TB`, and one action. Telegram already supplies the
timestamp, so node IDs, raw byte counts, and storage implementation terms are
not shown. Example:

```text
Blockzilla backup - CRITICAL
Backup storage problem
Status: Cloudflare R2 upload failed. Backup is paused; saved data is safe.
Storage: 402 MB free. 6 backup batches waiting locally.
Action: Check R2 credentials or service status; automatic retry is on.
```

| Alert | Meaning | Response |
| --- | --- | --- |
| Recorder restart / gRPC backup stalled | The process may still be alive, but no new block crossed the fsynced handoff boundary for three minutes. | Check the gRPC provider or connection. |
| gRPC reconnect not verified | The provider did not replay the last saved slot, so reconnect coverage is uncertain. | Compare the range with another source and repair any gaps. |
| Missing gRPC slots | A confirmed provider-history range is absent from the backup. | Repair that range from another source if needed. |
| Backup disk unavailable | Hetzner cannot use or inspect the backup disk, so recording is paused. | Check the Hetzner volume mount. |
| Backup storage problem | Cloudflare R2 upload is blocked or Hetzner space is too low. | Follow the single action shown in the alert. |
| Cloudflare R2 safety storage filling up | This recorder's verified safety copies reached 800 GB and become critical at 950 GB. | If Blockzilla has not signed the oldest spool, nothing is deleted; restore sync or expand the budget. |
| Blockzilla backup copy not confirmed | Local durable data is newer than the signed receiver ACK. An old ACK is normal when there is no newer local data. This does not report indexing or compaction progress. | Check the Blockzilla receiver connection. |

Because the notifier runs inside this container, it cannot report total host
loss, loss of all outbound networking, or a container killed before it sends.
Keep an external Dokploy or host uptime alert for those failures.

Relevant platform references: [Dokploy Compose persistence](https://docs.dokploy.com/docs/core/docker-compose),
[Dokploy mounts API](https://docs.dokploy.com/docs/api/mounts),
[Telegram `sendMessage`](https://core.telegram.org/bots/api#sendmessage), and
[Cloudflare R2 S3 compatibility](https://developers.cloudflare.com/r2/api/s3/api/).
