# Raw Yellowstone backup on Dokploy

This deployment keeps a live, PoH-capable shadow capture of confirmed
Yellowstone block envelopes. It has no inbound port and does not build the
Blockzilla archive or indexes in the recorder container.

The Hetzner host uses an exact, preallocated **3 GiB local cache**. The cache is
not long-term retention. A normal generation is capped at 384 MiB and follows
this lifecycle:

1. The Rust recorder durably appends to `/data/grpc-cache/active`.
2. At the generation byte limit, the supervisor stops the writer and fully
   verifies the WAL, journal, protobuf payloads, and required PoH structure.
3. It seeds a fresh generation with the exact last durable compressed block as
   frame zero, publishes that successor as `active`, and only then exposes the
   old generation under `sealed/`.
4. The uploader writes generation files to the versioned Backblaze bucket, then
   `manifest.json`, then `_COMMITTED` last. The manifest pins every file's exact
   Backblaze version ID, and the commit pins the manifest version ID. Each pinned
   single-PUT version is bound to the signed request SHA-256 and checked against
   Backblaze's returned MD5 ETag, then checked again with an exact-version signed
   `HEAD` for its length, SHA-256 metadata, version ID, and ETag.
5. A synced local receipt pins the exact manifest and commit version IDs and is
   published only after all remote verification succeeds. The supervisor checks
   that receipt and its predecessor chain before removing the sealed local
   generation.

Routine publication intentionally performs no object `GET`. Re-downloading
every generation during upload consumed about twice the ingest volume and can
exhaust Backblaze's separate daily download cap even while storage remains
below 10 GB. Exact-version payloads are fully downloaded and rehashed during a
restore or future Blockzilla pull; the standalone `upload-file` audit command
also retains full readback verification.

At the measured 5–6 GiB/hour input rate, 3 GiB is only 30–36 minutes of
theoretical storage. The 384 MiB emergency floor and rotation headroom make the
practical Backblaze-outage window roughly 25–30 minutes. If remote uploads fall
behind, the recorder alerts and pauses at the hard floor; it never deletes an
unverified generation.

Backblaze retention is separate from the 3 GiB host limit. This deployment has
no lifecycle deletion: the remote archive, including noncurrent versions, grows
indefinitely by approximately the incoming stream volume. The account-wide
usage monitor counts every upload version in every bucket plus unfinished
large-file parts. It warns at 8,000,000,000 bytes and escalates at
9,500,000,000 bytes before the decimal 10 GB free allowance. These alerts never
stop uploads or delete remote data.

## Current scope

This is an independent capture of the same Yellowstone gRPC source. It resumes
from its own last durable slot inclusively and checks the exact overlap block.
It does **not** yet implement the authenticated Blockzilla primary/replica
request, transfer, acknowledgement, or delete protocol. Consequently:

- it can alert when the Yellowstone feed stalls, the recorder restarts, the
  cache is invalid, Backblaze upload fails, or the cache backlog grows;
- it cannot yet know that the primary Blockzilla node disconnected or request a
  slot range on behalf of that primary;
- a Backblaze commit is the only local generation deletion authority today.

The optional primary-sync heartbeat remains disabled until the real sync
process writes one. The agreed pull/ACK design is documented in
[`docs/live-grpc-b2-sync.md`](../../docs/live-grpc-b2-sync.md); a generic ping is
not treated as synchronization or deletion authority.

## Host cache

Run [`provision-3gb-cache.sh`](provision-3gb-cache.sh) as root on the selected
Dokploy server. It is idempotent and fail-closed:

```sh
sudo ./deploy/dokploy/provision-3gb-cache.sh
```

The script:

- requires at least 7 GiB free before first provisioning, leaving a 4 GiB root
  reserve after allocating the cache;
- creates a non-sparse 3,221,225,472-byte ext4 image at
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

Build the recorder image before reserving the 3 GiB file when the server root
disk is tight. Docker build layers live under Docker's data root, outside this
cache.

## Dokploy configuration

Create a Compose application from this repository and select
`docker-compose.dokploy.yml`. Copy the non-secret settings from
`.env.example` into Dokploy's Environment panel.

Create three Compose **File** mounts under **Advanced → Mounts**:

| Dokploy file name | Local example | Container secret |
| --- | --- | --- |
| `yellowstone-x-token` | `yellowstone-x-token.example` | `/run/secrets/grpc_x_token` |
| `telegram-bot-token` | `telegram-bot-token.example` | `/run/secrets/telegram_bot_token` |
| `backblaze-blockzilla.env` | `backblaze-credentials.example` | `/run/secrets/backblaze_credentials` |

Keep these host-file settings:

```dotenv
BLOCKZILLA_GRPC_X_TOKEN_HOST_FILE=../files/yellowstone-x-token
BLOCKZILLA_TELEGRAM_BOT_TOKEN_HOST_FILE=../files/telegram-bot-token
BLOCKZILLA_B2_CREDENTIALS_HOST_FILE=../files/backblaze-blockzilla.env
```

The Backblaze file is parsed as literal allowlisted `KEY=value` data and is
never sourced as shell. Prefer a bucket-scoped application key. The uploader
does not delete committed remote objects; its only S3 `DELETE` is a best-effort
abort of an incomplete multipart upload.

The bounded-cache settings are:

```dotenv
BLOCKZILLA_RAW_CACHE_MODE=b2-generations
BLOCKZILLA_RAW_CACHE_ROOT=/data/grpc-cache
BLOCKZILLA_RAW_OUTPUT_DIR=/data/grpc-cache/active
BLOCKZILLA_RAW_MAX_GENERATION_BYTES=402653184
BLOCKZILLA_RAW_SEGMENT_TARGET_BYTES=67108864
BLOCKZILLA_RAW_MAX_RECORD_BYTES=134217728
BLOCKZILLA_RAW_MIN_FREE_BYTES=402653184
BLOCKZILLA_RAW_DISK_WARN_FREE_BYTES=805306368
BLOCKZILLA_RAW_DISK_RECOVERY_HYSTERESIS_BYTES=134217728
BLOCKZILLA_RAW_REPLAY_RESUME_HEADROOM_SLOTS=100
BLOCKZILLA_B2_REMOTE_PREFIX=grpc-raw/v1
BLOCKZILLA_B2_USAGE_ALERT_ENABLED=true
BLOCKZILLA_B2_USAGE_ALLOWANCE_BYTES=10000000000
BLOCKZILLA_B2_USAGE_WARNING_BYTES=8000000000
BLOCKZILLA_B2_USAGE_CRITICAL_BYTES=9500000000
BLOCKZILLA_B2_USAGE_RECOVERY_HYSTERESIS_BYTES=500000000
BLOCKZILLA_B2_USAGE_CHECK_INTERVAL_SECS=300
BLOCKZILLA_B2_USAGE_OVER_LIMIT_CHECK_INTERVAL_SECS=21600
```

The replay headroom lets a reconnect outrun an upstream replay floor that moves
during the TLS/gRPC handshake. The immutable schema-2 gap record distinguishes
the provider-confirmed expired range from these deliberately bypassed 100 slots.

Set `BLOCKZILLA_TELEGRAM_CHAT_ID` and keep Telegram enabled. For a forum topic,
also set `BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID`. Leave
`BLOCKZILLA_PRIMARY_SYNC_HEARTBEAT_FILE` empty for this shadow-recorder-only
deployment.

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

Wait for at least one complete rotation. A successful end-to-end check requires:

- the active journal continues growing after rotation from its seeded overlap;
- a generation receipt containing manifest and commit version IDs appears only
  after the Backblaze manifest and commit;
- the corresponding sealed directory disappears only after receipt validation;
- the remote prefix is
  `grpc-raw/v1/<cluster>/<origin>/slot-<20-digit-slot>/`;
- free space returns after the upload and remains bounded by the 3 GiB
  filesystem;
- the container remains healthy and no upload/backlog incident is active.

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
one opening per incident, one severity escalation, and one recovery; steady
incidents do not generate reminders. Incident state is durable across container
rebuilds. The default 900-second setting debounces a quick reopen after recovery,
while a failure that stays open beyond that window is announced. Failed
deliveries remain pending for retry.

| Alert | Meaning | Response |
| --- | --- | --- |
| Recorder restart / gRPC stale | The stream ended, the process failed, or no durable block arrived for 180 seconds. | Check endpoint reachability and credentials, then audit overlap after recovery. |
| Resume coverage warning | Yellowstone did not return the inclusively requested durable slot. | Compare against another recorder and repair the uncovered range. |
| Cache/volume invalid | The exact cache mount, marker, or rotation transaction is missing or inconsistent. Capture is stopped. | Restore the mount and inspect the transaction; never manufacture a marker on the root filesystem. |
| Backup pipeline blocked | Backblaze upload or immutable-version verification failed, sealed generations are backing up, or that backlog reached the local safety floor. These derivative symptoms are one incident, not three. | Check Backblaze credentials, Caps & Alerts, reachability, and logs. Do not manually delete a sealed generation. |
| Backblaze free-storage allowance | Complete account storage, including hidden versions and unfinished parts, reached 8.0 GB and escalated at 9.5 GB. Measurement failures are a separate alert. | If indefinite retention is intended, enable paid storage or raise the Backblaze storage cap before 10 GB; otherwise choose a retention plan. |
| Disk warning / critical | Cache free space is below 768 MiB / 384 MiB for a cause other than the already-reported upload backlog. At the hard floor capture pauses. | Restore safe capacity. Only remotely verified generations may be removed. |
| Primary-sync stale | Active only when a heartbeat path is configured. | Inspect the future authenticated primary/replica sync process. |

Because the notifier runs inside this container, it cannot report total host
loss, loss of all outbound networking, or a container killed before it sends.
Keep an external Dokploy or host uptime alert for those failures.

Relevant platform references: [Dokploy Compose persistence](https://docs.dokploy.com/docs/core/docker-compose),
[Dokploy mounts API](https://docs.dokploy.com/docs/api/mounts),
[Telegram `sendMessage`](https://core.telegram.org/bots/api#sendmessage), and
[Backblaze S3-compatible API](https://www.backblaze.com/apidocs/introduction-to-the-s3-compatible-api),
plus the [Backblaze Native API](https://www.backblaze.com/apidocs/introduction-to-the-native-api).
