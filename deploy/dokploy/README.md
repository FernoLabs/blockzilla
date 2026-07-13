# Raw Yellowstone recorder on Dokploy

This deployment runs only `record-grpc-raw`: it retains confirmed Yellowstone
protobuf update envelopes in the crash-recoverable segmented WAL and does not
build archives, indexes, PoH sidecars, or signatures. The full known-schema
block envelope includes the entry records needed to rebuild `poh.wincode` after
canonical block IDs are assigned. It has no inbound port.

This is an independent shadow capture of the same Yellowstone gRPC source. It
does **not** yet accept backup requests from Blockzilla, send data to Blockzilla,
receive durable acknowledgements, or delete acknowledged WAL segments. Those
request/transfer/ACK/GC semantics belong to the still-open authenticated
primary/replica protocol. The optional primary-sync heartbeat alert described
below therefore stays disabled until that protocol has a real heartbeat writer.

## Deploy

1. Mount a dedicated Hetzner volume at `/mnt/blockzilla-raw` and make the mount
   persistent in the host's boot configuration. Do not use the server root disk.
   Then create the fail-closed marker and the external Docker volume expected by
   Compose. Run this as root. It aborts before creating the marker unless the
   path is an exact mountpoint on a filesystem distinct from the host root filesystem,
   and it refuses to reuse a same-named Docker volume with different options:

   ```sh
   set -eu

   mount_path=/mnt/blockzilla-raw
   volume_name=blockzilla-live-raw-data-external
   marker_path=$mount_path/.blockzilla-raw-volume

   test "$(readlink -f "$mount_path")" = "$mount_path"
   findmnt -rn -M "$mount_path" >/dev/null
   test "$(findmnt -nr -o TARGET -M "$mount_path")" = "$mount_path"
   root_device=$(stat -c %d /)
   volume_device=$(stat -c %d "$mount_path")
   test "$volume_device" != "$root_device"
   df -h "$mount_path"

   if ! docker volume inspect "$volume_name" >/dev/null 2>&1; then
     docker volume create --driver local \
       --opt type=none --opt o=bind --opt device="$mount_path" \
       "$volume_name"
   fi
   existing_driver=$(docker volume inspect -f '{{.Driver}}' "$volume_name")
   existing_type=$(docker volume inspect -f '{{with .Options}}{{index . "type"}}{{end}}' "$volume_name")
   existing_options=$(docker volume inspect -f '{{with .Options}}{{index . "o"}}{{end}}' "$volume_name")
   existing_device=$(docker volume inspect -f '{{with .Options}}{{index . "device"}}{{end}}' "$volume_name")
   if [ "$existing_driver" != local ] || [ "$existing_type" != none ] || \
      [ "$existing_options" != bind ] || [ "$existing_device" != "$mount_path" ]; then
     echo "refusing Docker volume with unexpected driver options" >&2
     docker volume inspect "$volume_name" >&2
     exit 1
   fi
   docker volume inspect "$volume_name"

   chown 10001:10001 "$mount_path"
   chmod 0700 "$mount_path"
   marker_tmp=$(mktemp "$mount_path/.blockzilla-raw-volume.tmp.XXXXXX")
   (umask 077; printf '%s\n' "$volume_device" > "$marker_tmp")
   chown root:root "$marker_tmp"
   chmod 0444 "$marker_tmp"
   mv -f "$marker_tmp" "$marker_path"
   sync -f "$mount_path"
   ```

   Any failed check is a deployment stop. Inspect an existing volume and its
   data manually; do not delete or recreate it merely to make the checks pass.
   The marker contains the mounted filesystem's device ID. Startup, restart,
   and health checks require that ID to match `/data` and the recorder output,
   so a reboot that loses the Hetzner mount cannot silently record onto the root
   filesystem.
2. Create a Dokploy **Compose** application from this repository and select
   `docker-compose.dokploy.yml` as the Compose file. Copy the non-secret settings
   from `.env.example` into the Dokploy Environment panel. The external volume
   must already exist on the selected server.
3. In the Compose application's **Advanced → Mounts** panel, create two `File`
   mounts named `yellowstone-x-token` and `telegram-bot-token`, and paste one
   token into each file. Dokploy persists these files under the Compose
   application's sibling `../files` directory, which survives repository
   replacement during deployment. Keep these environment settings unchanged:

   ```dotenv
   BLOCKZILLA_GRPC_X_TOKEN_HOST_FILE=../files/yellowstone-x-token
   BLOCKZILLA_TELEGRAM_BOT_TOKEN_HOST_FILE=../files/telegram-bot-token
   ```

   Never put real tokens in Git, ordinary Dokploy variables, build arguments,
   or logs. [`yellowstone-x-token.example`](yellowstone-x-token.example) and
   [`telegram-bot-token.example`](telegram-bot-token.example) show the file
   shapes. Inside the container the credentials exist only at
   `/run/secrets/grpc_x_token` and `/run/secrets/telegram_bot_token`, not as
   environment values. Anyone with Docker or Dokploy administrative access can
   still read mounted secrets, so restrict that access.
4. Set `BLOCKZILLA_TELEGRAM_CHAT_ID` to the destination chat ID. For a forum
   topic, also set `BLOCKZILLA_TELEGRAM_MESSAGE_THREAD_ID`; otherwise leave it
   empty. Keep `BLOCKZILLA_TELEGRAM_ENABLED=true`. The default monitor runs
   every 30 seconds, warns below 30 GiB free, and suppresses repetitions of the
   same problem for 900 seconds. Disk recovery uses a 1 GiB margin so a
   filesystem hovering at a threshold does not flap between alert and recovery. Leave
   `BLOCKZILLA_PRIMARY_SYNC_HEARTBEAT_FILE` empty for this shadow-recorder-only
   deployment.

   This Dokploy Compose file treats Telegram configuration as a deployment
   requirement: the chat ID and token secret must exist even if the enabled flag
   is temporarily set to `false`. The flag is an operational kill switch, not a
   no-alerts deployment profile.
5. Before the first start, set `BLOCKZILLA_RAW_FROM_SLOT` to the earliest slot
   the provider can replay, if known. Leaving it empty starts at the provider's
   live position. This value is bootstrap-only: once a durable row exists, the
   recorder requests the last durable slot inclusively, skips the exact durable
   block after observing the overlap, and ignores later changes to the configured
   bootstrap slot. On a durable resume, it emits a coverage warning if the
   provider starts after the known overlap block.
6. Deploy, then open the recorder's Dokploy terminal and send a test alert:

   ```sh
   /usr/local/bin/blockzilla-raw-recorder --telegram-test
   ```

   Treat a failed test as a deployment failure. The host and container need
   outbound HTTPS access to the Telegram Bot API. Keep the existing NAS/Mac
   recorder alive until the Hetzner journal is advancing at least as fast as
   the finalized head and its start overlaps a durable slot from another
   recorder.

The image is built with Rust 1.96 for `linux/amd64`. Runtime UID/GID `10001`
must be able to write the external `raw_data` volume. The
endpoint, cluster ID, origin node ID, and source ID are part of the persisted
spool identity; use a new volume rather than changing them in place.

This Compose application builds the Rust image on the selected server. Docker's
build layers and BuildKit caches use the Docker data root, not `raw_data`; check
root-filesystem capacity before the first build and prune only audited build
cache. For a small root disk, publish the image from CI and deploy that immutable
image instead of relying on repeated on-host builds.

The default 768 MiB hard memory limit is deliberately above the recorder's
normal footprint while leaving headroom for a worst-case 128 MiB protobuf,
its encoded buffer, and its compressed buffer. Measure the live high-water
mark before lowering this limit; an OOM restart can exceed provider replay.

## Operate and verify

The supervisor reconnects after stream errors and the Rust recorder recovers
the WAL tail before requesting the last durable slot inclusively. A 20 GiB free-space
floor is enforced both before launch and before each append. Falling through
that floor pauses capture and makes the container unhealthy; it never deletes
old segments automatically. A 180-second durable-block idle timeout forces a
reconnect even when the transport remains open but stops delivering blocks;
application-level pings do not reset it.

The Dokploy deployment enables `BLOCKZILLA_RAW_REQUIRE_COMPLETE_POH=true` by
default. Before a block crosses the durable WAL boundary, the recorder requires
non-empty embedded entries, exact entry/transaction counts, contiguous unique
entry indexes, matching slots and transaction ranges, 32-byte entry hashes, and
a final entry hash equal to the blockhash. A provider that omits or structurally
malforms entry data therefore causes that update to be rejected before it can
enter the PoH backup. This proves reconstructable inputs, not cryptographic PoH
execution or continuous slot coverage.

The healthcheck validates the marker's filesystem device ID and reads only the
`raw-blocks.jsonl` size and modification time. It does not parse the actively
appended JSON tail. A journal older than 180 seconds is unhealthy after the
startup grace period. Docker's `unless-stopped` policy does not restart a merely
unhealthy container; reconnects are handled by the in-container idle watchdog
and supervisor, while `unhealthy` is an operator alert for a stale feed,
missing mount, invalid PoH, or exhausted disk.

### Telegram alerts

The tiny notifier is part of the supervisor, uses only outbound HTTPS, and does
not expose an inbound service. `BLOCKZILLA_TELEGRAM_ALERT_COOLDOWN_SECS=900`
rate-limits repeated copies of the same problem; distinct problems can still be
reported independently. The cooldown state lasts for the current container
run; stopping, restarting, or recreating it can resend an active generic incident.
Resume-coverage events are the exception: they are synced under
`/data/grpc-raw/.monitoring`, retained through restarts until their exact event ID
is delivered, and never overwritten by a later gap. Failed incident or recovery
deliveries are retried on a later monitor pass and do not clear the incident.
Telegram delivery is best-effort and never authorizes data deletion.
Because the notifier runs inside this container, it cannot report
a host outage, loss of all outbound networking, or a container that is killed
before the message is sent; retain an external Dokploy/host uptime alert for
those failures.

| Alert | What it means now | Operator response |
| --- | --- | --- |
| Recorder stopped / restarting or journal stale | The independent Hetzner Yellowstone capture exited, its stream ended, or no durable block arrived for 180 seconds. The supervisor retries from the durable overlap slot. Recovery is sent only after the journal grows by another durable record. | Check provider reachability, credentials, and recorder logs, then verify slot coverage after recovery. |
| Volume invalid | The dedicated mount or its fail-closed device marker disappeared or changed. Capture is stopped to avoid writing to an unintended filesystem. | Restore and verify the Hetzner volume mount and marker; never recreate the marker until the mount itself is proven correct. |
| Disk warning / critical floor | Free space fell below the 30 GiB warning threshold or the 20 GiB hard floor. At the floor, durable capture pauses. | Restore downstream capacity or add disk. Do not manually delete unverified WAL segments. |
| Resume coverage warning | Yellowstone did not return the inclusively requested durable resume slot. The recorder durably publishes this incident before accepting the later block and stops that attempt if the incident cannot be published. | Audit the missing slot range against another recorder and repair it; a reconnect alone cannot prove the gap is covered. |
| Primary-sync heartbeat stale | Only active when `BLOCKZILLA_PRIMARY_SYNC_HEARTBEAT_FILE` is non-empty. An existing heartbeat older than 600 seconds alerts; an initially absent file gets the same startup grace, while a file that disappears after first being seen alerts on the next check. Checks continue during recorder backoff and disk/volume pauses. | Inspect the future authenticated sync process and its ACK state. This alert is deliberately disabled today because that process does not exist yet. |

In particular, there is not yet a “Blockzilla disconnected, request Hetzner
backup, then delete after sync” event chain. Today Telegram can report the raw
gRPC feed/recorder stalling or exiting, volume invalidation, a missed resume
overlap, and disk pressure. Once the primary/replica protocol is
implemented, point `BLOCKZILLA_PRIMARY_SYNC_HEARTBEAT_FILE` at its heartbeat in
the shared volume (it must be a path below `/data`) to enable the heartbeat alert;
that heartbeat is monitoring only, not deletion authority.

Useful commands from the Dokploy terminal are:

```sh
df -h /data
test "$(cat /data/.blockzilla-raw-volume)" = "$(stat -c %d /data)"
stat /data/grpc-raw/raw-blocks.jsonl
```

Both inspection commands require a stopped recorder or a filesystem
snapshot/copy; neither treats an actively appended JSON line as a consistent
snapshot:

```sh
/usr/local/bin/blockzilla-live-producer inspect-grpc-raw \
  --output-dir /data/grpc-raw --verify-payloads
/usr/local/bin/blockzilla-live-producer verify-grpc-raw-poh \
  --output-dir /data/grpc-raw
```

The first command proves WAL/frame/checksum/protobuf recoverability. The second
also locks and audits the WAL, requires at least one record, proves journal/WAL
tail parity, and checks that every replayed block contains the semantic inputs
required to reconstruct the PoH sidecar. It deliberately does not assign
canonical archive `block_id` values; those belong to the later dedup/finality
writer. `--min-records 0` is available only for an intentional empty-spool
diagnostic.

Capacity must include the 20 GiB reserve **plus** the required outage window.
Measure the compressed byte growth on this provider before relying on it. Since
there is no automatic retention, arrange verified export/consumption and delete
segments only after the downstream durability protocol says they are safe.

Do not stop the prior recorder based only on a healthy badge. Compare the last
durable slot and its rate over several minutes, confirm overlap, then perform
the handoff. A provider that cannot replay the requested slot can still leave a
gap after any disconnect; redundant streams and downstream slot dedup/repair
remain necessary for a no-loss design.

Before calling this a usable shadow backup, retain at least 1,000 overlapping
blocks, stop or snapshot the recorder, run both verifiers (including
`verify-grpc-raw-poh --min-records 1000`), and compare
`(slot, blockhash)` plus the ordered PoH entry hashes/counts with the NAS copy.
Also test a container kill/restart, a stalled connection, an absent mount after
reboot, and the configured disk reserve. This is an independent PoH-capable
shadow recorder, not yet the authenticated primary/replica protocol.
