# Blockzilla raw replication receiver

This is an opt-in, NAS-side deployment for the durable raw-gRPC receiver and
the Blockzilla-initiated Hetzner pull client. It does not alter the current
indexer or compacter and the receiver does not make a receipt until the exact
received bytes and cursor are fsynced.

The receiver listens with mandatory mTLS and independently matches the exact
client leaf-certificate SHA-256 fingerprint to the permitted
`(cluster_id, origin_node_id, source_id)` tuple. Its Ed25519 receipt is the only
authority the Hetzner source accepts for local whole-generation deletion.

## 1. Bootstrap the trust bundle offline

Run the helper on a trusted machine and use a destination outside the Git
checkout. It refuses to overwrite any existing path:

```sh
BLOCKZILLA_SOURCE_ID=grpc-raw-hetzner-backup \
  ./scripts/generate-grpc-replication-pki.sh /secure/path/replication-pki

BLOCKZILLA_SOURCE_ID=grpc-raw-hetzner-backup \
  ./scripts/generate-grpc-pull-pki.sh \
  /secure/path/replication-pki \
  /secure/path/pull-pki
```

`grpc-raw-hetzner-backup` is the immutable source identity already present in
the production Hetzner WAL. Do not rename it while that WAL is retained.

The first result creates the receiver certificate and the stable Ed25519
receipt key. The second adds a distinct reverse-pull server/client identity
under the same offline CA and copies only public receipt material. Its result is
deliberately split:

- `pull-pki/blockzilla/` contains the outbound client certificate/private key,
  source CA, strict client allowlist, and receipt public key. Keep it on
  Blockzilla.
- `pull-pki/hetzner/` contains the pull-source certificate/private key, client
  CA, strict allowlist, and receipt public key. Copy only this directory to the
  new Dokploy pull-source application.
- `replication-pki/offline/` still contains the only CA private key. Keep it
  offline; neither live host needs it.

Never commit any generated directory. The example allowlist's all-zero
fingerprint is intentionally unusable; use the generated allowlist instead.
For rotation, generate a new isolated bundle, add the new receipt public key to
the sender trust list, prove the new path, and only then retire the old key.

## 2. Prepare durable Blockzilla storage

Choose a dedicated path on the same durable filesystem as the future replay
consumer. Before starting Compose, create it as the container's fixed UID/GID:

```sh
sudo install -d -o 10001 -g 10001 -m 0700 \
  /volume1/blockzilla-ingest-receiver \
  /volume1/blockzilla-ingest-receiver-derived
```

The receiver owns two separate namespaces below that bind mount:

- `/data/blockzilla-ingest-primary/replication-receiver/` is the received WAL;
- `/data/replication-control/primary-term` is the crash-safe fencing term.

The optional replay bridge mounts that receiver directory read-only and writes
only to `/volume1/blockzilla-ingest-receiver-derived`. It never mutates or
deletes the canonical receiver WAL.

Do not place either path on tmpfs, an overlay layer, or a network mount without
durable `fsync` and atomic rename semantics.

The example uses a 16 TiB receiver-spool ceiling and preserves 4 TiB of free
space on the shared NAS filesystem. At the observed July ingest rate, 1 TB is
only about six days of raw data; the 1 TB value is the R2 safety budget, not the
NAS local-retention ceiling. Keep compactor/index lag as a separate alert and
do not reclaim receiver data merely because Hetzner has received an ACK.

## 3. Configure, validate, and keep it disabled

Copy `ingest-primary.json.example`, `grpc-pull-client.json.example`, and
`.env.example` outside the checkout, then set their host paths. In the pull
client JSON, make the receipt key id match the receiver's configured signing
key id. Point the receiver allowlist at the generated pull-client fingerprint
before starting this client; receipt verification must use the public half of
the receiver's existing signing key.

Validate without starting a service:

```sh
docker compose \
  --env-file /secure/path/blockzilla-receiver.env \
  -f docker-compose.blockzilla-receiver.yml \
  --profile pull-client config --quiet

cargo run --locked -p blockzilla-live-producer -- \
  validate-ingest-config \
  --config /secure/path/ingest-primary.json
```

The profiles remain inert while `COMPOSE_PROFILES` is empty. Setting it to
`pull-client` starts both `raw-receiver` and the outbound client. The client has
no data mount and no inbound port: it reaches the receiver only at
`https://10.253.45.2:9443` on the internal Docker bridge. Its other network is
outbound-only egress to `https://188.245.147.127:10443`, with TLS name
`blockzilla-hetzner-source`. Do not add `bridge` yet. The current bounded bridge
is suitable for controlled catch-up/recovery proofs, but is not the final
long-lived indexer feed.

`BLOCKZILLA_PULL_CLIENT_IMAGE` is intentionally a different tag from
`BLOCKZILLA_RECEIVER_IMAGE`. Build or pull the client under that dedicated tag;
never reuse the receiver tag for the client. During a canary against an already
running receiver, start only the client service so Compose does not recreate
the receiver merely because it is a declared dependency:

```sh
docker compose \
  --env-file /secure/path/blockzilla-receiver.env \
  -f docker-compose.blockzilla-receiver.yml \
  --profile pull-client build pull-client

docker compose \
  --env-file /secure/path/blockzilla-receiver.env \
  -f docker-compose.blockzilla-receiver.yml \
  --profile pull-client up -d --no-deps pull-client
```

These commands do not stop the emergency capture or the already-running
`legacy-tunnel` rollback service.

## 4. Direct outbound mTLS and legacy rollback

The NAS administration interface already owns host port `9443`, so the receiver
does not publish any host port. Instead, Compose creates an internal-only
bridge at `10.253.45.0/29`. The receiver stays at `10.253.45.2`; the pull client
joins that bridge plus a separate NAT egress bridge. The receipt-signing
receiver itself still has no Internet route.
The receiver also binds specifically to `10.253.45.2:9443`, so attaching it to
an unintended network does not make it listen there.

The pull client uses TLS name `blockzilla-primary` for the local receiver. For
the public source it connects to the Hetzner IP but verifies TLS name
`blockzilla-hetzner-source`. Both sessions require the generated client
certificate and the strict certificate-fingerprint allowlist. Confirm the
internal subnet does not overlap either host before deployment.

`cloudflared` is now behind the explicit `legacy-tunnel` profile. This removes
it from new direct-pull starts, but **do not stop an already-running connector**
until the direct source-to-receiver path passes the full durable proof below.
Keeping it during the canary is harmless and preserves rollback. Stop it only
in a later, separate change.

## 5. Cut over in proof-first order

1. Leave the existing NAS emergency capture and any running legacy tunnel
   untouched.
2. Start the new standalone Dokploy pull source with one event per batch and GC
   disabled. Confirm TCP `10443` is not behind Traefik or another TLS terminator.
3. Enable the NAS `pull-client` profile. Confirm the receiver owns a nonzero
   fencing term and that the client opens only outbound connections.
4. Verify an exact record is fsynced by the receiver, its signed cumulative ACK
   is accepted by Hetzner, and
   `/data/replication-control/pull-cumulative-ack.wal` survives source restart.
5. Restart the client and receiver independently and prove the exact duplicate
   is idempotent, then prove the received WAL can be replayed byte-for-byte.
6. Only after that proof may local ACK-covered oldest-generation cleanup be
   enabled in a separate source-config change. R2 cleanup stays disabled.

After the first durable receiver record, discover the single 32-hex journal
directory below `replication-receiver` and use it for a controlled, bounded
bridge proof. The bridge has no network access, shares the receiver's capacity
admission lock, mounts its source read-only, stops its derived output at 8 TiB,
and preserves 6 TiB of free NAS space. Its output is a separate watermark: it
is not a receipt, an indexing ACK, or permission to delete data. Do not add the
`bridge` profile to the continuous deployment until the persistent follower
and downstream lifecycle described above are implemented.

Before any bridge proof, compare `realpath` for both host bind paths. The
derived path must not alias, contain, or be contained by the receiver path.

File-backed Compose secrets retain their host inode and permissions. When a
certificate, private key, allowlist, or tunnel token is rotated, validate the
new files and use `docker compose up -d --force-recreate`; do not assume a
container sees a replaced inode without recreation.

This receiver stores durable raw replication data only. The current live
indexer/compacter must remain running until the separate replay adapter has
been deployed and its catch-up proven. R2 receipts, tunnel health, disk
pressure, and generic pings are never deletion authority.
