# shred-reader

An experimental, zero-stake Solana Turbine receiver. It runs the minimum Agave control plane needed to advertise a TVU endpoint and observe any shreds assigned to the node:

- persistent Ed25519 identity;
- active two-way gossip;
- signed `ContactInfo` with public gossip and TVU addresses;
- raw TVU UDP reception;
- structural shred parsing, version filtering, bounded deduplication, and metrics.
- optional byte-for-byte UDP fan-out to explicitly configured downstream consumers.
- optional bounded Agave repair for recent, leader-verified FEC gaps, persisted to a separate
  provenance WAL.

It intentionally does **not** run voting, replay, AccountsDB, snapshots, transaction execution,
blockstore, or Turbine retransmission.

## Important limitation

Agave includes known zero-stake TVU peers in the Turbine node set, but places zero-weight identities after staked validators. Their exact positions also depend on each validator's local gossip view. Receiving shreds is therefore possible, but sparse or zero traffic is a valid experimental outcome—not proof that the process is disconnected.

This first stage answers three separate questions:

1. Can the node discover and remain connected to gossip peers?
2. Is its public gossip/TVU endpoint reachable and correctly advertised?
3. Does current mainnet Turbine assign it unsolicited shreds?

## Network ports

Defaults:

| Port | Protocol | Purpose |
| --- | --- | --- |
| `18001` | UDP | Solana gossip |
| `18001` | TCP | Solana IP echo/reachability service |
| `18002` | UDP | TVU shred reception |
| `19090` | TCP, loopback only | health and JSON metrics |

The gossip and TVU ports must be allowed through both the Hetzner Cloud Firewall and the host firewall. The deployment uses host networking so the advertised ports match the real UDP sockets without Docker NAT ambiguity.

## Run locally

Local execution can join gossip, but unsolicited TVU traffic is unlikely unless the machine has a public IP and forwarded UDP ports.

```bash
cargo run --release
```

Public reachability probing is disabled by default for local development. The Dokploy Compose deployment enables it and refuses to start unless an entrypoint can reach both UDP ports and the gossip TCP port.

The process discovers its public IPv4 address and current shred version through the configured Solana entrypoint. Override them only when necessary:

```bash
ADVERTISED_IP=203.0.113.10 SHRED_VERSION=50093 cargo run --release
```

See [.env.example](.env.example) for all settings. The application does not automatically load `.env` files.

## Observe it

```bash
curl http://127.0.0.1:19090/healthz
curl -i http://127.0.0.1:19090/readyz
curl http://127.0.0.1:19090/metrics
```

`/healthz` means the process and HTTP endpoint are alive. `/readyz` requires a compatible peer
record, a structurally valid same-version shred, at least one forwarding target, and a successful
forward within the last 60 seconds, with no forwarding error in the last 15 seconds. Old CRDS or
TVU activity therefore cannot keep the service ready indefinitely, and sustained queue loss or a
failing destination makes it unready. UDP success still is not a durable recorder acknowledgement; end-to-end
monitoring must compare this service's counters with Hivezilla's durable recorder status.

The process also emits a metrics summary every ten seconds and a sampled shred record at most once every fifteen seconds.
The summary includes host UDP, softnet, and selected NIC receive-drop deltas so an AF_XDP
experiment can be justified by measured kernel loss instead of assumed packet pressure. Startup
logs explicitly report `receive_backend=tokio_udp` and
`af_xdp=disabled_pending_measured_kernel_loss`; the first AF_XDP experiment remains a later,
separately gated change.

## Forward shreds to consumers

Set `SHRED_FORWARD_ADDRS` to a comma-separated list of UDP destinations. Every
structurally valid, same-version shred is forwarded byte-for-byte; duplicates
are intentionally preserved so each downstream durability boundary can make
its own replay/conflict decision.

```bash
SHRED_FORWARD_ADDRS=127.0.0.1:18003,192.0.2.20:18003 cargo run --release
```

By default the forwarding socket keeps the existing wildcard-IP, ephemeral-port behavior. For the
green cutover durability proof, give green a source port that no other local sender uses and target
the same-host Hivezilla listener:

```bash
SHRED_FORWARD_BIND_ADDR=127.0.0.1:18104
SHRED_FORWARD_ADDRS=127.0.0.1:18003
```

Reserve `127.0.0.1:18104` for green for the full proof window; blue must use a different port. The
downstream recorder should allowlist the complete source address, not merely the port. A fixed bind
must have a nonzero port and use the same IP family as every destination. Leave it unset when the
destination is remote or fixed attribution is not required. The effective sender address is logged
at startup and exposed as `forward_sender_addr` in the metrics JSON, including the chosen ephemeral
port when no fixed bind is configured.

The TVU loop puts each valid datagram into a bounded forwarding queue and immediately resumes
receiving. A dedicated task waits for UDP socket readiness, so a temporarily slow destination
does not stall TVU reception until that queue is exhausted. Repeated Turbine observations are
forwarded rather than suppressed, so a later duplicate can recover from an earlier kernel or
recorder-queue loss. A full forwarding queue or invalid destination increments
`forward_queue_dropped_total`; destination send errors increment `forward_send_errors_total`.
Their sum remains available as `forward_errors_total`. Successful destination sends increment
`forwarded_datagrams_total` and refresh forwarding readiness. Queue depth and accepted enqueue
count are exposed separately, so their units are never confused with per-destination sends.
Loopback UDP is the intended first Hivezilla integration because
both services run on the same Hetzner host. `FORWARD_QUEUE_CAPACITY` defaults to 16,384 datagrams.

## Bounded live repair

When `REPAIR_ENABLED=true` (the live Compose default), an isolated supervisor observes every valid
original Turbine copy, waits 200 ms for normal delivery and local FEC recovery opportunity, then
sends bounded Agave `WindowIndex` or `HighestWindowIndex` requests to a maximum of eight
gossip-discovered repair peers. Responses are accepted only when the peer address and identity,
request nonce, shred version, scheduled slot leader signature, and exact Merkle/chained-Merkle FEC
identity all match evidence learned from the original Turbine path.

A wholly absent fixed 32-data + 32-coding FEC has no local root of its own. Chained Merkle shreds
solve this only in one safe direction: a directly trusted successor commits the exact missing FEC
root. After the first matching response is durably written, its leader-signed chained root can
anchor the preceding FEC, allowing consecutive gaps to be repaired strictly backwards. The
`repair_root_anchored_shreds_accepted_total` counter and per-accept debug fields make this path
observable. Unchained, forward-inferred, conflicting, or unverified roots fail closed.

Accepted responses are fsynced to the segmented journal rooted at `REPAIR_WAL_PATH`, whose filename
must end in `.repair.wal`. `REPAIR_WAL_MAX_BYTES` is a per-segment rotation target (256 MiB by
default), not a lifetime cap. Segment zero keeps the configured legacy path and v2 header. Later
segments are adjacent files named `<stem>.segment-<20-digit-id>.repair.wal`; the highest id is the
active segment and all lower ids are immutable. Frame sequences are global and contiguous. Each v3
segment header binds its id and first sequence to the preceding segment's final sequence and
SHA-256 chain digest, while every unchanged v2 frame retains its own CRC and complete source
provenance. Startup validates every retained byte. An incomplete frame, including one that could be
either an interrupted write or a corrupted length prefix, fails closed without truncating or
changing the file; recovery requires an explicit offline proof. New headers are synced under an
ignored staging name and atomically published without replacement, so a crash cannot expose a
partial next segment and an existing destination is never overwritten.

The writer never overwrites or deletes a sealed segment. Consequently rotation prevents the first
256 MiB boundary from disabling repair, but it does not make disk capacity infinite. Total retained
bytes have warning, critical, and hard thresholds: 1 GiB
(`REPAIR_WAL_TOTAL_WARNING_BYTES`), 2 GiB (`REPAIR_WAL_TOTAL_CRITICAL_BYTES`), and 4 GiB
(`REPAIR_WAL_TOTAL_HARD_BYTES`) by default, in strictly increasing order. Every append reserves its
entire frame/rollover cost before writing and must also leave
`REPAIR_WAL_FILESYSTEM_RESERVE_BYTES` free (8 GiB by default) for raw capture and other writers on
the shared filesystem. Crossing the hard ceiling or filesystem reserve stops only repair; it never
deletes sealed data or compromises the raw forwarding readiness path.

The metrics endpoint is initialized from a read-only WAL inventory before peer/leader readiness, so
an oversized or corrupt generation cannot appear as zero bytes or a healthy admission state. It
reports retained and active bytes, segment count/id, rollover count, warning/critical/hard state,
filesystem availability/reserve state, validation/open error, and the exact inclusive
`repair_wal_durable_through_sequence`. NAS replication must copy and verify frames only through that
durable sequence; deletion remains forbidden until a durable consumer ACK retirement protocol is
implemented.

Read-only consumers discover one base file plus the exact sibling pattern above, reject symlinks or
segment-id gaps, verify each header/digest and frame CRC, and stop at the supplied inclusive durable
sequence. The writer-wide `<base>.writer.lock` plus the legacy base-file lock prevent a concurrent
cooperative writer while the process is active; locks alone provide no post-exit downgrade safety.
Before publishing segment 1, the writer first atomically publishes `<base>.v3-head`, then creates
`<base>.v3-seal`, which binds the legacy base's terminal sequence and full chain digest, and makes
segment zero read-only. The head is a CRC-protected checkpoint containing the active segment id,
exact active-file length, next global sequence, and terminal SHA-256 chain digest. After every WAL
sync, the writer writes and syncs a same-directory temporary head, atomically renames it, and syncs
the parent directory before returning the durable ACK. This detects deletion of the highest
segment and clean frame-boundary truncation rather than silently rolling back and reusing sequence
numbers. On restart, only the exclusive writer may preserve a fully proven WAL-ahead crash tail and
advance the head; read-only inspection requires an exact terminal match. A head without a seal is
the one recoverable transition state. A seal without a head, a missing/behind segment, an orphan
control file, or any digest/length mismatch fails closed without truncation. The production image
runs as unprivileged UID 10001 with capabilities dropped, so an older v2 binary cannot reopen the
base for append after the transition. Segment zero remains readable, but downgrading to a v2 writer
is unsupported and requires an explicit offline migration; do not treat file locks as a downgrade
gate.

All WAL discovery, validation, append, and fsync work runs on one ordered dedicated thread. A repair
shred is acknowledged to the async runtime only after its durable WAL append completes. The
repair UDP socket is drained continuously by a separate receive-only Tokio task into a bounded
`REPAIR_RESPONSE_QUEUE_CAPACITY` channel (4,096 datagrams by default). Parsing, nonce correlation,
peer/leader/FEC trust, and WAL admission remain serialized in the repair runtime; the receive task
cannot accept a shred. A full userspace channel drops only that repair response and increments
`repair_response_queue_dropped_total`, without backpressuring raw TVU capture. The repair socket
requests `REPAIR_UDP_RECV_BUFFER_BYTES` (64 MiB by default), reports the effective kernel value,
and on Linux reports its own `SO_RXQ_OVFL` counter separately from host-wide UDP loss. Responses
are serviced every 5 ms independently of the 50 ms request-planning cadence, while every accepted
record still waits for its ordered durable WAL acknowledgement. On shutdown, repair ingress is
closed first, the fixed staged queue is drained through that same serialized validation and
fsync-before-accept path, and the WAL is flushed. The drain has a 15-second between-record budget;
if it cannot finish, the shutdown error reports the exact staged remainder (and any dequeued
unresolved datagram) and propagates to the process as a non-clean shutdown instead of silently
discarding the queue. Filesystem availability is refreshed by the blocking WAL worker at most once
every five seconds even when repair is idle, so raw-spool growth on the shared filesystem is
reflected without blocking TVU receive. A failed refresh exposes availability as unknown and keeps
the WAL error visible until a later check or admitted append succeeds. The supervisor retries transient
initialization/runtime failures with exponential backoff from 1 to 60
seconds; storage-full, invalid-data, and permission failures use a 15-minute held retry to avoid
repeated large scans or disk thrash. `repair_state`, bounded last-error text, restart count, and last
success time expose that lifecycle. Repair health deliberately does not participate in `/readyz`:
raw Turbine receive and forwarding remain the primary service boundary.

Forward shutdown is also fail-closed. `forward_queue_depth` counts every unfinished datagram,
including at most one datagram currently awaiting its configured destinations. Shutdown permits a
20-second forward drain; a timeout aborts the worker and returns a non-clean process result with the
unfinished count. The forwarding and repair ceilings run concurrently (20 and 45 seconds), so the
production 60-second container grace leaves roughly 15 seconds for the remaining orderly joins.

Repair remains quarantined from the live raw journal and archive promotion path. The release
`shred-epoch-audit` can now opt in to a frozen accepted-repair prefix, independently revalidate its
provenance, and report raw-only versus raw-plus-repair reconstruction. Promotion and repair-segment
retirement still require an explicit policy and durable consumer acknowledgement.

## Dokploy

Deploy [docker-compose.dokploy.yml](docker-compose.dokploy.yml) from the Blockzilla monorepo as a
**Git-backed** Compose application. The production compose pulls a CI-built GHCR runtime image so
the live Hetzner disk never has to hold Agave's large Rust build tree.
`SHRED_READER_IMAGE_DIGEST` is required and is appended after `@`, so Docker rejects a mutable
tag-only deployment. `SHRED_READER_IMAGE_REPOSITORY` defaults to the production GHCR repository.
Keep the named `receiver-data` volume across redeployments because identity stability improves
gossip visibility.

Before starting it:

1. Open `18001/tcp`, `18001/udp`, and `18002/udp` on the cloud and host firewalls.
2. Confirm no other Solana TVU identity advertises the same public IP.
3. Leave `ADVERTISED_IP` and `SHRED_VERSION` unset unless entrypoint discovery fails.
4. Do not configure a Dokploy domain or isolated network for this service; host networking must remain intact.
5. Keep metrics bound to loopback unless protected monitoring access is added.
6. Confirm host time synchronization and raise `net.core.rmem_max` if the logged effective UDP receive buffer is below the requested value.

The Agave dependency graph includes native RocksDB/protobuf code even though this receiver does not open a ledger. The first image build is therefore intentionally heavy; allow ample build disk and time on the Docker host.

## Secrets

Never commit Dokploy tokens, validator identity files, or private keys. The `secrets/` and `data/` directories are excluded from Git and Docker build contexts. Deployment credentials are not read by the application.

## Current validation boundary

Ordinary forwarding preserves every structurally parsed, same-version Turbine datagram so the
downstream durable recorder can resolve duplicates. The stricter repair trust path independently
sanitizes the original packet and verifies its slot-leader signature before learning a FEC identity.
Repaired data is intentionally quarantined in the provenance WAL until the audit tooling can merge
and compare it without weakening the raw-journal durability boundary.
