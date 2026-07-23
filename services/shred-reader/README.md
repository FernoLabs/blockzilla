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

When `REPAIR_ENABLED=true`, one isolated task observes every valid original Turbine copy, waits 200 ms
for normal delivery and local FEC recovery opportunity, then sends bounded Agave `WindowIndex` or
`HighestWindowIndex` requests to a maximum of eight gossip-discovered repair peers. Responses are
accepted only when the peer address and identity, request nonce, shred version, scheduled slot
leader signature, and exact Merkle/chained-Merkle FEC identity all match evidence learned from the
original Turbine path.

A wholly absent fixed 32-data + 32-coding FEC has no local root of its own. Chained Merkle shreds
solve this only in one safe direction: a directly trusted successor commits the exact missing FEC
root. After the first matching response is durably written, its leader-signed chained root can
anchor the preceding FEC, allowing consecutive gaps to be repaired strictly backwards. The
`repair_root_anchored_shreds_accepted_total` counter and per-accept debug fields make this path
observable. Unchained, forward-inferred, conflicting, or unverified roots fail closed.

Accepted responses are fsynced to `REPAIR_WAL_PATH`, whose filename must end in `.repair.wal` and
whose canary hard cap is 256 MiB. This WAL has its own header, checksum, lock, sequence, and source
provenance. It never mutates the raw shred journal or its replication ACK. If RPC lookup, peer
selection, the repair socket, verification, or this WAL fails, repair becomes inactive while raw
capture and forwarding continue. Repair counters and queue loss are exposed through the normal
metrics endpoint.

This first repair deployment is deliberately provenance-only: the NAS raw-shred audit does not yet
merge repair WAL records into its reconstruction result. That merger and an explicit promotion
policy require a separate validation step.

## Dokploy

Deploy [docker-compose.dokploy.yml](docker-compose.dokploy.yml) from the Blockzilla monorepo as a
**Git-backed** Compose application. The production compose pulls a CI-built GHCR runtime image so
the live Hetzner disk never has to hold Agave's large Rust build tree. `SHRED_READER_IMAGE` is
required and must contain the verified manifest digest, not a mutable tag. Keep the named
`receiver-data` volume across redeployments because identity stability improves gossip visibility.

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
