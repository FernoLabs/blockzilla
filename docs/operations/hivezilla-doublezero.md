# Hivezilla and DoubleZero

Status: prepared, not connected. DoubleZero must remain a host networking service; its identity
must never be copied into a Hivezilla container or committed to this repository.

DoubleZero exposes two different integration paths. They solve different problems and must not be
treated as interchangeable:

| Path | What Hivezilla receives | Host interface | Activation gate |
| --- | --- | --- | --- |
| Permissioned IBRL | Existing gossip, Turbine, and repair traffic routed over DoubleZero where a route exists | `doublezero0` | DoubleZero Foundation allow-list approval |
| DoubleZero Edge | A direct Solana multicast shred feed on UDP `7733` | `doublezero1` | Subscriber entitlement or paid seat, usage-rights confirmation, and a separate Hivezilla journal |

IBRL does not make a zero-stake Turbine receiver complete. It can improve paths to participating
peers, but the receiver remains subject to Turbine assignment. DoubleZero Edge is the direct feed.

The published permissioned non-validator intake is currently shaped around at least two RPC hosts
in distinct metros. Hivezilla is a single non-validator ingest node, not a validator and not an RPC
pair. Ask the Foundation to approve and classify this topology; do not invent a validator identity
or a second host to satisfy the form.

Official references:

- <https://docs.malbeclabs.com/setup/>
- <https://docs.malbeclabs.com/Permissioned%20Connection/>
- <https://docs.malbeclabs.com/Edge%20Subscriber%20Connection/>
- <https://docs.malbeclabs.com/troubleshooting/>

## Shared safety rules

1. Install `doublezero` directly on the host, not in Docker.
2. Generate the identity as the host operator. Keep `~/.config/doublezero/id.json` mode `0600` and
   its parent directory mode `0700`.
3. Never mount that identity, the daemon socket, or host network-management capabilities into
   Hivezilla.
4. Treat `/run/doublezerod/doublezerod.sock` as a privileged local control surface. The current
   daemon makes it locally writable; do not run untrusted host users or expose the socket through a
   container.
5. Freeze a durable ingest cursor and record the exact activation time before changing routes or
   starting a new source.
6. Keep the current Turbine journal running. Every new feed gets a distinct source ID, journal ID,
   spool, status file, receiver allow-list entry, and acknowledgement cursor.
7. A valid DoubleZero tunnel is not proof that Hivezilla received every shred. Completeness still
   requires a finalized slot audit and sampled final-PoH-to-blockhash checks.

## IBRL readiness and activation

The existing Dokploy services use host networking. Once `doublezerod` installs selected routes,
the running shred reader's ordinary gossip and repair sockets use those routes without a Hivezilla
restart or code change.

Do not run `doublezero connect ibrl` until the Foundation confirms that the exact output of
`doublezero address` and the host's public IPv4 are allow-listed. Check without changing state:

```bash
doublezero status
doublezero latency
doublezero access-pass list | awk -v id="$(doublezero address)" 'NR == 1 || index($0, id)'
```

Immediately before activation, save these read-only baselines outside the source spool:

```bash
date -u
ip -brief address
ip -4 route show
ip rule show
doublezero status
curl --fail --silent http://127.0.0.1:19090/metrics
```

After approval, the activation command from the permissioned guide is:

```bash
doublezero connect ibrl
```

Wait up to one minute, then require all of the following:

- `doublezero status` is `up` and names `doublezero0`;
- the original default route and public IPv4 are unchanged;
- only expected peer routes use `doublezero0`;
- Dokploy remains reachable;
- the shred reader remains ready and forwards fresh shreds;
- the Hivezilla durable sequence and NAS signed acknowledgement continue advancing.

If any invariant fails, stop the canary with `doublezero disconnect`, verify that `doublezero0` and
its routes disappear, and preserve the before/after evidence. Do not restart or delete either raw
journal to hide a failed route canary.

## DoubleZero Edge canary

The Edge subscriber documentation currently identifies these Solana groups on UDP `7733`:

| Feed | Multicast address |
| --- | --- |
| Leader shreds | `233.84.178.1` |
| Root shreds | `233.84.178.16` |
| EU retransmit shreds | `233.84.178.12` |

The checked-in canary intentionally joins only the leader feed. The current recorder accepts one
multicast group per process, and all three feeds share port `7733`; running three independent
listeners is not yet a supported production topology. Start with one isolated source and measure
it before designing a combined multi-group adapter.

The Edge terms shown in the official guide restrict retransmission and describe the feed as for
internal use. Obtain written confirmation that the intended Hetzner-to-NAS replication, derived
block construction, archive publication, and client access are permitted before recording beyond
a bounded internal test. Do not purchase a seat or enable the reconciler merely to test the
checked-in configuration.

Once entitlement, usage rights, the host firewall, and `doublezero1` are confirmed, validate the
secret-free config locally:

```bash
cargo run --locked -p hivezilla --bin hivezilla -- \
  validate-ingest-config \
  --config services/hivezilla/config/ingest-doublezero-edge.example.json
```

The manual canary is opt-in, never restarts automatically, writes to a dedicated named volume,
fails closed after 2 GiB, and preserves at least 8 GiB of filesystem free space:

```bash
export DOUBLEZERO_EDGE_JOURNAL_ID="$(openssl rand -hex 16)"
docker compose \
  -f <path-to-your-deployment>/docker-compose.hivezilla-doublezero-edge.canary.yml \
  --profile doublezero-edge-canary \
  up --build hivezilla-doublezero-edge-canary
```

Generate that journal ID once, keep it in the private Dokploy environment, and reuse it for every
restart of the same named volume. A new ID denotes a deliberately new physical journal; it is not
a restart token.

Stop it manually after the bounded observation window. Do not remove the named volume. Confirm
that `recorder.json` reports fresh durable sequences, plausible Solana slots and shred versions,
zero or bounded invalid datagrams, and enough disk reserve. Also capture host UDP receive errors,
softnet drops, interface drops, CPU, compression throughput, and spool growth. The recorder's
status plus collector expose the recorder socket's Linux `SO_RXQ_OVFL` support and cumulative
overflow counter. Keep the independent host UDP, softnet, and interface counters as well: a
successful process status is not sufficient evidence of lossless capture.

## Promotion gates

Do not promote either path as an archive-grade source until a fixed post-activation range has been
audited against finalized `getBlocks`. Compare Turbine-only, DoubleZero-only, and deduplicated-union
results. The union must improve reconstruction without fork-identity, chained-root, FEC parity,
component-decoding, or final-PoH/blockhash regressions.

Before a long-running Edge deployment, add and test:

- a combined multi-group receiver or another conflict-free port/group strategy;
- verified zero-delta kernel, socket, queue, and forwarding drop telemetry;
- a dedicated NAS pull stream and allow-list tuple;
- signed acknowledgement monitoring and capacity projections at measured feed rate;
- explicit behavior when `doublezero1` disappears and returns; and
- watcher UI rows that keep Turbine and DoubleZero health separate.
