# Current-server shred-reader hardening rollout

This runbook upgrades the existing Nuremberg receiver without introducing a known raw-capture
gap. It does not add a second Hivezilla recorder or depend on the paused Helsinki server.

## Proven live baseline

Before changing anything, preserve a timestamped copy of the public status and receiver metrics.
The current baseline is acceptable only while all of these are true:

- TVU and Hivezilla both report `receiving`, with fresh timestamps and advancing slots;
- forwarding has one target and zero queue drops, send errors, and total errors;
- Hivezilla's durable sequence advances and its invalid-record counter does not;
- the spool periodically shrinks, proving recent ACK-authorized segment retirement;
- the filesystem remains above its configured reserve.

The public status treats repair as an isolated side path. Missing or malformed repair telemetry
must make only the `repair` section unavailable; it must never erase otherwise valid TVU,
forwarding, or Hivezilla health. This keeps a repair startup/backoff problem from producing a
misleading "raw receiver unavailable" alert.

The live host has shown UDP `RcvbufErrors` while NIC and softnet drop counters stayed at zero. The
reader requested 64 MiB but received an effective 15,000,000-byte buffer. Do not tune XDP,
`net.core.netdev_max_backlog`, IRQ affinity, or NIC offloads without new evidence.

## Candidate gates

Do not deploy a candidate until all of these gates pass:

1. The immutable source manifest and image digest are recorded.
2. Locked Rust checks and the complete shred-reader tests pass on Linux.
3. The frozen 10,000-slot raw-only control reproduces its stored manifest and hashes.
4. Two repair-assisted audit passes produce identical hashes and leave the frozen repair WAL
   unchanged.
5. The candidate exposes TVU and repair-socket `SO_RXQ_OVFL` separately.
6. The segmented repair WAL opens a legacy v2 journal and rolls safely without truncating,
   overwriting, renumbering, or deleting a segment.

The branch-only workflow
`.github/workflows/current-shred-reader-linux-gate.yml` is deliberately read-only: it tests the
Bookworm receiver image and the AES/SSE2 Bookworm reconstruction-tool image from
`services/hivezilla/Dockerfile.shred-audit`, but it never logs in to a
registry, publishes a mutable tag, or deploys. Keep this work off
`codex/shred-receiver-hardening`, because that older branch is watched by the production publish
workflow. A green branch gate is evidence for review, not deployment authority.

The base-image digests and Cargo lockfile make the selected bases and Rust dependency graph
repeatable, while Debian packages installed by `apt-get` are intentionally not claimed to be
bit-for-bit rebuildable. Record and deploy the resulting application image digest; a later rebuild
of the same commit is a new artifact and must pass the gates again.

## Host socket ceiling

Install `services/shred-reader/host/90-shred-reader-sockets.conf` as
`/etc/sysctl.d/90-shred-reader-sockets.conf`, then apply the sysctl configuration. Verify the
values directly:

```sh
sysctl net.core.rmem_max net.core.wmem_max
```

Expected value for both maxima: `134217728`. Leave `net.core.rmem_default` unchanged. Applying the
ceiling does not resize an existing socket; a newly started receiver must log an effective receive
buffer of at least `134217728` after requesting 64 MiB.

## Start green before stopping blue

A direct replacement creates wholly unseen slots during restart and is forbidden. Start a second
receiver process on this same host while the current process remains live:

- run it as a distinct Compose project (`COMPOSE_PROJECT_NAME=shred-reader-green`), never by
  updating the existing Dokploy application in place;
- use a separate persistent identity volume;
- use different public gossip/TVU ports and a different loopback metrics port;
- open only those exact gossip TCP/UDP and TVU UDP ports in both firewalls;
- bind its forwarding socket to the exact source `127.0.0.1:18104`;
- during the first recorder upgrade, forward to both the incumbent Hivezilla recorder at
  `127.0.0.1:18003` and the isolated candidate recorder described below;
- use a new, empty repair WAL generation and preserve the old volume unchanged;
- pin the candidate image by digest, never by a mutable tag.

Before `up`, render the Compose model and verify that the project, container, and volume names all
contain `shred-reader-green`; abort if the volume resolves to the blue receiver's volume. The
production Compose stop grace must remain longer than the receiver's 45-second repair shutdown
ceiling (currently 60 seconds).

Duplicate shreds during overlap are semantically safe: Hivezilla preserves observations and its
reconstruction path resolves exact duplicates later. They still double receiver-to-Hivezilla load,
so overlap is acceptable only while the blue TVU loss counter, Hivezilla socket-overflow counter,
Hivezilla queue-drop/backpressure counters, and filesystem reserve remain unchanged/healthy. Do
not create another long-lived Hivezilla recorder.

Keep blue live until green has passed reachability, joined gossip, received current-version TVU
traffic, forwarded successfully, and remained ready through a representative busy interval. A
shared Hivezilla durable cursor is **not** proof that green reached disk: blue can advance it while
green's UDP `send_to` succeeds without durable receipt. Blue must not stop until a green-specific
end-to-end commit proof shows that a green-origin datagram was included in an fsynced Hivezilla
group. Until that proof exists in the protocol/metrics, the cutover is blocked. Also verify that
green's per-socket overflow counter is supported and does not increase. If any gate fails, stop
green and leave blue untouched.

### First Hivezilla upgrade: bridge through an isolated candidate

The incumbent recorder does not yet publish source-specific post-fsync evidence. Do **not** restart
it merely to add that metric; its UDP input has no replay handshake, so a restart would create an
avoidable capture hole. Use a same-host recorder overlap for this first upgrade:

1. Start `hivezilla-green` on `127.0.0.1:18013` with a separate Compose project, data volume,
   status path, node/source identity, journal ID, and pull-source port. Its allowlist must contain
   `green=127.0.0.1:18104` and reserve `final=127.0.0.1:18204` before it starts. Never point two
   recorder writers at one spool.
2. Start the green bridge receiver with
   `SHRED_FORWARD_BIND_ADDR=127.0.0.1:18104` and both forwarding targets
   `127.0.0.1:18003,127.0.0.1:18013`. One forwarding socket sends each accepted datagram to both
   recorders, so the candidate can attribute the exact green source address after its group fsync.
3. Require two fresh samples in which `hivezilla-green.durable_sources.green` gains at least 1,000
   committed datagrams, its timestamp/sequence/slot advance, and green TVU overflow, forwarding
   loss, candidate socket overflow, queue drops/backpressure, and free-space reserve remain clean.
4. Replicate the candidate journal to a separate NAS receiver stream and prove its cumulative ACK
   is advancing. The extra recorder is a temporary migration boundary, not a second permanent
   Hivezilla.
5. Only then stop the original blue receiver. Keep both recorders live while the bridge receiver
   continues sending to both.
6. Start the final receiver with `SHRED_FORWARD_BIND_ADDR=127.0.0.1:18204` and only the candidate
   target `127.0.0.1:18013`. Apply the same two-sample +1,000 post-fsync proof to
   `durable_sources.final`, then stop the dual-target bridge receiver.
7. After the candidate NAS ACK covers the complete overlap and a frozen audit finds no unexplained
   produced-slot loss, stop the now-idle incumbent recorder. Preserve its volume read-only until
   the overlap report and hashes are stored.

This sequence always leaves at least one proven receiver-to-recorder path live. If the host cannot
carry the temporary duplicate TVU, disk, and NAS load without flat loss counters and adequate free
space, abort the overlap and leave the incumbent pair untouched.

Only after a recorded overlap window may blue be stopped. Preserve its identity and repair volume
until a post-cutover reconstruction audit proves the window. There is no rollback that deletes or
rewrites ingest data.

## Post-cutover acceptance

Record at least the following for the exact cutover window:

- first/last durable raw sequence and slot;
- finalized `getBlocks` manifest hash;
- reconstructed, incomplete, RPC-skipped, and never-observed produced-slot counts;
- FEC threshold deficits and integrity/fork conflict categories;
- TVU socket overflow, Hivezilla socket overflow, queue high-water, and backpressure deltas;
- receiver/Hivezilla restart counts and NAS ACK lag.

The current receiver becomes the sole source only after the audit shows no unexplained produced
slot loss. A second geographic receiver remains a later independent improvement, not a prerequisite
for this rollout.

Run the amd64 reconstruction image only after the target reports the required CPU feature:

```sh
grep -qw aes /proc/cpuinfo
```

Mount every frozen raw/repair input read-only, use a read-only container root plus a small `/tmp`
tmpfs, and mount only the report destination read-write. The image runs as an unprivileged user;
never run the frozen audit with a writable ingest mount.

## Remaining retention boundary

Segment rotation removes the old 256 MiB single-file failure, but it is not deletion authority.
Repair segments must not be retired until a durable NAS/R2 consumer verifies their exact sequence
and chain digest and returns a persisted cumulative ACK. Until that protocol exists, the configured
total repair-WAL hard ceiling remains intentionally fail-closed and repair storage must be alerted
before it reaches the limit; raw forwarding must remain unaffected.
