# Portable Hivezilla supervision

Date: 2026-07-17

Hivezilla includes a small process supervisor for long-lived source instances.
It provides the lifecycle controls normally supplied by systemd or a container
runtime without making either one a deployment requirement.

## Contract

`hivezilla supervise` owns one child process and provides:

- `never`, `on-failure`, and `always` restart policies;
- bounded exponential backoff;
- a restart burst/window fence that stops crash loops;
- an exclusive state-directory lock;
- optional tokenized readiness and heartbeat notifications;
- graceful termination followed by a bounded forced stop; and
- atomic, secret-free `status.json` snapshots.

The status file never contains the executable, arguments, environment, provider
URL, or credentials. On Unix, the child receives its own process group so a
graceful or forced stop also reaches descendants. Other platforms retain the
same configuration and status contract but currently terminate the direct
child only.

Example:

```bash
hivezilla supervise \
  --name mainnet-grpc-a \
  --state-dir /var/lib/blockzilla/supervisor/mainnet-grpc-a \
  --restart on-failure \
  --restart-burst 4 \
  --restart-window-secs 120 \
  --initial-backoff-ms 1000 \
  --max-backoff-ms 60000 \
  -- \
  /usr/local/bin/hivezilla record-grpc-raw [source arguments]
```

Use `--readiness-timeout-secs` to require an explicit notification. A child
inherits `BLOCKZILLA_SUPERVISOR_NOTIFY_FILE` and
`BLOCKZILLA_SUPERVISOR_NOTIFY_TOKEN`; code may use the library API, or an
operator script may invoke:

```bash
hivezilla notify-supervisor ready
hivezilla notify-supervisor heartbeat
```

Each attempt receives a new token. A stale notification from a previous child
cannot make its replacement ready. Heartbeat enforcement is opt-in and cannot
be enabled without readiness.

## What it does not do

A process supervisor cannot preserve an established outgoing Yellowstone gRPC
connection across executable replacement. Docker cannot do that either. The
portable continuity boundary is the Hivezilla WAL: reconnect resumes from the
durable cursor with overlap, while Blockzilla deduplicates and performs finite
offline validation.

For zero-downtime worker upgrades, keep the supervised Hivezilla source and WAL
stable, start the new Blockzilla consumer from its own committed cursor, let it
catch up and validate, then atomically promote it and drain the old consumer.

Inbound APIs can later use a separate socket-broker mode that owns the listening
socket while blue/green children change. That broker must not be confused with
outgoing source-stream durability and is intentionally not part of this first
supervisor implementation.

## Cloud-backed source spools

Remote Hivezilla instances may place sealed WAL generations in immutable object
storage while Blockzilla is offline. The supervisor can manage the recorder and
uploader processes, but its readiness or heartbeat is never a storage receipt.
Keep these transitions independent:

```text
local WAL durable
  -> cloud generation committed and version-pinned
  -> Blockzilla downloads, verifies, installs, and fsyncs the generation
  -> signed raw-durable ACK
  -> retention policy may consider cloud cleanup
  -> separate archive-committed receipt
```

Local pressure eviction may use a verified cloud commit when policy permits,
but an object-store credential, successful upload command, process heartbeat,
or mutable cloud head is insufficient. The deletion decision must remain bound
to the immutable generation identity and its durable receipt chain.
