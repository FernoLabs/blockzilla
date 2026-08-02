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
durable cursor with overlap, while downstream processing deduplicates and
performs finite validation.

For a terminal-consumer upgrade, keep the supervised source and WAL stable,
fence the old consumer before its replacement takes the dataset writer lock,
then resume from the dataset's committed cursor. The terminal store identity
names the durable dataset, not one process instance; a replacement dataset must
use a new identity and cannot inherit old ACK authority.

Inbound APIs can later use a separate socket-broker mode that owns the listening
socket while blue/green children change. That broker must not be confused with
outgoing source-stream durability and is intentionally not part of this first
supervisor implementation.

## Target: node-owned cloud overflow

The supervisor behavior above is implemented. The following minimal V1 storage
flow is a target contract and is not yet implemented end to end by the
supervisor.

Each source Hivezilla has its own private bucket or IAM-isolated namespace. It
may move sealed WAL segments there when local disk is under pressure. This
bucket is temporary overflow for that node's logical spool, not a second
custodian and not Archive V2 storage. The supervisor can manage recorder and
uploader processes, but readiness or heartbeat is never a storage receipt.
Keep these transitions independent:

```text
local WAL durable
  -> sealed segment uploaded with verified end-to-end checksum and catalogued
  -> local copy may be evicted under pressure; logical record is still live
  -> terminal consumer reconnects live at cutover T
  -> background fetches bounded record ranges [C, T) from disk/cloud
  -> terminal writes permanent exact objects to independent policy targets
  -> terminal persists a rebuildable range/copy index
  -> terminal sends one cumulative ACK for the contiguous permanent prefix
  -> source fsyncs ACK + retirement anchor, then deletes covered disk/cloud copies
```

Live and bulk transfer need separate resource budgets so catch-up cannot starve
capture or the live lane. Bulk ranges may complete out of order, but the one
configured terminal raw consumer may ACK only the exact contiguous prefix; a
later chunk cannot jump a hole.

A provider-verified end-to-end checksum (or verified read-back) plus the durable
range catalog may authorize eviction of the corresponding local copy. It never
retires the logical records.
An object-store credential, successful upload command, process heartbeat,
mutable cloud head, derived block, or archive commit cannot authorize deletion
from both tiers. A persisted terminal ACK only makes covered data eligible; the
source must then fsync its accepted-ACK receipt and the receipt-bound retirement
checkpoint before deleting it.

Archive work is separate from source supervision. In the V1 target, Blockzilla
owns scheduling and the canonical catalog, while a Hivezilla compact worker
executes one fenced job, uploads candidate Archive V2 objects, and waits for
Blockzilla's conditional catalog commit. Neither that worker nor the commit is
a raw-custody ACK.
