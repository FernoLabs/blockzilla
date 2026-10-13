# Foundation machine watch

Alert name: **Megaloadon**. Machine ID: `foundation`.
The operating-system hostname is currently `blockzilla`.

## Installed on 13 October 2026

An external Cloudflare Worker runs in the existing Blockzilla account:
`https://blockzilla-foundation-watch.cheron-augustin.workers.dev`.
Its source is [the Worker project](../../../edgezilla/foundation-watch).
Private status is `GET /status`, with the separate operator Bearer token.
There is no public status page with machine telemetry.

The Foundation sender runs as `hivezilla-foundation-heartbeat.service`, started
by `hivezilla-foundation-heartbeat.timer` every 30 seconds. Its current immutable
release is `ROOT/releases/foundation-watch-status-r2-20261013/heartbeat.py`, with
the pinned sibling `archive_status.py`, where
`ROOT=/home/ubuntu/live/hivezilla-validator-20260930`.
The service reads capture, compact, upload and storage reports, local RPC,
active writer locks and required service states. It changes only its own report.
Incomplete telemetry still sends a machine heartbeat with fixed error codes.
The strict service's first heartbeat was accepted with no collection errors.

The external watcher is armed. It received repeated live heartbeats, recorded
no current faults, and observed two new blocks on the free public blockhash
feed. Its Durable Object alarm checks every 30 seconds. A one-minute cron also
checks the saved state and repairs a missing alarm. These timers continue after
this chat or the local computer stops.

**Telegram notifications are active.** The owner selected Telegram and the name
**Megaloadon**, with the word play “mega load on”. The new
[Megaloadon Alerts bot](https://t.me/megaloadon_alerts_bot) was created through
the owner's Telegram app. The owner explicitly confirmed the exact private
recipient after Telegram delivered only `/start` rather than the setup phrase.
The new bot token and private chat ID are Worker secrets. Neither value is in
this repository. The external Worker sent a TEST event, checked the successful
Telegram reply and exact destination, and the same event ID was visible in the
native Telegram app. The outbox was empty, delivery had no error, and current
machine and public-stream checks had no fault. The external check runs without
the local computer or this chat. `POST /test-alert` requires the operator token
and sends a message clearly marked TEST; it does not stop capture. See
[the Telegram delivery instructions](../../../edgezilla/foundation-watch/README.md)
and [the delivery receipt](evidence/telegram-setup-20261013.json).

### Private status commands, 13 October

The bot now supports `/status` and `/help` in the owner-confirmed private chat.
The command menu and authenticated webhook are registered. A real `/status`
request and its reply were visible in the native Telegram app, with an empty
command outbox and no delivery error. Commands only read the latest heartbeat;
they cannot change capture, compaction, uploads or storage. A separate new
webhook secret authenticates incoming requests, and both numeric sender and
private chat must match the configured owner. Reply dates use Europe/Paris.

The heartbeat also reads the fixed CAR job's selected completion receipts,
stage status, and bounded tails of readable progress logs. It verifies receipt
identity, query/signature profile and remote size proof. Stage ETA is separate
from estimates for upload completion and the saved plan. History estimates use
the last two completed runs and current input size, with a margin; pauses or
missing progress suppress them. Old progress is not renewed by a new heartbeat.
CAR checksum verification has its own state and never reuses the old download
transfer ETA. No raw log, private recipient ID or credential enters a reply.

Epochs 1048–1050 have checked query/signature upload receipts, totalling 355.94 GB.
At 06:44 Europe/Paris, epoch 1051 was 66.3% compacted, with a stage ETA of
about 16 minutes and a recent-run estimate of about 25–40 minutes to upload
completion. The plan through 1054 was about 5–7.5 hours. Live compaction had
zero slot lag; the cloud queue was 192 events. The real 06:43 command reply
also verified Paris dates and rounded history estimates. These are dated
samples; use `/status` for the current values.

The owner later approved read-only group access to the single controller log:
`ROOT/jobs/car-backfill-20261012/controller.log` now keeps owner `root`, with
group `ubuntu` and mode `0640`. The heartbeat can read it without sudo. Only
bounded, parsed numeric statistics enter telemetry. Historical upload rows
proved the rate parser; the current compaction stage correctly has no upload
rate. Fresh `ETA -` rows retain their percentage and rate, report unavailable
ETA, and cannot reuse an older nonzero estimate. The tested parser is in the
new pinned sender release. Its first strict service send and external status
check passed with no errors. At 08:42 Paris, epoch 1051 had completed upload,
1052 was about 54% compacted, and native slot lag was zero. See
[the log permission receipt](evidence/controller-log-read-access-20261013.json),
[the updated sender check](evidence/heartbeat-archive-r2-install-20261013.json),
[command evidence](evidence/telegram-commands-20261013.json) and
[sender installation](evidence/heartbeat-archive-install-20261013.json).

## Checks and delivery

- A missing heartbeat for 90 seconds creates a failure alert at the next check.
  This can mean a machine, sender or network failure; it is not proof of power
  failure. Normal alarm checks run every 30 seconds, with the cron as backup.
- Capture must keep advancing. Fresh status timestamps alone are insufficient.
  Required services include validator, worker, bank v2 producer, publisher,
  live cloud storage and the live priority guard.
- The watcher checks compact delay, stalled uploads, old status reports,
  memory below 64 GiB, and free disk below the capture reserve plus 100 GiB.
- Each pipeline fault must persist for 60 seconds. Recovery requires 60 seconds
  of continuous all-clear. Separate issue timers prevent an intermittent fault
  from hiding a persistent capture failure.
- The public SSE check starts from the current head and requires two advancing
  block slots. It reads at most 64 KiB and has a ten-second timeout. It requires
  no private stream key. Private confirmed API readiness is not directly tested
  by this external public-feed probe.
- Boot ID and monotonic time order heartbeats. Duplicate retries do not extend
  the deadline. Retired boots cannot replay, and a clock fault does not create
  false machine silence.
- Alerts enter a durable SQLite outbox before delivery. Failed delivery retries
  the same event ID. An active earlier delivery blocks later alerts. A lost
  reply can still cause a duplicate unless the receiver honors the event ID.

The watcher has two new, monitor-specific authentication tokens. The selected
Telegram path also has the token for the newly created alert bot and a separate
new webhook secret. It has no SSH,
bucket, Cloudflare account or private stream credentials. Automatic approval review
rejected the proposed private stream-key copy; the public-feed implementation
removed that requirement. The new monitor-only tokens were approved and installed.
The host keeps only its heartbeat token in `ROOT/config/foundation-watch.env`
(mode 0600). The operator token is in the local private
`~/.config/blockzilla/foundation-watch-20261013.json`; it is not in this repository.
Redirects are not followed, and credentials are not written to reports.

## Code and operations review

The following defects were repaired:

1. The failed old source's publisher repeatedly read its last 14 events, then
   stopped before committing the partial batch. A finite drain committed them.
   Its checkpoint and remote head now both equal **623694**, with no pending
   intent. The completed drain and completed recovery one-shot are disabled.
2. The worker's 32 GiB limit existed only in runtime systemd configuration.
   A permanent override now keeps `MemoryMax=32G` and `CPUQuota=800%` across reboot.
3. The priority guard now requires capture sequence and root progress within
   60 seconds. Invalid samples pause background work without stopping the guard.
4. An interrupted registry count write could be reused without a stage receipt.
   An uncommitted, job-owned REGISTRIES stage now rebuilds with `--force`.
   Committed stages are still reused. The running import was not interrupted;
   the updated controller is available to its next restart.

Epoch 1048 completed its checked query/signature upload: **112,620,833,984 bytes**.
Epoch 1049 is downloading, with 1050–1054 still in the saved plan. At the later
check, native slot lag was zero, upload lag was 180 events and free disk was
963.13 GB. Validator PID 2630745 stayed unchanged.

Remaining operations work:

- Downstream source changes after validator restart still require an explicit
  handoff. The sender detects a new or different active capture and mismatched
  service bindings. An alert does not perform that handoff.
- Restored native files can stay on disk after their original retirement row is
  complete. A bounded restored-file cache with reader protection is still needed.
- The RPC backup checks its following boundary after the bulk work. Preserve
  that boundary earlier in a future recovery. Existing missing data remains missing.
- Append logs have no explicit rotation policy. Durable evidence must be kept
  separate from any later log retention policy.

Validation: 44 Worker/model/command runtime tests, 28 sender/archive tests, and 19 focused
upload/backfill/priority tests passed. Type checks and deployment build passed.
The sender read an uncheckpointed WAL with no write permission and preserved
its bytes. Live installation proved heartbeat receipt and public block progress.
Real Telegram delivery was verified with a clearly labelled test. No real
machine outage was tested. This is a finite check, not proof of uninterrupted
native coverage or sustained archive capacity.

Evidence is in [evidence](evidence) and `ROOT/reports/ops-review-20261013`.
