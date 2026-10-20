# Foundation machine watch

Alert name: **Megaloadon**. Machine ID: `foundation`.
The operating-system hostname is currently `blockzilla`.

## Storage transfer alerts, 20 October 2026

Event `bbdb2969-0b9e-4ae6-99a3-cd8d9087a47f`, created at 05:52:24 UTC
(07:52:24 Europe/Paris), reported `cloud_storage_not_running`,
`telemetry:cold_storage_failed` and `telemetry:storage_jobs_degraded`. These
were real stopped-job reports. At the first read, the latest saved failure was the existing
300-second `BundleTransferTimeout` at 05:50:44 UTC. The supervisor restarted
the job and durable archive and retirement progress resumed. The retry budget
was not exhausted. The saved child result does not identify the PUT or GET
phase that used the time, so the underlying cloud fault remains unknown.
Read [the findings](evidence/incident-20261020-storage/FINDINGS.json)
and [the history](evidence/incident-20261020-storage/ALERT-HISTORY-SUMMARY.json).

Two more process exits occurred before activation. The 06:00:42 UTC exit
has no retained exception type. The latest saved timeout was at 06:07:03 UTC;
the process exited at 06:07:14 and restarted at 06:08:32. These events use the
old helper. Read [the prepatch recurrence record](evidence/incident-20261020-storage/PREPATCH-RECURRENCES.json).

The active release is
`ROOT/releases/storage-recovery-20261020-f757f2be29f0-3a5fe9b9`.
Live storage uses PID 4054548, and
its supervisor uses PID 4054568. The source and all eight protected process
IDs stayed unchanged. Read [the installation](evidence/incident-20261020-storage/circuit/INSTALLATION.json).

The repair adds a durable source-wide recovery circuit inside live
storage. Three shared retry reservations wait 30, 60 and 120 seconds. The open
circuit then allows at most one recovery probe per 900 seconds. Reservations
are saved before use and survive process restarts. Fresh submissions pause
during these waits; pending bank and cap reservations remain. A successful
retry permits normal work, but budget reset needs 60 observed fault-free
seconds and later durable progress. Permanent authentication, source, local,
size, hash and proof faults persist `BLOCKED`; restart cannot clear them.

Recovery status reports the circuit state and wait time. It does not advance
archive progress. The original outage time remains available after restart,
so the existing 900-second storage progress-stall check remains effective.
The 300-second child deadline, full readback and retirement proof remain.
Tar packing stays outside the child deadline. The guardian, sender, Worker
thresholds and alert delivery rules do not change. The finite helpers and
absolute caps of 785546 and 308768 remain unchanged.

All 64 storage and supervisor tests passed on Foundation, including extended
outages, restart and probe timing, permanent faults, cap reservations and
retirement recovery. The real SDK smoke check also passed without network
access. Read [the build](evidence/incident-20261020-storage/circuit/BUILD.json)
and [the SDK check](evidence/incident-20261020-storage/circuit/SDK-SMOKE.json).
The [independent cloud proof](evidence/incident-20261020-storage/circuit/POSTVERIFY.json)
passed for a complete new bundle: 140861440 bytes, 64 banks, 1024 files and
128 custody notes. Both finite jobs stayed COMPLETE at their exact caps.
All [nine live check groups](evidence/incident-20261020-storage/circuit/VERIFY-LIVE.json)
passed. The repair made no validator or stream service restart.


The [121-second observation](evidence/incident-20261020-storage/circuit/FLOW-120S.json)
passed with 13 samples and zero faults. Archived and retired counts advanced
from 772134 to 773094. RPC stayed healthy, and source lag stayed at or below
1748 ms. The recovery state stayed NORMAL. The
[final monitor](evidence/incident-20261020-storage/MONITOR-END.json)
had no current issues, active incidents, sample errors or pending alerts.
The [retained history](evidence/incident-20261020-storage/ALERT-HISTORY-END.json)
contained no event after activation. These short checks do not prove capacity
through a complete snapshot cycle or recovery from a future cloud outage.

Capacity alarms are separate. The sampled capacity was OK during this
diagnosis. The current policy still requires the 400 GiB capture reserve and
500 GiB recovery minimum, plus the full next-archive budget. It does not
count expected snapshot purge as free space. A durable byte bound for retained
native metadata remains designed and unimplemented; the network circuit does
not solve that limit. Read [the capacity review](evidence/incident-20261020-storage/CAPACITY-REVIEW.json)
and [its addendum](evidence/incident-20261020-storage/CAPACITY-REVIEW-ADDENDUM.json).
The known raw-capture failure remains separate from native feed availability.

## Evening publisher alerts, 18 October 2026

The [retained history](evidence/incident-20261018-recurrence/evening-20261018/ALERT-HISTORY-SUMMARY.json)
shows two later incidents, in Europe/Paris time. The 18:30:08 storage supervisor
alert recovered at 18:31:56 after its bounded automatic retry. The 20:06:58
alert had only `publisher_status_stale`; it recovered at 20:08:28. The publisher
logged an HTTP 500 commit GET retry at 20:06:36 and kept the same process.
The old batch start and completion times were overwritten, so they cannot be
reconstructed from this journal entry.

The publisher now reports status every five seconds during remote batch work,
including catalog HEAD readback. It retains the last durable checkpoint until
the remote checks and local commit succeed. A fresh status therefore does not
hide an unchanged cloud position: the existing 300-second progress stall and
8192-event backlog checks remain. This adds no total batch deadline. Completed
batches longer than 60 seconds have a small numeric journal record for later
diagnosis.

The [reviewed installation](evidence/incident-20261018-recurrence/evening-20261018/publisher-status/INSTALLATION.json)
restarted only the v2 cloud publisher, to PID 4167494. Its new process made
durable progress, and two separate full GETs checked the remote HEAD and last
commit. All 44 Rust tests, 11 Linux install fixtures and
[nine live check groups](evidence/incident-20261018-recurrence/evening-20261018/LIVE-FINAL.json)
passed. The seven protected processes, including the validator, kept their PIDs.
The source, storage registry and finite cleanup caps stayed unchanged. Read
[the diagnosis](evidence/incident-20261018-recurrence/evening-20261018/DIAGNOSIS-BEFORE.json)
and [the code review](evidence/incident-20261018-recurrence/evening-20261018/publisher-status/REVIEW.json).

The [120-second observation](evidence/incident-20261018-recurrence/evening-20261018/publisher-status/FLOW-120S.json)
passed with 121 samples and 1106 new durable events. Status age stayed at or
below 4995 ms, with 120 samples during an active upload. The
[final monitor](evidence/incident-20261018-recurrence/evening-20261018/MONITOR-FINAL.json)
had no current issues, active incidents, sample errors or pending alerts.
The retained alert history did not gain a new event during this repair.

## Later capacity alerts, 18 October 2026

The 18 October installation used
`ROOT/releases/transfer-deadline-20261018-498831996a2dc34c`. Each transfer child
has a 300-second total deadline for the local digest, conditional PUT, SDK
retries and full GET verification. Tar packing stays outside this deadline.
On timeout, the parent kills the child and waits for it to exit. Archive commit
and local retirement still require exact full readback proof. All 51 storage and
supervisor tests passed on Foundation. Existing local and supervisor retry
limits and backoff remain unchanged.

Only live storage and its supervisor restarted, to PIDs 2353823 and 2353836.
The source, all eight protected process IDs and the finite bank caps stayed
unchanged. Read [the 18 October deadline installation](evidence/incident-20261018-recurrence/transfer-deadline/INSTALLATION.json).

The [cloud proof after installation](evidence/incident-20261018-recurrence/transfer-deadline/POSTVERIFY.json)
and [final live check](evidence/incident-20261018-recurrence/transfer-deadline/LIVE-FINAL.json)
**PASSED**. Independent full GET checked a new 64-bank bundle: 145121280 bytes,
1024 files and 128 custody notes. Live storage advanced from 60649 to 60969
archived and retired banks. All nine live check groups passed, with 743 ms
source lag.

The [final flow check](evidence/incident-20261018-recurrence/transfer-deadline/FLOW-120S.json)
**PASSED** over 121.211 seconds with 13 samples and zero faults. Its live count
advanced from 60777 to 60969 archived and retired banks. Final free space was
789595852800 bytes. The source, registry, eight protected process IDs and two
new storage process IDs stayed unchanged during these checks.

The [final external monitor](evidence/incident-20261018-recurrence/transfer-deadline/MONITOR-FINAL.json)
was armed and had no current issues, incidents, errors or pending alerts. Read
[the final consolidated receipt](evidence/incident-20261018-recurrence/RECEIPT-FINAL.json).

The active transport release is
`bank-connection-idle-20261018-2f3cbe5e-73506f06cdc6`, binary SHA-256
`73506f06cdc646437a1a635ec9186ce6b3dd01146ae5474d8828a1f9f66c1d7d`.
The v2 origin is PID 2134416, the v1 origin is PID 2135639, and the relay is
PID 2136117. The five protected native process IDs stayed unchanged, including
validator PID 2142808. That transport activation kept the source, storage
helpers and bank limits unchanged.
Read [the installation](evidence/incident-20261018-recurrence/connection-idle/INSTALLATION.json),
[the build](evidence/incident-20261018-recurrence/connection-idle/BUILD.json)
and [the release manifest](evidence/incident-20261018-recurrence/connection-idle/MANIFEST.json).

The later [stress check](evidence/incident-20261018-recurrence/HEAD-STRESS-FLOW.json)
found two isolated stale heads in 601 samples, 302 seconds apart. The common
server had a fixed 300-second total connection timer. Direct origin status was
live 184 ms before a relay fault; worker and producer status ages stayed below
813 ms and 471 ms. Samples around that fault stayed below 256 ms and 75 ms.
Read [the diagnosis](evidence/incident-20261018-recurrence/DIAGNOSIS.json)
and [the direct-origin check](evidence/incident-20261018-recurrence/HEAD-STRESS-ORIGIN-STATUS.json).
The new 300-second I/O idle timer extends only after a positive byte read or
write. TLS and header limits, caps, authentication and head health limits remain.
All 36 Foundation transport library tests passed. The old total timer failed
the SSE regression in a separate source copy; the new idle timer passed. Read
[the comparison](evidence/incident-20261018-recurrence/connection-idle/RED-COMPARISON.json).

The external monitor had cleared before these transport restarts. The
[630-second same-connection check](evidence/incident-20261018-recurrence/connection-idle/SAME-CONNECTION-630S.json)
**PASSED** over 630.508 seconds: 613 private head samples, zero faults and at
most 2084 ms source lag. Each of the three SSE connections stayed open for more
than 630 seconds, with one open call, no reconnect, and events after the 300-
and 600-second boundaries.

The first brief `bank_v2_not_live` faults also had a timing defect: the origin sent
health every five seconds, equal to the relay's five-second health lifetime.
Fresh confirmed blocks could therefore appear stale during message delivery.
The first repair changed the v2 origin to send health every second. The health
lifetime and confirmed source-age checks remain unchanged. All 33 transport
library tests passed on Foundation. At that repair, only the v2 origin restarted;
the validator, relay and source stayed unchanged. The sender and external
watcher still use their existing checks.

The 120.043-second post-change observation passed with 112 samples and no
freshness faults. Confirmed slots advanced and source lag stayed at or below
1891 ms. The external monitor retained only the valid early capacity warning;
it had no pending alerts. Read [the recurrence receipt](evidence/incident-20261018-recurrence/RECEIPT.json).

The full snapshot completed at 08:25 UTC. Agave removed the old archive, and
the urgent overlap warning cleared. Free space returned to about 644 GB. At
the later cleanup preflight it was about 635 GB, below the 674.31 GB recovery
threshold, so the early capacity warning was valid then. No reserve or alert
threshold was lowered.

A separate live-storage read timeout at 08:27 UTC triggered the new supervisor.
Its bounded retry resumed verified retirement at 08:29 UTC. The first repair
used release `readback-retry-20261018-e5db5eeaf39a4f88`. It retries only
selected temporary faults during complete cloud readback. Each attempt starts
from byte zero; every size/hash check remains. All 29 storage tests passed on
Foundation. The sender and external watcher code did not change.

At 09:17 UTC, live storage failed with `ResponseStreamingError` at 28871
archived/retired banks. The previous strict supervisor blocked the unknown
wrapper. The 09:33 UTC installation used release
`protocol-retry-20261018-ccc4e4475ec568d1`. The live helper SHA-256 at that installation was
`837b89f1c2dcfac6fb34415f783d88274ae0fd8d9200c53d5a70333cfbc57b63`; the
supervisor SHA-256 is
`9c586efd91790488dedaa282e88f16625c3a187791c6316c62e269bf2a12b376`.
Only the typed protocol wrapper receives up to three complete GET attempts.
The fixed `ProtocolError` report permits the supervisor's separate three-retry
limit. Generic wrappers, length faults and permanent proof failures stay
blocked. All 40 storage/supervisor tests passed locally and on Foundation;
[the real SDK wrapper check](evidence/incident-20261018-recurrence/approved-extra/SDK-SMOKE.json)
also passed. Read [the installation receipt](evidence/incident-20261018-recurrence/approved-extra/PROTOCOL-INSTALLATION.json).
Live storage PID was 1966513 at that installation. Validator PID 2142808
and the protected capture/feed process IDs stayed unchanged.

The owner [approved 51606 additional banks](evidence/incident-20261018-recurrence/approved-extra/APPROVAL.json)
for source `000000000000000018ddd9a6bb6fc50e-282459`. The finite job is now
**COMPLETE** at exactly 308768 archived and retired banks. The other old job
is complete at 785546. Both units are inactive. Each returned success and has
zero pending bundles. Their absolute caps and exact approved finite-job helpers stayed
unchanged.

The [independent cloud proof of the final bundle](evidence/incident-20261018-recurrence/approved-extra/CLOUD-PROOF-FINAL.json)
reopened the durable index read-only and checked all 200171520 bytes of the last
64-bank bundle, including 1024 member files and 128 custody notes. The
[cleanup flow observation](evidence/incident-20261018-recurrence/approved-extra/FLOW-POST-TRANSPORT.json)
completed after 3018.909 seconds with 301 samples, zero faults and at most
2530 ms source lag. The [cleanup final live check](evidence/incident-20261018-recurrence/approved-extra/LIVE-FINAL.json)
also passed.

The [completed status](evidence/incident-20261018-recurrence/approved-extra/FINAL-STATUS.json)
measured 788295614464 free bytes and a net increase of 151371489280 bytes from
its baseline. This net change is not attributable only to the cleanup; other
disk activity continued. Read [the deployment record](../direct-agave/README.md).

This cleanup supplies more time. It does not give total local disk use a fixed
bound. The separate [metadata checklist](evidence/incident-20261018-recurrence/approved-extra/METADATA-NEXT-STEPS.json)
is scoped but not implemented; no metadata migration has started.

## Continuous disk protection, 18 October 2026

The owner approved the prepared storage release. The old job completed the same
29440-bank remaining allowance at its absolute total cap of 785546. Its boot
configuration keeps that cap. The new storage supervisor checks only that job and the current live
storage job. It validates source, command, helper hash, cloud namespace and cap
before a retry. Only known network faults and selected HTTP errors can retry.
Permanent proof failures remain blocked. Retry state survives supervisor restarts.

The capacity collector includes space for the next snapshot. It warns below
500 GiB plus the observed/configured archive budget, currently 628 GiB free.
The priority guard pauses CAR disk-consuming work at this limit while verified
native storage jobs can continue when live ingestion is healthy. One early
capacity warning can remain while free space is restored; it does not mean that
the live feeds are down. Current private head and public stream checks remain
separate. Read [the deployment record](../direct-agave/README.md).

The active sender is
`ROOT/releases/storage-watch-20261018-beafd5034d4b6346/heartbeat.py`.
Its local `ROOT/reports/foundation-watch/STATUS.json` now keeps sanitized raw,
capacity and storage-job details after successful and failed sends. The external
watcher still receives the same schema and fixed error codes. Its code did not
change. Raw recording remains FAILED and optional; this state is explicit in
the local report, with no claim that an active recorder service records packets.
All 53 sender/archive tests passed locally and on Foundation.

The current recovery helper is selected by
`ROOT/config/native-recovery-helper.json`. The installed capacity collector
makes recovery require a fresh exact-source report and current free space for
500 GiB plus the next archive. A read-only gate check blocked recovery at the
known low margin. It did not stop a service or reload the plugin. All 40 focused
recovery tests passed on Foundation.

Metadata and unresolved evidence still need a separate cloud catalog and hot
window contract. The [metadata plan](evidence/incident-20261018/METADATA-PLAN.json)
is designed, not deployed. Current payload retirement is not a fixed bound on
all local disk use.

The final 702.522-second flow check passed with 71 samples and no faults. The
validator and protected feed process IDs stayed unchanged. The final private
payload hash and RPC bank check passed, and public SSE advanced. The external
monitor had no current faults, active incident faults or pending alerts. Free
space was 679283412992 bytes. Read [the final protection receipt](evidence/incident-20261018/PROTECTION-RECEIPT.json).

## Disk reserve alerts, 18 October 2026

These alerts followed a real native capture stop at the 400 GiB disk reserve.
RPC stayed healthy on Agave 4.3.2. Plugin reload restored capture and both live
feeds with source `000000000000000018df884b2474cf8e-20b258`; validator PID
2142808 stayed unchanged. The old publisher completed its remaining 14 events.
The capture gap and old failure evidence remain.

At 05:26:15 UTC, the external monitor had no current or active incident faults,
no host errors and no pending alerts. Separate live checks passed for RPC,
capture, compact output, the private head and payload, public SSE and cloud
publication. Read [the deployment record](../direct-agave/README.md) and
[the recovery evidence](evidence/incident-20261018/RECOVERY-EVIDENCE.json).

The optional raw shred capture remains failed after its earlier 500 GiB reserve
stop. Its service remains active while its listener waits. At this first recovery
check, the heartbeat did not monitor raw capture. The later sender above reports
it separately as optional and FAILED. A clear native-feed incident does not prove
that raw packets are recorded. A validator restart is required to reset that hook.

## Agave 4.3.2 restart and sender repair, 16 October 2026

The owner requested the validator upgrade to Agave 4.3.2. The planned capture
close and restart raised event `27d95815-b969-4518-9ced-9d23172bde1f` at
20:07:41 UTC. The validator then loaded its snapshot and caught up. The new
source is `000000000000000018df1be3bad56d9c-20b258`, at patched revision
`87167f3906bdf5111e99ead64d8f4098b7874e33`. Validator PID 2142808 uses the same
identity and `--no-voting`.

The sender's worker path check rejected the periods in the `agave-4.3.2` release
name. The separate sender release is now
`ROOT/releases/agave-upgrade-watch-20261016-0469f7ebd1d62b3b/heartbeat.py`.
It permits periods in one flat release directory. It still checks the exact
launcher, roots, option structure and binary names. The independent source,
writer, feed identity, freshness and private head checks remain active.
All 41 heartbeat and archive tests passed. A real sample and service send had
no errors. Only the sender restarted for this repair.

The complete live check passed with healthy 4.3.2 RPC, zero rooted compact lag,
a live private head and a hash-checked frame. The head's bank matched local RPC.
Public SSE and cloud publication advanced. At 20:30 UTC, the external watcher
had no current or active incident issues, no host errors and no pending alerts.
The restart alert had cleared. See [the deployment record](../direct-agave/README.md)
and [the final receipt](../agave-upgrade/evidence/20261016/RECEIPT.json) for the
live checks and the recorded native capture gap.

## Live flow and private v2 checks, 16 October 2026

The sender now runs from
`ROOT/releases/bank-v2-health-20261016-0c5a62ac594f8dd8/heartbeat.py`.
It reads the authenticated loopback v2 head with the existing key on Foundation.
It checks the exact active source, live state and source age, with a two-second
request timeout and a bounded response. A non-live, stale or unavailable head
produces a fixed fault code. No private stream key was copied to the external
watcher. The public SSE check remains a separate check.

The repaired cold storage helper uses complete catalog sweeps, so banks blocked
by age, replay or writer acknowledgement can qualify on a later sweep. It saves
the archive commit time in the same database transaction as the archive index.
The sender reports `cold_storage_no_archive_progress` when an eligible transfer
has no durable commit for 900 seconds. Legacy reports without these new fields
remain readable. The external watcher uses its existing host fault handling;
its code did not change.

The 903.6-second flow check passed with 61 samples and no faults. Capture,
compaction and cloud publication advanced; storage retired 6,208 checked banks
and released 14.80 GB. At 11:25:58 UTC, the private head was live with 1,656 ms
source lag and no host faults. All 56 storage, sender and archive tests passed
locally and on Foundation. Validator and feed PIDs stayed unchanged. Read
[the deployment and flow receipt](evidence/pipeline-flow-repair-20261016.json).
These finite checks do not prove uninterrupted future service. Automatic source
handoff after a validator restart remains open.

## Recovery check, 16 October 2026

Event `a3cdcf23-2192-4f5d-881c-a362b67dfa2e` followed a native compact failure
and then a capture stop at the disk reserve. A tested worker patch resolves a
zero native parent blockhash only with exact retained native proof. The source
handoff retained validator PID 2579205 and selected session
`000000000000000018deede1d641da13-275b05`. At 06:49:57 UTC, the external watcher
had no current or active incident issues. All six required services were active,
RPC was healthy, rooted compact lag was zero, and public block slots advanced.
The sender binary was unchanged. The priority guard now controls the resumed,
dated storage jobs for both failed sources with the existing lag thresholds.

These finite checks do not prove private v2 HTTP readiness, uninterrupted native
coverage or sustained disk capacity. See the
[deployment record](../direct-agave/README.md),
[repair receipt](evidence/incident-20261016-a3cdcf23.json),
[flow checks](evidence/incident-20261016-a3cdcf23-flow.json) and
[storage checks](evidence/incident-20261016-a3cdcf23-storage.json).

## Installed on 13 October 2026

An external Cloudflare Worker runs in the existing Blockzilla account:
`https://blockzilla-foundation-watch.cheron-augustin.workers.dev`.
Its source is [the Worker project](../../../edgezilla/foundation-watch).
Private status is `GET /status`, with the separate operator Bearer token.
There is no public status page with machine telemetry.
The same token permits private `GET /history`: up to 100 retained alerts in
newest insertion order, with only event ID, kind, issue codes, creation time,
delivery status and attempt count. It does not run a check or send a message.
Read the [route contract](../../../edgezilla/foundation-watch/README.md#private-alert-history)
and the [18 October deployment proof](evidence/incident-20261018-recurrence/evening-20261018/HISTORY-DEPLOY-VALIDATION.json).

The Foundation sender runs as `hivezilla-foundation-heartbeat.service`, started
by `hivezilla-foundation-heartbeat.timer` every 30 seconds. Its 13 October immutable
release was `ROOT/releases/storage-recovery-20261013-742b6735b4bbb606/heartbeat.py`, with
the pinned sibling `archive_status.py`, where
`ROOT=/home/ubuntu/live/hivezilla-validator-20260930`.
The service reads capture, compact, upload and storage reports, local RPC,
active writer locks and required service states. It changes only its own report.
Incomplete telemetry still sends a machine heartbeat with fixed error codes.
The strict service's first heartbeat was accepted with no collection errors.

The installed 13 October outage patch adds an explicit active-source pin at
`ROOT/config/native-active-source.json`. The patched sender and priority guard
prefer its four source identity fields. Only an absent pin permits the original
dated recovery-record fallback. An invalid pin does not select an older source.
The patched sender also preserves a valid compact `FAILED` state when its
terminal report has no counters. It sends fixed error codes, with no raw error
text. The source handoff selected capture
`000000000000000018de229d3fc8a9d9-275b05`, with validator PID 2579205 unchanged
during the handoff. At 16:42:43 UTC, RPC was healthy and all six required services
were active. Public blocks advanced, but `compaction_behind` remained active.
The backlog fell from 3,609 to 3,167 slots over five minutes. This is progress,
not an all-clear. See the
[storage and recovery patch](../direct-agave/README.md).

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
- The 16 October cleanup added bounded system log retention. Read
  [the cleanup receipt](evidence/storage-cleanup-20261016.json) for its limits.

Validation: 44 Worker/model/command runtime tests, 28 sender/archive tests, and 19 focused
upload/backfill/priority tests passed. Type checks and deployment build passed.
The sender read an uncheckpointed WAL with no write permission and preserved
its bytes. Live installation proved heartbeat receipt and public block progress.
Real Telegram delivery was verified with a clearly labelled test. No real
machine outage was tested. This is a finite check, not proof of uninterrupted
native coverage or sustained archive capacity.

Evidence is in [evidence](evidence) and `ROOT/reports/ops-review-20261013`.
