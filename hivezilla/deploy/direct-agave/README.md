# Direct Agave production deployment

Host: `ubuntu@213.239.141.61`.

Root: `/home/ubuntu/live/hivezilla-validator-20260930`.

The validator runs Agave 4.3.2 with the native Hivezilla Geyser plugin. It uses
the existing identity and `--no-voting`. The compact worker reads the durable
local journal. It does not use gRPC. The raw receive-hook capture is currently
FAILED while the recorder service remains active. Recovery requires a later
validator restart.

## Storage transfer recovery, 20 October 2026

At 05:52:24 UTC (07:52:24 Europe/Paris), event
`bbdb2969-0b9e-4ae6-99a3-cd8d9087a47f` reported
`cloud_storage_not_running`, `telemetry:cold_storage_failed` and
`telemetry:storage_jobs_degraded`. At the first read, the latest saved failure was
`BundleTransferTimeout` at 05:50:44 UTC. The 300-second child deadline worked,
but the failed transfer ended the live storage process. The supervisor
restarted it, and verified archive and retirement progress resumed. Its retry
budget was not exhausted. Earlier process exits were also recorded at 05:31
and 05:40 UTC; their exact failure types were not independently retained.
Read [the findings](../foundation-watch/evidence/incident-20261020-storage/FINDINGS.json)
and [the retained history](../foundation-watch/evidence/incident-20261020-storage/ALERT-HISTORY-SUMMARY.json).

Two more process exits occurred before activation. The 06:00:42 UTC exit
has no retained exception type. The latest saved timeout was at 06:07:03 UTC;
the process exited at 06:07:14 and restarted at 06:08:32. These events use the
old helper. Read [the prepatch recurrence record](../foundation-watch/evidence/incident-20261020-storage/PREPATCH-RECURRENCES.json).

The child did not retain PUT or GET phase timing. These records do not show
which operation used the deadline or establish the underlying cloud fault.
The sampled capacity was OK, with positive snapshot and recovery headroom.
This failure was not a disk-reserve failure.

The active release is
`ROOT/releases/storage-recovery-20261020-f757f2be29f0-3a5fe9b9`.
Live storage uses PID 4054548, and
its supervisor uses PID 4054568. The source and all eight protected process
IDs stayed unchanged. Read [the installation](../foundation-watch/evidence/incident-20261020-storage/circuit/INSTALLATION.json).

The repair keeps known temporary transfer faults inside the live
process. One durable recovery circuit covers the source, cloud namespace and
absolute bank cap across both transfer slots and process restarts. It reserves
three retries before use, with 30-, 60- and 120-second waits. After these
retries, the circuit opens and permits at most one recovery probe per 900
seconds. Fresh submissions pause during each wait. Pending banks keep their
cap reservation. An existing successful transfer can still commit after full
proof. A successful retry permits normal work, but the burst budget resets
only after 60 observed fault-free seconds and new durable progress after the
healthy baseline.

Permanent authentication, source, local-file, size, hash and proof faults
persist `BLOCKED` and stop the job. Restart does not clear this state. The
SQLite state and initialization marker reject missing, corrupt or foreign
state. The original outage time survives a restart, so waiting cannot reset
the existing 900-second progress-stall check. Status updates do not count as
durable progress. The guardian and monitor thresholds remain unchanged.

Each child retains the existing 300-second total deadline for the digest,
conditional PUT, SDK retries, complete GET verification and child exit. Tar
packing remains outside this deadline. Index commit and local retirement
still require exact full readback proof. No source, namespace, finite helper
or deletion allowance changes are part of this repair. The completed finite
jobs remain capped at 785546 and 308768 banks.

All 64 storage and supervisor tests passed on Foundation. They cover outage
and restart limits, reservation before launch, probe timing, permanent fault
retention, parallel cap reservations, lost PUT replies, full readback and
partial retirement recovery. The real SDK smoke check also passed without
network access. Read [the build](../foundation-watch/evidence/incident-20261020-storage/circuit/BUILD.json)
and [the SDK check](../foundation-watch/evidence/incident-20261020-storage/circuit/SDK-SMOKE.json).

The [independent cloud proof](../foundation-watch/evidence/incident-20261020-storage/circuit/POSTVERIFY.json)
passed for a complete new bundle: 140861440 bytes, 64 banks, 1024 files and
128 custody notes. Both finite jobs stayed COMPLETE at their exact caps.
All [nine live check groups](../foundation-watch/evidence/incident-20261020-storage/circuit/VERIFY-LIVE.json)
passed. The repair made no validator or stream service restart.

The [121-second observation](../foundation-watch/evidence/incident-20261020-storage/circuit/FLOW-120S.json)
passed with 13 samples and zero faults. Archived and retired counts advanced
from 772134 to 773094. RPC stayed healthy, and source lag stayed at or below
1748 ms. The recovery state stayed NORMAL. The
[final monitor](../foundation-watch/evidence/incident-20261020-storage/MONITOR-END.json)
had no current issues, active incidents, sample errors or pending alerts.
The [retained history](../foundation-watch/evidence/incident-20261020-storage/ALERT-HISTORY-END.json)
contained no event after activation. These short checks do not prove capacity
through a complete snapshot cycle or recovery from a future cloud outage.

Disk capacity remains a separate limit. The conservative capacity checks
keep the 400 GiB capture reserve and 500 GiB recovery minimum, plus the full
next-archive budget. Expected snapshot purge space is not current free space.
A durable byte bound for retained native metadata remains designed and
unimplemented. This repair adds no metadata deletion authority and does not
establish sustained capacity. Read [the capacity review](../foundation-watch/evidence/incident-20261020-storage/CAPACITY-REVIEW.json)
and [its addendum](../foundation-watch/evidence/incident-20261020-storage/CAPACITY-REVIEW-ADDENDUM.json).
The raw capture remains FAILED while its recorder service is active; its
recovery still requires a later validator restart.

## Evening publisher alerts, 18 October 2026

The retained alert history shows two later incidents, in Europe/Paris time.
At 18:30:08, the storage supervisor reported a degraded job after a transfer
deadline. Its bounded automatic retry recovered; the recovery alert was at
18:31:56. At 20:06:58, the only issue was `publisher_status_stale`; recovery
was at 20:08:28. The publisher logged an HTTP 500 commit GET retry at 20:06:36.
It kept the same process. The exact old batch timing was not retained.
Read [the diagnosis](../foundation-watch/evidence/incident-20261018-recurrence/evening-20261018/DIAGNOSIS-BEFORE.json).

The active publisher now uses
`ROOT/releases/publisher-status-20261018-92f2376d-7fb9e39d7506`, binary SHA-256
`7fb9e39d75062546cb407586596ca2f5303caaf8dd25725e768025f8266b816a`.
Its status updates every five seconds during remote work, including catalog
HEAD readback. It reports `phase=upload_commit` and `in_flight=true` with the
last durable checkpoint. Status updates do not advance that checkpoint.
The existing 300-second cloud progress and 8192-event backlog checks remain.
This repair adds no total batch deadline. Slow completed batches now have a
small numeric journal record.

All 44 Rust tests and 11 Linux install fixtures passed. Only the v2 cloud
publisher restarted, from PID 917318 to 4167494. Validator PID 2142808, the
other six protected processes, source, registry and finite cleanup caps stayed
unchanged. The new process advanced from its own checkpoint baseline, and
separate full GETs checked the remote HEAD and last commit. Read
[the installation](../foundation-watch/evidence/incident-20261018-recurrence/evening-20261018/publisher-status/INSTALLATION.json),
[the build](../foundation-watch/evidence/incident-20261018-recurrence/evening-20261018/publisher-status/BUILD.json)
and [the final live check](../foundation-watch/evidence/incident-20261018-recurrence/evening-20261018/LIVE-FINAL.json).
All nine live check groups passed, with 873 ms confirmed source lag. These
checks do not establish recovery of the failed raw capture.

The [120-second publisher observation](../foundation-watch/evidence/incident-20261018-recurrence/evening-20261018/publisher-status/FLOW-120S.json)
passed with 121 samples, 1106 new durable events and at most 4995 ms status age.
It included 120 samples during an active upload. The
[final monitor](../foundation-watch/evidence/incident-20261018-recurrence/evening-20261018/MONITOR-FINAL.json)
had no current issues, active incidents, sample errors or pending alerts.
No new alert record appeared during this repair.

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
unchanged. Read [the 18 October deadline installation](../foundation-watch/evidence/incident-20261018-recurrence/transfer-deadline/INSTALLATION.json).

The [cloud proof after installation](../foundation-watch/evidence/incident-20261018-recurrence/transfer-deadline/POSTVERIFY.json)
and [final live check](../foundation-watch/evidence/incident-20261018-recurrence/transfer-deadline/LIVE-FINAL.json)
**PASSED**. Independent full GET checked a new 64-bank bundle: 145121280 bytes,
1024 files and 128 custody notes. Live storage advanced from 60649 to 60969
archived and retired banks. All nine live check groups passed, with 743 ms
source lag.

The [final flow check](../foundation-watch/evidence/incident-20261018-recurrence/transfer-deadline/FLOW-120S.json)
**PASSED** over 121.211 seconds with 13 samples and zero faults. Its live count
advanced from 60777 to 60969 archived and retired banks. Final free space was
789595852800 bytes. The source, registry, eight protected process IDs and two
new storage process IDs stayed unchanged during these checks.

The [final external monitor](../foundation-watch/evidence/incident-20261018-recurrence/transfer-deadline/MONITOR-FINAL.json)
was armed and had no current issues, incidents, errors or pending alerts. Read
[the final consolidated receipt](../foundation-watch/evidence/incident-20261018-recurrence/RECEIPT-FINAL.json).

The active transport release is now
`ROOT/releases/bank-connection-idle-20261018-2f3cbe5e-73506f06cdc6`; its binary
SHA-256 is `73506f06cdc646437a1a635ec9186ce6b3dd01146ae5474d8828a1f9f66c1d7d`.
The v2 origin is PID 2134416, the v1 origin is PID 2135639, and the relay is
PID 2136117. The five protected native process IDs stayed unchanged, including
validator PID 2142808. That transport activation kept the source, storage
helpers and bank limits unchanged.
Read [the active installation](../foundation-watch/evidence/incident-20261018-recurrence/connection-idle/INSTALLATION.json),
[the build receipt](../foundation-watch/evidence/incident-20261018-recurrence/connection-idle/BUILD.json)
and [the release manifest](../foundation-watch/evidence/incident-20261018-recurrence/connection-idle/MANIFEST.json).

A later 600.024-second check found two isolated stale heads in 601 samples,
302 seconds apart. The common HTTP server closed active TLS and plain SSE
connections at a total age of 300 seconds. A direct origin status was live
184 ms before a relay fault. Worker and producer status ages stayed below
813 ms and 471 ms; the samples around that fault stayed below 256 ms and 75 ms.
Read [the diagnosis](../foundation-watch/evidence/incident-20261018-recurrence/DIAGNOSIS.json),
[the stress check](../foundation-watch/evidence/incident-20261018-recurrence/HEAD-STRESS-FLOW.json)
and [the direct-origin check](../foundation-watch/evidence/incident-20261018-recurrence/HEAD-STRESS-ORIGIN-STATUS.json).

The server now has a 300-second I/O idle limit. Only a positive byte read or
write extends the deadline. The five-second TLS and header limits, connection
and request caps, authentication and five-second head health lifetime remain.
All 36 transport library tests passed on Foundation. The active SSE regression
failed with the old total timer in a separate source copy and passed with the
new idle timer. The prior one-second status interval remains. Read
[the timer comparison](../foundation-watch/evidence/incident-20261018-recurrence/connection-idle/RED-COMPARISON.json).
The [630-second same-connection check](../foundation-watch/evidence/incident-20261018-recurrence/connection-idle/SAME-CONNECTION-630S.json)
**PASSED** over 630.508 seconds: 613 private head samples, zero faults and at
most 2084 ms source lag. Each of the three SSE connections stayed open for more
than 630 seconds, with one open call, no reconnect, and events after the 300-
and 600-second boundaries. The external monitor had cleared before the transport
restarts.

The first private v2 head repair addressed a separate timing defect. Its health
lifetime was five seconds, and the origin sent status every five seconds. Delivery delay could
expire health while confirmed blocks remained fresh. A 60-second observation
found four stale heads in 57 samples; confirmed source lag stayed below 2106 ms.
The regression failed with the old interval and passed with the new interval.

That repair changed the v2 origin to send status every second. Its five-second
health lifetime, confirmed observation age, source checks and all head checks remain unchanged.
Repeated health alone still cannot make old confirmed data live. All 33
transport library tests passed locally and on Foundation. The isolated host
build used a source graph equivalent to the deployed production code, with the
same dependency lock and no unrelated working changes.

At that first repair, only `hivezilla-bank-v2-origin.service` restarted, from
PID 917181 to 1834760.
Its release at that check was
`ROOT/releases/bank-head-health-20261018-c319ba5d-e769871d5197`; the binary SHA-256
is `e769871d519750a187814a82e1a1f3e48cfc9bde0948410674a70dca7c97d1b7`.
The existing spool and source remain. After the old health lifetime expired,
the confirmed sequence advanced and the private frame hash and RPC bank matched.
Validator PID 2142808, relay PID 917317 and the other protected capture/feed
process IDs stayed unchanged. No bank limit changed. Read
[the timing diagnosis](../foundation-watch/evidence/incident-20261018-recurrence/head-health/TIMING-DIAGNOSIS.json)
and [the build receipt](../foundation-watch/evidence/incident-20261018-recurrence/SSE-BUILD-READY.json).

The post-change observation passed: 112 private-head samples over 120.043
seconds, no freshness faults, confirmed slot advancement and at most 1891 ms
source lag. The final full live check passed for RPC, capture, compaction,
private payload hash and RPC bank, public SSE and cloud publication. Live
storage reached 25159 archived/retired banks. Free space was 640141398016 bytes.
The external monitor retained only the valid early capacity warning. Read
[the recurrence receipt](../foundation-watch/evidence/incident-20261018-recurrence/RECEIPT.json).

The next full snapshot started at 08:15:34 UTC and completed at 08:25:11 UTC.
Its archive used about 124.19 GB. Agave removed the previous full and incremental
archives after the new archive succeeded. Native capture and both feeds stayed
healthy. Free space recovered from about 525 GB during overlap to about 644 GB.
The early warning remains valid below the 674309865472-byte recovery threshold.
Do not lower the reserve or describe this warning as a feed outage.

Live cloud storage then failed with a streamed `ReadTimeoutError` at 08:27:38
UTC. It had archived and retired 15815 banks. The supervisor made its first
bounded retry at 08:29:45 UTC; verified storage progress resumed without a
validator or feed restart.

The first readback retry repair used
`ROOT/releases/readback-retry-20261018-e5db5eeaf39a4f88/cold_storage.py`, SHA-256
`5a320696a79da06a8f8e5ddd550e9d53da808b37119407c8708a44bd4c64c923`.
It allows three complete GET attempts only for selected typed network faults.
Each attempt starts at byte zero with a new counter and hash, and closes its
stream before retry. It reads through the wrapped body so content-length
validation remains active. Size, hash, credential and other permanent failures
do not receive these local retries. Full verified readback still precedes index
commit and local retirement. All 29 storage tests passed on Foundation; all 38
focused storage/supervisor tests passed locally. The live source, all eight
protected process IDs and existing bank limits stayed unchanged.

At 09:17 UTC, live storage failed with `ResponseStreamingError` after 28871
archived/retired banks. The previous strict supervisor blocked this unknown
wrapper. The 09:33 UTC installation used live-storage and supervisor release
`ROOT/releases/protocol-retry-20261018-ccc4e4475ec568d1`. Its `cold_storage.py`
SHA-256 is `837b89f1c2dcfac6fb34415f783d88274ae0fd8d9200c53d5a70333cfbc57b63`;
its `storage_jobs.py` SHA-256 is
`9c586efd91790488dedaa282e88f16625c3a187791c6316c62e269bf2a12b376`.

The helper recognises the typed protocol-error wrapper and permits up to three
complete GET attempts, each with a new counter and hash. Generic wrappers,
content-length faults and permanent size/hash failures remain blocked. A fixed
`ProtocolError` classification lets the supervisor apply its separate limit
of three retries. All 40 storage/supervisor tests passed locally and on
Foundation. A smoke check used the real SDK wrapper and confirmed that generic
and length faults stay blocked. Read
[the protocol installation receipt](../foundation-watch/evidence/incident-20261018-recurrence/approved-extra/PROTOCOL-INSTALLATION.json)
and [the SDK check](../foundation-watch/evidence/incident-20261018-recurrence/approved-extra/SDK-SMOKE.json).
Live storage PID was 1966513 at that installation. Validator PID 2142808
and the protected capture/feed process IDs stayed unchanged.

The owner [approved 51606 additional banks](../foundation-watch/evidence/incident-20261018-recurrence/approved-extra/APPROVAL.json)
from source `000000000000000018ddd9a6bb6fc50e-282459`. This finite job is now
**COMPLETE** at exactly 308768 archived and retired banks. The separate old job
is complete at 785546. Both units are inactive. Each returned success and has
zero pending bundles; both absolute caps remain unchanged. The extra job used its exact
approved `7f0d6719` helper, source and command throughout.

The [independent cloud proof of the final bundle](../foundation-watch/evidence/incident-20261018-recurrence/approved-extra/CLOUD-PROOF-FINAL.json)
reopened the durable index read-only and checked all 200171520 bytes of the last
64-bank bundle, including 1024 member files and 128 custody notes. The
[cleanup flow observation](../foundation-watch/evidence/incident-20261018-recurrence/approved-extra/FLOW-POST-TRANSPORT.json)
completed after 3018.909 seconds with 301 samples, zero faults and at most
2530 ms source lag. The [cleanup final live check](../foundation-watch/evidence/incident-20261018-recurrence/approved-extra/LIVE-FINAL.json)
also passed.

The [completed status](../foundation-watch/evidence/incident-20261018-recurrence/approved-extra/FINAL-STATUS.json)
measured 788295614464 free bytes and a net increase of 151371489280 bytes from
its baseline. This net change is not attributable only to the cleanup; other
disk activity continued. Preflight free space was 635038658560 bytes. At that
preflight, the early warning was valid below the 674309865472-byte recovery
threshold.

Snapshot and ledger cleanup both work. The sampled RPC transaction/address
metadata use about 330.78 GB, but their measured size fell during the longer
comparison. The short allocation samples do not prove a sustained ledger leak
or explain all of the baseline space change. Read
[the snapshot and ledger findings](../foundation-watch/evidence/incident-20261018-recurrence/LEDGER-SNAPSHOT-FINDINGS.json).
The fixed local storage design below is still not deployed. The separate
[implementation checklist](../foundation-watch/evidence/incident-20261018-recurrence/approved-extra/METADATA-NEXT-STEPS.json)
covers the missing retirement-proof reader, catalog verification, SQL expiry,
reader leases and the full disk budget. No metadata migration has started.

## Continuous disk protection, 18 October 2026

The owner approved the prepared storage release and requested a long-term
solution. The approved old-source job started at 05:37 UTC. Its absolute total
limit remains 785546 banks, including all earlier runs and pending transfers.
The 29440-bank remaining allowance was not increased. Unused upgrade compiler
targets released 22186106880 allocated file bytes. Installed binaries, source,
build reports, shared Cargo caches and captured data remain.

The approved job completed at 785546 archived and locally retired banks. All
29440 additional banks stayed within the approved limit. The database count and
source/namespace matched, and the completed unit was inactive. The 702.522-second
flow observation passed with 71 samples and no faults. The final private frame
hash and RPC bank check passed; public SSE and cloud publication advanced.
Free space was 679283412992 bytes at the final evidence sample. The external
monitor had no current faults, active incident faults or pending alerts. These
are finite checks. Read [the final protection receipt](../foundation-watch/evidence/incident-20261018/PROTECTION-RECEIPT.json).

The active protection release is
`ROOT/releases/storage-protection-20261018-94c2aaca04758c49`. Its
`MANIFEST.json` pins all helper and test bytes. All **132 tests passed on
Foundation** before activation. The validator and seven protected capture/feed
service PIDs stayed unchanged. A controlled live-storage restart passed; the
supervisor read the current database and remained healthy. Its writable mounts
cover the two cold-storage directories, so SQLite sidecar replacement does not
leave a mount bound to an old sidecar. Database connections remain read-only
with `query_only` enabled.

`hivezilla-native-capacity.service` samples disk and immediate snapshot archive
allocation every 30 seconds. It calculates headroom above the 400 GiB native
capture reserve, with at least 128 GiB for the next archive. It warns below
500 GiB plus that archive budget, currently **628 GiB**, before the 400 GiB
reserve is at risk. Recent
growth estimates require at least three samples over 60 seconds. They are
estimates, not future capacity guarantees. The collector reports under
`ROOT/reports/native-capacity`.

The priority guard pauses only CAR disk-consuming work when the installed
capacity report is stale, invalid or below that early limit. Resumption needs
32 GiB of extra margin. Native storage jobs can keep releasing verified copies
when live capture, conversion and publication are healthy. Existing live lag
and capture-progress rules still pause background work when necessary.
The validator, capture worker, producer, publishers and current live storage
job are outside the disk-consumer control list.

`hivezilla-native-storage-jobs.service` checks only the two jobs registered in
`ROOT/config/storage-jobs.json`: the current live storage job and the approved
old-source job. It binds each job to its exact source, command, helper hash,
cloud namespace and archive limit. It retries only known network faults and
selected retryable HTTP errors. A matching current systemd invocation and
failure record are required. Three consecutive retry attempts are allowed,
with durable backoff state. Reset requires 60 healthy seconds and a durable
archive-count increase. Hash, source, readback, permission and unknown failures
remain blocked. The old job resumes at boot with the same cap. Registry edits
and source handoff remain explicit.

The sender now checks both new reports. A fresh RUNNING timestamp cannot hide
a failed registered job. Raw capture has separate diagnostics and an explicit
optional policy in `ROOT/config/raw-capture-monitor.json`; it remains failed
and requires a validator restart to resume. Its optional status does not claim
that packets are recorded. The external watcher code did not change.
The final sender-only release is
`ROOT/releases/storage-watch-20261018-beafd5034d4b6346`. Its local sender report
retains the sanitized diagnostic details that the external watcher does not
store. All 53 sender/archive tests passed on Foundation.

Use `ROOT/config/native-recovery-helper.json` to select the current recovery
helper. Its path is
`ROOT/releases/capacity-recovery-20261018-94d35122f4f4af22/recover_session.py`.
With the collector installed, both new recovery and saved resume require a
fresh exact-source capacity report and current free space for the recovery
minimum plus the next snapshot. Stale or invalid capacity data blocks before
service actions. All 40 focused recovery tests passed on Foundation. The old
500 GiB-only recovery releases below are historical.

These controls do **not** give total local disk use a fixed bound. Retired banks
still keep about 36 KiB of marker files and directories per bank in the sample.
The measured catalogs add about 14.6 KB per rooted bank. At the sampled daily
bank range, metadata alone can grow by about **10–21 GB per day**. Closed-source
replay tails, unresolved forks, incomplete capture evidence and raw shreds also
need separate retention contracts. None received new deletion authority.

The lasting storage design is a bounded local hot window backed by verified
cloud catalogs. A 64-bank, in-memory lossless packing trial reduced exact marker
and archive-row data to about 2637 bytes per bank, before indexes and manifests.
It did not move or delete server metadata. The next migration must first add a
shared exact-byte metadata resolver to all Rust and Python readers, then seal
source-bound cloud shards and bound processed/v2 SQLite history separately.
Keep local metadata until cloud readback, a durable catalog commit and reader
compatibility tests pass. The proposed shard/cache limits and acceptance tests
are in [the metadata plan](../foundation-watch/evidence/incident-20261018/METADATA-PLAN.json).
That migration is designed; it is not deployed or proved to give a stable disk
footprint. Do not describe the current payload cleanup as a complete disk bound.
The complete host budget must also cover validator accounts, ledger, snapshot
overlap, packing space and a measured cloud-outage buffer. Check both two hot
windows and a full snapshot write/purge cycle before accepting a fixed bound.
More disk space supplies time; it does not stop continued growth.

## Disk reserve recovery, 18 October 2026

The validator stayed healthy on Agave 4.3.2 with PID **2142808**. Raw shred
capture stopped at its 500 GiB reserve at 01:15:56 UTC. Native capture stopped
at its separate 400 GiB reserve at 02:29:30 UTC. The failed native source is
`000000000000000018df1be3bad56d9c-20b258`. Its failure record, durable journal,
compact files and cloud checkpoint remain. A full snapshot completed at
02:32:48 UTC. Temporary snapshot overlap is consistent with the time of the
space loss; the retained records do not prove all disk use at failure time.

After free space exceeded the 500 GiB capture recovery gate, the recovery
helper reloaded the plugin and selected source
`000000000000000018df884b2474cf8e-20b258`. It did not restart the validator.
The old cloud publisher completed its remaining 14 events through checkpoint
1000493. Reports are under `ROOT/reports/native-recovery-20261018`.

Three live checks passed. Capture, compact output and cloud publication advanced.
Rooted compact lag was zero. Each private frame passed its hash check and its
bank matched local confirmed RPC. Public SSE slots advanced. At 05:26:15 UTC,
the external monitor had no current faults or active incident faults. The
native capture gap remains; it has not been rebuilt.

Raw capture `d90d7aea9cda2212fc9d0a1c275fff88` remains **FAILED**. Its recorder
service is active, but the validator's raw hook stops permanently after a
transport failure. Plugin reload cannot reset that hook. Raw recording needs
a later validator restart with sufficient storage. The live native feeds do
not depend on that recorder. A running recorder service alone does not prove
that packets are recorded.

The recovery helper now saves the exact source, root, old failure evidence,
runtime hashes and planned spool paths. An interrupted same-revision switch
can resume from its saved capture without another plugin reload. Existing
spool paths require the exact saved plan. Older reports without this evidence
remain rejected. All 61 bank-stream deployment tests passed.
The tested helper is installed at
`ROOT/releases/incident-resume-20261018-d25366e623ecead4/recover_session.py`.
Its SHA-256 is
`d25366e623ecead4f7b5e38d64a86e62b0235841f499c89a5ffdce7c15b1c3f7`.

The old source storage job for `000000000000000018de229d3fc8a9d9-275b05`
stopped on 16 October. Its report still said RUNNING. The exact earlier
exception is absent from the retained logs. The new helper writes FAILED while
it holds the archive lock. It saves durable counters and a bounded stack trace
without exception values. A failed lock contender cannot change the owner's
status. An absolute `--max-archived-banks` limit includes earlier commits and
all pending transfers, so retries cannot reset the allowance.

The new helper is installed and tested at
`ROOT/releases/cold-storage-cap-20261018-2deb8d9ea7f5cd1c/cold_storage.py`.
Its SHA-256 is
`2deb8d9ea7f5cd1c52a1b6e4f8e6ed8c988f952b74ca7959570cfc122d19e9f9`.
All 22 storage tests passed locally and on Foundation. The prepared old-source
job has an absolute total limit of 785546 banks. With 756106 already stored,
at most **29440 additional banks** can qualify. The sampled eligible data
suggests about 96 GB, but this is not a measured release total. All source,
replay, two-hour retention, native acknowledgement and full cloud readback
checks remain active.

Automatic approval review first rejected the service change and local file
retirement because it required direct user approval. The owner then approved
the plan. The job started with the same limit; see the continuous protection
record above.
The [prepared plan](../foundation-watch/evidence/incident-20261018/STORAGE-PLAN.json)
is saved under `ROOT/reports/native-recovery-20261018/storage-cap-plan`.
The live and capped old storage services now use the reviewed protection release.

Local [incident evidence](../foundation-watch/evidence/incident-20261018/RECOVERY-EVIDENCE.json)
and [the second live check](../foundation-watch/evidence/incident-20261018/LIVE-2.json)
record these finite checks. Sustained storage capacity still needs observation.

## Agave 4.3.2 upgrade, 16 October 2026

The owner requested the upgrade announced by Anza. The official release is
[Agave v4.3.2](https://github.com/anza-xyz/agave/releases/tag/v4.3.2), at upstream
revision `a203300045b405f86c9c84308b2eae76af69ac3b`. The installed host keeps the
native bank and raw shred hooks. Its patched revision is
`87167f3906bdf5111e99ead64d8f4098b7874e33`. The release directory is
`ROOT/releases/agave-4.3.2-20261016-87167f3906bdf511`.

The host, plugin and native worker use Rust 1.97.1. The compact builder uses
Rust 1.98.0. All four artifacts were built and tested on Foundation. The plugin
uses the exact patched 4.3.2 interface and transaction status crates. The native
Bank load test, frozen and confirmed tuple checks, real compact writer tests,
and host build passed. The stored bank patch now includes the RPC notifier trait
method that the previous patch file omitted. All 57 deployment tests and all
41 heartbeat and archive tests passed.

The old capture closed cleanly. The worker processed its full durable source
through sequence 516962261. Its final cloud checkpoint is 462109, with no pending
upload intent and no remaining events. The old source files, reports and cloud
checkpoint remain. The new validator PID is **2142808**. The identity and genesis
hash are the same, and `--no-voting` remains active. The running command now uses
the one-full and one-incremental snapshot limits. The startup disk check passed
with 1,443,541,491,712 bytes free against a 674,309,865,472-byte requirement.

The loader checked local health, public finalized progress and the live writer
before it accepted capture `000000000000000018df1be3bad56d9c-20b258`. The source
switch changed the worker and compact builder together. It kept the existing
parent proof repair, transport programs, storage rules and service limits.
A protected unit backup read stopped the first switch attempt. The fixed helper
read that file through sudo and resumed the saved switch. It did not reload the
plugin or reset the new spools.

The old last complete processed slot is 457764446. The new first complete slot
is 457767450. The interval 457764447–457767449 contains **3,003 slot numbers**.
This is the native capture gap from the planned restart. It is not a count of
lost produced or finalized blocks. The gap has not been rebuilt. Clients that
pin the old source must reconcile before they accept the new source. The new
source file is saved with the upgrade evidence and under
`ROOT/reports/agave-upgrade-4.3.2-20261016/handoff/expected-source.json`.

The first complete live check passed at 20:26 UTC. All nine checked services
were running. RPC reported 4.3.2 and healthy state. Rooted compact lag was zero.
The private head was live with 445 ms source age. Its 483,380-byte frame matched
the head's exact frame hash. The head's bank hash, parent and height matched the
local confirmed RPC block.
The public SSE slots advanced. The new cloud checkpoint had stored 2,026 blocks
and 1,097,219,621 bytes. The raw shred recorder also accepted the new executable
and recorded packets. Packet recording does not prove complete shred coverage.

The sender's release-name check rejected the periods in `4.3.2`. Its separate
repair permits periods in one flat release directory and keeps the exact
launcher, roots and binary-name checks. Only the heartbeat sender restarted for
that repair. Its release is
`ROOT/releases/agave-upgrade-watch-20261016-0469f7ebd1d62b3b`. A real sample and
service send had no errors. The planned restart raised Megaloadon event
`27d95815-b969-4518-9ced-9d23172bde1f`. At 20:30 UTC, the external monitor had
no current issues, no active incident issues, no host errors and no pending
alerts. The restart alert had cleared.

The final live check passed 273.7 seconds after the first passed check. All
nine validator and feed service PIDs stayed the same. Capture, compact output
and cloud counters advanced. Rooted compact lag was zero, and the private head
was live with 646 ms source age. Its 619,811-byte frame matched the head's hash;
the head's bank also matched local confirmed RPC. Cloud publication had stored
3,627 blocks and 1,956,393,009 bytes. These are finite live checks.

Reports are under `ROOT/reports/agave-upgrade-4.3.2-20261016`. The local
[final receipt](../agave-upgrade/evidence/20261016/RECEIPT.json) links the source,
artifacts, live checks and monitor result. The source and build files remain in
`/home/ubuntu/benchmarks/agave-upgrade-4.3.2-20261016`. The old source's storage
command is saved in the handoff report for a separate drain. Automatic source
handoff after a later validator restart remains open.

## Installed continuous storage scan repair, 16 October 2026

The live cold storage scan could pass a bank while its age, replay boundary or
writer acknowledgement blocked retirement. New banks kept the scan busy, so it
did not return to the older banks. The repaired scan uses a fixed catalog ceiling
for each sweep, then starts a new sweep. All retention, publication, replay,
source hash, acknowledgement and full cloud readback gates remain in force.

The installed helper is
`ROOT/releases/cold-storage-flow-20261016-6369a7ed78291021/cold_storage.py`, with
SHA-256 `d25f1c82945ee58c727e8f8d948ff936d0fbd9982573d9bde015c2a9890fa9fd`.
The source override selects this release for the next service start. Only cold
storage and the heartbeat sender were restarted. The validator and all eight
protected live service PIDs stayed unchanged.

The 903.6-second flow check passed with 61 samples and no faults. It archived
and retired 6,208 banks after full cloud readback and released 14,801,940,480
bytes. Capture, compact output and cloud publication advanced throughout the
check. Rooted compact lag stayed within the 32-slot check limit. This proves
finite progress; sustained capacity and uninterrupted future coverage still
need observation.

The sender now checks the private v2 head on loopback with the existing on-host
key. It validates the source, live state and source age. At 11:25:58 UTC, the
private head was live with 1,656 ms source lag and no sender faults. The key stays
on Foundation. Eligible transfers with no durable archive commit for 900 seconds
produce `cold_storage_no_archive_progress`; a fresh process timestamp alone does
not clear that fault. The sender release is
`ROOT/releases/bank-v2-health-20261016-0c5a62ac594f8dd8`.

All 56 cold storage, sender and archive status tests passed locally and on
Foundation. Read [the repair and flow receipt](../foundation-watch/evidence/pipeline-flow-repair-20261016.json).
The existing native gaps remain. The watcher alerts on faults; it does not
perform a source handoff after a validator restart. The dated old-source finite
drain also stopped before its original bank cap and needs a bounded resume.

## Installed native parent proof patch, 16 October 2026

Megaloadon event `a3cdcf23-2192-4f5d-881c-a362b67dfa2e` reported a stopped
pipeline. The compact worker failed at 02:00:52 UTC with
`contradictory native bank queue tuple`. Capture then reached its disk reserve
and stopped at 02:06:24 UTC. The validator and RPC remained healthy.

For bank 958752 at slot 457459658, the native confirmed observation had the
correct parent blockhash. A later frozen observation had the same bank, blockhash,
parent slot, height and expiry, but a zero parent blockhash. In the pinned Agave
hook, `bank.parent()` can be absent after squash detaches the parent. An earlier
native block footer retained the exact nonzero parent blockhash. The captured
records and their source digests are saved in
[the conflict fixture](../foundation-watch/evidence/incident-20261016-a3cdcf23-native-conflict.json).

The worker now resolves only a zero parent blockhash from an exact native
observation or a bounded, digest-checked native block footer for the same bank.
It checks both stored observation types. Conflicting nonzero hashes, bank
identity, parent slot, height or expiry still stop discovery. The original
source rows remain unchanged. The discovery transaction records the original
observation and its proof in `bank_parent_resolutions`; it preserves native
confirmation, timestamps and source positions. This does not infer confirmation
or expiry from time or processed state.

The installed worker is
`ROOT/releases/native-parent-proof-20261016-7c6f488fe28b757a/hivezilla-direct-agave`.
Its SHA-256 is
`7c6f488fe28b757abfaa57e71b6fa3357ad95f8f6a250e571f4f0d82509b2818`.
It was built offline on Foundation with Rust 1.98.0 and the pinned native
revision. All 23 native unit tests and both real-builder tests passed. The build
used an isolated copy of the installed source; unrelated local chain changes
were excluded. No validator or plugin binary changed.

The checked source handoff retained validator PID 2579205 and selected capture
`000000000000000018deede1d641da13-275b05`. The old source's final 24 publisher
events reached checkpoint 1913216, with no pending intent and a remote head check
by the publisher. At 06:49:57 UTC, Megaloadon had no current or active incident
issues, all six required services were active, RPC was healthy, rooted compact
lag was zero, and public block slots advanced. The cloud queue held 166 events.
These are finite recovery checks. The private v2 HTTP payload check was not run
on 16 October; public SSE does not prove that separate contract.

The old last complete processed compact slot is 457459657. The new first slot
is 457533344. The intervening interval is 457459658–457533343, or 73,686 slot
numbers. It is not a count of lost produced or finalized blocks. The source
files remain subject to the existing verified storage policy; the native gap
has not been rebuilt.

The owner's 12 October approval enabled continuing native cloud storage and
local retirement after exact cloud byte checks. The two saved old-source jobs
were resumed with the same two-hour minimum, publication, replay and native
acknowledgement gates. Both use 64 banks per batch, a two-core quota and a
4 GiB memory limit. The 13 October source job uses two parallel bundles and has a
64-batch limit per process run and restarts on failure. The 16 October source
job started with two bundles and a 1,024-batch limit. The first storage pilot
released 10.41 MB/s, but filesystem free space fell. It was changed to four
bundles. At restart, 1,472 new banks were already indexed, so the remaining
limit was reduced to 1,001 batches to preserve the original 65,536-bank cap.
It has no automatic restart. `--follow`
can wait when no bank qualifies; these limits do not promise drain completion.
No snapshot was removed manually.

Four transfers alone did not supply enough capacity. The old v2 metadata
database lacked `event_artifact(artifact,sequence)`, which the current
`confirmed.rs` already defines. The storage gate's exact artifact lookup was
scanning event history for each bank. The missing index was added to the closed
source under its exclusive producer `WORKER.lock`. All 2,869,813 event rows,
their sequence bounds and the sampled event contents were retained. The gate
query now uses the covering index. The same missing index was added to the
small active database in a 28 ms SQLite transaction, with producer PID 2995833
unchanged. No gate query, retention rule or event data changed. See
[the schema repair receipt](../foundation-watch/evidence/incident-20261016-a3cdcf23-bank-artifact-index.json)
and [the separate storage pilots](../foundation-watch/evidence/incident-20261016-a3cdcf23-storage-tuning.json).
The 199-second pilot after the index repair released 7.18 GB at 35.99 MB/s.
Filesystem free space rose by 3.99 GB to 567.37 GB. Live services remained
active, with rooted compact lag zero. This is a finite capacity check.

The priority guard in `ROOT/releases/native-priority-20261016-e148f16ca59d95de`
now includes both dated old-source units. It uses the existing capture health,
progress and lag rules. The current live storage unit remains outside this
background freeze list. All 11 priority tests passed. The live source still
keeps at least two hours locally. The running validator still uses its previous
snapshot retention limits; the installed one-full/one-incremental policy and
628 GiB minimum startup gate apply to the next start. Sustained storage capacity
and restored-file cache retirement remain open. The finite snapshot budget
estimated the next full trigger near 08:05 UTC at native block height 435600000.
The prior full write took 28 minutes 40 seconds before archive cleanup. Its
space must be available before cleanup. Read
[the dated budget](../foundation-watch/evidence/incident-20261016-a3cdcf23-snapshot-budget.json)
for the measured inputs and limits; it is not a guaranteed snapshot time.

Read [the repair receipt](../foundation-watch/evidence/incident-20261016-a3cdcf23.json),
[flow checks](../foundation-watch/evidence/incident-20261016-a3cdcf23-flow.json),
[storage checks](../foundation-watch/evidence/incident-20261016-a3cdcf23-storage.json)
and `ROOT/reports/native-parent-proof-20261016` for the measured result.

## Installed storage and recovery patch, 13 October 2026

The 15:18 UTC incident stopped native capture at the 400 GiB reserve. The
validator later failed at 15:52:48 UTC with `No space left on device` in account,
ledger and snapshot writes. Systemd restarted it before this patch was installed.
Agave's startup cleanup supplied free space. No snapshot was removed manually.
A 260-bank sample found no restored non-marker files in already retired banks;
restored-file growth was not proved to be the incident cause. See the
[first incident](../foundation-watch/evidence/incident-20261013-839ef9ea.json) and
[patch and recovery receipt](../foundation-watch/evidence/incident-20261013-4cddb9a9.json).

The operating scripts are installed in
`ROOT/releases/storage-recovery-20261013-742b6735b4bbb606`. The final source
handoff helper is in `ROOT/releases/source-handoff-20261013-679547f433f4`.
The handoff retained the restarted validator PID 2579205 and adopted its healthy
capture, `000000000000000018de229d3fc8a9d9-275b05`. The old failed source and its
files remain. Reports are in `ROOT/reports/bank-v2-recovery-20261013-4cddb9a9-r2`.

At the 16:42:43 UTC heartbeat, all six required services were active and RPC
was healthy. The public SSE feed returned advancing slots. A 542,418-byte v2
payload matched its frame hash. Compaction still had a 3,167-slot backlog, down
from 3,609 over five minutes; the cloud queue fell from 4,851 to 4,111 events.
The v2 head was still stale. The watcher still reported `compaction_behind`.
These checks prove flow and progress, not full recovery or stable disk capacity.

The installed unit sets a limit of one full archive and one matching incremental
archive for the next validator start. The running process still uses its previous
limits. The installer checks the exact host's help before it changes
the command. [snapshot_budget.py](snapshot_budget.py) only reads files and creates
a plan. It protects the newest full archive, its newest matching incremental,
active temporary files and unknown paths. Archive names do not prove valid
contents. The plan includes space for the next archive. Agave removes old archives
after a new archive completes, so the new limits do not supply that write space.
The startup check requires at least 500 GiB plus a next-archive budget. That
budget is at least 128 GiB and increases to the largest observed archive
allocation. The check cannot stop later growth in a running validator.
The policy and pinned source checks are in
[snapshot-retention-policy.json](snapshot-retention-policy.json).

Cloud restore now checks disk space before download and during each write. It
keeps a 400 GiB reserve and a 1 GiB margin, with space for the temporary bundle
and requested file. A shared lock prevents concurrent restores in sibling source
sessions from passing admission together. A restore writes only the requested
file and checks the bundle and saved receipt. It removes its own partial files
after failure. It does not remove files that a reader can still use. Restored
files still need a separate cache retirement policy.

The recovery helper now changes all six live storage source paths with the feed
services. It publishes `ROOT/config/native-active-source.json` only after the
running commands match the new source. Updated heartbeat and priority readers
prefer this pin; only an absent pin permits the dated recovery-record fallback.
An invalid pin remains a visible failure. Historical recovery records remain.
The updated readers were installed before this source handoff.

The old publisher tail has a separate finite drain. Its wrapper counts remaining
events again after each restart, so a partial cloud commit reduces the next
limit. This handoff committed all 30 remaining events. The saved checkpoint and
remote head both equal 634171, with no pending upload intent. The old cold-storage
command is saved for a separate background resume;
the helper does not start another old storage job. The two-hour live retention,
full cloud byte checks and failed-source evidence remain required. Recovery still
requires at least 500 GiB free and a separate public flow check. Source handoff
checks the effective command order before it stops feed services. A saved switch
can resume after a partial service change without loading another capture.

The old last complete processed compact slot is 456452820. The new first
complete slot is 456502438. Their intervening interval is 456452821–456502437,
49,617 slots. This is not a count of lost produced blocks or proof of canonical
coverage. The native gap remains unrecovered.

For an existing deployment, stage the eight script files named by
[install_storage_patch.py](../bank-stream-v2/install_storage_patch.py) in one
directory. Run that installer as `ubuntu` with `--source-dir`, `--root`, and
`--prepare-only` first. Review its report before the apply run. It installs
guards and future validator settings, and restarts the sender and priority guard.
The source handoff is separate. Do not use the full native binary installer for
this script update. A failed source switch remains resumable with
`recover_session.py --root ROOT --report SAVED_REPORT --resume-switch`.

Validation: 152 deployment tests passed. The patch has a clean application
check against the saved repository base. No native gap was rebuilt. Old-source
cold storage still needs a separate resume, and restored files still need a
bounded cache policy with reader protection.

## Operating status, 12 October 2026

Later operations review on 13 October added an external machine/pipeline
watcher and a read-only, 30-second host heartbeat. Its working alert label is
Megaloadon. The checker is active. Telegram notifications now use the new
Megaloadon Alerts bot and the owner-confirmed private chat. A real TEST event
matched the API delivery receipt and the message visible in Telegram. The old publisher's 14 remaining events were committed,
its remote checkpoint was verified, and the completed drain/recovery units were
disabled. The tested 32 GiB worker limit is now permanent across reboot. Capture
progress, invalid health samples and interrupted CAR registry writes have
focused fixes. See [the watch and review record](../foundation-watch/README.md)
for actual checks, known limits and the remaining source-handoff work.

Later on 13 October, private Telegram `/status` and `/help` commands were
deployed and a real status reply was checked in the app. The host heartbeat
now includes read-only CAR progress, selected upload receipts and available
stage/history estimates. Only the heartbeat sender changed; validator PID
2630745 and the live compact/publish services were retained. At 06:44 Paris,
CAR 1051 was 66.3% compacted and native compaction had zero slot lag.
The owner later approved read access to only the controller log, retaining
owner root with group ubuntu and mode 0640. A tested parser update prevents
old ETA reuse on the final transfer row. The new heartbeat release sent
successfully without restarting the live services. See the watch record above.

The disk reserve stopped capture again. Direct access worked after the owner
loaded the SSH identity. Space was recovered with verified lossless compression
of closed benchmark payloads and removal of unused compiler caches. The
validator and capture plugin kept their binaries and validator PID 2630745.
The capture plugin was reloaded into that same validator process.

The new source is `000000000000000018ddd9a6bb6fc50e-282459`. Public v1 and v2
heads advance. A 565,104-byte public v2 payload matched its frame hash. The new
processed R2 catalog advanced from sequence 2176 to 8064 during the checks.
These are finite flow checks, not full epoch or sustained capacity proof.

The owner approved exact native compact/custody cloud copies and local space
release. `hivezilla-native-cold-storage-20261012-live.service` keeps at least two
hours locally. It waits for cloud publication, reader replay boundaries and
native writer acknowledgement, then checks full remote bundle bytes before
local retirement. A second unit drains the failed source. The restore test
recovered all 16 sample bank files with matching hashes. The worker has restore
support in `releases/native-cloud-retention-20261012`; no new validator or plugin
build was installed. Bundles have a 512 MiB bound and at most 64 banks. Two
bundle transfers can run at once. The main thread commits the recovery index
and releases local files only after each full remote byte check has passed.

The old-to-new capture gap spans slots 456019164–456135396. Local finalized RPC
starts at 456074947, so its older 55,783-slot prefix cannot be recovered from
this local RPC. A separate backup saves available blocks and keeps this missing
prefix explicit. No rebuilt native compact gap, entry or shred completeness is
claimed. A later published CAR can supply separate query history.

Foundation also runs the separate CAR 1048–1054 backfill. Its download uses a
private memory cache. Compaction and upload are sequential and have disk/memory
guards. See [the current upload record](../foundation-upload/README.md) and
[the flow receipt](../foundation-upload/evidence/recovery-flow-20261012.json).
Check machine reports for current progress. Automatic source changes, invalid
fork retention, sustained capacity and archive query integration remain open.

### Later load check and priority control, 12 October

Live conversion fell behind during recovery. The worker now aggregates callback
updates per bank, uses indexes for pending work, reads at most 16,384 discovery
rows per batch, and waits for the block footer before decoding an incomplete
bank. Four bank builders and the ordered durable event writer remain. The
installed worker is `releases/native-cloud-retention-fast-r3-20261012`.
Fourteen native unit tests and two real-builder restart/reuse tests passed.
The validator and plugin were not restarted or rebuilt.

The R2 publisher now accepts up to 256 events per batch, with the existing
64 MiB byte bound and 64 upload workers. Saved upload intents and checkpoints
remain in place. Thirty-two relay/publisher tests passed, including a lost
reply and restart with a larger batch. The cloud queue can lag the public feed.

`hivezilla-live-priority-20261012.service` checks the current source every three
seconds. It freezes CAR backfill and the old source's cold-storage drain when
capture is unhealthy, compact output is stale, the compact slot lag exceeds 32,
or the cloud queue exceeds 8,192 events. It resumes them only with a healthy
source, slot lag at most eight, and queue at most 4,096 events. The current live
cold-storage process keeps running. This protects its two-hour local copy and
verified retirement gates. Read `ROOT/reports/live-priority-20261012/STATUS.json`
and the finite load receipt in the upload evidence directory.

The RPC gap backup reached its two-hour runtime limit while frozen. Its saved
files remain. The helper now resumes from the original finalized slot list,
checks saved compressed data without replacing it, fetches still available
missing blocks first, and records pruned missing blocks as errors. This backup
has no runtime limit and has a separate two-core/two-worker bound. The priority
guard does not freeze it: local RPC retention has a moving deadline. A resume
does not make previously pruned blocks recoverable. Six focused backup tests
passed, including restart after pruning, corruption, and missing coverage.

At the later 00:17 Europe/Paris check on 13 October, native slot lag was zero,
the cloud queue was about 150 events, and CAR 1048 was about 73% compacted.
The new live storage process had retired 33,216 verified banks and released
126.47 GB in that run. Both live storage and the old source drain were active.
The resumed RPC backup retained 41,405 blocks, 50,388,439 transactions and
49.34 GB of compressed data. Of its original 60,345 produced blocks, 18,940
are now missing from this RPC backup; the following boundary block was also
pruned before its RPC check. The receipt is `INCOMPLETE`, and the earlier
55,783-slot pruned prefix remains explicit. No full gap recovery is claimed.

## Earlier operating status, 11 October 2026

The validator still runs with PID 2630745, but native capture has failed with
`disk reserve reached`. The compact worker has stopped and the bank v2 producer
cannot advance. The origin and relay processes still run; this does not mean
that new blocks are available. Free disk space at the check was 421,370,728,448
bytes, below the 400 GiB capture reserve (429,496,729,600 bytes). The old capture
and compact files remain retained. The failed session needs an explicit source
change after storage is available; a worker restart alone cannot repair it.

The old benchmark directory uses about 2.10 TB. Its files remain retained.
Local compact/evidence retirement was not enabled at that check.
The checks below describe earlier successful tests, not current live health.
See the [operating receipt](../../stream-relay/evidence/bank-stream-v2-20261010/OPERATING-STATUS-20261011.json).

The restart-gap RPC backup has completed: 4,754 blocks and 5,170,993 transactions
were saved, with zero errors and matching boundary hashes. These blocks still
need compact reconstruction. This backup does not establish native entry or
shred coverage.

## Tested native bank v2 deployment, 10 October 2026

A separate `native-bank-compact-v2` profile is installed. Use the private
`/v2/banks/head`, `/v2/banks/events`, and `/v2/banks/chunks/SHA256` routes at
`https://stream.blockzilla.dev`. They require a backend Bearer header. V1 and
the public demo keep their contracts. The same complete processed payload is
reused for native confirmed/root controls; no second compact conversion occurs.
The head contains exact native block height and queue expiry. Frozen or processed
state alone cannot supply confirmed readiness.

The current validator PID is **2630745**, on tested host revision
`d688dd0e3a576b048a848e5faf2e8d277f8feb6e`. Its session is
`000000000000000018dd432f0e8f799c-282459`. The actual host/plugin load and native
queue test passed. The host did restart for this ABI change. Failed startup and
old-host sessions are excluded and retained in the deployment record. Later
transport changes have preserved this validator process and source.

Current reports are under `ROOT/reports/bank-stream-v2-20261010`. Read them for
live status. The public 11-minute Rust collection and restart checks passed:
9,091 events, 3,099,103 decoded transactions, and about 32.4 minutes of retained
replay. Callback-to-consumer p95 was 5,387 ms across the whole test, including
restart and publication pause; it was 1,563 ms over the final 120 seconds. The
proposed 250 ms target was not met. JavaScript finite resume passed but recorded
no hash-ready observations with full-detail decode. See
[the v2 deployment guide](../bank-stream-v2/README.md) and
[the acceptance record](../bank-stream-v2/ACCEPTANCE.md) for tested boundaries,
source/build identities, private consumer use, and measured limits. Ferno
application integration, a second relay, automatic source changes, and local
compact/evidence retirement after upload remain unfinished.

### Later restart and storage check, 10 October

The machine uptime was about 11 days; there was no machine reboot. The native
validator and compact worker retained their tested PIDs. Saved compact session
files remain present. The validator installation restart left two intervals
without compact output: slots 455373084–455375622 and 455376166–455377837,
4,211 slots in total. The intermediate old-host session has another 543 compact
blocks. A slot count alone is not a count of lost produced blocks.

Local finalized RPC lists the full 4,754-slot interval between the old and new
session boundaries. Full boundary samples include transaction wire bytes and
metadata when `maxSupportedTransactionVersion=1` is used. A separate backup has
completed under `ROOT/reports/bank-v2-restart-recovery-20261010/rpc-blocks`; read
its `RECEIPT.json` for saved block counts and chain verification.
It includes the 543 already compacted intermediate blocks. This backup is RPC
recovery evidence; it is not rebuilt compact output, a native journal, or proof
of entry or raw shred coverage. The excluded native journals remain retained.

The v2 origin reached its 8 GiB cgroup limit and restarted once. Its durable
checkpoint restored the live stream. The origin limit is now 16 GiB, applied
and persisted through `systemctl set-property` without another restart.
Validator, compact worker, source, and spool stayed unchanged. The
[restart audit](../../stream-relay/evidence/bank-stream-v2-20261010/RESTART-AUDIT.json)
and [memory limit receipt](../../stream-relay/evidence/bank-stream-v2-20261010/ORIGIN-MEMORY-LIMIT.json)
record these later operating checks. The immutable provider source snapshot
remains the tested build; this paragraph records subsequent operating state.

## Earlier processed stream deployment, 10 October 2026

The public endpoint is `https://stream.blockzilla.dev`. It now serves one
`native-processed-compact-v1` feed through SSE notices and finite binary GETs.
The full compact feed accepts the same private Bearer key. No SSH or WARP is required. The
origin and relay remain on Foundation. A second relay machine is not installed.

The validator PID stayed **1106933**. Its pinned Agave 4.3.0/Rust 1.97.1 plugin
was reloaded from immutable release `processed-worker-v3-20261010`. No validator
restart occurred. The worker uses `processed-worker-v4-20261010`, with four
parallel bank builders and one ordered event writer. The tested CLI and
origin use `processed-stream-v1-20261010`. The relay now uses
`stream-demo-v4-20261010`. The R2 publisher uses
`processed-publisher-v3-20261010`, with 64 bounded upload workers.

The new source session is `000000000000000018dd37e5c7e1f5cf-10e3f5`. Its output
is `ROOT/compact/direct-agave-processed-live/SESSION`. Each complete bank is
converted once before root. Small `root` and `invalidate` records contain no
second payload. Rooted custody evidence selects the same bytes for the local
archive catalog and permits exact source retirement. Competing bank IDs and
parent links remain separate. Resume uses `SESSION:pEVENT_SEQUENCE:FRAME_SHA`.

The public JavaScript SDK test checked 16 events, repeated finite GETs, and
exact resume. A decoded block's **1,127 ordered first signatures**, blockhash,
parent hash, and parent slot matched independent finalized local RPC. The Rust
probe received 16 events and passed a separate three-event exact resume check.
One earlier Rust probe stopped on a connection reset; the bounded probe does
not hide that failure. The last 128 sampled roots all followed their payload
builds. These are finite integration tests, not a sustained capacity test.

An independent public mainnet RPC check returned slot 455329815; the local node
returned 455329814 and the worker had built 455329812. The stream crossed into
epoch 1054 without a session or cursor reset. All 21 sampled bank parent links
across slots 455327990–455328010 match. This proves the tested rollover,
not full-epoch coverage.

Tests passed: 26 native engine tests, including the real writer across an epoch
boundary and no second conversion after root/restart; 19 relay/publisher tests;
12 Rust decoder tests; 17 JavaScript tests; three Rust probe tests; ten token
indexer tests; and the launcher/health tests. Cargo builds and tests ran on
Foundation. The native plugin used the validator's exact Rust toolchain.

R2 frame publication uses a separate namespace:
`native-stream/processed/direct-agave-live-v1/SESSION`. The old finalized
publisher and namespace remain intact. The processed publisher resumes the
same durable upload intent and checkpoint after restart. Its catalog head
advances only after each batch's payloads and receipts pass the remote checks.
Upload can be behind live delivery. Public R2 backfill and archive query
integration remain unfinished. R2 upload does not delay the live feed.
The measured 64-event batches spent 39 ms reading local source and about
8–13.2 seconds on upload and commit. R2 was still behind the native feed at the
final check. The new timing fields make that limit visible.

The capture reserve changed from 500 to **400 GiB**. About 17 GB of obsolete
Cargo debug cache was removed. With owner approval, the old epoch-1000 CAR input
was then removed: **767,389,334,224 bytes**. Free disk space after removal was
**1,249,992,630,272 bytes**, about 1.25 TB. Executables, benchmark reports, cache
metadata, slot ranges, native data, and comparison results remain. The
[cleanup receipt](../../stream-relay/evidence/processed-stream-20261010/EPOCH1000-CAR-CLEANUP.json)
records the exact path and checks the retained cache files. The validator,
worker, origin, relay, tunnel, and R2 publisher were running after removal.
The test capture reached its old reserve and stopped with an
explicit failure; its files remain. Earlier failed production sessions and
their gaps also remain. Local compact/evidence retirement after R2 upload is
not implemented, so storage is finite. No full uninterrupted epoch is claimed.

The token SQLite indexer requires finalized history. It now rejects a processed
head before backfill or checkpoint changes. Client bank undo and reconciliation
remain application work. The pinned Tower completeness gate is not yet an
Alpenglow gate. Raw shred recording is not current and has no completeness
claim. Source-session changes still require an explicit origin/publisher switch.

The operator installer is `deploy_processed.py`. It checks the releases, saves
old units/config, reloads the plugin, starts a fresh worker, and switches public
services only after the new event log is ready. Its report directory is unique;
do not rerun it over an existing deployment. The live health unit has no start
rate limit, so its 15-second timer can continue. The source database, WAL, and
compact files stay read-only in the origin/publisher; only SQLite shared lock
metadata is writable.

Small test receipts are in
`hivezilla/stream-relay/evidence/processed-stream-20261010`. Server records are
`ROOT/reports/processed-stream-deploy-20261010`. Read live reports for current
machine status.

### URL-key access and public browser demo

The relay accepts `?api_key=KEY` on its private read routes. Headers remain
supported. URL keys can appear in browser history and access logs; use headers
in applications. Keys do not appear in chunk notices or returned chunk URLs.
The mTLS origin still accepts headers only.

The free [browser demo](https://stream.blockzilla.dev/demo) shows complete SSE messages
from [the blockhash SSE view](https://stream.blockzilla.dev/v1/blockhashes).
It requires no key. Each `block` record's JSON contains only `slot`, base58
`blockhash`, and `parent_blockhash`. A `root` or `invalidate` record's JSON
contains only `slot` and `blockhash`. The SSE `event:` line gives the event kind;
the `id:` line gives the exact resume position. Neither is repeated in JSON.
`block` arrives at
processed commitment. Later `root` and `invalidate` records remain visible.
This view has no transaction data or binary block payloads. The private compact
routes remain protected. Public metadata and demo requests use a separate
eight-request budget; private capacity remains separate. The demo keeps the
latest 100 messages and supports pause/resume.
The demo shows complete `id:`, `event:`, and `data:` lines. It does not remove
fields from the displayed JSON. Parent links remain in block events; controls
refer to already published blocks. The private binary feed retains full native
bank identities and its existing resume contract.

Twenty-two Rust tests and seven public access/resume checks passed. Eight public
events matched their authenticated binary bank identities. The real in-app
browser showed messages, pause, and cursor resume. The immutable relay release
is selected by `/etc/systemd/system/hivezilla-stream-relay.service.d/20-demo-access.conf`.
The previous unit and override are saved under
`ROOT/reports/stream-demo-access-v2-20261010`. Only the relay was restarted.
Validator PID **1106933** remained unchanged; capture, worker, origin, R2
publisher, and tunnel were active after the change. Small receipts and a browser
image are in `hivezilla/stream-relay/evidence/demo-access-20261010`.
The light update has a separate deployment receipt under
`ROOT/reports/stream-demo-access-v3-20261010`; 22 Rust tests passed again.
The full-message update uses `stream-demo-v4-20261010` and a separate receipt
under `ROOT/reports/stream-demo-access-v4-20261010`. It removes redundant public
JSON fields and displays the full SSE envelope. Its build and public/browser
checks are saved separately in the same local evidence directory.
All 22 Rust tests and seven public checks passed for this release. Eight public
records matched authenticated binary events; exact resume passed. The browser
displayed all SSE lines, kept 100 messages, and paused/resumed. Sample block JSON
was 141–143 bytes, or 278–280 bytes including the complete SSE envelope. Sample
root JSON was 76–77 bytes. The validator kept PID 1106933; the current capture
had no failure and the worker and other live services remained active.
`hivezilla/stream-relay/deploy/demo_access.py` installs a tested immutable release,
saves the unit/override, checks relay progress, and restores the prior override
if the deployment check fails. It does not build the binary.

## Services

- `hivezilla-validator.service`: permanent validator service, enabled for boot.
- `hivezilla-shred-hook-recorder.service`: raw packet recorder, enabled for boot.
- `hivezilla-direct-worker.service`: one compact worker for each capture session.
- `hivezilla-direct-health.timer`: durable-state check every 15 seconds.
- `hivezilla-epoch-dashboard.service`: dashboard on loopback port 8766.

Dashboard v3 shows the live network epoch, next epoch number, next boundary slot,
a countdown, and the estimated start in Europe/Paris time near the top. It reads
finalized epoch geometry and ten recent network performance samples from local
RPC. The estimate changes with slot rate. Missing or stale performance data does
not produce an invented start time. Timing and capture counts are shown separately.
All 12 dashboard tests passed before deployment, including epoch rollover and
missing-rate cases. Only the dashboard service was restarted.

The earlier `hivezilla-validator-canary.service` is stopped. The old
`hivezilla-epoch-1047.service` gRPC collector is stopped and disabled. Old capture
files remain in place.

The validator starts after the raw recorder. Its `Wants` dependency lets the
validator continue if the recorder stops. The recorder must be ready before a
validator restart: the current receive hook cannot reconnect after a recorder
failure. Restarting the recorder alone does not restore that hook.

## Files

- `config.production.json`: native plugin config, with public node identity.
- `hivezilla-validator.service`: validator unit.
- `hivezilla-validator-shred-hook.conf`: validator receive-hook drop-in.
- `hivezilla-shred-hook-recorder.service`: recorder unit.
- `dashboard.conf`: dashboard executable drop-in.
- `hivezilla-direct-worker.service`: worker unit.
- `hivezilla-direct-health.service` and `.timer`: health units.

The validator drop-in belongs at
`/etc/systemd/system/hivezilla-validator.service.d/shred-hook.conf`. The dashboard
drop-in belongs at
`/etc/systemd/system/hivezilla-epoch-dashboard.service.d/direct-agave.conf`.
All service file paths refer to immutable tested releases on the host. Do not
replace a shared library while the validator has it loaded.

## Capture and compact output

Native source is in `ROOT/captures/direct-agave-live/SESSION`. Compact output is
in `ROOT/compact/direct-agave-live/SESSION/epoch-N`. Epoch directory changes are
automatic. A new plugin load starts a new session. Initial partial roots are
listed in the worker status; they are not counted as complete blocks.

Check `ROOT/reports/direct-agave-health.json`. A running service is not enough:
the durable sequence, latest rooted slot, compacted slot, and shred packet count
must advance. `FAILURE.json` and `RETIREMENT-FAILED.json` are latched failures.
An advancing worker with more than 256 slots of backlog shows a warning. A
failed source, stopped writer, or stale worker status shows failure.
The journal can retire rooted records only after compact output and full source
evidence are committed. It retains other forks and incomplete source.

The compact worker has a 32-block batch limit and a 128 MiB source byte limit.
Worker v6 reads and assembles up to four banks at once, then checks them in slot
order. Each bank has a 128 MiB source limit. The validator plugin remains on its
immutable reward-fix v7 release. It uses an exhaustive typed match for all six
reward types in the pinned Agave build, including DeactivatedStake and VATDebit.
The callback queue can hold 65,536 events and at most 1 GiB. These are the current
supported limits. Queue acceptance is not durability. A long disk delay can still
fill this queue and stop capture.
The capture journal limit is 64 GiB. The raw recorder limit is 1,200 GiB. Both
keep a 500 GiB free-disk reserve. The actual shared free space can stop capture
before either quota. These limits are visible in the dashboard and status files.

R2 upload is not configured. Local output is finite. Full-epoch completion must
be measured from coverage, not inferred from a running process. Raw packets do
not prove complete or canonical shreds. This deployment does not repair the
missing interval in the earlier epoch 1047 capture.

## Recovery

A worker stop leaves its exact pending batch in durable state. Start the worker
again to resume that batch. A worker signal stop has automatic recovery, limited
to five starts per five minutes. An explicit capture or build error does not
restart. The CLI preserves an interrupted output stage.
Capture failures stay visible; do not remove a failure file to imply continuous
coverage. Preserve old sessions and receipts when changing releases.

On 2026-10-02, the earlier 16,384-event queue filled during the benchmark cleanup
period. The exact cause of the writer delay is not established. Native capture
was loaded again with the larger queue, without a validator restart. The old
failed source and compact output remain in place. The dashboard keeps the earlier
coverage warning.

Slots 452710428 through 452710957 are absent from native compact output. All 530
finalized blocks, with 781,721 transactions and full RPC metadata, were saved from
the local node in `ROOT/recovery/direct-agave-queue-overflow-20261002`. Their parent
chain matches the adjacent finalized blocks. Raw shred recording stayed active.
This RPC backup does not retain entry boundaries and does not prove full shred
coverage. The native compact gap has not been rebuilt.
The recovery receipt is `RECEIPT.json` in that directory. The incident report is
`ROOT/reports/direct-agave-queue-overflow-20261002.json`. Use
`preserve_rpc_gap.py ROOT OLD_SESSION NEW_SESSION NEW_DESTINATION` to save a future
gap from the local RPC while those blocks are still available. It saves full wire
transactions and metadata, and records RPC recovery as a separate source.

The source and live-test evidence are in `hivezilla/direct-agave`. The final
engine passed 22 tests on the validator's Rust compiler. A live forced-stop test
recovered 679 unique blocks and 927,098 transactions.
Worker v6 processed the same 349 real blocks 38 percent faster than v5. All 741
output object hashes and source receipts matched. See
`direct-agave/evidence/direct-agave-worker-v6-benchmark-20261002.json`.

## Reward outage on 2026-10-03

The earlier native adapter rejected DeactivatedStake during the epoch transition.
It also lacked VATDebit. The fixed adapter uses Agave's typed enum and its storage
protocol codes 5 and 6. A regression test preserves every native reward type,
negative debit amounts, partition metadata, and basis-point commission in native
source. All 22 engine tests passed before the new library was loaded. The
validator did not restart; worker v6 is unchanged.

The native compact gap is slots 452735969 through 452861383: 125,415 slots. This
number is a slot span, not a count of produced blocks. The old failed capture and
compact files remain. The new complete compact output starts at slot 452861384.
This gap has not been rebuilt. Epoch 1048 is partial.

Raw recording stayed active. The closed raw segment paths and durable prefix are
listed in `ROOT/reports/direct-agave-reward-outage-raw-manifest-20261003.json`.
Their presence does not prove full or canonical replay coverage.

The low-priority `hivezilla-reward-gap-backup-20261003.service` saves the remaining
finalized blocks from local RPC in
`ROOT/recovery/direct-agave-reward-outage-20261003`. At backup start, slots through
452776091 were outside RPC retention. That prefix is marked unavailable, not
skipped. The backup has a 700 GiB free-disk reserve, four RPC requests at most,
and a three-hour runtime limit. `STATUS.json` reports live progress. `RECEIPT.json`
is final only when the job finishes. A pruned prefix remains explicit even when
all remaining blocks are saved. This backup does not retain entry boundaries.

The incident report is `ROOT/reports/direct-agave-reward-outage-20261003.json`.
The release receipt is `ROOT/reports/direct-agave-reward-fix-release-20261003.json`.

## Owner-requested old shred deletion on 2026-10-04

The owner asked to delete previous-epoch shreds and retain the epoch that can
currently be compacted. Epoch 1049 starts at slot 453168000. The cleanup removed
16,579 sealed production shred WAL files, totaling 1,112,520,511,617 bytes. Each
removed file contained only valid-shape shred headers below that boundary.
Cryptographic verification was not performed. The mixed boundary segment
`0000000000003cc2.wal` and all later files remain, including the active segment.
No fixture, native journal, compact output, ledger, snapshot, RPC backup, or
capture metadata was deleted. Validator and recorder PIDs did not change.

The receipt is
`ROOT/reports/shred-previous-epoch-delete-20261004/RECEIPT.json`.
Its plan and deletion log identify each file. The older raw-retention manifest
is historical: its deleted prefix is no longer available for raw replay. Do not
infer old-gap recovery from that manifest. This was an explicit one-time owner
deletion, not an automatic retention policy.

About 2.07 TB of disk space was free after deletion. The recorder's
`journal_bytes` and quota counter are cumulative bytes written by its current
process. Removing sealed files does not lower that live counter or reset its
1,200 GiB session budget. Correct retained-byte quota accounting remains needed;
the recorder was not restarted because its current hook cannot reconnect.

During inspection, the compact worker hit its 16 GiB memory limit and restarted
automatically. Native capture and raw recording stayed active. Its startup
retirement recovery then used an unindexed lookup of committed artifacts.
Adding `commits_artifact` to the live audit database changed this lookup from a
full table scan to an index search and allowed recovery to advance. The schema
change is saved in development source; all 23 engine tests passed, including
an existing-database migration and indexed recovery regression. The existing
worker executable is unchanged. The index persists in the current audit database.

Worker MemoryMax was raised to 32 GiB at runtime without restarting it. This is
temporary headroom, not a proved correction for memory growth; the boot unit
still specifies 16 GiB. The exact memory-growth cause has not been established.
Reports: `direct-agave-worker-recovery-index-fix-20261004.json` and
`direct-agave-worker-index-tests-20261004.txt` under `ROOT/reports`.

## Local compact stream pilot on 2026-10-04

The separate `hivezilla-stream-origin.service` and `hivezilla-stream-relay.service`
are enabled and running. Their executable is the immutable
`ROOT/releases/hivezilla-stream-v1-20261004/hivezilla-stream`. Source, protocol,
client helper, service units, and tests are in `hivezilla/stream-relay`.

The read-only origin uses the current compact session's audit database and
durable cursor. It starts from a recent 256-slot window. It binds TLS to
127.0.0.1:8870 and requires a client certificate and API key. The relay initiates
one HTTP/2 upstream feed, fsyncs its finite replay spool, and serves binary HTTP,
SSE chunk notices, and binary chunk fetches at 127.0.0.1:8871. Both windows are
128 MiB. Native validator, recorder, and worker PIDs did not change during setup.
The origin has a 512 MiB memory limit and the relay 1 GiB; each has one CPU quota.

The API keys and the dedicated relay CA are under `ROOT/config/stream-20261004`
with private directory and key permissions. Keys are required in Authorization
headers, not URLs. Certificates last 30 days; renewal is not automated. The
origin service selects the current capture session explicitly. Switching source
sessions is not automated. No public tunnel route or firewall was changed.
The second relay machine has not been identified or deployed.

Eleven Rust tests, nine JavaScript unit tests, and ten process-level checks passed.
Tests include missing TLS client certificates, wrong keys, HTTP/2, CORS,
binary/SSE byte equality, forced relay restart, origin restart, replay errors,
and unread clients. Ten live consumers received the same eight chunks, containing
12 blocks and 4,824,713 bytes per client, in about 3.09 seconds. Only eight
upstream chunks were admitted during that test. This is a same-machine check,
not an external uplink benchmark. Wire rate was about 12.5 Mbit/s per consumer
over this short sample.

The raw JavaScript transport was also tested in Node 26.8.1 and real Chrome
154.0.8037.97. Both binary HTTP and SSE returned the same five live chunks in
all four cases. It returns compact objects and block identities. A portable
compact transaction decoder and a supported public projection contract remain
unfinished. The source remains an operator-trusted finalized projection with
canonical=false and publication_ready=false; native evidence files are excluded
from the transport payload and retained on the machine.

A 20-second sample with one origin-to-relay feed and no deliberate client fan-out
used about 0.70 percent of one core for the origin and 0.47 percent for the relay.
Service memory was about 166 MB and 332 MB respectively, including charged file
cache. These are observations, not full-load capacity results. The native health
report at the end of that sample showed one slot of lag and no current problems.

Reports under `ROOT/reports`: `hivezilla-stream-pilot-20261004.json`,
`hivezilla-stream-tests-20261004.txt`, `hivezilla-stream-transport-tests-20261004.json`,
`hivezilla-stream-live-test-20261004.json`, and `hivezilla-stream-resources-20261004.json`.
The browser/Node report is in local `hivezilla/stream-relay/evidence`.

## First full-feed semantic SDK, 2026-10-04

The owner confirmed a simple full-feed client first. The selective index stream
remains a later source. A shared Rust/Wasm decoder now runs in Node.js and browsers
through `hivezilla/stream-relay/client/sdk.mjs`. It filters static and loaded
accounts, top-level program instruction references, and recorded inner/CPI
programs locally. It keeps execution failure separate from finalized inclusion.
Small transaction records are the default; compact message/metadata details are
optional. Dictionaries remain chunk-local; no stable epoch-wide ID is promised.

Ten Rust tests and fourteen JavaScript tests passed. Real Chrome and Node received
the same four blocks in binary and SSE modes. Independent finalized local RPC
checks matched every one of 4,461 transactions, all 338 program-filter matches,
914 account-filter matches, and 658 failed transactions. Checkpoint restart and
stable allocator memory on repeated reads passed. No native service was restarted.

Build source and targets are isolated under
`/home/ubuntu/benchmarks/blockzilla-sdk-20261004`. The browser target was added to
Rust 1.98.0; no compiler or Cargo build was run on the operator Mac. The tested
638,988-byte Wasm artifact is in `hivezilla/stream-relay/client/decoder.wasm`.
Local evidence: `sdk-live-20261004.json`, `sdk-rpc-oracle-20261004.json`,
`sdk-rust-tests-20261004.txt`, and `sdk-javascript-tests-20261004.txt`, under
`hivezilla/stream-relay/evidence`.

This remains the same operator-trusted projection, with canonical=false and
publication_ready=false. The decoder emits compact semantic values, not complete
RPC JSON or exact signed wire bytes. The public route, supported publication
contract, CDN caching, archive backfill, and index source remain unfinished.
The current SSE server sends chunk notices; inline signature receipts are not
implemented. The SDK filters after receiving every block, so filtering does not
reduce network traffic in this version.

## Continuous token index and recovery, 2026-10-04

The Rust CLI now has `index-tokens` and `token-db`. The first command writes
transaction receipts and recorded pre/post token balances into SQLite. Rows,
block identities, and the resume cursor commit together. The second command
uses a read-only connection while indexing continues. Raw token amounts use
decimal text to preserve the full UInt64 range.

The consumer retries network failures from its saved chunk cursor. An expired
cursor starts a bounded recovery to a saved live head. Recovery can read published
Blockzilla archives or retained committed native compact files. It checks parent
slots and hashes before returning to live delivery. Missing history or a broken
chain leaves an explicit BLOCKED job and retains accepted progress. A test block
limit cannot suppress a later recovery failure. This is consumer recovery from
existing compact blocks; it does not replay shreds or repair native capture gaps.

Nine CLI tests and eleven decoder tests passed. A ten-block live check matched
11,638 transaction receipts and all 49,722 token balance records with local
finalized RPC. Cloudflare epoch 900 reads and restart produced six blocks and
10,326 balance records. A real expired-window run with unavailable epoch 1049
history stopped without changing its ten-block checkpoint. Native-file recovery
stored 292 historical blocks, then two live blocks, with 102,572 USDC balance
records in total. Six blocks around this recovery matched all 6,788 receipts
and 2,001 USDC balance records with local finalized RPC. A separate one-chunk
history copy proved that partial recovery still exits with failure after reaching
`--max-blocks 1`; its accepted block and BLOCKED job remained in SQLite.

`blockzilla-token-indexer-demo.service` is enabled for boot. It runs the continuous
USDC example against loopback port 8871, with local native recovery for the current
session. The database is `ROOT/indexes/token-demo/usdc.sqlite`; the executable is
`ROOT/releases/blockzilla-token-indexer-20261004-r2/blockzilla`. The unit is saved
in `hivezilla/stream-relay/deploy`. It has a two-core CPU quota and a 2 GiB memory
limit. Only this separate consumer was started/restarted. The validator, compact
worker, recorder, origin, and relay kept their existing PIDs.

The executable uses a development profile with optimized decoder, JSON, and
SQLite dependencies. Builds and tests ran with four low-priority workers under
`/home/ubuntu/benchmarks/blockzilla-token-indexer-20261004`. No Cargo build ran on
the operator Mac. The native decoder adds a small token-balance projection; the
existing browser/Wasm API is unchanged.

The sample archive still lacks the current live epoch. External long-outage
recovery needs an archive source that contains the missing range. Public stream
routing, automatic source-session changes, database retention, and full-epoch
coverage remain unfinished. These rows are balance observations, not a complete
token-account state database. See `docs/start/token-indexer.md`.

The final native health check showed advancing compact output with three slots of
lag and no current compact problems. It also showed that raw shred capture had
stopped at its cumulative 1,200 GiB quota. The recorder process remained active,
but its packet count had stopped. No raw shred continuity claim is valid after
that stop. The existing receive hook cannot recover from a recorder-only restart;
this task did not restart it or the validator.

Local evidence is under `hivezilla/stream-relay/evidence/token-indexer-20261004`.
Deployment and RPC check reports are also under `ROOT/reports` as
`blockzilla-token-indexer-demo-20261004.json` and
`blockzilla-token-indexer-native-rpc-oracle-20261004.json`.

## Foundation native snapshot backup, 2026-10-04

The Foundation bucket credential was found in the existing Ferno BTC secret
file. Its destination is `solana-archive-data`. The separate
`hivezilla-foundation-native-backup.service` was enabled for reboot. It uses
`ROOT/releases/foundation-upload-20261004-r2` and a private configuration under
`ROOT/config/foundation-r2`. Only the new backup service was started and repaired;
no validator, worker, recorder, stream, or token-consumer service was restarted.

The job freezes the committed native artifact list from the three source audit
databases. Its snapshot has 363,496 chunks and 518,636 stored blocks, ending at
slot 453348420. It uploads standard tar packs and original source receipts under
`native-backup/direct-agave-20261004`. Its durable state is
`ROOT/reports/foundation-native-backup-20261004`.

The backup preserves known coverage gaps, `canonical=false`, and
`publication_ready=false`. It does not include later commits or configure the
public native archive API or continuous R2 publication. Local compact output is
retained. Job completion must be read from `STATUS.json` and the pack records.
See `hivezilla/deploy/foundation-upload/README.md` for checks and limits.

On 2026-10-05 the owner restricted cloud uploads to CAR-derived Compact V2
archives. The separate native backup service was stopped and disabled after
1,931 complete packs, with one pending pack retained. Its decision record is
`ROOT/reports/foundation-native-backup-20261004/STOPPED-BY-OWNER-20261005.json`.
Already uploaded packs and local source data were retained. Do not restart this
backup without a new owner instruction. The validator and live compact worker
were not restarted; live compaction continues locally.

## Stream repair and live R2 publication, 2026-10-05

The live check found the origin stopped with `attempt to write a readonly
database`. The compact worker kept running. The relay was FAILED and the token
consumer had stopped. SQLite WAL readers need mutable shared lock metadata;
the stream origin and native-history consumer units now permit writes to only
`committed.sqlite-shm`. The Rust source reader retries transient SQLite errors
at the same position. Database rows, the WAL, and compact files stay read-only.

Only the origin and relay were restarted for the repair. Their release is
`ROOT/releases/hivezilla-stream-r2-20261005`. The origin is still mTLS on loopback
8870. The relay is still on loopback 8871 and uses the new
`ROOT/stream-relay-spool-20261005`. The old failed spool remains in place.
Consumer checkpoints were retained. The USDC consumer was restarted and resumed
native-file recovery; sustained catch-up remains a separate performance issue.

The owner then requested publication from the Foundation machine to its R2
bucket. The new Rust `hivezilla-native-stream-publisher.service` is enabled for
reboot. It publishes the exact compact stream frames, ordered commit receipts,
and a catalog head under
`solana-archive-data/native-stream/direct-agave-live-v1/SESSION`. Its first slot
is **453600000**, the start of epoch **1050**. It continues into later epochs of
the same capture session. It has a one-core CPU limit, a 512 MiB memory limit,
16 upload workers, and a batch bound of 64 frames and 64 MiB. The release is
`ROOT/releases/hivezilla-stream-r2-20261005-r2`. Its state is
`ROOT/reports/native-stream-r2-live-20261005`; credentials are in the private
`ROOT/config/foundation-r2/native-stream.env`.

Sixteen Rust checks, nine JavaScript transport checks, and ten process-level
checks passed. A serial R2 check saved 17 blocks in 31.82 seconds. A parallel
check saved 84 more blocks in 18.61 seconds. The live read-back check at 14:13 UTC
found 473 published blocks and 303,638,334 bytes. It checked one complete
847,754-byte frame against all 12 source files. Its first produced slot was
453600000. Reports are under `ROOT/reports`; the R2 read-back receipt is
`native-stream-r2-readback-20261005.json`.

A later check at 14:20 UTC found 3,249 published blocks and 1,657,039,933 bytes.
Upload lag had decreased to 1,137 slots. A real publisher restart resumed its
saved state and advanced receipt sequence 1744 to 2320. Four live chunks were
identical through binary HTTP, SSE plus GET, and direct chunk GET. The receipt is
`native-stream-r2-final-check-20261005.json`, also copied into the stream crate's
`evidence/` directory. The publisher was RUNNING; it was still catching up.

The validator PID remained 1106933 and the compact worker PID remained 4105319.
The old native snapshot backup remained disabled. CAR archive upload uses a
separate source and prefix. Source files remain local. Native cloud data retains
`canonical=false`, `publication_ready=false`, and `evidence_included=false`.
Full epoch coverage, public R2 reads, automatic R2 SDK recovery, and a separate
Hetzner relay remain unfinished. The relay host still needs access details.

## Epoch 1049 stored-output verification, 2026-10-05

The owner requested full blockhash continuity and PoH checks for the completed
local epoch. The committed inventory contains 431,667 blocks from slot 453168000
through 453599999, in 302,501 compact chunks. It contains 501,801,592 recorded
transactions, 522,157,527 signatures, and 513,081,322 PoH entries.

The full metadata and registry continuity pass succeeded. Every parent slot and
parent hash matches across all chunks. Both the retained epoch-1048 predecessor
at 453167999 and epoch-1050 successor at 453600000 match. The 333 absent slots,
in 82 ranges, are skipped in this recorded rooted chain. This is stronger than
the earlier gap-only check, but is not an independent archive coverage certificate.
Local RPC has already pruned epoch 1049.

The native PoH adapter passed three new tests and all 44 archive verifier tests.
Five Python orchestration checks passed. The broader Rust test run initially
failed repair fixtures under the SSH session's group-writable default umask;
all 44 passed with private `umask 077`, which those fixtures require.
Five real chunks passed, including the first and last epoch blocks, a chunk
rebuilt after the worker restart, and a skipped-slot gap. Their 16 blocks contain
19,006 entries; 49,999,360 hash rounds were recomputed without external-root fallback.

`blockzilla-native-epoch-1049-verification-20261005.service` is a separate,
one-time verification job. It uses 48 parallel chunk workers, Nice 19, CPUWeight
10, IOWeight 10, a 48-core CPU ceiling, 8 GiB memory pressure limit, and 12 GiB
memory ceiling. Its binary and runner are in
`ROOT/releases/native-epoch-verifier-20261005`. The source is read-only except
for the SQLite shared lock metadata needed by the source reader. Validator,
compact worker, and publisher services are not restarted.

Progress and receipts are in
`ROOT/reports/native-epoch-1049-verification-20261005`. `CONTINUITY.json` is PASSED.
Read current `STATUS.json` for the full PoH run. Do not claim that PoH passed until
`RECEIPT.json` records every one of the 431,667 blocks and 513,081,322 entries.
The job keeps a separate result database and can resume without modifying
native artifacts. These checks do not establish independent consensus, complete
shred coverage, tick schedule validity, or Ed25519 signature validity. The source
projection and publication flags remain unchanged.

At 21:12 UTC the job had verified 8,835 blocks and 9,822,597 entries. Measured
CPU use was 47.97 core equivalents over five seconds, and charged memory was
about 3.15 GB. Throughput was about 53 blocks per second; the estimate was about
133 minutes remaining. Validator PID 1106933 remained healthy, capture remained
CAPTURING, and compact lag was three slots. Local evidence is under
`hivezilla/direct-agave/evidence/native-epoch-1049-verification-20261005`.
These are progress measurements, not a completed PoH receipt.

### Completed PoH and CAR comparison, 6 October 2026

The full PoH job is now **PASSED**. Its final receipt records all 302,501 chunks,
431,667 blocks, 513,081,322 entries, 501,801,592 transactions, and 522,157,527
signatures. It recomputed 1,079,986,176,000 hash rounds in 8,117.65 seconds.
This supersedes the earlier progress measurements. Ed25519 and tick schedule
checks were not run; the native publication flags remain unchanged.

The published epoch-1049 CAR slot list has independently confirmed **zero
missing native blocks and zero extra native blocks**. Both sources contain
431,667 blocks and 501,801,592 transactions. All 333 absent slots are absent
from the public CAR list as well. They are skipped slots, not lost compact
blocks. The separate content checks below establish the tested field matches.

The full CAR block and signature comparison **PASSED** on 6 October. The
760,850,591,135-byte public CAR was downloaded to the archive machine,
`ach@192.168.1.46`, and imported
under `/volume1/blockzilla/jobs/native-car-compare-e1049-20261006`. The comparison
kept native files and the native inventory on Foundation. Only public CAR
records moved from the archive machine to Foundation. `compare_car_epoch.py`
checked all 431,667 block identities, parent links, transaction counts, and
522,157,527 ordered signatures (33,418,081,728 signature bytes), with zero
mismatches. The import completed at 13:20:42 Europe/Paris. The final receipt is
`RECEIPT.json`.

`bench/native-car-compare` checked execution metadata in 146 sampled blocks,
including all 82 skipped-slot boundaries, with 191,421 transactions. All tested
message
account and instruction indexes, block times and heights, reward values, fees,
balances, errors, inner instructions, token balances, loaded addresses, return
data, and compute costs match. Block reward order differs between sources; the
comparison sorts resolved reward values and preserves duplicates. It does not
compare top-level instruction data or recent blockhash bytes.

Logs are **not fully equivalent**. The native sample contains 294 transactions
with `Log truncated` (0.15% of 191,421); the CAR sample has no such marker. Log
values differ in 92 sampled blocks. The pinned Agave runtime uses a 10,000-byte
log limit by default; no explicit override is present in the validator command.
The plugin copies the received log list. The compact output cannot restore log
lines that Agave did not supply. This is a source limit, not missing blocks.
The final sample receipt is `METADATA-RECEIPT.json`; its metadata state is
`FAILED`, with no non-log mismatches. Overall `STATUS.json` records
`LOG_CONTENT_DIFFERENCE`. Full-epoch execution metadata was not compared.

The bounded CAR test also exposed a V1 bug in the first-seen importer. V1
signatures follow the message, and first-seen keys must stay inline until
assignment. Both cases are fixed. All 39 first-seen tests passed, including four
new V1 tests. The fixed importer then matched the standard importer on the same
64 real CAR blocks, including all sampled metadata and signature bytes. The
native validator and NAS production converter were not replaced. The full CAR
job uses the existing standard importer.

The one-off `finish_car_comparison.py` controller completed its work: it waited
for the CAR import, exported public CAR records, compared them on Foundation,
and saved the full block and 146-block metadata receipts. Its process record and
log are in the local evidence folder. It did not restart a live service.

Reports are under `ROOT/reports/native-car-1049-comparison-20261006`. Small local
receipts are under `hivezilla/direct-agave/evidence/native-car-1049-20261006`.

A separate live check found that native capture stopped in epoch 1050 at
02:13:36 UTC (04:13:36 Europe/Paris) on 6 October, with
`disk reserve reached; capture stopped with explicit gap`. The compact worker
is FAILED; the validator remains running. This does not change the sealed
epoch-1049 output. This comparison task did not restart capture or the validator.

## Public HTTPS compact stream, 5 October 2026

The existing `hivezilla-validator` Cloudflare Tunnel now routes
`stream.blockzilla.dev` to the loopback consumer relay at `http://127.0.0.1:8871`.
Its remote configuration advanced from version 1 (catch-all 404 only) to version
2 with this one hostname and the unchanged catch-all. A proxied CNAME points to
`0cd18a26-d76a-408c-a1fa-55a1151fa3ae.cfargotunnel.com`. The NAS tunnel and its
hostnames were not changed. Cloudflare and Google public resolvers returned the
new record. The operator Mac's system resolver retained an earlier negative
answer during the check; a request using a verified public DNS address reached
the endpoint and returned the expected HTTP 401 without a key.

Consumers use `https://stream.blockzilla.dev` with a private Bearer API key.
No SSH forwarding or WARP is needed. The validated public transport is SSE chunk
notices plus finite binary GETs. A long binary response can be buffered by
Cloudflare Tunnel, so consumers must not select that public transport by default.
API-key admission remains in the Rust relay. Missing/wrong keys are rejected;
all four public read routes returned 401 with a wrong key. The node RPC,
dashboard, origin mTLS endpoint, and management sockets are not published by this
hostname. Browser origins still need explicit relay CORS configuration.

The new Rust source example is
`hivezilla/stream-relay/examples/rust-consumer`. It uses reqwest/Tokio,
`hivezilla_stream::frame::decode`, and `blockzilla_stream_decoder::decode`.
Three parser/URL tests passed. A client on the Foundation machine called the
public HTTPS hostname, decoded eight chunks containing 12 blocks and 12,817
transactions, then resumed from its received cursor. No SSH forwarding carried
the test data. Build and test source is isolated under
`/home/ubuntu/benchmarks/blockzilla-rust-consumer-20261005`; two low-priority build
workers were used. This is a bounded read-only probe, not a persistent indexer,
automatic reconnect client, or archive backfill implementation.

The consumer credential remains in private
`ROOT/config/stream-20261004/client-api.key`. The owner's local copy is outside
the repository, at `~/.config/blockzilla/foundation-stream-20261005.key`, mode
0600, in a mode-0700 directory. The credential is not in these records.
Local test evidence is in `hivezilla/stream-relay/evidence/public-stream-20261005`;
the server reports are `ROOT/reports/rust-stream-public-20261005.json` and
`ROOT/reports/public-stream-access-20261005.json`.

The validator, compact worker, origin, relay, tunnel, and R2 publisher were not
restarted. The existing tunnel applied its route without a process change.
The source still keeps `canonical=false` and `publication_ready=false`.
Public R2 recovery, CDN caching, sustained fan-out tests, certificate renewal,
automatic session changes, and a second relay machine remain unfinished.
