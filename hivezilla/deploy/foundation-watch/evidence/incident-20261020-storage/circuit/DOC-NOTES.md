# Draft deployment record text, 20 October 2026

This file contains proposed text only. Neither production README was changed.
At preparation, `BUILD.json` is `READY_NOT_ACTIVE`: all 64 host tests passed,
but activation and live proof are pending. Do not state that the circuit is
active until the installation and post-change proof are accepted. Do not
replace or change the 18 October evidence bindings.

## Direct Agave insertion point

Insert the following section in `hivezilla/deploy/direct-agave/README.md`
after the opening raw-capture paragraph and before
`## Evening publisher alerts, 18 October 2026`. The links in this block are
relative to that README.

```markdown
## Storage transfer recovery, 20 October 2026

At 05:52:24 UTC (07:52:24 Europe/Paris), event
`bbdb2969-0b9e-4ae6-99a3-cd8d9087a47f` reported
`cloud_storage_not_running`, `telemetry:cold_storage_failed` and
`telemetry:storage_jobs_degraded`. The latest saved failure was
`BundleTransferTimeout` at 05:50:44 UTC. The 300-second child deadline worked,
but the failed transfer ended the live storage process. The supervisor
restarted it, and verified archive and retirement progress resumed. Its retry
budget was not exhausted. Earlier process exits were also recorded at 05:31
and 05:40 UTC; their exact failure types were not independently retained.
Read [the findings](../foundation-watch/evidence/incident-20261020-storage/FINDINGS.json)
and [the retained history](../foundation-watch/evidence/incident-20261020-storage/ALERT-HISTORY-SUMMARY.json).

The child did not retain PUT or GET phase timing. These records do not show
which operation used the deadline or establish the underlying cloud fault.
The sampled capacity was OK, with positive snapshot and recovery headroom.
This failure was not a disk-reserve failure.

The prepared repair keeps known temporary transfer faults inside the live
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

Disk capacity remains a separate limit. The conservative capacity checks
keep the 400 GiB capture reserve and 500 GiB recovery minimum, plus the full
next-archive budget. Expected snapshot purge space is not current free space.
A durable byte bound for retained native metadata remains designed and
unimplemented. This repair adds no metadata deletion authority and does not
establish sustained capacity. Read [the capacity review](../foundation-watch/evidence/incident-20261020-storage/CAPACITY-REVIEW.json)
and [its addendum](../foundation-watch/evidence/incident-20261020-storage/CAPACITY-REVIEW-ADDENDUM.json).
The raw capture remains FAILED while its recorder service is active; its
recovery still requires a later validator restart.
```

## Foundation watch insertion point

Insert the following section in `hivezilla/deploy/foundation-watch/README.md`
after the machine identity and before
`## Evening publisher alerts, 18 October 2026`. The links in this block are
relative to that README.

```markdown
## Storage transfer alerts, 20 October 2026

Event `bbdb2969-0b9e-4ae6-99a3-cd8d9087a47f`, created at 05:52:24 UTC
(07:52:24 Europe/Paris), reported `cloud_storage_not_running`,
`telemetry:cold_storage_failed` and `telemetry:storage_jobs_degraded`. These
were real stopped-job reports. The latest saved failure was the existing
300-second `BundleTransferTimeout` at 05:50:44 UTC. The supervisor restarted
the job and durable archive and retirement progress resumed. The retry budget
was not exhausted. The saved child result does not identify the PUT or GET
phase that used the time, so the underlying cloud fault remains unknown.
Read [the findings](evidence/incident-20261020-storage/FINDINGS.json)
and [the history](evidence/incident-20261020-storage/ALERT-HISTORY-SUMMARY.json).

The prepared repair adds a durable source-wide recovery circuit inside live
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
Activation and live proof are pending.

Capacity alarms are separate. The sampled capacity was OK during this
diagnosis. The current policy still requires the 400 GiB capture reserve and
500 GiB recovery minimum, plus the full next-archive budget. It does not
count expected snapshot purge as free space. A durable byte bound for retained
native metadata remains designed and unimplemented; the network circuit does
not solve that limit. Read [the capacity review](evidence/incident-20261020-storage/CAPACITY-REVIEW.json)
and [its addendum](evidence/incident-20261020-storage/CAPACITY-REVIEW-ADDENDUM.json).
The known raw-capture failure remains separate from native feed availability.
```

## Required final edits after accepted activation proof

1. Replace “prepared repair” with the accepted active release description.
   Use only the release, new live PID, source and registry bindings from the
   canonical `INSTALLATION.json`. Do not infer these values from `BUILD.json`.
2. Replace the pending sentence with measured post-change results. Link the
   canonical installation, independent cloud proof, live check, flow check
   and final monitor only after these files exist and their states pass.
   Do not describe a local checkpoint as independent cloud verification.
3. In each existing `Later capacity alerts, 18 October 2026` section, change
   “Live cloud storage and its supervisor now use” to “The 18 October
   installation used”. Change “the active deadline installation” to “the
   18 October deadline installation”. The old PID statements and 51-test
   result describe that installation only; retain their original evidence.
4. Keep the known raw failure, unchanged finite caps, packing exclusion and
   unimplemented metadata byte bound explicit. Do not state that the new
   circuit proves sustained capacity or identifies the inner timeout cause.

## Preparation checks

The proposed links above refer to existing evidence. The build reports 64
tests passed, no service actions and no bank-cap changes. The direct Agave
introduction still states the raw failure. No activation or post-change pass
claim is made in this draft.
