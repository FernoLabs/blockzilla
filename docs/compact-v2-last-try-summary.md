# Compact V2 recovery — last try status and next step

Date: 2026-08-15 (local workspace snapshot)

## Where we are now
- Scheduler binary deployment is complete:
  `/home/ach/.config/systemd/user/blockzilla-archive-secure/blockzilla-compact-v2-4dedbdeb803889f3d4a4ebb6904ee9575`
- Management API is bound on loopback; control mode is observer-ready for recovery calls when paused.
- Source-audit cohort (11 receipts) is complete and attested.
- Directory modes for required targets are `0700`.
- Epoch 305 preserve path is `retry_ready` and clean.
- Latest reliable block showed recovery calls blocked only by deterministic control gates, not by data corruption.

## Blocking pattern to fix before next run
- `profile-neutral` endpoint returns `409` only when one of these fails: scheduler ownership, marker shape, or process scan checks.
- Main recent block message is:
  `profile-neutral registry recovery remains blocked ... reader process table is unobservable`.
- This is not solved by repeating the same call blindly.

## Critical work set
- The incident set is 18 epochs:
  `305,404,405,501,502,503,504,505,864,997,1000,1001,1002,1003,1004,1005,1006,1008`
- Epoch 305 already has `retry_ready` success evidence.
- Remaining profile-neutral `rebuild-profile-neutral` calls should run in this order (from last stable plan):
  `404,405,501,502,503,504,505,864,997,1000`
- Stale-PoH batch after that:
  `1001,1002,1003,1004,1005,1006,1008`

## One clean step now (if ready)
Run one detached unit at a time only, wait for result file `valid_success=true`, then proceed:

`systemd-run --user --unit=blockzilla-profile-neutral-recovery-<E>-4ded-manual --slice=app.slice --description="Resume profile-neutral epoch <E> recovery" --property=Type=exec --property=Restart=no --property=KillMode=control-group --property=TimeoutStopSec=20s --property=UMask=0077 --property=NoNewPrivileges=yes --property=PrivateTmp=yes --property=MemoryMax=268435456 --property=TasksMax=32 --property=OOMPolicy=stop --property=OOMScoreAdjust=200 -- /home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-detached-client-af3623859c0a88b0dc6c56e372fda64228a25f16fc4e9d0b0c61eb820bbc1adb.py /home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-recovery-<E>-4ded.result.jsonl <E>`

- Do not run another SSH session in `session-*.scope` at the same time.
- Keep one recovery unit only.
- No overlap, no polling loop while the 20s warm-up and hash window runs.

## If we switch to CAR-first bypass
- CAR fallback does not remove these 18 epoch transitions.
- It is only useful if all required CAR inputs are available and trusted locally.
- Current earlier checks found CAR availability incomplete; not a guaranteed speed win.
