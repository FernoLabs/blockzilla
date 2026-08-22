# Compact V2 Recovery Mini Handoff (2026-08-15)

## Current live state

- SSH target used for all NAS commands: `ssh ach@192.168.1.46 -p 22`
- Active scheduler: `blockzilla-archive.service` (disabled, running), PID `918121`
- Running binary: `/home/ach/.config/systemd/user/blockzilla-archive-secure/blockzilla-compact-v2-4dedbdeb803889f3d4a4ebb6904ee9575a19c9669fc96626dec61006aa422738`
- Scheduler mode: `paused=true`, lanes=0, running=0, registry concurrency=0, no active readers
- `/usr/bin/curl` on NAS is broken (missing `libquiche`), so recovery calls use the attached python client
  - Script: `/home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-detached-client-af3623859c0a88b0dc6c56e372fda64228a25f16fc4e9d0b0c61eb820bbc1adb.py`
  - Script SHA: `af3623859c0a88b0dc6c56e372fda64228a25f16fc4e9d0b0c61eb820bbc1adb`

## What is done

- Source-audit batch for 11 receipt-source old epochs was completed and attested.
- Scheduler fix is deployed and validated:
  - `blockzilla-compact-v2-4ded...738` running safely
  - reader/process lock and lock-order contracts are in place
- Directory modes were repaired:
  - epoch-305 quarantine and all 10 remaining target dirs are `0700`
  - identities (dev/inode/size/uid/gid/nlink/mtime and children) are unchanged except ctime/mode where expected
- Epoch 305 profile-neutral preserve finished:
  - Unit: `blockzilla-profile-neutral-recovery-305-4ded-quiet1.service`
  - Result: `HTTP200` + `valid_success=true` in  
    `/home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-recovery-305-4dedbdeb803889f3d4a4ebb6904ee9575.quiet1.result.jsonl`
  - Marker: `/volume1/blockzilla/scheduler-state/registry_reprocess/epoch-305.json` is `schema_version=4` and `state=retry_ready`
  - Target is absent; quarantine exists and is in mode `0700`

## Exact next work

1. Run the same detached, single-epoch one-shot flow for:
   `404, 405, 501, 502, 503, 504, 505, 864, 997, 1000`
2. For each epoch:
   - keep scheduler paused and idle
   - lock/identity gates must pass
   - use exact endpoint:  
     `POST /api/v1/jobs/archive_v2_registry_reprocess/{epoch}/rebuild-profile-neutral`
   - incident ID: `profile-neutral-registry-reprocess-post-rebuild-2026-08-14-v1`
   - authority SHA: `f471bb2078e719da508c4a8d22980a59e7d99140fe0682289bacb401ea10b5cf`
3. Require each response to be `HTTP 200` + `valid_success=true`.
4. Stop at first failure and do not retry without fixing the exact cause.
5. Do not run recovery from a live SSH session scope that leaves `session-*.scope` alive; each client must be launched as detached `app.slice` one-shot and let SSH exit immediately.
6. After all 11 are `retry_ready`, only then run stale-PoH closed set (`1001,1002,1003,1004,1005,1006,1008`) and then proceed to normal rebuild pass.

## Current known blockers

- A duplicate/noisy launch happened once; we must keep one detached client only.
- A long 409 is still possible if any reader-process census mismatch appears; this is now fail-closed and should be fixed explicitly, not retried blindly.
- Keep a single file in one place: no temporary source, no new service start without gates.

## Next milestone

- Complete all 10 remaining `rebuild-profile-neutral` epochs.
- Re-run marker and attestation pass for each to confirm `retry_ready`.
- Then set registry concurrency to `1`, resume scheduler, and run final deep audits after each clean rebuild.

