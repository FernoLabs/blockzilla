# Compact V2 recovery handoff — 2026-08-15

## Goal

Finish the clean, canonical Compact V2 recovery for 18 exceptional epochs without replacing or deleting validated source data.

All 736 archive epochs are already readable as Compact V2. This incident work repairs or certifies 11 old profile-neutral registry generations and 7 stale-PoH registry generations.

## Current NAS state at 01:19 CEST

- SSH path tested directly: `ssh ach@192.168.1.46 -p 22` (connection works from this environment).
- Scheduler service: `blockzilla-archive.service` is active/running, disabled, main PID `918121`.
  - `blockzilla-compact-v2` binary in use is `/home/ach/.config/systemd/user/blockzilla-archive-secure/blockzilla-compact-v2-4dedbdeb803889f3d4a4ebb6904ee9575a19c9669fc96626dec61006aa422738`.
- `status.json` under `firewatch-index` is currently idle (`running=0`, `capacity_configured=0`) and reports observer mode: `admission_blocked_reason = "observer mode; child execution is disabled"`.
- `/volume1/blockzilla/scheduler-state/registry_reprocess/epoch-305.json` is durable and still:
  - `schema_version=4`, `state=retry_ready`
  - `recovery_receipt_sha256 = a86bfc206ddd91f7cf4bd3f9a57de9e3289b4b8a3ea41eaeb4ea7fd96a4da8b7`.
- Generation modes are now `700` (owner-only, no group/other write) for:
  - 305 quarantine: `/volume1/blockzilla/archive/.usage-sorted-generations/.epoch-305.registry-reprocess.profile-neutral-post-v1.fca639213c23226516d24b1f3fbb789af23793fc584f277721799e51217cc924.quarantine`
  - target dirs: `/volume1/blockzilla/archive/.usage-sorted-generations/epoch-{305,404,405,501,502,503,504,505,864,997,1000}`
- Current recovery attempts:
  - `...-result.jsonl` -> HTTP 409 "directory identity changed" (expected while dir was `0777`).
  - `...b.retry.result.jsonl` and `...b.retry3.result.jsonl` -> HTTP 409 "reader process table is unobservable".
  - `...quiet1.result.jsonl` -> HTTP 200, with message: "legacy generation already preserved ... clean Post rebuild remains ready".
- No recovery process is currently running. There are terminal/failed one-shot units (`blockzilla-profile-neutral-directory-mode-repair-20260815.service`, `...recovery-305-4ded*.service`) that can be ignored.

## Current NAS state at 00:35 CEST

- Scheduler unit: active, running, disabled, and manually started.
- Scheduler PID: `918121`; restart count: `0`.
- Scheduler is globally paused. Registry concurrency is `0`. There are no lanes, children, or workers.
- Live scheduler binary:
  `/home/ach/.config/systemd/user/blockzilla-archive-secure/blockzilla-compact-v2-4dedbdeb803889f3d4a4ebb6904ee9575a19c9669fc96626dec61006aa422738`
- Binary SHA-256: `4dedbdeb803889f3d4a4ebb6904ee9575a19c9669fc96626dec61006aa422738`.
- Frozen scheduler source SHA-256: `55af0b6f726036fa5ccadf4a4593efec86c9bf200b2d0bc5066f9433aa4c738b`.
- Validation for this source: reader tests `19/19`, recovery tests `11/11`, controller-lock tests `2/2`, full Blockzilla tests `604/604`, independent review GO.
- No OOM, restart, or data-content failure is active.
- Directory-mode repair is now durable: the 11 required target/quarantine dirs are at `0700`.
- The initial helper run for this path used the same detached client and result artifacts now show successful `retry_ready` proof.

## Work completed

1. Epoch 998 PoH orphan-tail repair completed and was reconciled to a durable `complete` marker. Its canonical PoH and repair proof remain validated.
2. The 11 legacy source generations passed strict Post-profile audits. There are 11 valid attestations and all 169 receipt-bound source-file identities match.
3. The reviewed Firewatch auditor and controller artifacts were deployed. The 180 required NAS permission repairs were applied and verified.
4. The scheduler process scan was corrected and reviewed:
   - stable PID start-time and all four UID fields;
   - two equal process-table passes;
   - exact managed-reader controller lock held through rename and final proof;
   - only the exact systemd user-manager `init.scope` can use the private-surface exclusion;
   - SSH session scopes and all managed Blockzilla `app.slice` units remain fully scanned.
5. Epoch 305 preservation moved the exact old target to quarantine and published a schema-4 `retry_ready` marker.
6. Directory-mode repair is complete on all 11 required dirs, and epoch 305 detached proof now returns HTTP 200 (`clean Post rebuild remains ready; scheduler remains paused`).

## Epoch 305 exact state

- Marker: schema 4, state `retry_ready`, SHA-256 prefix `7245158a…9e130`.
- Original target path is absent.
- Preserved quarantine:
  `/volume1/blockzilla/archive/.usage-sorted-generations/.epoch-305.registry-reprocess.profile-neutral-post-v1.fca639213c23226516d24b1f3fbb789af23793fc584f277721799e51217cc924.quarantine`
- Quarantine identity remains device `64256`, inode `3059580972`.
- Recovery receipt SHA-256 prefix: `a86bfc20…da8b7`.
- Archived old marker SHA-256 prefix: `ccd0aafa…c483`.
- The final detached proof now returns HTTP 200 with `clean Post rebuild remains ready`; no file-content error occurred.

## Immediate next actions

### 1. Repair only the 11 directory modes

Use a detached, guarded helper. Do not use a recursive command or a glob.

- Already done.
- Hold the exact Firewatch controller lock, all 11 registry locks, and all 11 source-PoH locks.
- Require the scheduler to remain paused and idle with registry concurrency `0`.
- Require no readers, writers, or workers.
- Validate every exact path, receipt, directory identity, and child identity before mutation.
- Change only these directory modes from `0777` to `0700`:
  - the epoch 305 quarantine above;
  - the exact live target directories for epochs `404, 405, 501, 502, 503, 504, 505, 864, 997, 1000` under
    `/volume1/blockzilla/archive/.usage-sorted-generations/epoch-E`.
- Do not change file modes or content.
- After chmod, require device, inode, size, UID, GID, link count, mtime, receipt hash, and all child identities to be unchanged. Only directory mode and ctime may change.

### 2. Complete epoch 305 proof

- Completed for epoch 305.
- Current status of the epoch 305 run:
  - unit: `blockzilla-profile-neutral-recovery-305-4ded-quiet1.service`
  - result: `/home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-recovery-305-4dedbdeb803889f3d4a4ebb6904ee9575.quiet1.result.jsonl`
  - response: HTTP 200, `action=rebuild_profile_neutral`, `snapshot_sequence=324`.
- Next action now is epochs `404,405,501,502,503,504,505,864,997,1000` with the same exact endpoint and authority data.

The NAS `curl` binary is unusable because a required library is missing. Use the already prepared bounded standard-library client:

- Path: `/home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-detached-client-af3623859c0a88b0dc6c56e372fda64228a25f16fc4e9d0b0c61eb820bbc1adb.py`
- SHA-256: `af3623859c0a88b0dc6c56e372fda64228a25f16fc4e9d0b0c61eb820bbc1adb`
- Mode: `0500`; UID/GID: `1000/10`; link count: `1`.

The epoch 305 unit name is `blockzilla-profile-neutral-recovery-305-4ded.service`. Its first result file is
`/home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-recovery-305-4dedbdeb803889f3d4a4ebb6904ee9575.result.jsonl`
with SHA-256 `b90cc8ba8f75ace93de20d268bd8efcd605335afe56fe71696ad5f1cc0ce9946`.

### 3. Preserve the other 10 legacy generations

While the scheduler remains paused and registry concurrency remains `0`, run one detached request at a time in this order:

`404, 405, 501, 502, 503, 504, 505, 864, 997, 1000`

Endpoint:

`POST /api/v1/jobs/archive_v2_registry_reprocess/{epoch}/rebuild-profile-neutral`

Exact request authority:

- Incident: `profile-neutral-registry-reprocess-post-rebuild-2026-08-14-v1`
- Authority SHA-256: `f471bb2078e719da508c4a8d22980a59e7d99140fe0682289bacb401ea10b5cf`

For each epoch, require HTTP 200, exact quarantine and old-marker archive, an exact recovery receipt, and a schema-4 Post `retry_ready` marker. Stop on the first mismatch.

### 4. Recover the 7 stale-PoH registry targets

Process the closed set serially:

`1001, 1002, 1003, 1004, 1005, 1006, 1008`

Use the reviewed registry retry endpoint while paused. Preserve each old target and marker. Do not delete a quarantine.

### 5. Run clean rebuilds

- After every required preservation transition is durable, set registry concurrency to exactly `1`.
- Keep one registry lane only.
- Resume the scheduler.
- Require each marker to move through the reviewed schema-4 states and finish with an exact v3 Post-profile receipt, attempt, staging, handoff, and final deep audit.
- Stop on a failed marker, second worker, identity change, reader, OOM/max event, or sustained I/O-full pressure at or above 40%.

### 6. Final certification

- Deep-validate source and target semantics for all 18 affected epochs.
- Require usage-sorted registry order and exact Firewatch acceptance bindings.
- Require no staging or temporary paths.
- Keep all old targets and old markers in deterministic quarantine/archive until separate cleanup approval.

## Important operating rules

- Do not grant the scheduler root or `CAP_SYS_PTRACE`.
- Do not run the recovery request from an SSH session. A private `session-*.scope` process correctly blocks the census.
- Use a detached reviewed `app.slice` one-shot and let the launch SSH session exit before validation ends.
- Do not retry a deterministic 409 without first reading and fixing its exact cause. Each retry rehashes the full target.
- Do not enable more than one rebuild lane.
- Do not delete or overwrite any source, target, quarantine, marker archive, receipt, or proof.

## ETA note

Do not reuse the earlier full ETA until the first clean rebuild completes. The remaining time is dominated by the 18 serial rebuild and deep-audit cycles. Recalculate from the first live rebuild rate.
