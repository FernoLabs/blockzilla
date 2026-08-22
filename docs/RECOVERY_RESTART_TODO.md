# Blockzilla recovery resume status (2026-08-15 23:00+ CEST)

Current safe fact list (verified live at `now_unix_secs` 1786791762):

- NAS is reachable at `ssh ach@192.168.1.46 -p 22`.
- Scheduler is running with new binary `4dedbdeb...` and paused.
- No active recovery worker is running now.
- `systemctl --user list-units 'blockzilla-profile-neutral-*' --all` shows only terminal units.
- `/api/v1/status`:
  - `registry_reprocess_capacity_configured: 0`
  - `registry_reprocess_running: 0`
  - `registry_reprocess_admission_blocked_reason: "registry_reprocess:303 complete marker has no valid legacy or current binding"`
  - `observer_mode: false`
- Marker directory `/volume1/blockzilla/scheduler-state/registry_reprocess_profile_neutral_rebuild/markers` currently contains:
  - `305` state=`auditing`
  - `404` state=`auditing`
  - `405` state=`auditing`
  - `501` state=`auditing`
  - `502` state=`auditing`
  - `audit_retry_is_safe: false` for `502`.
- Last result file:
  - `/home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-recovery-502-4dedbdeb803889f3d4a4ebb6904ee9575.1786790068.result.jsonl`
  - contains HTTP `409` and message:
    `profile-neutral registry recovery remains blocked for epoch 502: preserved registry generation reader process table is unobservable`
- There is no terminal result yet for `503`, `504`, `505`, `864`, `997`, or `1000`.

Important probe:

Current same-UID process scan found readable/unreadable entries in same `/proc` namespace:
- Allowed init scope examples: `systemd --user` and `sd-pam` (init scope) still show unreadable `/proc/*`.
- Additional readable same-UID process scope is present:
  - `blockzilla-archive.service`, `blockzilla-monitor-public.service`, `stargazer-read-api`, `stargazer-frontend`.
- No active SSH session-scope readers with permission errors were seen after the last short verification.

Next step plan:

1) Confirm one operator action is approved to start epoch `503` again under strict rules:
   - single detached unit only
   - no parallel recovery units
   - no live SSH polling from another session during first 20-second client warm-up.
2) Command template (exact):

`systemd-run --user --unit=blockzilla-profile-neutral-recovery-503-4ded-manual2 --slice=app.slice --description="Resume profile-neutral epoch503 recovery" --property=Type=exec --property=Restart=no --property=KillMode=control-group --property=TimeoutStopSec=20s --property=UMask=0077 --property=NoNewPrivileges=yes --property=PrivateTmp=yes --property=MemoryMax=268435456 --property=TasksMax=32 --property=OOMPolicy=stop --property=OOMScoreAdjust=200 -- /home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-detached-client-af3623859c0a88b0dc6c56e372fda64228a25f16fc4e9d0b0c61eb820bbc1adb.py /home/ach/.config/systemd/user/blockzilla-archive-secure/profile-neutral-recovery-503-4ded-manual2.result.jsonl 503`

3) If this returns HTTP 200, continue in order:
   `504 -> 505 -> 864 -> 997 -> 1000`, one epoch at a time.
