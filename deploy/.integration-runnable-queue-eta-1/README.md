# Runnable queue ETA

Date: 2026-07-13

## Problem

The deployed `summary.eta_secs` used the ordinary scan capacity and added whole queue waves after
the slowest active job. Production was actually running three legacy compact/reuse workers, while
the estimate used a capacity of two. The dashboard also could not distinguish time to drain
scheduler-manageable work from full archive completion.

## Contract

The status API now exposes an explicit runnable historical queue estimate:

- `queue_eta_secs`: wall-clock time until active and queued historical worker jobs drain.
- `queue_eta_reason`: inputs and exclusions behind the current value.
- `queue_jobs_remaining` and `queue_capacity`: current modeled work and advancing workers.
- `queue_job_duration_secs` and `queue_duration_samples`: observed duration model and evidence.
- `eta_secs`: compatibility alias for `queue_eta_secs`.

Historical `blocked` and `failed` epochs are action-required work and are deliberately excluded.
They remain visible in the dashboard and continue to block any claim about full archive
completion. The main source keeps `archive_eta_secs` and `archive_eta_reason` as a separate,
stricter contract.

## Estimator

- Use exact elapsed time from the 32 most recent pipeline-owned successful jobs, bounded to 60
  seconds through seven days.
- Add mature active projections (`elapsed + remaining`) and choose the slower of the completed and
  active medians so current slowdown cannot make the forecast more optimistic.
- Use the number of currently advancing historical workers; use admitted capacity only before a
  queue has active lanes.
- Seed each active lane with its remaining time, then assign each queued job to the lane that frees
  first. This models work-conserving admission and keeps indivisible job durations.
- Return an unavailable reason when the scheduler is paused, no worker is advancing, or no timing
  model exists. An empty runnable queue returns zero even when action-required items remain.

The refresh path first filters in-memory progress and only opens ownership markers for plausible
recent samples. It does not load archive registries or accumulate an unbounded history.

## Dashboard

The top bar now says **Runnable queue ETA**, shows the projected queue drain clock, and displays
action-required items separately as outside the ETA. Full archive blocked/unavailable messaging
remains below the summary instead of being conflated with queue drain time. Older services fall
back to their scan/legacy ETA field.

## Validation

- Local Hivezilla suite: 53 passed, 0 failed.
- NAS/Linux integrated Hivezilla suite: 84 passed, 0 failed.
- Svelte autofixer: no issues; `svelte-check`: 0 errors and 0 warnings; static build passed.
- Isolated real-data smoke: 530 jobs, 3 workers, 7 samples, 2 historical action-required epochs
  excluded, and 1,262,568 seconds (about 14.6 days).
- Production API after rollout exposed the additive fields and the new UI bundle served the
  `Runnable queue ETA` label.
- Live producer PID 12591 and compact worker PIDs 217921, 284509, and 348612 retained their original
  process start ticks across the controller-only swap.

## NAS rollout

- Release:
  `/volume1/@home/ach/dev/blockzilla-pipeline/releases/blockzilla-nas-pipeline-2026.07.13-runnable-queue-eta-1`
- Hivezilla SHA-256:
  `9e3fb65a61c2fcf93533ac18c40617afcfb1f981c3224021c64f0aa8f134aaab`
- Integrated source SHA-256:
  `0d122e01bd4a3d797fb92fb2153bb1723b92a78ed67ccae8dc148fabbb7dfccf`
- Controller after rollout: PID 368739, bound to `0.0.0.0:8787`.
- Rollback release retained:
  `blockzilla-nas-pipeline-2026.07.13-live-status-monotonic-2`.
