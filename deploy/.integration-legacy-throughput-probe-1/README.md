# Legacy compact aggregate-throughput probe (not deployed)

This integration copy replaces IO-PSI-only lane saturation decisions with a causal A/B/A probe.

## Why

The measured controller experiment contradicted the old assumption:

- one lane: aggregate useful throughput about 92 blocks/s, IO PSI `full avg10` about 18-21;
- two lanes: aggregate useful throughput about 143 blocks/s, IO PSI `full avg10` about 15;
- physical disks remained around 25% utilized.

Linux IO PSI is system-wide stall telemetry. It does not identify which workload caused the stall and, in this experiment, moved in the opposite direction from useful throughput. It remains in the API for observability and command-line compatibility, but no longer pauses or resumes compact lanes by itself.

## Controller

1. Hold a stable lane set for `--legacy-compact-throughput-probe-window-secs` (default 120 seconds) and calculate aggregate useful throughput from `blocks_done` deltas. The controller deliberately does not compare lifetime `blocks_per_sec` values.
2. Record aggregate physical reads from `/proc/<pid>/io` `read_bytes` over the same window as corroborating telemetry. Physical reads are not the control objective because page cache and block-size mix can change them.
3. Start exactly one additional managed lane. This is sample B.
4. Keep it immediately when aggregate useful throughput improves by at least `--legacy-compact-throughput-min-gain-pct` (default 5%). The measured 92 -> 143 blocks/s case is therefore accepted.
5. If B misses the gain floor, `SIGSTOP` only the added managed process group and measure a second A window.
6. Confirm a ceiling only when stopping recovers at least the same percentage versus B and the second A remains within that percentage of the original A. If stopping does not help, resume the lane.
7. Keep a confirmed-saturated lane paused for `--legacy-compact-throughput-probe-backoff-secs` (default 900 seconds), then re-probe. Resume earlier when the comparison lane set changes or running lanes fall below the configured minimum.
8. Even after ramp-up, periodically audit every stable configuration above the minimum: measure B at N lanes, stop one managed lane and measure A at N-1, then resume and measure B2 at N. Keep the lane paused only when the stop benefit repeats across B/A/B. This catches later workload or external-IO changes even when the hard capacity is not full or no fourth epoch is currently admissible.
9. Do not begin a throughput audit while the scheduler is draining legacy work for an admissible live finalizer, acquisition, or scan. Once memory has recovered, resume a throughput-paused legacy lane so it can finish and release its ownership; manual pauses and memory pauses retain their existing ownership rules.

The decision uses aggregate throughput, not average throughput per worker. Per-worker speed normally falls as workers share a disk even while total completed work rises; the observed 92 blocks/s with one lane versus 143 blocks/s with two lanes is exactly that case.

Hard memory admission and MemAvailable hysteresis remain authoritative. Memory pressure resets the throughput sample window so a period containing a memory pause cannot contaminate A, B, or A2. Existing CPU, memory, disk-space, and configured hard lane caps remain unchanged.

## API telemetry

`summary` adds the current probe state (`measuring_baseline`, `baseline_ready`, `trial_b`, `confirm_a2`, `audit_n_minus_1`, `audit_n_recheck`, or `ceiling_confirmed`), window/backoff settings, aggregate useful blocks/s, aggregate physical read MiB/s, A/B/A rates, next audit time, and retry time.

## Verification

- Seven focused adaptive-scheduler tests pass, including the observed 92 -> 143 blocks/s case, a confirmed stop recovery, a false candidate where stopping does not recover throughput, a retry that refuses a stale pre-backoff baseline, a steady-state B/A/B audit, memory-interruption ownership cleanup, and priority-drain selection. Existing acquisition and live-drain scheduling tests also pass with explicit drain-detection assertions.
- `cargo check -p blockzilla-hivezilla --all-targets` passes in an isolated worktree using the frozen integration overlay and its existing Cargo/lib prerequisites.
- On macOS, the full candidate suite reports 82 passes and the same four `/proc`-dependent failures as the frozen baseline (75 passes and the identical four failures). Those tests require Linux; the new tests themselves do not require `/proc` read telemetry.

## Deployment risks and gate

- This is an integration candidate only. It has not touched the NAS controller, workers, or release tree.
- The currently deployed dashboard still labels the legacy PSI thresholds as pause/resume controls. Update that copy to show PSI as telemetry and the new A/B/A state before promoting this controller; otherwise the UI would describe behavior that no longer exists.
- Run the full suite on Linux before deployment.
- Canary with a 120-second window, 5% gain floor, 15-minute retry, hard capacity 4, and minimum running 2. The effective CPU/IO admission capacity must also be 4 (for the current 120 MiB/s per-lane projection, the IO budget must be at least 480 MiB/s); the throughput controller never bypasses a hard resource cap.
- Verify progress files update at least once per scheduler poll window and that `/proc/<pid>/io` is readable by the controller user. Missing physical-read telemetry is safe; missing or stale progress prevents a probe.
- A paused process keeps its RSS and open files. Memory admission continues to reserve that RSS; do not treat `SIGSTOP` as memory release.
