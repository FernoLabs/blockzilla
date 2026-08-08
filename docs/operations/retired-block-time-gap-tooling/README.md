# Retired: standalone block-time-gap backfill tooling

Retired 2026-08-06. Preserved here as reference for reimplementing this as
a real scheduler job kind (the same pattern the PoH signature-count
migration went through: standalone shell script → `ChildKind` integrated
into `blockzilla scheduler`), not as something to redeploy as-is.

## What this was

Two systemd user units on `Blockzilla-00`, both `disabled` (not running)
at time of retirement:

- **`blockzilla-gap-backfill.service`** ran [`run.sh`](run.sh) — a
  substantial (~850 line) bash supervisor that backfills `block-time-
  gaps.bin` sidecars into already-archived epochs. Worth reading before
  reimplementing: it already solves several of the same problems the real
  scheduler solves independently — pressure-aware admission (memory/IO
  PSI/load thresholds before starting new work), a singleton lock, a
  claim-directory mechanism so multiple workers can't double-process an
  epoch, exact process identity verification via `/proc/<pid>/exe` +
  start-tick comparison (the same technique `blockzilla/src/scheduler/
  mod.rs` uses for its own adoption/liveness checks), and receipt-based
  resumability. It also explicitly defers to live capture and to the
  archive finalizer (`FINALIZER_ACTIVE` check) — i.e. it already treats
  itself as strictly lower priority than the real pipeline, via `nice -n
  19` / `ionice -c2 -n 7` on its child extractor processes.
- **`blockzilla-watcher-block-time-gaps.service`** ran
  [`publish-block-time-gap-backfill.py`](publish-block-time-gap-backfill.py) —
  reads the above's `status.json` and republishes a bounded, secret-free
  public view of it for the retired watcher UI.

## Why retired rather than repaired

Both were already broken before this retirement, unrelated to the
decision to retire them: an earlier NAS cleanup pass deleted
`blockzilla-pipeline/state/block-time-gap-archive-v2-v1/` (the backfill's
own state/manifest directory) and
`blockzilla-pipeline/releases/blockzilla-nas-pipeline-2026.07.19-watcher-
recovery-3/` (the publisher's output directory), neither of which were
known to be referenced by anything at the time — they were only
discovered by reading these unit files after the fact. Since both were
`disabled` (not actively running), there was no live outage, but this is
exactly the failure mode standalone-script-plus-systemd-unit tooling
produces: dependencies invisible to anything except grepping unit files
by hand.

Given that, and that this functionality's actual job — walk already-
complete archive epochs, backfill/verify a sidecar, republish status — is
conceptually identical to what the scheduler already does for the PoH
migration, the decision was to not resurrect the standalone version but
fold this into `blockzilla scheduler` properly next: a new `ChildKind`
(mirroring `PohSignatureCountMigration`), a dedicated marker file (not
the shared archival ownership marker — see the marker-isolation writeup
in `nas-deployment-layout.md` for why that distinction matters), and
reusing the scheduler's own pressure/PSI gating instead of this script's
own hand-rolled reimplementation of it.

## Config referenced by the retired units (for context, not to restore)

- `run.sh` env vars: `BLOCKZILLA_BIN`, `STATE_DIR` (was `blockzilla-
  pipeline/state/block-time-gap-archive-v2-v1`, now deleted), `MANIFEST`,
  `PIPELINE_STATUS` (the scheduler's own `status.json`), `ARCHIVE_ROOT`,
  `WORKERS`, plus IO/pressure thresholds.
- The publisher's `--source`/`--output` pointed at that same state dir and
  at a now-deleted dated release directory respectively — the latter is
  the same "mutable output written into an immutable release directory"
  pattern flagged for `publish-runtime-operations` in
  `nas-deployment-layout.md`; don't repeat it when this gets rebuilt.
