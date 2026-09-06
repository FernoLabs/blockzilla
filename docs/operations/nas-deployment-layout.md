# NAS Deployment Layout

Current as of 2026-08-06 against `Blockzilla-00` (`<nas-lan-ip>`, Debian 12
bookworm, x86_64). Supersedes the ad hoc `nohup` + dated-directory
deployment history described in earlier revisions of this document.

## Canonical root: `/volume1/blockzilla/`

```
/volume1/blockzilla/
├── archive/            Archive V2 output, ~1036 epoch directories, ~90TB
│                        (moved from /volume1/@home/ach/dev/blockzilla-v2;
│                        same filesystem, so this was a fast rename, not a copy)
├── old-faithful/        raw source CAR/CAR.ZST files, genesis.tar.bz2,
│                        slot-index/, slot-index-v2/, .downloads/
│                        (moved from the old flat /volume1/blockzilla/*.car)
├── scheduler-state/     the scheduler's --state-root: status.json,
│                        control-state.json, progress/, logs/, etc.
│                        (moved from blockzilla-pipeline/state/nas-pipeline-v2)
├── live -> /volume1/@home/ach/dev/blockzilla-live
│                        left as a symlink deliberately -- the directory is
│                        empty (no live-capture data exists yet), and moving
│                        it would require updating two out-of-scope live-
│                        capture services (blockzilla-live-indexer.service,
│                        blockzilla-raw-live-fallback.service) for zero
│                        benefit. Revisit when live indexing is in scope.
├── bin/
│   ├── blockzilla                  (built from this repo, see DEPLOYED.md)
│   ├── blockzilla-watcher-gateway  (retired binary; no longer built)
│   └── blockzilla-monitor
├── config/
│   └── nas-pipeline-v2.env  sourced via EnvironmentFile= by
│                              blockzilla-archive.service; every BLOCKZILLA_*
│                              variable maps directly to a scheduler CLI flag
├── DEPLOYED.md            binary provenance -- update on every rebuild
├── systemd/                reference mirror of what's actually installed
│                            under ~/.config/systemd/user/ (source of truth
│                            is the deployed copy; this is for visibility/
│                            history, not itself consumed by systemd)
└── tools/
    └── ach-legacy/          old ~/ach/ directory contents (a gateway test
                              build, a benchmark, an old Python prototype of
                              the runtime-operations publisher) -- kept for
                              reference, not wired into anything
```

`/home/ach/blockzilla` and `/home/ach/ach` no longer exist -- everything
was moved (not symlinked) into the tree above, by explicit instruction.

## Config-driven scheduler flags

`blockzilla scheduler`'s CLI (`blockzilla/cli/src/scheduler/cli.rs`) now
accepts every non-boolean flag via a `BLOCKZILLA_*`-prefixed environment
variable (clap's native `env =` support -- `cargo add clap --features env`
equivalent: `features = ["derive", "env"]` in `blockzilla/cli/Cargo.toml`), at
lower precedence than the equivalent CLI flag. This is what lets
`blockzilla-archive.service`'s `ExecStart=` be just
`/volume1/blockzilla/bin/blockzilla scheduler --execute`, with every path
and tuning knob coming from `EnvironmentFile=/volume1/blockzilla/config/
nas-pipeline-v2.env`.

The four presence-only opt-in flags -- `--execute`, `--no-access`,
`--compact-auto-pause`, `--preflight-car` -- deliberately have **no** env
var. clap's env handling for a `SetTrue`-action flag treats the variable
merely being *set* (regardless of value) as enabling it, which would make
`--execute` silently activate at the next daemon-reload if a stale env
file were ever left with that variable in place -- exactly the "must not
activate itself without explicit operator opt-in" property this project
already applies to `--poh-migration-concurrency`. These four stay
CLI-flag-only.

Why a config file instead of baking `/volume1/blockzilla/...` as compiled
Rust defaults: every other path argument in this codebase (and every
argument in general) follows one consistent rule -- `default_value_t`/
`default_value` is used for portable settings (buffer sizes, thresholds,
enum choices), never for filesystem paths. Hardcoding this NAS's paths as
compiled defaults would make the exact same binary silently wrong if run
anywhere else (a laptop, CI, a different deployment). The env-var
mechanism keeps the binary itself hardware-agnostic; `config/nas-
pipeline-v2.env` is the one file that's specific to this machine.

## systemd units (all user-scope, `~/.config/systemd/user/`, lingering enabled)

| Unit | Binary | Status |
|---|---|---|
| `blockzilla-archive.service` | `/volume1/blockzilla/bin/blockzilla` | enabled, **running** |
| `blockzilla-monitor-public.service` | `/volume1/blockzilla/bin/blockzilla-monitor` | enabled, **running** (talks directly to the scheduler's `/api/v1/status` + `/api/v1/events`, no gateway hop) |
| `blockzilla-watcher-tunnel.service` | `/home/ach/.local/bin/cloudflared` | enabled, **running** -- ingress `watcher.blockzilla.dev` → `http://<nas-lan-ip>:8787` |
| `blockzilla-gateway-internal.service` | (retired) | **disabled**, kept as a reference file in `systemd/`, not deleted |
| `blockzilla-watcher-runtime-operations.service` | (retired) | **disabled**, kept as a reference file, not deleted -- see below |
| `blockzilla-live-indexer.service` | (live-capture, out of scope) | enabled, was already inactive |
| `blockzilla-raw-live-fallback.service` | (live-capture, out of scope) | disabled |

### Consolidation: 5 services down to 3 (done)

Investigated and confirmed rather than assumed:

- **`gateway-internal` retired.** Its `serve` subcommand did real work --
  secret/path redaction, a hard endpoint allowlist, fail-closed error
  handling, connection/CPU-transform rate limiting -- but `blockzilla-
  monitor` already has an equivalent-or-stronger security model of its
  own: it never proxies raw upstream JSON, only ever serves a curated,
  explicitly-named signal map built field-by-field from `DashboardState`
  (`blockzilla/monitor/src/api/stream.rs`, `state.rs`'s
  `to_signals()`). A new field on the wire literally cannot leak through
  monitor until someone explicitly adds it to that map -- safer by
  construction than gateway's generic strip-known-fields redaction, which
  has to be updated in lockstep with the wire schema to stay correct.
  Confirmed monitor has zero write/mutation capability (grepped the whole
  crate for POST/mutating routes -- the only match was generic framework
  code in the vendored `datastar.js`, never wired to an actual route), so
  the "read-only, sole public surface" property monitor needs already
  holds. Confirmed monitor doesn't consume gateway's separate
  `ingest-upstream` (port 8790) proxying either, so nothing was lost.
  `blockzilla-monitor-public.service` now runs `--upstream
  http://127.0.0.1:8786` (the scheduler directly) instead of `:8793`
  (the old gateway). Verified live: `archive_complete: 1001, archive_pct:
  99.0, connection_state: "live"` rendering correctly with the gateway
  process not running at all.
- **`watcher-runtime-operations` is NOT redundant with the scheduler**,
  so it does not fold into `blockzilla-archive.service`. It deliberately
  tracks the complement of what the scheduler manages: hivezilla's live
  capture (a wholly separate process), manually-run checksum/download
  processes the scheduler never spawned, and non-blockzilla I/O
  competing for resources -- explicitly filtering out anything
  scheduler-owned to avoid double-reporting
  (`is_scheduler_managed_aria_child`, `is_blockzilla_owned` in
  `blockzilla/monitor/src/process_telemetry.rs`). The
  scheduler's job is orchestrating its own children; this is system-wide
  observability, which belongs with monitor (the other observability
  process), not with the orchestrator.
- **2026-08-06: ported into monitor, service retired.** Rather than
  re-implementing the procfs scanner, the audited implementation was moved
  directly into `blockzilla-monitor`. The monitor calls `collect_processes`,
  `process_io_status`,
  `ProcessSample`/`ProcessCollection`/`ProcessIoStatus`/`ProcessIoEntry`
  in `runtime_operations.rs` were made `pub` (no behavior change -- same
  functions, with their existing tests), and
  a new `blockzilla/monitor/src/runtime_operations.rs` (~90
  lines) calls them directly on its own 5s tokio task -- no JSON file, no
  HTTP hop. Only the `process_io` portion of the old sidecar schema was
  ported -- `jobs`/`live_capture` (aria2c/sha256sum/hivezilla-capture
  tracking) existed only for the now-deleted `web/blockzilla-watcher`
  Svelte frontend and were never read by this dashboard's `snapshot.rs`.
  `state.rs` gained `set_local_process_io` plus a `last_snapshot` cache so
  the independently-ticking sampler can merge into and republish the
  dashboard without waiting for the next upstream snapshot/patch (see
  `recompute_and_publish`); `set_offline` clears `last_snapshot` so a
  process-I/O tick landing while the upstream connection is down can't
  resurrect a stale "live" dashboard. Deployed and restarted on the NAS;
  `blockzilla-watcher-runtime-operations.service` is now **disabled**
  (unit file kept as reference, same treatment as `gateway-internal`).
- **2026-08-06: full clean restart verified end-to-end, tunnel now live.**
  Stopped archive + monitor, confirmed zero blockzilla/cloudflared
  processes left running, then started archive → monitor → tunnel in
  order, checking health after each. Scheduler status API
  (`127.0.0.1:8786/api/v1/status`) responding; monitor rendering live
  data both on the LAN IP and, for the first time, through the actual
  public hostname `https://watcher.blockzilla.dev` (confirmed via browser
  fetch, not just curl-from-the-NAS) -- same `1001/1011 epochs, 99%`
  numbers on both paths. Final process list: exactly 3 processes
  (`blockzilla scheduler`, `blockzilla-monitor`, `cloudflared`) matching
  exactly 3 active systemd units -- nothing orphaned, nothing duplicated.
  `blockzilla-watcher-runtime-operations.service` deliberately left
  stopped at this point: since `gateway-internal`'s retirement, nothing
  HTTP-serves its output sidecar
  (`/api/v1/sidecars/runtime-operations/status.json`) anymore, so
  restarting it as-is would only write a file nobody reads. Superseded
  minutes later by the in-monitor port above.
- **2026-08-06: found and fixed `BLOCKZILLA_POH_MIGRATION_CONCURRENCY=0`
  in the deployed config.** Set to `0` during Stage 0 of the PoH migration
  work (deliberately, to keep the not-yet-deployed scheduler from
  double-running migrations while the manual shell scripts were still
  being wound down) and never turned back on after the scheduler-managed
  migration shipped -- so after this session's restart, the scheduler was
  healthy but silently doing zero of the remaining 577 migration epochs
  (`poh_migration_capacity_configured: 0`, confirmed via
  `127.0.0.1:8786/api/v1/status`). `legacy_compact` was unaffected
  (`BLOCKZILLA_COMPACT_CONCURRENCY=6`, a real nonzero value already) --
  its own idle `lanes: []` at the same moment is unrelated and expected:
  the only two non-complete, non-blocked epochs (1007, 1010) are near the
  live chain edge with nothing yet to scan. Bumped
  `BLOCKZILLA_POH_MIGRATION_CONCURRENCY` to `2` (the value this project's
  plan already documented as the safe default) in
  `config/nas-pipeline-v2.env` and restarted `blockzilla-archive.service`
  to pick it up (`EnvironmentFile=` is read at process start only, not
  live-reloaded). Confirmed within one poll cycle: two `poh_migration:*`
  lanes running (epochs 426, 427), load 1m 0.37, IO PSI avg10 1.22% (well
  under the 40% pause threshold) -- no repeat of the RAID-saturation
  incident that originally motivated this concurrency knob.
- **Future, not now**: monitor will also gain a Unix domain socket to a
  live-indexer service, once live indexing itself is in scope (explicitly
  deferred -- "next chapter").

## PoH migration: stale-binary incident, backlog clear, byte-based progress (2026-08-06/07)

Sequence of findings after the restart-verification above, in the order
they surfaced:

1. **`BLOCKZILLA_POH_MIGRATION_CONCURRENCY=0`** (documented above) was
   fixed to `2`, then confirmed via `lanes: [poh_migration:426,
   poh_migration:427]` -- looked like it was working.
2. **It wasn't.** Bumped to `6` on the (mistaken) belief that near-zero
   load meant headroom to spare. Within the next poll, dozens of epochs
   cycled through admission and instant-failure in seconds --
   `poh_migration:721 exited with failure but filesystem validation
   failed`. The actual epoch-721 log
   (`scheduler-state/logs/epoch-721-poh-migration.log`) read `error:
   unrecognized subcommand 'migrate-poh-signature-counts'`.
   `BLOCKZILLA_BIN` in `config/nas-pipeline-v2.env` pointed at
   `/volume1/@home/ach/dev/blockzilla-pipeline/releases/blockzilla-nas-
   pipeline-2026.07.13-resource-scheduled-reuse-ui-3/bin/blockzilla` -- a
   pre-existing, much older release that predates this feature entirely.
   `blockzilla_bin` (`scheduler/cli.rs`/`mod.rs`) is the single binary the
   scheduler spawns *every* child job through, not a legacy/repair-only
   path (that's the separate `BLOCKZILLA_REPAIR_BIN`, left untouched) --
   scan/compact worked fine throughout because those subcommands already
   existed in the July build; only the brand-new migration subcommand was
   missing. Fixed by pointing `BLOCKZILLA_BIN` at
   `/volume1/blockzilla/bin/blockzilla` (the same binary the scheduler
   process itself runs as). This means every earlier "PoH migration is
   running" report in this document from before the fix was wrong --
   `lanes[].state == "running"` only means a child was spawned, not that
   it's making progress; the tell that was missed is that
   `poh_migration_epochs_complete` never advanced across two separate
   checks.
3. **Fallout: 388 epochs marked `failed`, 8 more stuck `running` with
   dead pids.** The failure storm touched far more epochs than expected
   before the fix landed. `spawn_poh_migration`'s duplicate-writer guard
   (by design, see the plan) refuses to admit over any marker not in
   `retry_ready` state -- including a stale `failed` or orphaned
   `running` marker -- so none of these 396 epochs would ever be retried
   automatically. Resolution: confirmed each `running` marker's recorded
   pid was actually dead (`kill -0`, all 8 were; the other 6 concurrently
   `running` markers had live pids and were left alone) before deleting
   those 8 marker files directly (safe: the migration is whole-epoch
   atomic with no partial checkpoint, so a deleted marker for a
   confirmed-dead process just looks like "never attempted"). The 388
   `failed` markers were cleared through the scheduler's own management
   API, `POST http://127.0.0.1:8788/api/v1/jobs/
   poh_signature_count_migration/{epoch}/retry` (requires
   `Content-Type: application/json` -- `enforce_management_request` 403s
   without it; `curl` is broken on this NAS host, missing
   `libquiche.so.0`, so `wget --method=POST --header=...` was used
   instead) -- 387/387 succeeded (the 388th had already been used as a
   manual test of the endpoint).
4. **The "room for more workers" read was itself a symptom of #2**, not
   real headroom: load/IO pressure were near-zero because the workers
   were instant-failing, doing no real I/O at all. Once real migrations
   were running (post-fix), IO pressure full avg10 sat around 50-65%,
   well above the shared 40% pause threshold -- confirmed the
   admission-side PSI gate is doing its job (`poh_migration_running`
   briefly read `5` instead of the configured `6` as a slot freed up
   under sustained pressure, i.e. top-up correctly declined to refill it
   immediately). Concurrency was left at `6` rather than dialed back --
   already-admitted jobs run to completion regardless of pressure either
   way, and the pause gate caps further growth.
5. **Progress metric changed from epoch count to bytes.** An epoch-count
   "N of M complete" bar implies every epoch is equal-sized work, which
   isn't true here -- cost tracks PoH sidecar bytes actually
   read/patched. `ArtifactSnapshot.bytes` (`file_len()` on the sidecar)
   was already being computed once per epoch per poll for the existing
   schema-state classification, so summing it instead of counting cost
   nothing extra. `PipelineSummary.poh_migration_epochs_total/complete`
   (`usize`) became `poh_migration_bytes_total/done` (`u64`); monitor's
   `poh_migration_progress` component now renders `state
   .poh_migration_bytes_label()` ("2.3 TiB / 6.0 TiB processed") instead
   of an epoch count, reusing a `format_bytes` moved from
   `components.rs` into `state.rs` (matching how `format_thousands`/
   `format_duration` are already shared between the two). Both binaries
   must deploy together -- the old scheduler binary doesn't emit the new
   field names, so an old-scheduler/new-monitor mismatch reads
   `0 B / 0 B processed` until they match (`#[serde(default)]` tolerates
   the missing fields rather than erroring, but the display is
   misleading in between). Deliberately held the restart until an
   in-flight batch mostly drained (6 concurrent migrations, one already
   at 100%) rather than discarding real progress for a display-only
   change -- see `DEPLOYED.md` for the final deploy.
6. **Individual migration workers were still invisible after #5.** The
   aggregate byte bar and the generic `tasks_active` count gave no way to
   see *which* epochs were actually in flight -- migrating epochs stay
   `HistoricalState::Complete` (the migration is a post-hoc sidecar
   backfill, not part of building the archive), so they never appear in
   `DashboardState.epochs` (filtered to non-complete) or the `/epochs`
   page either. Added `DashboardState.poh_migration_lanes: Vec<EpochTask>`
   -- built from `snapshot.lanes` filtered to
   `kind == "poh_signature_count_migration"`, reusing the existing
   `EpochTask` shape and `epoch_row` component verbatim (the two lists
   can never share an epoch number, since an epoch is either still
   building or already archive-complete, never both, so both safely emit
   the same `epoch_{N}_pct`/`_blocks`/`_eta` live-update signals). New
   "PoH migration workers" panel on the Overview page, right below the
   aggregate bar; monitor-only change, no scheduler binary involved, no
   in-flight migration cost to deploying it.

## PoH migration: global ETA, epoch-count display, decimal precision (2026-08-07)

Follow-up round after the incident above, driven by dashboard feedback
("missing global ETA", "show epoch done/remaining not data size", "cap %
decimal", "is worker count dynamic").

- **Global ETA.** New `poh_migration_bytes_per_sec(epochs, lanes)` in
  `scheduler/mod.rs` (next to `estimate_runnable_queue_eta`/
  `active_block_processing_rate`, same file-local pattern): each running
  migration lane's own `blocks_per_sec` (already computed generically by
  `ProgressTracker`, see `main.rs`) scaled by that epoch's PoH sidecar
  bytes-per-block, summed across lanes. Deliberately not a cross-poll
  rolling average -- `poh_migration_bytes_done` only moves in whole-epoch
  jumps at completion (no partial-epoch checkpoint), so a poll-to-poll
  delta would be far too lumpy to rate from directly.
  `PipelineSummary.poh_migration_eta_secs` = remaining bytes / that rate.
- **Epoch counts are the primary label again, bytes are subtext.** Bytes
  still drive the percentage (epochs aren't equal-sized work -- this is
  why bytes replaced epoch counts as the *computation* basis in the prior
  round), but "N done, M remaining" is what's scannable at a glance;
  bytes moved to a small `text-xs` line underneath (`poh_migration_bytes_
  label`) and a `title=` hover on the primary count. Both
  `poh_migration_epochs_total`/`_done` (counts) and
  `poh_migration_bytes_total`/`_done` (bytes, for %) now ship on the wire
  side by side.
- **Decimal precision.** `archive_pct`/`poh_migration_pct`/`load_1m` were
  sent into the Datastar signal map as raw rounded floats
  (`round1(...).into()`). Rounding the *value* to one decimal isn't
  enough: 38.4 has no exact binary representation, so once Datastar reads
  a raw f32/f64 JSON number back as a JS number for a live `data-text`/
  `data-attr:style` patch, it re-expands to something like
  `38.400001525878906`. The initial server-rendered HTML was always fine
  (already a pre-formatted string); only values patched live over SSE
  showed the artifact. Fixed by sending pre-formatted 1-decimal strings
  (`format_pct`) instead -- safe for both the `data-text` and
  `data-attr:style` usages, since template-literal interpolation doesn't
  care whether the signal is a string or number. Separately,
  `poh_migration_pct()`'s own computation was fixed to divide in `f64`
  before narrowing to `f32`: at this job's actual TB-scale byte counts
  (~10^12), casting the raw `u64` operands straight to `f32` first loses
  real precision (`f32` carries ~7 significant digits) -- narrowing only
  the final 0-100 result is lossless.
- **Worker count: fixed ceiling, not dynamic -- and live proof the
  pause/resume gate works.** Per the original plan (decision #2), this
  was deliberately built as a small fixed ceiling gated by the shared PSI
  pause/resume signal, not a self-tuning pool like
  `LegacyThroughputTuner` (that tuner's probe/settle timing and MiB/s
  signal don't fit this job's mostly-no-op-skip-path profile). Watched
  live: at ceiling 6, `poh_migration_running` sat at `5` for a stretch
  under sustained ~50% IO PSI (above the 40% pause threshold) instead of
  immediately refilling the freed slot, then dropped to `2` as pressure
  climbed further, before both trending back down together as pressure
  eased -- the admission-side gate visibly self-limits without needing to
  raise the ceiling itself. Raising the ceiling further wasn't done this
  round; the plan's already-deferred "Stage 3: capacity refinement" (a
  simple up/down counter driven only by the pause/resume booleans, no
  throughput sampling) is the documented next step if the fixed ceiling
  proves mistuned in practice.
- **Incident, disclosed:** mid-investigation, `git stash` (no path scope)
  was run to isolate one file for a before/after test and instead reverted
  *every* uncommitted tracked-file change across the whole repo --
  including substantial pre-existing uncommitted work unrelated to this
  session (a large `web/` → `workers/` reorg, multiple crate-level
  changes). Caught immediately via the unexpected diff in a system
  reminder; `Cargo.lock`'s conflicting local diff (safely regeneratable)
  was reset to `HEAD` and `git stash pop` cleanly restored everything
  else. Verified every one of this session's edits was intact afterward
  (grepped for specific added functions/fields in each touched file) before
  continuing. Lesson: always path-scope `git stash` (`git stash push --
  <path>`) rather than a bare `git stash`, even for a "just this one file"
  test.
- **Found, not fixed (out of scope):** `cargo test -p blockzilla
  cli_tests::acquire_car_cli_expected_bytes_is_optional_but_must_be_
  positive` stack-overflows deterministically, reproducing even in
  isolation with `--test-threads=1`. Confirmed pre-existing and unrelated
  to this session's changes (passes cleanly against a stash of just this
  session's diff, i.e. the last-committed `main`) -- caused by something
  else in the large pre-existing uncommitted tree, not investigated
  further since it's outside this session's scope.

## Retired: standalone block-time-gap backfill tooling

`blockzilla-gap-backfill.service` and `blockzilla-watcher-block-time-
gaps.service` (both already `disabled`, and both already broken by an
earlier cleanup pass deleting their state/output directories before their
existence was known) were retired rather than repaired. Their unit files
and scripts are preserved at `docs/operations/retired-block-time-gap-
tooling/` for reference -- the plan is to fold this functionality into
`blockzilla scheduler` as a real `ChildKind` (mirroring how the PoH
signature-count migration went from a standalone script to a scheduler
job kind), not to resurrect the standalone version.

## Protected, never touched by any of this

- `/home/ach/backups/` -- personal data, unrelated to blockzilla entirely.
- `/home/ach/stargazer-deploy/` -- separate, unrelated project.

## Remaining work

1. ~~Restart everything under systemd, verifying health after each~~ --
   done 2026-08-06, see the consolidation section above.
2. **git access on the NAS**: a deploy key was generated
   (`~/.ssh/blockzilla_deploy_key` on the NAS) but not yet added to
   `FernoLabs/blockzilla`'s deploy keys on GitHub -- blocking. Once added,
   `git clone git@github.com-blockzilla:FernoLabs/blockzilla.git` closes
   the "unknown exact commit" gap in `DEPLOYED.md` for the scheduler and
   monitor binaries, and enables building from a tagged commit on the NAS
   itself with `cargo deb` (discussed, not yet started -- see the
   packaging discussion in this session for the FHS-vs-custom-tree
   tradeoffs, tabled in favor of the `/volume1/blockzilla` tree for now).
3. **`~/dev/` workspace sprawl** (~30 dated feature-experiment
   directories) is unaudited. One of them,
   `blockzilla-v1-registry-mphf-20260616`, is confirmed load-bearing (it's
   `blockzilla-live-indexer.service`'s `WorkingDirectory`) -- do not
   archive or delete anything under `~/dev/` without first grepping
   `~/.config/systemd/user/*.service` for references, the way this
   session should have from the start.
