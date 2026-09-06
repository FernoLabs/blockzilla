# Blockzilla Monitor

A server-rendered task/health dashboard for Blockzilla, built on
[Topcoat](https://github.com/tokio-rs/topcoat) with live updates pushed
over Server-Sent Events via `topcoat::datastar`.

The monitor uses Topcoat 0.7 and the workspace Rust 1.98 minimum. Views resolve
to a complete snapshot before each Datastar patch is sent. The SSE connection
still owns its capacity permit until its response body is dropped.

It reads the scheduler's private, read-only status API directly. The monitor
validates that wire data, maps it into an explicitly curated view model, and
applies public-tier path and credential redaction before serving browsers.

## Running it

Start the scheduler's status listener, then:

```
cargo run -p blockzilla-monitor -- --upstream http://127.0.0.1:8787
```

`--upstream` defaults to `http://127.0.0.1:8787` and can also be set via
`BLOCKZILLA_MONITOR_UPSTREAM`. `HOST`/`PORT` control this app's own bind
address (default `127.0.0.1:3000`).

`--firewatch-status-file <path>` (or
`BLOCKZILLA_MONITOR_FIREWATCH_STATUS_FILE`) overlays schema-1 status from the
local Firewatch controller on each accepted scheduler publication. It changes
only the Firewatch summary and `firewatch_index` rows. The file is limited to
4 MiB and must be a stable regular file, not a symlink. A missing, stale, or
malformed configured file leaves the main monitor live but shows Firewatch as
blocked and removes untrusted Firewatch rows. Without this option, raw
scheduler Firewatch values are preserved.

The current controller can report `epochs_blocked_wire_profile` and rows in
the `profile_audit_required` / `wire_profile_audit` state. When this field is
present, the monitor requires the eligible, registry-blocked, and
profile-audit counts to equal the full archive scope. Older schema-1 status
files can omit this additive field and keep the prior coverage rules.

The Firewatch summary shows accepted, active, queued, failed, and
profile-audit counts for all reported rows. The table is a bounded priority
sample. Its runnable-work ETA includes only active and queued work. Failed
epochs and profile-audit work are not part of that ETA.

`--tier public|full` (default `public`) controls how much of a real
snapshot reaches the page: `public` drops the process table entirely and
scrubs free-text fields (error messages, lane/workflow identifiers) for
paths and credentials as defense-in-depth; `full` shows everything. This
binary has no authentication of its own, so a `full` instance must only be
reachable from a network-gated path (Cloudflare Access, Tailscale, an IP
allowlist) -- see `docs/operations/blockzilla-monitor-roadmap.md` §3 for
the intended two-instance deployment shape. `--max-stream-connections`
(default `512`) caps concurrent `/api/stream` subscribers for the full
lifetime of each SSE response body. Zero is rejected. A tab that reaches
the cap receives `503 Service Unavailable` and retries with backoff.

If the scheduler isn't reachable, every page shows an honest "Scheduler
unavailable" screen with the real connection error. If a previously healthy
stream stops delivering bytes or valid scheduler state for 45 seconds, the
page becomes "Scheduler telemetry stale", removes the old task numbers, and
shows when the last accepted update arrived. This dashboard never substitutes
fabricated or default-zero numbers for a missing snapshot. Offline and stale
screens still own the normal live stream, so they recover in place when the
next valid snapshot arrives instead of requiring a reload.

For local UI iteration without a scheduler running:

```
cargo run -p blockzilla-monitor -- --demo
```

`--demo` drives the same views from a synthetic in-process ticker instead.
The header always shows a `demo data` badge in this mode so it can't be
mistaken for live telemetry.

## Layout

```
src/
  main.rs         entrypoint: parses --upstream/--demo, starts the
                   client (or demo simulator), starts the router
  app.rs           root <html> layout + the five pages (overview, history,
                   system, epochs, calendar)
  components.rs    view components (header, stat cards, epoch rows,
                   expandable system/error/process panels)
  calendar.rs      pure data/computation for the /calendar page: merges the
                   bundled reference calendar with any live-authoritative
                   dates, extrapolates a tail estimate, buckets epochs into
                   day-by-day year grids, and overlays the block-time-gap
                   sidecar as an "outage" marker. Faithful port of
                   web/blockzilla-watcher/src/lib/{epoch-calendar,
                   epoch-year-calendar}.ts -- see that module's doc comment
                   for the two deliberate simplifications
  calendar_view.rs rendering for calendar.rs's output (the actual grid
                   markup) -- kept separate from calendar.rs so the date
                   math stays independently testable without a view! macro
                   in the loop
  snapshot.rs      fail-closed schema-v3 Rust types and invariant/collection
                   validation for the real PipelineSnapshot wire served by
                   the scheduler at
                   /api/v1/status and /api/v1/events (mirrors
                   web/blockzilla-watcher/src/lib/pipeline-snapshot.ts),
                   plus the patch type and incremental apply_patch/
                   sequence_action logic (mirrors snapshot-patch.ts). Unknown
                   additive fields remain accepted
  client.rs        background task: bootstraps from GET /api/v1/status,
                   then applies incremental snapshot_patch events from SSE
                   /api/v1/events to an in-memory snapshot, with bounded
                   bodies, freshness watchdogs, and validated resync before
                   publication into state.rs
  state.rs         DashboardState model (the flat signal-map serializer),
                   the real-snapshot -> DashboardState mapping, and the
                   --demo ticker
  api/stream.rs    GET /api/stream -- this app's own SSE endpoint that
                   Datastar subscribes to on every page. A body-owned
                   semaphore permit caps concurrent subscribers for their
                   actual connection lifetime (--max-stream-connections)
  api/styles.rs    GET /app.css -- serves the pre-compiled stylesheet below
  api/scripts.rs   GET /datastar.js -- serves the vendored Datastar bundle
                   below, instead of loading it from a CDN
assets/app.css     Pre-compiled, minified Tailwind output for the classes
                   this crate's view! markup actually uses. Regenerate
                   after adding/removing classes:

                     tailwindcss -i <(echo '@import "tailwindcss";') \
                       -o blockzilla/monitor/src/assets/app.css \
                       --cwd blockzilla/monitor --minify

                   Get the standalone CLI (no Node needed) from
                   https://github.com/tailwindlabs/tailwindcss/releases --
                   this repo pins v4.3.2, matching what topcoat's own
                   tailwind feature downloads. Checked in rather than built
                   on every `cargo build`: it avoids a network dependency
                   and a full topcoat asset-bundle step (build.rs + content-
                   hashed AssetBundle) for a stylesheet that changes only
                   when the markup does.
assets/datastar.js Vendored Datastar client bundle (currently v1.0.2), same
                   reasoning as app.css: self-hosting avoids a render-
                   blocking third-party request, an unpinned/no-SRI supply-
                   chain risk, and a visitor-IP leak to the CDN on every
                   page load. Re-download to update the pinned version:

                     curl -fL \
                       https://cdn.jsdelivr.net/gh/starfederation/datastar@<version>/bundles/datastar.js \
                       -o blockzilla/monitor/src/assets/datastar.js
assets/mainnet-epoch-calendar.json
                   Bundled reference epoch calendar (genesis through
                   ~mid-2026, ~1000 entries) -- copied verbatim from
                   web/blockzilla-watcher/src/lib/data/mainnet-epoch-calendar.json.
                   Chain history doesn't change, so this only needs
                   re-copying to extend coverage further into the future,
                   not on every build.
```

## How the live updates flow

Two independent hops stay incremental for routine scalar updates. Full
state is fetched/sent on connect, explicit `resync`, or a sequence gap;
rare structural changes morph one complete route frame so list membership
and offline/live state cannot drift.

**Scheduler -> this process** (`client.rs`, `snapshot.rs`):

1. `client.rs` fetches `GET {upstream}/api/v1/status` once at startup
   (building the in-memory `PipelineSnapshot` in `Session`) and opens
   `GET {upstream}/api/v1/events`. Both operations have connection/body
   bounds. The 45-second transport and application-freshness deadlines cover
   nine default 5-second scheduler reconciles and three 15-second SSE
   keep-alive windows.
2. A `snapshot` SSE event replaces that in-memory snapshot wholesale. A
   `snapshot_patch` event is applied in place with
   `PipelineSnapshot::apply_patch` -- epochs are reconciled by key
   (`epochs_changed` upserted, `epochs_removed` deleted, the rest of the
   list untouched), every other field replaces the corresponding field on
   the snapshot. This is a faithful port of `applySnapshotPatch` in
   `web/blockzilla-watcher/src/lib/snapshot-patch.ts`.
3. `snapshot::sequence_action` (ported from `snapshotPatchSequenceAction`)
   decides per patch whether to apply it, ignore it as stale/duplicate, or
   fall back to a full `GET /api/v1/status` because of a sequence gap. An
   explicit `resync` SSE event always does the full `GET`.
4. Full and patch payloads must deserialize as schema v3 and pass collection,
   identity, state, counter, finite-number, and summary-consistency checks.
   Unknown additive fields remain tolerated. A malformed event triggers a
   validated full resync; if that fails, the dashboard goes stale/offline.
   Only an accepted snapshot or fully validated patch candidate calls
   `state::set_snapshot`.

**This process -> the browser** (`state.rs`, `api/stream.rs`):

1. `state::publish` maps the current `DashboardState` to its flat signal
   map (`to_signals`), diffs it against the signal map from the *previous*
   publish (`diff_signals`), and broadcasts only the changed keys -- keys
   that disappeared (e.g. an epoch leaving the active list) are sent as
   `null`, which Datastar's signal-patch protocol treats as "remove this
   signal." If scalar values did not change, no signal event is broadcast.
   Changes to live/offline state or any rendered list membership also emit
   one `Structure` marker; each subscriber turns it into a fresh frame for
   the route that tab is actually viewing.
2. `GET /api/stream` (the monitor's browser-facing endpoint) sends one
   full signal map and one freshly rendered `#dashboard-frame` the moment
   a tab connects -- a delta-only stream cannot self-correct values or rows
   that changed in the gap between the page render and stream opening --
   and then relays subsequent deltas. A client that falls behind the
   broadcast buffer (`Err(Lagged)`) gets the same complete signal + frame
   resync instead of silently missing whatever it dropped.
3. Every page wraps its content in `components::dashboard_shell`, which
   seeds `data-signals` from the current server-rendered state (first
   paint, before any SSE has connected) and opens `/api/stream` via
   `data-on:load`. The shell remains mounted around the route-specific
   `#dashboard-frame` in both live and offline states, so offline-first
   pages recover and live pages morph to the honest offline view without a
   manual refresh.

Snapshot, offline, and independently sampled local process-I/O publications
share one async publication gate. Recompute and broadcast therefore preserve
arrival order; a slow process sample cannot publish an older snapshot after a
disconnect or newer scheduler update. The optional local gap-index reader also
opens one nonblocking, no-follow file descriptor, accepts regular files only,
reads at most the configured maximum plus one byte, and rechecks file/path
identity after the read. A path swap, FIFO, or sparse oversized artifact is
rejected without an unbounded read.

## Known gaps

### Remaining hardening follow-ups from the 2026-08 monitor review

- Make non-Overview content intentionally live. History, System, Epoch
  field values, and Calendar are still primarily server-rendered snapshots;
  routine scalar ticks do not rebuild those page bodies unless a structural
  resync also occurs.
- Wire a real producer for `recent_compactions`; the History page and wire
  field exist, but the current scheduler path does not populate a useful
  completion history.
- Add collection timestamps/error state to local process telemetry so a
  stalled sampler is visibly stale rather than silently retaining its last
  successful values.
- **The scheduler has no HTTP route for the block-time-gap sidecar at
  all.** The scheduler's status server never registered a handler for
  `GET /api/v1/sidecars/block-time-gaps/index.json`
  (nor for `runtime-operations` or `shred-ingest`) -- every request 502s
  regardless of whether the index has been generated. Confirmed by grepping
  `blockzilla/cli/src/scheduler/mod.rs` for any of those route strings: nothing.
  Adding that route means shipping a new scheduler binary and restarting
  the live, actively-executing archiver process -- real production risk,
  not attempted here.
- **Worked around with `--gap-index-file` instead.** The per-epoch
  `block-time-gaps.bin` sidecars already exist from normal compaction
  (1007/1011 on the production archive as of 2026-08-05); running
  `blockzilla build-block-time-gap-index <archive-root> --output <path>
  --start-epoch 0 --end-epoch <n> --minimum-interruption-secs 300` (see
  `docs/reference/block-time-gap-sidecar.md`) aggregates them into one JSON
  file in seconds, read-only against the archive. `--gap-index-file <path>`
  (see `main.rs`) has this binary read that file directly from local disk
  on the same poll loop, instead of going over HTTP --
  safe because this binary already runs on the same host as the archive,
  and the file itself contains nothing more sensitive than epoch numbers,
  timestamps, slot numbers, and a source SHA-256 (comparable to the bundled
  reference calendar). The HTTP path (`start_gap_index_poller` in
  `client.rs`) stays available for deployments where that isn't true, or
  for whenever the scheduler grows a real route.
- **The generated index is a point-in-time snapshot, not auto-refreshing.**
  Re-run the `build-block-time-gap-index` command above (a few seconds) to
  pick up newly-compacted epochs; `--gap-index-file`'s poller re-reads the
  file every 10 minutes, so a cron re-running that command is enough to
  keep it current. No cron was set up as part of this work -- that's a
  standing-configuration change on the user's infrastructure, left to
  whoever owns the box.
- **Calendar tone taxonomy is simplified from the Svelte original.** Eleven
  fine-grained visual states (complete / first-seen-complete /
  legacy-complete / active / ready / finalizing / partial / queued /
  missing / attention / failed) collapse to five color buckets here, reusing
  this app's existing emerald/cyan/amber/rose/zinc palette -- the original
  distinction doesn't survive at cell size anyway, and the precise
  sub-status is still in the tooltip text. See `calendar.rs`'s module doc.
  Live-capture states (`snapshot.live[]`) aren't merged into the calendar
  either, matching how the rest of this dashboard already only reads
  `snapshot.epochs`.
- **No ingest-pipeline sidecar.** The old "Ingest" page's three hardcoded
  stage cards (raw-shred recorder / Triton ingest / indexer) had no backing
  field in `PipelineSnapshot` at all; they were fabricated. This dashboard's
  `/system` page shows `machine` and `process_io` instead, which *are* real
  fields on the snapshot. Wiring the separate
  `/api/v1/sidecars/ingest-pipeline/status.json` contract
  (`ingest-pipeline-status.ts`) is future work, not done here.
- **Read-only, permanently.** `scheduler.paused` is surfaced as a badge,
  but there's no pause/resume control, and there's not meant to be one --
  see `docs/operations/blockzilla-monitor-roadmap.md` for why (this
  dashboard is designed to be exposed to the public internet).
- **Scheduler reasoning is current-state only, not a history.** The
  "Scheduler reasoning" panel surfaces `admission_blocked_reason`,
  `legacy_compact_last_action`, `legacy_compact_tuning_last_decision`, and
  auto-paused lanes -- all real fields the scheduler already serves, just
  not previously read here. But the scheduler overwrites these in place
  every tick rather than logging them, so this can only ever show "right
  now," never "what happened at 3am." A real audit trail needs a scheduler-
  owned durable event stream and a bounded read-only endpoint -- tracked as
  roadmap §5, not done here.
