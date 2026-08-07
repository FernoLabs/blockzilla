# Blockzilla Monitor

A server-rendered task/health dashboard for Blockzilla, built on
[Topcoat](https://github.com/tokio-rs/topcoat) with live updates pushed
over Server-Sent Events via `topcoat::datastar`.

It is fed by **`services/blockzilla-watcher-gateway`** -- the redacted,
public-safe proxy in front of the scheduler's status API -- never by the
scheduler directly. That boundary strips secrets and absolute paths before
anything reaches this process; see `public_json.rs` in the gateway crate.

## Running it

Start the gateway first (see `services/blockzilla-watcher-gateway`, or
`TODO.md` at the repo root for the exact invocation), then:

```
cargo run -p blockzilla-monitor -- --upstream http://127.0.0.1:8787
```

`--upstream` defaults to `http://127.0.0.1:8787` and can also be set via
`BLOCKZILLA_MONITOR_UPSTREAM`. `HOST`/`PORT` control this app's own bind
address (default `127.0.0.1:3000`).

`--tier public|full` (default `public`) controls how much of a real
snapshot reaches the page: `public` drops the process table entirely and
scrubs free-text fields (error messages, lane/workflow identifiers) for
paths and credentials as defense-in-depth; `full` shows everything. This
binary has no authentication of its own, so a `full` instance must only be
reachable from a network-gated path (Cloudflare Access, Tailscale, an IP
allowlist) -- see `docs/operations/blockzilla-monitor-roadmap.md` §3 for
the intended two-instance deployment shape. `--max-stream-connections`
(default `512`) caps concurrent `/api/stream` subscribers.

If the gateway isn't reachable, every page shows an honest "Gateway
unavailable" screen with the real connection error -- this dashboard never
substitutes fabricated numbers for a missing snapshot.

For local UI iteration without a gateway running:

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
                   apps/blockzilla-watcher/src/lib/{epoch-calendar,
                   epoch-year-calendar}.ts -- see that module's doc comment
                   for the two deliberate simplifications
  calendar_view.rs rendering for calendar.rs's output (the actual grid
                   markup) -- kept separate from calendar.rs so the date
                   math stays independently testable without a view! macro
                   in the loop
  snapshot.rs      tolerant Rust types for the real PipelineSnapshot wire
                   schema served by blockzilla-watcher-gateway at
                   /api/v1/status and /api/v1/events (mirrors
                   apps/blockzilla-watcher/src/lib/pipeline-snapshot.ts),
                   plus the patch type and incremental apply_patch/
                   sequence_action logic (mirrors snapshot-patch.ts)
  client.rs        background task: bootstraps from GET /api/v1/status,
                   then applies incremental snapshot_patch events from SSE
                   /api/v1/events to an in-memory snapshot, republished
                   into state.rs
  state.rs         DashboardState model (the flat signal-map serializer),
                   the real-snapshot -> DashboardState mapping, and the
                   --demo ticker
  api/stream.rs    GET /api/stream -- this app's own SSE endpoint that
                   Datastar subscribes to on every page. Concurrent
                   subscribers are capped by a tower ConcurrencyLimitLayer
                   registered in main.rs (--max-stream-connections)
  api/styles.rs    GET /app.css -- serves the pre-compiled stylesheet below
  api/scripts.rs   GET /datastar.js -- serves the vendored Datastar bundle
                   below, instead of loading it from a CDN
assets/app.css     Pre-compiled, minified Tailwind output for the classes
                   this crate's view! markup actually uses. Regenerate
                   after adding/removing classes:

                     tailwindcss -i <(echo '@import "tailwindcss";') \
                       -o services/blockzilla-monitor/src/assets/app.css \
                       --cwd services/blockzilla-monitor --minify

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
                       -o services/blockzilla-monitor/src/assets/datastar.js
assets/mainnet-epoch-calendar.json
                   Bundled reference epoch calendar (genesis through
                   ~mid-2026, ~1000 entries) -- copied verbatim from
                   apps/blockzilla-watcher/src/lib/data/mainnet-epoch-calendar.json.
                   Chain history doesn't change, so this only needs
                   re-copying to extend coverage further into the future,
                   not on every build.
```

## How the live updates flow

Two independent hops, both incremental end to end -- nothing here
re-fetches or re-sends a full snapshot on a routine update, only on
connect, on an explicit `resync`, or when a sequence gap forces one.

**Gateway -> this process** (`client.rs`, `snapshot.rs`):

1. `client.rs` fetches `GET {upstream}/api/v1/status` once at startup
   (building the in-memory `PipelineSnapshot` in `Session`) and opens
   `GET {upstream}/api/v1/events`.
2. A `snapshot` SSE event replaces that in-memory snapshot wholesale. A
   `snapshot_patch` event is applied in place with
   `PipelineSnapshot::apply_patch` -- epochs are reconciled by key
   (`epochs_changed` upserted, `epochs_removed` deleted, the rest of the
   list untouched), every other field replaces the corresponding field on
   the snapshot. This is a faithful port of `applySnapshotPatch` in
   `apps/blockzilla-watcher/src/lib/snapshot-patch.ts`.
3. `snapshot::sequence_action` (ported from `snapshotPatchSequenceAction`)
   decides per patch whether to apply it, ignore it as stale/duplicate, or
   fall back to a full `GET /api/v1/status` because of a sequence gap. An
   explicit `resync` SSE event always does the full `GET`.
4. Every accepted snapshot or applied patch calls `state::set_snapshot`.

**This process -> the browser** (`state.rs`, `api/stream.rs`):

1. `state::publish` maps the current `DashboardState` to its flat signal
   map (`to_signals`), diffs it against the signal map from the *previous*
   publish (`diff_signals`), and broadcasts only the changed keys -- keys
   that disappeared (e.g. an epoch leaving the active list) are sent as
   `null`, which Datastar's signal-patch protocol treats as "remove this
   signal." If nothing changed, nothing is broadcast.
2. `GET /api/stream` (this app's own endpoint, not the gateway's) sends one
   full signal map the moment a tab connects -- a delta-only stream can't
   self-correct a value that changed in the gap between this page's server
   render and the SSE connection opening, the way a full-payload stream
   always could -- and then relays each subsequent delta as one
   `PatchSignals::json` Datastar event. A client that falls behind the
   broadcast buffer (`Err(Lagged)`) gets a fresh full map instead of
   silently missing whatever it dropped.
3. Every page wraps its content in `components::dashboard_shell`, which
   seeds `data-signals` from the current server-rendered state (first
   paint, before any SSE has connected) and opens `/api/stream` via
   `data-on:load`. All four pages need this, not just Overview -- the
   header's live dot and connection-state text bind to `$live` /
   `$connection_state`, and a page without the shell references signals
   Datastar never seeded.

## Known gaps

- **The scheduler has no HTTP route for the block-time-gap sidecar at
  all.** `blockzilla-watcher-gateway` allowlists
  `GET /api/v1/sidecars/block-time-gaps/index.json` and will proxy it, but
  the scheduler's status server never registered a handler for that path
  (nor for `runtime-operations` or `shred-ingest`) -- every request 502s
  regardless of whether the index has been generated. Confirmed by grepping
  `blockzilla/src/scheduler/mod.rs` for any of those route strings: nothing.
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
  on the same poll loop, instead of going over HTTP through the gateway --
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
  now," never "what happened at 3am." A real audit trail would read back
  `blockzilla-watcher-gateway`'s already-built (but currently
  HTTP-unreachable) `scheduler_incidents.rs` event log -- tracked as
  roadmap §5, not done here.
