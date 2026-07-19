# Blockzilla Watcher

Blockzilla Watcher is the operational UI for live indexing, archive work, and
NAS health. Its default view keeps the live epoch, active task ETAs, runnable
queue ETA, compact epoch timeline, NAS resources, recent errors, and external
disk I/O visible; deeper worker and live-pipeline details remain collapsible.
Completed compactions and recorded run durations live on the separate
`/history` page.

This directory contains the UI client only. The current
[`services/hivezilla`](../hivezilla/README.md) capture service does not provide
the watcher API.

## Run locally

Use Node.js 24 or newer, then point the development proxy at a compatible
watcher API:

```bash
cd services/blockzilla-watcher
npm ci
BLOCKZILLA_WATCHER_API_URL=http://127.0.0.1:8788 npm run dev
```

The production build uses same-origin `/api` requests:

```bash
npm test
npm run check
npm run build
```

The static build uses `index.html` as its SPA fallback. Production hosting must
rewrite direct requests such as `/history` to that fallback.

To keep the maintenance screen available while the watcher process or NAS is
offline, serve the static `build/` directory from hosting that is independent
of the watcher API and route only `/api/*` to the NAS. If the same unavailable
origin serves both the page and the API, the browser cannot load the client-side
maintenance screen.

## Epoch dates

The calendar bundles a generated mainnet reference at
`src/lib/data/mainnet-epoch-calendar.json`. A non-empty `epoch_calendar` from
the watcher API remains authoritative and replaces matching reference entries.

Regenerate the displayed dates from an archival Solana RPC endpoint:

```bash
SOLANA_RPC_URL=https://your-archival-rpc.example \
  npm run generate:epoch-calendar -- \
  --dates-only \
  --cutoff-slot 432431999 \
  --output src/lib/data/mainnet-epoch-calendar.json \
  --concurrency 4
```

Dates-only mode locates each epoch's first and last produced blocks and obtains
their chain timestamps. Unavailable historical times are interpolated between
observed timestamps and marked `estimated`.

If the live snapshot is newer than both the bundled reference and its optional
`epoch_calendar`, the UI projects only the missing tail. It uses the median of
the latest 16 known epoch durations and marks every projected range `estimated`;
`432,000 × 400 ms` is only the last fallback when no duration sample exists.
Regenerate the reference regularly because the target slot duration can change.

Omit `--dates-only` to perform the reproducible skipped-slot audit. Full mode
scans finalized history in non-overlapping `getBlocks` windows of at most
500,000 slots, keeps only per-epoch counts and boundaries, and records exact
produced/skipped counts in `epoch_stats`. It transfers several gigabytes for a
full mainnet history scan, so both modes resume from ignored checkpoints under
`target/epoch-calendar/`.

Use an archival endpoint. The generator refuses an endpoint whose first
available block is newer than the requested start, because pruned history must
not be counted as skipped slots. RPC endpoint URLs are never written to the
generated dataset or checkpoints.

## API contract

The client reads:

- `GET /api/v1/status` for a full pipeline snapshot.
- `GET /api/v1/events` for server-sent `snapshot`, `snapshot_patch`, and
  `resync` events. Event data uses the envelope `{ type, sequence, data }`.
- Optionally, `GET /api/v1/sidecars/block-time-gaps/status.json` for a
  sanitized block-time-gap sidecar backfill snapshot. The client polls this
  maintenance feed every 15 seconds and marks samples older than 60 seconds
  stale. Deployments without an active or retained backfill may return 404.
- Optionally, `GET /api/v1/sidecars/runtime-operations/status.json` for raw WAL
  capture, external CAR download/verification jobs, and bounded process I/O.
  The client polls this feed every five seconds and marks samples older than 20
  seconds stale. This keeps one-off NAS work visible without exposing command
  lines, endpoints, tokens, or filesystem paths. The fallback process sampler
  is deliberately limited to processes owned by the publisher's Unix user and
  excludes Blockzilla/Hivezilla work already represented elsewhere in the UI.

The standard-library-only runtime publisher can write that sidecar beside the
built UI:

```bash
python3 scripts/publish-runtime-operations.py \
  --output /path/to/ui/api/v1/sidecars/runtime-operations/status.json
```

## Public deployment

The scheduler keeps full filesystem paths internally. Its JSON serializers
publish only safe artifact basenames. When an older scheduler must remain in
service, place `scripts/public-status-proxy.py` on the public boundary instead:

```sh
python3 scripts/public-status-proxy.py \
  --listen 127.0.0.1:8787 \
  --upstream 127.0.0.1:8786
```

The proxy is read-only, preserves the status and SSE contracts, and strips
absolute storage paths from full snapshots, sidecar JSON, and real-time
patches. Keep the upstream on loopback. The listener may use loopback or the
explicit private address targeted by the tunnel; wildcard and public binds are
rejected.

Raw WAL capture is labeled as capture, not indexing. Archive materialization
remains a separate Blockzilla task. A completed CAR transfer undergoing SHA-256
is shown as `CAR verification`, rather than continuing to claim it is a
download.

Snapshots may optionally include `epoch_calendar` entries with `epoch`,
`start_unix_secs`, nullable `end_unix_secs`, and `precision` (`observed` or
`estimated`). Observed boundaries should come from the epoch's first and last
produced block times. The UI groups these chain dates by UTC month, marks
estimates with `~`, and never treats archive file timestamps as epoch dates.

Snapshots may optionally include a bounded `recent_compactions` array. Each
successful canonical archive publication has a stable `id`, `epoch`, `workflow`
(`historical`, `live`, or `recompact`), `completed_unix_secs`, and
`duration_secs`. Duration is wall-clock time from compactor worker start through
successful publication; it excludes queue wait and source download. Persisted
completion records remain authoritative. When a completed epoch has no record,
the UI may show the archive `metadata` artifact modification time as a labeled
approximate archive file time; duration remains unavailable. It must not use
source CAR times, later repair artifact times, inventory refreshes, or chain
dates as compaction completion times.

Snapshots may also include a `process_io` sample:

- `state`: `collecting`, `ready`, or `unavailable`.
- `sampled_unix_secs`, `sample_window_secs`, `active_count`,
  `inaccessible_count`, and `truncated` describe the bounded sample.
- `processes` contains stable `id`, `pid`, `/proc/<pid>/comm` `name`,
  `read_mib_per_sec`, `write_mib_per_sec`, and optional `cpu_percent`,
  `rss_bytes`, and `blockzilla_owned`. Raw command-line arguments must not be
  exposed because they can contain secrets.

The backend should exclude the watcher and every managed Blockzilla process
tree; the UI also hides entries explicitly marked `blockzilla_owned`. Rates
from `/proc/<pid>/io` cover all filesystems used by the process, not only the
archive device. CPU follows Linux `top` semantics: 100% is one logical core and
multithreaded processes may exceed 100%.
