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

## API contract

The client reads:

- `GET /api/v1/status` for a full pipeline snapshot.
- `GET /api/v1/events` for server-sent `snapshot`, `snapshot_patch`, and
  `resync` events. Event data uses the envelope `{ type, sequence, data }`.

Snapshots may optionally include `epoch_calendar` entries with `epoch`,
`start_unix_secs`, nullable `end_unix_secs`, and `precision` (`observed` or
`estimated`). Observed boundaries should come from the epoch's first and last
produced block times. The UI groups these chain dates by UTC month, marks
estimates with `~`, and never treats archive file timestamps as epoch dates.

Snapshots may optionally include a bounded `recent_compactions` array. Each
successful canonical archive publication has a stable `id`, `epoch`, `workflow`
(`historical`, `live`, or `recompact`), `completed_unix_secs`, and
`duration_secs`. Duration is wall-clock time from compactor worker start through
successful publication; it excludes queue wait and source download. Persist the
completion time and duration when the canonical commit marker is published—do
not infer them from inventory refreshes, epoch dates, or artifact modification
times. Older snapshots fall back to the highest complete epochs and label their
timings unavailable.

Snapshots may also include a `process_io` sample:

- `state`: `collecting`, `ready`, or `unavailable`.
- `sampled_unix_secs`, `sample_window_secs`, `active_count`,
  `inaccessible_count`, and `truncated` describe the bounded sample.
- `processes` contains stable `id`, `pid`, `/proc/<pid>/comm` `name`, optional
  `user`, `read_mib_per_sec`, `write_mib_per_sec`, and optional `cpu_percent`,
  `rss_bytes`, and `blockzilla_owned`. Raw command-line arguments must not be
  exposed because they can contain secrets.

The backend should exclude the watcher and every managed Blockzilla process
tree; the UI also hides entries explicitly marked `blockzilla_owned`. Rates
from `/proc/<pid>/io` cover all filesystems used by the process, not only the
archive device. CPU follows Linux `top` semantics: 100% is one logical core and
multithreaded processes may exceed 100%.
