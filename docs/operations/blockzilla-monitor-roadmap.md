# blockzilla-monitor: read-only ops dashboard roadmap

> Status: planning. Nothing in this document is implemented yet except where
> marked "done". Written after a 7-agent research pass (topcoat/Datastar
> compliance, Datastar security, monitoring-dashboard UX, blockzilla CLI
> inventory, scheduler explainability, hivezilla/multi-machine architecture,
> and a public-exposure security audit) on 2026-08-05. Pick tasks from here
> in any order; each phase lists exact files/fields so a task can be picked
> up without re-deriving this context.

## Constraints (from the product owner)

- **Read-only, permanently.** No `POST`/mutating routes ever get added to
  `blockzilla-monitor`, regardless of auth tier. Confirmed today: only
  `GET` routes exist (`app.rs`, `api/stream.rs`, `api/styles.rs`).
- **Exposed to the public internet.** Some of what it will show is real
  infrastructure telemetry, so exposure has to be designed deliberately, not
  assumed safe because "read-only."
- Two-tier exposure wanted: a fully public, anonymous tier with safe data
  only, and a separate authenticated tier for the fuller/leakier data. See
  §3.

## 1. Where things stand today

`blockzilla-monitor` (this crate) talks to `blockzilla-watcher-gateway`'s
`GET /api/v1/status` + `GET /api/v1/events` (SSE), which itself proxies
`blockzilla scheduler`'s status API. Four pages exist: Overview, History,
System, Epochs — all real data, no fabricated placeholders, verified
end-to-end against the live NAS deployment.

### 1.1 Topcoat/Datastar framework compliance

Good: SSE endpoint shape, diffed-signal patching (`state.rs` `diff_signals`),
escaped attribute interpolation, `.discover()` + explicit-path routing all
match documented topcoat idioms exactly.

Gaps:
- `datastar.js` is CDN-loaded (`app.rs`), unpinned by SRI — Datastar's own
  docs recommend self-hosting for production, and we already did this
  exercise for Tailwind. Should self-host the same way.
- `Cargo.toml` declares the `tailwind` feature but nothing uses
  `BuildConfig::render()`/`AssetBundle` — dead flag, since CSS is a
  checked-in file served by a hand-written route instead (a deliberate,
  documented tradeoff — see `README.md`). Worth removing the unused feature
  flag for clarity.
- Shared state (`state.rs`, `static SHARED: OnceLock<Shared>`) bypasses
  topcoat's `app_context`, the documented idiom for cross-request
  singletons. Works fine; just off-idiom.

### 1.2 Scheduler explainability — the highest-leverage finding

The gateway's live snapshot already carries rich "why" data, populated every
tick, already served over the wire:

| Field | Where | Currently read by monitor? |
|---|---|---|
| `summary.admission_blocked_reason` | `scheduler/mod.rs:531` | No |
| `summary.legacy_compact_admission_blocked_reason` | `scheduler/mod.rs:607` | No |
| `summary.legacy_compact_last_action` (+ `_unix_secs`) | `scheduler/mod.rs:591,593` | No |
| `summary.legacy_compact_tuning_last_decision` | `scheduler/mod.rs:565` | No |
| `summary.queue_eta_reason` | `scheduler/mod.rs:508` | Yes |
| `lanes[].auto_paused` / `.auto_pause_reason` | `scheduler/mod.rs:382-384` | No |
| `errors[]` (ring buffer, last 100, durably persisted to `errors.jsonl`) | `scheduler/mod.rs:105,14379-14392` | Yes (fully piped, just needs a richer UI) |

`services/blockzilla-monitor/src/snapshot.rs`'s `PipelineSummary` (lines
44-67) and `LaneStatus` (122-127) only read a subset of what's already on
the wire. **Adding the missing fields is zero backend work** — the wire
schema is additive/tolerant (`#[serde(default)]` throughout), so this is
purely a monitor-side change: extend the structs, map them into
`DashboardState`, add a "Scheduler reasoning" panel.

Bigger finding: `services/blockzilla-watcher-gateway/src/scheduler_incidents.rs`
is a **complete, already-built audit-trail pipeline** that nothing reads
back over HTTP:
- Typed `Reason` enum with `{code, observed, threshold, unit}`
  (`scheduler_incidents.rs:170-232`)
- Host metrics + process context snapshot at decision time
  (`HostMetrics`/`ProcessContext`, lines 171-255)
- A pre-event sample ring (`SampleSummary`, lines 258-263)
- A full `Incident` record with content-addressed `id`
  (`Incident`, lines 266-279; `make_event`, line 1224)
- Durable, rotating, permission-locked storage: `append_jsonl`
  (lines 1481-1524, `0600` perms, `O_NOFOLLOW`/`O_CLOEXEC`)

`record()` (line 338) is a standalone recorder process. Grepping the whole
gateway tree for readers of its `events.jsonl` output turns up nothing —
**it's write-only today.** Separately, `scheduler/mod.rs` also writes a
lower-level `control-events.jsonl` (raw `{at_unix_secs, action, target}`
lines, `append_control_event`, `mod.rs:14956-14972`) that's likewise never
read back.

Caveat: the scheduler itself has **zero structured logging** — `grep -c
"tracing::\|log::\|info!\|warn!\|error!\|debug!"` over all 23k+ lines of
`scheduler/mod.rs` returns 0. All "why" signal lives in typed fields and
these two JSONL sinks, not a conventional log stream. A timeline covering
*every* decision type (not just pause/resume/tuning) would need new
persistence for fields that are currently snapshot-only (e.g.
`admission_blocked_reason` is recomputed every tick but never durably
logged).

### 1.3 Public-exposure security audit — fix before/alongside going live

The gateway's redaction for `/api/v1/status` and `/api/v1/events` is
**denylist-based** (`public_json.rs:9-91`): it strips known-bad key names
and regex-matches paths/tokens/credentials inside string values. It does
not know which fields are *safe* — anything it didn't anticipate passes
through. Contrast with `/api/v1/sidecars/ingest-pipeline/status.json`
(`ingest_contract.rs:531-667`), which is a true allowlist/typed projection.

Concrete leaks today, none of which `blockzilla-monitor`'s own `state.rs`
adds further redaction for:
- **Process table** (`ProcessIoEntry.name`/`.pid`, `snapshot.rs:182-191`,
  kept for all non-Blockzilla processes at `state.rs:235`) — real host
  fingerprinting: reveals what other software runs on the box, live PIDs.
  Neither `name` nor `pid` is in the gateway's sensitive-key list.
- `PipelineError.message` — free-text operator strings, passed through
  as-is; the gateway's path regex only catches a fixed prefix list.
- `LaneStatus.id`/`.kind`, `CompactionHistoryEntry.workflow`/`.id` —
  internal naming/workflow identifiers.
- `MachineStatus` — exact hardware specs (total memory/disk).

Also unaddressed:
- **No connection/rate limit on `/api/stream`** — unbounded concurrent SSE
  subscribers. `blockzilla-watcher-gateway/src/public_proxy.rs:199-201,383-388`
  already solves this class of problem with an `Arc<Semaphore>`
  (`--max-requests`, default 64, returns 503 when exhausted) that we never
  ported over.
- `client.rs`'s hand-rolled SSE line parser has no size cap (the gateway's
  equivalent path uses `MAX_SSE_LINE_BYTES`/`read_limited`).
- No `robots.txt` / `X-Robots-Tag: noindex` anywhere.

## 2. Immediate: security hardening (do this regardless of what else gets built)

These are cheap, monitor-side (mostly), and close real gaps before public
launch grows the audience:

1. **Redact the process table** in `state.rs`'s `DashboardState::from_snapshot`
   mapping — drop `pid` entirely from the public tier, and either drop
   non-Blockzilla process `name`s or generalize them (e.g. bucket into
   "other process" without the literal binary name). This is the single
   highest-risk field set found.
2. **Add a defense-in-depth redaction pass** in the same mapping function
   for `message`/`id`/`workflow` strings, reusing the same class of
   path/credential regex `public_json.rs` already has, in case the gateway's
   filter is ever bypassed or the schema grows a new leaky field.
3. **Rate-limit `/api/stream`.** Port the gateway's semaphore pattern
   (`public_proxy.rs:199-201`) — cap concurrent subscribers, return 503 past
   the cap.
4. **Cap the SSE line parser buffer** in `client.rs` (currently unbounded
   `Vec<u8>` grown until `\n`) — same fix the gateway already has, lower
   urgency since the peer is the trusted internal gateway, but cheap.
5. **Self-host `datastar.js`** instead of the jsdelivr CDN tag — same
   pattern already used for Tailwind (`api/styles.rs`), removes a
   third-party dependency, a visitor-IP leak to jsdelivr, and closes the
   no-SRI gap.
6. **Add `X-Robots-Tag: noindex`** response header across all routes so
   operational telemetry doesn't get indexed.

## 3. Exposure model: public tier + authenticated tier

Recommended shape, given this is a single small Rust binary with no
existing auth/session infrastructure and the team runs it via a plain
`nohup`'d process on a NAS (no Kubernetes/ingress layer):

**Run two instances of the same binary, differentiated by a `--tier
public|full` flag, gated at the network layer rather than in-app.**

- `--tier public` (or a `--redact` flag): applies the redaction pass from
  §2 unconditionally — process table dropped, message/id/workflow strings
  scrubbed. This instance is the one that's actually anonymous-public.
- `--tier full`: no redaction, shows everything (process table, full error
  text, etc.) for real debugging. This instance should **not** get its own
  in-app login system — front it with something that already solves
  authn well and needs no app code: Cloudflare Access, or a Tailscale-only
  bind, or an IP allowlist at the reverse-proxy/firewall level. The UX
  research independently converged on this same recommendation
  ("Cloudflare Access or a Tailscale-fronted deployment... genuinely
  internal-only exposure").

Why not in-app session auth (topcoat has `topcoat-session` available):
building and maintaining even a lightweight login system is real ongoing
surface area (password/token storage, session expiry, brute-force
handling) for a problem a reverse proxy already solves for free. Revisit
only if Cloudflare Access / Tailscale isn't viable for this deployment for
some operational reason.

Implementation shape: `state.rs`'s redaction becomes a function of a
`RedactionTier` passed in at startup (from the `--tier` flag), applied once
inside `DashboardState::from_snapshot` before signals are built — both the
initial server-rendered page and the SSE broadcast stream derive from the
same already-redacted `DashboardState`, so there's no risk of the two tiers
sharing a broadcast channel and leaking full data to a public subscriber.

## 4. Scheduler reasoning panel (cheap, high value, do early)

1. Extend `snapshot.rs`'s `PipelineSummary` and `LaneStatus` with the fields
   listed in §1.2's table (all additive, all already tolerant of unknown
   fields).
2. Map them into `DashboardState` in `state.rs`.
3. New "Scheduler reasoning" panel (Overview page or its own section):
   surface `admission_blocked_reason`, `legacy_compact_last_action`,
   `legacy_compact_tuning_last_decision`, and per-lane `auto_pause_reason`
   using the UX pattern from the research — one short human sentence +
   the triggering condition/threshold where available, not a raw dump.
4. Richer error-log UI for the already-fully-piped `errors[]` — this is
   real durable history (`errors.jsonl`), just needs better presentation
   (level/scope grouping, search).

Everything in this phase is monitor-only work; zero gateway/scheduler
changes required.

## 5. Real decision audit trail (needs one new backend endpoint)

Wires up the orphaned `scheduler_incidents.rs` pipeline described in §1.2:

1. Add a `GET` endpoint on `blockzilla-watcher-gateway` that reads back
   `scheduler_incidents.rs`'s `events.jsonl` (rotating, `.1` backup) —
   paginated/time-windowed, not a full-file dump given it rotates at
   `MAX_PRIVATE_LOG_BYTES`.
2. Decide whether to also expose the lower-level `control-events.jsonl`
   (raw `{action, target}` lines, `scheduler/mod.rs:14956-14972`) as a
   fallback for events that predate/exceed the incident recorder's window,
   or treat incidents as the sole source of truth.
3. Monitor-side: a Kubernetes-Events-style timeline — one event per row,
   reason code + human message + timestamp, scoped per-job with a global
   feed, collapsing repeated events ("retried ×4") rather than one row
   each. This is the UX pattern the research converged on across
   Kubernetes/GitHub Actions/Grafana.
4. Redaction note: this is richer data than the status snapshot (host
   metrics, process context per incident) — almost certainly **full-tier
   only**, not public-tier, until it's been through the same redaction
   review as §2.

## 6. Subprocess stdout/stderr (needs investigation before scoping)

Lower priority per the product owner ("mainly decision-reasoning trail,
then to debug subprocess stdout/stderr"). Open question nothing in this
research answered: **where does subprocess output from jobs like
`build-archive-v2-hot-blocks` or `verify-archive-v2-poh` currently go** —
is it captured/persisted anywhere (a per-job log file, journald, piped to
the scheduler's own stdout) or lost once the process exits? Answering that
is the first task here, before any UI design — it determines whether this
is "read an existing file" or "add new capture plumbing to the scheduler's
subprocess spawning."

Once that's known, the log-viewer UX from the research applies: virtualized
rendering, explicit live-tail-vs-paused mode (queue new lines with a "N new
lines ↓" affordance rather than silently appending while scrolled up),
absolute timestamps, level-colored line markers.

## 7. Hivezilla integration

Hivezilla already exposes exactly the kind of endpoint `blockzilla-monitor`
already knows how to consume — same shape as the blockzilla gateway
pattern:
- `services/hivezilla/src/shred_status.rs`: `GET
  /api/v1/sidecars/shred-ingest/status.json` + `/healthz`, explicitly
  designed for external polling (README shows `--cors-origin
  https://watcher.blockzilla.dev`), response shape `PublicShredStatus`
  (`shred_status.rs:313-397`).
- `services/hivezilla/src/ingest_status.rs`: `GET
  /api/v1/sidecars/ingest-pipeline/status.json` + `/healthz`, currently
  loopback/private-bound only.

Plan: add a second `client.rs`-style poller (reuse the existing
HTTP+SSE-or-poll pattern) pointed at a hivezilla instance's shred-status
endpoint, a `HivezillaSnapshot` type mirroring `PublicShredStatus`, and a
new page/section. No hivezilla-side protocol work needed — the sanitized
endpoint already exists and was built for this purpose.

## 8. Multi-machine support

No fleet/machine registry exists anywhere in the codebase today — confirmed
deliberate ("no dynamic membership... in V1" per
`hivezilla-convergence.md:40-41`). The deployment model is static/operator-
configured.

Minimal viable shape that fits existing conventions: a static config list
(`--peers name=url,name=url` flag or a small TOML/JSON config file) of
`{name, base_url}` entries, one per blockzilla-gateway or hivezilla instance,
polled in parallel — mirroring the single-upstream pattern `client.rs`
already uses, fanned out to N.

UI: per the research's progressive-disclosure recommendation, **build the
URL/state model now as if multiple hosts exist** (e.g. route shape
`/{host}/...` instead of today's flat `/`, `/history`, `/system`,
`/epochs`), but hide the host-picker chrome entirely while there's only one
configured entry — so this doesn't need to wait for a second machine to
exist before the groundwork goes in, and doesn't clutter the UI today.
Keep host selection and service selection (blockzilla vs. hivezilla) as two
independent selectors rather than one flat list, since they'll grow
independently.

## Suggested build order

Roughly by leverage (cheap + high value first), not a hard sequence:

1. §2 security hardening — should land before or alongside any wider
   public rollout, independent of everything else.
2. §4 scheduler reasoning panel — zero backend work, immediate value.
3. §3 tiered exposure (`--tier` flag + network-level gating for the full
   tier) — unlocks putting richer data (full error text, process table) in
   front of the team without putting it in front of the whole internet.
4. §7 hivezilla integration — the endpoint already exists; mechanically
   similar to work already done for the blockzilla gateway.
5. §5 decision audit trail — needs one new gateway endpoint, biggest UI
   lift, highest value for "understand why the scheduler did things."
6. §8 multi-machine — do the routing groundwork whenever §7 or another
   page-shape change is already in flight, to avoid a second migration.
7. §6 subprocess logs — start with the open investigation question, scope
   after that answer is known.
