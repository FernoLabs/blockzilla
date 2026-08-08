# Blockzilla protocol commands (scheduler + command surface) v1.0

> Status: draft design. Sections 1–3 document the current implementation.
> Sections 4–6 specify deferred work and are not yet an operator contract.

The 2026-08-04 production inventory, failure analysis, format-layout decision,
and integrity-verifier backlog are recorded in
`archive-completion-audit-2026-08-04.md`.

## Scope

This document records the concrete runtime command surface that exists in code
today and the agreed design for its replacement:

- `blockzilla` control/processing CLI surface,
- `blockzilla scheduler` local control HTTP protocol,
- the monitor's read-only public endpoint used by the operator UI.

It is written for operational usage and later migration to a Unix-socket command
channel. Implementation is intentionally deferred while higher-priority archive
work continues.

## 1) Scheduler control protocol (active, local)

### Transport

- Status API listener: `--status-bind` (default `127.0.0.1:8787`)
- Management API listener: `--management-bind` (optional; must be loopback + non-zero port)
- HTTP methods are currently used; all management calls are `POST`

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` | `{ok, mode}` where `mode in {execute, observer}` |
| `GET` | `/api/v1/status` | Full snapshot (schema versioned) |
| `GET` | `/api/v1/events` | SSE stream: `snapshot` / `snapshot_patch` / `resync` |
| `POST` | `/api/v1/control/pause` | Pause scheduler globally |
| `POST` | `/api/v1/control/resume` | Resume scheduler globally |
| `POST` | `/api/v1/jobs/{kind}/{id}/pause` | Pause one running job by kind/id |
| `POST` | `/api/v1/jobs/{kind}/{id}/resume` | Resume one running job by kind/id |
| `POST` | `/api/v1/jobs/{kind}/{id}/retry` | Retry one failed/stalled job by kind/id |

### Management command arguments

`{kind}` / `{id}` currently accepted:

- `car_download:<epoch>`
- `car_preflight:<epoch>`
- `historical_scan:<epoch>`
- `historical_compact_reuse:<epoch>`
- `historical_finalizer:<epoch>`
- `live_finalizer:<capture-id>`

Examples:

- `POST /api/v1/jobs/historical_scan/789/retry`
- `POST /api/v1/jobs/live_finalizer/cap-2026-07-xx/retry`

### Request requirements

- `Content-Type: application/json` is required even for empty bodies.
- In execute mode only (management endpoints return 403 in observer mode).
- Management bind must be configured; otherwise endpoints are disabled (403).
- `Host` must match the configured `--management-bind` exactly.
- Optional browser-origin hardening:
  - `Origin` must be same-origin relative to that bind host, if present.
  - `Sec-Fetch-Site` must be `same-origin`, if present.

### Responses

Success:

```json
{
  "ok": true,
  "action": "pause|resume|retry",
  "target": "scheduler|kind/id",
  "message": "...",
  "snapshot_sequence": <u64>
}
```

Failure:
- HTTP 400 Bad Request for invalid target syntax,
- 403 Forbidden for disabled/not authorized context,
- 404 Not Found for unknown target,
- 409 Conflict for invalid state,
- 500 Internal for internal failures.

### SSE event model

`/api/v1/events` publishes:

- `snapshot` (first frame for bootstrap)
- `snapshot_patch` (delta updates)
- `resync` (stream catch-up hint when lagged)

Patch events are bounded by changed epochs/lanes and carry a monotonic `sequence`.

## 2) `blockzilla` CLI command set (operational surface)

### Top-level subcommands

- `scheduler` (runs scheduler worker; no normal Clap subcommand branch, parsed in a dedicated mode)
- `preflight-car`
- `acquire-car`
- `seed-previous-blockhash-tails`
- `build-block-time-gaps`
- `verify-block-time-gaps`
- `build-block-time-gap-index`
- `build-archive-v2`
- `build-archive-v2-no-registry`
- `build-archive-v2-no-registry-from-url`
- `build-archive-v2-registries`
- `bench-car-registry`
- `build-archive-v2-registry-index`
- `prepare-archive-v2-live-registry`
- `finalize-archive-v2-first-seen`
- `build-blockhash-registry`
- `optimize-archive-v2-no-registry`
- `bench-archive-v2`
- `bench-archive-v2-no-registry`
- `build-archive-v2-index`
- `bench-archive-v2-indexed`
- `repack-archive-v2-zstd-blocks`
- `build-archive-v2-hot-blocks`
- `build-archive-v2-hot-blocks-from-live`
- `materialize-archive-v2-live-repair`
- `build-archive-v2-degraded-hot-blocks-from-repair`
- `build-archive-v2-repair-block-access`
- `bench-archive-v2-hot-blocks`
- `bench-car-archive`
- `bench-archive-v2-hot-block-accounts`
- `repack-archive-v2-hot-blocks-raw`
- `build-archive-v2-block-access`
- `build-archive-v2-get-block-index`
- `bench-archive-v2-hot-blocks-raw`
- `dump-usdc-token-events`
- `dump-token-instructions`
- `dump-pumpfun-transactions`
- `extract-largest-archive-v2-hot-block`
- `bench-archive-v2-hot-block`
- `reparse-archive-v2-logs`
- `repack-archive-v2-hot-logs`
- `analyze-archive-v2-logs`
- `analyze-archive-v2-hot-logs`
- `analyze-archive-v2-instruction-data`
- `analyze-archive-v2-hot-instruction-data`
- `inspect-archive-v2`
- `inspect-car-order`
- `find-poh-gaps`

## 3) Public read service commands and endpoints (adjacent protocol)

### `blockzilla-archive-gateway`

Command surface:
- `blockzilla-archive-gateway serve`
- `blockzilla-archive-gateway generate-manifest`

Served routes:
- `GET /healthz`
- `GET /v1/catalog`
- `GET /v1/epochs/{epoch}/manifest`
- `GET /v1/epochs/{epoch}/files/{name}`

### `blockzilla-monitor` (topcoat demo/runtime status UI)

The monitor consumes the scheduler's private `GET /api/v1/status` and
`GET /api/v1/events` endpoints. It validates and projects the snapshot into a
curated browser model; it does not proxy raw scheduler responses.

Runtime API:
- `GET /api/stream` (Datastar/SSE patch stream used by dashboard UI)

## 4) Unix-socket control protocol (proposed v1)

### Decision

Use HTTP/1.1 with JSON bodies over a Unix domain socket, following the same
transport model as Docker. The daemon owns job selection and maps epoch-level
intent to its current internal job kind. Internal names such as
`historical_scan` and `historical_compact_reuse` are not part of the operator
protocol.

This avoids a custom framing or RPC implementation and lets us reuse the
existing Axum control functions, status JSON, SSE patches, HTTP error semantics,
and standard debugging tools such as `curl --unix-socket`.

Default socket:

```text
${XDG_RUNTIME_DIR}/blockzilla/scheduler.sock
```

For a system service without a user runtime directory:

```text
/run/blockzilla/scheduler.sock
```

The socket path is configurable with `blockzilla scheduler --control-socket`
and `blockzilla ctl --socket`. Do not use `/tmp` by default because its
permissions and lifetime are weaker.

### Operator CLI

The public control CLI has three concepts:

```text
blockzilla ctl status [--watch] [--json]
blockzilla ctl scheduler pause
blockzilla ctl scheduler resume

blockzilla ctl epoch show <epoch-or-range>
blockzilla ctl epoch enqueue <epoch-or-range>
blockzilla ctl epoch pause <epoch-or-range>
blockzilla ctl epoch resume <epoch-or-range>
blockzilla ctl epoch retry <epoch-or-range>

blockzilla ctl priority show
blockzilla ctl priority set <epoch-or-range>
blockzilla ctl priority clear
```

Examples:

```text
blockzilla ctl epoch enqueue 1005..1010
blockzilla ctl priority set 794..799
blockzilla ctl epoch retry 761
blockzilla ctl epoch pause 1008..1010
blockzilla ctl status --watch
```

Accepted range syntax is inclusive:

- `761` selects one epoch;
- `794..799` selects epochs 794 through 799;
- comma-separated selectors are deliberately excluded from v1. Multiple
  disjoint changes should be separate commands so audit records stay simple.

`enqueue` means “include these epochs in managed inventory and reconcile them.”
It does not force a particular implementation phase. The scheduler decides
whether an epoch needs acquisition, registry construction, compaction,
finalization, or no work.

### HTTP surface

Only six routes are public on the control socket:

| Method | Path | Mutates | Purpose |
|---|---|---:|---|
| `GET` | `/v1/ping` | no | Protocol/version and daemon identity probe |
| `GET` | `/v1/status` | no | Current scheduler snapshot |
| `GET` | `/v1/events` | no | Sequence-ordered SSE status stream |
| `PUT` | `/v1/scheduler` | yes | Set global paused state |
| `POST` | `/v1/epochs:apply` | yes | Apply one action to an epoch range |
| `PUT` | `/v1/priority` | yes | Set or clear the preferred epoch range |

The small route set is intentional. Adding an internal pipeline phase must not
require a protocol or CLI change. `epoch show` and `priority show` are client-
side projections of `/v1/status`, not additional routes.

#### `GET /v1/ping`

```json
{"protocol":1,"daemon":"blockzilla-scheduler","mode":"execute","pid":1234}
```

#### `PUT /v1/scheduler`

```json
{"paused":true,"expected_sequence":481}
```

`expected_sequence` is optional. A mismatch returns `409 Conflict` without
applying the mutation.

#### `POST /v1/epochs:apply`

```json
{
  "range":{"start":1005,"end":1010},
  "action":"enqueue",
  "expected_sequence":481
}
```

Allowed actions:

- `enqueue`: extend managed inventory to include the range;
- `pause`: stop or suspend current work and persist operator intent;
- `resume`: clear operator pause intent;
- `retry`: clear a retryable failure and reconcile the epoch.

The daemon resolves each epoch to its current acquisition, scan, compact,
finalizer, or repair task. A request never contains a job kind.

Range operations are atomic at the intent layer: either every selected epoch
passes validation and the desired-state update is persisted, or none changes.
Individual jobs may start later as resource admission permits.

#### `PUT /v1/priority`

Set:

```json
{"range":{"start":794,"end":799}}
```

Clear:

```json
{"range":null}
```

Priority is work-conserving preference, not permission to bypass dependency or
resource gates.

### HTTP limits and connection lifecycle

- request body limit: 64 KiB;
- status response limit: governed by the versioned snapshot schema;
- request header and read/write timeouts are bounded;
- ordinary CLI calls use one short-lived connection;
- keep-alive is supported but not required;
- `/v1/events` remains open as an SSE stream and emits heartbeats;
- unsupported content types return `415 Unsupported Media Type`;
- mutation requests require `Content-Type: application/json`.

Example without the Blockzilla CLI:

```bash
curl --unix-socket "$XDG_RUNTIME_DIR/blockzilla/scheduler.sock" \
  -H 'Content-Type: application/json' \
  -X POST http://localhost/v1/epochs:apply \
  --data '{"range":{"start":1005,"end":1010},"action":"enqueue"}'
```

The HTTP host is not an authority or authentication mechanism on this
transport. Unix peer credentials and socket permissions are authoritative.

### Success response

Mutation methods return the persisted intent and resulting sequence:

```json
{
  "changed": true,
  "sequence": 482,
  "accepted": {"start":1005,"end":1010},
  "message": "epochs queued for reconciliation"
}
```

Repeating an already-applied request succeeds with `changed: false`. Mutations
are therefore naturally idempotent; a separate request-id database is not
needed in v1.

### Errors

Use standard HTTP status codes with a stable JSON error body:

| Status | Meaning |
|---:|---|
| `400` | Invalid action, epoch, range, or JSON body |
| `403` | Peer is not authorized or daemon is observer-only |
| `404` | Route or selected epoch is not found |
| `409` | Sequence/state precondition failed or state is not retryable |
| `413` | Request body exceeds the limit |
| `415` | Mutation body is not JSON |
| `500` | Durable mutation or scheduler failure |

Errors include a stable machine-readable code and structured data when safe:

```json
{
  "error":"sequence_conflict",
  "message":"snapshot sequence changed",
  "details":{"expected":481,"actual":482}
}
```

### Permissions and trust boundary

- Create the parent directory with mode `0750` and the socket with mode `0660`.
- Socket owner is the daemon user; group is configurable, normally
  `blockzilla`.
- Authenticate using Unix peer credentials (`SO_PEERCRED` on Linux), not a
  bearer token.
- Allow mutation only in execute mode and only for the owner UID or configured
  control GID.
- Read-only status may use the same restriction in v1. Public monitoring
  continues through the existing HTTP/SSE endpoint and Cloudflare tunnel.
- Refuse to replace an existing non-socket path. On startup, unlink a stale
  socket only after proving it is a socket owned by the daemon UID and that no
  listener accepts a connection.
- Use `umask 007` while creating the socket.

macOS uses `getpeereid`; Linux uses `SO_PEERCRED`. The protocol and CLI remain
portable even though credential acquisition is platform-specific.

### Durable state

Persist operator intent beneath the existing scheduler state root before
acknowledging a successful mutation:

```text
control-state.v2.json
```

The file contains:

- scheduler paused state;
- paused epoch set/ranges;
- managed epoch bounds;
- preferred epoch range;
- schema version and last mutation timestamp.

Write through a temporary file, `fsync`, atomic rename, then directory `fsync`.
Job PIDs and transient phase names are never persisted as operator intent.

### Status streaming

`GET /v1/status` reuses the existing versioned `PipelineSnapshot` schema.

`GET /v1/events` reuses the existing SSE contract:

1. one `snapshot` event;
2. zero or more `snapshot_patch` events;
3. a `resync` event if the receiver lagged beyond retained patches;
4. heartbeat comments when no state changed during the heartbeat interval.

Every data event contains a monotonic scheduler `sequence`. The public TCP
HTTP/SSE listener and local Unix listener consume the same internal broadcast
channel and patch representation.

## 5) Task model and subprocess protocol

### Security boundary

The TCP listener is permanently read-only. It serves health, status, and event
streaming only. It must not mount mutation routes, even on loopback.

The following operations exist only on the Unix socket:

- scheduler pause/resume;
- epoch enqueue/pause/resume/retry;
- priority changes;
- task submission, cancellation, pause/resume, and retry.

This is a router-level separation, not an authorization check inside shared
handlers: construct one read-only TCP router and one local control router. A
future handler cannot accidentally become remotely writable merely because an
authorization flag was misconfigured.

### Operator commands

Common workflows get typed commands:

```text
blockzilla ctl car download <epoch-or-range>
blockzilla ctl archive compact <epoch-or-range>
blockzilla ctl archive verify <epoch-or-range> [--checks <list>]
blockzilla ctl archive upgrade <epoch-or-range> --to <format-version>
blockzilla ctl archive upgrade <epoch-or-range> --to <format-version> \
  --component logs
```

Verification checks:

```text
structure,poh,blockhash,signatures
```

- `structure`: manifest, file lengths, frame decoding, indexes, counts, and
  artifact hashes;
- `poh`: recompute PoH and compare entry/block hashes;
- `blockhash`: verify within-epoch and cross-epoch blockhash continuity;
- `signatures`: cryptographically verify transaction signatures against the
  exact archived message bytes;
- `all`: expand to every supported check for that archive format.

`archive upgrade` is component-aware. A log-codec migration can decode and
re-encode only log artifacts while byte-reusing unchanged components. The
upgrade still publishes a complete new generation atomically; it never mutates
the currently committed generation in place.

Advanced and maintenance workflows use the generic task interface:

```text
blockzilla ctl task definitions
blockzilla ctl task describe <task-name>
blockzilla ctl task list [--state running|failed|complete]
blockzilla ctl task show <task-id>
blockzilla ctl task run <task-name> --epochs <range> [task options]
blockzilla ctl task pause <task-id>
blockzilla ctl task resume <task-id>
blockzilla ctl task cancel <task-id>
blockzilla ctl task retry <task-id>
blockzilla ctl task logs <task-id> [--follow]
```

The typed CAR/archive commands compile to the same task request as `task run`;
they do not contain separate scheduling logic.

### Initial task definitions

| Task name | Purpose | Principal outputs |
|---|---|---|
| `car.download` | Acquire missing canonical CAR input | CAR plus acquisition receipt |
| `archive.compact` | Build current compact generation | Complete atomic archive generation |
| `archive.verify` | Run selected integrity checks | Signed/hash-bound verification receipt |
| `archive.upgrade` | Convert a committed generation | New complete generation plus parity receipt |

Task definitions are allowlisted by the daemon. A socket client cannot submit
an arbitrary executable path, shell command, environment variable, or output
directory. This preserves the Unix socket as a narrow control plane rather
than local remote-code execution.

### Task planning

A submitted range is expanded into per-epoch task instances and dependencies.
The scheduler may run independent instances concurrently.

Examples:

- `car.download(1005)` has no epoch dependency;
- `archive.compact(1005)` depends on its canonical CAR and the predecessor
  blockhash sidecars required by the target format;
- `archive.verify(blockhash,1006)` depends on epoch 1005's committed boundary;
- `archive.verify(signatures,1006)` is independent of epoch 1005 and may run in
  parallel;
- `archive.upgrade(1006,v3)` depends on a successful source-generation
  structural verification and publishes to a new generation directory.

The planner records the expanded DAG before acknowledging submission. Task
identity is derived from the canonical task name, normalized parameters, input
generation hashes, and epoch. Submitting the same work again returns the
existing active or completed task unless `--force` is explicitly supported by
that definition.

### Why a process protocol, not a native ABI

Rust has no stable native ABI between compiler versions. Loading task dynamic
libraries into the daemon would also let a decoder bug corrupt scheduler
memory. Tasks therefore use a versioned subprocess protocol:

- language and compiler independent;
- crash isolated;
- resource measurable per PID/cgroup;
- compatible with existing Blockzilla binaries;
- independently testable and replaceable.

The daemon launches only a resolved executable from its trusted task registry:

```text
<executable> task-worker --protocol 1 --spec-fd 3 --event-fd 4
```

The task specification is JSON read from `spec-fd`. Events are newline-
delimited JSON written to `event-fd`. Human or library diagnostics go to
stderr and are captured as logs; stdout is not part of the protocol.

File descriptors are used instead of temporary spec/event paths so secrets and
partially written events do not leak through the filesystem. Large payloads
remain referenced by validated paths or opened descriptors; they are never
embedded in JSON.

### Task specification v1

```json
{
  "protocol": 1,
  "task_id": "01J...",
  "definition": "archive.verify",
  "epoch": 1006,
  "attempt": 1,
  "inputs": {
    "archive_generation": "/srv/blockzilla/epoch-1006/generations/v2-abc",
    "expected_generation_hash": "sha256:..."
  },
  "output_staging": "/srv/blockzilla/.tasks/01J.../output",
  "checkpoint_dir": "/srv/blockzilla/.tasks/01J.../checkpoint",
  "parameters": {
    "checks": ["structure", "blockhash", "signatures"]
  }
}
```

The worker must treat all paths as fixed capabilities. It may write only to its
staging, checkpoint, and log descriptors. The daemon validates and atomically
publishes declared artifacts after successful completion.

### Event protocol v1

Every event contains:

```json
{
  "protocol": 1,
  "task_id": "01J...",
  "sequence": 42,
  "timestamp": "2026-08-04T16:20:00Z",
  "kind": "progress"
}
```

Event kinds:

| Kind | Required additional fields |
|---|---|
| `started` | `phase` |
| `phase` | `phase`, optional `total` |
| `progress` | `phase`, `completed`, optional `total`, `unit` |
| `checkpoint` | `name`, `digest`, `resumable` |
| `artifact` | `name`, `staged_path`, `bytes`, `digest` |
| `warning` | `code`, `message` |
| `completed` | `summary`, declared artifact names |

Progress units are from a small vocabulary:

```text
bytes,blocks,slots,transactions,signatures,files
```

Workers report monotonic raw counters, not smoothed rates or ETA. The daemon
samples wall time and process I/O, calculates rates/ETA consistently, persists
progress, and publishes it through the common status schema. A worker may
report phase totals late, but totals cannot decrease within an attempt.

Events have a 64 KiB limit. The daemon rejects a non-monotonic sequence,
wrong task ID, malformed event, undeclared artifact, or path outside staging.

### Logs

The daemon captures stderr as UTF-8 lines with these common fields:

```json
{
  "timestamp":"2026-08-04T16:20:00Z",
  "level":"info",
  "task_id":"01J...",
  "attempt":1,
  "epoch":1006,
  "phase":"signature_verify",
  "message":"verified batch",
  "fields":{"signatures":50000}
}
```

Workers may emit structured JSON lines using this shape. Non-JSON stderr is
wrapped as `level=info`, preserving the original text in `message`. Logs are
rotated and bounded by the daemon; task correctness must never depend on log
retention.

### Exit and process-control contract

| Exit | Meaning |
|---:|---|
| `0` | Successful task; declared artifacts still require daemon validation |
| `10` | Retryable input or transient I/O failure |
| `20` | Invalid task specification or unsupported parameters |
| `21` | Integrity verification failed; not automatically retryable |
| `22` | Input generation changed during execution |
| `30` | Resource limit reached; scheduler may retry with different admission |
| `40` | Graceful cancellation acknowledged |
| other | Worker failure; retry policy comes from the task definition |

Process controls:

- `SIGTERM`: request graceful cancellation and checkpoint publication;
- `SIGKILL`: bounded-time escalation after `SIGTERM`;
- `SIGSTOP` / `SIGCONT`: daemon-owned pause/resume on Unix;
- workers must not daemonize, fork untracked children, or escape their process
  group/cgroup.

### Task definition metadata

Each built-in definition declares:

- protocol versions and executable identity;
- typed parameter schema and defaults;
- required input artifact kinds;
- produced artifact kinds;
- resumability and retry policy;
- estimated peak memory and scratch-disk model;
- CPU and I/O scheduling class;
- exclusive resource keys;
- dependency planner;
- artifact validator and publication policy.

Definitions should be Rust data compiled into the daemon for v1. We can add a
signed declarative manifest later, after the validation and trust model is
proven. Avoid executable plugins or user-editable task manifests initially.

### Unix task routes

The generic socket API adds four route shapes:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/v1/tasks` | List definitions or task instances using query parameters |
| `GET` | `/v1/tasks/{id}` | Task status and normalized specification |
| `POST` | `/v1/tasks:submit` | Validate, plan, persist, and enqueue work |
| `POST` | `/v1/tasks/{id}:apply` | Apply `pause`, `resume`, `cancel`, or `retry` |

Task logs use the existing event transport rather than a separate unbounded
HTTP body. `/v1/events` may be filtered by task ID on the Unix listener.

## 6) Migration plan

1. Extract current pause/resume/retry logic from Axum handlers into transport-
   independent control functions.
2. Extract the existing Axum router so it can serve both TCP and Unix listeners.
3. Add `--control-socket` to `blockzilla scheduler`, create the Unix listener,
   and enforce peer credentials and socket permissions.
4. Add `blockzilla ctl` as a thin client with no scheduler business logic.
5. Implement `/v1/scheduler`, `/v1/epochs:apply`, and `/v1/priority` against
   durable desired state.
6. Reuse the current snapshot broadcast channel for `/v1/status` and
   `/v1/events`.
7. Mount mutations only on the Unix router; keep TCP status/SSE read-only.
8. Remove HTTP mutation routes, `--management-bind`, Host/Origin checks, and
   the management bearer token.
9. Add the task registry, subprocess worker adapter, and generic task routes.
10. Convert download, compact, verify, and upgrade workflows one at a time to
    the task protocol.

There is no compatibility period where write routes remain reachable over TCP.
The old job-kind HTTP controls are removed when Unix mutations ship.
