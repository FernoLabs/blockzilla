# Blockzilla watcher gateway

`blockzilla-watcher-gateway` is the bounded Rust public boundary for the
Blockzilla watcher. It lives in the main Cargo workspace but runs as a separate
process, so public HTTP traffic cannot interrupt archive scheduling or replay.

It supersedes the former Python proxy while preserving the explicit public
endpoint and header allowlists, strict ingest-status projection, JSON and SSE
redaction, body limits, private-address checks, and secret-free `502` responses.
Static watcher assets are streamed without buffering.

Run it against local-only watcher upstreams:

```console
cargo run --release -p blockzilla-watcher-gateway -- serve \
  --listen 127.0.0.1:8787 \
  --upstream 127.0.0.1:8786 \
  --ingest-upstream 127.0.0.1:8790
```

Test the contract with:

```console
cargo test -p blockzilla-watcher-gateway
```

Publish bounded runtime operations directly from Rust:

```console
cargo run --release -p blockzilla-watcher-gateway -- \
  publish-runtime-operations \
  --output /path/to/ui/api/v1/sidecars/runtime-operations/status.json \
  --interval-secs 5
```

Use `--once` for a single atomic publication. The collector samples only
same-user processes, keys counter history by both PID and process start time,
redacts command lines and paths from its output, and bounds published process
and job lists. Install the checked-in
`systemd/blockzilla-watcher-runtime-operations.service` as a user unit so the
publisher has the same Unix identity as the observed jobs. The former Python
publisher was retired after fixture and live-output parity.

Publish the block-time-gap backfill's public progress document from its
private scheduler status:

```console
cargo run --release -p blockzilla-watcher-gateway -- \
  publish-block-time-gap-backfill \
  --source /private/scheduler/block-time-gap-backfill.json \
  --output /served/ui/api/v1/sidecars/block-time-gaps/status.json \
  --interval-secs 15
```

Only typed counters and states cross this boundary. Free-form errors and
progress text are reduced to safe labels, the source is read through a
size-bounded non-following regular-file descriptor, and publication is an
atomic, synced rename. Use `--once` for one projection. The checked-in
`systemd/blockzilla-watcher-block-time-gap-backfill.service` replaces the
former Python publisher.

Record private scheduler incidents without giving the observer any control
authority:

```console
cargo run --release -p blockzilla-watcher-gateway -- \
  record-scheduler-incidents \
  --backfill-status /private/scheduler/block-time-gap-backfill.json \
  --control-events /private/scheduler/control-events.jsonl \
  --priority-lease /private/scheduler/priority-lease.json \
  --state-file /private/incidents/state.json \
  --events-output /private/incidents/events.jsonl
```

The recorder preserves PID/start-time process identities, `/proc` CPU, RSS,
I/O and blocked-state sampling, scheduler control-event rotation cursors,
priority-lease attribution, and the bounded pre-transition ring. It never
reads command lines, never signals a process, caps every input and retained
collection, and writes checkpoint/incident files as private `0600` data. The
checked-in `systemd/blockzilla-watcher-scheduler-incidents.service` is intended
to run as the same user as the scheduler. Do not serve its JSONL directly;
only a separate typed public projection may cross the watcher boundary.
