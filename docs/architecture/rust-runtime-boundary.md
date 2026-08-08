# Rust runtime boundary

Blockzilla and Hivezilla product runtime behavior belongs in Rust crates and
binaries in this Cargo workspace. This keeps state transitions, validation,
resource admission, crash recovery, telemetry, and durable publication under
one type system and one test/build toolchain.

## Required in Rust

The following behavior must be implemented in workspace Rust code:

- long-lived services and polling loops;
- retry, backoff, timeout, cancellation, and child-process supervision;
- filesystem validation, locking, crash recovery, and atomic publication;
- custody, receipt, retention, and deletion authority;
- scheduler admission, dependency discovery, and task reconciliation;
- public-status selection, redaction, and bounded HTTP serving;
- runtime process discovery and telemetry;
- archive acquisition orchestration and post-download validation; and
- any parser or validator whose result can admit, publish, replay, or delete
  production data.

## Deliberate external tools

Maintained external tools may remain behind a narrow Rust adapter:

- `aria2c` performs resumable CAR byte transfer. Rust owns its arguments,
  retries, lifecycle, progress interpretation, preflight, fsync, and no-clobber
  publication.
- `cloudflared` owns the Cloudflare Tunnel protocol and lifecycle below its
  systemd unit.
- systemd owns host service activation and cgroup policy.
- OpenSSL may be used by offline provisioning scripts for PKI generation.
- browser applications remain TypeScript/Svelte and are deployed as static
  production assets; a Node/Vite development server is not a production
  runtime dependency.

External tools never become sources of product authority merely because they
exit successfully. Rust must independently validate every result used by the
runtime.

## Script policy

Maintained custom logic is Rust even when it is an offline benchmark,
migration, administration command, or incident tool. Those commands belong in
the relevant product CLI or a small workspace `xtask` crate so they reuse the
same parsers, schemas, validation, and tests as the runtime.

Shell is limited to declarative host launchers that stage systemd-provided
configuration and finish with `exec`. Python and larger shell programs are
temporary migration inputs only: they may remain as differential-parity
oracles until their Rust replacement passes the migration gate, then they are
deleted. New product, benchmark, administration, and operational behavior must
not be added to them.

No production systemd service or timer may execute a custom Python program or
a custom long-running shell state machine.

## Migration rule

Each replacement must preserve the old implementation as a read-only test
oracle until fixtures cover success, malformed input, interruption, retry,
counter regression, PID reuse where relevant, no-clobber publication, and
crash-recovery boundaries. Production cuts over only after exact output or
semantic parity. The superseded runtime script and its deployment references
are then removed rather than maintained indefinitely in parallel.

## Migration status and order

Completed:

1. Verified predecessor-tail discovery/publication is a Blockzilla subcommand.
2. Runtime process telemetry runs inside `blockzilla-monitor`.
3. CAR acquisition orchestration is Rust-owned while `aria2c` remains the byte
   transfer engine.
4. `blockzilla-monitor` is the curated public status boundary and reads the
   scheduler's private status listener directly.
5. The bounded shred-status collector is `hivezilla serve-shred-status`; its
   Docker and Compose entrypoints no longer execute Python. The frozen Python
   implementation remains only as a CI parity oracle until the first live
   container rollout is verified.

Remaining, in priority order:

1. Add production S3-compatible and Backblaze adapters to
   `hivezilla-object-store`, then move recorder upload/retention orchestration
   into Hivezilla.
2. Reuse the bounded Rust shred-status service core for the ingest collector.
3. Move scheduler incident recording and ACK alert decisions to typed Rust
   events while preserving an independent failure-observation boundary.
4. Move maintained replay harnesses, archive administration, reference-corpus
   collection, and benchmark utilities into product subcommands or a workspace
   `xtask`; retire their Python and shell predecessors after fixture parity.

The largest transitional sources are tracked explicitly so they cannot be
mistaken for approved permanent exceptions:

- `services/hivezilla/scripts/linux-raw-grpc-recorder.sh` and
  `s3_multipart_upload.py` still own recorder supervision and object-store
  custody. They move only after production S3/R2/B2 adapters exist in Rust.
- `ingest_status_server.py` and `pull_ack_telegram_monitor.py` still contain
  long-running status or alert loops. `shred_status_server.py` is frozen and
  retained only as the temporary differential-parity oracle for its Rust
  replacement; it is no longer a production entrypoint.
- Scheduler incidents still need a typed durable producer and bounded reader.
- replay-marathon, Compact sync, archive-retirement, RPC-corpus, and benchmark
  scripts remain migration inputs for product subcommands or `xtask`.

Thin launch wrappers that only validate configuration and finish with `exec`
may remain shell. Any wrapper that retries, polls, mutates durable state, makes
retention decisions, or supervises a child is not thin and must migrate.
