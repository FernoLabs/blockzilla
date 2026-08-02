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
configuration and finish with `exec`. Larger shell programs are temporary
migration inputs only: they may remain as differential-parity oracles until
their Rust replacement passes the migration gate, then they are deleted. New
product, benchmark, administration, and operational behavior must not be added
to them. Custom Python source and embedded Python execution are forbidden in
the maintained tree and rejected by CI.

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
2. Runtime-operations collection is a Rust watcher-gateway subcommand.
3. CAR acquisition orchestration is Rust-owned while `aria2c` remains the byte
   transfer engine.
4. The public status boundary is the Rust watcher gateway; its superseded
   Python implementation has been retired.
5. The bounded shred-status collector is `hivezilla serve-shred-status`; its
   superseded Python implementation and tests have been retired.
6. The bounded ingest-status collector is `hivezilla serve-ingest-status`; its
   superseded Python implementation and tests have been retired.
7. The watcher gateway now owns the bounded block-time-gap status projection
   and typed private scheduler incident recorder; their Python implementations
   and tests have been retired.
8. Durable receiver-ACK warning and recovery alerts are emitted by
   `hivezilla monitor-pull-ack-telegram`; the Python monitor and tests have been
   retired.
9. Immutable S3/R2/B2 generation upload, provider-native verification,
   account-usage reporting, and crash-safe R2 retention are implemented by
   `blockzilla-s3-upload`; the Python uploader and tests have been retired.
10. Recorder support parsing, inherited-descriptor locks, generation scans,
    and read-only receiver progress snapshots are Rust-owned through
    `hivezilla raw-recorder-support`; recorder scripts contain no embedded
    interpreter programs.
11. RPC epoch benchmarks and correctness-corpus comparisons are native
    `blockzilla-get-block` binaries; their Python predecessors have been
    retired.

Remaining, in priority order:

1. Move the remaining recorder supervision state machine into Hivezilla and
   reduce `linux-raw-grpc-recorder.sh` to a declarative launcher.
2. Move the remaining maintained replay and Compact orchestration shell
   programs into product subcommands or a workspace `xtask` after fixture
   parity.

The largest transitional sources are tracked explicitly so they cannot be
mistaken for approved permanent exceptions:

- `services/hivezilla/scripts/linux-raw-grpc-recorder.sh` still owns recorder
  supervision, while its custody operations and security-sensitive parsers are
  native Rust commands.
- replay-marathon and Compact synchronization scripts remain migration inputs
  for product subcommands or `xtask`.

Thin launch wrappers that only validate configuration and finish with `exec`
may remain shell. Any wrapper that retries, polls, mutates durable state, makes
retention decisions, or supervises a child is not thin and must migrate.
