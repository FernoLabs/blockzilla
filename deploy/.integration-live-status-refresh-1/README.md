# Live capture status refresh (not deployed)

This isolated patch makes Hivezilla derive live counters from the append-only gRPC journal when a producer/supervisor progress snapshot is stale.

## Root cause

Hivezilla prefers any `<capture>/progress.json` over `<capture>/journal/progress.json`. It previously used the latest complete `grpc-blocks.jsonl` row only when `blocks_done` was zero or `last_slot` was absent, so a valid nonzero root snapshot permanently masked later journal growth.

The current repository supervisor writes the root snapshot at child start and exit. The observed NAS supervisor is a still-running deleted older script, however, and the frozen root snapshot came from the rollout/backfill rather than a periodic writer. The hotfix does not depend on which process created the shadowing file.

The observed capture demonstrated that exact split: the API stayed at 7,716 blocks and slot 432,663,030 while PID 12591 remained alive and the journal advanced through block IDs 48,542 and later 49,799.

## Patch

Apply `live-status-refresh.patch` from the repository root.

- Preserve richer snapshot fields such as phase, state, PID, first slot, and producer rate fields.
- Monotonically merge `block_id + 1`, slot, and journal mtime from the newest complete journal row. A stale or reordered row can never reduce an API counter.
- Ignore an incomplete trailing JSON row and use the preceding complete row.
- Restrict every status poll to at most 128 KiB from the journal tail. `Read::take` prevents `read_to_end` from chasing a file that is concurrently growing at many MiB/s.
- Preserve terminal-state semantics: a `closed` or `stopped` snapshot is still terminal; journal freshness only refreshes timestamps and counters.

## Verification

- `cargo check -p blockzilla-hivezilla --all-targets` passes in an isolated worktree.
- Both new tests pass: stale nonzero snapshot plus partial trailing row, and monotonic no-regression when the snapshot is newer.
- Full isolated Hivezilla suite: 51 passed, 0 failed.
- `rustfmt --check` and patch whitespace/application checks pass.

## Promotion gate

This patch has not changed or restarted the NAS service. Before deployment, apply it to the exact frozen controller source, rerun the Linux suite, rebuild Hivezilla, and verify two successive API polls advance `blocks_written`, `last_slot`, and `updated_unix_secs` while journal memory remains bounded.

Longer term, the supervisor should stop publishing a root progress file that shadows the producer's periodic journal snapshot, or update it periodically. The journal merge remains useful as the crash-tolerant source of truth even after that cleanup.

The narrow hotfix refreshes `blocks_written`, `last_slot`, and `updated_unix_secs`. It deliberately preserves root-derived `progress_pct`, rate, ETA, and transaction fields, so a UI progress bar that prioritizes those fields can remain stale. A follow-up should select the freshest valid producer snapshot and define one canonical epoch-progress formula before changing those derived values.
