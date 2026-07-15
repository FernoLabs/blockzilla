# Live epoch rotation and bounded finalization

Date: 2026-07-12

## Change

- A resumed `capture-grpc` now reads the epoch already present in its journal and refuses to append a block from a later epoch when `--stop-at-epoch-boundary` is enabled.
- The NAS supervisor treats `BLOCKZILLA_LIVE_CAPTURE_DIR` as the initial directory only. After a completed capture it resumes at `last_slot + 1` in a new epoch-specific directory.
- Closed-capture inspection and finalization run outside the ingest handoff. The supervisor reads `last_slot` from the completed capture report, starts the next epoch loop immediately, and inspects the immutable prior capture in a background worker.
- The NAS also runs `nas-continuous-grpc-recorder.sh` as an epoch-agnostic durable safety spool. It never passes `--stop-at-epoch-boundary`, so indexing, inspection, and packaging cannot all stop gRPC consumption at the same time.
- Bounded live finalization copies only the selected blockhash and PoH prefixes instead of copying whole mixed sidecars.
- A bounded finalization can explicitly use a bounded `pubkey-runs` directory, retaining the disk-backed low-memory registry merge.

## Epoch 1002 incident

The epoch-1001 capture closed at slot `432863999`, after which the old supervisor synchronously ran `inspect-capture`. The inspection blocked in kernel I/O, so epoch 1002 did not subscribe until slot `432865473`. Slots `432864000` through `432865472` are recorded as an explicit RPC-repair range.

The supervisor ordering above removes inspection from the capture critical path. A one-shot hot handoff activates the updated supervisor after the current capture child exits naturally, avoiding a forced termination of buffered archive writers during rollout. The continuous raw spool provides boundary-independent recovery coverage during that handoff and future archive rotations.

## Epoch 1000 recovery

The immutable compact-v2 capture contains 264,316 epoch-1000 streamed blocks through slot
432431999 followed by an epoch-1001 tail. Recovery uses a zero-copy hard-linked view and two
bounded pubkey-run workers. The abandoned full block rewrite was stopped and its derived output
removed.

The 76 RPC repairs for slots 432378275 through 432378350 remain an explicit publication gate.
They must be integrated into the canonical stream or shipped and documented as repair sidecars;
they must not be silently treated as streamed blocks.

## Verification

- `cargo fmt --all -- --check`
- `cargo test -p blockzilla-live-producer resumed_capture_keeps_epoch_from_existing_journal --release`
- `cargo check -p blockzilla --bin blockzilla --release`
- Release build of `blockzilla-live-producer` and `blockzilla` on the NAS optimization worktree

Production rollback binaries and supervisor script are under
`backups/live-epoch-rotation-20260712T183117Z` in the NAS production repository.
