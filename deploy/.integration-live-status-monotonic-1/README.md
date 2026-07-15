# Live status monotonic refresh

Date: 2026-07-13

## Incident

The live producer remained healthy, but Hivezilla exposed a frozen live card. The active producer
was advancing its append-only `journal/grpc-blocks.jsonl`, while a nonzero root `progress.json`
remained fixed at 7,716 blocks and slot 432,663,030. Hivezilla selected the root file first and only
consulted the journal when the snapshot counters were absent or zero.

## Fix

- Select the freshest valid snapshot from root and `journal/progress.json`.
- Monotonically merge the latest complete journal row into `blocks_done` and `last_slot`.
- Recompute epoch progress from the merged slot and use journal mtime for freshness.
- Ignore a partially written trailing journal row and fall back to the preceding complete row.
- Limit every journal-tail read to 128 KiB, including when the file grows during the read.
- Preserve terminal `closed` and `stopped` states; fresh journal data cannot reopen a capture.

The same behavior and regression tests are present in
`crates/hivezilla/src/nas_pipeline.rs` so a future integration does not lose the repair.

## Validation

- Local focused regressions: 3 passed.
- Local `cargo check -p blockzilla-hivezilla --all-targets`: passed.
- Linux/NAS Hivezilla suite: 83 passed, 0 failed.
- Isolated smoke test against the real live root reported 51,534 blocks and slot 432,706,868,
  compared with the stale production values of 7,716 and 432,663,030.
- After rollout, two production API samples advanced from 52,452 / 432,707,786 to
  52,485 / 432,707,819 in ten seconds.
- Live producer PID 12591 was not signaled or restarted.
- Compaction PIDs 217921, 284509, and 348612 were unchanged across the final controller swap.

## NAS rollout

- Release:
  `/volume1/@home/ach/dev/blockzilla-pipeline/releases/blockzilla-nas-pipeline-2026.07.13-live-status-monotonic-2`
- Hivezilla SHA-256:
  `dcaba48b3dbe4219d0669fee5f6982bc87a9f59f413881314318b0c8bd772100`
- Integrated source SHA-256:
  `f7674b78e2d327d414de3a90e40e04afa4b1dff0bde3d6709253b38c6048a7d8`
- Controller at rollout: PID 354358, bound to `0.0.0.0:8787`.
- Rollback release retained:
  `blockzilla-nas-pipeline-2026.07.13-live-status-monotonic-1`.

The active producer is an older generation that does not periodically publish
`journal/progress.json`. Keep it running until the epoch boundary; the journal-tail merge is the
safe compatibility path. Deploy the current producer/supervisor generation only at a clean capture
boundary, then verify atomic progress publication and reconnect behavior.
