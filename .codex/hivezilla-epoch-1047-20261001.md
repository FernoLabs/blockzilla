# Active epoch run

User authorized a full epoch run and dashboard progress for collected and
compacted blocks. Epoch 1047 is armed for slots 452304000 through 452735999.
Do not restart the validator. Do not commit, push, delete benchmark data, or
start more parallel agents. Use short ASD-STE100 messages.

Remote: ubuntu@213.239.141.61. Root:
`/home/ubuntu/live/hivezilla-validator-20260930`.

- Validator: `hivezilla-validator-canary.service`, PID 590165 at deployment.
- Collector: `hivezilla-epoch-1047.service`, PID 606063 at deployment.
- Dashboard: `hivezilla-epoch-dashboard.service`, PID 607874 after UI update.
- Services are enabled, active, and have no runtime deadline.
- Collector config: `epoch-runs/epoch-1047-20261001/config.json`.
- Checkpoint: same directory, `checkpoint.json`.
- Progress: `reports/epoch-progress.json`.
- Original release: `releases/epoch-runner-20261001`, runner and Blockzilla.
- Dashboard release: `releases/epoch-dashboard-r2-20261001`.
- Recorder update: `releases/capture-reader-r1-20261001`. Installed atomically
  over `bin/hivezilla` at 14:01:10 UTC on October 1. No validator or collector
  restart. Exact old binary remains as `hivezilla-before` in that release.

Capture has 128 finalized blocks per batch, a 1 GiB batch cap, and a 500 GiB
free-space reserve. Compaction runs separately. The cursor advances only
after a sealed exact local-RPC coverage check. A capture gap preserves the
cursor and is reported. Replay is limited to 512 slots; a long outage can
prevent a complete epoch. Compaction hard-links matching raw evidence copies
and removes only its own disposable normalized working directory.

Archive read-back verification is skipped per user instruction. Source
integrity and writer counts remain checked. Outputs are unverified,
noncanonical, and not publication-ready. R2 upload is disabled. Bucket,
prefix, and credential path remain unknown. No upload was attempted.

The separate raw-shred recorder has a 64 GiB root limit. It will not capture
the full epoch. Do not describe the gRPC run as a full raw-shred capture.

Six runner tests passed. Seven dashboard tests passed, including independent
collected/compacted counters and rejection of false completion. Real
preflight normalized and compacted 128 blocks; a separate replay probe
started at the exact requested slot. The first actual epoch batch passed:
128 collected and compacted, slots 452304000 through 452304127, with zero
missing blocks. Shared raw inode and removal of disposable normalized work
were checked. Capture had two boundary retries before it recovered. Evidence
is `hivezilla/deploy/epoch-runner/evidence/epoch-1047-first-batch-20261001.json`.

Repeated gRPC `lagged to send an update` errors were caused by a full client
send queue while the reader synced every entry to disk. The recorder now
groups syncs at blocks, 1 MiB, or an append-time 100 ms interval, and sets
32 MiB adaptive HTTP/2 receive windows. The full synced seal and frame format
are unchanged. Fourteen focused current-capture tests passed. Same 128 replay
blocks and 168,468 entries took 18.81 seconds before and 8.03 after, including
startup and seal. Evidence and deployment hashes are in the epoch deployment
directory. Post-install check at 14:03:16 UTC: three new sealed batches,
two already compacted, no new retries, no capture or compaction error,
and no missing blocks in the saved range. Total 4,864 collected and 4,736
compacted, 10 local-ledger skipped slots, lag 62 slots. Both process IDs remain
unchanged. See `evidence/capture-reader-after-20261001.json` for the fixed
snapshot; read live progress for current status.

The UI is at http://127.0.0.1:8766/ through a local SSH port forward. Remote
dashboard binds to loopback. No new public access was created. The original
in-app browser tab had a control timeout; a fresh tab is the dashboard output.

Project docs/evidence: `hivezilla/deploy/epoch-runner/README.md` and its
`evidence/` directory. Evidence files are fixed snapshots, not current status.
No local Cargo builds: use remote Rust 1.98, shared cache at
`/home/ubuntu/benchmarks/zilla-triton-20260930/source/target`, native RUSTFLAGS,
16 build jobs, and a bounded build service. The original isolated release
source and Cargo.lock remain in its remote release directory.

## Compaction recovery, 20:13 UTC on October 1

Chunk 230 normalized blocks were 539,625,154 bytes, above the old 512 MiB
input bound. Compaction retried without advancing and retained a new derived
normalization per attempt. Input files now have a separate 1 GiB bound;
the 8 GiB input total and 512 MiB publisher output bounds remain unchanged.
The exact blocked batch completed after an atomic compactor replacement.
Five compactor tests and six runner tests passed remotely.

Runner cleanup now preserves the normalization receipt and logs on a builder
error and removes disposable normalized data. Installed runner SHA256:
`93e534a3398e504d7a7302f1b530c47d9852dec10f5cd0e0e5cf9e70011ff05c`.
Installed compactor SHA256:
`40e0f200e3bb851badf540c3542a7f1fbb12cab3914f7f5262027e3b5e2f5d7d`.
Old binaries and cleanup report are in remote release
`releases/compact-bound-r2-20261001`. The collector alone restarted at
20:12:33 UTC, now PID 664124; validator PID 590165 and tunnel PID 480183
remain unchanged. One duplicate replay attempt was rejected, then capture
advanced with no error and no missing blocks in the saved range.

After the blocked chunk completed, 566 failed derived copies were removed,
with 314,259,931,136 bytes recovered. Raw captures, logs, and normalization
receipts remain. One full failed sample is retained at chunk 230,
`work-1790884897164-606063/normalized`. Latest checked snapshot at
20:13:47 UTC: 87,552 collected, 30,592 compacted, both advancing,
no capture/compaction error, missing blocks 0, RPC health ok.
Compaction still has a backlog; read live counters before reporting status.
Evidence: `compact-bound-after-20261001.json` and
`compact-bound-cleanup-20261001.json` in the deployment evidence directory.
