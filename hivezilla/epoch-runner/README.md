# Local epoch capture and compaction

This Rust service captures finalized Yellowstone blocks from the local
non-voting node. A separate worker builds local Compact V2 execution archives.
It writes a durable checkpoint and a small dashboard progress report. It uses
the existing native Hivezilla capture and normalization commands.

Run with an absolute config path:

```sh
hivezilla-epoch-runner --config /absolute/path/config.json
```

The config defines the epoch, its exact mainnet slot range, validator identity,
genesis hash, binary paths, run directory, progress file, batch size, and free
space reserve. Mainnet epochs must have 432,000 slots. The default batch has
128 blocks. The default disk reserve is 500 GiB. One process owns a run through
an exclusive file lock.

Capture starts near the epoch boundary. Each batch has a sealed raw WAL, an
inventory, the local finalized `getBlocks` response, and an exact coverage
receipt. A chain skip is a slot with no block in that local ledger. A missing
produced block is a capture gap. The cursor does not advance over a gap.
Failed attempts and their logs remain in the run directory.
The current recorder groups disk saves while preserving the frame format and
full final seal. Only a successful sealed batch advances the saved cursor.

The saved checkpoint has separate collected and compacted counts. Restart
resumes at the saved slot and retries pending compaction. The current plugin
keeps only 512 slots for replay, about two minutes at the observed slot rate.
An outage longer than that window can prevent a complete epoch. This runner
does not recover the original Geyser data from historical RPC blocks. Gap
checks use the local node; they are not an independent mainnet coverage proof.

Compaction retains sealed raw evidence and uses hard links for matching raw
copies. It removes only its own disposable normalization directory after
compaction succeeds. Compact byte counts exclude the shared raw WAL.

Archive read-back verification is skipped as the user requested. Source
integrity and writer counts still have checks. Outputs explicitly have
`archive_verified: false`, `canonical: false`, and `publication_ready: false`.
No R2 upload runs. These local execution archives do not grant canonical epoch
publication authority.

Deployment files and evidence for epoch 1047 are in
[the deployment directory](../deploy/epoch-runner/README.md). The separate
64 GiB raw-shred test is bounded and does not capture this full epoch.
