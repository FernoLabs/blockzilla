# Terminal-only block-time gap backfill

Date: 2026-07-18

## Incident

The standalone block-time gap backfill wrote derived timestamp artifacts into
legacy registry-only epoch directories. Those directories are valid inputs for
compact/reuse, but the deployed scheduler did not yet recognize the new
derived files and conservatively classified 282 epochs as ambiguous output.

The scheduler fix in `e4e278e` recognizes only the canonical gap sidecar and
persistent lock file. This companion backfill hardening prevents the standalone
extractor from touching migration candidates at all.

## Candidate contract

`build-manifest.sh` derives its complete candidate universe from the current
NAS pipeline status snapshot. Every candidate must:

- have scheduler state `complete`;
- publish nonempty `metadata`, `blocks`, and `block_index` artifacts in status;
- retain nonempty `archive-v2-meta.wincode`, `archive-v2-blocks.zstd`, and
  `archive-v2-blocks.index` files at the canonical epoch output path.

The manifest includes epochs with existing sidecars. The runner verifies those
sidecars and records receipts, keeping totals consistent after a manifest
rebuild. Immediately before any new extractor starts, the runner checks the
same terminal state and reader-core files again. It continues to use
`--source archive`; registry/V3 fallback is not permitted.

## Resource policy

The runner remains a singleton and starts at most one extractor. Children use
nice level 19 and best-effort I/O priority 7. Admission requires healthy live
capture, at least 3 GiB available memory, load at most 8, and I/O full pressure
at most 10%. Critical pressure pauses the active extractor. Multiple gap lanes
are intentionally unsupported because a real extraction used substantial
page cache and competed with the archive scheduler's fifth compact lane.

## Rollout validation

- Manifest: 641 terminal-complete epochs.
- Intersection with queued, scanning, blocked, or failed epochs: zero.
- Missing/nonempty reader-core validation failures: zero.
- Existing receipt outside the rebuilt manifest: zero.
- Gap runner and both watcher sidecar publishers are supervised by systemd.
- Public watcher sidecar reported epoch 302 with 348/641 epochs accounted for.

