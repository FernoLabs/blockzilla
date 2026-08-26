# Changelog

## 0.2.0 — unreleased

**Breaking: the published archive format changes.** Archive V2 becomes a
frozen legacy input and a read-only serving artifact. New generations are
published in the Blockzilla Index Archive format.

### Why this is a version boundary

Archive V2 is row-major, so filtering by account requires decoding the whole
archive, and a V0 transaction's own account list is only reachable by walking
into transaction status metadata. Building one reverse index costs a full
archive re-decode per 8M-account chunk. Replay pays for it too: messages and
metadata share one zstd frame, so replay decompresses ~2.8x the bytes it uses
on a modern block.

### What changes for consumers

- **Layout.** A generation is column planes, not block blobs: `ledger/` holds
  signed truth (core rows, resolved accounts, top-level instructions,
  instruction data, lookup descriptors), `runtime/` holds runtime output as one
  sidecar per kind (inner instructions, outcomes, balances, token balances,
  logs, return data, rewards), `indexes/` holds derived posting lists.
- **Resolved V0 loaded addresses are a first-class column**, so account
  filtering no longer decodes metadata.
- **Versioning.** One `format_major`, carried in the manifest and in every file
  header. No per-file version constants, no version-suffixed Rust types, and no
  trial-decoding: every object identifies its role and schema before its
  payload is read.
- **Archive V2 is retained as the cloud serving artifact** for get-block, where
  one range read per block matters. It is derived, not canonical.
- **Integrity vs validation are separated.** File digests are storage
  integrity, established once at publication after the PoH gate passes. They
  are not chain validation, and no serving read path recomputes them.

### Migration

Conversion is an offline column transform of existing V2 generations — no CAR
re-ingest — proven by reconstructing V2 blob bytes from the new columns and
comparing digests. Publication rides the existing `FiniteWorkKindV1`
compaction fence and CAS catalog head. Both formats are immutable and complete,
so they coexist by epoch and rollback is a catalog-head move.

See [docs/design/blockzilla-index-archive.md](docs/design/blockzilla-index-archive.md).

## 0.1.0

Archive V2 and everything before this changelog existed.
