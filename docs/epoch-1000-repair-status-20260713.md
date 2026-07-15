# Epoch 1000 repair status

Date: 2026-07-13
Updated: 2026-07-14

## State

Epoch 1000 is recoverable as a complete produced-block set, but it is not a canonical green
archive. The retained live captures contain complete PoH for 417,550 produced blocks. Regular
RPC can recover the remaining 14,231 produced blocks, but those RPC-only blocks permanently lack
the original PoH entries and shred-boundary metadata unless a CAR or another lossless event source
becomes available.

The repair must therefore remain `REPAIR-REQUIRED`; it must never be presented as `READY` or as a
canonical fully reconstructed epoch.

This permanent limitation applies to PoH and shredding, not to direct block access. A degraded hot
archive can contain the full produced-block archive and all three block-access files while keeping
`canonical=false` and `publication_ready=false`.

## Authoritative coverage

A finalized `getBlocks(432000000..432431999)` snapshot reported:

- produced slots: 431,781
- RPC-unlisted candidate skipped slots: 219
- first produced slot: 432,000,000
- last produced slot: 432,431,999
- compact JSON slot-list SHA-256:
  `7ff632a0966651a2914771127d5ece10f1e7c4a185c04276ed67d3db9f3b92b9`

The snapshot is retained at:
`capture-20260711T061451Z-compact-v2-live/repair/rpc-get-block/run-epoch1000-full-20260713T2245CEST/authoritative-produced-slots.json`.

The 14,231 live-capture misses are:

- epoch head `432000000..432011618`: 11,609 produced blocks and 10 skipped slots;
- `432032496..432032574`: 79 produced blocks;
- `432060360..432062826`: 2,467 produced blocks;
- `432378275..432378350`: 76 produced blocks.

There are no other produced-slot gaps.

## Retained live sources

The exact non-overlapping live union is:

1. `capture-20260710T123110Z-compact-v2`
   - retain exactly 153,234 block/index/PoH rows;
   - slots `432011619..432167486`;
   - ignore the longer shared PoH/journal tail after row 153,233.
2. `capture-20260711T061451Z-compact-v2-live`
   - retain exactly the first 264,316 rows;
   - slots `432167487..432431999`;
   - the same files continue into epoch 1001, so the row-count boundary is mandatory.

Together these sources provide 417,550 live blocks with no overlap at the audited handoff. Their
selected PoH records must be rewritten into the final produced-slot ID namespace; the second
capture's source-local IDs restart at zero.

## RPC recovery

RPC JSON sidecars are consolidated under:
`capture-20260711T061451Z-compact-v2-live/repair/rpc-get-block/epoch-1000`.

The two head workers use disjoint slot ranges. Their run writes `COMPLETE` only after both workers
exit and both reports succeed. Do not build or publish a repair view before that marker exists and
the directory contains exactly 14,231 unique authoritative produced slots.

The existing 2,391 files in the large internal gap were deeply validated before reuse. The
76-file gap was also validated. Existing files were hard-linked into the consolidated directory so
the already recovered payloads were not downloaded again.

Every final RPC file still needs a bounded parse, base64 transaction decode, blockhash and
previous-blockhash validation, parent-chain validation, and exact authoritative-slot membership
check. RPC-only blocks must explicitly carry both `MissingPoh` and `MissingShredding` state.

## Atomic repair view

`blockzilla-live-producer prepare-epoch-repair` prepares a same-filesystem view without copying the
two normalized block blobs. It must:

- require sealed/quiescent capture receipts and the RPC run `COMPLETE` marker;
- retain exact per-capture row counts and cutover slots;
- validate input identity/durability across parse, hard-link, and publication;
- assign produced-slot ordinals across both live and RPC-only blocks;
- rewrite retained PoH IDs, leaving explicit RPC-only gaps;
- include every produced blockhash in produced order;
- hard-link the immutable normalized block files, bounded live pubkey runs, and RPC JSON;
- publish by one final directory rename only after all checks pass;
- emit `REPAIR-REQUIRED.json`, never `READY`.

The normalized block frames still contain source-local blockhash IDs. A future materializer must
rewrite both current and previous blockhash references before the current hot finalizer can consume
them. The retained pubkey runs cover live blocks only; the materializer must also extract and merge
account-key usage from RPC-only transactions/meta/rewards before building a complete registry.

## Published repair bundle

The guarded builder completed and atomically published:
`/volume1/@home/ach/dev/blockzilla-live/epoch-1000-union-repair-view-20260713`.

The published `REPAIR-REQUIRED.json` has SHA-256
`d79a529208ede2ddcab2f69ce27f5e957dbeb3945bb502d9272ae792c9dc0007` and records:

- 417,550 live blocks and 14,231 RPC-only blocks, for 431,781 produced blocks;
- 431,781 produced blockhash records and no duplicate live blocks;
- 417,550 PoH records containing 372,527,506 entries, with the 14,231 RPC-only produced IDs
  represented as explicit PoH gaps;
- 1,443 retained pubkey-run files containing 205,351,260 records; and
- first/last produced slots 432,000,000 and 432,431,999.

An independent streamed validation merged the live plan with every RPC-only manifest entry and
matched all 431,781 positions against the retained finalized slot snapshot. It also checked the
produced-ID and parent chain, both normalized-block hard links, all 14,231 RPC hard links, every
pubkey-run file, the blockhash-file length, the input receipts, and the intentional absence of a
`READY` marker. The 12.96 GB PoH payload did not need to be loaded into memory for this coverage
proof.

## Degraded hot archive and block access

The repair workflow now has a third guarded phase after hot compaction:

    blockzilla build-archive-v2-repair-block-access \
      /volume1/@home/ach/dev/blockzilla-live/epoch-1000-union-repair-view-20260713 \
      /volume1/@home/ach/dev/blockzilla-v2/epoch-1000

The phase builds and validates:

- `archive-v2-block-access.wincode`;
- `archive-v2-block-access.index`; and
- `archive-v2-get-block.index`.

Produced block ID 0 is RPC-only. The command validates that retained getBlock JSON against the
repair manifest and derives the one-row `prev_blockhash_tail.bin` from its `parentSlot` and
`previousBlockhash`. That RPC provenance supplies the real cross-epoch seed even though a
canonical epoch-999 sidecar is not an input to this repair.

The command writes and syncs candidates first, validates them against the repair view and hot
archive, and atomically rewrites the existing `REPAIR-COMPACTED.json` last. The completed local
receipt remains
`state=degraded_hot_archive_missing_poh_and_shredding`, `canonical=false`, and
`publication_ready=false`, with `block_archive_ready=true` and `block_access_ready=true`. Repeating
the command after that validated state is an idempotent no-op. False-marker or partial-file retries
rebuild and validate the candidates before the same marker-last commit. An archive left by the
older two-phase runner may still say `block_access_ready=false`; that is a repairable publication
gap, not missing source data.

Local completion does not publish these sidecars to object storage. If the remote Worker serves
epoch 1000 from R2, the three access files still need to be uploaded to its expected epoch keys
before remote block access is available.

## One-call RPC/local slot audit

The bounded `hivezilla-epoch-slot-audit` worker independently repeated one finalized
`getBlocks(432000000..432431999)` request and compared its 54,000-byte bitmap with the published
repair union. Both sides contain exactly 431,781 produced slots, with zero slots missing locally
and zero extra locally. The RPC response leaves 219 slots unlisted.

The state is intentionally `agrees_unproven`, not `slot_coverage_verified`: the configured
provider was not marked as contractually guaranteeing complete archival history, so an RPC
omission cannot by itself prove a skipped slot. The worker records produced-slot membership only;
it does not claim block-payload integrity.

The durable NAS artifacts are:

- RPC bitmap snapshot:
  `/volume1/@home/ach/dev/blockzilla-pipeline/state/epoch-slot-audits/epoch-1000/rpc-produced-slots.json`
  (SHA-256 `fadd7c33f1f642a5e753acf98f988bc33070bc476aa0ea49688cdb205ef37034`);
- comparison receipt:
  `/volume1/@home/ach/dev/blockzilla-pipeline/state/epoch-slot-audits/epoch-1000/coverage-audit.json`
  (SHA-256 `bf9041ab202b6ba2911040469f2636f2634a0a35f8afadcd17ac340041c1c06f`); and
- versioned worker binary SHA-256
  `5e4c854314be5b48279ea90466cf2875eebc451be34c33259c8fadfc312ed338`.

A second run used a deliberately unreachable URL and succeeded from the cached snapshot with
`rpc_snapshot_reused=true`, proving that it made no second network request. Both JSON artifacts
are mode `0600`; a value-level scan confirmed that neither contains an RPC URL, token, or matching
runtime secret value.

## Cleanup gate

No source folder is deleted until the atomic repair view passes its manifest, slot-union, PoH,
blockhash, hard-link, and RPC validation.

After that validation:

- the original early raw capture may be removed, reclaiming 328,302,956,544 allocated bytes;
- the temporary epoch-1000 finalizer view may be removed after its pubkey runs are retained by the
  repair view;
- trivial failed/partial output directories may be removed after path/inode checks;
- keep the compact early source, the late source, raw gRPC WAL/protobuf data, journals, the current
  partial finalized archive, and every input explicitly excluded by the repair manifest.

Deleting an original directory entry does not reclaim a hard-linked block/run/RPC inode retained
by the repair view. Raw events and journals remain separate retention gates until a canonical
materializer/finalizer has completed.

The cleanup gate has now passed. The stopped original early raw capture and the temporary
epoch-1000 finalizer view were removed, reclaiming approximately 327.1 GB of free space. Before
unlinking, the unique 3.45 MB early block index and both small layout JSON files were preserved
with SHA-256 digests. The cleanup receipt is:
`/volume1/@home/ach/dev/blockzilla-pipeline/state/epoch1000-live-cleanup-20260713T230816Z/receipt.json`.

The compact early source, late source, their PoH and journals, all consolidated RPC files, the
active capture, and the published repair bundle remain present. Post-cleanup checks again found
14,231 retained RPC files, 1,443 retained run files, both retained normalized source blobs, and the
12,956,112,861-byte merged PoH sidecar.

## Canonical completion

Epoch 1000 can become fully canonical only after the 14,231 missing PoH records and the missing
shred stream/boundaries are obtained from a CAR or another lossless source and a single finalizer
commit rebuilds every dependent canonical sidecar. Block archive and block access can both be
complete without that source recovery; until then the epoch remains intentionally degraded rather
than green.
