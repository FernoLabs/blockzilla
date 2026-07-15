# Live repair materialization and degraded hot archives

The repair path is intentionally three-phase:

    blockzilla materialize-archive-v2-live-repair REPAIR_VIEW MATERIALIZED_OUTPUT
    blockzilla build-archive-v2-degraded-hot-blocks-from-repair MATERIALIZED_OUTPUT HOT_OUTPUT --level 1
    blockzilla build-archive-v2-repair-block-access REPAIR_VIEW HOT_OUTPUT

For the NAS epoch-1000 repair, `scripts/nas-compact-epoch1000-repair.sh` runs those phases
sequentially under one lock, at low CPU/I/O priority, and pauses the active phase after sustained
memory or I/O pressure. It never removes the repair view or its source captures.

The first command validates REPAIR-REQUIRED.json, the merge plan, produced blockhash chain,
available PoH records, source-frame boundaries, and RPC provenance. It remaps source-local IDs,
inserts decoded getBlock records, and spills bounded sorted pubkey runs. It resumes from fsynced
checkpoints; --max-blocks 1 leaves a hidden, non-published canary.

The second command reuses the optimized live registry-run merge and hot conversion. It emits the
readable zstd block archive, index, metadata, registry/MPHF, signatures, blockhashes, and vote-hash
registry. Its first durable `REPAIR-COMPACTED.json` receipt deliberately has
`block_access_ready=false`.

The third command validates both the original repair view and the degraded hot archive, then
builds all three files needed for direct block access:

- `archive-v2-block-access.wincode`
- `archive-v2-block-access.index`
- `archive-v2-get-block.index`

It also writes `prev_blockhash_tail.bin`. Produced block ID 0 must be RPC-only; the command hashes
and validates its retained getBlock JSON against `REPAIR-REQUIRED.json`, then uses that row's
`parentSlot` and `previousBlockhash` as the one-entry previous-epoch seed. It never invents a hash
or assumes an unavailable canonical epoch-999 sidecar. The command syncs and validates the new
files before atomically replacing `REPAIR-COMPACTED.json` last with `block_access_ready=true`. A
retry against an already complete, validated marker is an idempotent success. A false legacy
marker or partial candidate files are safely rebuilt and validated; a true marker that disagrees
with its files fails closed.

## Progress contract

During materialization, Hive can read this atomically replaced file:

    OUTPUT_PARENT/.OUTPUT_NAME.repair-materialize-stage/repair/materialization-progress.json

Version-1 fields are:

- version: integer, always 1
- phase: materializing or complete_noncanonical
- epoch: integer
- blocks_done and blocks_total: integers
- live_blocks_done and live_blocks_total: integers
- rpc_blocks_done and rpc_blocks_total: integers
- transactions_done: integer
- output_block_bytes: integer
- pubkey_run_files: integer
- rss_bytes: nullable integer (currently available on Linux)
- started_unix_secs and elapsed_secs: integers
- blocks_per_sec: finite floating-point number
- eta_secs: nullable integer
- updated_unix_secs: integer

The hot pass similarly exposes
.OUTPUT_NAME.repair-hot-stage/repair/hot-progress.json. Progress is never a completion receipt.
The block-access phase has no separate completion marker: only the marker-last transition in the
hot output is authoritative.

## REPAIR-COMPACTED.json

The hot output remains hidden until its initial root `REPAIR-COMPACTED.json` is durable; the whole
directory is then atomically renamed. The block-access command subsequently replaces that same
version-1 marker atomically and only after its sidecars are durable. The fixed fields for a
completed three-phase run are:

- version = 1
- state = degraded_hot_archive_missing_poh_and_shredding
- canonical = false
- publication_ready = false
- block_archive_ready = true
- block_access_ready = true
- epoch, epoch_start_slot, epoch_end_slot: unsigned integers
- live_blocks, rpc_only_blocks, produced_blocks: unsigned integers
- transactions, signatures: unsigned integers
- zstd_level: signed integer
- compressed_bytes, uncompressed_bytes: unsigned integers
- source_materialized_marker_sha256, source_manifest_sha256,
  source_merge_plan_sha256: lowercase 64-character SHA-256
- limitations: array of strings

The files object has these fixed paths:

- blocks = archive-v2-blocks.zstd
- index = archive-v2-blocks.index
- meta = archive-v2-meta.wincode
- registry = registry.bin
- registry_counts = registry_counts.bin
- registry_index = registry.mphf
- blockhashes = blockhash_registry.bin
- signatures = signatures.bin
- vote_hashes = vote_hash_registry.bin
- available_poh = repair/available-poh.wincode

When `block_access_ready=true`, the files object also names these fixed paths:

- block_access = archive-v2-block-access.wincode
- block_access_index = archive-v2-block-access.index
- get_block_index = archive-v2-get-block.index
- previous_blockhash_tail = prev_blockhash_tail.bin

The poh_coverage object contains available_records, available_entries, missing_records,
produced_id_space, record_ids_have_explicit_gaps=true, and the complete sorted
missing_record_ids array. The shredding_coverage object contains available_records=0,
missing_records=produced_blocks, and canonical_sidecar_emitted=false.

The exact source marker is retained at repair/source-REPAIR-MATERIALIZED.json; its digest must
equal source_materialized_marker_sha256.

The completed output must not contain `READY`, root `poh.wincode`, or root `shredding.wincode`.
Block access does not change the repair's canonical status: the available PoH file still has all
14,231 RPC-only gaps, and shredding remains absent for every produced block. Consumers must fail
closed if marker arithmetic, digests, paths, hot/access index totals, blockhash/previous-tail
validation, or PoH gaps disagree.

These files make local/NAS block access complete. They do not upload themselves. A remote Worker
that reads epoch artifacts from R2 still needs the three access files uploaded to the expected R2
keys (and any deployment metadata refreshed) before it can serve the repaired epoch through that
path.

## Resource and cleanup notes

Manifest reads are capped at 16 MiB, JSONL lines at 64 KiB, source frames at 256 MiB, each RPC
JSON at 32 MiB by default, and every block-access payload at the shared 64 MiB producer/consumer
limit. Pubkey memory is controlled by --pubkey-run-max-keys (default 250,000).

This safest implementation writes the remapped normalized intermediate before the hot pass. For
epoch 1000 that is roughly one terabyte, so check peak free space first. Do not remove the repair
view or forensic inputs until the final marker has `block_access_ready=true`, all named files
validate, and cleanup is separately approved.
