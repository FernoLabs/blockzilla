# CAR slot-index repair: epochs 100 and 200

## Completion update

Both indexes were rebuilt and passed range checks. Full CAR count reads match
V2 and V3, including every 9,000-slot bucket:

| Epoch | Blocks | Transactions | Inner instructions | Count time |
|---|---:|---:|---:|---:|
| 100 | 402,076 | 85,985,993 | 37,582,802 | 326.383652 s |
| 200 | 318,235 | 217,531,687 | 5,659,148 | 447.700091 s |

Both corrected R2 objects were uploaded with Wrangler and verified byte-for-byte
through the public Worker. The initial backup request failed because the Worker
rejects query parameters; the retry used plain URLs with `Cache-Control: no-cache`.
Public-object backups are in `/private/tmp/car-index-publication-20260906-retry/`.

The global NAS 12-byte indexes are replaced, with old copies retained in
`previous-local-indexes/` under the repair results folder. The retained CAR
directories are write-protected; their old indexes and protections remain
unchanged. The resumed benchmark uses `benchmark-archive/` under the repair
results folder, with links to the new indexes and the existing CAR files.
No CAR payload was changed.

CAR restarted as PID 1404244. Its authoritative status is
`/volume2/blockzilla-bench/results/all-car-local-indexfix-20260906/status.json`.
The earlier local publication failure was recovered by
`finish-car-index-promotion-20260906.py`; do not treat that earlier error as
the current benchmark status. `published.json` on NAS records the final layout.

## Evidence

The 12-byte raw index always has 432,000 rows: 5,184,000 bytes per epoch.
Each row is a little-endian u64 CAR offset plus a u32 length. Skipped slots
have zero rows. Correct file size does not prove correct slot membership.

The benchmark and public R2 copies had 387,576 nonempty rows for epoch 100
and 294,388 for epoch 200. Expected canonical block counts are 402,076 and
318,235. The newer 44-byte NAS indexes were also unsuitable: they omitted
9 and 60 canonical blocks, containing 2,067 and 48,118 transactions according
to the V2 block index, and had 1 and 3 range overlaps respectively.
Do not treat these missing ranges as empty canonical blocks.

## Authorized repair

Use the existing `of-car-slot-index` streaming builder with `--raw-only
--jobs 1` on the two retained local CAR files. Its only outputs are the new
12-byte indexes. Do not download or compact CAR files, build blockhash
registries, or hash archive payloads. All intermediate files and NAS results
are on SSD; the two CAR inputs remain on HDD.

Epoch 100 rebuilt 402,076 blocks in 134.33 seconds. Epoch 200 is in progress
at initial report creation. Later status files are authoritative.

## Validation and publication gates

1. Confirm source identity, size, and modification time are unchanged.
2. Check fixed index size, exact canonical slot membership against the V2
   index, zero skipped-slot rows, non-overlap, and CAR range bounds.
3. Run the actual CAR count example on both complete epochs. Compare block,
   transaction, inner-instruction totals, and all 9,000-slot buckets with both
   completed V2 and V3 outputs.
4. Only when both pass, copy the new indexes to the Mac. Back up the current
   public objects, upload the two index replacements with Wrangler, and check
   the public index bytes against the new local files. No public CAR reads.
5. Back up both NAS copies per epoch (retained and global slot-index folder),
   then replace paths atomically. Do not overwrite the existing hardlinks in
   place. Preserve older 44-byte indexes as diagnostic evidence.
6. Resume all CAR examples, reusing the four completed epoch-0 reads and the
   two corrected count checks. Compare each remaining result with both V2 and
   V3. Preserve all previous results. Stop on errors or mismatches.

The upload continuation runs on the Mac and requires it to remain awake and
connected. The build/count controller runs independently on NAS. These are
one-shot workflow processes, not recurring automations.

## Paths and processes

- NAS control: `/volume2/blockzilla-bench/control/car-index-rebuild-20260906/`
- NAS repair results: `/volume2/blockzilla-bench/results/car-index-rebuild-20260906/`
- NAS build/count controller at launch: PID 1377177; streaming builder PID 1377178.
- Mac publication controller at launch: PID 96626.
- Mac publication state and backups: `/private/tmp/car-index-publication-20260906/`
- Mac publication log: `/private/tmp/car-index-publication-20260906.log`
- New CAR benchmark: `/volume2/blockzilla-bench/results/all-car-local-indexfix-20260906/`
- CAR resume log: `car-resume.log` in NAS control; PID in `car-resume.pid`.

The publication controller uses the Cloudflare/Wrangler skill workflow and the
installed Wrangler 4.127.1. It changes only
`blockzilla-archive-samples-v1/car/100/epoch-100-slot-ranges.raw` and
`blockzilla-archive-samples-v1/car/200/epoch-200-slot-ranges.raw` in R2.
No reader binaries, Worker deployment, archive payloads, or other epochs are
changed by publication.
