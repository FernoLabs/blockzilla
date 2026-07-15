# Epoch 0 Genesis and archive audit

Date: 2026-07-13

## Outcome

Epoch 0 is not a Genesis-only or zero-block epoch. The NAS already contains a coherent,
Genesis-aware hot archive with 431,548 real blocks. Do not replace it with an empty archive or
synthesize empty PoH/shredding sidecars.

The audit was initially read-only. After an isolated clone passed the deployed Hivezilla reader,
the two narrowly scoped repairs were published to production with exact hash guards and
same-filesystem atomic renames:

- `registry.mphf` was generated from the existing 448-key `registry.bin`;
- the single legacy Genesis metadata tag byte was migrated from `1` to `4`.

No block, block-index, PoH, shredding, signature, vote-hash, registry, registry-count, or
blockhash-registry content was changed. Hivezilla now classifies epoch 0 as `complete`, with
`usage_sorted` registry order and both legacy block-access sidecars intentionally
`not_applicable`.

The immutable audit inputs, original metadata, candidates, build log, isolated verification tree,
hashes, and publication receipt are retained at:
`/volume1/@home/ach/dev/blockzilla-pipeline/state/epoch0-repair-20260713T205925Z`.

The guarded procedure is captured in `scripts/nas-repair-epoch0-genesis.sh`; its isolated,
idempotent fixture is `scripts/test-nas-repair-epoch0-genesis.sh`.

## Genesis archive

Path:
`/volume1/blockzilla/genesis.tar.bz2`

- Compressed bytes: 20,144
- SHA-256: `133f7eaefcd59466f3b291aadd1b0d3522432072cf5b539445218c6c125ea945`
- One regular tar member: `genesis.bin`, 132,347 bytes
- Genesis hash, hex:
  `45296998a6f8e2a784db5d9f95e18fc23f70441a1039446801089879b08c7ef0`
- Genesis hash, base58: `5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d`
- Creation time: `2020-03-16T14:29:00Z`
- Mainnet cluster id: 1
- Genesis accounts: 431, totaling 500,000,000 SOL and 93,534 account-data bytes
- Reward pools: 0
- Builtins: config, stake, system, and vote programs
- Ticks per slot: 64
- Tick duration: 6.25 ms
- Hashes per tick: 12,500
- Slot duration: 400 ms
- Epoch schedule: 432,000 slots, no warmup, normal epoch/slot both start at zero
- Fee target/min/max: 10,000 / 5,000 / 100,000 lamports per signature
- Rent: 3,480 lamports per byte-year, 2.0 exemption threshold

The field decoder consumed the entire `genesis.bin` with zero trailing bytes.

## Existing epoch archive

Path:
`/volume1/@home/ach/dev/blockzilla-v2/epoch-0`

- Indexed blocks: 431,548
- Covered slot range: 0 through 431,999
- Absent slots: 452
- Zero-transaction blocks: 4
- Transactions: 1,724,876
- Signatures: 1,724,881
- Block blob: 74,044,326 bytes
- Block index: 22,440,532 bytes, exact expected size
- PoH sidecar: 1,008,339,925 bytes
- Shredding sidecar: 63,399,571 bytes
- Blockhash registry: 431,549 entries

Every block id, increasing slot, compressed range, transaction ordinal, and signature ordinal was
validated. The blockhash registry has exactly one more row than the block index because entry zero
is the Genesis hash. Its first 32 bytes exactly match the decoded mainnet Genesis hash.

## Why Hivezilla reported blocked

The archive was built with a previous format and is missing `registry.mphf`. Its `registry.bin`
contains 448 structurally valid keys. Building the MPHF on a temporary path completed in 0.03
seconds and produced a valid 5,548-byte `BZKIDX1!` index.

There is also a reader-compatibility migration to apply. The old metadata encoded the Genesis enum
record with tag `1`; the current schema uses tag `4` because tag `1` now means block. The metadata
has exactly three framed records:

1. header, tag 0, payload 3 bytes;
2. Genesis, legacy tag 1, payload 99,539 bytes;
3. index, tag 2, payload 41 bytes.

On a temporary copy, changing only the Genesis tag byte at offset 7 from `1` to `4` changed the
metadata SHA-256 from
`62919229ca6cfd83019a8481de965818d825a3abd5eca3293dc11c13bf658383` to
`d50c0641d9f422ee01a8f7473c2f098e69c2b3cb26788188f239065458f0ae10`.
The current reader then decoded one Genesis record with all 431 accounts.

A complete temporary archive clone with both the tag migration and the generated MPHF was
classified by the deployed Hivezilla as:

- state: `complete`
- registry order: `usage_sorted`
- completion class: legacy no-access archive
- block-access sidecars: intentionally not applicable

The production files now have these post-repair values:

- metadata SHA-256:
  `d50c0641d9f422ee01a8f7473c2f098e69c2b3cb26788188f239065458f0ae10`
- registry MPHF: 5,548 bytes, `BZKIDX1!` version 2, 448 keys
- registry MPHF SHA-256:
  `077cb300d44b0a3a30cc1b953b6bcb89d563858fd49b40516a124bee6dc90a07`

## Recovery procedure used

1. Recheck all exact source hashes and ensure epoch 0 has no active worker.
2. Copy the original metadata to an external timestamped backup.
3. Produce a same-filesystem metadata candidate, require the exact three-frame shape above, change
   only the legacy Genesis tag, fsync it, and validate it with the current reader.
4. Build `registry.mphf` to a candidate path from the existing 448-key `registry.bin` and validate
   its header/key count.
5. Publish the MPHF candidate and then the metadata candidate with atomic renames, followed by a
   directory fsync. Until both names exist together, Hivezilla cannot classify the archive as
   complete.
6. Wait for Hivezilla reconciliation and require epoch 0 to become light-complete.
7. Preserve the original metadata and a publication receipt for deterministic rollback/audit.

The generic compact/reuse validator should separately learn that epoch 0 legitimately has
`blockhash_count == block_count + 1`. The extra entry is Genesis, not corrupt block coverage.

## Follow-up hardening

The general Genesis reader currently buffers the compressed archive before parsing and checks the
10 MB `genesis.bin` limit after reading. This particular trusted file is only 20 KB compressed, but
remote/untrusted Genesis ingestion should stream with compressed and decoded byte limits, bound
declared collection lengths, and pin the expected network Genesis hash.
