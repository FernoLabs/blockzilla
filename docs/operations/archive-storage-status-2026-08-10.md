# Archive storage status — 2026-08-10

> Point-in-time, read-only NAS inventory. The scan workers remained active.
> The inventory read file metadata only; it did not hash or read archive
> payloads. Decimal TB and GB are used unless stated otherwise.

## Result

- The 22 published usage-sorted generations are **9.729 GB smaller** than
  their first-seen sources: 4.591 TB becomes 4.582 TB. This is a **0.212%**
  logical-size reduction, or an average of 0.442 GB per epoch.
- The size reduction is real but small. The main benefits of usage sorting are
  smaller registry IDs in hot data and improved read efficiency, not a large
  whole-archive storage reduction.
- Both generations are retained today. They use 8.640 TB of distinct physical
  allocation. The sorted targets add 4.049 TB beyond the source generations.
- The 11 v3 conversions hard-link their signature sidecars. Those links avoid
  532.645 GB of duplicate allocation.
- If the old first-seen generations are retired after a catalog switch and a
  final validation, 4.059 TB becomes reclaimable. Do not delete them yet: the
  sorted generation is still separate from the canonical archive path.

## First-seen compared with usage-sorted

| Receipt group | Epochs | Source logical | Target logical | Reduction |
|---|---:|---:|---:|---:|
| v1 | 2 | 427.504 GB | 425.542 GB | 1.962 GB |
| v2 | 9 | 1,929.020 GB | 1,924.442 GB | 4.578 GB |
| v3 | 11 | 2,234.840 GB | 2,231.652 GB | 3.188 GB |
| **Total** | **22** | **4,591.364 GB** | **4,581.635 GB** | **9.729 GB** |

Every target is smaller than its source. The aggregate logical changes are:

| Data group | Target minus source |
|---|---:|
| Compressed hot blocks | −5.000 GB |
| Block Access data | −3.801 GB |
| Registry, counts, and MPHF | −1.110 GB |
| Removed first-seen control and seed files | −0.046 GB |
| New get-block indexes | +0.228 GB |
| New receipts | +0.000126 GB |
| Signatures, PoH, shredding, blockhash data, and other stable sidecars | No logical change |

The physical view differs from the logical view:

| Physical allocation | Size |
|---|---:|
| First-seen sources | 4.591 TB |
| Usage-sorted targets | 4.582 TB |
| Shared v3 signature inodes | 0.533 TB |
| Both sets, with shared inodes counted once | 8.640 TB |
| Current incremental cost of the targets | 4.049 TB |
| Reclaimable source-only allocation after a safe cutover | 4.059 TB |

### Exact varint-order result

The audit compared the same keys and counts in all 22 source and target
registries. It read registry, count, manifest, and receipt files only. It did
not read block payloads.

- The eligible reference set contains 135.594 billion pubkey references.
  Their direct ID bytes decrease from 215.864 GB to 200.541 GB. The exact
  saving is 15.323 GB, or 7.10%.
- The average eligible ID decreases from 1.592 bytes to 1.479 bytes. The
  saving is only 0.113 byte per reference.
- The complete typed-reference set contains 206.239 billion references.
  After the required conversion of dropped keys to 33-byte raw pubkeys, its
  bytes decrease from 297.464 GB to 282.639 GB. The exact net saving is
  14.825 GB, or 4.98%.
- Dropped log- and reward-only keys add 0.991 GB compared with a hypothetical
  sort that keeps every key.
- Every first-seen source already starts with the previous epoch's 65,536
  most-used keys. Before the full sort, 91.41% of eligible references already
  use one- or two-byte IDs. The full sort increases this to 95.78%.

The full sort therefore works, but it is an incremental optimization over an
already frequency-seeded source. Zstd also compresses repeated IDs in the hot
blocks, and Block Access stores a de-duplicated ID set for each block. Thus the
15.323 GB direct ID saving becomes a smaller whole-generation disk saving.

## Compact archive compared with Old Faithful

These totals are an inventory, not a format compression ratio. The Compact
archive contains about one thousand epochs. The local Old Faithful tree keeps
only 22 canonical source epochs, plus duplicates and a partial download.

| Tree | Current contents | Physical allocation |
|---|---|---:|
| Canonical Compact paths | 1,011 epoch directories, including active outputs, epoch 0 through 1010 | 98.126 TB |
| Whole archive tree, unique inodes | Canonical data, sorted generations, staging, and quarantine | 102.669 TB |
| Usage-sorted subtree, path-counted | 22 published targets and 4 staging directories | 4.716 TB |
| Archive quarantine | Retained recovery data | 0.360 TB |
| Whole Old Faithful tree | 2,030 files | 17.172 TB |

Old Faithful currently contains:

- 13 plain CAR files for epochs 1000–1012: 11.638 TB;
- 9 `car.zst` files for epochs 760, 761, and 793–799: 2.848 TB;
- two numbered full CAR copies, `1008.1` and `1011.1`: 2.031 TB;
- one sparse partial download for epoch 1006: 0.630 TB allocated;
- indexes: 0.024 TB.

The two copies and the partial download use 2.661 TB. They are cleanup
candidates only; matching sizes do not prove matching content or absent
ownership.

### Like-for-like retained source samples

With every uncompacted, active, queued, duplicate, and partial input excluded,
the fully published set is epochs 760, 761, 793–799, and 1000. Its retained
CAR and `car.zst` files use 3.616 TB; the corresponding Compact directories
use 1.481 TB. Compact saves 2.135 TB, a **59.04% reduction**, and is **2.441
times smaller**. This strict set mixes already-compressed `car.zst` files with
one plain CAR.

The retained plain CAR sample gives the best current raw-CAR comparison:

- Epoch 1000 plus scan-ready epochs 1001–1006 and 1008–1009 use 7.882 TB as
  plain CAR files and 2.312 TB as Compact archive directories.
- The current Compact result is **3.41 times smaller**, a 70.67% reduction.
- Eight of those directories still need their final MPHFs. Their final ratio
  will therefore be slightly lower than 3.41 times.
- Completed epoch 1000 alone is 3.246 times smaller, a 69.19% reduction.

For the nine retained `car.zst` epochs, the compressed CAR files use 2.848 TB
and their Compact directories use 1.245 TB. Compact is 2.288 times smaller, a
56.30% reduction. Active partial outputs for epochs 1007 and 1010 are excluded
from both comparisons.

The volume has 222.987 TB total, 128.100 TB used, and 94.887 TB available.
The unique-inode archive tree is about 80.1% of the currently used space.

## Pipeline state

- Scheduler range: epochs 277–1012.
- Complete: 724 epochs, through epoch 1000.
- Scan-ready: 8 epochs, 1001–1006 and 1008–1009.
- Scanning: epochs 1007 and 1010.
- Queued by predecessor dependency: epochs 1011 and 1012.
- Usage-sorted migration: 22 of the original 23 targets are published.
- Epoch 301 is an Access-only continuation; its Core result is retained.
- New registry Core admission is set to zero while the CAR backlog runs.

## Decisions

1. Keep the current first-seen CAR scanner on this 8-GiB NAS. The direct
   in-memory usage-sorted CAR builder needs close to 7 GiB of safe working
   memory at the observed key counts.
2. Design a bounded external-sort CAR path if usage-sorted publication must
   happen directly. It must not hold the complete key-count map and sorted key
   vector in memory at the same time.
3. Do not remove first-seen archive sources until readers use a durable
   generation catalog or current-generation pointer and the sorted targets
   pass their final retention gate.
4. Audit the two numbered CAR copies, the stale partial download, and the old
   epoch-305 staging directories. They are cleanup candidates, not approved
   deletions.
