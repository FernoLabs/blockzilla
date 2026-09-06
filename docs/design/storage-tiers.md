# Storage tiers: what is an archive and what is a cache

Date: 2026-08-22. Measured, with a proposed layout change.

**One directory currently holds two products with different owners, different
lifetimes and different cost models.** `/volume1/blockzilla/archive/epoch-N`
contains both the durable archive and a serving-tier cache built for a
Cloudflare Worker. They are counted together, replicated together, and reported
together, which makes every size number wrong in one direction or the other.

This proposes splitting them by directory and says what each tier is for.

---

## 1. The measurement that started it

An archive-wide `stat` sweep over all 1,019 published epochs, 2026-08-22:

| | bytes | TiB | share |
|---|---:|---:|---:|
| `archive-v2-blocks.zstd` | 51,009,379,173,862 | 46.39 | 52.83% |
| `signatures.bin` | 36,971,697,683,648 | 33.63 | 38.29% |
| `poh.wincode` | 6,938,803,192,845 | 6.31 | 7.19% |
| `registry.bin` | 684,425,351,520 | 0.62 | 0.71% |
| `shredding.wincode` | 600,408,416,338 | 0.55 | 0.62% |
| `registry.mphf` | 262,430,590,398 | 0.24 | 0.27% |
| everything else | 94,761,693,536 | 0.09 | 0.10% |
| **archive tier** | **96,561,906,102,147** | **87.82** | **100%** |
| `archive-v2-block-access.wincode` | 4,438,431,579,266 | 4.04 | *separate* |

The last row is present on **43 of 1,019** epochs. Counting it as part of the
archive makes the archive look 4.4% larger and 4.4% less complete than it is,
and neither is true.

---

## 2. `archive-v2-block-access.wincode` is not an index

The name invites the assumption that it is a small per-block lookup. It is not,
and the confusion is worth naming because two files share the prefix:

| file | size | what it is |
|---|---:|---|
| `archive-v2-block-access.index` | **32 B / block** | the actual index — `ROW_LEN = 4+8+8+4+4+4` ([archive.rs](../../crates/compact-v2/blockzilla-archive-v2/src/v2/archive.rs)) |
| `archive-v2-block-access.wincode` | **~250 KiB / block** | a denormalised per-block blob |

The blob's own doc says what it carries ([v2/mod.rs](../../crates/compact-v2/blockzilla-archive-v2/src/v2/mod.rs)):

> Per-block access sidecar for registry-free hot-path rendering. […] carries
> only the id->bytes entries that are needed by one block, **plus the block's
> signatures**.

Measured across the 43 epochs that have it:

| | GiB | share |
|---|---:|---:|
| blob total | 4,133.6 | 100% |
| — a second copy of each block's signatures | 1,796.0 | **43%** |
| — per-block `id → [u8; 32]` pubkey tables | 2,337.6 | **57%** |

So it duplicates `signatures.bin` outright, and repeats a 36-byte
`(id, pubkey)` entry in *every block* that touches an account — precisely the
duplication the registry exists to remove.

**That is not a defect.** It is the purchase price of a specific capability,
described next.

---

## 3. Who reads it, and why the shape is right for them

The only production consumer is `edgezilla/get-block` — a Cloudflare
Worker reading R2 with ranged GETs (`use worker::{ Range as R2Range, … }`,
[worker.rs](../../edgezilla/get-block/src/worker.rs)). Everything
else touching the blob in `blockzilla/cli/src` is build, verify or repair tooling;
nothing else *reads* it.

The blob lets that worker answer `getBlock` in **one object read**. The
alternative — resolving registry ids through `registry.bin` (1.2 GB) plus
`registry.mphf` (465 MB) — cannot be done per request at the edge.

It is already optional at the read path:

```rust
enum BlockBinAccessMode { Include, Skip }   // worker.rs:1044
```

with separate cache keys per mode, so the serving tier can run either way.

**Fewer object reads per request is the entire point.** That is a cloud cost
decision, and it should be budgeted as one.

---

## 4. Its cost is per block, so it is flat forever

Over the 43 epochs carrying a real blob:

| model | mean | CV |
|---|---:|---:|
| absolute per epoch | 96.1 GiB | **7.1%** |
| per block record | 7,734 | **8.4%** |
| proportional to `blocks.zstd` | 1.66× | 40.6% |

An epoch is always ~432,000 slots, so a per-block structure costs the same
whether those blocks are sparse or dense. The *proportional* figure collapses
from 2.4× (epochs 277–305) to 1.0× (epochs 997–1018) only because block
payloads grew around it — it is the misleading one.

Extrapolating the per-block model to the whole chain gives roughly **90 TiB**.

**Do not build that into the archive.** It would roughly double archive
footprint to store a cache, of which ~38 TiB would be a second copy of
`signatures.bin`. If the whole chain must be servable from the edge, 90 TiB is
the *object-store* budget for that decision, recurring, and it belongs in a
bucket rather than on the NAS.

### Four epochs carry a stub

Epochs **500, 600, 700, 800** have a 0 MB blob rather than a real one. Round
numbers, so almost certainly one-off test builds. They are excluded from every
statistic above and should be deleted rather than completed.

---

## 5. What the split buys, in numbers

Against old-faithful (CAR + its published indices, from the
[yellowstone-faithful CAR report][car], whose sizes agree with local byte
counts within ±0.4 GiB on six epochs):

| | TiB | ratio |
|---|---:|---:|
| old-faithful, whole chain | 522.64 | — |
| Compact V2 **as stored today** | 91.86 | 5.69× |
| Compact V2 **archive tier** | **87.82** | **5.95×** |

Per epoch the archive-tier ratio is flat across the entire chain — min 4.99×,
median 5.85×, max 7.96×, no trend from epoch 0 to 1018. On the six most recent
epochs it is **6.97×**.

Reporting the as-stored number understates the format by a quarter of a turn,
and does so unevenly: the 43 blob-carrying epochs drop to ~2.6×, which reads as
format variance when it is really tier contamination.

[car]: https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md

---

## 6. Proposed layout

Three roots under `/volume1/blockzilla`, one per product:

```text
archive/epoch-N/        durable archive tier — the thing we promise to keep
  archive-v2-blocks.zstd, archive-v2-blocks.index, archive-v2-meta.wincode
  signatures.bin, poh.wincode, shredding.wincode
  registry.bin, registry_counts.bin, registry.mphf
  blockhash_registry.bin, blockhash_index_v3.bin, prev_blockhash_tail.bin
  vote_hash_registry.bin, block-time-gaps.bin

old-faithful/           upstream CARs — transient, deletable once converted

edgezilla/epoch-N/      serving tier — derived, rebuildable, cloud-bound
  archive-v2-block-access.wincode     the blob
  archive-v2-block-access.index       addresses the blob
  archive-v2-get-block.index          addresses blob + blocks.zstd
```

One directory per epoch on both sides, and **the edge directory carries its own
indexes**. Nothing in the archive addresses anything in edgezilla.

### The invariant

> **The durable tier never references the derived tier. The derived tier may
> reference the durable tier.**

An archive object may hold offsets only into other archive objects, so the
archive stays readable and verifiable with no edge tier present at all. Edge
objects may point back into `blocks.zstd`, because losing the edge tier costs a
rebuild rather than data.

`archive-v2-get-block.index` is in the edge list because of that rule, and it is
the easy one to get wrong. It reads like an archive index, but its rows are
`(block_offset, block_len, access_offset, access_len)` — offsets into *both*
tiers — and `ArchiveV2GetBlockIndexRow::is_missing` reports a block as **missing
when `access_len == 0`**
([archive.rs](../../crates/compact-v2/blockzilla-archive-v2/src/v2/archive.rs)). An
archive index that declares blocks missing because a cache is absent is exactly
the coupling this split exists to remove. Its only production reader is
`edgezilla/get-block`, the same consumer as the blob.

The rule is mechanical enough to enforce: **any object whose rows carry an
`access_offset` is edge tier.** `blockzilla-archive-v2` encodes this as
`EDGE_TIER_FILES` with a test asserting it
([layout.rs](../../crates/compact-v2/blockzilla-archive-v2/src/v2/layout.rs)).

The properties that make this the right cut:

| | archive | edgezilla |
|---|---|---|
| durability | must never be lost | rebuildable from the archive |
| sizing | per byte of chain | per block, ~96 GiB/epoch |
| consumer | indexers, replay, verification | one edge worker |
| destination | NAS + custody | object store |
| completeness | all 1,019 epochs | only epochs we choose to serve |

The last row is the operational point. **Coverage is a choice for edgezilla and
an obligation for the archive.** Today's 43/1,019 looks like a gap only because
the two are in the same directory.

---

## 7. The code change

Every call site already builds the path as `<dir>.join(CONST)`, and
`archive_v2/repair.rs:3148` already threads a distinct `access_root` — so the
directory is a parameter almost everywhere. The change is to make that
parameter explicit and default it to a separate root.

1. **Add a layout type** in `blockzilla-format` that resolves both roots from a
   configured base, rather than assuming the epoch directory. **Done** —
   `v2/layout.rs`:

   ```rust
   ArchiveLayout::split(base)      // base/archive + base/edgezilla
   ArchiveLayout::colocated(root)  // historical: one directory, still supported
   layout.block_access_blob(epoch)
   layout.block_access_index(epoch)
   layout.get_block_index(epoch)
   tier_for_file(name) -> StorageTier
   ```

   `colocated` is not optional: every generation written before 2026-08-22 uses
   it, so readers must keep resolving both shapes.

2. **Writers** (`archive_v2.rs:899/1571/3013/13051`,
   `first_seen_finalization.rs:407`) emit the blob under `edge_dir`.

3. **Readers** (`edgezilla/get-block/src/main.rs:603`) resolve from
   `edge_dir`; the Worker itself is unaffected because it already reads objects
   by key from R2, not by filesystem path.

4. **Verification** (`archive_verify.rs:1828`) stops treating the blob as a
   required archive artifact. It becomes a separate edgezilla check. The
   14-artifact completeness list loses all three edge objects, which is what
   currently makes 976 epochs read as incomplete when they are not.

5. **`registry_reprocess`** currently remaps the blob as part of a generation
   ([registry_reprocess.rs](../../blockzilla/cli/src/archive_v2/registry_reprocess.rs)).
   Once split it can skip it entirely and let edgezilla be rebuilt after, which
   removes ~100 GiB of rewrite from every reprocessed epoch.

Point 5 is the one that pays for the change immediately: the usage-sorted
migration is rewriting a cache it does not need to rewrite.

### Migration

Moving the existing 4.04 TiB is a rename per epoch, not a rebuild — the blob is
already self-contained and its index is relative to itself. The 14-artifact
completeness list used by the build scripts drops to 14 archive artifacts with
the two block-access entries removed, which also fixes the false "incomplete"
reading on 976 epochs.

---

## 8. What this does not settle

- **Whether the blob's format is finished.** It is not, and that is a reason to
  keep it out of the archive's compatibility surface, not a reason to freeze it.
- **Which epochs should be servable.** 90 TiB is the price of all of them; the
  right answer is probably a recent window plus on-demand generation.
- **Whether the 43% signature duplication is necessary.** The worker needs
  signatures in the same object; whether it needs *all* of them, or could range
  into `signatures.bin` as a second read, is a live question worth measuring
  before the whole chain is built.
