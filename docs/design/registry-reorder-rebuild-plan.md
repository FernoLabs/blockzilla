# Rebuilding the 35 first-seen epochs with a sorted registry

> **WITHDRAWN 2026-08-16.** First-seen ordering is a deliberate design, not
> damage. `blockzilla/cli/src/bin/first_seen_hot_tier_bench.rs` benchmarks "the
> **one-pass** first-seen interner", alongside a dedicated
> `first_seen_finalization.rs` and a versioned manifest with seed chaining.
>
> The reason is structural: first-seen assigns an id the moment a key is first
> observed, so compaction is a single streaming pass. Usage-sorted requires two
> — count every reference, sort, then assign ids and rewrite. For a pipeline
> ingesting live chain data that is a real constraint, and Compact V2 is served
> live (hivezilla gRPC, the Firewatch indexer, the read SDK), not merely a
> migration source.
>
> **Do not execute Phase 2.** Rewriting 2.32 TB and 475 billion references would
> undo an intentional optimisation, and pushing it upstream into the compactor
> would slow the live ingest path. The two orderings are not in conflict:
> Compact V2 is optimised for write throughput, the Index Archive for read. The
> place to change the order is the converter — see
> [`registry-ordering-plan.md`](registry-ordering-plan.md).
>
> Retained below for the measured scope and cost, which remain accurate.

Date: 2026-08-16. Plan, not implemented. Companion to
[`registry-ordering-plan.md`](registry-ordering-plan.md), which covers making
usage order a guarantee of the *new* format; this covers normalising the
*Compact V2 sources* in place.

## Confirmed scope

Measured archive-wide with `registry-order`, reading `registry_counts.bin`
(present in all 1013 epochs, decoding 1:1 against `registry.bin`, 0 mismatches):

| | epochs |
|---|---:|
| canonical — usage-sorted from index 1 | **978** |
| first-seen — needs reorder | **35** |

The 35 are exactly the set carrying `registry-first-seen.manifest`: 277–281,
301–305, 401–405, 501–505, 864, 997, 1000–1012. **No undeclared epoch is
unsorted**, which was the risk worth ruling out.

Registry index 0 is a reserved sentinel with count 0 — the format's 1-based
ordinals treat 0 as the inline-key marker — so scoring starts at index 1.
Ignoring that produced 271 false positives on the first pass.

### What it costs

| | |
|---|---:|
| `archive-v2-blocks.zstd` to rewrite | **2.32 TB** |
| registry keys to permute | 890,792,951 |
| **account references to remap** | **475,304,225,482** |

Head purity of the 35 — the share of the top-65,536 accounts actually sitting in
the first 65,536 ordinals — runs 26–69% and **degrades with epoch age**: 65.3%
at epoch 277, 30.3% at 1000, **25.8% at 1012**. The `seeded_keys=65536` hot set
carried from the predecessor is decaying as a predictor, so the newest epochs
are the worst.

---

## Phase 1 — build the sorted registry (cheap, reversible, independently useful)

Produces no rewrite. For each of the 35:

1. Read `registry.bin` and `registry_counts.bin`.
2. Sort entries `1..n` by **count descending, then key ascending**. Index 0 stays
   pinned at index 0. The key tie-break is what makes the result reproducible —
   it matters, because ties are enormous: 34–85% of accounts have count exactly
   1, and there are only 12K–45K distinct count values across 30M+ keys, so the
   tie-break decides the position of roughly a third of the registry.
3. Emit `registry.bin.sorted`, `registry_counts.bin.sorted`, and a permutation
   `old_id → new_id` as a `u32` array.

**Gates before anything is rewritten:**

- permuted counts are non-increasing from index 1;
- the multiset of 32-byte keys is unchanged (sort both, compare);
- the permutation is a bijection over `0..n`;
- index 0 is still index 0;
- head purity of the result is 100%.

Phase 1 is worth doing on its own even if Phase 2 never runs: it is the same
permutation the converter needs for
[`registry-ordering-plan.md`](registry-ordering-plan.md), and it proves the sort
is well-defined on real data before any bytes move.

## Phase 2 — rewrite the blocks (the expensive part)

For each block in `archive-v2-blocks.zstd`: decompress, remap every
`CompactPubkey::Id` through the permutation, re-serialize, recompress, and
publish via temp + atomic rename — the pattern
`migrate-poh-signature-counts` already uses.

This touches **475 billion references across 2.32 TB**. Sizing it honestly on a
host that is already I/O-saturated is the first task, not an afterthought: the
PoH migration managed ~5,500 blocks/s while only *reading* blocks, and this both
reads and rewrites them.

Per-epoch verification, all of which must pass before the next epoch starts:

- block count, transaction count and slot range unchanged;
- `chain-verify` still reports zero faults (the header's `blockhash_id` and
  `previous_blockhash_id` are **not** registry ids and must be untouched);
- a sample of transactions resolves to the same 32-byte keys as before;
- `registry-order` reports canonical.

## Phase 3 — rebuild the derived files

`registry.mphf` (key → id) is invalidated by definition and must be rebuilt.
`registry_counts.bin` is replaced by the permuted version. `registry-hot-seed.bin`
and the manifest need regenerating, and the manifest's `registry_order` should
become the new value rather than being deleted — a stated order is the whole
point.

**The seed chain is the trap.** The manifest records
`seed_source=…/epoch-304/registry.bin` and `next_seed_file=registry-hot-seed.bin`,
so epochs feed each other. Reordering an epoch changes what its successor was
seeded from. Since the goal is to sort every one of the 35 by count, the seed
stops determining order — but the recorded provenance becomes wrong unless it is
regenerated too, and provenance that quietly lies is worse than provenance that
is absent.

## Unknown consumers — the risk that actually bites

A registry ordinal is meaningless outside its epoch, so anything holding one
must be found before the rewrite, not after. Known or suspected holders beyond
`blocks.zstd`:

- `registry.mphf` — rebuilt in Phase 3;
- `archive-v2-block-access.wincode` / `.index` — present on exactly these 35
  epochs, and needs checking for embedded ids;
- the Firewatch index tree — a separate tree built from these
  registries; if it stores ids rather than keys it must be rebuilt;
- `vote_hash_registry.bin`.

An audit of every file that can hold a registry id is a **prerequisite** for
Phase 2. A missed holder is a silent wrong-account bug, which is the worst
possible failure for this archive.

---

## The decision this plan does not make

If these 35 epochs are going to be converted to the Index Archive and their
Compact V2 sources deleted, **Phase 2 is wasted work**. The converter already
rewrites every account reference on its way into `ledger/transactions.wincode`,
so applying the permutation there costs a lookup it is already performing, and
it normalises all 1013 epochs rather than 35.

Phase 2 earns its cost only if Compact V2 stays a served format — because
Firewatch or another consumer reads it directly, or because the compaction
pipeline keeps producing first-seen epochs and they must match.

If Compact V2 keeps being produced, the more durable fix is upstream: make the
compactor emit usage-sorted registries so the problem stops being created. That
is a smaller change than rewriting 2.32 TB, and it is the only one that prevents
epoch 1013 from arriving with 25% head purity.

**Recommended order regardless of that decision:** Phase 1 now (cheap, gated,
reusable), the consumer audit next (it is a prerequisite for either path), and
Phase 2 only once the Compact V2 question is settled.
