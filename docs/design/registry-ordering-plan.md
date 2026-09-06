# Making usage-sorted registry order a format invariant

Date: 2026-08-16. Historical plan, not an implemented registry-order guarantee.
The converter and registry findings below describe that measured snapshot.
Current Archive V3 ownership and migration status are in the
[workspace plan](workspace-restructure.md). Do not reinterpret stored registry
IDs across sources or reorder a registry without rewriting all references.

**Goal:** every epoch in the Index Archive has a pubkey registry sorted by
descending reference count, regardless of what its Compact V2 source did.

## The problem, measured

The archive holds two orderings and only one group declares itself:

| `registry-first-seen.manifest` | epochs | descending-adjacent counts |
|---|---:|---:|
| absent | 978 | **100.0%** — usage-sorted |
| present, `registry_order=first_seen_v1` | 35 | **69–90%** — first-seen |

The 35 are 277–281, 301–305, 401–405, 501–505, 864, 997, and **1000–1012** —
so every recent epoch is first-seen, and the archive is trending that way.

The converter inherits this. `main.rs` copies `registry.bin` verbatim into
`dictionary/pubkeys.pages` — *"Existing source IDs stay unchanged"* — and never
reads `registry_counts.bin`. The constant
`ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE` exists in `blockzilla-format` with no
consumer anywhere.

## Do not fix this in Compact V2

**First-seen is deliberate.** `first_seen_hot_tier_bench.rs` benchmarks a
*one-pass* interner: an id is assigned the first time a key is seen, so
compaction never revisits. Usage-sorted needs two passes. Compact V2 is served
live, so its ordering is a write-throughput decision and should stay that way.

The orderings serve different masters, and that is fine — Compact V2 optimises
ingest, the Index Archive optimises reads. Converting is where they meet.

The obvious reading of "make all epochs usage-sorted" is to rewrite the 35
sources. That is the expensive and wrong option.

A registry ordinal is referenced by every account slot of every transaction.
Epoch 305's own manifest reports **`references=6437113615`** — 6.4 billion
references in one epoch. Reordering its registry means:

- decompress, rewrite and recompress the whole of `archive-v2-blocks.zstd`
  (36–103 GB per epoch, roughly 1.5–2 TB across the 35);
- rebuild `registry.mphf`, `registry_counts.bin` and `registry-hot-seed.bin`;
- re-seed the *next* epoch, since `next_seed_file` chains forward;

and all of it on data that the migration plan deletes once converted. It is a
full recompaction to change a sort order.

## Fix it in the converter, where the rewrite already happens

The converter **already rewrites every account reference**. Source ids pass
through `target_pubkeys.resolve_or_intern` on their way into
`ledger/transactions.wincode`. Applying a permutation there is a lookup added to
a lookup that already exists.

And the sort key is already in the source: `registry_counts.bin` is present in
every epoch, holds one count per registry entry in registry order
(`count_semantics=all_compact_pubkey_refs_v1`), and decodes 1:1 against
`registry.bin` — verified on epochs 305 and 902, 34,060,831 counts for
34,060,831 keys, consuming the file exactly.

So the change is single-pass and needs no extra I/O over the source:

1. Read `registry_counts.bin` at open.
2. Compute a permutation ordering entries by **descending count**, breaking ties
   by the 32-byte key so the result is deterministic.
3. Write `dictionary/pubkeys.pages` in permuted order instead of copying
   `registry.bin` verbatim.
4. Apply the permutation wherever a source ordinal becomes a target ordinal.
5. Append interned keys — raw pubkeys absent from the source registry — after
   the permuted block, since they have no source count. These are rare
   (`raw_account_keys` was 0 on the epoch-2 run).
6. Record the guarantee in the generation's own metadata, so it is a stated
   invariant rather than an inherited accident.

Cost per epoch: one sort of ~34M `(count, key)` pairs — seconds — plus a `u32`
indirection on references the converter is already writing.

## What this buys

- **The invariant becomes checkable.** Every generation can assert its registry
  is descending by count, and a verifier can prove it in one pass.
- **It revives the ordinal-threshold index split.** `where-the-bytes-are.md`
  §5.2 had to be withdrawn because the ordinal is not a frequency rank on 35
  epochs. Sorting at conversion makes it true on all 1013 by construction.
- **It improves the varint distribution.** Hot accounts get low ordinals, so
  more references encode in one LEB128 byte. On the fixture 69.2% already fit
  one byte with source ordering; first-seen epochs give up part of that today.
- **It is uniform.** All 1013 epochs come out the same way, so no consumer has
  to ask which ordering it got.

## Risks to close before building

1. **Every ordinal must move together.** `ledger/transactions`,
   `dictionary/account_flags`, and the account/program/selector indexes all key
   on the registry ordinal. All are written by the converter, so all get the
   permutation for free — but that must be asserted, not assumed. A test that
   converts one generation twice, once with an identity permutation and once
   with a reversal, and checks that every reader resolves the same 32-byte key,
   would catch a missed site.
2. **Determinism must survive.** Output is currently byte-identical across
   worker counts, and the sort must not break that — hence the key tie-break.
   The existing `canonical-candidate.sha256` comparison is the regression check.
3. **Count semantics need confirming for the 978 undeclared epochs.** Only the
   35 with a manifest state `all_compact_pubkey_refs_v1`. The others have the
   same file and the same 1:1 shape, and epoch 902's counts are perfectly
   descending against its own registry, which is strong evidence the semantics
   match — but it is inference, and worth asserting at open by checking that a
   sample of counts agrees with observed references during the pass.
4. **`registry.mphf` is not carried forward**, so nothing downstream depends on
   the source ordinal after conversion. Confirm no other consumer of
   `pubkeys.pages` assumes source order.

## Sequencing

This is cheap and it changes bytes the converter writes, so it belongs **before**
any bulk conversion run — reordering afterwards means reconverting. It does not
block the epoch-822 measurement run, which is about throughput and memory rather
than ordering.

It does not need Compact V2 touched at all, so it is independent of the
tail-repair and PoH work, and of the 35 first-seen epochs staying as they are.
