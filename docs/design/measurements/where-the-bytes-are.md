# Where the bytes actually are, and what that means for indexes

Sources: the epoch-2 conversion report (`epoch-2-real-conversion-2026-08-14.md`,
431,988 blocks / 25,536,956 transactions / 29,569,805 PoH entries) for the byte
distribution, and `index-shape` over the `epoch-822-biggest` fixture for the
index-shape numbers.

Reproduce the second with:

```bash
index-shape <generation-dir>
```

Status: **findings and proposals.** Nothing here is implemented.

---

## 1. The archive is not made of transactions

Splitting the epoch-2 sidecar group into its three objects gives the real
picture:

| object | bytes | share of archive |
|---|---:|---:|
| `sidecars/signatures` | 1,634,370,304 | **36.50%** |
| `sidecars/poh` | 1,431,927,012 | **31.98%** |
| `sidecars/shredding` | 478,300,800 | **10.68%** |
| derived indexes | 473,297,243 | 10.57% |
| ledger columns | 272,674,033 | 6.09% |
| block catalog | 124,416,704 | 2.78% |
| runtime columns | 62,723,448 | 1.40% |
| dictionaries | 18,970 | <0.01% |

**Ledger plus runtime — every fact about every transaction — is 7.49% of the
archive.** The other 92.5% is chain-verification material, indexes, and the
catalog.

This retires a line of work. The column codecs measured earlier
(`token_balances` −53%, `balances` −46%, `outcomes` −26%) were real, and they
optimised the 1.40% row. Instruction-data dedup, which measured a genuine win
over plain zstd at small page sizes, targets part of the 6.09% row. Neither can
move the archive by more than a couple of percent. **Stop optimising
transaction bytes.** The storage problem is PoH, signatures and shredding.

---

## 2. PoH: 21% of the archive is a hash we can recompute

A source PoH entry is:

```rust
pub struct CompactPohEntry {
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub tx_count: u32,
    pub signature_count: u32,
}
```

29,569,805 entries at 48.4 stored bytes each. The 32-byte hash alone is
946,233,760 bytes — **66% of the PoH object and 21.1% of the entire archive**.

That hash is *derived data*. Entry N's hash is SHA256 iterated `num_hashes`
times from entry N−1's hash, mixing in the entry's transaction signatures. Every
inifput to that computation is already stored: `num_hashes` here, the signatures
in `sidecars/signatures`, the structure in `tx_count` / `signature_count`.

The consequence is the part worth arguing about: **storing the hash buys no
verification.** PoH verification recomputes the chain. Comparing the result
against a stored hash proves only that the file agrees with itself — it is not
independent evidence, because the stored hash is the output of a function whose
inputs are in the same file. The one PoH hash per block that *is* externally
meaningful is the blockhash, and that already lives in
`dictionary/blockhashes`, referenced by every transaction.

### Proposal: checkpoints, not every hash

Store per entry: `num_hashes`, `tx_count`, `signature_count`. Store the full
32-byte hash only every K entries.

| | bytes |
|---|---:|
| per-entry fields, ~4 B LEB128 × 29.6M | ~118 MB raw |
| — after zstd (`num_hashes` is near-constant) | ~20–40 MB |
| checkpoints at K=1024: 28,876 × 32 B | ~0.9 MB |
| **total** | **~25–45 MB** |
| today | 1,432 MB |

Roughly a **97% cut to 32% of the archive.**

K sets the cost of a spot check: verifying one entry means recomputing from the
previous checkpoint, so K bounds the work. K=1024 keeps checkpoints under a
megabyte per epoch while making any single entry cheap to re-derive, and lets
verification of disjoint segments run in parallel.

**This must be proven before it is adopted**: rebuild an epoch's PoH from the
reduced form and run the existing verifier against the real chain. Until that
passes, this is arithmetic, not a result.

## 3. Shredding is a regression, not a cost

`CompactShredding` is two monotonically ascending `i64`. Native stores them raw
at 16.2 B/entry; the legacy sidecar delta-encodes at 2.45 B/entry — the new
object is **6.6× larger for identical information**.

Delta-encoding ascending values is the whole fix: 478 MB → ~73 MB, about **9% of
the archive**, with no design question to settle.

## 4. Signatures are the floor, and that is fine

64 random bytes per transaction, 25,536,971 of them. Ed25519 signatures do not
compress, and they cannot be dropped: they are both the proof of signed truth
and the lookup key users search by. There are 15 more signatures than
transactions, so there is no per-transaction waste to reclaim either.

After the PoH and shredding fixes, epoch 2 lands near:

| | bytes |
|---|---:|
| signatures | 1,634 MB |
| indexes | 473 MB |
| ledger | 273 MB |
| catalog | 124 MB |
| shredding | ~73 MB |
| runtime | 63 MB |
| PoH | ~40 MB |
| **total** | **~2,680 MB** |
| source | 3,161 MB |

**84.8% of source, against 141.65% today.** That is the number that decides
whether converting with source deletion is possible at all, and it flips it from
impossible to comfortable. Signatures then become ~61% of the archive, which is
the correct end state: the archive is mostly the one thing that is irreducible.

---

## 5. Indexes: three ways to spend far less

### 5.1 Index blocks, not transactions — 8.14× fewer postings

Reads are block-by-block by decision, so a posting list naming transactions is
finer than any reader consumes. Measured on the fixture block:

| | postings |
|---|---:|
| (account, transaction) | 44,152 |
| (account, block) | 5,424 |
| **collapse** | **8.14×** |

Honest bound: the collapse is capped by transactions per block, and the fixture
is a dense 2,969-transaction block while epoch 2 averages 59. So 8.14× is what
modern epochs would see, and early epochs see much less — which is the right way
round, because modern epochs are what dominate the 98 TB.

A reader that wants the transaction still gets it: it reads the block it was
pointed at, which it was going to read anyway.

### 5.2 The registry ordinal is NOT a reliable frequency rank

**Corrected 2026-08-16.** This section previously proposed splitting the account
index at an ordinal threshold, arguing "the pubkey registry is usage-ordered, so
the ordinal is already a frequency rank, and choosing between encodings needs no
metadata". That came from a code comment, not from the archive. Measured on the
NAS, the archive holds **two different orderings**, and they separate exactly:

| `registry-first-seen.manifest` | epochs sampled | descending-adjacent counts |
|---|---|---:|
| absent | 100, 400, 500, 700, 902, 950 | **100.0%** — sorted by usage |
| present | 277, 305, 405, 505, 864, 997, 1000, 1012 | **69–90%** — first-seen |

**978 epochs are usage-sorted; 35 are first-seen and declare it:**

```text
version=1
registry_order=first_seen_v1
count_semantics=all_compact_pubkey_refs_v1
seeded_keys=65536
next_seed_file=registry-hot-seed.bin
```

Two consequences:

1. **The ordinal-threshold trick cannot be a format invariant.** It is right on
   978 epochs and wrong on 35 — including **every epoch from 1000 to 1012**, so
   the archive is trending toward the ordering that breaks it.
2. **Low ordinals are still hot-biased on first-seen epochs, by a different
   mechanism.** The first `seeded_keys=65536` entries are carried forward from
   the predecessor's hot set, which is why those epochs measure 69–90%
   descending rather than the ~50% of pure arrival order. So the 1-byte-ordinal
   saving survives; the *frequency-rank* interpretation does not.

**What to do instead.** Key a hot/cold split on a fact the format states rather
than an ordering it hopes for: make the ordering manifest required rather than
optional, or derive the hot set from `registry_counts.bin` — present in every
epoch, holding per-key reference counts under
`count_semantics=all_compact_pubkey_refs_v1`.

**Detection test for the 978 undeclared epochs:** decode `registry_counts.bin`
and measure descending-adjacent pairs. 100% means usage-sorted, below ~90% means
first-seen. It separated the two groups with no overlap on every epoch sampled.

### 5.3 Do not index what a filter already answers

`dictionary/account_flags` answers "is this a program / was this a signer" for
one byte per account. Any index that re-encodes those facts per posting is
paying ~168× over for them (7.5B references vs 45M accounts in a modern epoch).

---

## 6. The format change that serves replay and indexing at once

The uncommitted refactor merged five ledger columns into one
`ledger/transactions.wincode` record because replay had to join five files per
transaction. That is a real cost and the fix is right. But it takes away the
projection the columnar layout had, and index building is exactly the workload
that needs it. Measured on the fixture:

| | stored | decoded |
|---|---:|---:|
| whole ledger | 80,910 | 473,766 |
| `accounts` alone | 18,996 | 66,679 |
| accounts as a share | 23.5% | **14.1%** |

**An index build that decodes whole transactions reads 7.1× the bytes it
needs.** Every account index, program index and account-flag pass in the archive
pays that multiple.

### Proposal: two regions inside the block record

Lay each block's `transactions` object out as two contiguous regions:

1. **account region** — every transaction's account id vector, back to back;
2. **body region** — headers, instructions, instruction data, lookups.

The catalog stores the boundary: one `u32` per block, 1.7 MB per epoch.

- **Replay** reads both regions as one sequential range. Identical cost to
  today — no join, nothing re-introduced.
- **Index building, account filtering, the `account_flags` pass** read only the
  account region: **7.1× fewer bytes**.
- **Point reads** are unaffected; `RowRestart` still addresses the body.

This is columnar *within* a block and row-wise *across* blocks, which is the
shape that matches both consumers instead of trading one against the other.

### And shrink the catalog

288 bytes per block × 431,988 = 124 MB, and the catalog is read by every query,
so its size is a read cost as well as a storage cost. Grouping objects already
cuts it from fourteen spans to four locators. Offsets are monotonic, so
delta-encoding them takes it to roughly 40 B/block — about 17 MB per epoch.

---

## 7. Priority

1. **Delta-encode shredding.** No design question, ~9% of the archive.
2. **Split the block record into account and body regions.** One `u32` per
   block, 7.1× off every index build, and it removes the only real objection to
   the merged-record refactor.
3. **PoH checkpoints.** Largest single win at ~31% of the archive, but gated on
   rebuilding an epoch and passing the real verifier.
4. **Block-granular postings, hot/cold split by ordinal.** Attacks the 10.57%
   index group with an 8.14× posting collapse on dense epochs.
5. **Delta-encode catalog offsets.**
6. Instruction-data dedup and further column codecs — **deprioritised**, because
   the entire ledger and runtime group is 7.49% of the archive.
