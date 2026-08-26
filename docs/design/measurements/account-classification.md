# Classifying accounts: what one generation can see, and what it cannot

Measured on the `epoch-822-biggest.car` block (2,969 transactions, 10,537
top-level + CPI instructions, 5,736 registry accounts). Reproduce with:

```bash
blockzilla build-archive-v2-hot-blocks <fixture.car> /tmp/v2hot
blockzilla-index-archive-convert /tmp/v2hot /tmp/planes
account-relations /tmp/planes
```

`account-relations` decodes the written planes through the format crate's own
decoders rather than re-deriving the encodings, and its counts are produced by a
code path independent of the converter's. The two agree exactly (1,337 / 93 /
4,306), which is the cross-check that makes the rest of this trustworthy.

Status: the flag table is **implemented**. The relation sidecar and the
recompaction pass are **findings, not decisions**.

---

## 1. Per account, not per reference

`dictionary/account_flags.pages` is one byte per registry entry: `FLAG_SIGNER`,
`FLAG_PROGRAM`, spare bits.

The ratio is what justifies it. Epoch 822 holds ~45M distinct accounts and ~7.5B
references to them — **168× more references than accounts**. Flagging the
account costs 45 MiB for the epoch; flagging each reference costs gigabytes to
say the same thing 168 times.

| | fixture | epoch 822 (projected) |
|---|---:|---:|
| raw | 5,736 B | ~45 MiB |
| zstd-3 | 1,110 B | ~8.7 MiB |

It also removes a column from a common filter. "Which programs did this wallet
call" previously had to walk `ledger/instructions` — 3.9 GiB at epoch scale —
purely to map program positions back to accounts.

## 2. What the fixture actually contains

| | accounts | share |
|---|---:|---:|
| signers | 1,337 | 23.3% |
| programs | 93 | 1.6% |
| both | 0 | — |
| **neither** | **4,306** | **75.1%** |

**24 of the 93 programs are reached only through CPI.** They appear in no
top-level `program_id_index` at all, so a filter that walks top-level
instructions alone misses 26% of the programs in the block — precisely the
inner ones. The converter's first version dropped these; it now collects both.

## 3. The "neither" bucket is not the PDA set

The tempting reading is that neither-signer-nor-program means program-derived
address. The relation data says otherwise. Grouping the 4,306 by how many
distinct programs named them in an instruction:

| distinct programs | accounts | share |
|---:|---:|---:|
| **0** | **313** | **7.3%** |
| 1 | 2,536 | 58.9% |
| 2 | 719 | 16.7% |
| 3 | 349 | 8.1% |
| 4–9 | 346 | 8.0% |
| 10+ | 32 | 0.7% |
| tail | 45 (max) | — |

**313 accounts appear in no instruction whatsoever.** They are present only as
fee or balance participants — wallets that received SOL and did nothing else.
Calling them PDAs is simply wrong, and nothing in the ledger distinguishes them
from a real PDA that happened to be idle.

This is the honest limit of use-derived classification: it observes roles, and
absence of a role is not a role.

### What would settle it

On-curve-ness. A PDA is off-curve **by construction** — derivation retries the
bump until the candidate is not a valid Ed25519 point, precisely so no private
key can exist for it. `CompressedEdwardsY(key).decompress().is_some()` is the
whole test, it needs the 32 key bytes and no ledger context, and `blockzilla-replay`
already runs it in `compiler.rs` to reject on-curve derivations.

That makes the sound classification `off-curve ∧ used-by-exactly-one-program`,
rather than the current `neither ∧ hopeful`.

`FLAG_ON_CURVE` is deliberately unallocated for now, and `set_flags` **rejects**
the spare bits rather than accepting them, so a generation written today can
never be misread as having computed a bit it did not.

## 4. The account→program relation

9,342 distinct `(account, program)` pairs against 44,152 account references in
instructions — **4.7× fewer**, and 1.63 pairs per account.

| storage | fixture | epoch 822 (floor) |
|---|---:|---:|
| single `u32` owner per account | 22,944 B | ~180 MiB |
| explicit pair list @ 8 B | 74,736 B | ~587 MiB |

Treat the epoch figures as a **floor**: this is one block, and an account's
program set can only grow as more blocks are folded in. 58.9% single-program is
likewise an upper bound on how often a single `u32` owner column suffices.

The relation is **usage, not ownership**. "Program P named account A in an
instruction" includes every wallet passed to a transfer. True ownership is the
account's `owner` field, which lives in account state, not in transaction data —
replay produces it, this archive does not carry it.

---

## 5. Why the flags belong in a recompaction pass, not only in conversion

Every number above is scoped to one generation, and that is the flags' real
weakness: a wallet that signed nothing *in this generation* is not flagged as a
signer, even though it signed in the next one. The 313-account anomaly in §3 is
this effect in miniature.

Conversion cannot fix it. It streams one generation at a time with no global
state — which is deliberate, because that is what lets the migration run N
epoch converters in parallel.

### The sidecar already exists

For signer and program, no new file is needed. Per generation,
`dictionary/pubkeys.pages` (the 32-byte keys) plus
`dictionary/account_flags.pages` (the byte per key) **is** the observation
record. Recompaction is a fold of those pairs across generations, keyed by the
32-byte pubkey.

The key must be the pubkey, not the ordinal, because:

### Ordinals are generation-local

The registry is copied verbatim from each V2 generation's own (usage-ordered
or first-seen -- see where-the-bytes-are.md 5.2)
registry, so ordinal 5 in one generation and ordinal 5 in the next are different
accounts. A global registry therefore has different ordinals from every
generation that feeds it. Two ways to reconcile:

1. **Rewrite `ledger/accounts.pages` to global ordinals.** 7.5B references per
   epoch rewritten, and every generation's bytes change, so every folder hash is
   invalidated.
2. **Keep generation-local ordinals; add a `local → global` map per
   generation.** `registry_entries × 4` bytes, a pure addition.

Option 2 is the one to build, and the reason is not size. It makes recompaction
**additive**: generations stay byte-identical, the folder hashes stay valid, and
the global layer can be *rebuilt from scratch* whenever the inputs improve —
when `FLAG_ON_CURVE` is added, when replay yields real owners, when a later
epoch reveals an account was a signer after all. A rewrite would make each of
those a migration.

It also puts on-curve in the right place: a curve decompress per **globally
distinct** key, paid once, instead of once per generation the account appears in.

### Suggested staging

1. Fold `(pubkeys, account_flags)` across generations into a global key→flags
   table. Nothing new is written during conversion.
2. Compute `FLAG_ON_CURVE` in that pass and allocate the bit.
3. Emit the `local → global` ordinal map per generation.
4. Only then decide the relation sidecar's shape, using an epoch-wide pair count
   rather than this block's 1.63/account floor.

Steps 1–3 need no change to any existing column, which is the property worth
protecting.
