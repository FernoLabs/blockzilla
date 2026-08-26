# Format and upgrader review: the merged layout

Date: 2026-08-14. **Rewritten.** The first draft of this file drew its size,
object-count, speed and point-read evidence from the deleted 26-object,
14-column layout and was wrong on every one of those numbers. All measurements
below come from
[`epoch-2-merged-conversion-2026-08-14.md`](measurements/epoch-2-merged-conversion-2026-08-14.md),
which records the shipped 19-object merged layout. Structural claims are
verified against the code and cited by file.

Reviewing against: **simple**, **fast**, **storage-efficient**, **good for
indexers** (specifically Firewatch), **good for replay**, and **able to rebuild
a compact block for cloud storage**.

Status: the merged format, converter, readers, effects and indexes are
**implemented**, and a real epoch-2 conversion has run. What follows is
assessment and proposals, not a description of unbuilt work.

---

## 0. What epoch 2 can and cannot prove

This matters more than any single number here, so it goes first.

Epoch 2's six runtime effect objects are **64 bytes each — a file header and
nothing else**. Inner instructions: 0. Token balances: 0. The pubkey dictionary
holds **154 records**, of which 69 ever signed and 3 were ever invoked as
programs.

So epoch 2 validates the physical layout, the converter's parallel path, the
catalog, and the ledger encoding. It proves **nothing** about outcomes, CPI,
token balances, logs, effect chunking, effect point reads, or any index whose
value depends on those — Firewatch included. Every claim below that depends on
runtime effects or on account-population scale is explicitly marked unvalidated.

A modern-epoch run is not a nice-to-have. It is the only thing that can move
most of this review from "argued" to "known".

It is worse than "epoch 2 is small". Epoch 2 is **unrepresentative in a
direction that flips the conclusions**: because it carries 59 transactions per
block against a modern block's thousands, tick-driven objects (PoH, shredding,
catalog) are inflated and transaction-driven objects (ledger, runtime, indexes)
are suppressed. §1 works through the arithmetic. Any sentence in any design
document that reasons from epoch-2 *shares* rather than epoch-2 *per-unit costs*
should be treated as suspect, including sentences in the previous revision of
this one.

---

## 1. Corrected measurements

| Measure | Value |
|---|---:|
| Required objects | **19** (20 at epoch 0, with `sidecars/genesis.bin`) |
| Source files | 3,161,090,840 |
| All target files | 3,127,823,899 |
| **Target / source** | **98.9476%** |
| Wall time | 643 s |
| CPU time | 2,124.651 s |
| Average cores used | 3.304 (of 8 workers) |
| Transactions/s | 39,715 |

### Where the bytes are

| Object | Bytes | Share |
|---|---:|---:|
| `sidecars/signatures.bin` | 1,634,366,208 | **52.25%** |
| `sidecars/poh.wincode` | 1,068,339,424 | **34.16%** |
| `ledger/transactions.wincode` | 224,526,909 | 7.18% |
| `sidecars/shredding.wincode` | 72,524,254 | 2.32% |
| `catalog/blocks.wincode` | 62,206,336 | 1.99% |
| derived indexes (4 objects) | 64,976,970 | 2.08% |
| `runtime/block_rewards.wincode` | 864,040 | 0.03% |
| dictionaries (3 objects) | 14,874 | <0.01% |
| runtime effects (6 objects) | 384 | ~0% |

### The reframing this forces, for epoch 2

**Signatures, PoH and shredding are 88.73% of this epoch**, and all three are
either irreducible random bytes or exact retained source frames. The target is
**98.95% of source**. Epoch 2's archive is, to a very good approximation, *the
source bytes reorganised*, plus 2.08% of derived indexes, minus a little.

The first draft's proposals to delta-encode shredding and to replace PoH hashes
with checkpoints are **withdrawn**. Both were computed against the old layout's
re-encoded objects, which no longer exist; both conflict with the decision to
retain those frames exactly; and shredding is 2.32%, far below the threshold
that would justify departing from exact retention. Exact retention is also the
stronger position: a retained frame is provable against the source, whereas a
re-encoded one is a new artifact needing its own proof.

### But do not generalise the shares — they invert on a modern epoch

This is the most important thing in this review, and it is a correction to the
paragraph the previous revision of this file put here.

Deriving per-unit costs from the report exposes what actually drives each object:

| object | per-unit cost | scales with |
|---|---:|---|
| `sidecars/signatures.bin` | 64.00 B/signature | **transactions** |
| `sidecars/poh.wincode` | 36.13 B/entry | **slots** (68.5 entries/block, tick-driven) |
| `sidecars/shredding.wincode` | 2.45 B/boundary | slots |
| `catalog/blocks.wincode` | 144.00 B/block | slots |
| `ledger/transactions.wincode` | 8.79 B/transaction | transactions |
| `indexes/accounts.pages` | 0.405 B/posting | transactions × accounts |

Epoch 2 carries **59.1 transactions per block**. A dense modern block carries
~2,969. PoH cost per block is the same in both cases, because ticks do not care
how busy the chain is. So:

| | epoch 2 | dense modern block |
|---|---:|---:|
| PoH per transaction | **41.84 B** | **0.83 B** |

**PoH is 34.16% of epoch 2 and roughly 0.6% of a modern epoch.** Using the
822-fixture plane totals as an indication of modern transaction content
(77.4 B/tx compressed), the modern mix lands near **ledger + runtime 54%,
signatures 45%, PoH under 1%**.

The epoch-2 distribution is therefore not merely imprecise for a modern epoch —
it is **structurally inverted**. PoH and signatures dominate epoch 2 *because*
epoch 2 has almost no transaction content. Since the 98 TB inventory is
dominated by modern epochs, the shares that matter are the ones not yet measured.

Three consequences:

1. **Retract the "no storage problem inside the format" conclusion** as a general
   claim. It is true of epoch 2 and probably false of epoch 822, where ledger and
   runtime encoding become roughly half the archive. That is the same
   over-generalisation the first draft made from the old layout, made again from
   a second unrepresentative sample.
2. **Exact PoH retention is safer than argued, not riskier.** At modern scale
   PoH is a rounding error, so there is no storage pressure to re-encode it at
   all. This strengthens the decision rather than merely conceding it.
3. **Runtime effect encoding is the biggest unvalidated cost in the format.**
   Epoch 2 reports 0 bytes for all six effect objects. On a modern epoch they are
   plausibly a third of the archive, and not one byte of that path has been
   measured in production.

### The index shares are the least transferable number in the report

`indexes/accounts.pages` costs **0.405 bytes per posting** — sub-byte — because
epoch 2 has **154 distinct accounts** carrying 127.7M postings, or ~829,000
postings per account. Delta-encoded ordinals inside a key that dense compress to
almost nothing, and 2,000 of the 2,002 pages are continuation pages for hot keys.

A modern epoch has ~45M accounts, and on the 822 fixture **69.1% of accounts
were referenced exactly once**. A singleton posting has nothing to delta against
and still costs a key entry plus a full ordinal. **Falsifiable prediction for the
epoch-822 run: bytes per account posting rise by roughly an order of magnitude,
and the derived-index share rises well above 2.08%.** If that does not happen,
the posting-page design is better than this review assumes and the hot/cold
question is settled in its favour.

### On fleet estimates

This run gives 4.916 MB/s of source. **Do not extrapolate it.** Epoch 2 has no
runtime effects and 154 accounts; a modern epoch exercises code paths that did
not execute here at all. The first draft's 174.8-day figure came from the old
converter on the old layout and should not be quoted. There is currently **no
valid fleet conversion estimate**, and producing one requires the epoch-822 run.

---

## 2. Firewatch: wallet signer → programs it interacted with

The requirement is right — Firewatch needs signers, direct programs, and CPI
programs. The first draft's proposal to add a required
`indexes/account_programs` object is **withdrawn**. Six problems, all
disqualifying for a *required archive index*:

1. **Outcome semantics are missing.** Firewatch counts successful transactions
   only. A bare (account, program) relation carries no success bit, so it cannot
   answer the actual question.
2. **The existing indexes disagree with it.** `indexes/accounts` and
   `indexes/programs` include failed transactions, so a relation built beside
   them would silently mean something different from its neighbours.
3. **It is not "nearly free".** A `BTreeSet` insert per relation per transaction
   is fine at fixture scale and is not fine at 127.7M account references per
   epoch. It needs a bounded accumulator or an external sort — the machinery the
   other builders already use.
4. **It quietly expands the goal.** Storing every account→program pair covers
   every PDA and passive account, when the requirement is wallet signers.
5. **The 90–150 MB estimate is unvalidated.** It was extrapolated from a single
   dense block in the old layout, and epoch 2 cannot check it.
6. **A wallet query cannot start.** `dictionary/pubkeys.pages` maps ordinal →
   key. There is **no reverse lookup** in the archive, so a query that begins
   with 32 wallet bytes has no way to reach an ordinal without an MPHF or an
   equivalent Firewatch-owned structure. This is a real gap and it blocks the
   use case regardless of what relation is stored.

### The right shape

Keep Firewatch as a **separate derived V3 projection**, not a required archive
object:

- use `dictionary/account_flags` to get the dense signer population;
- scan transactions, outcomes and CPI **once**;
- **exclude failed transactions**;
- emit signer→program pairs through a bounded external sort or the existing
  dense accumulator;
- bind the result to the archive ID, the pubkey dictionary, the lookup
  structure, the CPI policy, and a Firewatch semantic version.

The binding matters as much as the data: without it, a projection cannot be
shown to correspond to the archive it was built from, and it silently rots when
CPI policy or the dictionary changes.

Epoch 2 cannot exercise any of this — no outcomes, no CPI.

---

## 3. Wincode

This is the part of the first draft that survives, and the conclusions hold.

**Keep:**

- **Canonical LEB128 integers are correct.** `ArchiveWincodeConfig` is
  `Configuration<…, CanonicalLeb128, u8>`, so a `Vec<PubkeyId>` still costs about
  one byte per hot account and the usage-ordered registry still pays off. One
  value has exactly one byte string.
- **One-byte enum tags are correct.**
- **Fixed 144-byte catalog rows are correct** — `WireBlockRow` uses `[u8; 8]`
  fields deliberately, which is what makes rows positioned-readable and
  binary-searchable. Fixed width for the searchable index, varints for the
  payload.
- **The 64 MiB `PREALLOCATION_SIZE_LIMIT` is a per-sequence corruption guard,
  not a process-memory limit.** Given that the converter's real constraint is
  memory, this needs to compose with the pipeline byte budget rather than sit
  beside it.

**Two qualifications to add:**

1. **Do not force Wincode onto raw signatures, raw dictionaries, or compressed
   posting indexes.** The layout already gets this right —
   `sidecars/signatures.bin` and `dictionary/*.pages` are flat records and
   `sidecars/genesis.bin` is `FileEncoding::ExactBytes` — and it should stay that
   way. Wrapping fixed-stride random bytes in a schema adds framing and removes
   the stride arithmetic that makes them cheap.
2. **Pin every Compact V2 reader to exactly Wincode 0.5.5, with frozen
   real-frame fixtures.** Verified state: `blockzilla-index-archive-convert`
   declares `=0.5.5` and `blockzilla-index-archive-format` declares `=0.6.0`,
   both exact — but **`blockzilla-format`, the crate that owns the Compact V2
   wire structs, declares `wincode = "0.5.4"`, a caret range.** `Cargo.lock`
   happens to resolve it to 0.5.5 today, which hides the looseness; a
   `cargo update` can move it inside 0.5.x. Given that this whole project
   already lost a day to a Compact V2 enum changing without a version bump, the
   decoder crate is exactly the one that must not float.

**Correction to the first draft:** "one grammar for every structured object" and
"every object carries a 64-byte header" are both wrong. Retained PoH and
shredding use explicitly selected Compact V2 wire profiles, and epoch-zero
`genesis.bin` is headerless exact bytes. The accurate statement is: *one grammar
for every object the target itself defines; explicitly bound source profiles for
retained frames; exact bytes for genesis.*

---

## 4. Simplicity

**19 required objects** (20 at epoch 0), not 26. Each is single-purpose, and the
merges that produced this count are sound: `outcome + return_data` and
`inner_instructions + inner_instruction_data` were always decoded together.

The transaction record is the right unit. `Message::{Legacy, V0}` as a tagged
enum means V0-only facts cannot be absent on a legacy transaction or invented on
a V0 one, and `LoadedAddresses::Unavailable` keeps "unknown" distinct from
"none" — the distinction a naive schema always loses.

---

## 5. Speed

**There is no read-cost measurement for this layout at all.** That is the
headline, and it deserves more weight than it has been given.

Since the archive is 98.95% of source and 88.73% retained or irreducible bytes,
the format's entire value proposition is **read performance and indexability**,
not storage. Yet the only read evidence in the file was "5,810 bytes, 1.67 ms",
which was taken on the deleted column layout and is withdrawn. The merged run's
read checks passed at slot 872,069, but they were correctness checks, not cost
measurements. **The format is currently justified by a property nobody has
measured on it.**

What else can honestly be said:

- **3.304 average cores from 8 workers** is the parallel efficiency to improve,
  and it was measured on an epoch with no runtime effects — so the
  effect-encoding path the page workers exist to serve barely ran.
- **Effect point reads are unmeasured.** Chunks are 256 transactions wide
  (`EFFECT_CHUNK_TRANSACTIONS`), which is right for whole-block work and means a
  single transaction's outcome may decode up to 256 records.
- **The ledger arena is zstd-compressed per block** (`PageSpan::is_compressed`,
  `stored_len != decoded_len`), which is how 1.52 GB of instruction data lands in
  a 224 MB object. Epoch 2's vote-dominated payloads compress ~7×; the 822
  fixture measured 6.5× for top-level instruction data but only **2.0×** for CPI
  data. A modern epoch will not hold 7×, which is a second reason the 7.18%
  ledger share does not transfer.

The catalog is 62.2 MB, 1.99%. Delta-encoding it saves at most ~2% and
complicates the positioned-read path that fixed 144-byte rows exist to provide.
**Defer** — and note that on a modern epoch its share falls further, since it
scales with slots while transaction content grows.

---

## 6. Replay

The first draft said "nothing to change". That was wrong. The correct statement
is: **the replay facts are present, but the replay adapter and its proof are
incomplete.**

The data model is good — `Transaction` carries header, recent blockhash, static
accounts, resolved loaded addresses, lookups, and instructions with data inline,
so replay reads one object and never opens an effect. Instruction bytes come
from the exact signed message and a raw fallback is refused outright, which
closes the old §7.5 question.

Outstanding:

- **The target permits far more than Solana does.**
  `MAX_ACCOUNTS_PER_INSTRUCTION` and `MAX_INSTRUCTIONS_PER_TRANSACTION` are both
  `1 << 16`, but Solana instruction account indexes are `u8`, so at most **256**
  resolved positions can be addressed. The guard should reflect the real
  protocol bound, not a decoder-allocation bound.
- **Invalid shapes are not rejected**: fee payer as program, loaded V0 program
  IDs, empty lookups, and some version/header combinations.
- **There is no target-native signed-message serializer.**
- **Signature verification is partial.** The upgrader uses one fee-payer
  signature to disambiguate instruction bytes; it does not rebuild the completed
  target transaction and verify every signature.
- **The public reader does not return runtime-ready Solana transactions.**
- **Real replay additionally needs** account state, runtime and feature policy,
  fork context, and finality evidence — none of which this archive holds, by
  design.

---

## 7. Compact-block rebuild

Directionally correct in the first draft, and it stays the first major task.
Every input is present and range-addressable from one catalog row.

Keep:

- build a **deterministic cloud serving bundle**;
- compare **decoded values**, not old compressed bytes;
- use reconstruction as the **migration parity gate**.

The proof must be stronger than the first draft stated. It needs all of:

1. exact signed-message bytes;
2. **every** signature verified;
3. runtime-effect parity;
4. block-reward parity;
5. exact retained PoH and shredding frame checks;
6. `getBlock` result parity;
7. archive / source / profile binding.

A byte-identical old Compact V2 generation is neither achievable nor necessary.
A **semantically equal cloud serving bundle** is achievable, and it is what
authorises deleting a source — not the converter's exit code.

---

## 8. Priorities

1. **Target-native transaction reconstruction and all-signature verification.**
2. **Compact cloud-bundle builder and semantic round-trip gate.**
3. **Typed publication manifest, chain/finality proof, atomic cutover.**
4. **Complete historical Compact V2 source-profile inventory.**
5. **Selective production reader for Firewatch and replay.**
6. **Pubkey→ID lookup for wallet queries** (blocks Firewatch entirely).
7. **Full outcome, CPI, loaded-address and PoH coverage reporting.**
8. **Epoch-822 storage, memory, scratch and throughput run.**
9. Only then: catalog or transaction-region optimisations.

### One argument for re-ordering, offered rather than applied

This list is kept in the order it was given. Two findings from the latest pass
bear on items 8 and 9, and the call is the owner's, not this document's:

- **Item 8 (epoch-822 run) now gates more than it did.** The share inversion
  (§1) means the epoch-2 distribution predicts almost nothing about the 98 TB,
  and item 9 ("only then consider catalog or transaction-region optimisations")
  cannot be evaluated at all without it. Every deferred encoding decision below
  is deferred *pending item 8*, so it is closer to a prerequisite than to an
  eighth task.
- **A read-cost benchmark is missing from the list entirely.** The format's
  justification is read performance (§5) and no measurement of it exists on this
  layout. It is cheap next to items 1–3 and it is what tells you whether the
  merged record and 256-transaction effect chunks were the right calls. Suggest
  folding it into item 8 rather than adding a tenth item.

Neither changes items 1–7, which are correctness and publication gates and
rightly come first regardless of what the size numbers say.

Deferred, with the reason:

| item | status |
|---|---|
| Required `account_programs` index | replaced by an optional Firewatch projection (§2) |
| Delta-encode shredding | **removed** — conflicts with exact retention; 2.32% |
| PoH checkpoints | **removed** — conflicts with exact retention |
| Delta-encode catalog | deferred — ~2%, complicates point reads |
| Split account/body regions | deferred — needs a modern-epoch benchmark; helps account-only reads, not Firewatch |
| Measure effect point reads | **keep** — currently unmeasured |

---

## What survives, and what this file got wrong twice

**Survives:** merged transactions as one replay unit; effects split and never
read by replay; the Wincode grammar choices; semantic — not byte-identical —
cloud reconstruction as the round-trip gate. Those conclusions have now held
across two rounds of corrected measurements, which is the best evidence
available that they are structural rather than artifacts.

**Wrong in revision 1:** every size, object-count, speed and point-read number,
because they came from the deleted 26-object column layout.

**Wrong in revision 2:** the conclusion that there is "no storage problem inside
the format". That was correctly derived from epoch 2 and does not generalise,
because epoch 2's share distribution inverts on a modern epoch (§1).

The pattern in both failures is the same: reasoning from *shares* measured on
one unrepresentative sample instead of from *per-unit costs* and what each unit
scales with. The per-unit table in §1 is the durable artifact here; the shares
around it are provisional until the epoch-822 run.
