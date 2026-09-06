# Blockzilla Index Archive

Status: **format proposal + migration path**. Nothing here is frozen. Widths,
page sizes, and codec settings are provisional and are decided by the
measurements named in §7.

> **Active migration correction (2026-08-25).** The normalized transaction and
> runtime codecs below remain a possible final read format. They are not the
> active conversion contract. The fast migration first performs a mechanical,
> source-preserving split: it retains exact `CompactPubkey::{Id, Raw}` and
> blockhash namespaces, typed messages, compact logs, exact metadata fields,
> and raw/missing fallback states. Conversion does not verify Ed25519, run PoH,
> assign IDs, hash content, or reread output. Verification and sealing are
> separate later jobs. See
> [the fast-converter review](indexer-archive-fast-converter-review-2026-08-25.md).

This replaces Archive V2 as the published archive format. It is written for two
readers: an **indexer** filtering by account/program/instruction, and a
**replayer** loading transactions into a runtime. Everything else — get-block
serving, PoH verification, shredding — is served without penalising those two.

---

## 1. What decides this design

Design notes that are not grounded in the current read paths produce ceremony.
These are the facts this format is built around, each one checked against the
code rather than assumed.

### 1.1 The workload, as it actually exists

| Consumer | What it does today | Cost |
|---|---|---|
| Firewatch index build | Full archive re-decode **once per 8M-account chunk** — [`build.rs:550`](../../indexer/blockzilla-firebase-indexer/src/build.rs#L550), `DEFAULT_MAX_ACCOUNTS_PER_CHUNK = 8_000_000` | N full decodes of an epoch to build **one** index |
| Account resolution | `decode::decode_metadata_prefix` — a hand-written field-order walker that decodes into transaction status metadata purely to reach `loaded_addresses` ([`decode.rs:976`](../../indexer/blockzilla-firebase-indexer/src/decode.rs#L976)) | Every V0 transaction pays a metadata decode to learn its own account list |
| Remote read | Gateway serves whole files with HTTP `Range` — `/v1/epochs/{epoch}/files/{name}` ([`gateway lib.rs:501`](../../blockzilla/archive-gateway/src/lib.rs#L501)) | Access granularity is a ranged GET; per-request latency dominates, so page layout and directory size matter more than raw decode speed |
| Replay | Needs message + account list + recent blockhash, and explicitly *not* effects. `SplitCompactIndexRecord` already splits `block_offset/len` from `runtime_offset/len` | The separation is already understood; it just isn't the published shape |

The first row is the whole argument. **The archive is re-decoded N times to
build a single reverse index, because it is row-major and the account list is
not a column.** Every other complaint — version sprawl, unclear file layout,
lookup tables buried in metadata — is real but secondary to that.

### 1.2 What replay pays for today

`ArchiveV2HotBlockBlob` holds `message_bytes` and `metadata_bytes` in **one
zstd frame**. Replay needs the header, the transaction row directory, and the
messages. It needs none of the metadata — and it decompresses all of it anyway.

Measured on real Archive V2 hot blocks, built with
`blockzilla build-archive-v2-hot-blocks` from the two CAR fixtures:

| Fixture | Txs | message | metadata | metadata share | replay needs | waste |
|---|---:|---:|---:|---:|---:|---:|
| epoch-822 biggest (modern) | 2,969 | 503,069 B | 1,029,814 B | **67.2%** | 36.3% | **2.76×** |
| epoch-157 biggest (2020) | 4,208 | 360,892 B | 292,692 B | 44.8% | 62.1% | 1.61× |

Two results:

1. **Replay decompresses ~2.8× the bytes it uses on a modern block.** Splitting
   effects into their own plane is not a tidiness argument — it is a direct
   2.8× cut in replay's decompression budget, and it grows as blocks get more
   CPI- and log-heavy.
2. **Effects are two thirds of a modern block's payload.** That sets the prize
   for §5.2: any scheme that avoids storing recorded effects is aiming at the
   majority of archive bytes, not a rounding error.

The trend between the two epochs matters as much as the values. Metadata's
share went from 45% to 67%; message bytes barely moved because Archive V2
already stores registry IDs instead of 32-byte pubkeys. The effects plane is
where the growth is.

Reproduce with `v2-plane-split <generation-dir>`.

> Measured on Archive V2, not on the source CAR. The `*_frame_bytes` fields in
> the account-index reports describe old-faithful CAR frames — the migration
> *input* — where metadata is stored as zstd-compressed protobuf rather than
> `CompactMetaV1` with the compacted log stream. Those numbers say nothing
> about what our format costs and must not be read as if they did.

### 1.3 Storage facts, measured

From `epoch-822-biggest.car` (1 block, 2,969 transactions, 44,152 account
references) via `blockzilla-index-archive-measure`, zstd-3 at 64 KiB pages:

| Structure | Unit cost | Note |
|---|---|---|
| Pubkey dictionary | **32.0 B / unique key** | Incompressible. zstd *grew* it (182,334 vs 182,304 raw). Store dictionary pages raw. |
| Forward account column | **0.40 B / reference** | ULEB128 registry IDs + zstd |
| Reverse account postings | **0.36 B / posting** | ID gap + tx gap + 4-bit role, + zstd |

Two consequences, both load-bearing:

- **The dictionary dominates and cannot be compressed.** Epoch storage is
  driven by unique-pubkey count, not by encoding cleverness in the columns.
  Effort spent shaving column bytes is effort spent on the small half.
- **The reverse index is cheap.** At 0.36 B/posting, keeping both
  `tx → accounts` and `account → txs` costs ~7.8% on top of dictionary +
  forward column. The "are we storing it twice?" concern is settled: the
  *relationship* is stored twice, the *bytes* once, and the second copy is
  ~0.36 B per edge. Keep both directions.

### 1.4 One correction to the prior pass

The earlier draft proposed `dictionary/pubkey_lookup` as a sorted table of
64-bit SHA-256 fingerprints + `u32` ID, measured at 62,486 B — 34% of the
dictionary and the single largest index cost. The repo already solves this
better: [`registry.rs`](../../crates/compact-v2/blockzilla-registry/src/registry.rs) ships a
minimal perfect hash (`fmph::GOFunction`) with a tag table for miss detection,
file-backed and already bound into the indexer's query path. **Reuse it.** Do
not introduce a second pubkey→ID structure.

---

## 2. Identity and versioning

This is the complaint that started the review, and it is worse than it looked.
`blockzilla-format` carries **12 independent version constants** —
`WINCODE_ARCHIVE_V2_VERSION`, `ARCHIVE_V2_HOT_INDEX_VERSION`,
`ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION` (a "v3" file inside "V2"),
`KEY_INDEX_VERSION`, `BLOCK_TIME_GAP_VERSION`, and more — plus ~25
`V1`/`V2`-suffixed public types. None of them identifies an archive generation.
Readers additionally *trial-decode*: [`v2/mod.rs`](../../crates/compact-v2/blockzilla-archive-v2/src/v2/mod.rs)
tries a schema and updates it on fallback.

The rules:

1. The format identifier is `blockzilla-index-archive`; the first format major
   is `1`.
2. **Version lives at the wire boundary**: the manifest and a fixed 64-byte
   header on every headered binary object. Exact-byte objects, such as the
   epoch-zero genesis file, are selected and hashed by the manifest instead.
3. **No version suffixes on Rust types.** `BlockRow`, `Transaction`, and
   `TransactionOutcome` have no schema suffix. The type describes the current
   format; older bytes are handled by a decode-time shim at the boundary.
4. **No trial-decoding, ever.** A headered object states its role and schema
   before a byte of payload is read. The manifest supplies this identity for
   an exact-byte object. A reader that cannot identify an object fails; it does
   not guess.
5. The Rust module tree mirrors the physical archive tree, so "where do I look
   to read `indexes/accounts.pages`?" has exactly one answer:
   `src/indexes/accounts.rs`.

Rule 5 was the other half of the original complaint and the previous pass got
it right — the module tree already mirrors the layout. That is kept as-is.

---

## 3. Physical layout

```text
<epoch>/
  manifest.json                      # planned publication authority; not implemented yet
  catalog/
    blocks.wincode                   # block identity and four block-level locators
  dictionary/
    pubkeys.pages                    # 32 B/key, RAW pages (never zstd — §1.2)
    blockhashes.pages                # hashes not owned by the PoH sidecar
    account_flags.pages              # derived generation-wide signer/program flags
  ledger/                            # ← SIGNED TRUTH. replay reads one stream
    transactions.wincode             # complete runtime transaction input
  runtime/                           # ← RUNTIME OUTPUT. selected independently
    inner_instructions.wincode       # CPI structure and raw data together
    outcomes.wincode                 # outcome and return data together
    balances.wincode                 # pre/post lamports
    token_balances.wincode           # pre/post token balances
    logs.wincode
    rewards.wincode                  # per-transaction rewards
    block_rewards.wincode            # one block-scoped record per block
  indexes/                           # ← fully derived, deletable, rebuildable
    accounts.pages  programs.pages  selectors.pages
    slots.idx
  sidecars/
    poh.wincode  shredding.wincode  signatures.bin
    genesis.bin                         # epoch 0 only
```

### 3.1 Group by the read operation

The old candidate had five transaction files and nine runtime files. Replay had
to join five files to rebuild one transaction. The block catalog also carried
fourteen full page spans.

The new layout groups facts that a consumer always reads together:

- **Replay** reads `ledger/transactions.wincode`. It does not read effects.
- **An account filter** first reads `indexes/accounts.pages`.
- **A program or selector index build** reads transactions and CPI. Raw
  instruction data is next to its instruction in both streams.
- **get-block** reads transactions and only the requested effect streams.

Effects stay separate because they are not replay input. Two pairs are merged
because their members are always decoded together:

- CPI structure with CPI instruction data;
- transaction outcome with return data.

Block rewards stay block-scoped. They do not get a transaction index.

### 3.2 The transaction record

One logical transaction row contains all signed and runtime-input facts:

```text
Transaction
  message header
  recent blockhash reference
  Legacy or V0 message kind
  static account registry IDs
  resolved loaded writable and readonly account registry IDs
  V0 lookup descriptors
  top-level instructions with raw data inline
```

The vectors own their lengths. The record does not repeat account, lookup,
instruction, or data counts in another file. Signature bytes stay in
`sidecars/signatures.bin`. The message header owns the required-signature
count. A parallel one-byte `TransactionBlock.effect_states` lane is the fixed
effect index for each logical row; it is not repeated inside the variable-size
`Transaction` value.

The effect byte is the stable link from the transaction to its effects:

```text
bits 0..2  CPI state: unavailable, not-recorded, source/backfill empty/present
bit 3      outcome record exists and proves the source metadata envelope
bit 4      balance record exists; required when bit 3 is set
bit 5      non-empty token-balance record exists
bit 6      log record exists, including a recorded-empty log vector
bit 7      non-empty transaction-reward record exists
```

When bit 3 is set, a clear bit 5 or bit 7 is the sole canonical known-empty
encoding. The token-balance and transaction-reward streams reject empty dense
records. A clear log bit remains unavailable because the source records log
availability independently. CPI keeps its complete state in bits 0 through 2.
Thus unavailable, recorded-empty, and recorded-present remain different.

### 3.3 Effect location without a large transaction row

Do not put six full byte ranges in each transaction. Six `offset + length`
pairs cost 72 bytes per transaction. That is approximately 49 GB for epoch 822
before compression.

The transaction block instead owns a small chunk directory for each of the six
transaction effect streams. A chunk covers 256 transactions. Effect records
are dense and stay in transaction order. The effect bit gives presence. A bit
rank gives the dense record ordinal in the chunk.

A point read is bounded:

1. Read and decompress the block's transaction page.
2. Test the effect bit.
3. Calculate the dense ordinal.
4. Read one effect chunk.
5. Skip at most 255 borrowed Wincode records.

Physical chunk offsets are block-level derived data. They are not repeated in
each transaction. A restart every 32 transaction records bounds row traversal
after the block transaction page is decompressed; it does not make the current
compressed block page smaller.

The block catalog stores only four block-level locators: the transaction
block, block rewards, the framed PoH record, and the framed shredding record.
It does not repeat the six effect chunk locators. Those locators have one owner
inside the transaction block. The fixed catalog row is 144 bytes: a 139-byte
Wincode payload with five required zero bytes for direct row addressing.

The account vectors store the resolved runtime order: static keys, then loaded
writable keys, then loaded readonly keys. This data:

- removes the `decode::decode_metadata_prefix` call from the indexer scan path;
- is exactly the vector replay hands to the runtime, in the order it needs it;
- lets an account index builder scan one transaction stream;
- makes `indexes/accounts.pages` rebuildable from canonical data alone.

Order is semantic, so it is not sorted or delta-encoded. Wincode uses canonical
LEB128 IDs. A chunk can use zstd only when zstd makes it smaller.

### 3.4 Postings

```text
key-page directory entry (32 B):
  [ id_lo, id_hi, page_offset, page_len, restart_prev_id ]

page body, per key:
  ULEB128(id_gap), ULEB128(count), then count × ULEB128((tx_gap << 4) | roles)

roles: 1=signer  2=writable  4=top-level program  8=CPI program
```

Measured 0.36 B/posting. The restart field in each directory entry lets a
reader begin decoding key gaps at any page without reading its predecessors —
which is what makes this work over HTTP `Range`.

There is no per-epoch signature index. A later global signature index can
cover all epochs without adding a large file to each generation. Canonical
signature bytes remain in `sidecars/signatures.bin`.

---

## 4. Ownership, and the test that enforces it

The rule, stated once:

> **Canonical bytes exist exactly once. Derived indexes hold only IDs,
> offsets, counts, and flags, and must be reconstructible from canonical data
> alone.**

That is ordinary database design; it does not need an evidence taxonomy. What
it needs is a way to *fail*:

```bash
blockzilla-index-archive rebuild-indexes --verify <epoch>
```

Delete `indexes/`, rebuild it from `catalog/`, `dictionary/`, `ledger/`, and
the CPI effect stream, and require SHA-256 equality with the published files.
If the rebuild cannot reproduce an index, that index contains a fact it does
not own, and CI reports an error.

One command replaces several hundred lines of ownership validators, and unlike
them it can actually detect a violation in real data.

The specific duplications this removes from Archive V2, all confirmed present:
signature bytes in both `signatures.bin` and block-access blobs; pubkey bytes
in both `registry.bin` and block-access blobs; blockhash bytes across
`blockhash_registry.bin`, `blockhash_index_v3.bin`, `prev_blockhash_tail.bin`,
and access blobs; PoH fields on legacy block types that also live in the PoH
sidecar.

---

## 4.1 Trust model: what a digest does and does not prove

A file digest is **not** chain validation, and nothing in this format may be
written or described as if it were. Conflating the two is what produced the
previous pass's "verification receipts" and "finality evidence" stacked on top
of SHA-256 sums — a second, weaker authority sitting next to the real one.

Three distinct layers, with different costs and different guarantees:

| Layer | Mechanism | What it proves | When it runs |
|---|---|---|---|
| **1. Chain truth** | PoH chain, blockhash continuity, Ed25519 signatures | The data is what the cluster produced | Once, at ingest/verification |
| **2. Storage integrity** | XXH3-64 per page, SHA-256 per file | These are the bytes that were written | At publication, and on demand |
| **3. Unverifiable** | — | Nothing. Metadata and effects are runtime *output* and can only be confirmed by re-executing | Replay only |

Layer 3 is the honest part: fees, logs, balances, compute units, and inner
instructions cannot be verified from the archive by any means short of replay.
The manifest states this rather than implying that a digest covers it.

**Rule: no serving read path recomputes a digest.** Verification belongs to
publication and to explicit audits. The current gateway already follows this —
it recomputes nothing on serve and derives its ETag from the stored digest —
and the read SDK already tiers this as `AllFiles` / `ControlFiles` /
`SizesOnly`. That policy is inherited here, not reinvented.

### Cost, measured on an Apple M1

| Work | Rate | Per epoch |
|---|---|---|
| PoH verification | 30.7 M hashes/s per core (this repo's own benchmark) | The active hashes-per-tick profile is feature- and epoch-bound. For epoch 900 it is 62,500, so 432,000 slots × 64 ticks × 62,500 hashes = **1.728 T hashes ≈ 56,287 core-seconds**. Do not use a hard-coded historical profile. |
| Ed25519 verification | 5,460 verify/s per core | **≈ 18,300 core-seconds per 100 M signatures** |
| SHA-256 bulk | 1.95 GB/s per core (ARMv8 crypto extensions) | **0.51 core-seconds per GB** |

So hashing a 50 GB generation costs ~26 core-seconds: **0.23% of PoH
verification and 0.14% of verifying 100 M signatures.** At 500 GB it is still
~2% of PoH. Storage overhead is 8 bytes per 64 KiB page (**0.012%**) plus
roughly 1.6 KB of file digests per generation.

The cost objection does not survive the arithmetic. The *conceptual* objection
does, and it is why this section exists.

### What the PoH gate certifies, exactly

The intended order is **convert → verify PoH once → build the manifest by
hashing the epoch**. That is the right shape: the manifest becomes a
certificate that this generation passed verification, and the digests freeze
that verified state. It is worth being precise about what the gate covers,
because "PoH passed, so we have everything" is true of more than it looks —
and less than everything.

PoH record hashes mix the Merkle root of each entry's *ordered transaction
signatures*, and a signature is over the exact message bytes. So PoH plus
signature verification chain together:

| Fact | Covered by the gate? | How |
|---|---|---|
| Entry hashes, tick structure | Yes | PoH chain |
| Parent linkage across slots | Yes | blockhash continuity |
| Set and order of signatures per entry | Yes | PoH record hash over the signature Merkle root |
| **Static account keys** | Yes | signature covers the message |
| **Instruction data, program and account indexes** | Yes | signature covers the message |
| Recent blockhash, header counts | Yes | signature covers the message |
| V0 lookup *descriptors* (table address + indexes) | Yes | signature covers the message |
| **V0 resolved loaded addresses** | **No** | depends on lookup-table account state at that slot |
| Fees, balances, logs, inner instructions, return data, compute units | **No** | runtime output |
| Rewards, block time | **No** | not covered by the signature or the PoH chain |

The useful result: the gate already covers the bytes indexers care most about —
instruction data and static keys — because signatures cover the message. What
it does not cover is the resolved loaded addresses (§3.3) and every effect.
Those rest on the source, or on replay.

That is why `coverage.loaded_addresses` is a published manifest field rather
than an assumption: it is the one canonical column the PoH gate cannot vouch
for.

### Closing the loaded-address gap without full replay

Loaded addresses are the one canonical column the gate cannot vouch for — but
they do not need the SVM to verify, only the lookup tables' own history.

A table's contents are built by `AddressLookupTable` program instructions:
create, extend, deactivate, close. Those are **instruction data in ordinary
transactions**, so they are already covered by the gate (per the table above,
signatures cover the message). Replaying only that program's instruction
history in slot order reconstructs every table's address list — a tiny fraction
of the chain, no runtime required.

That upgrades `coverage.loaded_addresses` from *trusted from source* to
*chain-derived and checkable*: resolve each V0 transaction's descriptors
against the reconstructed table state and compare with the stored column.

The reconstruction has to be slot-accurate, not just order-accurate:

- addresses extended in the current slot are not yet usable for resolution;
- a deactivated or closed table stops resolving;
- a closed table's account address can be reused by a later table, so state is
  keyed by (address, incarnation), not address alone.

Where this lives: reconstructed table state is a migration verifier artifact
outside the target archive. The resolved addresses stay **in the transaction
plane** (§3.3) with the forward column and reverse index. Accounts are the
primary query axis, and putting them behind a sidecar join would reintroduce
the indirection this format exists to remove.

### The one thing digests are irreplaceable for

Chain primitives cannot prove that the Archive V2 → Index Archive conversion
preserved metadata — precisely because metadata is Layer 3. Reconstructing the
source blob from the new columns and comparing digests (§6.2) is the **only**
available proof that the re-encoding is lossless. The same applies to
`rebuild-indexes --verify` (§4) and to the content identity the existing
compaction protocol needs for exact-CAS publication.

Remove digests and the rewrite loses its correctness argument.

## 4.2 What building the first reader taught the format

This section records results from the discarded fourteen-plane prototype.
Names such as `AccountGroup`, `decode_page_flat`, and
`PageReader::next_visiting` are historical APIs. The current implementation
applies the same rules through `TransactionBlock`, row restarts, bounded effect
chunks, and positioned catalog reads.

### Decode shape dominates, not encoding

Reading one block: **io + decompress 350 µs, decode 1.45 ms.** The bytes were
never the bottleneck — turning them into Rust values was, at 80% of the time.

The cause was `decode_page` returning `Vec<AccountGroup>`, a `Vec<u32>` per
transaction: ~2,969 heap allocations for one epoch-822 block. The *encoding* is
columnar; the *decoded shape* was row-oriented, so a reader paid row-shaped
allocation to read a column.

`decode_page_flat` returns two allocations instead — every id contiguous, plus
one bound per transaction — and cut decode by ~30%. Iterating all accounts
becomes a linear scan of one buffer, which is exactly what a filter or an index
build wants.

**Rule: every variable-length column needs a flat decode, and it should be the
obvious one.** `PageReader::next_visiting` already existed and allocated
nothing, and the reader used the allocating path anyway, because `decode_page`
was the natural name. A fast path nobody reaches for is not a fast path.

### Reading an index whole defeats the index

The first reader for the earlier candidate called `fs::read` on the complete
block catalog and decoded every row to find one block: **118 MiB and 430,954
row decodes per query** at epoch scale. It was fast on a one-block fixture and
catastrophic on an epoch, which is precisely the failure a single-block test
cannot show.

The current `indexes/slots.idx` binary-searches with positioned reads: about
19 probes of 8 bytes. A direct catalog search also stays bounded because
`catalog/blocks.wincode` has fixed 144-byte rows.
This is the same defect as `StringTable::resolve` in the log codec, which
recomputes a string's offset by summing predecessors, and it was written in
full knowledge of that. **Any structure a reader consults to avoid reading data
must itself never be read whole.**

### Fixed width is the only O(1) shape

The block catalog is fixed width, so `row_at` is one offset calculation with no
allocation and no scan. Transaction records are variable width. The merged
transaction block therefore stores one restart every 32 transactions. The
current reader decompresses the full block page and decodes all rows. A future
row-at reader can start row decoding at the nearest restart, but it will still
read the block page until the transaction stream adds bounded subpages.

### Page size is the floor on a read

One page per block means the smallest possible read is one block. A consumer
wanting one transaction still decompresses its block's page. Smaller pages
would lower that floor and raise per-page overhead, which is the §7 measurement
that has not been run.

### Compression settings are not defaults

`zstd::encode_all` produced frames with **no content checksum**, because
libzstd defaults it off while the zstd CLI turns it on. Measured: 80.5% of
single-bit flips decoded into same-length wrong bytes with no error. Four bytes
a page fixed it.

**Rule: assert the encoder settings that matter rather than inheriting them,
and test the failure, not the success.** Every check here passed before the
flag was set — the corruption simply was not being looked for.

---

## 5. Deliberately not in this format

The previous pass grew replay admission control, finality proofs, chain
schedule identity, runtime descriptor binding, marker-policy digests, consumer
capability profiles, shadow-read cutover, and writer fencing. **All of that is
cut from the format crate.** Not because it is wrong, but because it belongs to
the catalog and scheduler layer, which already implements it —
[`blockzilla-compaction-job-v1.md`](blockzilla-compaction-job-v1.md) defines
`FiniteWorkKindV1`, a global attempt fence, immutable candidates, and
exact-CAS publication.

The format's responsibilities are: bytes, a manifest that binds them, and a
digest. Everything about *who may publish what, when* is already someone else's
solved problem.

Also excluded: program-specific decoded instruction variants; a second
completion footer; a second replay transaction stream beside the canonical
columns; and a block-access materialised cache **inside a canonical
generation** — see §5.1, which is a different thing.

---

## 5.1 Two products from one canonical set

The earlier attempt to split effects into sidecars was abandoned because
rebuilding a legacy block then needed several file reads. That objection is
correct — but it is a statement about *one consumer on one substrate*, not
about the format.

The read costs are not comparable:

| Substrate | Cost of N reads for one block |
|---|---|
| Local NVMe (indexing, replay) | N × ~100 µs — irrelevant |
| Object storage over HTTP (get-block serving) | N × 30–80 ms — fatal |

So there are two products, and only one of them is canonical:

- **True archive** — plane-split, as in §3. This is the indexing and replay
  product. A get-block against it costs several local reads and nobody cares.
- **Serving bundle** — a single-file, block-contiguous materialisation for
  cloud get-block. One range read per block. **This is the existing Archive V2
  compact hot block**, kept for exactly this purpose: it is already
  block-contiguous, already what `blockzilla-get-block` and the gateway
  consume, and its writer already exists. Retaining it as the cloud artifact
  costs no new code and no new format.

The serving bundle is a **derived projection, like an index**: rebuildable from
the planes, deletable, carrying no fact the planes do not already own, and
never a publication source. That keeps the §4 ownership rule intact — this is
not "storing it twice" in the sense that matters, because the second copy is
regenerable and authoritative for nothing.

Two conditions keep it honest:

1. It is **rebuildable and checked** by the same mechanism as any index —
   regenerate it from the planes and compare digests.
2. It is **materialised selectively**, for the epochs actually served hot from
   cloud, and rebuilt on demand for cold ones. Materialising every epoch
   doubles the stored bytes for a read pattern most epochs never see.

This is what the excluded "block-access materialised cache" was about: a cache
living *inside* a canonical generation, where it becomes a second owner of
facts and has to be kept consistent. A separately published, separately
retained serving artifact has neither problem.

---

## 5.2 Replay and recorded metadata

Recomputing effects by replay instead of storing them is the largest available
storage change, and it has one sharp edge that decides how it must be done.

**Replay-verify is additive and safe.** It is the missing Layer 3 (§4.1): the
only way to confirm that recorded fees, balances, logs, and inner instructions
are what the cluster actually produced. Nothing is lost by doing it.

**Replay-replace is a one-way door.** Logs, compute units, and error text
depend on the runtime version, feature-gate activation, and syscall behaviour
at that slot. Replaying epoch 822 under a current runtime does not necessarily
reproduce what validators emitted then. Delete the recorded values and the
archive stops asserting what the chain reported and starts asserting what your
pinned runtime reproduces — and every future runtime fix silently changes the
archive's answers. There is no recovery, because the difference is exactly what
was deleted.

The path that gets the storage win without the door closing:

> Store recorded effects as **replay output plus a stored diff**. Replay each
> epoch, compare against the recorded values, and keep only what differs.

If replay reproduces effects exactly, the diff is near-empty and the storage
win is nearly the whole effects plane. If it does not, the diff is precisely
the set of facts replay cannot reproduce — which is the thing worth knowing
before deleting anything. Either way the archive still asserts the recorded
values, so no consumer comparing against RPC output is broken.

The exception rate is measurable, and measuring it is the prerequisite. Order
of work: get the plane sizes from Phase 1 first. If effects turn out to be a
modest share of epoch bytes, this is a large amount of risk for a small win and
should wait; if they dominate, it is worth building. That number does not exist
yet.

---

## 6. Migration: from today's archive to this format

### 6.1 Archive V2 is a sufficient source

The earlier draft concluded that V2 cannot recover raw bytes for parsed System,
Vote, and Compute Budget instructions, and therefore that conversion needs CAR
or ledger evidence with a per-fact provenance matrix. **That is not what the
code does.**

- **Instruction data is only partly raw in Compact V2, and conversion proves
  its reconstruction.** An
  earlier revision of this document claimed it was preserved byte-exact, citing
  `CompactInstruction { data: &'a [u8] }`
  ([`compact/tx.rs`](../../crates/compact-v2/blockzilla-compact/src/compact/tx.rs)).
  That is the wrong type. The **published hot block** uses
  `ArchiveV2HotInstructionData`
  ([`v2/mod.rs`](../../crates/compact-v2/blockzilla-archive-v2/src/v2/mod.rs)), an
  enum that parses System, Vote, and Compute Budget instructions into typed
  variants — `System(..)`, `VoteTowerSync(..)`, `SetComputeUnitLimit(u32)` —
  and keeps raw bytes only in `Raw`, `UnknownSystem`, and `UnknownVote`. For
  the typed variants the original bytes are **not stored**. The converter
  creates every valid reconstruction candidate, rebuilds the complete signed
  message, and accepts exactly one candidate that verifies against the stored
  Ed25519 signature. No candidate or more than one candidate is a hard stop.
- Logs round-trip through a typed event stream with an `Unknown(StrId)` raw
  fallback and existing round-trip tests
  ([`compact/log.rs`](../../crates/compact-v2/blockzilla-compact/src/compact/log.rs)).
- Anything that failed to decode at write time is retained verbatim via
  `WincodeArchiveV2Payload::Raw { bytes, error }`, flagged with
  `TX_RAW_FALLBACK` / `METADATA_RAW_FALLBACK`, and **already counted per
  generation** (`tx_raw_fallbacks`, `metadata_raw_fallbacks`,
  `rewards_raw_fallbacks`).

The intended conversion is an **offline column transform of existing
generations**. No CAR re-ingest is needed for a supported source profile. The
source exceptions are finite and already counted, but the current converter
does **not** yet have canonical target lanes for raw transaction or raw
metadata fallbacks. It therefore stops on those rows. It must not publish an
empty transaction or silently discard the raw bytes.

The first converter implementation also supports only a manifest-bound
current-hot outer block schema, the current predecessor-tail schema, and the
external shredding sidecar. Shipped legacy hot-block schemas need separate,
manifest-bound decoders because one legacy schema owns shredding inside the
block header. Trial decoding is not a migration contract. The converter's
current source matrix and fail-closed limits are listed in
[`crates/archive-v3/blockzilla-archive-v3-convert/README.md`](../../crates/archive-v3/blockzilla-archive-v3-convert/README.md).

The target format can add compatibility lanes for raw fallbacks, but those
lanes become canonical owners. They cannot coexist with invented decoded rows
for the same transaction or effect.

Two genuine gaps remain, and both are countable rather than mysterious:

- **V0 loaded addresses absent from source metadata.** The manifest records an
  explicit `coverage.loaded_addresses` state. A generation claiming exact
  account-index coverage with a nonzero incomplete count **must not publish**.
  "Field absent" must never render as "empty list" — that turns a missing fact
  into a false negative in an account filter. (This rule from the previous pass
  is correct and is kept.)
- **Recorded CPI completeness**, same treatment, same rule.

### 6.2 The proof gate

An earlier revision proposed comparing converted output against the source CAR.
That is the wrong oracle: it proves only that two files agree, and it needs a
source that will not always exist. It also cannot detect an error the source and
the converter share.

**The signature is the oracle.** A signature covers the entire canonical
message — header, account keys, recent blockhash, and every instruction's
program index, accounts, and data. So:

> Reconstruct the canonical signed message from the archive and verify the
> stored Ed25519 signature against it. One wrong byte anywhere in the message
> fails verification.

Combined with the chain the gate already establishes:

| Step | Proves |
|---|---|
| PoH entry chain | entry hashes and tick structure |
| Blockhash continuity | parent linkage across slots |
| PoH record hash over the signature Merkle root | which signatures are in which entry, and in what order |
| **Ed25519 verify against the reconstructed message** | **the message bytes are exactly what was signed** |

That last row is what makes the whole ledger plane self-verifying, and it needs
no CAR, no external source, and no second copy — only the archive and
`sidecars/signatures.bin`.

```bash
blockzilla-index-archive verify-generation <epoch-dir>
```

Byte-comparison against the source is then a convenience for the conversion
step, not the correctness argument. The remaining conversion checks are: each
retained PoH or shredding frame compares byte-for-byte with its source frame,
and recomputed totals and coverage match the manifest. The complete target
sidecar also has a target common header and an 8-byte profile preamble, so the
whole source and target files are intentionally not byte-identical.

### 6.2.1 This gate is missing today, and something depends on it

The reconstruction it needs **already exists and is already in production**:
`instruction_data_base58` in
[`edgezilla/get-block/src/worker.rs:5252`](../../edgezilla/get-block/src/worker.rs#L5252)
rebuilds raw instruction bytes from the typed variants — a discriminant byte
plus little-endian fields for Compute Budget, `system_instruction_bytes` for
System, `vote_update_instruction_bytes` for the Vote variants.

Two problems with that:

1. **Nothing tests it.** There is no round-trip or signature test over any of
   those functions. If a discriminant or field order is wrong, get-block
   returns a transaction whose signature does not verify, and no test in the
   repo notices.
2. **It lives in a Cloudflare Worker.** The canonical reconstruction is not in
   a library, so the archive verifier, the converter, and the serving path
   cannot share one implementation — and cannot disagree loudly when they
   drift.

The vote path is the sharpest case: it takes a `resolver` and pulls vote hashes
from the block-access file, so reconstructing those bytes depends on a
*different file* being correct. That is a second failure surface with no check
over it.

So this gate is worth building for its own sake, before any migration. If it
passes, get-block is proven correct and the new format can safely store raw
instruction bytes. If it fails, there is a live serving bug — and §7 item 6 is
answered the other way.

This is not a new idea: the
[archive completion audit](../operations/archive-completion-audit-2026-08-04.md)
already records that `verify-archive-v2-poh` covers entries and final
blockhashes, and that *"cross-epoch continuity and signed-message verification
remain separate follow-up checks."* Signed-message verification is the follow-up
that was never built.

### 6.2.2 Blast radius, and whether CAR would be needed

Using the [completion audit](../operations/archive-completion-audit-2026-08-04.md),
the [dated storage total](measurements/epoch-2-real-conversion-2026-08-14.md),
and the [retained-CAR inventory](index-archive-HANDOFF.md):

| | |
|---|---|
| Compact V2 epoch directories | **1,011** (epoch 0–1010), **98.1 TB** |
| Complete per the scheduler audit | **994** |
| Epochs with CAR still retained locally | **22** — 13 plain CAR (1000–1012), 9 `car.zst` (760, 761, 793–799) |

So a rebuild-from-CAR remedy is not available for roughly **970 epochs without
re-fetching them from origin**, against a Compact tree that is already 2.3–3.4×
smaller than the CAR it came from. Re-downloading ~98 TB of source to repair a
re-encoding is not a maintenance operation; it is a second ingest of the chain.

That makes the distinction below the one that matters:

- **Renderer bug** — the typed variant holds every field, and the code that
  turns it back into bytes writes a wrong discriminant, field order, or width.
  **Fix the code; no data is lost and nothing is re-downloaded.** Every
  published epoch is repaired by shipping a corrected reader.
- **Encoding-lossy** — the typed variant cannot represent something the
  original bytes carried (a non-canonical encoding, trailing bytes, a dropped
  field). **Then the bytes are gone** and only the source can restore them.

The design already argues for the first: `Raw`, `UnknownSystem`, and
`UnknownVote` exist precisely so that anything which does not parse cleanly
keeps its original bytes. The intent is to parse only what round-trips. The
risk is therefore concentrated in the narrow case where parsing *succeeds* but
re-encoding differs — which is exactly what a signature check detects, per
transaction, with no source needed.

Run the gate on retained-CAR epochs first (760, 761, 793–799, 1000–1012): they
are the only ones where, if the result is bad, the source is still on disk to
confirm the diagnosis and repair from.

### 6.3 Publication rides the existing protocol

The converter is a finite worker under the existing contract, not a new state
machine:

```text
read published V2 generation  (immutable, digest-bound)
        ↓  one fenced attempt, FiniteWorkKindV1
build immutable candidate generation  (new archive ID, new format_id)
        ↓  verify-migration + rebuild-indexes --verify
publish via exact-CAS on the catalog head
```

Old and new generations are **both immutable and independently complete**, so
they coexist by epoch with no coordination. The gateway already serves by
epoch and file name; the read SDK selects on the manifest's `format_id`.

**Rollback is therefore trivial**: point the catalog head back. There is no
shadow-read fencing to design, because nothing is mutated and no reader is
mid-stream across formats — an indexer processes an epoch under one format or
the other. Retention of the V2 generation for two epochs covers the window.

### 6.4 Historical phasing, with current exit criteria

The current branch implements the target codecs, the current-hot converter,
and the four index-builder outputs from Phases 1 and 2. Their full-epoch exit
checks remain open. Phase 0 was not implemented. The list remains here to
record those exit criteria.

**Phase 0 — unblock the indexer now, without a new format.** *(days)*

Add two derived, deletable sidecars to the *existing* Archive V2 generation:
`resolved_accounts` (the §3.3 column) and `accounts.postings` (the §3.4 index).
Both are additive, rebuildable, and require no migration.

- Exit: `decode::decode_metadata_prefix` is off the indexer hot path, and
  index build no longer needs one full re-decode per account chunk.
- **This phase also produces the full-epoch numbers that Phases 1–2 need.**
  Nothing downstream should be frozen before it reports.

**Phase 1 — a real codec.** *(complete for the current target schemas)*

Encoder + decoder for `catalog/`, `dictionary/`, `ledger/`, and `runtime/`. One epoch in,
one epoch out.

- Exit: `verify-migration` passes on epoch 822; size and query latency reported
  against Archive V2 on the same hardware. Widths and page sizes are frozen
  *here*, on those numbers — not before.

**Phase 2 — indexes.**

`indexes/accounts`, `programs`, `selectors`, and `slots`. A signature index is
not generation-local; a later global signature index is outside this format.

- Exit: `rebuild-indexes --verify` green; parity gate green on epochs 900 and
  920; account-filter p99 measured over HTTP `Range` against the gateway.

**Phase 3 — converter as a finite-work product.**

- Exit: a candidate generation published and verified through the existing
  fence/CAS path, with no new scheduler primitives added.

**Phase 4 — cut readers over, retire the V2 writer.**

- Exit: two epochs served from the new format with the V2 generation retained;
  then the V2 writer is retired and the legacy decoder is kept read-only.

---

## 6.5 Lookup-table rebuilder and verifier

This is the component that closes the §4.1 coverage gap. It is specified here
because it is the only piece whose *correctness argument* is subtle; the rest
of the converter is a mechanical column transform.

### What it produces

The verifier produces a digest-bound verification report. It does not add a
`lookup_tables` object to the archive. Its working state and an optional
carried-forward checkpoint are migration artifacts outside the target layout.
The **resolved addresses stay in each V0 transaction record** (§3.3); the
verifier checks them but does not become a second owner.

### How it rebuilds

Table contents are built entirely by `AddressLookupTable` program
instructions, which are instruction data in ordinary transactions and are
therefore already covered by the PoH + signature gate. Rebuilding needs no SVM
and no account snapshots — only that program's own instruction history, in slot
order:

| Instruction | Effect on state |
|---|---|
| `CreateLookupTable` | opens a new incarnation at the derived address, empty, with `authority` and `deactivation_slot = none` |
| `ExtendLookupTable` | appends addresses, records `last_extended_slot` |
| `FreezeLookupTable` | clears the authority; contents become immutable |
| `DeactivateLookupTable` | sets `deactivation_slot`; the table stops resolving after the cooldown |
| `CloseLookupTable` | ends the incarnation; the address may later be reused |

Only successful transactions mutate state. A failed transaction's ALT
instructions must not be applied, so the rebuilder needs the result stream.
This is one of the few places where a `runtime/` fact feeds a migration
verification report.

### The three rules that make it correct

These are where a naive rebuild silently produces wrong addresses:

1. **Same-slot extends are not yet visible.** An address appended by an
   `Extend` in slot S cannot be used for resolution *in* slot S. Resolution at
   slot S sees only entries with `last_extended_slot < S`. Ignoring this
   over-resolves and produces addresses no transaction could have referenced.
2. **Deactivation has a cooldown**, so a table keeps resolving for a bounded
   period after `Deactivate`. Treating deactivation as immediate
   under-resolves.
3. **Addresses are reusable.** A closed table's account can be recreated as a
   different table. State must therefore key on `(address, incarnation)`, where
   incarnation increments on each `Create`. Keying on address alone silently
   merges two unrelated tables — the worst failure mode here, because it
   produces plausible addresses rather than an error.

### How it verifies

For each V0 transaction, resolve its stored lookup descriptors against the
reconstructed state at that transaction's slot, and compare with the resolved
addresses already in the V0 transaction record:

- **match** → that transaction's `LoadedAddressResolution` is chain-derived;
- **mismatch** → hard failure. The generation does not publish. A mismatch
  means either the source metadata was wrong or the rebuild is wrong, and
  neither is safe to paper over;
- **table not reconstructible** (its history predates the archive's range) →
  the transaction keeps its source-derived state and the manifest's
  `coverage.loaded_addresses` records that the generation is not fully
  chain-derived.

The third case is why this is a separate verification pass rather than a
converter step: an epoch cannot always rebuild tables created in an earlier
epoch, so the rebuilder needs either a carried-forward state snapshot from the
previous generation or an explicit "unverifiable from this range" result. It
must never guess.

### Order of work

Rebuild is cheap — one filtered scan for a single program's instructions —
so it runs as its own pass after conversion, not inside it. It reads the
converted generation and writes the verification report. That keeps the
converter free of chain semantics and makes the verifier independently
re-runnable when a rule turns out to be wrong.

---

## 7. Open questions that require measurement, not discussion

Nothing below should be settled by argument. Each needs Phase 0/1 numbers.

1. **Page size per plane.** Scan planes and index key pages have opposite
   access patterns over HTTP `Range`. 64 KiB is a starting candidate for scan
   planes only. Page size is a per-file header field, not a global constant.
2. **Row-group span.** Blocks per row group trades get-block locality against
   filter selectivity.
3. **Whether `indexes/selectors` earns its size** at full-epoch scale.
4. **zstd level per plane**, given that dictionary pages must stay raw.
5. **Payload dedup for instruction data — deferred, evidence recorded.**
   Top-level instruction data is 75.7% literal duplication: Vote is 56.5% of
   instruction bytes with 25 distinct payloads, and one oracle/crank program is
   13.2% with *one*. A shared table beats both plain zstd and a zstd trained
   dictionary once pages are small, because zstd cannot match across frame
   boundaries. Not implemented: building the table needs a pass over every
   payload in the archive, and the epoch-wide distinct count is unmeasured. See
   [instruction-data-compression.md](measurements/instruction-data-compression.md).

   The same evidence says a **per-program instruction codec is the wrong
   investment** — the byte-heavy programs are already ~100% duplicate, and V2
   already types Vote without capturing that. Typing captures structure; dedup
   captures repetition.

6. **Typed instruction reconstruction — resolved for supported sources.**
   Archive V2 stores System, Vote, and Compute Budget instructions as typed
   variants. The converter reconstructs bounded candidates for the complete
   signed message and uses the stored Ed25519 signature as the byte oracle.
   It writes the one verified raw byte sequence and stops if the proof is not
   unique. Unsupported raw fallbacks and source schemas still fail closed.

7. **Wincode wire profile — the two-version split was resolved, 2026-08-18.**
   The whole workspace now runs a single exact `0.6.1`. Compact V2's bytes did
   not move: `legacy_payload_has_exact_golden_bytes_and_hash` asserts its exact
   bytes *and* their hash, and passes unchanged, which is consistent with
   `blockzilla-format` using no `BitVec` — the one wire-affecting change in
   0.6.0. What unblocked it was `solana-short-vec` 3.2.1 → 3.3.0, which moves
   from `wincode ^0.5.0` to `^0.6.0`; the earlier reading that Compact V2's
   schemas were incompatible with 0.6 was wrong, as the compile error was
   version skew across a crate boundary rather than an API break. Confirmed on real data: the same
   converter built at both versions produces **byte-identical output across all
   32 files** for the epoch-157 and epoch-822 fixtures, 7,177 transactions
   total. Two blocks are still not 1013 epochs — see
   [`transaction-v1-support.md`](transaction-v1-support.md) §4. The paragraph below describes
   the profile as originally split, and is retained for the schema rules that
   still hold.

   Compact V2 stays on exact Wincode `0.5.5`.
   Structured objects in the new format use exact Wincode `0.6.0`. Their schema
   fixes little-endian byte order, canonical LEB128 integers, `u8` enum tags,
   alignment checks, and a 64 MiB preallocation limit. PoH and shredding keep
   their accepted varint-framed Wincode `0.5.5` records byte-for-byte; the
   target common header selects the sidecar schema and its 8-byte preamble
   selects the retained grammar. Golden bytes make a configuration change a
   schema change. Decoders reject trailing bytes and padded, non-canonical
   varints.

---

## 8. Status of this branch

`codex/indexer-first-archive` has been rebased onto `main` (it was 48/53
commits diverged; the three format commits are purely additive and replayed
cleanly). Tests pass.

What is kept from the previous pass: the layout, the module tree mirroring, the
ownership rule, the coverage-state rule, and the measurement tool — which is
real, deterministic, and reproduces byte-for-byte.

What was cut (commit `a695c7c`): `migration/archive_v2.rs` (2,574 lines of
provenance validation for a format that could not yet be written), `replay.rs`
(1,195 lines of admission control belonging to the catalog layer), and the
manifest's fork/finality/backfill surface. The crate went from 8,002 to 3,883
source lines.

This branch now has native current-schema codecs for the catalog, dictionaries,
ledger, runtime effects, retained PoH and shredding frames, signatures, and
all four required indexes. The Compact V2 converter uses a common header, a content-derived
archive ID, pinned source files, exact signed-message verification, bounded
parallel block work, and ordered deterministic writes. The converter also
builds the four index-builder outputs before it renames the staging directory.
`dictionary/account_flags.pages` is a fifth derived object produced during the
canonical scan. The
account, program, and selector index builders use bounded external sorting;
the slot index streams from the catalog. A signature index is intentionally
out of scope because it will be global across generations. A final structural
gate requires every layout object required for that epoch and one archive ID.
This gate makes a
complete physical candidate; it does not make a publishable generation.

The output is still an unpublished candidate. Remaining release work is a
typed target manifest, full semantic parity and finality verification, legacy
source-profile decoders, raw-fallback compatibility lanes, and the atomic
consumer cutover. The exact current command and source limits are in the
[converter README](../../crates/archive-v3/blockzilla-archive-v3-convert/README.md).
