# Index Archive — handoff

> **Superseded prototype snapshot.** This file describes the discarded
> fourteen-plane candidate. The current format of record is
> [`blockzilla-index-archive.md`](blockzilla-index-archive.md), and the current
> physical tree is in
> [`crates/archive-v3/blockzilla-archive-v3/README.md`](../../crates/archive-v3/blockzilla-archive-v3/README.md).
> Do not implement a reader or writer from the paths below.

Working branch: **`codex/indexer-first-archive`**, rebased onto `main` (merges
cleanly, purely additive except the gateway removal). Worktree lives at
`~/Developement/ferno/blockzilla-index-archive`. Backup of the original Codex
branch: `backup/codex-indexer-first-archive-orig`.

Read [`blockzilla-index-archive.md`](blockzilla-index-archive.md) first — it is
the design of record, ~700 lines. This file is only the state of play.

---

## 1. What exists and works

Working branch: `codex/indexer-first-archive`, worktree at
`~/Developement/ferno/blockzilla-index-archive`.

### The format, as built

A generation is **20 files**. Nothing from the source is dropped.

```
catalog/blocks.tbl               56 B identity + 14 page spans + first_signature,
                                 per block. The only index -- no manifest, no
                                 per-column directories.
dictionary/pubkeys.pages         32 B account keys, order varies per source epoch
                                 -- see where-the-bytes-are.md 5.2 (69.2% of
                                 references fit one ULEB128 byte)
dictionary/blockhashes.pages     32 B recent blockhashes + unresolvable/nonce hashes
ledger/     core, accounts, instructions, instruction_data, lookups
runtime/    inner_instructions, inner_instruction_data, outcomes, balances,
            token_balances, logs, return_data, rewards, block_rewards
sidecars/   signatures, poh, shredding[, genesis]
```

Pages are per block and compressed only when that is smaller, so equal
`stored_len`/`decoded_len` means raw — no codec field.

### Verified

- **Reads a block's ledger** in 3 of 14 columns, 5.3% of the generation, never
  opening logs, balances or token balances.
- **Reassembles a whole block** — all 14 columns, 2,969 transactions, in 4.4 ms
  reading 39.4%. Every count cross-checks the converter's own report.
- **Reassembles one transaction** end to end: signature, header, recent
  blockhash, accounts resolved to real pubkeys, instructions with programs
  named.
- **Converted two full epochs** on the NAS (250.8M and 683.9M transactions) at
  ~543k tx/s, before compression and the later columns existed.

### Size, like-for-like

Same facts both sides — block payload, signatures, both registries, PoH,
shredding, index, meta:

| | bytes |
|---|---:|
| Archive V2 | 802,665 |
| Index Archive | **752,592** (−6.2%) |

Storage is not where this format wins. Both sides carry identical incompressible
dictionaries (59% of this generation), and signatures and PoH are untouched. The
win is read cost: replay decompresses 2.8× what it uses in V2, and a selective
read here touches 5.3% rather than a whole block frame.

### Bugs this work found in the source format

- `StringTable::resolve` is O(n) per lookup, making log rendering quadratic on
  the get-block path (spawned as a separate task).
- `instruction_data_base58` and friends reconstruct instruction bytes in the
  get-block Worker with **zero test coverage**.

### Bugs found and fixed in this format

Each was silent data loss caught by checking rather than assuming:

- `blockhash_ref` pointed at a dictionary nothing wrote, and unresolvable
  hashes were written as `0` — 2.40% of epoch-822 transactions.
- Block rewards were flagged in the catalog and never stored.
- `block_time`/`block_height` were declared on the row and never encoded.
- Account references were registry ordinals with no dictionary carried.

## 2. The blocking question — §7.5

**Archive V2 does not retain original bytes for System, Vote, and Compute
Budget instructions.** `ArchiveV2HotInstructionData`
(`crates/blockzilla-format/src/v2/mod.rs:1594`) is an enum: `Raw`,
`UnknownSystem`, `UnknownVote` keep bytes; `System`, `ComputeBudget`,
`VoteTowerSync`, `VoteCompactUpdate*` are typed and re-derived on read.

This is a deliberate and effective compression choice — vote and compute-budget
instructions are enormously repetitive. It is not a bug. But it means
`ledger/instruction_data.pages` **cannot be written yet**, because whether the
typed form re-encodes byte-exactly is unproven. The converter therefore counts
variants instead of writing that plane.

Exposure measured:

| | re-derived share of top-level instructions |
|---|---:|
| epoch-157, full epoch | 15.8% (System only) |
| epoch-822, biggest block only | 57.6% (ComputeBudget 40.8, Vote 11.3, System 5.5) |

Inner instructions are **unaffected** — `CompactInnerInstruction.data` is raw
`Vec<u8>`.

### How to settle it

Not by comparing against CAR. **The signature is the oracle**: a signature
covers the whole canonical message, so reconstruct the message from the archive
and Ed25519-verify the stored signature against it. One wrong byte fails.
Needs only the archive and `signatures.bin`.

Two outcomes:

- **Round-trips** → new format stores raw instruction bytes; conversion is
  lossless; and get-block is proven correct.
- **Does not** → the canonical column must carry the typed form through, as
  `blockzilla-replay` already does (`compact.rs:1294`), with raw bytes only for
  the fallback variants.

The reconstruction already exists in production and is **untested**:
`instruction_data_base58` at `edgezilla/get-block/src/worker.rs:5198`,
plus `system_instruction_bytes` and `vote_update_instruction_bytes`. It lives in
a Cloudflare Worker, so it cannot be shared with a verifier. The vote path pulls
hashes from the block-access file — a second unchecked surface.
`hivezilla/service/src/ledger/grpc.rs:165-200` is the reference implementation
of the message serializer + `verify_strict` pattern.

Run it first on epochs whose CAR is still retained (760, 761, 793–799,
1000–1012). Only 22 of 1,011 epochs have local CAR, so a data-level remedy is
unavailable at scale — which is why it matters whether this is a renderer bug
(fix the code, no data lost) or encoding-lossiness (bytes gone).

---

## 3. Next tasks, in order

1. **Port the converter onto `ArchiveReader`.** It currently hand-rolls
   positioned reads and `zstd::decode_all` (fresh allocation per block) and uses
   the *owned* block decoder. The read SDK already streams one frame at a time
   with a reused decompression buffer and a borrowed decoder
   (`BorrowedDecodedBlock`, `deserialize_archive_v2_hot_block_blob_borrowed_current`).
   Needs `open_trusted` + `HashVerification::SizesOnly` because
   `archive/epoch-*` has no generation manifest. Do this before epoch-822
   (101 GB, ~10× the epoch-157 run).
2. **Build the signature-verification gate** (§2 above). Extract the
   reconstruction out of the worker into a library first.
3. **Run epoch-822** for modern numbers. Epoch 157 is 2020-era: no CPI, no vote
   compaction, so it under-represents the runtime planes.
4. **Remaining runtime planes** — outcomes, balances, token balances, logs,
   return data, rewards. Only `inner_instructions` is written today.
5. **Container + manifest** — file headers, page directories, digests. Page
   sizing is a §7 measurement; the converter currently writes one page per
   block with no directory.
6. **Lookup-table rebuilder/verifier** — spec is §6.5. Needs no SVM; ALT
   contents come from `AddressLookupTable` instructions, already covered by the
   PoH+signature gate. Three rules that a naive rebuild gets wrong: same-slot
   extends are not yet visible, deactivation has a cooldown, and addresses are
   reusable so state keys on `(address, incarnation)`.

---

## 4. The gateway / manifest removal — attempted, reverted, redo properly

Commit `afc710f` removed the gateway; `439381e` reverts it. **Read this before
trying again.**

The evidence that motivated it is real: `archive-v2-generation.json` exists
**0 times across 1,013 epoch directories and 128 TB**, and the gateway is not
deployed (running units are `blockzilla-archive.service`,
`blockzilla-monitor-public.service` — "sole tunnel-facing service … no gateway
hop" — and `blockzilla-watcher-tunnel`).

**But the gateway's `generate-manifest` subcommand is not dead code.** The
replay-compact generations for epochs 0 and 1 are a separate product from the
`archive/` tree, and they *do* get a manifest: written by
`blockzilla-archive-gateway generate-manifest` from `sync-replay-compact.sh`,
and required by `ArchiveReader::open_with_options`, which `blockzilla-replay`
uses in `compact.rs` plus five bin tools. Deleting the gateway removes the
producer while every consumer still requires the product.

### The finding that makes the real removal easy

`open_trusted` already synthesizes a manifest with `"0".repeat(64)` placeholders
for `sha256` and `generation_digest`
(`read-sdk/src/manifest.rs:186`), and `validate_generation_structure` only reads
**file names and sizes**. So the digest fields are not load-bearing for reading
at all — the published manifest's only real content is a file list.

### Correct order — attempted, and where it stops

The refactor was implemented and then reverted. **Do not retry it without
reading this.** What worked, and the wall it hits:

**Worked (read SDK + indexer, both compiled):**
- `GenerationManifest` → `GenerationDescriptor`: same struct, no serde, no
  `parse()`, built from the source instead of from JSON.
- Two open paths collapsed to one: `open(source, identity, options)`.
- `HttpRangeSource` deleted — it is the gateway's client, unreferenced, off by
  default.
- `verify_published_binding` deleted from the indexer. It verified the index
  against a JSON's *claim* about `registry.bin`; the surviving path hashes the
  file, so nothing is weakened. Relocation support was preserved.

**Two compatibility constraints that must be honoured (each would force a
12 GB, 107-index rebuild):**
1. The **generation digest formula is frozen**. It is persisted in every built
   index (`build.rs:945`) and compared on query (`query.rs:220`). A real one on
   the NAS reads `binding_kind: trusted_local_asserted_immutable`,
   `schema_version: 3`. Keep feeding it the old `schema_version = 1` constant.
2. **`binding_kind` must stay in `IndexManifest`.** It uses
   `deny_unknown_fields`, so removing the field or either variant makes all 107
   existing manifests fail to load. Nothing needs to branch on it.

Also: do **not** give the identity labels a `Default`. Real indexes carry a
content hash in `generation_id` (e.g. `6eeb31e7…`), so a default silently
produces an index no query can bind to. Make missing identity a hard error.

### The wall: replay's wire-profile selection

`message_schema_for_manifest` (`replay/src/compact.rs:786`) picks the message
decode grammar by matching the manifest's **real `blocks.sha256` and
`index.sha256`** against a hardcoded table of known May-24-2026 mainnet
generations. A match selects
`CompactMessageSchema::May24_2026PreUnknownInstructionFallbacks`; otherwise
`Current`.

The descriptor path fills every file hash with zeros, so every generation would
fall through to `Current` and **silently mis-decode those historical epochs**.
That is a wrong-answer failure, not an error.

It cannot be replaced by hashing on the fly: `HashVerification::ControlFiles`
hashes the registry and index but explicitly **not** the blocks file, and
epoch-822's `archive-v2-blocks.zstd` is **101 GB**.

So `archive-v2-generation.json` is genuinely load-bearing — not for the
`archive/` tree, where it has never existed, but as the mechanism that
identifies historical replay-compact generations needing the old grammar.

**Whoever picks this up must first decide how replay identifies a historical
generation without a manifest.** Options, none free:
- match on `(epoch, index_sha256)` only — index is ~22 MB, cheap to hash, but a
  weaker criterion than today's;
- record the schema in a small sidecar written at generation time;
- keep the manifest for replay-compact generations only, and remove it
  everywhere else.

Until that is decided, the manifest stays.

## 5. Environment

- Storage host access is environment-specific. Keep the host, user, port, and
  transfer settings in local deployment configuration, not in this document.
- Cross-compile: `x86_64-unknown-linux-musl`, `CC_x86_64_unknown_linux_musl=x86_64-linux-musl-gcc`,
  `RUSTFLAGS="-C target-feature=+aes,+sse2"`. Produces a static-pie binary.
- Run measurement jobs at low priority on a loaded storage host.
- A directory under `archive/` is safe if its name does not parse as an epoch
  number — `discover_epoch_entries` skips it. The trial output is a sibling
  tree outside the archive root.

## 6. Things not to repeat

- Do not measure the source CAR and describe it as our format. CAR stores
  metadata as compressed protobuf; V2 re-encodes through `CompactMetaV1`. The
  numbers differ substantially.
- Do not quote single-block fixture percentages as epoch percentages.
- Do not load a blocks file or a plane into memory. Epoch-822's blocks file is
  101 GB.
- A file digest is storage integrity, not chain validation. Chain truth is PoH,
  blockhash continuity, and signatures. Metadata is unverifiable without replay
  — say so rather than implying a digest covers it.
