# Live Archive Producer

Status: design draft

Blockzilla should be able to build its archive live from feeds we control or
subscribe to. Historical CAR data remains important, but the target production
path is not `CAR -> Blockzilla`; it is:

```text
live feed -> normalized block intake -> Blockzilla archive
                          ^
                          |
                    CAR repair lane
```

The CAR lane is a backup and repair source, especially for fields that are not
available from regular RPC responses, such as full PoH entry data or shred
boundary metadata. Exact CAR byte-for-byte reconstruction is not a live producer
goal.

## Goals

- Produce Blockzilla archives directly from live data.
- Treat epoch boundaries as first-class events so live output stays aligned
  with Old Faithful epoch files and Blockzilla per-epoch indexes.
- Preserve enough source provenance to know which blocks are semantically
  complete, incomplete, repaired, or still pending repair.
- Use Triton CAR data as the durable repair source when live intake misses data.
- Keep regular RPC fallback explicit and visibly incomplete when it cannot
  supply PoH entries or shred boundary data.
- Build a shred stream path for both live latency work and historical shred
  research.

## Primary Sources

### Own Feed / gRPC

The primary path should be our own live feed. A Yellowstone-compatible gRPC
source is a natural first interface because the rest of the ecosystem already
speaks it, and because block and entry subscriptions can map cleanly into the
Blockzilla block model.

The producer should subscribe to whole block data and ledger entries when the
feed supports them. For each slot it should emit a normalized `LiveBlockDraft`
containing:

- slot metadata
- ordered transactions
- transaction status metadata when available
- rewards when available
- ordered PoH entries when available
- block-level shredding metadata when available, written as a sidecar
- source timestamps and provenance

The live process should also subscribe to slot or block-meta updates and run an
epoch boundary tracker:

```text
startup getEpochInfo -> seed epoch tracker
slot update / block meta -> epoch(slot) -> boundary crossed? -> close epoch N
```

Startup RPC sync records `absoluteSlot`, `epoch`, `slotIndex`,
`slotsInEpoch`, the computed current epoch start/end slots, and the next
boundary slot. The producer should verify this is compatible with the fixed Old
Faithful `432000` slots-per-epoch layout before finalizing live output into an
OF-aligned epoch directory.

All startup sync and RPC backfill calls should run through a shared rate-limit
aware JSON-RPC client. The client needs local pacing for provider plan limits
and must honor server pressure signals such as HTTP `429`, `Retry-After`, and
JSON-RPC rate-limit errors before retrying.

On boundary close, the producer should flush the live block index, blockhash
tail, signatures, PoH sidecar, shred sidecar, and current pubkey counts for the
completed epoch. The next epoch starts with a fresh live registry and the
previous blockhash tail needed for recent-blockhash lookups.

### Fumarole / LaserStream

Fumarole and LaserStream-style feeds are useful as live or near-live sources
because they can provide gRPC-compatible data with replay windows. Fumarole is
especially interesting for archive production because it is designed around
persistent cursors and completeness. LaserStream is interesting because it keeps
the Yellowstone interface while focusing on low-latency delivery.

These should be adapters behind the same normalized producer interface, not
separate archive builders.

### DoubleZero / Shred Stream

Raw shreds should be modeled as a separate intake family. They are valuable for:

- low-latency pending block assembly
- understanding leader output before confirmed block delivery
- historical shred research
- comparing shred boundaries to archive-level PoH entries

Shreds should not be treated as equivalent to PoH entries. A shred is a network
data fragment used to transmit pieces of a block. A PoH entry is a ledger entry
with `num_hashes`, an entry hash, and a transaction list. Multiple shreds can
carry bytes that reconstruct entries, and entry boundaries can be inferred only
after deshredding and decoding the ledger data.

The producer should keep shred artifacts in a separate sidecar until the model
is proven:

```text
shreds.raw       raw or minimally decoded shred records
shreds.index     slot, shred index, source, arrival time, coding/data flags
poh.wincode      normalized ordered PoH entries used by Blockzilla
```

This lets us support users who care about historical shreds without confusing
shred history with the replay/verifiable PoH entry stream.

## Repair Sources

### Triton CAR Backup

Triton CAR data should be the canonical repair lane when live production misses
fields. The existing CAR reader already extracts:

- ordered transaction nodes
- PoH entry `num_hashes`
- PoH entry hashes
- transaction counts per entry
- block shredding metadata
- rewards dataframes

When a live-produced block is missing PoH or shredding data, the repair process
should read the corresponding CAR range and patch the Blockzilla sidecars. The
main block payload should not pretend it was complete at first write; it should
carry a repair state until patched.

### Regular RPC

Regular `getBlock` RPC is only a degraded fallback. It can provide useful block
metadata, transaction data, transaction metadata, rewards, and signatures, but it
does not provide full PoH entry lists or shred boundary information.

Any block built from regular RPC must be marked as incomplete:

```text
source = RpcGetBlock
completeness = MissingPoh | MissingShredding | MissingEntries
repair_required = true
```

RPC fallback is acceptable for keeping block availability moving, but not for
final archive completeness.

RPC backfill should be driven from the epoch coverage plan. For a completed
epoch, compute missing slot ranges from the live index/journal, call `getBlock`
for those ranges, and write the raw payloads into a repair lane. That keeps the
main archive moving while preserving the fact that PoH entries and shredding
still need live-feed or CAR repair.

## Completeness Model

Every produced block should carry a compact completeness record:

```text
LiveBlockCompleteness {
  has_block_meta
  has_transactions
  has_transaction_status
  has_rewards
  has_poh_entries
  has_shredding
  source_rank
  repair_required
}
```

Suggested states:

- `Pending`: seen live, not finalized enough to write durably
- `CompleteLive`: built from live feed with all Blockzilla-required fields
- `IncompleteLive`: live feed produced block data but missed one or more fields
- `RpcFallback`: built from regular RPC and known to need repair
- `CarRepaired`: missing fields were patched from CAR
- `CarOnly`: produced directly from CAR during backfill or outage

The archive reader can treat `CompleteLive`, `CarRepaired`, and `CarOnly` as
complete for normal reads. Validation and audit tools can still inspect the
source history.

## Shredding Sidecar

`CompactShredding` should not live in the hot block blob. It is repairable
metadata, and adding it after independent zstd block compression would otherwise
force a block-frame rewrite.

The target sidecar is:

```text
shredding.wincode:
  WincodeArchiveV2ShreddingRecord {
    block_id
    slot
    shredding: Vec<CompactShredding>
  }
```

Live gRPC blocks can be written immediately with an empty block-header
`shredding` field and `MissingShredding` in the journal. A later CAR repair job
or raw-shred-derived repair job can append the sidecar record independently.

## Producer Pipeline

```text
Source adapter
  -> normalize into LiveBlockDraft
  -> validate slot/order/basic hashes
  -> write hot block + runtime sidecars
  -> write PoH sidecar if available
  -> write source/completeness journal
  -> enqueue missing fields for CAR repair
```

The writer should be append-friendly because live production may need to write a
block before all repair data is available. Repairs should update sidecars or add
patch records without rewriting large compressed block blobs.

## Live Registries

Some registries can be built live, while others should be finalized at epoch
close.

`blockhash_registry.bin` is easy to build online because recent blockhashes only
need a short rolling window for transaction conversion, while the archive itself
can append each produced blockhash in block order. A live producer should write
the raw blockhash sidecar immediately and use dense block ids for block-local
references.

`signatures.bin` is also naturally append-only. Signatures can be dumped in
block order, transaction order, then signature order as blocks arrive. A
signature lookup index can be built either online as a mutable index or as an
epoch-finalization step over the append-only signature file.

The account/pubkey registry is different. The best compact ids depend on epoch
frequency, so the live path should keep producing a partial registry sidecar as
blocks arrive. The preferred sidecar is `index/pubkey-runs/*.bin`: sorted raw
`(pubkey[32], count:u32)` chunks built from per-block sorted/deduped pubkeys.

The block artifact should still move as much semantic work as possible into
capture. Instead of keeping only no-registry blocks, the next target format is
`blocks/live-pre-hot-blocks.bin`: a stream of `LivePreHotBlock` records whose
hot message/metadata payloads are already decoded and optimized, but whose
pubkey references remain `CompactPubkey::Raw([u8; 32])`. At epoch close:

1. merge the partial pubkey-run sidecar by usage frequency
2. write the final ordered `registry.bin`
3. stream pre-hot blocks and rewrite raw pubkey refs to `CompactPubkey::Id`
4. serialize/compress the canonical hot blocks and write final indexes
5. keep source/completeness journals so the repack can be audited

This makes epoch close a registry lookup plus re-encode/compress pass instead of
a full transaction/metadata/log decode pass. The desired bottleneck becomes
registry lookup, zstd, and disk I/O.

## First Implementation Slice

1. Define the normalized live block intake structs and completeness states.
2. Keep live production in a standalone `blockzilla-live-producer` app crate so
   it can be deployed independently from the historical CAR optimizer.
3. Add a CAR-backed adapter that produces the same structs. This becomes the
   golden test source.
4. Add a regular RPC adapter and make incomplete output impossible to miss.
5. Add the gRPC/Fumarole/LaserStream adapter.
6. Add the shred stream adapter and sidecar layout.
7. Add a repair worker that patches missing PoH/shredding from CAR.

The first useful milestone is not a perfect live network client. It is one
writer path that can accept either live blocks or CAR-derived blocks and produce
identical complete Blockzilla records when the same data is present.
