# Blockzilla Replay Projection V1

Status: **draft minimal specification; not implemented and not raw evidence**.

Replay Projection V1 is the compact, sequential execution input produced from
verified Solana shreds. It is deliberately different from Archive V2/Compact V2:
Compact V2 serves indexers and point reads, while this format exists to drive a
Bank/SVM replay that regenerates transaction runtime results.

The public name is `Replay V1`; the precise contract name is
`REPLAY_PROJECTION_V1`. “Projection” is intentional. The format omits outer
transaction signatures and therefore is not a self-authenticating copy of the
ledger. Exact raw shreds remain the permanent source of truth and the recovery
path for every omitted byte.

In this document, **signed message** means the exact Solana message bytes that
the signers authorized: the byte-exact result of the pinned Agave
`VersionedMessage::serialize()` operation. For Legacy and V0 transaction
envelopes these bytes follow the signature vector, but that envelope layout is
not true of every later transaction version and must never be used to extract
them. Replay V1 stores those message bytes (compactly for Legacy/V0); it does
not store the outer signatures themselves.

## 1. Decisions

V1 makes these choices:

1. Preserve ordered block components, PoH inputs, block identity, and the exact
   signed transaction message content. Parent identity is derived from the
   finalized linear chain and validated against markers, not repeated per slot.
2. Do not store outer transaction signatures, transaction status metadata,
   logs, inner instructions, balances, loaded addresses, rewards derived by the
   runtime, block time, block height, or shred geometry in the replay payload.
3. Store one 32-byte signature-mixin hash for each transaction-bearing PoH
   entry. This is enough to recompute the PoH entry chain without retaining all
   64-byte transaction signatures, but it is not enough to verify transaction
   authorization.
4. Preserve the historical duplicate cache's **status-key equivalence**, not
   signature bytes. Launch-era Solana compared a persisted 20-byte slice of the
   first signature, scoped by recent blockhash and fork; it did not compare the
   complete 64-byte signature. A fresh status key costs no extra payload byte
   because its presence bit shares the message-variant tag. Only a repeated
   class needs a backreference. Modern message-hash admission is derived from
   the stored message and needs no per-transaction status backreference; the
   generation-level offset/collision evidence still applies.
5. Preserve consensus/block-marker values in their exact order and canonical
   serialization. Agave-compatible component padding stays only in raw evidence.
   Marker
   certificates and signatures are not transaction signatures; current Agave
   uses some marker fields to update clock, rewards, parent selection, and bank
   state, so they cannot be discarded. Outer-transaction signature stripping
   never applies to marker/certificate signatures: a pinned runtime may serialize
   a complete certificate, including its BLS signature, into consensus account
   state and thereby change capitalization and Bank/account hashes.
6. Reuse Blockzilla's compact address-reference and recent-blockhash ideas, but
   use a replay-scoped registry derived only from ledger messages. Replay IDs
   must not depend on gRPC metadata, rewards, logs, or Compact V2 registry order.
7. Keep instruction data byte-exact. Replay V1 does not use Compact V2's
   semantic System, Vote, or Compute Budget instruction rewrites.
8. Do not create a program registry or canonical program-hint sidecar. Program
   roles are derivable cache data keyed by the Replay/address-registry digests,
   never replay input or publication identity.

The current public `SHRED_BLOCK_OBSERVATION_V1` remains a distinct, provisional
live product containing complete signed transactions. Replay V1 is a finite,
registry-bound execution projection and must receive a separate format identity.
Payload ID `8` is reserved for it, but it is not registered in executable code
until the schemas, bounds, and golden fixtures in this document land together.

### Minimal payload boundary

The hot sequential payload is intentionally much smaller than the surrounding
publication/checkpoint protocol:

| Scope | Bytes stored in Replay records |
| --- | --- |
| Slot | ordered components only; slot, parent, final PoH, and block ID are derived/bound outside |
| Entry | `num_hashes`, transaction count, and one 32-byte signature mixin only when non-empty |
| Transaction | one combined message/status tag, sparse historical duplicate backreference only when needed, and the compact exact signed message |
| Marker | one canonical supported marker value in original component order |

There are no outer signatures, runtime fields, entry hashes, per-slot parent
fields, program registry/hints, or per-transaction fresh-status tags. Finality,
status proof, runtime attachments, checkpoints, fencing, and recovery are
generation-level objects; none is repeated in every Replay record.

## 2. Trust boundary created by removing signatures

Solana transaction signatures serve three different purposes:

- they prove signer authorization over the serialized message;
- their Merkle root is mixed into every transaction-bearing PoH entry; and
- the first signature is the externally visible transaction ID/status key.

Dropping them saves roughly 64 bytes per required signer, but it has unavoidable
consequences:

- Replay V1 cannot reverify user authorization.
- Replay V1 cannot recreate transaction signatures, signature-indexed RPC
  history, or the original transaction IDs.
- A stock Agave ledger replay cannot consume Replay V1 unchanged because its
  transaction sanitation expects the exact signature count and its ledger
  verification reads the signatures.

Therefore a Replay V1 producer must verify every original transaction signature
before stripping it, must compute the entry signature mixin from those verified
signatures, and must bind the output to permanently protected raw-shred evidence.
A dedicated replay adapter may synthesize correctly sized placeholder signature
vectors or use a signature-verified construction path, but the exact behavior is
pinned by the replay-engine descriptor and tested against the target Agave
runtime. It must never pretend that placeholders passed cryptographic
verification.

Placeholder signatures are ephemeral adapter values. For a historical
signature-slice profile, the adapter maps each stored status-key class to one
private 20-byte token, repeats that token through the required 64-byte
placeholder, and proves that all 20-byte windows selected for classes sharing a
recent blockhash are collision-free. Equal classes therefore compare equal and
different classes do not accidentally merge even if a native cache chooses a
different offset. It retains the era-exact message-hash and/or private
class-key status entries required for duplicate admission, but never exposes a
placeholder as an RPC identity. Replay checkpoints persist class origins and
fork/status rows; class IDs and private tokens are rederived, and no fake
signature is stored. A checkpoint advertised as a stock/native Agave
snapshot instead requires transient rehydration of the real signatures from
the bound raw evidence and exact rebuilding of that cache. Replay V1 alone
cannot create such a native snapshot.

Signature omission is enabled only for slot ranges whose exact historical
runtime has been audited and differentially tested to use outer signature bytes
only for authorization, PoH mixing, status-cache behavior, and external
identity. Replay V1 preserves signer count, PoH mixin, the derivable message
hash, and the era-exact status-key equivalence relation, but not the signature
value. If a runtime reads any other function of the outer signature bytes during
consensus execution, Replay V1 is unsupported for that range; the producer must
rehydrate signatures from raw evidence or use a format successor.

Raw shreds cannot be retired in favor of Replay V1. Compact V2 may continue to
publish signatures for indexers, but that is a separate product and durability
decision.

## 3. Logical schema

One finite Replay V1 stream covers one complete finalized epoch and emits one
record per produced slot in ascending slot order. Skipped slots are absent from
the payload stream and are enumerated by the immutable finality manifest.
The final published generation binds the two-identity `FinalityManifestV1` from
the [compaction contract](blockzilla-compaction-job-v1.md#2-required-input-and-finalized-coverage).
Record sequence `n` maps to the `n`th `PRODUCED` entry in that manifest, not to
`slot - first_slot`; the entry supplies `final_poh_hash` and the era-exact
optional `consensus_block_id` omitted from the compact payload.

```text
ReplaySlotV1 {
  components: Vec<ReplayComponentV1>
}

ReplayComponentV1 =
    EntryBatch(Vec<ReplayEntryV1>)
  | BlockMarker(bytes)

ReplayEntryV1 {
  num_hashes: u64
  transaction_count: u32
  signature_mixin: [u8; 32] iff transaction_count > 0
  transactions: ReplayTransactionV1[transaction_count]
}

ReplayTransactionV1 {
  historical_status_backref: Option<StatusKeyClassRefV1>
  message: ReplayMessageV1
}

StatusKeyClassRefV1 =
    PriorTxDistance(u64)
  | PreviousClassId([u8; 24])

ReplayMessageV1 =
    Legacy(ReplayLegacyMessageV1)
  | V0(ReplayV0MessageV1)
  | Raw(ReplayRawMessageV1)

ReplayLegacyMessageV1 {
  header: [u8; 3]
  static_account_keys: Vec<ReplayAddressRefV1>
  recent_blockhash: RecentBlockhashRefV1
  instructions: Vec<ReplayInstructionV1>
}

ReplayV0MessageV1 {
  header: [u8; 3]
  static_account_keys: Vec<ReplayAddressRefV1>
  recent_blockhash: RecentBlockhashRefV1
  instructions: Vec<ReplayInstructionV1>
  address_table_lookups: Vec<ReplayAddressTableLookupV1>
}

RecentBlockhashRefV1 =
    PriorProducedDistance(u32)
  | PreviousTailIndex(u32)
  | Raw([u8; 32])

ReplayInstructionV1 {
  program_id_index: u8
  account_indexes: bytes
  data: bytes
}

ReplayAddressTableLookupV1 {
  table_account: ReplayAddressRefV1
  writable_indexes: bytes
  readonly_indexes: bytes
}

ReplayAddressRefV1 =
    RegistryId(u32)                    // ID 1..
  | Raw([u8; 32])                      // wire ID 0 followed by pubkey

ReplayRawMessageV1 {
  signed_message_bytes: bytes
}
```

The transaction's canonical one-byte tag packs the message variant and the
`historical_status_backref` presence bit; it is not followed by a separate
option tag. `None` is the implicit fresh class. The bit must be zero for a
runtime profile whose admission key is the derivable message hash.

### Slot and component rules

- A record's slot is not repeated in the payload. The enclosing sequence maps
  to the corresponding `PRODUCED` finality entry, which supplies the exact slot.
  Those resolved slot values strictly increase, and exactly one record exists
  for every produced slot selected by the manifest.
- The produced entries in the finality manifest form exactly one linear chain.
  Slot zero starts from the bound genesis hash. The first non-genesis record
  starts from the exact checkpoint/outside-generation parent; every later
  produced record's effective parent is the immediately preceding produced
  record, even across skipped slots. Every ordered `BlockHeader`/`UpdateParent`
  marker must resolve to that same slot and block identity. A branch, backward
  jump to a non-immediate ancestor, or alternate parent makes the generation
  incomplete; Replay V1 never carries multiple Bank states.
- The enclosing `RecordV1.sequence` is the zero-based produced-slot ordinal.
  Replaying the record derives its exact final PoH hash, which must equal the
  corresponding produced entry in the bound finality manifest. That entry also
  supplies the era-exact optional consensus/shred block ID. No redundant
  per-slot final hash, block ID, or final-hash registry is stored in Replay V1.
  In an era without a separate block ID, final PoH is the explicit fork
  identity; otherwise the finality entry must carry both distinct concepts.
- Components retain their original order and boundaries. An `EntryBatch` is
  non-empty. A `BlockMarker` contains the exact canonical serialized
  `VersionedBlockMarker` bytes, including its version/variant encoding and
  excluding only the enclosing `BlockComponent` entry-count discriminator that
  Replay V1's component tag replaces. The producer uses the pinned
  Agave-compatible component decoder, accepts only padding that decoder defines,
  and stores the canonical marker reserialization. Non-canonical source padding
  remains verifiable in raw shreds but is not copied into Replay V1.
- `components` is non-empty and contains at least one `EntryBatch`, so every
  record has a defined final PoH hash under the bound replay engine.
- Unknown marker versions are preserved by raw shreds but cannot enter a Replay
  V1 generation until the pinned replay engine declares support. They require a
  format or engine-descriptor successor rather than best-effort interpretation.

### Entry rules

- Transactions retain their exact order inside each entry and component.
- `transaction_count` is the exact number of following messages;
  `signature_mixin` is absent exactly when that count is zero. The conditional
  field has no separate option tag.
- For a non-empty entry it equals the pinned Solana
  `hash_signatures(all transaction signatures in entry order)` result.
- Starting with the exact genesis hash for slot zero, the exact
  outside-generation anchor for the first later record, or the immediately
  preceding produced record's final PoH hash, the producer and consumer
  recompute every entry hash from `num_hashes`
  plus the conditional mixin. The result must equal `final_poh_hash` in the
  matching finality entry. The protocol descriptor determines whether that hash
  is also the legacy fork identity or is separate from `consensus_block_id`.
- Individual entry hashes are intentionally omitted because they are derivable;
  object digests and the final boundary check detect corruption.
- Before hashing, sum `num_hashes` with overflow checks and enforce both the
  format limits below and the stricter genesis/era schedule. This resource check
  applies even in an era that no longer validates a legacy tick-hash count.

The V1 PoH algorithm is literal, not an ambient engine default. To construct a
non-empty entry's `signature_mixin`, flatten every 64-byte outer signature in
transaction order and signer order. Hash each leaf as
`SHA-256(0x00 || signature_64)`. Repeatedly hash adjacent nodes as
`SHA-256(0x01 || left_32 || right_32)`, duplicating the last node at any odd
level, until one root remains. The empty root is 32 zero bytes. The producer
stores that root only when `transaction_count > 0` after verifying all source
signatures.

Given previous hash `h0`, entry count `n = num_hashes`, and transaction count
`t`, derive the entry hash exactly as follows:

```text
if n == 0 and t == 0:
    entry_hash = h0
else:
    h = SHA-256 iterated max(n - 1, 0) times starting at h0
    if t == 0:
        entry_hash = SHA-256(h)
    else:
        entry_hash = SHA-256(h || signature_mixin)
```

This freezes Agave's `hash_signatures` and saturating-subtract
`next_hash_with_signatures` behavior, including zero-hash and tick/record edge
cases. A producer and replay engine use this formula for every ordered entry and
compare the last result with the finality entry. Golden fixtures cover empty
ticks, ordinary ticks, `num_hashes = 0` records, odd Merkle levels, multiple
signers, and multiple transactions.

### Historical status-key equivalence rules

Transactions receive a zero-based `tx_ordinal` by scanning published records,
components, entries, and transactions in canonical order. The runtime-profile
descriptor selects either derivable message-hash admission or the historical
signature-slice rule. Under the latter, status identity is the tuple
`(recent_blockhash, selected 20-byte first-signature slice, ancestor/root fork)`;
full-signature equality alone is not the rule. Replay V1 never stores the first
signature, but preserves that equivalence relation exactly:

- no backreference creates a fresh deterministic 24-byte class ID;
- `PriorTxDistance(d)` directly loads transaction ordinal `tx_ordinal - d`,
  requires `1 <= d <= tx_ordinal`, and requires the same resolved recent
  blockhash;
  and
- `PreviousClassId(id)` uses an exact live class for the same recent blockhash
  in the bound input checkpoint's historical status state.

`PriorTxDistance` uses the generation-wide `u64` ordinal. A backward distance
is used instead of an absolute ordinal because duplicate hits are normally near
their origin and therefore encode with fewer minimal-LEB128 bytes. The class-ID origin
uses the zero-based transaction ordinal within its slot, which is bounded by
the per-slot transaction maximum and therefore fits `u32`.

The class ID for the canonical origin occurrence represented by a class is:

```text
first_24_bytes(SHA-256(
  "blockzilla/replay/status-key-class/v1" ||
  cluster_genesis_hash ||
  origin_final_poh_hash ||
  canonical_option(origin_consensus_block_id) ||
  origin_slot_be_u64 ||
  origin_slot_transaction_ordinal_be_u32 ||
  recent_blockhash_32
))
```

The prior class ID is that same direct 24-byte value stored in the bound input
checkpoint's live signature-slice status state. Including both selected-lineage
block identities prevents equal coordinates on sibling forks from receiving the
same ID. The ID is independent of random stream IDs and reveals no signature
bytes. A checkpoint retains each live class's recent blockhash, canonical origin
coordinate and block identities, fork/status rows, and source-evidence anchor.
The class ID and private 20-byte token are deterministically derived on read and
are not redundantly copied into the checkpoint. A detected derived-ID or token
collision between distinct classes fails the generation rather than merging
them.

```text
private_token_20 = first_20_bytes(SHA-256(
  "blockzilla/replay/status-key-token/v1" || class_id_24
))
placeholder_signature[i] = private_token_20[i mod 20]  // i = 0..63
```

The adapter checks token uniqueness for distinct live classes within each
recent-blockhash cohort before inserting any placeholder.

The bound `StatusKeyEvidenceV1` object accounts for every recent-blockhash
cohort that is live in the input state or introduced/touched while scanning the
complete `validation_slots` range. Each cohort appears exactly once in one of
two classes:

1. `KnownIndex(i)`: the input start state or immutable resolution evidence
   supplies the exact persisted slice offset, and classes are equality of that
   selected 20-byte slice; or
2. `AllOffsetsCollisionFree`: for every pair of unequal first signatures in the
   cohort while they can coexist in status state, no permitted 20-byte slice is
   equal at any possible offset. Full-signature equality is then provably the
   same relation regardless of the missing historical random offset.

The era profile pins the cache revision, input key, offset function, and
permitted indices. The launch revision selected `0..11` even though it sliced a
64-byte first signature. A later generic cache selects
`0..=key_len.saturating_sub(21)`, which is `0..=43` for a 64-byte key and
`0..=11` for a 32-byte message hash. These rules are not interchangeable. The
all-offset proof covers every distinct key sharing the recent blockhash,
including live checkpoint rows and all following transactions until that hash
ages out. If an offset is unknown and the proof fails, the producer must
retain/rehydrate signatures or reject that range. It may not assume a 20-byte
collision did not matter merely because it is improbable.

For a modern message-hash profile, the exact 32-byte key is derived from the
stored signed message, so no per-transaction backreference is needed. Its native
cache still selects a 20-byte slice; `StatusKeyEvidenceV1` must therefore bind a
known `0..11` offset or prove all-offset collision freedom for those derivable
keys as well. The replay adapter installs the exact known index before inserting
that cohort; an all-offset proof permits its descriptor-pinned deterministic
index. “No per-transaction backreference” does not mean “ignore native slice
collisions.”

Encoding is canonical. If the status slice maps to a prior class in the current
generation, use `PriorTxDistance` pointing to its greatest earlier ordinal
(therefore the smallest positive distance).
Otherwise, if it maps to a live checkpoint class, use its exact
`PreviousClassId`; otherwise omit the backreference. Falsely merging or
splitting classes invalidates the generation. A separate prior-class registry
is intentionally omitted because cross-generation hits are exceptional. The
adapter keeps private status entries only where the pinned historical runtime
uses them, alongside message-hash entries where that era uses them, and never
returns a class ID, token, or placeholder as a transaction ID.

All non-payload Replay protocol structures from this point onward—including
status evidence, checkpoint, resolution, validation, publication, catalog, and
recovery objects—use the compaction contract's canonical big-endian fixed
integers, one-byte enum/option tags, `u32` vector/byte lengths, direct fixed
arrays, sorted fields where required, and trailing-byte rejection. Unless an
explicit semantic hash is defined, an `ObjectRefV1.sha256` is ordinary SHA-256
of those exact stored canonical bytes. The two named job-spec objects are the
declared exception: their stored bytes include their literal domain prefix
before the canonical struct. This outer-object rule is distinct from the
minimal-LEB128 Replay payload encoding in Section 5.

The evidence object is canonical rather than an informal audit note:

```text
StatusKeyCohortIdV1 {
  recent_blockhash: [u8; 32]
  introduced_slot: u64
  introduced_final_poh_hash: [u8; 32]
  introduced_consensus_block_id: Option<[u8; 32]>
  key_kind: u8                         // SIGNATURE_64=1 | MESSAGE_HASH_32=2
}

KnownStatusKeyIndexV1 {
  cohort: StatusKeyCohortIdV1
  key_index: u8
  source: KnownStatusKeyIndexSourceV1
}

KnownStatusKeyIndexSourceV1 =
    StartState = 0
  | ResolutionEvidenceOrdinal(u32) = 1

StatusKeyEvidenceV1 {
  cluster_genesis_hash: [u8; 32]
  resolution_spec_hash: [u8; 32]
  input_start_state_sha256: [u8; 32]
  published_slots: SlotRangeV1
  finality_validation_slots: SlotRangeV1
  validation_slots: SlotRangeV1
  status_cache_profile_sha256: [u8; 32]
  cohort_count: u64
  occurrence_count: u64
  distinct_key_count: u64
  all_offsets_cohort_count: u64
  all_offsets_scan_sha256: [u8; 32]
  known_indices: Vec<KnownStatusKeyIndexV1>
}
```

One native cohort is one uninterrupted live lifetime of
`(recent_blockhash, key_kind)` on the resolution's exact validation traversal.
For a checkpoint-live cohort, `introduced_*` is the cohort-creation coordinate
persisted in that checkpoint, including both era-defined block identities. For a
cohort created after the boundary, it is the slot and block identities of the
earliest canonical transaction occurrence whose native admission inserts that
cohort; transaction order is the Replay component/entry/transaction order. The
cohort ends exactly when the pinned runtime evicts it. Reuse of the same recent
blockhash after eviction creates a new cohort and new introduction coordinate.
The traversal and every suffix candidate identity are immutable resolution
evidence; arrival order and the evidence object's construction order never choose
the origin. An absent consensus block ID is permitted only in an era that does
not define one.

Known-index rows are sorted uniquely by canonical cohort ID. `StartState` is
valid only when the bound genesis/checkpoint materialization proves that exact
cohort and persisted offset. `ResolutionEvidenceOrdinal(n)` indexes the
resolution spec's canonical `evidence_object_inputs` vector directly; `n` must
be in range, and that exact input's registered format must prove the same
cohort, status-cache profile, and offset. The evidence object cannot introduce
an object that is absent from the frozen resolution spec.

All cohorts without a known-index row are scanned in canonical
`StatusKeyCohortIdV1` byte order. For each,
the producer forms the sorted distinct set of complete native input keys from
live checkpoint rows and every selected-lineage occurrence until the runtime
profile proves that recent blockhash evicted. It hashes the cohort row as:

```text
SHA-256(
  "blockzilla/replay/status-key-cohort/v1" || canonical_cohort_id ||
  occurrence_count_be_u64 || distinct_key_count_be_u64 ||
  SHA-256(repeated(key_len_u8 || SHA-256(complete_key_bytes)))
)
```

`all_offsets_scan_sha256` is SHA-256 of
`"blockzilla/replay/status-key-scan/v1"`, the canonical profile digest, the
all-offset cohort count, and those 32-byte cohort rows in that order. Each such
cohort must pass collision checking at every profile-permitted index. The
recomputed set of known-index plus all-offset cohorts must equal the complete
live/touched cohort set exactly; omission, duplication, or an extra row is
invalid. Counts in the outer object equal the sum across all all-offset and
known-index cohorts.
The validation range extends far enough to close every unknown-index cohort;
otherwise the resolution is `NOT_COMPLETE`. A known index is allowed only when
the referenced immutable native state proves that exact cohort/offset.

The object permits at most 1,048,576 cohorts, 65,536 sparse known-index rows,
and 268,435,456 encoded bytes. The resolution spec's ordinary 4,096-object input
bound also bounds evidence ordinals. Key/cohort scans are streaming and
overflow-checked; full keys remain in permanent evidence, not this receipt. The
publication job/result, final-byte receipt, runtime descriptor,
completion closure, and recovery mapping all bind this exact ObjectRef/digest.

The decoder resolves a `PriorTxDistance` in O(1) from an already-resolved live
class table; it never follows a recursive backreference chain. Entries are
evicted with their recent-blockhash cohort. At most 16,777,216 live status-class
rows may be required by one generation; exceeding that format working-set bound
rejects the range rather than allocating without limit.

### Message rules

- The three header bytes retain `num_required_signatures`,
  `num_readonly_signed_accounts`, and `num_readonly_unsigned_accounts`. Signature
  count is therefore preserved without retaining signature bytes.
- Before stripping signatures, the producer decodes and sanitizes the complete
  original signed transaction under the pinned era: message-version activation,
  full serialized-size limit, exact
  `signatures.len == num_required_signatures`, header/account relationships,
  instruction/lookup indexes, and every other era rule must pass. Placeholder
  reconstruction may never turn a malformed signed envelope into valid input.
- Static account order, instruction order, instruction account indexes,
  instruction data, and address-table lookup order/indexes are exact.
- Loaded addresses are not stored. Replay resolves them from the historical
  address-table account state.
- Expanding registry references and serializing a compact message must reproduce
  the exact original Solana signed-message bytes byte-for-byte. The producer
  compares this expansion with the message bytes extracted from the verified
  transaction before publication.
- Legacy and V0 messages must use their compact variants. `Raw` is the canonical
  escape hatch for another message version already supported by the bound replay
  engine; it contains the exact signed-message serialization, not the surrounding
  signature vector or transaction short-vector prefix. `Raw` must not encode a
  Legacy or V0 message.
- A message version unsupported by the bound replay engine still blocks Replay
  V1 production for that range. It is never silently converted or dropped. A
  later payload-format version may add a compact variant if the raw form becomes
  common enough to justify it.

### Canonical execution-core expansion

Runtime attachments and parity never hash storage-specific registry IDs. Every
published record expands to this exact semantic form:

```text
ExecutionCoreSlotV1 {
  cluster_genesis_hash: [u8; 32]
  slot: u64
  final_poh_hash: [u8; 32]
  consensus_block_id: Option<[u8; 32]>
  parent: ExecutionCoreParentV1
  components: Vec<ExecutionCoreComponentV1>
}

ExecutionCoreParentV1 =
    Genesis = 0
  | Parent = 1 {
      slot: u64,
      final_poh_hash: [u8; 32],
      consensus_block_id: Option<[u8; 32]>
    }

ExecutionCoreComponentV1 =
    EntryBatch(Vec<ExecutionCoreEntryV1>) = 0
  | BlockMarker(bytes) = 1

ExecutionCoreEntryV1 {
  num_hashes: u64
  signature_mixin: [u8; 32] iff transactions is non-empty
  transactions: Vec<ExecutionCoreTransactionV1>
}

ExecutionCoreTransactionV1 {
  historical_status_key_class_id: Option<[u8; 24]>
  signed_message_bytes: bytes
}
```

Resolve the slot and final identities from the matching finality entry, the
parent identities from the immediately preceding selected record/anchor, every
address and
recent-blockhash reference to its raw bytes, every Legacy/V0 message to its
exact Solana signed-message serialization, and every status-key backreference to its
deterministic 24-byte status-key class ID when the selected runtime profile uses
historical signature-slice admission. Preserve component, marker, entry, and transaction
order exactly. The expansion uses the compaction contract's canonical
big-endian integers, one-byte enum/option tags, `u32` vector/byte lengths, direct
fixed arrays, and trailing-byte rejection. Replay V1's hard bounds also bound
the expansion.

```text
execution_core_digest = SHA-256(
  "blockzilla/execution-core/v1" ||
  canonical_encode(ExecutionCoreSlotV1)
)

expanded_projection_sha256 = SHA-256(
  "blockzilla/replay/expanded-projection/v1" ||
  produced_count_be_u32 ||
  repeated(
    execution_core_len_be_u64 || canonical_encode(ExecutionCoreSlotV1)
  )
)
```

Rows are in produced-slot order. The length prefix removes concatenation
ambiguity. These preimages exclude stream ID, prefix hashes, registry/tail IDs,
compression, physical keys, runtime attachments, receipts, and catalog objects.
Golden fixtures include direct/raw references, current/previous blockhash refs,
all status-key variants, markers, skipped slots, and genesis.

## 4. Replay-scoped registries

Replay V1 reuses the existing compact-reference mechanism:

```text
address ref 1.. = one-based unsigned-LEB128 registry ID
address ref 0   = raw fallback followed by the exact 32-byte pubkey
```

`replay-address-registry.bin` is a headerless sequence of 32-byte pubkeys. It
contains only addresses referenced by the compact Legacy/V0 message variants:

- static message account keys; and
- address lookup table account keys.

It does not include loaded addresses, runtime-only CPI programs, reward accounts,
log-derived keys, token-balance keys, or other Compact V2 metadata. This makes
the registry reproducible from shreds alone.

For canonical ordering, count one occurrence for every static-account-key field
and one for every V0 lookup-table-account field in compact Legacy/V0 messages.
Instruction account/program indexes, loaded addresses, and raw-message bytes add
no occurrences. A pubkey with one occurrence is encoded through raw fallback and
is absent from the registry; a pubkey with at least two occurrences is
registered. Sort registered pubkeys by descending occurrence count, then by
ascending lexicographic 32-byte value, and encode them by ID everywhere. This is
always smaller at the logical-byte level even for a five-byte `u32` ID, while a
singleton registry row is not. The registry object is absent when no pubkey is
registered.

The registry SHA-256, when present, is pinned in the candidate/completion
manifest after the deterministic projection pass, not in the pre-provisioned
output stream manifest. `registry.mphf` may be built for encoding or lookup
speed, but it is rebuildable and is not required to resolve an ID.

`replay-prev-blockhash-tail.bin` contains a profitable final-PoH-hash tail for
compact messages near the epoch boundary. Construction temporarily tracks
`(slot, final_poh_hash)` rows. Scan
Replay records and their compact Legacy/V0 messages in canonical order while
tracking final PoH hashes already derived in the current generation. For each
message recent hash that does not match a current prior record, select the most
recent produced slot with that hash in the exact predecessor Replay generation,
or in the verified checkpoint's bound predecessor history when bootstrapping a
mid-chain first generation. Count how many references each selected row would
replace. Rows used once stay `Raw`; rows used at least twice are profitable even
with a five-byte `u32` index. Sort eligible rows by descending use count, then
ascending slot and final PoH bytes, retain at most the hard row limit, and encode
every unretained value as `Raw`. That exact set/order is canonical. A
`PriorProducedDistance(d)` in record sequence `s` resolves the final PoH hash of
sequence `s - d`; `PreviousTailIndex(n)` resolves row `n` of this
sidecar. A recent blockhash absent from both is stored through
`RecentBlockhashRefV1::Raw`; the format does not claim that every raw value is a
durable nonce.

The previous-tail sidecar is a headerless sequence of `final_poh_hash_32`; its
byte length must be divisible by 32 and rows follow the canonical
frequency/most-recent-slot/hash selection order above. Slot is construction
provenance only and is not copied into the replay sidecar.
`PriorProducedDistance(d)` requires `1 <= d <= s`; zero/underflow is invalid,
and a previous-tail index must be in range. Backward distance makes the common
recent reference smaller under minimal LEB128. This
keeps sequential replay possible and prevents a value that appears only in a
later block from becoming a forward reference.

Encoding choice is canonical. For a compact message, use the greatest earlier
current produced ordinal `n` whose final PoH hash matches and encode distance
`s - n`; otherwise use its exact
retained previous-tail row; otherwise use `Raw`. A producer must not emit `Raw`
or a tail reference when an eligible current reference exists, and must not emit
`Raw` when the hash is in the canonical retained tail. The predecessor
generation and tail digest are mandatory for any generation whose canonical
scan retains a row; otherwise the tail object is absent.

The replay registry is not the Compact V2 registry. An implementation may share
code and may deduplicate identical physical objects by digest, but Replay V1 IDs
cannot be influenced by runtime metadata. Compact V2 may use the replay address
registry as a base and add its own metadata registry/sidecar.

### Program-role cache

Every instruction already stores `program_id_index`; a second registry does not
reduce the signed message. Frequently repeated program pubkeys already qualify
for the ordinary replay address registry because they appear in the message's
static account-key list; the one-byte instruction index then points to that key.
Executability and loader/program-data selection also
change with historical account state. A consumer may build a program-role
bitset or hot-program ordering as an external cache keyed by the Replay
generation and address-registry digests. It is absent from Replay V1 schemas,
jobs, receipts, and catalog identity; deleting or rebuilding it cannot change
replay.

## 5. Canonical encoding and stream binding

The inner payload uses the same minimal integer rules as Blockzilla's compact
types:

- unsigned integers use minimal unsigned LEB128;
- fixed hashes and pubkeys are direct bytes;
- byte strings and vectors begin with an unsigned-LEB128 `u32` length, except
  that an entry's explicit `transaction_count` is also the following message
  array length and is not repeated;
- component tags are `0 = EntryBatch`, `1 = BlockMarker`;
- each transaction has one tag byte: bits 0..1 are `0 = Legacy`, `1 = V0`,
  `2 = Raw`; bit 2 says a historical status backreference follows; bits 3..7
  are zero;
- when bit 2 is set, the following status-ref tag is
  `0 = PriorTxDistance`, `1 = PreviousClassId`; an unknown tag is invalid;
- recent-blockhash tags are `0 = PriorProducedDistance`,
  `1 = PreviousTailIndex`, `2 = Raw`.

Fields are encoded in schema order. Non-minimal integers, unknown tags,
overflowing counts, out-of-range indexes, invalid registry IDs, unsupported
message/marker versions, and trailing bytes are rejected.

### V1 hard limits

These are protocol maxima, not deployment defaults. A job, runtime, or era
descriptor may narrow them but can never widen them. A source-era rule that
requires a larger value is unsupported by Replay V1 and requires a format
successor; no producer may silently split, truncate, or drop it.

| Value | Replay V1 maximum |
| --- | ---: |
| one `ReplaySlotV1` payload | 67,108,864 bytes |
| produced records in one epoch generation | 1,048,576 |
| components in one slot | 1,048,576 |
| entries in one `EntryBatch` or one slot in total | 1,048,576 |
| messages in one entry or one slot in total | 524,288 |
| `num_hashes` in one entry | 16,777,216 |
| sum of `num_hashes` in one slot | 67,108,864 |
| live historical status-class working rows | 16,777,216 |
| compact static account keys | 256 |
| top-level instructions in one compact message | 1,232 |
| account indexes in one instruction | 1,232 |
| instruction data in one compact instruction | 1,232 bytes |
| V0 address-table lookups | 1,232 |
| writable or readonly indexes in one lookup | 1,232 each |
| expanded Legacy/V0 signed-message bytes | 1,232 bytes |
| `Raw.signed_message_bytes` | 4,096 bytes |
| one supported V1 block marker | 65,540 bytes |
| all block-marker bytes in one slot | 67,108,864 bytes |
| replay address-registry entries | 134,217,728 |
| `replay-address-registry.bin` | 4,294,967,296 bytes |
| previous-blockhash-tail rows | 65,536 |
| `replay-prev-blockhash-tail.bin` | 2,097,152 bytes |
| compressed stored chunk | 268,435,456 bytes |
| decoded bytes from one stored chunk | 268,435,456 bytes |
| stored payload chunks in one generation | 32,700 |
| uncompressed `RecordV1` bytes in one generation | 4,398,046,511,104 bytes |
| `StatusKeyEvidenceV1` | 268,435,456 bytes |

The 1,232 count/byte maxima above are format allocation bounds derived from the
largest Legacy/V0 wire message, not current feature rules. They deliberately do
not freeze SIMD-era limits such as 64 instructions or 255 instruction accounts;
the pinned historical sanitizer may impose those only where activated. The
expanded message must satisfy that era's stricter limits. Aggregate limits are
checked with overflow-safe arithmetic against both the remaining message bytes
and the table before allocating a claimed vector or decompressing to an
unbounded buffer. The aggregate generation/chunk caps intentionally prevent the
product of every independent maximum from becoming an unrepresentable object
set and leave ample registered-name space for manifests and sidecars. Golden fixtures prove these constants; they do not
replace them.

The payload is carried in the ordinary Hivezilla `RecordV1` envelope:

```text
sequence_be_u64 || payload_len_be_u64 || ReplaySlotV1 || prefix_hash
```

This deliberately spends the existing envelope's fixed 48 bytes per produced
slot—at most 50,331,648 bytes at the V1 generation bound—to reuse Hivezilla's
framing, prefix-chain verification, retry, and reader tooling. The execution
payload itself does not repeat those fields. A future outer-container revision
may benchmark and remove that transport overhead without changing
`ReplaySlotV1`; V1 does not introduce a second framing protocol for a negligible
fraction of expected ledger bytes.

The format-8 stream is finite and derived. It has no overflow, terminal-store,
durability, or deletion-authorizing fields. After finality is frozen and before
granting a publication attempt, Blockzilla creates one exact output
`StreamManifestV1` with one random `stream_id` and embeds that immutable manifest
in the replay job. Every retry uses those same bytes; a worker never invents a
new output stream header. This keeps `RecordV1` prefix hashes byte-identical
across fences.

The stream header has the job's cluster, `payload_format = 8`, and
`payload_format_version = 1`. Because catalog generations—not source streams—
carry Replay lineage, `lineage`, `gap_event_stream_id`,
`overflow_namespace_sha256`, `deletion_authorizing_store_id`, and
`durability_policy` are all exactly `None`. A non-empty option is invalid rather
than harmless metadata.

The manifest's immutable producer descriptor binds:

- exactly one cluster/genesis and epoch;
- the exact protected raw ordinary/repair shred ranges, or the exact immutable
  historical signed-entry objects used for a declared backfill range;
- the immutable finality manifest and an outside-epoch parent anchor containing
  the exact parent slot, final PoH hash, and era-defined optional consensus
  block ID;
- leader schedule, shred trust, transaction-signature verification, FEC,
  component, PoH, and marker-processing descriptors;
- the address-registry, previous-blockhash-tail, and historical status-key
  construction algorithms and logical roles, but no
  attempt-specific object key;
- the deterministic projection/encoding algorithm descriptor; and
- any predecessor Replay V1 generation needed for the parent boundary.

The Replay candidate and completion manifests bind the exact output stream manifest plus
all chunk, address-registry, previous-blockhash-tail, descriptor, and
status-evidence object keys, lengths, and SHA-256 digests. Physical
keys may contain the attempt fence, but neither they nor provider version tokens
enter the pre-provisioned stream header or logical Replay V1 content identity.

Replay V1 uses a **separate replay-product catalog head**, provisioned for one
cluster, replay-format descriptor, and epoch schedule. It never appends an epoch
to the Archive V2 catalog head and is not inserted into the live-stream
registry. The replay job pins its exact output stream manifest, expected replay
catalog predecessor/generation, and output namespace. A Hivezilla worker may
upload only a fenced candidate; Blockzilla validates the result and advances
only the replay catalog by exact compare-and-swap. Payload ID 8 remains
non-executable until these product-specific job, completion, catalog, and golden
fixture schemas are implemented together. Replay publication never acknowledges
raw custody or authorizes raw deletion.

Compression and object chunking are outer storage concerns. A publisher may pack
consecutive `RecordV1` values into independently checksummed zstd objects for
sequential throughput and restart, but the compression descriptor, exact object
bytes, ranges, lengths, and digests are bound by the completion manifest. Codec
bytes never replace the uncompressed Replay V1 content identity.

## 6. Construction and replay promotion

Marker-derived finality cannot be an input to the work that derives it.
Blockzilla first freezes an immutable resolution specification:

```text
ReplayCheckpointRefV1 {
  checkpoint_format: HashedDescriptorV1
  checkpoint: ObjectRefV1
  state_through_slot: u64
  bank_slot: u64
  final_poh_hash: [u8; 32]
  consensus_block_id: Option<[u8; 32]>
  bank_hash: [u8; 32]
  accounts_hash: [u8; 32]
}

ReplayStartStateV1 =
    Genesis = 0 { genesis_bin: InputObjectV1 }
  | Checkpoint(ReplayCheckpointRefV1) = 1

ReplayCheckpointChunkV1 {
  decoded_offset: u64
  decoded_length: u64
  object: ObjectRefV1
}

ReplayCheckpointSectionV1 {
  role: bytes
  codec: HashedDescriptorV1
  decoded_length: u64
  decoded_sha256: [u8; 32]
  chunks: Vec<ReplayCheckpointChunkV1>
}

ReplayCheckpointManifestV1 {
  cluster_genesis_hash: [u8; 32]
  state_through_slot: u64
  bank_slot: u64
  final_poh_hash: [u8; 32]
  consensus_block_id: Option<[u8; 32]>
  bank_hash: [u8; 32]
  accounts_hash: [u8; 32]
  runtime_and_feature_map_sha256: [u8; 32]
  status_cache_profile_sha256: [u8; 32]
  sections: Vec<ReplayCheckpointSectionV1>
}

ReplayCheckpointInputPolicyV1 =
    Genesis = 0 {
      genesis_decoder_sha256: [u8; 32]
    }
  | Checkpoint = 1 {
      checkpoint_format_sha256: [u8; 32],
      runtime_and_feature_map_sha256: [u8; 32],
      status_cache_profile_sha256: [u8; 32]
    }

ReplayCheckpointTransitionV1 {
  input_policy: ReplayCheckpointInputPolicyV1
  migration_algorithm: HashedDescriptorV1
  successor_checkpoint_format_sha256: [u8; 32]
}

FinalityResolutionSpecV1 {
  resolution_id: [u8; 16]
  cluster_genesis_hash: [u8; 32]
  epoch: u64
  published_slots: SlotRangeV1
  finality_validation_slots: SlotRangeV1
  validation_slots: SlotRangeV1
  evidence_stream_inputs: Vec<InputStreamRangeV1>
  evidence_object_inputs: Vec<InputObjectV1>
  trust_context: Option<InputObjectV1>
  start_state: ReplayStartStateV1
  runtime_and_feature_map: HashedDescriptorV1
  status_cache_profile: HashedDescriptorV1
  checkpoint_transition: HashedDescriptorV1
  finality_rule: HashedDescriptorV1
  epoch_schedule: HashedDescriptorV1
  state_validation_format: HashedDescriptorV1
}

FinalityResolutionJobSpecV1 {
  work_kind: u8                         // REPLAY_FINALITY_RESOLUTION = 2
  resolution_id: [u8; 16]
  resolution_spec: ObjectRefV1
  output_namespace: bytes
  output_policy: HashedDescriptorV1
}

FinalityResolutionAttemptV1 {
  work_kind: u8                         // REPLAY_FINALITY_RESOLUTION = 2
  resolution_id: [u8; 16]
  job_spec: ObjectRefV1
  fence: u64
}

FinalityResolutionResultV1 {
  work_kind: u8                         // REPLAY_FINALITY_RESOLUTION = 2
  resolution_id: [u8; 16]
  job_spec_hash: [u8; 32]
  resolution_spec_hash: [u8; 32]
  fence: u64
  outcome: COMPLETE = 1 | NOT_COMPLETE = 2
  proposed_finality_manifest: Option<ObjectRefV1>
  published_epoch_validation: Option<ObjectRefV1>
  lookahead_finality_validation: Option<ObjectRefV1>
  status_key_evidence: Option<ObjectRefV1>
}
```

`Genesis` is valid only for the generation containing cluster slot zero and
binds the exact `genesis.bin` object/decoder; that generation's slot-zero
finality entry must be `PRODUCED`. `Checkpoint` is required for every
later generation. `state_through_slot` is the last slot disposition already
consumed, while `bank_slot` is the last produced Bank represented by the state;
they may differ across skipped slots. The checkpoint object contains all account
and non-account replay state listed in Section 7, including blockhash/status
queues and class-origin evidence mappings. `checkpoint` points to the canonical
`ReplayCheckpointManifestV1`, not one enormous state blob. Its scalar fields
equal `ReplayCheckpointRefV1` byte for byte. Section roles/codecs are a closed
set declared by `checkpoint_format`; sections are sorted by unique 1..128-byte
ASCII role, and their decoded chunks have contiguous offsets from zero with no
gap/overlap. Every chunk object is immutable and independently length/digest
verified; streaming decode must reproduce the section length and SHA-256.
The manifest has at most 256 sections and 16,777,216 encoded bytes; the whole
checkpoint has at most 32,768 chunks. Each chunk has at most 1,073,741,824
stored bytes and at most 1,073,741,824 decoded bytes; all chunks together have
at most 35,184,372,088,832 stored bytes and at most 35,184,372,088,832 decoded
bytes. Bounds are checked with overflow-safe sums before allocation, while
account/state decoding remains streaming.
Checkpoint bytes must not contain a validation-receipt, completion, catalog, or
runtime-attachment identity; those later objects may reference the checkpoint,
never the reverse.

`checkpoint_transition.bytes` is exactly the canonical encoding of
`ReplayCheckpointTransitionV1`; its ordinary descriptor digest must verify. For
a checkpoint start its input policy declares the one accepted tuple of
checkpoint-format, runtime/feature-map, and status-cache-profile digests. For
genesis it instead pins the exact `genesis_bin.format.sha256`; the union tag must
match `start_state`. `migration_algorithm` deterministically converts that input
into the spec's exact `runtime_and_feature_map` and `status_cache_profile` and
has a registered identity/no-op value. The decoded input checkpoint manifest
must match the declared input tuple; an undeclared “compatible” local snapshot
is invalid. The successor uses the declared checkpoint format and carries the
spec's exact runtime/feature-map and status-cache-profile digests. A policy
transition is therefore explicit and hashed rather than inferred from equal slot
numbers.

`published_slots` is exactly one epoch. Both validation ranges start at the
same slot, and `published_slots.next_slot <=
finality_validation_slots.next_slot <= validation_slots.next_slot`.
`finality_validation_slots - published_slots` is the exact descendant prefix
that the finality rule may project and statefully replay. The remaining
`validation_slots - finality_validation_slots` suffix is scanned only for the
status-key proof; it cannot settle finality, mutate replay state, enter the
successor checkpoint, or produce a generation record. Evidence vectors use the
ordering, identity, and bounds of the compaction contract. `trust_context` identifies the
exact leader/proof or historical-corpus trust object; a descriptor without its
immutable bytes is insufficient. The spec exists before any finality manifest,
is canonically encoded, and has semantic identity
`SHA-256("blockzilla/replay/finality-resolution/v1" || canonical_spec)`.
It cannot change across a retry. The exact immutable stored job-spec bytes are:

```text
FinalityResolutionJobSpecObjectV1 =
  "blockzilla/replay/finality-resolution-job/v1" ||
  canonical_encode(FinalityResolutionJobSpecV1)

job_spec_hash = SHA-256(FinalityResolutionJobSpecObjectV1)
```

Before the first grant, Blockzilla stores those exact bytes in a permanently
reader-visible namespace. `FinalityResolutionAttemptV1.job_spec.sha256` equals
`job_spec_hash`, and its length/digest must verify before execution. Only the
attempt envelope carries the globally monotonic `fence`; retries reuse the same
job-spec ObjectRef byte for byte. The active work ID is `resolution_id`, and
only work kind `REPLAY_FINALITY_RESOLUTION` may return this result.

`output_namespace` is a pre-provisioned, permanently reader-visible immutable
namespace. `output_policy` pins its credential scope, registered object roles,
maximum bytes, and deterministic keys. The worker has conditional-create only
inside that namespace; it cannot overwrite, delete, or write either catalog
head. Result objects and every referenced validation/finality object remain at
those exact keys—publication never copies them to a new locator. Blockzilla
accepts only the current job ID/spec hash/fence and verifies object length and
SHA-256 before freezing any result.

`COMPLETE` requires the proposed manifest, published-epoch validation, and
status-key evidence. `NOT_COMPLETE` requires all four optional references
absent. `lookahead_finality_validation` is present exactly when
`finality_validation_slots.next_slot > published_slots.next_slot`; otherwise it
is absent. A range extended only to close status-key cohorts therefore does not
fabricate a finality-lookahead object; that suffix is committed by
`status_key_evidence` instead.
The completed `StatusKeyEvidenceV1` has the spec's exact semantic
`resolution_spec_hash`, start-state digest, `published_slots`,
`finality_validation_slots`, `validation_slots`, and
`status_cache_profile.sha256`; it does not repeat the descriptor bytes, and a
merely locally compatible profile is invalid.
The proposed manifest must have the spec's exact cluster, epoch, published,
finality-validation, and full-validation ranges and evidence; its published
range must be one complete epoch under the spec's epoch schedule.
`published_epoch_validation` uses the exact
bounded format named by `state_validation_format` and binds the projected
candidate hashes for `published_slots` plus the checks required by the finality
rule. A marker/Tower rule includes its per-slot PoH/Bank/account validation; an
independent external authority may bind structural candidate checks only, but
the later Replay publication still must pass full stateful replay.
`lookahead_finality_validation` binds exactly the descendant
projection/evidence and checks in `finality_validation_slots - published_slots`.
That stateful lookahead runs from a copy of the published-epoch end state and
can never advance or mutate the successor checkpoint. The manifest
authority descriptor binds the resolution-spec hash and exact finality rule. An
unknown format or rule is unsupported, never a local default. Blockzilla accepts
only the current `(resolution_id, fence)`
and then freezes the manifest; a stale result cannot do so. An external finality
resolver uses the same evidence-bound spec but may finish before stateful replay.
A marker/Tower resolver must execute through `finality_validation_slots` and
preserve the epoch-boundary state before executing that finality lookahead. It
scans any remaining status-only suffix without executing it into the successor
state.

Projection and stateful validation are then separate gates. The projection
builder may encode a non-canonical candidate only after it completes these
state-independent checks:

1. verify scheduled-leader/retransmitter identity and signatures, except for
   slot zero as defined below;
2. verify Merkle proofs, FEC-set roots/chains, repair provenance, and every
   recovered shred;
3. prove complete data-shred coverage and decode all ordered block components;
4. retain every marker in component order, decode Agave-compatible source
   padding, and store/validate the canonical marker re-encoding,
   placement, parent-identity, bounds, and other state-independent checks;
5. decode and sanitize each complete original signed transaction under the
   pinned era, including version activation, full serialized-size limit, exact
   signature count, header relationships, and all message/index rules;
6. verify every transaction signature against the exact signed-message bytes;
7. compute each non-empty entry's signature mixin from the original signatures;
8. derive the canonical historical status-key equivalence refs from the exact
   era policy, original signatures, status-key evidence, and checkpoint's live
   status classes;
9. verify the complete PoH chain and the state-independent component/parent/block
   identity rules;
10. round-trip every compact message, or byte-compare every canonical `Raw`
   message, with the exact original signed-message bytes;
11. bind the candidate to the exact evidence set, protocol era, and registry
   generation.

Slot zero is not authenticated through the ordinary leader schedule. Its
slot-zero record is accepted only when its deterministic tick/entry sequence,
PoH start/final hash, and shred parameters match the descriptor-pinned
genesis-ledger construction from the exact bound `genesis.bin`. Agave creates
slot-zero shreds with an arbitrary fresh keypair, so `genesis.bin` alone does not
determine their exact signed bytes: the signer is ignored as authority while
all shred structure is checked. Byte-exact shred equality is required only when
the complete genesis archive/blockstore itself is digest-bound. Slot zero has no
user transactions or parent. If it is `PRODUCED` in the finality manifest, the
`Genesis` Replay record is mandatory; Bank 0 cannot silently move between the
payload and checkpoint.

Marker validation that reads historical Bank state cannot be a projection
prerequisite: footer bank hashes, clock bounds, genesis/finalization/reward
certificates, stake sets, and their state transitions are precisely replay work.
The replay-promotion gate therefore:

1. starts from the bound verified genesis/checkpoint and executes the candidate
   with the pinned runtime;
2. validates every state-dependent marker rule, certificate, clock/reward
   transition, and expected footer Bank hash in original component order;
3. verifies the resulting final PoH, Bank/account hashes, and parent state;
4. matches or produces the immutable finality entry carrying the era-exact final
   PoH hash and optional consensus block ID; and
5. permits Blockzilla's replay-catalog commit only after all prior slots and the
   whole generation pass.

An external finality authority may select the candidate before replay, but it
does not waive the stateful gate. If finality is derived from block-marker
certificates, candidate projection necessarily comes first and the verified
replay result creates the finality manifest; the same unvalidated certificate
cannot circularly authorize its own input. Blockzilla then freezes that manifest
and issues a separate deterministic publication pass with the pre-provisioned
format-8 stream manifest. The internal candidate is not itself the published
stream.

The manifest's `slots` range is the exact epoch published by this generation.
Its `finality_validation_slots` range may extend beyond the epoch because a
Tower root or marker-era finalization certificate in a descendant slot can
settle an earlier slot. Its full `validation_slots` range may extend farther
when an unknown status-cache offset requires collision scanning until a
recent-blockhash cohort expires. The pre-finality worker projects and
statefully replays the immutable bounded finality prefix, then streams the
status-only suffix without emitting either suffix as generation records.
`finality_validation_slots == slots` exactly when finality needs no descendant
lookahead; `validation_slots == finality_validation_slots` exactly when status
proof needs no additional suffix. Thus `validation_slots == slots` only when
neither proof needs later evidence. A trailing slot
that still lacks sufficient descendant evidence leaves the epoch unresolved
and unpublished rather than weakening the finality rule.

After finality is frozen, the publication worker builds the final registries,
tails, format-8 payloads, and chunks. Before catalog CAS, it must decode and
statefully replay **those exact final bytes** from the same checkpoint—not rely
on the earlier internal candidate—and write:

```text
ReplayValidationReceiptV1 {
  resolution_spec_hash: [u8; 32]
  resolution_published_validation_sha256: [u8; 32]
  resolution_lookahead_validation_sha256: Option<[u8; 32]>
  finality_manifest_sha256: [u8; 32]
  output_stream_manifest_hash: [u8; 32]
  expanded_projection_sha256: [u8; 32]
  input_start_state_sha256: [u8; 32]
  status_key_evidence_sha256: [u8; 32]
  runtime_and_feature_map_sha256: [u8; 32]
  per_slot_validation: ObjectRefV1
  successor_checkpoint: ReplayCheckpointRefV1
  replay_runtime_attachment_manifest: ObjectRefV1
}

ReplaySlotValidationV1 {
  bank_hash: [u8; 32]
  accounts_hash: [u8; 32]
}
```

`expanded_projection_sha256` hashes a domain-separated canonical expansion of
every published slot's resolved slot/parent, ordered components, `num_hashes`,
signature mixin, optional resolved historical status-key class, and exact
signed-message bytes.
`per_slot_validation` is a catalog-readable immutable headerless concatenation
of one fixed 64-byte `ReplaySlotValidationV1` for every `PRODUCED` slot in
produced ordinal order. Its `ObjectRefV1.encoded_len` equals
`produced_count * 64` with overflow checking (zero produced slots means an exact
zero-byte object), and its ordinary SHA-256 covers that exact concatenation. It
has at most 1,048,576 rows / 67,108,864 bytes. Slot/final identities come from
that ordinal's finality entry; execution-core identity comes from the exact
decoded Replay record and
same-ordinal attachment; the runtime-attachment digest is recomputed from that
inline attachment. Those already-bound values are not repeated in the row, so
its only new data is the Bank and accounts roots. The enclosing object remains
protected by its ordinary object SHA-256. The final replay must reproduce every
row and match every overlapping
published-epoch execution/PoH/state field in the accepted resolution validation;
otherwise publication fails. It does not replay descendant lookahead from the
successor state. `resolution_published_validation_sha256` equals the `.sha256`
field of the exact `published_epoch_validation` ObjectRef in the accepted
resolution result. `resolution_lookahead_validation_sha256` is absent/present
exactly with that result and, when present, equals its ObjectRef's `.sha256`
field. `finality_manifest_sha256` likewise equals the proposed finality
ObjectRef's `.sha256` field. These are hashes of stored object bytes, not
SHA-256 of a canonical `ObjectRefV1`. Lookahead remains bound finality
evidence but is not falsely claimed to be derivable from format-8 bytes that do
not contain it. The resolution spec already commits the complete stream/object
evidence set, so the receipt does not add a redundant evidence-set digest.

Runtime attachments are generated or revalidated by this final-byte replay and
key to both the receipt and final Replay generation. The completion manifest
binds the stable resolution references, receipt, and every typed reference
reachable from them. Blockzilla validates schemas, canonical encodings,
lengths, hashes, cross-object equalities, and the full closure before advancing
the Replay catalog; the Hivezilla worker performs the pinned SVM execution.
Blockzilla does not silently become a second replay engine.

`input_start_state_sha256` is
`SHA-256("blockzilla/replay/start-state/v1" || canonical_start_state)` and must
equal the resolution spec's exact start state. `output_stream_manifest_hash` is
the embedded `StreamManifestV1.stream_manifest_sha256` semantic field, verified
with the Hive stream-manifest formula; it is not the output manifest
ObjectRef's stored-byte digest. `status_key_evidence_sha256` equals the `.sha256`
field of the publication job-spec's exact `StatusKeyEvidenceV1` ObjectRef.
`runtime_and_feature_map_sha256` equals the resolution spec descriptor's
`.sha256` field. No receipt field hashes the canonical encoding of an
`ObjectRefV1`. `resolution_spec_hash` and `expanded_projection_sha256` are the
domain-separated semantic hashes defined in this specification, not stored-byte
object hashes. The successor checkpoint has
`state_through_slot = published_slots.next_slot - 1`; its slot/final identities
equal the last produced finality entry and its Bank/accounts roots equal the
last validation row, or all values equal the input Bank state when the epoch has
no produced slot. It is captured before any finality lookahead and contains
only state after `published_slots`: the resulting account state, blockhash queue,
message-hash/historical status-key state, hard-fork/feature state, and all other inputs
required by the next epoch. The next Replay catalog generation must consume
this exact checkpoint reference as its `start_state`, with no alternate snapshot
or numerically adjacent substitute.

### Replay publication and catalog envelope

These outer objects use the canonical encoding, object-reference, descriptor,
job-spec, immutable-object, and exact-CAS rules of the compaction contract. The
explicit product tag prevents an Archive object from decoding as Replay work.

```text
ReplayPublicationJobSpecV1 {
  product_tag: u8                         // REPLAY_V1 = 2
  job_id: [u8; 16]
  cluster_genesis_hash: [u8; 32]
  epoch: u64
  slots: SlotRangeV1
  resolution_spec: ObjectRefV1
  accepted_resolution_job_spec: ObjectRefV1
  accepted_resolution_result: ObjectRefV1
  finality_manifest: ObjectRefV1
  status_key_evidence: ObjectRefV1
  output_stream_manifest: ObjectRefV1
  replay_format: HashedDescriptorV1
  publication_policy: HashedDescriptorV1
  expected_catalog_predecessor: Option<[u8; 32]>
  expected_catalog_generation: u64
  output_namespace: bytes
}

ReplayPublicationAttemptV1 {
  product_tag: u8                         // REPLAY_V1 = 2
  job_id: [u8; 16]
  job_spec: ObjectRefV1
  fence: u64
}

ReplayCandidateManifestV1 {
  product_tag: u8
  job_id: [u8; 16]
  job_spec_hash: [u8; 32]
  fence: u64
  epoch: u64
  slots: SlotRangeV1
  finality_manifest: ObjectRefV1
  output_stream_manifest: ObjectRefV1
  validation_receipt: ObjectRefV1
  successor_checkpoint: ReplayCheckpointRefV1
  produced_count: u32
  skipped_count: u32
  objects: Vec<NamedObjectRefV1>
}

ReplayPublicationResultV1 {
  product_tag: u8
  job_id: [u8; 16]
  job_spec_hash: [u8; 32]
  fence: u64
  outcome: COMPLETE = 1 | NOT_COMPLETE = 2
  candidate_manifest: Option<ObjectRefV1>
}

ReplayCompletionManifestV1 {
  product_tag: u8
  catalog_generation: u64
  catalog_predecessor: Option<[u8; 32]>
  job_id: [u8; 16]
  job_spec_hash: [u8; 32]
  epoch: u64
  slots: SlotRangeV1
  job_spec: ObjectRefV1
  candidate_manifest: ObjectRefV1
  resolution_spec: ObjectRefV1
  accepted_resolution_job_spec: ObjectRefV1
  accepted_resolution_result: ObjectRefV1
  published_finality_manifest: ObjectRefV1
  output_stream_manifest: ObjectRefV1
  validation_receipt: ObjectRefV1
  successor_checkpoint: ReplayCheckpointRefV1
  produced_count: u32
  skipped_count: u32
  objects: Vec<NamedObjectRefV1>
}

ReplayCatalogEntryV1 {
  product_tag: u8
  generation: u64
  predecessor: Option<ObjectRefV1>
  completion_manifest: ObjectRefV1
}

ReplayCatalogHeadV1 {
  product_tag: u8
  generation: u64
  entry: ObjectRefV1
}
```

`ReplayPublicationJobSpecV1` has work kind `REPLAY_PUBLICATION` under the one
global `ActiveFiniteWorkV1` fence. Its exact accepted resolution job-spec/result and proposed finality
must agree exactly, and its output stream manifest is pre-provisioned once for
all retry fences. `publication_policy` pins registry/tail profitability,
chunk framing/compression, attachment instrumentation, and
every output role. An unknown option is unsupported.

The exact immutable stored job-spec bytes and hash are:

```text
ReplayPublicationJobSpecObjectV1 =
  "blockzilla/replay/publication-job/v1" ||
  canonical_encode(ReplayPublicationJobSpecV1)

job_spec_hash = SHA-256(ReplayPublicationJobSpecObjectV1)
```

Before the first grant, Blockzilla stores those exact bytes in a permanently
reader-visible namespace. `ReplayPublicationAttemptV1.job_spec.sha256` equals
`job_spec_hash`, and its length/digest must verify before execution. Retries
change only the attempt fence and reuse that same ObjectRef. Result completeness
and retry rules match the Archive contract. `COMPLETE` has exactly one candidate;
`NOT_COMPLETE` has none. Candidate and completion fields must equal the job-spec, accepted resolution,
finality, final-byte validation receipt, and successor checkpoint exactly.
Counts cover the finality manifest and no `UNRESOLVED` slot can complete.

`objects` is a sorted, unique root set with at most 65,536 registered logical
names. V1 registers chunk roles of the form
`replay.chunk.<first-produced-ordinal-16hex>`, plus
`replay.address-registry` and `replay.previous-blockhash-tail`. The status-key
evidence remains in the accepted resolution namespace and is reached through
`accepted_resolution_result`; it is not registered as a publication output.
The vector contains every output-stream chunk and every required publication
sidecar; receipt, per-slot validation, runtime-attachment
manifest/objects, and successor-checkpoint references form a typed transitive
closure. Every referenced byte must be reachable by following these schemas;
bucket listing is never discovery.

Blockzilla accepts only the current
`(REPLAY_PUBLICATION, job_id, fence)`, structurally decodes the final bytes,
recomputes their content identities, validates the worker's replay receipt and
full closure, makes every direct completion reference catalog-readable, and
creates the completion. It does not independently execute SVM. Its sorted
object vector is byte-for-byte the candidate's.
The semantic entry digest is:

```text
SHA-256("blockzilla/replay/catalog-entry/v1" ||
       canonical_encode(ReplayCatalogEntryV1))
```

The separately provisioned Replay head advances by the same linearizable exact
CAS and lost-response rules as Archive V2. Generation zero is either the epoch
containing genesis with `start_state = Genesis`, or an explicitly declared
activation epoch with a verified predecessor checkpoint. Later generations are
gap-free consecutive epochs, and their resolution spec's `start_state` must
equal the prior committed completion's `successor_checkpoint` byte for byte.
The predecessor entry/completion, format, cluster, and epoch schedule must also
match. Candidate objects, bucket listings, and unreferenced completions are
never canonical.

### Independent Replay recovery copy

Replay has its own recovery chain; Archive recovery does not implicitly absorb
Replay provenance. It reuses `RecoveryObjectMappingV1` and the conditional
create/readback/CAS rules in the compaction contract with these product-tagged
roots:

```text
ReplayRecoveryReceiptV1 {
  product_tag: u8                         // REPLAY_V1 = 2
  catalog_generation: u64
  catalog_entry: ObjectRefV1
  recovery_target_id: [u8; 16]
  recovery_failure_domain_id: [u8; 16]
  recovery_target: HashedDescriptorV1
  previous_receipt: Option<ObjectRefV1>
  objects: Vec<RecoveryObjectMappingV1>
}

ReplayRecoveryCheckpointV1 {
  product_tag: u8
  recovery_target_id: [u8; 16]
  catalog_head: ReplayCatalogHeadV1
  latest_receipt: ObjectRefV1
}
```

The sorted mapping covers the complete typed closure: catalog entry,
completion, publication job-spec/candidate, resolution spec/job-spec/result, published
finality and both resolution-validation objects, stream manifest/chunks,
registries/sidecars, final-byte receipt and per-slot rows, status-key evidence,
runtime attachment manifest/chunks, and successor checkpoint manifest/chunks. Every canonical
length and SHA-256 must match after independent readback. Discovery starts only
from exact CAS key
`blockzilla-replay-recovery/v1/<target-id-hex>/head.checkpoint`; receipt keys,
generation/predecessor continuity, lost-response handling, authority limits,
and failure-domain checks are identical to Archive recovery. Missing closure or
head continuity is degraded Replay recovery and alerts without changing either
catalog.

The shared recovery bound is 131,072 mappings and 536,870,912 encoded receipt
bytes. A larger Replay closure is unsupported by V1 recovery and cannot be
reported protected through a partial mapping.

An Archive generation that cites `ReplayArchiveDependencyV1` copies only its
own Archive serving closure. During recovery/provenance audit, the reader also
resolves the cited Replay entry through this separately recovered Replay head
and verifies the dependency again. It never treats an Archive copy as proof
that the Replay checkpoint or validation evidence survived.

Failure is not represented as an empty slot or partial Replay V1 record. The
slot remains raw evidence and the generation is incomplete until repaired or
explicitly unresolved. Different complete block IDs at one slot remain separate
candidates until era-appropriate finality selects one. Candidate encoding alone
is never canonical publication.

### Historical backfill evidence

Live Replay production uses verified permanent shreds. Replaying the whole chain
also requires onboarding slots from before Hivezilla shred capture. V1 permits a
digest-bound historical signed-entry corpus only when its descriptor and audit
prove complete ordered slots, parents, entry boundaries, `num_hashes`, recorded
entry hashes, full outer signatures, exact signed messages, and every marker
defined in that era. The producer verifies signatures, recomputes signature
mixins and PoH, and binds the immutable corpus object/coverage/finality evidence
exactly as it would bind shreds.

Such a CAR, validator ledger, or genesis archive is permanent historical
evidence but does not retroactively prove UDP/FEC/shred provenance it does not
contain. Missing signatures, entry boundaries, marker bytes, or authoritative
fork coverage makes that range unsupported rather than guessed. The first
activation checklist must prove continuous coverage from genesis (using these
historical inputs) or declare an explicit `activation_epoch` whose first slot is
the Replay catalog's first published slot, plus the verified predecessor
checkpoint. V1 has no partial bootstrap generation: an arbitrary mid-epoch
activation slot is invalid. The deployment may not claim whole-chain replay
while silently starting at the live-capture boundary.

## 7. Replay inputs outside this format

Replay V1 is the ordered ledger execution input, not a complete virtual machine
snapshot. Deterministic replay additionally requires:

- exact genesis configuration or a verified state checkpoint at the effective
  parent of the first replay record;
- all account state, owners, executable flags, program data, address lookup
  tables, sysvars, stake/vote state, and recent-blockhash state at that boundary;
- all consensus-relevant Bank state not reducible to accounts, including the
  blockhash queue, exact era-defined message-hash and historical 20-byte
  status-key classes plus bound origin-evidence coordinates needed for
  already-processed detection and subsequent class matching, hard forks,
  counters, capitalization, epoch stakes, timing, and parent/Bank/account
  hashes; fake RPC signature identity is excluded;
- a slot-range mapping to the exact compatible Agave/SVM implementation,
  feature set, builtin/precompile configuration, cost model, and account-hash
  rules; and
- an instrumentation policy defining which runtime attachments to collect.

These inputs are immutable objects referenced by a replay job and hashed in its
descriptor. A checkpoint is accepted only if its cluster,
`state_through_slot`, `bank_slot`, final PoH hash, era-defined optional consensus
block ID, Bank/account hashes, object digest, and predecessor generation verify.
Its Bank slot and both identities must equal the exact outside-generation
predecessor-parent anchor used by the first Replay record; numerical adjacency
alone is insufficient across skipped slots or forks. “Replay an isolated epoch”
without such a checkpoint is invalid; replay from genesis uses the explicit
`ReplayStartStateV1::Genesis` object instead. A `ReplayCheckpointRefV1` is not
advertised as a stock Agave snapshot unless real signatures were rehydrated and
the complete native status cache was rebuilt from raw evidence.

For every live historical status-key class, the checkpoint's origin-evidence
coordinate includes the selected-lineage block identity and canonical ledger
position plus either an exact
`RawRecordRefV1`/protected stream anchor or an immutable signed-corpus
`ObjectRefV1` and decoder descriptor. Projection may therefore rehydrate only
the one first signature needed for equality checking without storing that value
in Replay V1 or trusting a mutable lookup.

The replay engine is expected to regenerate, subject to the pinned runtime and
instrumentation policy:

- transaction result/error, fee, compute use, loaded addresses, return data,
  logs, and inner instructions;
- pre/post lamport and token balances used by Compact V2; full account writes
  still advance replay state but are not a mandatory Replay V1 attachment;
- token-balance projections derived from those states;
- runtime/epoch rewards when all consensus inputs are present; and
- Bank block height, Bank/account hashes used as replay checkpoints, and an
  era-pinned canonical block-time projection from the replayed vote/timestamp
  state or state-changing block marker.

It cannot regenerate omitted transaction signatures or exact signature-indexed
RPC identity. Nor does it recreate packet arrival order, provider commitment
timing, source-specific JSON/protobuf presentation, or raw shred/FEC evidence.
Its block-time value is the result of the declared historical derivation
algorithm; a provider's differently configured estimate is comparison evidence,
not silently canonicalized output.
Replay outputs attach to the Replay V1 generation plus checkpoint, runtime, and
instrumentation digests; they never mutate the ledger projection.

Transaction-level attachments use structural identity
`(slot, transaction_ordinal)` plus the recomputed signed-message hash, where the
ordinal is the zero-based flattening of component, entry, and transaction order
inside that slot. Compact V2 may join those attachments with signatures and
transaction IDs re-extracted from bound raw shreds or an order-preserving signed
block source. It then maps the flat ordinal back through Replay entry counts and
recomputes every signature mixin and PoH boundary. Placeholder replay signatures
are never join keys or published data.

## 8. Hivezilla migration boundary

Hivezilla still needs both raw shreds and raw gRPC blocks: shreds provide the
permanent ledger/FEC evidence, while gRPC supplies runtime results until replay
can regenerate and validate them. A deployment may also still use an external
finality authority until marker/certificate resolution is independently proven.
These inputs never share capture identity, cursor, custody ACK, or deletion
state. Comparison starts only after ledger-core equality; runtime metadata is a
separate attachment where missing is not empty.

The first epoch cutover changes only provider-versus-final-Replay attachment
selection; raw signed-shred evidence and the selected finality authority remain.
Complete gRPC capture continues through its fixed rollback horizon before
explicit irreversible production retirement, and a sampled canary is never
rollback input. Retirement stops capture and allows ACK-gated source-spool
cleanup; the protected terminal raw-gRPC prefix remains discoverable audit
evidence but is ineligible for rollback.

External-to-marker finality is a separate future-epoch policy migration. The
marker/certificate resolver is shadowed against the external authority over
forks, skips, repairs, and descendant lookahead before the immutable finality
rule changes. A separate rollback horizon may retain the external feed as an
eligible authority; after explicit demotion/retirement, retained evidence is
audit-only. If shred evidence cannot independently settle every slot, this
cutover does not occur and the deployment remains shred-primary.

The target is shred-only block-content, finality, and runtime derivation, with
raw shreds retained permanently. **Shred-only** means that verified
shreds/markers plus the pinned genesis, protocol, feature, and checkpoint state
are the sole continuing inputs. If any external RPC, gRPC, Tower/status, or
provider finality feed remains required, the deployment is **shred-primary**.
The exact product split, current/target flows, and cutover gates live in
[Hivezilla node roles](../architecture/hivezilla-node-roles.md#shreds-grpc-replay-v1-and-compact-v2)
and [Gate 5R](../architecture/hivezilla-v1-implementation-plan.md#gate-5r--replay-v1-and-deterministic-runtime-regeneration).

## 9. Conformance requirements

Before payload ID 8 becomes executable, golden and integration fixtures must
prove at least:

1. compact Legacy and V0 messages expand to the exact original signed-message
   bytes, canonical raw messages are unchanged, and Legacy/V0 raw encodings are
   rejected;
2. every original signed envelope passes its era's version, serialized-size,
   signature-count, header, and index sanitation before all signatures verify
   and are removed;
3. signature mixins reproduce every original entry and final PoH boundary, while
   adversarial `num_hashes` totals fail before expensive work;
4. historical status-key refs preserve exact recent-blockhash-scoped 20-byte
   slice equivalence for unique, repeated, colliding, failed, evicted,
   current-generation, and checkpoint-prior cases; known-offset and all-offset
   proof paths match a launch runtime, while modern message-hash admission uses
   no per-transaction status backreference;
5. `StatusKeyEvidenceV1` is byte-deterministic, verifies all live/start/future
   keys through cohort eviction, installs known native offsets, and rejects an
   unknown-offset collision or over-bound live working set;
6. ordered marker bytes round-trip and marker-era state transitions match pinned
   Agave behavior;
7. finality entries form one linear produced chain; a parent jump, sibling-fork
   class origin, changed checkpoint anchor, or lookahead state leaking into the
   successor checkpoint fails;
8. replay payload bytes, registry order, profitable raw fallbacks/tails, and
   IDs are deterministic across processes, worker counts, input arrival orders,
   hash seeds, and independently provisioned stream IDs; full `RecordV1` bytes
   are identical only for retries using the same pre-provisioned stream
   manifest because the prefix chain intentionally includes `stream_id`;
9. no runtime-only field changes Replay V1 bytes or registry IDs;
10. corrupt IDs, counts, indexes, marker/message versions, and trailing
   bytes fail before allocation or execution;
11. raw-evidence/finality/registry/status mismatch, or a checkpoint
    input/migration/successor-policy mismatch, prevents publication;
12. repeated replay from the same checkpoint produces identical Bank/account
   hashes, packed normalized attachments, and successor checkpoint;
13. an outer-transaction-signature-stripped projection can never authorize
    raw-source deletion or claim independent transaction authentication;
14. every format-wide and aggregate limit is enforced before
    allocation/decompression, a job descriptor can narrow but never widen it,
    and a complete historical-corpus scan proves every intended activation
    range fits those format limits;
15. the validation receipt proves that the exact final format-8 bytes, not only
    a pre-finality candidate, produced the accepted PoH/Bank/account hashes and
    runtime attachments; and
16. resolution/publication retries reuse the exact immutable fence-free job-spec
    objects, stale fences fail, and Replay-catalog CAS (including a lost success
    response) is idempotent while preserving identical logical Replay bytes; and
17. loss of the online Replay head/objects recovers only through the exact
    product-tagged recovery checkpoint and complete mapped closure; listing,
    an Archive-only copy, or a partial checkpoint mapping never promotes data;
    and
18. range fixtures reject unequal starts, non-nested endpoints,
    spec/manifest/status-evidence mismatch, missing or extra
    `lookahead_finality_validation`, finality checks over the status-only suffix,
    and either suffix entering Replay records or successor checkpoint state.
