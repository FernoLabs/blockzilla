# Blockzilla V1 minimal block candidate

Status: **draft internal normalization contract; not a HiveSync payload**.

A Hivezilla compact worker replays already-custodied raw streams into one
source-neutral block fragment for a Blockzilla-scheduled job. The fragment is
derived and reproducible, so it has no independent WAL, custody ACK, or
distributed protocol in V1. Blockzilla remains the canonical catalog authority;
running this conversion or uploading an archive object does not grant a worker
authority to commit canonical history.

The centralized job, fencing, finality, epoch-local ID, result, and publication
contract is the
[Blockzilla V1 compaction job protocol](blockzilla-compaction-job-v1.md).

## Schema

```text
BlockCandidateV1 {
  slot: u64
  parent_slot: u64
  final_poh_hash: [u8; 32]
  consensus_block_id: Option<[u8; 32]>

  parent_final_poh_hash: Option<[u8; 32]>
  parent_consensus_block_id: Option<[u8; 32]>
  transactions: Option<TransactionsV1>
  poh_entries: Option<Vec<PohEntryV1>>
  block_components: Option<Vec<BlockComponentLayoutV1>>
}

TransactionsV1 {
  entries: Vec<SignedTransactionEnvelopeV1>
}

SignedTransactionEnvelopeV1 {
  signatures: Vec<[u8; 64]>
  signed_message_bytes: bytes
}

RuntimeAttachmentSetV1 {
  execution_core_digest: [u8; 32]
  transaction_count: u32
  transaction_metadata: Option<Vec<NoRegistryMetaV1>>
  rewards: Option<RewardsV1>
  block_time: Option<i64>
  block_height: Option<u64>
}

RuntimeAttachmentManifestV1 {
  cluster_genesis_hash: [u8; 32]
  epoch: u64
  slots: SlotRangeV1
  finality_manifest_sha256: [u8; 32]
  execution_generation_sha256: [u8; 32]
  producer_kind: u8                    // PROVIDER_OBSERVATION=1 | FINAL_REPLAY=2
  producer_provenance: HashedDescriptorV1
  normalization_start_state_sha256: [u8; 32]
  runtime_and_feature_map_sha256: [u8; 32]
  instrumentation: HashedDescriptorV1
  entry_count: u32
  chunks: Vec<RuntimeAttachmentChunkRefV1>
}

FinalReplayProducerProvenanceV1 {
  resolution_spec_hash: [u8; 32]
  output_stream_manifest_hash: [u8; 32]
  expanded_projection_sha256: [u8; 32]
  status_key_evidence_sha256: [u8; 32]
}

RuntimeAttachmentChunkRefV1 {
  first_produced_ordinal: u32
  entry_count: u32
  chunk: ObjectRefV1
}

RuntimeAttachmentChunkV1 {
  entries: Vec<RuntimeAttachmentSetV1>
}

RuntimeParityManifestV1 {
  left_attachment_manifest: ObjectRefV1
  right_attachment_manifest: ObjectRefV1
  normalization: HashedDescriptorV1
  entry_count: u32
  left_normalized_stream_sha256: [u8; 32]
  right_normalized_stream_sha256: [u8; 32]
  unexplained_mismatch_count: u64
}

RewardsV1 {
  num_partitions: Option<u64>
  entries: Vec<NoRegistryRewardV1>
}

PohEntryV1 {
  num_hashes: u64
  hash: [u8; 32]
  tx_count: u32
}

BlockComponentLayoutV1 =
    EntryBatch {
      entries_through: u32,
      data_shreds_through: u32
    }
  | BlockMarker {
      bytes: bytes,
      data_shreds_through: u32
    }
```

`BlockCandidateV1` remains an in-process structural semantic value. The runtime attachment,
attachment-manifest and parity-manifest structures are immutable
protocol objects because Replay and Archive jobs reference them by
`ObjectRefV1`. Their wrappers use the canonical field, integer, vector, option,
and rejection rules from the compaction contract. Each producer/normalization
descriptor pins the exact bounded codec for the nested `NoRegistry*` values;
there is no ambient local-default codec. Golden fixtures are required before
these objects become executable inputs.

`None` means missing; `Some([])` means verified empty. Candidate transactions
may be verified empty. Present PoH vectors must be non-empty; a present
component-layout vector is also non-empty and contains at least one entry batch.
Runtime fields never live in `BlockCandidateV1`; they use immutable
attachment sets so provider-observed and replay-generated values can coexist.

The target Rust API constructs `BlockCandidatePartsV1` and passes it through
`BlockCandidateV1::new`; candidate fields are private and that constructor must
enforce the source-neutral structural rules below. It does not replace an
adapter's source-specific completeness/proof checks. The trust stages are
explicit and must not collapse into one self-asserted capability bitset:

```text
BlockCandidateV1 (structural)
  -> source-promoted candidate + evidence/policy receipt
  -> finality-selected candidate
  -> Replay/Archive projection
```

The structural value has no canonical wire identity and cannot be published.
A promotion receipt binds a semantic candidate hash, exact evidence references,
and the pinned source, era, trust, and verifier descriptors. Finality then binds
the exact selected identities and predecessor. The concrete promotion-receipt
wire is frozen with the product job protocol, not inferred from this Rust type.

An attachment's `transaction_count` equals the selected ledger core's exact
transaction count. When `transaction_metadata` is present, it has exactly that
many rows in ledger order. `None` remains missing and `Some([])` is valid only
when the bound core has zero transactions. The manifest's one shared producer
descriptor identifies the provider observation or exact Replay
stream/expanded-projection input; the manifest's adjacent fields bind its
checkpoint, runtime, and instrumentation policy. None is inferred from object
location. Producer provenance is stored once in the manifest, not repeated in
every attachment row. The descriptor must not name the later validation receipt,
which would create a content-address cycle: the receipt instead binds the
completed attachment manifest.

For `FINAL_REPLAY`, `producer_provenance.bytes` is exactly the canonical encoding
of `FinalReplayProducerProvenanceV1`; its four values equal the accepted
resolution's semantic spec hash, the embedded Hive stream-manifest hash, the
domain-separated expanded-projection hash, and the status-evidence ObjectRef's
stored-byte SHA-256. The surrounding attachment manifest already binds cluster,
epoch/range, finality, execution generation, start state, runtime/feature map,
and instrumentation, so those values are not repeated in this descriptor. Its
preimage explicitly excludes attachment rows/chunks/manifests, parity objects,
validation receipts, successor checkpoints, candidates, completions, catalogs,
attempt fences, and physical keys. A final-replay producer identity is therefore
known before attachment publication and cannot contain a backward hash edge.

Candidate transactions deliberately do not alias Archive V2 transaction
structures. Each signature has already passed the source adapter's fixed
64-byte structural check, while `signed_message_bytes` is the exact Solana
message serialization covered by those signatures. Promotion parses and
sanitizes those bytes under the pinned era, verifies every signature, and proves
that message reserialization is byte-identical. It also reconstructs the full
canonical transaction wire, including the signature-count short vector.
Sources that expose original transaction wire bytes, including shreds and
lossless CAR inputs, must byte-compare that reconstruction and reject
non-minimal short vectors, count mismatches, and trailing bytes. Yellowstone's
structured protobuf does not expose the original framing; its adapter instead
canonically serializes the supplied message and promotion verifies every
supplied signature over exactly those bytes. It must not claim original-wire
byte equality. The split is a minimal semantic envelope, not a claim that the
original transaction framing was stored twice. This admits future message
versions without weakening source-specific evidence; unsupported versions still
fail the selected Replay or Archive policy.

The currently pinned `yellowstone-grpc-proto` 12.4 schema has only a
`versioned` boolean. It cannot retain Agave's V1-only transaction-config field,
so a V0 message with no address-table lookups is structurally ambiguous with a
newer V1 message after decode/re-encode, and a future version could overlap a
different retained shape. The V1 adapter interprets any `versioned = true` row
as V0 only when signature zero verifies over the reconstructed V0 message bytes;
otherwise it fails closed. This is schema disambiguation, not a substitute for
promotion's complete signature and sanitation policy. The
known-schema raw gRPC WAL cannot recover a dropped V1 field, so a schema that
retains an explicit message version/config must create a new raw stream version
before V1 observations are admitted.

The runtime-only aliases are typed inner values from `blockzilla-format`:

```text
NoRegistryMetaV1   = WincodeArchiveV2NoRegistryMeta
NoRegistryRewardV1 = WincodeArchiveV2NoRegistryReward
PohEntryV1         = CompactPohEntry
```

The candidate/attachment contracts do not use `WincodeArchiveV2Payload` or the
current rewards wrapper: source lengths, raw fallbacks, and decode-error strings
stay with raw evidence and diagnostics.

Inside metadata, logs use one canonical `Compact` representation;
`WincodeZstd` and alternative raw encodings are not candidate values. This
keeps equality semantic rather than dependent on an adapter's compression
choice.

An attachment is created only after a complete selected ledger core exists.
Two equality domains remain distinct:

- **signed-ledger equality** includes cluster, slot, both block and parent
  identities, ordered full signed transaction wire bytes, PoH entries, and
  ordered component/marker layout; it is required before two full raw sources
  are considered the same ledger candidate; and
- `execution_core_digest` is the exact canonical
  `ExecutionCoreSlotV1` domain/hash from the
  [Replay specification](blockzilla-replay-projection-v1.md#canonical-execution-core-expansion):
  it contains cluster, resolved block/parent identities, ordered
  components/entries, `num_hashes`, signature mixin, optional historical
  status-key class, and exact signed-message bytes, but no outer signature value.

The second domain is exactly reproducible from Replay V1 and is the identity to
which runtime attachments bind. A full signed source derives it only after the
same epoch/checkpoint-aware class mapping and Replay normalization pass. Shred
boundaries and source provenance are outside both semantic identities. The
compaction protocol freezes the literal encodings and golden fixtures before
attachments become executable inputs.

Attachment chunk objects are immutable and multi-valued. A gRPC observation and
a final-byte Replay run produce separate manifests even when normalized bytes
match. `RuntimeParityManifestV1` compares their complete same-ordinal attachment
streams under one pinned normalizer; it never overwrites either. Zero unexplained
mismatches is a cutover gate, not permission to discard the provider baseline or
raw evidence.

An attachment manifest's canonical chunks contain exactly one row for every
produced slot covered by its declared ledger generation, in produced ordinal
order; it has no skipped-slot row. Slot is resolved from that ordinal in the
exact finality manifest and is not repeated. The semantic attachment digest is
`SHA-256("blockzilla/runtime-attachment/v1" || canonical_attachment)`; it is
recomputed from the inline value rather than copied beside it. The decoded
attachment carries the execution-core digest, while the parent manifest supplies
the one producer identity. Both producer kinds bind the same semantic
`normalization_start_state_sha256`: provider bytes still need the exact
checkpoint/status policy to normalize their signature-bearing ledger core.
`producer_provenance` separately identifies raw provider capture or the exact
attempt-neutral Replay input above. It is `FINAL_REPLAY` only for attachments
produced by executing the exact published format-8 bytes.

The manifest hash fields have one meaning each:

- `finality_manifest_sha256` equals the `.sha256` field of the exact bound
  `FinalityManifestV1` ObjectRef;
- `normalization_start_state_sha256` is
  `SHA-256("blockzilla/replay/start-state/v1" || canonical_start_state)`;
- `runtime_and_feature_map_sha256` equals the exact bound
  `HashedDescriptorV1.sha256`; and
- `execution_generation_sha256` is the semantic domain hash below, not an
  ObjectRef digest.

`execution_generation_sha256` is exactly:

```text
SHA-256(
  "blockzilla/execution-generation/v1" ||
  cluster_genesis_hash || epoch_be_u64 || canonical_encode(slots) ||
  finality_manifest_sha256 ||
  canonical_encode(ordered (slot, execution_core_digest) rows)
)
```

The preimage excludes attachment bytes/references, chunks, parity, receipts,
completion manifests, catalog entries, and physical keys, so a later receipt
can bind this manifest without a content-address cycle. It is not an
object-store locator.

A parity manifest has `entry_count` equal to both attachment manifests and
therefore covers their complete ordered produced-slot set.
`left_attachment_manifest` and `right_attachment_manifest` must be different
ObjectRefs: left decodes as `PROVIDER_OBSERVATION`, right as `FINAL_REPLAY`, and
their producer provenance must be distinct. A self-compare is invalid even if
its mismatch count is zero. At every produced ordinal `n`, the verifier resolves
both attachment values through their canonical chunk ranges and the `n`th
`PRODUCED` finality entry. Their execution-core digests, transaction counts, and
resolved slots must agree.

The normalization descriptor emits one bounded, canonical runtime-only byte
string per attachment and defines a deterministic semantic-leaf mismatch count.
It excludes core/provenance values already checked above and preserves missing
versus verified-empty unless an explicit registered normalization says otherwise.
Each side's generation-level hash is exactly:

```text
SHA-256(
  "blockzilla/runtime-attachment-normalized-stream/v1" ||
  normalization.sha256 || entry_count_be_u32 ||
  repeated(
    produced_ordinal_be_u32 ||
    normalized_len_be_u64 || normalized_bytes
  )
)
```

The hash is streamed in ordinal order; normalized values need not coexist in
memory. `unexplained_mismatch_count` is the overflow-checked sum of the
descriptor-defined leaf mismatch counts at every ordinal. It is zero if and only
if every pair of exact normalized semantic values is equal; a zero-mismatch gate
also requires `left_normalized_stream_sha256` to equal
`right_normalized_stream_sha256`. Per-slot mismatch details are non-canonical
diagnostics/alerts and can be regenerated from the retained attachments. No
parity row or parity chunk is stored.

Chunking is canonical and bounded. Rows follow produced-slot order. Each chunk
starts at its declared produced ordinal and closes before adding a row that
would exceed 8,192 rows or 268,435,456 encoded bytes; all non-final chunks must
be maximal under that rule. The byte bound is the complete canonical
`RuntimeAttachmentChunkV1` encoding, including its vector length. Every chunk
reference has `1 <= entry_count <= 8,192`, its `entry_count` equals the decoded
`entries.len()`, and its `first_produced_ordinal` equals the sum of all preceding
chunk counts. The first chunk therefore starts at ordinal zero. `chunks` is
empty exactly when manifest `entry_count` is zero; otherwise it is non-empty,
and empty chunks—including an appended empty final chunk—are invalid. One
attachment is at most 67,108,864 encoded bytes;
all attachments in a generation total at most 1,099,511,627,776 bytes. An
attachment manifest has at most 16,384 chunks and 134,217,728 encoded bytes; a
parity manifest has at most 2,097,152 encoded bytes. Normalized output for one
attachment is at most 67,108,864 bytes and all normalized outputs on one side
total at most 1,099,511,627,776 bytes. Manifest `entry_count` is at most
1,048,576; an attachment manifest count equals the sum of chunk counts. Every
chunk reference, attachment, normalized length, and aggregate is checked before
allocation. The exact manifest and attachment-chunk ObjectRefs are the discovery
path; object listing is never part of their meaning.

## Adapter contract

`R` is required, `O` may be legitimately absent, `E` is required exactly when
the pinned protocol era defines the field, and `-` is unsupported:

| Section | Yellowstone gRPC block | Full RPC block | Complete shred block |
| --- | :---: | :---: | :---: |
| parent slot and final PoH hash | R | R | R |
| consensus/shred block ID | - | - | E |
| parent final PoH hash, except genesis | R | R | R |
| parent consensus block ID, except genesis | - | - | E |
| transactions | R | R | R |
| runtime attachment: transaction metadata | R | R | - |
| runtime attachment: rewards | O | O | - |
| PoH entries | R | - | R |
| ordered component/marker layout with cumulative data-shred ends | - | - | R |
| runtime attachment: block time / height | O | O | - |

Every present section is complete. A missing or invalid `R` section, or invalid
bytes for an `O` section that was actually supplied, rejects the whole
adapter output. Yellowstone/RPC adapters emit a ledger candidate plus a separate
runtime attachment; shred adapters emit only the ledger candidate. Raw custody
remains available for replay/repair; an adapter must not silently convert
failure into ordinary absence.

Old Faithful CAR is a separate finite-object adapter, not the first table
column. It must consume the CID-ordering `LosslessCarBlock` path, after verifying
the exact CAR/CAR.ZST ObjectRef, bounded decompression, header/root, complete
DAG/CID topology, and EOF. It may supply exact Legacy/V0 transaction wires and
ordered non-empty PoH entries. It cannot independently supply the selected
parent final-PoH hash, finality/skipped-slot coverage, consensus IDs, or exact
marker/component geometry; the epoch assembler must bind the parent identity
from the selected predecessor/anchor, and the era policy must explicitly permit
every absent extension. Empty-entry blocks and eras requiring information the
CAR does not prove are unsupported, never repaired by guessing. Physical CAR
node order, filenames, and arithmetic block IDs are not ledger authority.

Before emission, an adapter proves:

- when source transaction or entry indexes exist, they are the unique sequence
  `0..len`; otherwise source-defined array order is retained;
- Yellowstone `starting_transaction_index`, when present, equals the prefix sum
  of prior PoH `tx_count` values;
- when transactions and PoH are both present,
  `sum(poh_entry.tx_count) == transactions.entries.len()`;
- when component layout and PoH are present, each `EntryBatch.entries_through`
  is a positive, strictly increasing, end-exclusive cumulative PoH-entry count;
  the last entry-batch boundary equals `poh_entries.len()`, markers retain exact
  source order and canonical typed bytes, every component's
  `data_shreds_through` is a positive strictly increasing end-exclusive count,
  and the layout expands to the original one-completed-shred-range-per-component
  sequence; Agave-compatible source padding remains only in raw evidence;
- final PoH entry hash equals `final_poh_hash`;
- all hashes decode to exactly 32 bytes;
- every transaction signature is exactly 64 bytes and its count equals the
  message header's required-signature count; a gRPC transaction's top-level
  signature is also exactly 64 bytes and equals signed-transaction signature
  zero; structured gRPC input is encoded to the canonical Legacy/V0
  signed-message bytes, while sources carrying original wire bytes additionally
  require exact decode/re-encode byte equality;
- a supplied parent final PoH hash is source- or selected-parent-derived, never
  synthesized from a consensus block ID; and
- slot zero canonically has `parent_slot = 0`,
  `parent_final_poh_hash = None`, and
  `parent_consensus_block_id = None`; and
- except for genesis, `parent_slot < slot`; publication later verifies that
  both parent identities match the finalized candidate at that exact slot.

The implemented live Yellowstone adapter requires real prefix-sum
`starting_transaction_index` values and is therefore an explicit Solana 1.18+
schema policy. Historical 1.17 plugins populate that field as all zero; a future
historical adapter must select a pinned legacy-all-zero policy and derive order
from the declared entry transaction counts. It must not switch policies by
inspecting whether one block happens to contain zeros.

For shreds, `parent_slot` and `parent_consensus_block_id` are the final values
after ordered `BlockHeader` / `UpdateParent` markers and chained-shred checks.
`parent_final_poh_hash` is then resolved from that exact selected parent
candidate or the immutable outside-range parent anchor; it is not the marker's
block ID. Missing or conflicting required markers/parents reject the candidate.
A shred candidate is emitted only after complete ordered entries derive
`final_poh_hash` and, in an era that defines one, the exact terminal shred
commitment derives `consensus_block_id`. In earlier eras that option is absent
and final PoH is the explicit fork identity; an implementation never fabricates
a block ID. Partial or unidentified assemblies remain raw.

Before shred-derived bytes enter this trusted candidate, the adapter also
applies the job-pinned shred-promotion gate: resolve the scheduled leader,
verify the applicable leader/retransmitter signature rule, verify Merkle/proof
and FEC-set root consistency for original, repaired, and recovered shreds, and
partition conflicting FEC chains. Raw capture remains unfiltered; evidence that
fails promotion stays raw and cannot contribute to a candidate.

Slot zero is the sole leader-schedule exception. Its candidate must use the
canonical parent values above. Its deterministic tick/entry sequence, PoH
start/final hash, and shred parameters must match the job-pinned construction
from the digest-bound `genesis.bin`; its arbitrary construction signer is not
authority. Exact shred bytes are required only when the complete genesis
archive/blockstore is itself digest-bound.

The component layout avoids storing PoH entries twice or maintaining a second
parallel shred-boundary vector. `EntryBatch.entries_through` slices the flat
`poh_entries` vector from the previous entry-batch boundary. Every component's
`data_shreds_through` names the end-exclusive cumulative data-shred count for
the exact completed range that encoded it; coding shreds are excluded. A marker
therefore advances data shreds without falsely advancing entries, including a
trailing footer after the final entry batch. `BlockMarker.bytes` stores the exact
canonical `VersionedBlockMarker` bytes specified by Replay V1. Stateful
footer/certificate/clock validation waits for runtime
replay, while decode/re-encode, ordering, parent identity, and other
state-independent checks are candidate-promotion requirements.

## Comparison and joins

The candidate store is bound to one `cluster_genesis_hash`. Within that
namespace `(slot, final_poh_hash)` groups observations for comparison, but it is
not a complete identity: shred-backed variants are additionally partitioned by
`consensus_block_id`.

V1 has no generic cross-source candidate merge. Missing sections from one
source are never filled with sections from another source to create a
Frankenstein observation. Exact duplicate suppression and fragment assembly are
allowed only within one immutable source/evidence identity under an
adapter-specific rule. Every admitted source observation must independently
supply all fields required by its product/era policy.

The implementation exposes two deliberately different comparisons:

- structural identity compares every value and every `Option` presence bit; a
  store may use it for deduplication only after the immutable evidence identity
  also matches; and
- pairwise compatibility treats one missing source extension as compatible with
  a present value, but treats two different present values as a conflict.

Compatibility is not transitive. An ID-less/marker-less gRPC observation may be
compatible with each of two shred observations whose present marker bytes or
consensus IDs conflict with one another. It creates only pairwise join edges;
implementations must never union a connected compatibility graph.

Signed-entry equality is decided before attaching provider runtime data:
slot and parent slot, observed parent final PoH when supplied, final PoH,
ordered PoH entries, transaction order, fixed outer signatures, and exact signed
message bytes must match. Shred extensions are compared only between sources
that independently provide them; gRPC or CAR cannot manufacture a component
layout, marker, data-shred geometry, or consensus ID. After finality selects an exact
candidate, the same Replay normalization derives `execution_core_digest`; only
then may a complete gRPC-produced `RuntimeAttachmentSetV1` bind to it. A
Replay-produced attachment can
compare at the execution-core boundary before outer signatures are rejoined,
but it cannot establish signed-ledger equality. Attachments never merge into
each other or into `BlockCandidateV1`; missing remains different from verified
empty, and distinct producer/runtime provenance remains visible even when
normalized values match.
A Compact V2 job selects one attachment set, or a policy-defined normalized
combination, by immutable reference and may require a zero-mismatch parity
manifest covering the whole produced-slot set.

[Replay V1](blockzilla-replay-projection-v1.md) is a separate derived execution
projection, not another candidate transaction encoding. Because it omits outer
signatures, its message equality can validate the signed-message core but cannot
replace the full transaction body or supply transaction IDs. Any Compact V2
signature fields still come from retained signed raw evidence, never from
placeholder signatures used by the replay adapter.

Different final PoH hashes at one slot remain ledger forks. The same final PoH
hash under different consensus block IDs remains distinct shred evidence; an
ID-less gRPC/RPC candidate or attachment may bind to a shred candidate only
after exact signed-ledger equality and cannot choose between multiple compatible
block IDs.
A slot-only status is not enough to choose either identity: the compaction job's
immutable finality manifest binds the finalized PoH hash and, whenever shred
evidence or Replay V1 is selected, the exact consensus block ID. The active
Hivezilla worker applies only the policy frozen in that job. V1 assigns
zero-based dense IDs to ascending produced slots within each complete epoch
generation; the worker materializes that fixed rule while building objects.
Blockzilla alone conditionally commits the result as canonical history.

## Provenance

Stream-backed provenance is stored once outside the candidate:

```text
RawRecordRefV1 {
  stream_id: [u8; 16]
  sequence: u64
  prefix_hash: [u8; 32]  // P(sequence + 1)
}
```

An evidence-set manifest sorts unique refs by `(stream_id, sequence)`, encodes
`count_be_u32` followed by each fixed-width ref, and hashes
`"blockzilla/evidence-set/v1" || encoded_manifest` with SHA-256. Large shred
sets reference that one manifest rather than repeating provenance per field.
Content-addressed finite inputs such as CAR/CAR.ZST are bound once by the
compaction job's `required_object_inputs`; their object reference and decoder
descriptor are provenance and are not rewritten as fake stream records.

Normalization progress never acknowledges raw custody or authorizes deletion.

## Canonical publication gate

Before uploading a proposed Archive V2 result, the compact worker additionally
requires:

1. the real Yellowstone parent final PoH hash, never an archive ID or consensus
   block ID transformed arithmetically;
2. validated PoH order, counts, ranges, and final hash;
3. absent PoH/shredding to remain absent, never an empty sidecar;
4. the exact required input prefixes and algorithm/policy/format descriptors
   from the active fenced job;
5. scheduled-leader, signature/provenance, and FEC-root validation for every
   promoted non-genesis shred chain, or the deterministic entry/PoH and
   structural genesis proof for slot zero; and
6. a complete finality manifest in which every slot is explicitly produced,
   skipped, or unresolved and every produced slot names its finalized
   final PoH hash and, where required, consensus block ID, with an authoritative
   predecessor-parent anchor carrying both identities when the parent is
   outside the epoch range.

`UNRESOLVED` prevents a complete result. `SKIPPED` is authoritative evidence,
never inferred from missing input. Evidence beyond a job's end cursors is late:
it may cause a replacement job before commit, but V1 has no overlapping
post-commit repair generation. It never mutates the active job or committed
bytes.

The uploaded candidate manifest remains non-canonical until Blockzilla verifies
the current job ID, fence, input prefixes, finality, object set, dense
IDs, and expected catalog predecessor. Blockzilla then writes the distinct
reader-visible completion manifest and advances the catalog by compare-and-swap.
There is exactly one active compact lease in V1.

This is an internal Rust semantic interface. A future remote normalizer needs a
separate framed wire specification and compatibility fixtures.

The [public exit protocol](hivezilla-public-exit-protocol.md) must not expose
this internal candidate directly. A Hivezilla processor may instead emit a
separate immutable, producer-specific observation after its own completeness
checks. That wire event is provisional, carries no Blockzilla canonical
authority, and is not this internal candidate representation.
