# Blockzilla V1 centralized compaction job protocol

Status: **draft; normative for the proposed V1 only**.

This contract is the only V1 boundary between the central Blockzilla
scheduler/catalog and a Hivezilla compact worker. Blockzilla defines immutable
work, grants one fenced attempt, and conditionally publishes one result.
Hivezilla reads already-custodied stream ranges and/or content-addressed finite
inputs, builds immutable candidate objects, and returns a result. A worker never
writes the canonical catalog and never acknowledges raw custody.

V1 has one active scheduler/catalog authority and at most one active finite-work
attempt across all product kinds. It has no peer discovery, election, consensus, active-active
compaction, or second publication authority.

The attempt fence is global across every finite-worker product, not one counter
per queue:

```text
FiniteWorkKindV1 =
    ARCHIVE_COMPACTION = 1
  | REPLAY_FINALITY_RESOLUTION = 2
  | REPLAY_PUBLICATION = 3

ActiveFiniteWorkV1 {
  kind: FiniteWorkKindV1
  work_id: [u8; 16]
  fence: u64
}
```

Blockzilla persists at most one active tuple. Every grant and result carries its
kind, ID, and fence either directly or through its product-specific envelope;
cross-kind substitution is rejected. Replacing any attempt increments the same
monotonic fence. This is serialization, not fairness: the scheduler policy and
capacity gate must also bound how long Replay resolution, Replay publication,
or Archive work can delay either other product.

Before any Replay cutover, Blockzilla freezes:

```text
FiniteWorkSchedulerPolicyV1 {
  weights: [u16; 3]                    // each > 0, kind order above
  max_consecutive_grants: [u16; 3]    // each > 0
  max_attempt_wall_ms: [u64; 3]       // finite, each > 0
  max_ready_wait_ms: [u64; 3]         // finite, each > 0
  max_backlog_epochs: [u32; 3]        // finite, each > 0
  minimum_capacity_headroom_bps: u16  // 1..9999
}
```

Scheduling is weighted round-robin **by grant**, so every grant has cost one.
The fixed cycle lists kinds `1, 2, 3`, each repeated by its weight. Blockzilla
persists the cycle cursor. Within a kind it selects ascending persisted
`ready_sequence`, then lexicographic `work_id`; a failed/expired retry receives a
new ready sequence at that kind's tail. Empty kinds are skipped, and
`max_consecutive_grants` can force the next non-empty kind earlier. Thus no
implicit runtime estimate or local queue order affects selection.

Each attempt must finish or return `NOT_COMPLETE` within its
`max_attempt_wall_ms`; V1 defines no cross-fence progress object. Immutable
partial uploads are non-canonical and may be reused only when the unchanged job
spec independently verifies them. An overrun is a capacity-policy failure, not
permission to invent an unbound resume cursor. `ready_wait_ms` is wall time from
persisted `ready_sequence` assignment to accepted grant; `backlog_epochs` is the
number of complete epoch work items ready but not committed for that kind. The
scheduler alerts and halts cutover if either limit is crossed. Measured arrival rate
times measured p95 service demand, summed over all three kinds, must remain below
one worker's capacity by at least `minimum_capacity_headroom_bps`. These numeric
values are deployment inputs, but `None`, infinity, or “best effort” is not a V1
policy.

This catalog chain is provisioned for Archive V2 only. Replay V1 reuses the
singular scheduler/fence and exact-CAS mechanics but publishes to a distinct
replay-product catalog head defined with the
[Replay Projection](blockzilla-replay-projection-v1.md). The two products never
append entries to each other's epoch chain.

## 1. Fixed encoding and identities

The structures below use one canonical encoding:

- fields appear in the order shown;
- unsigned integers are big-endian;
- enums are one unsigned byte;
- fixed byte arrays are written directly;
- variable bytes are `u32 length || bytes`;
- vectors are `u32 count || elements`;
- `Option<T>` is `0` for absent or `1 || T` for present; and
- maps, duplicate object keys, unknown enum values, non-minimal lengths, and
  trailing bytes are rejected.

Every implementation must publish golden fixtures before calling this V1 a
compatibility promise.

```text
ObjectRefV1 {
  key: bytes                 // opaque immutable object-store key
  object_version: Option<bytes> // provider token when one exists
  encoded_len: u64
  sha256: [u8; 32]
}

NamedObjectRefV1 {
  logical_name: bytes        // format-registered role/name, not a storage key
  object: ObjectRefV1
}

HashedDescriptorV1 {
  sha256: [u8; 32]
  bytes: bytes               // exact, bounded descriptor bytes
}
```

`ObjectRefV1.encoded_len` and `sha256` cover the exact stored object bytes;
`sha256` is ordinary SHA-256 with no domain prefix. Domain-separated hashes are
used only where this specification defines a semantic identity separately.
Keys are 1..4,096 bytes; a present provider version is 1..4,096 bytes.

`HashedDescriptorV1.sha256` is
`SHA-256("blockzilla/v1/descriptor" || bytes)`. Descriptors are immutable and
must include every option that can change candidate selection or output bytes.
An unknown descriptor is an unsupported job, never permission to use local
defaults. Descriptor bytes are 1..1,048,576 bytes.

An object key and version are interpreted only in the storage endpoint and
credential scope configured for their containing field. Input and finality
references use Blockzilla's configured read scope. Every candidate and
completion object key must be below the job's `output_namespace`; a key never
selects credentials. The configured store must preserve a named version when
present or enforce write-once keys; overwriting bytes behind an accepted
reference is a fatal storage-policy violation. Provider version IDs are
optional metadata, not a portability requirement; length plus digest and
immutable creation remain mandatory.

## 2. Required input and finalized coverage

```text
InputStreamRangeV1 {
  stream: StreamHeaderV1
  start: CursorV1            // inclusive; exact prefix anchor
  end: CursorV1              // exclusive; exact prefix anchor
}

InputObjectV1 {
  logical_name: bytes        // registered input role, such as old-faithful.car
  format: HashedDescriptorV1 // exact decoder and validation contract
  object: ObjectRefV1
}

SlotRangeV1 {
  first_slot: u64            // inclusive
  next_slot: u64             // exclusive
}

FinalizedSlotV1 {
  slot: u64
  disposition:
    PRODUCED = 1 { identity: FinalizedBlockIdentityV1 }
    SKIPPED = 2
    UNRESOLVED = 3
}

FinalizedBlockIdentityV1 {
  final_poh_hash: [u8; 32]
  consensus_block_id: Option<[u8; 32]>
}

FinalizedParentAnchorV1 {
  slot: u64
  identity: FinalizedBlockIdentityV1
}

FinalityManifestV1 {
  manifest_version: u16              // 2; implemented unversioned draft is invalid
  cluster_genesis_hash: [u8; 32]
  epoch: u64
  slots: SlotRangeV1                 // exact published epoch
  finality_validation_slots: SlotRangeV1 // published + finality-required prefix
  validation_slots: SlotRangeV1      // full range, possibly longer for status proof
  authority: HashedDescriptorV1
  evidence_stream_inputs: repeated InputStreamRangeV1
  evidence_object_inputs: repeated InputObjectV1
  predecessor_parent: Option<FinalizedParentAnchorV1>
  entries: repeated FinalizedSlotV1
}
```

`StreamHeaderV1` and `CursorV1` are the exact canonical types from the
[Hivezilla source-spool protocol](hivezilla-record-and-sync-protocol.md).
`first_slot` must be less than `next_slot`. Both validation ranges start at
`slots.first_slot`, and
`slots.next_slot <= finality_validation_slots.next_slot <=
validation_slots.next_slot`. The full validation range has at most 1,048,576
slots. `finality_validation_slots == slots` if and only if the finality
authority needs no descendant suffix. `validation_slots ==
finality_validation_slots` if and only if status proof needs no additional
suffix. A gratuitous unused suffix is non-canonical.

`manifest_version` must equal 2. This explicit tagged break prevents the
implemented unversioned single-hash draft—including an all-skipped manifest—from
being accepted as the two-identity contract by shape coincidence.

The finality entries are sorted by slot, contain every slot in published
`slots` exactly once, and contain no slot outside it. Slots in
`finality_validation_slots - slots` are the exact descendant prefix used by the
finality rule. Slots in `validation_slots - finality_validation_slots` are
status-proof evidence only. Neither suffix receives an entry or becomes output
of this generation. The manifest object is immutable and its
`ObjectRefV1` digest covers the canonical bytes above.
One manifest has at most 1,048,576 entries. Its cluster, epoch, and published
slot range must equal the job's. Every evidence stream range or object must
exactly equal one of the job's corresponding required inputs; an undeclared or
wider range and a changed object reference are invalid.

`evidence_stream_inputs` is sorted by `stream.stream_id` and contains no
duplicate stream ID. `evidence_object_inputs` follows the input-object
name/ordering rule below. These ordering and uniqueness rules are part of the
canonical manifest encoding, not presentation conventions.

Input-object logical names are unique, sorted by raw bytes, and use 1..128 ASCII
bytes from `[a-z0-9._-]`. V1 initially registers `old-faithful.car` and
`old-faithful.car-zst`; the format descriptor pins the CAR decoder, compression,
and validation behavior. Listing, a mutable URL, or a local path is not a finite
input identity: the exact immutable `ObjectRefV1` is mandatory.

Blockzilla owns the configured finality authority. Its descriptor identifies
the cluster, source and commitment rule used to bind a finalized final PoH hash,
an optional consensus/shred block ID, or an authoritative skip to a slot. A
slot-only status event is not sufficient to select among competing identities
unless the descriptor also defines and Blockzilla verifies an unambiguous link
to an identity-bearing finalized record.

When finality is derived from Tower roots or marker-era certificates, the
resolver may require complete descendants after `slots.next_slot`. Replay may
also require a longer suffix to close an unknown-offset status-key cohort. Both
exact boundaries and their inputs are frozen in the manifest. Descendant
candidates through `finality_validation_slots` are validated/replayed only to
settle entries inside `slots`, while the remaining status-only suffix is scanned
only for its proof; the
archive/replay generation still emits exactly its one epoch.
If the bounded lookahead does not settle every entry, the manifest contains
`UNRESOLVED` and cannot publish.

When evaluating those certificates itself requires stateful replay, the
manifest cannot be an input to the resolution work. Blockzilla first persists
the immutable evidence-, range-, trust-, checkpoint-, runtime-, and rule-bound
`FinalityResolutionSpecV1` defined by Replay V1, then executes it under the same
globally monotonic attempt fence used for finite work. Only a result matching
the current resolution ID and fence may create the immutable manifest. An
Archive `CompactionJobV1` is issued after that freeze and consumes the resulting
manifest; it never authorizes the unvalidated resolution candidate that
produced it.

Blockzilla may run the resolver in another process, but it owns acceptance of
the immutable manifest and pins every evidence range/object in the job before
issuing the compact job. A worker cannot self-declare finality. An RPC or
provider response used by the resolver must already be exact custodial evidence
in one of those ranges; an unrecorded live query is not job evidence.

The dispositions have exact consequences:

- `PRODUCED` requires exactly one complete accepted candidate with the stated
  final PoH hash and, when present, consensus block ID;
- `SKIPPED` emits no block and is not inferred from absence, `null`, timeout,
  `404`, or a gap; and
- `UNRESOLVED` prevents a complete result and therefore prevents catalog
  publication.

Every required stream or object input is mandatory even when another source appears to contain
the same block. A missing stream, unavailable byte, prefix mismatch, unexpected
stream reset, or record outside the declared chain makes the job incomplete.
This prevents an omitted source from being mistaken for a fully covered job.

### Replay-product dependency for Archive V2

Archive V2 may be built from a committed Replay V1 generation without treating
format 8 as raw custody input. The job uses one explicit transitive dependency:

```text
ReplayArchiveDependencyV1 {
  replay_catalog_entry: ObjectRefV1
  replay_validation_receipt: ObjectRefV1
  replay_runtime_attachment_manifest: ObjectRefV1
  required_parity_manifest: Option<ObjectRefV1>
  attachment_and_signature_join: HashedDescriptorV1
}
```

Before issuing the Archive job, Blockzilla proves that
`replay_catalog_entry` is reachable from the provisioned Replay V1 catalog head
and that its committed completion transitively names the exact validation
receipt, final format-8 generation, finality manifest, and runtime-attachment
manifest. The repeated references above must equal those reachable references
byte for byte; they are explicit job choices, not alternative locators. If
`required_parity_manifest` is present, it names an immutable
`RuntimeParityManifestV1` covering the exact produced-slot set. It compares each
produced ordinal from a distinct `PROVIDER_OBSERVATION` manifest on the left and
`FINAL_REPLAY` manifest on the right for the same execution core, has
`unexplained_mismatch_count == 0`, and has equal left/right normalized stream
hashes.
Equal attachment-manifest or producer-provenance identities are invalid; equal
normalized runtime results are expected, and a self-compare cannot satisfy
parity.
The parity manifest's `right_attachment_manifest` must equal
`ReplayArchiveDependencyV1.replay_runtime_attachment_manifest` byte for byte;
parity against any other Replay run cannot authorize the selected attachment.
Both parity attachment manifests must have the job's exact cluster, epoch,
published range, finality-manifest digest, execution-generation digest,
normalization start-state digest, runtime/feature-map digest, entry count, and
produced-ordinal set. The right instrumentation descriptor equals the committed
Replay attachment manifest. The left instrumentation descriptor must satisfy
the one explicit compatibility relation named by the parity normalizer and join
descriptor; an ambient “close enough” policy is invalid.
`attachment_and_signature_join` declares `NoParity` or
`RequireZeroMismatchV1`; the option is absent or present exactly as declared, so
a worker cannot silently skip a required shadow comparison.

The Replay generation and Archive job must have the same cluster, epoch,
published slot range, epoch schedule, produced/skipped dispositions, final PoH
hashes, consensus block IDs, and predecessor anchor. Their finality-manifest
bytes are identical, not merely semantically similar. The validation receipt
must prove execution of the exact published format-8 bytes from its bound
checkpoint/runtime and expose a per-produced-slot validation row as defined by
the [Replay Projection](blockzilla-replay-projection-v1.md).

Replay V1 deliberately omits outer signatures. Therefore at least one
top-level required raw stream or finite signed-entry object must contain the
complete signed transactions for every produced slot selected from Replay. The
join descriptor fixes the structural join, signature verification, attachment
selection, and any normalization. The worker joins by canonical
`(slot, transaction_ordinal)` and exact signed-message bytes. The ordinal is the
Replay component/entry order flattened within the slot; a source without entry
coordinates is valid only if its adapter proves that same complete transaction
order. The worker verifies every restored outer signature, maps the ordered
transactions back through Replay entry counts, recomputes every signature mixin
and PoH boundary, and establishes the selected candidate's exact signed-ledger
equality. A private Replay placeholder signature is never an Archive V2
signature, transaction ID, or join key. A missing, duplicated, reordered,
ambiguous, or conflicting join makes the job incomplete.

The dependency is immutable input provenance. It does not merge the Replay and
Archive catalog heads, make format 8 custody-bearing, or let an Archive result
change Replay publication. Archive recovery copies the Archive serving closure;
Replay catalog/receipt/attachment/checkpoint recovery remains the separate
product-tagged chain defined by Replay V1 and is re-resolved during provenance
audit.

## 3. Job and lease

```text
CompactionJobV1 {
  job_id: [u8; 16]
  cluster_genesis_hash: [u8; 32]
  epoch: u64
  slots: SlotRangeV1
  required_stream_inputs: repeated InputStreamRangeV1
  required_object_inputs: repeated InputObjectV1
  shred_trust_context: Option<InputObjectV1>
  finality_manifest: ObjectRefV1
  replay_dependency: Option<ReplayArchiveDependencyV1>

  selection_policy: HashedDescriptorV1
  normalization_algorithm: HashedDescriptorV1
  archive_format: HashedDescriptorV1
  epoch_schedule: HashedDescriptorV1

  expected_catalog_predecessor: Option<[u8; 32]>
  expected_catalog_generation: u64
  output_namespace: bytes

  fence: u64
}
```

`required_stream_inputs` are sorted by `stream.stream_id`; duplicate stream IDs
and empty or reversed ranges are invalid. They contain only custody-bearing raw
formats 1 through 5 or operational-evidence format 7 from the Hivezilla
protocol. Bounded derived format 6 is not a V1 compaction input. Every stream
header must carry the job's cluster genesis hash.

Each required/evidence stream-input vector and required/evidence object-input
vector has at most 4,096 elements. The combined required stream/object count is
nonzero, including when `replay_dependency` is present because signatures and
transaction identities still require exact signed evidence. `output_namespace`
is at most 4,096 bytes.

`required_object_inputs` follow the name/ordering rules above. The optional
`shred_trust_context` uses the same encoding and name rules, has the registered
logical name `solana.shred-trust-context`, and cannot duplicate a required
object name. It is present if and only if any job input can contribute
shred-derived candidate bytes. Its immutable object contains the complete
leader-schedule and signature/proof trust context covering every shred slot used
by the job, including the finality-validation lookahead; its format descriptor
defines that coverage and verification semantics. A live RPC lookup, local
schedule cache, or mutable URL is not trust context.

At least one required stream or finite object input is present; the trust
context alone is not evidence. The exact inputs, trust context, policies,
algorithms, optional replay dependency, and format descriptors are part of the
job so a retry cannot silently inherit different local behavior.

`epoch_schedule` canonically encodes the Solana epoch-schedule parameters for
this cluster. `epoch` and published `slots` must equal one complete epoch under
that descriptor. The manifest's finality-prefix and full validation ranges may
cross the following epoch boundary only as immutable finality/status evidence.
V1 publishes one complete epoch
per catalog generation; partial, overlapping, and multi-epoch output jobs are
invalid.

`job_id` identifies immutable work. Its epoch/range, inputs, finality manifest,
optional replay dependency, descriptors, predecessor, generation, and output
namespace do not change between attempts. Changing any of those creates a new
job ID.
`expected_catalog_generation` is the generation to create: generation zero has
no predecessor, and every later generation requires the current catalog to be
exactly generation minus one with digest `expected_catalog_predecessor`.
That predecessor is the domain-separated hash of the current `CatalogEntryV1`,
defined below. It is absent only for generation zero.

```text
JobSpecObjectV1 = "blockzilla/v1/compaction-job-spec" ||
                  canonical_encode(all CompactionJobV1 fields except fence)

job_spec_hash = SHA-256(JobSpecObjectV1)
```

Before granting the first execution, Blockzilla uploads exactly
`JobSpecObjectV1` as one immutable job-spec object in a catalog-readable namespace.
Its `ObjectRefV1.sha256` equals `job_spec_hash`; the eventual completion manifest
retains that reference so the catalog can be audited or rebuilt without a
separate scheduler database.

Blockzilla durably allocates `fence` monotonically across all finite
executions. The fence is also the attempt generation; V1 deliberately has no
second attempt counter. Before granting a replacement it expires or explicitly
revokes the previous execution, persists the larger fence, then issues the job.
At most one `(job_id, fence)` is active globally.
A worker must stop when it loses the lease, but safety does not depend on timely
stop: Blockzilla rejects every result whose tuple is not the current active
tuple. Lease heartbeat and expiry transport are deployment details; the durable
active tuple is the authority.

`output_namespace` is a non-empty byte prefix ending in `/`. An attempt writes
only below
`output_namespace || "jobs/" || lower_hex(job_id) || "/" ||
u64_hex(fence) || "/"`, where `u64_hex` is
exactly 16 lowercase hexadecimal digits. Object creation is conditional: an
existing key is reusable only when version, length, and digest identify the
same bytes. Conflicting bytes at one key fail the attempt.

Worker credentials grant writes only to that exact attempt prefix. They do not
cover `output_namespace || "catalog/"`, the completion/finality publication
prefix, the catalog-entry namespace, or the mutable head. Those remain
Blockzilla-only even though canonical readers can read the committed objects.

## 4. Mandatory candidate policy

The worker must replay every required stream and finite object exactly, verify
the optional Replay dependency and its exact signed-evidence join, and apply
only the descriptors pinned by those inputs and the job. Candidate construction
otherwise follows the
[minimal block candidate](blockzilla-block-candidate-v1.md).

Before any non-genesis shred-derived bytes may contribute to an Archive V2
candidate, the worker must, using the job-pinned algorithm and exact
`shred_trust_context`:

1. resolve the scheduled leader for the slot;
2. verify the applicable Solana shred signature/provenance rule, including the
   configured retransmitter rule when relevant;
3. verify Merkle/proof and FEC-set root consistency for original, repaired, and
   recovered shreds;
4. partition conflicting FEC chains instead of merging them; and
5. complete ordered components, verify the final PoH hash, and derive the exact
   terminal consensus/shred block ID without treating those hashes as aliases.

Slot zero instead has canonical `parent_slot = 0` and absent parent identities.
It is promotable only when its deterministic tick/entry sequence, PoH
start/final hash, and shred parameters match the job-pinned construction from
the digest-bound `genesis.bin`. The arbitrary construction keypair is not a
scheduled-leader trust root. Exact shred bytes are required only when the
complete genesis archive/blockstore is itself digest-bound.

Raw capture remains unfiltered forensic evidence. Failure of this promotion
gate quarantines the candidate; it never rewrites or deletes the raw records.
The selection or normalization descriptor must identify every signature,
retransmitter, proof, recovery, and conflict rule. The trust-context format
descriptor identifies the schedule/source semantics and exact covered slots;
an implementation must not fill either from local defaults. A missing,
unavailable, wrong-cluster, out-of-range, or digest-mismatched trust context
makes a shred-bearing job invalid.

For each `PRODUCED` slot, the worker applies the job's selection policy and must
obtain exactly the finalized `final_poh_hash` named by `FinalityManifestV1`.
When `consensus_block_id` is present, it must also match exactly. Conflicting
complete content under the same selected identity is unresolved. A candidate
with a different final PoH hash or a different present consensus block ID is
retained as non-canonical evidence but is not written into this canonical
Archive V2 result.

The optional block ID is an era-exact constraint, not permission to conflate the
two hashes. `None` is valid only where the pinned protocol descriptor defines no
separate consensus/shred block ID; in that legacy range the final PoH hash is
the explicit fork identity. Wherever the protocol defines a block ID, any
selected shred-backed candidate and every Replay V1 publication require
`consensus_block_id = Some(...)`. A gRPC/RPC authority that cannot bind that ID
cannot choose among block-ID variants; the slot is `UNRESOLVED` for such a job.

For every non-genesis produced candidate, `parent_slot < slot` and
`parent_final_poh_hash` equals the finalized final PoH hash at `parent_slot`.
That parent is exactly the greatest earlier `PRODUCED` slot in this manifest,
or `predecessor_parent` for the first produced slot; a jump over another
produced entry would fork the supposedly linear finalized chain and is invalid.
When the selected candidate has a parent consensus block ID, that ID must also
equal the parent's finalized ID. An in-range parent finality entry must be
`PRODUCED` with those identities. A parent before `first_slot` must exactly
equal `predecessor_parent`; no parent at or after `next_slot` is valid. The
anchor is present if and only if a produced candidate needs it. For generation
zero it is accepted from the pinned finality authority; for later generations
Blockzilla also verifies it against the most recent produced block reachable
through the catalog predecessor chain. Genesis alone has no parent anchor.

Archive V2 IDs are zero-based within each complete epoch generation. Assign
consecutive `u32` IDs to `PRODUCED` slots in ascending slot order; `SKIPPED`
slots receive no ID. Overflow or any different ordering fails the job.

## 5. Candidate result

```text
CandidateManifestV1 {
  job_id: [u8; 16]
  job_spec_hash: [u8; 32]
  fence: u64
  epoch: u64
  slots: SlotRangeV1
  finality_manifest: ObjectRefV1
  produced_count: u32
  skipped_count: u32
  objects: repeated NamedObjectRefV1
}

CompactionResultV1 {
  job_id: [u8; 16]
  job_spec_hash: [u8; 32]
  fence: u64
  outcome: COMPLETE = 1 | NOT_COMPLETE = 2
  candidate_manifest: Option<ObjectRefV1>
}
```

Candidate objects are immutable and `objects` is sorted by `logical_name`.
Names are unique, 1..128 ASCII bytes from `[a-z0-9._-]`, and registered by the
pinned archive-format descriptor; they identify roles such as block payload or
index without parsing an opaque storage key. The vector contains the complete required Archive V2
payload, index, sidecar, registry, coverage, and provenance object set for the
selected format; no required object may be discovered by bucket listing. The
candidate manifest is itself uploaded under the fenced execution prefix and is
not a publication marker.
The candidate/completion object vector contains 1..65,536 elements. Decoders
reject larger counts before allocation. Recovery mappings use their separate
131,072-element transitive-closure bound below.

The candidate manifest's job ID/hash, fence, epoch/range, and finality reference
exactly equal the active job. `COMPLETE` requires exactly one
candidate-manifest reference; every other outcome requires it absent. The
manifest and result are canonically encoded before their object digests or
transport fixtures are computed.

Only `COMPLETE` may carry a candidate manifest or proceed toward catalog commit.
Its counts must match the finality entries and satisfy
`produced_count + skipped_count == next_slot - first_slot`; an `UNRESOLVED`
slot, missing required input, conflict, failed validation, counter overflow, or
missing object produces a non-complete result. Diagnostics remain outside this
integrity contract.

`NOT_COMPLETE` is the only V1 failure outcome and cannot carry a candidate
manifest. Finality ambiguity, invalid input, conflicting content, policy
rejection, and transient execution failure remain structured diagnostics in the
scheduler's work log, outside the publication contract. The scheduler decides
whether to retry, repair infrastructure, or replace the immutable job; workers
do not make a portable retry-policy decision.

For identical job-spec bytes, accepted implementations must produce identical
canonical payload bytes, payload digests, dense IDs, and ordered logical object
roles. A candidate manifest may differ only in its fence and the resulting
fence-prefixed physical keys or provider version tokens; the
referenced bytes may not differ.

## 6. Blockzilla commit

The worker never creates the reader-visible completion manifest and never
writes the catalog pointer.

```text
CompletionManifestV1 {
  catalog_generation: u64
  catalog_predecessor: Option<[u8; 32]>
  job_id: [u8; 16]
  job_spec_hash: [u8; 32]
  epoch: u64
  slots: SlotRangeV1
  job_spec: ObjectRefV1
  candidate_manifest: ObjectRefV1
  published_finality_manifest: ObjectRefV1
  produced_count: u32
  skipped_count: u32
  objects: repeated NamedObjectRefV1
}

CatalogEntryV1 {
  generation: u64
  predecessor: Option<ObjectRefV1>
  completion_manifest: ObjectRefV1
}

CatalogHeadV1 {
  generation: u64
  entry: ObjectRefV1
}
```

The completion's generation, predecessor digest, job ID/hash, epoch/range,
job-spec reference, accepted candidate-manifest reference, finality bytes,
counts, and ordered object vector must all match the validated job and candidate.
Its `objects` vector is byte-for-byte the candidate manifest's sorted unique
vector; Blockzilla cannot omit, add, rename, or reorder a worker object while
publishing it.

The semantic digest of a catalog entry is
`SHA-256("blockzilla/v1/catalog-entry" || canonical_encode(CatalogEntryV1))`.
The job and completion manifest carry that digest for the expected predecessor;
`CatalogEntryV1.predecessor` carries the immutable object reference needed to
locate and verify its exact bytes. Generation zero has neither predecessor
digest nor predecessor object reference; later generations require both, and
the fetched entry's semantic digest must equal the expected digest.
The authoritative mutable state is either an empty head or exactly one
canonically encoded `CatalogHeadV1`.

One `CatalogHeadStore` is immutably provisioned for one cluster genesis hash,
archive-format descriptor, and epoch-schedule descriptor. Generation zero pins
that tuple; every later job and fetched predecessor job spec must match it.
Using a head from another configured catalog fails closed even if its generation
and object references otherwise decode.

One active catalog writer does not mean one unrecoverable catalog copy. The CAS
backend, immutable catalog entries, and an independently recoverable exact head
checkpoint are covered by a declared catalog durability policy. Blockzilla does
not advertise a successful commit until that checkpoint is durable. Disaster
recovery restores the exact checkpoint and verifies its entry/predecessor chain;
it never selects the numerically largest object found by bucket listing, because
uncommitted branches and orphan manifests may exist.

Blockzilla accepts a result only after it:

1. matches the current job and fence;
2. matches `job_spec_hash` and the expected predecessor/generation;
3. revalidates the finality and full slot coverage rules and, when present, the
   committed Replay dependency, final-byte validation receipt, selected runtime
   attachment, parity policy, and exact raw-signature join;
4. reads and verifies the candidate manifest and validates every declared
   object's version, length, and digest using a provider-authenticated checksum
   or policy-required readback;
5. confirms object/index/sidecar agreement, zero-based dense-ID assignment, and
   the finalized parent chain; and
6. verifies the immutable canonical job-spec object whose digest is
   `job_spec_hash`;
7. copies or conditionally creates the accepted finality-manifest bytes under a
   catalog-readable immutable namespace, verifies their exact length and
   digest, and uses that published reference in the completion manifest; then
8. builds and uploads the immutable completion manifest itself.

`published_finality_manifest` is therefore catalog-readable, not the private
scheduler/input-scope locator from the job. Its bytes and SHA-256 must equal the
job's accepted finality manifest exactly. Canonical readers can validate both
produced final-PoH/consensus-ID bindings and explicit skipped-slot evidence
without scheduler credentials.

Every object referenced directly by `CompletionManifestV1` is available in the
configured catalog-reader scope. Raw stream ranges and finite evidence objects
named inside the job/finality provenance may remain operator-only; canonical
serving verifies Blockzilla's catalog-committed decision and does not require
terminal-custody credentials or refetch all raw evidence.

Blockzilla then uploads the immutable `CatalogEntryV1` under the configured
catalog namespace and performs one compare-and-swap from the exact current
encoded `CatalogHeadV1` value to the new head containing its verified object
reference, or from empty to generation zero. Expected generation zero requires
an empty head. For expected generation `N > 0`, the exact current head, fetched
entry, and fetched completion all have generation `N - 1`, the fetched entry's
semantic digest equals `expected_catalog_predecessor`, the new epoch is the
predecessor epoch plus one, its `first_slot` equals the predecessor's
`next_slot`, and its epoch-schedule descriptor is unchanged. The new head,
entry, and completion all have generation `N`. These checks make the catalog an
append-only, gap-free, non-overlapping epoch chain. The catalog-head backend is a separate linearizable
mutable-value authority; conditional immutable object creation or bucket
listing is not an implementation of this CAS. The head CAS is the sole
canonical publication event. Readers ignore candidate manifests, unreferenced
completion manifests, bucket listings, and objects not reachable from the
current catalog chain.

A crash before the CAS leaves only unreachable immutable objects. If the CAS
response is lost, Blockzilla rereads the linearizable head: equality with the
new exact head proves success, equality with the old exact head permits the same
CAS retry, and any other value is a stale/conflicting commit. After success it
persists the independent exact-head checkpoint before reporting completion.
Repeating recovery never publishes a different entry. A stale fence may finish
uploading, but its result can never pass the active fence or catalog CAS.

### Independent archive recovery copy

Canonical object references may contain an online provider's version token. A
recovery provider is not required or permitted to reproduce that token. Instead
the optional copy/audit role writes this non-canonical mapping:

```text
RecoveryObjectMappingV1 {
  canonical: ObjectRefV1
  recovery: ObjectRefV1
  verification: u8  // PROVIDER_SHA256=1, FULL_READBACK_SHA256=2
}

ArchiveRecoveryReceiptV1 {
  catalog_generation: u64
  catalog_entry: ObjectRefV1
  recovery_target_id: [u8; 16]
  recovery_failure_domain_id: [u8; 16]
  recovery_target: HashedDescriptorV1
  previous_receipt: Option<ObjectRefV1>
  objects: repeated RecoveryObjectMappingV1
}

ArchiveRecoveryCheckpointV1 {
  recovery_target_id: [u8; 16]
  catalog_head: CatalogHeadV1
  latest_receipt: ObjectRefV1
}
```

`objects` is sorted by the canonical `(key, object_version)` with `None` before
`Some`, contains no duplicate canonical reference, and contains the complete
transitive publication set for that catalog entry: the entry itself, completion
manifest, job spec, candidate manifest, published finality manifest, and every
named archive object. When the job requires parity, it additionally contains the
exact parity manifest and the left `PROVIDER_OBSERVATION` attachment
manifest/chunks. The parity right side is the byte-identical committed Replay
attachment reference and is resolved through the separately protected Replay
recovery head; it is not silently replaced or treated as protected by the
Archive copy. Missing either typed closure makes provenance recovery degraded
and alerts. Each recovery length and SHA-256 exactly equals its
canonical peer; only key, optional provider version, and configured storage scope
may differ. The target descriptor is canonical and secret-free; credentials are
external. Before a receipt counts, its target ID, failure-domain ID, and
descriptor must exactly match the immutable recovery-target configuration; an
embedded value cannot relabel the copy into another failure domain. Unknown
targets, mismatches, and unknown verification values fail closed.

A recovery receipt contains at most 131,072 mappings and 536,870,912 canonical
encoded bytes. Counts and variable lengths are checked before allocation. A
generation whose complete closure exceeds either bound needs a successor
recovery format; it is never represented by a truncated receipt.

The receipt becomes complete only after every recovery object is independently
verified. It is itself conditionally created and verified in the recovery
failure domain at
`blockzilla-recovery/v1/<target-id-hex>/<generation-hex>-<catalog-entry-object-sha256-hex>.receipt`.
Target ID and digest are their complete lowercase hexadecimal bytes;
`generation-hex` is exactly 16 lowercase hexadecimal digits; and the digest is
the ordinary `catalog_entry.sha256` field from its `ObjectRefV1`, not the
domain-separated semantic catalog-entry digest.
`catalog_generation` and the referenced catalog entry's generation must equal
the generation encoded in that key. Generation zero has no previous receipt;
every later receipt increments by one and names the exact recovery-provider
object reference of the prior generation's receipt, whose catalog entry must be
the canonical predecessor. Receipt objects and chains are never selected by
listing.

After writing a complete receipt, the copier compare-and-swaps
`blockzilla-recovery/v1/<target-id-hex>/head.checkpoint` from the exact prior
checkpoint bytes to
`ArchiveRecoveryCheckpointV1`. Its catalog head must equal the generation and
entry covered by `latest_receipt`. Only after that checkpoint is durable is the
generation `archive_recovery_protected`. The checkpoint and receipt chain live
in the recovery failure domain and are backed up with its credentials, so loss
of the online provider does not remove their discovery path.

The checkpoint's `recovery_target_id` must equal the target ID encoded in its
checkpoint key, the immutable configured target ID, and the target ID in the
fetched `latest_receipt`; any mismatch fails closed, including on generation
zero. That receipt's target descriptor and failure-domain ID must also match the
same immutable target configuration.

Generation zero compare-and-swaps from an empty recovery checkpoint. Generation
`N > 0` requires the exact current checkpoint for `N - 1`, the same configured
target, a `previous_receipt` equal to the prior checkpoint's `latest_receipt`,
and a fetched new catalog entry whose `predecessor` equals the prior checkpoint's
catalog-head entry. A lost CAS response is resolved by
rereading: the exact new checkpoint proves success, the exact old checkpoint
permits the same retry, and any other value is a conflict. The copier never
advances by listing receipts or choosing the largest generation.

A recovery reader starts from that exact checkpoint, follows receipt links,
maps each canonical reference to its recovery locator/version, and re-verifies
length and SHA-256 while reading. A missing or partial receipt, or a receipt not
reachable from the checkpoint, is degraded recovery state and alerts; it neither
changes the catalog nor blocks online reads. The copier has no catalog, raw-ACK,
immutable-object overwrite, or delete authority. Its only mutable authority is
exact compare-and-swap on its one configured recovery-checkpoint key; recovery
objects and receipts are conditional-create-only.

## 7. Retry and late evidence

- A worker restart may resume the same still-active fence and reuse matching
  immutable objects.
- After expiry or revocation, Blockzilla increments the fence and uses a new
  execution prefix. The immutable job specification remains unchanged.
- A retry with the same `job_spec_hash` must be byte-deterministic apart from
  the permitted physical-reference differences. Different payload output is
  quarantined rather than selected by timing.
- Evidence arriving after an input `end` cursor is late and cannot mutate an
  active job. Before catalog commit, Blockzilla may cancel it and create a new
  job ID with new input/finality anchors. V1 cannot overlap or supersede a
  committed epoch; post-commit correction requires a future versioned repair
  protocol and reader rule outside this catalog. Committed objects are never
  edited in place.
- Late evidence that is an exact duplicate and changes neither coverage nor
  policy outcome may be recorded for audit without replacing the job.

Compaction outcome, upload, completion, or catalog commit never authorizes a
HiveSync raw-custody ACK.

## 8. Conformance gates

V1 fixtures must prove:

1. canonical encodings and hashes for valid single-stream, multi-stream,
   object-only CAR, object-only CAR.ZST, and mixed stream-plus-object jobs,
   manifests, results, and catalog entries, plus rejected empty or invalid
   boundary cases;
2. any missing required stream/object, cursor/prefix mismatch, object
   length/digest mismatch, or changed descriptor fails closed;
3. derived-format stream input, undeclared or duplicate object role, wrong
   finite-input format/digest, and unsorted or duplicate finality evidence are
   rejected;
4. a shred-bearing job without its exact immutable trust context, or with a
   missing, changed, wrong-cluster, out-of-range, or unavailable context, fails
   closed; no live schedule lookup is accepted;
5. forged, wrong-leader, bad-signature, bad-proof, conflicting-FEC-root, and
   invalid-PoH shreds cannot enter a promoted candidate;
6. every slot is exactly one of produced, skipped, or unresolved; absence alone
   never becomes skipped, unresolved prevents `COMPLETE`, and `NOT_COMPLETE`
   never carries a candidate manifest;
7. a produced candidate must match the finality-authority final PoH hash and any
   required consensus block ID; both parent identities must match the finalized
   in-range parent or exact external parent anchor;
8. each generation covers one complete epoch, follows its predecessor without a
   gap or overlap, and assigns zero-based dense IDs only over ascending produced
   slots with byte-identical retry output;
9. two workers cannot hold accepted active tuples, and a stale result cannot
   commit even if all of its objects uploaded successfully;
10. crashes before object upload, during upload, after candidate manifest, after
   completion manifest, immediately before CAS, and immediately after CAS are
   idempotently recoverable, including a successful CAS whose response is lost;
11. readers expose only objects reachable from the catalog-committed completion
   manifest and can read and verify its published finality manifest without
   scheduler/input credentials;
12. late evidence before commit follows replacement rules, while post-commit
   overlap/supersession is rejected without mutating committed bytes;
13. no worker result, manifest, or catalog transition advances raw retention;
14. catalog-head loss restores the exact independently retained checkpoint and
    never promotes an orphan or stale branch discovered by listing;
15. a recovery provider with different keys/version tokens maps the complete
    canonical publication set by length and SHA-256, while a missing, duplicate,
    partial, corrupt, unchained, target/failure-domain-mismatched, or
    checkpoint-unreachable mapping is never selected for recovery; and
16. recovery-checkpoint CAS from empty and an exact predecessor is idempotent
    across lost responses and never advances from a listed receipt; and
17. a Replay-backed Archive job accepts only a catalog-committed Replay
    generation with byte-identical finality, a valid final-byte replay receipt,
    its exact selected attachment/parity objects, and complete verified outer
    signatures rejoined from declared raw evidence; format 8 alone can never
    supply transaction IDs; and
18. finality fixtures reject unequal range starts, non-nested endpoints,
    spec/manifest/status-evidence range mismatch, missing or extra
    `lookahead_finality_validation`, finality checks over the status-only suffix,
    and either suffix entering generation records or successor state.
