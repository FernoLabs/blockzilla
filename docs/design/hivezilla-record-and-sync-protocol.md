# Hivezilla V1 source-spool and custody-transfer specification

Status: **draft; normative for the proposed V1 only**.

Hivezilla V1 preserves exact source records in a node-owned logical spool and
transfers their custody to a terminal raw consumer. The source spool may move
temporarily from local disk to that node's private cloud overflow. Normalization,
fork choice, compaction, and Archive V2 publication never acknowledge raw
custody.

## 1. Contract and failure model

V1 assumes trusted operators, mTLS-authenticated nodes, process/host crashes,
interrupted networks, bounded local storage, detectable corruption, and
temporarily unavailable object storage. It does not attempt Byzantine
consensus or dynamic membership.

The contract is **no deletion-authorized data loss**. Its states are deliberately
not synonyms:

- `captured_local` means a source record crossed the origin journal's configured
  local durability boundary; it is still one source-side copy and is not called
  protected;
- `overflow_durable` means an exact sealed origin segment is recoverable from
  that source node's verified overflow object; it is still source-owned spool
  and is not called protected;
- `protected` means the terminal dataset has independently committed and
  verified the exact record bytes on enough physical durability targets to
  satisfy the stream's immutable `DurabilityPolicyV1`;
- `ACK` means one exact contiguous protected prefix is indexed in that terminal
  dataset and became that one logical dataset's responsibility;
- `retired` means the source durably recorded that ACK and retirement anchor
  before deleting the covered local and overflow copies;
- an unreceived UDP packet or unsent provider item is outside the guarantee,
  but every detected discontinuity is explicit and alerted.

Cloud overflow is a storage tier of the origin spool, not a custody ACK and not
a second consumer. A successful compact block, derived observation, Archive V2
commit, or public subscriber receipt can never advance raw retirement.

The protocol prevents ACK or deletion from outrunning the configured terminal
durability policy. It does not label a successful upload, local fsync, provider
heartbeat, compact object, or source overflow object as protected. Loss of the
sole pre-ACK source disk remains an explicit residual risk. Once ACKed, the
terminal policy requires independently verified raw copies in distinct declared
failure domains; simultaneous failure of enough of those domains remains a
residual risk. Corruption is detected and fails closed; recovery requires a
remaining valid copy.

There is one homogeneous stream per producer instance and payload format. One
source adapter may produce multiple independent streams, such as ordinary
shreds, repair shreds, and slot status. A processor may produce a derived
stream using the same envelope without gaining raw-custody authority.

## 2. Stream and payload

```text
StreamHeaderV1 {
  stream_id: [u8; 16]
  cluster_genesis_hash: [u8; 32]
  payload_format: u32
  payload_format_version: u16
  producer_config_sha256: [u8; 32]
  stream_manifest_sha256: [u8; 32]
}
```

`stream_id` is random. A change to producer semantics, format, schema, codec, or
a producer-WAL reset creates a new stream. One stream has one writer protected
by an exclusive state lock. V1 has no automatic writer failover: an operator may
move the same logical spool only after proving the predecessor stopped;
otherwise the replacement creates a new stream ID.

The immutable stream metadata is:

```text
DurabilityTargetV1 {
  target_id: [u8; 16]
  failure_domain_id: [u8; 16]
  target_descriptor_sha256: [u8; 32]
}

DurabilityPolicyV1 {
  policy_id: [u8; 16]
  minimum_independent_copies: u8  // V1 requires >= 2
  catalog_descriptor_sha256: [u8; 32]
  targets: repeated DurabilityTargetV1
}

LineageV1 {
  predecessor_stream_id: [u8; 16]
  predecessor_last_known_cursor: CursorV1
  reason: u16       // CONFIG_CHANGE=1, WAL_RESET=2, UNSAFE_HANDOFF=3,
                    // SOURCE_HOST_LOSS=4, TERMINAL_STORE_ROLLOVER=5,
                    // OPERATOR_SPLIT=6
  continuity: u8    // CONTIGUOUS=1, GAP_POSSIBLE=2, GAP_CONFIRMED=3
}

StreamManifestV1 {
  manifest_version: u16       // 1
  stream: StreamHeaderV1
  producer_descriptor: bytes  // exact, canonical, and secret-free
  lineage: Option<LineageV1>
  gap_event_stream_id: Option<[u8; 16]>
  overflow_namespace_sha256: Option<[u8; 32]>
  deletion_authorizing_store_id: Option<[u8; 16]>
  durability_policy: Option<DurabilityPolicyV1>
}
```

`producer_config_sha256` is
`SHA-256("hive/v1/producer-config" || producer_descriptor)`. The descriptor
includes RPC projection or processor input/algorithm identity where applicable.
It is non-empty and at most 1,048,576 bytes. A durability policy contains
2..256 targets.
`stream_manifest_sha256` is
`SHA-256("hive/v1/manifest" || canonical_manifest_without_manifest_hash)`;
the exact canonical body is, in order:

```text
manifest_version_be_u16
stream_id
cluster_genesis_hash
payload_format_be_u32
payload_format_version_be_u16
producer_config_sha256
producer_descriptor_len_be_u64 || producer_descriptor
lineage_option
gap_event_stream_id_option
overflow_namespace_sha256_option
deletion_authorizing_store_id_option
durability_policy_option
```

The stored canonical manifest uses the same order but inserts
`stream_manifest_sha256` immediately after `producer_config_sha256`. The omitted
field is filled with the digest above. Target and catalog descriptor hashes use
the domains `"hive/v1/durability-target"` and `"hive/v1/terminal-catalog"`
followed by their exact descriptor bytes. Options are encoded with a one-byte
0/1 tag, byte strings with a big-endian `u64` length, and repeated targets with
a big-endian `u32` count sorted by `target_id`. Option contents use their
declared field order. All other integers are big-endian.
`fixed_encode(StreamHeaderV1)` is the header fields above in order.

A custody-bearing capture stream (formats 1 through 5) must contain its private
overflow namespace, exactly one `deletion_authorizing_store_id`, one durability
policy, and its dedicated gap event stream ID. A format-7 gap stream carries the
same custody fields but omits `gap_event_stream_id`; a derived format-6 stream
omits all custody fields. Reserved format 8 will also be derived if activated;
it is currently rejected by executable validators and has no custody fields.
Policy targets have unique target IDs; copies count
independently only when their `failure_domain_id` values differ. A source
overflow namespace and the source disk never count toward the terminal policy.
The one store ID names the logical dataset; its physical targets do not become
additional ACK peers. Exact secret-free target and catalog descriptors are
retained with deployment history and must reproduce their manifest hashes;
credentials are referenced by deployment configuration and are never embedded.
`minimum_independent_copies` must be at least two and no greater than the number
of distinct declared failure domains, or the manifest is invalid.
The capture stream and its referenced gap stream are separate streams: a
capture manifest cannot name its own `stream_id` as `gap_event_stream_id`, and a
format-7 descriptor cannot name its own stream as `target_stream_id`.

The manifest, descriptor, store binding, durability policy, and lineage are
immutable for the life of a stream. Before the first record, the source durably
creates the manifest beside the spool and fsyncs its namespace; deployment
history retains the same exact bytes. Before starting the first overflow upload,
the source also conditionally writes and verifies the manifest at its immutable
key in the stream's overflow prefix.

The terminal dataset stores and verifies the manifest before protecting any
object. A missing or mismatched manifest fails closed. Any descriptor, store,
policy, or lineage change creates a new stream ID. The overflow manifest remains
while the stream is open or any unretired overflow range exists; after closure
and full retirement, the deployment and terminal copies remain as lineage and
audit metadata even if the source copy is removed.

### Central stream registry

Blockzilla is the sole V1 author of the logical-name registry. It publishes
durable snapshots:

```text
StreamRegistryEntryV1 {
  logical_name: bytes
  stream_generation: u64
  stream_id: [u8; 16]
  stream_manifest_sha256: [u8; 32]
  status: u8  // ACTIVE=1, CLOSED=2, QUARANTINED=3
  successor_stream_id: Option<[u8; 16]>
}

StreamRegistrySnapshotV1 {
  blockzilla_authority_id: [u8; 16]
  registry_generation: u64
  previous_snapshot_sha256: [u8; 32]
  entries: repeated StreamRegistryEntryV1
  snapshot_sha256: [u8; 32]
}

StreamRegistryHeadV1 {
  registry_generation: u64
  snapshot_sha256: [u8; 32]
}
```

`logical_name` is lowercase ASCII matching
`[a-z0-9][a-z0-9._/-]{0,127}`. Entries are sorted by
`(logical_name bytes, stream_generation)` before hashing. Snapshot encoding uses
the option, length, count, and integer rules above. `snapshot_sha256` is
`SHA-256("hive/v1/stream-registry" || canonical_snapshot_without_snapshot_hash)`;
generation zero uses an all-zero `previous_snapshot_sha256`, and every later
snapshot increments `registry_generation` by one and names the exact preceding
digest. `StreamRegistryHeadV1` encodes its fields in the shown order with a
big-endian generation.

Each snapshot is the complete retained registry state, not a delta; duplicate
`(logical_name, stream_generation)` pairs are invalid.
V1 permits at most 65,536 entries and 33,554,432 canonical encoded bytes in one
snapshot. Blockzilla must alert and refuse a transition that would exceed either
bound; it never truncates history. A larger/sharded registry requires a new
protocol version before this finite V1 limit is reached.

For one logical name, `stream_generation` starts at zero and increments by one
for each new immutable manifest. The mapping from
`(logical_name, stream_generation)` to stream ID and manifest digest never
changes. At most one generation is `ACTIVE`; status may move from `ACTIVE` to
`CLOSED` or `QUARANTINED`, and from `CLOSED` to `QUARANTINED`, but never
backward. `successor_stream_id` may move only from absent to the ID of a higher
generation for the same logical name. `ACTIVE` requires an absent successor. In
the first snapshot that publishes a successor link, the successor entry and its
verified manifest are already present, and that manifest's lineage names the
predecessor. Cached committed snapshots therefore never contain a dangling
successor. One stream ID appears in exactly one registry entry.
A successor is discovery metadata, never permission
to migrate a cursor between prefix chains.

Blockzilla conditionally creates and verifies the immutable snapshot under its
digest before it changes the current head. It then uses a linearizable exact
value compare-and-swap from empty to generation zero, or from the prior
`(generation, digest)` head to the new `StreamRegistryHeadV1`. A lost CAS response
is resolved by rereading the head: exact new value means success, exact old value
permits retry, and any other value is a conflict. The current snapshot is never
selected by listing or by taking the numerically largest stored generation.
Blockzilla retains committed snapshots and an independent exact-head checkpoint;
head/checkpoint lag or failure is alertable. After a successful CAS, it durably
backs up the exact new head before advertising that generation. A crash between
CAS and backup recovers from the authoritative head, completes the backup, and
does not expose the new generation first.

Only after the CAS and exact-head backup does Blockzilla serve the head and
snapshot over its
authenticated control-plane endpoint. A reader accepts only the configured
`blockzilla_authority_id`, an exact head match, and an unbroken monotonic digest
chain; a missed generation is fetched from Blockzilla by generation or digest.
Conflicting reuse of a generation fails closed. Sources, exits, and terminal
stores consume this registry but do not gossip, elect, merge, or author it.
Registry status never authorizes ACK, source GC, custody transfer, or canonical
block publication.

| ID | Version 1 exact payload |
| ---: | --- |
| 1 | `YELLOWSTONE_SUBSCRIBE_UPDATE_ZSTD`: one zstd frame containing a deterministic encoding of fields known to the pinned `SubscribeUpdate` protobuf schema |
| 2 | `SOLANA_SHRED_DATAGRAM`: exact received UDP datagram |
| 3 | `SOLANA_REPAIR_SHRED_DATAGRAM`: exact accepted repair datagram in a stream distinct from ordinary Turbine data |
| 4 | `RPC_GET_BLOCK_ZSTD`: one zstd frame of `slot_be_u64 || http_status_be_u16 || body_len_be_u64 || exact_response_body`; request projection is bound by `producer_config_sha256` |
| 5 | `SLOT_STATUS`: `slot_be_u64 || status_u8`, with processed=1, confirmed=2, finalized=3, rooted=4, skipped=5 |
| 6 | `SHRED_BLOCK_OBSERVATION_V1`: the deterministic encoding registered by the [public exit specification](hivezilla-public-exit-protocol.md) |
| 7 | `GAP_EVENT_V1`: the exact `GapEventPayloadV1` encoding below, carried by a dedicated operational-evidence stream |
| 8 | **Reserved, not executable:** `REPLAY_PROJECTION_V1`, the outer-transaction-signature-stripped execution projection specified in [Blockzilla Replay Projection V1](blockzilla-replay-projection-v1.md); activation requires the bounded schema, validator changes, and golden fixtures to land together |

IDs zero and 8 are currently invalid on the executable V1 wire. ID 8 is reserved
only to prevent another draft from claiming it. A new format becomes valid only
with a registered ID/version, semantic bounds, and golden fixture; it is never
inferred from bytes. Each source/normalizer defines its smaller payload and, for
compressed formats, decoded-size limit in its pinned producer descriptor. Every
V1 `RecordV1.payload` is at most 134,217,728 bytes; larger input is rejected
before sequence assignment and creates an explicit adapter loss/failure
condition rather than an untransferable record.

The Yellowstone boundary is not the provider's original wire frame: the
current client re-encodes known protobuf fields and loses unknown fields. Every
format definition must state its exact capture boundary this plainly. For
format 1, the producer descriptor includes the exact protobuf descriptor-set
SHA-256, projected message/field set, zstd parameters, decompressed-byte limit,
and deterministic protobuf serialization rule. Any change creates a successor
stream; a decoder never chooses a locally installed schema by default.

Detected loss is data, not merely an alert:

```text
GapEventPayloadV1 {
  target_stream_id: [u8; 16]
  observed_at: CursorV1
  reason: u16  // UPSTREAM_SEQUENCE_JUMP=1, UDP_DROP_COUNTER=2,
               // LOCAL_SOURCE_LOSS=3, COLD_RECOVERY_INCOMPLETE=4,
               // OPERATOR_DECLARED=5
  expected_source_position: bytes
  observed_source_position: bytes
}

GapEventProducerDescriptorV1 {
  descriptor_version: u16       // 1
  target_stream_id: [u8; 16]
  target_producer_config_sha256: [u8; 32]
  permitted_reasons: repeated u16
  source_position_descriptor: bytes
}
```

`observed_at` is the exact target-stream durable tail at detection: every target
record before it is already durable and no record assigned after detection may
become durable before the gap event. The adapter serializes source-position
checks, target sequence assignment, and this boundary. The cursor must reproduce
the target stream's prefix chain; it is not a cursor in the format-7 event
stream.

The two position fields are length-prefixed canonical bytes defined by the
target adapter's producer descriptor; either may be empty when the transport
does not expose a position, and each is at most 4,096 bytes. The payload encodes
fields in the shown order, uses big-endian `reason`, the fixed cursor encoding,
and a big-endian `u64` length before each position. Unknown reasons, oversized
positions, and trailing bytes are rejected. V1 deliberately carries no
unverifiable evidence hash; adapter-specific evidence needed for interpretation
is inside these canonical position bytes and their pinned descriptor.

A capture manifest names one separate format-7 stream. That event stream uses
the same custody protocol and terminal policy, but its own manifest has no
`gap_event_stream_id`, avoiding recursion. Its `producer_descriptor` is exactly
the `GapEventProducerDescriptorV1` above: fields use the shown order and
big-endian integers; reasons use a big-endian `u32` count followed by sorted,
unique `u16` values; and the final bytes use a big-endian `u64` length. The
reason vector has 1..5 entries drawn only from the registered reasons, and the
canonical secret-free position descriptor is 1..65,536 bytes. It defines both
position fields, including counter-instance/delta representation and when empty
is valid; every event reason must appear in that vector. The target must be a
custody-bearing format 1..5 stream, and its ID/config digest and cluster must
match the capture stream that references this gap stream. The descriptor does
not embed the target manifest hash, so the gap stream can be provisioned first
without a hash cycle.
Custody treats `source_position_descriptor` and the two position values as
bounded opaque canonical bytes; only an adapter-specific operations decoder may
interpret them. An unknown descriptor is retained and reported as unsupported,
never guessed or rejected as raw evidence.
When a discontinuity is detected,
the producer durably appends the gap event before resuming any input that can be
paused or committing subsequent target progress. UDP capture reserves space for
the event; if even that write fails, it emits the explicit loss alert and fails
closed for subsequent durable progress. Alerts never substitute for the durable
event. This is static configured reporting, not membership, peer discovery, or
P2P.

## 3. Prefix and cursor

Sequences start at zero and are contiguous. They include duplicates and
conflicts exactly as captured. A Solana slot, shred index, or blockhash is a
secondary lookup key and is never a delivery or deletion cursor.

```text
P(0) = SHA-256("hive/v1/stream" || fixed_encode(StreamHeaderV1))

P(n + 1) = SHA-256(
  "hive/v1/record" || P(n) || n_be_u64 || payload_len_be_u64 || payload
)

RecordV1 {
  sequence: u64
  payload: bytes
  prefix_hash: [u8; 32]  // P(sequence + 1)
}

CursorV1 {
  next_sequence: u64     // covers [0, next_sequence)
  prefix_hash: [u8; 32]  // P(next_sequence)
}
```

This one chain detects changed bytes, sequence reuse, insertion, removal, and
reordering. The exact payload may itself be compressed; every tier and consumer
must reproduce the same `RecordV1` chain.

## 4. Journal

```text
SegmentHeaderV1 {
  magic: "HIVESEG1"
  stream: StreamHeaderV1
  first_sequence: u64
  previous_prefix_hash: [u8; 32]
}

repeated FrameV1 {
  magic: "HFR1"
  payload_len: u64
  payload: bytes
  prefix_hash: [u8; 32]
}

SegmentFooterV1 {
  magic: "HEND"
  next_sequence: u64
  prefix_hash: [u8; 32]
}
```

Integers are big-endian. Frame sequence is
`first_sequence + frame_index`. The bytes above are fixed. Implementations may
add sidecar indexes or checksums; changing inline framing requires a new segment
version and does not change record identity.

Journal invariants:

- record bytes reach durability before their cursor checkpoint;
- recovery recomputes frames and rejects a checkpoint ahead of the validated
  tail; it may durably advance a checkpoint that lagged valid frames;
- capture progress is exposed only through the durable checkpoint;
- a partial final frame in the active segment may be truncated under the
  exclusive writer lock; invalid interior data fails closed;
- sealing fsyncs the footer before the segment becomes immutable;
- durability includes filesystem namespace state: creating, linking, renaming,
  or unlinking a manifest, segment, receipt, catalog, or checkpoint fsyncs the
  affected file and parent directory in the required order, or uses an
  equivalent crash-atomic storage transaction;
- no checkpoint may reference a segment name that has not crossed that durable
  namespace boundary, and no deletion may precede its durable replacement or
  retirement record;
- payload, segment, journal, memory-queue, and filesystem-reserve limits are
  explicit.

## 5. Node-owned cloud overflow

Every source node has one private bucket or IAM-isolated namespace. Each stream
has a separate immutable prefix within it. The bucket extends that node's
logical spool only while its terminal consumer is behind.

```text
OverflowObjectV1 {
  stream_id: [u8; 16]
  start: CursorV1
  end: CursorV1
  encoded_len: u64
  encoded_sha256: [u8; 32]
  object_version: Option<bytes>
}
```

Its canonical metadata encoding is the fields above in order: fixed stream ID,
fixed cursor encodings, big-endian `encoded_len`, the 32 digest bytes, then a
one-byte 0/1 option tag. A present `object_version` is encoded as
`1 || version_len_be_u64 || version`; it is 1..4,096 opaque bytes. `None` is the
single byte `0`. Empty present versions, longer versions, invalid option tags,
truncation, and trailing bytes are rejected. The provider version remains
receipt metadata and is not included in the deterministic object key.

In V1 an overflow object is exactly one complete sealed journal segment:
header, all frames, and footer. `encoded_len` and `encoded_sha256` cover those
exact bytes, while `start` and `end` reproduce the header/footer chain. Segment
size is a bounded deployment setting, not a protocol identity. Its deterministic
key is
`hive-overflow/v1/<stream-id>/<start>-<end>-<sha256>.hseg`, with fixed lowercase
hex encoding: IDs and digests use their full bytes, while start/end are their
16-hex-digit `next_sequence` values. An object version is an opaque provider
receipt, not record or object identity. Providers without versions use `None`;
ETags are never treated as content checksums unless the provider explicitly
defines them as such. The manifest object key is
`hive-overflow/v1/<stream-id>/manifest-<manifest-sha256>.manifest`.

Before evicting covered bytes from local disk, the source must:

1. seal and fsync the exact segment;
2. parse its frames/footer, reproduce `end` from `start`, and compute its digest;
3. conditionally create the deterministic immutable key and verify its length
   and provider-attested end-to-end SHA-256; if the provider cannot attest that
   digest, read the complete object back and verify it; record an object version
   when one exists but never require one for portability;
4. fsync an overflow receipt and the local range-to-object catalog containing
   the key, optional version, length, digest, and boundary cursors;
5. persist `local_evicted_through` and its prefix anchor before unlinking the
   local segment.

Spill and local eviction proceed oldest-first, so `local_evicted_through` is one
contiguous cursor rather than an arbitrary set of holes.

This is tier movement, not logical retirement. The source must continue serving
the same stream across local and cloud locations. A mutable object head,
listing, successful upload command, or process heartbeat is insufficient.

Spill begins at a soft time-to-full watermark with enough reserve for an object
store outage; waiting for the filesystem to become full is a correctness bug.
Cloud overflow entry, recovery, upload failure, verification failure, and
cloud-only backlog are alert transitions.

Overflow objects have no independent lifecycle expiry. They remain until a
terminal consumer ACK covers them. Deletion credentials should be narrower
than upload/read credentials. HiveSync V1 does not expose source-bucket
credentials or object locators to the consumer.

Spill and retirement share one fenced metadata WAL and lock. Starting a spill
records its range and generation. After upload verification, the source must
recheck under that lock that the range is still live before committing the
overflow receipt. Retirement first invalidates covered spill intents and
persists `retired_through`; only then may deletion run. A late upload for an
invalidated or retired range is an orphan to delete, never a reason to recreate
the catalog entry or move a cursor backward. Recovery reconciles spill intents,
catalog entries, local files, and cloud objects under the same rule.

### Static cold recovery

Cloud-only overflow remains recoverable after loss of the source host without
peer discovery or an election. An operator starts a statically configured
replacement with the exact manifest/deployment copy and credentials for that
one source namespace, proves the old writer stopped, and opens the old stream in
read-only recovery mode. The replacement:

1. verifies the manifest against `stream_manifest_sha256` and the immutable
   manifest object in the overflow prefix;
2. enumerates candidate deterministic object keys, then downloads and validates
   every selected segment's header, length, digest, frames, footer, and cursor
   chain; listing is discovery only and never proof of completeness;
3. rebuilds a local receipt/catalog WAL from the maximal non-conflicting
   contiguous chains and fsyncs that state before serving them;
4. serves only verified ranges to the manifest's configured terminal store and
   applies its ACK/retirement rules normally.

An overlap with different bytes, an interior hole, a missing manifest, or an
unverifiable object quarantines the affected chain. Because local-only active
tail records may have died with the host, cold recovery never resumes writing
the old stream ID. New capture creates a new manifest with
`SOURCE_HOST_LOSS` lineage, the largest verified old cursor as the predecessor
last-known cursor, and `GAP_POSSIBLE` unless an external source position proves the
boundary contiguous. It also writes `COLD_RECOVERY_INCOMPLETE` when a hole is
known. This preserves recoverable cloud-only data without pretending that a
bucket listing proves an unknown local tail never existed.

## 6. HiveSync live-first resume and bulk backfill

HiveSync is a trusted-node storage protocol pinned to one source stream. The
terminal raw consumer connects outbound from the storage side. Public exit
subscribers have no terminal store ID, send no custody ACK, and never authorize
garbage collection.

A reconnect uses two independently budgeted lanes:

```text
consumer protected cursor = C
validated sealed tail established during open = T

bulk lane: [C, T)
live lane: [T, infinity)
```

```text
OpenV1 {
  stream_id: [u8; 16]
  terminal_store_id: [u8; 16]
  protected_cursor: Option<CursorV1>  // C, or None for P(0)
}

ResumeV1 {
  stream: StreamHeaderV1
  session_id: [u8; 16]       // random, transport-only fencing token
  first_available: CursorV1
  bulk_start: CursorV1        // validated C
  cutover: CursorV1          // T; also the live-lane start
  max_record_bytes: u64
  max_chunk_records: u32
  max_parallel_fetches: u16
}

FetchRangeV1 {
  session_id: [u8; 16]
  cutover: CursorV1
  first_sequence: u64        // inclusive
  next_sequence: u64         // exclusive; <= cutover.next_sequence
}

TransferChunkCommitV1 {
  start: CursorV1
  end: CursorV1
  encoded_len: u64
  encoded_sha256: [u8; 32]
}

FetchRangePartV1 = ChunkBytes(bytes)
                 | Commit(TransferChunkCommitV1)
                 | Error(ErrorV1)

AckV1 {
  stream_id: [u8; 16]
  terminal_store_id: [u8; 16]
  stream_manifest_sha256: [u8; 32]
  policy_id: [u8; 16]
  protected_cursor: CursorV1
}

AcceptedAckReceiptV1 {
  receipt_generation: u64
  previous_receipt_sha256: [u8; 32]
  stream: StreamHeaderV1
  authenticated_peer_id: bytes
  ack: AckV1
  receipt_sha256: [u8; 32]
}

SourceRetirementCheckpointV1 {
  stream: StreamHeaderV1
  retired_through: CursorV1
  accepted_ack_receipt_sha256: [u8; 32]
}

ErrorV1 {
  code: UNAUTHORIZED | CURSOR_MISMATCH | PREFIX_RETIRED |
        RECOVERY_INCOMPLETE | CHUNK_MISMATCH | TEMPORARILY_UNAVAILABLE |
        LIVE_BACKPRESSURE | LIMIT | STALE_SESSION
}
```

`authenticated_peer_id` is the exact stable, secret-free principal ID produced
by the static mTLS authorization mapping and is 1..1,024 bytes. Receipt
generation starts at zero; generation zero has an all-zero previous digest and
each later generation increments by one and names the prior receipt digest.
`receipt_sha256` is
`SHA-256("hive/v1/accepted-ack" || canonical_receipt_without_receipt_hash)`.
Receipt/checkpoint fields use the shown order, big-endian integers, fixed nested
encodings, and a big-endian `u64` length for the peer ID.
The nested stream and ACK must match, and the ACK cursor must strictly advance
the preceding accepted receipt; an exact duplicate is a no-op that reuses the
existing receipt. A source validates the ACK first, then fsyncs this receipt.
Only afterward may it advance and fsync a `SourceRetirementCheckpointV1` whose
cursor is no later than that receipt's protected cursor. This checkpoint is the
retirement anchor; neither an in-memory ACK nor an unlinked WAL position is one.
Its stream must equal the referenced retained receipt's stream, its receipt
digest must recompute exactly, and `retired_through` must validate on that
stream's prefix chain. With no prior checkpoint, the lower bound is `P(0)`;
otherwise the prior cursor must be no later than the new one. In all cases:

```text
prior_retired_through <= retired_through <= receipt.ack.protected_cursor
```

Construction and recovery enforce every predicate before GC. Absence of a
retirement checkpoint means `P(0)` and authorizes no record deletion.

`OpenV1`, `FetchRangeV1`, and `AckV1` flow consumer to source; `ResumeV1` and
live records flow source to consumer. The transport has one long-lived
live/control stream plus bounded parallel `FetchRange` calls. The consumer
partitions the numeric sequence interval `[C.next_sequence,T.next_sequence)`
into non-overlapping requests of at most `max_chunk_records`; no server-side
resume plan or unbounded descriptor list exists. The advertised record/count
limits bound individual records and the numeric range. The canonical body is
streamed and has no single-message transport-size requirement; its checked
`u64` length is bounded by the negotiated record count and record-size limits,
while every protobuf part obeys the per-message limits below.

Every range response streams this canonical body:

```text
ChunkHeaderV1 {
  magic: "HIVECHK1"
  stream_id: [u8; 16]
  start: CursorV1
}

repeated FrameV1

ChunkFooterV1 {
  magic: "HIVEEND1"
  end: CursorV1
}
```

Integers and cursors use the fixed encodings above. Frame sequence is inferred
from `start.next_sequence`; the body contains exactly
`end.next_sequence - start.next_sequence` frames and reproduces `end` from
`start`. `encoded_len` and `encoded_sha256` cover the entire header, frames, and
footer. A range may slice an origin journal segment only at record boundaries;
payload and prefix-hash bytes are unchanged.

The source streams a chunk inline, reading and reframing records from local disk
or its temporary cloud object as needed. It never returns the differently
encoded overflow segment itself and need not buffer or pre-hash the complete
response. One or more `ChunkBytes` parts carry the canonical body; exactly one
final `Commit` part carries its boundary cursors, total length, and SHA-256
computed while streaming. The commit part is not included in that digest. A
missing commit, bytes after commit, boundary/length/digest mismatch, or RPC error
invalidates the entire attempt and the consumer discards its partial staging.
This trailer shape avoids depending on provider versions or transport metadata.
`FetchRangeV1` is idempotent: repeating it after a temporary cloud failure
returns the same committed body or an explicit retryable
`TEMPORARILY_UNAVAILABLE`; it never advances ACK. Final protobuf field numbers
and decoding limits are fixed below.

### HiveSync V1 gRPC binding

The transport carries the canonical fixed encodings above inside protobuf
`bytes` fields; protobuf serialization itself is not hashed or persisted. The
service and field numbers are:

```proto
syntax = "proto3";
package hivezilla.sync.v1;

service HiveSyncV1 {
  rpc Sync(stream SyncClientFrameV1) returns (stream SyncServerFrameV1);
  rpc FetchRange(FetchRangeRequestV1) returns (stream FetchRangePartWireV1);
}

message OpenWireV1 {
  bytes stream_id = 1;               // exactly 16 bytes
  bytes terminal_store_id = 2;       // 16
  bytes protected_cursor = 3;        // empty/omitted=None; otherwise 40 bytes
}
message AckWireV1 {
  bytes stream_id = 1;               // 16
  bytes terminal_store_id = 2;       // 16
  bytes stream_manifest_sha256 = 3;  // 32
  bytes policy_id = 4;               // 16
  bytes protected_cursor = 5;        // 40
}
message SyncClientFrameV1 {
  oneof frame { OpenWireV1 open = 1; AckWireV1 ack = 2; }
}

message ResumeWireV1 {
  bytes stream = 1;                  // 118 fixed StreamHeaderV1 bytes
  bytes session_id = 2;              // 16 random bytes
  bytes first_available = 3;         // 40
  bytes bulk_start = 4;              // 40
  bytes cutover = 5;                 // 40
  uint64 max_record_bytes = 6;
  uint32 max_chunk_records = 7;
  uint32 max_parallel_fetches = 8;   // decoded value must fit u16
}
message RecordWireV1 { bytes record = 1; }
enum HiveSyncErrorCodeV1 {
  HIVE_SYNC_ERROR_UNSPECIFIED = 0;
  UNAUTHORIZED = 1;
  CURSOR_MISMATCH = 2;
  PREFIX_RETIRED = 3;
  RECOVERY_INCOMPLETE = 4;
  CHUNK_MISMATCH = 5;
  TEMPORARILY_UNAVAILABLE = 6;
  LIVE_BACKPRESSURE = 7;
  LIMIT = 8;
  STALE_SESSION = 9;
}
message HiveSyncErrorWireV1 { HiveSyncErrorCodeV1 code = 1; }
message SyncServerFrameV1 {
  oneof frame {
    ResumeWireV1 resume = 1;
    RecordWireV1 record = 2;
    HiveSyncErrorWireV1 error = 3;
  }
}

message FetchRangeRequestV1 {
  bytes session_id = 1;              // 16; must name the active Sync session
  bytes cutover = 2;                 // 40
  uint64 first_sequence = 3;
  uint64 next_sequence = 4;
}
message TransferChunkCommitWireV1 {
  bytes start = 1;                   // 40
  bytes end = 2;                     // 40
  uint64 encoded_len = 3;
  bytes encoded_sha256 = 4;          // 32
}
message FetchRangePartWireV1 {
  oneof part {
    bytes chunk_bytes = 1;
    TransferChunkCommitWireV1 commit = 2;
    HiveSyncErrorWireV1 error = 3;
  }
}
```

The first client frame is exactly one `open`; later client frames are ACKs. A
successful open receives exactly one initial `resume`; a rejected open receives
exactly one `error` and closes. Later Sync frames are records or one terminal
error. A FetchRange response has one or more byte parts followed by exactly one
commit, or exactly one error and no bytes/commit. `session_id` is CSPRNG-generated,
unguessable, valid only while that Sync session is active, and fences parallel
range calls after a reconnect. A stale token returns `STALE_SESSION`; source
unavailability returns the already defined exact error code.

Here, protobuf message size is the decompressed serialized protobuf byte length
presented to the decoder, excluding the gRPC length prefix and HTTP/2 framing.
V1 hard limits are: `RecordV1.payload <= 134,217,728` bytes; a control protobuf
message at most 65,536 such bytes; a `RecordWireV1` at most 134,217,781 bytes; a
`SyncServerFrameV1` carrying it at most 134,217,786 bytes; each non-empty
`chunk_bytes` part at most 4,194,304 bytes; `1 <= max_chunk_records <=
1,048,576`; and
`1 <= max_parallel_fetches <= 64`. `max_record_bytes` is the maximum complete
fixed-encoded `RecordV1` length and must be between 48 and payload plus its 48
framing bytes, inclusive. A deployment may advertise smaller values within
these ranges. An absent decoded
oneof, zero or unknown enum, wrong fixed length, a reversed/empty fetch
range, a range larger than the advertised record count, or any decoded value
above these limits is rejected before unbounded allocation. A
`FetchRangePartWireV1` carrying maximum chunk bytes is at most 4,194,309
protobuf bytes; the streamed response as a whole may be larger and is bounded
by the negotiated record count/size plus local transfer budgets. Standard
proto3 last-one-wins parsing applies
to repeated wire occurrences of oneof fields; protobuf bytes are not hashed or
used as identity. Golden fixtures cover every decoded message and enum.
The transport applies envelope and repeated-field limits to the raw
decompressed protobuf bytes before generated Tonic/Prost decoding. A typed
handler that first decodes under the large record ceiling and only later learns
that the oneof contains a control message does not satisfy the control limit.

Rules:

1. mTLS identity plus static configuration authorizes
   `(stream, terminal_store_id)`. A store ID names the receiving dataset, not a
   process. Only the stream's configured `deletion_authorizing_store_id` may
   send `AckV1`. One active live session is allowed for that pair; a new
   `OpenV1` fences the prior session. `OpenV1.protected_cursor` is also the
   authenticated retransmission of the terminal dataset's latest cumulative
   ACK. `None` means exactly `P(0)` and is accepted only before this store has an
   accepted ACK beyond `P(0)`; otherwise it is `CURSOR_MISMATCH`, never an
   implicit reset. A present cursor below the latest accepted ACK is likewise
   rejected. If it is ahead, the source applies every `AckV1` validation and
   fsyncs the equivalent `AcceptedAckReceiptV1`—using the active manifest's
   manifest/policy IDs—before cutover or `ResumeV1`. Equality is idempotent.
2. The source briefly holds the shared writer/cutover/spill/retirement lock.
   Under that lock it revalidates `C` against the current `first_available` and
   rejects a wrong prefix or future cursor. It finishes and fsyncs every complete
   assigned frame, truncates any incomplete final frame, seals and fsyncs the
   old segment, and defines `T` as that validated footer cursor.
   The checkpoint is durably advanced to exactly `T`; the new segment header is
   created with `first_sequence=T.next_sequence` and
   `previous_prefix_hash=T.prefix_hash`, and both its file and namespace are
   durable before live replay from `T` is installed. Only then does the source
   release the lock and send `ResumeV1`. Unassigned in-memory input remains
   queued for the new segment. Thus a valid frame ahead of a lagging checkpoint
   is included once in `[C,T)`, never omitted or assigned again. If any requested
   record is retired it returns `PREFIX_RETIRED`; a cold-recovery interior hole
   returns `RECOVERY_INCOMPLETE`.
3. The live lane starts inclusively at `T` and serves every later durable
   record. Records appended after the lock is released enter the new active
   segment and cannot fall between the fixed bulk range and live replay.
4. The consumer divides `[C,T)` into exact, non-overlapping numeric ranges and
   fetches them oldest-first with bounded parallelism. The source returns the
   requested start/end cursors; adjacent chunks must join by equal boundary
   cursors. Large ranges amortize transfer overhead, but range completion has no
   ACK semantics of its own.
5. The consumer may durably stage live and bulk ranges out of order. It verifies
   every frame and chain boundary and tracks one contiguous verified frontier
   from `C`; staging alone never advances its protected/Open/ACK cursor.
6. The consumer writes completed bulk ranges and live records as the
   `TerminalRawObjectV1` objects defined below, possibly out of order. Live
   objectization has configured maximum records, encoded bytes, open age, and
   total staging bytes. Reaching any object limit seals it; reaching the staging
   limit stops that live session rather than dropping a record. Only objects
   that complete the terminal commit order may advance the protected cursor.
   A completed later range or live object cannot jump a hole.
7. The source first requires the ACK's stream ID, store ID, manifest hash, and
   policy ID to equal the active stream manifest. It then accepts only a monotonic cursor no
   later than its validated tail whose prefix hash exactly matches its chain.
   It fsyncs the exact `AcceptedAckReceiptV1` containing the authenticated peer
   identity before advancing the `SourceRetirementCheckpointV1`, and may retire
   only fully covered objects/segments. An identical ACK is a no-op; a lower or
   same-sequence/different-prefix ACK is rejected.
8. ACKs may advance after every newly contiguous protected object. Once
   backfill closes the gap, the cursor may jump through already-protected live
   objects. An ACK at or beyond `T` is the only bulk-completion signal.
9. Disconnect loses no source data. Reconnect creates a new `T` from the
   consumer's last protected cumulative cursor. ACK or response loss is healed
   by the authenticated cursor retransmission in `OpenV1`; inclusive record
   replay remains safe, and the previously unserved live suffix becomes part of
   the new fixed bulk range.
10. The live send window and every queue are byte-bounded. If the consumer
    cannot drain the live window, the source ends that session with
    `LIVE_BACKPRESSURE` without advancing ACK; it never drops a record while
    continuing the same session. Live, bulk-local, and bulk-cloud lanes have
    separate concurrency, CPU, disk, and byte budgets, so bulk recovery cannot
    backpressure capture or starve live delivery.

The configured terminal dataset's first use of a newly bound stream opens at
`P(0)`. `first_available` is the earliest cursor recoverable from the source's
combined local and cloud tiers. If a process for that same logical dataset has
lost a local checkpoint and asks below a retired prefix, the source returns
`PREFIX_RETIRED`; the process rebuilds its cursor from its permanent terminal
objects/index. This is not permission to bootstrap a different store ID. Same
sequence/same prefix is idempotent; same sequence/different prefix is fatal
quarantine.

## 7. Terminal custody and source retirement

An ACK transfers responsibility. In V1, the logical terminal dataset is a
permanent cloud-backed raw archive, distinct from every source overflow bucket
and from compact Archive V2. One logical store ID may use several physical
targets, but it remains the only ACK identity seen by the source.

Every permanent raw object is self-describing:

```text
TerminalRawHeaderV1 {
  magic: "HIVERAW1"
  manifest: StreamManifestV1
  start: CursorV1
}

repeated FrameV1

TerminalRawFooterV1 {
  magic: "HIVEREND1"
  end: CursorV1
}

TerminalRawObjectV1 = TerminalRawHeaderV1 || repeated FrameV1 ||
                      TerminalRawFooterV1

TerminalCopyReceiptV1 {
  terminal_store_id: [u8; 16]
  policy_id: [u8; 16]
  target_id: [u8; 16]
  failure_domain_id: [u8; 16]
  stream_id: [u8; 16]
  start: CursorV1
  end: CursorV1
  object_key: bytes
  object_version: Option<bytes>
  encoded_len: u64
  encoded_sha256: [u8; 32]
  verification: u8  // PROVIDER_SHA256=1, FULL_READBACK_SHA256=2
}

TerminalRangeIndexV1 {
  stream_id: [u8; 16]
  start: CursorV1
  end: CursorV1
  encoded_len: u64
  encoded_sha256: [u8; 32]
  copies: repeated TerminalCopyReceiptV1
}

TerminalCursorCheckpointV1 {
  terminal_store_id: [u8; 16]
  stream_id: [u8; 16]
  stream_manifest_sha256: [u8; 32]
  policy_id: [u8; 16]
  protected_through: CursorV1
}
```

The terminal structures use the shown field order, big-endian integers, fixed
cursor/ID/hash encodings, a one-byte `0`/`1` option tag, a big-endian `u64`
length before `object_key` and a present `object_version`, and a big-endian
`u32` count before `copies`. `verification` is its registered one-byte value.
`TerminalRawHeaderV1` concatenates its magic, the exact self-delimiting stored
`StreamManifestV1` encoding, and `start`; it adds no second manifest length or
hash wrapper. Frames are the exact journal `FrameV1` encoding. Decoders consume
one complete embedded manifest and reject invalid option tags, unknown
verification values, non-canonical copy order, truncation, and trailing bytes.
Every externally supplied terminal raw object is rejected against configured,
finite, nonzero byte and record-count limits before frame payloads are decoded;
an unbounded terminal-object decoder is not part of the V1 runtime contract.
The decoder budget is a read-side resource ceiling and must be at least the
producer's objectization byte cap; it does not relax the separate requirement
below that the producer cap accommodate one maximum admitted record plus
framing.

`TerminalRangeIndexV1.copies` is sorted by `target_id` and contains exactly one
receipt per target ID. Before a receipt counts, its store ID, policy ID, stream
ID, start/end cursors, encoded length, and encoded SHA-256 must equal the
manifest, index, and exact object. Its `target_id` must name one
`DurabilityTargetV1` in the immutable policy and its `failure_domain_id` must
equal that target's configured failure domain; an embedded receipt value cannot
relabel a target into another domain. Object key/version and verification method
are target-specific evidence. Duplicate, unknown, or mismapped targets and
unknown verification values are rejected.

Committed range indexes for one stream are non-empty and do not partially
overlap. An exact repeat of the same start/end/length/digest is idempotent and
reuses the index; any other overlap is quarantined even if later prefix bytes
appear to join. This leaves one unambiguous ordered object tiling for replay and
protected-prefix reconstruction.

Every terminal object key is 1..4,096 bytes and every present provider version
is 1..4,096 bytes. Longer locators require a new protocol version rather than an
implementation-specific truncation.

The object validates its embedded manifest hash, contains a non-empty contiguous
range, and reproduces its footer from its header. Its length/digest cover the
exact header, frames, and footer. Its deterministic key is
`hive-raw/v1/<stream-id>/<start>-<end>-<sha256>.hraw`. Keys use fixed lowercase
hex with the same cursor-number rule as overflow keys. An optional provider
version is recorded when available but is not object identity; an immutable
conditional create plus key, length, and SHA-256 is the portable identity.
Provider ETags do not count as SHA-256 unless explicitly documented as such.

For each object the terminal commits in this order:

1. build and seal the exact object from verified contiguous records, then parse
   it locally and reproduce `end` from `start`;
2. conditionally create the deterministic key on policy targets whose distinct
   failure domains can satisfy `minimum_independent_copies`;
3. independently verify each target's exact length and provider-attested
   end-to-end SHA-256, or perform a complete readback and SHA-256 when such an
   attestation is unavailable;
4. transactionally persist the copy receipts and range index, including each
   key, optional version, length, digest, and verification method;
5. only after the index proves enough verified copies in distinct policy
   failure domains, recompute the largest gap-free protected prefix and durably
   persist `TerminalCursorCheckpointV1`;
6. send `AckV1` for that checkpoint. A successful PUT, one physical copy, local
   staging, or an index without verified objects never authorizes ACK.

Before step 3 performs provider I/O, the terminal preflights the complete index:
exact object length/digest, bounded object parse, manifest/range bindings, every
policy target and failure-domain mapping, and sufficient distinct domains must
all pass. A late-invalid receipt or known durability deficit therefore performs
zero provider readbacks.

The receipt/index transaction and cursor checkpoint may use one transactional
database or an ordered WAL plus crash-durable namespace operations. The cursor
is a derived checkpoint, never the sole evidence of protection. On restart the
terminal validates the manifest, store ID, and policy; treats its cursor as a
hint; validates indexed receipts against immutable object identity; and
recomputes the largest contiguous protected prefix. If the index is missing, it
may scan the statically configured deterministic target prefixes, parse the
self-describing objects, verify enough independent copies, and rebuild the
index. Valid objects beyond the old cursor may then advance it. A missing,
corrupt, or under-replicated object at or below a previously ACKed cursor is a
custody incident: the terminal stops ACKing and fails closed rather than
silently lowering, resetting, or changing its store ID.

After ACK, the terminal continuously audits and repairs policy copies from a
verified survivor. A later target deficit does not revoke the historical ACK or
move a cursor backward, but it is an immediate durability incident and alert.

The terminal ingest process may stage locally, but an ACK cursor is always the
end cursor of a protected `TerminalRawObjectV1`; it never names a partial,
open, or merely uploaded object. Bulk and live use the same object and commit
rules. Live objects are sealed at the first configured record, byte, or age
limit, so low traffic cannot leave an unbounded unprotected tail; total live
staging is also byte-bounded. These four limits are finite and nonzero, and the
object byte limit must accommodate one maximum-size record plus framing.

The dataset remains responsible for exact per-stream replay indefinitely. It
may use storage-layer deduplication, but every indexed object key must remain
readable with the same bytes and continue satisfying its policy receipts. It
must retain the mappings needed to reproduce every producer stream, sequence,
and prefix. Compact workers and raw-history exits read this terminal raw archive;
Archive V2 cannot replace it because compaction discards raw ordering,
duplicates, repair traffic, coding shreds, and losing forks.

A transient processor or compact worker is not a terminal consumer and cannot
ACK. The manifest's terminal store ID and durability policy never rotate within
one stream. A dataset reset or policy change creates a new logical store ID,
closes the old source stream, and starts a new stream with
`TERMINAL_STORE_ROLLOVER` lineage and the new immutable binding. When the old
dataset remains valid, it stays the only authority for its old stream's
remaining spool. If it was lost or reset, its prior receipts are audit evidence
only, its remaining source spool is held, and the loss is a custody incident.
The old identity cannot ACK the new stream. Reassigning responsibility for
already-retired old-stream data is not a V1 operation.

Each raw stream has exactly one configured deletion-authorizing terminal store
in V1. Its required policy targets are evidence behind that one ACK and never
ACK individually; copies outside the immutable policy and public subscribers
have no source-cleanup authority. Supporting multiple independent ACK
predicates or transferring responsibility is deferred. A source's own temporary
overflow bucket never counts as a terminal target.

A sealed origin segment or overflow object may be logically retired only when:

1. the configured terminal store's cumulative ACK covers its `end` cursor and
   exactly matches the stream manifest's store and policy IDs;
2. that ACK receipt, including the manifest hash, is fsynced locally;
3. the `SourceRetirementCheckpointV1`, including that receipt's digest and the
   exact `retired_through` prefix anchor, is fsynced before deletion;
4. local files and overflow objects are deleted or queued for retry without
   moving `retired_through` backward;
5. recovery distinguishes `LOCAL`, `CLOUD_OVERFLOW`, and
   `RETIRED_AFTER_ACK` from missing or corrupt data.

Cloud deletion failure retains an unnecessary copy and alerts; it does not
invalidate the already durable terminal copy. ACK loss is harmless because the
consumer replays its protected cursor. Normalization, observation processing,
candidate acceptance, compaction, and canonical archive progress never count
as raw custody.

### Minimum crash-durable state

A conforming source persists, per stream: the exact manifest and producer
descriptor; segment namespace and durable tail cursor; spill intents and
generations; overflow receipts and range catalog; `local_evicted_through`;
the complete digest-chained accepted-ACK receipts; the latest
`SourceRetirementCheckpointV1`; and enough pending deletion state to reconcile
local and cloud objects. The immutable deployment
copy persists the manifest, target/catalog descriptors, overflow namespace, and
authorized mTLS identity mapping. Recovery validates all cursor/hash relations
and gives `retired_through` precedence over stale spill or file evidence; it
never infers that missing data was retired without the durable retirement
anchor.

A conforming terminal persists, per stream: the exact manifest and durability
descriptors; logical store ID; every committed `TerminalRangeIndexV1` and its
copy receipts; and `TerminalCursorCheckpointV1`. Raw objects are the primary
data evidence and make the index/cursor rebuildable. Neither side must persist
sessions, in-memory queues, FetchRange attempts, or the last ACK transmission;
those are safely reconstructed by inclusive replay. Every required mutation
uses the ordered fsync/transaction boundaries above.

Blockzilla persists its authority ID, every committed registry snapshot, and
the linearizable current generation/digest head. An independent exact-head
checkpoint may lag only for an unadvertised CAS and may be refreshed only from
the authoritative head before serving it. An intact snapshot ahead of that head
is uncommitted and never advances recovery by listing or inference; a head may
never name unverified bytes or reuse a generation with different bytes.

## 8. Operational invariants

Emit entry and recovery alerts for:

- source down, detected source gap/drop, or journal corruption;
- manifest/descriptor mismatch, gap-event persistence failure, or cold-recovery
  quarantine;
- registry generation stall, predecessor/digest mismatch, conflicting
  logical-name generation, or exact-head checkpoint lag/loss;
- local time-to-full soft and critical watermarks;
- overflow start, cloud-only ranges, upload/auth/quota/verification failure,
  and cloud deletion failure;
- terminal consumer disconnect, oldest-unacknowledged age/bytes, and ACK stuck;
- live-lane lag, bulk-local and bulk-cloud throughput, staged-live bytes, and
  backfill ETA/stall;
- terminal raw-object/index upload or verification failure, physical-target
  failure-domain deficit, reindex failure, reset/loss, and protected-cursor
  stall.

Neither pressure nor alert-delivery failure weakens capture, ACK, or deletion
eligibility. If local and cloud capacity cannot preserve a record, the source
fails closed where the upstream can backpressure and emits an explicit loss
condition where it cannot.

## 9. Conformance

V1 fixtures must prove:

1. canonical manifest/descriptor/policy encoding, manifest hashing,
   format-specific required fields, and empty/single/multi-record prefix hashes;
2. registry snapshot/head hashing, monotonic generation, immutable logical-name
   mapping, status/successor transitions, exact-value CAS including a lost
   success response, crash recovery from the exact checkpoint without listing,
   and rejection of a broken predecessor chain or wrong Blockzilla authority;
3. lineage preserves the predecessor cursor and forces a new stream ID for WAL
   loss, unsafe handoff, source-host loss, and terminal-store rollover;
4. a detected discontinuity is durably captured in the configured gap stream,
   while inability to persist that event prevents silent durable progress;
5. torn-tail recovery, a valid frame ahead of its checkpoint, checkpoint crash
   ordering, and interior corruption failure;
6. crashes after segment create, rename, checkpoint, receipt, unlink, and parent
   namespace sync never expose a cursor whose bytes cannot be reopened;
7. exclusive-writer rejection and new stream identity after an unsafe handoff;
8. verified overflow upload, optional-version providers, local eviction,
   cloud-only restore, corrupt overflow rejection, and recovery at every spill
   intent/receipt/eviction crash boundary;
9. static cold recovery rebuilds a read-only catalog from self-describing cloud
   segments, rejects holes/conflicts, and never appends the old stream ID;
10. cutover derives `T` from the sealed footer after absorbing valid frames ahead
   of a lagging checkpoint, with no omission or sequence reuse while records
   arrive during cutover;
11. arbitrary record-boundary slices, parallel retries, and adjacent chunk joins
    converge to record-by-record replay; a missing or wrong final chunk `Commit`
    invalidates all partial bytes;
12. out-of-order bulk/live objects cannot advance the protected cursor or ACK
    across a hole;
13. one verified terminal copy, two receipts in one failure domain, an unknown
    or policy-mismapped target/failure-domain receipt, an unattested checksum, a
    mismatched embedded manifest, a partial range-index overlap, or a durable
    index without its objects cannot authorize ACK;
14. crashes before and after every terminal object, receipt/index, cursor, and
    ACK boundary either rebuild the same protected prefix or fail closed;
15. loss of the terminal index/cursor can be reindexed from enough independently
    verified self-describing objects, while loss below a prior ACK is a custody
    incident rather than a silent cursor rollback;
16. a wrong-prefix, future, unauthorized, stale-store, wrong-policy, or reset
    ACK is rejected and never advances retirement;
17. accepted-ACK receipt generations/digests, authenticated peer identity, and
    retirement-checkpoint receipt anchors validate; ACK loss before and after
    each fsync is reconciled by `OpenV1` retransmission without over-deletion,
    and no local or cloud object is retired before the exact configured ACK;
18. a terminal store/policy change starts a new stream and the old identity
    cannot regain cleanup authority for it;
19. a late spill upload cannot resurrect an invalidated or retired range;
20. temporary cloud failure and retry do not change range identity;
21. record, byte, age, and total-staging limits bound live objectization; live
    backpressure disconnects without dropping records or advancing ACK;
22. bulk saturation cannot starve capture or the live lane.

Legacy artifact readers are a migration requirement, not protocol conformance.

## 10. Deferred

- dynamic membership, elections, consensus, and peer-to-peer custody transfer;
- multiple deletion-authorizing terminal stores and ACK predicates;
- push replication and cross-stream multiplexing;
- non-contiguous deletion receipts or per-chunk GC authority;
- custody responsibility release or transfer after ACK;
- logical keys, cross-source deduplication, commitment, and fork selection;
- candidate WAL/ACK protocols and Archive V2 publication receipts.

The separate
[minimal Blockzilla block candidate](blockzilla-block-candidate-v1.md) defines
the internal normalization boundary without changing custody.

The separate
[public exit protocol](hivezilla-public-exit-protocol.md) defines bounded live
subscriptions for named raw and derived streams without changing HiveSync or
retention authority.
