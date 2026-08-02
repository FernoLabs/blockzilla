# Hivezilla V1 public live exit protocol

Status: **draft; normative for the proposed V1 only**.

A Hivezilla Exit gives public subscribers bounded access to one named live
stream. It does not reconstruct blocks, compact data, select forks, store
custody copies, or publish Archive V2. It is separate from the trusted-node
[HiveSync custody protocol](hivezilla-record-and-sync-protocol.md).

## 1. Initial feed kinds

V1 exposes two independent feed kinds:

```text
RAW_SHRED_V1
SHRED_BLOCK_OBSERVATION_V1
```

One subscription binds to one explicit producer stream. V1 has no cross-source
merge, filters, or server-selected “best” stream.

### Raw shred

`RAW_SHRED_V1` publishes the exact `SOLANA_SHRED_DATAGRAM` or
`SOLANA_REPAIR_SHRED_DATAGRAM` records from one Hivezilla source stream after
its capture durability boundary.

- Ordinary and repair shreds remain separate streams.
- Coding shreds, duplicates, conflicts, and arrival order are preserved.
- The public record and cursor retain the source `StreamHeaderV1`, `RecordV1`,
  and `CursorV1` identity defined by the custody protocol.
- A newly durable record may enter the exit's bounded live cache, but historical
  replay comes only from that cache or the terminal raw store, never from a
  source spool, private overflow bucket, or Archive V2.

### Shred block observation

`SHRED_BLOCK_OBSERVATION_V1` publishes complete but provisional blocks produced
by a separate Hivezilla shred-reconstruction processor. The processor tails
only terminal-protected raw records and writes a bounded derived journal before
an exit serves them.

An observation:

- for a non-genesis observation, is emitted only after scheduled-leader
  identity, applicable leader or retransmitter signatures, Merkle/proof and
  FEC-root consistency, repair provenance, recovered-shred validation, ordered
  components, slot-completion evidence, state-independent marker
  structure/parent rules, and final PoH all pass the processor's
  manifest-pinned rules;
- carries `(cluster, slot, block_identity, final_poh_hash)` and is bound to its
  processor input set by the producer descriptor;
- may represent a losing fork and is never called canonical;
- cannot contain transaction execution metadata, rewards, block time, or block
  height because shreds do not provide them;
- is immutable once emitted; another complete fork is another event.

The immutable payload is the complete shred-derived subset of the
[internal block candidate](blockzilla-block-candidate-v1.md):

```text
ShredBlockObservationPayloadV1 {
  slot: u64
  final_poh_hash: [u8; 32]
  block_identity: ObservationBlockIdentityV1
  parent: ObservationParentV1
  transactions: Vec<SignedTransactionEnvelopeV1>
  poh_entries: Vec<PohEntryV1>
  components: Vec<ObservationComponentV1>
}

ObservationBlockIdentityV1 =
    LegacyFinalPoh
  | ConsensusBlockId([u8; 32])

ObservationParentV1 =
    Genesis
  | Parent {
      slot: u64
      final_poh_hash: [u8; 32]
      block_identity: ObservationBlockIdentityV1
    }

ObservationComponentV1 =
    EntryBatch {
      entries_through: u32
      data_shreds_through: u32
    }
  | BlockMarker {
      bytes: bytes
      data_shreds_through: u32
    }
```

`SignedTransactionEnvelopeV1`, `PohEntryV1`, and the component layout have the
exact fields shown in the candidate specification. For shred evidence, the
producer reconstructs each full canonical transaction wire and byte-compares it
with the exact source bytes before retaining the fixed signatures and signed
message bytes. Components retain exact source order; entry batches are non-empty,
and marker bytes use the exact boundary defined by Replay V1. The component
vector is non-empty, at least one component is an entry batch, cumulative entry
and data-shred ends are strictly increasing under the candidate rules, the final
entry hash equals `final_poh_hash`, and the sum of all entry `tx_count` values
equals the transaction count. `Genesis` is valid
only for cluster slot zero. A non-genesis parent carries its final PoH hash and
era-exact block identity. `LegacyFinalPoh` is valid only where the
pinned protocol defines no separate block ID; a consensus block ID is never
substituted for a final PoH hash.

A slot-zero observation uses the same exception as the candidate contract: its
deterministic tick/entry sequence, PoH start/final hash, and shred parameters
must match the producer descriptor's pinned construction from the digest-bound
`genesis.bin`. The arbitrary construction signer is not independent authority.
Exact shred bytes are required only when the complete genesis
archive/blockstore is itself digest-bound.

Payload format 6, version 1 encodes fields in the shown order. Integers are
big-endian; fixed arrays are direct; an option is `0` or `1 || value`; every
vector begins with a big-endian `u32` count; parent tags are `0 = Genesis` and
`1 = Parent`; block-identity tags are `0 = LegacyFinalPoh` and
`1 = ConsensusBlockId`; component tags are `0 = EntryBatch` and
`1 = BlockMarker`. Each transaction encodes a `u32` signature count followed by
that many direct 64-byte signatures, then
`u32 signed_message_len || signed_message_bytes`; marker bytes use
`u32 marker_len || marker_bytes`. Unknown tags, non-minimal or overflowing
lengths, invalid transaction/marker bytes, inconsistent component counts, and
trailing bytes are rejected. This is the format-6 embedding; it does not assign
a standalone wire identity to the internal candidate Rust type. The
record's configured maximum payload size remains an exit limit. Exact per-shred
provenance remains in terminal raw custody and the internal compaction evidence
manifest; the public observation does not carry that potentially large set. A
processor reset or incompatible algorithm, input set, or encoding change creates
a new observation stream ID.

The format-6 `producer_descriptor` is the exact canonical encoding below:

```text
ObservationInputV1 {
  stream: StreamHeaderV1
  start: CursorV1
}

ShredTrustContextObjectV1 {
  key: bytes
  object_version: Option<bytes>
  encoded_len: u64
  sha256: [u8; 32]
  cluster_genesis_hash: [u8; 32]
  first_slot: u64                 // inclusive
  next_slot: u64                  // exclusive
  format_descriptor: bytes
}

ShredObservationProducerDescriptorV1 {
  descriptor_version: u16        // 1
  inputs: repeated ObservationInputV1
  processor_algorithm_descriptor: bytes
  trust_context: ShredTrustContextObjectV1
}
```

It uses the option, byte-string, vector, integer, and fixed-type encoding rules
from the custody protocol. `inputs` has 1..64 entries, is sorted by stream ID,
has no duplicate stream ID, and contains only raw format 2 or 3 streams. Each
input header's cluster must equal the observation stream cluster. Each start
cursor must match its header and identifies the first processor input record.
The algorithm and trust-format descriptors are non-empty, canonical,
secret-free, and at most 65,536 bytes each. Key and optional version are
non-empty when present and at most 1,024 bytes. Credentials and endpoints remain
external deployment configuration.

The referenced immutable object contains the leader schedule and
signature/proof context used for promotion, and its length and ordinary SHA-256
cover the exact stored bytes. `first_slot < next_slot`, its cluster equals the
output stream cluster, and every emitted slot is covered. Temporary
unavailability pauses emission and may resume the same stream only when the
exact reference and bytes return. Changed bytes/reference, a new input
generation, or any algorithm change closes the observation stream and requires
a successor. No live RPC or local unpinned schedule lookup may affect emitted
bytes.

## 2. Stream identity and cursor

Both feed kinds use the same `StreamHeaderV1`, `RecordV1`, prefix chain, and
`CursorV1` defined by the record specification. Raw shreds use payload formats
2 or 3; block observations use format 6. The observation stream's
`producer_config_sha256` binds its processor algorithm and exact input set.

Sequences start at zero and are contiguous. A processor makes an observation
record durable before advancing its visible cursor. Its derived journal is
rebuildable work state and never counts as raw custody. Custody formats 1–5 and
the operational-evidence format 7 carry the manifest's terminal binding;
derived observation format 6 does not.

A slot is never a delivery cursor. Raw shreds may repeat slots, and different
final PoH hashes or consensus block IDs at one slot are valid observations.

### Public stream registry

Random producer stream IDs are not a discovery interface. Every public exit
serves an exact Blockzilla registry head/snapshot pair plus exact hash-addressed
manifests for the streams enabled on that exit:

```text
ListStreamsV1 {}
PublicStreamListV1 {
  registry_head: StreamRegistryHeadV1
  registry: StreamRegistrySnapshotV1
  public_manifests: Vec<StreamManifestV1>
}
```

The registry head and snapshot are encoded and verified exactly as specified by
the custody protocol; their generation/digest must match and the exit does not
filter or rewrite snapshot entries. `public_manifests` is sorted by stream ID.
Every manifest must match one registry entry's stream ID and manifest digest and
must use public format 2, 3, or 6. An exit may expose a configured subset of
eligible manifests, and it accepts subscriptions only for that subset. Feed kind
is derived from `payload_format`; no duplicate public feed enum or manifest hash
is needed.

The registry maps a stable logical name to explicit immutable stream
generations and never silently moves a cursor between stream IDs. Publication
is centralized/static V1 control-plane state, not peer discovery or membership
consensus. An exit may serve a durably cached head/snapshot pair that it
previously verified as committed during a registry outage; it exposes that
pair's generation and does not claim it is the current head. It must not invent,
merge, or renumber entries. The
snapshot digest proves exact bytes and chain identity; public endpoint
authentication is provided by TLS, not by treating
`blockzilla_authority_id` as a signature.

## 3. Subscription protocol

```text
SubscribeV1 {
  stream_id: [u8; 16]
  start: LATEST | CURSOR(CursorV1)
}

HelloV1 {
  protocol_version: u16  // 1
  stream: StreamHeaderV1
  available: Vec<CursorRangeV1>
  live_tail: CursorV1
}

CursorRangeV1 {
  start: CursorV1        // inclusive
  end: CursorV1          // exclusive
}

RawReplayStatusV1 {
  stream: StreamHeaderV1
  status_generation: u64
  protected_through: CursorV1
  source_recoverable: Vec<CursorRangeV1>
  declared_lost: Vec<CursorRangeV1>
}

EventV1 { record: RecordV1 }

ReplayUnavailableV1 {
  reason: CURSOR_EXPIRED | STREAM_REPLACED | HISTORY_PENDING | HISTORY_LOST
  requested: CursorV1
  available: Vec<CursorRangeV1>
  successor_stream_id: Option<[u8; 16]>
  recovery: RETRY | CANONICAL_LOOKUP_ONLY | NONE
}

ErrorV1 {
  code: UNKNOWN_STREAM | CURSOR_MISMATCH | SLOW_SUBSCRIBER | LIMIT |
        UNAVAILABLE
}
```

The proposed transport is one unary discovery call and one server-streaming
gRPC call over TLS:
`ListStreams(ListStreamsV1) -> PublicStreamListV1` and
`Subscribe(SubscribeV1) -> stream (HelloV1 | EventV1 |
ReplayUnavailableV1 | ErrorV1)`. The exact binding is:

```proto
syntax = "proto3";
package hivezilla.public.v1;

service HivezillaPublicExitV1 {
  rpc ListStreams(ListStreamsRequestV1) returns (PublicStreamListWireV1);
  rpc Subscribe(SubscribeRequestV1) returns (stream PublicServerFrameV1);
}

message ListStreamsRequestV1 {}
message PublicStreamListWireV1 {
  bytes registry_head = 1;           // fixed StreamRegistryHeadV1 bytes
  bytes registry = 2;                // canonical StreamRegistrySnapshotV1
  repeated bytes public_manifests = 3; // canonical StreamManifestV1 values
}

message SubscribeRequestV1 {
  bytes stream_id = 1;               // exactly 16 bytes
  oneof start {
    bool latest = 2;                 // when selected, must be true
    bytes cursor = 3;                // 40 fixed CursorV1 bytes
  }
}
message CursorRangeWireV1 {
  bytes start = 1;                   // 40
  bytes end = 2;                     // 40
}
message PublicHelloWireV1 {
  uint32 protocol_version = 1;       // exactly 1
  bytes stream = 2;                  // 118 fixed StreamHeaderV1 bytes
  repeated CursorRangeWireV1 available = 3;
  bytes live_tail = 4;               // 40
}
message PublicEventWireV1 { bytes record = 1; }

enum ReplayReasonWireV1 {
  REPLAY_REASON_UNSPECIFIED = 0;
  REPLAY_CURSOR_EXPIRED = 1;
  REPLAY_STREAM_REPLACED = 2;
  REPLAY_HISTORY_PENDING = 3;
  REPLAY_HISTORY_LOST = 4;
}
enum RecoveryWireV1 {
  RECOVERY_UNSPECIFIED = 0;
  RECOVERY_RETRY = 1;
  RECOVERY_CANONICAL_LOOKUP_ONLY = 2;
  RECOVERY_NONE = 3;
}
message ReplayUnavailableWireV1 {
  ReplayReasonWireV1 reason = 1;
  bytes requested = 2;               // 40
  repeated CursorRangeWireV1 available = 3;
  bytes successor_stream_id = 4;     // empty/omitted=None; otherwise 16 bytes
  RecoveryWireV1 recovery = 5;
}

enum PublicErrorCodeWireV1 {
  PUBLIC_ERROR_UNSPECIFIED = 0;
  PUBLIC_UNKNOWN_STREAM = 1;
  PUBLIC_CURSOR_MISMATCH = 2;
  PUBLIC_SLOW_SUBSCRIBER = 3;
  PUBLIC_LIMIT = 4;
  PUBLIC_UNAVAILABLE = 5;
}
message PublicErrorWireV1 { PublicErrorCodeWireV1 code = 1; }
message PublicServerFrameV1 {
  oneof frame {
    PublicHelloWireV1 hello = 1;
    PublicEventWireV1 event = 2;
    ReplayUnavailableWireV1 replay_unavailable = 3;
    PublicErrorWireV1 error = 4;
  }
}
```

Canonical structures inside `bytes` fields use the record specification's
encoding; protobuf bytes themselves are not object identity. Message size is
the decompressed serialized protobuf byte length presented to the decoder,
excluding the gRPC length prefix and HTTP/2 framing. V1 limits one subscription
request to 4,096 such bytes, one discovery response to 67,108,864 bytes, one
non-event control frame to 1,048,576 bytes, one `PublicEventWireV1` to
134,217,781 bytes, its containing `PublicServerFrameV1` to 134,217,786 bytes,
and one availability vector to 1,024 ranges. An event must respect the shared
134,217,728-byte payload maximum. Wrong fixed lengths, an absent decoded
oneof, `latest=false`, zero/unknown enums, unsorted or invalid ranges, trailing
canonical bytes, or any limit violation is rejected. Standard proto3
last-one-wins parsing applies to repeated wire occurrences of oneof fields;
protobuf bytes are not identity. An exit never truncates a
registry, manifest set, or availability vector to fit: it fails the call. Golden
fixtures cover every message, enum, populated discovery and replay ranges, and
both start variants. Envelope and repeated-field limits are applied to the raw
decompressed protobuf bytes before generated Tonic/Prost decoding; checking an
already-decoded message is not sufficient because unknown fields have already
been discarded and nested allocations may already have occurred.

`available` is an atomic replay snapshot. Ranges are non-empty, sorted,
non-overlapping, and chained to the same stream. Adjacent or overlapping
physical sources are merged. A raw feed may have two disjoint ranges: the
terminal-protected prefix and the recent live-cache suffix, separated by an
uncustodied gap. An observation feed normally has one retained-journal range.

`RawReplayStatusV1` is an internal authenticated read-only status projection,
not a public client message or V1 interoperability wire type. The availability
controller is an in-process module in each exit deployment. It combines the
terminal checkpoint with source local/overflow range-catalog state through
statically configured, operator-authenticated adapters and publishes immutable,
monotonically generated snapshots to the local subscription handler on a
separate bounded budget. Those adapter APIs are deployment-private; separating
the controller into another process requires its own versioned internal
protocol and does not change this public contract. Its stream/manifest and every
cursor prefix must verify.
`source_recoverable` and `declared_lost` are sorted, non-overlapping internally
and with each other, and lie at or beyond `protected_through`. Recoverable ranges
come from durable source range metadata, not metrics; loss ranges require an
explicit durable source/operations declaration. Same-generation different bytes
fail closed. Within one controller lifetime, generation starts at zero and
increments exactly by one. `protected_through` never regresses. A declared-lost
range never disappears or becomes recoverable/protected in that stream
generation. A range removed from `source_recoverable` must have become protected
or declared lost; a new recoverable tail or interior range requires new durable
source-catalog evidence and cannot overlap declared loss. Anything not proven by the terminal, exit cache, or this status is
unknown. A packet lost before capture has no raw-stream cursor and is represented
by the gap-event stream, not invented as a lost cursor range. This status cannot
authorize ACK or deletion.

Rules:

1. The exit resolves the requested start cursor `S` and snapshots `live_tail`
   before `HelloV1`. It sends `HelloV1` only when one continuous available
   interval covers the half-open record interval
   `[S.next_sequence, live_tail.next_sequence)` with both boundary hashes.
   `S == live_tail` is a valid empty interval and is how `LATEST` opens; a future
   `S` is `CURSOR_MISMATCH`. Otherwise it returns
   `ReplayUnavailableV1` or `ErrorV1` for the first missing cursor and closes
   before emitting any event. A successful response begins with exactly one
   `HelloV1` and then emits records in producer sequence order.
2. The returned header must equal the exact `StreamHeaderV1` in the
   context-validated public manifest selected for the request, not merely copy
   its stream ID or manifest-digest field. Its payload format must be an enabled
   public format (2, 3, or 6). A cursor must match that stream's sequence and
   prefix hash; mismatch fails closed.
3. The client durably applies a record before checkpointing its next cursor.
   Lost checkpoint writes may cause an idempotent replay.
4. `LATEST` uses the atomically snapshotted `live_tail` as the caller's explicit
   starting cursor and sends every later record.
5. A missing cursor follows the exhaustive table below: an expired observation
   cursor returns `CURSOR_EXPIRED`, while a raw miss is pending, lost, replaced,
   or unknown according to authenticated status. The exit never jumps to its
   cache floor or live head.
6. A replaced producer uses a new stream ID. The registry links generations and
   a replay response may name the successor, but cursors never migrate silently.
7. The exit keeps no durable per-subscriber state and receives no subscriber
   custody ACK.
8. If an exact next record becomes unavailable after `HelloV1`, including a
   post-fsync fan-out gap, the exit sends no later record. It terminates with one
   feed-valid `ReplayUnavailableV1` from the table below when status proves its
   disposition, or `ErrorV1 { code: UNAVAILABLE }` when it cannot. In that terminal response,
   `requested` is the first missing cursor and `available` is refreshed. The
   stream never stalls indefinitely or jumps the gap.

## 4. Replay semantics

`ReplayUnavailableV1` combinations are exhaustive:

| Feed | Reason | Recovery | Successor |
| --- | --- | --- | --- |
| raw shred | `HISTORY_PENDING` | `RETRY` | absent |
| raw shred | `HISTORY_LOST` | `NONE` | absent |
| raw shred | `STREAM_REPLACED` | `NONE` | required and registry-verified |
| block observation | `CURSOR_EXPIRED` | `CANONICAL_LOOKUP_ONLY` | absent |
| block observation | `STREAM_REPLACED` | `NONE` | required and registry-verified |

Every other reason/recovery/feed combination is invalid. A successor is present
if and only if the reason is `STREAM_REPLACED`; it is a discovery hint, never
cursor migration. Raw protected history does not expire in V1, so raw
`CURSOR_EXPIRED` is invalid. Observation feeds never use `HISTORY_PENDING` or
`HISTORY_LOST`. Unknown or transient evidence always uses
`ErrorV1 { code: UNAVAILABLE }`, not a guessed replay reason. The same matrix
applies both before and after `HelloV1`. A concrete missing old cursor takes
precedence over replacement: pending/lost/expired describes that cursor.
`STREAM_REPLACED` is used only when the request reached the closed stream's
verified final tail and live progress continues under its registry successor.

The recovery mode depends on the feed:

| Missed event | Exact recovery |
| --- | --- |
| Raw shred still in the exit's bounded live cache | Replay the exact cached producer record |
| Raw shred covered by terminal custody | Read the same cursor from the permanent terminal raw dataset |
| Raw shred no longer cached, not protected, and explicitly known recoverable/in-flight | Return `HISTORY_PENDING`; the client retries after terminal progress |
| Raw shred explicitly lost or quarantined before protection | Return `HISTORY_LOST`; exact replay is impossible |
| Recent block observation | Read the processor's retained derived journal |
| Expired losing-fork observation | Retained observation history only; otherwise exact stream replay is unavailable |
| Finalized canonical block by slot | Query Blockzilla history or Archive V2 |

A source node's cloud bucket is temporary spool overflow, not permanent raw
history. Verified overflow may replace the local copy under disk pressure, but
only after its verified receipt, range catalog, and `local_evicted_through`
checkpoint are durable. The object remains part of the source's logical spool
until the one configured terminal raw consumer protects and cumulatively ACKs
the exact prefix. The source must then durably record that ACK and its retirement
anchor before deleting covered local or cloud copies.

A public exit never reads a capture node's disk or private overflow bucket.
Doing so would let anonymous replay compete with capture and terminal recovery.
If an exact raw cursor is absent from the bounded exit cache and lies beyond the
terminal dataset's `protected_through`, the exit returns `HISTORY_PENDING` only
when authenticated `RawReplayStatusV1` says that range remains recoverable and is
awaiting custody. A quarantined stream, declared source gap, or lost sole
pre-ACK copy returns `HISTORY_LOST`; silence or age alone never claims either
state. If status cannot determine recoverability, or the permanent dataset is
temporarily unavailable for an already protected range, the exit returns
`UNAVAILABLE`. It never jumps to a later cursor.

`CANONICAL_LOOKUP_ONLY` is a semantic fallback, not stream replay. It answers
which block ultimately finalized for a slot. It cannot reproduce a losing-fork
observation, its delivery order, or its processor cursor. The exit must never
replace a missing provisional event with a canonical block under the same
subscription.

If exact observation history is unavailable, return
`ReplayUnavailableV1 { reason: CURSOR_EXPIRED, recovery:
CANONICAL_LOOKUP_ONLY, ... }`. The client may independently choose canonical
Blockzilla lookup, but that is not replay of the observation stream.

For canonical lookup, Blockzilla should distinguish:

```text
BLOCK
SKIPPED
NOT_YET_COMMITTED
UNRESOLVED
```

A slot gap, `404`, `null`, or an unfinished archive is not proof that Solana
skipped the slot.

## 5. Isolation and limits

An exit reads through its bounded live cache, the terminal raw store, or a
processor journal. It never reads a source spool or UDP socket directly and
never runs in the recorder's fsync/backpressure path.

For low-latency raw delivery, a capture writer may send each post-fsync
`RecordV1` through a separate bounded non-custodial fan-out into an exit cache.
The fan-out has no replay, ACK, or retention authority. A full or disconnected
fan-out drops that exit session/cache suffix and alerts; it never blocks capture
or drops the source journal record. Terminal custody later supplies permanent
history.

Every deployment defines maximum clients, queued bytes per client, frame size,
replay distance, write deadline, and request rate. The V1 public endpoint
permits anonymous read-only access through TLS termination and those bounds; it
must not require the recorder's shared operator credential. Optional
authenticated or paid tiers are deployment concerns.

Raw and block-observation feeds have independent queues and quotas. A slow
subscriber is disconnected with `SLOW_SUBSCRIBER`; the exit never drops one
event and continues the same connection. Public subscribers never pin the
shared cache, derived journal, raw custody, or Archive V2.

## 6. Conformance

V1 fixtures must prove:

1. raw exit bytes and cursors match the source stream exactly;
2. raw and derived streams use the same prefix-chain and cursor fixtures, and
   payload format 6/version 1 has golden bytes for zero and multiple
   transactions;
3. cursor mismatch, expiry, and stream reset never skip silently;
4. complete fork observations remain distinct at the same slot;
5. partial reconstruction is never emitted as a block;
6. a slow or malicious subscriber cannot block capture, custody, processing,
   compaction, or another subscriber;
7. no public message or cursor can authorize raw garbage collection;
8. canonical lookup is never represented as exact provisional-feed replay;
9. public history load cannot consume source-spool disk, source-cloud, or
   HiveSync transfer budgets;
10. registry replacement links never migrate a cursor between stream IDs;
11. the exact registry head/snapshot pair and every exposed manifest digest
    validate;
12. a protected prefix plus disjoint live-cache suffix reports two availability
    ranges and never skips the middle gap;
13. `HISTORY_PENDING`, `HISTORY_LOST`, and transient `UNAVAILABLE` cannot be
    silently substituted for one another;
14. missing, wrong-cluster, or out-of-range observation trust context emits no
    observation; temporary unavailability pauses the same stream, while a
    changed reference or bytes requires a successor stream;
15. a requested-to-live range crossing an interior gap fails before `HelloV1`,
    and a new gap after `HelloV1` terminates explicitly without a later event;
16. every invalid reason/recovery/successor/feed combination is rejected, and a
    replacement never hides a concrete pending, lost, or expired cursor; and
17. `LATEST` succeeds on the exact empty start-to-tail interval, while a replay
    status generation cannot regress protection, erase declared loss, or remove
    recoverable data without protecting it or declaring it lost.

## 7. Deferred

- a merged multi-source block feed;
- fork choice or retractions at the exit;
- a finalized canonical Blockzilla topic;
- per-subscriber filters and projections;
- subscriber-to-subscriber relay or general peer discovery.
