# Hivezilla V1 implementation assessment and build plan

Status: **Gate 0 in progress; core custody persisted types, unsafe-publication
closures, the Replay hot-payload codec, and Archive finality/active-fence
acceptance foundations are implemented, while product publication, runtime
consumption, candidate promotion, and the Replay fixture freeze remain**,
updated 2026-07-28.

This document maps the current repository to the proposed V1 contracts. It is
not another protocol. The normative contracts are:

- [source spool, overflow, terminal custody, and HiveSync](../design/hivezilla-record-and-sync-protocol.md);
- [public live exit](../design/hivezilla-public-exit-protocol.md);
- [block candidate](../design/blockzilla-block-candidate-v1.md); and
- [compaction job and canonical catalog commit](../design/blockzilla-compaction-job-v1.md); and
- [outer-transaction-signature-stripped replay projection](../design/blockzilla-replay-projection-v1.md).

## Verdict

The repository already has strong crash-safety, capture, authentication,
bounded-queue, archive-writing, and scheduler primitives. It does **not** yet
implement the unified Hivezilla custody contract. The smallest safe route is to
adapt those primitives behind one record/cursor model, then add temporary
overflow, permanent terminal custody, and live-first recovery in that order.

Four authorities/identity classes remain deliberately singular:

1. one prefix chain per immutable stream generation;
2. one deletion-authorizing terminal-store authority and one monotonically
   advancing protected ACK cursor per stream;
3. one centralized Blockzilla stream registry; and
4. one Blockzilla catalog writer, with one distinct canonical head per product
   (Archive V2 and Replay V1).

Multiple capture nodes, physical raw copies, processors, exits, and compact
workers do not create additional ACK or catalog authorities.

## Immediate safety findings and Gate-0 closures

These are migration blockers, not cleanup opportunities:

1. The current receiver ACK is a **local staging ACK**. In
   `services/hivezilla/src/ingest/receiver.rs`, `push_batch` returns after the
   receiver spool and progress WAL are synced. It does not prove permanent raw
   objects, independent physical copies, or a rebuildable terminal index, and
   therefore must not authorize V1 source deletion.
2. **Closed in Gate 0:** both `services/hivezilla/src/grpc.rs` conversion paths
   now preserve and validate the real source parent final PoH hash; a
   non-genesis block without it is rejected. The separate shred/consensus parent
   block ID remains part of the Gate-0 two-identity refactor.
3. **Closed in Gate 0:** legacy live Archive V2 publication is disabled at the
   CLI boundary and removed from scheduler routing. The degraded repair path
   cannot emit PoH or shredding sidecars, so missing evidence is not represented
   as verified empty. Stopped raw-gRPC materialization now rejects incomplete
   PoH; proof-bearing promotion through the shared candidate remains open.
4. The current raw pull protocol is intentionally stop-and-wait with one
   outstanding batch. It cannot express atomic `C/T` cutover, a protected live
   lane, or parallel idempotent range recovery.
5. The current cloud uploader protects immutable generations well, but its
   generation-retention state is not the per-node temporary-overflow state
   machine. A successful upload or download is not a retirement ACK.
6. No permanent terminal raw-object store, multi-target durability policy,
   rebuildable range/copy index, or policy-bound terminal ACK exists.
7. The scheduler has substantial finite-task reconciliation, but its local
   process ownership markers are not immutable remote job specifications,
   globally monotonic fences, result validation, completion manifests, or
   predecessor-bound catalog CAS.
8. The Yellowstone relay is a useful bounded internal relay, but it is not the
   public exit: it uses slot replay, a shared operator token, source
   co-location, and no terminal-history fallback.
9. The current receiver performs semantic block/PoH validation before storing
   some records. Terminal custody must retain syntactically valid exact raw
   evidence even when candidate promotion will later reject it.

Until Gate 3 passes, legacy receiver ACKs must be named and treated as staging
receipts, and all source GC based on them stays disabled in a V1 deployment.
Until Gate 5 passes, candidate objects stay non-canonical.

## Current asset map

| Area | Current asset | Decision |
| --- | --- | --- |
| CLI and supervision | `services/hivezilla/src/main.rs`, `ingest/config.rs`, portable supervisor | Reuse validation, redaction, process isolation, limits, and health plumbing. Replace the target `Primary`/`Replica` model with explicit roles only after equivalent commands exist. |
| Durable journal | `services/hivezilla/src/ingest/spool.rs` | Reuse exclusive locks, no-follow checks, group commit, checkpoints, torn-tail recovery, corruption handling, audit, projection, and retirement ordering. Quota/free-reserve admission mainly comes from receiver and shred-capture code. Do not call the existing `BZIWAL01/BZIF/CMIT` bytes V1. |
| Yellowstone raw capture | `services/hivezilla/src/grpc_raw.rs` | Reuse reconnect/watchdog, deterministic known-field protobuf encoding, generation cutover, committed cursors, retention recovery, and PoH helpers. V1's exact boundary is the pinned-schema deterministic projection; unknown provider fields are not preserved and require a new schema/stream before use. In particular, Yellowstone schema 12.4 drops Agave's V1-only transaction config and cannot distinguish V1 from V0-without-lookups after decoding. The structural adapter now requires signatures to prove the V0 interpretation in that ambiguous shape, but this only fails closed; capture must gain an explicit version/config-preserving schema before V1 traffic can be retained. |
| Normalized gRPC path | `services/hivezilla/src/grpc.rs` | Reuse parsing and metadata conversion only after the parent-hash, complete-PoH, and missing-state blockers are fixed. Never use this path as the only raw evidence. |
| Shred capture | `services/hivezilla/src/ingest/shred_udp.rs` | Reuse high-rate socket draining, bounded queues, group-commit structure, reserve checks, and kernel-drop telemetry. V1 format 2 stores the exact received datagram, not the current zstd storage payload; compression requires another registered format/version. |
| Repair input | `services/shred-reader`, repair WAL tools | Extract leader schedule, trust, signature/Merkle, nonce, and repair-provenance logic from the current binary crate, then revalidate under the pinned processor/job policy. Keep repair and ordinary shreds as separate raw streams and retain legacy readers. |
| Local receiver and ACK | `receiver.rs`, `receipt_crypto.rs`, `replication.rs` | Reuse ordered WAL/crash recovery and fail-closed deletion ordering only. V1 idempotence is stream/cursor/prefix/object identity; `AckV1` is mTLS-authorized and does not inherit current semantic validation, Ed25519 disposition, primary term, or logical-content digest. |
| Network runtime | `replication_pull_runtime.rs`, `raw_replication.proto` | Reuse mTLS, static allowlists, deadlines, decoding limits, keepalive, and reconnect infrastructure. Keep the old protocol as migration compatibility; add HiveSync V1 separately. |
| Object upload | `crates/hivezilla-object-store`, `blockzilla-s3-upload`, and supervisor tests | Native S3/R2/B2 adapters own conditional create, bounded checksum/readback verification, immutable-collision rejection, commit-last receipts, provider-native version pinning, account-usage reporting, and crash-safe R2 retention. Keep terminal custody and source retirement gated by their separate signed protocols. |
| Shred reconstruction | `ingest/shred_compact.rs` | Reuse FEC grouping/recovery, conflict isolation, chained-root consistency, component decoding, and markers. Add all trust and promotion gates before continuous output. |
| Candidate model | `crates/blockzilla-format/src/candidate_v1.rs`; legacy `live_producer.rs` | The private-field structural candidate is now ledger-only, carries distinct final-PoH/consensus and parent identities, exact fixed signatures/signed-message bytes, and component-aligned marker/data-shred geometry. It has exact-identity and explicitly non-transitive pairwise-compatibility checks but no generic merge. It is still unpromoted: evidence/trust receipts, finality-owned wrappers, and product builders remain open. `LiveBlockDraft` stays incompatible. |
| Public relay | `services/hivezilla/src/grpc_relay.rs` | Reuse bounded-ring and slow-client tests as reference behavior. Its Yellowstone protobuf, slot/filter, token, and source coupling make a small new exact-cursor exit safer than extracting it wholesale. |
| Archive builder | `blockzilla/src/archive_v2.rs` | Reuse encoding/build algorithms only after a determinism audit. Current output can include absolute paths and uses process-random hashing in the first-seen registry, whose effect on emitted ordering has not been proved absent; do not yet claim retry-identical physical bytes. |
| Physical manifest/read path | `crates/blockzilla-read-sdk/src/manifest.rs`, archive gateway | Reuse deterministic file ordering, size/hash validation, range reads, hash-after-write, and no-clobber immutable publication. Add catalog resolution to canonical serving APIs; low-level audit tools may still inspect explicitly supplied generations. |
| Scheduler | `blockzilla/src/scheduler/mod.rs`, `live_materialize_task_model.rs` | Reuse local inventory, resource admission, restart observation, and status reporting. Do not derive remote leases/fences from PID/path ownership markers or assume current task states are the protocol authority. |

## Build gates

The gates are dependency-ordered. Later work may be prototyped in parallel but
must not gain its destructive or canonical authority before its prerequisite
gate passes.

### Gate 0 — freeze shared bytes and stop unsafe publication

Deliverables:

- [x] Add one small dependency-light core crate, provisionally
  `crates/hivezilla-protocol`, only for persisted stream/registry identity,
  `RecordV1`, `CursorV1`, journal, overflow, and terminal-custody types. It has no
  storage engine, network client, scheduler, source adapter, cloud SDK, candidate
  model, compaction control, or dependency on the broad `blockzilla-format`
  crate.
- [ ] Put fixed compaction job/result/completion/catalog types in a separate
  dependency-light module or crate reserved for Blockzilla and compact workers.
  The crate and the service-local generated HiveSync/public-exit protobuf
  bindings, semantic validators, fixed-length checks, and hard limits exist, but
  neither Blockzilla nor a compact-worker runtime consumes the compaction crate
  yet. Capture binaries must depend only on the core types they persist.
- [x] Adapt the source-neutral candidate primitives currently in
  `blockzilla-format` into a shared Rust semantic module so Blockzilla and the
  compact worker use the same `BlockCandidateV1` type without a dependency
  cycle. It has no V1 wire/hash encoding; archive-specific encoders stay in
  `blockzilla-format`. Refactor the current private-field type to carry
  `final_poh_hash`, optional era-defined `consensus_block_id`,
  `parent_final_poh_hash`, and optional parent consensus ID without treating
  them as aliases, plus exact fixed-size outer signatures, exact signed-message
  bytes, ordered entry-batch boundaries, and canonical marker bytes for
  shred-backed candidates. Remove runtime metadata/rewards/time/height and the
  generic cross-source merge from the candidate. Component-aligned cumulative
  data-shred ends allow marker-only ranges without inventing entry progress.
  Candidate promotion/finality wrappers remain a later gate.
- [x] Apply the same two-identity refactor to the implemented compaction-protocol
  finality and parent-anchor types and fixtures, including the distinct
  published-epoch, finality-validation/lookahead, and full status-proof
  validation ranges.
  `consensus_block_id = None` is legal only in a protocol era with no separate
  block ID; final PoH remains mandatory. The superseded single-hash bytes are
  rejected as an incompatible draft; manifest V2 and its literal golden fixture
  are frozen rather than decoding the old shape as the revised contract.
- [ ] Add golden byte and SHA-256 fixtures for empty, one-record, and
  multi-record prefixes; manifests/registry snapshots and heads; segments;
  transfer chunks; overflow and terminal objects; copy receipts; ACKs and
  accepted-ACK/retirement records; every HiveSync/public protobuf message; jobs;
  results; completions; catalog entries; and archive-recovery receipts and
  checkpoints. Candidate adapters instead use semantic equality fixtures
  because `BlockCandidateV1` is not a V1 wire type.
- [x] Preserve and validate the real 32-byte parent final PoH hash in both gRPC
  conversion paths. Reject a non-genesis block without it. This does not supply
  the separate parent consensus block ID available from verified shreds.
- [x] Keep terminal raw capture permissive, but make complete PoH validation
  mandatory before a gRPC observation becomes even a structural candidate.
  The pure gRPC adapter and stopped raw-gRPC materialization validate entry and
  transaction indexes, counts, prefix sums, final hash, source parent hash,
  signature widths/header cardinality, and canonical Legacy/V0 signed-message
  serialization. Unsafe legacy publication is disabled. This deliberately does
  **not** close promotion: signature/PoH recomputation, evidence/policy receipts,
  finality, and opaque product-builder inputs remain required before any
  candidate can be canonical.
- [x] Preserve the revised structural candidate's exact presence rules:
  transactions may be verified empty; PoH is absent or non-empty;
  component layout is absent or non-empty with exact canonical marker order and
  one strictly increasing cumulative data-shred end per component. Runtime
  metadata, rewards, time, and height are absent from the structural type.
  Consensus block-ID legality and other source-specific required sections remain
  promotion-policy checks rather than self-asserted capability bits.
- [x] Correct both legacy live-conversion publication paths, or disable and mark
  `BuildArchiveV2HotBlocksFromLive` plus its scheduler route non-canonical until
  Gate 5. Remove the live empty-shredding-sidecar publication path.
- [x] Define a provider-neutral immutable object-store interface plus a
  deterministic in-memory/filesystem fake supporting conditional create,
  streaming read, length, SHA-256 verification, optional opaque version, list
  for discovery, and delete.

Acceptance:

- [ ] Every persisted or hash-bearing V1 type has one canonical encoding and
  rejects trailing, non-canonical, oversized, or unknown-required data. Protobuf
  is transport-only: its package, service, field, and enum values are frozen,
  decoding is hard-bounded and semantically validated, and its serialized bytes
  are neither canonical identity nor hash input. The literal byte-plus-SHA
  fixture freeze remains incomplete.
- [ ] Real-parent fixtures prove no converter synthesizes either parent identity
  or substitutes a consensus block ID for a final PoH hash. The current gRPC
  fixture covers only the parent final PoH half.
- [ ] Invalid PoH indexes, entry counts, prefix sums, or final hash cannot reach
  a candidate through any source adapter or compact-worker path.
- [ ] Missing and verified-empty candidate values remain distinct in the
  semantic type, and every future source adapter has semantic-equality fixtures
  proving that it preserves the distinction. `BlockCandidateV1` itself has no V1
  wire round-trip.
- [x] No reachable legacy CLI or scheduler path can publish a synthesized parent
  hash, unverified PoH, or missing evidence encoded as verified-empty.
- [x] Existing retained artifacts remain readable; none are rewritten in place.

Current Gate-0 closure work is therefore narrow: finish the two-identity
candidate/finality refactor, connect Blockzilla and the compact worker to that
compaction contract, connect real gRPC/RPC/shred adapters to the validated
candidate type, make the source-specific proof boundary mandatory, and finish
the literal byte-plus-hash fixture matrix. The persisted protocol,
object-store, compaction-control, and transport foundations are implemented;
this does not imply a custody or catalog runtime exists.

### Gate 1 — common journal, lineage, gaps, and registry

Deliverables:

- [ ] Implement `services/hivezilla/src/journal/{writer,reader,recovery,legacy}.rs`
  beside the legacy spool. Adapt proven locking, fsync, checkpoint, quota, and
  recovery mechanics instead of mechanically converting the old file format.
- [ ] Add an explicit provisioning transaction before capture: pin the cluster
  32-byte genesis hash and verify it upstream; allocate CSPRNG data and gap
  stream IDs; reserve the logical registry generations; build canonical
  secret-free producer/overflow/target/catalog descriptors whose credentials
  remain external; bind the private overflow namespace, one terminal store ID,
  and a valid two-failure-domain durability policy; provision the gap stream
  first; then publish the capture manifest. A string such as `solana-mainnet`,
  time/PID/path hashing, or a later runtime default is not valid identity.
- [ ] Create and fsync an immutable `StreamManifestV1` before the first record;
  validate it on every reopen.
- [ ] Enforce one writer and one homogeneous payload format per stream. A WAL
  reset, incompatible config/schema, unsafe handoff, source-host loss, or
  terminal-policy change creates a successor stream with explicit lineage.
- [ ] Add the dedicated `GAP_EVENT_V1` journal and reserve enough local capacity
  to record detected UDP/adapter discontinuities. Serialize counter checks and
  target-record assignment so `observed_at` is the exact durable target cursor
  at detection. Persist the event before resuming pausable input or committing
  subsequent target progress; if persistence fails, alert and fail closed for
  later durable progress. Alerts remain additional, not substitute evidence.
- [ ] Close the pre-Hivezilla shred blind spot: either write directly into the
  V1 journal from the first-hop receiver or add a durable loss-event side
  channel carrying counter instance, delta, and canonical source positions for TVU
  socket loss, forward-queue loss, and UDP-forward failure. Asynchronous metric
  scraping alone is insufficient.
- [ ] Adapt Yellowstone, ordinary shreds, accepted repair shreds, exact RPC
  response envelopes, and slot/status inputs to the journal. Format-2 payload is
  the exact datagram at the declared Hivezilla boundary; if `shred-reader`
  filtered or transformed upstream traffic, bind that fact in the producer
  descriptor. Preserve duplicates and conflicts; perform no semantic dedup here.
- [ ] Keep raw admission syntactic only: validate framing, declared payload
  encoding, limits, stream identity, sequence, and prefix. Store semantically
  invalid blocks/shreds as forensic evidence and reject them later at promotion.
- [ ] Add a Blockzilla-authored, monotonically versioned, predecessor-linked
  registry repository/API with a linearizable exact-value current-head CAS.
  Write/verify the immutable snapshot before CAS; resolve a lost CAS response by
  rereading the head; never promote an ahead-of-head snapshot by listing. Retain
  an independently recoverable exact-head checkpoint, make it durable before
  advertising the CAS generation, and alert on its lag/loss.
  Capture nodes, terminal consumers, processors, and exits use exact manifest
  hashes from it and may serve a durable cached snapshot during a registry
  outage. There is no gossip membership path.
- [ ] Add the minimum read-only legacy bridge for each adapter before migrating
  that source. Replay the retained prefix into a new V1 stream generation; never
  rewrite legacy bytes or claim its old and new prefix hashes are comparable.
- [ ] Emit entry/recovery alerts for journal corruption, manifest mismatch,
  writer conflict, registry chain/stall, and gap-event persistence failure in
  this gate rather than deferring them to rollout.

Acceptance:

- Torn active tails recover; corrupt sealed or interior frames fail closed.
- A checkpoint ahead of bytes is rejected; a checkpoint behind valid bytes is
  safely reconciled.
- Concurrent writers, unsafe manifest reuse, illegal sequence reuse, and an
  unlinked registry successor are rejected.
- Every new adapter demonstrates the same prefix chain and crash order.
- Source loss counters and durable gap events agree in failure-injection tests.
- Store/policy rollover closes the old stream, creates a new store ID and stream
  generation, and never rebinds old cleanup authority.

### Gate 2 — per-node temporary overflow

Deliverables:

- [ ] Give each capture node one IAM-isolated bucket/namespace and each stream a
  deterministic immutable prefix containing its verified manifest.
- [ ] Implement at least one real provider backend behind the Gate-0 interface;
  verify provider-specific checksum/version behavior without promoting an ETag
  into a portable content identity.
- [ ] Implement the sealed-segment state machine: durable upload intent,
  conditional create, provider SHA-256 or full readback, durable receipt/range
  catalog, contiguous oldest-first local eviction, and deletion retry.
- [ ] Serialize spill and retirement under one fenced metadata WAL. A late
  upload after retirement becomes a deletable orphan and cannot resurrect a
  catalog entry.
- [ ] Serve one logical source range from local disk or private overflow without
  exposing object credentials or locators to consumers.
- [ ] Publish a bounded authenticated read-only source-range status projection
  from the durable local/overflow catalog. It may claim recoverability only for
  exact verified cursor ranges and cannot authorize ACK or deletion.
- [ ] Add history-only cold recovery: rebuild a verified catalog from
  self-describing deterministic objects and an exact manifest after proving the
  old writer stopped. Never append the recovered stream or infer completeness
  from bucket listing.
- [ ] Delete an overflow object only after the terminal ACK covers its end
  cursor and the source has durably recorded both the ACK receipt and retirement
  anchor. A completed download alone changes nothing.
- [ ] Emit entry/recovery alerts for pressure watermarks, overflow entry,
  cloud-only age/bytes, upload/auth/quota/verification failure, cold-recovery
  quarantine, and deletion retry.

Acceptance:

- Crashes at intent, upload, verification, receipt, eviction, retirement, and
  deletion boundaries produce no hole or false retirement.
- A same-key/different-bytes collision, wrong optional version, corrupt object,
  conflicting overlap, or interior cold-recovery hole fails closed.
- With a synthetic already-validated terminal ACK receipt, a cloud-only segment
  becomes retirement-eligible and deletes safely. End-to-end protection/ACK is
  a Gate-3 test.
- Object-store outage starts before the local reserve is exhausted and produces
  entry/recovery alerts.

### Gate 3A — permanent terminal objects, index, and readers

Deliverables:

- [ ] Implement one logical terminal store ID with the manifest-bound
  `DurabilityPolicyV1`; V1 requires at least two verified copies in distinct
  declared failure domains.
- [ ] Write bounded self-describing `TerminalRawObjectV1` objects to policy
  targets and persist exact per-target receipts plus a transactional,
  rebuildable range/copy index.
- [ ] Validate custody only by exact stream identity, manifest, cursor/prefix,
  framing, payload encoding, object identity, and limits. Do not apply candidate
  correctness, fork choice, parent-hash, or PoH promotion rules here.
- [ ] Recompute the largest gap-free protected prefix from objects and receipts
  on restart. Treat the cursor checkpoint as a cache, not sole durability
  evidence.
- [ ] Add a bounded read-only `TerminalRawReader` that resolves exact cursor
  ranges through the index and tails newly protected records for processors,
  compact workers, and exit history/cache. Give it read-only credentials and
  budgets separate from HiveSync ingest; it never exposes unprotected source
  storage.
- [ ] Expose the exact protected frontier through the separate authenticated
  read-only status budget used by exits; this is metadata, not a public raw read
  or custody transition.
- [ ] Audit and repair policy-copy deficits continuously. A later deficit does
  not revoke an old ACK, but it stops new protection if policy cannot be met and
  raises a custody incident.
- [ ] Emit entry/recovery alerts for object/index failure, copy/failure-domain
  deficit, reindex failure, protected-cursor stall, and acknowledged-object loss.

Acceptance:

- One copy, or two copies in the same failure domain, never advances
  `protected_through`. Enough independently verified copies plus the index do.
- An unknown target or a receipt whose target/failure-domain mapping differs
  from the immutable policy never counts toward protection.
- Crashes around every object, copy receipt, index transaction, and protected
  checkpoint rebuild the same prefix or fail closed.
- Loss of local terminal metadata rebuilds from enough self-describing policy
  copies; missing acknowledged data is a custody incident, not cursor rollback.
- The reader reproduces exact record/cursor bytes and cannot read a range not
  proven protected.

### Gate 3B — HiveSync cutover, ACK, and source retirement

Deliverables:

- [ ] Add `hive_sync.proto` with exactly the normative service, tags, enum
   values, fixed canonical byte fields, session token, and hard limits. Reuse the
   existing mTLS runtime pieces. Authorize exactly
   `(stream, terminal_store_id)`, allow one active live session, and make a new
   `OpenV1` fence the prior session and its `FetchRange` tokens. Enforce envelope
   byte and repeated-field limits before Tonic/Prost allocation; validating only
   the already-decoded generated message loses unknown-field bytes and is not a
   sufficient boundary.
- [ ] Accept `protected_cursor=None` only as `P(0)` on first use. Reject wrong
  prefix, future, retired, unauthorized, stale-store, wrong-policy, and reset
  opens/ACKs without changing any cursor. Treat a present Open cursor as the
  authenticated retransmission of the terminal's latest cumulative ACK: persist
  any valid advance as an accepted-ACK receipt before `Resume`, accept equality
  idempotently, and reject a value below the source's latest accepted ACK.
- [ ] Under the shared lock, absorb valid frames ahead of a lagging checkpoint,
  seal the old segment, derive `T` from its validated footer, and durably create
  the new segment/live replay point before sending `ResumeV1`.
- [ ] Start the low-latency lane at `[T,∞)` while parallel idempotent
  `FetchRangeV1` calls recover `[C,T)` from local or overflow storage. Encode the
  canonical chunk header/frames/footer and final `TransferChunkCommitV1`; a
  missing or mismatched commit discards all partial staging.
- [ ] Give live, bulk-local, and bulk-cloud independent byte, CPU, disk, and
  concurrency budgets. Bound live objectization by records, bytes, open age,
  and total staging. `LIVE_BACKPRESSURE` ends the session instead of dropping a
  record and continuing.
- [ ] Allow out-of-order staging but advance protection and ACK only through one
  contiguous verified prefix. An ACK cursor must be exactly the end of a
  protected terminal object and bind stream, store, manifest, policy, and prefix
  hash.
- [ ] Persist the digest-chained `AcceptedAckReceiptV1` with authenticated peer
  identity, then the receipt-bound `SourceRetirementCheckpointV1`; only then
  delete covered source disk/overflow objects.
- [ ] Keep legacy pull/receiver paths operational for migration, but name their
  receipts staging ACKs and prevent them from enabling V1 GC.
- [ ] Emit entry/recovery alerts for disconnect, live/backfill lag,
  oldest-unACKed age/bytes, local/cloud throughput, staged-live pressure,
  backpressure, ACK rejection/stall, and source-retirement failure.

Acceptance:

- Atomic cutover while capture continues places every record exactly in either
  `[C,T)` or `[T,∞)`; arbitrary parallel/retried chunks converge to the same
  prefix and a wrong/missing commit never stages a valid range.
- Out-of-order live data never advances across a backlog hole. Saturated bulk
  never starves capture or live delivery.
- Response loss, session replacement, and crashes around ACK receipt,
  retirement anchor, unlink, and cloud delete recover without false advancement
  or over-deletion.
- Only after a shadow run proves Gate 3A and 3B may source GC use terminal ACKs.

### Gate 4A — isolated public raw exit

Deliverables:

- [ ] Build a small new exit role/process using the exact registry head/snapshot
   and manifests, normative public protobuf tags/enums/limits, exact cursors,
   per-feed quotas, and slow-subscriber disconnects. Treat the current relay only
   as bounded-behavior test input. Apply raw envelope limits before generated
   protobuf decoding, then require the context-promoted discovery/subscription,
   Hello, and replay types rather than their structural forms.
- [ ] Add a bounded post-fsync, non-custodial capture fan-out for the raw live
  cache. A gap or disconnect drops only the exit cache suffix and cannot block
  capture, pin source retention, or read source storage.
- [ ] Add the co-located availability-controller module that combines terminal
  protected status with durable source recoverable/lost status through
  operator-authenticated, deployment-private adapters. Publish its immutable
  `RawReplayStatusV1` snapshot to the local exit handler on a budget independent
  of public load. Enforce non-regressing protection, irreversible declared loss,
  and recoverable-range removal only into protected/lost state; unproven state
  is `UNAVAILABLE`, not guessed pending/lost.
- [ ] Serve raw history through `TerminalRawReader` only. Report the protected
  prefix and live-cache suffix as disjoint availability ranges. Return
  `HISTORY_PENDING` only for an explicitly recoverable uncustodied miss,
  `HISTORY_LOST` for declared pre-protection loss, and `UNAVAILABLE` when state
  cannot be proven. Never touch capture disk, private overflow, or HiveSync
  budgets.
- [ ] Emit entry/recovery alerts for fan-out gaps, cache pressure, slow-client
  disconnects, overload, history pending/lost, registry staleness, and terminal
  read failure.

Acceptance:

- Public clients receive exact stream bytes/cursors and cannot affect capture,
  custody, retention, or another client.
- A protected prefix plus disjoint cache suffix reports two ranges and never
  jumps the gap; pending, lost, and unavailable are not conflated.
- A request whose start-to-live interval crosses that gap fails before `Hello`;
  a fan-out gap after `Hello` terminates explicitly before any later event.
- `LATEST` accepts the exact empty interval at the snapshotted tail, and every
  invalid feed/reason/recovery/successor combination is rejected.
- Stream replacement is explicit; no cursor migrates between stream IDs.

### Gate 4B — trusted shred processor and observation exit

Deliverables:

- [ ] Extract the required leader/trust/proof logic from `services/shred-reader`
  into a reusable library and turn `shred_compact.rs` into a continuous
  processor over protected ordinary and repair streams with a disk-backed
  FEC/fork/evidence index.
- [ ] Pin an immutable content-addressed leader-schedule/trust-context snapshot
  in the processor descriptor. Gate non-genesis observations on scheduled
  leader, signatures, Merkle/proof and FEC-root chain, repair provenance,
  recovered-shred validation, completion markers, ordered components, and full
  PoH; gate slot zero on deterministic entry/PoH construction from digest-bound
  genesis data, requiring exact shred bytes only when a complete genesis archive
  is bound. No live or unpinned schedule lookup can affect emitted bytes.
- [ ] Emit immutable source-labelled `SHRED_BLOCK_OBSERVATION_V1` records only
  for complete provisional blocks. The format-6 output journal contains only
  those complete records and preserves final PoH separately from the era-exact
  block identity, the corresponding parent identities, ordered entry batches,
  canonical typed marker bytes, and full signed wire transactions. Keep accepted
  source padding only in raw evidence. Persist incomplete,
  conflicting, and quarantined work in the processor work/evidence index or a
  separate diagnostic stream.
- [ ] Add the observation-feed exit over that journal. A canonical Blockzilla
  lookup is an explicit semantic fallback, never replay of a provisional event.
- [ ] Emit entry/recovery alerts for processor lag, trust-context failure,
  incomplete/conflicting/quarantined slots, derived-journal pressure, and
  observation replay expiry.

Acceptance:

- Valid ordinary, repaired, and reconstructed fixtures produce the same
  observation while retaining distinct raw evidence.
- Wrong leader/signature/proof/root, conflicting roots, invalid completion, or
  invalid PoH is quarantined; partial slots never emit complete observations.
- Observation clients cannot pin raw custody or confuse canonical lookup with
  exact provisional replay.

### Gate 5 — Hivezilla compact worker and Blockzilla catalog authority

Implementation status (2026-07-28): finality-manifest V2 now represents the
final-PoH and optional consensus identities plus distinct publication and
validation ranges. Archive candidate/completion validation now requires the
expected active fence. This is only the acceptance-contract foundation; the
resolver, object publication, completion write, and catalog CAS remain open.

Deliverables:

- [ ] Implement the Blockzilla-owned finality resolver that writes the immutable
  finality manifest from pinned evidence. It must bind a produced slot to an
  authoritative finalized final PoH hash plus the era-defined optional consensus
  block ID, and may mark `SKIPPED` only from explicit authority evidence, never
  from absence, `null`, `404`, or timeout. Keep the published epoch range
  distinct from the bounded validation range: descendant evidence may cross the
  next epoch boundary without becoming output of this generation.
- [ ] Implement explicit exact-raw gRPC/RPC/complete-shred and finite CAR/CAR.ZST
  adapters into the ledger-only `BlockCandidateV1`; gRPC/RPC runtime values go
  into separately immutable `RuntimeAttachmentSetV1` objects.
- [ ] Add an on-disk `(slot, final_poh_hash, optional_consensus_block_id)`
  candidate/evidence index. Group ID-less gRPC/RPC candidates by final PoH, keep
  shred block-ID variants distinct, and perform cross-source ledger-core
  equality, conflict/fork retention, and provenance grouping only here; raw
  cursors remain untouched. Keep provider and replay attachments multi-valued,
  immutable, and outside candidate merge state.
- [ ] Define the archive-format descriptor and a deterministic
  candidate-to-object API. Audit the existing builder by running identical
  inputs in different directories, processes, worker counts, and retry fences;
  remove absolute-path output and random hash-seed effects until referenced
  payload bytes are identical.
- [ ] Only after that API is stable, extract its physical writer into a reusable
  crate, provisionally `crates/blockzilla-archive-builder`, with no catalog
  authority.
- [ ] Pin exact custody-bearing raw stream ranges, any content-addressed finite
  CAR/CAR.ZST objects and decoder descriptors, immutable leader-schedule/trust
  context, validation policy, one complete published epoch and epoch schedule,
  any immutable finality-validation lookahead, output format, fixed zero-based
  ID order, predecessor, and a finality disposition for every published job
  slot. Bounded derived observations are not V1 job inputs.
  `UNRESOLVED` or missing required evidence blocks commit.
- [ ] Require each job to cover exactly one complete epoch. Pin and validate the
  external finalized parent anchor when a produced block's parent precedes the
  epoch; for later generations, match it to the most recent produced block in
  the catalog predecessor chain. Reject parent links outside the epoch/anchor
  rule, range overlaps, range gaps, and any non-zero-based ID assignment.
- [ ] Let the Hivezilla worker build/upload only immutable named candidate
  objects and return `CompactionResultV1 { COMPLETE | NOT_COMPLETE }`. Detailed
  failure/retry diagnostics stay outside the publication protocol. The worker
  never writes the catalog and never ACKs raw custody.
- [ ] Make Blockzilla persist the exact immutable compaction job-spec preimage,
  issue one active globally monotonic fence, expire/retry leases, and validate
  all returned identities and digests. Local PID/path ownership is not a lease.
- [ ] Have Blockzilla reject stale fences, validate candidate objects, write the
  accepted finality bytes into a catalog-readable immutable namespace, write the
  completion manifest with that published reference, then compare-and-swap the
  predecessor-bound catalog head.
- [ ] Add a separate `CatalogHeadStore` boundary with linearizable read and
  compare-and-swap semantics plus a deterministic race/failure fake. The
  immutable object-store interface is not sufficient for this one mutable
  authority.
- [ ] Persist and back up the exact catalog-head checkpoint. Recovery follows
  each catalog entry's explicit predecessor object reference; object listing
  never promotes an orphan candidate.
- [ ] Extend the read SDK and gateway to expose only catalog-reachable completion
  manifests and their verified objects through canonical APIs. Preserve
  explicit low-level generation inspection for audit/recovery.
- [ ] Track the optional independent archive recovery copy from the exact
  catalog-reachable completion/object set. Use a replaceable least-privilege
  copy/audit process or provider replication with read access to the online
  generation and write access only to its recovery namespace; it has no catalog
  or delete authority. Persist the exact canonical-reference to recovery
  key/version mapping, a predecessor-linked per-generation verification receipt
  in the recovery domain, and an exact recovery-head checkpoint that discovers
  the chain without listing. Each receipt carries and verifies its exact catalog
  generation and prior recovery receipt. Catalog commit may remain
  online-copy-complete while recovery-copy state is explicitly degraded and
  alerting, and recovery is reported only after every referenced byte and the
  checkpoint are independently verified again.
- [ ] Give the copier conditional-create authority only for recovery objects and
  receipts plus exact CAS authority only for its deterministic recovery-head
  key. Test empty-to-zero, exact `N-1` to `N`, lost-success reread/retry, wrong
  target/failure-domain mapping, and conflicting checkpoint transitions; never
  advance from object listing.
- [ ] Emit entry/recovery alerts for finality unresolved/conflict, candidate
  nondeterminism, lease expiry/stale fence, upload/result/completion failure,
  catalog-CAS uncertainty, exact head-backup lag/loss, canonical-read mismatch,
  and archive recovery-copy lag/verification failure/restoration.

Acceptance:

- Two workers racing or a lease expiring mid-upload produce at most one
  catalog-visible result; a stale fence can never publish.
- Crashes before/after candidate upload, result persistence, completion write,
  catalog CAS (including a lost success response), and head checkpoint are
  idempotent.
- Deterministic retry produces identical referenced payload bytes and dense IDs;
  only fence-prefixed keys/provider receipts may differ as specified.
- Every slot has exactly one finality disposition and incomplete data cannot be
  represented as a complete canonical generation.
- A catalog-reachable copy of the accepted finality manifest is readable and
  byte-identical without scheduler/input credentials.
- Recovery reads succeed with provider-specific locator/version tokens different
  from the online copy and fail closed on an incomplete, unchained, or
  target-mismatched mapping.
- Canonical readers ignore unreferenced objects; explicit audit tools remain
  able to inspect a supplied non-canonical generation without promoting it.

### Gate 5R — Replay V1 and deterministic runtime regeneration

This is a separate product gate, not an optimization inside Compact V2. Raw
shreds remain the permanent evidence, Replay V1 is the sequential execution
input, and replay outputs become optional Compact V2 runtime attachments.

Implementation status (2026-07-28): `blockzilla-format` now contains the
bounded Replay V1 hot-payload types, strict canonical encoder, event-streaming
decoder, and focused round-trip and malformed-input tests. Payload format 8 is
still rejected. The gate and atomic freeze remain open: registry/tail
resolution and exact expansion, status evidence, pinned Agave fixtures and
adapter, Replay-specific resolution/publication/catalog contracts, and
stateful replay of the exact final bytes are not implemented.

Deliverables:

- [ ] Freeze `REPLAY_PROJECTION_V1` only after its bounded encoder/decoder,
  replay-scoped address registry, previous-blockhash tail, sparse status-key
  references and `StatusKeyEvidenceV1`, exact producer and output-stream descriptors,
  product-specific immutable job-spec/attempt/result/completion/catalog types,
  explicit checkpoint-transition policy, test vectors, and
  malformed-input corpus land together. Keep reserved payload ID 8 rejected by
  executable validators until that atomic activation.
- [ ] Build a deterministic finite-epoch candidate pass over complete, promoted
  raw shreds plus the immutable evidence and bounded validation range in a
  pre-manifest `FinalityResolutionSpecV1`. Preserve ordered entry batches and
  canonical block-marker bytes after Agave-compatible padded decode; sanitize
  every complete signed transaction under its era and verify every signature
  before removing it; store each non-empty entry's signature mixin; validate final PoH and
  parent/block identities; and retain block-ID variants until era-appropriate
  finality selects one. Candidate bytes are not canonical Replay V1 publication.
  Stateful replay covers exactly the descendant prefix required by the finality
  rule; a possibly longer status-only suffix is streamed for cohort-collision
  closure without fabricating finality validation. Neither suffix is emitted in
  the published epoch generation.
- [ ] Implement the replay-only registry pass from signed-message static keys
  and address-table account keys. Register only deterministically profitable
  repeated addresses and use the canonical raw-pubkey escape for singletons.
  Prove that registry IDs and output bytes do not depend on gRPC metadata,
  worker count, map seed, arrival order, or Compact V2 registry state. Default
  program-role hints out of the canonical format. If useful, build them as an
  external cache keyed by Replay generation/address-registry digests; they never
  change replay semantics or catalog identity.
- [ ] Round-trip every Legacy/V0 compact message to its exact original Solana
  signed-message bytes. Store another message version as canonical raw signed
  message bytes only when the bound replay engine supports it; reject raw
  Legacy/V0 and fail the generation on engine-unsupported message or marker
  versions. Never rewrite, omit, or best-effort decode them.
- [ ] Preserve exact native status behavior without storing signature values.
  For every recent-blockhash cohort, bind its known native 20-byte slice offset
  or stream an all-permitted-offset collision proof across checkpoint rows and
  all occurrences until eviction. Encode only sparse historical status-class
  backreferences; modern message-hash keys are derived from messages but still
  require the offset/proof receipt. Unknown offset plus a collision blocks the
  generation.
- [ ] Split structural marker checks from stateful replay checks. Candidate
  projection validates canonical bytes, order, bounds, and parent identities;
  runtime replay validates footer Bank hashes, clock/reward transitions,
  stake-dependent certificates, and every other state-reading marker rule.
  Marker-derived finality is frozen only after that stateful pass and cannot
  circularly authorize its own unvalidated candidate. Permit the immutable
  finality-validation range to extend into descendant slots across the epoch
  boundary; unresolved trailing slots prevent publication.
- [ ] After finality is frozen, have Blockzilla issue a separate publication job
  with one exact format-8 `StreamManifestV1` and random stream ID so every fenced
  retry has the same prefix chain. Keep attempt object keys/provider versions
  and output-derived registry digests out of that stream manifest; bind their
  exact roles, lengths, and digests in the candidate and completion manifests.
- [ ] Decode and statefully replay the exact final format-8 bytes before Replay
  catalog CAS. Persist `ReplayValidationReceiptV1`, its per-produced-slot
  payload/PoH/Bank/account/attachment rows, and its evidence, checkpoint,
  runtime, finality, and stream-manifest bindings. Never promote only because an
  earlier internal candidate replayed successfully.
- [ ] Bind each pre-finality candidate to immutable raw-shred ranges, repair
  evidence, trust/leader descriptors, protocol era, checkpoint, registries, and
  projection algorithm. Bind the final published Replay generation additionally
  to frozen finality and the predecessor/outside-epoch anchor. Reuse the fenced
  worker and exact-CAS publication mechanics behind a separate replay-product
  catalog head; never append Replay V1 epochs to the Archive V2 catalog or live
  stream registry. A Replay V1 result cannot ACK custody or authorize raw
  deletion.
- [ ] Add a replay-engine adapter pinned to an exact Agave/SVM/runtime and
  feature descriptor. It may synthesize the required count of placeholder outer
  signatures and use a hash-only/already-verified execution path, but it must
  never expose that path as transaction-signature verification. Preserve exact
  historical duplicate behavior through canonical recent-blockhash-scoped
  20-byte status-key references plus checkpoint status state; use private injective placeholder
  class keys only inside the adapter. Never expose or checkpoint placeholders as
  signatures or RPC identities, and retain ordinary message-hash status where
  the pinned runtime requires it.
- [ ] Define verified genesis/ReplayCheckpoint inputs and crash-safe replay
  progress. Bind checkpoint slot, final PoH hash, era-defined optional consensus
  block ID, exact predecessor-parent anchor, Bank/account hashes,
  complete message-hash and historical status-key state with bound
  origin-evidence coordinates, blockhash queue, runtime build,
  hard-fork/feature state, and instrumentation policy; an
  epoch cannot replay in isolation without the preceding state. Producing a
  stock/native Agave snapshot is a separate path that must rehydrate real
  signatures from bound raw evidence and rebuild the exact native status cache.
  Every accepted final-byte replay emits a deterministic successor checkpoint
  carrying account and non-account state through the epoch; the completion binds
  it and the next Replay generation must consume that exact reference. Exclude
  receipt/completion/catalog identities from checkpoint bytes to avoid a hash
  cycle.
- [ ] Onboard historical pre-capture data only through digest-bound signed-entry
  corpora that preserve signatures, messages, entries, PoH counts/hashes,
  markers, parents, fork/finality evidence, and declared coverage. Otherwise
  publish an explicit Replay activation epoch beginning at its first slot and a
  verified predecessor checkpoint; V1 has no partial-epoch bootstrap. Do not
  claim replay from genesis.
- [ ] Emit immutable runtime attachments keyed by Replay V1 generation,
  checkpoint, runtime, and instrumentation digests. Regenerate results, fees,
  compute use, logs, CPI, return data, loaded addresses, balances,
  rewards, Bank block height, Bank/account hashes, and the era-pinned canonical
  block-time derivation without mutating the ledger projection. Compact V2 V1
  stores its existing balance metadata; full account writes advance checkpoint
  state and are optional instrumentation, not a required archive attachment.
- [ ] Require signed-ledger equality before binding gRPC metadata to a selected
  raw candidate, then compare gRPC and Replay attachments only under the exact
  outer-transaction-signature-free `execution_core_digest`. Keep missing distinct from
  verified-empty and distinguish semantic replay parity from provider-specific
  presentation, truncation, receipt time, or commitment history. Feed verified
  replay attachments into Compact V2 only after these two boundaries.
- [ ] Add `ReplayArchiveDependencyV1` to Archive jobs. It pins the committed
  Replay catalog entry, final-byte validation receipt, runtime-attachment
  manifest, optional zero-mismatch parity manifest, and exact join policy. Rejoin
  real outer signatures and transaction IDs only from declared raw signed
  evidence at the same structural position and exact signed-message bytes;
  placeholders are never archive identities.
- [ ] Implement `REPLAY_COPY` after Replay catalog publication. Copy the full
  typed closure—job/result, format-8 chunks, status/finality validation,
  attachment chunks, receipt, and checkpoint manifest/chunks—under
  `ReplayRecoveryReceiptV1`, verify readback, and exact-CAS only the
  product/target recovery head. Add head-loss, partial-copy, corruption, and
  restoration drills; an Archive-only recovery copy is insufficient.
- [ ] Add capacity, lag, checkpoint, unsupported-version, state-root, parity,
  product-starvation, and Replay recovery lag/failure/restoration alerts. With the single V1 leased worker, exercise
  serialized Replay-resolution, Replay-publication, and Archive workloads
  together; define scheduling fairness and prove bounded backlog while
  sustaining live rate plus measured catch-up headroom and deterministic
  restart behavior. Freeze `FiniteWorkSchedulerPolicyV1` weights, consecutive
  grant/attempt/ready-wait/backlog limits, and capacity headroom before cutover;
  each full attempt must fit its wall-time bound because V1 has no authoritative
  cross-fence resume cursor.

Acceptance:

- Different processes, worker counts, and raw arrival orders produce identical
  Replay payloads and registries for the same evidence set. Full `RecordV1`
  bytes are identical for retries under the same pre-provisioned random stream
  manifest; independent stream IDs intentionally produce different prefix
  chains.
- Expanded signed messages match the source bytes exactly; stored signature
  mixins reproduce every entry and the declared final PoH boundary.
- Missing, conflicting, incorrectly finalized, unsupported, or unauthenticated
  shred evidence prevents publication instead of producing a partial slot.
- Repeated execution from the same verified checkpoint produces identical
  Bank/account hashes and normalized runtime attachments, including marker-era
  state transitions.
- The exact final encoded Replay generation—not only its pre-finality
  candidate—reproduces every per-slot validation row and attachment bound by the
  receipt before catalog commit.
- Each committed generation's successor checkpoint is the next generation's
  exact input; a missing, substituted, root-mismatched, or discontinuous
  checkpoint prevents publication.
- A declared shadow range covering epoch/feature boundaries, failures, nonce,
  ALT, CPI, upgrades, rewards, repairs, and forks has zero unexplained semantic
  mismatches against the retained gRPC baseline.
- Replay V1 alone cannot return original transaction signatures/IDs, claim
  independent authorization, or replace the raw-shred evidence required to
  audit it.
- Loss of the online Replay head/objects restores the exact catalog and full
  validation/attachment/checkpoint closure from the Replay recovery head; a
  partial mapping or Archive recovery copy never counts as restoration.
- Under the one-worker scheduler, sustained combined Replay and Archive demand
  meets declared lag/backlog bounds and neither product can starve the other.

### Gate 6 — migration, rollout, and operations

Deliverables:

- [ ] Use the per-source read-only bridges added with each gate to import/audit
  retained `BZIWAL01`, raw gRPC generations, repair WAL, Helius per-slot files,
  and cloud-generation artifacts. Do not compare legacy and V1 prefix hashes:
  their identities/framing—and current shred payload encoding—differ.
- [ ] Prefer a controlled handoff to a new V1 stream generation over dual-writing
  UDP capture. Audit the stopped legacy prefix, preserve it immutably, publish
  explicit lineage/gap state, and start the new writer without rewriting old
  evidence.
- [ ] Shadow terminal ACKs with GC disabled, then enable deletion one source
  stream at a time only after crash/cold-recovery drills pass.
- [ ] Shadow the new compactor against current builders; enable catalog CAS only
  after deterministic object and finality comparisons pass.
- [ ] Shadow Replay V1 and runtime regeneration against retained gRPC while gRPC
  remains a production input. Do not retire gRPC merely because ledger messages
  match; all Gate-5R finality, state, determinism, runtime-parity, and capacity
  acceptance conditions must pass.
- [ ] Cut over Compact V2 only at an epoch boundary by changing the immutable
  Archive job's runtime-attachment selection from `PROVIDER_OBSERVATION` to a
  committed `FINAL_REPLAY` dependency. Raw signed-shred evidence, outer
  signatures, and identity-bearing finality inputs remain required and
  unchanged; gRPC is never substituted for that ledger evidence. Keep the
  Archive catalog chain continuous; do not invent or migrate a cross-source
  cursor or “producer generation.” Retain the prior policy's raw evidence and
  committed generations for rollback. Pin a
  finite `rollback_horizon_epochs` in the cutover plan; throughout that horizon,
  continue the complete production gRPC capture, custody, and parity path as a
  non-selected but Archive-eligible fallback.
- [ ] During the rollback horizon, permit only a future epoch's immutable job
  policy to return its runtime-attachment selection to the complete retained
  gRPC `PROVIDER_OBSERVATION`; never rewrite committed
  epochs. At horizon expiry, require an explicit irreversible retirement
  decision. Then quiesce every production raw-gRPC stream whose declared role is
  `PROVIDER_OBSERVATION` runtime capture: seal its final generation, publish
  terminal coverage/gap state, let terminal custody protect and ACK the final
  prefix, record its retirement anchor, and only then apply normal source-spool
  cleanup. This step does not quiesce a separately named gRPC-transported
  finality-authority stream while its independent
  `finality_rollback_horizon_epochs` remains open; that feed is demoted or
  retired only by the separate finality migration below. Before shadowing,
  assign runtime observation and external-finality evidence separate immutable
  logical stream roles. If one physical gRPC subscription transports both, it
  remains captured while either role is active, but each role has its own
  eligibility and retirement anchor; runtime fields observed after runtime
  retirement cannot re-enable runtime rollback. A sampled gRPC canary, if
  retained, is a new separately named non-authoritative stream with its own
  cursor, custody, and retention state; it can never satisfy an Archive input,
  rollback, or finality rule. After production-stream retirement, rollback to
  gRPC is impossible by design and requires a new fully captured source plus a
  new future-epoch policy.
  Retirement stops production capture and permits normal source-spool cleanup
  only after terminal ACK; it does not delete the protected terminal raw-gRPC
  prefix, which remains discoverable audit evidence but policy-ineligible for
  rollback.
- [ ] Treat finality authority as a separate migration. Shadow the exact
  marker/certificate-derived `FinalityResolutionSpecV1` results against the
  current external authority across the declared fork, skip, repair, epoch-tail,
  and descendant-lookahead corpus. Only after zero unexplained identity or
  disposition mismatches may a future epoch's immutable finality rule switch to
  shred/marker-derived authority; committed epochs are never rewritten.
- [ ] If finality rollback is required, keep the complete external authority
  evidence/feed eligible through its own fixed
  `finality_rollback_horizon_epochs`. At expiry, explicitly demote or retire it
  from future resolution policy. Retained terminal evidence remains immutable
  and auditable but cannot satisfy new finality work. If marker/certificate
  evidence cannot independently settle every slot, do not perform this cutover
  and continue to call the deployment shred-primary.
- [ ] Call the deployment **shred-only** only when verified shreds/markers plus
  pinned genesis, protocol, feature, and checkpoint state are the sole ongoing
  ledger, finality, and runtime inputs. Any required RPC, gRPC, Tower/status, or
  provider finality feed means **shred-primary**.
- [ ] Aggregate the entry/recovery alerts introduced in Gates 1–5R into dashboards
  that preserve every custody state; test notification loss without weakening
  any state transition.
- [ ] Retire old `Primary`/`Replica`, direct-normalize, local-receiver-GC, and
  canonical direct-generation serving paths only after replacements are
  deployed and rollback artifacts remain readable. Keep explicit low-level
  generation audit/recovery tools.
- [ ] Defer orphan candidate deletion until catalog reachability tooling can
  prove an object is unreachable; leaving immutable orphans is initially safe.

Acceptance:

- A source-host-loss drill recovers every cloud-only segment, reports any
  unknowable local-tail gap, and starts new capture under a successor stream.
- A terminal-target-loss drill repairs from an independent survivor without
  changing the logical ACK identity.
- A compactor-loss drill pauses archive progress without affecting capture,
  terminal custody, public raw feeds, or already committed reads.
- During the declared rollback horizon, a replay-source cutover can roll a
  future epoch's immutable Archive job policy back to the still-complete gRPC
  evidence without rewriting committed history or moving a cursor between
  streams. After the explicit retirement milestone, drills instead prove that
  all closed prefixes remain discoverable and that neither the canary nor stale
  retained bytes can be selected for rollback.
- A finality-policy drill switches only a future epoch between the still-eligible
  external authority and marker/certificate resolution, never changes a frozen
  manifest, and proves that after explicit authority retirement retained
  external evidence is audit-only and cannot enter a new resolution spec.
- Dashboards distinguish `captured_local`, `overflow_durable`, `protected`,
  `ACKed`, `retired`, `archived`, and `archive_recovery_protected`; no metric
  collapses them into “stored.”

## Dependency summary

```text
0A unsafe publication off ─┐
                           ├─> 1A journal/adapters/gaps ─> 2 overflow ─┐
0B fixed types/config ─────┘                                          ├─> 3A terminal/index/reader ─> 3B HiveSync/ACK
0B fixed types/config ───────> 1B registry ───────────────────────────┘

3B ─> 4A public raw exit                                      (independent)
3B ─> V shared shred verifier ─> 4B shred processor ─> observation exit
3B ─> 5A finality + non-shred adapters ─┐
V  ─> 5A shred adapter ─────────────────┴─> candidate comparison/exact dedup/provenance index
                                            ─> 5B deterministic builder + worker
                                            ─> 5C fenced result validation + completion + catalog CAS
                                            ─> canonical catalog resolver

V ─> FinalityResolutionSpec ─> 5R.1 pre-finality candidates ─> 5R.2 stateful replay ─┐
external finality (current) ────────────────────────────────────────────────────┤
5R.2 marker/cert finality (future) ──────────────────────────────────────────────┴─> frozen finality
frozen finality ─> 5R.3 deterministic format-8 publication ─> exact final-byte replay
exact final-byte replay ─> validation receipt + attachments ─> separate Replay catalog
retained gRPC ────────────────────────────────> differential parity <─ attachments
ReplayArchiveDependency + parity + raw signatures ─> Compact V2 ─> epoch-boundary Gate 6 cutover
marker/cert resolution + external-authority parity ─> future-epoch finality-policy cutover ─> shred-only
```

Gates 5 and 5R consume terminal raw data directly. They share verifier code
with Gate 4B but never depend on the observation journal or either public exit.

## Explicit non-goals for V1

Do not add these while closing the gates above:

- P2P membership, gossip routing, elections, consensus, or peer custody
  transfer;
- another deletion-authorizing terminal ACK authority;
- a second active compactor or distributed catalog writer;
- source-spool/private-overflow reads for public history;
- cross-source cursor order, a merged “best” live feed, or public fork choice;
- semantic dedup in capture or custody;
- bounded derived observations as required compaction inputs;
- initial destructive cleanup of unreachable candidate objects;
- direct canonical publication by a Hivezilla worker; or
- in-place conversion or deletion of retained legacy evidence.

These omissions remove coordination and destructive states without reducing
capture redundancy, physical raw durability, or public delivery scale.
