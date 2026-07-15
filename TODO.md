# TODO

- Evaluate `rkyv` for zero-copy / archive-friendly layouts
- Major repo clean up
  - application and tools in there own folder
  - crates/
    - blockzilla/
    - oldfiathtull/
    - hivezilla/

project should expose simple read format cli management for blockziall format
* unpack / repoack
* index reader

# Backlog

- Archive V2 durable nonce follow-up
  - Verify `OwnedCompactRecentBlockhash::Nonce` values against nonce-account state so raw recent-blockhash fallback cannot hide a parser/modeling mistake.
  - Add/extend nonce-account indexing; durable nonce accounts will likely need their own lookup path.

- explore seekable zstd https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md

## Redundant live ingest

Design: [`docs/live-ingest-redundancy.md`](docs/live-ingest-redundancy.md)

The configuration, identity/dedup model, durable raw spool, receipt WAL, and deletion-safety
primitives exist. Everything below is still required. Do not replace the current NAS
`capture-grpc` process until the production-cutover gate at the end of this checklist passes.

### P0 — source runtime and cursors

- [ ] Build one supervised task per enabled source (`grpc`, `shred_udp`, and `shred_quic`).
- [ ] Assign every source a durable journal UUID and monotonic observation sequence.
- [ ] Spool the complete source event before advancing its cursor or starting derived writes.
- [ ] Persist provider-specific cursors only after the corresponding spool frame is synced.
- [ ] Reconnect gRPC/QUIC with configured backoff, jitter, heartbeat, idle timeout, and an inclusive
      overlap; let exact dedup absorb the replay.
- [ ] Treat a provider's requested `from_slot` as untrusted: persist the first delivered slot after
      every subscribe, emit the exact uncovered range, and enqueue repair from another source. Live
      tests observed providers skipping requested replay ranges on Mac and Hetzner restarts.
- [ ] Keep cursor state separate per source, endpoint generation, and cluster identity.
- [ ] Treat EOF/timeouts as reconnect conditions, never as epoch completion.
- [ ] Add bounded per-source channels plus process-wide byte/event semaphores from the validated
      configuration.
- [ ] Pause replayable sources when queues or disk reach their high watermark; never silently evict
      an accepted event.
- [ ] Count and alert on UDP kernel/application drops because UDP cannot be lossless under finite
      disk or sustained overload.
- [ ] Add graceful shutdown: stop intake, drain bounded queues, sync spool/cursors, then exit.

Acceptance:

- [ ] Restart and reconnect tests prove an inclusive replay produces no duplicate archive effect.
- [ ] A cursor can never point beyond the last synced spool record.
- [ ] A stalled source cannot exceed its byte budget or consume the budgets of healthy sources.

### P0 — persistent dedup and merge

- [ ] Choose and implement the exact disk-backed `DurableDedupIndex` storage engine.
- [ ] Persist observation, content-digest, logical-key, block-slot/fork, and provenance indexes.
- [ ] Make the index rebuildable from spool segments after deletion or corruption.
- [ ] Keep payloads in the spool; index only bounded metadata and use a bounded recent-record cache.
- [ ] Page conflict/fork candidates instead of loading epoch-wide sets into memory.
- [ ] Record exact duplicates as additional provenance without writing the payload twice.
- [ ] Quarantine observation reuse, digest identity violations, and malformed source records.
- [ ] Persist conflicting payloads and fork candidates; never resolve them by source priority alone.
- [ ] Implement deterministic selection using commitment, completeness, configured source policy,
      then stable source rank only as a final tie-breaker.
- [ ] Add a repair queue for late prior-epoch data and unresolved conflicts.

Acceptance:

- [ ] Rebuilding the index from the same spool yields byte-identical decisions and canonical order.
- [ ] Duplicate/conflict/fork tests cover multiple providers and replicas in different arrival orders.
- [ ] A full-epoch soak has a documented, enforced memory ceiling independent of epoch size.

### P0 — single canonical archive writer

- [ ] Feed one canonical writer from durable dedup/merge output; never let source tasks append
      archive sidecars directly.
- [ ] Persist a writer commit manifest that atomically identifies the last complete effect across
      blocks and every derived sidecar.
- [ ] Make writer replay idempotent while preserving dense block IDs and deterministic ordering.
- [ ] Rebuild partial/missing sidecars from the spool or committed canonical output.
- [ ] Recover cleanly when a crash occurs between any two sidecar writes or manifest updates.
- [ ] Gate epoch closure on required-source watermarks, commitment, gap policy, and repair status.
- [ ] Route late events to repair instead of appending them to the next epoch.
- [ ] Expose a sealed epoch only after the canonical manifest and required sidecars are durable.

Acceptance:

- [ ] Kill/restart at every writer boundary produces the same archive as an uninterrupted run.
- [ ] Replaying a complete spool twice changes no block IDs, sidecar counts, or archive bytes.

### P0 — spool quotas and memory pressure

- [ ] Enforce `spool.max_bytes`, `reserve_free_bytes`, segment size, and configured full policy at
      runtime rather than validation only.
- [ ] Add high/critical disk watermarks with hysteresis so sources do not flap between pause/resume.
- [ ] Reserve capacity for WAL headers, receipts, cursors, manifests, tombstones, and recovery work.
- [ ] Bound serialization, decompression, protobuf, reconstruction, replication, and archive-writer
      buffers by bytes.
- [ ] Reject a single oversized event before allocation where possible and quarantine it with
      source/slot metadata.
- [ ] Add sync batching only if the cursor/receipt boundary still acknowledges nothing before the
      batch is durably synced.
- [ ] Benchmark sync latency, write amplification, segment size, and bounded-cache sizes on the NAS.

Acceptance:

- [ ] Long-running ingest stays within its configured RSS and queue budgets.
- [ ] ENOSPC/reserve-watermark tests stop safely without advancing cursors or losing acknowledged
      observations.

### P0 — authenticated primary/replica transport

- [ ] Implement the bounded streaming protocol for offer, exact-content lookup, payload request,
      chunk upload, and receipt response.
- [ ] Make offers idempotent by observation identity and exact content digest.
- [ ] Limit batch events, batch bytes, chunk bytes, in-flight bytes, and request decoding before
      allocation.
- [ ] Resolve secrets at runtime from environment/file references without logging or exposing them
      through status APIs.
- [ ] Validate key/certificate permissions and reject plaintext or incomplete TLS configuration.
- [ ] Implement mutual TLS, node allow-listing, and cluster/peer identity checks.
- [ ] Implement the concrete Ed25519 receipt signer and trusted-key verifier using the canonical
      signing bytes.
- [ ] Persist and fence `primary_term`; reject receipts from retired or non-current primaries.
- [ ] Implement receipt-key rotation and revocation while retaining old verification keys for the
      maximum receipt/spool retention period.
- [ ] Handle disconnects, duplicate offers, partial uploads, timeouts, and lost replies by retrying
      from the replica's durable spool.
- [ ] Verify payload length, logical identity, format version, cluster, and recomputed digest again
      at the primary before issuing a receipt.
- [ ] Decide and configure the replica deletion policy: primary-WAL durable, archive committed, or
      durable quorum. Default to the safest policy until failover requirements are explicit.

Acceptance:

- [ ] Wrong-node, wrong-cluster, wrong-key, revoked-key, stale-term, wrong-record, and wrong-digest
      receipts never become durable deletion authority.
- [ ] A lost response or connection cannot lose data and only causes an idempotent resend.

### P0 — sealed-segment acknowledgement and garbage collection

- [ ] Replay and reverify the local receipt WAL when rebuilding replica GC state after restart.
- [ ] Build a sealed-segment manifest containing every frame identity, digest, and location.
- [ ] Mark a segment eligible only when every frame has an exact verified receipt that was synced
      locally.
- [ ] Persist a segment tombstone and sync it before unlinking any spool segment.
- [ ] Unlink only whole sealed segments, then sync the containing directory before reporting space
      reclaimed.
- [ ] Persist a segment-ID high watermark so deleted IDs are never reused.
- [ ] Retain an auditable mapping from deleted replica frames to signed primary receipts.
- [ ] Make GC restart-safe at every tombstone/unlink/directory-sync boundary.
- [ ] Keep the current no-delete behavior until the entire manifest audit is implemented.

Acceptance:

- [ ] A network ACK alone, a partially written receipt, or one unacknowledged frame prevents segment
      deletion.
- [ ] Power loss at every GC boundary leaks at most reclaimable disk space and never loses the last
      durable copy.

### P0 — shred ingestion and reconstruction

- [ ] Spool raw UDP/QUIC shred datagrams immediately with source and receive metadata.
- [ ] Validate packet bounds and classify data/coding shreds without unbounded reconstruction state.
- [ ] Start with complete contiguous data-shred sets; keep incomplete sets in a bounded, disk-backed
      repair lane.
- [ ] Version-pin Solana shred formats and reject unknown versions safely.
- [ ] Add Reed–Solomon recovery, Merkle/signature checks, and leader-schedule verification.
- [ ] Run reconstruction in an isolated bounded worker/process, not in the socket receive loop.
- [ ] Compare reconstructed PoH/block hashes with gRPC candidates; retain mismatches as conflicts.

Acceptance:

- [ ] Duplicate, reordered, missing, invalid, and conflicting shreds cannot corrupt canonical output
      or grow memory without bound.

### P0 — observability and Hivezilla integration

- [ ] Expose source state, last event time, reconnect count, durable cursor, queue events/bytes,
      source watermark, and UDP drop counters.
- [ ] Expose spool bytes/free space, segment counts, sync latency/errors, and poisoned-writer state.
- [ ] Expose dedup replay/duplicate/conflict/fork/quarantine counts and repair backlog.
- [ ] Expose replica lag, unacknowledged bytes, receipt/key/term status, GC-eligible segments, and
      reclaimed bytes.
- [ ] Expose canonical-writer position and per-epoch source/sidecar readiness through the Hivezilla
      API.
- [ ] Add clear frontend states/colors for healthy, lagging/degraded, conflict/repair, disk pressure,
      replica-only durable, finalized, and finalized-with-optional-sidecar-missing.
- [ ] Add alerts for stalled required sources, cursor/spool divergence, corruption, disk watermarks,
      receipt rejection, stale keys/terms, and any data drop.
- [ ] Ensure all status structures and logs are secret-free and bounded in cardinality.

### P0 — fault, security, and resource tests

- [ ] Inject short writes, partial writes, `ENOSPC`, `EIO`, flush failure, and fsync failure at every
      spool, receipt, cursor, manifest, rotation, tombstone, and archive commit boundary.
- [ ] Truncate every possible byte of a final frame; recover only demonstrably incomplete tails.
- [ ] Corrupt headers, lengths, payloads, checksums, and middle frames; fail/quarantine without
      truncating later committed data.
- [ ] Test concurrent writers, symlinks, non-regular files, unsafe path components, and unrelated
      canary files.
- [ ] Test `kill -9`, reboot, power loss, disconnect, slow peers, lost ACKs, duplicate/reordered
      delivery, and primary restart.
- [ ] Test provider disagreement, forks, late events, missing slots, lagging optional/required
      sources, and epoch-boundary races.
- [ ] Test certificate expiry, key rotation/revocation, stale primary terms, and node allow-list
      changes.
- [ ] Run disk-pressure and multi-epoch memory soaks with measured RSS, allocator, queue, cache, and
      reconstruction-window maxima.
- [ ] Run Linux/NAS filesystem power-loss tests for file sync, directory sync, rename, tombstone,
      and unlink semantics.

### Production-cutover gate

- [ ] Install boot-managed NAS services for Hivezilla and the independent live producer, with
      restart-on-failure, restrictive secret-file permissions, health checks, bounded logs, and
      explicit crash-safe capture rotation. Verify a real NAS reboot restores `0.0.0.0:8787` and
      live ingestion without appending to an unaudited pre-reboot sidecar tail.
- [ ] Package the new ingest runtime without changing the current NAS service or archive paths.
- [ ] Run it in shadow mode on separate spool/output paths and compare slot coverage, hashes,
      conflicts, and sidecar totals with the current capture.
- [ ] Run a remote replica through disconnect, catch-up, duplicate, and disk-pressure scenarios.
- [ ] Give the Hetzner/Dokploy recorder a dedicated volume sized for retention plus reserve; the
      current 38 GB root disk sustains only a short 5--6 GB/hour verification window. Keep remote
      credentials in a root-owned host file mounted read-only as a Compose secret; do not place
      them in Dokploy variables or the container environment.
      Local deployment support now includes the external-volume guard, file-mounted Yellowstone
      token, inclusive gRPC resume overlap, durable-block watchdog, structural PoH completeness
      gate, locked WAL/PoH verifier, and file-mounted Telegram alerting for recorder/feed exits,
      stale durable writes, volume loss, resume-coverage gaps, and disk pressure. The
      resume-gap notification event is synced on the recorder volume before accepting a later
      block, retained until its exact Telegram delivery marker is durable, and disk recovery uses
      hysteresis. The optional primary-sync-heartbeat alert remains disabled until the authenticated
      request/transfer/ACK protocol exists. The item remains open until the real Hetzner volume and
      at least 1,000 overlapping blocks pass the documented restart/stall/disk/PoH comparison
      checks, including a delivered test alert.
- [ ] Complete at least one full epoch plus boundary/late-data handling under bounded memory.
- [ ] Verify recovery from an unclean stop without manual archive repair or duplicate effects.
- [ ] Document deployment, rollback, disk-full recovery, corruption quarantine, key rotation,
      replica resync, and emergency no-delete procedures.
- [ ] Require an explicit operator review before enabling segment GC.
- [ ] Only after every P0 acceptance item passes, switch Hivezilla/canonical finalization to the new
      writer and retire the old `capture-grpc` path.

### P1 — after the safe cutover

- [ ] Define and implement externally fenced replica promotion/failover if automatic promotion is
      required.
- [ ] Add durable quorum receipts if one primary copy is not sufficient for the retention policy.
- [ ] Tune segment size, batching, cache sizes, source grace periods, and reconstruction windows from
      production measurements.
- [ ] Add automated receipt/key retirement after the audited retention window.

## NAS scheduler and live-capture follow-up

### P0 — recorder-only capture and scheduler-owned materialization

- [ ] Keep the long-lived Yellowstone process limited to `record-grpc-raw`: maintain the
      connection, reconnect with inclusive overlap, and durably append checksummed events. It must
      not build block, PoH, signature, pubkey, registry, or compact archive sidecars.
- [ ] Add a read-only committed-prefix WAL reader that snapshots the durable handoff journal,
      verifies complete referenced frames/hashes/order, and never takes the recorder's exclusive
      writer lock or mutates the active spool.
- [x] Add bounded-memory `materialize-grpc-raw` output compatible with the existing live
      capture layout. Write into a hidden staging directory, publish a source-bound
      `RAW-MATERIALIZATION-COMPLETE.v1.json` receipt last, then atomically rename.
- [ ] Model each finite conversion as `live_materialize:<source_id>:<epoch>` / lane kind
      `live_materializer`, with explicit `raw_ready`, `materializing`, `repair_gate`,
      `ready_to_package`, `packaging`, `packaged`, and `complete` states.
- [ ] Resource-admit, pause, resume, retry, and report live materialization like every other
      scheduler task. A blocked or failed materializer must immediately yield to another runnable
      historical, repair, audit, or finalizer task.
- [ ] Keep transport health (`recording`, `reconnecting`, `stalled`, `low_disk`, `failed`) separate
      from task state. Live transport failure must never set historical compaction capacity to zero.
- [ ] Preserve old `capture-grpc` folders and the existing registry -> MPHF -> hot-rewrite tasks
      throughout migration.
- [ ] Shadow-materialize one closed epoch and compare slots, blockhashes, PoH, signatures,
      transaction/pubkey counts, missing flags, and final hot-archive reads before cutover.
- [ ] Disable `capture-grpc` only at a proven epoch boundary, then retain raw WAL for at least two
      full epochs and through restart/stall recovery. Keep raw deletion disabled until audited,
      source-bound archive receipts make whole-segment GC provably safe.

- [x] Make Hivezilla live counters monotonic across stale root snapshots, fresh journal snapshots,
      and the append-only gRPC journal; bound tail reads and preserve terminal capture states.
- [x] Split runnable queue drain time from full archive completion: exclude action-required epochs,
      use observed full-job durations and actual advancing capacity, and schedule remaining jobs
      lane by lane in the ETA model.
- [ ] Admit one low-priority `getBlocks` slot audit for each closed epoch before finalization, reuse
      its bounded bitmap receipt across retries, and require an explicit refresh to contact the RPC
      provider again.
- [ ] Keep slot audits independent of compact/finalize admission: a blocked, failed, or rate-limited
      audit must expose its reason and let the scheduler immediately choose another runnable task.
- [ ] Expose RPC/local missing directions, provider-guarantee policy, snapshot age, and
      `agrees_unproven` versus `slot_coverage_verified` in the epoch state and UI; quarantine
      provider disagreements instead of silently classifying omissions as skipped slots.
- [ ] At a clean epoch boundary, replace the old running producer/supervisor with the generation
      that atomically publishes `journal/progress.json` every three seconds. Treat progress-write
      errors as observable failures instead of discarding them.
- [ ] Promote the aggregate-throughput scheduler only after its Linux `/proc` suite, final frozen
      overlay review, and dashboard terminology update pass. PSI must remain telemetry, not the
      pause/resume control signal.
- [ ] Measure a controlled 3 -> 4 -> 3 lane probe before raising the production I/O budget above
      360 MiB/s; accept a fourth lane only if aggregate blocks/s improves reproducibly.
- [ ] Expose physical-device read throughput and per-member disk telemetry beside aggregate worker
      blocks/s so a controller decision can be audited from the UI.
- [ ] Harden adopted out-of-range legacy jobs so their remaining output growth is fully reserved in
      disk admission, even when the current bounded production range makes this nonblocking.
