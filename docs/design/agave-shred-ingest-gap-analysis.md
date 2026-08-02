# Agave shred-ingest gap analysis

Reviewed against Anza Agave commit
[`080e1c2a9430156af7ed322ca3671628fd5c782d`](https://github.com/anza-xyz/agave/commit/080e1c2a9430156af7ed322ca3671628fd5c782d)
on 2026-07-24.

Version scope matters: `shred-reader` pins Agave/Solana crates at 4.1.2, while the audited commit is
4.3.0-alpha. Wire enum order, shred layout, and repair behavior inferred from one version are not a
compatibility claim for the other; captured 4.3.0-alpha fixtures remain a rollout gate.

## Conclusion

Agave's reliability comes from one continuous pipeline:

```text
batched UDP receive
  -> cheap filtering
  -> leader/retransmitter signature verification
  -> blockstore insertion
  -> immediate FEC recovery
  -> targeted repair
  -> the same blockstore/FEC path
```

Blockzilla deliberately separates raw forensic capture from trusted promotion. This hardening
branch closes three immediate gaps: `shred-reader` can observe first-hop Linux socket overflow,
Hivezilla drains UDP into a bounded batch queue before compression or disk sync, and the release
auditor can merge a frozen, independently revalidated repair-WAL prefix while preserving separate
raw-only results. These changes are not deployed until the focused tests and repair-assisted replay
gates pass. The remaining structural difference is that Agave continuously feeds original,
recovered, and repaired shreds into one live blockstore. Blockzilla can evaluate and merge those
sources in an offline audit, but it has no trusted promotion path yet.

There is also one unacknowledged local hop before Hivezilla durability: `shred-reader` reports a
successful UDP send to the loopback recorder, not a recorder ACK. Its bounded forward queue counts
send/queue errors, but a successful UDP send cannot prove that the receiving socket admitted the
datagram. On Linux, `shred-reader` now observes the first-hop TVU socket's cumulative
`SO_RXQ_OVFL` counter through `recvmsg`, while Hivezilla separately exposes the loopback
second-hop socket's `SO_RXQ_OVFL`, queue pressure, and post-fsync cursors. Those measurements make
the two loss boundaries explicit; a sole-source archive still ultimately needs either an
in-process durable writer or a sequenced local stream/spool with post-fsync ACKs.

The first exact sample after the previously deployed receiver upgrade, slots
`434787198..434797197`, contained 9,967 finalized produced slots and 33 RPC-skipped slots. Raw
Turbine plus local FEC reconstructed 6,616 produced slots (66.3791%), left 3,351 incomplete, and had
zero wholly unobserved produced slots. This sample does **not** include the undeployed hardening
branch described here. The incomplete blocks were only 7,821 FEC-threshold shards short in total,
so verified repair merging is the highest-leverage next step.

## What Agave does that Blockzilla does not

| Priority | Agave | Current Blockzilla | Decision |
| --- | --- | --- | --- |
| P0 | Drains TVU UDP with `recvmmsg` into recycled 64-packet batches, coalesces for 5 ms, and hands them to a large bounded queue before downstream work. | On Linux, `shred-reader` now uses ancillary-aware `recvmsg`, drains up to 64 immediately available datagrams per readiness wake-up, and reports first-hop socket overflow before its counted forwarding and repair-observation queues; it still parses each shred inline before queue admission. Hivezilla independently has a dedicated second-hop socket-drain task, bounded byte/event FIFO, batching, and socket-overflow telemetry. Both paths use repeated `recvmsg` and owned datagram buffers instead of Agave's recycled `recvmmsg` batches. | Replay and deploy the observable burst-drained path first; optimize allocation/syscall shape or separate first-hop parsing only if telemetry shows pressure. |
| P0 | Keeps receive, verification, FEC recovery, and Blockstore insertion in one sequenced process pipeline; channel admission is observable even though validator overload policy can still evict. | `shred-reader` still crosses an unacknowledged UDP loopback hop into Hivezilla. First-hop TVU overflow, forwarding queue/send errors, repair-observation queue drops, Hivezilla second-hop overflow, and Hivezilla queue pressure are now distinct counters, but send success is not durable admission. | Require every loss counter to remain zero during rollout. Before sole-source promotion, replace the hop with an in-process writer or a sequenced local spool/stream whose ACK follows Hivezilla fsync. |
| P0 | Verifies ordinary Turbine and repaired shreds against the scheduled leader/retransmitter before trusted insertion. | Repair responses are scheduled-leader verified, but the raw-only capture/audit path checks structure/version and does not establish scheduled-leader provenance. | Preserve unfiltered forensic bytes, but add a separate schedule-verified audit/index before treating raw-only reconstruction as trusted. |
| P0 | Supports nonce-checked ordinary repair plus block-ID/FEC-root-aware repair; shreds for a requested block ID are inserted in the `Alternate` blockstore column and replay can switch to that block. | The planner emits only ordinary `HighestWindowIndex`/`WindowIndex` requests. `Orphan` exists in the 4.1.2 wire enum but is not emitted; block-ID requests, `Alternate` storage, and replay switching are not implemented. | Gate ordinary wire compatibility on captured 4.3.0-alpha fixtures, then treat block-ID/`Alternate` repair as a separate required capability rather than an assumed compatible response variant. |
| P0 | Sends accepted repair responses through signature verification, duplicate handling, blockstore insertion, and immediate FEC recovery in the continuous pipeline. | The repair runtime polls every 50 ms, handles responses serially, and awaits an every-record WAL fsync before the next accepted response. Its UDP socket has no `SO_RXQ_OVFL` telemetry, and shutdown flushes the WAL without first draining that socket or the Turbine observation queue. | Decouple and bound repair response draining, add repair-socket overflow telemetry, then drain response/observation queues before final WAL flush. Prove the behavior under burst and shutdown injection. |
| P0 | Drives per-FEC repair from durable `SlotMeta` (`consumed`, `received`, completion and parent state), retries while a need remains, and retains state until rooting/purge. | Accepted responses are durable, but repair need, outstanding requests, and retry state are not. The hot tracker expires the entire slot 12 seconds after its last observation even when individual FEC gaps remain; wholly unseen slots never enter it. | Persist bounded per-FEC need/retry state and retire it only on completion/root/explicit expiry, not a slot-wide 12-second timer. |
| P1 | Refreshes a stake-weighted peer cache every 10 seconds and samples per request; a retry can legitimately select the same peer again. | Freezes up to eight deterministically ordered peers at initialization and rotates retries through that fixed list. | Refresh peer weights/availability by TTL and sample each attempt while retaining nonce/source checks. |
| P1 | Prioritizes orphan/ancestor and fork-progress repair from rooted blockstore state. | The tracker has no orphan/ancestor request generation or durable fork priority; observed slots compete under flat bounded poll budgets. | Add root/fork-aware priority and orphan discovery after durable slot/FEC state exists. |
| P1 | Ordinary duplicate handling records a duplicate-proof pair rather than retaining every candidate; a consensus-selected block ID can be repaired into `Alternate` storage and selected by replay. | Raw capture preserves all packets, but raw-only reconstruction has no scheduled-leader gate, can let one conflict fail a slot, and has no persistent verified candidate/fork index. | Add a scheduled-leader-verified side index after raw durability, retain the evidence needed for duplicate proofs, and partition candidate FEC chains before final PoH/blockhash selection. |
| P2 | Re-signs and retransmits verified Turbine shreds to its stake-weighted children. | Receives only and does not participate in retransmit. | Add only after capture and repair are reliable; it is a network-participation improvement, not the first reconstruction fix. |

## Relevant Agave paths

- [`ShredFetchStage`](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/core/src/shred_fetch_stage.rs#L28-L180)
  separates UDP receiver threads from filtering and uses a bounded evicting batch channel.
- [`streamer` receive loop](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/streamer/src/streamer.rs#L152-L241)
  uses recycled/pinned packet batches; the Unix implementation uses
  [`recvmmsg`](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/streamer/src/packet.rs#L89-L267).
- [`sigverify_shreds`](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/turbine/src/sigverify_shreds.rs#L160-L278)
  batch-deduplicates, verifies, separates repair responses from ordinary Turbine retransmit, and
  sends both sources to window insertion.
- [`WindowService`](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/core/src/window_service.rs#L209-L258)
  inserts ordinary and repaired shreds into blockstore together.
- [`Blockstore` FEC recovery](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/ledger/src/blockstore.rs#L1658-L1765)
  recovers, verifies, reinserts, and optionally retransmits recovered data shreds immediately.
- [`RepairService`](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/core/src/repair/repair_service.rs#L60-L68)
  uses a 250 ms FEC delay, 150 ms outstanding-request timeout, and a 1 ms scheduler loop; fork
  prioritization is in
  [`repair_weight`](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/core/src/repair/repair_weight.rs#L220-L325).
- [`BlockIdRepairService`](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/core/src/repair/block_id_repair_service.rs#L252-L358)
  runs a separate block-ID repair response receiver and state machine.
- [`ServeRepair` request metadata](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/core/src/repair/serve_repair.rs#L1674-L1681)
  routes block-ID repair shreds to `BlockLocation::Alternate` rather than the ordinary column.
- [`ReplayStage` switch handling](https://github.com/anza-xyz/agave/blob/080e1c2a9430156af7ed322ca3671628fd5c782d/core/src/replay_stage.rs#L2381-L2463)
  defers a consensus-requested bank switch until the selected block and its ancestry are repaired.

## Implemented in this hardening branch

- On Linux, `shred-reader` receives TVU datagrams with Tokio-compatible nonblocking `recvmsg`,
  drains a bounded 64-datagram immediate burst, and exports supported-plus-cumulative first-hop
  `SO_RXQ_OVFL` telemetry. Non-Linux builds retain the Tokio `recv_from` fallback.
- Hivezilla socket draining is isolated from compression, spool admission, and group fsync behind a
  bounded FIFO with explicit queue/backpressure/second-hop kernel-overflow metrics.
- Accepted-repair WAL I/O runs on one ordered blocking worker; the async supervisor restarts only
  repair with bounded backoff while raw readiness remains independent.
- Repair storage has chained immutable segments, a sealed legacy base, fail-closed crash tails,
  retained-byte and filesystem-reserve admission limits, and preinitialized error/capacity metrics.
- `repair-wal-inspect` discovers a terminal cursor only while holding the writer's base lock, and
  `shred-epoch-audit` revalidates and fingerprints the exact repair prefix twice before merging it.

None of these branch changes is a production claim until the frozen repair replay and a fresh
separate-host candidate 10,000-slot window meet the gates below.

## What not to copy yet

### QUIC

Agave TVU receive is UDP. QUIC is not the missing reliability mechanism for shreds. Adding it would
create a private transport that current Turbine peers do not send.

### Receive-side XDP

Current Agave XDP integration accelerates transmit paths such as Turbine retransmit and repair.
TVU ingress still uses kernel UDP plus `recvmmsg`. Blockzilla should first measure loss after
bounded first-hop burst draining, correct socket-buffer configuration, and Hivezilla's decoupled
batching. Receive-side AF_XDP is justified only if kernel/NIC telemetry still proves loss.

### Early deduplication

Agave probabilistically deduplicates before signature verification because a validator needs a
fast execution path. Blockzilla's raw archive should continue preserving transport duplicates.
Deduplication belongs in the verified side index and reconstruction path, not before the forensic
durability boundary.

### Agave's overload eviction policy

Agave's bounded fetch channel evicts the oldest packet batch to prefer fresh validator progress
when consumers fall behind. That is sensible for replay/voting but wrong as Blockzilla's archive
contract: silently replacing older evidence makes exact gap accounting impossible. Hivezilla now
backpressures its bounded userspace queue and exposes both backpressure duration and Linux
`SO_RXQ_OVFL` loss; `shred-reader` independently exposes first-hop TVU `SO_RXQ_OVFL` and counted
forward/repair queue loss. If sustained pressure still reaches either kernel overflow counter, the
next fix is more receive or parallel/batched durability capacity (and then `recvmmsg`), not hidden
eviction.

## Rollout gates

Do not promote the hardened path as the sole archive source until all of these hold:

1. The repair WAL can rotate without disabling repair and sealed segments are retained until a
   durable consumer acknowledges them; its adjacent v3 durable-head checkpoint must also reject
   deleted/truncated terminal segments and sequence rollback in offline failure-injection tests.
2. Captured 4.3.0-alpha fixtures prove ordinary request/response compatibility from the pinned
   4.1.2 client. A separate integration gate covers block-ID repair, `Alternate` insertion, and the
   replay switch; none is inferred from ordinary wire compatibility.
3. Shutdown/restart failure injection proves that queued repair responses and Turbine observations
   are drained before the final WAL flush, and repair need survives beyond the current slot-wide
   12-second retention window until each FEC is completed or explicitly expired.
4. Raw-only and raw-plus-repair two-pass audits have matching immutable-prefix hashes; raw-only is
   labeled forensic rather than trusted, and a separate scheduled-leader-verified result is gated.
5. A fresh finalized 10,000-slot window reconstructs at least 99.9% of RPC-produced slots after
   verified repair merge, with zero unexplained wholly unobserved slots.
6. Sampled final PoH hashes match finalized RPC blockhashes with zero mismatches.
7. A separate-host candidate overlaps the incumbent receiver long enough to compare the same slots
   without sharing its NIC, kernel receive queues, or local recorder handoff.
8. Linux support is reported as true for both first-hop TVU and Hivezilla second-hop
   `SO_RXQ_OVFL`, and each cumulative overflow counter has delta zero across the window. Forward
   queue drops, forward send errors, and repair-observation queue drops likewise have delta zero;
   Hivezilla has no sustained queue backpressure. Before sole-source promotion, the repair socket
   must expose the same supported-plus-zero-delta evidence. These counters do not replace a future
   post-fsync ACK for the local handoff.
