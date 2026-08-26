# Blockzilla documentation

Start with the repository [fixture quick start](../README.md#quick-start-build-and-read-the-fixture).
It is the shortest working path through the project.

Current guides: [Blockzilla CLI](../blockzilla/README.md),
[supporting services](../services/README.md), the
[token API example](../examples/token-api/README.md), and
[developer scripts](../scripts/README.md).

## Implemented reference

- [Archive V2 hot-block format](reference/archive-v2-hot-block-format.md)
  documents the files and records implemented by `blockzilla-format` and the
  Blockzilla builders.
- [Block-time gap sidecar](reference/block-time-gap-sidecar.md) documents the
  locally derived slot/time discontinuity file emitted by current builders.
- [Blockzilla scheduler](../blockzilla/README.md#scheduler) documents the
  experimental finite-work scheduler and its read-only status boundary.
- [Blockzilla monitor](../services/blockzilla-monitor/README.md) documents the
  separate read-only operational UI and API contract.
- [FireWatch local archive indexing](guides/firewatch-local-archive-indexing.md)
  hands off the completed-epoch read SDK and authenticated Range-gateway flow;
  the FireWatch adapter itself remains work for the FireWatch repository.
- [Replay runtime POC evidence](benchmarks/replay-runtime-poc-2026-07-28.md)
  records the verified native AArch64 and x86-64 minor-program runs, parity
  tests, genesis fingerprint, and the exact boundary of the current crate.
- [Agave-inspired replay optimization tranche](benchmarks/replay-agave-optimizations-2026-07-30.md)
  records copy-on-write account state, VM/compiler reuse, BPF ABI allocation
  results, and the complete Compact epoch-0-to-1 replay measurement.
- [Replay CPU flamegraph](benchmarks/replay-cpu-flamegraph-2026-07-30.md)
  isolates steady epoch-73 replay from checkpoint startup and identifies native
  SBF guest-memory translation as the dominant CPU cost.
- [Replay conflict scheduling](benchmarks/replay-conflict-scheduling-2026-07-30.md)
  records the full epoch-73 account-conflict graph, deterministic worker
  schedules, planner overhead, memory use, and the parity gate for a real
  parallel executor.
- [gRPC ledger-projection shadow](operations/grpc-ledger-shadow.md) documents
  the authority-free offline migration canary and its fail-closed alerts.

Archive V2 is pre-1.0. Pin the Git revision used to produce and read an archive.

## Proposed architecture

- [Blockzilla Index Archive](design/blockzilla-index-archive.md): indexer-first
  replacement for Archive V2 — column planes for replay and filtering, resolved
  transaction accounts, rebuildable posting indexes, and the migration path
  from today's V2 generations via an offline column transform.
- [Rust runtime boundary](architecture/rust-runtime-boundary.md): ownership
  rule for production logic, the narrow external-tool exceptions, and the
  migration order for remaining Python and shell implementations.
- [Hivezilla convergence architecture](architecture/hivezilla-convergence.md):
  code-backed map of the current live paths, the target hive, retention model,
  alerts, and phased implementation plan.
- [Hivezilla V1 implementation assessment](architecture/hivezilla-v1-implementation-plan.md):
  critical current-code inventory, correctness blockers, dependency order, and
  acceptance-gated implementation backlog.
- [Hivezilla node roles and live streams](architecture/hivezilla-node-roles.md):
  minimal V1 data/control-plane roles plus the exact raw-shred and derived
  block-observation topology.
- [Hivezilla record and sync protocol](design/hivezilla-record-and-sync-protocol.md):
  normative raw journal, per-node temporary overflow, live-first bulk recovery,
  permanent raw-dataset ACK, and retirement contract.
- [Hivezilla public exit protocol](design/hivezilla-public-exit-protocol.md):
  named raw-shred and provisional block-observation subscriptions with bounded
  producer cursors and no custody or canonical authority.
- [Blockzilla V1 minimal block candidate](design/blockzilla-block-candidate-v1.md):
  ledger-only internal interface plus immutable provider/replay runtime
  attachment and parity manifests, without joining them to raw custody.
- [Blockzilla Replay Projection V1](design/blockzilla-replay-projection-v1.md):
  minimal outer-transaction-signature-stripped signed-message stream derived
  from verified shreds for deterministic Bank/SVM replay and runtime-metadata
  regeneration.
- [Replay runtime V0](design/replay-runtime-v0.md): trusted-history execution
  contract, historical epoch-0/1 profile, native program pipeline, state/diff
  model, and acceptance gates.
- [Replay Account Storage V0](design/replay-account-storage-v0.md): implemented
  in-process account index and atomic batches, plus the portable checkpoint,
  recovery log, mmap-segment, and crash-consistency contract needed for
  cross-generation resume.
- [Blockzilla V1 compaction job](design/blockzilla-compaction-job-v1.md):
  immutable inputs and finality, single-worker fencing, candidate/result
  validation, committed Replay-to-Archive dependencies, completion manifests,
  and predecessor-bound catalog commit.
- [Full system schema](architecture/full-system-schema.md): concise target flow
  from network input to storage, edge serving, and local indexers.
- [Live ingest and storage](architecture/live-ingest-and-storage.md): focused
  source-spool, terminal-custody, archive-worker, and catalog boundaries.
- [System overview](architecture/system-overview.md): product ownership and the
  proposed end state.
- [Local sync and indexing](architecture/local-streaming.md): local indexer
  direction and the boundary between the implemented reader foundation and
  still-proposed sync, sink, and checkpoint commands.

## Research and history

- [Live-ingest redundancy](design/live-ingest-redundancy.md)
- [Portable Hivezilla supervisor](design/portable-supervisor.md)
- [Archive V2 evolution](design/archive-v2-evolution.md)
- [Earlier live-producer design](guides/live-archive-producer.md)
- [Replay runtime reference ledger](reference/replay-runtime-references.md):
  pinned Agave, LiteSVM, QuasarSVM, Firedancer, Mithril, launch-era Solana,
  SBPF, Cranelift, genesis, and Old Faithful findings.

These documents preserve ideas and measurements; they are not all implemented
or current. Machine-specific runbooks, credentials, incidents, raw benchmark
output, and production deployment configuration do not belong here.
