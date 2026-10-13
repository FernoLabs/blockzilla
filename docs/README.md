# Blockzilla documentation

## Start here

The [user guide](start/overview.md) reads Solana history through Cloudflare.
The [quick start](start/quick-start.md) installs the CLI and reads real data in
about 10 minutes, most of it the first compile.

| User guide | Advanced reference |
| --- | --- |
| [Quick start](start/quick-start.md) | [File formats](advanced/formats.md) |
| [Read an archive](start/read-an-archive.md) | [Compact V2 byte layout](reference/archive-v2-hot-block-format.md) |
| [Live stream](start/live-stream.md) | [Stream transport](../hivezilla/stream-relay/README.md) |
| [Callbacks](start/callbacks.md) | [Canonical V3](advanced/archive-v3.md) |
| [Local filters](start/filters.md) | [Data and coverage](advanced/coverage.md) |
| [Cloudflare reads](start/network.md) | [Reader behavior](reference/archive-reader-formats.md) |

These pages also build into a small [documentation website](../web/blockzilla-docs/README.md).
The site uses the same Markdown, including the existing V2 format reference.

[Token index in SQLite](start/token-indexer.md), [Token dumper](start/token-dumper.md),
[Program dumper](start/program-dumper.md), [ClickHouse](start/clickhouse.md), and
[from Jetstreamer](start/jetstreamer.md) cover application examples. The live
stream is in private beta and needs a consumer key. The crates.io release is
pending.

## Engineering documentation

Start with [Archive formats and the read SDK](reference/archive-formats-and-read-sdk.md).
It is the main product guide for format choice, the common ordered scan API,
network setup, cache behavior, trust levels, and reader file layouts. The
[workspace map](design/workspace-restructure.md) explains format crates,
shared byte sources, top-level indexers, and the remaining V3 migration.

The repository [fixture quick start](../README.md#quick-start-build-and-read-the-fixture)
is an offline engineering check. It is not required to use the archive.

Release preparation: [publish the reader and CLI](advanced/publishing.md).

Current guides: [Blockzilla CLI](../blockzilla/cli/README.md),
[supporting services](architecture/services.md), and
[developer scripts](../scripts/README.md).

The [token API example](../examples/token-api/README.md) is an older,
format-specific example. It reads Compact V2 wire files directly and is not the
recommended read-SDK starting point.

## Implemented reference

- [Archive formats and the read SDK](reference/archive-formats-and-read-sdk.md)
  lists the dedicated CAR, Compact V2, frozen prototype, and canonical V3 readers.
- [Blockzilla query SDK guide](guides/blockzilla-query-sdk.md) documents the
  implemented source-neutral models, request policy, adapters, sinks, and
  receipts.
- [Archive V2 hot-block format](reference/archive-v2-hot-block-format.md)
  documents the files and records implemented by `blockzilla-archive-v2` and the
  Blockzilla builders.
- [Ordered V2 pipeline](design/reader-pipeline-rolling-window.md) documents
  continuous worker dispatch, ordered output, resource limits, and cancellation.
- [Indexed USDC balances](reference/usdc-indexed-balances-v1.md) documents the
  optional compact balance stream, source-scoped dictionary, and checked expansion.
- [Hivezilla provider decision](reference/hivezilla-provider-decision-2026-09-26.md)
  selects Shyft for the first trial, audits the old volume estimate, and defines
  the checks still needed for complete block capture and recovery.
- [Live source research](reference/live-source-providers-2026-09-25.md) records
  the earlier gRPC, shred, bare-metal and repair-source comparison. Its estimates
  and vendor claims are not live provider test results.
- [Dependency review](reference/dependency-review-2026-09-06.md) records current
  versions, major-release changes, and required protocol compatibility limits.
- [JavaScript and build tools](reference/js-dependency-review-2026-09-06.md)
  records package updates and application/Worker build checks.
- [Test suite review](reference/test-suite-review-2026-09-06.md) explains the
  removed duplicate checks, shared fixtures, and retained behavior tests.
- [Old Faithful slot ranges](../crates/old-faithful/of-slot-ranges/README.md)
  documents the builders, repair tools, and validators beside the CAR reader.
- [Epoch 300 dependency retest](benchmarks/epoch-300-dependency-review-2026-09-06.md)
  records the later V2 prefix comparison, exact counters, timings, and allocation
  changes after the dependency update.
- [Epoch 300 rolling-pipeline results](benchmarks/epoch-300-rolling-pipeline-2026-09-06.md)
  records full-epoch V2 timings and exact output checks for that frozen build.
  Earlier benchmark documents retain the results for their original builds.
- [Block-time gap sidecar](reference/block-time-gap-sidecar.md) documents the
  locally derived slot/time discontinuity file emitted by current builders.
- [Blockzilla scheduler](../blockzilla/cli/README.md#scheduler) documents the
  experimental finite-work scheduler and its read-only status boundary.
- [Blockzilla monitor](../blockzilla/monitor/README.md) documents the
  separate read-only operational UI and API contract.
- [Hivezilla validator dashboard](../hivezilla/dashboard/README.md) documents
  the loopback-only Topcoat page for validator metrics and canary status.
- [Validator Tunnel setup](architecture/hivezilla-validator-tunnel.md) records
  continuous operation, the prepared private gRPC route, and the Cloudflare
  access permissions still required for client use.
- [Yellowstone full-block profile](../hivezilla/deploy/yellowstone-blocks-only/README.md)
  records the reduced account-copy path, live test, metrics, and rollback files.
- [Hivezilla raw shred hook](../hivezilla/deploy/agave-shred-hook/README.md)
  records the staged receive hook, packet preservation test, and restart plan.
- [External FireWatch local archive indexing](guides/firewatch-local-archive-indexing.md)
  hands off the completed-epoch read SDK and authenticated Range-gateway flow;
  the FireWatch adapter itself remains work for the separate `ferno-watcher` repository.
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

- [Local validator execution test](../hivezilla/service/EXECUTION-CANARY.md):
  finalized current-schema capture, verified Compact V2 test output, and
  retained raw evidence under a separate source profile.
- [Live validator test operations](architecture/hivezilla-live-canary-operations.md):
  pinned Agave and Yellowstone releases, private local endpoints, and bounded
  startup and capture controls.

- [Blockzilla Index Archive design](design/blockzilla-index-archive.md): the
  design history for the Archive V3 replacement. Use the
  [V3 format reference](../crates/archive-v3/blockzilla-archive-v3/README.md) for
  the implemented canonical layout and its
  [converter guide](../crates/archive-v3/blockzilla-archive-v3-convert/README.md)
  for current commands.
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

- [User-program-index naming and compatibility](design/user-program-index-naming.md)

- [Full V2/V3 disk and network report, with CAR and Jetstreamer reference](benchmarks/full-v2-v3-reader-20260911.md)
- [Earlier reader performance summary](benchmarks/tiny-reader-performance-20260909.md)
- [Earlier all-sample reader details](benchmarks/all-samples-reader-comparison-2026-09.md)
- [CAR to V3: short format story](benchmarks/from-car-to-v3-accepted-results-20260908.md)
- [Epoch900 network reader pilot](benchmarks/network-reader-epoch900-pilot.md)

Latest reader diagnostic: [full epoch 1000 resident CAR decode profile](benchmarks/triton-resident-decode-profile-20260930.md).
Follow-up: [measured buffer-search and receipt-hash changes](benchmarks/triton-resident-decode-optimization-20260930.md).
Next code test: [metadata allocation reuse](benchmarks/triton-resident-metadata-reuse-20260930.md).
Earlier network diagnostic: [epoch 900 CPU profiles and resource use](benchmarks/epoch900-network-profile-20260908.md).

Latest allocation pass: [CAR metadata allocations and network capacity](benchmarks/car-allocation-network-20260909.md).
