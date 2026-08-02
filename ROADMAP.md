# Blockzilla open-source roadmap

Blockzilla is the main product and the only authority allowed to commit a
canonical Archive V2 generation to the catalog. Hivezilla protects live source
continuity and, under a fenced job, performs the physical compact/upload work.
Edgezilla exposes committed replicated archives for read-only access, and the
planned Blockzilla Streamer will feed local indexers from those archives.

Status words in this roadmap are intentional:

- **implemented** means the code is present and tested in this repository;
- **planned** means the contract is agreed but the product path does not exist;
- **research** means the design is still open.

The proposed V1 contracts and code-level dependency order are summarized in the
[Hivezilla V1 implementation assessment](docs/architecture/hivezilla-v1-implementation-plan.md).

## What is implemented now

- The `blockzilla` CLI builds Archive V2 from CAR/CAR.ZST, can finalize current
  Hivezilla capture artifacts, and provides inspection, validation, repair, and
  benchmark commands.
- The hot-block Archive V2 writer, indexes, sidecars, and readers are
  implemented. The current contract is documented in the
  [format reference](docs/reference/archive-v2-hot-block-format.md).
- The `services/hivezilla/` folder contains the current Yellowstone capture
  and repair prototype, including durable raw recording, mTLS push/pull
  replication, signed acknowledgements, and the `hivezilla` executable.
- The `blockzilla scheduler` command restores the finite archive inventory,
  repair/finalization scheduling, and read-only status API as an experimental
  contributor-facing implementation.
- `services/blockzilla-get-block/` contains a read-only R2-backed Worker and
  native inspection/correctness tools.
- `services/old-faithful-get-block/` contains the restored, buildable,
  read-only CAR-backed compatibility Worker. It is experimental and is not the
  canonical Blockzilla Archive V2 serving path.
- The optional token API is an example under `examples/token-api/`.

The repository does **not** yet contain `blockzilla sync`, `blockzilla stream`,
a production Blockzilla ingest boundary or publisher, a finished shred-to-block
promotion path, an isolated public Hivezilla exit, or an integrated R2/B2
publication pipeline. The recovered scheduler is experimental rather than a
production support promise.

## Phase 0 — publication safety

Status: completed for the current public refs; repeat before each release.

- Preserve the curated `main` history as evidence of the project's development.
- Keep private operations, credentials, incident data, and machine-specific
  reports outside the public checkout.
- Audit every public Git and pull-request ref for secrets and rotate anything
  that may have been exposed.
- Run secret scanning in CI before broader promotion.

Exit condition: every public ref is intentional and the tracked tree passes a
redacted history scan.

## Phase 1 — understandable public repository

Status: implemented by the current cleanup; continue refining documentation.

- Keep the main Blockzilla product in `blockzilla/` and deployable supporting
  processes in `services/`.
- Keep reusable format/reader libraries in `crates/`, examples in `examples/`,
  contributor scripts in `scripts/`, and public material in `docs/`.
- Expose `blockzilla` as the default product binary. Keep diagnostic,
  benchmark, and migration binaries explicitly opt-in.
- Remove Compact V1 commands from the public CLI while retaining compatibility
  decoding where a current reader still requires it.
- Keep every Edgezilla Worker surface read-only.
- Keep the experimental Old Faithful Worker as an explicit compatibility and
  reference implementation, outside the canonical Archive V2 flow.
- Label proposed architecture separately from current implementation.

`CODE_OF_CONDUCT.md` and `CONTRIBUTING.md` are deliberately deferred for now.

## Phase 2 — reproducible local archive path

Status: CAR build is implemented; the public fixture workflow is being
standardized.

- Maintain one small deterministic fixture path covering build, index
  validation, and block read.
- Define generation manifests and hashes for an Archive V2 epoch directory.
- Make incomplete or bounded smoke builds impossible to mistake for a complete
  validated generation. Canonical completion/catalog publication is Phase 5.
- Reduce the stable CLI to archive operations; keep research commands opt-in.

Exit condition: a new contributor can build and read the fixture without any
deployment configuration.

## Phase 3 — Blockzilla Streamer

Status: planned. `blockzilla sync` and `blockzilla stream` are not commands yet.

- Add a reusable committed-archive reader used by both commands.
- Render deterministic canonical blocks with stable archive event identities.
- Start with sequential local Archive V2 replay, stable event identities,
  per-block sink acknowledgements, and durable logical checkpoints.
- Add verified download/range-read support for committed edge epochs.
- Provide one minimal Rust indexer sink instead of coupling Blockzilla to a
  particular database.

Streamer reads only Blockzilla-committed local or edge archives. It never reads
Hivezilla source spools directly and never uses the point-read Edgezilla Worker
for bulk backfill.

## Phase 4 — Hivezilla source continuity

Status: Yellowstone capture foundations are implemented; the product boundary
and multi-source runtime are planned.

- Stabilize the public `hivezilla` command and its delivery protocol before
  treating either as a compatibility promise.
- Support multiple independently supervised Yellowstone gRPC and shred source
  instances, each with its own immutable manifest, identity, lineage, cursor,
  WAL, gap-event stream, and failure boundary.
- Publish a centralized, monotonically versioned Blockzilla stream registry for
  capture nodes, terminal consumers, processors, and public exits. Cache it for outages; do
  not add a gossip membership or peer-routing plane.
- Give each source node a private temporary cloud-overflow namespace. A sealed
  upload with a provider-verified end-to-end checksum (or verified read-back)
  may allow local eviction under disk pressure only after its receipt, range
  catalog, and `local_evicted_through` checkpoint are durable. It does not retire
  the logical record or count as another custodian.
- Preserve replayable raw evidence when normalization fails; compact delivery
  is an optimization, not the only recovery copy.
- Define typed, authenticated, idempotent delivery to one configured permanent
  logical Hivezilla raw dataset. It ACKs only after self-describing exact
  immutable objects, enough verified copies in distinct policy failure domains,
  and their rebuildable range/copy index are durable. Its one protected
  exact-prefix ACK makes covered source data eligible for retirement; the source
  records the ACK and retirement anchor before deleting local or cloud copies.
  Physical targets do not ACK independently.
- On terminal-consumer reconnect, atomically choose cutover `T`, resume the live
  lane at `T`, and bulk-download a session-fenced idempotent bounded partition of the fixed
  missing range `[C, T)` with independent resource budgets.
- Reconstruct cloud-only source history from deterministic self-describing
  overflow objects after source-host loss; never treat bucket listing as proof
  that an unknown local tail did not exist.
- Keep all live gRPC and shred sockets in Hivezilla.
- Turn the existing FEC/deshred foundations into a continuous processor that
  tails durable shreds and writes complete, source-labelled block observations
  to a bounded derived journal only after scheduled-leader, signature,
  proof/FEC-root, completion-marker, and full-PoH validation for non-genesis
  slots; slot zero instead must match deterministic entry/PoH construction from
  digest-bound genesis data.
- Isolate public egress from source recorders. Serve named raw-shred and derived
  block-observation feeds with independent cursors, queues, and quotas; public
  subscribers never participate in HiveSync custody or retention.
- Feed raw exits through a bounded post-fsync, non-custodial fan-out. Its failure
  may shorten the exit cache but cannot block capture or expose source storage.
- Serve historical raw records only from an exit's bounded live cache or the
  permanent terminal dataset. Never let anonymous history reads touch a capture
  spool or private overflow bucket; distinguish explicitly recoverable
  `HISTORY_PENDING`, declared `HISTORY_LOST`, and transient `UNAVAILABLE`.
- Return an explicit feed-specific replay-unavailable response when an exit
  cursor expires. Keep Blockzilla canonical lookup distinct from exact replay.

Exit condition: a Blockzilla outage stops canonical progress without silently
stopping source capture or independent Hivezilla live feeds within configured
capacity.

## Phase 5 — Archive scheduling and catalog authority

Status: finite builders and an experimental scheduler are implemented;
production boundary integration and publication remain planned.

- Integrate the permanent terminal Hivezilla raw dataset and exact replay path with
  Blockzilla cross-source repair and completeness gates.
- Stabilize the recovered finite idempotent scheduler jobs and their durable
  ownership/provenance contracts using the
  [V1 compaction job protocol](docs/design/blockzilla-compaction-job-v1.md).
- Keep scheduling and the canonical manifest/catalog commit in Blockzilla;
  execute compaction and object upload in the Hivezilla binary.
- Grant exactly one fenced active compaction lease in V1. Restart the worker and
  replay from the permanent raw dataset after failure; defer cross-host election
  and active standby.
- Independently reconstruct and validate only the job's pinned raw/finite
  evidence; bounded Hivezilla observation journals are not V1 job inputs.
- Have the leased Hivezilla worker upload immutable payloads and indexes first.
  Its immutable job binds exact input ranges, content-addressed finite inputs,
  validation/finality policy, one complete published epoch, any bounded
  descendant lookahead needed to validate finality, epoch schedule, output
  format, fixed zero-based ID order, predecessor, and monotonic fence.
  Blockzilla verifies the candidate result, writes the completion manifest,
  then compare-and-swaps the predecessor-bound catalog head last.
- Enumerate every job slot as produced, skipped, or unresolved in an immutable
  finality manifest carrying final PoH and the era-defined optional separate
  consensus/shred block ID. Lookahead slots settle the published epoch but are
  not output in that generation. Missing evidence and unresolved required slots
  fail closed.
- Persist and independently back up the exact catalog-head checkpoint. Recovery
  follows the committed predecessor chain and never promotes the largest
  unreferenced candidate discovered by object listing. Alert on exact-head
  backup lag, verification failure, and restoration.
- Scope worker write credentials to job output and grant only read access to
  pinned inputs/finality; keep canonical commit authority and conflict fencing
  in Blockzilla.

Exit condition: CAR and Hivezilla evidence converge on the same validated
Archive V2 contract, and incomplete data cannot be published as canonical.

## Phase 5R — Replay V1 and shred-only migration

Status: specified; implementation planned. Payload format 8 remains reserved
and rejected by executable validators until its schemas, hard bounds, fixtures,
and product-specific catalog contracts land atomically.

- Produce the minimal Replay V1 execution projection from verified shreds:
  exact signed-message payloads without outer transaction signatures, one
  signature mixin per non-empty entry, ordered component/entry boundaries, and
  exact state-changing block markers. Preserve launch-era duplicate behavior
  with sparse recent-blockhash-scoped 20-byte status-key classes plus an exact
  known-offset/all-offset proof receipt, not signature bytes.
- Use a replay-scoped profitable address registry with raw singleton fallback;
  do not add a second program-address registry or let gRPC metadata affect IDs.
  Program-role hints are an external rebuildable cache, not Replay payload or
  catalog state.
- Pin exact genesis/checkpoints, runtime/features, historical signed-entry or
  shred evidence, and immutable validation ranges. Stateful replay covers the
  descendant prefix required by finality; a possibly longer status-only suffix
  streams collision evidence through cohort eviction and never fabricates a
  finality receipt. Neither suffix is published in the epoch. Derive omitted
  entry hashes, verify final PoH and Bank/account hashes, replay the exact final
  encoded bytes, and publish their validation receipt through a Replay catalog
  head distinct from Archive V2. Commit the resulting full successor checkpoint
  and require it byte-for-byte as the next epoch's input.
- Protect each committed Replay generation through its own verified recovery
  mapping/receipt and exact recovery-head CAS, including validation,
  attachments, status evidence, and successor checkpoint chunks. Alert and
  drill lag, corruption, head loss, and exact restoration independently of
  Archive recovery.
- Regenerate results, fees, CPI, logs, return data, loaded addresses, balances,
  rewards, block height, and canonical time as immutable Compact V2-compatible
  runtime attachments. Full account writes advance replay/checkpoint state but
  are optional instrumentation. Keep signatures and transaction IDs recoverable
  only from bound permanent raw shreds.
- Consume both shreds and gRPC during shadow operation: shreds are permanent
  ledger/FEC evidence; gRPC is the current runtime source and parity oracle.
  At the epoch-boundary cutover, keep raw signed-shred ledger/finality inputs
  unchanged and switch only the selected runtime attachment to `FINAL_REPLAY`.
  Continue complete non-selected gRPC capture through a fixed rollback horizon;
  only then make an explicit irreversible seal/ACK/production-retirement
  decision. Protected terminal gRPC evidence remains discoverable and auditable
  but becomes policy-ineligible for rollback; a sampled canary cannot support
  rollback. Compact V2 pins a committed Replay
  dependency and rejoins signatures only from raw signed evidence. Any required external
  RPC, gRPC, Tower/status, or provider finality feed means shred-primary, not
  shred-only.
- Migrate external finality separately: shadow marker/certificate-derived slot
  identities and dispositions against the current authority, switch only a
  future epoch's immutable finality rule, retain the complete authority feed for
  a declared rollback horizon if needed, then explicitly demote/retire it from
  future resolution policy. Retained authority evidence stays audit-only. If
  shreds/markers cannot independently settle every slot, do not claim
  shred-only.

Exit condition: after both the runtime-attachment and finality-policy migrations
and their declared rollback horizons close, Compact V2 is built from verified
shred-derived Replay V1 plus regenerated attachments with zero unexplained
semantic mismatches. Verified shreds/markers and pinned
genesis/protocol/features/checkpoints are the sole continuing ledger, finality,
and runtime inputs; production gRPC/external-finality feeds are retired from
policy while their protected terminal evidence remains auditable. Raw shreds
remain permanent evidence.

## Phase 6 — Edgezilla replicated read plane

Status: the R2 read-only Worker is implemented; catalog-controlled publication
and independently verified B2 recovery are planned.

- Treat the Edgezilla archive as one boundary with a configured online target
  and an independently verified recovery target for the same committed
  generations; R2 online plus B2 recovery is the initial deployment, not a
  protocol identity.
- Alert on recovery-copy lag or verification failure and emit recovery only
  after the exact committed generation is independently verified again.
- Run copying through a replaceable least-privilege archive copy/audit process
  or provider replication. Persist the complete canonical-to-recovery mapping,
  predecessor-linked verification receipt, and exact recovery checkpoint. It
  receives no catalog, raw-ACK, immutable-overwrite, or delete authority; its
  sole mutable write is exact CAS on that checkpoint.
- Give the leased Hivezilla archive worker scoped object-write credentials;
  keep manifest/catalog commit authority in Blockzilla and Edgezilla read-only.
- Serve `getBlock` only from generations whose completion manifest and object
  sizes match the published contract.
- Add shared correctness fixtures for native and Worker readers.

Exit condition: remote copies are independently verifiable and public reads can
never observe a partially published generation.

## Continuous contributor gates

Every phase should keep:

- Rust formatting, workspace check, and tests green;
- the Worker WASM build checked separately from native binaries;
- the deterministic build/validate/read fixture green;
- Markdown links valid;
- public docs free of credentials, hostnames, incidents, private paths, and
  deployment-specific tuning;
- storage/protocol compatibility changes explicit and fixture-backed.

Add clippy, dependency/license policy, release artifacts, and more platform
coverage when the corresponding public support promises are made.
