# Blockzilla future work

This is the canonical short backlog for cross-cutting work and product decisions.
Detailed implementation checklists stay in their linked design and operations
documents rather than being duplicated here.

## Decisions

- Keep archive registry IDs epoch- and generation-local. Per-epoch
  `registry.mphf` files and generation-bound derived indexes provide lookups
  without loading every registry. Consider a separate cross-epoch
  presence/translation index only if measured multi-epoch queries justify it.
- Treat signer-to-program relations as a generic Blockzilla capability.
  Firewatch is the first consumer, not the owner of the format. Its current
  semantics remain explicit: successful transactions only; every required
  signer maps to every directly or indirectly invoked program.
- Keep broader account-to-program and inverse program-to-signer indexes as
  separate, versioned capabilities if concrete product queries justify them.
- Prefer additive indexes and sidecars. Measure compressed message and metadata
  proportions before changing Archive V2 for partial metadata or
  inner-instruction reads.
- Prefer immutable object-store publication for shared delivery. Keep the NAS
  archive gateway authenticated and privately cacheable unless a separate
  digest-addressed CDN and authentication design is approved.

## Next

### Generic signer-to-program index

The implementation currently lives in `indexer/blockzilla-firebase-indexer/`;
the package name is provisional because the index is not Firewatch-specific.

- [ ] Choose a product-neutral public crate, binary, and format name before
      release. `blockzilla-signer-program-index` describes the current
      semantics without overstating them as all wallet or account activity.
- [ ] Rebuild epoch 900, compare counts, and spot-check known signers and
      programs.
- [ ] Measure full-epoch release memory, build time, and cold/warm query
      latency before choosing production thread and shard settings.
- [ ] Move full shard verification out of each lookup and benchmark a
      long-lived query service with verified handles cached.
- [ ] Implement the Firewatch adapter and durable checkpoint described in
      [Firewatch local archive indexing](docs/guides/firewatch-local-archive-indexing.md).

The detailed index status, validation gates, and remaining risks live in the
[signer-to-program redesign](indexer/blockzilla-firebase-indexer/REDESIGN.md).

### Archive lifecycle

- [ ] Finish streaming structure, blockhash, and signature verification
      receipts.
- [ ] Implement a generation-safe archive upgrade/remapping workflow.
- [ ] Migrate the remaining `first_seen` generations to `usage_sorted`.
- [ ] Garbage-collect replaced generations only after semantic parity and
      publication checks pass.

See the [archive completion audit](docs/operations/archive-completion-audit-2026-08-04.md)
for the concrete integrity and migration backlog.

## Later or decision-gated

- [ ] Turn the manual NAS release process into an idempotent installer covering
      binaries, checksums and provenance, configuration, systemd units, atomic
      swap, and rollback. The deployed layout is already defined in the
      [NAS deployment layout](docs/operations/nas-deployment-layout.md).
- [ ] Implement the deferred Unix-socket scheduler control and task protocol
      specified in the
      [scheduler control protocol](docs/operations/scheduler-control-protocol.md).
- [ ] Connect committed local archives to R2/B2 publication and implement the
      planned `blockzilla sync` and `blockzilla stream` flow.
- [ ] Define and benchmark account-to-program semantics if multisig or account
      discovery becomes a concrete requirement.
- [ ] Add program-to-signer postings only if inverse queries become necessary.
- [ ] Measure indexing decode costs before designing metadata subranges or an
      inner-instruction projection. Any such change requires a versioned format;
      see [Archive V2 evolution](docs/design/archive-v2-evolution.md).
- [ ] Revisit a cross-epoch pubkey presence/translation index only after
      measured demand demonstrates a benefit over per-epoch lookups.
- [ ] Complete catalog-bound immutable edge publication rather than enabling
      shared caching for authenticated mutable NAS paths.

## Reconciled or complete

- [x] Blockzilla is installed and running on the NAS.
- [x] The NAS folder and service layout is documented and deployed.
- [x] Archive inspection, dump, analysis, log reparse, and repack commands
      exist.
- [x] Generation-bound signer discovery and signer-to-program indexing exist.
- [x] An authenticated read-only Range gateway exists for completed archives.
- [x] The proposed Unix-socket operator protocol has a written design; its
      implementation is intentionally deferred.

Build dashboard for Sanctum
https://x.com/JamesHanley/status/2085262007834644821