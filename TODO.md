# Hivezilla / Blockzilla TODO

The ordered implementation plan is
[`docs/architecture/hivezilla-v1-implementation-plan.md`](docs/architecture/hivezilla-v1-implementation-plan.md).
This file is the short execution queue; it must not redefine protocol rules.

## Current migration slice: deploy gRPC ledger projection in shadow only

Implemented and regression-tested:

- The shared `BlockCandidateV1` is ledger-only, keeps final PoH distinct from
  optional consensus identity, preserves exact fixed signatures/signed-message
  bytes, and models marker-only shred ranges without duplicating PoH entries.
- The pure Yellowstone adapter requires complete indexed PoH, the exact source
  parent hash, consistent top-level/signature-zero identity, and canonical
  Legacy/V0 message bytes. Its structural projection is crate-private and has no
  finality, publication, ACK, or retention authority.
- `verify-grpc-raw-ledger-shadow` audits a stopped or snapshotted raw generation,
  projects every record in memory, prints deterministic counters, and writes no
  artifact.
- The pinned Yellowstone 12.4 schema's version ambiguity fails closed: every
  versioned row must have a fee-payer signature proving the reconstructed V0
  bytes. This detects V1 data loss but cannot recover the config field already
  dropped during known-schema capture.

Before any canonical or live deployment:

- Run the shadow command against a real immutable capture and alert on any
  projection failure; the current workspace has no deployment target or raw
  gRPC snapshot configured.
- Upgrade raw gRPC custody to retain an explicit message version/config (or
  exact upstream protobuf bytes) in a new stream schema before accepting V1.
- Add opaque evidence-bound promoted and finality-selected candidate types;
  Archive/Replay builders must never accept the structural type directly.
- Add an explicit historical entry-index policy for Solana 1.17 rather than
  heuristically accepting all-zero `starting_transaction_index` values.
- Build the lossless CAR adapter only after strict CAR header/root/DAG/EOF
  validation, exact transaction round trips, parent resolution, and era gates.
- Complete trusted shred promotion before producing the runtime-free Replay V1
  epoch product.

## Now: freeze Replay V1 before enabling payload 8

Implemented foundation, not activation:

- `blockzilla-format` now has bounded `REPLAY_PROJECTION_V1` hot-payload types,
  a strict canonical codec, a streaming decoder, and focused malformed-input
  tests.
- The compaction protocol now has finality-manifest V2 and requires the expected
  active fence when accepting an Archive candidate/completion.
- Payload format 8 remains rejected.

Remaining before the Replay freeze:

- Add golden fixtures for Legacy/V0/raw messages, status-key backreferences,
  canonical markers, PoH signature mixins, skipped slots, registries/tails, and
  byte-identical retries, pinned against the exact Agave adapter. Keep payload 8
  rejected until the entire fixture set lands atomically.
- Implement the replay-scoped registry/tail resolver and exact message
  expansion; the hot-payload codec alone is not a publishable Replay product.
- Implement `StatusKeyEvidenceV1`: pinned launch/current cache profiles, sparse
  known-index rows, streaming all-offset collision scan, exact digest, and
  checkpoint/live-working-set bounds.
- Sanitize and verify the complete original signed transaction before removing
  signatures. Bind every Replay output to permanent raw/signed evidence.
- Implement Replay finality-resolution and publication job/result/completion
  objects with immutable fence-free job-spec ObjectRefs and fence-only attempt
  envelopes; add the separate exact-CAS Replay catalog, final-byte validation
  receipt, explicit checkpoint transition policy, deterministic successor
  checkpoint, and Replay recovery-copy chain.
- Decode and statefully replay the exact final format-8 bytes before catalog
  commit; internal candidate replay is not sufficient.

## Next: deterministic replay and Compact V2 join

- Adapt the runtime to consume exact format-8 bytes from
  `ReplayStartStateV1`, skip only upstream Ed25519 work, reproduce native status
  semantics, and regenerate bounded runtime attachments in canonical chunks.
- Differential-test PoH, Bank/account roots, results, fees, compute, logs, CPI,
  balances, rewards, height, and canonical time against the retained complete
  gRPC baseline across epoch/feature/fork/failure cases.
- Implement `ReplayArchiveDependencyV1`. Join real outer signatures by
  `(slot, transaction_ordinal, exact signed message)` from declared raw evidence,
  recompute every entry mixin/PoH boundary, and never publish placeholders.
- Run the one worker under the frozen weighted fairness/attempt/backlog policy;
  prove live rate plus catch-up headroom before cutover.

## Cutover

- For the Replay attachment cutover, change only a future epoch Archive job
  policy from provider attachments to the committed Replay dependency; keep the
  selected finality authority unchanged.
- Keep complete gRPC capture Archive-eligible through the declared rollback
  horizon. Retire it only through the explicit irreversible seal/ACK/anchor
  milestone; protected terminal evidence remains auditable but rollback-ineligible,
  and a sampled canary is never a rollback source.
- Separately shadow marker/certificate-derived finality against the current
  external authority. Switch only a future epoch after exact identity/skip/fork
  parity; retain the external authority through its own rollback horizon if
  required, then explicitly demote/retire it from new resolution policy.
- Call the system `shred-only` only after shreds/markers plus pinned
  genesis/protocol/features/checkpoints are the sole continuing ledger,
  finality, and runtime inputs.

## Existing product backlog

- Clean up script/bin ownership; retain CAR download, PoH verification, compact
  build/repack, and single-block debugging tools.
- Build Blockzilla stream consumers for token/program indexers.
- Keep Watcher as the archive/ingest operational UI.
- Scale Edgezilla/`blockzilla-get-block` only from catalog-committed Archive V2.

## Explicitly deferred

- General P2P membership, election, or consensus.
- A second active compactor/catalog writer.
- A program-address registry or canonical program-hint sidecar.
- Raw-shred deletion based on Replay or Archive publication.
