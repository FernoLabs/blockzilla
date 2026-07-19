# Blockzilla open-source roadmap

Blockzilla is the main product and the only component allowed to publish a
canonical Archive V2 generation. Hivezilla protects live source continuity,
Edgezilla exposes replicated archives for read-only access, and the planned
Blockzilla Streamer will feed local indexers from committed archives.

Status words in this roadmap are intentional:

- **implemented** means the code is present and tested in this repository;
- **planned** means the contract is agreed but the product path does not exist;
- **research** means the design is still open.

## What is implemented now

- The `blockzilla` CLI builds Archive V2 from CAR/CAR.ZST, can finalize current
  Hivezilla capture artifacts, and provides inspection, validation, repair, and
  benchmark commands.
- The hot-block Archive V2 writer, indexes, sidecars, and readers are
  implemented. The current contract is documented in the
  [format reference](docs/reference/archive-v2-hot-block-format.md).
- The `services/hivezilla/` folder contains the current Yellowstone capture
  and repair prototype, including the `hivezilla` executable.
- `services/blockzilla-get-block/` contains a read-only R2-backed Worker and
  native inspection/correctness tools.
- `services/old-faithful-get-block/` contains the restored, buildable,
  read-only CAR-backed compatibility Worker. It is experimental and is not the
  canonical Blockzilla Archive V2 serving path.
- The optional token API is an example under `examples/token-api/`.

The repository does **not** yet contain `blockzilla sync`, `blockzilla stream`,
a production Blockzilla ingest server/scheduler/publisher, a finished shred
adapter, or an integrated R2/B2 replication pipeline.

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
- Define completion manifests and hashes for an Archive V2 epoch directory.
- Make incomplete or bounded smoke builds impossible to mistake for a complete
  canonical epoch.
- Reduce the stable CLI to archive operations; keep research commands opt-in.

Exit condition: a new contributor can build and read the fixture without any
deployment configuration.

## Phase 3 — Blockzilla Streamer

Status: planned. `blockzilla sync` and `blockzilla stream` are not commands yet.

- Add a reusable committed-archive reader used by both commands.
- Start with sequential local Archive V2 replay, stable event identities,
  per-block sink acknowledgements, and durable logical checkpoints.
- Add verified download/range-read support for committed edge epochs.
- Provide one minimal Rust indexer sink instead of coupling Blockzilla to a
  particular database.

Streamer reads only Blockzilla-produced local or edge archives. It never reads
Hivezilla directly and never uses the point-read Edgezilla Worker for bulk
backfill.

## Phase 4 — Hivezilla source continuity

Status: Yellowstone capture foundations are implemented; the product boundary
and multi-source runtime are planned.

- Stabilize the public `hivezilla` command and its delivery protocol before
  treating either as a compatibility promise.
- Support multiple independently supervised Yellowstone gRPC and shred source
  instances, each with its own identity, cursor, WAL, and failure boundary.
- Preserve replayable raw evidence when normalization fails; compact delivery
  is an optimization, not the only recovery copy.
- Define typed, authenticated, idempotent deliveries and durable ACKs to
  Blockzilla.
- Keep all live gRPC and shred sockets in Hivezilla.

Exit condition: a Blockzilla outage stops canonical progress without silently
stopping source capture within configured capacity.

## Phase 5 — Blockzilla storage authority

Status: planned around the implemented finite builders.

- Add a durable Hivezilla receiver, raw fallback staging, cross-source repair,
  completeness gates, and finite idempotent scheduler jobs.
- Run scheduling and publication with the Blockzilla storage authority, not in
  Hivezilla or Edgezilla.
- Publish payloads and indexes first and a verified completion manifest last.
- Make Blockzilla the sole writer for both the canonical local archive and its
  R2/B2 copies.

Exit condition: CAR and Hivezilla evidence converge on the same validated
Archive V2 contract, and incomplete data cannot be published as canonical.

## Phase 6 — Edgezilla replicated read plane

Status: the R2 read-only Worker is implemented; Blockzilla-owned publication
and independently verified B2 recovery are planned.

- Treat the Edgezilla archive as one boundary: R2 is the online copy and B2 is
  an independently verified recovery copy of the same committed generations.
- Keep replication and publication credentials in Blockzilla; Edgezilla has
  read access only.
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
