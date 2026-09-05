# Workspace restructure

Status: agreed target layout, 2026-09-05. Branch `refactor`.

The workspace grew by accretion: 23 packages split across `crates/`, `services/`,
`workers/`, `apps/` and a top-level `blockzilla/`, with categories that reflect
deployment target rather than subject. Merging `codex/sample-archive-benchmark`
adds roughly fourteen more crates, so the layout is decided before that merge
rather than after.

## Principle

**Group by subject, not by architectural layer.**

Earlier drafts used `core/`, `format/`, `parse/`, `types/`, `transport/`. Those
are implementation words; they read as arbitrary because the same crate can
plausibly belong to two of them. Every folder below is named after something
that exists in the domain — an archive format, a Solana convention, a role —
so "where does the V3 reader go" answers itself.

The one exception is `compat/`, which earns a folder for an operational reason:
retiring the legacy path must be `git rm -r crates/compat` plus two workspace
member lines. If that command does not work, the quarantine was never real.

## Target layout

```
crates/
  blockzilla-model                 canonical block model + ordering contract
                                   (was blockzilla-query-sdk)
  blockzilla-transaction-error     the error enums, currently transcribed 4x

  source/                          byte transport only
    blockzilla-source              RangeSource trait
    blockzilla-source-local        Local, PinnedLocal, Overlay
    blockzilla-source-http         HTTP range + ETag/length pinning
    blockzilla-source-cache        on-disk object mirror

  solana-codec/                    Solana's wire conventions, not ours
    solana-shred-codec             was blockzilla-shred-codec
    solana-rpc-codec               getBlock JSON + protobuf shapes
    solana-grpc-codec              Geyser / yellowstone shapes

  parser/
    blockzilla-log-parser
    blockzilla-dex-parser

  car/
    of-car                         types
    of-car-reader                  reader (absorbs blockzilla-car-read-sdk)
  compact-v2/
    blockzilla-compact             record model
    blockzilla-registry            registry + MPHF, blockhash registry
    blockzilla-archive-v2          container
    blockzilla-compact-v2-reader   engine + facade merged
  index-archive/
    blockzilla-index-archive       container + sidecars
    blockzilla-index-archive-reader

  index/
    blockzilla-signer-program-index    blockzilla-spyx-query
    blockzilla-user-program-index      blockzilla-token-transaction-dump
    blockzilla-token-metadata-index    blockzilla-token-balance-audit

  compat/                          deletable in one command
    blockzilla-archive-v2-compat   legacy decoders
    blockzilla-archive-v2-migrate  converters + migration bins

runtime/
  blockzilla-replay                SVM experiments

blockzilla/
  cli/                             + index-archive convert subcommands
  archive-gateway/                 NAS origin, authenticated Range
  monitor/

hivezilla/
  service/  protocol/  object-store/
  ledger-compat/                   Agave shred parsing for replay
  gossip-compat/                   gossip / repair peer accounting

edgezilla/
  get-block/  of-get-block/  archive-samples/  r2-gateway/

examples/
  workloads/                       shared sinks: firewatch, pump, usdc, identity
  read-car/  read-compact-v2/  read-indexer-v3/  token-api/

bench/
  reader-profile/  index-archive-measure/

web/    docs/    scripts/
```

Every format folder has the same shape: **types plus one reader**. That symmetry
is why `blockzilla-car-read-sdk` folds into `of-car-reader` and
`blockzilla-compact-v2-read-sdk` folds into the V2 engine.

## Decisions and why

### `blockzilla-model` is kept, and renamed

It was nearly deleted on the belief that no cross-format interface exists. That
belief is wrong, and the evidence is not the trait:

- `blockzilla-example-workloads` has **exactly one workspace dependency** and
  zero format SDKs. Four sinks by three readers is twelve binaries running
  identical sink code.
- The sample harness **byte-compares output across formats**
  (`scripts/archive_sample_matrix.py:219-230`).
- The boundary is **CI-enforced**
  (`examples/archive-token-events/tests/sdk_boundary.rs:11-24`).
- `OrderedBlockPublisher` is shared ordering and coverage enforcement
  instantiated inside all three engines.

Deleting it forks twelve workload binaries into three divergent copies, kills
the cross-format parity assertion, and breaks 29 example call sites, because
neither `identity()` nor `scan_ordered()` has an inherent implementation.

The rename is honest about the balance: the `ArchiveInstructionSource` trait is
thin — every fast path bypasses it, `scan_ordered_parallel` is inherent on both
V2 and V3, and `blockzilla-reader-profile` wrote its own `enum Archive { V2, V3 }`
rather than importing it. What carries weight is the shared block model and the
ordering contract. Name it for that.

### Solana conventions are separated from Blockzilla's

`solana-ledger-compat` (636 lines), `solana-gossip` (497) and
`blockzilla-shred-codec` (110) describe Agave's shapes, not ours — the last one
says so in its own docstring while carrying a `blockzilla-` prefix.

They split by consumer rather than moving wholesale: shred, RPC and gRPC codecs
are needed by hivezilla, the get-block workers, and any later RPC or forwarding
service, so they belong in `crates/solana-codec/`. Ledger and gossip compat are
hivezilla-only (Agave shred parsing for replay, gossip peer accounting) and stay
beside it.

### `source/` must be extracted before the V2 facade merges

`blockzilla-read-sdk` currently holds the V2 decode engine **and** the shared
range sources and HTTP cache. That is why `blockzilla-indexer-v3-read-sdk`
depends on it — a V3 crate reaching into the V2 crate for plumbing.

Extract transport first and each format folder becomes self-contained; the
facade merge is then just folding 821 lines into the engine. Do it in the other
order and the merge target is a crate V3 still reaches into.

### No shared RPC renderer yet

The two get-block workers were believed to duplicate ~2,600 lines. They do not:
genuinely cloned text is ~150-210 lines, dominated by two error renderers at
0.99 similarity. But they are also not cleanly separate converters — the two
**RPC** paths have never been diffed, and the comparison that suggested they
were compared Old Faithful's RPC renderer against Blockzilla's REST renderer.

What is ready is `blockzilla-transaction-error`: ~350 lines absorbed plus ~149
lines of `From` impls deleted, with the enums currently transcribed four times.
A shared renderer waits until the two RPC paths are diffed and their wire
divergences fixed — see the separate conformance work.

### Deletions

- `blockzilla-archive-sdk` — first-generation unified facade, superseded three
  days after landing (`4a0fa59b` -> `b3c5765b`), never touched since,
  sequential-only. It also defines `ArchiveIoSnapshot` a second time publicly
  with 7 fields against the canonical 8, silently dropping
  `incomplete_body_retries`. Its only consumer is
  `examples/archive-token-events`, which moves to a facade.
- `blockzilla-index-archive-convert` — becomes `blockzilla` CLI subcommands. It
  ships six bins today (`ia-index`, `ia-read`, `ia-program-index`,
  `ia-selector-index`, `ia-validate-candidate`, `ia-build-basic-indexes`), so
  this is real work and gets its own step.

### Known asymmetries, accepted

`of-car-reader` will own decoding *and* network layout admission, where V2 and
V3 keep those separate. Acceptable because CAR is an interop input that is only
read, never produced.

`blockzilla-compact` sits inside `compact-v2/` although Index Archive reuses the
record model. Readability was preferred over strict layering. A line in
`compact-v2/README.md` should state that the record model is shared and the
container is not, so the dependency is not misread as "V3 depends on V2".

## Sequencing

Mechanical and semantic changes never share a commit. Each step ends with a
green `cargo check --workspace --all-targets`.

1. **Snapshot the working tree.** Done — `05406382` on branch `refactor`.
   84,713 lines across four crates existed only in the working tree.
2. **Merge `codex/sample-archive-benchmark`.** 20 commits, diverged at
   `f5ad4758`. Must precede any move: its commits touch the current paths.
3. **Extract `source/`** from `blockzilla-read-sdk`.
4. **Merge the V2 facade** into the engine; rename to
   `blockzilla-compact-v2-reader`.
5. **Split `blockzilla-format`** into `blockzilla-compact`,
   `blockzilla-registry`, `blockzilla-archive-v2`.
6. **Extract `solana-codec/`**; leave ledger and gossip compat in hivezilla.
7. **Quarantine `compat/`.** Gated on the V2 freeze — `ArchiveV2WireProfile` has
   452 references across 26 files, so this is not mechanical.
8. **Fold the converter** into CLI subcommands.
9. **Move the folders.** Last, because it is the cheapest step to redo and the
   most expensive to merge against.

## Verification

The folder move is verified by target-set equality, not by inspection:

```bash
cargo metadata --no-deps --format-version 1 > before.json   # before moving
# move, rewrite workspace members and path dependencies
cargo metadata --no-deps --format-version 1 > after.json    # after
# compare package names and every target name+kind; the sets must be identical
```

This was exercised twice already on this branch — a 397-rename move and its
full reversal — and both times reported 23 packages and 69 binary targets,
byte-identical. Two `include_bytes!` provenance paths broke during the move and
the compiler caught both immediately.

Watch for cross-crate relative paths, which the target-set check does not
cover: `include_bytes!` / `include_str!` reaching outside a crate. Two exist
today, in `blockzilla-spyx-query/src/market_builder.rs` (source-provenance
hashes of the projectors) and `blockzilla/src/bin/big_block_log_bench.rs` (a CAR
fixture).
