# Reader IDs and SPYX account discovery — 6 September 2026

Status: source review. No new extraction or benchmark was run for this note.
The V2 implementation of compact USDC output is covered by local tests and a
completed NAS comparison. Exact expansion passes; output plus dictionary is
47.97% smaller. See [the measured results](epoch-300-indexed-usdc-and-allocation-retest-2026-09-06.md)
and [implementation and checks](reader-allocation-and-indexed-usdc-2026-09-06.md).
The standalone V3 example has not gained this optional output mode.

## What SPYX actually skips

The active SPYX extractor discovers successful SPL Token and Token-2022
`InitializeAccount`, `InitializeAccount2`, and `InitializeAccount3` instructions.
Discovery includes direct instructions and CPI. It records the first successful
creation of each account. It does not discover accounts from balance rows or
remove them after close/reuse events.

Failed transactions cannot add discovered accounts. However, raw-copy Pass B
**includes failed transactions** when their complete transaction account list
matches the mint or an eligible discovered account. The raw dump is a historical
superset, while the current USDC and Pump.fun examples exclude known failures.
These are different workloads. See the [extractor contract](../../indexer/blockzilla-token-transaction-dump/README.md)
and [extract.rs](../../indexer/blockzilla-token-transaction-dump/src/extract.rs):4216, 5876.

The older stateful extractor is disabled with `#[cfg(any())]` at `extract.rs:1535`.
Its fully resolved transaction-facts code is not the active creation-discovery path.

## Mechanisms that can be shared

| Mechanism | Evidence |
|---|---|
| Keep account references as epoch-local IDs or inline raw keys. Use fixed account arrays and reusable candidate/count vectors. | `extract.rs:250–305`; `DiscoveryScratch` retains storage across transactions and uses generation tags for loaded accounts. |
| Reuse known mappings before reading the registry. Sort and deduplicate new candidates, then resolve them in bounded batches. | `extract.rs:4554–4582`, `4689–4716`; `resolve_creation_candidates_bulk` at `752`. |
| Verify a positive MPHF lookup against the exact 32-byte registry row. Keep the registry file open and pinned. | `extract.rs:525–601`; source IDs are one-based and use byte offset `(id - 1) * 32`. |
| Match IDs with bitsets and retain the exact creation position. | `extract.rs:1322–1388`; newly found accounts cannot match an earlier transaction in the same batch. |
| Avoid reading and decompressing a source batch twice. | Single-read mode runs discovery, merges new accounts in source order, then copies from the retained decoded batch. Optional match hints recheck negative results when a batch adds accounts. See the extractor README. |

The regression test at `extract.rs:9979` checks repeated discovery across batches.
The second occurrence adds no registry row read, read call, or MPHF lookup. Its
fixture reads 96 registry bytes in total, including the mint and token program.
This is a test assertion, not a new performance measurement.

## Mapping records and later reconciliation

[format.rs](../../indexer/blockzilla-token-transaction-dump/src/format.rs):154–211
already defines the required provenance:

- `EpochCreationLog`: epoch, source-generation digest, target mint, source
  reference, raw account key, and first creation coordinate.
- `DiscoveredAccountList`: global raw keys, creation positions, and the verified
  mint anchor.
- `EpochAccountIdLog`: epoch, source-generation digest, local ID when present,
  raw key, role, and first creation position.

The files are `discoveries/epoch-N/creations.wincode`, `accounts.wincode`, and
`epochs/epoch-N/account-ids.wincode`. Their digests participate in manifests and
resume validation. Each epoch remaps applicable raw keys to that epoch's registry;
an ID from one epoch must not be reused as an ID in another. Consolidation checks
that logged ID/key pairs still agree with the pinned source registry before it
writes its mapping: [consolidate_v3.rs](../../indexer/blockzilla-token-transaction-dump/src/consolidate_v3.rs):1700–1711.

These fields describe the scope used by the SPYX code. They are not unconditional
proof of registry contents. `SourceIdentity.binding` can be an operator archive
ID or an internal candidate identity, as well as a stronger source binding:
[source.rs](../../crates/blockzilla-model/src/source.rs):59–74.
V2's `GenerationBinding.registry_sha256` is synthesized from descriptor identity
and object sizes in the local/object-set paths. Its field name does not make that
value a verified registry content hash:
[reader.rs](../../crates/compact-v2/blockzilla-compact-v2-reader/src/reader.rs):3019–3113.

## Current SDK and USDC limits

The canonical [RecordedTokenBalance](../../crates/blockzilla-model/src/model.rs):87
contains raw mint, owner, and token-program keys, plus `account_index`.
**`account_index` is a position in one transaction's message account list. It is
not an epoch registry ID.** Account discovery must first map that position through
the validated static and loaded account references.

The V2 SDK already compares a selected mint by bound ID before resolving output
keys. It then resolves owner and token-program keys for each selected balance:
[compact_query.rs](../../crates/compact-v2/blockzilla-compact-v2-reader/src/compact_query.rs):1364–1417.
A shared registry serves these lookups from memory (`compact_query.rs:2980`);
repeated resolution does not necessarily mean repeated disk I/O.

`BZUSDC02` writes 136-byte rows with raw keys and transaction coordinates. It has
no account-discovery dictionary and does not prove account creation. See
[usdc.rs](../../examples/workloads/src/usdc.rs):19–22, 234. A first observed balance
in a partial historical scan must be labelled first observation, not creation.

`Vec::new()` does not itself allocate. Current USDC uses reused token buffers and
pooled output vectors. Earlier allocation comparisons used a thread-exit counter
that can miss worker counts; those totals remain provisional. Repeated parsing
was the main identified USDC issue. The corrected counter now measures about
3,200 canonical or 1,600 indexed USDC calls for 3.27 million transactions with
twelve workers. See the [measured retest](epoch-300-indexed-usdc-and-allocation-retest-2026-09-06.md).
Candidate allocation reductions include Pump.fun CPI/loaded-address vectors and
message lists that exceed their inline capacity. Reusable bounded storage is a
better starting point than replacing every small vector.

## Safe additive design

Add an SDK projection that exposes source-bound compact references and borrowed
account/instruction views. Reuse the existing validated traversal and scratch
storage. Resolve a newly observed account once, retain the verified mapping, and
publish its mapping record before dependent output rows become durable. Record
epoch and generation identity, local ID or inline key, raw key, first-observed
position, and the discovery reason. Only an observed successful initialization
can use a creation label.

Scope each dictionary to the pinned source **and registry**, and record the
verification strength. For local sources, retain the registry's
`PinnedLocalObjectIdentity` with the source identity; it includes device, inode,
size, and modification/change times. It is file metadata, not a content hash:
[local source](../../crates/source/blockzilla-source-local/src/lib.rs):165–181.
For remote object sets, retain the exact object identity and its pinned strong
ETag/length validators; the object-set label alone is opaque:
[descriptor.rs](../../crates/compact-v2/blockzilla-compact-v2-reader/src/descriptor.rs):88–96.
Reject dictionary reuse when this scope changes, or re-verify all retained
ID/key pairs against the new registry. Do not use a synthetic digest, an epoch
number, or `BlockUniverseFingerprint` alone to join local IDs across sources.
That fingerprint covers block ordinals, slots, and transaction counts, not
registry rows or transaction contents:
[fingerprint.rs](../../crates/blockzilla-model/src/fingerprint.rs):5–10.

An optional USDC IDs-plus-mapping stream needs a new explicit output schema.
Expanding it through its mapping should reproduce the existing selected balance
rows and coverage. Keep `BZUSDC02` and its public API available. Reader-only
optimizations can continue without this format change.

Do not copy SPYX's validation shortcuts into the canonical SDK. No-hint discovery
can skip flagged failures before decoding (`extract.rs:4141`, `4216`), and it
reads metadata only when its account/instruction selection requires it (`4304`).
Its visitors use `LogPayloadValidation::StructureOnly` (`4418`, `5962`) because
raw-copy output preserves source bytes. Those paths do not establish the same
complete metadata/status validation as the canonical reader. Retain decoded
status checks, row-flag agreement, bounds, exact record consumption, and explicit
unknown coverage while transferring the ID and storage-reuse mechanisms.
