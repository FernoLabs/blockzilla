# Compact V2 to Index Archive converter

This crate converts one immutable Compact V2 generation to the new
indexer-first archive layout.

The converter is fail-closed. It writes a conversion candidate only when it
can reconstruct and verify the exact signed transaction message. It does not
replace missing facts with empty values.

## Current source scope

The current binary supports the Compact V2 current-hot source profile:

- the current hot-block outer schema;
- the current or manifest-bound May 2024 message schema;
- the boundary-prefixed blockhash registry, where record 0 is the predecessor;
- legacy unprefixed registries with manifest-bound 40-byte predecessor-tail rows;
- a complete external shredding sidecar;
- complete signature, PoH, blockhash, pubkey, and required registry files.

The binary stops when it finds one of these source forms:

- a raw transaction or raw metadata fallback;
- a legacy hot-block outer schema;
- embedded legacy shredding without an exact external sidecar;
- an old previous-blockhash tail without a trusted schema marker;
- V0 loaded-address counts that do not match the signed lookup descriptors;
- instruction bytes that the stored signature cannot prove.

These stops are intentional. Support for each old source profile needs an
explicit, manifest-bound decoder. The converter must not guess a wire schema.

## Convert canonical data

```text
cargo run --release -p blockzilla-index-archive-convert -- \
  <compact-v2-generation> <candidate-directory> \
  --workers <N> --pipeline-memory-limit-mib <MiB>
```

`--workers` runs block reads, decompression, exact message reconstruction, and
independent page compression on multiple CPU cores. Output order comes only
from the Compact V2 block index. The same input and policy must produce the
same bytes for all worker counts.

An unpublished test source has no manifest that can select its message wire
schema. It must name the frozen schema explicitly:

```text
cargo run --release -p blockzilla-index-archive-convert -- \
  <compact-v2-generation> <candidate-directory> \
  --fixture-source --epoch <N> --slots-per-epoch <N> \
  --fixture-message-schema current|may24-pre-unknown-fallbacks
```

The converter does not probe messages to choose this value. Raw instructions
have the same tag in both schemas, so an incorrect schema can appear to work
for many blocks before the first structured instruction.

The memory option is a pipeline budget. It is not yet a strict process RSS
limit because dictionary lookup tables, task descriptors, and fixed writer
buffers are outside that budget. PoH and shredding are not loaded as complete
files: the converter validates and retains one bounded frame at a time. The
conversion report states this limit.

## Derived indexes

The canonical bytes own each fact once. The four rebuildable index-builder
outputs contain keys, ordinals, role bits, and page locations only. The
separate `dictionary/account_flags.pages` object is also derived during the
canonical scan; it is not a fifth index-builder job.

The main conversion command builds all four index outputs after it closes the
canonical files. It uses the same worker and pipeline-memory settings for the
bounded external-sort jobs. The commands below are maintenance tools for an
explicit rebuild or a focused test:

```text
cargo run --release -p blockzilla-index-archive-convert \
  --bin ia-build-basic-indexes -- <candidate-directory>

cargo run --release -p blockzilla-index-archive-convert \
  --bin ia-index -- <candidate-directory>

cargo run --release -p blockzilla-index-archive-convert \
  --bin ia-program-index -- <candidate-directory>

cargo run --release -p blockzilla-index-archive-convert \
  --bin ia-selector-index -- <candidate-directory>
```

The account, program, and selector builders use bounded external sort runs.
The slot index streams directly from the fixed block catalog. The library
`build_all_derived_indexes` can run the four independent builder jobs in
parallel while it divides one total sort-memory budget between them. Account
index schema 2 splits a hot account into deterministic continuation pages of
at most 65,536 postings. Lookup streams those pages and does not collect the
complete list unless a caller explicitly requests that convenience API.

## Read checks

Use the selective reader to check one block without reading runtime effects:

```text
cargo run --release -p blockzilla-index-archive-convert \
  --bin ia-read -- <candidate-directory> <slot>
```

Add `--full` to decode all canonical runtime columns for that block.

## Candidate status

The converter writes `complete-physical-candidate-not-publishable` in its
report. Before it renames the staging directory, it requires every layout
object required for that epoch, validates all object headers against one
archive ID, and syncs each archive subdirectory.
`canonical-candidate.sha256` binds the canonical files only. Derived indexes
can be rebuilt without changing the canonical archive ID.

Do not activate or delete the Compact V2 source from this output yet. Final
publication still needs a typed target manifest, full cross-plane semantic
verification, chain and finality evidence, and an atomic catalog cutover
receipt.

PoH hashes exist only in the retained Wincode frames in
`sidecars/poh.wincode`. A PoH hash reference names the catalog block whose
final PoH entry owns that hash; it does not copy the hash bytes. Signature
bytes exist only in `sidecars/signatures.bin`. There is no generation-local
signature index.
