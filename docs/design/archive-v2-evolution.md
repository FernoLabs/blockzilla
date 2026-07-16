# Archive V2 evolution ideas

Status: **non-normative design backlog**.

The implemented layout is documented in the
[Archive V2 hot-block reference](../reference/archive-v2-hot-block-format.md).
Nothing in this note is part of that contract until code, fixtures, and version
changes land together.

## Design rules

- Preserve one independently addressable block as the main read unit.
- Keep optional indexes external so they can be rebuilt without rewriting hot
  block blobs.
- Require deterministic output and validate every optional index against the
  hot-index row count and registry generation.
- Prefer additive sidecars before changing the block schema.
- Bump the relevant version when a reader cannot distinguish old and new bytes
  unambiguously.

## Block account filter

A block-level account sidecar could let indexers skip blocks that cannot touch
their tracked keys:

```text
archive-v2-block-accounts.index
  row[block_id] = { offset: u64, byte_len: u32 }

archive-v2-block-accounts.delta
  block = varint(unique_count) + delta_varint(sorted_registry_ids)
```

Two semantics should stay explicit:

- **message accounts**: static message keys plus loaded addresses;
- **semantic accounts**: message accounts plus referenced mint, owner, program,
  reward, log, and other metadata keys.

The first is cheaper to build; the second can answer more filters. A sidecar
must declare which semantics it uses. Registry IDs are preferable to raw
pubkeys when the sidecar is bound to one exact registry generation.

Before adoption, benchmark total index size, build throughput, false-positive
rate for realistic key sets, and intersection performance. The implementation
must prove that no relevant account reference is omitted for its declared
semantics.

## Signature lookup index

`signatures.bin` already supports block reconstruction through ordinal ranges.
It does not provide signature-to-transaction lookup.

A future lookup index could map a signature to:

```text
{ block_id: u32, tx_ordinal: u32, signature_index: u8 }
```

The existing filter/minimal-perfect-hash approach used by Old Faithful tooling
is a candidate, but its values must refer to Archive V2 block-local positions,
not CAR offsets. Collision handling and negative lookups require correctness
fixtures before this becomes a public format.

## Metadata subranges

The current transaction row makes the whole metadata payload skippable. A
future schema could add subranges for logs, inner instructions, balances,
loaded addresses, and return data so readers can decode only the requested
section.

Possible improvements include:

- block-level string and binary-data tables for logs;
- offset tables for repeated metadata sections;
- compact typed transaction errors;
- explicit per-section presence and raw-fallback flags.

These changes affect the hot payload and therefore require a versioned schema,
not an undocumented reinterpretation of current offsets.

## Publication manifest

The current file formats validate their own indexes and payloads, but file
presence alone is not a complete publication protocol. A future Blockzilla
generation manifest should bind:

- cluster/genesis and epoch identity;
- Archive V2 and index versions;
- the exact required and optional object set;
- byte length and cryptographic digest for every object;
- completeness and repair policy;
- the immutable generation identity.

Blockzilla should upload data first and the destination manifest last. R2 and
B2 need separate verified publication receipts even when they contain the same
logical generation.

## Open questions

- Which optional indexes provide enough reader benefit to justify permanent
  storage?
- Should metadata evolve through new subrange tables or a separate cold
  sidecar?
- Which completion facts belong in the archive footer versus the publication
  manifest?
- How should a reader advertise supported hot-block and optional-index
  versions before remote range reads begin?
