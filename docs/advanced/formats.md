# File formats

An archive is a set of files. Indexes locate the data. Dictionaries store repeated keys once. The reader joins these files to return a useful block or transaction view.

Use the Rust reader for application work. Use this section when you need to inspect files, build tools, or understand the storage layout.

## Format families

| Format | Storage model | Current use |
| --- | --- | --- |
| Compact V2 hot blocks | One compressed frame per block, with shared registries | Current ordered callback and HTTPS path |
| Canonical Archive V3 | Block catalog, transaction stream, separate runtime effects | Replacement format; local reader and converter candidates |
| Frozen Indexer V3 prototype | Standalone payload and separate semantic planes | Retained examples; not canonical V3 |

The name “V3” alone is not enough to choose a decoder. Inspect the physical file paths.

## Compact V2 in one view

```text
archive-v2-blocks.index
  ├─ byte range → archive-v2-blocks.zstd → hot block
  ├─ transaction ordinals
  └─ signature ordinals → signatures.bin

hot block
  ├─ header
  ├─ fixed transaction rows
  ├─ compact message bytes → registry.bin, blockhash registries
  └─ metadata bytes
```

Each index row identifies one block frame. A transaction row selects its message and metadata within that decoded block. Public-key IDs resolve through the registry for that source.

The [Compact V2 format reference](../reference/archive-v2-hot-block-format.md) contains exact file names, index offsets, flag bits, record layouts, and compatibility rules.

## Canonical V3 in one view

```text
catalog/blocks.wincode
  ├─ transactions → ledger/transactions.wincode
  │                  ├─ signed message data
  │                  ├─ effect states
  │                  └─ effect locators → runtime/*.wincode
  ├─ block rewards → runtime/block_rewards.wincode
  ├─ PoH → sidecars/poh.wincode
  └─ shredding → sidecars/shredding.wincode

dictionaries → canonical keys and non-PoH hashes
indexes      → derived lookup data
```

V3 keeps each canonical fact in one place. A transaction read can select the runtime effect files it needs. Rebuildable indexes contain lookup data, not another copy of ledger facts.

Read [the V3 format guide](archive-v3.md) for headers, catalog rows, effect states, and the local read sequence.

## Keep IDs with their source

A numeric public-key ID is meaningful only with its archive dictionary. Never compare IDs from different epochs or builds as if they were public keys.

When you save indexed output, keep the source identity and the dictionary binding. Compare resolved 32-byte keys when you combine sources.

## Encoding and compatibility

The formats use fixed-width little-endian index fields and Wincode payload schemas. Current structured V3 payloads use canonical unsigned LEB128, zigzag signed values, and one-byte tags. Retained PoH and shredding frames have an explicit source wire profile.

Do not choose a historical schema by trial decoding each transaction. Schema choice belongs to archive admission or an explicit conversion profile.

The project is pre-1.0. Record the producing and reading Git revisions with your data. Use the schema definitions and golden byte fixtures in the format crates as the exact authority.

## Completeness is a separate check

A valid file header or successful sample decode does not prove complete epoch coverage, finality, or publication readiness. Keep produced blocks, skipped slots, missing intervals, and source provenance separate.

The [data and coverage guide](coverage.md) explains how to handle missing facts and verification levels.
