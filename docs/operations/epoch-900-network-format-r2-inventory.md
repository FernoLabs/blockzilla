# Epoch 900 network benchmark R2 inventory

This document defines the one fixed R2 object inventory for corrected epoch
900. The release ID is `e900-current-typed-errors-v1`.

The inventory is manifest-free. It uses fixed file names and exact byte sizes.
It does not contain hashes, schema markers, or a completion object.

## Fixed totals

| Set | Objects | Bytes |
| --- | ---: | ---: |
| Compact V2 serving prefix | 12 | 104,077,157,709 |
| Indexer V3 serving prefix | 25 | 106,322,193,125 |
| Both serving prefixes | 37 | 210,399,350,834 |
| Local files uploaded to private staging | 28 | 166,272,129,727 |
| Server-side copies in private staging | 9 | 44,127,221,107 |

The inventory has 35 `payload` rows, two `control` rows, and zero
`completion` rows. All payload rows come before both control rows.

## Compact V2 objects

All 12 Compact objects are local payload rows.

| Object | Bytes |
| --- | ---: |
| `archive-v2-blocks.index` | 22,456,652 |
| `archive-v2-blocks.zstd` | 59,899,113,036 |
| `archive-v2-meta.wincode` | 66 |
| `blockhash_registry.bin` | 13,819,456 |
| `poh.wincode` | 9,681,441,209 |
| `prev_blockhash_tail.bin` | 12,000 |
| `registry.bin` | 889,551,808 |
| `registry.mphf` | 341,082,690 |
| `registry_counts.bin` | 28,366,914 |
| `shredding.wincode` | 792,857,572 |
| `signatures.bin` | 32,380,385,536 |
| `vote_hash_registry.bin` | 28,070,770 |

The target prefix is:

```text
compact-v2/releases/e900-current-typed-errors-v1
```

## Local Indexer V3 objects

The V3 source supplies these 16 local files. The two small metadata files use
the `control` role. The other 14 files use the `payload` role.

| Role | Object | Bytes |
| --- | --- | ---: |
| control | `archive-v2-retained-sidecars.candidate.json` | 1,527 |
| control | `archive-v2-standalone-account-postings-adaptive-v3.control` | 120 |
| payload | `archive-v2-standalone-account-postings-adaptive-v3.coverage` | 46,512 |
| payload | `archive-v2-standalone-account-postings-adaptive-v3.pages` | 4,688,130,905 |
| payload | `archive-v2-standalone-balances.wincode` | 10,248,823,687 |
| payload | `archive-v2-standalone-block-rewards.wincode` | 31,853,726 |
| payload | `archive-v2-standalone-blocks.index` | 107,100,848 |
| payload | `archive-v2-standalone-inner-instructions.wincode` | 12,373,908,023 |
| payload | `archive-v2-standalone-loaded-addresses.wincode` | 990,259,848 |
| payload | `archive-v2-standalone-logs.wincode` | 13,733,124,021 |
| payload | `archive-v2-standalone-messages.wincode` | 13,201,297,110 |
| payload | `archive-v2-standalone-outcomes.wincode` | 1,128,420,825 |
| payload | `archive-v2-standalone-raw-metadata-fallbacks.wincode` | 64 |
| payload | `archive-v2-standalone-token-balances.wincode` | 4,350,091,593 |
| payload | `archive-v2-standalone-transaction-directory.wincode` | 1,341,913,145 |
| payload | `archive-v2-standalone-transaction-rewards.wincode` | 64 |

These local V3 files total 62,194,972,018 bytes.

## V3 server-side copies

The publisher copies these nine Compact staging keys to same-name V3 staging
keys. It does not upload the same NAS bytes a second time.

| Object | Bytes |
| --- | ---: |
| `archive-v2-meta.wincode` | 66 |
| `blockhash_registry.bin` | 13,819,456 |
| `poh.wincode` | 9,681,441,209 |
| `prev_blockhash_tail.bin` | 12,000 |
| `registry.bin` | 889,551,808 |
| `registry.mphf` | 341,082,690 |
| `shredding.wincode` | 792,857,572 |
| `signatures.bin` | 32,380,385,536 |
| `vote_hash_registry.bin` | 28,070,770 |

The V3 target prefix is:

```text
indexer-v3/releases/e900-current-typed-errors-v1
```

## Build the TSV

The builder reads directory entries and file sizes only. It does not read
payload bodies, calculate hashes, contact R2, or change a source file.

```bash
scripts/build-epoch-900-network-format-r2-inventory.sh \
  --compact-dir /volume1/blockzilla/archive-metadata-normalization/staging/epoch-900-current-typed-errors-v1-20260828T124710CEST \
  --v3-dir /volume1/blockzilla/index-archive-trial/foundation-optimized-split-v3-current-r1/epoch-900-full-2g-r2 \
  --output /absolute/new/path/epoch-900-r2-inventory.tsv
```

The output is a read-only, no-clobber TSV. The builder stops on a missing
object, a wrong size, a symbolic link, an unknown top-level item, a noncanonical
path, or an existing output.

Named private evidence can stay in the source directories. The builder does
not add it to the TSV. This includes the two Compact metadata-normalization
records, the V3 benchmark reports, and `evidence` or `reports` directories.

The builder rejects old archive manifests, schema-marker files, hash sidecars,
and all other unknown files. In particular, it never publishes:

- `archive-v2-generation.json`;
- `benchmark-manifest.json`;
- a `*.sha256` file;
- a `*.marker` file.

Run the local fixture test with:

```bash
scripts/test-build-epoch-900-network-format-r2-inventory.sh
```

The test uses sparse local files. It does not contact the NAS or R2.
