# Real Compact V2 conversion: epoch 2 merged layout

Date: 2026-08-14

This document records the current 19-object merged layout. It does not record
the earlier 26-object, 14-column layout. The earlier result remains in
[`epoch-2-real-conversion-2026-08-14.md`](epoch-2-real-conversion-2026-08-14.md)
as a historical measurement.

The current layout puts complete replay input in
`ledger/transactions.wincode`. Runtime effects stay in separate files. CPI
structure and CPI data share one file. Transaction outcomes and return data
share one file.

## Input and run profile

- Source: `<archive-root>/epoch-2`
- Target: `<trial-root>/epoch-2-merged-effects-20260814-r1`
- Epoch: 2
- Slots per epoch: 432,000
- Source profile: `archive-v2-may24-pre-unknown-fallbacks-v1`
- Source file bytes: 3,161,090,840
- Workers: 8
- Pipeline memory limit: 1,073,741,824 bytes
- Start: 14:51:53 CEST
- End: 15:02:36 CEST
- Wall time: 643 s
- CPU time: 35 min 24.651 s, or 2,124.651 s
- Average CPU use: 3.304 cores
- Maximum observed `MemoryCurrent`: 4,593,881,088 bytes

The memory value is the largest sampled value. It is not an exact peak value.
The report also states that process memory is not strictly bounded.

The source has no publication proof. The converter used fixture mode with the
historical message schema. The result is a physical candidate, not a
publishable archive.

## Size result

| Measure | Bytes or ratio |
| --- | ---: |
| Source files | 3,161,090,840 |
| 19 required objects | 3,127,819,399 |
| Report and candidate checksum | 4,500 |
| All target files | 3,127,823,899 |
| Target minus source | -33,266,941 |
| Size change | -1.052388% |
| Target/source | 98.947612% |

The 4,500 support bytes are `convert-report.json` at 3,131 bytes and
`canonical-candidate.sha256` at 1,369 bytes.

## Required object files

| Object | Bytes |
| --- | ---: |
| `catalog/blocks.wincode` | 62,206,336 |
| `dictionary/account_flags.pages` | 218 |
| `dictionary/blockhashes.pages` | 9,664 |
| `dictionary/pubkeys.pages` | 4,992 |
| `indexes/accounts.pages` | 51,748,139 |
| `indexes/programs.pages` | 24,573 |
| `indexes/selectors.pages` | 9,748,290 |
| `indexes/slots.idx` | 3,455,968 |
| `ledger/transactions.wincode` | 224,526,909 |
| `runtime/balances.wincode` | 64 |
| `runtime/block_rewards.wincode` | 864,040 |
| `runtime/inner_instructions.wincode` | 64 |
| `runtime/logs.wincode` | 64 |
| `runtime/outcomes.wincode` | 64 |
| `runtime/rewards.wincode` | 64 |
| `runtime/token_balances.wincode` | 64 |
| `sidecars/poh.wincode` | 1,068,339,424 |
| `sidecars/shredding.wincode` | 72,524,254 |
| `sidecars/signatures.bin` | 1,634,366,208 |
| **Total** | **3,127,819,399** |

## Converter report

| Validation field | Value |
| --- | --- |
| Archive ID | `f458c55128ce69ba124dbbbfd5a19cc2` |
| Source published | `false` |
| Source generation digest | `null` |
| Output status | `complete-physical-candidate-not-publishable` |
| Physical layout valid | `true` |
| Missing required objects | none |
| Required objects | 19 |
| Fixture previous blockhash | `null` |
| Fixture previous slot | `null` |

| Data count | Value |
| --- | ---: |
| Blocks | 431,988 |
| Transactions | 25,536,956 |
| Signatures | 25,536,971 |
| Top-level instructions | 25,536,965 |
| Inner instructions | 0 |
| Account references | 127,684,791 |
| PoH entries | 29,569,805 |
| Shredding boundaries | 29,569,805 |
| Block rewards stored | 431,988 |
| Raw fallback transactions | 0 |
| Loaded addresses unavailable | 0 |
| CPI not recorded | 0 |
| Raw account keys | 0 |
| Nonce blockhashes | 46 |

| Derived index field | Value |
| --- | ---: |
| Account postings | 127,684,791 |
| Account pages | 2,002 |
| Account continuation pages | 2,000 |
| Maximum postings per page | 65,536 |
| Peak page postings | 65,536 |
| Program postings | 25,536,965 |
| Selector postings | 25,536,965 |
| Derived-index workers | 4 |
| Sort memory | 1,073,741,824 bytes |
| Sort memory per builder | 357,913,941 bytes |

| Payload and dictionary field | Value |
| --- | ---: |
| Raw instruction-data variants | 25,536,956 |
| System instruction-data variants | 9 |
| Instruction bytes retained | 25,536,956 |
| Instruction bytes rederived | 9 |
| Retained payload bytes | 1,522,835,605 |
| Instruction-data bytes | 1,522,836,428 |
| Inner-instruction-data bytes | 0 |
| Token balances paired | 0 |
| Token balances total | 0 |
| Blockhash dictionary records | 300 |
| Pubkey dictionary records | 154 |
| Program accounts | 3 |
| Signer accounts | 69 |
| Unused accounts | 82 |
| Nonce hashes interned | 0 |

The PoH source schema was `archive-v2-current-wincode-0.5.5`. No block needed
PoH signature-count recovery, and no block had an unknown legacy signature
count. There were no recorded empty shredding blocks. The only raw-page count
was 431,988 pages for `runtime/block_rewards.wincode`.

The worker split was four block workers, one intra-block worker, and four page
workers, with at most eight spawned worker threads. The pipeline had at most
eight blocks and 544,001 bytes in flight.

## Read and layout checks

The physical validator found all 19 required objects. Both the normal read and
the full read passed at slot 872,069. These checks used the current merged
reader. They are not the old full 14-column read check.

This result gives 671.832 blocks/s, 39,715.328 transactions/s, and 4.916 decimal
MB/s of source data. These rates apply only to this host, source, and layout.
