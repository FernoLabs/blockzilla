# Token Instruction Dump Bench - 2026-05-18

Fixture:

- Input: `local-slices/epoch-644-usdc-mint-window/hot-zstd/archive-v2-blocks.zstd`
- Blocks: 1,939
- Transactions: 3,108,414
- Token instructions emitted in full mode: 1,557,104
- Compressed input size: 322,796,310 bytes
- Decoded block bytes: about 1.19 GiB

Changes tested:

- Replaced per-transaction materialized full key vectors with direct `HotKeyLookup` over static and loaded account slices.
- Replaced per-transaction instruction vector materialization with streaming outer/inner instruction scanning.
- Added `--no-output` for parser-only benchmarks.
- Added `--outer-only` for lower-bound benchmarks that skip metadata and inner instructions.
- Added metadata prefix decoding for txs that need inner instructions but do not need loaded addresses. This avoids parsing logs/balances/rewards for the common non-loaded-address case.
- Changed `dump-token-instructions` default hot-block `--chunk-size` from 64 to 512.

Local baseline before these changes:

| Mode | Workers | Time | Tx/s |
| --- | ---: | ---: | ---: |
| full output | 1 | 18.31s | 169,766 |
| full output | 4 | 7.71s | 403,166 |
| full output | 8 | 6.99s | 444,694 |

Best current full-output run after changes:

| Chunk | Workers | Time | Tx/s | Token ix/s | Read MiB/s | Decoded MiB/s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 512 | 1 | 5.88s | 528,389 | 264,687 | 52.33 | 202.19 |
| 512 | 2 | 3.76s | 826,383 | 413,962 | 81.84 | 316.22 |
| 512 | 4 | 2.39s | 1,302,297 | 652,362 | 128.97 | 498.34 |
| 512 | 8 | 2.56s | 1,213,391 | 607,826 | 120.17 | 464.32 |

Parser-only current run (`--no-output`, chunk 512):

| Workers | Time | Tx/s | Token ix/s |
| ---: | ---: | ---: | ---: |
| 1 | 6.70s | 464,123 | 232,494 |
| 2 | 3.35s | 928,288 | 465,009 |
| 4 | 2.16s | 1,436,665 | 719,671 |
| 8 | 2.46s | 1,262,145 | 632,249 |

Outer-only lower bound (`--outer-only --no-output`, before prefix optimization, chunk 128):

| Workers | Time | Tx/s | Token ixs |
| ---: | ---: | ---: | ---: |
| 1 | 6.56s | 474,176 | 406,271 |
| 4 | 2.14s | 1,451,791 | 406,271 |
| 8 | 1.63s | 1,904,802 | 406,271 |

Notes:

- The main remaining cost is still metadata and message deserialization. The fixture has inner-instruction metadata on every tx (`metadata_inner_needed=3,108,414`) but loaded addresses on only 232,435 txs.
- 4 workers beat 8 workers on this local fixture after the prefix optimization, probably because the input is small enough that scheduling and output merging dominate beyond 4 workers.
- On larger NAS epochs, 8 workers may still win if disk and decompression have enough work. Use both 4 and 8 in the next real-epoch benchmark.
