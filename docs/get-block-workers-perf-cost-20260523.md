# getBlock worker perf and Cloudflare cost - 2026-05-23

Run artifact: `target/rpc-bench/workers-vs-rpc-surface-20260523T180116Z`

Scope:
- Epochs: `10,100,200,300,400,500,600,700,800,900,920`
- Slots: 10 random present slots per epoch plus first/last present boundaries, 132 slots per case
- Endpoints: Blockzilla V2 worker, old Old-Faithful worker, Helius, Triton
- Cases: `full/accounts/signatures/none` JSON without rewards, plus `full/none` JSON with rewards
- Primary correctness reference: Helius

## Perf

Values are successful calls only. Latency is client-observed wall time.

| Case | Endpoint | OK/error | Avg ms | P50 ms | P95 ms | Avg body |
|---|---:|---:|---:|---:|---:|---:|
| `none,false` | Blockzilla | 132/0 | 207 | 160 | 431 | 0.2 KiB |
| `none,false` | Old OF | 132/0 | 376 | 291 | 752 | 0.2 KiB |
| `none,false` | Helius | 132/0 | 86 | 63 | 250 | 0.2 KiB |
| `none,false` | Triton | 132/0 | 72 | 55 | 218 | 0.2 KiB |
| `signatures,false` | Blockzilla | 132/0 | 261 | 205 | 521 | 104 KiB |
| `signatures,false` | Old OF | 132/0 | 621 | 508 | 1552 | 104 KiB |
| `signatures,false` | Helius | 132/0 | 149 | 114 | 373 | 104 KiB |
| `signatures,false` | Triton | 132/0 | 135 | 117 | 299 | 104 KiB |
| `accounts,false` | Blockzilla | 132/0 | 1360 | 1229 | 3127 | 1.92 MiB |
| `accounts,false` | Old OF | 132/0 | 1657 | 1479 | 3621 | 1.93 MiB |
| `accounts,false` | Helius | 132/0 | 1336 | 1243 | 2994 | 1.92 MiB |
| `accounts,false` | Triton | 132/0 | 1634 | 1423 | 3827 | 1.92 MiB |
| `full,false` | Blockzilla | 77/55 | 798 | 467 | 2185 | 1.60 MiB |
| `full,false` | Old OF | 132/0 | 1343 | 1217 | 2571 | 2.71 MiB |
| `full,false` | Helius | 132/0 | 850 | 700 | 2331 | 2.71 MiB |
| `full,false` | Triton | 132/0 | 1334 | 1078 | 3877 | 2.71 MiB |
| `none,true` | Blockzilla | 132/0 | 2872 | 225 | 13239 | 3.99 MiB |
| `none,true` | Old OF | 27/105 | 825 | 725 | 1591 | 7.8 KiB |
| `none,true` | Helius | 131/1 | 2016 | 166 | 10603 | 2.86 MiB |
| `none,true` | Triton | 128/4 | 202 | 96 | 597 | 108 KiB |
| `full,true` | Blockzilla | 77/55 | 3512 | 927 | 14543 | 8.44 MiB |
| `full,true` | Old OF | 37/95 | 500 | 387 | 1348 | 555 KiB |
| `full,true` | Helius | 131/1 | 3367 | 1897 | 8690 | 5.54 MiB |
| `full,true` | Triton | 128/4 | 2009 | 1912 | 4738 | 2.90 MiB |

## Findings

- `Avg body` is averaged over successful calls only, so `full,true` is not an apples-to-apples JSON-size comparison across endpoints. The success sets differ: Blockzilla succeeded on 77 slots, Helius on 131, Triton on 128, Old OF on 37.
- On the 76 common successful `full,true` slots for Blockzilla and Helius, average body size is effectively identical: Blockzilla 6.462 MiB vs Helius 6.461 MiB. On the 37 slots common to all four endpoints: Blockzilla 0.539 MiB, Old OF 0.542 MiB, Helius 0.535 MiB, Triton 0.535 MiB.
- The headline `full,true` average is skewed by huge reward boundary slots and provider failures. Blockzilla included slot `302400000` at 159.04 MiB; Helius failed to read that body in this run. Triton returned `-32004 Block not available` for `172800020`, `216000000`, `259200004`, and `302400000`, removing several huge reward blocks from its average.
- The JSON is not byte-identical everywhere. The main Blockzilla-larger cases in common successful slots are older inner-instruction/log surfaces where Blockzilla has archived values and Helius returns `null`; other known differences include token balance owner/programId fields, reward ordering/availability, and provider availability. Use common-slot averages and correctness diffs before drawing size/perf conclusions.
- Blockzilla V2 is faster than Old OF on the healthy small/medium cases, especially `none` and `signatures`.
- `full` on Blockzilla V2 still exposes missing vote-hash sidecar data on later epochs, but the transport failure is patched in deploy `53a3644d-7a55-4dcc-9c50-eacad6e75dc0`: repro slot `216004682`, `transactionDetails=full`, `rewards=true/false` now returns a structured JSON-RPC `-32000` error instead of HTTP 200 with an empty body.
- Old OF rewards continuation delivery is fixed for normal rewards: slot `388800000`, `transactionDetails=none`, `rewards=true` returns 200 with 809 rewards.
- Old OF still cannot serve huge reward slots in Worker runtime: slots like `216000000` and `259200004` hit Cloudflare 1102 CPU limit.
- Triton missed some reward boundary blocks with `-32004 Block not available`; Helius had one body decode failure in reward-heavy cases.
- Existing known semantic diffs remain: older inner instructions/logs, token balance owner/programId, previousBlockhash/provider formatting, reward ordering/availability.

## Cloudflare Cost Estimate

Pricing used:
- Workers Standard: $5/mo base, 10M requests included, then $0.30/M requests; 30M CPU-ms included, then $0.02/M CPU-ms.
- R2 Standard: $0.015/GB-month; Class B reads $0.36/M; egress free.

Per-request read shape:
- Blockzilla V2 profile on slot `397462159`: 3 R2 Class B range reads: get-block index, access sidecar, block blob.
- Old OF current deploy: 1 R2 Class B read for slot index, archive bytes from `files.old-faithful.net` HTTP. If hosting Old OF CARs in R2, add archive storage and archive Class B reads.

Variable cost per 1M successful requests, using measured wall time as a conservative CPU-ms upper bound:

| Method | Case | Request | CPU upper | R2 reads | Total upper |
|---|---|---:|---:|---:|---:|
| Blockzilla | `none,false` | $0.30 | $4.13 | $1.08 | $5.51 |
| Old OF | `none,false` | $0.30 | $7.51 | $0.36 | $8.17 |
| Blockzilla | `signatures,false` | $0.30 | $5.21 | $1.08 | $6.59 |
| Old OF | `signatures,false` | $0.30 | $12.41 | $0.36 | $13.07 |
| Blockzilla | `accounts,false` | $0.30 | $27.20 | $1.08 | $28.58 |
| Old OF | `accounts,false` | $0.30 | $33.14 | $0.36 | $33.80 |
| Old OF | `full,false` | $0.30 | $26.87 | $0.36 | $27.53 |
| Blockzilla | `none,true` | $0.30 | $57.43 | $1.08 | $58.81 |

Storage:
- Blockzilla Archive V2 hot-zstd projected storage: roughly 45-48 TB, about $674-$713/mo on R2 Standard.
- Raw Old Faithful CAR + OF indexes: about 488 TB, about $7.33k/mo on R2 Standard.

Storage amortized per 1M requests:

| Monthly requests | Blockzilla storage / 1M | Raw OF+index storage / 1M |
|---:|---:|---:|
| 1M | ~$700 | ~$7,330 |
| 10M | ~$70 | ~$733 |
| 100M | ~$7 | ~$73 |
| 1B | ~$0.70 | ~$7.33 |

Notes:
- The CPU rows are deliberately worst-case because Cloudflare bills CPU time, not wall time; R2/network wait should not count as CPU. Real CPU cost should be lower than the upper-bound table.
- The fixed storage cost dominates at low volume. At high volume, per-request CPU and R2 read operations dominate.

Sources:
- Cloudflare Workers pricing: https://developers.cloudflare.com/workers/platform/pricing/
- Cloudflare R2 pricing: https://developers.cloudflare.com/r2/pricing/
- Storage projection basis: `docs/blockzilla-storage-scan-worker-cost-20260522.md`
