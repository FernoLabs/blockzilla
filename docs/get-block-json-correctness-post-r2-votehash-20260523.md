# getBlock JSON correctness after vote-hash R2 sync

Run root: `target/rpc-bench/post-r2-votehash-json-3providers-20260523T195407Z`

Scope:
- Endpoints: Blockzilla get-block worker, Helius primary, Triton.
- Epochs: 10, 100, 200, 300, 400, 500, 600, 700, 800, 900, 920.
- Slots: 100 random per epoch plus epoch boundary slots, 1,122 slots total.
- Methods: `getBlock` with JSON encoding, transactionDetails `full`, `accounts`, `signatures`, `none`; rewards false and selected rewards true cases.
- JSON comparison is strict. Arrays are compared by index, so ordering differences are reported as JSON diffs.

## Summary

| Case | Calls | Endpoint errors | Compared | Matches | JSON diffs | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `full,json,rewards=false` | 3,366 | 101 | 2,244 | 1,160 | 112,966 | Blockzilla epoch 920 vote-hash errors; large Triton/provider diffs |
| `accounts,json,rewards=false` | 3,366 | 0 | 2,244 | 1,907 | 101,308 | Blockzilla mostly clean; Triton token-balance/provider diffs dominate |
| `signatures,json,rewards=false` | 3,366 | 0 | 2,244 | 2,233 | 12 | Nearly clean |
| `none,json,rewards=false` | 3,366 | 0 | 2,244 | 2,233 | 12 | Nearly clean |
| `full,json,rewards=true` | 3,366 | 107 | 2,240 | 1,144 | 135,064 | Same epoch 920 issue plus reward/provider oddities |
| `none,json,rewards=true` | 3,366 | 5 | 2,242 | 2,231 | 1,037 | Mostly epoch 920 rewards/header/provider differences |

## Endpoint Shape

| Case | Endpoint | OK | Errors | Compared | Matches | Diffs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `full,false` | Blockzilla | 1,021 | 101 | 1,122 | 363 | 8,915 |
| `full,false` | Triton | 1,122 | 0 | 1,122 | 797 | 104,051 |
| `full,true` | Blockzilla | 1,021 | 101 | 1,120 | 361 | 9,428 |
| `full,true` | Helius | 1,120 | 2 | primary | primary | primary |
| `full,true` | Triton | 1,118 | 4 | 1,120 | 783 | 125,636 |
| `accounts,false` | Blockzilla | 1,122 | 0 | 1,122 | 1,109 | 52 |
| `accounts,false` | Triton | 1,122 | 0 | 1,122 | 798 | 101,256 |
| `signatures,false` | Blockzilla | 1,122 | 0 | 1,122 | 1,121 | 2 |
| `signatures,false` | Triton | 1,122 | 0 | 1,122 | 1,112 | 10 |
| `none,false` | Blockzilla | 1,122 | 0 | 1,122 | 1,121 | 2 |
| `none,false` | Triton | 1,122 | 0 | 1,122 | 1,112 | 10 |
| `none,true` | Blockzilla | 1,122 | 0 | 1,121 | 1,120 | 514 |
| `none,true` | Triton | 1,118 | 4 | 1,121 | 1,111 | 523 |

## Error Body Size Check

The previous empty-body regression did not reproduce for Blockzilla. Blockzilla errors are JSON-RPC error bodies:

| Case | Endpoint | Error count | Epochs | Bytes min/avg/max | Message class |
| --- | --- | ---: | --- | --- | --- |
| `full,false` | Blockzilla | 101 | 920 only | 166 / 167 / 168 | `-32000`, missing vote hash block id; fallback disabled |
| `full,true` | Blockzilla | 101 | 920 only | 166 / 167 / 168 | `-32000`, missing vote hash block id; fallback disabled |
| `full,true` | Helius | 2 | 600, 700 | 0 / 0 / 0 | body decode failure in harness/provider response |
| `full,true` | Triton | 4 | 400, 500, 600, 700 | 99 / 99 / 99 | `-32004`, block not available |
| `none,true` | Helius | 1 | 700 | 0 / 0 / 0 | body decode failure in harness/provider response |
| `none,true` | Triton | 4 | 400, 500, 600, 700 | 99 / 99 / 99 | `-32004`, block not available |

Blockzilla failing slots are all epoch 920 random samples, for example `397447655`, `397453538`, `397457649`, `397461330`, `397466729`.

## Main Diff Buckets

Blockzilla-specific:
- Epoch 920 still needs the vote-hash access rebuild/sync path. Full render returns `failed to stream getBlock response: block access is missing vote hash block id ...; fallback is disabled`.
- `full,false/full,true` have older inner-instruction/log-message differences, mostly early epochs such as 100 and 400.
- `none/signatures` are otherwise almost clean; epoch 920 boundary has `previousBlockhash`/`numRewardPartitions` differences.

Provider-specific:
- `accounts,false` has 50 Helius-only `BorshIoError` representation diffs across 25 transactions. Blockzilla and Triton emit `"BorshIoError"`; public mainnet-beta confirmed `"BorshIoError"` for all 25 affected transactions on 2026-05-24. Keep Blockzilla behavior.
- Triton dominates token-balance differences in `accounts/full`, especially epochs 10, 100, and 920.
- Triton reports `Block not available` for reward-heavy boundary slots `172800020`, `216000000`, `259200004`, `302400000`.
- Helius body decode failures are limited to large reward boundary slots in this run.

## Next Fixes

1. Rebuild and sync epoch 920 access/get-block sidecars with embedded vote hashes.
2. Treat Helius' `{"BorshIoError":"Unknown"}` shape as a provider semantic difference unless another canonical source disagrees with mainnet-beta.
3. Re-run the same matrix after epoch 920 is rebuilt, then use public mainnet only as a rate-limited tie-breaker for Helius/Triton disagreements.
