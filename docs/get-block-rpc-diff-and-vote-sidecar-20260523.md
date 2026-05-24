# getBlock RPC Diff And Vote Sidecar Check - 2026-05-23

Scope: `getBlock`, `encoding=json`, `transactionDetails=full`, `rewards=false`, epochs 10, 100, 200, 300, 400, 500, 600, 700, 800, 900. Providers compared: Blockzilla Worker, Triton, Helius, and public `api.mainnet-beta.solana.com` as tie-breaker where Helius and Triton disagreed.

## Provider Diffs

| Area | Count / sample | Providers | Mainnet verdict | Action |
|---|---:|---|---|---|
| Epoch-boundary `previousBlockhash` | 10 boundary slots | Triton returns `11111111111111111111111111111111`; Blockzilla and Helius return the real previous blockhash | Mainnet agrees with Blockzilla/Helius on all 10 | Keep Blockzilla behavior |
| Old `postTokenBalances` epoch 10 | 101 Triton diffs; sampled 3 txs | Blockzilla/Helius `[]`; Triton `null` | Mainnet agrees with Triton: `null` | Patch old-epoch token balance nullability for epoch 10 |
| Old `postTokenBalances` epoch 100 | 100 Triton diffs; sampled 3 txs | Blockzilla/Helius `[]`; Triton `null` | Mainnet agrees with Blockzilla/Helius: `[]` | Keep Blockzilla behavior for epoch 100 |
| Old `innerInstructions` epoch 100 | 101 Helius diffs; sampled 8 txs | Blockzilla/Triton populated arrays; Helius `null` | Mainnet agrees with Blockzilla/Triton | Keep Blockzilla behavior |
| `BorshIoError` rendering | 1 tx | Blockzilla had `{"BorshIoError":"Unknown"}`; Triton had `"BorshIoError"` | Mainnet full `getBlock` agrees with Triton | Fixed in Worker |
| Log text | 247 Helius/Triton concordant diffs | Helius, Triton, and sampled mainnet agree against Blockzilla | Mainnet confirms sampled cases | Still needs log-rendering fixes |
| Invalid/partial JSON on epochs 500-800 | 404 Helius/Triton concordant failures | Helius/Triton return valid blocks; old Blockzilla returned empty or truncated body | Mainnet confirms sampled blocks exist | Worker now returns structured JSON-RPC errors; archive sidecar still needs repair |
| Huge epoch reward blocks | slots `216000000`, `259200004`, `302400000` with `transactionDetails=none,rewards=true` | Blockzilla and Helius return matching 85.2 MB, 160.3 MB, and 166.8 MB JSON-RPC bodies; public mainnet also returns the blocks with tiny formatting/order differences; Triton returns `-32004 Block not available` for all 3 | Mainnet confirms blocks exist and have huge reward arrays | Treat Triton as provider availability oddity; do not patch Blockzilla to match |

## Transaction Samples

| Bucket | Slot | Tx index | Signature | Blockzilla | Reference / mainnet |
|---|---:|---:|---|---|---|
| epoch 10 `postTokenBalances` | 4329654 | 0 | `EyQ9L6q9f9rgPNQidngcSxpfVtHiG9kzgUviZYCd3H3kzLCcvcsxSVQ4qsDNvCmFgsxbdoWUsvTG3mAyV91w5Te` | `[]` | `null` |
| epoch 10 `postTokenBalances` | 4335481 | 0 | `4kDUgNvipxFQL3CqnrPHoiWbkgxZhhtrEKNiWoratgGBoDWHyABp2RbmYVeuSNZhpSf9BZ48xtsrkE8QXJ2r6vq4` | `[]` | `null` |
| epoch 100 `postTokenBalances` | 43203903 | 0 | `531cEiy3RaU9Vo7Z7JzeK1hVcMLiLPFpZtgPW5LueMY4uPS57sgxQU7yTX836zKh2jmUxYrbXZe71kYrkGBvZnuZ` | `[]` | `[]` |
| epoch 100 `innerInstructions` | 43203903 | 3 | `prug4MEZ9AsGFXzQxWEMdotxVgCL1tAizFZAWsimwdKj4MD51tq4qKxYfTWSZh9reJ3qZPC7r42TLHjTk6cL4Go` | populated | populated |
| `BorshIoError` | 130012038 | 238 | `5R3Q4jHRcT7YitM2MDkx9tCjvKTKrjKA3TipguvgJPSCiqDN7JUFKLBXAS1Gp3QZcG9uxhWjLgxefc7i3qzjks18` | was object; now `"BorshIoError"` | `"BorshIoError"` |
| log trailing space | 86492003 | 188 | `RCh1RNMiBZhcYku4Z1jR8n9REdaXLyEKScSNvebLwEtKBcWTaEupDMKxJhF5Z7mde9UJVTzBEbFotbRp9qNNi6D` | missing final space | final space present |
| log truncation position | 86440850 | 78 | `fPRktTNQgbXtQy5eSVJnQ1BHQZCt4sBNBQXKWq5wxDQEs1RsTC1gPeDnNDaqRHjjsBo4rUpKiH1Ze3jHAU2HQ5u` | `"Log truncated"` too early | log line still present |
| quoted pubkey log | 129762843 | 1448 | `3zyVWbrPdYdT5z4he4mNnWW4jqYNMGC7zCZwoLfC3STwEBQoLnJTFSCvkSwjLAtSbFKjZj8ZfeopXe6b6Dm4yaeJ` | `Signed by "pubkey"` | `Signed by pubkey` |

Raw comparison artifacts on NAS:

- `/tmp/bz-wide-correctness-20260523T001330/full-json-rewards-false`
- `/tmp/bz-wide-helius-20260523T104916/full-json-rewards-false`
- `/tmp/bz-provider-disagree-mainnet-light-20260523T111253/mainnet_light_merged.tsv`
- `/tmp/bz-concordance-mainnet-20260523T105624/mainnet_fixed_probe.tsv`

Recent local/provider artifacts:

- `target/rpc-bench/reward-only-size-compare-20260523T170229Z/none-json-rewards-true`
- `target/rpc-bench/reward-continuation-blockzilla-v2-worker-bounded-20260523T164750Z/full-json-rewards-true`

## Large Rewards Provider Oddity

The epoch-boundary reward blocks are extremely large but appear legitimate. Local
compact inspection showed mostly staking/voting rewards:

| Slot | Rewards | Type breakdown | Blockzilla bytes | Helius bytes | Public mainnet bytes | Triton |
|---:|---:|---|---:|---:|---:|---|
| `216000000` | 607,854 | Staking 603,694; Voting 3,706; Rent 454 | 85,205,809 | 85,205,809 | 85,205,810 | `-32004 Block not available` |
| `259200004` | 1,151,822 | Staking 1,147,274; Voting 4,548 | 160,280,264 | 160,280,264 | 160,280,265 | `-32004 Block not available` |
| `302400000` | 1,201,913 | Staking 1,196,874; Voting 5,039 | 166,761,894 | 166,761,894 | 166,761,895 | `-32004 Block not available` |

Notes:

- The local renderer body is 34 bytes smaller than Blockzilla/Helius because the local measurement omits the JSON-RPC wrapper.
- Public mainnet uses a different reward array order for slots `216000000` and `259200004`, and emits reward object fields in a different key order. Multiset comparison of `(pubkey, lamports, postBalance, rewardType, commission)` is identical to Blockzilla for both slots.
- Triton did not serve these slots in the latest checks even though Helius and public mainnet did.
- Blockzilla originally truncated these huge responses because the Worker stream used an unbounded queue and exceeded memory; the bounded stream fix returns full valid bodies.

## Worker Patch Already Applied

Deployed Worker version: `0f128f8e-34e7-4bec-ab27-f25498785f3d`.

- `BorshIoError` now renders as the string `"BorshIoError"`.
- Lazy `getBlock` response streaming was disabled for now. Render-time failures are detected before the HTTP body starts, so the Worker returns a valid JSON-RPC error instead of an empty or truncated JSON body.

Known affected slots now return structured errors:

- slot `216003648`: `block access is missing vote hash block id 3482`
- slot `216145526`: `block access is missing vote hash block id 142959`

## Vote Hash Sidecar Verification

Verifier: `blockzilla/src/bin/verify_access_vote_hashes.rs`.

The verifier compares:

1. vote-hash block IDs required by compact vote instructions in the hot block,
2. rows embedded in that slot's `archive-v2-block-access.wincode`,
3. expected hashes from `vote_hash_registry.bin`.

Sample results from NAS local sidecars:

| Epoch | Slot | Block id | Required vote hashes | Embedded vote hashes | Missing IDs |
|---:|---:|---:|---:|---:|---|
| 500 | 216003648 | 3483 | 24 | 0 | `3388,3389,3408,3447,3449,3450,3455,3456,3457,3458,3461,3462,3463,3464,3472,3474,3475,3476,3477,3478,3479,3480,3481,3482` |
| 500 | 216145526 | 142961 | 10 | 0 | `142852,142938,142946,142950,142955,142956,142957,142958,142959,142960` |
| 600 | 259200521 | 340 | 20 | 0 | `221,230,285,298,304,315,316,322,325,329,330,331,332,333,334,335,336,337,338,339` |
| 600 | 259202066 | 1677 | 11 | 0 | `1543,1551,1586,1667,1669,1670,1672,1673,1674,1675,1676` |
| 700 | 302403983 | 3886 | 7 | 0 | `3852,3880,3881,3882,3883,3884,3885` |
| 700 | 302409215 | 9029 | 7 | 0 | `8996,9017,9024,9025,9026,9027,9028` |
| 800 | 345609957 | 9912 | 3 | 0 | `9909,9910,9911` |
| 800 | 345610348 | 10303 | 2 | 0 | `10301,10302` |

Conclusion: the current NAS local block-access sidecars for sampled epoch 500-800 failures are not vote-hash-upgraded. `vote_hash_registry.bin` exists, but per-slot access blobs embed zero vote hash rows, so the Worker cannot reconstruct compact vote instructions without fallback.

Next repair step: rebuild or upgrade `archive-v2-block-access.wincode`, `archive-v2-block-access.index`, and `archive-v2-get-block.index` for affected epochs using the vote-hash upgrade path, then upload those sidecars to R2.
