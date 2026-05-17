# Big Block Log Bench

Fixture: `crates/old-faithful/car-reader/benches/fixtures/epoch-822-biggest.car`.

Command:

```bash
cargo run --release -p blockzilla --bin big-block-log-bench --features known-program-logs -- --iters 1000 --serialize-iters 200 --markdown docs/big-block-log-bench.md
```

## Summary

| Metric | Value |
|---|---:|
| Fixture bytes | 4654582 |
| Blocks | 1 |
| Transactions | 2969 |
| Transactions with logs | 2968 |
| Log lines | 26767 |
| Registry keys for bench | 5706 |
| Raw UTF-8 bytes incl newline | 1749896 |
| Raw `Vec<String>` wincode bytes | 1937273 |
| Compact log wincode bytes | 422196 |
| Size reduction vs raw wincode | 78.21% |
| Raw/compact ratio | 4.59x |
| Compact events | 26767 |
| Top-level known events | 26767 (100.00%) |
| Plain/unparsed events | 0 (0.00%) |
| Unknown program payload events | 679 (2.54%) |
| String table entries | 2607 |
| String table bytes | 74398 |
| Data table arrays | 438 |
| Data table bytes | 55162 |
| Compact events wincode bytes | 278664 |
| Compact strings wincode bytes | 84842 |
| Compact data wincode bytes | 58690 |
| Parse-only throughput | 4939821 logs/s over 1000 iters |
| Parse + wincode serialize throughput | 5011289 logs/s over 200 iters |
| Unknown payload UTF-8 bytes | 28380 |
| Unknown payload string-table contribution | 31096 |
| Unknown payload programs | 29 |
| Unknown payload shapes | 147 |
| Unknown exact payloads | 270 |

## Parser Kinds

| Kind | Count |
|---|---:|
| Consumed | 3815 |
| Failure | 1387 |
| Invoke | 9168 |
| ProgramConsumption | 24 |
| ProgramData | 300 |
| ProgramLog | 4154 |
| ProgramReturn | 138 |
| Success | 7781 |

## Unknown Program Payloads By Program

| Program | Count | Payload bytes |
|---|---:|---:|
| `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz` | 186 | 4245 |
| `dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH` | 69 | 9016 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | 66 | 4318 |
| `6MWVTis8rmmk6Vt9zmAJJbmb3VuLpzoQ1aHH4N6wQEGh` | 62 | 1152 |
| `6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma` | 46 | 2775 |
| `NA247a7YE9S3p9CdKmMyETx8TTwbSdVbVYHHxpnHTUV` | 45 | 1170 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | 38 | 504 |
| `NA365bsPdvZ8sP58qJ5QFg7eXygCe8aPRRxR9oeMbR5` | 30 | 660 |
| `kekYAXVpBcKEVL5hGw2411qWZZjcnDdi7epDo3Di5et` | 30 | 636 |
| `9w97YXpxtxVZSRooYgs79MogBVJXdDKT8S14R3hKvC4y` | 16 | 752 |
| `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | 14 | 258 |
| `7FT7XejtDRUw62S96VBnUPkJN7EK1PZjamU3Mv6TBu17` | 11 | 495 |
| `CataF1xj8FR7sUGeDtAuqfWj8gzCsD11jbmUosBmpUJs` | 10 | 133 |
| `4pP8eDKACuV7T2rbFPE8CHxGKDYAzSdRsdMsGvz2k4oc` | 9 | 273 |
| `J9G2mzdy3vrgY25GQA1pbysvNNtbnM4mQB2jH4tWT3Mx` | 8 | 32 |
| `GDDMwNyyx8uB6zrqwBFHjLLG3TBYk2F8Az4yrQC5RzMp` | 7 | 495 |
| `CFU3XT4yiuERvM4298sGWuWFxMRmJkeyganCQ3WtuMK3` | 6 | 378 |
| `routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS` | 5 | 217 |
| `HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt` | 5 | 90 |
| `obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y` | 3 | 79 |
| `2TQ4a9igqmz91BtHjnAk5hk7JjVuRwyQBsQfrGGpcXYK` | 2 | 81 |
| `DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm` | 2 | 57 |
| `mmm3XBJg5gk8XJxEKBvdgptZz6SgK4tXvn36sodowMc` | 2 | 38 |
| `King7ki4SKMBPb3iupnQwTyjsq294jaXsgLmJo8cb7T` | 2 | 32 |
| `FVbHFJ5fKrpityVrGimDbiZSLct5XG3ukeuFUkiHffgb` | 1 | 255 |
| `SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE` | 1 | 137 |
| `M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K` | 1 | 38 |
| `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | 1 | 36 |
| `3i5JeuZuUxeKtVysUnwQNGerJP2bSMX9fTFfS4Nxe3Br` | 1 | 28 |

## Unknown Program Payload Shapes

| Program | Shape | Count | Payload bytes |
|---|---|---:|---:|
| `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz` | `SolanaMevBot.com` | 93 | 1488 |
| `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz` | `No profitable arbitrage found` | 88 | 2552 |
| `dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH` | `<B64>` | 45 | 7740 |
| `NA247a7YE9S3p9CdKmMyETx8TTwbSdVbVYHHxpnHTUV` | `No arbitrage profit found!` | 45 | 1170 |
| `6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma` | `<PUBKEY>` | 26 | 1142 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `Sending batch <N> with header and <N> market events total events sent <N>` | 20 | 1397 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `Discriminant for phoenix::program::accounts::MarketHeader is <N>` | 19 | 1520 |
| `9w97YXpxtxVZSRooYgs79MogBVJXdDKT8S14R3hKvC4y` | `tm <PUBKEY>` | 16 | 752 |
| `NA365bsPdvZ8sP58qJ5QFg7eXygCe8aPRRxR9oeMbR5` | `No arbitrage profit found!` | 15 | 390 |
| `NA365bsPdvZ8sP58qJ5QFg7eXygCe8aPRRxR9oeMbR5` | `<N> <N> <N>` | 15 | 270 |
| `dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH` | `Posting new lazer update current ts <N> < next ts <N>` | 12 | 960 |
| `dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH` | `Price updated to <N>` | 12 | 316 |
| `7FT7XejtDRUw62S96VBnUPkJN7EK1PZjamU3Mv6TBu17` | `All left dlmm bin arrays are consumed idx <N>` | 11 | 495 |
| `6MWVTis8rmmk6Vt9zmAJJbmb3VuLpzoQ1aHH4N6wQEGh` | `<N> - <N> <N> <N>` | 11 | 115 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `Discriminant for phoenix::program::accounts::Seat is <N>` | 8 | 576 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `PhoenixInstruction::CancelUpToWithFreeFunds` | 8 | 344 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `f_in <N>` | 8 | 96 |
| `J9G2mzdy3vrgY25GQA1pbysvNNtbnM4mQB2jH4tWT3Mx` | `🤑` | 8 | 32 |
| `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | `Right <N>` | 7 | 134 |
| `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | `Left <N>` | 7 | 124 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `PhoenixInstruction::PlaceMultiplePostOnlyOrders` | 6 | 282 |
| `kekYAXVpBcKEVL5hGw2411qWZZjcnDdi7epDo3Di5et` | `log_1 <N>` | 6 | 48 |
| `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz` | `No profitable arbitrage opportunity found` | 5 | 205 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `mi s <N> ma s <N> bp <N>` | 4 | 116 |
| `HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt` | `INVARIANT SWAP` | 4 | 60 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `<N> fra <N>` | 4 | 43 |
| `6MWVTis8rmmk6Vt9zmAJJbmb3VuLpzoQ1aHH4N6wQEGh` | `b e > m` | 4 | 28 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `e p <N>` | 4 | 24 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `p <N>` | 4 | 12 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `r <N>` | 4 | 12 |

## Unknown Exact Payloads

| Program | Payload | Count | Payload bytes |
|---|---|---:|---:|
| `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz` | `SolanaMevBot.com` | 93 | 1488 |
| `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz` | `No profitable arbitrage found` | 88 | 2552 |
| `NA247a7YE9S3p9CdKmMyETx8TTwbSdVbVYHHxpnHTUV` | `No arbitrage profit found!` | 45 | 1170 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `Discriminant for phoenix::program::accounts::MarketHeader is 8167313896524341111` | 19 | 1520 |
| `NA365bsPdvZ8sP58qJ5QFg7eXygCe8aPRRxR9oeMbR5` | `No arbitrage profit found!` | 15 | 390 |
| `NA365bsPdvZ8sP58qJ5QFg7eXygCe8aPRRxR9oeMbR5` | `500000000000 1 251` | 15 | 270 |
| `7FT7XejtDRUw62S96VBnUPkJN7EK1PZjamU3Mv6TBu17` | `All left dlmm bin arrays are consumed, idx: 2` | 11 | 495 |
| `dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH` | `Posting new lazer update. current ts 1753307455800000 < next ts 1753307456200000` | 9 | 720 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `Discriminant for phoenix::program::accounts::Seat is 2002603505298356104` | 8 | 576 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `PhoenixInstruction::CancelUpToWithFreeFunds` | 8 | 344 |
| `J9G2mzdy3vrgY25GQA1pbysvNNtbnM4mQB2jH4tWT3Mx` | `🤑` | 8 | 32 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `PhoenixInstruction::PlaceMultiplePostOnlyOrders` | 6 | 282 |
| `kekYAXVpBcKEVL5hGw2411qWZZjcnDdi7epDo3Di5et` | `log_1: 0` | 6 | 48 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `Sending batch 1 with header and 8 market events, total events sent: 8` | 5 | 345 |
| `MEViEnscUm6tsQRoGd9h6nLQaQspKj7DB2M5FwM3Xvz` | `No profitable arbitrage opportunity found` | 5 | 205 |
| `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY` | `Sending batch 1 with header and 32 market events, total events sent: 32` | 4 | 284 |
| `9w97YXpxtxVZSRooYgs79MogBVJXdDKT8S14R3hKvC4y` | `tm F5sc3CGLmKueyUqaCkwopo9u61HsgpmJc8Cmc9xibonk` | 4 | 188 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `mi s: 0, ma s: 50000000 bp: 0` | 4 | 116 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `f_in: 15000000000` | 4 | 68 |
| `HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt` | `INVARIANT: SWAP` | 4 | 60 |
| `6MWVTis8rmmk6Vt9zmAJJbmb3VuLpzoQ1aHH4N6wQEGh` | `b e > m` | 4 | 28 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `f_in: 0` | 4 | 28 |
| `CatMoR2RWH47v8TYnKi76oV57E5DhYMRqAroKUGCMxTu` | `e p: 0` | 4 | 24 |
| `dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH` | `Posting new lazer update. current ts 1753307455969000 < next ts 1753307456368000` | 3 | 240 |
| `9w97YXpxtxVZSRooYgs79MogBVJXdDKT8S14R3hKvC4y` | `tm 9GtvcnDUvGsuibktxiMjLQ2yyBq5akUahuBs8yANbonk` | 3 | 141 |
| `9w97YXpxtxVZSRooYgs79MogBVJXdDKT8S14R3hKvC4y` | `tm 9tqjeRS1swj36Ee5C1iGiwAxjQJNGAVCzaTLwFY8bonk` | 3 | 141 |
| `4pP8eDKACuV7T2rbFPE8CHxGKDYAzSdRsdMsGvz2k4oc` | `The provided timestamp is valid.` | 3 | 96 |
| `4pP8eDKACuV7T2rbFPE8CHxGKDYAzSdRsdMsGvz2k4oc` | `Received timestamp: 1753307476` | 3 | 90 |
| `4pP8eDKACuV7T2rbFPE8CHxGKDYAzSdRsdMsGvz2k4oc` | `Current timestamp: 1753307456` | 3 | 87 |
| `6MWVTis8rmmk6Vt9zmAJJbmb3VuLpzoQ1aHH4N6wQEGh` | `1 < 345568.47 320237.56` | 3 | 69 |
