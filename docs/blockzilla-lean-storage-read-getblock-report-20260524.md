# Blockzilla lean report - 2026-05-24

## Summary

Reference epoch for the fresh scanner and getBlock comparisons: **920**.

Storage totals are computed by summing the per-epoch CAR report rows, then applying measured compression curves. Rows marked with `*` are expected totals: CAR.zst uses the measured corpus for epochs 0-963 and CAR-growth extrapolation for 964-974; Blockzilla uses measured reference epochs and a CAR-growth interpolation between adjacent anchors. This avoids a single epoch multiplier and preserves the real one-epoch-at-a-time growth curve. Values in parentheses are compression multipliers: `x2` means 2 times smaller than the relevant baseline.

| Epoch | CAR | CAR + OF index | CAR.zst | BZ blocks | BZ + epoch mat. | BZ + block mat. |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 15.0 GB | 21.0 GB | 5.9 GB (x2.5) | 0.7 GB (x21.4) | 3.6 GB (x5.8) | 6.6 GB (x3.2) |
| 100 | 59.0 GB | 69.0 GB | 22.8 GB (x2.6) | 3.4 GB (x17.4) | 13.2 GB (x5.2) | 24.9 GB (x2.8) |
| 200 | 143.0 GB | 166.0 GB | 55.9 GB (x2.6) | 10.1 GB (x14.2) | 31.3 GB (x5.3) | 59.1 GB (x2.8) |
| 300 | 473.0 GB | 535.0 GB | 206.3 GB (x2.3) | 43.7 GB (x10.8) | 99.2 GB (x5.4) | 201.7 GB (x2.7) |
| 400 | 615.0 GB | 693.0 GB | 245.1 GB (x2.5) | 57.1 GB (x10.8) | 116.6 GB (x5.9) | 229.4 GB (x3.0) |
| 500 | 477.0 GB | 541.0 GB | 180.7 GB (x2.6) | 38.7 GB (x12.3) | 199.8 GB (x2.7) | 302.0 GB (x1.8) |
| 600 | 555.0 GB | 615.0 GB | 237.0 GB (x2.3) | 64.2 GB (x8.6) | 207.6 GB (x3.0) | 305.9 GB (x2.0) |
| 700 | 627.0 GB | 698.0 GB | 298.2 GB (x2.1) | 75.0 GB (x8.4) | 257.7 GB (x2.7) | 375.2 GB (x1.9) |
| 800 | 768.0 GB | 848.0 GB | 331.1 GB (x2.3) | 83.3 GB (x9.2) | 259.1 GB (x3.3) | 378.3 GB (x2.2) |
| 900 | 491.0 GB | 544.0 GB | 226.1 GB (x2.2) | 59.9 GB (x8.2) | 103.5 GB (x5.3) | 187.0 GB (x2.9) |
| 920 | 866.0 GB | 954.0 GB | 390.8 GB (x2.2) | 104.0 GB (x8.3) | 163.6 GB (x5.8) | 276.2 GB (x3.5) |
| Total | 443.7 TB | 496.3 TB | 189.5 TB* (x2.3) | 45.8 TB* (x9.7) | 130.3 TB* (x3.8) | 210.3 TB* (x2.4) |

`mat.` means materialization data: sidecar/metadata tables used to reconstruct rendered blocks, not to search. `BZ + epoch mat.` keeps shared epoch-level materialization data with less redundancy; `BZ + block mat.` adds per-block materialization data for faster rendering at higher storage cost.

## Read Speed

All scanner rows below use epoch 920, full-epoch scan, outer instructions only, no output writes.

| Workload | Threads | Time | Blocks/s | Tx/s | Read MiB/s | Decoded MiB/s | Result |
|---|---:|---:|---:|---:|---:|---:|---|
| Token instruction scan | 1 | 419.8s | 1,027 | 1.48M | 236 | 814 | 95.9M decoded token ix |
| Pump.fun tx scan | 1 | 423.3s | 1,019 | 1.47M | 234 | 808 | 3.59M matched tx |
| Pump.fun tx scan | 2 | 236.1s | 1,827 | 2.63M | 420 | 1,448 | 3.59M matched tx |
| Pump.fun tx scan | 4 | 147.8s | 2,918 | 4.20M | 671 | 2,313 | 3.59M matched tx |
| Pump.fun tx scan | 8 | 109.4s | 3,943 | 5.67M | 907 | 3,126 | 3.59M matched tx |

Full metadata token scanning currently fails on the current epoch 920 hot-block metadata view; the token row above is therefore the clean read/index speed for outer instruction scans.

CAR reader format comparison uses epoch 10, full-epoch scan, single core, outer instructions only, no output writes. Epoch 10 has no Token/Pump.fun matches in this scan, so the table is a reader-throughput comparison.

| Workload | Format | Time | Blocks/s | Tx/s | Signal/s | Read MiB/s | Decoded MiB/s |
|---|---|---:|---:|---:|---:|---:|---:|
| Token instruction scan | CAR | 22.6s | 18.6k | 1.14M | 0 | 687 | 687 |
| Token instruction scan | CAR.zst | 15.4s | 27.3k | 1.67M | 0 | 1,010 | 1,010 |
| Token instruction scan | BZ blocks | 4.5s | 93.7k | 5.74M | 0 | 153 | 835 |
| Pump.fun tx scan | CAR | 24.1s | 17.4k | 1.07M | 0 | 644 | 644 |
| Pump.fun tx scan | CAR.zst | 15.4s | 27.2k | 1.67M | 0 | 1,006 | 1,006 |
| Pump.fun tx scan | BZ blocks | 4.6s | 90.5k | 5.54M | 0 | 147 | 806 |

Jetstreamer comparison uses epoch 920 slots `397440000-397441999` with the program-tracking plugin.

| Method | Threads | Time | TPS | Notes |
|---|---:|---:|---:|---|
| Jetstreamer + local ClickHouse | 1 | 106s | 27.9k | persisted locally |
| Jetstreamer, ClickHouse off | 1 | 102s | ~28.5k | plugin loaded, no persistence |
| Jetstreamer, ClickHouse off | 8 | 28s | ~103.8k | plugin loaded, no persistence |

The 8-thread local-ClickHouse run is excluded because it hung after a ClickHouse decompression error.

## getBlock Speed

All getBlock tables use the same 132-slot test set: epochs `10,100,200,300,400,500,600,700,800,900,920`, first/last boundary plus 10 random slots per epoch. Blockzilla `full` rows were refreshed after the latest production fix; provider rows are from the original comparison run.

### full

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| Blockzilla | 132/0 | 1,290 | 1,091 | 2,749 | 2.71 MiB |
| OF worker | 132/0 | 1,343 | 1,217 | 2,239 | 2.71 MiB |
| Triton | 132/0 | 1,334 | 1,056 | 2,944 | 2.71 MiB |
| Helius | 132/0 | 850 | 670 | 1,916 | 2.71 MiB |

### signatures

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| Blockzilla | 132/0 | 261 | 205 | 464 | 104 KiB |
| OF worker | 132/0 | 621 | 488 | 1,213 | 104 KiB |
| Triton | 132/0 | 135 | 117 | 216 | 104 KiB |
| Helius | 132/0 | 149 | 114 | 267 | 104 KiB |

### accounts

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| Blockzilla | 132/0 | 1,360 | 1,218 | 2,771 | 1.92 MiB |
| OF worker | 132/0 | 1,657 | 1,476 | 3,155 | 1.93 MiB |
| Triton | 132/0 | 1,633 | 1,421 | 3,405 | 1.92 MiB |
| Helius | 132/0 | 1,336 | 1,235 | 2,476 | 1.92 MiB |

### full + rewards

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| Blockzilla | 132/0 | 3,079 | 1,440 | 3,593 | 6.70 MiB |
| OF worker | 37/95 | 500 | 387 | 1,072 | 555 KiB |
| Triton | 128/4 | 2,009 | 1,909 | 3,597 | 2.90 MiB |
| Helius | 131/1 | 3,367 | 1,897 | 4,773 | 5.54 MiB |

`OF worker` full+rewards is biased because only 37/132 requests succeeded and huge reward blocks mostly failed.

## Cloudflare Cost

Pricing assumptions checked on Cloudflare docs: Workers Standard $0.30/M extra requests and $0.02/M extra CPU-ms; R2 Standard $0.015/GB-month and $0.36/M Class B reads; R2 egress free.

Variable cost per 1M successful requests, using measured wall time as a conservative CPU upper bound:

| Method | full | signatures | accounts | full+rewards |
|---|---:|---:|---:|---:|
| Blockzilla worker | ~$26.10 | ~$6.60 | ~$28.58 | ~$61.88 |
| OF worker | ~$27.52 | ~$13.08 | ~$33.80 | ~$10.66* |
| Triton / Helius | external provider cost | external provider cost | external provider cost | external provider cost |

Storage and R2-read cost per month. Cells show fixed storage plus the Class B read variable cost for the request volume; Worker CPU/request cost is the separate variable table above.

| Storage layout | Fixed storage/mo | 10M req/mo | 100M req/mo | 1B req/mo |
|---|---:|---:|---:|---:|
| BZ blocks | ~$687 | ~$687 + $4 = $691 | ~$687 + $36 = $723 | ~$687 + $360 = $1,047 |
| BZ + epoch mat. | ~$1,955 | ~$1,955 + $4 = $1,958 | ~$1,955 + $36 = $1,991 | ~$1,955 + $360 = $2,315 |
| BZ + block mat. | ~$3,155 | ~$3,155 + $4 = $3,158 | ~$3,155 + $36 = $3,191 | ~$3,155 + $360 = $3,515 |
| CAR.zst | ~$2,843 | ~$2,843 + $4 = $2,846 | ~$2,843 + $36 = $2,879 | ~$2,843 + $360 = $3,203 |
| CAR+OF index | ~$7,445 | ~$7,445 + $4 = $7,448 | ~$7,445 + $36 = $7,481 | ~$7,445 + $360 = $7,805 |

Live gRPC stream ingest fixed cost, using Triton PAYG streaming bandwidth pricing as the reference:

| Stream source | Assumption | Fixed cost/mo |
|---|---:|---:|
| Triton gRPC full-block stream | epoch 920 CAR proxy: 866 GB/epoch * ~15 epochs/mo * $0.08/GB | ~$1,039 |

Recommended billing floor, assuming `BZ + epoch mat.`, the highest measured Blockzilla request class (`full+rewards`, ~$61.88/M variable), R2 Class B reads, the Triton-priced gRPC full-block stream fixed cost, and a 20% safety margin:

| Monthly volume | Cost to cover | Recommended bill floor |
|---:|---:|---:|
| 1M req/mo | ~$3,056/M | **~$3,668/M** = ~$0.003668/request |
| 10M req/mo | ~$362/M | **~$434/M** = ~$0.000434/request |
| 100M req/mo | ~$92/M | **~$111/M** = ~$0.000111/request |
| 1B req/mo | ~$65/M | **~$78/M** = ~$0.000078/request |

## Annex: RPC Oddities And Bugs

Small test set: the getBlock speed matrix uses 132 slots across epochs `10,100,200,300,400,500,600,700,800,900,920`. The broader correctness run used 1,122 slots across the same epochs. Helius was the primary reference, with Triton and public mainnet used as cross-checks when providers disagreed.

The important lesson is that byte-for-byte RPC parity is not enough by itself. Providers disagree, omit fields, or fail on large historical blocks. Blockzilla should aim at replayable archive data plus explicit transaction-metadata verification: signatures, instruction data, inner instructions, logs, balances, rewards, and vote hashes need independent sidecars/checks so we can prove what was rendered.

### Current Blockzilla / OF Bugs

| Source | Where seen | Current impact | Notes |
|---|---|---:|---|
| Blockzilla missing vote-hash sidecar rows | Older `full` / `full+rewards` checks on epochs 500-920 and epoch 920 | Fixed for the 132-slot production rerun: `132/0` for both `full,false` and `full,true` | This was the body-size skew in the earlier table. Access rebuild/sync plus structured errors removed empty-body behavior. |
| Blockzilla signed-message vs sanitized account writability | tx [4m77...9Zk](https://explorer.solana.com/tx/4m77cwnoB2PnbKdNaX6Z6Hiyvvwh7u1tWReYvPSDF9QmLU2GDewVFbord5fJoWWskER8xFsr14ewtjdxoJKaz9Zk), slot [397698816](https://explorer.solana.com/block/397698816) | Review decision | Signature verification passes for the original message where the System Program is in a writable lookup bucket. Helius/Triton/mainnet render the sanitized account list as `writable:false`. |
| Blockzilla old log rendering differences | Sampled old log text/truncation cases | Still needs cleanup | Helius, Triton, and sampled mainnet agreed against older Blockzilla output on log whitespace/truncation. |
| Old OF worker large reward failures | `full+rewards`, `none+rewards` | 95/132 failures in `full+rewards`; 105/132 in `none+rewards` | Biases OF avg body downward; not representative for large rewards. |

### Helius / Triton Oddities

| Criticality | Oddity | Owner / verdict | Evidence |
|---|---|---|---|
| Critical | Missing or wrong inner instructions | Helius returned `null` for old epoch 100 cases while Blockzilla/Triton/mainnet had populated arrays | tx [prug4...cL4Go](https://explorer.solana.com/tx/prug4MEZ9AsGFXzQxWEMdotxVgCL1tAizFZAWsimwdKj4MD51tq4qKxYfTWSZh9reJ3qZPC7r42TLHjTk6cL4Go); tx [3feSo...BZgg](https://explorer.solana.com/tx/3feSoagy1oR8F1cNnq5s4e9su6PzY8p6VKTAMrMDX1sw9F5g7iDNfFAy8xHnxGUi3hdb86sDPoJ1q3WC9NzsBZgg) |
| High | Triton `CallChainTooDeep` transaction error disagreement | Helius and public mainnet agreed against Triton in targeted checks | tx [4G2my...PHL](https://explorer.solana.com/tx/4G2myULx9mtXTTumYAedNuvtEZPtLFgx84N1W93SeupMtJNzQvss1R1v8VMfogbNqqBTiNC81gSe33HNuodBdPHL); tx [DPWX...J5B](https://explorer.solana.com/tx/DPWXg6nEjQTFAfuPM5ZZE8L5STT3EybpUEaB7DHFDjwUVxW5JSn5FeaPYVtG9M1ozLTozKc8RiFAycnoYyqfJ5B) |
| High | Triton reward block availability | Triton returned `-32004 Block not available` for reward-heavy boundary slots that Helius and public mainnet served | slot [216000000](https://explorer.solana.com/block/216000000); slot [259200004](https://explorer.solana.com/block/259200004); slot [302400000](https://explorer.solana.com/block/302400000) |
| Medium | Triton skipped-slot / blockTime behavior | Triton returned skipped-slot errors or timestamps where other references returned `null` | slot [129600000](https://explorer.solana.com/block/129600000); slot [172800000](https://explorer.solana.com/block/172800000); slot [4320000](https://explorer.solana.com/block/4320000) |
| Medium | Helius `BorshIoError` provider shape | Helius emitted `{"BorshIoError":"Unknown"}`; Blockzilla/Triton/public mainnet emit `"BorshIoError"` | tx [5R3Q...ks18](https://explorer.solana.com/tx/5R3Q4jHRcT7YitM2MDkx9tCjvKTKrjKA3TipguvgJPSCiqDN7JUFKLBXAS1Gp3QZcG9uxhWjLgxefc7i3qzjks18) |
| Medium | Huge reward ordering/format differences | Public mainnet can order rewards differently from Blockzilla/Helius while reward multisets match | slot [216000000](https://explorer.solana.com/block/216000000); slot [259200004](https://explorer.solana.com/block/259200004) |
| Medium | Epoch-boundary `previousBlockhash` | Triton returns zero hash on some boundaries; Blockzilla/Helius/mainnet return real hash | slot [4320000](https://explorer.solana.com/block/4320000); slot [43200000](https://explorer.solana.com/block/43200000); slot [172800020](https://explorer.solana.com/block/172800020) |
| Medium | Token balance nullability and owner/programId differences | Mostly Triton/provider-era differences; epoch 10 `postTokenBalances` nullability differs | tx [EyQ9...w5Te](https://explorer.solana.com/tx/EyQ9L6q9f9rgPNQidngcSxpfVtHiG9kzgUviZYCd3H3kzLCcvcsxSVQ4qsDNvCmFgsxbdoWUsvTG3mAyV91w5Te); tx [ftuy...Rpvw](https://explorer.solana.com/tx/ftuyTgxFZ63f9CyYA1aYf4yzMVG6ecUVXPYqWZ8SJiQvb9qK2ieAoN7k7Ju2K6UsNA8ZQydDmcdWZ1SCMreRpvw) |
| Medium | Helius large reward body decode | Helius had isolated body decode failures on reward-heavy boundary slots | slot [259200004](https://explorer.solana.com/block/259200004); slot [302400000](https://explorer.solana.com/block/302400000) |
| Low | `uiTokenAmount.uiAmount` rounding | Ignored only as tiny float noise | tx [3rUE...zrot](https://explorer.solana.com/tx/3rUEdvWyJ14sNoXGbFxiq4vNQHWurPtiBEfcy6SG5LTbhCFNxCCw8wX7ipkPcSVgmpJVjsahf7cy4khWB8pyzrot); tx [22Ys...JaC](https://explorer.solana.com/tx/22Ys5LJiiLsBsTZNUDBuEsvyDJ7WeN1N6WDZPySt7BSLBYe1MPgZPuBN23eX3SXVCejrNtmq5PmW2kKktAGcsJaC) |

## Annex: Test Machine

Scanner, Jetstreamer, CAR/CAR.zst reader, and local Blockzilla build/rebuild measurements were run on the same archive host. getBlock HTTP latency rows compare deployed workers/providers, so they include provider/network behavior rather than only this machine's local read speed.

| Component | Value |
|---|---|
| Host | Blockzilla-00 |
| OS / kernel | Debian GNU/Linux 12, Linux 6.12.30+ |
| CPU | Intel Core i5-1235U, 12 logical CPUs, 10 cores, 1 socket |
| Memory | 7.5 GiB RAM, 9.7 GiB swap |
| Primary archive storage | 8 x 28 TB Seagate Exos HDDs, RAID0/LVM, 203.6 TB usable |
| Cache tier | 2 x 4 TB Crucial P310 NVMe SSDs, RAID1/LVM cache |
| Network | 10 Gbit/s Ethernet, full duplex |

## Sources

- Old Faithful CAR report: https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md
- Triton One pricing: https://triton.one/pricing
- Cloudflare Workers pricing: https://developers.cloudflare.com/workers/platform/pricing/
- Cloudflare R2 pricing: https://developers.cloudflare.com/r2/pricing/
