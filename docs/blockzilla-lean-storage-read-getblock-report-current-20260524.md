# Blockzilla lean report - current snapshot - 2026-05-24

## Summary

Reference epoch for the fresh scanner and getBlock comparisons: **920**.

Current-code snapshot: base HEAD `0c824d9d03d8` with a dirty working tree copied and built in release mode on 2026-05-24. Fresh reruns cover scanner/read speed, getBlock HTTP speed, and `blockBin` binary bundle speed. Storage rows use the same measured archive corpus and projection method as the clean 2026-05-24 report because the stored archive files did not change during this rerun.

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
| Token instruction scan | 1 | 420.7s | 1,025 | 1.47M | 236 | 813 | 95.9M decoded token ix |
| Pump.fun tx scan | 1 | 424.2s | 1,017 | 1.46M | 234 | 806 | 3.59M matched tx |
| Pump.fun tx scan | 2 | 236.2s | 1,826 | 2.63M | 420 | 1,447 | 3.59M matched tx |
| Pump.fun tx scan | 4 | 149.2s | 2,890 | 4.16M | 664 | 2,291 | 3.59M matched tx |
| Pump.fun tx scan | 8 | 108.9s | 3,959 | 5.69M | 910 | 3,138 | 3.59M matched tx |

Full metadata token scanning currently fails on the current epoch 920 hot-block metadata view; the token row above is therefore the clean read/index speed for outer instruction scans.

CAR reader format comparison uses epoch 10, full-epoch scan, single core, outer instructions only, no output writes. Epoch 10 has no Token/Pump.fun matches in this scan, so the table is a reader-throughput comparison.

| Workload | Format | Time | Blocks/s | Tx/s | Signal/s | Read MiB/s | Decoded MiB/s |
|---|---|---:|---:|---:|---:|---:|---:|
| Token instruction scan | CAR | 24.2s | 17.4k | 1.06M | 0 | 643 | 643 |
| Token instruction scan | CAR.zst | 15.8s | 26.7k | 1.63M | 0 | 987 | 987 |
| Token instruction scan | BZ blocks | 5.0s | 84.3k | 5.16M | 0 | 137 | 752 |
| Pump.fun tx scan | CAR | 23.8s | 17.7k | 1.08M | 0 | 654 | 654 |
| Pump.fun tx scan | CAR.zst | 16.0s | 26.3k | 1.61M | 0 | 973 | 973 |
| Pump.fun tx scan | BZ blocks | 5.1s | 82.6k | 5.05M | 0 | 134 | 736 |

Jetstreamer comparison uses epoch 920 slots `397440000-397441999` with the program-tracking plugin.

| Method | Threads | Time | TPS | Notes |
|---|---:|---:|---:|---|
| Jetstreamer + local ClickHouse | 1 | 106s | 27.9k | persisted locally |
| Jetstreamer, ClickHouse off | 1 | 102s | ~28.5k | plugin loaded, no persistence |
| Jetstreamer, ClickHouse off | 8 | 28s | ~103.8k | plugin loaded, no persistence |

The 8-thread local-ClickHouse run is excluded because it hung after a ClickHouse decompression error.

## getBlock Speed

All getBlock tables use the same 132-slot test set: epochs `10,100,200,300,400,500,600,700,800,900,920`, first/last boundary plus 10 random slots per epoch.

### full

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| Blockzilla | 132/0 | 483 | 485 | 836 | 2.71 MiB |
| OF worker | 132/0 | 734 | 592 | 1,388 | 2.71 MiB |
| Triton | 132/0 | 423 | 318 | 1,108 | 2.71 MiB |
| Helius | 132/0 | 240 | 181 | 516 | 2.71 MiB |

### signatures

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| Blockzilla | 132/0 | 228 | 149 | 398 | 104 KiB |
| OF worker | 132/0 | 531 | 402 | 1,099 | 104 KiB |
| Triton | 132/0 | 58 | 55 | 85 | 104 KiB |
| Helius | 132/0 | 86 | 76 | 119 | 104 KiB |

### accounts

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| Blockzilla | 132/0 | 274 | 218 | 556 | 1.92 MiB |
| OF worker | 132/0 | 548 | 432 | 974 | 1.93 MiB |
| Triton | 132/0 | 292 | 210 | 833 | 1.92 MiB |
| Helius | 132/0 | 271 | 178 | 698 | 1.92 MiB |

### full + rewards

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| Blockzilla | 131/1 | 1,030 | 702 | 1,495 | 5.59 MiB |
| OF worker | 50/82 | 4,512 | 12 | 1,404 | 1.54 MiB |
| Triton | 128/4 | 466 | 265 | 497 | 2.90 MiB |
| Helius | 132/0 | 473 | 152 | 432 | 6.70 MiB |

`Blockzilla` full+rewards failed once on slot `259200004` with HTTP 500 and an invalid JSON body; the average body excludes that failure and is therefore slightly biased down for the largest reward case. `OF worker` full+rewards is heavily biased because only 50/132 requests succeeded and huge reward blocks mostly failed.

### blockBin

`blockBin` is the direct binary route for dumping the Blockzilla wincode/zstd block bundle for client-side decode. The materialized mode includes the block-access blob; `access=0` returns only the hot block bundle.

| Method | OK/error | Avg ms | P50 ms | P90 ms | Avg body |
|---|---:|---:|---:|---:|---:|
| blockBin, no materialization | 132/0 | 217 | 149 | 467 | 0.79 MiB |
| blockBin + materialization | 132/0 | 389 | 357 | 620 | 1.58 MiB |

## Cloudflare Cost

Pricing assumptions checked on Cloudflare docs: Workers Standard $0.30/M extra requests and $0.02/M extra CPU-ms; R2 Standard $0.015/GB-month and $0.36/M Class B reads; R2 egress free.

Variable Worker cost per 1M successful requests, using measured wall time as a conservative CPU upper bound:

| Method | full | signatures | accounts | full+rewards | blockBin no mat. | blockBin + mat. |
|---|---:|---:|---:|---:|---:|---:|
| Blockzilla worker | ~$9.96 | ~$4.86 | ~$5.78 | ~$20.90 | ~$4.65 | ~$8.07 |
| OF worker | ~$14.98 | ~$10.92 | ~$11.26 | ~$90.54* | n/a | n/a |
| Triton / Helius | external provider cost | external provider cost | external provider cost | external provider cost | n/a | n/a |

`blockBin` R2 read variable cost, added to Worker cost when pricing that route directly:

| Binary route | R2 reads/request | Worker + R2 reads per 1M |
|---|---:|---:|
| blockBin, no materialization | 2 | ~$5.37 |
| blockBin + materialization | 3 | ~$9.15 |

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

Recommended billing floor, assuming `BZ + epoch mat.`, the highest measured Blockzilla JSON request class (`full+rewards`, ~$20.90/M variable), R2 Class B reads, the Triton-priced gRPC full-block stream fixed cost, and a 20% safety margin:

| Monthly volume | Cost to cover | Recommended bill floor |
|---:|---:|---:|
| 1M req/mo | ~$3,015/M | **~$3,618/M** = ~$0.003618/request |
| 10M req/mo | ~$321/M | **~$385/M** = ~$0.000385/request |
| 100M req/mo | ~$51/M | **~$61/M** = ~$0.000061/request |
| 1B req/mo | ~$24/M | **~$29/M** = ~$0.000029/request |

## Annex: RPC Oddities And Bugs

Small test set: the getBlock speed matrix uses 132 slots across epochs `10,100,200,300,400,500,600,700,800,900,920`. The broader correctness run used 1,122 slots across the same epochs. Helius was the primary reference, with Triton and public mainnet used as cross-checks when providers disagreed.

The important lesson is that byte-for-byte RPC parity is not enough by itself. Providers disagree, omit fields, or fail on large historical blocks. Blockzilla should aim at replayable archive data plus explicit transaction-metadata verification: signatures, instruction data, inner instructions, logs, balances, rewards, and vote hashes need independent sidecars/checks so we can prove what was rendered.

### Current Blockzilla / OF Bugs

| Source | Where seen | Current impact | Notes |
|---|---|---:|---|
| Blockzilla full+rewards large boundary failure | slot [259200004](https://explorer.solana.com/block/259200004) | 1/132 failure in the current speed rerun | Worker returned HTTP 500 with a 16-byte invalid JSON body. This should be fixed before claiming full rewards parity. |
| Blockzilla signed-message vs sanitized account writability | tx [4m77...9Zk](https://explorer.solana.com/tx/4m77cwnoB2PnbKdNaX6Z6Hiyvvwh7u1tWReYvPSDF9QmLU2GDewVFbord5fJoWWskER8xFsr14ewtjdxoJKaz9Zk), slot [397698816](https://explorer.solana.com/block/397698816) | Review decision | Signature verification passes for the original message where the System Program is in a writable lookup bucket. Helius/Triton/mainnet render the sanitized account list as `writable:false`. |
| Blockzilla old log rendering differences | Sampled old log text/truncation cases | Still needs cleanup | Helius, Triton, and sampled mainnet agreed against older Blockzilla output on log whitespace/truncation. |
| Old OF worker large reward failures | `full+rewards`, `none+rewards` | 82/132 failures in current `full+rewards` speed rerun | Biases OF avg body downward; not representative for large rewards. |

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

Scanner, Jetstreamer, CAR/CAR.zst reader, and local Blockzilla build/rebuild measurements were run on the same archive host. getBlock and blockBin HTTP latency rows compare deployed workers/providers, so they include provider/network behavior rather than only this machine's local read speed.

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
