# Epoch 300: SSD input, HDD output

All nine tests passed. Each example ran once, with the same reader binaries as the HDD baseline. The run finished on 5 September 2026 at 13:26:37 Paris time. No further tests were queued.

## Results

Speed-up is HDD time divided by SSD time. Read speed is the reader-reported logical input rate in decimal MB/s. TPS uses total execution time, including setup.

| Format | Example | HDD seconds | SSD seconds | Speed-up | SSD read MB/s | SSD TPS |
|---|---|---:|---:|---:|---:|---:|
| Compact V2 | Count | 238.34 | 73.24 | 3.25× | 597.13 | 9,895,330 |
| Compact V2 | USDC | 249.96 | 89.05 | 2.81× | 495.64 | 8,138,018 |
| Compact V2 | Pump.fun | 270.08 | 146.33 | 1.85× | 298.74 | 4,952,776 |
| Compact V2 | FireWatch | 257.23 | 117.74 | 2.18× | 371.20 | 6,155,263 |
| Indexer V3 | Count | 111.86 | 56.20 | 1.99× | 252.45 | 12,894,807 |
| Indexer V3 | USDC | 121.06 | 73.63 | 1.64× | 197.41 | 9,842,452 |
| Indexer V3 | Pump.fun | 1.455 | 0.186 | 7.83× | — | — |
| Indexer V3 | FireWatch | 0.155 | 0.131 | 1.18× | — | — |
| CAR | Count | 3,553.72 | 817.01 | 4.35× | 622.21 | 887,056 |

V3 Pump.fun and FireWatch selected zero candidate blocks and decoded zero transactions for these epoch-300 queries. Their times measure setup and index lookup. The raw metrics contain effective TPS for the full requested epoch; that is not decoded TPS. No payload read bandwidth is reported for these two cases. These results do not measure a positive-match Pump.fun or FireWatch dump.

## Per-example detail

The CPU column uses the sampled process CPU time: 100% is one logical CPU. Memory is the largest resident-memory sample, not an exact peak. Samples were collected about every 10 seconds. The two very short V3 lookups ended before useful CPU or memory samples were collected; their missing measurements are not zero use.

| Format | Example | Setup seconds | Scan seconds | Logical input GB | Output rows | Output GB | Sampled CPU | Sampled max RAM MiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| V2 | Count | 0.085 | 73.154 | 43.683 | Hour buckets on stdout | — | 882% | 92.4 |
| V2 | USDC | 0.040 | 89.014 | 44.119 | 72,587,929 | 9.872 | 892% | 742.3 |
| V2 | Pump.fun | 0.091 | 146.236 | 43.686 | 0 | 44-byte header only | 936% | 388.0 |
| V2 | FireWatch | 0.050 | 117.690 | 43.686 | 0 | 44-byte header only | 914% | 172.3 |
| V3 | Count | 0.151 | 56.047 | 14.149 | Hour buckets on stdout | — | 915% | 154.1 |
| V3 | USDC | 0.167 | 73.435 | 14.497 | 72,587,929 | 9.872 | 832% | 744.9 |
| V3 | Pump.fun | 0.159 | 0.022 | 0 payload | 0 | 44-byte header only | Not sampled | Not sampled |
| V3 | FireWatch | 0.113 | 0.015 | 0 payload | 0 | 44-byte header only | Not sampled | Not sampled |
| CAR | Count | 0.018 | 816.987 | 508.338 | Hour buckets on stdout | — | 130% | 72.7 |

All count examples scan the epoch and group counts into 9,000-slot windows. USDC emits recorded pre/post token balances, not parsed mint/burn/transfer instruction events. Pump.fun emits matching transactions. FireWatch emits distinct programs reached by successful transactions from the configured signer wallet. These runs use binary dump files, not SQLite databases.

### Count

V2 and V3 both used 12 workers, and both reached 12 active workers. V3 read 3.09 times fewer bytes and completed 1.30 times faster than V2 on SSD. CAR read 35.9 times more bytes than V3; its reader pipeline used about 1.30 logical CPUs on average. This result is not evidence of a 12-worker CAR scan.

### USDC

Both formats emitted 72,587,929 rows, totaling 9,871,958,388 bytes. V3 selected all 408,989 blocks and decoded all 724,730,034 transactions. Its 1.21× advantage over V2 on SSD is therefore not due to skipping blocks in this test. The smaller logical input and format-specific decode path remain relevant. Output still went to HDD.

### Pump.fun

There were no matching rows in this epoch. V2 scanned about 43.69 GB and took 146.33 seconds. V3 rejected all blocks through its index and took 0.186 seconds. This is an empty-result filtering comparison; it does not show the cost of writing a large Pump.fun dump.

### FireWatch

The configured wallet query also returned no rows. V2 scanned about 43.69 GB and took 117.74 seconds. V3 selected no blocks and took 0.131 seconds. A later epoch with matching wallet activity is required to measure positive-result index construction.

### Where the next optimization review can focus

V2's recorded input-queue wait was only 0.026–0.055 seconds per SSD test. The workers used roughly 8.8–9.4 logical CPUs. This shifts attention toward decoding and field selection rather than input starvation for these runs. Pump.fun had the largest summed worker field-selection time, 1,248 seconds across workers. Worker sums overlap and must not be added to wall time or to other overlapping stage timers. These measurements do not identify a particular allocation or lock as the cause; profiling is still needed.

CAR sustained 622 MB/s at much lower CPU use. Its next review should examine input delivery and reader-stage overlap as well as decode work. We did not change its thread count during this test.

## Correctness checks

The three SSD count results match each other and their respective HDD results, including every slot-derived hourly bucket:

- 408,989 blocks.
- 724,730,034 transactions.
- 102,252,970 recorded inner instructions.

For all six V2/V3 workload runs, output schema, row count, byte count, completeness, indeterminate-transaction count, and coverage digest match the corresponding HDD result. The runner also reports cross-format parity for each workload within the SSD run. The additional HDD comparison checked these metrics, not every output byte. These checks are not full ledger cryptographic validation.

## What changed

Only the archive input path changed in the benchmark command. Outputs and result logs remained on the HDD pool. The launcher checked reader binary hashes against the HDD baseline and checked source/destination file lengths. It did not hash archive files.

- HDD input: ext4 on eight-drive RAID0.
- SSD input: Btrfs on two-NVMe RAID0, with 64 KiB RAID chunks.
- V2 and V3 used 12 workers. CAR used its existing reader pipeline without a worker-count override.
- Input order: all four V2 examples, all four V3 examples, then CAR count. Each read the full selected epoch, subject to the format's query filtering.

This strongly supports storage input as a major limit in the HDD run. It does not prove that hardware alone explains the whole difference: filesystem, cache state, and background activity were not controlled.

V3 count still finishes before V2 count while reporting fewer MB/s. It reads about 14.15 GB of logical input, versus 43.68 GB for V2 and 508.34 GB for CAR. Reading fewer bytes is part of the format benefit; MB/s alone is not an application-performance ranking.

## Copy and measurement limits

The copy completed at 13:02:48 Paris time, including the final write flush. It took 4,313 seconds (71 minutes 53 seconds). The copy tool reported 150.54 MB/s, with about 649.2 GB transferred for a 704.85 GB logical file set. Shared hard links avoided duplicate transfers.

The epoch-1027 download was active when copying began, but its process was no longer present in the 12:16 check. The OS cache was not cleared. Files were copied before benchmarking; this is not a controlled cold-cache test. Each measurement is one pass, not a statistical estimate.

## Saved evidence

- [SSD metrics](epoch-300-ssd-input-2026-09-05-summary.tsv).
- [HDD baseline metrics](epoch-300-2026-09-05-summary.tsv).
- [V2 count and hourly buckets](epoch-300-ssd-input-2026-09-05-compact-v2-count.json).
- [V3 count and hourly buckets](epoch-300-ssd-input-2026-09-05-indexer-v3-count.json).
- [CAR count and hourly buckets](epoch-300-ssd-input-2026-09-05-car-count.json).

NAS results: `/volume1/blockzilla/benchmark-results/epoch-300-ssd-input-20260905`.

SSD archive root: `/volume2/blockzilla-bench/archive`.

Next work, not started: use this SSD input as the baseline for CPU, allocation, and thread-wait profiling. Test SSD output separately if output writes appear to limit a workload.
