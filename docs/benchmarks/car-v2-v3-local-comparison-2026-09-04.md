# CAR, Compact V2, and Indexer V3: local comparison

Status: Compact V2 and Indexer V3 are complete for all 11 sample epochs. The
CAR run is in progress. CAR measurements in this report are preliminary.

## Stored size

| Format | All 11 sample epochs | Relative to V3 |
|---|---:|---:|
| CAR | 5.290 TB | 5.39x larger |
| Compact V2 | 986.97 GB | 0.47% larger |
| Indexer V3 | 982.37 GB | baseline |

V3 is not mainly a smaller replacement for V2. Its main benefit is selective
reading. CAR is much larger because its general content-addressed structure
retains data and framing that these application readers do not need.

## Complete V2 and V3 result

Each value is the sum of one local-disk run on epochs 0, 100, ..., 1000.

| Workload | Compact V2 | Indexer V3 | V3 speedup |
|---|---:|---:|---:|
| Count transactions and inner instructions | 53m 07s | 22m 29s | 2.36x |
| Recorded USDC token balances | 57m 10s | 23m 28s | 2.44x |
| Pump.fun transaction dump | 1h 18m 28s | 34m 45s | 2.26x |
| FireWatch wallet to program list | 1h 00m 01s | 8.21s | 438.79x |

The FireWatch result is the strongest V3 design result. Its reverse index can
reject almost every block before payload decoding. V2 must scan the epoch.

## Large-epoch full scan

Epoch 300 is the first large CAR scan in the active run. The CAR time is a live
projection from its stable scan rate. V2 and V3 times are complete results.

| Metric | CAR | Compact V2 | Indexer V3 |
|---|---:|---:|---:|
| Count workload time | about 87m | 3m 02s | 1m 32s |
| Transactions per second | about 140K | 3.99M | 7.91M |
| Data read | about 508.3 GB | 43.7 GB | 14.1 GB |
| Read rate | about 95-110 MB/s | 240.6 MB/s | 155.9 MB/s |
| Speedup from CAR | baseline | about 28.8x | about 57.1x |

V3 has a lower MB/s value than V2 but finishes faster. It reads about one third
as many bytes. MB/s measures transfer rate; it does not measure avoided work.

## Tiny-epoch cross-check

Epoch 0 is complete for all three formats and all outputs match.

| Workload | CAR | Compact V2 | Indexer V3 |
|---|---:|---:|---:|
| Count | 44.80s | 1.39s | 3.16s |
| USDC | 19.38s | 0.60s | 1.98s |
| Pump.fun | 17.31s | 1.67s | 2.62s |
| FireWatch | 17.29s | 0.57s | 0.18s |

The tiny epoch exposes fixed setup cost. V2 starts faster than V3 for the three
full-scan jobs. V3 still wins the indexed FireWatch query.

## Current CAR limits

Epoch 0 passed all four CAR workloads. Epochs 100 and 200 stop at setup because
their raw CAR range indexes omit canonical blocks that have no reconstructed
CAR byte range. The SDK correctly requires an exact canonical-slot plan for
these epochs. The other nine sample indexes have the expected number of rows.

The active CAR reader is mainly single-threaded. V2 and V3 use 12 decode workers.
This is part of the measured reader comparison and must be stated in an article.
The final report must replace the projected CAR figures after the run completes.
