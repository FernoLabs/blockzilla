# Compact V2 and Indexer V3: local sample result

Status: complete V2/V3 local-disk comparison, 2026-09-04.

## Result

The test scanned sample epochs 0, 100, 200, ..., 1000 on the 12-thread NAS.
Each time below is the sum of 11 complete epoch jobs. TPS is total source
transactions divided by total time. Network reads are not included.

| Workload | Compact V2 | Indexer V3 | V3 speedup | V2 TPS | V3 TPS |
|---|---:|---:|---:|---:|---:|
| Count transactions and inner instructions | 53m 07s | 22m 29s | **2.36×** | 1.80M | 4.27M |
| Recorded USDC token balances | 57m 10s | 23m 28s | **2.44×** | 1.68M | 4.09M |
| Pump.fun transaction dump | 1h 18m 28s | 34m 45s | **2.26×** | 1.22M | 2.76M |
| FireWatch wallet → program list | 1h 00m 01s | 8.21s | **438.79×** | 1.60M | 701.10M effective |

FireWatch's effective TPS uses all source transactions as the numerator. V3
does not decode all of them: its reverse index proves that almost all blocks
cannot contain the selected wallet. For example, it decoded 10 blocks in
epoch 900 and zero blocks in epoch 1000. Completion time and exact output are
the useful measures for this sparse workload.

## Stored size

| Scope | Compact V2 | Indexer V3 | Difference |
|---|---:|---:|---:|
| All 11 samples | 986.97 GB | 982.37 GB | V3 is 0.47% smaller |
| Epoch 900 | 104.05 GB | 106.32 GB | V3 is 2.18% larger |

V3 is not mainly a smaller format. Its semantic planes let a reader avoid
unused data, and its reverse index can reject blocks before payload decoding.

## Correctness and scope

Application outputs match between V2 and V3. The first V2 run reported three
reader failures. A final-wave channel race caused the epoch 500 and 900
Pump.fun failures; an unnecessarily broad instruction projection caused the
epoch 100 FireWatch failure. The corrected V2 reruns passed and matched V3
byte-for-byte. No archive was rebuilt or repaired.

The active matrix keeps the original failed rows for provenance. This report
replaces only those three rows with results from the separate corrected-reader
validation package. CAR disk tests are still running. Public WAN tests were
deferred and are not part of this report.

These are one-pass measurements on one NAS. The operating-system cache state
was not controlled. Use the results as measured engineering evidence, not as
a hardware-independent throughput guarantee.
