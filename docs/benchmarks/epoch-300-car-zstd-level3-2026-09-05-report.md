# Epoch 300: raw CAR versus zstd level 3

The same CAR count example passed with both files. The compressed file was 59.41% smaller. The compressed run took 63.91 seconds less time: 7.82% less elapsed time, or 1.085 times the throughput.

## Results

| Metric | Raw CAR | CAR with zstd level 3 |
| --- | ---: | ---: |
| CAR file size, decimal GB | 508.338 | 206.321 |
| Complete example time | 817.005932 s | 753.091564 s |
| Complete example TPS | 887,056 | 962,340 |
| Decoded CAR throughput, MB/s | 622.211 | 675.039 |
| Stored CAR bytes / scan time, MB/s | 622.211 | 273.981 |

The slot index is an additional 5,184,000 bytes in both cases. Compression saved 302,016,749,313 bytes (302.017 GB).

Compression used `zstd -3 -T12` and took 2,641 seconds (44 min 1 sec), including final write flush. Compression time is separate from reader time. The count example finished at 15:57:51 CEST on 2026-09-05.

## Correctness

The comparison returned `PASS` and `MATCH`. Both runs produced:

- 408,989 blocks.
- 724,730,034 transactions.
- 102,252,970 recorded inner instructions.
- Identical count buckets for each fixed 9,000-slot interval.

This is the complete read-and-count example, not an isolated decompression test. The reader binary was unchanged. The 12-thread setting applies to compression; it does not mean the CAR reader has 12 decode workers.

## Conditions and limits

Both input files were on the NAS SSD RAID0 Btrfs volume. Result files were on HDD. No network archive reads were used. The raw result came from the earlier SSD run; these were not simultaneous tests or repeated cold-cache trials.

Compression can leave data in the filesystem cache. No caches were cleared. Thus the 8.5% throughput gain describes this run, not a proven gain for every epoch or storage condition.

The reader's logical byte counters describe decoded CAR bytes. The 675.039 MB/s value is not compressed disk traffic. The 273.981 MB/s value is compressed file size divided by scan time, not a physical disk measurement.

## Recommendation

Use zstd level 3 as the preferred candidate for the full SSD sample copy: this test used much less space and did not make the count example slower. Confirm compressed-file selection and size accounting in the full-run script before starting it. Keep the retained raw CAR files until a separate decision authorizes removal.

No full sample copy or new benchmark was started. Network tests remain stopped. Original CAR files remain unchanged. The stopped level-1 partial output is also retained; it is not a valid archive or part of these results.

## Saved evidence

- [Comparison JSON](epoch-300-car-zstd-level3-2026-09-05-comparison.json)
- [Reader metrics TSV](epoch-300-car-zstd-level3-2026-09-05-summary.tsv)
- [Earlier SSD report](epoch-300-ssd-input-2026-09-05-report.md)

NAS log: `/volume2/blockzilla-bench/control/all-samples-20260905/zstd-level3.log`.

NAS results: `/volume1/blockzilla/benchmark-results/epoch-300-car-zstd-20260905/`.
