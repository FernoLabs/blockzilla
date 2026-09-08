# Archive readers: accepted local and network comparison
> **8 September, monitored CPU profiles:** all eight V2/V3 local/network
> diagnostic runs passed exact transaction-identity hashes. Network scan rates
> were 313,762 TPS / 60.24 MB/s for V2 and 253,850 TPS / 24.35 MB/s for V3.
> Both network processes averaged under 0.3 CPU cores out of 12; input waits
> remain the main limit. These hash-and-discard runs are separate from durable
> export and full-epoch results. See the [report and four flame graphs](epoch900-network-profile-20260908.md).


> **8 September, streaming update:** a controlled V3 old/new/new/old prefix
> test passed exact output checks. Default startup now caches the block index
> and streams the directory. Mean total time fell from 63.88 to 31.71 seconds;
> measured memory stayed near 200 MiB. This is a short transaction-export test,
> not the full-epoch workloads below. See the [streaming report](epoch900-network-streaming-20260908.md).

> **8 September, completed short test:** V2/V3 local and network transaction
> exports match for the first 8,192 blocks of epoch 900 (8,925,832 transactions).
> Network scan rates were 433,468 TPS for V2 and 310,940 TPS for V3. V3 setup
> took 62.20 seconds versus 15.47 seconds for V2. This is a different workload
> from the count/CPI baseline below and does not establish a before/after gain.
> See the [short test report](epoch900-network-prefix-20260908.md).

> **8 September, network-reader update:** the input changes and bounded HTTP
> server-error retries pass 394 library tests, 44 benchmark-script tests and
> the Linux build. A new epoch-900 V2/V3 comparison was started under
> `network-window-retry-20260908T070846Z`, then stopped for repeated server
> errors and long input pauses. Its first predecessor stopped on
> HTTP 500 and is preserved as a failed attempt. No new speed gain is claimed
> here until exact candidate acceptance. An independent 7.6 MB HTTP read
> also stalled for 35.29 seconds; normal reads of the same range took
> 0.23–0.40 seconds. All returned identical bytes. Full timing is paused. The tables below remain the accepted
> baseline. See [implementation details](../design/network-reader-window.md).

8 September 2026. **132 local tests and nine network tests passed acceptance.**
The local suite covers 11 epochs, four workloads and three formats. V3 was
fastest in 31 of the 44 local comparisons; V2 was fastest in 13. CAR was not
fastest in this cohort. Two V2 leads were below 1%, so one pass cannot establish
a stable advantage in those cases.

V3 in this report means the frozen standalone Indexer V3 prototype. It does
not mean the canonical Archive V3 converter and reader intended to replace V2.

## Results and scope

| Selected work | Result |
| --- | --- |
| Local: epochs 0, 100, …, 1000; four workloads; V2, V3 and CAR | 132/132 PASS |
| Network: epoch 900; four V2 and four V3 workloads; one CAR count | 9/9 PASS |
| Jetstreamer: 512 canonical blocks, one and 12 workers | Both PASS; 545,576 transactions each |
| Jetstreamer: full epoch 900, 12 workers | FAILED with block-read timeouts; no accepted full-epoch TPS |
| New shared V2/V3 network download window | Design reviewed; not implemented or measured |

Network CAR application tests and other network epochs remain deferred.
No new full benchmark was started to hide the Jetstreamer failure.

## Local transaction rate

Covered TPS is the requested transaction count divided by total reader time,
including setup, scan and output. Index skips count as query coverage. This
metric measures how fast the query covers the epoch; it is not always a full
transaction decoding rate. Tables and plots use absolute TPS, not TPS per GB.

![Local covered TPS](artifacts/all-samples-reader-20260908/local-tps.png)

| Epoch | Workload | V2 TPS | V3 TPS | CAR TPS |
| ---: | --- | ---: | ---: | ---: |
| 0 | Transaction / CPI count | 2,572,956 | 870,839 | 237,235 |
| 0 | USDC balances | 2,384,782 | 865,729 | 251,226 |
| 0 | Pump.fun | 2,174,155 | 761,727 | 257,639 |
| 0 | User-program-index | 2,454,131 | 10,741,387 | 257,431 |
| 100 | Transaction / CPI count | 13,184,083 | 10,850,241 | 343,360 |
| 100 | USDC balances | 10,510,485 | 9,933,748 | 347,810 |
| 100 | Pump.fun | 4,784,366 | 648,086,133 | 351,165 |
| 100 | User-program-index | 4,730,932 | 683,600,631 | 346,812 |
| 200 | Transaction / CPI count | 11,746,177 | 12,774,758 | 947,691 |
| 200 | USDC balances | 9,901,493 | 9,832,434 | 793,852 |
| 200 | Pump.fun | 6,887,468 | 2,297,147,949 | 882,886 |
| 200 | User-program-index | 8,480,759 | 2,492,146,551 | 914,516 |
| 300 | Transaction / CPI count | 10,982,375 | 12,610,012 | 888,859 |
| 300 | USDC balances | 9,346,479 | 9,872,425 | 716,580 |
| 300 | Pump.fun | 6,140,325 | 3,970,577,449 | 786,409 |
| 300 | User-program-index | 6,165,077 | 5,435,510,980 | 833,975 |
| 400 | Transaction / CPI count | 9,265,700 | 11,389,346 | 892,375 |
| 400 | USDC balances | 8,204,885 | 9,761,403 | 575,687 |
| 400 | Pump.fun | 5,745,522 | 6,195,841,465 | 720,836 |
| 400 | User-program-index | 3,917,896 | 8,139,661,389 | 759,652 |
| 500 | Transaction / CPI count | 10,604,948 | 9,814,835 | 933,038 |
| 500 | USDC balances | 9,489,179 | 8,441,201 | 823,805 |
| 500 | Pump.fun | 6,151,555 | 4,797,162,178 | 791,448 |
| 500 | User-program-index | 7,147,100 | 6,280,767,211 | 874,717 |
| 600 | Transaction / CPI count | 6,117,701 | 7,896,630 | 591,475 |
| 600 | USDC balances | 5,693,271 | 6,956,700 | 330,967 |
| 600 | Pump.fun | 3,555,016 | 2,591,133 | 467,767 |
| 600 | User-program-index | 1,500,256 | 2,584,886,990 | 484,422 |
| 700 | Transaction / CPI count | 6,700,355 | 7,972,840 | 565,272 |
| 700 | USDC balances | 5,982,115 | 6,367,503 | 321,616 |
| 700 | Pump.fun | 2,078,918 | 1,794,004 | 390,390 |
| 700 | User-program-index | 1,566,153 | 3,461,965,533 | 419,287 |
| 800 | Transaction / CPI count | 5,617,071 | 7,533,265 | 439,786 |
| 800 | USDC balances | 5,071,994 | 5,689,647 | 210,799 |
| 800 | Pump.fun | 1,844,366 | 1,716,191 | 422,513 |
| 800 | User-program-index | 1,289,368 | 3,269,589,989 | 441,455 |
| 900 | Transaction / CPI count | 5,882,574 | 7,238,017 | 584,874 |
| 900 | USDC balances | 5,309,423 | 6,242,336 | 273,253 |
| 900 | Pump.fun | 1,707,335 | 1,693,006 | 445,707 |
| 900 | User-program-index | 1,514,989 | 1,077,992,566 | 468,289 |
| 1000 | Transaction / CPI count | 4,805,294 | 6,312,823 | 458,881 |
| 1000 | USDC balances | 4,297,824 | 4,955,152 | 197,410 |
| 1000 | Pump.fun | 1,368,149 | 1,216,509 | 336,247 |
| 1000 | User-program-index | 889,910 | 2,455,005,285 | 355,069 |

Epoch 0 USDC and Pump.fun outputs agree but remain incomplete: each reports
1,724,876 indeterminate transactions. Agreement does not recover missing
historical data. The full technical evidence retains coverage and zero values.

## Local read rate

Logical MB/s is bytes supplied by the source to the reader divided by scan
time. For local CAR with outer zstd, it uses the CAR stream after decompression.
It is not SSD throughput. V2 and V3 read compressed format ranges and selected
sidecars. A smaller read rate can accompany a faster query because fewer bytes
are needed. Physical disk traffic, cache bytes and decoded stream bytes must
not be combined.

![Local logical read rate](artifacts/all-samples-reader-20260908/local-logical-mbs.png)

| Epoch | Workload | V2 logical MB/s | V3 logical MB/s | CAR logical MB/s |
| ---: | --- | ---: | ---: | ---: |
| 0 | Transaction / CPI count | 116.86 | 41.87 | 590.72 |
| 0 | USDC balances | 105.91 | 41.10 | 625.49 |
| 0 | Pump.fun | 240.26 | 87.76 | 641.14 |
| 0 | User-program-index | 108.73 | 0.00 | 640.39 |
| 100 | Transaction / CPI count | 520.75 | 169.91 | 251.34 |
| 100 | USDC balances | 414.84 | 144.56 | 254.59 |
| 100 | Pump.fun | 188.43 | 0.00 | 257.05 |
| 100 | User-program-index | 186.26 | 0.00 | 253.86 |
| 200 | Transaction / CPI count | 544.47 | 188.34 | 670.58 |
| 200 | USDC balances | 460.92 | 159.16 | 561.72 |
| 200 | Pump.fun | 319.13 | 0.00 | 624.73 |
| 200 | User-program-index | 393.00 | 0.00 | 647.11 |
| 300 | Transaction / CPI count | 662.22 | 246.81 | 623.47 |
| 300 | USDC balances | 569.14 | 200.96 | 502.63 |
| 300 | Pump.fun | 370.23 | 0.00 | 551.61 |
| 300 | User-program-index | 371.71 | 0.00 | 584.98 |
| 400 | Transaction / CPI count | 617.64 | 284.08 | 688.64 |
| 400 | USDC balances | 550.17 | 229.32 | 444.25 |
| 400 | Pump.fun | 382.96 | 0.00 | 556.26 |
| 400 | User-program-index | 261.12 | 0.00 | 586.22 |
| 500 | Transaction / CPI count | 479.03 | 156.37 | 558.37 |
| 500 | USDC balances | 431.32 | 120.19 | 492.99 |
| 500 | Pump.fun | 277.83 | 0.00 | 473.63 |
| 500 | User-program-index | 322.82 | 0.00 | 523.46 |
| 600 | Transaction / CPI count | 688.83 | 447.87 | 618.73 |
| 600 | USDC balances | 648.29 | 297.63 | 346.22 |
| 600 | Pump.fun | 637.67 | 224.67 | 489.32 |
| 600 | User-program-index | 168.90 | 0.00 | 506.74 |
| 700 | Transaction / CPI count | 682.59 | 367.14 | 517.42 |
| 700 | USDC balances | 627.20 | 2,419.17 | 294.39 |
| 700 | Pump.fun | 348.95 | 197.34 | 357.34 |
| 700 | User-program-index | 159.55 | 0.00 | 383.79 |
| 800 | Transaction / CPI count | 686.05 | 437.04 | 531.76 |
| 800 | USDC balances | 628.41 | 2,651.80 | 254.88 |
| 800 | Pump.fun | 347.80 | 213.33 | 510.87 |
| 800 | User-program-index | 157.48 | 0.00 | 533.77 |
| 900 | Transaction / CPI count | 740.51 | 442.73 | 647.57 |
| 900 | USDC balances | 678.30 | 274.89 | 302.54 |
| 900 | Pump.fun | 331.03 | 216.15 | 493.48 |
| 900 | User-program-index | 190.68 | 3.43 | 518.48 |
| 1000 | Transaction / CPI count | 749.53 | 511.87 | 642.72 |
| 1000 | USDC balances | 679.61 | 4,681.75 | 276.49 |
| 1000 | Pump.fun | 305.76 | 180.20 | 470.95 |
| 1000 | User-program-index | 138.80 | 0.00 | 497.31 |

## Measured winners

Each cell selects the shortest total reader time among all three formats.
The displayed rate is covered TPS. Epoch 200 USDC and epoch 900 Pump.fun have
V2 leads below 1%. These are measured winners for this pass, not stable rankings.

![Measured winner matrix](artifacts/all-samples-reader-20260908/winner-matrix.png)

## Stored size

This is the sum of the inventoried archive files, including sidecars. V3 has
a smaller query-bound subset; using only that subset would understate its
complete storage cost. CAR epoch 300 is raw. Other local CAR samples use
outer zstd, so the epoch-300 size increase includes a representation change.

![Stored archive sizes](artifacts/all-samples-reader-20260908/stored-size.png)

| Epoch | V2 stored GB | V3 stored GB | CAR stored GB |
| ---: | ---: | ---: | ---: |
| 0 | 1.349 | 1.467 | 2.240 |
| 100 | 13.290 | 13.044 | 22.817 |
| 200 | 31.515 | 30.564 | 55.945 |
| 300 | 99.504 | 97.004 | 508.343 |
| 400 | 116.692 | 113.079 | 245.124 |
| 500 | 97.868 | 93.977 | 180.697 |
| 600 | 109.749 | 108.505 | 237.004 |
| 700 | 133.420 | 131.601 | 298.251 |
| 800 | 140.496 | 145.922 | 331.059 |
| 900 | 104.049 | 106.322 | 226.074 |
| 1000 | 139.038 | 140.889 | 299.445 |

The optional [TPS per stored GB appendix](artifacts/all-samples-reader-20260908/optional-tps-per-stored-gb.tsv)
divides covered TPS by the complete stored archive size. It is a secondary
capacity-efficiency view. It inherits index-skip gains and is not decoder speed.

## Epoch 900 network pilot

All nine network outputs matched their accepted local counterparts. The
network inputs used fresh private application caches. Total HTTP MB/s counts
response bodies consumed during setup and scan, including counted partial
retry bodies. It excludes headers and is not sampled host network traffic.

![Network TPS](artifacts/all-samples-reader-20260908/network-tps.png)

| Reader | Workload | Total seconds | Covered TPS |
| --- | --- | ---: | ---: |
| Compact V2 | Transaction / CPI count | 2,515.88 | 189,209 |
| Compact V2 | USDC balances | 2,431.19 | 195,800 |
| Compact V2 | Pump.fun | 2,151.12 | 221,292 |
| Compact V2 | User-program-index | 1,962.24 | 242,594 |
| V3 prototype | Transaction / CPI count | 4,315.46 | 110,307 |
| V3 prototype | USDC balances | 3,545.64 | 134,257 |
| V3 prototype | Pump.fun | 6,605.37 | 72,067 |
| V3 prototype | User-program-index | 14.98 | 31,775,595 |
| CAR | Transaction / CPI count | 2,552.40 | 186,501 |

![Network HTTP rate](artifacts/all-samples-reader-20260908/network-http-mbs.png)

| Reader | Workload | HTTP body GB | Total HTTP MB/s |
| --- | --- | ---: | ---: |
| Compact V2 | Transaction / CPI count | 60.811 | 24.17 |
| Compact V2 | USDC balances | 60.819 | 25.02 |
| Compact V2 | Pump.fun | 93.198 | 43.33 |
| Compact V2 | User-program-index | 60.819 | 30.99 |
| V3 prototype | Transaction / CPI count | 29.143 | 6.75 |
| V3 prototype | USDC balances | 21.019 | 5.93 |
| V3 prototype | Pump.fun | 61.197 | 9.26 |
| V3 prototype | User-program-index | 0.451 | 30.13 |
| CAR | Transaction / CPI count | 527.051 | 206.49 |

The V3 user-program-index network query covers 476,026,811 transactions but
decodes only 11,412 in ten selected blocks. Its scan decode rate is 2,607.55
TPS; its total coverage rate is 31,775,595 TPS. Its setup dominates total time.
V3 Pump.fun selects 426,475 of 431,858 blocks and decodes 470,452,521
transactions. Its scan decode rate is 71,460.88 TPS. These rates have different
numerators and time intervals, so the evidence records each separately.

Network CAR reads raw CAR; local epoch-900 CAR reads outer zstd. Their stored
and transferred sizes differ. The HTTP count needed 527.05 GB including its
index, compared with 226.07 GB stored for the local zstd archive and index.

## Why the formats differ

CAR preserves content-addressed source nodes. A slot index can find the start
of a range, but a broad transaction query still traverses nodes and decodes
source records. Outer zstd reduces storage and transfer size; the local reader
decompresses it while reading. It does not restore the whole raw file first.
This cohort has no paired raw-versus-zstd run for the same epoch, so it cannot
assign a measured speed boost to outer zstd alone.

Compact V2 stores ordered compressed block frames. Compact numeric IDs replace
repeated public keys; shared registries resolve them when required. Sidecars
keep signatures and other data separate. The reader can decode borrowed data
and reuse buffers. The four measured examples scan their requested block
range; filters reduce projected work but do not use a reverse target index
to skip whole V2 blocks.

The V3 prototype separates data into files for messages, outcomes, balances,
inner instructions and other fields. A query reads only the required fields.
Reverse indexes select candidate blocks for Pump.fun and signer-wallet
queries. The reader checks exact matches after selection and retains blocks
with incomplete index coverage. USDC needs balance data; account postings are
not a substitute for a mint index.

At epoch 900, local count took 813.90 seconds with CAR, 80.92 with V2 and
65.77 with V3. Relative to CAR, V2 was 10.06× and V3 was 12.38× faster.
V3 was 1.23× faster than V2 for count and 1.18× for USDC. V2 was slightly
faster for Pump.fun: 278.81 versus 281.17 seconds, a lead of 0.85%.
The large V3 wallet-query gain comes from skipping most blocks; it must not
be described as an equivalent increase in full transaction decode speed.

## Network-reader review

V2 has one input producer while decode work runs in parallel. Its count input
wait was 2,490.031 seconds during a 2,490.509-second scan. Those overlapping
counters show input starvation, but do not isolate HTTP latency from copying
or body transfer. V3 makes small requests tied to four-block decode jobs.
Adjacent reads stop at each job boundary, and each job reads its selected
field files in sequence.

Jetstreamer separates these tasks. Its parallel path keeps long HTTP streams
open after a slot-index seek. Its sequential epoch-start path fills a new
download buffer while the consumer reads the previous one. Our CAR HTTP
reader already uses bounded concurrent ranges and reached 206.49 MB/s in the
accepted pilot. V2 count reached 24.17 MB/s and V3 count 6.75 MB/s. This is
evidence against a fixed 24 MB/s host limit, not proof of a specific request
latency or an attainable speed multiplier.

The next implementation should let the format index plan exact selected
ranges, combine adjacent ranges across decode jobs, and use one bounded
download pool ahead of the decode workers. Compressed buffers can be shared
without a copy per block. The pool must preserve sparse selection, object
validation, total memory limits and ordered output. The current examples
expose existing request/retry/cache counters. No window improvement has been
implemented or measured. See the [review and implementation plan](../design/network-reader-window.md).

## Jetstreamer and failed attempts

The reference used unchanged Jetstreamer firehose 0.7.0, commit
`cffaf3d891b3cbe45a46dd963d6d3571b2aa1a24`, in parallel decoded mode.
Both 512-block checks passed an external canonical block/count plan.
The full run exited with code 1 after 1,549.322 seconds, with 24,415 blocks
and 27,111,371 transaction callbacks before shutdown. Its report records
12 block-read timeouts and is invalid. It contributes no full-epoch TPS.

The strict adapter stops on reported upstream errors. Normal Jetstreamer can
restart a stream after an error. The upstream parallel block-read timeout is
15 seconds; it does not establish the cause of the stall. Periodic upstream
statistics are not final counts. The reference decodes transactions and status,
hashes messages and classifies votes; the SDK count projection performs
different work. It is another CAR reader, not another stored format.

The first local suite stopped on an epoch-100 CAR signer query that decoded
unused contradictory CPI data. V2 already omitted this projection for
non-matching or unsuccessful signer transactions. CAR and V3 were corrected
to follow that request contract, while full-detail validation remained in
place. Two full epoch-100 proofs passed. The selected cohort retains the
44 unchanged V2 cases and uses 44 fresh V3 plus 44 fresh CAR cases. The old
V3/CAR attempts are preserved and excluded.

The first network launch failed before scanning because NAS cache permissions
were too broad. Fresh private result roots fixed that launch condition.
An initial acceptance helper also failed on a missing optional log field;
its corrected check used exact command settings and repeated validation.
The final network acceptance initially received HTTP 403 for a full index
GET; a checked closed range with an explicit user agent succeeded. These
failed checks remain preserved. They are not selected performance results.

## Method, limits and evidence

- Each format/workload/epoch has one selected measured pass. Small leads need repeated trials.
- Twelve processing workers were requested where supported. V2 and V3 report actual worker use; sparse V3 queries can use fewer. CAR overlaps input and projection but does not gain 12 decode workers from that setting.
- No OS or CDN cache flush was performed. Local tests and fresh application-cache network tests do not establish cold-device or cold-origin performance.
- The operator reported NAS CPU compaction work that did not touch the benchmark SSD. Its exact overlap and effect are unknown. No correction factor is applied.
- Accepted parity includes ordered count buckets, exact application bytes, schemas, counts, coverage, source inventory continuity and frozen binary bindings. Equal incomplete outputs do not establish complete historical data.
- The early 05:25:58 UTC snapshot and historical zstd/pipeline measurements remain separate. They were not edited or mixed into this cohort.
- The frozen benchmark IDs still use `firewatch`; current display names and code use user-program-index. Historical IDs are retained in technical evidence.
- The refactor commit `38e1dcaa63345eeaff50233c2b208d0ae4ae4853` and projection repair `f46db7b7284af209a3d96b53b9d37d979e944674` are pushed. Later local naming and diagnostic changes are not part of the frozen timing build.

[All selected metrics and provenance](artifacts/all-samples-reader-20260908/results.json) ·
[Machine-readable table](artifacts/all-samples-reader-20260908/results.tsv) ·
[Winner table](artifacts/all-samples-reader-20260908/winners.tsv) ·
[Short format story](from-car-to-v3-accepted-results-20260908.md) ·
[Network details](network-reader-epoch900-pilot.md)

Acceptance receipts and original attempt paths are under
`target/nas-validation/all-samples-signer-fix-20260907T001310Z/`. The JSON above
records receipt hashes and each selected attempt path. Charts are available as
PNG and SVG in the same artifact directory. No report or blog was pushed.
