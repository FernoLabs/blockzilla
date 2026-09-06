# Epoch 300 rolling-pipeline comparison — 2026-09-06

Status: verified. All 24 prefix cases and nine full scans passed. All seven full
output checks passed, including checked indexed expansion.

## Change under test

The V2 reader now uses fixed private worker threads and a bounded rolling window
instead of waiting for each group of up to four blocks per worker. Reads remain
batched, but workers and ordered output can progress across former group boundaries.
Worker buffers and caller state are reused. Transaction/message/metadata validation,
failed-transaction omission rules and output schemas are unchanged by this update.

The candidate is `epoch300-rolling-pipeline-20260906T181908Z`, built from
`2672d42714da7560672b7a99b420c8315eb733e2` plus the frozen source patch
`664333c67252bd5de1c49ae513df058da0c9cb121d5b093f6a1da3ce7a2c7851`.
Its 1,038-file source manifest was unchanged during the Linux build. All 3,563
workspace tests pass across 131 harnesses, with one existing ignored test. The
new pipeline adds 12 regression tests. Format and patch checks pass.

The controls are the immediately preceding verified V2 build, not a fresh run of
the historical oldReader. Full examples use `epoch300-allocation-review-20260906T161500Z`.
Prefix timing and allocation runs both use the corrected immediate-atomic profiler
from `epoch300-allocation-counter-check-20260906T172000Z`. Candidate and controls use
Rust 1.98.1, Linux musl release builds and `+aes,+sse2`.

## Archive and host preflight

The NAS root was scanned before starting jobs:

- `/volume2/blockzilla-bench/archive/compact-v2/<epoch>`
- `/volume2/blockzilla-bench/archive/indexer-v3/<epoch>`
- `/volume2/blockzilla-bench/archive/car/<epoch>`

All 11 sample epochs are present. The V2 and V3 paths did not need a launcher change.
All 11 V2 epoch 300 file sizes and exact modification timestamps match prior
inventories. The saved registry device, inode, size, modification and change times
also match. New index/metadata SHA256 values are retained in the preflight; there
was no historical full-payload hash to compare. The candidate's 21 package files
were checked on the NAS before launch.

The scan found a separate raw-CAR epoch 300 restoration. It was stopped under the
user's first correction before the next message confirmed that the raw baseline
was intentional. The compressed source and 211,677,609,984-byte partial were
preserved. No CAR restoration runs during V2 timings or output checks. A separate
continuation verifies the saved prefix before appending and does not repeat the
folder merge or alter other epochs. CAR SDK compressed reads already exist;
when both final `.car` and `.car.zst` files exist, local discovery prefers `.car`.

## Method

Prefix results: `epoch300-rolling-prefix-20260906T183306Z`. Twenty-four sequential
processes cover USDC, Pump and indexed USDC, with 1 and 12 workers, one warmup,
three timing iterations and two separate allocation iterations. All 60 records
pass counter, source-read, worker and admission checks. All three semantic oracles
also match the retained corrected-atomic run. Prefix oracles do not compare every
output byte.

Full results: `epoch300-rolling-full-20260906T183521Z`. USDC and Pump each run in
baseline/candidate/candidate/baseline order, then indexed USDC runs once. All scans
use 12 workers. Exact canonical/Pump comparisons and checked indexed expansion run
after all timed scans. No cache flush is performed. Host load, memory, CPU/I/O and
copy/compression processes are sampled; sampled memory peaks can miss short peaks.

Allocation counters record Rust allocator calls and requested bytes, not live heap
size or native zstd allocation. Allocation-mode elapsed time is excluded from speed
results. Worker stage sums measure elapsed callback intervals, not CPU usage. The
retained decode/project wall metric now spans first admission to last completion;
its meaning differs from the old group-time sum. The old projection-buffer wait
now measures admission wait, and result-send wait is zero because slots are reserved.

## Bounded results

| Workload | Workers | Baseline median, s | Candidate median, s | Time change |
|---|---:|---:|---:|---:|
| USDC | 1 | 1.848103 | 1.829454 | -1.01% |
| USDC | 12 | 0.561810 | 0.502432 | -10.57% |
| Pump | 1 | 2.121710 | 2.127845 | +0.29% |
| Pump | 12 | 0.611331 | 0.539715 | -11.71% |
| Indexed USDC | 1 | 1.883930 | 1.861719 | -1.18% |
| Indexed USDC | 12 | 0.567063 | 0.504529 | -11.03% |

At 12 workers, median USDC allocator calls change from 3,323 to 3,633 (+9.33%),
with requested bytes +0.20%. Indexed calls change from 1,539.5 to 1,599 (+3.86%),
with bytes +1.90%. Pump calls change from 439,856.5 to 403,614 (-8.24%), with bytes
-10.00%. This is a speed improvement, not a general reduction in allocator calls
or sampled RSS.

The largest candidate admission peaks across prefix records are 96 blocks,
131,072 transactions and 50,493,562 declared uncompressed bytes with 12 workers.
At one worker the peaks are 8 blocks, 20,034 transactions and 20,914,414 bytes.
These all meet the configured limits. Declared bytes are source geometry, not a
whole-process memory cap.

## Full results

| Workload | Baseline scan repeats, s | Candidate scan repeats, s | Baseline mean, s | Candidate mean, s | Time change |
|---|---:|---:|---:|---:|---:|
| USDC | 87.213 / 90.676 | 79.673 / 79.898 | 88.945 | 79.785 | -10.30% |
| Pump | 133.338 / 134.204 | 120.591 / 121.329 | 133.771 | 120.960 | -9.58% |

USDC repeat spread (max/min - 1) is 3.97% for baseline and 0.28% for candidate.
Pump repeat spread is 0.65% and 0.61%. Full sampled peak RSS is 602,628/604,024 KiB
for baseline USDC versus 597,240/596,624 KiB for candidate; Pump is 374,472/375,576
versus 383,188/377,192 KiB. Memory does not show a general reduction. Across 462 host samples, no copy or
compression process was observed during the timed runs. Sampled process CPU time
per elapsed second rose from about 8.94 to 10.71 cores for USDC, and from 9.36 to
10.82 for Pump. These are measured process intervals, not worker-stage idle estimates.

The indexed confirmation takes 80.676569 seconds of scan time (80.843883 seconds
application total) and has a sampled peak RSS of 601,256 KiB. Its data plus dictionary
uses 2,138,768,342 bytes versus 4,110,523,748 canonical bytes, 47.97% less. This is one
full indexed scan, not a paired estimate of indexed performance change.

All nine runs process 408,989 blocks and 724,730,034 transactions, with 111,179,816
known failed transactions omitted. USDC retains all 30,224,439 balance rows and
complete coverage. Full canonical comparisons pass byte for byte, with SHA256
`c307d97f8999ad4cc7a6ad753007c4d6cf4e5a874cfba5da81bd43700ef1d1a2`.
Checked indexed expansion returns exactly the same bytes and exits successfully.
The indexed data, dictionary and source-sidecar hashes also match the previous
verified indexed output, confirming unchanged stored ID assignment and dictionary
order. All output files remain on the NAS.

Epoch 300 has no matching Pump event rows. Its 44-byte header output is identical
across runs; this workload measures scanning, validation and filtering, rather than
substantial event-output writing.

Against the older historical oldReader totals (not rerun in this experiment),
USDC is now about 4.75% higher and Pump about 3.71% lower. The USDC reference is
the later single-pass retest pair, averaging 76.1920375 seconds. An earlier pair
of the same binary averaged 76.374256 seconds; that variation is not a code change. The fresh paired result
above is the supported estimate of this pipeline update's effect. Prefix one-worker
times remain almost unchanged while twelve-worker times fall, which supports the
scheduling explanation. The update does not remove validation to gain speed.

The intended raw-CAR epoch 300 restoration was continued only after timed scans,
byte checks and independent indexed hash checks finished. Recovery control is
`/volume2/blockzilla-bench/control/car-epoch300-continue-20260906T184423Z`.
It checks the complete 211,677,609,984-byte saved prefix before appending. It decodes
from the start but does not rewrite that prefix. The compressed archive, slot index
and all other epochs remain intact. At report capture, the saved prefix had
passed verification and appending was in progress: the saved receipt recorded
245,126,135,808 of 508,337,873,180 bytes. This receipt does not establish that
the raw baseline had completed; use the recovery completion record for that check.

The accompanying JSON retains individual measurements, verification receipts,
source/build provenance, and the independent prefix review.
