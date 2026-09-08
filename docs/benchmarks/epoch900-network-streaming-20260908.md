# V3 streaming startup and memory review

8 September 2026. **PASS:** the new V3 reader streams transaction-directory
data instead of downloading the complete file before a scan. All five test
outputs match the previous verified local reference. The V2 reader was reviewed
but was not changed in this iteration.

## Code changes

- The default `Streaming` cache profile retains only the block index. Directory
  ranges use the existing bounded semantic input groups with the selected
  payload planes. The explicit `Sequential` profile retains the old complete
  directory cache. The reverse-index `Selective` profile is unchanged.
- Shared prefetched input is limited to 64 MiB, down from 128 MiB. A group has
  at most 16 MiB of stored data and 128 adjacent blocks, down from 32 MiB.
- Each processing worker releases semantic workspace above 16 MiB after a job,
  down from 64 MiB. Ordered output, four-block decode jobs, source identity
  checks, sparse selection, and the existing oversized-job path are preserved.

These limits do not cap total process memory. The block index, projected
results, registry data, application-held data, and oversized jobs have separate
storage needs. The canonical output still owns selected transaction fields;
this change does not make the complete SDK path zero-copy.

V2 already streams block payloads with bounded concurrent input. Its startup
registry is a lookup sidecar. Its cache and registry-sharing policy were kept
in this iteration to avoid changing public-key query behavior without a
matching workload test.

## Controlled test

Epoch 900, first 8,192 blocks, 12 processing workers. Each run exports
8,925,832 ordered transaction identities, including primary signatures. It
does not perform the count/CPI, USDC, Pump.fun, or user-program-index workload.

The new local run passed first. The network order was **old → new → new → old**.
Every network run used a new private application cache and output path. One
reader ran at a time. The old binary is the previously tested network candidate,
not the older full-epoch baseline in the main comparison report. No files from
either baseline were replaced.

### Timing and transaction rate

| Run | Setup seconds | Scan seconds | Total seconds | Scan TPS | Total TPS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Old 1 | 28.67 | 25.33 | 54.10 | 352,325 | 165,000 |
| New 1 | 5.21 | 31.42 | 36.68 | 284,116 | 243,319 |
| New 2 | 4.21 | 22.50 | 26.74 | 396,718 | 333,777 |
| Old 2 | 42.73 | 30.91 | 73.67 | 288,758 | 121,153 |

Mean total time fell from **63.88 to 31.71 seconds**, a 50.4% reduction in this
short test. Combined total throughput was **139,717 → 281,458 TPS** (2.01×),
calculated from the total transaction count divided by the sum of run times.
Mean setup time fell from **35.70 to 4.71 seconds**. Scan times overlap; these
two runs per version do not establish a stable scan-only speed increase.

### HTTP bytes and rate

Decimal GB and MB/s measure reader-consumed HTTP body bytes.

| Version | Setup GB per run | Scan GB per run | Total GB per run | Combined scan MB/s | Combined total MB/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Old | 1.449 | 0.832 | 2.281 | 29.58 | 35.70 |
| New | 0.107 | 0.856 | 0.963 | 31.76 | 30.37 |

The old setup downloads the full 1.342 GB transaction directory. The new scan
reads about 24.3 MB of directory data for the selected blocks. This explains
why scan bytes increase slightly while total bytes decrease substantially.
Lower total MB/s with the new reader does not mean a slower query: it avoids
most of the old transfer and completes sooner.

### Memory

Process RSS was sampled every 100 milliseconds. Mean sampled peaks were
**197.3 MiB old and 199.5 MiB new**. The new local run peaked at 181.1 MiB.
Thus **no process-memory reduction was measured**. The smaller buffer limits
reduce permitted retention but were not the main memory cost in this fixture.
The validated block index and owned projection data remain resident costs.
Do not compare these samples with the earlier local run's two-second sampling,
which missed most of those short-lived processes.

## Correctness and source checks

The new local output and all four network outputs have 8,192 blocks,
8,925,832 records, and 714,066,608 output bytes. Independent file hashing after
all timed reads stopped matched the previous local V2/V3 reference:

```text
54531e48c946cb66c0c148d7339fdbb11511cffaaec9bc9c5a33f650a10d3685
```

The first output slot is 388800002; the last is 388808197. Exact record counts,
output lengths and hashes, binary hashes, local source identities, HTTP lengths
and strong ETags passed checks. All reader processes and the log observation
have ended.

143 affected library tests passed, including default cache selection,
dense/sparse output parity, input failure, cancellation, and shared byte-credit
lifetime. The HTTP fixture initially lacked sandbox socket permission; it
passed when rerun with local socket access. The Linux release build passed and
all 639 recorded source hashes stayed unchanged through the build and test run.

## Limits and next work

The gateway log captured an R2 `get_range` on `indexer-v3/900/signatures.bin`
taking **4,640 ms** during Old 1. That call requested 273,600 bytes at offset
45,726,016. This identifies one storage operation that delayed a response.
It does not explain every timing difference. The timer stops when R2 returns
the stream handle, so body-transfer stalls are still outside this measurement.

Fresh application caches do not control CDN or OS caches. There were only two
runs per version, and this change combines cache policy with buffer limits.
No full-epoch speed gain or application-workload speed gain is established.
The new policy also changes when directory requests occur during long scans;
that needs a separate long-scan comparison.

Further review should measure the small signature-range reads and the input
producer's sequential plane reads. They remain possible latency costs. Any
further memory reduction should target measured live allocations rather than
lowering limits and claiming an RSS gain that did not occur.

[Machine-readable results](artifacts/network-streaming-20260908.json) contain
all metrics and receipt hashes. Full receipts, source snapshot, build logs and
test logs are retained locally under
`target/nas-validation/network-streaming-20260908T102107Z/`. The NAS received
only executables, control scripts and hash metadata. No source commit or push
was made for this iteration.
