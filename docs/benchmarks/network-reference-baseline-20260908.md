# Network reader baseline — 8 September 2026

> **Follow-up diagnosis:** a detailed mirror run reproduced an incomplete HTTP body.
> A separate allocator test found substantial overhead in our Jetstreamer build.
> See the [diagnosis and limits](network-reader-diagnosis-20260908.md); original baseline measurements remain unchanged.

The accepted Triton run is the Jetstreamer reference for future network-reader
work. The existing normal V2 and V3 network runs are the SDK references. All
three cover the first 8,192 canonical blocks of epoch 900: 8,925,832 transactions,
with 12 workers. These are short-run baselines; they are not full-epoch results.

| Reader and workload | Source | Setup (s) | Scan (s) | Total / firehose call (s) | Total / call TPS |
| --- | --- | ---: | ---: | ---: | ---: |
| V2 transaction identities, hash and discard | Our gateway | 26.642 | 28.448 | 55.090 | 162,022 |
| V3 transaction identities, hash and discard | Our gateway | 4.473 | 35.162 | 39.635 | 225,203 |
| Jetstreamer decoded transactions and metadata | Triton | Not separate | Not separate | 471.002 | 18,951 |

V2 and V3 scan-only rates are 313,762 and 253,850 TPS. Their scan HTTP body rates
are 60.24 and 24.35 MB/s. Jetstreamer has no exact HTTP body-byte counter, so its
host traffic must not be used as an accepted MB/s result.

**Compare each reader with its own baseline.** Jetstreamer decodes more data
and uses a different HTTP source. These rows must not be used to claim a V2/V3
format speedup over Jetstreamer. A direct comparison needs an equivalent decoded
workload and explicit source controls. The earlier incomplete mirror attempt
remains a failed diagnostic, not a speed baseline.

A change must preserve complete ordered output. V2/V3 must match their recorded
identity hash. Jetstreamer must match every expected block and transaction
count. Keep normal speed measurements separate from instrumented diagnostics,
report retries, and repeat affected cases before claiming a stable gain. Do not
expand to a full epoch merely because this short reference passes.

The current V2/V3 target is input scheduling: overlap signature reads and combine
small V3 signature requests within the existing memory limits. The previous
Jetstreamer response-body failure is a separate investigation. It did not recur
in the accepted Triton run, while high system CPU use occurred with both sources.

[Baseline data with source receipt hashes](artifacts/network-reference-baseline-20260908.json).
[Triton reference and limits](jetstreamer-triton-20260908.md).
[V2/V3 normal runs and CPU profiles](epoch900-network-profile-20260908.md).
