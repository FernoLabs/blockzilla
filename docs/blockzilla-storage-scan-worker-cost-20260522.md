# Blockzilla Archive Cost and Read Performance Snapshot

Date: 2026-05-22

## Executive Summary

This report uses the public Old Faithful CAR report as the reference corpus. The
comparison is not "raw archive versus a black-box service"; it is a progression
of file formats and cloud read paths over the same historical Solana data.

1. **Old Faithful baseline:** raw `epoch-N.car` files plus external indices, as
   listed by the Yellowstone Faithful CAR report.
2. **Blockzilla v1 / first optimization:** CAR.zst-backed storage and indexing.
   This is the first practical cloud-worker version: keep the Old Faithful data
   model, compress the CAR files, index them, and serve `getBlock` from object
   storage.
3. **Blockzilla Archive V2 / public v1 direction:** Blockzilla's own
   block-oriented archive format. This is the real long-term format: smaller
   than CAR.zst, directly indexed by block, and much faster for scan/index
   workloads.

The core result: for epochs `0-963`, the Old Faithful raw CAR corpus is
**436.57 TB** before indices, and **488.44 TB** including the Old Faithful index
footprint. The CAR.zst corpus is **186.48 TB**. The current Blockzilla hot-block
format projects to roughly **45-48 TB** for the same range based on sampled
hot-zstd/CAR.zst ratios.

At Cloudflare R2 Standard pricing, that is approximately:

| Storage path | Size | R2 Standard / mo | Relative to raw CAR |
|---|---:|---:|---:|
| Old Faithful raw CAR | 436.57 TB | ~$6.55k/mo | 100% |
| Old Faithful raw CAR + OF indices | 488.44 TB | ~$7.33k/mo | 111.9% |
| Blockzilla v1 CAR.zst corpus | 186.48 TB | ~$2.80k/mo | 42.7% |
| Blockzilla Archive V2 hot-zstd estimate | 45.0-47.6 TB | ~$674-713/mo | 10.3-10.9% |

The storage story matters because the read path is serverless and object-backed.
Smaller files mean lower monthly storage cost, fewer bytes to scan, less
range-read waste, and a better cost envelope for public historical
`getBlock`/index APIs.

## Reference Corpus

Reference: Yellowstone Faithful `CAR-REPORT.md`.

For the same epoch range as the measured CAR.zst corpus, `0-963`:

| Metric | Value |
|---|---:|
| Epochs present | 964 |
| Raw CAR total | 436.57 TB |
| Old Faithful index total | 51.87 TB |
| Raw CAR + Old Faithful indices | 488.44 TB |
| R2 Standard for raw CAR | ~$6.55k/mo |
| R2 Standard for raw CAR + indices | ~$7.33k/mo |

The Old Faithful report is the baseline because it is the public archive layout
people already know: `epoch-N.car`, hashes/checks, PoH checks, and associated
indices. Blockzilla should be presented as reducing the cost of keeping and
serving this same historical corpus, not as changing the data source.

## Format Progression

| Stage | Format | Role | Storage shape | Read/index implication |
|---|---|---|---|---|
| Reference | Old Faithful raw CAR | Source of truth | Large raw CAR files plus separate indices | Correct, public, but expensive to store and awkward to scan repeatedly |
| First optimization | CAR.zst + indexes | Blockzilla v1 worker path | Same CAR model, compressed | Proves file-backed cloud `getBlock`; cheaper than raw CAR, but still CAR-shaped |
| Current Blockzilla format | Archive V2 hot blocks + sidecars | Public v1 direction | Block-oriented zstd frames, block index, registries/sidecars | Smaller, direct block lookup, faster scans and indexing |

The naming is slightly confusing internally: the repo calls the custom format
Archive V2, but externally this is the real Blockzilla v1 story. The first worker
path is useful proof that `getBlock` can be served from files in object storage;
the custom format is where the storage and indexing advantage becomes large.

## Storage Comparison

Actual CAR.zst corpus:

| Metric | Value |
|---|---:|
| Epochs present | 964 |
| Range | 0-963 |
| Total CAR.zst | 186.48 TB |
| R2 Standard storage | ~$2.80k/mo |
| Reduction versus raw CAR | ~57.3% smaller |
| Reduction versus raw CAR + OF indices | ~61.8% smaller |

CAR.zst bucket distribution compared to Old Faithful raw CAR:

| Epoch bucket | Epochs | Raw CAR total | CAR.zst total | CAR.zst / raw |
|---|---:|---:|---:|---:|
| 0-99 | 100 | 3.0 TB | 1.2 TB | 40.4% |
| 100-199 | 100 | 13.1 TB | 5.0 TB | 37.8% |
| 200-299 | 100 | 35.5 TB | 14.5 TB | 40.9% |
| 300-399 | 100 | 51.8 TB | 21.4 TB | 41.3% |
| 400-499 | 100 | 52.4 TB | 19.9 TB | 38.1% |
| 500-599 | 100 | 49.3 TB | 20.9 TB | 42.5% |
| 600-699 | 100 | 51.3 TB | 23.0 TB | 44.8% |
| 700-799 | 100 | 72.9 TB | 31.6 TB | 43.4% |
| 800-899 | 100 | 63.2 TB | 28.9 TB | 45.7% |
| 900-963 | 64 | 44.1 TB | 20.1 TB | 45.6% |

Blockzilla Archive V2 hot-zstd estimate using sampled hot-zstd/CAR.zst ratios:

| Basis | Hot-zstd / CAR.zst | Estimated hot-zstd corpus | R2 Standard / mo | Relative to raw CAR |
|---|---:|---:|---:|---:|
| All sampled epochs | 24.1% | 45.0 TB | ~$674/mo | 10.3% |
| Last 5 sampled epochs | 25.2% | 47.0 TB | ~$705/mo | 10.8% |
| Last 3 sampled epochs | 25.5% | 47.6 TB | ~$713/mo | 10.9% |

Use actual corpus totals as the anchor. For future projection, prefer adjacent
epoch deltas or rolling gap windows over mean-per-epoch extrapolation.

## Epoch 900 Scan Comparison

Epoch 900 is useful because it is modern enough to be large, but already measured
across the old and new paths.

| Workload | Reader / Format | Threads | Size Read | Time | Blocks/s | Tx/s | Read MiB/s | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---|
| USDC dump | Old Faithful raw CAR | 1 | 527.0 GB | 1386s | 312 | 343k | 363 | baseline |
| USDC dump | Old Faithful CAR.zst | 1 | 226.1 GB | 1269s | 340 | 375k | 396 | first optimization |
| USDC dump | Blockzilla hot-zstd | 1 | 59.9 GB | 662s | 652 | 719k | 86 compressed / 298 decoded | best sequential |
| USDC dump | Blockzilla hot-zstd | 8 | 59.9 GB | 795s | 543 | 599k | 144 compressed / 496 decoded | parallel two-pass overhead |
| USDC dump | Blockzilla hot-raw | 1 | 206.6 GB | 717s | 603 | 664k | 275 | no decompression |
| USDC dump | Blockzilla hot-raw | 8 | 206.6 GB | 859s | 503 | 554k | 459 | parallel two-pass overhead |

Takeaway: CAR.zst is already better than raw CAR for storage and slightly better
for this scan. The custom Blockzilla hot-zstd format is the large jump:
**59.9 GB read instead of 226.1 GB**, and **719k tx/s instead of 375k tx/s** on
the same single-worker USDC dump path.

Pump.fun fast scanner, epoch 900:

| Workload | Reader / Format | Threads | Scope | Output | Time | Tx/s | Read MiB/s | Matches |
|---|---|---:|---:|---|---:|---:|---:|---:|
| Pump.fun scan | Blockzilla hot-raw | 8 | 100k blocks | no output | 40.5s | 2.77M | 1,164 | 619k tx |
| Pump.fun dump | Blockzilla hot-raw | 8 | 20k blocks | 6.8 MB | 19.4s | 1.12M | 447 | 89.5k tx |

## Jetstreamer Baseline

Epoch 900, slots `388800000-388801999`, program-tracking plugin:

| Threads | Slots | Blocks | Tx | Elapsed | TPS |
|---:|---:|---:|---:|---:|---:|
| 1 | 2,000 | 1,995 | 2.15M | 49.43s | 43.5k |
| 4 | 2,000 | 1,995 | 2.15M | 19.70s | 109.2k |
| 8 | 2,000 | 1,995 | 2.15M | 13.02s | 165.3k |

This comparison is not meant to say Blockzilla replaces Jetstreamer. It shows
why storage layout matters underneath any historical replay or indexing layer: a
compact, block-indexed archive is a better substrate for repeated historical
reads.

## Cloud Worker getBlock Comparison

The current production worker proves the first cloud delivery step: serve Solana
`getBlock` over indexed historical files instead of relying only on a remote RPC
provider. This benchmark compares the deployed Worker path against a Triton RPC
endpoint.

Values are p50 / p90 unless noted.

| Method | Worker R2/OF | Triton RPC | Worker internal profile | Worker/Triton p50 |
|---|---:|---:|---:|---:|
| `getBlockTime` | 1.348s / 2.679s | 0.398s / 0.985s | block path | 3.39x |
| `getBlock:none` | 0.708s / 1.815s | 0.374s / 2.131s | 0.117s / 0.337s | 1.89x |
| `getBlock:signatures` | 0.744s / 2.033s | 0.488s / 1.829s | 0.113s / 0.385s | 1.52x |
| `getBlock:accounts` | 1.580s / 6.406s | 1.031s / 4.789s | 0.111s / 0.314s | 1.53x |
| `getBlock:full` | 1.398s / 14.961s | 0.740s / 9.746s | 0.138s / 0.327s | 1.89x |

Interpretation:

- The Worker is already in the same broad latency class as Triton for many
  calls, while reading from file-backed archive storage.
- The Worker internal profile is often low hundreds of milliseconds; large
  client-observed p90 values are heavily affected by JSON response size,
  buffering, and transfer time.
- This is still the CAR/Old-Faithful-shaped path. Moving the worker to
  Blockzilla's custom format should reduce block lookup work, range-read waste,
  and scan/index cost.

Compression is important for the cloud delivery path. Single `curl` probes
showed:

| Call | Client mode | Total time | Downloaded bytes |
|---|---|---:|---:|
| `getBlock:full`, slot `173004611` | plain curl | 4.746s | 5,097,437 |
| `getBlock:full`, slot `173004611` | `curl --compressed` | 1.884s | 559,297 |
| `getBlock:signatures`, slot `397698816` | plain curl | 1.454s | 165,047 |
| `getBlock:signatures`, slot `397698816` | `curl --compressed` | 1.127s | 121,860 |

The next cloud benchmark should run the same JSON-RPC surface on top of Archive
V2 files. That will make the comparison cleaner: same Worker protocol, same R2
pricing model, but different archive format underneath.

## Per-Request Cost Model

Cloudflare marginal units used here, excluding the base paid plan and included
monthly quotas:

| Component | Price | Per request formula |
|---|---:|---:|
| Worker request | $0.30 / 1M after included requests | $0.00000030 |
| Worker CPU | $0.02 / 1M CPU-ms after included CPU | `cpu_ms * $0.00000002` |
| R2 read op | $0.36 / 1M Class B | `r2_reads * $0.00000036` |
| R2 egress | free | $0 |
| Storage | separate | `monthly_storage_cost / monthly_requests` |

Variable cost, no storage amortization:

| Path | Worker CPU | R2 reads | Cost/request | Cost/1M |
|---|---:|---:|---:|---:|
| Direct public R2 slot bin | 0 ms | 1 | $0.00000036 | $0.36 |
| Worker getSlotBin proxy | ~2 ms | 1 | $0.00000070 | $0.70 |
| JSON-RPC `getBlock:none` | ~10 ms | 2 | $0.00000122 | $1.22 |
| JSON-RPC signatures | ~15 ms | 2 | $0.00000132 | $1.32 |
| JSON-RPC accounts | ~150 ms | 3 | $0.00000438 | $4.38 |
| JSON-RPC full | ~300 ms | 3 | $0.00000738 | $7.38 |
| JSON-RPC full worst + margin | ~600 ms | 3 | $0.00001338 | $13.38 |

With storage amortized assuming hot-zstd around $700/mo:

| Monthly requests | Storage added / request | Storage added / 1M |
|---:|---:|---:|
| 10M | $0.000070 | $70.00 |
| 100M | $0.000007 | $7.00 |
| 1B | $0.0000007 | $0.70 |
| 10B | $0.00000007 | $0.07 |

The important distinction is fixed storage versus variable serving cost. At low
traffic, storage dominates. At high traffic, the compact archive makes storage
amortization small enough that the request path can stay in the single-digit
dollars per million requests for most JSON-RPC modes.

## Practical Conclusion

The story should be presented as a staged proof:

1. **Reference:** Old Faithful raw CAR is the baseline and source of truth.
2. **First Blockzilla step:** CAR.zst plus indexes proves that historical
   `getBlock` can be served from cloud object storage, with lower storage cost
   than raw CAR.
3. **Real Blockzilla format:** Archive V2/custom hot-block files cut the storage
   footprint much further and improve scan/index throughput.
4. **Business implication:** less storage, fewer bytes read, and direct block
   indexing make historical Solana reads cheaper to keep online and faster to
   serve.

The strongest next benchmark is a production Worker running the same `getBlock`
matrix over Archive V2. That gives the cleanest apples-to-apples result: same
API, same Cloudflare worker/R2 environment, Old Faithful-shaped storage versus
Blockzilla-shaped storage.

## Sources

- Old Faithful CAR reference: https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md
- Cloudflare R2 pricing: https://developers.cloudflare.com/r2/pricing/
- Cloudflare Workers pricing: https://developers.cloudflare.com/workers/platform/pricing/
