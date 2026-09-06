# SDK stream and allocation pass — 4 September 2026

Scope: SDK code used by the existing dedicated CAR, Compact V2, and Indexer V3
examples. No archive bytes, format encoding, scheduler, or compactor changed.
The user requested a release build and immediate replacement of the benchmark,
without unit tests or a separate validation run. Build success is not a claim
that these changes are validated.

## Implemented

1. **Direct counts in V2 and V3.** `ScanRequest::count_instructions_only()` selects
   a per-block `BlockCounts` result. The shared compact message parser validates
   borrowed bytes without allocating static-key, instruction, or lookup vectors.
   The metadata count parser does not allocate CPI groups, inner instruction
   vectors, or loaded-key vectors. No canonical transaction graph is built.
   It keeps recorded/absent/raw metadata distinctions, instruction coverage,
   source flags, message geometry, and ordered block publication.
2. **Direct CAR counts.** The CAR adapter counts borrowed decoded instructions
   without building a second canonical transaction/instruction graph. The CAR
   wire and protobuf metadata decoders still allocate. CAR is not allocation-free.
3. **Shared output reuse.** `ProjectionPool` retains cleared instruction and
   token-balance vectors for reuse by the same reader worker. The retained payload
   is capped at 8 MiB per pool, with bounded vector counts. V2 returns completed
   blocks to their worker through a per-block queue; V3 extends its existing
   worker-owned recycling. CAR reuses output buffers on its sequential reader.
   Small nested account/data allocations in full-field queries can still occur.
4. **Overlapped V2 pipeline.** The source producer, decode/project stage, and
   ordered sink can run at the same time. Two recycled result vectors bound the
   output window. A decode group has at most four times the worker count, within
   the existing input batch limits. This gives the pool more work to distribute
   across unequal CPU cores and removes the former decode/publish phase barrier.
   The sink remains on the calling thread; its public API does not require Send.
   Closing queues on error releases blocked producers and consumers. Projection
   errors are still delivered in archive order.
5. **Selective V2 signatures.** Pump.fun allows adapters to omit signatures for
   complete non-matches. V2 skips signature reads for such blocks, but retains
   signatures needed by positive matches and incomplete-coverage records. It
   still coalesces requested signature windows. V3 keeps its existing candidate
   block selection and can retain extra signatures in candidate blocks.
6. **Direct signature-buffer reads.** V2 and V3 fill their final signature arrays
   through `read_range_into_slice`, without an intermediate response-to-record
   copy. Signature windows reuse capacity during a scan. Custom RangeSource
   implementations can still use the default allocating fallback; the supplied
   local, metered, and HTTP wrappers forward the reusable read methods.
7. **Stage metrics.** V2 count and workload programs now print input read time,
   input and buffer waits, decode/project wall time, summed worker decode and
   projection time, and ordered-consumer time. The runner records these fields.
   Stages overlap: do not add them to obtain total wall time. Worker sums are not
   wall time. Existing progress, MB/s, TPS, block/s, CPU, memory, and ETA remain.

The count examples keep their 9,000-slot groups and strict slot order. Full
transaction queries remain available. Count-only results explicitly expose
`BlockView.counts`; they do not present an empty transaction vector as a complete
transaction stream. Existing struct-literal callers now set `counts: None`.

## Review limits and remaining work

- This is not a claim of zero allocation for every API. Buffer growth, setup,
  output ownership, and exceptional records still require storage.
- The CAR query adapter still uses its sequential lossless CAR assembler. It
  does not yet use the newer multi-worker compactor CAR path. This remains an
  SDK integration gap and must not be described as fixed by this release.
- Full V2/V3 instruction projections still allocate metadata CPI views and
  large message views. USDC still parses temporary token-balance vectors. The
  new pool removes repeated canonical-output allocation, not every temporary
  allocation. A borrowed metadata iterator is a separate remaining change.
- V3 already overlaps jobs and returns outputs to workers. This pass preserves
  that implementation rather than replacing it with the V2 pipeline.
- Registry IDs are still bound once per epoch. Count scans do not read registry
  keys. Real selected output rows can still require owner or program public keys.
- The existing epoch 100 FireWatch failed-instruction-boundary error is not a
  performance issue and was not suppressed.

No unit tests, smoke run, full-output comparison, or separate archive validation
was run before deployment. The normal benchmark still records reader errors and
its existing count/output parity results. Do not claim new throughput until the
replacement run produces completed records.

## Run replacement

Previous NAS package: `sample-reader-package-20260904-id-filter-final`.
Its runner was PID 1281842. At the preparation check it was on local V2 epoch 300
FireWatch, with 15 of 264 jobs completed. Existing results are preserved.

Replacement package: `sample-reader-package-20260904-stream-reuse-final`.
The release build succeeded and the package was uploaded. Old runner 1281842
received SIGTERM and exited before new runner **1429090** started. Its log is
`/volume1/blockzilla/benchmark-results/sample-reader-package-20260904-stream-reuse-final/runner.log`.
The run uses the same local archive mirror, all 11 epochs, four workloads, and
V2 → V3 → CAR order, for disk and network. V2/V3 use 12 workers. CAR remains
sequential. Logs and results belong to the new package, never the previous one.

## First completed results from the replacement run

The first six jobs passed their normal reader checks. The runner then started
local V2 epoch 100 Pump.fun. No separate test was started to obtain these numbers.

| Local V2 job | Previous scan | New scan | New source MB/s | New transactions/s |
| --- | ---: | ---: | ---: | ---: |
| Epoch 0 count | 3.644594 s | 0.846796 s | 87.44 | 2,036,943 |
| Epoch 100 count | 110.571493 s | 24.563001 s | 137.63 | 3,500,631 |
| Epoch 100 USDC | 23.275361 s | 19.060192 s | 177.44 | 4,511,287 |

Epoch 100 count is 4.50x faster in this run. It reports 23.113145 s of producer
read time and 0.053086 s of ordered-consumer time. Worker decode and projection
sums are 17.077894 s and 39.734591 s. Read time is now important; these overlapping
measurements do not show that the disk has a fixed 138 MB/s limit. Cache state
and concurrent NAS work were not controlled. Cross-format parity is still pending.
