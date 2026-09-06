# Reader allocation review — 2026-09-05

Base: `ffffcbe1`, branch `codex/sample-archive-benchmark`.

## Scope and run state

Stopped the NAS CAR example loop (PID 3762892) and its epoch-400 reader
(PID 3762943). No compaction, archive conversion, or upload job was stopped.
The final check also found an older paused CAR reader (690104). Its CAR-only
launchers (688906 and 795234) and the reader were terminated to prevent a restart.
Do not restart the benchmark as part of this patch.

The example bundle contains count/slot-hours, USDC, Pump.fun, and FireWatch
for each of CAR, Compact V2, and Indexer V3: 12 Linux binaries. This is not
a new Jetstreamer comparison.

The rebuilt NAS package is
`/volume1/blockzilla/benchmark-results/blockzilla-reader-review-20260905-final`.
Its source revision and source patch identify the SDK changes used by the binaries.

## Changes

- CAR query projection uses `VersionedTransactionReuse`. Transaction storage
  returns to the workspace after projection, including error returns. The
  outer block transaction vector also returns after the callback.
- CAR has a bounded two-buffer raw-block read-ahead stage. One thread reads
  and parses; the caller projects and publishes in order. Blocks move between
  stages without cloning payloads. Queues close before the thread is joined,
  including partial scans and sink failures. EOF is explicit, so a worker
  disconnect is not accepted as clean EOF. Stream inputs now require `Send`
  for scanning; they do not require `'static`.
- Protobuf CAR count scans visit metadata without building an owned metadata
  graph. They retain instruction geometry checks, loaded-address counts,
  missing-metadata coverage, and the stored-error decoder. Historical bincode
  metadata still uses its existing decoder.
- Other CAR queries materialize only metadata used by their projection.
  They skip logs, lamport balances, rewards, and return data. USDC also avoids
  copying inner instruction payloads. Group geometry is still checked.
- The shared protobuf visitor no longer allocates a temporary instruction
  slice list per CPI group. Two bounded passes over group fields preserve
  protobuf field-order semantics and include empty groups.
- V2/V3 raw instruction candidates borrow their payload with `Cow::Borrowed`.
  The usual single candidate stays inline. Reconstructed candidates remain
  owned. The asynchronous output still owns its required data; it cannot
  borrow a buffer that a worker will reuse.
- The shared output pool retains nested instruction-data and account buffers,
  not only the outer vectors. Retained capacities remain byte- and count-bounded.
- V3 jobs keep block selections as a range or a shared slice plus a range.
  They no longer allocate/copy a block-number vector per job. The selection
  iterator retains its exact length.
- Example output buffers increase to 1 MiB. No new CLI switch, runtime format
  selector, per-transaction logging, or reader logic was added to examples.

## Example checks

All three example packages use workspace path dependencies for their SDKs.
Count requests omit keys, signatures, and instruction payloads. USDC requests
token balances for the selected mint. Pump.fun requests selected program IDs
and signatures. FireWatch filters by signer and does not request instruction
account lists or data. V2/V3 examples retain their parallel SDK calls; V3
sparse workloads retain their reverse-index path. Progress remains per block,
with a log interval of ten seconds. Output is flushed at completion, not per row.

The existing V2 exact-size row iterator was already correct and is unchanged.
The passing V2/V3 tests check that count scans do not load the key registry
and that selected filters bind IDs before projection.

## Validation

- All targets in the three reader example packages compile.
- V2 query: 31 tests pass.
- Shared message projection: 9 tests pass, including borrowed-payload pointer
  identity and inline single-candidate storage.
- Signed-message reconstruction: 6 tests pass.
- V3 query: 46 tests pass, including one/many-worker parity, sparse selection,
  exact output, cancellation, and ordered errors.
- Application workloads: 17 tests pass.
- Nested output buffer reuse: the pointer-reuse test passes.
- Metadata decoder: 15 tests pass.
- CAR query: 16 tests pass, including real CAR blocks from epochs 157 and 822
  with identical full-projection and count-only transaction/instruction counts,
  compressed metadata, malformed groups, partial scans, and sink failures.

Two existing CAR tests fail on both the unchanged base and this patch. One
expects CID-based reordering of out-of-order transaction frames; the ordered
reader rejects these frames. The other expects the old reconstruction error
text. This patch does not restore CID lookup tables to satisfy those tests.

The wider V2 SDK suite has 13 integrity/signature test failures that require a
published manifest on an operator-trusted reader. All 13 also fail on the
unchanged base. They are not hidden or removed by this patch.

## Limits and next measurements

This is not a claim of zero allocation for every scan. Requested owned output,
reconstructed instruction data, historical metadata, and buffer growth can
still allocate. CAR query projection is not a 12-worker transaction decoder;
its new concurrency overlaps raw reading with projection.

The V2 48-block wave barrier at 12 workers and its worker-state locks remain
unchanged. Their performance cost was a review hypothesis, not a measured
bottleneck. A replacement must preserve output-memory bounds, ordered errors,
and cancellation. Measure worker idle time before changing that pipeline.
V3 still allocates a small output-block vector per job.

No full-epoch performance result was produced by these checks. Compare elapsed
time, transaction rate, stored bytes read, decoded bytes, and peak memory on the
same files before claiming a speed improvement. Zstd CAR decoded MB/s is not
physical disk MB/s.
