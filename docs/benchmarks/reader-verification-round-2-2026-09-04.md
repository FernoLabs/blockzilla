# Reader verification, round 2 — 4 September 2026

This review used the active NAS run and existing source code. No reader code,
binary, archive, or running job was changed. No extra benchmark was started.

## Measured result: Compact V2, local epoch 200

| Workload | Previous scan | Current scan | Speed gain | Current source MB/s | Current transactions/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Slot-hour counts | 366.91 s | 206.87 s | 1.77x | 48.69 | 1,051,522 |
| USDC recorded balances | 266.85 s | 69.60 s | 3.83x | 145.35 | 3,125,339 |
| Pump.fun transactions | 462.49 s | 307.19 s | 1.51x | 78.52 | 708,144 |

These are observations, not a controlled A/B test. OS cache state and other NAS
work were not held constant. Both builds requested 12 workers. The rates use
decimal MB and bytes actually read during the scan, not the full archive size.
USDC means the recorded-balance example, not a complete parsed token-event dump.

The count result matches the previous result in all 48 slot-based hour groups:
318,235 blocks, 217,531,687 transactions, and 5,659,148 recorded inner instructions.
The SDK reports 263,388,422 total instructions and no incomplete instruction or
CPI coverage for this count scan. This is old/new V2 parity, not yet full-epoch
cross-format parity for the new build.

USDC reports the same 14,859,371 output rows and 4,264,058 matching transactions
as before. This review compared its counters, not the complete 2 GB output file.
Pump.fun reports zero matches and complete coverage. It does not measure the cost
of writing a positive Pump.fun match.

## Findings

### 1. Count still creates transaction and instruction objects

V2 creates a transaction vector for each block and an instruction vector for each
transaction with instructions. It then walks these objects to measure retained
capacity. Ordered publication validates the objects and counts coverage. The
example walks instructions again to count inner instructions. For epoch 200,
this path builds 263 million instruction records to produce 48 count groups.

V3 also creates instruction vectors. Its output ownership is better: it returns
objects to the worker that created them and reuses outer transaction vectors.
It does not yet have a direct count-only callback.

The first optimization removed public-key expansion from count requests; it did
not remove this object construction. Existing V2 tests make registry reads fail
and still pass with one and twelve workers. V3 has corresponding key-expansion
checks. No instruction payload copy is required by these count requests.

Relevant code:

- `crates/blockzilla-read-sdk/src/compact_query.rs`: parallel projection,
  `project_instructions`, and `canonical_projection_owned_payload_bytes`.
- `crates/blockzilla-query-sdk/src/source.rs`: `OrderedBlockPublisher::publish`.
- `examples/read-compact-v2/src/bin/read-compact-v2-slot-hours.rs`: count sink.
- `crates/blockzilla-firebase-indexer/src/indexer_v3_query.rs`:
  `project_instructions` and `recycle_parallel_transaction_buffer`.

### 2. V2 has a barrier after each group of 12 blocks

`ArchiveReader::process_borrowed_blocks_parallel_ordered` finishes one group of
up to 12 decode/projection tasks, then publishes that group in order. It starts
the next group only after publication completes. V2 also frees projected objects
on the ordered thread. The input producer runs separately and reuses buffers,
but decode/projection does not overlap ordered publication in this loop.

The completed count scan averaged about 500% process CPU: roughly five logical
cores, not twelve fully occupied cores. In a five-second Pump.fun sample, each
decode worker used about 24–29% of one core. At the final sample, all twelve
decode workers were in a futex wait; the ordered thread was in
`folio_wait_bit_common`, a file-page wait. This sample agrees with the code's
serial work and waits, but it does not measure their share of the full scan.

Do not copy this V2 finding to V3: V3 already has queued work and worker-owned
output reclamation. CAR's example adapter remains sequential.

### 3. V2 Pump.fun reads signatures before it knows they are needed

The request asks for primary signatures globally. `ContiguousSignatureScan`
therefore loads all signature windows on the ordered thread, including blocks
with no matches. `load_signature_batch` also copies range-read bytes into a new
signature vector. Those vectors are not recycled between windows.

Epoch 200 count read 10.073 GB. Pump.fun read 24.121 GB and produced zero rows.
Almost all the extra 14.048 GB is signature data; the small remainder includes
filter-key binding. A selected-signature path could avoid this extra I/O for
proven non-matches. It must retain signatures needed by incomplete-coverage
records, not only signatures for positive output rows.

### 4. Detailed timing exists but is not printed

`OrderedParallelBlockStats` already measures producer read time, producer buffer
wait, coordinator input wait, decode/projection wall time, worker decode time,
and worker projection time. `finish_count` and `print_run` omit these fields.
Current logs cannot divide the elapsed time precisely between decompression,
allocation, parsing, publication, and I/O waits. No function-level CPU or heap
profile was collected in this review. Do not give these costs invented percentages.

Progress logging does not walk transaction objects: it uses the block vector
length and prints about once every ten seconds.

## Meaning of the 300–500 MB/s target

The count job reads about 46.3 compressed source bytes per transaction. At that
ratio, 300–500 MB/s requires about 6.48–10.80 million transactions/s, including
instruction parsing. The measured rate is 1.05 million transactions/s. A raw
file-read rate is not the same measurement.

The count job made 4,973 source reads, averaging about 2.03 MB each. It is not
issuing one disk read per transaction. The USDC job reached about 145 MB/s in
both logical reads and sampled physical reads on this same epoch. Thus 49 MB/s
is not an established NAS disk limit. Nor does this prove that disk waits have
no effect, especially when Pump.fun reads a second file on the ordered thread.

## Recommended next changes and acceptance checks

1. Print the existing pipeline timings from the shared example support code.
   Label worker sums separately from wall time. Keep timing out of per-transaction
   loops. Keep the dedicated examples small.
2. Add a direct count scan to each format SDK. Parse borrowed bytes and return
   small per-block counts and coverage, without canonical instruction vectors or
   public-key expansion. Preserve malformed-input errors, raw/absent metadata
   coverage, and strict slot order. Compare every slot-hour group with the current
   baseline, with one and twelve workers where supported, on disk and network.
3. Overlap V2 decode/projection with ordered publication, with bounded queues and
   worker-owned output reuse. Preserve the first error in archive order. Verify
   shutdown, bounded memory, and byte-identical workload output before replacement.
4. Make Pump.fun signature reads selective and reuse read buffers. Verify direct
   matches, CPI-only matches, absent targets, missing metadata, raw fallbacks,
   coverage records, and identical output. Coalesce selected network ranges;
   do not replace sequential windows with one HTTP request per transaction.

Measure each change separately before claiming the 300–500 MB/s target is met.
Do not drop coverage or count fewer records to increase the reported rate.

## Evidence and run state

NAS root: `/volume1/blockzilla/benchmark-results/`.

- Previous: `sample-reader-package-20260904-final/results/`.
- Current: `sample-reader-package-20260904-id-filter-final/results/`.
- Metrics: `summary.tsv` and each job's `stdout.log` and `resources.jsonl`.
- Count comparison: all `approximate_hour=` lines and the total line for epoch 200.

At the last check, runner 1281842 had completed 11 of 264 jobs and was running
`compact-v2/local/epoch-200/firewatch`. Epoch 100 FireWatch still has its known
failed-instruction-boundary error. It is not a valid speed result. The current
full run has not reached V3 or CAR, so this review makes no new full-epoch speed
claim for those formats. No job was stopped or restarted during this review.
