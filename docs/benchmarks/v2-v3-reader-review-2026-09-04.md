# V2 and V3 reader review — 2026-09-04

## Scope and result

This is a code review, not another benchmark pass. It covers the current dirty
worktree on `codex/sample-archive-benchmark`, based on commit
`4bac6a0d5729312c89a1c8148211fcfd42d962ab`.

The native count path removes the transaction and instruction object graph.
The other examples still allocate temporary objects. V3 also has short input
groups that limit read coalescing. There are two count-mode API issues to fix
before a release. The code findings below are from static inspection. Their
individual performance costs have not been measured.

No reader code, CAR code, scheduler, archive, or running job was changed for
this review. No build, test, or extra archive validation was run.

## Existing NAS results

Source: `sample-reader-package-20260904-stream-reuse-final/results` under
`/volume1/blockzilla/benchmark-results`. These are complete V2 local
`slot-hours` runs, with 12 configured workers. Each reported 12 effective
workers and a peak of 12 active workers.

| Epoch | Scan seconds | Source MB/s | Million tx/s | Local read calls | Mean MB/read |
| --- | ---: | ---: | ---: | ---: | ---: |
| 100 | 24.563 | 137.63 | 3.501 | 6,283 | 0.538 |
| 200 | 70.507 | 142.87 | 3.085 | 4,973 | 2.026 |
| 300 | 175.473 | 248.94 | 4.130 | 11,250 | 3.883 |
| 400 | 172.617 | 330.52 | 4.960 | 13,338 | 4.277 |

MB means 1,000,000 bytes. Mean read size is local bytes divided by local read
calls. It is not a disk transfer-size measurement.

Epoch 400 spent 129.519 seconds in the producer's source reads, 43.033 seconds
waiting for a free compressed buffer, and 0.122 seconds in the ordered consumer.
Stages overlap; do not add their times. This shows that the ordered count sink
is not the main cost. It does not prove a fixed disk bandwidth limit. Background
NAS work and cache state differ between jobs. Cross-format parity for this new
build is not yet established by these V2-only results.

The existing V2 epoch 100 FireWatch failure remains:
`inner instruction belongs to outer index 4, after failed outer index 3`.
The count example does not check that same execution-boundary rule. A successful
count run therefore does not prove that FireWatch will accept the archive. Do
not suppress this error or include its partial run as a successful speed result.

## Findings, in proposed fix order

### 1. P1 — The block comparison helper ignores native counts

`crates/blockzilla-query-sdk/src/fingerprint.rs:25` uses
`block.transactions.len()`. A native count block deliberately has an empty
transaction slice, with the real total in `block.counts.transactions`.

Thus `for_each_block_fingerprinted` records zero transactions for a count scan.
Its result differs from a full projection of the same archive. Two count scans
with different transaction totals can also produce the same block comparison
value when their slots and ordinals agree.

Fix: give `BlockView` a count-aware accessor, as `CanonicalBlock` already has,
and use it here. This changes the existing optional block comparison helper;
it does not add a manifest or any archive-file hashing. The current slot-hour
examples do not use this helper and read the correct count fields.

Acceptance, not run: an owned block and a count-only block with the same header
and transaction total must produce the same comparison value. A changed total
must change it. Keep the existing checked conversion to `u32`.

### 2. P2 — The transaction visitor silently accepts count-only requests

`crates/blockzilla-query-sdk/src/source.rs:547` accepts a count-only request,
walks the empty transaction slices, and returns a successful receipt without
calling the transaction visitor. The receipt can report millions of transactions.

Fix: reject this incompatible request before starting I/O. Keep count-only
results on the block visitor. Do not rebuild transaction objects to satisfy a
count request.

Acceptance, not run: count-only plus `for_each_transaction` returns an explicit
invalid-request error, with no scan. Normal transaction visitors remain unchanged.

### 3. P2 — USDC still builds message objects and unfiltered balance lists

The USDC request needs recorded token balances, not instruction or signer
objects. Both adapters still call the general message projection:

- `crates/blockzilla-read-sdk/src/compact_query.rs:934`
- `crates/blockzilla-firebase-indexer/src/indexer_v3_query.rs:2619`

The empty instruction-data selection skips payload reconstruction, but still
retains compact key IDs, outer instruction descriptors, and V0 lookup
descriptors. SmallVec avoids some allocations, not all of them. This is not a
regression to public-key resolution: the unwanted objects contain compact IDs.

Both token-balance projections then allocate complete pre/post lists before
the adapters apply the bound mint-ID filter:
`crates/blockzilla-read-sdk/src/metadata_projection.rs:288`, `:352`, and `:941`.
The new output pool cannot reuse these temporary lists.

Fix: use the count/limits message parser for a balances-only request. Add a
shared token-balance visitor that parses one row at a time, checks the mint ID,
and writes only selected rows to the reused output buffer. Keep this in the
SDK, with no new example flags or parser code in the binaries.

Acceptance, not run: preserve pre/post side, original balance index, amount,
decimals, coverage, and missing-mint rows. Resolve owner/program keys only for
retained rows. Preserve existing schema and bounds checks. After buffer warm-up,
nonmatching valid rows must not require temporary heap lists.

### 4. P2 — V3 read groups stop at four blocks and recreate signature storage

`crates/blockzilla-firebase-indexer/src/indexer_v3_query.rs:71` sets four blocks
per parallel job. At `:1686` each job creates a block-ordinal vector, a result
vector, and a new signature reader. The signature reader starts with no batch.
Its buffer reuse therefore does not extend to the next job.

The semantic workspace and zstd state DO survive between jobs. However, each
contiguous semantic scan is limited to that job's block range. The shared plane
reader cannot combine adjacent ranges beyond four blocks, even when its 32 MiB
stored-byte budget would permit it. It reads the required planes in sequence
within each worker. Small groups add requests on WAN and can reduce sequential
I/O efficiency on disk; the actual cost is not yet measured.

Fix: keep signature storage and job-list storage in worker state. Separate the
bounded I/O window from the bounded output window, or use byte-based input jobs
with smaller ordered output groups. Do not simply increase every buffer limit.
For sparse scans, keep gaps excluded and preserve the reverse-index block filter.

Acceptance, not run: retain exact block order and signature ordinals; no duplicate
plane reads for an admitted range; fewer source requests on a dense scan; bounded
retained memory; safe stop on the first ordered error.

### 5. P2 — V3 rewrites the full input buffer with zeroes before each read

`crates/blockzilla-firebase-indexer/src/bin/archive-v2-account-projection/standalone_v2.rs:3992`
clears the reused vector, reserves space, and resizes it with zeroes before the
source overwrites all bytes. `load_semantic_stored_batch` also clears each plane
before this helper. Capacity reuse avoids allocation but does not avoid this
extra memory write.

Fix: preserve initialized buffer length between reads; extend only when needed,
and truncate to the requested length. Remove the earlier clear for planes that
will be read. Retain exact-length and short-read errors. No unsafe uninitialized
memory is needed.

Acceptance, not run: equal-size repeated reads do not zero the whole destination;
growth and smaller reads remain correct; no stale bytes are exposed on a failure.

### 6. P2 — Pump.fun and FireWatch still allocate temporary CPI/loaded-key lists

`crates/blockzilla-read-sdk/src/metadata_projection.rs:754` allocates a CPI group
vector and an instruction vector per nonempty group. At `:990`, loaded key IDs
also get a new vector. Both V2 and V3 use this projection.

Instruction account/data slices already borrow the input bytes. The remaining
allocation is for the descriptors and key-ID lists. Clearing the final output
pool cannot reuse those temporary vectors. It also drops nested owned
instruction account/data buffers when those fields are requested.

Fix: use a borrowing CPI iterator/visitor and reusable loaded-ID scratch storage.
Keep the existing compact-ID filters. Preserve the metadata-absent, raw, and
unknown-coverage cases. Do not make a skipped key lookup mean a confirmed match
or non-match when the evidence is incomplete.

Acceptance, not run: equal selected output and coverage; no temporary CPI graph
allocation after warm-up; all borrowed references expire before the worker reuses
its input buffer. Do not try to borrow decoded data across the ordered queue
after its source buffer has been reused.

### 7. P2 — Fast count routing is not uniform across V3 entry points

The dense sequential V3 method routes count requests to the native parallel
implementation with one worker. The selected sequential method at
`crates/blockzilla-firebase-indexer/src/indexer_v3_query.rs:1203` does not. It
still projects every transaction and publishes `counts: None`.

Fix: share the native count implementation for selected sequential and parallel
entry points. Preserve the selective receipt and its requested/candidate totals.

Acceptance, not run: the same selected blocks produce the same totals and count
views with one or multiple workers, on local and network sources, without
allocating the transaction graph. The current dense slot-hour benchmark already
uses the fast path.

## Next tuning and measurement

V2 still ties source reads to 64 blocks, 32 MiB of declared decoded bytes, and
65,536 transactions. See `compact_query.rs:69` and the planner in `reader.rs`.
The 0.538 MB mean read at epoch 100 shows that the compressed-byte target is not
the only limit. Review separate I/O and output budgets before changing limits.
Keep the ordered output window bounded; a count scan does not need to retain
all decoded transactions in an input batch.

Also add equivalent coarse stage metrics to V3. In V2, time spent waiting for a
free projection vector and sending a decoded result is not currently reported
as a separate stage (`reader.rs:1636`, `:1735`). Do not add per-transaction clocks.
Output-capacity accounting still walks instruction graphs a second time in both
adapters; account for capacity changes during construction if profiling shows
that walk is costly. Include retained pools in memory reporting.

Recommended sequence: fix count API issues; remove USDC temporary objects;
improve V3 input/signature reuse; replace temporary CPI lists; unify SDK entry
points; then tune input sizes from measured results. Leave CAR until this V2/V3
pass is complete. No speed increase is promised before measurement.
