# Archive V2 compactor hot-path benchmark — 2026-09-03

## Last validated result

The latest Archive V2 compactor processed the fixed 1,024-block sample with a
median time of 17.36 seconds when it used three CAR read-ahead buffers (D3).
The original build took 50.76 seconds. The latest median is 65.8% lower and is
a 2.92x speed-up.

The best warm D3 run took 17.31 seconds. It processed about 59.2 blocks/s and
78,300 transactions/s. The median D3 rates were 58.99 blocks/s and 78,048
transactions/s.

The tested binary SHA-256 was
`30b795037861c55a4629665388775bf6e8c74260e8168c0d4505c4a42a1465f7`.
All six balanced D2/D3 runs produced the exact expected counts. All 11 output
files from each run match the original output byte for byte. The archive format
did not change. The output directories contain no forbidden legacy or manifest
files.

These numbers are the validated baseline for the new ordered-reader candidate.
They do not include the final ordered-reader changes described below.

## Ordered-reader candidate

The candidate reader uses the audited physical order of an Old Faithful CAR.
It appends transaction nodes and compact entry summaries in that order. It does
not build transaction, entry, or rewards CID lookup tables. Entry summaries
keep only the PoH values and the transaction count. The terminal block summary
keeps the entry count instead of a vector of entry CIDs.

The reader keeps low-cost shape checks. It compares the terminal entry count
with the collected entry count. It also compares the transaction count that it
accumulated while it read the entries with the collected transaction count.
For normal external rewards, it makes one CID and slot check per block. It does
not make one CID check for each transaction or entry.

Transaction data and transaction metadata must each fit in one frame. The
reader stops with an error if either frame has a continuation. Normal rewards
use the existing rewards buffer through a borrowed byte slice. Rare rewards
continuations are read in their physical sequence, validated in one sequential
pass, and joined in one pooled buffer. This path does not use a CID lookup
table.

The read-ahead stage and the ordered build stage exchange the transaction,
entry, and rewards containers with constant-time container swaps. They do not
drain and rebuild the containers. The block shredding vector also returns to
the reader and is reused for the next block.

### Final candidate results

- Final binary SHA-256:
  `303266fe2084f1151f3b374ec3d7292ec321cb60361787ebdc0284f0eb7e11db`
- Validated worker count: 12. A balanced 8/10/12 test is still needed to select
  the fastest count.

| Decode workers | Hot build time | Blocks/s | Transactions/s | CAR payload rate | Status |
|---:|---:|---:|---:|---:|---|
| 8 | Not run for this candidate | — | — | — | Pending |
| 10 | Not run for this candidate | — | — | — | Pending |
| 12 | 18.06 s | 56.7 | 75,033 | 210.4 MiB/s | Passed; all 11 files match |

The full command took 34 seconds because loading and validating the reused
35.7-million-key registry MPHF took 16.65 seconds before the measured hot
build began.

Full real-epoch order and continuation audit:

- Input and epoch: epoch 1026, `epoch-1026.car`
- CAR file bytes: 849,808,078,118; decoded payload bytes: 809,264,737,030;
  wall time: 3,803.52 seconds
- Counts: 431,824 blocks; 537,619,154 transactions; 542,379,513 entries;
  431,812 rewards nodes; zero standalone DataFrames
- Transaction data continuations: 0
- Transaction metadata continuations: 0
- Rewards continuations: 0
- Physical-order result: all 431,824 block entry, transaction, and rewards
  groups matched; zero mismatches

Candidate output validation:

- Reference output: `opt-30b79503-d3-r3-epoch-1026`
- Candidate output: `ordered-303266fe-w12-epoch-1026`
- Exact comparison: all 11 expected files match byte for byte
- Counts: 1,024 blocks, 1,354,917 transactions, and 1,415,402 signatures in
  both outputs

## Fixed test input

The tests used the same input and options:

- Epoch 1026 CAR on local NAS storage.
- The first 1,024 produced blocks, slots 443232000 through 443233023.
- 1,354,917 transactions and 1,415,402 signatures.
- An existing ordered registry and the epoch-1025 predecessor blockhash.
- Eight transaction decode workers.
- Zstandard level 1.
- Access-index output disabled.

An older full epoch-1026 compaction job was active during the first optimization
tests. This caused read-rate variation. The final D2/D3 comparison used the
balanced order D2, D3, D3, D2, D2, D3. This order reduces, but does not remove,
the effect of changes in NAS load.

## Wall-time results

| Build | Hot time | Time per block | Blocks/s | Transactions/s | Change from original |
|---|---:|---:|---:|---:|---:|
| Original | 50.76 s | 49.57 ms | 20.2 | 26,693 | baseline |
| Deferred statistics and signature arena | 49.09 s | 47.94 ms | 20.9 | 27,601 | 3.3% faster |
| Read-ahead and metadata reuse, best run | 31.32 s | 30.59 ms | 32.7 | 43,260 | 38.3% faster |
| Same code with hard reuse limits, reviewed run | 34.35 s | 33.54 ms | 29.8 | 39,445 | 32.3% faster |
| Direct vote parser and vote-vector reuse | 29.62 s | 28.93 ms | 34.6 | 45,743 | 41.7% faster |
| Latest binary, D2 median | 19.06 s | 18.61 ms | 53.7 | 71,087 | 62.5% faster; 2.66x |
| Latest binary, D3 median | 17.36 s | 16.95 ms | 59.0 | 78,048 | 65.8% faster; 2.92x |
| Latest binary, D3 warm best | 17.31 s | 16.90 ms | 59.2 | 78,300 | 65.9% faster; 2.93x |

The three D2 hot runs took 19.06, 21.61, and 17.90 seconds. Their median was
19.06 seconds. The three D3 hot runs took 17.84, 17.36, and 17.31 seconds.
Their median was 17.36 seconds. D3 was 8.9% faster than D2 by median time, so
three read-ahead buffers are now the default. The environment setting still
permits D2 through D4 for controlled tests.

The earlier read-ahead runs used the same pipeline. Their CAR read time varied
from 22.46 to 27.33 seconds because of concurrent NAS work. The direct-vote run
cut the ordered message-build stage from 1.124 to 0.783 seconds, a 30.3%
reduction.

## Earlier full epoch-1026 completion

An earlier build completed the full epoch-1026 job. This is a completion and
scale reference, not a clean speed result for the latest binary. The job shared
the NAS with other work, including the small optimization runs.

- Wall time: 14,165.06 seconds, or 3 h 56 min 5.06 s.
- Slot range: 443232000 through 443663999.
- Produced blocks: 431,824.
- Transactions: 537,619,154.
- Signatures: 567,378,438.
- Rate: 30.485 blocks/s and 37,953.883 transactions/s.
- Input: 809,247,031,232 bytes at 45.013 MiB/s.
- Block data: 321,806,090,643 uncompressed bytes and 87,397,618,197
  compressed bytes, for a 27.16% ratio.
- Stored result without an access index: about 146.44 GB
  (146,441,667,836 bytes).

This run confirms full-epoch block, transaction, signature, slot, and output
geometry. It does not measure the latest direct-CAR and allocation changes. A
new isolated full-epoch run is still required for that comparison.

## Earlier reviewed per-block work

The values below come from the earlier 34.35-second D2 run. They explain the
pipeline, but they are not timings for the latest binary.

| Work | Total | Average per block | Meaning |
|---|---:|---:|---|
| CAR reader active | 27.326 s | 26.686 ms | Reader-thread scan and node decode |
| Main thread waiting for a ready block | 16.170 s | 15.791 ms | Time with no completed input block available |
| Reader waiting for a recyclable block buffer | 6.618 s | 6.463 ms | Both read-ahead buffers were held by later stages |
| Block record build | 6.313 s | 6.165 ms | Record creation, including parallel transaction work |
| Parallel transaction decode wall time | 4.930 s | 4.814 ms | One ordered Rayon join per block |
| Ordered hot-block build | 11.052 s | 10.793 ms | Message and metadata encoding plus ordered state |
| Message build | 1.124 s | 1.098 ms | Message conversion and typed instruction work |
| Message encoding | 2.495 s | 2.437 ms | Wincode output |
| Metadata encoding | 6.675 s | 6.519 ms | Wincode output |
| Async Zstandard compression | 1.843 s | 1.800 ms | Runs in the output-writer stage |
| Output-writer channel wait | 0.004 s | 0.004 ms | Producer wait; not a limit |
| PoH output | 0.273 s | 0.267 ms | Ordered PoH sidecar output |

These stage times overlap. Do not add them. For example, the reader works on
the next block while the main thread builds the current block, and the writer
compresses an earlier block.

The two reader wait timers also overlap with downstream work and can overlap
with each other. Consumer wait means that the main thread had no completed
input block. It is evidence that input was on the critical path during that
interval. Recycle wait means that all read-ahead block objects were in use by
later stages. It shows bounded-pipeline backpressure. It does not show a lock
or a deadlock. Writer send and recycle waits show whether output is the limit.

## Direct CAR input path

The latest D3 run recorded these CAR input totals:

| Measure | Value |
|---|---:|
| CAR entries | 2,715,402 |
| CAR wire bytes | 2,299,186,889 |
| CAR payload bytes | 2,197,323,237 |
| Direct-buffer entries | 2,714,885 |
| Direct-buffer payload bytes | 2,193,111,594 |
| Scratch entries | 517 |
| Scratch payload bytes | 4,211,643 |
| Direct entry share | 99.98% |
| Direct payload share | 99.81% |

The scanner decodes an entry directly from the input buffer when the complete
payload is contiguous. Only an entry that crosses an input-buffer boundary is
copied to reusable scratch storage. The direct and scratch counts and bytes
must sum to the CAR totals reported by this scan. These counters are internal
reader telemetry. The reader does not validate them against the CAR footer.

## Synchronization model

| Stage | Concurrency | Required ordering |
|---|---|---|
| CAR scan | One read-ahead thread and two to four reusable whole-block buffers; default three | FIFO block order |
| Transaction decode | Eight Rayon workers inside one block | Result collection keeps transaction order |
| Hot-block build | One ordered main thread | Recent blockhashes, vote references, block IDs, PoH, and output offsets |
| Compression and write | One bounded writer thread and two reusable output buffers | FIFO output order |

The registry is immutable during this phase. It has no application mutex. In
the earlier reviewed 1,024-block run, the producer waited only 4 ms for the
writer. The writer was not the limit in that run.

The main required barrier is the per-block transaction join. The next block
cannot use the recent-blockhash and vote state until the current block has
updated that state. A read-ahead thread is safe because CAR scanning does not
change this ordered state. It overlaps input work without changing archive
order.

More read-ahead buffers can keep the stages busy during short bursts. They
cannot make the steady-state rate higher than the slower of the input and
consumer stages. They also retain one more reusable whole-block object. The D3
decision therefore comes from the balanced measurements, not from the queue
depth alone.

The old code also broadcast a statistics closure to every Rayon worker after
every block. Statistics now drain once after the last block. This removes
1,023 broadcasts and 8,184 worker closures from this sample.

## Allocation and buffer reuse

The main gains came from reuse and less copying, not from more decode workers.

- Metadata conversion returns its vectors to the same transaction-position
  slot after ordered serialization.
- Transaction message decoding reuses outer vectors and inner byte buffers.
  Account keys and hashes can borrow contiguous CAR input. Instruction account
  bytes, instruction data, and V0 lookup index bytes use owned pooled buffers.
- Transaction signatures use one reusable block arena. This avoids one decoder
  signature-reference vector and one owned outer signature vector for each
  nonempty transaction, plus one owned byte vector for each signature. For this
  sample, it avoids 4,125,236 old allocation events.
- Compact output reuses its account-key, instruction, and lookup outer vectors.
- The direct vote parser uses a fixed 31-entry stack array and a small lockout
  pool. The old parser created three temporary vectors for each of the 693,872
  compact-vote instructions. The new path removes or reuses about 2,081,616
  allocation events.
- The ordered CAR reader does not create CID tables or CID-reference vectors
  for transactions, entries, or rewards. It keeps transactions in one vector,
  compact entry summaries in one vector, and at most one normal rewards node.
- The reader and builder transfer those whole containers with constant-time
  swaps. There is no per-node move pass between the two stages.
- Normal rewards use their existing buffer through a borrowed byte slice.
  Rare rewards continuations use one sequential validation pass and one pooled
  join buffer. Transaction data and metadata continuations are rejected.
- The terminal block gives its shredding vector back to the reader. The next
  block reuses its allocation.

The validated baseline D3 allocation counters were:

| Counter | Value |
|---|---:|
| Metadata recycle operations | 1,354,917 |
| Metadata reuse slots | 3,060 |
| Metadata discarded slots | 0 |
| Metadata retained capacity | 178,813,631 bytes |
| Largest metadata slot | 163,660 bytes |
| Transaction outer vectors reused | 3,008,608 |
| Transaction outer vectors fresh | 8,672 |
| Transaction inner buffers reused | 7,737,895 |
| Transaction inner buffers fresh | 106,436 |
| Compact-output outer vectors reused | 3,008,608 |
| Compact-output outer vectors fresh | 8,672 |
| Compact-output growth events | 19,691 |
| Compact-output discarded allocations | 0 |
| Transaction workspace retained capacity | 21,649,015 bytes |
| Largest transaction workspace slot | 17,738 bytes |

The validated baseline D3 CAR dataframe pool retained 24,983,680 bytes at the
end of the sample. D2 retained 18,319,360 bytes. These values are from the
baseline reader, not the ordered-reader candidate. Candidate retention must be
measured in the final NAS run.

The enforced retention limits are:

| Area | Limit and action |
|---|---|
| Metadata workspace | 256 KiB per internal vector, 512 KiB per transaction-position slot, and 256 MiB for the full pool |
| Transaction workspace | 16 KiB per internal vector, 64 KiB per transaction-position slot, and 128 MiB for the full pool |
| Signature arena | 1 MiB for counts and 8 MiB for signature bytes |
| Metadata Zstandard output | Reject more than 256 MiB; if retained capacity exceeds 32 MiB, reset it to 128 KiB |
| CAR rewards continuation pool | 32 MiB per buffer and 256 MiB per whole-block pool |
| CAR whole-block scratch | 32 MiB; transaction and entry containers keep at most 65,536 items |
| Hot output vectors | Keep at most 64 MiB; replace a larger vector with an empty zero-capacity vector |
| Sidecar writers | Fixed 16 MiB buffer for each sequential output |
| Vote lockout pool | At most 64 vectors, with at most 32 lockout entries in each retained vector |

For the validated baseline, D3 permitted three CAR dataframe pools, or 768 MiB.
The measured retained total was about 25.0 MB. The ordered-reader candidate
uses this pool only for rare rewards continuations. Its measured value is still
TODO. An oversized hot vector is released; the trim path does not replace it
with another 64 MiB allocation.

In live process sampling, metadata reuse reduced minor page faults from about
61,300/s to 10,900/s and system CPU from about 69% to 15%. These are sampled
values, not isolated laboratory measurements, but they agree with the large
drop in metadata worker time.

## Correctness checks

The comparison covers these files:

- `archive-v2-blocks.zstd`
- `archive-v2-blocks.index`
- `archive-v2-meta.wincode`
- `signatures.bin`
- `vote_hash_registry.bin`
- `poh.wincode`
- `shredding.wincode`
- `registry.bin`
- `registry_counts.bin`
- `registry.mphf`
- `blockhash_registry.bin`

Every file from every one of the six latest D2/D3 runs has the same size and
content as the original build. All runs reported 1,024 blocks, 1,354,917
transactions, and 1,415,402 signatures. No run created a forbidden legacy or
manifest file.

The direct CAR counters also form two exact checks:

- 2,714,885 direct entries plus 517 scratch entries equals 2,715,402 entries.
- 2,193,111,594 direct bytes plus 4,211,643 scratch bytes equals
  2,197,323,237 payload bytes.

A final shape guard rejects a block if its collected transaction-node count
differs from the transaction count in its entries. A second shape guard checks
the collected entry count against the terminal block entry count. These checks
do not validate a CAR footer.

## Remaining work

The ordered-reader candidate must first pass the TODO validation section above.
After that result is recorded, the remaining work must use the same byte-exact
output checks:

- Improve the serial path for small blocks. The parallel path starts at 64
  transactions, so small blocks do not use the worker workspace.
- Reuse more reward conversion storage.
- Make the access-index encoder borrow more transient input data. Access output
  was disabled in this benchmark.
- Run one isolated full epoch-1026 test with the latest binary. This will give a
  valid full-epoch speed and memory result without competing NAS work.
