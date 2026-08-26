# Converter microbenchmark: the bottleneck is the serial commit

Date: 2026-08-14. Measured locally while the NAS job runs, so the epoch-822
production run is still the authority on fleet numbers.

## Fixture

`v2-replicate` builds a synthetic Compact V2 generation by repeating a real
block. Input here is **200 copies of `epoch-822-biggest`: 593,800 transactions,
58.6 MiB of frames** — dense modern blocks, ~2,969 transactions each.

This matters: epoch 2 averages **59 transactions per block**, so it exercises the
tick-driven paths and barely touches the per-transaction ones. This fixture is
the opposite, and is much closer to what the 98 TB actually contains.

Two honest limits. Block contents repeat, so the decoded working set stays
cache-resident in a way a real epoch's would not — absolute throughput is
optimistic. And the replicator stamps synthetic PoH hashes and a synthetic
predecessor tail to satisfy the converter's structural checks, so the fixture is
**valid input and meaningless chain data**; `verify-archive-v2-poh` would
rightly reject it.

Host: 8 logical CPUs (4 performance + 4 efficiency).

## Scaling curve

```text
blockzilla-index-archive-convert <src> <dst> --workers N \
  --pipeline-memory-limit-mib 1024 --epoch 822 --slots-per-epoch 432000 \
  --fixture-source --fixture-message-schema current
```

| workers | wall (s) | CPU (s) | cores used | tx/s |
|---:|---:|---:|---:|---:|
| 1 | 57.46 | 55.73 | **0.97** | 10,334 |
| 2 | 56.32 | 55.83 | **0.99** | 10,543 |
| 4 | 24.67 | 62.36 | 2.53 | 24,070 |
| 6 | 22.43 | 62.00 | **2.76** | 26,473 |
| 8 | 22.64 | 63.06 | 2.79 | 26,228 |
| 12 | 23.77 | 64.35 | 2.71 | 24,981 |

Three things fell out, and one was a bug:

1. **`--workers 2` bought nothing.** 0.99 cores, within noise of one worker.
2. **Scaling saturated at ~2.8 of 8 cores — 35% efficiency.**
3. **`--workers 12` was slower than 6.** Oversubscription on 4P+4E cores.

### After the worker-split fix

`page_workers` reserved half the budget for effect encoding *before* allocating
any block worker, so `--workers 2` produced one page worker and one block worker.
Changed to a third, with none below three workers:

| workers | before (tx/s) | after (tx/s) | change |
|---:|---:|---:|---:|
| 1 | 10,334 | 11,862 | +15% |
| 2 | 10,543 | **23,895** | **+127%** |
| 4 | 24,070 | 26,368 | +10% |
| 6 | 26,473 | 24,721 | −7% |
| 8 | 26,228 | 22,707 | −13% |

Repeat runs put measurement noise at **±8%**, so 4/6/8 are all ~25–26k tx/s and
the apparent regressions at 6 and 8 are noise. The `--workers 2` result is far
outside it. Output was **byte-identical to baseline at every worker count**,
which is the property the converter guarantees and the regression check used
throughout.

The ceiling did not move: ~26k tx/s and ~2.8 cores either way. The split fix
removes a pathology; it does not raise the limit.

## Where the time goes — measured, not proxied

`reconstruct_transaction` → `select_signed_message_candidate` enumerates
candidate instruction-data combinations, serializes the full signed message for
each, and calls `verify` — `signature.verify(signer, message)` at
`main.rs:1510`. That is one Ed25519 verification per candidate per transaction.

The obvious way to size it is to benchmark the primitive. `openssl speed
ed25519` reports **84.7 µs/verify** on this host, which against 96.8 µs per
transaction at one worker suggested verification was ~87% of the work.

**That estimate was wrong, and the method was wrong.** OpenSSL is not the
implementation the converter links, and a primitive benchmark cannot see how
many candidates a real transaction enumerates. The correct measurement is to
run the converter with verification stubbed out and diff the wall clock:

| | with verify | verify stubbed | verification cost |
|---|---:|---:|---:|
| 1 worker | 50.06 s | 28.05 s | **22.0 s — 44%** |
| 4 workers | 22.52 s | 21.34 s | **1.2 s — 5%** |

Two conclusions, both the opposite of the proxy's:

1. **`ed25519-dalek` is roughly twice as fast as OpenSSL here.** Verification is
   44% of single-threaded cost, not 87%.
2. **At 4 workers verification is 5% of wall time.** It parallelises almost
   perfectly across block workers. Everything else does not: non-verification
   work goes from 28.05 s to 21.34 s across four workers, a **1.31× speedup**,
   and it is then **95% of the wall clock**.

**The bottleneck is the non-verification path, and it is serial.** Batch
verification — which the previous revision of this document ranked first — would
save at most ~1.2 s at four workers and is not worth building.

### What is in the serial path

The ordered commit callback runs on the driver thread and does, per block:

```rust
let transaction_page = PreparedPage::compress(
    transactions_codec::encode_block(&transaction_block)?,
)?;
let transaction_span = transactions_plane.push_prepared(transaction_page)?;
```

That is a full Wincode encode of the block's transaction arena **and its zstd
compression**, on the serial thread, before any byte is written. Effect chunks
are already dispatched to the page pool; the transaction arena is not.

Only `push_prepared` genuinely needs to be ordered — it assigns absolute
offsets. Encoding and compressing do not.

The obstacle is that `TransactionBlock` embeds `effect_files: [EffectFileIndex;
6]`, whose `first_chunk_offset` is an absolute file offset known only at commit.
So the whole block cannot be encoded in a worker as the type stands. The bulk —
`transaction_rows`, the concatenated per-transaction arena — carries no offsets
and could be built and compressed in the worker, leaving commit to encode only
the small header and append. That is the change worth costing.

### Fixture note: this is a candidate-heavy workload

Of 1,806,600 instructions, **1,040,600 are rederived typed variants**
(ComputeBudget 736,600, VoteTowerSync 204,600, System 99,400) against 766,000
`Raw`. Epoch 2 was almost entirely `Raw` with 9 rederived instructions in the
whole epoch. Typed variants are what create multiple candidates per instruction,
so this fixture enumerates and verifies considerably more per transaction than
epoch 2 does — and modern epochs look like this fixture, not like epoch 2.

## Where the effort should go

Ranked by the corrected profile, not the proxy:

### 1. Move the transaction arena off the commit thread

The largest identified serial cost. `transaction_rows` carries no absolute
offsets, so building and compressing it in the block worker is possible; commit
would encode only the small header (`effect_states`, `row_restarts`,
`effect_files`) and append. Requires either splitting `encode_block` into
header-and-arena halves or letting a pre-encoded arena be passed through.

### 2. Use the zero-copy decoder that already exists

`decode_source_block` calls
`wincode::config::deserialize_exact::<ArchiveV2HotBlockBlob>`, whose
`message_bytes` and `metadata_bytes` are owned `Vec<u8>`, so the whole block
payload is copied out of the decompressed buffer.
`blockzilla-format` already ships
`deserialize_archive_v2_hot_block_blob_borrowed_current`, documented as
borrowing "its large contiguous regions".

The cost is written into the converter's own memory model — `decoded * 8` bytes
reserved per in-flight block, largely because the decode is owned. A smaller
reservation also lets more blocks be in flight under the same pipeline budget,
so the fix compounds.

### 3. Stop copying instruction bytes that are already resident

`select_signed_message_candidate` returns
`instructions.iter().map(|i| i.data.to_vec()).collect()`, giving
`exact_instruction_data: Vec<Vec<Vec<u8>>>` — one outer `Vec` per transaction,
one inner `Vec` per instruction, each a heap copy of bytes already in the decoded
frame. With a borrowed decode these become offset ranges.

### What is not worth doing

- **Batch Ed25519 verification.** Worth ~1.2 s at four workers. The previous
  revision ranked this first on the strength of the OpenSSL proxy; the direct
  measurement retires it.
- **Iterator-versus-loop and `collect`-hunting in the per-transaction path.**
  Real, but small next to a serial encode-and-compress per block.
- **Per-block scratch buffers in `SourceWorker`.** `decode_source_block`
  allocates twice per *block*, so 200 blocks over four workers is ~400
  allocations for the whole run. Negligible — an earlier draft of this document
  overstated it by conflating per-block with per-transaction cost.

## What this says about the fleet

Dense-block throughput is **26,473 tx/s at 8 workers**, against epoch 2's
39,715 tx/s. Modern epochs are slower per transaction *and* carry far more
transactions per slot, so the epoch-2 rate should not be used for planning in
either direction.

No fleet estimate is offered here. The epoch-822 production run is the number
that counts, and this fixture's cache-resident repetition would flatter it.

## Reproduce

```bash
v2-replicate <single-block-generation> <bench-generation> 200
blockzilla-index-archive-convert <bench-generation> <out> --workers 8 \
  --pipeline-memory-limit-mib 1024 --epoch 822 --slots-per-epoch 432000 \
  --fixture-source --fixture-message-schema current
```
