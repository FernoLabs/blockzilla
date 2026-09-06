# Archive reader formats

Status: implemented reader reference, 2026-08-31.

CAR, Compact V2, and Indexer V3 have different physical layouts. Each format
has a dedicated SDK. Each SDK publishes the ordered block and transaction
views from
[`blockzilla-model`](../../crates/blockzilla-model/README.md).

Use the [sample layout and design guide](archive-sample-layout-and-design.md)
for the complete public object names and the matching local folder layout.

## Reader summary

| Format | Dedicated SDK | Main read model | Network verification |
|---|---|---|---|
| CAR | [`of-car-reader`](../../crates/old-faithful/of-car-reader/Readme.md) | Ordered ranges over one CAR object and its slot-to-offset index | Strong ETags on the sample Worker; explicit `operator-trusted` mode for Old Faithful |
| Compact V2 | [`blockzilla-compact-v2-reader`](../../crates/compact-v2/blockzilla-compact-v2-reader/README.md) | Compressed block rows, control files, and sidecars | `object-set-bound` |
| Indexer V3 | [`blockzilla-archive-v3-reader`](../../crates/archive-v3/blockzilla-archive-v3-reader/README.md) | Transaction directory, semantic planes, and reverse lookup | `object-set-bound` |

The small reference applications are:

- [`read-car`](../../examples/read-car/README.md)
- [`read-compact-v2`](../../examples/read-compact-v2/README.md)
- [`read-indexer-v3`](../../examples/read-archive-v3/README.md)

Each application selects one format at build time. It does not use a large
run-time format switch.

## Common archive geometry

The sample routes use fixed 432,000-slot archive windows. Archive epoch `E`
starts at slot `E * 432_000`. The SDK checks this geometry and keeps produced
blocks in slot order.

## CAR

The CAR reader set has two objects:

```text
epoch-E.car
epoch-E-slot-ranges.raw
```

The 5,184,000-byte index has 432,000 fixed-width rows. Each row has an 8-byte
little-endian CAR offset and a 4-byte little-endian length. The SDK validates
the full index before it schedules CAR ranges.

The SDK hides CAR node traversal, bounded HTTP ranges, and historical CAR
metadata normalization. The application receives the common ordered view.

CAR remains an independent source representation and a useful sequential
reference. A direct slot read uses the slot-to-offset index.

## Compact V2

Compact V2 stores compressed block rows, a direct block index, transaction
metadata, one public-key registry, and fixed sidecars. The SDK owns object
discovery, cache binding, range coalescing, decompression, registry lookup, and
ordered parallel delivery.

The clean sample set uses the current normalized wire grammar. The beginner
examples do not have an old/new metadata option. A local mirror uses
`archive/compact-v2/<epoch>/`.

A full scan can load one bounded registry image and share it between workers.
The reader reuses compressed and decompressed buffers. The callback stays in
canonical block order even when decode work runs in parallel.

Compact V2 is row-oriented. A query can omit signatures or instruction data
that it does not use. A sparse target query must still inspect the applicable
blocks because Compact V2 has no general application reverse index.

## Indexer V3

Indexer V3 separates its block index, transaction directory, messages,
loaded addresses, inner instructions, logs, token balances, lamport balances,
outcomes, and rewards. The SDK reads only the semantic planes selected by the
request.

The adaptive account-posting objects add sound early block selection for a
signer wallet or reached program. The reader keeps positive blocks and blocks
with incomplete coverage. It can skip a block only when the index proves that
the target is absent. The application must still confirm each candidate.

This rule prevents a false negative. It also explains the main V3 speed
design: a sparse job can avoid signature and transaction-payload reads for
most blocks.

A USDC token-balance scan is different. An account posting does not prove a
mint match in token-balance metadata, so this job scans the applicable
token-balance plane.

## Common ordered projection

The three SDKs implement the same `ArchiveInstructionSource` boundary. A scan
can publish:

- source identity and verification level;
- epoch, block ordinal, and slot;
- transactions in canonical transaction-index order;
- primary signatures and required signer keys;
- resolved program and account keys;
- outer and recorded inner instructions in canonical order;
- explicit coverage states; and
- one receipt with logical work and I/O totals.

Borrowed views are valid during the callback. Copy only data that the
application must keep.

Missing data is not empty data. A named incomplete-coverage state is different
from a confirmed empty list. Workload outputs keep this distinction in their
coverage result.

## Source binding

The readers do not require an archive publication manifest, a partial archive
hash, a complete archive hash, or an epoch seal.

For Compact V2 and Indexer V3 network sources, `object-set-bound` means that
the SDK fixes the object names and binds each present object by exact URL,
length, and strong ETag. The CAR sample Worker also requires exact lengths and
strong ETags for the CAR and slot index. Its explicit Old Faithful mode uses
an operator-accepted canonical block plan because that service does not send
strong ETags.

Local readers pin the opened files and check that they did not change before
the result is accepted. Structural checks validate headers, lengths, offsets,
counts, codecs, and cross-file relations.

For the beginner workloads, compare output row and byte counts, then compare
the output files byte-for-byte outside the timed reader run. The coverage
count and digest remain the explicit check for incomplete source data. These
application checks do not change the source binding and are not archive
publication hashes.

## Measure speed correctly

Use total time and total transactions per second for the main comparison.
Also record the exact application output and coverage result before you
compare speed.

Keep these byte values separate:

- complete stored format size;
- logical bytes requested by the SDK;
- HTTP response-body bytes;
- persistent-cache bytes; and
- local read bytes.

A format can finish sooner with a lower MB/s value when it reads fewer bytes.
A warm cache result is not a cold network result.

## Format choice

Use CAR for the independent Old Faithful representation or a sequential
reference.

Use Compact V2 for a compact row-oriented archive and fast ordered scans.

Use Indexer V3 when semantic planes or reverse lookup can remove input before
decode.
