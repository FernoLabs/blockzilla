# Archive formats and the read SDK

Status: implemented product guide, 2026-08-28.

This is the main entry point for Blockzilla archive readers. It explains the
three archive formats, the common ordered scan API, the high-level network
facade, source trust, and the files that each reader uses.

The common API and all three reference adapters are implemented. The wire
references and Rust source remain the authority for exact record layouts.

Do not use a reachable URL as proof that an archive is canonical. Reachability,
format validity, source binding, and publication approval are separate checks.

## Start here

Use these crates by responsibility:

| Crate | Responsibility |
|---|---|
| `blockzilla-archive-sdk` | High-level network and cache setup through `NetworkEpoch`. It returns one runtime source type for all three formats. |
| `blockzilla-query-sdk` | Source-neutral block, transaction, instruction, request, sink, identity, and receipt types. |
| `blockzilla-read-sdk` | Compact Archive V2 generation admission, local and HTTP range sources, caching, and the Compact V2 adapter. |
| `of-car-reader` | Old Faithful CAR decoding and the CAR query adapter. |
| `blockzilla-firebase-indexer` | Indexer V3 wire reader and query adapter. |

For a network application, start with `blockzilla-archive-sdk::NetworkEpoch`.
After source setup, use only `ArchiveInstructionSource` and its common views.
For a local or format-specific tool, construct the applicable adapter directly
and use the same query interface.

## Names and implementation status

Use these names in code, reports, and new documents.

| Canonical name | Short name | Adapter status | `NetworkEpoch` verification |
|---|---|---|---|
| Old Faithful CAR | CAR | Implemented reference adapter | `operator-trusted` |
| Compact Archive V2 | Compact V2 | Implemented supported reader and adapter | `published-manifest` |
| Indexer-first Standalone V3 | Indexer V3 | Implemented candidate reader and adapter | `internal-binding-only` |

These verification values are for the high-level network facade. A direct
Compact V2 adapter over an explicit unpublished fixture can instead be
`operator-trusted`.

“Archive V2,” “Compact V2,” and “Compact Archive V2” refer to the same format
family. Use **Compact Archive V2** on first use and **Compact V2** after that.

Do not use “V3” alone when the meaning is not clear. The repository has used
that label for more than one index design. Use **Indexer-first Standalone V3**
when the distinction is important.

Indexer V3 has an implemented reader and common adapter. Its retained samples
are still `unverified-nonpublishable`. This status is a publication limit, not
an adapter implementation limit.

## Choose a format

| Capability | CAR | Compact V2 | Indexer V3 |
|---|---:|---:|---:|
| Common ordered adapter | Yes | Yes | Yes |
| `NetworkEpoch` source | Yes | Yes | Yes |
| Direct local source | Yes | Yes | Yes |
| Bounded network reads | Concurrent ordered CAR ranges | Block and signature ranges | Semantic-plane ranges |
| Preserve the Old Faithful source graph | Yes | No | No |
| Read one block without a full scan | With an external range plan | Yes | Yes |
| Read selected semantic planes | No | Partial | Yes |
| Resolve compact public-key IDs | Not applicable | Yes | Yes |
| Primary signature | From CAR data | Optional sidecar | Optional retained sidecar |
| Published Blockzilla generation manifest | No | Yes | No |

Choose CAR for an independent external reference and a simple sequential scan.
Choose Compact V2 for the current published Blockzilla generation contract.
Choose Indexer V3 for selective indexer reads, while you keep its candidate
trust state in every result.

## Common logical model

Applications must not depend on physical file order or wire details. Each
adapter publishes:

- source identity and verification level;
- epoch, block ordinal, and slot;
- transactions in canonical `tx_index` order;
- execution status and the failed outer-instruction index, when present;
- primary signature and required signer keys, when available;
- resolved program and account keys for each instruction, in instruction
  account order;
- outer instructions in message order;
- inner instructions after their parent outer instruction, in recorded order;
- exact instruction coordinates;
- exact instruction bytes, or explicit non-exact coverage;
- separate instruction, CPI, and execution coverage states; and
- a `ScanReceipt` with block, transaction, instruction, coverage, and I/O
  counts.

The complete application position combines fields from the block, transaction,
and instruction views:

```text
BlockHeader.block_ordinal
TransactionHeader.tx_index
InstructionCoordinate.order
InstructionCoordinate.outer_index
InstructionCoordinate.inner_index
InstructionCoordinate.stack_height
```

`InstructionCoordinate` contains only the last four fields. A token Batch event
can also have a token-specific `batch_index`; that value is not part of the
common archive instruction coordinate.

The adapter must not change order to match storage order. Compact V2 can store
transaction rows in an order that differs from `tx_index`. The adapter uses
canonical `tx_index` order and keeps signature offsets with the physical row
that supplied them.

### Missing data is data

The API does not convert an unknown fact into an empty fact.

| API value | Meaning |
|---|---|
| `InstructionCoverage::Complete` | The outer instructions and resolved keys are exact. |
| `CpiCoverage::Complete` | The recorded CPI list is exact. |
| `CpiCoverage::NotRecorded` | The source explicitly does not record CPI data. |
| `InstructionDataCoverage::Exact` | The instruction bytes are exact. |
| `InstructionDataCoverage::NotRequested` | The request did not select these bytes. |
| An `Unknown(CoverageReason)` variant | A raw fallback, unavailable field, ambiguity, or another named reason prevents exact instruction, CPI, data, or execution coverage. |
| Scan error | Source bytes, indexes, or references are invalid under the request policy. |

For example, decoded `inner_instructions = None` is not the same as absent
transaction metadata. The first state is exact. The second state has unknown
CPI coverage.

## Common query API

`ArchiveInstructionSource` is the format-neutral source trait. It exposes the
source `identity` and the ordered `scan_ordered` operation.

`scan_ordered` accepts:

- a `ScanRequest`, which sets the block range and coverage policy; and
- a mutable `BlockSink`, which receives one `BlockView` for each block,
  including an empty block.

It returns a `ScanReceipt`. The receipt reports logical work and exact I/O
counters when the adapter can measure them.

`ArchiveInstructionSourceExt` adds the short `for_each_block` and
`for_each_transaction` callbacks. These helpers work with a concrete adapter,
the facade `ArchiveSource`, or `dyn ArchiveInstructionSource`.

This small function shows the base contract:

```rust,ignore
use blockzilla_query_sdk::{
    ArchiveInstructionSource, BlockSink, Result, ScanReceipt, ScanRequest,
};

fn scan(
    source: &mut dyn ArchiveInstructionSource,
    request: &ScanRequest,
    sink: &mut dyn BlockSink,
) -> Result<ScanReceipt> {
    source.scan_ordered(request, sink)
}
```

Use a `BlockSink` for state, batches, and restart rules. Use
`ArchiveInstructionSourceExt` for a small scan. Borrowed views are valid only
during the callback. Copy data that the application must keep.

### Request policy

`ScanRequest::all()` scans the full source. `ScanRequest::bounded(range)` scans
one nonempty range of block ordinals.

The default request:

- accepts a published source or an explicit operator-trusted source;
- requires complete instruction and CPI coverage;
- requires known execution state; and
- requires exact bytes for all instructions.

`with_instruction_data_for` can limit exact instruction bytes to selected
program IDs. `without_instruction_data` requests no instruction bytes.
`allow_incomplete_instruction_data` delivers explicit non-exact coverage
instead of stopping for selected ambiguous or unavailable bytes.

`allow_unverified_source` is required for an `internal-binding-only` Indexer V3
candidate. It changes request policy. It does not change the source identity.

## High-level network facade

`blockzilla-archive-sdk::NetworkEpoch` owns network setup, immutable-object
binding, persistent-cache setup, and the canonical block plan for one epoch.
It first admits the published Compact V2 generation. That generation supplies
the reference epoch geometry and canonical slot plan for the three sources.
Before it returns Indexer V3, the facade checks that every V3 block row is the
same dense prefix of that Compact V2 slot plan. The requested range must fit in
the V3 prefix.

The facade then opens one explicit format with `open_source` or
`open_source_for`. The returned `ArchiveSource` implements
`ArchiveInstructionSource`. Each format can be opened once from one
`NetworkEpoch` value.

Opening `ArchiveFormat::Car` is the explicit operator decision to trust that
CAR object and the Compact V2 slot plan. The resulting identity remains
`operator-trusted`; source selection does not convert the ETag into publication
proof.

The following code uses one application callback for all formats:

```rust,ignore
use std::{num::NonZeroU32, path::Path};

use blockzilla_archive_sdk::{
    ArchiveFormat, ArchiveInstructionSource, ArchiveInstructionSourceExt,
    NetworkEpoch, ScanReceipt, ScanRequest, WORKER_FORMATS,
};

fn scan_network_epoch(
    origin: &str,
    epoch_number: u64,
    cache_root: &Path,
) -> Result<Vec<(ArchiveFormat, ScanReceipt)>, Box<dyn std::error::Error>> {
    let mut epoch = NetworkEpoch::open(origin, epoch_number, cache_root)?;
    let range = epoch.bounded_range(0, NonZeroU32::new(1_024).unwrap())?;
    let mut receipts = Vec::new();

    for format in WORKER_FORMATS {
        let mut source = epoch.open_source_for(format, range)?;
        let identity = source.identity();
        println!(
            "format={} epoch={} blocks={} verification={:?}",
            identity.format,
            identity.epoch,
            identity.block_count,
            identity.verification,
        );

        let request = match format {
            ArchiveFormat::IndexerV3 => {
                ScanRequest::bounded(range).allow_unverified_source()
            }
            _ => ScanRequest::bounded(range),
        };

        let receipt = source.for_each_block(&request, |block| {
            for transaction in block.transaction_views() {
                consume(transaction)?;
            }
            Ok(())
        })?;
        receipts.push((format, receipt));
    }

    Ok(receipts)
}
# fn consume(_: blockzilla_archive_sdk::TransactionView<'_>)
#     -> blockzilla_query_sdk::Result<()> { Ok(()) }
```

The `NetworkEpoch` facade supports `mainnet-beta` and derives the exact first
slot from Solana's warm-up-aware epoch schedule. `NetworkEpochOptions` changes
only the local-fixture transport policy. A custom cluster must use the direct
format adapters and supply its exact first slot there. Set
`allow_insecure_http: true` only for a controlled local fixture.

`ArchiveSource::open_receipt` reports setup work. `ArchiveSource::io_snapshot`
reports a point-in-time transport and cache snapshot. `ArchiveSource::finish_io`
consumes the source, waits for background transport work to stop, and returns
the final counters; benchmarks must use it for totals. `ScanReceipt::io`
reports the I/O that the adapter assigns to the scan. Keep setup time, scan
time, network bytes, and cache bytes separate in a benchmark.

## Trust and publication

Use these levels in this order. A higher level includes the checks below it.

| Level | Meaning |
|---|---|
| Reachable | The object can be read from a path or URL. |
| Size checked | The expected objects have the expected lengths. |
| Structurally checked | Indexes, rows, offsets, counts, and internal relations are valid. |
| Internally bound | Selected planes bind to one candidate identity. |
| Manifest bound | A publication manifest binds all required object hashes and the generation digest. |
| Source authenticated | Accepted source authority binds the manifest. |
| Canonical | Publication policy accepts the source, format, and completeness evidence. |

The current `NetworkEpoch` identities are:

- CAR: `operator-trusted`. The network adapter binds the URL, exact object
  length, strong ETag, and canonical slot-plan digest. An ETag is not a CAR
  content hash or a root-CID proof.
- Compact V2: `published-manifest`. A complete generation manifest binds the
  generation digest, required files, sizes, and schemas.
- Indexer V3: `internal-binding-only`. Required object lengths, strong ETags,
  and internal headers form a stable candidate binding. No publication
  manifest binds this candidate to source authority.

Semantic parity does not increase source trust. Two formats can produce the
same application rows while one source remains a nonpublishable candidate.
A direct Compact V2 fixture can be `operator-trusted`; this does not change the
published network-generation identity above.

## Reader file layout

The lists in this section describe reader inputs. Builder scratch files and
audit reports are not reader inputs unless a format contract names them.

### Old Faithful CAR

The normal source object is:

```text
epoch-E.car
```

An external reader can also use:

```text
epoch-E-slot-ranges.raw
```

The raw range index has one 12-byte row per possible epoch slot: an 8-byte
little-endian offset and a 4-byte little-endian length. A zero length means
that the slot has no indexed CAR range.

The common CAR adapter follows CAR references and reconstructs blocks. It does
not use file order as transaction order. The `NetworkEpoch` path reads the CAR
object through ordered HTTPS ranges and uses the Compact V2 reference slot plan
for the same epoch.

### Compact Archive V2

The required generation objects are:

```text
archive-v2-generation.json
archive-v2-blocks.zstd
archive-v2-blocks.index
archive-v2-meta.wincode
registry.bin
```

The high-level common query path also requires the bound public-key lookup
index:

```text
registry.mphf
```

Optional read sidecars include:

```text
signatures.bin
genesis.bin
blockhash_registry.bin
prev_blockhash_tail.bin
vote_hash_registry.bin
poh.wincode
shredding.wincode
```

A `mainnet-beta` generation for epoch 0, 1, or 2 must bind exactly one admitted
message-schema marker. A later generation can omit both message markers, which
selects the Current grammar. For metadata, a bound legacy marker selects the
Legacy Raw Error grammar; marker absence selects Current Typed Error. The read
SDK selects both grammars during admission and does not change them during a
scan.

The reader checks manifest completion, generation digest, object sizes, block
index bounds, metadata footer totals, registry shape, schemas, and epoch
geometry before it publishes application data.

### Indexer-first Standalone V3

The internally bound ledger has a block index and eleven semantic planes:

```text
archive-v2-standalone-blocks.index
archive-v2-standalone-transaction-directory.wincode
archive-v2-standalone-messages.wincode
archive-v2-standalone-loaded-addresses.wincode
archive-v2-standalone-inner-instructions.wincode
archive-v2-standalone-logs.wincode
archive-v2-standalone-token-balances.wincode
archive-v2-standalone-balances.wincode
archive-v2-standalone-outcomes.wincode
archive-v2-standalone-transaction-rewards.wincode
archive-v2-standalone-raw-metadata-fallbacks.wincode
archive-v2-standalone-block-rewards.wincode
```

The common projection also requires the retained `registry.bin`. These
retained sidecars are optional until exact message proof needs them:

```text
signatures.bin
blockhash_registry.bin
prev_blockhash_tail.bin
vote_hash_registry.bin
```

The production candidate uses the `VarintDelta` transaction-directory
checkpoint codec. The measurement-only fixed-width codec is not accepted by
the common adapter.

The high-level network facade caches the bounded block index and required
`registry.bin`. The transaction directory, optional signatures, and semantic
payload planes can be large, so they remain strongly pinned, uncached range
reads. Before publication to the application, the facade checks that the V3
block rows are a dense prefix of the Compact V2 canonical slot plan.

The optional account posting index has separate control, coverage, and page
objects. It is not required for the ordered instruction stream.

## Network and cache rules

The implemented network readers:

- require HTTPS by default;
- disable redirects and ambient proxy discovery;
- use identity content encoding;
- pin exact object lengths and strong validators;
- send bounded closed range requests;
- check status, `Content-Range`, response length, and validator;
- deliver concurrent CAR ranges in object order;
- limit active and retained response bodies; and
- report network and cache work separately.

Persistent cache entries bind the source identity, epoch, object name, strong
validator, and exact length. A warm cache hit does not prove that a source is
canonical. The reader checks the remote identity again.

Large Indexer V3 sidecars and payload planes are not copied into the persistent
cache. Their bounded range reads remain in the network counters and do not
become cache reads.

## Compatibility rules

- Bind message and metadata grammar during source admission.
- Add an explicit marker or format version when a wire meaning changes.
- Keep unknown semantic variants as exact raw bytes when the contract permits
  it.
- Do not use a permissive fallback to repair a published marker mismatch.
- Keep candidate manifests immutable. Publish a corrected candidate instead
  of changing an old candidate in place.
- Pin the repository revision for all pre-1.0 archives and reports.

## Detailed references

- [Blockzilla query SDK guide](../guides/blockzilla-query-sdk.md)
- [Archive reader format reference](archive-reader-formats.md)
- [Archive Instruction Stream V1](archive-instruction-stream-v1.md)
- [Compact V2 hot-block wire format](archive-v2-hot-block-format.md)
- [Compact V2 read SDK](../../crates/blockzilla-read-sdk/README.md)
- [Old Faithful CAR reader](../../crates/old-faithful/car-reader/Readme.md)
- [Old Faithful slot-range index](../../crates/old-faithful/slot-ranges/README.md)
- [USDC token event ledger V1](usdc-token-event-ledger-v1.md)
- [Query SDK and Jetstreamer benchmark evidence](../benchmarks/query-sdk-vs-jetstreamer.md)

Benchmark results are dated evidence. They are not part of the format
contract. Compare speed only when readers publish the same block and
transaction universe and the same application digest.
