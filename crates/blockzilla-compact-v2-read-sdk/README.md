# Compact V2 read SDK

`blockzilla-compact-v2-read-sdk` is the small entry point for Compact V2. It
supports a strongly bound network object set and an operator-trusted local
object set. Both modes return the same ordered query stream.

```rust,no_run
use std::num::NonZeroU32;
use blockzilla_compact_v2_read_sdk::{
    ArchiveInstructionSourceExt, CompactV2Archive, ScanRequest,
};

let mut archive = CompactV2Archive::open(
    "https://blockzilla.example",
    900,
    "/private/tmp/blockzilla-cache",
)?;
let range = archive.bounded_range(0, NonZeroU32::new(1_024).unwrap())?;
let request = ScanRequest::bounded(range)
    .allow_incomplete_instructions()
    .allow_incomplete_cpi()
    .allow_unknown_execution()
    .without_instruction_data();
let receipt = archive.for_each_block(&request, |_| Ok(()))?;
println!("{} transactions", receipt.transactions);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The application does not build HTTP URLs, discover object names, manage a
cache, or decode Compact V2 wire records.

## Ordered parallel application scans

The SDK has a parallel path for applications that do not request instruction
payload bytes. The USDC, Pump.fun, and FireWatch examples use this path. The
default is the number of logical CPUs available to the process. Set the worker
count explicitly for a reproducible benchmark.

```rust,no_run
use blockzilla_compact_v2_read_sdk::{
    BlockView, CompactV2Archive, CompactV2ParallelScanConfig, FnBlockSink,
    QueryResult, ScanRequest,
};

fn visit(_block: BlockView<'_>) -> QueryResult<()> {
    Ok(())
}

let mut archive = CompactV2Archive::open(
    "https://blockzilla.example",
    900,
    "/private/tmp/blockzilla-cache",
)?;
let request = ScanRequest::all().without_instruction_data();
let mut sink = FnBlockSink::new(visit);
let receipt = archive.scan_ordered_parallel(
    &request,
    &mut sink,
    CompactV2ParallelScanConfig::new(12),
)?;
println!("{} transactions", receipt.scan.transactions);
# Ok::<(), Box<dyn std::error::Error>>(())
```

One producer reads frame-aligned ranges in increasing file order. Up to three
compressed buffers are reused. Worker-owned zstd decoders and decompressed
buffers are also reused. Current-schema transaction rows, message bytes, and
metadata bytes stay borrowed during worker projection. The common query model
owns its canonical transaction vectors, so the final application projection
is not fully zero-copy.

One source batch is bounded to 64 blocks, 65,536 indexed transactions, and 32
MiB of declared decompressed source data. One block larger than the byte limit
can run alone. A block above the transaction limit is rejected. Projection and
ordered delivery run in waves no larger than the requested worker count, so a
12-worker run retains at most 12 block results before delivery. Results remain
in exact block-index order.

The canonical transactions and their nested vectors are owned. Their expanded
memory can be larger than the declared source size. There is no direct
pre-allocation byte permit for that expanded payload. The receipt reports the
measured `max_projected_block_bytes` and `max_projected_batch_bytes` high-water
values. Reusable account-key and selection scratch has a 1 MiB retained-payload
cutoff per scratch buffer; larger capacities are released after the
transaction.

For a full scan that needs public keys, the SDK reads the complete registry
once in 32 MiB windows and shares one immutable `Arc` allocation across all
workers. It also uses this path for a partial scan with at least 1,000,000
requested transactions. The default complete-registry limit is 1 GiB. A small
partial scan, a larger registry, a disabled limit, or an allocation failure
uses the bounded worker-local sparse cache. Network setup has already
persisted the strong-ETag-bound registry, so the same caller-selected memory
policy applies to cached network scans.

The parallel receipt reports `requested_workers`, `effective_workers`, and
`max_active_workers`. Effective workers are distinct private-pool workers that
decoded at least one block. Maximum active workers is the peak number of
simultaneous decode-and-project callbacks. Registry telemetry reports the
selected mode and the one-pass prefetch reads. `resident_bound_bytes` is the
exact shared registry payload in `shared-full` mode. In
`sparse-worker-cache` mode, it is a checked upper bound for retained key bytes,
not an exact allocation measurement.

Use the sequential `scan_ordered` API when a request selects exact instruction
payload bytes. That proof path can load blockhash and vote-hash sidecars. The
SDK keeps one bounded copy of that state instead of one copy per worker.

The high-level `CompactV2Archive` also applies the dense registry policy to
sequential scans. A full scan, or a partial scan with at least 1,000,000
requested transactions, loads `registry.bin` once when its payload fits the
default 1 GiB limit. The reader then resolves public-key IDs from that
immutable image. This avoids repeated registry chunk reads during exact
instruction reconstruction. The prefetch calls and bytes are included in the
normal `ScanReceipt::io` counters. A later scan reuses the resident image and
does not count the original prefetch again.

Use `archive.set_full_registry_limit(0)` to force the bounded sparse cache, or
call `archive.release_full_registry()` to release a resident image. The
lower-level `CompactV2InstructionSource` keeps sparse behavior in its common
trait method for API compatibility. Its
`scan_ordered_with_registry_policy` method enables the same explicit bounded
policy.

## Operator-trusted local mode

Use `CompactV2Archive::open_local` for a reader set in one local or NAS
directory. Supply a `CompactV2LocalDescriptor`. This descriptor has the
cluster, epoch, slot range, and candidate ID. It is an operator input. Its
identity is not derived from file content. The SDK always uses the current
message and metadata schemas.

```rust,no_run
use std::num::NonZeroU32;
use blockzilla_compact_v2_read_sdk::{
    ArchiveInstructionSourceExt, CompactV2Archive, CompactV2LocalDescriptor,
    ScanRequest,
};

let descriptor = CompactV2LocalDescriptor::mainnet(
    900,
    "epoch-900-corrected-v2",
)?;
let mut archive = CompactV2Archive::open_local(
    "/mnt/nas/compact-v2/epoch-900",
    descriptor,
)?;
let range = archive.bounded_range(0, NonZeroU32::new(1_024).unwrap())?;
let request = ScanRequest::bounded(range)
    .allow_incomplete_instructions()
    .allow_incomplete_cpi()
    .allow_unknown_execution()
    .without_instruction_data();
let receipt = archive.for_each_block(&request, |_| Ok(()))?;
archive.verify_local_unchanged()?;
println!("{} transactions", receipt.transactions);
# Ok::<(), Box<dyn std::error::Error>>(())
```

The SDK pins the root directory and each opened regular file. It does not
follow a replacement path during a scan. It reads the real file sizes and
builds a separate in-memory descriptor.

Local setup checks required files, exact sizes, index bounds, contiguous block
ranges, registry shape, metadata totals, optional signature length, and epoch
geometry. It does not hash file content. The operator must change
`candidate_id` when the files are replaced, even if their sizes do not change.

`CompactV2LocalDescriptor::mainnet` selects the one supported current message
and metadata schema. If the current reader rejects a sample, repair or rebuild
the sample before publication. Do not change the reader for that epoch.

The two snippets count rows and do not use instruction payload bytes. They
therefore accept named historical instruction, CPI, and execution coverage
gaps. Remove the three `allow_*` options when the application requires
complete coverage. Use `with_instruction_data_for` when the callback needs
exact instruction bytes.

## Network binding policy

The facade uses a bounded HTTP object-set policy:

- HTTPS is required. Redirects and ambient HTTP proxies are disabled.
- The SDK probes only the fixed Compact V2 object names.
- Every present object must provide an exact length and a strong ETag.
- `CompactV2Archive::open` selects the current message and metadata grammars.
- Payload responses must keep the pinned length and strong ETag.
- The reader performs structural index, offset, footer, registry, and
  signature-length checks.
- The cache stores the block index, metadata, registry, and optional epoch-0
  genesis in a private directory bound to the origin, epoch, and ETag set.
- Large block and signature planes stay as bounded range reads.

The ETag-set ID is SHA-256 over object names, lengths, and server validators.
It is only a compact metadata label for cache and query identity. The SDK does
not read file payloads to make it.

## Run the example

```text
cargo run -p blockzilla-read-compact-v2 -- \
  https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev \
  900 \
  /private/tmp/blockzilla-compact-v2-cache \
  1024
```

Read a local candidate:

```text
cargo run --release --locked -p blockzilla-read-compact-v2 -- \
  local \
  /mnt/nas/compact-v2/epoch-900 \
  900 \
  epoch-900-corrected-v2 \
  1024
```

The example scans at most 1,024 block rows. It prints blocks, transactions,
elapsed seconds, transactions per second, `scan_network_mb_s`,
`scan_aggregate_io_mb_s`, `total_network_mb_s`, and
`total_aggregate_io_mb_s`. Use the `total_*` fields for a format comparison.
They include source admission, cache setup, the scan, and reader shutdown.
For local mode, it also prints `transport_kind=local-directory`,
`candidate_id`, and setup, scan, and total local read calls, bytes, and MB/s.
