# Archive V3 reader

This crate owns the reader engine and public entry points for one Archive V3
epoch. It hides Worker
routes, HTTP range reads, strong ETag checks, object-set binding, cache
selection, reverse target lookup, local split routing, and the low-level V3
reader.

The engine, adaptive posting reader, candidate selection, and registry lookup
are implemented in this crate. `blockzilla-firebase-indexer` retains its prior
exports for existing consumers. Archive V3 does not depend on that crate.
Shared compact projection and metadata decoding still use the V2 reader and
`blockzilla-user-program-index`. The crate is named
`blockzilla-archive-v3-reader`. Its public Rust types keep the `IndexerV3`
prefix for compatibility.

## Sequential scan

`IndexerV3Archive::open` selects the sequential profile:

```rust,no_run
use std::num::NonZeroU32;

use blockzilla_archive_v3_reader::{
    ArchiveInstructionSourceExt, IndexerV3Archive, ScanRequest,
};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut archive = IndexerV3Archive::open(
    "https://archive.example",
    0,
    "/private/tmp/blockzilla-v3-cache",
)?;
let range = archive.bounded_range(0, NonZeroU32::new(1_024).unwrap())?;
let request = ScanRequest::bounded(range)
    .allow_incomplete_instructions()
    .allow_incomplete_cpi()
    .allow_unknown_execution()
    .without_instruction_data();
let receipt = archive.for_each_block(&request, |_| Ok(()))?;
println!("{} blocks, {} transactions", receipt.blocks, receipt.transactions);
# Ok(())
# }
```

The sequential profile caches the block index and the complete transaction
directory for the selected V3 source. It range-reads signatures, registry
rows, and required semantic payload planes. The cold setup can be large. A
later ordered scan can read the transaction directory from the local cache.

## Parallel scan

Use `scan_ordered_parallel` for an ordered application scan. Use
`scan_target_candidates_parallel` for a targeted scan. The convenience
methods `for_each_reached_program_candidate_block_parallel` and
`for_each_signer_wallet_candidate_block_parallel` use the V3 reverse indexes
before they decode candidate blocks.

The caller supplies a nonzero worker count. `default_worker_count()` selects
the available logical CPU count. The SDK rejects more than 64 workers before
it reads archive data. A benchmark can pass 12 workers explicitly.

`parallel_instruction_source(workers)` returns an adapter for applications
that already use the common `ArchiveInstructionSource` interface. The adapter
binds `identity()` and `scan_ordered()` to the same archive and keeps the last
V3 parallel receipt for measurement. Thus, an application does not need to
copy the common request or checkpoint rules to use the parallel reader.

The output order is the same for one worker and for many workers. A worker
keeps its zstd state, semantic plane buffers, projection scratch, and sparse
registry cache for later jobs. A dense registry image has one shared
allocation. The ordered merge moves each owned canonical block. It does not
make a second copy of all canonical transactions.

This path is not fully zero-copy. The range and decode buffers are reused, but
the canonical query model owns the selected vectors that it gives to the
application. One job has at most four projected blocks. The reader has one
global pending-work bound of the requested worker count multiplied by eight
projected blocks. At 12 workers, this is 96 blocks. It also has a 256 MiB
declared decoded-payload budget and a 100,000-transaction budget across jobs
that execute or wait for ordered publication. A valid block that is larger
than one budget runs alone. Each worker releases semantic buffers when their
retained capacity is more than 64 MiB after a job. It also releases projection
scratch above 8 MiB and does not recycle an outer transaction vector above
4 MiB. These bounds do not include data that the application keeps after its
callback returns.

The decoded-byte and transaction gates are structural admission bounds. They
are not a hard byte limit on the expanded canonical output. The receipt gives
the exact observed owned canonical payload capacity for the largest block and
for the complete active and ordered-result window. Use these high-water marks
to size a production host from a representative scan.

Parallel scans support exact selected instruction payload bytes. The SDK reads
signature data in bounded block windows when an ambiguous payload needs proof.
Vote-hash and blockhash proof sidecars are loaded only when they are needed.
All workers then share the same immutable sidecar allocation, so worker count
does not multiply this memory or source-I/O cost.

The parallel receipt reports the requested worker count, the effective worker
count, and the measured maximum number of worker jobs that were active at the
same time. Active work includes source waits; this value does not claim that
all workers executed CPU instructions at the same instant. The receipt also
reports job count, projected block count, and the ordered-window high-water
marks. These values show what a benchmark run used; they are not inferred from
the command line.

## Selective target scan

`IndexerV3Archive::open_selective` selects the selective profile. It caches:

- the block index;
- `registry.mphf`;
- the adaptive reverse control object; and
- the adaptive incomplete-coverage object.

It does not download the complete transaction directory. It range-reads only
the needed posting pages, transaction-directory rows, signatures, and
semantic payloads. It uses a small `registry.bin` chunk cache for sparse
queries. For a dense query, it can load the complete bounded registry once.

This example makes a reached-program query and confirms each exact match:

```rust,no_run
use std::num::NonZeroU32;

use blockzilla_archive_v3_reader::{IndexerV3Archive, ScanRequest};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let program_id = [7_u8; 32];
let mut archive = IndexerV3Archive::open_selective(
    "https://archive.example",
    0,
    "/private/tmp/blockzilla-v3-selective-cache",
)?;
let range = archive.bounded_range(0, NonZeroU32::new(1_024).unwrap())?;
let request = ScanRequest::bounded(range)
    .allow_incomplete_instructions()
    .allow_incomplete_cpi()
    .allow_unknown_execution()
    .without_instruction_data();
let mut matched_transactions = 0_u64;
let receipt = archive.for_each_reached_program_candidate_block(
    &program_id,
    &request,
    |block| {
        for transaction in block.transactions {
            if transaction
                .instructions
                .iter()
                .any(|instruction| instruction.program_id == program_id)
            {
                matched_transactions += 1;
            }
        }
        Ok(())
    },
)?;
println!(
    "matches={} candidates={} skipped={}",
    matched_transactions,
    receipt.scan.candidate_blocks,
    receipt.scan.skipped_blocks,
);
# Ok(())
# }
```

Use these high-level methods:

- `for_each_signer_wallet_candidate_block` for a query where the wallet must
  be a required signer;
- `for_each_reached_program_candidate_block` for top-level or CPI program
  reach; and
- `scan_target_candidates` when the application must select the target policy
  directly.

Use the corresponding `*_parallel` methods when the application must use more
than one worker and keep the same deterministic output order.

These methods return `IndexerV3TargetedScanReceipt`. The receipt separates the
full requested block and transaction universe from the candidate, skipped,
posting-read, and fallback work.

The SDK selects the registry strategy. A query is dense only when it has at
least 1,000,000 candidate transactions and the candidates contain at least
half of all requested transactions. If the complete registry is no larger
than the default 1 GiB memory limit, the SDK loads it with reads of at most
32 MiB and then uses direct in-memory lookup. A sparse query keeps the bounded
eight-chunk cache. Applications do not need to select a strategy.

Use `set_full_registry_limit(0)` when the host must disable complete-registry
loading. Lowering the limit immediately releases a retained complete registry
when that image is larger than the new limit. `release_full_registry()`
releases it without changing the future policy. Another byte limit changes
the memory gate but does not change the density gates. The scan receipt reports
the selected mode, prefetch calls and bytes, lookup counts, cache hits and
misses, evictions, and resident bytes.

The callback receives candidate blocks only. A candidate is not a final match.
The application must confirm the exact signer or program in the decoded
transactions. If the request permits incomplete coverage, an unconfirmed
fallback transaction stays indeterminate. It is not a negative match.

The snippets explicitly accept historical instruction, CPI, and execution
coverage gaps because they are small count examples. They also state that
instruction bytes are not needed. Omit the three `allow_*` options when the
application requires complete coverage. Select exact instruction data with
`with_instruction_data_for` when the application uses those bytes.

Primary transaction signatures are included by default. Add
`without_primary_signatures()` only when the callback does not use them. V3
then skips the signature sidecar unless selected instruction data still needs
signature proof. The USDC and FireWatch examples use this option. The Pump.fun
example keeps primary signatures because it writes them to its output.

## Local public-tree copy

`IndexerV3Archive::open_local` reads the same flat layout as the public
archive:

```text
https://archive.example/indexer-v3/900/<V3 object>
archive/indexer-v3/900/<V3 objects>
```

```rust,no_run
use blockzilla_archive_v3_reader::IndexerV3Archive;

let archive = IndexerV3Archive::open_local("archive", 900)?;
# Ok::<(), blockzilla_archive_v3_reader::Error>(())
```

The V3 file header records the encoded message and metadata schema. The reader
uses those fields for every published sample, so the caller does not supply a
schema option. The same open call reads every sample epoch.

## Local split candidate

`IndexerV3Archive::open_local_split` reads one candidate whose V3 ledger and
adaptive files are in one local directory and whose retained sidecars are in
another local directory:

```rust,no_run
use std::num::NonZeroU32;

use blockzilla_archive_v3_reader::{
    ArchiveInstructionSourceExt, IndexerV3Archive, ScanRequest,
};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let mut archive = IndexerV3Archive::open_local_split(
    "/archive/epoch-900-v3",
    "/archive/epoch-900-retained-sidecars",
    900,
    "epoch-900-local-candidate",
)?;
let range = archive.bounded_range(0, NonZeroU32::new(1_024).unwrap())?;
let request = ScanRequest::bounded(range)
    .allow_incomplete_instructions()
    .allow_incomplete_cpi()
    .allow_unknown_execution()
    .without_instruction_data();
let receipt = archive.for_each_block(&request, |_| Ok(()))?;
archive.verify_local_unchanged()?;
let transport = archive.transport_snapshot();
println!(
    "blocks={} local_bytes={}",
    receipt.blocks, transport.local_read_bytes,
);
# Ok(())
# }
```

The ledger root supplies the V3 ledger and adaptive reverse objects. The
retained-sidecar root supplies `registry.bin`, `registry.mphf`, signatures,
and the other retained sidecars. The candidate ID is an operator label for the
exact input pair. It is not a manifest, seal, or content hash.

This path has `operator-trusted` source verification. The default source gate
accepts it. It does not create a cache or copy the input files. HTTP and cache
counters stay separate from `local_read_calls` and `local_read_bytes`. Call
`verify_local_unchanged` before accepting a result. It checks the identities of
all local files that the archive opened.

## Safe block skips

The SDK never treats incomplete reverse coverage as proof of absence:

- A signer-wallet query includes all signer-positive blocks and all blocks
  with incomplete account coverage.
- A reached-program query includes all top-level-program and CPI-program
  positive blocks. It also includes all blocks with incomplete account or CPI
  coverage.
- If the target key is absent from `registry.mphf`, the SDK still includes the
  applicable incomplete-coverage blocks. It returns no candidates only when
  the applicable coverage proves absence.

The sparse scanner does not fetch signatures or semantic payloads for skipped
blocks. It keeps canonical block and transaction order for the candidates.
The dense-registry path changes only public-key resolution. It does not decode
skipped blocks and it does not change candidate or exact-match semantics.

## Select the correct workload

The selective path fits a targeted Pump.fun program query. It also fits a
targeted FireWatch program query, or a FireWatch wallet query that has signer
semantics. A signer-wallet query does not mean that the wallet can occur in any
account role.

A full FireWatch `wallet -> program list` build for all wallets must process
the complete relation set. One targeted lookup is not an estimate for that
build.

Do not use the reverse account postings to select a USDC token-balance dump.
They do not prove a mint match in pre/post token-balance metadata. Scan the
applicable token-balance data for that output. A Token-program instruction
query can use the reached-program path, but it is a different result.

## Source binding

The normal `ScanRequest::bounded` verification policy works. The SDK binds all
required V3 ledger objects and retained sidecars by exact URL, length, and
strong ETag. It also records absence for optional sidecars. This trust level is
`object-set-bound`. It does not claim a published complete-epoch manifest.

Both cache profiles use this full object-set binding. The selective profile's
smaller cache does not mean that fewer objects are bound.

The local split path has a different trust boundary. It anchors and checks the
two local roots and reports `operator-trusted`. It does not create a network
object-set binding or a publication claim.

The first slot uses the archive contract's fixed 432,000-slot window. The
reader rejects an epoch or slot geometry that does not match that window.

`source_scope()` reports `selected-prefix` or `full-selection` from the V3 file
header. `full-selection` means that the writer did not use its benchmark prefix
option. It does not prove a complete epoch, and it is not a publication claim.

The selective layout is designed to reduce data reads for sparse targets. This
design alone does not prove a speed result. Use the same target, block
universe, cache state, request policy, and exact result check for a speed
comparison.
