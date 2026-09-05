# Archive formats and the read SDK

Status: **product guide with explicit implementation status**.

This guide explains the three archive families that Blockzilla uses for read
tests. It also defines the common data model that an application must see.
The detailed wire schemas and the Rust code remain the source of truth.

Do not use this guide as proof that a candidate is ready for publication. A
public URL only proves that bytes are reachable. It does not prove their
origin, completeness, or content identity.

## Names and current status

Use these names in code, reports, and new documents.

| Canonical name | Short name | Current status |
| --- | --- | --- |
| Old Faithful CAR | CAR | External reference format. The repository has a sequential CAR reader. |
| Compact Archive V2 | Compact V2 | Implemented Blockzilla format. `blockzilla-read-sdk` is the supported reader. |
| Indexer-first Standalone V3 | Indexer V3 | Benchmark candidate. The retained sample outputs are internally checked, but they are `unverified-nonpublishable`. |

“Archive V2,” “Compact V2,” and “Compact Archive V2” refer to the same format
family. Use **Compact Archive V2** on first use and **Compact V2** after that.

“V3” alone is not sufficient. The repository has used that label for more
than one index design. Use **Indexer-first Standalone V3** when the distinction
is important.

## Choose a format

| Capability | CAR | Compact V2 | Indexer V3 candidate |
| --- | ---: | ---: | ---: |
| Preserve the Old Faithful source graph | Yes | No | No |
| Read blocks in source order | Yes | Yes | Yes |
| Read transactions in canonical block order | Adapter required | Yes | Candidate reader required |
| Read one block without a full scan | With a slot-range index | Yes | Yes |
| Read selected semantic planes | No | Partial | Yes |
| Resolve compact public-key IDs | Not applicable | Yes | Yes |
| Read primary signatures | From CAR transaction data | Optional sidecar | Optional retained sidecar |
| Read from local files | Yes | Yes | Candidate reader required |
| Read with bounded HTTP ranges | Separate transport | Yes | Candidate reader required |
| Use a verified publication manifest | External source contract | Yes | No current published manifest |

The table describes format capability. It does not promise that one crate
currently gives one common constructor for all three formats. The target SDK
interface is in [The application interface](#the-application-interface).

## The common logical model

Applications must not depend on storage order or wire details. A reader
adapter must publish these facts:

- source identity and trust level;
- epoch, block ordinal, and slot;
- canonical transaction index in the block;
- transaction execution status, when it is available;
- primary signature and required signer keys, when they are available;
- resolved account keys in Solana message order;
- outer instructions in message order;
- inner instructions after their parent outer instruction, in recorded order;
- an exact coordinate for each instruction;
- exact instruction bytes, or an explicit reason why they are not available;
- metadata coverage, including the difference between absent, raw fallback,
  and decoded `None`;
- a receipt with read counts and byte counts that have a stated meaning.

An instruction coordinate has these parts:

```text
block ordinal
transaction index
outer instruction index
optional inner instruction index
optional batch-child index
```

The adapter must not change order to match physical storage. Compact V2 can
store transaction rows in an order that differs from `tx_index`. The adapter
must use canonical `tx_index` order and keep signature offsets bound to the
physical row that supplied them.

### Missing data is data

The SDK must not convert an unknown fact into an empty fact.

| State | Meaning |
| --- | --- |
| Present | The adapter decoded and validated the fact. |
| Not recorded | The source explicitly recorded that the optional fact is absent. |
| Raw fallback | The source retained bytes that this decoder does not understand. |
| Unavailable | A required sidecar or source field is absent. |
| Ambiguous | More than one exact byte candidate exists and proof is absent. |
| Invalid | Source bytes or indexes conflict. The scan stops. |

For example, decoded `inner_instructions = None` is not the same as absent
transaction metadata. The first state is exact. The second state has unknown
inner-instruction coverage.

## Old Faithful CAR

### Purpose

CAR is the external reference archive. It stores a content-addressed graph of
Solana blocks, entries, transactions, metadata, rewards, and data frames. The
graph references define object relationships. File order alone does not define
canonical transaction order.

The reader implementation is in
[`crates/old-faithful/car-reader`](../../crates/old-faithful/car-reader/Readme.md).
Slot-to-byte-range indexes are documented in
[`crates/old-faithful/slot-ranges`](../../crates/old-faithful/slot-ranges/README.md).

### Files used by the network tests

```text
epoch-E.car
epoch-E-slot-ranges.raw
```

The raw slot-range index has one 12-byte row per possible epoch slot:

```text
offset: u64 little-endian
length: u32 little-endian
```

`length == 0` means that the slot has no indexed CAR range.

### Reader rules

- Follow CAR CIDs. Do not use raw file order as transaction order.
- Validate every referenced node before publication to an application.
- Bind a scan to an explicit epoch and canonical slot plan.
- Treat a truncated varint or a retained but incomplete node group as an
  error, not as clean end-of-file.
- Put practical limits on entry bytes, transaction bytes, metadata bytes, and
  references before allocation.
- If an HTTP reader uses an ETag, require one strong ETag and the same exact
  object length for all ranges. This is an operator identity, not a content
  hash.

### Compaction reader and concurrency

The registry builder and the Compact V2 builder use the same lossless block
reader in `of-car-reader`. This reader follows the CAR graph, returns entries
and transactions in reference order, and reuses payload buffers between
blocks. A reader improvement in this shared path applies to both builders.

For an uncompressed CAR, each builder gives the file directly to
`CarBlockReader`. `CarBlockReader` supplies the one buffered read layer.

The two builders use workers for different tasks:

- The registry builder uses `--workers` to scan registry counts.
- The Compact V2 builder uses `--decode-workers` to convert transactions and
  transaction metadata in parallel inside each block.

Both worker counts have a limit of eight. Each registry worker can retain one
complete decoded block, so a larger unbounded worker count is not safe for
large blocks.

The Compact V2 coordinator commits the converted data in transaction order
and block order. Thus, a change to the decode-worker count does not change the
output bytes or the archive order.

A compressed CAR is still sequential at the CAR stream layer. Decode workers
can start only after the stream supplies the block data. A later phase can use
the slot-range index and positioned reads to decode independent CAR ranges in
parallel. That indexed read design is not part of the current compactor.

## Compact Archive V2

### Purpose

Compact V2 stores one independently addressable compressed frame per block.
It keeps common public keys and hashes in registries. This reduces storage and
lets a reader fetch a bounded block range without a full archive download.

The detailed format is in
[`archive-v2-hot-block-format.md`](archive-v2-hot-block-format.md). The reader
contract and examples are in
[`crates/blockzilla-read-sdk/README.md`](../../crates/blockzilla-read-sdk/README.md).

### File classes

Do not treat every file in a builder directory as part of one reader bundle.

#### Core reader files

```text
archive-v2-blocks.zstd
archive-v2-blocks.index
archive-v2-meta.wincode
registry.bin
```

#### Publication controls

```text
archive-v2-generation.json
archive-v2-message-schema-pre-unknown-fallbacks-v1.marker
archive-v2-message-schema-post-unknown-fallbacks-v1.marker
archive-v2-metadata-schema-current-typed-errors-v1.marker
```

One admitted message-profile marker is present. A current published metadata
generation also carries its admitted metadata-profile marker. The exact names
and rules are defined by the read SDK. Do not accept an informal `current`
alias as format authority.

The current metadata profile stores a present transaction error as a typed
`CompactTransactionError`. Some unmarked historical generations store a
length-delimited raw transaction-error value instead. This raw-error layout is
an explicit compatibility input only. A new published generation must pass the
complete exact metadata audit and bind the current typed-error marker.

#### Optional read sidecars

```text
signatures.bin
registry.mphf
blockhash_registry.bin
prev_blockhash_tail.bin
vote_hash_registry.bin
poh.wincode
shredding.wincode
genesis.bin
```

#### Builder and audit files

Examples include `registry_counts.bin`, candidate receipts, temporary files,
and optional block-access indexes. Their presence in a work directory does not
make them part of the basic read contract.

### Reader rules

- Admit the generation before returning application data.
- Select the message and metadata grammar once during admission.
- Validate the block index, object sizes, footer totals, row bounds, and
  registry shape.
- Resolve one-based registry IDs. ID zero is not a registry row.
- Preserve canonical `tx_index` order.
- Keep each signature ordinal bound to its physical transaction row.
- Fetch only bounded ranges. Coalesce adjacent frames for a sequential scan.
- Return raw or indeterminate states explicitly. Do not skip them as if they
  were a valid empty transaction.

### Trust modes

The normal publication path uses `archive-v2-generation.json`. It binds the
generation identity and file SHA-256 values.

The SDK also has an explicit trusted-local mode for operator-controlled data
that has no publication manifest. That mode still validates structure, sizes,
profiles, and ordering. It does not authenticate content. Its identity and
profile assertions are caller inputs.

## Indexer-first Standalone V3

### Purpose

Indexer V3 splits selected transaction semantics into separate planes. A
scanner can read only the planes needed by an indexer. It can also group
contiguous ranges across several blocks.

The current sample outputs are benchmark candidates. They have internal
structural bindings, but they do not have a published source-generation and
registry digest binding. Keep the status `unverified-nonpublishable` in code,
reports, and user interfaces.

### Native ledger objects

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

The directory checkpoint codec used by a production candidate is
`VarintDelta`. The measurement-only fixed-width checkpoint codec is not an
accepted production candidate.

### Reverse-index objects used by the benchmark

```text
archive-v2-standalone-account-postings-adaptive-v3.control
archive-v2-standalone-account-postings-adaptive-v3.coverage
archive-v2-standalone-account-postings-adaptive-v3.pages
```

The report JSON is an audit result. It is not a query plane.

### Retained Compact sidecars

The candidate reader can also use these Compact files:

```text
archive-v2-meta.wincode
registry.bin
registry.mphf
signatures.bin
blockhash_registry.bin
prev_blockhash_tail.bin
vote_hash_registry.bin
poh.wincode
shredding.wincode
```

The object store keeps `signatures.bin` under both the Compact V2 and V3
prefixes. The bytes can be the same, but the two object keys are distinct.

### Candidate controls

```text
archive-v2-retained-sidecars.candidate.json
benchmark-manifest.json
```

These files describe a candidate. They are not a canonical publication
manifest. The epoch-0 benchmark manifest also predates the separate public
signature route. Treat its object list as historical evidence, not as a
complete current bundle definition.

### Reader rules

- Require the V3 format tag and all core plane bindings to agree.
- Require dense selected block IDs and increasing slots.
- Check plane lengths, row counts, offsets, and registry IDs before use.
- Use practical limits that are independent of values in the candidate
  header. Use fallible allocation.
- Accept only the production checkpoint codec.
- Join the message, loaded-address, inner-instruction, and outcome planes into
  the common logical model.
- Do not choose one ambiguous instruction byte form by preference. Use exact
  signature proof or return explicit ambiguous coverage when the caller has
  allowed incomplete instruction data.
- A missing optional proof sidecar can produce incomplete coverage only when
  the caller asked for that policy. A present but invalid sidecar is an error.
- Expose the candidate binding and the `unverified-nonpublishable` trust state
  to the application.

## Trust ladder

Use these terms in this order. A higher level includes the checks below it.

| Level | Meaning |
| --- | --- |
| Reachable | The object can be read from a path or URL. |
| Size checked | The expected objects have the expected lengths. |
| Structurally checked | Indexes, rows, offsets, counts, and internal relations are valid. |
| Internally bound | The format binds its own selected planes to one candidate identity. |
| Manifest bound | A published manifest binds all required object hashes and the generation digest. |
| Source authenticated | The manifest is bound to the accepted source-generation authority. |
| Canonical | Publication policy accepted the source, format, and completeness evidence. |

Current labels:

- Old Faithful CAR: use the external provider and object-identity contract.
- Compact V2 with a valid generation manifest: manifest bound; higher levels
  depend on the publication workflow.
- Compact V2 trusted-local open: structurally checked with a caller assertion.
- Indexer V3 samples: internally bound, `unverified-nonpublishable`.

## The application interface

An example must show the application task, not the archive implementation.
It must not open archive data files directly, reproduce wincode settings,
decompress zstd frames, join V0 loaded addresses, resolve registry IDs, map
signature ordinals, or join inner instructions. Those operations belong in a
reader adapter.

The target interface uses an explicit source constructor and one common
ordered scan callback:

```rust,ignore
let source = CompactV2Source::local("/data/epoch-900")?;
let archive = Ledger::open(source)?;

let request = Scan::all()
    .with_signatures()
    .with_instructions_for([TOKEN_PROGRAM_ID])
    .require_complete_inner_instructions();

let receipt = archive.for_each_transaction(&request, |transaction| {
    consume(transaction)?;
    Ok(ScanControl::Continue)
})?;

println!(
    "format={} trust={} transactions={} source_bytes={}",
    archive.identity().format,
    archive.identity().trust,
    receipt.transactions,
    receipt.source_bytes,
);
```

This is a target interface until all three adapters are present in the
persistent workspace. Do not copy the snippet as an implemented API without
checking the current crate documentation.

The source constructor must be explicit:

```rust,ignore
CarSource::local(car, canonical_slot_plan)
CompactV2Source::local(epoch_directory)
CompactV2Source::http(base_url, epoch).with_cache(cache_directory)
IndexerV3Source::local(candidate_directory, candidate_binding)
IndexerV3Source::http(base_url, epoch, candidate_binding).with_cache(cache_directory)
```

Do not detect a format from bytes and then silently select a decoder.

### Basic API and advanced controls

The basic API must choose safe limits, bounded batching, exact order, and the
strict missing-data policy.

Advanced callers can change:

- block range;
- selected program instruction bytes;
- strict or incomplete coverage policy;
- parallel worker and retained-byte limits;
- cache root and cache byte limits;
- hash verification policy for an explicit trusted-local flow;
- stop behavior and read receipts.

Advanced controls must not permit an adapter to hide its trust state or to
publish invalid order.

## Network and cache contract

A network reader must:

- require HTTPS by default;
- disable redirects and ambient proxy discovery unless the application has an
  explicit policy for them;
- use identity content encoding;
- pin the exact object length and one strong validator;
- send bounded closed range requests;
- validate status, `Content-Range`, response length, and validator on every
  response;
- deliver bytes in object order when requests run in parallel;
- put a fixed limit on active and retained response bodies;
- separate network bytes from local cache bytes in its receipt.

A persistent cache must:

- use an existing private cache directory;
- bind every entry to the source identity, epoch, object name, strong
  validator, and exact length;
- write a private temporary file and publish it atomically without overwrite;
- synchronize the file and directory before it reports success;
- reject symlinks and stale same-size objects;
- show the complete first-use download plan before body reads.

Warm-cache results must still validate the remote identity. A cache hit is not
proof that the remote candidate is canonical.

## Compatibility policy

- Bind message and metadata grammar during source admission.
- Add a new explicit marker or format version when a wire interpretation
  changes.
- Keep unknown semantic variants as exact raw bytes when the format contract
  permits this.
- Do not use a permissive fallback to repair a published marker mismatch.
- Add readers before writers when a compatible migration requires two wire
  profiles.
- Keep candidate manifests immutable. Publish a corrected version instead of
  changing an old candidate in place.
- Pin the repository revision for all pre-1.0 archives and reports.

## Source and result documents

- [Compact V2 hot-block wire reference](archive-v2-hot-block-format.md)
- [Compact V2 read SDK](../../crates/blockzilla-read-sdk/README.md)
- [Old Faithful CAR reader](../../crates/old-faithful/car-reader/Readme.md)
- [Old Faithful slot-range index](../../crates/old-faithful/slot-ranges/README.md)
- [NAS sample-epoch upload readiness](../../benchmark-results/NAS-SAMPLE-EPOCH-UPLOAD-READINESS-2026-08-28.md)
- [Public sample-epoch inventory](../../benchmark-results/PUBLIC-SAMPLE-EPOCH-INVENTORY-2026-08-28.md)
- [Retained Mac network result](../../benchmark-results/mac-network-epoch-0-r1/RESULTS.md)
- [All-sample read estimate](../../benchmark-results/ALL-SAMPLE-EPOCH-READ-ESTIMATE.md)

Benchmark results are dated evidence. They are not part of the format
contract. Compare speed only when all readers publish the same block and
transaction universe and the same application digest.
