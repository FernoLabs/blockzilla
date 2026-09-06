# blockzilla-compact-v2-reader

For format choice and the common all-format API, start with
[`Archive formats and the read SDK`](../../../docs/reference/archive-formats-and-read-sdk.md).
This README gives the Compact V2 reader details.

Read-only Rust SDK for an immutable Blockzilla Archive V2 generation. It is
designed for local indexers and client applications: cache the small control
files locally, stream compressed block frames from Blockzilla, filter compact
transactions by epoch registry IDs, read only matching signatures, and build
an application database on the client.

The SDK never asks Blockzilla to reconstruct a Solana block. It returns the
compact Archive V2 message and metadata values that the application parser can
consume locally.

## Current application entry point

Enable the `http` feature to use `CompactV2Archive` for local and network
sources. `open_local` takes an absolute epoch directory and an explicit
`CompactV2LocalDescriptor`; `open` takes a sample origin, epoch, and cache root.
These paths admit fixed object names and pin local files or HTTP object
identities. They do not require a publication manifest or hash the full archive.
See the [working examples](../../../examples/read-compact-v2/README.md).

`scan_ordered_parallel` uses one sequential input producer, reusable private
workers, and a bounded rolling output window. The ordered sink receives a block
as soon as that block is ready. A slow later block does not hold back a completed
prefix. Admission remains charged until the sink returns. Worker threads join
before the scan returns. See the [pipeline contract](../../../docs/design/reader-pipeline-rolling-window.md)
for block, transaction, byte, and shutdown limits.

`scan_token_balances_indexed_parallel` is the optional token-only interface.
It retains registry references in flat balance rows and lets an `IndexedTokenSink`
resolve keys for new dictionary entries. The [indexed USDC output](../../../docs/reference/usdc-indexed-balances-v1.md)
can expand to the existing canonical balance format. Status and complete metadata
validation remain active when the request omits details for known failures.

## Published-generation entry points

The lower-level `ArchiveReader::open` API below reads a published generation.
Its manifest and hashing policy are separate from the application entry points
above.

`ArchiveReader` refuses to open a generation unless:

- `archive-v2-generation.json` is schema version 1, `complete` is true, and its
  deterministic generation digest is valid;
- `archive-v2-blocks.zstd`, `archive-v2-blocks.index`,
  `archive-v2-meta.wincode`, and `registry.bin` are declared and present;
- declared object sizes, index ranges/ordinals, registry shape, and epoch slots
  agree;
- metadata ends in a footer whose block and transaction totals agree with the
  index;
- epoch-0 `genesis.bin`, when declared, is bounded, matches the inline length,
  and hashes to the inline genesis identity; and
- `signatures.bin`, when declared, has exactly 64 bytes per indexed signature.

The default `ArchiveReader::open` uses `HashVerification::AllFiles`. That is the
right choice for a completed local download, but it intentionally reads and
hashes the entire blocks and signatures files before opening. Do **not** use the
default for lazy HTTP streaming.

The manifest can bind exactly one message-schema marker. The explicit Current
marker is `archive-v2-message-schema-current-v1.marker`. The historical marker
is `archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker`. Both
markers in one generation are invalid. Mainnet epochs 0, 1, and 2 must bind one
of them. Later unmarked generations keep the Current compatibility default.

## Source modes

The `blockzilla-dump` CLI exposes the same source choices as `--archive PATH`
for a complete local generation and `--gateway URL` for a Cloudflare gateway.
For direct SDK use, open a local generation with `LocalRangeSource`:

```rust,no_run
use blockzilla_compact_v2_reader::{ArchiveReader, LocalRangeSource};

let source = LocalRangeSource::new("archive/epoch-900");
let archive = ArchiveReader::open(source)?;
println!("{} blocks", archive.index().rows.len());
# Ok::<(), blockzilla_compact_v2_reader::Error>(())
```

## Local cache plus HTTP streaming

Enable the HTTP source:

```toml
blockzilla-compact-v2-reader = { version = "0.2", features = ["http"] }
```

The gateway routes are:

```text
GET/HEAD /v1/epochs/{epoch}/manifest
GET/HEAD /v1/epochs/{epoch}/files/{name}
```

`HttpRangeSource` requires TLS by default, disables redirects and ambient HTTP
proxies, redacts its bearer token from `Debug`, and requires exact `206` plus
`Content-Range` responses for file ranges.

For the intended hybrid flow, first cache the verified manifest, block index,
metadata, and `registry.bin`. A user-program index build also caches the bound
`registry.mphf` in the same directory. Then leave blocks and signatures absent
so the overlay streams their bounded ranges from the gateway:

```rust,no_run
use blockzilla_compact_v2_reader::{
    ArchiveReader, HashVerification, HttpRangeSource, LocalRangeSource,
    OpenOptions, OverlayRangeSource, SelectorOutcome, TransactionMatch,
};

let epoch = 999;
let local = LocalRangeSource::new("cache/epoch-999");
let remote = HttpRangeSource::new(
    "https://blockzilla.example",
    epoch,
    Some("bearer token loaded from the process secret store"),
)?;
let source = OverlayRangeSource::new(local, remote);

let options = OpenOptions {
    hash_verification: HashVerification::ControlFiles,
    ..OpenOptions::default()
};
let archive = ArchiveReader::open_with_options(source, options)?;

// The manifest selects one message schema and one metadata schema for the
// complete generation. The reader never guesses a schema per transaction.
let _message_schema = archive.message_schema();
let _metadata_schema = archive.metadata_schema();

let program_id = [7u8; 32];
let filter = archive.compile_pubkey_filter([program_id])?;
for block in archive.scan(&filter)? {
    let block = block?;
    for transaction in block.transactions {
        // The account scan is a safe prefilter for program invocations because
        // each invoked program must be one of the message accounts.
        match transaction.outcome {
            TransactionMatch::NoMatch => continue,
            TransactionMatch::Indeterminate(reason) => {
                eprintln!("account coverage is indeterminate: {reason:?}");
                continue;
            }
            TransactionMatch::Match { .. } => {}
        }

        match archive.select_program_invocations(
            &filter,
            &transaction.row,
            transaction.message.as_ref(),
            transaction.metadata.decoded(),
        )? {
            SelectorOutcome::Match(invocation) => {
                let signatures =
                    archive.read_transaction_signatures(transaction.signatures)?;
                println!(
                    "slot={} direct={} cpi={} signatures={}",
                    transaction.slot,
                    invocation.direct_count,
                    invocation.cpi_count,
                    signatures.len(),
                );
            }
            SelectorOutcome::NoMatch => {}
            SelectorOutcome::Indeterminate(reason) => {
                eprintln!("program coverage is indeterminate: {reason:?}");
            }
        }
    }
}
# Ok::<(), blockzilla_compact_v2_reader::Error>(())
```

`ControlFiles` hashes `registry.bin`, `archive-v2-blocks.index`,
`archive-v2-meta.wincode`, and a declared `genesis.bin`, while only
size-checking remote blocks and signatures. This avoids a full archive download
before the first block. It assumes the generation URL is immutable and served
through authenticated TLS. Run `AllFiles` after a complete download when
end-to-end file hashing is required.

Sequential `blocks()` and `scan()` calls coalesce adjacent compressed frames
into bounded contiguous reads (64 MiB by default, matching the gateway cap).
Random `read_block(row)` remains a single-frame range request.

`select_token_balances` matches recorded pre- and post-token-balance mints.
Do not use the account scan as its prefilter: a recorded mint is not required
to be a transaction account. Decode metadata for every row in scope, and keep
each `Indeterminate` result in the coverage report.

## Filtering semantics

`compile_pubkey_filter` scans `registry.bin` without loading a registry-sized
hash map. Memory use is proportional to the watched pubkey set. The compiled
filter is bound to both the generation digest and registry SHA-256; using it
with another generation fails explicitly.

For descriptor-based local and object-set sources, binding values can describe
the admitted source and registry metadata. They are not proof that the registry
content was hashed. Keep the reported verification level with any saved filter
or account dictionary.

Each transaction produces one of:

- `Match`: a requested pubkey appears in static accounts, inline raw pubkeys,
  or v0 loaded writable/readonly addresses;
- `NoMatch`: the SDK had enough information to prove no requested pubkey is
  present;
- `Indeterminate`: a raw transaction fallback, unavailable v0 loaded-address
  metadata, or invalid registry reference prevents a safe decision.

Never silently treat `Indeterminate` as `NoMatch` for an indexer.

`TransactionMatch` answers only the account-filter question. It does not mean a
transaction is parser-ready. A downstream parser must additionally require
decoded metadata and its other required fields. `Absent` or `RawFallback`
metadata must stop the lossless path or use an explicit fallback.

`signatures.bin` is optional. The SDK computes each transaction's flat
signature ordinal from the hot index and row counts. It performs one selective
range read only when `read_transaction_signatures` or
`read_signature_ordinal` is called.

## Generation digest

`compute_generation_digest` is the shared publisher/reader implementation. Its
SHA-256 preimage is domain-separated with
`blockzilla/archive-v2-generation\0`, encodes the schema and generation
identity with fixed little-endian lengths/integers, and appends the file table
sorted by raw UTF-8 name. File hashes are decoded to 32 raw bytes. The
`generation_digest` field and the manifest file itself are excluded, avoiding
a circular digest. See the function documentation for the byte-exact layout.

## Test

```bash
cargo test -p blockzilla-compact-v2-reader --all-features
```

## High-level archive interface

With the `http` feature, `archive::CompactV2Archive` opens local or network
archives. This module replaces the former `blockzilla-compact-v2-read-sdk`
crate. Its error and result types are `archive::Error` and `archive::Result`;
the low-level reader retains the root `Error` and `Result` types.
