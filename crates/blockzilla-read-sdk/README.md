# blockzilla-read-sdk

Read-only Rust SDK for an immutable Blockzilla Archive V2 generation. It is
designed for the Mac/FireWatch flow: cache the small control files locally,
stream compressed block frames from Blockzilla, filter compact transactions by
epoch registry IDs, read only matching signatures, and build the application
database on the client.

The SDK never asks Blockzilla to reconstruct a Solana block. It returns the
compact Archive V2 message and metadata values that the application parser can
consume locally.

## Safety contract

`ArchiveReader` refuses to open a generation unless:

- `archive-v2-generation.json` is schema version 1, `complete` is true, and its
  deterministic generation digest is valid;
- `archive-v2-blocks.zstd`, `archive-v2-blocks.index`,
  `archive-v2-meta.wincode`, and `registry.bin` are declared and present;
- declared object sizes, index ranges/ordinals, registry shape, and epoch slots
  agree;
- metadata ends in a footer whose block and transaction totals agree with the
  index;
- `signatures.bin`, when declared, has exactly 64 bytes per indexed signature.

The default `ArchiveReader::open` uses `HashVerification::AllFiles`. That is the
right choice for a completed local download, but it intentionally reads and
hashes the entire blocks and signatures files before opening. Do **not** use the
default for lazy HTTP streaming.

## Mac cache plus HTTP streaming

Enable the HTTP source:

```toml
blockzilla-read-sdk = { path = "../blockzilla-read-sdk", features = ["http"] }
```

The gateway routes are:

```text
GET/HEAD /v1/epochs/{epoch}/manifest
GET/HEAD /v1/epochs/{epoch}/files/{name}
```

`HttpRangeSource` requires TLS by default, disables redirects and ambient HTTP
proxies, redacts its bearer token from `Debug`, and requires exact `206` plus
`Content-Range` responses for file ranges.

For the intended hybrid flow, place the verified manifest, registry, index and
metadata in `cache/epoch-999/`. Leave blocks and signatures absent so the
overlay routes those objects to the gateway:

```rust,no_run
use blockzilla_read_sdk::{
    ArchiveReader, HashVerification, HttpRangeSource, LocalRangeSource,
    MetadataState, OpenOptions, OverlayRangeSource, TransactionMatch,
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

let watched = [[7u8; 32], [8u8; 32]];
let filter = archive.compile_pubkey_filter(watched)?;
for block in archive.scan(&filter)? {
    let block = block?;
    for transaction in block.transactions {
        if matches!(transaction.outcome, TransactionMatch::Match { .. })
            && matches!(&transaction.metadata, MetadataState::Decoded(_))
        {
            let signatures =
                archive.read_transaction_signatures(transaction.signatures)?;
            // Pass transaction.message, transaction.metadata and signatures to
            // the FireWatch parser, then commit slot progress transactionally.
        }
    }
}
# Ok::<(), blockzilla_read_sdk::Error>(())
```

`ControlFiles` hashes `registry.bin`, `archive-v2-blocks.index`, and
`archive-v2-meta.wincode`, while only size-checking remote blocks and
signatures. This avoids a full archive download before the first block. It
assumes the generation URL is immutable and served through authenticated TLS.
Run `AllFiles` after a complete download when end-to-end file hashing is
required.

Sequential `blocks()` and `scan()` calls coalesce adjacent compressed frames
into bounded contiguous reads (64 MiB by default, matching the gateway cap).
Random `read_block(row)` remains a single-frame range request.

## Filtering semantics

`compile_pubkey_filter` scans `registry.bin` without loading a registry-sized
hash map. Memory use is proportional to the watched pubkey set. The compiled
filter is bound to both the generation digest and registry SHA-256; using it
with another generation fails explicitly.

Each transaction produces one of:

- `Match`: a requested pubkey appears in static accounts, inline raw pubkeys,
  or v0 loaded writable/readonly addresses;
- `NoMatch`: the SDK had enough information to prove no requested pubkey is
  present;
- `Indeterminate`: a raw transaction fallback, unavailable v0 loaded-address
  metadata, or invalid registry reference prevents a safe decision.

Never silently treat `Indeterminate` as `NoMatch` for an indexer.

`TransactionMatch` answers only the account-filter question. It does not mean a
transaction is parser-ready. FireWatch must additionally require decoded
metadata (and any other parser-required fields); `Absent` or `RawFallback`
metadata must stop the lossless path or be handled by an explicit fallback.

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
cargo test -p blockzilla-read-sdk --all-features
```
