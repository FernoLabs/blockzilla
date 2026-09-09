# of-car-reader

For the Blockzilla common CAR, Compact V2, and Indexer V3 API, start with
[`Archive formats and the read SDK`](../../../docs/reference/archive-formats-and-read-sdk.md).
This README gives the direct CAR reader details.

Streaming readers for Ferno/Old Faithful Solana CAR archives.

Use this crate to:

- stream `.car` and `.car.zst` archives block by block;
- inspect block metadata, rewards, entries, and transactions;
- scan raw CAR entries with offsets and CIDs;
- read Old Faithful slot range and compact index formats.

## Install

```toml
[dependencies]
of-car-reader = "0.2.0"
```

Default features enable genesis parsing and native zstd support. For plain CAR
reading only:

```toml
[dependencies]
of-car-reader = { version = "0.2.0", default-features = false }
```

## Quick Start

```rust,no_run
use of_car_reader::CarStream;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = CarStream::open_zstd(Path::new("epoch-800.car.zst"))?;

    while let Some(group) = stream.next_group()? {
        let slot = group.slot.unwrap_or_default();
        let (tx_count, _tx_bytes) = group.get_len();
        println!("slot={slot} txs={tx_count}");
    }

    Ok(())
}
```

See the crate docs for complete examples using `CarStream`, `CarBlockReader`,
transaction iterators, raw entry scanning, rewards, and feature flags:

<https://docs.rs/of-car-reader>

## Allocation choices

For streaming metadata scans, use `TxMetadataIter::next_metadata_visit` with a
`TransactionStatusMetaVisitor`. Enable only the fields the application needs.
Protobuf strings and byte fields borrow the input or reusable zstd buffer;
callbacks do not need a vector of logs, balances, or instructions. Consume
borrowed values before the next iterator step. The iterator reports missing
metadata separately and returns an owned decoder result for legacy metadata.
Its existing continuation restrictions still apply.

The generated borrowed metadata view still allocates vectors for repeated
fields. Use the visitor when the application does not need to retain a complete
metadata object. A full visitor must also decode `reward_raw` submessages;
that callback alone does not decode their fields.

`VersionedTransactionReuse` keeps transaction decode buffers between calls.
Return them with `recycle_transaction` after use. Keys and signatures borrow
input bytes; instruction and lookup payloads use reusable owned buffers. This
is not a completely zero-copy transaction representation. Legacy metadata
conversion moves payload buffers into its result instead of cloning them.

The [CAR decode probe](../../../bench/reader-profile/README-car-decode.md)
compares owned metadata with a visitor that consumes all known fields. It also
provides separate allocation counts and bounded HTTP concurrency tests.

## Features

- `zstd-native` enables `.car.zst` reading through the native `zstd` crate.
- `zstd-wasm` enables wasm-compatible zstd decoding primitives.
- `genesis` enables Solana genesis archive parsing.
- `compact-index` enables compact Old Faithful index parsing.
- `query-sdk` enables `CarInstructionSource` and the common ordered query API.
- `query-sdk-http` adds the bounded concurrent HTTPS stream for that adapter.
  It retries an incomplete response body at most twice, checks the same file
  identity and exact range on every attempt, and counts retry bytes. It never
  delivers a partial range to the decoder.
- `archive` adds `archive::CarArchive`, fixed sample object discovery, local
  raw/zstd selection, HTTP source checks, and the common ordered query API.
  It includes native zstd support.
- `reader` builds the diagnostic `reader` binary.

Default features: `genesis`, `zstd-native`.

## Archive and slot-index tools

`CarArchive::open_local` reads `car/<epoch>/epoch-<epoch>.car` or
`epoch-<epoch>.car.zst`, plus `epoch-<epoch>-slot-ranges.raw`. It prefers a
final raw CAR when both forms exist. Compressed files are decoded sequentially;
slot offsets refer to the decoded stream. See the
[CAR examples](../../../examples/read-car/README.md) for source setup and output checks.

The [of-slot-ranges package](../of-slot-ranges/README.md) stays beside this
reader. It contains slot-index construction, bounded repair, and validation
commands. Its tools are part of the Old Faithful format family.
