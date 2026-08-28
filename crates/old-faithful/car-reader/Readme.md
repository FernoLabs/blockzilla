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

## Features

- `zstd-native` enables `.car.zst` reading through the native `zstd` crate.
- `zstd-wasm` enables wasm-compatible zstd decoding primitives.
- `genesis` enables Solana genesis archive parsing.
- `compact-index` enables compact Old Faithful index parsing.
- `query-sdk` enables `CarInstructionSource` and the common ordered query API.
- `query-sdk-http` adds the bounded concurrent HTTPS stream for that adapter.
- `reader` builds the diagnostic `reader` binary.

Default features: `genesis`, `zstd-native`.
