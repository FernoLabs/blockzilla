# of-car-reader

Streaming readers for Ferno/Old Faithful Solana CAR archives.

Use this crate to:

- stream `.car` and `.car.zst` archives block by block;
- inspect block metadata, rewards, entries, and transactions;
- scan raw CAR entries with offsets and CIDs;
- read Old Faithful slot range and compact index formats.

## Install

```toml
[dependencies]
of-car-reader = "0.1.2"
```

Default features enable genesis parsing and native zstd support. For plain CAR
reading only:

```toml
[dependencies]
of-car-reader = { version = "0.1.2", default-features = false }
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
- `reader` builds the diagnostic `reader` binary.

Default features: `genesis`, `zstd-native`.
