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
of-car-reader = "0.1.3"
```

Default features enable genesis parsing and native zstd support. For plain CAR
reading only:

```toml
[dependencies]
of-car-reader = { version = "0.1.3", default-features = false }
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

## Shared Lossless Block Reader

The registry builder and the compact builder use this block reader. It keeps
the raw nodes and their CAR locations:

```rust,no_run
use of_car_reader::{reconstruct::LosslessCarBlock, CarBlockReader};
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open("epoch-800.car")?;
    let mut reader = CarBlockReader::with_capacity(file, 8 * 1024 * 1024);
    reader.skip_header()?;

    let mut block = LosslessCarBlock::default();
    let mut car_entries = 0u64;
    let mut wire_bytes = 0u64;

    loop {
        let read = reader.read_until_block_lossless_with_stats(&mut block)?;

        // Add the physical input statistics before the EOF check.
        car_entries += read.stats.car_entries;
        wire_bytes += read.stats.wire_bytes;

        if !read.has_block {
            break;
        }

        let slot = block.block.as_ref().expect("block is present").slot;
        println!(
            "slot={slot} entries={} txs={}",
            block.entries.len(),
            block.transactions.len()
        );
    }

    println!("car_entries={car_entries} wire_bytes={wire_bytes}");
    Ok(())
}
```

The reader resolves CIDs. It returns entries in block-reference order and
transactions in entry-reference order. This graph order is the canonical
order. Reuse one `LosslessCarBlock`; the reader keeps a bounded pool of data
buffers for the next block. The last read can consume physical subset or epoch
nodes and return `has_block == false`, so always add its statistics.

## Features

- `zstd-native` enables `.car.zst` reading through the native `zstd` crate.
- `zstd-wasm` enables wasm-compatible zstd decoding primitives.
- `genesis` enables Solana genesis archive parsing.
- `compact-index` enables compact Old Faithful index parsing.
- `reader` builds the diagnostic `reader` binary.

Default features: `genesis`, `zstd-native`.
