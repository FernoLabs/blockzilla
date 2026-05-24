# of-car-reader

`of-car-reader` contains the low-level readers and format helpers used by
Ferno's Old Faithful Solana archive tools.

The crate is intentionally small at the public boundary:

- `CarBlockReader` streams CAR entries while tracking CAR byte offsets.
- `node` decodes the node prefixes and block metadata needed for fast scans.
- `slot_ranges` reads and writes the worker slot index format.
- `compact_index` is available behind the `compact-index` feature.

## Install

Applications normally consume this crate through another Ferno tool, but it can
be used directly:

```toml
[dependencies]
of-car-reader = "0.1"
```

## Slot Range Format

The raw worker slot index stores one 12-byte little-endian row per epoch slot:

```text
offset:u64_le len:u32_le
```

Rows are addressed with `slot % 432000`. An empty row has `len == 0`.

The v2 row format appends the previous blockhash:

```text
offset:u64_le len:u32_le previous_blockhash:[u8;32]
```

## Features

- `compact-index`: compact Old Faithful index parsing.
- `genesis`: Solana genesis archive decoding.
- `reader`: enables the diagnostic `reader` binary.
- `zstd-native`: zstd decompression through the native zstd crate.
- `zstd-wasm`: zstd decompression through the wasm-compatible decoder.

The default features are `genesis` and `zstd-native`.

## Example

```rust,no_run
use of_car_reader::CarBlockReader;
use std::fs::File;

let file = File::open("epoch-800.car")?;
let mut reader = CarBlockReader::with_capacity(file, 64 * 1024 * 1024);
reader.skip_header()?;

let mut scratch = Vec::new();
while let Some(entry) = reader.read_entry_payload_with_scratch(&mut scratch)? {
    println!("entry at CAR offset {}", entry.location.car_offset);
}

# Ok::<(), Box<dyn std::error::Error>>(())
```
