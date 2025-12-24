# car-reader

Zero-copy CAR (Content Addressable aRchive) file reader for Solana archive nodes.

## Features

- Zero-copy reading using memory-mapped files
- Single-core focused design
- Reusable and auditable implementation

## Usage
```rust
use car_reader::CarReader;

let reader = CarReader::new("epoch-0.car")?;
let header = reader.read_header()?;

for block in reader.blocks() {
    let block = block?;
    // Process block
}
```
