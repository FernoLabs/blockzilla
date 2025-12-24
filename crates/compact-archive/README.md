# compact-archive

Compacted archive format for Blockzilla, optimized for storage and zero-copy reading.

## Features

- Postcard encoding with varint compression
- Pubkey deduplication via registry
- Binary log format
- Zero-copy reading

## Format

Each epoch contains:
- `epoch-N-registry.bin` - Pubkey registry
- `epoch-N-slot-index.bin` - Slot metadata
- `epoch-N-block.bin` - Block data
- `epoch-N-runtime.bin` - Runtime information
