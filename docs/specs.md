# Blockzilla v1 (WIP)

Blockzialla is solana archive node made for long terme storage easy indexing and validation of the chain.

## design goal

We should maximise desing to use streaming and 0 copy strategy
Using postcard to encode optimized archive (we mostly care about varint every where it is possible)

## Format

Every solana epoch follow the same format
a folder containing multiple binary based format
a block arrchive containing blocks, transaction
a metadata file containning runtime based info
with the exception of lookup table
Addressed inside lookup table are runtime info but are added into the block loaded address for easy filtering of blocks.
inner instruction are usually containing important information it may also make sense to add them to the block info.

Epoch index
- account index
- slot/Block index : skiped / blockhash, data offset, tx signatures, loaded address

Block data (all data needed for replay the actual archive node should we remove vote tx ?)
   -  instructions array
      - address idx
      - data
      - tx idx

Block runtime info
    - inner ix
      - tx id
      - stack level
      - address idx
      - data
    - logs
      - tx id
      - structure logs
    - sol balance
    - token balance

Per epoch accout data diff
Build reverse diff keep latest state alive and history of change to go to initial state

OK two pass fo inner instruction may be ok ?
filter all instruction invlving a program
then filter all inner instructions
Reconstruction may be slower ?


## Blockzilla folder structure

Cache contains all compress oldfaithfull car files
Blockzilla contains our atcual archive folder is versioned in the future we may have blockzilla-v2

registry contain a list of pubkeys sorted by most used in the epoch and is used as a map of id to pubk.
block index contains metadata about slots for easy filterings

```
cache/
 epoch-0.car.zstd
 ...
 epoch-800.car.zstd

blockzilla-v1/
    epoch-0/
        epoch-0-registry.bin
        epoch-0-slot-index.bin
        epoch-0-block.bin
        epoch-0-runtime.bin
    ...
    epoch-800/
        epoch-800-registry.bin
        epoch-800-block-index.bin
        epoch-800-block.bin
        epoch-800-runtime.bin
```

## project structure

Crate car-reader: contains only CAR parsing and reading logic.
this crate should be reusable by other project and easly auditable / verifiable against other implentation.
Focuse on single core 0 copy reading, contains mostly data structure.
We currently have 3 implementations
* the go from triton included in yellow stone
* a rust implementation from jetstream
* and the one from blockzilla v0
For correctness triton and jsetstream should be used but blockziall should be used for speed

Crate compact-archive: defines the compacted archive format and provides read/write APIs (encoding, decoding, IO).
This should conatins mostly data structures, focuse on single core 0 copy reading.

Binary optimize-car-archive: consumes CAR files via car-reader and produces a compacted archive via compact-archive.
Our main focus for now, we need this to be correct and fast.
This should have two command, the first step is to build registry files then we can optimize.

Binary blockzilla: reads and extracts data from the compacted archive via compact-archive.
this should stream using 0 copy to decode info and output TPS (transaction per second) + counter block, tx, instruction, inner instruction 
One command should use car-reader
an other should use optimize-archive
