# Blockzilla

⚠️🚧 EXPERIMENTAL / R&D CODE – NOT PRODUCTION READY 🚧⚠️

This repo contains the tooling used to build and maintain [Blockzilla](http://blockzilla.dev), the [Ferno](https://ferno.ag) Solana archive node.

## Download and set up the epoch cache

```bash
# Download and compress epochs 0 to 900
./build_cache.sh 0 900

# Epoch 0 also places genesis.tar.bz2 in the cache directory.
# You can also download a single epoch for testing.
# Note: you will also need the previous epoch.
./build_cache.sh 799 800
```

Inspect the cached Solana genesis file, downloading it into `epochs/` first if missing:

```
cargo run --release --bin reader --features reader -- --genesis --archive-dir epochs
```

Archive V2 treats epoch 0 genesis as part of the format: `genesis.tar.bz2` is loaded from the cache, the genesis accounts are emitted as a genesis record, their account/owner/builtin pubkeys are present in `registry.bin`, and the genesis hash is blockhash id `0`.

## Compress archive

```
cargo run --release -p of-archive-importer -- build-all
cargo run --release -p of-archive-importer -- build 800
```

## Run analyze

```
cargo run --release --bin blockzilla analyze --input blockzilla-v1/epoch-800/compact.bin
```

## test and benchmark

```
cargo flamegraph --profile release-debug --bin reader --features reader -- --decode-tx epochs/bench-0-900.car
cargo flamegraph --bench big_block --profile release-debug -- --bench
```

