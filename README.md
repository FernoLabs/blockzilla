# Blockzilla

⚠️🚧 EXPERIMENTAL / R&D CODE – NOT PRODUCTION READY 🚧⚠️

This repo contains the tooling used to build and maintain [Blockzilla](http://blockzilla.dev), the [Ferno](https://ferno.ag) Solana archive node.

## Download and set up the epoch cache

```bash
# Download and compress epochs 0 to 900
./build_cache.sh 0 900

# You can also download a single epoch for testing
# Note: you will also need the previous epoch
./build_cache.sh 799 800
```

## Compress archive

```
cargo run --release --bin optimize-car-archive build-all
cargo run --release --bin optimize-car-archive build 800
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


