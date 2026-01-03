# Blockzilla

⚠️🚧 EXPERIMENTAL / R&D CODE – NOT PRODUCTION READY 🚧⚠️

This repo contains the tooling used to build and maintain a “ferno” Solana archive node.

## Downlaod and setup file

```
# download and compress epoch 0 to 900
./build_cache.sh 0 900
# you can also just dl one epoch for test (you will need previous epoch to)
./build_cache.sh 799 800 
```

## Compress archive

```
cargo run --release --bin optimize-car-archive build-all
cargo run --release --bin optimize-car-archive build 800
```

## run analyze

```
cargo run --release --bin blockzilla analyze --input blockzilla-v1/epoch-800/compact.bin
```