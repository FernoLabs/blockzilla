## Build test file 

```
# Make a single-block CAR with first block of every epoch
bench-reader build-bench --start-epoch 0 --end-epoch 10 --cache-dir epochs

# Make a single-block CAR with the biggest block node of epoch 800
bench-reader make-biggest --epoch 800 --cache-dir epochs --output epoch-800-biggest.car
```

## bench

```
cargo flamegraph --profile release-debug --features reader --bin reader epoch-800-biggest.car
```
