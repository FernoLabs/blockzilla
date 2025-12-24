# optimize-car-archive

Converts CAR files to the optimized Blockzilla compact archive format.

## Usage

### Build Registry
```bash
optimize-car-archive build-registry \
  --input cache/epoch-0.car.zstd \
  --output blockzilla-v1/epoch-0 \
  --epoch 0
```

### Optimize Archive
```bash
optimize-car-archive optimize \
  --input cache/epoch-0.car.zstd \
  --output blockzilla-v1/epoch-0 \
  --epoch 0
```
