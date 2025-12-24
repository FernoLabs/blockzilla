# blockzilla

Main binary for reading and analyzing Blockzilla archives.

## Usage

### Analyze CAR Archive
```bash
blockzilla analyze-car --input cache/epoch-0.car.zstd
```

### Analyze Compact Archive
```bash
blockzilla analyze-compact \
  --input blockzilla-v1/epoch-0 \
  --epoch 0
```

## Metrics

The analyzer outputs:
- TPS (transactions per second)
- Total blocks
- Total transactions
- Total instructions
- Total inner instructions
