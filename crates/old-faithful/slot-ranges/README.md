# of-slot-ranges

Build per-epoch slot-to-CAR byte-range indexes for Old Faithful archives.

## Range-only index from compact indexes

Use `of-slot-ranges` when the Old Faithful compact indexes are available. It
does not scan the CAR body.

```bash
cargo run --locked -p of-slot-ranges --bin of-slot-ranges -- \
  --start-epoch 800 \
  --end-epoch 800 \
  --indexes-dir /path/to/indexes \
  --output-dir ./slot-index \
  --raw-only
```

This writes the legacy 12-byte range index only. It is suitable for
`getBlockTime` or readers that obtain `previousBlockhash` elsewhere, not for
Edgezilla's full `getBlock` response.

If `--cars-dir` is omitted, the command fetches the CAR header prefix from
`files.old-faithful.net`; pass a local plain-CAR directory to avoid that
request.

## Full getBlock index from CAR files

Use `of-car-slot-index` to scan a CAR and build the v2 index carrying
`previousBlockhash`:

```bash
cargo run --locked -p of-slot-ranges --bin of-car-slot-index -- \
  /data/epoch-800.car.zst \
  --output-dir ./slot-index \
  --seed-blockhash-dir /data/blockhash-registry \
  --require-seed
```

Inputs may be local files or directories, HTTP(S) URLs, or `s3://` URLs. Run
either binary with `--help` for authentication, storage, and v2 index options.
For epochs after genesis, the seed directory must contain the previous epoch's
blockhash registry; a one-off run can instead use `--seed-previous-blockhash`.
`--require-seed` prevents an incomplete v2 index from being written.

## Raw Format

Each row is 12 bytes and is addressed by `slot % 432000`:

```text
offset:u64_le len:u32_le
```

An empty row has `len == 0`.
