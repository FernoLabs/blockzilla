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

The sidecar stitch also fails when an epoch after genesis has no predecessor
registry. When a build starts at epoch N, download both epoch N and epoch N-1
registries. The stitch uses epoch N-1's last hash for the first present block,
then uses each current block's hash for the next present block.

Validate a directory of raw indexes before or after an R2 sync with the native
validator (the sync helper invokes this binary automatically):

```bash
cargo run --locked -p of-slot-ranges --bin of-validate-slot-index -- ./slot-index
```

Validate full v2 indexes against their raw indexes and blockhash registries:

```bash
cargo run --locked -p of-slot-ranges --bin of-validate-slot-index-v2 -- \
  ./slot-index /data/blockhash-registry \
  --start-epoch 800 \
  --end-epoch 800
```

This validator checks exact file and row sizes, CAR range order, canonical
empty rows, and previous blockhash continuity. For epoch N after genesis, it
requires epoch N-1's registry and checks the epoch boundary.

After validation, upload only v2 files to the production prefix:

```bash
SLOT_INDEX_V2_VALIDATE_BIN=target/release/of-validate-slot-index-v2 \
  ./sync-slot-index-r2.sh push-v2 \
  ./slot-index /data/blockhash-registry \
  r2:blockzilla/slot-index-v2
```

`push-v2` runs the strict validator before it starts the upload, refuses to
change an existing remote object, and checks the uploaded objects. The build
wrappers use this path automatically when `SYNC_R2_AFTER=1`.

## Raw Format

Each row is 12 bytes and is addressed by `slot % 432000`:

```text
offset:u64_le len:u32_le
```

An empty row has `len == 0`.
