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

## Full getBlock index from compact indexes and blockhash registries

The normal v2 build does not need Archive V2. It uses the Old Faithful
slot-to-CID index for the ordered unique block slots, the CID-to-offset index
for CAR ranges, and `blockhash_registry.bin` for hash bytes:

```bash
cargo run --locked -p of-slot-ranges --bin of-slot-ranges -- \
  --start-epoch 800 \
  --end-epoch 800 \
  --indexes-dir /path/to/indexes \
  --blockhash-dir /data/blockhash-registry \
  --output-dir ./slot-index
```

When `--overwrite-v2` reuses an existing raw range file, the builder reads the
slot-to-CID index again. It never infers block order from non-empty ranges.
`--archive-v2-dir` remains available only for legacy builds and audits.
The range-build wrapper downloads only slot-to-CID indexes when all requested
raw range files already exist.

The production epoch-0 registry has the mainnet genesis hash first, followed
by one hash for each ordered block slot. Each later epoch has exactly one hash
for each ordered block slot. A direct-CAR epoch-0 registry without the genesis
prefix needs an explicit `--seed-previous-blockhash`. Per-epoch registries must
be under `epoch-N` or `N`; a multi-epoch build does not reuse a registry file
from the root directory.

The builder stops if two candidate slots resolve to the same CID. This result
can be caused by an ambiguous compact hash match, so it is not silently
deduplicated.

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

Validate normal v2 indexes against the same Old Faithful compact indexes and
blockhash registries used by the builder:

```bash
cargo run --locked -p of-slot-ranges --bin of-validate-slot-index-v2 -- \
  ./slot-index /data/blockhash-registry \
  --indexes-dir /path/to/indexes \
  --start-epoch 800 \
  --end-epoch 800
```

This validator checks exact file and row sizes, CAR range order, canonical
non-indexed rows, and previous blockhash continuity across every unique block
slot from the Old Faithful slot-to-CID index. For epoch N after genesis, it
requires epoch N-1's registry and checks the epoch boundary.

For an unprefixed direct-CAR epoch-0 registry, pass the same base58 value with
`--seed-previous-blockhash` to the builder, validator, and sync helper. The
direct-CAR wrapper supplies the known mainnet genesis hash by default.
When validation starts at a nonzero epoch, an explicit seed is the predecessor
for that first selected epoch. Without it, the validator reads the last hash
from the preceding epoch registry.

The Old Faithful raw index is the only source for CAR offsets and lengths.
All blockhash bytes come from `blockhash_registry.bin`.

For a legacy Archive V2 audit, add `--archive-v2`. In that mode,
`archive-v2-blocks.index` supplies only the `block_id` order and slot mapping.
The validator still ignores its range and offset fields.

The direct CAR build wrapper creates registries but does not create Archive V2
block indexes. That wrapper uses the explicit `--registry-only` mode. Do not
use this mode for Archive V2 sidecars or indexes built from a different source.

After validation, upload only v2 files to the production prefix:

```bash
SLOT_INDEX_V2_VALIDATE_BIN=target/release/of-validate-slot-index-v2 \
  ./sync-slot-index-r2.sh push-v2 \
  ./slot-index /path/to/indexes /data/blockhash-registry \
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
