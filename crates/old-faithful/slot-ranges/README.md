# of-slot-ranges

Build per-epoch slot-to-CAR byte-range indexes for Old Faithful archives.

## Range-only index from compact indexes

Use `of-slot-ranges` when the Old Faithful compact indexes are available. The
normal path does not scan the CAR body. If the compact slot index gives the
same full CID for more than one candidate slot, the builder reads only that
CID's exact CAR frame to prove the correct slot.

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

If `--cars-dir` is omitted, the command fetches the CAR header prefix and any
needed collision-proof frame from `files.old-faithful.net`. Pass a local
plain-CAR directory to avoid those requests. Use `--base-url` to select a
different Old Faithful mirror.

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

If two candidate slots resolve to the same full CID, the builder looks up the
CID's exact `(offset,size)` in the CID index. It reads that exact CAR frame,
checks the frame CID, recomputes the CID from the payload, decodes the Block,
and selects the decoded slot only when it is one of the candidates. It stops
if any proof step fails.

## Direct v2 rebuild from existing raw ranges and public slot lists

Use `--slot-list-dir` to rebuild the production v2 files without compact
indexes, CID indexes, CAR files, or Archive V2. This path needs existing
12-byte raw range files, the public Old Faithful slot lists, and the blockhash
registries:

```bash
curl -fL https://files.old-faithful.net/800/800.slots.txt \
  -o /data/slot-lists/800.slots.txt

cargo run --locked -p of-slot-ranges --bin of-slot-ranges -- \
  --start-epoch 800 \
  --end-epoch 800 \
  --output-dir ./slot-index \
  --blockhash-dir /data/blockhash-registry \
  --slot-list-dir /data/slot-lists \
  --overwrite-v2
```

The local file name can be `epoch-N.slots.txt` or `N.slots.txt`. Lines are
decimal slots in strictly increasing order. A list can have at most one slot
from the preceding epoch, followed by the current-epoch block slots, and at
most one slot from the next epoch. The boundary values are the actual last or
first present blocks; they are not necessarily epoch boundary slot numbers.
The builder uses only current-epoch slots as members. When an adjacent slot
list is present in the same directory, it checks a listed predecessor against
the prior list's last current slot and a listed successor against the next
list's first current slot. A listed current block can have an empty raw range
and still receives the correct previous blockhash in the v2 output.

Do not rebuild verified historical v2 files without checking their public raw
inputs. The builder stops if a nonempty raw row is not in the slot list, and
the authoritative validator stops on invalid or overlapping ranges. Keep an
existing verified v2 file when an older public raw file fails these checks.

## Repair malformed public raw ranges without CID indexes

Use `of-repair-slot-ranges` when the public 12-byte raw file has missing,
overlapping, oversized, or nonmember rows. The repair needs the public slot
list and a seekable plain CAR file. It does not read a slot-to-CID or
CID-to-offset index.

```bash
cargo run --locked -p of-slot-ranges --bin of-repair-slot-ranges -- \
  --epoch 4 \
  --raw /data/public-raw \
  --slots /data/slot-lists \
  --car 'https://files.old-faithful.net/' \
  --assume-immutable-http \
  --plan

cargo run --locked -p of-slot-ranges --bin of-repair-slot-ranges -- \
  --epoch 4 \
  --raw /data/public-raw \
  --slots /data/slot-lists \
  --car 'https://files.old-faithful.net/' \
  --assume-immutable-http \
  --output /data/candidate/epoch-4-slot-ranges.raw
```

`--raw` and `--slots` accept either the exact file or a directory. A CAR URL
ending in `/` resolves to `{epoch}/epoch-{epoch}.car`; an exact URL or a URL
with `{epoch}` is also accepted. HTTP sources must support exact Range
responses. By default, the tool requires and pins a strong ETag. The public
Old Faithful endpoint does not provide one, so its immutable epoch URLs need
the explicit `--assume-immutable-http` audit
override. In that mode, the tool fetches every selected segment twice in
reverse verification order, requires matching SHA-256 values, and requires the
same exact object length on every read. This check detects a change during the
repair, but the operator must still guarantee that the epoch URL stays
immutable after the check. `--plan` prints the selected and double-read
transfer byte counts without reading the selected segments or writing output.
Use `--overwrite` only for an existing candidate output.

For a CAR with a trusted whole-object digest, use
`--expected-car-sha256`. This mode forces one segment over the complete plain
CAR, rebuilds every listed current-epoch Block range, and checks the streaming
SHA-256 before it writes the candidate. The digest authorizes one pass from an
HTTP source that has no ETag, so `--assume-immutable-http` is not needed and no
second HTTP read occurs. `--plan` reports one full-object transfer.

```bash
cargo run --release --locked -p of-slot-ranges --bin of-repair-slot-ranges -- \
  --epoch 304 \
  --raw /data/public-raw \
  --slots /data/slot-lists \
  --car 'https://files.old-faithful.net/' \
  --expected-car-sha256 7cd069372272ea081de4f3b2755ec56023669c062be50882bcfed19d667ede21 \
  --output /data/candidate/epoch-304-slot-ranges.raw
```

The official plain-CAR SHA-256 values for the large repair epochs are:

| Epoch | SHA-256 |
| ---: | --- |
| 304 | `7cd069372272ea081de4f3b2755ec56023669c062be50882bcfed19d667ede21` |
| 312 | `e92f38a13f1e51827c843450f164c05356b88cd67985cd317e6c9cd6725ee5bd` |

The repair keeps structurally coherent block rows. It clears all rows that are
not in `N.slots.txt`. For each incoherent run, it reads only the CAR bytes
between the adjacent coherent block boundaries. A run at the start uses the
CAR header and verifies a listed preceding-epoch Block when present. If the
list omits that boundary, it can decode at most one adjacent-epoch boundary
Block from the CAR. A run at the end uses the exact CAR object length and uses
the same rule for an optional next-epoch Block. The tool parses all frames in
the selected bytes with `of-car-reader`. For every Block candidate it loads the
complete payload, recomputes and compares the CAR CID, decodes the full Block,
and requires the current Block slots to equal the selected slot-list run in
order. It writes no output unless all current members have contiguous, nonzero
ranges of at most 64 MiB, all other rows are zero, and the result has exactly
432000 rows.

Compressed `.car.zst` files cannot support bounded repair reads. Use
`of-car-slot-index --raw-only` to scan one of those files and rebuild the
complete epoch instead.

The range wrapper supports the same path and skips compact-index downloads:

```bash
SLOT_LIST_DIR=/data/slot-lists \
BLOCKHASH_DIR=/data/blockhash-registry \
SLOT_INDEX_DIR=./slot-index \
OVERWRITE_V2=1 \
  ./build-slot-index-range.sh 800
```

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

For the production serving artifact, validate the v2 files as the source of
truth:

```bash
cargo run --locked -p of-slot-ranges --bin of-validate-slot-index-v2 -- \
  ./slot-index /data/blockhash-registry \
  --v2-authoritative \
  --start-epoch 800 \
  --end-epoch 800
```

This mode reads only `epoch-N-slot-ranges-v2.raw` and the matching
`blockhash_registry.bin`. It does not open the 12-byte raw index, compact
indexes, CID indexes, CAR files, or Archive V2 files. Each v2 file must have
432000 rows of 44 bytes. A nonzero `previousBlockhash` marks an indexed block
slot. Such a slot can have an empty CAR range. A row with a zero
`previousBlockhash` must have a zero offset and a zero length. Nonempty ranges
must not overflow or overlap.

The validator aligns the indexed rows with the registry and checks that each
row has the hash of the prior indexed block. This check includes empty-range
indexed rows and epoch boundaries. The epoch-0 registry can have the mainnet
genesis prefix. An unprefixed epoch-0 registry needs
`--seed-previous-blockhash`. If validation starts at a later epoch, the
validator uses the explicit seed or reads the last hash from the preceding
epoch registry.

This registry check proves that the v2 chain and registry have the same count,
order, and hashes. It cannot independently prove that the slots marked by the
v2 file are the correct block slots, because this mode uses the v2 file itself
as the slot-membership source.

Use the normal CID-backed mode only for an optional independent range and slot
audit against the Old Faithful compact indexes:

```bash
cargo run --locked -p of-slot-ranges --bin of-validate-slot-index-v2 -- \
  ./slot-index /data/blockhash-registry \
  --indexes-dir /path/to/indexes \
  --start-epoch 800 \
  --end-epoch 800
```

This validator checks exact file and row sizes, rebuilds every canonical range
from the CAR header and CID-index `(offset,size)`, compares every raw row, and
checks previous blockhash continuity across every resolved CID group. A block
whose exact CID end does not advance beyond the prior canonical end has a zero
range row but stays in the blockhash chain. For epoch N after genesis, the
validator requires epoch N-1's registry and checks the epoch boundary.

For an ambiguous compact slot match, the validator uses the local CID index
when present. Otherwise, it opens the CID index at `files.old-faithful.net`
with bounded Range requests. It then verifies the exact CAR frame before it
selects the block slot. Use `--cars-dir` for local plain CAR files and
`--base-url` for a different mirror.

Use `--reuse-raw` only when the 12-byte raw indexes are existing trusted
artifacts and the local CID-to-offset indexes are intentionally absent. This
mode skips only the all-CID canonical range rebuild. It still proves ambiguous
CID groups with the exact CID index and CAR frame. It also checks raw file
structure, raw and v2 prefix equality, zero rows for absent slots, registry
count and order, previous-blockhash alignment, and epoch boundaries. The
default mode keeps the full canonical CID range proof. The range-build wrapper
selects `--reuse-raw` only when every requested raw file exists and
`OVERWRITE=1` is not set.

For an unprefixed direct-CAR epoch-0 registry, pass the same base58 value with
`--seed-previous-blockhash` to the builder, validator, and sync helper. The
direct-CAR wrapper supplies the known mainnet genesis hash by default.
When validation starts at a nonzero epoch, an explicit seed is the predecessor
for that first selected epoch. Without it, the validator reads the last hash
from the preceding epoch registry.

The normal builder derives CAR offsets and lengths only from the Old Faithful
CID index. The normal validator rebuilds those ranges and requires the raw and
v2 rows to match. All blockhash bytes come from `blockhash_registry.bin`.
Archive V2 range and offset fields are not used.

For a legacy Archive V2 audit, add `--archive-v2`. In that mode,
`archive-v2-blocks.index` supplies only the `block_id` order and slot mapping.
The validator still ignores its range and offset fields.

The direct CAR build wrapper creates registries but does not create Archive V2
block indexes. That wrapper uses the explicit `--registry-only` mode. Do not
use this mode for Archive V2 sidecars or indexes built from a different source.

After validation, upload only v2 files to the production prefix:

```bash
SLOT_INDEX_V2_VALIDATE_BIN=target/release/of-validate-slot-index-v2 \
  ./sync-slot-index-r2.sh push-v2-authoritative \
  ./slot-index /data/blockhash-registry \
  r2:blockzilla/slot-index-v2-verified
```

`push-v2-authoritative` runs only the v2-authoritative validator before it
starts the upload. It refuses to change an existing remote object and checks
the uploaded objects. Its built-in default remains the legacy
`r2:blockzilla/slot-index-v2` prefix. Production verified-only uploads must
pass `r2:blockzilla/slot-index-v2-verified` explicitly. The build wrappers use
this path when they sync a v2 build that has a blockhash registry root, and
they require `SLOT_INDEX_V2_REMOTE` so they cannot select a production prefix
implicitly. Set both
`SLOT_INDEX_START_EPOCH` and `SLOT_INDEX_END_EPOCH` to restrict sync validation
and upload to one inclusive epoch range. The upload include list contains only
the validated epochs. `push-v2` remains available as the optional CID-backed
audit path. Set `SLOT_INDEX_V2_REUSE_RAW=1` only for that explicit trusted-raw
audit.

## Raw Format

Each row is 12 bytes and is addressed by `slot % 432000`:

```text
offset:u64_le len:u32_le
```

An empty row has `len == 0`.
