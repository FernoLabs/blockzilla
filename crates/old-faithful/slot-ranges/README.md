# of-slot-ranges

`of-slot-ranges` builds per-epoch slot-to-CAR-range indexes for Old Faithful
Solana CAR archives.

For a worker that already gets `getBlockTime` and `previousBlockhash` from a
database, the raw 12-byte slot range index is enough. The fastest path uses the
Old Faithful compact indexes as input and does not scan the CAR body:

```sh
cargo install of-slot-ranges

of-slot-ranges \
  --start-epoch 800 \
  --end-epoch 800 \
  --indexes-dir /path/to/indexes \
  --output-dir ./slot-index \
  --raw-only
```

The index directory is expected to use the Old Faithful per-epoch layout:

```text
indexes/
  800/
    epoch-800.cid
    epoch-800-<epoch-cid>-mainnet-slot-to-cid.index
    epoch-800-<epoch-cid>-mainnet-cid-to-offset-and-size.index
```

The command reads the two compact indexes, computes the CAR byte ranges, and
writes:

```text
slot-index/epoch-800-slot-ranges.raw
```

It does not need a CAR file argument. If `--cars-dir` is omitted, the tool
fetches only the tiny CAR header prefix from `https://files.old-faithful.net`
to learn the CAR header length. If you have private/local CARs and want to avoid
that remote header request, pass `--cars-dir /path/to/plain-cars` together with
`--raw-only`; only the CAR header is read.

## CAR Scan Fallback

The package also provides the `of-car-slot-index` binary. Use this only when the
compact indexes are unavailable and the CAR itself must be scanned.

Inputs can be local CAR files, local directories, HTTP(S) URLs, or S3-style
object URLs:

```sh
of-car-slot-index /data/epoch-800.car.zst --output-dir ./slot-index --raw-only

of-car-slot-index https://files.example.net/800/epoch-800.car.zst \
  --output-dir ./slot-index \
  --raw-only

of-car-slot-index s3://my-bucket/old-faithful/epoch-800.car.zst \
  --s3-region us-east-1 \
  --output-dir ./slot-index \
  --raw-only

of-car-slot-index s3://my-r2-bucket/old-faithful/epoch-800.car.zst \
  --s3-endpoint https://<account>.r2.cloudflarestorage.com \
  --s3-region auto \
  --output-dir ./slot-index \
  --raw-only
```

S3 credentials are read from `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN` unless overridden
with the matching CLI flags.

For each `epoch-N.car` or `epoch-N.car.zst`, this writes:

```text
slot-index/epoch-N-slot-ranges.raw
```

## Raw Format

Each row is 12 bytes and is addressed by `slot % 432000`:

```text
offset:u64_le len:u32_le
```

An empty row has `len == 0`.

Without `--raw-only`, the tool also writes the legacy blockhash sidecar and can
write the v2 44-byte rows used by workers that need `previousBlockhash` in the
slot index itself.
