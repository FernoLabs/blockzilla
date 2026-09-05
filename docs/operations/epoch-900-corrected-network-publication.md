# Epoch 900 corrected network publication

Date: 2026-08-31

Status: **published and active; the cold and warm WAN benchmark is in progress**.

The immutable R2 release and public Worker routes became active on 2026-08-31.
The activation happened only after the object inventory, Worker tests, public
HTTP checks, and both SDK smoke tests passed.

## Fixed release

| Item | Value |
| --- | --- |
| Cluster | `mainnet-beta` |
| Epoch | `900` |
| Slots per epoch | `432000` |
| Release ID | `e900-current-typed-errors-v1` |
| Compact source | `/volume1/blockzilla/archive-metadata-normalization/staging/epoch-900-current-typed-errors-v1-20260828T124710CEST` |
| V3 source | `/volume1/blockzilla/index-archive-trial/foundation-optimized-split-v3-current-r1/epoch-900-full-2g-r2` |
| Private bucket | `blockzilla-network-format-benchmark-staging-v1` |
| Serving bucket | `blockzilla-network-format-benchmark-v1` |
| Compact R2 prefix | `compact-v2/releases/e900-current-typed-errors-v1` |
| V3 R2 prefix | `indexer-v3/releases/e900-current-typed-errors-v1` |
| Public origin | `https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev` |
| Active Worker version | `73ae3918-cb8a-4e01-b39f-025f37507061` |

Do not reuse an old prefix. Do not replace an object in either fixed prefix.
Use a new release ID if one source object or byte size changes.

## Publication contract

The public release uses fixed names and sizes. It has no archive manifest,
partial-file hash, hash sidecar, schema marker, or R2 existence gate.

| Set | Objects | Bytes |
| --- | ---: | ---: |
| Compact V2 | 12 | 104,077,157,709 |
| Indexer V3 | 25 | 106,322,193,125 |
| Total | 37 | 210,399,350,834 |

The NAS sends 28 local files, or 166,272,129,727 bytes. Nine V3 keys, or
44,127,221,107 bytes, are server-side copies from the Compact private-staging
prefix. See `epoch-900-network-format-r2-inventory.md` for every name and size.

The Compact set includes `registry_counts.bin`. It does not include old schema
markers. The V3 set includes the retained-sidecar candidate control and the
adaptive reverse-index control. There is no completion row.

## Completed publication record

The private stage and serving promotion completed on 2026-08-31. The
publisher wrote `publish-complete:e900-current-typed-errors-v1` only after its
exact check passed. A separate serving-bucket list check then found:

| Check | Result |
| --- | ---: |
| Compact V2 objects | 12 |
| Compact V2 bytes | 104,077,157,709 |
| Indexer V3 objects | 25 |
| Indexer V3 bytes | 106,322,193,125 |
| Total objects | 37 |
| Total bytes | 210,399,350,834 |
| Missing, extra, or wrong-size objects | 0 |

All 30 Worker tests and the JavaScript syntax check passed after the route-map
change. The deployment dry run passed before the Worker was deployed.

The public checks then confirmed:

- exact `HEAD` sizes of 66 bytes for the Compact metadata and 107,100,848
  bytes for the V3 block index;
- strong ETags, `Accept-Ranges: bytes`, and `Cache-Control: no-store`;
- exact 1,024-byte closed ranges for one large object in each format;
- `416` for an open range;
- `404` for an unknown file and the old `/manifest` path; and
- `200` for the existing epoch-0 route.

The Compact V2 and Indexer V3 SDKs each opened the public epoch-900 object set
and completed an isolated one-block transaction-identity read. These smoke
tests used separate cache directories. They did not change the cold benchmark
caches.

## 1. Prepare clean source directories

Confirm that every fixed source file exists with the exact size in the
inventory document. The inventory builder uses file metadata only. It does not
calculate or require a new hash.

The two source directories can keep only the named private reports and evidence
that the builder accepts. Move old publication controls out of the two source
directories before you build the TSV. This includes:

- `archive-v2-generation.json`;
- `benchmark-manifest.json`;
- all schema-marker files;
- all `*.sha256` files;
- old publication locks or temporary files.

Do not remove source evidence. Keep it in a separate private evidence
directory when necessary.

## 2. Build and review the fixed TSV

```bash
scripts/build-epoch-900-network-format-r2-inventory.sh \
  --compact-dir /volume1/blockzilla/archive-metadata-normalization/staging/epoch-900-current-typed-errors-v1-20260828T124710CEST \
  --v3-dir /volume1/blockzilla/index-archive-trial/foundation-optimized-split-v3-current-r1/epoch-900-full-2g-r2 \
  --output /absolute/new/path/epoch-900-r2-inventory.tsv
```

Review the TSV. It must have:

- 37 rows after the header;
- 35 payload rows;
- two control rows;
- zero completion rows;
- 28 local rows;
- nine staged-copy rows;
- total target bytes of `210399350834`.

Run the offline builder test before you use the NAS inventory:

```bash
scripts/test-build-epoch-900-network-format-r2-inventory.sh
```

## 3. Stage and check the private release

Use the fixed staged publisher. Do not use a direct serving-bucket upload.

```bash
scripts/publish-network-format-benchmark-r2.sh \
  --inventory /absolute/new/path/epoch-900-r2-inventory.tsv \
  --state-dir /absolute/new/path/epoch-900-publisher-state \
  --release-id e900-current-typed-errors-v1 \
  --scope compact-v2/releases/e900-current-typed-errors-v1 \
  --scope indexer-v3/releases/e900-current-typed-errors-v1 \
  --mode stage \
  --rclone-remote r2
```

Before promotion, list both private prefixes. Compare every key and size with
the TSV. Run the Compact V2 and Indexer V3 network readers against a private
test origin if one is available. Do not add the public Worker route yet.

## 4. Promote to the serving bucket

```bash
scripts/publish-network-format-benchmark-r2.sh \
  --inventory /absolute/new/path/epoch-900-r2-inventory.tsv \
  --state-dir /absolute/new/path/epoch-900-publisher-state \
  --release-id e900-current-typed-errors-v1 \
  --scope compact-v2/releases/e900-current-typed-errors-v1 \
  --scope indexer-v3/releases/e900-current-typed-errors-v1 \
  --mode promote \
  --resume \
  --rclone-remote r2
```

List both serving prefixes. Compare all 37 keys and sizes with the same TSV.
Confirm that the serving prefix contains no extra object.

## 5. Activate the Worker map

Only after the serving review passes, change `BENCHMARK_RELEASE_MAP` in
`workers/blockzilla-archive-gateway/wrangler.jsonc` to this exact value:

```text
compact-v2:0=compact-v2/epoch-0,indexer-v3:0=indexer-v3/epoch-0,compact-v2:900=compact-v2/releases/e900-current-typed-errors-v1,indexer-v3:900=indexer-v3/releases/e900-current-typed-errors-v1
```

Run the Worker tests and syntax check. Review the diff. Then deploy the Worker
through the normal reviewed deployment process.

The activation is atomic at the route-map level. Before activation, both
indexed epoch 900 routes return `benchmark_release_not_published`. After
activation, each route reads only its immutable release prefix.

## 6. Public checks

Check one small file, one large-file range, and one SDK read for both formats.
Use `no-store` requests during the benchmark.

Example public paths:

```text
https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev/compact-v2/v1/epochs/900/files/archive-v2-meta.wincode
https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev/compact-v2/v1/epochs/900/files/registry_counts.bin
https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev/indexer-v3/v1/epochs/900/files/archive-v2-standalone-blocks.index
https://blockzilla-network-format-benchmark-v1.cheron-augustin.workers.dev/indexer-v3/v1/epochs/900/files/signatures.bin
```

Required results:

- `HEAD` returns the expected `Content-Length`, strong R2 `ETag`,
  `Accept-Ranges: bytes`, and `Cache-Control: no-store`;
- a closed range of at most 64 MiB returns `206` and the exact range;
- an open range returns `416`;
- an unknown file and the old `/manifest` path return `404`;
- both SDK readers open epoch 900 and complete their test read;
- the full WAN runner records download bytes, MB/s, transactions per second,
  and elapsed time.

## Rollback

If a public check fails, remove only the two epoch 900 entries from
`BENCHMARK_RELEASE_MAP` and deploy the reviewed epoch-0-only map again. Do not
delete, edit, or replace the R2 objects. Keep the release for diagnosis. A
corrected object set must use a new immutable release ID and new prefixes.
