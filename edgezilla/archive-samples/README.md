# Blockzilla archive samples gateway

This Worker gives public, read-only access to the Blockzilla sample archives.
The R2 bucket stays private. The Worker is the only public entry point.

The public URL and the R2 object key use the same path:

```text
/{car|compact-v2|indexer-v3}/<epoch>/<fixed-name>
```

A local `archive/` directory uses the same tree. Remove the origin from a public
URL and add `archive` to get the local path:

```text
Public: https://<worker>/indexer-v3/900/signatures.bin
R2 key: indexer-v3/900/signatures.bin
Local:  archive/indexer-v3/900/signatures.bin
```

There is no release map and no second path rule.

## Published epochs

The source has one explicit allowlist:

```text
0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000
```

An object is not public only because it exists in R2. Its epoch and file name
must also be in the Worker allowlists. A missing object returns `404`, so the
bucket can be filled in stages. Upload an object only after it passes the
offline format and size checks. The Worker does not detect or select an old
metadata schema. Rebuild an old archive before you upload it.

## Exact file sets

CAR has two epoch-derived names:

```text
epoch-<epoch>.car
epoch-<epoch>-slot-ranges.raw
```

Compact V2 has ten data files for epoch 0 and eleven for later sample epochs.
`prev_blockhash_tail.bin` is absent from epoch 0 by format contract.

```text
archive-v2-blocks.index
archive-v2-blocks.zstd
archive-v2-meta.wincode
blockhash_registry.bin
poh.wincode
prev_blockhash_tail.bin
registry.bin
registry.mphf
shredding.wincode
signatures.bin
vote_hash_registry.bin
```

Indexer V3 has twenty-three data files for epoch 0 and twenty-four for later
sample epochs. `prev_blockhash_tail.bin` is absent from epoch 0 by format
contract.

```text
archive-v2-meta.wincode
archive-v2-standalone-account-postings-adaptive-v3.control
archive-v2-standalone-account-postings-adaptive-v3.coverage
archive-v2-standalone-account-postings-adaptive-v3.pages
archive-v2-standalone-balances.wincode
archive-v2-standalone-block-rewards.wincode
archive-v2-standalone-blocks.index
archive-v2-standalone-inner-instructions.wincode
archive-v2-standalone-loaded-addresses.wincode
archive-v2-standalone-logs.wincode
archive-v2-standalone-messages.wincode
archive-v2-standalone-outcomes.wincode
archive-v2-standalone-raw-metadata-fallbacks.wincode
archive-v2-standalone-token-balances.wincode
archive-v2-standalone-transaction-directory.wincode
archive-v2-standalone-transaction-rewards.wincode
blockhash_registry.bin
poh.wincode
prev_blockhash_tail.bin
registry.bin
registry.mphf
shredding.wincode
signatures.bin
vote_hash_registry.bin
```

The public set does not contain manifests, hashes, seals, reports, candidate
JSON, schema markers, completion objects, or `registry_counts.bin`.

## Read behavior

- `GET` streams the R2 body. It does not buffer an archive in Worker memory.
- `HEAD` returns the object size, content type, strong ETag, and modification
  time.
- `Range` accepts one closed, open, or suffix byte range.
- A bad or unsatisfied range returns `416` and `Content-Range: bytes */<size>`.
- `If-None-Match` and strong ETag `If-Range` requests are supported.
- A file change between the range metadata read and body read returns `503`.
- Other methods, names, epochs, routes, and query strings fail closed.

The R2 development URL and R2 custom domains must stay disabled. They would
bypass the Worker allowlists.

## Local checks

Install the pinned development dependency when you first use this project.
Then run:

```sh
npm test
npm run types
npm run check
```

`npm run check` makes a local dry-run bundle. It does not deploy the Worker.

The Wrangler configuration binds `ARCHIVE_BUCKET` to the private
`blockzilla-archive-samples-v1` bucket. Creating the bucket, uploading objects,
and deploying the Worker are separate, explicit operations.

## Storage diagnostics

The operation logging records `archive_sample_r2_operation` for R2
calls that fail or take at least one second. `operation` distinguishes `head`,
`get_full`, and `get_range`. Each record has the published object key, elapsed
milliseconds, and outcome. Range calls also include the validated offset and
length. Raw error messages, request headers and body data are omitted.

The clock surrounds the awaited R2 operation. Its duration ends when R2 returns
metadata and the stream handle; it does not measure the later body transfer.
Cloudflare documents this use of timers around I/O in its
[performance API reference](https://developers.cloudflare.com/workers/runtime-apis/performance/).
Fast successful calls produce no additional log. Existing response status,
range/ETag validation, streaming and error response fields stay the same.

This diagnostic change was deployed on 8 September 2026. Twelve live exact-range
checks passed; these checks do not measure SDK throughput. See
[the review and validation record](../../docs/design/archive-sample-operation-logging.md).
