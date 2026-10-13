# Cloudflare reads

The archive is on Cloudflare. The reader on your machine downloads only the byte
ranges it needs and keeps a local cache.

## Archive source

The CLI and the Rust reader use this source by default. It needs no API key.

```text
https://blockzilla-archive-samples-v1.cheron-augustin.workers.dev
```

It serves the [sample epochs](read-an-archive.md#available-epochs).

To use another Blockzilla archive source, pass `--origin` or set
`BLOCKZILLA_ORIGIN`. `--origin` wins when you set both.

```bash
blockzilla stream --epoch 900 --origin https://your-archive.example --max-blocks 10
```

## What the reader downloads

For each epoch, the reader uses four files:

| File | Size (epoch 900) | When |
| --- | --- | --- |
| `archive-v2-blocks.index` | 22 MB | At start |
| `archive-v2-meta.wincode` | small | At start |
| `registry.bin` (public keys) | 890 MB | At start |
| `archive-v2-blocks.zstd` (blocks) | 60 GB | Only the ranges you read |

The index and key registry download before the first block. Their size depends
on the epoch: the registry is 14 KB for epoch 0, 43 MB for epoch 200, and
2.2 GB for epoch 700. After that, a 10-block read downloads only a few
megabytes. A full-epoch scan downloads all block data of that epoch. Large
scans can also hold the key registry in memory, so plan RAM for it.

The cache is in `.blockzilla-cache` in the current folder. Change it with
`--cache-root`. Reuse the same cache folder to avoid a second download.

## Requirements for another source

If you serve your own copy of the archive, the server must:

- support HTTP range requests,
- send exact `Content-Length` values and strong `ETag` values,
- never change a file after it is published.

The cache trusts the file identity (URL, length, and ETag). Publish changed data
under a new path.

## Live blocks

`stream.blockzilla.dev` is a different service: it sends new blocks, not
archive files. See [Live stream](live-stream.md).

## Lower-level API

Operators can run an authenticated generation gateway with
`/v1/epochs/{epoch}/manifest` and `/v1/epochs/{epoch}/files/{name}` routes.
Use `HttpRangeSource` and `ArchiveReader` for it. See the
[reader guide](../../crates/compact-v2/blockzilla-compact-v2-reader/README.md#local-cache-plus-http-streaming).
