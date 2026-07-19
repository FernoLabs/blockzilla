# Blockzilla Archive Gateway

This is a deliberately small, read-only HTTP origin for completed Archive V2
generations. It does not decode blocks, build indexes, write cache files, scan
directories, or hash archive payloads while serving. The manifest is the exact
file allowlist.

The intended deployment is on the Blockzilla NAS, behind TLS or a private
network. Clients cache the registry and index, stream byte ranges of compact
blocks, decode and filter locally, and fetch signature ranges only for matching
transactions.

## Publication boundary

An archive is invisible until an offline operator publishes
`archive-v2-generation.json`. A complete generation must contain:

- `archive-v2-blocks.zstd`
- `archive-v2-blocks.index`
- `archive-v2-meta.wincode`
- `registry.bin`

`signatures.bin` is included automatically when present. Additional immutable
sidecars, such as `registry.mphf`, must be explicitly named with `--file`.

The generator validates every hot-index row and ordinal, dictionary-free zstd
flags, metadata Header/Footer totals, and the exact optional signature length.
It then hashes every published file once, checks that none changed during the
audit, computes the canonical generation digest, and atomically creates the
manifest without overwriting an existing one:

```sh
cargo run --release -p blockzilla-archive-gateway -- generate-manifest \
  --archive-dir /archives/mainnet/999 \
  --cluster-id mainnet-beta \
  --epoch 999 \
  --generation-id epoch-999-final \
  --file registry.mphf
```

After publication, freeze the generation and serve it through a read-only bind
mount or storage snapshot. The server never writes archive files and detects a
normal path replacement while it is running, but it does not change Unix mode
bits, create a snapshot, or re-hash payloads at startup. Files that remain
writable are therefore not an immutable publication boundary.

The generation digest is SHA-256 over this exact binary sequence:

```text
"blockzilla/archive-v2-generation\0"
schema_version:u32le
len(cluster_id):u32le | cluster_id:utf8
epoch:u64le
len(generation_id):u32le | generation_id:utf8
slots_per_epoch:u64le
complete:u8 (0 or 1)
file_count:u32le
for each file sorted by raw UTF-8 name:
  len(name):u32le | name:utf8 | size:u64le | sha256:32 raw bytes
```

`generation_digest` itself is excluded. Hash strings in JSON are lowercase
hex. Clients must bind cached IDs and ranges to this digest, not just an epoch.

## Server

```sh
export BLOCKZILLA_ARCHIVE_GATEWAY_TOKEN='replace-me'
cargo run --release -p blockzilla-archive-gateway -- serve \
  --listen 127.0.0.1:8787 \
  --require-auth \
  --archive-dir /archives/mainnet/998 \
  --archive-dir /archives/mainnet/999
```

Routes:

- `GET|HEAD /healthz` — unauthenticated liveness only
- `GET|HEAD /v1/catalog`
- `GET|HEAD /v1/epochs/{epoch}/manifest`
- `GET|HEAD /v1/epochs/{epoch}/files/{name}`

File responses support one HTTP byte range, `ETag`, `If-None-Match`, and
`Accept-Ranges: bytes`. Multi-range requests and ranges larger than the
configured limit are rejected. A full-file response is streamed and is not
buffered in memory. The download semaphore remains held for the lifetime of
the response body. Manifest and file responses use private client caching;
catalog and authentication errors are not cacheable by shared proxies.

When `BLOCKZILLA_ARCHIVE_GATEWAY_TOKEN` is set, every `/v1` request requires
`Authorization: Bearer …`. Use `--require-auth` in deployments so a missing
secret cannot accidentally make the archive public. The process opens only
manifest-listed regular files with `O_NOFOLLOW` on Unix and never opens a file
for writing.
