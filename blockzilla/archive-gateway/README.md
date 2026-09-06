# Blockzilla Archive Gateway

This is a deliberately small, read-only HTTP origin for completed Archive V2
generations. The server path does not decode blocks, build indexes, write cache
files, scan directories, or hash archive payloads. The offline publication
command does the required full validation. The manifest is the exact file
allowlist.

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
- `registry.mphf`

`signatures.bin` is included automatically when present. For epoch 0,
`genesis.bin` is also included automatically when present; replay treats those
exact manifest-bound bytes as authoritative because the legacy inline V2
genesis projection omits launch fields. Other immutable sidecars must be
explicitly named with `--file`.

The generator validates every hot-index row and ordinal, dictionary-free zstd
flags, metadata Header/Footer totals, and the exact optional signature length.
It also proves that manifest-bound `registry.mphf` maps each `registry.bin`
key to its exact one-based row ID. This rejects duplicate registry keys and an
index made from different registry bytes.
The operator must name the candidate wire profile that wrote the archive. The
generator then decodes every typed message with a bounded reader. The selected
profile must decode all messages. The other profile must fail on at least one
message, or both profiles must give the same meaning for all messages. A
dual-valid generation with different meanings is rejected. The generator
publishes the SDK marker only after this proof succeeds. It then hashes every
published file once, checks that none changed during the audit, computes the
canonical generation digest, and atomically creates the manifest without
overwriting an existing one:

```sh
cargo run --release -p blockzilla-archive-gateway -- generate-manifest \
  --archive-dir /archives/mainnet/999 \
  --cluster-id mainnet-beta \
  --epoch 999 \
  --generation-id epoch-999-final \
  --wire-profile post-unknown-instruction-fallbacks-v1
```

Use `pre-unknown-instruction-fallbacks-v1` only for a generation that was
written with the historical instruction tag order. The generator never uses
the epoch, a partial probe, or a compatibility default to select a profile.

The generator hashes every pinned input a second time under the shared
publication lock immediately before it publishes the marker and manifest. On
startup, the server hashes every manifest object, scans every typed message,
checks the selected profile again, and repeats the exact registry/index mapping
proof. It also rejects a changed file identity when a request opens an object.

After publication, freeze the generation and serve it through a read-only bind
mount or storage snapshot. The server does not change Unix mode bits or create
a filesystem snapshot. A process that can modify an open inode after startup
can still violate immutability, so writable files are not a publication
boundary.

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
