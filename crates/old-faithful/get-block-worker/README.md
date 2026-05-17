# of-get-block-worker

`of-get-block-worker` serves a deliberately narrow Solana JSON-RPC surface from Old Faithful
CAR files plus local sidecar indexes. Old Faithful data is historical, so the
local server only answers methods that are tied to an explicit block slot and do
not pretend to describe the current cluster. Methods outside that surface can be
forwarded to an upstream Solana RPC with `--proxy-url`.

`jsonParsed` is rejected explicitly. Other encodings/detail modes that are not
served locally can be handled by the proxy.

Locally served methods:

- `getBlock`
- `getBlockTime`
- `getVersion`

Cluster-state and ledger-summary methods such as `getSlot`, `getBlockHeight`,
`getBlocks`, `getEpochInfo`, `getHealth`, `getBlockCommitment`, leader schedule
queries, and snapshot queries are left to `--proxy-url`.

## Build the v2 Slot Index

```sh
cargo run -p of-get-block-worker -- build-slot-index-v2 \
  --epoch 800 \
  --out ./slot-index/epoch-800-slot-ranges-v2.raw
```

The v2 index stores one 44-byte row per epoch slot:

```text
offset:u64_le len:u32_le previous_blockhash:[u8;32]
```

For the first produced block in an epoch, pass
`--seed-previous-blockhash <base58>` when the previous epoch is not available in
the same build flow.

## Serve JSON-RPC

```sh
cargo run -p of-get-block-worker -- serve \
  --index-dir . \
  --base-url https://files.old-faithful.net \
  --proxy-url https://api.mainnet-beta.solana.com
```

The server listens on `127.0.0.1:8899` by default and accepts standard JSON-RPC
2.0 HTTP POST bodies.

## Deploy the Cloudflare Worker

The Cloudflare Worker is built from this same `of-get-block-worker` crate with the `worker`
feature. There is no separate Worker app crate.

```sh
cd crates/old-faithful/get-block-worker
npx wrangler@latest deploy --strict
```

`wrangler.toml` runs:

```sh
worker-build --release . --no-default-features --features worker
```

The Worker and native server share the same getBlock config parsing and response
rendering path. Storage access differs by target only at the transport/source
adapter layer.

## Source Modes

Public or authenticated HTTP:

```sh
cargo run -p of-get-block-worker -- serve \
  --index-dir . \
  --base-url https://example.invalid/old-faithful \
  --header 'Authorization=Bearer token'
```

Bearer token from the environment:

```sh
export OF_SOURCE_TOKEN=...
cargo run -p of-get-block-worker -- serve \
  --index-dir . \
  --base-url https://example.invalid/old-faithful \
  --bearer-token-env OF_SOURCE_TOKEN
```

S3-compatible storage, including Backblaze B2 and Cloudflare R2:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

cargo run -p of-get-block-worker -- serve \
  --index-dir . \
  --source-kind s3 \
  --s3-endpoint https://s3.us-west-004.backblazeb2.com \
  --s3-region us-west-004 \
  --s3-bucket my-old-faithful-mirror
```

For Cloudflare R2, use the R2 S3 endpoint and keep `--s3-region auto`.
