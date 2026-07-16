# Edgezilla getBlock Worker

## Purpose and status

`blockzilla-get-block` is Edgezilla's experimental read-only serving
implementation. It reads an existing Archive V2 object tree from Cloudflare R2
or an S3-compatible store and renders Solana JSON-RPC responses. It also builds
as a native CLI for local archive-read benchmarks.

This crate is pre-1.0 and is not a turnkey deployment. The repository does not
contain production bindings, credentials, or a supported archive replication
job.

## Supported read surface

- JSON-RPC `getBlock`, `getBlockTime`, and `getVersion` on `POST /`;
- direct diagnostic block reads on `/block/:slot` and `/block-lite/:slot`;
- compact binary block-bundle reads on the documented `.bin` route; and
- probe routes used by correctness and storage checks.

Write, upload, delete, and replication operations are intentionally outside the
public Worker surface.

## Native CLI

Build the native companion and inspect its commands:

```bash
cargo build --locked -p blockzilla-get-block \
  --bin blockzilla-get-block

cargo run --locked -p blockzilla-get-block \
  --bin blockzilla-get-block -- --help

cargo run --locked -p blockzilla-get-block \
  --bin blockzilla-get-block -- local-bench --help
```

`local-bench` requires an existing archive root containing `epoch-N`
directories and the getBlock serving sidecars. It does not download or modify
archive data.

## Worker build

Prerequisites are the repository's pinned Rust toolchain, the Wasm target, and
the `worker-build` command version used by CI:

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build --locked --version 0.8.3

cd edgezilla/blockzilla-get-block
worker-build --release . --no-default-features --features worker
```

For local Worker development, install the pinned Wrangler development
dependency and use the example configuration:

```bash
npm install
npm run build
npm run dev
```

There is deliberately no deployment script in `package.json`.

The checked-in `wrangler.example.toml` file is for local development and must be
copied and adapted outside the repository for a real deployment. At minimum,
the Worker uses:

- `BZ_ARCHIVE_SOURCE`: `r2`, `s3`, `backblaze`, `b2`, or `auto`;
- `BZ_ARCHIVE_PREFIX`: object prefix containing `epoch-N` directories;
- `BZ_R2_BUCKET_BINDING`: R2 binding name when it differs from the default; or
- `BZ_S3_ENDPOINT`, `BZ_S3_BUCKET`, `BZ_S3_REGION`,
  `BZ_S3_ACCESS_KEY_ID`, and `BZ_S3_SECRET_ACCESS_KEY` for S3-compatible reads.

Treat S3 credentials as Worker secrets, not plain Wrangler variables. Grant
only object-read permissions.

## Source map

| Path | Responsibility |
| --- | --- |
| `src/worker.rs` | Worker routing, object-range reads, Archive V2 decode, and JSON rendering. |
| `src/main.rs` | Native local benchmark companion. |
| `src/lib.rs` | Worker library entry point. |
| `wrangler.example.toml` | Non-production local/example Worker configuration. |

## Validation

```bash
cargo fmt --all -- --check
cargo check --locked -p blockzilla-get-block --all-targets
cargo test --locked -p blockzilla-get-block --all-targets
```

After installing `worker-build`, also run the Worker build shown above. See the
repository [security policy](../../SECURITY.md) before reporting a serving or
credential-handling issue.
