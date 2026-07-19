# old-faithful-get-block

`old-faithful-get-block` is Edgezilla's experimental compatibility reader for
official Old Faithful CAR archives. It range-reads CAR data over HTTPS and uses
precomputed indexes from R2; it does not require Archive V2 conversion.

The Worker exposes JSON-RPC `getBlock`, `getBlockTime`, and `getVersion` on
`POST /`, plus `/info`, `/block/:slot`, and `/block-lite/:slot`.

## Read-only and correctness boundary

- Request handlers never upload, delete, or warm object-store data.
- Operators populate and verify the `OF_INDEXES` bucket outside the Worker.
- Use a dedicated, disposable index bucket because an R2 binding is not itself
  restricted to read operations.

Full `getBlock` output requires a validated
`slot-index/epoch-N-slot-ranges-v2.raw` object carrying `previousBlockhash`.
Legacy indexes can still locate blocks for `getBlockTime`, but full block routes
fail with `previous_blockhash_unavailable` rather than returning an invalid
hash.

## Run locally

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build --locked --version 0.8.3

cd services/old-faithful-get-block
npm install
npm run build
npm run dev
```

`wrangler.example.toml` is non-production and the package has no deploy command.
Keep account IDs, bucket names, routes, and credentials outside the repository.

`npm run bench -- --help` describes the optional live benchmark. It can create
provider traffic and cost, so start with a small duration and concurrency. See
the repository [security policy](../../SECURITY.md).
