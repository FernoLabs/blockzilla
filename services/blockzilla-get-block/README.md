# blockzilla-get-block

`blockzilla-get-block` is Edgezilla's experimental read-only Archive V2
server. It reads existing archives from Cloudflare R2 or S3-compatible storage
and also builds as a native local-read benchmark.

The Worker provides:

- JSON-RPC `getBlock`, `getBlockTime`, and `getVersion` on `POST /`;
- `/info`, `/block/:slot`, and `/block-lite/:slot` reads;
- compact bundles at `/block/:slot.bin`; and
- diagnostic probe routes.

There are no upload, delete, or replication endpoints.

## Run locally

Inspect the native benchmark:

```bash
cargo run --locked -p blockzilla-get-block \
  --bin blockzilla-get-block -- local-bench --help
```

Build or run the Worker:

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build --locked --version 0.8.3

cd services/blockzilla-get-block
npm install
npm run build
npm run dev
```

`wrangler.example.toml` is a non-production local configuration, and the
package deliberately has no deploy command. Copy and adapt the configuration
outside the repository for a real environment.

R2 mode can select the source, object prefix, and binding with
`BZ_ARCHIVE_SOURCE`, `BZ_ARCHIVE_PREFIX`, and `BZ_R2_BUCKET_BINDING`.
S3-compatible mode requires `BZ_S3_ENDPOINT`, `BZ_S3_BUCKET`, `BZ_S3_REGION`,
`BZ_S3_ACCESS_KEY_ID`, and `BZ_S3_SECRET_ACCESS_KEY`.

Store credentials as Worker secrets and grant object-read access only. See the
repository [security policy](../../SECURITY.md) before reporting a serving or
credential-handling issue.
