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
`slot-index-v2-verified/epoch-N-slot-ranges-v2.raw` object carrying
`previousBlockhash`. The default `OF_SLOT_INDEX_FALLBACK=verified-only` policy
uses only this prefix for the primary slot-to-CAR lookup. If an object is absent
or malformed, the request fails; it does not silently use an older slot index.

Some Old Faithful blocks store rewards outside the primary CAR range. Reward
reconstruction still uses each epoch CID file and its CID-to-offset-and-size
index. Keep those objects and their CAR files while this path is enabled. The
`verified-only` setting does not replace or disable this reward dependency.

Two explicit fallback policies exist for a temporary, controlled rollback:

- `validated-v2` also checks `slot-index-v2/` and the v2 files under
  `slot-index/`, in that order.
- `validated-legacy` additionally permits the 12-byte raw slot ranges and the
  compact slot-to-CID/CID-to-offset indexes.

The policy names are operator assertions. Do not set either fallback policy
unless all objects that it can select were independently validated against the
CAR files. An unset value is the same as `verified-only`. Any other value is a
configuration error and the Worker fails closed.

`/info` returns HTTP 200 only when the configured policy is valid and a
correctly sized v2 index is present. It returns HTTP 503 with `ok: false`, a
stable `index_presence_error` code, and `Cache-Control: no-store` when the
policy, R2 lookup, object size, or previous-blockhash index is not ready. A
cluster epoch-hint failure alone does not make the Worker unhealthy.

## Verified-prefix rollout and rollback

1. Build and validate every epoch in an isolated staging directory. Check the
   previous blockhash for the first block of each epoch against the last block
   of the preceding epoch.
2. Upload the files to `slot-index-v2-verified/` without changing existing
   objects. Compare the object count, exact sizes, and SHA-256 manifest with
   the staged set.
3. Keep `OF_SLOT_INDEX_FALLBACK` unset or set it to `verified-only`. Deploy the
   Worker only after the complete prefix passes the remote checks. The Worker
   uses a new cache namespace for this policy, so it does not reuse block
   responses that an older index produced.
4. Test an early, middle, and latest epoch, an epoch boundary, and a skipped
   slot. Compare returned `previousBlockhash` values with the registry.

To roll back Worker code, restore the prior Cloudflare Worker version and its
settings. Do not enable an old slot-index prefix as a quick rollback. Use
`validated-v2` or `validated-legacy` only when that complete fallback source
has its own CAR-based validation record. After the verified-prefix rollout is
stable, remove only the obsolete slot-index prefixes and the temporary fallback
setting. Keep the epoch CID and CID-to-offset objects that reward reconstruction
uses.

## Run locally

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build --locked --version 0.8.3

cd workers/old-faithful-get-block
npm install
npm run build
npm run dev
```

`wrangler.example.toml` is non-production and the package has no deploy command.
Keep account IDs, bucket names, routes, and credentials outside the repository.

`npm run bench -- --help` describes the optional live benchmark. It can create
provider traffic and cost, so start with a small duration and concurrency.
