# blockzilla-get-block

`blockzilla-get-block` is the read-only HTTP and JSON-RPC gateway for
Blockzilla Archive V2 data in Cloudflare R2 or S3-compatible storage.

The customer data routes require a Bearer API key. `GET /` and `GET /info`
stay public. Upload, delete, replication, and anonymous diagnostic probe
routes are not exposed.

## Choose a transport

Use HTTP binary when possible. It returns the compact Blockzilla bundle and is
the lowest-overhead path:

```bash
curl -sS https://RPC_HOST/block/321000000.bin \
  -H "Authorization: Bearer $BLOCKZILLA_RPC_API_KEY" \
  --output 321000000.bin
```

Set `access=0` to omit the optional block-access blob. `Range` requests are
rejected for now so cache-hit and cache-miss behavior stays identical.

Direct JSON is the compatibility path. Both the explicit `.json` extension and
the extensionless form work:

```bash
curl -sS 'https://RPC_HOST/block/321000000.json?rewards=false' \
  -H "Authorization: Bearer $BLOCKZILLA_RPC_API_KEY"

curl -sS https://RPC_HOST/block-lite/321000000 \
  -H "Authorization: Bearer $BLOCKZILLA_RPC_API_KEY"
```

Direct binary and JSON responses use the Workers Cache API. JSON-RPC remains
available for Solana clients that call `getBlock`, but it is uncached and
therefore slower for repeated reads:

```bash
curl -sS https://RPC_HOST/ \
  -H "Authorization: Bearer $BLOCKZILLA_RPC_API_KEY" \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[321000000]}'
```

Supported JSON-RPC methods are `getBlock`, `getBlockTime`, and `getVersion`.
Request bodies are limited to 1 MiB and batch requests are rejected.

## Cache and billing

The current offer is $1 per 1 million billable archive-backed requests. Each
authenticated HTTP request records `total_requests = 1`, but
`billable_backend_reads` is only `1` when that request actually attempts at
least one R2 or S3 archive read. Any number of archive range reads within the
same HTTP request still bills as one.

Consequences:

- a direct HTTP cache hit is free;
- `getVersion`, malformed JSON, unsupported methods, invalid parameters,
  rejected batches, and other failures before archive access are free;
- a cache miss is billable only if execution reaches R2 or S3;
- missing data and backend/decode errors are billable when storage was already
  attempted; and
- JSON-RPC is uncached, so a valid `getBlock` or `getBlockTime` normally bills
  one archive-backed request.

Every authenticated response exposes an authoritative request receipt:

- `X-Blockzilla-Cache: HIT|MISS|BYPASS|UNCACHED`
- `X-Blockzilla-Billable-Reads: 0|1`

Authentication runs before every cache lookup. Internal cache keys contain only
the origin, renderer/schema version, archive-prefix generation, normalized slot,
representation, and relevant content options (`rewards` or `access`).
Authorization headers, API keys, customer IDs, and unrelated query parameters
never enter the shared key. Profile/debug requests bypass the cache.

The reusable internal cache object is created before receipt headers are added.
Every customer-facing direct response is normalized to
`Cache-Control: private, no-store`; it never varies on `Authorization`. The
Workers Cache API is per data center and can evict entries, so the same content
can miss—and become billable again—in another region or after eviction.

## API keys

The Worker hashes the presented key with SHA-256 and looks up the lowercase
hex digest in the `BZ_RPC_API_KEYS` KV namespace. Raw API keys are never stored
by the Worker. Each KV value has this schema:

```json
{
  "keyId": "key_acme_main",
  "customerId": "customer_acme",
  "label": "Acme production",
  "status": "enabled"
}
```

`keyId` is the stable per-key metrics index. `customerId` groups keys owned by
one customer. IDs accept 1-64 ASCII letters, digits, underscores, or hyphens;
labels accept 1-128 non-control characters. Set `status` to `disabled` to
revoke a key. KV is eventually consistent, so a cross-region change can take
time to become visible.

Generate key material locally:

```bash
cd edgezilla/get-block
npm run rpc-key:generate -- \
  --key-id key_acme_main \
  --customer-id customer_acme \
  --label "Acme production"
```

The helper prints the raw key once, plus its digest and KV JSON value. It does
not call Cloudflare or mutate a namespace. Give the raw key to the customer,
then store only the digest and JSON record through a reviewed admin or IaC
workflow. Never put raw keys in Wrangler config, KV values, source control, or
logs.

Malformed, missing, unknown, and disabled keys receive the same `401` response.
A missing or unreadable auth or metrics binding fails closed with `503`.

## Usage metrics

Every request with a valid, enabled key writes one point to the
`BZ_RPC_METRICS` Analytics Engine dataset, including free requests. Requests
rejected before authentication are not recorded.

| Column | Meaning |
| --- | --- |
| `index1` | stable `keyId` |
| `blob1` | stable `customerId` |
| `blob2` | method, such as `getBlock`, `getBlockLite`, `getBlockTime`, `getVersion`, `other`, `batch`, or `invalid` |
| `blob3` | bounded outcome, such as `ok`, `not_found`, `backend_error`, `rpc_error`, `parse_error`, `batch_rejected`, `invalid_request`, or `config_error` |
| `blob4` | cache status: `hit`, `miss`, `bypass`, or `uncached` |
| `blob5` | transport: `http_binary`, `http_json`, `json_rpc`, or `unknown` |
| `double1` | total request count (`1`) |
| `double2` | billable backend reads (`0` or `1`) |
| `double3` | HTTP status |
| `double4` | Worker handling latency through response creation in milliseconds; streamed transfer time is excluded |
| `double5` | buffered request bytes; direct GETs report `0` |

Account for Analytics Engine sampling in usage queries. At the current price:

```sql
SELECT
  index1 AS key_id,
  sum(_sample_interval * double1) AS total_requests,
  sum(_sample_interval * double2) AS billable_backend_reads,
  sum(_sample_interval * double2) / 1000000.0 AS estimated_usd
FROM blockzilla_rpc_metrics
WHERE index1 = 'key_acme_main'
  AND timestamp >= toDateTime('2026-07-01 00:00:00')
  AND timestamp < toDateTime('2026-08-01 00:00:00')
GROUP BY key_id
```

Analytics Engine is a sampled usage-metering system, so the query is a billing
approximation. Add an authoritative durable ledger and reconciliation process
before issuing invoices that require exact, auditable counts.

## Run locally

Inspect the native benchmark:

```bash
cargo run --locked -p blockzilla-get-block \
  --bin blockzilla-get-block -- local-bench --help
```

Build or run the Worker:

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build --locked --version 0.8.5

cd edgezilla/get-block
npm install
npm run build
npm run dev
```

`wrangler.example.toml` is a non-production local configuration with
placeholder R2 and KV resource IDs. The package deliberately has no deploy
command. Copy and adapt the configuration outside the repository for a real
environment.

R2 mode can select the source, object prefix, and binding with
`BZ_ARCHIVE_SOURCE`, `BZ_ARCHIVE_PREFIX`, and `BZ_R2_BUCKET_BINDING`.
S3-compatible mode requires `BZ_S3_ENDPOINT`, `BZ_S3_BUCKET`, `BZ_S3_REGION`,
`BZ_S3_ACCESS_KEY_ID`, and `BZ_S3_SECRET_ACCESS_KEY`.

Store credentials as Worker secrets and grant object-read access only.
