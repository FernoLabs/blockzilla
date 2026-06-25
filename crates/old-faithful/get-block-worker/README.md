# of-get-block-worker

Minimal Cloudflare Worker for serving Solana `getBlock` from official Old
Faithful archives.

The Worker is intentionally narrow and auditable:

- reads only `https://files.old-faithful.net`
- uses official Old Faithful compact indexes
- uses HTTP range requests only
- has no R2 bucket, S3, mirror, database, or sidecar index dependency
- serves JSON-RPC over HTTP POST

## Deploy

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/rpcpool/yellowstone-faithful/tree/master/crates/old-faithful/get-block-worker)

Or deploy with Wrangler:

```sh
npm install
npm run deploy
```

`wrangler.toml` builds the Rust Worker with:

```sh
worker-build --release .
```

## JSON-RPC

POST `/` with a standard JSON-RPC 2.0 request.

Supported methods:

- `getBlock`
- `getBlockTime`
- `getVersion`

Example:

```sh
curl -X POST "$WORKER_URL" \
  -H 'content-type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[424223999,{"transactionDetails":"none","rewards":false}]}'
```

The Worker rejects `jsonParsed` and batch requests on purpose. The goal is a
fast historical `getBlock` implementation, not a full Solana RPC replacement.

## HTTP Routes

- `GET /info`: Worker capabilities and latest complete Old Faithful epoch/slot.
- `GET /block/:slot`: rendered block JSON for quick manual checks.
- `GET /block-lite/:slot`: lighter block JSON for quick manual checks.

## Throughput Benchmark

Use the bundled bench script against a deployed Worker to measure delivery
rate, transfer rate, latency, cache status, and the point where 429/5xx starts.

Hot-cache ceiling, reusing one slot:

```sh
WORKER_URL=https://your-worker.workers.dev \
  npm run bench -- --ramp 1,2,4,8,16,32,64 --duration 20 --same-slot
```

Harder Worker/archive path, using different URLs:

```sh
WORKER_URL=https://your-worker.workers.dev \
  npm run bench -- --route block-lite --format json --rewards false \
  --start 424223999 --ramp 1,2,4,8,16,32 --duration 20 --cache-bust
```

The script prints `blocks/s`, `MiB/s`, average response size, p50/p95/p99
latency, HTTP status counts, and `CF-Cache-Status` counts when present.

## Availability

Old Faithful epochs contain `432000` slots. Some Solana slots are skipped, so
slot ranges are estimated from epoch timing but block existence is decided by
the official Old Faithful indexes.

`/info` reports:

- `latest_complete_epoch`
- `latest_available_slot`
- `slots_per_epoch`
- whether the matching CAR and compact index objects are available

The optional cluster epoch hint uses `OF_CLUSTER_RPC_URL` when set, otherwise it
tries `https://api.mainnet-beta.solana.com`. The Worker works without this hint;
it is only used to choose a smarter cache TTL for `/info`.

## Source Layout

```text
src/lib.rs                 Cloudflare Worker routes and Old Faithful range lookup
src/rpc.rs                 JSON-RPC request/response handling
src/source.rs              official Old Faithful HTTP range reader
src/get_block.rs           getBlock parameter parsing and rendering dispatch
src/car_to_json.rs         protobuf response rendering
src/car_to_json_stream.rs  JSON response renderer
```

There are no native binaries in this package. Index building and archive tooling
belong in `of-car-reader` / `of-slot-ranges`; this crate is the deployable Worker.
