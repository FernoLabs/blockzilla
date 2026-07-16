# Old Faithful getBlock Worker

## Purpose and status

`old-faithful-get-block` is Edgezilla's experimental compatibility reader for
official Old Faithful CAR archives. It range-reads the public CAR objects and
renders Solana JSON-RPC responses without first converting the epoch to
Blockzilla Archive V2.

This implementation was recovered from the project's preserved history. It is
useful as an independent reference path, but it is pre-1.0 and not a turnkey
deployment.

## Read-only boundary

The Worker exposes `getBlock`, `getBlockTime`, and `getVersion`, plus direct
block and information routes. Request handlers do not upload, delete, or warm
object-store data. Cloudflare's response cache may still be populated as an
ordinary serving optimization.

The `OF_INDEXES` R2 binding contains precomputed slot-range indexes and may
mirror the official compact index objects. Operators must populate and verify
those objects outside this Worker. CAR bytes are fetched by HTTPS from the
official Old Faithful archive host.

Read-only is enforced by this Worker's code: it never calls R2 upload, write,
delete, or multipart APIs. A direct R2 binding is not itself scoped to
object-read operations, so deploy with a dedicated serving bucket and review
the Worker before every production release. Treat that bucket as a disposable,
rebuildable index mirror rather than archive authority.

For complete `getBlock` output, each populated epoch needs a validated
`slot-index/epoch-N-slot-ranges-v2.raw` object. The v2 rows include
`previousBlockhash`; the legacy raw and compact-index fallback paths can locate
a CAR range for methods such as `getBlockTime`, but `getBlock` and direct block
routes fail with `previous_blockhash_unavailable` instead of returning an
invalid null/empty hash. For epochs after genesis, index generation must be
seeded from the preceding blockhash and validated before serving. The `/info`
response reports only observed index-object presence. It does not claim that an
epoch is complete or RPC-ready, and its `epoch_end_slot` is the epoch boundary,
not a claim that every slot in the epoch exists. Cluster-RPC errors are
sanitized so an operator-supplied URL cannot be returned to clients.

## Worker build

```bash
rustup target add wasm32-unknown-unknown
cargo install worker-build --locked --version 0.8.3

cd edgezilla/old-faithful-get-block
worker-build --release .
```

For local development, install the pinned Wrangler dependency and use the
non-production example configuration:

```bash
npm install
npm run build
npm run dev
```

There is deliberately no deploy command. Copy and adapt
`wrangler.example.toml` outside the repository before any real deployment;
never commit account IDs, bucket names, routes, or credentials.

## Source map

| Path | Responsibility |
| --- | --- |
| `src/lib.rs` | Worker routes, request caching, and response dispatch. |
| `src/archive/` | Old Faithful HTTP range reads and read-only index lookup. |
| `src/rpc/` | JSON-RPC parsing and method dispatch. |
| `src/render/` | JSON, protobuf, and base58 rendering. |
| `scripts/bench-worker.mjs` | Optional network benchmark for an operator-supplied URL. |
| `wrangler.example.toml` | Non-production local/example configuration. |

## Safety

The benchmark sends live requests and can create provider traffic. Start with a
small concurrency and duration. Keep URLs containing tokens out of command
history and generated reports, and follow the repository
[security policy](../../SECURITY.md).
