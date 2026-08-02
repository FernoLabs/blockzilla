# Helius WebSocket block recorder

Status: **implemented legacy source-specific recorder; not the Hivezilla V1
journal or custody contract**.

The per-slot files, coverage journal, and `cursor.json` below are authoritative
only inside this recorder's current layout. V1 ingests the exact RPC response
envelope into a homogeneous `RecordV1` stream with a prefix cursor and retains
this layout through a read-only migration adapter.

## Decision

Use Helius `rootSubscribe` as a cheap finalized-head notification and retrieve
block bodies through standard HTTP JSON-RPC. Do not use or emulate a direct
`blockSubscribe`: Helius documents that method as unsupported, and the endpoint
returns JSON-RPC `Method not found`.

The durable order is:

1. observe a finalized root;
2. find produced slots (only when catching up or confirming a null block);
3. fetch and atomically publish every produced block;
4. sync a coverage record containing produced and skipped slots; and
5. atomically advance `cursor.json`.

On restart, the legacy cursor is authoritative for this layout. Files published after the previous
cursor but before a crash are detected and reused. This can repeat work but
cannot intentionally skip an unfinished range.

## Cost model

Helius currently charges one credit for a standard RPC request, one credit to
open a WebSocket, and two credits per 0.1 MB streamed through a standard
WebSocket. Because only root notifications travel over the socket, the WebSocket
data charge is negligible compared with block RPC calls.

At roughly 2.5 Solana slots per second, live-head capture is about 6.48 million
`getBlock` calls per 30-day month, before retries and skipped-slot confirmation.
Catch-up adds one `getBlocks` call per configured range. The CLI reports observed
request counts and response bytes so a canary can be projected from real data.

Official references:

- <https://www.helius.dev/docs/api-reference/rpc/websocket/blocksubscribe>
- <https://www.helius.dev/docs/billing/credits>
- <https://www.helius.dev/docs/billing/plans>

## Data limitation

Standard `getBlock` returns the block and transaction data but not Yellowstone
PoH entry updates. Stored records therefore declare
`poh_entries_available: false`. They must not be materialized as entry-complete
Hivezilla captures. A later transaction-only importer can consume this spool
without weakening the existing PoH completeness checks.
