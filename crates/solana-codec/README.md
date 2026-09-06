# solana-codec

Solana's own wire conventions, not Blockzilla's.

Crates here describe shapes defined by Agave and the wider Solana ecosystem, so
they carry `solana-` names and are shared by every consumer that speaks those
formats: Hivezilla ingest, the get-block workers, and any later RPC or
forwarding service.

| Crate | What it describes |
| --- | --- |
| `solana-shred-codec` | Shred transport envelope: payload constants, header parser, compact codec helpers |

Not here, deliberately:

- `solana-ledger-compat` and `solana-gossip` are Hivezilla-only. The first
  mirrors a small shard of Agave's `shred` surface for replay; the second
  carries the gossip and repair peer accounting Hivezilla needs. Both move
  beside Hivezilla rather than into this shared group.
- Geyser gRPC messages come from the upstream `yellowstone-grpc-proto` crate.
  There is nothing to define here.
- getBlock JSON and protobuf shapes are still inside the two get-block workers.
  Extracting them waits on the two RPC paths being diffed and their wire
  divergences fixed; see the conformance work.
