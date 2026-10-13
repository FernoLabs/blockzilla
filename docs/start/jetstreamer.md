# Move from Jetstreamer

[Jetstreamer](https://github.com/anza-xyz/jetstreamer) reads Old Faithful CAR
files and calls your plugins. Blockzilla keeps the same flow: select history,
process transactions on your machine, write the results. The difference is the
source: Blockzilla reads a compact archive through Cloudflare, so you download
only the byte ranges you need.

Jetstreamer plugins do not run on Blockzilla directly. You move their logic into
a Blockzilla callback.

## Map your workload

| In Jetstreamer | In Blockzilla |
| --- | --- |
| Stream an epoch | `blockzilla stream --epoch N`, or the [Rust reader](callbacks.md) |
| Plugin `on_transaction` / `on_block` | [Block and transaction callbacks](callbacks.md) |
| Program invocation counts | [Program selector](filters.md#track-a-program) in a callback |
| Instruction data | Sequential callback with instruction data requested |
| Token activity | [Token dumper](token-dumper.md) or [token index](token-indexer.md) |
| ClickHouse output | JSON Lines into [ClickHouse](clickhouse.md) |
| Entry and reward callbacks | Not available yet |
| Geyser plugin | Not supported |
| Superbank ingestion | Not available yet. See [ClickHouse](clickhouse.md#superbank) |
| Live blocks | [Live stream](live-stream.md) (private beta) |

## Differences to plan for

- **Callbacks.** Blockzilla gives you ordered block views that contain the
  transactions. There are no separate entry or reward callbacks yet.
- **Order.** Blocks always arrive in archive order, also with several decode
  workers. Start with four workers (`--threads 4`).
- **Instruction data.** The parallel scan does not return instruction data
  bytes. Use the sequential callback when you parse instruction data.
- **Restart.** The reader does not save your position. Save the next block
  number with your output, in one database transaction. See
  [Callbacks](callbacks.md#save-your-position).
- **Epochs.** Today the archive has [sample epochs](read-an-archive.md#available-epochs)
  only.

Checked against Jetstreamer revision `048d2cd25af949679c56347440b7cf5a9a900063`.
