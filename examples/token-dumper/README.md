# Token balance dumper

For a continuous SQLite writer with local queries, see
[Live token index](../../docs/start/token-indexer.md).

The example is part of the Blockzilla CLI. It reads a historical archive through
Cloudflare and emits recorded pre- and post-token balances as JSON Lines.

```bash
blockzilla dump-tokens --epoch 900 --max-blocks 100 > token-balances.jsonl
```

Use `--mint <base58-key>` for one mint. The output includes source identity,
original row coordinates, execution status, nullable keys, and exact raw amounts
as decimal strings. It stops on unknown execution or incomplete balance coverage.

The implementation is [dump_tokens.rs](../../blockzilla/cli/src/dump_tokens.rs).
The [user guide](../../docs/start/token-dumper.md) explains the data contract.
[ClickHouse setup](../../docs/start/clickhouse.md) uses [schema.sql](schema.sql).
This table is for token analytics. It is not Superbank's full ledger schema.
The [live stream guide](../../docs/start/live-stream.md) covers new blocks.
The [program dumper](../../docs/start/program-dumper.md) selects program
transactions with the live SDK or a separate historical SQLite command.

## Check record

Output tests cover UInt64 precision, nullable keys, failed transaction status,
original coordinates, invalid mints, and rejection of an incomplete block before
its first output row. On 2026-10-03, the public Cloudflare epoch 900 read passed for three blocks:
4,718 balance rows in 36.1 seconds, including cold registry setup. The JSON check
confirmed unique source row coordinates, exact UInt64 amount strings, and
ordered block ordinals. Ten epoch 0 block summaries also passed in 3.6 seconds.
The source CLI install and all four new output tests passed.

The SQL recipe has not been run against a ClickHouse server in this check. The
local Docker service did not answer its status request. Full Superbank ingestion
is not implemented.
