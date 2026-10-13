# Load ClickHouse

Export token balances with the [token dumper](token-dumper.md), then insert the
JSON Lines file into ClickHouse.

> **Not yet tested on a ClickHouse server.** The export is tested. The SQL below
> follows the ClickHouse documentation but has not run against a server.

You need a running ClickHouse server and `clickhouse-client`.

## 1. Create the table

From the root of your clone:

```bash
clickhouse-client --multiquery < examples/token-dumper/schema.sql
```

This creates `blockzilla.token_balances`, a ReplacingMergeTree table. Its
sorting key is the row's position in the archive, so a row that you insert twice
is merged into one.

## 2. Export and insert

```bash
blockzilla dump-tokens --epoch 200 --max-blocks 100 > tokens.partial.jsonl &&
  mv tokens.partial.jsonl tokens.jsonl

clickhouse-client \
  --query 'INSERT INTO blockzilla.token_balances FORMAT JSONEachRow' \
  < tokens.jsonl
```

Add your normal connection options to `clickhouse-client`.

## 3. Query

The ten mints with the most recorded balances:

```sql
SELECT mint, count() AS recorded_rows
FROM blockzilla.token_balances FINAL
WHERE execution_status = 'succeeded'
  AND side = 'post'
  AND mint IS NOT NULL
GROUP BY mint
ORDER BY recorded_rows DESC
LIMIT 10;
```

- `FINAL` removes duplicate rows at query time. Without it, duplicates stay
  until ClickHouse merges the parts in the background. See
  [ReplacingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree).
- `amount` is a string, so no precision is lost. Use `toUInt64(amount)` in
  queries.
- To get a balance change, join the `pre` and `post` rows on `source_id`,
  `slot`, `tx_index`, `account_index`, and `mint`. `balance_index` can differ
  between the two sides. A missing side is not a zero balance: the account can
  be new or closed.
- Rows with different `source_id` values come from different archive builds.
  Do not mix them.

## Superbank

**Not available yet.** [Superbank](https://github.com/solana-rpc/superbank)
needs full transactions, block metadata, rewards, and PoH entries in its own
`transactions`, `blocks_metadata`, and `entries` tables. The token table on this
page does not supply that data. Do not insert Blockzilla block summaries or
token rows into Superbank tables.

A Blockzilla adapter for Superbank is planned. It must pass Superbank's
ingestion and RPC checks before it can replace the current
[Jetstreamer ingestion plugin](https://github.com/solana-rpc/superbank/tree/2e4e5cbd6dc41aaea490685f47731531e9dc5963/ingest/jetstreamer-clickhouse-plugin).
