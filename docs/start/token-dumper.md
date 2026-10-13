# Token dumper

`blockzilla dump-tokens` reads an epoch and writes every recorded token balance
as one JSON line. Use it for a one-time export, or to load a database such as
[ClickHouse](clickhouse.md).

For a database that resumes after a stop, use the
[token index](token-indexer.md) instead.

## Export USDC balances

```bash
blockzilla dump-tokens --epoch 200 --max-blocks 20 \
  --mint EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v > usdc.jsonl
```

This writes 1,334 rows. Remove `--mint` to export all mints. Remove
`--max-blocks` to export the whole epoch.

The command accepts the same options as [`stream`](read-an-archive.md#options).

## Output fields

One line is one balance:

| Field | Meaning |
| --- | --- |
| `source_id` | ID of the archive build that the row came from |
| `epoch`, `block_ordinal`, `slot`, `tx_index` | Where the transaction is |
| `execution_status` | `succeeded` or `failed` |
| `side` | `pre` (before the transaction) or `post` (after it) |
| `balance_index` | Position in Solana's pre or post balance list |
| `account_index` | Position of the token account in the transaction's account list |
| `mint`, `owner`, `token_program` | Base58 addresses, or `null` when not recorded |
| `amount` | Raw amount as a decimal string, so no precision is lost |
| `decimals` | Decimal places of the mint |

The token account address is not in the output yet. `account_index` gives its
position in the transaction.

## What the rows mean

- Rows are balances recorded before and after each transaction. They are not
  transfers. To get a change, pair the `pre` and `post` rows of the same
  transaction and `account_index`.
- Failed transactions are included. Filter on
  `execution_status = 'succeeded'` for committed changes.
- If a range has no rows for your mint, no transaction in that range recorded a
  balance for it. It does not mean the mint has no history.
- With `--mint`, rows with a `null` mint stay in the output, because they can
  be a match.
- If execution status or token data is missing, the command stops. It does not
  skip rows.

## Save a complete file

The command has no resume position. If it fails, the output is incomplete.
Write to a temporary name and rename only after success:

```bash
blockzilla dump-tokens --epoch 200 > tokens.partial.jsonl &&
  mv tokens.partial.jsonl tokens-200.jsonl
```

After a failure, run the full export again. Do not append to the partial file.

## Change the code

The command is [dump_tokens.rs](../../blockzilla/cli/src/dump_tokens.rs). To
write your own version, see [Read token balances](filters.md#read-token-balances).
