# Token index in SQLite

`blockzilla index-tokens` writes recorded token balances into a SQLite
database. It saves its position in the same database transaction as the rows, so
a restart continues where it stopped. You can query the database while the
indexer runs.

The indexer has two sources:

| Source | Who can use it |
| --- | --- |
| **History**: a published epoch from the archive | Everyone. No key needed. |
| **Legacy finalized live source** | Direct finalized relay only. The public feed is now processed. |

Install the CLI first: see [Quick start](quick-start.md#1-install-the-cli).

## Index an epoch from history

```bash
blockzilla index-tokens --epoch 200 --max-blocks 3 --db ./history-200.sqlite
blockzilla token-db --db ./history-200.sqlite --status
```

Run the first command again to index the next three blocks. Remove
`--max-blocks` to index the rest of the epoch.

| Option | Effect |
| --- | --- |
| `--mint ADDRESS` | Keep only this mint. |
| `--from-slot`, `--to-slot` | Index an inclusive slot range inside the epoch. |
| `--origin URL` | Read from another Blockzilla archive source. |
| `--cache-root DIR` | Put the download cache in this folder. |

## Query the database

In another terminal:

```bash
blockzilla token-db --db ./history-200.sqlite --status
blockzilla token-db --db ./history-200.sqlite --limit 10
blockzilla token-db --db ./history-200.sqlite --owner OWNER_ADDRESS
blockzilla token-db --db ./history-200.sqlite --signature TRANSACTION_SIGNATURE
```

`--mint` also works here. Each output line is JSON with the slot, signature,
success flag, `pre` or `post` side, token account position, keys, raw amount,
and decimals.

Or use any SQLite client:

```sql
SELECT b.slot, t.signature, b.mint, b.owner, b.amount, b.decimals
FROM token_balances AS b
JOIN transactions AS t USING (slot, tx_index)
WHERE t.success = 1 AND b.side = 'post'
ORDER BY b.slot DESC, b.tx_index DESC
LIMIT 20;
```

Amounts are decimal text, because a token amount can be larger than SQLite's
largest integer. Do not cast them to integers or floats when you need exact
values.

The table layout is in
[index_tokens_schema.sql](../../blockzilla/cli/src/index_tokens_schema.sql).

## Index live blocks

> **Not available on the current public feed.** `stream.blockzilla.dev` now sends
> processed banks. This indexer requires finalized blocks and rejects that feed.
> Bank undo and processed-to-history reconciliation remain unfinished. Use
> historical indexing above, or implement bank handling with the live SDK.

```bash
blockzilla index-tokens \
  --url https://YOUR_FINALIZED_RELAY \
  --api-key-file ~/.config/blockzilla/stream.key \
  --db ./usdc.sqlite \
  --mint EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
```

This starts at the newest block. The key is sent in request headers and is not
stored in the database.

**Known limit:** this command uses the binary transport. Cloudflare can buffer
that transport on `stream.blockzilla.dev`, so delivery can be late. Live
indexing is tested on a direct relay connection. SSE support for this command is
planned.

To start from an earlier slot, add `--from-slot SLOT`. The indexer first reads
that range from the archive, then continues with live blocks. The archive must
contain the range.

## Restart and recovery

Restart with the same command and the same database. One database belongs to
one source and one mint filter. To change either, use a new database file.

- **Interrupted write:** rows and position commit together. The indexer repeats
  the interrupted chunk and does not add duplicate rows.
- **Network error:** the indexer reconnects from its saved position.
- **Long outage:** if the relay no longer has the saved position, the indexer
  reads the missing blocks from the archive. It checks every parent link before
  it continues live.
- **Missing history:** if the archive does not contain the missing range, the
  indexer stops with status `BLOCKED`. It never skips a gap. Restart after the
  range is available.

Today the public archive has sample epochs only, not the current epoch. A live
outage longer than the relay window (about two minutes) therefore stops with
`BLOCKED`.

## What the data means

- Rows are the pre- and post-balances that Solana recorded for each transaction.
  They are not decoded transfers and not a full account state. An account that
  does not appear in your range is not in the database.
- Failed transactions stay in the database. Use `success = 1` for committed
  changes.
- Live rows include the token account address. History rows give only the
  account position; their account address is `null`.
- If required data is missing or unknown, the indexer stops before it commits
  the affected data. It does not write a partial block.
- One indexer writes a database. Other programs can read it. Keep the `-wal`
  and `-shm` files next to the database while the indexer runs.
- The database grows with every block. There is no automatic pruning.

The implementation is in [index_tokens.rs](../../blockzilla/cli/src/index_tokens.rs).
