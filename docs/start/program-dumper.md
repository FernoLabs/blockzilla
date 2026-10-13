# Dump transactions for a program

Find the transactions that call your program, at top level or through a
recorded inner call (CPI).

| Need | Tool |
| --- | --- |
| Print new transactions for a program | [Live stream](live-stream.md) example with a program address |
| Save historical matches in SQLite | `blockzilla-dump program` (this page) |
| Process matches in your own Rust code | [Local filters](filters.md#track-a-program) |

## Print live program transactions

> **Private beta.** You need a consumer key. See [Live stream](live-stream.md).

From the repository root:

```bash
BLOCKZILLA_STREAM_MODE=sse node hivezilla/stream-relay/client/example.mjs \
  https://stream.blockzilla.dev ~/.config/blockzilla/stream.key ./pumpfun.cursor \
  6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P > pumpfun-transactions.jsonl
```

Replace the last address with your program. Transaction lines have `slot`,
`bankId`, `commitment`, `blockhash`, `signature`, `success`, `topLevelPrograms`,
and `innerPrograms`. Failed transactions are included. The output also contains
small `root` and `invalidate` records. A processed bank can be invalidated.

This is a console example. If it stops, some printed lines can repeat on the
next run. For a crash-safe application, use the SDK as shown in
[Live stream](live-stream.md#use-the-sdk-in-your-application).

## Save historical matches in SQLite

`blockzilla-dump` is a separate program. It reads archive files from a local
folder and writes the matching transactions to SQLite. It can resume after a
stop.

**Requirement:** a local copy of a Compact V2 epoch, with all files from the same
build. It cannot read the public sample URL directly.

Install it from the repository root:

```bash
cargo install --locked --path indexer/blockzilla-dump --bin blockzilla-dump --debug
```

Run it:

```bash
blockzilla-dump program \
  --archive /data/blockzilla/compact-v2/900 \
  --source-generation-prefix my-copy-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --program 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P \
  --threads 4 \
  --output pumpfun-900.sqlite
```

- `--source-generation-prefix` is your name for this copy of the files. Keep it
  the same when you resume. Use a new name and a new output file when the files
  change.
- `--message-schema current` and `--metadata-schema current-typed-error` are
  correct for the published sample epochs. For other files, use the schema that
  was recorded when they were built.
- `--archive` can also be a folder with `epoch-N` subfolders. Repeat `--epoch`
  to scan several epochs.
- The command scans the full epoch. It has no `--max-blocks` option.

To resume after a stop, run the same command again.

## Query the result

Check progress:

```bash
blockzilla-dump status --output pumpfun-900.sqlite --json
```

List the newest matches:

```sql
SELECT t.epoch, t.slot, t.tx_index, t.primary_signature_base58,
       p.direct_count, p.cpi_count
FROM transactions AS t
JOIN program_matches AS p USING (epoch, slot, tx_index)
ORDER BY t.epoch DESC, t.slot DESC, t.tx_index DESC
LIMIT 20;
```

- `direct_count` counts top-level calls. `cpi_count` counts recorded inner
  calls.
- A transaction that only lists the program as an account does not match.
- Failed transactions are included. This schema has no `success` column: the
  execution result is inside the compact metadata blob.
- The database also stores all signatures, the transaction accounts, and the
  compact message and metadata bytes. These use Blockzilla's compact schema,
  not Solana wire format or RPC JSON.

## Exit status

| Exit | Meaning |
| --- | --- |
| `0` | The scan finished and every match was decided. |
| `1` | An error stopped the scan. Run the same command to resume. |
| `2` | Only with `--on-indeterminate record`: the scan finished, but some transactions could not be decided. They are listed in the `coverage_issues` table. |

By default (`--on-indeterminate fail`) the scan stops when it cannot decide a
match. It never drops a possible match silently.

Reference: [blockzilla-dump README](../../indexer/blockzilla-dump/README.md),
[selector rules](../../indexer/blockzilla-dump/src/scan.rs), and
[database schema](../../indexer/blockzilla-dump/src/database.rs).
