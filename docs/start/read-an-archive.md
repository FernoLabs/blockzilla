# Read an epoch

`blockzilla stream` reads one epoch from the archive and prints one JSON line
per block.

```bash
blockzilla stream --epoch 200 --max-blocks 100
```

## Available epochs

The archive serves these sample epochs today:

**0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000**

Other epochs return HTTP 404. More epochs will follow.

Epochs are Solana mainnet epochs: epoch N starts at slot N × 432,000.

## Save the output

```bash
blockzilla stream --epoch 200 > blocks-200.jsonl
```

Each line is a block:

```json
{"epoch":200,"block_ordinal":1,"slot":86400001,"transactions":608}
```

Progress goes to standard error, so it does not mix with the file. A file from a
command that failed is incomplete. Check the exit status before you use it.

## Options

| Option | Default | Effect |
| --- | --- | --- |
| `--epoch N` | required | The epoch to read |
| `--max-blocks N` | all | Stop after N blocks |
| `--threads N` | 4 | Decode workers, 1 to 64 |
| `--cache-root DIR` | `.blockzilla-cache` | Download cache folder |
| `--origin URL` | public archive | Another archive source; also `BLOCKZILLA_ORIGIN` |
| `--archive-dir DIR` | none | Read local files instead; needs `--source-id` |

`--max-blocks` counts blocks, not slots. Skipped slots have no block.

Reuse the same cache folder for later reads of the same epoch. Archive files
never change, so the cache stays valid.

## Read local files

If you have a local copy of an epoch:

```bash
blockzilla stream --archive-dir /data/compact-v2/900 \
  --source-id my-copy-900-v1 --epoch 900 --max-blocks 10
```

Keep all files of one archive build together in the folder. `--source-id` is
your name for this copy. Keep it the same for the same files, and change it when
you replace them. See [File formats](../advanced/formats.md) for the file list.

Next: process transactions with [Rust callbacks](callbacks.md), or export
[token balances](token-dumper.md).
