# Quick start

Install the CLI and read Solana blocks from the archive. You need no API key, no
validator, and no RPC node.

**Time:** about 10 minutes. Most of it is the first compile.

## Before you start

| You need | How to check or install |
| --- | --- |
| Rust 1.98 or later | `rustc --version`. Update with `rustup update`. |
| Git and a C compiler | macOS: `xcode-select --install`. Debian/Ubuntu: `sudo apt install git build-essential`. |
| About 3 GB of free disk | For the first build. |

## 1. Install the CLI

```bash
git clone --depth 1 https://github.com/FernoLabs/blockzilla.git
cd blockzilla
cargo install --locked --path blockzilla/cli --bin blockzilla --debug
```

The first build compiles about 350 crates. It took 8 minutes on an Apple M1.
`--debug` makes the build faster. Reads in this guide are still fast.

Check the install:

```bash
blockzilla --version
```

If your shell cannot find `blockzilla`, add `~/.cargo/bin` to your `PATH` and
open a new terminal.

> The crates.io package is not published yet. After the release, step 1 becomes
> `cargo install blockzilla --locked`.

## 2. Stream an epoch

Read the first five blocks of epoch 200 (July 2021):

```bash
blockzilla stream --epoch 200 --max-blocks 5
```

Each line is one block:

```json
{"epoch":200,"block_ordinal":0,"slot":86400000,"transactions":0}
{"epoch":200,"block_ordinal":1,"slot":86400001,"transactions":608}
{"epoch":200,"block_ordinal":2,"slot":86400002,"transactions":143}
{"epoch":200,"block_ordinal":3,"slot":86400003,"transactions":458}
{"epoch":200,"block_ordinal":4,"slot":86400005,"transactions":775}
```

The first run takes about 20 seconds: the reader downloads the epoch index and
key registry (65 MB) into `.blockzilla-cache`. Later reads of the same epoch
start at once. Slot 86400004 is missing because that slot has no block.

Remove `--max-blocks` to stream the whole epoch.

## 3. Export real data

Export the USDC balances that Solana recorded in the first 20 blocks:

```bash
blockzilla dump-tokens --epoch 200 --max-blocks 20 \
  --mint EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v > usdc.jsonl
```

This writes 1,334 rows in about 4 seconds. One row:

```json
{"source_id":"731ea4e9…","epoch":200,"block_ordinal":1,"slot":86400001,"tx_index":8,"execution_status":"succeeded","side":"post","balance_index":2,"account_index":9,"mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","owner":null,"token_program":null,"amount":"121034816016","decimals":6}
```

`amount` is the raw amount: 121034816016 with 6 decimals is 121,034.816016 USDC.
See [Token dumper](token-dumper.md) for all fields.

## Next

| I want to… | Go to |
| --- | --- |
| Read other epochs and set options | [Read an epoch](read-an-archive.md) |
| Process transactions in Rust | [Rust callbacks](callbacks.md) |
| Keep token balances in SQLite | [Token index](token-indexer.md) |
| Load ClickHouse | [ClickHouse](clickhouse.md) |
| Follow new blocks | [Live stream](live-stream.md) |

For full-epoch scans, install without `--debug`. The build takes longer, and
decoding is faster.

## If something fails

| Problem | Fix |
| --- | --- |
| `rustc` is older than 1.98 | Run `rustup update`. |
| `linker 'cc' not found` or a C build error | Install the C compiler (see [Before you start](#before-you-start)). |
| `No space left on device` | Free about 3 GB and run the install again. |
| `blockzilla: command not found` | Add `~/.cargo/bin` to `PATH`, then open a new terminal. |
| `cargo install` cannot find `Cargo.toml` | Run it from the `blockzilla` folder that `git clone` created. |
| HTTP 404 for an epoch | Use an [available epoch](read-an-archive.md#available-epochs). |
| No output for a while | The reader is downloading the index and key registry. For large epochs this is up to 2.2 GB. |
| The command stops with an error | The output file is incomplete. Run the command again. |
