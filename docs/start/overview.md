# Blockzilla

Read Solana history from a compact archive on Cloudflare. The reader runs on
your machine: it downloads only the byte ranges it needs and gives your code
blocks and transactions in order.

You do not need a validator, an RPC node, or archive files of your own.

## Start here

**[Quick start](quick-start.md)**: install the CLI and read your first blocks.

## Choose your path

| I want to… | Go to |
| --- | --- |
| Read an epoch from the command line | [Read an epoch](read-an-archive.md) |
| Process transactions in Rust | [Rust callbacks](callbacks.md) |
| Select a program or a token | [Local filters](filters.md) |
| Export token balances | [Token dumper](token-dumper.md) |
| Keep token balances in SQLite | [Token index](token-indexer.md) |
| Save a program's transactions in SQLite | [Program dumper](program-dumper.md) |
| Load data into ClickHouse | [ClickHouse](clickhouse.md) |
| Follow new blocks live | [Live stream](live-stream.md) |
| Move a Jetstreamer workload | [From Jetstreamer](jetstreamer.md) |
| Understand the file format | [File formats](../advanced/formats.md) |

## How it works

```text
History:  archive on Cloudflare ──► reader on your machine ──► your code or database
Live:     Blockzilla relay ──► SDK on your machine (filters run here) ──► your code
```

Every client receives the same data. Filters, subscriptions, and hooks run on
your machine, not on a provider's server.

## What works today

| Feature | Status |
| --- | --- |
| CLI: `stream`, `dump-tokens`, `index-tokens`, `token-db` | Works. Install from source; crates.io release pending. |
| Rust reader with callbacks and filters | Works. Source dependency; crates.io release pending. |
| Archive epochs | Sample epochs 0, 100, 200 … 1000. More epochs later. |
| Live stream (Node.js, browser, Rust) | Private beta. Needs a consumer key. |
| ClickHouse example | Export works. SQL not yet tested on a server. |
| Superbank ingestion | Planned. |

The data comes with its gaps visible: a missing or unknown value is never
reported as empty or successful. See [Data and coverage](../advanced/coverage.md).
