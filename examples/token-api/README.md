# Token API example

`blockzilla-token-api` builds a local token and swap index from Archive V2 and
serves Birdeye-shaped HTTP routes. It is an experimental example, not a
supported market-data service or part of Blockzilla's archive contract.

This example reads Compact V2 wire objects directly. It predates the
source-neutral read interface and is not the recommended SDK example. New
examples must keep archive decoding, registry resolution, decompression, and
transport policy inside a reader adapter. See the
[archive format and read SDK guide](../../docs/reference/archive-formats-and-read-sdk.md).

This code is made as a learning entrypoint. It is built for clarity.
Use it as a first parser step, then replace pieces for production work.

## Add a parser (simple path)

The parser flow is small:

1. Read a decoded block.
2. Read one transaction metadata and balance changes.
3. Pass a transaction context to registered decoders.
4. Write found `SwapRecord` rows.

To add a new parser today:

- Add a new `DexProgramSpec` entry in
  `src/dex.rs::DEFAULT_DEX_PROGRAMS` if you want a known DEX ID.
- Add a decoder that implements `DexDecoder`.
- Register that decoder from `DexRegistry::new` or `indexer::index_archive_v2`
  before `process_tx` runs.

For a very simple first parser, start with `BalanceDeltaDexDecoder` style:
use token owner net deltas and emit a swap when one account has two
non-zero mint deltas that move in opposite directions.

## Run

Build an index:

```bash
cargo run --locked -p blockzilla-token-api -- \
  index-archive-v2 \
  /data/blockzilla/epoch-700/archive-v2-blocks.zstd \
  /data/blockzilla-token-api/epoch-700
```

Add `--max-blocks N` for a small trial or `--profile price-api` to omit the full
balance and account data.

Serve it on loopback:

```bash
cargo run --locked -p blockzilla-token-api -- \
  serve /data/blockzilla-token-api/epoch-700 \
  --listen 127.0.0.1:8080
```

Open `http://127.0.0.1:8080/` for the included browser. Run the command with
`--help` for all options.

## Limits

- Swaps and prices are inferred from token-balance deltas. They are incomplete
  and must not be used for trading or financial decisions.
- The HTTP server has no authentication or TLS. Keep it on loopback unless you
  place it behind an appropriate gateway.
- The on-disk format is unstable. Use a fresh output directory and retain the
  producing Archive V2 revision.

## Check

```bash
cargo test --locked -p blockzilla-token-api --all-targets
```

Report security issues via the repository maintainer process.
