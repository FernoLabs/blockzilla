# Token API example

`blockzilla-token-api` builds a local token and swap index from Archive V2 and
serves Birdeye-shaped HTTP routes. It is an experimental example, not a
supported market-data service or part of Blockzilla's archive contract.

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

Report security issues using the repository [security policy](../../SECURITY.md).
