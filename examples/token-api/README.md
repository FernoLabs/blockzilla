# Token API example

## Purpose and status

`blockzilla-token-api` demonstrates how a local indexer can derive a compact
database from Archive V2 and serve it over HTTP. It is an experimental example,
not a supported market-data service or part of Blockzilla's core archive
contract.

The example reads `archive-v2-blocks.zstd` (or raw hot blocks), the matching
block index, and `registry.bin`. It writes fixed-width token, account, balance,
and swap records that can be reopened without rescanning the archive.

## Commands

Build an index from an existing Archive V2 directory:

```bash
cargo run --locked -p blockzilla-token-api -- \
  index-archive-v2 \
  /data/blockzilla/epoch-700/archive-v2-blocks.zstd \
  /data/blockzilla-token-api/epoch-700
```

For a small trial, add `--max-blocks N`. `--profile price-api` omits the full
balance/account dump and retains the smaller token and swap data set.

Serve the result on loopback:

```bash
cargo run --locked -p blockzilla-token-api -- \
  serve /data/blockzilla-token-api/epoch-700 \
  --listen 127.0.0.1:8080
```

Then open `http://127.0.0.1:8080/` for the included archival token browser. Run
`cargo run --locked -p blockzilla-token-api -- --help` for the current flags.

## Output

The full profile can create:

- `pubkeys.bin`;
- `tokens.bin`;
- `token_accounts.bin`;
- `balance_changes.bin`;
- `swaps.bin`; and
- `meta.json`.

The API exposes Birdeye-shaped network, price, price-history, OHLCV, token-list,
token-overview, and transaction routes so an indexer integration can be tested
against familiar response shapes.

## Important limitations

- Swap candidates are inferred from token-balance deltas; this is not complete
  instruction-level trade attribution.
- USD prices require a swap against a configured quote mint. The defaults cover
  common Solana USDC and USDT mints but are not a general price oracle.
- DEX recognition is an evolving catalog with a generic balance-delta decoder,
  not a promise of complete protocol coverage.
- The HTTP server has no authentication or TLS. Keep it on loopback unless an
  operator deliberately places it behind an appropriate gateway.
- The on-disk index is experimental and can change with the producing commit.

## Source map

| Path | Responsibility |
| --- | --- |
| `src/indexer.rs` | Archive scan and derived-index construction. |
| `src/archive.rs` | Archive V2 input access. |
| `src/format.rs`, `src/store.rs` | Derived record formats and readers. |
| `src/dex.rs` | DEX catalog and swap-candidate decoders. |
| `src/api.rs` | HTTP routes and static browser. |
| `src/main.rs` | `index-archive-v2` and `serve` command definitions. |

## Validation and safety

```bash
cargo check --locked -p blockzilla-token-api --all-targets
cargo test --locked -p blockzilla-token-api --all-targets
```

Use a fresh output directory and retain the source Archive V2 revision. Do not
use experimental prices for trading or financial decisions. Report security
issues using the repository [security policy](../../SECURITY.md).
