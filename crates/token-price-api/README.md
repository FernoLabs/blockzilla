# Blockzilla Token API

`blockzilla-token-api` is the first token indexer and Birdeye-shaped price API
for Blockzilla Archive V2 hot blocks.

It has two modes:

```bash
cargo run --release -p blockzilla-token-api -- \
  index-archive-v2 /data/blockzilla-v2/epoch-700/archive-v2-blocks.zstd /data/token-api/epoch-700

cargo run --release -p blockzilla-token-api -- \
  serve /data/token-api/epoch-700 --listen 127.0.0.1:8080
```

Then open `http://127.0.0.1:8080/` for the built-in archival token browser.

The indexer reads `archive-v2-blocks.zstd` plus `archive-v2-blocks.index` and
`registry.bin`, then writes compact fixed-width files:

- `pubkeys.bin`: observed global registry id to pubkey rows.
- `tokens.bin`: observed mint/program/decimal summary rows.
- `token_accounts.bin`: observed token account to mint/owner rows.
- `balance_changes.bin`: pre/post token-balance observations by transaction.
- `swaps.bin`: owner-level swap candidates inferred from token balance deltas.
- `meta.json`: format, input, quote mint, and row-count metadata.

The initial API intentionally mirrors the common Birdeye DeFi routes:

- `GET /defi/networks`
- `GET /defi/price?address=<mint>`
- `GET|POST /defi/multi_price`
- `GET /defi/history_price?address=<mint>&type=1m`
- `GET /defi/ohlcv?address=<mint>&type=1m`
- `GET /defi/token_overview?address=<mint>`
- `GET /defi/tokenlist`
- `GET /defi/txs/token?address=<mint>`
- `GET /defi/v3/token/txs?address=<mint>`
- `GET /defi/v3/ohlcv?address=<mint>&type=1m`

Price derivation is conservative in this first version: swaps against configured
quote mints are used as USD prices. By default the quote set is USDC and USDT on
Solana. Add more quote mints with repeated `--quote-mint` flags when indexing.

## DEX decoding

DEX support is built around `blockzilla_token_api::dex::DexRegistry`. The default
catalog is seeded from the public Carbon decoder list and Vixen parser coverage,
with Birdeye overlap marked where the public docs name the venue. It currently
tracks the major Solana venues we need first: Jupiter, Raydium, Orca, Meteora,
Pump.fun/PumpSwap, Phoenix, OpenBook, Lifinity, Drift, OKX, PancakeSwap,
Bonkswap, Fluxbeam, Marinade, Zeta, and the newer Vixen-covered launch venues.

The built-in `BalanceDeltaDexDecoder` emits swap candidates from owner-level
two-token balance deltas. Program-specific decoders can be added without touching
the indexer loop:

```rust
use blockzilla_token_api::{
    dex::{DexDecoder, DexRegistry, DexTxContext, ResolvedDexProgram},
    format::SwapRecord,
};

struct MyDexDecoder;

impl DexDecoder for MyDexDecoder {
    fn name(&self) -> &'static str {
        "my-dex"
    }

    fn decode(&self, registry: &DexRegistry, ctx: DexTxContext<'_>, out: &mut Vec<SwapRecord>) {
        for instruction in ctx.instructions() {
            let Some(program_id) = ctx.instruction_program_id(instruction) else {
                continue;
            };
            if registry.program(program_id).is_none() {
                continue;
            }

            // Parse instruction.data here and push SwapRecord rows into out.
        }
    }
}

let mut registry = DexRegistry::new(vec![ResolvedDexProgram::custom(
    123,
    "YourProgram1111111111111111111111111111111111".to_string(),
)]);
registry.register_decoder(MyDexDecoder);
```

For one-off archival runs, add extra router/program ids with repeated
`--dex-program <program_id>` flags. Those programs are registered as manual DEX
entries and will be tagged by the balance-delta decoder immediately.
