# Blockzilla DEX parser

This crate classifies Solana DEX instructions from borrowed archive data. It is
made for the SPYx scan path.

## Hot-path rules

- The decoder reads borrowed instruction bytes and borrowed compact account IDs.
- One instruction decode does not allocate.
- The decoder does not clone public keys or make text values.
- Bounds checks reject short data and short account lists.
- The registry dispatch table is dense. It is built once for an epoch and can
  reuse its allocation for the next epoch.
- Program matching does not hash a public key for each instruction.

## Semantic boundary

The decoder identifies a swap, route, or order instruction. It also identifies
account roles and declared amount limits when a verified layout supplies them.
It does not call a route a venue swap. It does not treat an order-book action as
an AMM swap.

A downstream reducer must use successful CPI token transfers and pre/post token
balances before it publishes executed amounts, price, or volume. A declared
minimum output or maximum input is not an executed amount.

Older programs without a verified layout do not emit semantic account or
amount claims. A structural decoder can report a proven discriminator, but it
does not claim that it validated the complete instruction body. It sets
`Evidence::STRUCTURAL_ONLY`, so a reducer can reject it unless a successful
token-flow proof supplies the missing semantics.

## Basic use

```rust
use blockzilla_dex_parser::{DecodeOutcome, DispatchTable};

let table = DispatchTable::from_resolver(registry_len, |address| {
    registry.compact_id(address)
});

match table.decode(program_id, instruction_data, account_ids) {
    DecodeOutcome::Decoded(instruction) => {
        // Reconcile instruction with committed token flow.
    }
    DecodeOutcome::Unsupported { .. }
    | DecodeOutcome::Malformed(_)
    | DecodeOutcome::UnknownProgram => {}
}
```

`DispatchTable::rebind` lets a long scan reuse the same dense table allocation
when it changes to a new epoch registry.
