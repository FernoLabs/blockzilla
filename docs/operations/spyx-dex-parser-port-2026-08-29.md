# SPYx DEX parser port

Date: 2026-08-29

## Result

The old Firescope direct instruction-selector set is now represented by the
`blockzilla-dex-parser` crate. The port covers all 54 program addresses and all
73 explicit selector branches from the 52 old modules that used direct
instruction decoders. It also adds the current OKX v2 router, the current OKX
v3 router, and the Raydium AMM route program. The dispatch set contains 57
unique program addresses, with no missing or duplicate entries.

This is an instruction classifier for the SPYx scan path. It does not publish
a trade from an instruction alone. The event reducer must also prove that the
transaction succeeded and reconcile the instruction with committed CPI token
transfers and pre/post token balances.

## SPYx data reach

The SPYx program registry contains 46 of the 57 supported addresses. Of these
46 addresses, 44 occur as instruction program IDs. Those 44 addresses account
for:

- 20,643,487 instruction occurrences
- 3,734,397 outer instruction occurrences
- 16,909,090 inner instruction occurrences
- 4,828,179 unique candidate transactions

Serum DEX v3 and OpenBook v1 occur in the registry, but they do not occur as
instruction program IDs. The other 11 addresses do not occur in this SPYx
dump. Their parsers are based on the old parser collection, local IDLs,
official source layouts, and strict synthetic tests. They cannot yet have SPYx
archive golden tests.

The 11 absent addresses are Raydium legacy v2, Symmetry v2, legacy 2NZ9, Crema
Finance, GooseFX SSL, Lifinity v1, GooseFX v2, Penguin Finance, Sencha, Cykura,
and Dradex.

## Exact SPYx coverage census

The full scan read 7,311,137 transactions and 84,063,841 stored instruction
occurrences. It read the 14.6 GB dump once. The run completed in 58.7 seconds,
at approximately 237 MiB/s.

Transaction coverage is:

- 4,828,179 transactions contain at least one supported program address. This
  is 66.04% of all transactions.
- 4,706,156 transactions contain at least one decoded instruction. This is
  97.47% of candidate transactions and 64.37% of all transactions.
- 4,520,886 transactions contain at least one decoded semantic venue swap.
  This is 93.64% of candidate transactions and 61.84% of all transactions.
- 3,232,374 successful transactions are parser candidates.
- 2,951,316 successful transactions contain a decoded semantic venue swap.
  This is 91.30% of successful candidate transactions and 59.95% of all
  successful transactions in the dump.

Instruction coverage is:

- 8,860,341 of 10,255,094 venue-address instructions decode. This is 86.40%.
- 3,023,752 of 10,388,393 router-address instructions decode. This is 29.11%.
- 11,884,093 of 20,643,487 combined supported-address instructions decode.
  This is 57.57%.
- 8,833,281 decoded instructions are semantic venue swaps.

The combined 57.57% value is not the best trade-coverage value. Router calls
make up approximately half of the supported-address hits. A router record is
route evidence. It is not a trade. The inner venue call and the committed token
flow are the trade evidence.

The largest unsupported-selector groups are Jupiter v6 (3.742 million), OKX
v3 (3.610 million), Meteora DLMM (805 thousand), Meteora DAMM v2 (203
thousand), and PumpSwap (127 thousand). Jupiter and OKX explain most of the
router gap. Raydium CLMM and Orca Whirlpool both decode more than 97% of their
address hits. PancakeSwap decodes more than 99%.

This census measures parser yield. It does not yet prove SPYx trade recall. A
multi-hop or arbitrage transaction can contain a decoded venue swap that is not
the SPYx leg. The reducer must match each decoded instruction to successful CPI
token transfers and pre/post token balances before it publishes a SPYx trade or
volume.

## Parser levels

Thirty-eight programs have at least one semantic swap or order decoder:

- Raydium CLMM, CPMM, AMM v4, Stable Swap, LaunchLab, and legacy liquidity
  pool v2
- Orca Whirlpool, Orca v1, and Orca v2
- Meteora DLMM, DAMM v2, pools, and Dynamic Bonding Curve
- PancakeSwap, PumpSwap, Pump.fun, Lifinity v1/v2, BonkSwap, Byreal, FluxBeam,
  Saros, Saber, SolFi, ZeroFi, STEPN DEX, Plasma/Gavel, Stabble weighted/stable,
  GooseFX SSL/v2, Penguin Finance, and Cykura
- Phoenix, Serum DEX v3, OpenBook v1/v2, and Dradex order instructions

Fifteen programs are structural-only because their complete amount or account
layout is not proven:

- legacy 9tKE, legacy CTMA, legacy D3BB, and unidentified legacy 2NZ9
- Aldrin v1/v2, Crema CLMM, Crema Finance, 1DEX, Cropper, Invariant, Obric v2,
  Step Finance Swap, Symmetry v2, and Sencha

SolFi also has a structural-only legacy `swap_v1` variant. Its verified
`swap_v2` variant is semantic.

Four programs are routers and are not counted as venue swaps:

- Jupiter v6
- OKX DEX Router v2
- OKX DEX Router v3
- Raydium AMM routing

Router records give attribution and route-container evidence. The reducer must
use the inner venue instruction and token flow as the execution source.

Across all 57 programs, 40 are semantic for every recognized selector, 15 are
structural-only, SolFi is mixed, and Raydium legacy v2 is partial because two
historical vault layouts are ambiguous.

## Hot-path contract

- Instruction data is a borrowed `&[u8]` slice.
- Account IDs are a borrowed `&[u32]` slice.
- A decode returns a fixed-size, copyable value.
- A decode does not allocate, clone a public key, build a string, or use a
  hash table.
- A dense `Vec<Option<Program>>` is built once from the compact registry. Its
  allocation is reused when the scanner binds the next epoch registry.
- Every byte and account read is bounds checked. Invalid bool, enum, option,
  and order-packet values fail closed where the layout is known.
- Variable router route plans are not materialized. The parser validates fixed
  fields and safe vector lower bounds without an allocation.
- The crate forbids unsafe Rust and has no runtime dependency.

## Correctness rules

- A router instruction is `Route`, not `Swap`.
- Phoenix, Serum, OpenBook, and Dradex order instructions are `Order`, not AMM
  swaps.
- Declared minimum output and maximum input values are limits. They are not
  executed amounts.
- Optional Anchor account slots are not reported when the compact-ID decoder
  cannot distinguish a real account from the program-ID sentinel.
- `vault_a` and `vault_b` are used only for fixed protocol sides. Directional
  source and destination vaults use `input_vault` and `output_vault` instead.
- Historical layouts with uncertain vault positions omit those roles instead
  of selecting a likely account.
- Structural recognizers set `Evidence::STRUCTURAL_ONLY`. They prove a known
  selector, not a complete instruction-body schema.
- An unknown discriminator is `Unsupported`. A matching but short or invalid
  known layout is `Malformed`.

The address `routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS` was corrected to
Raydium AMM routing. It is not an OKX router. The old known-program log schema
name remains as a compatibility alias so that this identity correction does
not change the archive wire format.

## Validation

- 104 library tests pass.
- 18 command-line tests pass.
- Strict Clippy passes for all crate targets with warnings denied.
- Workspace formatting passes.
- `blockzilla-format` passes with the `known-program-logs` feature.
- `blockzilla-token-api` passes its compile check after the Raydium route label
  correction.

The old Jupiter and Pump.fun modules used event or inner-instruction
aggregation instead of a direct selector table. Four other old modules also
contained log-derived ticker or executed-event logic. That reducer layer is
intentionally not in this instruction-classifier crate. The SPYx reducer must
derive it from successful CPI token flows, balances, and logs.

## Integration sequence

1. Build one `DispatchTable` after each epoch registry is loaded.
2. Decode outer and inner instructions in their original order.
3. Ignore failed transactions for trade accounting.
4. Prefer a proven inner venue event for executed swap identity. Keep the
   router record for route attribution only.
5. Reconcile the instruction with CPI token transfers and pre/post balances.
6. Emit one canonical trade event with executed input, output, fee, pool,
   trader, signature, slot, and instruction position.
7. Record counters for decoded, unsupported, malformed, and token-flow-rejected
   instructions by program and discriminator.
8. Run an archive golden scan before the parser feeds the Firescope API and UI.

This sequence supplies the base event stream for price candles, volume,
holders, token-account balances, and instruction-level transaction detail.
