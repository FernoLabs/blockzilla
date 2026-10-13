# Local filters

You select data on your own machine. The archive sends the same bytes to
everyone; your request decides what the reader decodes and what your callback
receives.

The examples use the imports and the opened `archive` from
[Rust callbacks](callbacks.md).

## Track a program

Pass the 32-byte program address. This example prints every transaction that
calls the program:

```rust
let request = ScanRequest::all()
    .with_instruction_programs_for([program_id])
    .without_primary_signatures();

let receipt = archive.for_each_block(&request, |block| {
    for tx in block.transaction_views() {
        if tx.instructions.iter().any(|ix| ix.program_id == Some(program_id)) {
            println!("slot={} tx={}", block.header.slot, tx.header.tx_index);
        }
    }
    Ok(())
})?;
```

`program_id` is a `[u8; 32]`. To get it from a base58 address, use
`solana_pubkey::Pubkey::from_str_const("6EF8…").to_bytes()` or the `bs58`
crate.

- The request decodes the program of every top-level and recorded inner
  instruction. It skips the account lists and data bytes.
- Your callback still sees every transaction. The `if` selects the matches.
- To also get the instruction data of that program, add
  `.with_instruction_data_for([program_id])`.

A failed transaction can still contain the instruction. Check
`tx.header.status` before you treat a match as a state change. `Unknown` is not
success.

For a ready-made SQLite dump, see [Program dumper](program-dumper.md).

## Read token balances

Add the mints you want:

```rust
let request = ScanRequest::all()
    .without_instructions()
    .without_primary_signatures()
    .without_required_signers()
    .with_token_balances_for([mint]);

let receipt = archive.for_each_block(&request, |block| {
    for tx in block.transaction_views() {
        for balance in tx.token_balances {
            println!("slot={} amount={}", block.header.slot, balance.amount);
        }
    }
    Ok(())
})?;
```

`tx.token_balances` contains the pre- and post-balances that Solana recorded
for that mint. Use `.with_token_balances()` for all mints.

- These are balances at transaction boundaries, not a full feed of account
  updates.
- A transaction can change a token balance without listing the mint as an
  account. Do not filter on account keys before you look at the balances.

The [token dumper](token-dumper.md) does this for you and writes JSON Lines.

## Missing data

By default, the scan stops when the archive cannot supply data that your
request requires. This keeps a gap visible.

Some low-level selectors return three results: `Match`, `NoMatch`, and
`Indeterminate`. Treat `Indeterminate` as "maybe". Do not drop it as a
non-match.

See [Data and coverage](../advanced/coverage.md) for the rules.
