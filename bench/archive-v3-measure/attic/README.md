# Measurement binaries awaiting a port

These two read the **pre-refactor** layout: `catalog/blocks.tbl` with fourteen
per-column page spans, and `ledger/{core,accounts,instructions}.pages`. The
merged-record refactor replaced both with `catalog/blocks.wincode` and
`ledger/transactions.wincode`, so they no longer compile and could not read a
current generation if they did.

They are parked here rather than deleted because their measurements are cited in
`docs/design/measurements/`:

- `account-relations.rs` — signer / program / derived classification and the
  account→program relation. Cited by `account-classification.md`.
- `index-shape.rs` — posting collapse at block granularity, reference skew by
  registry ordinal, and account bytes as a share of the ledger. Cited by
  `where-the-bytes-are.md`.

Porting is mechanical once the layout settles: read the block through
`ledger::transactions::decode_block`, then walk `Message::static_accounts` plus
`LoadedAddresses` where the old code walked an `AccountGroup`, and
`Message::instructions` where it walked a decoded instructions page. The fixture
must also be regenerated with the current converter, since the numbers above
came from a generation written in the old layout.
