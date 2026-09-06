# blockzilla-index-archive-format

This crate is the dependency-light wire boundary for the Blockzilla Index
Archive. Its public module tree mirrors the physical archive tree. A reader can
start at the object path and find its schema, row type, validation, and codec in
the module with the same name.

## Current physical tree

```text
catalog/
  blocks.wincode                fixed 144-byte block rows and block spans
dictionary/
  pubkeys.pages                 canonical pubkey bytes
  blockhashes.pages             canonical non-PoH hash bytes
  account_flags.pages           rebuildable account-role summary
ledger/
  transactions.wincode          replay input plus all six effect locators
runtime/
  inner_instructions.wincode    CPI structure and data
  outcomes.wincode              result, units, fee, and return data
  balances.wincode              pre balances plus sparse post deltas
  token_balances.wincode        one identity plus pre/post amount delta
  logs.wincode                  exact tokenized log lines
  rewards.wincode               transaction rewards
  block_rewards.wincode         block-scoped rewards
indexes/
  slots.idx
  accounts.pages
  programs.pages
  selectors.pages
sidecars/
  signatures.bin
  poh.wincode                   retained framed Wincode with a profile preamble
  shredding.wincode             retained framed Wincode with a profile preamble
  genesis.bin                   epoch 0 only
```

There is no per-epoch signature index. A future signature index is global and
is outside this format.

## Transaction point read

`catalog/blocks.wincode` locates one `TransactionBlock`. Its transaction row
contains the complete replay message. The parallel one-byte `EffectState` is
the fixed part of that logical row. It states which dense effect files contain
a record. `TransactionBlock::effect_files` owns all six locator directories;
the effect files contain only their independently compressed chunks. A point
read uses the state-bit rank inside a 256-transaction chunk and decodes at most
256 Wincode records.

The catalog does not copy the six effect locators. It owns only four block
ranges: transactions, block rewards, PoH, and shredding.

## One-owner rules

- Top-level instruction data exists only in `ledger/transactions.wincode`.
- Inner-instruction data exists only with its inner instruction.
- Return data exists only with its transaction outcome.
- PoH and shredding facts exist only in their retained sidecars.
- A catalog sidecar span is a derived byte range, not a second fact copy.
- `ObjectSpec::canonical_facts` permits one merged object to own several facts.
  Layout validation requires every fact to have exactly one owner.

Whole-record absence is stored in `EffectState` or `FactLocator`, not as an
empty payload. The Outcome bit also proves that the source runtime-metadata
envelope existed, and it requires a Balances record. With that proof, a clear
TokenBalances or Rewards bit is the sole canonical encoding of a known-empty
vector; their dense files reject empty records. A clear Logs bit stays
unavailable because log availability is independent. CPI has explicit
unavailable, not-recorded, source-empty, source-present, backfill-empty, and
backfill-present states.

## Wire rules

Current structured objects use Wincode 0.6.0 with canonical unsigned LEB128,
zigzag signed integers, one-byte tags, and a 64 MiB preallocation guard. Golden
tests freeze field order and bytes. PoH and shredding retain the existing
canonical varint-framed Wincode 0.5.5 records. Their target preambles select one
source grammar, so a reader never uses trial decode.

Archive V2 and CAR conversion belong to migration code outside these current
wire modules.
