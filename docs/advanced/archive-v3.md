# Canonical Archive V3

Canonical V3 separates signed transaction data from recorded execution effects. Its local reader can read one block or add the runtime effect columns for that block.

**Current boundary:** the converter produces physical candidates. Canonical V3 does not yet implement the common callback interface or HTTPS source. The frozen `IndexerV3Archive` prototype is a different format.

## Physical files

```text
catalog/
  blocks.wincode
dictionary/
  pubkeys.pages
  blockhashes.pages
  account_flags.pages
ledger/
  transactions.wincode
runtime/
  inner_instructions.wincode
  outcomes.wincode
  balances.wincode
  token_balances.wincode
  logs.wincode
  rewards.wincode
  block_rewards.wincode
indexes/
  slots.idx
  accounts.pages
  programs.pages
  selectors.pages
sidecars/
  signatures.bin
  poh.wincode
  shredding.wincode
  genesis.bin                 # epoch 0 only
```

`dictionary/account_flags.pages` and the four index files are derived. Signature bytes occur only in the signature sidecar. A global signature lookup index is outside this per-archive format.

## Common file header

Headered binary objects start with a 64-byte envelope. The exact genesis source file remains unwrapped.

| Byte offset | Bytes | Field |
| --- | --- | --- |
| 0 | 8 | Magic: ASCII `BZIAFILE` |
| 8 | 2 | Format major, current value `1` |
| 10 | 2 | Object schema version |
| 12 | 2 | Object role code |
| 14 | 2 | Flags, current value `0` |
| 16 | 4 | Header length, current value `64` |
| 20 | 4 | Reserved, must be zero |
| 24 | 8 | Record count |
| 32 | 8 | Decoded byte count |
| 40 | 16 | Archive ID |
| 56 | 8 | Payload byte count |

Fixed integers are little-endian. The archive family is called V3, but its current envelope `format_major` is `1`. Do not put `3` in this field.

Validate the role, schema, archive ID, and exact file length. For a headered object, `file_bytes = 64 + payload_bytes`. Files from different archive IDs must not be combined.

The exact implementation is [header.rs](../../crates/archive-v3/blockzilla-archive-v3/src/header.rs). The role and schema table is in [layout.rs](../../crates/archive-v3/blockzilla-archive-v3/src/layout.rs).

## Block catalog

`catalog/blocks.wincode` has fixed 144-byte rows after the common file header. The row's Wincode payload uses 139 bytes. The last five bytes are zero padding.

For block ordinal `n`:

```text
catalog byte offset = 64 + n × 144
```

Each row contains the slot, parent slot, first transaction ordinal, transaction count, hash references, optional time and height, first signature ordinal, and four block-level locations:

| Location | Target |
| --- | --- |
| Transactions | `ledger/transactions.wincode` |
| Block rewards | `runtime/block_rewards.wincode` |
| PoH | `sidecars/poh.wincode` |
| Shredding | `sidecars/shredding.wincode` |

A `PageSpan` contains an offset, stored length, and decoded length. A `FactLocator` states `Unavailable`, `Source(span)`, or `Backfilled(span)`. Source and backfilled facts keep different provenance.

The catalog does not duplicate transaction effect locators. See [catalog/blocks.rs](../../crates/archive-v3/blockzilla-archive-v3/src/catalog/blocks.rs) for the exact row codec.

## Transaction data and effect locators

`TransactionBlock` owns the transaction rows, one-byte effect states, row restart positions, and all six effect locator directories. A restart occurs every 32 transaction rows. Runtime effects are addressed in chunks of 256 transaction positions.

| Effect file | Contents |
| --- | --- |
| `inner_instructions.wincode` | Recorded CPI groups and instruction bytes |
| `outcomes.wincode` | Error, execution units, fee, and return data |
| `balances.wincode` | Pre-balances and sparse post-balance deltas |
| `token_balances.wincode` | Token identity and pre/post amount changes |
| `logs.wincode` | Exact tokenized log lines |
| `rewards.wincode` | Transaction rewards |

Each effect file contains independent compressed chunks. A point read uses the effect-state rank within a chunk to select its record. It decodes at most 256 records from that effect chunk.

## One-byte effect state

Bits 0–2 store the CPI state:

| Value | CPI state |
| --- | --- |
| 0 | Unavailable |
| 1 | Not recorded |
| 2 | Source empty |
| 3 | Source present |
| 4 | Backfill empty |
| 5 | Backfill present |
| 6–7 | Invalid |

The remaining bits state whether a dense effect record exists:

| Bit | Effect |
| --- | --- |
| 3 | Outcome |
| 4 | Balances |
| 5 | Token balances |
| 6 | Logs |
| 7 | Rewards |

An Outcome record proves that the source runtime-metadata envelope existed and requires a Balances record. With Outcome present, a clear TokenBalances or Rewards bit means a known-empty vector. Without that proof, absence is not known-empty. A clear Logs bit remains unavailable.

CPI source-empty and backfill-empty states have no dense payload. They still prove recorded emptiness. The [transaction schema](../../crates/archive-v3/blockzilla-archive-v3/src/ledger/transactions.rs) defines the validation rules.

## Read one block

From the repository root, use an existing canonical candidate and a slot that is in its catalog:

```bash
cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin ia-read -- /path/to/candidate 388800000
```

Replace the path and slot with your data. Add `--full` to include runtime effect columns:

```bash
cargo run --release --locked -p blockzilla-read-archive-v3 \
  --bin ia-read -- /path/to/candidate 388800000 --full
```

The reader sequence is:

1. Open and validate the catalog with the archive ID.
2. Find the slot and its transaction span.
3. Read the transaction block and select transaction rows.
4. Resolve keys and hashes through their owning files when needed.
5. Read selected runtime effect chunks when requested.

## Conversion status

```bash
cargo run --release --locked -p blockzilla -- archive-v3 convert \
  /path/to/compact-v2-generation /path/to/new-candidate \
  --workers 4 --pipeline-memory-limit-mib 512
```

Use a complete compatible source and a new output directory. The converter requires exact signed-message reconstruction and the required source sidecars. The quick-start one-block build is not a supported complete conversion source.

The report marks the result `complete-physical-candidate-not-publishable`. It is not an active published archive. Keep the V2 source until the publication and verification gates are complete.

The [converter guide](../../crates/archive-v3/blockzilla-archive-v3-convert/README.md) gives source profiles, conversion checks, and index rebuild commands. The [format crate guide](../../crates/archive-v3/blockzilla-archive-v3/README.md) gives the one-owner rules.
