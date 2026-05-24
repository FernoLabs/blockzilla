# Archive V2 Hot Block Format

Status: design proposal

This proposal summarizes the current Archive V2 format optimization direction. The main read unit remains a whole block. Runtime metadata stays colocated with the block for now, but the binary layout should make metadata cheap to skip after decompression. Cold, high-entropy, or audit-only data should move out of the hot block blob.

## Goals

- Keep one independently compressed zstd frame per block.
- Optimize the hot path for full-block reads and block scans.
- Let stream and zero-copy readers parse block headers and transaction messages without allocating metadata structures.
- Remove transaction signatures from the compressed block payload.
- Keep enough sidecar data to reconstruct full transactions, serve signature lookups, and validate/replay when needed.
- Keep block blobs simple: block payloads should not be wrapped in a larger archive-record enum.

## Non-Goals

- Do not physically split all runtime metadata into a separate `runtime.bin` in the first iteration.
- Do not optimize for random byte seeking inside a compressed block frame.
- Do not store CAR reconstruction or build diagnostics in the hot block blob.
- Do not make the block file self-indexing with embedded index records.

## File Layout

The target epoch directory is:

```text
epoch-N/
  registry.bin
  registry_counts.bin
  blockhash_registry.bin
  vote_hash_registry.bin
  poh.wincode

  archive-v2-blocks.zstd
  archive-v2-blocks.index
  archive-v2-meta.wincode

  signatures.bin
  signature.index/
```

`archive-v2-blocks.zstd` contains concatenated independently compressed block blobs. `archive-v2-blocks.index` is the authoritative block offset table. `archive-v2-meta.wincode` stores archive-level records such as header, genesis, and footer.

For production batches, store the completed epoch directories under one stable archive root such as `<archive-root>/blockzilla-v2/epoch-N/`. Builders should process epochs sequentially enough that `epoch-(N-1)` is available before `epoch-N` starts. When the current epoch needs the previous recent-blockhash window, it should read `epoch-(N-1)/blockhash_registry.bin` plus `archive-v2-blocks.index` when available. If the hot index is missing, the fallback is the brutal blockhash scan: stream the previous CAR, remember the latest Entry hash, and append that hash whenever the next Block node arrives.

`signatures.bin` stores raw transaction signatures in archive transaction order. `signature.index/` maps signatures to block-local transaction positions.

`vote_hash_registry.bin` stores vote-only hash side data discovered while reading transactions. This is separate from `blockhash_registry.bin`: vote-state `hash` is the bank hash checked against the `SlotHashes` sysvar, and TowerSync `block_id` is Agave's chain block-id hash. Those are not the PoH entry hash used as the transaction recent blockhash.

## Block Index

The zstd block index is the first lookup structure for block reads.

```text
ArchiveV2BlockIndexHeader {
  magic
  version
  row_count
  blob_file_bytes
  flags
}

ArchiveV2BlockIndexRow {
  block_id: u32
  slot: u64
  compressed_offset: u64
  compressed_len: u32
  uncompressed_len: u32
  tx_count: u32
  first_tx_ordinal: u64
  first_signature_ordinal: u64
  signature_count: u32
}
```

`block_id` is dense and epoch-local. The row order is block order, so `block_id` can be used as a direct row index after validation.

The index should be external only. Embedded `Index` records inside the block stream are not needed once this file exists.

## Block Blob

Each compressed frame decompresses to exactly one block blob, not an enum variant such as `WincodeArchiveV2Record::Block`.

```text
ArchiveV2BlockBlob {
  header: BlockHeaderHot
  tx_count: u32
  tx_rows: [TxRow; tx_count]
  message_bytes: [u8]
  metadata_bytes: [u8]
}
```

The row table is the key reader optimization. A reader can decode the header and table, then choose which payload ranges to materialize. Metadata remains in the same decompressed block, but it is a byte range that can be skipped without walking the metadata schema.

Prefer fixed-width little-endian fields for `TxRow`, so `tx_rows[n]` is O(1). Use compact varints inside the message and metadata payloads where they still help.

```text
TxRow {
  tx_index: u32
  flags: u32

  message_offset: u32
  message_len: u32

  metadata_offset: u32
  metadata_len: u32

  signature_count: u8
  reserved: [u8; 3]
}
```

Offsets are relative to their containing payload region. `metadata_len = 0` means metadata is absent. `signature_count` is normally equal to the message header's required signature count, but storing it in the row lets signature sidecar offsets be computed without parsing the message first.

Suggested `flags`:

```text
HAS_METADATA          = 1 << 0
MESSAGE_V0           = 1 << 1
TX_RAW_FALLBACK      = 1 << 2
METADATA_RAW_FALLBACK = 1 << 3
HAS_RETURN_DATA      = 1 << 4
HAS_LOGS             = 1 << 5
HAS_INNER_IX         = 1 << 6
HAS_TOKEN_BALANCES   = 1 << 7
HAS_LOADED_ADDRESSES = 1 << 8
HAS_ERROR            = 1 << 9
```

The high-level flags let block scanners answer common questions from the row table without decoding full metadata.

## Hot Block Header

The block header should keep only fields that help identify and read the block.

```text
BlockHeaderHot {
  slot: u64
  parent_slot: u64
  blockhash_id: u32
  previous_blockhash_id: u32
  block_time: Option<i64>
  block_height: Option<u64>
  rewards: Option<BlockRewards>
}
```

PoH entries remain in `poh.wincode`, addressed by `block_id`. Shredding metadata remains in `shredding.wincode`, also addressed by `block_id`. Full blockhash bytes remain in `blockhash_registry.bin`.

The current `CompactBlockHeader` fields `poh_entries`, `shredding`, and legacy raw `rewards` should not be populated in the hot block blob when equivalent sidecars or decoded rewards exist.

## Transaction Message Payload

The transaction message payload stores the compact transaction message without signatures.

```text
MessagePayload {
  header: CompactMessageHeader
  account_keys: Vec<CompactPubkey>
  recent_blockhash: OwnedCompactRecentBlockhash
  instructions: Vec<OwnedCompactInstruction>
  address_table_lookups: Vec<OwnedCompactAddressTableLookup> // v0 only
}
```

This is equivalent to the current `OwnedCompactTransaction.message` without the `signatures` field.

Vote program compact instructions may replace raw instruction data with semantic payloads for:

- `CompactUpdateVoteState`
- `CompactUpdateVoteStateSwitch`
- `TowerSync`
- `TowerSyncSwitch`

Their vote-state `hash` and TowerSync `block_id` fields use `VoteHashRef` values:

```text
VoteHashRef =
  Zero
  Block(block_id) // resolves through vote_hash_registry.bin
  Raw([u8; 32])
```

The `Block(block_id)` reference is epoch-local Archive V2 block order, matching the main block index. For cross-epoch or not-yet-known slots, the builder stores the raw hash inline.

If transaction decoding fails, `TX_RAW_FALLBACK` is set and the message payload contains the raw transaction bytes. Diagnostic error strings should go to an audit sidecar, not the hot block blob.

## Metadata Payload

Metadata remains colocated with the block in this iteration.

The important format change is framing: every metadata payload has a length in `TxRow`. A block-info or message-only reader can skip `metadata_len` bytes without parsing `CompactMetaV1`.

Decoded metadata can continue using the current compact metadata model:

```text
CompactMetaV1 {
  err
  fee
  pre_balances
  post_balances
  inner_instructions
  logs
  token_balances
  rewards
  loaded_addresses
  return_data
  compute_units_consumed
  cost_units
}
```

Future metadata improvements should focus on making the inner layout more table-like:

- block-level string/data tables for logs instead of per-transaction tables
- offset tables for log strings and binary data
- compact error codes instead of raw error byte blobs where possible
- separate byte ranges for logs, inner instructions, balances, and loaded addresses

## Block Account Filter Sidecar

Status: benchmark prototype only. This is useful for token/event scanners, wallet history
queries, and any reader that can skip an entire block when none of its tracked accounts are
present.

The sidecar stores the sorted unique registry ids touched by each block:

```text
archive-v2-block-accounts.index
  row[block_id] = { offset: u64, byte_len: u32 }

archive-v2-block-accounts.delta
  block = varint(unique_count) + delta_varint(sorted_account_ids)
```

Readers use the existing `archive-v2-blocks.index` for block offsets and this sidecar only
for the account predicate. A scanner with one target id uses binary search. A scanner with a
tracked account set walks two sorted lists and skips the block if the intersection is empty.

Two flavors are worth keeping separate:

- `message_accounts`: static message keys plus loaded addresses. This is enough for wallet
  history and most account-activity filters.
- `semantic_accounts`: message accounts plus compact metadata references such as token-balance
  mint, owner, and program ids. This catches mint-only filters like an initial USDC scan, but
  requires metadata decode while building the sidecar.

Local benchmark on the 2048-slot epoch-644 USDC window:

```text
blocks=1939 txs=3108414 compressed_block_bytes=322796310
message_accounts sidecar ~= 7.19 MB, 3709 bytes/block
semantic_accounts sidecar ~= 8.02 MB, 4135 bytes/block

message_accounts build/read:
  1 worker: 814k tx/s
  4 workers: 3.32M tx/s
  8 workers: 4.29M tx/s

semantic_accounts build/read:
  4 workers: 1.24M-2.00M tx/s on warmed local cache
```

Filter behavior on the same window:

```text
target=USDC mint:
  message_accounts skips 12.7% blocks / 12.5% txs
  semantic_accounts skips 1.1% blocks / 1.0% txs

target=one discovered USDC token account:
  message_accounts skips 97.2% blocks / 96.5% txs
```

The USDC mint itself is a poor skip key in this launch window because it appears almost
everywhere in token balances. The table becomes much more interesting once the scanner has a
tracked token-account or wallet set.

Format notes:

- Account ids should stay epoch-registry ids, not raw pubkeys.
- Delta-varint encoding looks worthwhile: about 7.2 MB vs 26.2 MB fixed `u32` ids for the
  message-account table in the local window.
- Keep the account sidecar external so block blobs remain stable and so it can be rebuilt with
  different semantics.
- The sidecar must be deterministic and should be validated against block row counts and
  registry length.

These are internal metadata improvements; they do not require physically splitting metadata out of the block frame.

## Signature Sidecar

Signatures are cold, high-entropy data and should not live in `archive-v2-blocks.zstd`.

`signatures.bin` stores every signature as a raw fixed-width row:

```text
signatures.bin:
  [signature_0: [u8; 64]]
  [signature_1: [u8; 64]]
  ...
```

Rows are ordered by block order, transaction order, then signature order inside the transaction.

For block-local reconstruction:

```text
block_row.first_signature_ordinal
tx_signature_offset = sum(tx_rows[0..tx_ordinal].signature_count)
signature_file_offset = (block_row.first_signature_ordinal + tx_signature_offset) * 64
```

For signature lookup:

```text
SignatureLookupRecord {
  fingerprint: u64
  block_id: u32
  tx_ordinal: u32
  signature_index: u8
}
```

`tx_ordinal` is the offset in the block transaction array. This matches the reader's natural access path: signature -> block id -> block blob -> tx row.

The existing `of-signature-index` crate already has the right broad shape: a filter, minimal perfect hash, and compact value table. The value record should change from CAR file offsets to block-local archive positions.

## Reader Paths

Block summary:

1. Read `archive-v2-blocks.index`.
2. Seek to the compressed block frame.
3. Decompress one block.
4. Decode `BlockHeaderHot` and `TxRow` table.
5. Stop.

Messages only:

1. Read and decompress the block.
2. Decode `TxRow` table.
3. Decode only the message byte ranges.
4. Ignore `metadata_bytes` and `signatures.bin`.

Full block / RPC block:

1. Read and decompress the block.
2. Decode messages and metadata ranges.
3. Join signatures from `signatures.bin` if the response requires them.
4. Join PoH or blockhash sidecars only when requested.

Signature lookup:

1. Query `signature.index/`.
2. Get `{ block_id, tx_ordinal, signature_index }`.
3. Read the block index row.
4. Decompress the block.
5. Decode the requested tx row and message/metadata ranges.
6. Read signatures from `signatures.bin` if needed.

## Data Placement Rules

Keep in hot block blob:

- block identity fields
- transaction row table
- compact transaction messages
- metadata, length-framed and skippable
- decoded block rewards if they are commonly served with block reads

Move to sidecars:

- signatures
- PoH entries
- full blockhash bytes
- archive header/genesis/footer
- reconstruction-only data
- build diagnostics and decode error strings
- source byte lengths used only for audit statistics

## Migration Plan

1. Add a message-only transaction type that removes `signatures`.
2. Add `signatures.bin` writer and block-index signature ordinals.
3. Change zstd block repack to serialize raw block blobs, not `WincodeArchiveV2Record::Block`.
4. Replace nested `Vec<WincodeArchiveV2Transaction>` with `tx_rows + message_bytes + metadata_bytes`.
5. Update the reader to support:
   - block summary
   - message-only block read
   - full block read with signature join
   - signature lookup by `{ block_id, tx_ordinal }`
6. Move audit-only fields such as `source_len` and diagnostic strings out of the hot blob.

## Open Questions

- Whether `TxRow` should store `first_signature_delta` directly, or derive it from prior `signature_count` values.
- Whether decoded rewards are hot enough to stay in the block header, or should become another sidecar.
- Whether metadata internals should move to block-level tables before or after the signature sidecar lands.
- Whether this format should be a new Archive V2 flag or a version bump.
