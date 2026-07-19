# Archive V2 hot-block format

Status: **implemented format reference**.

This document describes the hot-block Archive V2 files written and read by
current `main`. The Rust schemas and constants in
[`crates/blockzilla-format/src/v2/mod.rs`](../../crates/blockzilla-format/src/v2/mod.rs)
and
[`crates/blockzilla-format/src/v2/archive.rs`](../../crates/blockzilla-format/src/v2/archive.rs)
are the executable source of truth.

This reference covers the independently addressable hot-block family used by
Blockzilla and Edgezilla. It does not describe the older monolithic semantic
Archive V2 stream, CAR byte-for-byte reconstruction, or a future publication
manifest.

## Versions and encoding

| Contract | Current value |
| --- | --- |
| Hot-block payload version | `2` |
| Block-access payload version | `2` |
| Hot-block index version | `1` |
| Block-access index version | `1` |
| Archive integer encoding | little-endian wincode with unsigned LEB128; signed integers use zigzag LEB128 |
| Hot transaction row size | 28 bytes |
| Maximum block-access payload | 64 MiB |

Wincode schemas encode enums, sequences, and optional fields according to the
workspace-resolved `wincode` implementation. Consumers should use
`blockzilla-format::wincode_leb128_config()` rather than reproduce the schema by
hand.

The metadata, PoH, and shredding streams are framed as:

```text
unsigned-LEB128 u32 payload length
wincode payload bytes
```

The hot-block and block-access blob files are not length-prefixed. Their fixed
indexes provide every byte offset and length.

## Epoch directory

A normal hot-block build produces or consumes the following files. Some
registry preparation and live-finalization modes add internal manifests or
temporary markers that are not part of the reader contract.

| File | Role |
| --- | --- |
| `archive-v2-blocks.zstd` | Concatenated independent zstd frames, one serialized hot block per frame |
| `archive-v2-blocks.index` | Fixed-width block offset and ordinal index |
| `archive-v2-meta.wincode` | Framed archive header, optional genesis record, and footer |
| `registry.bin` | Raw 32-byte pubkeys in registry order |
| `registry_counts.bin` | Registry reference counts used by builders and audits |
| `registry.mphf` | Lookup index for `registry.bin` |
| `blockhash_registry.bin` | Raw 32-byte blockhash rows |
| `blockhash_index_v3.bin` | Slot/blockhash/time lookup index emitted by complete current compactors |
| `block-time-gaps.bin` | Sparse, source-fingerprinted slot/time discontinuities derived from the V3 index |
| `prev_blockhash_tail.bin` | Previous-epoch recent-blockhash window used by builders/readers when required |
| `signatures.bin` | Raw 64-byte signatures in archive transaction order |
| `vote_hash_registry.bin` | Fixed 65-byte vote hash rows |
| `poh.wincode` | Framed PoH records by block ID and slot |
| `shredding.wincode` | Framed shredding records by block ID and slot when source evidence is available |
| `archive-v2-block-access.wincode` | Optional concatenated block-local access payloads |
| `archive-v2-block-access.index` | Optional fixed-width access-payload index |
| `archive-v2-get-block.index` | Optional direct slot-offset table joining hot and access blobs |

`archive-v2-blocks.wincode` and
`archive-v2-blocks.wincode.zst` are implemented repack variants. The former
stores concatenated raw hot-block payloads; the latter compresses that entire
raw stream as one zstd stream. Both use the same hot-block index with the
`RAW_BLOCKS` flag, whose offsets refer to the decompressed raw stream.

## Hot-block index

`archive-v2-blocks.index` begins with a 36-byte little-endian header.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic: ASCII `BZV2HIX1` |
| 8 | 2 | version: `1` |
| 10 | 2 | reserved: `0` |
| 12 | 8 | row count (`u64`) |
| 20 | 8 | indexed blob-file bytes (`u64`) |
| 28 | 4 | zstd level (`i32`) |
| 32 | 4 | flags (`u32`) |

Index flags:

| Bit | Name | Meaning |
| ---: | --- | --- |
| 0 | `DICTIONARY` | Independent frames require the external zstd dictionary selected by the caller |
| 1 | `RAW_BLOCKS` | Rows address raw serialized block payloads instead of independent zstd frames |

The header is followed by exactly `row_count` rows of 52 bytes.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | `block_id: u32` |
| 4 | 8 | `slot: u64` |
| 12 | 8 | `compressed_offset: u64` |
| 20 | 4 | `compressed_len: u32` |
| 24 | 4 | `uncompressed_len: u32` |
| 28 | 4 | `tx_count: u32` |
| 32 | 8 | `first_tx_ordinal: u64` |
| 40 | 8 | `first_signature_ordinal: u64` |
| 48 | 4 | `signature_count: u32` |

The field names retain `compressed_*` for compatibility. With `RAW_BLOCKS`
set, the offset addresses raw payload bytes and readers do not decompress the
range.

Current writers emit rows in block order with dense epoch-local `block_id`
values starting at zero. A reader must validate the exact index length, indexed
blob size, every offset/length range, row order assumptions it relies on, and
the decoded slot/transaction count before trusting a row.

## Hot-block blob

Without `RAW_BLOCKS`, each range in `archive-v2-blocks.zstd` is one complete
zstd frame. Decompression yields one wincode-encoded `ArchiveV2HotBlockBlob`:

```text
ArchiveV2HotBlockBlob {
  header: ArchiveV2HotBlockHeader
  tx_count: u32
  tx_rows: Vec<ArchiveV2HotTxRow>
  message_bytes: Vec<u8>
  metadata_bytes: Vec<u8>
}
```

There is no archive-record enum tag or embedded index around this blob.
`tx_count` must equal `tx_rows.len()` and the matching hot-index row's
`tx_count`.

### Block header

```text
ArchiveV2HotBlockHeader {
  slot: u64
  parent_slot: u64
  blockhash_id: u32
  previous_blockhash_id: u32
  block_time: Option<i64>
  block_height: Option<u64>
  rewards: Option<ArchiveV2HotRewards>
}

ArchiveV2HotRewards {
  num_partitions: Option<u64>
  decoded: Vec<CompactReward>
}
```

Full blockhash bytes live in `blockhash_registry.bin` or the block-access
payload. PoH entries and shredding metadata are sidecars, not header fields in
the current writer.

### Transaction row

`ArchiveV2HotTxRow` has a custom fixed-width 28-byte encoding even though its
surrounding blob uses LEB128 wincode.

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | `tx_index: u32` |
| 4 | 4 | `flags: u32` |
| 8 | 4 | `message_offset: u32` |
| 12 | 4 | `message_len: u32` |
| 16 | 4 | `metadata_offset: u32` |
| 20 | 4 | `metadata_len: u32` |
| 24 | 1 | `signature_count: u8` |
| 25 | 3 | reserved; current writers emit zero |

Message offsets are relative to `message_bytes`; metadata offsets are relative
to `metadata_bytes`. Each `offset + len` must be checked for overflow and must
stay inside its region before decoding. Absent metadata has
`HAS_METADATA == 0` and `metadata_len == 0`.

Transaction flags:

| Bit | Constant |
| ---: | --- |
| 0 | `HAS_METADATA` |
| 1 | `MESSAGE_V0` |
| 2 | `TX_RAW_FALLBACK` |
| 3 | `METADATA_RAW_FALLBACK` |
| 4 | `HAS_RETURN_DATA` |
| 5 | `HAS_LOGS` |
| 6 | `HAS_INNER_IX` |
| 7 | `HAS_TOKEN_BALANCES` |
| 8 | `HAS_LOADED_ADDRESSES` |
| 9 | `HAS_ERROR` |
| 10 | `HAS_COMPACT_VOTE_IX` |

The current primary hot writer requires decoded transactions and normally does
not emit `TX_RAW_FALLBACK`; the bit remains part of the reader schema.
`METADATA_RAW_FALLBACK` means the selected metadata range contains source bytes
rather than `CompactMetaV1`.

### Message and metadata regions

A normal message range contains one `ArchiveV2HotMessagePayload`:

```text
ArchiveV2HotMessagePayload = Legacy {
  header, account_keys, recent_blockhash, instructions
} | V0 {
  header, account_keys, recent_blockhash, instructions,
  address_table_lookups
}
```

`CompactPubkey::Id` resolves through the one-based `registry.bin`; ID zero is
reserved as the raw-pubkey sentinel. `OwnedCompactRecentBlockhash::Id(i32)`
resolves through the current blockhash registry or the previous-epoch tail;
`Nonce([u8; 32])` stores a durable nonce inline.

Instruction data is either preserved (`Raw`, `UnknownSystem`, or
`UnknownVote`) or encoded into one of the implemented Compute Budget, System,
or Vote semantic variants. Unknown/unsupported System and Vote instruction
bytes remain byte-preserving fallbacks.

A normal metadata range contains one `CompactMetaV1` encoded with the same
wincode configuration. The row flags allow scanners to decide whether logs,
inner instructions, token balances, loaded addresses, return data, or an error
are present before decoding that metadata range.

## Signatures and registries

`signatures.bin` is a headerless sequence of 64-byte signatures ordered by hot
index row, transaction row, then signature position. For a block row:

```text
signature byte offset = first_signature_ordinal * 64
signature byte length = signature_count * 64
```

The sum of transaction-row `signature_count` values must equal the hot-index
row's `signature_count`.

`registry.bin` is a headerless sequence of 32-byte pubkeys. IDs are one-based:
ID 1 addresses bytes `0..32`.

`blockhash_registry.bin` is a headerless sequence of 32-byte hashes. The exact
current/previous-epoch interpretation is carried by the compact blockhash
reference type and `prev_blockhash_tail.bin`.

`vote_hash_registry.bin` contains fixed 65-byte rows indexed by Archive V2
block ID:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 1 | presence flags: bit 0 bank hash, bit 1 Agave block-ID hash |
| 1 | 32 | bank hash bytes; ignored when bit 0 is clear |
| 33 | 32 | block-ID hash bytes; ignored when bit 1 is clear |

Vote instructions use `ArchiveV2VoteHashRef::{Zero, Block, Raw}`. `Block(id)`
resolves the appropriate column of row `id`; conflicting or out-of-epoch hashes
remain inline as `Raw`.

## Metadata, PoH, and shredding streams

`archive-v2-meta.wincode` contains framed `ArchiveV2HotMetaRecord` values:

1. `Header { version: 2, flags }`, where `LEB128` is set; first-seen builds
   additionally set `FIRST_SEEN_REGISTRY` and `ALL_PUBKEY_REF_COUNTS`;
2. optional `Genesis(...)` for epoch zero;
3. `Footer(...)` after a successful complete write.

The footer contains build counts and fallback/audit totals. Current completion
logic treats metadata publication as the last visibility boundary in live
finalization; readers must not infer completion from the block file alone.

`poh.wincode` contains framed records:

```text
WincodeArchiveV2PohRecord { block_id, slot, entries }
```

`shredding.wincode` contains framed records when the input supplies shredding
evidence:

```text
WincodeArchiveV2ShreddingRecord { block_id, slot, shredding }
```

Both sidecars are aligned by `block_id` and slot with the hot index. Missing
shredding evidence is not equivalent to an empty verified shred set.

## Block-access payload

The optional block-access files make one block self-contained for serving
without fetching whole epoch registries.

`archive-v2-block-access.wincode` is a concatenation of unframed wincode
`ArchiveV2BlockAccessBlob` values:

```text
ArchiveV2BlockAccessBlob {
  version: u16                         // current: 2
  flags: u32
  blockhash: [u8; 32]
  previous_blockhash: [u8; 32]
  signature_counts: Vec<u8>
  signatures: Vec<u8>
  pubkeys: Vec<{ id: u32, pubkey: [u8; 32] }>
  blockhashes: Vec<{ id: i32, blockhash: [u8; 32] }>
  vote_hashes: Vec<{
    block_id: u32,
    bank_hash: Option<[u8; 32]>,
    block_id_hash: Option<[u8; 32]>
  }>
}
```

Only IDs referenced by that block are included. Pubkey, blockhash, and vote
rows are sorted/deduplicated by their IDs by the current builder. The signature
bytes and counts must agree with the hot block. A payload or advertised access
length above 64 MiB is rejected.

### Block-access index

`archive-v2-block-access.index` has a 32-byte header:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | magic: ASCII `BZV2AIX1` |
| 8 | 2 | version: `1` |
| 10 | 2 | reserved: `0` |
| 12 | 8 | row count (`u64`) |
| 20 | 8 | access blob-file bytes (`u64`) |
| 28 | 4 | flags (`u32`) |

Each row is 32 bytes:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | `block_id: u32` |
| 4 | 8 | `slot: u64` |
| 12 | 8 | `access_offset: u64` |
| 20 | 4 | `access_len: u32` |
| 24 | 4 | `tx_count: u32` |
| 28 | 4 | `signature_count: u32` |

The current builder aligns access rows one-for-one with hot rows.

### Direct getBlock index

`archive-v2-get-block.index` has no header. It contains one 24-byte row for
each slot offset in the epoch (`432,000` rows with the current schedule):

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | hot-block offset (`u64`) |
| 8 | 4 | hot-block length (`u32`) |
| 12 | 8 | block-access offset (`u64`) |
| 20 | 4 | block-access length (`u32`) |

A row is missing when either length is zero; current writers emit all-zero
missing rows. The slot's row number is `slot % 432_000`. The index is valid only
with the exact hot and block-access blobs from which it was built.

## Reader sequence

For an indexed full block read:

1. Select the hot row directly or through the per-slot getBlock row.
2. Validate the selected ranges against their declared blob sizes.
3. Fetch the hot range and decompress it unless `RAW_BLOCKS` is set.
4. Decode `ArchiveV2HotBlockBlob` and validate slot, transaction count, row
   regions, and signature counts.
5. Fetch and decode the matching block-access range when full key/hash/signature
   reconstruction is required.
6. Resolve compact IDs and construct the requested view.

The helper `deserialize_archive_v2_hot_block_blob` reads the current schema and
two legacy hot-block layouts retained for compatibility. Current writers emit
only the schema documented above.

## Change policy

- A changed fixed index layout requires an index version change and matching
  reader/writer fixtures.
- A changed hot or block-access schema requires a payload version change unless
  the existing decoder can distinguish it unambiguously.
- Writers must preserve raw bytes when a semantic instruction or metadata form
  cannot be represented losslessly.
- Publication completeness, object hashes, and generation manifests are a
  separate contract and must not be inferred solely from file presence.

Proposed account-filter indexes, signature lookup indexes, and metadata layout
changes live in the non-normative
[Archive V2 evolution note](../design/archive-v2-evolution.md).
