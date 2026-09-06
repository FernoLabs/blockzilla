use core::mem::MaybeUninit;

use of_car_reader::{
    node::{Shredding, SlotMeta},
    reconstruct::{
        Cid36, NodeLocation, RawBlockNode, RawCidRef, RawDataFrame, RawEntryNode, RawEpochNode,
        RawNode, RawRewardsNode, RawSubsetNode, RawTransactionNode, StandaloneDataFrame,
    },
};
use serde::{Deserialize, Serialize};
use wincode::{
    ReadResult, SchemaRead, SchemaWrite, WriteResult,
    config::Config,
    error::invalid_value,
    int_encoding::{ByteOrder, IntEncoding},
    io::{Reader, Writer},
    len::SeqLen,
};

use crate::CompactLogStream;
use crate::{
    CompactBlockHeader, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
    CompactPubkey, CompactReward, CompactShredding, CompactTransactionConfig,
    CompactTransactionError, OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash,
    OwnedCompactTransaction, SplitCompactIndexRecord, WincodeLeb128Config, wincode_leb128_config,
};

mod archive;
pub use archive::*;

mod block_time_gaps;
pub use block_time_gaps::*;

/// Maximum one-sequence allocation admitted by the Archive V2 object decoders.
///
/// This matches the existing registry-reprocess retained-sequence limit. It
/// prevents a short hostile input from using a forged sequence count to
/// request an unbounded allocation before input exhaustion.
pub const ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES: usize = 64 * 1024 * 1024;

mod wire_rewrite;
pub use wire_rewrite::*;

pub const WINCODE_LOG_ARCHIVE_V2_VERSION: u16 = 2;
pub const WINCODE_LOG_ARCHIVE_KEYS_FREQUENCY_SORTED: u32 = 1 << 0;
pub const WINCODE_ARCHIVE_V2_VERSION: u16 = 2;
pub const WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION: u16 = 2;
pub const WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION: u16 = 2;
/// Maximum serialized size of one block-local access payload.
///
/// Producers and consumers share this bound so an index can never advertise a frame that a
/// validator refuses to allocate or decode.
pub const ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES: u64 = 64 * 1024 * 1024;
pub const WINCODE_BLOCKZILLA_GET_BLOCK_BUNDLE_VERSION: u16 = 1;
/// Archive records use unsigned LEB128 integer encoding.
pub const WINCODE_ARCHIVE_V2_FLAG_LEB128: u32 = 1 << 0;
/// Archive blocks contain raw pubkeys rather than a finalized pubkey registry.
pub const WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY: u32 = 1 << 1;
/// Registry IDs use seeded first-seen order rather than same-epoch frequency order.
pub const WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY: u32 = 1 << 2;
/// Registry counts include every typed `CompactPubkey` reference, including rewards and logs.
pub const WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS: u32 = 1 << 3;
pub const ARCHIVE_V2_HOT_TX_ROW_LEN: usize = 28;

#[derive(Debug, Clone, Copy)]
pub struct Leb128;

unsafe impl<B: ByteOrder> IntEncoding<B> for Leb128 {
    const STATIC: bool = false;
    const ZERO_COPY: bool = false;

    #[inline]
    fn encode_u16(val: u16, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(val as u128, writer)
    }

    #[inline]
    fn size_of_u16(val: u16) -> usize {
        unsigned_leb128_size(val as u128)
    }

    #[inline]
    fn decode_u16<'de>(reader: impl Reader<'de>) -> ReadResult<u16> {
        Ok(decode_unsigned_leb128(reader, u16::BITS)? as u16)
    }

    #[inline]
    fn encode_u32(val: u32, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(val as u128, writer)
    }

    #[inline]
    fn size_of_u32(val: u32) -> usize {
        unsigned_leb128_size(val as u128)
    }

    #[inline]
    fn decode_u32<'de>(reader: impl Reader<'de>) -> ReadResult<u32> {
        Ok(decode_unsigned_leb128(reader, u32::BITS)? as u32)
    }

    #[inline]
    fn encode_u64(val: u64, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(val as u128, writer)
    }

    #[inline]
    fn size_of_u64(val: u64) -> usize {
        unsigned_leb128_size(val as u128)
    }

    #[inline]
    fn decode_u64<'de>(reader: impl Reader<'de>) -> ReadResult<u64> {
        Ok(decode_unsigned_leb128(reader, u64::BITS)? as u64)
    }

    #[inline]
    fn encode_u128(val: u128, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(val, writer)
    }

    #[inline]
    fn size_of_u128(val: u128) -> usize {
        unsigned_leb128_size(val)
    }

    #[inline]
    fn decode_u128<'de>(reader: impl Reader<'de>) -> ReadResult<u128> {
        decode_unsigned_leb128(reader, u128::BITS)
    }

    #[inline]
    fn encode_i16(val: i16, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(zigzag_i16(val) as u128, writer)
    }

    #[inline]
    fn size_of_i16(val: i16) -> usize {
        unsigned_leb128_size(zigzag_i16(val) as u128)
    }

    #[inline]
    fn decode_i16<'de>(reader: impl Reader<'de>) -> ReadResult<i16> {
        Ok(unzigzag_i16(
            decode_unsigned_leb128(reader, u16::BITS)? as u16
        ))
    }

    #[inline]
    fn encode_i32(val: i32, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(zigzag_i32(val) as u128, writer)
    }

    #[inline]
    fn size_of_i32(val: i32) -> usize {
        unsigned_leb128_size(zigzag_i32(val) as u128)
    }

    #[inline]
    fn decode_i32<'de>(reader: impl Reader<'de>) -> ReadResult<i32> {
        Ok(unzigzag_i32(
            decode_unsigned_leb128(reader, u32::BITS)? as u32
        ))
    }

    #[inline]
    fn encode_i64(val: i64, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(zigzag_i64(val) as u128, writer)
    }

    #[inline]
    fn size_of_i64(val: i64) -> usize {
        unsigned_leb128_size(zigzag_i64(val) as u128)
    }

    #[inline]
    fn decode_i64<'de>(reader: impl Reader<'de>) -> ReadResult<i64> {
        Ok(unzigzag_i64(
            decode_unsigned_leb128(reader, u64::BITS)? as u64
        ))
    }

    #[inline]
    fn encode_i128(val: i128, writer: impl Writer) -> WriteResult<()> {
        encode_unsigned_leb128(zigzag_i128(val), writer)
    }

    #[inline]
    fn size_of_i128(val: i128) -> usize {
        unsigned_leb128_size(zigzag_i128(val))
    }

    #[inline]
    fn decode_i128<'de>(reader: impl Reader<'de>) -> ReadResult<i128> {
        Ok(unzigzag_i128(decode_unsigned_leb128(reader, u128::BITS)?))
    }
}

#[inline]
fn unsigned_leb128_size(mut value: u128) -> usize {
    let mut size = 1usize;
    while value >= 0x80 {
        value >>= 7;
        size += 1;
    }
    size
}

#[inline]
fn encode_unsigned_leb128(mut value: u128, mut writer: impl Writer) -> WriteResult<()> {
    let mut bytes = [0u8; 19];
    let mut len = 0usize;

    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        bytes[len] = byte;
        len += 1;
        if value == 0 {
            break;
        }
    }

    writer.write(&bytes[..len])?;
    Ok(())
}

#[inline]
fn decode_unsigned_leb128<'de>(mut reader: impl Reader<'de>, max_bits: u32) -> ReadResult<u128> {
    let max = if max_bits == u128::BITS {
        u128::MAX
    } else {
        (1u128 << max_bits) - 1
    };
    let max_bytes = max_bits.div_ceil(7) as usize;
    let mut value = 0u128;

    for index in 0..max_bytes {
        let byte = reader.take_byte()?;
        let payload = u128::from(byte & 0x7f);
        let shift = (index * 7) as u32;
        if payload > (u128::MAX >> shift) {
            return Err(invalid_value("LEB128 integer overflow"));
        }
        value |= payload << shift;

        if byte & 0x80 == 0 {
            if value > max {
                return Err(invalid_value("LEB128 integer overflow"));
            }
            return Ok(value);
        }
    }

    Err(invalid_value("LEB128 integer overflow"))
}

macro_rules! zigzag_pair {
    ($encode:ident, $decode:ident, $signed:ty, $unsigned:ty) => {
        #[inline]
        fn $encode(value: $signed) -> $unsigned {
            let unsigned = value as $unsigned;
            unsigned.wrapping_shl(1) ^ ((value >> (<$signed>::BITS - 1)) as $unsigned)
        }

        #[inline]
        fn $decode(value: $unsigned) -> $signed {
            ((value >> 1) as $signed) ^ (-((value & 1) as $signed))
        }
    };
}

zigzag_pair!(zigzag_i16, unzigzag_i16, i16, u16);
zigzag_pair!(zigzag_i32, unzigzag_i32, i32, u32);
zigzag_pair!(zigzag_i64, unzigzag_i64, i64, u64);
zigzag_pair!(zigzag_i128, unzigzag_i128, i128, u128);

/// Header for the log-only wincode benchmark format.
///
/// This is intentionally not a CAR-equivalent archive header. It only describes
/// compacted transaction logs plus the key registry needed by that log stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(C)]
pub struct WincodeLogArchiveHeaderV2 {
    pub version: u16,
    pub flags: u32,
    pub block_count: u64,
    pub tx_count: u64,
    pub tx_with_logs: u64,
    pub log_line_count: u64,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite,
)]
#[repr(C)]
pub struct WincodeTxLogRange {
    /// Transaction ordinal in the scanned fixture.
    pub tx_index: u32,
    /// First log event index for this transaction.
    pub start: u32,
    /// Number of log events emitted by this transaction.
    pub count: u32,
}

/// Log-only wincode archive used by the big-block log benchmark.
///
/// This stores a key registry, transaction-to-log ranges, and parsed log events.
/// It does not store full transaction bytes, metadata bytes, POH entry nodes,
/// block rewards, CAR CIDs, CAR offsets, dataframes, or exact block CBOR. The
/// full CAR-preserving split format below is the source of truth for a
/// wincode/postcard archive that can replace a `.car` file.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeLogArchiveV2 {
    pub header: WincodeLogArchiveHeaderV2,
    pub keys: Vec<[u8; 32]>,
    pub tx_log_ranges: Vec<WincodeTxLogRange>,
    pub logs: CompactLogStream,
}

/// Semantic Solana archive v2, framed one record at a time with wincode/LEB128.
///
/// Unlike `WincodeLogArchiveV2`, this is intended to cover the full Solana data
/// carried by the CAR stream: block/slot metadata, PoH entries, rewards,
/// transactions, and transaction status metadata. It intentionally does not keep
/// CAR header bytes, CIDs, CAR offsets, or exact CBOR section bytes.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum WincodeArchiveV2Record {
    Header(WincodeArchiveV2Header),
    Block(WincodeArchiveV2Block),
    Index(SplitCompactIndexRecord),
    Footer(WincodeArchiveV2Footer),
    Genesis(WincodeArchiveV2Genesis),
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2Header {
    pub version: u16,
    pub flags: u32,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2Genesis {
    pub genesis_hash: [u8; 32],
    pub genesis_bin_len: u64,
    pub creation_time_unix: i64,
    pub cluster_id: u32,
    pub ticks_per_slot: u64,
    pub poh_params: WincodeArchiveV2GenesisPohParams,
    pub fees: WincodeArchiveV2GenesisFeeParams,
    pub rent: WincodeArchiveV2GenesisRentParams,
    pub inflation: WincodeArchiveV2GenesisInflationParams,
    pub epoch_schedule: WincodeArchiveV2GenesisEpochSchedule,
    pub accounts: Vec<WincodeArchiveV2GenesisAccount>,
    pub builtins: Vec<WincodeArchiveV2GenesisBuiltin>,
    pub reward_pools: Vec<WincodeArchiveV2GenesisAccount>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2GenesisAccount {
    pub pubkey: CompactPubkey,
    pub lamports: u64,
    pub owner: CompactPubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2GenesisBuiltin {
    pub key: String,
    pub pubkey: CompactPubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2GenesisPohParams {
    pub tick_duration_secs: u64,
    pub tick_duration_nanos: u32,
    pub tick_count: Option<u64>,
    pub hashes_per_tick: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2GenesisFeeParams {
    pub target_lamports_per_sig: u64,
    pub target_sigs_per_slot: u64,
    pub min_lamports_per_sig: u64,
    pub max_lamports_per_sig: u64,
    pub burn_percent: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2GenesisRentParams {
    pub lamports_per_byte_year: u64,
    pub exemption_threshold: f64,
    pub burn_percent: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2GenesisInflationParams {
    pub initial: f64,
    pub terminal: f64,
    pub taper: f64,
    pub foundation: f64,
    pub foundation_term: f64,
    pub padding: [u8; 8],
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2GenesisEpochSchedule {
    pub slots_per_epoch: u64,
    pub leader_schedule_slot_offset: u64,
    pub warmup: bool,
    pub first_normal_epoch: u64,
    pub first_normal_slot: u64,
}

/// PoH sidecar record for Archive V2.
///
/// Block records intentionally do not carry the full PoH entry list; this
/// sidecar keeps the ordered entries addressable by block id/slot.
#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2PohRecord {
    pub block_id: u32,
    pub slot: u64,
    pub entries: Vec<crate::CompactPohEntry>,
}

/// `WincodeArchiveV2PohRecord` before `CompactPohEntry::signature_count` was added.
#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2PohRecordLegacyNoSignatureCount {
    pub block_id: u32,
    pub slot: u64,
    pub entries: Vec<crate::CompactPohEntryLegacyNoSignatureCount>,
}

impl From<WincodeArchiveV2PohRecordLegacyNoSignatureCount> for WincodeArchiveV2PohRecord {
    fn from(value: WincodeArchiveV2PohRecordLegacyNoSignatureCount) -> Self {
        WincodeArchiveV2PohRecord {
            block_id: value.block_id,
            slot: value.slot,
            entries: value.entries.into_iter().map(Into::into).collect(),
        }
    }
}

/// Decode a `poh.wincode` frame, falling back to the pre-`signature_count` schema.
///
/// `poh.wincode` frames are read one at a time from a fixed-length LEB128 frame (see
/// `WincodeLeb128FramedReader`), so a schema mismatch fails cleanly within that frame's byte
/// slice rather than reading into the next record. Every entry in a legacy-decoded record gets
/// `signature_count: 0`, which callers must treat as "unknown, derive if needed" rather than "no
/// signatures" — `verify-archive-v2-poh` relies on this to fail its cross-check and fall back to
/// decompression instead of silently trusting an unpopulated count.
///
/// This always probes the current schema first. A caller decoding every frame of one sidecar
/// (every frame in a `poh.wincode` shares one schema — it's written by a single archive
/// generation) should use [`deserialize_archive_v2_poh_record_with_schema`] instead: on a
/// legacy sidecar this function's current-schema probe fails and falls back on *every* frame,
/// effectively decoding each frame twice.
pub fn deserialize_archive_v2_poh_record(bytes: &[u8]) -> ReadResult<WincodeArchiveV2PohRecord> {
    let mut schema = PohRecordSchema::Current;
    deserialize_archive_v2_poh_record_with_schema(bytes, &mut schema)
}

/// Which `poh.wincode` frame schema last decoded successfully, for
/// [`deserialize_archive_v2_poh_record_with_schema`]. `Default` starts at `Current` since
/// that's what every freshly built archive writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PohRecordSchema {
    #[default]
    Current,
    LegacyNoSignatureCount,
}

/// Decode a `poh.wincode` frame, trying `schema` first and updating it on a fallback.
///
/// Callers that decode every frame of a single sidecar should hold one `PohRecordSchema`
/// across the whole read loop (starting from `PohRecordSchema::default()`) and pass it here
/// each time, instead of calling [`deserialize_archive_v2_poh_record`] per frame. Every frame
/// in a `poh.wincode` sidecar shares one schema, so after the first frame this makes every
/// later decode single-shot rather than probing the current schema and falling back on every
/// single frame of a legacy (pre-`signature_count`) archive.
///
/// If `schema` stops matching partway through a stream (a malformed or hand-edited sidecar),
/// this still falls back to the other schema before giving up, so it never returns a spurious
/// error just because the cached hint was wrong for one frame — it only costs the double
/// decode on that one frame, then re-settles `schema` for the rest of the stream.
pub fn deserialize_archive_v2_poh_record_with_schema(
    bytes: &[u8],
    schema: &mut PohRecordSchema,
) -> ReadResult<WincodeArchiveV2PohRecord> {
    match schema {
        PohRecordSchema::Current => {
            match wincode::config::deserialize(bytes, wincode_leb128_config()) {
                Ok(record) => Ok(record),
                Err(primary_error) => {
                    match wincode::config::deserialize::<
                        WincodeArchiveV2PohRecordLegacyNoSignatureCount,
                        _,
                    >(bytes, wincode_leb128_config())
                    {
                        Ok(record) => {
                            *schema = PohRecordSchema::LegacyNoSignatureCount;
                            Ok(record.into())
                        }
                        Err(_) => Err(primary_error),
                    }
                }
            }
        }
        PohRecordSchema::LegacyNoSignatureCount => {
            match wincode::config::deserialize::<WincodeArchiveV2PohRecordLegacyNoSignatureCount, _>(
                bytes,
                wincode_leb128_config(),
            ) {
                Ok(record) => Ok(record.into()),
                Err(_) => match wincode::config::deserialize(bytes, wincode_leb128_config()) {
                    Ok(record) => {
                        *schema = PohRecordSchema::Current;
                        Ok(record)
                    }
                    Err(current_error) => Err(current_error),
                },
            }
        }
    }
}

/// Shredding sidecar record for Archive V2.
///
/// Shred boundary metadata is intentionally addressable outside hot block blobs
/// so it can be repaired from CAR or raw shred sources without rewriting
/// independently compressed block frames.
#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2ShreddingRecord {
    pub block_id: u32,
    pub slot: u64,
    pub shredding: Vec<crate::CompactShredding>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2Block {
    pub header: WincodeArchiveV2BlockHeader,
    pub txs: Vec<WincodeArchiveV2Transaction>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2BlockHeader {
    pub compact: CompactBlockHeader,
    pub rewards: Option<WincodeArchiveV2Rewards>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2Rewards {
    pub source_len: u64,
    pub num_partitions: Option<u64>,
    pub decoded: Option<Vec<CompactReward>>,
    pub raw_fallback: Option<Vec<u8>>,
    pub decode_error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2Transaction {
    pub tx_index: u32,
    pub tx: WincodeArchiveV2Payload<OwnedCompactTransaction>,
    pub metadata: Option<WincodeArchiveV2Payload<CompactMetaV1>>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum WincodeArchiveV2Payload<T> {
    Decoded { source_len: u64, value: T },
    Raw { bytes: Vec<u8>, error: String },
}

/// Hot-block Archive V2 payload.
///
/// This is the block-local unit intended to be independently zstd-compressed.
/// It deliberately does not include transaction signatures, PoH entries, CAR
/// reconstruction data, record tags, or embedded index records.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2HotBlockBlob {
    pub header: ArchiveV2HotBlockHeader,
    pub tx_count: u32,
    pub tx_rows: Vec<ArchiveV2HotTxRow>,
    pub message_bytes: Vec<u8>,
    pub metadata_bytes: Vec<u8>,
}

/// A zero-copy view of the allocation-heavy fields in the current hot-block schema.
///
/// The header is decoded by value because rewards are structured data. Transaction rows remain
/// in their canonical contiguous 28-byte wire representation and are decoded by value while
/// iterating. Message and metadata regions borrow directly from the input frame.
///
/// This view recognizes only the current schema. Callers that support historical schemas should
/// fall back to [`deserialize_archive_v2_hot_block_blob`] when this decoder returns an error.
#[derive(Debug)]
pub struct BorrowedArchiveV2HotBlockBlob<'a> {
    pub header: ArchiveV2HotBlockHeader,
    pub tx_count: u32,
    tx_rows: &'a [[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]],
    pub message_bytes: &'a [u8],
    pub metadata_bytes: &'a [u8],
}

/// Replay-oriented zero-copy view of a current hot block.
///
/// Unlike [`BorrowedArchiveV2HotBlockBlob`], this view deliberately does not decode or retain
/// structured block rewards. The decoder still consumes and validates every reward field and
/// element. It lends the exact encoded reward-option field for consumers that must copy the
/// original bytes without re-encoding them.
#[derive(Debug)]
pub struct BorrowedArchiveV2HotBlockBlobWithoutRewards<'a> {
    pub header: ArchiveV2HotBlockHeader,
    pub tx_count: u32,
    tx_rows: &'a [[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]],
    pub message_bytes: &'a [u8],
    pub metadata_bytes: &'a [u8],
    rewards_field_bytes: &'a [u8],
}

impl BorrowedArchiveV2HotBlockBlobWithoutRewards<'_> {
    #[inline]
    pub fn tx_rows_len(&self) -> usize {
        self.tx_rows.len()
    }

    #[inline]
    pub fn tx_rows(&self) -> ArchiveV2HotTxRowIter<'_> {
        ArchiveV2HotTxRowIter {
            rows: self.tx_rows.iter(),
        }
    }

    /// The exact encoded `Option<ArchiveV2HotRewards>` field from the source block.
    #[inline]
    pub fn rewards_field_bytes(&self) -> &[u8] {
        self.rewards_field_bytes
    }
}

impl BorrowedArchiveV2HotBlockBlob<'_> {
    #[inline]
    pub fn tx_rows_len(&self) -> usize {
        self.tx_rows.len()
    }

    #[inline]
    pub fn tx_rows(&self) -> ArchiveV2HotTxRowIter<'_> {
        ArchiveV2HotTxRowIter {
            rows: self.tx_rows.iter(),
        }
    }
}

/// Exact iterator over borrowed hot-block transaction-row wire records.
#[derive(Debug, Clone)]
pub struct ArchiveV2HotTxRowIter<'a> {
    rows: core::slice::Iter<'a, [u8; ARCHIVE_V2_HOT_TX_ROW_LEN]>,
}

impl Iterator for ArchiveV2HotTxRowIter<'_> {
    type Item = ArchiveV2HotTxRow;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(ArchiveV2HotTxRow::from_wire_bytes)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl DoubleEndedIterator for ArchiveV2HotTxRowIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.rows
            .next_back()
            .map(ArchiveV2HotTxRow::from_wire_bytes)
    }
}

impl ExactSizeIterator for ArchiveV2HotTxRowIter<'_> {}

impl core::iter::FusedIterator for ArchiveV2HotTxRowIter<'_> {}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2HotBlockHeader {
    pub slot: u64,
    pub parent_slot: u64,
    pub blockhash_id: u32,
    pub previous_blockhash_id: u32,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub rewards: Option<ArchiveV2HotRewards>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct ArchiveV2HotBlockBlobLegacyShredding {
    header: ArchiveV2HotBlockHeaderLegacyShredding,
    tx_count: u32,
    tx_rows: Vec<ArchiveV2HotTxRow>,
    message_bytes: Vec<u8>,
    metadata_bytes: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct ArchiveV2HotBlockHeaderLegacyShredding {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    shredding: Vec<CompactShredding>,
    rewards: Option<ArchiveV2HotRewards>,
}

impl From<ArchiveV2HotBlockBlobLegacyShredding> for ArchiveV2HotBlockBlob {
    fn from(value: ArchiveV2HotBlockBlobLegacyShredding) -> Self {
        Self {
            header: ArchiveV2HotBlockHeader {
                slot: value.header.slot,
                parent_slot: value.header.parent_slot,
                blockhash_id: value.header.blockhash_id,
                previous_blockhash_id: value.header.previous_blockhash_id,
                block_time: value.header.block_time,
                block_height: value.header.block_height,
                rewards: value.header.rewards,
            },
            tx_count: value.tx_count,
            tx_rows: value.tx_rows,
            message_bytes: value.message_bytes,
            metadata_bytes: value.metadata_bytes,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct ArchiveV2HotBlockBlobLegacyRewardsVec {
    header: ArchiveV2HotBlockHeaderLegacyRewardsVec,
    tx_count: u32,
    tx_rows: Vec<ArchiveV2HotTxRow>,
    message_bytes: Vec<u8>,
    metadata_bytes: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct ArchiveV2HotBlockHeaderLegacyRewardsVec {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    rewards: Vec<CompactReward>,
}

impl From<ArchiveV2HotBlockBlobLegacyRewardsVec> for ArchiveV2HotBlockBlob {
    fn from(value: ArchiveV2HotBlockBlobLegacyRewardsVec) -> Self {
        let rewards = (!value.header.rewards.is_empty()).then_some(ArchiveV2HotRewards {
            num_partitions: None,
            decoded: value.header.rewards,
        });
        Self {
            header: ArchiveV2HotBlockHeader {
                slot: value.header.slot,
                parent_slot: value.header.parent_slot,
                blockhash_id: value.header.blockhash_id,
                previous_blockhash_id: value.header.previous_blockhash_id,
                block_time: value.header.block_time,
                block_height: value.header.block_height,
                rewards,
            },
            tx_count: value.tx_count,
            tx_rows: value.tx_rows,
            message_bytes: value.message_bytes,
            metadata_bytes: value.metadata_bytes,
        }
    }
}

// All shipped hot-block schemas begin with the same outer `header` field and the same fields
// through `block_time`. Decoding only this prefix avoids allocating or validating the transaction,
// message, metadata, shredding, and rewards vectors when a caller only needs archive timing data.
#[derive(SchemaRead)]
struct ArchiveV2HotBlockSlotTimePrefix {
    header: ArchiveV2HotBlockSlotTimeHeaderPrefix,
}

#[derive(SchemaRead)]
struct ArchiveV2HotBlockSlotTimeHeaderPrefix {
    slot: u64,
    _parent_slot: u64,
    _blockhash_id: u32,
    _previous_blockhash_id: u32,
    block_time: Option<i64>,
}

/// Decode only the slot and optional block time shared by current and legacy hot-block payloads.
///
/// The decoder intentionally stops after the common header prefix, so large block-local vectors
/// in the remainder of the payload are neither decoded nor allocated.
pub fn deserialize_archive_v2_hot_block_slot_time(bytes: &[u8]) -> ReadResult<(u64, Option<i64>)> {
    let prefix: ArchiveV2HotBlockSlotTimePrefix =
        wincode::config::deserialize(bytes, wincode_leb128_config())?;
    Ok((prefix.header.slot, prefix.header.block_time))
}

pub fn deserialize_archive_v2_hot_block_blob(bytes: &[u8]) -> ReadResult<ArchiveV2HotBlockBlob> {
    match wincode::config::deserialize(bytes, wincode_leb128_config()) {
        Ok(block) => Ok(block),
        Err(primary_error) => {
            if let Ok(block) = wincode::config::deserialize::<ArchiveV2HotBlockBlobLegacyShredding, _>(
                bytes,
                wincode_leb128_config(),
            ) {
                return Ok(block.into());
            }
            match wincode::config::deserialize::<ArchiveV2HotBlockBlobLegacyRewardsVec, _>(
                bytes,
                wincode_leb128_config(),
            ) {
                Ok(block) => Ok(block.into()),
                Err(_) => Err(primary_error),
            }
        }
    }
}

/// Decode the current hot-block schema while borrowing its large contiguous regions.
///
/// The tuple has the same field-by-field wire layout as [`ArchiveV2HotBlockBlob`]. Using byte
/// arrays for transaction rows is safe on every target: they have alignment one and every bit
/// pattern is valid. Row fields are converted from little-endian bytes only when iterated.
pub fn deserialize_archive_v2_hot_block_blob_borrowed_current(
    bytes: &[u8],
) -> ReadResult<BorrowedArchiveV2HotBlockBlob<'_>> {
    let (header, tx_count, tx_rows, message_bytes, metadata_bytes): (
        ArchiveV2HotBlockHeader,
        u32,
        &[[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]],
        &[u8],
        &[u8],
    ) = wincode::config::deserialize(bytes, wincode_leb128_config())?;
    Ok(BorrowedArchiveV2HotBlockBlob {
        header,
        tx_count,
        tx_rows,
        message_bytes,
        metadata_bytes,
    })
}

#[derive(Debug, SchemaRead)]
struct ArchiveV2HotBlockHeaderWithoutRewardsPrefix {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
}

#[derive(Debug)]
struct DiscardedArchiveV2HotRewards;

// SAFETY: the implementation initializes the inhabited zero-sized destination exactly on success.
// It uses the configured sequence-length decoder and `CompactReward`'s normal `SchemaRead`
// implementation, so it consumes and validates the same wire fields as `Vec<CompactReward>`
// without retaining the decoded elements.
unsafe impl<'de, C: Config> SchemaRead<'de, C> for DiscardedArchiveV2HotRewards {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let _num_partitions = <Option<u64> as SchemaRead<'de, C>>::get(reader.by_ref())?;
        let reward_count = <C::LengthEncoding as SeqLen<C>>::read_prealloc_check::<CompactReward>(
            reader.by_ref(),
        )?;
        for _ in 0..reward_count {
            let _reward = <CompactReward as SchemaRead<'de, C>>::get(reader.by_ref())?;
        }
        dst.write(Self);
        Ok(())
    }
}

/// Decode the current hot-block schema while borrowing its contiguous regions and discarding
/// decoded rewards after validating their complete wire representation.
///
/// `header.rewards` is always `None` in the returned replay-only view, regardless of whether the
/// source carried rewards. Use [`deserialize_archive_v2_hot_block_blob_borrowed_current`] when the
/// caller needs reward values. [`BorrowedArchiveV2HotBlockBlobWithoutRewards::rewards_field_bytes`]
/// returns the exact original encoded reward-option field without allocating or re-encoding.
pub fn deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(
    bytes: &[u8],
) -> ReadResult<BorrowedArchiveV2HotBlockBlobWithoutRewards<'_>> {
    let mut remaining = bytes;
    let header = <ArchiveV2HotBlockHeaderWithoutRewardsPrefix as SchemaRead<
        '_,
        WincodeLeb128Config,
    >>::get(remaining.by_ref())?;

    let rewards_field_start = remaining;
    let _rewards =
        <Option<DiscardedArchiveV2HotRewards> as SchemaRead<'_, WincodeLeb128Config>>::get(
            remaining.by_ref(),
        )?;
    let rewards_field_len = rewards_field_start.len() - remaining.len();
    let rewards_field_bytes = &rewards_field_start[..rewards_field_len];

    let (tx_count, tx_rows, message_bytes, metadata_bytes): (
        u32,
        &[[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]],
        &[u8],
        &[u8],
    ) = wincode::config::deserialize_exact(remaining, wincode_leb128_config())?;
    Ok(BorrowedArchiveV2HotBlockBlobWithoutRewards {
        header: ArchiveV2HotBlockHeader {
            slot: header.slot,
            parent_slot: header.parent_slot,
            blockhash_id: header.blockhash_id,
            previous_blockhash_id: header.previous_blockhash_id,
            block_time: header.block_time,
            block_height: header.block_height,
            rewards: None,
        },
        tx_count,
        tx_rows,
        message_bytes,
        metadata_bytes,
        rewards_field_bytes,
    })
}

#[cfg(test)]
mod hot_block_slot_time_tests {
    use super::*;

    fn current_hot_block_fixture() -> ArchiveV2HotBlockBlob {
        ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: 123,
                parent_slot: 122,
                blockhash_id: 7,
                previous_blockhash_id: 6,
                block_time: Some(1_700_000_123),
                block_height: Some(120),
                rewards: None,
            },
            tx_count: 2,
            tx_rows: vec![
                ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: 0x1234,
                    message_offset: 0,
                    message_len: 2,
                    metadata_offset: 0,
                    metadata_len: 1,
                    signature_count: 2,
                    reserved: [0; 3],
                },
                ArchiveV2HotTxRow {
                    tx_index: 1,
                    flags: 0x4321,
                    message_offset: 2,
                    message_len: 3,
                    metadata_offset: 1,
                    metadata_len: 2,
                    signature_count: 1,
                    reserved: [0; 3],
                },
            ],
            message_bytes: vec![10, 11, 12, 13, 14],
            metadata_bytes: vec![20, 21, 22],
        }
    }

    fn reward_fixture(marker: u8) -> CompactReward {
        CompactReward {
            pubkey: CompactPubkey::raw([marker; 32]),
            lamports: -i64::from(marker),
            post_balance: 10_000 + u64::from(marker),
            reward_type: i32::from(marker),
            commission: Some(marker),
        }
    }

    fn assert_replay_view_matches_owned(
        replay: &BorrowedArchiveV2HotBlockBlobWithoutRewards<'_>,
        owned: &ArchiveV2HotBlockBlob,
    ) {
        assert_eq!(replay.header.slot, owned.header.slot);
        assert_eq!(replay.header.parent_slot, owned.header.parent_slot);
        assert_eq!(replay.header.blockhash_id, owned.header.blockhash_id);
        assert_eq!(
            replay.header.previous_blockhash_id,
            owned.header.previous_blockhash_id
        );
        assert_eq!(replay.header.block_time, owned.header.block_time);
        assert_eq!(replay.header.block_height, owned.header.block_height);
        assert!(replay.header.rewards.is_none());
        assert_eq!(replay.tx_count, owned.tx_count);
        assert_eq!(replay.tx_rows().collect::<Vec<_>>(), owned.tx_rows);
        assert_eq!(replay.message_bytes, owned.message_bytes);
        assert_eq!(replay.metadata_bytes, owned.metadata_bytes);
    }

    fn assert_exact_rewards_field(block: ArchiveV2HotBlockBlob) {
        let header_prefix = (
            block.header.slot,
            block.header.parent_slot,
            block.header.blockhash_id,
            block.header.previous_blockhash_id,
            block.header.block_time,
            block.header.block_height,
        );
        let prefix_bytes =
            wincode::config::serialize(&header_prefix, wincode_leb128_config()).unwrap();
        let expected_field =
            wincode::config::serialize(&block.header.rewards, wincode_leb128_config()).unwrap();
        let bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let expected_source_field =
            &bytes[prefix_bytes.len()..prefix_bytes.len() + expected_field.len()];
        assert_eq!(expected_source_field, expected_field);

        let replay =
            deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(&bytes).unwrap();
        assert_eq!(replay.rewards_field_bytes(), expected_source_field);
        assert_eq!(
            replay.rewards_field_bytes().as_ptr(),
            expected_source_field.as_ptr()
        );
    }

    #[test]
    fn prefix_decoder_supports_current_hot_block_encoding() {
        let block = current_hot_block_fixture();
        let bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();

        assert_eq!(
            deserialize_archive_v2_hot_block_slot_time(&bytes).unwrap(),
            (123, Some(1_700_000_123))
        );
    }

    #[test]
    fn prefix_decoder_supports_legacy_shredding_hot_block_encoding() {
        let block = ArchiveV2HotBlockBlobLegacyShredding {
            header: ArchiveV2HotBlockHeaderLegacyShredding {
                slot: 456,
                parent_slot: 455,
                blockhash_id: 9,
                previous_blockhash_id: 8,
                block_time: None,
                block_height: Some(450),
                shredding: vec![CompactShredding {
                    entry_end_idx: 3,
                    shred_end_idx: 4,
                }],
                rewards: None,
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();

        assert_eq!(
            deserialize_archive_v2_hot_block_slot_time(&bytes).unwrap(),
            (456, None)
        );
    }

    #[test]
    fn prefix_decoder_supports_legacy_rewards_vec_hot_block_encoding() {
        let block = ArchiveV2HotBlockBlobLegacyRewardsVec {
            header: ArchiveV2HotBlockHeaderLegacyRewardsVec {
                slot: 789,
                parent_slot: 788,
                blockhash_id: 11,
                previous_blockhash_id: 10,
                block_time: Some(-42),
                block_height: None,
                rewards: Vec::new(),
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();

        assert_eq!(
            deserialize_archive_v2_hot_block_slot_time(&bytes).unwrap(),
            (789, Some(-42))
        );
    }

    #[test]
    fn poh_record_decoder_supports_current_encoding() {
        let record = WincodeArchiveV2PohRecord {
            block_id: 42,
            slot: 999,
            entries: vec![crate::CompactPohEntry {
                num_hashes: 10,
                hash: [7; 32],
                tx_count: 2,
                signature_count: 3,
            }],
        };
        let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();

        let decoded = deserialize_archive_v2_poh_record(&bytes).unwrap();
        assert_eq!(decoded.block_id, 42);
        assert_eq!(decoded.slot, 999);
        assert_eq!(decoded.entries[0].signature_count, 3);
    }

    #[test]
    fn poh_record_decoder_falls_back_to_pre_signature_count_encoding() {
        let record = WincodeArchiveV2PohRecordLegacyNoSignatureCount {
            block_id: 42,
            slot: 999,
            entries: vec![crate::CompactPohEntryLegacyNoSignatureCount {
                num_hashes: 10,
                hash: [7; 32],
                tx_count: 2,
            }],
        };
        let bytes = wincode::config::serialize(&record, wincode_leb128_config()).unwrap();

        let decoded = deserialize_archive_v2_poh_record(&bytes).unwrap();
        assert_eq!(decoded.block_id, 42);
        assert_eq!(decoded.slot, 999);
        assert_eq!(decoded.entries[0].tx_count, 2);
        // Not recoverable from a legacy record; callers must treat this as "unknown", never as
        // a real zero-signature entry.
        assert_eq!(decoded.entries[0].signature_count, 0);
    }

    #[test]
    fn borrowed_current_decoder_matches_owned_and_borrows_large_regions() {
        let expected = current_hot_block_fixture();
        let bytes = wincode::config::serialize(&expected, wincode_leb128_config()).unwrap();
        let owned = deserialize_archive_v2_hot_block_blob(&bytes).unwrap();
        let borrowed = deserialize_archive_v2_hot_block_blob_borrowed_current(&bytes).unwrap();

        assert_eq!(borrowed.header.slot, owned.header.slot);
        assert_eq!(borrowed.header.parent_slot, owned.header.parent_slot);
        assert_eq!(borrowed.header.blockhash_id, owned.header.blockhash_id);
        assert_eq!(
            borrowed.header.previous_blockhash_id,
            owned.header.previous_blockhash_id
        );
        assert_eq!(borrowed.header.block_time, owned.header.block_time);
        assert_eq!(borrowed.header.block_height, owned.header.block_height);
        assert!(borrowed.header.rewards.is_none());
        assert_eq!(borrowed.tx_count, owned.tx_count);
        assert_eq!(borrowed.tx_rows_len(), owned.tx_rows.len());
        assert_eq!(borrowed.tx_rows().collect::<Vec<_>>(), owned.tx_rows);
        assert_eq!(borrowed.message_bytes, owned.message_bytes);
        assert_eq!(borrowed.metadata_bytes, owned.metadata_bytes);
        assert_eq!(borrowed.tx_rows().len(), 2);
        assert_eq!(borrowed.tx_rows().next_back().unwrap().tx_index, 1);

        let frame_start = bytes.as_ptr() as usize;
        let frame_end = frame_start.checked_add(bytes.len()).unwrap();
        for region in [borrowed.message_bytes, borrowed.metadata_bytes] {
            let region_start = region.as_ptr() as usize;
            let region_end = region_start.checked_add(region.len()).unwrap();
            assert!(region_start >= frame_start);
            assert!(region_end <= frame_end);
        }
    }

    #[test]
    fn reward_discarding_decoder_matches_none_empty_and_populated_blocks() {
        let mut fixtures = Vec::new();
        fixtures.push(current_hot_block_fixture());

        let mut empty = current_hot_block_fixture();
        empty.header.rewards = Some(ArchiveV2HotRewards {
            num_partitions: Some(8),
            decoded: Vec::new(),
        });
        fixtures.push(empty);

        let mut populated = current_hot_block_fixture();
        populated.header.rewards = Some(ArchiveV2HotRewards {
            num_partitions: None,
            decoded: vec![reward_fixture(17), reward_fixture(29)],
        });
        fixtures.push(populated);

        for expected in fixtures {
            let bytes = wincode::config::serialize(&expected, wincode_leb128_config()).unwrap();
            let owned = deserialize_archive_v2_hot_block_blob(&bytes).unwrap();
            let replay =
                deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(&bytes)
                    .unwrap();
            assert_replay_view_matches_owned(&replay, &owned);

            let frame_start = bytes.as_ptr() as usize;
            let frame_end = frame_start.checked_add(bytes.len()).unwrap();
            for region in [replay.message_bytes, replay.metadata_bytes] {
                let region_start = region.as_ptr() as usize;
                let region_end = region_start.checked_add(region.len()).unwrap();
                assert!(region_start >= frame_start);
                assert!(region_end <= frame_end);
            }
        }
    }

    #[test]
    fn reward_discarding_decoder_lends_exact_none_field() {
        let block = current_hot_block_fixture();
        assert_eq!(
            wincode::config::serialize(&block.header.rewards, wincode_leb128_config()).unwrap(),
            [0]
        );
        assert_exact_rewards_field(block);
    }

    #[test]
    fn reward_discarding_decoder_lends_exact_some_empty_field() {
        let mut block = current_hot_block_fixture();
        block.header.rewards = Some(ArchiveV2HotRewards {
            num_partitions: Some(8),
            decoded: Vec::new(),
        });
        assert_exact_rewards_field(block);
    }

    #[test]
    fn reward_discarding_decoder_lends_exact_some_id_and_raw_field() {
        let mut indexed = reward_fixture(17);
        indexed.pubkey = CompactPubkey::id(513);
        let mut block = current_hot_block_fixture();
        block.header.rewards = Some(ArchiveV2HotRewards {
            num_partitions: None,
            decoded: vec![indexed, reward_fixture(29)],
        });
        assert_exact_rewards_field(block);
    }

    #[test]
    fn reward_discarding_decoder_validates_each_reward() {
        let reward = reward_fixture(73);
        let encoded_reward = wincode::config::serialize(&reward, wincode_leb128_config()).unwrap();
        let mut block = current_hot_block_fixture();
        block.header.rewards = Some(ArchiveV2HotRewards {
            num_partitions: Some(3),
            decoded: vec![reward],
        });
        let mut bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let reward_start = bytes
            .windows(encoded_reward.len())
            .position(|window| window == encoded_reward)
            .expect("unique reward bytes occur in the block");
        let commission_tag = reward_start + encoded_reward.len() - 2;
        assert_eq!(bytes[commission_tag], 1);
        bytes[commission_tag] = 2;

        assert!(deserialize_archive_v2_hot_block_blob_borrowed_current(&bytes).is_err());
        assert!(
            deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(&bytes).is_err()
        );
    }

    #[test]
    fn reward_discarding_decoder_rejects_truncated_and_trailing_frames() {
        let mut block = current_hot_block_fixture();
        block.header.rewards = Some(ArchiveV2HotRewards {
            num_partitions: None,
            decoded: vec![reward_fixture(41)],
        });
        let bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();

        let mut truncated = bytes.clone();
        truncated.pop();
        assert!(
            deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(&truncated)
                .is_err()
        );

        let mut trailing = bytes;
        trailing.push(0xff);
        assert!(
            deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(&trailing)
                .is_err()
        );
    }

    #[test]
    fn borrowed_and_owned_decoders_both_reject_a_truncated_current_block() {
        let mut bytes =
            wincode::config::serialize(&current_hot_block_fixture(), wincode_leb128_config())
                .unwrap();
        bytes.pop();

        assert!(deserialize_archive_v2_hot_block_blob_borrowed_current(&bytes).is_err());
        assert!(deserialize_archive_v2_hot_block_blob(&bytes).is_err());
    }

    #[test]
    fn historical_shredding_schema_remains_an_owned_fallback() {
        let block = ArchiveV2HotBlockBlobLegacyShredding {
            header: ArchiveV2HotBlockHeaderLegacyShredding {
                slot: 456,
                parent_slot: 455,
                blockhash_id: 9,
                previous_blockhash_id: 8,
                block_time: None,
                block_height: Some(450),
                // Length two is an invalid `Option` tag in the current schema, making the
                // current-vs-legacy distinction deterministic for this fixture.
                shredding: vec![
                    CompactShredding {
                        entry_end_idx: 3,
                        shred_end_idx: 4,
                    },
                    CompactShredding {
                        entry_end_idx: 5,
                        shred_end_idx: 6,
                    },
                ],
                rewards: None,
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let bytes = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();

        assert!(deserialize_archive_v2_hot_block_blob_borrowed_current(&bytes).is_err());
        assert!(
            deserialize_archive_v2_hot_block_blob_borrowed_current_without_rewards(&bytes).is_err()
        );
        let decoded = deserialize_archive_v2_hot_block_blob(&bytes).unwrap();
        assert_eq!(decoded.header.slot, 456);
        assert!(decoded.tx_rows.is_empty());
    }
}

/// Per-block access sidecar for registry-free hot-path rendering.
///
/// Hot block blobs keep transaction structure compact by storing pubkeys and
/// recent blockhashes as ids. This wincode sidecar carries only the id->bytes
/// entries that are needed by one block, plus the block's signatures.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2BlockAccessBlob {
    pub version: u16,
    pub flags: u32,
    pub blockhash: [u8; 32],
    pub previous_blockhash: [u8; 32],
    pub signature_counts: Vec<u8>,
    pub signatures: Vec<u8>,
    pub pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    pub blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
    pub vote_hashes: Vec<ArchiveV2BlockAccessVoteHash>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2BlockAccessPubkey {
    pub id: u32,
    pub pubkey: [u8; 32],
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2BlockAccessBlockhash {
    pub id: i32,
    pub blockhash: [u8; 32],
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2BlockAccessVoteHash {
    pub block_id: u32,
    pub bank_hash: Option<[u8; 32]>,
    pub block_id_hash: Option<[u8; 32]>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2HotRewards {
    pub num_partitions: Option<u64>,
    pub decoded: Vec<CompactReward>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(C)]
pub struct ArchiveV2HotTxRow {
    pub tx_index: u32,
    pub flags: u32,
    pub message_offset: u32,
    pub message_len: u32,
    pub metadata_offset: u32,
    pub metadata_len: u32,
    pub signature_count: u8,
    pub reserved: [u8; 3],
}

impl ArchiveV2HotTxRow {
    #[inline]
    fn from_wire_bytes(bytes: &[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]) -> Self {
        Self {
            tx_index: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            flags: u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            message_offset: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            message_len: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            metadata_offset: u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
            metadata_len: u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]),
            signature_count: bytes[24],
            reserved: [bytes[25], bytes[26], bytes[27]],
        }
    }
}

unsafe impl<C: wincode::config::ConfigCore> SchemaWrite<C> for ArchiveV2HotTxRow {
    type Src = Self;

    #[inline]
    fn size_of(_src: &Self::Src) -> WriteResult<usize> {
        Ok(ARCHIVE_V2_HOT_TX_ROW_LEN)
    }

    #[inline]
    fn write(mut writer: impl Writer, src: &Self::Src) -> WriteResult<()> {
        writer.write(&src.tx_index.to_le_bytes())?;
        writer.write(&src.flags.to_le_bytes())?;
        writer.write(&src.message_offset.to_le_bytes())?;
        writer.write(&src.message_len.to_le_bytes())?;
        writer.write(&src.metadata_offset.to_le_bytes())?;
        writer.write(&src.metadata_len.to_le_bytes())?;
        writer.write(&[src.signature_count])?;
        writer.write(&src.reserved)?;
        Ok(())
    }
}

unsafe impl<'de, C: wincode::config::ConfigCore> SchemaRead<'de, C> for ArchiveV2HotTxRow {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let bytes = reader.take_array::<ARCHIVE_V2_HOT_TX_ROW_LEN>()?;
        dst.write(Self::from_wire_bytes(&bytes));
        Ok(())
    }
}

pub const ARCHIVE_V2_TX_FLAG_HAS_METADATA: u32 = 1 << 0;
pub const ARCHIVE_V2_TX_FLAG_MESSAGE_V0: u32 = 1 << 1;
pub const ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK: u32 = 1 << 2;
pub const ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK: u32 = 1 << 3;
pub const ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA: u32 = 1 << 4;
pub const ARCHIVE_V2_TX_FLAG_HAS_LOGS: u32 = 1 << 5;
pub const ARCHIVE_V2_TX_FLAG_HAS_INNER_IX: u32 = 1 << 6;
pub const ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES: u32 = 1 << 7;
pub const ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES: u32 = 1 << 8;
pub const ARCHIVE_V2_TX_FLAG_HAS_ERROR: u32 = 1 << 9;
pub const ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX: u32 = 1 << 10;

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ArchiveV2HotMetaRecord {
    Header(WincodeArchiveV2Header),
    Genesis(WincodeArchiveV2Genesis),
    Footer(WincodeArchiveV2Footer),
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ArchiveV2HotMessagePayload {
    Legacy(ArchiveV2HotLegacyMessage),
    V0(ArchiveV2HotV0Message),
    // Appended so the existing tags stay put.
    V1(ArchiveV2HotV1Message),
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2HotLegacyMessage {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<CompactPubkey>,
    pub recent_blockhash: OwnedCompactRecentBlockhash,
    pub instructions: Vec<ArchiveV2HotInstruction>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2HotV0Message {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<CompactPubkey>,
    pub recent_blockhash: OwnedCompactRecentBlockhash,
    pub instructions: Vec<ArchiveV2HotInstruction>,
    pub address_table_lookups: Vec<OwnedCompactAddressTableLookup>,
}

/// A v1 hot message. No lookup tables, and the compute budget lives in the
/// header instead of the instruction list.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2HotV1Message {
    pub header: CompactMessageHeader,
    pub config: CompactTransactionConfig,
    pub account_keys: Vec<CompactPubkey>,
    pub recent_blockhash: OwnedCompactRecentBlockhash,
    pub instructions: Vec<ArchiveV2HotInstruction>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2HotInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: ArchiveV2HotInstructionData,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ArchiveV2HotInstructionData {
    Raw(Vec<u8>),
    UnknownSystem(Vec<u8>),
    UnknownVote(Vec<u8>),
    ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    System(ArchiveV2SystemInstructionData),
    VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate),
    VoteCompactUpdateVoteStateSwitch {
        update: ArchiveV2VoteStateUpdate,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
    VoteTowerSync(ArchiveV2VoteTowerSync),
    VoteTowerSyncSwitch {
        tower: ArchiveV2VoteTowerSync,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ArchiveV2ComputeBudgetInstructionData {
    Unused,
    RequestHeapFrame(u32),
    SetComputeUnitLimit(u32),
    SetComputeUnitPrice(u64),
    SetLoadedAccountsDataSizeLimit(u32),
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ArchiveV2SystemInstructionData {
    CreateAccount {
        lamports: u64,
        space: u64,
        owner: [u8; 32],
    },
    Assign {
        owner: [u8; 32],
    },
    Transfer {
        lamports: u64,
    },
    CreateAccountWithSeed {
        base: [u8; 32],
        seed: String,
        lamports: u64,
        space: u64,
        owner: [u8; 32],
    },
    AdvanceNonceAccount,
    WithdrawNonceAccount {
        lamports: u64,
    },
    InitializeNonceAccount {
        authority: [u8; 32],
    },
    AuthorizeNonceAccount {
        authority: [u8; 32],
    },
    Allocate {
        space: u64,
    },
    AllocateWithSeed {
        base: [u8; 32],
        seed: String,
        space: u64,
        owner: [u8; 32],
    },
    AssignWithSeed {
        base: [u8; 32],
        seed: String,
        owner: [u8; 32],
    },
    TransferWithSeed {
        lamports: u64,
        from_seed: String,
        from_owner: [u8; 32],
    },
    UpgradeNonceAccount,
    CreateAccountAllowPrefund {
        lamports: u64,
        space: u64,
        owner: [u8; 32],
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2VoteStateUpdate {
    pub root: Option<u64>,
    pub lockout_offsets: Vec<ArchiveV2VoteLockoutOffset>,
    pub hash: ArchiveV2VoteHashRef,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2VoteTowerSync {
    pub update: ArchiveV2VoteStateUpdate,
    pub block_id_hash: ArchiveV2VoteHashRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ArchiveV2VoteLockoutOffset {
    pub offset: u64,
    pub confirmation_count: u8,
}

/// Reference to a 32-byte vote hash.
///
/// `Block` is an epoch-local Archive V2 block id. For a vote-state `hash`, it
/// resolves through the vote hash sidecar's bank-hash column. For a TowerSync
/// `block_id`, it resolves through the same row's Agave block-id column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ArchiveV2VoteHashRef {
    Zero,
    Block(u32),
    Raw([u8; 32]),
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum WincodeArchiveV2NoRegistryRecord {
    Header(WincodeArchiveV2Header),
    Block(WincodeArchiveV2NoRegistryBlock),
    Index(SplitCompactIndexRecord),
    Footer(WincodeArchiveV2Footer),
    Genesis(WincodeArchiveV2NoRegistryGenesis),
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryGenesis {
    pub genesis_hash: [u8; 32],
    pub genesis_bin_len: u64,
    pub creation_time_unix: i64,
    pub cluster_id: u32,
    pub ticks_per_slot: u64,
    pub poh_params: WincodeArchiveV2GenesisPohParams,
    pub fees: WincodeArchiveV2GenesisFeeParams,
    pub rent: WincodeArchiveV2GenesisRentParams,
    pub inflation: WincodeArchiveV2GenesisInflationParams,
    pub epoch_schedule: WincodeArchiveV2GenesisEpochSchedule,
    pub accounts: Vec<WincodeArchiveV2NoRegistryGenesisAccount>,
    pub builtins: Vec<WincodeArchiveV2NoRegistryGenesisBuiltin>,
    pub reward_pools: Vec<WincodeArchiveV2NoRegistryGenesisAccount>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryGenesisAccount {
    pub pubkey: [u8; 32],
    pub lamports: u64,
    pub owner: [u8; 32],
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryGenesisBuiltin {
    pub key: String,
    pub pubkey: [u8; 32],
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryBlock {
    pub header: WincodeArchiveV2NoRegistryBlockHeader,
    pub txs: Vec<WincodeArchiveV2NoRegistryTransaction>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryBlockHeader {
    pub compact: CompactBlockHeader,
    pub rewards: Option<WincodeArchiveV2NoRegistryRewards>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryRewards {
    pub source_len: u64,
    pub num_partitions: Option<u64>,
    pub decoded: Option<Vec<WincodeArchiveV2NoRegistryReward>>,
    pub raw_fallback: Option<Vec<u8>>,
    pub decode_error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryTransaction {
    pub tx_index: u32,
    pub tx: WincodeArchiveV2Payload<WincodeArchiveV2NoRegistryTx>,
    pub metadata: Option<WincodeArchiveV2Payload<WincodeArchiveV2NoRegistryMeta>>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryTx {
    pub signatures: Vec<Vec<u8>>,
    pub message: WincodeArchiveV2NoRegistryMessage,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum WincodeArchiveV2NoRegistryMessage {
    Legacy(WincodeArchiveV2NoRegistryLegacyMessage),
    V0(WincodeArchiveV2NoRegistryV0Message),
    // Appended so the existing tags stay put.
    V1(WincodeArchiveV2NoRegistryV1Message),
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryLegacyMessage {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<[u8; 32]>,
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<WincodeArchiveV2NoRegistryInstruction>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryAddressTableLookup {
    pub account_key: [u8; 32],
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryV0Message {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<[u8; 32]>,
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<WincodeArchiveV2NoRegistryInstruction>,
    pub address_table_lookups: Vec<WincodeArchiveV2NoRegistryAddressTableLookup>,
}

/// A v1 message. No lookup tables, and the compute budget travels in the
/// header rather than as instructions.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryV1Message {
    pub header: CompactMessageHeader,
    pub config: CompactTransactionConfig,
    pub account_keys: Vec<[u8; 32]>,
    pub recent_blockhash: [u8; 32],
    pub instructions: Vec<WincodeArchiveV2NoRegistryInstruction>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryMeta {
    pub err: Option<CompactTransactionError>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<CompactInnerInstructions>>,
    pub logs: Option<WincodeArchiveV2NoRegistryLogs>,
    pub pre_token_balances: Vec<WincodeArchiveV2NoRegistryTokenBalance>,
    pub post_token_balances: Vec<WincodeArchiveV2NoRegistryTokenBalance>,
    pub rewards: Vec<WincodeArchiveV2NoRegistryReward>,
    pub loaded_writable_addresses: Vec<[u8; 32]>,
    pub loaded_readonly_addresses: Vec<[u8; 32]>,
    pub return_data: Option<WincodeArchiveV2NoRegistryReturnData>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum WincodeArchiveV2NoRegistryLogs {
    Raw(Vec<String>),
    WincodeZstd {
        uncompressed_len: u64,
        bytes: Vec<u8>,
    },
    Compact(CompactLogStream),
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryTokenBalance {
    pub account_index: u32,
    pub mint: Option<[u8; 32]>,
    pub owner: Option<[u8; 32]>,
    pub program_id: Option<[u8; 32]>,
    pub amount: u64,
    pub decimals: u8,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryReward {
    pub pubkey: [u8; 32],
    pub lamports: i64,
    pub post_balance: u64,
    pub reward_type: i32,
    pub commission: Option<u8>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2NoRegistryReturnData {
    pub program_id: [u8; 32],
    pub data: Vec<u8>,
}

/// Binary getBlock envelope returned by `blockzilla-get-block`.
///
/// This is not a replacement for `ArchiveV2HotBlockBlob`. It packages the two
/// block-local blobs a client needs to reconstruct a JSON getBlock response:
/// the independently compressed hot block and, unless omitted by request, the
/// block-access sidecar with signatures and id-to-value mappings.
#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct BlockzillaGetBlockBundleV1 {
    pub version: u16,
    pub slot: u64,
    pub hot_block_encoding: BlockzillaGetBlockBlobEncoding,
    pub hot_block: Vec<u8>,
    pub block_access_encoding: BlockzillaGetBlockBlobEncoding,
    pub block_access: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[wincode(tag_encoding = "u8")]
pub enum BlockzillaGetBlockBlobEncoding {
    Wincode,
    ZstdWincode,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct WincodeArchiveV2Footer {
    pub blocks: u64,
    pub transactions: u64,
    pub entries: u64,
    pub rewards: u64,
    pub dataframes: u64,
    pub subset_nodes_ignored: u64,
    pub epoch_nodes_ignored: u64,
    pub car_entries: u64,
    pub car_payload_bytes: u64,
    pub decoded_node_payload_bytes: u64,
    pub tx_source_bytes: u64,
    pub metadata_source_bytes: u64,
    pub rewards_source_bytes: u64,
    pub tx_raw_fallbacks: u64,
    pub metadata_raw_fallbacks: u64,
    pub rewards_raw_fallbacks: u64,
    pub nonce_recent_blockhashes: u64,
    pub decode_errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LosslessV2Header {
    /// Exact CAR header bytes, including the length varint prefix.
    pub encoded_car_header: Vec<u8>,
}

/// Historical and replay-verifiable node content for a CAR-equivalent archive.
///
/// Together with `RuntimeArchiveRecord` and `ReconstructionArchiveRecord`, this
/// preserves every CAR node needed to validate CIDs and emit the original CAR
/// stream again. This is the path to port to wincode for the full archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HistoricalArchiveRecord {
    Transaction(HistoricalTransactionRecord),
    Entry(RawEntryNode),
    Block(HistoricalBlockRecord),
    DataFrame(StandaloneDataFrame),
    Subset(RawSubsetNode),
    Epoch(RawEpochNode),
}

/// Runtime and metadata-only node content for a CAR-equivalent archive.
///
/// Transaction metadata and reward dataframes live here so the historical stream
/// can stay focused on replay-critical content without losing any CAR bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuntimeArchiveRecord {
    Transaction(RuntimeTransactionRecord),
    Rewards(RawRewardsNode),
    Block(RuntimeBlockRecord),
}

/// Reconstruction order and original CAR offsets.
///
/// This stream is what makes the split historical/runtime files lossless: it
/// records each node kind, original location, and CID in CAR order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReconstructionArchiveRecord {
    Header(LosslessV2Header),
    Node(ReconstructionNodeRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconstructionNodeKind {
    Transaction,
    Entry,
    Rewards,
    Block,
    Subset,
    Epoch,
    DataFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconstructionNodeRecord {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub kind: ReconstructionNodeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalTransactionRecord {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub slot: u64,
    pub index: Option<u64>,
    /// Raw transaction dataframe, including continuation references.
    pub data: RawDataFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeTransactionRecord {
    pub location: NodeLocation,
    pub cid: Cid36,
    /// Raw transaction status metadata dataframe, including continuation refs.
    pub metadata: RawDataFrame,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalBlockRecord {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub slot: u64,
    pub shredding: Vec<Shredding>,
    /// Exact CBOR section from the CAR block payload.
    pub shredding_cbor: Vec<u8>,
    pub entries: Vec<RawCidRef>,
    /// Exact CBOR section from the CAR block payload.
    pub entries_cbor: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeBlockRecord {
    pub location: NodeLocation,
    pub cid: Cid36,
    pub slot: u64,
    pub meta: SlotMeta,
    /// Exact CBOR section from the CAR block payload.
    pub meta_cbor: Vec<u8>,
    /// Optional reward-node CID reference from the block payload.
    pub rewards: Option<RawCidRef>,
}

impl ReconstructionNodeRecord {
    pub fn from_raw(node: &RawNode) -> Self {
        let kind = match node {
            RawNode::Transaction(_) => ReconstructionNodeKind::Transaction,
            RawNode::Entry(_) => ReconstructionNodeKind::Entry,
            RawNode::Rewards(_) => ReconstructionNodeKind::Rewards,
            RawNode::Block(_) => ReconstructionNodeKind::Block,
            RawNode::Subset(_) => ReconstructionNodeKind::Subset,
            RawNode::Epoch(_) => ReconstructionNodeKind::Epoch,
            RawNode::DataFrame(_) => ReconstructionNodeKind::DataFrame,
        };
        Self {
            location: node.location(),
            cid: node.cid(),
            kind,
        }
    }
}

impl HistoricalTransactionRecord {
    pub fn from_raw(node: &RawTransactionNode) -> Self {
        Self {
            location: node.location,
            cid: node.cid,
            slot: node.slot,
            index: node.index,
            data: node.data.clone(),
        }
    }

    pub fn with_runtime(self, runtime: RuntimeTransactionRecord) -> RawTransactionNode {
        RawTransactionNode {
            location: self.location,
            cid: self.cid,
            slot: self.slot,
            index: self.index,
            data: self.data,
            metadata: runtime.metadata,
        }
    }
}

impl RuntimeTransactionRecord {
    pub fn from_raw(node: &RawTransactionNode) -> Self {
        Self {
            location: node.location,
            cid: node.cid,
            metadata: node.metadata.clone(),
        }
    }
}

impl HistoricalBlockRecord {
    pub fn from_raw(node: &RawBlockNode) -> Self {
        Self {
            location: node.location,
            cid: node.cid,
            slot: node.slot,
            shredding: node.shredding.clone(),
            shredding_cbor: node.shredding_cbor.clone(),
            entries: node.entries.clone(),
            entries_cbor: node.entries_cbor.clone(),
        }
    }

    pub fn with_runtime(self, runtime: RuntimeBlockRecord) -> RawBlockNode {
        RawBlockNode {
            location: self.location,
            cid: self.cid,
            slot: self.slot,
            shredding: self.shredding,
            shredding_cbor: self.shredding_cbor,
            entries: self.entries,
            entries_cbor: self.entries_cbor,
            meta: runtime.meta,
            meta_cbor: runtime.meta_cbor,
            rewards: runtime.rewards,
        }
    }
}

impl RuntimeBlockRecord {
    pub fn from_raw(node: &RawBlockNode) -> Self {
        Self {
            location: node.location,
            cid: node.cid,
            slot: node.slot,
            meta: node.meta.clone(),
            meta_cbor: node.meta_cbor.clone(),
            rewards: node.rewards.clone(),
        }
    }
}
