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
    error::invalid_value,
    int_encoding::{ByteOrder, IntEncoding},
    io::{Reader, Writer},
};

use crate::CompactLogStream;
use crate::{
    CompactBlockHeader, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
    CompactPubkey, CompactReward, CompactShredding, CompactTransactionError,
    OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash, OwnedCompactTransaction,
    SplitCompactIndexRecord, wincode_leb128_config,
};

mod archive;
pub use archive::*;

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
        dst.write(Self {
            tx_index: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            flags: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            message_offset: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            message_len: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            metadata_offset: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            metadata_len: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            signature_count: bytes[24],
            reserved: [bytes[25], bytes[26], bytes[27]],
        });
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

/// Binary getBlock envelope returned by `blockzilla-get-block-worker`.
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
