use core::fmt;
use core::ops::Deref;
use std::borrow::Cow;

use prost::Message;
use quick_protobuf::{BytesReader, Error as QuickProtobufError, MessageRead};
#[cfg(all(feature = "zstd-wasm", not(feature = "zstd-native")))]
use ruzstd::decoding::{FrameDecoder, errors::FrameDecoderError};
#[cfg(feature = "zstd-native")]
use zstd::zstd_safe;

use crate::confirmed_block::{Rewards, TransactionStatusMeta};
use crate::confirmed_block_borrowed::solana::storage::ConfirmedBlock::{
    InnerInstructions as BorrowedInnerInstructions, ReturnData as BorrowedReturnData,
    Reward as BorrowedReward, TokenBalance as BorrowedTokenBalance,
    TransactionError as BorrowedTransactionError,
    TransactionStatusMeta as BorrowedTransactionStatusMeta,
};
use crate::stored_transaction::{
    StoredConfirmedBlockReward, StoredExtendedReward, StoredTransactionStatusMeta,
    StoredTransactionStatusMetaLegacyLogs, StoredTransactionStatusMetaV2,
};

pub const BINCODE_EPOCH_CUTOFF: u64 = 156;

#[derive(Debug)]
pub enum MetadataDecodeError {
    ZstdDecompress(std::io::Error),
    Bincode(String),
    ProstDecode(prost::DecodeError),
    ProtoConvert(String),
}

impl fmt::Display for MetadataDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetadataDecodeError::ZstdDecompress(e) => write!(f, "zstd decompress: {e}"),
            MetadataDecodeError::Bincode(e) => write!(f, "bincode decode: {e}"),
            MetadataDecodeError::ProstDecode(e) => write!(f, "protobuf decode: {e}"),
            MetadataDecodeError::ProtoConvert(e) => write!(f, "protobuf convert: {e}"),
        }
    }
}

impl std::error::Error for MetadataDecodeError {}

#[derive(Debug)]
pub enum RewardsDecodeError {
    ZstdDecompress(std::io::Error),
    Protobuf(prost::DecodeError),
    Bincode(String),
}

impl fmt::Display for RewardsDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RewardsDecodeError::ZstdDecompress(e) => write!(f, "zstd decompress: {e}"),
            RewardsDecodeError::Protobuf(e) => write!(f, "protobuf decode: {e}"),
            RewardsDecodeError::Bincode(e) => write!(f, "bincode decode: {e}"),
        }
    }
}

impl std::error::Error for RewardsDecodeError {}

pub struct BorrowedTransactionStatusMetaView<'a> {
    pub meta: BorrowedTransactionStatusMeta<'a>,
    pub compute_units_consumed: Option<u64>,
    pub cost_units: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct UiTokenAmountVisit<'a> {
    pub ui_amount: f64,
    pub decimals: u32,
    pub amount: &'a str,
    pub ui_amount_string: &'a str,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TokenBalanceVisit<'a> {
    pub account_index: u32,
    pub mint: &'a str,
    pub ui_token_amount: Option<UiTokenAmountVisit<'a>>,
    pub owner: &'a str,
    pub program_id: &'a str,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReturnDataVisit<'a> {
    pub program_id: &'a [u8],
    pub data: &'a [u8],
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InnerInstructionVisit<'a> {
    pub outer_instruction_index: u32,
    pub inner_instruction_index: u32,
    pub stack_height: Option<u32>,
    pub program_id_index: u32,
    pub accounts: &'a [u8],
    pub data: &'a [u8],
}

/// Callback API for scanning protobuf `TransactionStatusMeta` without materializing vectors.
///
/// Scalar fields are always decoded because they are cheap and required to advance the wire
/// reader. Repeated strings, nested token balances, rewards, addresses, and return data are only
/// decoded when the corresponding `wants_*` method returns `true`.
pub trait TransactionStatusMetaVisitor<'a> {
    #[inline]
    fn wants_status_error(&self) -> bool {
        false
    }

    #[inline]
    fn wants_pre_balances(&self) -> bool {
        false
    }

    #[inline]
    fn wants_post_balances(&self) -> bool {
        false
    }

    #[inline]
    fn wants_inner_instructions(&self) -> bool {
        false
    }

    #[inline]
    fn wants_log_messages(&self) -> bool {
        false
    }

    #[inline]
    fn wants_pre_token_balances(&self) -> bool {
        false
    }

    #[inline]
    fn wants_post_token_balances(&self) -> bool {
        false
    }

    #[inline]
    fn wants_rewards(&self) -> bool {
        false
    }

    #[inline]
    fn wants_loaded_addresses(&self) -> bool {
        false
    }

    #[inline]
    fn wants_return_data(&self) -> bool {
        false
    }

    #[inline]
    fn status_ok(&mut self) {}

    #[inline]
    fn status_error(&mut self, _err: &'a [u8]) {}

    #[inline]
    fn fee(&mut self, _fee: u64) {}

    #[inline]
    fn pre_balance(&mut self, _index: usize, _lamports: u64) {}

    #[inline]
    fn post_balance(&mut self, _index: usize, _lamports: u64) {}

    #[inline]
    fn inner_instructions_raw(&mut self, _bytes: &'a [u8]) {}

    #[inline]
    fn inner_instruction(&mut self, _instruction: InnerInstructionVisit<'a>) {}

    #[inline]
    fn inner_instructions_none(&mut self, _none: bool) {}

    #[inline]
    fn log_message(&mut self, _message: &'a str) {}

    #[inline]
    fn log_messages_none(&mut self, _none: bool) {}

    #[inline]
    fn pre_token_balance(&mut self, _balance: TokenBalanceVisit<'a>) {}

    #[inline]
    fn post_token_balance(&mut self, _balance: TokenBalanceVisit<'a>) {}

    #[inline]
    fn reward_raw(&mut self, _bytes: &'a [u8]) {}

    #[inline]
    fn loaded_writable_address(&mut self, _address: &'a [u8]) {}

    #[inline]
    fn loaded_readonly_address(&mut self, _address: &'a [u8]) {}

    #[inline]
    fn return_data(&mut self, _return_data: ReturnDataVisit<'a>) {}

    #[inline]
    fn return_data_none(&mut self, _none: bool) {}

    #[inline]
    fn compute_units_consumed(&mut self, _units: u64) {}

    #[inline]
    fn cost_units(&mut self, _units: u64) {}
}

const INNER_INSTRUCTIONS_RESERVE: usize = 4;
const LOG_MESSAGES_RESERVE: usize = 16;
const TOKEN_BALANCES_RESERVE: usize = 8;
const REWARDS_RESERVE: usize = 1;
const LOADED_ADDRESSES_RESERVE: usize = 8;

impl<'a> Deref for BorrowedTransactionStatusMetaView<'a> {
    type Target = BorrowedTransactionStatusMeta<'a>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.meta
    }
}

#[inline]
fn looks_like_zstd_frame(data: &[u8]) -> bool {
    // zstd frame magic number: 28 B5 2F FD
    data.len() >= 4 && data[0..4] == [0x28, 0xB5, 0x2F, 0xFD]
}

/// Reusable zstd context + reusable output buffer.
/// Keep one per worker thread. Do not share across threads.
pub struct ZstdReusableDecoder {
    #[cfg(feature = "zstd-native")]
    dctx: zstd::zstd_safe::DCtx<'static>,
    #[cfg(all(feature = "zstd-wasm", not(feature = "zstd-native")))]
    decoder: FrameDecoder,
    len: usize,
    out: Vec<u8>,
}

const INITIAL_ZSTD_OUTPUT_BYTES: usize = 128 * 1024;
const MAX_ZSTD_OUTPUT_BYTES: usize = 256 * 1024 * 1024;

impl Default for ZstdReusableDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ZstdReusableDecoder {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "zstd-native")]
            dctx: zstd::zstd_safe::DCtx::create(),
            #[cfg(all(feature = "zstd-wasm", not(feature = "zstd-native")))]
            decoder: FrameDecoder::new(),
            out: vec![0; INITIAL_ZSTD_OUTPUT_BYTES],
            len: 0,
        }
    }
    #[inline]
    pub fn output(&self) -> &[u8] {
        &self.out[..self.len]
    }

    /// If `input` is zstd, decompress into the internal buffer and return Ok(true).
    /// If it is not zstd, return Ok(false) and leave output empty.
    pub fn decompress_if_zstd(&mut self, input: &[u8]) -> Result<bool, std::io::Error> {
        if !looks_like_zstd_frame(input) {
            return Ok(false);
        }

        self.decompress_zstd_frame(input)?;
        Ok(true)
    }

    #[cfg(feature = "zstd-native")]
    fn decompress_zstd_frame(&mut self, input: &[u8]) -> Result<(), std::io::Error> {
        loop {
            match self.dctx.decompress(&mut self.out, input) {
                Ok(read) => {
                    self.len = read;
                    return Ok(());
                }
                Err(code) => {
                    let name = zstd_safe::get_error_name(code);
                    if name.contains("Destination buffer is too small")
                        && self.out.len() < MAX_ZSTD_OUTPUT_BYTES
                    {
                        let next_len = self.out.len().saturating_mul(2).min(MAX_ZSTD_OUTPUT_BYTES);
                        self.out.resize(next_len, 0);
                        self.dctx = zstd::zstd_safe::DCtx::create();
                        continue;
                    }

                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "zstd decode failed: {name} (raw={code}) input {} buffer {}",
                            input.len(),
                            self.out.len()
                        ),
                    ));
                }
            }
        }
    }

    #[cfg(all(feature = "zstd-wasm", not(feature = "zstd-native")))]
    fn decompress_zstd_frame(&mut self, input: &[u8]) -> Result<(), std::io::Error> {
        self.out.clear();
        if self.out.capacity() < INITIAL_ZSTD_OUTPUT_BYTES {
            self.out
                .reserve(INITIAL_ZSTD_OUTPUT_BYTES - self.out.capacity());
        }

        loop {
            match self.decoder.decode_all_to_vec(input, &mut self.out) {
                Ok(()) => {
                    self.len = self.out.len();
                    return Ok(());
                }
                Err(FrameDecoderError::TargetTooSmall)
                    if self.out.capacity() < MAX_ZSTD_OUTPUT_BYTES =>
                {
                    let next_capacity = self
                        .out
                        .capacity()
                        .max(INITIAL_ZSTD_OUTPUT_BYTES)
                        .saturating_mul(2)
                        .min(MAX_ZSTD_OUTPUT_BYTES);
                    self.out.reserve(next_capacity - self.out.capacity());
                    self.out.clear();
                    self.decoder = FrameDecoder::new();
                }
                Err(err) => {
                    self.len = 0;
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "zstd decode failed: {err} input {} buffer {}",
                            input.len(),
                            self.out.capacity()
                        ),
                    ));
                }
            }
        }
    }

    #[cfg(not(any(feature = "zstd-native", feature = "zstd-wasm")))]
    fn decompress_zstd_frame(&mut self, input: &[u8]) -> Result<(), std::io::Error> {
        let _ = input;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "of-car-reader was built without a zstd backend; enable zstd-native or zstd-wasm",
        ))
    }
}

/// Decode TransactionStatusMeta from a "frame" (possibly zstd-compressed; possibly empty).
///
/// Behavior:
/// - empty => default meta
/// - if zstd magic, decompress using reusable decoder
/// - else treat bytes as raw
pub fn decode_transaction_status_meta_from_frame(
    slot: u64,
    reassembled_metadata: &[u8],
    out: &mut TransactionStatusMeta,
    zstd: &mut ZstdReusableDecoder,
) -> Result<(), MetadataDecodeError> {
    out.clear();

    if reassembled_metadata.is_empty() {
        return Ok(());
    }

    if zstd
        .decompress_if_zstd(reassembled_metadata)
        .map_err(MetadataDecodeError::ZstdDecompress)?
    {
        decode_transaction_status_meta_into(slot, zstd.output(), out)
    } else {
        decode_transaction_status_meta_into(slot, reassembled_metadata, out)
    }
}

/// Decode TransactionStatusMeta from raw bytes (either bincode StoredTransactionStatusMeta
/// for early epochs, or protobuf for later epochs).
pub fn decode_transaction_status_meta_into(
    slot: u64,
    metadata_bytes: &[u8],
    out: &mut TransactionStatusMeta,
) -> Result<(), MetadataDecodeError> {
    if slot_uses_protobuf_metadata(slot) {
        decode_protobuf(out, slot, metadata_bytes)?;
    } else {
        let epoch = slot_to_epoch(slot);
        match epoch {
            // epoch 156 slot between 67_681_336 and 67_769_679 are bincode 2
            156 => {
                decode_bincode_2(out, slot, metadata_bytes)?;
            }
            _ => {
                decode_bincode(out, slot, metadata_bytes)?;
            }
        }
    }

    Ok(())
}

pub fn decode_transaction_status_meta(
    slot: u64,
    metadata_bytes: &[u8],
) -> Result<TransactionStatusMeta, MetadataDecodeError> {
    let mut meta = TransactionStatusMeta::default();
    decode_transaction_status_meta_into(slot, metadata_bytes, &mut meta)?;
    Ok(meta)
}

/// Decode block rewards from a reassembled rewards DataFrame.
///
/// Yellowstone Faithful stores the frame payload zstd-compressed in normal CARs. The uncompressed
/// payload is protobuf for newer epochs, but old epochs can contain one of two legacy bincode
/// shapes. Keep the same fallback order as upstream: protobuf, legacy `Rewards`, then legacy
/// `StoredConfirmedBlockRewards`.
pub fn decode_rewards_from_frame(
    reassembled_rewards: &[u8],
    out: &mut Rewards,
    zstd: &mut ZstdReusableDecoder,
) -> Result<(), RewardsDecodeError> {
    out.clear();

    if reassembled_rewards.is_empty() {
        return Ok(());
    }

    if zstd
        .decompress_if_zstd(reassembled_rewards)
        .map_err(RewardsDecodeError::ZstdDecompress)?
    {
        decode_rewards_into(zstd.output(), out)
    } else {
        decode_rewards_into(reassembled_rewards, out)
    }
}

pub fn decode_rewards_into(
    rewards_bytes: &[u8],
    out: &mut Rewards,
) -> Result<(), RewardsDecodeError> {
    out.clear();

    match Rewards::decode(rewards_bytes) {
        Ok(decoded) => {
            *out = decoded;
            Ok(())
        }
        Err(proto_err) => decode_bincode_rewards(rewards_bytes, out).map_err(|bincode_err| {
            RewardsDecodeError::Bincode(format!("{bincode_err}; protobuf fallback was {proto_err}"))
        }),
    }
}

fn decode_bincode_rewards(rewards_bytes: &[u8], out: &mut Rewards) -> Result<(), String> {
    if let Ok(rewards) = wincode::deserialize::<Vec<StoredExtendedReward>>(rewards_bytes) {
        out.rewards = rewards
            .iter()
            .map(crate::convert_metadata::stored_reward_to_proto)
            .collect();
        return Ok(());
    }

    let stored_rewards = wincode::deserialize::<Vec<StoredConfirmedBlockReward>>(rewards_bytes)
        .map_err(|err| err.to_string())?;
    out.rewards = stored_rewards
        .iter()
        .map(crate::convert_metadata::stored_confirmed_block_reward_to_proto)
        .collect();
    Ok(())
}

/// Decode protobuf-encoded transaction status metadata as a borrowed quick-protobuf view.
///
/// This only handles the protobuf wire format used after the bincode-era cutoff. The returned
/// value borrows strings and byte fields from `metadata_bytes`, so callers must keep the input
/// buffer alive for at least as long as the returned view.
///
/// This is an experimental fast path. The generated quick-protobuf struct represents proto3
/// optional scalar fields as plain scalars, so this wrapper restores exact presence for
/// `compute_units_consumed` and `cost_units` while decoding.
#[doc(hidden)]
#[inline]
pub fn decode_protobuf_transaction_status_meta_borrowed<'a>(
    metadata_bytes: &'a [u8],
) -> quick_protobuf::Result<BorrowedTransactionStatusMetaView<'a>> {
    let mut reader = BorrowedMetadataReader::new(metadata_bytes);
    let mut meta = BorrowedTransactionStatusMeta::default();
    let mut compute_units_consumed = None;
    let mut cost_units = None;

    // Keep this in lockstep with pb-rs generated TransactionStatusMeta::from_reader.
    while !reader.is_eof() {
        match reader.next_tag() {
            Ok(10) => meta.err = Some(reader.read_message::<BorrowedTransactionError>()?),
            Ok(16) => meta.fee = reader.read_uint64()?,
            Ok(26) => meta.pre_balances = reader.read_packed_u64()?,
            Ok(34) => meta.post_balances = reader.read_packed_u64()?,
            Ok(42) => {
                reserve_if_empty(&mut meta.inner_instructions, INNER_INSTRUCTIONS_RESERVE);
                meta.inner_instructions
                    .push(reader.read_message::<BorrowedInnerInstructions>()?);
            }
            Ok(80) => meta.inner_instructions_none = reader.read_bool()?,
            Ok(50) => {
                reserve_if_empty(&mut meta.log_messages, LOG_MESSAGES_RESERVE);
                meta.log_messages
                    .push(reader.read_string().map(Cow::Borrowed)?);
            }
            Ok(88) => meta.log_messages_none = reader.read_bool()?,
            Ok(58) => {
                reserve_if_empty(&mut meta.pre_token_balances, TOKEN_BALANCES_RESERVE);
                meta.pre_token_balances
                    .push(reader.read_message::<BorrowedTokenBalance>()?);
            }
            Ok(66) => {
                reserve_if_empty(&mut meta.post_token_balances, TOKEN_BALANCES_RESERVE);
                meta.post_token_balances
                    .push(reader.read_message::<BorrowedTokenBalance>()?);
            }
            Ok(74) => {
                reserve_if_empty(&mut meta.rewards, REWARDS_RESERVE);
                meta.rewards.push(reader.read_message::<BorrowedReward>()?);
            }
            Ok(98) => {
                reserve_if_empty(
                    &mut meta.loaded_writable_addresses,
                    LOADED_ADDRESSES_RESERVE,
                );
                meta.loaded_writable_addresses
                    .push(reader.read_bytes().map(Cow::Borrowed)?);
            }
            Ok(106) => {
                reserve_if_empty(
                    &mut meta.loaded_readonly_addresses,
                    LOADED_ADDRESSES_RESERVE,
                );
                meta.loaded_readonly_addresses
                    .push(reader.read_bytes().map(Cow::Borrowed)?);
            }
            Ok(114) => meta.return_data = Some(reader.read_message::<BorrowedReturnData>()?),
            Ok(120) => meta.return_data_none = reader.read_bool()?,
            Ok(128) => {
                let value = reader.read_uint64()?;
                meta.compute_units_consumed = value;
                compute_units_consumed = Some(value);
            }
            Ok(136) => {
                let value = reader.read_uint64()?;
                meta.cost_units = value;
                cost_units = Some(value);
            }
            Ok(tag) => {
                reader.skip_unknown(tag)?;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(BorrowedTransactionStatusMetaView {
        meta,
        compute_units_consumed,
        cost_units,
    })
}

/// Visit protobuf-encoded transaction status metadata without building a metadata struct.
///
/// This handles the protobuf wire format used after the bincode-era cutoff. Borrowed string and
/// byte slices passed to the visitor live as long as `metadata_bytes`.
#[inline]
pub fn visit_protobuf_transaction_status_meta<'a, V>(
    metadata_bytes: &'a [u8],
    visitor: &mut V,
) -> quick_protobuf::Result<()>
where
    V: TransactionStatusMetaVisitor<'a> + ?Sized,
{
    let wants_status_error = visitor.wants_status_error();
    let wants_pre_balances = visitor.wants_pre_balances();
    let wants_post_balances = visitor.wants_post_balances();
    let wants_inner_instructions = visitor.wants_inner_instructions();
    let wants_log_messages = visitor.wants_log_messages();
    let wants_pre_token_balances = visitor.wants_pre_token_balances();
    let wants_post_token_balances = visitor.wants_post_token_balances();
    let wants_rewards = visitor.wants_rewards();
    let wants_loaded_addresses = visitor.wants_loaded_addresses();
    let wants_return_data = visitor.wants_return_data();

    let mut reader = BorrowedMetadataReader::new(metadata_bytes);
    let mut saw_error = false;
    let mut pre_balance_index = 0usize;
    let mut post_balance_index = 0usize;

    while !reader.is_eof() {
        match reader.next_tag() {
            Ok(10) => {
                saw_error = true;
                let bytes = reader.read_bytes()?;
                if wants_status_error {
                    visitor.status_error(read_transaction_error(bytes)?);
                }
            }
            Ok(16) => visitor.fee(reader.read_uint64()?),
            Ok(24) => {
                let lamports = reader.read_uint64()?;
                if wants_pre_balances {
                    visitor.pre_balance(pre_balance_index, lamports);
                }
                pre_balance_index += 1;
            }
            Ok(26) => {
                if wants_pre_balances {
                    reader.visit_packed_u64(&mut pre_balance_index, |index, lamports| {
                        visitor.pre_balance(index, lamports);
                    })?;
                } else {
                    reader.skip_length_delimited()?;
                }
            }
            Ok(32) => {
                let lamports = reader.read_uint64()?;
                if wants_post_balances {
                    visitor.post_balance(post_balance_index, lamports);
                }
                post_balance_index += 1;
            }
            Ok(34) => {
                if wants_post_balances {
                    reader.visit_packed_u64(&mut post_balance_index, |index, lamports| {
                        visitor.post_balance(index, lamports);
                    })?;
                } else {
                    reader.skip_length_delimited()?;
                }
            }
            Ok(42) => {
                let bytes = reader.read_bytes()?;
                if wants_inner_instructions {
                    visitor.inner_instructions_raw(bytes);
                    read_inner_instructions_visit(bytes, visitor)?;
                }
            }
            Ok(80) => visitor.inner_instructions_none(reader.read_bool()?),
            Ok(50) => {
                if wants_log_messages {
                    visitor.log_message(reader.read_string()?);
                } else {
                    reader.skip_length_delimited()?;
                }
            }
            Ok(88) => visitor.log_messages_none(reader.read_bool()?),
            Ok(58) => {
                let bytes = reader.read_bytes()?;
                if wants_pre_token_balances {
                    visitor.pre_token_balance(read_token_balance_visit(bytes)?);
                }
            }
            Ok(66) => {
                let bytes = reader.read_bytes()?;
                if wants_post_token_balances {
                    visitor.post_token_balance(read_token_balance_visit(bytes)?);
                }
            }
            Ok(74) => {
                let bytes = reader.read_bytes()?;
                if wants_rewards {
                    visitor.reward_raw(bytes);
                }
            }
            Ok(98) => {
                let bytes = reader.read_bytes()?;
                if wants_loaded_addresses {
                    visitor.loaded_writable_address(bytes);
                }
            }
            Ok(106) => {
                let bytes = reader.read_bytes()?;
                if wants_loaded_addresses {
                    visitor.loaded_readonly_address(bytes);
                }
            }
            Ok(114) => {
                let bytes = reader.read_bytes()?;
                if wants_return_data {
                    visitor.return_data(read_return_data_visit(bytes)?);
                }
            }
            Ok(120) => visitor.return_data_none(reader.read_bool()?),
            Ok(128) => visitor.compute_units_consumed(reader.read_uint64()?),
            Ok(136) => visitor.cost_units(reader.read_uint64()?),
            Ok(tag) => reader.skip_unknown(tag)?,
            Err(err) => return Err(err),
        }
    }

    if !saw_error {
        visitor.status_ok();
    }

    Ok(())
}

#[inline]
fn reserve_if_empty<T>(values: &mut Vec<T>, additional: usize) {
    if values.is_empty() {
        values.reserve(additional);
    }
}

fn read_transaction_error(bytes: &[u8]) -> quick_protobuf::Result<&[u8]> {
    let mut reader = BorrowedMetadataReader::new(bytes);
    let mut err = &[][..];
    while !reader.is_eof() {
        match reader.next_tag() {
            Ok(10) => err = reader.read_bytes()?,
            Ok(tag) => reader.skip_unknown(tag)?,
            Err(err) => return Err(err),
        }
    }
    Ok(err)
}

fn read_inner_instructions_visit<'a, V>(
    bytes: &'a [u8],
    visitor: &mut V,
) -> quick_protobuf::Result<()>
where
    V: TransactionStatusMetaVisitor<'a> + ?Sized,
{
    let mut reader = BorrowedMetadataReader::new(bytes);
    let mut outer_instruction_index = 0u32;
    let mut instructions = Vec::new();
    while !reader.is_eof() {
        match reader.next_tag() {
            Ok(8) => outer_instruction_index = reader.read_uint32()?,
            Ok(18) => instructions.push(reader.read_bytes()?),
            Ok(tag) => reader.skip_unknown(tag)?,
            Err(err) => return Err(err),
        }
    }

    for (inner_instruction_index, bytes) in instructions.into_iter().enumerate() {
        let instruction = read_inner_instruction_visit(
            outer_instruction_index,
            inner_instruction_index as u32,
            bytes,
        )?;
        visitor.inner_instruction(instruction);
    }

    Ok(())
}

fn read_inner_instruction_visit(
    outer_instruction_index: u32,
    inner_instruction_index: u32,
    bytes: &[u8],
) -> quick_protobuf::Result<InnerInstructionVisit<'_>> {
    let mut reader = BorrowedMetadataReader::new(bytes);
    let mut instruction = InnerInstructionVisit {
        outer_instruction_index,
        inner_instruction_index,
        ..Default::default()
    };
    while !reader.is_eof() {
        match reader.next_tag() {
            Ok(8) => instruction.program_id_index = reader.read_uint32()?,
            Ok(18) => instruction.accounts = reader.read_bytes()?,
            Ok(26) => instruction.data = reader.read_bytes()?,
            Ok(32) => instruction.stack_height = Some(reader.read_uint32()?),
            Ok(tag) => reader.skip_unknown(tag)?,
            Err(err) => return Err(err),
        }
    }
    Ok(instruction)
}

fn read_token_balance_visit(bytes: &[u8]) -> quick_protobuf::Result<TokenBalanceVisit<'_>> {
    let mut reader = BorrowedMetadataReader::new(bytes);
    let mut token_balance = TokenBalanceVisit::default();
    while !reader.is_eof() {
        match reader.next_tag() {
            Ok(8) => token_balance.account_index = reader.read_uint32()?,
            Ok(18) => token_balance.mint = reader.read_string()?,
            Ok(26) => {
                token_balance.ui_token_amount =
                    Some(read_ui_token_amount_visit(reader.read_bytes()?)?)
            }
            Ok(34) => token_balance.owner = reader.read_string()?,
            Ok(42) => token_balance.program_id = reader.read_string()?,
            Ok(tag) => reader.skip_unknown(tag)?,
            Err(err) => return Err(err),
        }
    }
    Ok(token_balance)
}

fn read_ui_token_amount_visit(bytes: &[u8]) -> quick_protobuf::Result<UiTokenAmountVisit<'_>> {
    let mut reader = BorrowedMetadataReader::new(bytes);
    let mut ui_token_amount = UiTokenAmountVisit::default();
    while !reader.is_eof() {
        match reader.next_tag() {
            Ok(9) => ui_token_amount.ui_amount = reader.read_double()?,
            Ok(16) => ui_token_amount.decimals = reader.read_uint32()?,
            Ok(26) => ui_token_amount.amount = reader.read_string()?,
            Ok(34) => ui_token_amount.ui_amount_string = reader.read_string()?,
            Ok(tag) => reader.skip_unknown(tag)?,
            Err(err) => return Err(err),
        }
    }
    Ok(ui_token_amount)
}

fn read_return_data_visit(bytes: &[u8]) -> quick_protobuf::Result<ReturnDataVisit<'_>> {
    let mut reader = BorrowedMetadataReader::new(bytes);
    let mut return_data = ReturnDataVisit::default();
    while !reader.is_eof() {
        match reader.next_tag() {
            Ok(10) => return_data.program_id = reader.read_bytes()?,
            Ok(18) => return_data.data = reader.read_bytes()?,
            Ok(tag) => reader.skip_unknown(tag)?,
            Err(err) => return Err(err),
        }
    }
    Ok(return_data)
}

struct BorrowedMetadataReader<'a> {
    bytes: &'a [u8],
    pos: usize,
    end: usize,
}

impl<'a> BorrowedMetadataReader<'a> {
    #[inline]
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            pos: 0,
            end: bytes.len(),
        }
    }

    #[inline]
    fn is_eof(&self) -> bool {
        self.pos >= self.end
    }

    #[inline]
    fn next_tag(&mut self) -> quick_protobuf::Result<u32> {
        let value = self.read_varint64()?;
        u32::try_from(value).map_err(|_| QuickProtobufError::Varint)
    }

    #[inline]
    fn read_bool(&mut self) -> quick_protobuf::Result<bool> {
        Ok(self.read_varint64()? != 0)
    }

    #[inline]
    fn read_uint64(&mut self) -> quick_protobuf::Result<u64> {
        self.read_varint64()
    }

    #[inline]
    fn read_uint32(&mut self) -> quick_protobuf::Result<u32> {
        let value = self.read_varint64()?;
        u32::try_from(value).map_err(|_| QuickProtobufError::Varint)
    }

    #[inline]
    fn read_double(&mut self) -> quick_protobuf::Result<f64> {
        Ok(f64::from_bits(self.read_fixed64()?))
    }

    #[inline]
    fn read_fixed64(&mut self) -> quick_protobuf::Result<u64> {
        let start = self.pos;
        self.advance(8)?;
        let bytes = &self.bytes[start..self.pos];
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    #[inline]
    fn read_bytes(&mut self) -> quick_protobuf::Result<&'a [u8]> {
        let len = self.read_len()?;
        let start = self.pos;
        self.advance(len)?;
        Ok(&self.bytes[start..self.pos])
    }

    #[inline]
    fn read_string(&mut self) -> quick_protobuf::Result<&'a str> {
        core::str::from_utf8(self.read_bytes()?).map_err(Into::into)
    }

    #[inline]
    fn read_message<M>(&mut self) -> quick_protobuf::Result<M>
    where
        M: MessageRead<'a>,
    {
        let bytes = self.read_bytes()?;
        let mut reader = BytesReader::from_bytes(bytes);
        M::from_reader(&mut reader, bytes)
    }

    #[inline]
    fn read_packed_u64(&mut self) -> quick_protobuf::Result<Vec<u64>> {
        let bytes = self.read_bytes()?;
        let count = count_varints(bytes)?;
        let mut values = Vec::with_capacity(count);
        let mut pos = 0usize;
        while pos < bytes.len() {
            values.push(read_varint64_from(bytes, &mut pos, bytes.len())?);
        }
        Ok(values)
    }

    #[inline]
    fn visit_packed_u64(
        &mut self,
        index: &mut usize,
        mut visit: impl FnMut(usize, u64),
    ) -> quick_protobuf::Result<()> {
        let bytes = self.read_bytes()?;
        let mut pos = 0usize;
        while pos < bytes.len() {
            let value = read_varint64_from(bytes, &mut pos, bytes.len())?;
            visit(*index, value);
            *index += 1;
        }
        Ok(())
    }

    #[inline]
    fn skip_length_delimited(&mut self) -> quick_protobuf::Result<()> {
        let len = self.read_len()?;
        self.advance(len)
    }

    fn skip_unknown(&mut self, tag: u32) -> quick_protobuf::Result<()> {
        self.skip_wire_type((tag & 0x7) as u8)
    }

    fn skip_wire_type(&mut self, wire: u8) -> quick_protobuf::Result<()> {
        match wire {
            0 => {
                let _ = self.read_varint64()?;
            }
            1 => self.advance(8)?,
            2 => {
                let len = self.read_len()?;
                self.advance(len)?;
            }
            3 => loop {
                let tag = self.next_tag()?;
                let wire = (tag & 0x7) as u8;
                if wire == 4 {
                    break;
                }
                self.skip_wire_type(wire)?;
            },
            4 => return Err(QuickProtobufError::Deprecated("unexpected end group")),
            5 => self.advance(4)?,
            other => return Err(QuickProtobufError::UnknownWireType(other)),
        }
        Ok(())
    }

    #[inline]
    fn read_len(&mut self) -> quick_protobuf::Result<usize> {
        let len = self.read_varint64()?;
        usize::try_from(len).map_err(|_| QuickProtobufError::Varint)
    }

    #[inline]
    fn read_varint64(&mut self) -> quick_protobuf::Result<u64> {
        read_varint64_from(self.bytes, &mut self.pos, self.end)
    }

    #[inline]
    fn advance(&mut self, len: usize) -> quick_protobuf::Result<()> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(QuickProtobufError::UnexpectedEndOfBuffer)?;
        if end > self.end {
            return Err(QuickProtobufError::UnexpectedEndOfBuffer);
        }
        self.pos = end;
        Ok(())
    }
}

fn count_varints(bytes: &[u8]) -> quick_protobuf::Result<usize> {
    let mut pos = 0usize;
    let mut count = 0usize;
    while pos < bytes.len() {
        let _ = read_varint64_from(bytes, &mut pos, bytes.len())?;
        count += 1;
    }
    Ok(count)
}

fn read_varint64_from(bytes: &[u8], pos: &mut usize, end: usize) -> quick_protobuf::Result<u64> {
    let mut out = 0u64;
    let mut shift = 0u32;

    for _ in 0..10 {
        if *pos >= end {
            return Err(QuickProtobufError::UnexpectedEndOfBuffer);
        }
        let byte = bytes[*pos];
        *pos += 1;

        out |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(out);
        }
        shift += 7;
    }

    Err(QuickProtobufError::Varint)
}

#[inline(always)]
fn decode_bincode(
    out: &mut TransactionStatusMeta,
    slot: u64,
    metadata_bytes: &[u8],
) -> Result<(), MetadataDecodeError> {
    *out = wincode::deserialize::<StoredTransactionStatusMeta>(metadata_bytes)
        .map(Into::into)
        .or_else(|_| {
            wincode::deserialize::<StoredTransactionStatusMetaLegacyLogs>(metadata_bytes)
                .map(Into::into)
        })
        .inspect_err(|_err| println!("invalid bincode metadata : {slot} {:?}", metadata_bytes))
        .map_err(|err| MetadataDecodeError::Bincode(err.to_string()))?;
    Ok(())
}

#[inline(always)]
fn decode_bincode_2(
    out: &mut TransactionStatusMeta,
    slot: u64,
    metadata_bytes: &[u8],
) -> Result<(), MetadataDecodeError> {
    *out = wincode::deserialize::<StoredTransactionStatusMetaV2>(metadata_bytes)
        .inspect_err(|_err| println!("invalid bincode metadata : {slot} {:?}", metadata_bytes))
        .map_err(|err| MetadataDecodeError::Bincode(err.to_string()))?
        .into();
    Ok(())
}

#[inline(always)]
fn decode_protobuf(
    out: &mut TransactionStatusMeta,
    slot: u64,
    metadata_bytes: &[u8],
) -> Result<(), MetadataDecodeError> {
    out.merge(metadata_bytes)
        .inspect_err(|_err| println!("invalid protobuf metadata : {slot} {:?}", metadata_bytes))
        .map_err(MetadataDecodeError::ProstDecode)?;
    Ok(())
}

#[inline(always)]
pub const fn slot_to_epoch(slot: u64) -> u64 {
    slot / 432000
}

#[inline(always)]
pub const fn slot_uses_protobuf_metadata(slot: u64) -> bool {
    let epoch = slot_to_epoch(slot);
    epoch > BINCODE_EPOCH_CUTOFF
        || epoch == 148
        || epoch == 153
        || (epoch == 156 && !(67_681_336 <= slot && slot < 67_769_679))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_protobuf_decode_reuses_input_for_strings_and_bytes() {
        let meta = TransactionStatusMeta {
            err: Some(crate::confirmed_block::TransactionError { err: vec![1, 2, 3] }),
            log_messages: vec!["Program log: borrowed".to_string()],
            pre_token_balances: vec![crate::confirmed_block::TokenBalance {
                account_index: 2,
                mint: "Mint111111111111111111111111111111111111111".to_string(),
                ui_token_amount: Some(crate::confirmed_block::UiTokenAmount {
                    amount: "12345".to_string(),
                    decimals: 6,
                    ui_amount_string: "0.012345".to_string(),
                    ..Default::default()
                }),
                owner: "Owner11111111111111111111111111111111111111".to_string(),
                program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            }],
            loaded_writable_addresses: vec![vec![7; 32]],
            compute_units_consumed: Some(42),
            cost_units: Some(0),
            ..Default::default()
        };
        let bytes = meta.encode_to_vec();

        let borrowed = decode_protobuf_transaction_status_meta_borrowed(&bytes).unwrap();

        assert_eq!(borrowed.err.as_ref().unwrap().err.as_ref(), &[1, 2, 3]);
        assert_eq!(borrowed.log_messages[0].as_ref(), "Program log: borrowed");
        assert_eq!(
            borrowed.pre_token_balances[0].mint.as_ref(),
            "Mint111111111111111111111111111111111111111"
        );
        assert_eq!(borrowed.loaded_writable_addresses[0].as_ref(), &[7; 32]);
        assert_eq!(borrowed.compute_units_consumed, Some(42));
        assert_eq!(borrowed.cost_units, Some(0));
    }

    #[test]
    fn borrowed_protobuf_decode_preserves_absent_compute_and_cost_units() {
        let bytes = TransactionStatusMeta::default().encode_to_vec();
        let borrowed = decode_protobuf_transaction_status_meta_borrowed(&bytes).unwrap();

        assert_eq!(borrowed.compute_units_consumed, None);
        assert_eq!(borrowed.cost_units, None);
    }

    #[test]
    fn visitor_protobuf_decode_visits_selected_fields_without_building_meta() {
        let meta = TransactionStatusMeta {
            err: Some(crate::confirmed_block::TransactionError { err: vec![9, 8, 7] }),
            fee: 5000,
            pre_balances: vec![10, 20],
            post_balances: vec![5, 25],
            log_messages: vec!["Program log: visitor".to_string()],
            pre_token_balances: vec![crate::confirmed_block::TokenBalance {
                account_index: 3,
                mint: "Mint111111111111111111111111111111111111111".to_string(),
                ui_token_amount: Some(crate::confirmed_block::UiTokenAmount {
                    amount: "12345".to_string(),
                    decimals: 6,
                    ui_amount_string: "0.012345".to_string(),
                    ..Default::default()
                }),
                owner: "Owner11111111111111111111111111111111111111".to_string(),
                program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
            }],
            loaded_writable_addresses: vec![vec![7; 32]],
            return_data: Some(crate::confirmed_block::ReturnData {
                program_id: vec![1; 32],
                data: vec![2, 3, 4],
            }),
            compute_units_consumed: Some(42),
            cost_units: Some(13),
            ..Default::default()
        };
        let bytes = meta.encode_to_vec();
        let mut visitor = RecordingVisitor::default();

        visit_protobuf_transaction_status_meta(&bytes, &mut visitor).unwrap();

        assert_eq!(visitor.status_error, Some(&[9, 8, 7][..]));
        assert!(!visitor.status_ok);
        assert_eq!(visitor.fee, Some(5000));
        assert_eq!(visitor.pre_balances, vec![(0, 10), (1, 20)]);
        assert_eq!(visitor.post_balances, vec![(0, 5), (1, 25)]);
        assert_eq!(visitor.log_messages, vec!["Program log: visitor"]);
        assert_eq!(visitor.pre_token_balances.len(), 1);
        assert_eq!(visitor.pre_token_balances[0].account_index, 3);
        assert_eq!(
            visitor.pre_token_balances[0].mint,
            "Mint111111111111111111111111111111111111111"
        );
        assert_eq!(
            visitor.pre_token_balances[0]
                .ui_token_amount
                .as_ref()
                .unwrap()
                .amount,
            "12345"
        );
        assert_eq!(visitor.loaded_writable_addresses, vec![&[7; 32][..]]);
        assert_eq!(
            visitor.return_data,
            Some(ReturnDataVisit {
                program_id: &[1; 32],
                data: &[2, 3, 4],
            })
        );
        assert_eq!(visitor.compute_units_consumed, Some(42));
        assert_eq!(visitor.cost_units, Some(13));
    }

    #[test]
    fn visitor_protobuf_decode_reports_ok_when_error_absent() {
        let bytes = TransactionStatusMeta::default().encode_to_vec();
        let mut visitor = RecordingVisitor::default();

        visit_protobuf_transaction_status_meta(&bytes, &mut visitor).unwrap();

        assert!(visitor.status_ok);
        assert_eq!(visitor.status_error, None);
    }

    #[test]
    fn visitor_protobuf_decode_visits_inner_instructions() {
        let meta = TransactionStatusMeta {
            inner_instructions: vec![crate::confirmed_block::InnerInstructions {
                index: 2,
                instructions: vec![crate::confirmed_block::InnerInstruction {
                    program_id_index: 3,
                    accounts: vec![4, 5],
                    data: vec![6, 7],
                    stack_height: Some(8),
                }],
            }],
            ..Default::default()
        };
        let bytes = meta.encode_to_vec();
        let mut visitor = RecordingVisitor::default();

        visit_protobuf_transaction_status_meta(&bytes, &mut visitor).unwrap();

        assert_eq!(
            visitor.inner_instructions,
            vec![InnerInstructionVisit {
                outer_instruction_index: 2,
                inner_instruction_index: 0,
                stack_height: Some(8),
                program_id_index: 3,
                accounts: &[4, 5],
                data: &[6, 7],
            }]
        );
    }

    #[derive(Default)]
    struct RecordingVisitor<'a> {
        status_ok: bool,
        status_error: Option<&'a [u8]>,
        fee: Option<u64>,
        pre_balances: Vec<(usize, u64)>,
        post_balances: Vec<(usize, u64)>,
        log_messages: Vec<&'a str>,
        pre_token_balances: Vec<TokenBalanceVisit<'a>>,
        inner_instructions: Vec<InnerInstructionVisit<'a>>,
        loaded_writable_addresses: Vec<&'a [u8]>,
        return_data: Option<ReturnDataVisit<'a>>,
        compute_units_consumed: Option<u64>,
        cost_units: Option<u64>,
    }

    impl<'a> TransactionStatusMetaVisitor<'a> for RecordingVisitor<'a> {
        fn wants_status_error(&self) -> bool {
            true
        }

        fn wants_pre_balances(&self) -> bool {
            true
        }

        fn wants_post_balances(&self) -> bool {
            true
        }

        fn wants_log_messages(&self) -> bool {
            true
        }

        fn wants_pre_token_balances(&self) -> bool {
            true
        }

        fn wants_inner_instructions(&self) -> bool {
            true
        }

        fn wants_loaded_addresses(&self) -> bool {
            true
        }

        fn wants_return_data(&self) -> bool {
            true
        }

        fn status_ok(&mut self) {
            self.status_ok = true;
        }

        fn status_error(&mut self, err: &'a [u8]) {
            self.status_error = Some(err);
        }

        fn fee(&mut self, fee: u64) {
            self.fee = Some(fee);
        }

        fn pre_balance(&mut self, index: usize, lamports: u64) {
            self.pre_balances.push((index, lamports));
        }

        fn post_balance(&mut self, index: usize, lamports: u64) {
            self.post_balances.push((index, lamports));
        }

        fn log_message(&mut self, message: &'a str) {
            self.log_messages.push(message);
        }

        fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
            self.pre_token_balances.push(balance);
        }

        fn inner_instruction(&mut self, instruction: InnerInstructionVisit<'a>) {
            self.inner_instructions.push(instruction);
        }

        fn loaded_writable_address(&mut self, address: &'a [u8]) {
            self.loaded_writable_addresses.push(address);
        }

        fn return_data(&mut self, return_data: ReturnDataVisit<'a>) {
            self.return_data = Some(return_data);
        }

        fn compute_units_consumed(&mut self, units: u64) {
            self.compute_units_consumed = Some(units);
        }

        fn cost_units(&mut self, units: u64) {
            self.cost_units = Some(units);
        }
    }

    #[test]
    fn decode_156_67684801_ui_str_amount_empty() {
        let metadata = &[
            0, 0, 0, 0, 168, 97, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 65, 162, 45, 59, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 96, 77, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 216, 181, 116, 7, 185, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 128, 81, 99, 67, 0, 0, 0, 0,
            11, 0, 0, 0, 0, 0, 0, 0, 217, 200, 176, 58, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0,
            240, 29, 31, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0, 96,
            77, 22, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 216, 181, 116,
            7, 185, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 128, 81, 99, 67, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 4, 0, 0, 0, 0, 0, 0, 0, 2, 43, 0, 0, 0, 0, 0,
            0, 0, 83, 82, 77, 117, 65, 112, 86, 78, 100, 120, 88, 111, 107, 107, 53, 71, 84, 55,
            88, 68, 53, 99, 85, 85, 103, 88, 77, 66, 67, 111, 65, 122, 50, 76, 72, 101, 117, 65,
            111, 75, 87, 82, 116, 1, 0, 0, 0, 0, 0, 0, 0, 0, 6, 1, 0, 0, 0, 0, 0, 0, 0, 48, 1, 0,
            0, 0, 0, 0, 0, 0, 48, 3, 43, 0, 0, 0, 0, 0, 0, 0, 83, 82, 77, 117, 65, 112, 86, 78,
            100, 120, 88, 111, 107, 107, 53, 71, 84, 55, 88, 68, 53, 99, 85, 85, 103, 88, 77, 66,
            67, 111, 65, 122, 50, 76, 72, 101, 117, 65, 111, 75, 87, 82, 116, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 6, 1, 0, 0, 0, 0, 0, 0, 0, 48, 1, 0, 0, 0, 0, 0, 0, 0, 48, 4, 43, 0, 0, 0, 0, 0,
            0, 0, 83, 82, 77, 117, 65, 112, 86, 78, 100, 120, 88, 111, 107, 107, 53, 71, 84, 55,
            88, 68, 53, 99, 85, 85, 103, 88, 77, 66, 67, 111, 65, 122, 50, 76, 72, 101, 117, 65,
            111, 75, 87, 82, 116, 1, 0, 0, 0, 0, 0, 0, 0, 0, 6, 1, 0, 0, 0, 0, 0, 0, 0, 48, 1, 0,
            0, 0, 0, 0, 0, 0, 48, 1, 44, 0, 0, 0, 0, 0, 0, 0, 52, 87, 83, 84, 111, 88, 75, 111,
            119, 85, 106, 119, 72, 90, 81, 118, 105, 77, 78, 83, 115, 72, 69, 112, 102, 89, 98, 72,
            57, 77, 74, 83, 117, 113, 104, 121, 119, 87, 118, 54, 118, 84, 77, 106, 1, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let slot = 67684801;

        let mut out = TransactionStatusMeta::default();

        let res = decode_transaction_status_meta_into(slot, metadata, &mut out)
            .inspect_err(|err| println!("{err}"));
        assert!(res.is_ok())
    }

    #[test]
    fn decode_156_67681336_v2_token_amount() {
        let metadata = &[
            0, 0, 0, 0, 16, 39, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 72, 118, 213, 243, 220,
            4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0,
            0, 0, 192, 56, 7, 0, 0, 0, 0, 128, 81, 99, 67, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 56,
            79, 213, 243, 220, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 29, 31, 0, 0, 0, 0, 0, 240,
            29, 31, 0, 0, 0, 0, 0, 0, 192, 56, 7, 0, 0, 0, 0, 128, 81, 99, 67, 0, 0, 0, 0, 0, 1, 0,
            0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 3, 43, 0, 0, 0, 0, 0, 0, 0, 107, 105,
            110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109, 85, 103,
            116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65, 72, 74,
            113, 54, 1, 0, 0, 0, 0, 0, 184, 149, 64, 5, 9, 0, 0, 0, 0, 0, 0, 0, 49, 51, 57, 48, 48,
            48, 48, 48, 48, 4, 0, 0, 0, 0, 0, 0, 0, 49, 51, 57, 48, 2, 43, 0, 0, 0, 0, 0, 0, 0,
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, 1, 70, 153, 141, 175, 228, 222, 157, 65, 5, 14, 0, 0, 0, 0, 0, 0, 0,
            49, 50, 53, 50, 56, 54, 54, 57, 57, 56, 56, 56, 50, 56, 15, 0, 0, 0, 0, 0, 0, 0, 49,
            50, 53, 50, 56, 54, 54, 57, 57, 46, 56, 56, 56, 50, 56, 1, 2, 0, 0, 0, 0, 0, 0, 0, 2,
            43, 0, 0, 0, 0, 0, 0, 0, 107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69,
            117, 81, 110, 113, 109, 85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67,
            101, 86, 88, 53, 105, 65, 72, 74, 113, 54, 1, 70, 153, 141, 215, 228, 222, 157, 65, 5,
            14, 0, 0, 0, 0, 0, 0, 0, 49, 50, 53, 50, 56, 54, 55, 48, 57, 56, 56, 56, 50, 56, 15, 0,
            0, 0, 0, 0, 0, 0, 49, 50, 53, 50, 56, 54, 55, 48, 57, 46, 56, 56, 56, 50, 56, 3, 43, 0,
            0, 0, 0, 0, 0, 0, 107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117,
            81, 110, 113, 109, 85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101,
            86, 88, 53, 105, 65, 72, 74, 113, 54, 1, 0, 0, 0, 0, 0, 144, 149, 64, 5, 9, 0, 0, 0, 0,
            0, 0, 0, 49, 51, 56, 48, 48, 48, 48, 48, 48, 4, 0, 0, 0, 0, 0, 0, 0, 49, 51, 56, 48,
        ];

        let slot = 67681336;

        let mut out = TransactionStatusMeta::default();

        let res = decode_transaction_status_meta_into(slot, metadata, &mut out)
            .inspect_err(|err| println!("{err}"));
        assert!(res.is_ok())
    }

    #[test]
    fn decode_148_first() {
        let metadata = &[
            16, 136, 39, 26, 14, 133, 136, 213, 196, 188, 9, 146, 130, 204, 203, 2, 1, 1, 1, 34,
            14, 253, 224, 212, 196, 188, 9, 146, 130, 204, 203, 2, 1, 1, 1, 50, 62, 80, 114, 111,
            103, 114, 97, 109, 32, 86, 111, 116, 101, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49,
            49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49,
            49, 49, 49, 49, 49, 49, 32, 105, 110, 118, 111, 107, 101, 32, 91, 49, 93, 50, 59, 80,
            114, 111, 103, 114, 97, 109, 32, 86, 111, 116, 101, 49, 49, 49, 49, 49, 49, 49, 49, 49,
            49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49, 49,
            49, 49, 49, 49, 49, 49, 49, 49, 32, 115, 117, 99, 99, 101, 115, 115,
        ];
        let slot = (148) * 432_000;

        let mut out = TransactionStatusMeta::default();

        let res = decode_transaction_status_meta_into(slot, metadata, &mut out)
            .inspect_err(|err| println!("{err}"));
        assert!(res.is_ok())
    }

    #[test]
    fn decode_153_first() {
        let metadata = &[
            10, 15, 10, 13, 8, 0, 0, 0, 1, 25, 0, 0, 0, 41, 0, 0, 0, 16, 136, 39, 26, 61, 128, 188,
            173, 247, 145, 183, 25, 192, 153, 219, 1, 192, 188, 186, 17, 192, 188, 187, 230, 6,
            192, 188, 251, 217, 1, 192, 188, 251, 217, 1, 240, 187, 124, 240, 187, 124, 192, 210,
            145, 11, 192, 210, 145, 11, 240, 187, 124, 240, 187, 124, 0, 128, 155, 175, 149, 4,
            128, 151, 154, 229, 5, 34, 61, 248, 148, 173, 247, 145, 183, 25, 192, 153, 219, 1, 192,
            188, 186, 17, 192, 188, 187, 230, 6, 192, 188, 251, 217, 1, 192, 188, 251, 217, 1, 240,
            187, 124, 240, 187, 124, 192, 210, 145, 11, 192, 210, 145, 11, 240, 187, 124, 240, 187,
            124, 0, 128, 155, 175, 149, 4, 128, 151, 154, 229, 5, 50, 63, 80, 114, 111, 103, 114,
            97, 109, 32, 69, 85, 113, 111, 106, 119, 87, 65, 50, 114, 100, 49, 57, 70, 90, 114,
            122, 101, 66, 110, 99, 74, 115, 109, 51, 56, 74, 109, 49, 104, 69, 104, 69, 51, 122,
            115, 109, 88, 51, 98, 82, 99, 50, 111, 32, 105, 110, 118, 111, 107, 101, 32, 91, 49,
            93, 50, 90, 80, 114, 111, 103, 114, 97, 109, 32, 69, 85, 113, 111, 106, 119, 87, 65,
            50, 114, 100, 49, 57, 70, 90, 114, 122, 101, 66, 110, 99, 74, 115, 109, 51, 56, 74,
            109, 49, 104, 69, 104, 69, 51, 122, 115, 109, 88, 51, 98, 82, 99, 50, 111, 32, 99, 111,
            110, 115, 117, 109, 101, 100, 32, 53, 49, 53, 56, 32, 111, 102, 32, 50, 48, 48, 48, 48,
            48, 32, 99, 111, 109, 112, 117, 116, 101, 32, 117, 110, 105, 116, 115, 50, 60, 80, 114,
            111, 103, 114, 97, 109, 32, 69, 85, 113, 111, 106, 119, 87, 65, 50, 114, 100, 49, 57,
            70, 90, 114, 122, 101, 66, 110, 99, 74, 115, 109, 51, 56, 74, 109, 49, 104, 69, 104,
            69, 51, 122, 115, 109, 88, 51, 98, 82, 99, 50, 111, 32, 115, 117, 99, 99, 101, 115,
            115, 50, 63, 80, 114, 111, 103, 114, 97, 109, 32, 69, 85, 113, 111, 106, 119, 87, 65,
            50, 114, 100, 49, 57, 70, 90, 114, 122, 101, 66, 110, 99, 74, 115, 109, 51, 56, 74,
            109, 49, 104, 69, 104, 69, 51, 122, 115, 109, 88, 51, 98, 82, 99, 50, 111, 32, 105,
            110, 118, 111, 107, 101, 32, 91, 49, 93, 50, 90, 80, 114, 111, 103, 114, 97, 109, 32,
            69, 85, 113, 111, 106, 119, 87, 65, 50, 114, 100, 49, 57, 70, 90, 114, 122, 101, 66,
            110, 99, 74, 115, 109, 51, 56, 74, 109, 49, 104, 69, 104, 69, 51, 122, 115, 109, 88,
            51, 98, 82, 99, 50, 111, 32, 99, 111, 110, 115, 117, 109, 101, 100, 32, 52, 52, 51, 55,
            32, 111, 102, 32, 50, 48, 48, 48, 48, 48, 32, 99, 111, 109, 112, 117, 116, 101, 32,
            117, 110, 105, 116, 115, 50, 87, 80, 114, 111, 103, 114, 97, 109, 32, 69, 85, 113, 111,
            106, 119, 87, 65, 50, 114, 100, 49, 57, 70, 90, 114, 122, 101, 66, 110, 99, 74, 115,
            109, 51, 56, 74, 109, 49, 104, 69, 104, 69, 51, 122, 115, 109, 88, 51, 98, 82, 99, 50,
            111, 32, 102, 97, 105, 108, 101, 100, 58, 32, 99, 117, 115, 116, 111, 109, 32, 112,
            114, 111, 103, 114, 97, 109, 32, 101, 114, 114, 111, 114, 58, 32, 48, 120, 50, 57,
        ];
        let slot = (153) * 432_000;

        let mut out = TransactionStatusMeta::default();

        let res = decode_transaction_status_meta_into(slot, metadata, &mut out)
            .inspect_err(|err| println!("{err}"));
        assert!(res.is_ok())
    }

    #[test]
    fn decode_early_epoch_metadata() {
        let metadata: &[u8] = &[
            0, 0, 0, 0, // status: Ok(())
            16, 39, 0, 0, 0, 0, 0, 0, // fee: u64 = 10_000
            // pre_balances: Vec<u64>
            6, 0, 0, 0, 0, 0, 0, 0, // pre_balances len = 6
            72, 25, 41, 83, 215, 9, 0, 0, // pre_balances[0] (u64 LE)
            0, 0, 0, 0, 0, 0, 0, 0, // pre_balances[1]
            240, 29, 31, 0, 0, 0, 0, 0, // pre_balances[2]
            240, 29, 31, 0, 0, 0, 0, 0, // pre_balances[3]
            0, 192, 56, 7, 0, 0, 0, 0, // pre_balances[4]
            128, 205, 171, 66, 0, 0, 0, 0, // pre_balances[5]
            // post_balances: Vec<u64>
            6, 0, 0, 0, 0, 0, 0, 0, // post_balances len = 6
            56, 242, 40, 83, 215, 9, 0, 0, // post_balances[0]
            0, 0, 0, 0, 0, 0, 0, 0, // post_balances[1]
            240, 29, 31, 0, 0, 0, 0, 0, // post_balances[2]
            240, 29, 31, 0, 0, 0, 0, 0, // post_balances[3]
            0, 192, 56, 7, 0, 0, 0, 0, // post_balances[4]
            128, 205, 171, 66, 0, 0, 0, 0, // post_balances[5]
            // inner_instructions: Option<Vec<InnerInstructions>>
            1, // Option::Some
            0, 0, 0, 0, 0, 0, 0, 0, // Vec len = 0 (Some(vec![]))
            // log_messages: Option<Vec<String>>
            1, // Option::Some
            7, 0, 0, 0, 0, 0, 0, 0, // log_messages len = 7
            62, 0, 0, 0, 0, 0, 0, 0, // log[0] string len = 62
            80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102,
            72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69,
            113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 105, 110, 118, 111, 107, 101,
            32, 91, 49, 93, 88, 0, 0, 0, 0, 0, 0, 0, // log[1] string len = 88
            80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102,
            72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69,
            113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 99, 111, 110, 115, 117, 109,
            101, 100, 32, 51, 49, 50, 32, 111, 102, 32, 50, 48, 48, 48, 48, 48, 32, 99, 111, 109,
            112, 117, 116, 101, 32, 117, 110, 105, 116, 115, 59, 59, 0, 0, 0, 0, 0, 0,
            0, // log[2] string len = 59
            80, 114, 111, 103, 114, 97, 109, 32, 77, 101, 109, 111, 49, 85, 104, 107, 74, 82, 102,
            72, 121, 118, 76, 77, 99, 86, 117, 99, 74, 119, 120, 88, 101, 117, 68, 55, 50, 56, 69,
            113, 86, 68, 68, 119, 81, 68, 120, 70, 77, 78, 111, 32, 115, 117, 99, 99, 101, 115,
            115, 62, 0, 0, 0, 0, 0, 0, 0, // log[3] string len = 62
            80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102,
            101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66,
            118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 105, 110, 118, 111, 107,
            101, 32, 91, 49, 93, 34, 0, 0, 0, 0, 0, 0, 0, // log[4] string len = 34
            80, 114, 111, 103, 114, 97, 109, 32, 108, 111, 103, 58, 32, 73, 110, 115, 116, 114,
            117, 99, 116, 105, 111, 110, 58, 32, 84, 114, 97, 110, 115, 102, 101, 114, 89, 0, 0, 0,
            0, 0, 0, 0, // log[5] string len = 89
            80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102,
            101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66,
            118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 99, 111, 110, 115, 117, 109,
            101, 100, 32, 53, 51, 50, 56, 32, 111, 102, 32, 50, 48, 48, 48, 48, 48, 32, 99, 111,
            109, 112, 117, 116, 101, 32, 117, 110, 105, 116, 115, 59, 59, 0, 0, 0, 0, 0, 0,
            0, // log[6] string len = 59
            80, 114, 111, 103, 114, 97, 109, 32, 84, 111, 107, 101, 110, 107, 101, 103, 81, 102,
            101, 90, 121, 105, 78, 119, 65, 74, 98, 78, 98, 71, 75, 80, 70, 88, 67, 87, 117, 66,
            118, 102, 57, 83, 115, 54, 50, 51, 86, 81, 53, 68, 65, 32, 115, 117, 99, 99, 101, 115,
            115, // pre_token_balances: Option<Vec<TokenBalance>>
            1,   // Option::Some
            2, 0, 0, 0, 0, 0, 0, 0, // len = 2
            2, // token_balance[0].account_index
            43, 0, 0, 0, 0, 0, 0, 0, // token_balance[0].mint len = 43
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, // token_balance[0].ui_token_amount: StoredTokenAmount
            236, 23, 236, 20, 137, 62, 95, 65, // ui_amount: f64 = 8_190_500.32691
            5,  // decimals: u8
            12, 0, 0, 0, 0, 0, 0, 0, // amount String len = 12
            56, 49, 57, 48, 53, 48, 48, 51, 50, 54, 57, 49, // "819050032691"
            3,  // token_balance[1].account_index
            43, 0, 0, 0, 0, 0, 0, 0, // token_balance[1].mint len = 43
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, // token_balance[1].ui_token_amount: StoredTokenAmount
            0, 0, 0, 0, 0, 112, 167, 64, // ui_amount: f64 = 3000.0
            5,  // decimals: u8
            9, 0, 0, 0, 0, 0, 0, 0, // amount String len = 9
            51, 48, 48, 48, 48, 48, 48, 48, 48, // "300000000"
            // post_token_balances: Option<Vec<TokenBalance>>
            1, // Option::Some
            2, 0, 0, 0, 0, 0, 0, 0, // len = 2
            3, // token_balance[0].account_index
            43, 0, 0, 0, 0, 0, 0, 0, // token_balance[0].mint len = 43
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, // token_balance[0].ui_token_amount: StoredTokenAmount
            0, 0, 0, 0, 0, 64, 159, 64, // ui_amount: f64 = 2000.0
            5,  // decimals: u8
            9, 0, 0, 0, 0, 0, 0, 0, // amount String len = 9
            50, 48, 48, 48, 48, 48, 48, 48, 48, // "200000000"
            2,  // token_balance[1].account_index
            43, 0, 0, 0, 0, 0, 0, 0, // token_balance[1].mint len = 43
            107, 105, 110, 88, 100, 69, 99, 112, 68, 81, 101, 72, 80, 69, 117, 81, 110, 113, 109,
            85, 103, 116, 89, 121, 107, 113, 75, 71, 86, 70, 113, 54, 67, 101, 86, 88, 53, 105, 65,
            72, 74, 113, 54, // token_balance[1].ui_token_amount: StoredTokenAmount
            236, 23, 236, 20, 131, 63, 95, 65, // ui_amount: f64 = 8_191_500.32691
            5,  // decimals: u8
            12, 0, 0, 0, 0, 0, 0, 0, // amount String len = 12
            56, 49, 57, 49, 53, 48, 48, 51, 50, 54, 57, 49, // "819150032691"
        ];

        // Any slot < BINCODE_EPOCH_CUTOFF * 432000 forces bincode path
        let slot = (BINCODE_EPOCH_CUTOFF - 1) * 432_000;

        let mut out = TransactionStatusMeta::default();

        let res = decode_transaction_status_meta_into(slot, metadata, &mut out)
            .inspect_err(|err| println!("{err}"));
        assert!(res.is_ok())
    }
}
