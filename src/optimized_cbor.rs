use std::convert::Infallible;

use crate::carblock_to_compact::{
    CompactAddressTableLookup, CompactBlock, CompactInnerInstruction, CompactInnerInstructions,
    CompactInstructionError, CompactMetadata, CompactMetadataPayload, CompactReturnData,
    CompactReward, CompactRewardType, CompactTokenBalanceMeta, CompactTxError, CompactVersionedTx,
};
use crate::cbor_utils::CborArrayView;
use crate::compact_log::CompactLogStream;
use crate::transaction_parser::{CompiledInstruction, MessageHeader, Signature};
use minicbor::decode::Error as DecodeError;
use minicbor::encode::Error as EncodeError;
use minicbor::encode::Write;
use minicbor::{Decoder, Encoder, data::Type};

pub fn encode_compact_block<W>(
    enc: &mut Encoder<W>,
    block: &CompactBlock,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(3)?;
    enc.u64(block.slot)?;
    enc.array(block.txs.len() as u64)?;
    for tx in &block.txs {
        encode_compact_versioned_tx(enc, tx)?;
    }
    enc.array(block.rewards.len() as u64)?;
    for reward in &block.rewards {
        encode_compact_reward(enc, reward)?;
    }
    Ok(())
}

pub fn encode_compact_block_to_vec(
    block: &CompactBlock,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError<Infallible>> {
    let mut enc = Encoder::new(buf);
    encode_compact_block(&mut enc, block)
}

fn encode_compact_versioned_tx<W>(
    enc: &mut Encoder<W>,
    tx: &CompactVersionedTx,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(8)?;
    enc.array(tx.signatures.len() as u64)?;
    for sig in &tx.signatures {
        enc.bytes(&sig.0)?;
    }

    encode_message_header(enc, &tx.header)?;

    enc.array(tx.account_ids.len() as u64)?;
    for id in &tx.account_ids {
        enc.u32(*id)?;
    }

    enc.bytes(&tx.recent_blockhash)?;

    enc.array(tx.instructions.len() as u64)?;
    for ix in &tx.instructions {
        encode_compiled_instruction(enc, ix)?;
    }

    if let Some(lookups) = &tx.address_table_lookups {
        enc.array(lookups.len() as u64)?;
        for lookup in lookups {
            encode_compact_address_table_lookup(enc, lookup)?;
        }
    } else {
        enc.null()?;
    }

    enc.bool(tx.is_versioned)?;

    if let Some(meta) = &tx.metadata {
        encode_compact_metadata_payload(enc, meta)?;
    } else {
        enc.null()?;
    }

    Ok(())
}

fn encode_message_header<W>(
    enc: &mut Encoder<W>,
    header: &MessageHeader,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(3)?;
    enc.u8(header.num_required_signatures)?;
    enc.u8(header.num_readonly_signed_accounts)?;
    enc.u8(header.num_readonly_unsigned_accounts)?;
    Ok(())
}

fn encode_compiled_instruction<W>(
    enc: &mut Encoder<W>,
    ix: &CompiledInstruction,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(3)?;
    enc.u8(ix.program_id_index)?;
    enc.bytes(&ix.accounts)?;
    enc.bytes(&ix.data)?;
    Ok(())
}

fn encode_compact_address_table_lookup<W>(
    enc: &mut Encoder<W>,
    lookup: &CompactAddressTableLookup,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(3)?;
    enc.u32(lookup.table_account_id)?;
    enc.bytes(&lookup.writable_indexes)?;
    enc.bytes(&lookup.readonly_indexes)?;
    Ok(())
}

fn encode_compact_metadata_payload<W>(
    enc: &mut Encoder<W>,
    payload: &CompactMetadataPayload,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(2)?;
    match payload {
        CompactMetadataPayload::Compact(meta) => {
            enc.u8(0)?;
            encode_compact_metadata(enc, meta)?;
        }
        CompactMetadataPayload::Raw(bytes) => {
            enc.u8(1)?;
            enc.bytes(bytes)?;
        }
    }
    Ok(())
}

fn encode_compact_metadata<W>(
    enc: &mut Encoder<W>,
    meta: &CompactMetadata,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(11)?;
    enc.u64(meta.fee)?;
    if let Some(err) = &meta.err {
        encode_compact_tx_error(enc, err)?;
    } else {
        enc.null()?;
    }

    encode_u64_array(enc, &meta.pre_balances)?;
    encode_u64_array(enc, &meta.post_balances)?;

    enc.array(meta.pre_token_balances.len() as u64)?;
    for b in &meta.pre_token_balances {
        encode_compact_token_balance_meta(enc, b)?;
    }

    enc.array(meta.post_token_balances.len() as u64)?;
    for b in &meta.post_token_balances {
        encode_compact_token_balance_meta(enc, b)?;
    }

    encode_u32_array(enc, &meta.loaded_writable_ids)?;
    encode_u32_array(enc, &meta.loaded_readonly_ids)?;

    if let Some(ret) = &meta.return_data {
        encode_compact_return_data(enc, ret)?;
    } else {
        enc.null()?;
    }

    if let Some(inner) = &meta.inner_instructions {
        enc.array(inner.len() as u64)?;
        for i in inner {
            encode_compact_inner_instructions(enc, i)?;
        }
    } else {
        enc.null()?;
    }

    if let Some(logs) = &meta.log_messages {
        encode_compact_log_stream(enc, logs)?;
    } else {
        enc.null()?;
    }

    Ok(())
}

fn encode_u64_array<W>(enc: &mut Encoder<W>, values: &[u64]) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(values.len() as u64)?;
    for v in values {
        enc.u64(*v)?;
    }
    Ok(())
}

fn encode_u32_array<W>(enc: &mut Encoder<W>, values: &[u32]) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(values.len() as u64)?;
    for v in values {
        enc.u32(*v)?;
    }
    Ok(())
}

fn encode_compact_token_balance_meta<W>(
    enc: &mut Encoder<W>,
    meta: &CompactTokenBalanceMeta,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(5)?;
    enc.u32(meta.mint_id)?;
    enc.u32(meta.owner_id)?;
    enc.u32(meta.program_id_id)?;
    enc.u64(meta.amount)?;
    enc.u8(meta.decimals)?;
    Ok(())
}

fn encode_compact_return_data<W>(
    enc: &mut Encoder<W>,
    data: &CompactReturnData,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(2)?;
    enc.u32(data.program_id_id)?;
    enc.bytes(&data.data)?;
    Ok(())
}

fn encode_compact_inner_instructions<W>(
    enc: &mut Encoder<W>,
    item: &CompactInnerInstructions,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(2)?;
    enc.u32(item.index)?;
    enc.array(item.instructions.len() as u64)?;
    for ix in &item.instructions {
        encode_compact_inner_instruction(enc, ix)?;
    }
    Ok(())
}

fn encode_compact_inner_instruction<W>(
    enc: &mut Encoder<W>,
    item: &CompactInnerInstruction,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(4)?;
    enc.u32(item.program_id_index)?;
    enc.bytes(&item.accounts)?;
    enc.bytes(&item.data)?;
    if let Some(stack) = item.stack_height {
        enc.u32(stack)?;
    } else {
        enc.null()?;
    }
    Ok(())
}

fn encode_compact_log_stream<W>(
    enc: &mut Encoder<W>,
    logs: &CompactLogStream,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(2)?;
    enc.bytes(&logs.bytes)?;
    enc.array(logs.strings.len() as u64)?;
    for s in &logs.strings {
        enc.str(s)?;
    }
    Ok(())
}

fn encode_compact_tx_error<W>(
    enc: &mut Encoder<W>,
    err: &CompactTxError,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    match err {
        CompactTxError::InstructionError { index, error } => {
            enc.array(3)?;
            enc.u8(0)?;
            enc.u32(*index)?;
            encode_compact_instruction_error(enc, error)?;
        }
        CompactTxError::AccountInUse => {
            enc.array(1)?;
            enc.u8(1)?;
        }
        CompactTxError::AccountLoadedTwice => {
            enc.array(1)?;
            enc.u8(2)?;
        }
        CompactTxError::AccountNotFound => {
            enc.array(1)?;
            enc.u8(3)?;
        }
        CompactTxError::ProgramAccountNotFound => {
            enc.array(1)?;
            enc.u8(4)?;
        }
        CompactTxError::InsufficientFundsForFee => {
            enc.array(1)?;
            enc.u8(5)?;
        }
        CompactTxError::InvalidAccountForFee => {
            enc.array(1)?;
            enc.u8(6)?;
        }
        CompactTxError::AlreadyProcessed => {
            enc.array(1)?;
            enc.u8(7)?;
        }
        CompactTxError::BlockhashNotFound => {
            enc.array(1)?;
            enc.u8(8)?;
        }
        CompactTxError::CallChainTooDeep => {
            enc.array(1)?;
            enc.u8(9)?;
        }
        CompactTxError::MissingSignatureForFee => {
            enc.array(1)?;
            enc.u8(10)?;
        }
        CompactTxError::InvalidAccountIndex => {
            enc.array(1)?;
            enc.u8(11)?;
        }
        CompactTxError::SignatureFailure => {
            enc.array(1)?;
            enc.u8(12)?;
        }
        CompactTxError::SanitizedTransactionError => {
            enc.array(1)?;
            enc.u8(13)?;
        }
        CompactTxError::ProgramError => {
            enc.array(1)?;
            enc.u8(14)?;
        }
        CompactTxError::Other { name } => {
            enc.array(2)?;
            enc.u8(15)?;
            enc.str(name)?;
        }
    }
    Ok(())
}

fn encode_compact_instruction_error<W>(
    enc: &mut Encoder<W>,
    err: &CompactInstructionError,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(2)?;
    match err {
        CompactInstructionError::Builtin(code) => {
            enc.u8(0)?;
            enc.u32(*code)?;
        }
        CompactInstructionError::Custom(code) => {
            enc.u8(1)?;
            enc.u32(*code)?;
        }
    }
    Ok(())
}

fn encode_compact_reward<W>(
    enc: &mut Encoder<W>,
    reward: &CompactReward,
) -> Result<(), EncodeError<W::Error>>
where
    W: Write,
{
    enc.array(5)?;
    enc.u32(reward.account_id)?;
    enc.i64(reward.lamports)?;
    enc.u64(reward.post_balance)?;
    enc.u8(reward.reward_type as u8)?;
    if let Some(c) = reward.commission {
        enc.u8(c)?;
    } else {
        enc.null()?;
    }
    Ok(())
}

// ---------------------
// Zero-copy decoding
// ---------------------

#[derive(Debug, minicbor::Decode)]
#[cbor(array)]
pub struct CompactBlockRef<'a> {
    #[n(0)]
    pub slot: u64,
    #[n(1)]
    #[cbor(borrow = "'a + 'bytes")]
    pub txs: CborArrayView<'a, CompactVersionedTxRef<'a>>,
    #[n(2)]
    #[cbor(borrow = "'a + 'bytes")]
    pub rewards: CborArrayView<'a, CompactRewardRef>,
}

#[derive(Debug, Clone)]
pub struct CompactVersionedTxRef<'a> {
    slice: &'a [u8],
}

impl<'b, C> minicbor::Decode<'b, C> for CompactVersionedTxRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let start = d.position();
        d.skip()?;
        let end = d.position();
        let input = d.input();
        Ok(Self {
            slice: &input[start..end],
        })
    }
}

impl<'a> CompactVersionedTxRef<'a> {
    pub fn view(&self) -> Result<CompactVersionedTxView<'a>, DecodeError> {
        let mut d = Decoder::new(self.slice);
        let len = d.array()?.unwrap_or(0);
        if len != 8 {
            return Err(DecodeError::message(
                "CompactVersionedTx expects array of length 8",
            ));
        }

        let signatures: CborArrayView<'a, SignatureRef<'a>> = d.decode_with(&mut ())?;
        let header = decode_message_header(&mut d)?;
        let account_ids: CborArrayView<'a, u32> = d.decode_with(&mut ())?;
        let recent_blockhash = decode_bytes_fixed(&mut d, 32, "recent_blockhash")?;
        let instructions: CborArrayView<'a, CompiledInstructionRef<'a>> = d.decode_with(&mut ())?;

        let address_table_lookups = match peek_is_null(&mut d)? {
            Some(()) => None,
            None => Some(d.decode_with(&mut ())?),
        };

        let is_versioned = d.bool()?;

        let metadata = match peek_is_null(&mut d)? {
            Some(()) => None,
            None => Some(CompactMetadataPayloadRef::decode(&mut d)?),
        };

        Ok(CompactVersionedTxView {
            signatures,
            header,
            account_ids,
            recent_blockhash,
            instructions,
            address_table_lookups,
            is_versioned,
            metadata,
        })
    }

    pub fn to_owned(&self) -> Result<CompactVersionedTx, DecodeError> {
        let view = self.view()?;
        let mut signatures = Vec::with_capacity(view.signatures.len());
        for sig in view.signatures.iter() {
            let sig = sig?;
            if sig.bytes.len() != 64 {
                return Err(DecodeError::message("signature must be 64 bytes"));
            }
            let mut arr = [0u8; 64];
            arr.copy_from_slice(sig.bytes);
            signatures.push(Signature(arr));
        }

        let mut account_ids = Vec::with_capacity(view.account_ids.len());
        for id in view.account_ids.iter() {
            account_ids.push(id?);
        }

        let mut instructions = Vec::with_capacity(view.instructions.len());
        for ix in view.instructions.iter() {
            let ix = ix?;
            instructions.push(ix.to_owned());
        }

        let lookups = match &view.address_table_lookups {
            Some(arr) => {
                let mut out = Vec::with_capacity(arr.len());
                for l in arr.iter() {
                    out.push(l?.to_owned());
                }
                Some(out)
            }
            None => None,
        };

        let metadata = match &view.metadata {
            Some(CompactMetadataPayloadRef::Compact(meta)) => {
                Some(CompactMetadataPayload::Compact(meta.to_owned()?))
            }
            Some(CompactMetadataPayloadRef::Raw(raw)) => {
                Some(CompactMetadataPayload::Raw(raw.to_vec()))
            }
            None => None,
        };

        let mut recent_blockhash = [0u8; 32];
        if view.recent_blockhash.len() != 32 {
            return Err(DecodeError::message("recent_blockhash must be 32 bytes"));
        }
        recent_blockhash.copy_from_slice(view.recent_blockhash);

        Ok(CompactVersionedTx {
            signatures,
            header: view.header,
            account_ids,
            recent_blockhash,
            instructions,
            address_table_lookups: lookups,
            is_versioned: view.is_versioned,
            metadata,
        })
    }
}

#[derive(Debug)]
pub struct CompactVersionedTxView<'a> {
    pub signatures: CborArrayView<'a, SignatureRef<'a>>,
    pub header: MessageHeader,
    pub account_ids: CborArrayView<'a, u32>,
    pub recent_blockhash: &'a [u8],
    pub instructions: CborArrayView<'a, CompiledInstructionRef<'a>>,
    pub address_table_lookups: Option<CborArrayView<'a, CompactAddressTableLookupRef<'a>>>,
    pub is_versioned: bool,
    pub metadata: Option<CompactMetadataPayloadRef<'a>>,
}

#[derive(Debug, Clone, Copy)]
pub struct SignatureRef<'a> {
    pub bytes: &'a [u8],
}

impl<'b, C> minicbor::Decode<'b, C> for SignatureRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let bytes = d.bytes()?;
        Ok(SignatureRef { bytes })
    }
}

#[derive(Debug, Clone)]
pub struct CompiledInstructionRef<'a> {
    pub program_id_index: u8,
    pub accounts: &'a [u8],
    pub data: &'a [u8],
}

impl<'b, C> minicbor::Decode<'b, C> for CompiledInstructionRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 3 {
            return Err(DecodeError::message("CompiledInstruction expects array[3]"));
        }
        let program_id_index = d.u32()? as u8;
        let accounts = d.bytes()?;
        let data = d.bytes()?;
        Ok(Self {
            program_id_index,
            accounts,
            data,
        })
    }
}

impl<'a> CompiledInstructionRef<'a> {
    pub fn to_owned(&self) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: self.program_id_index,
            accounts: self.accounts.to_vec(),
            data: self.data.to_vec(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactAddressTableLookupRef<'a> {
    pub table_account_id: u32,
    pub writable_indexes: &'a [u8],
    pub readonly_indexes: &'a [u8],
}

impl<'b, C> minicbor::Decode<'b, C> for CompactAddressTableLookupRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 3 {
            return Err(DecodeError::message(
                "CompactAddressTableLookup expects array[3]",
            ));
        }
        let table_account_id = d.u32()?;
        let writable_indexes = d.bytes()?;
        let readonly_indexes = d.bytes()?;
        Ok(Self {
            table_account_id,
            writable_indexes,
            readonly_indexes,
        })
    }
}

impl<'a> CompactAddressTableLookupRef<'a> {
    pub fn to_owned(&self) -> CompactAddressTableLookup {
        CompactAddressTableLookup {
            table_account_id: self.table_account_id,
            writable_indexes: self.writable_indexes.to_vec(),
            readonly_indexes: self.readonly_indexes.to_vec(),
        }
    }
}

#[derive(Debug, minicbor::Decode)]
#[cbor(array)]
pub struct CompactRewardRef {
    #[n(0)]
    pub account_id: u32,
    #[n(1)]
    pub lamports: i64,
    #[n(2)]
    pub post_balance: u64,
    #[n(3)]
    pub reward_type: CompactRewardType,
    #[n(4)]
    pub commission: Option<u8>,
}

impl CompactRewardRef {
    pub fn to_owned(&self) -> CompactReward {
        CompactReward {
            account_id: self.account_id,
            lamports: self.lamports,
            post_balance: self.post_balance,
            reward_type: self.reward_type,
            commission: self.commission,
        }
    }
}

#[derive(Debug)]
pub enum CompactMetadataPayloadRef<'a> {
    Compact(CompactMetadataRef<'a>),
    Raw(&'a [u8]),
}

impl<'a> CompactMetadataPayloadRef<'a> {
    fn decode(d: &mut Decoder<'a>) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 2 {
            return Err(DecodeError::message(
                "CompactMetadataPayload expects array[2]",
            ));
        }
        let variant = d.u32()?;
        match variant {
            0 => {
                let meta = CompactMetadataRef::decode(d)?;
                Ok(CompactMetadataPayloadRef::Compact(meta))
            }
            1 => {
                let raw = d.bytes()?;
                Ok(CompactMetadataPayloadRef::Raw(raw))
            }
            _ => Err(DecodeError::message(
                "unknown CompactMetadataPayload variant",
            )),
        }
    }
}

#[derive(Debug)]
pub struct CompactMetadataRef<'a> {
    pub fee: u64,
    pub err: Option<CompactTxErrorRef<'a>>,
    pub pre_balances: CborArrayView<'a, u64>,
    pub post_balances: CborArrayView<'a, u64>,
    pub pre_token_balances: CborArrayView<'a, CompactTokenBalanceMetaRef>,
    pub post_token_balances: CborArrayView<'a, CompactTokenBalanceMetaRef>,
    pub loaded_writable_ids: CborArrayView<'a, u32>,
    pub loaded_readonly_ids: CborArrayView<'a, u32>,
    pub return_data: Option<CompactReturnDataRef<'a>>,
    pub inner_instructions: Option<CborArrayView<'a, CompactInnerInstructionsRef<'a>>>,
    pub log_messages: Option<CompactLogStreamRef<'a>>,
}

impl<'a> CompactMetadataRef<'a> {
    fn decode(d: &mut Decoder<'a>) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 11 {
            return Err(DecodeError::message("CompactMetadata expects array[11]"));
        }
        let fee = d.u64()?;
        let err = match peek_is_null(d)? {
            Some(()) => None,
            None => Some(CompactTxErrorRef::decode(d)?),
        };
        let pre_balances = d.decode_with(&mut ())?;
        let post_balances = d.decode_with(&mut ())?;
        let pre_token_balances = d.decode_with(&mut ())?;
        let post_token_balances = d.decode_with(&mut ())?;
        let loaded_writable_ids = d.decode_with(&mut ())?;
        let loaded_readonly_ids = d.decode_with(&mut ())?;
        let return_data = match peek_is_null(d)? {
            Some(()) => None,
            None => Some(d.decode_with(&mut ())?),
        };
        let inner_instructions = match peek_is_null(d)? {
            Some(()) => None,
            None => Some(d.decode_with(&mut ())?),
        };
        let log_messages = match peek_is_null(d)? {
            Some(()) => None,
            None => Some(d.decode_with(&mut ())?),
        };

        Ok(Self {
            fee,
            err,
            pre_balances,
            post_balances,
            pre_token_balances,
            post_token_balances,
            loaded_writable_ids,
            loaded_readonly_ids,
            return_data,
            inner_instructions,
            log_messages,
        })
    }

    pub fn to_owned(&self) -> Result<CompactMetadata, DecodeError> {
        let err = match &self.err {
            Some(e) => Some(e.to_owned()?),
            None => None,
        };

        let mut pre_balances = Vec::with_capacity(self.pre_balances.len());
        for v in self.pre_balances.iter() {
            pre_balances.push(v?);
        }
        let mut post_balances = Vec::with_capacity(self.post_balances.len());
        for v in self.post_balances.iter() {
            post_balances.push(v?);
        }

        let mut pre_token_balances = Vec::with_capacity(self.pre_token_balances.len());
        for v in self.pre_token_balances.iter() {
            pre_token_balances.push(v?.to_owned());
        }
        let mut post_token_balances = Vec::with_capacity(self.post_token_balances.len());
        for v in self.post_token_balances.iter() {
            post_token_balances.push(v?.to_owned());
        }

        let mut loaded_writable_ids = Vec::with_capacity(self.loaded_writable_ids.len());
        for v in self.loaded_writable_ids.iter() {
            loaded_writable_ids.push(v?);
        }
        let mut loaded_readonly_ids = Vec::with_capacity(self.loaded_readonly_ids.len());
        for v in self.loaded_readonly_ids.iter() {
            loaded_readonly_ids.push(v?);
        }

        let return_data = match &self.return_data {
            Some(r) => Some(r.to_owned()),
            None => None,
        };

        let inner_instructions = match &self.inner_instructions {
            Some(v) => {
                let mut out = Vec::with_capacity(v.len());
                for item in v.iter() {
                    out.push(item?.to_owned()?);
                }
                Some(out)
            }
            None => None,
        };

        let log_messages = match &self.log_messages {
            Some(logs) => Some(logs.to_owned()?),
            None => None,
        };

        Ok(CompactMetadata {
            fee: self.fee,
            err,
            pre_balances,
            post_balances,
            pre_token_balances,
            post_token_balances,
            loaded_writable_ids,
            loaded_readonly_ids,
            return_data,
            inner_instructions,
            log_messages,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CompactTokenBalanceMetaRef {
    pub mint_id: u32,
    pub owner_id: u32,
    pub program_id_id: u32,
    pub amount: u64,
    pub decimals: u8,
}

impl<'b, C> minicbor::Decode<'b, C> for CompactTokenBalanceMetaRef {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 5 {
            return Err(DecodeError::message(
                "CompactTokenBalanceMeta expects array[5]",
            ));
        }
        let mint_id = d.u32()?;
        let owner_id = d.u32()?;
        let program_id_id = d.u32()?;
        let amount = d.u64()?;
        let decimals = d.u32()? as u8;
        Ok(Self {
            mint_id,
            owner_id,
            program_id_id,
            amount,
            decimals,
        })
    }
}

impl CompactTokenBalanceMetaRef {
    pub fn to_owned(&self) -> CompactTokenBalanceMeta {
        CompactTokenBalanceMeta {
            mint_id: self.mint_id,
            owner_id: self.owner_id,
            program_id_id: self.program_id_id,
            amount: self.amount,
            decimals: self.decimals,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactReturnDataRef<'a> {
    pub program_id_id: u32,
    pub data: &'a [u8],
}

impl<'b, C> minicbor::Decode<'b, C> for CompactReturnDataRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 2 {
            return Err(DecodeError::message("CompactReturnData expects array[2]"));
        }
        let program_id_id = d.u32()?;
        let data = d.bytes()?;
        Ok(Self {
            program_id_id,
            data,
        })
    }
}

impl<'a> CompactReturnDataRef<'a> {
    pub fn to_owned(&self) -> CompactReturnData {
        CompactReturnData {
            program_id_id: self.program_id_id,
            data: self.data.to_vec(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactInnerInstructionsRef<'a> {
    pub index: u32,
    pub instructions: CborArrayView<'a, CompactInnerInstructionRef<'a>>,
}

impl<'b, C> minicbor::Decode<'b, C> for CompactInnerInstructionsRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 2 {
            return Err(DecodeError::message(
                "CompactInnerInstructions expects array[2]",
            ));
        }
        let index = d.u32()?;
        let instructions = d.decode_with(&mut ())?;
        Ok(Self {
            index,
            instructions,
        })
    }
}

impl<'a> CompactInnerInstructionsRef<'a> {
    pub fn to_owned(&self) -> Result<CompactInnerInstructions, DecodeError> {
        let mut instructions = Vec::with_capacity(self.instructions.len());
        for ix in self.instructions.iter() {
            instructions.push(ix?.to_owned());
        }
        Ok(CompactInnerInstructions {
            index: self.index,
            instructions,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CompactInnerInstructionRef<'a> {
    pub program_id_index: u32,
    pub accounts: &'a [u8],
    pub data: &'a [u8],
    pub stack_height: Option<u32>,
}

impl<'b, C> minicbor::Decode<'b, C> for CompactInnerInstructionRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 4 {
            return Err(DecodeError::message(
                "CompactInnerInstruction expects array[4]",
            ));
        }
        let program_id_index = d.u32()?;
        let accounts = d.bytes()?;
        let data = d.bytes()?;
        let stack_height = match peek_is_null(d)? {
            Some(()) => None,
            None => Some(d.u32()?),
        };
        Ok(Self {
            program_id_index,
            accounts,
            data,
            stack_height,
        })
    }
}

impl<'a> CompactInnerInstructionRef<'a> {
    pub fn to_owned(&self) -> CompactInnerInstruction {
        CompactInnerInstruction {
            program_id_index: self.program_id_index,
            accounts: self.accounts.to_vec(),
            data: self.data.to_vec(),
            stack_height: self.stack_height,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactLogStreamRef<'a> {
    pub bytes: &'a [u8],
    pub strings: CborArrayView<'a, StrRef<'a>>,
}

impl<'b, C> minicbor::Decode<'b, C> for CompactLogStreamRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 2 {
            return Err(DecodeError::message("CompactLogStream expects array[2]"));
        }
        let bytes = d.bytes()?;
        let strings = d.decode_with(&mut ())?;
        Ok(Self { bytes, strings })
    }
}

impl<'a> CompactLogStreamRef<'a> {
    pub fn to_owned(&self) -> Result<CompactLogStream, DecodeError> {
        let mut strings = Vec::with_capacity(self.strings.len());
        for s in self.strings.iter() {
            strings.push(s?.to_string());
        }
        Ok(CompactLogStream {
            bytes: self.bytes.to_vec(),
            strings,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StrRef<'a> {
    pub value: &'a str,
}

impl<'b, C> minicbor::Decode<'b, C> for StrRef<'b> {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let value = d.str()?;
        Ok(StrRef { value })
    }
}

impl<'a> StrRef<'a> {
    fn to_string(&self) -> String {
        self.value.to_owned()
    }
}

#[derive(Debug, Clone)]
pub enum CompactTxErrorRef<'a> {
    InstructionError {
        index: u32,
        error: CompactInstructionErrorRef,
    },
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    SanitizedTransactionError,
    ProgramError,
    Other {
        name: &'a str,
    },
}

impl<'a> CompactTxErrorRef<'a> {
    fn decode(d: &mut Decoder<'a>) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len == 0 {
            return Err(DecodeError::message(
                "CompactTxError expects non-empty array",
            ));
        }
        let variant = d.u32()?;
        let result = match variant {
            0 => {
                let index = d.u32()?;
                let error = CompactInstructionErrorRef::decode(d)?;
                CompactTxErrorRef::InstructionError { index, error }
            }
            1 => CompactTxErrorRef::AccountInUse,
            2 => CompactTxErrorRef::AccountLoadedTwice,
            3 => CompactTxErrorRef::AccountNotFound,
            4 => CompactTxErrorRef::ProgramAccountNotFound,
            5 => CompactTxErrorRef::InsufficientFundsForFee,
            6 => CompactTxErrorRef::InvalidAccountForFee,
            7 => CompactTxErrorRef::AlreadyProcessed,
            8 => CompactTxErrorRef::BlockhashNotFound,
            9 => CompactTxErrorRef::CallChainTooDeep,
            10 => CompactTxErrorRef::MissingSignatureForFee,
            11 => CompactTxErrorRef::InvalidAccountIndex,
            12 => CompactTxErrorRef::SignatureFailure,
            13 => CompactTxErrorRef::SanitizedTransactionError,
            14 => CompactTxErrorRef::ProgramError,
            15 => {
                let name = d.str()?;
                CompactTxErrorRef::Other { name }
            }
            _ => return Err(DecodeError::message("unknown CompactTxError variant")),
        };
        Ok(result)
    }

    pub fn to_owned(&self) -> Result<CompactTxError, DecodeError> {
        Ok(match self {
            CompactTxErrorRef::InstructionError { index, error } => {
                CompactTxError::InstructionError {
                    index: *index,
                    error: error.to_owned(),
                }
            }
            CompactTxErrorRef::AccountInUse => CompactTxError::AccountInUse,
            CompactTxErrorRef::AccountLoadedTwice => CompactTxError::AccountLoadedTwice,
            CompactTxErrorRef::AccountNotFound => CompactTxError::AccountNotFound,
            CompactTxErrorRef::ProgramAccountNotFound => CompactTxError::ProgramAccountNotFound,
            CompactTxErrorRef::InsufficientFundsForFee => CompactTxError::InsufficientFundsForFee,
            CompactTxErrorRef::InvalidAccountForFee => CompactTxError::InvalidAccountForFee,
            CompactTxErrorRef::AlreadyProcessed => CompactTxError::AlreadyProcessed,
            CompactTxErrorRef::BlockhashNotFound => CompactTxError::BlockhashNotFound,
            CompactTxErrorRef::CallChainTooDeep => CompactTxError::CallChainTooDeep,
            CompactTxErrorRef::MissingSignatureForFee => CompactTxError::MissingSignatureForFee,
            CompactTxErrorRef::InvalidAccountIndex => CompactTxError::InvalidAccountIndex,
            CompactTxErrorRef::SignatureFailure => CompactTxError::SignatureFailure,
            CompactTxErrorRef::SanitizedTransactionError => {
                CompactTxError::SanitizedTransactionError
            }
            CompactTxErrorRef::ProgramError => CompactTxError::ProgramError,
            CompactTxErrorRef::Other { name } => CompactTxError::Other {
                name: (*name).to_owned(),
            },
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompactInstructionErrorRef {
    Builtin(u32),
    Custom(u32),
}

impl CompactInstructionErrorRef {
    fn decode(d: &mut Decoder<'_>) -> Result<Self, DecodeError> {
        let len = d.array()?.unwrap_or(0);
        if len != 2 {
            return Err(DecodeError::message(
                "CompactInstructionError expects array[2]",
            ));
        }
        let variant = d.u32()?;
        let code = d.u32()?;
        match variant {
            0 => Ok(CompactInstructionErrorRef::Builtin(code)),
            1 => Ok(CompactInstructionErrorRef::Custom(code)),
            _ => Err(DecodeError::message(
                "unknown CompactInstructionError variant",
            )),
        }
    }

    pub fn to_owned(&self) -> CompactInstructionError {
        match self {
            CompactInstructionErrorRef::Builtin(code) => CompactInstructionError::Builtin(*code),
            CompactInstructionErrorRef::Custom(code) => CompactInstructionError::Custom(*code),
        }
    }
}

impl<'a> CompactBlockRef<'a> {
    pub fn to_owned(&self) -> Result<CompactBlock, DecodeError> {
        let mut txs = Vec::with_capacity(self.txs.len());
        for tx in self.txs.iter() {
            txs.push(tx?.to_owned()?);
        }

        let mut rewards = Vec::with_capacity(self.rewards.len());
        for reward in self.rewards.iter() {
            rewards.push(reward?.to_owned());
        }

        Ok(CompactBlock {
            slot: self.slot,
            txs,
            rewards,
        })
    }
}

impl<'a> CompactBlockRef<'a> {
    pub fn tx_iter(
        &self,
    ) -> impl Iterator<Item = Result<CompactVersionedTxRef<'a>, DecodeError>> + 'a {
        self.txs.iter()
    }

    pub fn reward_iter(&self) -> impl Iterator<Item = Result<CompactRewardRef, DecodeError>> + 'a {
        self.rewards.iter()
    }
}

fn decode_message_header(d: &mut Decoder<'_>) -> Result<MessageHeader, DecodeError> {
    let len = d.array()?.unwrap_or(0);
    if len != 3 {
        return Err(DecodeError::message("MessageHeader expects array[3]"));
    }
    let num_required_signatures = d.u32()? as u8;
    let num_readonly_signed_accounts = d.u32()? as u8;
    let num_readonly_unsigned_accounts = d.u32()? as u8;
    Ok(MessageHeader {
        num_required_signatures,
        num_readonly_signed_accounts,
        num_readonly_unsigned_accounts,
    })
}

fn decode_bytes_fixed<'a>(
    d: &mut Decoder<'a>,
    expected: usize,
    label: &str,
) -> Result<&'a [u8], DecodeError> {
    let bytes = d.bytes()?;
    if bytes.len() != expected {
        return Err(DecodeError::message(&format!(
            "{label} expected {expected} bytes"
        )));
    }
    Ok(bytes)
}

fn peek_is_null(d: &mut Decoder<'_>) -> Result<Option<()>, DecodeError> {
    if d.datatype()? == Type::Null {
        d.null()?;
        Ok(Some(()))
    } else {
        Ok(None)
    }
}

impl<'b, C> minicbor::Decode<'b, C> for CompactRewardType {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut C) -> Result<Self, DecodeError> {
        let value = d.u32()? as u8;
        let reward_type = match value {
            0 => CompactRewardType::Fee,
            1 => CompactRewardType::Rent,
            2 => CompactRewardType::Staking,
            3 => CompactRewardType::Voting,
            4 => CompactRewardType::Revoked,
            255 => CompactRewardType::Unspecified,
            _ => return Err(DecodeError::message("invalid CompactRewardType")),
        };
        Ok(reward_type)
    }
}

pub fn decode_compact_block(bytes: &[u8]) -> Result<CompactBlockRef<'_>, DecodeError> {
    let mut d = Decoder::new(bytes);
    let block: CompactBlockRef = d.decode()?;
    Ok(block)
}

pub fn decode_owned_compact_block(bytes: &[u8]) -> Result<CompactBlock, DecodeError> {
    let view = decode_compact_block(bytes)?;
    view.to_owned()
}
