use std::convert::Infallible;

use crate::carblock_to_compact::{
    CompactAddressTableLookup, CompactBlock, CompactInnerInstruction, CompactInnerInstructions,
    CompactInstructionError, CompactMetadata, CompactMetadataPayload, CompactReturnData,
    CompactReward, CompactTokenBalanceMeta, CompactTxError, CompactVersionedTx,
};
use crate::compact_log::CompactLogStream;
use crate::transaction_parser::{CompiledInstruction, MessageHeader};
use minicbor::Encoder;
use minicbor::encode::Error as EncodeError;
use minicbor::encode::Write;

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
