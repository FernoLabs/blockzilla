use crate::carblock_to_compact::{
    CompactInnerInstruction, CompactInnerInstructions, CompactMetadata, CompactReturnData,
    CompactTokenBalanceMeta,
};
use car_reader::cbor_utils::CborArrayView;
use minicbor::Decoder;
use minicbor::decode::Error as DecodeError;

use super::errors::CompactTxErrorRef;
use super::logs::CompactLogStreamRef;
use super::utils::peek_is_null;

#[derive(Debug)]
pub enum CompactMetadataPayloadRef<'a> {
    Compact(CompactMetadataRef<'a>),
    Raw(&'a [u8]),
}

impl<'a> CompactMetadataPayloadRef<'a> {
    pub(super) fn decode(d: &mut Decoder<'a>) -> Result<Self, DecodeError> {
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
    pub(super) fn decode(d: &mut Decoder<'a>) -> Result<Self, DecodeError> {
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

        Ok(CompactMetadataRef {
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
            Some(err) => Some(err.to_owned()),
            None => None,
        };

        let mut pre_balances = Vec::with_capacity(self.pre_balances.len());
        for value in self.pre_balances.iter() {
            pre_balances.push(value?);
        }
        let mut post_balances = Vec::with_capacity(self.post_balances.len());
        for value in self.post_balances.iter() {
            post_balances.push(value?);
        }

        let mut pre_token_balances = Vec::with_capacity(self.pre_token_balances.len());
        for b in self.pre_token_balances.iter() {
            pre_token_balances.push(b?.to_owned());
        }

        let mut post_token_balances = Vec::with_capacity(self.post_token_balances.len());
        for b in self.post_token_balances.iter() {
            post_token_balances.push(b?.to_owned());
        }

        let mut loaded_writable_ids = Vec::with_capacity(self.loaded_writable_ids.len());
        for value in self.loaded_writable_ids.iter() {
            loaded_writable_ids.push(value?);
        }
        let mut loaded_readonly_ids = Vec::with_capacity(self.loaded_readonly_ids.len());
        for value in self.loaded_readonly_ids.iter() {
            loaded_readonly_ids.push(value?);
        }

        let return_data = match &self.return_data {
            Some(ret) => Some(ret.to_owned()),
            None => None,
        };

        let inner_instructions = match &self.inner_instructions {
            Some(arr) => {
                let mut out = Vec::with_capacity(arr.len());
                for item in arr.iter() {
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
