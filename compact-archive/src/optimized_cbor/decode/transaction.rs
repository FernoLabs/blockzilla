use crate::carblock_to_compact::{
    CompactAddressTableLookup, CompactMetadataPayload, CompactVersionedTx,
};
use car_reader::cbor_utils::CborArrayView;
use crate::transaction_parser::{CompiledInstruction, MessageHeader, Signature};
use minicbor::Decoder;
use minicbor::decode::Error as DecodeError;

use super::metadata::CompactMetadataPayloadRef;
use super::utils::{decode_bytes_fixed, decode_message_header, peek_is_null};

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
