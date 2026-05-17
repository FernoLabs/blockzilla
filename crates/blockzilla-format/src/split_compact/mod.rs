use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    CompactAddressTableLookup, CompactBlockHeader, CompactInstruction, CompactMessage,
    CompactMessageHeader, CompactMetaV1, CompactPubkey, CompactRecentBlockhash, CompactTransaction,
};

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SplitCompactIndexHeader {
    pub version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SplitCompactIndexRecord {
    pub slot: u64,
    pub block_id: u32,
    pub block_offset: u64,
    pub block_len: u32,
    pub runtime_offset: u64,
    pub runtime_len: u32,
    pub tx_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct OwnedCompactInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct OwnedCompactAddressTableLookup {
    pub account_key: CompactPubkey,
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum OwnedCompactRecentBlockhash {
    Id(i32),
    Nonce([u8; 32]),
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct OwnedCompactLegacyMessage {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<CompactPubkey>,
    pub recent_blockhash: OwnedCompactRecentBlockhash,
    pub instructions: Vec<OwnedCompactInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct OwnedCompactV0Message {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<CompactPubkey>,
    pub recent_blockhash: OwnedCompactRecentBlockhash,
    pub instructions: Vec<OwnedCompactInstruction>,
    pub address_table_lookups: Vec<OwnedCompactAddressTableLookup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum OwnedCompactMessage {
    Legacy(OwnedCompactLegacyMessage),
    V0(OwnedCompactV0Message),
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct OwnedCompactTransaction {
    pub signatures: Vec<Vec<u8>>,
    pub message: OwnedCompactMessage,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SplitCompactBlockRecord {
    pub header: CompactBlockHeader,
    pub txs: Vec<OwnedCompactTransaction>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SplitCompactRuntimeRecord {
    pub slot: u64,
    pub tx_metadata: Vec<Option<CompactMetaV1>>,
}

impl OwnedCompactTransaction {
    pub fn from_borrowed(value: &CompactTransaction<'_>) -> Self {
        let signatures = value.signatures.iter().map(|sig| sig.0.to_vec()).collect();
        let message = match &value.message {
            CompactMessage::Legacy(message) => {
                OwnedCompactMessage::Legacy(OwnedCompactLegacyMessage {
                    header: message.header,
                    account_keys: message.account_keys.clone(),
                    recent_blockhash: owned_recent_blockhash(&message.recent_blockhash),
                    instructions: message.instructions.iter().map(owned_instruction).collect(),
                })
            }
            CompactMessage::V0(message) => OwnedCompactMessage::V0(OwnedCompactV0Message {
                header: message.header,
                account_keys: message.account_keys.clone(),
                recent_blockhash: owned_recent_blockhash(&message.recent_blockhash),
                instructions: message.instructions.iter().map(owned_instruction).collect(),
                address_table_lookups: message
                    .address_table_lookups
                    .iter()
                    .map(owned_lookup)
                    .collect(),
            }),
        };

        Self {
            signatures,
            message,
        }
    }
}

fn owned_instruction(value: &CompactInstruction<'_>) -> OwnedCompactInstruction {
    OwnedCompactInstruction {
        program_id_index: value.program_id_index,
        accounts: value.accounts.to_vec(),
        data: value.data.to_vec(),
    }
}

fn owned_lookup(value: &CompactAddressTableLookup<'_>) -> OwnedCompactAddressTableLookup {
    OwnedCompactAddressTableLookup {
        account_key: value.account_key,
        writable_indexes: value.writable_indexes.to_vec(),
        readonly_indexes: value.readonly_indexes.to_vec(),
    }
}

fn owned_recent_blockhash(value: &CompactRecentBlockhash<'_>) -> OwnedCompactRecentBlockhash {
    match value {
        CompactRecentBlockhash::Id(id) => OwnedCompactRecentBlockhash::Id(*id),
        CompactRecentBlockhash::Nonce(nonce) => OwnedCompactRecentBlockhash::Nonce(*nonce.0),
    }
}
