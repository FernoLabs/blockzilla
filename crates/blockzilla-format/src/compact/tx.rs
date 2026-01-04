use serde::{Deserialize, Serialize};

use crate::{Nonce, Signature};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTransaction<'a> {
    #[serde(borrow)]
    pub signatures: heapless::Vec<Signature<'a>, 32>,
    #[serde(borrow)]
    pub message: CompactMessage<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactMessage<'a> {
    Legacy(#[serde(borrow)] CompactLegacyMessage<'a>),
    V0(#[serde(borrow)] CompactV0Message<'a>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactMessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactInstruction<'a> {
    pub program_id_index: u8,
    #[serde(borrow)]
    pub accounts: &'a [u8],
    #[serde(borrow)]
    pub data: &'a [u8],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactLegacyMessage<'a> {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<u32>,
    pub recent_blockhash: CompactRecentBlockhash<'a>,
    #[serde(borrow)]
    pub instructions: Vec<CompactInstruction<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactAddressTableLookup<'a> {
    pub account_key: u32, // registry index of the table address
    #[serde(borrow)]
    pub writable_indexes: &'a [u8],
    #[serde(borrow)]
    pub readonly_indexes: &'a [u8],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactRecentBlockhash<'a> {
    /// Normal case: index into epoch blockhash registry.
    Id(i32),
    /// Durable nonce case: store the nonce value inline.
    #[serde(borrow)]
    Nonce(Nonce<'a>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactV0Message<'a> {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<u32>,
    pub recent_blockhash: CompactRecentBlockhash<'a>,
    #[serde(borrow)]
    pub instructions: Vec<CompactInstruction<'a>>,
    #[serde(borrow)]
    pub address_table_lookups: Vec<CompactAddressTableLookup<'a>>,
}
