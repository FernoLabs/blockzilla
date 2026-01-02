use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactTransaction {
    pub signatures: Vec<solana_transaction::Signature>,
    pub message: CompactMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactMessage {
    Legacy(CompactLegacyMessage),
    V0(CompactV0Message),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactMessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactLegacyMessage {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<u32>,                   // registry indices
    pub recent_blockhash: CompactRecentBlockhash, // blockhash registry id
    pub instructions: Vec<CompactInstruction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactAddressTableLookup {
    pub account_key: u32, // registry index of the table address
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactRecentBlockhash {
    /// Normal case: index into epoch blockhash registry.
    Id(i32),
    /// Durable nonce case: store the nonce value inline.
    Nonce([u8; 32]),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactV0Message {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<u32>, // registry indices of static keys
    pub recent_blockhash: CompactRecentBlockhash,
    pub instructions: Vec<CompactInstruction>,
    pub address_table_lookups: Vec<CompactAddressTableLookup>,
}

