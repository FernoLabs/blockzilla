use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use wincode::{SchemaRead, SchemaWrite};

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Signature(#[serde(with = "BigArray")] pub [u8; 64]);

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactTransaction<'a> {
    pub signatures: Vec<Signature>,
    #[serde(borrow)]
    pub message: CompactMessage<'a>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum CompactMessage<'a> {
    Legacy(#[serde(borrow)] CompactLegacyMessage<'a>),
    V0(#[serde(borrow)] CompactV0Message<'a>),
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactMessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactInstruction<'a> {
    pub program_id_index: u8,
    #[serde(borrow)]
    pub accounts: &'a [u8],
    #[serde(borrow)]
    pub data: &'a [u8],
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactLegacyMessage<'a> {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<u32>,                   // registry indices
    pub recent_blockhash: CompactRecentBlockhash, // blockhash registry id
    #[serde(borrow)]
    pub instructions: Vec<CompactInstruction<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactAddressTableLookup<'a> {
    pub account_key: u32, // registry index of the table address
    #[serde(borrow)]
    pub writable_indexes: &'a [u8],
    #[serde(borrow)]
    pub readonly_indexes: &'a [u8],
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum CompactRecentBlockhash {
    /// Normal case: index into epoch blockhash registry.
    Id(i32),
    /// Durable nonce case: store the nonce value inline.
    Nonce([u8; 32]),
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactV0Message<'a> {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<u32>, // registry indices of static keys
    pub recent_blockhash: CompactRecentBlockhash,
    #[serde(borrow)]
    pub instructions: Vec<CompactInstruction<'a>>,
    #[serde(borrow)]
    pub address_table_lookups: Vec<CompactAddressTableLookup<'a>>,
}
