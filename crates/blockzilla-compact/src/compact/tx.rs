use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{Nonce, Signature};
use blockzilla_primitives::{CompactPubkey};

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
    // Appended, so Legacy and V0 keep tags 0 and 1 and existing generations
    // decode unchanged.
    V1(#[serde(borrow)] CompactV1Message<'a>),
}

/// The compute budget a v1 message carries in its header (SIMD-0385).
///
/// Legacy and v0 expressed these as ComputeBudget instructions; v1 moves them
/// out of the instruction list, so the archive has to carry them as their own
/// field or lose them. Which fields are set determines the wire config mask,
/// and the values are written in bit order, so the original header bytes stay
/// reconstructible for signature verification.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, SchemaRead, SchemaWrite,
)]
pub struct CompactTransactionConfig {
    pub priority_fee: Option<u64>,
    pub compute_unit_limit: Option<u32>,
    pub loaded_accounts_data_size_limit: Option<u32>,
    pub heap_size: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[wincode(assert_zero_copy)]
#[repr(C)]
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
    pub account_keys: Vec<CompactPubkey>,
    pub recent_blockhash: CompactRecentBlockhash<'a>,
    #[serde(borrow)]
    pub instructions: Vec<CompactInstruction<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactAddressTableLookup<'a> {
    pub account_key: CompactPubkey,
    #[serde(borrow)]
    pub writable_indexes: &'a [u8],
    #[serde(borrow)]
    pub readonly_indexes: &'a [u8],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompactRecentBlockhash<'a> {
    /// Normal case: index into epoch blockhash registry.
    Id(i32),
    /// Raw inline fallback. The historical wire name is `Nonce`, but this also
    /// stores a valid recent hash that is outside this epoch's ID dictionary.
    #[serde(borrow)]
    Nonce(Nonce<'a>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactV0Message<'a> {
    pub header: CompactMessageHeader,
    pub account_keys: Vec<CompactPubkey>,
    pub recent_blockhash: CompactRecentBlockhash<'a>,
    #[serde(borrow)]
    pub instructions: Vec<CompactInstruction<'a>>,
    #[serde(borrow)]
    pub address_table_lookups: Vec<CompactAddressTableLookup<'a>>,
}

/// A v1 message. There is no `address_table_lookups` field because v1 has no
/// lookup tables — every account key is inline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactV1Message<'a> {
    pub header: CompactMessageHeader,
    pub config: CompactTransactionConfig,
    pub account_keys: Vec<CompactPubkey>,
    pub recent_blockhash: CompactRecentBlockhash<'a>,
    #[serde(borrow)]
    pub instructions: Vec<CompactInstruction<'a>>,
}
