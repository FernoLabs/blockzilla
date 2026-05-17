use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::{
    CompactMetaV1, CompactPohEntry, CompactReward, CompactShredding, OwnedCompactTransaction,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveArchiveSource {
    OwnGrpc,
    Fumarole,
    LaserStream,
    DoubleZeroShred,
    ShredStream,
    RpcGetBlock,
    TritonCar,
    LocalCar,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveBlockState {
    Pending,
    CompleteLive,
    IncompleteLive,
    RpcFallback,
    CarRepaired,
    CarOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveBlockMissingField {
    BlockMeta,
    Transactions,
    TransactionStatus,
    Rewards,
    PohEntries,
    Shredding,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveBlockCompleteness {
    pub state: LiveBlockState,
    pub has_block_meta: bool,
    pub has_transactions: bool,
    pub has_transaction_status: bool,
    pub has_rewards: bool,
    pub has_poh_entries: bool,
    pub has_shredding: bool,
    pub source_rank: u8,
    pub repair_required: bool,
    pub missing: Vec<LiveBlockMissingField>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveSourceProvenance {
    pub source: LiveArchiveSource,
    pub received_at_unix_micros: Option<i64>,
    pub source_slot: Option<u64>,
    pub sequence: Option<u64>,
    pub endpoint: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveBlockDraft {
    pub slot: u64,
    pub parent_slot: Option<u64>,
    pub blockhash: Option<[u8; 32]>,
    pub previous_blockhash: Option<[u8; 32]>,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub transactions: Vec<LiveTransactionDraft>,
    pub rewards: Option<LiveRewardsPayload>,
    pub poh_entries: Option<Vec<CompactPohEntry>>,
    pub shredding: Option<Vec<CompactShredding>>,
    pub completeness: LiveBlockCompleteness,
    pub provenance: Vec<LiveSourceProvenance>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveTransactionDraft {
    pub tx_index: u32,
    pub payload: LiveTransactionPayload,
    pub metadata: Option<LiveTransactionMetadataPayload>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveTransactionPayload {
    Compact(OwnedCompactTransaction),
    Raw(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[allow(clippy::large_enum_variant)]
pub enum LiveTransactionMetadataPayload {
    Compact(CompactMetaV1),
    Raw(Vec<u8>),
    Missing,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum LiveRewardsPayload {
    Compact(Vec<CompactReward>),
    Raw(Vec<u8>),
    Missing,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveShredRecord {
    pub slot: u64,
    pub shred_index: u32,
    pub fec_set_index: Option<u32>,
    pub is_coding: bool,
    pub is_last_in_slot: Option<bool>,
    pub payload: Vec<u8>,
    pub provenance: LiveSourceProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveBlockRepairRecord {
    pub slot: u64,
    pub block_id: Option<u32>,
    pub source: LiveArchiveSource,
    pub repaired_fields: Vec<LiveBlockMissingField>,
    pub poh_entries: Option<Vec<CompactPohEntry>>,
    pub shredding: Option<Vec<CompactShredding>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LiveSignatureIndexRecord {
    pub signature_ordinal: u64,
    pub slot: u64,
    pub block_id: u32,
    pub tx_index: u32,
    pub signature_index: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct LivePubkeyCountRecord {
    pub pubkey: [u8; 32],
    pub count: u64,
}

impl LiveBlockCompleteness {
    pub fn complete_live(source_rank: u8) -> Self {
        Self {
            state: LiveBlockState::CompleteLive,
            has_block_meta: true,
            has_transactions: true,
            has_transaction_status: true,
            has_rewards: true,
            has_poh_entries: true,
            has_shredding: true,
            source_rank,
            repair_required: false,
            missing: Vec::new(),
        }
    }

    pub fn rpc_fallback(missing: Vec<LiveBlockMissingField>) -> Self {
        Self {
            state: LiveBlockState::RpcFallback,
            has_block_meta: !missing.contains(&LiveBlockMissingField::BlockMeta),
            has_transactions: !missing.contains(&LiveBlockMissingField::Transactions),
            has_transaction_status: !missing.contains(&LiveBlockMissingField::TransactionStatus),
            has_rewards: !missing.contains(&LiveBlockMissingField::Rewards),
            has_poh_entries: !missing.contains(&LiveBlockMissingField::PohEntries),
            has_shredding: !missing.contains(&LiveBlockMissingField::Shredding),
            source_rank: u8::MAX,
            repair_required: true,
            missing,
        }
    }
}
