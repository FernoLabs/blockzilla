use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

use crate::CompactMetaV1;

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactBlockRecord<'a> {
    pub header: CompactBlockHeader,
    #[serde(borrow)]
    pub txs: Vec<CompactTxWithMeta<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactShredding {
    pub entry_end_idx: i64,
    pub shred_end_idx: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactPohEntry {
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub tx_count: u32,
    /// Total signatures across this entry's transactions (sum of each transaction's
    /// `signatures.len()`, not the transaction count). Lets `verify-archive-v2-poh`
    /// slice `signatures.bin` per entry directly from the PoH sidecar instead of
    /// decompressing and decoding the hot block just to read per-transaction
    /// signature counts.
    pub signature_count: u32,
}

/// `CompactPohEntry` before `signature_count` was added. Every `poh.wincode` sidecar
/// written before that migration lands has this layout; see
/// [`blockzilla_archive_v2::deserialize_archive_v2_poh_record`].
#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactPohEntryLegacyNoSignatureCount {
    pub num_hashes: u64,
    pub hash: [u8; 32],
    pub tx_count: u32,
}

impl From<CompactPohEntryLegacyNoSignatureCount> for CompactPohEntry {
    fn from(value: CompactPohEntryLegacyNoSignatureCount) -> Self {
        CompactPohEntry {
            num_hashes: value.num_hashes,
            hash: value.hash,
            tx_count: value.tx_count,
            // Real signature counts aren't recoverable from this legacy record alone; callers
            // that need them must derive from the hot block's `tx_rows` (see
            // `patch_poh_entry_signature_counts` and the `migrate-poh-signature-counts` command).
            // Verifiers must treat 0 here as "unknown", not "no signatures".
            signature_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactDataFrame {
    pub hash: Option<u64>,
    pub index: Option<u64>,
    pub total: Option<u64>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CompactBlockHeader {
    pub slot: u64,
    pub parent_slot: u64,
    pub blockhash: u32,
    pub previous_blockhash: u32,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub shredding: Vec<CompactShredding>,
    pub poh_entries: Vec<CompactPohEntry>,
    pub rewards: Option<CompactDataFrame>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompactTxWithMeta<'a> {
    #[serde(borrow)]
    pub tx: crate::compact::CompactTransaction<'a>,
    pub metadata: Option<CompactMetaV1>,
}
