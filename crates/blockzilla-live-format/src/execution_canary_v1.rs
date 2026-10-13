//! Explicitly noncanonical, finite execution canary contracts.
//!
//! Finality in this profile is an operator's trust in its own finalized validator
//! subscription and RPC. It is not independent consensus or shred verification.

use serde::{Deserialize, Serialize};

pub const EXECUTION_SOURCE_PROFILE: &str = "validator-finalized-execution-v1";
pub const NORMALIZED_EXECUTION_SCHEMA: &str = "hivezilla-finalized-execution-normalized-v1";
pub const NORMALIZED_EXECUTION_FILE: &str = "FINALIZED-EXECUTION-NORMALIZED.v1.json";
pub const EXECUTION_CANARY_SCHEMA: &str = "blockzilla-live-execution-canary-v1";
pub const EXECUTION_CANARY_FILE: &str = "LIVE-EXECUTION-CANARY.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionFileV1 {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

/// An operator-retained reply from the same validator's finalized getBlocks.
/// The raw JSON response is stored alongside this file as rpc-get-blocks.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizedRpcCoverageV1 {
    pub schema: String,
    pub genesis_hash: String,
    pub node_identity: String,
    pub commitment: String,
    pub start_slot: u64,
    pub end_slot_exclusive: u64,
    pub finalized_slot: u64,
    pub produced_slots: Vec<u64>,
    pub rpc_response_sha256: String,
    pub source_receipt_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionBlockV1 {
    pub block_id: u32,
    pub slot: u64,
    pub bank_id: u64,
    pub blockhash: [u8; 32],
    pub parent_slot: u64,
    pub parent_blockhash: [u8; 32],
    pub transactions: u64,
    pub signatures: u64,
    pub entries: u64,
    pub source_sequence: u64,
    pub source_raw_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedExecutionV1 {
    pub schema: String,
    pub source_profile: String,
    pub state: String,
    pub canonical: bool,
    pub finalized: bool,
    pub finality_basis: String,
    pub epoch: u64,
    pub start_slot: u64,
    pub end_slot_exclusive: u64,
    pub genesis_hash: String,
    pub node_identity: String,
    pub source_receipt_sha256: String,
    pub coverage_receipt_sha256: String,
    pub rpc_response_sha256: String,
    pub transactions: u64,
    pub signatures: u64,
    pub entries: u64,
    pub blocks: Vec<ExecutionBlockV1>,
    pub files: Vec<ExecutionFileV1>,
    pub poh_structure_verified: bool,
    pub poh_hashes_recomputed: bool,
    pub signatures_cryptographically_verified: bool,
    pub shred_evidence: String,
    pub execution_projection: String,
    pub authoritative_execution: String,
}
