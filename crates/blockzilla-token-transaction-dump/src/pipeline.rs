use std::path::PathBuf;

use anyhow::Result;
use blockzilla_read_sdk::ArchiveV2WireProfile;
use serde::Serialize;

use crate::{consolidate_epoch_shards, extract_epoch_shards};

#[derive(Debug, Clone)]
pub struct ExtractConfig {
    pub archive_root: PathBuf,
    pub output: PathBuf,
    pub mint: String,
    pub mint_slot: u64,
    pub mint_signature: String,
    pub workers: usize,
    pub last_epoch: Option<u64>,
    pub source_mode: ExtractSourceMode,
    /// Continue a previously checkpointed extraction in the same output root.
    pub resume: bool,
    /// Run discovery and pass-B in one epoch loop with a barrier between them.
    ///
    /// `false` (default): run all discovery epochs, then run all raw-copy epochs.
    /// `true`: run discovery for one epoch, then raw-copy that same epoch.
    pub epoch_barrier: bool,
    /// Read each source batch once, run parallel discovery, merge its creations,
    /// then scan the retained decompressed bytes for matching transactions.
    pub single_read_batches: bool,
    /// Record exact pre-merge account matches during single-read stage A and
    /// use them to avoid a second account-list scan when it is safe.
    pub single_read_match_hints: bool,
    pub allow_indeterminate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractSourceMode {
    /// Use a manifest-less local directory whose identity the operator
    /// explicitly asserts. Only admitted file names and sizes are bound.
    TrustedLocal {
        cluster_id: String,
        slots_per_epoch: u64,
        wire_profile: ArchiveV2WireProfile,
    },
}

#[derive(Debug, Clone)]
pub struct ConsolidateConfig {
    pub archive_root: PathBuf,
    pub input: PathBuf,
    pub output: PathBuf,
}

/// Read-only settings for a bounded scan of one trusted local epoch.
///
/// This mode does not create a dump. It exists only to measure the first
/// extraction pass against a manifest-less local archive.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub epoch_path: PathBuf,
    pub cluster_id: String,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub start_slot: u64,
    pub expected_start_row: Option<usize>,
    pub max_blocks: usize,
    pub mint: String,
    pub mint_signature: String,
    pub workers: usize,
    pub wire_profile: ArchiveV2WireProfile,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ProbeReaderStats {
    pub block_count: u64,
    pub batch_count: u64,
    pub read_call_count: u64,
    pub compressed_bytes: u64,
    pub producer_read_seconds: f64,
    pub decode_and_project_seconds: f64,
    pub producer_wait_for_buffer_seconds: f64,
    pub coordinator_wait_for_batch_seconds: f64,
    pub max_compressed_batch_bytes: usize,
    pub max_declared_uncompressed_batch_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProbeReport {
    pub schema_version: u32,
    pub kind: &'static str,
    pub epoch_path: PathBuf,
    pub epoch: u64,
    pub wire_profile: &'static str,
    pub workers: usize,
    pub requested_start_slot: u64,
    pub start_row: usize,
    pub end_row_exclusive: usize,
    pub first_slot: u64,
    pub last_slot: u64,
    pub blocks: u64,
    pub transactions: u64,
    pub selected_transactions: u64,
    pub tracked_accounts: usize,
    pub owned_block_fallbacks: u64,
    pub compressed_bytes: u64,
    pub elapsed_seconds: f64,
    pub blocks_per_second: f64,
    pub transactions_per_second: f64,
    pub compressed_mib_per_second: f64,
    pub reader: ProbeReaderStats,
}

pub fn extract_dump(config: ExtractConfig) -> Result<()> {
    extract_epoch_shards(&config)
}

pub fn consolidate_dump(config: ConsolidateConfig) -> Result<()> {
    consolidate_epoch_shards(&config.archive_root, &config.input, &config.output)
}
