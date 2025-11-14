use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::Path;
use wincode::{SchemaRead, SchemaWrite};

#[derive(Clone)]
#[allow(dead_code)]
struct BlockProgramUsage {
    pub epoch: u64,
    pub slot: u64,
    pub block_time: Option<i64>,
    // inclusive range into a pooled records vec
    pub start: usize,
    pub len: usize,
}

struct PerBlockStats {
    pub blocks: Vec<BlockProgramUsage>,
    pub records: Vec<ProgramUsageRecord>,
}

struct ProgramStatsCollection {
    pub aggregated: Vec<ProgramUsageRecord>,
    #[allow(dead_code)]
    pub per_block: Option<PerBlockStats>,
    pub per_epoch: Vec<ProgramUsageEpochPart>,
    pub processed_epochs: Vec<u64>,
}

#[derive(Default, Clone, Copy)]
struct EpochMetrics {
    pub blocks_scanned: u64,
    pub txs_seen: u64,
    pub instructions_seen: u64,
    pub inner_instructions_seen: u64,
    pub bytes_count: u64,
}

struct EpochProcessSummary {
    pub epoch: u64,
    pub last_slot: u64,
    pub last_blocktime: Option<i64>,
    pub records: Vec<ProgramUsageRecord>,
    pub per_block: Option<PerBlockStats>,
    pub metrics: EpochMetrics,
}

#[derive(Debug, Clone, Copy, Default, SchemaRead, SchemaWrite, Serialize, Deserialize)]
pub struct ProgramUsageStats {
    pub tx_count: u64,
    pub instruction_count: u64,
    pub inner_instruction_count: u64,
}

impl ProgramUsageStats {
    fn accumulate(&mut self, other: &ProgramUsageStats) {
        self.tx_count += other.tx_count;
        self.instruction_count += other.instruction_count;
        self.inner_instruction_count += other.inner_instruction_count;
    }
}

const PROGRAM_USAGE_RECORDS_PREALLOCATION_LIMIT: usize = 512 << 20; // 512 MiB
const PROGRAM_USAGE_EPOCHS_PREALLOCATION_LIMIT: usize = 64 << 20; // 64 MiB

mod program_usage_wincode {
    use super::*;

    pub type RecordVec = wincode::containers::Vec<
        wincode::containers::Elem<ProgramUsageRecord>,
        wincode::len::BincodeLen<{ PROGRAM_USAGE_RECORDS_PREALLOCATION_LIMIT }>,
    >;

    pub type EpochVec = wincode::containers::Vec<
        wincode::containers::Elem<ProgramUsageEpochPart>,
        wincode::len::BincodeLen<{ PROGRAM_USAGE_EPOCHS_PREALLOCATION_LIMIT }>,
    >;
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct ProgramUsageRecord {
    pub program: [u8; 32],
    pub stats: ProgramUsageStats,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct ProgramUsageEpochPartV1 {
    pub epoch: u64,
    pub last_slot: u64,
    #[wincode(with = "program_usage_wincode::RecordVec")]
    pub records: Vec<ProgramUsageRecord>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct ProgramUsageEpochPart {
    pub epoch: u64,
    pub last_slot: u64,
    pub last_blocktime: Option<i64>,
    #[wincode(with = "program_usage_wincode::RecordVec")]
    pub records: Vec<ProgramUsageRecord>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct ProgramStatsProgress {
    pub processed_epochs: Vec<u64>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct ProgramUsageExport {
    pub version: u32,
    pub start_epoch: u64,
    pub start_slot: u64,
    pub top_level_only: bool,
    pub processed_epochs: Vec<u64>,
    #[wincode(with = "program_usage_wincode::RecordVec")]
    pub aggregated: Vec<ProgramUsageRecord>,
    #[wincode(with = "program_usage_wincode::EpochVec")]
    pub epochs: Vec<ProgramUsageEpochPart>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramUsagePartsPreference {
    Auto,
    TopLevelOnly,
    IncludeInner,
}

impl ProgramUsagePartsPreference {
    fn ensure_matches(self, top_level_only: bool, source: &Path) -> Result<()> {
        match self {
            ProgramUsagePartsPreference::Auto => Ok(()),
            ProgramUsagePartsPreference::TopLevelOnly if top_level_only => Ok(()),
            ProgramUsagePartsPreference::IncludeInner if !top_level_only => Ok(()),
            ProgramUsagePartsPreference::TopLevelOnly => Err(anyhow!(
                "top-level stats were requested but {} contains full stats",
                source.display()
            )),
            ProgramUsagePartsPreference::IncludeInner => Err(anyhow!(
                "full stats were requested but {} only contains top-level stats",
                source.display()
            )),
        }
    }
}

const PROGRAM_USAGE_EXPORT_VERSION: u32 = 1;
const PROGRESS_FILE_NAME: &str = "progress.bin";

mod processing;
mod reading;

pub use processing::{dump_program_stats, dump_program_stats_csv, dump_program_stats_par};
