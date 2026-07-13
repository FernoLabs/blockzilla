use std::{
    collections::HashMap,
    env,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    net::IpAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use blockzilla_format::{
    CompactBlockHeader, CompactInnerInstruction, CompactInnerInstructions, CompactLogParseStats,
    CompactLogParseStatsReport, CompactMessageHeader, CompactPohEntry, CompactTransactionError,
    LIVE_PUBKEY_RUN_HOT_FILE, LIVE_PUBKEY_RUN_RECORD_LEN, LIVE_PUBKEY_RUNS_DIR,
    LiveBlockMissingField, LivePubkeyCountRecord, LiveSignatureIndexRecord, RawPubkeyCompactor,
    SplitCompactIndexHeader, SplitCompactIndexRecord, WincodeArchiveV2NoRegistryAddressTableLookup,
    WincodeArchiveV2NoRegistryBlock, WincodeArchiveV2NoRegistryBlockHeader,
    WincodeArchiveV2NoRegistryInstruction, WincodeArchiveV2NoRegistryLegacyMessage,
    WincodeArchiveV2NoRegistryLogs, WincodeArchiveV2NoRegistryMessage,
    WincodeArchiveV2NoRegistryMeta, WincodeArchiveV2NoRegistryReturnData,
    WincodeArchiveV2NoRegistryReward, WincodeArchiveV2NoRegistryRewards,
    WincodeArchiveV2NoRegistryTokenBalance, WincodeArchiveV2NoRegistryTransaction,
    WincodeArchiveV2NoRegistryTx, WincodeArchiveV2NoRegistryV0Message, WincodeArchiveV2Payload,
    WincodeArchiveV2PohRecord, WincodeLeb128FramedReader, WincodeLeb128FramedWriter,
    encode_with_scratch, parse_log_strs_with_compactor, parse_log_strs_with_compactor_and_stats,
    parse_logs_with_compactor, parse_logs_with_compactor_and_stats, read_u32_varint,
    wincode_leb128_config,
};
use futures::StreamExt;
use gxhash::{GxBuildHasher, HashMap as GxHashMap};
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, Message as GrpcMessage, Reward as GrpcReward, Rewards as GrpcRewards,
    SlotStatus, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterEntry, SubscribeRequestFilterSlots, SubscribeUpdateBlock,
    TokenBalance as GrpcTokenBalance, Transaction as GrpcTransaction,
    TransactionStatusMeta as GrpcTransactionStatusMeta, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

use crate::{
    epoch::{EpochBoundaryEvent, EpochBoundaryTracker, EpochSlot, OLD_FAITHFUL_SLOTS_PER_EPOCH},
    layout::ProducerLayout,
    rpc::{RpcEpochSyncConfig, RpcEpochSyncReport, RpcRateLimitConfig, sync_epoch_info},
};

type LivePubkeyCounts = GxHashMap<[u8; 32], u32>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrpcRawBlockStorage {
    #[default]
    All,
    Failure,
    None,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrpcPubkeyIndexMode {
    #[default]
    Counts,
    Touches,
    Runs,
    CountsAndTouches,
    CountsAndRuns,
    None,
}

impl GrpcPubkeyIndexMode {
    fn writes_counts(self) -> bool {
        matches!(
            self,
            Self::Counts | Self::CountsAndTouches | Self::CountsAndRuns
        )
    }

    fn writes_touches(self) -> bool {
        matches!(self, Self::Touches | Self::CountsAndTouches)
    }

    fn writes_runs(self) -> bool {
        matches!(self, Self::Runs | Self::CountsAndRuns)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcProbeConfig {
    pub endpoint: String,
    pub max_updates: usize,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcCaptureConfig {
    pub endpoint: String,
    pub archive_dir: PathBuf,
    pub max_blocks: usize,
    pub timeout_secs: u64,
    pub from_slot: Option<u64>,
    pub slots_per_epoch: u64,
    pub stop_at_epoch_boundary: bool,
    pub raw_block_storage: GrpcRawBlockStorage,
    pub pubkey_index_mode: GrpcPubkeyIndexMode,
    pub pubkey_hot_registry_path: Option<PathBuf>,
    pub pubkey_hot_count: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrpcEpochCommitment {
    Processed,
    Confirmed,
    #[default]
    Finalized,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcEpochWatchConfig {
    pub endpoint: String,
    pub startup_rpc_url: Option<String>,
    pub startup_rpc_commitment: String,
    pub startup_rpc_rate_limit: RpcRateLimitConfig,
    pub timeout_secs: u64,
    pub max_updates: usize,
    pub max_boundaries: usize,
    pub from_slot: Option<u64>,
    pub slots_per_epoch: u64,
    pub commitment: GrpcEpochCommitment,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcCaptureReport {
    pub endpoint: String,
    pub archive_dir: PathBuf,
    pub blocks_seen: usize,
    pub blocks_written: usize,
    pub entries_written: usize,
    pub transactions_written: usize,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub first_epoch: Option<u64>,
    pub last_epoch: Option<u64>,
    pub stopped_at_epoch_boundary: bool,
    pub elapsed_ms: u128,
    pub processing_ms: u128,
    pub block_bytes_written: u64,
    pub raw_block_bytes_written: u64,
    pub poh_bytes_written: u64,
    pub block_index_bytes_written: u64,
    pub blockhash_bytes_written: u64,
    pub signature_bytes_written: u64,
    pub signature_index_bytes_written: u64,
    pub pubkey_count_records: usize,
    pub mb_per_sec: f64,
    pub processing_mb_per_sec: f64,
    pub blocks_per_sec: f64,
    pub processing_blocks_per_sec: f64,
    pub block_path: PathBuf,
    pub raw_blocks_path: PathBuf,
    pub poh_path: PathBuf,
    pub block_index_path: PathBuf,
    pub blockhash_registry_path: PathBuf,
    pub signatures_path: PathBuf,
    pub signature_index_path: PathBuf,
    pub pubkey_counts_path: PathBuf,
    pub pubkey_touches_path: PathBuf,
    pub pubkey_runs_dir: PathBuf,
    pub pubkey_touch_bytes_written: u64,
    pub pubkey_run_bytes_written: u64,
    pub pubkey_run_files: usize,
    pub pubkey_run_records: u64,
    pub pubkey_hot_keys: usize,
    pub pubkey_hot_records: usize,
    pub journal_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcPubkeyRunBackfillConfig {
    pub archive_dir: PathBuf,
    pub output_run_dir: Option<PathBuf>,
    pub start_block_id: u32,
    pub max_blocks: Option<usize>,
    pub reset_output_dir: bool,
    pub pubkey_hot_registry_path: Option<PathBuf>,
    pub pubkey_hot_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcPubkeyRunBackfillReport {
    pub archive_dir: PathBuf,
    pub block_path: PathBuf,
    pub output_run_dir: PathBuf,
    pub start_block_id: u32,
    pub blocks_scanned: usize,
    pub transactions_scanned: usize,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub input_start_offset: u64,
    pub input_snapshot_bytes: u64,
    pub input_bytes_read: u64,
    pub stopped_by_max_blocks: bool,
    pub tail_truncated: bool,
    pub elapsed_ms: u128,
    pub blocks_per_sec: f64,
    pub tx_per_sec: f64,
    pub pubkey_run_bytes_written: u64,
    pub pubkey_run_files: usize,
    pub pubkey_run_records: u64,
    pub pubkey_hot_keys: usize,
    pub pubkey_hot_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcCompactLogsBackfillConfig {
    pub archive_dir: PathBuf,
    pub output_archive_dir: PathBuf,
    pub max_blocks: Option<usize>,
    pub start_block_id: u32,
    pub append_output: bool,
    pub overwrite_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcCompactLogsBackfillReport {
    pub archive_dir: PathBuf,
    pub output_archive_dir: PathBuf,
    pub input_block_path: PathBuf,
    pub output_block_path: PathBuf,
    pub output_block_index_path: PathBuf,
    pub start_block_id: u32,
    pub append_output: bool,
    pub blocks_rewritten: usize,
    pub transactions_scanned: usize,
    pub txs_with_logs: usize,
    pub raw_log_streams_compacted: usize,
    pub zstd_log_streams_compacted: usize,
    pub compact_log_streams_kept: usize,
    pub log_lines_compacted: u64,
    pub input_block_bytes: u64,
    pub input_start_offset: u64,
    pub input_bytes_read: u64,
    pub output_block_bytes: u64,
    pub output_start_block_bytes: u64,
    pub output_start_block_index_rows: u32,
    pub sidecar_files_linked: usize,
    pub sidecar_files_copied: usize,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub stopped_by_max_blocks: bool,
    pub tail_truncated: bool,
    pub elapsed_ms: u128,
    pub blocks_per_sec: f64,
    pub tx_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureInspectReport {
    pub archive_dir: PathBuf,
    pub block_frames: usize,
    pub raw_block_frames: usize,
    pub block_index_rows: usize,
    pub poh_frames: usize,
    pub signature_index_rows: usize,
    pub signatures: usize,
    pub pubkey_count_records: usize,
    pub pubkey_run_files: usize,
    pub pubkey_run_records: u64,
    pub pubkey_run_bytes: u64,
    pub transactions: usize,
    pub poh_entries: usize,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GrpcCapturedBlockJournal {
    slot: u64,
    epoch: u64,
    epoch_slot_index: u64,
    block_id: u32,
    parent_slot: u64,
    transactions: usize,
    entries: usize,
    executed_transaction_count: u64,
    entries_count: u64,
    updated_account_count: u64,
    missing: Vec<LiveBlockMissingField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcCaptureProgress {
    pub schema_version: u32,
    pub pid: u32,
    pub phase: String,
    pub state: String,
    pub capture_dir: PathBuf,
    pub epoch: Option<u64>,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub epoch_slot_index: Option<u64>,
    pub blocks_done: u64,
    pub transactions_done: u64,
    pub entries_done: u64,
    pub slots_total: u64,
    pub elapsed_secs: f64,
    pub blocks_per_sec: f64,
    pub progress_pct: Option<f64>,
    pub eta_secs: Option<f64>,
    pub stopped_at_epoch_boundary: bool,
    pub updated_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcProbeReport {
    pub endpoint: String,
    pub version: Option<String>,
    pub finalized_slot: Option<u64>,
    pub confirmed_slot: Option<u64>,
    pub replay_first_available: Option<u64>,
    pub updates_seen: usize,
    pub block_meta_seen: usize,
    pub entries_seen: usize,
    pub slots_seen: usize,
    pub first_block_meta: Option<GrpcProbeBlockMeta>,
    pub first_entry: Option<GrpcProbeEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcProbeBlockMeta {
    pub slot: u64,
    pub parent_slot: u64,
    pub executed_transaction_count: u64,
    pub entries_count: u64,
    pub has_block_time: bool,
    pub has_block_height: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcProbeEntry {
    pub slot: u64,
    pub index: u64,
    pub num_hashes: u64,
    pub hash_len: usize,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcEpochWatchReport {
    pub endpoint: String,
    pub startup_epoch: Option<RpcEpochSyncReport>,
    pub commitment: GrpcEpochCommitment,
    pub from_slot: Option<u64>,
    pub slots_per_epoch: u64,
    pub updates_seen: usize,
    pub slot_updates_seen: usize,
    pub block_meta_seen: usize,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub current_epoch: Option<u64>,
    pub boundaries: Vec<GrpcEpochBoundaryRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcEpochBoundaryRecord {
    pub event: EpochBoundaryEvent,
    pub source: GrpcEpochBoundarySource,
    pub slot_status: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrpcEpochBoundarySource {
    Slot,
    BlockMeta,
}

impl Default for GrpcEpochWatchConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            startup_rpc_url: None,
            startup_rpc_commitment: "finalized".to_string(),
            startup_rpc_rate_limit: RpcRateLimitConfig::default(),
            timeout_secs: 60,
            max_updates: 0,
            max_boundaries: 1,
            from_slot: None,
            slots_per_epoch: OLD_FAITHFUL_SLOTS_PER_EPOCH,
            commitment: GrpcEpochCommitment::Finalized,
        }
    }
}

impl From<GrpcEpochCommitment> for CommitmentLevel {
    fn from(value: GrpcEpochCommitment) -> Self {
        match value {
            GrpcEpochCommitment::Processed => Self::Processed,
            GrpcEpochCommitment::Confirmed => Self::Confirmed,
            GrpcEpochCommitment::Finalized => Self::Finalized,
        }
    }
}

pub async fn probe_grpc(config: GrpcProbeConfig) -> Result<GrpcProbeReport> {
    let mut client = connect_grpc(&config.endpoint).await?;

    let version = client.get_version().await.ok().map(|value| value.version);
    let finalized_slot = client
        .get_slot(Some(CommitmentLevel::Finalized))
        .await
        .ok()
        .map(|value| value.slot);
    let confirmed_slot = client
        .get_slot(Some(CommitmentLevel::Confirmed))
        .await
        .ok()
        .map(|value| value.slot);
    let replay_first_available = client
        .subscribe_replay_info()
        .await
        .ok()
        .and_then(|value| value.first_available);

    let request = SubscribeRequest {
        blocks_meta: HashMap::from([(
            "block-meta".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        entry: HashMap::from([("entry".to_string(), SubscribeRequestFilterEntry {})]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    };

    let mut stream = client.subscribe_once(request).await?;
    let mut report = GrpcProbeReport {
        endpoint: config.endpoint,
        version,
        finalized_slot,
        confirmed_slot,
        replay_first_available,
        updates_seen: 0,
        block_meta_seen: 0,
        entries_seen: 0,
        slots_seen: 0,
        first_block_meta: None,
        first_entry: None,
    };

    let deadline = Duration::from_secs(config.timeout_secs);
    let max_updates = config.max_updates.max(1);
    let read_updates = async {
        while report.updates_seen < max_updates {
            let Some(update) = stream.next().await else {
                break;
            };
            let update = update?;
            report.updates_seen += 1;

            match update.update_oneof {
                Some(UpdateOneof::BlockMeta(meta)) => {
                    report.block_meta_seen += 1;
                    if report.first_block_meta.is_none() {
                        report.first_block_meta = Some(GrpcProbeBlockMeta {
                            slot: meta.slot,
                            parent_slot: meta.parent_slot,
                            executed_transaction_count: meta.executed_transaction_count,
                            entries_count: meta.entries_count,
                            has_block_time: meta.block_time.is_some(),
                            has_block_height: meta.block_height.is_some(),
                        });
                    }
                }
                Some(UpdateOneof::Entry(entry)) => {
                    report.entries_seen += 1;
                    if report.first_entry.is_none() {
                        report.first_entry = Some(GrpcProbeEntry {
                            slot: entry.slot,
                            index: entry.index,
                            num_hashes: entry.num_hashes,
                            hash_len: entry.hash.len(),
                            executed_transaction_count: entry.executed_transaction_count,
                            starting_transaction_index: entry.starting_transaction_index,
                        });
                    }
                }
                Some(UpdateOneof::Slot(_slot)) => {
                    report.slots_seen += 1;
                }
                _ => {}
            }

            if report.first_block_meta.is_some() && report.first_entry.is_some() {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    timeout(deadline, read_updates)
        .await
        .map_err(|_| anyhow!("timed out waiting for gRPC stream updates"))??;

    Ok(report)
}

pub async fn watch_grpc_epoch_boundaries(
    config: GrpcEpochWatchConfig,
) -> Result<GrpcEpochWatchReport> {
    let mut client = connect_grpc(&config.endpoint).await?;
    let startup_epoch = match &config.startup_rpc_url {
        Some(rpc_url) => Some(
            sync_epoch_info(RpcEpochSyncConfig {
                rpc_url: rpc_url.clone(),
                commitment: config.startup_rpc_commitment.clone(),
                timeout_secs: config.timeout_secs.clamp(1, 10),
                rate_limit: config.startup_rpc_rate_limit.clone(),
            })
            .await
            .with_context(|| format!("startup RPC epoch sync from {rpc_url}"))?,
        ),
        None => None,
    };
    let commitment = CommitmentLevel::from(config.commitment);
    let request = SubscribeRequest {
        slots: HashMap::from([(
            "slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        )]),
        blocks_meta: HashMap::from([(
            "block-meta".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]),
        commitment: Some(commitment as i32),
        from_slot: config.from_slot,
        ..Default::default()
    };
    let mut stream = client.subscribe_once(request).await?;
    let mut tracker = EpochBoundaryTracker::new(config.slots_per_epoch);
    if let Some(startup) = &startup_epoch {
        tracker.seed(startup.epoch, Some(startup.absolute_slot));
    }
    let mut report = GrpcEpochWatchReport {
        endpoint: config.endpoint,
        startup_epoch,
        commitment: config.commitment,
        from_slot: config.from_slot,
        slots_per_epoch: config.slots_per_epoch.max(1),
        updates_seen: 0,
        slot_updates_seen: 0,
        block_meta_seen: 0,
        first_slot: None,
        last_slot: None,
        current_epoch: tracker.current_epoch(),
        boundaries: Vec::new(),
    };

    let deadline = Duration::from_secs(config.timeout_secs);
    let max_updates = config.max_updates;
    let max_boundaries = config.max_boundaries.max(1);
    let read_updates = async {
        loop {
            if max_updates > 0 && report.updates_seen >= max_updates {
                break;
            }
            if report.boundaries.len() >= max_boundaries {
                break;
            }

            let Some(update) = stream.next().await else {
                break;
            };
            let update = update?;
            report.updates_seen += 1;
            match update.update_oneof {
                Some(UpdateOneof::Slot(slot)) => {
                    report.slot_updates_seen += 1;
                    let status = SlotStatus::try_from(slot.status)
                        .ok()
                        .map(|status| status.as_str_name().to_string());
                    observe_epoch_slot(
                        slot.slot,
                        GrpcEpochBoundarySource::Slot,
                        status,
                        &mut tracker,
                        &mut report,
                    );
                }
                Some(UpdateOneof::BlockMeta(meta)) => {
                    report.block_meta_seen += 1;
                    observe_epoch_slot(
                        meta.slot,
                        GrpcEpochBoundarySource::BlockMeta,
                        None,
                        &mut tracker,
                        &mut report,
                    );
                }
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(())
    };

    timeout(deadline, read_updates)
        .await
        .map_err(|_| anyhow!("timed out waiting for epoch boundary updates"))??;

    report.current_epoch = tracker.current_epoch();
    Ok(report)
}

pub async fn capture_grpc_blocks(config: GrpcCaptureConfig) -> Result<GrpcCaptureReport> {
    let layout = ProducerLayout::create(&config.archive_dir)?;
    let block_path = layout.blocks_dir.join("live-no-registry-blocks.bin");
    let raw_blocks_path = layout.blocks_dir.join("grpc-raw-blocks.bin");
    let failed_raw_blocks_path = layout.repair_dir.join("grpc-failed-blocks.bin");
    let failed_raw_blocks_journal_path = layout.repair_dir.join("grpc-failed-blocks.jsonl");
    let poh_path = layout.poh_dir.join("poh.wincode");
    let block_index_path = layout.index_dir.join("block-index.bin");
    let blockhash_registry_path = layout.index_dir.join("blockhash_registry.bin");
    let signatures_path = layout.index_dir.join("signatures.bin");
    let signature_index_path = layout.index_dir.join("signature-index.bin");
    let pubkey_counts_path = layout.index_dir.join("pubkey-counts.bin");
    let pubkey_touches_path = layout.index_dir.join("pubkey-touches.bin");
    let pubkey_runs_dir = layout.index_dir.join(LIVE_PUBKEY_RUNS_DIR);
    let journal_path = layout.journal_dir.join("grpc-blocks.jsonl");
    let progress_path = layout.journal_dir.join("progress.json");
    // Preserve the epoch already owned by a resumed capture. Otherwise the first replayed block
    // becomes this invocation's epoch and a retry can append the next epoch to the same files.
    let mut capture_epoch = read_existing_capture_epoch(&journal_path)?;

    let next_block_id = count_existing_index_rows(&block_index_path)?;
    let mut block_offset = file_len(&block_path)?;
    let block_bytes_before = block_offset;
    let raw_block_bytes_before = file_len(&raw_blocks_path)?;
    let poh_bytes_before = file_len(&poh_path)?;
    let block_index_bytes_before = file_len(&block_index_path)?;
    let blockhash_bytes_before = file_len(&blockhash_registry_path)?;
    let signature_bytes_before = file_len(&signatures_path)?;
    let signature_index_bytes_before = file_len(&signature_index_path)?;
    let mut signature_ordinal = signature_bytes_before / 64;
    let pubkey_touch_bytes_before = file_len(&pubkey_touches_path)?;
    let pubkey_run_bytes_before = dir_file_len(&pubkey_runs_dir)?;
    let mut pubkey_counts = if config.pubkey_index_mode.writes_counts() {
        Some(read_pubkey_counts(&pubkey_counts_path)?)
    } else {
        None
    };
    let mut pubkey_run_writer = if config.pubkey_index_mode.writes_runs() {
        Some(PubkeyRunWriter::open(
            &pubkey_runs_dir,
            config.pubkey_hot_registry_path.as_deref(),
            config.pubkey_hot_count,
        )?)
    } else {
        None
    };

    let block_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&block_path)
        .with_context(|| format!("open {}", block_path.display()))?;
    let raw_blocks_file = if config.raw_block_storage == GrpcRawBlockStorage::All {
        Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&raw_blocks_path)
                .with_context(|| format!("open {}", raw_blocks_path.display()))?,
        )
    } else {
        None
    };
    let failed_raw_blocks_file = if config.raw_block_storage == GrpcRawBlockStorage::Failure {
        Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&failed_raw_blocks_path)
                .with_context(|| format!("open {}", failed_raw_blocks_path.display()))?,
        )
    } else {
        None
    };
    let failed_raw_blocks_journal_file = if config.raw_block_storage == GrpcRawBlockStorage::Failure
    {
        Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&failed_raw_blocks_journal_path)
                .with_context(|| format!("open {}", failed_raw_blocks_journal_path.display()))?,
        )
    } else {
        None
    };
    let poh_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&poh_path)
        .with_context(|| format!("open {}", poh_path.display()))?;
    let mut block_index_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&block_index_path)
        .with_context(|| format!("open {}", block_index_path.display()))?;
    if block_index_bytes_before == 0 {
        let mut header_writer = WincodeLeb128FramedWriter::new(&mut block_index_file);
        header_writer.write(&SplitCompactIndexHeader { version: 1 })?;
        header_writer.flush()?;
    }
    let blockhash_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&blockhash_registry_path)
        .with_context(|| format!("open {}", blockhash_registry_path.display()))?;
    let signatures_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&signatures_path)
        .with_context(|| format!("open {}", signatures_path.display()))?;
    let signature_index_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&signature_index_path)
        .with_context(|| format!("open {}", signature_index_path.display()))?;
    let pubkey_touches_file = if config.pubkey_index_mode.writes_touches() {
        Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&pubkey_touches_path)
                .with_context(|| format!("open {}", pubkey_touches_path.display()))?,
        )
    } else {
        None
    };
    let journal_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .with_context(|| format!("open {}", journal_path.display()))?;

    let mut block_writer = WincodeLeb128FramedWriter::new(BufWriter::new(block_file));
    let mut raw_blocks_writer = raw_blocks_file
        .map(BufWriter::new)
        .map(WincodeLeb128FramedWriter::new);
    let mut failed_raw_blocks_writer = failed_raw_blocks_file
        .map(BufWriter::new)
        .map(WincodeLeb128FramedWriter::new);
    let mut failed_raw_blocks_journal_writer = failed_raw_blocks_journal_file.map(BufWriter::new);
    let mut poh_writer = WincodeLeb128FramedWriter::new(BufWriter::new(poh_file));
    let mut block_index_writer = WincodeLeb128FramedWriter::new(BufWriter::new(block_index_file));
    let mut blockhash_writer = BufWriter::new(blockhash_file);
    let mut signatures_writer = BufWriter::new(signatures_file);
    let mut signature_index_writer =
        WincodeLeb128FramedWriter::new(BufWriter::new(signature_index_file));
    let mut pubkey_touches_writer = pubkey_touches_file.map(BufWriter::new);
    let mut journal_writer = BufWriter::new(journal_file);

    let mut client = connect_grpc(&config.endpoint).await?;
    let request = SubscribeRequest {
        blocks: HashMap::from([(
            "blocks".to_string(),
            SubscribeRequestFilterBlocks {
                include_transactions: Some(true),
                include_accounts: Some(false),
                include_entries: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        from_slot: config.from_slot,
        ..Default::default()
    };
    let mut stream = client.subscribe_once(request).await?;

    let started_at = Instant::now();
    let mut last_progress_write = None;
    let mut report = GrpcCaptureReport {
        endpoint: config.endpoint,
        archive_dir: layout.archive_dir,
        blocks_seen: 0,
        blocks_written: 0,
        entries_written: 0,
        transactions_written: 0,
        first_slot: None,
        last_slot: None,
        first_epoch: None,
        last_epoch: None,
        stopped_at_epoch_boundary: false,
        elapsed_ms: 0,
        processing_ms: 0,
        block_bytes_written: 0,
        raw_block_bytes_written: 0,
        poh_bytes_written: 0,
        block_index_bytes_written: 0,
        blockhash_bytes_written: 0,
        signature_bytes_written: 0,
        signature_index_bytes_written: 0,
        pubkey_count_records: 0,
        pubkey_touch_bytes_written: 0,
        mb_per_sec: 0.0,
        processing_mb_per_sec: 0.0,
        blocks_per_sec: 0.0,
        processing_blocks_per_sec: 0.0,
        block_path,
        raw_blocks_path,
        poh_path,
        block_index_path,
        blockhash_registry_path,
        signatures_path,
        signature_index_path,
        pubkey_counts_path,
        pubkey_touches_path,
        pubkey_runs_dir,
        journal_path,
        pubkey_run_bytes_written: 0,
        pubkey_run_files: 0,
        pubkey_run_records: 0,
        pubkey_hot_keys: 0,
        pubkey_hot_records: 0,
    };

    let deadline = Duration::from_secs(config.timeout_secs);
    let max_blocks = config.max_blocks.max(1);
    let mut processing_duration = Duration::ZERO;
    let mut block_scratch = Vec::with_capacity(2 * 1024 * 1024);
    let mut pubkey_run_block_keys = Vec::new();
    let read_blocks = async {
        while report.blocks_written < max_blocks {
            let Some(update) = stream.next().await else {
                break;
            };
            let update = update?;
            let Some(UpdateOneof::Block(block)) = update.update_oneof else {
                continue;
            };
            report.blocks_seen += 1;
            let processing_started_at = Instant::now();
            let epoch_slot = EpochSlot::from_slot(block.slot, config.slots_per_epoch);
            if config.stop_at_epoch_boundary
                && let Some(existing_epoch) = capture_epoch
                && epoch_slot.epoch != existing_epoch
            {
                report.stopped_at_epoch_boundary = true;
                break;
            }
            capture_epoch.get_or_insert(epoch_slot.epoch);

            let local_block_index = u32::try_from(report.blocks_written)
                .context("captured block count exceeds u32::MAX")?;
            let block_id = next_block_id
                .checked_add(local_block_index)
                .context("block id overflow")?;
            let mut raw_block_bytes = Vec::with_capacity(block.encoded_len());
            block.encode(&mut raw_block_bytes)?;
            let converted = match convert_grpc_block(&block, block_id) {
                Ok(converted) => converted,
                Err(err) => {
                    if let Some(writer) = failed_raw_blocks_writer.as_mut() {
                        writer.write_bytes(&raw_block_bytes)?;
                        writer.flush()?;
                    }
                    if let Some(writer) = failed_raw_blocks_journal_writer.as_mut() {
                        serde_json::to_writer(
                            &mut *writer,
                            &serde_json::json!({
                                "slot": block.slot,
                                "block_id": block_id,
                                "parent_slot": block.parent_slot,
                                "raw_len": raw_block_bytes.len(),
                                "error": err.to_string(),
                            }),
                        )?;
                        writer.write_all(b"\n")?;
                        writer.flush()?;
                    }
                    return Err(err).with_context(|| {
                        format!(
                            "convert gRPC block slot {} block_id {}",
                            block.slot, block_id
                        )
                    });
                }
            };
            encode_with_scratch(&converted.block, &mut block_scratch)
                .context("wincode encode normalized block")?;
            let block_len =
                u32::try_from(block_scratch.len()).context("normalized block exceeds u32::MAX")?;
            let tx_count = u32::try_from(converted.transaction_count)
                .context("transaction count exceeds u32::MAX")?;

            poh_writer.write(&WincodeArchiveV2PohRecord {
                block_id,
                slot: block.slot,
                entries: converted.poh_entries,
            })?;
            if let Some(writer) = raw_blocks_writer.as_mut() {
                writer.write_bytes(&raw_block_bytes)?;
            }
            block_writer.write_bytes(&block_scratch)?;
            block_index_writer.write(&SplitCompactIndexRecord {
                slot: block.slot,
                block_id,
                block_offset,
                block_len,
                runtime_offset: 0,
                runtime_len: 0,
                tx_count,
            })?;
            blockhash_writer.write_all(&converted.blockhash)?;
            index_pubkeys_from_block(
                &converted.block,
                pubkey_counts.as_mut(),
                pubkey_touches_writer.as_mut(),
                pubkey_run_writer.as_mut(),
                &mut pubkey_run_block_keys,
            )?;
            write_signatures(
                &converted.block,
                block_id,
                block.slot,
                &mut signature_ordinal,
                &mut signatures_writer,
                &mut signature_index_writer,
            )?;
            block_offset = block_offset
                .checked_add(frame_len_u64(block_len))
                .context("block offset overflow")?;

            let journal = GrpcCapturedBlockJournal {
                slot: block.slot,
                epoch: epoch_slot.epoch,
                epoch_slot_index: epoch_slot.slot_index,
                block_id,
                parent_slot: block.parent_slot,
                transactions: block.transactions.len(),
                entries: block.entries.len(),
                executed_transaction_count: block.executed_transaction_count,
                entries_count: block.entries_count,
                updated_account_count: block.updated_account_count,
                missing: converted.missing,
            };
            serde_json::to_writer(&mut journal_writer, &journal)?;
            journal_writer.write_all(b"\n")?;
            processing_duration += processing_started_at.elapsed();

            report.first_slot.get_or_insert(block.slot);
            report.last_slot = Some(block.slot);
            report.first_epoch.get_or_insert(epoch_slot.epoch);
            report.last_epoch = Some(epoch_slot.epoch);
            report.blocks_written += 1;
            report.entries_written += block.entries.len();
            report.transactions_written += block.transactions.len();
            let now = Instant::now();
            if last_progress_write
                .is_none_or(|last: Instant| now.duration_since(last) >= Duration::from_secs(3))
            {
                // Keep the append-only journal close to the status snapshot so a dashboard never
                // advertises progress that is still trapped in this process's userspace buffer.
                journal_writer.flush()?;
                let _ = write_grpc_capture_progress(
                    &progress_path,
                    &report,
                    next_block_id,
                    config.slots_per_epoch,
                    started_at,
                    "capturing",
                );
                last_progress_write = Some(now);
            }
        }

        block_writer.flush()?;
        if let Some(writer) = raw_blocks_writer.as_mut() {
            writer.flush()?;
        }
        poh_writer.flush()?;
        block_index_writer.flush()?;
        blockhash_writer.flush()?;
        signatures_writer.flush()?;
        signature_index_writer.flush()?;
        if let Some(writer) = pubkey_touches_writer.as_mut() {
            writer.flush()?;
        }
        if let Some(writer) = pubkey_run_writer.as_mut() {
            let run_report = writer.finish()?;
            report.pubkey_run_files = run_report.run_files;
            report.pubkey_run_records = run_report.run_records;
            report.pubkey_hot_keys = run_report.hot_keys;
            report.pubkey_hot_records = run_report.hot_records;
        }
        journal_writer.flush()?;
        if let Some(pubkey_counts) = pubkey_counts.as_ref() {
            write_pubkey_counts(&report.pubkey_counts_path, pubkey_counts)?;
        }
        Ok::<_, anyhow::Error>(())
    };

    timeout(deadline, read_blocks)
        .await
        .map_err(|_| anyhow!("timed out waiting for gRPC block updates"))??;

    let elapsed = started_at.elapsed();
    report.elapsed_ms = elapsed.as_millis();
    report.processing_ms = processing_duration.as_millis();
    report.block_bytes_written = file_len(&report.block_path)?.saturating_sub(block_bytes_before);
    report.raw_block_bytes_written =
        file_len(&report.raw_blocks_path)?.saturating_sub(raw_block_bytes_before);
    report.poh_bytes_written = file_len(&report.poh_path)?.saturating_sub(poh_bytes_before);
    report.block_index_bytes_written =
        file_len(&report.block_index_path)?.saturating_sub(block_index_bytes_before);
    report.blockhash_bytes_written =
        file_len(&report.blockhash_registry_path)?.saturating_sub(blockhash_bytes_before);
    report.signature_bytes_written =
        file_len(&report.signatures_path)?.saturating_sub(signature_bytes_before);
    report.signature_index_bytes_written =
        file_len(&report.signature_index_path)?.saturating_sub(signature_index_bytes_before);
    report.pubkey_touch_bytes_written =
        file_len(&report.pubkey_touches_path)?.saturating_sub(pubkey_touch_bytes_before);
    report.pubkey_run_bytes_written =
        dir_file_len(&report.pubkey_runs_dir)?.saturating_sub(pubkey_run_bytes_before);
    report.pubkey_count_records = pubkey_counts.as_ref().map_or(0, GxHashMap::len);
    let total_bytes = report.block_bytes_written
        + report.raw_block_bytes_written
        + report.poh_bytes_written
        + report.block_index_bytes_written
        + report.blockhash_bytes_written
        + report.signature_bytes_written
        + report.signature_index_bytes_written
        + report.pubkey_touch_bytes_written
        + report.pubkey_run_bytes_written;
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let processing_secs = processing_duration.as_secs_f64().max(0.001);
    report.mb_per_sec = (total_bytes as f64 / 1_048_576.0) / elapsed_secs;
    report.processing_mb_per_sec = (total_bytes as f64 / 1_048_576.0) / processing_secs;
    report.blocks_per_sec = report.blocks_written as f64 / elapsed_secs;
    report.processing_blocks_per_sec = report.blocks_written as f64 / processing_secs;

    let final_state = if report.stopped_at_epoch_boundary {
        "closed"
    } else {
        "stopped"
    };
    let _ = write_grpc_capture_progress(
        &progress_path,
        &report,
        next_block_id,
        config.slots_per_epoch,
        started_at,
        final_state,
    );

    Ok(report)
}

fn write_grpc_capture_progress(
    path: &Path,
    report: &GrpcCaptureReport,
    existing_blocks: u32,
    slots_per_epoch: u64,
    started_at: Instant,
    state: &str,
) -> Result<()> {
    let elapsed_secs = started_at.elapsed().as_secs_f64().max(0.001);
    let blocks_done = u64::from(existing_blocks)
        .checked_add(
            u64::try_from(report.blocks_written).context("capture progress block overflow")?,
        )
        .context("capture progress total block overflow")?;
    let epoch_slot_index = report.last_slot.map(|slot| slot % slots_per_epoch.max(1));
    let progress_pct = epoch_slot_index.map(|slot_index| {
        ((slot_index.saturating_add(1)) as f64 / slots_per_epoch.max(1) as f64 * 100.0).min(100.0)
    });
    let slots_per_sec = report
        .first_slot
        .zip(report.last_slot)
        .map(|(first, last)| last.saturating_sub(first) as f64 / elapsed_secs)
        .unwrap_or(0.0);
    let eta_secs = epoch_slot_index.and_then(|slot_index| {
        (slots_per_sec > 0.0).then(|| {
            slots_per_epoch.saturating_sub(slot_index.saturating_add(1)) as f64 / slots_per_sec
        })
    });
    let progress = GrpcCaptureProgress {
        schema_version: 1,
        pid: std::process::id(),
        phase: "Live gRPC capture".to_string(),
        state: state.to_string(),
        capture_dir: report.archive_dir.clone(),
        epoch: report.last_epoch.or(report.first_epoch),
        first_slot: report.first_slot,
        last_slot: report.last_slot,
        epoch_slot_index,
        blocks_done,
        transactions_done: u64::try_from(report.transactions_written)
            .context("capture progress transaction overflow")?,
        entries_done: u64::try_from(report.entries_written)
            .context("capture progress entry overflow")?,
        slots_total: slots_per_epoch,
        elapsed_secs,
        blocks_per_sec: report.blocks_written as f64 / elapsed_secs,
        progress_pct,
        eta_secs,
        stopped_at_epoch_boundary: report.stopped_at_epoch_boundary,
        updated_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs()),
    };
    let parent = path
        .parent()
        .context("capture progress path has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create capture progress dir {}", parent.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("progress.json");
    let temp_path = path.with_file_name(format!(".{name}.{}.tmp", std::process::id()));
    let json = serde_json::to_vec(&progress).context("serialize gRPC capture progress")?;
    std::fs::write(&temp_path, json)
        .with_context(|| format!("write capture progress temp {}", temp_path.display()))?;
    std::fs::rename(&temp_path, path).with_context(|| {
        format!(
            "publish capture progress {} -> {}",
            temp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn read_existing_capture_epoch(journal_path: &Path) -> Result<Option<u64>> {
    let file = match File::open(journal_path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| format!("open {}", journal_path.display()));
        }
    };
    for line in BufReader::new(file).lines() {
        let line = line.with_context(|| format!("read {}", journal_path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(row) = serde_json::from_str::<GrpcCapturedBlockJournal>(&line) {
            return Ok(Some(row.epoch));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod capture_epoch_tests {
    use super::*;

    #[test]
    fn resumed_capture_keeps_epoch_from_existing_journal() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "blockzilla-capture-epoch-{}-{unique}.jsonl",
            std::process::id()
        ));
        std::fs::write(
            &path,
            concat!(
                "not-json\n",
                "{\"slot\":432431999,\"epoch\":1000,\"epoch_slot_index\":431999,",
                "\"block_id\":7,\"parent_slot\":432431998,\"transactions\":1,",
                "\"entries\":1,\"executed_transaction_count\":1,\"entries_count\":1,",
                "\"updated_account_count\":0,\"missing\":[]}\n"
            ),
        )
        .unwrap();

        assert_eq!(read_existing_capture_epoch(&path).unwrap(), Some(1000));
        std::fs::remove_file(path).unwrap();
    }
}

fn observe_epoch_slot(
    slot: u64,
    source: GrpcEpochBoundarySource,
    slot_status: Option<String>,
    tracker: &mut EpochBoundaryTracker,
    report: &mut GrpcEpochWatchReport,
) {
    report.first_slot.get_or_insert(slot);
    report.last_slot = Some(report.last_slot.map_or(slot, |last| last.max(slot)));
    let events = tracker.observe_slot(slot);
    report.current_epoch = tracker.current_epoch();
    for event in events {
        report.boundaries.push(GrpcEpochBoundaryRecord {
            event,
            source,
            slot_status: slot_status.clone(),
        });
    }
}

pub fn inspect_capture(archive_dir: PathBuf) -> Result<CaptureInspectReport> {
    let layout = ProducerLayout::new(&archive_dir);
    let block_path = layout.blocks_dir.join("live-no-registry-blocks.bin");
    let raw_blocks_path = layout.blocks_dir.join("grpc-raw-blocks.bin");
    let poh_path = layout.poh_dir.join("poh.wincode");
    let block_index_path = layout.index_dir.join("block-index.bin");
    let signatures_path = layout.index_dir.join("signatures.bin");
    let signature_index_path = layout.index_dir.join("signature-index.bin");
    let pubkey_counts_path = layout.index_dir.join("pubkey-counts.bin");
    let pubkey_runs_dir = layout.index_dir.join(LIVE_PUBKEY_RUNS_DIR);

    let mut block_reader = WincodeLeb128FramedReader::new(BufReader::new(
        File::open(&block_path).with_context(|| format!("open {}", block_path.display()))?,
    ));
    let mut poh_reader = WincodeLeb128FramedReader::new(BufReader::new(
        File::open(&poh_path).with_context(|| format!("open {}", poh_path.display()))?,
    ));

    let mut report = CaptureInspectReport {
        archive_dir,
        block_frames: 0,
        raw_block_frames: 0,
        block_index_rows: 0,
        poh_frames: 0,
        signature_index_rows: 0,
        signatures: 0,
        pubkey_count_records: 0,
        pubkey_run_files: 0,
        pubkey_run_records: 0,
        pubkey_run_bytes: 0,
        transactions: 0,
        poh_entries: 0,
        first_slot: None,
        last_slot: None,
    };

    while let Some((_len, block)) = block_reader.read::<WincodeArchiveV2NoRegistryBlock>()? {
        report.block_frames += 1;
        report.transactions += block.txs.len();
        report.first_slot.get_or_insert(block.header.compact.slot);
        report.last_slot = Some(block.header.compact.slot);
    }

    if raw_blocks_path.exists() && file_len(&raw_blocks_path)? > 0 {
        report.raw_block_frames = count_raw_frames(&raw_blocks_path)?;
    }

    while let Some((_len, poh)) = poh_reader.read::<WincodeArchiveV2PohRecord>()? {
        report.poh_frames += 1;
        report.poh_entries += poh.entries.len();
    }

    if block_index_path.exists() && file_len(&block_index_path)? > 0 {
        let mut index_reader = WincodeLeb128FramedReader::new(BufReader::new(
            File::open(&block_index_path)
                .with_context(|| format!("open {}", block_index_path.display()))?,
        ));
        let _header = index_reader
            .read::<SplitCompactIndexHeader>()?
            .map(|(_len, header)| header)
            .context("block index missing header")?;
        while index_reader.read::<SplitCompactIndexRecord>()?.is_some() {
            report.block_index_rows += 1;
        }
    }
    report.signatures = (file_len(&signatures_path)? / 64) as usize;
    if signature_index_path.exists() && file_len(&signature_index_path)? > 0 {
        let mut signature_index_reader = WincodeLeb128FramedReader::new(BufReader::new(
            File::open(&signature_index_path)
                .with_context(|| format!("open {}", signature_index_path.display()))?,
        ));
        while signature_index_reader
            .read::<LiveSignatureIndexRecord>()?
            .is_some()
        {
            report.signature_index_rows += 1;
        }
    }
    if pubkey_counts_path.exists() && file_len(&pubkey_counts_path)? > 0 {
        let mut pubkey_count_reader = WincodeLeb128FramedReader::new(BufReader::new(
            File::open(&pubkey_counts_path)
                .with_context(|| format!("open {}", pubkey_counts_path.display()))?,
        ));
        while pubkey_count_reader
            .read::<LivePubkeyCountRecord>()?
            .is_some()
        {
            report.pubkey_count_records += 1;
        }
    }
    report.pubkey_run_files = count_pubkey_run_files(&pubkey_runs_dir)?;
    report.pubkey_run_records = count_pubkey_run_records(&pubkey_runs_dir)?;
    report.pubkey_run_bytes = dir_file_len(&pubkey_runs_dir)?;

    Ok(report)
}

pub fn backfill_pubkey_runs(
    config: GrpcPubkeyRunBackfillConfig,
) -> Result<GrpcPubkeyRunBackfillReport> {
    let layout = ProducerLayout::new(&config.archive_dir);
    let block_path = layout.blocks_dir.join("live-no-registry-blocks.bin");
    let block_index_path = layout.index_dir.join("block-index.bin");
    let output_run_dir = config
        .output_run_dir
        .unwrap_or_else(|| layout.index_dir.join(LIVE_PUBKEY_RUNS_DIR));

    if config.reset_output_dir && output_run_dir.exists() {
        fs::remove_dir_all(&output_run_dir)
            .with_context(|| format!("remove {}", output_run_dir.display()))?;
    }

    let snapshot_bytes = file_len(&block_path)?;
    let start_offset = if config.start_block_id == 0 {
        0
    } else {
        find_block_offset(&block_index_path, config.start_block_id)?.with_context(|| {
            format!(
                "block id {} not found in {}",
                config.start_block_id,
                block_index_path.display()
            )
        })?
    };
    anyhow::ensure!(
        start_offset <= snapshot_bytes,
        "start offset {} is past snapshot length {} for {}",
        start_offset,
        snapshot_bytes,
        block_path.display()
    );

    let run_bytes_before = dir_file_len(&output_run_dir)?;
    let mut run_writer = PubkeyRunWriter::open(
        &output_run_dir,
        config.pubkey_hot_registry_path.as_deref(),
        config.pubkey_hot_count,
    )?;

    let mut file =
        File::open(&block_path).with_context(|| format!("open {}", block_path.display()))?;
    file.seek(SeekFrom::Start(start_offset))
        .with_context(|| format!("seek {} to {}", block_path.display(), start_offset))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut remaining_bytes = snapshot_bytes.saturating_sub(start_offset);
    let mut frame_scratch = Vec::with_capacity(2 * 1024 * 1024);
    let mut pubkey_run_block_keys = Vec::new();
    let started_at = Instant::now();

    let mut report = GrpcPubkeyRunBackfillReport {
        archive_dir: layout.archive_dir,
        block_path,
        output_run_dir,
        start_block_id: config.start_block_id,
        blocks_scanned: 0,
        transactions_scanned: 0,
        first_slot: None,
        last_slot: None,
        input_start_offset: start_offset,
        input_snapshot_bytes: snapshot_bytes,
        input_bytes_read: 0,
        stopped_by_max_blocks: false,
        tail_truncated: false,
        elapsed_ms: 0,
        blocks_per_sec: 0.0,
        tx_per_sec: 0.0,
        pubkey_run_bytes_written: 0,
        pubkey_run_files: 0,
        pubkey_run_records: 0,
        pubkey_hot_keys: 0,
        pubkey_hot_records: 0,
    };

    loop {
        if config
            .max_blocks
            .is_some_and(|max_blocks| report.blocks_scanned >= max_blocks)
        {
            report.stopped_by_max_blocks = true;
            break;
        }

        let Some((frame_bytes, block)) =
            read_no_registry_block_frame(&mut reader, &mut remaining_bytes, &mut frame_scratch)?
        else {
            break;
        };

        report.input_bytes_read = report.input_bytes_read.saturating_add(frame_bytes);
        report.first_slot.get_or_insert(block.header.compact.slot);
        report.last_slot = Some(block.header.compact.slot);
        report.transactions_scanned = report.transactions_scanned.saturating_add(block.txs.len());
        report.blocks_scanned += 1;
        index_pubkeys_from_block(
            &block,
            None,
            None,
            Some(&mut run_writer),
            &mut pubkey_run_block_keys,
        )?;
    }

    let run_report = run_writer.finish()?;
    let elapsed = started_at.elapsed();
    report.tail_truncated = !report.stopped_by_max_blocks && remaining_bytes > 0;
    report.elapsed_ms = elapsed.as_millis();
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    report.blocks_per_sec = report.blocks_scanned as f64 / elapsed_secs;
    report.tx_per_sec = report.transactions_scanned as f64 / elapsed_secs;
    report.pubkey_run_bytes_written =
        dir_file_len(&report.output_run_dir)?.saturating_sub(run_bytes_before);
    report.pubkey_run_files = run_report.run_files;
    report.pubkey_run_records = run_report.run_records;
    report.pubkey_hot_keys = run_report.hot_keys;
    report.pubkey_hot_records = run_report.hot_records;

    Ok(report)
}

pub fn backfill_compact_logs(
    config: GrpcCompactLogsBackfillConfig,
) -> Result<GrpcCompactLogsBackfillReport> {
    anyhow::ensure!(
        config.archive_dir != config.output_archive_dir,
        "output archive dir must be different from input archive dir"
    );
    let input_archive_abs = fs::canonicalize(&config.archive_dir)
        .with_context(|| format!("canonicalize {}", config.archive_dir.display()))?;
    let output_archive_abs = absolute_output_path(&config.output_archive_dir)?;
    anyhow::ensure!(
        !output_archive_abs.starts_with(&input_archive_abs),
        "output archive dir must not be inside input archive dir: {} is under {}",
        output_archive_abs.display(),
        input_archive_abs.display()
    );
    if config.output_archive_dir.exists() && !config.append_output {
        anyhow::ensure!(
            config.overwrite_output,
            "output archive dir already exists: {}",
            config.output_archive_dir.display()
        );
        fs::remove_dir_all(&config.output_archive_dir)
            .with_context(|| format!("remove {}", config.output_archive_dir.display()))?;
    }

    let input_layout = ProducerLayout::new(&config.archive_dir);
    let mut sidecar_counts = CaptureSidecarCopyCounts::default();
    let output_layout = if config.output_archive_dir.exists() {
        ProducerLayout::new(&config.output_archive_dir)
    } else {
        let output_layout = ProducerLayout::create(&config.output_archive_dir)?;
        copy_capture_sidecars(
            &input_layout.archive_dir,
            &output_layout.archive_dir,
            &mut sidecar_counts,
        )?;
        output_layout
    };
    if !config.append_output && sidecar_counts.linked == 0 && sidecar_counts.copied == 0 {
        copy_capture_sidecars(
            &input_layout.archive_dir,
            &output_layout.archive_dir,
            &mut sidecar_counts,
        )?;
    }

    let input_block_path = input_layout.blocks_dir.join("live-no-registry-blocks.bin");
    let input_block_index_path = input_layout.index_dir.join("block-index.bin");
    let output_block_path = output_layout.blocks_dir.join("live-no-registry-blocks.bin");
    let output_block_index_path = output_layout.index_dir.join("block-index.bin");

    let input_block_bytes = file_len(&input_block_path)?;
    let input_start_offset = if config.start_block_id == 0 {
        0
    } else {
        find_block_offset(&input_block_index_path, config.start_block_id)?.with_context(|| {
            format!(
                "block id {} not found in {}",
                config.start_block_id,
                input_block_index_path.display()
            )
        })?
    };
    anyhow::ensure!(
        input_start_offset <= input_block_bytes,
        "start offset {} is past snapshot length {} for {}",
        input_start_offset,
        input_block_bytes,
        input_block_path.display()
    );

    let mut input_file = File::open(&input_block_path)
        .with_context(|| format!("open {}", input_block_path.display()))?;
    input_file
        .seek(SeekFrom::Start(input_start_offset))
        .with_context(|| {
            format!(
                "seek {} to {input_start_offset}",
                input_block_path.display()
            )
        })?;
    let mut input_reader = BufReader::with_capacity(1024 * 1024, &mut input_file);
    let mut remaining_bytes = input_block_bytes.saturating_sub(input_start_offset);

    let output_start_block_bytes = if config.append_output && output_block_path.exists() {
        file_len(&output_block_path)?
    } else {
        0
    };
    let output_start_block_index_rows = if config.append_output && output_block_index_path.exists()
    {
        count_existing_index_rows(&output_block_index_path)?
    } else {
        0
    };
    anyhow::ensure!(
        output_start_block_index_rows == config.start_block_id,
        "output index rows ({}) must equal start block id ({}) for append",
        output_start_block_index_rows,
        config.start_block_id
    );

    if let Some(parent) = output_block_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    if let Some(parent) = output_block_index_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let output_block_file = OpenOptions::new()
        .create(true)
        .append(config.append_output)
        .write(true)
        .truncate(!config.append_output)
        .open(&output_block_path)
        .with_context(|| format!("open {}", output_block_path.display()))?;
    let mut block_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(1024 * 1024, output_block_file));
    let output_index_file = OpenOptions::new()
        .create(true)
        .append(config.append_output)
        .write(true)
        .truncate(!config.append_output)
        .open(&output_block_index_path)
        .with_context(|| format!("open {}", output_block_index_path.display()))?;
    let mut block_index_writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(1024 * 1024, output_index_file));
    if output_start_block_index_rows == 0 {
        block_index_writer.write(&SplitCompactIndexHeader { version: 1 })?;
    }

    let started_at = Instant::now();
    let mut frame_scratch = Vec::with_capacity(2 * 1024 * 1024);
    let mut encode_scratch = Vec::with_capacity(2 * 1024 * 1024);
    let mut block_offset = output_start_block_bytes;
    let mut report = GrpcCompactLogsBackfillReport {
        archive_dir: input_layout.archive_dir,
        output_archive_dir: output_layout.archive_dir,
        input_block_path,
        output_block_path,
        output_block_index_path,
        start_block_id: config.start_block_id,
        append_output: config.append_output,
        blocks_rewritten: 0,
        transactions_scanned: 0,
        txs_with_logs: 0,
        raw_log_streams_compacted: 0,
        zstd_log_streams_compacted: 0,
        compact_log_streams_kept: 0,
        log_lines_compacted: 0,
        input_block_bytes,
        input_start_offset,
        input_bytes_read: 0,
        output_block_bytes: 0,
        output_start_block_bytes,
        output_start_block_index_rows,
        sidecar_files_linked: sidecar_counts.linked,
        sidecar_files_copied: sidecar_counts.copied,
        first_slot: None,
        last_slot: None,
        stopped_by_max_blocks: false,
        tail_truncated: false,
        elapsed_ms: 0,
        blocks_per_sec: 0.0,
        tx_per_sec: 0.0,
    };

    loop {
        if config
            .max_blocks
            .is_some_and(|max_blocks| report.blocks_rewritten >= max_blocks)
        {
            report.stopped_by_max_blocks = true;
            break;
        }

        let Some((frame_bytes, mut block)) = read_no_registry_block_frame(
            &mut input_reader,
            &mut remaining_bytes,
            &mut frame_scratch,
        )?
        else {
            break;
        };

        let slot = block.header.compact.slot;
        report.input_bytes_read = report.input_bytes_read.saturating_add(frame_bytes);
        report.first_slot.get_or_insert(slot);
        report.last_slot = Some(slot);
        report.transactions_scanned = report.transactions_scanned.saturating_add(block.txs.len());
        compact_block_logs(&mut block, &mut report)
            .with_context(|| format!("compact logs for slot {slot}"))?;

        encode_with_scratch(&block, &mut encode_scratch)
            .with_context(|| format!("encode compact-log block slot {slot}"))?;
        let block_len = u32::try_from(encode_scratch.len())
            .with_context(|| format!("slot {slot} compact-log block exceeds u32::MAX"))?;
        block_writer.write_bytes(&encode_scratch)?;
        let block_id = config
            .start_block_id
            .checked_add(
                u32::try_from(report.blocks_rewritten).context("rewritten blocks exceed u32")?,
            )
            .context("block id overflow")?;
        block_index_writer.write(&SplitCompactIndexRecord {
            slot,
            block_id,
            block_offset,
            block_len,
            runtime_offset: 0,
            runtime_len: 0,
            tx_count: u32::try_from(block.txs.len()).context("tx count exceeds u32::MAX")?,
        })?;
        block_offset = block_offset
            .checked_add(frame_len_u64(block_len))
            .context("block offset overflow")?;
        report.blocks_rewritten += 1;
    }

    block_writer.flush()?;
    block_index_writer.flush()?;
    report.tail_truncated = !report.stopped_by_max_blocks && remaining_bytes > 0;
    report.output_block_bytes = file_len(&report.output_block_path)?;
    let elapsed = started_at.elapsed();
    report.elapsed_ms = elapsed.as_millis();
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    report.blocks_per_sec = report.blocks_rewritten as f64 / elapsed_secs;
    report.tx_per_sec = report.transactions_scanned as f64 / elapsed_secs;

    Ok(report)
}

fn compact_block_logs(
    block: &mut WincodeArchiveV2NoRegistryBlock,
    report: &mut GrpcCompactLogsBackfillReport,
) -> Result<()> {
    for tx in &mut block.txs {
        let Some(WincodeArchiveV2Payload::Decoded { value: meta, .. }) = tx.metadata.as_mut()
        else {
            continue;
        };
        let Some(logs) = meta.logs.take() else {
            continue;
        };
        report.txs_with_logs += 1;
        meta.logs = Some(compact_no_registry_logs(logs, report)?);
    }
    Ok(())
}

fn compact_no_registry_logs(
    logs: WincodeArchiveV2NoRegistryLogs,
    report: &mut GrpcCompactLogsBackfillReport,
) -> Result<WincodeArchiveV2NoRegistryLogs> {
    match logs {
        WincodeArchiveV2NoRegistryLogs::Raw(lines) => {
            report.raw_log_streams_compacted += 1;
            report.log_lines_compacted = report
                .log_lines_compacted
                .saturating_add(u64::try_from(lines.len()).unwrap_or(u64::MAX));
            compact_live_logs(&lines)
        }
        WincodeArchiveV2NoRegistryLogs::WincodeZstd {
            uncompressed_len,
            bytes,
        } => {
            let decoded = zstd::stream::decode_all(bytes.as_slice())
                .context("decode live log zstd payload")?;
            anyhow::ensure!(
                decoded.len() as u64 == uncompressed_len,
                "live log zstd decoded length {} != expected {uncompressed_len}",
                decoded.len()
            );
            let lines: Vec<String> =
                wincode::config::deserialize(&decoded, wincode_leb128_config())
                    .context("decode live log string vector")?;
            report.zstd_log_streams_compacted += 1;
            report.log_lines_compacted = report
                .log_lines_compacted
                .saturating_add(u64::try_from(lines.len()).unwrap_or(u64::MAX));
            compact_live_logs(&lines)
        }
        WincodeArchiveV2NoRegistryLogs::Compact(logs) => {
            report.compact_log_streams_kept += 1;
            Ok(WincodeArchiveV2NoRegistryLogs::Compact(logs))
        }
    }
}

#[derive(Default)]
struct CaptureSidecarCopyCounts {
    linked: usize,
    copied: usize,
}

fn copy_capture_sidecars(
    input_root: &Path,
    output_root: &Path,
    counts: &mut CaptureSidecarCopyCounts,
) -> Result<()> {
    copy_capture_sidecars_inner(input_root, output_root, input_root, counts)
}

fn copy_capture_sidecars_inner(
    input_root: &Path,
    output_root: &Path,
    current: &Path,
    counts: &mut CaptureSidecarCopyCounts,
) -> Result<()> {
    for entry in fs::read_dir(current).with_context(|| format!("read {}", current.display()))? {
        let entry = entry?;
        let source = entry.path();
        let relative = source
            .strip_prefix(input_root)
            .with_context(|| format!("strip {} from {}", input_root.display(), source.display()))?;
        if should_skip_compact_log_backfill_sidecar(relative) {
            continue;
        }
        let target = output_root.join(relative);
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            fs::create_dir_all(&target).with_context(|| format!("create {}", target.display()))?;
            copy_capture_sidecars_inner(input_root, output_root, &source, counts)?;
        } else if metadata.is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            match fs::hard_link(&source, &target) {
                Ok(()) => counts.linked += 1,
                Err(_) => {
                    fs::copy(&source, &target).with_context(|| {
                        format!("copy {} to {}", source.display(), target.display())
                    })?;
                    counts.copied += 1;
                }
            }
        }
    }
    Ok(())
}

fn should_skip_compact_log_backfill_sidecar(relative: &Path) -> bool {
    relative == Path::new("producer-layout.json")
        || relative == Path::new("blocks").join("live-no-registry-blocks.bin")
        || relative == Path::new("index").join("block-index.bin")
}

fn absolute_output_path(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return fs::canonicalize(path).with_context(|| format!("canonicalize {}", path.display()));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("canonicalize output parent {}", parent.display()))?;
    Ok(parent.join(
        path.file_name()
            .ok_or_else(|| anyhow!("output path has no final component: {}", path.display()))?,
    ))
}

pub(crate) async fn connect_grpc(
    endpoint: &str,
) -> Result<yellowstone_grpc_client::GeyserGrpcClient> {
    connect_grpc_with_max_decoding_message_size(endpoint, 128 * 1024 * 1024).await
}

pub(crate) async fn connect_grpc_with_max_decoding_message_size(
    endpoint: &str,
    max_decoding_message_size: u64,
) -> Result<yellowstone_grpc_client::GeyserGrpcClient> {
    let transport = GrpcTransportOptions::from_env()?;
    let token = grpc_x_token()?;
    let max_decoding_message_size = usize::try_from(max_decoding_message_size)
        .context("gRPC maximum decoding message size exceeds usize")?;
    if max_decoding_message_size == 0 {
        return Err(anyhow!(
            "gRPC maximum decoding message size must be non-zero"
        ));
    }
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(Some(token))?
        .max_decoding_message_size(max_decoding_message_size);
    if let Some(encoding) = transport.accept_compression {
        builder = builder.accept_compressed(encoding);
    }
    if let Some(enabled) = transport.http2_adaptive_window {
        builder = builder.http2_adaptive_window(enabled);
    }
    if let Some(interval) = transport.http2_keep_alive_interval {
        builder = builder.http2_keep_alive_interval(interval);
    }
    if let Some(timeout) = transport.http2_keep_alive_timeout {
        builder = builder.keep_alive_timeout(timeout);
    }
    if let Some(enabled) = transport.http2_keep_alive_while_idle {
        builder = builder.keep_alive_while_idle(enabled);
    }
    if let Some(local_address) = transport.local_address {
        builder.endpoint = builder.endpoint.local_address(Some(local_address));
    }
    if endpoint.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(builder.connect().await?)
}

fn grpc_x_token() -> Result<String> {
    let token = match env::var("BLOCKZILLA_GRPC_X_TOKEN") {
        Ok(token) => token,
        Err(env::VarError::NotPresent) => {
            let path = env::var("BLOCKZILLA_GRPC_X_TOKEN_FILE").context(
                "missing BLOCKZILLA_GRPC_X_TOKEN or BLOCKZILLA_GRPC_X_TOKEN_FILE environment variable",
            )?;
            let metadata =
                fs::metadata(&path).with_context(|| format!("inspect gRPC x-token file {path}"))?;
            if !metadata.is_file() {
                return Err(anyhow!("gRPC x-token path is not a regular file: {path}"));
            }
            fs::read_to_string(&path)
                .with_context(|| format!("read gRPC x-token file {path}"))?
                .trim_end_matches(['\r', '\n'])
                .to_owned()
        }
        Err(err) => return Err(err).context("read BLOCKZILLA_GRPC_X_TOKEN environment variable"),
    };
    if token.is_empty() {
        return Err(anyhow!("gRPC x-token is empty"));
    }
    if token.contains(['\r', '\n']) {
        return Err(anyhow!("gRPC x-token contains an embedded newline"));
    }
    Ok(token)
}

const GRPC_ACCEPT_COMPRESSION_ENV: &str = "BLOCKZILLA_GRPC_ACCEPT_COMPRESSION";
const GRPC_HTTP2_ADAPTIVE_WINDOW_ENV: &str = "BLOCKZILLA_GRPC_HTTP2_ADAPTIVE_WINDOW";
const GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS_ENV: &str =
    "BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS";
const GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS_ENV: &str =
    "BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS";
const GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE_ENV: &str = "BLOCKZILLA_GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE";
const GRPC_LOCAL_ADDRESS_ENV: &str = "BLOCKZILLA_GRPC_LOCAL_ADDRESS";

#[derive(Debug, Clone, Copy)]
struct GrpcTransportOptions {
    accept_compression: Option<CompressionEncoding>,
    http2_adaptive_window: Option<bool>,
    http2_keep_alive_interval: Option<Duration>,
    http2_keep_alive_timeout: Option<Duration>,
    http2_keep_alive_while_idle: Option<bool>,
    local_address: Option<IpAddr>,
}

impl GrpcTransportOptions {
    fn from_env() -> Result<Self> {
        let accept_compression = optional_env(GRPC_ACCEPT_COMPRESSION_ENV)?;
        let http2_adaptive_window = optional_env(GRPC_HTTP2_ADAPTIVE_WINDOW_ENV)?;
        let http2_keep_alive_interval = optional_env(GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS_ENV)?;
        let http2_keep_alive_timeout = optional_env(GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS_ENV)?;
        let http2_keep_alive_while_idle = optional_env(GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE_ENV)?;
        let local_address = optional_env(GRPC_LOCAL_ADDRESS_ENV)?;

        Ok(Self {
            accept_compression: parse_accept_compression(accept_compression.as_deref())?,
            http2_adaptive_window: parse_optional_bool(
                GRPC_HTTP2_ADAPTIVE_WINDOW_ENV,
                http2_adaptive_window.as_deref(),
            )?,
            http2_keep_alive_interval: parse_optional_positive_duration_secs(
                GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS_ENV,
                http2_keep_alive_interval.as_deref(),
            )?,
            http2_keep_alive_timeout: parse_optional_positive_duration_secs(
                GRPC_HTTP2_KEEP_ALIVE_TIMEOUT_SECS_ENV,
                http2_keep_alive_timeout.as_deref(),
            )?,
            http2_keep_alive_while_idle: parse_optional_bool(
                GRPC_HTTP2_KEEP_ALIVE_WHILE_IDLE_ENV,
                http2_keep_alive_while_idle.as_deref(),
            )?,
            local_address: parse_optional_ip_address(local_address.as_deref())?,
        })
    }
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(anyhow!("{name} must contain valid Unicode")),
    }
}

fn parse_accept_compression(value: Option<&str>) -> Result<Option<CompressionEncoding>> {
    match value {
        None | Some("none") => Ok(None),
        Some("gzip") => Ok(Some(CompressionEncoding::Gzip)),
        Some("zstd") => Ok(Some(CompressionEncoding::Zstd)),
        Some(value) => Err(anyhow!(
            "{GRPC_ACCEPT_COMPRESSION_ENV} must be one of none, gzip, or zstd; got {value:?}"
        )),
    }
}

fn parse_optional_bool(name: &str, value: Option<&str>) -> Result<Option<bool>> {
    match value {
        None => Ok(None),
        Some("true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some(value) => Err(anyhow!(
            "{name} must be either true or false; got {value:?}"
        )),
    }
}

fn parse_optional_positive_duration_secs(
    name: &str,
    value: Option<&str>,
) -> Result<Option<Duration>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let seconds = value.parse::<u64>().with_context(|| {
        format!("{name} must be a positive integer number of seconds; got {value:?}")
    })?;
    if seconds == 0 {
        return Err(anyhow!(
            "{name} must be a positive integer number of seconds; got {value:?}"
        ));
    }
    Ok(Some(Duration::from_secs(seconds)))
}

fn parse_optional_ip_address(value: Option<&str>) -> Result<Option<IpAddr>> {
    value
        .map(|value| {
            value.parse::<IpAddr>().with_context(|| {
                format!(
                    "{GRPC_LOCAL_ADDRESS_ENV} must be an IPv4 or IPv6 address without a port; got {value:?}"
                )
            })
        })
        .transpose()
}

#[cfg(test)]
mod grpc_transport_tests {
    use super::*;

    #[test]
    fn response_compression_is_strict_and_defaults_to_none() {
        assert_eq!(parse_accept_compression(None).unwrap(), None);
        assert_eq!(parse_accept_compression(Some("none")).unwrap(), None);
        assert_eq!(
            parse_accept_compression(Some("gzip")).unwrap(),
            Some(CompressionEncoding::Gzip)
        );
        assert_eq!(
            parse_accept_compression(Some("zstd")).unwrap(),
            Some(CompressionEncoding::Zstd)
        );
        let error = parse_accept_compression(Some("GZIP")).unwrap_err();
        assert!(error.to_string().contains(GRPC_ACCEPT_COMPRESSION_ENV));
    }

    #[test]
    fn optional_boole_are_strict() {
        assert_eq!(
            parse_optional_bool(GRPC_HTTP2_ADAPTIVE_WINDOW_ENV, None).unwrap(),
            None
        );
        assert_eq!(
            parse_optional_bool(GRPC_HTTP2_ADAPTIVE_WINDOW_ENV, Some("true")).unwrap(),
            Some(true)
        );
        assert_eq!(
            parse_optional_bool(GRPC_HTTP2_ADAPTIVE_WINDOW_ENV, Some("false")).unwrap(),
            Some(false)
        );
        assert!(parse_optional_bool(GRPC_HTTP2_ADAPTIVE_WINDOW_ENV, Some("1")).is_err());
    }

    #[test]
    fn keep_alive_durations_must_be_positive_integer_seconds() {
        assert_eq!(
            parse_optional_positive_duration_secs(
                GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS_ENV,
                Some("30")
            )
            .unwrap(),
            Some(Duration::from_secs(30))
        );
        assert!(
            parse_optional_positive_duration_secs(
                GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS_ENV,
                Some("0")
            )
            .is_err()
        );
        assert!(
            parse_optional_positive_duration_secs(
                GRPC_HTTP2_KEEP_ALIVE_INTERVAL_SECS_ENV,
                Some("1.5")
            )
            .is_err()
        );
    }

    #[test]
    fn local_address_accepts_only_bare_ip_addresses() {
        assert_eq!(parse_optional_ip_address(None).unwrap(), None);
        assert_eq!(
            parse_optional_ip_address(Some("192.168.0.25")).unwrap(),
            Some("192.168.0.25".parse::<IpAddr>().unwrap())
        );
        assert_eq!(
            parse_optional_ip_address(Some("2001:db8::25")).unwrap(),
            Some("2001:db8::25".parse::<IpAddr>().unwrap())
        );
        assert!(parse_optional_ip_address(Some("192.168.0.25:443")).is_err());
        assert!(parse_optional_ip_address(Some("en0")).is_err());
    }
}

fn file_len(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err).with_context(|| format!("stat {}", path.display())),
    }
}

fn count_existing_index_rows(path: &Path) -> Result<u32> {
    if file_len(path)? == 0 {
        return Ok(0);
    }

    let mut reader = WincodeLeb128FramedReader::new(BufReader::new(
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    ));
    let _header = reader
        .read::<SplitCompactIndexHeader>()?
        .map(|(_len, header)| header)
        .context("block index missing header")?;
    let mut rows = 0u32;
    while reader.read::<SplitCompactIndexRecord>()?.is_some() {
        rows = rows.checked_add(1).context("block index row overflow")?;
    }
    Ok(rows)
}

fn find_block_offset(path: &Path, block_id: u32) -> Result<Option<u64>> {
    if file_len(path)? == 0 {
        return Ok(None);
    }

    let mut reader = WincodeLeb128FramedReader::new(BufReader::new(
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    ));
    let _header = reader
        .read::<SplitCompactIndexHeader>()?
        .map(|(_len, header)| header)
        .context("block index missing header")?;
    while let Some((_len, record)) = reader.read::<SplitCompactIndexRecord>()? {
        if record.block_id == block_id {
            return Ok(Some(record.block_offset));
        }
        if record.block_id > block_id {
            return Ok(None);
        }
    }
    Ok(None)
}

fn read_no_registry_block_frame<R: Read>(
    reader: &mut R,
    remaining_bytes: &mut u64,
    scratch: &mut Vec<u8>,
) -> Result<Option<(u64, WincodeArchiveV2NoRegistryBlock)>> {
    let Some((len, len_bytes)) = read_u32_varint_limited(reader, remaining_bytes)? else {
        return Ok(None);
    };
    let len = len as usize;
    if len as u64 > *remaining_bytes {
        return Ok(None);
    }

    scratch.resize(len, 0);
    if let Err(err) = reader.read_exact(scratch) {
        if err.kind() == ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(err).context("read no-registry block frame");
    }
    *remaining_bytes -= len as u64;
    let block = wincode::config::deserialize(scratch, wincode_leb128_config())
        .context("decode no-registry block frame")?;
    Ok(Some((u64::from(len_bytes) + len as u64, block)))
}

fn read_u32_varint_limited<R: Read>(
    reader: &mut R,
    remaining_bytes: &mut u64,
) -> Result<Option<(u32, u8)>> {
    if *remaining_bytes == 0 {
        return Ok(None);
    }

    let mut x = 0u32;
    let mut shift = 0;
    let mut bytes_read = 0u8;
    loop {
        if *remaining_bytes == 0 {
            return Ok(None);
        }
        let mut b = [0u8; 1];
        let n = reader.read(&mut b)?;
        if n == 0 {
            return Ok(None);
        }
        *remaining_bytes -= 1;
        bytes_read += 1;
        let byte = b[0];
        x |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(Some((x, bytes_read)));
        }
        shift += 7;
        anyhow::ensure!(shift <= 28, "varint overflow");
    }
}

fn count_raw_frames(path: &Path) -> Result<usize> {
    let mut reader =
        BufReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?);
    let mut frames = 0usize;
    loop {
        let Some(len) = read_u32_varint(&mut reader)
            .with_context(|| format!("read frame len from {}", path.display()))?
        else {
            break;
        };
        let len = len as usize;
        let mut remaining = len;
        let mut buf = [0u8; 8192];
        while remaining > 0 {
            let chunk = remaining.min(buf.len());
            reader
                .read_exact(&mut buf[..chunk])
                .with_context(|| format!("read frame payload from {}", path.display()))?;
            remaining -= chunk;
        }
        frames += 1;
    }
    Ok(frames)
}

fn frame_len_u64(payload_len: u32) -> u64 {
    u64::from(payload_len) + u64::from(u32_varint_len(payload_len))
}

fn u32_varint_len(mut value: u32) -> u8 {
    let mut len = 1u8;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn read_pubkey_counts(path: &Path) -> Result<LivePubkeyCounts> {
    let mut counts = LivePubkeyCounts::with_hasher(GxBuildHasher::default());
    if file_len(path)? == 0 {
        return Ok(counts);
    }
    let mut reader = WincodeLeb128FramedReader::new(BufReader::new(
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    ));
    while let Some((_len, record)) = reader.read::<LivePubkeyCountRecord>()? {
        let count = u32::try_from(record.count).unwrap_or(u32::MAX);
        counts.insert(record.pubkey, count);
    }
    Ok(counts)
}

fn write_pubkey_counts(path: &Path, counts: &LivePubkeyCounts) -> Result<()> {
    let tmp_path = path.with_extension("bin.tmp");
    let file = File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut writer = WincodeLeb128FramedWriter::new(BufWriter::new(file));
    let mut records = counts
        .iter()
        .map(|(pubkey, count)| (*pubkey, *count))
        .collect::<Vec<_>>();
    records.sort_unstable_by(|(left_key, left_count), (right_key, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_key.cmp(right_key))
    });
    for (pubkey, count) in records {
        writer.write(&LivePubkeyCountRecord {
            pubkey,
            count: u64::from(count),
        })?;
    }
    writer.flush()?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))
}

const PUBKEY_RUN_CHUNK_RECORDS: usize = 1_000_000;

#[derive(Clone, Copy)]
struct PubkeyRunRecord {
    pubkey: [u8; 32],
    count: u32,
}

#[derive(Debug, Clone, Copy, Default)]
struct PubkeyRunWriterReport {
    run_files: usize,
    run_records: u64,
    hot_keys: usize,
    hot_records: usize,
}

struct PubkeyRunWriter {
    dir: PathBuf,
    chunk: Vec<PubkeyRunRecord>,
    hot_counts: GxHashMap<[u8; 32], u32>,
    next_run_index: usize,
}

impl PubkeyRunWriter {
    fn open(dir: &Path, hot_registry_path: Option<&Path>, hot_count: usize) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let mut hot_counts = if let Some(path) = hot_registry_path {
            load_hot_pubkeys(path, hot_count)?
        } else {
            GxHashMap::with_hasher(GxBuildHasher::default())
        };
        let hot_run_path = dir.join(LIVE_PUBKEY_RUN_HOT_FILE);
        if hot_run_path.exists() {
            read_pubkey_run_records(&hot_run_path, |record| {
                hot_counts
                    .entry(record.pubkey)
                    .and_modify(|count| *count = count.saturating_add(record.count))
                    .or_insert(record.count);
                Ok(())
            })?;
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            chunk: Vec::with_capacity(PUBKEY_RUN_CHUNK_RECORDS),
            hot_counts,
            next_run_index: next_pubkey_run_index(dir)?,
        })
    }

    fn push_block_keys(&mut self, keys: &mut Vec<[u8; 32]>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        keys.sort_unstable();
        let mut index = 0usize;
        while index < keys.len() {
            let pubkey = keys[index];
            let mut count = 1u32;
            index += 1;
            while index < keys.len() && keys[index] == pubkey {
                count = count.saturating_add(1);
                index += 1;
            }

            if let Some(hot_count) = self.hot_counts.get_mut(&pubkey) {
                *hot_count = hot_count.saturating_add(count);
            } else {
                self.chunk.push(PubkeyRunRecord { pubkey, count });
            }
        }
        keys.clear();

        if self.chunk.len() >= PUBKEY_RUN_CHUNK_RECORDS {
            self.spill_chunk()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<PubkeyRunWriterReport> {
        self.spill_chunk()?;
        let hot_records = self.write_hot_run()?;
        Ok(PubkeyRunWriterReport {
            run_files: count_pubkey_run_files(&self.dir)?,
            run_records: count_pubkey_run_records(&self.dir)?,
            hot_keys: self.hot_counts.len(),
            hot_records,
        })
    }

    fn spill_chunk(&mut self) -> Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        self.chunk
            .sort_unstable_by(|left, right| left.pubkey.cmp(&right.pubkey));
        let path = self.dir.join(format!("run-{:06}.bin", self.next_run_index));
        self.next_run_index += 1;
        write_collapsed_pubkey_run(&path, &self.chunk)
            .with_context(|| format!("write pubkey run {}", path.display()))?;
        self.chunk.clear();
        Ok(())
    }

    fn write_hot_run(&self) -> Result<usize> {
        let mut records = self
            .hot_counts
            .iter()
            .filter_map(|(pubkey, count)| {
                (*count > 0).then_some(PubkeyRunRecord {
                    pubkey: *pubkey,
                    count: *count,
                })
            })
            .collect::<Vec<_>>();
        records.sort_unstable_by(|left, right| left.pubkey.cmp(&right.pubkey));
        let path = self.dir.join(LIVE_PUBKEY_RUN_HOT_FILE);
        write_pubkey_run_records(&path, &records)
            .with_context(|| format!("write hot pubkey run {}", path.display()))?;
        Ok(records.len())
    }
}

fn load_hot_pubkeys(path: &Path, limit: usize) -> Result<GxHashMap<[u8; 32], u32>> {
    let bytes = file_len(path)?;
    anyhow::ensure!(
        bytes % 32 == 0,
        "hot registry {} has non-key-aligned length {}",
        path.display(),
        bytes
    );
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut out = GxHashMap::with_capacity_and_hasher(limit, GxBuildHasher::default());
    for index in 0..limit {
        let mut pubkey = [0u8; 32];
        match reader.read_exact(&mut pubkey) {
            Ok(()) => {
                out.entry(pubkey).or_insert(0);
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                if index == 0 {
                    anyhow::ensure!(
                        file_len(path)? == 0,
                        "hot registry {} is shorter than one pubkey",
                        path.display()
                    );
                }
                break;
            }
            Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
        }
    }
    Ok(out)
}

fn next_pubkey_run_index(dir: &Path) -> Result<usize> {
    let mut next = 0usize;
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(stem) = name
            .strip_prefix("run-")
            .and_then(|rest| rest.strip_suffix(".bin"))
        else {
            continue;
        };
        if let Ok(index) = stem.parse::<usize>() {
            next = next.max(index.saturating_add(1));
        }
    }
    Ok(next)
}

fn write_collapsed_pubkey_run(path: &Path, sorted_records: &[PubkeyRunRecord]) -> Result<usize> {
    let tmp_path = path.with_extension("bin.tmp");
    let file = File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    let mut written = 0usize;
    let mut index = 0usize;
    while index < sorted_records.len() {
        let pubkey = sorted_records[index].pubkey;
        let mut count = sorted_records[index].count;
        index += 1;
        while index < sorted_records.len() && sorted_records[index].pubkey == pubkey {
            count = count.saturating_add(sorted_records[index].count);
            index += 1;
        }
        write_pubkey_run_record(&mut writer, PubkeyRunRecord { pubkey, count })?;
        written += 1;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(written)
}

fn write_pubkey_run_records(path: &Path, records: &[PubkeyRunRecord]) -> Result<()> {
    let tmp_path = path.with_extension("bin.tmp");
    let file = File::create(&tmp_path).with_context(|| format!("create {}", tmp_path.display()))?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    for record in records {
        write_pubkey_run_record(&mut writer, *record)?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))?;
    Ok(())
}

fn write_pubkey_run_record(writer: &mut impl Write, record: PubkeyRunRecord) -> Result<()> {
    writer.write_all(&record.pubkey)?;
    writer.write_all(&record.count.to_le_bytes())?;
    Ok(())
}

fn read_pubkey_run_records(
    path: &Path,
    mut visit: impl FnMut(PubkeyRunRecord) -> Result<()>,
) -> Result<()> {
    let bytes = file_len(path)?;
    anyhow::ensure!(
        bytes % LIVE_PUBKEY_RUN_RECORD_LEN as u64 == 0,
        "pubkey run {} has non-record-aligned length {}",
        path.display(),
        bytes
    );
    let mut reader = BufReader::with_capacity(
        1024 * 1024,
        File::open(path).with_context(|| format!("open {}", path.display()))?,
    );
    loop {
        let mut pubkey = [0u8; 32];
        match reader.read_exact(&mut pubkey) {
            Ok(()) => {
                let mut count = [0u8; 4];
                reader.read_exact(&mut count)?;
                visit(PubkeyRunRecord {
                    pubkey,
                    count: u32::from_le_bytes(count),
                })?;
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
        }
    }
    Ok(())
}

fn count_pubkey_run_files(dir: &Path) -> Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut files = 0usize;
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        if is_pubkey_run_file_name(&entry.file_name().to_string_lossy()) {
            files += 1;
        }
    }
    Ok(files)
}

fn count_pubkey_run_records(dir: &Path) -> Result<u64> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut records = 0u64;
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        if is_pubkey_run_file_name(&entry.file_name().to_string_lossy()) {
            let bytes = entry.metadata()?.len();
            anyhow::ensure!(
                bytes % LIVE_PUBKEY_RUN_RECORD_LEN as u64 == 0,
                "pubkey run {} has non-record-aligned length {}",
                entry.path().display(),
                bytes
            );
            records = records.saturating_add(bytes / LIVE_PUBKEY_RUN_RECORD_LEN as u64);
        }
    }
    Ok(records)
}

fn is_pubkey_run_file_name(name: &str) -> bool {
    name == LIVE_PUBKEY_RUN_HOT_FILE || (name.starts_with("run-") && name.ends_with(".bin"))
}

fn dir_file_len(dir: &Path) -> Result<u64> {
    if !dir.exists() {
        return Ok(0);
    }
    let mut total = 0u64;
    for entry in fs::read_dir(dir).with_context(|| format!("read {}", dir.display()))? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

pub(crate) fn write_signatures(
    block: &WincodeArchiveV2NoRegistryBlock,
    block_id: u32,
    slot: u64,
    signature_ordinal: &mut u64,
    signatures_writer: &mut impl Write,
    signature_index_writer: &mut WincodeLeb128FramedWriter<impl Write>,
) -> Result<()> {
    for tx in &block.txs {
        let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx else {
            continue;
        };
        for (signature_index, signature) in value.signatures.iter().enumerate() {
            signatures_writer.write_all(&bytes64(signature)?)?;
            signature_index_writer.write(&LiveSignatureIndexRecord {
                signature_ordinal: *signature_ordinal,
                slot,
                block_id,
                tx_index: tx.tx_index,
                signature_index: u8::try_from(signature_index)
                    .context("signature index exceeds u8::MAX")?,
            })?;
            *signature_ordinal = signature_ordinal
                .checked_add(1)
                .context("signature ordinal overflow")?;
        }
    }
    Ok(())
}

fn index_pubkeys_from_block(
    block: &WincodeArchiveV2NoRegistryBlock,
    mut counts: Option<&mut LivePubkeyCounts>,
    mut touches: Option<&mut BufWriter<File>>,
    mut runs: Option<&mut PubkeyRunWriter>,
    run_block_keys: &mut Vec<[u8; 32]>,
) -> Result<()> {
    if runs.is_some() {
        run_block_keys.clear();
    }
    visit_pubkeys_from_block(block, |pubkey| {
        if let Some(counts) = counts.as_deref_mut() {
            increment_pubkey(counts, pubkey);
        }
        if let Some(writer) = touches.as_deref_mut() {
            writer.write_all(&pubkey)?;
        }
        if runs.is_some() {
            run_block_keys.push(pubkey);
        }
        Ok::<_, anyhow::Error>(())
    })?;
    if let Some(writer) = runs.as_deref_mut() {
        writer.push_block_keys(run_block_keys)?;
    }
    Ok(())
}

pub(crate) fn visit_pubkeys_from_block(
    block: &WincodeArchiveV2NoRegistryBlock,
    mut visit: impl FnMut([u8; 32]) -> Result<()>,
) -> Result<()> {
    if let Some(rewards) = block.header.rewards.as_ref()
        && let Some(decoded) = rewards.decoded.as_ref()
    {
        for reward in decoded {
            visit(reward.pubkey)?;
        }
    }

    for tx in &block.txs {
        if let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx {
            match &value.message {
                WincodeArchiveV2NoRegistryMessage::Legacy(message) => {
                    for pubkey in &message.account_keys {
                        visit(*pubkey)?;
                    }
                }
                WincodeArchiveV2NoRegistryMessage::V0(message) => {
                    for pubkey in &message.account_keys {
                        visit(*pubkey)?;
                    }
                    for lookup in &message.address_table_lookups {
                        visit(lookup.account_key)?;
                    }
                }
            }
        }
        if let Some(WincodeArchiveV2Payload::Decoded { value, .. }) = tx.metadata.as_ref() {
            for pubkey in &value.loaded_writable_addresses {
                visit(*pubkey)?;
            }
            for pubkey in &value.loaded_readonly_addresses {
                visit(*pubkey)?;
            }
            for balance in value
                .pre_token_balances
                .iter()
                .chain(value.post_token_balances.iter())
            {
                for pubkey in [balance.mint, balance.owner, balance.program_id]
                    .into_iter()
                    .flatten()
                {
                    visit(pubkey)?;
                }
            }
            for reward in &value.rewards {
                visit(reward.pubkey)?;
            }
            if let Some(return_data) = value.return_data.as_ref() {
                visit(return_data.program_id)?;
            }
        }
    }
    Ok(())
}

fn increment_pubkey(counts: &mut LivePubkeyCounts, pubkey: [u8; 32]) {
    let entry = counts.entry(pubkey).or_insert(0);
    *entry = entry.saturating_add(1);
}

pub(crate) struct ConvertedGrpcBlock {
    pub(crate) block: WincodeArchiveV2NoRegistryBlock,
    pub(crate) poh_entries: Vec<CompactPohEntry>,
    pub(crate) blockhash: [u8; 32],
    pub(crate) transaction_count: usize,
    pub(crate) missing: Vec<LiveBlockMissingField>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GrpcConvertTimingReport {
    pub blockhash_ms: u128,
    pub poh_entries_ms: u128,
    pub transactions_total_ms: u128,
    pub tx_signatures_ms: u128,
    pub tx_message_ms: u128,
    pub tx_metadata_ms: u128,
    pub message_account_keys_ms: u128,
    pub message_instructions_ms: u128,
    pub message_address_table_lookups_ms: u128,
    pub meta_error_ms: u128,
    pub meta_balances_ms: u128,
    pub meta_inner_instructions_ms: u128,
    pub meta_logs_ms: u128,
    pub meta_token_balances_ms: u128,
    pub meta_rewards_ms: u128,
    pub meta_loaded_addresses_ms: u128,
    pub meta_return_data_ms: u128,
    pub block_rewards_ms: u128,
    pub block_finish_ms: u128,
    pub token_pubkey_cache_hits: u64,
    pub token_pubkey_cache_misses: u64,
    pub token_pubkey_cache_max_entries: usize,
    pub meta_log_parse_stats: CompactLogParseStatsReport,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct GrpcConvertTimings {
    blockhash: Duration,
    poh_entries: Duration,
    transactions_total: Duration,
    tx_signatures: Duration,
    tx_message: Duration,
    tx_metadata: Duration,
    message_account_keys: Duration,
    message_instructions: Duration,
    message_address_table_lookups: Duration,
    meta_error: Duration,
    meta_balances: Duration,
    meta_inner_instructions: Duration,
    meta_logs: Duration,
    meta_token_balances: Duration,
    meta_rewards: Duration,
    meta_loaded_addresses: Duration,
    meta_return_data: Duration,
    block_rewards: Duration,
    block_finish: Duration,
    token_pubkey_cache_hits: u64,
    token_pubkey_cache_misses: u64,
    token_pubkey_cache_max_entries: usize,
    meta_log_parse_stats: CompactLogParseStats,
    collect_log_parse_stats: bool,
}

impl GrpcConvertTimings {
    pub(crate) fn set_collect_log_parse_stats(&mut self, enabled: bool) {
        self.collect_log_parse_stats = enabled;
    }

    pub(crate) fn report(&self) -> GrpcConvertTimingReport {
        GrpcConvertTimingReport {
            blockhash_ms: self.blockhash.as_millis(),
            poh_entries_ms: self.poh_entries.as_millis(),
            transactions_total_ms: self.transactions_total.as_millis(),
            tx_signatures_ms: self.tx_signatures.as_millis(),
            tx_message_ms: self.tx_message.as_millis(),
            tx_metadata_ms: self.tx_metadata.as_millis(),
            message_account_keys_ms: self.message_account_keys.as_millis(),
            message_instructions_ms: self.message_instructions.as_millis(),
            message_address_table_lookups_ms: self.message_address_table_lookups.as_millis(),
            meta_error_ms: self.meta_error.as_millis(),
            meta_balances_ms: self.meta_balances.as_millis(),
            meta_inner_instructions_ms: self.meta_inner_instructions.as_millis(),
            meta_logs_ms: self.meta_logs.as_millis(),
            meta_token_balances_ms: self.meta_token_balances.as_millis(),
            meta_rewards_ms: self.meta_rewards.as_millis(),
            meta_loaded_addresses_ms: self.meta_loaded_addresses.as_millis(),
            meta_return_data_ms: self.meta_return_data.as_millis(),
            block_rewards_ms: self.block_rewards.as_millis(),
            block_finish_ms: self.block_finish.as_millis(),
            token_pubkey_cache_hits: self.token_pubkey_cache_hits,
            token_pubkey_cache_misses: self.token_pubkey_cache_misses,
            token_pubkey_cache_max_entries: self.token_pubkey_cache_max_entries,
            meta_log_parse_stats: self.meta_log_parse_stats.report(24),
        }
    }
}

#[derive(Default)]
struct PubkeyStringDecodeCache {
    enabled: bool,
    values: GxHashMap<String, [u8; 32]>,
    hits: u64,
    misses: u64,
    max_entries: usize,
}

impl PubkeyStringDecodeCache {
    fn new(enabled: bool) -> Self {
        Self {
            enabled,
            values: GxHashMap::with_hasher(GxBuildHasher::default()),
            hits: 0,
            misses: 0,
            max_entries: 0,
        }
    }

    fn decode_optional(&mut self, value: &str) -> Result<Option<[u8; 32]>> {
        if value.is_empty() {
            return Ok(None);
        }
        if !self.enabled {
            self.misses = self.misses.saturating_add(1);
            return decode_required_pubkey_string(value).map(Some);
        }
        if let Some(pubkey) = self.values.get(value) {
            self.hits = self.hits.saturating_add(1);
            return Ok(Some(*pubkey));
        }

        let pubkey = decode_required_pubkey_string(value)?;
        self.misses = self.misses.saturating_add(1);
        self.values.insert(value.to_owned(), pubkey);
        self.max_entries = self.max_entries.max(self.values.len());
        Ok(Some(pubkey))
    }
}

pub(crate) fn convert_grpc_block(
    block: &SubscribeUpdateBlock,
    block_id: u32,
) -> Result<ConvertedGrpcBlock> {
    convert_grpc_block_inner(block, block_id, None, true)
}

pub(crate) fn convert_grpc_block_timed_with_pubkey_cache(
    block: &SubscribeUpdateBlock,
    block_id: u32,
    timings: &mut GrpcConvertTimings,
    pubkey_string_cache: bool,
) -> Result<ConvertedGrpcBlock> {
    convert_grpc_block_inner(block, block_id, Some(timings), pubkey_string_cache)
}

pub(crate) fn convert_grpc_block_frame_timed_with_pubkey_cache(
    frame: &[u8],
    block_id: u32,
    timings: &mut GrpcConvertTimings,
    pubkey_string_cache: bool,
) -> Result<ConvertedGrpcBlock> {
    convert_grpc_block_frame_inner(frame, block_id, Some(timings), pubkey_string_cache)
}

pub(crate) fn convert_grpc_block_frame_with_pubkey_cache(
    frame: &[u8],
    block_id: u32,
    pubkey_string_cache: bool,
) -> Result<ConvertedGrpcBlock> {
    convert_grpc_block_frame_inner(frame, block_id, None, pubkey_string_cache)
}

macro_rules! borrowed_timing_start {
    ($timings:expr) => {
        if $timings.is_some() {
            Some(Instant::now())
        } else {
            None
        }
    };
}

macro_rules! borrowed_record_timing {
    ($timings:expr, $started_at:expr, $field:ident) => {
        if let Some(started_at) = $started_at {
            if let Some(timings) = $timings.as_deref_mut() {
                timings.$field += started_at.elapsed();
            }
        }
    };
}

fn convert_grpc_block_frame_inner(
    frame: &[u8],
    block_id: u32,
    mut timings: Option<&mut GrpcConvertTimings>,
    pubkey_string_cache: bool,
) -> Result<ConvertedGrpcBlock> {
    let mut reader = BorrowedProtoReader::new(frame);
    let mut cache = PubkeyStringDecodeCache::new(pubkey_string_cache);

    let mut slot = 0u64;
    let mut parent_slot = 0u64;
    let mut blockhash = None;
    let mut block_time = None;
    let mut block_height = None;
    let mut rewards = None;
    let mut entries_count = 0u64;
    let mut executed_transaction_count = 0u64;
    let mut txs = Vec::new();
    let mut entries = Vec::<(u64, CompactPohEntry)>::new();
    let mut missing_transaction_status = false;

    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => slot = reader.read_uint64()?,
            18 => {
                let started_at = borrowed_timing_start!(timings);
                blockhash = Some(decode_required_hash_string(reader.read_string()?)?);
                borrowed_record_timing!(timings, started_at, blockhash);
            }
            26 => {
                let rewards_bytes = reader.read_bytes()?;
                let started_at = borrowed_timing_start!(timings);
                rewards = Some(read_borrowed_rewards(rewards_bytes)?);
                borrowed_record_timing!(timings, started_at, block_rewards);
            }
            34 => block_time = Some(read_borrowed_unix_timestamp(reader.read_bytes()?)?),
            42 => block_height = Some(read_borrowed_block_height(reader.read_bytes()?)?),
            50 => {
                let tx_bytes = reader.read_bytes()?;
                let started_at = borrowed_timing_start!(timings);
                let tx = read_borrowed_transaction_info(
                    tx_bytes,
                    slot,
                    timings.as_deref_mut(),
                    &mut cache,
                )?;
                if tx.metadata.is_none() {
                    missing_transaction_status = true;
                }
                txs.push(tx);
                borrowed_record_timing!(timings, started_at, transactions_total);
            }
            56 => parent_slot = reader.read_uint64()?,
            66 | 90 => reader.skip_length_delimited()?,
            72 => executed_transaction_count = reader.read_uint64()?,
            80 => {
                let _ = reader.read_uint64()?;
            }
            96 => entries_count = reader.read_uint64()?,
            106 => {
                let entry_bytes = reader.read_bytes()?;
                let started_at = borrowed_timing_start!(timings);
                let (index, entry) = read_borrowed_entry(entry_bytes)?;
                entries.push((index, entry));
                borrowed_record_timing!(timings, started_at, poh_entries);
            }
            tag => reader.skip_unknown(tag)?,
        }
    }

    let started_at = borrowed_timing_start!(timings);
    entries.sort_by_key(|(index, _entry)| *index);
    let poh_entries = entries
        .into_iter()
        .map(|(_index, entry)| entry)
        .collect::<Vec<_>>();

    let mut missing = Vec::new();
    if entries_count as usize != poh_entries.len() {
        missing.push(LiveBlockMissingField::PohEntries);
    }
    if executed_transaction_count as usize != txs.len() {
        missing.push(LiveBlockMissingField::Transactions);
    }
    if missing_transaction_status {
        missing.push(LiveBlockMissingField::TransactionStatus);
    }
    missing.push(LiveBlockMissingField::Shredding);

    let transaction_count = txs.len();
    let blockhash = blockhash.context("missing blockhash")?;
    let header = CompactBlockHeader {
        slot,
        parent_slot,
        blockhash: block_id,
        previous_blockhash: block_id.saturating_sub(1),
        block_time,
        block_height,
        shredding: Vec::new(),
        poh_entries: Vec::new(),
        rewards: None,
    };

    if let Some(started_at) = started_at {
        if let Some(timings) = timings.as_deref_mut() {
            timings.block_finish += started_at.elapsed();
        }
    }
    if let Some(timings) = timings.as_deref_mut() {
        timings.token_pubkey_cache_hits =
            timings.token_pubkey_cache_hits.saturating_add(cache.hits);
        timings.token_pubkey_cache_misses = timings
            .token_pubkey_cache_misses
            .saturating_add(cache.misses);
        timings.token_pubkey_cache_max_entries = timings
            .token_pubkey_cache_max_entries
            .max(cache.max_entries);
    }

    Ok(ConvertedGrpcBlock {
        block: WincodeArchiveV2NoRegistryBlock {
            header: WincodeArchiveV2NoRegistryBlockHeader {
                compact: header,
                rewards,
            },
            txs,
        },
        poh_entries,
        blockhash,
        transaction_count,
        missing,
    })
}

fn convert_grpc_block_inner(
    block: &SubscribeUpdateBlock,
    block_id: u32,
    mut timings: Option<&mut GrpcConvertTimings>,
    pubkey_string_cache: bool,
) -> Result<ConvertedGrpcBlock> {
    let mut pubkey_string_cache = PubkeyStringDecodeCache::new(pubkey_string_cache);

    let started_at = Instant::now();
    let blockhash = decode_required_hash_string(&block.blockhash)?;
    if let Some(timings) = timings.as_deref_mut() {
        timings.blockhash += started_at.elapsed();
    }

    let started_at = Instant::now();
    let mut entries = Vec::with_capacity(block.entries.len());
    for entry in &block.entries {
        let hash = bytes32(&entry.hash).with_context(|| {
            format!(
                "slot {} entry {} had invalid PoH hash length {}",
                entry.slot,
                entry.index,
                entry.hash.len()
            )
        })?;
        entries.push((
            entry.index,
            CompactPohEntry {
                num_hashes: entry.num_hashes,
                hash,
                tx_count: u32::try_from(entry.executed_transaction_count).with_context(|| {
                    format!(
                        "slot {} entry {} tx count exceeds u32::MAX",
                        entry.slot, entry.index
                    )
                })?,
            },
        ));
    }
    entries.sort_by_key(|(index, _entry)| *index);
    let poh_entries = entries
        .into_iter()
        .map(|(_index, entry)| entry)
        .collect::<Vec<_>>();
    if let Some(timings) = timings.as_deref_mut() {
        timings.poh_entries += started_at.elapsed();
    }

    let started_at = Instant::now();
    let mut txs = Vec::with_capacity(block.transactions.len());
    for tx in &block.transactions {
        let transaction = tx.transaction.as_ref().with_context(|| {
            format!(
                "slot {} tx {} missing transaction payload",
                block.slot, tx.index
            )
        })?;
        let source_len = transaction.encoded_len() as u64;

        let metadata = if let Some(meta) = tx.meta.as_ref() {
            let meta_started_at = Instant::now();
            let source_len = meta.encoded_len() as u64;
            let value = convert_grpc_meta(meta, timings.as_deref_mut(), &mut pubkey_string_cache)?;
            if let Some(timings) = timings.as_deref_mut() {
                timings.tx_metadata += meta_started_at.elapsed();
            }
            Some(WincodeArchiveV2Payload::Decoded { source_len, value })
        } else {
            None
        };

        txs.push(WincodeArchiveV2NoRegistryTransaction {
            tx_index: u32::try_from(tx.index).with_context(|| {
                format!("slot {} tx index {} exceeds u32::MAX", block.slot, tx.index)
            })?,
            tx: WincodeArchiveV2Payload::Decoded {
                source_len,
                value: convert_grpc_transaction(transaction, timings.as_deref_mut())?,
            },
            metadata,
        });
    }
    if let Some(timings) = timings.as_deref_mut() {
        timings.transactions_total += started_at.elapsed();
    }

    let started_at = Instant::now();
    let mut missing = Vec::new();
    if block.entries_count as usize != poh_entries.len() {
        missing.push(LiveBlockMissingField::PohEntries);
    }
    if block.executed_transaction_count as usize != txs.len() {
        missing.push(LiveBlockMissingField::Transactions);
    }
    if block.transactions.iter().any(|tx| tx.meta.is_none()) {
        missing.push(LiveBlockMissingField::TransactionStatus);
    }
    missing.push(LiveBlockMissingField::Shredding);

    let transaction_count = txs.len();
    let header = CompactBlockHeader {
        slot: block.slot,
        parent_slot: block.parent_slot,
        blockhash: block_id,
        previous_blockhash: block_id.saturating_sub(1),
        block_time: block.block_time.as_ref().map(|time| time.timestamp),
        block_height: block
            .block_height
            .as_ref()
            .map(|height| height.block_height),
        shredding: Vec::new(),
        poh_entries: Vec::new(),
        rewards: None,
    };

    let rewards = {
        let rewards_started_at = Instant::now();
        let rewards = block
            .rewards
            .as_ref()
            .map(convert_grpc_rewards)
            .transpose()?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.block_rewards += rewards_started_at.elapsed();
        }
        rewards
    };
    if let Some(timings) = timings.as_deref_mut() {
        timings.block_finish += started_at.elapsed();
        timings.token_pubkey_cache_hits = timings
            .token_pubkey_cache_hits
            .saturating_add(pubkey_string_cache.hits);
        timings.token_pubkey_cache_misses = timings
            .token_pubkey_cache_misses
            .saturating_add(pubkey_string_cache.misses);
        timings.token_pubkey_cache_max_entries = timings
            .token_pubkey_cache_max_entries
            .max(pubkey_string_cache.max_entries);
    }

    Ok(ConvertedGrpcBlock {
        block: WincodeArchiveV2NoRegistryBlock {
            header: WincodeArchiveV2NoRegistryBlockHeader {
                compact: header,
                rewards,
            },
            txs,
        },
        poh_entries,
        blockhash,
        transaction_count,
        missing,
    })
}

fn read_borrowed_transaction_info(
    bytes: &[u8],
    block_slot: u64,
    mut timings: Option<&mut GrpcConvertTimings>,
    pubkey_string_cache: &mut PubkeyStringDecodeCache,
) -> Result<WincodeArchiveV2NoRegistryTransaction> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut transaction = None;
    let mut metadata = None;
    let mut tx_index = 0u64;

    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => reader.skip_length_delimited()?,
            16 => {
                let _ = reader.read_bool()?;
            }
            26 => {
                let transaction_bytes = reader.read_bytes()?;
                let source_len = transaction_bytes.len() as u64;
                transaction = Some(WincodeArchiveV2Payload::Decoded {
                    source_len,
                    value: read_borrowed_transaction(transaction_bytes, timings.as_deref_mut())?,
                });
            }
            34 => {
                let metadata_bytes = reader.read_bytes()?;
                let source_len = metadata_bytes.len() as u64;
                let started_at = borrowed_timing_start!(timings);
                let value = read_borrowed_meta(
                    metadata_bytes,
                    timings.as_deref_mut(),
                    pubkey_string_cache,
                )?;
                borrowed_record_timing!(timings, started_at, tx_metadata);
                metadata = Some(WincodeArchiveV2Payload::Decoded { source_len, value });
            }
            40 => tx_index = reader.read_uint64()?,
            tag => reader.skip_unknown(tag)?,
        }
    }

    Ok(WincodeArchiveV2NoRegistryTransaction {
        tx_index: u32::try_from(tx_index)
            .with_context(|| format!("slot {block_slot} tx index {tx_index} exceeds u32::MAX"))?,
        tx: transaction
            .with_context(|| format!("slot {block_slot} tx {tx_index} missing transaction"))?,
        metadata,
    })
}

fn read_borrowed_transaction(
    bytes: &[u8],
    mut timings: Option<&mut GrpcConvertTimings>,
) -> Result<WincodeArchiveV2NoRegistryTx> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut signatures = Vec::new();
    let mut message = None;

    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => {
                let started_at = borrowed_timing_start!(timings);
                signatures.push(reader.read_bytes()?.to_vec());
                borrowed_record_timing!(timings, started_at, tx_signatures);
            }
            18 => {
                let started_at = borrowed_timing_start!(timings);
                message = Some(read_borrowed_message(
                    reader.read_bytes()?,
                    timings.as_deref_mut(),
                )?);
                borrowed_record_timing!(timings, started_at, tx_message);
            }
            tag => reader.skip_unknown(tag)?,
        }
    }

    Ok(WincodeArchiveV2NoRegistryTx {
        signatures,
        message: message.context("transaction missing message")?,
    })
}

fn read_borrowed_message(
    bytes: &[u8],
    mut timings: Option<&mut GrpcConvertTimings>,
) -> Result<WincodeArchiveV2NoRegistryMessage> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut header = None;
    let mut account_keys = Vec::new();
    let mut recent_blockhash = None;
    let mut instructions = Vec::new();
    let mut versioned = false;
    let mut address_table_lookups = Vec::new();

    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => header = Some(read_borrowed_message_header(reader.read_bytes()?)?),
            18 => {
                let started_at = borrowed_timing_start!(timings);
                account_keys.push(bytes32(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, message_account_keys);
            }
            26 => recent_blockhash = Some(bytes32(reader.read_bytes()?)?),
            34 => {
                let started_at = borrowed_timing_start!(timings);
                instructions.push(read_borrowed_compiled_instruction(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, message_instructions);
            }
            40 => versioned = reader.read_bool()?,
            50 => {
                let started_at = borrowed_timing_start!(timings);
                address_table_lookups
                    .push(read_borrowed_address_table_lookup(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, message_address_table_lookups);
            }
            tag => reader.skip_unknown(tag)?,
        }
    }

    let header = header.context("message missing header")?;
    let recent_blockhash = recent_blockhash.context("message missing recent blockhash")?;
    if versioned {
        Ok(WincodeArchiveV2NoRegistryMessage::V0(
            WincodeArchiveV2NoRegistryV0Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                address_table_lookups,
            },
        ))
    } else {
        Ok(WincodeArchiveV2NoRegistryMessage::Legacy(
            WincodeArchiveV2NoRegistryLegacyMessage {
                header,
                account_keys,
                recent_blockhash,
                instructions,
            },
        ))
    }
}

fn read_borrowed_message_header(bytes: &[u8]) -> Result<CompactMessageHeader> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut num_required_signatures = 0u32;
    let mut num_readonly_signed_accounts = 0u32;
    let mut num_readonly_unsigned_accounts = 0u32;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => num_required_signatures = reader.read_uint32()?,
            16 => num_readonly_signed_accounts = reader.read_uint32()?,
            24 => num_readonly_unsigned_accounts = reader.read_uint32()?,
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(CompactMessageHeader {
        num_required_signatures: u8::try_from(num_required_signatures)
            .context("num_required_signatures exceeds u8::MAX")?,
        num_readonly_signed_accounts: u8::try_from(num_readonly_signed_accounts)
            .context("num_readonly_signed_accounts exceeds u8::MAX")?,
        num_readonly_unsigned_accounts: u8::try_from(num_readonly_unsigned_accounts)
            .context("num_readonly_unsigned_accounts exceeds u8::MAX")?,
    })
}

fn read_borrowed_compiled_instruction(
    bytes: &[u8],
) -> Result<WincodeArchiveV2NoRegistryInstruction> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut program_id_index = 0u32;
    let mut accounts = Vec::new();
    let mut data = Vec::new();
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => program_id_index = reader.read_uint32()?,
            18 => accounts = reader.read_bytes()?.to_vec(),
            26 => data = reader.read_bytes()?.to_vec(),
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(WincodeArchiveV2NoRegistryInstruction {
        program_id_index: u8::try_from(program_id_index)
            .context("program_id_index exceeds u8::MAX")?,
        accounts,
        data,
    })
}

fn read_borrowed_address_table_lookup(
    bytes: &[u8],
) -> Result<WincodeArchiveV2NoRegistryAddressTableLookup> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut account_key = None;
    let mut writable_indexes = Vec::new();
    let mut readonly_indexes = Vec::new();
    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => account_key = Some(bytes32(reader.read_bytes()?)?),
            18 => writable_indexes = reader.read_bytes()?.to_vec(),
            26 => readonly_indexes = reader.read_bytes()?.to_vec(),
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(WincodeArchiveV2NoRegistryAddressTableLookup {
        account_key: account_key.context("address table lookup missing account key")?,
        writable_indexes,
        readonly_indexes,
    })
}

fn read_borrowed_meta(
    bytes: &[u8],
    mut timings: Option<&mut GrpcConvertTimings>,
    pubkey_string_cache: &mut PubkeyStringDecodeCache,
) -> Result<WincodeArchiveV2NoRegistryMeta> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut err = None;
    let mut fee = 0u64;
    let mut pre_balances = Vec::new();
    let mut post_balances = Vec::new();
    let mut inner_instructions = Vec::new();
    let mut inner_instructions_none = false;
    let mut logs = Vec::new();
    let mut log_messages_none = false;
    let mut pre_token_balances = Vec::new();
    let mut post_token_balances = Vec::new();
    let mut rewards = Vec::new();
    let mut loaded_writable_addresses = Vec::new();
    let mut loaded_readonly_addresses = Vec::new();
    let mut return_data = None;
    let mut compute_units_consumed = None;
    let mut cost_units = None;

    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => {
                let started_at = borrowed_timing_start!(timings);
                err = Some(read_borrowed_transaction_error(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, meta_error);
            }
            16 => fee = reader.read_uint64()?,
            24 => {
                let started_at = borrowed_timing_start!(timings);
                pre_balances.push(reader.read_uint64()?);
                borrowed_record_timing!(timings, started_at, meta_balances);
            }
            26 => {
                let started_at = borrowed_timing_start!(timings);
                read_packed_u64_into(reader.read_bytes()?, &mut pre_balances)?;
                borrowed_record_timing!(timings, started_at, meta_balances);
            }
            32 => {
                let started_at = borrowed_timing_start!(timings);
                post_balances.push(reader.read_uint64()?);
                borrowed_record_timing!(timings, started_at, meta_balances);
            }
            34 => {
                let started_at = borrowed_timing_start!(timings);
                read_packed_u64_into(reader.read_bytes()?, &mut post_balances)?;
                borrowed_record_timing!(timings, started_at, meta_balances);
            }
            42 => {
                let started_at = borrowed_timing_start!(timings);
                inner_instructions.push(read_borrowed_inner_instructions(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, meta_inner_instructions);
            }
            50 => {
                let started_at = borrowed_timing_start!(timings);
                logs.push(reader.read_string()?);
                borrowed_record_timing!(timings, started_at, meta_logs);
            }
            58 => {
                let started_at = borrowed_timing_start!(timings);
                pre_token_balances.push(read_borrowed_token_balance(
                    reader.read_bytes()?,
                    pubkey_string_cache,
                )?);
                borrowed_record_timing!(timings, started_at, meta_token_balances);
            }
            66 => {
                let started_at = borrowed_timing_start!(timings);
                post_token_balances.push(read_borrowed_token_balance(
                    reader.read_bytes()?,
                    pubkey_string_cache,
                )?);
                borrowed_record_timing!(timings, started_at, meta_token_balances);
            }
            74 => {
                let started_at = borrowed_timing_start!(timings);
                rewards.push(read_borrowed_reward(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, meta_rewards);
            }
            80 => inner_instructions_none = reader.read_bool()?,
            88 => log_messages_none = reader.read_bool()?,
            98 => {
                let started_at = borrowed_timing_start!(timings);
                loaded_writable_addresses.push(bytes32(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, meta_loaded_addresses);
            }
            106 => {
                let started_at = borrowed_timing_start!(timings);
                loaded_readonly_addresses.push(bytes32(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, meta_loaded_addresses);
            }
            114 => {
                let started_at = borrowed_timing_start!(timings);
                return_data = Some(read_borrowed_return_data(reader.read_bytes()?)?);
                borrowed_record_timing!(timings, started_at, meta_return_data);
            }
            120 => {
                let _ = reader.read_bool()?;
            }
            128 => compute_units_consumed = Some(reader.read_uint64()?),
            136 => cost_units = Some(reader.read_uint64()?),
            tag => reader.skip_unknown(tag)?,
        }
    }

    let logs = if log_messages_none {
        None
    } else {
        let started_at = borrowed_timing_start!(timings);
        let logs = if let Some(timings) = timings.as_deref_mut()
            && timings.collect_log_parse_stats
        {
            compact_live_log_strs_with_stats(&logs, &mut timings.meta_log_parse_stats)?
        } else {
            compact_live_log_strs(&logs)?
        };
        borrowed_record_timing!(timings, started_at, meta_logs);
        Some(logs)
    };

    Ok(WincodeArchiveV2NoRegistryMeta {
        err,
        fee,
        pre_balances,
        post_balances,
        inner_instructions: if inner_instructions_none {
            None
        } else {
            Some(inner_instructions)
        },
        logs,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data,
        compute_units_consumed,
        cost_units,
    })
}

fn read_borrowed_transaction_error(bytes: &[u8]) -> Result<CompactTransactionError> {
    let mut reader = BorrowedProtoReader::new(bytes);
    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => {
                return CompactTransactionError::from_stored_wincode_bytes(reader.read_bytes()?);
            }
            tag => reader.skip_unknown(tag)?,
        }
    }
    CompactTransactionError::from_stored_wincode_bytes(&[])
}

fn read_borrowed_inner_instructions(bytes: &[u8]) -> Result<CompactInnerInstructions> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut index = 0u32;
    let mut instructions = Vec::new();
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => index = reader.read_uint32()?,
            18 => instructions.push(read_borrowed_inner_instruction(reader.read_bytes()?)?),
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(CompactInnerInstructions {
        index,
        instructions,
    })
}

fn read_borrowed_inner_instruction(bytes: &[u8]) -> Result<CompactInnerInstruction> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut program_id_index = 0u32;
    let mut accounts = Vec::new();
    let mut data = Vec::new();
    let mut stack_height = None;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => program_id_index = reader.read_uint32()?,
            18 => accounts = reader.read_bytes()?.to_vec(),
            26 => data = reader.read_bytes()?.to_vec(),
            32 => stack_height = Some(reader.read_uint32()?),
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(CompactInnerInstruction {
        program_id_index,
        accounts,
        data,
        stack_height,
    })
}

fn read_borrowed_token_balance(
    bytes: &[u8],
    pubkey_string_cache: &mut PubkeyStringDecodeCache,
) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut account_index = 0u32;
    let mut mint = None;
    let mut owner = None;
    let mut program_id = None;
    let mut ui_token_amount = None;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => account_index = reader.read_uint32()?,
            18 => mint = pubkey_string_cache.decode_optional(reader.read_string()?)?,
            26 => ui_token_amount = Some(read_borrowed_ui_token_amount(reader.read_bytes()?)?),
            34 => owner = pubkey_string_cache.decode_optional(reader.read_string()?)?,
            42 => program_id = pubkey_string_cache.decode_optional(reader.read_string()?)?,
            tag => reader.skip_unknown(tag)?,
        }
    }
    let (amount, decimals) = if let Some(ui) = ui_token_amount {
        (
            ui.amount
                .parse::<u64>()
                .context("parse token balance amount")?,
            u8::try_from(ui.decimals).context("token balance decimals exceeds u8::MAX")?,
        )
    } else {
        (0, 0)
    };
    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index,
        mint,
        owner,
        program_id,
        amount,
        decimals,
    })
}

struct BorrowedUiTokenAmount {
    amount: String,
    decimals: u32,
}

fn read_borrowed_ui_token_amount(bytes: &[u8]) -> Result<BorrowedUiTokenAmount> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut amount = String::new();
    let mut decimals = 0u32;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            16 => decimals = reader.read_uint32()?,
            26 => amount = reader.read_string()?.to_owned(),
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(BorrowedUiTokenAmount { amount, decimals })
}

fn read_borrowed_reward(bytes: &[u8]) -> Result<WincodeArchiveV2NoRegistryReward> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut pubkey = None;
    let mut lamports = 0i64;
    let mut post_balance = 0u64;
    let mut reward_type = 0i32;
    let mut commission = None;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => pubkey = Some(decode_required_pubkey_string(reader.read_string()?)?),
            16 => lamports = reader.read_int64()?,
            24 => post_balance = reader.read_uint64()?,
            32 => reward_type = reader.read_int32()?,
            42 => {
                let value = reader.read_string()?;
                if !value.is_empty() {
                    commission = Some(
                        value
                            .parse::<u8>()
                            .with_context(|| format!("parse reward commission {value}"))?,
                    );
                }
            }
            50 => reader.skip_length_delimited()?,
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(WincodeArchiveV2NoRegistryReward {
        pubkey: pubkey.context("reward missing pubkey")?,
        lamports,
        post_balance,
        reward_type,
        commission,
    })
}

fn read_borrowed_rewards(bytes: &[u8]) -> Result<WincodeArchiveV2NoRegistryRewards> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut rewards = Vec::new();
    let mut num_partitions = None;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => rewards.push(read_borrowed_reward(reader.read_bytes()?)?),
            18 => num_partitions = Some(read_borrowed_num_partitions(reader.read_bytes()?)?),
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(WincodeArchiveV2NoRegistryRewards {
        source_len: bytes.len() as u64,
        num_partitions,
        decoded: Some(rewards),
        raw_fallback: None,
        decode_error: None,
    })
}

fn read_borrowed_return_data(bytes: &[u8]) -> Result<WincodeArchiveV2NoRegistryReturnData> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut program_id = None;
    let mut data = Vec::new();
    while let Some(tag) = reader.next_tag()? {
        match tag {
            10 => program_id = Some(bytes32(reader.read_bytes()?)?),
            18 => data = reader.read_bytes()?.to_vec(),
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(WincodeArchiveV2NoRegistryReturnData {
        program_id: program_id.context("return data missing program id")?,
        data,
    })
}

fn read_borrowed_num_partitions(bytes: &[u8]) -> Result<u64> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut num_partitions = 0u64;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => num_partitions = reader.read_uint64()?,
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(num_partitions)
}

fn read_borrowed_unix_timestamp(bytes: &[u8]) -> Result<i64> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut timestamp = 0i64;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => timestamp = reader.read_int64()?,
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(timestamp)
}

fn read_borrowed_block_height(bytes: &[u8]) -> Result<u64> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut block_height = 0u64;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => block_height = reader.read_uint64()?,
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok(block_height)
}

fn read_borrowed_entry(bytes: &[u8]) -> Result<(u64, CompactPohEntry)> {
    let mut reader = BorrowedProtoReader::new(bytes);
    let mut index = 0u64;
    let mut num_hashes = 0u64;
    let mut hash = None;
    let mut tx_count = 0u64;
    while let Some(tag) = reader.next_tag()? {
        match tag {
            8 => {
                let _ = reader.read_uint64()?;
            }
            16 => index = reader.read_uint64()?,
            24 => num_hashes = reader.read_uint64()?,
            34 => hash = Some(bytes32(reader.read_bytes()?)?),
            40 => tx_count = reader.read_uint64()?,
            48 => {
                let _ = reader.read_uint64()?;
            }
            tag => reader.skip_unknown(tag)?,
        }
    }
    Ok((
        index,
        CompactPohEntry {
            num_hashes,
            hash: hash.context("entry missing PoH hash")?,
            tx_count: u32::try_from(tx_count).context("entry tx count exceeds u32::MAX")?,
        },
    ))
}

fn read_packed_u64_into(bytes: &[u8], out: &mut Vec<u64>) -> Result<()> {
    let mut reader = BorrowedProtoReader::new(bytes);
    while !reader.is_eof() {
        out.push(reader.read_uint64()?);
    }
    Ok(())
}

struct BorrowedProtoReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> BorrowedProtoReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn next_tag(&mut self) -> Result<Option<u32>> {
        if self.is_eof() {
            return Ok(None);
        }
        let tag = self.read_varint64()?;
        let tag = u32::try_from(tag).context("protobuf tag exceeds u32::MAX")?;
        if tag == 0 {
            return Err(anyhow!("protobuf tag 0 is invalid"));
        }
        Ok(Some(tag))
    }

    fn read_bool(&mut self) -> Result<bool> {
        Ok(self.read_uint64()? != 0)
    }

    fn read_uint32(&mut self) -> Result<u32> {
        u32::try_from(self.read_uint64()?).context("protobuf uint32 exceeds u32::MAX")
    }

    fn read_uint64(&mut self) -> Result<u64> {
        self.read_varint64()
    }

    fn read_int32(&mut self) -> Result<i32> {
        Ok(self.read_uint32()? as i32)
    }

    fn read_int64(&mut self) -> Result<i64> {
        Ok(self.read_uint64()? as i64)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.read_len()?;
        let start = self.pos;
        self.advance(len)?;
        Ok(&self.bytes[start..self.pos])
    }

    fn read_string(&mut self) -> Result<&'a str> {
        std::str::from_utf8(self.read_bytes()?).context("protobuf string was not utf-8")
    }

    fn skip_length_delimited(&mut self) -> Result<()> {
        let len = self.read_len()?;
        self.advance(len)
    }

    fn skip_unknown(&mut self, tag: u32) -> Result<()> {
        match (tag & 0x07) as u8 {
            0 => {
                let _ = self.read_varint64()?;
                Ok(())
            }
            1 => self.advance(8),
            2 => self.skip_length_delimited(),
            5 => self.advance(4),
            wire => Err(anyhow!(
                "unsupported protobuf wire type {wire} for tag {tag}"
            )),
        }
    }

    fn read_len(&mut self) -> Result<usize> {
        usize::try_from(self.read_varint64()?).context("protobuf length exceeds usize::MAX")
    }

    fn read_varint64(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            if self.pos >= self.bytes.len() {
                return Err(anyhow!("truncated protobuf varint"));
            }
            let byte = self.bytes[self.pos];
            self.pos += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(anyhow!("protobuf varint exceeds 64 bits"))
    }

    fn advance(&mut self, len: usize) -> Result<()> {
        let end = self
            .pos
            .checked_add(len)
            .context("protobuf cursor overflow")?;
        if end > self.bytes.len() {
            return Err(anyhow!(
                "truncated protobuf field: need {} bytes at {}, input {}",
                len,
                self.pos,
                self.bytes.len()
            ));
        }
        self.pos = end;
        Ok(())
    }
}

fn convert_grpc_transaction(
    tx: &GrpcTransaction,
    mut timings: Option<&mut GrpcConvertTimings>,
) -> Result<WincodeArchiveV2NoRegistryTx> {
    let message = tx.message.as_ref().context("transaction missing message")?;
    let started_at = Instant::now();
    let converted_message = convert_grpc_message(message, timings.as_deref_mut())?;
    if let Some(timings) = timings.as_deref_mut() {
        timings.tx_message += started_at.elapsed();
    }

    let started_at = Instant::now();
    let signatures = tx.signatures.clone();
    if let Some(timings) = timings.as_deref_mut() {
        timings.tx_signatures += started_at.elapsed();
    }

    Ok(WincodeArchiveV2NoRegistryTx {
        signatures,
        message: converted_message,
    })
}

fn convert_grpc_message(
    message: &GrpcMessage,
    mut timings: Option<&mut GrpcConvertTimings>,
) -> Result<WincodeArchiveV2NoRegistryMessage> {
    let header = message.header.as_ref().context("message missing header")?;
    let header = CompactMessageHeader {
        num_required_signatures: u8::try_from(header.num_required_signatures)
            .context("num_required_signatures exceeds u8::MAX")?,
        num_readonly_signed_accounts: u8::try_from(header.num_readonly_signed_accounts)
            .context("num_readonly_signed_accounts exceeds u8::MAX")?,
        num_readonly_unsigned_accounts: u8::try_from(header.num_readonly_unsigned_accounts)
            .context("num_readonly_unsigned_accounts exceeds u8::MAX")?,
    };

    let started_at = Instant::now();
    let mut account_keys = Vec::with_capacity(message.account_keys.len());
    for key in &message.account_keys {
        account_keys.push(bytes32(key)?);
    }
    if let Some(timings) = timings.as_deref_mut() {
        timings.message_account_keys += started_at.elapsed();
    }

    let recent_blockhash = bytes32(&message.recent_blockhash)?;
    let started_at = Instant::now();
    let mut instructions = Vec::with_capacity(message.instructions.len());
    for ix in &message.instructions {
        instructions.push(WincodeArchiveV2NoRegistryInstruction {
            program_id_index: u8::try_from(ix.program_id_index)
                .context("program_id_index exceeds u8::MAX")?,
            accounts: ix.accounts.clone(),
            data: ix.data.clone(),
        });
    }
    if let Some(timings) = timings.as_deref_mut() {
        timings.message_instructions += started_at.elapsed();
    }

    if message.versioned {
        let started_at = Instant::now();
        let mut address_table_lookups = Vec::with_capacity(message.address_table_lookups.len());
        for lookup in &message.address_table_lookups {
            address_table_lookups.push(WincodeArchiveV2NoRegistryAddressTableLookup {
                account_key: bytes32(&lookup.account_key)?,
                writable_indexes: lookup.writable_indexes.clone(),
                readonly_indexes: lookup.readonly_indexes.clone(),
            });
        }
        if let Some(timings) = timings.as_deref_mut() {
            timings.message_address_table_lookups += started_at.elapsed();
        }
        Ok(WincodeArchiveV2NoRegistryMessage::V0(
            WincodeArchiveV2NoRegistryV0Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                address_table_lookups,
            },
        ))
    } else {
        Ok(WincodeArchiveV2NoRegistryMessage::Legacy(
            WincodeArchiveV2NoRegistryLegacyMessage {
                header,
                account_keys,
                recent_blockhash,
                instructions,
            },
        ))
    }
}

fn convert_grpc_meta(
    meta: &GrpcTransactionStatusMeta,
    mut timings: Option<&mut GrpcConvertTimings>,
    pubkey_string_cache: &mut PubkeyStringDecodeCache,
) -> Result<WincodeArchiveV2NoRegistryMeta> {
    let started_at = Instant::now();
    let err = meta
        .err
        .as_ref()
        .map(|err| CompactTransactionError::from_stored_wincode_bytes(&err.err))
        .transpose()?;
    if let Some(timings) = timings.as_deref_mut() {
        timings.meta_error += started_at.elapsed();
    }

    let started_at = Instant::now();
    let pre_balances = meta.pre_balances.clone();
    let post_balances = meta.post_balances.clone();
    if let Some(timings) = timings.as_deref_mut() {
        timings.meta_balances += started_at.elapsed();
    }

    let started_at = Instant::now();
    let inner_instructions = if meta.inner_instructions_none {
        None
    } else {
        let mut out = Vec::with_capacity(meta.inner_instructions.len());
        for inner in &meta.inner_instructions {
            let mut instructions = Vec::with_capacity(inner.instructions.len());
            for ix in &inner.instructions {
                instructions.push(CompactInnerInstruction {
                    program_id_index: ix.program_id_index,
                    accounts: ix.accounts.clone(),
                    data: ix.data.clone(),
                    stack_height: ix.stack_height,
                });
            }
            out.push(CompactInnerInstructions {
                index: inner.index,
                instructions,
            });
        }
        Some(out)
    };
    if let Some(timings) = timings.as_deref_mut() {
        timings.meta_inner_instructions += started_at.elapsed();
    }

    let started_at = Instant::now();
    let logs = if meta.log_messages_none {
        None
    } else {
        let logs = if let Some(timings) = timings.as_deref_mut()
            && timings.collect_log_parse_stats
        {
            compact_live_logs_with_stats(&meta.log_messages, &mut timings.meta_log_parse_stats)?
        } else {
            compact_live_logs(&meta.log_messages)?
        };
        Some(logs)
    };
    if let Some(timings) = timings.as_deref_mut() {
        timings.meta_logs += started_at.elapsed();
    }

    let started_at = Instant::now();
    let mut pre_token_balances = Vec::with_capacity(meta.pre_token_balances.len());
    for balance in &meta.pre_token_balances {
        pre_token_balances.push(convert_grpc_token_balance(balance, pubkey_string_cache)?);
    }
    let mut post_token_balances = Vec::with_capacity(meta.post_token_balances.len());
    for balance in &meta.post_token_balances {
        post_token_balances.push(convert_grpc_token_balance(balance, pubkey_string_cache)?);
    }
    if let Some(timings) = timings.as_deref_mut() {
        timings.meta_token_balances += started_at.elapsed();
    }

    let started_at = Instant::now();
    let mut rewards = Vec::with_capacity(meta.rewards.len());
    for reward in &meta.rewards {
        rewards.push(convert_grpc_reward(reward)?);
    }
    if let Some(timings) = timings.as_deref_mut() {
        timings.meta_rewards += started_at.elapsed();
    }

    let started_at = Instant::now();
    let mut loaded_writable_addresses = Vec::with_capacity(meta.loaded_writable_addresses.len());
    for address in &meta.loaded_writable_addresses {
        loaded_writable_addresses.push(bytes32(address)?);
    }
    let mut loaded_readonly_addresses = Vec::with_capacity(meta.loaded_readonly_addresses.len());
    for address in &meta.loaded_readonly_addresses {
        loaded_readonly_addresses.push(bytes32(address)?);
    }
    if let Some(timings) = timings.as_deref_mut() {
        timings.meta_loaded_addresses += started_at.elapsed();
    }

    let started_at = Instant::now();
    let return_data = meta
        .return_data
        .as_ref()
        .map(|data| {
            Ok::<_, anyhow::Error>(WincodeArchiveV2NoRegistryReturnData {
                program_id: bytes32(&data.program_id)?,
                data: data.data.clone(),
            })
        })
        .transpose()?;
    if let Some(timings) = timings.as_deref_mut() {
        timings.meta_return_data += started_at.elapsed();
    }

    Ok(WincodeArchiveV2NoRegistryMeta {
        err,
        fee: meta.fee,
        pre_balances,
        post_balances,
        inner_instructions,
        logs,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
}

fn compact_live_logs(logs: &[String]) -> Result<WincodeArchiveV2NoRegistryLogs> {
    Ok(WincodeArchiveV2NoRegistryLogs::Compact(
        parse_logs_with_compactor(logs, &RawPubkeyCompactor),
    ))
}

fn compact_live_logs_with_stats(
    logs: &[String],
    stats: &mut CompactLogParseStats,
) -> Result<WincodeArchiveV2NoRegistryLogs> {
    Ok(WincodeArchiveV2NoRegistryLogs::Compact(
        parse_logs_with_compactor_and_stats(logs, &RawPubkeyCompactor, stats),
    ))
}

fn compact_live_log_strs(logs: &[&str]) -> Result<WincodeArchiveV2NoRegistryLogs> {
    Ok(WincodeArchiveV2NoRegistryLogs::Compact(
        parse_log_strs_with_compactor(logs, &RawPubkeyCompactor),
    ))
}

fn compact_live_log_strs_with_stats(
    logs: &[&str],
    stats: &mut CompactLogParseStats,
) -> Result<WincodeArchiveV2NoRegistryLogs> {
    Ok(WincodeArchiveV2NoRegistryLogs::Compact(
        parse_log_strs_with_compactor_and_stats(logs, &RawPubkeyCompactor, stats),
    ))
}

fn convert_grpc_rewards(rewards: &GrpcRewards) -> Result<WincodeArchiveV2NoRegistryRewards> {
    Ok(WincodeArchiveV2NoRegistryRewards {
        source_len: rewards.encoded_len() as u64,
        num_partitions: rewards
            .num_partitions
            .as_ref()
            .map(|partitions| partitions.num_partitions),
        decoded: Some(
            rewards
                .rewards
                .iter()
                .map(convert_grpc_reward)
                .collect::<Result<Vec<_>>>()?,
        ),
        raw_fallback: None,
        decode_error: None,
    })
}

fn convert_grpc_token_balance(
    balance: &GrpcTokenBalance,
    pubkey_string_cache: &mut PubkeyStringDecodeCache,
) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let ui = balance.ui_token_amount.as_ref();
    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: balance.account_index,
        mint: pubkey_string_cache.decode_optional(&balance.mint)?,
        owner: pubkey_string_cache.decode_optional(&balance.owner)?,
        program_id: pubkey_string_cache.decode_optional(&balance.program_id)?,
        amount: ui
            .map(|ui| ui.amount.parse::<u64>())
            .transpose()
            .context("parse token balance amount")?
            .unwrap_or(0),
        decimals: ui
            .map(|ui| u8::try_from(ui.decimals))
            .transpose()
            .context("token balance decimals exceeds u8::MAX")?
            .unwrap_or(0),
    })
}

fn convert_grpc_reward(reward: &GrpcReward) -> Result<WincodeArchiveV2NoRegistryReward> {
    Ok(WincodeArchiveV2NoRegistryReward {
        pubkey: decode_required_pubkey_string(&reward.pubkey)?,
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: if reward.commission.is_empty() {
            None
        } else {
            Some(
                reward
                    .commission
                    .parse::<u8>()
                    .with_context(|| format!("parse reward commission {}", reward.commission))?,
            )
        },
    })
}

fn decode_hash_string(value: &str) -> Result<Option<[u8; 32]>> {
    if value.is_empty() {
        return Ok(None);
    }
    decode_base58_32(value, "blockhash").map(Some)
}

fn decode_required_hash_string(value: &str) -> Result<[u8; 32]> {
    decode_hash_string(value)?.context("missing blockhash")
}

fn decode_required_pubkey_string(value: &str) -> Result<[u8; 32]> {
    decode_base58_32(value, "pubkey")
}

fn decode_base58_32(value: &str, kind: &str) -> Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    five8::decode_32(value, &mut bytes).map_err(|err| anyhow!("decode {kind} {value}: {err:?}"))?;
    Ok(bytes)
}

fn bytes32(value: &[u8]) -> Result<[u8; 32]> {
    let bytes: [u8; 32] = value
        .try_into()
        .map_err(|_| anyhow!("expected 32 bytes, got {}", value.len()))?;
    Ok(bytes)
}

fn bytes64(value: &[u8]) -> Result<[u8; 64]> {
    let bytes: [u8; 64] = value
        .try_into()
        .map_err(|_| anyhow!("expected 64 bytes, got {}", value.len()))?;
    Ok(bytes)
}
