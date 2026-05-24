use std::{
    collections::HashMap,
    env,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use blockzilla_format::{
    CompactBlockHeader, CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader,
    CompactPohEntry, CompactTransactionError, LiveBlockMissingField, LivePubkeyCountRecord,
    LiveSignatureIndexRecord, SplitCompactIndexHeader, SplitCompactIndexRecord,
    WincodeArchiveV2NoRegistryAddressTableLookup, WincodeArchiveV2NoRegistryBlock,
    WincodeArchiveV2NoRegistryBlockHeader, WincodeArchiveV2NoRegistryInstruction,
    WincodeArchiveV2NoRegistryLegacyMessage, WincodeArchiveV2NoRegistryMessage,
    WincodeArchiveV2NoRegistryMeta, WincodeArchiveV2NoRegistryReturnData,
    WincodeArchiveV2NoRegistryReward, WincodeArchiveV2NoRegistryRewards,
    WincodeArchiveV2NoRegistryTokenBalance, WincodeArchiveV2NoRegistryTransaction,
    WincodeArchiveV2NoRegistryTx, WincodeArchiveV2NoRegistryV0Message, WincodeArchiveV2Payload,
    WincodeArchiveV2PohRecord, WincodeLeb128FramedReader, WincodeLeb128FramedWriter,
    encode_with_scratch, read_u32_varint,
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

use crate::{
    epoch::{EpochBoundaryEvent, EpochBoundaryTracker, EpochSlot, OLD_FAITHFUL_SLOTS_PER_EPOCH},
    layout::ProducerLayout,
    rpc::{RpcEpochSyncConfig, RpcEpochSyncReport, RpcRateLimitConfig, sync_epoch_info},
};

type LivePubkeyCounts = GxHashMap<[u8; 32], u32>;

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
    pub journal_path: PathBuf,
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
    let poh_path = layout.poh_dir.join("poh.wincode");
    let block_index_path = layout.index_dir.join("block-index.bin");
    let blockhash_registry_path = layout.index_dir.join("blockhash_registry.bin");
    let signatures_path = layout.index_dir.join("signatures.bin");
    let signature_index_path = layout.index_dir.join("signature-index.bin");
    let pubkey_counts_path = layout.index_dir.join("pubkey-counts.bin");
    let journal_path = layout.journal_dir.join("grpc-blocks.jsonl");

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
    let mut pubkey_counts = read_pubkey_counts(&pubkey_counts_path)?;

    let block_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&block_path)
        .with_context(|| format!("open {}", block_path.display()))?;
    let raw_blocks_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&raw_blocks_path)
        .with_context(|| format!("open {}", raw_blocks_path.display()))?;
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
    let journal_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .with_context(|| format!("open {}", journal_path.display()))?;

    let mut block_writer = WincodeLeb128FramedWriter::new(BufWriter::new(block_file));
    let mut raw_blocks_writer = WincodeLeb128FramedWriter::new(BufWriter::new(raw_blocks_file));
    let mut poh_writer = WincodeLeb128FramedWriter::new(BufWriter::new(poh_file));
    let mut block_index_writer = WincodeLeb128FramedWriter::new(BufWriter::new(block_index_file));
    let mut blockhash_writer = BufWriter::new(blockhash_file);
    let mut signatures_writer = BufWriter::new(signatures_file);
    let mut signature_index_writer =
        WincodeLeb128FramedWriter::new(BufWriter::new(signature_index_file));
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
        journal_path,
    };

    let deadline = Duration::from_secs(config.timeout_secs);
    let max_blocks = config.max_blocks.max(1);
    let mut processing_duration = Duration::ZERO;
    let mut block_scratch = Vec::with_capacity(2 * 1024 * 1024);
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
                && let Some(first_epoch) = report.first_epoch
                && epoch_slot.epoch != first_epoch
            {
                report.stopped_at_epoch_boundary = true;
                break;
            }

            let local_block_index = u32::try_from(report.blocks_written)
                .context("captured block count exceeds u32::MAX")?;
            let block_id = next_block_id
                .checked_add(local_block_index)
                .context("block id overflow")?;
            let converted = convert_grpc_block(&block, block_id)?;
            let mut raw_block_bytes = Vec::with_capacity(block.encoded_len());
            block.encode(&mut raw_block_bytes)?;
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
            raw_blocks_writer.write_bytes(&raw_block_bytes)?;
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
            count_pubkeys_from_block(&converted.block, &mut pubkey_counts);
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
        }

        block_writer.flush()?;
        raw_blocks_writer.flush()?;
        poh_writer.flush()?;
        block_index_writer.flush()?;
        blockhash_writer.flush()?;
        signatures_writer.flush()?;
        signature_index_writer.flush()?;
        journal_writer.flush()?;
        write_pubkey_counts(&report.pubkey_counts_path, &pubkey_counts)?;
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
    report.pubkey_count_records = pubkey_counts.len();
    let total_bytes = report.block_bytes_written
        + report.raw_block_bytes_written
        + report.poh_bytes_written
        + report.block_index_bytes_written
        + report.blockhash_bytes_written
        + report.signature_bytes_written
        + report.signature_index_bytes_written;
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let processing_secs = processing_duration.as_secs_f64().max(0.001);
    report.mb_per_sec = (total_bytes as f64 / 1_048_576.0) / elapsed_secs;
    report.processing_mb_per_sec = (total_bytes as f64 / 1_048_576.0) / processing_secs;
    report.blocks_per_sec = report.blocks_written as f64 / elapsed_secs;
    report.processing_blocks_per_sec = report.blocks_written as f64 / processing_secs;

    Ok(report)
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

    Ok(report)
}

async fn connect_grpc(endpoint: &str) -> Result<yellowstone_grpc_client::GeyserGrpcClient> {
    let token = env::var("BLOCKZILLA_GRPC_X_TOKEN")
        .context("missing BLOCKZILLA_GRPC_X_TOKEN environment variable")?;
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(Some(token))?
        .max_decoding_message_size(128 * 1024 * 1024);
    if endpoint.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(builder.connect().await?)
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
        .map(|(pubkey, count)| LivePubkeyCountRecord {
            pubkey: *pubkey,
            count: u64::from(*count),
        })
        .collect::<Vec<_>>();
    records.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.pubkey.cmp(&b.pubkey)));
    for record in records {
        writer.write(&record)?;
    }
    writer.flush()?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("rename {} to {}", tmp_path.display(), path.display()))
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

fn count_pubkeys_from_block(
    block: &WincodeArchiveV2NoRegistryBlock,
    counts: &mut LivePubkeyCounts,
) {
    visit_pubkeys_from_block(block, |pubkey| increment_pubkey(counts, pubkey));
}

pub(crate) fn visit_pubkeys_from_block(
    block: &WincodeArchiveV2NoRegistryBlock,
    mut visit: impl FnMut([u8; 32]),
) {
    if let Some(rewards) = block.header.rewards.as_ref()
        && let Some(decoded) = rewards.decoded.as_ref()
    {
        for reward in decoded {
            visit(reward.pubkey);
        }
    }

    for tx in &block.txs {
        if let WincodeArchiveV2Payload::Decoded { value, .. } = &tx.tx {
            match &value.message {
                WincodeArchiveV2NoRegistryMessage::Legacy(message) => {
                    for pubkey in &message.account_keys {
                        visit(*pubkey);
                    }
                }
                WincodeArchiveV2NoRegistryMessage::V0(message) => {
                    for pubkey in &message.account_keys {
                        visit(*pubkey);
                    }
                    for lookup in &message.address_table_lookups {
                        visit(lookup.account_key);
                    }
                }
            }
        }
        if let Some(WincodeArchiveV2Payload::Decoded { value, .. }) = tx.metadata.as_ref() {
            for pubkey in &value.loaded_writable_addresses {
                visit(*pubkey);
            }
            for pubkey in &value.loaded_readonly_addresses {
                visit(*pubkey);
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
                    visit(pubkey);
                }
            }
            for reward in &value.rewards {
                visit(reward.pubkey);
            }
            if let Some(return_data) = value.return_data.as_ref() {
                visit(return_data.program_id);
            }
        }
    }
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

pub(crate) fn convert_grpc_block(
    block: &SubscribeUpdateBlock,
    block_id: u32,
) -> Result<ConvertedGrpcBlock> {
    let blockhash = decode_required_hash_string(&block.blockhash)?;
    let mut entries = block
        .entries
        .iter()
        .map(|entry| {
            let hash = bytes32(&entry.hash).with_context(|| {
                format!(
                    "slot {} entry {} had invalid PoH hash length {}",
                    entry.slot,
                    entry.index,
                    entry.hash.len()
                )
            })?;
            Ok((
                entry.index,
                CompactPohEntry {
                    num_hashes: entry.num_hashes,
                    hash,
                    tx_count: u32::try_from(entry.executed_transaction_count).with_context(
                        || {
                            format!(
                                "slot {} entry {} tx count exceeds u32::MAX",
                                entry.slot, entry.index
                            )
                        },
                    )?,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by_key(|(index, _entry)| *index);
    let poh_entries = entries
        .into_iter()
        .map(|(_index, entry)| entry)
        .collect::<Vec<_>>();

    let txs = block
        .transactions
        .iter()
        .map(|tx| {
            let transaction = tx.transaction.as_ref().with_context(|| {
                format!(
                    "slot {} tx {} missing transaction payload",
                    block.slot, tx.index
                )
            })?;
            let source_len = transaction.encoded_len() as u64;
            let metadata = tx
                .meta
                .as_ref()
                .map(|meta| {
                    Ok::<_, anyhow::Error>(WincodeArchiveV2Payload::Decoded {
                        source_len: meta.encoded_len() as u64,
                        value: convert_grpc_meta(meta)?,
                    })
                })
                .transpose()?;

            Ok(WincodeArchiveV2NoRegistryTransaction {
                tx_index: u32::try_from(tx.index).with_context(|| {
                    format!("slot {} tx index {} exceeds u32::MAX", block.slot, tx.index)
                })?,
                tx: WincodeArchiveV2Payload::Decoded {
                    source_len,
                    value: convert_grpc_transaction(transaction)?,
                },
                metadata,
            })
        })
        .collect::<Result<Vec<_>>>()?;

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

    Ok(ConvertedGrpcBlock {
        block: WincodeArchiveV2NoRegistryBlock {
            header: WincodeArchiveV2NoRegistryBlockHeader {
                compact: header,
                rewards: block
                    .rewards
                    .as_ref()
                    .map(convert_grpc_rewards)
                    .transpose()?,
            },
            txs,
        },
        poh_entries,
        blockhash,
        transaction_count,
        missing,
    })
}

fn convert_grpc_transaction(tx: &GrpcTransaction) -> Result<WincodeArchiveV2NoRegistryTx> {
    let message = tx.message.as_ref().context("transaction missing message")?;
    let converted_message = convert_grpc_message(message)?;
    Ok(WincodeArchiveV2NoRegistryTx {
        signatures: tx.signatures.clone(),
        message: converted_message,
    })
}

fn convert_grpc_message(message: &GrpcMessage) -> Result<WincodeArchiveV2NoRegistryMessage> {
    let header = message.header.as_ref().context("message missing header")?;
    let header = CompactMessageHeader {
        num_required_signatures: u8::try_from(header.num_required_signatures)
            .context("num_required_signatures exceeds u8::MAX")?,
        num_readonly_signed_accounts: u8::try_from(header.num_readonly_signed_accounts)
            .context("num_readonly_signed_accounts exceeds u8::MAX")?,
        num_readonly_unsigned_accounts: u8::try_from(header.num_readonly_unsigned_accounts)
            .context("num_readonly_unsigned_accounts exceeds u8::MAX")?,
    };
    let account_keys = message
        .account_keys
        .iter()
        .map(|key| bytes32(key))
        .collect::<Result<Vec<_>>>()?;
    let recent_blockhash = bytes32(&message.recent_blockhash)?;
    let instructions = message
        .instructions
        .iter()
        .map(|ix| {
            Ok(WincodeArchiveV2NoRegistryInstruction {
                program_id_index: u8::try_from(ix.program_id_index)
                    .context("program_id_index exceeds u8::MAX")?,
                accounts: ix.accounts.clone(),
                data: ix.data.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if message.versioned {
        let address_table_lookups = message
            .address_table_lookups
            .iter()
            .map(|lookup| {
                Ok(WincodeArchiveV2NoRegistryAddressTableLookup {
                    account_key: bytes32(&lookup.account_key)?,
                    writable_indexes: lookup.writable_indexes.clone(),
                    readonly_indexes: lookup.readonly_indexes.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
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

fn convert_grpc_meta(meta: &GrpcTransactionStatusMeta) -> Result<WincodeArchiveV2NoRegistryMeta> {
    let inner_instructions = if meta.inner_instructions_none {
        None
    } else {
        Some(
            meta.inner_instructions
                .iter()
                .map(|inner| {
                    Ok(CompactInnerInstructions {
                        index: inner.index,
                        instructions: inner
                            .instructions
                            .iter()
                            .map(|ix| CompactInnerInstruction {
                                program_id_index: ix.program_id_index,
                                accounts: ix.accounts.clone(),
                                data: ix.data.clone(),
                                stack_height: ix.stack_height,
                            })
                            .collect(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )
    };

    Ok(WincodeArchiveV2NoRegistryMeta {
        err: meta
            .err
            .as_ref()
            .map(|err| CompactTransactionError::from_stored_wincode_bytes(&err.err))
            .transpose()?,
        fee: meta.fee,
        pre_balances: meta.pre_balances.clone(),
        post_balances: meta.post_balances.clone(),
        inner_instructions,
        logs: if meta.log_messages_none {
            None
        } else {
            Some(meta.log_messages.clone())
        },
        pre_token_balances: meta
            .pre_token_balances
            .iter()
            .map(convert_grpc_token_balance)
            .collect::<Result<Vec<_>>>()?,
        post_token_balances: meta
            .post_token_balances
            .iter()
            .map(convert_grpc_token_balance)
            .collect::<Result<Vec<_>>>()?,
        rewards: meta
            .rewards
            .iter()
            .map(convert_grpc_reward)
            .collect::<Result<Vec<_>>>()?,
        loaded_writable_addresses: meta
            .loaded_writable_addresses
            .iter()
            .map(|address| bytes32(address))
            .collect::<Result<Vec<_>>>()?,
        loaded_readonly_addresses: meta
            .loaded_readonly_addresses
            .iter()
            .map(|address| bytes32(address))
            .collect::<Result<Vec<_>>>()?,
        return_data: meta
            .return_data
            .as_ref()
            .map(|data| {
                Ok::<_, anyhow::Error>(WincodeArchiveV2NoRegistryReturnData {
                    program_id: bytes32(&data.program_id)?,
                    data: data.data.clone(),
                })
            })
            .transpose()?,
        compute_units_consumed: meta.compute_units_consumed,
        cost_units: meta.cost_units,
    })
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
) -> Result<WincodeArchiveV2NoRegistryTokenBalance> {
    let ui = balance.ui_token_amount.as_ref();
    Ok(WincodeArchiveV2NoRegistryTokenBalance {
        account_index: balance.account_index,
        mint: decode_optional_pubkey_string(&balance.mint)?,
        owner: decode_optional_pubkey_string(&balance.owner)?,
        program_id: decode_optional_pubkey_string(&balance.program_id)?,
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
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode blockhash {value}"))?;
    bytes32(&bytes).map(Some)
}

fn decode_required_hash_string(value: &str) -> Result<[u8; 32]> {
    decode_hash_string(value)?.context("missing blockhash")
}

fn decode_optional_pubkey_string(value: &str) -> Result<Option<[u8; 32]>> {
    if value.is_empty() {
        return Ok(None);
    }
    decode_required_pubkey_string(value).map(Some)
}

fn decode_required_pubkey_string(value: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode pubkey {value}"))?;
    bytes32(&bytes)
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
