use ahash::{AHashMap, AHashSet};
use anyhow::{Context, Result, anyhow};
use blockzilla::{
    car_block_reader::{CarBlock, CarBlockReader},
    confirmed_block::TransactionStatusMeta,
    node::{Node, TransactionNode},
    open_epoch::{self, FetchMode},
    transaction_parser::VersionedTransaction,
};
use cid::Cid;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use prost::Message;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::fmt::Write as FmtWrite;
use std::io::Write as IoWrite;
use std::io::{ErrorKind, IsTerminal, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs;
use wincode::Deserialize as _;
use wincode::{SchemaRead, SchemaWrite};

use crate::LOG_INTERVAL_SECS;

const BLOCK_LOG_CHUNK: u64 = 500;

#[derive(Clone)]
#[allow(dead_code)]
struct BlockProgramUsage {
    pub epoch: u64,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub records: Vec<ProgramUsageRecord>,
}

struct ProgramStatsCollection {
    pub aggregated: Vec<ProgramUsageRecord>,
    #[allow(dead_code)]
    pub per_block: Option<Vec<BlockProgramUsage>>,
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
    pub per_block: Option<Vec<BlockProgramUsage>>,
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

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct ProgramUsageRecord {
    pub program: [u8; 32],
    pub stats: ProgramUsageStats,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct ProgramUsageEpochPartV1 {
    pub epoch: u64,
    pub last_slot: u64,
    pub records: Vec<ProgramUsageRecord>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Serialize, Deserialize)]
struct ProgramUsageEpochPart {
    pub epoch: u64,
    pub last_slot: u64,
    pub last_blocktime: Option<i64>,
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
    pub aggregated: Vec<ProgramUsageRecord>,
    pub epochs: Vec<ProgramUsageEpochPart>,
}

const PROGRAM_USAGE_EXPORT_VERSION: u32 = 1;
const PROGRESS_FILE_NAME: &str = "progress.bin";

fn decode_epoch_part(bytes: &[u8]) -> Result<ProgramUsageEpochPart> {
    if let Ok(part) = wincode::deserialize::<ProgramUsageEpochPart>(bytes) {
        return Ok(part);
    }

    let legacy = wincode::deserialize::<ProgramUsageEpochPartV1>(bytes)?;
    Ok(ProgramUsageEpochPart {
        epoch: legacy.epoch,
        last_slot: legacy.last_slot,
        last_blocktime: None,
        records: legacy.records,
    })
}

async fn load_progress(parts_dir: &Path) -> Result<AHashSet<u64>> {
    let path = parts_dir.join(PROGRESS_FILE_NAME);
    let mut processed = AHashSet::with_capacity(256);

    match fs::read(&path).await {
        Ok(bytes) => match wincode::deserialize::<ProgramStatsProgress>(&bytes) {
            Ok(progress) => {
                for epoch in progress.processed_epochs {
                    processed.insert(epoch);
                }
            }
            Err(err) => {
                tracing::warn!("failed to decode progress file {}: {}", path.display(), err);
            }
        },
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }

    Ok(processed)
}

async fn persist_progress(parts_dir: &Path, processed: &AHashSet<u64>) -> Result<()> {
    fs::create_dir_all(parts_dir).await?;

    let mut epochs: Vec<u64> = processed.iter().copied().collect();
    epochs.sort_unstable();
    let progress = ProgramStatsProgress {
        processed_epochs: epochs,
    };

    let encoded = wincode::serialize(&progress)?;
    let path = parts_dir.join(PROGRESS_FILE_NAME);
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, &encoded).await?;
    fs::rename(&tmp_path, &path).await?;
    Ok(())
}

async fn load_program_usage_export(input_path: &Path) -> Result<ProgramUsageExport> {
    match fs::read(input_path).await {
        Ok(data) => {
            let export: ProgramUsageExport = wincode::deserialize(&data)?;
            if export.version != PROGRAM_USAGE_EXPORT_VERSION {
                return Err(anyhow!(
                    "unsupported program usage export version {}",
                    export.version
                ));
            }
            Ok(export)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            load_program_usage_export_from_parts(input_path).await
        }
        Err(e) => Err(e.into()),
    }
}

async fn load_program_usage_export_from_parts(input_path: &Path) -> Result<ProgramUsageExport> {
    async fn collect_part_entries(parts_dir: &Path) -> Result<Option<Vec<(u64, PathBuf)>>> {
        let mut rd = match fs::read_dir(parts_dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mut entries = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }

            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(epoch_str) = name
                .strip_prefix("epoch-")
                .and_then(|s| s.strip_suffix(".bin"))
            else {
                continue;
            };
            let Ok(epoch) = epoch_str.parse::<u64>() else {
                continue;
            };

            entries.push((epoch, entry.path()));
        }

        if entries.is_empty() {
            return Ok(None);
        }

        entries.sort_unstable_by_key(|(epoch, _)| *epoch);
        Ok(Some(entries))
    }

    let parts_path = input_path.with_extension("parts");
    let parts_top_level_path = input_path.with_extension("parts.top_level");

    let (parts_dir, top_level_only, part_entries) = match collect_part_entries(&parts_path).await? {
        Some(entries) => (parts_path, false, entries),
        None => match collect_part_entries(&parts_top_level_path).await? {
            Some(entries) => (parts_top_level_path, true, entries),
            None => {
                return Err(anyhow!(
                    "no program usage export found at {} or {}",
                    parts_path.display(),
                    parts_top_level_path.display()
                ));
            }
        },
    };

    let mut aggregated: AHashMap<[u8; 32], ProgramUsageStats> = AHashMap::with_capacity(16_384);
    let mut epochs = Vec::with_capacity(part_entries.len());

    for (expected_epoch, path) in part_entries {
        let data = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(
                    "failed to read cached epoch stats {}: {}",
                    path.display(),
                    e
                );
                continue;
            }
        };

        let part = match decode_epoch_part(&data) {
            Ok(part) => part,
            Err(e) => {
                tracing::warn!(
                    "failed to decode cached epoch stats {}: {}",
                    path.display(),
                    e
                );
                continue;
            }
        };

        for record in part.records.iter() {
            aggregated
                .entry(record.program)
                .or_insert_with(ProgramUsageStats::default)
                .accumulate(&record.stats);
        }

        if part.epoch != expected_epoch {
            tracing::warn!(
                "cached epoch stats {} has mismatched epoch {} (expected {})",
                path.display(),
                part.epoch,
                expected_epoch
            );
        }

        epochs.push(part);
    }

    if epochs.is_empty() {
        return Err(anyhow!(
            "no program usage export parts found in {}",
            parts_dir.display()
        ));
    }

    epochs.sort_unstable_by_key(|part| part.epoch);

    let mut aggregated_records: Vec<ProgramUsageRecord> = aggregated
        .into_iter()
        .map(|(program, stats)| ProgramUsageRecord { program, stats })
        .collect();
    aggregated_records
        .sort_unstable_by(|a, b| b.stats.instruction_count.cmp(&a.stats.instruction_count));

    let processed_epochs_set = load_progress(&parts_dir).await?;
    let mut processed_epochs: Vec<u64> = processed_epochs_set.iter().copied().collect();
    for part in epochs.iter() {
        if !processed_epochs_set.contains(&part.epoch) {
            processed_epochs.push(part.epoch);
        }
    }
    processed_epochs.sort_unstable();
    processed_epochs.dedup();

    let start_epoch = epochs.first().map(|part| part.epoch).unwrap_or(0);

    Ok(ProgramUsageExport {
        version: PROGRAM_USAGE_EXPORT_VERSION,
        start_epoch,
        start_slot: 0,
        top_level_only,
        processed_epochs,
        aggregated: aggregated_records,
        epochs,
    })
}

async fn process_epoch(
    epoch: u64,
    cache_dir: &str,
    parts_dir: &Path,
    collect_per_block: bool,
    top_level_only: bool,
    min_slot: u64,
) -> Result<EpochProcessSummary> {
    tracing::info!("opening epoch {epoch:04}");

    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
        .await
        .with_context(|| format!("failed to open epoch {epoch:04}"))?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let mut epoch_stats: AHashMap<[u8; 32], ProgramUsageStats> = AHashMap::with_capacity(4096);
    let mut epoch_last_slot = 0u64;
    let mut epoch_last_blocktime = None;

    let mut per_block_records = if collect_per_block {
        Some(Vec::new())
    } else {
        None
    };

    let epoch_start = Instant::now();
    let mut last_log = epoch_start - Duration::from_secs(LOG_INTERVAL_SECS);

    let mut metrics = EpochMetrics::default();

    let mut skipped_blocks = 0u64;
    let mut skipped_bytes = 0u64;
    let mut last_skipped_slot = 0u64;
    let mut printed_skipped_summary = false;

    let mut buf_tx = Vec::with_capacity(128 * 1024);
    let mut buf_meta = Vec::with_capacity(128 * 1024);
    let mut reusable_tx = std::mem::MaybeUninit::<VersionedTransaction>::uninit();
    let mut resolved_keys = SmallVec::<[Pubkey; 512]>::new();
    let mut tx_programs = SmallVec::<[[u8; 32]; 64]>::new();
    let mut msg_buf = String::with_capacity(256);

    while let Some(block) = car.next_block().await? {
        metrics.bytes_count += block.entries.iter().map(|entry| entry.len()).sum::<usize>() as u64;

        let mut block_stats = if collect_per_block {
            Some(AHashMap::<[u8; 32], ProgramUsageStats>::with_capacity(128))
        } else {
            None
        };

        let slot = match peek_block_slot(&block) {
            Ok(slot) => slot,
            Err(e) => {
                tracing::warn!("failed to peek block slot: {e}");
                continue;
            }
        };

        if slot < min_slot {
            skipped_blocks += 1;
            skipped_bytes += block.entries.iter().map(|entry| entry.len()).sum::<usize>() as u64;
            last_skipped_slot = slot;
            continue;
        }

        if !printed_skipped_summary && skipped_blocks > 0 {
            printed_skipped_summary = true;
            tracing::info!(
                "reached start_slot={min_slot} | skipped {} blocks ({:.2} MB), last skipped slot {last_skipped_slot}",
                skipped_blocks,
                skipped_bytes as f64 / (1024.0 * 1024.0)
            );
        }

        let block_node = match block.block() {
            Ok(node) => node,
            Err(e) => {
                tracing::warn!("failed to decode block: {e}");
                continue;
            }
        };

        if let Some(blocktime) = block_node.meta.blocktime {
            epoch_last_blocktime = Some(blocktime);
        }

        for entry_cid_res in block_node.entries.iter() {
            let entry_cid = match entry_cid_res {
                Ok(cid) => cid,
                Err(e) => {
                    tracing::warn!("failed to decode entry cid: {e}");
                    continue;
                }
            };

            let entry = match block.decode(entry_cid.hash_bytes()) {
                Ok(Node::Entry(entry)) => entry,
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!("failed to decode entry: {e}");
                    continue;
                }
            };

            for tx_cid_res in entry.transactions.iter() {
                let tx_cid = match tx_cid_res {
                    Ok(cid) => cid,
                    Err(e) => {
                        tracing::warn!("failed to decode transaction cid: {e}");
                        continue;
                    }
                };

                let tx_node = match block.decode(tx_cid.hash_bytes()) {
                    Ok(Node::Transaction(tx_node)) => tx_node,
                    Ok(_) => continue,
                    Err(e) => {
                        tracing::warn!("failed to decode transaction node: {e}");
                        continue;
                    }
                };

                let tx_bytes_slice = match transaction_bytes(&block, &tx_node, &mut buf_tx) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        tracing::warn!("failed to read transaction bytes: {e}");
                        continue;
                    }
                };

                metrics.txs_seen += 1;

                if let Err(e) =
                    VersionedTransaction::deserialize_into(tx_bytes_slice, &mut reusable_tx)
                {
                    tracing::warn!("failed to parse transaction: {e}");
                    continue;
                }

                let tx_ref = unsafe { reusable_tx.assume_init_ref() };

                resolved_keys.clear();
                resolved_keys.reserve(
                    tx_ref.message.static_account_keys().len()
                        + if top_level_only { 0 } else { 16 },
                );
                for key in tx_ref.message.static_account_keys() {
                    resolved_keys.push(Pubkey::new_from_array(*key));
                }

                let mut inner_instruction_groups = None;

                if !top_level_only {
                    if let Ok(meta_bytes) = metadata_bytes(&block, &tx_node, &mut buf_meta) {
                        if let Some(meta) = decode_transaction_meta(meta_bytes) {
                            extend_with_loaded_addresses(
                                &mut resolved_keys,
                                &meta.loaded_writable_addresses,
                            );
                            extend_with_loaded_addresses(
                                &mut resolved_keys,
                                &meta.loaded_readonly_addresses,
                            );

                            if !meta.inner_instructions_none && !meta.inner_instructions.is_empty()
                            {
                                inner_instruction_groups = Some(meta.inner_instructions);
                            }
                        }
                    }
                }

                for ix in tx_ref.message.instructions_iter() {
                    let program_idx = ix.program_id_index as usize;
                    if program_idx >= resolved_keys.len() {
                        continue;
                    }
                    let program_key = resolved_keys[program_idx].to_bytes();
                    let entry = epoch_stats
                        .entry(program_key)
                        .or_insert_with(ProgramUsageStats::default);
                    entry.tx_count += 1;
                    entry.instruction_count += 1;

                    if let Some(block_stats) = block_stats.as_mut() {
                        block_stats
                            .entry(program_key)
                            .or_insert_with(ProgramUsageStats::default)
                            .instruction_count += 1;
                    }

                    if !tx_programs.iter().any(|existing| existing == &program_key) {
                        tx_programs.push(program_key);
                    }
                    metrics.instructions_seen += 1;
                }

                if let Some(groups) = inner_instruction_groups.as_ref() {
                    for group in groups {
                        for inner_ix in group.instructions.iter() {
                            let program_idx = inner_ix.program_id_index as usize;
                            if program_idx >= resolved_keys.len() {
                                continue;
                            }
                            let program_key = resolved_keys[program_idx].to_bytes();
                            let entry = epoch_stats
                                .entry(program_key)
                                .or_insert_with(ProgramUsageStats::default);
                            entry.inner_instruction_count += 1;

                            if let Some(block_stats) = block_stats.as_mut() {
                                block_stats
                                    .entry(program_key)
                                    .or_insert_with(ProgramUsageStats::default)
                                    .inner_instruction_count += 1;
                            }

                            if !tx_programs.iter().any(|existing| existing == &program_key) {
                                tx_programs.push(program_key);
                            }
                            metrics.inner_instructions_seen += 1;
                        }
                    }
                }

                for program_key in tx_programs.iter() {
                    if let Some(block_stats) = block_stats.as_mut() {
                        block_stats
                            .entry(*program_key)
                            .or_insert_with(ProgramUsageStats::default)
                            .tx_count += 1;
                    }
                }
                tx_programs.clear();

                unsafe { std::ptr::drop_in_place(reusable_tx.as_mut_ptr()) };
            }
        }

        epoch_last_slot = slot;
        metrics.blocks_scanned += 1;

        if let (Some(block_stats), Some(per_block_records)) =
            (block_stats, per_block_records.as_mut())
        {
            let mut records: Vec<ProgramUsageRecord> = block_stats
                .into_iter()
                .map(|(program, stats)| ProgramUsageRecord { program, stats })
                .collect();
            records
                .sort_unstable_by(|a, b| b.stats.instruction_count.cmp(&a.stats.instruction_count));
            per_block_records.push(BlockProgramUsage {
                epoch,
                slot,
                block_time: block_node.meta.blocktime,
                records,
            });
        }

        let now = Instant::now();
        let time_due = now.duration_since(last_log) >= Duration::from_secs(LOG_INTERVAL_SECS);
        let chunk_due = metrics.blocks_scanned % BLOCK_LOG_CHUNK == 0;
        if time_due || chunk_due {
            last_log = now;
            let elapsed = now.duration_since(epoch_start);
            let secs = elapsed.as_secs_f64().max(1e-6);
            let blk_s = metrics.blocks_scanned as f64 / secs;
            let mb_s = (metrics.bytes_count as f64 / (1024.0 * 1024.0)) / secs;
            let tps = if elapsed.as_secs() > 0 {
                metrics.txs_seen / elapsed.as_secs()
            } else {
                0
            };

            msg_buf.clear();
            write!(
                &mut msg_buf,
                "epoch {:04} | slot={} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | instr={} inner={}",
                epoch,
                epoch_last_slot,
                metrics.blocks_scanned,
                blk_s,
                mb_s,
                tps,
                metrics.instructions_seen,
                metrics.inner_instructions_seen
            )
            .unwrap();

            tracing::info!("{}", msg_buf);
        }
    }

    fs::create_dir_all(parts_dir).await?;
    let part_path = parts_dir.join(format!("epoch-{epoch:04}.bin"));
    let tmp_path = part_path.with_extension("bin.tmp");

    let mut records: Vec<ProgramUsageRecord> = epoch_stats
        .into_iter()
        .map(|(program, stats)| ProgramUsageRecord { program, stats })
        .collect();
    records.sort_unstable_by(|a, b| b.stats.instruction_count.cmp(&a.stats.instruction_count));

    let epoch_dump = ProgramUsageEpochPart {
        epoch,
        last_slot: epoch_last_slot,
        last_blocktime: epoch_last_blocktime,
        records: records.clone(),
    };
    let encoded_epoch = wincode::serialize(&epoch_dump)?;
    fs::write(&tmp_path, &encoded_epoch).await?;
    fs::rename(&tmp_path, &part_path).await?;

    if let Some(per_block_records) = per_block_records.as_mut() {
        per_block_records.sort_by(|a, b| a.slot.cmp(&b.slot));
    }

    Ok(EpochProcessSummary {
        epoch,
        last_slot: epoch_last_slot,
        last_blocktime: epoch_last_blocktime,
        records,
        per_block: per_block_records,
        metrics,
    })
}

async fn collect_program_stats(
    start_epoch: u64,
    start_slot: u64,
    cache_dir: &str,
    output_path: &Path,
    collect_per_block: bool,
    top_level_only: bool,
) -> Result<ProgramStatsCollection> {
    let cache_path = Path::new(cache_dir);
    let Some(last_epoch) = find_last_cached_epoch(cache_path).await? else {
        return Err(anyhow!(
            "no cached epochs found in {}",
            cache_path.display()
        ));
    };

    if start_epoch > last_epoch {
        return Err(anyhow!(
            "start epoch {start_epoch:04} is beyond last cached epoch {last_epoch:04}"
        ));
    }

    let parts_dir = if top_level_only {
        output_path.with_extension("parts.top_level")
    } else {
        output_path.with_extension("parts")
    };

    let mut aggregated: AHashMap<[u8; 32], ProgramUsageStats> = AHashMap::with_capacity(16_384);
    let mut processed_epochs = load_progress(&parts_dir).await?;
    let mut epoch_summaries: AHashMap<u64, ProgramUsageEpochPart> = AHashMap::with_capacity(256);

    let mut per_block_records = if collect_per_block {
        Some(Vec::new())
    } else {
        None
    };

    let mut last_processed_slot = 0u64;
    let mut epoch_last_slots: AHashMap<u64, u64> = AHashMap::with_capacity(256);

    if let Ok(mut rd) = fs::read_dir(&parts_dir).await {
        let mut part_entries: Vec<(u64, PathBuf)> = Vec::new();
        while let Some(entry) = rd.next_entry().await? {
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(epoch_str) = name
                .strip_prefix("epoch-")
                .and_then(|s| s.strip_suffix(".bin"))
            else {
                continue;
            };
            let Ok(epoch) = epoch_str.parse::<u64>() else {
                continue;
            };
            part_entries.push((epoch, entry.path()));
        }
        part_entries.sort_unstable_by_key(|(epoch, _)| *epoch);

        for (epoch, path) in part_entries {
            if epoch < start_epoch || epoch > last_epoch {
                continue;
            }

            let data = match fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(
                        "failed to read cached epoch stats {}: {}",
                        path.display(),
                        e
                    );
                    continue;
                }
            };

            let part = match decode_epoch_part(&data) {
                Ok(part) => part,
                Err(e) => {
                    tracing::warn!(
                        "failed to decode cached epoch stats {}: {}",
                        path.display(),
                        e
                    );
                    continue;
                }
            };

            for record in part.records.iter() {
                aggregated
                    .entry(record.program)
                    .or_insert_with(ProgramUsageStats::default)
                    .accumulate(&record.stats);
            }

            processed_epochs.insert(part.epoch);
            epoch_last_slots.insert(part.epoch, part.last_slot);
            epoch_summaries.insert(part.epoch, part.clone());
            if part.epoch == start_epoch {
                last_processed_slot = part.last_slot;
            }
        }
    }

    persist_progress(&parts_dir, &processed_epochs).await?;

    let draw_target = if std::io::stdout().is_terminal() {
        ProgressDrawTarget::stderr_with_hz(8)
    } else {
        ProgressDrawTarget::hidden()
    };
    let pb = ProgressBar::with_draw_target(Some(0), draw_target);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
    pb.enable_steady_tick(Duration::from_millis(120));

    let start = Instant::now();

    let mut blocks_scanned = 0u64;
    let mut txs_seen = 0u64;
    let mut instructions_seen = 0u64;
    let mut inner_instructions_seen = 0u64;
    let mut bytes_count = 0u64;

    let mut msg_buf = String::with_capacity(256);
    write!(
        &mut msg_buf,
        "starting program stats | start_epoch={:04} start_slot={} | scanning up to {:04}",
        start_epoch, start_slot, last_epoch
    )
    .unwrap();
    pb.set_message(msg_buf.clone());

    let mut last_epoch_processed = processed_epochs.iter().copied().max();

    let start_epoch_min_slot = if processed_epochs.contains(&start_epoch) {
        0
    } else {
        start_slot
    };

    let mut epochs_to_process: Vec<(u64, u64)> = Vec::new();
    for epoch in start_epoch..=last_epoch {
        if processed_epochs.contains(&epoch) {
            if pb.is_hidden() {
                tracing::info!("skipping epoch {epoch:04} (already cached)");
            } else {
                pb.println(format!("skipping epoch {epoch:04} (already cached)"));
            }
            last_epoch_processed = Some(last_epoch_processed.map_or(epoch, |cur| cur.max(epoch)));
            continue;
        }

        let min_slot = if epoch == start_epoch {
            start_epoch_min_slot
        } else {
            0
        };
        epochs_to_process.push((epoch, min_slot));
    }

    if !epochs_to_process.is_empty() {
        for (epoch, min_slot) in epochs_to_process {
            let summary = process_epoch(
                epoch,
                cache_dir,
                &parts_dir,
                collect_per_block,
                top_level_only,
                min_slot,
            )
            .await?;

            let EpochProcessSummary {
                epoch: summary_epoch,
                last_slot: summary_last_slot,
                last_blocktime: summary_last_blocktime,
                records: summary_records,
                per_block: summary_per_block,
                metrics: summary_metrics,
            } = summary;

            bytes_count += summary_metrics.bytes_count;
            blocks_scanned += summary_metrics.blocks_scanned;
            txs_seen += summary_metrics.txs_seen;
            instructions_seen += summary_metrics.instructions_seen;
            inner_instructions_seen += summary_metrics.inner_instructions_seen;

            for record in summary_records.iter() {
                aggregated
                    .entry(record.program)
                    .or_insert_with(ProgramUsageStats::default)
                    .accumulate(&record.stats);
            }

            if let (Some(blocks), Some(per_block_records)) =
                (summary_per_block, per_block_records.as_mut())
            {
                per_block_records.extend(blocks);
            }

            processed_epochs.insert(summary_epoch);
            epoch_last_slots.insert(summary_epoch, summary_last_slot);
            epoch_summaries.insert(
                summary_epoch,
                ProgramUsageEpochPart {
                    epoch: summary_epoch,
                    last_slot: summary_last_slot,
                    last_blocktime: summary_last_blocktime,
                    records: summary_records,
                },
            );
            persist_progress(&parts_dir, &processed_epochs).await?;
            last_epoch_processed =
                Some(last_epoch_processed.map_or(summary_epoch, |cur| cur.max(summary_epoch)));

            msg_buf.clear();
            let elapsed = start.elapsed();
            let secs = elapsed.as_secs_f64().max(1e-6);
            let blk_s = blocks_scanned as f64 / secs;
            let mb_s = (bytes_count as f64 / (1024.0 * 1024.0)) / secs;
            let tps = if elapsed.as_secs() > 0 {
                txs_seen / elapsed.as_secs()
            } else {
                0
            };

            write!(
                &mut msg_buf,
                "epoch {:04} done | slot={} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | instr={} inner={}",
                summary_epoch,
                summary_last_slot,
                blocks_scanned,
                blk_s,
                mb_s,
                tps,
                instructions_seen,
                inner_instructions_seen
            )
            .unwrap();

            if pb.is_hidden() {
                tracing::info!("{}", msg_buf);
            } else {
                pb.set_message(msg_buf.clone());
                pb.tick();
            }
        }
    }

    if let Some(per_block_records) = per_block_records.as_mut() {
        per_block_records.sort_by(|a, b| match a.epoch.cmp(&b.epoch) {
            std::cmp::Ordering::Equal => a.slot.cmp(&b.slot),
            other => other,
        });
    }

    let Some(last_epoch_processed) = last_epoch_processed else {
        return Err(anyhow!(
            "no epochs processed between {start_epoch:04} and {last_epoch:04}"
        ));
    };

    if let Some(slot) = epoch_last_slots.get(&last_epoch_processed) {
        last_processed_slot = *slot;
    }

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }

    let mut aggregated_records: Vec<ProgramUsageRecord> = aggregated
        .into_iter()
        .map(|(program, stats)| ProgramUsageRecord { program, stats })
        .collect();
    aggregated_records
        .sort_unstable_by(|a, b| b.stats.instruction_count.cmp(&a.stats.instruction_count));

    let elapsed = start.elapsed();
    let secs = elapsed.as_secs_f64().max(1e-6);
    let blk_s = blocks_scanned as f64 / secs;
    let mb_s = (bytes_count as f64 / (1024.0 * 1024.0)) / secs;
    let tps = if elapsed.as_secs() > 0 {
        txs_seen / elapsed.as_secs()
    } else {
        0
    };

    let range_label = if start_epoch == last_epoch_processed {
        format!("epoch {start_epoch:04}")
    } else {
        format!("epochs {start_epoch:04}-{last_epoch_processed:04}")
    };

    let final_line = format!(
        "{range_label} | slot={} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | instr={} inner={} txs={}",
        last_processed_slot,
        blocks_scanned,
        blk_s,
        mb_s,
        tps,
        instructions_seen,
        inner_instructions_seen,
        txs_seen
    );

    if pb.is_hidden() {
        tracing::info!("{final_line}");
    } else {
        pb.finish_and_clear();
        eprintln!("{final_line}");
    }

    let mut per_epoch: Vec<ProgramUsageEpochPart> = epoch_summaries.into_values().collect();
    per_epoch.sort_unstable_by_key(|part| part.epoch);

    let mut processed_epochs_list: Vec<u64> = processed_epochs.into_iter().collect();
    processed_epochs_list.sort_unstable();

    Ok(ProgramStatsCollection {
        aggregated: aggregated_records,
        per_block: per_block_records,
        per_epoch,
        processed_epochs: processed_epochs_list,
    })
}

pub async fn dump_program_stats(
    start_epoch: u64,
    start_slot: u64,
    cache_dir: &str,
    output_path: &Path,
    limit: Option<usize>,
    top_level_only: bool,
) -> Result<()> {
    let collection = collect_program_stats(
        start_epoch,
        start_slot,
        cache_dir,
        output_path,
        false,
        top_level_only,
    )
    .await?;

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }

    let mut aggregated = collection.aggregated;
    let limit = limit.unwrap_or(aggregated.len());
    aggregated.truncate(limit);

    let program_order = aggregated
        .iter()
        .enumerate()
        .map(|(idx, record)| (record.program, idx))
        .collect::<AHashMap<_, _>>();

    let mut per_epoch = collection.per_epoch;
    for epoch in per_epoch.iter_mut() {
        epoch
            .records
            .retain(|record| program_order.contains_key(&record.program));
        epoch.records.sort_unstable_by_key(|record| {
            program_order
                .get(&record.program)
                .copied()
                .unwrap_or(usize::MAX)
        });
    }

    let export = ProgramUsageExport {
        version: PROGRAM_USAGE_EXPORT_VERSION,
        start_epoch,
        start_slot,
        top_level_only,
        processed_epochs: collection.processed_epochs,
        aggregated,
        epochs: per_epoch,
    };

    let encoded = wincode::serialize(&export)?;
    let tmp_path = output_path.with_extension("tmp");
    fs::write(&tmp_path, &encoded).await?;
    fs::rename(&tmp_path, output_path).await?;

    Ok(())
}

pub async fn dump_program_stats_csv(
    input_path: &Path,
    output_path: &Path,
    limit: Option<usize>,
) -> Result<()> {
    let export = load_program_usage_export(input_path)
        .await
        .with_context(|| {
            format!(
                "failed to load program usage export {}",
                input_path.display()
            )
        })?;

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }

    let mut aggregated = export.aggregated;
    let limit = limit.unwrap_or(aggregated.len());
    aggregated.truncate(limit);

    let program_keys = aggregated
        .iter()
        .map(|record| record.program)
        .collect::<Vec<_>>();
    let program_labels = aggregated
        .iter()
        .map(|record| bs58::encode(record.program).into_string())
        .collect::<Vec<_>>();

    let mut csv_bytes = Vec::with_capacity(export.epochs.len() * (program_keys.len() + 3) * 8);
    let mut line_buf = String::new();

    line_buf.clear();
    line_buf.push_str("epoch,last_slot,block_time");
    for program_str in program_labels.iter() {
        line_buf.push(',');
        line_buf.push_str(program_str);
    }
    line_buf.push('\n');
    csv_bytes.write_all(line_buf.as_bytes())?;

    for epoch in export.epochs.iter() {
        line_buf.clear();
        let block_time = epoch
            .last_blocktime
            .map(|ts| ts.to_string())
            .unwrap_or_default();
        write!(
            &mut line_buf,
            "{},{},{}",
            epoch.epoch, epoch.last_slot, block_time
        )
        .unwrap();

        let mut totals = AHashMap::with_capacity(epoch.records.len());
        for record in epoch.records.iter() {
            totals.insert(record.program, record.stats.tx_count);
        }

        for program in program_keys.iter() {
            let count = totals.get(program).copied().unwrap_or(0);
            write!(&mut line_buf, ",{}", count).unwrap();
        }

        line_buf.push('\n');
        csv_bytes.write_all(line_buf.as_bytes())?;
    }

    fs::write(output_path, csv_bytes).await?;

    Ok(())
}

async fn find_last_cached_epoch(cache_dir: &Path) -> Result<Option<u64>> {
    let mut rd = match fs::read_dir(cache_dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut max_epoch = None;
    while let Some(entry) = rd.next_entry().await? {
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };

        let Some(epoch_str) = name.strip_prefix("epoch-").and_then(|s| {
            s.strip_suffix(".car")
                .or_else(|| s.strip_suffix(".car.zst"))
        }) else {
            continue;
        };

        let Ok(epoch) = epoch_str.parse::<u64>() else {
            continue;
        };

        max_epoch = Some(max_epoch.map_or(epoch, |current: u64| current.max(epoch)));
    }

    Ok(max_epoch)
}

#[inline]
fn peek_block_slot(block: &CarBlock) -> Result<u64> {
    let mut decoder = minicbor::Decoder::new(block.block_bytes.as_ref());
    decoder
        .array()
        .map_err(|e| anyhow!("failed to decode block header array: {e}"))?;
    let kind = decoder
        .u64()
        .map_err(|e| anyhow!("failed to decode block kind: {e}"))?;
    if kind != 2 {
        return Err(anyhow!("unexpected block kind {kind}, expected Block"));
    }
    let slot = decoder
        .u64()
        .map_err(|e| anyhow!("failed to decode block slot: {e}"))?;
    Ok(slot)
}

#[inline]
fn copy_dataframe_into<'a>(
    block: &CarBlock,
    cid: &Cid,
    dst: &'a mut Vec<u8>,
    inline_prefix: Option<&[u8]>,
) -> Result<&'a [u8]> {
    let prefix_len = inline_prefix.map_or(0, |p| p.len());
    dst.clear();
    dst.reserve(prefix_len + (64 << 10));

    if let Some(prefix) = inline_prefix {
        dst.extend_from_slice(prefix);
    }

    let mut rdr = block.dataframe_reader(cid);
    rdr.read_to_end(dst)
        .map_err(|e| anyhow!("read dataframe chain: {e}"))?;
    Ok(&*dst)
}

#[inline]
fn transaction_bytes<'a>(
    block: &CarBlock,
    tx_node: &TransactionNode<'_>,
    buf: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    match tx_node.data.next {
        None => {
            buf.clear();
            buf.extend_from_slice(tx_node.data.data);
            Ok(&*buf)
        }
        Some(df_cbor) => {
            let df_cid = df_cbor.to_cid()?;
            copy_dataframe_into(block, &df_cid, buf, None)
        }
    }
}

#[inline]
fn metadata_bytes<'a>(
    block: &CarBlock,
    tx_node: &TransactionNode<'a>,
    buf: &'a mut Vec<u8>,
) -> Result<&'a [u8]> {
    match tx_node.metadata.next {
        None => Ok(tx_node.metadata.data),
        Some(df_cbor) => {
            let df_cid = df_cbor.to_cid()?;
            copy_dataframe_into(block, &df_cid, buf, None)
        }
    }
}

fn extend_with_loaded_addresses(dest: &mut SmallVec<[Pubkey; 512]>, addrs: &[Vec<u8>]) {
    for addr in addrs {
        if addr.len() == 32 {
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(addr);
            dest.push(Pubkey::new_from_array(bytes));
        } else {
            tracing::warn!(
                "loaded address has invalid length: expected 32 bytes, got {}",
                addr.len()
            );
        }
    }
}

fn decode_transaction_meta(bytes: &[u8]) -> Option<TransactionStatusMeta> {
    if bytes.is_empty() {
        return None;
    }

    let raw = if is_zstd(bytes) {
        match zstd::decode_all(bytes) {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!("failed to decompress metadata: {e}");
                return None;
            }
        }
    } else {
        bytes.to_vec()
    };

    match TransactionStatusMeta::decode(raw.as_slice()) {
        Ok(meta) => Some(meta),
        Err(e) => {
            tracing::warn!("failed to decode TransactionStatusMeta protobuf: {e}");
            None
        }
    }
}

#[inline]
fn is_zstd(buf: &[u8]) -> bool {
    buf.get(0..4)
        .map(|m| m == [0x28, 0xB5, 0x2F, 0xFD])
        .unwrap_or(false)
}
