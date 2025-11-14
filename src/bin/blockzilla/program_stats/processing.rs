use super::{
    BlockProgramUsage, EpochMetrics, EpochProcessSummary, PROGRAM_USAGE_EXPORT_VERSION,
    PerBlockStats, ProgramStatsCollection, ProgramUsageEpochPart, ProgramUsageExport,
    ProgramUsagePartsPreference, ProgramUsageRecord, ProgramUsageStats, Result, reading,
};
use crate::LOG_INTERVAL_SECS;
use ahash::{AHashMap, AHashSet};
use anyhow::{Context, anyhow};
use blockzilla::{
    car_block_reader::{CarBlock, CarBlockReader},
    node::Node,
    open_epoch::{self, FetchMode},
    transaction_parser::VersionedTransaction,
};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::fmt::Write as FmtWrite;
use std::io::{IsTerminal, Write as IoWrite};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::sync::{Mutex, mpsc};
use tokio::task::{JoinHandle, JoinSet};
use wincode::Deserialize as _;

fn emit_progress(pb: &ProgressBar, message: String, tick: bool) {
    if pb.is_hidden() {
        tracing::info!("{message}");
    } else {
        pb.set_message(message);
        if tick {
            pb.tick();
        }
    }
}

async fn process_epoch(
    epoch: u64,
    cache_dir: &str,
    parts_dir: &Path,
    collect_per_block: bool,
    top_level_only: bool,
    min_slot: u64,
    pb: &ProgressBar,
) -> Result<EpochProcessSummary> {
    let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
        .await
        .with_context(|| format!("failed to open epoch {epoch:04}"))?;
    let mut car = CarBlockReader::new(reader);
    car.read_header().await?;

    let mut epoch_stats: AHashMap<[u8; 32], ProgramUsageStats> = AHashMap::with_capacity(4096);
    let mut epoch_last_slot = 0u64;
    let mut epoch_last_blocktime = None;

    let mut per_block_stats = if collect_per_block {
        Some(PerBlockStats {
            blocks: Vec::new(),
            records: Vec::new(),
        })
    } else {
        None
    };

    // Reused per-block map
    let mut per_block_map = if collect_per_block {
        Some(AHashMap::<[u8; 32], ProgramUsageStats>::with_capacity(128))
    } else {
        None
    };

    let epoch_start = Instant::now();
    let mut last_log = epoch_start;

    let mut metrics = EpochMetrics::default();

    let mut buf_tx = Vec::with_capacity(128 * 1024);
    // buffer used only for dataframe reader for metadata
    let mut buf_meta_df = Vec::with_capacity(64 * 1024);
    // scratch buffer for zstd/prost decoding
    let mut meta_scratch = Vec::with_capacity(64 * 1024);
    // raw 32-byte keys (static + loaded)
    let mut resolved_keys = Vec::<[u8; 32]>::with_capacity(512);
    let mut msg_buf = String::with_capacity(256);

    // Error counters (log once at end instead of spamming)
    let mut decode_errors = 0u64;

    while let Some(block) = car.next_block().await? {
        let now = Instant::now();
        if now.duration_since(last_log) >= Duration::from_secs(LOG_INTERVAL_SECS) {
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
                "E{:04} | {} blk | {:.0} blk/s | {:.1} MB/s | {} TPS | {} programs",
                epoch,
                metrics.blocks_scanned,
                blk_s,
                mb_s,
                tps,
                epoch_stats.len()
            )
            .unwrap();
            emit_progress(pb, msg_buf.clone(), true);
        }

        // Compute block size once
        let block_bytes = block.entries.iter().map(|entry| entry.len()).sum::<usize>() as u64;
        metrics.bytes_count += block_bytes;

        if let Some(map) = per_block_map.as_mut() {
            map.clear();
        }

        let slot = match reading::peek_block_slot(&block) {
            Ok(slot) => slot,
            Err(_) => {
                decode_errors += 1;
                continue;
            }
        };

        if slot < min_slot {
            continue;
        }

        let block_node = match block.block() {
            Ok(node) => node,
            Err(_) => {
                decode_errors += 1;
                continue;
            }
        };

        if let Some(blocktime) = block_node.meta.blocktime {
            epoch_last_blocktime = Some(blocktime);
        }

        for entry_cid_res in block_node.entries.iter() {
            let entry_cid = match entry_cid_res {
                Ok(cid) => cid,
                Err(_) => {
                    decode_errors += 1;
                    continue;
                }
            };

            let entry = match block.decode(entry_cid.hash_bytes()) {
                Ok(Node::Entry(entry)) => entry,
                Ok(_) => continue,
                Err(_) => {
                    decode_errors += 1;
                    continue;
                }
            };

            for tx_cid_res in entry.transactions.iter() {
                let tx_cid = match tx_cid_res {
                    Ok(cid) => cid,
                    Err(_) => {
                        decode_errors += 1;
                        continue;
                    }
                };

                let tx_node = match block.decode(tx_cid.hash_bytes()) {
                    Ok(Node::Transaction(tx_node)) => tx_node,
                    Ok(_) => continue,
                    Err(_) => {
                        decode_errors += 1;
                        continue;
                    }
                };

                let tx_bytes_slice = match reading::transaction_bytes(&block, &tx_node, &mut buf_tx)
                {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        decode_errors += 1;
                        continue;
                    }
                };

                metrics.txs_seen += 1;

                let tx = match VersionedTransaction::deserialize(tx_bytes_slice) {
                    Ok(tx) => tx,
                    Err(_) => {
                        decode_errors += 1;
                        continue;
                    }
                };

                let message = &tx.message;

                // tx_count: +1 per static account key
                for key in message.static_account_keys() {
                    let program_key: [u8; 32] = *key;
                    let entry = epoch_stats
                        .entry(program_key)
                        .or_insert_with(ProgramUsageStats::default);
                    entry.tx_count += 1;

                    if let Some(map) = per_block_map.as_mut() {
                        map.entry(program_key)
                            .or_insert_with(ProgramUsageStats::default)
                            .tx_count += 1;
                    }
                }

                // Build resolved_keys: static only by default
                let static_keys = message.static_account_keys();
                resolved_keys.clear();
                resolved_keys.reserve(static_keys.len() + if top_level_only { 0 } else { 16 });
                resolved_keys.extend_from_slice(static_keys);

                if top_level_only {
                    // Top level only, no metadata, no inner instructions
                    for ix in message.instructions_iter() {
                        let program_idx = ix.program_id_index as usize;
                        if program_idx >= resolved_keys.len() {
                            continue;
                        }
                        let program_key = resolved_keys[program_idx];

                        let entry = epoch_stats
                            .entry(program_key)
                            .or_insert_with(ProgramUsageStats::default);
                        entry.instruction_count += 1;

                        if let Some(map) = per_block_map.as_mut() {
                            map.entry(program_key)
                                .or_insert_with(ProgramUsageStats::default)
                                .instruction_count += 1;
                        }

                        metrics.instructions_seen += 1;
                    }
                } else {
                    // Full mode: load metadata, extend loaded addresses, count inner
                    let inner_instruction_groups =
                        match reading::metadata_bytes(&block, &tx_node, &mut buf_meta_df) {
                            Ok(meta_bytes) => {
                                reading::decode_transaction_meta(meta_bytes, &mut meta_scratch)
                                    .and_then(|meta| {
                                        reading::extend_with_loaded_addresses(
                                            &mut resolved_keys,
                                            &meta.loaded_writable_addresses,
                                        );
                                        reading::extend_with_loaded_addresses(
                                            &mut resolved_keys,
                                            &meta.loaded_readonly_addresses,
                                        );

                                        if !meta.inner_instructions_none
                                            && !meta.inner_instructions.is_empty()
                                        {
                                            Some(meta.inner_instructions)
                                        } else {
                                            None
                                        }
                                    })
                            }
                            Err(_) => None,
                        };

                    // Top-level
                    for ix in message.instructions_iter() {
                        let program_idx = ix.program_id_index as usize;
                        if program_idx >= resolved_keys.len() {
                            continue;
                        }
                        let program_key = resolved_keys[program_idx];

                        let entry = epoch_stats
                            .entry(program_key)
                            .or_insert_with(ProgramUsageStats::default);
                        entry.instruction_count += 1;

                        if let Some(map) = per_block_map.as_mut() {
                            map.entry(program_key)
                                .or_insert_with(ProgramUsageStats::default)
                                .instruction_count += 1;
                        }

                        metrics.instructions_seen += 1;
                    }

                    // Inner
                    if let Some(groups) = inner_instruction_groups.as_ref() {
                        for group in groups {
                            for inner_ix in group.instructions.iter() {
                                let program_idx = inner_ix.program_id_index as usize;
                                if program_idx >= resolved_keys.len() {
                                    continue;
                                }
                                let program_key = resolved_keys[program_idx];

                                let entry = epoch_stats
                                    .entry(program_key)
                                    .or_insert_with(ProgramUsageStats::default);
                                entry.inner_instruction_count += 1;

                                if let Some(map) = per_block_map.as_mut() {
                                    map.entry(program_key)
                                        .or_insert_with(ProgramUsageStats::default)
                                        .inner_instruction_count += 1;
                                }

                                metrics.inner_instructions_seen += 1;
                            }
                        }
                    }
                }
            }
        }

        epoch_last_slot = slot;
        metrics.blocks_scanned += 1;

        if metrics.blocks_scanned % 1000 == 0 {
            if buf_tx.capacity() > 256 * 1024 {
                buf_tx.shrink_to(128 * 1024);
            }
            if buf_meta_df.capacity() > 128 * 1024 {
                buf_meta_df.shrink_to(64 * 1024);
            }
            if meta_scratch.capacity() > 128 * 1024 {
                meta_scratch.shrink_to(64 * 1024);
            }
            if resolved_keys.capacity() > 1024 {
                resolved_keys.shrink_to(512);
            }
            if let Some(map) = per_block_map.as_mut() {
                if map.capacity() > 8192 {
                    map.shrink_to(4096);
                }
            }
        }

        if let (Some(map), Some(per_block_stats)) =
            (per_block_map.as_ref(), per_block_stats.as_mut())
        {
            let start = per_block_stats.records.len();
            per_block_stats
                .records
                .extend(map.iter().map(|(program, stats)| ProgramUsageRecord {
                    program: *program,
                    stats: *stats,
                }));
            let end = per_block_stats.records.len();

            per_block_stats.blocks.push(BlockProgramUsage {
                epoch,
                slot,
                block_time: block_node.meta.blocktime,
                start,
                len: end - start,
            });
        }
    }

    if decode_errors > 0 {
        tracing::warn!("epoch {epoch:04} had {decode_errors} decode errors");
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
        records,
    };
    let encoded_epoch = wincode::serialize(&epoch_dump)?;
    fs::write(&tmp_path, &encoded_epoch).await?;
    fs::rename(&tmp_path, &part_path).await?;

    let ProgramUsageEpochPart { records, .. } = epoch_dump;

    if let Some(per_block_stats) = per_block_stats.as_mut() {
        per_block_stats.blocks.sort_by(|a, b| a.slot.cmp(&b.slot));
    }

    Ok(EpochProcessSummary {
        epoch,
        last_slot: epoch_last_slot,
        last_blocktime: epoch_last_blocktime,
        records,
        per_block: per_block_stats,
        metrics,
    })
}

struct BlockProcessResult {
    pub slot: Option<u64>,
    pub block_time: Option<i64>,
    pub stats: Vec<([u8; 32], ProgramUsageStats)>,
    pub metrics: EpochMetrics,
    pub decode_errors: u64,
}

async fn consume_epoch_results(
    epoch: u64,
    parts_dir: PathBuf,
    mut rx: mpsc::Receiver<BlockProcessResult>,
) -> Result<EpochProcessSummary> {
    let mut epoch_stats: AHashMap<[u8; 32], ProgramUsageStats> = AHashMap::with_capacity(4096);
    let mut metrics = EpochMetrics::default();
    let mut epoch_last_slot = 0u64;
    let mut epoch_last_blocktime: Option<i64> = None;
    let mut decode_errors_total = 0u64;

    while let Some(block) = rx.recv().await {
        metrics.blocks_scanned += block.metrics.blocks_scanned;
        metrics.txs_seen += block.metrics.txs_seen;
        metrics.instructions_seen += block.metrics.instructions_seen;
        metrics.inner_instructions_seen += block.metrics.inner_instructions_seen;
        metrics.bytes_count += block.metrics.bytes_count;

        decode_errors_total += block.decode_errors;

        if let Some(slot) = block.slot {
            if slot > epoch_last_slot {
                epoch_last_slot = slot;
            }
        }

        if let Some(block_time) = block.block_time {
            epoch_last_blocktime =
                Some(epoch_last_blocktime.map_or(block_time, |cur| cur.max(block_time)));
        }

        for (program, stats) in block.stats.into_iter() {
            epoch_stats
                .entry(program)
                .or_insert_with(ProgramUsageStats::default)
                .accumulate(&stats);
        }
    }

    if decode_errors_total > 0 {
        tracing::warn!("epoch {epoch:04} had {decode_errors_total} decode errors (parallel)");
    }

    fs::create_dir_all(&parts_dir).await?;
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
        records,
    };
    let encoded_epoch = wincode::serialize(&epoch_dump)?;
    fs::write(&tmp_path, &encoded_epoch).await?;
    fs::rename(&tmp_path, &part_path).await?;

    let ProgramUsageEpochPart { records, .. } = epoch_dump;

    Ok(EpochProcessSummary {
        epoch,
        last_slot: epoch_last_slot,
        last_blocktime: epoch_last_blocktime,
        records,
        per_block: None,
        metrics,
    })
}

async fn process_epoch_par(
    epoch: u64,
    cache_dir: &str,
    parts_dir: &Path,
    top_level_only: bool,
    min_slot: u64,
    jobs: usize,
) -> Result<EpochProcessSummary> {
    let (block_tx, block_rx) = mpsc::channel::<CarBlock>(jobs * 2);
    let block_rx = Arc::new(Mutex::new(block_rx));
    let (result_tx, result_rx) = mpsc::channel::<BlockProcessResult>(jobs * 2);

    let consumer_handle = tokio::spawn(consume_epoch_results(
        epoch,
        parts_dir.to_path_buf(),
        result_rx,
    ));

    let mut handles: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(jobs);

    for _ in 0..jobs {
        let rx = Arc::clone(&block_rx);
        let result_tx = result_tx.clone();
        let worker_top_level_only = top_level_only;
        let handle = tokio::spawn(async move {
            let mut buf_tx = Vec::with_capacity(128 * 1024);
            let mut buf_meta_df = Vec::with_capacity(64 * 1024);
            let mut meta_scratch = Vec::with_capacity(64 * 1024);
            let mut resolved_keys = Vec::<[u8; 32]>::with_capacity(512);
            let mut block_stats: AHashMap<[u8; 32], ProgramUsageStats> =
                AHashMap::with_capacity(4096);
            let mut stats_vec = Vec::with_capacity(4096);
            let mut block_count = 0u64;

            loop {
                let block = {
                    let mut guard = rx.lock().await;
                    match guard.recv().await {
                        Some(b) => b,
                        None => break,
                    }
                };

                block_stats.clear();

                let block_bytes =
                    block.entries.iter().map(|entry| entry.len()).sum::<usize>() as u64;
                let mut block_metrics = EpochMetrics {
                    bytes_count: block_bytes,
                    ..EpochMetrics::default()
                };
                let mut block_decode_errors = 0u64;
                let mut block_time: Option<i64> = None;

                let slot = match reading::peek_block_slot(&block) {
                    Ok(slot) => slot,
                    Err(_) => {
                        block_decode_errors += 1;
                        result_tx
                            .send(BlockProcessResult {
                                slot: None,
                                block_time: None,
                                stats: Vec::new(),
                                metrics: block_metrics,
                                decode_errors: block_decode_errors,
                            })
                            .await
                            .map_err(|_| anyhow!("result channel closed"))?;
                        continue;
                    }
                };

                if slot < min_slot {
                    result_tx
                        .send(BlockProcessResult {
                            slot: None,
                            block_time: None,
                            stats: Vec::new(),
                            metrics: block_metrics,
                            decode_errors: block_decode_errors,
                        })
                        .await
                        .map_err(|_| anyhow!("result channel closed"))?;
                    continue;
                }

                let block_slot = Some(slot);

                let block_node = match block.block() {
                    Ok(node) => node,
                    Err(_) => {
                        block_decode_errors += 1;
                        result_tx
                            .send(BlockProcessResult {
                                slot: block_slot,
                                block_time: None,
                                stats: Vec::new(),
                                metrics: block_metrics,
                                decode_errors: block_decode_errors,
                            })
                            .await
                            .map_err(|_| anyhow!("result channel closed"))?;
                        continue;
                    }
                };

                if let Some(bt) = block_node.meta.blocktime {
                    block_time = Some(bt);
                }

                for entry_cid_res in block_node.entries.iter() {
                    let entry_cid = match entry_cid_res {
                        Ok(cid) => cid,
                        Err(_) => {
                            block_decode_errors += 1;
                            continue;
                        }
                    };

                    let entry = match block.decode(entry_cid.hash_bytes()) {
                        Ok(Node::Entry(entry)) => entry,
                        Ok(_) => continue,
                        Err(_) => {
                            block_decode_errors += 1;
                            continue;
                        }
                    };

                    for tx_cid_res in entry.transactions.iter() {
                        let tx_cid = match tx_cid_res {
                            Ok(cid) => cid,
                            Err(_) => {
                                block_decode_errors += 1;
                                continue;
                            }
                        };

                        let tx_node = match block.decode(tx_cid.hash_bytes()) {
                            Ok(Node::Transaction(tx_node)) => tx_node,
                            Ok(_) => continue,
                            Err(_) => {
                                block_decode_errors += 1;
                                continue;
                            }
                        };

                        let tx_bytes_slice =
                            match reading::transaction_bytes(&block, &tx_node, &mut buf_tx) {
                                Ok(bytes) => bytes,
                                Err(_) => {
                                    block_decode_errors += 1;
                                    continue;
                                }
                            };

                        block_metrics.txs_seen += 1;

                        let tx = match VersionedTransaction::deserialize(tx_bytes_slice) {
                            Ok(tx) => tx,
                            Err(_) => {
                                block_decode_errors += 1;
                                continue;
                            }
                        };
                        let message = &tx.message;

                        for key in message.static_account_keys() {
                            let program_key: [u8; 32] = *key;
                            let entry = block_stats
                                .entry(program_key)
                                .or_insert_with(ProgramUsageStats::default);
                            entry.tx_count += 1;
                        }

                        let static_keys = message.static_account_keys();
                        resolved_keys.clear();
                        resolved_keys.reserve(
                            static_keys.len() + if worker_top_level_only { 0 } else { 16 },
                        );
                        resolved_keys.extend_from_slice(static_keys);

                        if worker_top_level_only {
                            for ix in message.instructions_iter() {
                                let program_idx = ix.program_id_index as usize;
                                if program_idx >= resolved_keys.len() {
                                    continue;
                                }
                                let program_key = resolved_keys[program_idx];

                                let entry = block_stats
                                    .entry(program_key)
                                    .or_insert_with(ProgramUsageStats::default);
                                entry.instruction_count += 1;

                                block_metrics.instructions_seen += 1;
                            }
                        } else {
                            let inner_instruction_groups =
                                match reading::metadata_bytes(&block, &tx_node, &mut buf_meta_df) {
                                    Ok(meta_bytes) => reading::decode_transaction_meta(
                                        meta_bytes,
                                        &mut meta_scratch,
                                    )
                                    .and_then(|meta| {
                                        reading::extend_with_loaded_addresses(
                                            &mut resolved_keys,
                                            &meta.loaded_writable_addresses,
                                        );
                                        reading::extend_with_loaded_addresses(
                                            &mut resolved_keys,
                                            &meta.loaded_readonly_addresses,
                                        );

                                        if !meta.inner_instructions_none
                                            && !meta.inner_instructions.is_empty()
                                        {
                                            Some(meta.inner_instructions)
                                        } else {
                                            None
                                        }
                                    }),
                                    Err(_) => None,
                                };

                            for ix in message.instructions_iter() {
                                let program_idx = ix.program_id_index as usize;
                                if program_idx >= resolved_keys.len() {
                                    continue;
                                }
                                let program_key = resolved_keys[program_idx];

                                let entry = block_stats
                                    .entry(program_key)
                                    .or_insert_with(ProgramUsageStats::default);
                                entry.instruction_count += 1;

                                block_metrics.instructions_seen += 1;
                            }

                            if let Some(groups) = inner_instruction_groups.as_ref() {
                                for group in groups {
                                    for inner_ix in group.instructions.iter() {
                                        let program_idx = inner_ix.program_id_index as usize;
                                        if program_idx >= resolved_keys.len() {
                                            continue;
                                        }
                                        let program_key = resolved_keys[program_idx];

                                        let entry = block_stats
                                            .entry(program_key)
                                            .or_insert_with(ProgramUsageStats::default);
                                        entry.inner_instruction_count += 1;

                                        block_metrics.inner_instructions_seen += 1;
                                    }
                                }
                            }
                        }
                    }
                }

                block_metrics.blocks_scanned += 1;
                stats_vec.clear();
                stats_vec.extend(
                    block_stats
                        .iter()
                        .map(|(program, stats)| (*program, *stats)),
                );

                result_tx
                    .send(BlockProcessResult {
                        slot: block_slot,
                        block_time,
                        stats: std::mem::take(&mut stats_vec),
                        metrics: block_metrics,
                        decode_errors: block_decode_errors,
                    })
                    .await
                    .map_err(|_| anyhow!("result channel closed"))?;

                if stats_vec.capacity() < 4096 {
                    stats_vec.reserve(4096 - stats_vec.capacity());
                }

                block_count += 1;
                if block_count % 500 == 0 {
                    if block_stats.capacity() > 8192 {
                        block_stats.shrink_to(4096);
                    }
                    if buf_tx.capacity() > 256 * 1024 {
                        buf_tx.shrink_to(128 * 1024);
                    }
                    if buf_meta_df.capacity() > 128 * 1024 {
                        buf_meta_df.shrink_to(64 * 1024);
                    }
                    if meta_scratch.capacity() > 128 * 1024 {
                        meta_scratch.shrink_to(64 * 1024);
                    }
                    if resolved_keys.capacity() > 1024 {
                        resolved_keys.shrink_to(512);
                    }
                    if stats_vec.capacity() > 8192 {
                        stats_vec.shrink_to(4096);
                    }
                }
            }

            Ok(())
        });

        handles.push(handle);
    }

    drop(result_tx);

    let cache_dir_owned = cache_dir.to_string();
    let reader_sender = block_tx.clone();
    let reader_handle = tokio::spawn(async move {
        let reader = open_epoch::open_epoch(epoch, &cache_dir_owned, FetchMode::Offline)
            .await
            .with_context(|| format!("failed to open epoch {epoch:04}"))?;
        let mut car = CarBlockReader::new(reader);
        car.read_header().await?;

        while let Some(block) = car.next_block().await? {
            if reader_sender.send(block).await.is_err() {
                break;
            }
        }

        drop(reader_sender);

        Ok::<_, anyhow::Error>(())
    });

    drop(block_tx);

    reader_handle
        .await
        .map_err(|e| anyhow!("reader join error: {e}"))??;

    for handle in handles {
        handle
            .await
            .map_err(|e| anyhow!("worker join error: {e}"))??;
    }

    consumer_handle
        .await
        .map_err(|e| anyhow!("consumer join error: {e}"))?
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
    let Some(last_epoch) = reading::find_last_cached_epoch(cache_path).await? else {
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
    let mut processed_epochs: AHashSet<u64> = reading::load_progress(&parts_dir).await?;
    let mut epoch_summaries: AHashMap<u64, ProgramUsageEpochPart> = AHashMap::with_capacity(256);

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
                    if processed_epochs.remove(&epoch) {
                        tracing::warn!(
                            "removing epoch {epoch:04} from cached progress due to unreadable file"
                        );
                    }
                    if let Err(remove_err) = fs::remove_file(&path).await {
                        tracing::warn!(
                            "failed to delete unreadable cache file {}: {}",
                            path.display(),
                            remove_err
                        );
                    }
                    continue;
                }
            };

            let part = match reading::decode_epoch_part(&data) {
                Ok(part) => part,
                Err(e) => {
                    tracing::warn!(
                        "failed to decode cached epoch stats {}: {}",
                        path.display(),
                        e
                    );
                    if processed_epochs.remove(&epoch) {
                        tracing::warn!(
                            "removing epoch {epoch:04} from cached progress due to decode failure"
                        );
                    }
                    if let Err(remove_err) = fs::remove_file(&path).await {
                        tracing::warn!(
                            "failed to delete corrupt cache file {}: {}",
                            path.display(),
                            remove_err
                        );
                    }
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
        }
    }

    reading::persist_progress(&parts_dir, &processed_epochs).await?;

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
    emit_progress(&pb, msg_buf.clone(), false);

    let mut last_epoch_processed = processed_epochs.iter().copied().max();

    let start_epoch_min_slot = if processed_epochs.contains(&start_epoch) {
        0
    } else {
        start_slot
    };

    let mut epochs_to_process: Vec<(u64, u64)> = Vec::new();
    for epoch in start_epoch..=last_epoch {
        if processed_epochs.contains(&epoch) {
            let msg = format!("skipping epoch {epoch:04} (already cached)");
            emit_progress(&pb, msg, true);
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

    let mut per_block_global = if collect_per_block {
        Some(PerBlockStats {
            blocks: Vec::new(),
            records: Vec::new(),
        })
    } else {
        None
    };

    let total_epochs = epochs_to_process.len();

    if total_epochs > 0 {
        let mut persist_counter = 0usize;
        for (idx, (epoch, min_slot)) in epochs_to_process.into_iter().enumerate() {
            let summary = process_epoch(
                epoch,
                cache_dir,
                &parts_dir,
                collect_per_block,
                top_level_only,
                min_slot,
                &pb,
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

            if let (Some(epoch_stats), Some(global_stats)) =
                (summary_per_block, per_block_global.as_mut())
            {
                let offset = global_stats.records.len();
                global_stats.records.extend(epoch_stats.records.into_iter());
                global_stats
                    .blocks
                    .extend(epoch_stats.blocks.into_iter().map(|mut blk| {
                        blk.start += offset;
                        blk
                    }));
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
            persist_counter += 1;
            if persist_counter % 10 == 0 || idx == total_epochs - 1 {
                reading::persist_progress(&parts_dir, &processed_epochs).await?;
            }
            last_epoch_processed =
                Some(last_epoch_processed.map_or(summary_epoch, |cur| cur.max(summary_epoch)));

            msg_buf.clear();
            let elapsed = start.elapsed();
            let secs = elapsed.as_secs_f64().max(1e-6);
            let blk_s = blocks_scanned as f64 / secs;
            let mb_s = (bytes_count as f64 / (1024.0 * 1024.0)) / secs;
            write!(
                &mut msg_buf,
                "E{:04}✓ ({}/{}) | {} blk | {:.0} blk/s | {:.1} MB/s",
                summary_epoch,
                processed_epochs.len(),
                last_epoch - start_epoch + 1,
                summary_metrics.blocks_scanned,
                blk_s,
                mb_s
            )
            .unwrap();

            emit_progress(&pb, msg_buf.clone(), true);
        }
    }

    if let Some(global_stats) = per_block_global.as_mut() {
        global_stats
            .blocks
            .sort_by(|a, b| match a.epoch.cmp(&b.epoch) {
                std::cmp::Ordering::Equal => a.slot.cmp(&b.slot),
                other => other,
            });
    }

    let Some(last_epoch_processed) = last_epoch_processed else {
        return Err(anyhow!(
            "no epochs processed between {start_epoch:04} and {last_epoch:04}"
        ));
    };

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
        "{range_label} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | instr={} inner={} txs={}",
        blocks_scanned, blk_s, mb_s, tps, instructions_seen, inner_instructions_seen, txs_seen
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
        per_block: per_block_global,
        per_epoch,
        processed_epochs: processed_epochs_list,
    })
}

async fn collect_program_stats_par(
    start_epoch: u64,
    start_slot: u64,
    cache_dir: &str,
    output_path: &Path,
    top_level_only: bool,
    jobs: usize,
) -> Result<ProgramStatsCollection> {
    // Limit the number of concurrently processed epochs so that block-level worker count can
    // be tuned independently of epoch-level parallelism.
    const MAX_PAR_EPOCH_CONCURRENCY: usize = 4;

    let cache_path = Path::new(cache_dir);
    let Some(last_epoch) = reading::find_last_cached_epoch(cache_path).await? else {
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
    let mut processed_epochs: AHashSet<u64> = reading::load_progress(&parts_dir).await?;
    let mut epoch_summaries: AHashMap<u64, ProgramUsageEpochPart> = AHashMap::with_capacity(256);

    let mut epoch_last_slots: AHashMap<u64, u64> = AHashMap::with_capacity(256);

    // load cached parts
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

            let part = match reading::decode_epoch_part(&data) {
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
        }
    }

    reading::persist_progress(&parts_dir, &processed_epochs).await?;

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
        "starting program stats (par) | start_epoch={:04} start_slot={} | scanning up to {:04}",
        start_epoch, start_slot, last_epoch
    )
    .unwrap();
    emit_progress(&pb, msg_buf.clone(), false);

    let mut last_epoch_processed = processed_epochs.iter().copied().max();

    let start_epoch_min_slot = if processed_epochs.contains(&start_epoch) {
        0
    } else {
        start_slot
    };

    let mut epochs_to_process: Vec<(u64, u64)> = Vec::new();
    for epoch in start_epoch..=last_epoch {
        if processed_epochs.contains(&epoch) {
            let msg = format!("skipping epoch {epoch:04} (already cached)");
            emit_progress(&pb, msg, true);
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
        let total_epochs = epochs_to_process.len();
        let mut epochs_since_progress_persist = 0usize;
        let max_inflight = std::cmp::min(total_epochs, MAX_PAR_EPOCH_CONCURRENCY);
        let mut pending = epochs_to_process.into_iter();
        let mut join_set: JoinSet<Result<EpochProcessSummary>> = JoinSet::new();

        let spawn_epoch = |join_set: &mut JoinSet<Result<EpochProcessSummary>>, epoch, min_slot| {
            let cache_dir_owned = cache_dir.to_string();
            let parts_dir_buf = parts_dir.clone();
            join_set.spawn(async move {
                process_epoch_par(
                    epoch,
                    cache_dir_owned.as_str(),
                    parts_dir_buf.as_path(),
                    top_level_only,
                    min_slot,
                    jobs,
                )
                .await
            });
        };

        for _ in 0..max_inflight {
            if let Some((epoch, min_slot)) = pending.next() {
                spawn_epoch(&mut join_set, epoch, min_slot);
            }
        }

        while let Some(result) = join_set.join_next().await {
            let summary = result.map_err(|e| anyhow!("epoch task join error: {e}"))??;

            let EpochProcessSummary {
                epoch: summary_epoch,
                last_slot: summary_last_slot,
                last_blocktime: summary_last_blocktime,
                records: summary_records,
                per_block: _,
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
            epochs_since_progress_persist += 1;
            last_epoch_processed =
                Some(last_epoch_processed.map_or(summary_epoch, |cur| cur.max(summary_epoch)));

            msg_buf.clear();
            let elapsed = start.elapsed();
            let secs = elapsed.as_secs_f64().max(1e-6);
            let blk_s = blocks_scanned as f64 / secs;
            let mb_s = (bytes_count as f64 / (1024.0 * 1024.0)) / secs;

            let active = join_set.len();
            let queued = pending.len();
            let overall_total = (last_epoch - start_epoch + 1) as usize;

            write!(
                &mut msg_buf,
                "E{:04}✓ ({}/{}) | {} active | {} queued | {:.0} blk/s | {:.1} MB/s",
                summary_epoch,
                processed_epochs.len(),
                overall_total,
                active,
                queued,
                blk_s,
                mb_s
            )
            .unwrap();

            emit_progress(&pb, msg_buf.clone(), true);

            if let Some((epoch, min_slot)) = pending.next() {
                spawn_epoch(&mut join_set, epoch, min_slot);
            }

            if epochs_since_progress_persist >= 10 || (pending.len() == 0 && join_set.is_empty()) {
                reading::persist_progress(&parts_dir, &processed_epochs).await?;
                epochs_since_progress_persist = 0;
            }
        }
    }

    let Some(last_epoch_processed) = last_epoch_processed else {
        return Err(anyhow!(
            "no epochs processed between {start_epoch:04} and {last_epoch:04}"
        ));
    };

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
        "[par] {range_label} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | instr={} inner={} txs={}",
        blocks_scanned, blk_s, mb_s, tps, instructions_seen, inner_instructions_seen, txs_seen
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
        per_block: None,
        per_epoch,
        processed_epochs: processed_epochs_list,
    })
}

pub async fn dump_program_stats(
    start_epoch: u64,
    start_slot: u64,
    cache_dir: &str,
    output_path: &Path,
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

    let aggregated = collection.aggregated;
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

pub async fn dump_program_stats_par(
    start_epoch: u64,
    start_slot: u64,
    cache_dir: &str,
    output_path: &Path,
    top_level_only: bool,
    jobs: usize,
) -> Result<()> {
    let collection = collect_program_stats_par(
        start_epoch,
        start_slot,
        cache_dir,
        output_path,
        top_level_only,
        jobs,
    )
    .await?;

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }

    let aggregated = collection.aggregated;
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
    parts_preference: ProgramUsagePartsPreference,
) -> Result<()> {
    let export = reading::load_program_usage_export(input_path, parts_preference)
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
    let limit = limit.unwrap_or(usize::MAX);
    if limit != usize::MAX {
        aggregated.truncate(limit);
    }

    let mut program_keys = Vec::with_capacity(aggregated.len());
    let mut seen_programs = AHashSet::with_capacity(aggregated.len());
    for record in aggregated.iter() {
        if seen_programs.insert(record.program) {
            program_keys.push(record.program);
        }
    }

    if limit != usize::MAX {
        for epoch in export.epochs.iter() {
            for record in epoch.records.iter().take(limit) {
                if seen_programs.insert(record.program) {
                    program_keys.push(record.program);
                }
            }
        }
    }

    let program_labels = program_keys
        .iter()
        .map(|program| bs58::encode(program).into_string())
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
