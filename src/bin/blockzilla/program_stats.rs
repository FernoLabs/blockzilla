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
use std::io::{ErrorKind, IsTerminal, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::fs;
use wincode::Deserialize as _;
use wincode::{SchemaRead, SchemaWrite};

use crate::LOG_INTERVAL_SECS;

const BLOCK_LOG_CHUNK: u64 = 500;

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
struct ProgramUsageEpochDump {
    pub epoch: u64,
    pub last_slot: u64,
    pub records: Vec<ProgramUsageRecord>,
}

pub async fn dump_program_stats(
    start_epoch: u64,
    start_slot: u64,
    cache_dir: &str,
    output_path: &Path,
    limit: Option<usize>,
) -> Result<()> {
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

    let parts_dir = output_path.with_extension("parts");

    let mut aggregated: AHashMap<[u8; 32], ProgramUsageStats> = AHashMap::with_capacity(16_384);
    let mut processed_epochs: AHashSet<u64> = AHashSet::with_capacity(256);

    let mut last_processed_slot = 0u64;

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

            let part: ProgramUsageEpochDump = match wincode::deserialize(&data) {
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
            if part.epoch == start_epoch {
                last_processed_slot = part.last_slot;
            }
        }
    }

    let draw_target = if std::io::stdout().is_terminal() {
        ProgressDrawTarget::stderr_with_hz(8)
    } else {
        ProgressDrawTarget::hidden()
    };
    let pb = ProgressBar::with_draw_target(Some(0), draw_target);
    pb.set_style(ProgressStyle::with_template("{spinner:.green} {msg}").unwrap());
    pb.enable_steady_tick(Duration::from_millis(120));

    let start = Instant::now();
    let mut last_log = start - Duration::from_secs(LOG_INTERVAL_SECS);

    let mut blocks_scanned = 0u64;
    let mut txs_seen = 0u64;
    let mut instructions_seen = 0u64;
    let mut inner_instructions_seen = 0u64;
    let mut bytes_count = 0u64;

    let mut msg_buf = String::with_capacity(256);
    msg_buf.clear();
    write!(
        &mut msg_buf,
        "starting program stats | start_epoch={:04} start_slot={} | scanning up to {:04}",
        start_epoch, start_slot, last_epoch
    )
    .unwrap();
    pb.set_message(msg_buf.clone());

    let mut buf_tx = Vec::with_capacity(128 * 1024);
    let mut buf_meta = Vec::with_capacity(128 * 1024);
    let mut reusable_tx = std::mem::MaybeUninit::<VersionedTransaction>::uninit();
    let mut resolved_keys = SmallVec::<[Pubkey; 512]>::new();
    let mut tx_programs = SmallVec::<[[u8; 32]; 64]>::new();

    let mut last_epoch_processed = None;

    let mut min_slot = if processed_epochs.contains(&start_epoch) {
        0
    } else {
        start_slot
    };
    let mut skipped_blocks = 0u64;
    let mut skipped_bytes = 0u64;
    let mut last_skipped_slot = 0u64;
    let mut printed_skipped_summary = false;

    for epoch in start_epoch..=last_epoch {
        if processed_epochs.contains(&epoch) {
            if pb.is_hidden() {
                tracing::info!("skipping epoch {epoch:04} (already cached)");
            } else {
                pb.println(format!("skipping epoch {epoch:04} (already cached)"));
            }
            last_epoch_processed = Some(epoch);
            continue;
        }

        if pb.is_hidden() {
            tracing::info!("opening epoch {epoch:04}");
        } else {
            pb.println(format!("opening epoch {epoch:04}"));
        }

        let reader = open_epoch::open_epoch(epoch, cache_dir, FetchMode::Offline)
            .await
            .with_context(|| format!("failed to open epoch {epoch:04}"))?;
        let mut car = CarBlockReader::new(reader);
        car.read_header().await?;

        let mut epoch_stats: AHashMap<[u8; 32], ProgramUsageStats> = AHashMap::with_capacity(4096);
        let mut epoch_last_slot = 0u64;

        while let Some(block) = car.next_block().await? {
            bytes_count += block.entries.iter().map(|entry| entry.len()).sum::<usize>() as u64;

            let slot = match peek_block_slot(&block) {
                Ok(slot) => slot,
                Err(e) => {
                    tracing::warn!("failed to peek block slot: {e}");
                    continue;
                }
            };

            if slot < min_slot {
                skipped_blocks += 1;
                skipped_bytes +=
                    block.entries.iter().map(|entry| entry.len()).sum::<usize>() as u64;
                last_skipped_slot = slot;
                continue;
            }

            if !printed_skipped_summary && skipped_blocks > 0 {
                printed_skipped_summary = true;
                let line = format!(
                    "reached start_slot={min_slot} | skipped {} blocks ({:.2} MB), last skipped slot {last_skipped_slot}",
                    skipped_blocks,
                    skipped_bytes as f64 / (1024.0 * 1024.0)
                );
                if pb.is_hidden() {
                    tracing::info!("{line}");
                } else {
                    pb.println(line);
                }
            }

            let block_node = match block.block() {
                Ok(node) => node,
                Err(e) => {
                    tracing::warn!("failed to decode block: {e}");
                    continue;
                }
            };

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

                    txs_seen += 1;

                    if let Err(e) =
                        VersionedTransaction::deserialize_into(tx_bytes_slice, &mut reusable_tx)
                    {
                        tracing::warn!("failed to parse transaction: {e}");
                        continue;
                    }

                    let tx_ref = unsafe { reusable_tx.assume_init_ref() };

                    resolved_keys.clear();
                    resolved_keys.reserve(tx_ref.message.static_account_keys().len() + 16);
                    for key in tx_ref.message.static_account_keys() {
                        resolved_keys.push(Pubkey::new_from_array(*key));
                    }

                    let mut inner_instruction_groups = None;

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

                    for ix in tx_ref.message.instructions_iter() {
                        let program_idx = ix.program_id_index as usize;
                        if program_idx >= resolved_keys.len() {
                            continue;
                        }
                        let program_key = resolved_keys[program_idx].to_bytes();
                        let entry = epoch_stats
                            .entry(program_key)
                            .or_insert_with(ProgramUsageStats::default);
                        entry.instruction_count += 1;
                        instructions_seen += 1;
                        if !tx_programs.iter().any(|existing| existing == &program_key) {
                            tx_programs.push(program_key);
                        }
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
                                inner_instructions_seen += 1;
                                if !tx_programs.iter().any(|existing| existing == &program_key) {
                                    tx_programs.push(program_key);
                                }
                            }
                        }
                    }

                    for program_key in tx_programs.iter() {
                        let entry = epoch_stats
                            .entry(*program_key)
                            .or_insert_with(ProgramUsageStats::default);
                        entry.tx_count += 1;
                    }
                    tx_programs.clear();

                    unsafe { std::ptr::drop_in_place(reusable_tx.as_mut_ptr()) };
                }
            }

            epoch_last_slot = slot;
            blocks_scanned += 1;

            let now = Instant::now();
            let time_due = now.duration_since(last_log) >= Duration::from_secs(LOG_INTERVAL_SECS);
            let chunk_due = blocks_scanned % BLOCK_LOG_CHUNK == 0;
            if time_due || chunk_due {
                last_log = now;
                let elapsed = now.duration_since(start);
                let secs = elapsed.as_secs_f64().max(1e-6);
                let blk_s = blocks_scanned as f64 / secs;
                let mb_s = (bytes_count as f64 / (1024.0 * 1024.0)) / secs;
                let tps = if elapsed.as_secs() > 0 {
                    txs_seen / elapsed.as_secs()
                } else {
                    0
                };

                msg_buf.clear();
                write!(
                    &mut msg_buf,
                    "epoch {:04} | slot={} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} TPS | instr={} inner={}",
                    epoch,
                    epoch_last_slot,
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

        fs::create_dir_all(&parts_dir).await?;
        let part_path = parts_dir.join(format!("epoch-{epoch:04}.bin"));
        let tmp_path = part_path.with_extension("bin.tmp");

        let mut records: Vec<ProgramUsageRecord> = epoch_stats
            .into_iter()
            .map(|(program, stats)| ProgramUsageRecord { program, stats })
            .collect();
        records.sort_unstable_by(|a, b| b.stats.instruction_count.cmp(&a.stats.instruction_count));

        let epoch_dump = ProgramUsageEpochDump {
            epoch,
            last_slot: epoch_last_slot,
            records: records.clone(),
        };
        let encoded_epoch = wincode::serialize(&epoch_dump)?;
        fs::write(&tmp_path, &encoded_epoch).await?;
        fs::rename(&tmp_path, &part_path).await?;

        for record in records {
            aggregated
                .entry(record.program)
                .or_insert_with(ProgramUsageStats::default)
                .accumulate(&record.stats);
        }

        processed_epochs.insert(epoch);
        last_processed_slot = epoch_last_slot;
        last_epoch_processed = Some(epoch);
        min_slot = 0;
        skipped_blocks = 0;
        skipped_bytes = 0;
        printed_skipped_summary = false;
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

    let limit = limit.unwrap_or(aggregated_records.len());
    let export_records = aggregated_records.iter().take(limit).collect::<Vec<_>>();

    let mut output_rows = Vec::with_capacity(export_records.len());
    for record in export_records {
        let program_str = bs58::encode(record.program).into_string();
        output_rows.push(serde_json::json!({
            "program": program_str,
            "tx_count": record.stats.tx_count,
            "instruction_count": record.stats.instruction_count,
            "inner_instruction_count": record.stats.inner_instruction_count,
        }));
    }

    let json_output = serde_json::to_string_pretty(&output_rows)?;
    fs::write(output_path, json_output).await?;

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
