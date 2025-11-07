use anyhow::{Context, Result, anyhow};
use indicatif::ProgressBar;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, BufReader},
    sync::{Mutex, mpsc},
    task,
};

use crate::optimizer::OptimizedFormat;
use blockzilla::{
    carblock_to_compact::{CompactBlock, CompactMetadataPayload},
    compact_log::{self, DecodeConfig},
    optimized_cbor::decode_owned_compact_block,
    transaction_parser::Signature,
};
use bs58;
use serde::Serialize;

pub const LOG_INTERVAL_SECS: u64 = 2;

fn optimized_dir(base: &Path, epoch: u64) -> PathBuf {
    base.join(format!("epoch-{epoch:04}/optimized"))
}
fn optimized_blocks_file(dir: &Path, format: OptimizedFormat) -> PathBuf {
    match format {
        OptimizedFormat::Postcard => dir.join("blocks.bin"),
        OptimizedFormat::Cbor => dir.join("blocks.cbor"),
    }
}

fn resolve_blocks_path(input_dir: &str, epoch: u64) -> Option<(PathBuf, OptimizedFormat)> {
    let base = Path::new(input_dir);
    let direct_cbor = base.join(format!("epoch-{epoch:04}.cbor"));
    if direct_cbor.exists() {
        return Some((direct_cbor, OptimizedFormat::Cbor));
    }

    let direct_bin = base.join(format!("epoch-{epoch:04}.bin"));
    if direct_bin.exists() {
        return Some((direct_bin, OptimizedFormat::Postcard));
    }

    let nested = optimized_dir(base, epoch);
    let cbor_nested = optimized_blocks_file(&nested, OptimizedFormat::Cbor);
    if cbor_nested.exists() {
        return Some((cbor_nested, OptimizedFormat::Cbor));
    }

    let bin_nested = optimized_blocks_file(&nested, OptimizedFormat::Postcard);
    if bin_nested.exists() {
        return Some((bin_nested, OptimizedFormat::Postcard));
    }

    None
}

struct OptimizedStream {
    rdr: BufReader<File>,
    format: OptimizedFormat,
    scratch: Vec<u8>,
}

async fn read_varint_async<R>(reader: &mut R) -> Result<Option<(usize, usize)>>
where
    R: AsyncRead + Unpin,
{
    let mut value: usize = 0;
    let mut shift = 0usize;
    let mut bytes = 0usize;
    loop {
        let mut byte = [0u8; 1];
        match reader.read_exact(&mut byte).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if bytes == 0 {
                    return Ok(None);
                } else {
                    return Err(e.into());
                }
            }
            Err(e) => return Err(e.into()),
        }
        bytes += 1;
        if bytes > 10 {
            anyhow::bail!("varint too long");
        }
        let b = byte[0];
        value |= ((b & 0x7F) as usize) << shift;
        if (b & 0x80) == 0 {
            return Ok(Some((value, bytes)));
        }
        shift += 7;
        if shift >= usize::BITS as usize {
            anyhow::bail!("varint overflow");
        }
    }
}

impl OptimizedStream {
    async fn open(input_dir: &str, epoch: u64) -> Result<Self> {
        let (path, format) = resolve_blocks_path(input_dir, epoch)
            .with_context(|| format!("Blocks file not found in {input_dir}"))?;
        let f = File::open(&path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        Ok(Self {
            rdr: BufReader::with_capacity(32 * 1024 * 1024, f),
            format,
            scratch: Vec::with_capacity(256 << 10),
        })
    }

    async fn next_block(&mut self) -> Result<Option<(CompactBlock, usize, usize)>> {
        match self.format {
            OptimizedFormat::Postcard => {
                let mut len_bytes = [0u8; 4];
                match self.rdr.read_exact(&mut len_bytes).await {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                    Err(e) => return Err(e.into()),
                }
                let len = u32::from_le_bytes(len_bytes) as usize;

                self.scratch.resize(len, 0);
                self.rdr.read_exact(&mut self.scratch).await?;

                let block: CompactBlock = postcard::from_bytes(&self.scratch)
                    .map_err(|e| anyhow!("deserialize CompactBlock: {e}"))?;
                Ok(Some((block, len, len + 4)))
            }
            OptimizedFormat::Cbor => {
                let (len, header_len) = match read_varint_async(&mut self.rdr).await? {
                    Some(v) => v,
                    None => return Ok(None),
                };

                self.scratch.resize(len, 0);
                self.rdr.read_exact(&mut self.scratch).await?;

                let block = decode_owned_compact_block(&self.scratch)
                    .map_err(|e| anyhow!("deserialize CompactBlock cbor: {e}"))?;
                Ok(Some((block, len, len + header_len)))
            }
        }
    }
}

#[derive(Default)]
struct AnalyzeStats {
    blocks: u64,
    total_block_bytes: u64,
    total_frame_bytes: u64,
    total_txs: u64,
    total_instructions: u64,
    total_rewards: u64,
    slot_bytes: u64,
    txs_bytes: u64,
    rewards_bytes: u64,
    tx_signatures_bytes: u64,
    tx_header_bytes: u64,
    tx_account_ids_bytes: u64,
    tx_recent_blockhash_bytes: u64,
    tx_instructions_bytes: u64,
    tx_lookups_bytes: u64,
    tx_is_versioned_bytes: u64,
    tx_metadata_option_bytes: u64,
    tx_metadata_none_bytes: u64,
    tx_metadata_compact_payload_bytes: u64,
    tx_metadata_raw_payload_bytes: u64,
    tx_metadata_compact_count: u64,
    tx_metadata_raw_count: u64,
    tx_metadata_none_count: u64,
    metadata_fee_bytes: u64,
    metadata_err_bytes: u64,
    metadata_pre_balances_bytes: u64,
    metadata_post_balances_bytes: u64,
    metadata_pre_token_balances_bytes: u64,
    metadata_post_token_balances_bytes: u64,
    metadata_loaded_writable_bytes: u64,
    metadata_loaded_readonly_bytes: u64,
    metadata_return_data_option_bytes: u64,
    metadata_return_data_payload_bytes: u64,
    metadata_inner_instructions_option_bytes: u64,
    metadata_inner_instructions_payload_bytes: u64,
    metadata_log_option_bytes: u64,
    metadata_log_stream_bytes: u64,
    metadata_log_event_bytes: u64,
    metadata_log_string_bytes: u64,
    metadata_log_string_count: u64,
    tx_with_logs: u64,
}

fn serialized_size<T: Serialize>(value: &T) -> usize {
    postcard::to_allocvec(value)
        .expect("serialize for sizing")
        .len()
}

fn print_size(label: &str, bytes: u64, total: u64) {
    let pct = if total > 0 {
        (bytes as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    println!("    {:<28} {:>12} bytes ({:>5.1}%)", label, bytes, pct);
}

fn parse_signature_str(sig_str: &str) -> Result<Signature> {
    let bytes = bs58::decode(sig_str)
        .into_vec()
        .map_err(|e| anyhow!("decode signature {sig_str}: {e}"))?;
    if bytes.len() != 64 {
        anyhow::bail!(
            "signature {sig_str} decoded to {} bytes (expected 64)",
            bytes.len()
        );
    }
    let mut arr = [0u8; 64];
    arr.copy_from_slice(&bytes);
    Ok(Signature(arr))
}

fn render_signatures(signatures: &[Signature]) -> String {
    if signatures.is_empty() {
        "<no signatures>".into()
    } else {
        signatures
            .iter()
            .map(|sig| bs58::encode(sig.0).into_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub async fn read_compressed_blocks(epoch: u64, input_dir: &str) -> Result<()> {
    let mut stream = OptimizedStream::open(input_dir, epoch).await?;

    let start = Instant::now();
    let mut last_log = start;
    let pb = ProgressBar::new_spinner();

    let mut blocks_count = 0u64;
    let mut instr_count = 0u64;
    let mut tx_count = 0u64;
    let mut reward_count = 0u64;

    while let Some((block, _payload_len, _frame_len)) = stream.next_block().await? {
        tx_count += block.txs.len() as u64;
        reward_count += block.rewards.len() as u64;
        instr_count += block
            .txs
            .iter()
            .map(|t| t.instructions.len() as u64)
            .sum::<u64>();
        blocks_count += 1;

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>8} ixs | {:>6.1} ix/s | {:>7} rwd",
                blocks_count,
                blocks_count as f64 / elapsed,
                tx_count,
                instr_count,
                instr_count as f64 / elapsed,
                reward_count,
            ));
        }
    }

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>8} ixs | {:>6.1} ix/s | {:>7} rwd | {:.1}s",
        blocks_count,
        blocks_count as f64 / elapsed,
        tx_count,
        instr_count,
        instr_count as f64 / elapsed,
        reward_count,
        elapsed,
    ));

    Ok(())
}

pub async fn read_compressed_blocks_par(epoch: u64, input_dir: &str, jobs: usize) -> Result<()> {
    let (path, format) = resolve_blocks_path(input_dir, epoch)
        .with_context(|| format!("Blocks file not found in {input_dir}"))?;

    let blocks_count = Arc::new(AtomicU64::new(0));
    let instr_count = Arc::new(AtomicU64::new(0));
    let tx_count = Arc::new(AtomicU64::new(0));
    let reward_count = Arc::new(AtomicU64::new(0));

    let (tx, rx) = mpsc::channel::<Vec<u8>>(jobs.max(1) * 8);
    let rx = Arc::new(Mutex::new(rx));

    for _ in 0..jobs.max(1) {
        let rx = Arc::clone(&rx);
        let blocks_count = Arc::clone(&blocks_count);
        let instr_count = Arc::clone(&instr_count);
        let tx_count = Arc::clone(&tx_count);
        let reward_count = Arc::clone(&reward_count);

        let format = format;
        task::spawn(async move {
            loop {
                let frame_opt = {
                    let mut guard = rx.lock().await;
                    guard.recv().await
                };
                let Some(frame) = frame_opt else { break };

                let decoded = match format {
                    OptimizedFormat::Postcard => postcard::from_bytes::<CompactBlock>(&frame)
                        .map_err(|e| anyhow!("deserialize CompactBlock: {e}")),
                    OptimizedFormat::Cbor => decode_owned_compact_block(&frame)
                        .map_err(|e| anyhow!("deserialize CompactBlock cbor: {e}")),
                };

                match decoded {
                    Ok(block) => {
                        let ixs_in_block: u64 =
                            block.txs.iter().map(|t| t.instructions.len() as u64).sum();
                        instr_count.fetch_add(ixs_in_block, Ordering::Relaxed);
                        tx_count.fetch_add(block.txs.len() as u64, Ordering::Relaxed);
                        reward_count.fetch_add(block.rewards.len() as u64, Ordering::Relaxed);
                        blocks_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => eprintln!("Deserialization error: {e}"),
                }
            }
        });
    }

    let file = File::open(&path).await?;
    let mut rdr = BufReader::with_capacity(32 * 1024 * 1024, file);

    let pb = ProgressBar::new_spinner();
    let start = Instant::now();
    let mut last_log = start;

    loop {
        let (len, _header_len) = match format {
            OptimizedFormat::Postcard => {
                let mut len_bytes = [0u8; 4];
                match rdr.read_exact(&mut len_bytes).await {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }
                (u32::from_le_bytes(len_bytes) as usize, 4usize)
            }
            OptimizedFormat::Cbor => match read_varint_async(&mut rdr).await? {
                Some(v) => v,
                None => break,
            },
        };

        let mut buf = vec![0u8; len];
        rdr.read_exact(&mut buf).await?;

        if tx.send(buf).await.is_err() {
            break;
        }

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);

            let b = blocks_count.load(Ordering::Relaxed);
            let i = instr_count.load(Ordering::Relaxed);
            let t = tx_count.load(Ordering::Relaxed);
            let r = reward_count.load(Ordering::Relaxed);

            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>8} ixs | {:>6.1} ix/s | {:>7} rwd",
                b,
                b as f64 / elapsed,
                t,
                i,
                i as f64 / elapsed,
                r,
            ));
        }
    }

    drop(tx);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    let b = blocks_count.load(Ordering::Relaxed);
    let i = instr_count.load(Ordering::Relaxed);
    let t = tx_count.load(Ordering::Relaxed);
    let r = reward_count.load(Ordering::Relaxed);

    pb.finish_with_message(format!(
        "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>8} ixs | {:>6.1} ix/s | {:>7} rwd | {:.1}s",
        b,
        b as f64 / elapsed,
        t,
        i,
        i as f64 / elapsed,
        r,
        elapsed,
    ));

    Ok(())
}

pub async fn analyze_compressed_blocks(epoch: u64, input_dir: &str) -> Result<()> {
    let mut stream = OptimizedStream::open(input_dir, epoch).await?;

    let mut stats = AnalyzeStats::default();

    let pb = ProgressBar::new_spinner();
    let start = Instant::now();
    let mut last_log = start;

    while let Some((block, payload_len, frame_len)) = stream.next_block().await? {
        stats.blocks += 1;
        stats.total_block_bytes += payload_len as u64;
        stats.total_frame_bytes += frame_len as u64;
        stats.total_txs += block.txs.len() as u64;
        stats.total_rewards += block.rewards.len() as u64;
        stats.total_instructions += block
            .txs
            .iter()
            .map(|t| t.instructions.len() as u64)
            .sum::<u64>();

        stats.slot_bytes += serialized_size(&block.slot) as u64;
        stats.txs_bytes += serialized_size(&block.txs) as u64;
        stats.rewards_bytes += serialized_size(&block.rewards) as u64;

        for tx in &block.txs {
            stats.tx_signatures_bytes += serialized_size(&tx.signatures) as u64;
            stats.tx_header_bytes += serialized_size(&tx.header) as u64;
            stats.tx_account_ids_bytes += serialized_size(&tx.account_ids) as u64;
            stats.tx_recent_blockhash_bytes += serialized_size(&tx.recent_blockhash) as u64;
            stats.tx_instructions_bytes += serialized_size(&tx.instructions) as u64;
            stats.tx_lookups_bytes += serialized_size(&tx.address_table_lookups) as u64;
            stats.tx_is_versioned_bytes += serialized_size(&tx.is_versioned) as u64;

            let metadata_option_bytes = serialized_size(&tx.metadata) as u64;
            stats.tx_metadata_option_bytes += metadata_option_bytes;

            match &tx.metadata {
                Some(CompactMetadataPayload::Compact(meta)) => {
                    stats.tx_metadata_compact_count += 1;
                    stats.tx_metadata_compact_payload_bytes += serialized_size(meta) as u64;

                    stats.metadata_fee_bytes += serialized_size(&meta.fee) as u64;
                    stats.metadata_err_bytes += serialized_size(&meta.err) as u64;
                    stats.metadata_pre_balances_bytes += serialized_size(&meta.pre_balances) as u64;
                    stats.metadata_post_balances_bytes +=
                        serialized_size(&meta.post_balances) as u64;
                    stats.metadata_pre_token_balances_bytes +=
                        serialized_size(&meta.pre_token_balances) as u64;
                    stats.metadata_post_token_balances_bytes +=
                        serialized_size(&meta.post_token_balances) as u64;
                    stats.metadata_loaded_writable_bytes +=
                        serialized_size(&meta.loaded_writable_ids) as u64;
                    stats.metadata_loaded_readonly_bytes +=
                        serialized_size(&meta.loaded_readonly_ids) as u64;

                    let return_data_option_bytes = serialized_size(&meta.return_data) as u64;
                    stats.metadata_return_data_option_bytes += return_data_option_bytes;
                    if let Some(return_data) = &meta.return_data {
                        stats.metadata_return_data_payload_bytes +=
                            serialized_size(return_data) as u64;
                    }

                    let inner_option_bytes = serialized_size(&meta.inner_instructions) as u64;
                    stats.metadata_inner_instructions_option_bytes += inner_option_bytes;
                    if let Some(inner) = &meta.inner_instructions {
                        stats.metadata_inner_instructions_payload_bytes +=
                            serialized_size(inner) as u64;
                    }

                    let log_option_bytes = serialized_size(&meta.log_messages) as u64;
                    stats.metadata_log_option_bytes += log_option_bytes;
                    if let Some(logs) = &meta.log_messages {
                        stats.tx_with_logs += 1;
                        stats.metadata_log_stream_bytes += serialized_size(logs) as u64;
                        stats.metadata_log_event_bytes += logs.bytes.len() as u64;
                        stats.metadata_log_string_bytes +=
                            logs.strings.iter().map(|s| s.len() as u64).sum::<u64>();
                        stats.metadata_log_string_count += logs.strings.len() as u64;
                    }
                }
                Some(CompactMetadataPayload::Raw(raw)) => {
                    stats.tx_metadata_raw_count += 1;
                    stats.tx_metadata_raw_payload_bytes += serialized_size(raw) as u64;
                }
                None => {
                    stats.tx_metadata_none_count += 1;
                    stats.tx_metadata_none_bytes += metadata_option_bytes;
                }
            }
        }

        let now = Instant::now();
        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            let elapsed = now.duration_since(start).as_secs_f64().max(0.001);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>8} tx | {:>6.1} tx/s",
                stats.blocks,
                stats.blocks as f64 / elapsed,
                stats.total_txs,
                stats.total_txs as f64 / elapsed,
            ));
            last_log = now;
        }
    }

    let elapsed = start.elapsed().as_secs_f64().max(0.001);
    pb.finish_and_clear();

    println!("\n📊 Analysis Results:");
    println!("  Epoch:                 {}", epoch);
    println!("  Total Blocks:          {}", stats.blocks);
    println!("  Total Transactions:    {}", stats.total_txs);
    println!("  Total Instructions:    {}", stats.total_instructions);
    println!("  Total Rewards:         {}", stats.total_rewards);
    println!(
        "  Total Block Bytes:     {} (payload)",
        stats.total_block_bytes
    );
    println!(
        "  Total Block Bytes:     {} (payload + frame prefix)",
        stats.total_frame_bytes
    );
    if stats.blocks > 0 {
        println!(
            "  Avg Block Payload:     {:.2} bytes",
            stats.total_block_bytes as f64 / stats.blocks as f64
        );
        println!(
            "  Avg Block On-Disk:     {:.2} bytes",
            stats.total_frame_bytes as f64 / stats.blocks as f64
        );
        println!(
            "  Avg Tx per Block:      {:.2}",
            stats.total_txs as f64 / stats.blocks as f64
        );
        println!(
            "  Avg Ixs per Block:     {:.2}",
            stats.total_instructions as f64 / stats.blocks as f64
        );
        println!(
            "  Avg Rewards per Block: {:.2}",
            stats.total_rewards as f64 / stats.blocks as f64
        );
    }
    println!(
        "  Throughput (avg):      {:.2} tx/s | {:.2} blk/s",
        stats.total_txs as f64 / elapsed,
        stats.blocks as f64 / elapsed
    );

    if stats.total_block_bytes > 0 {
        println!("\n  Block field sizes:");
        print_size("Slot", stats.slot_bytes, stats.total_block_bytes);
        print_size("Transactions", stats.txs_bytes, stats.total_block_bytes);
        print_size("Rewards", stats.rewards_bytes, stats.total_block_bytes);

        println!("\n  Transaction field sizes:");
        let tx_total_bytes = stats.txs_bytes;
        if tx_total_bytes > 0 {
            print_size("Signatures", stats.tx_signatures_bytes, tx_total_bytes);
            print_size("Header", stats.tx_header_bytes, tx_total_bytes);
            print_size("Account IDs", stats.tx_account_ids_bytes, tx_total_bytes);
            print_size(
                "Recent blockhash",
                stats.tx_recent_blockhash_bytes,
                tx_total_bytes,
            );
            print_size("Instructions", stats.tx_instructions_bytes, tx_total_bytes);
            print_size(
                "Address table lookups",
                stats.tx_lookups_bytes,
                tx_total_bytes,
            );
            print_size("Version flag", stats.tx_is_versioned_bytes, tx_total_bytes);
            print_size(
                "Metadata field",
                stats.tx_metadata_option_bytes,
                tx_total_bytes,
            );
        }

        println!("\n    Metadata usage:");
        println!(
            "      Compact metadata txs: {}",
            stats.tx_metadata_compact_count
        );
        println!(
            "      Raw metadata txs:     {}",
            stats.tx_metadata_raw_count
        );
        println!(
            "      No metadata txs:      {}",
            stats.tx_metadata_none_count
        );

        if stats.tx_metadata_option_bytes > 0 {
            print_size(
                "      • None (Option tag)",
                stats.tx_metadata_none_bytes,
                stats.tx_metadata_option_bytes,
            );
            let some_tag_bytes =
                (stats.tx_metadata_compact_count + stats.tx_metadata_raw_count) as u64;
            print_size(
                "      • Some tag overhead",
                some_tag_bytes,
                stats.tx_metadata_option_bytes,
            );
            print_size(
                "      • Compact payload",
                stats.tx_metadata_compact_payload_bytes,
                stats.tx_metadata_option_bytes,
            );
            print_size(
                "      • Raw payload",
                stats.tx_metadata_raw_payload_bytes,
                stats.tx_metadata_option_bytes,
            );
        }

        if stats.tx_metadata_compact_payload_bytes > 0 {
            println!("\n    Compact metadata field sizes:");
            let meta_total = stats.tx_metadata_compact_payload_bytes;
            print_size("Fee", stats.metadata_fee_bytes, meta_total);
            print_size("Error", stats.metadata_err_bytes, meta_total);
            print_size(
                "Pre balances",
                stats.metadata_pre_balances_bytes,
                meta_total,
            );
            print_size(
                "Post balances",
                stats.metadata_post_balances_bytes,
                meta_total,
            );
            print_size(
                "Pre token balances",
                stats.metadata_pre_token_balances_bytes,
                meta_total,
            );
            print_size(
                "Post token balances",
                stats.metadata_post_token_balances_bytes,
                meta_total,
            );
            print_size(
                "Loaded writable",
                stats.metadata_loaded_writable_bytes,
                meta_total,
            );
            print_size(
                "Loaded readonly",
                stats.metadata_loaded_readonly_bytes,
                meta_total,
            );
            print_size(
                "Return data (Option)",
                stats.metadata_return_data_option_bytes,
                meta_total,
            );
            print_size(
                "Inner ixs (Option)",
                stats.metadata_inner_instructions_option_bytes,
                meta_total,
            );
            print_size(
                "Log messages (Option)",
                stats.metadata_log_option_bytes,
                meta_total,
            );

            if stats.metadata_return_data_payload_bytes > 0 {
                print_size(
                    "      • Return data payload",
                    stats.metadata_return_data_payload_bytes,
                    meta_total,
                );
            }
            if stats.metadata_inner_instructions_payload_bytes > 0 {
                print_size(
                    "      • Inner ixs payload",
                    stats.metadata_inner_instructions_payload_bytes,
                    meta_total,
                );
            }
            if stats.metadata_log_stream_bytes > 0 {
                print_size(
                    "      • Log stream",
                    stats.metadata_log_stream_bytes,
                    meta_total,
                );
                println!(
                    "        Log event bytes:   {}",
                    stats.metadata_log_event_bytes
                );
                println!(
                    "        Log string bytes:  {} ({} strings)",
                    stats.metadata_log_string_bytes, stats.metadata_log_string_count
                );
                println!("        Txs with logs:     {}", stats.tx_with_logs);
            }
        }
    }

    Ok(())
}

pub async fn dump_logs(epoch: u64, input_dir: &str, signature: Option<&str>) -> Result<()> {
    let mut stream = OptimizedStream::open(input_dir, epoch).await?;

    let sig_filter = match signature {
        Some(sig_str) => Some(parse_signature_str(sig_str)?),
        None => None,
    };

    let mut scanned_txs = 0u64;
    let mut txs_with_logs = 0u64;
    let mut total_log_lines = 0u64;

    while let Some((block, _payload_len, _frame_len)) = stream.next_block().await? {
        for tx in &block.txs {
            scanned_txs += 1;

            if let Some(filter) = &sig_filter {
                if !tx.signatures.iter().any(|sig| sig == filter) {
                    continue;
                }
            }

            let Some(metadata) = tx.metadata.as_ref() else {
                if sig_filter.is_some() {
                    println!(
                        "Slot {} | Signatures: {}",
                        block.slot,
                        render_signatures(&tx.signatures)
                    );
                    println!("  ⚠️ metadata missing; no logs available");
                    println!();
                }
                continue;
            };

            match metadata {
                CompactMetadataPayload::Compact(meta) => {
                    if let Some(log_stream) = &meta.log_messages {
                        txs_with_logs += 1;
                        println!(
                            "Slot {} | Signatures: {}",
                            block.slot,
                            render_signatures(&tx.signatures)
                        );

                        let decoded = compact_log::decode_logs(
                            log_stream,
                            |pid| format!("program-{pid}"),
                            DecodeConfig {
                                emit_unparsed_lines: true,
                            },
                        );
                        total_log_lines += decoded.len() as u64;
                        for line in decoded {
                            println!("  {line}");
                        }
                        println!();
                    } else if sig_filter.is_some() {
                        println!(
                            "Slot {} | Signatures: {}",
                            block.slot,
                            render_signatures(&tx.signatures)
                        );
                        println!("  ℹ️ compact metadata has no log messages");
                        println!();
                    }
                }
                CompactMetadataPayload::Raw(raw) => {
                    if sig_filter.is_some() {
                        println!(
                            "Slot {} | Signatures: {}",
                            block.slot,
                            render_signatures(&tx.signatures)
                        );
                        println!(
                            "  ⚠️ raw metadata ({} bytes); unable to decode logs",
                            raw.len()
                        );
                        println!();
                    }
                }
            }
        }
    }

    if txs_with_logs == 0 {
        if let Some(sig) = signature {
            println!("No log messages found for epoch {epoch:04} with signature {sig}");
        } else {
            println!("No log messages found for epoch {epoch:04}");
        }
    }

    println!("📜 Log Summary:");
    println!("  Epoch:                  {epoch:04}");
    if let Some(sig) = signature {
        println!("  Filter signature:       {sig}");
    }
    println!("  Transactions scanned:   {scanned_txs}");
    println!("  Transactions with logs: {txs_with_logs}");
    println!("  Total log lines:        {total_log_lines}");

    Ok(())
}
