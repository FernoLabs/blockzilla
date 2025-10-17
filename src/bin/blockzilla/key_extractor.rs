use ahash::AHashMap;
use anyhow::Result;
use blockzilla::{block_stream::SolanaBlockStream, node::Node};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use solana_sdk::pubkey::Pubkey;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::transaction_parser::{self, VersionedTransaction};
use crate::{
    config::{LOG_INTERVAL_SECS, TEMP_CHUNK_DIR},
    merge::{dump_chunk_to_bin, merge_chunks_and_dump},
    reader::build_epoch_reader,
    types::{DownloadMode, KeyStats},
};

pub async fn extract_all_pubkeys(
    base: &PathBuf,
    out_dir: &PathBuf,
    max_epoch_inclusive: u64,
    max_threads: usize,
    download_mode: DownloadMode,
) {
    fs::create_dir_all(out_dir).expect("failed to create output dir");

    let mp = Arc::new(MultiProgress::new());
    let header = mp.add(ProgressBar::new_spinner());
    header.set_style(
        ProgressStyle::with_template("🌍 {spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    header.enable_steady_tick(Duration::from_millis(250));

    let start_global = Instant::now();

    // Collect epochs to process
    let mut work_items: Vec<u64> = vec![];
    for epoch in 0..=max_epoch_inclusive {
        let bin = out_dir.join(format!("pubkeys-{epoch:04}.bin"));
        if bin.exists() {
            continue;
        }
        work_items.push(epoch);
    }

    // ----------------------------
    // 🔹 PHASE 1: Parallel extraction
    // ----------------------------
    let limit = Arc::new(Semaphore::new(max_threads));
    let client = Client::new();
    let mut tasks = vec![];
    let chunk_capacity = crate::config::auto_chunk_capacity(max_threads);

    for epoch in work_items.clone() {
        let permit = Arc::clone(&limit).acquire_owned().await.unwrap();
        let client = client.clone();
        let base = base.clone();
        let out_dir = out_dir.clone();

        let pb = mp.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::with_template("🧮 [epoch {prefix}] {spinner:.cyan} {wide_msg}")
                .unwrap()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
        );
        pb.set_prefix(format!("{epoch:04}"));
        pb.enable_steady_tick(Duration::from_millis(250));

        let task = tokio::spawn(async move {
            let _permit = permit;
            pb.set_message(format!("⏳ epoch {epoch:04} processing..."));
            let chunk_dir = out_dir.join(format!("{TEMP_CHUNK_DIR}-{epoch}"));
            fs::create_dir_all(&chunk_dir).ok();

            let res = async {
                let (reader, file_size) =
                    build_epoch_reader(&base, epoch, download_mode, &pb, &client).await?;
                let mut stream = SolanaBlockStream::new(reader).await?;
                extract_one_epoch_streamed(
                    &mut stream,
                    &out_dir.join(format!("pubkeys-{epoch:04}.bin")),
                    &chunk_dir,
                    chunk_capacity,
                    epoch,
                    pb.clone(),
                    file_size,
                )
                .await
            }
            .await;

            match res {
                Ok((blocks, count, file_size)) => {
                    pb.finish_with_message(format!(
                        "✅ epoch {epoch:04} — {blocks} blk, {count} keys ({:.1} MB)",
                        file_size as f64 / 1_000_000.0
                    ));
                    info!(epoch, blocks, keys = count, "Epoch completed successfully");
                }
                Err(e) => {
                    //pb.finish_with_message(format!("❌ epoch {epoch:04} — {e:?}"));
                    error!(epoch, error=?e, "Epoch failed");
                }
            }
        });

        tasks.push(task);
    }

    // Wait for all epochs to finish
    for task in tasks {
        let _ = task.await;
    }

    mp.clear().ok();
    println!("\n🧩 Starting single-threaded merge phase...");

    // ----------------------------
    // 🔹 PHASE 2: Sequential merge
    // ----------------------------

    for epoch in work_items {
        let chunk_dir = out_dir.join(format!("{TEMP_CHUNK_DIR}-{epoch}"));
        if !chunk_dir.exists() {
            continue;
        }

        let bin_path = out_dir.join(format!("pubkeys-{epoch:04}.bin"));
        if bin_path.exists() {
            continue;
        }

        let merge_pb = ProgressBar::new_spinner();
        merge_pb.set_style(
            ProgressStyle::with_template("🧩 [epoch {prefix}] {spinner:.cyan} {wide_msg}")
                .unwrap()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
        );
        merge_pb.set_prefix(format!("{epoch:04}"));
        merge_pb.enable_steady_tick(Duration::from_millis(250));

        // Count chunks first for nicer progress
        let chunk_count = fs::read_dir(&chunk_dir)
            .map(|rd| rd.count() as u32)
            .unwrap_or(0);

        merge_pb.set_message(format!("merging {chunk_count} chunks..."));

        let current_map = AHashMap::new();
        match crate::merge::merge_chunks_and_dump_singlethread(
            &bin_path,
            &chunk_dir,
            current_map,
            chunk_count,
            epoch,
        ) {
            Ok(count) => {
                merge_pb.finish_with_message(format!(
                    "✅ epoch {epoch:04} merged successfully ({count} keys)"
                ));
                fs::remove_dir_all(&chunk_dir).ok();
            }
            Err(e) => {
                merge_pb.finish_with_message(format!("❌ epoch {epoch:04} merge failed: {e:?}"));
            }
        }
    }

    println!("✅ Extraction + merge complete.");
}

/// Stream-based extraction + chunk merging
pub async fn extract_one_epoch_streamed<R: tokio::io::AsyncRead + Unpin + Send>(
    stream: &mut SolanaBlockStream<R>,
    bin_path: &PathBuf,
    chunk_dir: &PathBuf,
    chunk_capacity: usize,
    epoch: u64,
    pb: ProgressBar,
    known_file_size: u64,
) -> Result<(u64, usize, u64)> {
    let mut chunk_num = 0;
    let mut current_map: AHashMap<Pubkey, KeyStats> = AHashMap::with_capacity(chunk_capacity);
    let mut next_id: u32 = 1;
    let mut blocks = 0u64;
    let start = Instant::now();
    let mut last_log = start;

    while let Some(cb) = stream.next_solana_block().await? {
        transaction_parser::extract_transactions(&cb, &mut current_map, &mut next_id, epoch)?;
        blocks += 1;

        let now = Instant::now();
        if now.duration_since(last_log).as_secs() >= LOG_INTERVAL_SECS {
            let elapsed = now.duration_since(start).as_secs_f64();
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>9} in-mem",
                blocks,
                blocks as f64 / elapsed,
                current_map.len()
            ));
            last_log = now;
        }

        if current_map.len() >= chunk_capacity {
            let chunk_path = chunk_dir.join(format!("chunk-{chunk_num:06}.bin"));
            dump_chunk_to_bin(&chunk_path, &current_map)?;
            current_map.clear();
            current_map.shrink_to(0);
            chunk_num += 1;
            pb.set_message(format!(
                "epoch {epoch:04} — spilled to disk (chunk {chunk_num})"
            ));
        }
    }

    let final_count = if chunk_num > 0 {
        merge_chunks_and_dump(bin_path, chunk_dir, current_map, chunk_num)?
    } else {
        crate::merge::dump_pubkeys_to_bin(bin_path, &current_map)?;
        current_map.len()
    };

    fs::remove_dir_all(chunk_dir).ok();
    Ok((blocks, final_count, known_file_size))
}

pub async fn process_single_epoch_file(
    file: &PathBuf,
    output_dir: &PathBuf,
    download_mode: DownloadMode,
) -> Result<()> {
    let epoch = file
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.split('-').find(|x| x.chars().all(|c| c.is_ascii_digit())))
        .and_then(|num| num.parse::<u64>().ok())
        .ok_or_else(|| anyhow::anyhow!("Could not parse epoch number from file name"))?;

    let base = file.parent().unwrap_or(Path::new("."));

    let client = reqwest::Client::new();
    let pb = ProgressBar::new_spinner();
    let (reader, file_size) = build_epoch_reader(&base, epoch, download_mode, &pb, &client).await?;
    let mut stream = SolanaBlockStream::new(reader).await?;
    let mut blocks_count = 0;
    let mut bytes_count = 0;
    let mut tx_count = 0;
    let start = Instant::now();
    let mut last_log = Instant::now();

    while let Some(cb) = stream.next_solana_block().await? {
        for entry in cb.entries.get_block_entries() {
            for tx_cid in &entry.transactions {
                let Some(Node::Transaction(tx)) = cb.entries.get(&tx_cid.0) else {
                    error!("Got a nin tx entry");
                    continue;
                };
                // Merge dataframes if fragmented
                let tx_bytes = if tx.data.next.is_none() {
                    tx.data.data
                } else {
                    &cb.merge_dataframe(tx.data)?
                };
                bytes_count += tx_bytes.len();
                let vt: VersionedTransaction = wincode::deserialize(&tx_bytes)?;
                tx_count += 1;
            }
        }
        blocks_count += 1;
        let now = Instant::now();

        if now.duration_since(last_log) > Duration::from_secs(LOG_INTERVAL_SECS) {
            last_log = now;
            let elapsed = now.duration_since(start);
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {} MB/s | {} avg blk size | {} TPS",
                blocks_count,
                blocks_count as f64 / elapsed.as_secs_f64(),
                bytes_count as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
                bytes_count / blocks_count,
                tx_count / elapsed.as_secs()
            ));
        }
    }

    let now = Instant::now();
    let elapsed = now.duration_since(start);
    let message = format!(
        "DONE epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>5.2} MB/s | {} avg blk size  | {} TPS",
        blocks_count,
        blocks_count as f64 / elapsed.as_secs_f64(),
        bytes_count as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64(),
        bytes_count / blocks_count,
        tx_count / elapsed.as_secs()
    );
    info!("{}", message);

    Ok(())
}
