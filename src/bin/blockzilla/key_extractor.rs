use ahash::{AHashMap, AHashSet};
use anyhow::{Result, anyhow};
use blockzilla::{
    block_stream::{CarBlock, SolanaBlockStream},
    node::Node,
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rayon::{prelude::*, ThreadPoolBuilder};
use reqwest::Client;
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::BTreeMap,
    fs,
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    fs as tokio_fs,
    io::{AsyncReadExt, AsyncWriteExt, BufReader as TokioBufReader},
    time::Instant as TokioInstant,
};
use tokio_util::io::StreamReader;
use futures::TryStreamExt;
use tracing::{error, info};
use futures::StreamExt;

use crate::transaction_parser;

const LOG_INTERVAL_SECS: u64 = 10;
const HTTP_BUFFER_SIZE: usize = 4 * 1024 * 1024; // 4MB buffer for better throughput
const CHUNK_CAPACITY: usize = 10_000_000; // 10M keys per chunk before spilling to disk
const TEMP_CHUNK_DIR: &str = ".epoch_chunks";

/// How to handle missing .car files
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadMode {
    /// Never download missing files (strict offline)
    NoDownload,
    /// Stream file directly from remote HTTP without saving
    Stream,
    /// Download and cache file locally before processing
    Cache,
}

/// Stats tracked for each unique pubkey
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct KeyStats {
    pub id: u32,
    pub count: u32,
    pub first_fee_payer: u32,
    pub first_epoch: u16,
    pub last_epoch: u16,
}

/// Global stats shared across threads
struct GlobalStats {
    planned_bytes: u64,
    done_bytes: u64,
    planned_epochs: u64,
    done_epochs: u64,
    skipped_existing: u64,
    skipped_missing: u64,
}

/// Parallel HTTP download using range requests (like aria2c)
pub async fn download_with_parallel_ranges(
    url: &str,
    output_path: &Path,
    client: &Client,
    num_connections: usize,
) -> Result<()> {
    // Get file size first
    let head_resp = client.head(url).send().await?;
    let total_size = head_resp
        .headers()
        .get("content-length")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("Could not determine file size"))?;

    let chunk_size = (total_size / num_connections as u64).max(1024 * 1024); // Min 1MB chunks

    let mut handles = vec![];
    let mut temp_files = vec![];

    for i in 0..num_connections {
        let start = i as u64 * chunk_size;
        let end = if i == num_connections - 1 {
            total_size - 1
        } else {
            (i as u64 + 1) * chunk_size - 1
        };

        if start >= total_size {
            break;
        }

        let url = url.to_string();
        let client = client.clone();
        let temp_path = output_path.with_extension(format!("part-{}", i));
        temp_files.push(temp_path.clone());

        let handle = tokio::spawn(async move {
            let resp = client
                .get(&url)
                .header("Range", format!("bytes={}-{}", start, end))
                .send()
                .await?;

            if !resp.status().is_success() && resp.status().as_u16() != 206 {
                return Err(anyhow!("HTTP error {} for range {}-{}", resp.status(), start, end));
            }

            let mut file = tokio_fs::File::create(&temp_path).await?;
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                file.write_all(&chunk).await?;
            }
            Ok::<(), anyhow::Error>(())
        });

        handles.push(handle);
    }

    // Wait for all downloads to complete
    for handle in handles {
        handle.await??;
    }

    // Concatenate all parts
    let mut output = BufWriter::new(File::create(output_path)?);
    for temp_path in temp_files {
        let mut file = BufReader::new(File::open(&temp_path)?);
        std::io::copy(&mut file, &mut output)?;
        let _ = fs::remove_file(&temp_path);
    }
    output.flush()?;

    Ok(())
}


pub async fn build_epoch_reader(
    base: &Path,
    epoch: u64,
    mode: DownloadMode,
    pb: &ProgressBar,
    client: &Client,
) -> Result<(Box<dyn tokio::io::AsyncRead + Unpin + Send>, u64)> {
    let epoch_source = resolve_epoch_source(base, epoch, mode, pb.clone(), client).await?;
    match epoch_source {
        EpochSource::Local(path) => {
            let file = tokio_fs::File::open(&path).await?;
            let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            let buffered = TokioBufReader::with_capacity(HTTP_BUFFER_SIZE, file);
            Ok((Box::new(buffered), size))
        }
        EpochSource::Stream(url) => {
            let resp = client.get(&url).send().await?;
            if !resp.status().is_success() {
                return Err(anyhow!("HTTP error {} for {}", resp.status(), url));
            }
            let stream = resp.bytes_stream();
            let reader = StreamReader::new(stream.map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("HTTP stream error: {e}"))
            }));
            let buffered = TokioBufReader::with_capacity(HTTP_BUFFER_SIZE, reader);
            Ok((Box::new(buffered), 0))
        }
    }
}

/// Process a single .car (or remote stream) into .bin
pub async fn process_single_epoch_file(
    file: &Path,
    output_dir: &Path,
    download_mode: DownloadMode,
) -> Result<()> {
    let epoch = file
        .file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.split('-').find(|x| x.chars().all(|c| c.is_ascii_digit())))
        .and_then(|num| num.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("Could not parse epoch number from file name"))?;

    fs::create_dir_all(output_dir)?;
    let bin_path = output_dir.join(format!("pubkeys-{epoch:04}.bin"));
    if bin_path.is_dir() {
        return Err(anyhow!(
            "Output path '{}' is a directory, expected a file",
            bin_path.display()
        ));
    }

    let base = file.parent().unwrap_or(Path::new("."));
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("🧱 {spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
    );
    pb.enable_steady_tick(Duration::from_millis(250));

    let client = Client::new();
    let (reader, size) = build_epoch_reader(base, epoch, download_mode, &pb, &client).await?;
    let mut stream = SolanaBlockStream::new(reader).await?;
    let (blocks, count, _) =
        extract_one_epoch(&mut stream, &bin_path, epoch, pb.clone(), size).await?;
    pb.finish_with_message(format!(
        "✅ epoch {epoch:04} processed: {blocks} blocks, {count} unique keys"
    ));
    Ok(())
}

/// Main entry point for all epochs (async version)
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
    let stats = Arc::new(Mutex::new(GlobalStats {
        planned_bytes: 0,
        done_bytes: 0,
        planned_epochs: 0,
        done_epochs: 0,
        skipped_existing: 0,
        skipped_missing: 0,
    }));

    // --- Planning phase: collect work items ---
    let mut work_items: Vec<(u64, u64)> = Vec::new();
    for epoch in 0..=max_epoch_inclusive {
        let bin = out_dir.join(format!("pubkeys-{epoch:04}.bin"));
        if bin.exists() {
            let mut s = stats.lock().unwrap();
            s.skipped_existing += 1;
            continue;
        }

        let car = base.join(format!("epoch-{epoch}.car"));
        if car.exists() {
            let size = fs::metadata(&car).map(|m| m.len()).unwrap_or(0);
            let mut s = stats.lock().unwrap();
            s.planned_epochs += 1;
            s.planned_bytes += size;
            work_items.push((epoch, size));
        } else {
            match download_mode {
                DownloadMode::NoDownload => {
                    let mut s = stats.lock().unwrap();
                    s.skipped_missing += 1;
                }
                DownloadMode::Stream | DownloadMode::Cache => {
                    let mut s = stats.lock().unwrap();
                    s.planned_epochs += 1;
                    work_items.push((epoch, 0));
                }
            }
        }
    }

    work_items.sort_by(|a, b| b.1.cmp(&a.1));

    // --- Header update thread ---
    {
        let stats = Arc::clone(&stats);
        let header = header.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(2));
            let s = match stats.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let elapsed = start_global.elapsed().as_secs_f64().max(0.001);
            let mb_done = s.done_bytes as f64 / 1_000_000.0;
            let mb_total = s.planned_bytes as f64 / 1_000_000.0;
            let speed = mb_done / elapsed;
            let eta_min = if s.planned_bytes > 0 && speed > 0.0 {
                ((mb_total - mb_done).max(0.0) / speed) / 60.0
            } else {
                0.0
            };
            let speed_disp = if speed > 1000.0 {
                format!("{:.2} GB/s", speed / 1000.0)
            } else {
                format!("{:.1} MB/s", speed)
            };
            header.set_message(format!(
                "Done {}/{} | skipped: existing {} / missing {} | {:.1}/{:.1} GB read | {} | ETA {:.1} min",
                s.done_epochs, s.planned_epochs, s.skipped_existing, s.skipped_missing,
                mb_done / 1000.0, mb_total / 1000.0, speed_disp, eta_min
            ));
            if s.done_epochs >= s.planned_epochs {
                break;
            }
        });
    }

    // --- Thread pool setup ---
    let bars: Vec<_> = (0..max_threads)
        .map(|i| {
            let pb = mp.add(ProgressBar::new_spinner());
            pb.set_style(
                ProgressStyle::with_template("🧮 [CPU {prefix}] {spinner:.cyan} {wide_msg}")
                    .unwrap()
                    .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ "),
            );
            pb.set_prefix(format!("{i:02}"));
            pb.enable_steady_tick(Duration::from_millis(250));
            pb
        })
        .collect();

    let pool = ThreadPoolBuilder::new()
        .num_threads(max_threads)
        .build()
        .expect("failed to build rayon pool");

    // Use tokio's existing runtime - spawn concurrent tasks
    let client = Client::new();
    let mut tasks = vec![];

    for (epoch, _size) in work_items {
        let bin_path = out_dir.join(format!("pubkeys-{epoch:04}.bin"));
        let chunk_dir = out_dir.join(format!("{TEMP_CHUNK_DIR}-{epoch}"));
        let base = base.clone();
        let client = client.clone();
        let pb = bars[epoch as usize % max_threads].clone();
        let stats = Arc::clone(&stats);

        let task = tokio::spawn(async move {
            pb.set_message(format!("⏳ epoch {epoch:04} processing..."));
            fs::create_dir_all(&chunk_dir).ok();

            let res = async {
                let (reader, file_size) =
                    build_epoch_reader(&base, epoch, download_mode, &pb, &client).await?;
                let mut stream = SolanaBlockStream::new(reader).await?;
                extract_one_epoch_streamed(&mut stream, &bin_path, &chunk_dir, epoch, pb.clone(), file_size).await
            }.await;

            match res {
                Ok((blocks, count, file_size)) => {
                    let mut s = match stats.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    s.done_epochs += 1;
                    s.done_bytes += file_size;
                    pb.set_message(format!(
                        "✅ epoch {epoch:04} — {:.1} MB",
                        file_size as f64 / 1_000_000.0
                    ));
                    info!(epoch, blocks, keys = count, "Epoch completed successfully");
                }
                Err(e) => {
                    let mut s = match stats.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    s.done_epochs += 1;
                    pb.set_message(format!("❌ epoch {epoch:04} — {e:?}"));
                    error!(epoch, error=?e, "Epoch failed");
                }
            }
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    for task in tasks {
        let _ = task.await;
    }

    let _ = mp.clear();
    let s = match stats.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    println!(
        "\n📊 Summary: done {}/{} | skipped existing {} | skipped missing {} | {:.2} GB read in {:.1?}",
        s.done_epochs, s.planned_epochs, s.skipped_existing, s.skipped_missing,
        s.done_bytes as f64 / 1_000_000_000.0, start_global.elapsed()
    );
    println!("✅ Extraction complete.");
}

/// Extract one epoch worth of pubkeys (delegates to streamed version)
pub async fn extract_one_epoch<R: tokio::io::AsyncRead + Unpin + Send>(
    stream: &mut SolanaBlockStream<R>,
    bin_path: &Path,
    epoch: u64,
    pb: ProgressBar,
    known_file_size: u64,
) -> Result<(u64, usize, u64)> {
    let chunk_dir = PathBuf::from(format!(".{TEMP_CHUNK_DIR}-{epoch}"));
    fs::create_dir_all(&chunk_dir).ok();
    let result = extract_one_epoch_streamed(stream, bin_path, &chunk_dir, epoch, pb, known_file_size).await?;
    let _ = fs::remove_dir_all(&chunk_dir);
    Ok(result)
}

/// Extract one epoch with chunked processing (for very large files)
pub async fn extract_one_epoch_streamed<R: tokio::io::AsyncRead + Unpin + Send>(
    stream: &mut SolanaBlockStream<R>,
    bin_path: &Path,
    chunk_dir: &Path,
    epoch: u64,
    pb: ProgressBar,
    known_file_size: u64,
) -> Result<(u64, usize, u64)> {
    let mut chunk_num = 0u32;
    // Pre-allocate with 20% headroom to reduce rehashing
    let mut current_map: AHashMap<Pubkey, KeyStats> = AHashMap::with_capacity((CHUNK_CAPACITY as f64 * 1.2) as usize);
    let mut next_id: u32 = 1;
    let mut blocks = 0u64;
    let start = TokioInstant::now();
    let mut last_log = start;

    // Process blocks, spilling to disk when chunk gets too large
    while let Some(cb) = stream.next_solana_block().await? {
        extract_transactions(&cb, &mut current_map, &mut next_id, epoch)?;
        blocks += 1;

        let now = TokioInstant::now();
        if now.duration_since(last_log).as_secs() >= LOG_INTERVAL_SECS {
            let elapsed = now.duration_since(start).as_secs_f64();
            let blk_per_s = (blocks as f64) / elapsed;
            pb.set_message(format!(
                "epoch {epoch:04} | {:>7} blk | {:>6.1} blk/s | {:>9} in-mem",
                blocks, blk_per_s, current_map.len()
            ));
            last_log = now;
        }

        // Spill to disk if chunk too large
        if current_map.len() >= CHUNK_CAPACITY {
            let chunk_path = chunk_dir.join(format!("chunk-{chunk_num:06}.bin"));
            dump_chunk_to_bin(&chunk_path, &current_map)?;
            current_map.clear();
            // Shrink the allocation before growing again
            current_map.shrink_to(0);
            chunk_num += 1;
            pb.set_message(format!(
                "epoch {epoch:04} — spilled to disk (chunk {chunk_num})"
            ));
        }
    }

    // Merge all chunks + remaining data
    let final_count = if chunk_num > 0 {
        merge_chunks_and_dump(bin_path, chunk_dir, current_map, chunk_num)?
    } else {
        dump_pubkeys_to_bin(bin_path, &current_map)?;
        current_map.len()
    };

    // Clean up temp chunks
    let _ = fs::remove_dir_all(chunk_dir);

    Ok((blocks, final_count, known_file_size))
}

/// Dump chunk to binary (doesn't need to be sorted)
fn dump_chunk_to_bin(path: &Path, map: &AHashMap<Pubkey, KeyStats>) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writer.write_all(&(map.len() as u64).to_le_bytes())?;
    for (pk, meta) in map {
        writer.write_all(&pk.to_bytes())?;
        writer.write_all(&meta.id.to_le_bytes())?;
        writer.write_all(&meta.count.to_le_bytes())?;
        writer.write_all(&meta.first_fee_payer.to_le_bytes())?;
        writer.write_all(&meta.first_epoch.to_le_bytes())?;
        writer.write_all(&meta.last_epoch.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}

/// Merge all disk chunks + final in-memory map
fn merge_chunks_and_dump(
    bin_path: &Path,
    chunk_dir: &Path,
    final_map: AHashMap<Pubkey, KeyStats>,
    chunk_count: u32,
) -> Result<usize> {
    // Read all chunks into a merged map (using sorted merge to save memory)
    let mut merged: AHashMap<Pubkey, KeyStats> = final_map;
    let mut next_id = merged.values().map(|v| v.id).max().unwrap_or(0) + 1;

    for i in 0..chunk_count {
        let chunk_path = chunk_dir.join(format!("chunk-{i:06}.bin"));
        let file = File::open(&chunk_path)?;
        let mut reader = BufReader::new(file);

        let mut len_bytes = [0u8; 8];
        reader.read_exact(&mut len_bytes)?;
        let len = u64::from_le_bytes(len_bytes) as usize;

        for _ in 0..len {
            let mut pk_bytes = [0u8; 32];
            reader.read_exact(&mut pk_bytes)?;
            let pk = Pubkey::new_from_array(pk_bytes);

            let mut id_bytes = [0u8; 4];
            reader.read_exact(&mut id_bytes)?;
            let id = u32::from_le_bytes(id_bytes);

            let mut count_bytes = [0u8; 4];
            reader.read_exact(&mut count_bytes)?;
            let count = u32::from_le_bytes(count_bytes);

            let mut fee_payer_bytes = [0u8; 4];
            reader.read_exact(&mut fee_payer_bytes)?;
            let fee_payer = u32::from_le_bytes(fee_payer_bytes);

            let mut first_epoch_bytes = [0u8; 2];
            reader.read_exact(&mut first_epoch_bytes)?;
            let first_epoch = u16::from_le_bytes(first_epoch_bytes);

            let mut last_epoch_bytes = [0u8; 2];
            reader.read_exact(&mut last_epoch_bytes)?;
            let last_epoch = u16::from_le_bytes(last_epoch_bytes);

            merged
                .entry(pk)
                .and_modify(|e| {
                    e.count += count;
                    e.last_epoch = e.last_epoch.max(last_epoch);
                })
                .or_insert(KeyStats {
                    id: next_id,
                    count,
                    first_fee_payer: fee_payer,
                    first_epoch,
                    last_epoch,
                });
            next_id += 1;
        }
    }

    dump_pubkeys_to_bin(bin_path, &merged)?;
    Ok(merged.len())
}

/// Source of the epoch file
pub enum EpochSource {
    Local(PathBuf),
    Stream(String),
}

/// Fetch or resolve local/remote epoch source
pub async fn resolve_epoch_source(
    base: &Path,
    epoch: u64,
    mode: DownloadMode,
    pb: ProgressBar,
    client: &Client,
) -> Result<EpochSource> {
    let local_path = base.join(format!("epoch-{epoch}.car"));
    if local_path.exists() {
        return Ok(EpochSource::Local(local_path));
    }

    match mode {
        DownloadMode::NoDownload => Err(anyhow!("epoch-{epoch}.car missing and downloads disabled")),
        DownloadMode::Stream => {
            let url = format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car");
            pb.set_message(format!("🌐 streaming {}", url));
            // Use reqwest with connection pooling for parallel range requests
            Ok(EpochSource::Stream(url))
        }
        DownloadMode::Cache => {
            let url = format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car");
            pb.set_message(format!("🌐 downloading {}", url));
            
            // Use parallel range requests (16 connections like aria2c)
            download_with_parallel_ranges(&url, &local_path, client, 16).await?;
            
            pb.set_message(format!("✅ cached epoch-{epoch}.car"));
            Ok(EpochSource::Local(local_path))
        }
    }
}

/// Extract pubkeys from block transactions
fn extract_transactions(
    cb: &CarBlock,
    map: &mut AHashMap<Pubkey, KeyStats>,
    next_id: &mut u32,
    epoch: u64,
) -> Result<()> {
    for entry in cb.entries.get_block_entries() {
        for tx_cid in &entry.transactions {
            let Some(Node::Transaction(tx)) = cb.entries.get(&tx_cid.0) else { continue };
            let tx_bytes = if tx.data.next.is_none() {
                tx.data.data
            } else {
                &cb.merge_dataframe(tx.data)?
            };
            let mut keys = AHashSet::with_capacity(32);
            let fee_payer_opt = transaction_parser::parse_account_keys_only(tx_bytes, &mut keys)?;
            let fee_payer_id = if let Some(fee_payer) = fee_payer_opt {
                ensure_key(fee_payer, map, next_id, epoch, 0)
            } else {
                0
            };
            for key in keys {
                let entry = map.entry(key).or_insert_with(|| {
                    let id = *next_id;
                    *next_id += 1;
                    KeyStats {
                        id,
                        count: 0,
                        first_fee_payer: fee_payer_id,
                        first_epoch: epoch as u16,
                        last_epoch: epoch as u16,
                    }
                });
                entry.count += 1;
                entry.last_epoch = epoch as u16;
            }
        }
    }
    Ok(())
}

/// Ensure key exists and return ID
fn ensure_key(
    key: Pubkey,
    map: &mut AHashMap<Pubkey, KeyStats>,
    next_id: &mut u32,
    epoch: u64,
    fee_payer_id: u32,
) -> u32 {
    map.entry(key)
        .or_insert_with(|| {
            let id = *next_id;
            *next_id += 1;
            KeyStats {
                id,
                count: 0,
                first_fee_payer: fee_payer_id,
                first_epoch: epoch as u16,
                last_epoch: epoch as u16,
            }
        })
        .id
}

/// Dump to binary file
fn dump_pubkeys_to_bin(path: &Path, map: &AHashMap<Pubkey, KeyStats>) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    writer.write_all(&(map.len() as u64).to_le_bytes())?;
    for (pk, meta) in map {
        writer.write_all(&pk.to_bytes())?;
        writer.write_all(&meta.id.to_le_bytes())?;
        writer.write_all(&meta.count.to_le_bytes())?;
        writer.write_all(&meta.first_fee_payer.to_le_bytes())?;
        writer.write_all(&meta.first_epoch.to_le_bytes())?;
        writer.write_all(&meta.last_epoch.to_le_bytes())?;
    }
    writer.flush()?;
    Ok(())
}