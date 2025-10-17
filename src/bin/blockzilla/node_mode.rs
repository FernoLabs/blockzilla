use anyhow::Result;
use blockzilla::{
    car_reader::AsyncCarReader,
    node::{Node, decode_node},
};
use indicatif::ProgressBar;
use std::{
    path::{Path, PathBuf},
    time::Instant,
};
use tracing::info;

use crate::{reader::build_epoch_reader, types::DownloadMode};

pub async fn run_node_mode(file: &PathBuf, download_mode: DownloadMode) -> Result<()> {
    info!("🔄 Reading CAR file: {file:?} ({download_mode:?})");
    let start = Instant::now();

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

    let mut stream = AsyncCarReader::new(reader);
    stream.read_header().await?;

    let mut total: u64 = 0;
    let mut tx_count: u64 = 0;
    let mut entry_count: u64 = 0;
    let mut block_count: u64 = 0;
    let mut subset_count: u64 = 0;
    let mut epoch_count: u64 = 0;
    let mut rewards_count: u64 = 0;
    let mut dataframe_count: u64 = 0;

    let mut last_log = Instant::now();
    let log_interval = 10.0; // seconds

    while let Some(car_block) = stream.next_block().await? {
        let node = decode_node(&car_block.data)?;
        total += 1;

        match &node {
            Node::Transaction(_) => tx_count += 1,
            Node::Entry(_) => entry_count += 1,
            Node::Block(_) => block_count += 1,
            Node::Subset(_) => subset_count += 1,
            Node::Epoch(_) => epoch_count += 1,
            Node::Rewards(_) => rewards_count += 1,
            Node::DataFrame(_) => dataframe_count += 1,
        }

        if last_log.elapsed().as_secs_f64() >= log_interval {
            let elapsed = start.elapsed().as_secs_f64();
            let total_rate = total as f64 / elapsed;
            let block_rate = block_count as f64 / elapsed;

            info!(
                "[{:>10}] nodes in {:>6.2?} ({:>8.0} n/s, {:>6.1} blk/s) \
                 Tx:{} Entry:{} Block:{} Subset:{} Epoch:{} Rewards:{} DF:{}",
                total,
                std::time::Duration::from_secs_f64(elapsed),
                total_rate,
                block_rate,
                tx_count,
                entry_count,
                block_count,
                subset_count,
                epoch_count,
                rewards_count,
                dataframe_count
            );

            last_log = Instant::now();
        }
    }

    let total_time = start.elapsed().as_secs_f64();
    let total_rate = total as f64 / total_time;
    let block_rate = block_count as f64 / total_time;

    info!(
        "✅ Done. Parsed {total} nodes in {:.2?} ({:.0} n/s, {:.1} blk/s)",
        std::time::Duration::from_secs_f64(total_time),
        total_rate,
        block_rate
    );

    info!(
        "📊 Final counts — Tx:{} Entry:{} Block:{} Subset:{} Epoch:{} Rewards:{} DF:{}",
        tx_count,
        entry_count,
        block_count,
        subset_count,
        epoch_count,
        rewards_count,
        dataframe_count
    );

    Ok(())
}

pub async fn run_car_mode(file: &PathBuf, download_mode: DownloadMode) -> Result<()> {
    info!("🔄 Reading CAR file: {file:?} ({download_mode:?})");
    let start = Instant::now();

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
    info!("Will read {file_size} bytes");

    let mut stream = AsyncCarReader::new(reader);
    stream.read_header().await?;

    let mut last_log = Instant::now();
    let log_interval = 10.0; // seconds
    let mut total = 0;
    let mut bytes_count = 0;

    while let Some(car_block) = stream.next_block().await? {
        total += 1;
        bytes_count += car_block.data.len();

        if last_log.elapsed().as_secs_f64() >= log_interval {
            let elapsed = start.elapsed().as_secs_f64();
            let total_rate = total as f64 / elapsed;

            info!(
                "{:>10} car entry in {:>6.2?} ({:>8.0} n/s) {:>5.2} MB/s",
                total,
                std::time::Duration::from_secs_f64(elapsed),
                total_rate,
                bytes_count as f64 / elapsed
            );

            last_log = Instant::now();
        }
    }

    let total_time = start.elapsed().as_secs_f64();
    let total_rate = total as f64 / total_time;

    info!(
        "✅ Done. Parsed {total} nodes in {:.2?} ({:.0} n/s)",
        std::time::Duration::from_secs_f64(total_time),
        total_rate,
    );

    Ok(())
}
