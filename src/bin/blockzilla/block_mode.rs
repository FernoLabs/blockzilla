use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use blockzilla::{block_stream::SolanaBlockStream, node::Node};
use indicatif::ProgressBar;
use tokio::time::Instant;
use tracing::{error, info};

use crate::{
    config::LOG_INTERVAL_SECS, reader::build_epoch_reader,
    transaction_parser::VersionedTransaction, types::DownloadMode,
};

pub async fn run_block_mode(file: &PathBuf, download_mode: DownloadMode) -> anyhow::Result<()> {
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
