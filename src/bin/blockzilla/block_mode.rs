use anyhow::Result;
use blockzilla::block_stream::SolanaBlockStream;
use futures_util::io::AllowStdIo;
use std::{fs::File, time::Instant};
use tracing::info;

pub async fn run_block_mode(path: &str) -> Result<()> {
    info!("🔄 Reading CAR file: {path}");
    let start = Instant::now();

    let file = File::open(path)?;
    let reader = AllowStdIo::new(file);
    let mut stream = SolanaBlockStream::new(reader).await?;

    info!("Header: {:?}", stream.header());

    let mut count: u64 = 0;
    let mut total_entries: u64 = 0;
    let mut total_txs: u64 = 0;
    let mut last_log = Instant::now();

    while let Some(block) = stream.next_solana_block().await? {
        count += 1;

        total_entries += block.entries.len() as u64;
        total_txs += block.transactions.len() as u64;

        if count.is_multiple_of(10) || last_log.elapsed().as_secs() >= 5 {
            let elapsed = start.elapsed();
            let rate = count as f64 / elapsed.as_secs_f64().max(0.001);

            info!(
                "[{:>7}] blocks in {:>7.2?} ({:.1} blk/s) — slot {} ({} entries, {} txs)",
                count,
                elapsed,
                rate,
                block.slot,
                block.entries.len(),
                block.transactions.len()
            );
            last_log = Instant::now();
        }
    }

    let total = start.elapsed();
    let rate = count as f64 / total.as_secs_f64().max(0.001);
    let avg_e = total_entries as f64 / count.max(1) as f64;
    let avg_t = total_txs as f64 / count.max(1) as f64;

    info!(
        "✅ Done. Parsed {count} blocks in {:.2?} ({:.2} blk/s) avg=({:.1} e/b, {:.1} tx/b)",
        total, rate, avg_e, avg_t
    );

    Ok(())
}
