use anyhow::Result;
use blockzilla::{
    car_reader::AsyncCarReader,
    node::{Node, decode_node},
};
use std::time::Instant;
use tracing::info;

pub async fn run_node_mode(path: &str) -> Result<()> {
    info!("🔄 Reading CAR file: {path}");
    let start = Instant::now();

    let mut stream = AsyncCarReader::<tokio::fs::File>::open(path).await?;

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
