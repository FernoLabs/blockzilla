use anyhow::Result;
use blockzilla::CarStream;
use futures_util::io::AllowStdIo;
use std::{fs::File, time::Instant};
use tracing::info;

pub async fn run_node_mode(path: &str) -> Result<()> {
    info!("🔄 Reading CAR file: {path}");
    let start = Instant::now();

    let file = File::open(path)?;
    let reader = AllowStdIo::new(file);
    let mut stream = CarStream::new(reader).await?;

    info!("Header: {:?}", stream.header());

    let mut total: u64 = 0;
    let mut tx_count: u64 = 0;
    let mut entry_count: u64 = 0;
    let mut block_count: u64 = 0;
    let mut subset_count: u64 = 0;
    let mut epoch_count: u64 = 0;
    let mut rewards_count: u64 = 0;
    let mut dataframe_count: u64 = 0;

    let mut last_log = Instant::now();

    while let Some((_cid, node)) = stream.next().await? {
        total += 1;

        match &node {
            blockzilla::Node::Transaction(_) => tx_count += 1,
            blockzilla::Node::Entry(_) => entry_count += 1,
            blockzilla::Node::Block(_) => block_count += 1,
            blockzilla::Node::Subset(_) => subset_count += 1,
            blockzilla::Node::Epoch(_) => epoch_count += 1,
            blockzilla::Node::Rewards(_) => rewards_count += 1,
            blockzilla::Node::DataFrame(_) => dataframe_count += 1,
        }

        if total.is_multiple_of(10_000) || last_log.elapsed().as_secs() >= 5 {
            let elapsed = start.elapsed();
            let rate = total as f64 / elapsed.as_secs_f64().max(0.001);

            info!(
                "[{:>10}] nodes in {:>6.2?} ({:.2} n/s) \
                 Tx:{} Entry:{} Block:{} Subset:{} Epoch:{} Rewards:{} DF:{}",
                total,
                elapsed,
                rate,
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

    let total_time = start.elapsed();
    let rate = total as f64 / total_time.as_secs_f64().max(0.001);

    info!(
        "✅ Done. Parsed {total} nodes in {:.2?} ({:.2} n/s)",
        total_time, rate
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
