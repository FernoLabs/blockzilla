use anyhow::Result;
use blockzilla::{
    block_stream::SolanaBlockStream,
    rpc_block::{RpcReward, solana_block_to_rpc},
};
use futures_util::io::AllowStdIo;
use std::{fs::File, time::Instant};
use tracing::info;

pub async fn run_rpcblock_mode(path: &str, output_dir: Option<String>) -> Result<()> {
    info!("🔄 Reading CAR file: {path}");
    let start = Instant::now();

    let file = File::open(path)?;
    let reader = AllowStdIo::new(file);
    let mut stream = SolanaBlockStream::new(reader).await?;

    let mut count: u64 = 0;
    let mut last_log = Instant::now();

    while let Some(block) = stream.next_solana_block().await? {
        count += 1;

        let rpc_block = solana_block_to_rpc(
            &block,
            format!("blockhash_{}", block.slot),
            format!("prevhash_{}", block.slot.saturating_sub(1)),
            block.meta.parent_slot,
            None,
            None,
            None::<Vec<RpcReward>>,
        )?;

        if let Some(dir) = &output_dir {
            std::fs::create_dir_all(dir)?;
            let path = format!("{}/{}.json", dir, block.slot);
            std::fs::write(&path, serde_json::to_string(&rpc_block)?)?;
        }

        if count.is_multiple_of(10) || last_log.elapsed().as_secs() >= 5 {
            let elapsed = start.elapsed();
            let rate = count as f64 / elapsed.as_secs_f64().max(0.001);
            info!(
                "[{:>7}] RPC blocks in {:>7.2?} ({:.1} blk/s) — slot {}",
                count, elapsed, rate, block.slot
            );
            last_log = Instant::now();
        }
    }

    let total = start.elapsed();
    let rate = count as f64 / total.as_secs_f64().max(0.001);
    info!(
        "✅ Done. Serialized {count} RPC blocks in {:.2?} ({:.2} blk/s)",
        total, rate
    );

    Ok(())
}
