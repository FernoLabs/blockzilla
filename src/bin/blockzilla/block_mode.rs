use anyhow::Result;
use blockzilla::block_stream::SolanaBlockStream;
use solana_transaction_status_client_types::EncodedConfirmedBlock;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tracing::info;

#[derive(Default)]
struct StageStats {
    decode_ms: f64,
    compact_ms: f64,
    serialize_ms: f64,
    compress_ms: f64,
    blocks: u64,
}

impl StageStats {
    fn record(&mut self, d: f64, cpt: f64, s: f64, c: f64) {
        self.decode_ms += d;
        self.compact_ms += cpt;
        self.serialize_ms += s;
        self.compress_ms += c;
        self.blocks += 1;
    }

    fn print_periodic(&self, count: u64, bytes: u64, start: Instant) {
        if self.blocks == 0 {
            info!("🧮 {:>7} blk | collecting…", count);
            return;
        }
        let total = self.decode_ms + self.compact_ms + self.serialize_ms + self.compress_ms;
        let pct = |x: f64| if total > 0.0 { x / total * 100.0 } else { 0.0 };
        let avg_ms = total / self.blocks as f64;
        let throughput = count as f64 / start.elapsed().as_secs_f64().max(0.001);
        info!(
            "🧮 {:>7} blk | decode {:>6.2}% | compact {:>6.2}% | ser {:>6.2}% | comp {:>6.2}% | avg {:>6.2} ms/blk | {:.1} blk/s | {:.2} MB",
            count,
            pct(self.decode_ms),
            pct(self.compact_ms),
            pct(self.serialize_ms),
            pct(self.compress_ms),
            avg_ms,
            throughput,
            bytes as f64 / 1_000_000.0
        );
    }
}

pub async fn run_block_mode(path: &str) -> Result<()> {
    info!("🔄 Reading CAR file: {path}");
    let start = Instant::now();

    let file = File::open(path).await?;
    let mut stream = SolanaBlockStream::new(file).await?;

    let mut count: u64 = 0;
    let mut total_entries: u64 = 0;
    let mut total_txs: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut last_log = Instant::now();

    let mut timings = StageStats::default();

    while let Some(block) = stream.next_solana_block().await? {
        let entries_count = block.entries.index.len() as u64; // ✅ store before move
        let t0 = Instant::now();
        let rpc_block: EncodedConfirmedBlock = block.try_into()?; // consumes `block`
        let t1 = Instant::now();

        // --- Intermediate log #1 (after decode)
        if count.is_multiple_of(100) {
            let ms = (t1 - t0).as_secs_f64() * 1000.0;
            info!("blk {count:>6} | decode took {ms:.2} ms");
        }

        // Compact / serialize / compress instrumentation
        let t2 = Instant::now();
        let compact_bytes = postcard::to_allocvec(&rpc_block)?;
        let t3 = Instant::now();
        let compressed = zstd::bulk::compress(&compact_bytes, 1)?;
        let t4 = Instant::now();

        // --- Intermediate log #2 (after compression)
        if count.is_multiple_of(100) {
            let decode_ms = (t1 - t0).as_secs_f64() * 1000.0;
            let compact_ms = (t2 - t1).as_secs_f64() * 1000.0;
            let serialize_ms = (t3 - t2).as_secs_f64() * 1000.0;
            let compress_ms = (t4 - t3).as_secs_f64() * 1000.0;
            let total_ms = (t4 - t0).as_secs_f64() * 1000.0;
            info!(
                "⏱️ blk {:>6} | decode {:>6.2} | compact {:>6.2} | serialize {:>6.2} | compress {:>6.2} | total {:>6.2}",
                count, decode_ms, compact_ms, serialize_ms, compress_ms, total_ms
            );
        }

        timings.record(
            (t1 - t0).as_secs_f64() * 1000.0,
            (t2 - t1).as_secs_f64() * 1000.0,
            (t3 - t2).as_secs_f64() * 1000.0,
            (t4 - t3).as_secs_f64() * 1000.0,
        );

        count += 1;
        total_entries += entries_count; // ✅ use stored count
        total_txs += rpc_block.transactions.len() as u64;
        total_bytes += compressed.len() as u64;

        // --- Periodic cumulative log
        if last_log.elapsed() > Duration::from_secs(5) || count.is_multiple_of(1000) {
            timings.print_periodic(count, total_bytes, start);
            last_log = Instant::now();
        }
    }

    let elapsed = start.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64().max(0.001);
    let avg_entries = total_entries as f64 / count.max(1) as f64;
    let avg_txs = total_txs as f64 / count.max(1) as f64;

    timings.print_periodic(count, total_bytes, start);
    info!(
        "✅ Done. Parsed {count} blocks in {:.2?} ({:.2} blk/s) avg=({:.1} entries/block, {:.1} tx/block)",
        elapsed, rate, avg_entries, avg_txs
    );

    Ok(())
}
