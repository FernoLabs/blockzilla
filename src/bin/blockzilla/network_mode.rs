//! src/bin/blockzilla/network_mode.rs
use anyhow::Result;
use blockzilla::block_stream::SolanaBlockStream;
use futures::TryStreamExt;
use reqwest::Client;
use solana_transaction_status_client_types::EncodedConfirmedBlock;
use std::{
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio_util::{compat::TokioAsyncReadCompatExt, io::StreamReader};

use crate::optimizer::{KeyRegistry, to_compact_block};

const ZSTD_LEVEL: i32 = 1;
const LOG_EVERY: u64 = 10_000; // print + flush every N blocks
const SAVE_REG_EVERY: u64 = 10_000; // dump registry to SQLite every N blocks

fn extract_epoch_from_url(url: &str) -> u64 {
    url.split('/')
        .last()
        .and_then(|f| f.strip_prefix("epoch-"))
        .and_then(|f| f.strip_suffix(".car"))
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

#[derive(Default)]
struct Timers {
    decode_ms: f64,
    compact_ms: f64,
    serialize_ms: f64,
    compress_ms: f64,
    write_ms: f64,
    blocks: u64,
}
impl Timers {
    fn record(&mut self, d: f64, cpt: f64, s: f64, c: f64, w: f64) {
        self.decode_ms += d;
        self.compact_ms += cpt;
        self.serialize_ms += s;
        self.compress_ms += c;
        self.write_ms += w;
        self.blocks += 1;
    }
    fn print(&self, written_bytes: u64, start: Instant) {
        if self.blocks == 0 {
            return;
        }
        let tot =
            self.decode_ms + self.compact_ms + self.serialize_ms + self.compress_ms + self.write_ms;
        let pct = |x: f64| if tot > 0.0 { x / tot * 100.0 } else { 0.0 };
        let avg_ms = tot / self.blocks as f64;
        let elapsed = start.elapsed().as_secs_f64();
        let throughput = self.blocks as f64 / elapsed;
        println!(
            "🧮 {:>7} blk | decode {:>6.2}% | compact {:>6.2}% | ser {:>6.2}% | comp {:>6.2}% | write {:>6.2}% | avg {:>6.2} ms/blk | {:.2} MB | {:.1} blk/s",
            self.blocks,
            pct(self.decode_ms),
            pct(self.compact_ms),
            pct(self.serialize_ms),
            pct(self.compress_ms),
            pct(self.write_ms),
            avg_ms,
            written_bytes as f64 / 1_000_000.0,
            throughput
        );
    }
}

pub async fn run_network_optimizer(source: &str, output_dir: Option<String>) -> Result<()> {
    println!("🌐 Streaming CAR file from {source}");

    // Output prep
    let out_dir = PathBuf::from(output_dir.unwrap_or_else(|| "optimized".into()));
    fs::create_dir_all(&out_dir)?;
    let epoch = extract_epoch_from_url(source);
    let bin_path = out_dir.join(format!("epoch-{epoch}.bin"));
    let idx_path = out_dir.join(format!("epoch-{epoch}.idx"));
    let reg_path = out_dir.join("registry.sqlite");

    let bin_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&bin_path)?;
    let mut bin = BufWriter::with_capacity(8 * 1024 * 1024, bin_file);
    let idx_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&idx_path)?;
    let mut idx = BufWriter::with_capacity(1 * 1024 * 1024, idx_file);

    // HTTP stream → AsyncRead
    let client = Client::new();
    let resp = client.get(source).send().await?.error_for_status()?;
    let reader = StreamReader::new(
        resp.bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    )
    .compat();

    // CAR → block stream
    let mut stream = SolanaBlockStream::new(reader).await?;

    // Registry + counters
    let mut reg = KeyRegistry::new();
    let mut count: u64 = 0;
    let mut offset: u64 = 0;
    let mut timers = Timers::default();
    let start = Instant::now();

    // Main single-threaded loop
    while let Some(car_block) = stream.next_solana_block().await? {
        // decode CAR -> EncodedConfirmedBlock
        let t0 = Instant::now();
        let rpc_block: EncodedConfirmedBlock = car_block.try_into()?;
        let t1 = Instant::now();

        // to_compact_block (walk + map pubkeys)
        let compact = to_compact_block(&rpc_block, &mut reg)?;
        let t2 = Instant::now();

        // serialize (postcard)
        let raw = postcard::to_allocvec(&compact)?;
        let t3 = Instant::now();

        // compress (zstd)
        let compressed = zstd::bulk::compress(&raw, ZSTD_LEVEL)?;
        let t4 = Instant::now();

        // write (bin + idx)
        let len = compressed.len() as u32;
        bin.write_all(&len.to_le_bytes())?;
        bin.write_all(&compressed)?;
        idx.write_all(&compact.slot.to_le_bytes())?;
        idx.write_all(&offset.to_le_bytes())?;
        offset += 4 + compressed.len() as u64;
        let t5 = Instant::now();

        // per-block timings (ms)
        let d = (t1 - t0).as_secs_f64() * 1000.0;
        let cp = (t2 - t1).as_secs_f64() * 1000.0;
        let s = (t3 - t2).as_secs_f64() * 1000.0;
        let c = (t4 - t3).as_secs_f64() * 1000.0;
        let w = (t5 - t4).as_secs_f64() * 1000.0;
        timers.record(d, cp, s, c, w);
        if count % 100 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let blk_s = count as f64 / elapsed;
            let total_ms = d + cp + s + c + w;
            println!(
                "⏱️ blk {:>7} | decode {:>6.2}ms | compact {:>6.2} | ser {:>5.2} | comp {:>5.2} | write {:>5.2} | total {:>6.2}ms | {:>6.1} blk/s",
                count, d, cp, s, c, w, total_ms, blk_s
            );
        }

        count += 1;
        if count % LOG_EVERY == 0 {
            bin.flush()?;
            idx.flush()?;
            reg.save_to_sqlite(reg_path.to_str().unwrap())?;
            timers.print(offset, start);
        }
    }

    // Finalize
    bin.flush()?;
    idx.flush()?;
    reg.save_to_sqlite(reg_path.to_str().unwrap())?;

    timers.print(offset, start);
    println!(
        "🏁 Done: {} blocks → {}, {:.2} MB, {:.1} blk/s",
        count,
        bin_path.display(),
        offset as f64 / 1_000_000.0,
        (count as f64 / start.elapsed().as_secs_f64()).max(0.0)
    );

    Ok(())
}
