use clap::Parser;
use tracing::{Level, info};

use car_reader::{
    car_block_group::CarBlockGroup,
    car_stream::CarStream,
    error::{CarReadError as CarError, CarReadResult as Result},
};

use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "carread", about = "Stream and read a CAR (.car.zst) archive")]
struct Args {
    /// Input CAR file path (.car.zst)
    #[arg(value_name = "FILE")]
    input: String,

    /// Print stats every N seconds
    #[arg(long, default_value_t = 2)]
    stats_every: u64,

    /// Run for N seconds (0 = until EOF)
    #[arg(long, default_value_t = 60)]
    seconds: u64,

    /// Decode transactions and compute TPS
    #[arg(long)]
    decode_tx: bool,
}

#[derive(Default)]
struct Stats {
    blocks: u64,
    entries: u64,
    bytes: u64,
    txs: u64,
    txs_with_meta: u64,
    // cache the CID length once (same within file for your format)
    cid_len: Option<u64>,
}

impl Stats {
    #[inline]
    fn reset(&mut self) {
        self.blocks = 0;
        self.entries = 0;
        self.bytes = 0;
        self.txs = 0;
        self.txs_with_meta = 0;
        // keep cid_len cached across intervals
    }

    #[inline]
    fn add_group(&mut self, group: &CarBlockGroup, decode_tx: bool) -> Result<()> {
        self.blocks += 1;

        // entries + bytes
        let n_entries = group.payloads.len() as u64;
        self.entries += n_entries;

        if self.cid_len.is_none() {
            // Avoid walking the whole map. If you really need it, this is O(1) average,
            // but still touches the hash map. You can also just hardcode if fixed.
            self.cid_len = group.cid_map.keys().next().map(|cid| cid.len() as u64);
        }
        let cid_len = self.cid_len.unwrap_or(0);

        // payload bytes: still a sum, but it's just iterating a Vec<Bytes>
        let payload_bytes: u64 = group.payloads.iter().map(|p| p.len() as u64).sum();
        self.bytes += payload_bytes + cid_len * n_entries;

        // optional tx decode
        if decode_tx {
            let mut it = group.transactions().map_err(|e| {
                CarError::InvalidData(format!("transaction iteration failed: {e:?}"))
            })?;

            while let Some((_tx, maybe_meta)) = it
                .next_tx()
                .map_err(|e| CarError::InvalidData(format!("transaction decode failed: {e:?}")))?
            {
                self.txs += 1;
                if maybe_meta.is_some() {
                    self.txs_with_meta += 1;
                }
            }
        }

        Ok(())
    }

    fn print_interval(&self, dt: f64, decode_tx: bool) {
        let mib_s = (self.bytes as f64 / (1024.0 * 1024.0)) / dt;
        let blocks_s = (self.blocks as f64) / dt;
        let entries_s = (self.entries as f64) / dt;

        if decode_tx {
            let tps = (self.txs as f64) / dt;
            let meta_pct = if self.txs > 0 {
                (self.txs_with_meta as f64 / self.txs as f64) * 100.0
            } else {
                0.0
            };
            info!(
                "read: {:.1} MiB/s | {:.0} blocks/s | {:.0} tx/s ({:.1}% meta) | {:.0} entries/s",
                mib_s, blocks_s, tps, meta_pct, entries_s
            );
        } else {
            info!(
                "read: {:.1} MiB/s | {:.0} blocks/s | {:.0} entries/s",
                mib_s, blocks_s, entries_s
            );
        }
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let args = Args::parse();

    info!(
        "Reading CAR archive: {} (decode_tx={})",
        args.input, args.decode_tx
    );

    let stats_every = Duration::from_secs(args.stats_every.max(1));
    let start = Instant::now();
    let end = if args.seconds == 0 {
        None
    } else {
        Some(start + Duration::from_secs(args.seconds))
    };

    let mut stats = Stats::default();
    let mut last_print = Instant::now();

    let mut stream = CarStream::open_zstd(Path::new(&args.input))?;
    while let Some(group) = stream.next_group()? {
        stats.add_group(&group, args.decode_tx)?;

        let now = Instant::now();
        if now.duration_since(last_print) >= stats_every {
            let dt = now.duration_since(last_print).as_secs_f64().max(1e-9);
            stats.print_interval(dt, args.decode_tx);
            stats.reset();
            last_print = now;
        }

        if end.map_or(false, |dl| now >= dl) {
            break;
        }
    }

    // Print final partial interval (optional, but useful)
    let now = Instant::now();
    let dt = now.duration_since(last_print).as_secs_f64();
    if dt > 0.0 && (stats.blocks > 0 || stats.entries > 0) {
        stats.print_interval(dt.max(1e-9), args.decode_tx);
    }

    Ok(())
}
