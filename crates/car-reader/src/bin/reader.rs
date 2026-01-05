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
}

impl Stats {
    #[inline]
    fn reset(&mut self) {
        self.blocks = 0;
        self.entries = 0;
        self.bytes = 0;
        self.txs = 0;
        self.txs_with_meta = 0;
    }

    #[inline]
    fn add_group(&mut self, group: &CarBlockGroup, decode_tx: bool) -> Result<()> {
        self.blocks += 1;

        let (entries_count, bytes_size) = group.get_len();
        // Count entries from the cid_map
        let n_entries = entries_count as u64;
        self.entries += n_entries;

        // Total bytes from the backing buffer
        self.bytes += bytes_size as u64;

        // Optional tx decode
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

fn run_stream<R: std::io::Read>(stream: &mut CarStream<R>, args: &Args) -> Result<()> {
    let stats_every = Duration::from_secs(args.stats_every.max(1));
    let start = Instant::now();
    let end = if args.seconds == 0 {
        None
    } else {
        Some(start + Duration::from_secs(args.seconds))
    };

    let mut stats = Stats::default();
    let mut last_print = Instant::now();

    while let Some(group) = stream.next_group()? {
        stats.add_group(group, args.decode_tx)?;

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
    // Print final partial interval
    let now = Instant::now();
    let dt = now.duration_since(last_print).as_secs_f64();
    if dt > 0.0 && (stats.blocks > 0 || stats.entries > 0) {
        stats.print_interval(dt.max(1e-9), args.decode_tx);
    }
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let args = Args::parse();

    info!(
        "Reading CAR archive: {} (decode_tx={})",
        args.input, args.decode_tx
    );

    let path = Path::new(&args.input);
    if path.extension().unwrap() == "zst" {
        let mut stream = CarStream::open_zstd(path)?;
        run_stream(&mut stream, &args)?;
    } else {
        let mut stream = CarStream::open(path)?;
        run_stream(&mut stream, &args)?;
    };

    Ok(())
}
