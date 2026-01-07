use clap::Parser;
use tracing::{Level, info};

use car_reader::{
    car_block_group::CarBlockGroup,
    car_stream::CarStream,
    error::{CarReadError as CarError, CarReadResult as Result},
};

use std::io::{self, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "carread", about = "Stream and read a CAR (.car[.zst]) archive")]
struct Args {
    /// Input CAR file path, URL (http(s)://...), or '-' for stdin.
    /// If omitted, reads from stdin.
    #[arg(value_name = "FILE_OR_URL", default_value = None)]
    input: Option<String>,

    /// Print stats every N seconds
    #[arg(long, default_value_t = 2)]
    stats_every: u64,

    /// Run for N seconds (0 = until EOF)
    #[arg(long, default_value_t = 0)]
    seconds: u64,

    /// Decode transactions and compute TPS
    #[arg(long)]
    decode_tx: bool,

    /// Buffer size for stdin/HTTP reader (bytes)
    #[arg(long, default_value_t = 32 << 20)]
    buf_size: usize,
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
        self.entries += entries_count as u64;
        self.bytes += bytes_size as u64;

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

    let now = Instant::now();
    let dt = now.duration_since(last_print).as_secs_f64();
    if dt > 0.0 && (stats.blocks > 0 || stats.entries > 0) {
        stats.print_interval(dt.max(1e-9), args.decode_tx);
    }

    Ok(())
}

fn looks_like_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

fn has_zst_suffix(s: &str) -> bool {
    s.ends_with(".zst")
}

fn run_stdin(args: &Args) -> Result<()> {
    let stdin = io::stdin();
    let reader = BufReader::with_capacity(args.buf_size, stdin.lock());
    let mut stream = CarStream::from_reader(reader)?;
    run_stream(&mut stream, args)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let args = Args::parse();

    match args.input.as_deref() {
        None => {
            info!("Reading CAR archive: stdin (decode_tx={})", args.decode_tx);
            return run_stdin(&args);
        }
        Some("-") => {
            info!("Reading CAR archive: stdin (decode_tx={})", args.decode_tx);
            return run_stdin(&args);
        }
        Some(input) => {
            info!(
                "Reading CAR archive: {} (decode_tx={})",
                input, args.decode_tx
            );

            if looks_like_url(input) {
                if has_zst_suffix(input) {
                    return Err(CarError::InvalidData(
                        "input looks like a URL ending with .zst, but URL zstd is not supported (download locally or add url+zstd support)".to_string(),
                    ));
                }

                let client = reqwest::blocking::Client::builder()
                    .timeout(None)
                    .no_gzip()
                    .no_brotli()
                    .no_deflate()
                    // optional A/B test for Cloudflare:
                    // .http1_only()
                    .build()
                    .map_err(|e| CarError::Io(format!("build http client: {e}")))?;

                let resp = client
                    .get(input)
                    .header(reqwest::header::ACCEPT_ENCODING, "identity")
                    .send()
                    .map_err(|e| CarError::Io(format!("GET {input}: {e}")))?
                    .error_for_status()
                    .map_err(|e| CarError::Io(format!("GET {input} (http error): {e}")))?;

                let reader = BufReader::with_capacity(args.buf_size, resp);
                let mut stream = CarStream::from_reader(reader)?;
                return run_stream(&mut stream, &args);
            }

            // Local path
            let path = Path::new(input);

            let is_zst = path
                .extension()
                .and_then(|s| s.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("zst"))
                .unwrap_or(false);

            if is_zst {
                let mut stream = CarStream::open_zstd(path)?;
                run_stream(&mut stream, &args)?;
            } else {
                let mut stream = CarStream::open(path)?;
                run_stream(&mut stream, &args)?;
            }
        }
    }

    Ok(())
}
