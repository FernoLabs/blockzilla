use clap::Parser;
use tracing::{Level, info};

use car_reader::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    error::{CarReadError as CarError, CarReadResult as Result},
};

use crossbeam_channel as chan;

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
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

    /// Buffer size for stdin/HTTP/file reader (bytes)
    #[arg(long, default_value_t = 32 << 20)]
    buf_size: usize,

    /// Number of worker threads (1 = single-threaded)
    #[arg(short = 'j', long, default_value_t = 1)]
    jobs: usize,
}

#[derive(Default, Clone, Debug)]
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
    fn merge_from(&mut self, other: &Stats) {
        self.blocks += other.blocks;
        self.entries += other.entries;
        self.bytes += other.bytes;
        self.txs += other.txs;
        self.txs_with_meta += other.txs_with_meta;
    }

    fn print_interval(&self, dt: f64, decode_tx: bool) {
        let dt = dt.max(1e-9);
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

    fn print_final(&self, dt: f64, decode_tx: bool) {
        let dt = dt.max(1e-9);
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
                "total: {:.1}s | {:.1} MiB/s | {:.0} blocks/s | {:.0} tx/s ({:.1}% meta) | {:.0} entries/s",
                dt, mib_s, blocks_s, tps, meta_pct, entries_s
            );
        } else {
            info!(
                "total: {:.1}s | {:.1} MiB/s | {:.0} blocks/s | {:.0} entries/s",
                dt, mib_s, blocks_s, entries_s
            );
        }
    }
}

fn looks_like_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

fn has_zst_suffix(s: &str) -> bool {
    s.ends_with(".zst")
}

/// -------- pools (duplicated types, minimal) --------

pub struct CarBlockGroupPoolSafe {
    inner: Mutex<Vec<CarBlockGroup>>,
    cv: Condvar,
}
impl CarBlockGroupPoolSafe {
    pub fn with_capacity(pool_size: usize) -> Self {
        let mut v = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            v.push(CarBlockGroup::new());
        }
        Self {
            inner: Mutex::new(v),
            cv: Condvar::new(),
        }
    }
    pub fn checkout(self: &Arc<Self>) -> PooledGroupSafe {
        let mut guard = self.inner.lock().unwrap();
        loop {
            if let Some(mut g) = guard.pop() {
                g.clear();
                return PooledGroupSafe {
                    pool: Arc::clone(self),
                    group: Some(g),
                };
            }
            guard = self.cv.wait(guard).unwrap();
        }
    }
    fn put_back(&self, mut g: CarBlockGroup) {
        g.clear();
        let mut guard = self.inner.lock().unwrap();
        guard.push(g);
        self.cv.notify_one();
    }
}
pub struct PooledGroupSafe {
    pool: Arc<CarBlockGroupPoolSafe>,
    group: Option<CarBlockGroup>,
}
impl PooledGroupSafe {
    #[inline]
    pub fn as_mut(&mut self) -> &mut CarBlockGroup {
        self.group.as_mut().unwrap()
    }
}
impl Drop for PooledGroupSafe {
    fn drop(&mut self) {
        if let Some(g) = self.group.take() {
            self.pool.put_back(g);
        }
    }
}

/// Small stats report emitted by a worker thread.
#[derive(Clone, Debug)]
struct WorkerReport {
    worker_id: usize,
    dt: f64,
    stats: Stats,
}

/// Simple sequential CAR group reader
struct CarGroupReader<R: Read> {
    car: CarBlockReader<R>,
}
impl<R: Read> CarGroupReader<R> {
    fn new(reader: R, car_buf_size: usize) -> Result<Self> {
        let mut car = CarBlockReader::with_capacity(reader, car_buf_size);
        car.skip_header()?;
        Ok(Self { car })
    }

    #[inline(always)]
    fn next_group_into(&mut self, group: &mut CarBlockGroup) -> Result<bool> {
        self.car.read_until_block_into(group)
    }
}

fn run_stream_single_thread<R: Read>(mut car: CarGroupReader<R>, args: &Args) -> Result<()> {
    let stats_every = Duration::from_secs(args.stats_every.max(1));
    let start = Instant::now();
    let end = if args.seconds == 0 {
        None
    } else {
        Some(start + Duration::from_secs(args.seconds))
    };

    let mut stats = Stats::default();
    let mut last_print = Instant::now();

    let mut group = CarBlockGroup::new();

    loop {
        let ok = car.next_group_into(&mut group)?;
        if !ok {
            break;
        }

        stats.blocks += 1;
        let (entries_count, bytes_size) = group.get_len();
        stats.entries += entries_count as u64;
        stats.bytes += bytes_size as u64;

        if args.decode_tx {
            let mut it = group.transactions();

            while let Some((_tx, maybe_meta)) = it
                .next_tx()
                .map_err(|e| CarError::TxDecode(format!("transaction decode failed: {e:?}")))?
            {
                stats.txs += 1;
                if maybe_meta.is_some() {
                    stats.txs_with_meta += 1;
                }
            }
        }

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

fn run_stream_parallel<R: Read + Send + 'static>(
    mut car: CarGroupReader<R>,
    args: &Args,
) -> Result<()> {
    let workers = args.jobs.max(1);

    let channel_bound = workers * 8;
    let pool_size = channel_bound + workers + 2;

    let pool = Arc::new(CarBlockGroupPoolSafe::with_capacity(pool_size));
    let (tx, rx) = chan::bounded::<PooledGroupSafe>(channel_bound);

    let (rtx, rrx) = chan::unbounded::<WorkerReport>();

    let decode_tx = args.decode_tx;
    let logger = thread::spawn(move || {
        while let Ok(rep) = rrx.recv() {
            let dt = rep.dt.max(1e-9);

            let mib_s = (rep.stats.bytes as f64 / (1024.0 * 1024.0)) / dt;
            let blocks_s = (rep.stats.blocks as f64) / dt;
            let entries_s = (rep.stats.entries as f64) / dt;

            if decode_tx {
                let tps = (rep.stats.txs as f64) / dt;
                let meta_pct = if rep.stats.txs > 0 {
                    (rep.stats.txs_with_meta as f64 / rep.stats.txs as f64) * 100.0
                } else {
                    0.0
                };
                info!(
                    "[w{:02}] {:.1} MiB/s | {:.0} blocks/s | {:.0} tx/s ({:.1}% meta) | {:.0} entries/s",
                    rep.worker_id, mib_s, blocks_s, tps, meta_pct, entries_s
                );
            } else {
                info!(
                    "[w{:02}] {:.1} MiB/s | {:.0} blocks/s | {:.0} entries/s",
                    rep.worker_id, mib_s, blocks_s, entries_s
                );
            }
        }
    });

    let stats_every = Duration::from_secs(args.stats_every.max(1));

    let mut handles = Vec::with_capacity(workers);
    for worker_id in 0..workers {
        let rx = rx.clone();
        let rtx = rtx.clone();
        let decode_tx = args.decode_tx;
        let stats_every = stats_every;

        handles.push(thread::spawn(move || -> Result<Stats> {
            let mut total = Stats::default();
            let mut window = Stats::default();
            let mut last_report = Instant::now();

            for mut pg in rx.iter() {
                window.blocks += 1;

                let (entries_count, bytes_size) = pg.as_mut().get_len();
                window.entries += entries_count as u64;
                window.bytes += bytes_size as u64;

                if decode_tx {
                    let mut it = pg.as_mut().transactions();

                    while let Some((_tx, maybe_meta)) = it.next_tx().map_err(|e| {
                        CarError::TxDecode(format!("transaction decode failed: {e:?}"))
                    })? {
                        window.txs += 1;
                        if maybe_meta.is_some() {
                            window.txs_with_meta += 1;
                        }
                    }
                }

                let now = Instant::now();
                if now.duration_since(last_report) >= stats_every {
                    let dt = now.duration_since(last_report).as_secs_f64().max(1e-9);

                    let report_stats = window.clone();
                    total.merge_from(&report_stats);
                    window.reset();
                    last_report = now;

                    let _ = rtx.try_send(WorkerReport {
                        worker_id,
                        dt,
                        stats: report_stats,
                    });
                }
            }

            let now = Instant::now();
            let dt = now.duration_since(last_report).as_secs_f64();
            if dt > 0.0 && (window.blocks > 0 || window.entries > 0) {
                let report_stats = window.clone();
                total.merge_from(&report_stats);

                let _ = rtx.try_send(WorkerReport {
                    worker_id,
                    dt: dt.max(1e-9),
                    stats: report_stats,
                });
            }

            Ok(total)
        }));
    }
    drop(rx);
    drop(rtx);

    let start = Instant::now();
    let end = if args.seconds == 0 {
        None
    } else {
        Some(start + Duration::from_secs(args.seconds))
    };

    loop {
        let now = Instant::now();
        if end.map_or(false, |dl| now >= dl) {
            break;
        }

        let mut pg = pool.checkout();
        let ok = car.next_group_into(pg.as_mut())?;
        if !ok {
            break;
        }

        if tx.send(pg).is_err() {
            break;
        }
    }
    drop(tx);

    let mut total = Stats::default();
    for h in handles {
        let s = h.join().unwrap()?;
        total.merge_from(&s);
    }

    let _ = logger.join();

    total.print_final(start.elapsed().as_secs_f64(), args.decode_tx);
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let args = Args::parse();

    match args.input.as_deref() {
        None | Some("-") => {
            info!(
                "Reading CAR archive: stdin (decode_tx={}, jobs={})",
                args.decode_tx, args.jobs
            );
            let stdin = io::stdin();
            let reader = BufReader::with_capacity(args.buf_size, stdin.lock());

            if args.jobs > 1 {
                return Err(CarError::TxDecode(
                    "parallel mode (-j > 1) is not supported for stdin (use a file path or URL)"
                        .to_string(),
                ));
            }

            let car = CarGroupReader::new(reader, args.buf_size)?;
            run_stream_single_thread(car, &args)
        }

        Some(input) => {
            info!(
                "Reading CAR archive: {} (decode_tx={}, jobs={})",
                input, args.decode_tx, args.jobs
            );

            if looks_like_url(input) {
                info!("Using network mode");
                if has_zst_suffix(input) {
                    return Err(CarError::TxDecode(
                        "input looks like a URL ending with .zst, but URL zstd is not supported (download locally or add url+zstd support)".to_string(),
                    ));
                }

                let client = reqwest::blocking::Client::builder()
                    .timeout(None)
                    .no_gzip()
                    .no_brotli()
                    .no_deflate()
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
                let car = CarGroupReader::new(reader, args.buf_size)?;

                if args.jobs <= 1 {
                    run_stream_single_thread(car, &args)
                } else {
                    run_stream_parallel(car, &args)
                }
            } else {
                let path = Path::new(input);
                let is_zst = path
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("zst"))
                    .unwrap_or(false);

                if is_zst {
                    info!("Using zstd mode");
                    let file = File::open(path)
                        .map_err(|e| CarError::Io(format!("open {}: {e}", path.display())))?;
                    let file = BufReader::with_capacity(args.buf_size, file);
                    let zstd = zstd::Decoder::with_buffer(file).map_err(|e| {
                        CarError::TxDecode(format!("zstd decoder init failed: {e}"))
                    })?;

                    let car = CarGroupReader::new(zstd, args.buf_size)?;

                    if args.jobs <= 1 {
                        run_stream_single_thread(car, &args)
                    } else {
                        run_stream_parallel(car, &args)
                    }
                } else {
                    info!("Using file mode");
                    let file = File::open(path)
                        .map_err(|e| CarError::Io(format!("open {}: {e}", path.display())))?;
                    let reader = BufReader::with_capacity(args.buf_size, file);

                    let car = CarGroupReader::new(reader, args.buf_size)?;

                    if args.jobs <= 1 {
                        run_stream_single_thread(car, &args)
                    } else {
                        run_stream_parallel(car, &args)
                    }
                }
            }
        }
    }
}
