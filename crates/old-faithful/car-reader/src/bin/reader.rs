use clap::Parser;
use tracing::{Level, info};

use of_car_reader::{
    CarBlockReader,
    car_block_group::CarBlockGroup,
    error::{CarReadError as CarError, CarReadResult as Result},
    genesis::{
        GenesisAccountEntry, MAINNET_GENESIS_URL, bytes_to_hex, pubkey_to_base58,
        read_genesis_archive_from_file,
    },
};
use sha2::{Digest, Sha256};

use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
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

    /// Ensure, read, and print the Solana genesis.tar.bz2 stored in the archive cache.
    #[arg(long)]
    genesis: bool,

    /// Directory that stores epoch CARs and genesis.tar.bz2.
    #[arg(long, default_value = "epochs")]
    archive_dir: PathBuf,

    /// URL used when --genesis is requested and genesis.tar.bz2 is missing.
    #[arg(long, default_value = MAINNET_GENESIS_URL)]
    genesis_url: String,

    /// Number of genesis accounts to print (0 = all).
    #[arg(long, default_value_t = 0)]
    genesis_account_limit: usize,
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
}

fn ensure_genesis_file(archive_dir: &Path, genesis_url: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(archive_dir)
        .map_err(|e| CarError::Io(format!("create {}: {e}", archive_dir.display())))?;

    let path = archive_dir.join("genesis.tar.bz2");
    if path
        .metadata()
        .map(|m| m.is_file() && m.len() > 0)
        .unwrap_or(false)
    {
        return Ok(path);
    }

    let tmp = archive_dir.join("genesis.tar.bz2.tmp");
    info!(
        "genesis.tar.bz2 missing from {}; downloading {}",
        archive_dir.display(),
        genesis_url
    );
    let mut resp = reqwest::blocking::get(genesis_url)
        .map_err(|e| CarError::Io(format!("download genesis from {genesis_url}: {e}")))?;
    if !resp.status().is_success() {
        return Err(CarError::Io(format!(
            "download genesis from {genesis_url}: HTTP {}",
            resp.status()
        )));
    }

    let mut file =
        File::create(&tmp).map_err(|e| CarError::Io(format!("create {}: {e}", tmp.display())))?;
    io::copy(&mut resp, &mut file)
        .map_err(|e| CarError::Io(format!("write {}: {e}", tmp.display())))?;
    file.flush()
        .map_err(|e| CarError::Io(format!("flush {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, &path).map_err(|e| {
        CarError::Io(format!(
            "rename {} to {}: {e}",
            tmp.display(),
            path.display()
        ))
    })?;

    Ok(path)
}

fn print_genesis(args: &Args) -> Result<()> {
    let path = ensure_genesis_file(&args.archive_dir, &args.genesis_url)?;
    let archive = read_genesis_archive_from_file(&path)?;
    let genesis = &archive.genesis;
    let accounts = genesis.account_stats();
    let reward_pools = genesis.reward_pool_stats();

    println!("genesis archive: {}", path.display());
    println!("  genesis.bin bytes: {}", archive.genesis_bin_len);
    println!(
        "  genesis hash sha256: {}",
        bytes_to_hex(&archive.genesis_hash)
    );
    println!(
        "  genesis hash base58: {}",
        pubkey_to_base58(&archive.genesis_hash)
    );
    println!("  tar entries:");
    for entry in &archive.archive_entries {
        println!(
            "    - {} (declared={} read={})",
            entry.path, entry.size, entry.bytes_read
        );
    }
    println!("  creation_time_unix: {}", genesis.creation_time_unix);
    println!("  cluster_id: {}", genesis.cluster_id);
    println!("  ticks_per_slot: {}", genesis.ticks_per_slot);
    println!(
        "  poh: tick_duration={}s+{}ns tick_count={:?} hashes_per_tick={:?}",
        genesis.poh_params.tick_duration_secs,
        genesis.poh_params.tick_duration_nanos,
        genesis.poh_params.tick_count,
        genesis.poh_params.hashes_per_tick
    );
    println!(
        "  fees: target_lamports_per_sig={} target_sigs_per_slot={} min={} max={} burn_percent={}",
        genesis.fees.target_lamports_per_sig,
        genesis.fees.target_sigs_per_slot,
        genesis.fees.min_lamports_per_sig,
        genesis.fees.max_lamports_per_sig,
        genesis.fees.burn_percent
    );
    println!(
        "  rent: lamports_per_byte_year={} exemption_threshold={} burn_percent={}",
        genesis.rent.lamports_per_byte_year,
        genesis.rent.exemption_threshold,
        genesis.rent.burn_percent
    );
    println!(
        "  inflation: initial={} terminal={} taper={} foundation={} foundation_term={} padding={}",
        genesis.inflation.initial,
        genesis.inflation.terminal,
        genesis.inflation.taper,
        genesis.inflation.foundation,
        genesis.inflation.foundation_term,
        bytes_to_hex(&genesis.inflation.padding)
    );
    println!(
        "  epoch_schedule: slots_per_epoch={} leader_schedule_slot_offset={} warmup={} first_normal_epoch={} first_normal_slot={}",
        genesis.epoch_schedule.slots_per_epoch,
        genesis.epoch_schedule.leader_schedule_slot_offset,
        genesis.epoch_schedule.warmup,
        genesis.epoch_schedule.first_normal_epoch,
        genesis.epoch_schedule.first_normal_slot
    );
    println!("  builtins: {}", genesis.builtins.len());
    for (idx, builtin) in genesis.builtins.iter().enumerate() {
        println!(
            "    [{idx}] {} {}",
            builtin.key,
            pubkey_to_base58(&builtin.pubkey)
        );
    }
    println!(
        "  accounts: count={} total_lamports={} ({}) total_data_bytes={} executable={}",
        accounts.count,
        accounts.total_lamports,
        format_lamports(accounts.total_lamports),
        accounts.total_data_bytes,
        accounts.executable_accounts
    );
    print_accounts("accounts", &genesis.accounts, args.genesis_account_limit);
    println!(
        "  reward_pools: count={} total_lamports={} ({}) total_data_bytes={} executable={}",
        reward_pools.count,
        reward_pools.total_lamports,
        format_lamports(reward_pools.total_lamports),
        reward_pools.total_data_bytes,
        reward_pools.executable_accounts
    );
    print_accounts(
        "reward_pools",
        &genesis.reward_pools,
        args.genesis_account_limit,
    );

    Ok(())
}

fn print_accounts(label: &str, entries: &[GenesisAccountEntry], limit: usize) {
    let shown = if limit == 0 {
        entries.len()
    } else {
        entries.len().min(limit)
    };

    if shown == 0 {
        return;
    }

    println!("  {label}_entries:");
    for (idx, entry) in entries.iter().take(shown).enumerate() {
        let account = &entry.account;
        let data_sha256 = if account.data.is_empty() {
            "empty".to_string()
        } else {
            bytes_to_hex(&Sha256::digest(&account.data))
        };
        let data_preview = bytes_to_hex(&account.data[..account.data.len().min(32)]);
        println!(
            "    [{idx}] pubkey={} lamports={} owner={} executable={} rent_epoch={} data_len={} data_sha256={} data_preview={}",
            pubkey_to_base58(&entry.pubkey),
            account.lamports,
            pubkey_to_base58(&account.owner),
            account.executable,
            account.rent_epoch,
            account.data.len(),
            data_sha256,
            data_preview
        );
    }

    if shown < entries.len() {
        println!("    ... {} more", entries.len() - shown);
    }
}

fn format_lamports(lamports: u128) -> String {
    let whole = lamports / 1_000_000_000;
    let frac = lamports % 1_000_000_000;
    format!("{whole}.{frac:09} SOL")
}

// -------- pools (duplicated types, minimal) --------
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
impl AsMut<CarBlockGroup> for PooledGroupSafe {
    #[inline]
    fn as_mut(&mut self) -> &mut CarBlockGroup {
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
        let (tx_count, bytes_size) = group.get_len();
        stats.entries += group.entry_count() as u64;
        stats.bytes += bytes_size as u64;

        println!(
            "block {}: {} txs, {} entries, {} poh_hashes, {} rewards, {} shredding spans",
            stats.blocks,
            tx_count,
            group.entry_count(),
            group.poh_hashes.len(),
            group.rewards.is_some(),
            group.shredding.len()
        );

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

        if end.is_some_and(|dl| now >= dl) {
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

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let args = Args::parse();

    if args.genesis {
        print_genesis(&args)?;
        if args.input.is_none() {
            return Ok(());
        }
    }

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
                let zstd = zstd::Decoder::with_buffer(file)
                    .map_err(|e| CarError::TxDecode(format!("zstd decoder init failed: {e}")))?;

                let car = CarGroupReader::new(zstd, args.buf_size)?;
                run_stream_single_thread(car, &args)
            } else {
                info!("Using file mode");
                let file = File::open(path)
                    .map_err(|e| CarError::Io(format!("open {}: {e}", path.display())))?;
                let reader = BufReader::with_capacity(args.buf_size, file);

                let car = CarGroupReader::new(reader, args.buf_size)?;

                run_stream_single_thread(car, &args)
            }
        }
    }
}
