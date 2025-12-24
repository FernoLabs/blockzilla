use clap::{Parser, Subcommand};
use tracing::{info, Level};

use car_reader::{
    car_block_group::CarBlockGroup,
    error::{CarReadError as CarError, CarReadResult as Result},
    CarBlockReader,
};

use pprof::ProfilerGuard;
use prost::Message;

use std::io::{BufReader, Write};
use std::time::{Duration, Instant};
use std::{fs::File, io::Read};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "blockzilla")]
#[command(about = "Blockzilla archive node reader and analyzer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Print stats every N seconds
    #[arg(long, default_value_t = 2)]
    stats_every: u64,
}

#[derive(Subcommand)]
enum Commands {
    /// Analyze CAR archive (.car.zst)
    AnalyzeCar {
        /// Input CAR file path
        #[arg(short, long)]
        input: String,
    },

    /// Profile CAR reader for N seconds and output a flamegraph (and optional pprof protobuf)
    Profile {
        /// Input CAR file path
        #[arg(short, long)]
        input: String,

        /// Profiling duration in seconds
        #[arg(long, default_value_t = 60)]
        seconds: u64,

        /// Output flamegraph SVG path
        #[arg(long, default_value = "flamegraph.svg")]
        out: String,

        /// Optional output pprof protobuf path (profile.pb)
        #[arg(long)]
        pb: Option<String>,
    },

    /// Analyze compact archive
    AnalyzeCompact {
        /// Input epoch directory
        #[arg(short, long)]
        input: String,

        /// Epoch number
        #[arg(short, long)]
        epoch: u64,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();
    let cli = Cli::parse();

    match cli.command {
        Commands::AnalyzeCar { input } => analyze_car(&input, cli.stats_every),
        Commands::Profile {
            input,
            seconds,
            out,
            pb,
        } => profile_car(&input, cli.stats_every, seconds, &out, pb.as_deref()),
        Commands::AnalyzeCompact { input, epoch } => {
            info!("Analyzing compact archive for epoch {}: {}", epoch, input);
            Err(CarError::InvalidData(
                "analyze-compact not implemented".to_string(),
            ))
        }
    }
}

fn analyze_car(path: &str, stats_every_secs: u64) -> Result<()> {
    info!("Analyzing CAR archive: {}", path);

    let mut car = open_car_reader(path)?;
    car.skip_header()?;

    // Reused group buffers (avoids realloc each iteration)
    let mut group = CarBlockGroup::new();

    let stats_every = Duration::from_secs(stats_every_secs.max(1));
    let start = Instant::now();
    let mut last = start;

    // totals
    let mut blocks: u64 = 0;
    let mut entries: u64 = 0;
    let mut bytes: u64 = 0;

    // interval counters
    let mut blocks_i: u64 = 0;
    let mut entries_i: u64 = 0;
    let mut bytes_i: u64 = 0;

    while car.read_until_block_into(&mut group)? {
        blocks += 1;
        blocks_i += 1;

        let n_entries = group.payloads.len() as u64;
        entries += n_entries;
        entries_i += n_entries;

        let cid_len = group
            .cid_map
            .keys()
            .map(|cid| cid.len() as u64)
            .next()
            .unwrap_or(0);

        let payload_bytes: u64 = group.payloads.iter().map(|p| p.len() as u64).sum();
        let group_bytes = payload_bytes + cid_len * n_entries;

        bytes += group_bytes;
        bytes_i += group_bytes;

        let now = Instant::now();
        if now.duration_since(last) >= stats_every {
            log_stats(
                now.duration_since(last),
                blocks_i,
                entries_i,
                bytes_i,
                blocks,
                entries,
                bytes,
            );

            last = now;
            blocks_i = 0;
            entries_i = 0;
            bytes_i = 0;
        }
    }

    let total_dt = start.elapsed();
    log_done(total_dt, blocks, entries, bytes);

    Ok(())
}

fn profile_car(
    path: &str,
    stats_every_secs: u64,
    seconds: u64,
    out_svg: &str,
    out_pb: Option<&str>,
) -> Result<()> {
    let seconds = seconds.max(1);
    info!(
        "Profiling CAR reader: {} ({}s) -> {}{}",
        path,
        seconds,
        out_svg,
        out_pb.map(|p| format!(", {}", p)).unwrap_or_default()
    );

    // Start profiler (100Hz is a common default)
    let guard = ProfilerGuard::new(100)
        .map_err(|e| CarError::InvalidData(format!("pprof init failed: {e}")))?;

    let mut car = open_car_reader(path)?;
    car.skip_header()?;

    let mut group = CarBlockGroup::new();

    let stats_every = Duration::from_secs(stats_every_secs.max(1));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(seconds);
    let mut last = start;

    // totals
    let mut blocks: u64 = 0;
    let mut entries: u64 = 0;
    let mut bytes: u64 = 0;

    // interval counters
    let mut blocks_i: u64 = 0;
    let mut entries_i: u64 = 0;
    let mut bytes_i: u64 = 0;

    while car.read_until_block_into(&mut group)? {
        let _tx_count = group.transactions().unwrap().count();
        blocks += 1;
        blocks_i += 1;

        let n_entries = group.payloads.len() as u64;
        entries += n_entries;
        entries_i += n_entries;

        let cid_len = group
            .cid_map
            .keys()
            .map(|cid| cid.len() as u64)
            .next()
            .unwrap_or(0);

        let payload_bytes: u64 = group.payloads.iter().map(|p| p.len() as u64).sum();
        let group_bytes = payload_bytes + cid_len * n_entries;

        bytes += group_bytes;
        bytes_i += group_bytes;

        let now = Instant::now();
        if now.duration_since(last) >= stats_every {
            log_stats(
                now.duration_since(last),
                blocks_i,
                entries_i,
                bytes_i,
                blocks,
                entries,
                bytes,
            );

            last = now;
            blocks_i = 0;
            entries_i = 0;
            bytes_i = 0;
        }

        if now >= deadline {
            break;
        }
    }

    let total_dt = start.elapsed();
    log_done(total_dt, blocks, entries, bytes);

    // Build report and write outputs
    let report = guard
        .report()
        .build()
        .map_err(|e| CarError::InvalidData(format!("pprof report build failed: {e}")))?;

    // flamegraph.svg
    let svg = File::create(out_svg).map_err(|e| CarError::Io(format!("create {out_svg}: {e}")))?;
    report
        .flamegraph(svg)
        .map_err(|e| CarError::InvalidData(format!("write flamegraph: {e}")))?;
    info!("wrote {}", out_svg);

    // Optional profile.pb
    if let Some(pb_path) = out_pb {
        let profile = report
            .pprof()
            .map_err(|e| CarError::InvalidData(format!("build pprof: {e}")))?;
        let mut content = Vec::new();
        profile
            .encode(&mut content)
            .map_err(|e| CarError::InvalidData(format!("encode pprof: {e}")))?;

        let mut f =
            File::create(pb_path).map_err(|e| CarError::Io(format!("create {pb_path}: {e}")))?;
        f.write_all(&content)
            .map_err(|e| CarError::Io(format!("write {pb_path}: {e}")))?;
        info!("wrote {}", pb_path);
    }

    Ok(())
}

fn open_car_reader(path: &str) -> Result<CarBlockReader<impl Read>> {
    let file = File::open(path).map_err(|e| CarError::Io(e.to_string()))?;
    let file = BufReader::with_capacity(64 << 20, file);
    let zstd = zstd::Decoder::with_buffer(file)
        .map_err(|e| CarError::InvalidData(format!("zstd decoder init failed: {e}")))?;

    Ok(CarBlockReader::with_capacity(zstd, 64 << 20))
}

fn log_stats(
    dt: Duration,
    blocks_i: u64,
    entries_i: u64,
    bytes_i: u64,
    blocks: u64,
    entries: u64,
    bytes: u64,
) {
    let dt = dt.as_secs_f64().max(1e-9);

    let mib_s = (bytes_i as f64 / (1024.0 * 1024.0)) / dt;
    let blocks_s = (blocks_i as f64) / dt;
    let entries_s = (entries_i as f64) / dt;

    info!(
        "read: {:.1} MiB/s | {:.0} blocks/s | {:.0} entries/s totals: blocks={}, entries={}, bytes={:.1} GiB",
        mib_s,
        blocks_s,
        entries_s,
        blocks,
        entries,
        (bytes as f64) / (1024.0 * 1024.0 * 1024.0),
    );
}

fn log_done(total_dt: Duration, blocks: u64, entries: u64, bytes: u64) {
    let total_s = total_dt.as_secs_f64().max(1e-9);
    let mib_s = (bytes as f64 / (1024.0 * 1024.0)) / total_s;
    let blocks_s = (blocks as f64) / total_s;
    let entries_s = (entries as f64) / total_s;

    info!(
        "done: {:.1} MiB/s | {:.0} blocks/s | {:.0} entries/s blocks={}, entries={}, bytes={:.2} GiB, time={:.1}s",
        mib_s,
        blocks_s,
        entries_s,
        blocks,
        entries,
        (bytes as f64) / (1024.0 * 1024.0 * 1024.0),
        total_s,
    );
}
