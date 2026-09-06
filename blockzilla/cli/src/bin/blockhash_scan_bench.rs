use anyhow::{Context, Result, anyhow};
use clap::{Parser, ValueEnum};
use of_car_reader::{
    CarBlockReader,
    node::{decode_entry_hash, is_block_node, is_entry_node},
};
use std::{
    fs::File,
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Copy,
    Payload,
    Prefix,
    Blockhash,
    PrefixSkip,
    BlockhashPrefixSkip,
}

#[derive(Parser, Debug)]
struct Args {
    input: PathBuf,

    #[arg(long, value_enum, default_value_t = Mode::Blockhash)]
    mode: Mode,

    #[arg(long, default_value_t = 1)]
    iters: usize,

    #[arg(long, default_value_t = 256)]
    buf_mib: usize,

    #[arg(long, default_value_t = 96)]
    prefix_len: usize,

    #[arg(long)]
    compressed: Option<bool>,
}

#[derive(Default)]
struct Stats {
    input_bytes: u64,
    copied_bytes: u64,
    car_bytes: u64,
    payload_bytes: u64,
    nodes: u64,
    entry_nodes: u64,
    block_nodes: u64,
    skipped_nodes: u64,
    latest_hashes: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    anyhow::ensure!(args.iters > 0, "--iters must be > 0");
    let compressed = args.compressed.unwrap_or_else(|| is_zstd_path(&args.input));
    let buf_bytes = args
        .buf_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| anyhow!("buffer size overflow"))?;
    let input_bytes = std::fs::metadata(&args.input)
        .with_context(|| format!("stat {}", args.input.display()))?
        .len();

    let started = Instant::now();
    let mut total = Stats {
        input_bytes: input_bytes * args.iters as u64,
        ..Stats::default()
    };
    for _ in 0..args.iters {
        let stats = match args.mode {
            Mode::Copy => bench_copy(&args.input, compressed, buf_bytes)?,
            Mode::Payload => bench_car(&args.input, compressed, buf_bytes, false, false)?,
            Mode::Prefix => bench_car(&args.input, compressed, buf_bytes, true, false)?,
            Mode::Blockhash => bench_car(&args.input, compressed, buf_bytes, true, true)?,
            Mode::PrefixSkip => {
                bench_car_prefix_skip(&args.input, compressed, buf_bytes, args.prefix_len, false)?
            }
            Mode::BlockhashPrefixSkip => {
                bench_car_prefix_skip(&args.input, compressed, buf_bytes, args.prefix_len, true)?
            }
        };
        total.copied_bytes += stats.copied_bytes;
        total.car_bytes += stats.car_bytes;
        total.payload_bytes += stats.payload_bytes;
        total.nodes += stats.nodes;
        total.entry_nodes += stats.entry_nodes;
        total.block_nodes += stats.block_nodes;
        total.skipped_nodes += stats.skipped_nodes;
        total.latest_hashes += stats.latest_hashes;
    }

    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "mode={:?} compressed={} iters={} elapsed_s={:.6} input_mib_s={:.2} copy_mib_s={:.2} car_mib_s={:.2} payload_mib_s={:.2} nodes_s={:.0} entries={} blocks={} skipped={} latest_hashes={}",
        args.mode,
        compressed,
        args.iters,
        elapsed,
        mib_s(total.input_bytes, elapsed),
        mib_s(total.copied_bytes, elapsed),
        mib_s(total.car_bytes, elapsed),
        mib_s(total.payload_bytes, elapsed),
        total.nodes as f64 / elapsed,
        total.entry_nodes,
        total.block_nodes,
        total.skipped_nodes,
        total.latest_hashes,
    );

    Ok(())
}

fn bench_copy(path: &Path, compressed: bool, buf_bytes: usize) -> Result<Stats> {
    let mut reader = open_input(path, compressed, buf_bytes)?;
    let copied_bytes = io::copy(&mut reader, &mut io::sink())
        .with_context(|| format!("copy {}", path.display()))?;
    Ok(Stats {
        copied_bytes,
        ..Stats::default()
    })
}

fn bench_car(
    path: &Path,
    compressed: bool,
    buf_bytes: usize,
    classify: bool,
    decode_blockhash: bool,
) -> Result<Stats> {
    let reader = open_input(path, compressed, buf_bytes)?;
    let mut car = CarBlockReader::with_capacity(reader, buf_bytes);
    car.skip_header().map_err(|err| anyhow!("{err}"))?;

    let mut scratch = Vec::new();
    let mut stats = Stats::default();
    let mut latest_hash: Option<[u8; 32]> = None;
    while let Some(entry) = car
        .read_entry_payload_with_scratch(&mut scratch)
        .map_err(|err| anyhow!("{err}"))?
    {
        stats.nodes += 1;
        stats.car_bytes += entry.total_len as u64;
        stats.payload_bytes += entry.payload_len as u64;

        if !classify {
            continue;
        }

        if is_entry_node(entry.payload) {
            stats.entry_nodes += 1;
            if decode_blockhash {
                let hash = decode_entry_hash(entry.payload)
                    .map_err(|err| anyhow!("{err}"))
                    .with_context(|| format!("entry {} decode hash", entry.cid))?;
                latest_hash = Some(hash_32(hash)?);
                stats.latest_hashes += 1;
            }
        } else if is_block_node(entry.payload) {
            stats.block_nodes += 1;
            if decode_blockhash {
                let _slot = decode_block_slot(entry.payload)?;
                let _blockhash = latest_hash
                    .take()
                    .context("block node has no preceding Entry hash")?;
            }
        } else {
            stats.skipped_nodes += 1;
        }
    }
    Ok(stats)
}

fn bench_car_prefix_skip(
    path: &Path,
    compressed: bool,
    buf_bytes: usize,
    prefix_len: usize,
    decode_blockhash: bool,
) -> Result<Stats> {
    let reader = open_input(path, compressed, buf_bytes)?;
    let mut car = CarBlockReader::with_capacity(reader, buf_bytes);
    car.skip_header().map_err(|err| anyhow!("{err}"))?;

    let mut scratch = Vec::new();
    let mut stats = Stats::default();
    let mut latest_hash: Option<[u8; 32]> = None;
    while let Some(entry) = car
        .read_entry_payload_if_prefix_with_scratch(&mut scratch, prefix_len, |_| false)
        .map_err(|err| anyhow!("{err}"))?
    {
        stats.nodes += 1;
        stats.car_bytes += entry.total_len as u64;
        stats.payload_bytes += entry.prefix.len() as u64;

        if is_entry_node(entry.prefix) {
            stats.entry_nodes += 1;
            if decode_blockhash {
                let hash = decode_entry_hash(entry.prefix)
                    .map_err(|err| anyhow!("{err}"))
                    .with_context(|| format!("entry {} decode hash from prefix", entry.cid))?;
                latest_hash = Some(hash_32(hash)?);
                stats.latest_hashes += 1;
            }
        } else if is_block_node(entry.prefix) {
            stats.block_nodes += 1;
            if decode_blockhash {
                let _slot = decode_block_slot(entry.prefix)?;
                let _blockhash = latest_hash
                    .take()
                    .context("block node has no preceding Entry hash")?;
            }
        } else {
            stats.skipped_nodes += 1;
        }
    }
    Ok(stats)
}

fn open_input(path: &Path, compressed: bool, buf_bytes: usize) -> Result<Box<dyn Read>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::with_capacity(buf_bytes, file);
    if compressed {
        Ok(Box::new(
            zstd::stream::read::Decoder::new(reader)
                .with_context(|| format!("zstd decode {}", path.display()))?,
        ))
    } else {
        Ok(Box::new(reader))
    }
}

fn decode_block_slot(payload: &[u8]) -> Result<u64> {
    let mut decoder = minicbor::Decoder::new(payload);
    let array_len = decoder
        .array()?
        .ok_or_else(|| anyhow!("indefinite block arrays are not supported"))?;
    anyhow::ensure!(array_len >= 2, "block array too short: {array_len}");
    let kind = decoder.u64()?;
    anyhow::ensure!(kind == 2, "expected block kind 2, got {kind}");
    decoder.u64().context("decode block slot")
}

fn hash_32(bytes: &[u8]) -> Result<[u8; 32]> {
    let mut out = [0u8; 32];
    anyhow::ensure!(
        bytes.len() == out.len(),
        "expected 32 byte hash, got {}",
        bytes.len()
    );
    out.copy_from_slice(bytes);
    Ok(out)
}

fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
}

fn mib_s(bytes: u64, elapsed_s: f64) -> f64 {
    if bytes == 0 || elapsed_s == 0.0 {
        0.0
    } else {
        bytes as f64 / 1024.0 / 1024.0 / elapsed_s
    }
}
