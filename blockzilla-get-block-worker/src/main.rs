use anyhow::{Context, Result, anyhow, bail};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN, ArchiveV2BlockAccessBlob, ArchiveV2BlockAccessBlockhash,
    ArchiveV2BlockAccessPubkey, ArchiveV2GetBlockIndexRow, ArchiveV2HotRewards, ArchiveV2HotTxRow,
    CompactPubkey, WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION, WincodeLeb128Config,
    deserialize_archive_v2_hot_block_blob, wincode_leb128_config,
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Instant,
};
use wincode::{SchemaRead, io::Reader};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

const SLOTS_PER_EPOCH: u64 = 432_000;
const SIGNATURE_BYTES: usize = 64;

#[derive(Parser)]
#[command(name = "blockzilla-get-block-worker")]
#[command(about = "Blockzilla getBlock Worker companion CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the live Worker URL.
    Info,

    /// Benchmark direct getBlock archive reads from local files.
    LocalBench {
        /// Archive root containing epoch-N directories.
        #[arg(long, default_value = "blockzilla-v2")]
        archive_root: PathBuf,
        /// Epochs to sample, comma/space separated. Ranges like 200-205 are accepted.
        #[arg(long, default_value = "200")]
        epochs: String,
        /// Explicit slots to include, comma/space separated. Ranges are accepted.
        #[arg(long, default_value = "")]
        slots: String,
        /// Random present slots per epoch, in addition to first/last present slots.
        #[arg(long, default_value_t = 1000)]
        samples_per_epoch: usize,
        /// Deterministic sample seed.
        #[arg(long, default_value_t = 4242)]
        seed: u64,
        /// Measured iterations over the selected slots.
        #[arg(long, default_value_t = 1)]
        iterations: usize,
        /// Unmeasured warmup iterations over the selected slots.
        #[arg(long, default_value_t = 0)]
        warmup: usize,
        /// Worker threads.
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        /// Skip archive-v2-block-access.wincode reads/decode.
        #[arg(long)]
        no_access: bool,
        /// Reuse per-thread compressed/decompressed/access buffers between jobs.
        #[arg(long)]
        reuse_buffers: bool,
        /// Decode only the block fields needed for light getBlock modes.
        ///
        /// This skips block rewards plus message/metadata byte vectors after
        /// counting tx rows and signatures.
        #[arg(long)]
        lite_block_decode: bool,
        /// Locally render a minimal getBlock JSON shape: off, none, signatures, or rewards.
        #[arg(long, default_value = "off")]
        render_json: String,
        /// Local JSON output strategy for --render-json: buffered or chunked.
        ///
        /// buffered matches the current Worker behavior by accumulating the full
        /// response in one Vec. chunked simulates a streaming response writer by
        /// flushing fixed-size chunks to a sink and never keeping the full JSON
        /// body in memory.
        #[arg(long, default_value = "buffered")]
        render_json_output: String,
        /// Sample process RSS around each job stage. On non-Linux targets this records zero.
        #[arg(long)]
        measure_rss: bool,
        /// Write an in-process CPU flamegraph SVG for the measured jobs.
        #[arg(long)]
        flamegraph_out: Option<PathBuf>,
        /// Sampling frequency used with --flamegraph-out.
        #[arg(long, default_value_t = 997)]
        profile_frequency: i32,
        /// Optional directory for plan.tsv, requests.tsv, and summary.json.
        #[arg(long)]
        out_dir: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    match Cli::parse().command {
        Command::Info => {
            let url = std::env::var("BLOCKZILLA_WORKER_URL")
                .context("BLOCKZILLA_WORKER_URL must be set")?;
            println!("{url}");
        }
        Command::LocalBench {
            archive_root,
            epochs,
            slots,
            samples_per_epoch,
            seed,
            iterations,
            warmup,
            concurrency,
            no_access,
            reuse_buffers,
            lite_block_decode,
            render_json,
            render_json_output,
            measure_rss,
            flamegraph_out,
            profile_frequency,
            out_dir,
        } => {
            run_local_bench(LocalBenchConfig {
                archive_root,
                epochs: parse_u64_specs(&epochs)?,
                slots: parse_u64_specs(&slots)?,
                samples_per_epoch,
                seed,
                iterations,
                warmup,
                concurrency: concurrency.max(1),
                include_access: !no_access,
                reuse_buffers,
                lite_block_decode,
                render_json: RenderJsonMode::parse(&render_json)?,
                render_json_output: RenderJsonOutput::parse(&render_json_output)?,
                measure_rss,
                flamegraph_out,
                profile_frequency,
                out_dir,
            })?;
        }
    }
    Ok(())
}

struct LocalBenchConfig {
    archive_root: PathBuf,
    epochs: Vec<u64>,
    slots: Vec<u64>,
    samples_per_epoch: usize,
    seed: u64,
    iterations: usize,
    warmup: usize,
    concurrency: usize,
    include_access: bool,
    reuse_buffers: bool,
    lite_block_decode: bool,
    render_json: RenderJsonMode,
    render_json_output: RenderJsonOutput,
    measure_rss: bool,
    flamegraph_out: Option<PathBuf>,
    profile_frequency: i32,
    out_dir: Option<PathBuf>,
}

#[derive(Clone)]
struct BenchEpoch {
    index: Arc<File>,
    blocks: Arc<File>,
    access: Option<Arc<File>>,
}

#[derive(Clone, Copy)]
struct BenchJob {
    slot: u64,
    iteration: usize,
}

#[derive(Debug, Clone, Serialize)]
struct BenchRow {
    slot: u64,
    epoch: u64,
    iteration: usize,
    status: String,
    total_us: u128,
    index_read_us: u128,
    block_read_us: u128,
    zstd_decode_us: u128,
    block_decode_us: u128,
    access_read_us: u128,
    access_decode_us: u128,
    render_json_us: u128,
    block_compressed_bytes: usize,
    block_uncompressed_bytes: usize,
    access_bytes: usize,
    render_json_bytes: usize,
    render_json_capacity: usize,
    compressed_capacity: usize,
    block_uncompressed_capacity: usize,
    access_capacity: usize,
    temp_capacity_bytes: usize,
    tx_count: usize,
    signature_count: usize,
    rss_start_kb: u64,
    rss_after_block_read_kb: u64,
    rss_after_zstd_decode_kb: u64,
    rss_after_block_decode_kb: u64,
    rss_after_access_read_kb: u64,
    rss_after_access_decode_kb: u64,
    rss_end_kb: u64,
    rss_peak_kb: u64,
    error: String,
}

#[derive(Default, Serialize)]
struct StageSummary {
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenderJsonMode {
    Off,
    None,
    Signatures,
    Rewards,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenderJsonOutput {
    Buffered,
    Chunked,
}

impl RenderJsonOutput {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "buffered" => Ok(Self::Buffered),
            "chunked" | "streaming" => Ok(Self::Chunked),
            _ => bail!("--render-json-output must be one of: buffered, chunked"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Chunked => "chunked",
        }
    }
}

impl RenderJsonMode {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "off" => Ok(Self::Off),
            "none" => Ok(Self::None),
            "signatures" => Ok(Self::Signatures),
            "rewards" => Ok(Self::Rewards),
            _ => bail!("--render-json must be one of: off, none, signatures, rewards"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::None => "none",
            Self::Signatures => "signatures",
            Self::Rewards => "rewards",
        }
    }
}

fn run_local_bench(config: LocalBenchConfig) -> Result<()> {
    let mut slots = BTreeSet::new();
    for epoch in &config.epochs {
        for slot in sample_epoch_slots(
            &config.archive_root,
            *epoch,
            config.samples_per_epoch,
            config.seed ^ epoch,
        )? {
            slots.insert(slot);
        }
    }
    for slot in &config.slots {
        slots.insert(*slot);
    }
    let slots = slots.into_iter().collect::<Vec<_>>();
    if slots.is_empty() {
        bail!("local-bench selected no slots");
    }

    let mut archives = BTreeMap::new();
    for slot in &slots {
        let epoch = slot / SLOTS_PER_EPOCH;
        if let std::collections::btree_map::Entry::Vacant(entry) = archives.entry(epoch) {
            entry.insert(
                open_bench_epoch(&config.archive_root, epoch, config.include_access)
                    .with_context(|| format!("open epoch {epoch}"))?,
            );
        }
    }
    let archives = Arc::new(archives);

    if let Some(out_dir) = &config.out_dir {
        std::fs::create_dir_all(out_dir)
            .with_context(|| format!("create {}", out_dir.display()))?;
        write_plan_tsv(&out_dir.join("plan.tsv"), &slots)?;
    }

    let measured_jobs = build_jobs(&slots, config.iterations.max(1));
    let warmup_jobs = build_jobs(&slots, config.warmup);
    eprintln!(
        "local getBlock bench: archive_root={} slots={} warmup_jobs={} measured_jobs={} concurrency={} include_access={}",
        config.archive_root.display(),
        slots.len(),
        warmup_jobs.len(),
        measured_jobs.len(),
        config.concurrency,
        config.include_access
    );
    eprintln!(
        "local getBlock bench instrumentation: reuse_buffers={} lite_block_decode={} render_json={} render_json_output={} measure_rss={}",
        config.reuse_buffers,
        config.lite_block_decode,
        config.render_json.as_str(),
        config.render_json_output.as_str(),
        config.measure_rss
    );

    if !warmup_jobs.is_empty() {
        let _ = run_local_bench_jobs(
            Arc::clone(&archives),
            warmup_jobs,
            config.concurrency,
            config.reuse_buffers,
            config.lite_block_decode,
            config.render_json,
            config.render_json_output,
            false,
            false,
        );
    }

    let profiler = start_profiler(config.flamegraph_out.as_ref(), config.profile_frequency)?;
    let wall_started = Instant::now();
    let rows = run_local_bench_jobs(
        Arc::clone(&archives),
        measured_jobs,
        config.concurrency,
        config.reuse_buffers,
        config.lite_block_decode,
        config.render_json,
        config.render_json_output,
        config.measure_rss,
        true,
    );
    let wall_s = wall_started.elapsed().as_secs_f64();
    if let Some(guard) = profiler {
        write_flamegraph_outputs(guard, config.flamegraph_out.as_ref().expect("path checked"))?;
    }
    print_local_bench_summary(
        &rows,
        slots.len(),
        config.concurrency,
        config.include_access,
        config.reuse_buffers,
        config.lite_block_decode,
        config.render_json,
        config.render_json_output,
        config.measure_rss,
        wall_s,
    );

    if let Some(out_dir) = &config.out_dir {
        write_rows_tsv(&out_dir.join("requests.tsv"), &rows)?;
        write_summary_json(
            &out_dir.join("summary.json"),
            &rows,
            slots.len(),
            config.concurrency,
            config.include_access,
            config.reuse_buffers,
            config.lite_block_decode,
            config.render_json,
            config.render_json_output,
            config.measure_rss,
            wall_s,
        )?;
    }

    Ok(())
}

fn start_profiler(
    flamegraph_out: Option<&PathBuf>,
    frequency: i32,
) -> Result<Option<pprof::ProfilerGuard<'static>>> {
    if flamegraph_out.is_none() {
        return Ok(None);
    }
    if frequency <= 0 {
        bail!("--profile-frequency must be positive");
    }
    let mut builder = pprof::ProfilerGuardBuilder::default().frequency(frequency);
    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64"
    ))]
    {
        builder = builder.blocklist(&["libc", "libgcc", "pthread", "vdso"]);
    }
    builder
        .build()
        .map(Some)
        .map_err(|err| anyhow!("start pprof profiler: {err}"))
}

fn write_flamegraph_outputs(guard: pprof::ProfilerGuard<'static>, path: &Path) -> Result<()> {
    let report = guard
        .report()
        .build()
        .map_err(|err| anyhow!("build pprof report: {err}"))?;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let flamegraph = File::create(path).with_context(|| format!("create {}", path.display()))?;
    report
        .flamegraph(flamegraph)
        .map_err(|err| anyhow!("write flamegraph {}: {err}", path.display()))?;

    let mut top_path = path.to_path_buf();
    top_path.set_extension("top.tsv");
    write_pprof_top_tsv(&top_path, &report)?;
    eprintln!(
        "cpu profile: flamegraph={} top={}",
        path.display(),
        top_path.display()
    );
    Ok(())
}

fn write_pprof_top_tsv(path: &Path, report: &pprof::Report) -> Result<()> {
    let mut leaves = BTreeMap::<String, isize>::new();
    let mut total_samples = 0isize;
    for (frames, count) in &report.data {
        total_samples += *count;
        let leaf = frames
            .frames
            .first()
            .and_then(|symbols| symbols.first())
            .map(|symbol| symbol.name())
            .unwrap_or_else(|| "unknown".to_string());
        *leaves.entry(leaf).or_default() += *count;
    }
    let mut values = leaves.into_iter().collect::<Vec<_>>();
    values.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    writeln!(writer, "rank\tleaf_samples\tpercent\tfunction")?;
    for (rank, (name, samples)) in values.into_iter().take(80).enumerate() {
        let percent = if total_samples > 0 {
            samples as f64 * 100.0 / total_samples as f64
        } else {
            0.0
        };
        writeln!(
            writer,
            "{}\t{}\t{percent:.2}\t{}",
            rank + 1,
            samples,
            name.replace(['\t', '\n'], " ")
        )?;
    }
    Ok(())
}

fn parse_u64_specs(value: &str) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for part in value
        .split(|ch: char| ch == ',' || ch == ';' || ch.is_whitespace())
        .filter(|part| !part.is_empty())
    {
        if let Some((left, right)) = part.split_once('-') {
            let start = left
                .parse::<u64>()
                .with_context(|| format!("parse range start {left:?}"))?;
            let end = right
                .parse::<u64>()
                .with_context(|| format!("parse range end {right:?}"))?;
            if start <= end {
                out.extend(start..=end);
            } else {
                out.extend((end..=start).rev());
            }
        } else {
            out.push(
                part.parse::<u64>()
                    .with_context(|| format!("parse integer {part:?}"))?,
            );
        }
    }
    Ok(out)
}

fn sample_epoch_slots(root: &Path, epoch: u64, samples: usize, seed: u64) -> Result<Vec<u64>> {
    let index_path = epoch_dir(root, epoch).join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
    let bytes =
        std::fs::read(&index_path).with_context(|| format!("read {}", index_path.display()))?;
    let expected_len = SLOTS_PER_EPOCH as usize * ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN;
    if bytes.len() != expected_len {
        bail!(
            "{} has {} bytes, expected {}",
            index_path.display(),
            bytes.len(),
            expected_len
        );
    }

    let mut present = Vec::new();
    for slot_offset in 0..SLOTS_PER_EPOCH as usize {
        let off = slot_offset * ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN;
        if !decode_get_block_index_row(&bytes[off..off + ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN])
            .is_missing()
        {
            present.push(epoch * SLOTS_PER_EPOCH + slot_offset as u64);
        }
    }
    if present.is_empty() {
        eprintln!(
            "epoch={epoch}: no present slots in {}",
            index_path.display()
        );
        return Ok(Vec::new());
    }

    let mut selected = BTreeSet::new();
    selected.insert(present[0]);
    selected.insert(*present.last().expect("present checked"));
    let interior = if present.len() > 2 {
        &present[1..present.len() - 1]
    } else {
        &present[..0]
    };
    if samples >= interior.len() {
        selected.extend(interior.iter().copied());
    } else {
        let mut rng = XorShift64::new(seed);
        while selected.len() < samples + 2 {
            let idx = rng.next_usize(interior.len());
            selected.insert(interior[idx]);
        }
    }
    eprintln!(
        "epoch={epoch} present={} selected={} first={} last={} index={}",
        present.len(),
        selected.len(),
        present[0],
        present[present.len() - 1],
        index_path.display()
    );
    Ok(selected.into_iter().collect())
}

fn open_bench_epoch(root: &Path, epoch: u64, include_access: bool) -> Result<BenchEpoch> {
    let dir = epoch_dir(root, epoch);
    let index = Arc::new(
        File::open(dir.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)).with_context(|| {
            format!(
                "open {}",
                dir.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE).display()
            )
        })?,
    );
    let blocks = Arc::new(
        File::open(dir.join(ARCHIVE_V2_BLOCKS_FILE))
            .with_context(|| format!("open {}", dir.join(ARCHIVE_V2_BLOCKS_FILE).display()))?,
    );
    let access = if include_access {
        Some(Arc::new(
            File::open(dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE)).with_context(|| {
                format!("open {}", dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE).display())
            })?,
        ))
    } else {
        None
    };
    Ok(BenchEpoch {
        index,
        blocks,
        access,
    })
}

fn epoch_dir(root: &Path, epoch: u64) -> PathBuf {
    root.join(format!("epoch-{epoch}"))
}

fn build_jobs(slots: &[u64], iterations: usize) -> Vec<BenchJob> {
    let mut jobs = Vec::with_capacity(slots.len().saturating_mul(iterations));
    for iteration in 0..iterations {
        for slot in slots {
            jobs.push(BenchJob {
                slot: *slot,
                iteration,
            });
        }
    }
    jobs
}

fn run_local_bench_jobs(
    archives: Arc<BTreeMap<u64, BenchEpoch>>,
    jobs: Vec<BenchJob>,
    concurrency: usize,
    reuse_buffers: bool,
    lite_block_decode: bool,
    render_json: RenderJsonMode,
    render_json_output: RenderJsonOutput,
    measure_rss: bool,
    keep_rows: bool,
) -> Vec<BenchRow> {
    let jobs = Arc::new(jobs);
    let next = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let archives = Arc::clone(&archives);
        let jobs = Arc::clone(&jobs);
        let next = Arc::clone(&next);
        handles.push(thread::spawn(move || {
            let mut rows = Vec::new();
            let mut scratch = reuse_buffers.then(BenchScratch::default);
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(job) = jobs.get(index).copied() else {
                    break;
                };
                let row = run_local_bench_job(
                    &archives,
                    job,
                    lite_block_decode,
                    render_json,
                    render_json_output,
                    measure_rss,
                    scratch.as_mut(),
                );
                if keep_rows {
                    rows.push(row);
                }
            }
            rows
        }));
    }

    let mut rows = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(mut worker_rows) => rows.append(&mut worker_rows),
            Err(_) => rows.push(BenchRow {
                slot: 0,
                epoch: 0,
                iteration: 0,
                status: "panic".to_string(),
                total_us: 0,
                index_read_us: 0,
                block_read_us: 0,
                zstd_decode_us: 0,
                block_decode_us: 0,
                access_read_us: 0,
                access_decode_us: 0,
                render_json_us: 0,
                block_compressed_bytes: 0,
                block_uncompressed_bytes: 0,
                access_bytes: 0,
                render_json_bytes: 0,
                render_json_capacity: 0,
                compressed_capacity: 0,
                block_uncompressed_capacity: 0,
                access_capacity: 0,
                temp_capacity_bytes: 0,
                tx_count: 0,
                signature_count: 0,
                rss_start_kb: 0,
                rss_after_block_read_kb: 0,
                rss_after_zstd_decode_kb: 0,
                rss_after_block_decode_kb: 0,
                rss_after_access_read_kb: 0,
                rss_after_access_decode_kb: 0,
                rss_end_kb: 0,
                rss_peak_kb: 0,
                error: "worker thread panicked".to_string(),
            }),
        }
    }
    rows.sort_by_key(|row| (row.iteration, row.slot));
    rows
}

#[derive(Default)]
struct BenchScratch {
    compressed: Vec<u8>,
    block_bytes: Vec<u8>,
    access_bytes: Vec<u8>,
}

#[derive(Debug)]
struct BenchBlockStats {
    slot: u64,
    parent_slot: u64,
    block_time: Option<i64>,
    block_height: Option<u64>,
    tx_count: usize,
    signature_count: usize,
    rewards: Option<ArchiveV2HotRewards>,
}

struct LocalRenderAccess {
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
}

#[derive(Debug, Deserialize, SchemaRead)]
struct LegacyBlockAccessBlobV1 {
    version: u16,
    flags: u32,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
}

impl From<LegacyBlockAccessBlobV1> for ArchiveV2BlockAccessBlob {
    fn from(value: LegacyBlockAccessBlobV1) -> Self {
        Self {
            version: value.version,
            flags: value.flags,
            blockhash: value.blockhash,
            previous_blockhash: value.previous_blockhash,
            signature_counts: value.signature_counts,
            signatures: value.signatures,
            pubkeys: value.pubkeys,
            blockhashes: value.blockhashes,
            vote_hashes: Vec::new(),
        }
    }
}

fn run_local_bench_job(
    archives: &BTreeMap<u64, BenchEpoch>,
    job: BenchJob,
    lite_block_decode: bool,
    render_json: RenderJsonMode,
    render_json_output: RenderJsonOutput,
    measure_rss: bool,
    mut scratch: Option<&mut BenchScratch>,
) -> BenchRow {
    let total_started = Instant::now();
    let epoch = job.slot / SLOTS_PER_EPOCH;
    let mut row = BenchRow {
        slot: job.slot,
        epoch,
        iteration: job.iteration,
        status: "ok".to_string(),
        total_us: 0,
        index_read_us: 0,
        block_read_us: 0,
        zstd_decode_us: 0,
        block_decode_us: 0,
        access_read_us: 0,
        access_decode_us: 0,
        render_json_us: 0,
        block_compressed_bytes: 0,
        block_uncompressed_bytes: 0,
        access_bytes: 0,
        render_json_bytes: 0,
        render_json_capacity: 0,
        compressed_capacity: 0,
        block_uncompressed_capacity: 0,
        access_capacity: 0,
        temp_capacity_bytes: 0,
        tx_count: 0,
        signature_count: 0,
        rss_start_kb: rss_kb(measure_rss),
        rss_after_block_read_kb: 0,
        rss_after_zstd_decode_kb: 0,
        rss_after_block_decode_kb: 0,
        rss_after_access_read_kb: 0,
        rss_after_access_decode_kb: 0,
        rss_end_kb: 0,
        rss_peak_kb: 0,
        error: String::new(),
    };
    row.rss_peak_kb = row.rss_start_kb;

    let result = (|| -> Result<()> {
        let archive = archives
            .get(&epoch)
            .with_context(|| format!("epoch {epoch} is not open"))?;
        let slot_offset = job.slot % SLOTS_PER_EPOCH;
        let index_offset = slot_offset
            .checked_mul(ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN as u64)
            .context("index offset overflow")?;
        let mut index_buf = [0u8; ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN];
        let started = Instant::now();
        read_exact_at(&archive.index, &mut index_buf, index_offset)?;
        row.index_read_us = started.elapsed().as_micros();
        let index_row = decode_get_block_index_row(&index_buf);
        if index_row.is_missing() {
            row.status = "missing".to_string();
            return Ok(());
        }

        let block_stats = if let Some(scratch) = scratch.as_mut() {
            scratch.compressed.resize(index_row.block_len as usize, 0);
            let started = Instant::now();
            read_exact_at(
                archive.blocks.as_ref(),
                &mut scratch.compressed,
                index_row.block_offset,
            )?;
            row.block_read_us = started.elapsed().as_micros();
            row.block_compressed_bytes = scratch.compressed.len();
            row.compressed_capacity = scratch.compressed.capacity();
            sample_rss(
                measure_rss,
                &mut row.rss_after_block_read_kb,
                &mut row.rss_peak_kb,
            );

            scratch.block_bytes.clear();
            let started = Instant::now();
            zstd::stream::read::Decoder::new(&scratch.compressed[..])
                .with_context(|| format!("zstd decoder init slot {}", job.slot))?
                .read_to_end(&mut scratch.block_bytes)
                .with_context(|| format!("zstd decode slot {}", job.slot))?;
            row.zstd_decode_us = started.elapsed().as_micros();
            row.block_uncompressed_bytes = scratch.block_bytes.len();
            row.block_uncompressed_capacity = scratch.block_bytes.capacity();
            sample_rss(
                measure_rss,
                &mut row.rss_after_zstd_decode_kb,
                &mut row.rss_peak_kb,
            );

            let started = Instant::now();
            let block_stats = decode_bench_block_stats(
                &scratch.block_bytes,
                lite_block_decode,
                job.slot,
                measure_rss,
                &mut row.rss_after_block_decode_kb,
                &mut row.rss_peak_kb,
            )?;
            row.block_decode_us = started.elapsed().as_micros();
            block_stats
        } else {
            let mut compressed_tmp = Vec::new();
            compressed_tmp.resize(index_row.block_len as usize, 0);
            let started = Instant::now();
            read_exact_at(
                archive.blocks.as_ref(),
                &mut compressed_tmp,
                index_row.block_offset,
            )?;
            row.block_read_us = started.elapsed().as_micros();
            row.block_compressed_bytes = compressed_tmp.len();
            row.compressed_capacity = compressed_tmp.capacity();
            sample_rss(
                measure_rss,
                &mut row.rss_after_block_read_kb,
                &mut row.rss_peak_kb,
            );

            let mut block_tmp = Vec::new();
            let started = Instant::now();
            zstd::stream::read::Decoder::new(&compressed_tmp[..])
                .with_context(|| format!("zstd decoder init slot {}", job.slot))?
                .read_to_end(&mut block_tmp)
                .with_context(|| format!("zstd decode slot {}", job.slot))?;
            row.zstd_decode_us = started.elapsed().as_micros();
            row.block_uncompressed_bytes = block_tmp.len();
            row.block_uncompressed_capacity = block_tmp.capacity();
            sample_rss(
                measure_rss,
                &mut row.rss_after_zstd_decode_kb,
                &mut row.rss_peak_kb,
            );

            let started = Instant::now();
            let block_stats = decode_bench_block_stats(
                &block_tmp,
                lite_block_decode,
                job.slot,
                measure_rss,
                &mut row.rss_after_block_decode_kb,
                &mut row.rss_peak_kb,
            )?;
            row.block_decode_us = started.elapsed().as_micros();
            block_stats
        };
        if block_stats.slot != job.slot {
            bail!(
                "decoded slot {} but requested {}",
                block_stats.slot,
                job.slot
            );
        }
        row.tx_count = block_stats.tx_count;
        row.signature_count = block_stats.signature_count;

        let mut access_for_render = None;
        if let Some(access_file) = &archive.access {
            if index_row.access_len == 0 {
                bail!("slot {} has no block-access range", job.slot);
            }
            let mut access_tmp = Vec::new();
            let access_bytes = if let Some(scratch) = scratch.as_mut() {
                scratch
                    .access_bytes
                    .resize(index_row.access_len as usize, 0);
                &mut scratch.access_bytes
            } else {
                access_tmp.resize(index_row.access_len as usize, 0);
                &mut access_tmp
            };
            let started = Instant::now();
            read_exact_at(access_file.as_ref(), access_bytes, index_row.access_offset)?;
            row.access_read_us = started.elapsed().as_micros();
            row.access_bytes = access_bytes.len();
            row.access_capacity = access_bytes.capacity();
            sample_rss(
                measure_rss,
                &mut row.rss_after_access_read_kb,
                &mut row.rss_peak_kb,
            );

            let started = Instant::now();
            if render_json == RenderJsonMode::Off {
                let access = decode_full_access_blob(access_bytes, job.slot)?;
                validate_access_blob(&access, &row)
                    .with_context(|| format!("validate block-access slot {}", job.slot))?;
            } else {
                let access = decode_local_render_access(access_bytes, render_json)
                    .with_context(|| format!("lite decode block-access slot {}", job.slot))?;
                validate_local_render_access(&access, &row)
                    .with_context(|| format!("validate lite block-access slot {}", job.slot))?;
                access_for_render = Some(access);
            }
            row.access_decode_us = started.elapsed().as_micros();
            sample_rss(
                measure_rss,
                &mut row.rss_after_access_decode_kb,
                &mut row.rss_peak_kb,
            );
        }
        if render_json != RenderJsonMode::Off {
            let access = access_for_render
                .as_ref()
                .context("--render-json requires archive-v2-block-access data")?;
            let started = Instant::now();
            let rendered =
                render_local_get_block_json(render_json, render_json_output, &block_stats, access)?;
            row.render_json_us = started.elapsed().as_micros();
            row.render_json_bytes = rendered.bytes;
            row.render_json_capacity = rendered.capacity;
        }
        row.temp_capacity_bytes = row
            .compressed_capacity
            .saturating_add(row.block_uncompressed_capacity)
            .saturating_add(row.access_capacity)
            .saturating_add(row.render_json_capacity);
        Ok(())
    })();

    row.total_us = total_started.elapsed().as_micros();
    row.rss_end_kb = rss_kb(measure_rss);
    row.rss_peak_kb = row.rss_peak_kb.max(row.rss_end_kb);
    if let Err(err) = result {
        row.status = "error".to_string();
        row.error = format!("{err:#}");
    }
    row
}

fn decode_bench_block_stats(
    bytes: &[u8],
    lite_block_decode: bool,
    slot: u64,
    measure_rss: bool,
    rss_after_block_decode_kb: &mut u64,
    rss_peak_kb: &mut u64,
) -> Result<BenchBlockStats> {
    if lite_block_decode {
        let stats = decode_hot_block_lite_stats(bytes)
            .with_context(|| format!("lite wincode decode hot block slot {slot}"))?;
        sample_rss(measure_rss, rss_after_block_decode_kb, rss_peak_kb);
        return Ok(stats);
    }

    let block = deserialize_archive_v2_hot_block_blob(bytes)
        .with_context(|| format!("wincode decode hot block slot {slot}"))?;
    let stats = BenchBlockStats {
        slot: block.header.slot,
        parent_slot: block.header.parent_slot,
        block_time: block.header.block_time,
        block_height: block.header.block_height,
        tx_count: block.tx_rows.len(),
        signature_count: block
            .tx_rows
            .iter()
            .map(|tx| tx.signature_count as usize)
            .sum(),
        rewards: block.header.rewards,
    };
    sample_rss(measure_rss, rss_after_block_decode_kb, rss_peak_kb);
    Ok(stats)
}

fn decode_hot_block_lite_stats(bytes: &[u8]) -> Result<BenchBlockStats> {
    let mut reader = bytes;
    let slot: u64 = read_wincode_value(&mut reader).context("read header.slot")?;
    let parent_slot: u64 = read_wincode_value(&mut reader).context("read header.parent_slot")?;
    let _: u32 = read_wincode_value(&mut reader).context("read header.blockhash_id")?;
    let _: u32 = read_wincode_value(&mut reader).context("read header.previous_blockhash_id")?;
    let block_time: Option<i64> =
        read_wincode_value(&mut reader).context("read header.block_time")?;
    let block_height: Option<u64> =
        read_wincode_value(&mut reader).context("read header.block_height")?;
    skip_option_hot_rewards(&mut reader).context("skip header.rewards")?;

    let tx_count: u32 = read_wincode_value(&mut reader).context("read tx_count")?;
    let tx_rows_len = read_wincode_len(&mut reader).context("read tx_rows len")?;
    if tx_rows_len != tx_count as usize {
        bail!("tx_count {tx_count} does not match tx_rows len {tx_rows_len}");
    }
    let mut signature_count = 0usize;
    for _ in 0..tx_rows_len {
        let row: ArchiveV2HotTxRow = read_wincode_value(&mut reader).context("read tx row")?;
        signature_count = signature_count.saturating_add(row.signature_count as usize);
    }
    skip_byte_vec(&mut reader).context("skip message_bytes")?;
    skip_byte_vec(&mut reader).context("skip metadata_bytes")?;

    Ok(BenchBlockStats {
        slot,
        parent_slot,
        block_time,
        block_height,
        tx_count: tx_rows_len,
        signature_count,
        rewards: None,
    })
}

struct LocalRenderStats {
    bytes: usize,
    capacity: usize,
}

fn render_local_get_block_json(
    mode: RenderJsonMode,
    output: RenderJsonOutput,
    block: &BenchBlockStats,
    access: &LocalRenderAccess,
) -> Result<LocalRenderStats> {
    let estimated = if mode == RenderJsonMode::Signatures {
        block
            .signature_count
            .saturating_mul(five8::BASE58_ENCODED_64_MAX_LEN)
            .saturating_add(256)
    } else if mode == RenderJsonMode::Rewards {
        block
            .rewards
            .as_ref()
            .map(|rewards| {
                rewards
                    .decoded
                    .len()
                    .saturating_mul(128)
                    .saturating_add(256)
            })
            .unwrap_or(256)
    } else {
        256
    };
    match output {
        RenderJsonOutput::Buffered => {
            let mut w = JsonByteWriter::with_capacity(estimated);
            render_local_get_block_json_into(mode, block, access, &mut w)?;
            Ok(LocalRenderStats {
                bytes: w.len(),
                capacity: w.capacity(),
            })
        }
        RenderJsonOutput::Chunked => {
            let mut w = ChunkedJsonByteWriter::with_capacity(estimated.min(16 * 1024));
            render_local_get_block_json_into(mode, block, access, &mut w)?;
            Ok(w.finish())
        }
    }
}

fn render_local_get_block_json_into<W: LocalJsonWrite>(
    mode: RenderJsonMode,
    block: &BenchBlockStats,
    access: &LocalRenderAccess,
    w: &mut W,
) -> Result<()> {
    w.raw(b"{\"blockhash\":");
    w.base58_32(&access.blockhash);
    w.raw(b",\"previousBlockhash\":");
    w.base58_32(&access.previous_blockhash);
    w.raw(b",\"parentSlot\":");
    w.u64(block.parent_slot);
    w.raw(b",\"blockTime\":");
    w.option_i64(block.block_time);
    w.raw(b",\"blockHeight\":");
    w.option_u64(block.block_height);

    if mode == RenderJsonMode::Signatures {
        w.raw(b",\"signatures\":[");
        let mut signature_index = 0usize;
        let mut first = true;
        for count in &access.signature_counts {
            if *count > 0 {
                let start = signature_index
                    .checked_mul(SIGNATURE_BYTES)
                    .context("signature byte offset overflow")?;
                let end = start
                    .checked_add(SIGNATURE_BYTES)
                    .context("signature byte end overflow")?;
                let signature = access
                    .signatures
                    .get(start..end)
                    .context("signature sidecar ended before transaction signature")?;
                let signature: &[u8; 64] = signature.try_into().expect("checked signature len");
                if !first {
                    w.raw(b",");
                }
                first = false;
                w.base58_64(signature);
            }
            signature_index = signature_index
                .checked_add(usize::from(*count))
                .context("signature index overflow")?;
        }
        w.raw(b"]");
    }

    if mode == RenderJsonMode::Rewards {
        let rewards = block
            .rewards
            .as_ref()
            .context("--render-json rewards requires full block decode")?;
        w.raw(b",\"rewards\":[");
        for (index, reward) in rewards.decoded.iter().enumerate() {
            if index > 0 {
                w.raw(b",");
            }
            w.raw(b"{\"pubkey\":");
            let pubkey = resolve_local_pubkey(access, reward.pubkey)
                .with_context(|| format!("resolve reward pubkey {index}"))?;
            w.base58_32(&pubkey);
            w.raw(b",\"lamports\":");
            w.i64(reward.lamports);
            w.raw(b",\"postBalance\":");
            w.u64(reward.post_balance);
            w.raw(b",\"rewardType\":");
            match reward.reward_type {
                1 => w.raw(br#""Fee""#),
                2 => w.raw(br#""Rent""#),
                3 => w.raw(br#""Staking""#),
                4 => w.raw(br#""Voting""#),
                _ => w.raw(b"null"),
            }
            w.raw(b",\"commission\":");
            w.option_u8(reward.commission);
            w.raw(b"}");
        }
        w.raw(b"]");
    }

    w.raw(b"}");
    Ok(())
}

fn resolve_local_pubkey(access: &LocalRenderAccess, key: CompactPubkey) -> Result<[u8; 32]> {
    match key {
        CompactPubkey::Raw(pubkey) => Ok(pubkey),
        CompactPubkey::Id(id) => access
            .pubkeys
            .binary_search_by_key(&id, |entry| entry.id)
            .ok()
            .and_then(|index| access.pubkeys.get(index))
            .map(|entry| entry.pubkey)
            .with_context(|| format!("missing block-access pubkey id {id}")),
    }
}

fn decode_local_render_access(bytes: &[u8], mode: RenderJsonMode) -> Result<LocalRenderAccess> {
    let mut reader = bytes;
    let version: u16 = read_wincode_value(&mut reader).context("read access.version")?;
    if version != WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION && version != 1 {
        bail!("block-access version {version} is unsupported");
    }
    let _: u32 = read_wincode_value(&mut reader).context("read access.flags")?;
    let blockhash = reader.take_array::<32>().context("read access.blockhash")?;
    let previous_blockhash = reader
        .take_array::<32>()
        .context("read access.previous_blockhash")?;
    if mode == RenderJsonMode::None {
        return Ok(LocalRenderAccess {
            blockhash,
            previous_blockhash,
            signature_counts: Vec::new(),
            signatures: Vec::new(),
            pubkeys: Vec::new(),
        });
    }
    let signature_counts: Vec<u8> =
        read_wincode_value(&mut reader).context("read access.signature_counts")?;
    let signatures: Vec<u8> = read_wincode_value(&mut reader).context("read access.signatures")?;
    let pubkeys = if mode == RenderJsonMode::Rewards {
        read_wincode_value(&mut reader).context("read access.pubkeys")?
    } else {
        Vec::new()
    };
    Ok(LocalRenderAccess {
        blockhash,
        previous_blockhash,
        signature_counts,
        signatures,
        pubkeys,
    })
}

fn decode_full_access_blob(bytes: &[u8], slot: u64) -> Result<ArchiveV2BlockAccessBlob> {
    match wincode::config::deserialize::<ArchiveV2BlockAccessBlob, _>(
        bytes,
        wincode_leb128_config(),
    ) {
        Ok(access) => Ok(access),
        Err(primary_err) => {
            let legacy: LegacyBlockAccessBlobV1 =
                wincode::config::deserialize(bytes, wincode_leb128_config()).with_context(|| {
                    format!(
                        "wincode decode block-access slot {slot}: {primary_err}; legacy v1 decode failed"
                    )
                })?;
            if legacy.version != 1 {
                bail!("block-access version {} is unsupported", legacy.version);
            }
            Ok(legacy.into())
        }
    }
}

fn validate_local_render_access(access: &LocalRenderAccess, row: &BenchRow) -> Result<()> {
    if access.signature_counts.is_empty() && access.signatures.is_empty() {
        return Ok(());
    }
    if access.signature_counts.len() != row.tx_count {
        bail!(
            "block-access signature_counts len {}, expected {} txs",
            access.signature_counts.len(),
            row.tx_count
        );
    }
    let signature_count = access
        .signature_counts
        .iter()
        .map(|count| usize::from(*count))
        .sum::<usize>();
    if signature_count != row.signature_count {
        bail!(
            "block-access signature count {}, expected {}",
            signature_count,
            row.signature_count
        );
    }
    let signature_bytes = signature_count
        .checked_mul(SIGNATURE_BYTES)
        .context("signature bytes overflow")?;
    if access.signatures.len() != signature_bytes {
        bail!(
            "block-access signature bytes {}, expected {}",
            access.signatures.len(),
            signature_bytes
        );
    }
    Ok(())
}

struct JsonByteWriter {
    out: Vec<u8>,
}

impl JsonByteWriter {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            out: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.out.len()
    }

    fn capacity(&self) -> usize {
        self.out.capacity()
    }
}

trait LocalJsonWrite {
    fn raw(&mut self, bytes: &[u8]) {
        self.write_raw(bytes);
    }

    fn write_raw(&mut self, bytes: &[u8]);

    fn u64(&mut self, value: u64) {
        let mut buffer = itoa::Buffer::new();
        self.raw(buffer.format(value).as_bytes());
    }

    fn i64(&mut self, value: i64) {
        let mut buffer = itoa::Buffer::new();
        self.raw(buffer.format(value).as_bytes());
    }

    fn option_i64(&mut self, value: Option<i64>) {
        if let Some(value) = value {
            self.i64(value);
        } else {
            self.raw(b"null");
        }
    }

    fn option_u64(&mut self, value: Option<u64>) {
        if let Some(value) = value {
            self.u64(value);
        } else {
            self.raw(b"null");
        }
    }

    fn option_u8(&mut self, value: Option<u8>) {
        if let Some(value) = value {
            self.u64(u64::from(value));
        } else {
            self.raw(b"null");
        }
    }

    fn base58_32(&mut self, bytes: &[u8; 32]) {
        let mut buf = [0u8; five8::BASE58_ENCODED_32_MAX_LEN];
        let len = five8::encode_32(bytes, &mut buf) as usize;
        self.raw(b"\"");
        self.raw(&buf[..len]);
        self.raw(b"\"");
    }

    fn base58_64(&mut self, bytes: &[u8; 64]) {
        let mut buf = [0u8; five8::BASE58_ENCODED_64_MAX_LEN];
        let len = five8::encode_64(bytes, &mut buf) as usize;
        self.raw(b"\"");
        self.raw(&buf[..len]);
        self.raw(b"\"");
    }
}

impl LocalJsonWrite for JsonByteWriter {
    fn write_raw(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }
}

struct ChunkedJsonByteWriter {
    chunk: Vec<u8>,
    flushed_bytes: usize,
    max_capacity: usize,
    flush_threshold: usize,
}

impl ChunkedJsonByteWriter {
    fn with_capacity(capacity: usize) -> Self {
        let flush_threshold = capacity.max(1024);
        Self {
            chunk: Vec::with_capacity(flush_threshold),
            flushed_bytes: 0,
            max_capacity: flush_threshold,
            flush_threshold,
        }
    }

    fn flush(&mut self) {
        self.flushed_bytes = self.flushed_bytes.saturating_add(self.chunk.len());
        self.chunk.clear();
        self.max_capacity = self.max_capacity.max(self.chunk.capacity());
    }

    fn finish(mut self) -> LocalRenderStats {
        self.flush();
        LocalRenderStats {
            bytes: self.flushed_bytes,
            capacity: self.max_capacity,
        }
    }
}

impl LocalJsonWrite for ChunkedJsonByteWriter {
    fn write_raw(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            if self.chunk.len() == self.flush_threshold {
                self.flush();
            }
            if self.chunk.is_empty() && bytes.len() >= self.flush_threshold {
                self.flushed_bytes = self.flushed_bytes.saturating_add(bytes.len());
                self.max_capacity = self.max_capacity.max(self.flush_threshold);
                break;
            }
            let available = self.flush_threshold.saturating_sub(self.chunk.len());
            let take = available.min(bytes.len());
            self.chunk.extend_from_slice(&bytes[..take]);
            self.max_capacity = self.max_capacity.max(self.chunk.capacity());
            bytes = &bytes[take..];
        }
    }
}

fn read_wincode_value<'de, T>(reader: &mut impl Reader<'de>) -> Result<T>
where
    T: SchemaRead<'de, WincodeLeb128Config, Dst = T>,
{
    Ok(T::get(reader.by_ref())?)
}

fn read_wincode_len<'de>(reader: &mut impl Reader<'de>) -> Result<usize> {
    let len: u64 = read_wincode_value(reader)?;
    usize::try_from(len).context("wincode length does not fit usize")
}

fn skip_raw_bytes<'de>(reader: &mut impl Reader<'de>, len: usize) -> Result<()> {
    reader.take_scoped(len)?;
    Ok(())
}

fn read_option_tag<'de>(reader: &mut impl Reader<'de>) -> Result<u8> {
    let tag: u8 = read_wincode_value(reader)?;
    match tag {
        0 | 1 => Ok(tag),
        _ => bail!("invalid option tag {tag}"),
    }
}

fn skip_option_u8<'de>(reader: &mut impl Reader<'de>) -> Result<()> {
    if read_option_tag(reader)? == 1 {
        let _: u8 = read_wincode_value(reader)?;
    }
    Ok(())
}

fn skip_option_u64<'de>(reader: &mut impl Reader<'de>) -> Result<()> {
    if read_option_tag(reader)? == 1 {
        let _: u64 = read_wincode_value(reader)?;
    }
    Ok(())
}

fn skip_byte_vec<'de>(reader: &mut impl Reader<'de>) -> Result<()> {
    let len = read_wincode_len(reader)?;
    skip_raw_bytes(reader, len)
}

fn skip_option_hot_rewards<'de>(reader: &mut impl Reader<'de>) -> Result<()> {
    if read_option_tag(reader)? == 1 {
        skip_option_u64(reader)?;
        skip_rewards_vec(reader)?;
    }
    Ok(())
}

fn skip_rewards_vec<'de>(reader: &mut impl Reader<'de>) -> Result<()> {
    let len = read_wincode_len(reader)?;
    for _ in 0..len {
        skip_compact_pubkey(reader)?;
        let _: i64 = read_wincode_value(reader)?;
        let _: u64 = read_wincode_value(reader)?;
        let _: i32 = read_wincode_value(reader)?;
        skip_option_u8(reader)?;
    }
    Ok(())
}

fn skip_compact_pubkey<'de>(reader: &mut impl Reader<'de>) -> Result<()> {
    let id: u32 = read_wincode_value(reader)?;
    if id == CompactPubkey::RAW_SENTINEL {
        skip_raw_bytes(reader, 32)?;
    }
    Ok(())
}

fn validate_access_blob(access: &ArchiveV2BlockAccessBlob, row: &BenchRow) -> Result<()> {
    if access.version != WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION && access.version != 1 {
        bail!("block-access version {} is unsupported", access.version);
    }
    if access.signature_counts.len() != row.tx_count {
        bail!(
            "block-access signature_counts len {}, expected {} txs",
            access.signature_counts.len(),
            row.tx_count
        );
    }
    let signature_count = access
        .signature_counts
        .iter()
        .map(|count| usize::from(*count))
        .sum::<usize>();
    if signature_count != row.signature_count {
        bail!(
            "block-access signature count {}, expected {}",
            signature_count,
            row.signature_count
        );
    }
    let signature_bytes = signature_count
        .checked_mul(SIGNATURE_BYTES)
        .context("signature bytes overflow")?;
    if access.signatures.len() != signature_bytes {
        bail!(
            "block-access signature bytes {}, expected {}",
            access.signatures.len(),
            signature_bytes
        );
    }
    Ok(())
}

fn decode_get_block_index_row(bytes: &[u8]) -> ArchiveV2GetBlockIndexRow {
    ArchiveV2GetBlockIndexRow {
        block_offset: u64::from_le_bytes(bytes[0..8].try_into().expect("row block_offset")),
        block_len: u32::from_le_bytes(bytes[8..12].try_into().expect("row block_len")),
        access_offset: u64::from_le_bytes(bytes[12..20].try_into().expect("row access_offset")),
        access_len: u32::from_le_bytes(bytes[20..24].try_into().expect("row access_len")),
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
    while !buf.is_empty() {
        let n = file
            .read_at(buf, offset)
            .with_context(|| format!("pread at offset {offset}"))?;
        if n == 0 {
            bail!("unexpected EOF at offset {offset}");
        }
        offset += n as u64;
        buf = &mut buf[n..];
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_exact_at(_file: &File, _buf: &mut [u8], _offset: u64) -> Result<()> {
    bail!("local-bench requires Unix FileExt::read_at support")
}

fn print_local_bench_summary(
    rows: &[BenchRow],
    slots: usize,
    concurrency: usize,
    include_access: bool,
    reuse_buffers: bool,
    lite_block_decode: bool,
    render_json: RenderJsonMode,
    render_json_output: RenderJsonOutput,
    measure_rss: bool,
    wall_s: f64,
) {
    let ok = rows.iter().filter(|row| row.status == "ok").count();
    let missing = rows.iter().filter(|row| row.status == "missing").count();
    let errors = rows.iter().filter(|row| row.status == "error").count();
    let txs: usize = rows
        .iter()
        .filter(|row| row.status == "ok")
        .map(|row| row.tx_count)
        .sum();
    let compressed: usize = rows
        .iter()
        .filter(|row| row.status == "ok")
        .map(|row| row.block_compressed_bytes)
        .sum();
    let uncompressed: usize = rows
        .iter()
        .filter(|row| row.status == "ok")
        .map(|row| row.block_uncompressed_bytes)
        .sum();
    let access_bytes: usize = rows
        .iter()
        .filter(|row| row.status == "ok")
        .map(|row| row.access_bytes)
        .sum();
    let render_json_bytes: usize = rows
        .iter()
        .filter(|row| row.status == "ok")
        .map(|row| row.render_json_bytes)
        .sum();
    let temp_capacity: usize = rows
        .iter()
        .filter(|row| row.status == "ok")
        .map(|row| row.temp_capacity_bytes)
        .sum();
    let rss_peak_kb = rows.iter().map(|row| row.rss_peak_kb).max().unwrap_or(0);
    println!(
        "local_get_block_bench slots={slots} jobs={} ok={ok} missing={missing} errors={errors} concurrency={concurrency} include_access={include_access} reuse_buffers={reuse_buffers} lite_block_decode={lite_block_decode} render_json={} render_json_output={} measure_rss={measure_rss} wall_s={wall_s:.3} jobs_s={:.2} tx_s={:.2} compressed_MiB_s={:.2} uncompressed_MiB_s={:.2} access_MiB_s={:.2} render_json_MiB_s={:.2} temp_capacity_MiB_sum={:.2} rss_peak_kb={rss_peak_kb}",
        rows.len(),
        render_json.as_str(),
        render_json_output.as_str(),
        if wall_s > 0.0 {
            rows.len() as f64 / wall_s
        } else {
            0.0
        },
        if wall_s > 0.0 {
            txs as f64 / wall_s
        } else {
            0.0
        },
        mib_s(compressed, wall_s),
        mib_s(uncompressed, wall_s),
        mib_s(access_bytes, wall_s),
        mib_s(render_json_bytes, wall_s),
        temp_capacity as f64 / (1024.0 * 1024.0)
    );
    for (name, summary) in stage_summaries(rows) {
        println!(
            "stage={name} avg_ms={:.3} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3}",
            summary.avg_ms, summary.p50_ms, summary.p95_ms, summary.p99_ms, summary.max_ms
        );
    }
    if errors > 0 {
        for row in rows.iter().filter(|row| row.status == "error").take(10) {
            eprintln!(
                "error slot={} iteration={}: {}",
                row.slot, row.iteration, row.error
            );
        }
    }
}

fn stage_summaries(rows: &[BenchRow]) -> BTreeMap<&'static str, StageSummary> {
    let ok = rows
        .iter()
        .filter(|row| row.status == "ok")
        .collect::<Vec<_>>();
    [
        (
            "total",
            ok.iter().map(|row| row.total_us).collect::<Vec<_>>(),
        ),
        (
            "index_read",
            ok.iter().map(|row| row.index_read_us).collect::<Vec<_>>(),
        ),
        (
            "block_read",
            ok.iter().map(|row| row.block_read_us).collect::<Vec<_>>(),
        ),
        (
            "zstd_decode",
            ok.iter().map(|row| row.zstd_decode_us).collect::<Vec<_>>(),
        ),
        (
            "block_decode",
            ok.iter().map(|row| row.block_decode_us).collect::<Vec<_>>(),
        ),
        (
            "access_read",
            ok.iter().map(|row| row.access_read_us).collect::<Vec<_>>(),
        ),
        (
            "access_decode",
            ok.iter()
                .map(|row| row.access_decode_us)
                .collect::<Vec<_>>(),
        ),
        (
            "render_json",
            ok.iter().map(|row| row.render_json_us).collect::<Vec<_>>(),
        ),
    ]
    .into_iter()
    .map(|(name, values)| (name, summarize_us(values)))
    .collect()
}

fn summarize_us(mut values: Vec<u128>) -> StageSummary {
    if values.is_empty() {
        return StageSummary::default();
    }
    values.sort_unstable();
    let sum = values.iter().copied().sum::<u128>() as f64;
    let len = values.len();
    StageSummary {
        avg_ms: sum / len as f64 / 1000.0,
        p50_ms: percentile_us(&values, 0.50) / 1000.0,
        p95_ms: percentile_us(&values, 0.95) / 1000.0,
        p99_ms: percentile_us(&values, 0.99) / 1000.0,
        max_ms: values[len - 1] as f64 / 1000.0,
    }
}

fn percentile_us(values: &[u128], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let rank = ((values.len() - 1) as f64 * percentile).ceil() as usize;
    values[rank.min(values.len() - 1)] as f64
}

fn mib_s(bytes: usize, seconds: f64) -> f64 {
    if seconds > 0.0 {
        bytes as f64 / seconds / (1024.0 * 1024.0)
    } else {
        0.0
    }
}

fn rss_kb(measure: bool) -> u64 {
    if measure {
        current_rss_kb().unwrap_or(0)
    } else {
        0
    }
}

fn sample_rss(measure: bool, target: &mut u64, peak: &mut u64) {
    let value = rss_kb(measure);
    *target = value;
    *peak = (*peak).max(value);
}

#[cfg(target_os = "linux")]
fn current_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        return rest
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok());
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn current_rss_kb() -> Option<u64> {
    None
}

fn write_plan_tsv(path: &Path, slots: &[u64]) -> Result<()> {
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    writeln!(writer, "epoch\tslot")?;
    for slot in slots {
        writeln!(writer, "{}\t{}", slot / SLOTS_PER_EPOCH, slot)?;
    }
    Ok(())
}

fn write_rows_tsv(path: &Path, rows: &[BenchRow]) -> Result<()> {
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    writeln!(
        writer,
        "epoch\tslot\titeration\tstatus\ttotal_us\tindex_read_us\tblock_read_us\tzstd_decode_us\tblock_decode_us\taccess_read_us\taccess_decode_us\trender_json_us\tblock_compressed_bytes\tblock_uncompressed_bytes\taccess_bytes\trender_json_bytes\trender_json_capacity\tcompressed_capacity\tblock_uncompressed_capacity\taccess_capacity\ttemp_capacity_bytes\ttx_count\tsignature_count\trss_start_kb\trss_after_block_read_kb\trss_after_zstd_decode_kb\trss_after_block_decode_kb\trss_after_access_read_kb\trss_after_access_decode_kb\trss_end_kb\trss_peak_kb\terror"
    )?;
    for row in rows {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.epoch,
            row.slot,
            row.iteration,
            row.status,
            row.total_us,
            row.index_read_us,
            row.block_read_us,
            row.zstd_decode_us,
            row.block_decode_us,
            row.access_read_us,
            row.access_decode_us,
            row.render_json_us,
            row.block_compressed_bytes,
            row.block_uncompressed_bytes,
            row.access_bytes,
            row.render_json_bytes,
            row.render_json_capacity,
            row.compressed_capacity,
            row.block_uncompressed_capacity,
            row.access_capacity,
            row.temp_capacity_bytes,
            row.tx_count,
            row.signature_count,
            row.rss_start_kb,
            row.rss_after_block_read_kb,
            row.rss_after_zstd_decode_kb,
            row.rss_after_block_decode_kb,
            row.rss_after_access_read_kb,
            row.rss_after_access_decode_kb,
            row.rss_end_kb,
            row.rss_peak_kb,
            row.error.replace(['\t', '\n'], " ")
        )?;
    }
    Ok(())
}

fn write_summary_json(
    path: &Path,
    rows: &[BenchRow],
    slots: usize,
    concurrency: usize,
    include_access: bool,
    reuse_buffers: bool,
    lite_block_decode: bool,
    render_json: RenderJsonMode,
    render_json_output: RenderJsonOutput,
    measure_rss: bool,
    wall_s: f64,
) -> Result<()> {
    let ok = rows.iter().filter(|row| row.status == "ok").count();
    let missing = rows.iter().filter(|row| row.status == "missing").count();
    let errors = rows.iter().filter(|row| row.status == "error").count();
    let summary = json!({
        "slots": slots,
        "jobs": rows.len(),
        "ok": ok,
        "missing": missing,
        "errors": errors,
        "concurrency": concurrency,
        "includeAccess": include_access,
        "reuseBuffers": reuse_buffers,
        "liteBlockDecode": lite_block_decode,
        "renderJson": render_json.as_str(),
        "renderJsonOutput": render_json_output.as_str(),
        "measureRss": measure_rss,
        "wallSeconds": wall_s,
        "jobsPerSecond": if wall_s > 0.0 { rows.len() as f64 / wall_s } else { 0.0 },
        "stages": stage_summaries(rows),
        "bytes": byte_summary(rows),
        "rss": rss_summary(rows),
    });
    std::fs::write(path, serde_json::to_vec_pretty(&summary)?)
        .with_context(|| format!("write {}", path.display()))
}

fn byte_summary(rows: &[BenchRow]) -> serde_json::Value {
    let ok = rows.iter().filter(|row| row.status == "ok");
    let mut jobs = 0usize;
    let mut compressed = 0usize;
    let mut uncompressed = 0usize;
    let mut access = 0usize;
    let mut render_json_bytes = 0usize;
    let mut render_json_capacity = 0usize;
    let mut temp_capacity = 0usize;
    for row in ok {
        jobs += 1;
        compressed = compressed.saturating_add(row.block_compressed_bytes);
        uncompressed = uncompressed.saturating_add(row.block_uncompressed_bytes);
        access = access.saturating_add(row.access_bytes);
        render_json_bytes = render_json_bytes.saturating_add(row.render_json_bytes);
        render_json_capacity = render_json_capacity.saturating_add(row.render_json_capacity);
        temp_capacity = temp_capacity.saturating_add(row.temp_capacity_bytes);
    }
    json!({
        "okJobs": jobs,
        "compressedBytes": compressed,
        "uncompressedBytes": uncompressed,
        "accessBytes": access,
        "renderJsonBytes": render_json_bytes,
        "renderJsonCapacityBytes": render_json_capacity,
        "tempCapacityBytes": temp_capacity,
        "avgCompressedBytes": avg_usize(compressed, jobs),
        "avgUncompressedBytes": avg_usize(uncompressed, jobs),
        "avgAccessBytes": avg_usize(access, jobs),
        "avgRenderJsonBytes": avg_usize(render_json_bytes, jobs),
        "avgRenderJsonCapacityBytes": avg_usize(render_json_capacity, jobs),
        "avgTempCapacityBytes": avg_usize(temp_capacity, jobs),
    })
}

fn rss_summary(rows: &[BenchRow]) -> serde_json::Value {
    let peak = rows.iter().map(|row| row.rss_peak_kb).max().unwrap_or(0);
    let start = rows
        .iter()
        .filter(|row| row.rss_start_kb > 0)
        .map(|row| row.rss_start_kb)
        .min()
        .unwrap_or(0);
    let end = rows
        .iter()
        .filter(|row| row.rss_end_kb > 0)
        .map(|row| row.rss_end_kb)
        .max()
        .unwrap_or(0);
    json!({
        "startKbMin": start,
        "endKbMax": end,
        "peakKb": peak,
        "deltaEndMinusStartKb": end.saturating_sub(start),
    })
}

fn avg_usize(total: usize, count: usize) -> f64 {
    if count == 0 {
        0.0
    } else {
        total as f64 / count as f64
    }
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 0x9e3779b97f4a7c15 } else { seed },
        }
    }

    fn next(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        debug_assert!(upper > 0);
        (self.next() as usize) % upper
    }
}
