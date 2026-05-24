use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use of_car_reader::{
    CarBlockReader,
    node::{decode_block_metadata, decode_entry_hash, is_block_node, is_entry_node},
    slot_ranges::{
        SLOTS_PER_EPOCH, SlotRange, SlotRangeWithPreviousBlockhash, epoch_for_slot, slot_in_epoch,
        write_slot_ranges_raw, write_slot_ranges_v2_raw,
    },
};
use reqwest::blocking::Client;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    env,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Instant,
};
use time::{OffsetDateTime, macros::format_description};

const DEFAULT_BUFFER_MIB: usize = 64;
const NODE_KIND_PREFIX_LEN: usize = 2;
const BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";

#[derive(Debug, Parser)]
#[command(name = "of-car-slot-index")]
#[command(about = "Build Old Faithful slot range indexes by streaming .car or .car.zst files")]
struct Cli {
    /// CAR file(s), directory roots, HTTP(S) URLs, or s3://bucket/key URLs.
    #[arg(required = true)]
    inputs: Vec<String>,

    /// Directory for epoch-N-slot-ranges.raw and epoch-N-slot-ranges-v2.raw.
    #[arg(long = "output-dir", default_value = "out")]
    output_dir: PathBuf,

    /// Root for epoch-N/blockhash_registry.bin. Defaults to output-dir/blockhash-registry.
    #[arg(long = "blockhash-dir")]
    blockhash_dir: Option<PathBuf>,

    /// Root used to seed epoch N from epoch N-1/blockhash_registry.bin.
    /// Defaults to blockhash-dir.
    #[arg(long = "seed-blockhash-dir")]
    seed_blockhash_dir: Option<PathBuf>,

    /// Extra HTTP header as NAME=VALUE for HTTP(S) inputs. May be repeated.
    #[arg(long = "header")]
    headers: Vec<String>,

    /// Read a bearer token from this environment variable and send `Authorization: Bearer <token>`.
    #[arg(long)]
    bearer_token_env: Option<String>,

    /// S3-compatible endpoint for s3:// inputs, for example `https://s3.us-west-004.backblazeb2.com`.
    /// If omitted, AWS S3 virtual-hosted URLs are used.
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// S3 signing region for s3:// inputs.
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,

    #[arg(long, default_value = "AWS_ACCESS_KEY_ID")]
    s3_access_key_id_env: String,

    #[arg(long, default_value = "AWS_SECRET_ACCESS_KEY")]
    s3_secret_access_key_env: String,

    #[arg(long)]
    s3_session_token_env: Option<String>,

    /// Base58 previous blockhash for the first present block. Intended for
    /// one-off single-epoch runs; seed-blockhash-dir is better for batches.
    #[arg(long = "seed-previous-blockhash")]
    seed_previous_blockhash: Option<String>,

    /// First epoch to process, inclusive.
    #[arg(long = "start-epoch")]
    start_epoch: Option<u64>,

    /// Last epoch to process, inclusive.
    #[arg(long = "end-epoch")]
    end_epoch: Option<u64>,

    /// Number of epoch files to scan concurrently.
    #[arg(long, default_value_t = 1)]
    jobs: usize,

    /// Prefer .car.zst when both .car and .car.zst exist for an epoch.
    #[arg(long)]
    prefer_zst: bool,

    /// Skip writing v2 output. Useful for a first parallel pass that only
    /// materializes raw ranges and blockhash registries.
    #[arg(long)]
    no_v2: bool,

    /// Write only epoch-N-slot-ranges.raw. This skips v2 rows and blockhash
    /// registry output for workers that source previousBlockhash elsewhere.
    #[arg(long)]
    raw_only: bool,

    /// Fail v2 builds for epoch > 0 when no previous blockhash seed is found.
    #[arg(long)]
    require_seed: bool,

    /// Replace existing outputs.
    #[arg(long)]
    overwrite: bool,

    /// I/O buffer size per worker.
    #[arg(long, default_value_t = DEFAULT_BUFFER_MIB)]
    buffer_mib: usize,
}

#[derive(Debug, Clone)]
struct Config {
    output_dir: PathBuf,
    blockhash_dir: PathBuf,
    seed_blockhash_dir: PathBuf,
    seed_previous_blockhash: Option<[u8; 32]>,
    headers: Vec<(String, String)>,
    bearer_token: Option<String>,
    s3_endpoint: Option<String>,
    s3_region: String,
    s3_access_key_id_env: String,
    s3_secret_access_key_env: String,
    s3_session_token_env: Option<String>,
    jobs: usize,
    prefer_zst: bool,
    no_v2: bool,
    raw_only: bool,
    require_seed: bool,
    overwrite: bool,
    buffer_bytes: usize,
}

#[derive(Debug, Clone)]
struct EpochInput {
    epoch: u64,
    source: InputSource,
}

#[derive(Debug, Clone)]
enum InputSource {
    Local(PathBuf),
    Http(String),
    S3(S3Object),
}

#[derive(Debug, Clone)]
struct S3Object {
    bucket: String,
    key: String,
}

#[derive(Debug)]
struct BuildSummary {
    epoch: u64,
    source: String,
    raw_out: PathBuf,
    v2_out: Option<PathBuf>,
    registry_out: Option<PathBuf>,
    entries: u64,
    entry_nodes: u64,
    blocks: u64,
    present_slots: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    out_of_epoch_blocks: u64,
    skipped: bool,
    elapsed_ms: u128,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let output_dir = cli.output_dir;
    let raw_only = cli.raw_only;
    let blockhash_dir = cli
        .blockhash_dir
        .unwrap_or_else(|| output_dir.join("blockhash-registry"));
    let seed_blockhash_dir = cli
        .seed_blockhash_dir
        .clone()
        .unwrap_or_else(|| blockhash_dir.clone());
    let seed_previous_blockhash = cli
        .seed_previous_blockhash
        .as_deref()
        .map(decode_base58_hash)
        .transpose()?;
    let headers = cli
        .headers
        .into_iter()
        .map(parse_header)
        .collect::<Result<Vec<_>>>()?;
    let bearer_token = cli
        .bearer_token_env
        .as_deref()
        .map(env::var)
        .transpose()
        .context("read bearer token env")?;

    let config = Arc::new(Config {
        output_dir,
        blockhash_dir,
        seed_blockhash_dir,
        seed_previous_blockhash,
        headers,
        bearer_token,
        s3_endpoint: cli.s3_endpoint,
        s3_region: cli.s3_region,
        s3_access_key_id_env: cli.s3_access_key_id_env,
        s3_secret_access_key_env: cli.s3_secret_access_key_env,
        s3_session_token_env: cli.s3_session_token_env,
        jobs: cli.jobs.max(1),
        prefer_zst: cli.prefer_zst,
        no_v2: cli.no_v2 || raw_only,
        raw_only,
        require_seed: cli.require_seed,
        overwrite: cli.overwrite,
        buffer_bytes: cli
            .buffer_mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| anyhow!("buffer-mib overflow"))?,
    });

    let inputs = discover_inputs(&cli.inputs, &config, cli.start_epoch, cli.end_epoch)?;
    if inputs.is_empty() {
        return Err(anyhow!("no epoch CAR files found"));
    }

    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("create {}", config.output_dir.display()))?;
    if !config.raw_only {
        fs::create_dir_all(&config.blockhash_dir)
            .with_context(|| format!("create {}", config.blockhash_dir.display()))?;
    }

    let jobs = config.jobs.min(inputs.len()).max(1);
    eprintln!(
        "of-car-slot-index: epochs={} jobs={} output={} blockhash_dir={} no_v2={} raw_only={}",
        inputs.len(),
        jobs,
        config.output_dir.display(),
        if config.raw_only {
            "-".to_string()
        } else {
            config.blockhash_dir.display().to_string()
        },
        config.no_v2,
        config.raw_only
    );

    let work = Arc::new(Mutex::new(VecDeque::from(inputs)));
    let (tx, rx) = mpsc::channel();
    for _ in 0..jobs {
        let work = Arc::clone(&work);
        let tx = tx.clone();
        let config = Arc::clone(&config);
        thread::spawn(move || {
            loop {
                let item = {
                    let mut work = work.lock().expect("work mutex poisoned");
                    work.pop_front()
                };
                let Some(input) = item else {
                    break;
                };
                let epoch = input.epoch;
                let result =
                    build_epoch(input, &config).with_context(|| format!("build epoch {epoch}"));
                if tx.send((epoch, result)).is_err() {
                    break;
                }
            }
        });
    }
    drop(tx);

    let mut failures = 0usize;
    let mut completed = 0usize;
    for (epoch, result) in rx {
        match result {
            Ok(summary) => {
                completed += 1;
                print_summary(&summary);
            }
            Err(err) => {
                failures += 1;
                eprintln!("epoch={epoch}: ERROR {err:?}");
            }
        }
    }

    if failures != 0 {
        return Err(anyhow!("{failures} epoch build(s) failed"));
    }
    eprintln!("of-car-slot-index: completed {completed} epoch(s)");
    Ok(())
}

fn discover_inputs(
    inputs: &[String],
    config: &Config,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
) -> Result<Vec<EpochInput>> {
    let mut by_epoch = BTreeMap::<u64, InputSource>::new();
    for input in inputs {
        if let Some(source) = remote_input_source(input)? {
            add_input_source(&mut by_epoch, source, config, start_epoch, end_epoch)?;
            continue;
        }

        let path = PathBuf::from(input);
        if path.is_dir() {
            for entry in fs::read_dir(&path).with_context(|| format!("read {}", path.display()))? {
                let path = entry?.path();
                add_input_path(&mut by_epoch, path, config, start_epoch, end_epoch)?;
            }
        } else {
            add_input_path(&mut by_epoch, path, config, start_epoch, end_epoch)?;
        }
    }

    Ok(by_epoch
        .into_iter()
        .map(|(epoch, source)| EpochInput { epoch, source })
        .collect())
}

fn add_input_path(
    by_epoch: &mut BTreeMap<u64, InputSource>,
    path: PathBuf,
    config: &Config,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
) -> Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    let Some(epoch) = epoch_from_path(&path) else {
        return Ok(());
    };
    add_epoch_source(
        by_epoch,
        epoch,
        InputSource::Local(path),
        config,
        start_epoch,
        end_epoch,
    )
}

fn add_input_source(
    by_epoch: &mut BTreeMap<u64, InputSource>,
    source: InputSource,
    config: &Config,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
) -> Result<()> {
    let Some(epoch) = epoch_from_source(&source) else {
        return Ok(());
    };
    add_epoch_source(by_epoch, epoch, source, config, start_epoch, end_epoch)
}

fn add_epoch_source(
    by_epoch: &mut BTreeMap<u64, InputSource>,
    epoch: u64,
    source: InputSource,
    config: &Config,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
) -> Result<()> {
    if start_epoch.is_some_and(|start| epoch < start) || end_epoch.is_some_and(|end| epoch > end) {
        return Ok(());
    }

    match by_epoch.get(&epoch) {
        Some(existing) if prefer_candidate(existing, &source, config.prefer_zst) => {
            by_epoch.insert(epoch, source);
        }
        None => {
            by_epoch.insert(epoch, source);
        }
        Some(_) => {}
    }
    Ok(())
}

fn build_epoch(input: EpochInput, config: &Config) -> Result<BuildSummary> {
    let started = Instant::now();
    let paths = output_paths(input.epoch, config);
    if !config.overwrite
        && paths.raw_out.is_file()
        && (config.raw_only || paths.registry_out.is_file())
        && (config.no_v2 || paths.v2_out.is_file())
    {
        return Ok(BuildSummary {
            epoch: input.epoch,
            source: input.source.display(),
            raw_out: paths.raw_out,
            v2_out: (!config.no_v2).then_some(paths.v2_out),
            registry_out: (!config.raw_only).then_some(paths.registry_out),
            entries: 0,
            entry_nodes: 0,
            blocks: 0,
            present_slots: 0,
            first_slot: None,
            last_slot: None,
            out_of_epoch_blocks: 0,
            skipped: true,
            elapsed_ms: started.elapsed().as_millis(),
        });
    }

    let previous_blockhash = if config.no_v2 {
        None
    } else {
        seed_previous_blockhash(input.epoch, config)?
    };

    let source_label = input.source.display();
    let reader = input
        .source
        .open(config)
        .with_context(|| format!("open {source_label}"))?;
    let scan = if input.source.is_zstd() {
        let decoder =
            zstd::Decoder::new(reader).with_context(|| format!("open zstd {source_label}"))?;
        scan_car_reader(
            decoder,
            input.epoch,
            previous_blockhash,
            config.buffer_bytes,
            !config.raw_only,
            !config.no_v2,
        )
    } else {
        scan_car_reader(
            reader,
            input.epoch,
            previous_blockhash,
            config.buffer_bytes,
            !config.raw_only,
            !config.no_v2,
        )
    }
    .with_context(|| format!("scan {source_label}"))?;

    write_raw_atomic(&paths.raw_out, &scan.raw_ranges)?;
    if !config.raw_only {
        write_registry_atomic(&paths.registry_out, &scan.blockhashes)?;
    }
    if !config.no_v2 {
        write_v2_atomic(&paths.v2_out, &scan.v2_ranges)?;
    }

    Ok(BuildSummary {
        epoch: input.epoch,
        source: source_label,
        raw_out: paths.raw_out,
        v2_out: (!config.no_v2).then_some(paths.v2_out),
        registry_out: (!config.raw_only).then_some(paths.registry_out),
        entries: scan.entries,
        entry_nodes: scan.entry_nodes,
        blocks: scan.blocks,
        present_slots: scan.present_slots,
        first_slot: scan.first_slot,
        last_slot: scan.last_slot,
        out_of_epoch_blocks: scan.out_of_epoch_blocks,
        skipped: false,
        elapsed_ms: started.elapsed().as_millis(),
    })
}

struct OutputPaths {
    raw_out: PathBuf,
    v2_out: PathBuf,
    registry_out: PathBuf,
}

fn output_paths(epoch: u64, config: &Config) -> OutputPaths {
    OutputPaths {
        raw_out: config
            .output_dir
            .join(format!("epoch-{epoch}-slot-ranges.raw")),
        v2_out: config
            .output_dir
            .join(format!("epoch-{epoch}-slot-ranges-v2.raw")),
        registry_out: config
            .blockhash_dir
            .join(format!("epoch-{epoch}"))
            .join(BLOCKHASH_REGISTRY_FILE),
    }
}

struct ScanOutput {
    raw_ranges: Vec<SlotRange>,
    v2_ranges: Vec<SlotRangeWithPreviousBlockhash>,
    blockhashes: Vec<[u8; 32]>,
    entries: u64,
    entry_nodes: u64,
    blocks: u64,
    present_slots: u64,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    out_of_epoch_blocks: u64,
}

fn scan_car_reader<R: Read>(
    reader: R,
    epoch: u64,
    seed_previous_blockhash: Option<[u8; 32]>,
    buffer_bytes: usize,
    collect_blockhashes: bool,
    build_v2: bool,
) -> Result<ScanOutput> {
    let mut reader = CarBlockReader::with_capacity(reader, buffer_bytes);
    reader.skip_header().context("read CAR header")?;

    let mut raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut v2_ranges = if build_v2 {
        vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize]
    } else {
        Vec::new()
    };
    let mut blockhashes = if collect_blockhashes {
        Vec::with_capacity(SLOTS_PER_EPOCH as usize)
    } else {
        Vec::new()
    };
    let mut scratch = Vec::with_capacity(1024);
    let mut pending_start: Option<u64> = None;
    let mut pending_blockhash = [0u8; 32];
    let mut pending_has_blockhash = false;
    let mut previous_blockhash = seed_previous_blockhash;
    let mut entries = 0u64;
    let mut entry_nodes = 0u64;
    let mut blocks = 0u64;
    let mut present_slots = 0u64;
    let mut first_slot = None;
    let mut last_slot = None;
    let mut out_of_epoch_blocks = 0u64;

    while let Some(entry) = reader
        .read_entry_payload_if_prefix_with_scratch(&mut scratch, NODE_KIND_PREFIX_LEN, |prefix| {
            is_entry_node(prefix) || is_block_node(prefix)
        })
        .with_context(|| format!("read CAR entry #{entries}"))?
    {
        pending_start.get_or_insert(entry.location.car_offset);
        entries += 1;

        let Some(payload) = entry.payload else {
            continue;
        };

        if is_entry_node(entry.prefix) {
            entry_nodes += 1;
            if collect_blockhashes {
                let hash = decode_entry_hash(payload).with_context(|| {
                    format!("decode entry hash at {}", entry.location.car_offset)
                })?;
                if hash.len() != 32 {
                    return Err(anyhow!(
                        "entry hash length {} at CAR offset {}",
                        hash.len(),
                        entry.location.car_offset
                    ));
                }
                pending_blockhash.copy_from_slice(hash);
                pending_has_blockhash = true;
            }
            continue;
        }

        if !is_block_node(entry.prefix) {
            continue;
        }

        let (slot, _meta) = decode_block_metadata(payload)
            .with_context(|| format!("decode block metadata at {}", entry.location.car_offset))?;
        blocks += 1;
        if collect_blockhashes && !pending_has_blockhash {
            return Err(anyhow!(
                "block slot {slot} at offset {} had no preceding Entry hash",
                entry.location.car_offset
            ));
        }

        let blockhash = pending_blockhash;
        let start = pending_start.unwrap_or(entry.location.car_offset);
        let end = entry
            .location
            .car_offset
            .checked_add(entry.total_len as u64)
            .ok_or_else(|| anyhow!("CAR range end overflow for slot {slot}"))?;
        let len = u32::try_from(end.saturating_sub(start))
            .with_context(|| format!("slot {slot} CAR range exceeds u32"))?;

        if epoch_for_slot(slot) == epoch {
            let idx =
                usize::try_from(slot_in_epoch(slot)).context("slot-in-epoch exceeds usize")?;
            if !raw_ranges[idx].is_empty() {
                return Err(anyhow!("duplicate block for slot {slot}"));
            }

            let range = SlotRange { offset: start, len };
            raw_ranges[idx] = range;
            if build_v2 {
                let previous = previous_blockhash
                    .or_else(|| (slot == 0 && collect_blockhashes).then_some(blockhash))
                    .unwrap_or([0; 32]);
                v2_ranges[idx] = SlotRangeWithPreviousBlockhash {
                    range,
                    previous_blockhash: previous,
                };
            }
            if collect_blockhashes {
                blockhashes.push(blockhash);
            }
            present_slots += 1;
            first_slot.get_or_insert(slot);
            last_slot = Some(slot);
        } else {
            out_of_epoch_blocks += 1;
        }

        if collect_blockhashes {
            previous_blockhash = Some(blockhash);
        }
        pending_start = None;
        pending_blockhash = [0; 32];
        pending_has_blockhash = false;
    }

    Ok(ScanOutput {
        raw_ranges,
        v2_ranges,
        blockhashes,
        entries,
        entry_nodes,
        blocks,
        present_slots,
        first_slot,
        last_slot,
        out_of_epoch_blocks,
    })
}

fn seed_previous_blockhash(epoch: u64, config: &Config) -> Result<Option<[u8; 32]>> {
    if let Some(seed) = config.seed_previous_blockhash {
        return Ok(Some(seed));
    }
    if epoch == 0 {
        return Ok(None);
    }

    let path = config
        .seed_blockhash_dir
        .join(format!("epoch-{}", epoch - 1))
        .join(BLOCKHASH_REGISTRY_FILE);
    if path.is_file() {
        return read_last_blockhash_registry(&path).map(Some);
    }

    if config.require_seed {
        return Err(anyhow!(
            "missing previous blockhash seed for epoch {epoch}: {}",
            path.display()
        ));
    }
    eprintln!(
        "epoch={epoch}: warning no previous blockhash seed at {}; first v2 row will use zeros",
        path.display()
    );
    Ok(None)
}

fn write_raw_atomic(path: &Path, ranges: &[SlotRange]) -> Result<()> {
    with_tmp_path(path, |tmp| {
        let mut writer = BufWriter::with_capacity(256 * 1024, File::create(tmp)?);
        write_slot_ranges_raw(&mut writer, ranges)?;
        writer.flush()?;
        Ok(())
    })
}

fn write_v2_atomic(path: &Path, ranges: &[SlotRangeWithPreviousBlockhash]) -> Result<()> {
    with_tmp_path(path, |tmp| {
        let mut writer = BufWriter::with_capacity(256 * 1024, File::create(tmp)?);
        write_slot_ranges_v2_raw(&mut writer, ranges)?;
        writer.flush()?;
        Ok(())
    })
}

fn write_registry_atomic(path: &Path, blockhashes: &[[u8; 32]]) -> Result<()> {
    with_tmp_path(path, |tmp| {
        let mut writer = BufWriter::with_capacity(256 * 1024, File::create(tmp)?);
        for hash in blockhashes {
            writer.write_all(hash)?;
        }
        writer.flush()?;
        Ok(())
    })
}

fn with_tmp_path(path: &Path, write_tmp: impl FnOnce(&Path) -> Result<()>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = path.with_file_name(format!(
        "{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("slot-index"),
        std::process::id()
    ));
    write_tmp(&tmp).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("publish {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn read_last_blockhash_registry(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    if len == 0 || len % 32 != 0 {
        return Err(anyhow!(
            "{} has invalid length {} (expected a non-empty multiple of 32)",
            path.display(),
            len
        ));
    }
    file.seek(SeekFrom::End(-32))
        .with_context(|| format!("seek {}", path.display()))?;
    let mut hash = [0u8; 32];
    file.read_exact(&mut hash)
        .with_context(|| format!("read last blockhash from {}", path.display()))?;
    Ok(hash)
}

fn print_summary(summary: &BuildSummary) {
    if summary.skipped {
        eprintln!(
            "epoch={}: skip existing raw={} v2={} registry={}",
            summary.epoch,
            summary.raw_out.display(),
            summary
                .v2_out
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".to_string()),
            summary
                .registry_out
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".to_string())
        );
        return;
    }

    eprintln!(
        "epoch={}: built from {} entries={} entry_nodes={} blocks={} present_slots={} first_slot={} last_slot={} out_of_epoch_blocks={} elapsed_s={:.2} raw={} v2={} registry={}",
        summary.epoch,
        summary.source,
        summary.entries,
        summary.entry_nodes,
        summary.blocks,
        summary.present_slots,
        display_opt_u64(summary.first_slot),
        display_opt_u64(summary.last_slot),
        summary.out_of_epoch_blocks,
        summary.elapsed_ms as f64 / 1000.0,
        summary.raw_out.display(),
        summary
            .v2_out
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "-".to_string()),
        summary
            .registry_out
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "-".to_string()),
    );
}

impl InputSource {
    fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::Http(url) => url.clone(),
            Self::S3(object) => format!("s3://{}/{}", object.bucket, object.key),
        }
    }

    fn is_zstd(&self) -> bool {
        match self {
            Self::Local(path) => is_zstd_name(&path.display().to_string()),
            Self::Http(url) => is_zstd_name(url),
            Self::S3(object) => is_zstd_name(&object.key),
        }
    }

    fn open(&self, config: &Config) -> Result<Box<dyn Read + Send>> {
        match self {
            Self::Local(path) => {
                let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
                Ok(Box::new(BufReader::with_capacity(
                    config.buffer_bytes,
                    file,
                )))
            }
            Self::Http(url) => open_http_input(url, config),
            Self::S3(object) => open_s3_input(object, config),
        }
    }
}

fn prefer_candidate(existing: &InputSource, candidate: &InputSource, prefer_zst: bool) -> bool {
    let existing_zst = existing.is_zstd();
    let candidate_zst = candidate.is_zstd();
    if prefer_zst {
        !existing_zst && candidate_zst
    } else {
        existing_zst && !candidate_zst
    }
}

fn epoch_from_path(path: &Path) -> Option<u64> {
    let filename = path.file_name()?.to_str()?;
    epoch_from_filename(filename)
}

fn epoch_from_source(source: &InputSource) -> Option<u64> {
    match source {
        InputSource::Local(path) => epoch_from_path(path),
        InputSource::Http(url) => epoch_from_name(url),
        InputSource::S3(object) => epoch_from_name(&object.key),
    }
}

fn epoch_from_name(name: &str) -> Option<u64> {
    let clean = name
        .split_once('?')
        .map(|(head, _)| head)
        .unwrap_or(name)
        .split_once('#')
        .map(|(head, _)| head)
        .unwrap_or(name);
    let filename = clean.rsplit('/').next()?;
    epoch_from_filename(filename)
}

fn epoch_from_filename(filename: &str) -> Option<u64> {
    let rest = filename.strip_prefix("epoch-")?;
    if !(rest.ends_with(".car") || rest.ends_with(".car.zst")) {
        return None;
    }
    let digits_len = rest
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digits_len == 0 {
        return None;
    }
    rest[..digits_len].parse().ok()
}

fn is_zstd_name(name: &str) -> bool {
    name.split_once('?')
        .map(|(head, _)| head)
        .unwrap_or(name)
        .split_once('#')
        .map(|(head, _)| head)
        .unwrap_or(name)
        .rsplit('/')
        .next()
        .is_some_and(|name| name.ends_with(".zst"))
}

fn remote_input_source(input: &str) -> Result<Option<InputSource>> {
    if input.starts_with("http://") || input.starts_with("https://") {
        return Ok(Some(InputSource::Http(input.to_string())));
    }
    if let Some(rest) = input.strip_prefix("s3://") {
        let Some((bucket, key)) = rest.split_once('/') else {
            bail!("s3 input must be s3://bucket/key, got {input}");
        };
        if bucket.is_empty() || key.is_empty() {
            bail!("s3 input must include a non-empty bucket and key, got {input}");
        }
        return Ok(Some(InputSource::S3(S3Object {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })));
    }
    Ok(None)
}

fn parse_header(value: String) -> Result<(String, String)> {
    let Some((name, raw_value)) = value.split_once('=') else {
        bail!("header must be NAME=VALUE");
    };
    if name.trim().is_empty() {
        bail!("header name cannot be empty");
    }
    Ok((name.trim().to_string(), raw_value.to_string()))
}

fn open_http_input(url: &str, config: &Config) -> Result<Box<dyn Read + Send>> {
    let client = Client::builder()
        .tcp_nodelay(true)
        .build()
        .context("build HTTP client")?;
    let mut request = client.get(url);
    for (name, value) in &config.headers {
        request = request.header(name, value);
    }
    if let Some(token) = &config.bearer_token {
        request = request.bearer_auth(token);
    }
    let response = request.send().with_context(|| format!("GET {url}"))?;
    if !response.status().is_success() {
        bail!("{url} returned HTTP {}", response.status());
    }
    Ok(Box::new(response))
}

fn open_s3_input(object: &S3Object, config: &Config) -> Result<Box<dyn Read + Send>> {
    let access_key = env::var(&config.s3_access_key_id_env)
        .with_context(|| format!("read ${}", config.s3_access_key_id_env))?;
    let secret_key = env::var(&config.s3_secret_access_key_env)
        .with_context(|| format!("read ${}", config.s3_secret_access_key_env))?;
    let session_token = config
        .s3_session_token_env
        .as_ref()
        .map(env::var)
        .transpose()
        .context("read S3 session token env")?;

    if config.s3_endpoint.is_none() && config.s3_region == "auto" {
        bail!(
            "--s3-region must be set for AWS S3 inputs, or pass --s3-endpoint for S3-compatible storage"
        );
    }
    let endpoint = config
        .s3_endpoint
        .clone()
        .unwrap_or_else(|| aws_s3_endpoint(&object.bucket, &config.s3_region));
    let endpoint = endpoint.trim_end_matches('/').to_string();
    let url = if config.s3_endpoint.is_some() {
        format!(
            "{}/{}/{}",
            endpoint,
            uri_encode_path_segment(&object.bucket),
            uri_encode_path(&object.key)
        )
    } else {
        format!("{}/{}", endpoint, uri_encode_path(&object.key))
    };
    let host = host_from_url(&url)?;
    let canonical_uri = if config.s3_endpoint.is_some() {
        format!(
            "/{}/{}",
            uri_encode_path_segment(&object.bucket),
            uri_encode_path(&object.key)
        )
    } else {
        format!("/{}", uri_encode_path(&object.key))
    };
    let (date, amz_date) = amz_dates()?;

    let mut headers = vec![
        ("host".to_string(), host.clone()),
        (
            "x-amz-content-sha256".to_string(),
            "UNSIGNED-PAYLOAD".to_string(),
        ),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    if let Some(token) = &session_token {
        headers.push(("x-amz-security-token".to_string(), token.clone()));
    }
    headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical_headers = headers
        .iter()
        .map(|(name, value)| format!("{name}:{}\n", value.trim()))
        .collect::<String>();
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request =
        format!("GET\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\nUNSIGNED-PAYLOAD");
    let credential_scope = format!("{date}/{}/s3/aws4_request", config.s3_region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        hex_lower(&Sha256::digest(canonical_request.as_bytes()))
    );
    let signing_key = signing_key(&secret_key, &date, &config.s3_region);
    let signature = hex_lower(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let client = Client::builder()
        .tcp_nodelay(true)
        .build()
        .context("build HTTP client")?;
    let mut request = client
        .get(&url)
        .header("host", host)
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
        .header("x-amz-date", amz_date)
        .header("authorization", authorization);
    if let Some(token) = &session_token {
        request = request.header("x-amz-security-token", token);
    }

    let response = request
        .send()
        .with_context(|| format!("S3 GET s3://{}/{}", object.bucket, object.key))?;
    if !response.status().is_success() {
        bail!(
            "S3 GET s3://{}/{} returned HTTP {}",
            object.bucket,
            object.key,
            response.status()
        );
    }
    Ok(Box::new(response))
}

fn aws_s3_endpoint(bucket: &str, region: &str) -> String {
    if region == "us-east-1" {
        format!("https://{bucket}.s3.amazonaws.com")
    } else {
        format!("https://{bucket}.s3.{region}.amazonaws.com")
    }
}

fn host_from_url(url: &str) -> Result<String> {
    let parsed = reqwest::Url::parse(url).with_context(|| format!("parse URL {url}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("URL must include a host: {url}"))?;
    Ok(match parsed.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    })
}

fn decode_base58_hash(value: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode base58 blockhash {value}"))?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow!("blockhash must be 32 bytes, got {}", bytes.len()))
}

fn display_opt_u64(value: Option<u64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn amz_dates() -> Result<(String, String)> {
    let now = OffsetDateTime::now_utc();
    let date = now.format(format_description!("[year][month][day]"))?;
    let amz_date = now.format(format_description!(
        "[year][month][day]T[hour][minute][second]Z"
    ))?;
    Ok((date, amz_date))
}

fn signing_key(secret_key: &str, date: &str, region: &str) -> [u8; 32] {
    let k_date = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut key_block = [0u8; 64];
    if key.len() > 64 {
        key_block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0x36u8; 64];
    let mut outer_pad = [0x5cu8; 64];
    for i in 0..64 {
        inner_pad[i] ^= key_block[i];
        outer_pad[i] ^= key_block[i];
    }

    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(message);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_hash);
    let out = outer.finalize();
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&out);
    bytes
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn uri_encode_path(path: &str) -> String {
    path.split('/')
        .map(uri_encode_path_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn uri_encode_path_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => {
                out.push('%');
                out.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
                out.push(char::from(b"0123456789ABCDEF"[(byte & 0x0f) as usize]));
            }
        }
    }
    out
}
