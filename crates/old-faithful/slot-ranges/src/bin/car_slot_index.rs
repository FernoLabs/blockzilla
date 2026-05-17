use anyhow::{Context, Result, anyhow};
use clap::Parser;
use of_car_reader::{
    CarBlockReader,
    node::{decode_block_metadata, decode_entry_hash, is_block_node, is_entry_node},
    slot_ranges::{
        SLOTS_PER_EPOCH, SlotRange, SlotRangeWithPreviousBlockhash, epoch_for_slot, slot_in_epoch,
        write_slot_ranges_raw, write_slot_ranges_v2_raw,
    },
};
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Instant,
};

const DEFAULT_BUFFER_MIB: usize = 64;
const NODE_KIND_PREFIX_LEN: usize = 2;
const BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";

#[derive(Debug, Parser)]
#[command(name = "of-car-slot-index")]
#[command(about = "Build Old Faithful slot range indexes by streaming .car or .car.zst files")]
struct Cli {
    /// CAR file(s) or directory roots containing epoch-N.car / epoch-N.car.zst.
    #[arg(required = true)]
    inputs: Vec<PathBuf>,

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
    jobs: usize,
    prefer_zst: bool,
    no_v2: bool,
    require_seed: bool,
    overwrite: bool,
    buffer_bytes: usize,
}

#[derive(Debug, Clone)]
struct EpochInput {
    epoch: u64,
    path: PathBuf,
}

#[derive(Debug)]
struct BuildSummary {
    epoch: u64,
    path: PathBuf,
    raw_out: PathBuf,
    v2_out: Option<PathBuf>,
    registry_out: PathBuf,
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

    let config = Arc::new(Config {
        output_dir,
        blockhash_dir,
        seed_blockhash_dir,
        seed_previous_blockhash,
        jobs: cli.jobs.max(1),
        prefer_zst: cli.prefer_zst,
        no_v2: cli.no_v2,
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
    fs::create_dir_all(&config.blockhash_dir)
        .with_context(|| format!("create {}", config.blockhash_dir.display()))?;

    let jobs = config.jobs.min(inputs.len()).max(1);
    eprintln!(
        "of-car-slot-index: epochs={} jobs={} output={} blockhash_dir={} no_v2={}",
        inputs.len(),
        jobs,
        config.output_dir.display(),
        config.blockhash_dir.display(),
        config.no_v2
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
    inputs: &[PathBuf],
    config: &Config,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
) -> Result<Vec<EpochInput>> {
    let mut by_epoch = BTreeMap::<u64, PathBuf>::new();
    for input in inputs {
        if input.is_dir() {
            for entry in fs::read_dir(input).with_context(|| format!("read {}", input.display()))? {
                let path = entry?.path();
                add_input_path(&mut by_epoch, path, config, start_epoch, end_epoch)?;
            }
        } else {
            add_input_path(&mut by_epoch, input.clone(), config, start_epoch, end_epoch)?;
        }
    }

    Ok(by_epoch
        .into_iter()
        .map(|(epoch, path)| EpochInput { epoch, path })
        .collect())
}

fn add_input_path(
    by_epoch: &mut BTreeMap<u64, PathBuf>,
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
    if start_epoch.is_some_and(|start| epoch < start) || end_epoch.is_some_and(|end| epoch > end) {
        return Ok(());
    }

    match by_epoch.get(&epoch) {
        Some(existing) if prefer_candidate(existing, &path, config.prefer_zst) => {
            by_epoch.insert(epoch, path);
        }
        None => {
            by_epoch.insert(epoch, path);
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
        && paths.registry_out.is_file()
        && (config.no_v2 || paths.v2_out.is_file())
    {
        return Ok(BuildSummary {
            epoch: input.epoch,
            path: input.path,
            raw_out: paths.raw_out,
            v2_out: (!config.no_v2).then_some(paths.v2_out),
            registry_out: paths.registry_out,
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

    let scan = if is_zstd_path(&input.path) {
        let file =
            File::open(&input.path).with_context(|| format!("open {}", input.path.display()))?;
        let file = BufReader::with_capacity(config.buffer_bytes, file);
        let decoder = zstd::Decoder::with_buffer(file)
            .with_context(|| format!("open zstd {}", input.path.display()))?;
        scan_car_reader(
            decoder,
            input.epoch,
            previous_blockhash,
            config.buffer_bytes,
        )
    } else {
        let file =
            File::open(&input.path).with_context(|| format!("open {}", input.path.display()))?;
        scan_car_reader(file, input.epoch, previous_blockhash, config.buffer_bytes)
    }
    .with_context(|| format!("scan {}", input.path.display()))?;

    write_raw_atomic(&paths.raw_out, &scan.raw_ranges)?;
    write_registry_atomic(&paths.registry_out, &scan.blockhashes)?;
    if !config.no_v2 {
        write_v2_atomic(&paths.v2_out, &scan.v2_ranges)?;
    }

    Ok(BuildSummary {
        epoch: input.epoch,
        path: input.path,
        raw_out: paths.raw_out,
        v2_out: (!config.no_v2).then_some(paths.v2_out),
        registry_out: paths.registry_out,
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
) -> Result<ScanOutput> {
    let mut reader = CarBlockReader::with_capacity(reader, buffer_bytes);
    reader.skip_header().context("read CAR header")?;

    let mut raw_ranges = vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut v2_ranges = vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut blockhashes = Vec::with_capacity(SLOTS_PER_EPOCH as usize);
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
            let hash = decode_entry_hash(payload)
                .with_context(|| format!("decode entry hash at {}", entry.location.car_offset))?;
            if hash.len() != 32 {
                return Err(anyhow!(
                    "entry hash length {} at CAR offset {}",
                    hash.len(),
                    entry.location.car_offset
                ));
            }
            pending_blockhash.copy_from_slice(hash);
            pending_has_blockhash = true;
            entry_nodes += 1;
            continue;
        }

        if !is_block_node(entry.prefix) {
            continue;
        }

        let (slot, _meta) = decode_block_metadata(payload)
            .with_context(|| format!("decode block metadata at {}", entry.location.car_offset))?;
        blocks += 1;
        if !pending_has_blockhash {
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
            let previous = previous_blockhash
                .or_else(|| (slot == 0).then_some(blockhash))
                .unwrap_or([0; 32]);
            raw_ranges[idx] = range;
            v2_ranges[idx] = SlotRangeWithPreviousBlockhash {
                range,
                previous_blockhash: previous,
            };
            blockhashes.push(blockhash);
            present_slots += 1;
            first_slot.get_or_insert(slot);
            last_slot = Some(slot);
        } else {
            out_of_epoch_blocks += 1;
        }

        previous_blockhash = Some(blockhash);
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
            summary.registry_out.display()
        );
        return;
    }

    eprintln!(
        "epoch={}: built from {} entries={} entry_nodes={} blocks={} present_slots={} first_slot={} last_slot={} out_of_epoch_blocks={} elapsed_s={:.2} raw={} v2={} registry={}",
        summary.epoch,
        summary.path.display(),
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
        summary.registry_out.display(),
    );
}

fn prefer_candidate(existing: &Path, candidate: &Path, prefer_zst: bool) -> bool {
    let existing_zst = is_zstd_path(existing);
    let candidate_zst = is_zstd_path(candidate);
    if prefer_zst {
        !existing_zst && candidate_zst
    } else {
        existing_zst && !candidate_zst
    }
}

fn epoch_from_path(path: &Path) -> Option<u64> {
    let filename = path.file_name()?.to_str()?;
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

fn is_zstd_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("zst"))
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
