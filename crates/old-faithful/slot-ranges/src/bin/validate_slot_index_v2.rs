use anyhow::{Context, Result, anyhow, bail};
use of_car_reader::{
    compact_index::decode_offset_and_size,
    slot_ranges::{
        SLOT_RANGE_ENTRY_SIZE, SLOT_RANGE_V2_ENTRY_SIZE, SLOTS_PER_EPOCH, SlotRange,
        decode_slot_range_entry, decode_slot_range_v2_entry,
    },
};
use of_slot_ranges::{
    AsyncCompactIndex, BlockSlotCandidate, BuildSlotRangesConfig, LocalFileRangeReader,
    RangeReader, build_block_slot_candidates_from_slot_index,
    build_slot_ranges_from_indexes_with_block_slots, decode_block_slot_from_car_frame,
    decode_car_header_total_size,
};
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_RANGE, HeaderValue, RANGE};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    ffi::{OsStr, OsString},
    fs,
    future::{Ready, ready},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::ExitCode,
};

const BLOCKHASH_BYTES: usize = 32;
const BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";
const ARCHIVE_V2_BLOCK_INDEX_FILE: &str = "archive-v2-blocks.index";
const ARCHIVE_V2_LEGACY_INDEX_MAGIC: &[u8; 8] = b"BZV2IDX1";
const ARCHIVE_V2_LEGACY_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8;
const ARCHIVE_V2_LEGACY_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 8 + 4 + 4;
const ARCHIVE_V2_HOT_INDEX_MAGIC: &[u8; 8] = b"BZV2HIX1";
const ARCHIVE_V2_HOT_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_HOT_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8 + 4 + 4;
const ARCHIVE_V2_HOT_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 4 + 4 + 4 + 8 + 8 + 4;
const MAX_BUCKET_SIZE: usize = 64 * 1024 * 1024;
const MAINNET_GENESIS_HASH_BASE58: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
const DEFAULT_BASE_URL: &str = "https://files.old-faithful.net";

#[derive(Debug)]
struct Cli {
    index_dir: PathBuf,
    blockhash_dir: PathBuf,
    indexes_dir: PathBuf,
    cars_dir: Option<PathBuf>,
    base_url: String,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
    seed_previous_blockhash: Option<[u8; 32]>,
    reuse_raw: bool,
    mode: ValidationMode,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ValidationMode {
    OldFaithfulIndex,
    ArchiveV2,
    RegistryOnly,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct ArchiveV2BlockIndexRow {
    block_id: u32,
    slot: u64,
}

#[derive(Debug)]
struct EpochSidecars {
    rows: Vec<ArchiveV2BlockIndexRow>,
    blockhashes: Vec<[u8; 32]>,
    registry_offset: usize,
    canonical_ranges: Option<Vec<SlotRange>>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct EpochSummary {
    epoch: u64,
    indexed_blocks: usize,
    present_slots: usize,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    registry_records: usize,
    registry_offset: usize,
    last_blockhash: Option<[u8; 32]>,
}

fn main() -> ExitCode {
    match parse_cli().and_then(run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("slot-index-v2 validation failed: {error:#}");
            ExitCode::from(1)
        }
    }
}

fn usage() -> &'static str {
    "usage: of-validate-slot-index-v2 <slot-index-dir> <blockhash-dir> [--indexes-dir DIR] [--cars-dir DIR] [--base-url URL] [--start-epoch N] [--end-epoch N] [--seed-previous-blockhash BASE58] [--reuse-raw] [--archive-v2|--registry-only]"
}

fn parse_cli() -> Result<Cli> {
    parse_args(env::args_os().skip(1))
}

fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Cli> {
    let mut arguments = arguments.into_iter();
    let index_dir = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!(usage()))?;
    let blockhash_dir = arguments
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!(usage()))?;
    let mut start_epoch = None;
    let mut end_epoch = None;
    let mut indexes_dir = PathBuf::from("indexes");
    let mut indexes_dir_set = false;
    let mut cars_dir = None;
    let mut base_url = DEFAULT_BASE_URL.to_string();
    let mut base_url_set = false;
    let mut seed_previous_blockhash = None;
    let mut archive_v2 = false;
    let mut registry_only = false;
    let mut reuse_raw = false;
    while let Some(argument) = arguments.next() {
        let argument = argument
            .to_str()
            .ok_or_else(|| anyhow!("argument is not valid UTF-8"))?;
        let target = match argument {
            "--start-epoch" => Some(&mut start_epoch),
            "--end-epoch" => Some(&mut end_epoch),
            "--indexes-dir" => {
                if indexes_dir_set {
                    bail!("duplicate argument --indexes-dir");
                }
                let value = arguments
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --indexes-dir"))?;
                indexes_dir = PathBuf::from(value);
                indexes_dir_set = true;
                None
            }
            "--cars-dir" => {
                if cars_dir.is_some() {
                    bail!("duplicate argument --cars-dir");
                }
                let value = arguments
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --cars-dir"))?;
                cars_dir = Some(PathBuf::from(value));
                None
            }
            "--base-url" => {
                if base_url_set {
                    bail!("duplicate argument --base-url");
                }
                let value = arguments
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --base-url"))?;
                base_url = value
                    .to_str()
                    .ok_or_else(|| anyhow!("--base-url value is not valid UTF-8"))?
                    .trim_end_matches('/')
                    .to_string();
                if base_url.is_empty() {
                    bail!("--base-url must not be empty");
                }
                base_url_set = true;
                None
            }
            "--seed-previous-blockhash" => {
                if seed_previous_blockhash.is_some() {
                    bail!("duplicate argument --seed-previous-blockhash");
                }
                let value = arguments
                    .next()
                    .ok_or_else(|| anyhow!("missing value for --seed-previous-blockhash"))?;
                let value = value
                    .to_str()
                    .ok_or_else(|| anyhow!("--seed-previous-blockhash value is not valid UTF-8"))?;
                seed_previous_blockhash = Some(decode_base58_hash(value)?);
                None
            }
            "--archive-v2" => {
                if archive_v2 {
                    bail!("duplicate argument --archive-v2");
                }
                archive_v2 = true;
                None
            }
            "--registry-only" => {
                if registry_only {
                    bail!("duplicate argument --registry-only");
                }
                registry_only = true;
                None
            }
            "--reuse-raw" => {
                if reuse_raw {
                    bail!("duplicate argument --reuse-raw");
                }
                reuse_raw = true;
                None
            }
            "-h" | "--help" => bail!(usage()),
            _ => bail!("unknown argument {argument:?}; {}", usage()),
        };
        let Some(target) = target else {
            continue;
        };
        if target.is_some() {
            bail!("duplicate argument {argument}");
        }
        let value = arguments
            .next()
            .ok_or_else(|| anyhow!("missing value for {argument}"))?;
        let value = value
            .to_str()
            .ok_or_else(|| anyhow!("{argument} value is not valid UTF-8"))?
            .parse::<u64>()
            .with_context(|| format!("parse {argument}"))?;
        *target = Some(value);
    }
    if start_epoch
        .zip(end_epoch)
        .is_some_and(|(start, end)| start > end)
    {
        bail!("start epoch is greater than end epoch");
    }
    if archive_v2 && registry_only {
        bail!("--archive-v2 conflicts with --registry-only");
    }
    if reuse_raw && (archive_v2 || registry_only) {
        bail!("--reuse-raw is available only in normal Old Faithful index mode");
    }
    Ok(Cli {
        index_dir,
        blockhash_dir,
        indexes_dir,
        cars_dir,
        base_url,
        start_epoch,
        end_epoch,
        seed_previous_blockhash,
        reuse_raw,
        mode: if archive_v2 {
            ValidationMode::ArchiveV2
        } else if registry_only {
            ValidationMode::RegistryOnly
        } else {
            ValidationMode::OldFaithfulIndex
        },
    })
}

fn run(cli: Cli) -> Result<()> {
    if !cli.index_dir.is_dir() {
        bail!("missing slot index directory: {}", cli.index_dir.display());
    }
    if !cli.blockhash_dir.is_dir() {
        bail!(
            "missing blockhash directory: {}",
            cli.blockhash_dir.display()
        );
    }
    if cli.mode == ValidationMode::OldFaithfulIndex && !cli.indexes_dir.is_dir() {
        bail!(
            "missing Old Faithful compact indexes directory: {}",
            cli.indexes_dir.display()
        );
    }
    let http = Client::builder()
        .user_agent("of-validate-slot-index-v2/1.0")
        .build()
        .context("build HTTP client")?;
    let discovered = discover_v2_indexes(&cli.index_dir)?;
    if discovered.is_empty() {
        bail!(
            "no epoch-*-slot-ranges-v2.raw files in {}",
            cli.index_dir.display()
        );
    }
    let selected = select_epochs(discovered, cli.start_epoch, cli.end_epoch)?;

    let mut total_present_slots = 0usize;
    let mut total_indexed_blocks = 0usize;
    let mut prior_validated: Option<(u64, Option<[u8; 32]>)> = None;
    for (epoch, v2_path) in &selected {
        let predecessor = select_predecessor_blockhash(
            *epoch,
            prior_validated,
            cli.seed_previous_blockhash,
            |predecessor_epoch| {
                read_epoch_last_blockhash(
                    &cli.blockhash_dir,
                    &cli.indexes_dir,
                    predecessor_epoch,
                    cli.mode,
                    &http,
                    cli.cars_dir.as_deref(),
                    &cli.base_url,
                )
            },
        )?;
        let summary = validate_epoch(
            *epoch,
            v2_path,
            &cli.index_dir,
            &cli.blockhash_dir,
            &cli.indexes_dir,
            cli.mode,
            predecessor,
            &http,
            cli.cars_dir.as_deref(),
            &cli.base_url,
            cli.reuse_raw,
        )?;
        total_present_slots = total_present_slots
            .checked_add(summary.present_slots)
            .ok_or_else(|| anyhow!("present slot count overflow"))?;
        total_indexed_blocks = total_indexed_blocks
            .checked_add(summary.indexed_blocks)
            .ok_or_else(|| anyhow!("indexed block count overflow"))?;
        println!(
            "epoch={} mode={} range_proof={} indexed_blocks={} present_slots={} missing_ranges={} first_slot={} last_slot={} registry_records={} registry_offset={}",
            summary.epoch,
            cli.mode.label(),
            cli.mode.range_proof_label(cli.reuse_raw),
            summary.indexed_blocks,
            summary.present_slots,
            summary.indexed_blocks - summary.present_slots,
            display_optional_slot(summary.first_slot),
            display_optional_slot(summary.last_slot),
            summary.registry_records,
            summary.registry_offset,
        );
        prior_validated = Some((*epoch, summary.last_blockhash));
    }
    println!(
        "validated_epochs={} total_indexed_blocks={total_indexed_blocks} total_present_slots={total_present_slots}",
        selected.len()
    );
    Ok(())
}

fn select_predecessor_blockhash(
    epoch: u64,
    prior_validated: Option<(u64, Option<[u8; 32]>)>,
    first_selected_seed: Option<[u8; 32]>,
    mut read_epoch_last: impl FnMut(u64) -> Result<[u8; 32]>,
) -> Result<Option<[u8; 32]>> {
    if epoch == 0 {
        return Ok(first_selected_seed);
    }
    if let Some((prior_epoch, prior_last)) = prior_validated {
        if prior_epoch + 1 == epoch {
            return Ok(Some(prior_last.ok_or_else(|| {
                anyhow!("epoch {prior_epoch} block index has no last blockhash")
            })?));
        }
        return read_epoch_last(epoch - 1).map(Some);
    }
    match first_selected_seed {
        Some(seed) => Ok(Some(seed)),
        None => read_epoch_last(epoch - 1).map(Some),
    }
}

impl ValidationMode {
    fn label(self) -> &'static str {
        match self {
            Self::OldFaithfulIndex => "old-faithful-index",
            Self::ArchiveV2 => "archive-v2",
            Self::RegistryOnly => "registry-only",
        }
    }

    fn range_proof_label(self, reuse_raw: bool) -> &'static str {
        match self {
            Self::OldFaithfulIndex if reuse_raw => "reused-raw",
            Self::OldFaithfulIndex => "canonical-cid-index",
            Self::ArchiveV2 => "archive-v2-structure",
            Self::RegistryOnly => "registry-only-structure",
        }
    }
}

fn discover_v2_indexes(root: &Path) -> Result<BTreeMap<u64, PathBuf>> {
    let mut indexes = BTreeMap::new();
    for entry in root
        .read_dir()
        .with_context(|| format!("read {}", root.display()))?
    {
        let entry = entry.with_context(|| format!("read entry in {}", root.display()))?;
        let Some(epoch) = epoch_from_v2_name(&entry.file_name()) else {
            continue;
        };
        let path = entry.path();
        let metadata = path
            .symlink_metadata()
            .with_context(|| format!("inspect {}", path.display()))?;
        if !metadata.is_file() {
            bail!("v2 index is not a regular file: {}", path.display());
        }
        if indexes.insert(epoch, path.clone()).is_some() {
            bail!("duplicate v2 index for epoch {epoch}: {}", path.display());
        }
    }
    Ok(indexes)
}

fn select_epochs(
    discovered: BTreeMap<u64, PathBuf>,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
) -> Result<BTreeMap<u64, PathBuf>> {
    let selected = discovered
        .into_iter()
        .filter(|(epoch, _)| {
            start_epoch.is_none_or(|start| *epoch >= start)
                && end_epoch.is_none_or(|end| *epoch <= end)
        })
        .collect::<BTreeMap<_, _>>();
    if selected.is_empty() {
        bail!("no v2 indexes in the requested epoch range");
    }
    if let (Some(start), Some(end)) = (start_epoch, end_epoch) {
        for epoch in start..=end {
            if !selected.contains_key(&epoch) {
                bail!("missing v2 index for requested epoch {epoch}");
            }
        }
    }
    Ok(selected)
}

fn validate_epoch(
    epoch: u64,
    v2_path: &Path,
    index_dir: &Path,
    blockhash_root: &Path,
    indexes_dir: &Path,
    mode: ValidationMode,
    predecessor: Option<[u8; 32]>,
    http: &Client,
    cars_dir: Option<&Path>,
    base_url: &str,
    reuse_raw: bool,
) -> Result<EpochSummary> {
    let v2_bytes = read_exact_size(
        v2_path,
        SLOTS_PER_EPOCH as usize * SLOT_RANGE_V2_ENTRY_SIZE,
        "v2 slot index",
    )?;
    let raw_path = index_dir.join(format!("epoch-{epoch}-slot-ranges.raw"));
    let raw_bytes = read_exact_size(
        &raw_path,
        SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE,
        "raw slot index",
    )?;
    let sidecars = match mode {
        ValidationMode::OldFaithfulIndex => read_old_faithful_index_sidecars(
            blockhash_root,
            indexes_dir,
            epoch,
            http,
            cars_dir,
            base_url,
            !reuse_raw,
        )?,
        ValidationMode::ArchiveV2 => read_epoch_sidecars(blockhash_root, epoch)?,
        ValidationMode::RegistryOnly => {
            read_registry_only_sidecars(blockhash_root, epoch, &raw_bytes)?
        }
    };
    validate_epoch_bytes(epoch, &v2_bytes, &raw_bytes, &sidecars, predecessor)
        .with_context(|| format!("validate epoch {epoch} from {}", v2_path.display()))
}

fn validate_epoch_bytes(
    epoch: u64,
    v2_bytes: &[u8],
    raw_bytes: &[u8],
    sidecars: &EpochSidecars,
    predecessor: Option<[u8; 32]>,
) -> Result<EpochSummary> {
    let expected_v2_len = SLOTS_PER_EPOCH as usize * SLOT_RANGE_V2_ENTRY_SIZE;
    if v2_bytes.len() != expected_v2_len {
        bail!(
            "v2 index has {} bytes, expected {expected_v2_len} ({} rows of {SLOT_RANGE_V2_ENTRY_SIZE} bytes)",
            v2_bytes.len(),
            SLOTS_PER_EPOCH
        );
    }
    let expected_raw_len = SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE;
    if raw_bytes.len() != expected_raw_len {
        bail!(
            "raw index has {} bytes, expected {expected_raw_len} ({} rows of {SLOT_RANGE_ENTRY_SIZE} bytes)",
            raw_bytes.len(),
            SLOTS_PER_EPOCH
        );
    }
    if let Some(canonical_ranges) = &sidecars.canonical_ranges {
        if canonical_ranges.len() != SLOTS_PER_EPOCH as usize {
            bail!(
                "canonical range list has {} rows, expected {}",
                canonical_ranges.len(),
                SLOTS_PER_EPOCH
            );
        }
        let epoch_start = epoch
            .checked_mul(SLOTS_PER_EPOCH)
            .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
        for (slot_in_epoch, (raw_row, expected)) in raw_bytes
            .chunks_exact(SLOT_RANGE_ENTRY_SIZE)
            .zip(canonical_ranges)
            .enumerate()
        {
            let actual = decode_slot_range_entry(raw_row)?;
            if actual != *expected {
                bail!(
                    "slot {} raw range offset={} len={} differs from canonical CID-index range offset={} len={}",
                    epoch_start + slot_in_epoch as u64,
                    actual.offset,
                    actual.len,
                    expected.offset,
                    expected.len
                );
            }
        }
    }
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let mut block_at_slot = vec![None; SLOTS_PER_EPOCH as usize];
    for (position, row) in sidecars.rows.iter().enumerate() {
        let slot_in_epoch = usize::try_from(row.slot - epoch_start)
            .context("slot-in-epoch exceeds address space")?;
        if block_at_slot[slot_in_epoch].replace(position).is_some() {
            bail!(
                "ordered block slot list contains duplicate slot {}",
                row.slot
            );
        }
    }

    let mut present_slots = 0usize;
    let mut first_slot = None;
    let mut last_slot = None;
    let mut previous_range_end = None;
    for (slot_in_epoch, (v2_row, raw_row)) in v2_bytes
        .chunks_exact(SLOT_RANGE_V2_ENTRY_SIZE)
        .zip(raw_bytes.chunks_exact(SLOT_RANGE_ENTRY_SIZE))
        .enumerate()
    {
        let slot = epoch_start
            .checked_add(slot_in_epoch as u64)
            .ok_or_else(|| anyhow!("slot overflow"))?;
        if &v2_row[..SLOT_RANGE_ENTRY_SIZE] != raw_row {
            bail!("slot {slot} range differs between v2 and raw indexes");
        }
        let entry = decode_slot_range_v2_entry(v2_row)?;
        let raw_entry = decode_slot_range_entry(raw_row)?;
        debug_assert_eq!(entry.range, raw_entry);
        let indexed = block_at_slot[slot_in_epoch].is_some();
        if entry.range.is_empty() {
            if entry.range.offset != 0 {
                bail!(
                    "empty slot {slot} has nonzero offset {}",
                    entry.range.offset
                );
            }
            if !indexed && entry.previous_blockhash != [0; 32] {
                bail!(
                    "slot {slot} is absent from the ordered block slot list but has a previous blockhash"
                );
            }
            continue;
        }
        if !indexed {
            bail!("raw-present slot {slot} is absent from the ordered block slot list");
        }
        if entry.range.offset == 0 {
            bail!("present slot {slot} has zero CAR offset");
        }
        let range_end = entry.range.end_exclusive().ok_or_else(|| {
            anyhow!(
                "slot {slot} CAR range overflows: offset={} len={}",
                entry.range.offset,
                entry.range.len
            )
        })?;
        if previous_range_end.is_some_and(|prior_end| entry.range.offset < prior_end) {
            bail!(
                "slot {slot} CAR range starts at {} before the prior present range ends at {}",
                entry.range.offset,
                previous_range_end.unwrap()
            );
        }
        present_slots += 1;
        first_slot.get_or_insert(slot);
        last_slot = Some(slot);
        previous_range_end = Some(range_end);
    }

    let boundary_previous = registry_boundary_previous(
        epoch,
        sidecars.registry_offset,
        &sidecars.blockhashes,
        predecessor,
    )?;
    for (position, row) in sidecars.rows.iter().enumerate() {
        let slot_in_epoch = usize::try_from(row.slot - epoch_start)
            .context("slot-in-epoch exceeds address space")?;
        let row_start = slot_in_epoch
            .checked_mul(SLOT_RANGE_V2_ENTRY_SIZE)
            .ok_or_else(|| anyhow!("v2 row offset overflow for slot {}", row.slot))?;
        let entry =
            decode_slot_range_v2_entry(&v2_bytes[row_start..row_start + SLOT_RANGE_V2_ENTRY_SIZE])?;
        let hash_index = sidecars
            .registry_offset
            .checked_add(position)
            .ok_or_else(|| anyhow!("blockhash registry index overflow"))?;
        let expected_previous = if position == 0 {
            boundary_previous
        } else {
            sidecars.blockhashes[hash_index - 1]
        };
        if row.slot != 0 && entry.previous_blockhash == [0; 32] {
            bail!(
                "indexed non-genesis slot {} has a zero previous blockhash",
                row.slot
            );
        }
        if entry.previous_blockhash != expected_previous {
            bail!(
                "slot {} previous blockhash {} differs from prior ordered blockhash {}",
                row.slot,
                hex32(entry.previous_blockhash),
                hex32(expected_previous)
            );
        }
    }

    let last_blockhash = sidecars
        .rows
        .last()
        .map(|_| sidecars.blockhashes[sidecars.registry_offset + sidecars.rows.len() - 1]);
    Ok(EpochSummary {
        epoch,
        indexed_blocks: sidecars.rows.len(),
        present_slots,
        first_slot,
        last_slot,
        registry_records: sidecars.blockhashes.len(),
        registry_offset: sidecars.registry_offset,
        last_blockhash,
    })
}

fn registry_boundary_previous(
    epoch: u64,
    registry_offset: usize,
    blockhashes: &[[u8; 32]],
    explicit_seed: Option<[u8; 32]>,
) -> Result<[u8; 32]> {
    if epoch > 0 {
        if registry_offset != 0 {
            bail!("only epoch 0 can contain a genesis-prefixed blockhash registry");
        }
        let predecessor = explicit_seed.ok_or_else(|| {
            anyhow!(
                "epoch {epoch} needs the last blockhash from epoch {}",
                epoch - 1
            )
        })?;
        if predecessor == [0; 32] {
            bail!("epoch {} predecessor blockhash is zero", epoch - 1);
        }
        return Ok(predecessor);
    }

    match registry_offset {
        0 => {
            let seed = explicit_seed.ok_or_else(|| {
                anyhow!("epoch 0 needs a genesis-prefixed registry or --seed-previous-blockhash")
            })?;
            if seed == [0; 32] {
                bail!("epoch 0 genesis seed is zero");
            }
            Ok(seed)
        }
        1 => {
            let registry_genesis = blockhashes[0];
            let expected_genesis = explicit_seed.unwrap_or(mainnet_genesis_hash()?);
            if registry_genesis != expected_genesis {
                bail!(
                    "epoch 0 blockhash registry genesis prefix does not match {}",
                    if explicit_seed.is_some() {
                        "--seed-previous-blockhash"
                    } else {
                        "the mainnet genesis hash"
                    }
                );
            }
            Ok(registry_genesis)
        }
        offset => bail!("epoch 0 blockhash registry has invalid offset {offset}"),
    }
}

fn read_epoch_sidecars(root: &Path, epoch: u64) -> Result<EpochSidecars> {
    let epoch_dir = archive_v2_epoch_dir(root, epoch)?;
    let rows = read_archive_v2_block_index_rows(&epoch_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    let blockhashes = read_blockhash_registry(&epoch_dir.join(BLOCKHASH_REGISTRY_FILE))?;
    validate_sidecar_parts(epoch, rows, blockhashes)
        .with_context(|| format!("validate Archive V2 sidecars in {}", epoch_dir.display()))
}

fn read_old_faithful_index_sidecars(
    blockhash_root: &Path,
    indexes_dir: &Path,
    epoch: u64,
    http: &Client,
    cars_dir: Option<&Path>,
    base_url: &str,
    prove_canonical_ranges: bool,
) -> Result<EpochSidecars> {
    let (candidates, cid_index_path) = read_old_faithful_block_candidates(indexes_dir, epoch)?;
    let block_slots = resolve_old_faithful_block_slots(
        epoch,
        &candidates,
        &cid_index_path,
        http,
        cars_dir,
        base_url,
    )?;
    let canonical_ranges = prove_canonical_ranges
        .then(|| {
            build_canonical_old_faithful_ranges(
                epoch,
                indexes_dir,
                &block_slots,
                http,
                cars_dir,
                base_url,
            )
        })
        .transpose()?;
    if let Some(ranges) = &canonical_ranges {
        let canonical_present = ranges.iter().filter(|range| !range.is_empty()).count();
        eprintln!(
            "epoch={epoch}: resolved_cid_groups={} canonical_nonempty_ranges={} canonical_empty_blocks={}",
            block_slots.len(),
            canonical_present,
            block_slots.len() - canonical_present
        );
    }
    let rows = block_slots
        .into_iter()
        .enumerate()
        .map(|(block_id, slot)| {
            Ok(ArchiveV2BlockIndexRow {
                block_id: u32::try_from(block_id).context("block count exceeds u32")?,
                slot,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let epoch_dir = registry_only_epoch_dir(blockhash_root, epoch)?;
    let blockhashes = read_blockhash_registry(&epoch_dir.join(BLOCKHASH_REGISTRY_FILE))?;
    let mut sidecars = validate_sidecar_parts(epoch, rows, blockhashes).with_context(|| {
        format!(
            "validate Old Faithful slot order against registry in {}",
            epoch_dir.display()
        )
    })?;
    sidecars.canonical_ranges = canonical_ranges;
    Ok(sidecars)
}

fn read_old_faithful_block_candidates(
    indexes_dir: &Path,
    epoch: u64,
) -> Result<(Vec<BlockSlotCandidate>, PathBuf)> {
    let epoch_dir = indexes_dir.join(epoch.to_string());
    let cid_path = epoch_dir.join(format!("epoch-{epoch}.cid"));
    let epoch_cid =
        fs::read_to_string(&cid_path).with_context(|| format!("read {}", cid_path.display()))?;
    let slot_index_path = epoch_dir.join(format!(
        "epoch-{epoch}-{}-mainnet-slot-to-cid.index",
        epoch_cid.trim()
    ));
    let cid_index_path = epoch_dir.join(format!(
        "epoch-{epoch}-{}-mainnet-cid-to-offset-and-size.index",
        epoch_cid.trim()
    ));
    let reader = LocalFileRangeReader::open(&slot_index_path)?;
    let mut slot_index = futures::executor::block_on(AsyncCompactIndex::open(
        reader,
        slot_index_path.display().to_string(),
    ))?;
    let output = futures::executor::block_on(build_block_slot_candidates_from_slot_index(
        epoch,
        &mut slot_index,
        BuildSlotRangesConfig {
            max_bucket_payload_bytes: MAX_BUCKET_SIZE,
            allow_node_read_fallback: true,
        },
    ))?;
    Ok((output.candidates, cid_index_path))
}

fn resolve_old_faithful_block_slots(
    epoch: u64,
    candidates: &[BlockSlotCandidate],
    cid_index_path: &Path,
    http: &Client,
    cars_dir: Option<&Path>,
    base_url: &str,
) -> Result<Vec<u64>> {
    let mut groups: HashMap<[u8; 36], Vec<&BlockSlotCandidate>> = HashMap::new();
    for candidate in candidates {
        groups.entry(candidate.cid).or_default().push(candidate);
    }

    let mut cid_index = if groups.values().any(|group| group.len() > 1) {
        let (reader, source) = if cid_index_path.is_file() {
            (
                IndexRangeReader::Local(LocalFileRangeReader::open(cid_index_path)?),
                cid_index_path.display().to_string(),
            )
        } else {
            let file_name = cid_index_path
                .file_name()
                .and_then(OsStr::to_str)
                .ok_or_else(|| anyhow!("CID index path has no UTF-8 file name"))?;
            let url = format!("{base_url}/{epoch}/{file_name}");
            (
                IndexRangeReader::Http(HttpRangeReader::new(http.clone(), url.clone())),
                url,
            )
        };
        let index = futures::executor::block_on(AsyncCompactIndex::open(reader, source.clone()))
            .with_context(|| format!("open {source}"))?;
        if index.value_size() != 9 {
            bail!("{source} has value_size {}, expected 9", index.value_size());
        }
        Some(index)
    } else {
        None
    };

    let mut selected = Vec::with_capacity(groups.len());
    for group in groups.values_mut() {
        group.sort_unstable_by_key(|candidate| candidate.slot);
        if group.len() == 1 {
            selected.push(group[0].slot);
            continue;
        }

        let index = cid_index
            .as_mut()
            .expect("duplicate groups create a CID index reader");
        let mut value = [0u8; 9];
        let found =
            futures::executor::block_on(index.lookup_into_node_reads(&group[0].cid, &mut value))?;
        if !found {
            bail!(
                "CID index has no exact entry for duplicate CID at candidate slots {}",
                display_candidate_slots(group)
            );
        }
        let (offset, size) = decode_offset_and_size(&value)?;
        let bytes = read_car_range(http, epoch, cars_dir, base_url, offset, u64::from(size))?;
        let decoded_slot = decode_block_slot_from_car_frame(&group[0].cid, &bytes)
            .context("verify exact CID-index CAR frame")?;
        let evidence = "exact CID-index CAR frame";

        let matches = group
            .iter()
            .filter(|candidate| candidate.slot == decoded_slot)
            .count();
        if matches != 1 {
            bail!(
                "{evidence} decodes block slot {decoded_slot}, which is not exactly one of candidate slots {}",
                display_candidate_slots(group)
            );
        }
        eprintln!(
            "epoch={epoch}: resolved duplicate CID candidate slots {} to slot {decoded_slot} using {evidence}",
            display_candidate_slots(group)
        );
        selected.push(decoded_slot);
    }
    selected.sort_unstable();
    Ok(selected)
}

fn display_candidate_slots(candidates: &[&BlockSlotCandidate]) -> String {
    candidates
        .iter()
        .map(|candidate| candidate.slot.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_canonical_old_faithful_ranges(
    epoch: u64,
    indexes_dir: &Path,
    block_slots: &[u64],
    http: &Client,
    cars_dir: Option<&Path>,
    base_url: &str,
) -> Result<Vec<SlotRange>> {
    let epoch_dir = indexes_dir.join(epoch.to_string());
    let cid_path = epoch_dir.join(format!("epoch-{epoch}.cid"));
    let epoch_cid =
        fs::read_to_string(&cid_path).with_context(|| format!("read {}", cid_path.display()))?;
    let slot_index_path = epoch_dir.join(format!(
        "epoch-{epoch}-{}-mainnet-slot-to-cid.index",
        epoch_cid.trim()
    ));
    let cid_index_path = epoch_dir.join(format!(
        "epoch-{epoch}-{}-mainnet-cid-to-offset-and-size.index",
        epoch_cid.trim()
    ));

    let slot_reader = LocalFileRangeReader::open(&slot_index_path)?;
    let mut slot_index = futures::executor::block_on(AsyncCompactIndex::open(
        slot_reader,
        slot_index_path.display().to_string(),
    ))?;
    let (cid_reader, cid_source) = if cid_index_path.is_file() {
        (
            IndexRangeReader::Local(LocalFileRangeReader::open(&cid_index_path)?),
            cid_index_path.display().to_string(),
        )
    } else {
        let file_name = cid_index_path
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or_else(|| anyhow!("CID index path has no UTF-8 file name"))?;
        let url = format!("{base_url}/{epoch}/{file_name}");
        (
            IndexRangeReader::Http(HttpRangeReader::new(http.clone(), url.clone())),
            url,
        )
    };
    let mut cid_index =
        futures::executor::block_on(AsyncCompactIndex::open(cid_reader, cid_source.clone()))
            .with_context(|| format!("open {cid_source}"))?;
    let prefix = read_car_range(http, epoch, cars_dir, base_url, 0, 16)?;
    let car_header_size = decode_car_header_total_size(&prefix, "Old Faithful CAR")?;
    let output = futures::executor::block_on(build_slot_ranges_from_indexes_with_block_slots(
        epoch,
        car_header_size,
        &mut slot_index,
        &mut cid_index,
        BuildSlotRangesConfig {
            max_bucket_payload_bytes: MAX_BUCKET_SIZE,
            allow_node_read_fallback: true,
        },
        block_slots,
    ))?;
    if output.block_slots != block_slots {
        bail!("canonical range builder changed the resolved block slot order");
    }
    Ok(output.ranges)
}

enum IndexRangeReader {
    Local(LocalFileRangeReader),
    Http(HttpRangeReader),
}

impl RangeReader for IndexRangeReader {
    type ReadFuture<'a>
        = Ready<Result<()>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        match self {
            Self::Local(reader) => reader.read_exact_at(offset, out),
            Self::Http(reader) => reader.read_exact_at(offset, out),
        }
    }
}

struct HttpRangeReader {
    client: Client,
    url: String,
}

impl HttpRangeReader {
    fn new(client: Client, url: String) -> Self {
        Self { client, url }
    }
}

impl RangeReader for HttpRangeReader {
    type ReadFuture<'a>
        = Ready<Result<()>>
    where
        Self: 'a;

    fn read_exact_at<'a>(&'a mut self, offset: u64, out: &'a mut [u8]) -> Self::ReadFuture<'a> {
        ready(
            http_range_get_exact(&self.client, &self.url, offset, out.len()).and_then(|bytes| {
                out.copy_from_slice(&bytes);
                Ok(())
            }),
        )
    }
}

fn read_car_range(
    http: &Client,
    epoch: u64,
    cars_dir: Option<&Path>,
    base_url: &str,
    offset: u64,
    len: u64,
) -> Result<Vec<u8>> {
    if len == 0 {
        bail!("cannot read an empty CAR range");
    }
    let byte_len = usize::try_from(len).context("CAR range length exceeds address space")?;
    if let Some(path) = find_local_plain_car(epoch, cars_dir) {
        let mut file = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
        file.seek(SeekFrom::Start(offset))
            .with_context(|| format!("seek {} to {offset}", path.display()))?;
        let mut bytes = vec![0; byte_len];
        file.read_exact(&mut bytes)
            .with_context(|| format!("read {len} bytes at {offset} from {}", path.display()))?;
        return Ok(bytes);
    }

    let url = format!("{base_url}/{epoch}/epoch-{epoch}.car");
    http_range_get_exact(http, &url, offset, byte_len)
}

fn http_range_get_exact(http: &Client, url: &str, offset: u64, len: usize) -> Result<Vec<u8>> {
    if len == 0 {
        bail!("cannot request an empty HTTP range from {url}");
    }
    let end = offset
        .checked_add(u64::try_from(len).context("HTTP range length exceeds u64")? - 1)
        .ok_or_else(|| anyhow!("HTTP range end overflow"))?;
    let response = http
        .get(url)
        .header(
            RANGE,
            HeaderValue::from_str(&format!("bytes={offset}-{end}"))?,
        )
        .send()
        .with_context(|| format!("range GET {url}"))?;
    if response.status().as_u16() != 206 {
        bail!(
            "range GET {url} returned HTTP {}, expected 206",
            response.status().as_u16()
        );
    }
    let content_range = response
        .headers()
        .get(CONTENT_RANGE)
        .ok_or_else(|| anyhow!("range GET {url} has no Content-Range header"))?
        .to_str()
        .with_context(|| format!("decode Content-Range from {url}"))?;
    let expected_prefix = format!("bytes {offset}-{end}/");
    let total = content_range.strip_prefix(&expected_prefix).ok_or_else(|| {
        anyhow!(
            "range GET {url} returned Content-Range {content_range:?}, expected {expected_prefix}..."
        )
    })?;
    if total.is_empty() {
        bail!("range GET {url} returned an empty Content-Range total");
    }
    let bytes = response
        .bytes()
        .with_context(|| format!("read range response from {url}"))?;
    if bytes.len() != len {
        bail!(
            "range GET {url} returned {} bytes, expected {len}",
            bytes.len()
        );
    }
    Ok(bytes.to_vec())
}

fn find_local_plain_car(epoch: u64, cars_dir: Option<&Path>) -> Option<PathBuf> {
    let root = cars_dir?;
    let name = format!("epoch-{epoch}.car");
    [
        root.join(&name),
        root.join(epoch.to_string()).join(&name),
        root.join(format!("epoch-{epoch}")).join(&name),
    ]
    .into_iter()
    .find(|path| path.is_file())
}

fn read_registry_only_sidecars(root: &Path, epoch: u64, raw_bytes: &[u8]) -> Result<EpochSidecars> {
    let expected_raw_len = SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE;
    if raw_bytes.len() != expected_raw_len {
        bail!(
            "registry-only raw index has {} bytes, expected {expected_raw_len}",
            raw_bytes.len()
        );
    }
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let mut rows = Vec::new();
    for (slot_in_epoch, raw_row) in raw_bytes.chunks_exact(SLOT_RANGE_ENTRY_SIZE).enumerate() {
        if decode_slot_range_entry(raw_row)?.is_empty() {
            continue;
        }
        rows.push(ArchiveV2BlockIndexRow {
            block_id: u32::try_from(rows.len()).context("registry-only block count exceeds u32")?,
            slot: epoch_start + slot_in_epoch as u64,
        });
    }
    let epoch_dir = registry_only_epoch_dir(root, epoch)?;
    let blockhashes = read_blockhash_registry(&epoch_dir.join(BLOCKHASH_REGISTRY_FILE))?;
    validate_sidecar_parts(epoch, rows, blockhashes)
        .with_context(|| format!("validate direct-CAR registry in {}", epoch_dir.display()))
}

fn read_epoch_last_blockhash(
    root: &Path,
    indexes_dir: &Path,
    epoch: u64,
    mode: ValidationMode,
    http: &Client,
    cars_dir: Option<&Path>,
    base_url: &str,
) -> Result<[u8; 32]> {
    match mode {
        ValidationMode::OldFaithfulIndex => {
            let sidecars = read_old_faithful_index_sidecars(
                root,
                indexes_dir,
                epoch,
                http,
                cars_dir,
                base_url,
                false,
            )?;
            sidecars
                .rows
                .last()
                .map(|_| sidecars.blockhashes[sidecars.registry_offset + sidecars.rows.len() - 1])
                .ok_or_else(|| anyhow!("epoch {epoch} ordered block slot list has no rows"))
        }
        ValidationMode::RegistryOnly => {
            let epoch_dir = registry_only_epoch_dir(root, epoch)?;
            let blockhashes = read_blockhash_registry(&epoch_dir.join(BLOCKHASH_REGISTRY_FILE))?;
            let last = blockhashes
                .last()
                .copied()
                .ok_or_else(|| anyhow!("epoch {epoch} blockhash registry is empty"))?;
            if last == [0; 32] {
                bail!("epoch {epoch} blockhash registry ends with a zero blockhash");
            }
            Ok(last)
        }
        ValidationMode::ArchiveV2 => {
            let sidecars = read_epoch_sidecars(root, epoch)?;
            sidecars
                .rows
                .last()
                .map(|_| sidecars.blockhashes[sidecars.registry_offset + sidecars.rows.len() - 1])
                .ok_or_else(|| anyhow!("epoch {epoch} block index has no rows"))
        }
    }
}

fn validate_sidecar_parts(
    epoch: u64,
    mut rows: Vec<ArchiveV2BlockIndexRow>,
    blockhashes: Vec<[u8; 32]>,
) -> Result<EpochSidecars> {
    rows.sort_unstable_by_key(|row| row.block_id);
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let epoch_end = epoch_start
        .checked_add(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch end slot overflow for epoch {epoch}"))?;
    let mut seen_slots = vec![false; SLOTS_PER_EPOCH as usize];
    let mut previous_slot = None;
    for (position, row) in rows.iter().enumerate() {
        let expected_block_id = u32::try_from(position).context("block index exceeds u32")?;
        if row.block_id != expected_block_id {
            bail!(
                "block index row {position} has block_id {}, expected {expected_block_id}",
                row.block_id
            );
        }
        if !(epoch_start..epoch_end).contains(&row.slot) {
            bail!(
                "block_id {} has slot {}, outside epoch {epoch} range {epoch_start}..{epoch_end}",
                row.block_id,
                row.slot
            );
        }
        let slot_in_epoch = usize::try_from(row.slot - epoch_start)
            .context("slot-in-epoch exceeds address space")?;
        if std::mem::replace(&mut seen_slots[slot_in_epoch], true) {
            bail!(
                "ordered block slot list contains duplicate slot {}",
                row.slot
            );
        }
        if previous_slot.is_some_and(|previous| row.slot <= previous) {
            bail!(
                "ordered block slots are not strictly increasing at block_id {}: {} follows {}",
                row.block_id,
                row.slot,
                previous_slot.unwrap()
            );
        }
        previous_slot = Some(row.slot);
    }
    let registry_offset = blockhashes.len().checked_sub(rows.len()).ok_or_else(|| {
        anyhow!(
            "blockhash registry has {} records for {} block-index rows",
            blockhashes.len(),
            rows.len()
        )
    })?;
    let valid_offset = (epoch == 0 && registry_offset <= 1) || (epoch > 0 && registry_offset == 0);
    if !valid_offset {
        bail!(
            "epoch {epoch} blockhash registry has {} records for {} block-index rows (offset {registry_offset})",
            blockhashes.len(),
            rows.len()
        );
    }
    for (record, blockhash) in blockhashes.iter().enumerate() {
        if *blockhash == [0; 32] {
            bail!("blockhash registry record {record} is zero");
        }
    }
    Ok(EpochSidecars {
        rows,
        blockhashes,
        registry_offset,
        canonical_ranges: None,
    })
}

fn archive_v2_epoch_dir(root: &Path, epoch: u64) -> Result<PathBuf> {
    let candidates = [
        root.join(format!("epoch-{epoch}")),
        root.join(epoch.to_string()),
    ];
    candidates
        .into_iter()
        .find(|path| {
            path.join(BLOCKHASH_REGISTRY_FILE).is_file()
                && path.join(ARCHIVE_V2_BLOCK_INDEX_FILE).is_file()
        })
        .ok_or_else(|| {
            anyhow!(
                "missing epoch {epoch} {BLOCKHASH_REGISTRY_FILE} or {ARCHIVE_V2_BLOCK_INDEX_FILE} under {}",
                root.display()
            )
        })
}

fn registry_only_epoch_dir(root: &Path, epoch: u64) -> Result<PathBuf> {
    let candidates = [
        root.join(format!("epoch-{epoch}")),
        root.join(epoch.to_string()),
    ];
    candidates
        .into_iter()
        .find(|path| path.join(BLOCKHASH_REGISTRY_FILE).is_file())
        .ok_or_else(|| {
            anyhow!(
                "missing epoch {epoch} direct-CAR {BLOCKHASH_REGISTRY_FILE} under {}",
                root.display()
            )
        })
}

fn read_archive_v2_block_index_rows(path: &Path) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    decode_archive_v2_block_index_rows(path, &bytes)
}

fn decode_archive_v2_block_index_rows(
    path: &Path,
    bytes: &[u8],
) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    if bytes.len() < 8 {
        bail!("{} is shorter than Archive V2 index magic", path.display());
    }
    match &bytes[..8] {
        magic if magic == ARCHIVE_V2_LEGACY_INDEX_MAGIC => decode_archive_v2_index_rows(
            path,
            bytes,
            ARCHIVE_V2_LEGACY_INDEX_VERSION,
            ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN,
            ARCHIVE_V2_LEGACY_INDEX_ROW_LEN,
            "legacy",
        ),
        magic if magic == ARCHIVE_V2_HOT_INDEX_MAGIC => decode_archive_v2_index_rows(
            path,
            bytes,
            ARCHIVE_V2_HOT_INDEX_VERSION,
            ARCHIVE_V2_HOT_INDEX_HEADER_LEN,
            ARCHIVE_V2_HOT_INDEX_ROW_LEN,
            "hot",
        ),
        _ => bail!("{} is not an Archive V2 block index", path.display()),
    }
}

fn decode_archive_v2_index_rows(
    path: &Path,
    bytes: &[u8],
    expected_version: u16,
    header_len: usize,
    row_len: usize,
    format_name: &str,
) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    if bytes.len() < header_len {
        bail!(
            "{} is shorter than Archive V2 {format_name} index header",
            path.display()
        );
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().expect("checked header"));
    if version != expected_version {
        bail!(
            "{} has unsupported Archive V2 {format_name} index version {version}",
            path.display()
        );
    }
    let row_count = u64::from_le_bytes(bytes[12..20].try_into().expect("checked header"));
    let row_count_usize =
        usize::try_from(row_count).context("block-index row count exceeds usize")?;
    let expected_len = row_count_usize
        .checked_mul(row_len)
        .and_then(|rows_len| header_len.checked_add(rows_len))
        .ok_or_else(|| anyhow!("Archive V2 block-index length overflow"))?;
    if bytes.len() != expected_len {
        bail!(
            "{} has {} bytes, expected {expected_len} for {row_count} {format_name} rows",
            path.display(),
            bytes.len()
        );
    }
    Ok(bytes[header_len..]
        .chunks_exact(row_len)
        // Archive V2 supplies only block order and slot mapping here. CAR
        // ranges come from the Old Faithful raw index, and hash bytes come
        // from blockhash_registry.bin.
        .map(|row| ArchiveV2BlockIndexRow {
            block_id: u32::from_le_bytes(row[0..4].try_into().expect("fixed row")),
            slot: u64::from_le_bytes(row[4..12].try_into().expect("fixed row")),
        })
        .collect())
}

fn read_exact_size(path: &Path, expected_len: usize, label: &str) -> Result<Vec<u8>> {
    let metadata = path
        .symlink_metadata()
        .with_context(|| format!("inspect {label} {}", path.display()))?;
    if !metadata.is_file() {
        bail!("{label} is not a regular file: {}", path.display());
    }
    if metadata.len() != expected_len as u64 {
        bail!(
            "{label} {} has {} bytes, expected {expected_len}",
            path.display(),
            metadata.len()
        );
    }
    fs::read(path).with_context(|| format!("read {label} {}", path.display()))
}

fn read_blockhash_registry(path: &Path) -> Result<Vec<[u8; 32]>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() % BLOCKHASH_BYTES != 0 {
        bail!(
            "{} has {} bytes, not a multiple of {BLOCKHASH_BYTES}",
            path.display(),
            bytes.len()
        );
    }
    Ok(bytes
        .chunks_exact(BLOCKHASH_BYTES)
        .map(|bytes| bytes.try_into().expect("fixed hash length"))
        .collect())
}

fn epoch_from_v2_name(name: &OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let epoch = name
        .strip_prefix("epoch-")?
        .strip_suffix("-slot-ranges-v2.raw")?;
    if epoch.is_empty() || !epoch.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    epoch.parse().ok()
}

fn display_optional_slot(slot: Option<u64>) -> String {
    slot.map_or_else(|| "none".to_owned(), |slot| slot.to_string())
}

fn hex32(hash: [u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in hash {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("write to string");
    }
    out
}

fn mainnet_genesis_hash() -> Result<[u8; 32]> {
    decode_base58_hash(MAINNET_GENESIS_HASH_BASE58)
}

fn decode_base58_hash(value: &str) -> Result<[u8; 32]> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("decode base58 blockhash {value}"))?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow!("blockhash must be 32 bytes, got {}", bytes.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn empty_indexes() -> (Vec<u8>, Vec<u8>) {
        (
            vec![0; SLOTS_PER_EPOCH as usize * SLOT_RANGE_V2_ENTRY_SIZE],
            vec![0; SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE],
        )
    }

    fn block(block_id: u32, epoch: u64, slot_in_epoch: u64) -> ArchiveV2BlockIndexRow {
        ArchiveV2BlockIndexRow {
            block_id,
            slot: epoch * SLOTS_PER_EPOCH + slot_in_epoch,
        }
    }

    fn sidecars(
        epoch: u64,
        rows: Vec<ArchiveV2BlockIndexRow>,
        blockhashes: Vec<[u8; 32]>,
    ) -> EpochSidecars {
        validate_sidecar_parts(epoch, rows, blockhashes).expect("valid sidecars")
    }

    fn set_range_and_previous(
        v2: &mut [u8],
        raw: &mut [u8],
        slot_in_epoch: usize,
        offset: u64,
        len: u32,
        previous_blockhash: [u8; 32],
    ) {
        let raw_start = slot_in_epoch * SLOT_RANGE_ENTRY_SIZE;
        raw[raw_start..raw_start + 8].copy_from_slice(&offset.to_le_bytes());
        raw[raw_start + 8..raw_start + 12].copy_from_slice(&len.to_le_bytes());
        let v2_start = slot_in_epoch * SLOT_RANGE_V2_ENTRY_SIZE;
        v2[v2_start..v2_start + SLOT_RANGE_ENTRY_SIZE]
            .copy_from_slice(&raw[raw_start..raw_start + SLOT_RANGE_ENTRY_SIZE]);
        v2[v2_start + SLOT_RANGE_ENTRY_SIZE..v2_start + SLOT_RANGE_V2_ENTRY_SIZE]
            .copy_from_slice(&previous_blockhash);
    }

    fn set_previous(v2: &mut [u8], slot_in_epoch: usize, previous_blockhash: [u8; 32]) {
        let start = slot_in_epoch * SLOT_RANGE_V2_ENTRY_SIZE + SLOT_RANGE_ENTRY_SIZE;
        v2[start..start + BLOCKHASH_BYTES].copy_from_slice(&previous_blockhash);
    }

    #[test]
    fn validates_previous_not_current_blockhash_across_epoch_boundary() {
        let (mut v2, mut raw) = empty_indexes();
        let predecessor = [7; 32];
        let first_blockhash = [8; 32];
        let second_blockhash = [9; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, predecessor);
        set_range_and_previous(&mut v2, &mut raw, 2, 159, 120, first_blockhash);
        let sidecars = sidecars(
            1,
            vec![block(0, 1, 0), block(1, 1, 2)],
            vec![first_blockhash, second_blockhash],
        );
        let summary = validate_epoch_bytes(1, &v2, &raw, &sidecars, Some(predecessor))
            .expect("valid continuity");
        assert_eq!(summary.indexed_blocks, 2);
        assert_eq!(summary.present_slots, 2);

        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, first_blockhash);
        let error = validate_epoch_bytes(1, &v2, &raw, &sidecars, Some(predecessor))
            .expect_err("current blockhash must not be used as previous blockhash");
        assert!(error.to_string().contains("prior ordered blockhash"));
    }

    #[test]
    fn first_selected_nonzero_epoch_uses_explicit_seed_before_registry_lookup() {
        let seed = [7; 32];
        let reads = Cell::new(0usize);
        let predecessor = select_predecessor_blockhash(4, None, Some(seed), |_| {
            reads.set(reads.get() + 1);
            Err(anyhow!("missing predecessor registry"))
        })
        .expect("explicit seed must avoid the missing epoch-3 registry");
        assert_eq!(predecessor, Some(seed));
        assert_eq!(reads.get(), 0);

        let predecessor = select_predecessor_blockhash(4, None, None, |epoch| {
            reads.set(reads.get() + 1);
            assert_eq!(epoch, 3);
            Ok([6; 32])
        })
        .expect("without a seed, read the epoch-3 registry");
        assert_eq!(predecessor, Some([6; 32]));
        assert_eq!(reads.get(), 1);
    }

    #[test]
    fn validates_missing_car_range_inside_registry_chain() {
        let (mut v2, mut raw) = empty_indexes();
        let predecessor = [6; 32];
        let first_blockhash = [7; 32];
        let missing_range_blockhash = [8; 32];
        let third_blockhash = [9; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, predecessor);
        set_previous(&mut v2, 1, first_blockhash);
        set_range_and_previous(&mut v2, &mut raw, 2, 159, 120, missing_range_blockhash);
        let sidecars = sidecars(
            4,
            vec![block(0, 4, 0), block(1, 4, 1), block(2, 4, 2)],
            vec![first_blockhash, missing_range_blockhash, third_blockhash],
        );
        let summary = validate_epoch_bytes(4, &v2, &raw, &sidecars, Some(predecessor))
            .expect("missing raw range remains in the blockhash chain");
        assert_eq!(summary.indexed_blocks, 3);
        assert_eq!(summary.present_slots, 2);

        set_previous(&mut v2, 1, [0; 32]);
        let error = validate_epoch_bytes(4, &v2, &raw, &sidecars, Some(predecessor))
            .expect_err("missing raw range still needs the prior blockhash");
        assert!(error.to_string().contains("zero previous blockhash"));
    }

    #[test]
    fn canonical_cid_ranges_reject_mutually_equal_wrong_raw_and_v2_rows() {
        let (mut v2, mut raw) = empty_indexes();
        let seed = [7; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 999, 12, seed);
        let mut sidecars = sidecars(0, vec![block(0, 0, 0)], vec![[8; 32]]);
        sidecars.canonical_ranges = Some(vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize]);
        let error = validate_epoch_bytes(0, &v2, &raw, &sidecars, Some(seed))
            .expect_err("equal raw and v2 rows must still match canonical CID boundaries");
        assert!(error.to_string().contains("canonical CID-index range"));
    }

    #[test]
    fn canonical_empty_range_keeps_the_blockhash_chain_entry() {
        let (mut v2, raw) = empty_indexes();
        let seed = [7; 32];
        set_previous(&mut v2, 0, seed);
        let mut sidecars = sidecars(0, vec![block(0, 0, 0)], vec![[8; 32]]);
        sidecars.canonical_ranges = Some(vec![SlotRange::EMPTY; SLOTS_PER_EPOCH as usize]);
        let summary = validate_epoch_bytes(0, &v2, &raw, &sidecars, Some(seed))
            .expect("a canonical empty range remains in the blockhash chain");
        assert_eq!(summary.indexed_blocks, 1);
        assert_eq!(summary.present_slots, 0);
        assert_eq!(summary.last_blockhash, Some([8; 32]));
    }

    #[test]
    fn rejects_current_hash_for_later_indexed_block() {
        let (mut v2, mut raw) = empty_indexes();
        let predecessor = [7; 32];
        let first_blockhash = [8; 32];
        let second_blockhash = [9; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, predecessor);
        set_range_and_previous(&mut v2, &mut raw, 2, 159, 120, second_blockhash);
        let sidecars = sidecars(
            1,
            vec![block(0, 1, 0), block(1, 1, 2)],
            vec![first_blockhash, second_blockhash],
        );
        let error = validate_epoch_bytes(1, &v2, &raw, &sidecars, Some(predecessor))
            .expect_err("off-by-one hash must fail");
        assert!(error.to_string().contains("prior ordered blockhash"));
    }

    #[test]
    fn permits_epoch_zero_genesis_prefix() {
        let (mut v2, mut raw) = empty_indexes();
        let genesis = mainnet_genesis_hash().expect("mainnet genesis hash");
        let slot_zero_blockhash = [2; 32];
        let next_blockhash = [3; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, genesis);
        set_range_and_previous(&mut v2, &mut raw, 1, 159, 100, slot_zero_blockhash);
        let sidecars = sidecars(
            0,
            vec![block(0, 0, 0), block(1, 0, 1)],
            vec![genesis, slot_zero_blockhash, next_blockhash],
        );
        let summary = validate_epoch_bytes(0, &v2, &raw, &sidecars, None)
            .expect("epoch zero registry with genesis prefix");
        assert_eq!(summary.registry_offset, 1);
    }

    #[test]
    fn permits_epoch_zero_unprefixed_registry_with_explicit_seed() {
        let (mut v2, mut raw) = empty_indexes();
        let seed = [1; 32];
        let slot_zero_blockhash = [2; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, seed);
        let sidecars = sidecars(0, vec![block(0, 0, 0)], vec![slot_zero_blockhash]);
        let summary = validate_epoch_bytes(0, &v2, &raw, &sidecars, Some(seed))
            .expect("explicit seed validates an unprefixed epoch-zero registry");
        assert_eq!(summary.registry_offset, 0);
        assert_eq!(summary.last_blockhash, Some(slot_zero_blockhash));
    }

    #[test]
    fn normal_and_reuse_raw_modes_accept_epoch_zero_unprefixed_registry_with_seed() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let index_dir = temporary.path().join("slot-index");
        let indexes_dir = temporary.path().join("indexes");
        let blockhash_dir = temporary.path().join("registry");
        let compact_epoch_dir = indexes_dir.join("0");
        let registry_epoch_dir = blockhash_dir.join("epoch-0");
        fs::create_dir_all(&index_dir).expect("create slot-index directory");
        fs::create_dir_all(&compact_epoch_dir).expect("create compact-index directory");
        fs::create_dir_all(&registry_epoch_dir).expect("create registry directory");

        let seed = [1; 32];
        let current_blockhash = [2; 32];
        let (mut v2, raw) = empty_indexes();
        set_previous(&mut v2, 0, seed);
        let v2_path = index_dir.join("epoch-0-slot-ranges-v2.raw");
        fs::write(&v2_path, v2).expect("write v2 index");
        fs::write(index_dir.join("epoch-0-slot-ranges.raw"), raw).expect("write raw index");
        fs::write(compact_epoch_dir.join("epoch-0.cid"), "fixture-cid\n").expect("write CID file");
        fs::write(
            compact_epoch_dir.join("epoch-0-fixture-cid-mainnet-slot-to-cid.index"),
            tiny_compact_index(&0u64.to_le_bytes(), &[1; 36]),
        )
        .expect("write slot-to-CID index");
        fs::write(
            compact_epoch_dir.join("epoch-0-fixture-cid-mainnet-cid-to-offset-and-size.index"),
            tiny_compact_index(&[1; 36], &offset_size_value(0, 1)),
        )
        .expect("write CID-to-offset index");
        fs::write(temporary.path().join("epoch-0.car"), car_header_prefix())
            .expect("write CAR header fixture");
        fs::write(
            registry_epoch_dir.join(BLOCKHASH_REGISTRY_FILE),
            current_blockhash,
        )
        .expect("write unprefixed registry");

        let summary = validate_epoch(
            0,
            &v2_path,
            &index_dir,
            &blockhash_dir,
            &indexes_dir,
            ValidationMode::OldFaithfulIndex,
            Some(seed),
            &Client::new(),
            Some(temporary.path()),
            DEFAULT_BASE_URL,
            false,
        )
        .expect("normal mode must accept an explicit epoch-zero seed");
        assert_eq!(summary.registry_offset, 0);
        assert_eq!(summary.last_blockhash, Some(current_blockhash));

        fs::remove_file(
            compact_epoch_dir.join("epoch-0-fixture-cid-mainnet-cid-to-offset-and-size.index"),
        )
        .expect("remove CID-to-offset index");
        fs::remove_file(temporary.path().join("epoch-0.car")).expect("remove CAR fixture");
        let reused = validate_epoch(
            0,
            &v2_path,
            &index_dir,
            &blockhash_dir,
            &indexes_dir,
            ValidationMode::OldFaithfulIndex,
            Some(seed),
            &Client::new(),
            None,
            "http://127.0.0.1:1",
            true,
        )
        .expect("reuse-raw mode must not rebuild all canonical CID ranges");
        assert_eq!(reused.registry_offset, 0);
        assert_eq!(reused.last_blockhash, Some(current_blockhash));
    }

    #[test]
    fn rejects_epoch_zero_prefix_that_differs_from_explicit_seed() {
        let (mut v2, mut raw) = empty_indexes();
        let registry_genesis = mainnet_genesis_hash().expect("mainnet genesis hash");
        let explicit_seed = [9; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, registry_genesis);
        let sidecars = sidecars(0, vec![block(0, 0, 0)], vec![registry_genesis, [2; 32]]);
        let error = validate_epoch_bytes(0, &v2, &raw, &sidecars, Some(explicit_seed))
            .expect_err("a present genesis prefix must match the explicit seed");
        assert!(error.to_string().contains("--seed-previous-blockhash"));
    }

    #[test]
    fn rejects_epoch_zero_current_hash_as_previous_without_seed() {
        let (mut v2, mut raw) = empty_indexes();
        let current_blockhash = [2; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, current_blockhash);
        let sidecars = sidecars(0, vec![block(0, 0, 0)], vec![current_blockhash]);
        let error = validate_epoch_bytes(0, &v2, &raw, &sidecars, None)
            .expect_err("epoch zero offset zero requires an explicit seed");
        assert!(error.to_string().contains("--seed-previous-blockhash"));
    }

    #[test]
    fn rejects_genesis_prefix_after_epoch_zero() {
        let error = validate_sidecar_parts(1, vec![block(0, 1, 0)], vec![[7; 32], [8; 32]])
            .expect_err("later epoch must use an exact registry length");
        assert!(error.to_string().contains("offset 1"));
    }

    #[test]
    fn rejects_non_increasing_ordered_block_slots() {
        let error = validate_sidecar_parts(
            1,
            vec![block(0, 1, 2), block(1, 1, 1)],
            vec![[7; 32], [8; 32]],
        )
        .expect_err("ordered slots must increase with block ID");
        assert!(error.to_string().contains("not strictly increasing"));
    }

    #[test]
    fn rejects_non_indexed_hash_and_raw_range() {
        let (mut v2, mut raw) = empty_indexes();
        let genesis_seed = [3; 32];
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, genesis_seed);
        set_previous(&mut v2, 1, [2; 32]);
        let sidecars = sidecars(0, vec![block(0, 0, 0)], vec![[1; 32]]);
        let error = validate_epoch_bytes(0, &v2, &raw, &sidecars, Some(genesis_seed))
            .expect_err("non-indexed hash must fail");
        assert!(
            error
                .to_string()
                .contains("absent from the ordered block slot list")
        );

        set_previous(&mut v2, 1, [0; 32]);
        set_range_and_previous(&mut v2, &mut raw, 2, 159, 100, [0; 32]);
        let error = validate_epoch_bytes(0, &v2, &raw, &sidecars, Some(genesis_seed))
            .expect_err("non-indexed raw range must fail");
        assert!(error.to_string().contains("raw-present slot"));
    }

    #[test]
    fn rejects_overlapping_present_ranges() {
        let (mut v2, mut raw) = empty_indexes();
        set_range_and_previous(&mut v2, &mut raw, 0, 59, 100, [1; 32]);
        set_range_and_previous(&mut v2, &mut raw, 1, 100, 100, [2; 32]);
        let sidecars = sidecars(
            0,
            vec![block(0, 0, 0), block(1, 0, 1)],
            vec![[1; 32], [2; 32]],
        );
        let error = validate_epoch_bytes(0, &v2, &raw, &sidecars, Some([3; 32]))
            .expect_err("overlap must fail");
        assert!(error.to_string().contains("prior present range ends"));
    }

    #[test]
    fn decodes_legacy_and_hot_block_indexes() {
        let expected = vec![block(0, 7, 0), block(1, 7, 2)];
        for (magic, header_len, row_len) in [
            (
                ARCHIVE_V2_LEGACY_INDEX_MAGIC,
                ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN,
                ARCHIVE_V2_LEGACY_INDEX_ROW_LEN,
            ),
            (
                ARCHIVE_V2_HOT_INDEX_MAGIC,
                ARCHIVE_V2_HOT_INDEX_HEADER_LEN,
                ARCHIVE_V2_HOT_INDEX_ROW_LEN,
            ),
        ] {
            let mut bytes = vec![0; header_len + expected.len() * row_len];
            bytes[..8].copy_from_slice(magic);
            bytes[8..10].copy_from_slice(&1u16.to_le_bytes());
            bytes[12..20].copy_from_slice(&(expected.len() as u64).to_le_bytes());
            for (position, row) in expected.iter().enumerate() {
                let start = header_len + position * row_len;
                bytes[start..start + 4].copy_from_slice(&row.block_id.to_le_bytes());
                bytes[start + 4..start + 12].copy_from_slice(&row.slot.to_le_bytes());
            }
            assert_eq!(
                decode_archive_v2_block_index_rows(Path::new("fixture.index"), &bytes)
                    .expect("decode block index"),
                expected
            );
        }
    }

    #[test]
    fn registry_only_mode_derives_rows_from_direct_car_ranges() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let epoch_dir = temporary.path().join("epoch-1");
        fs::create_dir_all(&epoch_dir).expect("create epoch directory");
        let mut registry = Vec::new();
        registry.extend_from_slice(&[8; 32]);
        registry.extend_from_slice(&[9; 32]);
        fs::write(epoch_dir.join(BLOCKHASH_REGISTRY_FILE), registry)
            .expect("write direct-CAR registry");

        let (_, mut raw) = empty_indexes();
        let mut ignored_v2 = vec![0; SLOTS_PER_EPOCH as usize * SLOT_RANGE_V2_ENTRY_SIZE];
        set_range_and_previous(&mut ignored_v2, &mut raw, 0, 59, 100, [7; 32]);
        set_range_and_previous(&mut ignored_v2, &mut raw, 2, 159, 100, [8; 32]);
        let sidecars =
            read_registry_only_sidecars(temporary.path(), 1, &raw).expect("registry-only sidecars");
        assert_eq!(sidecars.rows, vec![block(0, 1, 0), block(1, 1, 2)]);
        assert_eq!(sidecars.registry_offset, 0);
    }

    #[test]
    fn old_faithful_mode_reads_order_from_slot_to_cid_index() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let indexes_dir = temporary.path().join("indexes");
        let registry_dir = temporary.path().join("registry");
        let index_epoch_dir = indexes_dir.join("4");
        let registry_epoch_dir = registry_dir.join("epoch-4");
        fs::create_dir_all(&index_epoch_dir).expect("create index directory");
        fs::create_dir_all(&registry_epoch_dir).expect("create registry directory");
        fs::write(index_epoch_dir.join("epoch-4.cid"), "fixture-cid\n").expect("write CID file");
        let expected_slot = 4 * SLOTS_PER_EPOCH + 1;
        fs::write(
            index_epoch_dir.join("epoch-4-fixture-cid-mainnet-slot-to-cid.index"),
            tiny_compact_index(&expected_slot.to_le_bytes(), &[1; 36]),
        )
        .expect("write slot-to-CID index");
        fs::write(
            index_epoch_dir.join("epoch-4-fixture-cid-mainnet-cid-to-offset-and-size.index"),
            tiny_compact_index(&[1; 36], &offset_size_value(0, 1)),
        )
        .expect("write CID-to-offset index");
        fs::write(temporary.path().join("epoch-4.car"), car_header_prefix())
            .expect("write CAR header fixture");
        fs::write(registry_epoch_dir.join(BLOCKHASH_REGISTRY_FILE), [8; 32])
            .expect("write blockhash registry");

        let sidecars = read_old_faithful_index_sidecars(
            &registry_dir,
            &indexes_dir,
            4,
            &Client::new(),
            Some(temporary.path()),
            DEFAULT_BASE_URL,
            true,
        )
        .expect("read independent validation inputs");
        assert_eq!(sidecars.rows, vec![block(0, 4, 1)]);
        assert_eq!(sidecars.blockhashes, vec![[8; 32]]);
    }

    #[test]
    fn old_faithful_collision_uses_exact_cid_index_car_frame() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let payload = test_block_payload(7);
        let cid = *of_car_reader::reconstruct::Cid36::compute(&payload).car_bytes();
        let frame = test_car_frame(cid, &payload);
        let offset = 59u64;
        let mut car = vec![0; offset as usize];
        car.extend_from_slice(&frame);
        fs::write(temporary.path().join("epoch-0.car"), car).expect("write CAR fixture");

        let cid_index_path = temporary.path().join("cid.index");
        fs::write(
            &cid_index_path,
            tiny_compact_index(&cid, &offset_size_value(offset, frame.len() as u32)),
        )
        .expect("write CID index");
        let candidates = [
            BlockSlotCandidate { slot: 7, cid },
            BlockSlotCandidate { slot: 8, cid },
        ];
        let slots = resolve_old_faithful_block_slots(
            0,
            &candidates,
            &cid_index_path,
            &Client::new(),
            Some(temporary.path()),
            DEFAULT_BASE_URL,
        )
        .expect("resolve duplicate CID from exact CAR frame");
        assert_eq!(slots, vec![7]);
    }

    fn test_block_payload(slot: u8) -> Vec<u8> {
        assert!(slot < 24);
        vec![0x86, 0x02, slot, 0x80, 0x80, 0x83, 0xf6, 0xf6, 0xf6, 0xf6]
    }

    fn test_car_frame(cid: [u8; 36], payload: &[u8]) -> Vec<u8> {
        let entry_len = cid.len() + payload.len();
        assert!(entry_len < 128);
        let mut frame = Vec::with_capacity(entry_len + 1);
        frame.push(entry_len as u8);
        frame.extend_from_slice(&cid);
        frame.extend_from_slice(payload);
        frame
    }

    fn offset_size_value(offset: u64, size: u32) -> [u8; 9] {
        assert!(offset < (1u64 << 48));
        assert!(size < (1u32 << 24));
        let mut value = [0; 9];
        value[..6].copy_from_slice(&offset.to_le_bytes()[..6]);
        value[6..].copy_from_slice(&size.to_le_bytes()[..3]);
        value
    }

    fn car_header_prefix() -> [u8; 16] {
        let mut prefix = [0; 16];
        prefix[0] = 58;
        prefix
    }

    fn tiny_compact_index(key: &[u8], value: &[u8]) -> Vec<u8> {
        use of_car_reader::compact_index::{
            BUCKET_HEADER_SIZE, COMPACT_INDEX_FIXED_HEADER_SIZE, COMPACT_INDEX_MAGIC,
            truncate_entry_hash,
        };

        let hash_domain = 11u32;
        let hash_len = 8u8;
        let header_len = 13u32;
        let bucket_count = 1u32;
        let data_offset = COMPACT_INDEX_FIXED_HEADER_SIZE + BUCKET_HEADER_SIZE;
        let target = truncate_entry_hash(hash_domain, key, hash_len as usize);
        let mut out = Vec::new();
        out.extend_from_slice(COMPACT_INDEX_MAGIC);
        out.extend_from_slice(&header_len.to_le_bytes());
        out.extend_from_slice(&(value.len() as u64).to_le_bytes());
        out.extend_from_slice(&bucket_count.to_le_bytes());
        out.push(1);
        let mut bucket_header = [0u8; BUCKET_HEADER_SIZE];
        bucket_header[0..4].copy_from_slice(&hash_domain.to_le_bytes());
        bucket_header[4..8].copy_from_slice(&1u32.to_le_bytes());
        bucket_header[8] = hash_len;
        bucket_header[10..16].copy_from_slice(&(data_offset as u64).to_le_bytes()[..6]);
        out.extend_from_slice(&bucket_header);
        out.extend_from_slice(&target.to_le_bytes());
        out.extend_from_slice(value);
        out
    }

    #[test]
    fn alternate_validation_modes_are_explicit() {
        let independent = parse_args([OsString::from("slot-index"), OsString::from("registry")])
            .expect("default arguments");
        assert_eq!(independent.mode, ValidationMode::OldFaithfulIndex);
        assert_eq!(independent.indexes_dir, PathBuf::from("indexes"));
        assert_eq!(independent.seed_previous_blockhash, None);
        assert_eq!(independent.base_url, DEFAULT_BASE_URL);
        assert!(!independent.reuse_raw);

        let reused = parse_args([
            OsString::from("slot-index"),
            OsString::from("registry"),
            OsString::from("--reuse-raw"),
        ])
        .expect("reuse-raw arguments");
        assert_eq!(reused.mode, ValidationMode::OldFaithfulIndex);
        assert!(reused.reuse_raw);

        let seeded = parse_args([
            OsString::from("slot-index"),
            OsString::from("registry"),
            OsString::from("--seed-previous-blockhash"),
            OsString::from(MAINNET_GENESIS_HASH_BASE58),
        ])
        .expect("seeded arguments");
        assert_eq!(
            seeded.seed_previous_blockhash,
            Some(mainnet_genesis_hash().expect("mainnet genesis hash"))
        );

        let archive = parse_args([
            OsString::from("slot-index"),
            OsString::from("archive-v2"),
            OsString::from("--archive-v2"),
        ])
        .expect("Archive V2 arguments");
        assert_eq!(archive.mode, ValidationMode::ArchiveV2);

        let direct_car = parse_args([
            OsString::from("slot-index"),
            OsString::from("blockhash-registry"),
            OsString::from("--registry-only"),
        ])
        .expect("registry-only arguments");
        assert_eq!(direct_car.mode, ValidationMode::RegistryOnly);

        for alternate_mode in ["--archive-v2", "--registry-only"] {
            assert!(
                parse_args([
                    OsString::from("slot-index"),
                    OsString::from("sidecars"),
                    OsString::from(alternate_mode),
                    OsString::from("--reuse-raw"),
                ])
                .is_err(),
                "{alternate_mode} must reject --reuse-raw"
            );
        }

        assert!(
            parse_args([
                OsString::from("slot-index"),
                OsString::from("sidecars"),
                OsString::from("--archive-v2"),
                OsString::from("--registry-only"),
            ])
            .is_err()
        );
    }

    #[test]
    fn parses_only_strict_v2_names() {
        assert_eq!(
            epoch_from_v2_name(OsStr::new("epoch-73-slot-ranges-v2.raw")),
            Some(73)
        );
        for name in [
            "epoch--slot-ranges-v2.raw",
            "epoch-7x-slot-ranges-v2.raw",
            "epoch-7-slot-ranges.raw",
            "prefix-epoch-7-slot-ranges-v2.raw",
        ] {
            assert_eq!(epoch_from_v2_name(OsStr::new(name)), None, "{name}");
        }
    }
}
