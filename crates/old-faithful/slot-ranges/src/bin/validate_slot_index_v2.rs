use anyhow::{Context, Result, anyhow, bail};
use of_car_reader::slot_ranges::{
    SLOT_RANGE_ENTRY_SIZE, SLOT_RANGE_V2_ENTRY_SIZE, SLOTS_PER_EPOCH, decode_slot_range_entry,
    decode_slot_range_v2_entry,
};
use std::{
    collections::BTreeMap,
    env,
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

const BLOCKHASH_BYTES: usize = 32;
const BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";

#[derive(Debug)]
struct Cli {
    index_dir: PathBuf,
    blockhash_dir: PathBuf,
    start_epoch: Option<u64>,
    end_epoch: Option<u64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct EpochSummary {
    epoch: u64,
    present_slots: usize,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    registry_records: usize,
    registry_offset: usize,
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
    "usage: of-validate-slot-index-v2 <slot-index-dir> <blockhash-dir> [--start-epoch N] [--end-epoch N]"
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
    while let Some(argument) = arguments.next() {
        let argument = argument
            .to_str()
            .ok_or_else(|| anyhow!("argument is not valid UTF-8"))?;
        let target = match argument {
            "--start-epoch" => &mut start_epoch,
            "--end-epoch" => &mut end_epoch,
            "-h" | "--help" => bail!(usage()),
            _ => bail!("unknown argument {argument:?}; {}", usage()),
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
    Ok(Cli {
        index_dir,
        blockhash_dir,
        start_epoch,
        end_epoch,
    })
}

fn run(cli: Cli) -> Result<()> {
    if !cli.index_dir.is_dir() {
        bail!("missing slot index directory: {}", cli.index_dir.display());
    }
    if !cli.blockhash_dir.is_dir() {
        bail!(
            "missing blockhash registry directory: {}",
            cli.blockhash_dir.display()
        );
    }

    let discovered = discover_v2_indexes(&cli.index_dir)?;
    if discovered.is_empty() {
        bail!(
            "no epoch-*-slot-ranges-v2.raw files in {}",
            cli.index_dir.display()
        );
    }
    let selected = select_epochs(discovered, cli.start_epoch, cli.end_epoch)?;

    let mut total_present_slots = 0usize;
    for (epoch, v2_path) in &selected {
        let summary = validate_epoch(*epoch, v2_path, &cli.index_dir, &cli.blockhash_dir)?;
        total_present_slots = total_present_slots
            .checked_add(summary.present_slots)
            .ok_or_else(|| anyhow!("present slot count overflow"))?;
        println!(
            "epoch={} present_slots={} first_slot={} last_slot={} registry_records={} registry_offset={}",
            summary.epoch,
            summary.present_slots,
            display_optional_slot(summary.first_slot),
            display_optional_slot(summary.last_slot),
            summary.registry_records,
            summary.registry_offset,
        );
    }
    println!(
        "validated_epochs={} total_present_slots={total_present_slots}",
        selected.len()
    );
    Ok(())
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
    blockhash_dir: &Path,
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

    let registry_path = blockhash_registry_path(blockhash_dir, epoch)?;
    let blockhashes = read_blockhash_registry(&registry_path)?;
    let predecessor = if epoch == 0 {
        None
    } else {
        let predecessor_path = blockhash_registry_path(blockhash_dir, epoch - 1)?;
        Some(read_last_blockhash(&predecessor_path)?)
    };

    validate_epoch_bytes(epoch, &v2_bytes, &raw_bytes, &blockhashes, predecessor)
        .with_context(|| format!("validate epoch {epoch} from {}", v2_path.display()))
}

fn validate_epoch_bytes(
    epoch: u64,
    v2_bytes: &[u8],
    raw_bytes: &[u8],
    blockhashes: &[[u8; 32]],
    predecessor_last_blockhash: Option<[u8; 32]>,
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

    let present_slots = v2_bytes
        .chunks_exact(SLOT_RANGE_V2_ENTRY_SIZE)
        .filter(|row| u32::from_le_bytes(row[8..12].try_into().expect("fixed row")) != 0)
        .count();
    let registry_offset = blockhashes
        .len()
        .checked_sub(present_slots)
        .ok_or_else(|| {
            anyhow!(
                "blockhash registry has {} records for {present_slots} present slots",
                blockhashes.len()
            )
        })?;
    let valid_offset = (epoch == 0 && registry_offset <= 1) || (epoch > 0 && registry_offset == 0);
    if !valid_offset {
        bail!(
            "epoch {epoch} blockhash registry has {} records for {present_slots} present slots (offset {registry_offset})",
            blockhashes.len()
        );
    }
    if epoch > 0 && predecessor_last_blockhash.is_none() {
        bail!(
            "epoch {epoch} needs the last blockhash from epoch {}",
            epoch - 1
        );
    }
    if let Some(predecessor) = predecessor_last_blockhash {
        if predecessor == [0; 32] {
            bail!("epoch {} predecessor blockhash is zero", epoch - 1);
        }
    }

    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;
    let genesis_seed = (registry_offset == 1).then(|| blockhashes[0]);
    let boundary_previous = predecessor_last_blockhash.or(genesis_seed);
    let mut present_index = 0usize;
    let mut first_slot = None;
    let mut last_slot = None;
    let mut previous_range_end = None;

    for (slot_in_epoch, (v2_row, raw_row)) in v2_bytes
        .chunks_exact(SLOT_RANGE_V2_ENTRY_SIZE)
        .zip(raw_bytes.chunks_exact(SLOT_RANGE_ENTRY_SIZE))
        .enumerate()
    {
        if &v2_row[..SLOT_RANGE_ENTRY_SIZE] != raw_row {
            bail!(
                "slot {} range differs between v2 and raw indexes",
                epoch_start + slot_in_epoch as u64
            );
        }
        let entry = decode_slot_range_v2_entry(v2_row)?;
        let raw_entry = decode_slot_range_entry(raw_row)?;
        debug_assert_eq!(entry.range, raw_entry);
        let slot = epoch_start
            .checked_add(slot_in_epoch as u64)
            .ok_or_else(|| anyhow!("slot overflow"))?;

        if entry.range.is_empty() {
            if entry.range.offset != 0 {
                bail!(
                    "empty slot {slot} has nonzero offset {}",
                    entry.range.offset
                );
            }
            if entry.previous_blockhash != [0; 32] {
                bail!("empty slot {slot} has a nonzero previous blockhash");
            }
            continue;
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
        if previous_range_end.is_some_and(|previous_end| entry.range.offset < previous_end) {
            bail!(
                "slot {slot} CAR range starts at {} before the prior present range ends at {}",
                entry.range.offset,
                previous_range_end.unwrap()
            );
        }

        let hash_index = registry_offset
            .checked_add(present_index)
            .ok_or_else(|| anyhow!("blockhash registry index overflow"))?;
        let current_blockhash = *blockhashes.get(hash_index).ok_or_else(|| {
            anyhow!("missing blockhash record {hash_index} for present slot {slot}")
        })?;
        if current_blockhash == [0; 32] {
            bail!("slot {slot} has a zero current blockhash in the registry");
        }
        let expected_previous = if present_index == 0 {
            boundary_previous
        } else {
            blockhashes.get(hash_index - 1).copied()
        };
        if slot != 0 && entry.previous_blockhash == [0; 32] {
            bail!("present non-genesis slot {slot} has a zero previous blockhash");
        }
        if let Some(expected_previous) = expected_previous {
            if entry.previous_blockhash != expected_previous {
                bail!(
                    "slot {slot} previous blockhash {} differs from prior present blockhash {}",
                    hex32(entry.previous_blockhash),
                    hex32(expected_previous)
                );
            }
        } else if slot != 0 {
            bail!("slot {slot} has no predecessor blockhash source");
        }

        present_index += 1;
        first_slot.get_or_insert(slot);
        last_slot = Some(slot);
        previous_range_end = Some(range_end);
    }

    debug_assert_eq!(present_index, present_slots);
    Ok(EpochSummary {
        epoch,
        present_slots,
        first_slot,
        last_slot,
        registry_records: blockhashes.len(),
        registry_offset,
    })
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

fn blockhash_registry_path(root: &Path, epoch: u64) -> Result<PathBuf> {
    let candidates = [
        root.join(format!("epoch-{epoch}"))
            .join(BLOCKHASH_REGISTRY_FILE),
        root.join(epoch.to_string()).join(BLOCKHASH_REGISTRY_FILE),
    ];
    candidates
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| {
            anyhow!(
                "missing epoch {epoch} {BLOCKHASH_REGISTRY_FILE} under {}",
                root.display()
            )
        })
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

fn read_last_blockhash(path: &Path) -> Result<[u8; 32]> {
    let blockhashes = read_blockhash_registry(path)?;
    blockhashes
        .last()
        .copied()
        .ok_or_else(|| anyhow!("{} is empty", path.display()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_indexes() -> (Vec<u8>, Vec<u8>) {
        (
            vec![0; SLOTS_PER_EPOCH as usize * SLOT_RANGE_V2_ENTRY_SIZE],
            vec![0; SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE],
        )
    }

    fn set_present(
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

    #[test]
    fn validates_previous_not_current_blockhash_across_epoch_boundary() {
        let (mut v2, mut raw) = empty_indexes();
        let predecessor = [7; 32];
        let first_blockhash = [8; 32];
        let second_blockhash = [9; 32];
        set_present(&mut v2, &mut raw, 0, 59, 100, predecessor);
        set_present(&mut v2, &mut raw, 2, 159, 120, first_blockhash);

        let summary = validate_epoch_bytes(
            1,
            &v2,
            &raw,
            &[first_blockhash, second_blockhash],
            Some(predecessor),
        )
        .expect("valid continuity");
        assert_eq!(summary.present_slots, 2);

        set_present(&mut v2, &mut raw, 0, 59, 100, first_blockhash);
        let error = validate_epoch_bytes(
            1,
            &v2,
            &raw,
            &[first_blockhash, second_blockhash],
            Some(predecessor),
        )
        .expect_err("current blockhash must not be used as previous blockhash");
        assert!(error.to_string().contains("prior present blockhash"));
    }

    #[test]
    fn rejects_current_hash_for_later_present_slot() {
        let (mut v2, mut raw) = empty_indexes();
        let predecessor = [7; 32];
        let first_blockhash = [8; 32];
        let second_blockhash = [9; 32];
        set_present(&mut v2, &mut raw, 0, 59, 100, predecessor);
        set_present(&mut v2, &mut raw, 2, 159, 120, second_blockhash);

        let error = validate_epoch_bytes(
            1,
            &v2,
            &raw,
            &[first_blockhash, second_blockhash],
            Some(predecessor),
        )
        .expect_err("off-by-one hash must fail");
        assert!(error.to_string().contains("prior present blockhash"));
    }

    #[test]
    fn permits_epoch_zero_genesis_prefix() {
        let (mut v2, mut raw) = empty_indexes();
        let genesis = [1; 32];
        let slot_zero_blockhash = [2; 32];
        let next_blockhash = [3; 32];
        set_present(&mut v2, &mut raw, 0, 59, 100, genesis);
        set_present(&mut v2, &mut raw, 1, 159, 100, slot_zero_blockhash);
        let summary = validate_epoch_bytes(
            0,
            &v2,
            &raw,
            &[genesis, slot_zero_blockhash, next_blockhash],
            None,
        )
        .expect("epoch zero registry with genesis prefix");
        assert_eq!(summary.registry_offset, 1);
    }

    #[test]
    fn rejects_zero_previous_hash_for_non_genesis_block() {
        let (mut v2, mut raw) = empty_indexes();
        set_present(&mut v2, &mut raw, 0, 59, 100, [0; 32]);
        let error = validate_epoch_bytes(1, &v2, &raw, &[[8; 32]], Some([7; 32]))
            .expect_err("zero previous blockhash must fail");
        assert!(error.to_string().contains("zero previous blockhash"));
    }

    #[test]
    fn rejects_noncanonical_empty_row_and_overlapping_ranges() {
        let (mut v2, raw) = empty_indexes();
        v2[SLOT_RANGE_ENTRY_SIZE] = 1;
        let error = validate_epoch_bytes(0, &v2, &raw, &[], None)
            .expect_err("empty row metadata must be zero");
        assert!(error.to_string().contains("nonzero previous blockhash"));

        let (mut v2, mut raw) = empty_indexes();
        set_present(&mut v2, &mut raw, 0, 59, 100, [1; 32]);
        set_present(&mut v2, &mut raw, 1, 100, 100, [2; 32]);
        let error = validate_epoch_bytes(0, &v2, &raw, &[[1; 32], [2; 32]], None)
            .expect_err("overlap must fail");
        assert!(error.to_string().contains("prior present range ends"));
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
