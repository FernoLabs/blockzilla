use anyhow::{Context, Result, anyhow};
use clap::Parser;
use of_car_reader::{
    CarBlockReader,
    node::{Node, decode_node},
    slot_ranges::{
        SLOT_RANGE_ENTRY_SIZE, SLOTS_PER_EPOCH, SlotRange, SlotRangeWithPreviousBlockhash,
        decode_slot_range_entry, epoch_for_slot, slot_in_epoch,
        write_slot_ranges_raw as write_slot_ranges_raw_entries,
        write_slot_ranges_v2_raw as write_slot_ranges_v2_raw_entries,
    },
};
use of_slot_ranges::{
    AsyncCompactIndex, BuildSlotRangesConfig, LocalFileRangeReader, build_slot_ranges_from_indexes,
    decode_car_header_total_size,
};
use reqwest::blocking::Client;
use reqwest::header::{HeaderMap, HeaderValue, RANGE};
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAX_BUCKET_SIZE: usize = 64 * 1024 * 1024;

// We only need up to 10 bytes to decode a uvarint64.
// We read a few extra bytes (16) to be robust against short reads and proxies,
// but we still only *use* the first 10 for decoding.
const CAR_HEADER_PREFIX_READ_LEN: usize = 16;

#[derive(Parser, Debug)]
#[command(name = "of-slot-ranges")]
struct Cli {
    /// First epoch to process, inclusive.
    #[arg(long)]
    start_epoch: u64,

    /// Last epoch to process, inclusive.
    #[arg(long)]
    end_epoch: u64,

    /// Root directory containing per-epoch compact index files.
    #[arg(long = "indexes-dir", default_value = "indexes", alias = "index-dir")]
    indexes_dir: PathBuf,

    /// Optional directory containing local plain `.car` files.
    /// If a CAR is missing, only the remote CAR header is fetched in memory.
    #[arg(long = "cars-dir", alias = "car-dir")]
    cars_dir: Option<PathBuf>,

    /// Optional root containing Archive V2 per-epoch sidecars.
    ///
    /// When present, v2 slot ranges are rebuilt from `blockhash_registry.bin`,
    /// using the 12-byte slot range output for CAR offsets. If
    /// `archive-v2-blocks.index` exists it is used as the slot/order source;
    /// otherwise blockhashes are consumed in non-empty raw slot-range order.
    /// This avoids rescanning full CAR files for blockhashes.
    #[arg(long = "archive-v2-dir", alias = "block-index-dir")]
    archive_v2_dir: Option<PathBuf>,

    /// Directory where `epoch-*-slot-ranges.raw` files are written.
    #[arg(long = "output-dir", default_value = "out", alias = "out-dir")]
    output_dir: PathBuf,

    /// Rebuild output files even when they already exist.
    #[arg(long)]
    overwrite: bool,

    /// Rebuild only the v2 output file when it already exists.
    #[arg(long)]
    overwrite_v2: bool,

    /// Write only `epoch-N-slot-ranges.raw`.
    ///
    /// This skips v2 output and avoids reading local CAR bodies when `--cars-dir`
    /// is provided only to decode the CAR header length.
    #[arg(long)]
    raw_only: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    fs::create_dir_all(&cli.output_dir)?;

    let http = Client::builder()
        .user_agent("of-slot-ranges/1.0")
        .build()
        .context("build reqwest client")?;

    let mut previous_epoch_last_blockhash: Option<[u8; 32]> = None;

    for epoch in cli.start_epoch..=cli.end_epoch {
        let out_path = cli
            .output_dir
            .join(format!("epoch-{epoch}-slot-ranges.raw"));
        let out_v2_path = cli
            .output_dir
            .join(format!("epoch-{epoch}-slot-ranges-v2.raw"));

        if out_v2_path.exists() && !cli.overwrite && !cli.overwrite_v2 {
            eprintln!("skip epoch={epoch} exists: {}", out_v2_path.display());
            if let Some(last) =
                last_blockhash_from_archive_v2(epoch, cli.archive_v2_dir.as_deref(), true)?
            {
                previous_epoch_last_blockhash = Some(last);
            } else {
                previous_epoch_last_blockhash = None;
            }
            continue;
        }

        let mut index_block_slots = None;

        if out_path.exists() && !cli.overwrite {
            eprintln!("epoch={epoch}: keep existing {}", out_path.display());
        } else {
            let epoch_dir = cli.indexes_dir.join(epoch.to_string());
            let cid_path = epoch_dir.join(format!("epoch-{epoch}.cid"));
            let epoch_cid = fs::read_to_string(&cid_path)
                .with_context(|| format!("read {}", cid_path.display()))?;
            let epoch_cid = epoch_cid.trim().to_string();

            let slot_idx_name = format!("epoch-{epoch}-{epoch_cid}-mainnet-slot-to-cid.index");
            let cid_idx_name =
                format!("epoch-{epoch}-{epoch_cid}-mainnet-cid-to-offset-and-size.index");

            let slot_idx_path = epoch_dir.join(&slot_idx_name);
            let cid_idx_path = epoch_dir.join(&cid_idx_name);

            eprintln!(
                "epoch={epoch}: open slot-to-cid.index: {}",
                slot_idx_path.display()
            );
            eprintln!(
                "epoch={epoch}: open cid-to-offset-and-size.index: {}",
                cid_idx_path.display()
            );

            let slot_reader = LocalFileRangeReader::open(&slot_idx_path)?;
            let cid_reader = LocalFileRangeReader::open(&cid_idx_path)?;
            let mut slot_index = futures::executor::block_on(AsyncCompactIndex::open(
                slot_reader,
                slot_idx_path.display().to_string(),
            ))?;
            let mut cid_index = futures::executor::block_on(AsyncCompactIndex::open(
                cid_reader,
                cid_idx_path.display().to_string(),
            ))?;

            // IMPORTANT: We do NOT download CAR files.
            // We either read the header from a local plain `.car` or fetch a tiny remote prefix.
            let car_hdr = car_header_total_size(&http, epoch, cli.cars_dir.as_deref())?;
            eprintln!("epoch={epoch}: car_header_size={car_hdr}");

            let t0 = std::time::Instant::now();
            let output = futures::executor::block_on(build_slot_ranges_from_indexes(
                epoch,
                car_hdr,
                &mut slot_index,
                &mut cid_index,
                BuildSlotRangesConfig {
                    max_bucket_payload_bytes: MAX_BUCKET_SIZE,
                    allow_node_read_fallback: true,
                },
            ))?;
            eprintln!(
                "epoch={epoch}: done build ranges in {:.2}s present_slots={} slot_bucket_read={} MiB cid_bucket_read={} MiB max_slot_bucket={} MiB max_cid_bucket={} MiB slot_node_fallbacks={} cid_node_fallbacks={}",
                t0.elapsed().as_secs_f64(),
                output.stats.present_slots,
                output.stats.slot_bucket_payload_bytes_read / (1024 * 1024),
                output.stats.cid_bucket_payload_bytes_read / (1024 * 1024),
                output.stats.max_slot_bucket_payload_bytes / (1024 * 1024),
                output.stats.max_cid_bucket_payload_bytes / (1024 * 1024),
                output.stats.slot_node_read_fallbacks,
                output.stats.cid_node_read_fallbacks,
            );

            eprintln!("epoch={epoch}: write {}", out_path.display());
            write_slot_ranges_raw_file(&out_path, &output.ranges)?;
            index_block_slots = Some(output.block_slots);
        }

        if cli.raw_only {
            previous_epoch_last_blockhash = None;
            eprintln!("epoch={epoch}: raw-only, skip slot ranges v2");
        } else if out_v2_path.exists() && !cli.overwrite && !cli.overwrite_v2 {
            eprintln!("epoch={epoch}: keep existing {}", out_v2_path.display());
            if let Some(last) =
                last_blockhash_from_archive_v2(epoch, cli.archive_v2_dir.as_deref(), true)?
            {
                previous_epoch_last_blockhash = Some(last);
            } else {
                previous_epoch_last_blockhash = None;
            }
        } else if let Some(archive_v2_root) = cli.archive_v2_dir.as_deref() {
            let epoch_dir = find_archive_v2_blockhash_dir(archive_v2_root, epoch, true)
                .ok_or_else(|| {
                    anyhow!(
                        "epoch={epoch}: Archive V2 blockhash sidecar dir not found under {}",
                        archive_v2_root.display()
                    )
                })?;
            eprintln!(
                "epoch={epoch}: build slot ranges v2 from Archive V2 sidecars in {}",
                epoch_dir.display()
            );
            let raw_ranges = read_slot_ranges_raw_file(&out_path)
                .with_context(|| format!("read {}", out_path.display()))?;
            let initial_previous_blockhash = match previous_epoch_last_blockhash {
                Some(hash) => Some(hash),
                None if epoch > 0 => {
                    last_blockhash_from_archive_v2(epoch - 1, cli.archive_v2_dir.as_deref(), false)?
                }
                None => None,
            };
            let v2 = build_slot_ranges_v2_from_archive_v2_sidecars(
                &epoch_dir,
                epoch,
                &raw_ranges,
                index_block_slots.as_deref(),
                initial_previous_blockhash,
            )?;
            previous_epoch_last_blockhash = v2.last_blockhash;
            eprintln!("epoch={epoch}: write {}", out_v2_path.display());
            write_slot_ranges_v2_raw_file(&out_v2_path, &v2.ranges)?;
        } else if let Some(local_car_path) = find_local_car(epoch, cli.cars_dir.as_deref()) {
            eprintln!(
                "epoch={epoch}: build slot ranges v2 with previous blockhash from {}",
                local_car_path.display()
            );
            let v2 = build_slot_ranges_v2_from_local_car(
                &local_car_path,
                epoch,
                previous_epoch_last_blockhash,
            )?;
            previous_epoch_last_blockhash = v2.last_blockhash;
            eprintln!("epoch={epoch}: write {}", out_v2_path.display());
            write_slot_ranges_v2_raw_file(&out_v2_path, &v2.ranges)?;
        } else {
            previous_epoch_last_blockhash = None;
            eprintln!(
                "epoch={epoch}: skip slot ranges v2 (requires local plain epoch-{epoch}.car via --cars-dir)"
            );
        }

        eprintln!("epoch={epoch}: done");
    }

    Ok(())
}

/* ---------------- output writer ---------------- */

fn write_slot_ranges_raw_file(path: &Path, ranges: &[SlotRange]) -> Result<()> {
    if ranges.len() != SLOTS_PER_EPOCH as usize {
        return Err(anyhow!("ranges wrong length"));
    }

    let mut f = std::io::BufWriter::with_capacity(256 * 1024, std::fs::File::create(path)?);
    write_slot_ranges_raw_entries(&mut f, ranges)?;
    f.flush()?;
    Ok(())
}

fn read_slot_ranges_raw_file(path: &Path) -> Result<Vec<SlotRange>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let expected_len = SLOTS_PER_EPOCH as usize * SLOT_RANGE_ENTRY_SIZE;
    if bytes.len() != expected_len {
        return Err(anyhow!(
            "{} has {} bytes, expected {expected_len}",
            path.display(),
            bytes.len()
        ));
    }

    bytes
        .chunks_exact(SLOT_RANGE_ENTRY_SIZE)
        .map(|chunk| decode_slot_range_entry(chunk).map_err(Into::into))
        .collect()
}

fn write_slot_ranges_v2_raw_file(
    path: &Path,
    ranges: &[SlotRangeWithPreviousBlockhash],
) -> Result<()> {
    if ranges.len() != SLOTS_PER_EPOCH as usize {
        return Err(anyhow!("v2 ranges wrong length"));
    }

    let mut f = std::io::BufWriter::with_capacity(256 * 1024, std::fs::File::create(path)?);
    write_slot_ranges_v2_raw_entries(&mut f, ranges)?;
    f.flush()?;
    Ok(())
}

struct SlotRangesV2Build {
    ranges: Vec<SlotRangeWithPreviousBlockhash>,
    last_blockhash: Option<[u8; 32]>,
}

const ARCHIVE_V2_BLOCK_INDEX_FILE: &str = "archive-v2-blocks.index";
const ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE: &str = "blockhash_registry.bin";
const ARCHIVE_V2_LEGACY_INDEX_MAGIC: &[u8; 8] = b"BZV2IDX1";
const ARCHIVE_V2_LEGACY_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8;
const ARCHIVE_V2_LEGACY_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 8 + 4 + 4;
const ARCHIVE_V2_HOT_INDEX_MAGIC: &[u8; 8] = b"BZV2HIX1";
const ARCHIVE_V2_HOT_INDEX_VERSION: u16 = 1;
const ARCHIVE_V2_HOT_INDEX_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 8 + 4 + 4;
const ARCHIVE_V2_HOT_INDEX_ROW_LEN: usize = 4 + 8 + 8 + 4 + 4 + 4 + 8 + 8 + 4;

#[derive(Debug, Clone, Copy)]
struct ArchiveV2BlockIndexRow {
    block_id: u32,
    slot: u64,
}

fn build_slot_ranges_v2_from_archive_v2_sidecars(
    epoch_dir: &Path,
    epoch: u64,
    raw_ranges: &[SlotRange],
    index_block_slots: Option<&[u64]>,
    initial_previous_blockhash: Option<[u8; 32]>,
) -> Result<SlotRangesV2Build> {
    if raw_ranges.len() != SLOTS_PER_EPOCH as usize {
        return Err(anyhow!("raw ranges wrong length"));
    }

    let block_index_path = epoch_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    if !block_index_path.is_file() {
        return build_slot_ranges_v2_from_blockhash_registry_sidecar(
            epoch_dir,
            epoch,
            raw_ranges,
            index_block_slots,
            initial_previous_blockhash,
        );
    }

    let mut rows = read_archive_v2_block_index_rows(&block_index_path)?;
    rows.sort_by_key(|row| row.block_id);
    let blockhashes = read_blockhash_registry(&epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?;
    let blockhash_id_offset = blockhash_id_offset(rows.len(), blockhashes.len())?;

    let mut ranges = vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize];
    let genesis_previous_blockhash = (blockhash_id_offset == 1)
        .then(|| blockhashes.first().copied())
        .flatten();
    let mut previous_blockhash = initial_previous_blockhash.or(genesis_previous_blockhash);
    let mut last_blockhash = None;
    let mut present_slots = 0u64;

    for row in rows {
        if epoch_for_slot(row.slot) != epoch {
            continue;
        }
        let hash_index = row
            .block_id
            .checked_add(blockhash_id_offset)
            .ok_or_else(|| anyhow!("blockhash id overflow for block_id {}", row.block_id))?
            as usize;
        let blockhash = *blockhashes.get(hash_index).ok_or_else(|| {
            anyhow!(
                "missing blockhash id {hash_index} for block_id {} slot {}",
                row.block_id,
                row.slot
            )
        })?;

        let idx =
            usize::try_from(slot_in_epoch(row.slot)).context("slot-in-epoch exceeds usize")?;
        let range = raw_ranges[idx];
        if range.is_empty() {
            eprintln!(
                "epoch={epoch}: warning slot {} is present in Archive V2 block index but empty in raw slot ranges",
                row.slot
            );
        }
        let previous = previous_blockhash
            .or_else(|| (row.slot == 0).then_some(blockhash))
            .unwrap_or([0; 32]);
        ranges[idx] = SlotRangeWithPreviousBlockhash {
            range,
            previous_blockhash: previous,
        };
        previous_blockhash = Some(blockhash);
        last_blockhash = Some(blockhash);
        present_slots += 1;
    }

    eprintln!(
        "epoch={epoch}: built v2 from Archive V2 block index present_slots={present_slots} blockhash_id_offset={blockhash_id_offset}"
    );

    Ok(SlotRangesV2Build {
        ranges,
        last_blockhash,
    })
}

fn build_slot_ranges_v2_from_blockhash_registry_sidecar(
    epoch_dir: &Path,
    epoch: u64,
    raw_ranges: &[SlotRange],
    index_block_slots: Option<&[u64]>,
    initial_previous_blockhash: Option<[u8; 32]>,
) -> Result<SlotRangesV2Build> {
    let blockhashes = read_blockhash_registry(&epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE))?;
    let raw_present_slots = raw_ranges
        .iter()
        .copied()
        .filter(|range| !range.is_empty())
        .count();
    let raw_block_slots: Vec<u64>;
    let block_slots = if raw_present_slots == blockhashes.len()
        || raw_present_slots.checked_add(1) == Some(blockhashes.len())
    {
        raw_block_slots = raw_ranges
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(slot_in_epoch, range)| {
                (!range.is_empty()).then(|| epoch * SLOTS_PER_EPOCH + slot_in_epoch as u64)
            })
            .collect();
        &raw_block_slots
    } else if let Some(slots) = index_block_slots {
        slots
    } else {
        return Err(anyhow!(
            "blockhash registry has {} hashes for {raw_present_slots} non-empty raw ranges; rebuild v1 ranges or provide block slot order",
            blockhashes.len()
        ));
    };
    let blockhash_id_offset = blockhash_id_offset(block_slots.len(), blockhashes.len())?;
    let genesis_previous_blockhash = (blockhash_id_offset == 1)
        .then(|| blockhashes.first().copied())
        .flatten();

    let mut ranges = vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut previous_blockhash = initial_previous_blockhash.or(genesis_previous_blockhash);
    let mut last_blockhash = None;
    let epoch_start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .ok_or_else(|| anyhow!("epoch start slot overflow for epoch {epoch}"))?;

    for (block_i, slot) in block_slots.iter().copied().enumerate() {
        if epoch_for_slot(slot) != epoch {
            continue;
        }
        let slot_in_epoch =
            usize::try_from(slot_in_epoch(slot)).context("slot-in-epoch exceeds usize")?;
        let range = raw_ranges[slot_in_epoch];

        let hash_index = block_i
            .checked_add(blockhash_id_offset as usize)
            .ok_or_else(|| anyhow!("blockhash id overflow for block index {block_i}"))?;
        let blockhash = *blockhashes.get(hash_index).ok_or_else(|| {
            anyhow!("missing blockhash id {hash_index} for block index {block_i}")
        })?;
        let previous = previous_blockhash.unwrap_or([0; 32]);
        ranges[slot_in_epoch] = SlotRangeWithPreviousBlockhash {
            range,
            previous_blockhash: previous,
        };
        previous_blockhash = Some(blockhash);
        last_blockhash = Some(blockhash);
    }

    eprintln!(
        "epoch={epoch}: built v2 from blockhash registry only block_slots={} raw_present_slots={} blockhash_id_offset={blockhash_id_offset} first_slot={} last_slot={}",
        block_slots.len(),
        raw_present_slots,
        block_slots.first().copied().unwrap_or(epoch_start),
        block_slots.last().copied().unwrap_or(epoch_start),
    );

    Ok(SlotRangesV2Build {
        ranges,
        last_blockhash,
    })
}

fn last_blockhash_from_archive_v2(
    epoch: u64,
    archive_v2_root: Option<&Path>,
    allow_root_fallback: bool,
) -> Result<Option<[u8; 32]>> {
    let Some(root) = archive_v2_root else {
        return Ok(None);
    };
    let Some(epoch_dir) = find_archive_v2_blockhash_dir(root, epoch, allow_root_fallback) else {
        return Ok(None);
    };
    read_last_blockhash_registry(&epoch_dir.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)).map(Some)
}

fn blockhash_id_offset(row_count: usize, blockhash_count: usize) -> Result<u32> {
    match blockhash_count.checked_sub(row_count) {
        Some(0) => Ok(0),
        Some(1) => Ok(1),
        Some(extra) => Err(anyhow!(
            "blockhash registry has {blockhash_count} hashes for {row_count} rows; expected equal length or one genesis-prefixed hash, got {extra} extra"
        )),
        None => Err(anyhow!(
            "blockhash registry has {blockhash_count} hashes but block index has {row_count} rows"
        )),
    }
}

fn read_blockhash_registry(path: &Path) -> Result<Vec<[u8; 32]>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() % 32 != 0 {
        return Err(anyhow!(
            "{} has invalid length {} (not divisible by 32)",
            path.display(),
            bytes.len()
        ));
    }
    Ok(bytes
        .chunks_exact(32)
        .map(|chunk| {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(chunk);
            hash
        })
        .collect())
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

fn read_archive_v2_block_index_rows(path: &Path) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    if bytes.len() < 8 {
        return Err(anyhow!(
            "{} is shorter than Archive V2 index magic",
            path.display()
        ));
    }

    match &bytes[..8] {
        magic if magic == ARCHIVE_V2_LEGACY_INDEX_MAGIC => {
            read_archive_v2_legacy_block_index_rows(path, &bytes)
        }
        magic if magic == ARCHIVE_V2_HOT_INDEX_MAGIC => {
            read_archive_v2_hot_block_index_rows(path, &bytes)
        }
        _ => Err(anyhow!(
            "{} is not an Archive V2 block index",
            path.display()
        )),
    }
}

fn read_archive_v2_legacy_block_index_rows(
    path: &Path,
    bytes: &[u8],
) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    if bytes.len() < ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN {
        return Err(anyhow!(
            "{} is shorter than Archive V2 legacy index header",
            path.display()
        ));
    }
    let header = &bytes[..ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN];
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    if version != ARCHIVE_V2_LEGACY_INDEX_VERSION {
        return Err(anyhow!(
            "{} has unsupported Archive V2 legacy block index version {version}",
            path.display()
        ));
    }
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let expected_len = ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN as u64
        + row_count * ARCHIVE_V2_LEGACY_INDEX_ROW_LEN as u64;
    if bytes.len() as u64 != expected_len {
        return Err(anyhow!(
            "{} has {} bytes, expected {expected_len} for {row_count} rows",
            path.display(),
            bytes.len()
        ));
    }

    let mut rows = Vec::with_capacity(usize::try_from(row_count)?);
    for chunk in
        bytes[ARCHIVE_V2_LEGACY_INDEX_HEADER_LEN..].chunks_exact(ARCHIVE_V2_LEGACY_INDEX_ROW_LEN)
    {
        rows.push(ArchiveV2BlockIndexRow {
            block_id: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(chunk[4..12].try_into().unwrap()),
        });
    }
    Ok(rows)
}

fn read_archive_v2_hot_block_index_rows(
    path: &Path,
    bytes: &[u8],
) -> Result<Vec<ArchiveV2BlockIndexRow>> {
    if bytes.len() < ARCHIVE_V2_HOT_INDEX_HEADER_LEN {
        return Err(anyhow!(
            "{} is shorter than Archive V2 hot-block index header",
            path.display()
        ));
    }
    let header = &bytes[..ARCHIVE_V2_HOT_INDEX_HEADER_LEN];
    let version = u16::from_le_bytes(header[8..10].try_into().unwrap());
    if version != ARCHIVE_V2_HOT_INDEX_VERSION {
        return Err(anyhow!(
            "{} has unsupported Archive V2 hot-block index version {version}",
            path.display()
        ));
    }
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let expected_len =
        ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64 + row_count * ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64;
    if bytes.len() as u64 != expected_len {
        return Err(anyhow!(
            "{} has {} bytes, expected {expected_len} for {row_count} hot rows",
            path.display(),
            bytes.len()
        ));
    }

    let mut rows = Vec::with_capacity(usize::try_from(row_count)?);
    for chunk in bytes[ARCHIVE_V2_HOT_INDEX_HEADER_LEN..].chunks_exact(ARCHIVE_V2_HOT_INDEX_ROW_LEN)
    {
        rows.push(ArchiveV2BlockIndexRow {
            block_id: u32::from_le_bytes(chunk[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(chunk[4..12].try_into().unwrap()),
        });
    }
    Ok(rows)
}

fn find_archive_v2_blockhash_dir(
    root: &Path,
    epoch: u64,
    allow_root_fallback: bool,
) -> Option<PathBuf> {
    let mut candidates = vec![
        root.join(format!("epoch-{epoch}")),
        root.join(epoch.to_string()),
    ];
    if allow_root_fallback {
        candidates.push(root.to_path_buf());
    }
    candidates
        .into_iter()
        .find(|path| path.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE).is_file())
}

fn build_slot_ranges_v2_from_local_car(
    path: &Path,
    epoch: u64,
    initial_previous_blockhash: Option<[u8; 32]>,
) -> Result<SlotRangesV2Build> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = CarBlockReader::with_capacity(file, 16 << 20);
    reader
        .skip_header()
        .with_context(|| format!("read CAR header from {}", path.display()))?;

    let mut ranges = vec![SlotRangeWithPreviousBlockhash::EMPTY; SLOTS_PER_EPOCH as usize];
    let mut scratch = Vec::with_capacity(256 << 10);
    let mut pending_start: Option<u64> = None;
    let mut pending_blockhash = [0u8; 32];
    let mut previous_blockhash = initial_previous_blockhash;
    let mut last_blockhash = None;

    while let Some(entry) = reader
        .read_entry_payload_with_scratch(&mut scratch)
        .with_context(|| format!("read CAR entry from {}", path.display()))?
    {
        pending_start.get_or_insert(entry.location.car_offset);

        let node = decode_node(entry.payload).with_context(|| {
            format!(
                "decode CAR node at entry {} offset {}",
                entry.location.entry_index, entry.location.car_offset
            )
        })?;

        match node {
            Node::Entry(entry_node) => {
                if entry_node.hash.len() != 32 {
                    return Err(anyhow!(
                        "entry hash length {} at CAR offset {}",
                        entry_node.hash.len(),
                        entry.location.car_offset
                    ));
                }
                pending_blockhash.copy_from_slice(entry_node.hash);
            }
            Node::Block(block) => {
                let start = pending_start.unwrap_or(entry.location.car_offset);
                let end = entry
                    .location
                    .car_offset
                    .checked_add(entry.total_len as u64)
                    .ok_or_else(|| anyhow!("CAR range end overflow"))?;

                if epoch_for_slot(block.slot) == epoch {
                    let len = u32::try_from(end.saturating_sub(start))
                        .context("CAR block range exceeds u32")?;
                    let previous = previous_blockhash
                        .or_else(|| (block.slot == 0).then_some(pending_blockhash))
                        .unwrap_or([0; 32]);
                    let idx = usize::try_from(slot_in_epoch(block.slot))
                        .context("slot-in-epoch exceeds usize")?;
                    ranges[idx] = SlotRangeWithPreviousBlockhash {
                        range: SlotRange { offset: start, len },
                        previous_blockhash: previous,
                    };
                }

                previous_blockhash = Some(pending_blockhash);
                last_blockhash = Some(pending_blockhash);
                pending_start = None;
                pending_blockhash = [0; 32];
            }
            Node::Transaction(_)
            | Node::Rewards(_)
            | Node::DataFrame(_)
            | Node::Subset(_)
            | Node::Epoch(_) => {}
        }
    }

    Ok(SlotRangesV2Build {
        ranges,
        last_blockhash,
    })
}

/* ---------------- CAR header size ---------------- */

fn car_header_total_size(http: &Client, epoch: u64, cars_dir: Option<&Path>) -> Result<u64> {
    if let Some(local_car_path) = find_local_car(epoch, cars_dir) {
        eprintln!(
            "epoch={epoch}: read CAR header from local file: {}",
            local_car_path.display()
        );
        return car_header_total_size_from_local_car(&local_car_path);
    }

    car_header_total_size_from_remote_car(http, epoch)
}

fn find_local_car(epoch: u64, cars_dir: Option<&Path>) -> Option<PathBuf> {
    let cars_dir = cars_dir?;
    let file_name = format!("epoch-{epoch}.car");
    let candidates = [
        cars_dir.join(&file_name),
        cars_dir.join(epoch.to_string()).join(&file_name),
    ];

    candidates.into_iter().find(|path| path.is_file())
}

fn car_header_total_size_from_local_car(path: &Path) -> Result<u64> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut prefix = [0u8; CAR_HEADER_PREFIX_READ_LEN];
    let prefix_len = file
        .read(&mut prefix)
        .with_context(|| format!("read {}", path.display()))?;
    let source = path.display().to_string();

    decode_car_header_total_size(&prefix[..prefix_len], &source)
}

fn car_header_total_size_from_remote_car(http: &Client, epoch: u64) -> Result<u64> {
    // NOTE: This uses the *plain* .car, because you cannot get the uncompressed CAR header
    // out of a .car.zst with a simple Range request.
    let url_car = format!("https://files.old-faithful.net/{epoch}/epoch-{epoch}.car");

    eprintln!(
        "epoch={epoch}: range fetch remote CAR prefix ({} bytes): {}",
        CAR_HEADER_PREFIX_READ_LEN, url_car
    );
    let prefix = http_range_get(http, &url_car, 0, CAR_HEADER_PREFIX_READ_LEN as u64 - 1)
        .with_context(|| format!("range GET {url_car}"))?;

    decode_car_header_total_size(&prefix, &url_car)
}

fn http_range_get(http: &Client, url: &str, start: u64, end: u64) -> Result<Vec<u8>> {
    let mut headers = HeaderMap::new();
    headers.insert(
        RANGE,
        HeaderValue::from_str(&format!("bytes={start}-{end}"))?,
    );

    let resp = http
        .get(url)
        .headers(headers)
        .send()
        .with_context(|| "send request")?;

    // Many servers respond 206 for Range, but some may ignore Range and return 200.
    // Either is fine as long as we got enough bytes.
    let status = resp.status();
    if !(status.is_success()) {
        return Err(anyhow!("HTTP {} for {}", status.as_u16(), url));
    }

    let bytes = resp.bytes().with_context(|| "read response body")?;
    Ok(bytes.to_vec())
}
