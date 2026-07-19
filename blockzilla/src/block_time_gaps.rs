use anyhow::{Context, Result, bail};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN, ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN, ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION,
    ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY,
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_HOT_INDEX_HEADER_LEN,
    ARCHIVE_V2_HOT_INDEX_ROW_LEN, ARCHIVE_V2_RAW_BLOCKS_FILE, BLOCK_TIME_GAP_FILE,
    BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING, BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING,
    BLOCK_TIME_GAP_FLAG_TIME_DECREASING, BLOCK_TIME_GAP_HEADER_LEN, BLOCK_TIME_GAP_MAGIC,
    BLOCK_TIME_GAP_MISSING_TIME, BLOCK_TIME_GAP_ROW_LEN, BLOCK_TIME_GAP_TIME_THRESHOLD_SECS,
    BLOCK_TIME_GAP_VERSION, BlockTimeGapHeader, BlockTimeGapRow, BlockTimeGapSidecar,
    BlockTimeGapSourceKind, deserialize_archive_v2_hot_block_slot_time,
    read_archive_v2_hot_block_index, read_block_time_gap_sidecar, write_block_time_gap_sidecar,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Cursor, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tracing::info;

use crate::SLOTS_PER_EPOCH;

#[derive(Debug, Clone)]
pub(crate) struct BuildBlockTimeGapsConfig<'a> {
    pub input: &'a Path,
    /// Expected epoch. When omitted, a non-empty V3 index derives it from its first slot.
    pub epoch: Option<u64>,
    pub output: Option<&'a Path>,
    pub force: bool,
    pub source: BuildBlockTimeGapsSource,
    pub progress_json: Option<&'a Path>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuildBlockTimeGapsSource {
    /// Prefer the small V3 timestamp index when present, otherwise scan Archive V2 hot blocks.
    Auto,
    /// Require `blockhash_index_v3.bin`.
    BlockhashIndexV3,
    /// Read slot and block time directly from Archive V2 hot blocks.
    ArchiveV2Hot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuildBlockTimeGapsSummary {
    pub source: PathBuf,
    pub output: PathBuf,
    pub blocks: u64,
    pub gaps: u64,
    pub slot_gap_rows: u64,
    pub time_only_gap_rows: u64,
    pub missing_slots: u64,
    pub timed_gaps: u64,
    pub gaps_missing_time: u64,
    pub decreasing_time_gaps: u64,
    pub largest_slot_gap: u64,
    pub longest_elapsed_gap_secs: Option<u64>,
    pub reused: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceFingerprint {
    bytes: u64,
    modified: Option<SystemTime>,
}

#[derive(Debug)]
enum ResolvedSource {
    BlockhashIndexV3 {
        path: PathBuf,
    },
    ArchiveV2Hot {
        root: PathBuf,
        index: PathBuf,
        blocks: PathBuf,
        raw_blocks: bool,
    },
}

impl ResolvedSource {
    fn display_path(&self) -> &Path {
        match self {
            Self::BlockhashIndexV3 { path } => path,
            Self::ArchiveV2Hot { root, .. } => root,
        }
    }

    fn default_output(&self) -> PathBuf {
        match self {
            Self::BlockhashIndexV3 { path } => path.with_file_name(BLOCK_TIME_GAP_FILE),
            Self::ArchiveV2Hot { root, .. } => root.join(BLOCK_TIME_GAP_FILE),
        }
    }

    fn source_paths(&self) -> Vec<&Path> {
        match self {
            Self::BlockhashIndexV3 { path } => vec![path],
            Self::ArchiveV2Hot { index, blocks, .. } => vec![index, blocks],
        }
    }
}

const MAX_ARCHIVE_BLOCK_BYTES: usize = 512 << 20;
const MAX_ARCHIVE_SLOT_TIME_PREFIX_BYTES: u64 = 64;
const ARCHIVE_PROGRESS_INTERVAL: Duration = Duration::from_secs(3);
const ARCHIVE_FINGERPRINT_DOMAIN: &[u8] = b"blockzilla:block-time-gaps:archive-v2-hot:v1";

fn max_archive_index_bytes() -> Result<u64> {
    (ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            SLOTS_PER_EPOCH
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .context("Archive V2 maximum index length overflow")?,
        )
        .context("Archive V2 maximum index length overflow")
}

#[derive(Debug, Serialize)]
struct GapBuildProgress<'a> {
    state: &'a str,
    epoch: Option<u64>,
    blocks_done: u64,
    blocks_total: u64,
    source_bytes_done: u64,
    source_bytes_total: u64,
    elapsed_seconds: f64,
    eta_seconds: Option<f64>,
    updated_unix_seconds: u64,
}

pub(crate) fn build_block_time_gaps(
    config: BuildBlockTimeGapsConfig<'_>,
) -> Result<BuildBlockTimeGapsSummary> {
    let source = resolve_source(config.input, config.source)?;
    let output = config
        .output
        .map(Path::to_path_buf)
        .unwrap_or_else(|| source.default_output());
    ensure_distinct_source_and_output(&source, &output)?;
    if let Some(progress_json) = config.progress_json {
        ensure_distinct_progress_path(&source, &output, progress_json)?;
    }
    let _output_lock = acquire_output_lock(&output)?;

    let sidecar = match &source {
        ResolvedSource::BlockhashIndexV3 { path } => {
            let before = source_fingerprint(path)?;
            let bytes = fs::read(path)
                .with_context(|| format!("read block-time gap source {}", path.display()))?;
            let after = source_fingerprint(path)?;
            anyhow::ensure!(
                before == after && before.bytes == bytes.len() as u64,
                "source {} changed while it was being read",
                path.display()
            );
            let source_sha256: [u8; 32] = Sha256::digest(&bytes).into();
            collect_from_blockhash_index_v3(&bytes, config.epoch, source_sha256)
                .with_context(|| format!("scan {}", path.display()))?
        }
        ResolvedSource::ArchiveV2Hot {
            index,
            blocks,
            raw_blocks,
            ..
        } => collect_from_archive_v2_hot(
            index,
            blocks,
            *raw_blocks,
            config.epoch,
            config.progress_json,
        )
        .with_context(|| format!("scan Archive V2 source {}", source.display_path().display()))?,
    };

    let reused = if output.exists() {
        match read_sidecar_path(&output) {
            Ok(existing) if existing == sidecar => true,
            // A no-force fallback scan may encounter a valid sidecar produced from a source that
            // is no longer retained. Preserve it only when every semantic field and row agrees.
            // Same-kind fingerprint changes remain stale, and --force keeps its documented meaning.
            Ok(existing)
                if !config.force
                    && existing.header.source_kind != sidecar.header.source_kind
                    && sidecars_semantically_equal(&existing, &sidecar) =>
            {
                true
            }
            Ok(_) if config.force => {
                write_sidecar_atomic(&output, &sidecar)?;
                false
            }
            Ok(_) => {
                bail!(
                    "{} already exists but does not match {}; pass --force to replace it",
                    output.display(),
                    source.display_path().display()
                );
            }
            Err(error) if config.force => {
                info!(
                    "Replacing unreadable block-time gap sidecar {} because --force was supplied: {error:#}",
                    output.display()
                );
                write_sidecar_atomic(&output, &sidecar)?;
                false
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "existing sidecar {} is unreadable; pass --force to replace it",
                        output.display()
                    )
                });
            }
        }
    } else {
        write_sidecar_atomic(&output, &sidecar)?;
        false
    };

    let summary = summarize(source.display_path(), &output, &sidecar, reused);
    info!(
        "Block-time gap sidecar {}: epoch={} blocks={} gap_rows={} slot_gap_rows={} time_only_gap_rows={} missing_slots={} timed_gaps={} gaps_missing_time={} decreasing_time_gaps={} largest_slot_gap={} longest_elapsed_gap_secs={} source={} output={}",
        if reused { "reused" } else { "complete" },
        sidecar.header.epoch,
        summary.blocks,
        summary.gaps,
        summary.slot_gap_rows,
        summary.time_only_gap_rows,
        summary.missing_slots,
        summary.timed_gaps,
        summary.gaps_missing_time,
        summary.decreasing_time_gaps,
        summary.largest_slot_gap,
        summary
            .longest_elapsed_gap_secs
            .map_or_else(|| "unavailable".to_string(), |seconds| seconds.to_string()),
        summary.source.display(),
        summary.output.display()
    );
    Ok(summary)
}

pub(crate) fn verify_block_time_gaps(path: &Path, expected_epoch: Option<u64>) -> Result<()> {
    let sidecar = read_sidecar_path(path)?;
    if let Some(expected_epoch) = expected_epoch {
        anyhow::ensure!(
            sidecar.header.epoch == expected_epoch,
            "{} belongs to epoch {}, expected {expected_epoch}",
            path.display(),
            sidecar.header.epoch
        );
    }
    info!(
        "Block-time gap sidecar valid: epoch={} source_kind={:?} blocks={} gap_rows={} missing_slots={} source_bytes={} path={}",
        sidecar.header.epoch,
        sidecar.header.source_kind,
        sidecar.header.block_count,
        sidecar.header.gap_count,
        sidecar.header.missing_slot_count,
        sidecar.header.source_bytes,
        path.display()
    );
    Ok(())
}

fn resolve_source(input: &Path, preference: BuildBlockTimeGapsSource) -> Result<ResolvedSource> {
    let input_is_v3 = input
        .file_name()
        .is_some_and(|name| name == ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
    anyhow::ensure!(
        input.is_dir() || (input_is_v3 && input.is_file()),
        "input {} must be an existing epoch directory or {} file",
        input.display(),
        ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE
    );
    anyhow::ensure!(
        preference != BuildBlockTimeGapsSource::ArchiveV2Hot || input.is_dir(),
        "Archive V2 source requires an epoch directory, got {}",
        input.display()
    );
    let root = if input.is_dir() {
        input.to_path_buf()
    } else {
        input
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    };
    let v3_path = if input_is_v3 {
        input.to_path_buf()
    } else {
        root.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE)
    };

    if preference == BuildBlockTimeGapsSource::BlockhashIndexV3
        || (preference == BuildBlockTimeGapsSource::Auto && v3_path.is_file())
    {
        return Ok(ResolvedSource::BlockhashIndexV3 { path: v3_path });
    }

    anyhow::ensure!(
        preference != BuildBlockTimeGapsSource::Auto || input.is_dir() || !input_is_v3,
        "{} does not exist and an Archive V2 directory was not supplied",
        v3_path.display()
    );
    let index = root.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    anyhow::ensure!(
        index.is_file(),
        "Archive V2 index {} does not exist",
        index.display()
    );
    ensure_archive_index_size(&index)?;
    let parsed = read_archive_v2_hot_block_index(&index)
        .with_context(|| format!("read Archive V2 index {}", index.display()))?;
    let raw_blocks = parsed.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0;
    let blocks = if raw_blocks {
        root.join(ARCHIVE_V2_RAW_BLOCKS_FILE)
    } else {
        root.join(ARCHIVE_V2_BLOCKS_FILE)
    };
    anyhow::ensure!(
        blocks.is_file(),
        "Archive V2 block file {} does not exist",
        blocks.display()
    );
    Ok(ResolvedSource::ArchiveV2Hot {
        root,
        index,
        blocks,
        raw_blocks,
    })
}

fn ensure_distinct_source_and_output(source: &ResolvedSource, output: &Path) -> Result<()> {
    let output_parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_parent)
        .with_context(|| format!("create output directory {}", output_parent.display()))?;
    let canonical_output = if output.exists() {
        fs::canonicalize(output)
            .with_context(|| format!("resolve existing output path {}", output.display()))?
    } else {
        let file_name = output
            .file_name()
            .context("block-time gap output path must name a file")?;
        fs::canonicalize(output_parent)
            .with_context(|| format!("resolve output directory {}", output_parent.display()))?
            .join(file_name)
    };
    for source_path in source.source_paths() {
        let canonical_source = fs::canonicalize(source_path)
            .with_context(|| format!("resolve source path {}", source_path.display()))?;
        anyhow::ensure!(
            canonical_source != canonical_output,
            "block-time gap sidecar must not replace its source {}",
            canonical_source.display()
        );
    }
    Ok(())
}

fn ensure_distinct_progress_path(
    source: &ResolvedSource,
    output: &Path,
    progress: &Path,
) -> Result<()> {
    let canonical_progress = canonical_destination(progress, "progress")?;
    let canonical_output = canonical_destination(output, "output")?;
    anyhow::ensure!(
        canonical_progress != canonical_output,
        "block-time gap progress path must not replace output {}",
        canonical_output.display()
    );
    let canonical_lock = canonical_destination(&output_lock_path(output), "output lock")?;
    anyhow::ensure!(
        canonical_progress != canonical_lock,
        "block-time gap progress path must not replace output lock {}",
        canonical_lock.display()
    );
    for source_path in source.source_paths() {
        let canonical_source = fs::canonicalize(source_path)
            .with_context(|| format!("resolve source path {}", source_path.display()))?;
        anyhow::ensure!(
            canonical_progress != canonical_source,
            "block-time gap progress path must not replace source {}",
            canonical_source.display()
        );
    }
    Ok(())
}

fn canonical_destination(path: &Path, label: &str) -> Result<PathBuf> {
    if path.exists() {
        return fs::canonicalize(path)
            .with_context(|| format!("resolve existing {label} path {}", path.display()));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create {label} directory {}", parent.display()))?;
    let name = path
        .file_name()
        .with_context(|| format!("{label} path must name a file"))?;
    Ok(fs::canonicalize(parent)
        .with_context(|| format!("resolve {label} directory {}", parent.display()))?
        .join(name))
}

fn output_lock_path(output: &Path) -> PathBuf {
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(BLOCK_TIME_GAP_FILE);
    parent.join(format!(".{name}.lock"))
}

fn acquire_output_lock(output: &Path) -> Result<File> {
    let lock_path = output_lock_path(output);
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("open output lock {}", lock_path.display()))?;
    lock_file_blocking(&file).with_context(|| format!("lock output {}", lock_path.display()))?;
    Ok(file)
}

#[cfg(unix)]
fn lock_file_blocking(file: &File) -> std::io::Result<()> {
    loop {
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(not(unix))]
fn lock_file_blocking(_file: &File) -> std::io::Result<()> {
    Ok(())
}

fn source_fingerprint(path: &Path) -> Result<SourceFingerprint> {
    let metadata =
        fs::metadata(path).with_context(|| format!("read source metadata {}", path.display()))?;
    anyhow::ensure!(metadata.is_file(), "{} is not a file", path.display());
    Ok(SourceFingerprint {
        bytes: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

fn ensure_archive_index_size(path: &Path) -> Result<SourceFingerprint> {
    let fingerprint = source_fingerprint(path)?;
    let maximum = max_archive_index_bytes()?;
    anyhow::ensure!(
        fingerprint.bytes <= maximum,
        "Archive V2 index {} has {} bytes, exceeding the {maximum} byte epoch limit",
        path.display(),
        fingerprint.bytes
    );
    Ok(fingerprint)
}

fn collect_from_blockhash_index_v3(
    bytes: &[u8],
    expected_epoch: Option<u64>,
    source_sha256: [u8; 32],
) -> Result<BlockTimeGapSidecar> {
    anyhow::ensure!(
        bytes.len() >= ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN,
        "V3 blockhash index is shorter than its {} byte header",
        ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN
    );
    let mut reader = Cursor::new(bytes);
    let mut magic = [0u8; 8];
    reader.read_exact(&mut magic)?;
    anyhow::ensure!(
        magic == *ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC,
        "invalid V3 blockhash index magic"
    );
    let version = read_u16(&mut reader)?;
    anyhow::ensure!(
        version == ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION,
        "unsupported V3 blockhash index version {version}"
    );
    let row_len = usize::from(read_u16(&mut reader)?);
    anyhow::ensure!(
        row_len == ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN,
        "invalid V3 blockhash index row length {row_len}"
    );
    let block_count = read_u64(&mut reader)?;
    let expected_len = u64::try_from(ARCHIVE_V2_BLOCKHASH_INDEX_V3_HEADER_LEN)?
        .checked_add(
            block_count
                .checked_mul(u64::try_from(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN)?)
                .context("V3 blockhash index length overflow")?,
        )
        .context("V3 blockhash index length overflow")?;
    anyhow::ensure!(
        expected_len == bytes.len() as u64,
        "V3 blockhash index declares {block_count} rows ({expected_len} bytes) but file has {} bytes",
        bytes.len()
    );

    anyhow::ensure!(
        block_count <= SLOTS_PER_EPOCH,
        "V3 blockhash index declares {block_count} blocks, exceeding {SLOTS_PER_EPOCH} slots per epoch"
    );
    let capacity = usize::try_from(block_count).context("V3 block count exceeds usize")?;
    let mut points = Vec::new();
    points
        .try_reserve_exact(capacity)
        .context("reserve V3 slot/time points")?;

    for row_index in 0..block_count {
        let slot =
            read_u64(&mut reader).with_context(|| format!("read slot from V3 row {row_index}"))?;
        let mut _blockhash = [0u8; 32];
        reader
            .read_exact(&mut _blockhash)
            .with_context(|| format!("read blockhash from V3 row {row_index}"))?;
        let raw_block_time = read_i64(&mut reader)
            .with_context(|| format!("read block time from V3 row {row_index}"))?;
        let block_time = if raw_block_time == 0 {
            BLOCK_TIME_GAP_MISSING_TIME
        } else {
            raw_block_time
        };
        points.push((slot, block_time));
    }

    collect_from_slot_times(
        &points,
        expected_epoch,
        BlockTimeGapSourceKind::BlockhashIndexV3,
        bytes.len() as u64,
        source_sha256,
        "V3",
    )
}

fn collect_from_archive_v2_hot(
    index_path: &Path,
    blocks_path: &Path,
    raw_blocks: bool,
    expected_epoch: Option<u64>,
    progress_json: Option<&Path>,
) -> Result<BlockTimeGapSidecar> {
    let index_before = ensure_archive_index_size(index_path)?;
    let index_bytes = fs::read(index_path)
        .with_context(|| format!("read Archive V2 index {}", index_path.display()))?;
    let index_after_read = source_fingerprint(index_path)?;
    anyhow::ensure!(
        index_before == index_after_read && index_before.bytes == index_bytes.len() as u64,
        "Archive V2 index {} changed while it was being read",
        index_path.display()
    );
    let index = read_archive_v2_hot_block_index(index_path)
        .with_context(|| format!("parse Archive V2 index {}", index_path.display()))?;
    let known_flags = ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY | ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS;
    anyhow::ensure!(
        index.flags & !known_flags == 0,
        "Archive V2 index has unknown flags {:#x}",
        index.flags & !known_flags
    );
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY == 0,
        "Archive V2 hot blocks require a zstd dictionary; dictionary-backed extraction is not supported yet"
    );
    anyhow::ensure!(
        raw_blocks == (index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0),
        "Archive V2 source mode changed while resolving the index"
    );
    anyhow::ensure!(
        index.rows.len() as u64 <= SLOTS_PER_EPOCH,
        "Archive V2 index has {} rows, exceeding {SLOTS_PER_EPOCH} slots per epoch",
        index.rows.len()
    );

    let blocks_before = source_fingerprint(blocks_path)?;
    anyhow::ensure!(
        blocks_before.bytes == index.blob_file_bytes,
        "Archive V2 index expects {} blob bytes but {} has {}",
        index.blob_file_bytes,
        blocks_path.display(),
        blocks_before.bytes
    );
    let source_bytes = index_before
        .bytes
        .checked_add(blocks_before.bytes)
        .context("Archive V2 source byte count overflow")?;

    let mut resolved_epoch = expected_epoch;
    let mut epoch_range = expected_epoch.map(epoch_slot_range).transpose()?;
    let mut expected_offset = 0u64;
    let mut previous_slot = None;
    for (row_index, row) in index.rows.iter().enumerate() {
        let expected_block_id =
            u32::try_from(row_index).context("Archive V2 row index exceeds u32")?;
        anyhow::ensure!(
            row.block_id == expected_block_id,
            "Archive V2 row {row_index} has block_id {}, expected {expected_block_id}",
            row.block_id
        );
        anyhow::ensure!(
            row.compressed_offset == expected_offset,
            "Archive V2 row {row_index} begins at {}, expected contiguous offset {expected_offset}",
            row.compressed_offset
        );
        anyhow::ensure!(
            row.compressed_len != 0 && row.uncompressed_len != 0,
            "Archive V2 row {row_index} has an empty block frame"
        );
        anyhow::ensure!(
            row.compressed_len as usize <= MAX_ARCHIVE_BLOCK_BYTES
                && row.uncompressed_len as usize <= MAX_ARCHIVE_BLOCK_BYTES,
            "Archive V2 row {row_index} exceeds the {} MiB safety limit",
            MAX_ARCHIVE_BLOCK_BYTES >> 20
        );
        if raw_blocks {
            anyhow::ensure!(
                row.compressed_len == row.uncompressed_len,
                "raw Archive V2 row {row_index} has differing stored and decoded lengths"
            );
        }
        expected_offset = expected_offset
            .checked_add(u64::from(row.compressed_len))
            .context("Archive V2 block offset overflow")?;
        if let Some(previous_slot) = previous_slot {
            anyhow::ensure!(
                row.slot > previous_slot,
                "Archive V2 row {row_index} slot {} is not strictly after {previous_slot}",
                row.slot
            );
        }
        let epoch = *resolved_epoch.get_or_insert(row.slot / SLOTS_PER_EPOCH);
        let (epoch_start, epoch_end) = *epoch_range.get_or_insert(epoch_slot_range(epoch)?);
        anyhow::ensure!(
            (epoch_start..epoch_end).contains(&row.slot),
            "Archive V2 row {row_index} slot {} is outside epoch {epoch} range {epoch_start}..{epoch_end}",
            row.slot
        );
        previous_slot = Some(row.slot);
    }
    anyhow::ensure!(
        expected_offset == index.blob_file_bytes,
        "Archive V2 rows cover {expected_offset} bytes but the index declares {}",
        index.blob_file_bytes
    );

    let started = Instant::now();
    let blocks_total = index.rows.len() as u64;
    write_gap_progress(
        progress_json,
        "running",
        resolved_epoch,
        0,
        blocks_total,
        index_before.bytes,
        source_bytes,
        started,
    )?;

    let mut hasher = Sha256::new();
    hasher.update((ARCHIVE_FINGERPRINT_DOMAIN.len() as u64).to_le_bytes());
    hasher.update(ARCHIVE_FINGERPRINT_DOMAIN);
    hash_component_header(&mut hasher, b"archive-v2-blocks.index", index_before.bytes);
    hasher.update(&index_bytes);
    hash_component_header(
        &mut hasher,
        if raw_blocks {
            b"archive-v2-blocks.wincode"
        } else {
            b"archive-v2-blocks.zstd"
        },
        blocks_before.bytes,
    );

    let file = File::open(blocks_path)
        .with_context(|| format!("open Archive V2 blocks {}", blocks_path.display()))?;
    let mut reader = BufReader::with_capacity(8 << 20, file);
    let mut stored = Vec::new();
    let mut points = Vec::new();
    points
        .try_reserve_exact(index.rows.len())
        .context("reserve Archive V2 slot/time points")?;
    let mut bytes_done = index_before.bytes;
    let mut last_progress = Instant::now();

    for (row_index, row) in index.rows.iter().enumerate() {
        let stored_len = row.compressed_len as usize;
        if stored.capacity() < stored_len {
            stored.clear();
            stored
                .try_reserve_exact(stored_len)
                .with_context(|| format!("reserve Archive V2 row {row_index} input buffer"))?;
        }
        stored.resize(stored_len, 0);
        reader.read_exact(&mut stored).with_context(|| {
            format!("read Archive V2 row {row_index} block_id {}", row.block_id)
        })?;
        hasher.update(&stored);
        bytes_done = bytes_done
            .checked_add(stored_len as u64)
            .context("Archive V2 progress byte count overflow")?;

        let decoded_slot_time = if raw_blocks {
            deserialize_archive_v2_hot_block_slot_time(&stored)
        } else {
            // Slot and block_time are the first fields in every supported hot-block schema.
            // Stop after that bounded prefix instead of materializing the transaction payload.
            let mut decoder = zstd::stream::read::Decoder::with_buffer(Cursor::new(&stored))
                .with_context(|| format!("open zstd frame for Archive V2 row {row_index}"))?;
            let mut prefix = Vec::with_capacity(MAX_ARCHIVE_SLOT_TIME_PREFIX_BYTES as usize);
            decoder
                .by_ref()
                .take(MAX_ARCHIVE_SLOT_TIME_PREFIX_BYTES)
                .read_to_end(&mut prefix)
                .with_context(|| {
                    format!(
                        "decompress slot/time prefix from Archive V2 row {row_index} block_id {}",
                        row.block_id
                    )
                })?;
            let remaining_decoded = std::io::copy(&mut decoder, &mut std::io::sink())
                .with_context(|| format!("validate zstd frame for Archive V2 row {row_index}"))?;
            let decoded_len = (prefix.len() as u64)
                .checked_add(remaining_decoded)
                .context("Archive V2 decoded byte count overflow")?;
            anyhow::ensure!(
                decoded_len == u64::from(row.uncompressed_len),
                "Archive V2 row {row_index} decoded {decoded_len} bytes, expected {}",
                row.uncompressed_len
            );
            deserialize_archive_v2_hot_block_slot_time(&prefix)
        };
        let (decoded_slot, block_time) = decoded_slot_time.with_context(|| {
            format!(
                "decode slot/time prefix from Archive V2 row {row_index} block_id {}",
                row.block_id
            )
        })?;
        anyhow::ensure!(
            decoded_slot == row.slot,
            "Archive V2 row {row_index} index slot {} does not match decoded slot {decoded_slot}",
            row.slot
        );
        points.push((
            decoded_slot,
            match block_time {
                None | Some(0) => BLOCK_TIME_GAP_MISSING_TIME,
                Some(block_time) => block_time,
            },
        ));

        if last_progress.elapsed() >= ARCHIVE_PROGRESS_INTERVAL {
            let blocks_done = row_index as u64 + 1;
            write_gap_progress(
                progress_json,
                "running",
                resolved_epoch,
                blocks_done,
                blocks_total,
                bytes_done,
                source_bytes,
                started,
            )?;
            let elapsed = started.elapsed().as_secs_f64();
            let mib_per_second = if elapsed > 0.0 {
                (bytes_done - index_before.bytes) as f64 / elapsed / (1024.0 * 1024.0)
            } else {
                0.0
            };
            let eta = estimate_eta_seconds(bytes_done, source_bytes, elapsed);
            info!(
                "Archive V2 slot/time scan: blocks={blocks_done}/{blocks_total} bytes={bytes_done}/{source_bytes} throughput_MiB_s={mib_per_second:.2} eta_seconds={}",
                eta.map_or_else(|| "unavailable".to_string(), |value| format!("{value:.0}"))
            );
            last_progress = Instant::now();
        }
    }
    let mut trailing = [0u8; 1];
    anyhow::ensure!(
        reader
            .read(&mut trailing)
            .context("check Archive V2 trailing bytes")?
            == 0,
        "Archive V2 block file has trailing bytes beyond the indexed rows"
    );
    drop(reader);

    let index_after_scan = source_fingerprint(index_path)?;
    let blocks_after = source_fingerprint(blocks_path)?;
    anyhow::ensure!(
        index_before == index_after_scan && blocks_before == blocks_after,
        "Archive V2 source changed while slot/time rows were scanned"
    );
    anyhow::ensure!(
        bytes_done == source_bytes,
        "Archive V2 scan read {bytes_done} source bytes, expected {source_bytes}"
    );
    let source_sha256: [u8; 32] = hasher.finalize().into();
    write_gap_progress(
        progress_json,
        "scanned",
        resolved_epoch,
        blocks_total,
        blocks_total,
        source_bytes,
        source_bytes,
        started,
    )?;
    collect_from_slot_times(
        &points,
        expected_epoch,
        BlockTimeGapSourceKind::ArchiveV2Hot,
        source_bytes,
        source_sha256,
        "Archive V2",
    )
}

fn collect_from_slot_times(
    points: &[(u64, i64)],
    expected_epoch: Option<u64>,
    source_kind: BlockTimeGapSourceKind,
    source_bytes: u64,
    source_sha256: [u8; 32],
    source_label: &str,
) -> Result<BlockTimeGapSidecar> {
    anyhow::ensure!(
        points.len() as u64 <= SLOTS_PER_EPOCH,
        "{source_label} source has {} blocks, exceeding {SLOTS_PER_EPOCH} slots per epoch",
        points.len()
    );
    let mut epoch = expected_epoch;
    let mut epoch_range = expected_epoch.map(epoch_slot_range).transpose()?;
    let mut rows = Vec::new();
    let mut previous: Option<(u64, i64)> = None;
    let mut first: Option<(u64, i64)> = None;
    let mut last = None;
    let mut missing_slot_count = 0u64;
    let mut timed_gap_count = 0u64;
    let mut missing_time_gap_count = 0u64;
    let mut decreasing_time_gap_count = 0u64;

    for (row_index, &(slot, block_time)) in points.iter().enumerate() {
        let resolved_epoch = *epoch.get_or_insert(slot / SLOTS_PER_EPOCH);
        let (epoch_start, epoch_end) =
            *epoch_range.get_or_insert(epoch_slot_range(resolved_epoch)?);
        anyhow::ensure!(
            (epoch_start..epoch_end).contains(&slot),
            "{source_label} row {row_index} slot {slot} is outside epoch {resolved_epoch} range {epoch_start}..{epoch_end}"
        );
        if let Some((previous_slot, previous_time)) = previous {
            anyhow::ensure!(
                slot > previous_slot,
                "{source_label} row {row_index} slot {slot} is not strictly after {previous_slot}"
            );
            let missing_slots = slot - previous_slot - 1;
            let both_times_available = previous_time != BLOCK_TIME_GAP_MISSING_TIME
                && block_time != BLOCK_TIME_GAP_MISSING_TIME;
            let time_delta =
                both_times_available.then(|| i128::from(block_time) - i128::from(previous_time));
            let has_time_discontinuity = time_delta.is_some_and(|delta| {
                delta < 0 || delta > i128::from(BLOCK_TIME_GAP_TIME_THRESHOLD_SECS)
            });
            if missing_slots > 0 || has_time_discontinuity {
                missing_slot_count = missing_slot_count
                    .checked_add(missing_slots)
                    .context("missing slot count overflow")?;
                let mut flags = 0u32;
                if previous_time == BLOCK_TIME_GAP_MISSING_TIME {
                    flags |= BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING;
                }
                if block_time == BLOCK_TIME_GAP_MISSING_TIME {
                    flags |= BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING;
                }
                if flags
                    & (BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING
                        | BLOCK_TIME_GAP_FLAG_NEXT_TIME_MISSING)
                    != 0
                {
                    missing_time_gap_count = missing_time_gap_count
                        .checked_add(1)
                        .context("missing-time gap count overflow")?;
                } else if block_time < previous_time {
                    flags |= BLOCK_TIME_GAP_FLAG_TIME_DECREASING;
                    decreasing_time_gap_count = decreasing_time_gap_count
                        .checked_add(1)
                        .context("decreasing time gap count overflow")?;
                } else {
                    timed_gap_count = timed_gap_count
                        .checked_add(1)
                        .context("timed gap count overflow")?;
                }
                rows.push(BlockTimeGapRow {
                    previous_slot,
                    next_slot: slot,
                    previous_block_time: previous_time,
                    next_block_time: block_time,
                    flags,
                    reserved: 0,
                });
            }
        }
        let point = (slot, block_time);
        first.get_or_insert(point);
        previous = Some(point);
        last = Some(point);
    }

    let epoch = epoch.with_context(|| {
        format!(
            "cannot infer an epoch from an empty {source_label} source; pass --epoch explicitly"
        )
    })?;
    let _ = epoch_range.get_or_insert(epoch_slot_range(epoch)?);
    let (first_slot, first_block_time, last_slot, last_block_time) = match (first, last) {
        (Some((first_slot, first_block_time)), Some((last_slot, last_block_time))) => {
            (first_slot, first_block_time, last_slot, last_block_time)
        }
        (None, None) => (
            0,
            BLOCK_TIME_GAP_MISSING_TIME,
            0,
            BLOCK_TIME_GAP_MISSING_TIME,
        ),
        _ => unreachable!("first and last block points are updated together"),
    };
    Ok(BlockTimeGapSidecar {
        header: BlockTimeGapHeader {
            magic: BLOCK_TIME_GAP_MAGIC,
            version: BLOCK_TIME_GAP_VERSION,
            header_len: BLOCK_TIME_GAP_HEADER_LEN as u16,
            row_len: BLOCK_TIME_GAP_ROW_LEN as u16,
            flags: 0,
            epoch,
            slots_per_epoch: SLOTS_PER_EPOCH,
            source_kind,
            time_gap_threshold_secs: u32::try_from(BLOCK_TIME_GAP_TIME_THRESHOLD_SECS)
                .context("block-time gap threshold does not fit u32")?,
            source_bytes,
            source_sha256,
            block_count: points.len() as u64,
            gap_count: rows.len() as u64,
            missing_slot_count,
            first_slot,
            first_block_time,
            last_slot,
            last_block_time,
            timed_gap_count,
            missing_time_gap_count,
            decreasing_time_gap_count,
        },
        rows,
    })
}

fn hash_component_header(hasher: &mut Sha256, label: &[u8], bytes: u64) {
    hasher.update((label.len() as u64).to_le_bytes());
    hasher.update(label);
    hasher.update(bytes.to_le_bytes());
}

#[allow(clippy::too_many_arguments)]
fn write_gap_progress(
    path: Option<&Path>,
    state: &str,
    epoch: Option<u64>,
    blocks_done: u64,
    blocks_total: u64,
    source_bytes_done: u64,
    source_bytes_total: u64,
    started: Instant,
) -> Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let progress = GapBuildProgress {
        state,
        epoch,
        blocks_done,
        blocks_total,
        source_bytes_done,
        source_bytes_total,
        elapsed_seconds,
        eta_seconds: estimate_eta_seconds(source_bytes_done, source_bytes_total, elapsed_seconds),
        updated_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create progress directory {}", parent.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("block-time-gap-progress.json");
    let temp = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let bytes = serde_json::to_vec(&progress).context("encode block-time gap progress JSON")?;
    fs::write(&temp, bytes).with_context(|| format!("write progress {}", temp.display()))?;
    fs::rename(&temp, path).with_context(|| {
        format!(
            "publish block-time gap progress {} -> {}",
            temp.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn estimate_eta_seconds(done: u64, total: u64, elapsed_seconds: f64) -> Option<f64> {
    if done == 0 || done > total || elapsed_seconds <= 0.0 {
        return None;
    }
    Some((total - done) as f64 * elapsed_seconds / done as f64)
}

fn sidecars_semantically_equal(
    existing: &BlockTimeGapSidecar,
    candidate: &BlockTimeGapSidecar,
) -> bool {
    let left = &existing.header;
    let right = &candidate.header;
    left.magic == right.magic
        && left.version == right.version
        && left.header_len == right.header_len
        && left.row_len == right.row_len
        && left.flags == right.flags
        && left.epoch == right.epoch
        && left.slots_per_epoch == right.slots_per_epoch
        && left.time_gap_threshold_secs == right.time_gap_threshold_secs
        && left.block_count == right.block_count
        && left.gap_count == right.gap_count
        && left.missing_slot_count == right.missing_slot_count
        && left.first_slot == right.first_slot
        && left.first_block_time == right.first_block_time
        && left.last_slot == right.last_slot
        && left.last_block_time == right.last_block_time
        && left.timed_gap_count == right.timed_gap_count
        && left.missing_time_gap_count == right.missing_time_gap_count
        && left.decreasing_time_gap_count == right.decreasing_time_gap_count
        && existing.rows == candidate.rows
}

fn epoch_slot_range(epoch: u64) -> Result<(u64, u64)> {
    let start = epoch
        .checked_mul(SLOTS_PER_EPOCH)
        .context("epoch slot range overflow")?;
    let end = start
        .checked_add(SLOTS_PER_EPOCH)
        .context("epoch slot range overflow")?;
    Ok((start, end))
}

fn read_sidecar_path(path: &Path) -> Result<BlockTimeGapSidecar> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    read_block_time_gap_sidecar(BufReader::new(file))
        .with_context(|| format!("read {}", path.display()))
}

fn write_sidecar_atomic(path: &Path, sidecar: &BlockTimeGapSidecar) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .with_context(|| format!("create output directory {}", parent.display()))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(BLOCK_TIME_GAP_FILE);
    let temp_path = parent.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_nanos())
    ));

    let result = (|| -> Result<()> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .with_context(|| format!("create temporary sidecar {}", temp_path.display()))?;
        let mut writer = BufWriter::new(file);
        write_block_time_gap_sidecar(&mut writer, sidecar)
            .with_context(|| format!("write temporary sidecar {}", temp_path.display()))?;
        writer
            .flush()
            .with_context(|| format!("flush temporary sidecar {}", temp_path.display()))?;
        writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("sync temporary sidecar {}", temp_path.display()))?;
        drop(writer);
        fs::rename(&temp_path, path).with_context(|| {
            format!(
                "publish block-time gap sidecar {} -> {}",
                temp_path.display(),
                path.display()
            )
        })?;
        File::open(parent)
            .with_context(|| format!("open output directory {}", parent.display()))?
            .sync_all()
            .with_context(|| format!("sync output directory {}", parent.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

fn summarize(
    source: &Path,
    output: &Path,
    sidecar: &BlockTimeGapSidecar,
    reused: bool,
) -> BuildBlockTimeGapsSummary {
    let largest_slot_gap = sidecar
        .rows
        .iter()
        .map(|row| row.next_slot - row.previous_slot - 1)
        .max()
        .unwrap_or(0);
    let slot_gap_rows = sidecar
        .rows
        .iter()
        .filter(|row| row.next_slot > row.previous_slot + 1)
        .count() as u64;
    let longest_elapsed_gap_secs = sidecar
        .rows
        .iter()
        .filter_map(BlockTimeGapRow::elapsed_seconds)
        .max();
    BuildBlockTimeGapsSummary {
        source: source.to_path_buf(),
        output: output.to_path_buf(),
        blocks: sidecar.header.block_count,
        gaps: sidecar.header.gap_count,
        slot_gap_rows,
        time_only_gap_rows: sidecar.header.gap_count - slot_gap_rows,
        missing_slots: sidecar.header.missing_slot_count,
        timed_gaps: sidecar.header.timed_gap_count,
        gaps_missing_time: sidecar.header.missing_time_gap_count,
        decreasing_time_gaps: sidecar.header.decreasing_time_gap_count,
        largest_slot_gap,
        longest_elapsed_gap_secs,
        reused,
    }
}

fn read_u16(reader: &mut impl Read) -> Result<u16> {
    let mut bytes = [0u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_i64(reader: &mut impl Read) -> Result<i64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(i64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let id = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "blockzilla-block-time-gaps-{}-{id}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write_v3(path: &Path, rows: &[(u64, i64)]) {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC);
        bytes.extend_from_slice(&ARCHIVE_V2_BLOCKHASH_INDEX_V3_VERSION.to_le_bytes());
        bytes.extend_from_slice(&(ARCHIVE_V2_BLOCKHASH_INDEX_V3_ROW_LEN as u16).to_le_bytes());
        bytes.extend_from_slice(&(rows.len() as u64).to_le_bytes());
        for (index, (slot, block_time)) in rows.iter().enumerate() {
            bytes.extend_from_slice(&slot.to_le_bytes());
            bytes.extend_from_slice(&[index as u8; 32]);
            bytes.extend_from_slice(&block_time.to_le_bytes());
        }
        fs::write(path, bytes).unwrap();
    }

    fn write_hot_archive(path: &Path, rows: &[(u64, Option<i64>)]) {
        use blockzilla_format::{
            ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndexRow,
            wincode_leb128_config, write_archive_v2_hot_block_index,
        };

        let mut blob = Vec::new();
        let mut index_rows = Vec::new();
        for (block_id, &(slot, block_time)) in rows.iter().enumerate() {
            let block = ArchiveV2HotBlockBlob {
                header: ArchiveV2HotBlockHeader {
                    slot,
                    parent_slot: slot.saturating_sub(1),
                    blockhash_id: block_id as u32,
                    previous_blockhash_id: block_id.saturating_sub(1) as u32,
                    block_time,
                    block_height: Some(slot),
                    rewards: None,
                },
                tx_count: 0,
                tx_rows: Vec::new(),
                message_bytes: Vec::new(),
                metadata_bytes: Vec::new(),
            };
            let encoded = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            let compressed = zstd::bulk::compress(&encoded, 1).unwrap();
            index_rows.push(ArchiveV2HotBlockIndexRow {
                block_id: block_id as u32,
                slot,
                compressed_offset: blob.len() as u64,
                compressed_len: compressed.len() as u32,
                uncompressed_len: encoded.len() as u32,
                tx_count: 0,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 0,
            });
            blob.extend_from_slice(&compressed);
        }
        fs::write(path.join(ARCHIVE_V2_BLOCKS_FILE), &blob).unwrap();
        write_archive_v2_hot_block_index(
            &path.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            blob.len() as u64,
            1,
            0,
            &index_rows,
        )
        .unwrap();
    }

    #[test]
    fn builds_sparse_sidecar_from_v3_index() {
        let temp = TempDir::new();
        let epoch = 314;
        let base = epoch * SLOTS_PER_EPOCH;
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(
            &source,
            &[
                (base, 1_700_000_000),
                (base + 1, 1_700_000_000),
                (base + 4, 1_700_000_002),
                (base + 5, 0),
                (base + 10, 1_700_000_006),
                (base + 11, 1_700_000_010),
            ],
        );

        let summary = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &temp.0,
            epoch: Some(epoch),
            output: None,
            force: false,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap();

        assert_eq!(summary.blocks, 6);
        assert_eq!(summary.gaps, 3);
        assert_eq!(summary.slot_gap_rows, 2);
        assert_eq!(summary.time_only_gap_rows, 1);
        assert_eq!(summary.missing_slots, 6);
        assert_eq!(summary.timed_gaps, 2);
        assert_eq!(summary.gaps_missing_time, 1);
        assert_eq!(summary.decreasing_time_gaps, 0);
        assert_eq!(summary.largest_slot_gap, 4);
        assert_eq!(summary.longest_elapsed_gap_secs, Some(4));
        let sidecar = read_sidecar_path(&summary.output).unwrap();
        assert_eq!(sidecar.rows[0].previous_slot, base + 1);
        assert_eq!(sidecar.rows[0].next_slot, base + 4);
        assert_eq!(
            sidecar.rows[1].flags,
            BLOCK_TIME_GAP_FLAG_PREVIOUS_TIME_MISSING
        );

        let reused = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &temp.0,
            epoch: Some(epoch),
            output: None,
            force: false,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap();
        assert!(reused.reused);

        fs::write(&summary.output, b"broken").unwrap();
        let error = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &temp.0,
            epoch: Some(epoch),
            output: None,
            force: false,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("pass --force"));
        let replaced = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &temp.0,
            epoch: Some(epoch),
            output: None,
            force: true,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap();
        assert!(!replaced.reused);
        assert!(read_sidecar_path(&replaced.output).is_ok());
    }

    #[test]
    fn archive_v2_scan_matches_v3_semantics_and_reuses_v3_sidecar() {
        let temp = TempDir::new();
        let epoch = 42;
        let base = epoch * SLOTS_PER_EPOCH;
        let rows = [
            (base, 1_700_000_000),
            (base + 1, 1_700_000_000),
            (base + 4, 1_700_000_002),
            (base + 5, 0),
            (base + 10, 1_700_000_006),
            (base + 11, 1_700_000_010),
        ];
        write_v3(&temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE), &rows);
        write_hot_archive(
            &temp.0,
            &rows
                .iter()
                .map(|&(slot, time)| (slot, (time != 0).then_some(time)))
                .collect::<Vec<_>>(),
        );
        let output = temp.0.join("comparison.bin");
        let v3 = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &temp.0,
            epoch: Some(epoch),
            output: Some(&output),
            force: false,
            source: BuildBlockTimeGapsSource::BlockhashIndexV3,
            progress_json: None,
        })
        .unwrap();
        assert!(!v3.reused);
        let v3_bytes = fs::read(&output).unwrap();
        let v3_sidecar = read_sidecar_path(&output).unwrap();

        let progress = temp.0.join("progress.json");
        let archive = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &temp.0,
            epoch: Some(epoch),
            output: Some(&output),
            force: false,
            source: BuildBlockTimeGapsSource::ArchiveV2Hot,
            progress_json: Some(&progress),
        })
        .unwrap();
        assert!(archive.reused);
        assert_eq!(fs::read(&output).unwrap(), v3_bytes);
        assert_eq!(
            read_sidecar_path(&output).unwrap().header.source_kind,
            BlockTimeGapSourceKind::BlockhashIndexV3
        );
        let progress_json: serde_json::Value =
            serde_json::from_slice(&fs::read(progress).unwrap()).unwrap();
        assert_eq!(progress_json["state"], "scanned");

        let archive_output = temp.0.join("archive-only.bin");
        build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &temp.0,
            epoch: Some(epoch),
            output: Some(&archive_output),
            force: false,
            source: BuildBlockTimeGapsSource::ArchiveV2Hot,
            progress_json: None,
        })
        .unwrap();
        let archive_sidecar = read_sidecar_path(&archive_output).unwrap();
        assert_eq!(
            archive_sidecar.header.source_kind,
            BlockTimeGapSourceKind::ArchiveV2Hot
        );
        assert!(sidecars_semantically_equal(&v3_sidecar, &archive_sidecar));
        assert_eq!(v3_sidecar.rows, archive_sidecar.rows);
    }

    #[test]
    fn records_decreasing_gap_time() {
        let epoch = 1;
        let base = epoch * SLOTS_PER_EPOCH;
        let mut bytes = Vec::new();
        let temp = TempDir::new();
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(&source, &[(base, 100), (base + 3, 99)]);
        bytes.extend(fs::read(&source).unwrap());
        let sidecar =
            collect_from_blockhash_index_v3(&bytes, Some(epoch), Sha256::digest(&bytes).into())
                .unwrap();
        assert_eq!(sidecar.header.decreasing_time_gap_count, 1);
        assert_eq!(sidecar.rows[0].flags, BLOCK_TIME_GAP_FLAG_TIME_DECREASING);
        assert_eq!(sidecar.rows[0].elapsed_seconds(), None);
    }

    #[test]
    fn equal_second_slot_gap_is_timed_but_normal_adjacent_pair_is_omitted() {
        let epoch = 2;
        let base = epoch * SLOTS_PER_EPOCH;
        let temp = TempDir::new();
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(&source, &[(base, 100), (base + 1, 100), (base + 3, 100)]);
        let bytes = fs::read(&source).unwrap();
        let sidecar =
            collect_from_blockhash_index_v3(&bytes, Some(epoch), Sha256::digest(&bytes).into())
                .unwrap();
        assert_eq!(sidecar.rows.len(), 1);
        assert_eq!(sidecar.rows[0].previous_slot, base + 1);
        assert_eq!(sidecar.rows[0].flags, 0);
        assert_eq!(sidecar.rows[0].elapsed_seconds(), Some(0));
        assert_eq!(sidecar.header.timed_gap_count, 1);
        assert_eq!(sidecar.header.decreasing_time_gap_count, 0);
    }

    #[test]
    fn rejects_out_of_epoch_or_non_monotonic_rows() {
        let temp = TempDir::new();
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(&source, &[(SLOTS_PER_EPOCH - 1, 10)]);
        let error = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &source,
            epoch: Some(1),
            output: None,
            force: false,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap_err();
        assert!(error.to_string().contains("scan"));

        write_v3(
            &source,
            &[(SLOTS_PER_EPOCH + 2, 10), (SLOTS_PER_EPOCH + 1, 11)],
        );
        let error = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &source,
            epoch: Some(1),
            output: None,
            force: false,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("not strictly after"));
    }

    #[test]
    fn rejects_declared_row_count_mismatch() {
        let epoch = 2;
        let temp = TempDir::new();
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(&source, &[(epoch * SLOTS_PER_EPOCH, 10)]);
        let mut bytes = fs::read(&source).unwrap();
        bytes[12..20].copy_from_slice(&2u64.to_le_bytes());
        let error =
            collect_from_blockhash_index_v3(&bytes, Some(epoch), Sha256::digest(&bytes).into())
                .unwrap_err();
        assert!(error.to_string().contains("declares 2 rows"));
    }

    #[test]
    fn empty_v3_index_produces_canonical_empty_sidecar() {
        let epoch = 3;
        let temp = TempDir::new();
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(&source, &[]);

        let summary = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &source,
            epoch: Some(epoch),
            output: None,
            force: false,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap();
        assert_eq!(summary.blocks, 0);
        assert_eq!(summary.gaps, 0);
        let sidecar = read_sidecar_path(&summary.output).unwrap();
        assert_eq!(sidecar.header.first_slot, 0);
        assert_eq!(sidecar.header.first_block_time, BLOCK_TIME_GAP_MISSING_TIME);
        assert!(sidecar.rows.is_empty());
    }

    #[test]
    fn force_refuses_output_alias_of_source() {
        let temp = TempDir::new();
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(&source, &[(SLOTS_PER_EPOCH, 10)]);
        fs::create_dir(temp.0.join("alias-parent")).unwrap();
        let alias = temp
            .0
            .join("alias-parent")
            .join("..")
            .join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);

        let error = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &source,
            epoch: Some(1),
            output: Some(&alias),
            force: true,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap_err();
        assert!(error.to_string().contains("must not replace its source"));
        assert!(
            fs::read(&source)
                .unwrap()
                .starts_with(ARCHIVE_V2_BLOCKHASH_INDEX_V3_MAGIC)
        );
    }

    #[test]
    fn infers_epoch_from_first_v3_slot() {
        let epoch = 9;
        let base = epoch * SLOTS_PER_EPOCH;
        let temp = TempDir::new();
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(&source, &[(base + 7, 100), (base + 9, 102)]);

        let summary = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &source,
            epoch: None,
            output: None,
            force: false,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap();
        assert_eq!(
            read_sidecar_path(&summary.output).unwrap().header.epoch,
            epoch
        );
    }

    #[test]
    fn empty_v3_requires_explicit_epoch() {
        let temp = TempDir::new();
        let source = temp.0.join(ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE);
        write_v3(&source, &[]);

        let error = build_block_time_gaps(BuildBlockTimeGapsConfig {
            input: &source,
            epoch: None,
            output: None,
            force: false,
            source: BuildBlockTimeGapsSource::Auto,
            progress_json: None,
        })
        .unwrap_err();
        assert!(format!("{error:#}").contains("cannot infer an epoch"));
    }
}
