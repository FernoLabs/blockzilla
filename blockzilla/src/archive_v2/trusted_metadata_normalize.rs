//! One-time metadata repair for a trusted local Compact Archive V2 directory.
//!
//! This module has no publication authority. It reads one descriptor-pinned
//! source, writes one fresh staging directory, and validates the result with
//! the current read SDK. It does not create or verify content hashes, seals,
//! profile markers, receipts, or generation control documents. The normal
//! current reader remains the separate acceptance gate for the finished
//! archive.

use anyhow::{Context, Result, ensure};
use blockzilla_archive_v2::{ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE, ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES, ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_GENESIS_BIN_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN, ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY, ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_HOT_INDEX_HEADER_LEN, ARCHIVE_V2_HOT_INDEX_MAGIC, ARCHIVE_V2_HOT_INDEX_ROW_LEN, ARCHIVE_V2_HOT_INDEX_VERSION, ARCHIVE_V2_META_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE, ArchiveV2BlockAccessIndexRow, ArchiveV2GetBlockIndexRow, ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndex, ArchiveV2HotBlockIndexRow, ArchiveV2HotMetaRecord, ArchiveV2HotTxRow, ArchiveV2WireFallbackReason, ArchiveV2WireIdentityVisitor, ArchiveV2WireMetadataErrorSchema, ArchiveV2WireRewriteErrorKind, ArchiveV2WireRewriteLimits, BLOCK_TIME_GAP_FILE, WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS, WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY, WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, canonicalize_archive_v2_metadata_owned, deserialize_archive_v2_hot_block_blob_borrowed_current_with_preallocation_limit, read_archive_v2_block_access_index, read_archive_v2_get_block_index, rewrite_archive_v2_metadata_wire, validate_archive_v2_metadata_error_prefix_for_selected_schema, write_archive_v2_get_block_index, write_archive_v2_hot_block_index};
use blockzilla_compact::CompactMetaV1;
use blockzilla_primitives::{WincodeLeb128FramedReader, wincode_leb128_config};
use blockzilla_read_sdk::{PinnedLocalEntryKind, PinnedLocalRangeSource, RangeSource};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const IO_BUFFER_BYTES: usize = 8 << 20;
const MAX_SOURCE_INDEX_BYTES: usize = 256 << 20;
const TRUSTED_HOT_BLOCK_PREALLOCATION_LIMIT_BYTES: usize = 256 << 20;
const KNOWN_ARCHIVE_V2_TX_FLAGS: u32 = (1 << 11) - 1;

const COPY_SIDECARS: &[&str] = &[
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_GENESIS_BIN_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_SHREDDING_FILE,
    BLOCK_TIME_GAP_FILE,
    ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
];

#[derive(Debug, Clone)]
pub(crate) struct TrustedMetadataNormalizeOptions {
    pub source: PathBuf,
    pub staging: PathBuf,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub zstd_level: i32,
    pub max_metadata_bytes: usize,
    pub progress_blocks: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TrustedMetadataNormalizeSummary {
    pub blocks: u64,
    pub transactions: u64,
    pub metadata_records: u64,
    pub current_error_records: u64,
    pub legacy_error_records: u64,
    pub ambiguous_error_records: u64,
    pub copied_blocks: u64,
    pub recompressed_blocks: u64,
    pub source_block_bytes: u64,
    pub target_block_bytes: u64,
    pub copied_sidecars: u64,
    pub copied_sidecar_bytes: u64,
    pub rebuilt_get_block_rows: u64,
    pub validated_output_blocks: u64,
}

impl fmt::Display for TrustedMetadataNormalizeSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "blocks={} transactions={} metadata_records={} current_error_records={} legacy_error_records={} ambiguous_error_records={} copied_blocks={} recompressed_blocks={} source_block_bytes={} target_block_bytes={} copied_sidecars={} copied_sidecar_bytes={} rebuilt_get_block_rows={} validated_output_blocks={}",
            self.blocks,
            self.transactions,
            self.metadata_records,
            self.current_error_records,
            self.legacy_error_records,
            self.ambiguous_error_records,
            self.copied_blocks,
            self.recompressed_blocks,
            self.source_block_bytes,
            self.target_block_bytes,
            self.copied_sidecars,
            self.copied_sidecar_bytes,
            self.rebuilt_get_block_rows,
            self.validated_output_blocks,
        )
    }
}

#[derive(Debug, Clone)]
struct SourcePayloadFile {
    name: &'static str,
    bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MetadataCounts {
    records: u64,
    current_error_records: u64,
    legacy_error_records: u64,
    ambiguous_error_records: u64,
    input_bytes: u64,
    output_bytes: u64,
}

impl MetadataCounts {
    fn add(&mut self, other: Self) -> Result<()> {
        self.records = checked_add(self.records, other.records, "metadata record count")?;
        self.current_error_records = checked_add(
            self.current_error_records,
            other.current_error_records,
            "current metadata error count",
        )?;
        self.legacy_error_records = checked_add(
            self.legacy_error_records,
            other.legacy_error_records,
            "legacy metadata error count",
        )?;
        self.ambiguous_error_records = checked_add(
            self.ambiguous_error_records,
            other.ambiguous_error_records,
            "ambiguous metadata error count",
        )?;
        self.input_bytes = checked_add(
            self.input_bytes,
            other.input_bytes,
            "metadata input byte count",
        )?;
        self.output_bytes = checked_add(
            self.output_bytes,
            other.output_bytes,
            "metadata output byte count",
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FrameProcessingCounts {
    copied_blocks: u64,
    copied_bytes: u64,
    recompressed_blocks: u64,
    recompressed_source_bytes: u64,
    recompressed_target_bytes: u64,
}

impl FrameProcessingCounts {
    fn observe_copy(&mut self, bytes: u64) -> Result<()> {
        self.copied_blocks = checked_add(self.copied_blocks, 1, "copied block count")?;
        self.copied_bytes = checked_add(self.copied_bytes, bytes, "copied block bytes")?;
        Ok(())
    }

    fn observe_recompression(&mut self, source_bytes: u64, target_bytes: u64) -> Result<()> {
        self.recompressed_blocks =
            checked_add(self.recompressed_blocks, 1, "recompressed block count")?;
        self.recompressed_source_bytes = checked_add(
            self.recompressed_source_bytes,
            source_bytes,
            "recompressed source bytes",
        )?;
        self.recompressed_target_bytes = checked_add(
            self.recompressed_target_bytes,
            target_bytes,
            "recompressed target bytes",
        )?;
        Ok(())
    }
}

struct RewriteOptions {
    source_zstd_level: i32,
    source_index_flags: u32,
    target_zstd_level: i32,
    max_metadata_bytes: usize,
    progress_blocks: u64,
}

struct RewriteOutput {
    target_rows: Vec<ArchiveV2HotBlockIndexRow>,
    blocks: u64,
    transactions: u64,
    target_bytes: u64,
    metadata: MetadataCounts,
    frames: FrameProcessingCounts,
}

pub(crate) fn normalize_trusted_metadata(
    options: &TrustedMetadataNormalizeOptions,
) -> Result<TrustedMetadataNormalizeSummary> {
    ensure!(
        options.source.is_absolute(),
        "--source must be an absolute path"
    );
    ensure!(
        options.staging.is_absolute(),
        "--staging must be an absolute path"
    );
    ensure!(
        options.slots_per_epoch > 0,
        "--slots-per-epoch must be positive"
    );
    ensure!(
        options.max_metadata_bytes > 0,
        "--max-metadata-bytes must be positive"
    );
    let epoch_start_slot = options
        .epoch
        .checked_mul(options.slots_per_epoch)
        .context("epoch start slot overflows u64")?;
    let source = PinnedLocalRangeSource::open_directory(&options.source)
        .map_err(anyhow::Error::new)
        .context("open source through one no-follow directory descriptor")?;
    let source_files = pin_source_payload_files(&source)?;
    prepare_fresh_staging(&options.source, &options.staging)?;

    let source_index = read_pinned_hot_index(&source)?;
    let source_footer = read_pinned_footer(&source, options.epoch)?;
    ensure!(
        source_footer.tx_raw_fallbacks == 0 && source_footer.metadata_raw_fallbacks == 0,
        "source footer reports raw transaction or metadata fallbacks"
    );
    ensure!(
        source_index.flags
            & (ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY | ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS)
            == 0
            && source_index.flags == 0,
        "metadata repair requires independent dictionary-free zstd frames; flags={:#x}",
        source_index.flags
    );
    validate_source_index_geometry(&source_index.rows, source_index.blob_file_bytes)?;
    validate_epoch_hot_slots(
        &source_index.rows,
        epoch_start_slot,
        options.slots_per_epoch,
    )?;

    let (copied_sidecars, copied_sidecar_bytes) =
        copy_payload_sidecars(&source, &options.staging, &source_files)?;
    let source_blocks = source
        .open_file(ARCHIVE_V2_BLOCKS_FILE)
        .map_err(anyhow::Error::new)?;
    let source_block_bytes = source_blocks.metadata()?.len();
    ensure!(
        source_block_bytes == source_index.blob_file_bytes,
        "source block length differs from its hot index"
    );
    let target_blocks = create_new_file(&options.staging, ARCHIVE_V2_BLOCKS_FILE)?;
    let rewrite = rewrite_blocks(
        source_blocks,
        target_blocks,
        &source_index.rows,
        RewriteOptions {
            source_zstd_level: source_index.level,
            source_index_flags: source_index.flags,
            target_zstd_level: options.zstd_level,
            max_metadata_bytes: options.max_metadata_bytes,
            progress_blocks: options.progress_blocks,
        },
    )?;
    ensure!(
        rewrite.blocks == source_index.rows.len() as u64,
        "metadata repair did not process every hot-index row"
    );
    ensure!(
        rewrite.blocks == source_footer.blocks
            && rewrite.transactions == source_footer.transactions,
        "repair coverage differs from the structurally admitted source footer"
    );
    assert_rewrite_index_construction(
        &source_index.rows,
        &rewrite.target_rows,
        rewrite.target_bytes,
    )?;

    let target_index_path = options.staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    ensure!(
        !target_index_path.exists(),
        "fresh staging unexpectedly contains the target hot index"
    );
    write_archive_v2_hot_block_index(
        &target_index_path,
        rewrite.target_bytes,
        options.zstd_level,
        0,
        &rewrite.target_rows,
    )?;
    sync_regular_file(&target_index_path)?;

    let rebuilt_get_block = rebuild_get_block_if_present(
        &source,
        &options.staging,
        &source_files,
        &source_index.rows,
        &rewrite.target_rows,
        epoch_start_slot,
        options.slots_per_epoch,
    )?;
    sync_directory(&options.staging)?;
    source
        .verify_unchanged()
        .map_err(anyhow::Error::new)
        .context("source changed during repair")?;

    let target_source = PinnedLocalRangeSource::open_directory(&options.staging)
        .map_err(anyhow::Error::new)
        .context("open completed staging through one no-follow directory descriptor")?;
    let target_index = read_pinned_hot_index(&target_source)?;
    ensure!(
        same_hot_index_geometry(
            &target_index,
            options.zstd_level,
            rewrite.target_bytes,
            &rewrite.target_rows
        ),
        "persisted target hot index differs from the repair result"
    );
    let target_footer = read_pinned_footer(&target_source, options.epoch)?;
    ensure!(
        target_footer.blocks == rewrite.blocks
            && target_footer.transactions == rewrite.transactions,
        "persisted target footer differs from the repair totals"
    );
    let validated_output_blocks = validate_persisted_output_blocks(
        target_source
            .open_file(ARCHIVE_V2_BLOCKS_FILE)
            .map_err(anyhow::Error::new)?,
        &target_index.rows,
    )?;
    ensure!(
        validated_output_blocks == rewrite.blocks,
        "output validation did not read every block"
    );
    target_source
        .verify_unchanged()
        .map_err(anyhow::Error::new)
        .context("staging changed during current SDK validation")?;
    source
        .verify_unchanged()
        .map_err(anyhow::Error::new)
        .context("source changed before repair completion")?;
    assert_exact_staging_inventory(&target_source, &source_files, rebuilt_get_block.is_some())?;

    Ok(TrustedMetadataNormalizeSummary {
        blocks: rewrite.blocks,
        transactions: rewrite.transactions,
        metadata_records: rewrite.metadata.records,
        current_error_records: rewrite.metadata.current_error_records,
        legacy_error_records: rewrite.metadata.legacy_error_records,
        ambiguous_error_records: rewrite.metadata.ambiguous_error_records,
        copied_blocks: rewrite.frames.copied_blocks,
        recompressed_blocks: rewrite.frames.recompressed_blocks,
        source_block_bytes,
        target_block_bytes: rewrite.target_bytes,
        copied_sidecars,
        copied_sidecar_bytes,
        rebuilt_get_block_rows: rebuilt_get_block.unwrap_or(0),
        validated_output_blocks,
    })
}

fn pin_source_payload_files(source: &PinnedLocalRangeSource) -> Result<Vec<SourcePayloadFile>> {
    let inventory = source.inventory().map_err(anyhow::Error::new)?;
    let mut by_name = BTreeMap::new();
    for entry in inventory {
        let Some(name) = entry.name.to_str() else {
            continue;
        };
        by_name.insert(name.to_owned(), entry);
    }
    for name in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ] {
        let entry = by_name
            .get(name)
            .with_context(|| format!("source is missing required payload file {name}"))?;
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile,
            "required payload {name} is not a regular file"
        );
        source.open_file(name).map_err(anyhow::Error::new)?;
    }

    let mut files = Vec::new();
    for &name in COPY_SIDECARS {
        let Some(entry) = by_name.get(name) else {
            continue;
        };
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile,
            "payload sidecar {name} is not a regular file"
        );
        source.open_file(name).map_err(anyhow::Error::new)?;
        files.push(SourcePayloadFile {
            name,
            bytes: entry.bytes,
        });
    }
    if let Some(entry) = by_name.get(ARCHIVE_V2_GET_BLOCK_INDEX_FILE) {
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile,
            "get-block index is not a regular file"
        );
        source
            .open_file(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)
            .map_err(anyhow::Error::new)?;
        for required in [
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ] {
            ensure!(
                files.iter().any(|file| file.name == required),
                "get-block index exists without required sidecar {required}"
            );
        }
    }
    Ok(files)
}

fn prepare_fresh_staging(source: &Path, staging: &Path) -> Result<()> {
    match fs::symlink_metadata(staging) {
        Ok(_) => anyhow::bail!("staging already exists: {}", staging.display()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("inspect staging path"),
    }
    let source = fs::canonicalize(source).context("canonicalize source path")?;
    let parent = staging
        .parent()
        .context("staging has no parent directory")?;
    let parent = fs::canonicalize(parent).context("canonicalize staging parent")?;
    ensure!(
        !parent.starts_with(&source),
        "staging must not be inside the source directory"
    );
    fs::create_dir(staging)
        .with_context(|| format!("create fresh staging {}", staging.display()))?;
    Ok(())
}

fn copy_payload_sidecars(
    source: &PinnedLocalRangeSource,
    staging: &Path,
    files: &[SourcePayloadFile],
) -> Result<(u64, u64)> {
    let mut copied_files = 0u64;
    let mut copied_bytes = 0u64;
    for file in files {
        let mut input = source.open_file(file.name).map_err(anyhow::Error::new)?;
        input.seek(SeekFrom::Start(0))?;
        let output = create_new_file(staging, file.name)?;
        let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, input);
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, output);
        let bytes = io::copy(&mut reader, &mut writer)
            .with_context(|| format!("copy payload sidecar {}", file.name))?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
        ensure!(
            bytes == file.bytes,
            "payload sidecar {} changed length during copy",
            file.name
        );
        copied_files = checked_add(copied_files, 1, "copied sidecar count")?;
        copied_bytes = checked_add(copied_bytes, bytes, "copied sidecar bytes")?;
    }
    Ok((copied_files, copied_bytes))
}

fn create_new_file(staging: &Path, name: &str) -> Result<File> {
    ensure!(
        !name.is_empty() && !name.contains('/') && name != "." && name != "..",
        "invalid staging object name"
    );
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(staging.join(name))
        .with_context(|| format!("create fresh staging object {name}"))
}

fn sync_regular_file(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync {}", path.display()))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {} for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn read_pinned_hot_index(source: &PinnedLocalRangeSource) -> Result<ArchiveV2HotBlockIndex> {
    let bytes = source
        .read_all_bounded(ARCHIVE_V2_BLOCK_INDEX_FILE, MAX_SOURCE_INDEX_BYTES)
        .map_err(anyhow::Error::new)
        .context("read pinned hot index")?;
    ensure!(
        bytes.len() >= ARCHIVE_V2_HOT_INDEX_HEADER_LEN,
        "hot index header is truncated"
    );
    ensure!(
        &bytes[..8] == ARCHIVE_V2_HOT_INDEX_MAGIC,
        "hot index has bad magic"
    );
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    ensure!(
        version == ARCHIVE_V2_HOT_INDEX_VERSION,
        "hot index has unsupported version {version}"
    );
    ensure!(
        bytes[10..12] == [0, 0],
        "hot index reserved bytes are non-zero"
    );
    let row_count = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let expected_bytes = (ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            row_count
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .context("hot-index row bytes overflow")?,
        )
        .context("hot-index bytes overflow")?;
    ensure!(
        bytes.len() as u64 == expected_bytes,
        "hot index has {} bytes, expected {expected_bytes}",
        bytes.len()
    );
    let row_count = usize::try_from(row_count).context("hot-index row count exceeds usize")?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .context("reserve hot-index rows")?;
    for row in bytes[ARCHIVE_V2_HOT_INDEX_HEADER_LEN..].chunks_exact(ARCHIVE_V2_HOT_INDEX_ROW_LEN) {
        rows.push(ArchiveV2HotBlockIndexRow {
            block_id: u32::from_le_bytes(row[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(row[4..12].try_into().unwrap()),
            compressed_offset: u64::from_le_bytes(row[12..20].try_into().unwrap()),
            compressed_len: u32::from_le_bytes(row[20..24].try_into().unwrap()),
            uncompressed_len: u32::from_le_bytes(row[24..28].try_into().unwrap()),
            tx_count: u32::from_le_bytes(row[28..32].try_into().unwrap()),
            first_tx_ordinal: u64::from_le_bytes(row[32..40].try_into().unwrap()),
            first_signature_ordinal: u64::from_le_bytes(row[40..48].try_into().unwrap()),
            signature_count: u32::from_le_bytes(row[48..52].try_into().unwrap()),
        });
    }
    Ok(ArchiveV2HotBlockIndex {
        blob_file_bytes: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
        level: i32::from_le_bytes(bytes[28..32].try_into().unwrap()),
        flags: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
        rows,
    })
}

fn read_pinned_footer(
    source: &PinnedLocalRangeSource,
    epoch: u64,
) -> Result<WincodeArchiveV2Footer> {
    let file = source
        .open_file(ARCHIVE_V2_META_FILE)
        .map_err(anyhow::Error::new)?;
    let mut reader =
        WincodeLeb128FramedReader::new(BufReader::with_capacity(IO_BUFFER_BYTES, file));
    let mut position = 0usize;
    let mut saw_genesis = false;
    let mut footer = None;
    while let Some((_, record)) = reader.read::<ArchiveV2HotMetaRecord>()? {
        ensure!(
            footer.is_none(),
            "metadata contains records after its footer"
        );
        match (position, record) {
            (0, ArchiveV2HotMetaRecord::Header(header)) => {
                ensure!(
                    header.version == WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
                    "metadata has unsupported hot-block version {}",
                    header.version
                );
                ensure!(
                    header.flags & WINCODE_ARCHIVE_V2_FLAG_LEB128 != 0
                        && header.flags & WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY == 0,
                    "metadata header does not describe one registry-backed LEB128 archive"
                );
                let known_flags = WINCODE_ARCHIVE_V2_FLAG_LEB128
                    | WINCODE_ARCHIVE_V2_FLAG_NO_REGISTRY
                    | WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY
                    | WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS;
                ensure!(
                    header.flags & !known_flags == 0,
                    "metadata header has unknown flags {:#x}",
                    header.flags & !known_flags
                );
            }
            (0, _) => anyhow::bail!("metadata does not begin with a header"),
            (_, ArchiveV2HotMetaRecord::Header(_)) => {
                anyhow::bail!("metadata contains a duplicate header")
            }
            (_, ArchiveV2HotMetaRecord::Genesis(_)) => {
                ensure!(
                    epoch == 0 && !saw_genesis,
                    "metadata contains an unexpected or duplicate genesis record"
                );
                saw_genesis = true;
            }
            (_, ArchiveV2HotMetaRecord::Footer(value)) => footer = Some(value),
        }
        position += 1;
    }
    let footer = footer.context("metadata does not end in a footer")?;
    ensure!(
        footer.decode_errors.is_empty(),
        "metadata footer reports source decode errors"
    );
    Ok(footer)
}

fn same_hot_index_geometry(
    index: &ArchiveV2HotBlockIndex,
    level: i32,
    blob_bytes: u64,
    rows: &[ArchiveV2HotBlockIndexRow],
) -> bool {
    index.blob_file_bytes == blob_bytes
        && index.level == level
        && index.flags == 0
        && index.rows.len() == rows.len()
        && index.rows.iter().zip(rows).all(|(left, right)| {
            left.block_id == right.block_id
                && left.slot == right.slot
                && left.compressed_offset == right.compressed_offset
                && left.compressed_len == right.compressed_len
                && left.uncompressed_len == right.uncompressed_len
                && left.tx_count == right.tx_count
                && left.first_tx_ordinal == right.first_tx_ordinal
                && left.first_signature_ordinal == right.first_signature_ordinal
                && left.signature_count == right.signature_count
        })
}

fn validate_persisted_output_blocks(
    mut blocks_file: File,
    rows: &[ArchiveV2HotBlockIndexRow],
) -> Result<u64> {
    blocks_file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, blocks_file);
    let mut decompressor = zstd::bulk::Decompressor::new().context("create output validator")?;
    let mut compressed = Vec::new();
    let mut decompressed = Vec::new();
    let mut canonical_metadata = Vec::new();
    for row in rows {
        compressed.resize(row.compressed_len as usize, 0);
        reader
            .read_exact(&mut compressed)
            .with_context(|| format!("read persisted output slot {}", row.slot))?;
        ensure!(
            is_exact_single_zstd_frame(&compressed),
            "persisted output slot {} is not exactly one zstd frame",
            row.slot
        );
        decompressed.clear();
        decompressed
            .try_reserve(row.uncompressed_len as usize)
            .context("reserve persisted output block")?;
        let decoded_len = decompressor
            .decompress_to_buffer(&compressed, &mut decompressed)
            .with_context(|| format!("decompress persisted output slot {}", row.slot))?;
        ensure!(
            decoded_len == row.uncompressed_len as usize
                && decompressed.len() == row.uncompressed_len as usize,
            "persisted output slot {} length differs from its index",
            row.slot
        );
        let block =
            deserialize_archive_v2_hot_block_blob_borrowed_current_with_preallocation_limit::<
                TRUSTED_HOT_BLOCK_PREALLOCATION_LIMIT_BYTES,
            >(&decompressed)
            .map_err(anyhow::Error::new)
            .with_context(|| format!("decode persisted output slot {} as current", row.slot))?;
        ensure!(
            block.header.slot == row.slot
                && block.tx_count == row.tx_count
                && block.tx_rows_len() == row.tx_count as usize,
            "persisted output slot {} identity or count differs from its index",
            row.slot
        );
        let output_rows = block.tx_rows().collect::<Vec<_>>();
        validate_source_block_row_geometry(
            row,
            &output_rows,
            block.message_bytes,
            block.metadata_bytes,
        )?;
        for transaction in &output_rows {
            if transaction.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
                continue;
            }
            ensure!(
                transaction.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK == 0,
                "persisted output slot {} tx {} retained raw metadata",
                row.slot,
                transaction.tx_index
            );
            let start = transaction.metadata_offset as usize;
            let end = start
                .checked_add(transaction.metadata_len as usize)
                .context("persisted metadata range overflow")?;
            let bytes = block
                .metadata_bytes
                .get(start..end)
                .context("persisted metadata range is outside its block")?;
            let current: CompactMetaV1 =
                wincode::config::deserialize_exact(bytes, wincode_leb128_config())
                    .map_err(anyhow::Error::new)
                    .with_context(|| {
                        format!(
                            "decode persisted output slot {} tx {} as current CompactMetaV1",
                            row.slot, transaction.tx_index
                        )
                    })?;
            ensure!(
                current.err.is_some() == (transaction.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0),
                "persisted output slot {} tx {} error flag differs from current metadata",
                row.slot,
                transaction.tx_index
            );
            canonical_metadata.clear();
            wincode::config::serialize_into(
                &mut canonical_metadata,
                &current,
                wincode_leb128_config(),
            )?;
            ensure!(
                canonical_metadata == bytes,
                "persisted output slot {} tx {} is not canonical current metadata",
                row.slot,
                transaction.tx_index
            );
        }
    }
    let mut trailing = [0u8; 1];
    ensure!(
        reader.read(&mut trailing)? == 0,
        "persisted output has bytes outside its hot index"
    );
    Ok(rows.len() as u64)
}

fn validate_source_index_geometry(
    rows: &[ArchiveV2HotBlockIndexRow],
    blob_bytes: u64,
) -> Result<()> {
    let mut compressed_offset = 0u64;
    let mut transaction_ordinal = 0u64;
    let mut signature_ordinal = 0u64;
    let mut previous_slot = None;
    for (number, row) in rows.iter().enumerate() {
        ensure!(
            row.block_id as usize == number,
            "source block IDs are not canonical"
        );
        ensure!(
            row.compressed_offset == compressed_offset,
            "source compressed ranges are not contiguous at block {}",
            row.block_id
        );
        ensure!(
            row.compressed_len > 0 && row.uncompressed_len > 0,
            "source block {} has an empty frame",
            row.block_id
        );
        ensure!(
            row.first_tx_ordinal == transaction_ordinal,
            "source transaction ordinals are not contiguous at block {}",
            row.block_id
        );
        ensure!(
            row.first_signature_ordinal == signature_ordinal,
            "source signature ordinals are not contiguous at block {}",
            row.block_id
        );
        if let Some(previous_slot) = previous_slot {
            ensure!(
                row.slot > previous_slot,
                "source slots are not strictly increasing"
            );
        }
        previous_slot = Some(row.slot);
        compressed_offset = checked_add(
            compressed_offset,
            u64::from(row.compressed_len),
            "source compressed offset",
        )?;
        transaction_ordinal = checked_add(
            transaction_ordinal,
            u64::from(row.tx_count),
            "source transaction ordinal",
        )?;
        signature_ordinal = checked_add(
            signature_ordinal,
            u64::from(row.signature_count),
            "source signature ordinal",
        )?;
    }
    ensure!(
        compressed_offset == blob_bytes,
        "source index covers {compressed_offset} of {blob_bytes} block bytes"
    );
    Ok(())
}

fn validate_epoch_hot_slots(
    rows: &[ArchiveV2HotBlockIndexRow],
    epoch_start_slot: u64,
    slots_per_epoch: u64,
) -> Result<()> {
    let epoch_end_slot = epoch_start_slot
        .checked_add(slots_per_epoch)
        .context("epoch end slot overflows u64")?;
    for row in rows {
        ensure!(
            (epoch_start_slot..epoch_end_slot).contains(&row.slot),
            "slot {} is outside [{epoch_start_slot}, {epoch_end_slot})",
            row.slot
        );
    }
    Ok(())
}

fn is_exact_single_zstd_frame(compressed: &[u8]) -> bool {
    zstd::zstd_safe::find_frame_compressed_size(compressed)
        .is_ok_and(|frame_bytes| frame_bytes == compressed.len())
}

fn assert_normalized_row_construction(
    source_rows: impl ExactSizeIterator<Item = ArchiveV2HotTxRow>,
    target_rows: &[ArchiveV2HotTxRow],
    slot: u64,
) -> Result<()> {
    ensure!(
        source_rows.len() == target_rows.len(),
        "slot {slot} source and repaired transaction-row counts differ"
    );
    let mut visited = 0usize;
    for (source, target) in source_rows.zip(target_rows) {
        ensure!(
            source.tx_index == target.tx_index
                && source.flags == target.flags
                && source.message_offset == target.message_offset
                && source.message_len == target.message_len
                && source.signature_count == target.signature_count
                && source.reserved == target.reserved,
            "slot {slot} transaction {} changed outside metadata geometry",
            source.tx_index
        );
        visited += 1;
    }
    ensure!(
        visited == target_rows.len(),
        "transaction-row construction proof is incomplete"
    );
    Ok(())
}

fn assert_rewrite_index_construction(
    source_rows: &[ArchiveV2HotBlockIndexRow],
    target_rows: &[ArchiveV2HotBlockIndexRow],
    target_blob_bytes: u64,
) -> Result<()> {
    ensure!(
        source_rows.len() == target_rows.len(),
        "source and target hot indexes have different row counts"
    );
    let mut target_offset = 0u64;
    for (source, target) in source_rows.iter().zip(target_rows) {
        ensure!(
            source.block_id == target.block_id
                && source.slot == target.slot
                && source.tx_count == target.tx_count
                && source.first_tx_ordinal == target.first_tx_ordinal
                && source.first_signature_ordinal == target.first_signature_ordinal
                && source.signature_count == target.signature_count,
            "target hot-index identity or count changed at block {}",
            source.block_id
        );
        ensure!(
            target.compressed_offset == target_offset
                && target.compressed_len > 0
                && target.uncompressed_len > 0,
            "target hot-index compressed geometry is not canonical at block {}",
            target.block_id
        );
        target_offset = checked_add(
            target_offset,
            u64::from(target.compressed_len),
            "target compressed coverage",
        )?;
    }
    ensure!(
        target_offset == target_blob_bytes,
        "target index covers {target_offset} of {target_blob_bytes} block bytes"
    );
    Ok(())
}

fn validate_source_block_row_geometry(
    source_index: &ArchiveV2HotBlockIndexRow,
    rows: &[ArchiveV2HotTxRow],
    message_bytes: &[u8],
    metadata_bytes: &[u8],
) -> Result<()> {
    ensure!(
        rows.len() == source_index.tx_count as usize,
        "slot {} transaction-row count differs from its hot index",
        source_index.slot
    );
    validate_tx_index_permutation(rows, source_index.slot)?;
    let mut message_cursor = 0u32;
    let mut metadata_cursor = 0u32;
    let mut signatures = 0u32;
    for row in rows {
        ensure!(
            row.reserved == [0; 3],
            "slot {} tx {} has non-zero reserved bytes",
            source_index.slot,
            row.tx_index
        );
        ensure!(
            row.flags & !KNOWN_ARCHIVE_V2_TX_FLAGS == 0,
            "slot {} tx {} has unknown flags {:#x}",
            source_index.slot,
            row.tx_index,
            row.flags & !KNOWN_ARCHIVE_V2_TX_FLAGS
        );
        ensure!(
            row.message_len > 0 && row.message_offset == message_cursor,
            "slot {} tx {} has an empty or non-contiguous message range",
            source_index.slot,
            row.tx_index
        );
        message_cursor = row
            .message_offset
            .checked_add(row.message_len)
            .context("source message offset overflow")?;
        ensure!(
            message_cursor as usize <= message_bytes.len(),
            "slot {} tx {} message range is outside the block",
            source_index.slot,
            row.tx_index
        );
        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                row.metadata_len == 0,
                "slot {} tx {} has metadata bytes without HAS_METADATA",
                source_index.slot,
                row.tx_index
            );
        } else {
            ensure!(
                row.metadata_len > 0 && row.metadata_offset == metadata_cursor,
                "slot {} tx {} has an empty or non-contiguous metadata range",
                source_index.slot,
                row.tx_index
            );
            metadata_cursor = row
                .metadata_offset
                .checked_add(row.metadata_len)
                .context("source metadata offset overflow")?;
            ensure!(
                metadata_cursor as usize <= metadata_bytes.len(),
                "slot {} tx {} metadata range is outside the block",
                source_index.slot,
                row.tx_index
            );
        }
        signatures = signatures
            .checked_add(u32::from(row.signature_count))
            .context("source block signature count overflow")?;
    }
    ensure!(
        message_cursor as usize == message_bytes.len(),
        "slot {} has unindexed trailing message bytes",
        source_index.slot
    );
    ensure!(
        metadata_cursor as usize == metadata_bytes.len(),
        "slot {} has unindexed trailing metadata bytes",
        source_index.slot
    );
    ensure!(
        signatures == source_index.signature_count,
        "slot {} row signature count differs from its hot index",
        source_index.slot
    );
    Ok(())
}

fn rewrite_blocks(
    mut source_file: File,
    target_file: File,
    source_rows: &[ArchiveV2HotBlockIndexRow],
    options: RewriteOptions,
) -> Result<RewriteOutput> {
    source_file.seek(SeekFrom::Start(0))?;
    let mut source = BufReader::with_capacity(IO_BUFFER_BYTES, source_file);
    let mut target = BufWriter::with_capacity(IO_BUFFER_BYTES, target_file);
    let mut decompressor = zstd::bulk::Decompressor::new().context("create zstd decompressor")?;
    let mut compressor =
        zstd::bulk::Compressor::new(options.target_zstd_level).context("create zstd compressor")?;
    let mut compressed = Vec::new();
    let mut decompressed = Vec::new();
    let mut repaired_metadata = Vec::new();
    let mut repaired_rows = Vec::new();
    let mut serialized = Vec::new();
    let mut target_compressed = Vec::new();
    let mut target_rows = Vec::with_capacity(source_rows.len());
    let mut metadata = MetadataCounts::default();
    let mut frames = FrameProcessingCounts::default();
    let mut target_offset = 0u64;
    let mut transactions = 0u64;
    let limits = ArchiveV2WireRewriteLimits {
        max_input_bytes: options.max_metadata_bytes,
        max_output_bytes: options.max_metadata_bytes,
        ..ArchiveV2WireRewriteLimits::default()
    };

    for (number, source_row) in source_rows.iter().copied().enumerate() {
        compressed.resize(source_row.compressed_len as usize, 0);
        source
            .read_exact(&mut compressed)
            .with_context(|| format!("read compressed slot {}", source_row.slot))?;
        ensure!(
            is_exact_single_zstd_frame(&compressed),
            "slot {} index range is not exactly one zstd frame",
            source_row.slot
        );
        decompressed.clear();
        decompressed
            .try_reserve(source_row.uncompressed_len as usize)
            .context("reserve decompressed block")?;
        let decompressed_len = decompressor
            .decompress_to_buffer(&compressed, &mut decompressed)
            .with_context(|| format!("decompress slot {}", source_row.slot))?;
        ensure!(
            decompressed_len == source_row.uncompressed_len as usize
                && decompressed.len() == source_row.uncompressed_len as usize,
            "slot {} decompressed length differs from its hot index",
            source_row.slot
        );
        let block =
            deserialize_archive_v2_hot_block_blob_borrowed_current_with_preallocation_limit::<
                TRUSTED_HOT_BLOCK_PREALLOCATION_LIMIT_BYTES,
            >(&decompressed)
            .map_err(anyhow::Error::new)
            .with_context(|| {
                format!(
                    "decode slot {} with the current outer hot-block schema",
                    source_row.slot
                )
            })?;
        ensure!(
            block.header.slot == source_row.slot
                && block.tx_count == source_row.tx_count
                && block.tx_rows_len() == block.tx_count as usize,
            "slot {} block identity or transaction count differs from its index",
            source_row.slot
        );
        repaired_rows.clear();
        repaired_rows.extend(block.tx_rows());
        validate_source_block_row_geometry(
            &source_row,
            &repaired_rows,
            block.message_bytes,
            block.metadata_bytes,
        )?;
        let block_metadata = normalize_metadata_region(
            &mut repaired_rows,
            block.message_bytes,
            block.metadata_bytes,
            &mut repaired_metadata,
            limits,
            block.header.slot,
        )?;
        metadata.add(block_metadata)?;
        assert_normalized_row_construction(block.tx_rows(), &repaired_rows, block.header.slot)?;

        serialized.clear();
        if serialized.capacity() < decompressed.len() {
            serialized
                .try_reserve_exact(decompressed.len())
                .context("reserve repaired block serialization")?;
        }
        serialize_current_block_parts(
            &block.header,
            block.tx_count,
            &repaired_rows,
            block.message_bytes,
            &repaired_metadata,
            &mut serialized,
        )?;
        verify_serialized_geometry(
            &serialized,
            &block.header,
            &repaired_rows,
            block.message_bytes,
            &repaired_metadata,
        )?;

        let copy_source_frame = serialized == decompressed
            && options.source_zstd_level == options.target_zstd_level
            && options.source_index_flags == 0;
        let output_frame = if copy_source_frame {
            frames.observe_copy(compressed.len() as u64)?;
            compressed.as_slice()
        } else {
            target_compressed.clear();
            let bound = zstd::zstd_safe::compress_bound(serialized.len());
            if target_compressed.capacity() < bound {
                target_compressed
                    .try_reserve_exact(bound)
                    .context("reserve target compressed block")?;
            }
            compressor
                .compress_to_buffer(&serialized, &mut target_compressed)
                .with_context(|| format!("compress repaired slot {}", source_row.slot))?;
            ensure!(
                !target_compressed.is_empty(),
                "slot {} compressed to zero bytes",
                source_row.slot
            );
            frames
                .observe_recompression(compressed.len() as u64, target_compressed.len() as u64)?;
            target_compressed.as_slice()
        };
        target
            .write_all(output_frame)
            .with_context(|| format!("write repaired slot {}", source_row.slot))?;
        let compressed_len = u32::try_from(output_frame.len())
            .context("target compressed block exceeds u32::MAX")?;
        let uncompressed_len =
            u32::try_from(serialized.len()).context("target block exceeds u32::MAX")?;
        let mut target_row = source_row;
        target_row.compressed_offset = target_offset;
        target_row.compressed_len = compressed_len;
        target_row.uncompressed_len = uncompressed_len;
        target_rows.push(target_row);
        target_offset = checked_add(
            target_offset,
            u64::from(compressed_len),
            "target compressed offset",
        )?;
        transactions = checked_add(
            transactions,
            u64::from(source_row.tx_count),
            "transaction count",
        )?;

        let blocks = (number + 1) as u64;
        if options.progress_blocks != 0
            && (blocks.is_multiple_of(options.progress_blocks) || number + 1 == source_rows.len())
        {
            eprintln!(
                "repair_progress blocks={blocks} total_blocks={} source_bytes={} target_bytes={} legacy_error_records={} copied_blocks={} recompressed_blocks={}",
                source_rows.len(),
                source_row.compressed_offset + u64::from(source_row.compressed_len),
                target_offset,
                metadata.legacy_error_records,
                frames.copied_blocks,
                frames.recompressed_blocks,
            );
        }
    }
    let mut trailing = [0u8; 1];
    ensure!(
        source.read(&mut trailing)? == 0,
        "source block blob has bytes outside its hot index"
    );
    target.flush()?;
    target.get_ref().sync_all()?;
    ensure!(
        frames.copied_blocks + frames.recompressed_blocks == source_rows.len() as u64,
        "frame accounting does not cover every source block"
    );

    Ok(RewriteOutput {
        target_rows,
        blocks: source_rows.len() as u64,
        transactions,
        target_bytes: target_offset,
        metadata,
        frames,
    })
}

fn normalize_metadata_region(
    rows: &mut [ArchiveV2HotTxRow],
    source_messages: &[u8],
    source_metadata: &[u8],
    target_metadata: &mut Vec<u8>,
    limits: ArchiveV2WireRewriteLimits,
    slot: u64,
) -> Result<MetadataCounts> {
    target_metadata.clear();
    if target_metadata.capacity() < source_metadata.len() {
        target_metadata
            .try_reserve_exact(source_metadata.len())
            .context("reserve repaired metadata region")?;
    }
    let mut message_cursor = 0usize;
    let mut metadata_cursor = 0usize;
    let mut counts = MetadataCounts::default();
    let mut visitor = ArchiveV2WireIdentityVisitor;

    for row in rows {
        ensure!(
            row.reserved == [0; 3],
            "slot {slot} tx {} has non-zero reserved bytes",
            row.tx_index
        );
        ensure!(
            row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK == 0,
            "slot {slot} tx {} is a raw transaction fallback",
            row.tx_index
        );
        ensure!(
            row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK == 0,
            "slot {slot} tx {} is a raw metadata fallback",
            row.tx_index
        );
        ensure!(
            row.message_offset as usize == message_cursor,
            "slot {slot} tx {} has a non-canonical message offset",
            row.tx_index
        );
        message_cursor = message_cursor
            .checked_add(row.message_len as usize)
            .context("message cursor overflow")?;
        ensure!(
            message_cursor <= source_messages.len(),
            "slot {slot} tx {} message range is outside the block",
            row.tx_index
        );

        let source_offset = row.metadata_offset as usize;
        let source_len = row.metadata_len as usize;
        row.metadata_offset = u32::try_from(target_metadata.len())
            .context("repaired metadata region exceeds u32::MAX")?;
        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                source_len == 0,
                "slot {slot} tx {} has metadata bytes without HAS_METADATA",
                row.tx_index
            );
            ensure!(
                row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR == 0,
                "slot {slot} tx {} has HAS_ERROR without metadata",
                row.tx_index
            );
            row.metadata_len = 0;
            continue;
        }
        ensure!(
            source_len > 0 && source_offset == metadata_cursor,
            "slot {slot} tx {} has an empty or non-canonical metadata range",
            row.tx_index
        );
        metadata_cursor = metadata_cursor
            .checked_add(source_len)
            .context("metadata cursor overflow")?;
        ensure!(
            metadata_cursor <= source_metadata.len(),
            "slot {slot} tx {} metadata range is outside the block",
            row.tx_index
        );
        let input = &source_metadata[source_offset..metadata_cursor];
        ensure!(
            input.len() <= limits.max_input_bytes,
            "slot {slot} tx {} metadata exceeds the input limit",
            row.tx_index
        );
        let has_error = input.first() == Some(&1);
        ensure!(
            has_error == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0),
            "slot {slot} tx {} HAS_ERROR flag differs from metadata",
            row.tx_index
        );

        let output_start = target_metadata.len();
        let selected_schema =
            match rewrite_archive_v2_metadata_wire(input, target_metadata, &mut visitor, limits) {
                Ok(stats) => {
                    ensure!(
                        stats.input_bytes == input.len()
                            && stats.output_bytes == target_metadata.len() - output_start,
                        "slot {slot} tx {} metadata rewrite coverage differs",
                        row.tx_index
                    );
                    stats.metadata_error_schema
                }
                Err(error)
                    if matches!(error.kind(), ArchiveV2WireRewriteErrorKind::Fallback(_)) =>
                {
                    ensure!(
                        target_metadata.len() == output_start,
                        "slot {slot} tx {} fallback did not roll back output",
                        row.tx_index
                    );
                    if error.fallback_reason()
                        == Some(ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous)
                    {
                        counts.ambiguous_error_records = checked_add(
                            counts.ambiguous_error_records,
                            1,
                            "ambiguous metadata error count",
                        )?;
                    }
                    let (canonical, schema) = canonicalize_archive_v2_metadata_owned(input)
                        .with_context(|| {
                            format!(
                                "repair slot {slot} tx {} through the exact dual decoder",
                                row.tx_index
                            )
                        })?;
                    ensure!(
                        canonical.len() <= limits.max_output_bytes,
                        "slot {slot} tx {} repaired metadata exceeds the output limit",
                        row.tx_index
                    );
                    target_metadata.extend_from_slice(&canonical);
                    has_error.then_some(schema)
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error)
                        .context(format!("repair slot {slot} tx {} metadata", row.tx_index)));
                }
            };

        if !has_error {
            ensure!(
                selected_schema.is_none() && &target_metadata[output_start..] == input,
                "slot {slot} tx {} successful metadata changed",
                row.tx_index
            );
        } else {
            match selected_schema.context("error metadata has no selected source schema")? {
                ArchiveV2WireMetadataErrorSchema::Current => {
                    counts.current_error_records = checked_add(
                        counts.current_error_records,
                        1,
                        "current metadata error count",
                    )?;
                }
                ArchiveV2WireMetadataErrorSchema::Legacy => {
                    counts.legacy_error_records = checked_add(
                        counts.legacy_error_records,
                        1,
                        "legacy metadata error count",
                    )?;
                }
            }
        }
        let output = &target_metadata[output_start..];
        validate_archive_v2_metadata_error_prefix_for_selected_schema(
            output,
            ArchiveV2WireMetadataErrorSchema::Current,
            limits.max_output_bytes,
        )
        .map_err(anyhow::Error::new)
        .with_context(|| {
            format!(
                "validate slot {slot} tx {} repaired current error prefix",
                row.tx_index
            )
        })?;
        row.metadata_len =
            u32::try_from(output.len()).context("one repaired metadata record exceeds u32::MAX")?;
        counts.records = checked_add(counts.records, 1, "metadata record count")?;
        counts.input_bytes = checked_add(
            counts.input_bytes,
            input.len() as u64,
            "metadata input bytes",
        )?;
        counts.output_bytes = checked_add(
            counts.output_bytes,
            output.len() as u64,
            "metadata output bytes",
        )?;
    }
    ensure!(
        message_cursor == source_messages.len(),
        "slot {slot} transaction rows do not cover all message bytes"
    );
    ensure!(
        metadata_cursor == source_metadata.len(),
        "slot {slot} transaction rows do not cover all metadata bytes"
    );
    Ok(counts)
}

fn validate_tx_index_permutation(rows: &[ArchiveV2HotTxRow], slot: u64) -> Result<()> {
    if rows
        .iter()
        .enumerate()
        .all(|(number, row)| row.tx_index == number as u32)
    {
        return Ok(());
    }
    let mut seen = vec![false; rows.len()];
    for row in rows {
        let index = row.tx_index as usize;
        ensure!(
            index < rows.len(),
            "slot {slot} has out-of-range tx_index {}",
            row.tx_index
        );
        ensure!(
            !seen[index],
            "slot {slot} has duplicate tx_index {}",
            row.tx_index
        );
        seen[index] = true;
    }
    Ok(())
}

struct BoundedVecWriter<'a> {
    output: &'a mut Vec<u8>,
}

impl wincode::io::Writer for BoundedVecWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> wincode::io::WriteResult<()> {
        let next = self
            .output
            .len()
            .checked_add(bytes.len())
            .ok_or(wincode::io::WriteError::WriteSizeLimit(usize::MAX))?;
        if next > u32::MAX as usize {
            return Err(wincode::io::WriteError::WriteSizeLimit(next));
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

fn serialize_current_block_parts(
    header: &ArchiveV2HotBlockHeader,
    tx_count: u32,
    rows: &[ArchiveV2HotTxRow],
    messages: &[u8],
    metadata: &[u8],
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    wincode::config::serialize_into(
        BoundedVecWriter { output },
        &(header, tx_count, rows, messages, metadata),
        wincode_leb128_config(),
    )?;
    Ok(())
}

fn verify_serialized_geometry(
    serialized: &[u8],
    expected_header: &ArchiveV2HotBlockHeader,
    expected_rows: &[ArchiveV2HotTxRow],
    expected_messages: &[u8],
    expected_metadata: &[u8],
) -> Result<()> {
    let decoded =
        deserialize_archive_v2_hot_block_blob_borrowed_current_with_preallocation_limit::<
            TRUSTED_HOT_BLOCK_PREALLOCATION_LIMIT_BYTES,
        >(serialized)
        .map_err(anyhow::Error::new)
        .context("verify repaired current outer block")?;
    ensure!(
        same_hot_block_header(&decoded.header, expected_header),
        "repaired block header or rewards changed"
    );
    ensure!(
        decoded.tx_count as usize == expected_rows.len(),
        "repaired block transaction count changed"
    );
    ensure!(
        decoded.message_bytes == expected_messages,
        "repaired block message bytes changed"
    );
    ensure!(
        decoded.metadata_bytes == expected_metadata,
        "repaired block metadata bytes changed after serialization"
    );
    ensure!(
        decoded.tx_rows().eq(expected_rows.iter().copied()),
        "repaired transaction rows changed after serialization"
    );
    Ok(())
}

fn same_hot_block_header(left: &ArchiveV2HotBlockHeader, right: &ArchiveV2HotBlockHeader) -> bool {
    left.slot == right.slot
        && left.parent_slot == right.parent_slot
        && left.blockhash_id == right.blockhash_id
        && left.previous_blockhash_id == right.previous_blockhash_id
        && left.block_time == right.block_time
        && left.block_height == right.block_height
        && match (&left.rewards, &right.rewards) {
            (None, None) => true,
            (Some(left), Some(right)) => {
                left.num_partitions == right.num_partitions
                    && left.decoded.len() == right.decoded.len()
                    && left
                        .decoded
                        .iter()
                        .zip(&right.decoded)
                        .all(|(left, right)| {
                            left.pubkey == right.pubkey
                                && left.lamports == right.lamports
                                && left.post_balance == right.post_balance
                                && left.reward_type == right.reward_type
                                && left.commission == right.commission
                        })
            }
            _ => false,
        }
}

fn rebuild_get_block_if_present(
    source: &PinnedLocalRangeSource,
    staging: &Path,
    source_files: &[SourcePayloadFile],
    source_rows: &[ArchiveV2HotBlockIndexRow],
    target_rows: &[ArchiveV2HotBlockIndexRow],
    epoch_start_slot: u64,
    slots_per_epoch: u64,
) -> Result<Option<u64>> {
    let Some(get_block_bytes) = source
        .size(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)
        .map_err(anyhow::Error::new)?
    else {
        return Ok(None);
    };
    ensure!(
        get_block_bytes <= MAX_SOURCE_INDEX_BYTES as u64,
        "get-block index exceeds the repair limit"
    );
    let source_get_block = source
        .read_all_bounded(ARCHIVE_V2_GET_BLOCK_INDEX_FILE, MAX_SOURCE_INDEX_BYTES)
        .map_err(anyhow::Error::new)?;
    let source_get_block_rows = parse_get_block_index_bytes(&source_get_block)?;
    let access_index_path = staging.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let access_index = read_archive_v2_block_access_index(&access_index_path)?;
    ensure!(
        access_index.flags == 0,
        "block-access index has unsupported flags {:#x}",
        access_index.flags
    );
    let access_blob_bytes = source_files
        .iter()
        .find(|file| file.name == ARCHIVE_V2_BLOCK_ACCESS_FILE)
        .context("get-block index exists without its access blob")?
        .bytes;
    ensure!(
        access_index.blob_file_bytes == access_blob_bytes,
        "block-access blob length differs from its index"
    );
    let (target_get_block_rows, rebuilt) = rebuild_get_block_rows(
        &source_get_block_rows,
        source_rows,
        target_rows,
        &access_index.rows,
        access_blob_bytes,
        epoch_start_slot,
        slots_per_epoch,
    )?;
    let target_path = staging.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
    ensure!(
        !target_path.exists(),
        "fresh staging unexpectedly contains a get-block index"
    );
    write_archive_v2_get_block_index(&target_path, &target_get_block_rows)?;
    sync_regular_file(&target_path)?;
    let persisted = read_archive_v2_get_block_index(&target_path)?;
    ensure!(
        persisted.rows.len() == target_get_block_rows.len()
            && persisted
                .rows
                .iter()
                .zip(&target_get_block_rows)
                .all(|(left, right)| same_get_block_row(*left, *right)),
        "persisted get-block index differs from its rebuilt rows"
    );
    Ok(Some(rebuilt))
}

fn parse_get_block_index_bytes(bytes: &[u8]) -> Result<Vec<ArchiveV2GetBlockIndexRow>> {
    ensure!(
        bytes
            .len()
            .is_multiple_of(ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN),
        "get-block index length is not a row multiple"
    );
    let mut rows = Vec::new();
    rows.try_reserve_exact(bytes.len() / ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN)
        .context("reserve get-block rows")?;
    for (slot_offset, row) in bytes
        .chunks_exact(ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN)
        .enumerate()
    {
        let parsed = ArchiveV2GetBlockIndexRow {
            block_offset: u64::from_le_bytes(row[0..8].try_into().unwrap()),
            block_len: u32::from_le_bytes(row[8..12].try_into().unwrap()),
            access_offset: u64::from_le_bytes(row[12..20].try_into().unwrap()),
            access_len: u32::from_le_bytes(row[20..24].try_into().unwrap()),
        };
        ensure!(
            u64::from(parsed.access_len) <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
            "get-block row {slot_offset} access length exceeds the shared limit"
        );
        rows.push(parsed);
    }
    Ok(rows)
}

fn rebuild_get_block_rows(
    source_get_block_rows: &[ArchiveV2GetBlockIndexRow],
    source_hot_rows: &[ArchiveV2HotBlockIndexRow],
    target_hot_rows: &[ArchiveV2HotBlockIndexRow],
    access_rows: &[ArchiveV2BlockAccessIndexRow],
    access_blob_bytes: u64,
    epoch_start_slot: u64,
    slots_per_epoch: u64,
) -> Result<(Vec<ArchiveV2GetBlockIndexRow>, u64)> {
    ensure!(
        source_hot_rows.len() == target_hot_rows.len(),
        "source and target hot indexes have different row counts"
    );
    ensure!(
        access_rows.len() == source_hot_rows.len(),
        "block-access and hot indexes have different row counts"
    );
    ensure!(
        source_get_block_rows.len()
            == usize::try_from(slots_per_epoch).context("slots_per_epoch exceeds usize")?,
        "get-block index has {} rows, expected {slots_per_epoch}",
        source_get_block_rows.len()
    );
    let mut access_cursor = 0u64;
    let mut by_slot = BTreeMap::new();
    for (number, ((source, target), access)) in source_hot_rows
        .iter()
        .zip(target_hot_rows)
        .zip(access_rows)
        .enumerate()
    {
        ensure!(
            source.block_id == target.block_id && source.slot == target.slot,
            "target hot index changed block identity"
        );
        ensure!(
            access.block_id as usize == number
                && access.block_id == source.block_id
                && access.slot == source.slot,
            "block-access row {number} identity differs from the hot index"
        );
        ensure!(
            access.tx_count == source.tx_count && access.signature_count == source.signature_count,
            "block-access row {number} count differs from the hot index"
        );
        ensure!(
            access.access_offset == access_cursor && access.access_len > 0,
            "block-access row {number} has non-canonical geometry"
        );
        access_cursor = checked_add(
            access_cursor,
            u64::from(access.access_len),
            "block-access cursor",
        )?;
        ensure!(
            by_slot.insert(source.slot, number).is_none(),
            "hot index contains a duplicate slot"
        );
    }
    ensure!(
        access_cursor == access_blob_bytes,
        "block-access index covers {access_cursor} of {access_blob_bytes} bytes"
    );

    let mut output = source_get_block_rows.to_vec();
    let mut rebuilt = 0u64;
    for (slot_offset, row) in output.iter_mut().enumerate() {
        let slot = epoch_start_slot
            .checked_add(slot_offset as u64)
            .context("get-block slot overflows u64")?;
        let Some(&number) = by_slot.get(&slot) else {
            ensure!(
                get_block_row_is_exactly_missing(*row),
                "get-block row {slot_offset} declares data for missing slot {slot}"
            );
            continue;
        };
        let source = source_hot_rows[number];
        let target = target_hot_rows[number];
        let access = access_rows[number];
        ensure!(
            row.block_offset == source.compressed_offset
                && row.block_len == source.compressed_len
                && row.access_offset == access.access_offset
                && row.access_len == access.access_len,
            "get-block row {slot_offset} differs from source indexes for slot {slot}"
        );
        row.block_offset = target.compressed_offset;
        row.block_len = target.compressed_len;
        rebuilt = checked_add(rebuilt, 1, "rebuilt get-block row count")?;
    }
    ensure!(
        rebuilt == source_hot_rows.len() as u64,
        "get-block index covers {rebuilt} of {} hot blocks",
        source_hot_rows.len()
    );
    Ok((output, rebuilt))
}

fn get_block_row_is_exactly_missing(row: ArchiveV2GetBlockIndexRow) -> bool {
    row.block_offset == 0 && row.block_len == 0 && row.access_offset == 0 && row.access_len == 0
}

fn same_get_block_row(left: ArchiveV2GetBlockIndexRow, right: ArchiveV2GetBlockIndexRow) -> bool {
    left.block_offset == right.block_offset
        && left.block_len == right.block_len
        && left.access_offset == right.access_offset
        && left.access_len == right.access_len
}

fn assert_exact_staging_inventory(
    staging: &PinnedLocalRangeSource,
    source_files: &[SourcePayloadFile],
    has_get_block: bool,
) -> Result<()> {
    let mut expected = BTreeSet::from([
        ARCHIVE_V2_BLOCKS_FILE.to_owned(),
        ARCHIVE_V2_BLOCK_INDEX_FILE.to_owned(),
    ]);
    expected.extend(source_files.iter().map(|file| file.name.to_owned()));
    if has_get_block {
        expected.insert(ARCHIVE_V2_GET_BLOCK_INDEX_FILE.to_owned());
    }
    let inventory = staging.inventory().map_err(anyhow::Error::new)?;
    let mut actual = BTreeSet::new();
    for entry in inventory {
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile,
            "staging contains a non-regular entry"
        );
        let name = entry
            .name
            .to_str()
            .context("staging contains a non-UTF-8 name")?;
        actual.insert(name.to_owned());
    }
    ensure!(
        actual == expected,
        "staging payload inventory differs from expected files"
    );
    Ok(())
}

fn checked_add(left: u64, right: u64, name: &'static str) -> Result<u64> {
    left.checked_add(right)
        .with_context(|| format!("{name} overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_archive_v2::{ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_METADATA, ArchiveV2BlockAccessIndexRow, ArchiveV2HotBlockBlob, ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload, WINCODE_ARCHIVE_V2_FLAG_LEB128, WincodeArchiveV2Footer, WincodeArchiveV2Header, read_archive_v2_hot_block_index, write_archive_v2_block_access_index};
    use blockzilla_compact::{CompactInstructionError, CompactMessageHeader, CompactTransactionError, OwnedCompactRecentBlockhash};
    use blockzilla_primitives::{CompactPubkey, WincodeLeb128FramedWriter};
    use blockzilla_registry::KeyIndex;
    use of_car_reader::stored_transaction::{
        InstructionError as StoredInstructionError, StoredTransactionError,
    };
    use tempfile::TempDir;

    fn empty_metadata(error: Option<CompactTransactionError>) -> CompactMetaV1 {
        CompactMetaV1 {
            err: error,
            fee: 5,
            pre_balances: Vec::new(),
            post_balances: Vec::new(),
            inner_instructions: None,
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards: Vec::new(),
            loaded_writable_addresses: Vec::new(),
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn current_metadata(error: Option<CompactTransactionError>) -> Vec<u8> {
        wincode::config::serialize(&empty_metadata(error), wincode_leb128_config()).unwrap()
    }

    fn legacy_error_metadata(instruction_index: u8) -> Vec<u8> {
        let message = "legacy-error".repeat(12);
        let current_error = CompactTransactionError::InstructionError(
            instruction_index,
            CompactInstructionError::BorshIoError(message.clone()),
        );
        let current_error_bytes =
            wincode::config::serialize(&current_error, wincode_leb128_config()).unwrap();
        let current = current_metadata(Some(current_error));
        let stored = wincode::serialize(&StoredTransactionError::InstructionError(
            instruction_index,
            StoredInstructionError::BorshIoError(message),
        ))
        .unwrap();
        let stored_vec = wincode::config::serialize(&stored, wincode_leb128_config()).unwrap();
        let mut legacy = vec![1];
        legacy.extend_from_slice(&stored_vec);
        legacy.extend_from_slice(&current[1 + current_error_bytes.len()..]);
        legacy
    }

    fn transaction_row(
        tx_index: u32,
        message_offset: u32,
        message_len: u32,
        metadata_offset: u32,
        metadata_len: u32,
        has_error: bool,
    ) -> ArchiveV2HotTxRow {
        ArchiveV2HotTxRow {
            tx_index,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | if has_error {
                    ARCHIVE_V2_TX_FLAG_HAS_ERROR
                } else {
                    0
                },
            message_offset,
            message_len,
            metadata_offset,
            metadata_len,
            signature_count: 1,
            reserved: [0; 3],
        }
    }

    fn assert_current_canonical(bytes: &[u8]) {
        let current: CompactMetaV1 =
            wincode::config::deserialize_exact(bytes, wincode_leb128_config()).unwrap();
        let encoded = wincode::config::serialize(&current, wincode_leb128_config()).unwrap();
        assert_eq!(encoded, bytes);
    }

    #[test]
    fn metadata_region_repairs_legacy_error_and_rebuilds_offsets() {
        let success = current_metadata(None);
        let legacy = legacy_error_metadata(0);
        let mut input = success.clone();
        input.extend_from_slice(&legacy);
        let mut rows = vec![
            transaction_row(0, 0, 1, 0, success.len() as u32, false),
            transaction_row(1, 1, 1, success.len() as u32, legacy.len() as u32, true),
        ];
        let source_rows = rows.clone();
        let mut output = Vec::new();
        let counts = normalize_metadata_region(
            &mut rows,
            &[1, 2],
            &input,
            &mut output,
            ArchiveV2WireRewriteLimits {
                max_input_bytes: 1 << 20,
                max_output_bytes: 1 << 20,
                ..ArchiveV2WireRewriteLimits::default()
            },
            10,
        )
        .unwrap();

        assert_eq!(counts.records, 2);
        assert_eq!(counts.current_error_records, 0);
        assert_eq!(counts.legacy_error_records, 1);
        assert_eq!(&output[..success.len()], success);
        assert_eq!(rows[0].metadata_offset, 0);
        assert_eq!(rows[0].metadata_len as usize, success.len());
        assert_eq!(rows[1].metadata_offset as usize, success.len());
        assert_ne!(rows[1].metadata_len, source_rows[1].metadata_len);
        assert_current_canonical(&output[rows[1].metadata_offset as usize..]);
        assert_eq!(input[..success.len()], success);
        assert_eq!(input[success.len()..], legacy);
    }

    fn test_message() -> Vec<u8> {
        wincode::config::serialize(
            &ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header: CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 1,
                },
                account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: Vec::new(),
                    data: ArchiveV2HotInstructionData::Raw(vec![7]),
                }],
            }),
            wincode_leb128_config(),
        )
        .unwrap()
    }

    fn make_frame(slot: u64, metadata: Vec<u8>, has_error: bool) -> (Vec<u8>, Vec<u8>) {
        let message = test_message();
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot,
                parent_slot: slot - 1,
                blockhash_id: 1,
                previous_blockhash_id: 0,
                block_time: Some(1_700_000_000 + slot as i64),
                block_height: Some(slot),
                rewards: None,
            },
            tx_count: 1,
            tx_rows: vec![transaction_row(
                0,
                0,
                message.len() as u32,
                0,
                metadata.len() as u32,
                has_error,
            )],
            message_bytes: message,
            metadata_bytes: metadata,
        };
        let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
        (uncompressed, compressed)
    }

    fn write_fixture(source: &Path) {
        fs::create_dir(source).unwrap();
        let (first_uncompressed, first_compressed) = make_frame(10, current_metadata(None), false);
        let (second_uncompressed, second_compressed) =
            make_frame(12, legacy_error_metadata(0), true);
        let mut blocks = first_compressed.clone();
        blocks.extend_from_slice(&second_compressed);
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), &blocks).unwrap();
        write_archive_v2_hot_block_index(
            &source.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            blocks.len() as u64,
            3,
            0,
            &[
                ArchiveV2HotBlockIndexRow {
                    block_id: 0,
                    slot: 10,
                    compressed_offset: 0,
                    compressed_len: first_compressed.len() as u32,
                    uncompressed_len: first_uncompressed.len() as u32,
                    tx_count: 1,
                    first_tx_ordinal: 0,
                    first_signature_ordinal: 0,
                    signature_count: 1,
                },
                ArchiveV2HotBlockIndexRow {
                    block_id: 1,
                    slot: 12,
                    compressed_offset: first_compressed.len() as u64,
                    compressed_len: second_compressed.len() as u32,
                    uncompressed_len: second_uncompressed.len() as u32,
                    tx_count: 1,
                    first_tx_ordinal: 1,
                    first_signature_ordinal: 1,
                    signature_count: 1,
                },
            ],
        )
        .unwrap();

        let access = b"accessaccess";
        fs::write(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE), access).unwrap();
        write_archive_v2_block_access_index(
            &source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
            access.len() as u64,
            0,
            &[
                ArchiveV2BlockAccessIndexRow {
                    block_id: 0,
                    slot: 10,
                    access_offset: 0,
                    access_len: 6,
                    tx_count: 1,
                    signature_count: 1,
                },
                ArchiveV2BlockAccessIndexRow {
                    block_id: 1,
                    slot: 12,
                    access_offset: 6,
                    access_len: 6,
                    tx_count: 1,
                    signature_count: 1,
                },
            ],
        )
        .unwrap();
        let mut get_block = vec![ArchiveV2GetBlockIndexRow::missing(); 20];
        get_block[10] = ArchiveV2GetBlockIndexRow {
            block_offset: 0,
            block_len: first_compressed.len() as u32,
            access_offset: 0,
            access_len: 6,
        };
        get_block[12] = ArchiveV2GetBlockIndexRow {
            block_offset: first_compressed.len() as u64,
            block_len: second_compressed.len() as u32,
            access_offset: 6,
            access_len: 6,
        };
        write_archive_v2_get_block_index(&source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE), &get_block)
            .unwrap();

        let registry = vec![[1u8; 32], [2u8; 32]];
        fs::write(
            source.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            registry.concat(),
        )
        .unwrap();
        KeyIndex::build(registry)
            .write(&source.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();
        fs::write(source.join(ARCHIVE_V2_SIGNATURES_FILE), [9u8; 128]).unwrap();

        let meta = File::create(source.join(ARCHIVE_V2_META_FILE)).unwrap();
        let mut meta = WincodeLeb128FramedWriter::new(meta);
        meta.write(&ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
        }))
        .unwrap();
        meta.write(&ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
            blocks: 2,
            transactions: 2,
            ..WincodeArchiveV2Footer::default()
        }))
        .unwrap();
        meta.flush().unwrap();
        fs::write(source.join("operator-note.txt"), b"must not be copied").unwrap();
    }

    fn directory_bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
        fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name().into_string().unwrap(),
                    fs::read(entry.path()).unwrap(),
                )
            })
            .collect()
    }

    fn fixture_paths() -> (TempDir, PathBuf, PathBuf) {
        let temp = TempDir::new().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let source = root.join("source");
        let staging = root.join("staging");
        (temp, source, staging)
    }

    #[test]
    fn complete_repair_writes_fresh_current_payload_and_preserves_source() {
        let (_temp, source, staging) = fixture_paths();
        write_fixture(&source);
        let before = directory_bytes(&source);
        let summary = normalize_trusted_metadata(&TrustedMetadataNormalizeOptions {
            source: source.clone(),
            staging: staging.clone(),
            epoch: 0,
            slots_per_epoch: 20,
            zstd_level: 1,
            max_metadata_bytes: 1 << 20,
            progress_blocks: 0,
        })
        .unwrap();

        assert_eq!(summary.blocks, 2);
        assert_eq!(summary.transactions, 2);
        assert_eq!(summary.legacy_error_records, 1);
        assert_eq!(summary.validated_output_blocks, 2);
        assert_eq!(summary.rebuilt_get_block_rows, 2);
        assert_eq!(directory_bytes(&source), before);
        assert!(!staging.join("operator-note.txt").exists());

        let target_index =
            read_archive_v2_hot_block_index(&staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        assert_eq!(target_index.level, 1);
        let target_get =
            read_archive_v2_get_block_index(&staging.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE))
                .unwrap();
        assert_eq!(target_get.rows[10].access_offset, 0);
        assert_eq!(target_get.rows[10].access_len, 6);
        assert_eq!(
            target_get.rows[12].block_offset,
            target_index.rows[1].compressed_offset
        );
        assert_eq!(
            target_get.rows[12].block_len,
            target_index.rows[1].compressed_len
        );

        let target = PinnedLocalRangeSource::open_directory(&staging).unwrap();
        assert_eq!(
            validate_persisted_output_blocks(
                target.open_file(ARCHIVE_V2_BLOCKS_FILE).unwrap(),
                &target_index.rows,
            )
            .unwrap(),
            2
        );
    }

    #[test]
    fn existing_staging_is_rejected_before_source_changes() {
        let (_temp, source, staging) = fixture_paths();
        write_fixture(&source);
        fs::create_dir(&staging).unwrap();
        let before = directory_bytes(&source);
        let error = normalize_trusted_metadata(&TrustedMetadataNormalizeOptions {
            source: source.clone(),
            staging,
            epoch: 0,
            slots_per_epoch: 20,
            zstd_level: 1,
            max_metadata_bytes: 1 << 20,
            progress_blocks: 0,
        })
        .unwrap_err();
        assert!(error.to_string().contains("staging already exists"));
        assert_eq!(directory_bytes(&source), before);
    }
}
