//! Build a local, unpublished Archive V2 staging generation whose metadata
//! uses the canonical current typed transaction-error wire format.
//!
//! This program never changes or publishes the source generation. It writes a
//! fresh staging directory and leaves publication to a separate audited step.

mod archive_v2_source_authority_common;

use anyhow::{Context, Result, anyhow, ensure};
use archive_v2_source_authority_common::{
    AuthorityDisposition as SourceDisposition, SourceAuthorityInventory, known_disposition,
    looks_like_archive_or_control,
};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN, ARCHIVE_V2_BLOCK_ACCESS_INDEX_MAGIC,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN, ARCHIVE_V2_BLOCK_ACCESS_INDEX_VERSION,
    ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES, ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_GET_BLOCK_INDEX_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN,
    ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY, ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS,
    ARCHIVE_V2_HOT_INDEX_HEADER_LEN, ARCHIVE_V2_HOT_INDEX_MAGIC, ARCHIVE_V2_HOT_INDEX_ROW_LEN,
    ARCHIVE_V2_HOT_INDEX_VERSION, ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ArchiveV2BlockAccessIndex, ArchiveV2BlockAccessIndexRow, ArchiveV2GetBlockIndexRow,
    ArchiveV2HotBlockHeader, ArchiveV2HotBlockIndex, ArchiveV2HotBlockIndexRow, ArchiveV2HotTxRow,
    ArchiveV2WireFallbackReason, ArchiveV2WireIdentityVisitor, ArchiveV2WireMetadataErrorSchema,
    ArchiveV2WireRewriteErrorKind, ArchiveV2WireRewriteLimits,
    canonicalize_archive_v2_metadata_owned, deserialize_archive_v2_hot_block_blob_borrowed_current,
    rewrite_archive_v2_metadata_wire, wincode_leb128_config,
};
use blockzilla_read_sdk::{
    ArchiveReader, ArchiveV2MetadataProfileAdmission, ArchiveV2MetadataSchemaClassification,
    ArchiveV2MetadataSchemaClassifier, ArchiveV2MetadataSchemaCounts, ArchiveV2MetadataWireProfile,
    ArchiveV2WireProfile, AuditedCurrentMetadataMarkerPublication,
    CURRENT_TYPED_ERRORS_MARKER_BYTES, CURRENT_TYPED_ERRORS_MARKER_FILE,
    CURRENT_TYPED_ERRORS_MARKER_SHA256, CURRENT_TYPED_ERRORS_MARKER_SIZE, HashVerification,
    OpenOptions as ReaderOpenOptions, PinnedLocalEntryKind, PinnedLocalRangeSource, RangeSource,
    SourceError, SourceResult, audit_current_metadata_for_marker_publication,
    manifest::{
        GENERATION_MANIFEST_FILE, GENERATION_MANIFEST_SCHEMA_VERSION, GenerationFile,
        GenerationManifest, compute_generation_digest,
    },
    wire_profile_marker, wire_profile_marker_bytes,
};
use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{CStr, CString, OsStr, OsString},
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::{
    fd::{AsRawFd, FromRawFd},
    unix::{ffi::OsStrExt, fs::MetadataExt},
};

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!(
    "archive-v2-normalize-metadata requires Linux or macOS descriptor-relative no-follow filesystem APIs"
);

const IO_BUFFER_BYTES: usize = 8 << 20;
const DEFAULT_PROGRESS_BLOCKS: u64 = 10_000;
const DEFAULT_MAX_METADATA_BYTES: usize = 64 << 20;
const DEFAULT_SLOTS_PER_EPOCH: u64 = 432_000;
const MAX_SOURCE_INDEX_BYTES: usize = 256 << 20;
// Keep this equal to the SDK's structurally admitted Archive V2 transaction flags.
const KNOWN_ARCHIVE_V2_TX_FLAGS: u32 = (1 << 11) - 1;
const NORMALIZATION_MANIFEST_FILE: &str = "archive-v2-metadata-normalization.candidate.v1.json";
const NORMALIZATION_RECEIPT_FILE: &str = "archive-v2-metadata-normalization.receipt.v1.json";

#[derive(Debug, Parser)]
#[command(
    name = "archive-v2-normalize-metadata",
    version,
    about = "Build an unpublished Archive V2 staging generation with one metadata wire format"
)]
struct Args {
    /// Existing local Archive V2 generation. The command opens it read-only.
    #[arg(long)]
    source: PathBuf,

    /// External immutable inventory for a source without a generation manifest.
    #[arg(long, requires = "source_authority_sha256")]
    source_authority_inventory: Option<PathBuf>,

    /// Exact SHA-256 of the external source-authority inventory bytes.
    #[arg(long, requires = "source_authority_inventory")]
    source_authority_sha256: Option<String>,

    /// Fresh staging generation directory. It must not exist.
    #[arg(long)]
    staging: PathBuf,

    /// Exact source and target epoch.
    #[arg(long)]
    epoch: u64,

    /// Number of fixed get-block rows in this epoch.
    #[arg(long, default_value_t = DEFAULT_SLOTS_PER_EPOCH)]
    slots_per_epoch: u64,

    /// New local candidate identity. It is not a published generation ID.
    #[arg(long)]
    candidate_id: String,

    /// Zstd level for rewritten target blocks. The source level is the default.
    #[arg(long)]
    zstd_level: Option<i32>,

    /// Maximum accepted input or output bytes for one metadata record.
    #[arg(long, default_value_t = DEFAULT_MAX_METADATA_BYTES)]
    max_metadata_bytes: usize,

    /// Print one progress record after this many blocks. Zero disables it.
    #[arg(long, default_value_t = DEFAULT_PROGRESS_BLOCKS)]
    progress_blocks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct FileBinding {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct NamedFileBinding {
    name: String,
    bytes: u64,
    sha256: String,
}

struct AdmittedSourceAuthority {
    kind: &'static str,
    authority_id: String,
    authority_binding: FileBinding,
    cluster_id: String,
    source_generation_id: String,
    source_generation_digest: String,
    epoch: u64,
    slots_per_epoch: u64,
    message_wire_profile: blockzilla_read_sdk::ArchiveV2WireProfile,
    metadata_wire_profile: ArchiveV2MetadataWireProfile,
    expected_files: BTreeMap<String, (FileBinding, SourceDisposition)>,
    external_document: Option<ExternalAuthorityDocument>,
}

struct ExternalAuthorityDocument {
    file: File,
    identity: FileIdentity,
    binding: FileBinding,
}

struct TargetCandidateAuthority {
    manifest: GenerationManifest,
    marker_bytes: BTreeMap<String, &'static [u8]>,
    message_marker: NamedFileBinding,
    metadata_marker: NamedFileBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct FileIdentity {
    bytes: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    mode: u32,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
    #[cfg(not(unix))]
    modified_unix_nanoseconds: Option<u128>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SourceFileBinding {
    identity: FileIdentity,
    content: FileBinding,
    disposition: SourceDisposition,
}

struct SourceSnapshot {
    source: PinnedLocalRangeSource,
    directory_identity: FileIdentity,
    files: BTreeMap<String, SourceFileBinding>,
    ignored_unrelated_entries: Vec<String>,
}

struct StagingDirectory {
    display_path: PathBuf,
    parent: File,
    directory: File,
    name: OsString,
    identity: FileIdentity,
}

struct StagingLocation {
    display_path: PathBuf,
    parent: File,
    name: OsString,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct MetadataCounts {
    records: u64,
    successful_records: u64,
    current_error_records: u64,
    legacy_error_records: u64,
    owned_fallback_records: u64,
    ambiguous_owned_fallback_records: u64,
    target_current_only_records: u64,
    target_both_equal_records: u64,
    input_bytes: u64,
    output_bytes: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
struct FrameProcessingCounts {
    copied_blocks: u64,
    copied_bytes: u64,
    recompressed_blocks: u64,
    recompressed_source_bytes: u64,
    recompressed_target_bytes: u64,
}

impl FrameProcessingCounts {
    fn observe_copy(&mut self, bytes: u64) -> Result<()> {
        self.copied_blocks = checked_add(self.copied_blocks, 1, "copied frame count")?;
        self.copied_bytes = checked_add(self.copied_bytes, bytes, "copied frame bytes")?;
        Ok(())
    }

    fn observe_recompression(&mut self, source_bytes: u64, target_bytes: u64) -> Result<()> {
        self.recompressed_blocks =
            checked_add(self.recompressed_blocks, 1, "recompressed frame count")?;
        self.recompressed_source_bytes = checked_add(
            self.recompressed_source_bytes,
            source_bytes,
            "recompressed source frame bytes",
        )?;
        self.recompressed_target_bytes = checked_add(
            self.recompressed_target_bytes,
            target_bytes,
            "recompressed target frame bytes",
        )?;
        Ok(())
    }

    fn validate(self, blocks: u64, source_bytes: u64, target_bytes: u64) -> Result<()> {
        ensure!(
            checked_add(
                self.copied_blocks,
                self.recompressed_blocks,
                "processed frame count"
            )? == blocks,
            "frame disposition counts do not cover every block"
        );
        ensure!(
            checked_add(
                self.copied_bytes,
                self.recompressed_source_bytes,
                "processed source frame bytes"
            )? == source_bytes,
            "frame disposition bytes do not cover the source block blob"
        );
        ensure!(
            checked_add(
                self.copied_bytes,
                self.recompressed_target_bytes,
                "processed target frame bytes"
            )? == target_bytes,
            "frame disposition bytes do not cover the target block blob"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct RewriteOptions {
    source_zstd_level: i32,
    source_index_flags: u32,
    target_zstd_level: i32,
    max_metadata_bytes: usize,
    progress_blocks: u64,
}

#[derive(Default)]
struct MetadataClassificationState {
    source_classifier: ArchiveV2MetadataSchemaClassifier,
    source_profile_counts: ArchiveV2MetadataSchemaCounts,
    target_classifier: ArchiveV2MetadataSchemaClassifier,
}

impl MetadataCounts {
    fn add(&mut self, other: Self) -> Result<()> {
        self.records = checked_add(self.records, other.records, "metadata record count")?;
        self.successful_records = checked_add(
            self.successful_records,
            other.successful_records,
            "successful metadata record count",
        )?;
        self.current_error_records = checked_add(
            self.current_error_records,
            other.current_error_records,
            "current-error metadata record count",
        )?;
        self.legacy_error_records = checked_add(
            self.legacy_error_records,
            other.legacy_error_records,
            "legacy-error metadata record count",
        )?;
        self.owned_fallback_records = checked_add(
            self.owned_fallback_records,
            other.owned_fallback_records,
            "owned metadata fallback count",
        )?;
        self.ambiguous_owned_fallback_records = checked_add(
            self.ambiguous_owned_fallback_records,
            other.ambiguous_owned_fallback_records,
            "ambiguous metadata fallback count",
        )?;
        self.target_current_only_records = checked_add(
            self.target_current_only_records,
            other.target_current_only_records,
            "target current-only metadata record count",
        )?;
        self.target_both_equal_records = checked_add(
            self.target_both_equal_records,
            other.target_both_equal_records,
            "target both-equal metadata record count",
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

#[derive(Debug, Serialize)]
struct NormalizationManifest {
    schema_version: u32,
    kind: &'static str,
    state: &'static str,
    metadata_schema: &'static str,
    source: String,
    staging: String,
    epoch: u64,
    slots_per_epoch: u64,
    cluster_id: String,
    source_generation_id: String,
    source_generation_digest: String,
    source_message_wire_profile: String,
    source_metadata_wire_profile: String,
    candidate_id: String,
    target_candidate_digest: String,
    authorized_message_marker: NamedFileBinding,
    authorized_metadata_marker: NamedFileBinding,
    source_authority_kind: &'static str,
    source_authority_id: String,
    source_authority_binding: FileBinding,
    source_directory_identity: FileIdentity,
    source_files: BTreeMap<String, SourceFileBinding>,
    files: BTreeMap<String, FileBinding>,
    frame_processing: FrameProcessingCounts,
    omitted_source_controls: Vec<String>,
    ignored_unrelated_source_entries: Vec<String>,
}

#[derive(Debug, Serialize)]
struct NormalizationReceipt {
    schema_version: u32,
    kind: &'static str,
    state: &'static str,
    canonical_publication_performed: bool,
    source: String,
    staging: String,
    epoch: u64,
    slots_per_epoch: u64,
    completed_unix_seconds: u64,
    source_blocks: FileBinding,
    source_index: FileBinding,
    target_blocks: FileBinding,
    target_index: FileBinding,
    candidate_manifest: FileBinding,
    message_marker: NamedFileBinding,
    metadata_marker: NamedFileBinding,
    cluster_id: String,
    source_generation_id: String,
    source_generation_digest: String,
    target_candidate_id: String,
    target_candidate_digest: String,
    source_authority_kind: &'static str,
    source_authority_id: String,
    source_authority_binding: FileBinding,
    message_wire_profile: String,
    source_metadata_profile: String,
    source_metadata_profile_counts: ArchiveV2MetadataSchemaCounts,
    blocks: u64,
    transactions: u64,
    message_bytes: u64,
    message_sha256: String,
    metadata: MetadataCounts,
    frame_processing: FrameProcessingCounts,
    target_metadata_profile: &'static str,
    target_metadata_profile_counts: ArchiveV2MetadataSchemaCounts,
    copied_sidecars: u64,
    copied_sidecar_bytes: u64,
    get_block_rows_rebuilt: u64,
    target_zstd_level: i32,
    source_revalidated_at_completion: bool,
    source_directory_identity: FileIdentity,
    source_files: BTreeMap<String, SourceFileBinding>,
    ignored_unrelated_source_entries: Vec<String>,
}

#[derive(Debug)]
struct RewriteOutput {
    target_rows: Vec<ArchiveV2HotBlockIndexRow>,
    target_blocks: FileBinding,
    blocks: u64,
    transactions: u64,
    message_bytes: u64,
    message_sha256: String,
    metadata: MetadataCounts,
    source_metadata_profile_counts: ArchiveV2MetadataSchemaCounts,
    frame_processing: FrameProcessingCounts,
}

#[derive(Debug)]
struct SidecarCopyOutput {
    bindings: BTreeMap<String, FileBinding>,
    omitted_controls: Vec<String>,
    copied_files: u64,
    copied_bytes: u64,
}

struct NormalizeOptions<'a> {
    source: &'a Path,
    source_authority_inventory: Option<&'a Path>,
    source_authority_sha256: Option<&'a str>,
    staging: &'a Path,
    epoch: u64,
    slots_per_epoch: u64,
    candidate_id: &'a str,
    requested_zstd_level: Option<i32>,
    max_metadata_bytes: usize,
    progress_blocks: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let receipt = normalize_generation(NormalizeOptions {
        source: &args.source,
        source_authority_inventory: args.source_authority_inventory.as_deref(),
        source_authority_sha256: args.source_authority_sha256.as_deref(),
        staging: &args.staging,
        epoch: args.epoch,
        slots_per_epoch: args.slots_per_epoch,
        candidate_id: &args.candidate_id,
        requested_zstd_level: args.zstd_level,
        max_metadata_bytes: args.max_metadata_bytes,
        progress_blocks: args.progress_blocks,
    })?;
    println!("{}", serde_json::to_string(&receipt)?);
    Ok(())
}

fn normalize_generation(options: NormalizeOptions<'_>) -> Result<NormalizationReceipt> {
    let NormalizeOptions {
        source,
        source_authority_inventory,
        source_authority_sha256,
        staging,
        epoch,
        slots_per_epoch,
        candidate_id,
        requested_zstd_level,
        max_metadata_bytes,
        progress_blocks,
    } = options;
    ensure!(
        max_metadata_bytes > 0,
        "--max-metadata-bytes must be positive"
    );
    ensure!(slots_per_epoch > 0, "--slots-per-epoch must be positive");
    ensure!(!candidate_id.is_empty(), "--candidate-id must not be empty");
    let epoch_start_slot = epoch
        .checked_mul(slots_per_epoch)
        .context("epoch start slot overflows u64")?;

    let descriptor_source = PinnedLocalRangeSource::open_directory(source)
        .map_err(|error| anyhow!(error))
        .context("open the source through one no-follow directory capability")?;
    let source_directory_identity =
        FileIdentity::from_directory_metadata(&descriptor_source.directory_file()?.metadata()?)?;
    let staging_location = prepare_staging_location(staging, &source_directory_identity)?;
    let (authority, source_manifest, source_admission, source_overlay) = admit_source_authority(
        &descriptor_source,
        source_authority_inventory,
        source_authority_sha256,
    )?;
    ensure!(
        authority.epoch == epoch && authority.slots_per_epoch == slots_per_epoch,
        "source authority epoch geometry differs from the command"
    );
    let source_snapshot =
        SourceSnapshot::admit(descriptor_source.clone(), &authority.expected_files)?;
    let source_reader = ArchiveReader::open_candidate_with_metadata_admission(
        source_overlay,
        source_manifest,
        ReaderOpenOptions {
            // SourceSnapshot already hashed every authority-bound file through
            // this same descriptor cache. The reader supplies structural
            // admission here and must not hash blocks.bin a second time.
            hash_verification: HashVerification::SizesOnly,
            ..ReaderOpenOptions::default()
        },
        source_admission,
    )
    .map_err(|error| anyhow!(error))
    .context("admit the complete authority-bound source generation")?;
    ensure!(
        source_reader.manifest().cluster_id == authority.cluster_id
            && source_reader.manifest().epoch == authority.epoch
            && source_reader.manifest().slots_per_epoch == authority.slots_per_epoch
            && source_reader.wire_profile() == authority.message_wire_profile
            && source_reader.metadata_wire_profile() == authority.metadata_wire_profile,
        "source reader identity differs from the selected source authority"
    );
    let source_metadata_profile = source_reader.metadata_wire_profile();

    let source_index_bytes =
        source_snapshot.read_all_bounded(ARCHIVE_V2_BLOCK_INDEX_FILE, MAX_SOURCE_INDEX_BYTES)?;
    let index = parse_hot_index_bytes(&source_index_bytes)?;
    ensure!(
        same_hot_index(&index, source_reader.index()),
        "pinned source hot index differs from complete-generation admission"
    );
    let source_index_binding = source_snapshot
        .binding(ARCHIVE_V2_BLOCK_INDEX_FILE)?
        .content
        .clone();
    ensure!(
        index.flags & (ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY | ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS)
            == 0
            && index.flags == 0,
        "metadata normalizer requires independent dictionary-free zstd frames; flags={:#x}",
        index.flags
    );
    let source_blocks_binding = source_snapshot
        .binding(ARCHIVE_V2_BLOCKS_FILE)?
        .content
        .clone();
    let blocks_bytes = source_blocks_binding.bytes;
    ensure!(
        blocks_bytes == index.blob_file_bytes,
        "source block blob has {blocks_bytes} bytes, but its index declares {}",
        index.blob_file_bytes
    );
    validate_source_index_geometry(&index.rows, index.blob_file_bytes)?;
    validate_epoch_hot_slots(&index.rows, epoch_start_slot, slots_per_epoch)?;

    let staging = staging_location.create()?;
    let sidecars = copy_sidecars(&source_snapshot, &staging)?;
    let target_zstd_level = requested_zstd_level.unwrap_or(index.level);
    let rewrite = rewrite_blocks(
        source_snapshot.file(ARCHIVE_V2_BLOCKS_FILE)?,
        staging.create_file(ARCHIVE_V2_BLOCKS_FILE)?,
        &index.rows,
        RewriteOptions {
            source_zstd_level: index.level,
            source_index_flags: index.flags,
            target_zstd_level,
            max_metadata_bytes,
            progress_blocks,
        },
    )?;
    ensure!(
        source_blocks_binding.bytes == index.blob_file_bytes,
        "source blocks changed length during normalization"
    );
    rewrite.frame_processing.validate(
        rewrite.blocks,
        source_blocks_binding.bytes,
        rewrite.target_blocks.bytes,
    )?;
    ensure!(
        rewrite
            .source_metadata_profile_counts
            .checked_total()
            .map_err(|error| anyhow!(error))?
            == rewrite.metadata.records,
        "source metadata classifications do not cover every metadata record"
    );
    source_metadata_profile
        .admit_counts(rewrite.source_metadata_profile_counts)
        .map_err(|error| anyhow!(error))
        .context("admit source classifications under the authority-bound metadata profile")?;
    ensure!(
        rewrite.source_metadata_profile_counts.raw_fallback == 0
            && rewrite.source_metadata_profile_counts.both_different == 0
            && rewrite.source_metadata_profile_counts.invalid == 0,
        "source metadata classifications contain raw, divergent, or invalid records"
    );
    ensure!(
        rewrite.blocks == source_reader.metadata_footer().blocks
            && rewrite.transactions == source_reader.metadata_footer().transactions
            && rewrite.source_metadata_profile_counts.raw_fallback
                == source_reader.metadata_footer().metadata_raw_fallbacks,
        "rewrite coverage differs from the structurally admitted source metadata footer"
    );
    let target_metadata_profile_counts = ArchiveV2MetadataSchemaCounts {
        no_error: rewrite.metadata.successful_records,
        current_only: rewrite.metadata.target_current_only_records,
        legacy_only: 0,
        both_equal: rewrite.metadata.target_both_equal_records,
        both_different: 0,
        invalid: 0,
        raw_fallback: 0,
    };
    ensure!(
        target_metadata_profile_counts
            .checked_total()
            .map_err(|error| anyhow!(error))?
            == rewrite.metadata.records,
        "target metadata profile count does not cover every metadata record"
    );
    ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
        .admit_counts(target_metadata_profile_counts)
        .map_err(|error| anyhow!(error))
        .context("admit normalized records as current typed-error metadata")?;
    ensure!(
        rewrite.blocks == index.rows.len() as u64,
        "rewrite did not process every source hot-index row"
    );
    assert_rewrite_index_construction(
        &index.rows,
        &rewrite.target_rows,
        rewrite.target_blocks.bytes,
    )?;

    let target_index_binding = write_hot_index_file(
        staging.create_file(ARCHIVE_V2_BLOCK_INDEX_FILE)?,
        rewrite.target_blocks.bytes,
        target_zstd_level,
        0,
        &rewrite.target_rows,
    )?;

    let mut output_files = sidecars.bindings;
    let get_block_rows_rebuilt = rebuild_get_block_if_present(
        &source_snapshot,
        &staging,
        epoch_start_slot,
        slots_per_epoch,
        &index.rows,
        &rewrite.target_rows,
        &mut output_files,
    )?;
    ensure!(
        output_files
            .insert(
                ARCHIVE_V2_BLOCKS_FILE.to_owned(),
                rewrite.target_blocks.clone()
            )
            .is_none(),
        "target blocks binding conflicts with a copied sidecar"
    );
    ensure!(
        output_files
            .insert(
                ARCHIVE_V2_BLOCK_INDEX_FILE.to_owned(),
                target_index_binding.clone()
            )
            .is_none(),
        "target index binding conflicts with a copied sidecar"
    );
    staging.sync_file(ARCHIVE_V2_BLOCK_INDEX_FILE)?;
    staging.sync()?;

    assert_staging_inventory(&staging, output_files.keys().map(String::as_str))?;

    let target_candidate = build_target_candidate_manifest(
        &output_files,
        &authority.cluster_id,
        epoch,
        slots_per_epoch,
        candidate_id,
        source_reader.wire_profile(),
    )?;
    let target_manifest = target_candidate.manifest;
    let message_marker_binding = target_candidate.message_marker;
    let metadata_marker_binding = target_candidate.metadata_marker;
    let target_candidate_digest = target_manifest.generation_digest.clone();
    let target_source = PinnedLocalRangeSource::from_directory_file(
        staging.display_path.clone(),
        staging.directory.try_clone()?,
    )
    .map_err(|error| anyhow!(error))?;
    pin_forbidden_target_controls_absent(&target_source)?;
    let target_reader = ArchiveReader::open_candidate(
        MarkerOverlay::new(target_source.clone(), target_candidate.marker_bytes),
        target_manifest.clone(),
        ReaderOpenOptions {
            hash_verification: HashVerification::AllFiles,
            ..ReaderOpenOptions::default()
        },
    )
    .map_err(|error| anyhow!(error))
    .context("open the complete unpublished target through its in-memory candidate manifest")?;
    let marker_proof = audit_current_metadata_for_marker_publication(&target_reader)
        .map_err(|error| anyhow!(error))
        .context("audit the complete unpublished target before authorizing marker bytes")?;
    ensure!(
        marker_proof.source_binding() == target_reader.profiled_binding()
            && marker_proof.audit().counts == target_metadata_profile_counts
            && marker_proof.audit().blocks == rewrite.blocks,
        "marker authorization audit differs from the rewrite proof"
    );
    ensure_marker_proof_binding(&marker_proof, &metadata_marker_binding)?;
    target_source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("target changed during its strict complete audit")?;
    validate_manifest_bindings(&target_manifest, &output_files)?;
    assert_staging_inventory(&staging, output_files.keys().map(String::as_str))?;
    source_snapshot.revalidate_identity_inventory()?;
    authority.revalidate_document_identity()?;
    staging.recheck_anchor()?;
    target_source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("target payload or forbidden publication controls changed before receipt")?;
    let source_files = source_snapshot.bindings();

    let manifest = NormalizationManifest {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-candidate",
        state: "audited-unpublished-candidate",
        metadata_schema: "current-typed-errors-v1",
        source: source.display().to_string(),
        staging: staging.display_path.display().to_string(),
        epoch,
        slots_per_epoch,
        cluster_id: source_reader.manifest().cluster_id.clone(),
        source_generation_id: authority.source_generation_id.clone(),
        source_generation_digest: authority.source_generation_digest.clone(),
        source_message_wire_profile: source_reader.wire_profile().to_string(),
        source_metadata_wire_profile: source_metadata_profile.stable_name().to_owned(),
        candidate_id: candidate_id.to_owned(),
        target_candidate_digest: target_candidate_digest.clone(),
        authorized_message_marker: message_marker_binding.clone(),
        authorized_metadata_marker: metadata_marker_binding.clone(),
        source_authority_kind: authority.kind,
        source_authority_id: authority.authority_id.clone(),
        source_authority_binding: authority.authority_binding.clone(),
        source_directory_identity: source_snapshot.directory_identity.clone(),
        source_files: source_files.clone(),
        files: output_files.clone(),
        frame_processing: rewrite.frame_processing,
        omitted_source_controls: sidecars.omitted_controls,
        ignored_unrelated_source_entries: source_snapshot.ignored_unrelated_entries.clone(),
    };
    write_json_last(&staging, NORMALIZATION_MANIFEST_FILE, &manifest)?;
    let candidate_manifest_binding = staging.hash_file(NORMALIZATION_MANIFEST_FILE)?;
    let mut expected_with_candidate = output_files.keys().cloned().collect::<BTreeSet<_>>();
    expected_with_candidate.insert(NORMALIZATION_MANIFEST_FILE.to_owned());
    assert_staging_inventory(&staging, expected_with_candidate.iter().map(String::as_str))?;
    source_snapshot.revalidate_identity_inventory()?;
    authority.revalidate_document_identity()?;
    staging.recheck_anchor()?;
    target_source
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("target payload or forbidden publication controls changed before receipt")?;

    let target_blocks_binding = output_files
        .get(ARCHIVE_V2_BLOCKS_FILE)
        .context("target blocks binding is missing")?
        .clone();
    let target_index_binding = output_files
        .get(ARCHIVE_V2_BLOCK_INDEX_FILE)
        .context("target index binding is missing")?
        .clone();

    let receipt = NormalizationReceipt {
        schema_version: 1,
        kind: "archive-v2-metadata-normalization-receipt",
        state: "complete-unpublished-staging-generation",
        canonical_publication_performed: false,
        source: source.display().to_string(),
        staging: staging.display_path.display().to_string(),
        epoch,
        slots_per_epoch,
        completed_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_secs(),
        source_blocks: source_blocks_binding,
        source_index: source_index_binding,
        target_blocks: target_blocks_binding,
        target_index: target_index_binding,
        candidate_manifest: candidate_manifest_binding,
        message_marker: message_marker_binding,
        metadata_marker: metadata_marker_binding,
        cluster_id: source_reader.manifest().cluster_id.clone(),
        source_generation_id: authority.source_generation_id.clone(),
        source_generation_digest: authority.source_generation_digest.clone(),
        target_candidate_id: candidate_id.to_owned(),
        target_candidate_digest,
        source_authority_kind: authority.kind,
        source_authority_id: authority.authority_id.clone(),
        source_authority_binding: authority.authority_binding.clone(),
        message_wire_profile: source_reader.wire_profile().to_string(),
        source_metadata_profile: source_metadata_profile.stable_name().to_owned(),
        source_metadata_profile_counts: rewrite.source_metadata_profile_counts,
        blocks: rewrite.blocks,
        transactions: rewrite.transactions,
        message_bytes: rewrite.message_bytes,
        message_sha256: rewrite.message_sha256,
        metadata: rewrite.metadata,
        frame_processing: rewrite.frame_processing,
        target_metadata_profile: ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1.stable_name(),
        target_metadata_profile_counts,
        copied_sidecars: sidecars.copied_files,
        copied_sidecar_bytes: sidecars.copied_bytes,
        get_block_rows_rebuilt,
        target_zstd_level,
        source_revalidated_at_completion: true,
        source_directory_identity: source_snapshot.directory_identity.clone(),
        source_files,
        ignored_unrelated_source_entries: source_snapshot.ignored_unrelated_entries.clone(),
    };
    write_json_last(&staging, NORMALIZATION_RECEIPT_FILE, &receipt)?;
    Ok(receipt)
}

impl FileIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Result<Self> {
        ensure!(metadata.is_file(), "source entry is not a regular file");
        #[cfg(unix)]
        {
            Ok(Self {
                bytes: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                mode: metadata.mode(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
                changed_seconds: metadata.ctime(),
                changed_nanoseconds: metadata.ctime_nsec(),
            })
        }
        #[cfg(not(unix))]
        {
            let modified_unix_nanoseconds = metadata.modified().ok().and_then(|modified| {
                modified
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|duration| duration.as_nanos())
            });
            Ok(Self {
                bytes: metadata.len(),
                modified_unix_nanoseconds,
            })
        }
    }

    fn from_directory_metadata(metadata: &fs::Metadata) -> Result<Self> {
        ensure!(metadata.is_dir(), "source directory is not a directory");
        #[cfg(unix)]
        {
            Ok(Self {
                bytes: metadata.len(),
                device: metadata.dev(),
                inode: metadata.ino(),
                mode: metadata.mode(),
                modified_seconds: metadata.mtime(),
                modified_nanoseconds: metadata.mtime_nsec(),
                changed_seconds: metadata.ctime(),
                changed_nanoseconds: metadata.ctime_nsec(),
            })
        }
        #[cfg(not(unix))]
        {
            let modified_unix_nanoseconds = metadata.modified().ok().and_then(|modified| {
                modified
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|duration| duration.as_nanos())
            });
            Ok(Self {
                bytes: metadata.len(),
                modified_unix_nanoseconds,
            })
        }
    }

    fn same_object(&self, other: &Self) -> bool {
        self.device == other.device && self.inode == other.inode
    }
}

fn cstring_component(name: &OsStr) -> Result<CString> {
    let bytes = name.as_bytes();
    ensure!(
        !bytes.is_empty()
            && bytes != b"."
            && bytes != b".."
            && !bytes.contains(&b'/')
            && !bytes.contains(&0),
        "invalid filesystem path component {name:?}"
    );
    CString::new(bytes).context("filesystem component contains NUL")
}

fn openat_file(directory: &File, name: &OsStr, flags: i32, mode: libc::mode_t) -> io::Result<File> {
    let name = cstring_component(name).map_err(io::Error::other)?;
    // SAFETY: directory and name stay live for the system call. A successful
    // call returns one owned descriptor.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags,
            libc::c_uint::from(mode),
        )
    };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a new descriptor owned by this function.
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

fn openat_directory_nofollow(directory: &File, name: &OsStr) -> io::Result<File> {
    openat_file(
        directory,
        name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
        0,
    )
}

fn openat_parent_directory(directory: &File) -> io::Result<File> {
    let name = CString::new("..").expect("parent component has no NUL");
    // SAFETY: the descriptor and constant component stay live for the call.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
            0,
        )
    };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: openat returned a new descriptor.
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

fn openat_regular_nofollow(directory: &File, name: &OsStr) -> io::Result<File> {
    let file = openat_file(
        directory,
        name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        0,
    )?;
    if file.metadata()?.is_file() {
        Ok(file)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "descriptor-relative object is not a regular file",
        ))
    }
}

fn createat_regular_nofollow(directory: &File, name: &OsStr) -> io::Result<File> {
    openat_file(
        directory,
        name,
        libc::O_WRONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_CREAT | libc::O_EXCL,
        0o600,
    )
}

fn open_absolute_directory_nofollow(path: &Path) -> Result<(File, Option<File>, Option<OsString>)> {
    ensure!(path.is_absolute(), "directory path must be absolute");
    let root_name = CString::new("/").expect("root path has no NUL");
    // SAFETY: the constant root path stays live and a successful call returns
    // one new owned descriptor.
    let descriptor = unsafe {
        libc::open(
            root_name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_DIRECTORY,
        )
    };
    ensure!(
        descriptor >= 0,
        "open filesystem root: {}",
        io::Error::last_os_error()
    );
    // SAFETY: open returned a new descriptor.
    let mut current = unsafe { File::from_raw_fd(descriptor) };
    let mut parent = None;
    let mut final_name = None;
    for component in path.components() {
        use std::path::Component;
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                let next = openat_directory_nofollow(&current, name).with_context(|| {
                    format!("open directory component {name:?} without following links")
                })?;
                parent = Some(current);
                final_name = Some(name.to_os_string());
                current = next;
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                anyhow::bail!("directory path must be absolute and lexically normalized")
            }
        }
    }
    Ok((current, parent, final_name))
}

fn open_absolute_regular_nofollow(path: &Path) -> Result<File> {
    ensure!(path.is_absolute(), "regular-file path must be absolute");
    let parent = path.parent().context("regular-file path has no parent")?;
    let name = path
        .file_name()
        .context("regular-file path has no final component")?;
    let (directory, _, _) = open_absolute_directory_nofollow(parent)?;
    openat_regular_nofollow(&directory, name)
        .with_context(|| format!("open {} without following path links", path.display()))
}

fn directory_is_at_or_below(directory: &File, possible_ancestor: &FileIdentity) -> Result<bool> {
    let mut current = directory.try_clone()?;
    loop {
        let current_identity = FileIdentity::from_directory_metadata(&current.metadata()?)?;
        if current_identity.same_object(possible_ancestor) {
            return Ok(true);
        }
        let parent = openat_parent_directory(&current)?;
        let parent_identity = FileIdentity::from_directory_metadata(&parent.metadata()?)?;
        if parent_identity.same_object(&current_identity) {
            return Ok(false);
        }
        current = parent;
    }
}

fn prepare_staging_location(
    path: &Path,
    source_directory_identity: &FileIdentity,
) -> Result<StagingLocation> {
    ensure!(path.is_absolute(), "--staging must be absolute");
    let parent_path = path.parent().context("staging path has no parent")?;
    let name = path
        .file_name()
        .context("staging path has no final component")?
        .to_os_string();
    cstring_component(&name)?;
    let (parent, _, _) = open_absolute_directory_nofollow(parent_path)?;
    ensure!(
        !directory_is_at_or_below(&parent, source_directory_identity)?,
        "staging directory would be inside the source directory through a path or mount alias"
    );
    match openat_file(
        &parent,
        &name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        0,
    ) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Ok(_) => anyhow::bail!("staging path already exists: {}", path.display()),
        Err(error) => {
            return Err(error).with_context(|| format!("check staging path {}", path.display()));
        }
    }
    Ok(StagingLocation {
        display_path: path.to_path_buf(),
        parent,
        name,
    })
}

impl StagingLocation {
    fn create(self) -> Result<StagingDirectory> {
        let name = cstring_component(&self.name)?;
        // SAFETY: the parent and name stay live for the system call.
        let result = unsafe { libc::mkdirat(self.parent.as_raw_fd(), name.as_ptr(), 0o700) };
        ensure!(
            result == 0,
            "create private staging {}: {}",
            self.display_path.display(),
            io::Error::last_os_error()
        );
        let directory = openat_directory_nofollow(&self.parent, &self.name)
            .context("pin the newly created staging directory")?;
        let identity = FileIdentity::from_directory_metadata(&directory.metadata()?)?;
        self.parent.sync_all()?;
        Ok(StagingDirectory {
            display_path: self.display_path,
            parent: self.parent,
            directory,
            name: self.name,
            identity,
        })
    }
}

impl StagingDirectory {
    fn create_file(&self, name: &str) -> Result<File> {
        createat_regular_nofollow(&self.directory, OsStr::new(name))
            .with_context(|| format!("create staging object {name}"))
    }

    fn open_file(&self, name: &str) -> Result<File> {
        openat_regular_nofollow(&self.directory, OsStr::new(name))
            .with_context(|| format!("open staging object {name}"))
    }

    fn sync_file(&self, name: &str) -> Result<()> {
        self.open_file(name)?.sync_all()?;
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.directory.sync_all()?;
        self.parent.sync_all()?;
        Ok(())
    }

    fn hash_file(&self, name: &str) -> Result<FileBinding> {
        hash_open_regular_file(&self.open_file(name)?)
    }

    fn read_all_bounded(&self, name: &str, limit: usize) -> Result<Vec<u8>> {
        let mut file = self.open_file(name)?;
        let bytes = file.metadata()?.len();
        ensure!(
            bytes <= limit as u64,
            "staging object {name} exceeds the read limit"
        );
        file.seek(SeekFrom::Start(0))?;
        let mut output = Vec::with_capacity(bytes as usize);
        file.read_to_end(&mut output)?;
        ensure!(
            output.len() as u64 == bytes,
            "staging object {name} changed while it was read"
        );
        Ok(output)
    }

    fn recheck_anchor(&self) -> Result<()> {
        let current = openat_directory_nofollow(&self.parent, &self.name)?;
        let current_identity = FileIdentity::from_directory_metadata(&current.metadata()?)?;
        let pinned_identity = FileIdentity::from_directory_metadata(&self.directory.metadata()?)?;
        ensure!(
            current_identity.same_object(&self.identity)
                && pinned_identity.same_object(&self.identity),
            "staging directory anchor changed"
        );
        Ok(())
    }
}

impl SourceSnapshot {
    fn admit(
        source: PinnedLocalRangeSource,
        expected: &BTreeMap<String, (FileBinding, SourceDisposition)>,
    ) -> Result<Self> {
        let directory_identity =
            FileIdentity::from_directory_metadata(&source.directory_file()?.metadata()?)?;
        let mut files = BTreeMap::new();
        let mut ignored_unrelated_entries = Vec::new();
        let mut found = BTreeSet::new();
        for entry in source.inventory()? {
            let name = entry
                .name
                .into_string()
                .map_err(|_| anyhow!("source contains a non-UTF-8 root entry"))?;
            match entry.kind {
                PinnedLocalEntryKind::RegularFile => {
                    let Some((expected_binding, disposition)) = expected.get(&name) else {
                        ensure!(
                            !looks_like_archive_or_control(&name),
                            "source archive/control entry {name} is outside its authority"
                        );
                        ignored_unrelated_entries.push(name);
                        continue;
                    };
                    let file = source.open_file(&name)?;
                    let identity = FileIdentity::from_metadata(&file.metadata()?)?;
                    ensure!(
                        identity.device == entry.device
                            && identity.inode == entry.inode
                            && identity.bytes == entry.bytes,
                        "source entry {name} changed during descriptor admission"
                    );
                    let content = hash_open_regular_file(&file)?;
                    ensure!(
                        &content == expected_binding,
                        "source entry {name} differs from its authority binding"
                    );
                    files.insert(
                        name.clone(),
                        SourceFileBinding {
                            identity,
                            content,
                            disposition: *disposition,
                        },
                    );
                    found.insert(name);
                }
                PinnedLocalEntryKind::Directory => {
                    ensure!(
                        !looks_like_archive_or_control(&name) && name != "repair",
                        "source archive/control directory {name} is outside its authority"
                    );
                    ignored_unrelated_entries.push(name);
                }
                _ => {
                    anyhow::bail!("source entry {name} is a symlink or unsupported special object")
                }
            }
        }
        ensure!(
            found.len() == expected.len() && expected.keys().all(|name| found.contains(name)),
            "source inventory does not contain every authority-bound file"
        );
        ignored_unrelated_entries.sort();
        Ok(Self {
            source,
            directory_identity,
            files,
            ignored_unrelated_entries,
        })
    }

    fn file(&self, name: &str) -> Result<File> {
        ensure!(
            self.files.contains_key(name),
            "source authority is missing {name}"
        );
        self.source
            .open_file(name)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("open authority-bound source file {name}"))
    }

    fn optional_file(&self, name: &str) -> Result<Option<File>> {
        if !self.files.contains_key(name) {
            return Ok(None);
        }
        self.source
            .open_file(name)
            .map(Some)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("open authority-bound optional source file {name}"))
    }

    fn binding(&self, name: &str) -> Result<&SourceFileBinding> {
        self.files
            .get(name)
            .with_context(|| format!("source authority is missing {name}"))
    }

    fn read_all_bounded(&self, name: &str, limit: usize) -> Result<Vec<u8>> {
        ensure!(
            self.files.contains_key(name),
            "source authority is missing {name}"
        );
        self.source
            .read_all_bounded(name, limit)
            .map_err(|error| anyhow!(error))
            .with_context(|| format!("read authority-bound source file {name}"))
    }

    fn bindings(&self) -> BTreeMap<String, SourceFileBinding> {
        self.files.clone()
    }

    fn revalidate_identity_inventory(&self) -> Result<()> {
        self.source
            .verify_unchanged()
            .map_err(|error| anyhow!(error))
            .context("descriptor-pinned source changed")?;
        let directory =
            FileIdentity::from_directory_metadata(&self.source.directory_file()?.metadata()?)?;
        ensure!(
            directory.same_object(&self.directory_identity),
            "descriptor-pinned source root identity changed"
        );
        let mut found = BTreeSet::new();
        for entry in self.source.inventory()? {
            let name = entry
                .name
                .into_string()
                .map_err(|_| anyhow!("source contains a non-UTF-8 root entry"))?;
            match entry.kind {
                PinnedLocalEntryKind::RegularFile => {
                    if self.files.contains_key(&name) {
                        found.insert(name);
                    } else {
                        ensure!(
                            !looks_like_archive_or_control(&name),
                            "source archive/control entry {name} appeared outside its authority"
                        );
                    }
                }
                PinnedLocalEntryKind::Directory => ensure!(
                    !looks_like_archive_or_control(&name) && name != "repair",
                    "source archive/control directory {name} appeared outside its authority"
                ),
                _ => anyhow::bail!(
                    "source entry {name} became a symlink or unsupported special object"
                ),
            }
        }
        ensure!(
            found.len() == self.files.len() && self.files.keys().all(|name| found.contains(name)),
            "source no longer contains every authority-bound file"
        );
        for (name, binding) in &self.files {
            let file = self.file(name)?;
            ensure!(
                FileIdentity::from_metadata(&file.metadata()?)? == binding.identity,
                "source entry {name} identity changed"
            );
        }
        Ok(())
    }
}

#[derive(Clone)]
struct MarkerOverlay {
    source: PinnedLocalRangeSource,
    markers: Arc<BTreeMap<String, &'static [u8]>>,
}

impl MarkerOverlay {
    fn new(source: PinnedLocalRangeSource, markers: BTreeMap<String, &'static [u8]>) -> Self {
        Self {
            source,
            markers: Arc::new(markers),
        }
    }
}

impl RangeSource for MarkerOverlay {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        if let Some(bytes) = self.markers.get(object) {
            return Ok(Some(bytes.len() as u64));
        }
        self.source.size(object)
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        if let Some(bytes) = self.markers.get(object) {
            let start = usize::try_from(offset).map_err(|_| SourceError::OutOfBounds {
                object: object.to_owned(),
                offset,
                length,
                size: bytes.len() as u64,
            })?;
            let end = start
                .checked_add(length)
                .ok_or_else(|| SourceError::OutOfBounds {
                    object: object.to_owned(),
                    offset,
                    length,
                    size: bytes.len() as u64,
                })?;
            if end > bytes.len() {
                return Err(SourceError::OutOfBounds {
                    object: object.to_owned(),
                    offset,
                    length,
                    size: bytes.len() as u64,
                });
            }
            return Ok(bytes[start..end].to_vec());
        }
        self.source.read_range(object, offset, length)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        if self.markers.contains_key(object) {
            *destination = self.read_range(object, offset, length)?;
            return Ok(());
        }
        self.source
            .read_range_into(object, offset, length, destination)
    }
}

fn parse_message_profile(value: &str) -> Result<ArchiveV2WireProfile> {
    match value {
        "post-unknown-instruction-fallbacks-v1" => {
            Ok(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1)
        }
        "pre-unknown-instruction-fallbacks-v1" => {
            Ok(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1)
        }
        _ => anyhow::bail!("unknown message wire profile {value:?}"),
    }
}

fn parse_metadata_profile(value: &str) -> Result<ArchiveV2MetadataWireProfile> {
    match value {
        "unmarked-historical-compatibility" => {
            Ok(ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility)
        }
        "current-typed-errors-v1" => Ok(ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1),
        _ => anyhow::bail!("unknown metadata wire profile {value:?}"),
    }
}

fn generation_binding_map(
    manifest: &GenerationManifest,
) -> Result<BTreeMap<String, (FileBinding, SourceDisposition)>> {
    let mut files = BTreeMap::new();
    for entry in &manifest.files {
        let disposition = known_disposition(&entry.name)
            .with_context(|| format!("manifest lists unknown archive object {}", entry.name))?;
        ensure!(
            files
                .insert(
                    entry.name.clone(),
                    (
                        FileBinding {
                            bytes: entry.size,
                            sha256: entry.sha256.clone(),
                        },
                        disposition,
                    ),
                )
                .is_none(),
            "manifest contains a duplicate file"
        );
    }
    Ok(files)
}

impl AdmittedSourceAuthority {
    fn revalidate_document_identity(&self) -> Result<()> {
        let Some(document) = &self.external_document else {
            return Ok(());
        };
        ensure!(
            FileIdentity::from_metadata(&document.file.metadata()?)? == document.identity,
            "external source-authority inventory identity changed"
        );
        Ok(())
    }
}

fn read_authority_file_once(
    path: &Path,
    expected_sha256: &str,
) -> Result<(Vec<u8>, ExternalAuthorityDocument)> {
    let file = open_absolute_regular_nofollow(path)?;
    let identity = FileIdentity::from_metadata(&file.metadata()?)?;
    let binding = hash_open_regular_file(&file)?;
    ensure!(
        binding.sha256 == expected_sha256,
        "external source-authority inventory SHA-256 differs from --source-authority-sha256"
    );
    ensure!(
        binding.bytes <= 16 << 20,
        "external source-authority inventory exceeds 16 MiB"
    );
    let mut reader = file.try_clone()?;
    reader.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::with_capacity(binding.bytes as usize);
    reader.read_to_end(&mut bytes)?;
    ensure!(
        hash_bytes(&bytes) == binding,
        "external source-authority inventory changed while it was read"
    );
    ensure!(
        FileIdentity::from_metadata(&file.metadata()?)? == identity,
        "external source-authority inventory identity changed while it was read"
    );
    Ok((
        bytes,
        ExternalAuthorityDocument {
            file,
            identity,
            binding,
        },
    ))
}

fn admit_source_authority(
    source: &PinnedLocalRangeSource,
    external_path: Option<&Path>,
    external_sha256: Option<&str>,
) -> Result<(
    AdmittedSourceAuthority,
    GenerationManifest,
    ArchiveV2MetadataProfileAdmission,
    MarkerOverlay,
)> {
    if source.size(GENERATION_MANIFEST_FILE)?.is_some() {
        ensure!(
            external_path.is_none() && external_sha256.is_none(),
            "a published source manifest and an external source authority cannot both be selected"
        );
        let bytes = source.read_all_bounded(GENERATION_MANIFEST_FILE, 16 << 20)?;
        let manifest = GenerationManifest::parse(&bytes).map_err(|error| anyhow!(error))?;
        let manifest_binding = hash_bytes(&bytes);
        let mut admitted_files = generation_binding_map(&manifest)?;
        ensure!(
            admitted_files
                .insert(
                    GENERATION_MANIFEST_FILE.to_owned(),
                    (manifest_binding.clone(), SourceDisposition::OmitControl),
                )
                .is_none(),
            "generation manifest lists itself"
        );
        let message_wire_profile = ArchiveV2WireProfile::for_published_manifest(&manifest)
            .map_err(|error| anyhow!(error))?;
        let metadata_wire_profile = ArchiveV2MetadataWireProfile::for_manifest(
            &manifest,
            ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
        )
        .map_err(|error| anyhow!(error))?;
        let authority = AdmittedSourceAuthority {
            kind: "published-generation-manifest",
            authority_id: manifest.generation_id.clone(),
            authority_binding: manifest_binding,
            cluster_id: manifest.cluster_id.clone(),
            source_generation_id: manifest.generation_id.clone(),
            source_generation_digest: manifest.generation_digest.clone(),
            epoch: manifest.epoch,
            slots_per_epoch: manifest.slots_per_epoch,
            message_wire_profile,
            metadata_wire_profile,
            expected_files: admitted_files,
            external_document: None,
        };
        return Ok((
            authority,
            manifest,
            ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
            MarkerOverlay::new(source.clone(), BTreeMap::new()),
        ));
    }

    let path = external_path.context(
        "unmanifested source requires --source-authority-inventory and --source-authority-sha256",
    )?;
    let expected_sha256 = external_sha256.context(
        "unmanifested source requires --source-authority-inventory and --source-authority-sha256",
    )?;
    let source_identity =
        FileIdentity::from_directory_metadata(&source.directory_file()?.metadata()?)?;
    let authority_parent = path
        .parent()
        .context("source-authority inventory path has no parent")?;
    let (authority_parent, _, _) = open_absolute_directory_nofollow(authority_parent)?;
    ensure!(
        !directory_is_at_or_below(&authority_parent, &source_identity)?,
        "external source-authority inventory must be outside the source tree"
    );
    let (bytes, external_document) = read_authority_file_once(path, expected_sha256)?;
    let authority_binding = external_document.binding.clone();
    let inventory: SourceAuthorityInventory =
        serde_json::from_slice(&bytes).context("parse external source-authority inventory")?;
    inventory.validate()?;
    let message_wire_profile = parse_message_profile(&inventory.message_wire_profile)?;
    let metadata_wire_profile = parse_metadata_profile(&inventory.metadata_wire_profile)?;
    let mut admitted_files = BTreeMap::new();
    let mut manifest_files = Vec::new();
    for file in &inventory.files {
        let binding = FileBinding {
            bytes: file.bytes,
            sha256: file.sha256.clone(),
        };
        admitted_files.insert(file.name.clone(), (binding, file.disposition));
        manifest_files.push(GenerationFile {
            name: file.name.clone(),
            size: file.bytes,
            sha256: file.sha256.clone(),
        });
    }
    let mut markers = BTreeMap::new();
    let message_marker = wire_profile_marker(message_wire_profile);
    if !admitted_files.contains_key(&message_marker.name) {
        markers.insert(
            message_marker.name.clone(),
            wire_profile_marker_bytes(message_wire_profile),
        );
        manifest_files.push(message_marker);
    }
    if metadata_wire_profile == ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1
        && !admitted_files.contains_key(CURRENT_TYPED_ERRORS_MARKER_FILE)
    {
        markers.insert(
            CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
            CURRENT_TYPED_ERRORS_MARKER_BYTES,
        );
        manifest_files.push(GenerationFile {
            name: CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
            size: CURRENT_TYPED_ERRORS_MARKER_SIZE,
            sha256: CURRENT_TYPED_ERRORS_MARKER_SHA256.to_owned(),
        });
    }
    manifest_files.sort_by(|left, right| left.name.cmp(&right.name));
    let mut manifest = GenerationManifest {
        schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
        cluster_id: inventory.cluster_id.clone(),
        epoch: inventory.epoch,
        generation_id: inventory.authority_id.clone(),
        generation_digest: "0".repeat(64),
        slots_per_epoch: inventory.slots_per_epoch,
        complete: true,
        files: manifest_files,
    };
    manifest.generation_digest =
        compute_generation_digest(&manifest).map_err(|error| anyhow!(error))?;
    manifest.validate().map_err(|error| anyhow!(error))?;
    let admission = if metadata_wire_profile == ArchiveV2MetadataWireProfile::CurrentTypedErrorsV1 {
        ArchiveV2MetadataProfileAdmission::RequireCurrentTypedErrors
    } else {
        ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical
    };
    let authority = AdmittedSourceAuthority {
        kind: "external-source-authority-inventory",
        authority_id: inventory.authority_id.clone(),
        authority_binding,
        cluster_id: inventory.cluster_id,
        source_generation_id: inventory.authority_id,
        source_generation_digest: inventory.authority_digest,
        epoch: inventory.epoch,
        slots_per_epoch: inventory.slots_per_epoch,
        message_wire_profile,
        metadata_wire_profile,
        expected_files: admitted_files,
        external_document: Some(external_document),
    };
    Ok((
        authority,
        manifest,
        admission,
        MarkerOverlay::new(source.clone(), markers),
    ))
}

fn hash_open_regular_file(file: &File) -> Result<FileBinding> {
    let expected_bytes = file.metadata()?.len();
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = checked_add(bytes, read as u64, "hashed source byte count")?;
    }
    ensure!(
        bytes == expected_bytes,
        "source file changed length while hashing"
    );
    Ok(FileBinding {
        bytes,
        sha256: hex_lower(&hasher.finalize()),
    })
}

fn hash_bytes(bytes: &[u8]) -> FileBinding {
    FileBinding {
        bytes: bytes.len() as u64,
        sha256: hex_lower(&Sha256::digest(bytes)),
    }
}

fn validate_source_index_geometry(
    rows: &[ArchiveV2HotBlockIndexRow],
    blob_bytes: u64,
) -> Result<()> {
    let mut compressed_offset = 0u64;
    let mut tx_ordinal = 0u64;
    let mut signature_ordinal = 0u64;
    let mut previous_slot = None;
    for (number, row) in rows.iter().enumerate() {
        ensure!(
            row.block_id as usize == number,
            "source block IDs are not canonical"
        );
        ensure!(
            row.compressed_offset == compressed_offset,
            "source compressed block ranges are not contiguous at block {}",
            row.block_id
        );
        ensure!(
            row.compressed_len > 0,
            "source block {} has zero compressed bytes",
            row.block_id
        );
        ensure!(
            row.uncompressed_len > 0,
            "source block {} has zero uncompressed bytes",
            row.block_id
        );
        ensure!(
            row.first_tx_ordinal == tx_ordinal,
            "source transaction ordinals are not contiguous at block {}",
            row.block_id
        );
        ensure!(
            row.first_signature_ordinal == signature_ordinal,
            "source signature ordinals are not contiguous at block {}",
            row.block_id
        );
        if let Some(previous) = previous_slot {
            ensure!(
                row.slot > previous,
                "source slots are not strictly increasing"
            );
        }
        previous_slot = Some(row.slot);
        compressed_offset = checked_add(
            compressed_offset,
            u64::from(row.compressed_len),
            "source compressed offset",
        )?;
        tx_ordinal = checked_add(
            tx_ordinal,
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

fn same_hot_index(left: &ArchiveV2HotBlockIndex, right: &ArchiveV2HotBlockIndex) -> bool {
    left.blob_file_bytes == right.blob_file_bytes
        && left.level == right.level
        && left.flags == right.flags
        && left.rows.len() == right.rows.len()
        && left.rows.iter().zip(&right.rows).all(|(left, right)| {
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

#[cfg(test)]
fn rehash_target_files(
    staging: &StagingDirectory,
    expected: &BTreeMap<String, FileBinding>,
) -> Result<BTreeMap<String, FileBinding>> {
    let mut actual = BTreeMap::new();
    for (name, expected_binding) in expected {
        staging.sync_file(name)?;
        let binding = staging.hash_file(name)?;
        ensure!(
            &binding == expected_binding,
            "on-disk target file {name} differs from its write proof"
        );
        actual.insert(name.clone(), binding);
    }
    staging.sync()?;
    Ok(actual)
}

fn validate_manifest_bindings(
    manifest: &GenerationManifest,
    files: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    for (name, binding) in files {
        let entry = manifest
            .required_file(name)
            .map_err(|error| anyhow!(error))?;
        ensure!(
            entry.size == binding.bytes && entry.sha256 == binding.sha256,
            "target candidate manifest binding differs for {name}"
        );
    }
    ensure!(
        manifest.files.len() == files.len() + 2,
        "target candidate manifest has unexpected non-payload entries"
    );
    Ok(())
}

fn build_target_candidate_manifest(
    files: &BTreeMap<String, FileBinding>,
    cluster_id: &str,
    epoch: u64,
    slots_per_epoch: u64,
    candidate_id: &str,
    message_profile: ArchiveV2WireProfile,
) -> Result<TargetCandidateAuthority> {
    let message_marker = wire_profile_marker(message_profile);
    let metadata_marker = GenerationFile {
        name: CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        size: CURRENT_TYPED_ERRORS_MARKER_SIZE,
        sha256: CURRENT_TYPED_ERRORS_MARKER_SHA256.to_owned(),
    };
    ensure!(
        !files.contains_key(&message_marker.name)
            && !files.contains_key(CURRENT_TYPED_ERRORS_MARKER_FILE)
            && !files.contains_key(GENERATION_MANIFEST_FILE),
        "unpublished target payload contains an official marker or generation manifest"
    );
    let mut manifest_files = files
        .iter()
        .map(|(name, binding)| GenerationFile {
            name: name.clone(),
            size: binding.bytes,
            sha256: binding.sha256.clone(),
        })
        .collect::<Vec<_>>();
    manifest_files.push(message_marker.clone());
    manifest_files.push(metadata_marker.clone());
    manifest_files.sort_by(|left, right| left.name.cmp(&right.name));
    let mut manifest = GenerationManifest {
        schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
        cluster_id: cluster_id.to_owned(),
        epoch,
        generation_id: candidate_id.to_owned(),
        generation_digest: "0".repeat(64),
        slots_per_epoch,
        complete: true,
        files: manifest_files,
    };
    manifest.generation_digest =
        compute_generation_digest(&manifest).map_err(|error| anyhow!(error))?;
    manifest.validate().map_err(|error| anyhow!(error))?;
    let mut markers = BTreeMap::new();
    markers.insert(
        message_marker.name.clone(),
        wire_profile_marker_bytes(message_profile),
    );
    markers.insert(
        CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        CURRENT_TYPED_ERRORS_MARKER_BYTES,
    );
    Ok(TargetCandidateAuthority {
        manifest,
        marker_bytes: markers,
        message_marker: NamedFileBinding {
            name: message_marker.name,
            bytes: message_marker.size,
            sha256: message_marker.sha256,
        },
        metadata_marker: NamedFileBinding {
            name: metadata_marker.name,
            bytes: metadata_marker.size,
            sha256: metadata_marker.sha256,
        },
    })
}

fn ensure_marker_proof_binding(
    proof: &AuditedCurrentMetadataMarkerPublication,
    expected: &NamedFileBinding,
) -> Result<()> {
    let entry = proof.marker_manifest_entry();
    ensure!(
        entry.name == expected.name
            && entry.name == CURRENT_TYPED_ERRORS_MARKER_FILE
            && entry.size == expected.bytes
            && entry.sha256 == expected.sha256
            && proof.marker_bytes() == CURRENT_TYPED_ERRORS_MARKER_BYTES,
        "audited current metadata marker proof has an unexpected binding"
    );
    Ok(())
}

fn assert_staging_inventory<'a>(
    staging: &StagingDirectory,
    expected: impl Iterator<Item = &'a str>,
) -> Result<()> {
    let expected = expected.map(str::to_owned).collect::<BTreeSet<_>>();
    let source = PinnedLocalRangeSource::from_directory_file(
        staging.display_path.clone(),
        staging.directory.try_clone()?,
    )
    .map_err(|error| anyhow!(error))?;
    let mut actual = BTreeSet::new();
    for entry in source.inventory()? {
        let name = entry
            .name
            .into_string()
            .map_err(|_| anyhow!("staging contains a non-UTF-8 entry"))?;
        ensure!(
            entry.kind == PinnedLocalEntryKind::RegularFile,
            "staging entry {name} is not a regular file"
        );
        ensure!(actual.insert(name), "staging contains a duplicate entry");
    }
    ensure!(
        actual == expected,
        "staging inventory differs from the exact unpublished candidate inventory"
    );
    let forbidden = [
        GENERATION_MANIFEST_FILE.to_owned(),
        wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1).name,
        wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name,
        CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        ".archive-v2-publication.lock".to_owned(),
        NORMALIZATION_RECEIPT_FILE.to_owned(),
    ];
    for forbidden in &forbidden {
        ensure!(
            !actual.contains(forbidden.as_str()),
            "staging contains forbidden publication control {forbidden}"
        );
    }
    Ok(())
}

fn pin_forbidden_target_controls_absent(source: &PinnedLocalRangeSource) -> Result<()> {
    for name in [
        GENERATION_MANIFEST_FILE.to_owned(),
        wire_profile_marker(ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1).name,
        wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name,
        CURRENT_TYPED_ERRORS_MARKER_FILE.to_owned(),
        ".archive-v2-publication.lock".to_owned(),
        NORMALIZATION_RECEIPT_FILE.to_owned(),
    ] {
        ensure!(
            source.size(&name)?.is_none(),
            "staging contains forbidden publication control {name}"
        );
    }
    Ok(())
}

struct BindingWriter {
    inner: BufWriter<File>,
    hasher: Sha256,
    bytes: u64,
}

impl BindingWriter {
    fn new(file: File) -> Self {
        Self {
            inner: BufWriter::with_capacity(IO_BUFFER_BYTES, file),
            hasher: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(mut self) -> Result<FileBinding> {
        self.flush()?;
        self.inner.get_ref().sync_all()?;
        Ok(FileBinding {
            bytes: self.bytes,
            sha256: hex_lower(&self.hasher.finalize()),
        })
    }
}

impl Write for BindingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("binding writer byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn write_hot_index_file(
    file: File,
    blob_file_bytes: u64,
    level: i32,
    flags: u32,
    rows: &[ArchiveV2HotBlockIndexRow],
) -> Result<FileBinding> {
    let mut writer = BindingWriter::new(file);
    writer.write_all(ARCHIVE_V2_HOT_INDEX_MAGIC)?;
    writer.write_all(&ARCHIVE_V2_HOT_INDEX_VERSION.to_le_bytes())?;
    writer.write_all(&0u16.to_le_bytes())?;
    writer.write_all(&(rows.len() as u64).to_le_bytes())?;
    writer.write_all(&blob_file_bytes.to_le_bytes())?;
    writer.write_all(&level.to_le_bytes())?;
    writer.write_all(&flags.to_le_bytes())?;
    for row in rows {
        writer.write_all(&row.block_id.to_le_bytes())?;
        writer.write_all(&row.slot.to_le_bytes())?;
        writer.write_all(&row.compressed_offset.to_le_bytes())?;
        writer.write_all(&row.compressed_len.to_le_bytes())?;
        writer.write_all(&row.uncompressed_len.to_le_bytes())?;
        writer.write_all(&row.tx_count.to_le_bytes())?;
        writer.write_all(&row.first_tx_ordinal.to_le_bytes())?;
        writer.write_all(&row.first_signature_ordinal.to_le_bytes())?;
        writer.write_all(&row.signature_count.to_le_bytes())?;
    }
    writer.finish()
}

fn write_get_block_index_file(
    file: File,
    rows: &[ArchiveV2GetBlockIndexRow],
) -> Result<FileBinding> {
    let mut writer = BindingWriter::new(file);
    for (slot_offset, row) in rows.iter().enumerate() {
        ensure!(
            u64::from(row.access_len) <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
            "get-block index row {slot_offset} exceeds the shared access-frame limit"
        );
        writer.write_all(&row.block_offset.to_le_bytes())?;
        writer.write_all(&row.block_len.to_le_bytes())?;
        writer.write_all(&row.access_offset.to_le_bytes())?;
        writer.write_all(&row.access_len.to_le_bytes())?;
    }
    writer.finish()
}

fn parse_hot_index_bytes(bytes: &[u8]) -> Result<ArchiveV2HotBlockIndex> {
    ensure!(
        bytes.len() >= ARCHIVE_V2_HOT_INDEX_HEADER_LEN,
        "source hot index header is truncated"
    );
    ensure!(
        &bytes[..8] == ARCHIVE_V2_HOT_INDEX_MAGIC,
        "source hot index has bad magic"
    );
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    ensure!(
        version == ARCHIVE_V2_HOT_INDEX_VERSION,
        "source hot index has unsupported version {version}"
    );
    ensure!(
        bytes[10..12] == [0, 0],
        "source hot index reserved bytes are non-zero"
    );
    let row_count = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let expected_bytes = (ARCHIVE_V2_HOT_INDEX_HEADER_LEN as u64)
        .checked_add(
            row_count
                .checked_mul(ARCHIVE_V2_HOT_INDEX_ROW_LEN as u64)
                .context("source hot index row bytes overflow")?,
        )
        .context("source hot index bytes overflow")?;
    ensure!(
        bytes.len() as u64 == expected_bytes,
        "source hot index has {} bytes, expected {expected_bytes}",
        bytes.len()
    );
    let row_count = usize::try_from(row_count).context("hot index row count exceeds usize")?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .context("reserve hot index rows")?;
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

fn parse_get_block_index_bytes(bytes: &[u8]) -> Result<Vec<ArchiveV2GetBlockIndexRow>> {
    ensure!(
        bytes
            .len()
            .is_multiple_of(ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN),
        "source get-block index length is not a row multiple"
    );
    let mut rows = Vec::new();
    rows.try_reserve_exact(bytes.len() / ARCHIVE_V2_GET_BLOCK_INDEX_ROW_LEN)
        .context("reserve get-block index rows")?;
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
            "source get-block row {slot_offset} access length exceeds the shared limit"
        );
        rows.push(parsed);
    }
    Ok(rows)
}

fn parse_block_access_index_bytes(bytes: &[u8]) -> Result<ArchiveV2BlockAccessIndex> {
    ensure!(
        bytes.len() >= ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN,
        "source block-access index header is truncated"
    );
    ensure!(
        &bytes[..8] == ARCHIVE_V2_BLOCK_ACCESS_INDEX_MAGIC,
        "source block-access index has bad magic"
    );
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    ensure!(
        version == ARCHIVE_V2_BLOCK_ACCESS_INDEX_VERSION,
        "source block-access index has unsupported version {version}"
    );
    ensure!(
        bytes[10..12] == [0, 0],
        "source block-access index reserved bytes are non-zero"
    );
    let row_count = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let expected_bytes = (ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN as u64)
        .checked_add(
            row_count
                .checked_mul(ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN as u64)
                .context("source block-access index row bytes overflow")?,
        )
        .context("source block-access index bytes overflow")?;
    ensure!(
        bytes.len() as u64 == expected_bytes,
        "source block-access index has {} bytes, expected {expected_bytes}",
        bytes.len()
    );
    let row_count = usize::try_from(row_count).context("block-access row count exceeds usize")?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .context("reserve block-access rows")?;
    for row in bytes[ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN..]
        .chunks_exact(ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN)
    {
        let parsed = ArchiveV2BlockAccessIndexRow {
            block_id: u32::from_le_bytes(row[0..4].try_into().unwrap()),
            slot: u64::from_le_bytes(row[4..12].try_into().unwrap()),
            access_offset: u64::from_le_bytes(row[12..20].try_into().unwrap()),
            access_len: u32::from_le_bytes(row[20..24].try_into().unwrap()),
            tx_count: u32::from_le_bytes(row[24..28].try_into().unwrap()),
            signature_count: u32::from_le_bytes(row[28..32].try_into().unwrap()),
        };
        ensure!(
            u64::from(parsed.access_len) <= ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
            "source block-access row {} exceeds the shared frame limit",
            parsed.block_id
        );
        rows.push(parsed);
    }
    Ok(ArchiveV2BlockAccessIndex {
        blob_file_bytes: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
        flags: u32::from_le_bytes(bytes[28..32].try_into().unwrap()),
        rows,
    })
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
        "slot {slot} source and normalized transaction-row counts differ"
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
            "slot {slot} transaction {} changed outside rebuilt metadata geometry",
            source.tx_index
        );
        visited += 1;
    }
    ensure!(
        visited == target_rows.len(),
        "row construction proof is incomplete"
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
            "target hot-index identity, count, or ordinal geometry changed at block {}",
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
            "target hot-index compressed coverage",
        )?;
    }
    ensure!(
        target_offset == target_blob_bytes,
        "target hot index covers {target_offset} of {target_blob_bytes} block bytes"
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
        "slot {} rows report {signatures} signatures, but its hot index reports {}",
        source_index.slot,
        source_index.signature_count
    );
    Ok(())
}

fn rewrite_blocks(
    mut source_file: File,
    target_file: File,
    source_rows: &[ArchiveV2HotBlockIndexRow],
    options: RewriteOptions,
) -> Result<RewriteOutput> {
    let RewriteOptions {
        source_zstd_level,
        source_index_flags,
        target_zstd_level,
        max_metadata_bytes,
        progress_blocks,
    } = options;
    source_file.seek(SeekFrom::Start(0))?;
    let mut source = BufReader::with_capacity(IO_BUFFER_BYTES, source_file);
    let mut target = BufWriter::with_capacity(IO_BUFFER_BYTES, target_file);
    let mut decompressor = zstd::bulk::Decompressor::new().context("create zstd decompressor")?;
    let mut compressor =
        zstd::bulk::Compressor::new(target_zstd_level).context("create zstd compressor")?;
    let mut compressed = Vec::new();
    let mut decompressed = Vec::new();
    let mut normalized_metadata = Vec::new();
    let mut normalized_rows = Vec::new();
    let mut serialized = Vec::new();
    let mut target_compressed = Vec::new();
    let mut target_rows = Vec::with_capacity(source_rows.len());
    let mut target_hasher = Sha256::new();
    let mut message_hasher = Sha256::new();
    let mut metadata_counts = MetadataCounts::default();
    let mut classification = MetadataClassificationState::default();
    let mut frame_processing = FrameProcessingCounts::default();
    let mut target_offset = 0u64;
    let mut transactions = 0u64;
    let mut message_bytes = 0u64;

    let limits = ArchiveV2WireRewriteLimits {
        max_input_bytes: max_metadata_bytes,
        max_output_bytes: max_metadata_bytes,
        ..ArchiveV2WireRewriteLimits::default()
    };

    for (number, source_row) in source_rows.iter().copied().enumerate() {
        compressed.resize(source_row.compressed_len as usize, 0);
        source
            .read_exact(&mut compressed)
            .with_context(|| format!("read compressed slot {}", source_row.slot))?;
        let exact_single_frame = is_exact_single_zstd_frame(&compressed);
        ensure!(
            exact_single_frame,
            "slot {} index range is not exactly one complete zstd frame",
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
            "slot {} decompressed to {} bytes, expected {}",
            source_row.slot,
            decompressed.len(),
            source_row.uncompressed_len
        );

        let block = deserialize_archive_v2_hot_block_blob_borrowed_current(&decompressed)
            .map_err(|error| anyhow!(error))
            .with_context(|| {
                format!(
                    "decode slot {} as the current outer hot-block schema",
                    source_row.slot
                )
            })?;
        ensure!(
            block.header.slot == source_row.slot,
            "slot differs between block and index"
        );
        ensure!(
            block.tx_count == source_row.tx_count,
            "transaction count differs between block and index at slot {}",
            source_row.slot
        );
        ensure!(
            block.tx_rows_len() == block.tx_count as usize,
            "transaction rows differ from block count at slot {}",
            source_row.slot
        );
        normalized_rows.clear();
        normalized_rows.extend(block.tx_rows());
        validate_source_block_row_geometry(
            &source_row,
            &normalized_rows,
            block.message_bytes,
            block.metadata_bytes,
        )?;
        let block_counts = normalize_metadata_region(
            &mut normalized_rows,
            block.message_bytes,
            block.metadata_bytes,
            &mut normalized_metadata,
            limits,
            block.header.slot,
            &mut classification,
        )?;
        metadata_counts.add(block_counts)?;
        assert_normalized_row_construction(block.tx_rows(), &normalized_rows, block.header.slot)?;
        message_hasher.update(block.message_bytes);
        message_bytes = checked_add(
            message_bytes,
            block.message_bytes.len() as u64,
            "message byte count",
        )?;

        serialized.clear();
        if serialized.capacity() < decompressed.len() {
            serialized
                .try_reserve_exact(decompressed.len())
                .context("reserve normalized block serialization")?;
        }
        serialize_current_block_parts(
            &block.header,
            block.tx_count,
            &normalized_rows,
            block.message_bytes,
            &normalized_metadata,
            &mut serialized,
        )?;
        verify_serialized_geometry(
            &serialized,
            &block.header,
            &normalized_rows,
            block.message_bytes,
            &normalized_metadata,
        )?;

        let copy_source_frame = serialized == decompressed
            && source_zstd_level == target_zstd_level
            && source_index_flags == 0
            && exact_single_frame;
        let output_frame = if copy_source_frame {
            frame_processing.observe_copy(compressed.len() as u64)?;
            compressed.as_slice()
        } else {
            target_compressed.clear();
            let compress_bound = zstd::zstd_safe::compress_bound(serialized.len());
            if target_compressed.capacity() < compress_bound {
                target_compressed
                    .try_reserve_exact(compress_bound)
                    .context("reserve target compressed block")?;
            }
            compressor
                .compress_to_buffer(&serialized, &mut target_compressed)
                .with_context(|| format!("compress normalized slot {}", source_row.slot))?;
            ensure!(
                !target_compressed.is_empty(),
                "slot {} compressed to zero bytes",
                source_row.slot
            );
            frame_processing
                .observe_recompression(compressed.len() as u64, target_compressed.len() as u64)?;
            target_compressed.as_slice()
        };
        target
            .write_all(output_frame)
            .with_context(|| format!("write normalized slot {}", source_row.slot))?;
        target_hasher.update(output_frame);
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
        if progress_blocks != 0
            && (blocks.is_multiple_of(progress_blocks) || number + 1 == source_rows.len())
        {
            eprintln!(
                "{{\"kind\":\"archive-v2-metadata-normalization-progress\",\"blocks\":{blocks},\"total_blocks\":{},\"source_bytes\":{},\"target_bytes\":{},\"legacy_error_records\":{},\"copied_blocks\":{},\"copied_bytes\":{},\"recompressed_blocks\":{},\"recompressed_source_bytes\":{},\"recompressed_target_bytes\":{}}}",
                source_rows.len(),
                source_row.compressed_offset + u64::from(source_row.compressed_len),
                target_offset,
                metadata_counts.legacy_error_records,
                frame_processing.copied_blocks,
                frame_processing.copied_bytes,
                frame_processing.recompressed_blocks,
                frame_processing.recompressed_source_bytes,
                frame_processing.recompressed_target_bytes,
            );
        }
    }
    let mut trailing = [0u8; 1];
    ensure!(
        source.read(&mut trailing)? == 0,
        "source block blob has bytes outside the index"
    );
    target.flush()?;
    target.get_ref().sync_all()?;
    drop(target);
    frame_processing.validate(
        source_rows.len() as u64,
        source_rows
            .last()
            .map(|row| row.compressed_offset + u64::from(row.compressed_len))
            .unwrap_or(0),
        target_offset,
    )?;

    Ok(RewriteOutput {
        target_rows,
        target_blocks: FileBinding {
            bytes: target_offset,
            sha256: hex_lower(&target_hasher.finalize()),
        },
        blocks: source_rows.len() as u64,
        transactions,
        message_bytes,
        message_sha256: hex_lower(&message_hasher.finalize()),
        metadata: metadata_counts,
        source_metadata_profile_counts: classification.source_profile_counts,
        frame_processing,
    })
}

fn normalize_metadata_region(
    rows: &mut [ArchiveV2HotTxRow],
    source_messages: &[u8],
    source_metadata: &[u8],
    target_metadata: &mut Vec<u8>,
    limits: ArchiveV2WireRewriteLimits,
    slot: u64,
    classification: &mut MetadataClassificationState,
) -> Result<MetadataCounts> {
    target_metadata.clear();
    if target_metadata.capacity() < source_metadata.len() {
        target_metadata
            .try_reserve_exact(source_metadata.len())
            .context("reserve normalized metadata region")?;
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
            "slot {slot} tx {} has non-canonical message offset {} instead of {message_cursor}",
            row.tx_index,
            row.message_offset
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
            .context("normalized metadata region exceeds u32::MAX")?;

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
            row.tx_index,
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
            "slot {slot} tx {} metadata exceeds the configured input limit",
            row.tx_index
        );
        let has_error = input.first() == Some(&1);
        ensure!(
            has_error == (row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0),
            "slot {slot} tx {} HAS_ERROR flag differs from metadata",
            row.tx_index
        );
        let source_classification = classification.source_classifier.classify(input);
        classification
            .source_profile_counts
            .checked_observe(source_classification)
            .map_err(|error| anyhow!(error))?;
        ensure!(
            !matches!(
                source_classification,
                ArchiveV2MetadataSchemaClassification::BothDifferent
                    | ArchiveV2MetadataSchemaClassification::Invalid
                    | ArchiveV2MetadataSchemaClassification::RawFallback
            ),
            "slot {slot} tx {} source metadata is divergent, invalid, or raw: {source_classification:?}",
            row.tx_index
        );
        let output_start = target_metadata.len();
        let selected_schema =
            match rewrite_archive_v2_metadata_wire(input, target_metadata, &mut visitor, limits) {
                Ok(stats) => {
                    ensure!(
                        stats.input_bytes == input.len(),
                        "slot {slot} tx {} metadata input coverage differs",
                        row.tx_index
                    );
                    ensure!(
                        stats.output_bytes == target_metadata.len() - output_start,
                        "slot {slot} tx {} metadata output coverage differs",
                        row.tx_index
                    );
                    stats.metadata_error_schema
                }
                Err(error)
                    if matches!(error.kind(), ArchiveV2WireRewriteErrorKind::Fallback(_)) =>
                {
                    ensure!(
                        target_metadata.len() == output_start,
                        "slot {slot} tx {} metadata fallback did not roll back",
                        row.tx_index
                    );
                    counts.owned_fallback_records = checked_add(
                        counts.owned_fallback_records,
                        1,
                        "owned metadata fallback count",
                    )?;
                    if error.fallback_reason()
                        == Some(ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous)
                    {
                        counts.ambiguous_owned_fallback_records = checked_add(
                            counts.ambiguous_owned_fallback_records,
                            1,
                            "ambiguous metadata fallback count",
                        )?;
                    }
                    let (canonical, schema) = canonicalize_archive_v2_metadata_owned(input)
                        .with_context(|| {
                            format!(
                                "normalize slot {slot} tx {} through exact dual decoder",
                                row.tx_index
                            )
                        })?;
                    ensure!(
                        canonical.len() <= limits.max_output_bytes,
                        "slot {slot} tx {} canonical metadata exceeds the configured output limit",
                        row.tx_index
                    );
                    target_metadata.extend_from_slice(&canonical);
                    has_error.then_some(schema)
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error).context(format!(
                        "normalize slot {slot} tx {} metadata",
                        row.tx_index
                    )));
                }
            };
        if !has_error {
            ensure!(
                source_classification == ArchiveV2MetadataSchemaClassification::NoError,
                "slot {slot} tx {} successful metadata has a non-success source classification",
                row.tx_index
            );
            ensure!(
                &target_metadata[output_start..] == input,
                "slot {slot} tx {} successful metadata changed bytes",
                row.tx_index
            );
            counts.successful_records = checked_add(
                counts.successful_records,
                1,
                "successful metadata record count",
            )?;
        } else {
            match selected_schema.context("present metadata error has no selected schema")? {
                ArchiveV2WireMetadataErrorSchema::Current => {
                    ensure!(
                        matches!(
                            source_classification,
                            ArchiveV2MetadataSchemaClassification::CurrentOnly
                                | ArchiveV2MetadataSchemaClassification::BothEqual
                        ),
                        "slot {slot} tx {} current rewrite selection differs from exact source classification",
                        row.tx_index
                    );
                    counts.current_error_records = checked_add(
                        counts.current_error_records,
                        1,
                        "current-error metadata record count",
                    )?;
                }
                ArchiveV2WireMetadataErrorSchema::Legacy => {
                    ensure!(
                        matches!(
                            source_classification,
                            ArchiveV2MetadataSchemaClassification::LegacyOnly
                                | ArchiveV2MetadataSchemaClassification::BothEqual
                        ),
                        "slot {slot} tx {} legacy rewrite selection differs from exact source classification",
                        row.tx_index
                    );
                    counts.legacy_error_records = checked_add(
                        counts.legacy_error_records,
                        1,
                        "legacy-error metadata record count",
                    )?;
                }
            }
        }
        row.metadata_len = u32::try_from(target_metadata.len() - output_start)
            .context("one normalized metadata record exceeds u32::MAX")?;
        if has_error {
            let target_record = &target_metadata[output_start..];
            match classification.target_classifier.classify(target_record) {
                ArchiveV2MetadataSchemaClassification::CurrentOnly => {
                    counts.target_current_only_records = checked_add(
                        counts.target_current_only_records,
                        1,
                        "target current-only metadata record count",
                    )?;
                }
                ArchiveV2MetadataSchemaClassification::BothEqual => {
                    counts.target_both_equal_records = checked_add(
                        counts.target_both_equal_records,
                        1,
                        "target both-equal metadata record count",
                    )?;
                }
                classification => {
                    bail_target_metadata_classification(slot, row.tx_index, classification)?
                }
            }
        }
        counts.records = checked_add(counts.records, 1, "metadata record count")?;
        counts.input_bytes = checked_add(
            counts.input_bytes,
            input.len() as u64,
            "metadata input bytes",
        )?;
        counts.output_bytes = checked_add(
            counts.output_bytes,
            (target_metadata.len() - output_start) as u64,
            "metadata output bytes",
        )?;
    }
    ensure!(
        message_cursor == source_messages.len(),
        "slot {slot} transaction rows cover {message_cursor} of {} message bytes",
        source_messages.len()
    );
    ensure!(
        metadata_cursor == source_metadata.len(),
        "slot {slot} transaction rows cover {metadata_cursor} of {} metadata bytes",
        source_metadata.len()
    );
    let proven_target_errors = checked_add(
        counts.target_current_only_records,
        counts.target_both_equal_records,
        "proven target error record count",
    )?;
    let source_errors = checked_add(
        counts.current_error_records,
        counts.legacy_error_records,
        "source error record count",
    )?;
    ensure!(
        proven_target_errors == source_errors,
        "slot {slot} target current-error proof does not cover every source error record"
    );
    Ok(counts)
}

fn bail_target_metadata_classification(
    slot: u64,
    tx_index: u32,
    classification: ArchiveV2MetadataSchemaClassification,
) -> Result<()> {
    Err(anyhow!(
        "slot {slot} tx {tx_index} target metadata is not canonical current typed-error wire: {classification:?}"
    ))
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
    let decoded = deserialize_archive_v2_hot_block_blob_borrowed_current(serialized)
        .map_err(|error| anyhow!(error))
        .context("verify normalized current outer block")?;
    ensure!(
        same_hot_block_header(&decoded.header, expected_header),
        "normalized block header or reward value changed"
    );
    ensure!(
        decoded.tx_count as usize == expected_rows.len(),
        "normalized block transaction count changed"
    );
    ensure!(
        decoded.message_bytes == expected_messages,
        "normalized block message bytes changed"
    );
    ensure!(
        decoded.metadata_bytes == expected_metadata,
        "normalized block metadata bytes changed"
    );
    ensure!(
        decoded.tx_rows().eq(expected_rows.iter().copied()),
        "normalized block transaction-row geometry changed after serialization"
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
    source: &SourceSnapshot,
    staging: &StagingDirectory,
    epoch_start_slot: u64,
    slots_per_epoch: u64,
    source_rows: &[ArchiveV2HotBlockIndexRow],
    target_rows: &[ArchiveV2HotBlockIndexRow],
    output_files: &mut BTreeMap<String, FileBinding>,
) -> Result<u64> {
    let Some(_) = source.optional_file(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)? else {
        return Ok(0);
    };
    let access_index_binding = source
        .binding(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE)
        .context("source get-block index exists without its required block-access index")?;
    let access_blob_binding = source
        .binding(ARCHIVE_V2_BLOCK_ACCESS_FILE)
        .context("source get-block index exists without its required block-access blob")?;
    let get_block_bytes =
        source.read_all_bounded(ARCHIVE_V2_GET_BLOCK_INDEX_FILE, MAX_SOURCE_INDEX_BYTES)?;
    let source_get_block_rows = parse_get_block_index_bytes(&get_block_bytes)?;
    let access_index_bytes =
        source.read_all_bounded(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE, MAX_SOURCE_INDEX_BYTES)?;
    let access_index = parse_block_access_index_bytes(&access_index_bytes)?;
    ensure!(
        access_index.flags == 0,
        "source block-access index has unsupported flags {:#x}",
        access_index.flags
    );
    ensure!(
        access_index.blob_file_bytes == access_blob_binding.identity.bytes,
        "source block-access blob length differs from its index"
    );
    ensure!(
        access_index_binding.identity.bytes == access_index_bytes.len() as u64,
        "pinned block-access index length changed"
    );
    let (rows, rebuilt) = rebuild_get_block_rows(
        &source_get_block_rows,
        source_rows,
        target_rows,
        &access_index.rows,
        access_index.blob_file_bytes,
        epoch_start_slot,
        slots_per_epoch,
    )?;
    let binding =
        write_get_block_index_file(staging.create_file(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)?, &rows)?;
    staging.sync_file(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)?;
    staging.sync()?;
    let persisted = parse_get_block_index_bytes(
        &staging.read_all_bounded(ARCHIVE_V2_GET_BLOCK_INDEX_FILE, MAX_SOURCE_INDEX_BYTES)?,
    )?;
    ensure!(
        persisted.len() == rows.len()
            && persisted.iter().zip(&rows).all(|(left, right)| {
                left.block_offset == right.block_offset
                    && left.block_len == right.block_len
                    && left.access_offset == right.access_offset
                    && left.access_len == right.access_len
            }),
        "persisted target get-block rows differ from the proven rebuild"
    );
    ensure!(
        output_files
            .insert(ARCHIVE_V2_GET_BLOCK_INDEX_FILE.to_owned(), binding)
            .is_none(),
        "get-block index conflicts with a copied sidecar"
    );
    Ok(rebuilt)
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
            "block-access row {number} transaction or signature totals differ from the hot index"
        );
        ensure!(
            access.access_offset == access_cursor && access.access_len > 0,
            "block-access row {number} has non-canonical access geometry"
        );
        access_cursor = checked_add(
            access_cursor,
            u64::from(access.access_len),
            "block-access blob cursor",
        )?;
        ensure!(
            by_slot.insert(source.slot, number).is_none(),
            "hot index contains a duplicate slot"
        );
    }
    ensure!(
        access_cursor == access_blob_bytes,
        "block-access index covers {access_cursor} of {access_blob_bytes} blob bytes"
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
            "get-block row {slot_offset} does not exactly match hot/access indexes for slot {slot}"
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

fn copy_sidecars(source: &SourceSnapshot, staging: &StagingDirectory) -> Result<SidecarCopyOutput> {
    let mut bindings = BTreeMap::new();
    let mut omitted_controls = Vec::new();
    let mut copied_files = 0u64;
    let mut copied_bytes = 0u64;
    for (name, pinned) in &source.files {
        match pinned.disposition {
            SourceDisposition::CopySidecar => {
                let binding = copy_regular_file_exact(
                    source.file(name)?,
                    staging.create_file(name)?,
                    &pinned.content,
                )?;
                ensure!(
                    binding == pinned.content,
                    "copied sidecar {name} differs from its admitted source hash"
                );
                copied_files = checked_add(copied_files, 1, "copied sidecar count")?;
                copied_bytes = checked_add(copied_bytes, binding.bytes, "copied sidecar bytes")?;
                ensure!(bindings.insert(name.clone(), binding).is_none());
            }
            SourceDisposition::OmitControl => {
                omitted_controls.push(name.clone());
            }
            SourceDisposition::RewriteBlocks
            | SourceDisposition::RewriteHotIndex
            | SourceDisposition::RebuildGetBlockIndex => {}
        }
    }
    Ok(SidecarCopyOutput {
        bindings,
        omitted_controls,
        copied_files,
        copied_bytes,
    })
}

fn copy_regular_file_exact(
    mut source_file: File,
    target_file: File,
    expected: &FileBinding,
) -> Result<FileBinding> {
    source_file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, source_file);
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, target_file);
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut bytes = 0u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        bytes = checked_add(bytes, read as u64, "copied sidecar byte count")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    ensure!(
        bytes == expected.bytes,
        "copied sidecar byte count differs from its authority binding"
    );
    Ok(expected.clone())
}

#[cfg(test)]
fn hash_regular_file(path: &Path) -> Result<FileBinding> {
    let file = open_absolute_regular_nofollow(path)
        .with_context(|| format!("open {} for hashing", path.display()))?;
    hash_open_regular_file(&file)
}

fn write_json_last(
    staging: &StagingDirectory,
    file_name: &str,
    value: &impl Serialize,
) -> Result<()> {
    cstring_component(OsStr::new(file_name))?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_nanos();
    let temporary = format!(".{file_name}.tmp.{}.{nonce}", std::process::id());
    let file = staging.create_file(&temporary)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    let temporary_c = cstring_component(OsStr::new(&temporary))?;
    let final_c = cstring_component(OsStr::new(file_name))?;
    let renamed = renameat_noreplace(
        &staging.directory,
        &temporary_c,
        &staging.directory,
        &final_c,
    );
    if renamed != 0 {
        let rename_error = io::Error::last_os_error();
        // SAFETY: the temporary name and pinned directory stay live for the call.
        let removed =
            unsafe { libc::unlinkat(staging.directory.as_raw_fd(), temporary_c.as_ptr(), 0) };
        ensure!(
            removed == 0,
            "publish staging control {file_name} failed ({rename_error}); also failed to remove its private temporary: {}",
            io::Error::last_os_error()
        );
        staging.sync()?;
        return Err(rename_error)
            .with_context(|| format!("publish staging control {file_name} without replacement"));
    }
    staging.sync()
}

#[cfg(target_os = "macos")]
fn renameat_noreplace(
    source_directory: &File,
    source_name: &CStr,
    target_directory: &File,
    target_name: &CStr,
) -> libc::c_int {
    // SAFETY: both pinned descriptors and names stay live for the call.
    unsafe {
        libc::renameatx_np(
            source_directory.as_raw_fd(),
            source_name.as_ptr(),
            target_directory.as_raw_fd(),
            target_name.as_ptr(),
            libc::RENAME_EXCL,
        )
    }
}

#[cfg(target_os = "linux")]
fn renameat_noreplace(
    source_directory: &File,
    source_name: &CStr,
    target_directory: &File,
    target_name: &CStr,
) -> libc::c_int {
    const RENAME_NOREPLACE: libc::c_uint = 1;
    // SAFETY: both pinned descriptors and names stay live for the system call.
    unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            source_directory.as_raw_fd(),
            source_name.as_ptr(),
            target_directory.as_raw_fd(),
            target_name.as_ptr(),
            RENAME_NOREPLACE,
        ) as libc::c_int
    }
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64> {
    left.checked_add(right)
        .with_context(|| format!("{label} overflow"))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive_v2_source_authority_common::{
        SOURCE_AUTHORITY_KIND, SOURCE_AUTHORITY_SCHEMA_VERSION, SourceAuthorityFile,
        compute_authority_digest,
    };
    use blockzilla_format::{
        ARCHIVE_V2_META_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_TX_FLAG_HAS_ERROR, ArchiveV2HotBlockBlob, ArchiveV2HotInstruction,
        ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
        ArchiveV2HotMetaRecord, ArchiveV2HotRewards, CompactMessageHeader, CompactMetaV1,
        CompactPubkey, CompactReward, CompactTransactionError, KeyIndex,
        OwnedCompactRecentBlockhash, WINCODE_ARCHIVE_V2_FLAG_LEB128,
        WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer, WincodeArchiveV2Header,
        WincodeLeb128FramedWriter, write_archive_v2_block_access_index,
        write_archive_v2_get_block_index, write_archive_v2_hot_block_index,
    };
    use blockzilla_read_sdk::{
        ArchiveV2WireProfile,
        manifest::{GENERATION_MANIFEST_SCHEMA_VERSION, GenerationFile, compute_generation_digest},
        wire_profile_marker_bytes,
    };
    use of_car_reader::stored_transaction::{
        InstructionError as StoredInstructionError, StoredTransactionError,
    };
    use serde_json::Value;
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

    fn legacy_error_metadata() -> Vec<u8> {
        legacy_error_metadata_at(7)
    }

    fn legacy_error_metadata_at(instruction_index: u8) -> Vec<u8> {
        let message = "x".repeat(96);
        let current_error = CompactTransactionError::InstructionError(
            instruction_index,
            blockzilla_format::CompactInstructionError::BorshIoError(message.clone()),
        );
        let current_error_bytes =
            wincode::config::serialize(&current_error, wincode_leb128_config()).unwrap();
        let current = current_metadata(Some(current_error));
        assert_eq!(current[0], 1);
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

    fn write_complete_legacy_source(source: &Path) -> BTreeMap<String, Vec<u8>> {
        fs::create_dir(source).unwrap();
        let message = wincode::config::serialize(
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
        .unwrap();
        let make_frame = |slot: u64, metadata: Vec<u8>, has_error: bool| {
            let row = ArchiveV2HotTxRow {
                tx_index: 0,
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | if has_error {
                        ARCHIVE_V2_TX_FLAG_HAS_ERROR
                    } else {
                        0
                    },
                message_offset: 0,
                message_len: message.len() as u32,
                metadata_offset: 0,
                metadata_len: metadata.len() as u32,
                signature_count: 1,
                reserved: [0; 3],
            };
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
                tx_rows: vec![row],
                message_bytes: message.clone(),
                metadata_bytes: metadata,
            };
            let uncompressed = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
            let compressed = zstd::bulk::compress(&uncompressed, 3).unwrap();
            (uncompressed, compressed)
        };
        let (first_uncompressed, first_compressed) = make_frame(10, current_metadata(None), false);
        let (second_uncompressed, second_compressed) =
            make_frame(12, legacy_error_metadata_at(0), true);
        let mut compressed_blocks = first_compressed.clone();
        compressed_blocks.extend_from_slice(&second_compressed);
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), &compressed_blocks).unwrap();
        write_archive_v2_hot_block_index(
            &source.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed_blocks.len() as u64,
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
        let access_bytes = b"accessaccess";
        fs::write(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE), access_bytes).unwrap();
        write_archive_v2_block_access_index(
            &source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
            access_bytes.len() as u64,
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
        let mut get_block_rows = vec![ArchiveV2GetBlockIndexRow::missing(); 20];
        get_block_rows[10] = ArchiveV2GetBlockIndexRow {
            block_offset: 0,
            block_len: first_compressed.len() as u32,
            access_offset: 0,
            access_len: 6,
        };
        get_block_rows[12] = ArchiveV2GetBlockIndexRow {
            block_offset: first_compressed.len() as u64,
            block_len: second_compressed.len() as u32,
            access_offset: 6,
            access_len: 6,
        };
        write_archive_v2_get_block_index(
            &source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
            &get_block_rows,
        )
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

        fs::write(
            source.join("archive-v2-pre-to-post.receipt.json"),
            b"{\"source_lineage\":true}\n",
        )
        .unwrap();
        fs::write(source.join("operator-notes.txt"), b"ignored debris\n").unwrap();

        let message_profile = ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;
        let message_marker = wire_profile_marker(message_profile);
        fs::write(
            source.join(&message_marker.name),
            wire_profile_marker_bytes(message_profile),
        )
        .unwrap();
        let published_names = [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
            ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
            ARCHIVE_V2_SIGNATURES_FILE,
            "archive-v2-pre-to-post.receipt.json",
            message_marker.name.as_str(),
        ];
        let mut files = published_names
            .iter()
            .map(|name| {
                let binding = hash_regular_file(&source.join(name)).unwrap();
                GenerationFile {
                    name: (*name).to_owned(),
                    size: binding.bytes,
                    sha256: binding.sha256,
                }
            })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| left.name.cmp(&right.name));
        let mut manifest = GenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 0,
            generation_id: "legacy-source".to_owned(),
            generation_digest: "0".repeat(64),
            slots_per_epoch: 20,
            complete: true,
            files,
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        manifest.validate().unwrap();
        let mut manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
        manifest_bytes.push(b'\n');
        fs::write(source.join(GENERATION_MANIFEST_FILE), manifest_bytes).unwrap();

        directory_file_bytes(source)
    }

    fn directory_file_bytes(root: &Path) -> BTreeMap<String, Vec<u8>> {
        let mut files = BTreeMap::new();
        fn visit(root: &Path, relative: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
            for entry in fs::read_dir(root.join(relative)).unwrap() {
                let entry = entry.unwrap();
                let child = relative.join(entry.file_name());
                if entry.file_type().unwrap().is_dir() {
                    visit(root, &child, files);
                } else {
                    files.insert(
                        child.to_str().unwrap().to_owned(),
                        fs::read(entry.path()).unwrap(),
                    );
                }
            }
        }
        visit(root, Path::new(""), &mut files);
        files
    }

    fn write_external_source_authority(root: &Path, source: &Path) -> (PathBuf, String) {
        fs::remove_file(source.join(GENERATION_MANIFEST_FILE)).unwrap();
        let mut files = fs::read_dir(source)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_type().unwrap().is_file())
            .filter_map(|entry| {
                let name = entry.file_name().into_string().unwrap();
                let disposition = known_disposition(&name)?;
                let binding = hash_regular_file(&entry.path()).unwrap();
                Some(SourceAuthorityFile {
                    disposition,
                    name,
                    bytes: binding.bytes,
                    sha256: binding.sha256,
                })
            })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| left.name.cmp(&right.name));
        let mut inventory = SourceAuthorityInventory {
            schema_version: SOURCE_AUTHORITY_SCHEMA_VERSION,
            kind: SOURCE_AUTHORITY_KIND.to_owned(),
            complete: true,
            authority_id: "external-legacy-source".to_owned(),
            authority_digest: "0".repeat(64),
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 0,
            slots_per_epoch: 20,
            message_wire_profile: "post-unknown-instruction-fallbacks-v1".to_owned(),
            metadata_wire_profile: "unmarked-historical-compatibility".to_owned(),
            files,
        };
        inventory.authority_digest = compute_authority_digest(&inventory).unwrap();
        inventory.validate().unwrap();
        let path = root.join("external-source-authority.json");
        let mut bytes = serde_json::to_vec_pretty(&inventory).unwrap();
        bytes.push(b'\n');
        fs::write(&path, bytes).unwrap();
        let sha256 = hash_regular_file(&path).unwrap().sha256;
        (path, sha256)
    }

    fn row(
        tx_index: u32,
        message_offset: u32,
        metadata_offset: u32,
        metadata_len: u32,
        error: bool,
    ) -> ArchiveV2HotTxRow {
        ArchiveV2HotTxRow {
            tx_index,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | if error {
                    ARCHIVE_V2_TX_FLAG_HAS_ERROR
                } else {
                    0
                },
            message_offset,
            message_len: 1,
            metadata_offset,
            metadata_len,
            signature_count: 1,
            reserved: [0; 3],
        }
    }

    #[test]
    fn metadata_region_normalizes_legacy_error_and_rebuilds_offsets() {
        let success = current_metadata(None);
        let legacy = legacy_error_metadata();
        let mut source = success.clone();
        source.extend_from_slice(&legacy);
        let mut rows = vec![
            row(0, 0, 0, success.len() as u32, false),
            row(1, 1, success.len() as u32, legacy.len() as u32, true),
        ];
        let mut target = Vec::new();
        let mut classification = MetadataClassificationState::default();
        let counts = normalize_metadata_region(
            &mut rows,
            b"xy",
            &source,
            &mut target,
            ArchiveV2WireRewriteLimits::default(),
            42,
            &mut classification,
        )
        .unwrap();

        assert_eq!(&target[..success.len()], &success);
        assert_eq!(rows[0].metadata_offset, 0);
        assert_eq!(rows[1].metadata_offset, success.len() as u32);
        assert_ne!(rows[1].metadata_len, legacy.len() as u32);
        assert_eq!(counts.records, 2);
        assert_eq!(counts.successful_records, 1);
        assert_eq!(counts.legacy_error_records, 1);
        assert_eq!(classification.source_profile_counts.no_error, 1);
        assert_eq!(classification.source_profile_counts.legacy_only, 1);
        let second_start = rows[1].metadata_offset as usize;
        let second_end = second_start + rows[1].metadata_len as usize;
        let decoded: CompactMetaV1 = wincode::config::deserialize_exact(
            &target[second_start..second_end],
            blockzilla_format::bounded_wincode_leb128_config::<DEFAULT_MAX_METADATA_BYTES>(),
        )
        .unwrap();
        assert!(matches!(
            decoded.err,
            Some(CompactTransactionError::InstructionError(
                7,
                blockzilla_format::CompactInstructionError::BorshIoError(ref message)
            )) if message.len() == 96
        ));
    }

    #[test]
    fn frame_processing_counts_require_exact_block_and_byte_coverage() {
        let mut counts = FrameProcessingCounts::default();
        counts.observe_copy(10).unwrap();
        counts.observe_recompression(20, 15).unwrap();
        counts.validate(2, 30, 25).unwrap();

        assert!(counts.validate(1, 30, 25).is_err());
        assert!(counts.validate(2, 29, 25).is_err());
        assert!(counts.validate(2, 30, 24).is_err());
    }

    #[test]
    fn folded_source_row_admission_checks_exact_sdk_geometry() {
        let metadata = current_metadata(None);
        let valid = row(0, 0, 0, metadata.len() as u32, false);
        let index = ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: 42,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count: 1,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 1,
        };
        validate_source_block_row_geometry(&index, &[valid], b"x", &metadata).unwrap();

        let mut unknown_flags = valid;
        unknown_flags.flags |= 1 << 31;
        assert!(
            validate_source_block_row_geometry(&index, &[unknown_flags], b"x", &metadata).is_err()
        );
        let mut empty_message = valid;
        empty_message.message_len = 0;
        assert!(
            validate_source_block_row_geometry(&index, &[empty_message], b"x", &metadata).is_err()
        );
        let mut wrong_signature_total = index;
        wrong_signature_total.signature_count = 2;
        assert!(
            validate_source_block_row_geometry(&wrong_signature_total, &[valid], b"x", &metadata,)
                .is_err()
        );

        let no_metadata = ArchiveV2HotTxRow {
            flags: 0,
            metadata_offset: 99,
            metadata_len: 0,
            ..valid
        };
        validate_source_block_row_geometry(&index, &[no_metadata], b"x", &[]).unwrap();
    }

    #[test]
    fn header_construction_assertion_includes_each_reward_field() {
        let header = |lamports| ArchiveV2HotBlockHeader {
            slot: 42,
            parent_slot: 41,
            blockhash_id: 2,
            previous_blockhash_id: 1,
            block_time: Some(1_700_000_000),
            block_height: Some(40),
            rewards: Some(ArchiveV2HotRewards {
                num_partitions: Some(4),
                decoded: vec![CompactReward {
                    pubkey: CompactPubkey::Id(1),
                    lamports,
                    post_balance: 99,
                    reward_type: 2,
                    commission: Some(5),
                }],
            }),
        };
        assert!(same_hot_block_header(&header(7), &header(7)));
        assert!(!same_hot_block_header(&header(7), &header(8)));
    }

    #[test]
    fn same_bytes_recompress_when_the_target_zstd_level_differs() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source-generation");
        let source_files = write_complete_legacy_source(&source);
        let source_index = parse_hot_index_bytes(
            source_files
                .get(ARCHIVE_V2_BLOCK_INDEX_FILE)
                .expect("source hot index"),
        )
        .unwrap();
        let source_blocks = source_files
            .get(ARCHIVE_V2_BLOCKS_FILE)
            .expect("source blocks");
        let mut row = source_index.rows[0];
        row.compressed_offset = 0;
        let frame = &source_blocks[..row.compressed_len as usize];
        let one_frame = root.join("one-frame.bin");
        fs::write(&one_frame, frame).unwrap();
        let target = root.join("recompressed.bin");

        let output = rewrite_blocks(
            File::open(one_frame).unwrap(),
            File::create(&target).unwrap(),
            &[row],
            RewriteOptions {
                source_zstd_level: 3,
                source_index_flags: 0,
                target_zstd_level: 4,
                max_metadata_bytes: DEFAULT_MAX_METADATA_BYTES,
                progress_blocks: 0,
            },
        )
        .unwrap();

        assert_eq!(output.frame_processing.copied_blocks, 0);
        assert_eq!(output.frame_processing.copied_bytes, 0);
        assert_eq!(output.frame_processing.recompressed_blocks, 1);
        assert_eq!(
            output.frame_processing.recompressed_source_bytes,
            frame.len() as u64
        );
        output
            .frame_processing
            .validate(1, frame.len() as u64, output.target_blocks.bytes)
            .unwrap();
        assert_eq!(
            fs::metadata(target).unwrap().len(),
            output.target_blocks.bytes
        );
    }

    #[test]
    fn every_source_range_must_be_exactly_one_zstd_frame_before_any_branch() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source-generation");
        let source_files = write_complete_legacy_source(&source);
        let source_index = parse_hot_index_bytes(
            source_files
                .get(ARCHIVE_V2_BLOCK_INDEX_FILE)
                .expect("source hot index"),
        )
        .unwrap();
        let source_blocks = source_files
            .get(ARCHIVE_V2_BLOCKS_FILE)
            .expect("source blocks");
        let first_row = source_index.rows[0];
        let first_frame = &source_blocks[..first_row.compressed_len as usize];
        let second_frame = zstd::bulk::compress(b"another complete frame", 3).unwrap();

        for (malformation, suffix) in [
            (second_frame.as_slice(), "concatenated"),
            (&b"trailing bytes"[..], "trailing"),
        ] {
            for target_level in [3, 4] {
                let mut malformed = first_frame.to_vec();
                malformed.extend_from_slice(malformation);
                let input = root.join(format!("{suffix}-level-{target_level}.bin"));
                fs::write(&input, &malformed).unwrap();
                let target = root.join(format!("{suffix}-level-{target_level}.target"));
                let mut row = first_row;
                row.compressed_offset = 0;
                row.compressed_len = malformed.len() as u32;

                let error = rewrite_blocks(
                    File::open(input).unwrap(),
                    File::create(target).unwrap(),
                    &[row],
                    RewriteOptions {
                        source_zstd_level: 3,
                        source_index_flags: 0,
                        target_zstd_level: target_level,
                        max_metadata_bytes: DEFAULT_MAX_METADATA_BYTES,
                        progress_blocks: 0,
                    },
                )
                .unwrap_err();
                assert!(
                    format!("{error:#}").contains("exactly one complete zstd frame"),
                    "unexpected error for {suffix} input at target level {target_level}: {error:#}"
                );
            }
        }
    }

    #[test]
    fn get_block_rebuild_changes_only_block_geometry() {
        let source_hot = vec![
            ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: 10,
                compressed_offset: 0,
                compressed_len: 10,
                uncompressed_len: 20,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 1,
            },
            ArchiveV2HotBlockIndexRow {
                block_id: 1,
                slot: 12,
                compressed_offset: 10,
                compressed_len: 30,
                uncompressed_len: 50,
                tx_count: 1,
                first_tx_ordinal: 1,
                first_signature_ordinal: 1,
                signature_count: 1,
            },
        ];
        let mut target_hot = source_hot.clone();
        target_hot[0].compressed_len = 12;
        target_hot[1].compressed_offset = 12;
        target_hot[1].compressed_len = 28;
        let access = vec![
            ArchiveV2BlockAccessIndexRow {
                block_id: 0,
                slot: 10,
                access_offset: 0,
                access_len: 55,
                tx_count: 1,
                signature_count: 1,
            },
            ArchiveV2BlockAccessIndexRow {
                block_id: 1,
                slot: 12,
                access_offset: 55,
                access_len: 44,
                tx_count: 1,
                signature_count: 1,
            },
        ];
        let source_get = vec![
            ArchiveV2GetBlockIndexRow {
                block_offset: 0,
                block_len: 10,
                access_offset: 0,
                access_len: 55,
            },
            ArchiveV2GetBlockIndexRow::missing(),
            ArchiveV2GetBlockIndexRow {
                block_offset: 10,
                block_len: 30,
                access_offset: 55,
                access_len: 44,
            },
        ];
        let (target, rebuilt) =
            rebuild_get_block_rows(&source_get, &source_hot, &target_hot, &access, 99, 10, 3)
                .unwrap();
        assert_eq!(rebuilt, 2);
        assert_eq!(target[0].block_offset, 0);
        assert_eq!(target[0].block_len, 12);
        assert_eq!(target[0].access_offset, 0);
        assert_eq!(target[0].access_len, 55);
        assert_eq!(target[2].block_offset, 12);
        assert_eq!(target[2].block_len, 28);
        assert_eq!(target[2].access_offset, 55);
        assert_eq!(target[2].access_len, 44);

        let mut permuted = source_get.clone();
        permuted.swap(0, 2);
        assert!(
            rebuild_get_block_rows(&permuted, &source_hot, &target_hot, &access, 99, 10, 3)
                .is_err()
        );
        let mut missing = source_get.clone();
        missing[2] = ArchiveV2GetBlockIndexRow::missing();
        assert!(
            rebuild_get_block_rows(&missing, &source_hot, &target_hot, &access, 99, 10, 3).is_err()
        );
        let mut corrupt_access = access.clone();
        corrupt_access[1].access_offset = 54;
        assert!(
            rebuild_get_block_rows(
                &source_get,
                &source_hot,
                &target_hot,
                &corrupt_access,
                99,
                10,
                3,
            )
            .is_err()
        );
    }

    #[test]
    fn complete_source_builds_one_strict_unpublished_generation() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source");
        let staging = root.join("staging");
        let source_before = write_complete_legacy_source(&source);
        let source_hot = parse_hot_index_bytes(
            source_before
                .get(ARCHIVE_V2_BLOCK_INDEX_FILE)
                .expect("source hot index"),
        )
        .unwrap();
        let source_blocks = source_before
            .get(ARCHIVE_V2_BLOCKS_FILE)
            .expect("source blocks");
        let first_source_frame =
            source_blocks[..source_hot.rows[0].compressed_len as usize].to_vec();

        let receipt = normalize_generation(NormalizeOptions {
            source: &source,
            source_authority_inventory: None,
            source_authority_sha256: None,
            staging: &staging,
            epoch: 0,
            slots_per_epoch: 20,
            candidate_id: "normalized-current-target",
            requested_zstd_level: Some(3),
            max_metadata_bytes: DEFAULT_MAX_METADATA_BYTES,
            progress_blocks: 0,
        })
        .unwrap();

        assert_eq!(directory_file_bytes(&source), source_before);
        assert_eq!(
            receipt.source_metadata_profile,
            "unmarked-historical-compatibility"
        );
        assert_eq!(receipt.target_metadata_profile, "current-typed-errors-v1");
        assert_eq!(receipt.blocks, 2);
        assert_eq!(receipt.transactions, 2);
        assert_eq!(receipt.metadata.legacy_error_records, 1);
        assert_eq!(receipt.metadata.successful_records, 1);
        assert_eq!(receipt.source_metadata_profile_counts.no_error, 1);
        assert_eq!(receipt.source_metadata_profile_counts.legacy_only, 1);
        assert_eq!(receipt.frame_processing.copied_blocks, 1);
        assert_eq!(receipt.frame_processing.recompressed_blocks, 1);
        assert_eq!(
            receipt.frame_processing.copied_bytes,
            first_source_frame.len() as u64
        );
        assert_eq!(
            receipt.frame_processing.recompressed_source_bytes,
            u64::from(source_hot.rows[1].compressed_len)
        );
        receipt
            .frame_processing
            .validate(
                receipt.blocks,
                receipt.source_blocks.bytes,
                receipt.target_blocks.bytes,
            )
            .unwrap();
        assert_eq!(receipt.get_block_rows_rebuilt, 2);
        assert!(receipt.source_revalidated_at_completion);
        assert!(!receipt.canonical_publication_performed);
        assert_eq!(
            fs::read(staging.join(ARCHIVE_V2_SIGNATURES_FILE)).unwrap(),
            [9u8; 128]
        );
        assert!(!staging.join("archive-v2-pre-to-post.receipt.json").exists());
        assert!(!staging.join("operator-notes.txt").exists());
        assert_eq!(
            receipt.ignored_unrelated_source_entries,
            vec!["operator-notes.txt".to_owned()]
        );
        assert!(!staging.join(GENERATION_MANIFEST_FILE).exists());
        assert!(!staging.join(CURRENT_TYPED_ERRORS_MARKER_FILE).exists());
        assert!(
            !staging
                .join(
                    wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1)
                        .name
                )
                .exists()
        );
        let persisted_get_block = parse_get_block_index_bytes(
            &fs::read(staging.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)).unwrap(),
        )
        .unwrap();
        let persisted_hot =
            parse_hot_index_bytes(&fs::read(staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap())
                .unwrap();
        let target_blocks = fs::read(staging.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        assert_eq!(
            &target_blocks[..first_source_frame.len()],
            &first_source_frame
        );
        assert_eq!(persisted_hot.rows[0].block_id, source_hot.rows[0].block_id);
        assert_eq!(persisted_hot.rows[0].slot, source_hot.rows[0].slot);
        assert_eq!(
            persisted_hot.rows[0].compressed_offset,
            source_hot.rows[0].compressed_offset
        );
        assert_eq!(
            persisted_hot.rows[0].compressed_len,
            source_hot.rows[0].compressed_len
        );
        assert_eq!(
            persisted_hot.rows[0].uncompressed_len,
            source_hot.rows[0].uncompressed_len
        );
        assert_eq!(persisted_hot.rows[1].block_id, source_hot.rows[1].block_id);
        assert_eq!(persisted_hot.rows[1].slot, source_hot.rows[1].slot);
        assert_eq!(persisted_hot.rows[1].first_tx_ordinal, 1);
        assert_eq!(persisted_hot.rows[1].first_signature_ordinal, 1);
        assert_eq!(persisted_get_block.len(), 20);
        assert_eq!(
            persisted_get_block[10].block_offset,
            persisted_hot.rows[0].compressed_offset
        );
        assert_eq!(
            persisted_get_block[10].block_len,
            persisted_hot.rows[0].compressed_len
        );
        assert_eq!(persisted_get_block[10].access_offset, 0);
        assert_eq!(persisted_get_block[10].access_len, 6);
        assert_eq!(
            persisted_get_block[12].block_offset,
            persisted_hot.rows[1].compressed_offset
        );
        assert_eq!(
            persisted_get_block[12].block_len,
            persisted_hot.rows[1].compressed_len
        );
        assert_eq!(persisted_get_block[12].access_offset, 6);
        assert_eq!(persisted_get_block[12].access_len, 6);

        let receipt_path = staging.join(NORMALIZATION_RECEIPT_FILE);
        assert!(receipt_path.is_file());
        let receipt_json: Value =
            serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
        assert_eq!(
            receipt_json["state"],
            "complete-unpublished-staging-generation"
        );
        assert_eq!(
            receipt_json["source_files"]["archive-v2-pre-to-post.receipt.json"]["disposition"],
            "omit-control"
        );
        assert_eq!(
            receipt_json["message_marker"]["name"],
            wire_profile_marker(ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1).name
        );
        assert_eq!(
            receipt_json["metadata_marker"]["name"],
            CURRENT_TYPED_ERRORS_MARKER_FILE
        );
        assert_eq!(receipt_json["frame_processing"]["copied_blocks"], 1);
        assert_eq!(receipt_json["frame_processing"]["recompressed_blocks"], 1);
        let candidate_json: Value =
            serde_json::from_slice(&fs::read(staging.join(NORMALIZATION_MANIFEST_FILE)).unwrap())
                .unwrap();
        assert_eq!(candidate_json["state"], "audited-unpublished-candidate");
        assert_eq!(candidate_json["frame_processing"]["copied_blocks"], 1);
        assert_eq!(candidate_json["frame_processing"]["recompressed_blocks"], 1);
        assert_eq!(
            candidate_json["target_candidate_digest"],
            receipt_json["target_candidate_digest"]
        );
        let mut target_files = BTreeMap::new();
        for name in [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
            ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
            ARCHIVE_V2_SIGNATURES_FILE,
        ] {
            target_files.insert(
                name.to_owned(),
                hash_regular_file(&staging.join(name)).unwrap(),
            );
        }
        let target_candidate = build_target_candidate_manifest(
            &target_files,
            "mainnet-beta",
            0,
            20,
            "normalized-current-target",
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        )
        .unwrap();
        let target_source = PinnedLocalRangeSource::open_directory(&staging).unwrap();
        let target_reader = ArchiveReader::open_candidate(
            MarkerOverlay::new(target_source, target_candidate.marker_bytes),
            target_candidate.manifest,
            ReaderOpenOptions {
                hash_verification: HashVerification::AllFiles,
                ..ReaderOpenOptions::default()
            },
        )
        .unwrap();
        let proof = audit_current_metadata_for_marker_publication(&target_reader).unwrap();
        let audit = proof.audit();
        assert_eq!(audit.blocks, 2);
        assert_eq!(audit.counts.no_error, 1);
        assert_eq!(audit.counts.current_only + audit.counts.both_equal, 1);
    }

    #[test]
    fn external_authority_normalizes_an_unmanifested_complete_source() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source");
        let staging = root.join("staging");
        write_complete_legacy_source(&source);
        let (inventory, inventory_sha256) = write_external_source_authority(&root, &source);
        let source_before = directory_file_bytes(&source);

        let receipt = normalize_generation(NormalizeOptions {
            source: &source,
            source_authority_inventory: Some(&inventory),
            source_authority_sha256: Some(&inventory_sha256),
            staging: &staging,
            epoch: 0,
            slots_per_epoch: 20,
            candidate_id: "external-normalized-target",
            requested_zstd_level: Some(3),
            max_metadata_bytes: DEFAULT_MAX_METADATA_BYTES,
            progress_blocks: 0,
        })
        .unwrap();

        assert_eq!(directory_file_bytes(&source), source_before);
        assert_eq!(
            receipt.source_authority_kind,
            "external-source-authority-inventory"
        );
        assert_eq!(receipt.source_authority_binding.sha256, inventory_sha256);
        assert!(staging.join(NORMALIZATION_MANIFEST_FILE).is_file());
        assert!(staging.join(NORMALIZATION_RECEIPT_FILE).is_file());
        assert!(!staging.join(GENERATION_MANIFEST_FILE).exists());
        assert!(!staging.join(CURRENT_TYPED_ERRORS_MARKER_FILE).exists());
    }

    #[test]
    fn authority_rejects_unbound_archive_entries() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source");
        let staging = root.join("staging");
        write_complete_legacy_source(&source);
        fs::write(source.join("archive-v2-unbound-sidecar.bin"), b"debris").unwrap();

        let result = normalize_generation(NormalizeOptions {
            source: &source,
            source_authority_inventory: None,
            source_authority_sha256: None,
            staging: &staging,
            epoch: 0,
            slots_per_epoch: 20,
            candidate_id: "must-not-exist",
            requested_zstd_level: Some(3),
            max_metadata_bytes: DEFAULT_MAX_METADATA_BYTES,
            progress_blocks: 0,
        });
        assert!(result.is_err());
        assert!(!staging.exists());
    }

    #[test]
    fn partial_source_never_gets_a_complete_staging_receipt() {
        let temp = TempDir::new().unwrap();
        let source = temp.path().join("source");
        let staging = temp.path().join("staging");
        fs::create_dir(&source).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"not-an-archive").unwrap();
        write_archive_v2_hot_block_index(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE), 0, 1, 0, &[])
            .unwrap();
        let result = normalize_generation(NormalizeOptions {
            source: &source,
            source_authority_inventory: None,
            source_authority_sha256: None,
            staging: &staging,
            epoch: 0,
            slots_per_epoch: 1,
            candidate_id: "partial-must-fail",
            requested_zstd_level: Some(1),
            max_metadata_bytes: DEFAULT_MAX_METADATA_BYTES,
            progress_blocks: 0,
        });
        assert!(result.is_err());
        assert!(!staging.join(NORMALIZATION_RECEIPT_FILE).exists());
    }

    #[test]
    fn pinned_source_detects_content_mutation_and_target_rehash_detects_corruption() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source");
        let target = root.join("target");
        fs::create_dir(&source).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"blocks").unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCK_INDEX_FILE), b"index").unwrap();
        let descriptor_source = PinnedLocalRangeSource::open_directory(&source).unwrap();
        let mut expected_source = BTreeMap::new();
        for (name, disposition) in [
            (ARCHIVE_V2_BLOCKS_FILE, SourceDisposition::RewriteBlocks),
            (
                ARCHIVE_V2_BLOCK_INDEX_FILE,
                SourceDisposition::RewriteHotIndex,
            ),
        ] {
            expected_source.insert(
                name.to_owned(),
                (hash_regular_file(&source.join(name)).unwrap(), disposition),
            );
        }
        let pinned = SourceSnapshot::admit(descriptor_source, &expected_source).unwrap();
        let target = prepare_staging_location(&target, &pinned.directory_identity)
            .unwrap()
            .create()
            .unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"mutate").unwrap();
        assert!(pinned.revalidate_identity_inventory().is_err());

        let mut target_file = target.create_file("sidecar.bin").unwrap();
        target_file.write_all(b"expected").unwrap();
        target_file.sync_all().unwrap();
        drop(target_file);
        let target_path = target.display_path.join("sidecar.bin");
        let mut expected = BTreeMap::new();
        expected.insert(
            "sidecar.bin".to_owned(),
            target.hash_file("sidecar.bin").unwrap(),
        );
        fs::write(&target_path, b"corrupt!").unwrap();
        assert!(rehash_target_files(&target, &expected).is_err());
    }

    #[test]
    fn json_completion_control_never_replaces_an_existing_file() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source");
        let staging_path = root.join("staging");
        fs::create_dir(&source).unwrap();
        let source_identity = FileIdentity::from_directory_metadata(
            &open_absolute_directory_nofollow(&source)
                .unwrap()
                .0
                .metadata()
                .unwrap(),
        )
        .unwrap();
        let staging = prepare_staging_location(&staging_path, &source_identity)
            .unwrap()
            .create()
            .unwrap();
        let receipt = staging_path.join(NORMALIZATION_RECEIPT_FILE);
        fs::write(&receipt, b"existing").unwrap();
        let value = serde_json::json!({"complete": true});
        assert!(write_json_last(&staging, NORMALIZATION_RECEIPT_FILE, &value).is_err());
        assert_eq!(fs::read(receipt).unwrap(), b"existing");
    }

    #[cfg(unix)]
    #[test]
    fn source_admission_rejects_every_symlink_entry() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"blocks").unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCK_INDEX_FILE), b"index").unwrap();
        fs::write(root.join("outside"), b"sidecar").unwrap();
        symlink(root.join("outside"), source.join("linked-sidecar.bin")).unwrap();

        let descriptor_source = PinnedLocalRangeSource::open_directory(&source).unwrap();
        let mut expected = BTreeMap::new();
        expected.insert(
            ARCHIVE_V2_BLOCKS_FILE.to_owned(),
            (
                hash_regular_file(&source.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap(),
                SourceDisposition::RewriteBlocks,
            ),
        );
        expected.insert(
            ARCHIVE_V2_BLOCK_INDEX_FILE.to_owned(),
            (
                hash_regular_file(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap(),
                SourceDisposition::RewriteHotIndex,
            ),
        );
        assert!(SourceSnapshot::admit(descriptor_source, &expected).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_anchors_reject_source_aliases_and_directory_swaps() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let source = root.join("source");
        fs::create_dir(&source).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"initial").unwrap();
        let alias = root.join("source-alias");
        symlink(&source, &alias).unwrap();
        assert!(PinnedLocalRangeSource::open_directory(&alias).is_err());

        let pinned = PinnedLocalRangeSource::open_directory(&source).unwrap();
        assert_eq!(pinned.size(ARCHIVE_V2_BLOCKS_FILE).unwrap(), Some(7));
        let moved_source = root.join("source-moved");
        fs::rename(&source, &moved_source).unwrap();
        fs::create_dir(&source).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"replacement").unwrap();
        assert!(pinned.verify_unchanged().is_err());

        let source_identity = FileIdentity::from_directory_metadata(
            &open_absolute_directory_nofollow(&source)
                .unwrap()
                .0
                .metadata()
                .unwrap(),
        )
        .unwrap();
        let staging_path = root.join("staging");
        let staging = prepare_staging_location(&staging_path, &source_identity)
            .unwrap()
            .create()
            .unwrap();
        let moved_staging = root.join("staging-moved");
        fs::rename(&staging_path, &moved_staging).unwrap();
        fs::create_dir(&staging_path).unwrap();
        assert!(staging.recheck_anchor().is_err());
    }
}
