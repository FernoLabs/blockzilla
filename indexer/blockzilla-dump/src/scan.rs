use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::{self, File, OpenOptions as FsOpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use blockzilla_archive_v2::{
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotMessagePayload, ArchiveV2HotTxRow,
};
use blockzilla_compact::{CompactMessageHeader, CompactMetaV1};
use blockzilla_compact_v2_reader::{
    ArchiveIdentity, ArchiveReader, ArchiveSourceBinding, BorrowedDecodedBlock,
    COMPACT_V2_OPTIONAL_OBJECTS, COMPACT_V2_REQUIRED_OBJECTS, CompactV2MessageSchema,
    CompactV2MetadataSchema, CompiledPubkeyFilter, HashVerification, HttpObjectIdentity,
    HttpRangeSource, HttpRangeSourceOptions, MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES,
    OpenOptions, OrderedParallelBlockConfig, OverlayRangeSource, PinnedLocalRangeSource,
    RangeSource, SelectorIndeterminateReason, SelectorOutcome, SignatureReference, SourceError,
    SourceResult,
};
use blockzilla_primitives::CompactPubkey;
use sha2::{Digest, Sha256};

use crate::database::{
    CheckpointBatch, CoverageIssue, DumpDatabase, DumpKind, DumpSpec, MatchRecord, MessageState,
    MetadataState, OnIndeterminate, ProgramMatch, TokenBalanceRecord, TokenBalanceSide, TokenMatch,
    TransactionAccountRecord, TransactionAccountSource,
};

const CACHE_DOWNLOAD_CHUNK: usize = 8 * 1024 * 1024;
const REGISTRY_RESOLVER_ENTRIES: usize = 16_384;
const MAX_THREADS: usize = blockzilla_compact_v2_reader::MAX_ORDERED_PARALLEL_DECODE_WORKERS;
const OBJECT_SET_ID_DOMAIN: &[u8] = b"blockzilla/dump/compact-v2-etag-set/v1\0";
pub(crate) const LOCAL_OBJECT_SET: [&str; 19] = [
    blockzilla_archive_v2::ARCHIVE_V2_BLOCKS_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_BLOCK_INDEX_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_META_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_SIGNATURES_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_GENESIS_BIN_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_POH_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_SHREDDING_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_BLOCK_ACCESS_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    blockzilla_archive_v2::ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
];
static CACHE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub struct GatewayDumpSource {
    source: OverlayRangeSource<PinnedLocalRangeSource, HttpRangeSource>,
    http: HttpRangeSource,
    objects: Arc<BTreeMap<String, HttpObjectIdentity>>,
}

impl GatewayDumpSource {
    fn require_object(&self, object: &str) -> SourceResult<()> {
        if self.objects.contains_key(object) {
            Ok(())
        } else {
            Err(SourceError::NotFound(object.to_owned()))
        }
    }

    fn verify_unchanged(&self, epoch: u64) -> SourceResult<()> {
        for name in compact_v2_object_names(epoch) {
            match (self.objects.get(name), self.http.strong_identity(name)) {
                (Some(expected), Ok(actual)) if *expected == actual => {}
                (Some(_), Ok(_)) => {
                    return Err(SourceError::Protocol(format!(
                        "object {name} changed after the object set was bound"
                    )));
                }
                (Some(_), Err(error)) => return Err(error),
                (None, Err(SourceError::NotFound(_))) => {}
                (None, Ok(_)) => {
                    return Err(SourceError::Protocol(format!(
                        "object {name} became present after its absence was bound"
                    )));
                }
                (None, Err(error)) => return Err(error),
            }
        }
        Ok(())
    }
}

impl RangeSource for GatewayDumpSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        Ok(self.objects.get(object).map(|identity| identity.length))
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        self.require_object(object)?;
        self.source.read_range(object, offset, length)
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        self.require_object(object)?;
        self.source
            .read_range_into(object, offset, length, destination)
    }

    fn read_range_into_slice(
        &self,
        object: &str,
        offset: u64,
        destination: &mut [u8],
    ) -> SourceResult<()> {
        self.require_object(object)?;
        self.source
            .read_range_into_slice(object, offset, destination)
    }
}

#[derive(Clone)]
pub enum DumpSource {
    Local(PinnedLocalRangeSource),
    Gateway(GatewayDumpSource),
}

impl RangeSource for DumpSource {
    fn size(&self, object: &str) -> SourceResult<Option<u64>> {
        match self {
            Self::Local(source) => source.size(object),
            Self::Gateway(source) => source.size(object),
        }
    }

    fn read_range(&self, object: &str, offset: u64, length: usize) -> SourceResult<Vec<u8>> {
        match self {
            Self::Local(source) => source.read_range(object, offset, length),
            Self::Gateway(source) => source.read_range(object, offset, length),
        }
    }

    fn read_range_into(
        &self,
        object: &str,
        offset: u64,
        length: usize,
        destination: &mut Vec<u8>,
    ) -> SourceResult<()> {
        match self {
            Self::Local(source) => source.read_range_into(object, offset, length, destination),
            Self::Gateway(source) => source.read_range_into(object, offset, length, destination),
        }
    }
}

pub type DumpArchive = ArchiveReader<DumpSource>;

pub struct SourceOptions {
    pub archive: Option<PathBuf>,
    pub gateway: Option<String>,
    pub bearer_token: Option<String>,
    pub cache: Option<PathBuf>,
    pub allow_insecure_http: bool,
    pub cluster_id: String,
    pub local_generation_prefix: Option<String>,
    pub epoch_zero_first_slot: u64,
    pub slots_per_epoch: u64,
    pub message_schema: CompactV2MessageSchema,
    pub metadata_schema: CompactV2MetadataSchema,
}

impl SourceOptions {
    pub fn validate(&self) -> Result<()> {
        match (&self.archive, &self.gateway) {
            (Some(_), None) => {
                ensure!(
                    self.bearer_token.is_none(),
                    "a bearer token is valid only with --gateway"
                );
                ensure!(self.cache.is_none(), "a cache is valid only with --gateway");
                ensure!(
                    self.local_generation_prefix
                        .as_ref()
                        .is_some_and(|value| !value.is_empty()),
                    "--source-generation-prefix is required with --archive"
                );
            }
            (None, Some(_)) => {
                ensure!(self.cache.is_some(), "--cache is required with --gateway");
                ensure!(
                    self.local_generation_prefix.is_none(),
                    "--source-generation-prefix is valid only with --archive"
                );
            }
            (Some(_), Some(_)) => bail!("use exactly one of --archive or --gateway"),
            (None, None) => bail!("use exactly one of --archive or --gateway"),
        }
        ensure!(
            !self.cluster_id.is_empty(),
            "--cluster-id must not be empty"
        );
        ensure!(
            self.slots_per_epoch > 0,
            "--slots-per-epoch must be positive"
        );
        self.epoch_zero_first_slot
            .checked_add(self.slots_per_epoch - 1)
            .context("source slot geometry overflows u64")?;
        Ok(())
    }

    pub fn identity(&self) -> Result<String> {
        self.validate()?;
        match (&self.archive, &self.gateway) {
            (Some(path), None) => {
                let canonical = fs::canonicalize(path)
                    .with_context(|| format!("canonicalize local archive {}", path.display()))?;
                Ok(format!("archive:{}", canonical.display()))
            }
            (None, Some(gateway)) => Ok(format!("gateway:{gateway}")),
            _ => unreachable!("validate accepted exactly one source"),
        }
    }

    fn identity_for(&self, epoch: u64, generation_id: String) -> Result<ArchiveIdentity> {
        let first_slot = epoch
            .checked_mul(self.slots_per_epoch)
            .and_then(|offset| self.epoch_zero_first_slot.checked_add(offset))
            .context("epoch first slot overflows u64")?;
        Ok(ArchiveIdentity {
            cluster_id: self.cluster_id.clone(),
            epoch,
            generation_id,
            first_slot,
            slots_per_epoch: self.slots_per_epoch,
        })
    }
}

pub struct DumpRunConfig {
    pub source: SourceOptions,
    pub epochs: Vec<u64>,
    pub output: PathBuf,
    pub threads: usize,
    pub on_indeterminate: OnIndeterminate,
    pub kind: DumpKind,
    pub target_pubkey: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DumpRunResult {
    pub partial: bool,
}

pub struct PreparedEpoch {
    pub archive: DumpArchive,
    pub source_root: PathBuf,
    pub source_identity: String,
    pub identity: ArchiveIdentity,
    pub source_descriptor_json: String,
}

impl PreparedEpoch {
    pub fn verify_source_unchanged(&self) -> Result<()> {
        match self.archive.source() {
            DumpSource::Local(source) => source.verify_unchanged().map_err(anyhow::Error::from),
            DumpSource::Gateway(source) => source
                .verify_unchanged(self.identity.epoch)
                .map_err(anyhow::Error::from),
        }
    }
}

pub fn run_dump(config: &DumpRunConfig) -> Result<DumpRunResult> {
    ensure!(!config.epochs.is_empty(), "at least one epoch is required");
    ensure!(
        config.threads > 0 && config.threads <= MAX_THREADS,
        "threads must be in 1..={MAX_THREADS}"
    );
    let mut epochs = config.epochs.clone();
    epochs.sort_unstable();
    epochs.dedup();
    let spec = DumpSpec {
        kind: config.kind,
        target_pubkey: config.target_pubkey,
        source: config.source.identity()?,
        on_indeterminate: config.on_indeterminate,
        epochs: epochs.clone(),
    };
    let mut database = DumpDatabase::open_or_create(&config.output, &spec)
        .with_context(|| format!("open dump database {}", config.output.display()))?;

    for epoch in epochs {
        if let Err(error) = scan_epoch(&mut database, config, epoch) {
            let message = format!("{error:#}");
            if let Err(mark_error) = database.fail_epoch(epoch, &message) {
                return Err(error).context(format!(
                    "also failed to record epoch {epoch} failure: {mark_error}"
                ));
            }
            return Err(error);
        }
    }
    let state = database.complete_dump().context("complete dump")?;
    database
        .integrity_check()
        .context("check final SQLite dump")?;
    Ok(DumpRunResult {
        partial: matches!(state, crate::database::DumpState::CompleteWithGaps),
    })
}

pub fn prepare_epoch(source_options: &SourceOptions, epoch: u64) -> Result<PreparedEpoch> {
    source_options.validate()?;
    if let Some(archive_root) = &source_options.archive {
        let resolved_root = resolve_local_epoch_root(archive_root, epoch)?;
        let prefix = source_options
            .local_generation_prefix
            .as_deref()
            .expect("local source validation requires a generation prefix");
        let identity = source_options.identity_for(epoch, format!("{prefix}-epoch-{epoch}"))?;
        let source = PinnedLocalRangeSource::new_anchored(&resolved_root, &LOCAL_OBJECT_SET)
            .context("pin local Compact V2 reader directory")?;
        let options = OpenOptions {
            epoch_first_slot: Some(identity.first_slot),
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_pinned_with_schemas(
            DumpSource::Local(source),
            identity.clone(),
            options,
            source_options.message_schema,
            source_options.metadata_schema,
        )
        .with_context(|| format!("open pinned local epoch {epoch}"))?;
        let source_descriptor_json = source_descriptor_json(&archive, None)?;
        return Ok(PreparedEpoch {
            archive,
            source_root: resolved_root.clone(),
            source_identity: format!(
                "archive:{}#{}#first-slot={}",
                resolved_root.display(),
                identity.generation_id,
                identity.first_slot
            ),
            identity,
            source_descriptor_json,
        });
    }

    let gateway = source_options
        .gateway
        .as_deref()
        .context("gateway source is missing")?;
    let cache = source_options
        .cache
        .as_ref()
        .context("--cache is required with --gateway")?;
    fs::create_dir_all(cache)
        .with_context(|| format!("create cache directory {}", cache.display()))?;
    let http_options = HttpRangeSourceOptions {
        allow_insecure_http: source_options.allow_insecure_http,
        ..HttpRangeSourceOptions::default()
    };
    let http = HttpRangeSource::with_options(
        gateway,
        epoch,
        source_options.bearer_token.as_deref(),
        http_options,
    )
    .context("create HTTP range source")?;
    let objects = discover_gateway_objects(&http, epoch)?;
    let objects = Arc::new(objects);
    let object_set_id = gateway_object_set_id(epoch, &objects);
    let identity = source_options.identity_for(epoch, object_set_id.clone())?;
    let cache_root = cache.join(format!("epoch-{epoch}")).join(&object_set_id);
    fs::create_dir_all(&cache_root)
        .with_context(|| format!("create object-set cache {}", cache_root.display()))?;

    for name in [
        blockzilla_archive_v2::ARCHIVE_V2_BLOCK_INDEX_FILE,
        blockzilla_archive_v2::ARCHIVE_V2_META_FILE,
        blockzilla_archive_v2::ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        if let Some(binding) = objects.get(name) {
            cache_etag_object(&http, &cache_root, name, binding)?;
        }
    }

    let cache_source = PinnedLocalRangeSource::new_anchored(&cache_root, &LOCAL_OBJECT_SET)
        .context("pin gateway cache directory")?;
    let source = DumpSource::Gateway(GatewayDumpSource {
        source: OverlayRangeSource::new(cache_source, http.clone()),
        http,
        objects: Arc::clone(&objects),
    });
    let options = OpenOptions {
        hash_verification: HashVerification::SizesOnly,
        epoch_first_slot: Some(identity.first_slot),
        ..OpenOptions::default()
    };
    let archive = ArchiveReader::open_object_set_with_schemas(
        source,
        identity.clone(),
        &object_set_id,
        options,
        source_options.message_schema,
        source_options.metadata_schema,
    )
    .with_context(|| format!("open strong-ETag object set for epoch {epoch}"))?;
    let source_descriptor_json = source_descriptor_json(&archive, Some(objects.as_ref()))?;
    Ok(PreparedEpoch {
        archive,
        source_root: cache_root,
        source_identity: format!(
            "gateway:{gateway}/epoch-{epoch}@{object_set_id}#first-slot={}",
            identity.first_slot
        ),
        identity,
        source_descriptor_json,
    })
}

fn compact_v2_object_names(epoch: u64) -> Vec<&'static str> {
    let mut names = Vec::with_capacity(LOCAL_OBJECT_SET.len());
    names.extend(COMPACT_V2_REQUIRED_OBJECTS);
    names.extend(COMPACT_V2_OPTIONAL_OBJECTS);
    if epoch == 0 {
        names.push(blockzilla_archive_v2::ARCHIVE_V2_GENESIS_BIN_FILE);
    }
    names
}

fn discover_gateway_objects(
    http: &HttpRangeSource,
    epoch: u64,
) -> Result<BTreeMap<String, HttpObjectIdentity>> {
    let mut objects = BTreeMap::new();
    for name in COMPACT_V2_REQUIRED_OBJECTS {
        let identity = http
            .strong_identity(name)
            .with_context(|| format!("bind required gateway object {name}"))?;
        objects.insert(name.to_owned(), identity);
    }
    for name in compact_v2_object_names(epoch) {
        if objects.contains_key(name) {
            continue;
        }
        match http.strong_identity(name) {
            Ok(identity) => {
                objects.insert(name.to_owned(), identity);
            }
            Err(SourceError::NotFound(_)) => {}
            Err(error) => {
                return Err(error).with_context(|| format!("bind optional gateway object {name}"));
            }
        }
    }
    Ok(objects)
}

fn gateway_object_set_id(epoch: u64, objects: &BTreeMap<String, HttpObjectIdentity>) -> String {
    let mut digest = Sha256::new();
    digest.update(OBJECT_SET_ID_DOMAIN);
    digest.update(epoch.to_le_bytes());
    digest.update((objects.len() as u64).to_le_bytes());
    for (name, identity) in objects {
        digest.update((name.len() as u64).to_le_bytes());
        digest.update(name.as_bytes());
        digest.update(identity.length.to_le_bytes());
        digest.update((identity.strong_etag.len() as u64).to_le_bytes());
        digest.update(identity.strong_etag.as_bytes());
    }
    format!("etag-set-{}", hex_lower(&digest.finalize()))
}

fn source_descriptor_json(
    archive: &DumpArchive,
    source_objects: Option<&BTreeMap<String, HttpObjectIdentity>>,
) -> Result<String> {
    let descriptor = archive
        .archive_descriptor()
        .context("object-set reader did not expose its runtime descriptor")?;
    let source_binding = match &descriptor.source_binding {
        ArchiveSourceBinding::PinnedLocal => {
            let DumpSource::Local(source) = archive.source() else {
                bail!("pinned-local descriptor is not backed by a local pinned source");
            };
            serde_json::json!({
                "kind": "pinned-local",
                "file_identities": source.pinned_object_identities()?,
            })
        }
        ArchiveSourceBinding::StrongEtags { object_set_id } => serde_json::json!({
            "kind": "strong-etags",
            "object_set_id": object_set_id
        }),
    };
    let objects = descriptor
        .objects
        .iter()
        .map(|object| {
            let mut json = serde_json::json!({
                "name": object.name.as_str(),
                "size": object.size,
            });
            if let Some(source_objects) = source_objects {
                let identity = source_objects.get(object.name.as_str()).context(format!(
                    "missing strong identity for object {}",
                    object.name
                ))?;
                if let serde_json::Value::Object(ref mut fields) = json {
                    fields.insert(
                        "strong_etag".into(),
                        serde_json::json!(identity.strong_etag),
                    );
                }
            }
            Ok::<_, anyhow::Error>(json)
        })
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_string_pretty(&serde_json::json!({
        "kind": "compact-v2-runtime-source-descriptor-v1",
        "cluster_id": descriptor.identity.cluster_id.as_str(),
        "epoch": descriptor.identity.epoch,
        "generation_id": descriptor.identity.generation_id.as_str(),
        "first_slot": descriptor.identity.first_slot,
        "slots_per_epoch": descriptor.identity.slots_per_epoch,
        "message_schema": message_schema_name(archive.message_schema()),
        "metadata_schema": metadata_schema_name(archive.metadata_schema()),
        "source_binding": source_binding,
        "objects": objects,
    }))
    .context("serialize runtime source descriptor")
}

fn resolve_local_epoch_root(root: &Path, epoch: u64) -> Result<PathBuf> {
    let direct_index = root.join(blockzilla_archive_v2::ARCHIVE_V2_BLOCK_INDEX_FILE);
    let candidate = if direct_index.is_file() {
        root.to_path_buf()
    } else {
        root.join(format!("epoch-{epoch}"))
    };
    let canonical = fs::canonicalize(&candidate).with_context(|| {
        format!(
            "find local epoch {epoch} at {} or {}",
            root.display(),
            root.join(format!("epoch-{epoch}")).display()
        )
    })?;
    ensure!(
        canonical.is_dir(),
        "{} is not a directory",
        canonical.display()
    );
    Ok(canonical)
}

fn scan_epoch(database: &mut DumpDatabase, config: &DumpRunConfig, epoch: u64) -> Result<()> {
    let prepared = prepare_epoch(&config.source, epoch)?;
    let archive = &prepared.archive;
    ensure!(
        archive.signatures_available(),
        "epoch {epoch} does not publish signatures.bin; the dump cannot be self-contained"
    );
    let filter = archive
        .compile_pubkey_filter([config.target_pubkey])
        .context("compile target pubkey filter")?;
    let binding = crate::database::EpochBinding {
        epoch,
        source_identity: prepared.source_identity.clone(),
        cluster_id: prepared.identity.cluster_id.clone(),
        generation_id: prepared.identity.generation_id.clone(),
        slots_per_epoch: prepared.identity.slots_per_epoch,
        message_schema: message_schema_name(archive.message_schema()).into(),
        metadata_schema: metadata_schema_name(archive.metadata_schema()).into(),
        source_descriptor_json: prepared.source_descriptor_json.clone(),
        block_rows_total: archive.index().rows.len() as u64,
    };
    let mut checkpoint = database
        .begin_epoch(&binding)
        .context("bind epoch checkpoint")?;
    let start = usize::try_from(checkpoint.next_block_row)
        .context("checkpoint block row does not fit this platform")?;
    let end = archive.index().rows.len();
    if start < end {
        let parallel = OrderedParallelBlockConfig {
            decode_workers: config.threads,
            compressed_buffer_count: config.threads.clamp(1, 3),
            max_blocks_per_batch: 1_024,
            uncompressed_batch_budget_bytes: 256 * 1024 * 1024,
            retained_decompressed_bytes_per_worker: (32 * 1024 * 1024)
                .min(MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES / config.threads),
            discard_rewards: true,
            ..OrderedParallelBlockConfig::default()
        };
        archive.process_borrowed_blocks_parallel_ordered(
            start..end,
            parallel,
            |_| Ok::<_, anyhow::Error>(RegistryResolver::new(REGISTRY_RESOLVER_ENTRIES)),
            |resolver, _row_number, block| {
                project_block(
                    archive,
                    &filter,
                    config.kind,
                    config.on_indeterminate,
                    resolver,
                    block,
                )
            },
            |row_number, mut projection| {
                checkpoint.next_block_row =
                    u64::try_from(row_number + 1).context("block row checkpoint overflow")?;
                checkpoint.scanned_blocks = checkpoint.next_block_row;
                checkpoint.scanned_transactions = checkpoint
                    .scanned_transactions
                    .checked_add(projection.scanned_transactions)
                    .context("scanned transaction count overflow")?;
                checkpoint.matched_transactions = checkpoint
                    .matched_transactions
                    .checked_add(projection.matched_transactions)
                    .context("matched transaction count overflow")?;
                checkpoint.indeterminate_transactions = checkpoint
                    .indeterminate_transactions
                    .checked_add(projection.indeterminate_transactions)
                    .context("indeterminate transaction count overflow")?;
                projection.batch.checkpoint = checkpoint;
                database
                    .commit_checkpoint(epoch, &projection.batch)
                    .with_context(|| format!("commit epoch {epoch} block row {row_number}"))
            },
        )?;
    }
    prepared
        .verify_source_unchanged()
        .with_context(|| format!("verify epoch {epoch} source stability"))?;
    database.complete_epoch(epoch).context("complete epoch")?;
    Ok(())
}

struct BlockProjection {
    scanned_transactions: u64,
    matched_transactions: u64,
    indeterminate_transactions: u64,
    batch: CheckpointBatch,
}

fn project_block(
    archive: &DumpArchive,
    filter: &CompiledPubkeyFilter,
    kind: DumpKind,
    policy: OnIndeterminate,
    resolver: &mut RegistryResolver,
    block: BorrowedDecodedBlock<'_>,
) -> Result<BlockProjection> {
    let epoch = archive
        .archive_descriptor()
        .expect("dump readers use object-set admission")
        .identity
        .epoch;
    let slot = block.header().slot;
    let block_id = block.index_row.block_id;
    let mut first_signature_ordinal = block.index_row.first_signature_ordinal;
    let mut projection = BlockProjection {
        scanned_transactions: u64::from(block.tx_count()),
        matched_transactions: 0,
        indeterminate_transactions: 0,
        batch: CheckpointBatch::default(),
    };
    for row in block.tx_rows() {
        let message_bytes = lane_region(
            block.message_bytes(),
            row.message_offset,
            row.message_len,
            "message",
            slot,
            row.tx_index,
        )?;
        let metadata_bytes = lane_region(
            block.metadata_bytes(),
            row.metadata_offset,
            row.metadata_len,
            "metadata",
            slot,
            row.tx_index,
        )?;
        let signature_reference = SignatureReference {
            reader_id: archive.reader_id(),
            first_ordinal: first_signature_ordinal,
            count: row.signature_count,
        };
        first_signature_ordinal = first_signature_ordinal
            .checked_add(u64::from(row.signature_count))
            .context("signature ordinal overflow")?;

        let selected = match kind {
            DumpKind::Program => {
                let message = decode_message_for_selector(archive, &row, message_bytes)?;
                let metadata = decode_metadata_for_selector(archive, &row, metadata_bytes, true)?;
                let outcome = archive.select_program_invocations(
                    filter,
                    &row,
                    message.as_ref(),
                    metadata.as_ref(),
                )?;
                Selected::Program(outcome, message, metadata)
            }
            DumpKind::Token => {
                let needs_metadata = row.flags & ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES != 0;
                let metadata =
                    decode_metadata_for_selector(archive, &row, metadata_bytes, needs_metadata)?;
                let outcome = archive.select_token_balances(filter, &row, metadata.as_ref())?;
                let message = if matches!(outcome, SelectorOutcome::Match(_)) {
                    decode_message_for_selector(archive, &row, message_bytes)?
                } else {
                    None
                };
                Selected::Token(outcome, message, metadata)
            }
        };

        match selected.outcome() {
            SelectorOutcome::NoMatch => {}
            SelectorOutcome::Indeterminate(reason) => {
                projection.indeterminate_transactions += 1;
                handle_indeterminate(
                    policy,
                    &mut projection.batch,
                    epoch,
                    slot,
                    row.tx_index,
                    reason,
                )?;
            }
            SelectorOutcome::Match(_) => {
                let (message, metadata) = selected.decoded_parts().ok_or_else(|| {
                    anyhow!(
                        "selector matched epoch {epoch} slot {slot} transaction {}, but decoded source bytes are unavailable",
                        row.tx_index
                    )
                })?;
                let signatures = archive
                    .read_transaction_signatures(signature_reference)
                    .with_context(|| {
                        format!(
                            "read signatures for slot {slot} transaction {}",
                            row.tx_index
                        )
                    })?;
                projection.batch.transactions.push(MatchRecord {
                    epoch,
                    slot,
                    block_id,
                    tx_index: row.tx_index,
                    source_flags: row.flags,
                    first_signature_ordinal: signature_reference.first_ordinal,
                    signatures,
                    message_state: if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
                        MessageState::RawFallback
                    } else {
                        MessageState::Decoded
                    },
                    message_bytes: message_bytes.to_vec(),
                    metadata_state: if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
                        MetadataState::Absent
                    } else if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                        MetadataState::RawFallback
                    } else {
                        MetadataState::Decoded
                    },
                    metadata_wincode: (row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0)
                        .then(|| metadata_bytes.to_vec()),
                });
                append_transaction_accounts(
                    archive,
                    resolver,
                    TransactionCoordinates {
                        epoch,
                        slot,
                        tx_index: row.tx_index,
                    },
                    message,
                    metadata,
                    &mut projection.batch.transaction_accounts,
                )?;
                match selected {
                    Selected::Program(SelectorOutcome::Match(summary), _, _) => {
                        projection.batch.program_matches.push(ProgramMatch {
                            epoch,
                            slot,
                            tx_index: row.tx_index,
                            direct_count: summary.direct_count,
                            cpi_count: summary.cpi_count,
                        });
                    }
                    Selected::Token(SelectorOutcome::Match(summary), _, _) => {
                        projection.batch.token_matches.push(TokenMatch {
                            epoch,
                            slot,
                            tx_index: row.tx_index,
                            pre_count: summary.pre_count,
                            post_count: summary.post_count,
                        });
                        append_token_balances(
                            archive,
                            resolver,
                            epoch,
                            slot,
                            row.tx_index,
                            metadata,
                            &mut projection.batch.token_balances,
                        )?;
                    }
                    _ => unreachable!("matched outcome was checked above"),
                }
                projection.matched_transactions += 1;
            }
        }
    }
    Ok(projection)
}

enum Selected {
    Program(
        SelectorOutcome<blockzilla_compact_v2_reader::ProgramInvocationMatch>,
        Option<ArchiveV2HotMessagePayload>,
        Option<CompactMetaV1>,
    ),
    Token(
        SelectorOutcome<blockzilla_compact_v2_reader::TokenBalanceMatch>,
        Option<ArchiveV2HotMessagePayload>,
        Option<CompactMetaV1>,
    ),
}

impl Selected {
    fn outcome(&self) -> SelectorOutcome<()> {
        match self {
            Self::Program(outcome, _, _) => erase_outcome(*outcome),
            Self::Token(outcome, _, _) => erase_outcome(*outcome),
        }
    }

    fn decoded_parts(&self) -> Option<(&ArchiveV2HotMessagePayload, &CompactMetaV1)> {
        match self {
            Self::Program(_, Some(message), Some(metadata))
            | Self::Token(_, Some(message), Some(metadata)) => Some((message, metadata)),
            _ => None,
        }
    }
}

fn erase_outcome<T: Copy>(outcome: SelectorOutcome<T>) -> SelectorOutcome<()> {
    match outcome {
        SelectorOutcome::Match(_) => SelectorOutcome::Match(()),
        SelectorOutcome::NoMatch => SelectorOutcome::NoMatch,
        SelectorOutcome::Indeterminate(reason) => SelectorOutcome::Indeterminate(reason),
    }
}

fn decode_message_for_selector(
    archive: &DumpArchive,
    row: &ArchiveV2HotTxRow,
    bytes: &[u8],
) -> Result<Option<ArchiveV2HotMessagePayload>> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 || bytes.is_empty() {
        return Ok(None);
    }
    archive
        .decode_message(bytes)
        .map(Some)
        .map_err(anyhow::Error::from)
}

fn decode_metadata_for_selector(
    archive: &DumpArchive,
    row: &ArchiveV2HotTxRow,
    bytes: &[u8],
    read: bool,
) -> Result<Option<CompactMetaV1>> {
    if !read
        || row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0
        || row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0
        || bytes.is_empty()
    {
        return Ok(None);
    }
    archive
        .decode_metadata(bytes)
        .map(Some)
        .map_err(anyhow::Error::from)
}

fn handle_indeterminate(
    policy: OnIndeterminate,
    batch: &mut CheckpointBatch,
    epoch: u64,
    slot: u64,
    tx_index: u32,
    reason: SelectorIndeterminateReason,
) -> Result<()> {
    let reason_text = indeterminate_reason(reason);
    match policy {
        OnIndeterminate::Fail => bail!(
            "indeterminate selector result at epoch {epoch}, slot {slot}, transaction {tx_index}: {reason_text}"
        ),
        OnIndeterminate::Record => batch.coverage_issues.push(CoverageIssue {
            epoch,
            slot,
            tx_index,
            reason: reason_text.into(),
            detail: None,
        }),
        OnIndeterminate::Skip => {}
    }
    Ok(())
}

fn append_transaction_accounts(
    archive: &DumpArchive,
    resolver: &mut RegistryResolver,
    transaction: TransactionCoordinates,
    message: &ArchiveV2HotMessagePayload,
    metadata: &CompactMetaV1,
    output: &mut Vec<TransactionAccountRecord>,
) -> Result<()> {
    let (header, static_keys) = message_header_and_keys(message);
    let required = usize::from(header.num_required_signatures);
    ensure!(
        required <= static_keys.len(),
        "message signer count exceeds static account count"
    );
    let readonly_signed = usize::from(header.num_readonly_signed_accounts);
    let readonly_unsigned = usize::from(header.num_readonly_unsigned_accounts);
    ensure!(
        readonly_signed <= required,
        "readonly signer count exceeds signer count"
    );
    ensure!(
        readonly_unsigned <= static_keys.len() - required,
        "readonly unsigned count exceeds unsigned account count"
    );
    let writable_signed = required - readonly_signed;
    let writable_unsigned_end = static_keys.len() - readonly_unsigned;
    for (index, reference) in static_keys.iter().enumerate() {
        output.push(TransactionAccountRecord {
            epoch: transaction.epoch,
            slot: transaction.slot,
            tx_index: transaction.tx_index,
            account_index: u32::try_from(index).context("static account index overflow")?,
            pubkey: resolver.resolve(archive, reference)?,
            source: TransactionAccountSource::Static,
            is_signer: index < required,
            is_writable: if index < required {
                index < writable_signed
            } else {
                index < writable_unsigned_end
            },
        });
    }
    let mut next = static_keys.len();
    for (source, writable, keys) in [
        (
            TransactionAccountSource::LoadedWritable,
            true,
            metadata.loaded_writable_addresses.as_slice(),
        ),
        (
            TransactionAccountSource::LoadedReadonly,
            false,
            metadata.loaded_readonly_addresses.as_slice(),
        ),
    ] {
        for reference in keys {
            output.push(TransactionAccountRecord {
                epoch: transaction.epoch,
                slot: transaction.slot,
                tx_index: transaction.tx_index,
                account_index: u32::try_from(next).context("loaded account index overflow")?,
                pubkey: resolver.resolve(archive, reference)?,
                source,
                is_signer: false,
                is_writable: writable,
            });
            next = next.checked_add(1).context("account index overflow")?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct TransactionCoordinates {
    epoch: u64,
    slot: u64,
    tx_index: u32,
}

fn append_token_balances(
    archive: &DumpArchive,
    resolver: &mut RegistryResolver,
    epoch: u64,
    slot: u64,
    tx_index: u32,
    metadata: &CompactMetaV1,
    output: &mut Vec<TokenBalanceRecord>,
) -> Result<()> {
    for (side, balances) in [
        (
            TokenBalanceSide::Pre,
            metadata.pre_token_balances.as_slice(),
        ),
        (
            TokenBalanceSide::Post,
            metadata.post_token_balances.as_slice(),
        ),
    ] {
        for (index, balance) in balances.iter().enumerate() {
            let mint = balance
                .mint
                .as_ref()
                .ok_or_else(|| anyhow!("matched token transaction has a balance with no mint"))?;
            output.push(TokenBalanceRecord {
                epoch,
                slot,
                tx_index,
                side,
                balance_index: u32::try_from(index).context("token balance index overflow")?,
                account_index: balance.account_index,
                mint: resolver.resolve(archive, mint)?,
                owner: balance
                    .owner
                    .as_ref()
                    .map(|reference| resolver.resolve(archive, reference))
                    .transpose()?,
                token_program: balance
                    .program_id
                    .as_ref()
                    .map(|reference| resolver.resolve(archive, reference))
                    .transpose()?,
                amount: balance.amount,
                decimals: balance.decimals,
            });
        }
    }
    Ok(())
}

fn message_header_and_keys(
    message: &ArchiveV2HotMessagePayload,
) -> (&CompactMessageHeader, &[CompactPubkey]) {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => (&message.header, &message.account_keys),
        ArchiveV2HotMessagePayload::V0(message) => (&message.header, &message.account_keys),
        ArchiveV2HotMessagePayload::V1(message) => (&message.header, &message.account_keys),
    }
}

fn lane_region<'a>(
    lane: &'a [u8],
    offset: u32,
    length: u32,
    name: &str,
    slot: u64,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let end = start
        .checked_add(length as usize)
        .with_context(|| format!("{name} range overflow at slot {slot}, transaction {tx_index}"))?;
    lane.get(start..end).ok_or_else(|| {
        anyhow!(
            "{name} range {start}..{end} is outside {} bytes at slot {slot}, transaction {tx_index}",
            lane.len()
        )
    })
}

struct RegistryResolver {
    capacity: usize,
    values: HashMap<u32, [u8; 32]>,
    order: VecDeque<u32>,
}

impl RegistryResolver {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            values: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
        }
    }

    fn resolve(&mut self, archive: &DumpArchive, reference: &CompactPubkey) -> Result<[u8; 32]> {
        let CompactPubkey::Id(id) = reference else {
            return archive
                .resolve_pubkey(reference)
                .map_err(anyhow::Error::from);
        };
        if let Some(value) = self.values.get(id) {
            return Ok(*value);
        }
        let value = archive.resolve_pubkey(reference)?;
        if self.capacity != 0 {
            if self.values.len() == self.capacity
                && let Some(evicted) = self.order.pop_front()
            {
                self.values.remove(&evicted);
            }
            self.values.insert(*id, value);
            self.order.push_back(*id);
        }
        Ok(value)
    }
}

fn cache_etag_object(
    source: &HttpRangeSource,
    cache_root: &Path,
    name: &str,
    binding: &HttpObjectIdentity,
) -> Result<()> {
    let destination = cache_root.join(name);
    if cached_object_matches(&destination, binding)? {
        return Ok(());
    }
    let _lock = CacheLock::acquire(&destination)?;
    if cached_object_matches(&destination, binding)? {
        return Ok(());
    }
    let replace_existing = destination.exists();
    let temporary = temporary_cache_path(&destination)?;
    let result = (|| {
        let mut file = FsOpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| format!("create cache staging file {}", temporary.display()))?;
        let mut offset = 0u64;
        while offset < binding.length {
            let length =
                usize::try_from((binding.length - offset).min(CACHE_DOWNLOAD_CHUNK as u64))
                    .context("cache download range exceeds this platform")?;
            let bytes = source
                .read_range(name, offset, length)
                .with_context(|| format!("download {name} at byte {offset}"))?;
            ensure!(
                bytes.len() == length,
                "gateway returned {} bytes for {}, expected {length}",
                bytes.len(),
                name
            );
            file.write_all(&bytes)
                .with_context(|| format!("write cache file {}", temporary.display()))?;
            offset += length as u64;
        }
        ensure!(
            source.strong_identity(name)? == *binding,
            "gateway object {name} changed while it was cached"
        );
        file.sync_all()
            .with_context(|| format!("sync cache file {}", temporary.display()))?;
        drop(file);
        publish_verified_cache_file(&temporary, &destination, replace_existing, binding)?;
        publish_cache_bytes(
            &cache_binding_path(&destination)?,
            &cache_binding_bytes(binding)?,
        )?;
        sync_directory(cache_root)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn publish_cache_bytes(destination: &Path, bytes: &[u8]) -> Result<()> {
    if fs::read(destination).ok().as_deref() == Some(bytes) {
        return Ok(());
    }
    let _lock = CacheLock::acquire(destination)?;
    if fs::read(destination).ok().as_deref() == Some(bytes) {
        return Ok(());
    }
    let replace_existing = destination.exists();
    let parent = destination
        .parent()
        .context("cache destination has no parent directory")?;
    let temporary = temporary_cache_path(destination)?;
    let result = (|| {
        let mut file = FsOpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .with_context(|| format!("create cache staging file {}", temporary.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("write cache staging file {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("sync cache staging file {}", temporary.display()))?;
        drop(file);
        if replace_existing {
            fs::rename(&temporary, destination).with_context(|| {
                format!(
                    "replace cache file {} with {}",
                    destination.display(),
                    temporary.display()
                )
            })?;
        } else {
            match fs::hard_link(&temporary, destination) {
                Ok(()) => fs::remove_file(&temporary).with_context(|| {
                    format!("remove cache staging link {}", temporary.display())
                })?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    ensure!(
                        fs::read(destination).ok().as_deref() == Some(bytes),
                        "cache publication conflict at {}",
                        destination.display()
                    );
                    fs::remove_file(&temporary).with_context(|| {
                        format!("remove cache staging file {}", temporary.display())
                    })?;
                }
                Err(error) => return Err(error).context("publish cache file without replacement"),
            }
        }
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn cache_binding_path(destination: &Path) -> Result<PathBuf> {
    let name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .context("cache object name is not valid UTF-8")?;
    Ok(destination.with_file_name(format!(".{name}.etag.json")))
}

fn cache_binding_bytes(binding: &HttpObjectIdentity) -> Result<Vec<u8>> {
    serde_json::to_vec(&serde_json::json!({
        "length": binding.length,
        "strong_etag": binding.strong_etag.as_str(),
    }))
    .context("serialize cache ETag binding")
}

fn cached_object_matches(path: &Path, binding: &HttpObjectIdentity) -> Result<bool> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("stat cache file {}", path.display()));
        }
    };
    if !metadata.is_file() || metadata.len() != binding.length {
        return Ok(false);
    }
    let binding_path = cache_binding_path(path)?;
    Ok(fs::read(binding_path).ok().as_deref() == Some(cache_binding_bytes(binding)?.as_slice()))
}

fn publish_verified_cache_file(
    temporary: &Path,
    destination: &Path,
    replace_existing: bool,
    binding: &HttpObjectIdentity,
) -> Result<()> {
    if replace_existing {
        fs::rename(temporary, destination).with_context(|| {
            format!(
                "replace invalid cache file {} with {}",
                destination.display(),
                temporary.display()
            )
        })?;
        return Ok(());
    }
    match fs::hard_link(temporary, destination) {
        Ok(()) => fs::remove_file(temporary)
            .with_context(|| format!("remove cache staging link {}", temporary.display())),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure!(
                cached_object_matches(destination, binding)?,
                "cache publication conflict at {}",
                destination.display()
            );
            fs::remove_file(temporary)
                .with_context(|| format!("remove cache staging file {}", temporary.display()))
        }
        Err(error) => Err(error).context("publish cache file without replacement"),
    }
}

struct CacheLock {
    path: PathBuf,
}

impl CacheLock {
    fn acquire(destination: &Path) -> Result<Self> {
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .context("cache object name is not valid UTF-8")?;
        let path = destination.with_file_name(format!(".{name}.lock"));
        FsOpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| {
                format!(
                    "acquire cache publication lock {}; another process may be downloading this object",
                    path.display()
                )
            })?;
        Ok(Self { path })
    }
}

impl Drop for CacheLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn temporary_cache_path(destination: &Path) -> Result<PathBuf> {
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .context("cache object name is not valid UTF-8")?;
    let sequence = CACHE_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    Ok(destination.with_file_name(format!(".{name}.{}.{}.tmp", std::process::id(), sequence)))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open cache directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync cache directory {}", path.display()))
}

fn message_schema_name(schema: CompactV2MessageSchema) -> &'static str {
    match schema {
        CompactV2MessageSchema::Current => "current",
        CompactV2MessageSchema::May24PreUnknownFallbacks => "may24-pre-unknown-fallbacks",
    }
}

fn metadata_schema_name(schema: CompactV2MetadataSchema) -> &'static str {
    match schema {
        CompactV2MetadataSchema::CurrentTypedError => "current-typed-error",
        CompactV2MetadataSchema::LegacyRawError => "legacy-raw-error",
    }
}

fn indeterminate_reason(reason: SelectorIndeterminateReason) -> &'static str {
    match reason {
        SelectorIndeterminateReason::RawTransactionFallback => "raw-transaction-fallback",
        SelectorIndeterminateReason::RawMetadataFallback => "raw-metadata-fallback",
        SelectorIndeterminateReason::MessageUnavailable => "message-unavailable",
        SelectorIndeterminateReason::MetadataUnavailable => "metadata-unavailable",
        SelectorIndeterminateReason::InvalidRegistryReference => "invalid-registry-reference",
        SelectorIndeterminateReason::InvalidAccountReference => "invalid-account-reference",
        SelectorIndeterminateReason::TokenMintUnavailable => "token-mint-unavailable",
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from_digit(u32::from(byte >> 4), 16).expect("hex nibble"));
        output.push(char::from_digit(u32::from(byte & 0x0f), 16).expect("hex nibble"));
    }
    output
}
