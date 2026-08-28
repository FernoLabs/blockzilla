use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File, OpenOptions as FsOpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use blockzilla_format::{
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotMessagePayload, ArchiveV2HotTxRow,
    CompactMessageHeader, CompactMetaV1, CompactPubkey,
};
use blockzilla_read_sdk::{
    ArchiveReader, BorrowedDecodedBlock, CompactV2MessageSchema, CompactV2MetadataSchema,
    CompiledPubkeyFilter, HashVerification, HttpRangeSource, HttpRangeSourceOptions,
    MAX_ORDERED_PARALLEL_RETAINED_DECOMPRESSED_BYTES, OpenOptions, OrderedParallelBlockConfig,
    OverlayRangeSource, PinnedLocalRangeSource, RangeSource, SelectorIndeterminateReason,
    SelectorOutcome, SignatureReference, SourceResult,
    manifest::{
        BLOCK_INDEX_FILE, GENERATION_MANIFEST_FILE, GenerationFile, GenerationManifest, META_FILE,
        REGISTRY_FILE,
    },
};
use sha2::{Digest, Sha256};

use crate::database::{
    CheckpointBatch, CoverageIssue, DumpDatabase, DumpKind, DumpSpec, MatchRecord, MessageState,
    MetadataState, OnIndeterminate, ProgramMatch, TokenBalanceRecord, TokenBalanceSide, TokenMatch,
    TransactionAccountRecord, TransactionAccountSource,
};

const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
const CACHE_DOWNLOAD_CHUNK: usize = 8 * 1024 * 1024;
const REGISTRY_RESOLVER_ENTRIES: usize = 16_384;
const MAX_THREADS: usize = blockzilla_read_sdk::MAX_ORDERED_PARALLEL_DECODE_WORKERS;
static CACHE_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub enum DumpSource {
    Local(PinnedLocalRangeSource),
    Gateway(OverlayRangeSource<PinnedLocalRangeSource, HttpRangeSource>),
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
                Ok(())
            }
            (None, Some(_)) => {
                ensure!(self.cache.is_some(), "--cache is required with --gateway");
                Ok(())
            }
            (Some(_), Some(_)) => bail!("use exactly one of --archive or --gateway"),
            (None, None) => bail!("use exactly one of --archive or --gateway"),
        }
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
    pub manifest_json: String,
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
        let source = PinnedLocalRangeSource::new(&resolved_root);
        let manifest_bytes = source
            .read_all_bounded(GENERATION_MANIFEST_FILE, MAX_MANIFEST_BYTES)
            .context("read local generation manifest")?;
        let manifest = GenerationManifest::parse(&manifest_bytes)
            .context("validate local generation manifest")?;
        ensure!(
            manifest.epoch == epoch,
            "local archive is epoch {}, expected {epoch}",
            manifest.epoch
        );
        let manifest_json =
            String::from_utf8(manifest_bytes).context("generation manifest is not valid UTF-8")?;
        let options = OpenOptions {
            hash_verification: HashVerification::ControlFiles,
            ..OpenOptions::default()
        };
        let archive = ArchiveReader::open_with_options(DumpSource::Local(source), options)
            .with_context(|| format!("open local published epoch {epoch}"))?;
        return Ok(PreparedEpoch {
            archive,
            source_root: resolved_root.clone(),
            source_identity: format!("archive:{}", resolved_root.display()),
            manifest_json,
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
    let manifest_bytes = http
        .read_all_bounded(GENERATION_MANIFEST_FILE, MAX_MANIFEST_BYTES)
        .context("download generation manifest")?;
    let manifest =
        GenerationManifest::parse(&manifest_bytes).context("validate generation manifest")?;
    ensure!(
        manifest.epoch == epoch,
        "gateway returned epoch {}, expected {epoch}",
        manifest.epoch
    );
    let manifest_json = String::from_utf8(manifest_bytes.clone())
        .context("generation manifest is not valid UTF-8")?;
    let cache_root = cache
        .join(format!("epoch-{epoch}"))
        .join(&manifest.generation_digest);
    fs::create_dir_all(&cache_root)
        .with_context(|| format!("create generation cache {}", cache_root.display()))?;
    publish_cache_bytes(&cache_root.join(GENERATION_MANIFEST_FILE), &manifest_bytes)?;

    for name in [
        BLOCK_INDEX_FILE,
        META_FILE,
        REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        let file = manifest
            .required_file(name)
            .with_context(|| format!("network publication must bind cache control file {name}"))?;
        cache_manifest_object(&http, &cache_root, file)?;
    }
    for name in [
        blockzilla_read_sdk::COMPACT_V2_CURRENT_MESSAGE_SCHEMA_MARKER_FILE,
        blockzilla_read_sdk::COMPACT_V2_MAY24_MESSAGE_SCHEMA_MARKER_FILE,
        blockzilla_read_sdk::COMPACT_V2_LEGACY_METADATA_SCHEMA_MARKER_FILE,
    ] {
        if let Some(file) = manifest.file(name) {
            cache_manifest_object(&http, &cache_root, file)?;
        }
    }

    let source = DumpSource::Gateway(OverlayRangeSource::new(
        PinnedLocalRangeSource::new(&cache_root),
        http,
    ));
    let options = OpenOptions {
        hash_verification: HashVerification::ControlFiles,
        ..OpenOptions::default()
    };
    let archive = ArchiveReader::open_with_options(source, options)
        .with_context(|| format!("open published epoch {epoch}"))?;
    ensure!(
        archive.manifest().generation_digest == manifest.generation_digest,
        "opened archive generation differs from the downloaded manifest"
    );
    Ok(PreparedEpoch {
        archive,
        source_root: cache_root,
        source_identity: format!(
            "gateway:{gateway}/epoch-{epoch}@{}",
            manifest.generation_digest
        ),
        manifest_json,
    })
}

fn resolve_local_epoch_root(root: &Path, epoch: u64) -> Result<PathBuf> {
    let direct_manifest = root.join(GENERATION_MANIFEST_FILE);
    let candidate = if direct_manifest.is_file() {
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
        source_identity: prepared.source_identity,
        cluster_id: archive.manifest().cluster_id.clone(),
        generation_id: archive.manifest().generation_id.clone(),
        generation_digest: archive.binding().generation_digest,
        slots_per_epoch: archive.manifest().slots_per_epoch,
        message_schema: message_schema_name(archive.message_schema()).into(),
        metadata_schema: metadata_schema_name(archive.metadata_schema()).into(),
        manifest_json: prepared.manifest_json,
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
    let epoch = archive.manifest().epoch;
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
            generation_digest: archive.binding().generation_digest,
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
        SelectorOutcome<blockzilla_read_sdk::ProgramInvocationMatch>,
        Option<ArchiveV2HotMessagePayload>,
        Option<CompactMetaV1>,
    ),
    Token(
        SelectorOutcome<blockzilla_read_sdk::TokenBalanceMatch>,
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

fn cache_manifest_object(
    source: &HttpRangeSource,
    cache_root: &Path,
    binding: &GenerationFile,
) -> Result<()> {
    let destination = cache_root.join(&binding.name);
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
        let mut hasher = Sha256::new();
        let mut offset = 0u64;
        while offset < binding.size {
            let length = usize::try_from((binding.size - offset).min(CACHE_DOWNLOAD_CHUNK as u64))
                .context("cache download range exceeds this platform")?;
            let bytes = source
                .read_range(&binding.name, offset, length)
                .with_context(|| format!("download {} at byte {offset}", binding.name))?;
            ensure!(
                bytes.len() == length,
                "gateway returned {} bytes for {}, expected {length}",
                bytes.len(),
                binding.name
            );
            file.write_all(&bytes)
                .with_context(|| format!("write cache file {}", temporary.display()))?;
            hasher.update(&bytes);
            offset += length as u64;
        }
        let digest = hex_lower(&hasher.finalize());
        ensure!(
            digest == binding.sha256,
            "downloaded {} SHA-256 is {digest}, expected {}",
            binding.name,
            binding.sha256
        );
        file.sync_all()
            .with_context(|| format!("sync cache file {}", temporary.display()))?;
        drop(file);
        publish_verified_cache_file(&temporary, &destination, replace_existing, binding)?;
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

fn cached_object_matches(path: &Path, binding: &GenerationFile) -> Result<bool> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("stat cache file {}", path.display()));
        }
    };
    if !metadata.is_file() || metadata.len() != binding.size {
        return Ok(false);
    }
    Ok(sha256_file(path)? == binding.sha256)
}

fn publish_verified_cache_file(
    temporary: &Path,
    destination: &Path,
    replace_existing: bool,
    binding: &GenerationFile,
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

fn sha256_file(path: &Path) -> Result<String> {
    use std::io::Read as _;

    let mut file =
        File::open(path).with_context(|| format!("open cache file {}", path.display()))?;
    let mut buffer = vec![0u8; CACHE_DOWNLOAD_CHUNK];
    let mut hasher = Sha256::new();
    loop {
        let count = file
            .read(&mut buffer)
            .with_context(|| format!("hash cache file {}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex_lower(&hasher.finalize()))
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
