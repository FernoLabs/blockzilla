//! Convert an Archive V2 generation into Index Archive planes.
//!
//! This is the offline column transform described in §6 of
//! `docs/design/blockzilla-index-archive.md`. It reads a published V2
//! generation and fans each block out into the ledger and runtime planes,
//! paying the metadata decode exactly once so that no later reader has to.
//!
//! Typed Compact V2 instructions are reconstructed into exact Solana message
//! bytes. Ambiguous historical vote encodings are accepted only when exactly
//! one complete message candidate verifies against the fee-payer signature.
//! The converter fails closed when exact bytes cannot be proved.
//!
//! One page per block. Page sizing is a §7 measurement and belongs to the
//! container work, not here.

mod effect_chunks;
mod outcome_error;
mod retained_sidecars;

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, Read, Write},
    os::unix::fs::FileExt,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_index_archive_convert::source_v2::{
    CompactV2MessageSchema, CompactV2MetadataSchema, LoadedAddressCounts,
    MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS, PinnedPubkeyResolver, ResolvedAddressTableLookup,
    SignedInstructionCandidates, SignedMessageCandidates, SignedMessageVersion,
    SignedTransactionConfig, VoteHashRegistry, decode_message_with_schema, decode_metadata,
    reconstruct_instruction_data_candidates, select_signed_message_candidate,
    validate_v0_loaded_address_counts,
};
use blockzilla_index_archive_convert::source_v2_sidecars::{
    BlockhashResolver, PreviousBlockhashTail, PreviousBlockhashTailSchema,
    parse_previous_blockhash_tail,
};
use blockzilla_index_archive_convert::{
    candidate::validate_complete_candidate,
    container::{HeaderedWriter, copy_file_payload, copy_file_payload_with_suffix, write_payload},
    derived_indexes::{DerivedIndexBuildOptions, build_all_derived_indexes},
    pipeline::{
        OrderedTask, PipelineConfig, run_inline_ordered_encoding_stage, run_ordered_encoding_stage,
    },
    transaction_view::{PreparedTransactionArena, TransactionArenaEncoder},
};

/// zstd level for page payloads. Provisional -- §7 measurement.
const ZSTD_LEVEL: i32 = 3;
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_GENESIS_BIN_FILE, ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX,
    ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_LOGS,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA,
    ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ArchiveV2HotInstruction, ArchiveV2HotInstructionData, ArchiveV2HotMessagePayload,
    CompactLogStream, CompactPubkey, CompactTokenBalance, FileBackedKeyIndex, KeyIndex,
    OwnedCompactRecentBlockhash, PubkeyResolver, read_archive_v2_hot_block_index_file, render_logs,
    wincode_leb128_config,
};

const KNOWN_SOURCE_TX_FLAGS: u32 = (1 << 11) - 1;

/// Coarse wall-clock and summed work timings for converter profiling.
///
/// Wall-clock fields can be added to estimate end-to-end time. Summed worker
/// fields can be larger than wall time because workers run at the same time.
#[derive(Debug, Default, Serialize)]
struct PhaseTimings {
    source_admission_ms: u64,
    source_shape_ms: u64,
    retained_sidecars_ms: u64,
    dictionary_admission_ms: u64,
    block_pipeline_wall_ms: u64,
    source_read_ms: u64,
    source_decode_project_ms: u64,
    source_outer_decode_sum_ms: u64,
    source_projection_sum_ms: u64,
    borrowed_to_owned_sum_ms: u64,
    source_message_decode_sum_ms: u64,
    signed_message_proof_sum_ms: u64,
    source_metadata_decode_sum_ms: u64,
    /// Source-only shape validation, excluding inline-log public-key discovery.
    source_validation_sum_ms: u64,
    /// Typed inspection used to extract and validate Compact log pubkeys.
    source_inline_log_pubkey_discovery_sum_ms: u64,
    source_wait_for_free_buffer_ms: u64,
    source_wait_for_ready_batch_ms: u64,
    ordered_commit_sum_ms: u64,
    transaction_transform_sum_ms: u64,
    ordered_id_hash_assignment_sum_ms: u64,
    ordered_raw_pubkey_assignment_sum_ms: u64,
    ordered_hash_assignment_sum_ms: u64,
    ordered_serial_preparation_sum_ms: u64,
    ordered_resolution_validation_sum_ms: u64,
    pure_resolved_conversion_sum_ms: u64,
    stage3_pure_resolved_conversion_sum_ms: u64,
    effect_worker_prepare_sum_ms: u64,
    effect_final_append_sum_ms: u64,
    effect_prepare_and_write_sum_ms: u64,
    transaction_page_worker_sum_ms: u64,
    transaction_page_final_commit_sum_ms: u64,
    stage3_worker_effect_and_arena_sum_ms: u64,
    final_append_and_offset_commit_sum_ms: u64,
    /// Worker preparation plus the final ordered transaction-page write.
    transaction_page_prepare_and_write_sum_ms: u64,
    catalog_commit_sum_ms: u64,
    finalize_planes_and_sidecars_ms: u64,
    content_binding_ms: u64,
    derived_indexes_ms: u64,
    candidate_validation_ms: u64,
    conversion_before_report_ms: u64,
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn emit_phase(phase: &str, state: &str, elapsed: Duration) {
    eprintln!(
        "{{\"kind\":\"index-archive-convert-phase\",\"phase\":\"{phase}\",\"state\":\"{state}\",\"elapsed_ms\":{}}}",
        duration_millis(elapsed)
    );
}

struct ProgressReporter {
    started: Instant,
    last_emit: Instant,
    blocks: u64,
    source_compressed_bytes: u64,
    total_blocks: u64,
    total_source_compressed_bytes: u64,
}

impl ProgressReporter {
    fn new(total_blocks: u64, total_source_compressed_bytes: u64) -> Self {
        let now = Instant::now();
        Self {
            started: now,
            last_emit: now,
            blocks: 0,
            source_compressed_bytes: 0,
            total_blocks,
            total_source_compressed_bytes,
        }
    }

    fn record(&mut self, source_compressed_bytes: u64, transactions: u64, force: bool) {
        self.blocks = self.blocks.saturating_add(1);
        self.source_compressed_bytes = self
            .source_compressed_bytes
            .saturating_add(source_compressed_bytes);
        let now = Instant::now();
        if !force && now.duration_since(self.last_emit) < Duration::from_secs(10) {
            return;
        }
        let elapsed = now.duration_since(self.started).as_secs_f64();
        let mib_per_second = if elapsed > 0.0 {
            self.source_compressed_bytes as f64 / (1024.0 * 1024.0) / elapsed
        } else {
            0.0
        };
        eprintln!(
            "{{\"kind\":\"index-archive-convert-progress\",\"blocks\":{},\"total_blocks\":{},\"transactions\":{},\"source_compressed_bytes\":{},\"total_source_compressed_bytes\":{},\"elapsed_seconds\":{elapsed:.3},\"mib_per_second\":{mib_per_second:.3}}}",
            self.blocks,
            self.total_blocks,
            transactions,
            self.source_compressed_bytes,
            self.total_source_compressed_bytes,
        );
        self.last_emit = now;
    }
}
use blockzilla_index_archive_format::{
    ArchiveId, FILE_HEADER_LEN, FORMAT_ID, FORMAT_MAJOR, FileClass, FileEncoding, LAYOUT,
    catalog::blocks as catalog_blocks,
    dictionary::{account_flags, blockhashes, pubkeys},
    indexes as target_indexes,
    ledger::transactions as transactions_codec,
    runtime::{
        balances as balances_plane_codec, block_rewards as block_rewards_codec, inner_instructions,
        logs as logs_codec, outcomes as outcomes_codec, rewards as rewards_codec,
        token_balances as token_balances_codec,
    },
};
use blockzilla_read_sdk::{
    ArchiveReader, BorrowedDecodedBlock, HashVerification, MAX_ORDERED_PARALLEL_DECODE_WORKERS,
    OpenOptions, OrderedParallelBlockConfig, PinnedLocalRangeSource, RangeSource,
    manifest::{GENERATION_MANIFEST_FILE, TrustedGenerationIdentity},
    select_compact_v2_message_schema,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use solana_pubkey::Pubkey;
use solana_signature::Signature;

/// Byte counts and populations for one converted generation.
#[derive(Debug, Default, Serialize)]
struct Report {
    /// Common identity in every headered target object.
    archive_id: String,
    source_published: bool,
    source_generation_digest: Option<String>,
    epoch: u64,
    slots_per_epoch: u64,
    source_profile: &'static str,
    metadata_source_profile: &'static str,
    /// This converter writes a complete physical candidate. Publication still
    /// requires the typed manifest and the semantic, chain, and finality gates.
    output_status: &'static str,
    missing_required_objects: Vec<&'static str>,
    physical_layout_valid: bool,
    required_objects: u64,
    derived_index_workers: usize,
    derived_index_sort_memory_bytes: usize,
    derived_index_sort_memory_per_builder_bytes: usize,
    account_index_postings: u64,
    account_index_pages: u64,
    account_index_continuation_pages: u64,
    account_index_max_postings_per_page: usize,
    account_index_peak_page_postings: usize,
    program_index_postings: u64,
    selector_index_postings: u64,
    /// This limit covers queued block work and ordered commit scratch. Source
    /// sidecars use bounded frame streaming, but dictionaries and resolver maps
    /// are not yet part of one process-wide cap.
    pipeline_memory_limit_bytes: usize,
    process_memory_is_strictly_bounded: bool,
    fixture_previous_blockhash: Option<String>,
    fixture_previous_slot: Option<u64>,
    blocks: u64,
    transactions: u64,
    top_level_instructions: u64,
    inner_instructions: u64,
    account_references: u64,
    /// Transactions whose message could not be decoded, carried as raw by V2.
    raw_fallback_transactions: u64,
    /// V0 transactions whose loaded addresses were not recoverable.
    loaded_addresses_unavailable: u64,
    /// Transactions with no recorded inner instructions, which is not the same
    /// as a recorded empty list.
    cpi_not_recorded: u64,
    /// Account keys stored inline rather than as a registry id.
    raw_account_keys: u64,
    /// Recent blockhashes that did not resolve to a registry id.
    ///
    /// On a generation built from a single-block fixture without
    /// `--previous-car` this equals the transaction count, because every
    /// recent blockhash refers to a slot outside the fixture and the blockhash
    /// registry holds one entry. That is a fixture artifact, not a property of
    /// the format -- read it as a signal that the source generation was not
    /// seeded, and re-run against a real epoch before drawing any conclusion.
    nonce_blockhashes: u64,
    plane_bytes: BTreeMap<String, u64>,
    /// Columns where at least one page compressed no smaller than raw.
    raw_pages: BTreeMap<String, u64>,
    /// §7.5: how many top-level instructions keep their original bytes.
    instruction_data_variants: BTreeMap<String, u64>,
    /// Instructions whose original payload bytes are still in the archive.
    instructions_bytes_retained: u64,
    /// Instructions whose Compact V2 source stored only typed semantics. The
    /// converter reconstructs these bytes and proves them with the signature
    /// before it writes the exact target byte lane.
    instructions_bytes_rederived: u64,
    /// Payload bytes actually present, summed over retained instructions only.
    retained_payload_bytes: u64,
    /// Total exact Solana top-level instruction payload bytes written.
    instruction_data_bytes: u64,
    /// CPI payload bytes written. These are byte-exact: the source keeps them
    /// as raw `Vec<u8>`.
    inner_instruction_data_bytes: u64,
    /// Token accounts appearing in both pre and post, whose identity is now
    /// stored once instead of twice.
    token_balances_paired: u64,
    token_balances_total: u64,
    signatures: u64,
    /// Non-PoH hashes in dictionary/blockhashes: previous-epoch values, durable
    /// nonce values, and published hashes for blocks without a PoH final entry.
    blockhash_dictionary_records: u64,
    poh_source_schema: &'static str,
    poh_entries: u64,
    poh_signature_count_recovered_blocks: u64,
    poh_signature_count_legacy_unknown_blocks: u64,
    shredding_boundaries: u64,
    shredding_recorded_empty_blocks: u64,
    nonce_hashes_interned: u64,
    pubkey_dictionary_records: u64,
    block_rewards_stored: u64,
    program_accounts: u64,
    signer_accounts: u64,
    /// Neither signed nor invoked here. The shape of a derived address, but an
    /// inference from observed use rather than a derivation: a wallet that only
    /// received funds this generation lands here too.
    unused_accounts: u64,
    /// Worker threads used for source reads, decompression, decode, and exact
    /// signed-message reconstruction.
    workers: usize,
    /// Parallel block tasks. Large inputs normally use all workers here.
    block_workers: usize,
    /// Per-block signed-message lanes, including the source worker itself.
    /// This is larger than one only for a single-block input.
    intra_block_workers: usize,
    /// One mostly blocked helper that performs monotonic source range reads.
    source_io_workers: usize,
    /// Long-lived threads that encode and compress target pages. Zero means
    /// pages are prepared on the ordered commit thread.
    page_workers: usize,
    /// Page workers that encode and compress transaction row arenas.
    transaction_page_workers: usize,
    /// Page workers that encode transaction-effect chunks.
    effect_workers: usize,
    /// Maximum spawned threads that can exist at once, excluding the ordered
    /// commit coordinator. The monotonic I/O helper is additional to the
    /// requested compute-worker budget, so this is at most `workers + 1`.
    max_spawned_worker_threads: usize,
    /// Highest number of blocks retained by the bounded parallel pipeline.
    peak_in_flight_blocks: usize,
    /// Highest caller-reserved byte total retained by the pipeline.
    peak_in_flight_bytes: usize,
    /// High-water marks for the offset-independent transaction-page queue.
    transaction_page_peak_in_flight_blocks: usize,
    transaction_page_peak_in_flight_bytes: usize,
    source_read_calls: u64,
    source_read_batches: u64,
    source_max_batch_blocks: usize,
    source_max_compressed_batch_bytes: usize,
    source_max_declared_uncompressed_batch_bytes: u64,
    source_max_retained_decompressed_buffer_bytes: usize,
    /// Profiling values for this conversion. These values are diagnostic and
    /// are not part of the candidate content identity.
    timings: PhaseTimings,
    source_block_bytes: u64,
    source_decoded_block_bytes: u64,
    source_first_slot: u64,
    source_last_slot: u64,
    block_pipeline_transactions_per_second: f64,
    block_pipeline_source_mib_per_second: f64,
    benchmark_prefix_blocks: Option<usize>,
    source_total_blocks: usize,
}

#[derive(Debug)]
struct Args {
    source: PathBuf,
    output: PathBuf,
    workers: usize,
    pipeline_memory_limit_bytes: usize,
    epoch: Option<u64>,
    slots_per_epoch: Option<u64>,
    fixture_source: bool,
    fixture_message_schema: Option<CompactV2MessageSchema>,
    fixture_metadata_schema: Option<CompactV2MetadataSchema>,
    fixture_previous_blockhash: Option<[u8; 32]>,
    fixture_previous_slot: Option<u64>,
    benchmark_prefix_blocks: Option<usize>,
}

fn parse_args() -> Result<Args> {
    let mut positional = Vec::new();
    let mut workers = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    let mut pipeline_memory_limit_mib = 4_096usize;
    let mut epoch = None;
    let mut slots_per_epoch = None;
    let mut fixture_source = false;
    let mut fixture_message_schema = None;
    let mut fixture_metadata_schema = None;
    let mut fixture_previous_blockhash = None;
    let mut fixture_previous_slot = None;
    let mut benchmark_prefix_blocks = None;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workers" => {
                workers = args
                    .next()
                    .context("--workers requires a positive integer")?
                    .parse()
                    .context("--workers must be a positive integer")?;
            }
            "--pipeline-memory-limit-mib" => {
                pipeline_memory_limit_mib = args
                    .next()
                    .context("--pipeline-memory-limit-mib requires a positive integer")?
                    .parse()
                    .context("--pipeline-memory-limit-mib must be a positive integer")?;
            }
            "--epoch" => {
                epoch = Some(
                    args.next()
                        .context("--epoch requires an integer")?
                        .parse()
                        .context("--epoch must be an integer")?,
                );
            }
            "--slots-per-epoch" => {
                slots_per_epoch = Some(
                    args.next()
                        .context("--slots-per-epoch requires a positive integer")?
                        .parse()
                        .context("--slots-per-epoch must be a positive integer")?,
                );
            }
            "--fixture-source" => fixture_source = true,
            "--fixture-message-schema" => {
                fixture_message_schema =
                    Some(parse_fixture_message_schema(&args.next().context(
                        "--fixture-message-schema requires current or may24-pre-unknown-fallbacks",
                    )?)?);
            }
            "--fixture-metadata-schema" => {
                fixture_metadata_schema =
                    Some(parse_fixture_metadata_schema(&args.next().context(
                        "--fixture-metadata-schema requires current-typed-error or legacy-raw-error",
                    )?)?);
            }
            "--fixture-previous-blockhash-hex" => {
                fixture_previous_blockhash =
                    Some(parse_hex_32(&args.next().context(
                        "--fixture-previous-blockhash-hex requires 64 hex characters",
                    )?)?);
            }
            "--fixture-previous-slot" => {
                fixture_previous_slot = Some(
                    args.next()
                        .context("--fixture-previous-slot requires an integer")?
                        .parse()
                        .context("--fixture-previous-slot must be an integer")?,
                );
            }
            "--benchmark-prefix-blocks" => {
                benchmark_prefix_blocks = Some(
                    args.next()
                        .context("--benchmark-prefix-blocks requires a positive integer")?
                        .parse()
                        .context("--benchmark-prefix-blocks must be a positive integer")?,
                );
            }
            _ if argument.starts_with('-') => bail!("unknown option {argument}"),
            _ => positional.push(PathBuf::from(argument)),
        }
    }
    ensure!(
        positional.len() == 2,
        "usage: blockzilla-index-archive-convert <v2-generation-dir> <output-dir> \
         [--workers N] [--pipeline-memory-limit-mib N] [--epoch N --slots-per-epoch N \
         --fixture-source --fixture-message-schema current|may24-pre-unknown-fallbacks \
         --fixture-metadata-schema current-typed-error|legacy-raw-error \
         [--fixture-previous-blockhash-hex HEX \
         --fixture-previous-slot SLOT] [--benchmark-prefix-blocks N]]"
    );
    ensure!(workers > 0, "--workers must be greater than zero");
    ensure!(
        workers <= MAX_ORDERED_PARALLEL_DECODE_WORKERS,
        "--workers must not exceed {MAX_ORDERED_PARALLEL_DECODE_WORKERS}"
    );
    ensure!(
        pipeline_memory_limit_mib > 0,
        "--pipeline-memory-limit-mib must be greater than zero"
    );
    let pipeline_memory_limit_bytes = pipeline_memory_limit_mib
        .checked_mul(1024 * 1024)
        .context("--pipeline-memory-limit-mib overflows this platform")?;
    ensure!(
        fixture_previous_blockhash.is_some() == fixture_previous_slot.is_some(),
        "fixture predecessor hash and slot must be supplied together"
    );
    ensure!(
        fixture_source || fixture_previous_blockhash.is_none(),
        "fixture predecessor evidence is valid only with --fixture-source"
    );
    ensure!(
        fixture_source || fixture_message_schema.is_none(),
        "a fixture message schema is valid only with --fixture-source"
    );
    ensure!(
        fixture_source || fixture_metadata_schema.is_none(),
        "a fixture metadata schema is valid only with --fixture-source"
    );
    ensure!(
        benchmark_prefix_blocks.is_none_or(|blocks| blocks > 0),
        "--benchmark-prefix-blocks must be greater than zero"
    );
    Ok(Args {
        source: positional.remove(0),
        output: positional.remove(0),
        workers,
        pipeline_memory_limit_bytes,
        epoch,
        slots_per_epoch,
        fixture_source,
        fixture_message_schema,
        fixture_metadata_schema,
        fixture_previous_blockhash,
        fixture_previous_slot,
        benchmark_prefix_blocks,
    })
}

fn parse_fixture_message_schema(value: &str) -> Result<CompactV2MessageSchema> {
    match value {
        "current" => Ok(CompactV2MessageSchema::Current),
        "may24-pre-unknown-fallbacks" => Ok(CompactV2MessageSchema::May24PreUnknownFallbacks),
        _ => bail!(
            "unsupported fixture message schema {value}; use current or may24-pre-unknown-fallbacks"
        ),
    }
}

fn parse_fixture_metadata_schema(value: &str) -> Result<CompactV2MetadataSchema> {
    match value {
        "current-typed-error" => Ok(CompactV2MetadataSchema::CurrentTypedError),
        "legacy-raw-error" => Ok(CompactV2MetadataSchema::LegacyRawError),
        _ => bail!(
            "unsupported fixture metadata schema {value}; use current-typed-error or legacy-raw-error"
        ),
    }
}

struct SourceContext {
    epoch: u64,
    slots_per_epoch: u64,
    generation_digest: Option<String>,
    published: bool,
    source: PinnedLocalRangeSource,
    reader: Arc<ArchiveReader<PinnedLocalRangeSource>>,
    index: Option<blockzilla_format::ArchiveV2HotBlockIndex>,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
}

fn validate_source_publication(source: &Path, args: &Args) -> Result<SourceContext> {
    let pinned = PinnedLocalRangeSource::new(source);
    if pinned.size(GENERATION_MANIFEST_FILE)?.is_some() {
        ensure!(
            !args.fixture_source,
            "--fixture-source cannot weaken a published source generation"
        );
        ensure!(
            args.fixture_message_schema.is_none(),
            "--fixture-message-schema cannot override a published source generation"
        );
        ensure!(
            args.fixture_metadata_schema.is_none(),
            "--fixture-metadata-schema cannot override a published source generation"
        );
        let options = OpenOptions {
            hash_verification: HashVerification::AllFiles,
            ..OpenOptions::default()
        };
        let validated = ArchiveReader::open_with_options(pinned.clone(), options)
            .context("validate published Compact V2 generation")?;
        let manifest = validated.manifest();
        if let Some(epoch) = args.epoch {
            ensure!(
                epoch == manifest.epoch,
                "--epoch does not match source manifest"
            );
        }
        if let Some(slots) = args.slots_per_epoch {
            ensure!(
                slots == manifest.slots_per_epoch,
                "--slots-per-epoch does not match source manifest"
            );
        }
        ensure!(manifest.complete, "source generation is not complete");
        for name in [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
            ARCHIVE_V2_SIGNATURES_FILE,
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ARCHIVE_V2_POH_FILE,
            ARCHIVE_V2_SHREDDING_FILE,
        ] {
            manifest.required_file(name).with_context(|| {
                format!("published source manifest does not bind required converter input {name}")
            })?;
        }
        if manifest.epoch == 0 {
            manifest
                .required_file(ARCHIVE_V2_GENESIS_BIN_FILE)
                .context("epoch-zero source manifest does not bind genesis.bin")?;
        } else {
            manifest
                .required_file(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
                .context("source manifest does not bind prev_blockhash_tail.bin")?;
        }
        if pinned.size(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE)?.is_some() {
            manifest
                .required_file(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE)
                .context("source manifest does not bind vote_hash_registry.bin")?;
        }
        let message_schema = select_compact_v2_message_schema(&pinned, manifest)
            .context("select manifest-bound Compact V2 message grammar")?;
        let metadata_schema = select_published_metadata_schema(manifest)?;
        let index = validated.index().clone();
        return Ok(SourceContext {
            epoch: manifest.epoch,
            slots_per_epoch: manifest.slots_per_epoch,
            generation_digest: Some(manifest.generation_digest.clone()),
            published: true,
            source: pinned,
            reader: Arc::new(validated),
            index: Some(index),
            message_schema,
            metadata_schema,
        });
    }

    ensure!(
        args.fixture_source,
        "source has no archive-v2-generation.json; use --fixture-source only for a local test input"
    );
    let epoch = args
        .epoch
        .context("an unpublished fixture source requires --epoch")?;
    let slots_per_epoch = args
        .slots_per_epoch
        .context("an unpublished fixture source requires --slots-per-epoch")?;
    ensure!(slots_per_epoch > 0, "--slots-per-epoch must be positive");
    let message_schema = args.fixture_message_schema.context(
        "an unpublished fixture source requires --fixture-message-schema; its wire grammar cannot be inferred",
    )?;
    let metadata_schema = args.fixture_metadata_schema.context(
        "an unpublished fixture source requires --fixture-metadata-schema; its wire grammar cannot be inferred",
    )?;
    let reader = ArchiveReader::open_trusted(
        pinned.clone(),
        TrustedGenerationIdentity {
            cluster_id: "mainnet-beta".to_owned(),
            epoch,
            generation_id: format!("unpublished-index-archive-source-{epoch}"),
            slots_per_epoch,
        },
        OpenOptions {
            hash_verification: HashVerification::SizesOnly,
            ..OpenOptions::default()
        },
    )
    .context("validate trusted unpublished Compact V2 source for borrowed reads")?;
    Ok(SourceContext {
        epoch,
        slots_per_epoch,
        generation_digest: None,
        published: false,
        source: pinned,
        reader: Arc::new(reader),
        index: None,
        message_schema,
        metadata_schema,
    })
}

fn select_published_metadata_schema(
    manifest: &blockzilla_read_sdk::manifest::GenerationManifest,
) -> Result<CompactV2MetadataSchema> {
    bail!(
        "published source generation {} has no manifest-bound Compact V2 metadata-schema selector; refusing to infer current-typed-error or legacy-raw-error from transaction data",
        manifest.generation_digest
    )
}

fn read_pinned_all(source: &PinnedLocalRangeSource, object: &str) -> Result<Vec<u8>> {
    let size = source
        .size(object)?
        .with_context(|| format!("required source object {object} is missing"))?;
    let length = usize::try_from(size).with_context(|| format!("{object} is too large to map"))?;
    source
        .read_range(object, 0, length)
        .with_context(|| format!("read pinned source object {object}"))
}

fn usable_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn staging_path(output: &Path) -> Result<PathBuf> {
    let parent = usable_parent(output);
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .context("output path must end in a UTF-8 file name")?;
    Ok(parent.join(format!(".{name}.building-{}", std::process::id())))
}

fn task_reservation_bytes(row: blockzilla_format::ArchiveV2HotBlockIndexRow) -> Result<usize> {
    // A worker holds the compressed frame, the decoded frame, the owned
    // decoded value, reconstructed instruction bytes, and target page inputs
    // at the same time. Ordered commit can also compress several target pages
    // at once. Eight decoded lengths are a conservative bound for these values
    // and their stored-page output retained until ordered commit finishes.
    let decoded = usize::try_from(row.uncompressed_len).context("decoded block length overflow")?;
    let compressed = usize::try_from(row.compressed_len).context("block length overflow")?;
    decoded
        .checked_mul(8)
        .and_then(|bytes| bytes.checked_add(compressed))
        .context("block memory reservation overflow")
}

fn same_source_index_row(
    left: blockzilla_format::ArchiveV2HotBlockIndexRow,
    right: blockzilla_format::ArchiveV2HotBlockIndexRow,
) -> bool {
    left.block_id == right.block_id
        && left.slot == right.slot
        && left.compressed_offset == right.compressed_offset
        && left.compressed_len == right.compressed_len
        && left.uncompressed_len == right.uncompressed_len
        && left.tx_count == right.tx_count
        && left.first_tx_ordinal == right.first_tx_ordinal
        && left.first_signature_ordinal == right.first_signature_ordinal
        && left.signature_count == right.signature_count
}

struct DecodedSourceBlock {
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    block: blockzilla_format::ArchiveV2HotBlockBlob,
    transactions: Vec<ProjectedSourceTransaction>,
    borrowed_to_owned_time: Duration,
    message_decode_time: Duration,
    signed_message_proof_time: Duration,
    metadata_decode_time: Duration,
    validation_time: Duration,
    inline_log_pubkey_discovery_time: Duration,
    raw_pubkey_visits: Vec<[u8; 32]>,
}

#[derive(Debug, Clone, Copy)]
struct ValidatedTransactionShape {
    static_account_count: u32,
    loaded_address_counts: LoadedAddressCounts,
    resolved_account_count: usize,
}

/// Source-owned transaction data after decode and signed-message proof, but
/// before the block-wide ordered validation pass.
struct ReconstructedSourceTransaction {
    payload: ArchiveV2HotMessagePayload,
    metadata: Option<blockzilla_format::CompactMetaV1>,
    exact_instruction_data: Vec<Vec<u8>>,
    message_decode_time: Duration,
    signed_message_proof_time: Duration,
    metadata_decode_time: Duration,
}

/// Source-owned transaction data that is safe to move out of the borrowed
/// reader worker. Message decoding already happens for the signed-message
/// proof, and all source-only shape checks have completed in transaction
/// order after the full block was reconstructed.
#[derive(Debug)]
struct ProjectedSourceTransaction {
    payload: ArchiveV2HotMessagePayload,
    metadata: Option<blockzilla_format::CompactMetaV1>,
    exact_instruction_data: Vec<Vec<u8>>,
    message_decode_time: Duration,
    signed_message_proof_time: Duration,
    metadata_decode_time: Duration,
    shape: ValidatedTransactionShape,
    inline_log_pubkeys: Vec<[u8; 32]>,
}

/// One transaction after every public-key and recent-hash ID is fixed in
/// deterministic source order. All remaining work is offset-independent.
struct ResolvedSourceTransaction {
    payload: ArchiveV2HotMessagePayload,
    metadata: Option<blockzilla_format::CompactMetaV1>,
    exact_instruction_data: Vec<Vec<u8>>,
    recent_blockhash: transactions_codec::HashRef,
    inline_log_pubkeys: Vec<[u8; 32]>,
    shape: ValidatedTransactionShape,
}

/// A block boundary between ordered dictionary assignment and parallel target
/// conversion. The source block remains owned until its prepared target bytes
/// are committed, and its full reservation remains charged for that lifetime.
struct ResolvedBlockInput {
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    block: blockzilla_format::ArchiveV2HotBlockBlob,
    transactions: Vec<ResolvedSourceTransaction>,
    pubkeys: ResolvedPubkeyTable,
    blockhash: transactions_codec::HashRef,
    previous_blockhash: transactions_codec::HashRef,
    delta: BlockConversionDelta,
}

struct ConvertedResolvedBlock {
    commit: TransactionCommitInput,
    transactions: Vec<transactions_codec::Transaction>,
    block_rewards: Option<PreparedPage>,
    delta: BlockConversionDelta,
}

/// Ordered, offset-independent target data for one block.
///
/// Global dictionary and hash assignment is complete before this value is
/// made. The transaction arena worker can therefore encode and compress its
/// rows without touching target files or changing deterministic IDs.
/// Values retained until the final ordered effect and catalog write.
struct TransactionCommitInput {
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    slot: u64,
    parent_slot: u64,
    block_time: Option<i64>,
    block_height: Option<u64>,
    blockhash: transactions_codec::HashRef,
    previous_blockhash: transactions_codec::HashRef,
    effect_states: Vec<transactions_codec::EffectState>,
    inner: Vec<Option<(inner_instructions::TransactionInner, usize, usize)>>,
    outcomes: Vec<Option<outcomes_codec::TransactionOutcome>>,
    balances: Vec<Option<balances_plane_codec::Balances>>,
    token_balances: Vec<Option<Vec<token_balances_codec::TokenBalance>>>,
    logs: Vec<Option<Vec<logs_codec::LogLine>>>,
    rewards: Vec<Option<Vec<rewards_codec::Reward>>>,
}

/// Offset-bearing block data after all effect bytes are worker-prepared.
struct TransactionFinalCommitInput {
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    slot: u64,
    parent_slot: u64,
    block_time: Option<i64>,
    block_height: Option<u64>,
    blockhash: transactions_codec::HashRef,
    previous_blockhash: transactions_codec::HashRef,
    effect_states: Vec<transactions_codec::EffectState>,
}

/// Worker result. Absolute effect and page offsets are still unset.
struct PreparedTransactionBlock {
    commit: TransactionFinalCommitInput,
    prepared_effects:
        [Vec<Option<effect_chunks::PreparedEffectChunk>>; transactions_codec::EFFECT_KIND_COUNT],
    arena: PreparedTransactionArena,
    block_rewards: Option<PreparedPage>,
    delta: BlockConversionDelta,
    pure_conversion_time: Duration,
    effect_worker_time: Duration,
    arena_worker_time: Duration,
}

fn prepare_transaction_effects(
    encoder: &effect_chunks::EffectEncoder,
    commit: TransactionCommitInput,
) -> Result<(
    TransactionFinalCommitInput,
    [Vec<Option<effect_chunks::PreparedEffectChunk>>; transactions_codec::EFFECT_KIND_COUNT],
)> {
    let TransactionCommitInput {
        row,
        slot,
        parent_slot,
        block_time,
        block_height,
        blockhash,
        previous_blockhash,
        effect_states,
        inner,
        outcomes,
        balances,
        token_balances,
        logs,
        rewards,
    } = commit;
    let shared_states = Arc::new(effect_states);
    let prepared_effects = encoder.prepare([
        {
            let states = Arc::clone(&shared_states);
            effect_chunks::effect_job(0, move || {
                effect_chunks::prepare_effect_chunks(
                    &inner,
                    &states,
                    transactions_codec::EffectKind::InnerInstructions,
                    |(record, top_level_count, account_count)| {
                        inner_instructions::encode_record(record, *top_level_count, *account_count)
                    },
                )
            })
        },
        {
            let states = Arc::clone(&shared_states);
            effect_chunks::effect_job(1, move || {
                effect_chunks::prepare_effect_chunks(
                    &outcomes,
                    &states,
                    transactions_codec::EffectKind::Outcome,
                    outcomes_codec::encode_record,
                )
            })
        },
        {
            let states = Arc::clone(&shared_states);
            effect_chunks::effect_job(2, move || {
                effect_chunks::prepare_effect_chunks(
                    &balances,
                    &states,
                    transactions_codec::EffectKind::Balances,
                    balances_plane_codec::encode_record,
                )
            })
        },
        {
            let states = Arc::clone(&shared_states);
            effect_chunks::effect_job(3, move || {
                effect_chunks::prepare_effect_chunks(
                    &token_balances,
                    &states,
                    transactions_codec::EffectKind::TokenBalances,
                    |values| token_balances_codec::encode_record(values),
                )
            })
        },
        {
            let states = Arc::clone(&shared_states);
            effect_chunks::effect_job(4, move || {
                effect_chunks::prepare_effect_chunks(
                    &logs,
                    &states,
                    transactions_codec::EffectKind::Logs,
                    |values| logs_codec::encode_record(values),
                )
            })
        },
        {
            let states = Arc::clone(&shared_states);
            effect_chunks::effect_job(5, move || {
                effect_chunks::prepare_effect_chunks(
                    &rewards,
                    &states,
                    transactions_codec::EffectKind::Rewards,
                    |values| rewards_codec::encode_record(values),
                )
            })
        },
    ])?;
    let effect_states =
        Arc::try_unwrap(shared_states).unwrap_or_else(|states| states.as_ref().clone());
    Ok((
        TransactionFinalCommitInput {
            row,
            slot,
            parent_slot,
            block_time,
            block_height,
            blockhash,
            previous_blockhash,
            effect_states,
        },
        prepared_effects,
    ))
}

struct SourceWorker {
    registry: File,
    signatures: File,
    registry_entries: u32,
    source_hashes: Arc<BlockhashResolver>,
    vote_hashes: Option<Arc<VoteHashRegistry>>,
    intra_block_workers: usize,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    blockhash_registry_offset: u32,
}

struct SourceWorkerConfig {
    registry_entries: u32,
    source_hashes: Arc<BlockhashResolver>,
    vote_hashes: Option<Arc<VoteHashRegistry>>,
    intra_block_workers: usize,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    blockhash_registry_offset: u32,
}

impl SourceWorker {
    fn open(source: &PinnedLocalRangeSource, config: SourceWorkerConfig) -> Result<Self> {
        let SourceWorkerConfig {
            registry_entries,
            source_hashes,
            vote_hashes,
            intra_block_workers,
            message_schema,
            metadata_schema,
            blockhash_registry_offset,
        } = config;
        Ok(Self {
            registry: source
                .pinned_file_clone(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)?
                .context("source pubkey registry is missing")?,
            signatures: source
                .pinned_file_clone(ARCHIVE_V2_SIGNATURES_FILE)?
                .context("source signatures are missing")?,
            registry_entries,
            source_hashes,
            vote_hashes,
            intra_block_workers,
            message_schema,
            metadata_schema,
            blockhash_registry_offset,
        })
    }

    fn reconstruct_transaction(
        &self,
        block: &blockzilla_format::ArchiveV2HotBlockBlob,
        transaction: &blockzilla_format::ArchiveV2HotTxRow,
        signature_ordinal: u64,
    ) -> Result<ReconstructedSourceTransaction> {
        ensure!(
            transaction.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK == 0,
            "slot {} transaction {} uses raw message fallback; exact signed input is unavailable",
            block.header.slot,
            transaction.tx_index
        );
        let message_decode_started = Instant::now();
        let payload = decode_message_with_schema(self.message_schema, block, transaction)
            .context("decode checked hot message in source worker")?;
        let message_decode_time = message_decode_started.elapsed();
        let (header, account_keys, recent_blockhash, instructions, _) = convert_message(&payload);
        ensure!(
            transaction.signature_count > 0
                && transaction.signature_count == header.num_required_signatures,
            "slot {} transaction {} signature/header count mismatch",
            block.header.slot,
            transaction.tx_index
        );
        let signed_message_proof_started = Instant::now();
        let exact_instruction_data = reconstruct_signed_instruction_data(
            &payload,
            *header,
            account_keys,
            recent_blockhash,
            instructions,
            signature_ordinal,
            SignedMessageProofContext {
                registry: &self.registry,
                registry_entries: self.registry_entries,
                signatures: &self.signatures,
                source_hashes: &self.source_hashes,
                vote_hashes: self.vote_hashes.as_deref(),
            },
        )
        .with_context(|| {
            format!(
                "slot {} transaction {} exact signed message",
                block.header.slot, transaction.tx_index
            )
        })?;
        let signed_message_proof_time = signed_message_proof_started.elapsed();
        let metadata_decode_started = Instant::now();
        let metadata = decode_metadata(self.metadata_schema, block, transaction)
            .context("decode checked metadata in source worker")?;
        let metadata_decode_time = metadata_decode_started.elapsed();
        Ok(ReconstructedSourceTransaction {
            payload,
            metadata,
            exact_instruction_data,
            message_decode_time,
            signed_message_proof_time,
            metadata_decode_time,
        })
    }

    fn project_borrowed(&self, borrowed: BorrowedDecodedBlock<'_>) -> Result<DecodedSourceBlock> {
        ensure!(
            !borrowed.uses_owned_fallback(),
            "slot {} used the historical owned block fallback; the fast converter requires the current borrowed schema",
            borrowed.index_row.slot
        );
        let borrowed_to_owned_started = Instant::now();
        let owned = borrowed
            .into_owned()
            .context("copy the borrowed projection needed by ordered mapping")?;
        let borrowed_to_owned_time = borrowed_to_owned_started.elapsed();
        let row = owned.index_row;
        let mut decoded = DecodedSourceBlock {
            row,
            block: owned.block,
            transactions: Vec::new(),
            borrowed_to_owned_time,
            message_decode_time: Duration::ZERO,
            signed_message_proof_time: Duration::ZERO,
            metadata_decode_time: Duration::ZERO,
            validation_time: Duration::ZERO,
            inline_log_pubkey_discovery_time: Duration::ZERO,
            raw_pubkey_visits: Vec::new(),
        };
        let mut signature_ordinal = row.first_signature_ordinal;
        let mut signature_ordinals = Vec::with_capacity(decoded.block.tx_rows.len());
        for transaction in &decoded.block.tx_rows {
            signature_ordinals.push(signature_ordinal);
            signature_ordinal = signature_ordinal
                .checked_add(u64::from(transaction.signature_count))
                .context("worker signature ordinal overflow")?;
        }
        ensure!(
            signature_ordinal
                == row
                    .first_signature_ordinal
                    .checked_add(u64::from(row.signature_count))
                    .context("worker block signature range overflow")?,
            "slot {} transaction signatures do not cover the indexed block range",
            row.slot
        );
        let lane_count = self
            .intra_block_workers
            .min(decoded.block.tx_rows.len().max(1));
        let reconstructed = if lane_count == 1 || decoded.block.tx_rows.is_empty() {
            decoded
                .block
                .tx_rows
                .iter()
                .zip(&signature_ordinals)
                .map(|(transaction, ordinal)| {
                    self.reconstruct_transaction(&decoded.block, transaction, *ordinal)
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            let chunk_len = decoded.block.tx_rows.len().div_ceil(lane_count);
            std::thread::scope(|scope| {
                let mut chunks = decoded
                    .block
                    .tx_rows
                    .chunks(chunk_len)
                    .zip(signature_ordinals.chunks(chunk_len));
                let (first_transactions, first_ordinals) =
                    chunks.next().expect("non-empty block has a first lane");
                let mut handles = Vec::with_capacity(lane_count - 1);
                for (transactions, ordinals) in chunks {
                    let block = &decoded.block;
                    handles.push(scope.spawn(move || {
                        transactions
                            .iter()
                            .zip(ordinals)
                            .map(|(transaction, ordinal)| {
                                self.reconstruct_transaction(block, transaction, *ordinal)
                            })
                            .collect::<Result<Vec<_>>>()
                    }));
                }
                // The source worker handles the first lane itself. Only the
                // remaining lanes create nested threads, so the configured
                // lane count includes the source worker instead of exceeding
                // the global worker budget.
                let mut rebuilt = first_transactions
                    .iter()
                    .zip(first_ordinals)
                    .map(|(transaction, ordinal)| {
                        self.reconstruct_transaction(&decoded.block, transaction, *ordinal)
                    })
                    .collect::<Result<Vec<_>>>()?;
                rebuilt.reserve(decoded.block.tx_rows.len() - rebuilt.len());
                for handle in handles {
                    rebuilt.extend(
                        handle
                            .join()
                            .map_err(|_| anyhow::anyhow!("signed-message worker panicked"))??,
                    );
                }
                Ok::<_, anyhow::Error>(rebuilt)
            })?
        };
        // Preserve block/transaction error order: every transaction completes
        // decode, proof, and metadata reconstruction before source-only shape
        // validation begins. The validation pass also freezes the exact raw
        // public-key first-use trace consumed by ordered dictionary assignment.
        let validation_started = Instant::now();
        let (transactions, raw_pubkey_visits, inline_log_pubkey_discovery_time) =
            validate_reconstructed_transactions(
                decoded.block.header.slot,
                &decoded.block.tx_rows,
                reconstructed,
                self.registry_entries,
            )?;
        decoded.raw_pubkey_visits = raw_pubkey_visits;
        validate_source_block_tail(
            row,
            &decoded.block.header,
            self.blockhash_registry_offset,
            self.registry_entries,
            &mut decoded.raw_pubkey_visits,
        )?;
        decoded.validation_time = validation_started
            .elapsed()
            .saturating_sub(inline_log_pubkey_discovery_time);
        decoded.inline_log_pubkey_discovery_time = inline_log_pubkey_discovery_time;
        decoded.transactions = transactions;
        decoded.message_decode_time = decoded
            .transactions
            .iter()
            .fold(Duration::ZERO, |sum, transaction| {
                sum.saturating_add(transaction.message_decode_time)
            });
        decoded.signed_message_proof_time = decoded
            .transactions
            .iter()
            .fold(Duration::ZERO, |sum, transaction| {
                sum.saturating_add(transaction.signed_message_proof_time)
            });
        decoded.metadata_decode_time = decoded
            .transactions
            .iter()
            .fold(Duration::ZERO, |sum, transaction| {
                sum.saturating_add(transaction.metadata_decode_time)
            });
        Ok(decoded)
    }
}

fn decode_source_block(
    blocks: &File,
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
) -> Result<DecodedSourceBlock> {
    let mut frame = vec![0; usize::try_from(row.compressed_len).context("frame length overflow")?];
    blocks
        .read_exact_at(&mut frame, row.compressed_offset)
        .with_context(|| format!("read block frame at slot {}", row.slot))?;
    let decoded_len = usize::try_from(row.uncompressed_len).context("decoded length overflow")?;
    let mut raw = vec![0_u8; decoded_len];
    let mut decoder = zstd::stream::read::Decoder::new(frame.as_slice())
        .with_context(|| format!("open zstd block frame at slot {}", row.slot))?;
    decoder
        .read_exact(&mut raw)
        .with_context(|| format!("zstd block frame at slot {} ended early", row.slot))?;
    let mut trailing = [0_u8; 1];
    ensure!(
        decoder
            .read(&mut trailing)
            .with_context(|| format!("finish zstd block frame at slot {}", row.slot))?
            == 0,
        "slot {} expands beyond its declared {} decoded bytes",
        row.slot,
        row.uncompressed_len
    );
    let block: blockzilla_format::ArchiveV2HotBlockBlob =
        wincode::config::deserialize_exact(&raw, wincode_leb128_config()).with_context(|| {
            format!(
                "decode exact current-hot-v1 block schema at slot {}; legacy trial decoding is disabled",
                row.slot
            )
        })?;
    ensure!(
        block.header.slot == row.slot,
        "slot mismatch in decoded block"
    );
    ensure!(
        block.tx_count == row.tx_count,
        "transaction-count mismatch at slot {}",
        row.slot
    );
    ensure!(
        block.tx_rows.len() == row.tx_count as usize,
        "transaction-row count mismatch at slot {}",
        row.slot
    );
    let mut expected_message_offset = 0_u32;
    let mut expected_metadata_offset = 0_u32;
    let mut signature_count = 0_u32;
    for (position, transaction) in block.tx_rows.iter().enumerate() {
        ensure!(
            transaction.tx_index as usize == position,
            "slot {} transaction row {position} has index {}",
            row.slot,
            transaction.tx_index
        );
        ensure!(
            transaction.reserved == [0; 3],
            "slot {} transaction {} has non-zero reserved bytes",
            row.slot,
            transaction.tx_index
        );
        ensure!(
            transaction.flags & !KNOWN_SOURCE_TX_FLAGS == 0,
            "slot {} transaction {} has unknown flags {:#x}",
            row.slot,
            transaction.tx_index,
            transaction.flags & !KNOWN_SOURCE_TX_FLAGS
        );
        ensure!(
            transaction.message_len != 0 && transaction.message_offset == expected_message_offset,
            "slot {} transaction {} has an empty or non-contiguous message range",
            row.slot,
            transaction.tx_index
        );
        block
            .message_bytes
            .get(
                transaction.message_offset as usize
                    ..transaction
                        .message_offset
                        .checked_add(transaction.message_len)
                        .context("message range overflow")? as usize,
            )
            .context("message range is outside the block lane")?;
        expected_message_offset = transaction
            .message_offset
            .checked_add(transaction.message_len)
            .context("message offset overflow")?;
        if transaction.flags & blockzilla_format::ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                transaction.metadata_len == 0,
                "slot {} transaction {} has metadata bytes without HAS_METADATA",
                row.slot,
                transaction.tx_index
            );
        } else {
            ensure!(
                transaction.metadata_len != 0
                    && transaction.metadata_offset == expected_metadata_offset,
                "slot {} transaction {} has an empty or non-contiguous metadata range",
                row.slot,
                transaction.tx_index
            );
            block
                .metadata_bytes
                .get(
                    transaction.metadata_offset as usize
                        ..transaction
                            .metadata_offset
                            .checked_add(transaction.metadata_len)
                            .context("metadata range overflow")? as usize,
                )
                .context("metadata range is outside the block lane")?;
            expected_metadata_offset = transaction
                .metadata_offset
                .checked_add(transaction.metadata_len)
                .context("metadata offset overflow")?;
        }
        signature_count = signature_count
            .checked_add(u32::from(transaction.signature_count))
            .context("block signature count overflow")?;
    }
    ensure!(
        expected_message_offset as usize == block.message_bytes.len(),
        "slot {} has unindexed trailing message bytes",
        row.slot
    );
    ensure!(
        expected_metadata_offset as usize == block.metadata_bytes.len(),
        "slot {} has unindexed trailing metadata bytes",
        row.slot
    );
    ensure!(
        signature_count == row.signature_count,
        "slot {} transaction signature sum is {signature_count}, index declares {}",
        row.slot,
        row.signature_count
    );
    Ok(DecodedSourceBlock {
        row,
        block,
        transactions: Vec::new(),
        borrowed_to_owned_time: Duration::ZERO,
        message_decode_time: Duration::ZERO,
        signed_message_proof_time: Duration::ZERO,
        metadata_decode_time: Duration::ZERO,
        validation_time: Duration::ZERO,
        inline_log_pubkey_discovery_time: Duration::ZERO,
        raw_pubkey_visits: Vec::new(),
    })
}

fn validate_source_shape(
    source: &PinnedLocalRangeSource,
    index: &blockzilla_format::ArchiveV2HotBlockIndex,
    pipeline_memory_limit: usize,
) -> Result<()> {
    ensure!(
        index.flags == 0,
        "source block index flags {:#x} are not supported; dictionary-compressed and raw-block \
         sources need their declared decoder and must not be guessed",
        index.flags
    );
    let block_bytes = source
        .size(ARCHIVE_V2_BLOCKS_FILE)?
        .context("required source blocks file is missing")?;
    ensure!(
        block_bytes == index.blob_file_bytes,
        "blocks file has {block_bytes} bytes, index declares {}",
        index.blob_file_bytes
    );
    let mut expected_offset = 0u64;
    let mut expected_tx = 0u64;
    let mut expected_signatures = 0u64;
    for (position, row) in index.rows.iter().enumerate() {
        ensure!(
            row.block_id as usize == position,
            "block ids are not contiguous"
        );
        ensure!(
            row.compressed_offset == expected_offset,
            "slot {} block frame is not contiguous",
            row.slot
        );
        ensure!(
            row.first_tx_ordinal == expected_tx,
            "slot {} transaction ordinals are not contiguous",
            row.slot
        );
        ensure!(
            row.first_signature_ordinal == expected_signatures,
            "slot {} signature ordinals are not contiguous",
            row.slot
        );
        ensure!(
            row.compressed_len != 0 && row.uncompressed_len != 0,
            "slot {} has an empty source frame",
            row.slot
        );
        let reservation = task_reservation_bytes(*row)?;
        ensure!(
            reservation <= pipeline_memory_limit,
            "slot {} needs a {reservation}-byte pipeline reservation, above the configured {pipeline_memory_limit}-byte limit",
            row.slot
        );
        expected_offset = expected_offset
            .checked_add(u64::from(row.compressed_len))
            .context("block offset overflow")?;
        expected_tx = expected_tx
            .checked_add(u64::from(row.tx_count))
            .context("transaction count overflow")?;
        expected_signatures = expected_signatures
            .checked_add(u64::from(row.signature_count))
            .context("signature count overflow")?;
    }
    ensure!(
        expected_offset == index.blob_file_bytes,
        "block frames do not cover the file"
    );

    for (name, record_len, expected_records) in [
        (ARCHIVE_V2_PUBKEY_REGISTRY_FILE, 32u64, None),
        (ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, 32u64, None),
        (ARCHIVE_V2_SIGNATURES_FILE, 64u64, Some(expected_signatures)),
    ] {
        let bytes = source
            .size(name)?
            .with_context(|| format!("required source object {name} is missing"))?;
        ensure!(
            bytes.is_multiple_of(record_len),
            "{name} is not record-aligned"
        );
        if let Some(records) = expected_records {
            ensure!(
                bytes / record_len == records,
                "{name} has {} records, expected {records}",
                bytes / record_len
            );
        }
    }
    for name in [ARCHIVE_V2_POH_FILE, ARCHIVE_V2_SHREDDING_FILE] {
        ensure!(
            source.size(name)?.is_some(),
            "required source object {name} is missing"
        );
    }
    Ok(())
}

/// One target object, streamed to disk as pages or effect chunks are produced.
///
/// Stored bytes are appended in block order and are not retained for the full
/// epoch.
struct Plane {
    name: &'static str,
    file: HeaderedWriter,
    bytes: u64,
    pages: u64,
    compressed_pages: u64,
    raw_pages: u64,
}

/// One encoded target page after the raw-versus-zstd choice is complete.
///
/// The stored bytes own exactly the data that will be appended. The losing
/// representation is dropped in the compression lane, so the ordered commit
/// does not retain both forms while it writes the planes.
#[derive(Debug, PartialEq, Eq)]
struct PreparedPage {
    stored: Vec<u8>,
    decoded_len: u32,
    compressed: bool,
}

impl PreparedPage {
    fn compress(page: Vec<u8>) -> Result<Self> {
        let decoded_len = u32::try_from(page.len()).context("page exceeds u32")?;
        // Content checksum on. libzstd defaults it off and the zstd CLI turns
        // it on, so encode_all silently produced unchecked frames: measured on
        // a real page, 80.5% of single-bit flips decoded into same-length wrong
        // bytes with no error, passing even the decoded-length check. With the
        // checksum, 400 of 400 flips fail loudly, for four bytes a page.
        let compressed = {
            let mut encoder =
                zstd::Encoder::new(Vec::new(), ZSTD_LEVEL).context("create zstd encoder")?;
            encoder.include_checksum(true).context("enable checksum")?;
            encoder
                .set_pledged_src_size(Some(page.len() as u64))
                .context("set zstd content size")?;
            encoder.write_all(&page).context("compress page")?;
            encoder.finish().context("finish compressed page")?
        };
        if compressed.len() < page.len() {
            Ok(Self {
                stored: compressed,
                decoded_len,
                compressed: true,
            })
        } else {
            Ok(Self {
                stored: page,
                decoded_len,
                compressed: false,
            })
        }
    }
}

impl Plane {
    fn create(root: &Path, name: &'static str) -> Result<Self> {
        Ok(Self {
            name,
            file: HeaderedWriter::create(root, name, 1 << 20)?,
            bytes: blockzilla_index_archive_format::FILE_HEADER_LEN as u64,
            pages: 0,
            compressed_pages: 0,
            raw_pages: 0,
        })
    }

    /// Append one prepared page and return where it landed, for the catalog row.
    ///
    /// A page stored raw has equal stored and decoded lengths, which is how a
    /// reader knows not to decompress. File mutation stays in ordered commit.
    fn push_prepared(&mut self, page: PreparedPage) -> Result<catalog_blocks::PageSpan> {
        if page.compressed {
            self.compressed_pages += 1;
        } else {
            self.raw_pages += 1;
        }
        let offset = self
            .file
            .append(&page.stored, u64::from(page.decoded_len))?;
        let span = catalog_blocks::PageSpan {
            offset,
            stored_len: u32::try_from(page.stored.len()).context("page exceeds u32")?,
            decoded_len: page.decoded_len,
        };
        self.bytes += page.stored.len() as u64;
        self.pages += 1;
        Ok(span)
    }

    fn push_effect_chunks(
        &mut self,
        chunks: Vec<Option<effect_chunks::PreparedEffectChunk>>,
    ) -> Result<transactions_codec::EffectFileIndex> {
        let mut first_chunk_offset = 0_u64;
        let mut frames = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let Some(chunk) = chunk else {
                frames.push(transactions_codec::ChunkFrame::EMPTY);
                continue;
            };
            let frame = chunk.frame()?;
            if frame.is_raw() {
                self.raw_pages += 1;
            } else {
                self.compressed_pages += 1;
            }
            let offset = self
                .file
                .append(&chunk.stored, u64::from(chunk.decoded_len))?;
            if first_chunk_offset == 0 {
                first_chunk_offset = offset;
            }
            self.bytes = self
                .bytes
                .checked_add(chunk.stored.len() as u64)
                .context("effect plane byte count overflow")?;
            self.pages += 1;
            frames.push(frame);
        }
        Ok(transactions_codec::EffectFileIndex {
            first_chunk_offset,
            chunks: frames,
        })
    }

    fn finish(self, archive_id: ArchiveId, record_count: u64, report: &mut Report) -> Result<()> {
        let finished = self.file.finish(archive_id, record_count)?;
        ensure!(
            finished.file_bytes == self.bytes,
            "plane byte accounting drift"
        );
        report
            .plane_bytes
            .insert(self.name.to_owned(), finished.file_bytes);
        if self.raw_pages > 0 {
            report
                .raw_pages
                .insert(self.name.to_owned(), self.raw_pages);
        }
        Ok(())
    }
}

fn hash_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) -> Result<()> {
    hasher.update(
        u64::try_from(bytes.len())
            .context("content-root component length exceeds u64")?
            .to_le_bytes(),
    );
    hasher.update(bytes);
    Ok(())
}

/// Derive the archive identity from the target canonical bytes.
///
/// Header archive-ID bytes are zeroed in the transcript to avoid a circular
/// hash. Derived indexes are excluded: rebuilding an index must not change the
/// identity of the canonical archive it indexes.
fn derive_content_archive_id(root: &Path, epoch: u64) -> Result<ArchiveId> {
    const DOMAIN: &[u8] = b"blockzilla/index-archive/canonical-content-id/v1\0";
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN);
    hasher.update(FORMAT_ID.as_bytes());
    hasher.update(FORMAT_MAJOR.to_le_bytes());
    hasher.update(epoch.to_le_bytes());
    for spec in LAYOUT
        .iter()
        .filter(|spec| spec.class == FileClass::Canonical)
    {
        let path = root.join(spec.path);
        if !path.is_file() {
            ensure!(
                !spec.required_for_epoch(epoch),
                "canonical target object {} is missing",
                spec.path
            );
            continue;
        }
        let spec_binding = serde_json::to_vec(&(
            spec.path,
            spec.role,
            spec.schema,
            spec.class,
            spec.encoding,
            spec.requirement,
            spec.canonical_facts,
            spec.derived_from,
        ))
        .context("encode canonical object specification")?;
        hash_len_prefixed(&mut hasher, &spec_binding)?;

        let source_file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let file_bytes = source_file.metadata()?.len();
        hasher.update(file_bytes.to_le_bytes());
        let mut file = BufReader::with_capacity(8 << 20, source_file);
        if spec.encoding == FileEncoding::HeaderedBinary {
            let mut header_bytes = [0_u8; FILE_HEADER_LEN];
            file.read_exact(&mut header_bytes)
                .with_context(|| format!("read {} header", path.display()))?;
            let header = blockzilla_index_archive_format::FileHeader::decode(&header_bytes)
                .with_context(|| format!("decode {} header", path.display()))?;
            ensure!(
                header.archive_id == ArchiveId::new([0_u8; 16]),
                "{} does not carry the required provisional zero archive ID",
                path.display()
            );
            header
                .validate_for(spec, header.archive_id, file.get_ref().metadata()?.len())
                .with_context(|| format!("validate {} before content binding", path.display()))?;
            header_bytes[40..56].fill(0);
            hasher.update(header_bytes);
        }
        let mut buffer = vec![0_u8; 8 << 20];
        loop {
            let read = file
                .read(&mut buffer)
                .with_context(|| format!("read {} for content binding", path.display()))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Ok(ArchiveId::new(bytes))
}

fn patch_archive_id(root: &Path, old: ArchiveId, new: ArchiveId) -> Result<()> {
    for spec in LAYOUT
        .iter()
        .filter(|spec| spec.encoding == FileEncoding::HeaderedBinary)
    {
        let path = root.join(spec.path);
        if !path.is_file() {
            continue;
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open {} to bind archive id", path.display()))?;
        let mut bytes = [0_u8; FILE_HEADER_LEN];
        file.read_exact_at(&mut bytes, 0)
            .with_context(|| format!("read {} header", path.display()))?;
        let mut header = blockzilla_index_archive_format::FileHeader::decode(&bytes)
            .with_context(|| format!("decode {} header", path.display()))?;
        header
            .validate_for(spec, old, file.metadata()?.len())
            .with_context(|| format!("validate {} provisional id", path.display()))?;
        header.archive_id = new;
        file.write_all_at(&header.encode(), 0)
            .with_context(|| format!("patch {} archive id", path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {} archive id", path.display()))?;
    }
    Ok(())
}

fn variant_name(data: &ArchiveV2HotInstructionData) -> (&'static str, bool) {
    // (variant, retains the original instruction bytes)
    match data {
        ArchiveV2HotInstructionData::Raw(_) => ("Raw", true),
        ArchiveV2HotInstructionData::UnknownSystem(_) => ("UnknownSystem", true),
        ArchiveV2HotInstructionData::UnknownVote(_) => ("UnknownVote", true),
        ArchiveV2HotInstructionData::ComputeBudget(_) => ("ComputeBudget", false),
        ArchiveV2HotInstructionData::System(_) => ("System", false),
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(_) => ("VoteCompactUpdate", false),
        ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch { .. } => {
            ("VoteCompactUpdateSwitch", false)
        }
        ArchiveV2HotInstructionData::VoteTowerSync(_) => ("VoteTowerSync", false),
        ArchiveV2HotInstructionData::VoteTowerSyncSwitch { .. } => ("VoteTowerSyncSwitch", false),
    }
}

fn validate_source_transaction_flags(
    row: &blockzilla_format::ArchiveV2HotTxRow,
    message: &ArchiveV2HotMessagePayload,
    metadata: Option<&blockzilla_format::CompactMetaV1>,
) -> Result<()> {
    let instructions = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => message.instructions.as_slice(),
        ArchiveV2HotMessagePayload::V0(message) => message.instructions.as_slice(),
        ArchiveV2HotMessagePayload::V1(message) => message.instructions.as_slice(),
    };
    let has_compact_vote = instructions.iter().any(|instruction| {
        matches!(
            instruction.data,
            ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(_)
                | ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch { .. }
                | ArchiveV2HotInstructionData::VoteTowerSync(_)
                | ArchiveV2HotInstructionData::VoteTowerSyncSwitch { .. }
        )
    });

    let mut expected = 0_u32;
    if matches!(message, ArchiveV2HotMessagePayload::V0(_)) {
        expected |= ARCHIVE_V2_TX_FLAG_MESSAGE_V0;
    }
    if has_compact_vote {
        expected |= ARCHIVE_V2_TX_FLAG_HAS_COMPACT_VOTE_IX;
    }
    if let Some(metadata) = metadata {
        expected |= ARCHIVE_V2_TX_FLAG_HAS_METADATA;
        if metadata.err.is_some() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_ERROR;
        }
        if metadata.return_data.is_some() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA;
        }
        if metadata.logs.is_some() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_LOGS;
        }
        if metadata.inner_instructions.is_some() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
        }
        if !metadata.pre_token_balances.is_empty() || !metadata.post_token_balances.is_empty() {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
        }
        if !metadata.loaded_writable_addresses.is_empty()
            || !metadata.loaded_readonly_addresses.is_empty()
        {
            expected |= ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        }
    }
    ensure!(
        row.flags == expected,
        "transaction {} flags {:#x} do not match decoded message and metadata {expected:#x}",
        row.tx_index,
        row.flags
    );
    Ok(())
}

/// Length of an instruction's payload, when V2 still has the original bytes.
fn retained_data_len(data: &ArchiveV2HotInstructionData) -> Option<u32> {
    match data {
        ArchiveV2HotInstructionData::Raw(bytes)
        | ArchiveV2HotInstructionData::UnknownSystem(bytes)
        | ArchiveV2HotInstructionData::UnknownVote(bytes) => Some(bytes.len() as u32),
        _ => None,
    }
}

fn source_registry_id(key: &CompactPubkey, registry_entries: u32) -> Result<u32> {
    match key {
        CompactPubkey::Id(id) if *id > 0 && *id <= registry_entries => Ok(*id),
        CompactPubkey::Id(id) => {
            bail!("pubkey registry id {id} is outside the valid 1..={registry_entries} range")
        }
        CompactPubkey::Raw(key) => bail!(
            "inline pubkey {} cannot be changed to reserved id 0; this source needs the \
             deterministic dictionary-interning pass before publication",
            hex_lower(key)
        ),
    }
}

fn validate_source_pubkey_visit(
    key: &CompactPubkey,
    registry_entries: u32,
    raw_visits: &mut Vec<[u8; 32]>,
) -> Result<()> {
    match key {
        CompactPubkey::Id(_) => {
            source_registry_id(key, registry_entries)?;
        }
        CompactPubkey::Raw(bytes) => raw_visits.push(*bytes),
    }
    Ok(())
}

fn validate_optional_source_pubkey_visit(
    key: Option<&CompactPubkey>,
    registry_entries: u32,
    raw_visits: &mut Vec<[u8; 32]>,
) -> Result<()> {
    if let Some(key) = key {
        validate_source_pubkey_visit(key, registry_entries, raw_visits)?;
    }
    Ok(())
}

fn validate_token_balance_visits(
    pre: &[CompactTokenBalance],
    post: &[CompactTokenBalance],
    resolved_account_count: usize,
    registry_entries: u32,
    raw_visits: &mut Vec<[u8; 32]>,
) -> Result<()> {
    let mut seen: BTreeMap<u32, bool> = BTreeMap::new();
    for balance in pre {
        ensure!(
            (balance.account_index as usize) < resolved_account_count,
            "pre-token-balance account index {} is outside {resolved_account_count} resolved accounts",
            balance.account_index
        );
        validate_optional_source_pubkey_visit(balance.mint.as_ref(), registry_entries, raw_visits)?;
        validate_optional_source_pubkey_visit(
            balance.owner.as_ref(),
            registry_entries,
            raw_visits,
        )?;
        validate_optional_source_pubkey_visit(
            balance.program_id.as_ref(),
            registry_entries,
            raw_visits,
        )?;
        ensure!(
            seen.insert(balance.account_index, false).is_none(),
            "duplicate pre-token-balance account index {}",
            balance.account_index
        );
    }
    for balance in post {
        ensure!(
            (balance.account_index as usize) < resolved_account_count,
            "post-token-balance account index {} is outside {resolved_account_count} resolved accounts",
            balance.account_index
        );
        validate_optional_source_pubkey_visit(balance.mint.as_ref(), registry_entries, raw_visits)?;
        validate_optional_source_pubkey_visit(
            balance.owner.as_ref(),
            registry_entries,
            raw_visits,
        )?;
        validate_optional_source_pubkey_visit(
            balance.program_id.as_ref(),
            registry_entries,
            raw_visits,
        )?;
        match seen.get_mut(&balance.account_index) {
            Some(post_seen) => {
                ensure!(
                    !*post_seen,
                    "duplicate post-token-balance account index {}",
                    balance.account_index
                );
                *post_seen = true;
            }
            None => {
                seen.insert(balance.account_index, true);
            }
        }
    }
    Ok(())
}

fn validate_and_project_source_transaction(
    slot: u64,
    transaction: &blockzilla_format::ArchiveV2HotTxRow,
    reconstructed: ReconstructedSourceTransaction,
    registry_entries: u32,
    raw_visits: &mut Vec<[u8; 32]>,
) -> Result<(ProjectedSourceTransaction, Duration)> {
    ensure!(
        transaction.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK == 0,
        "slot {slot} transaction {} uses raw message fallback; exact target rows are unavailable",
        transaction.tx_index
    );
    let ReconstructedSourceTransaction {
        payload,
        metadata,
        exact_instruction_data,
        message_decode_time,
        signed_message_proof_time,
        metadata_decode_time,
    } = reconstructed;
    let (header, account_keys, _, hot_instructions, is_v0) = convert_message(&payload);

    if let ArchiveV2HotMessagePayload::V0(message) = &payload {
        for lookup in &message.address_table_lookups {
            validate_source_pubkey_visit(&lookup.account_key, registry_entries, raw_visits)
                .context("validate lookup-table key in source projection")?;
        }
    }

    validate_source_transaction_flags(transaction, &payload, metadata.as_ref()).with_context(
        || {
            format!(
                "slot {slot} transaction {} source flag projection",
                transaction.tx_index
            )
        },
    )?;

    for key in account_keys {
        validate_source_pubkey_visit(key, registry_entries, raw_visits)?;
    }
    let static_account_count = account_keys.len();
    let loaded_address_counts = if let ArchiveV2HotMessagePayload::V0(message) = &payload {
        validate_v0_loaded_address_counts(
            message,
            metadata.as_ref().map(|meta| {
                (
                    meta.loaded_writable_addresses.as_slice(),
                    meta.loaded_readonly_addresses.as_slice(),
                )
            }),
        )
        .with_context(|| {
            format!(
                "slot {slot} transaction {} loaded addresses are not exact",
                transaction.tx_index
            )
        })?
    } else {
        if let Some(meta) = &metadata {
            ensure!(
                meta.loaded_writable_addresses.is_empty()
                    && meta.loaded_readonly_addresses.is_empty(),
                "legacy transaction contains unsigned loaded-address metadata"
            );
        }
        LoadedAddressCounts::default()
    };
    let mut resolved_account_count = static_account_count;
    if let Some(meta) = &metadata {
        for key in meta
            .loaded_writable_addresses
            .iter()
            .chain(meta.loaded_readonly_addresses.iter())
        {
            validate_source_pubkey_visit(key, registry_entries, raw_visits)?;
            resolved_account_count = resolved_account_count
                .checked_add(1)
                .context("resolved account count overflow")?;
        }
    } else {
        ensure!(
            !is_v0 || (loaded_address_counts.writable == 0 && loaded_address_counts.readonly == 0),
            "V0 loaded addresses are unavailable"
        );
    }

    ensure!(
        transaction.signature_count > 0,
        "slot {slot} transaction {} has no signatures",
        transaction.tx_index
    );
    ensure!(
        transaction.signature_count == header.num_required_signatures,
        "slot {slot} transaction {} signature/header count mismatch",
        transaction.tx_index
    );
    ensure!(
        usize::from(header.num_required_signatures) <= static_account_count,
        "required signatures exceed static accounts"
    );
    ensure!(
        header.num_readonly_signed_accounts <= header.num_required_signatures,
        "readonly signed accounts exceed signers"
    );
    ensure!(
        usize::from(header.num_readonly_unsigned_accounts)
            <= static_account_count - usize::from(header.num_required_signatures),
        "readonly unsigned accounts exceed unsigned static accounts"
    );
    ensure!(
        exact_instruction_data.len() == hot_instructions.len(),
        "signed-message selector changed instruction count"
    );
    for hot in hot_instructions {
        ensure!(
            usize::from(hot.program_id_index) < resolved_account_count,
            "top-level program index is outside resolved accounts"
        );
        ensure!(
            hot.accounts
                .iter()
                .all(|position| usize::from(*position) < resolved_account_count),
            "top-level account index is outside resolved accounts"
        );
    }
    match metadata
        .as_ref()
        .and_then(|meta| meta.inner_instructions.as_ref())
    {
        Some(inner_sets) => {
            ensure!(
                transaction.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0,
                "metadata records CPI but the transaction flag does not"
            );
            for set in inner_sets {
                ensure!(
                    usize::try_from(set.index)
                        .ok()
                        .is_some_and(|index| index < hot_instructions.len()),
                    "CPI parent index {} is outside {} top-level instructions",
                    set.index,
                    hot_instructions.len()
                );
                for entry in &set.instructions {
                    ensure!(
                        usize::try_from(entry.program_id_index)
                            .ok()
                            .is_some_and(|position| position < resolved_account_count),
                        "CPI program index is outside resolved accounts"
                    );
                    ensure!(
                        entry
                            .accounts
                            .iter()
                            .all(|position| usize::from(*position) < resolved_account_count),
                        "CPI account index is outside resolved accounts"
                    );
                }
            }
        }
        None => ensure!(
            transaction.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX == 0,
            "transaction says CPI was recorded but metadata has no CPI field"
        ),
    }
    if let Some(meta) = &metadata {
        ensure!(
            meta.pre_balances.len() == resolved_account_count
                && meta.post_balances.len() == resolved_account_count,
            "slot {slot} transaction {} balance lanes do not match {resolved_account_count} resolved accounts",
            transaction.tx_index
        );
    }

    let mut discovered_inline_log_pubkeys = Vec::new();
    let mut inline_log_time = Duration::ZERO;
    if let Some(meta) = &metadata {
        if let Some(return_data) = &meta.return_data {
            validate_source_pubkey_visit(&return_data.program_id, registry_entries, raw_visits)?;
        }
        validate_token_balance_visits(
            &meta.pre_token_balances,
            &meta.post_token_balances,
            resolved_account_count,
            registry_entries,
            raw_visits,
        )?;
        let inline_log_started = Instant::now();
        discovered_inline_log_pubkeys = meta
            .logs
            .as_ref()
            .map(|stream| inspect_log_pubkeys(stream, Some(registry_entries)))
            .transpose()?
            .unwrap_or_default();
        inline_log_time = inline_log_started.elapsed();
        raw_visits.extend(discovered_inline_log_pubkeys.iter().copied());
        for reward in &meta.rewards {
            validate_source_pubkey_visit(&reward.pubkey, registry_entries, raw_visits)?;
        }
    }

    Ok((
        ProjectedSourceTransaction {
            payload,
            metadata,
            exact_instruction_data,
            message_decode_time,
            signed_message_proof_time,
            metadata_decode_time,
            shape: ValidatedTransactionShape {
                static_account_count: u32::try_from(static_account_count)
                    .context("static account count exceeds u32")?,
                loaded_address_counts,
                resolved_account_count,
            },
            inline_log_pubkeys: discovered_inline_log_pubkeys,
        },
        inline_log_time,
    ))
}

fn validate_source_block_tail(
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    header: &blockzilla_format::ArchiveV2HotBlockHeader,
    blockhash_registry_offset: u32,
    registry_entries: u32,
    raw_visits: &mut Vec<[u8; 32]>,
) -> Result<()> {
    let expected_current_source_id = blockhash_registry_offset
        .checked_add(row.block_id)
        .context("current source blockhash id exceeds u32")?;
    ensure!(
        header.blockhash_id == expected_current_source_id,
        "slot {} current blockhash id {} does not match indexed block {}",
        header.slot,
        header.blockhash_id,
        expected_current_source_id
    );
    if let Some(block_rewards) = &header.rewards {
        for reward in &block_rewards.decoded {
            validate_source_pubkey_visit(&reward.pubkey, registry_entries, raw_visits)?;
        }
    }
    Ok(())
}

fn validate_reconstructed_transactions(
    slot: u64,
    rows: &[blockzilla_format::ArchiveV2HotTxRow],
    reconstructed: Vec<ReconstructedSourceTransaction>,
    registry_entries: u32,
) -> Result<(Vec<ProjectedSourceTransaction>, Vec<[u8; 32]>, Duration)> {
    ensure!(
        rows.len() == reconstructed.len(),
        "slot {slot} reconstructed transaction count mismatch"
    );
    let mut projected = Vec::with_capacity(reconstructed.len());
    let mut raw_pubkey_visits = Vec::new();
    let mut inline_log_pubkey_discovery_time = Duration::ZERO;
    for (transaction, reconstructed) in rows.iter().zip(reconstructed) {
        let (transaction, inline_time) = validate_and_project_source_transaction(
            slot,
            transaction,
            reconstructed,
            registry_entries,
            &mut raw_pubkey_visits,
        )?;
        inline_log_pubkey_discovery_time =
            inline_log_pubkey_discovery_time.saturating_add(inline_time);
        projected.push(transaction);
    }
    Ok((
        projected,
        raw_pubkey_visits,
        inline_log_pubkey_discovery_time,
    ))
}

struct TargetPubkeyDictionary {
    source_index: FileBackedKeyIndex,
    source_registry: File,
    source_records: u32,
    appended_by_key: BTreeMap<[u8; 32], u32>,
    appended_bytes: Vec<u8>,
}

impl TargetPubkeyDictionary {
    fn open(
        source: &PinnedLocalRangeSource,
        source_root: &Path,
        source_records: u32,
    ) -> Result<Self> {
        let label = source_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        let validation_index = KeyIndex::load_file(
            source
                .pinned_file_clone(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)?
                .context("source registry lookup is missing")?,
            &label,
        )
        .context("load pinned source registry lookup")?;
        ensure!(
            validation_index.len() == source_records as usize,
            "source registry lookup has {} records but registry.bin has {source_records}",
            validation_index.len()
        );
        let registry_label = source_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
        let registry_file = source
            .pinned_file_clone(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)?
            .context("source registry is missing")?;
        let mut registry_reader = BufReader::with_capacity(
            8 << 20,
            registry_file
                .try_clone()
                .context("clone source registry for sequential validation")?,
        );
        let mut records_left = source_records;
        let mut first_id = 1_u32;
        let mut buffer = vec![0_u8; 8 << 20];
        while records_left != 0 {
            let batch_records = records_left.min((buffer.len() / 32) as u32);
            let batch_bytes = batch_records as usize * 32;
            registry_reader
                .read_exact(&mut buffer[..batch_bytes])
                .with_context(|| {
                    format!("read source registry from {}", registry_label.display())
                })?;
            for (index, bytes) in buffer[..batch_bytes].chunks_exact(32).enumerate() {
                let key: &[u8; 32] = bytes.try_into().expect("32-byte registry record");
                let expected_id = first_id
                    .checked_add(u32::try_from(index).expect("validation batch fits u32"))
                    .context("source registry ID overflow")?;
                ensure!(
                    validation_index.lookup(key) == Some(expected_id),
                    "source registry lookup does not map record {expected_id} to its exact ID"
                );
            }
            first_id = first_id
                .checked_add(batch_records)
                .context("source registry ID overflow")?;
            records_left -= batch_records;
        }
        drop(validation_index);
        let source_index = FileBackedKeyIndex::load_file(
            source
                .pinned_file_clone(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)?
                .context("source registry lookup is missing")?,
            &label,
        )
        .context("open file-backed source registry lookup")?;
        Ok(Self {
            source_index,
            source_registry: registry_file,
            source_records,
            appended_by_key: BTreeMap::new(),
            appended_bytes: Vec::new(),
        })
    }

    fn resolve_or_intern(&mut self, key: &CompactPubkey) -> Result<u32> {
        self.resolve_or_intern_inner(key)
    }

    fn resolve_or_intern_inner(&mut self, key: &CompactPubkey) -> Result<u32> {
        match key {
            CompactPubkey::Id(_) => source_registry_id(key, self.source_records),
            CompactPubkey::Raw(bytes) => {
                if let Some(id) = self.source_index.lookup(bytes)? {
                    let mut exact = [0_u8; 32];
                    self.source_registry
                        .read_exact_at(&mut exact, u64::from(id - 1) * 32)
                        .with_context(|| format!("verify source registry id {id}"))?;
                    if exact == *bytes {
                        return Ok(id);
                    }
                }
                if let Some(id) = self.appended_by_key.get(bytes) {
                    return Ok(*id);
                }
                let appended = u32::try_from(self.appended_by_key.len())
                    .context("target pubkey dictionary exceeds u32 records")?;
                let id = self
                    .source_records
                    .checked_add(appended)
                    .and_then(|last| last.checked_add(1))
                    .context("target pubkey dictionary exceeds u32 records")?;
                self.appended_by_key.insert(*bytes, id);
                self.appended_bytes.extend_from_slice(bytes);
                Ok(id)
            }
        }
    }

    fn record_count(&self) -> u32 {
        self.source_records
            + u32::try_from(self.appended_by_key.len())
                .expect("resolve_or_intern keeps the count in u32")
    }

    fn appended_bytes(&self) -> &[u8] {
        &self.appended_bytes
    }
}

trait TargetPubkeyIdLookup {
    fn target_pubkey_id(&mut self, key: &CompactPubkey) -> Result<u32>;
}

impl TargetPubkeyIdLookup for TargetPubkeyDictionary {
    fn target_pubkey_id(&mut self, key: &CompactPubkey) -> Result<u32> {
        self.resolve_or_intern(key)
    }
}

/// Per-block immutable replay table for public-key IDs assigned by the
/// ordered stage. Source IDs need no entry; raw keys keep their exact assigned
/// ID so workers never mutate the global dictionary.
#[derive(Default)]
struct ResolvedPubkeyTable {
    source_records: u32,
    raw_ids: BTreeMap<[u8; 32], u32>,
}

impl ResolvedPubkeyTable {
    fn new(source_records: u32) -> Self {
        Self {
            source_records,
            raw_ids: BTreeMap::new(),
        }
    }

    fn record(&mut self, key: &CompactPubkey, id: u32) -> Result<()> {
        match key {
            CompactPubkey::Id(_) => ensure!(
                source_registry_id(key, self.source_records)? == id,
                "resolved source public-key ID changed while recording a block"
            ),
            CompactPubkey::Raw(bytes) => {
                if let Some(previous) = self.raw_ids.insert(*bytes, id) {
                    ensure!(
                        previous == id,
                        "raw public key received two target dictionary IDs"
                    );
                }
            }
        }
        Ok(())
    }

    fn resolve(&self, key: &CompactPubkey) -> Result<u32> {
        match key {
            CompactPubkey::Id(_) => source_registry_id(key, self.source_records),
            CompactPubkey::Raw(bytes) => self
                .raw_ids
                .get(bytes)
                .copied()
                .context("stage-3 public key was not assigned by the ordered stage"),
        }
    }
}

impl TargetPubkeyIdLookup for ResolvedPubkeyTable {
    fn target_pubkey_id(&mut self, key: &CompactPubkey) -> Result<u32> {
        self.resolve(key)
    }
}

fn resolve_and_record_pubkey(
    target: &mut TargetPubkeyDictionary,
    resolved: &mut ResolvedPubkeyTable,
    key: &CompactPubkey,
) -> Result<u32> {
    let id = target.resolve_or_intern(key)?;
    resolved.record(key, id)?;
    Ok(id)
}

#[derive(Default)]
struct BlockConversionDelta {
    transactions: u64,
    top_level_instructions: u64,
    inner_instructions: u64,
    account_references: u64,
    cpi_not_recorded: u64,
    raw_account_keys: u64,
    nonce_blockhashes: u64,
    instruction_data_variants: BTreeMap<String, u64>,
    instructions_bytes_retained: u64,
    instructions_bytes_rederived: u64,
    retained_payload_bytes: u64,
    instruction_data_bytes: u64,
    inner_instruction_data_bytes: u64,
    token_balances_paired: u64,
    token_balances_total: u64,
    block_rewards_stored: u64,
    account_flags: BTreeMap<u32, u8>,
    effect_record_counts: [u64; transactions_codec::EFFECT_KIND_COUNT],
    nonce_hashes: BTreeSet<[u8; 32]>,
    max_non_poh_hash_ref: Option<u32>,
    max_poh_hash_ref: Option<u32>,
}

impl BlockConversionDelta {
    fn add_counter(target: &mut u64, value: u64, label: &str) -> Result<()> {
        *target = target
            .checked_add(value)
            .with_context(|| format!("{label} counter overflow"))?;
        Ok(())
    }

    fn mark_account(&mut self, id: u32, flags: u8) -> Result<()> {
        ensure!(id != 0, "account flag uses reserved public-key ID zero");
        let entry = self.account_flags.entry(id).or_default();
        *entry |= flags;
        Ok(())
    }

    fn observe_hash(&mut self, ordinal: u32, poh_owned: bool) {
        let target = if poh_owned {
            &mut self.max_poh_hash_ref
        } else {
            &mut self.max_non_poh_hash_ref
        };
        *target = Some(target.map_or(ordinal, |previous| previous.max(ordinal)));
    }

    fn merge(
        self,
        report: &mut Report,
        observed_account_flags: &mut Vec<u8>,
        effect_record_counts: &mut [u64; transactions_codec::EFFECT_KIND_COUNT],
        nonce_hashes: &mut BTreeSet<[u8; 32]>,
        max_non_poh_hash_ref: &mut Option<u32>,
        max_poh_hash_ref: &mut Option<u32>,
    ) -> Result<()> {
        for (target, value, label) in [
            (&mut report.transactions, self.transactions, "transactions"),
            (
                &mut report.top_level_instructions,
                self.top_level_instructions,
                "top-level instructions",
            ),
            (
                &mut report.inner_instructions,
                self.inner_instructions,
                "inner instructions",
            ),
            (
                &mut report.account_references,
                self.account_references,
                "account references",
            ),
            (
                &mut report.cpi_not_recorded,
                self.cpi_not_recorded,
                "CPI not recorded",
            ),
            (
                &mut report.raw_account_keys,
                self.raw_account_keys,
                "raw account keys",
            ),
            (
                &mut report.nonce_blockhashes,
                self.nonce_blockhashes,
                "nonce blockhashes",
            ),
            (
                &mut report.instructions_bytes_retained,
                self.instructions_bytes_retained,
                "retained instructions",
            ),
            (
                &mut report.instructions_bytes_rederived,
                self.instructions_bytes_rederived,
                "rederived instructions",
            ),
            (
                &mut report.retained_payload_bytes,
                self.retained_payload_bytes,
                "retained payload bytes",
            ),
            (
                &mut report.instruction_data_bytes,
                self.instruction_data_bytes,
                "instruction data bytes",
            ),
            (
                &mut report.inner_instruction_data_bytes,
                self.inner_instruction_data_bytes,
                "inner-instruction data bytes",
            ),
            (
                &mut report.token_balances_paired,
                self.token_balances_paired,
                "paired token balances",
            ),
            (
                &mut report.token_balances_total,
                self.token_balances_total,
                "token balances",
            ),
            (
                &mut report.block_rewards_stored,
                self.block_rewards_stored,
                "stored block rewards",
            ),
        ] {
            Self::add_counter(target, value, label)?;
        }
        for (name, value) in self.instruction_data_variants {
            Self::add_counter(
                report.instruction_data_variants.entry(name).or_default(),
                value,
                "instruction-data variant",
            )?;
        }
        for (id, flags) in self.account_flags {
            observed_account_flags.resize(
                observed_account_flags
                    .len()
                    .max(account_flags::byte_len(id)),
                0,
            );
            account_flags::set_flags(observed_account_flags, id, flags)
                .context("merge sparse account flags")?;
        }
        for (target, value) in effect_record_counts
            .iter_mut()
            .zip(self.effect_record_counts)
        {
            Self::add_counter(target, value, "effect records")?;
        }
        nonce_hashes.extend(self.nonce_hashes);
        if let Some(value) = self.max_non_poh_hash_ref {
            *max_non_poh_hash_ref = Some(max_non_poh_hash_ref.map_or(value, |old| old.max(value)));
        }
        if let Some(value) = self.max_poh_hash_ref {
            *max_poh_hash_ref = Some(max_poh_hash_ref.map_or(value, |old| old.max(value)));
        }
        Ok(())
    }
}

fn source_pubkey_bytes(
    key: &CompactPubkey,
    registry: &File,
    registry_entries: u32,
) -> Result<[u8; 32]> {
    match key {
        CompactPubkey::Raw(bytes) => Ok(*bytes),
        CompactPubkey::Id(id) => {
            source_registry_id(key, registry_entries)?;
            let offset = u64::from(id - 1)
                .checked_mul(32)
                .context("pubkey source offset overflow")?;
            let mut bytes = [0_u8; 32];
            registry
                .read_exact_at(&mut bytes, offset)
                .with_context(|| format!("read pubkey registry id {id}"))?;
            Ok(bytes)
        }
    }
}

fn source_signature(signatures: &File, ordinal: u64) -> Result<Signature> {
    let offset = ordinal
        .checked_mul(64)
        .context("source signature offset overflow")?;
    let mut bytes = [0_u8; 64];
    signatures
        .read_exact_at(&mut bytes, offset)
        .with_context(|| format!("read source signature ordinal {ordinal}"))?;
    Ok(Signature::from(bytes))
}

#[derive(Clone, Copy)]
struct SignedMessageProofContext<'a> {
    registry: &'a File,
    registry_entries: u32,
    signatures: &'a File,
    source_hashes: &'a BlockhashResolver,
    vote_hashes: Option<&'a VoteHashRegistry>,
}

fn reconstruct_signed_instruction_data(
    payload: &ArchiveV2HotMessagePayload,
    header: blockzilla_format::CompactMessageHeader,
    account_keys: &[CompactPubkey],
    recent_blockhash: &OwnedCompactRecentBlockhash,
    hot_instructions: &[ArchiveV2HotInstruction],
    transaction_signature_ordinal: u64,
    proof: SignedMessageProofContext<'_>,
) -> Result<Vec<Vec<u8>>> {
    let static_account_keys = account_keys
        .iter()
        .map(|key| source_pubkey_bytes(key, proof.registry, proof.registry_entries))
        .collect::<Result<Vec<_>>>()?;
    let recent_blockhash = match recent_blockhash {
        OwnedCompactRecentBlockhash::Id(id) => proof.source_hashes.resolve(*id)?,
        OwnedCompactRecentBlockhash::Nonce(hash) => *hash,
    };
    let instruction_candidates = hot_instructions
        .iter()
        .map(|instruction| {
            Ok((
                instruction.program_id_index,
                instruction.accounts.as_slice(),
                reconstruct_instruction_data_candidates(
                    &instruction.data,
                    proof.vote_hashes.map(|resolver| resolver as _),
                )?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let candidate_views = instruction_candidates
        .iter()
        .map(
            |(program_id_index, accounts, candidates)| SignedInstructionCandidates {
                program_id_index: *program_id_index,
                accounts,
                data_candidates: candidates,
            },
        )
        .collect::<Vec<_>>();
    let resolved_lookups = match payload {
        ArchiveV2HotMessagePayload::Legacy(_) | ArchiveV2HotMessagePayload::V1(_) => Vec::new(),
        ArchiveV2HotMessagePayload::V0(message) => message
            .address_table_lookups
            .iter()
            .map(|lookup| {
                Ok(ResolvedAddressTableLookup {
                    account_key: source_pubkey_bytes(
                        &lookup.account_key,
                        proof.registry,
                        proof.registry_entries,
                    )?,
                    writable_indexes: &lookup.writable_indexes,
                    readonly_indexes: &lookup.readonly_indexes,
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    let version = match payload {
        ArchiveV2HotMessagePayload::Legacy(_) => SignedMessageVersion::Legacy,
        ArchiveV2HotMessagePayload::V0(_) => SignedMessageVersion::V0 {
            address_table_lookups: &resolved_lookups,
        },
        ArchiveV2HotMessagePayload::V1(message) => SignedMessageVersion::V1 {
            config: SignedTransactionConfig {
                priority_fee: message.config.priority_fee,
                compute_unit_limit: message.config.compute_unit_limit,
                loaded_accounts_data_size_limit: message.config.loaded_accounts_data_size_limit,
                heap_size: message.config.heap_size,
            },
        },
    };
    let signer = static_account_keys
        .first()
        .context("signed message has no fee-payer key")?;
    let signature = source_signature(proof.signatures, transaction_signature_ordinal)?;
    let selected = select_signed_message_candidate(
        &SignedMessageCandidates {
            version,
            header,
            static_account_keys: &static_account_keys,
            recent_blockhash,
            instructions: &candidate_views,
        },
        MAX_SIGNED_MESSAGE_CANDIDATE_COMBINATIONS,
        |message| signature.verify(signer, message),
    )
    .context("select instruction bytes with the fee-payer signature")?;
    Ok(selected.instruction_data)
}

/// Pair `pre` and `post` token balances by the account they describe.
///
/// V2 stores two independent lists whose entries repeat `account_index`,
/// `mint`, `owner`, `program_id` and `decimals` verbatim; only `amount`
/// differs. Pairing on `account_index` stores that identity once. An account
/// on only one side keeps that side's identity and leaves the other amount
/// absent, which is distinct from an amount of zero.
fn pair_token_balances<R: TargetPubkeyIdLookup>(
    pre: &[CompactTokenBalance],
    post: &[CompactTokenBalance],
    report: &mut BlockConversionDelta,
    pubkeys: &mut R,
    resolved_account_count: usize,
) -> Result<Vec<token_balances_codec::TokenBalance>> {
    let mut by_index: BTreeMap<u32, token_balances_codec::TokenBalance> = BTreeMap::new();
    for balance in pre {
        ensure!(
            (balance.account_index as usize) < resolved_account_count,
            "pre-token-balance account index {} is outside {resolved_account_count} resolved accounts",
            balance.account_index
        );
        let replaced = by_index.insert(
            balance.account_index,
            token_balances_codec::TokenBalance {
                account_index: balance.account_index,
                mint: compact_optional_id(balance.mint.as_ref(), pubkeys)?,
                owner: compact_optional_id(balance.owner.as_ref(), pubkeys)?,
                program_id: compact_optional_id(balance.program_id.as_ref(), pubkeys)?,
                decimals: balance.decimals,
                post_identity: None,
                pre: Some(balance.amount),
                post: None,
            },
        );
        ensure!(
            replaced.is_none(),
            "duplicate pre-token-balance account index {}",
            balance.account_index
        );
    }
    for balance in post {
        ensure!(
            (balance.account_index as usize) < resolved_account_count,
            "post-token-balance account index {} is outside {resolved_account_count} resolved accounts",
            balance.account_index
        );
        match by_index.get_mut(&balance.account_index) {
            Some(existing) => {
                let post_identity = token_balances_codec::TokenBalanceIdentity {
                    mint: compact_optional_id(balance.mint.as_ref(), pubkeys)?,
                    owner: compact_optional_id(balance.owner.as_ref(), pubkeys)?,
                    program_id: compact_optional_id(balance.program_id.as_ref(), pubkeys)?,
                    decimals: balance.decimals,
                };
                let identity_changed = existing.mint != post_identity.mint
                    || existing.owner != post_identity.owner
                    || existing.program_id != post_identity.program_id
                    || existing.decimals != post_identity.decimals;
                ensure!(
                    existing.post.is_none(),
                    "duplicate post-token-balance account index {}",
                    balance.account_index
                );
                existing.post_identity = identity_changed.then_some(post_identity);
                existing.post = Some(balance.amount);
                BlockConversionDelta::add_counter(
                    &mut report.token_balances_paired,
                    1,
                    "paired token balances",
                )?;
            }
            None => {
                by_index.insert(
                    balance.account_index,
                    token_balances_codec::TokenBalance {
                        account_index: balance.account_index,
                        mint: compact_optional_id(balance.mint.as_ref(), pubkeys)?,
                        owner: compact_optional_id(balance.owner.as_ref(), pubkeys)?,
                        program_id: compact_optional_id(balance.program_id.as_ref(), pubkeys)?,
                        decimals: balance.decimals,
                        post_identity: None,
                        pre: None,
                        post: Some(balance.amount),
                    },
                );
            }
        }
    }
    BlockConversionDelta::add_counter(
        &mut report.token_balances_total,
        by_index.len() as u64,
        "token balances",
    )?;
    Ok(by_index.into_values().collect())
}

fn compact_reward<R: TargetPubkeyIdLookup>(
    reward: &blockzilla_format::CompactReward,
    pubkeys: &mut R,
) -> Result<rewards_codec::Reward> {
    Ok(rewards_codec::Reward {
        pubkey_id: pubkeys
            .target_pubkey_id(&reward.pubkey)
            .context("reward recipient is not in the canonical dictionary")?,
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: reward.reward_type,
        commission: reward.commission,
    })
}

fn compact_optional_id<R: TargetPubkeyIdLookup>(
    key: Option<&CompactPubkey>,
    pubkeys: &mut R,
) -> Result<Option<u32>> {
    key.map(|key| pubkeys.target_pubkey_id(key)).transpose()
}

struct TrackingPubkeyResolver<'a, R: ?Sized> {
    inner: &'a R,
    ids: RefCell<BTreeSet<u32>>,
}

impl<'a, R: ?Sized> TrackingPubkeyResolver<'a, R> {
    fn new(inner: &'a R) -> Self {
        Self {
            inner,
            ids: RefCell::new(BTreeSet::new()),
        }
    }
}

impl<R: PubkeyResolver + ?Sized> PubkeyResolver for TrackingPubkeyResolver<'_, R> {
    fn resolve_pubkey(&self, id: u32) -> Option<[u8; 32]> {
        let key = self.inner.resolve_pubkey(id)?;
        self.ids.borrow_mut().insert(id);
        Some(key)
    }
}

fn tokenize_log_line(line: String, candidates: &[(u32, String)]) -> logs_codec::LogLine {
    let mut fragments = Vec::new();
    let mut pubkey_ids = Vec::new();
    let mut cursor = 0_usize;
    while cursor < line.len() {
        let mut best: Option<(usize, usize, u32)> = None;
        for (id, pubkey) in candidates {
            let Some(relative) = line[cursor..].find(pubkey) else {
                continue;
            };
            let start = cursor + relative;
            let candidate = (start, pubkey.len(), *id);
            if best.is_none_or(|current| {
                candidate.0 < current.0 || (candidate.0 == current.0 && candidate.1 > current.1)
            }) {
                best = Some(candidate);
            }
        }
        let Some((start, len, id)) = best else {
            break;
        };
        fragments.push(line[cursor..start].to_owned());
        pubkey_ids.push(id);
        cursor = start + len;
    }
    fragments.push(line[cursor..].to_owned());
    logs_codec::LogLine {
        fragments,
        pubkey_ids,
    }
}

fn inspect_log_pubkeys(
    stream: &CompactLogStream,
    registry_entries: Option<u32>,
) -> Result<Vec<[u8; 32]>> {
    let mut keys = Vec::new();
    stream.try_for_each_pubkey(|pubkey| {
        match pubkey {
            CompactPubkey::Raw(bytes) => keys.push(bytes),
            CompactPubkey::Id(id) => {
                if let Some(registry_entries) = registry_entries {
                    ensure!(
                        id > 0 && id <= registry_entries,
                        "CompactLogStream pubkey registry id {id} is outside the valid 1..={registry_entries} range"
                    );
                }
            }
        }
        Ok(())
    })?;
    keys.sort_unstable();
    keys.dedup();
    Ok(keys)
}

#[cfg(test)]
fn log_pubkey_visits_json_oracle(stream: &CompactLogStream) -> Result<Vec<CompactPubkey>> {
    fn visit(value: &serde_json::Value, pubkeys: &mut Vec<CompactPubkey>) -> Result<()> {
        match value {
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(value, pubkeys)?;
                }
            }
            serde_json::Value::Object(fields) => {
                if let Some(raw) = fields.get("Raw") {
                    let bytes = raw
                        .as_array()
                        .context("CompactLogStream Raw pubkey is not a byte array")?;
                    ensure!(
                        bytes.len() == 32,
                        "CompactLogStream Raw pubkey has {} bytes, expected 32",
                        bytes.len()
                    );
                    let mut key = [0_u8; 32];
                    for (target, value) in key.iter_mut().zip(bytes) {
                        *target = u8::try_from(
                            value
                                .as_u64()
                                .context("CompactLogStream Raw pubkey byte is not an integer")?,
                        )
                        .context("CompactLogStream Raw pubkey byte exceeds u8")?;
                    }
                    pubkeys.push(CompactPubkey::Raw(key));
                    return Ok(());
                }
                if let Some(id) = fields.get("Id") {
                    let id = u32::try_from(
                        id.as_u64()
                            .context("CompactLogStream Id pubkey is not an integer")?,
                    )
                    .context("CompactLogStream Id pubkey exceeds u32")?;
                    pubkeys.push(CompactPubkey::Id(id));
                    return Ok(());
                }
                for value in fields.values() {
                    visit(value, pubkeys)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    let value = serde_json::to_value(stream).context("inspect CompactLogStream pubkeys")?;
    let mut pubkeys = Vec::new();
    visit(&value, &mut pubkeys)?;
    Ok(pubkeys)
}

#[cfg(test)]
fn inspect_log_pubkeys_json_oracle(
    stream: &CompactLogStream,
    registry_entries: Option<u32>,
) -> Result<Vec<[u8; 32]>> {
    let mut keys = Vec::new();
    for pubkey in log_pubkey_visits_json_oracle(stream)? {
        match pubkey {
            CompactPubkey::Raw(bytes) => keys.push(bytes),
            CompactPubkey::Id(id) => {
                if let Some(registry_entries) = registry_entries {
                    ensure!(
                        id > 0 && id <= registry_entries,
                        "CompactLogStream pubkey registry id {id} is outside the valid 1..={registry_entries} range"
                    );
                }
            }
        }
    }
    keys.sort_unstable();
    keys.dedup();
    Ok(keys)
}

#[cfg(test)]
fn inline_log_pubkeys(stream: &CompactLogStream) -> Result<Vec<[u8; 32]>> {
    inspect_log_pubkeys(stream, None)
}

#[cfg(test)]
fn convert_log_stream<R: TargetPubkeyIdLookup>(
    stream: &CompactLogStream,
    resolver: &PinnedPubkeyResolver,
    pubkeys: &mut R,
) -> Result<Vec<logs_codec::LogLine>> {
    let inline_pubkeys = inline_log_pubkeys(stream)?;
    convert_log_stream_with_inline_pubkeys(stream, resolver, pubkeys, &inline_pubkeys)
}

fn convert_log_stream_with_inline_pubkeys<R: TargetPubkeyIdLookup>(
    stream: &CompactLogStream,
    resolver: &PinnedPubkeyResolver,
    pubkeys: &mut R,
    inline_pubkeys: &[[u8; 32]],
) -> Result<Vec<logs_codec::LogLine>> {
    let tracking = TrackingPubkeyResolver::new(resolver);
    let rendered = catch_unwind(AssertUnwindSafe(|| render_logs(stream, &tracking)))
        .map_err(|_| anyhow::anyhow!("Compact V2 log stream contains an invalid reference"))?;
    let mut candidates_by_id = BTreeMap::new();
    for id in tracking.ids.into_inner() {
        let bytes = resolver
            .resolve_pubkey(id)
            .with_context(|| format!("log pubkey id {id} is outside the source dictionary"))?;
        candidates_by_id.insert(id, Pubkey::new_from_array(bytes).to_string());
    }
    for bytes in inline_pubkeys {
        let id = pubkeys.target_pubkey_id(&CompactPubkey::Raw(*bytes))?;
        candidates_by_id.insert(id, Pubkey::new_from_array(*bytes).to_string());
    }
    let candidates = candidates_by_id.into_iter().collect::<Vec<_>>();
    Ok(rendered
        .into_iter()
        .map(|line| tokenize_log_line(line, &candidates))
        .collect())
}

fn convert_message(
    payload: &ArchiveV2HotMessagePayload,
) -> (
    &blockzilla_format::CompactMessageHeader,
    &[CompactPubkey],
    &OwnedCompactRecentBlockhash,
    &[ArchiveV2HotInstruction],
    bool,
) {
    match payload {
        ArchiveV2HotMessagePayload::Legacy(message) => (
            &message.header,
            &message.account_keys,
            &message.recent_blockhash,
            &message.instructions,
            false,
        ),
        ArchiveV2HotMessagePayload::V0(message) => (
            &message.header,
            &message.account_keys,
            &message.recent_blockhash,
            &message.instructions,
            true,
        ),
        ArchiveV2HotMessagePayload::V1(message) => (
            &message.header,
            &message.account_keys,
            &message.recent_blockhash,
            &message.instructions,
            true,
        ),
    }
}

fn convert_resolved_block(
    input: ResolvedBlockInput,
    log_pubkeys: &PinnedPubkeyResolver,
) -> Result<ConvertedResolvedBlock> {
    let ResolvedBlockInput {
        row,
        block,
        transactions: resolved_transactions,
        mut pubkeys,
        blockhash,
        previous_blockhash,
        mut delta,
    } = input;
    ensure!(
        block.tx_rows.len() == resolved_transactions.len(),
        "slot {} resolved transaction count does not match source rows",
        block.header.slot
    );

    let mut transactions = Vec::with_capacity(block.tx_count as usize);
    let mut effect_states = Vec::with_capacity(block.tx_count as usize);
    let mut inner: Vec<Option<(inner_instructions::TransactionInner, usize, usize)>> =
        Vec::with_capacity(block.tx_count as usize);
    let mut outcomes: Vec<Option<outcomes_codec::TransactionOutcome>> =
        Vec::with_capacity(block.tx_count as usize);
    let mut balances: Vec<Option<balances_plane_codec::Balances>> =
        Vec::with_capacity(block.tx_count as usize);
    let mut token_balances: Vec<Option<Vec<token_balances_codec::TokenBalance>>> =
        Vec::with_capacity(block.tx_count as usize);
    let mut logs: Vec<Option<Vec<logs_codec::LogLine>>> =
        Vec::with_capacity(block.tx_count as usize);
    let mut rewards: Vec<Option<Vec<rewards_codec::Reward>>> =
        Vec::with_capacity(block.tx_count as usize);
    let mut transaction_signature_ordinal = row.first_signature_ordinal;

    for (tx, resolved) in block.tx_rows.iter().zip(resolved_transactions) {
        BlockConversionDelta::add_counter(&mut delta.transactions, 1, "transactions")?;
        let ResolvedSourceTransaction {
            payload,
            metadata,
            exact_instruction_data,
            recent_blockhash,
            inline_log_pubkeys,
            shape,
        } = resolved;
        let (header, account_keys, _, hot_instructions, is_v0) = convert_message(&payload);
        validate_source_transaction_flags(tx, &payload, metadata.as_ref())?;
        ensure!(
            tx.signature_count > 0 && tx.signature_count == header.num_required_signatures,
            "slot {} transaction {} signature/header count mismatch",
            block.header.slot,
            tx.tx_index
        );
        ensure!(
            usize::from(header.num_required_signatures)
                <= usize::try_from(shape.static_account_count)?,
            "required signatures exceed static accounts"
        );
        ensure!(
            header.num_readonly_signed_accounts <= header.num_required_signatures,
            "readonly signed accounts exceed signers"
        );
        ensure!(
            usize::from(header.num_readonly_unsigned_accounts)
                <= usize::try_from(shape.static_account_count)?
                    - usize::from(header.num_required_signatures),
            "readonly unsigned accounts exceed unsigned static accounts"
        );
        let checked_loaded_counts = if let ArchiveV2HotMessagePayload::V0(message) = &payload {
            validate_v0_loaded_address_counts(
                message,
                metadata.as_ref().map(|meta| {
                    (
                        meta.loaded_writable_addresses.as_slice(),
                        meta.loaded_readonly_addresses.as_slice(),
                    )
                }),
            )?
        } else {
            if let Some(meta) = &metadata {
                ensure!(
                    meta.loaded_writable_addresses.is_empty()
                        && meta.loaded_readonly_addresses.is_empty(),
                    "legacy transaction contains unsigned loaded-address metadata"
                );
            }
            LoadedAddressCounts::default()
        };
        ensure!(
            checked_loaded_counts == shape.loaded_address_counts,
            "source-validated loaded-address counts drifted"
        );
        ensure!(
            exact_instruction_data.len() == hot_instructions.len(),
            "signed-message selector changed instruction count"
        );
        for hot in hot_instructions {
            ensure!(
                usize::from(hot.program_id_index) < shape.resolved_account_count,
                "top-level program index is outside resolved accounts"
            );
            ensure!(
                hot.accounts
                    .iter()
                    .all(|position| { usize::from(*position) < shape.resolved_account_count }),
                "top-level account index is outside resolved accounts"
            );
        }
        match metadata
            .as_ref()
            .and_then(|meta| meta.inner_instructions.as_ref())
        {
            Some(inner_sets) => {
                ensure!(
                    tx.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0,
                    "metadata records CPI but the transaction flag does not"
                );
                for set in inner_sets {
                    ensure!(
                        usize::try_from(set.index)
                            .ok()
                            .is_some_and(|index| index < hot_instructions.len()),
                        "CPI parent index {} is outside {} top-level instructions",
                        set.index,
                        hot_instructions.len()
                    );
                    for entry in &set.instructions {
                        ensure!(
                            usize::try_from(entry.program_id_index)
                                .ok()
                                .is_some_and(|position| position < shape.resolved_account_count),
                            "CPI program index is outside resolved accounts"
                        );
                        ensure!(
                            entry.accounts.iter().all(|position| {
                                usize::from(*position) < shape.resolved_account_count
                            }),
                            "CPI account index is outside resolved accounts"
                        );
                    }
                }
            }
            None => ensure!(
                tx.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX == 0,
                "transaction says CPI was recorded but metadata has no CPI field"
            ),
        }
        if let Some(meta) = &metadata {
            ensure!(
                meta.pre_balances.len() == shape.resolved_account_count
                    && meta.post_balances.len() == shape.resolved_account_count,
                "slot {} transaction {} balance lanes do not match {} resolved accounts",
                block.header.slot,
                tx.tx_index,
                shape.resolved_account_count
            );
        }
        let tx_lookups = match &payload {
            ArchiveV2HotMessagePayload::V0(message) => message
                .address_table_lookups
                .iter()
                .map(|lookup| {
                    Ok(transactions_codec::AddressTableLookup {
                        table_id: transactions_codec::PubkeyId::new(
                            pubkeys
                                .target_pubkey_id(&lookup.account_key)
                                .context("lookup-table key was not resolved by ordered mapping")?,
                        )?,
                        writable_indexes: lookup.writable_indexes.clone(),
                        readonly_indexes: lookup.readonly_indexes.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            ArchiveV2HotMessagePayload::Legacy(_) | ArchiveV2HotMessagePayload::V1(_) => Vec::new(),
        };

        let mut ids = Vec::with_capacity(account_keys.len());
        for key in account_keys {
            BlockConversionDelta::add_counter(
                &mut delta.raw_account_keys,
                u64::from(matches!(key, CompactPubkey::Raw(_))),
                "raw account keys",
            )?;
            ids.push(pubkeys.target_pubkey_id(key)?);
        }
        let static_count = u32::try_from(ids.len()).context("static account count exceeds u32")?;
        ensure!(
            static_count == shape.static_account_count,
            "source-validated static account count drifted"
        );
        let loaded_counts = shape.loaded_address_counts;
        let (mut loaded_writable_count, mut loaded_readonly_count) = (0, 0);
        if let Some(meta) = &metadata {
            loaded_writable_count = u32::try_from(meta.loaded_writable_addresses.len())
                .context("loaded writable count exceeds u32")?;
            loaded_readonly_count = u32::try_from(meta.loaded_readonly_addresses.len())
                .context("loaded readonly count exceeds u32")?;
            for key in meta
                .loaded_writable_addresses
                .iter()
                .chain(meta.loaded_readonly_addresses.iter())
            {
                BlockConversionDelta::add_counter(
                    &mut delta.raw_account_keys,
                    u64::from(matches!(key, CompactPubkey::Raw(_))),
                    "raw account keys",
                )?;
                ids.push(pubkeys.target_pubkey_id(key)?);
            }
        } else {
            ensure!(
                !is_v0 || (loaded_counts.writable == 0 && loaded_counts.readonly == 0),
                "V0 loaded addresses are unavailable"
            );
        }
        ensure!(
            ids.len() == shape.resolved_account_count,
            "source-validated resolved account count drifted"
        );
        BlockConversionDelta::add_counter(
            &mut delta.account_references,
            ids.len() as u64,
            "account references",
        )?;

        ensure!(
            exact_instruction_data.len() == hot_instructions.len(),
            "signed-message selector changed instruction count"
        );
        let mut instruction_rows = Vec::with_capacity(hot_instructions.len());
        for (hot, data) in hot_instructions.iter().zip(exact_instruction_data) {
            let (name, retains) = variant_name(&hot.data);
            BlockConversionDelta::add_counter(
                delta
                    .instruction_data_variants
                    .entry(name.to_owned())
                    .or_default(),
                1,
                "instruction-data variant",
            )?;
            if retains {
                BlockConversionDelta::add_counter(
                    &mut delta.instructions_bytes_retained,
                    1,
                    "retained instructions",
                )?;
                BlockConversionDelta::add_counter(
                    &mut delta.retained_payload_bytes,
                    u64::from(retained_data_len(&hot.data).unwrap_or(0)),
                    "retained payload bytes",
                )?;
            } else {
                BlockConversionDelta::add_counter(
                    &mut delta.instructions_bytes_rederived,
                    1,
                    "rederived instructions",
                )?;
            }
            ensure!(
                usize::from(hot.program_id_index) < ids.len(),
                "top-level program index is outside resolved accounts"
            );
            ensure!(
                hot.accounts
                    .iter()
                    .all(|position| usize::from(*position) < ids.len()),
                "top-level account index is outside resolved accounts"
            );
            BlockConversionDelta::add_counter(
                &mut delta.instruction_data_bytes,
                data.len() as u64,
                "instruction data bytes",
            )?;
            delta.mark_account(
                ids[usize::from(hot.program_id_index)],
                account_flags::FLAG_PROGRAM,
            )?;
            instruction_rows.push(transactions_codec::Instruction {
                program_position: u32::from(hot.program_id_index),
                account_positions: hot
                    .accounts
                    .iter()
                    .map(|position| u32::from(*position))
                    .collect(),
                data,
            });
        }
        BlockConversionDelta::add_counter(
            &mut delta.top_level_instructions,
            instruction_rows.len() as u64,
            "top-level instructions",
        )?;
        let top_level_instruction_count = instruction_rows.len();
        let resolved_account_count = ids.len();

        let mut transaction_inner = inner_instructions::TransactionInner::default();
        let cpi_state = match metadata
            .as_ref()
            .and_then(|meta| meta.inner_instructions.as_ref())
        {
            Some(inner_sets) => {
                let mut paired_groups = Vec::with_capacity(inner_sets.len());
                for set in inner_sets {
                    let mut list = Vec::with_capacity(set.instructions.len());
                    for entry in &set.instructions {
                        BlockConversionDelta::add_counter(
                            &mut delta.inner_instruction_data_bytes,
                            entry.data.len() as u64,
                            "inner-instruction data bytes",
                        )?;
                        delta.mark_account(
                            ids[entry.program_id_index as usize],
                            account_flags::FLAG_PROGRAM,
                        )?;
                        list.push(inner_instructions::InnerInstruction {
                            stack_height: entry.stack_height,
                            instruction: transactions_codec::Instruction {
                                program_position: entry.program_id_index,
                                account_positions: entry
                                    .accounts
                                    .iter()
                                    .map(|position| u32::from(*position))
                                    .collect(),
                                data: entry.data.clone(),
                            },
                        });
                    }
                    BlockConversionDelta::add_counter(
                        &mut delta.inner_instructions,
                        list.len() as u64,
                        "inner instructions",
                    )?;
                    paired_groups.push(inner_instructions::InnerGroup {
                        parent_index: set.index,
                        instructions: list,
                    });
                }
                paired_groups.sort_by_key(|group| group.parent_index);
                transaction_inner.groups.extend(paired_groups);
                if inner_sets.is_empty() {
                    transactions_codec::CpiState::SourceEmpty
                } else {
                    transactions_codec::CpiState::SourcePresent
                }
            }
            None => {
                if metadata.is_some() {
                    BlockConversionDelta::add_counter(
                        &mut delta.cpi_not_recorded,
                        1,
                        "CPI not recorded",
                    )?;
                    transactions_codec::CpiState::NotRecorded
                } else {
                    transactions_codec::CpiState::Unavailable
                }
            }
        };

        match &metadata {
            Some(meta) => {
                let merged_return_data = meta
                    .return_data
                    .as_ref()
                    .map(|value| {
                        Ok::<_, anyhow::Error>(outcomes_codec::ReturnData {
                            program_id: transactions_codec::PubkeyId::new(
                                pubkeys.target_pubkey_id(&value.program_id)?,
                            )?,
                            data: value.data.clone(),
                        })
                    })
                    .transpose()?;
                outcomes.push(Some(outcomes_codec::TransactionOutcome {
                    error: meta.err.as_ref().map(outcome_error::transaction),
                    fee: meta.fee,
                    compute_units_consumed: meta.compute_units_consumed,
                    cost_units: meta.cost_units,
                    return_data: merged_return_data,
                }));
                balances.push(Some(balances_plane_codec::Balances {
                    pre: meta.pre_balances.clone(),
                    post: meta.post_balances.clone(),
                }));
                let paired_token_balances = pair_token_balances(
                    &meta.pre_token_balances,
                    &meta.post_token_balances,
                    &mut delta,
                    &mut pubkeys,
                    ids.len(),
                )?;
                token_balances.push(
                    (!meta.pre_token_balances.is_empty() || !meta.post_token_balances.is_empty())
                        .then_some(paired_token_balances),
                );
                logs.push(
                    meta.logs
                        .as_ref()
                        .map(|stream| {
                            convert_log_stream_with_inline_pubkeys(
                                stream,
                                log_pubkeys,
                                &mut pubkeys,
                                &inline_log_pubkeys,
                            )
                        })
                        .transpose()?,
                );
                let transaction_rewards = meta
                    .rewards
                    .iter()
                    .map(|reward| compact_reward(reward, &mut pubkeys))
                    .collect::<Result<Vec<_>>>()?;
                rewards.push((!transaction_rewards.is_empty()).then_some(transaction_rewards));
            }
            None => {
                outcomes.push(None);
                balances.push(None);
                token_balances.push(None);
                logs.push(None);
                rewards.push(None);
            }
        }

        let static_end =
            usize::try_from(static_count).context("static account count exceeds usize")?;
        let writable_end = static_end
            .checked_add(usize::try_from(loaded_writable_count)?)
            .context("loaded writable account range overflows")?;
        let readonly_end = writable_end
            .checked_add(usize::try_from(loaded_readonly_count)?)
            .context("loaded readonly account range overflows")?;
        ensure!(
            readonly_end == ids.len(),
            "resolved account partitions drifted"
        );
        let pubkey_ids = |values: &[u32]| {
            values
                .iter()
                .copied()
                .map(transactions_codec::PubkeyId::new)
                .collect::<std::result::Result<Vec<_>, _>>()
        };
        let static_accounts = pubkey_ids(&ids[..static_end])?;
        let message = if is_v0 {
            let loaded_addresses = if metadata.is_some() {
                transactions_codec::LoadedAddresses::Source {
                    writable: pubkey_ids(&ids[static_end..writable_end])?,
                    readonly: pubkey_ids(&ids[writable_end..readonly_end])?,
                }
            } else {
                transactions_codec::LoadedAddresses::Unavailable
            };
            transactions_codec::Message::V0 {
                static_accounts,
                loaded_addresses,
                lookups: tx_lookups,
                instructions: instruction_rows,
            }
        } else {
            ensure!(tx_lookups.is_empty(), "legacy transaction has V0 lookups");
            transactions_codec::Message::Legacy {
                static_accounts,
                instructions: instruction_rows,
            }
        };
        transactions.push(transactions_codec::Transaction {
            header: transactions_codec::MessageHeader {
                num_required_signatures: header.num_required_signatures,
                num_readonly_signed: header.num_readonly_signed_accounts,
                num_readonly_unsigned: header.num_readonly_unsigned_accounts,
            },
            recent_blockhash,
            message,
        });
        let mut effect_state = transactions_codec::EffectState::new(cpi_state);
        let has_metadata = metadata.is_some();
        effect_state.set_present(transactions_codec::EffectKind::Outcome, has_metadata);
        effect_state.set_present(transactions_codec::EffectKind::Balances, has_metadata);
        effect_state.set_present(
            transactions_codec::EffectKind::TokenBalances,
            token_balances.last().is_some_and(Option::is_some),
        );
        effect_state.set_present(
            transactions_codec::EffectKind::Logs,
            metadata.as_ref().is_some_and(|meta| meta.logs.is_some()),
        );
        effect_state.set_present(
            transactions_codec::EffectKind::Rewards,
            rewards.last().is_some_and(Option::is_some),
        );
        effect_states.push(effect_state);
        for id in ids.iter().take(usize::from(header.num_required_signatures)) {
            delta.mark_account(*id, account_flags::FLAG_SIGNER)?;
        }
        inner.push(cpi_state.has_record().then_some((
            transaction_inner,
            top_level_instruction_count,
            resolved_account_count,
        )));
        transaction_signature_ordinal = transaction_signature_ordinal
            .checked_add(u64::from(header.num_required_signatures))
            .context("transaction signature ordinal overflow")?;
    }
    ensure!(
        transaction_signature_ordinal
            == row
                .first_signature_ordinal
                .checked_add(u64::from(row.signature_count))
                .context("block signature range overflow")?,
        "slot {} transaction signature counts do not cover the block range",
        block.header.slot
    );

    let block_rewards = block
        .header
        .rewards
        .as_ref()
        .map(|value| {
            let rewards = block_rewards_codec::BlockRewards {
                num_partitions: value.num_partitions,
                rewards: value
                    .decoded
                    .iter()
                    .map(|reward| compact_reward(reward, &mut pubkeys))
                    .collect::<Result<Vec<_>>>()?,
            };
            let encoded = block_rewards_codec::encode_record(&rewards)
                .context("encode block rewards in stage 3")?;
            PreparedPage::compress(encoded)
        })
        .transpose()?;
    if block_rewards.is_some() {
        BlockConversionDelta::add_counter(
            &mut delta.block_rewards_stored,
            1,
            "stored block rewards",
        )?;
    }

    let expected = block.tx_count as usize;
    for (column, actual) in [
        ("effect_states", effect_states.len()),
        ("inner_instructions", inner.len()),
        ("outcomes", outcomes.len()),
        ("balances", balances.len()),
        ("token_balances", token_balances.len()),
        ("logs", logs.len()),
        ("rewards", rewards.len()),
    ] {
        ensure!(
            actual == expected,
            "slot {}: column {column} has {actual} entries, expected {expected} -- positional alignment is broken",
            block.header.slot
        );
    }
    ensure!(
        transactions.len() == expected,
        "slot {} has {} target transactions, expected {expected}",
        block.header.slot,
        transactions.len()
    );
    for state in &effect_states {
        for kind in transactions_codec::EffectKind::ALL {
            let present = u64::from(state.has_record(kind)?);
            BlockConversionDelta::add_counter(
                &mut delta.effect_record_counts[kind.index()],
                present,
                "effect records",
            )?;
        }
    }

    Ok(ConvertedResolvedBlock {
        commit: TransactionCommitInput {
            row,
            slot: block.header.slot,
            parent_slot: block.header.parent_slot,
            block_time: block.header.block_time,
            block_height: block.header.block_height,
            blockhash,
            previous_blockhash,
            effect_states,
            inner,
            outcomes,
            balances,
            token_balances,
            logs,
            rewards,
        },
        transactions,
        block_rewards,
        delta,
    })
}

/// Write a checksum receipt for canonical candidate objects only.
///
/// This is not a publication manifest. Derived indexes can be built or rebuilt
/// without making this canonical receipt stale. Final publication uses the
/// compaction protocol after every required derived object is present.
fn write_conversion_candidate_hash(root: &Path) -> Result<()> {
    let mut files = LAYOUT
        .iter()
        .filter(|spec| spec.class == FileClass::Canonical && root.join(spec.path).is_file())
        .map(|spec| spec.path)
        .collect::<Vec<_>>();
    files.sort();

    let mut lines = String::new();
    let mut combined = Sha256::new();
    for relative in &files {
        let path = root.join(relative);
        let file = File::open(&path).with_context(|| format!("read {relative} for hashing"))?;
        let mut reader = BufReader::with_capacity(8 << 20, file);
        let mut file_hasher = Sha256::new();
        let mut buffer = vec![0; 8 << 20];
        loop {
            let read = reader
                .read(&mut buffer)
                .with_context(|| format!("read {relative} for hashing"))?;
            if read == 0 {
                break;
            }
            file_hasher.update(&buffer[..read]);
        }
        let digest = file_hasher.finalize();
        let hex = hex_lower(&digest);
        // The combined digest covers the path as well as the content, so a
        // renamed or moved file is a mismatch rather than a silent pass.
        combined.update(relative.as_bytes());
        combined.update(b"\0");
        combined.update(digest);
        lines.push_str(&format!("{hex}  {relative}\n"));
    }
    lines.push_str(&format!(
        "{}  GENERATION\n",
        hex_lower(&combined.finalize())
    ));
    write_and_sync(&root.join("canonical-candidate.sha256"), lines.as_bytes())
        .context("write canonical-candidate.sha256")?;
    Ok(())
}

fn write_and_sync(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync {}", path.display()))
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open directory {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

fn sync_candidate_tree(root: &Path) -> Result<()> {
    for relative in [
        "catalog",
        "dictionary",
        "ledger",
        "runtime",
        "sidecars",
        "indexes",
    ] {
        let path = root.join(relative);
        if path.is_dir() {
            sync_directory(&path)?;
        }
    }
    sync_directory(root)
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from_digit((byte >> 4) as u32, 16).expect("nibble"));
        out.push(char::from_digit((byte & 0x0f) as u32, 16).expect("nibble"));
    }
    out
}

fn parse_hex_32(value: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64,
        "fixture predecessor hash must have exactly 64 lowercase or uppercase hex characters"
    );
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let pair = std::str::from_utf8(pair).expect("hex input is ASCII when digits are valid");
        bytes[index] = u8::from_str_radix(pair, 16)
            .with_context(|| format!("invalid hex byte {pair:?} at position {}", index * 2))?;
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConvertedHashRef {
    NonPoh(u32),
    Poh(u32),
}

fn target_hash_ref(converted: ConvertedHashRef) -> (transactions_codec::HashRef, bool) {
    match converted {
        ConvertedHashRef::NonPoh(ordinal) => (
            transactions_codec::HashRef {
                owner: transactions_codec::HashOwner::NonPoh,
                ordinal: u64::from(ordinal),
            },
            false,
        ),
        ConvertedHashRef::Poh(ordinal) => (
            transactions_codec::HashRef {
                owner: transactions_codec::HashOwner::PohBlockFinal,
                ordinal: u64::from(ordinal),
            },
            true,
        ),
    }
}

fn convert_hash_value(
    hash: [u8; 32],
    final_poh_hash_index: &[([u8; 32], u32)],
    non_poh_ordinals: &mut BTreeMap<[u8; 32], u32>,
    non_poh_dictionary: &mut Vec<u8>,
) -> Result<ConvertedHashRef> {
    if let Ok(position) = final_poh_hash_index.binary_search_by_key(&hash, |entry| entry.0) {
        return Ok(ConvertedHashRef::Poh(final_poh_hash_index[position].1));
    }
    Ok(ConvertedHashRef::NonPoh(intern_non_poh_hash(
        hash,
        non_poh_ordinals,
        non_poh_dictionary,
    )?))
}

fn intern_non_poh_hash(
    hash: [u8; 32],
    ordinals: &mut BTreeMap<[u8; 32], u32>,
    dictionary: &mut Vec<u8>,
) -> Result<u32> {
    if let Some(ordinal) = ordinals.get(&hash) {
        return Ok(*ordinal);
    }
    let ordinal = u32::try_from(ordinals.len()).context("non-PoH dictionary exceeds u32")?;
    ordinals.insert(hash, ordinal);
    dictionary.extend_from_slice(&hash);
    Ok(ordinal)
}

#[derive(Clone, Copy)]
struct HashConversionTables<'a> {
    resolver: &'a BlockhashResolver,
    registry_offset: u32,
    final_poh_block_by_source_hash: &'a [Option<u64>],
    final_poh_hash_index: &'a [([u8; 32], u32)],
}

fn convert_source_hash_id(
    id: i32,
    tables: HashConversionTables<'_>,
    non_poh_ordinals: &mut BTreeMap<[u8; 32], u32>,
    non_poh_dictionary: &mut Vec<u8>,
) -> Result<ConvertedHashRef> {
    if id >= 0 {
        let id = u32::try_from(id).expect("non-negative i32 fits u32");
        if let Some(block_ordinal) = id.checked_sub(tables.registry_offset)
            && let Some(Some(target_block_ordinal)) = tables
                .final_poh_block_by_source_hash
                .get(block_ordinal as usize)
        {
            return Ok(ConvertedHashRef::Poh(
                u32::try_from(*target_block_ordinal).context("PoH block ordinal exceeds u32")?,
            ));
        }
    }
    let hash = tables
        .resolver
        .resolve(id)
        .context("resolve source blockhash id")?;
    convert_hash_value(
        hash,
        tables.final_poh_hash_index,
        non_poh_ordinals,
        non_poh_dictionary,
    )
}

fn convert_header_previous_hash(
    blockhash_id: u32,
    previous_id: u32,
    tables: HashConversionTables<'_>,
    non_poh_ordinals: &mut BTreeMap<[u8; 32], u32>,
    non_poh_dictionary: &mut Vec<u8>,
) -> Result<ConvertedHashRef> {
    if blockhash_id == 0 && previous_id == 0 && !tables.resolver.previous().entries.is_empty() {
        let hash = tables
            .resolver
            .previous()
            .entries
            .last()
            .expect("checked non-empty tail")
            .hash;
        return convert_hash_value(
            hash,
            tables.final_poh_hash_index,
            non_poh_ordinals,
            non_poh_dictionary,
        );
    }
    let previous = i32::try_from(previous_id).context("previous blockhash id exceeds i32")?;
    convert_source_hash_id(previous, tables, non_poh_ordinals, non_poh_dictionary)
}

fn main() -> Result<()> {
    let conversion_started = Instant::now();
    let args = parse_args()?;
    let source_admission_started = Instant::now();
    let source_context = validate_source_publication(&args.source, &args)?;
    let source_admission_elapsed = source_admission_started.elapsed();
    emit_phase("source_admission", "complete", source_admission_elapsed);
    let fixture_predecessor = args
        .fixture_previous_blockhash
        .zip(args.fixture_previous_slot);
    let source = args.source.clone();
    let final_output = args.output.clone();
    ensure!(
        !final_output.exists(),
        "output {} already exists; conversion never mixes an old and new attempt",
        final_output.display()
    );
    let output = staging_path(&final_output)?;
    ensure!(
        !output.exists(),
        "staging directory {} already exists; inspect or move it before retrying",
        output.display()
    );
    fs::create_dir_all(&output).with_context(|| format!("create {}", output.display()))?;

    let source_shape_started = Instant::now();
    let mut index = match &source_context.index {
        Some(index) => index.clone(),
        None => read_archive_v2_hot_block_index_file(
            source_context
                .source
                .pinned_file_clone(ARCHIVE_V2_BLOCK_INDEX_FILE)?
                .context("fixture hot block index is missing")?,
            &source.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        )
        .context("read pinned fixture hot block index")?,
    };
    validate_source_shape(
        &source_context.source,
        &index,
        args.pipeline_memory_limit_bytes,
    )
    .context("validate Compact V2 source")?;
    let source_total_blocks = index.rows.len();
    if let Some(blocks) = args.benchmark_prefix_blocks {
        ensure!(
            blocks < source_total_blocks,
            "--benchmark-prefix-blocks must be smaller than the full {source_total_blocks}-block generation"
        );
        index.rows.truncate(blocks);
    }
    let source_shape_elapsed = source_shape_started.elapsed();
    emit_phase("source_shape", "complete", source_shape_elapsed);
    let source_block_bytes = index.rows.iter().try_fold(0_u64, |bytes, row| {
        bytes
            .checked_add(u64::from(row.compressed_len))
            .context("source compressed byte total overflow")
    })?;
    let source_decoded_block_bytes = index.rows.iter().try_fold(0_u64, |bytes, row| {
        bytes
            .checked_add(u64::from(row.uncompressed_len))
            .context("source decoded byte total overflow")
    })?;
    let source_expected_transactions = index.rows.iter().try_fold(0_u64, |count, row| {
        count
            .checked_add(u64::from(row.tx_count))
            .context("source transaction total overflow")
    })?;
    let source_first_slot = index
        .rows
        .first()
        .context("selected source range has no blocks")?
        .slot;
    let source_last_slot = index
        .rows
        .last()
        .context("selected source range has no blocks")?
        .slot;
    let selected_signature_records = index.rows.iter().try_fold(0_u64, |count, row| {
        count
            .checked_add(u64::from(row.signature_count))
            .context("selected signature count overflow")
    })?;
    // Headers are completed with a temporary zero ID. After every canonical
    // object is durable, a normalized content transcript derives the final ID
    // and the converter patches all present headered objects in one staging
    // directory. Nothing outside staging can observe the temporary value.
    let archive_id = ArchiveId::new([0_u8; 16]);
    // Validate and retain PoH and shredding one complete source frame at a
    // time. Their Wincode/varint bytes are not rewritten. Each target catalog
    // row points at the complete self-delimiting frame.
    let retained_sidecars_started = Instant::now();
    let source_blockhashes =
        read_pinned_all(&source_context.source, ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE)?;
    let repair_blocks = source_context
        .source
        .pinned_file_clone(ARCHIVE_V2_BLOCKS_FILE)?
        .context("source blocks are missing for PoH repair")?;
    let poh_source = source_context
        .source
        .pinned_file_clone(ARCHIVE_V2_POH_FILE)?
        .context("source PoH sidecar is missing")?;
    let retained_poh = if args.benchmark_prefix_blocks.is_some() {
        retained_sidecars::retain_poh_prefix(
            poh_source,
            &output,
            archive_id,
            &index.rows,
            source_total_blocks,
            &source_blockhashes,
            source_context.epoch,
        )
    } else {
        retained_sidecars::retain_poh(
            poh_source,
            &output,
            archive_id,
            &index.rows,
            &source_blockhashes,
            source_context.epoch,
        )
    }
    .context("validate and retain PoH sidecar")?;
    let shredding_source = source_context
        .source
        .pinned_file_clone(ARCHIVE_V2_SHREDDING_FILE)?
        .context("source shredding sidecar is missing")?;
    let retained_shredding = if args.benchmark_prefix_blocks.is_some() {
        retained_sidecars::retain_shredding_prefix(
            shredding_source,
            &output,
            archive_id,
            &index.rows,
        )
    } else {
        retained_sidecars::retain_shredding(shredding_source, &output, archive_id, &index.rows)
    }
    .context("validate and retain shredding sidecar")?;
    ensure!(
        retained_shredding.recorded_empty_blocks == 0,
        "source has a present-but-empty shredding record. Old live archives used this shape as an unknown placeholder, so exact shredding needs embedded-header evidence or a trusted backfill"
    );

    let previous_tail = if source_context.epoch == 0 {
        PreviousBlockhashTail {
            schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
            entries: Vec::new(),
        }
    } else if let Some((hash, slot)) = fixture_predecessor {
        let first_row = index
            .rows
            .first()
            .copied()
            .context("fixture source has no blocks")?;
        let first_parent = decode_source_block(&repair_blocks, first_row)?
            .block
            .header
            .parent_slot;
        ensure!(
            slot == first_parent,
            "fixture predecessor slot {slot} does not match the first block predecessor {first_parent}"
        );
        PreviousBlockhashTail {
            schema: PreviousBlockhashTailSchema::CurrentHashAndSlot,
            entries: vec![
                blockzilla_index_archive_convert::source_v2_sidecars::PreviousBlockhash {
                    hash,
                    slot: Some(slot),
                },
            ],
        }
    } else {
        let bytes = read_pinned_all(&source_context.source, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)?;
        let tail =
            parse_previous_blockhash_tail(&bytes, PreviousBlockhashTailSchema::CurrentHashAndSlot)
                .context("decode exact current hash-and-slot predecessor-tail schema")?;
        let epoch_start = source_context
            .epoch
            .checked_mul(source_context.slots_per_epoch)
            .context("epoch start overflows u64")?;
        let previous_start = epoch_start
            .checked_sub(source_context.slots_per_epoch)
            .context("previous epoch start underflows")?;
        ensure!(
            tail.entries.iter().all(|entry| entry
                .slot
                .is_some_and(|slot| (previous_start..epoch_start).contains(&slot))),
            "current predecessor tail contains a slot outside the previous epoch"
        );
        tail
    };
    if let Some(first_row) = index.rows.first()
        && source_context.epoch != 0
    {
        let predecessor = previous_tail
            .entries
            .last()
            .context("nonzero-epoch source has no predecessor-tail entry")?;
        ensure!(
            predecessor.slot == Some(first_row.slot.saturating_sub(1))
                || predecessor.slot.is_some_and(|slot| slot < first_row.slot),
            "predecessor-tail slot is not before the first source block"
        );
        let first_parent = decode_source_block(&repair_blocks, *first_row)?
            .block
            .header
            .parent_slot;
        ensure!(
            predecessor.slot == Some(first_parent),
            "predecessor-tail slot {:?} does not match first block parent {first_parent}",
            predecessor.slot
        );
    }
    let source_hash_resolver = Arc::new(
        BlockhashResolver::from_bytes(&source_blockhashes, previous_tail)
            .context("build source blockhash resolver")?,
    );
    let final_poh_block_by_source_hash = retained_poh
        .mappings
        .iter()
        .map(|mapping| mapping.final_entry_ordinal.map(|_| mapping.block_ordinal))
        .collect::<Vec<_>>();
    let mut final_poh_hash_index = retained_poh
        .mappings
        .iter()
        .enumerate()
        .filter_map(|(block, mapping)| {
            mapping.final_entry_ordinal.map(|_| {
                let registry = retained_poh.blockhash_registry_offset as usize + block;
                let hash = source_blockhashes[registry * 32..(registry + 1) * 32]
                    .try_into()
                    .expect("validated blockhash registry record");
                Ok((
                    hash,
                    u32::try_from(mapping.block_ordinal)
                        .context("PoH block ordinal exceeds u32")?,
                ))
            })
        })
        .collect::<Result<Vec<([u8; 32], u32)>>>()?;
    final_poh_hash_index.sort_unstable_by_key(|entry| entry.0);
    ensure!(
        final_poh_hash_index
            .windows(2)
            .all(|pair| pair[0].0 != pair[1].0),
        "two retained blocks have the same final PoH hash"
    );
    let retained_sidecars_elapsed = retained_sidecars_started.elapsed();
    emit_phase("retained_sidecars", "complete", retained_sidecars_elapsed);

    // Non-PoH hashes have their own compact dictionary. Previous-epoch hashes
    // are seeded first. Durable nonce values are appended on first use. No PoH
    // entry hash is copied here.
    drop(source_blockhashes);
    let mut blockhash_dictionary = Vec::new();
    let mut non_poh_ordinals: BTreeMap<[u8; 32], u32> = BTreeMap::new();
    let hash_conversion_tables = HashConversionTables {
        resolver: &source_hash_resolver,
        registry_offset: retained_poh.blockhash_registry_offset,
        final_poh_block_by_source_hash: &final_poh_block_by_source_hash,
        final_poh_hash_index: &final_poh_hash_index,
    };
    for entry in &source_hash_resolver.previous().entries {
        if final_poh_hash_index
            .binary_search_by_key(&entry.hash, |candidate| candidate.0)
            .is_ok()
        {
            continue;
        }
        if non_poh_ordinals.contains_key(&entry.hash) {
            continue;
        }
        let ordinal =
            u32::try_from(non_poh_ordinals.len()).context("non-PoH dictionary exceeds u32")?;
        non_poh_ordinals.insert(entry.hash, ordinal);
        blockhash_dictionary.extend_from_slice(&entry.hash);
    }

    fs::create_dir_all(&output).with_context(|| format!("create {}", output.display()))?;
    // Reserve at most a third of the CPU budget for unified, per-block target
    // encoding. Each stage-3 worker prepares the transaction arena and all six
    // effect streams. Only ordered commit writes bytes and assigns offsets.
    //
    // Block work is Ed25519-bound -- one signature verification per transaction
    // dominates conversion -- so the block stage is the pipeline's throughput,
    // and effect encoding is a minority of the work it feeds. Reserving half the
    // budget for pages before allocating any block worker inverted that: at
    // `--workers 2` it produced one page worker and one block worker, which
    // measured identical to `--workers 1`. A third leaves the block stage the
    // clear majority at every budget, and two workers now means two block
    // workers with stage 3 running inline on the coordinator thread.
    let page_workers = if args.workers > 2 {
        6.min(args.workers / 3)
    } else {
        0
    };
    let transaction_page_workers = page_workers;
    // Effects are now prepared by the same per-block stage-3 workers as the
    // transaction arena. No separate effect pool is needed.
    let effect_workers = 0;
    let source_worker_budget = args
        .workers
        .checked_sub(page_workers)
        .context("page workers exhausted the global worker budget")?;
    let block_workers = source_worker_budget.min(index.rows.len().max(1));
    let intra_block_workers = if index.rows.len() == 1 {
        source_worker_budget
    } else {
        1
    };
    let nested_message_workers = intra_block_workers.saturating_sub(1);
    let source_io_workers = usize::from(!index.rows.is_empty());
    let max_spawned_worker_threads = block_workers
        .checked_add(page_workers)
        .and_then(|threads| threads.checked_add(nested_message_workers))
        .and_then(|threads| threads.checked_add(source_io_workers))
        .context("worker count overflow")?;
    ensure!(
        max_spawned_worker_threads <= args.workers.saturating_add(1),
        "worker allocation exceeds the requested compute budget plus one source I/O helper"
    );
    let mut report = Report {
        archive_id: archive_id.to_hex(),
        source_published: source_context.published,
        source_generation_digest: source_context.generation_digest.clone(),
        epoch: source_context.epoch,
        slots_per_epoch: source_context.slots_per_epoch,
        source_profile: match source_context.message_schema {
            CompactV2MessageSchema::Current => "archive-v2-current-hot-v1",
            CompactV2MessageSchema::May24PreUnknownFallbacks => {
                "archive-v2-may24-pre-unknown-fallbacks-v1"
            }
        },
        metadata_source_profile: match source_context.metadata_schema {
            CompactV2MetadataSchema::CurrentTypedError => {
                "archive-v2-current-typed-error-v1"
            }
            CompactV2MetadataSchema::LegacyRawError => "archive-v2-legacy-raw-error-v1",
        },
        poh_source_schema: match retained_poh.source_schema {
            blockzilla_index_archive_convert::source_v2_sidecars::SourcePohSchema::Current => {
                "archive-v2-current-wincode-0.5.5"
            }
            blockzilla_index_archive_convert::source_v2_sidecars::SourcePohSchema::LegacyNoSignatureCount => {
                "archive-v2-legacy-no-signature-count-wincode-0.5.5"
            }
            blockzilla_index_archive_convert::source_v2_sidecars::SourcePohSchema::NoEntrySchemaEvidence => {
                unreachable!("retained PoH rejects an unproved source schema")
            }
        },
        output_status: if args.benchmark_prefix_blocks.is_some() {
            "benchmark-prefix-not-publishable"
        } else {
            "complete-physical-candidate-not-publishable"
        },
        pipeline_memory_limit_bytes: args.pipeline_memory_limit_bytes,
        process_memory_is_strictly_bounded: false,
        fixture_previous_blockhash: fixture_predecessor.map(|(hash, _)| hex_lower(&hash)),
        fixture_previous_slot: fixture_predecessor.map(|(_, slot)| slot),
        workers: args.workers,
        block_workers,
        intra_block_workers,
        source_io_workers,
        page_workers,
        transaction_page_workers,
        effect_workers,
        max_spawned_worker_threads,
        timings: PhaseTimings {
            source_admission_ms: duration_millis(source_admission_elapsed),
            source_shape_ms: duration_millis(source_shape_elapsed),
            retained_sidecars_ms: duration_millis(retained_sidecars_elapsed),
            ..PhaseTimings::default()
        },
        source_block_bytes,
        source_decoded_block_bytes,
        source_first_slot,
        source_last_slot,
        signatures: selected_signature_records,
        benchmark_prefix_blocks: args.benchmark_prefix_blocks,
        source_total_blocks,
        ..Report::default()
    };
    let mut transactions_plane = Plane::create(&output, transactions_codec::PATH)?;
    let mut inner_plane = Plane::create(&output, inner_instructions::PATH)?;
    let mut outcomes_plane = Plane::create(&output, outcomes_codec::PATH)?;
    let mut balances_plane = Plane::create(&output, balances_plane_codec::PATH)?;
    let mut token_balances_plane = Plane::create(&output, token_balances_codec::PATH)?;
    let mut logs_plane = Plane::create(&output, logs_codec::PATH)?;
    let mut rewards_plane = Plane::create(&output, rewards_codec::PATH)?;
    let mut block_rewards_plane = Plane::create(&output, block_rewards_codec::PATH)?;
    // Catalog rows are assigned and written only by ordered commit. Streaming
    // avoids retaining and then encoding about 2 * 118 MiB for a full epoch.
    let mut catalog_writer = HeaderedWriter::create(&output, catalog_blocks::PATH, 8 << 20)?;
    let mut previous_catalog_row: Option<catalog_blocks::BlockRow> = None;
    let mut first_transaction = 0_u64;
    let mut effect_record_counts = [0_u64; transactions_codec::EFFECT_KIND_COUNT];

    // A nonce transaction stores a nonce account hash in the recent-blockhash
    // position. It is not a PoH entry, so this dictionary is its only owner.
    let dictionary_admission_started = Instant::now();
    let registry_entries = u32::try_from(
        source_context
            .source
            .size(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)?
            .context("source pubkey registry is missing")?
            / 32,
    )
    .context("pubkey registry exceeds u32 records")?;
    let log_pubkeys =
        PinnedPubkeyResolver::open(&source_context.source, ARCHIVE_V2_PUBKEY_REGISTRY_FILE)?
            .context("source pubkey registry is missing for exact log rendering")?;
    ensure!(
        log_pubkeys.record_count() == registry_entries,
        "source pubkey registry changed while opening the log resolver"
    );
    let log_pubkeys = Arc::new(log_pubkeys);
    let mut target_pubkeys =
        TargetPubkeyDictionary::open(&source_context.source, &source, registry_entries)?;
    let vote_hashes = source_context
        .source
        .size(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE)?
        .is_some()
        .then(|| read_pinned_all(&source_context.source, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE))
        .transpose()?
        .map(|bytes| VoteHashRegistry::from_bytes(&bytes))
        .transpose()
        .context("validate vote-hash registry")?
        .map(Arc::new);
    let dictionary_admission_elapsed = dictionary_admission_started.elapsed();
    report.timings.dictionary_admission_ms = duration_millis(dictionary_admission_elapsed);
    emit_phase(
        "dictionary_admission",
        "complete",
        dictionary_admission_elapsed,
    );
    let mut nonce_hashes = std::collections::BTreeSet::new();
    let mut max_non_poh_hash_ref = None;
    let mut max_poh_hash_ref = None;
    // What each account was used as, one byte per registry entry rather than
    // per reference: an epoch has ~45M accounts and ~7.5B references to them,
    // so this is ~168x smaller than flagging each reference, and it removes
    // ledger/instructions from the "which programs did this touch" filter.
    //
    // Collected as ordinal sets rather than written straight into the table
    // because the table's length is the registry size, which is only known
    // after the copy below.
    let mut observed_account_flags = vec![0_u8; account_flags::byte_len(registry_entries)];
    let uncompressed_batch_budget_bytes = args
        .pipeline_memory_limit_bytes
        .saturating_div(16)
        .clamp(1, 512 * 1024 * 1024);
    let reader_config = OrderedParallelBlockConfig {
        compressed_batch_target_bytes: 16 * 1024 * 1024,
        uncompressed_batch_budget_bytes,
        // Match one decode task to each source worker. A larger batch creates
        // a projection/consume barrier: all source workers become idle while
        // the bounded stage-3 queue drains the rest of that batch.
        max_blocks_per_batch: 1_024,
        compressed_buffer_count: 3,
        decode_workers: block_workers,
        retained_decompressed_bytes_per_worker: 32 * 1024 * 1024,
        discard_rewards: false,
    };
    let worker_source = source_context.source.clone();
    let worker_hash_resolver = source_hash_resolver.clone();
    let worker_vote_hashes = vote_hashes.clone();
    let message_schema = source_context.message_schema;
    let metadata_schema = source_context.metadata_schema;
    let source_blockhash_registry_offset = retained_poh.blockhash_registry_offset;
    let expected_rows = Arc::new(index.rows.clone());
    let mut progress = ProgressReporter::new(
        u64::try_from(index.rows.len()).context("source block count exceeds u64")?,
        source_block_bytes,
    );
    let mut ordered_commit_sum = Duration::ZERO;
    let mut borrowed_to_owned_sum = Duration::ZERO;
    let mut source_message_decode_sum = Duration::ZERO;
    let mut signed_message_proof_sum = Duration::ZERO;
    let mut source_metadata_decode_sum = Duration::ZERO;
    let mut source_validation_sum = Duration::ZERO;
    let mut source_inline_log_pubkey_discovery_sum = Duration::ZERO;
    let mut transaction_transform_sum = Duration::ZERO;
    let mut ordered_raw_pubkey_assignment_sum = Duration::ZERO;
    let mut ordered_hash_assignment_sum = Duration::ZERO;
    let mut stage3_pure_resolved_conversion_sum = Duration::ZERO;
    let mut effect_prepare_and_write_sum = Duration::ZERO;
    let mut effect_worker_prepare_sum = Duration::ZERO;
    let mut effect_final_append_sum = Duration::ZERO;
    let mut transaction_page_worker_sum = Duration::ZERO;
    let mut transaction_page_final_commit_sum = Duration::ZERO;
    let mut stage3_worker_effect_and_arena_sum = Duration::ZERO;
    let mut final_append_and_offset_commit_sum = Duration::ZERO;
    let mut transaction_page_prepare_and_write_sum = Duration::ZERO;
    let mut catalog_commit_sum = Duration::ZERO;
    let block_pipeline_started = Instant::now();
    emit_phase("block_pipeline", "start", Duration::ZERO);
    let mut committed_blocks = 0_u64;
    let mut committed_transactions = 0_u64;
    let mut commit_prepared = |sequence: u64, prepared: PreparedTransactionBlock| -> Result<()> {
        let ordered_commit_started = Instant::now();
        let PreparedTransactionBlock {
            commit,
            prepared_effects,
            arena,
            block_rewards,
            delta,
            pure_conversion_time,
            effect_worker_time,
            arena_worker_time,
        } = prepared;
        let TransactionFinalCommitInput {
            row,
            slot,
            parent_slot,
            block_time,
            block_height,
            blockhash,
            previous_blockhash,
            effect_states,
        } = commit;
        ensure!(
            sequence == u64::from(row.block_id),
            "prepared block sequence {sequence} does not match source block {}",
            row.block_id
        );
        ensure!(
            arena.transaction_count() == row.tx_count,
            "slot {slot} prepared transaction count does not match the source row"
        );
        ensure!(
            arena.signature_count() == row.signature_count,
            "slot {slot} prepared signature count does not match the source row"
        );
        ensure!(
            delta.transactions == u64::from(row.tx_count),
            "slot {slot} conversion delta has {} transactions, expected {}",
            delta.transactions,
            row.tx_count
        );
        stage3_pure_resolved_conversion_sum =
            stage3_pure_resolved_conversion_sum.saturating_add(pure_conversion_time);
        effect_worker_prepare_sum = effect_worker_prepare_sum.saturating_add(effect_worker_time);
        transaction_page_worker_sum = transaction_page_worker_sum.saturating_add(arena_worker_time);
        stage3_worker_effect_and_arena_sum = stage3_worker_effect_and_arena_sum
            .saturating_add(effect_worker_time)
            .saturating_add(arena_worker_time);
        effect_prepare_and_write_sum =
            effect_prepare_and_write_sum.saturating_add(effect_worker_time);
        transaction_page_prepare_and_write_sum =
            transaction_page_prepare_and_write_sum.saturating_add(arena_worker_time);

        let effect_append_started = Instant::now();
        let [
            inner_chunks,
            outcome_chunks,
            balance_chunks,
            token_balance_chunks,
            log_chunks,
            reward_chunks,
        ] = prepared_effects;
        let effect_files = [
            inner_plane.push_effect_chunks(inner_chunks)?,
            outcomes_plane.push_effect_chunks(outcome_chunks)?,
            balances_plane.push_effect_chunks(balance_chunks)?,
            token_balances_plane.push_effect_chunks(token_balance_chunks)?,
            logs_plane.push_effect_chunks(log_chunks)?,
            rewards_plane.push_effect_chunks(reward_chunks)?,
        ];
        let effect_append_elapsed = effect_append_started.elapsed();
        effect_final_append_sum = effect_final_append_sum.saturating_add(effect_append_elapsed);
        effect_prepare_and_write_sum =
            effect_prepare_and_write_sum.saturating_add(effect_append_elapsed);
        final_append_and_offset_commit_sum =
            final_append_and_offset_commit_sum.saturating_add(effect_append_elapsed);

        let transaction_page_started = Instant::now();
        let transaction_page = arena
            .into_page(effect_states, effect_files)
            .context("finish prepared transaction page")?;
        let transaction_span = transactions_plane.push_prepared(PreparedPage {
            stored: transaction_page.stored,
            decoded_len: transaction_page.decoded_len,
            compressed: transaction_page.compressed,
        })?;
        let transaction_page_elapsed = transaction_page_started.elapsed();
        transaction_page_final_commit_sum =
            transaction_page_final_commit_sum.saturating_add(transaction_page_elapsed);
        transaction_page_prepare_and_write_sum =
            transaction_page_prepare_and_write_sum.saturating_add(transaction_page_elapsed);
        final_append_and_offset_commit_sum =
            final_append_and_offset_commit_sum.saturating_add(transaction_page_elapsed);

        let catalog_commit_started = Instant::now();
        let block_rewards_locator = match block_rewards {
            Some(prepared) => {
                let span = block_rewards_plane.push_prepared(prepared)?;
                catalog_blocks::FactLocator::Source(span)
            }
            None => catalog_blocks::FactLocator::Unavailable,
        };
        let block_index = usize::try_from(row.block_id).context("block ID exceeds usize")?;
        let poh_span = *retained_poh
            .spans
            .get(block_index)
            .context("missing retained PoH frame span")?;
        let shredding_span = *retained_shredding
            .spans
            .get(block_index)
            .context("missing retained shredding frame span")?;
        let catalog_row = catalog_blocks::BlockRow {
            slot,
            parent_slot,
            first_transaction,
            transaction_count: row.tx_count,
            blockhash,
            previous_blockhash,
            block_time,
            block_height,
            first_signature: row.first_signature_ordinal,
            transactions: transaction_span,
            block_rewards: block_rewards_locator,
            poh: catalog_blocks::FactLocator::Source(poh_span),
            shredding: catalog_blocks::FactLocator::Source(shredding_span),
        };
        let encoded_catalog_row = catalog_row.encode()?;
        catalog_blocks::BlockRow::decode(&encoded_catalog_row)
            .context("validate encoded catalog row")?;
        if let Some(previous) = previous_catalog_row {
            ensure!(
                catalog_row.slot > previous.slot,
                "catalog slots are not ascending"
            );
            ensure!(
                catalog_row.parent_slot == previous.slot,
                "slot {} parent {} does not match previous produced slot {}",
                catalog_row.slot,
                catalog_row.parent_slot,
                previous.slot
            );
            ensure!(
                catalog_row.previous_blockhash == previous.blockhash,
                "slot {} predecessor hash does not match the previous produced block",
                catalog_row.slot
            );
            ensure!(
                catalog_row.first_transaction == previous.transactions_end()?,
                "catalog transaction ranges are not contiguous"
            );
        }
        catalog_writer.append(&encoded_catalog_row, catalog_blocks::ROW_LEN as u64)?;
        previous_catalog_row = Some(catalog_row);
        first_transaction = first_transaction
            .checked_add(u64::from(row.tx_count))
            .context("catalog transaction count overflow")?;
        committed_blocks = committed_blocks.saturating_add(1);
        committed_transactions = committed_transactions
            .checked_add(u64::from(row.tx_count))
            .context("committed transaction count overflow")?;
        delta.merge(
            &mut report,
            &mut observed_account_flags,
            &mut effect_record_counts,
            &mut nonce_hashes,
            &mut max_non_poh_hash_ref,
            &mut max_poh_hash_ref,
        )?;
        report.blocks = report
            .blocks
            .checked_add(1)
            .context("block report count overflow")?;
        let catalog_commit_elapsed = catalog_commit_started.elapsed();
        catalog_commit_sum = catalog_commit_sum.saturating_add(catalog_commit_elapsed);
        final_append_and_offset_commit_sum =
            final_append_and_offset_commit_sum.saturating_add(catalog_commit_elapsed);
        ordered_commit_sum = ordered_commit_sum.saturating_add(ordered_commit_started.elapsed());
        progress.record(
            u64::from(row.compressed_len),
            committed_transactions,
            committed_blocks == progress.total_blocks,
        );
        Ok(())
    };
    let mut reader_stats_result = None;
    let produce =
        |submit: &mut dyn FnMut(OrderedTask<ResolvedBlockInput>) -> Result<()>| -> Result<()> {
            let stats = source_context.reader.process_borrowed_blocks_parallel_ordered(
        0..index.rows.len(),
        reader_config,
        move |_| {
            SourceWorker::open(
                &worker_source,
                SourceWorkerConfig {
                    registry_entries,
                    source_hashes: worker_hash_resolver.clone(),
                    vote_hashes: worker_vote_hashes.clone(),
                    intra_block_workers,
                    message_schema,
                    metadata_schema,
                    blockhash_registry_offset: source_blockhash_registry_offset,
                },
            )
        },
        {
            let expected_rows = Arc::clone(&expected_rows);
            move |
                worker: &mut SourceWorker,
                ordinal: usize,
                borrowed: BorrowedDecodedBlock<'_>,
            | -> Result<DecodedSourceBlock> {
                let expected = expected_rows
                    .get(ordinal)
                    .copied()
                    .context("borrowed reader returned an out-of-range ordinal")?;
                ensure!(
                    same_source_index_row(borrowed.index_row, expected),
                    "borrowed reader row {ordinal} does not match the admitted source index"
                );
                worker.project_borrowed(borrowed)
            }
        },
        |ordinal, decoded| {
            borrowed_to_owned_sum =
                borrowed_to_owned_sum.saturating_add(decoded.borrowed_to_owned_time);
            source_message_decode_sum =
                source_message_decode_sum.saturating_add(decoded.message_decode_time);
            signed_message_proof_sum =
                signed_message_proof_sum.saturating_add(decoded.signed_message_proof_time);
            source_metadata_decode_sum =
                source_metadata_decode_sum.saturating_add(decoded.metadata_decode_time);
            source_validation_sum =
                source_validation_sum.saturating_add(decoded.validation_time);
            source_inline_log_pubkey_discovery_sum = source_inline_log_pubkey_discovery_sum
                .saturating_add(decoded.inline_log_pubkey_discovery_time);
            let row = decoded.row;
            let mut block = decoded.block;
            let projected_transactions = decoded.transactions;
            let raw_pubkey_visits = decoded.raw_pubkey_visits;
            ensure!(
                projected_transactions.len() == block.tx_rows.len(),
                "slot {} projected transaction count mismatch",
                row.slot
            );
            let ordered_resolution_started = Instant::now();
            let mut resolved_pubkeys = ResolvedPubkeyTable::new(registry_entries);
            let mut resolved_transactions = Vec::with_capacity(projected_transactions.len());
            let mut delta = BlockConversionDelta::default();

            // Source IDs are identity-mapped and were range-checked by the
            // source worker. Only raw values can change the target dictionary.
            // Their compact visit trace freezes the exact old first-use order
            // without replaying message and metadata shape work here.
            let raw_pubkey_assignment_started = Instant::now();
            for bytes in raw_pubkey_visits {
                resolve_and_record_pubkey(
                    &mut target_pubkeys,
                    &mut resolved_pubkeys,
                    &CompactPubkey::Raw(bytes),
                )?;
            }
            ordered_raw_pubkey_assignment_sum = ordered_raw_pubkey_assignment_sum
                .saturating_add(raw_pubkey_assignment_started.elapsed());

            for (tx, projected) in block.tx_rows.iter().zip(projected_transactions) {
                let ProjectedSourceTransaction {
                    payload,
                    metadata,
                    exact_instruction_data,
                    message_decode_time: _,
                    signed_message_proof_time: _,
                    metadata_decode_time: _,
                    shape,
                    inline_log_pubkeys,
                } = projected;
                let (_, _, recent_blockhash, _, _) = convert_message(&payload);

                let hash_assignment_started = Instant::now();
                let converted_blockhash = match recent_blockhash {
                    OwnedCompactRecentBlockhash::Id(id) => convert_source_hash_id(
                        *id,
                        hash_conversion_tables,
                        &mut non_poh_ordinals,
                        &mut blockhash_dictionary,
                    )
                    .with_context(|| {
                        format!(
                            "slot {} transaction {} recent blockhash",
                            block.header.slot, tx.tx_index
                        )
                    })?,
                    OwnedCompactRecentBlockhash::Nonce(hash) => {
                        BlockConversionDelta::add_counter(
                            &mut delta.nonce_blockhashes,
                            1,
                            "nonce blockhashes",
                        )?;
                        let converted = convert_hash_value(
                            *hash,
                            &final_poh_hash_index,
                            &mut non_poh_ordinals,
                            &mut blockhash_dictionary,
                        )?;
                        if matches!(converted, ConvertedHashRef::NonPoh(_)) {
                            delta.nonce_hashes.insert(*hash);
                        }
                        converted
                    }
                };
                let (recent_blockhash, recent_in_poh) = target_hash_ref(converted_blockhash);
                delta.observe_hash(
                    u32::try_from(recent_blockhash.ordinal)
                        .expect("source hash ordinal fits u32"),
                    recent_in_poh,
                );
                ordered_hash_assignment_sum = ordered_hash_assignment_sum
                    .saturating_add(hash_assignment_started.elapsed());
                resolved_transactions.push(ResolvedSourceTransaction {
                    payload,
                    metadata,
                    exact_instruction_data,
                    recent_blockhash,
                    inline_log_pubkeys,
                    shape,
                });
            }
            let hash_assignment_started = Instant::now();
            let current_blockhash = convert_source_hash_id(
                i32::try_from(block.header.blockhash_id)
                    .context("current blockhash ID exceeds i32")?,
                hash_conversion_tables,
                &mut non_poh_ordinals,
                &mut blockhash_dictionary,
            )?;
            let previous_blockhash = convert_header_previous_hash(
                block.header.blockhash_id,
                block.header.previous_blockhash_id,
                hash_conversion_tables,
                &mut non_poh_ordinals,
                &mut blockhash_dictionary,
            )?;
            let (blockhash, blockhash_in_poh) = target_hash_ref(current_blockhash);
            let (previous_blockhash, previous_in_poh) = target_hash_ref(previous_blockhash);
            delta.observe_hash(
                u32::try_from(blockhash.ordinal).expect("source hash ordinal fits u32"),
                blockhash_in_poh,
            );
            delta.observe_hash(
                u32::try_from(previous_blockhash.ordinal).expect("source hash ordinal fits u32"),
                previous_in_poh,
            );
            ordered_hash_assignment_sum = ordered_hash_assignment_sum
                .saturating_add(hash_assignment_started.elapsed());

            // Message and metadata lanes are no longer needed after source
            // projection and ordered validation. Release their allocations
            // before this block can wait behind out-of-order stage-3 work.
            block.message_bytes = Vec::new();
            block.metadata_bytes = Vec::new();

            let reservation = task_reservation_bytes(row)?;
            let sequence = u64::try_from(ordinal).context("block ordinal exceeds u64")?;
            transaction_transform_sum = transaction_transform_sum
                .saturating_add(ordered_resolution_started.elapsed());
            submit(OrderedTask::new(
                sequence,
                reservation,
                ResolvedBlockInput {
                    row,
                    block,
                    transactions: resolved_transactions,
                    pubkeys: resolved_pubkeys,
                    blockhash,
                    previous_blockhash,
                    delta,
                },
            ))?;
            Ok(())
        },
    )
    .map_err(|error| anyhow::anyhow!(error))?;
            reader_stats_result = Some(stats);
            Ok(())
        };
    let page_config = PipelineConfig {
        worker_count: transaction_page_workers,
        max_in_flight_tasks: transaction_page_workers.max(1).saturating_mul(2),
        max_in_flight_bytes: args.pipeline_memory_limit_bytes,
        first_sequence: 0,
    };
    let mut make_page_encoder = |_| {
        let effect_encoder = effect_chunks::EffectEncoder::new(0)?;
        let mut arena_encoder = TransactionArenaEncoder::new();
        let log_pubkeys = Arc::clone(&log_pubkeys);
        Ok::<_, anyhow::Error>(move |task: ResolvedBlockInput| {
            let pure_conversion_started = Instant::now();
            let ConvertedResolvedBlock {
                commit,
                transactions,
                block_rewards,
                delta,
            } = convert_resolved_block(task, &log_pubkeys)?;
            let pure_conversion_time = pure_conversion_started.elapsed();
            let effect_worker_started = Instant::now();
            let (commit, prepared_effects) = prepare_transaction_effects(&effect_encoder, commit)?;
            let effect_worker_time = effect_worker_started.elapsed();
            let arena_worker_started = Instant::now();
            let arena = arena_encoder.prepare(&transactions)?;
            Ok(PreparedTransactionBlock {
                commit,
                prepared_effects,
                arena,
                block_rewards,
                delta,
                pure_conversion_time,
                effect_worker_time,
                arena_worker_time: arena_worker_started.elapsed(),
            })
        })
    };
    let encoding_stats = if transaction_page_workers == 0 {
        run_inline_ordered_encoding_stage(
            page_config,
            &mut make_page_encoder,
            &mut commit_prepared,
            |sink| {
                produce(&mut |task| {
                    sink.submit(task)
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                })
            },
        )
    } else {
        run_ordered_encoding_stage(
            page_config,
            &mut make_page_encoder,
            &mut commit_prepared,
            |sink| {
                produce(&mut |task| {
                    sink.submit(task)
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                })
            },
        )
    }
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let reader_stats = reader_stats_result.context("ordered reader did not return statistics")?;
    report.transaction_page_peak_in_flight_blocks = encoding_stats.peak_in_flight_tasks;
    report.transaction_page_peak_in_flight_bytes = encoding_stats.peak_reserved_bytes;
    report.peak_in_flight_blocks = reader_stats
        .max_blocks_per_batch
        .saturating_add(encoding_stats.peak_in_flight_tasks);
    report.peak_in_flight_bytes = reader_stats
        .max_compressed_batch_bytes
        .saturating_mul(reader_config.compressed_buffer_count)
        .saturating_add(
            usize::try_from(reader_stats.max_declared_uncompressed_batch_bytes)
                .unwrap_or(usize::MAX)
                .saturating_mul(8),
        )
        .saturating_add(
            block_workers.saturating_mul(reader_stats.max_retained_decompressed_buffer_bytes),
        )
        .saturating_add(encoding_stats.peak_reserved_bytes);
    report.source_read_calls = reader_stats.read_call_count;
    report.source_read_batches = reader_stats.batch_count;
    report.source_max_batch_blocks = reader_stats.max_blocks_per_batch;
    report.source_max_compressed_batch_bytes = reader_stats.max_compressed_batch_bytes;
    report.source_max_declared_uncompressed_batch_bytes =
        reader_stats.max_declared_uncompressed_batch_bytes;
    report.source_max_retained_decompressed_buffer_bytes =
        reader_stats.max_retained_decompressed_buffer_bytes;
    ensure!(
        reader_stats.block_count == report.blocks
            && committed_blocks == report.blocks
            && report.blocks
                == u64::try_from(index.rows.len()).context("block count exceeds u64")?,
        "ordered reader, page commit, and converter block counts do not match the selected source range"
    );
    ensure!(
        reader_stats.compressed_bytes == source_block_bytes,
        "ordered reader consumed {} compressed bytes, expected {source_block_bytes}",
        reader_stats.compressed_bytes
    );
    ensure!(
        report.transactions == source_expected_transactions
            && committed_transactions == report.transactions,
        "converter mapped {} transactions and committed {committed_transactions}, expected {source_expected_transactions}",
        report.transactions,
    );
    let block_pipeline_elapsed = block_pipeline_started.elapsed();
    report.timings.block_pipeline_wall_ms = duration_millis(block_pipeline_elapsed);
    report.timings.source_read_ms = duration_millis(reader_stats.producer_read_wall_time);
    report.timings.source_decode_project_ms =
        duration_millis(reader_stats.coordinator_decode_project_wall_time);
    report.timings.source_outer_decode_sum_ms =
        duration_millis(reader_stats.worker_decompress_decode_sum_time);
    report.timings.source_projection_sum_ms =
        duration_millis(reader_stats.worker_projection_sum_time);
    report.timings.borrowed_to_owned_sum_ms = duration_millis(borrowed_to_owned_sum);
    report.timings.source_message_decode_sum_ms = duration_millis(source_message_decode_sum);
    report.timings.signed_message_proof_sum_ms = duration_millis(signed_message_proof_sum);
    report.timings.source_metadata_decode_sum_ms = duration_millis(source_metadata_decode_sum);
    report.timings.source_validation_sum_ms = duration_millis(source_validation_sum);
    report.timings.source_inline_log_pubkey_discovery_sum_ms =
        duration_millis(source_inline_log_pubkey_discovery_sum);
    report.timings.source_wait_for_free_buffer_ms =
        duration_millis(reader_stats.producer_wait_for_free_buffer_time);
    report.timings.source_wait_for_ready_batch_ms =
        duration_millis(reader_stats.coordinator_wait_for_ready_batch_time);
    report.timings.ordered_commit_sum_ms = duration_millis(ordered_commit_sum);
    let ordered_id_hash_assignment_sum =
        ordered_raw_pubkey_assignment_sum.saturating_add(ordered_hash_assignment_sum);
    let ordered_resolution_validation_sum = Duration::ZERO;
    report.timings.transaction_transform_sum_ms = duration_millis(
        transaction_transform_sum.saturating_add(stage3_pure_resolved_conversion_sum),
    );
    report.timings.ordered_id_hash_assignment_sum_ms =
        duration_millis(ordered_id_hash_assignment_sum);
    report.timings.ordered_raw_pubkey_assignment_sum_ms =
        duration_millis(ordered_raw_pubkey_assignment_sum);
    report.timings.ordered_hash_assignment_sum_ms = duration_millis(ordered_hash_assignment_sum);
    report.timings.ordered_serial_preparation_sum_ms = duration_millis(transaction_transform_sum);
    report.timings.ordered_resolution_validation_sum_ms =
        duration_millis(ordered_resolution_validation_sum);
    report.timings.pure_resolved_conversion_sum_ms =
        duration_millis(stage3_pure_resolved_conversion_sum);
    report.timings.stage3_pure_resolved_conversion_sum_ms =
        duration_millis(stage3_pure_resolved_conversion_sum);
    report.timings.effect_worker_prepare_sum_ms = duration_millis(effect_worker_prepare_sum);
    report.timings.effect_final_append_sum_ms = duration_millis(effect_final_append_sum);
    report.timings.effect_prepare_and_write_sum_ms = duration_millis(effect_prepare_and_write_sum);
    report.timings.transaction_page_worker_sum_ms = duration_millis(transaction_page_worker_sum);
    report.timings.transaction_page_final_commit_sum_ms =
        duration_millis(transaction_page_final_commit_sum);
    report.timings.stage3_worker_effect_and_arena_sum_ms =
        duration_millis(stage3_worker_effect_and_arena_sum);
    report.timings.final_append_and_offset_commit_sum_ms =
        duration_millis(final_append_and_offset_commit_sum);
    report.timings.transaction_page_prepare_and_write_sum_ms =
        duration_millis(transaction_page_prepare_and_write_sum);
    report.timings.catalog_commit_sum_ms = duration_millis(catalog_commit_sum);
    let block_pipeline_seconds = block_pipeline_elapsed.as_secs_f64();
    if block_pipeline_seconds > 0.0 {
        report.block_pipeline_transactions_per_second =
            report.transactions as f64 / block_pipeline_seconds;
        report.block_pipeline_source_mib_per_second =
            source_block_bytes as f64 / (1024.0 * 1024.0) / block_pipeline_seconds;
    }
    emit_phase("block_pipeline", "complete", block_pipeline_elapsed);
    let finalize_planes_started = Instant::now();
    transactions_plane.finish(archive_id, report.transactions, &mut report)?;
    inner_plane.finish(
        archive_id,
        effect_record_counts[transactions_codec::EffectKind::InnerInstructions.index()],
        &mut report,
    )?;
    outcomes_plane.finish(
        archive_id,
        effect_record_counts[transactions_codec::EffectKind::Outcome.index()],
        &mut report,
    )?;
    balances_plane.finish(
        archive_id,
        effect_record_counts[transactions_codec::EffectKind::Balances.index()],
        &mut report,
    )?;
    token_balances_plane.finish(
        archive_id,
        effect_record_counts[transactions_codec::EffectKind::TokenBalances.index()],
        &mut report,
    )?;
    logs_plane.finish(
        archive_id,
        effect_record_counts[transactions_codec::EffectKind::Logs.index()],
        &mut report,
    )?;
    rewards_plane.finish(
        archive_id,
        effect_record_counts[transactions_codec::EffectKind::Rewards.index()],
        &mut report,
    )?;
    block_rewards_plane.finish(archive_id, report.block_rewards_stored, &mut report)?;

    if args.benchmark_prefix_blocks.is_some() {
        let catalog_object = catalog_writer
            .finish(archive_id, report.blocks)
            .context("finish benchmark catalog/blocks.wincode")?;
        report
            .plane_bytes
            .insert(catalog_blocks::PATH.to_owned(), catalog_object.file_bytes);
        report.plane_bytes.insert(
            blockzilla_index_archive_format::sidecars::poh::PATH.to_owned(),
            retained_poh.object.file_bytes,
        );
        report.poh_entries = retained_poh.entry_count;
        report.plane_bytes.insert(
            blockzilla_index_archive_format::sidecars::shredding::PATH.to_owned(),
            retained_shredding.object.file_bytes,
        );
        report.shredding_boundaries = retained_shredding.boundary_count;
        report.shredding_recorded_empty_blocks = retained_shredding.recorded_empty_blocks;
        let finalize_planes_elapsed = finalize_planes_started.elapsed();
        report.timings.finalize_planes_and_sidecars_ms = duration_millis(finalize_planes_elapsed);
        report.timings.conversion_before_report_ms = duration_millis(conversion_started.elapsed());
        source_context
            .source
            .verify_unchanged()
            .context("source object changed during benchmark")?;
        emit_phase(
            "finalize_benchmark_planes",
            "complete",
            finalize_planes_elapsed,
        );
        let report_json = serde_json::to_string_pretty(&report)? + "\n";
        write_and_sync(
            &output.join("benchmark-report.json"),
            report_json.as_bytes(),
        )
        .context("write benchmark-report.json")?;
        sync_candidate_tree(&output).context("sync benchmark tree")?;
        fs::rename(&output, &final_output).with_context(|| {
            format!(
                "finish non-publishable benchmark {} as {}",
                output.display(),
                final_output.display()
            )
        })?;
        sync_directory(usable_parent(&final_output)).context("sync benchmark parent directory")?;
        print!("{report_json}");
        return Ok(());
    }

    // Signatures are already a flat array of 64-byte records in transaction
    // order, so they are carried across verbatim. The catalog's first_signature
    // plus each transaction row's num_required_signatures associates a transaction
    // with its exact signature range.
    let signatures_src = source.join(ARCHIVE_V2_SIGNATURES_FILE);
    let signature_records = source_context
        .source
        .size(ARCHIVE_V2_SIGNATURES_FILE)?
        .context("source signatures are missing")?
        / 64;
    let signatures = copy_file_payload(
        source_context
            .source
            .pinned_file_clone(ARCHIVE_V2_SIGNATURES_FILE)?
            .context("source signatures are missing")?,
        &signatures_src,
        &output,
        blockzilla_index_archive_format::sidecars::signatures::PATH,
        archive_id,
        signature_records,
    )
    .context("copy signatures")?;
    report.plane_bytes.insert(
        blockzilla_index_archive_format::sidecars::signatures::PATH.to_owned(),
        signatures.file_bytes,
    );
    report.signatures = signature_records;

    // Existing source IDs stay unchanged. Inline keys are resolved through the
    // authenticated source MPHF and appended once only when they are truly new.
    // This keeps dictionary/pubkeys.pages as the sole owner of 32-byte keys.
    let registry_src = source.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let registry_records = u64::from(target_pubkeys.record_count());
    let pubkey_dictionary = copy_file_payload_with_suffix(
        source_context
            .source
            .pinned_file_clone(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)?
            .context("source pubkey registry is missing")?,
        &registry_src,
        &output,
        pubkeys::PATH,
        archive_id,
        registry_records,
        target_pubkeys.appended_bytes(),
    )
    .context("write target pubkey dictionary")?;
    report
        .plane_bytes
        .insert(pubkeys::PATH.to_owned(), pubkey_dictionary.file_bytes);
    report.pubkey_dictionary_records = registry_records;

    report.plane_bytes.insert(
        blockzilla_index_archive_format::sidecars::poh::PATH.to_owned(),
        retained_poh.object.file_bytes,
    );
    report.poh_entries = retained_poh.entry_count;
    report.poh_signature_count_recovered_blocks = 0;
    report.poh_signature_count_legacy_unknown_blocks = retained_poh
        .mappings
        .iter()
        .filter(|mapping| {
            mapping.signature_count_coverage
                == blockzilla_index_archive_convert::source_v2_sidecars::BlockSignatureCountCoverage::LegacyUnknown
        })
        .count() as u64;
    report.plane_bytes.insert(
        blockzilla_index_archive_format::sidecars::shredding::PATH.to_owned(),
        retained_shredding.object.file_bytes,
    );
    report.shredding_boundaries = retained_shredding.boundary_count;
    report.shredding_recorded_empty_blocks = retained_shredding.recorded_empty_blocks;

    // Genesis also remains the exact source byte image.
    for (name, target) in [(ARCHIVE_V2_GENESIS_BIN_FILE, "sidecars/genesis.bin")] {
        if source_context.source.size(name)?.is_none() {
            continue;
        }
        let out = output.join(target);
        fs::create_dir_all(out.parent().expect("has parent")).context("create sidecars dir")?;
        let bytes = read_pinned_all(&source_context.source, name)?;
        write_and_sync(&out, &bytes).with_context(|| format!("write exact {target}"))?;
        report.plane_bytes.insert(
            target.to_owned(),
            u64::try_from(bytes.len()).context("genesis size exceeds u64")?,
        );
    }

    // Written after the loop when every account use has been observed. Both
    // flags come from this generation's own ledger, so they cannot disagree
    // with it. The dense byte plane avoids millions of ordered-tree nodes.
    let registry_entries = report.pubkey_dictionary_records as u32;
    observed_account_flags.resize(account_flags::byte_len(registry_entries), 0);
    ensure!(
        observed_account_flags.len() == account_flags::byte_len(registry_entries),
        "account-flag plane length drifted from the pubkey dictionary"
    );
    let flags_path = output.join(account_flags::PATH);
    fs::create_dir_all(flags_path.parent().expect("has parent"))
        .context("create dictionary dir")?;
    let flags_object = write_payload(
        &output,
        account_flags::PATH,
        archive_id,
        u64::from(registry_entries),
        &observed_account_flags,
    )
    .context("write account flags")?;
    report
        .plane_bytes
        .insert(account_flags::PATH.to_owned(), flags_object.file_bytes);
    report.program_accounts =
        account_flags::count_with(&observed_account_flags, account_flags::FLAG_PROGRAM) as u64;
    report.signer_accounts =
        account_flags::count_with(&observed_account_flags, account_flags::FLAG_SIGNER) as u64;
    report.unused_accounts = u64::from(registry_entries)
        - account_flags::count_with(&observed_account_flags, account_flags::KNOWN_FLAGS) as u64;

    let hash_records = u64::try_from(blockhash_dictionary.len() / blockhashes::RECORD_LEN)
        .context("blockhash record count exceeds u64")?;
    if let Some(ordinal) = max_non_poh_hash_ref {
        ensure!(
            u64::from(ordinal) < hash_records,
            "non-PoH hash reference {ordinal} is outside {hash_records} records"
        );
    }
    if let Some(ordinal) = max_poh_hash_ref {
        ensure!(
            u64::from(ordinal) < report.blocks,
            "PoH hash reference {ordinal} is outside {} blocks",
            report.blocks
        );
    }
    for hash in blockhash_dictionary.chunks_exact(32) {
        let hash: [u8; 32] = hash.try_into().expect("blockhash dictionary is aligned");
        ensure!(
            final_poh_hash_index
                .binary_search_by_key(&hash, |candidate| candidate.0)
                .is_err(),
            "a published PoH blockhash is duplicated in dictionary/blockhashes"
        );
    }
    let dictionary_object = write_payload(
        &output,
        blockhashes::PATH,
        archive_id,
        hash_records,
        &blockhash_dictionary,
    )
    .context("write blockhash dictionary")?;
    report
        .plane_bytes
        .insert(blockhashes::PATH.to_owned(), dictionary_object.file_bytes);
    report.nonce_hashes_interned =
        u64::try_from(nonce_hashes.len()).context("nonce hash count exceeds u64")?;
    report.blockhash_dictionary_records =
        (blockhash_dictionary.len() / blockhashes::RECORD_LEN) as u64;

    let catalog_object = catalog_writer
        .finish(archive_id, report.blocks)
        .context("finish catalog/blocks.wincode")?;
    report
        .plane_bytes
        .insert(catalog_blocks::PATH.to_owned(), catalog_object.file_bytes);
    let finalize_planes_elapsed = finalize_planes_started.elapsed();
    report.timings.finalize_planes_and_sidecars_ms = duration_millis(finalize_planes_elapsed);
    emit_phase(
        "finalize_planes_and_sidecars",
        "complete",
        finalize_planes_elapsed,
    );

    // Every production read used descriptors from this shared pinned source.
    // Identity, size, and change timestamps must still match before handoff.
    let content_binding_started = Instant::now();
    source_context
        .source
        .verify_unchanged()
        .context("source object changed during conversion")?;
    let content_archive_id = derive_content_archive_id(&output, source_context.epoch)
        .context("derive canonical target content ID")?;
    patch_archive_id(&output, archive_id, content_archive_id)
        .context("bind target objects to canonical content ID")?;
    report.archive_id = content_archive_id.to_hex();

    write_conversion_candidate_hash(&output).context("write conversion candidate checksum")?;
    let content_binding_elapsed = content_binding_started.elapsed();
    report.timings.content_binding_ms = duration_millis(content_binding_elapsed);
    emit_phase("content_binding", "complete", content_binding_elapsed);

    let derived_indexes_started = Instant::now();
    let derived = build_all_derived_indexes(
        &output,
        DerivedIndexBuildOptions {
            workers: args.workers,
            total_sort_memory_bytes: args.pipeline_memory_limit_bytes,
        },
    )
    .context("build all required derived indexes")?;
    ensure!(
        derived.archive_id == content_archive_id,
        "derived indexes have a different archive identity"
    );
    ensure!(
        derived.blocks == report.blocks && derived.basic.transactions == report.transactions,
        "derived-index populations disagree with canonical conversion totals"
    );
    report.derived_index_workers = derived.workers_used;
    report.derived_index_sort_memory_bytes = derived.total_sort_memory_bytes;
    report.derived_index_sort_memory_per_builder_bytes =
        derived.sort_memory_per_active_builder_bytes;
    report.account_index_postings = derived.accounts.postings;
    report.account_index_pages = derived.accounts.pages;
    report.account_index_continuation_pages = derived.accounts.continuation_pages;
    report.account_index_max_postings_per_page = derived.accounts.max_postings_per_page;
    report.account_index_peak_page_postings = derived.accounts.peak_page_postings;
    report.program_index_postings = derived.programs.postings;
    report.selector_index_postings = derived.selectors.postings;
    for (path, bytes) in [
        (target_indexes::slots::PATH, derived.basic.slots_bytes),
        (
            target_indexes::accounts::PATH,
            derived.accounts.object_bytes,
        ),
        (
            target_indexes::programs::PATH,
            derived.programs.object_bytes,
        ),
        (
            target_indexes::selectors::PATH,
            derived.selectors.object_bytes,
        ),
    ] {
        report.plane_bytes.insert(path.to_owned(), bytes);
    }
    let derived_indexes_elapsed = derived_indexes_started.elapsed();
    report.timings.derived_indexes_ms = duration_millis(derived_indexes_elapsed);
    emit_phase("derived_indexes", "complete", derived_indexes_elapsed);

    let candidate_validation_started = Instant::now();
    report.missing_required_objects = LAYOUT
        .iter()
        .filter(|spec| spec.required_for_epoch(source_context.epoch))
        .filter(|spec| !output.join(spec.path).is_file())
        .map(|spec| spec.path)
        .collect();
    ensure!(
        report.missing_required_objects.is_empty(),
        "required candidate objects are missing: {:?}",
        report.missing_required_objects
    );
    let validation = validate_complete_candidate(&output, source_context.epoch)
        .context("validate complete physical candidate")?;
    ensure!(
        validation.archive_id == content_archive_id,
        "complete candidate has a different archive identity"
    );
    report.physical_layout_valid = true;
    report.required_objects = validation.required_objects;
    let candidate_validation_elapsed = candidate_validation_started.elapsed();
    report.timings.candidate_validation_ms = duration_millis(candidate_validation_elapsed);
    report.timings.conversion_before_report_ms = duration_millis(conversion_started.elapsed());
    emit_phase(
        "candidate_validation",
        "complete",
        candidate_validation_elapsed,
    );

    let report_json = serde_json::to_string_pretty(&report)? + "\n";
    write_and_sync(&output.join("convert-report.json"), report_json.as_bytes())
        .context("write convert-report.json")?;
    sync_candidate_tree(&output).context("sync completed conversion candidate tree")?;
    fs::rename(&output, &final_output).with_context(|| {
        format!(
            "finish non-publishable conversion candidate {} as {}",
            output.display(),
            final_output.display()
        )
    })?;
    sync_directory(usable_parent(&final_output))
        .context("sync conversion candidate parent directory")?;
    print!("{report_json}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use blockzilla_format::{
        DataTable, KeyIndex, LogEvent, RawPubkeyCompactor, StringTable, parse_logs_with_compactor,
        program_logs::{
            ProgramLog,
            system_program::{
                NonceAction, PubkeyOrString, SystemAddress, SystemInstructionLog, SystemProgramLog,
            },
            token_2022::Token2022Log,
        },
    };
    use tempfile::tempdir;

    use super::*;

    fn empty_test_metadata(account_count: usize) -> blockzilla_format::CompactMetaV1 {
        blockzilla_format::CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: vec![0; account_count],
            post_balances: vec![0; account_count],
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

    fn legacy_projection_fixture(
        metadata: blockzilla_format::CompactMetaV1,
    ) -> (
        blockzilla_format::ArchiveV2HotTxRow,
        ReconstructedSourceTransaction,
    ) {
        let mut flags = ARCHIVE_V2_TX_FLAG_HAS_METADATA;
        if metadata.err.is_some() {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_ERROR;
        }
        if metadata.return_data.is_some() {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA;
        }
        if metadata.logs.is_some() {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_LOGS;
        }
        if metadata.inner_instructions.is_some() {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_INNER_IX;
        }
        if !metadata.pre_token_balances.is_empty() || !metadata.post_token_balances.is_empty() {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES;
        }
        if !metadata.loaded_writable_addresses.is_empty()
            || !metadata.loaded_readonly_addresses.is_empty()
        {
            flags |= ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES;
        }
        (
            blockzilla_format::ArchiveV2HotTxRow {
                tx_index: 0,
                flags,
                signature_count: 1,
                ..blockzilla_format::ArchiveV2HotTxRow::default()
            },
            ReconstructedSourceTransaction {
                payload: ArchiveV2HotMessagePayload::Legacy(
                    blockzilla_format::ArchiveV2HotLegacyMessage {
                        header: blockzilla_format::CompactMessageHeader {
                            num_required_signatures: 1,
                            num_readonly_signed_accounts: 0,
                            num_readonly_unsigned_accounts: 0,
                        },
                        account_keys: vec![CompactPubkey::Id(1)],
                        recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
                        instructions: Vec::new(),
                    },
                ),
                metadata: Some(metadata),
                exact_instruction_data: Vec::new(),
                message_decode_time: Duration::ZERO,
                signed_message_proof_time: Duration::ZERO,
                metadata_decode_time: Duration::ZERO,
            },
        )
    }

    #[test]
    fn bare_output_uses_the_current_directory_for_staging_and_sync() {
        let output = Path::new("archive-out");
        assert_eq!(usable_parent(output), Path::new("."));
        assert_eq!(
            staging_path(output).unwrap(),
            PathBuf::from(format!("./.archive-out.building-{}", std::process::id()))
        );
    }

    #[test]
    fn fixture_message_schema_requires_an_explicit_known_name() {
        assert_eq!(
            parse_fixture_message_schema("current").unwrap(),
            CompactV2MessageSchema::Current
        );
        assert_eq!(
            parse_fixture_message_schema("may24-pre-unknown-fallbacks").unwrap(),
            CompactV2MessageSchema::May24PreUnknownFallbacks
        );
        assert!(parse_fixture_message_schema("auto").is_err());
    }

    #[test]
    fn fixture_metadata_schema_requires_an_explicit_known_name() {
        assert_eq!(
            parse_fixture_metadata_schema("current-typed-error").unwrap(),
            CompactV2MetadataSchema::CurrentTypedError
        );
        assert_eq!(
            parse_fixture_metadata_schema("legacy-raw-error").unwrap(),
            CompactV2MetadataSchema::LegacyRawError
        );
        assert!(parse_fixture_metadata_schema("auto").is_err());
    }

    #[test]
    fn unpublished_source_without_an_explicit_message_schema_fails_at_admission() {
        let root = tempdir().unwrap();
        let args = Args {
            source: root.path().to_path_buf(),
            output: root.path().join("output"),
            workers: 1,
            pipeline_memory_limit_bytes: 1 << 20,
            epoch: Some(2),
            slots_per_epoch: Some(432_000),
            fixture_source: true,
            fixture_message_schema: None,
            fixture_metadata_schema: Some(CompactV2MetadataSchema::CurrentTypedError),
            fixture_previous_blockhash: None,
            fixture_previous_slot: None,
            benchmark_prefix_blocks: None,
        };
        let error = validate_source_publication(root.path(), &args)
            .err()
            .expect("fixture source without explicit schema must fail");
        assert!(
            error
                .to_string()
                .contains("requires --fixture-message-schema")
        );
    }

    #[test]
    fn unpublished_source_without_an_explicit_metadata_schema_fails_at_admission() {
        let root = tempdir().unwrap();
        let args = Args {
            source: root.path().to_path_buf(),
            output: root.path().join("output"),
            workers: 1,
            pipeline_memory_limit_bytes: 1 << 20,
            epoch: Some(900),
            slots_per_epoch: Some(432_000),
            fixture_source: true,
            fixture_message_schema: Some(CompactV2MessageSchema::Current),
            fixture_metadata_schema: None,
            fixture_previous_blockhash: None,
            fixture_previous_slot: None,
            benchmark_prefix_blocks: None,
        };
        let error = validate_source_publication(root.path(), &args)
            .err()
            .expect("fixture source without explicit metadata schema must fail");
        assert!(
            error
                .to_string()
                .contains("requires --fixture-metadata-schema")
        );
    }

    #[test]
    fn published_source_without_bound_metadata_selector_fails_closed() {
        let manifest = blockzilla_read_sdk::manifest::GenerationManifest {
            schema_version: 1,
            cluster_id: "mainnet-beta".to_owned(),
            epoch: 900,
            generation_id: "test".to_owned(),
            generation_digest: "a".repeat(64),
            slots_per_epoch: 432_000,
            complete: true,
            files: Vec::new(),
        };
        let error = select_published_metadata_schema(&manifest).unwrap_err();
        assert!(error.to_string().contains("no manifest-bound"));
        assert!(error.to_string().contains("refusing to infer"));
    }

    #[test]
    fn inline_pubkeys_are_interned_once_and_logs_store_only_the_target_id() {
        let root = tempdir().unwrap();
        let source_key = [1_u8; 32];
        let inline_key = [2_u8; 32];
        fs::write(
            root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            source_key,
        )
        .unwrap();
        KeyIndex::build(vec![source_key])
            .write(&root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();

        let source = PinnedLocalRangeSource::new(root.path());
        let resolver = PinnedPubkeyResolver::open(&source, ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
            .unwrap()
            .unwrap();
        let mut target = TargetPubkeyDictionary::open(&source, root.path(), 1).unwrap();
        assert_eq!(
            target
                .resolve_or_intern(&CompactPubkey::Raw(source_key))
                .unwrap(),
            1
        );
        assert_eq!(
            target
                .resolve_or_intern(&CompactPubkey::Raw(inline_key))
                .unwrap(),
            2
        );
        assert_eq!(
            target
                .resolve_or_intern(&CompactPubkey::Raw(inline_key))
                .unwrap(),
            2
        );
        assert_eq!(target.appended_bytes(), inline_key);

        let program = Pubkey::new_from_array(inline_key).to_string();
        let source_logs = parse_logs_with_compactor(
            &[format!("Program {program} invoke [1]")],
            &RawPubkeyCompactor,
        );
        let target_logs = convert_log_stream(&source_logs, &resolver, &mut target).unwrap();
        assert_eq!(target_logs.len(), 1);
        assert_eq!(target_logs[0].pubkey_ids, [2]);
        assert_eq!(target_logs[0].fragments, ["Program ", " invoke [1]"]);
        assert_eq!(target.record_count(), 2);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct DictionaryParityOutput {
        appended_pubkeys: Vec<u8>,
        account_flags: Vec<u8>,
        blockhashes: Vec<u8>,
        committed_ids: Vec<Vec<u32>>,
        raw_account_keys: u64,
    }

    struct DictionaryParityTask {
        sequence: u64,
        pubkeys: ResolvedPubkeyTable,
        visits: Vec<CompactPubkey>,
        delta: BlockConversionDelta,
    }

    fn dictionary_parity_output(workers: usize) -> DictionaryParityOutput {
        let root = tempdir().unwrap();
        let source_key = [1_u8; 32];
        fs::write(
            root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            source_key,
        )
        .unwrap();
        KeyIndex::build(vec![source_key])
            .write(&root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();
        let source = PinnedLocalRangeSource::new(root.path());
        let mut dictionary = TargetPubkeyDictionary::open(&source, root.path(), 1).unwrap();
        let mut blockhash_ordinals = BTreeMap::new();
        let mut blockhashes = Vec::new();
        let mut report = Report::default();
        let mut flags = vec![0; account_flags::byte_len(1)];
        let mut effect_counts = [0; transactions_codec::EFFECT_KIND_COUNT];
        let mut nonce_hashes = BTreeSet::new();
        let mut max_non_poh = None;
        let mut max_poh = None;
        let mut committed_ids = Vec::new();

        let blocks = [
            // This order freezes all first-seen sources used by B2: V0 table,
            // static/source key, loaded account, return data, token pre fields,
            // token post-only owner, inline-log raw key, transaction reward,
            // then block reward.
            vec![
                CompactPubkey::Raw([2; 32]),
                CompactPubkey::Raw(source_key),
                CompactPubkey::Raw([3; 32]),
                CompactPubkey::Raw([4; 32]),
                CompactPubkey::Raw([5; 32]),
                CompactPubkey::Raw([6; 32]),
                CompactPubkey::Raw([7; 32]),
                CompactPubkey::Raw([5; 32]),
                CompactPubkey::Raw([8; 32]),
                CompactPubkey::Raw([7; 32]),
                CompactPubkey::Raw([9; 32]),
                CompactPubkey::Raw([10; 32]),
                CompactPubkey::Raw([11; 32]),
            ],
            vec![CompactPubkey::Raw([8; 32]), CompactPubkey::Raw([12; 32])],
        ];
        let config = PipelineConfig {
            worker_count: workers,
            max_in_flight_tasks: 2,
            max_in_flight_bytes: 2,
            first_sequence: 0,
        };
        let produce = |sink: &mut dyn FnMut(OrderedTask<DictionaryParityTask>) -> Result<()>| {
            for (sequence, visits) in blocks.into_iter().enumerate() {
                let mut resolved = ResolvedPubkeyTable::new(1);
                for key in &visits {
                    resolve_and_record_pubkey(&mut dictionary, &mut resolved, key)?;
                }
                let flag_id = resolved.resolve(&CompactPubkey::Raw([7; 32])).ok();
                let mut delta = BlockConversionDelta {
                    raw_account_keys: if sequence == 0 { 2 } else { 1 },
                    ..BlockConversionDelta::default()
                };
                if let Some(flag_id) = flag_id {
                    delta.mark_account(
                        flag_id,
                        account_flags::FLAG_SIGNER | account_flags::FLAG_PROGRAM,
                    )?;
                }
                for byte in [40_u8 + sequence as u8, 50_u8 + sequence as u8] {
                    intern_non_poh_hash([byte; 32], &mut blockhash_ordinals, &mut blockhashes)?;
                }
                sink(OrderedTask::new(
                    sequence as u64,
                    1,
                    DictionaryParityTask {
                        sequence: sequence as u64,
                        pubkeys: resolved,
                        visits,
                        delta,
                    },
                ))?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let mut make_encoder = |_| {
            Ok::<_, anyhow::Error>(|mut task: DictionaryParityTask| {
                if workers > 1 && task.sequence == 0 {
                    std::thread::sleep(Duration::from_millis(10));
                }
                let ids = task
                    .visits
                    .iter()
                    .map(|key| task.pubkeys.target_pubkey_id(key))
                    .collect::<Result<Vec<_>>>()?;
                Ok((ids, task.delta))
            })
        };
        let mut commit = |_: u64, (ids, delta): (Vec<u32>, BlockConversionDelta)| {
            committed_ids.push(ids);
            delta.merge(
                &mut report,
                &mut flags,
                &mut effect_counts,
                &mut nonce_hashes,
                &mut max_non_poh,
                &mut max_poh,
            )
        };
        if workers == 0 {
            run_inline_ordered_encoding_stage(config, &mut make_encoder, &mut commit, |sink| {
                produce(&mut |task| {
                    sink.submit(task)
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                })
            })
            .unwrap();
        } else {
            run_ordered_encoding_stage(config, &mut make_encoder, &mut commit, |sink| {
                produce(&mut |task| {
                    sink.submit(task)
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                })
            })
            .unwrap();
        }
        flags.resize(account_flags::byte_len(dictionary.record_count()), 0);
        DictionaryParityOutput {
            appended_pubkeys: dictionary.appended_bytes().to_vec(),
            account_flags: flags,
            blockhashes,
            committed_ids,
            raw_account_keys: report.raw_account_keys,
        }
    }

    fn b3_projection_pipeline_output(workers: usize) -> (Vec<u8>, Vec<Vec<u32>>) {
        let root = tempdir().unwrap();
        let source_key = [1_u8; 32];
        fs::write(
            root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            source_key,
        )
        .unwrap();
        KeyIndex::build(vec![source_key])
            .write(&root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();
        let source = PinnedLocalRangeSource::new(root.path());
        let mut dictionary = TargetPubkeyDictionary::open(&source, root.path(), 1).unwrap();
        let mut committed = Vec::new();
        let config = PipelineConfig {
            worker_count: workers,
            max_in_flight_tasks: 2,
            max_in_flight_bytes: 2,
            first_sequence: 0,
        };
        let mut produce =
            |sink: &mut dyn FnMut(OrderedTask<DictionaryParityTask>) -> Result<()>| {
                for (sequence, (return_key, reward_key)) in
                    [(2_u8, 3_u8), (3, 4)].into_iter().enumerate()
                {
                    let mut metadata = empty_test_metadata(1);
                    metadata.return_data = Some(blockzilla_format::CompactReturnData {
                        program_id: CompactPubkey::Raw([return_key; 32]),
                        data: Vec::new(),
                    });
                    metadata.rewards.push(blockzilla_format::CompactReward {
                        pubkey: CompactPubkey::Raw([reward_key; 32]),
                        lamports: 1,
                        post_balance: 1,
                        reward_type: 0,
                        commission: None,
                    });
                    let (row, reconstructed) = legacy_projection_fixture(metadata);
                    let (_, raw_visits, _) = validate_reconstructed_transactions(
                        sequence as u64,
                        &[row],
                        vec![reconstructed],
                        1,
                    )?;
                    let visits = raw_visits
                        .into_iter()
                        .map(CompactPubkey::Raw)
                        .collect::<Vec<_>>();
                    let mut resolved = ResolvedPubkeyTable::new(1);
                    for key in &visits {
                        resolve_and_record_pubkey(&mut dictionary, &mut resolved, key)?;
                    }
                    sink(OrderedTask::new(
                        sequence as u64,
                        1,
                        DictionaryParityTask {
                            sequence: sequence as u64,
                            pubkeys: resolved,
                            visits,
                            delta: BlockConversionDelta::default(),
                        },
                    ))?;
                }
                Ok(())
            };
        let mut make_encoder = |_| {
            Ok::<_, anyhow::Error>(|mut task: DictionaryParityTask| {
                if workers > 1 && task.sequence == 0 {
                    std::thread::sleep(Duration::from_millis(10));
                }
                task.visits
                    .iter()
                    .map(|key| task.pubkeys.target_pubkey_id(key))
                    .collect::<Result<Vec<_>>>()
            })
        };
        let mut commit = |_: u64, ids| {
            committed.push(ids);
            Ok::<_, anyhow::Error>(())
        };
        if workers == 0 {
            run_inline_ordered_encoding_stage(config, &mut make_encoder, &mut commit, |sink| {
                produce(&mut |task| {
                    sink.submit(task)
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                })
            })
            .unwrap();
        } else {
            run_ordered_encoding_stage(config, &mut make_encoder, &mut commit, |sink| {
                produce(&mut |task| {
                    sink.submit(task)
                        .map_err(|error| anyhow::anyhow!(error.to_string()))
                })
            })
            .unwrap();
        }
        (dictionary.appended_bytes().to_vec(), committed)
    }

    #[test]
    fn b2_dictionary_flags_and_hashes_match_inline_one_and_reverse_many_workers() {
        let inline = dictionary_parity_output(0);
        assert_eq!(dictionary_parity_output(1), inline);
        assert_eq!(dictionary_parity_output(4), inline);
        assert_eq!(inline.raw_account_keys, 3);
        let flagged_id = 7_u32;
        assert_eq!(
            account_flags::flags_at(&inline.account_flags, flagged_id).unwrap(),
            account_flags::FLAG_SIGNER | account_flags::FLAG_PROGRAM
        );
        let expected_pubkeys = (2_u8..=12).flat_map(|byte| [byte; 32]).collect::<Vec<_>>();
        assert_eq!(inline.appended_pubkeys, expected_pubkeys);
        let expected_hashes = [40_u8, 50, 41, 51]
            .into_iter()
            .flat_map(|byte| [byte; 32])
            .collect::<Vec<_>>();
        assert_eq!(inline.blockhashes, expected_hashes);
    }

    #[test]
    fn b3_production_projection_matches_inline_one_and_reverse_many_workers() {
        let inline = b3_projection_pipeline_output(0);
        assert_eq!(b3_projection_pipeline_output(1), inline);
        assert_eq!(b3_projection_pipeline_output(4), inline);
        assert_eq!(
            inline.0,
            (2_u8..=4).flat_map(|byte| [byte; 32]).collect::<Vec<_>>()
        );
        assert_eq!(inline.1, vec![vec![2, 3], vec![3, 4]]);
    }

    struct IdTwoCompactor;

    impl blockzilla_format::PubkeyCompactor for IdTwoCompactor {
        fn compact_str(&self, _: &str) -> Option<CompactPubkey> {
            Some(CompactPubkey::Id(2))
        }
    }

    #[test]
    fn b3_source_projection_rejects_ids_from_each_metadata_lane() {
        for lane in ["return", "token", "transaction reward", "log"] {
            let mut metadata = empty_test_metadata(1);
            match lane {
                "return" => {
                    metadata.return_data = Some(blockzilla_format::CompactReturnData {
                        program_id: CompactPubkey::Id(2),
                        data: Vec::new(),
                    });
                }
                "token" => {
                    metadata.pre_token_balances.push(CompactTokenBalance {
                        account_index: 0,
                        mint: Some(CompactPubkey::Id(2)),
                        owner: None,
                        program_id: None,
                        amount: 1,
                        decimals: 0,
                    });
                }
                "transaction reward" => {
                    metadata.rewards.push(blockzilla_format::CompactReward {
                        pubkey: CompactPubkey::Id(2),
                        lamports: 1,
                        post_balance: 1,
                        reward_type: 0,
                        commission: None,
                    });
                }
                "log" => {
                    let key = Pubkey::new_from_array([33; 32]).to_string();
                    metadata.logs = Some(parse_logs_with_compactor(
                        &[format!("Program {key} invoke [1]")],
                        &IdTwoCompactor,
                    ));
                }
                _ => unreachable!(),
            }
            let (tx, reconstructed) = legacy_projection_fixture(metadata);
            let error =
                validate_and_project_source_transaction(1, &tx, reconstructed, 1, &mut Vec::new())
                    .unwrap_err();
            assert!(
                error.to_string().contains("outside the valid 1..=1 range"),
                "{lane} returned {error:#}"
            );
        }
    }

    #[test]
    fn b3_block_validation_reports_the_lowest_transaction_first() {
        let (mut first_row, first) = legacy_projection_fixture(empty_test_metadata(1));
        first_row.flags = 0;
        let (mut second_row, second) = legacy_projection_fixture(empty_test_metadata(1));
        second_row.tx_index = 1;
        second_row.flags = 0;
        let error = validate_reconstructed_transactions(
            1,
            &[first_row, second_row],
            vec![first, second],
            1,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("transaction 0 source flag projection"),
            "{error:#}"
        );
    }

    #[test]
    fn b3_source_validation_rejects_malformed_shape_table() {
        for (case, expected) in [
            ("header", "required signatures exceed static accounts"),
            ("instruction", "top-level program index"),
            ("balances", "balance lanes do not match"),
        ] {
            let mut metadata = empty_test_metadata(1);
            if case == "balances" {
                metadata.pre_balances.clear();
            }
            let (mut row, mut reconstructed) = legacy_projection_fixture(metadata);
            match (&mut reconstructed.payload, case) {
                (ArchiveV2HotMessagePayload::Legacy(message), "header") => {
                    message.header.num_required_signatures = 2;
                    row.signature_count = 2;
                }
                (ArchiveV2HotMessagePayload::Legacy(message), "instruction") => {
                    message.instructions.push(ArchiveV2HotInstruction {
                        program_id_index: 1,
                        accounts: Vec::new(),
                        data: ArchiveV2HotInstructionData::Raw(Vec::new()),
                    });
                    reconstructed.exact_instruction_data.push(Vec::new());
                }
                _ => {}
            }
            let error =
                validate_and_project_source_transaction(1, &row, reconstructed, 1, &mut Vec::new())
                    .unwrap_err();
            assert!(
                format!("{error:#}").contains(expected),
                "{case} returned {error:#}"
            );
        }
    }

    #[test]
    fn b3_source_projection_rejects_loaded_and_block_reward_ids() {
        let mut metadata = empty_test_metadata(2);
        metadata.loaded_writable_addresses = vec![CompactPubkey::Id(2)];
        let payload = ArchiveV2HotMessagePayload::V0(blockzilla_format::ArchiveV2HotV0Message {
            header: blockzilla_format::CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
            instructions: Vec::new(),
            address_table_lookups: vec![blockzilla_format::OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(1),
                writable_indexes: vec![0],
                readonly_indexes: Vec::new(),
            }],
        });
        let tx = blockzilla_format::ArchiveV2HotTxRow {
            tx_index: 0,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
            signature_count: 1,
            ..blockzilla_format::ArchiveV2HotTxRow::default()
        };
        let error = validate_and_project_source_transaction(
            1,
            &tx,
            ReconstructedSourceTransaction {
                payload,
                metadata: Some(metadata),
                exact_instruction_data: Vec::new(),
                message_decode_time: Duration::ZERO,
                signed_message_proof_time: Duration::ZERO,
                metadata_decode_time: Duration::ZERO,
            },
            1,
            &mut Vec::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("outside the valid 1..=1 range"));

        let row = blockzilla_format::ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: 1,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count: 0,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 0,
        };
        let mut header = blockzilla_format::ArchiveV2HotBlockHeader {
            slot: 1,
            parent_slot: 0,
            blockhash_id: 9,
            previous_blockhash_id: 8,
            block_time: None,
            block_height: None,
            rewards: Some(blockzilla_format::ArchiveV2HotRewards {
                num_partitions: None,
                decoded: vec![blockzilla_format::CompactReward {
                    pubkey: CompactPubkey::Id(2),
                    lamports: 1,
                    post_balance: 1,
                    reward_type: 0,
                    commission: None,
                }],
            }),
        };
        let error = validate_source_block_tail(row, &header, 10, 1, &mut Vec::new()).unwrap_err();
        assert!(error.to_string().contains("does not match indexed block"));
        header.blockhash_id = 10;
        let error = validate_source_block_tail(row, &header, 10, 1, &mut Vec::new()).unwrap_err();
        assert!(error.to_string().contains("outside the valid 1..=1 range"));
    }

    #[test]
    fn b3_token_validation_keeps_range_before_key_and_duplicate_after_key() {
        let invalid_key = CompactTokenBalance {
            account_index: 1,
            mint: Some(CompactPubkey::Id(2)),
            owner: None,
            program_id: None,
            amount: 1,
            decimals: 0,
        };
        let range_error =
            validate_token_balance_visits(&[invalid_key], &[], 1, 1, &mut Vec::new()).unwrap_err();
        assert!(
            range_error
                .to_string()
                .contains("outside 1 resolved accounts")
        );

        let duplicate = CompactTokenBalance {
            account_index: 0,
            mint: Some(CompactPubkey::Raw([2; 32])),
            owner: None,
            program_id: None,
            amount: 1,
            decimals: 0,
        };
        let mut visits = Vec::new();
        let duplicate_error =
            validate_token_balance_visits(&[duplicate.clone(), duplicate], &[], 1, 1, &mut visits)
                .unwrap_err();
        assert!(duplicate_error.to_string().contains("duplicate pre-token"));
        assert_eq!(visits, vec![[2; 32], [2; 32]]);
    }

    #[test]
    fn b3_stage3_rejects_a_malformed_cpi_index_without_panicking() {
        let root = tempdir().unwrap();
        fs::write(
            root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            [1_u8; 32],
        )
        .unwrap();
        let source = PinnedLocalRangeSource::new(root.path());
        let log_pubkeys = PinnedPubkeyResolver::open(&source, ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
            .unwrap()
            .unwrap();
        let tx = blockzilla_format::ArchiveV2HotTxRow {
            tx_index: 0,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
            signature_count: 1,
            ..blockzilla_format::ArchiveV2HotTxRow::default()
        };
        let payload =
            ArchiveV2HotMessagePayload::Legacy(blockzilla_format::ArchiveV2HotLegacyMessage {
                header: blockzilla_format::CompactMessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![CompactPubkey::Id(1)],
                recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
                instructions: vec![ArchiveV2HotInstruction {
                    program_id_index: 0,
                    accounts: Vec::new(),
                    data: ArchiveV2HotInstructionData::Raw(Vec::new()),
                }],
            });
        let mut metadata = empty_test_metadata(1);
        metadata.inner_instructions = Some(vec![blockzilla_format::CompactInnerInstructions {
            index: 0,
            instructions: vec![blockzilla_format::CompactInnerInstruction {
                program_id_index: 9,
                accounts: Vec::new(),
                data: Vec::new(),
                stack_height: None,
            }],
        }]);
        let row = blockzilla_format::ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: 1,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count: 1,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 1,
        };
        let input = ResolvedBlockInput {
            row,
            block: blockzilla_format::ArchiveV2HotBlockBlob {
                header: blockzilla_format::ArchiveV2HotBlockHeader {
                    slot: 1,
                    parent_slot: 0,
                    blockhash_id: 0,
                    previous_blockhash_id: 0,
                    block_time: None,
                    block_height: None,
                    rewards: None,
                },
                tx_count: 1,
                tx_rows: vec![tx],
                message_bytes: Vec::new(),
                metadata_bytes: Vec::new(),
            },
            transactions: vec![ResolvedSourceTransaction {
                payload,
                metadata: Some(metadata),
                exact_instruction_data: vec![Vec::new()],
                recent_blockhash: transactions_codec::HashRef {
                    owner: transactions_codec::HashOwner::NonPoh,
                    ordinal: 0,
                },
                inline_log_pubkeys: Vec::new(),
                shape: ValidatedTransactionShape {
                    static_account_count: 1,
                    loaded_address_counts: LoadedAddressCounts::default(),
                    resolved_account_count: 1,
                },
            }],
            pubkeys: ResolvedPubkeyTable::new(1),
            blockhash: transactions_codec::HashRef {
                owner: transactions_codec::HashOwner::PohBlockFinal,
                ordinal: 0,
            },
            previous_blockhash: transactions_codec::HashRef {
                owner: transactions_codec::HashOwner::NonPoh,
                ordinal: 0,
            },
            delta: BlockConversionDelta::default(),
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            convert_resolved_block(input, &log_pubkeys)
        }));
        let error = match result.expect("stage3 shape validation must not panic") {
            Ok(_) => panic!("malformed CPI index must fail stage3 validation"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("CPI program index"));
    }

    #[test]
    fn b3_source_projection_freezes_the_exact_raw_resolver_trace() {
        let root = tempdir().unwrap();
        let source_key = [1_u8; 32];
        fs::write(
            root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            source_key,
        )
        .unwrap();
        KeyIndex::build(vec![source_key])
            .write(&root.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))
            .unwrap();
        let source = PinnedLocalRangeSource::new(root.path());
        let log_key_high = Pubkey::new_from_array([12; 32]).to_string();
        let log_key_low = Pubkey::new_from_array([11; 32]).to_string();
        let logs = parse_logs_with_compactor(
            &[
                format!("Program {log_key_high} invoke [1]"),
                format!("Program {log_key_low} invoke [1]"),
            ],
            &RawPubkeyCompactor,
        );
        let token =
            |account_index, mint, owner, program_id| blockzilla_format::CompactTokenBalance {
                account_index,
                mint: Some(CompactPubkey::Raw([mint; 32])),
                owner: Some(CompactPubkey::Raw([owner; 32])),
                program_id: Some(CompactPubkey::Raw([program_id; 32])),
                amount: 1,
                decimals: 0,
            };
        let metadata = blockzilla_format::CompactMetaV1 {
            err: None,
            fee: 0,
            pre_balances: vec![0; 4],
            post_balances: vec![0; 4],
            inner_instructions: None,
            logs: Some(logs),
            pre_token_balances: vec![token(0, 3, 4, 5)],
            post_token_balances: vec![token(0, 6, 7, 8)],
            rewards: vec![blockzilla_format::CompactReward {
                pubkey: CompactPubkey::Raw([11; 32]),
                lamports: 1,
                post_balance: 1,
                reward_type: 0,
                commission: None,
            }],
            loaded_writable_addresses: vec![CompactPubkey::Raw([14; 32])],
            loaded_readonly_addresses: Vec::new(),
            return_data: Some(blockzilla_format::CompactReturnData {
                program_id: CompactPubkey::Raw([2; 32]),
                data: Vec::new(),
            }),
            compute_units_consumed: None,
            cost_units: None,
        };
        let payload = ArchiveV2HotMessagePayload::V0(blockzilla_format::ArchiveV2HotV0Message {
            header: blockzilla_format::CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![
                CompactPubkey::Raw(source_key),
                CompactPubkey::Id(1),
                CompactPubkey::Raw([3; 32]),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(1),
            instructions: Vec::new(),
            address_table_lookups: vec![blockzilla_format::OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Raw([2; 32]),
                writable_indexes: vec![0],
                readonly_indexes: Vec::new(),
            }],
        });
        let tx = blockzilla_format::ArchiveV2HotTxRow {
            tx_index: 0,
            flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_RETURN_DATA
                | ARCHIVE_V2_TX_FLAG_HAS_LOGS
                | ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
            signature_count: 1,
            ..blockzilla_format::ArchiveV2HotTxRow::default()
        };
        let reconstructed = ReconstructedSourceTransaction {
            payload,
            metadata: Some(metadata),
            exact_instruction_data: Vec::new(),
            message_decode_time: Duration::ZERO,
            signed_message_proof_time: Duration::ZERO,
            metadata_decode_time: Duration::ZERO,
        };
        let mut raw_visits = Vec::new();
        let (projected, _) =
            validate_and_project_source_transaction(50, &tx, reconstructed, 1, &mut raw_visits)
                .unwrap();
        assert_eq!(projected.inline_log_pubkeys, vec![[11; 32], [12; 32]]);
        assert_eq!(projected.shape.static_account_count, 3);
        assert_eq!(projected.shape.loaded_address_counts.writable, 1);
        assert_eq!(projected.shape.loaded_address_counts.readonly, 0);
        assert_eq!(projected.shape.resolved_account_count, 4);

        let row = blockzilla_format::ArchiveV2HotBlockIndexRow {
            block_id: 2,
            slot: 50,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count: 1,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 1,
        };
        let header = blockzilla_format::ArchiveV2HotBlockHeader {
            slot: 50,
            parent_slot: 49,
            blockhash_id: 102,
            previous_blockhash_id: 101,
            block_time: None,
            block_height: None,
            rewards: Some(blockzilla_format::ArchiveV2HotRewards {
                num_partitions: None,
                decoded: vec![blockzilla_format::CompactReward {
                    pubkey: CompactPubkey::Raw([15; 32]),
                    lamports: 1,
                    post_balance: 1,
                    reward_type: 0,
                    commission: None,
                }],
            }),
        };
        validate_source_block_tail(row, &header, 100, 1, &mut raw_visits).unwrap();
        assert_eq!(
            raw_visits,
            [2_u8, 1, 3, 14, 2, 3, 4, 5, 6, 7, 8, 11, 12, 11, 15]
                .into_iter()
                .map(|byte| [byte; 32])
                .collect::<Vec<_>>()
        );

        let mut dictionary = TargetPubkeyDictionary::open(&source, root.path(), 1).unwrap();
        let mut resolved = ResolvedPubkeyTable::new(1);
        for bytes in raw_visits {
            resolve_and_record_pubkey(&mut dictionary, &mut resolved, &CompactPubkey::Raw(bytes))
                .unwrap();
        }
        let expected_appended = [2_u8, 3, 14, 4, 5, 6, 7, 8, 11, 12, 15]
            .into_iter()
            .flat_map(|byte| [byte; 32])
            .collect::<Vec<_>>();
        assert_eq!(dictionary.appended_bytes(), expected_appended);
    }

    fn raw_log_pubkey(byte: u8) -> CompactPubkey {
        CompactPubkey::Raw([byte; 32])
    }

    fn log_stream(events: Vec<LogEvent>) -> CompactLogStream {
        CompactLogStream {
            events,
            strings: StringTable::default(),
            data: DataTable::default(),
        }
    }

    fn typed_log_pubkey_visits(stream: &CompactLogStream) -> Vec<CompactPubkey> {
        let mut pubkeys = Vec::new();
        let result: std::result::Result<(), std::convert::Infallible> =
            stream.try_for_each_pubkey(|pubkey| {
                pubkeys.push(pubkey);
                Ok(())
            });
        result.unwrap();
        pubkeys
    }

    fn exhaustive_log_event_variant_name(event: &LogEvent) -> &'static str {
        match event {
            LogEvent::System(_) => "System",
            LogEvent::LogTruncated => "LogTruncated",
            LogEvent::StakeMergingAccounts => "StakeMergingAccounts",
            LogEvent::LoaderUpgradedProgram { .. } => "LoaderUpgradedProgram",
            LogEvent::LoaderFinalizedAccount { .. } => "LoaderFinalizedAccount",
            LogEvent::ProgramLog(_) => "ProgramLog",
            LogEvent::ProgramLogError { .. } => "ProgramLogError",
            LogEvent::ProgramIdLog { .. } => "ProgramIdLog",
            LogEvent::ProgramPlainLog(_) => "ProgramPlainLog",
            LogEvent::ProgramAccountNotWritable => "ProgramAccountNotWritable",
            LogEvent::ProgramIdMismatch => "ProgramIdMismatch",
            LogEvent::ProgramNotUpgradeable => "ProgramNotUpgradeable",
            LogEvent::ProgramAndProgramDataAccountMismatch => {
                "ProgramAndProgramDataAccountMismatch"
            }
            LogEvent::ProgramWasExtendedInThisBlockAlready => {
                "ProgramWasExtendedInThisBlockAlready"
            }
            LogEvent::Invoke { .. } => "Invoke",
            LogEvent::BpfInvoke { .. } => "BpfInvoke",
            LogEvent::Consumed { .. } => "Consumed",
            LogEvent::BpfConsumed { .. } => "BpfConsumed",
            LogEvent::Success { .. } => "Success",
            LogEvent::BpfSuccess { .. } => "BpfSuccess",
            LogEvent::Failure { .. } => "Failure",
            LogEvent::BpfFailure { .. } => "BpfFailure",
            LogEvent::FailureCustomProgramError { .. } => "FailureCustomProgramError",
            LogEvent::BpfFailureCustomProgramError { .. } => "BpfFailureCustomProgramError",
            LogEvent::FailureInvalidAccountData { .. } => "FailureInvalidAccountData",
            LogEvent::BpfFailureInvalidAccountData { .. } => "BpfFailureInvalidAccountData",
            LogEvent::FailureInvalidProgramArgument { .. } => "FailureInvalidProgramArgument",
            LogEvent::BpfFailureInvalidProgramArgument { .. } => "BpfFailureInvalidProgramArgument",
            LogEvent::FailedToComplete { .. } => "FailedToComplete",
            LogEvent::CustomProgramError { .. } => "CustomProgramError",
            LogEvent::Return { .. } => "Return",
            LogEvent::Data { .. } => "Data",
            LogEvent::Consumption { .. } => "Consumption",
            LogEvent::CbRequestUnits { .. } => "CbRequestUnits",
            LogEvent::ProgramNotDeployed { .. } => "ProgramNotDeployed",
            LogEvent::ProgramNotCached { .. } => "ProgramNotCached",
            LogEvent::UnknownProgram { .. } => "UnknownProgram",
            LogEvent::UnknownAccount { .. } => "UnknownAccount",
            LogEvent::VerifyEd25519 => "VerifyEd25519",
            LogEvent::VerifySecp256k1 => "VerifySecp256k1",
            LogEvent::RuntimeWritablePrivilegeEscalated { .. } => {
                "RuntimeWritablePrivilegeEscalated"
            }
            LogEvent::RuntimeSignerPrivilegeEscalated { .. } => "RuntimeSignerPrivilegeEscalated",
            LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { .. } => {
                "RuntimeAccountOwnerBalanceVerificationFailed"
            }
            LogEvent::CloseContextState => "CloseContextState",
            LogEvent::Plain { .. } => "Plain",
            LogEvent::Unparsed { .. } => "Unparsed",
        }
    }

    #[test]
    fn b5_typed_visitor_matches_json_oracle_for_every_log_event_variant() {
        let fixtures = vec![
            (
                "System",
                LogEvent::System(SystemProgramLog::Instruction(
                    SystemInstructionLog::RevokePendingActivation,
                )),
            ),
            ("LogTruncated", LogEvent::LogTruncated),
            ("StakeMergingAccounts", LogEvent::StakeMergingAccounts),
            (
                "LoaderUpgradedProgram",
                LogEvent::LoaderUpgradedProgram {
                    program: raw_log_pubkey(1),
                },
            ),
            (
                "LoaderFinalizedAccount",
                LogEvent::LoaderFinalizedAccount {
                    account: raw_log_pubkey(2),
                },
            ),
            ("ProgramLog", LogEvent::ProgramLog(ProgramLog::Empty)),
            ("ProgramLogError", LogEvent::ProgramLogError { msg: 0 }),
            (
                "ProgramIdLog",
                LogEvent::ProgramIdLog {
                    program: raw_log_pubkey(4),
                    log: ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
                        account_key: raw_log_pubkey(3),
                        error: 0,
                    }),
                },
            ),
            (
                "ProgramPlainLog",
                LogEvent::ProgramPlainLog(ProgramLog::Empty),
            ),
            (
                "ProgramAccountNotWritable",
                LogEvent::ProgramAccountNotWritable,
            ),
            ("ProgramIdMismatch", LogEvent::ProgramIdMismatch),
            ("ProgramNotUpgradeable", LogEvent::ProgramNotUpgradeable),
            (
                "ProgramAndProgramDataAccountMismatch",
                LogEvent::ProgramAndProgramDataAccountMismatch,
            ),
            (
                "ProgramWasExtendedInThisBlockAlready",
                LogEvent::ProgramWasExtendedInThisBlockAlready,
            ),
            (
                "Invoke",
                LogEvent::Invoke {
                    program: raw_log_pubkey(5),
                    depth: 1,
                },
            ),
            (
                "BpfInvoke",
                LogEvent::BpfInvoke {
                    program: raw_log_pubkey(6),
                },
            ),
            (
                "Consumed",
                LogEvent::Consumed {
                    program: raw_log_pubkey(7),
                    used: 1,
                    limit: 2,
                },
            ),
            ("BpfConsumed", LogEvent::BpfConsumed { used: 1, limit: 2 }),
            (
                "Success",
                LogEvent::Success {
                    program: raw_log_pubkey(8),
                },
            ),
            (
                "BpfSuccess",
                LogEvent::BpfSuccess {
                    program: raw_log_pubkey(9),
                },
            ),
            (
                "Failure",
                LogEvent::Failure {
                    program: raw_log_pubkey(10),
                    reason: 0,
                },
            ),
            (
                "BpfFailure",
                LogEvent::BpfFailure {
                    program: raw_log_pubkey(11),
                    reason: 0,
                },
            ),
            (
                "FailureCustomProgramError",
                LogEvent::FailureCustomProgramError {
                    program: raw_log_pubkey(12),
                    code: 1,
                },
            ),
            (
                "BpfFailureCustomProgramError",
                LogEvent::BpfFailureCustomProgramError {
                    program: raw_log_pubkey(13),
                    code: 1,
                },
            ),
            (
                "FailureInvalidAccountData",
                LogEvent::FailureInvalidAccountData {
                    program: raw_log_pubkey(14),
                },
            ),
            (
                "BpfFailureInvalidAccountData",
                LogEvent::BpfFailureInvalidAccountData {
                    program: raw_log_pubkey(15),
                },
            ),
            (
                "FailureInvalidProgramArgument",
                LogEvent::FailureInvalidProgramArgument {
                    program: raw_log_pubkey(16),
                },
            ),
            (
                "BpfFailureInvalidProgramArgument",
                LogEvent::BpfFailureInvalidProgramArgument {
                    program: raw_log_pubkey(17),
                },
            ),
            ("FailedToComplete", LogEvent::FailedToComplete { reason: 0 }),
            (
                "CustomProgramError",
                LogEvent::CustomProgramError { code: 1 },
            ),
            (
                "Return",
                LogEvent::Return {
                    program: raw_log_pubkey(18),
                    data: 0,
                },
            ),
            ("Data", LogEvent::Data { data: 0 }),
            ("Consumption", LogEvent::Consumption { units: 1 }),
            ("CbRequestUnits", LogEvent::CbRequestUnits { units: 1 }),
            (
                "ProgramNotDeployed",
                LogEvent::ProgramNotDeployed {
                    program: Some(raw_log_pubkey(19)),
                },
            ),
            (
                "ProgramNotCached",
                LogEvent::ProgramNotCached { program: None },
            ),
            ("UnknownProgram", LogEvent::UnknownProgram { program: 0 }),
            ("UnknownAccount", LogEvent::UnknownAccount { account: 0 }),
            ("VerifyEd25519", LogEvent::VerifyEd25519),
            ("VerifySecp256k1", LogEvent::VerifySecp256k1),
            (
                "RuntimeWritablePrivilegeEscalated",
                LogEvent::RuntimeWritablePrivilegeEscalated {
                    account: raw_log_pubkey(20),
                },
            ),
            (
                "RuntimeSignerPrivilegeEscalated",
                LogEvent::RuntimeSignerPrivilegeEscalated {
                    account: raw_log_pubkey(21),
                },
            ),
            (
                "RuntimeAccountOwnerBalanceVerificationFailed",
                LogEvent::RuntimeAccountOwnerBalanceVerificationFailed {
                    account: raw_log_pubkey(22),
                },
            ),
            ("CloseContextState", LogEvent::CloseContextState),
            ("Plain", LogEvent::Plain { text: 0 }),
            ("Unparsed", LogEvent::Unparsed { text: 0 }),
        ];

        assert_eq!(fixtures.len(), 46);
        for (expected_name, event) in fixtures {
            assert_eq!(exhaustive_log_event_variant_name(&event), expected_name);
            let stream = log_stream(vec![event]);
            assert_eq!(
                typed_log_pubkey_visits(&stream),
                log_pubkey_visits_json_oracle(&stream).unwrap(),
                "{expected_name} visitor order differs from the JSON oracle"
            );
            assert_eq!(
                inspect_log_pubkeys(&stream, Some(100)).unwrap(),
                inspect_log_pubkeys_json_oracle(&stream, Some(100)).unwrap(),
                "{expected_name} extracted keys differ from the JSON oracle"
            );
        }
    }

    #[test]
    fn b5_typed_visitor_matches_nested_system_and_token_2022_pubkeys() {
        let events = vec![
            LogEvent::System(SystemProgramLog::CreateAddressMismatch {
                provided_addr: CompactPubkey::Id(2),
                derived_addr: PubkeyOrString::Pubkey(CompactPubkey::Id(1)),
            }),
            LogEvent::System(SystemProgramLog::TransferFromAddressMismatch {
                provided_addr: raw_log_pubkey(4),
                derived_addr: PubkeyOrString::Pubkey(raw_log_pubkey(3)),
            }),
            LogEvent::System(SystemProgramLog::CreateAccountAlreadyInUse {
                addr: SystemAddress::Pubkey(PubkeyOrString::Pubkey(raw_log_pubkey(5))),
            }),
            LogEvent::System(SystemProgramLog::AllocateAlreadyInUse {
                addr: SystemAddress::Pubkey(PubkeyOrString::Text(0)),
            }),
            LogEvent::System(SystemProgramLog::AllocateToMustSign {
                addr: SystemAddress::Debug {
                    address: PubkeyOrString::Pubkey(raw_log_pubkey(6)),
                    base: Some(PubkeyOrString::Pubkey(CompactPubkey::Id(7))),
                },
            }),
            LogEvent::System(SystemProgramLog::AllocateAccountAlreadyInUse {
                addr: SystemAddress::Debug {
                    address: PubkeyOrString::Text(0),
                    base: None,
                },
            }),
            LogEvent::System(SystemProgramLog::AssignAccountMustSign {
                addr: SystemAddress::Pubkey(PubkeyOrString::Pubkey(raw_log_pubkey(13))),
            }),
            LogEvent::System(SystemProgramLog::CreateAccountAccountAlreadyInUse {
                addr: SystemAddress::Debug {
                    address: PubkeyOrString::Pubkey(CompactPubkey::Id(14)),
                    base: Some(PubkeyOrString::Text(0)),
                },
            }),
            LogEvent::System(SystemProgramLog::TransferFromMustSign {
                from: raw_log_pubkey(15),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountMustBeWriteable {
                action: NonceAction::Advance,
                account: PubkeyOrString::Pubkey(CompactPubkey::Id(16)),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountMustBeSigner {
                action: NonceAction::Withdraw,
                account: PubkeyOrString::Pubkey(raw_log_pubkey(17)),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountMustSign {
                action: NonceAction::Initialize,
                account: PubkeyOrString::Text(0),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountStateInvalid {
                action: NonceAction::Authorize,
                account: PubkeyOrString::Pubkey(raw_log_pubkey(18)),
            }),
            LogEvent::ProgramLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
                account_key: raw_log_pubkey(8),
                error: 0,
            })),
            LogEvent::ProgramLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom2 {
                account_key: CompactPubkey::Id(9),
                error: 0,
            })),
            LogEvent::ProgramLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom3 {
                account_key: raw_log_pubkey(10),
                error: 0,
            })),
            LogEvent::ProgramIdLog {
                program: CompactPubkey::Id(12),
                log: ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom4 {
                    account_key: CompactPubkey::Id(11),
                    error: 0,
                }),
            },
        ];
        let stream = log_stream(events);
        let oracle = log_pubkey_visits_json_oracle(&stream).unwrap();
        assert_eq!(typed_log_pubkey_visits(&stream), oracle);
        assert_eq!(
            oracle,
            vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                raw_log_pubkey(3),
                raw_log_pubkey(4),
                raw_log_pubkey(5),
                raw_log_pubkey(6),
                CompactPubkey::Id(7),
                raw_log_pubkey(13),
                CompactPubkey::Id(14),
                raw_log_pubkey(15),
                CompactPubkey::Id(16),
                raw_log_pubkey(17),
                raw_log_pubkey(18),
                raw_log_pubkey(8),
                CompactPubkey::Id(9),
                raw_log_pubkey(10),
                CompactPubkey::Id(11),
                CompactPubkey::Id(12),
            ]
        );
    }

    #[test]
    fn b5_typed_visitor_keeps_json_first_invalid_id_order() {
        for (case, event, expected_id) in [
            (
                "program id log",
                LogEvent::ProgramIdLog {
                    program: CompactPubkey::Id(5),
                    log: ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
                        account_key: CompactPubkey::Id(4),
                        error: 0,
                    }),
                },
                4,
            ),
            (
                "system address mismatch",
                LogEvent::System(SystemProgramLog::CreateAddressMismatch {
                    provided_addr: CompactPubkey::Id(5),
                    derived_addr: PubkeyOrString::Pubkey(CompactPubkey::Id(4)),
                }),
                4,
            ),
        ] {
            let stream = log_stream(vec![event]);
            let typed_error = inspect_log_pubkeys(&stream, Some(3)).unwrap_err();
            let oracle_error = inspect_log_pubkeys_json_oracle(&stream, Some(3)).unwrap_err();
            assert_eq!(typed_error.to_string(), oracle_error.to_string(), "{case}");
            assert!(
                typed_error
                    .to_string()
                    .contains(&format!("id {expected_id}"))
            );
        }
    }

    #[test]
    fn b5_typed_visitor_rejects_zero_and_upper_bound_ids() {
        for id in [0, 4] {
            let stream = log_stream(vec![LogEvent::Invoke {
                program: CompactPubkey::Id(id),
                depth: 1,
            }]);
            let typed_error = inspect_log_pubkeys(&stream, Some(3)).unwrap_err();
            let oracle_error = inspect_log_pubkeys_json_oracle(&stream, Some(3)).unwrap_err();
            assert_eq!(typed_error.to_string(), oracle_error.to_string());
            assert!(typed_error.to_string().contains(&format!("id {id}")));
        }
    }

    #[test]
    fn b5_raw_pubkeys_stay_byte_sorted_and_deduplicated() {
        let stream = log_stream(vec![
            LogEvent::Invoke {
                program: raw_log_pubkey(9),
                depth: 1,
            },
            LogEvent::ProgramIdLog {
                program: raw_log_pubkey(3),
                log: ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
                    account_key: raw_log_pubkey(9),
                    error: 0,
                }),
            },
            LogEvent::Return {
                program: raw_log_pubkey(1),
                data: 0,
            },
        ]);
        let expected = vec![[1; 32], [3; 32], [9; 32]];
        assert_eq!(inspect_log_pubkeys(&stream, Some(1)).unwrap(), expected);
        assert_eq!(
            inspect_log_pubkeys_json_oracle(&stream, Some(1)).unwrap(),
            expected
        );
    }
}
