//! Stream-decode one epoch's Archive V2 Compact generation and build its
//! signer user -> reached-program reverse index.
//!
//! V1 maps every required transaction signer (fee payer and co-signers) to
//! every distinct direct or recorded inner/CPI program reached by that
//! transaction. Only successful transactions are included; votes are included.
//! The relation does not depend on whether a signer is repeated in an
//! individual instruction's account list.
//!
//! Only the message header/instructions and the metadata inner-instruction
//! list are decoded; logs, token balances, rewards, and return data are
//! never touched. Accounts are indexed by their compact registry id
//! (`CompactPubkey::Id`), never resolved to a 32-byte pubkey. The hot scan
//! never resolves registry contents. Published archives are fully hash-checked
//! once before scanning; trusted-local builds make one sequential registry
//! pass to bind the index manifest to its real SHA-256.

use std::{
    fs::{self, File},
    io::{BufReader, Read, Seek, SeekFrom},
    num::NonZeroUsize,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::mpsc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use blockzilla_archive_v2::{
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_MESSAGE_V0, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotTxRow,
};
use blockzilla_compact_v2_reader::{
    ArchiveReader, CompactV2MessageSchema, CompactV2MetadataSchema, HashVerification, OpenOptions,
    PinnedLocalRangeSource, RangeSource, manifest::TrustedGenerationIdentity,
};
use blockzilla_primitives::CompactPubkey;
use blockzilla_registry::FileBackedKeyIndex;
use rustix::fs::{CWD, RenameFlags, renameat_with};
use sha2::{Digest, Sha256};
use smallvec::SmallVec;

use crate::{
    decode,
    dense_accumulator::DenseAccumulator,
    format::{
        FORMAT_VERSION, GenerationBindingKind, IndexBuilder, IndexFileBinding, IndexManifest,
        IndexSemantics, MANIFEST_SCHEMA_VERSION, OmissionCounts, ProgramTracker, RecordOutcome,
        RegistryFileIdentity, ShardBinding, ShardWriter, bind_shard, open_file, write_program_map,
    },
    signer_rank::{SignerRank, SignerSetBinding, SignerSetBuilder},
};

/// Default cap on distinct account ids held in memory by one chunk. Bounds a
/// single build's peak memory regardless of the epoch's registry size — see
/// `format.rs`'s module docs for the chunking/sharding scheme this drives.
/// Chosen to keep one chunk's fixed empty-array cost comfortably bounded on
/// modest hardware (~48 bytes/slot empty, so 8M accounts is ~384MB before
/// any real instruction data accumulates on top).
pub const DEFAULT_MAX_ACCOUNTS_PER_CHUNK: u32 = 8_000_000;
pub const MAX_SCAN_THREADS: usize = 256;
pub const DEFAULT_RELATION_BATCH_PAIRS: usize = 16_384;
pub const DEFAULT_QUEUED_RELATION_BATCHES: usize = 32;
pub const MAX_RELATION_BATCH_PAIRS: usize = 1_048_576;
pub const MAX_QUEUED_RELATION_BATCHES: usize = 4_096;
pub const MAX_RELATION_PIPELINE_BUFFER_BYTES: usize = 256 * 1024 * 1024;

/// Return the bounded host parallelism used by new dense-build commands.
pub fn default_scan_threads() -> usize {
    std::thread::available_parallelism()
        .map(NonZeroUsize::get)
        .unwrap_or(1)
        .min(MAX_SCAN_THREADS)
}

/// Tuning values for the two-pass dense builder.
///
/// These values affect parallelism and shard layout only. They do not change
/// the indexed relation or the index wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DenseIndexBuildOptions {
    pub shard_width: u32,
    pub threads: usize,
    pub batch_pairs: usize,
    pub queued_batches: usize,
}

impl Default for DenseIndexBuildOptions {
    fn default() -> Self {
        Self {
            shard_width: DEFAULT_MAX_ACCOUNTS_PER_CHUNK,
            threads: default_scan_threads(),
            batch_pairs: DEFAULT_RELATION_BATCH_PAIRS,
            queued_batches: DEFAULT_QUEUED_RELATION_BATCHES,
        }
    }
}

/// Identity to assert when opening a generation with no published
/// `archive-v2-generation.json` (see `blockzilla-read-sdk`'s `open_trusted`).
/// Only meaningful for archives on a filesystem the caller already trusts.
pub struct TrustLocal {
    pub cluster_id: String,
    pub generation_id: String,
    pub slots_per_epoch: u64,
}

fn archive_hash_verification(trusted_local: bool) -> HashVerification {
    if trusted_local {
        HashVerification::SizesOnly
    } else {
        HashVerification::AllFiles
    }
}

/// Open an epoch's archive for the `build`/microbench hot path. Published
/// generations receive full content-hash verification before indexing;
/// explicitly trusted-local generations use size/structure checks because
/// they have no published hashes. The transaction scan itself keeps accounts
/// as registry ids and never resolves registry bytes.
pub fn open_archive(
    archive_root: &Path,
    epoch: u64,
    trust_local: Option<TrustLocal>,
) -> Result<ArchiveReader<PinnedLocalRangeSource>> {
    let source = PinnedLocalRangeSource::new(archive_root);
    let archive = if let Some(trust_local) = trust_local {
        let options = OpenOptions {
            hash_verification: archive_hash_verification(true),
            ..OpenOptions::default()
        };
        let identity = TrustedGenerationIdentity {
            cluster_id: trust_local.cluster_id,
            epoch,
            generation_id: trust_local.generation_id,
            slots_per_epoch: trust_local.slots_per_epoch,
        };
        ArchiveReader::open_trusted(source, identity, options).with_context(|| {
            format!(
                "open Archive V2 generation at {} (trusted local, no manifest)",
                archive_root.display()
            )
        })?
    } else {
        let options = OpenOptions {
            // A published manifest is only a content binding after every
            // indexed input is hashed. Size-only validation could let a
            // same-length blocks replacement produce answers under the old
            // generation digest.
            hash_verification: archive_hash_verification(false),
            ..OpenOptions::default()
        };
        ArchiveReader::open_with_options(source, options)
            .with_context(|| format!("open Archive V2 generation at {}", archive_root.display()))?
    };

    let manifest_epoch = archive.manifest().epoch;
    if manifest_epoch != epoch {
        anyhow::bail!(
            "archive at {} is epoch {manifest_epoch}, not requested epoch {epoch}",
            archive_root.display()
        );
    }
    Ok(archive)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ScanStats {
    pub blocks_scanned: u64,
    pub transactions_scanned: u64,
    pub failed_transactions_excluded: u64,
}

/// Stream a block-row range of an already-open archive into `builder`. This
/// is the hot path: no disk writes, no registry resolution, just decode +
/// index. Shared by `scan_into_builder` (`range` = the whole archive) and
/// `scan_into_builder_parallel` (`range` = one thread's disjoint slice) —
/// see `discover_signers`/`scan_signers_range` for why concurrent calls
/// across disjoint ranges on a shared `&ArchiveReader` are safe.
fn scan_range_into_builder<S: RangeSource>(
    archive: &ArchiveReader<S>,
    range: std::ops::Range<usize>,
    builder: &mut IndexBuilder,
    log_progress: bool,
) -> Result<ScanStats> {
    let mut stats = ScanStats::default();
    let total_blocks = range.len();
    let mut blocks = archive
        .borrowed_blocks_without_rewards_range(range.clone())
        .with_context(|| format!("open block range {range:?}"))?;
    while let Some(block) = blocks.next_block() {
        let block = block.with_context(|| format!("decode block in range {range:?}"))?;
        let slot = block.header().slot;
        stats.blocks_scanned += 1;
        if log_progress && stats.blocks_scanned.is_multiple_of(50_000) {
            tracing::info!(
                blocks_scanned = stats.blocks_scanned,
                total_blocks,
                transactions_scanned = stats.transactions_scanned,
                wallets = builder.wallet_count(),
                "build progress"
            );
        }

        for row in block.tx_rows() {
            stats.transactions_scanned += 1;
            if !transaction_is_in_semantic_scope(&row)
                .with_context(|| format!("classify slot {slot} tx {}", row.tx_index))?
            {
                stats.failed_transactions_excluded += 1;
                continue;
            }
            index_transaction_with_schemas(
                builder,
                archive.registry_entries(),
                archive.message_schema(),
                archive.metadata_schema(),
                slot,
                &row,
                block.message_bytes(),
                block.metadata_bytes(),
            )?;
        }
    }
    Ok(stats)
}

fn transaction_is_in_semantic_scope(row: &ArchiveV2HotTxRow) -> Result<bool> {
    if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        anyhow::bail!("raw transaction fallback")
    }
    // Failed transactions are outside successful-only V1. Their metadata is
    // not required, so a raw failed metadata payload is not an omission.
    if row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0 {
        return Ok(false);
    }
    if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        anyhow::bail!("raw metadata fallback for successful transaction")
    }
    Ok(true)
}

/// Stream every block of an already-open archive into `builder`, single-
/// threaded. This is the hot path: no disk writes, no registry resolution,
/// just decode + index.
pub fn scan_into_builder<S: RangeSource>(
    archive: &ArchiveReader<S>,
    builder: &mut IndexBuilder,
    log_progress: bool,
) -> Result<ScanStats> {
    let total_rows = archive.index().rows.len();
    scan_range_into_builder(archive, 0..total_rows, builder, log_progress)
}

/// Same as `scan_into_builder`, but splits the archive's block-row index
/// into `thread_count` contiguous ranges and scans them concurrently, each
/// thread decoding into its own `IndexBuilder` covering the *same* chunk
/// (`chunk_start`/`chunk_width`/`max_program_id`), merged (see
/// `IndexBuilder::merge`) after every thread joins.
///
/// **Memory tradeoff, not a free win**: each thread's builder is a full
/// `chunk_width`-sized array, so peak memory during the scan is roughly
/// `thread_count` times what `scan_into_builder` would use for the same
/// chunk — this trades memory for wall-clock time, it doesn't shrink either
/// one for free. If your chunk width was already chosen to safely fill
/// available memory at 1 thread, either shrink `chunk_width` (roughly by
/// `thread_count`, which also proportionally increases `shard_count` — see
/// the wall-clock math in this crate's development notes, since that
/// combination alone doesn't reduce total wall-clock, only peak-memory) or
/// budget for the multiplied peak explicitly before raising `thread_count`.
///
/// Only pays off at all if the underlying storage has spare concurrent-I/O
/// throughput beyond what one sequential reader already saturates — see
/// `discover_signers_parallel`'s docs, and bench with `discover-signers
/// --threads N` (much cheaper to iterate on) before assuming a given
/// `thread_count` helps on your target storage.
pub fn scan_into_builder_parallel<S: RangeSource>(
    archive: &ArchiveReader<S>,
    chunk_start: u32,
    chunk_width: u32,
    max_program_id: u32,
    thread_count: usize,
    log_progress: bool,
) -> Result<(IndexBuilder, ScanStats)> {
    let total_rows = archive.index().rows.len();
    anyhow::ensure!(
        thread_count > 0 && thread_count <= MAX_SCAN_THREADS,
        "thread_count must be in 1..={MAX_SCAN_THREADS}"
    );
    let thread_count = thread_count.min(total_rows.max(1));

    if thread_count == 1 || total_rows <= 1 {
        let mut builder = IndexBuilder::new(chunk_start, chunk_width, max_program_id);
        let stats = scan_range_into_builder(archive, 0..total_rows, &mut builder, log_progress)?;
        return Ok((builder, stats));
    }

    let rows_per_thread = total_rows.div_ceil(thread_count);
    let ranges: Vec<std::ops::Range<usize>> = (0..thread_count)
        .map(|i| (i * rows_per_thread).min(total_rows)..((i + 1) * rows_per_thread).min(total_rows))
        .filter(|range| !range.is_empty())
        .collect();

    let per_thread_results: Vec<Result<(IndexBuilder, ScanStats)>> = std::thread::scope(|scope| {
        let handles: Vec<_> = ranges
            .into_iter()
            .map(|range| {
                scope.spawn(move || -> Result<(IndexBuilder, ScanStats)> {
                    let mut builder = IndexBuilder::new(chunk_start, chunk_width, max_program_id);
                    // Only the first worker logs progress — with N threads
                    // each reporting their own counters, interleaved logs
                    // from every worker would just be noise.
                    let stats = scan_range_into_builder(
                        archive,
                        range.clone(),
                        &mut builder,
                        log_progress && range.start == 0,
                    )?;
                    Ok((builder, stats))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| match handle.join() {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("build worker thread panicked")),
            })
            .collect()
    });

    let mut merged_builder: Option<IndexBuilder> = None;
    let mut stats = ScanStats::default();
    for result in per_thread_results {
        let (builder, thread_stats) = result?;
        match &mut merged_builder {
            Some(accumulator) => accumulator.merge(builder),
            None => merged_builder = Some(builder),
        }
        stats.blocks_scanned += thread_stats.blocks_scanned;
        stats.transactions_scanned += thread_stats.transactions_scanned;
        stats.failed_transactions_excluded += thread_stats.failed_transactions_excluded;
    }

    Ok((
        merged_builder
            .unwrap_or_else(|| IndexBuilder::new(chunk_start, chunk_width, max_program_id)),
        stats,
    ))
}

fn scan_range_relations<S: RangeSource>(
    archive: &ArchiveReader<S>,
    range: std::ops::Range<usize>,
    log_progress: bool,
    mut on_relation: impl FnMut(u32, u32) -> Result<()>,
) -> Result<ScanStats> {
    let mut stats = ScanStats::default();
    let total_blocks = range.len();
    let mut blocks = archive
        .borrowed_blocks_without_rewards_range(range.clone())
        .with_context(|| format!("open block range {range:?}"))?;
    while let Some(block) = blocks.next_block() {
        let block = block.with_context(|| format!("decode block in range {range:?}"))?;
        let slot = block.header().slot;
        stats.blocks_scanned += 1;
        if log_progress && stats.blocks_scanned.is_multiple_of(50_000) {
            tracing::info!(
                blocks_scanned = stats.blocks_scanned,
                total_blocks,
                transactions_scanned = stats.transactions_scanned,
                "dense build decode progress"
            );
        }

        for row in block.tx_rows() {
            stats.transactions_scanned += 1;
            if !transaction_is_in_semantic_scope(&row)
                .with_context(|| format!("classify slot {slot} tx {}", row.tx_index))?
            {
                stats.failed_transactions_excluded += 1;
                continue;
            }
            visit_transaction_relations(
                archive.registry_entries(),
                archive.message_schema(),
                archive.metadata_schema(),
                slot,
                &row,
                block.message_bytes(),
                block.metadata_bytes(),
                &mut on_relation,
            )?;
        }
    }
    Ok(stats)
}

struct RelationBatch {
    worker: usize,
    pairs: Vec<(u32, u32)>,
}

/// Run Stage 3 pass 2. Decoder workers own disjoint block ranges and send
/// bounded dense-rank relation batches to this thread, which is the sole
/// accumulator owner. Empty batch buffers are returned to their originating
/// worker, keeping steady-state allocations bounded by queue depth + workers.
#[allow(clippy::too_many_arguments)]
pub fn scan_into_dense_accumulator<S: RangeSource>(
    archive: &ArchiveReader<S>,
    signer_rank: &SignerRank,
    expected_signer_binding: SignerSetBinding,
    accumulator: &mut DenseAccumulator,
    thread_count: usize,
    batch_pairs: usize,
    queued_batches: usize,
    log_progress: bool,
) -> Result<ScanStats> {
    anyhow::ensure!(
        thread_count > 0 && thread_count <= MAX_SCAN_THREADS,
        "thread_count must be in 1..={MAX_SCAN_THREADS}"
    );
    anyhow::ensure!(batch_pairs > 0, "batch_pairs must be greater than zero");
    anyhow::ensure!(
        batch_pairs <= MAX_RELATION_BATCH_PAIRS,
        "batch_pairs must not exceed {MAX_RELATION_BATCH_PAIRS}"
    );
    anyhow::ensure!(
        batch_pairs <= accumulator.max_batch_pairs().get(),
        "batch_pairs {batch_pairs} exceeds accumulator maximum {}",
        accumulator.max_batch_pairs()
    );
    anyhow::ensure!(
        queued_batches > 0 && queued_batches <= MAX_QUEUED_RELATION_BATCHES,
        "queued_batches must be in 1..={MAX_QUEUED_RELATION_BATCHES}"
    );
    validate_relation_pipeline_buffer(thread_count, batch_pairs, queued_batches)?;
    anyhow::ensure!(
        signer_rank.binding() == expected_signer_binding,
        "signer rank generation/registry binding does not match the retained build inputs"
    );
    anyhow::ensure!(
        signer_rank.signer_count() == accumulator.signer_count(),
        "signer rank contains {} signers but accumulator has {} slots",
        signer_rank.signer_count(),
        accumulator.signer_count()
    );
    anyhow::ensure!(
        signer_rank.registry_entries() == archive.registry_entries(),
        "signer rank registry size does not match archive"
    );
    anyhow::ensure!(
        accumulator.max_program_id() == archive.registry_entries(),
        "accumulator program-id bound does not match archive registry"
    );

    let total_rows = archive.index().rows.len();
    let thread_count = thread_count.min(total_rows.max(1));
    if thread_count == 1 || total_rows <= 1 {
        let mut last_signer_rank = None::<(u32, u32)>;
        return scan_range_relations(archive, 0..total_rows, log_progress, |signer, program| {
            let dense_rank = match last_signer_rank {
                Some((last_signer, rank)) if last_signer == signer => rank,
                _ => {
                    let rank = signer_rank.rank(signer).with_context(|| {
                        format!("signer registry id {signer} is absent from pass-1 signer set")
                    })?;
                    last_signer_rank = Some((signer, rank));
                    rank
                }
            };
            accumulator.record(dense_rank, program)?;
            Ok(())
        });
    }

    let rows_per_thread = total_rows.div_ceil(thread_count);
    let ranges: Vec<_> = (0..thread_count)
        .map(|worker| {
            (
                worker,
                (worker * rows_per_thread).min(total_rows)
                    ..((worker + 1) * rows_per_thread).min(total_rows),
            )
        })
        .filter(|(_, range)| !range.is_empty())
        .collect();
    let (full_sender, full_receiver) = mpsc::sync_channel::<RelationBatch>(queued_batches);
    let mut recycle_senders = Vec::with_capacity(ranges.len());
    let mut recycle_receivers = Vec::with_capacity(ranges.len());
    for _ in 0..ranges.len() {
        let (sender, receiver) = mpsc::channel::<Vec<(u32, u32)>>();
        recycle_senders.push(sender);
        recycle_receivers.push(Some(receiver));
    }

    let worker_results = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(ranges.len());
        for (worker, range) in ranges {
            let sender = full_sender.clone();
            let recycle = recycle_receivers[worker]
                .take()
                .expect("one recycle receiver per worker");
            handles.push(scope.spawn(move || -> Result<ScanStats> {
                let mut batch = Vec::with_capacity(batch_pairs);
                let mut last_signer_rank = None::<(u32, u32)>;
                let stats = scan_range_relations(
                    archive,
                    range,
                    log_progress && worker == 0,
                    |signer, program| {
                        let dense_rank = match last_signer_rank {
                            Some((last_signer, rank)) if last_signer == signer => rank,
                            _ => {
                                let rank = signer_rank.rank(signer).with_context(|| {
                                    format!(
                                        "signer registry id {signer} is absent from pass-1 signer set"
                                    )
                                })?;
                                last_signer_rank = Some((signer, rank));
                                rank
                            }
                        };
                        batch.push((dense_rank, program));
                        if batch.len() == batch_pairs {
                            sender
                                .send(RelationBatch {
                                    worker,
                                    pairs: std::mem::take(&mut batch),
                                })
                                .map_err(|_| anyhow::anyhow!("dense accumulator stopped"))?;
                            batch = recycle
                                .try_recv()
                                .unwrap_or_else(|_| Vec::with_capacity(batch_pairs));
                            debug_assert!(batch.is_empty());
                        }
                        Ok(())
                    },
                )?;
                if !batch.is_empty() {
                    sender
                        .send(RelationBatch {
                            worker,
                            pairs: batch,
                        })
                        .map_err(|_| anyhow::anyhow!("dense accumulator stopped"))?;
                }
                Ok(stats)
            }));
        }
        drop(full_sender);

        let mut accumulator_error = None;
        while let Ok(mut batch) = full_receiver.recv() {
            if accumulator_error.is_none()
                && let Err(error) = accumulator.record_rank_batch(&batch.pairs)
            {
                accumulator_error = Some(anyhow::Error::from(error));
            }
            batch.pairs.clear();
            let _ = recycle_senders[batch.worker].send(batch.pairs);
        }

        let results = handles
            .into_iter()
            .map(|handle| match handle.join() {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("dense build worker thread panicked")),
            })
            .collect::<Vec<_>>();
        (results, accumulator_error)
    });

    if let Some(error) = worker_results.1 {
        return Err(error).context("record dense relation batch");
    }
    let mut aggregate = ScanStats::default();
    for result in worker_results.0 {
        stats_add(&mut aggregate, result?);
    }
    Ok(aggregate)
}

/// Streams the archive once per chunk of the account id space (see
/// `format.rs`'s module docs), writing one shard per chunk under `out_dir`.
/// Peak memory is bounded by `max_accounts_per_chunk` regardless of how
/// large the epoch's registry is; wall time scales roughly linearly with
/// the number of chunks, since each is a full re-decode of the archive.
///
/// `threads`: 1 scans each shard sequentially (`scan_into_builder`); more
/// than 1 scans each shard's full archive pass with that many concurrent
/// readers (`scan_into_builder_parallel`) before writing it. This multiplies
/// *that shard's* peak memory by roughly `threads` — see
/// `scan_into_builder_parallel`'s docs before raising it on a memory-
/// constrained host; `max_accounts_per_chunk` may need to shrink in step.
pub fn build_index(
    archive_root: &Path,
    epoch: u64,
    out_dir: &Path,
    trust_local: Option<TrustLocal>,
    max_accounts_per_chunk: u32,
    threads: usize,
) -> Result<()> {
    anyhow::ensure!(
        !out_dir.exists(),
        "refusing to overwrite existing index output {}; choose a new immutable output path",
        out_dir.display()
    );
    let staging = create_staging_dir(out_dir)?;
    let binding_kind = if trust_local.is_some() {
        GenerationBindingKind::TrustedLocalAssertedImmutable
    } else {
        GenerationBindingKind::PublishedManifest
    };
    let result = build_index_to_staging(
        archive_root,
        epoch,
        &staging,
        trust_local,
        binding_kind,
        max_accounts_per_chunk,
        threads,
    )
    .and_then(|()| sync_tree(&staging))
    .and_then(|()| {
        publish_staging_directory(&staging, out_dir)?;
        sync_directory(output_parent(out_dir))
    });
    if result.is_err()
        && staging.exists()
        && let Err(cleanup_error) = fs::remove_dir_all(&staging)
    {
        tracing::warn!(
            path = %staging.display(),
            %cleanup_error,
            "failed to remove owned index staging directory"
        );
    }
    result
}

/// Build the same version-3 index with the Stage 3 two-pass algorithm:
/// discover/rank only real signers, then decode the archive once into one
/// compact dense accumulator. `signer_set` may reuse a pass-1 artifact for a
/// published generation; when omitted, both passes run against the same set
/// of retained archive handles in this process.
#[allow(clippy::too_many_arguments)]
pub fn build_dense_index(
    archive_root: &Path,
    epoch: u64,
    out_dir: &Path,
    trust_local: Option<TrustLocal>,
    signer_set: Option<&Path>,
    shard_width: u32,
    threads: usize,
    batch_pairs: usize,
    queued_batches: usize,
) -> Result<()> {
    anyhow::ensure!(shard_width > 0, "shard_width must be greater than zero");
    anyhow::ensure!(
        threads > 0 && threads <= MAX_SCAN_THREADS,
        "threads must be in 1..={MAX_SCAN_THREADS}"
    );
    anyhow::ensure!(
        batch_pairs > 0 && batch_pairs <= MAX_RELATION_BATCH_PAIRS,
        "batch_pairs must be in 1..={MAX_RELATION_BATCH_PAIRS}"
    );
    anyhow::ensure!(
        queued_batches > 0 && queued_batches <= MAX_QUEUED_RELATION_BATCHES,
        "queued_batches must be in 1..={MAX_QUEUED_RELATION_BATCHES}"
    );
    validate_relation_pipeline_buffer(threads, batch_pairs, queued_batches)?;
    anyhow::ensure!(
        !out_dir.exists(),
        "refusing to overwrite existing index output {}; choose a new immutable output path",
        out_dir.display()
    );
    anyhow::ensure!(
        !(trust_local.is_some() && signer_set.is_some()),
        "a persisted signer set cannot be reused with --trust-local; omit --signers so both passes share retained handles"
    );
    let staging = create_staging_dir(out_dir)?;
    let binding_kind = if trust_local.is_some() {
        GenerationBindingKind::TrustedLocalAssertedImmutable
    } else {
        GenerationBindingKind::PublishedManifest
    };
    let result = build_dense_index_to_staging(
        archive_root,
        epoch,
        &staging,
        trust_local,
        binding_kind,
        signer_set,
        shard_width,
        threads,
        batch_pairs,
        queued_batches,
    )
    .and_then(|()| sync_tree(&staging))
    .and_then(|()| {
        publish_staging_directory(&staging, out_dir)?;
        sync_directory(output_parent(out_dir))
    });
    if result.is_err()
        && staging.exists()
        && let Err(cleanup_error) = fs::remove_dir_all(&staging)
    {
        tracing::warn!(
            path = %staging.display(),
            %cleanup_error,
            "failed to remove owned dense-index staging directory"
        );
    }
    result
}

/// Build the dense index from an archive reader that the caller already
/// opened, including readers backed by HTTP range requests.
///
/// `registry_path` and `registry_index_path` are local control-file cache
/// paths. They must be named `registry.bin` and `registry.mphf`, and they must
/// have the same directory. The builder hashes and retains both files, checks
/// both published bindings, proves their exact mapping, and records that cache
/// directory in the index manifest. The archive reader must refer to a published immutable
/// generation. The caller is responsible for opening it with the required
/// content-verification policy before this function starts.
///
/// This function uses the same scan, shard writer, manifest, staging, and
/// no-replace publication path as [`build_dense_index`]. It always performs
/// both scan passes and does not accept a persisted signer-rank artifact.
pub fn build_dense_index_from_reader<S: RangeSource>(
    archive: &ArchiveReader<S>,
    registry_path: &Path,
    registry_index_path: &Path,
    out_dir: &Path,
    options: DenseIndexBuildOptions,
) -> Result<()> {
    validate_dense_build_options(options)?;
    anyhow::ensure!(
        !out_dir.exists(),
        "refusing to overwrite existing index output {}; choose a new immutable output path",
        out_dir.display()
    );

    let canonical_registry_root =
        canonical_registry_cache_root(registry_path, registry_index_path)?;
    let canonical_registry_path = canonical_registry_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let canonical_registry_index_path =
        canonical_registry_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    let staging = create_staging_dir(out_dir)?;
    let result = open_registry_artifacts_at(
        &canonical_registry_path,
        &canonical_registry_index_path,
        archive,
        GenerationBindingKind::PublishedManifest,
    )
    .and_then(|registry| {
        verify_published_registry_index_binding(archive, &registry)?;
        build_dense_index_from_open_reader_to_staging(
            &canonical_registry_root,
            archive,
            &registry,
            &staging,
            GenerationBindingKind::PublishedManifest,
            None,
            options,
            &|| Ok(()),
        )
    })
    .and_then(|()| sync_tree(&staging))
    .and_then(|()| {
        publish_staging_directory(&staging, out_dir)?;
        sync_directory(output_parent(out_dir))
    });
    if result.is_err()
        && staging.exists()
        && let Err(cleanup_error) = fs::remove_dir_all(&staging)
    {
        tracing::warn!(
            path = %staging.display(),
            %cleanup_error,
            "failed to remove owned dense-index staging directory"
        );
    }
    result
}

fn validate_dense_build_options(options: DenseIndexBuildOptions) -> Result<()> {
    anyhow::ensure!(
        options.shard_width > 0,
        "shard_width must be greater than zero"
    );
    anyhow::ensure!(
        options.threads > 0 && options.threads <= MAX_SCAN_THREADS,
        "threads must be in 1..={MAX_SCAN_THREADS}"
    );
    anyhow::ensure!(
        options.batch_pairs > 0 && options.batch_pairs <= MAX_RELATION_BATCH_PAIRS,
        "batch_pairs must be in 1..={MAX_RELATION_BATCH_PAIRS}"
    );
    anyhow::ensure!(
        options.queued_batches > 0 && options.queued_batches <= MAX_QUEUED_RELATION_BATCHES,
        "queued_batches must be in 1..={MAX_QUEUED_RELATION_BATCHES}"
    );
    validate_relation_pipeline_buffer(options.threads, options.batch_pairs, options.queued_batches)
}

fn validate_relation_pipeline_buffer(
    threads: usize,
    batch_pairs: usize,
    queued_batches: usize,
) -> Result<()> {
    if threads <= 1 {
        return Ok(());
    }
    let retained_batches = threads
        .checked_mul(2)
        .and_then(|worker_batches| queued_batches.checked_add(worker_batches))
        .and_then(|batches| batches.checked_add(1))
        .context("relation pipeline buffer-count overflow")?;
    let bytes = retained_batches
        .checked_mul(batch_pairs)
        .and_then(|pairs| pairs.checked_mul(std::mem::size_of::<(u32, u32)>()))
        .context("relation pipeline byte-size overflow")?;
    anyhow::ensure!(
        bytes <= MAX_RELATION_PIPELINE_BUFFER_BYTES,
        "relation pipeline could retain about {bytes} bytes, above the {}-byte safety limit; reduce --batch-pairs, --queued-batches, or --threads",
        MAX_RELATION_PIPELINE_BUFFER_BYTES
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_dense_index_to_staging(
    archive_root: &Path,
    epoch: u64,
    out_dir: &Path,
    trust_local: Option<TrustLocal>,
    binding_kind: GenerationBindingKind,
    signer_set: Option<&Path>,
    shard_width: u32,
    threads: usize,
    batch_pairs: usize,
    queued_batches: usize,
) -> Result<()> {
    anyhow::ensure!(shard_width > 0, "shard_width must be greater than zero");
    anyhow::ensure!(
        threads > 0 && threads <= MAX_SCAN_THREADS,
        "threads must be in 1..={MAX_SCAN_THREADS}"
    );
    anyhow::ensure!(
        batch_pairs > 0 && batch_pairs <= MAX_RELATION_BATCH_PAIRS,
        "batch_pairs must be in 1..={MAX_RELATION_BATCH_PAIRS}"
    );
    anyhow::ensure!(
        queued_batches > 0 && queued_batches <= MAX_QUEUED_RELATION_BATCHES,
        "queued_batches must be in 1..={MAX_QUEUED_RELATION_BATCHES}"
    );

    let canonical_archive_root = fs::canonicalize(archive_root)
        .with_context(|| format!("canonicalize archive root {}", archive_root.display()))?;
    let archive = open_archive(&canonical_archive_root, epoch, trust_local)?;
    let registry = open_registry_artifacts(&canonical_archive_root, &archive, binding_kind)?;
    let verify_source = || {
        archive
            .source()
            .verify_unchanged()
            .map_err(anyhow::Error::from)
    };
    build_dense_index_from_open_reader_to_staging(
        &canonical_archive_root,
        &archive,
        &registry,
        out_dir,
        binding_kind,
        signer_set,
        DenseIndexBuildOptions {
            shard_width,
            threads,
            batch_pairs,
            queued_batches,
        },
        &verify_source,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_dense_index_from_open_reader_to_staging<S: RangeSource>(
    canonical_archive_root: &Path,
    archive: &ArchiveReader<S>,
    registry: &RegistryArtifacts,
    out_dir: &Path,
    binding_kind: GenerationBindingKind,
    signer_set: Option<&Path>,
    options: DenseIndexBuildOptions,
    verify_source: &impl Fn() -> Result<()>,
) -> Result<()> {
    let DenseIndexBuildOptions {
        shard_width,
        threads,
        batch_pairs,
        queued_batches,
    } = options;
    let epoch = archive.manifest().epoch;
    let expected_signer_binding = registry.signer_set_binding(archive);

    let (signer_rank, discovery_stats) = match signer_set {
        Some(path) => {
            anyhow::ensure!(
                binding_kind == GenerationBindingKind::PublishedManifest,
                "persisted signer sets require a published archive generation"
            );
            (
                SignerRank::open(path, expected_signer_binding)
                    .with_context(|| format!("open signer set {}", path.display()))?,
                None,
            )
        }
        None => {
            tracing::info!(threads, "dense build pass 1: discovering signers");
            let (rank, stats) = discover_signer_rank(archive, expected_signer_binding, threads)?;
            (rank, Some(stats))
        }
    };
    verify_source().context("archive object changed during dense build pass 1")?;

    let maximum_batch = NonZeroUsize::new(batch_pairs).expect("validated nonzero batch size");
    let mut accumulator = DenseAccumulator::new(
        signer_rank.signer_count(),
        archive.registry_entries(),
        maximum_batch,
    );
    tracing::info!(
        signers = signer_rank.signer_count(),
        threads,
        batch_pairs,
        queued_batches,
        "dense build pass 2: decoding relations"
    );
    let scan_stats = scan_into_dense_accumulator(
        archive,
        &signer_rank,
        expected_signer_binding,
        &mut accumulator,
        threads,
        batch_pairs,
        queued_batches,
        true,
    )?;
    if let Some(discovery) = discovery_stats {
        anyhow::ensure!(
            discovery.transactions_scanned == scan_stats.transactions_scanned
                && discovery.blocks_scanned == scan_stats.blocks_scanned
                && discovery.failed_transactions_excluded
                    == scan_stats.failed_transactions_excluded,
            "dense pass 1 scanned {} tx / {} blocks / {} excluded failed tx, but pass 2 scanned {} tx / {} blocks / {} excluded failed tx",
            discovery.transactions_scanned,
            discovery.blocks_scanned,
            discovery.failed_transactions_excluded,
            scan_stats.transactions_scanned,
            scan_stats.blocks_scanned,
            scan_stats.failed_transactions_excluded,
        );
    }

    let registry_entries = archive.registry_entries();
    let chunk_width = shard_width.min(registry_entries);
    let shard_count = registry_entries.div_ceil(chunk_width);
    let mut shard_bindings = Vec::with_capacity(shard_count as usize);
    let mut program_tracker = ProgramTracker::new(registry_entries);
    let mut wallets = accumulator.wallets(signer_rank.iter_ids()).peekable();
    let mut program_scratch = Vec::<u32>::new();
    let mut wallet_count = 0u64;
    let mut relation_count = 0usize;

    for shard in 0..shard_count {
        let chunk_start = 1 + shard * chunk_width;
        let chunk_end = (chunk_start - 1)
            .checked_add(chunk_width)
            .context("dense shard range overflow")?
            .min(registry_entries);
        let shard_dir = out_dir.join(format!("shard-{shard}"));
        let mut writer = ShardWriter::create(&shard_dir)
            .with_context(|| format!("create dense shard {shard}"))?;

        loop {
            let consume = match wallets.peek() {
                Some(Ok(wallet)) => wallet.wallet_id() <= chunk_end,
                Some(Err(_)) => true,
                None => false,
            };
            if !consume {
                break;
            }
            let wallet = wallets
                .next()
                .expect("peeked wallet exists")
                .context("join dense ranks to signer registry ids")?;
            anyhow::ensure!(
                wallet.wallet_id() >= chunk_start,
                "wallet {} appeared before dense shard {shard} range {chunk_start}..={chunk_end}",
                wallet.wallet_id()
            );
            program_scratch.clear();
            program_scratch.extend(wallet.programs());
            relation_count = relation_count
                .checked_add(program_scratch.len())
                .context("dense relation output count overflow")?;
            for &program_id in &program_scratch {
                program_tracker.observe_id(program_id);
            }
            writer
                .push_sorted(wallet.wallet_id(), &program_scratch)
                .with_context(|| {
                    format!("write wallet {} to dense shard {shard}", wallet.wallet_id())
                })?;
        }
        wallet_count += writer
            .finish()
            .with_context(|| format!("finish dense shard {shard}"))?;
        shard_bindings.push(
            bind_shard(shard, &shard_dir, chunk_width, registry_entries)
                .with_context(|| format!("bind immutable dense shard {shard}"))?,
        );
    }
    match wallets.next() {
        Some(Ok(wallet)) => anyhow::bail!(
            "wallet {} remains after the final dense shard",
            wallet.wallet_id()
        ),
        Some(Err(error)) => return Err(error).context("finish dense wallet mapping"),
        None => {}
    }
    anyhow::ensure!(
        wallet_count == u64::from(accumulator.wallet_count()),
        "wrote {wallet_count} wallets but dense accumulator contains {}",
        accumulator.wallet_count()
    );
    anyhow::ensure!(
        relation_count == accumulator.relation_count(),
        "wrote {relation_count} relations but dense accumulator contains {}",
        accumulator.relation_count()
    );
    anyhow::ensure!(
        program_tracker.count() == accumulator.distinct_program_count() as usize,
        "wrote {} programs but dense accumulator tracks {}",
        program_tracker.count(),
        accumulator.distinct_program_count()
    );

    finalize_index(
        canonical_archive_root,
        archive,
        registry,
        out_dir,
        binding_kind,
        epoch,
        chunk_width,
        shard_count,
        shard_bindings,
        wallet_count,
        &program_tracker,
        scan_stats,
        verify_source,
    )
}

/// Atomically publish a fully synced staging directory without ever replacing
/// an existing path. The initial `exists` check in [`build_index`] gives an
/// early, friendly error; `NOREPLACE` closes the check/rename race and is the
/// operation that enforces immutable generation publication.
fn publish_staging_directory(staging: &Path, final_path: &Path) -> Result<()> {
    anyhow::ensure!(
        !final_path.exists(),
        "refusing to overwrite index output {} because it appeared while the build was running",
        final_path.display()
    );
    renameat_with(CWD, staging, CWD, final_path, RenameFlags::NOREPLACE)
        .map_err(std::io::Error::from)
        .with_context(|| {
            format!(
                "atomically publish {} as {} without replacement",
                staging.display(),
                final_path.display()
            )
        })
}

struct RegistryArtifacts {
    registry_path: PathBuf,
    registry_file: File,
    registry_identity: RegistryFileIdentity,
    registry_binding: IndexFileBinding,
    registry_sha256: [u8; 32],
    registry_index_path: PathBuf,
    registry_index_file: File,
    registry_index_identity: RegistryFileIdentity,
    registry_index_binding: IndexFileBinding,
}

impl RegistryArtifacts {
    fn signer_set_binding<S: RangeSource>(&self, archive: &ArchiveReader<S>) -> SignerSetBinding {
        SignerSetBinding {
            registry_entries: archive.registry_entries(),
            generation_digest: archive.binding().generation_digest,
            registry_size: self.registry_identity.size,
            registry_sha256: self.registry_sha256,
        }
    }

    fn verify_unchanged(&self) -> Result<()> {
        anyhow::ensure!(
            sha256_file_handle(&self.registry_file, &self.registry_path)?
                == self.registry_binding.sha256
                && file_identity_for_file(&self.registry_file, &self.registry_path)?
                    == self.registry_identity
                && file_identity(&self.registry_path)? == self.registry_identity,
            "registry.bin changed while the index was being built"
        );
        anyhow::ensure!(
            sha256_file_handle(&self.registry_index_file, &self.registry_index_path)?
                == self.registry_index_binding.sha256
                && file_identity_for_file(&self.registry_index_file, &self.registry_index_path,)?
                    == self.registry_index_identity
                && file_identity(&self.registry_index_path)? == self.registry_index_identity,
            "registry.mphf changed while the index was being built"
        );
        Ok(())
    }
}

fn canonical_registry_cache_root(
    registry_path: &Path,
    registry_index_path: &Path,
) -> Result<PathBuf> {
    anyhow::ensure!(
        registry_path.file_name().and_then(|name| name.to_str())
            == Some(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
        "registry cache path must end in {ARCHIVE_V2_PUBKEY_REGISTRY_FILE}"
    );
    anyhow::ensure!(
        registry_index_path
            .file_name()
            .and_then(|name| name.to_str())
            == Some(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE),
        "registry index cache path must end in {ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE}"
    );
    let registry_root = fs::canonicalize(output_parent(registry_path)).with_context(|| {
        format!(
            "canonicalize registry cache directory {}",
            output_parent(registry_path).display()
        )
    })?;
    let registry_index_root =
        fs::canonicalize(output_parent(registry_index_path)).with_context(|| {
            format!(
                "canonicalize registry index cache directory {}",
                output_parent(registry_index_path).display()
            )
        })?;
    anyhow::ensure!(
        registry_root == registry_index_root,
        "registry.bin and registry.mphf cache paths must have the same directory"
    );
    Ok(registry_root)
}

fn open_registry_artifacts<S: RangeSource>(
    canonical_archive_root: &Path,
    archive: &ArchiveReader<S>,
    binding_kind: GenerationBindingKind,
) -> Result<RegistryArtifacts> {
    let registry_path = canonical_archive_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let registry_index_path = canonical_archive_root.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    open_registry_artifacts_at(&registry_path, &registry_index_path, archive, binding_kind)
}

fn open_registry_artifacts_at<S: RangeSource>(
    registry_path: &Path,
    registry_index_path: &Path,
    archive: &ArchiveReader<S>,
    binding_kind: GenerationBindingKind,
) -> Result<RegistryArtifacts> {
    anyhow::ensure!(archive.registry_entries() > 0, "archive registry is empty");
    let registry_path = registry_path.to_path_buf();
    let registry_file = open_file(&registry_path)
        .with_context(|| format!("open retained registry {}", registry_path.display()))?;
    let registry_identity = file_identity_for_file(&registry_file, &registry_path)?;
    let registry_sha256 = sha256_file_handle_bytes(&registry_file, &registry_path)?;
    let registry_binding = IndexFileBinding {
        size: registry_identity.size,
        sha256: encode_sha256(registry_sha256),
    };

    let registry_index_path = registry_index_path.to_path_buf();
    let registry_index_file = open_file(&registry_index_path).with_context(|| {
        format!(
            "open retained registry index {}",
            registry_index_path.display()
        )
    })?;
    let registry_index_identity =
        file_identity_for_file(&registry_index_file, &registry_index_path)?;
    let registry_index_binding = IndexFileBinding {
        size: registry_index_identity.size,
        sha256: sha256_file_handle(&registry_index_file, &registry_index_path)?,
    };
    let key_index = FileBackedKeyIndex::load_file(
        registry_index_file
            .try_clone()
            .context("clone retained registry.mphf handle")?,
        &registry_index_path,
    )
    .with_context(|| format!("validate {}", registry_index_path.display()))?;
    anyhow::ensure!(
        key_index.len() == archive.registry_entries() as usize,
        "registry.mphf contains {} keys, archive registry contains {}",
        key_index.len(),
        archive.registry_entries()
    );
    validate_registry_index_mapping(
        &registry_file,
        &registry_path,
        &key_index,
        archive.registry_entries(),
    )?;
    anyhow::ensure!(
        file_identity_for_file(&registry_file, &registry_path)? == registry_identity
            && file_identity_for_file(&registry_index_file, &registry_index_path)?
                == registry_index_identity,
        "registry.bin or registry.mphf changed while their mapping was being validated"
    );
    if binding_kind == GenerationBindingKind::PublishedManifest {
        let declared = archive
            .manifest()
            .file(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)
            .context("published generation manifest has no registry.bin entry")?;
        anyhow::ensure!(
            declared.sha256 == registry_binding.sha256,
            "retained registry.bin does not match the fully verified generation manifest"
        );
    }
    anyhow::ensure!(
        registry_identity.size == u64::from(archive.registry_entries()) * 32,
        "registry identity size changed while opening the archive"
    );

    Ok(RegistryArtifacts {
        registry_path,
        registry_file,
        registry_identity,
        registry_binding,
        registry_sha256,
        registry_index_path,
        registry_index_file,
        registry_index_identity,
        registry_index_binding,
    })
}

fn verify_published_registry_index_binding<S: RangeSource>(
    archive: &ArchiveReader<S>,
    registry: &RegistryArtifacts,
) -> Result<()> {
    let declared = archive
        .manifest()
        .file(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)
        .context("published generation manifest has no registry.mphf entry")?;
    anyhow::ensure!(
        declared.size == registry.registry_index_binding.size
            && declared.sha256 == registry.registry_index_binding.sha256,
        "retained registry.mphf does not match the fully verified generation manifest"
    );
    Ok(())
}

fn build_index_to_staging(
    archive_root: &Path,
    epoch: u64,
    out_dir: &Path,
    trust_local: Option<TrustLocal>,
    binding_kind: GenerationBindingKind,
    max_accounts_per_chunk: u32,
    threads: usize,
) -> Result<()> {
    anyhow::ensure!(
        max_accounts_per_chunk > 0,
        "max_accounts_per_chunk must be greater than zero"
    );
    anyhow::ensure!(threads > 0, "threads must be greater than zero");
    anyhow::ensure!(
        threads <= MAX_SCAN_THREADS,
        "threads must not exceed {MAX_SCAN_THREADS}"
    );
    let canonical_archive_root = fs::canonicalize(archive_root)
        .with_context(|| format!("canonicalize archive root {}", archive_root.display()))?;
    let archive = open_archive(&canonical_archive_root, epoch, trust_local)?;
    let registry = open_registry_artifacts(&canonical_archive_root, &archive, binding_kind)?;
    let registry_entries = archive.registry_entries();

    let chunk_width = max_accounts_per_chunk.min(registry_entries.max(1));
    let shard_count = registry_entries.max(1).div_ceil(chunk_width);

    let mut program_tracker = ProgramTracker::new(registry_entries);
    let mut aggregate = ScanStats::default();
    let mut wallet_count: u64 = 0;
    let mut shard_bindings = Vec::<ShardBinding>::with_capacity(shard_count as usize);

    for shard in 0..shard_count {
        let chunk_start = 1 + shard * chunk_width;
        let this_chunk_width = chunk_width.min(registry_entries - (chunk_start - 1));

        tracing::info!(
            shard,
            shard_count,
            chunk_start,
            chunk_width = this_chunk_width,
            threads,
            "shard starting"
        );

        let (mut builder, stats) = if threads == 1 {
            let mut builder = IndexBuilder::new(chunk_start, this_chunk_width, registry_entries);
            let stats = scan_into_builder(&archive, &mut builder, true)?;
            (builder, stats)
        } else {
            scan_into_builder_parallel(
                &archive,
                chunk_start,
                this_chunk_width,
                registry_entries,
                threads,
                true,
            )?
        };

        let shard_dir = out_dir.join(format!("shard-{shard}"));
        let shard_wallets = builder
            .write(&shard_dir)
            .with_context(|| format!("write shard {shard} to {}", out_dir.display()))?;
        program_tracker.observe(&builder);
        shard_bindings.push(
            bind_shard(shard, &shard_dir, chunk_width, registry_entries)
                .with_context(|| format!("bind immutable shard {shard}"))?,
        );
        wallet_count += shard_wallets;

        if shard == 0 {
            aggregate.transactions_scanned = stats.transactions_scanned;
            aggregate.blocks_scanned = stats.blocks_scanned;
            aggregate.failed_transactions_excluded = stats.failed_transactions_excluded;
        } else {
            // Every shard re-decodes the same archive; if a later pass
            // disagrees on how many transactions/blocks it saw, the archive
            // changed underneath us mid-build (or a pass has a real bug) —
            // either way the resulting shards would be inconsistent with
            // each other, so fail loudly rather than publish a bad index.
            anyhow::ensure!(
                stats.transactions_scanned == aggregate.transactions_scanned
                    && stats.blocks_scanned == aggregate.blocks_scanned
                    && stats.failed_transactions_excluded == aggregate.failed_transactions_excluded,
                "shard {shard} scanned {} tx / {} blocks / {} excluded failed tx, but shard 0 scanned {} tx / {} blocks / {} excluded failed tx \
                 — the archive may have changed between passes",
                stats.transactions_scanned,
                stats.blocks_scanned,
                stats.failed_transactions_excluded,
                aggregate.transactions_scanned,
                aggregate.blocks_scanned,
                aggregate.failed_transactions_excluded,
            );
        }

        tracing::info!(shard, shard_count, shard_wallets, "shard complete");
    }

    let verify_source = || {
        archive
            .source()
            .verify_unchanged()
            .map_err(anyhow::Error::from)
    };
    finalize_index(
        &canonical_archive_root,
        &archive,
        &registry,
        out_dir,
        binding_kind,
        epoch,
        chunk_width,
        shard_count,
        shard_bindings,
        wallet_count,
        &program_tracker,
        aggregate,
        &verify_source,
    )
}

#[allow(clippy::too_many_arguments)]
fn finalize_index<S: RangeSource>(
    canonical_archive_root: &Path,
    archive: &ArchiveReader<S>,
    registry: &RegistryArtifacts,
    out_dir: &Path,
    binding_kind: GenerationBindingKind,
    epoch: u64,
    chunk_width: u32,
    shard_count: u32,
    shard_bindings: Vec<ShardBinding>,
    wallet_count: u64,
    program_tracker: &ProgramTracker,
    aggregate: ScanStats,
    verify_source: &impl Fn() -> Result<()>,
) -> Result<()> {
    anyhow::ensure!(
        shard_bindings.len() == shard_count as usize,
        "built {} shard bindings but expected {shard_count}",
        shard_bindings.len()
    );
    let mut program_entries = Vec::new();
    program_entries
        .try_reserve_exact(program_tracker.count())
        .context("reserve distinct program registry map")?;
    for program_id in program_tracker.ids() {
        program_entries.push((
            program_id,
            registry_pubkey_at(&registry.registry_file, program_id)
                .with_context(|| format!("resolve program registry id {program_id}"))?,
        ));
    }
    let program_map = write_program_map(out_dir, &program_entries)
        .with_context(|| format!("write bound program map to {}", out_dir.display()))?;

    let built_unix_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let manifest = IndexManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        format_version: FORMAT_VERSION,
        semantics: IndexSemantics::current(),
        complete: true,
        omissions: OmissionCounts::default(),
        binding_kind,
        cluster_id: archive.manifest().cluster_id.clone(),
        epoch,
        archive_root: canonical_archive_root.display().to_string(),
        generation_id: archive.manifest().generation_id.clone(),
        generation_digest: archive.manifest().generation_digest.clone(),
        registry: registry.registry_binding.clone(),
        registry_file_identity: registry.registry_identity.clone(),
        registry_index: registry.registry_index_binding.clone(),
        registry_index_file_identity: registry.registry_index_identity.clone(),
        registry_entries: archive.registry_entries(),
        chunk_width,
        shard_count,
        shards: shard_bindings,
        program_map,
        wallet_count,
        program_count: program_tracker.count() as u64,
        transactions_scanned: aggregate.transactions_scanned,
        blocks_scanned: aggregate.blocks_scanned,
        failed_transactions_excluded: aggregate.failed_transactions_excluded,
        built_unix_time,
        tool_version: env!("CARGO_PKG_VERSION").to_string(),
    };

    tracing::info!(
        shard_count,
        chunk_width,
        wallets = wallet_count,
        programs = manifest.program_count,
        transactions_scanned = manifest.transactions_scanned,
        blocks_scanned = manifest.blocks_scanned,
        failed_transactions_excluded = manifest.failed_transactions_excluded,
        "build complete, writing manifest"
    );
    registry.verify_unchanged()?;
    verify_source().context("archive object changed while the index was being built")?;
    manifest
        .write(out_dir)
        .with_context(|| format!("write manifest to {}", out_dir.display()))
}

fn create_staging_dir(final_path: &Path) -> Result<PathBuf> {
    let parent = output_parent(final_path);
    fs::create_dir_all(parent)
        .with_context(|| format!("create index output parent {}", parent.display()))?;
    let name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .context("index output path must have a UTF-8 file name")?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for attempt in 0..100u32 {
        let candidate = parent.join(format!(
            ".{name}.staging-{}-{now}-{attempt}",
            std::process::id()
        ));
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create staging directory {}", candidate.display()));
            }
        }
    }
    anyhow::bail!("could not allocate a unique index staging directory")
}

fn output_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn sync_tree(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path).with_context(|| format!("read {}", path.display()))? {
        let entry = entry.with_context(|| format!("read entry under {}", path.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("inspect {}", entry.path().display()))?;
        if file_type.is_dir() {
            sync_tree(&entry.path())?;
        } else if file_type.is_file() {
            File::open(entry.path())
                .and_then(|file| file.sync_all())
                .with_context(|| format!("sync {}", entry.path().display()))?;
        } else {
            anyhow::bail!(
                "unexpected non-file entry in index staging directory: {}",
                entry.path().display()
            );
        }
    }
    sync_directory(path)
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("sync directory {}", path.display()))
}

fn file_identity(path: &Path) -> Result<RegistryFileIdentity> {
    let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
    file_identity_from_metadata(metadata, path)
}

fn file_identity_for_file(file: &File, path: &Path) -> Result<RegistryFileIdentity> {
    let metadata = file
        .metadata()
        .with_context(|| format!("stat retained {}", path.display()))?;
    file_identity_from_metadata(metadata, path)
}

fn file_identity_from_metadata(
    metadata: fs::Metadata,
    path: &Path,
) -> Result<RegistryFileIdentity> {
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    Ok(RegistryFileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

fn sha256_file_handle(file: &File, path: &Path) -> Result<String> {
    Ok(encode_sha256(sha256_file_handle_bytes(file, path)?))
}

fn sha256_file_handle_bytes(file: &File, path: &Path) -> Result<[u8; 32]> {
    let mut buffer = vec![0u8; 8 * 1024 * 1024];
    let mut hasher = Sha256::new();
    let expected_len = file
        .metadata()
        .with_context(|| format!("stat {}", path.display()))?
        .len();
    let mut offset = 0u64;
    while offset < expected_len {
        let remaining = usize::try_from((expected_len - offset).min(buffer.len() as u64))
            .context("registry hash chunk length exceeds usize")?;
        let read = file
            .read_at(&mut buffer[..remaining], offset)
            .with_context(|| format!("hash {}", path.display()))?;
        if read == 0 {
            anyhow::bail!("{} was truncated while hashing", path.display());
        }
        hasher.update(&buffer[..read]);
        offset += read as u64;
    }
    anyhow::ensure!(
        file.metadata()
            .with_context(|| format!("re-stat {}", path.display()))?
            .len()
            == expected_len,
        "{} changed length while hashing",
        path.display()
    );
    Ok(hasher.finalize().into())
}

fn encode_sha256(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn registry_pubkey_at(file: &File, id: u32) -> Result<[u8; 32]> {
    anyhow::ensure!(id != 0, "registry ids are one-based");
    let offset = u64::from(id - 1)
        .checked_mul(32)
        .context("registry pubkey offset overflow")?;
    let mut pubkey = [0u8; 32];
    file.read_exact_at(&mut pubkey, offset)
        .with_context(|| format!("read registry.bin at byte offset {offset}"))?;
    Ok(pubkey)
}

/// Prove that `registry.mphf` belongs to this exact `registry.bin`, not just
/// to some other registry with the same cardinality. Both inputs are retained
/// handles opened by the caller; no pathname is reopened while validating the
/// correspondence.
fn validate_registry_index_mapping(
    registry_file: &File,
    registry_path: &Path,
    key_index: &FileBackedKeyIndex,
    registry_entries: u32,
) -> Result<()> {
    anyhow::ensure!(
        key_index.len() == registry_entries as usize,
        "registry.mphf contains {} keys, registry.bin contains {registry_entries}",
        key_index.len()
    );

    let mut reader = BufReader::with_capacity(8 << 20, registry_file);
    reader
        .seek(SeekFrom::Start(0))
        .with_context(|| format!("rewind retained registry {}", registry_path.display()))?;
    let mut pubkey = [0u8; 32];
    for id in 1..=registry_entries {
        reader.read_exact(&mut pubkey).with_context(|| {
            format!(
                "read registry.bin key {id} from {}",
                registry_path.display()
            )
        })?;
        let actual = key_index
            .lookup(&pubkey)
            .with_context(|| format!("lookup registry.bin key {id} in registry.mphf"))?;
        anyhow::ensure!(
            actual == Some(id),
            "registry.mphf does not map registry.bin key {id} to its 1-based position (got {actual:?})"
        );
    }
    Ok(())
}

/// Rough empirical cost of one *empty* `IndexBuilder` slot (an inline-
/// capacity `SmallVec<[u32; 8]>`, no heap spill yet), measured by
/// `index-bench`'s allocator counters: a 60,000,000-slot builder allocated
/// 2,898,315,052 bytes, ≈48.3 bytes/slot. Real chunks cost more once actual
/// relations accumulate; this is a floor, not an estimate of total memory.
pub const APPROX_BYTES_PER_EMPTY_SLOT: u64 = 48;

#[derive(Debug, Clone, Copy)]
pub struct DiscoverSignersStats {
    pub registry_entries: u32,
    pub distinct_signers: u64,
    pub transactions_scanned: u64,
    pub blocks_scanned: u64,
    pub failed_transactions_excluded: u64,
}

/// Scans one contiguous block-row range, decoding *only* enough of each
/// transaction to learn its signers (see `decode::decode_signers_with_schema` — no
/// instructions, no metadata, not even the rest of `account_keys`), and
/// marks each distinct signer registry id seen in `seen`. Shared by the
/// sequential (`discover_signers`, one range = the whole archive) and
/// parallel (`discover_signers_parallel`, one range per thread) entry
/// points. Each reader call stays in its assigned range. `RangeSource`
/// requires `Send + Sync` and immutable generation objects, so workers can
/// share one `&ArchiveReader` safely.
fn scan_signers_range<S: RangeSource>(
    archive: &ArchiveReader<S>,
    range: std::ops::Range<usize>,
    seen: &mut SignerSetBuilder,
) -> Result<ScanStats> {
    let mut stats = ScanStats::default();

    let mut blocks = archive
        .borrowed_blocks_without_rewards_range(range.clone())
        .with_context(|| format!("open block range {range:?}"))?;
    while let Some(block) = blocks.next_block() {
        let block = block.with_context(|| format!("decode block in range {range:?}"))?;
        let slot = block.header().slot;
        stats.blocks_scanned += 1;

        for row in block.tx_rows() {
            stats.transactions_scanned += 1;
            if !transaction_is_in_semantic_scope(&row)
                .with_context(|| format!("classify slot {slot} tx {}", row.tx_index))?
            {
                stats.failed_transactions_excluded += 1;
                continue;
            }
            let mut cursor = slice_range(
                block.message_bytes(),
                row.message_offset,
                row.message_len,
                "message",
                slot,
                row.tx_index,
            )?;
            let signers = decode::decode_signers_with_schema(&mut cursor, archive.message_schema())
                .with_context(|| format!("decode signers (slot {slot} tx {})", row.tx_index))?;
            anyhow::ensure!(
                signers.len() == usize::from(row.signature_count),
                "message requires {} signatures but the transaction row records {} (slot {slot} tx {})",
                signers.len(),
                row.signature_count,
                row.tx_index
            );
            for key in signers {
                match key {
                    CompactPubkey::Id(id) if id != 0 && id <= archive.registry_entries() => {
                        seen.insert(id)?;
                    }
                    CompactPubkey::Id(id) => anyhow::bail!(
                        "signer registry id {id} is outside 1..={} (slot {slot} tx {})",
                        archive.registry_entries(),
                        row.tx_index
                    ),
                    CompactPubkey::Raw(_) => anyhow::bail!(
                        "unresolved raw signer pubkey at slot {slot} tx {}",
                        row.tx_index
                    ),
                }
            }
        }
    }

    Ok(stats)
}

/// Scans the whole archive once, single-threaded, and returns the count of
/// **distinct** signer registry ids from V1-successful transactions. A cheap
/// way to learn the index's signer user population — typically far smaller than
/// its full registry, since most registered ids are token accounts, PDAs, and
/// programs that never sign — before committing to a build chunk width.
pub fn discover_signers<S: RangeSource>(
    archive: &ArchiveReader<S>,
) -> Result<DiscoverSignersStats> {
    let (seen, stats) = discover_signer_set_builder(archive, 1)?;
    Ok(discover_stats(&seen, stats))
}

/// Same as `discover_signers`, but splits the archive's block-row index into
/// `thread_count` contiguous ranges and scans them concurrently — each
/// thread reads/decodes its own range independently (see
/// `scan_signers_range`'s docs for why this is safe) into its own bitset,
/// merged (bitset OR) after every thread joins. This module never spawns
/// threads on its own; call this only when you've confirmed splitting I/O
/// across threads actually helps on the target storage (see `index-bench`'s
/// module docs for the reasoning — the archive read path is single-threaded
/// and synchronous by default, and threading only pays off if the
/// underlying storage has spare concurrent-I/O throughput to give).
pub fn discover_signers_parallel<S: RangeSource>(
    archive: &ArchiveReader<S>,
    thread_count: usize,
) -> Result<DiscoverSignersStats> {
    let (seen, stats) = discover_signer_set_builder(archive, thread_count)?;
    Ok(discover_stats(&seen, stats))
}

/// Discover and rank the exact signer population used by the V1 relation.
/// The caller supplies the already-verified archive/registry provenance that
/// will be embedded in a reusable pass-1 artifact.
pub fn discover_signer_rank<S: RangeSource>(
    archive: &ArchiveReader<S>,
    binding: SignerSetBinding,
    thread_count: usize,
) -> Result<(SignerRank, DiscoverSignersStats)> {
    let (seen, stats) = discover_signer_set_builder(archive, thread_count)?;
    let discovered = discover_stats(&seen, stats);
    let rank = seen.finish(binding)?;
    Ok((rank, discovered))
}

/// Run pass 1 and atomically persist its generation-bound signer/rank map.
/// This is intentionally limited to published generations: a trusted-local
/// manifest has no content digest for the archive payload, so an artifact
/// reused by a later process could otherwise be paired with changed blocks of
/// the same size.
pub fn discover_signers_to_file(
    archive: &ArchiveReader<PinnedLocalRangeSource>,
    out: &Path,
    thread_count: usize,
) -> Result<DiscoverSignersStats> {
    anyhow::ensure!(
        !out.exists(),
        "refusing to overwrite existing signer-set output {}",
        out.display()
    );
    fs::create_dir_all(output_parent(out)).with_context(|| {
        format!(
            "create signer-set output parent {}",
            output_parent(out).display()
        )
    })?;
    let generation_binding = archive.binding();
    anyhow::ensure!(
        generation_binding.registry_sha256 != [0; 32],
        "persisted signer discovery requires a published, content-bound archive generation"
    );
    let binding = SignerSetBinding {
        registry_entries: archive.registry_entries(),
        generation_digest: generation_binding.generation_digest,
        registry_size: u64::from(archive.registry_entries()) * 32,
        registry_sha256: generation_binding.registry_sha256,
    };
    let (rank, stats) = discover_signer_rank(archive, binding, thread_count)?;
    archive
        .source()
        .verify_unchanged()
        .context("archive object changed while discovering signers")?;
    rank.write_atomic(out)
        .with_context(|| format!("write signer set to {}", out.display()))?;
    Ok(stats)
}

fn discover_stats(seen: &SignerSetBuilder, stats: ScanStats) -> DiscoverSignersStats {
    DiscoverSignersStats {
        registry_entries: seen.registry_entries(),
        distinct_signers: u64::from(seen.signer_count()),
        transactions_scanned: stats.transactions_scanned,
        blocks_scanned: stats.blocks_scanned,
        failed_transactions_excluded: stats.failed_transactions_excluded,
    }
}

fn discover_signer_set_builder<S: RangeSource>(
    archive: &ArchiveReader<S>,
    thread_count: usize,
) -> Result<(SignerSetBuilder, ScanStats)> {
    let registry_entries = archive.registry_entries();
    let total_rows = archive.index().rows.len();
    anyhow::ensure!(
        thread_count > 0 && thread_count <= MAX_SCAN_THREADS,
        "thread_count must be in 1..={MAX_SCAN_THREADS}"
    );
    let thread_count = thread_count.min(total_rows.max(1));

    if thread_count == 1 || total_rows <= 1 {
        let mut seen = SignerSetBuilder::new(registry_entries)?;
        let stats = scan_signers_range(archive, 0..total_rows, &mut seen)?;
        return Ok((seen, stats));
    }

    let chunk_rows = total_rows.div_ceil(thread_count);
    let ranges: Vec<std::ops::Range<usize>> = (0..thread_count)
        .map(|i| (i * chunk_rows).min(total_rows)..((i + 1) * chunk_rows).min(total_rows))
        .filter(|range| !range.is_empty())
        .collect();

    let per_thread_results: Vec<Result<(SignerSetBuilder, ScanStats)>> =
        std::thread::scope(|scope| {
            let handles: Vec<_> = ranges
                .into_iter()
                .map(|range| {
                    scope.spawn(move || -> Result<(SignerSetBuilder, ScanStats)> {
                        let mut seen = SignerSetBuilder::new(registry_entries)?;
                        let stats = scan_signers_range(archive, range, &mut seen)?;
                        Ok((seen, stats))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| match handle.join() {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("discover-signers worker thread panicked")),
                })
                .collect()
        });

    let mut merged = SignerSetBuilder::new(registry_entries)?;
    let mut aggregate = ScanStats::default();
    for result in per_thread_results {
        let (bitset, worker_stats) = result?;
        merged.merge(bitset)?;
        stats_add(&mut aggregate, worker_stats);
    }
    Ok((merged, aggregate))
}

fn stats_add(total: &mut ScanStats, part: ScanStats) {
    total.transactions_scanned += part.transactions_scanned;
    total.blocks_scanned += part.blocks_scanned;
    total.failed_transactions_excluded += part.failed_transactions_excluded;
}

/// V1's exact semantic relation: for a successful transaction, associate every
/// required signer with every distinct program actually reached by its
/// top-level or recorded inner/CPI instructions. Failed transactions are
/// excluded before this function is called. Votes are included because the
/// compact vote-instruction flag is not an exact whole-transaction classifier.
struct ProgramIndexSet {
    seen: [bool; decode::MAX_MESSAGE_ACCOUNTS],
    values: [u8; decode::MAX_MESSAGE_ACCOUNTS],
    len: usize,
}

impl ProgramIndexSet {
    fn new() -> Self {
        Self {
            seen: [false; decode::MAX_MESSAGE_ACCOUNTS],
            values: [0; decode::MAX_MESSAGE_ACCOUNTS],
            len: 0,
        }
    }

    #[inline]
    fn insert(&mut self, index: u8) {
        let index_usize = usize::from(index);
        if !self.seen[index_usize] {
            self.seen[index_usize] = true;
            self.values[self.len] = index;
            self.len += 1;
        }
    }

    fn as_slice(&self) -> &[u8] {
        &self.values[..self.len]
    }
}

#[cfg(test)]
fn index_transaction(
    builder: &mut IndexBuilder,
    registry_entries: u32,
    slot: u64,
    row: &ArchiveV2HotTxRow,
    message_bytes: &[u8],
    metadata_bytes: &[u8],
) -> Result<()> {
    index_transaction_with_schemas(
        builder,
        registry_entries,
        CompactV2MessageSchema::Current,
        CompactV2MetadataSchema::CurrentTypedError,
        slot,
        row,
        message_bytes,
        metadata_bytes,
    )
}

#[allow(clippy::too_many_arguments)]
fn index_transaction_with_schemas(
    builder: &mut IndexBuilder,
    registry_entries: u32,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    slot: u64,
    row: &ArchiveV2HotTxRow,
    message_bytes: &[u8],
    metadata_bytes: &[u8],
) -> Result<()> {
    visit_transaction_relations(
        registry_entries,
        message_schema,
        metadata_schema,
        slot,
        row,
        message_bytes,
        metadata_bytes,
        |signer, program| match builder.record(signer, program) {
            RecordOutcome::Recorded | RecordOutcome::OutOfChunk => Ok(()),
            RecordOutcome::InvalidProgram => anyhow::bail!(
                "program registry id {program} is invalid (slot {slot} tx {})",
                row.tx_index
            ),
        },
    )
}

/// Decode one successful transaction into its exact V1 signer/program
/// cross-product. Keeping the semantic decoder independent of its sink lets
/// the legacy chunked builder record directly while Stage 3 workers stream
/// bounded relation batches to one dense accumulator.
#[allow(clippy::too_many_arguments)]
fn visit_transaction_relations(
    registry_entries: u32,
    message_schema: CompactV2MessageSchema,
    metadata_schema: CompactV2MetadataSchema,
    slot: u64,
    row: &ArchiveV2HotTxRow,
    message_bytes: &[u8],
    metadata_bytes: &[u8],
    mut on_relation: impl FnMut(u32, u32) -> Result<()>,
) -> Result<()> {
    let mut message_cursor = slice_range(
        message_bytes,
        row.message_offset,
        row.message_len,
        "message",
        slot,
        row.tx_index,
    )?;

    let mut program_indexes = ProgramIndexSet::new();
    let decoded_message =
        decode::decode_message_with_schema(&mut message_cursor, message_schema, |instruction| {
            program_indexes.insert(instruction.program_id_index);
        })
        .with_context(|| format!("decode message (slot {slot} tx {})", row.tx_index))?;
    let signer_count = usize::from(decoded_message.num_required_signatures);
    anyhow::ensure!(
        message_cursor.is_empty(),
        "message decode left {} trailing bytes (slot {slot} tx {})",
        message_cursor.len(),
        row.tx_index,
    );
    anyhow::ensure!(
        decoded_message.is_v0 == (row.flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "message version disagrees with row flags (slot {slot} tx {})",
        row.tx_index,
    );
    anyhow::ensure!(
        signer_count <= decoded_message.account_keys.len(),
        "required signer count exceeds static account keys (slot {slot} tx {})",
        row.tx_index
    );
    anyhow::ensure!(
        signer_count == usize::from(row.signature_count),
        "message requires {signer_count} signatures but the transaction row records {} (slot {slot} tx {})",
        row.signature_count,
        row.tx_index
    );

    let has_metadata = row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0 && row.metadata_len != 0;
    anyhow::ensure!(
        has_metadata,
        "successful transaction has no decoded metadata (slot {slot} tx {})",
        row.tx_index
    );
    let need_inner = row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0;
    let need_loaded = row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0;
    anyhow::ensure!(
        !need_loaded || decoded_message.is_v0,
        "legacy message declares loaded addresses (slot {slot} tx {})",
        row.tx_index
    );
    let expected_loaded = decoded_message
        .expected_loaded_writable
        .checked_add(decoded_message.expected_loaded_readonly)
        .context("loaded address count overflow")?;
    anyhow::ensure!(
        need_loaded == (expected_loaded != 0),
        "loaded-address presence disagrees with message lookups (slot {slot} tx {})",
        row.tx_index
    );
    let total_message_accounts = decoded_message
        .account_keys
        .len()
        .checked_add(expected_loaded)
        .context("total message account count overflow")?;
    anyhow::ensure!(
        program_indexes
            .as_slice()
            .iter()
            .all(|index| usize::from(*index) < total_message_accounts),
        "top-level instruction program index is outside message accounts (slot {slot} tx {})",
        row.tx_index
    );

    let mut accounts = decoded_message.account_keys;
    let mut metadata_cursor = slice_range(
        metadata_bytes,
        row.metadata_offset,
        row.metadata_len,
        "metadata",
        slot,
        row.tx_index,
    )?;
    // Legacy transactions without recorded CPIs keep the one-field outcome
    // fast path. V0 must always reach the loaded vectors—even when empty—so
    // their writable/readonly lengths can be matched exactly to the message.
    let metadata_error = if decoded_message.is_v0 || need_inner {
        let decoded_metadata = decode::decode_metadata_prefix_with_schema(
            &mut metadata_cursor,
            metadata_schema,
            decoded_message.is_v0,
            decode::MetadataDecodeLimits {
                total_message_accounts,
                top_level_instruction_count: decoded_message.instruction_count,
            },
            |instruction| {
                // `decode_metadata_prefix` validates this against the exact
                // message-account count before invoking the callback.
                program_indexes.insert(instruction.program_id_index as u8);
            },
        )
        .with_context(|| format!("decode metadata (slot {slot} tx {})", row.tx_index))?;
        anyhow::ensure!(
            decoded_metadata.inner_instructions_present == need_inner,
            "inner-instruction presence disagrees with row flags (slot {slot} tx {})",
            row.tx_index
        );
        match decoded_metadata.loaded_addresses {
            Some((writable, readonly)) => {
                anyhow::ensure!(
                    writable.len() == decoded_message.expected_loaded_writable,
                    "metadata has {} loaded writable addresses but message lookups require {} (slot {slot} tx {})",
                    writable.len(),
                    decoded_message.expected_loaded_writable,
                    row.tx_index
                );
                anyhow::ensure!(
                    readonly.len() == decoded_message.expected_loaded_readonly,
                    "metadata has {} loaded readonly addresses but message lookups require {} (slot {slot} tx {})",
                    readonly.len(),
                    decoded_message.expected_loaded_readonly,
                    row.tx_index
                );
                accounts.extend(writable);
                accounts.extend(readonly);
            }
            None => anyhow::ensure!(
                !decoded_message.is_v0,
                "V0 metadata did not contain loaded-address vectors (slot {slot} tx {})",
                row.tx_index
            ),
        }
        decoded_metadata.has_error
    } else {
        decode::decode_metadata_error_with_schema(&mut metadata_cursor, metadata_schema)
            .with_context(|| format!("decode metadata outcome (slot {slot} tx {})", row.tx_index))?
    };
    anyhow::ensure!(
        !metadata_error,
        "metadata outcome disagrees with successful row flags (slot {slot} tx {})",
        row.tx_index
    );

    let mut signers: SmallVec<[u32; 2]> = SmallVec::new();
    for key in accounts.iter().take(signer_count) {
        signers.push(required_registry_id(
            key,
            registry_entries,
            "signer",
            slot,
            row.tx_index,
        )?);
    }
    signers.sort_unstable();
    signers.dedup();

    let mut programs: SmallVec<[u32; 8]> = SmallVec::new();
    for &program_index in program_indexes.as_slice() {
        let program_index = usize::from(program_index);
        let key = accounts.get(program_index).with_context(|| {
            format!(
                "program account index {program_index} is out of range (slot {slot} tx {})",
                row.tx_index
            )
        })?;
        programs.push(required_registry_id(
            key,
            registry_entries,
            "program",
            slot,
            row.tx_index,
        )?);
    }
    programs.sort_unstable();
    programs.dedup();

    for signer in signers {
        for &program in &programs {
            on_relation(signer, program)?;
        }
    }
    Ok(())
}

fn required_registry_id(
    key: &CompactPubkey,
    registry_entries: u32,
    role: &str,
    slot: u64,
    tx_index: u32,
) -> Result<u32> {
    match key {
        CompactPubkey::Id(id) if *id != 0 && *id <= registry_entries => Ok(*id),
        CompactPubkey::Id(id) => anyhow::bail!(
            "{role} registry id {id} is outside 1..={registry_entries} (slot {slot} tx {tx_index})"
        ),
        CompactPubkey::Raw(_) => {
            anyhow::bail!("unresolved raw {role} pubkey (slot {slot} tx {tx_index})")
        }
    }
}

fn slice_range<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    kind: &str,
    slot: u64,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let end = start
        .checked_add(len as usize)
        .with_context(|| format!("{kind} range overflow (slot {slot} tx {tx_index})"))?;
    bytes
        .get(start..end)
        .with_context(|| format!("{kind} range outside block payload (slot {slot} tx {tx_index})"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::IndexReader;
    use blockzilla_archive_v2::{
        ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotInstruction,
        ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
        ArchiveV2HotV0Message,
    };
    use blockzilla_compact::{
        CompactInnerInstruction, CompactInnerInstructions, CompactMessageHeader, CompactMetaV1,
        OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash,
    };
    use blockzilla_primitives::wincode_leb128_config;

    // Keep the historical wire shape inline. Boxing this test-only variant
    // would change the bytes produced by the schema writer.
    #[allow(dead_code, clippy::large_enum_variant)]
    #[derive(wincode::SchemaWrite)]
    enum TestMay24MessagePayload {
        Legacy(TestMay24LegacyMessage),
        V0(()),
    }

    #[derive(wincode::SchemaWrite)]
    struct TestMay24LegacyMessage {
        header: CompactMessageHeader,
        account_keys: SmallVec<[CompactPubkey; 4]>,
        recent_blockhash: OwnedCompactRecentBlockhash,
        instructions: SmallVec<[TestMay24Instruction; 4]>,
    }

    #[derive(wincode::SchemaWrite)]
    struct TestMay24Instruction {
        program_id_index: u8,
        accounts: SmallVec<[u8; 8]>,
        data: TestMay24InstructionData,
    }

    #[allow(dead_code)]
    #[derive(wincode::SchemaWrite)]
    enum TestMay24InstructionData {
        Raw(SmallVec<[u8; 32]>),
        ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    }

    #[test]
    fn published_builds_hash_every_archive_object() {
        assert_eq!(archive_hash_verification(false), HashVerification::AllFiles);
        assert_eq!(archive_hash_verification(true), HashVerification::SizesOnly);
    }

    #[test]
    fn reader_builder_requires_one_exact_registry_cache_directory() {
        let registry_cache = tempfile::tempdir().unwrap();
        let registry = registry_cache.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
        let registry_index = registry_cache
            .path()
            .join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        assert_eq!(
            canonical_registry_cache_root(&registry, &registry_index).unwrap(),
            fs::canonicalize(registry_cache.path()).unwrap()
        );

        let other_cache = tempfile::tempdir().unwrap();
        let misplaced_index = other_cache
            .path()
            .join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        assert!(canonical_registry_cache_root(&registry, &misplaced_index).is_err());
        assert!(
            canonical_registry_cache_root(
                &registry_cache.path().join("renamed-registry.bin"),
                &registry_index,
            )
            .is_err()
        );
    }

    #[test]
    fn relation_scan_uses_historical_message_schema() {
        let historical_message = TestMay24MessagePayload::Legacy(TestMay24LegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: SmallVec::from_vec(vec![CompactPubkey::Id(1), CompactPubkey::Id(2)]),
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: SmallVec::from_vec(vec![TestMay24Instruction {
                program_id_index: 1,
                accounts: SmallVec::new(),
                data: TestMay24InstructionData::ComputeBudget(
                    ArchiveV2ComputeBudgetInstructionData::SetComputeUnitLimit(1),
                ),
            }]),
        });
        let message_bytes = serialize(&historical_message);
        let metadata_bytes = serialize(&success_metadata(None, vec![], vec![]));
        let row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        );

        let mut current_builder = IndexBuilder::new(1, 2, 2);
        assert!(
            index_transaction_with_schemas(
                &mut current_builder,
                2,
                CompactV2MessageSchema::Current,
                CompactV2MetadataSchema::LegacyRawError,
                10,
                &row,
                &message_bytes,
                &metadata_bytes,
            )
            .is_err()
        );

        let mut historical_builder = IndexBuilder::new(1, 2, 2);
        index_transaction_with_schemas(
            &mut historical_builder,
            2,
            CompactV2MessageSchema::May24PreUnknownFallbacks,
            CompactV2MetadataSchema::LegacyRawError,
            10,
            &row,
            &message_bytes,
            &metadata_bytes,
        )
        .unwrap();
        assert_eq!(written_programs(&mut historical_builder, 1), vec![2]);
    }

    #[test]
    fn relation_scan_uses_historical_metadata_schema() {
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
        });
        let message_bytes = serialize(&message);
        // This deliberately conflicts with the successful row flag. The
        // large raw-error length is a probe: the legacy grammar skips it,
        // while the typed-error grammar rejects its first byte as an enum tag.
        let metadata_bytes = serialize(&Some(vec![0u8; 255]));
        let row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        );

        let mut legacy_builder = IndexBuilder::new(1, 2, 2);
        let legacy_error = index_transaction_with_schemas(
            &mut legacy_builder,
            2,
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::LegacyRawError,
            10,
            &row,
            &message_bytes,
            &metadata_bytes,
        )
        .unwrap_err()
        .to_string();
        assert!(legacy_error.contains("metadata outcome disagrees"));

        let mut current_builder = IndexBuilder::new(1, 2, 2);
        let current_error = index_transaction_with_schemas(
            &mut current_builder,
            2,
            CompactV2MessageSchema::Current,
            CompactV2MetadataSchema::CurrentTypedError,
            10,
            &row,
            &message_bytes,
            &metadata_bytes,
        )
        .unwrap_err()
        .to_string();
        assert!(current_error.contains("decode metadata outcome"));
    }

    #[test]
    fn dense_pipeline_caps_combined_buffer_capacity() {
        assert!((1..=MAX_SCAN_THREADS).contains(&default_scan_threads()));
        assert_eq!(
            DenseIndexBuildOptions::default().threads,
            default_scan_threads()
        );
        assert!(
            validate_relation_pipeline_buffer(
                6,
                DEFAULT_RELATION_BATCH_PAIRS,
                DEFAULT_QUEUED_RELATION_BATCHES,
            )
            .is_ok()
        );
        assert!(
            validate_relation_pipeline_buffer(
                MAX_SCAN_THREADS,
                MAX_RELATION_BATCH_PAIRS,
                MAX_QUEUED_RELATION_BATCHES,
            )
            .is_err()
        );
        // The direct path allocates no channel/batch pool, so its otherwise
        // inert queue knobs cannot create this aggregate allocation.
        assert!(
            validate_relation_pipeline_buffer(
                1,
                MAX_RELATION_BATCH_PAIRS,
                MAX_QUEUED_RELATION_BATCHES,
            )
            .is_ok()
        );
    }

    #[test]
    fn registry_index_must_match_exact_same_cardinality_registry() {
        let directory = tempfile::tempdir().unwrap();
        let registry_path = directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
        let registry_index_path = directory.path().join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
        let registry_keys = [[1u8; 32], [2u8; 32], [3u8; 32]];
        let stale_keys = [[4u8; 32], [5u8; 32], [6u8; 32]];
        let registry_bytes: Vec<u8> = registry_keys.into_iter().flatten().collect();
        fs::write(&registry_path, registry_bytes).unwrap();
        blockzilla_registry::KeyIndex::build(stale_keys.to_vec())
            .write(&registry_index_path)
            .unwrap();

        let registry_file = File::open(&registry_path).unwrap();
        let key_index = FileBackedKeyIndex::load(&registry_index_path).unwrap();
        let error = validate_registry_index_mapping(
            &registry_file,
            &registry_path,
            &key_index,
            registry_keys.len() as u32,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("does not map registry.bin key 1")
        );
    }

    fn serialize<T: wincode::SchemaWrite<blockzilla_primitives::WincodeLeb128Config, Src = T>>(
        value: &T,
    ) -> Vec<u8> {
        wincode::config::serialize(value, wincode_leb128_config()).unwrap()
    }

    fn success_metadata(
        inner_instructions: Option<Vec<CompactInnerInstructions>>,
        loaded_writable_addresses: Vec<CompactPubkey>,
        loaded_readonly_addresses: Vec<CompactPubkey>,
    ) -> CompactMetaV1 {
        CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions,
            logs: None,
            pre_token_balances: vec![],
            post_token_balances: vec![],
            rewards: vec![],
            loaded_writable_addresses,
            loaded_readonly_addresses,
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        }
    }

    fn row(message: &[u8], metadata: &[u8], flags: u32) -> ArchiveV2HotTxRow {
        ArchiveV2HotTxRow {
            tx_index: 0,
            flags,
            message_offset: 0,
            message_len: message.len() as u32,
            metadata_offset: 0,
            metadata_len: metadata.len() as u32,
            signature_count: 1,
            reserved: [0; 3],
        }
    }

    fn written_programs(builder: &mut IndexBuilder, wallet: u32) -> Vec<u32> {
        let directory = tempfile::tempdir().unwrap();
        let shard = directory.path().join("shard-0");
        builder.write(&shard).unwrap();
        IndexReader::open(&shard).unwrap().query(wallet).unwrap()
    }

    #[test]
    fn successful_transaction_cross_products_all_signers_with_direct_and_cpi_programs() {
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 2,
            },
            account_keys: vec![
                CompactPubkey::Id(1),
                CompactPubkey::Id(2),
                CompactPubkey::Id(3),
                CompactPubkey::Id(4),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            // Neither instruction passes signer 1; the relation is based on
            // signing the successful transaction, not ix-account membership.
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 2,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
        });
        let metadata = success_metadata(
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 3,
                    accounts: vec![],
                    data: vec![],
                    stack_height: Some(2),
                }],
            }]),
            vec![],
            vec![],
        );
        let message_bytes = serialize(&message);
        let metadata_bytes = serialize(&metadata);
        let mut row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        row.signature_count = 2;
        let mut builder = IndexBuilder::new(1, 10, 10);
        index_transaction(&mut builder, 10, 100, &row, &message_bytes, &metadata_bytes).unwrap();

        assert_eq!(written_programs(&mut builder, 1), vec![3, 4]);
        // Rebuild because write borrows the first builder's output directory
        // only for the duration of written_programs and wallet 2 is in it too.
        let mut builder = IndexBuilder::new(1, 10, 10);
        index_transaction(&mut builder, 10, 100, &row, &message_bytes, &metadata_bytes).unwrap();
        assert_eq!(written_programs(&mut builder, 2), vec![3, 4]);
    }

    #[test]
    fn repeated_top_level_program_indexes_stay_fixed_size_and_deduplicated() {
        let mut indexes = ProgramIndexSet::new();
        for _ in 0..1_000_000 {
            indexes.insert(1);
        }
        assert_eq!(indexes.as_slice(), &[1]);
        assert!(std::mem::size_of::<ProgramIndexSet>() <= 1024);

        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: (0..50_000)
                .map(|_| ArchiveV2HotInstruction {
                    program_id_index: 1,
                    accounts: vec![],
                    data: ArchiveV2HotInstructionData::Raw(vec![]),
                })
                .collect(),
        });
        let metadata = success_metadata(None, vec![], vec![]);
        let message_bytes = serialize(&message);
        let metadata_bytes = serialize(&metadata);
        let row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        );
        let mut builder = IndexBuilder::new(1, 2, 2);
        index_transaction(&mut builder, 2, 100, &row, &message_bytes, &metadata_bytes).unwrap();
        assert_eq!(written_programs(&mut builder, 1), vec![2]);
    }

    #[test]
    fn repeated_inner_program_indexes_stay_fixed_size_and_deduplicated() {
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
        });
        let metadata = success_metadata(
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![
                    CompactInnerInstruction {
                        program_id_index: 1,
                        accounts: vec![],
                        data: vec![],
                        stack_height: Some(2),
                    };
                    50_000
                ],
            }]),
            vec![],
            vec![],
        );
        let message_bytes = serialize(&message);
        let metadata_bytes = serialize(&metadata);
        let row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let mut builder = IndexBuilder::new(1, 2, 2);
        index_transaction(&mut builder, 2, 100, &row, &message_bytes, &metadata_bytes).unwrap();
        assert_eq!(written_programs(&mut builder, 1), vec![2]);
    }

    #[test]
    fn v0_loaded_cpi_program_is_resolved_and_indexed() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(4),
                writable_indexes: vec![],
                readonly_indexes: vec![0],
            }],
        });
        let metadata = success_metadata(
            Some(vec![CompactInnerInstructions {
                index: 0,
                instructions: vec![CompactInnerInstruction {
                    program_id_index: 2,
                    accounts: vec![],
                    data: vec![],
                    stack_height: Some(2),
                }],
            }]),
            vec![],
            vec![CompactPubkey::Id(5)],
        );
        let message_bytes = serialize(&message);
        let metadata_bytes = serialize(&metadata);
        let row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
        );
        let mut builder = IndexBuilder::new(1, 10, 10);
        index_transaction(&mut builder, 10, 100, &row, &message_bytes, &metadata_bytes).unwrap();
        assert_eq!(written_programs(&mut builder, 1), vec![2, 5]);
    }

    #[test]
    fn v0_loaded_address_counts_must_match_lookup_classes_exactly() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(4),
                writable_indexes: vec![0],
                readonly_indexes: vec![],
            }],
        });
        // The total loaded count matches, but the writable/readonly classes
        // are swapped. Program-index ordering depends on this distinction.
        let metadata = success_metadata(None, vec![], vec![CompactPubkey::Id(5)]);
        let message_bytes = serialize(&message);
        let metadata_bytes = serialize(&metadata);
        let row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
        );
        let mut builder = IndexBuilder::new(1, 10, 10);

        let error = index_transaction(&mut builder, 10, 100, &row, &message_bytes, &metadata_bytes)
            .unwrap_err();
        assert!(error.to_string().contains("loaded writable addresses"));
    }

    #[test]
    fn inner_instruction_group_must_reference_a_top_level_instruction() {
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
        });
        let metadata = success_metadata(
            Some(vec![CompactInnerInstructions {
                index: 1,
                instructions: vec![],
            }]),
            vec![],
            vec![],
        );
        let message_bytes = serialize(&message);
        let metadata_bytes = serialize(&metadata);
        let row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
        );
        let mut builder = IndexBuilder::new(1, 10, 10);

        assert!(
            index_transaction(&mut builder, 10, 100, &row, &message_bytes, &metadata_bytes,)
                .is_err()
        );
    }

    #[test]
    fn v0_loaded_addresses_cannot_satisfy_the_static_signer_count() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 2,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            // Malformed: only static message keys may be signers. The loaded
            // key below must not be allowed to make this count appear valid.
            account_keys: vec![CompactPubkey::Id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::Id(4),
                writable_indexes: vec![0],
                readonly_indexes: vec![],
            }],
        });
        let metadata = success_metadata(None, vec![CompactPubkey::Id(2)], vec![]);
        let message_bytes = serialize(&message);
        let metadata_bytes = serialize(&metadata);
        let mut row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA
                | ARCHIVE_V2_TX_FLAG_MESSAGE_V0
                | ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES,
        );
        row.signature_count = 2;
        let mut builder = IndexBuilder::new(1, 10, 10);

        let error = index_transaction(&mut builder, 10, 100, &row, &message_bytes, &metadata_bytes)
            .unwrap_err();
        assert!(error.to_string().contains("static account keys"));
    }

    #[test]
    fn row_policy_excludes_failures_before_considering_raw_metadata() {
        let failed_raw_metadata = ArchiveV2HotTxRow {
            flags: ARCHIVE_V2_TX_FLAG_HAS_ERROR | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
            ..row(&[0], &[0], 0)
        };
        assert!(!transaction_is_in_semantic_scope(&failed_raw_metadata).unwrap());

        let successful_raw_metadata = ArchiveV2HotTxRow {
            flags: ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
            ..row(&[0], &[0], 0)
        };
        assert!(transaction_is_in_semantic_scope(&successful_raw_metadata).is_err());

        let raw_transaction = ArchiveV2HotTxRow {
            flags: ARCHIVE_V2_TX_FLAG_HAS_ERROR | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
            ..row(&[0], &[0], 0)
        };
        assert!(transaction_is_in_semantic_scope(&raw_transaction).is_err());
    }

    #[test]
    fn unresolved_required_program_fails_closed() {
        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 1,
            },
            account_keys: vec![CompactPubkey::Id(1), CompactPubkey::Raw([9; 32])],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![],
                data: ArchiveV2HotInstructionData::Raw(vec![]),
            }],
        });
        let metadata = success_metadata(None, vec![], vec![]);
        let message_bytes = serialize(&message);
        let metadata_bytes = serialize(&metadata);
        let row = row(
            &message_bytes,
            &metadata_bytes,
            ARCHIVE_V2_TX_FLAG_HAS_METADATA,
        );
        let mut builder = IndexBuilder::new(1, 10, 10);
        assert!(
            index_transaction(&mut builder, 10, 100, &row, &message_bytes, &metadata_bytes,)
                .is_err()
        );
    }

    #[test]
    fn staging_directory_uses_the_final_paths_real_parent() {
        assert_eq!(output_parent(Path::new("index")), Path::new("."));
        let parent = tempfile::tempdir().unwrap();
        let final_path = parent.path().join("index");
        let staging = create_staging_dir(&final_path).unwrap();
        assert_eq!(staging.parent().unwrap(), parent.path());
        assert!(!final_path.exists());
        fs::remove_dir(staging).unwrap();
    }

    #[test]
    fn atomic_publication_never_replaces_even_a_dangling_destination() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let staging = parent.path().join(".index.staging");
        fs::create_dir(&staging).unwrap();
        fs::write(staging.join("marker"), b"complete").unwrap();
        let final_path = parent.path().join("index");
        symlink("missing-generation", &final_path).unwrap();
        assert!(
            !final_path.exists(),
            "the destination is deliberately dangling"
        );

        assert!(publish_staging_directory(&staging, &final_path).is_err());
        assert!(staging.join("marker").is_file());
        assert_eq!(
            fs::read_link(final_path).unwrap(),
            Path::new("missing-generation")
        );
    }
}
