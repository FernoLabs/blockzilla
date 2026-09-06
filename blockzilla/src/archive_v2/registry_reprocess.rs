//! Generation-safe first-seen to usage-sorted registry migration.
//!
//! The migration intentionally reads an already committed Compact-V2 generation and writes a
//! separate generation.  It never mutates or hard-links the source.  Registry-independent
//! sidecars are reflinked when the host filesystem supports copy-on-write and byte-copied
//! otherwise.

use anyhow::{Context, Result, anyhow, bail, ensure};
use blockzilla_format::{
    ARCHIVE_V2_BLOCK_ACCESS_FILE, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN, ARCHIVE_V2_BLOCK_ACCESS_INDEX_MAGIC,
    ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN, ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES,
    ARCHIVE_V2_BLOCK_INDEX_FILE, ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE, ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE, ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS, ARCHIVE_V2_HOT_TX_ROW_LEN, ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_POH_FILE, ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE, ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE, ARCHIVE_V2_SHREDDING_FILE, ARCHIVE_V2_SIGNATURES_FILE,
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ArchiveV2BlockAccessBlob, ArchiveV2BlockAccessBlockhash, ArchiveV2BlockAccessIndexRow,
    ArchiveV2BlockAccessPubkey, ArchiveV2BlockAccessVoteHash, ArchiveV2GetBlockIndexRow,
    ArchiveV2HotBlockBlob, ArchiveV2HotBlockHeader, ArchiveV2HotMetaRecord, ArchiveV2HotRewards,
    ArchiveV2HotTxRow, ArchiveV2WireFallbackReason, ArchiveV2WireMetadataErrorSchema,
    ArchiveV2WireReferenceClass, ArchiveV2WireRewriteErrorKind, ArchiveV2WireRewriteLimits,
    ArchiveV2WireRewriteStats, ArchiveV2WireRewriteVisitor, BLOCK_TIME_GAP_FILE,
    CompactInnerInstructions, CompactLogStream, CompactMetaV1, CompactPubkey, CompactReturnData,
    CompactReward, CompactShredding, CompactTokenBalance, CompactTransactionError, KeyIndex,
    Leb128, LogEvent, WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
    WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS, WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY,
    WINCODE_ARCHIVE_V2_FLAG_LEB128, WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION, WincodeArchiveV2Footer,
    WincodeArchiveV2Header, WincodeLeb128Config, WincodeLeb128FramedWriter,
    program_logs::{
        ProgramLog,
        system_program::{PubkeyOrString, SystemAddress, SystemProgramLog},
        token_2022::Token2022Log,
    },
    read_archive_v2_block_access_index, read_archive_v2_get_block_index,
    read_archive_v2_hot_block_index, rewrite_archive_v2_metadata_wire, wincode_leb128_config,
    write_archive_v2_block_access_index, write_archive_v2_get_block_index,
    write_archive_v2_hot_block_index,
};
#[cfg(test)]
use blockzilla_format::{
    ArchiveV2ComputeBudgetInstructionData, ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef,
    ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync, CompactMessageHeader,
    OwnedCompactRecentBlockhash,
};
use blockzilla_read_sdk::{
    ArchiveReader, ArchiveV2MessageProjector, ArchiveV2MetadataProfileAdmission,
    ArchiveV2MetadataWireProfile, ArchiveV2WireProfile, HashVerification,
    OpenOptions as ReaderOpenOptions, POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
    PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE, PinnedLocalRangeSource,
    UnprovenWireProfileDecision, audit_full_generation_wire_profile,
    manifest::{GENERATION_MANIFEST_FILE, GenerationManifest, TrustedGenerationIdentity},
    wire_profile_marker, wire_profile_marker_bytes,
};
use memmap2::{Mmap, MmapOptions};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    mem::MaybeUninit,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    sync::mpsc,
    time::{Duration, Instant},
};
use tracing::info;
use wincode::{SchemaRead, SchemaWrite};

struct DiscardSeq<T, Len>(PhantomData<T>, PhantomData<Len>);

struct DiscardBytes<Len>(PhantomData<Len>);

// SAFETY: the implementation uses the declared element and length schemas for every byte. It
// initializes the destination only after all elements have been validated and consumed.
unsafe impl<'de, C, T, Len> SchemaRead<'de, C> for DiscardSeq<T, Len>
where
    C: wincode::config::ConfigCore,
    Len: wincode::len::SeqLen<C>,
    T: SchemaRead<'de, C>,
{
    type Dst = Vec<T::Dst>;

    fn read(
        mut reader: impl wincode::io::Reader<'de>,
        dst: &mut MaybeUninit<Self::Dst>,
    ) -> wincode::ReadResult<()> {
        let len = Len::read_prealloc_check::<T::Dst>(reader.by_ref())?;
        for _ in 0..len {
            T::get(reader.by_ref())?;
        }
        dst.write(Vec::new());
        Ok(())
    }
}

// SAFETY: the length schema applies the configured allocation bound before the reader advances.
// The destination is initialized only after the complete byte region is present.
unsafe impl<'de, C, Len> SchemaRead<'de, C> for DiscardBytes<Len>
where
    C: wincode::config::ConfigCore,
    Len: wincode::len::SeqLen<C>,
{
    type Dst = Vec<u8>;

    fn read(
        mut reader: impl wincode::io::Reader<'de>,
        dst: &mut MaybeUninit<Self::Dst>,
    ) -> wincode::ReadResult<()> {
        let len = Len::read_prealloc_check::<u8>(reader.by_ref())?;
        reader.take_scoped(len)?;
        dst.write(Vec::new());
        Ok(())
    }
}

use crate::ProgressTracker;

fn bounded_wincode_config<const LIMIT: usize>() -> impl wincode::config::Config {
    wincode::config::Configuration::default()
        .with_preallocation_size_limit::<LIMIT>()
        .with_int_encoding::<Leb128>()
}

struct BoundedVecWriter<'a> {
    output: &'a mut Vec<u8>,
    max_len: usize,
}

impl wincode::io::Writer for BoundedVecWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> wincode::io::WriteResult<()> {
        let next_len = self
            .output
            .len()
            .checked_add(bytes.len())
            .ok_or(wincode::io::WriteError::WriteSizeLimit(usize::MAX))?;
        if next_len > self.max_len {
            return Err(wincode::io::WriteError::WriteSizeLimit(next_len));
        }
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

fn serialize_bounded_into<T>(output: &mut Vec<u8>, value: &T, max_len: usize) -> Result<usize>
where
    T: SchemaWrite<WincodeLeb128Config, Src = T> + ?Sized,
{
    let start = output.len();
    ensure!(
        start <= max_len,
        "bounded wincode output already contains {start} bytes, exceeding {max_len}"
    );
    wincode::config::serialize_into(
        BoundedVecWriter { output, max_len },
        value,
        wincode_leb128_config(),
    )?;
    let written = output
        .len()
        .checked_sub(start)
        .context("bounded wincode output length underflow")?;
    ensure!(
        output.len() <= max_len,
        "bounded wincode output exceeded {max_len} bytes"
    );
    Ok(written)
}

fn reserve_byte_capacity(output: &mut Vec<u8>, required: usize, label: &'static str) -> Result<()> {
    if output.capacity() < required {
        output
            .try_reserve_exact(required.saturating_sub(output.len()))
            .with_context(|| format!("reserve {required} bytes for {label}"))?;
    }
    Ok(())
}

fn decompress_zstd_reused(
    decompressor: &mut zstd::bulk::Decompressor<'static>,
    source: &[u8],
    expected_len: usize,
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    reserve_byte_capacity(output, expected_len, "registry rewrite zstd decode")?;
    let written = decompressor
        .decompress_to_buffer(source, output)
        .context("zstd decompress registry rewrite block")?;
    ensure!(
        written == expected_len && output.len() == expected_len,
        "zstd decoded length mismatch: returned={written} buffer={} expected={expected_len}",
        output.len()
    );
    Ok(())
}

fn compress_zstd_reused(
    compressor: &mut zstd::bulk::Compressor<'static>,
    source: &[u8],
    output: &mut Vec<u8>,
) -> Result<()> {
    output.clear();
    let bound = zstd::zstd_safe::compress_bound(source.len());
    reserve_byte_capacity(output, bound, "registry rewrite zstd encode")?;
    let written = compressor
        .compress_to_buffer(source, output)
        .context("zstd compress registry rewrite block")?;
    ensure!(
        written == output.len(),
        "zstd encoded length mismatch: returned={written} buffer={}",
        output.len()
    );
    Ok(())
}

struct RegistryRewriteWorkerScratch {
    level: i32,
    decompressor: zstd::bulk::Decompressor<'static>,
    compressor: zstd::bulk::Compressor<'static>,
    decoded: Vec<u8>,
    encoded: Vec<u8>,
    compressed: Vec<u8>,
    rows: Vec<ArchiveV2HotTxRow>,
    target_messages: Vec<u8>,
    target_metadata: Vec<u8>,
}

impl RegistryRewriteWorkerScratch {
    fn new(level: i32) -> Result<Self> {
        Ok(Self {
            level,
            decompressor: zstd::bulk::Decompressor::new()
                .context("create registry rewrite zstd decompressor")?,
            compressor: zstd::bulk::Compressor::new(level)
                .context("create registry rewrite zstd compressor")?,
            decoded: Vec::new(),
            encoded: Vec::new(),
            compressed: Vec::new(),
            rows: Vec::new(),
            target_messages: Vec::new(),
            target_metadata: Vec::new(),
        })
    }

    fn set_level(&mut self, level: i32) -> Result<()> {
        if self.level != level {
            self.compressor = zstd::bulk::Compressor::new(level)
                .context("recreate registry rewrite zstd compressor")?;
            self.level = level;
        }
        Ok(())
    }

    fn retained_vector_bytes(&self) -> usize {
        let byte_vectors = [
            self.decoded.capacity(),
            self.encoded.capacity(),
            self.compressed.capacity(),
            self.target_messages.capacity(),
            self.target_metadata.capacity(),
        ]
        .into_iter()
        .fold(0usize, usize::saturating_add);
        byte_vectors.saturating_add(
            self.rows
                .capacity()
                .saturating_mul(std::mem::size_of::<ArchiveV2HotTxRow>()),
        )
    }

    /// Clear normal worker buffers for reuse. An outlier drops every retained vector together so
    /// the post-completion allocation is again below the precharged eight-MiB cap.
    fn finish_block(&mut self) -> bool {
        self.decoded.clear();
        self.encoded.clear();
        self.compressed.clear();
        self.rows.clear();
        self.target_messages.clear();
        self.target_metadata.clear();
        if self.retained_vector_bytes() <= REGISTRY_REWRITE_WORKER_RETAINED_VECTOR_BYTES {
            return false;
        }
        self.decoded = Vec::new();
        self.encoded = Vec::new();
        self.compressed = Vec::new();
        self.rows = Vec::new();
        self.target_messages = Vec::new();
        self.target_metadata = Vec::new();
        true
    }
}

thread_local! {
    static REGISTRY_REWRITE_WORKER_SCRATCH: RefCell<Option<RegistryRewriteWorkerScratch>> =
        const { RefCell::new(None) };
}

fn with_registry_rewrite_worker_scratch<T>(
    level: i32,
    work: impl FnOnce(&mut RegistryRewriteWorkerScratch) -> Result<T>,
) -> Result<T> {
    REGISTRY_REWRITE_WORKER_SCRATCH.with(|slot| {
        let mut slot = slot
            .try_borrow_mut()
            .map_err(|_| anyhow!("registry rewrite worker scratch was used recursively"))?;
        if slot.is_none() {
            *slot = Some(RegistryRewriteWorkerScratch::new(level)?);
        }
        let scratch = slot.as_mut().expect("registry rewrite scratch initialized");
        scratch.set_level(level)?;
        let result = work(scratch);
        scratch.finish_block();
        result
    })
}

pub(crate) const REGISTRY_REPROCESS_RECEIPT_FILE: &str =
    "archive-v2-registry-reprocess.receipt.json";
const REGISTRY_REPROCESS_RECEIPT_TEMP_FILE: &str =
    ".archive-v2-registry-reprocess.receipt.json.registry-access.tmp";
const RECEIPT_VERSION_V1: u32 = 1;
const RECEIPT_VERSION_V2: u32 = 2;
const RECEIPT_VERSION: u32 = 3;
const RECEIPT_ALGORITHM_V1: &str = "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v1";
const RECEIPT_ALGORITHM_V2: &str = "compact_v2_first_seen_v1_to_usage_sorted_historical_car_v2";
const RECEIPT_ALGORITHM: &str = "compact_v2_first_seen_v1_to_usage_sorted_staged_access_v3";
const ACCESS_ASSEMBLY_MODE: &str = "source_access_wire_remap_v1";
const SIGNATURE_PROVENANCE: &str = "source_access_duplicate_v1";
// One-time repair for the exact legacy epoch-301 source boundary diagnosed on the NAS.  Keep
// these values pinned: this is not a general "repair bad access" escape hatch.
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_MODE: &str =
    "epoch_301_legacy_row_0_self_previous_blockhash_v1";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH: u64 = 301;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID: u32 = 0;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT: u64 = 130_032_004;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_PREDECESSOR_SLOT: u64 = 130_031_999;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX: &str =
    "0c0640dd3fa01691bf9e820266f68353bafc907388a552cee7e0cf669ad3ab18";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX: &str =
    "df7b307732e1422211ca43fee78b99d97ff97e3db0c5fce6ba00c1dd19f9ad95";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_BYTES: u64 = 12_000;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_ROWS: usize = 300;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_SHA256: &str =
    "1378ba987517b633440f21ef9e7a7d0930dc815728c1546dc7b541a4fca6e47c";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_MANIFEST_SHA256: &str =
    "7ec2c39e8841f479f9112acf41139cf3bb71f50c123018ff7ee4e07580e35b5b";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_BYTES: u64 = 95_862_882_123;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_SHA256: &str =
    "a7ccc5852dae14df85553b4e8403bcbb47afab2e6df3b01411ecf36bdc8042e3";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_BYTES: u64 = 12_738_912;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_ROWS: usize = 398_090;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_SHA256: &str =
    "6229578531aa6eb4391dd328d9b1649e67b0982793da237747b9465ab595c24b";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_0_BYTES: u32 = 15_360_984;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_0_SHA256: &str =
    "b0564e9f8d96eaaa59014be63c710166519ad4987e7a1e935a9f07c4f4bad276";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCK_ID: u32 = 1;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_SLOT: u64 = 130_032_005;
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCKHASH_HEX: &str =
    "06280ad0c148ad65d38fb9da82d7660b8955f4d060e578b8c37b7e0f29646f07";
const EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_SHA256: &str =
    "202a111337919fc549edd1705e2b299331cc833132bd91867ae1131378b8f9b5";
const RECEIPT_MAX_BYTES: u64 = 8 << 20;
const MANIFEST_MAX_BYTES: u64 = 64 << 10;
const GENERATION_MANIFEST_MAX_BYTES: u64 = 4 << 20;
const MAX_PROFILE_MESSAGE_BYTES: usize = 16 << 20;
const MAX_HOT_BLOCK_FRAME_BYTES: u64 = 512 << 20;
const MAX_HOT_BLOCK_FRAME_BYTES_USIZE: usize = MAX_HOT_BLOCK_FRAME_BYTES as usize;
const PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES: usize = 16 << 20;
const PASS1_REWARDS_RETAINED_SEQUENCE_MAX_BYTES: usize = 64 << 20;
// Limits aggregate advertised input plus decompressed bytes admitted to one parallel batch.
const HOT_BATCH_MEMORY_BUDGET_BYTES: u64 = 512 << 20;
const MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK: usize = 4 << 20;
const HOT_UNCOMPRESSED_WORKING_SET_MULTIPLIER: u64 = 5;
// Worker-local vectors remain alive after a completed item leaves the ordered pipeline. Charge
// their maximum retained capacity, plus a conservative allowance for both native zstd contexts,
// before admitting any pass-2 work.
const REGISTRY_REWRITE_WORKER_RETAINED_VECTOR_BYTES: usize = 8 << 20;
const REGISTRY_REWRITE_WORKER_ZSTD_CONTEXT_BYTES: u64 = 4 << 20;
const REGISTRY_REWRITE_WORKER_PERSISTENT_BYTES: u64 = REGISTRY_REWRITE_WORKER_RETAINED_VECTOR_BYTES
    as u64
    + REGISTRY_REWRITE_WORKER_ZSTD_CONTEXT_BYTES;
// Covers the completed-result value plus the sequence key and allocator/tree bookkeeping kept by
// the ordered reorder map. Vector backing allocations are charged separately at their capacity.
const ORDERED_PIPELINE_RESULT_OVERHEAD_BYTES: u64 = 512;
#[allow(dead_code)] // Retained for compatibility decoder tests and forensic tooling.
const MAX_ACCESS_FRAME_BYTES_USIZE: usize = ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES as usize;
const MAX_META_FRAME_BYTES: usize = 64 << 20;
const IO_BUFFER_BYTES: usize = 8 << 20;
const SORT_RECORD_BYTES: usize = 40;
const SORT_RUN_MAGIC: &[u8; 8] = b"BZRSRUN1";
const SEMANTIC_DOMAIN: &[u8] = b"blockzilla.registry-reprocess.semantic.v1";
const GENERATION_DOMAIN: &[u8] = b"blockzilla.registry-reprocess.generation.v1";
const REPROCESS_CHECKPOINT_FILE: &str = ".archive-v2-registry-reprocess.checkpoint.json";
const REPROCESS_HANDOFF_FILE: &str = ".archive-v2-registry-reprocess.handoff.json";
const REPROCESS_REMAP_FILE: &str = ".archive-v2-registry-remap.u32";
const SOURCE_REGISTRY_SNAPSHOT_FILE: &str = ".source-registry.snapshot";
const SOURCE_ANCHOR_FILES: [&str; 7] = [
    ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    ARCHIVE_V2_META_FILE,
    ARCHIVE_V2_BLOCK_INDEX_FILE,
    ARCHIVE_V2_BLOCKS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
    ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
];
const REPROCESS_CHECKPOINT_VERSION: u32 = 3;
const REPROCESS_HANDOFF_VERSION: u32 = 3;
const REPROCESS_CORE_RESULT_VERSION: u32 = 2;
const REPROCESS_REMAP_VERSION: u32 = 1;
const REPROCESS_REMAP_MAGIC: &[u8; 8] = b"BZRRMAP1";
const REPROCESS_REMAP_HEADER_BYTES: u64 = 32;
const CORE_COMPLETE_STATE: &str = "block_access_rebuild_required";
const ACCESS_TEMP_SUFFIX: &str = ".registry-access.tmp";
#[cfg(unix)]
const PUBLISHED_GENERATION_MODE: u32 = 0o755;
const PHASE_TIMING_ENV: &str = "BLOCKZILLA_REGISTRY_REPROCESS_PHASE_TIMING";

// One marker-free production cohort has a reviewed, generation-bound recovery receipt. Keep the
// child admission boundary closed over the same exact inventory as the scheduler. A new cohort
// needs a new reviewed authority entry; a CLI-selected profile is never authority by itself.
const PROFILE_NEUTRAL_RECOVERY_INCIDENT_ID: &str =
    "profile-neutral-registry-reprocess-post-rebuild-2026-08-14-v1";
const PROFILE_NEUTRAL_RECOVERY_AUTHORITY_SHA256: &str =
    "f471bb2078e719da508c4a8d22980a59e7d99140fe0682289bacb401ea10b5cf";
const PROFILE_NEUTRAL_RECOVERY_AUTHORITY_DOMAIN: &[u8] =
    b"blockzilla.registry-reprocess.profile-neutral-rebuild-authority.v1\0";
const PROFILE_NEUTRAL_RECOVERY_RECEIPT_SCHEMA_VERSION: u32 = 2;
const PROFILE_NEUTRAL_RECOVERY_RECEIPT_KIND: &str = "archive_v2_profile_neutral_registry_rebuild";
const PROFILE_NEUTRAL_RECOVERY_ATTESTATION_KIND: &str = "archive_v2_wire_profile_attestation";
const PROFILE_NEUTRAL_RECOVERY_ATTESTATION_ALGORITHM: &str =
    "archive-v2-borrowed-dual-profile-full-generation-v2";
const PROFILE_NEUTRAL_RECOVERY_ATTESTATION_GENERATION_KIND: &str =
    "registry-receipt-source-files-v1";
const PROFILE_NEUTRAL_RECOVERY_EVIDENCE_V3: &str = "full-generation-borrowed-dual-sdk-audit-v3";
const MAX_PROFILE_AUTHORITY_BYTES: u64 = 64 << 10;
const MAY_24_2026_MAINNET_EPOCH_1_BLOCKS_SHA256: &str =
    "3f02379494439c87c70cfd9ab1a6bbdd30c296b29dbd2b13cf6c609f7cda925d";
const MAY_24_2026_MAINNET_EPOCH_1_INDEX_SHA256: &str =
    "5c663da2dd58f3bc6acfce90dd42ba63224f20f224bb78de7145d495c571db58";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProfileNeutralRecoveryAuthority {
    epoch: u64,
    receipt_version: u32,
    receipt_sha256: &'static str,
    source_generation_sha256: &'static str,
    target_generation_sha256: &'static str,
}

const PROFILE_NEUTRAL_RECOVERY_AUTHORITIES: [ProfileNeutralRecoveryAuthority; 11] = [
    ProfileNeutralRecoveryAuthority {
        epoch: 305,
        receipt_version: 1,
        receipt_sha256: "c674f51fad3e22e185c17c76c733de8e3ad88c71a328ce7b2925aa45f90e9b17",
        source_generation_sha256: "5ee2077966ae54f3e42424597c5d779baba1aa99f9c36cd6b78fef6dbbe79a6f",
        target_generation_sha256: "fca639213c23226516d24b1f3fbb789af23793fc584f277721799e51217cc924",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 404,
        receipt_version: 2,
        receipt_sha256: "299cf01de99f06738f30413c6e24b5322ada7d21b9ae866073ab381a6cc39d14",
        source_generation_sha256: "d5e90c4096b7d73911740ed7d72f2223288a7b28de46b92e6dd2a8674331d376",
        target_generation_sha256: "f37af441ff02ea9a5f0ba3a5dddfa3268bf429a92e3f76b5033ff2815aab09b7",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 405,
        receipt_version: 2,
        receipt_sha256: "988b0875b3d347627efd187f8788337a3403575dfb82843c19a1fed3b589d886",
        source_generation_sha256: "f229e57ee373feb5f9b73101d59f382e397ac9f85cbc9feb6ff5ccd38a876c9c",
        target_generation_sha256: "5269f4218827ec0bcd22987c1ce7ad3993b4514f50cb1dcaf157314b7e319338",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 501,
        receipt_version: 2,
        receipt_sha256: "834b821bae74e79dcaa372654b4453263ee5658f6d3321c4078337b460e597e9",
        source_generation_sha256: "995a22303a86d603c3c4510e8901578187b4cb0dc521092056f776da6cddf425",
        target_generation_sha256: "2ebee8b7abe7288d2d670acf75caada3a51f977d0c929afc83bb9d21619379b1",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 502,
        receipt_version: 2,
        receipt_sha256: "d59f3c4a38dd2a7a3cdd60f7a24fb278899ef3a469351661b11d5910e0d7e11e",
        source_generation_sha256: "fffea7443758dc75d5fe2107ab726c246857fd29ec2d40800e6290df239dfe28",
        target_generation_sha256: "61419ca6389ccd1a68d63af5bf94227c6f92f7de6c6b07b079f4fde9560757ac",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 503,
        receipt_version: 2,
        receipt_sha256: "a3b4b192de9656b3fa16aae98ebf4984f3e04e3b29f0d7283e8ee2581bb9dbe2",
        source_generation_sha256: "6df2e91dd0c9b39d1f1748c077da5e7b4547cd33d420fc15990cbb58a61fde5e",
        target_generation_sha256: "d9f228f74177594b3e3e00aa968a36a6ecba89bff28fa8e7f70118c55ba0eeef",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 504,
        receipt_version: 2,
        receipt_sha256: "57bdd578f5a16bc82aaff73fb5cd93cd651533726732efa7680d27cacf345493",
        source_generation_sha256: "e8968325694f00973b8a696640b292a0b449c2a953b1abf8ed8c363e780d722c",
        target_generation_sha256: "c598e7d3d248f8ebb99792526f8bcbe9dae85966eeee6c49656a64487dfede3d",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 505,
        receipt_version: 2,
        receipt_sha256: "b60707aeea95ec25471febbed081b4324021cdb4c77f7642b96fc75df192b2a5",
        source_generation_sha256: "140296e509f4a86e7e93f2764198beb6f19aa69a50addd19f046872c5d311195",
        target_generation_sha256: "acc537045772da389cb6f2879c72ee72db88fb0394a1445500c9ecd07be3d59f",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 864,
        receipt_version: 2,
        receipt_sha256: "4909fb75938fc5faaa17a92529fa679172cbfabde13d76bf096e02d52242c62c",
        source_generation_sha256: "dd6d0f77f2463be40b9c5659831a07bd6af929392048ec1dfdac7ee8fc385067",
        target_generation_sha256: "a36dbef4ecf6872e142c264827c9344648949cd166f21b528a0239695b90105e",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 997,
        receipt_version: 1,
        receipt_sha256: "ede95aa5c868e5d2c502e97808c6eb2163c0d0b7d90a2f9471f864430a4a6f9a",
        source_generation_sha256: "b7d216975d136e9e0c23421487b60147f63204053f81eb82aeef2d446bc05522",
        target_generation_sha256: "6874518ff2c9da93b289d3d27cf1c9ae2c7b30c763e71cabdca5eb4ee8d6bdca",
    },
    ProfileNeutralRecoveryAuthority {
        epoch: 1000,
        receipt_version: 2,
        receipt_sha256: "5e5e7f7ea2ca3dbd3d14dc2379fd8618c9b6ac714c8fd87bf41b3a9e6517fa04",
        source_generation_sha256: "c7b8d45a9d89b984ffc89f9e82901cd2590e66697043cba153169a41a71f76d4",
        target_generation_sha256: "5711922e7651eaecbd227baf5a53d764fb9f707b762701611bbf91073bc9e197",
    },
];

/// Options for one immutable-generation registry migration.
#[derive(Debug, Clone)]
pub(crate) struct RegistryReprocessOptions {
    pub(crate) source_dir: PathBuf,
    pub(crate) target_dir: PathBuf,
    pub(crate) epoch: u64,
    pub(crate) threads: usize,
    /// Hard cap for the external sort's auxiliary in-memory record chunk.  This does not include
    /// the required O(source keys) count/remap vector or bounded block worker buffers.
    pub(crate) sort_memory_mib: usize,
    pub(crate) level: i32,
    pub(crate) attempt_id: String,
    pub(crate) staging_dir: PathBuf,
    /// One authoritative message grammar for the complete source and target generation.
    pub(crate) wire_profile: ArchiveV2WireProfile,
    /// Exact reviewed recovery receipt for a marker-free source. Marker-bearing sources must omit
    /// it. The selected profile alone is never sufficient authority.
    pub(crate) wire_profile_authority_receipt: Option<PathBuf>,
}

/// Options for the access-only completion phase of a staged registry migration.
#[derive(Debug, Clone)]
pub(crate) struct RegistryReprocessAccessOptions {
    pub(crate) source_dir: PathBuf,
    pub(crate) staging_dir: PathBuf,
    pub(crate) target_dir: PathBuf,
    pub(crate) epoch: u64,
    pub(crate) attempt_id: String,
    pub(crate) handoff_sha256: String,
    pub(crate) expected_continuation_state: RegistryReprocessAccessContinuationState,
    pub(crate) wire_profile: ArchiveV2WireProfile,
    pub(crate) wire_profile_authority_receipt: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileBinding {
    pub(crate) bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileAuthorityFileIdentity {
    size: u64,
    device: u64,
    inode: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SourceWireProfileAuthority {
    PublishedManifest {
        manifest_file: FileBinding,
        manifest: GenerationManifest,
    },
    PinnedHistoricalIdentity {
        epoch: u64,
        blocks: FileBinding,
        block_index: FileBinding,
    },
    ProfileNeutralRecoveryReceipt {
        path: PathBuf,
        receipt: FileBinding,
        identity: ProfileAuthorityFileIdentity,
        source_generation_sha256: String,
        legacy_source_files: BTreeMap<String, FileBinding>,
    },
}

#[derive(Debug, Clone)]
enum MarkerFreeSourceAuthorityEvidence {
    PinnedHistoricalIdentity,
    ProfileNeutralRecoveryReceipt {
        source_file_identities: BTreeMap<String, ProfileAuthorityFileIdentity>,
    },
}

#[derive(Debug, Clone)]
struct ResolvedSourceWireProfileAuthority {
    authority: SourceWireProfileAuthority,
    marker_free_evidence: Option<MarkerFreeSourceAuthorityEvidence>,
}

#[derive(Debug, Clone)]
struct ValidatedSourceWireProfileAuthority {
    authority: SourceWireProfileAuthority,
    /// The descriptor view used by the full SDK audit. Initial checkpoint construction must use
    /// this same view so a pathname swap cannot join authority from one generation to semantics
    /// or a source anchor from another generation.
    audited_marker_free_source: Option<PinnedLocalRangeSource>,
}

impl SourceWireProfileAuthority {
    fn recovery_receipt_path(&self) -> Option<&Path> {
        match self {
            Self::ProfileNeutralRecoveryReceipt { path, .. } => Some(path),
            Self::PublishedManifest { .. } | Self::PinnedHistoricalIdentity { .. } => None,
        }
    }

    fn expected_source_binding(&self, name: &str) -> Option<&FileBinding> {
        match self {
            Self::PinnedHistoricalIdentity {
                blocks,
                block_index,
                ..
            } => match name {
                ARCHIVE_V2_BLOCKS_FILE => Some(blocks),
                ARCHIVE_V2_BLOCK_INDEX_FILE => Some(block_index),
                _ => None,
            },
            Self::ProfileNeutralRecoveryReceipt {
                legacy_source_files,
                ..
            } => legacy_source_files.get(name),
            Self::PublishedManifest { .. } => None,
        }
    }
}

fn validate_source_binding_against_profile_authority(
    authority: &SourceWireProfileAuthority,
    name: &str,
    actual: &FileBinding,
) -> Result<()> {
    if let SourceWireProfileAuthority::PublishedManifest { manifest, .. } = authority
        && let Some(expected) = manifest.file(name)
    {
        ensure!(
            actual.bytes == expected.size && actual.sha256 == expected.sha256,
            "source artifact {name} differs from its published generation manifest binding"
        );
        return Ok(());
    }
    if let Some(expected) = authority.expected_source_binding(name) {
        ensure!(
            actual == expected,
            "source artifact {name} differs from its wire-profile authority binding"
        );
    }
    Ok(())
}

fn validate_source_wire_profile_authority_shape(
    epoch: u64,
    profile: ArchiveV2WireProfile,
    authority: &SourceWireProfileAuthority,
) -> Result<()> {
    match authority {
        SourceWireProfileAuthority::PublishedManifest {
            manifest_file,
            manifest,
        } => {
            manifest
                .validate()
                .map_err(|error| anyhow!(error))
                .context("validate published generation manifest authority")?;
            ensure!(
                manifest.complete && manifest.epoch == epoch,
                "published generation manifest does not authorize this epoch"
            );
            let manifest_profile = ArchiveV2WireProfile::for_published_manifest(manifest)
                .map_err(|error| anyhow!(error))
                .context("resolve published generation wire profile")?;
            ensure!(
                manifest_profile == profile,
                "published generation manifest does not authorize the selected wire profile"
            );
            for required in [
                ARCHIVE_V2_BLOCKS_FILE,
                ARCHIVE_V2_BLOCK_INDEX_FILE,
                ARCHIVE_V2_META_FILE,
                ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ] {
                manifest
                    .required_file(required)
                    .map_err(|error| anyhow!(error))
                    .with_context(|| {
                        format!("published generation manifest omits required file {required}")
                    })?;
            }
            let expected = wire_profile_marker(profile);
            let marker = manifest
                .required_file(&expected.name)
                .map_err(|error| anyhow!(error))
                .context("published generation manifest omits selected wire-profile marker")?;
            ensure!(
                marker.size == expected.size && marker.sha256 == expected.sha256,
                "published generation marker binding differs from the selected wire profile"
            );
            ensure!(
                manifest_file.bytes > 0 && manifest_file.bytes <= GENERATION_MANIFEST_MAX_BYTES,
                "published generation manifest file binding has an invalid size"
            );
            validate_hex_sha256(
                &manifest_file.sha256,
                "published generation manifest file digest",
            )?;
        }
        SourceWireProfileAuthority::PinnedHistoricalIdentity {
            epoch: identity_epoch,
            blocks,
            block_index,
        } => ensure!(
            epoch == 1
                && *identity_epoch == epoch
                && profile == ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
                && blocks.sha256 == MAY_24_2026_MAINNET_EPOCH_1_BLOCKS_SHA256
                && block_index.sha256 == MAY_24_2026_MAINNET_EPOCH_1_INDEX_SHA256,
            "pinned historical identity does not authorize this source profile"
        ),
        SourceWireProfileAuthority::ProfileNeutralRecoveryReceipt {
            path,
            receipt,
            identity,
            source_generation_sha256,
            legacy_source_files,
        } => {
            ensure!(
                profile == ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
                    && path.is_absolute()
                    && receipt.bytes > 0
                    && receipt.bytes <= MAX_PROFILE_AUTHORITY_BYTES
                    && receipt.bytes == identity.size
                    && !legacy_source_files.is_empty()
                    && legacy_source_files.contains_key(ARCHIVE_V2_BLOCKS_FILE),
                "profile-neutral recovery authority shape is invalid"
            );
            validate_hex_sha256(&receipt.sha256, "profile authority receipt digest")?;
            validate_hex_sha256(
                source_generation_sha256,
                "profile authority source generation digest",
            )?;
            validate_profile_authority_identity(identity)?;
            for (name, binding) in legacy_source_files {
                ensure!(
                    !name.is_empty() && Path::new(name).components().count() == 1,
                    "profile authority source file name is invalid"
                );
                validate_hex_sha256(&binding.sha256, "profile authority source file digest")?;
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileNeutralRecoveryReceipt {
    schema_version: u32,
    kind: String,
    incident_id: String,
    authority_sha256: String,
    epoch: u64,
    wire_profile: ArchiveV2WireProfile,
    legacy_receipt_version: u32,
    legacy_receipt_path: PathBuf,
    legacy_receipt_sha256: String,
    legacy_receipt_identity: ProfileAuthorityFileIdentity,
    legacy_generation_device: u64,
    legacy_generation_inode: u64,
    legacy_target_file_identities: BTreeMap<String, ProfileAuthorityFileIdentity>,
    recovery_threads: usize,
    source_generation_sha256: String,
    target_generation_sha256: String,
    source_attestation_path: PathBuf,
    source_attestation_sha256: String,
    source_attestation_identity: ProfileAuthorityFileIdentity,
    original_marker_path: PathBuf,
    original_marker_sha256: String,
    quarantine: PathBuf,
    archived_marker: PathBuf,
    archived_marker_identity: ProfileAuthorityFileIdentity,
    created_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileNeutralSourceAttestation {
    schema_version: u32,
    kind: String,
    audit_algorithm: String,
    audited_profiles: [ArchiveV2WireProfile; 2],
    cluster_id: String,
    epoch: u64,
    archive: PathBuf,
    registry_order: String,
    generation_kind: String,
    content_generation_sha256: String,
    archive_files: BTreeMap<String, ProfileAuthorityFileIdentity>,
    wire_profile: ArchiveV2WireProfile,
    evidence: String,
    attested_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SemanticBinding {
    pub(crate) blocks: u64,
    pub(crate) transactions: u64,
    pub(crate) pubkey_references: u64,
    pub(crate) reference_sha256: String,
    pub(crate) normalized_structure_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RewriteStats {
    pub(crate) blocks: u64,
    pub(crate) transactions: u64,
    pub(crate) pubkey_references: u64,
}

/// Durable provenance for the single allowed legacy block-access boundary correction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AccessBoundaryRepair {
    pub(crate) mode: String,
    pub(crate) block_id: u32,
    pub(crate) block_slot: u64,
    pub(crate) trusted_predecessor_slot: u64,
    pub(crate) original_previous_blockhash_hex: String,
    pub(crate) corrected_previous_blockhash_hex: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Epoch301AccessBoundaryEvidence {
    tail_binding: FileBinding,
    tail_rows: usize,
    manifest_sha256: String,
    source_blob_bytes: u64,
    source_index_binding: FileBinding,
    source_index_rows: usize,
    source_index_blob_bytes: u64,
    row_0_access_len: u32,
    row_0_frame_sha256: String,
    row_1_block_id: u32,
    row_1_slot: u64,
    row_1_previous_blockhash_hex: String,
    row_1_blockhash_hex: String,
    row_1_frame_sha256: String,
    first_hot_parent_slot: u64,
}

/// Publication-last receipt binding the exact rewrite inputs and complete target generation.
/// Version 1 also carries the retired rewrite-time semantic digests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegistryReprocessReceipt {
    pub(crate) version: u32,
    pub(crate) algorithm: String,
    pub(crate) epoch: u64,
    pub(crate) threads: usize,
    pub(crate) sort_memory_mib: usize,
    pub(crate) level: i32,
    pub(crate) source_anchor_sha256: String,
    pub(crate) source_dir: String,
    pub(crate) target_dir: String,
    pub(crate) source_generation_sha256: String,
    pub(crate) target_generation_sha256: String,
    pub(crate) source_files: BTreeMap<String, FileBinding>,
    pub(crate) target_files: BTreeMap<String, FileBinding>,
    pub(crate) source_registry_keys: u64,
    pub(crate) target_registry_keys: u64,
    pub(crate) eligible_references: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) source_semantics: Option<SemanticBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) target_semantics: Option<SemanticBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) rewrite_stats: Option<RewriteStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) attempt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) handoff_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) assembly_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) signature_provenance: Option<String>,
    /// Present for profile-bound v3 rewrites. Legacy accepted receipts omit this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) wire_profile: Option<ArchiveV2WireProfile>,
    // Normal receipts omit this field and remain byte-compatible with the original v3 schema.
    // A repaired receipt intentionally requires a reader that knows this provenance object;
    // older deny-unknown-fields readers cannot safely validate that exceptional generation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) access_boundary_repair: Option<AccessBoundaryRepair>,
}

/// Machine-readable result from the registry-only phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegistryReprocessCoreResult {
    pub(crate) version: u32,
    pub(crate) state: String,
    pub(crate) attempt_id: String,
    pub(crate) epoch: u64,
    pub(crate) source_dir: String,
    pub(crate) target_dir: String,
    pub(crate) staging_dir: String,
    pub(crate) handoff_sha256: String,
    pub(crate) wire_profile: ArchiveV2WireProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegistryReprocessAccessContinuationState {
    ReceiptReady,
    CoreOrPartialRebuild,
}

impl RegistryReprocessAccessContinuationState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ReceiptReady => "receipt-ready",
            Self::CoreOrPartialRebuild => "core-or-partial-rebuild",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegistryReprocessAccessContinuationProbe {
    pub(crate) state: RegistryReprocessAccessContinuationState,
    pub(crate) core_result: RegistryReprocessCoreResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReprocessCheckpoint {
    version: u32,
    algorithm: String,
    source_dir: String,
    target_dir: String,
    epoch: u64,
    threads: usize,
    sort_memory_mib: usize,
    level: i32,
    source_anchor_sha256: String,
    attempt_id: String,
    staging_dir: String,
    wire_profile: ArchiveV2WireProfile,
    wire_profile_authority: SourceWireProfileAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryReprocessHandoff {
    version: u32,
    state: String,
    attempt_id: String,
    epoch: u64,
    source_dir: String,
    target_dir: String,
    staging_dir: String,
    threads: usize,
    sort_memory_mib: usize,
    level: i32,
    source_anchor_sha256: String,
    source_registry_keys: u64,
    target_registry_keys: u64,
    eligible_references: u64,
    rewrite_stats: RewriteStats,
    source_blocks: FileBinding,
    core_files: BTreeMap<String, FileBinding>,
    source_registry_snapshot: FileBinding,
    remap_file: FileBinding,
    wire_profile: ArchiveV2WireProfile,
    wire_profile_authority: SourceWireProfileAuthority,
}

#[derive(Debug)]
struct SourceManifest {
    registry_keys: u64,
    references: u64,
}

struct MappedRegistry {
    _file: File,
    mmap: Mmap,
    len: usize,
}

struct MappedRegistryRemap {
    _file: File,
    mmap: Mmap,
    source_keys: usize,
    target_keys: u64,
}

impl MappedRegistryRemap {
    fn open(path: &Path) -> Result<Self> {
        let (file, metadata) = open_regular_read(path)?;
        ensure!(
            metadata.len() >= REPROCESS_REMAP_HEADER_BYTES,
            "registry remap is shorter than its header"
        );
        // SAFETY: the retained read-only file descriptor keeps the mapped inode alive. The
        // staging generation is private and the exact file binding is checked before this call.
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        ensure!(&mmap[..8] == REPROCESS_REMAP_MAGIC);
        ensure!(u32::from_le_bytes(mmap[8..12].try_into().unwrap()) == REPROCESS_REMAP_VERSION);
        ensure!(
            u32::from_le_bytes(mmap[12..16].try_into().unwrap()) == 0,
            "registry remap reserved flags are nonzero"
        );
        let source_keys_u64 = u64::from_le_bytes(mmap[16..24].try_into().unwrap());
        let target_keys = u64::from_le_bytes(mmap[24..32].try_into().unwrap());
        let expected = REPROCESS_REMAP_HEADER_BYTES
            .checked_add(
                source_keys_u64
                    .checked_mul(4)
                    .context("registry remap length overflow")?,
            )
            .context("registry remap file length overflow")?;
        ensure!(
            metadata.len() == expected,
            "registry remap length does not match its header"
        );
        let source_keys =
            usize::try_from(source_keys_u64).context("registry remap key count exceeds usize")?;
        Ok(Self {
            _file: file,
            mmap,
            source_keys,
            target_keys,
        })
    }

    fn get(&self, old_id: u32) -> Result<u32> {
        ensure!(old_id != 0, "registry remap cannot resolve ID 0");
        let index = usize::try_from(old_id - 1).context("registry remap ID exceeds usize")?;
        ensure!(
            index < self.source_keys,
            "registry remap ID {old_id} exceeds source key count"
        );
        let offset = 32usize
            .checked_add(
                index
                    .checked_mul(4)
                    .context("registry remap offset overflow")?,
            )
            .context("registry remap offset overflow")?;
        Ok(u32::from_le_bytes(
            self.mmap[offset..offset + 4].try_into().unwrap(),
        ))
    }
}

impl MappedRegistry {
    fn open(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options
            .open(path)
            .with_context(|| format!("open immutable registry {}", path.display()))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("stat {}", path.display()))?;
        ensure!(
            metadata.file_type().is_file(),
            "registry is not a regular file: {}",
            path.display()
        );
        let bytes = metadata.len();
        ensure!(
            bytes > 0 && bytes.is_multiple_of(32),
            "registry {} has invalid byte length {bytes}; expected a non-zero multiple of 32",
            path.display()
        );
        let keys = bytes / 32;
        ensure!(
            keys <= u64::from(u32::MAX),
            "registry {} has {keys} keys, exceeding the u32 ID space",
            path.display()
        );
        let len = usize::try_from(keys).context("registry key count exceeds usize")?;
        // SAFETY: the file is held open for the mapping lifetime and only read through this type.
        let mmap = unsafe { MmapOptions::new().map(&file) }
            .with_context(|| format!("mmap {}", path.display()))?;
        Ok(Self {
            _file: file,
            mmap,
            len,
        })
    }

    #[inline]
    fn key(&self, id: u32) -> Result<[u8; 32]> {
        ensure!(id != 0, "compact pubkey uses reserved ID 0");
        let index = usize::try_from(id - 1).context("pubkey ID exceeds usize")?;
        let start = index
            .checked_mul(32)
            .context("registry byte offset overflow")?;
        let bytes = self
            .mmap
            .get(start..start + 32)
            .ok_or_else(|| anyhow!("pubkey registry ID {id} is outside 1..={} ", self.len))?;
        Ok(bytes
            .try_into()
            .expect("registry key slice has exact length"))
    }

    fn keys(&self) -> &[[u8; 32]] {
        // SAFETY: `[u8; 32]` has alignment one, every bit-pattern is valid, and the mapping's
        // length was checked to be exactly `len * 32` above.
        unsafe { std::slice::from_raw_parts(self.mmap.as_ptr().cast::<[u8; 32]>(), self.len) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReferenceClass {
    Eligible,
    Excluded,
}

#[derive(Debug)]
struct SemanticAccumulator {
    reference: Sha256,
    structure: Sha256,
    blocks: u64,
    transactions: u64,
    pubkey_references: u64,
}

impl SemanticAccumulator {
    fn new() -> Self {
        let mut reference = Sha256::new();
        reference.update(SEMANTIC_DOMAIN);
        reference.update(b".references");
        let mut structure = Sha256::new();
        structure.update(SEMANTIC_DOMAIN);
        structure.update(b".structure");
        Self {
            reference,
            structure,
            blocks: 0,
            transactions: 0,
            pubkey_references: 0,
        }
    }

    fn push(&mut self, block: &BlockSemantic) -> Result<()> {
        ensure!(
            block.block_id == self.blocks,
            "semantic block order mismatch: got {}, expected {}",
            block.block_id,
            self.blocks
        );
        self.reference.update(block.block_id.to_le_bytes());
        self.reference.update(block.slot.to_le_bytes());
        self.reference.update(block.references.to_le_bytes());
        self.reference.update(block.reference_sha256);
        self.structure.update(block.block_id.to_le_bytes());
        self.structure.update(block.slot.to_le_bytes());
        self.structure.update(block.normalized_len.to_le_bytes());
        self.structure.update(block.normalized_sha256);
        self.blocks = self
            .blocks
            .checked_add(1)
            .context("semantic block count overflow")?;
        self.transactions = self
            .transactions
            .checked_add(u64::from(block.transactions))
            .context("semantic transaction count overflow")?;
        self.pubkey_references = self
            .pubkey_references
            .checked_add(block.references)
            .context("semantic pubkey reference count overflow")?;
        Ok(())
    }

    fn finish(self) -> SemanticBinding {
        SemanticBinding {
            blocks: self.blocks,
            transactions: self.transactions,
            pubkey_references: self.pubkey_references,
            reference_sha256: hex_digest(self.reference.finalize()),
            normalized_structure_sha256: hex_digest(self.structure.finalize()),
        }
    }
}

#[derive(Debug)]
struct BlockSemantic {
    block_id: u64,
    slot: u64,
    transactions: u32,
    references: u64,
    reference_sha256: [u8; 32],
    normalized_len: u64,
    normalized_sha256: [u8; 32],
}

#[derive(Debug)]
struct BlockRewriteStats {
    block_id: u64,
    slot: u64,
    transactions: u32,
    references: u64,
}

#[derive(Debug, Default)]
struct RewriteStatsAccumulator {
    blocks: u64,
    transactions: u64,
    pubkey_references: u64,
    previous_slot: Option<u64>,
}

impl RewriteStatsAccumulator {
    fn push(&mut self, block: &BlockRewriteStats) -> Result<()> {
        ensure!(
            block.block_id == self.blocks,
            "rewrite block order mismatch: got {}, expected {}",
            block.block_id,
            self.blocks
        );
        if let Some(previous_slot) = self.previous_slot {
            ensure!(
                block.slot > previous_slot,
                "rewrite slot order mismatch: got {}, previous {}",
                block.slot,
                previous_slot
            );
        }
        self.previous_slot = Some(block.slot);
        self.blocks = self
            .blocks
            .checked_add(1)
            .context("rewrite block count overflow")?;
        self.transactions = self
            .transactions
            .checked_add(u64::from(block.transactions))
            .context("rewrite transaction count overflow")?;
        self.pubkey_references = self
            .pubkey_references
            .checked_add(block.references)
            .context("rewrite pubkey reference count overflow")?;
        Ok(())
    }

    fn finish(self) -> RewriteStats {
        RewriteStats {
            blocks: self.blocks,
            transactions: self.transactions,
            pubkey_references: self.pubkey_references,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Pass2PhaseTiming {
    zstd_decompress: Duration,
    outer_decode: Duration,
    message_metadata_rewrite: Duration,
    access_build_serialize: Duration,
    whole_block_serialize: Duration,
    zstd_compress: Duration,
    count_run_sort: Duration,
    wire_message_fast_records: u64,
    wire_message_fallback_records: u64,
    wire_metadata_fast_records: u64,
    wire_metadata_fallback_records: u64,
    wire_metadata_success_fast_records: u64,
    wire_metadata_current_error_fast_records: u64,
    wire_metadata_legacy_error_fast_records: u64,
    wire_metadata_ambiguous_fallback_records: u64,
    wire_metadata_error_prefix_fallback_records: u64,
    wire_metadata_rollback_fallback_records: u64,
}

impl Pass2PhaseTiming {
    fn add(&mut self, other: Self) {
        self.zstd_decompress += other.zstd_decompress;
        self.outer_decode += other.outer_decode;
        self.message_metadata_rewrite += other.message_metadata_rewrite;
        self.access_build_serialize += other.access_build_serialize;
        self.whole_block_serialize += other.whole_block_serialize;
        self.zstd_compress += other.zstd_compress;
        self.count_run_sort += other.count_run_sort;
        self.wire_message_fast_records += other.wire_message_fast_records;
        self.wire_message_fallback_records += other.wire_message_fallback_records;
        self.wire_metadata_fast_records += other.wire_metadata_fast_records;
        self.wire_metadata_fallback_records += other.wire_metadata_fallback_records;
        self.wire_metadata_success_fast_records += other.wire_metadata_success_fast_records;
        self.wire_metadata_current_error_fast_records +=
            other.wire_metadata_current_error_fast_records;
        self.wire_metadata_legacy_error_fast_records +=
            other.wire_metadata_legacy_error_fast_records;
        self.wire_metadata_ambiguous_fallback_records +=
            other.wire_metadata_ambiguous_fallback_records;
        self.wire_metadata_error_prefix_fallback_records +=
            other.wire_metadata_error_prefix_fallback_records;
        self.wire_metadata_rollback_fallback_records +=
            other.wire_metadata_rollback_fallback_records;
    }
}

#[derive(Debug)]
struct CompressedBlockInput {
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    bytes: Vec<u8>,
    signatures: Option<Vec<u8>>,
}

#[derive(Debug)]
struct SourceBlockAnalysis {
    eligible: Vec<(u32, u32)>,
    all: Vec<(u32, u32)>,
    semantic: BlockSemantic,
}

#[derive(Debug)]
struct SourceExclusionAnalysis {
    excluded: Vec<(u32, u32)>,
    transactions: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HotBlockOuterSchema {
    Current,
    LegacyShredding,
    LegacyRewardsVec,
}

struct DecodedHotBlock {
    block: ArchiveV2HotBlockBlob,
    outer_schema: HotBlockOuterSchema,
}

#[derive(Debug)]
struct RewrittenBlock {
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    compressed: Vec<u8>,
    uncompressed_len: u32,
    stats: BlockRewriteStats,
    eligible: Vec<(u32, u32)>,
    excluded: Vec<(u32, u32)>,
    access: Option<Vec<u8>>,
    phase_timing: Pass2PhaseTiming,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct OrderedPipelineReport {
    admitted: usize,
    completed: usize,
    max_active_workers: usize,
    max_pending_results: usize,
    max_accounted_bytes: u64,
}

struct OrderedPipelineCompletion<T> {
    sequence: usize,
    reserved_bytes: u64,
    result: Result<T>,
}

enum OrderedPipelinePending<T> {
    Ready { retained_bytes: u64, value: T },
    Failed(anyhow::Error),
}

struct AccessBuildContext {
    blockhashes: Vec<[u8; 32]>,
    previous_tail: Vec<super::PreviousBlockhash>,
    vote_hashes: Vec<super::VoteHashRegistryRow>,
}

#[derive(Default)]
struct AccessReferenceSet {
    pubkey_ids: Vec<u32>,
    blockhash_ids: Vec<i32>,
    vote_hash_block_ids: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RegistryWireCheckpoint {
    eligible_len: usize,
    excluded_len: usize,
    references: u64,
    access_lengths: Option<(usize, usize, usize)>,
}

struct RegistryWireVisitor<'a> {
    source_registry: &'a MappedRegistry,
    old_to_new: &'a [u32],
    block_id: u32,
    eligible_ids: &'a mut Vec<u32>,
    excluded_ids: &'a mut Vec<u32>,
    references: &'a mut u64,
    access: Option<&'a mut AccessReferenceSet>,
}

#[derive(Debug, Clone, Copy)]
struct SortRecord {
    count: u32,
    key: [u8; 32],
    old_id: u32,
}

impl SortRecord {
    fn cmp_canonical(&self, other: &Self) -> Ordering {
        other
            .count
            .cmp(&self.count)
            .then_with(|| self.key.cmp(&other.key))
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct HeapRecord {
    record: SortRecord,
    run: usize,
}

impl Ord for HeapRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .record
            .cmp_canonical(&self.record)
            .then_with(|| other.run.cmp(&self.run))
    }
}

impl PartialOrd for HeapRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SortRecord {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && self.key == other.key && self.old_id == other.old_id
    }
}

impl Eq for SortRecord {}

struct SortRunReader {
    reader: BufReader<File>,
    remaining: u64,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyCompactMetaV1 {
    err: Option<Vec<u8>>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<CompactInnerInstructions>>,
    logs: Option<CompactLogStream>,
    pre_token_balances: Vec<CompactTokenBalance>,
    post_token_balances: Vec<CompactTokenBalance>,
    rewards: Vec<CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<CompactReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

#[derive(Debug, SchemaRead)]
struct SelectiveInnerInstructions {
    index: u32,
    #[wincode(with = "DiscardSeq<SelectiveInnerInstruction, wincode::len::BincodeLen>")]
    instructions: Vec<SelectiveInnerInstruction>,
}

#[derive(Debug, SchemaRead)]
struct SelectiveInnerInstruction {
    program_id_index: u32,
    #[wincode(with = "DiscardBytes<wincode::len::BincodeLen>")]
    accounts: Vec<u8>,
    #[wincode(with = "DiscardBytes<wincode::len::BincodeLen>")]
    data: Vec<u8>,
    stack_height: Option<u32>,
}

#[derive(Debug, SchemaRead)]
struct SelectiveReturnData {
    program_id: CompactPubkey,
    #[wincode(with = "DiscardBytes<wincode::len::BincodeLen>")]
    data: Vec<u8>,
}

#[derive(Debug, SchemaRead)]
struct SelectiveStringTable {
    #[wincode(with = "DiscardSeq<u32, wincode::len::BincodeLen>")]
    lengths: Vec<u32>,
    #[wincode(with = "DiscardBytes<wincode::len::BincodeLen>")]
    bytes: Vec<u8>,
}

#[derive(Debug, SchemaRead)]
struct SelectiveDataTable {
    #[wincode(with = "DiscardSeq<blockzilla_format::DataArray, wincode::len::BincodeLen>")]
    arrays: Vec<blockzilla_format::DataArray>,
    #[wincode(with = "DiscardSeq<u32, wincode::len::BincodeLen>")]
    chunk_lengths: Vec<u32>,
    #[wincode(with = "DiscardBytes<wincode::len::BincodeLen>")]
    bytes: Vec<u8>,
}

#[derive(Debug, SchemaRead)]
struct SelectiveLogStream {
    #[wincode(
        with = "wincode::containers::Vec<LogEvent, wincode::len::BincodeLen<PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES>>"
    )]
    events: Vec<LogEvent>,
    strings: SelectiveStringTable,
    data: SelectiveDataTable,
}

#[allow(dead_code)]
#[derive(Debug, SchemaRead)]
struct SelectiveCurrentMetaV1 {
    err: Option<CompactTransactionError>,
    fee: u64,
    #[wincode(with = "DiscardSeq<u64, wincode::len::BincodeLen>")]
    pre_balances: Vec<u64>,
    #[wincode(with = "DiscardSeq<u64, wincode::len::BincodeLen>")]
    post_balances: Vec<u64>,
    #[wincode(with = "Option<DiscardSeq<SelectiveInnerInstructions, wincode::len::BincodeLen>>")]
    inner_instructions: Option<Vec<SelectiveInnerInstructions>>,
    logs: Option<SelectiveLogStream>,
    #[wincode(with = "DiscardSeq<CompactTokenBalance, wincode::len::BincodeLen>")]
    pre_token_balances: Vec<CompactTokenBalance>,
    #[wincode(with = "DiscardSeq<CompactTokenBalance, wincode::len::BincodeLen>")]
    post_token_balances: Vec<CompactTokenBalance>,
    #[wincode(with = "DiscardSeq<CompactReward, wincode::len::BincodeLen>")]
    rewards: Vec<CompactReward>,
    #[wincode(with = "DiscardSeq<CompactPubkey, wincode::len::BincodeLen>")]
    loaded_writable_addresses: Vec<CompactPubkey>,
    #[wincode(with = "DiscardSeq<CompactPubkey, wincode::len::BincodeLen>")]
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<SelectiveReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, SchemaRead)]
struct SelectiveLegacyMetaV1 {
    // Retain the uncommon legacy error payload so pass 1 can apply the same semantic wire-schema
    // gate as the full decoder. Merely skipping these bytes can make current metadata appear to be
    // a successful legacy decode after the current error tag is misread as a byte-vector length.
    #[wincode(
        with = "Option<wincode::containers::Vec<u8, wincode::len::BincodeLen<PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES>>>"
    )]
    err: Option<Vec<u8>>,
    fee: u64,
    #[wincode(with = "DiscardSeq<u64, wincode::len::BincodeLen>")]
    pre_balances: Vec<u64>,
    #[wincode(with = "DiscardSeq<u64, wincode::len::BincodeLen>")]
    post_balances: Vec<u64>,
    #[wincode(with = "Option<DiscardSeq<SelectiveInnerInstructions, wincode::len::BincodeLen>>")]
    inner_instructions: Option<Vec<SelectiveInnerInstructions>>,
    logs: Option<SelectiveLogStream>,
    #[wincode(with = "DiscardSeq<CompactTokenBalance, wincode::len::BincodeLen>")]
    pre_token_balances: Vec<CompactTokenBalance>,
    #[wincode(with = "DiscardSeq<CompactTokenBalance, wincode::len::BincodeLen>")]
    post_token_balances: Vec<CompactTokenBalance>,
    #[wincode(with = "DiscardSeq<CompactReward, wincode::len::BincodeLen>")]
    rewards: Vec<CompactReward>,
    #[wincode(with = "DiscardSeq<CompactPubkey, wincode::len::BincodeLen>")]
    loaded_writable_addresses: Vec<CompactPubkey>,
    #[wincode(with = "DiscardSeq<CompactPubkey, wincode::len::BincodeLen>")]
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<SelectiveReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

#[derive(Debug, SchemaRead)]
struct Pass1HotRewards {
    num_partitions: Option<u64>,
    #[wincode(
        with = "wincode::containers::Vec<CompactReward, wincode::len::BincodeLen<PASS1_REWARDS_RETAINED_SEQUENCE_MAX_BYTES>>"
    )]
    decoded: Vec<CompactReward>,
}

impl From<Pass1HotRewards> for ArchiveV2HotRewards {
    fn from(value: Pass1HotRewards) -> Self {
        Self {
            num_partitions: value.num_partitions,
            decoded: value.decoded,
        }
    }
}

#[derive(Debug, SchemaRead)]
struct Pass1CurrentHotBlockHeader {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    rewards: Option<Pass1HotRewards>,
}

impl From<Pass1CurrentHotBlockHeader> for ArchiveV2HotBlockHeader {
    fn from(value: Pass1CurrentHotBlockHeader) -> Self {
        Self {
            slot: value.slot,
            parent_slot: value.parent_slot,
            blockhash_id: value.blockhash_id,
            previous_blockhash_id: value.previous_blockhash_id,
            block_time: value.block_time,
            block_height: value.block_height,
            rewards: value.rewards.map(Into::into),
        }
    }
}

#[derive(Debug, SchemaRead)]
struct Pass1LegacyHotBlockHeaderWithShredding {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    #[wincode(with = "DiscardSeq<CompactShredding, wincode::len::BincodeLen>")]
    shredding: Vec<CompactShredding>,
    rewards: Option<Pass1HotRewards>,
}

impl From<Pass1LegacyHotBlockHeaderWithShredding> for ArchiveV2HotBlockHeader {
    fn from(value: Pass1LegacyHotBlockHeaderWithShredding) -> Self {
        Self {
            slot: value.slot,
            parent_slot: value.parent_slot,
            blockhash_id: value.blockhash_id,
            previous_blockhash_id: value.previous_blockhash_id,
            block_time: value.block_time,
            block_height: value.block_height,
            rewards: value.rewards.map(Into::into),
        }
    }
}

#[derive(Debug, SchemaRead)]
struct Pass1LegacyHotBlockHeaderWithRewardsVec {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    #[wincode(
        with = "wincode::containers::Vec<CompactReward, wincode::len::BincodeLen<PASS1_REWARDS_RETAINED_SEQUENCE_MAX_BYTES>>"
    )]
    rewards: Vec<CompactReward>,
}

impl From<Pass1LegacyHotBlockHeaderWithRewardsVec> for ArchiveV2HotBlockHeader {
    fn from(value: Pass1LegacyHotBlockHeaderWithRewardsVec) -> Self {
        let rewards = (!value.rewards.is_empty()).then_some(ArchiveV2HotRewards {
            num_partitions: None,
            decoded: value.rewards,
        });
        Self {
            slot: value.slot,
            parent_slot: value.parent_slot,
            blockhash_id: value.blockhash_id,
            previous_blockhash_id: value.previous_blockhash_id,
            block_time: value.block_time,
            block_height: value.block_height,
            rewards,
        }
    }
}

#[derive(Debug, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyHotBlockWithShredding {
    header: LegacyHotBlockHeaderWithShredding,
    tx_count: u32,
    tx_rows: Vec<blockzilla_format::ArchiveV2HotTxRow>,
    message_bytes: Vec<u8>,
    metadata_bytes: Vec<u8>,
}

#[derive(Debug, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyHotBlockHeaderWithShredding {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    shredding: Vec<CompactShredding>,
    rewards: Option<ArchiveV2HotRewards>,
}

#[derive(Debug, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyHotBlockWithRewardsVec {
    header: LegacyHotBlockHeaderWithRewardsVec,
    tx_count: u32,
    tx_rows: Vec<blockzilla_format::ArchiveV2HotTxRow>,
    message_bytes: Vec<u8>,
    metadata_bytes: Vec<u8>,
}

#[derive(Debug, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyHotBlockHeaderWithRewardsVec {
    slot: u64,
    parent_slot: u64,
    blockhash_id: u32,
    previous_blockhash_id: u32,
    block_time: Option<i64>,
    block_height: Option<u64>,
    rewards: Vec<CompactReward>,
}

impl From<LegacyHotBlockWithShredding> for ArchiveV2HotBlockBlob {
    fn from(value: LegacyHotBlockWithShredding) -> Self {
        Self {
            header: ArchiveV2HotBlockHeader {
                slot: value.header.slot,
                parent_slot: value.header.parent_slot,
                blockhash_id: value.header.blockhash_id,
                previous_blockhash_id: value.header.previous_blockhash_id,
                block_time: value.header.block_time,
                block_height: value.header.block_height,
                rewards: value.header.rewards,
            },
            tx_count: value.tx_count,
            tx_rows: value.tx_rows,
            message_bytes: value.message_bytes,
            metadata_bytes: value.metadata_bytes,
        }
    }
}

impl From<LegacyHotBlockWithRewardsVec> for ArchiveV2HotBlockBlob {
    fn from(value: LegacyHotBlockWithRewardsVec) -> Self {
        let rewards = (!value.header.rewards.is_empty()).then_some(ArchiveV2HotRewards {
            num_partitions: None,
            decoded: value.header.rewards,
        });
        Self {
            header: ArchiveV2HotBlockHeader {
                slot: value.header.slot,
                parent_slot: value.header.parent_slot,
                blockhash_id: value.header.blockhash_id,
                previous_blockhash_id: value.header.previous_blockhash_id,
                block_time: value.header.block_time,
                block_height: value.header.block_height,
                rewards,
            },
            tx_count: value.tx_count,
            tx_rows: value.tx_rows,
            message_bytes: value.message_bytes,
            metadata_bytes: value.metadata_bytes,
        }
    }
}

impl TryFrom<LegacyCompactMetaV1> for CompactMetaV1 {
    type Error = anyhow::Error;

    fn try_from(value: LegacyCompactMetaV1) -> Result<Self> {
        let err = value
            .err
            .as_deref()
            .map(CompactTransactionError::from_stored_wincode_bytes)
            .transpose()?;
        Ok(Self {
            err,
            fee: value.fee,
            pre_balances: value.pre_balances,
            post_balances: value.post_balances,
            inner_instructions: value.inner_instructions,
            logs: value.logs,
            pre_token_balances: value.pre_token_balances,
            post_token_balances: value.post_token_balances,
            rewards: value.rewards,
            loaded_writable_addresses: value.loaded_writable_addresses,
            loaded_readonly_addresses: value.loaded_readonly_addresses,
            return_data: value.return_data,
            compute_units_consumed: value.compute_units_consumed,
            cost_units: value.cost_units,
        })
    }
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyBlockAccessBlobV1 {
    version: u16,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
}

#[derive(Debug, Serialize, Deserialize, SchemaRead, SchemaWrite)]
struct LegacyBlockAccessBlobV2NoVotes {
    version: u16,
    flags: u32,
    blockhash: [u8; 32],
    previous_blockhash: [u8; 32],
    signature_counts: Vec<u8>,
    signatures: Vec<u8>,
    pubkeys: Vec<ArchiveV2BlockAccessPubkey>,
    blockhashes: Vec<ArchiveV2BlockAccessBlockhash>,
}

/// Rewrite one committed first-seen generation into a durable registry-only staging generation.
///
/// This phase never reads signature data, builds block-access, writes a final receipt, or
/// publishes the target. The access-completion phase owns those operations.
pub(crate) fn reprocess_first_seen_registry(
    options: &RegistryReprocessOptions,
) -> Result<RegistryReprocessCoreResult> {
    let started = Instant::now();
    validate_options(options)?;
    let source_dir = fs::canonicalize(&options.source_dir)
        .with_context(|| format!("canonicalize {}", options.source_dir.display()))?;
    ensure!(
        source_dir.is_dir(),
        "source is not a directory: {}",
        source_dir.display()
    );
    let target_dir = canonical_target_path(&options.target_dir)?;
    ensure!(
        source_dir != target_dir
            && !target_dir.starts_with(&source_dir)
            && !source_dir.starts_with(&target_dir),
        "source and target generations must be distinct, non-nested directories"
    );
    let target_parent = target_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| anyhow!("target has no parent: {}", target_dir.display()))?;
    fs::create_dir_all(target_parent)
        .with_context(|| format!("create target parent {}", target_parent.display()))?;
    let staging = canonical_staging_path(&options.staging_dir, &target_dir, &options.attempt_id)?;
    let _lock = acquire_reprocess_lock(&source_dir, &target_dir, options.epoch)?;
    ensure!(
        !target_dir.exists(),
        "registry reprocess target already exists and will not be replaced: {}",
        target_dir.display()
    );
    let checkpoint = build_checkpoint(&source_dir, &target_dir, &staging, options)?;
    if let Some(result) = reuse_core_handoff_if_complete(&staging, &checkpoint)? {
        return Ok(result);
    }
    prepare_staging(&staging, &checkpoint)?;

    info!(
        source = %source_dir.display(),
        target = %target_dir.display(),
        epoch = options.epoch,
        threads = options.threads,
        sort_memory_mib = options.sort_memory_mib,
        zstd_level = options.level,
        "starting Compact-V2 registry reprocess"
    );

    let source_validation_started = Instant::now();
    let manifest = read_source_manifest(&source_dir)?;
    let source_registry_path = source_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let source_registry_snapshot_path = staging.join(SOURCE_REGISTRY_SNAPSHOT_FILE);
    let _snapshot_copy_binding =
        clone_or_copy_file(&source_registry_path, &source_registry_snapshot_path)?;
    let source_registry_metadata = regular_file_metadata(&source_registry_path)?;
    ensure!(
        regular_file_metadata(&source_registry_snapshot_path)?.len()
            == source_registry_metadata.len(),
        "private source registry snapshot has the wrong length"
    );
    let source_registry = MappedRegistry::open(&source_registry_snapshot_path)?;
    let source_registry_keys = source_registry.len as u64;
    validate_source_registry_index(&source_dir, &source_registry)?;
    ensure!(
        manifest.registry_keys == source_registry.len as u64,
        "first-seen manifest registry_keys={} but registry.bin has {} keys",
        manifest.registry_keys,
        source_registry.len
    );
    let source_counts = read_registry_counts(
        &source_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        source_registry.len,
    )?;
    let source_count_sum = source_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("source registry reference count overflow")
    })?;
    ensure!(
        source_count_sum == manifest.references,
        "first-seen manifest references={} but registry_counts sum={source_count_sum}",
        manifest.references
    );

    let source_meta = validate_and_rewrite_meta(&source_dir, &staging)?;
    let source_blocks_path = source_dir.join(ARCHIVE_V2_BLOCKS_FILE);
    let source_index_path = source_dir.join(ARCHIVE_V2_BLOCK_INDEX_FILE);
    let hot_index = read_archive_v2_hot_block_index(&source_index_path)?;
    validate_hot_index(&source_blocks_path, &hot_index, options.epoch)?;
    ensure!(
        source_meta.blocks == hot_index.rows.len() as u64,
        "metadata footer blocks={} but hot index has {} rows",
        source_meta.blocks,
        hot_index.rows.len()
    );
    let total_transactions = hot_index
        .rows
        .last()
        .map(|row| {
            row.first_tx_ordinal
                .checked_add(u64::from(row.tx_count))
                .context("final transaction ordinal overflow")
        })
        .transpose()?
        .unwrap_or(0);
    let total_signatures = hot_index
        .rows
        .last()
        .map(|row| {
            row.first_signature_ordinal
                .checked_add(u64::from(row.signature_count))
                .context("final signature ordinal overflow")
        })
        .transpose()?
        .unwrap_or(0);
    let signature_bytes = total_signatures
        .checked_mul(64)
        .context("signature sidecar byte length overflow")?;
    signature_link_preflight(&source_dir, &staging, signature_bytes).context(
        "signature sidecar cannot be linked into the staged generation without reading data",
    )?;
    ensure!(
        source_meta.transactions == total_transactions,
        "metadata footer transactions={} but hot index covers {total_transactions}",
        source_meta.transactions
    );
    info!(
        elapsed_secs = source_validation_started.elapsed().as_secs_f64(),
        blocks = hot_index.rows.len(),
        transactions = total_transactions,
        signatures = total_signatures,
        source_registry_keys = source_registry.len,
        source_reference_count = manifest.references,
        compressed_bytes = hot_index.blob_file_bytes,
        "validated first-seen source generation"
    );

    let total_progress = (hot_index.rows.len() as u64)
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .context("registry reprocess progress total overflow")?;
    let mut progress = ProgressTracker::new("registry reprocess");
    progress.set_estimated_total_blocks(total_progress);

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(options.threads)
        .thread_name(|index| format!("registry-reprocess-{index}"))
        .build()
        .context("build registry reprocess worker pool")?;
    let batch_size = parallel_batch_rows(options.threads);
    let phase_timing_enabled = std::env::var_os(PHASE_TIMING_ENV).is_some();
    let mut source_blocks = File::open(&source_blocks_path)
        .with_context(|| format!("open {}", source_blocks_path.display()))?;
    let mut source_blocks_hash = Sha256::new();
    // First-seen counts cover every typed CompactPubkey. Historical CAR ordering excludes only
    // block rewards and pubkeys found solely in structured logs. Start from the authenticated
    // all-reference counts and subtract that small excluded set. Pass 2 independently traverses
    // every reference and verifies both the all-reference and eligible vectors before publication.
    let mut eligible_counts = source_counts.clone();
    let mut input_bytes_done = 0u64;
    let pass1_started = Instant::now();

    let mut batch_start = 0usize;
    while batch_start < hot_index.rows.len() {
        let batch_end = hot_batch_end(&hot_index.rows, batch_start, batch_size, false)?;
        let rows = &hot_index.rows[batch_start..batch_end];
        let inputs =
            read_compressed_block_batch(&mut source_blocks, rows, Some(&mut source_blocks_hash))?;
        input_bytes_done = input_bytes_done
            .checked_add(
                inputs
                    .iter()
                    .map(|item| item.bytes.len() as u64)
                    .sum::<u64>(),
            )
            .context("pass1 input byte count overflow")?;
        let analyses = pool.install(|| {
            inputs
                .into_par_iter()
                .map(analyze_source_exclusions)
                .collect::<Result<Vec<_>>>()
        })?;
        for analysis in analyses {
            merge_count_runs(&mut eligible_counts, &analysis.excluded, true)?;
            progress.update_input_bytes(input_bytes_done);
            progress.update(1, u64::from(analysis.transactions));
        }
        batch_start = batch_end;
    }
    info!(
        elapsed_secs = pass1_started.elapsed().as_secs_f64(),
        blocks = hot_index.rows.len(),
        transactions = total_transactions,
        compressed_bytes = input_bytes_done,
        "completed count-only registry reprocess pass 1"
    );

    let eligible_references = eligible_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("eligible reference count overflow")
    })?;
    let sort_started = Instant::now();
    let (old_to_new, target_registry_keys) = build_usage_sorted_registry(
        &source_registry,
        &eligible_counts,
        &staging,
        options.sort_memory_mib,
        &pool,
    )?;
    info!(
        elapsed_secs = sort_started.elapsed().as_secs_f64(),
        source_registry_keys = source_registry.len,
        target_registry_keys,
        eligible_references,
        auxiliary_sort_memory_mib = options.sort_memory_mib,
        "built canonical usage-sorted registry"
    );
    build_registry_index(&staging)?;
    let target_registry = MappedRegistry::open(&staging.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    validate_registry_remap(&source_registry, &old_to_new, &target_registry)?;

    source_blocks.seek(SeekFrom::Start(0))?;
    let target_blocks_path = staging.join(ARCHIVE_V2_BLOCKS_FILE);
    let mut target_blocks = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        File::create(&target_blocks_path)
            .with_context(|| format!("create {}", target_blocks_path.display()))?,
    );
    let mut target_rows = Vec::with_capacity(hot_index.rows.len());
    let mut rewrite_stats = RewriteStatsAccumulator::default();
    let mut worker_phase_timing = Pass2PhaseTiming::default();
    let mut coordinator_write_time = Duration::ZERO;
    let mut all_counts_remaining = source_counts;
    let mut eligible_counts_remaining = eligible_counts;
    let mut target_blocks_hash = Sha256::new();
    let mut target_offset = 0u64;
    let pass2_started = Instant::now();
    let pass2_pipeline_memory_budget = registry_rewrite_pipeline_memory_budget(options.threads)?;
    let pass2_initial_admissible_workers = if hot_index.rows.is_empty() {
        0
    } else {
        hot_batch_end_with_budget(
            &hot_index.rows,
            0,
            options.threads,
            false,
            pass2_pipeline_memory_budget,
        )?
    };
    let pass2_pipeline = run_bounded_ordered_pipeline(
        &pool,
        hot_index.rows.len(),
        options.threads,
        pass2_pipeline_memory_budget,
        |sequence| hot_worker_reservation_bytes(&hot_index.rows[sequence], false),
        |sequence| read_compressed_block(&mut source_blocks, hot_index.rows[sequence], None),
        |input| {
            rewrite_source_block(
                input,
                &source_registry,
                &old_to_new,
                &target_registry,
                options.level,
                options.wire_profile,
                None,
                phase_timing_enabled,
            )
        },
        rewritten_block_retained_bytes,
        |_, item| {
            let coordinator_write_started = phase_timing_enabled.then(Instant::now);
            merge_count_runs(&mut all_counts_remaining, &item.eligible, true)?;
            merge_count_runs(&mut all_counts_remaining, &item.excluded, true)?;
            merge_count_runs(&mut eligible_counts_remaining, &item.eligible, true)?;
            ensure!(
                item.compressed.len() as u64 <= MAX_HOT_BLOCK_FRAME_BYTES,
                "rewritten compressed block {} is {} bytes, exceeding {} byte limit",
                item.row.block_id,
                item.compressed.len(),
                MAX_HOT_BLOCK_FRAME_BYTES
            );
            let compressed_len = u32::try_from(item.compressed.len())
                .context("rewritten compressed block exceeds u32::MAX")?;
            target_blocks
                .write_all(&item.compressed)
                .with_context(|| format!("write {}", target_blocks_path.display()))?;
            target_blocks_hash.update(&item.compressed);
            let mut row = item.row;
            row.compressed_offset = target_offset;
            row.compressed_len = compressed_len;
            row.uncompressed_len = item.uncompressed_len;
            target_offset = target_offset
                .checked_add(u64::from(compressed_len))
                .context("target block offset overflow")?;
            ensure!(
                item.access.is_none(),
                "registry-only phase unexpectedly built block-access for block_id {}",
                row.block_id
            );
            rewrite_stats.push(&item.stats)?;
            target_rows.push(row);
            progress.update(1, u64::from(item.stats.transactions));
            worker_phase_timing.add(item.phase_timing);
            if let Some(started) = coordinator_write_started {
                coordinator_write_time += started.elapsed();
            }
            Ok(())
        },
    )?;
    ensure!(
        pass2_pipeline.max_active_workers >= pass2_initial_admissible_workers,
        "ordered pass-2 pipeline reached only {} active workers; initial rows admit {pass2_initial_admissible_workers}",
        pass2_pipeline.max_active_workers
    );
    validate_consumed_reference_counts(&all_counts_remaining, &eligible_counts_remaining)?;
    drop(all_counts_remaining);
    drop(eligible_counts_remaining);
    target_blocks
        .flush()
        .with_context(|| format!("flush {}", target_blocks_path.display()))?;
    drop(target_blocks);
    let rewrite_stats = rewrite_stats.finish();
    let target_blocks_binding = FileBinding {
        bytes: target_offset,
        sha256: hex_digest(target_blocks_hash.finalize()),
    };
    write_archive_v2_hot_block_index(
        &staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
        target_offset,
        options.level,
        0,
        &target_rows,
    )?;
    let source_blocks_binding = FileBinding {
        bytes: hot_index.blob_file_bytes,
        sha256: hex_digest(source_blocks_hash.finalize()),
    };
    validate_source_binding_against_profile_authority(
        &checkpoint.wire_profile_authority,
        ARCHIVE_V2_BLOCKS_FILE,
        &source_blocks_binding,
    )?;
    let remap_binding = write_registry_remap(
        &staging.join(REPROCESS_REMAP_FILE),
        &old_to_new,
        target_registry_keys,
    )?;
    drop(target_registry);
    drop(source_registry);
    let source_registry_snapshot = hash_file(&source_registry_snapshot_path)?;
    validate_source_binding_against_profile_authority(
        &checkpoint.wire_profile_authority,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        &source_registry_snapshot,
    )?;
    write_wire_profile_marker(&staging, options.wire_profile)?;
    let core_files =
        core_staging_file_bindings(&staging, target_blocks_binding, options.wire_profile)?;
    info!(
        elapsed_secs = pass2_started.elapsed().as_secs_f64(),
        blocks = target_rows.len(),
        transactions = rewrite_stats.transactions,
        typed_references = rewrite_stats.pubkey_references,
        compressed_bytes = target_offset,
        pipeline_initial_admissible_workers = pass2_initial_admissible_workers,
        pipeline_max_active_workers = pass2_pipeline.max_active_workers,
        pipeline_max_pending_results = pass2_pipeline.max_pending_results,
        pipeline_max_accounted_mib = pass2_pipeline.max_accounted_bytes as f64 / (1 << 20) as f64,
        pipeline_memory_budget_mib = pass2_pipeline_memory_budget as f64 / (1 << 20) as f64,
        worker_scratch_precharge_mib = (HOT_BATCH_MEMORY_BUDGET_BYTES
            - pass2_pipeline_memory_budget) as f64
            / (1 << 20) as f64,
        wire_message_fast_records = worker_phase_timing.wire_message_fast_records,
        wire_message_fallback_records = worker_phase_timing.wire_message_fallback_records,
        wire_metadata_fast_records = worker_phase_timing.wire_metadata_fast_records,
        wire_metadata_fallback_records = worker_phase_timing.wire_metadata_fallback_records,
        wire_metadata_success_fast_records = worker_phase_timing.wire_metadata_success_fast_records,
        wire_metadata_current_error_fast_records =
            worker_phase_timing.wire_metadata_current_error_fast_records,
        wire_metadata_legacy_error_fast_records =
            worker_phase_timing.wire_metadata_legacy_error_fast_records,
        wire_metadata_ambiguous_fallback_records =
            worker_phase_timing.wire_metadata_ambiguous_fallback_records,
        wire_metadata_error_prefix_fallback_records =
            worker_phase_timing.wire_metadata_error_prefix_fallback_records,
        wire_metadata_rollback_fallback_records =
            worker_phase_timing.wire_metadata_rollback_fallback_records,
        "completed registry-only reprocess pass 2"
    );

    ensure!(
        rebuild_checkpoint_against_authority(
            &source_dir,
            &target_dir,
            &staging,
            options,
            &checkpoint.wire_profile_authority,
        )? == checkpoint,
        "source generation changed while registry reprocess was running"
    );
    sync_generation(&staging)?;
    let handoff = RegistryReprocessHandoff {
        version: REPROCESS_HANDOFF_VERSION,
        state: CORE_COMPLETE_STATE.to_owned(),
        attempt_id: options.attempt_id.clone(),
        epoch: options.epoch,
        source_dir: source_dir.display().to_string(),
        target_dir: target_dir.display().to_string(),
        staging_dir: staging.display().to_string(),
        threads: options.threads,
        sort_memory_mib: options.sort_memory_mib,
        level: options.level,
        source_anchor_sha256: checkpoint.source_anchor_sha256.clone(),
        source_registry_keys,
        target_registry_keys,
        eligible_references,
        rewrite_stats,
        source_blocks: source_blocks_binding,
        core_files,
        source_registry_snapshot,
        remap_file: remap_binding,
        wire_profile: options.wire_profile,
        wire_profile_authority: checkpoint.wire_profile_authority.clone(),
    };
    validate_handoff_shape(&handoff)?;
    let handoff_binding = write_handoff(&staging, &handoff)?;
    let result = core_result_from_handoff(&handoff, handoff_binding.sha256)?;
    progress.update(1, 0);
    progress.final_report();
    if phase_timing_enabled {
        info!(
            zstd_decompress_worker_secs = worker_phase_timing.zstd_decompress.as_secs_f64(),
            outer_decode_worker_secs = worker_phase_timing.outer_decode.as_secs_f64(),
            message_metadata_rewrite_worker_secs =
                worker_phase_timing.message_metadata_rewrite.as_secs_f64(),
            access_build_serialize_worker_secs =
                worker_phase_timing.access_build_serialize.as_secs_f64(),
            whole_block_serialize_worker_secs =
                worker_phase_timing.whole_block_serialize.as_secs_f64(),
            zstd_compress_worker_secs = worker_phase_timing.zstd_compress.as_secs_f64(),
            count_run_sort_worker_secs = worker_phase_timing.count_run_sort.as_secs_f64(),
            wire_message_fast_records = worker_phase_timing.wire_message_fast_records,
            wire_message_fallback_records = worker_phase_timing.wire_message_fallback_records,
            wire_metadata_fast_records = worker_phase_timing.wire_metadata_fast_records,
            wire_metadata_fallback_records = worker_phase_timing.wire_metadata_fallback_records,
            wire_metadata_success_fast_records =
                worker_phase_timing.wire_metadata_success_fast_records,
            wire_metadata_current_error_fast_records =
                worker_phase_timing.wire_metadata_current_error_fast_records,
            wire_metadata_legacy_error_fast_records =
                worker_phase_timing.wire_metadata_legacy_error_fast_records,
            wire_metadata_ambiguous_fallback_records =
                worker_phase_timing.wire_metadata_ambiguous_fallback_records,
            wire_metadata_error_prefix_fallback_records =
                worker_phase_timing.wire_metadata_error_prefix_fallback_records,
            wire_metadata_rollback_fallback_records =
                worker_phase_timing.wire_metadata_rollback_fallback_records,
            coordinator_write_secs = coordinator_write_time.as_secs_f64(),
            "registry reprocess detailed phase timing"
        );
    }
    info!(
        elapsed_secs = started.elapsed().as_secs_f64(),
        staging = %staging.display(),
        handoff = %staging.join(REPROCESS_HANDOFF_FILE).display(),
        target_registry_keys,
        eligible_references,
        "completed durable registry-only staging generation"
    );
    Ok(result)
}

struct AccessRemapBindings {
    source_blob: FileBinding,
    source_index: FileBinding,
    target_blob: FileBinding,
    target_index: FileBinding,
    target_get_block: FileBinding,
    signatures: FileBinding,
    boundary_repair: Option<AccessBoundaryRepair>,
}

fn require_access_continuation_state(
    expected: RegistryReprocessAccessContinuationState,
    actual: RegistryReprocessAccessContinuationState,
) -> Result<()> {
    ensure!(
        expected == actual,
        "access continuation state changed after scheduler admission: expected {}, found {}",
        expected.as_str(),
        actual.as_str()
    );
    Ok(())
}

/// Complete a durable registry-only staging generation by remapping trusted source access rows.
///
/// This normal path never opens or reads signature file data. Its binding comes from the
/// duplicate signature stream inside validated source access. The target sidecar is a strict
/// hard link.
pub(crate) fn complete_first_seen_registry_access(
    options: &RegistryReprocessAccessOptions,
) -> Result<RegistryReprocessReceipt> {
    let started = Instant::now();
    validate_attempt_id(&options.attempt_id)?;
    validate_hex_sha256(&options.handoff_sha256, "requested handoff digest")?;
    ensure!(
        options.epoch != 0,
        "first-seen registry reprocessing does not support epoch 0 genesis"
    );
    let source = fs::canonicalize(&options.source_dir)
        .with_context(|| format!("canonicalize {}", options.source_dir.display()))?;
    ensure!(source.is_dir(), "source is not a directory");
    let target = canonical_target_path(&options.target_dir)?;
    let expected_staging =
        canonical_staging_path(&options.staging_dir, &target, &options.attempt_id)?;
    let _lock = acquire_reprocess_lock(&source, &target, options.epoch)?;

    if target.try_exists()? {
        let receipt = probe_published_reprocess_without_signature_data(&target, options.epoch)?;
        ensure_v3_attempt_matches(&receipt, options, &source, &target)?;
        return Ok(receipt);
    }

    let staging = fs::canonicalize(&expected_staging)
        .with_context(|| format!("canonicalize staging {}", expected_staging.display()))?;
    ensure!(
        staging == expected_staging,
        "staging path changed before access completion"
    );
    validate_access_staging_directory(&staging)?;
    if staging.join(REGISTRY_REPROCESS_RECEIPT_FILE).try_exists()? {
        require_access_continuation_state(
            options.expected_continuation_state,
            RegistryReprocessAccessContinuationState::ReceiptReady,
        )?;
        return resume_staged_publication_without_signature_data(
            options, &source, &staging, &target,
        );
    }
    let corrupt_receipt_temp = if staging
        .join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE)
        .try_exists()?
    {
        match read_receipt_temp(&staging).and_then(|receipt| {
            validate_staged_receipt_without_signature_data(
                options, &source, &staging, &target, &receipt, false,
            )
        }) {
            Ok(()) => {
                require_access_continuation_state(
                    options.expected_continuation_state,
                    RegistryReprocessAccessContinuationState::ReceiptReady,
                )?;
                return resume_staged_receipt_temp_without_signature_data(
                    options, &source, &staging, &target,
                );
            }
            Err(error) => Some(error),
        }
    } else {
        None
    };
    require_access_continuation_state(
        options.expected_continuation_state,
        RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
    )?;

    let (handoff, handoff_binding) = read_handoff(&staging)?;
    validate_handoff_shape(&handoff)?;
    ensure!(
        handoff_binding.sha256 == options.handoff_sha256,
        "handoff digest does not match the scheduler claim"
    );
    ensure_handoff_matches_access_options(&handoff, options, &source, &staging, &target)?;
    let checkpoint = read_checkpoint(&staging)?;
    ensure!(
        handoff_matches_checkpoint(&handoff, &checkpoint),
        "handoff and initial checkpoint disagree"
    );
    let core_options = RegistryReprocessOptions {
        source_dir: source.clone(),
        target_dir: target.clone(),
        epoch: handoff.epoch,
        threads: handoff.threads,
        sort_memory_mib: handoff.sort_memory_mib,
        level: handoff.level,
        attempt_id: handoff.attempt_id.clone(),
        staging_dir: staging.clone(),
        wire_profile: handoff.wire_profile,
        wire_profile_authority_receipt: options.wire_profile_authority_receipt.clone(),
    };
    ensure!(
        rebuild_checkpoint_against_authority(
            &source,
            &target,
            &staging,
            &core_options,
            &checkpoint.wire_profile_authority,
        )? == checkpoint,
        "core source inputs changed before access completion"
    );
    if corrupt_receipt_temp.is_some() {
        validate_access_partial_staging_bounded(&staging, &handoff)?;
    }
    if let Some(error) = corrupt_receipt_temp {
        ensure!(
            remove_regular_if_present(&staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE))?,
            "invalid access receipt temp disappeared before exact cleanup"
        );
        sync_directory(&staging)?;
        info!(error = %error, "discarded corrupt access receipt temp after core validation");
    }
    remove_owned_phase2_outputs(&staging)?;
    validate_core_staging(&staging, &handoff)?;
    let source_hot = read_archive_v2_hot_block_index(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    let target_hot = read_archive_v2_hot_block_index(&staging.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    validate_hot_index_geometry_for_access(&source_hot.rows, &target_hot.rows)?;
    ensure!(
        source_hot.rows.len() as u64 == handoff.rewrite_stats.blocks,
        "handoff block count does not match the source hot index"
    );
    let source_registry = MappedRegistry::open(&staging.join(SOURCE_REGISTRY_SNAPSHOT_FILE))?;
    let target_registry = MappedRegistry::open(&staging.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    ensure!(
        source_registry.len as u64 == handoff.source_registry_keys
            && target_registry.len as u64 == handoff.target_registry_keys,
        "handoff registry key counts do not match staged registries"
    );
    let remap = MappedRegistryRemap::open(&staging.join(REPROCESS_REMAP_FILE))?;
    ensure!(
        remap.source_keys == source_registry.len
            && remap.target_keys == handoff.target_registry_keys,
        "registry remap dimensions do not match the handoff"
    );
    let target_counts = read_registry_counts(
        &staging.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        target_registry.len,
    )?;
    validate_mapped_registry_remap(&source_registry, &remap, &target_registry, &target_counts)?;
    let access_context = load_access_build_context(&source, source_hot.rows.len(), options.epoch)?
        .context("source has no complete access set to remap")?;

    let remap_started = Instant::now();
    let access = remap_source_access(
        &source,
        &staging,
        options.epoch,
        &source_registry,
        &remap,
        &source_hot.rows,
        &target_hot.rows,
        &target_registry,
        &access_context,
    )?;
    let remap_elapsed = remap_started.elapsed();
    let copy_started = Instant::now();
    let copied = copy_independent_sidecars(&source, &staging, &BTreeMap::new())?;
    let copy_elapsed = copy_started.elapsed();
    let link_started = Instant::now();
    create_strict_signature_hard_link(&source, &staging, access.signatures.bytes)?;
    let link_elapsed = link_started.elapsed();

    let assembly_started = Instant::now();
    ensure!(
        rebuild_checkpoint_against_authority(
            &source,
            &target,
            &staging,
            &core_options,
            &checkpoint.wire_profile_authority,
        )? == checkpoint,
        "core source inputs changed during access completion"
    );
    let mut source_files = source_file_bindings(
        &source,
        &copied,
        handoff.source_blocks.clone(),
        RECEIPT_VERSION,
    )?;
    add_binding_if_file(
        &mut source_files,
        &source,
        ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
    )?;
    add_binding_if_file(&mut source_files, &source, GENERATION_MANIFEST_FILE)?;
    add_binding_if_file(&mut source_files, &source, ARCHIVE_V2_PUBKEY_HOT_SEED_FILE)?;
    let source_profile_marker = wire_profile_marker(handoff.wire_profile);
    add_binding_if_file(&mut source_files, &source, &source_profile_marker.name)?;
    insert_unique_binding(
        &mut source_files,
        ARCHIVE_V2_BLOCK_ACCESS_FILE,
        access.source_blob.clone(),
    )?;
    insert_unique_binding(
        &mut source_files,
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        access.source_index.clone(),
    )?;
    insert_unique_binding(
        &mut source_files,
        ARCHIVE_V2_SIGNATURES_FILE,
        access.signatures.clone(),
    )?;
    for (name, binding) in &source_files {
        validate_source_binding_against_profile_authority(
            &checkpoint.wire_profile_authority,
            name,
            binding,
        )?;
    }

    let mut target_files = target_file_bindings(
        &staging,
        &copied,
        handoff
            .core_files
            .get(ARCHIVE_V2_BLOCKS_FILE)
            .cloned()
            .context("handoff omits target blocks binding")?,
        Some(access.target_blob.clone()),
    )?;
    let profile_marker = wire_profile_marker(handoff.wire_profile);
    insert_unique_binding(
        &mut target_files,
        &profile_marker.name,
        handoff
            .core_files
            .get(&profile_marker.name)
            .cloned()
            .context("handoff omits selected wire-profile marker")?,
    )?;
    ensure!(
        target_files.get(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE) == Some(&access.target_index)
            && target_files.get(ARCHIVE_V2_GET_BLOCK_INDEX_FILE) == Some(&access.target_get_block),
        "target access index bindings changed during final assembly"
    );
    insert_unique_binding(
        &mut target_files,
        ARCHIVE_V2_SIGNATURES_FILE,
        access.signatures.clone(),
    )?;
    ensure!(!target_files.contains_key(REGISTRY_REPROCESS_RECEIPT_FILE));
    let receipt = RegistryReprocessReceipt {
        version: RECEIPT_VERSION,
        algorithm: RECEIPT_ALGORITHM.to_owned(),
        epoch: handoff.epoch,
        threads: handoff.threads,
        sort_memory_mib: handoff.sort_memory_mib,
        level: handoff.level,
        source_anchor_sha256: handoff.source_anchor_sha256.clone(),
        source_dir: source.display().to_string(),
        target_dir: target.display().to_string(),
        source_generation_sha256: generation_digest(&source_files),
        target_generation_sha256: generation_digest(&target_files),
        source_files,
        target_files,
        source_registry_keys: handoff.source_registry_keys,
        target_registry_keys: handoff.target_registry_keys,
        eligible_references: handoff.eligible_references,
        source_semantics: None,
        target_semantics: None,
        rewrite_stats: Some(handoff.rewrite_stats.clone()),
        attempt_id: Some(handoff.attempt_id.clone()),
        handoff_sha256: Some(handoff_binding.sha256),
        assembly_mode: Some(ACCESS_ASSEMBLY_MODE.to_owned()),
        signature_provenance: Some(SIGNATURE_PROVENANCE.to_owned()),
        wire_profile: Some(handoff.wire_profile),
        access_boundary_repair: access.boundary_repair,
    };
    validate_receipt_shape(&receipt, options.epoch)?;
    sync_bound_files_without_signatures(&staging, &receipt.target_files)?;
    write_receipt_temp(&staging, &receipt)?;
    remove_internal_staging_files(&staging)?;
    promote_receipt_temp(&staging)?;
    sync_directory(&staging)?;
    let assembly_elapsed = assembly_started.elapsed();
    ensure!(
        !target.try_exists()?,
        "registry reprocess target appeared before publication: {}",
        target.display()
    );
    prepare_staging_directory_for_publication(&staging)?;
    let publish_started = Instant::now();
    publish_directory_no_replace(&staging, &target)?;
    sync_directory(target.parent().context("target has no parent")?)?;
    let publish_elapsed = publish_started.elapsed();
    info!(
        elapsed_secs = started.elapsed().as_secs_f64(),
        access_remap_secs = remap_elapsed.as_secs_f64(),
        independent_sidecar_copy_secs = copy_elapsed.as_secs_f64(),
        signature_link_secs = link_elapsed.as_secs_f64(),
        final_assembly_secs = assembly_elapsed.as_secs_f64(),
        publish_secs = publish_elapsed.as_secs_f64(),
        source_access_bytes = access.source_blob.bytes,
        target_access_bytes = access.target_blob.bytes,
        signature_bytes = access.signatures.bytes,
        target = %target.display(),
        "completed staged registry access remap and publication"
    );
    Ok(receipt)
}

fn validate_private_staging_directory(staging: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(staging)
        .with_context(|| format!("inspect staging {}", staging.display()))?;
    ensure!(
        metadata.file_type().is_dir(),
        "staging is not a non-symlink directory"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            metadata.mode() & 0o777 == 0o700,
            "staging directory must have mode 0700"
        );
        // SAFETY: geteuid has no preconditions.
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() },
            "staging directory is not owned by this process user"
        );
    }
    Ok(())
}

fn validate_access_staging_directory(staging: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(staging)
        .with_context(|| format!("inspect staging {}", staging.display()))?;
    ensure!(
        metadata.file_type().is_dir(),
        "staging is not a non-symlink directory"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions.
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() },
            "staging directory is not owned by this process user"
        );
        let mode = metadata.mode() & 0o777;
        ensure!(
            mode == 0o700
                || (mode == PUBLISHED_GENERATION_MODE
                    && staging.join(REGISTRY_REPROCESS_RECEIPT_FILE).try_exists()?
                    && !staging
                        .join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE)
                        .try_exists()?),
            "access staging has unsafe mode {mode:#o}"
        );
    }
    Ok(())
}

#[cfg(unix)]
fn prepare_staging_directory_for_publication(staging: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        staging,
        fs::Permissions::from_mode(PUBLISHED_GENERATION_MODE),
    )
    .with_context(|| format!("set published generation mode on {}", staging.display()))?;
    sync_directory(staging)
}

#[cfg(not(unix))]
fn prepare_staging_directory_for_publication(staging: &Path) -> Result<()> {
    sync_directory(staging)
}

fn validate_published_generation_mode(target: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(target)?;
        ensure!(
            metadata.file_type().is_dir() && metadata.mode() & 0o777 == PUBLISHED_GENERATION_MODE,
            "published generation directory must have mode 0755"
        );
    }
    Ok(())
}

fn ensure_handoff_matches_access_options(
    handoff: &RegistryReprocessHandoff,
    options: &RegistryReprocessAccessOptions,
    source: &Path,
    staging: &Path,
    target: &Path,
) -> Result<()> {
    ensure!(
        handoff.epoch == options.epoch
            && handoff.attempt_id == options.attempt_id
            && Path::new(&handoff.source_dir) == source
            && Path::new(&handoff.staging_dir) == staging
            && Path::new(&handoff.target_dir) == target
            && handoff.wire_profile == options.wire_profile
            && handoff.wire_profile_authority.recovery_receipt_path()
                == options.wire_profile_authority_receipt.as_deref(),
        "handoff identity does not match the exact access-completion request"
    );
    Ok(())
}

fn ensure_v3_attempt_matches(
    receipt: &RegistryReprocessReceipt,
    options: &RegistryReprocessAccessOptions,
    source: &Path,
    target: &Path,
) -> Result<()> {
    ensure!(
        receipt.version == RECEIPT_VERSION
            && receipt.attempt_id.as_deref() == Some(options.attempt_id.as_str())
            && receipt.handoff_sha256.as_deref() == Some(options.handoff_sha256.as_str())
            && receipt.epoch == options.epoch
            && Path::new(&receipt.source_dir) == source
            && Path::new(&receipt.target_dir) == target
            && receipt.wire_profile == Some(options.wire_profile),
        "published receipt does not match the exact access-completion attempt"
    );
    Ok(())
}

fn insert_unique_binding(
    bindings: &mut BTreeMap<String, FileBinding>,
    name: &str,
    binding: FileBinding,
) -> Result<()> {
    ensure!(
        bindings.insert(name.to_owned(), binding).is_none(),
        "duplicate artifact binding {name}"
    );
    Ok(())
}

fn validate_hot_index_geometry_for_access(
    source: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    target: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
) -> Result<()> {
    ensure!(
        source.len() == target.len(),
        "source and target hot indices have different row counts"
    );
    for (position, (source, target)) in source.iter().zip(target).enumerate() {
        ensure!(
            source.block_id as usize == position
                && target.block_id as usize == position
                && source.block_id == target.block_id
                && source.slot == target.slot
                && source.tx_count == target.tx_count
                && source.first_tx_ordinal == target.first_tx_ordinal
                && source.first_signature_ordinal == target.first_signature_ordinal
                && source.signature_count == target.signature_count,
            "source/target hot-index geometry mismatch at block_id {}",
            source.block_id
        );
    }
    Ok(())
}

fn validate_mapped_registry_remap(
    source: &MappedRegistry,
    remap: &MappedRegistryRemap,
    target: &MappedRegistry,
    target_counts: &[u32],
) -> Result<()> {
    ensure!(source.len == remap.source_keys);
    ensure!(target.len as u64 == remap.target_keys);
    ensure!(target.len == target_counts.len());
    let mut retained = 0usize;
    let mut mapped_target_ids = vec![false; target.len + 1];
    let builtin = compute_budget_key();
    let mut source_has_builtin = false;
    for old_index in 0..source.len {
        let old_id = u32::try_from(old_index + 1).context("source registry ID exceeds u32")?;
        source_has_builtin |= source.key(old_id)? == builtin;
        let new_id = remap.get(old_id)?;
        if new_id == 0 {
            continue;
        }
        let new_index = usize::try_from(new_id)
            .ok()
            .filter(|&id| id != 0 && id <= target.len)
            .context("registry remap target ID is outside the target registry")?;
        ensure!(
            !std::mem::replace(&mut mapped_target_ids[new_index], true),
            "multiple source IDs map to target ID {new_id}"
        );
        ensure!(
            new_index <= target.len,
            "registry remap target ID {new_id} is outside the target registry"
        );
        ensure!(
            source.key(old_id)? == target.key(new_id)?,
            "registry remap changes the key for source ID {old_id}"
        );
        retained = retained
            .checked_add(1)
            .context("retained registry key count overflow")?;
    }
    if retained == target.len {
        ensure!(mapped_target_ids[1..].iter().all(|mapped| *mapped));
    } else {
        ensure!(
            retained.checked_add(1) == Some(target.len)
                && !source_has_builtin
                && target.keys().first() == Some(&builtin)
                && target_counts.first() == Some(&0)
                && !mapped_target_ids[1]
                && mapped_target_ids[2..].iter().all(|mapped| *mapped),
            "registry remap has an invalid unmatched target key"
        );
    }
    Ok(())
}

fn phase2_output_names() -> impl Iterator<Item = &'static str> {
    [
        ARCHIVE_V2_BLOCK_ACCESS_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
    ]
    .into_iter()
    .chain(INDEPENDENT_SIDECARS.iter().copied())
}

fn remove_regular_if_present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_file(),
                "owned phase-2 path is not a regular file: {}",
                path.display()
            );
            fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
            Ok(true)
        }
    }
}

fn remove_owned_phase2_outputs(staging: &Path) -> Result<()> {
    ensure!(
        !staging.join(REGISTRY_REPROCESS_RECEIPT_FILE).try_exists()?
            && !staging
                .join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE)
                .try_exists()?,
        "cannot reset phase-2 outputs after receipt preparation"
    );
    let mut changed = false;
    for name in phase2_output_names() {
        changed |= remove_regular_if_present(&staging.join(name))?;
        changed |= remove_regular_if_present(&staging.join(format!("{name}{ACCESS_TEMP_SUFFIX}")))?;
    }
    if changed {
        sync_directory(staging)?;
    }
    Ok(())
}

fn remove_internal_staging_files(staging: &Path) -> Result<()> {
    for name in [
        REPROCESS_CHECKPOINT_FILE,
        REPROCESS_HANDOFF_FILE,
        REPROCESS_REMAP_FILE,
        SOURCE_REGISTRY_SNAPSHOT_FILE,
    ] {
        remove_regular_if_present(&staging.join(name))?;
    }
    Ok(())
}

fn sync_bound_files_without_signatures(
    directory: &Path,
    bindings: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    for (name, binding) in bindings {
        let path = directory.join(name);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("inspect final artifact {}", path.display()))?;
        ensure!(
            metadata.file_type().is_file() && metadata.len() == binding.bytes,
            "final artifact shape mismatch for {name}"
        );
        if name == ARCHIVE_V2_SIGNATURES_FILE {
            continue;
        }
        File::open(&path)
            .with_context(|| format!("open {name} for sync"))?
            .sync_all()
            .with_context(|| format!("sync {name}"))?;
    }
    sync_directory(directory)
}

fn probe_binding_sizes_without_signature_data(
    directory: &Path,
    files: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    for (name, binding) in files {
        let path = directory.join(name);
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("inspect published artifact {}", path.display()))?;
        ensure!(
            metadata.file_type().is_file() && metadata.len() == binding.bytes,
            "published artifact size mismatch for {name}"
        );
    }
    Ok(())
}

fn probe_published_reprocess_without_signature_data(
    target: &Path,
    epoch: u64,
) -> Result<RegistryReprocessReceipt> {
    let target = fs::canonicalize(target)
        .with_context(|| format!("canonicalize published target {}", target.display()))?;
    let receipt = read_receipt(&target)?;
    validate_receipt_shape(&receipt, epoch)?;
    ensure!(Path::new(&receipt.target_dir) == target);
    ensure!(
        generation_digest(&receipt.target_files) == receipt.target_generation_sha256
            && generation_digest(&receipt.source_files) == receipt.source_generation_sha256,
        "published generation digest mismatch"
    );
    validate_probe_core_files(&receipt.target_files, false, receipt.version)?;
    validate_probe_core_files(&receipt.source_files, true, receipt.version)?;
    probe_binding_sizes_without_signature_data(&target, &receipt.target_files)?;
    let source = fs::canonicalize(&receipt.source_dir)?;
    ensure!(source == Path::new(&receipt.source_dir));
    probe_binding_sizes_without_signature_data(&source, &receipt.source_files)?;
    validate_signature_hard_link_metadata(
        &source,
        &target,
        receipt
            .target_files
            .get(ARCHIVE_V2_SIGNATURES_FILE)
            .context("v3 receipt omits signatures binding")?
            .bytes,
    )?;
    validate_staged_receipt_entry_allowlist(&target, &receipt, true)?;
    validate_published_generation_mode(&target)?;
    Ok(receipt)
}

fn validate_bound_files_except_blocks_and_signatures(
    directory: &Path,
    expected: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    for (name, expected) in expected {
        if matches!(
            name.as_str(),
            ARCHIVE_V2_BLOCKS_FILE | ARCHIVE_V2_SIGNATURES_FILE
        ) {
            continue;
        }
        let actual = hash_file(&directory.join(name))?;
        ensure!(&actual == expected, "artifact binding mismatch for {name}");
    }
    Ok(())
}

fn resume_staged_publication_without_signature_data(
    options: &RegistryReprocessAccessOptions,
    source: &Path,
    staging: &Path,
    target: &Path,
) -> Result<RegistryReprocessReceipt> {
    let receipt = read_receipt(staging)?;
    validate_staged_receipt_without_signature_data(
        options, source, staging, target, &receipt, true,
    )?;
    remove_internal_staging_files(staging)?;
    sync_directory(staging)?;
    ensure!(!target.try_exists()?);
    prepare_staging_directory_for_publication(staging)?;
    publish_directory_no_replace(staging, target)?;
    sync_directory(target.parent().context("target has no parent")?)?;
    Ok(receipt)
}

fn resume_staged_receipt_temp_without_signature_data(
    options: &RegistryReprocessAccessOptions,
    source: &Path,
    staging: &Path,
    target: &Path,
) -> Result<RegistryReprocessReceipt> {
    let receipt = read_receipt_temp(staging)?;
    validate_staged_receipt_without_signature_data(
        options, source, staging, target, &receipt, false,
    )?;
    remove_internal_staging_files(staging)?;
    promote_receipt_temp(staging)?;
    sync_directory(staging)?;
    ensure!(!target.try_exists()?);
    prepare_staging_directory_for_publication(staging)?;
    publish_directory_no_replace(staging, target)?;
    sync_directory(target.parent().context("target has no parent")?)?;
    Ok(receipt)
}

fn validate_staged_receipt_without_signature_data(
    options: &RegistryReprocessAccessOptions,
    source: &Path,
    staging: &Path,
    target: &Path,
    receipt: &RegistryReprocessReceipt,
    final_receipt: bool,
) -> Result<()> {
    validate_receipt_shape(&receipt, options.epoch)?;
    ensure_v3_attempt_matches(&receipt, options, source, target)?;
    ensure!(
        generation_digest(&receipt.source_files) == receipt.source_generation_sha256
            && generation_digest(&receipt.target_files) == receipt.target_generation_sha256,
        "staged receipt generation digest mismatch"
    );
    probe_binding_sizes_without_signature_data(source, &receipt.source_files)?;
    probe_binding_sizes_without_signature_data(staging, &receipt.target_files)?;
    validate_bound_files_except_blocks_and_signatures(source, &receipt.source_files)?;
    validate_bound_files_except_blocks_and_signatures(staging, &receipt.target_files)?;
    validate_signature_hard_link_metadata(
        source,
        staging,
        receipt
            .target_files
            .get(ARCHIVE_V2_SIGNATURES_FILE)
            .context("v3 receipt omits signatures binding")?
            .bytes,
    )?;
    validate_staged_receipt_entry_allowlist(staging, receipt, final_receipt)
}

/// Cheap bounded steady-state probe.  This parses only the publication-last receipt and checks
/// its identity fields; it deliberately does not hash archive payloads.
pub(crate) fn probe_published_reprocess(
    target: &Path,
    epoch: u64,
) -> Result<RegistryReprocessReceipt> {
    let target = fs::canonicalize(target)
        .with_context(|| format!("canonicalize published target {}", target.display()))?;
    let receipt = read_receipt(&target)?;
    validate_receipt_shape(&receipt, epoch)?;
    ensure!(
        Path::new(&receipt.target_dir) == target,
        "receipt target_dir={} does not identify published generation {}",
        receipt.target_dir,
        target.display()
    );
    ensure!(
        generation_digest(&receipt.target_files) == receipt.target_generation_sha256,
        "target generation digest mismatch in receipt"
    );
    ensure!(
        generation_digest(&receipt.source_files) == receipt.source_generation_sha256,
        "source input digest mismatch in receipt"
    );
    validate_probe_core_files(&receipt.target_files, false, receipt.version)?;
    validate_probe_core_files(&receipt.source_files, true, receipt.version)?;
    let source = fs::canonicalize(&receipt.source_dir)
        .with_context(|| format!("canonicalize receipt source {}", receipt.source_dir))?;
    ensure!(source == Path::new(&receipt.source_dir));
    if receipt.version == RECEIPT_VERSION {
        probe_binding_sizes_without_signature_data(&target, &receipt.target_files)?;
        probe_binding_sizes_without_signature_data(&source, &receipt.source_files)?;
        validate_signature_hard_link_metadata(
            &source,
            &target,
            receipt
                .target_files
                .get(ARCHIVE_V2_SIGNATURES_FILE)
                .context("v3 receipt omits signatures binding")?
                .bytes,
        )?;
        validate_staged_receipt_entry_allowlist(&target, &receipt, true)?;
        validate_published_generation_mode(&target)?;
    } else {
        probe_binding_sizes(&target, &receipt.target_files)?;
        probe_binding_sizes(&source, &receipt.source_files)?;
    }
    Ok(receipt)
}

fn validate_probe_core_files(
    files: &BTreeMap<String, FileBinding>,
    first_seen_source: bool,
    receipt_version: u32,
) -> Result<()> {
    for name in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_POH_FILE,
        ARCHIVE_V2_SHREDDING_FILE,
    ] {
        ensure!(
            files.contains_key(name),
            "receipt omits core artifact {name}"
        );
    }
    ensure!(
        files.contains_key(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE) == first_seen_source,
        "receipt first-seen manifest presence mismatch"
    );
    let blob = files.contains_key(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let index = files.contains_key(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let get_block = files.contains_key(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
    if first_seen_source && receipt_version == RECEIPT_VERSION_V2 {
        ensure!(
            !blob && !index && !get_block,
            "v2 source inputs include unused block-access artifacts"
        );
        return Ok(());
    }
    if first_seen_source && receipt_version == RECEIPT_VERSION {
        ensure!(
            blob && index && !get_block,
            "v3 source receipt requires only the trusted block-access blob/index"
        );
    }
    ensure!(
        blob == index,
        "receipt contains only one block-access artifact"
    );
    if !first_seen_source {
        ensure!(
            get_block == blob,
            "target receipt contains an incomplete block-access/get-block artifact set"
        );
    }
    Ok(())
}

fn probe_binding_sizes(directory: &Path, files: &BTreeMap<String, FileBinding>) -> Result<()> {
    for (name, binding) in files {
        let (file, metadata) = open_regular_read(&directory.join(name))?;
        ensure!(
            metadata.len() == binding.bytes,
            "published artifact size mismatch for {name}"
        );
        ensure_open_file_unchanged(&directory.join(name), &file, &metadata)?;
    }
    Ok(())
}

/// Deep restart/exit validation. This authenticates every bound source input and target file,
/// checks canonical registry order and metadata flags, and repeats semantic normalization from
/// both generations. It is intentionally unsuitable for a five-second scheduler poll.
pub(crate) fn validate_published_reprocess(
    source: &Path,
    target: &Path,
    epoch: u64,
) -> Result<RegistryReprocessReceipt> {
    let source = fs::canonicalize(source)
        .with_context(|| format!("canonicalize source {}", source.display()))?;
    let target = fs::canonicalize(target)
        .with_context(|| format!("canonicalize target {}", target.display()))?;
    let receipt = probe_published_reprocess(&target, epoch)?;
    ensure!(
        Path::new(&receipt.source_dir) == source,
        "receipt source_dir={} does not identify source generation {}",
        receipt.source_dir,
        source.display()
    );
    validate_bound_files_for_deep(&source, &receipt.source_files, receipt.version)?;
    validate_bound_files_for_deep(&target, &receipt.target_files, receipt.version)?;
    if receipt.access_boundary_repair.is_some() {
        // The source deep scan above has just hashed the actual blob and matched it to this
        // receipt binding. Pin that authenticated binding to the diagnosed production blob.
        validate_epoch_301_source_access_blob_binding(
            receipt
                .source_files
                .get(ARCHIVE_V2_BLOCK_ACCESS_FILE)
                .context("repaired deep validation omits the source access blob")?,
        )?;
    }
    ensure!(
        generation_digest(&receipt.source_files) == receipt.source_generation_sha256,
        "source input digest mismatch in receipt"
    );
    ensure!(
        generation_digest(&receipt.target_files) == receipt.target_generation_sha256,
        "target generation digest mismatch in receipt"
    );
    validate_canonical_registry(&target, receipt.target_registry_keys)?;
    validate_target_meta(&target)?;
    let (source_semantics, source_blocks) =
        recompute_source_canonical_counts(&source, &target, &receipt, epoch)?;
    ensure!(
        receipt.source_files.get(ARCHIVE_V2_BLOCKS_FILE) == Some(&source_blocks),
        "source block artifact binding mismatch"
    );
    let wire_profile = receipt
        .wire_profile
        .context("registry receipt has no admitted Archive V2 wire profile")?;
    let (target_semantics, target_blocks) =
        scan_target_generation_semantics(&target, epoch, receipt.threads, wire_profile)?;
    ensure!(
        receipt.target_files.get(ARCHIVE_V2_BLOCKS_FILE) == Some(&target_blocks),
        "target block artifact binding mismatch"
    );
    ensure!(
        source_semantics == target_semantics,
        "published semantic parity mismatch"
    );
    match receipt.version {
        RECEIPT_VERSION_V1 => {
            ensure!(
                receipt.source_semantics.as_ref() == Some(&source_semantics),
                "source semantic receipt mismatch"
            );
            ensure!(
                receipt.target_semantics.as_ref() == Some(&target_semantics),
                "target semantic receipt mismatch"
            );
        }
        RECEIPT_VERSION_V2 | RECEIPT_VERSION => {
            let expected = RewriteStats {
                blocks: source_semantics.blocks,
                transactions: source_semantics.transactions,
                pubkey_references: source_semantics.pubkey_references,
            };
            ensure!(
                receipt.rewrite_stats.as_ref() == Some(&expected),
                "rewrite-stat receipt mismatch"
            );
        }
        _ => unreachable!("receipt shape rejects unsupported versions"),
    }
    if receipt.version == RECEIPT_VERSION {
        validate_v3_access_remap(&source, &target, &receipt)?;
    }
    Ok(receipt)
}

fn validate_options(options: &RegistryReprocessOptions) -> Result<()> {
    ensure!(
        options.epoch != 0,
        "first-seen registry reprocessing does not support epoch 0 genesis"
    );
    ensure!(options.threads > 0, "--threads must be greater than zero");
    ensure!(
        options.sort_memory_mib > 0,
        "--sort-memory-mib must be greater than zero"
    );
    validate_attempt_id(&options.attempt_id)?;
    Ok(())
}

fn validate_attempt_id(attempt_id: &str) -> Result<()> {
    ensure!(
        !attempt_id.is_empty()
            && attempt_id.len() <= 64
            && attempt_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')),
        "attempt ID must contain 1..=64 ASCII letters, digits, '-' or '_'"
    );
    Ok(())
}

fn canonical_target_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolve current directory")?
            .join(path)
    };
    let name = absolute
        .file_name()
        .ok_or_else(|| anyhow!("target has no final path component: {}", absolute.display()))?;
    let parent = absolute
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| anyhow!("target has no parent: {}", absolute.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create target parent {}", parent.display()))?;
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("canonicalize target parent {}", parent.display()))?;
    Ok(parent.join(name))
}

fn expected_staging_path(target: &Path, attempt_id: &str) -> Result<PathBuf> {
    validate_attempt_id(attempt_id)?;
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("target name is not valid UTF-8: {}", target.display()))?;
    Ok(parent.join(format!(".{name}.registry-reprocess.{attempt_id}.staging")))
}

fn canonical_staging_path(requested: &Path, target: &Path, attempt_id: &str) -> Result<PathBuf> {
    let absolute = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        std::env::current_dir()
            .context("resolve current directory")?
            .join(requested)
    };
    let parent = absolute
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .context("staging directory has no parent")?;
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("canonicalize staging parent {}", parent.display()))?;
    let name = absolute
        .file_name()
        .context("staging directory has no final component")?;
    let canonical = parent.join(name);
    let expected = expected_staging_path(target, attempt_id)?;
    ensure!(
        canonical == expected,
        "staging directory must be the exact attempt-bound sibling {}",
        expected.display()
    );
    Ok(canonical)
}

fn build_checkpoint(
    source: &Path,
    target: &Path,
    staging: &Path,
    options: &RegistryReprocessOptions,
) -> Result<ReprocessCheckpoint> {
    let validated = validate_source_wire_profile_authority(
        source,
        target,
        options.epoch,
        options.wire_profile,
        options.wire_profile_authority_receipt.as_deref(),
    )?;
    build_checkpoint_with_authority(
        source,
        target,
        staging,
        options,
        validated.authority,
        validated.audited_marker_free_source.as_ref(),
    )
}

fn rebuild_checkpoint_against_authority(
    source: &Path,
    target: &Path,
    staging: &Path,
    options: &RegistryReprocessOptions,
    expected_authority: &SourceWireProfileAuthority,
) -> Result<ReprocessCheckpoint> {
    let resolved = resolve_source_wire_profile_authority(
        source,
        target,
        options.epoch,
        options.wire_profile,
        options.wire_profile_authority_receipt.as_deref(),
    )?;
    ensure!(
        &resolved.authority == expected_authority,
        "source wire-profile authority changed after initial admission"
    );
    build_checkpoint_with_authority(source, target, staging, options, resolved.authority, None)
}

fn build_checkpoint_with_authority(
    source: &Path,
    target: &Path,
    staging: &Path,
    options: &RegistryReprocessOptions,
    wire_profile_authority: SourceWireProfileAuthority,
    audited_marker_free_source: Option<&PinnedLocalRangeSource>,
) -> Result<ReprocessCheckpoint> {
    if let Some(pinned) = audited_marker_free_source {
        ensure!(
            pinned.root() == source,
            "audited marker-free descriptor view belongs to a different source"
        );
    }
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla.registry-reprocess.source-anchor.v2");
    let wire_profile = options.wire_profile.to_string();
    hasher.update((wire_profile.len() as u64).to_le_bytes());
    hasher.update(wire_profile.as_bytes());
    let authority_bytes = serde_json::to_vec(&wire_profile_authority)?;
    hasher.update((authority_bytes.len() as u64).to_le_bytes());
    hasher.update(&authority_bytes);
    for name in [
        ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
    ] {
        let binding = if let Some(pinned) = audited_marker_free_source {
            hash_pinned_source_file(pinned, name)?
        } else {
            hash_file(&source.join(name))?
        };
        validate_source_binding_against_profile_authority(&wire_profile_authority, name, &binding)?;
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(binding.bytes.to_le_bytes());
        hasher.update(binding.sha256.as_bytes());
    }
    for name in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        let metadata = if let Some(pinned) = audited_marker_free_source {
            pinned_source_file_metadata(pinned, name)?
        } else {
            regular_file_metadata(&source.join(name))?
        };
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(metadata.len().to_le_bytes());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            hasher.update(metadata.dev().to_le_bytes());
            hasher.update(metadata.ino().to_le_bytes());
            hasher.update(metadata.mtime().to_le_bytes());
            hasher.update(metadata.mtime_nsec().to_le_bytes());
            hasher.update(metadata.ctime().to_le_bytes());
            hasher.update(metadata.ctime_nsec().to_le_bytes());
        }
    }
    let checkpoint = ReprocessCheckpoint {
        version: REPROCESS_CHECKPOINT_VERSION,
        algorithm: RECEIPT_ALGORITHM.to_owned(),
        source_dir: source.display().to_string(),
        target_dir: target.display().to_string(),
        epoch: options.epoch,
        threads: options.threads,
        sort_memory_mib: options.sort_memory_mib,
        level: options.level,
        source_anchor_sha256: hex_digest(hasher.finalize()),
        attempt_id: options.attempt_id.clone(),
        staging_dir: staging.display().to_string(),
        wire_profile: options.wire_profile,
        wire_profile_authority,
    };
    if let Some(pinned) = audited_marker_free_source {
        pinned
            .verify_unchanged()
            .map_err(|error| anyhow!(error))
            .context("marker-free source changed between its audit and initial checkpoint")?;
    }
    Ok(checkpoint)
}

fn validate_source_wire_profile_authority(
    source: &Path,
    target: &Path,
    epoch: u64,
    selected: ArchiveV2WireProfile,
    recovery_receipt: Option<&Path>,
) -> Result<ValidatedSourceWireProfileAuthority> {
    let resolved =
        resolve_source_wire_profile_authority(source, target, epoch, selected, recovery_receipt)?;
    let audited_marker_free_source =
        audit_admitted_source_wire_profile(source, epoch, selected, &resolved)?;
    Ok(ValidatedSourceWireProfileAuthority {
        authority: resolved.authority,
        audited_marker_free_source,
    })
}

fn resolve_source_wire_profile_authority(
    source: &Path,
    target: &Path,
    epoch: u64,
    selected: ArchiveV2WireProfile,
    recovery_receipt: Option<&Path>,
) -> Result<ResolvedSourceWireProfileAuthority> {
    let mut selected_marker = None;
    for profile in [
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
    ] {
        let marker = wire_profile_marker(profile);
        let path = source.join(&marker.name);
        if !path.try_exists()? {
            continue;
        }
        ensure!(
            profile == selected,
            "source contains the conflicting Archive V2 wire-profile marker {}",
            marker.name
        );
        let actual = hash_file(&path)?;
        ensure!(
            actual.bytes == marker.size && actual.sha256 == marker.sha256,
            "source Archive V2 wire-profile marker {} has the wrong binding",
            marker.name
        );
        let bytes = fs::read(&path)?;
        ensure!(
            bytes == wire_profile_marker_bytes(profile),
            "source Archive V2 wire-profile marker {} has the wrong bytes",
            marker.name
        );
        ensure!(
            selected_marker.is_none(),
            "source contains conflicting Archive V2 wire-profile markers"
        );
        selected_marker = Some(FileBinding {
            bytes: marker.size,
            sha256: marker.sha256,
        });
    }

    let generation_manifest_path = source.join(GENERATION_MANIFEST_FILE);
    let resolved = if generation_manifest_path.try_exists()? {
        ensure!(
            recovery_receipt.is_none(),
            "published-manifest source must not also claim a recovery receipt authority"
        );
        let authority =
            read_published_manifest_authority(epoch, selected, &generation_manifest_path)?;
        validate_source_wire_profile_authority_shape(epoch, selected, &authority)?;
        ResolvedSourceWireProfileAuthority {
            authority,
            marker_free_evidence: None,
        }
    } else if selected_marker.is_some() {
        bail!(
            "source wire-profile marker is not authority by itself; publish an authenticated generation manifest that binds the exact source"
        );
    } else if let Some(receipt) = recovery_receipt {
        validate_profile_neutral_recovery_authority(
            source,
            target,
            epoch,
            selected,
            receipt,
            &PROFILE_NEUTRAL_RECOVERY_AUTHORITIES,
            PROFILE_NEUTRAL_RECOVERY_AUTHORITY_SHA256,
        )?
    } else if epoch == 1 && selected == ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1 {
        let blocks = hash_file(&source.join(ARCHIVE_V2_BLOCKS_FILE))?;
        let block_index = hash_file(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
        if blocks.sha256 == MAY_24_2026_MAINNET_EPOCH_1_BLOCKS_SHA256
            && block_index.sha256 == MAY_24_2026_MAINNET_EPOCH_1_INDEX_SHA256
        {
            ResolvedSourceWireProfileAuthority {
                authority: SourceWireProfileAuthority::PinnedHistoricalIdentity {
                    epoch,
                    blocks,
                    block_index,
                },
                marker_free_evidence: Some(
                    MarkerFreeSourceAuthorityEvidence::PinnedHistoricalIdentity,
                ),
            }
        } else {
            bail!(
                "marker-free source has no generation-bound Archive V2 wire-profile authority; provide the exact reviewed recovery receipt or use a pinned historical identity"
            );
        }
    } else {
        bail!(
            "marker-free source has no generation-bound Archive V2 wire-profile authority; provide the exact reviewed recovery receipt or use a pinned historical identity"
        );
    };

    validate_source_wire_profile_authority_shape(epoch, selected, &resolved.authority)?;
    Ok(resolved)
}

fn read_published_manifest_authority(
    epoch: u64,
    selected: ArchiveV2WireProfile,
    manifest_path: &Path,
) -> Result<SourceWireProfileAuthority> {
    let (mut file, metadata) = open_regular_read(manifest_path)?;
    ensure!(
        metadata.len() > 0 && metadata.len() <= GENERATION_MANIFEST_MAX_BYTES,
        "published generation manifest must be a non-empty regular file no larger than {GENERATION_MANIFEST_MAX_BYTES} bytes"
    );
    let capacity = usize::try_from(metadata.len())
        .context("published generation manifest is too large for this host")?;
    let mut bytes = Vec::with_capacity(capacity);
    (&mut file)
        .take(GENERATION_MANIFEST_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == metadata.len(),
        "published generation manifest changed length while reading"
    );
    ensure_open_file_unchanged(manifest_path, &file, &metadata)?;

    let manifest = GenerationManifest::parse(&bytes)
        .map_err(|error| anyhow!(error))
        .context("parse published generation manifest authority")?;
    let authority = SourceWireProfileAuthority::PublishedManifest {
        manifest_file: FileBinding {
            bytes: metadata.len(),
            sha256: hex_digest(Sha256::digest(&bytes)),
        },
        manifest,
    };
    validate_source_wire_profile_authority_shape(epoch, selected, &authority)?;

    Ok(authority)
}

fn pinned_source_file_metadata(
    source: &PinnedLocalRangeSource,
    name: &str,
) -> Result<fs::Metadata> {
    let file = source
        .open_file(name)
        .map_err(|error| anyhow!(error))
        .with_context(|| format!("open pinned source artifact {name}"))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("stat pinned source artifact {name}"))?;
    ensure!(
        metadata.file_type().is_file(),
        "pinned source artifact is not a regular file: {name}"
    );
    Ok(metadata)
}

fn hash_pinned_source_file(source: &PinnedLocalRangeSource, name: &str) -> Result<FileBinding> {
    let mut file = source
        .open_file(name)
        .map_err(|error| anyhow!(error))
        .with_context(|| format!("open pinned source artifact {name}"))?;
    let before = file
        .metadata()
        .with_context(|| format!("stat pinned source artifact {name}"))?;
    ensure!(
        before.file_type().is_file(),
        "pinned source artifact is not a regular file: {name}"
    );
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut bytes = 0u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("pinned source artifact byte count overflow")?;
    }
    let after = reader.get_ref().metadata()?;
    ensure!(
        bytes == before.len() && same_file_snapshot(&before, &after),
        "pinned source artifact changed while hashing: {name}"
    );
    Ok(FileBinding {
        bytes,
        sha256: hex_digest(hasher.finalize()),
    })
}

fn pin_marker_free_source_against_authority(
    source: &Path,
    resolved: &ResolvedSourceWireProfileAuthority,
) -> Result<PinnedLocalRangeSource> {
    let pinned = PinnedLocalRangeSource::new(source);
    for name in SOURCE_ANCHOR_FILES {
        pinned_source_file_metadata(&pinned, name)?;
    }

    match (&resolved.authority, &resolved.marker_free_evidence) {
        (
            SourceWireProfileAuthority::PinnedHistoricalIdentity {
                blocks,
                block_index,
                ..
            },
            Some(MarkerFreeSourceAuthorityEvidence::PinnedHistoricalIdentity),
        ) => {
            ensure!(
                hash_pinned_source_file(&pinned, ARCHIVE_V2_BLOCKS_FILE)? == *blocks
                    && hash_pinned_source_file(&pinned, ARCHIVE_V2_BLOCK_INDEX_FILE)?
                        == *block_index,
                "pinned historical audit descriptors differ from the reviewed source bindings"
            );
        }
        (
            SourceWireProfileAuthority::ProfileNeutralRecoveryReceipt {
                legacy_source_files,
                ..
            },
            Some(MarkerFreeSourceAuthorityEvidence::ProfileNeutralRecoveryReceipt {
                source_file_identities,
            }),
        ) => {
            ensure!(
                source_file_identities.len() == legacy_source_files.len()
                    && source_file_identities.keys().eq(legacy_source_files.keys()),
                "recovery audit descriptor evidence differs from the receipt file set"
            );
            for name in SOURCE_ANCHOR_FILES {
                ensure!(
                    source_file_identities.contains_key(name),
                    "recovery audit descriptor evidence omits source anchor artifact {name}"
                );
            }
            for (name, expected) in source_file_identities {
                let actual =
                    profile_authority_file_identity(&pinned_source_file_metadata(&pinned, name)?);
                ensure!(
                    &actual == expected,
                    "recovery audit descriptor differs from attested source identity: {name}"
                );
            }
        }
        _ => bail!("marker-free authority and descriptor evidence kind disagree"),
    }

    pinned
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("marker-free source changed while binding audit descriptors")?;
    Ok(pinned)
}

fn audit_admitted_source_wire_profile(
    source: &Path,
    epoch: u64,
    selected: ArchiveV2WireProfile,
    resolved: &ResolvedSourceWireProfileAuthority,
) -> Result<Option<PinnedLocalRangeSource>> {
    let authority = &resolved.authority;
    let (reader, audited_marker_free_source) = match authority {
        SourceWireProfileAuthority::PublishedManifest { manifest, .. } => {
            ensure!(
                resolved.marker_free_evidence.is_none(),
                "published authority unexpectedly contains marker-free evidence"
            );
            let pinned_source = PinnedLocalRangeSource::new(source);
            let options = ReaderOpenOptions {
                hash_verification: HashVerification::AllFiles,
                ..ReaderOpenOptions::default()
            };
            (
                ArchiveReader::open_candidate_with_metadata_admission(
                    pinned_source,
                    manifest.clone(),
                    options,
                    ArchiveV2MetadataProfileAdmission::AllowUnmarkedHistorical,
                )
                .map_err(|error| anyhow!(error))
                .context("authenticate and validate the published source generation")?,
                None,
            )
        }
        SourceWireProfileAuthority::PinnedHistoricalIdentity { .. }
        | SourceWireProfileAuthority::ProfileNeutralRecoveryReceipt { .. } => {
            let pinned_source = pin_marker_free_source_against_authority(source, resolved)
                .context(
                    "bind marker-free audit descriptors to the exact source authority evidence",
                )?;
            let generation_id = match authority {
                SourceWireProfileAuthority::PinnedHistoricalIdentity { .. } => {
                    "registry-reprocess-pinned-historical"
                }
                SourceWireProfileAuthority::ProfileNeutralRecoveryReceipt { .. } => {
                    "registry-reprocess-controlled-recovery"
                }
                SourceWireProfileAuthority::PublishedManifest { .. } => unreachable!(),
            };
            let options = ReaderOpenOptions {
                hash_verification: HashVerification::SizesOnly,
                ..ReaderOpenOptions::default()
            };
            (
                ArchiveReader::open_trusted_with_metadata_profile(
                    pinned_source.clone(),
                    TrustedGenerationIdentity {
                        cluster_id: "mainnet-beta".to_owned(),
                        epoch,
                        generation_id: generation_id.to_owned(),
                        slots_per_epoch: crate::SLOTS_PER_EPOCH,
                        wire_profile: selected,
                    },
                    ArchiveV2MetadataWireProfile::UnmarkedHistoricalCompatibility,
                    options,
                )
                .map_err(|error| anyhow!(error))
                .context("validate the authority-bound marker-free source generation")?,
                Some(pinned_source),
            )
        }
    };
    ensure!(
        !reader.index().rows.is_empty(),
        "authority-bound source generation has no hot blocks"
    );
    let decision = audit_full_generation_wire_profile(&reader, MAX_PROFILE_MESSAGE_BYTES)
        .map_err(|error| anyhow!(error))
        .context("audit the admitted source wire profile")?
        .require_unproven_authority()
        .map_err(|error| anyhow!(error))
        .context("prove the admitted source wire profile from all typed messages")?;
    if decision == UnprovenWireProfileDecision::AllSemanticallyEquivalent {
        ensure!(
            selected == ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            "an all-equivalent generation must use the canonical post-fallback wire profile"
        );
    }
    reader
        .source()
        .verify_unchanged()
        .map_err(|error| anyhow!(error))
        .context("authority-bound source changed during its wire-profile audit")?;
    if let SourceWireProfileAuthority::PublishedManifest { manifest_file, .. } = authority {
        let manifest_after = hash_file(&source.join(GENERATION_MANIFEST_FILE))?;
        ensure!(
            &manifest_after == manifest_file,
            "published generation manifest changed while its source files were authenticated"
        );
    }
    Ok(audited_marker_free_source)
}

#[cfg(unix)]
fn profile_authority_file_identity(metadata: &fs::Metadata) -> ProfileAuthorityFileIdentity {
    use std::os::unix::fs::MetadataExt;
    ProfileAuthorityFileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    }
}

#[cfg(not(unix))]
fn profile_authority_file_identity(_metadata: &fs::Metadata) -> ProfileAuthorityFileIdentity {
    ProfileAuthorityFileIdentity {
        size: 0,
        device: 0,
        inode: 0,
        modified_seconds: 0,
        modified_nanoseconds: 0,
        changed_seconds: 0,
        changed_nanoseconds: 0,
    }
}

fn validate_profile_authority_identity(identity: &ProfileAuthorityFileIdentity) -> Result<()> {
    ensure!(
        identity.size > 0
            && identity.device > 0
            && identity.inode > 0
            && (0..1_000_000_000).contains(&identity.modified_nanoseconds)
            && (0..1_000_000_000).contains(&identity.changed_nanoseconds),
        "profile authority file identity is invalid"
    );
    Ok(())
}

fn validate_protected_profile_authority_path(path: &Path, label: &str) -> Result<()> {
    ensure!(path.is_absolute(), "{label} path is not absolute");
    let parent = path
        .parent()
        .context("profile authority path has no parent")?;
    let parent_metadata = fs::symlink_metadata(parent)
        .with_context(|| format!("inspect {label} parent {}", parent.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            parent_metadata.file_type().is_dir()
                && parent_metadata.uid() == unsafe { libc::geteuid() }
                && parent_metadata.mode() & 0o022 == 0
                && fs::canonicalize(parent)? == parent,
            "{label} parent is not one canonical protected euid-owned directory"
        );
    }
    #[cfg(not(unix))]
    ensure!(
        parent_metadata.file_type().is_dir() && fs::canonicalize(parent)? == parent,
        "{label} parent is not one canonical directory"
    );
    Ok(())
}

fn read_profile_authority_file(
    path: &Path,
    label: &str,
) -> Result<(Vec<u8>, FileBinding, ProfileAuthorityFileIdentity)> {
    validate_protected_profile_authority_path(path, label)?;
    let (mut file, metadata) = open_regular_read(path)?;
    ensure!(
        metadata.len() > 0 && metadata.len() <= MAX_PROFILE_AUTHORITY_BYTES,
        "{label} has an invalid bounded size"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.nlink() == 1
                && metadata.mode() & 0o022 == 0,
            "{label} is not one protected euid-owned file"
        );
    }
    let identity = profile_authority_file_identity(&metadata);
    validate_profile_authority_identity(&identity)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(MAX_PROFILE_AUTHORITY_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == metadata.len(),
        "{label} grew while reading"
    );
    ensure_open_file_unchanged(path, &file, &metadata)?;
    let binding = FileBinding {
        bytes: bytes.len() as u64,
        sha256: hex_digest(Sha256::digest(&bytes)),
    };
    Ok((bytes, binding, identity))
}

fn validate_profile_neutral_attestation_evidence(evidence: &str) -> Result<()> {
    let mut fields = evidence.split(';');
    ensure!(
        fields.next() == Some(PROFILE_NEUTRAL_RECOVERY_EVIDENCE_V3),
        "source profile attestation evidence is not the full-generation v3 contract"
    );
    let mut values = BTreeMap::new();
    for field in fields {
        let (name, value) = field
            .split_once('=')
            .context("source profile attestation evidence field has no value")?;
        ensure!(
            !name.is_empty() && !value.is_empty() && values.insert(name, value).is_none(),
            "source profile attestation evidence has an empty or duplicate field"
        );
    }
    let expected = BTreeSet::from([
        "generation_kind",
        "blocks",
        "messages",
        "raw_transaction_fallbacks",
        "selected_profile_failures",
        "alternate_profile_failures",
        "both_semantically_equivalent",
        "both_semantically_divergent",
        "decision_basis",
        "pinned_inputs_unchanged",
        "exact_input_before_after_equal",
    ]);
    ensure!(
        values.keys().copied().collect::<BTreeSet<_>>() == expected,
        "source profile attestation evidence field set is not exact"
    );
    ensure!(
        values["generation_kind"] == PROFILE_NEUTRAL_RECOVERY_ATTESTATION_GENERATION_KIND
            && values["raw_transaction_fallbacks"] == "0"
            && values["selected_profile_failures"] == "0"
            && values["pinned_inputs_unchanged"] == "true"
            && values["exact_input_before_after_equal"] == "true",
        "source profile attestation does not prove one strict unchanged audit"
    );
    let parse_count = |name: &str| -> Result<u64> {
        let text = values
            .get(name)
            .context("source profile attestation count is missing")?;
        let count = text
            .parse::<u64>()
            .with_context(|| format!("source profile attestation count is invalid: {name}"))?;
        ensure!(
            *text == count.to_string(),
            "source profile attestation count is not canonical: {name}"
        );
        Ok(count)
    };
    let blocks = parse_count("blocks")?;
    let messages = parse_count("messages")?;
    let alternate = parse_count("alternate_profile_failures")?;
    let equivalent = parse_count("both_semantically_equivalent")?;
    let divergent = parse_count("both_semantically_divergent")?;
    ensure!(blocks > 0 && messages > 0, "source profile audit is empty");
    ensure!(
        alternate
            .checked_add(equivalent)
            .and_then(|value| value.checked_add(divergent))
            == Some(messages),
        "source profile audit classification is incomplete"
    );
    match values["decision_basis"] {
        "unique_full_generation_decode" => ensure!(
            alternate > 0,
            "unique full-generation profile decision has no alternate rejection"
        ),
        "all_semantically_equivalent" => ensure!(
            alternate == 0 && divergent == 0 && equivalent == messages,
            "all-equivalent profile decision contains ambiguous evidence"
        ),
        _ => bail!("source profile attestation has no recovery-safe decision"),
    }
    Ok(())
}

fn validate_profile_neutral_recovery_authority(
    source: &Path,
    target: &Path,
    epoch: u64,
    selected: ArchiveV2WireProfile,
    recovery_receipt_path: &Path,
    authorities: &[ProfileNeutralRecoveryAuthority],
    authority_sha256: &str,
) -> Result<ResolvedSourceWireProfileAuthority> {
    ensure!(
        profile_neutral_recovery_authority_sha256(authorities) == authority_sha256,
        "compiled profile-neutral recovery authority digest is invalid"
    );
    ensure!(
        selected == ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        "profile-neutral recovery authority selects only the Post wire profile"
    );
    let authority = authorities
        .iter()
        .find(|authority| authority.epoch == epoch)
        .context("marker-free source epoch has no reviewed recovery authority")?;
    let expected_name = format!("epoch-{epoch}.{}.json", authority.target_generation_sha256);
    ensure!(
        recovery_receipt_path
            .file_name()
            .and_then(|name| name.to_str())
            == Some(expected_name.as_str()),
        "profile authority receipt name differs from the exact generation identity"
    );
    let (receipt_bytes, receipt_binding, receipt_identity) =
        read_profile_authority_file(recovery_receipt_path, "profile authority receipt")?;
    let receipt: ProfileNeutralRecoveryReceipt = serde_json::from_slice(&receipt_bytes)
        .with_context(|| format!("parse {}", recovery_receipt_path.display()))?;
    let expected_quarantine = target.with_file_name(format!(
        ".{}.registry-reprocess.profile-neutral-post-v1.{}.quarantine",
        target
            .file_name()
            .and_then(|name| name.to_str())
            .context("registry target name is not valid UTF-8")?,
        authority.target_generation_sha256,
    ));
    ensure!(
        receipt.schema_version == PROFILE_NEUTRAL_RECOVERY_RECEIPT_SCHEMA_VERSION
            && receipt.kind == PROFILE_NEUTRAL_RECOVERY_RECEIPT_KIND
            && receipt.incident_id == PROFILE_NEUTRAL_RECOVERY_INCIDENT_ID
            && receipt.authority_sha256 == authority_sha256
            && receipt.epoch == epoch
            && receipt.wire_profile == selected
            && receipt.legacy_receipt_version == authority.receipt_version
            && receipt.legacy_receipt_path == target.join(REGISTRY_REPROCESS_RECEIPT_FILE)
            && receipt.legacy_receipt_sha256 == authority.receipt_sha256
            && receipt.source_generation_sha256 == authority.source_generation_sha256
            && receipt.target_generation_sha256 == authority.target_generation_sha256
            && receipt.quarantine == expected_quarantine
            && (1..=256).contains(&receipt.recovery_threads)
            && receipt.created_unix_secs > 0,
        "profile authority receipt differs from the compiled recovery authority"
    );
    validate_hex_sha256(&receipt.original_marker_sha256, "original marker digest")?;
    validate_hex_sha256(
        &receipt.source_attestation_sha256,
        "source profile attestation digest",
    )?;
    validate_profile_authority_identity(&receipt.legacy_receipt_identity)?;
    validate_profile_authority_identity(&receipt.source_attestation_identity)?;
    validate_profile_authority_identity(&receipt.archived_marker_identity)?;

    let quarantine_metadata = fs::symlink_metadata(&receipt.quarantine)
        .with_context(|| format!("inspect quarantine {}", receipt.quarantine.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            quarantine_metadata.file_type().is_dir()
                && quarantine_metadata.dev() == receipt.legacy_generation_device
                && quarantine_metadata.ino() == receipt.legacy_generation_inode
                && fs::canonicalize(&receipt.quarantine)? == receipt.quarantine,
            "profile authority quarantine generation identity changed"
        );
    }
    #[cfg(not(unix))]
    ensure!(
        quarantine_metadata.file_type().is_dir()
            && fs::canonicalize(&receipt.quarantine)? == receipt.quarantine,
        "profile authority quarantine generation is invalid"
    );
    for (name, expected) in &receipt.legacy_target_file_identities {
        ensure!(
            !name.is_empty() && Path::new(name).components().count() == 1,
            "profile authority has an invalid legacy target file name"
        );
        validate_profile_authority_identity(expected)?;
        let metadata = fs::symlink_metadata(receipt.quarantine.join(name))?;
        ensure!(
            metadata.file_type().is_file()
                && profile_authority_file_identity(&metadata) == *expected,
            "profile authority legacy target file identity changed: {name}"
        );
    }

    let legacy_receipt_path = receipt.quarantine.join(REGISTRY_REPROCESS_RECEIPT_FILE);
    let (legacy_bytes, legacy_binding, legacy_identity) =
        read_profile_authority_file(&legacy_receipt_path, "legacy registry receipt")?;
    ensure!(
        legacy_binding.sha256 == authority.receipt_sha256
            && legacy_identity == receipt.legacy_receipt_identity,
        "legacy registry receipt differs from the compiled recovery authority"
    );
    let legacy: RegistryReprocessReceipt =
        serde_json::from_slice(&legacy_bytes).context("parse legacy registry receipt authority")?;
    validate_receipt_shape(&legacy, epoch)?;
    ensure!(
        legacy.version == authority.receipt_version
            && Path::new(&legacy.source_dir) == source
            && Path::new(&legacy.target_dir) == target
            && legacy.source_generation_sha256 == authority.source_generation_sha256
            && legacy.target_generation_sha256 == authority.target_generation_sha256
            && generation_digest(&legacy.source_files) == legacy.source_generation_sha256
            && generation_digest(&legacy.target_files) == legacy.target_generation_sha256,
        "legacy registry receipt does not bind the reviewed source and target generations"
    );

    let (attestation_bytes, attestation_binding, attestation_identity) =
        read_profile_authority_file(
            &receipt.source_attestation_path,
            "source profile attestation",
        )?;
    ensure!(
        attestation_binding.sha256 == receipt.source_attestation_sha256
            && attestation_identity == receipt.source_attestation_identity,
        "source profile attestation differs from the recovery receipt binding"
    );
    let attestation: ProfileNeutralSourceAttestation =
        serde_json::from_slice(&attestation_bytes).context("parse source profile attestation")?;
    ensure!(
        attestation.schema_version == 2
            && attestation.kind == PROFILE_NEUTRAL_RECOVERY_ATTESTATION_KIND
            && attestation.audit_algorithm == PROFILE_NEUTRAL_RECOVERY_ATTESTATION_ALGORITHM
            && attestation.audited_profiles
                == [
                    ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
                    ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
                ]
            && attestation.cluster_id == "mainnet-beta"
            && attestation.epoch == epoch
            && attestation.archive == source
            && attestation.registry_order == "first_seen"
            && attestation.generation_kind == PROFILE_NEUTRAL_RECOVERY_ATTESTATION_GENERATION_KIND
            && attestation.content_generation_sha256 == authority.source_generation_sha256
            && attestation.wire_profile == selected
            && attestation.attested_unix_secs > 0
            && attestation.archive_files.len() == legacy.source_files.len()
            && attestation
                .archive_files
                .keys()
                .eq(legacy.source_files.keys()),
        "source profile attestation differs from the reviewed recovery generation"
    );
    validate_profile_neutral_attestation_evidence(&attestation.evidence)?;
    for required in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_SIGNATURES_FILE,
    ] {
        ensure!(
            attestation.archive_files.contains_key(required),
            "source profile attestation omits required artifact {required}"
        );
    }
    for (name, expected) in &attestation.archive_files {
        ensure!(
            !name.is_empty()
                && Path::new(name).components().count() == 1
                && !is_wire_profile_marker_name(name),
            "source profile attestation contains an invalid marker-free artifact name"
        );
        validate_profile_authority_identity(expected)?;
        let metadata = fs::symlink_metadata(source.join(name))?;
        ensure!(
            metadata.file_type().is_file()
                && profile_authority_file_identity(&metadata) == *expected,
            "attested source artifact identity changed: {name}"
        );
        ensure!(
            legacy.source_files.get(name).is_some_and(|binding| {
                binding.bytes == expected.size
                    && validate_hex_sha256(&binding.sha256, "legacy source binding").is_ok()
            }),
            "attested source artifact differs from the legacy receipt binding: {name}"
        );
    }

    Ok(ResolvedSourceWireProfileAuthority {
        authority: SourceWireProfileAuthority::ProfileNeutralRecoveryReceipt {
            path: recovery_receipt_path.to_path_buf(),
            receipt: receipt_binding,
            identity: receipt_identity,
            source_generation_sha256: authority.source_generation_sha256.to_owned(),
            legacy_source_files: legacy.source_files,
        },
        marker_free_evidence: Some(
            MarkerFreeSourceAuthorityEvidence::ProfileNeutralRecoveryReceipt {
                source_file_identities: attestation.archive_files,
            },
        ),
    })
}

fn profile_neutral_recovery_authority_sha256(
    authorities: &[ProfileNeutralRecoveryAuthority],
) -> String {
    fn update_len_prefixed(hasher: &mut Sha256, value: &str) {
        hasher.update((value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }

    let mut hasher = Sha256::new();
    hasher.update(PROFILE_NEUTRAL_RECOVERY_AUTHORITY_DOMAIN);
    update_len_prefixed(&mut hasher, PROFILE_NEUTRAL_RECOVERY_INCIDENT_ID);
    update_len_prefixed(
        &mut hasher,
        &ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1.to_string(),
    );
    hasher.update((authorities.len() as u64).to_le_bytes());
    for authority in authorities {
        hasher.update(authority.epoch.to_le_bytes());
        hasher.update(authority.receipt_version.to_le_bytes());
        update_len_prefixed(&mut hasher, authority.receipt_sha256);
        update_len_prefixed(&mut hasher, authority.source_generation_sha256);
        update_len_prefixed(&mut hasher, authority.target_generation_sha256);
    }
    hex_digest(hasher.finalize())
}

fn regular_file_metadata(path: &Path) -> Result<fs::Metadata> {
    let link = fs::symlink_metadata(path).with_context(|| format!("inspect {}", path.display()))?;
    ensure!(
        link.file_type().is_file(),
        "source artifact is not a regular non-symlink file: {}",
        path.display()
    );
    Ok(link)
}

fn open_regular_read(path: &Path) -> Result<(File, fs::Metadata)> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    let file = options
        .open(path)
        .with_context(|| format!("open regular file {}", path.display()))?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.file_type().is_file(),
        "path is not a regular non-symlink file: {}",
        path.display()
    );
    let path_metadata = fs::symlink_metadata(path)?;
    ensure!(
        path_metadata.file_type().is_file() && same_file_snapshot(&metadata, &path_metadata),
        "file path changed while opening: {}",
        path.display()
    );
    Ok((file, metadata))
}

fn ensure_open_file_unchanged(path: &Path, file: &File, before: &fs::Metadata) -> Result<()> {
    let after = file.metadata()?;
    let path_metadata = fs::symlink_metadata(path)?;
    ensure!(
        same_file_snapshot(before, &after)
            && path_metadata.file_type().is_file()
            && same_file_snapshot(before, &path_metadata),
        "file changed while reading: {}",
        path.display()
    );
    Ok(())
}

fn prepare_staging(staging: &Path, expected: &ReprocessCheckpoint) -> Result<()> {
    let mut create = false;
    match fs::symlink_metadata(&staging) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => create = true,
        Err(error) => {
            return Err(error).with_context(|| format!("inspect staging {}", staging.display()));
        }
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_dir(),
                "stale staging path is not a directory: {}",
                staging.display()
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                ensure!(
                    metadata.mode() & 0o777 == 0o700,
                    "scheduler-owned staging must have mode 0700: {}",
                    staging.display()
                );
                // SAFETY: geteuid has no preconditions and returns the effective process owner.
                ensure!(
                    metadata.uid() == unsafe { libc::geteuid() },
                    "scheduler-owned staging has the wrong owner: {}",
                    staging.display()
                );
            }
            let mut entries = fs::read_dir(staging)
                .with_context(|| format!("enumerate staging {}", staging.display()))?;
            if entries.next().transpose()?.is_none() {
                // The scheduler publishes the exact empty 0700 directory before it spawns the
                // core child. The core owns only the contents, not directory creation.
            } else {
                ensure!(
                    !staging.join(REPROCESS_HANDOFF_FILE).try_exists()?,
                    "durable core handoff exists and must not be removed: {}",
                    staging.display()
                );
                let actual = read_checkpoint(staging).with_context(|| {
                    format!(
                        "partial staging has no valid attempt checkpoint; explicit discard is required: {}",
                        staging.display()
                    )
                })?;
                ensure!(
                    &actual == expected,
                    "partial staging checkpoint does not match this exact attempt: {}",
                    staging.display()
                );
                bail!(
                    "matching partial staging is retained for explicit retry or discard: {}",
                    staging.display()
                );
            }
        }
    }
    if create {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            builder
                .create(staging)
                .with_context(|| format!("create staging {}", staging.display()))?;
        }
        #[cfg(not(unix))]
        fs::create_dir(staging).with_context(|| format!("create staging {}", staging.display()))?;
    }
    let checkpoint_path = staging.join(REPROCESS_CHECKPOINT_FILE);
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&checkpoint_path)?,
    );
    serde_json::to_writer_pretty(&mut writer, expected)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    sync_directory(staging)
}

fn read_checkpoint(staging: &Path) -> Result<ReprocessCheckpoint> {
    let path = staging.join(REPROCESS_CHECKPOINT_FILE);
    let (mut file, metadata) = open_regular_read(&path)?;
    ensure!(
        metadata.len() > 0 && metadata.len() <= RECEIPT_MAX_BYTES,
        "invalid staging checkpoint size"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(RECEIPT_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= RECEIPT_MAX_BYTES);
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    let checkpoint: ReprocessCheckpoint = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse checkpoint {}", path.display()))?;
    ensure!(
        checkpoint.version == REPROCESS_CHECKPOINT_VERSION
            && checkpoint.algorithm == RECEIPT_ALGORITHM,
        "unsupported registry reprocess checkpoint"
    );
    validate_source_wire_profile_authority_shape(
        checkpoint.epoch,
        checkpoint.wire_profile,
        &checkpoint.wire_profile_authority,
    )?;
    Ok(checkpoint)
}

fn core_staging_file_bindings(
    staging: &Path,
    blocks: FileBinding,
    wire_profile: ArchiveV2WireProfile,
) -> Result<BTreeMap<String, FileBinding>> {
    let mut bindings = BTreeMap::new();
    bindings.insert(ARCHIVE_V2_BLOCKS_FILE.to_owned(), blocks);
    for name in [
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        ensure!(
            bindings
                .insert(name.to_owned(), hash_file(&staging.join(name))?)
                .is_none(),
            "duplicate core staging binding {name}"
        );
    }
    let marker = wire_profile_marker(wire_profile);
    ensure!(
        bindings
            .insert(marker.name.clone(), hash_file(&staging.join(&marker.name))?)
            .is_none(),
        "duplicate core wire-profile marker binding"
    );
    Ok(bindings)
}

fn write_wire_profile_marker(directory: &Path, profile: ArchiveV2WireProfile) -> Result<()> {
    let marker = wire_profile_marker(profile);
    let bytes = wire_profile_marker_bytes(profile);
    ensure!(bytes.len() as u64 == marker.size);
    ensure!(hex_digest(Sha256::digest(bytes)) == marker.sha256);
    for other in [
        PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
        POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE,
    ] {
        if other != marker.name {
            ensure!(
                !directory.join(other).try_exists()?,
                "staging contains conflicting Archive V2 wire-profile marker {other}"
            );
        }
    }
    let path = directory.join(&marker.name);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    sync_directory(directory)
}

fn write_registry_remap(path: &Path, mapping: &[u32], target_keys: u64) -> Result<FileBinding> {
    ensure!(!path.try_exists()?, "registry remap already exists");
    let source_keys = u64::try_from(mapping.len()).context("source remap length exceeds u64")?;
    let bytes = REPROCESS_REMAP_HEADER_BYTES
        .checked_add(
            source_keys
                .checked_mul(4)
                .context("registry remap byte length overflow")?,
        )
        .context("registry remap file length overflow")?;
    let mut file = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        OpenOptions::new().write(true).create_new(true).open(path)?,
    );
    let mut hasher = Sha256::new();
    for chunk in [
        REPROCESS_REMAP_MAGIC.as_slice(),
        &REPROCESS_REMAP_VERSION.to_le_bytes(),
        &[0u8; 4],
        &source_keys.to_le_bytes(),
        &target_keys.to_le_bytes(),
    ] {
        file.write_all(chunk)?;
        hasher.update(chunk);
    }
    for &id in mapping {
        let bytes = id.to_le_bytes();
        file.write_all(&bytes)?;
        hasher.update(bytes);
    }
    file.flush()?;
    file.get_ref().sync_all()?;
    ensure!(
        file.get_ref().metadata()?.len() == bytes,
        "registry remap file length changed while writing"
    );
    Ok(FileBinding {
        bytes,
        sha256: hex_digest(hasher.finalize()),
    })
}

fn validate_handoff_shape(handoff: &RegistryReprocessHandoff) -> Result<()> {
    ensure!(
        handoff.version == REPROCESS_HANDOFF_VERSION,
        "unsupported registry reprocess handoff version"
    );
    ensure!(
        handoff.state == CORE_COMPLETE_STATE,
        "registry reprocess handoff has invalid state"
    );
    validate_attempt_id(&handoff.attempt_id)?;
    ensure!(handoff.epoch != 0);
    ensure!(handoff.threads > 0 && handoff.sort_memory_mib > 0);
    ensure!(
        handoff.source_registry_keys > 0 && handoff.source_registry_keys <= u64::from(u32::MAX)
    );
    ensure!(
        handoff.target_registry_keys > 0
            && handoff.target_registry_keys <= handoff.source_registry_keys + 1
    );
    ensure!(handoff.rewrite_stats.blocks > 0);
    ensure!(handoff.rewrite_stats.pubkey_references >= handoff.eligible_references);
    validate_hex_sha256(&handoff.source_anchor_sha256, "handoff source anchor")?;
    validate_source_wire_profile_authority_shape(
        handoff.epoch,
        handoff.wire_profile,
        &handoff.wire_profile_authority,
    )?;
    for binding in handoff.core_files.values().chain([
        &handoff.source_blocks,
        &handoff.source_registry_snapshot,
        &handoff.remap_file,
    ]) {
        validate_hex_sha256(&binding.sha256, "handoff artifact digest")?;
    }
    for name in [
        ARCHIVE_V2_BLOCKS_FILE,
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        ensure!(
            handoff.core_files.contains_key(name),
            "core handoff omits {name}"
        );
    }
    let marker = wire_profile_marker(handoff.wire_profile);
    ensure!(
        handoff.core_files.get(&marker.name)
            == Some(&FileBinding {
                bytes: marker.size,
                sha256: marker.sha256,
            }),
        "core handoff has no exact selected wire-profile marker binding"
    );
    ensure!(
        handoff.core_files.len() == 7,
        "core handoff contains an unexpected final artifact"
    );
    ensure!(
        !handoff.core_files.contains_key(ARCHIVE_V2_SIGNATURES_FILE),
        "core handoff contains signatures"
    );
    ensure!(
        !handoff
            .core_files
            .contains_key(ARCHIVE_V2_BLOCK_ACCESS_FILE)
            && !handoff
                .core_files
                .contains_key(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE)
            && !handoff
                .core_files
                .contains_key(ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
        "core handoff contains block-access"
    );
    Ok(())
}

fn write_handoff(staging: &Path, handoff: &RegistryReprocessHandoff) -> Result<FileBinding> {
    let path = staging.join(REPROCESS_HANDOFF_FILE);
    ensure!(!path.try_exists()?, "registry handoff already exists");
    let mut bytes = serde_json::to_vec_pretty(handoff)?;
    bytes.push(b'\n');
    ensure!(
        bytes.len() as u64 <= RECEIPT_MAX_BYTES,
        "registry handoff exceeds bounded size"
    );
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_directory(staging)?;
    Ok(FileBinding {
        bytes: bytes.len() as u64,
        sha256: hex_digest(Sha256::digest(&bytes)),
    })
}

fn read_handoff(staging: &Path) -> Result<(RegistryReprocessHandoff, FileBinding)> {
    let path = staging.join(REPROCESS_HANDOFF_FILE);
    let (mut file, metadata) = open_regular_read(&path)?;
    ensure!(
        metadata.len() > 0 && metadata.len() <= RECEIPT_MAX_BYTES,
        "invalid registry handoff size"
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(RECEIPT_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= RECEIPT_MAX_BYTES);
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    let handoff = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse registry handoff {}", path.display()))?;
    Ok((
        handoff,
        FileBinding {
            bytes: bytes.len() as u64,
            sha256: hex_digest(Sha256::digest(&bytes)),
        },
    ))
}

fn core_result_from_handoff(
    handoff: &RegistryReprocessHandoff,
    handoff_sha256: String,
) -> Result<RegistryReprocessCoreResult> {
    validate_hex_sha256(&handoff_sha256, "handoff digest")?;
    Ok(RegistryReprocessCoreResult {
        version: REPROCESS_CORE_RESULT_VERSION,
        state: handoff.state.clone(),
        attempt_id: handoff.attempt_id.clone(),
        epoch: handoff.epoch,
        source_dir: handoff.source_dir.clone(),
        target_dir: handoff.target_dir.clone(),
        staging_dir: handoff.staging_dir.clone(),
        handoff_sha256,
        wire_profile: handoff.wire_profile,
    })
}

fn validate_core_staging(staging: &Path, handoff: &RegistryReprocessHandoff) -> Result<()> {
    for (name, expected) in &handoff.core_files {
        let actual = hash_file(&staging.join(name))?;
        ensure!(
            &actual == expected,
            "core staging binding mismatch for {name}"
        );
    }
    ensure!(
        hash_file(&staging.join(SOURCE_REGISTRY_SNAPSHOT_FILE))?
            == handoff.source_registry_snapshot,
        "source registry snapshot binding mismatch"
    );
    ensure!(
        hash_file(&staging.join(REPROCESS_REMAP_FILE))? == handoff.remap_file,
        "registry remap binding mismatch"
    );
    validate_staging_entry_allowlist(staging, &strict_core_entry_names(handoff), "core staging")
}

fn handoff_matches_checkpoint(
    handoff: &RegistryReprocessHandoff,
    checkpoint: &ReprocessCheckpoint,
) -> bool {
    handoff.version == REPROCESS_HANDOFF_VERSION
        && handoff.state == CORE_COMPLETE_STATE
        && handoff.attempt_id == checkpoint.attempt_id
        && handoff.epoch == checkpoint.epoch
        && handoff.source_dir == checkpoint.source_dir
        && handoff.target_dir == checkpoint.target_dir
        && handoff.staging_dir == checkpoint.staging_dir
        && handoff.threads == checkpoint.threads
        && handoff.sort_memory_mib == checkpoint.sort_memory_mib
        && handoff.level == checkpoint.level
        && handoff.source_anchor_sha256 == checkpoint.source_anchor_sha256
        && handoff.wire_profile == checkpoint.wire_profile
        && handoff.wire_profile_authority == checkpoint.wire_profile_authority
}

fn reuse_core_handoff_if_complete(
    staging: &Path,
    checkpoint: &ReprocessCheckpoint,
) -> Result<Option<RegistryReprocessCoreResult>> {
    if !staging.try_exists()? {
        return Ok(None);
    }
    let handoff_path = staging.join(REPROCESS_HANDOFF_FILE);
    if !handoff_path.try_exists()? {
        return Ok(None);
    }
    let (handoff, binding) = read_handoff(staging)?;
    validate_handoff_shape(&handoff)?;
    ensure!(
        handoff_matches_checkpoint(&handoff, checkpoint),
        "durable handoff does not match this exact core attempt"
    );
    validate_core_staging(staging, &handoff)?;
    core_result_from_handoff(&handoff, binding.sha256).map(Some)
}

/// Bounded restart probe for one durable registry-only handoff.
pub(crate) fn probe_registry_reprocess_core_handoff(
    staging: &Path,
    source: &Path,
    target: &Path,
    epoch: u64,
    attempt_id: &str,
    wire_profile: ArchiveV2WireProfile,
) -> Result<RegistryReprocessCoreResult> {
    validate_attempt_id(attempt_id)?;
    ensure!(epoch != 0);
    let source = fs::canonicalize(source)
        .with_context(|| format!("canonicalize source {}", source.display()))?;
    let target = canonical_target_path(target)?;
    ensure!(
        !target.try_exists()?,
        "published target exists; core handoff is no longer the active state"
    );
    let expected_staging = canonical_staging_path(staging, &target, attempt_id)?;
    let staging = fs::canonicalize(&expected_staging)
        .with_context(|| format!("canonicalize staging {}", expected_staging.display()))?;
    ensure!(staging == expected_staging);
    validate_private_staging_directory(&staging)?;
    let checkpoint = read_checkpoint(&staging)?;
    let (handoff, binding) = read_handoff(&staging)?;
    validate_handoff_shape(&handoff)?;
    ensure!(
        handoff_matches_checkpoint(&handoff, &checkpoint)
            && handoff.epoch == epoch
            && handoff.attempt_id == attempt_id
            && Path::new(&handoff.source_dir) == source
            && Path::new(&handoff.target_dir) == target
            && Path::new(&handoff.staging_dir) == staging
            && handoff.wire_profile == wire_profile,
        "core handoff does not match the exact restart claim"
    );
    let options = RegistryReprocessOptions {
        source_dir: source.clone(),
        target_dir: target.clone(),
        epoch,
        threads: handoff.threads,
        sort_memory_mib: handoff.sort_memory_mib,
        level: handoff.level,
        attempt_id: attempt_id.to_owned(),
        staging_dir: staging.clone(),
        wire_profile: handoff.wire_profile,
        wire_profile_authority_receipt: checkpoint
            .wire_profile_authority
            .recovery_receipt_path()
            .map(Path::to_path_buf),
    };
    ensure!(
        rebuild_checkpoint_against_authority(
            &source,
            &target,
            &staging,
            &options,
            &checkpoint.wire_profile_authority,
        )? == checkpoint,
        "core source inputs changed after handoff"
    );
    validate_core_staging_bounded(&staging, &handoff)?;
    core_result_from_handoff(&handoff, binding.sha256)
}

/// Non-mutating restart probe for an access-phase continuation.
///
/// Receipt-ready recovery authenticates every bound non-block, non-signature file and can be
/// I/O-heavy. Signature contents remain unopened.
#[allow(dead_code)] // Kept as the stable result-only wrapper for non-scheduler callers.
pub(crate) fn probe_registry_reprocess_access_continuation(
    staging: &Path,
    source: &Path,
    target: &Path,
    epoch: u64,
    attempt_id: &str,
    handoff_sha256: &str,
    wire_profile: ArchiveV2WireProfile,
) -> Result<RegistryReprocessCoreResult> {
    Ok(probe_registry_reprocess_access_continuation_state(
        staging,
        source,
        target,
        epoch,
        attempt_id,
        handoff_sha256,
        wire_profile,
    )?
    .core_result)
}

/// Non-mutating restart probe that also classifies the exact work left to do.
///
/// Receipt-ready recovery authenticates every bound non-block, non-signature file and can be
/// I/O-heavy. Signature contents remain unopened.
pub(crate) fn probe_registry_reprocess_access_continuation_state(
    staging: &Path,
    source: &Path,
    target: &Path,
    epoch: u64,
    attempt_id: &str,
    handoff_sha256: &str,
    wire_profile: ArchiveV2WireProfile,
) -> Result<RegistryReprocessAccessContinuationProbe> {
    validate_attempt_id(attempt_id)?;
    validate_hex_sha256(handoff_sha256, "access continuation handoff digest")?;
    ensure!(epoch != 0);
    let source = fs::canonicalize(source)
        .with_context(|| format!("canonicalize source {}", source.display()))?;
    let target = canonical_target_path(target)?;
    ensure!(
        !target.try_exists()?,
        "published target exists; access continuation is no longer pending"
    );
    let expected_staging = canonical_staging_path(staging, &target, attempt_id)?;
    let staging = fs::canonicalize(&expected_staging)
        .with_context(|| format!("canonicalize staging {}", expected_staging.display()))?;
    ensure!(staging == expected_staging);
    validate_access_staging_directory(&staging)?;
    // A valid final or temporary receipt is the publication-last identity for the completed
    // generation. Publication intentionally removes the private checkpoint and handoff files,
    // so authenticate the receipt before asking for either internal file. If no valid receipt is
    // present, the core-continuation path below still requires both files in full.
    let options = RegistryReprocessAccessOptions {
        source_dir: source.clone(),
        staging_dir: staging.clone(),
        target_dir: target.clone(),
        epoch,
        attempt_id: attempt_id.to_owned(),
        handoff_sha256: handoff_sha256.to_owned(),
        expected_continuation_state: RegistryReprocessAccessContinuationState::ReceiptReady,
        wire_profile,
        wire_profile_authority_receipt: None,
    };

    let has_receipt = staging.join(REGISTRY_REPROCESS_RECEIPT_FILE).try_exists()?;
    let has_receipt_temp = staging
        .join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE)
        .try_exists()?;
    ensure!(
        !(has_receipt && has_receipt_temp),
        "staging contains both the final and temporary access receipt"
    );
    if has_receipt {
        let receipt = read_receipt(&staging)?;
        validate_staged_receipt_without_signature_data(
            &options, &source, &staging, &target, &receipt, true,
        )?;
        let core_result = probe_staged_access_receipt_continuation(
            &staging,
            &source,
            &target,
            epoch,
            attempt_id,
            handoff_sha256,
            &receipt,
            true,
        )?;
        return Ok(RegistryReprocessAccessContinuationProbe {
            state: RegistryReprocessAccessContinuationState::ReceiptReady,
            core_result,
        });
    }
    let invalid_receipt_temp = if has_receipt_temp {
        match read_receipt_temp(&staging).and_then(|receipt| {
            validate_staged_receipt_without_signature_data(
                &options, &source, &staging, &target, &receipt, false,
            )?;
            probe_staged_access_receipt_continuation(
                &staging,
                &source,
                &target,
                epoch,
                attempt_id,
                handoff_sha256,
                &receipt,
                false,
            )
        }) {
            Ok(core_result) => {
                return Ok(RegistryReprocessAccessContinuationProbe {
                    state: RegistryReprocessAccessContinuationState::ReceiptReady,
                    core_result,
                });
            }
            Err(error) => Some(error),
        }
    } else {
        None
    };

    let result = probe_access_handoff_continuation(&staging, &source, &target, epoch, attempt_id)
        .with_context(|| {
        invalid_receipt_temp.as_ref().map_or_else(
            || "validate durable core access continuation".to_owned(),
            |error| format!("invalid access receipt temp and no valid core fallback: {error:#}"),
        )
    })?;
    ensure!(
        result.handoff_sha256 == handoff_sha256 && result.wire_profile == wire_profile,
        "durable core handoff does not match the access continuation claim"
    );
    Ok(RegistryReprocessAccessContinuationProbe {
        state: RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
        core_result: result,
    })
}

fn probe_access_handoff_continuation(
    staging: &Path,
    source: &Path,
    target: &Path,
    epoch: u64,
    attempt_id: &str,
) -> Result<RegistryReprocessCoreResult> {
    let checkpoint = read_checkpoint(staging)?;
    let (handoff, binding) = read_handoff(staging)?;
    validate_handoff_shape(&handoff)?;
    ensure!(
        handoff_matches_checkpoint(&handoff, &checkpoint)
            && handoff.epoch == epoch
            && handoff.attempt_id == attempt_id
            && Path::new(&handoff.source_dir) == source
            && Path::new(&handoff.target_dir) == target
            && Path::new(&handoff.staging_dir) == staging,
        "core handoff does not match the exact access continuation claim"
    );
    let options = RegistryReprocessOptions {
        source_dir: source.to_path_buf(),
        target_dir: target.to_path_buf(),
        epoch,
        threads: handoff.threads,
        sort_memory_mib: handoff.sort_memory_mib,
        level: handoff.level,
        attempt_id: attempt_id.to_owned(),
        staging_dir: staging.to_path_buf(),
        wire_profile: handoff.wire_profile,
        wire_profile_authority_receipt: checkpoint
            .wire_profile_authority
            .recovery_receipt_path()
            .map(Path::to_path_buf),
    };
    ensure!(
        rebuild_checkpoint_against_authority(
            source,
            target,
            staging,
            &options,
            &checkpoint.wire_profile_authority,
        )? == checkpoint,
        "core source inputs changed after handoff"
    );
    validate_access_partial_staging_bounded(staging, &handoff)?;
    core_result_from_handoff(&handoff, binding.sha256)
}

#[allow(clippy::too_many_arguments)]
fn probe_staged_access_receipt_continuation(
    staging: &Path,
    source: &Path,
    target: &Path,
    epoch: u64,
    attempt_id: &str,
    handoff_sha256: &str,
    receipt: &RegistryReprocessReceipt,
    final_receipt: bool,
) -> Result<RegistryReprocessCoreResult> {
    validate_receipt_shape(receipt, epoch)?;
    ensure!(
        receipt.version == RECEIPT_VERSION
            && receipt.attempt_id.as_deref() == Some(attempt_id)
            && receipt.handoff_sha256.as_deref() == Some(handoff_sha256)
            && Path::new(&receipt.source_dir) == source
            && Path::new(&receipt.target_dir) == target,
        "staged access receipt does not match the exact continuation claim"
    );
    ensure!(
        generation_digest(&receipt.source_files) == receipt.source_generation_sha256
            && generation_digest(&receipt.target_files) == receipt.target_generation_sha256,
        "staged access receipt generation digest mismatch"
    );
    validate_probe_core_files(&receipt.source_files, true, receipt.version)?;
    validate_probe_core_files(&receipt.target_files, false, receipt.version)?;
    ensure!(
        receipt
            .target_files
            .keys()
            .all(|name| allowed_v3_target_artifact(name)),
        "staged access receipt declares an unexpected target artifact"
    );
    probe_binding_sizes_without_signature_data(source, &receipt.source_files)?;
    probe_binding_sizes_without_signature_data(staging, &receipt.target_files)?;
    validate_signature_hard_link_metadata(
        source,
        staging,
        receipt
            .target_files
            .get(ARCHIVE_V2_SIGNATURES_FILE)
            .context("v3 receipt omits signatures binding")?
            .bytes,
    )?;

    validate_staged_receipt_entry_allowlist(staging, receipt, final_receipt)?;
    Ok(RegistryReprocessCoreResult {
        version: REPROCESS_CORE_RESULT_VERSION,
        state: CORE_COMPLETE_STATE.to_owned(),
        attempt_id: attempt_id.to_owned(),
        epoch,
        source_dir: source.display().to_string(),
        target_dir: target.display().to_string(),
        staging_dir: staging.display().to_string(),
        handoff_sha256: handoff_sha256.to_owned(),
        wire_profile: receipt
            .wire_profile
            .context("profile-bound receipt omits wire profile")?,
    })
}

fn allowed_v3_target_artifact(name: &str) -> bool {
    matches!(
        name,
        ARCHIVE_V2_BLOCKS_FILE
            | ARCHIVE_V2_BLOCK_INDEX_FILE
            | ARCHIVE_V2_META_FILE
            | ARCHIVE_V2_PUBKEY_REGISTRY_FILE
            | ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE
            | ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE
            | ARCHIVE_V2_BLOCK_ACCESS_FILE
            | ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE
            | ARCHIVE_V2_GET_BLOCK_INDEX_FILE
            | ARCHIVE_V2_SIGNATURES_FILE
            | ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE
            | ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE
            | ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE
            | ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE
            | ARCHIVE_V2_POH_FILE
            | ARCHIVE_V2_SHREDDING_FILE
            | BLOCK_TIME_GAP_FILE
    ) || is_wire_profile_marker_name(name)
}

fn is_wire_profile_marker_name(name: &str) -> bool {
    matches!(
        name,
        PRE_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE
            | POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE
    )
}

fn validate_receipt_wire_profile_binding(receipt: &RegistryReprocessReceipt) -> Result<()> {
    let receipt_profile = receipt
        .wire_profile
        .context("profile-bound receipt omits Archive V2 wire profile")?;
    let mut bound = None;
    for profile in [
        ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1,
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
    ] {
        let marker = wire_profile_marker(profile);
        if let Some(binding) = receipt.target_files.get(&marker.name) {
            ensure!(
                bound.is_none(),
                "receipt target binds conflicting Archive V2 wire-profile markers"
            );
            ensure!(
                binding.bytes == marker.size && binding.sha256 == marker.sha256,
                "receipt target has a malformed Archive V2 wire-profile marker binding"
            );
            bound = Some(profile);
        }
        if let Some(binding) = receipt.source_files.get(&marker.name) {
            ensure!(
                binding.bytes == marker.size && binding.sha256 == marker.sha256,
                "receipt source has a malformed Archive V2 wire-profile marker binding"
            );
            ensure!(
                receipt.wire_profile == Some(profile),
                "receipt source wire-profile marker conflicts with migration provenance"
            );
        }
    }
    ensure!(
        bound == Some(receipt_profile),
        "receipt wire profile and target marker binding differ"
    );
    Ok(())
}

fn allowed_v3_source_artifact(name: &str) -> bool {
    name != ARCHIVE_V2_GET_BLOCK_INDEX_FILE
        && (allowed_v3_target_artifact(name)
            || matches!(
                name,
                ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE
                    | ARCHIVE_V2_PUBKEY_HOT_SEED_FILE
                    | GENERATION_MANIFEST_FILE
            ))
}

fn validate_staged_receipt_entry_allowlist(
    staging: &Path,
    receipt: &RegistryReprocessReceipt,
    final_receipt: bool,
) -> Result<()> {
    let receipt_name = if final_receipt {
        REGISTRY_REPROCESS_RECEIPT_FILE
    } else {
        REGISTRY_REPROCESS_RECEIPT_TEMP_FILE
    };
    let mut allowed = receipt
        .target_files
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    allowed.insert(receipt_name.to_owned());
    if !final_receipt {
        for name in [
            REPROCESS_CHECKPOINT_FILE,
            REPROCESS_HANDOFF_FILE,
            REPROCESS_REMAP_FILE,
            SOURCE_REGISTRY_SNAPSHOT_FILE,
        ] {
            allowed.insert(name.to_owned());
        }
    }
    validate_staging_entry_allowlist(staging, &allowed, "staged access receipt")
}

fn validate_core_staging_bounded(staging: &Path, handoff: &RegistryReprocessHandoff) -> Result<()> {
    validate_core_staging_files_bounded(staging, handoff)?;
    let expected_names = strict_core_entry_names(handoff);
    validate_staging_entry_allowlist(staging, &expected_names, "core handoff")
}

fn validate_access_partial_staging_bounded(
    staging: &Path,
    handoff: &RegistryReprocessHandoff,
) -> Result<()> {
    validate_core_staging_files_bounded(staging, handoff)?;
    let mut expected_names = strict_core_entry_names(handoff);
    for name in phase2_output_names() {
        expected_names.insert(name.to_owned());
        expected_names.insert(format!("{name}{ACCESS_TEMP_SUFFIX}"));
    }
    expected_names.insert(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE.to_owned());
    validate_staging_entry_allowlist(staging, &expected_names, "access continuation")
}

fn strict_core_entry_names(
    handoff: &RegistryReprocessHandoff,
) -> std::collections::BTreeSet<String> {
    handoff
        .core_files
        .keys()
        .cloned()
        .chain([
            REPROCESS_CHECKPOINT_FILE.to_owned(),
            REPROCESS_HANDOFF_FILE.to_owned(),
            REPROCESS_REMAP_FILE.to_owned(),
            SOURCE_REGISTRY_SNAPSHOT_FILE.to_owned(),
        ])
        .collect()
}

fn validate_core_staging_files_bounded(
    staging: &Path,
    handoff: &RegistryReprocessHandoff,
) -> Result<()> {
    for (name, binding) in &handoff.core_files {
        let metadata = fs::symlink_metadata(staging.join(name))?;
        ensure!(
            metadata.file_type().is_file() && metadata.len() == binding.bytes,
            "bounded core handoff probe found an invalid {name}"
        );
    }
    for (name, binding) in [
        (
            SOURCE_REGISTRY_SNAPSHOT_FILE,
            &handoff.source_registry_snapshot,
        ),
        (REPROCESS_REMAP_FILE, &handoff.remap_file),
    ] {
        let metadata = fs::symlink_metadata(staging.join(name))?;
        ensure!(
            metadata.file_type().is_file() && metadata.len() == binding.bytes,
            "bounded core handoff probe found an invalid {name}"
        );
    }
    let remap = MappedRegistryRemap::open(&staging.join(REPROCESS_REMAP_FILE))?;
    ensure!(
        remap.source_keys as u64 == handoff.source_registry_keys
            && remap.target_keys == handoff.target_registry_keys,
        "bounded core handoff probe found invalid remap dimensions"
    );
    Ok(())
}

fn validate_staging_entry_allowlist(
    staging: &Path,
    expected_names: &std::collections::BTreeSet<String>,
    state: &str,
) -> Result<()> {
    for entry in fs::read_dir(staging)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .to_str()
            .context("staging entry name is not UTF-8")?
            .to_owned();
        ensure!(
            expected_names.contains(&name)
                && entry.file_type()?.is_file()
                && !entry.file_type()?.is_symlink(),
            "bounded {state} probe found unexpected staging entry {name}"
        );
    }
    Ok(())
}

/// Remove one exact failed core attempt. A durable handoff is never removable through this API.
pub(crate) fn discard_registry_reprocess_core_partial(
    staging: &Path,
    source: &Path,
    target: &Path,
    epoch: u64,
    attempt_id: &str,
) -> Result<()> {
    validate_attempt_id(attempt_id)?;
    let source = fs::canonicalize(source)?;
    let target = canonical_target_path(target)?;
    let expected_staging = canonical_staging_path(staging, &target, attempt_id)?;
    let staging = fs::canonicalize(&expected_staging)?;
    ensure!(staging == expected_staging);
    validate_private_staging_directory(&staging)?;
    ensure!(
        !staging.join(REPROCESS_HANDOFF_FILE).try_exists()?,
        "durable core handoff cannot be discarded"
    );
    let entries = fs::read_dir(&staging)?.collect::<io::Result<Vec<_>>>()?;
    if entries.is_empty() {
        fs::remove_dir(&staging)
            .with_context(|| format!("discard exact empty core staging {}", staging.display()))?;
        return sync_directory(target.parent().context("target has no parent")?);
    }
    let checkpoint = read_checkpoint(&staging)?;
    ensure!(
        checkpoint.version == REPROCESS_CHECKPOINT_VERSION
            && checkpoint.algorithm == RECEIPT_ALGORITHM
            && checkpoint.epoch == epoch
            && checkpoint.attempt_id == attempt_id
            && Path::new(&checkpoint.source_dir) == source
            && Path::new(&checkpoint.target_dir) == target
            && Path::new(&checkpoint.staging_dir) == staging,
        "partial checkpoint does not match the exact discard request"
    );
    let options = RegistryReprocessOptions {
        source_dir: source.clone(),
        target_dir: target.clone(),
        epoch,
        threads: checkpoint.threads,
        sort_memory_mib: checkpoint.sort_memory_mib,
        level: checkpoint.level,
        attempt_id: attempt_id.to_owned(),
        staging_dir: staging.clone(),
        wire_profile: checkpoint.wire_profile,
        wire_profile_authority_receipt: checkpoint
            .wire_profile_authority
            .recovery_receipt_path()
            .map(Path::to_path_buf),
    };
    ensure!(
        rebuild_checkpoint_against_authority(
            &source,
            &target,
            &staging,
            &options,
            &checkpoint.wire_profile_authority,
        )? == checkpoint,
        "source inputs changed; partial staging requires manual inspection"
    );
    for entry in entries {
        ensure!(
            entry.file_type()?.is_file() && !entry.file_type()?.is_symlink(),
            "partial staging contains a non-regular entry and will not be discarded"
        );
    }
    fs::remove_dir_all(&staging)
        .with_context(|| format!("discard exact core partial {}", staging.display()))?;
    sync_directory(target.parent().context("target has no parent")?)
}

#[cfg(unix)]
fn acquire_reprocess_lock(source: &Path, target: &Path, epoch: u64) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .context("target name is not UTF-8")?;
    let path = parent.join(format!(".{name}.registry-reprocess.lock"));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(&path)?;
    let lock_metadata = file.metadata()?;
    let lock_path_metadata = fs::symlink_metadata(&path)?;
    ensure!(
        lock_metadata.file_type().is_file()
            && lock_path_metadata.file_type().is_file()
            && same_file_identity(&lock_metadata, &lock_path_metadata),
        "registry reprocess lock is not a regular file: {}",
        path.display()
    );
    use std::os::fd::AsRawFd;
    // SAFETY: `file` owns a valid descriptor for the entire migration and flock does not retain
    // a pointer. LOCK_NB prevents a second scheduler/manual process from waiting indefinitely.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(io::Error::last_os_error())
            .with_context(|| format!("acquire registry reprocess lock {}", path.display()));
    }
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    writeln!(file, "version=1")?;
    writeln!(file, "pid={}", std::process::id())?;
    writeln!(file, "source={}", source.display())?;
    writeln!(file, "target={}", target.display())?;
    writeln!(file, "epoch={epoch}")?;
    file.flush()?;
    Ok(file)
}

#[cfg(not(unix))]
fn acquire_reprocess_lock(_source: &Path, _target: &Path, _epoch: u64) -> Result<File> {
    bail!("registry reprocess locking is unsupported on this operating system")
}

fn read_source_manifest(source: &Path) -> Result<SourceManifest> {
    let path = source.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE);
    let (mut file, metadata) = open_regular_read(&path)?;
    ensure!(
        metadata.file_type().is_file()
            && metadata.len() > 0
            && metadata.len() <= MANIFEST_MAX_BYTES,
        "first-seen manifest must be a non-empty regular file no larger than {MANIFEST_MAX_BYTES} bytes: {}",
        path.display()
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(MANIFEST_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MANIFEST_MAX_BYTES,
        "first-seen manifest grew while reading"
    );
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    let text = std::str::from_utf8(&bytes).context("first-seen manifest is not UTF-8")?;
    let mut values = BTreeMap::<&str, &str>::new();
    for (line_index, line) in text.lines().enumerate() {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| anyhow!("malformed first-seen manifest line {}", line_index + 1))?;
        ensure!(
            !key.is_empty(),
            "empty first-seen manifest key on line {}",
            line_index + 1
        );
        ensure!(
            values.insert(key, value).is_none(),
            "duplicate first-seen manifest key {key}"
        );
    }
    ensure!(
        values.get("version") == Some(&"1"),
        "first-seen manifest version is not 1"
    );
    ensure!(
        values.get("registry_order") == Some(&"first_seen_v1"),
        "source manifest does not declare registry_order=first_seen_v1"
    );
    ensure!(
        values.get("count_semantics") == Some(&"all_compact_pubkey_refs_v1"),
        "source manifest does not declare all-reference count semantics"
    );
    let registry_keys = values
        .get("registry_keys")
        .context("first-seen manifest missing registry_keys")?
        .parse::<u64>()
        .context("invalid first-seen registry_keys")?;
    let references = values
        .get("references")
        .context("first-seen manifest missing references")?
        .parse::<u64>()
        .context("invalid first-seen references")?;
    ensure!(registry_keys > 0 && registry_keys <= u64::from(u32::MAX));
    Ok(SourceManifest {
        registry_keys,
        references,
    })
}

fn read_registry_counts(path: &Path, expected: usize) -> Result<Vec<u32>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut counts = Vec::new();
    counts
        .try_reserve_exact(expected)
        .context("allocate registry count vector")?;
    for index in 0..expected {
        let count = read_canonical_u32_varint(&mut reader)?
            .ok_or_else(|| anyhow!("registry counts ended before row {}", index + 1))?;
        counts.push(count);
    }
    ensure!(
        read_canonical_u32_varint(&mut reader)?.is_none(),
        "registry counts contains more than {expected} rows"
    );
    Ok(counts)
}

fn read_canonical_u32_varint(reader: &mut impl Read) -> Result<Option<u32>> {
    let mut first = [0u8; 1];
    if reader.read(&mut first)? == 0 {
        return Ok(None);
    }
    let mut value = u32::from(first[0] & 0x7f);
    let mut byte = first[0];
    let mut bytes = 1usize;
    let mut shift = 7u32;
    while byte & 0x80 != 0 {
        ensure!(bytes < 5, "u32 varint exceeds five bytes");
        let mut next = [0u8; 1];
        reader
            .read_exact(&mut next)
            .context("truncated u32 varint")?;
        byte = next[0];
        let payload = u32::from(byte & 0x7f);
        ensure!(
            shift < 32 && payload <= (u32::MAX >> shift),
            "u32 varint overflow"
        );
        value |= payload << shift;
        shift += 7;
        bytes += 1;
    }
    let canonical_bytes = if value == 0 {
        1
    } else {
        ((u32::BITS - value.leading_zeros()) as usize).div_ceil(7)
    };
    ensure!(
        bytes == canonical_bytes,
        "non-canonical u32 varint encoding"
    );
    Ok(Some(value))
}

fn write_u32_varint(writer: &mut impl Write, mut value: u32) -> Result<()> {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            return Ok(());
        }
    }
}

fn validate_and_rewrite_meta(source: &Path, target: &Path) -> Result<WincodeArchiveV2Footer> {
    let source_path = source.join(ARCHIVE_V2_META_FILE);
    let target_path = target.join(ARCHIVE_V2_META_FILE);
    let source_file =
        File::open(&source_path).with_context(|| format!("open {}", source_path.display()))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, source_file);
    let first_bytes =
        read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?.context("hot metadata is empty")?;
    let first: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &first_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let ArchiveV2HotMetaRecord::Header(header) = first else {
        bail!("first hot metadata record is not a header");
    };
    let expected_flags = WINCODE_ARCHIVE_V2_FLAG_LEB128
        | WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY
        | WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS;
    ensure!(
        header.version == WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
        "unsupported first-seen hot metadata version {}",
        header.version
    );
    ensure!(
        header.flags == expected_flags,
        "first-seen hot metadata flags {:#x} do not exactly match expected {expected_flags:#x}",
        header.flags
    );
    let second_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("hot metadata is missing footer")?;
    let second: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &second_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let footer = match second {
        ArchiveV2HotMetaRecord::Footer(footer) => footer,
        ArchiveV2HotMetaRecord::Genesis(_) => {
            bail!("first-seen registry reprocessing safely rejects epoch-0 genesis metadata")
        }
        ArchiveV2HotMetaRecord::Header(_) => bail!("duplicate hot metadata header"),
    };
    ensure!(
        read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?.is_none(),
        "hot metadata contains trailing records after footer"
    );
    let output_file =
        File::create(&target_path).with_context(|| format!("create {}", target_path.display()))?;
    let mut writer =
        WincodeLeb128FramedWriter::new(BufWriter::with_capacity(IO_BUFFER_BYTES, output_file));
    writer.write(&ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
        version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
        flags: WINCODE_ARCHIVE_V2_FLAG_LEB128,
    }))?;
    writer.write(&ArchiveV2HotMetaRecord::Footer(footer.clone()))?;
    writer.flush()?;
    Ok(footer)
}

fn validate_target_meta(target: &Path) -> Result<()> {
    let path = target.join(ARCHIVE_V2_META_FILE);
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let first_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("target metadata is empty")?;
    let first: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &first_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let ArchiveV2HotMetaRecord::Header(header) = first else {
        bail!("target metadata does not start with a header");
    };
    ensure!(
        header.version == WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION
            && header.flags == WINCODE_ARCHIVE_V2_FLAG_LEB128,
        "target metadata is not canonical LEB128 Compact-V2"
    );
    let second_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("target metadata is missing footer")?;
    let second: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &second_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    ensure!(
        matches!(second, ArchiveV2HotMetaRecord::Footer(_)),
        "target metadata is missing its footer or contains genesis"
    );
    ensure!(
        read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?.is_none(),
        "target metadata has trailing records"
    );
    Ok(())
}

fn read_bounded_frame(reader: &mut impl Read, max_bytes: usize) -> Result<Option<Vec<u8>>> {
    let Some(len) = read_canonical_u32_varint(reader)? else {
        return Ok(None);
    };
    let len = len as usize;
    ensure!(
        len <= max_bytes,
        "wincode frame length {len} exceeds {max_bytes} byte limit"
    );
    let mut bytes = vec![0u8; len];
    reader
        .read_exact(&mut bytes)
        .context("truncated wincode frame")?;
    Ok(Some(bytes))
}

fn validate_hot_index(
    blocks_path: &Path,
    index: &blockzilla_format::ArchiveV2HotBlockIndex,
    epoch: u64,
) -> Result<()> {
    ensure!(
        index.flags == 0,
        "first-seen source hot index flags must be zero, got {:#x}",
        index.flags
    );
    ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS == 0,
        "raw hot-block sources are not supported"
    );
    let actual_bytes = fs::metadata(blocks_path)
        .with_context(|| format!("stat {}", blocks_path.display()))?
        .len();
    ensure!(
        actual_bytes == index.blob_file_bytes,
        "hot blocks size {actual_bytes} != index blob_file_bytes {}",
        index.blob_file_bytes
    );
    let mut compressed_offset = 0u64;
    let mut tx_ordinal = 0u64;
    let mut signature_ordinal = 0u64;
    let mut previous_slot = None;
    for (position, row) in index.rows.iter().enumerate() {
        ensure!(
            row.block_id as usize == position,
            "non-contiguous hot block ID at row {position}"
        );
        ensure!(
            row.slot / crate::SLOTS_PER_EPOCH == epoch,
            "slot {} is outside epoch {epoch}",
            row.slot
        );
        if let Some(previous) = previous_slot {
            ensure!(
                row.slot > previous,
                "hot index slots are not strictly increasing"
            );
        }
        ensure!(
            row.compressed_offset == compressed_offset,
            "non-contiguous compressed offset at block {}",
            row.block_id
        );
        ensure!(
            row.compressed_len > 0 && row.uncompressed_len > 0,
            "empty hot block {}",
            row.block_id
        );
        ensure!(
            u64::from(row.compressed_len) <= MAX_HOT_BLOCK_FRAME_BYTES
                && u64::from(row.uncompressed_len) <= MAX_HOT_BLOCK_FRAME_BYTES,
            "hot block {} frame lengths compressed={} uncompressed={} exceed {} byte limit",
            row.block_id,
            row.compressed_len,
            row.uncompressed_len,
            MAX_HOT_BLOCK_FRAME_BYTES
        );
        ensure!(
            row.first_tx_ordinal == tx_ordinal,
            "transaction ordinal discontinuity at block {}",
            row.block_id
        );
        ensure!(
            row.first_signature_ordinal == signature_ordinal,
            "signature ordinal discontinuity at block {}",
            row.block_id
        );
        compressed_offset = compressed_offset
            .checked_add(u64::from(row.compressed_len))
            .context("hot block compressed offset overflow")?;
        tx_ordinal = tx_ordinal
            .checked_add(u64::from(row.tx_count))
            .context("hot block transaction ordinal overflow")?;
        signature_ordinal = signature_ordinal
            .checked_add(u64::from(row.signature_count))
            .context("hot block signature ordinal overflow")?;
        previous_slot = Some(row.slot);
    }
    ensure!(
        compressed_offset == index.blob_file_bytes,
        "hot index does not cover blocks file exactly"
    );
    Ok(())
}

fn read_compressed_block_batch(
    file: &mut File,
    rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    mut hasher: Option<&mut Sha256>,
) -> Result<Vec<CompressedBlockInput>> {
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        output.push(read_compressed_block(file, *row, hasher.as_deref_mut())?);
    }
    Ok(output)
}

fn read_compressed_block(
    file: &mut File,
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    hasher: Option<&mut Sha256>,
) -> Result<CompressedBlockInput> {
    file.seek(SeekFrom::Start(row.compressed_offset))
        .with_context(|| format!("seek source block_id {}", row.block_id))?;
    let mut bytes = vec![0u8; row.compressed_len as usize];
    file.read_exact(&mut bytes)
        .with_context(|| format!("read source block_id {}", row.block_id))?;
    if let Some(hasher) = hasher {
        hasher.update(&bytes);
    }
    Ok(CompressedBlockInput {
        row,
        bytes,
        signatures: None,
    })
}

fn parallel_batch_rows(threads: usize) -> usize {
    // A deeper queue smooths large-block skew. hot_batch_end remains the hard aggregate memory
    // bound, so increasing the row cap does not increase the admitted byte budget.
    threads.saturating_mul(8).clamp(1, 256)
}

fn registry_rewrite_pipeline_memory_budget(threads: usize) -> Result<u64> {
    let worker_count =
        u64::try_from(threads).context("registry rewrite thread count exceeds u64")?;
    let persistent = worker_count
        .checked_mul(REGISTRY_REWRITE_WORKER_PERSISTENT_BYTES)
        .context("registry rewrite worker scratch reservation overflow")?;
    let available = HOT_BATCH_MEMORY_BUDGET_BYTES
        .checked_sub(persistent)
        .context("registry rewrite worker scratch exceeds the pass-2 memory budget")?;
    ensure!(
        available > 0,
        "registry rewrite worker scratch leaves no pass-2 pipeline memory"
    );
    Ok(available)
}

fn hot_batch_end(
    rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    start: usize,
    max_rows: usize,
    include_access: bool,
) -> Result<usize> {
    hot_batch_end_with_budget(
        rows,
        start,
        max_rows,
        include_access,
        HOT_BATCH_MEMORY_BUDGET_BYTES,
    )
}

fn hot_batch_end_with_budget(
    rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    start: usize,
    max_rows: usize,
    include_access: bool,
    memory_budget_bytes: u64,
) -> Result<usize> {
    ensure!(start < rows.len(), "hot batch start is outside index rows");
    ensure!(max_rows > 0, "hot batch row limit must be non-zero");
    ensure!(
        memory_budget_bytes > 0 && memory_budget_bytes <= HOT_BATCH_MEMORY_BUDGET_BYTES,
        "hot batch memory budget {memory_budget_bytes} is outside the supported range"
    );
    let mut bytes = 0u64;
    let mut end = start;
    while end < rows.len() && end - start < max_rows {
        let row = &rows[end];
        let row_bytes = hot_worker_reservation_bytes(row, include_access)?;
        ensure!(
            row_bytes <= memory_budget_bytes,
            "hot block {} advertises {} compressed+uncompressed bytes, exceeding {} byte batch limit",
            row.block_id,
            row_bytes,
            memory_budget_bytes
        );
        let next = bytes
            .checked_add(row_bytes)
            .context("hot batch byte total overflow")?;
        if end != start && next > memory_budget_bytes {
            break;
        }
        bytes = next;
        end += 1;
    }
    ensure!(end > start, "hot batch builder made no progress");
    Ok(end)
}

fn hot_worker_reservation_bytes(
    row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
    include_access: bool,
) -> Result<u64> {
    let signature_bytes = u64::from(row.signature_count)
        .checked_mul(64)
        .context("hot worker signature byte total overflow")?;
    let uncompressed_working_set = u64::from(row.uncompressed_len)
        .checked_mul(HOT_UNCOMPRESSED_WORKING_SET_MULTIPLIER)
        .context("hot worker uncompressed working-set estimate overflow")?;
    u64::from(row.compressed_len)
        .checked_add(uncompressed_working_set)
        .and_then(|bytes| bytes.checked_add(signature_bytes.saturating_mul(2)))
        // The shared 64 MiB access cap is reserved only while a worker owns the block. Once the
        // worker completes, the pipeline replaces this reservation with the actual retained Vec
        // capacities. This keeps the safety margin without imposing seven-row batch waves.
        .and_then(|bytes| {
            bytes.checked_add(if include_access {
                ARCHIVE_V2_BLOCK_ACCESS_MAX_FRAME_BYTES
            } else {
                0
            })
        })
        .and_then(|bytes| bytes.checked_add(ORDERED_PIPELINE_RESULT_OVERHEAD_BYTES))
        .context("hot worker advertised byte total overflow")
}

fn rewritten_block_retained_bytes(block: &RewrittenBlock) -> Result<u64> {
    fn vec_capacity_bytes<T>(values: &Vec<T>, label: &'static str) -> Result<u64> {
        let bytes = values
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .with_context(|| format!("{label} retained capacity overflow"))?;
        u64::try_from(bytes).with_context(|| format!("{label} retained capacity exceeds u64"))
    }

    let mut bytes = ORDERED_PIPELINE_RESULT_OVERHEAD_BYTES;
    for retained in [
        vec_capacity_bytes(&block.compressed, "compressed block")?,
        vec_capacity_bytes(&block.eligible, "eligible count runs")?,
        vec_capacity_bytes(&block.excluded, "excluded count runs")?,
    ] {
        bytes = bytes
            .checked_add(retained)
            .context("rewritten block retained capacity overflow")?;
    }
    if let Some(access) = &block.access {
        bytes = bytes
            .checked_add(vec_capacity_bytes(access, "block access")?)
            .context("rewritten block retained access capacity overflow")?;
    }
    Ok(bytes)
}

#[allow(clippy::too_many_arguments)]
fn run_bounded_ordered_pipeline<Input, Output, Reserve, ReadItem, Work, Retained, WriteItem>(
    pool: &rayon::ThreadPool,
    item_count: usize,
    max_workers: usize,
    memory_budget_bytes: u64,
    mut reservation_bytes: Reserve,
    mut read_item: ReadItem,
    work: Work,
    retained_bytes: Retained,
    mut write_item: WriteItem,
) -> Result<OrderedPipelineReport>
where
    Input: Send,
    Output: Send,
    Reserve: FnMut(usize) -> Result<u64> + Send,
    ReadItem: FnMut(usize) -> Result<Input> + Send,
    Work: Fn(Input) -> Result<Output> + Send + Sync,
    Retained: Fn(&Output) -> Result<u64> + Send,
    WriteItem: FnMut(usize, Output) -> Result<()> + Send,
{
    ensure!(
        max_workers > 0,
        "ordered pipeline worker limit must be non-zero"
    );
    ensure!(
        memory_budget_bytes > 0,
        "ordered pipeline memory budget must be non-zero"
    );

    // There can be at most max_workers completions in this channel because the coordinator never
    // admits more tasks. An unbounded sender is intentional: workers must never hold all worker
    // slots while waiting for the coordinator to receive a later sequence.
    let (completion_tx, completion_rx) = mpsc::channel::<OrderedPipelineCompletion<Output>>();
    let work = std::sync::Arc::new(work);
    // Keep the coordinator on its calling thread. `scope_fifo` would run this blocking receive
    // loop on one pool worker and would deadlock a one-thread pool; `in_place_scope_fifo` leaves
    // every configured pool thread available for block work.
    let (report, final_error) = pool.in_place_scope_fifo(move |scope| {
        let mut report = OrderedPipelineReport::default();
        let mut final_error = None;
        let mut next_admit = 0usize;
        let mut next_write = 0usize;
        let mut active_workers = 0usize;
        let mut accounted_bytes = 0u64;
        let mut pending = BTreeMap::<usize, OrderedPipelinePending<Output>>::new();
        let mut stop_admission = false;

        loop {
            while !stop_admission
                && next_admit < item_count
                && active_workers < max_workers
            {
                let sequence = next_admit;
                let reserved_bytes = match reservation_bytes(sequence) {
                    Ok(bytes) if bytes > 0 => bytes,
                    Ok(_) => {
                        pending.insert(
                            sequence,
                            OrderedPipelinePending::Failed(anyhow!(
                                "ordered pipeline item {sequence} has a zero-byte reservation"
                            )),
                        );
                        stop_admission = true;
                        break;
                    }
                    Err(error) => {
                        pending.insert(
                            sequence,
                            OrderedPipelinePending::Failed(
                                error.context("calculate ordered pipeline reservation"),
                            ),
                        );
                        stop_admission = true;
                        break;
                    }
                };
                if reserved_bytes > memory_budget_bytes {
                    pending.insert(
                        sequence,
                        OrderedPipelinePending::Failed(anyhow!(
                            "ordered pipeline item {sequence} reserves {reserved_bytes} bytes, exceeding {memory_budget_bytes} byte budget"
                        )),
                    );
                    stop_admission = true;
                    break;
                }
                let Some(next_accounted) = accounted_bytes.checked_add(reserved_bytes) else {
                    pending.insert(
                        sequence,
                        OrderedPipelinePending::Failed(anyhow!(
                            "ordered pipeline accounted byte total overflow"
                        )),
                    );
                    stop_admission = true;
                    break;
                };
                if next_accounted > memory_budget_bytes {
                    break;
                }

                let input = match read_item(sequence) {
                    Ok(input) => input,
                    Err(error) => {
                        pending.insert(
                            sequence,
                            OrderedPipelinePending::Failed(
                                error.context("read ordered pipeline input"),
                            ),
                        );
                        stop_admission = true;
                        break;
                    }
                };
                accounted_bytes = next_accounted;
                report.max_accounted_bytes = report.max_accounted_bytes.max(accounted_bytes);
                active_workers += 1;
                report.max_active_workers = report.max_active_workers.max(active_workers);
                report.admitted += 1;
                next_admit += 1;

                let completion_tx = completion_tx.clone();
                let work = std::sync::Arc::clone(&work);
                scope.spawn_fifo(move |_| {
                    let result = match catch_unwind(AssertUnwindSafe(|| work(input))) {
                        Ok(result) => result,
                        Err(payload) => {
                            let message = payload
                                .downcast_ref::<&str>()
                                .copied()
                                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                                .unwrap_or("non-string panic payload");
                            Err(anyhow!(
                                "ordered pipeline worker {sequence} panicked: {message}"
                            ))
                        }
                    };
                    // The receiver stays alive until every scoped worker has completed.
                    let _ = completion_tx.send(OrderedPipelineCompletion {
                        sequence,
                        reserved_bytes,
                        result,
                    });
                });
            }

            // Consume every now-contiguous result before waiting. This is the only output path, so
            // writes and all count/hash/progress updates remain in source order.
            while final_error.is_none() {
                let Some(entry) = pending.remove(&next_write) else {
                    break;
                };
                match entry {
                    OrderedPipelinePending::Ready {
                        retained_bytes,
                        value,
                    } => {
                        let result = write_item(next_write, value)
                            .with_context(|| format!("write ordered pipeline item {next_write}"));
                        accounted_bytes = accounted_bytes
                            .checked_sub(retained_bytes)
                            .expect("ordered pipeline retained-byte accounting underflow");
                        next_write += 1;
                        if let Err(error) = result {
                            final_error = Some(error);
                            stop_admission = true;
                        }
                    }
                    OrderedPipelinePending::Failed(error) => {
                        final_error = Some(error);
                        stop_admission = true;
                        next_write += 1;
                    }
                }
            }

            if final_error.is_some() && !pending.is_empty() {
                for (_, entry) in std::mem::take(&mut pending) {
                    if let OrderedPipelinePending::Ready { retained_bytes, .. } = entry {
                        accounted_bytes = accounted_bytes
                            .checked_sub(retained_bytes)
                            .expect("ordered pipeline pending-byte accounting underflow");
                    }
                }
            }

            if active_workers == 0 {
                if final_error.is_some()
                    || (next_admit == item_count && pending.is_empty())
                    || stop_admission
                {
                    break;
                }
                final_error = Some(anyhow!(
                    "ordered pipeline cannot admit item {next_admit} within the memory budget"
                ));
                break;
            }

            let completion = match completion_rx.recv() {
                Ok(completion) => completion,
                Err(_) => {
                    final_error = Some(anyhow!(
                        "ordered pipeline completion channel closed with {active_workers} active workers"
                    ));
                    break;
                }
            };
            active_workers -= 1;
            report.completed += 1;
            accounted_bytes = accounted_bytes
                .checked_sub(completion.reserved_bytes)
                .expect("ordered pipeline reservation accounting underflow");

            if final_error.is_some() {
                continue;
            }
            let pending_entry = match completion.result {
                Ok(value) => match retained_bytes(&value) {
                    Ok(bytes) if bytes <= completion.reserved_bytes => {
                        accounted_bytes = accounted_bytes
                            .checked_add(bytes)
                            .expect("ordered pipeline retained-byte total overflow");
                        report.max_accounted_bytes =
                            report.max_accounted_bytes.max(accounted_bytes);
                        OrderedPipelinePending::Ready {
                            retained_bytes: bytes,
                            value,
                        }
                    }
                    Ok(bytes) => {
                        stop_admission = true;
                        OrderedPipelinePending::Failed(anyhow!(
                            "ordered pipeline item {} retained {bytes} bytes after reserving only {} bytes",
                            completion.sequence,
                            completion.reserved_bytes
                        ))
                    }
                    Err(error) => {
                        stop_admission = true;
                        OrderedPipelinePending::Failed(
                            error.context("calculate ordered pipeline retained bytes"),
                        )
                    }
                },
                Err(error) => {
                    stop_admission = true;
                    OrderedPipelinePending::Failed(error)
                }
            };
            if pending
                .insert(completion.sequence, pending_entry)
                .is_some()
            {
                final_error = Some(anyhow!(
                    "ordered pipeline received duplicate sequence {}",
                    completion.sequence
                ));
                stop_admission = true;
            }
            report.max_pending_results = report.max_pending_results.max(pending.len());
        }

        debug_assert_eq!(active_workers, 0);
        debug_assert_eq!(accounted_bytes, 0);
        (report, final_error)
    });

    if let Some(error) = final_error {
        return Err(error);
    }
    ensure!(
        report.admitted == item_count && report.completed == item_count,
        "ordered pipeline completed {} of {} admitted items (expected {item_count})",
        report.completed,
        report.admitted
    );
    Ok(report)
}

fn decode_hot_block(input: &CompressedBlockInput) -> Result<ArchiveV2HotBlockBlob> {
    let decoded = zstd::bulk::decompress(&input.bytes, input.row.uncompressed_len as usize)
        .with_context(|| format!("zstd decompress block_id {}", input.row.block_id))?;
    ensure!(
        decoded.len() == input.row.uncompressed_len as usize,
        "block_id {} uncompressed length {} != index {}",
        input.row.block_id,
        decoded.len(),
        input.row.uncompressed_len
    );
    decode_hot_block_bytes(&decoded, input.row)
}

fn decode_hot_block_bytes(
    decoded: &[u8],
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
) -> Result<ArchiveV2HotBlockBlob> {
    decode_hot_block_bytes_with_schema(decoded, row).map(|decoded| decoded.block)
}

type BorrowedCurrentRewriteHotBlock<'a> = (
    ArchiveV2HotBlockHeader,
    u32,
    &'a [[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]],
    &'a [u8],
    &'a [u8],
);

fn decode_current_rewrite_hot_block_borrowed(
    decoded: &[u8],
) -> wincode::ReadResult<BorrowedCurrentRewriteHotBlock<'_>> {
    wincode::config::deserialize_exact(
        decoded,
        bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
    )
}

fn validate_hot_block_header_and_rows(
    header: &ArchiveV2HotBlockHeader,
    tx_count: u32,
    rows: &[ArchiveV2HotTxRow],
    index_row: blockzilla_format::ArchiveV2HotBlockIndexRow,
) -> Result<()> {
    ensure!(
        header.slot == index_row.slot,
        "block/index slot mismatch at block_id {}",
        index_row.block_id
    );
    ensure!(
        tx_count == index_row.tx_count,
        "block/index tx_count mismatch at block_id {}",
        index_row.block_id
    );
    ensure!(
        rows.len() == index_row.tx_count as usize,
        "block tx row count mismatch at block_id {}",
        index_row.block_id
    );
    let signatures = rows.iter().try_fold(0u32, |sum, row| {
        sum.checked_add(u32::from(row.signature_count))
            .context("block signature count overflow")
    })?;
    ensure!(
        signatures == index_row.signature_count,
        "block/index signature_count mismatch at block_id {}: rows={} index={}",
        index_row.block_id,
        signatures,
        index_row.signature_count
    );
    Ok(())
}

fn decode_legacy_hot_block_bytes(
    decoded: &[u8],
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    current_error: &str,
) -> Result<DecodedHotBlock> {
    let (block, outer_schema): (ArchiveV2HotBlockBlob, HotBlockOuterSchema) =
        match wincode::config::deserialize_exact::<LegacyHotBlockWithShredding, _>(
            decoded,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        ) {
            Ok(block) => (block.into(), HotBlockOuterSchema::LegacyShredding),
            Err(shredding_error) => {
                let legacy: LegacyHotBlockWithRewardsVec = wincode::config::deserialize_exact(
                    decoded,
                    bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
                )
                .with_context(|| {
                    format!(
                        "decode hot block_id {}: current={current_error}; legacy-shredding={shredding_error}",
                        row.block_id
                    )
                })?;
                (legacy.into(), HotBlockOuterSchema::LegacyRewardsVec)
            }
        };
    validate_hot_block_header_and_rows(&block.header, block.tx_count, &block.tx_rows, row)?;
    Ok(DecodedHotBlock {
        block,
        outer_schema,
    })
}

fn decode_hot_block_bytes_with_schema(
    decoded: &[u8],
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
) -> Result<DecodedHotBlock> {
    let (block, outer_schema) = match wincode::config::deserialize_exact::<ArchiveV2HotBlockBlob, _>(
        decoded,
        bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
    ) {
        Ok(block) => (block, HotBlockOuterSchema::Current),
        Err(current_error) => {
            match wincode::config::deserialize_exact::<LegacyHotBlockWithShredding, _>(
                decoded,
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            ) {
                Ok(block) => (block.into(), HotBlockOuterSchema::LegacyShredding),
                Err(shredding_error) => {
                    let legacy: LegacyHotBlockWithRewardsVec = wincode::config::deserialize_exact(
                    decoded,
                    bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
                )
                .with_context(|| {
                    format!(
                        "decode hot block_id {}: current={current_error}; legacy-shredding={shredding_error}",
                        row.block_id
                    )
                })?;
                    (legacy.into(), HotBlockOuterSchema::LegacyRewardsVec)
                }
            }
        }
    };
    ensure!(
        block.header.slot == row.slot,
        "block/index slot mismatch at block_id {}",
        row.block_id
    );
    ensure!(
        block.tx_count == row.tx_count,
        "block/index tx_count mismatch at block_id {}",
        row.block_id
    );
    ensure!(
        block.tx_rows.len() == row.tx_count as usize,
        "block tx row count mismatch at block_id {}",
        row.block_id
    );
    let signatures = block.tx_rows.iter().try_fold(0u32, |sum, row| {
        sum.checked_add(u32::from(row.signature_count))
            .context("block signature count overflow")
    })?;
    ensure!(
        signatures == row.signature_count,
        "block/index signature_count mismatch at block_id {}: rows={} index={}",
        row.block_id,
        signatures,
        row.signature_count
    );
    Ok(DecodedHotBlock {
        block,
        outer_schema,
    })
}

fn analyze_source_exclusions(input: CompressedBlockInput) -> Result<SourceExclusionAnalysis> {
    let row = input.row;
    let decoded = zstd::bulk::decompress(&input.bytes, row.uncompressed_len as usize)
        .with_context(|| format!("zstd decompress block_id {}", row.block_id))?;
    ensure!(
        decoded.len() == row.uncompressed_len as usize,
        "block_id {} uncompressed length {} != index {}",
        row.block_id,
        decoded.len(),
        row.uncompressed_len
    );

    type BorrowedCurrentHotBlock<'a> = (
        Pass1CurrentHotBlockHeader,
        u32,
        &'a [[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]],
        &'a [u8],
        &'a [u8],
    );
    let current = wincode::config::deserialize_exact::<BorrowedCurrentHotBlock<'_>, _>(
        &decoded,
        bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
    );

    let current_error = match current {
        Ok((header, tx_count, tx_rows, message_bytes, metadata_bytes)) => {
            let mut header = header.into();
            let rows = tx_rows.iter().map(decode_hot_tx_row);
            return analyze_source_exclusion_regions(
                row,
                &mut header,
                tx_count,
                tx_rows.len(),
                rows,
                message_bytes,
                metadata_bytes,
            );
        }
        Err(error) => error.to_string(),
    };

    // Historical outer schemas are uncommon. They borrow the same large row/message/metadata
    // regions as the current schema, discard shredding, and retain only rewards under the smaller
    // pass-1 field limit. A current-schema cap failure never reaches a 512 MiB owned decoder.
    type BorrowedLegacyShreddingHotBlock<'a> = (
        Pass1LegacyHotBlockHeaderWithShredding,
        u32,
        &'a [[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]],
        &'a [u8],
        &'a [u8],
    );
    let legacy_shredding =
        wincode::config::deserialize_exact::<BorrowedLegacyShreddingHotBlock<'_>, _>(
            &decoded,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        );
    let shredding_error = match legacy_shredding {
        Ok((header, tx_count, tx_rows, message_bytes, metadata_bytes)) => {
            let mut header = header.into();
            let rows = tx_rows.iter().map(decode_hot_tx_row);
            return analyze_source_exclusion_regions(
                row,
                &mut header,
                tx_count,
                tx_rows.len(),
                rows,
                message_bytes,
                metadata_bytes,
            );
        }
        Err(error) => error.to_string(),
    };

    type BorrowedLegacyRewardsVecHotBlock<'a> = (
        Pass1LegacyHotBlockHeaderWithRewardsVec,
        u32,
        &'a [[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]],
        &'a [u8],
        &'a [u8],
    );
    let (header, tx_count, tx_rows, message_bytes, metadata_bytes):
        BorrowedLegacyRewardsVecHotBlock<'_> = wincode::config::deserialize_exact(
        &decoded,
        bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
    )
    .with_context(|| {
        format!(
            "decode pass-1 hot block_id {}: current={current_error}; legacy-shredding={shredding_error}",
            row.block_id
        )
    })?;
    let mut header = header.into();
    let rows = tx_rows.iter().map(decode_hot_tx_row);
    analyze_source_exclusion_regions(
        row,
        &mut header,
        tx_count,
        tx_rows.len(),
        rows,
        message_bytes,
        metadata_bytes,
    )
}

fn decode_hot_tx_row(bytes: &[u8; ARCHIVE_V2_HOT_TX_ROW_LEN]) -> ArchiveV2HotTxRow {
    ArchiveV2HotTxRow {
        tx_index: u32::from_le_bytes(bytes[0..4].try_into().expect("fixed tx row field")),
        flags: u32::from_le_bytes(bytes[4..8].try_into().expect("fixed tx row field")),
        message_offset: u32::from_le_bytes(bytes[8..12].try_into().expect("fixed tx row field")),
        message_len: u32::from_le_bytes(bytes[12..16].try_into().expect("fixed tx row field")),
        metadata_offset: u32::from_le_bytes(bytes[16..20].try_into().expect("fixed tx row field")),
        metadata_len: u32::from_le_bytes(bytes[20..24].try_into().expect("fixed tx row field")),
        signature_count: bytes[24],
        reserved: bytes[25..28].try_into().expect("fixed tx row field"),
    }
}

fn validate_tx_index_permutation<I>(indices: I, row_count: usize, slot: u64) -> Result<()>
where
    I: Iterator<Item = u32> + Clone,
{
    let mut observed = 0usize;
    let mut canonical = true;
    for (position, tx_index) in indices.clone().enumerate() {
        observed = observed
            .checked_add(1)
            .context("transaction row count overflow")?;
        canonical &= tx_index as usize == position;
    }
    ensure!(
        observed == row_count,
        "slot {slot} has {observed} transaction indexes for {row_count} rows"
    );
    if canonical {
        return Ok(());
    }

    let words = row_count
        .checked_add(63)
        .context("transaction permutation bitset length overflow")?
        / 64;
    let mut seen = Vec::new();
    seen.try_reserve_exact(words)
        .context("allocate transaction permutation bitset")?;
    seen.resize(words, 0u64);
    for tx_index in indices {
        let index = tx_index as usize;
        ensure!(
            index < row_count,
            "slot {slot} tx_index {tx_index} is outside 0..{row_count}"
        );
        let word = index / 64;
        let mask = 1u64 << (index % 64);
        ensure!(
            seen[word] & mask == 0,
            "slot {slot} has duplicate tx_index {tx_index}"
        );
        seen[word] |= mask;
    }
    Ok(())
}

fn analyze_source_exclusion_regions(
    row: blockzilla_format::ArchiveV2HotBlockIndexRow,
    header: &mut ArchiveV2HotBlockHeader,
    tx_count: u32,
    tx_rows_len: usize,
    tx_rows: impl Iterator<Item = ArchiveV2HotTxRow> + Clone,
    message_bytes: &[u8],
    metadata_bytes: &[u8],
) -> Result<SourceExclusionAnalysis> {
    let mut excluded_ids = Vec::new();
    let mut push_excluded = |key: &mut CompactPubkey, class: ReferenceClass| -> Result<()> {
        ensure!(
            class == ReferenceClass::Excluded,
            "count-only pass received an eligible reference"
        );
        let CompactPubkey::Id(id) = *key else {
            bail!(
                "strict first-seen block {} contains a raw typed CompactPubkey",
                row.block_id
            );
        };
        push_bounded_reference_id(&mut excluded_ids, id, row.block_id)
    };

    ensure!(
        header.slot == row.slot,
        "block/index slot mismatch at block_id {}",
        row.block_id
    );
    ensure!(
        tx_count == row.tx_count,
        "block/index tx_count mismatch at block_id {}",
        row.block_id
    );
    ensure!(
        tx_rows_len == row.tx_count as usize,
        "block tx row count mismatch at block_id {}",
        row.block_id
    );

    if let Some(rewards) = &mut header.rewards {
        for reward in &mut rewards.decoded {
            push_excluded(&mut reward.pubkey, ReferenceClass::Excluded)?;
        }
    }

    // Messages contain only canonical eligible references, so pass 1 validates their regions but
    // does not decode them. Metadata must be decoded because structured logs contain the other
    // excluded reference class. Pass 2 performs the strict full traversal before publication.
    let mut message_cursor = 0usize;
    let mut metadata_cursor = 0usize;
    let mut signatures = 0u32;
    validate_tx_index_permutation(
        tx_rows.clone().map(|tx| tx.tx_index),
        tx_rows_len,
        header.slot,
    )?;
    for tx in tx_rows {
        signatures = signatures
            .checked_add(u32::from(tx.signature_count))
            .context("block signature count overflow")?;
        ensure!(
            tx.reserved == [0; 3],
            "tx row {} has non-zero reserved bytes",
            tx.tx_index
        );
        ensure!(
            tx.message_offset as usize == message_cursor,
            "tx row {} message offset {} is not canonical cursor {}",
            tx.tx_index,
            tx.message_offset,
            message_cursor
        );
        checked_region(
            message_bytes,
            tx.message_offset,
            tx.message_len,
            header.slot,
            tx.tx_index,
            "message",
        )?;
        message_cursor = message_cursor
            .checked_add(tx.message_len as usize)
            .context("source message region cursor overflow")?;

        ensure!(
            tx.metadata_offset as usize == metadata_cursor,
            "tx row {} metadata offset {} is not canonical cursor {}",
            tx.tx_index,
            tx.metadata_offset,
            metadata_cursor
        );
        let metadata = checked_region(
            metadata_bytes,
            tx.metadata_offset,
            tx.metadata_len,
            header.slot,
            tx.tx_index,
            "metadata",
        )?;
        metadata_cursor = metadata_cursor
            .checked_add(tx.metadata_len as usize)
            .context("source metadata region cursor overflow")?;
        if tx.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                metadata.is_empty(),
                "tx row {} has metadata bytes without HAS_METADATA",
                tx.tx_index
            );
        } else {
            ensure!(
                !metadata.is_empty(),
                "tx row {} declares empty metadata",
                tx.tx_index
            );
            if tx.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK == 0 {
                scan_metadata_excluded_pubkeys(metadata, &mut push_excluded).with_context(
                    || {
                        format!(
                            "scan hot metadata block_id={} tx_index={}",
                            row.block_id, tx.tx_index
                        )
                    },
                )?;
            }
        }
    }
    ensure!(
        message_cursor == message_bytes.len(),
        "hot block message rows cover {message_cursor} of {} bytes",
        message_bytes.len()
    );
    ensure!(
        metadata_cursor == metadata_bytes.len(),
        "hot block metadata rows cover {metadata_cursor} of {} bytes",
        metadata_bytes.len()
    );
    ensure!(
        signatures == row.signature_count,
        "block/index signature_count mismatch at block_id {}: rows={} index={}",
        row.block_id,
        signatures,
        row.signature_count
    );

    Ok(SourceExclusionAnalysis {
        excluded: compress_id_counts(excluded_ids)?,
        transactions: row.tx_count,
    })
}

fn analyze_source_block(
    input: CompressedBlockInput,
    registry: &MappedRegistry,
    wire_profile: ArchiveV2WireProfile,
) -> Result<SourceBlockAnalysis> {
    let row = input.row;
    let mut block = decode_hot_block(&input)?;
    let mut eligible_ids = Vec::new();
    let mut all_ids = Vec::new();
    let semantic = normalize_block(
        &mut block,
        u64::from(row.block_id),
        row.slot,
        wire_profile,
        |key, class| {
            let CompactPubkey::Id(id) = *key else {
                bail!(
                    "strict first-seen block {} contains a raw typed CompactPubkey",
                    row.block_id
                );
            };
            let raw = registry.key(id)?;
            push_bounded_reference_id(&mut all_ids, id, row.block_id)?;
            if class == ReferenceClass::Eligible {
                push_bounded_reference_id(&mut eligible_ids, id, row.block_id)?;
            }
            Ok(raw)
        },
    )?;
    Ok(SourceBlockAnalysis {
        eligible: compress_id_counts(eligible_ids)?,
        all: compress_id_counts(all_ids)?,
        semantic,
    })
}

fn push_bounded_reference_id(ids: &mut Vec<u32>, id: u32, block_id: u32) -> Result<()> {
    ensure!(
        ids.len() < MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK,
        "block {block_id} exceeds the {} typed-pubkey-reference safety limit",
        MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK
    );
    if ids.len() == ids.capacity() {
        let remaining = MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK - ids.len();
        let additional = ids.capacity().max(1_024).min(remaining);
        ids.try_reserve_exact(additional)
            .context("allocate bounded block pubkey-reference vector")?;
    }
    ids.push(id);
    Ok(())
}

fn rewrite_source_block(
    input: CompressedBlockInput,
    source_registry: &MappedRegistry,
    old_to_new: &[u32],
    target_registry: &MappedRegistry,
    level: i32,
    wire_profile: ArchiveV2WireProfile,
    access_context: Option<&AccessBuildContext>,
    phase_timing_enabled: bool,
) -> Result<RewrittenBlock> {
    with_registry_rewrite_worker_scratch(level, |scratch| {
        rewrite_source_block_with_scratch(
            input,
            source_registry,
            old_to_new,
            target_registry,
            wire_profile,
            access_context,
            phase_timing_enabled,
            scratch,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn rewrite_source_block_with_scratch(
    mut input: CompressedBlockInput,
    source_registry: &MappedRegistry,
    old_to_new: &[u32],
    target_registry: &MappedRegistry,
    wire_profile: ArchiveV2WireProfile,
    access_context: Option<&AccessBuildContext>,
    phase_timing_enabled: bool,
    scratch: &mut RegistryRewriteWorkerScratch,
) -> Result<RewrittenBlock> {
    let row = input.row;
    let mut phase_timing = Pass2PhaseTiming::default();
    let decompress_started = phase_timing_enabled.then(Instant::now);
    decompress_zstd_reused(
        &mut scratch.decompressor,
        &input.bytes,
        input.row.uncompressed_len as usize,
        &mut scratch.decoded,
    )
    .with_context(|| format!("zstd decompress block_id {}", input.row.block_id))?;
    if let Some(started) = decompress_started {
        phase_timing.zstd_decompress = started.elapsed();
    }

    let mut eligible_ids = Vec::new();
    let mut excluded_ids = Vec::new();
    let mut references = 0u64;
    let outer_decode_started = phase_timing_enabled.then(Instant::now);
    let current = decode_current_rewrite_hot_block_borrowed(&scratch.decoded);
    let (mut block, outer_schema, access_references) = match current {
        Ok((mut header, tx_count, row_bytes, message_bytes, metadata_bytes)) => {
            scratch.rows.clear();
            if scratch.rows.capacity() < row_bytes.len() {
                scratch
                    .rows
                    .try_reserve_exact(row_bytes.len().saturating_sub(scratch.rows.len()))
                    .context("reserve registry rewrite transaction rows")?;
            }
            scratch.rows.extend(row_bytes.iter().map(decode_hot_tx_row));
            validate_hot_block_header_and_rows(&header, tx_count, &scratch.rows, row)?;
            if let Some(started) = outer_decode_started {
                phase_timing.outer_decode = started.elapsed();
            }

            let mut access_references = access_context.map(|_| {
                let mut references = AccessReferenceSet::default();
                super::collect_access_blockhash_id(
                    header.blockhash_id as i32,
                    &mut references.blockhash_ids,
                );
                super::collect_access_blockhash_id(
                    header.previous_blockhash_id as i32,
                    &mut references.blockhash_ids,
                );
                references
            });
            let rewrite_started = phase_timing_enabled.then(Instant::now);
            {
                let mut visitor = RegistryWireVisitor {
                    source_registry,
                    old_to_new,
                    block_id: row.block_id,
                    eligible_ids: &mut eligible_ids,
                    excluded_ids: &mut excluded_ids,
                    references: &mut references,
                    access: access_references.as_mut(),
                };
                rewrite_registry_current_regions_with_access_wire(
                    &mut header,
                    &mut scratch.rows,
                    message_bytes,
                    metadata_bytes,
                    wire_profile,
                    &mut visitor,
                    &mut phase_timing,
                    &mut scratch.target_messages,
                    &mut scratch.target_metadata,
                )?;
            }
            if let Some(started) = rewrite_started {
                phase_timing.message_metadata_rewrite = started.elapsed();
            }
            let block = ArchiveV2HotBlockBlob {
                header,
                tx_count,
                tx_rows: std::mem::take(&mut scratch.rows),
                message_bytes: std::mem::take(&mut scratch.target_messages),
                metadata_bytes: std::mem::take(&mut scratch.target_metadata),
            };
            (block, HotBlockOuterSchema::Current, access_references)
        }
        Err(current_error) => {
            let current_error = current_error.to_string();
            let decoded_block =
                decode_legacy_hot_block_bytes(&scratch.decoded, row, &current_error)?;
            ensure!(
                decoded_block.outer_schema != HotBlockOuterSchema::Current,
                "borrowed current decoder rejected block {} but owned routing accepted it as current",
                row.block_id
            );
            let outer_schema = decoded_block.outer_schema;
            let mut block = decoded_block.block;
            if let Some(started) = outer_decode_started {
                phase_timing.outer_decode = started.elapsed();
            }
            let mut access_references = access_context.map(|_| {
                let mut references = AccessReferenceSet::default();
                super::collect_access_blockhash_id(
                    block.header.blockhash_id as i32,
                    &mut references.blockhash_ids,
                );
                super::collect_access_blockhash_id(
                    block.header.previous_blockhash_id as i32,
                    &mut references.blockhash_ids,
                );
                references
            });
            let rewrite_started = phase_timing_enabled.then(Instant::now);
            {
                let mut visitor = RegistryWireVisitor {
                    source_registry,
                    old_to_new,
                    block_id: row.block_id,
                    eligible_ids: &mut eligible_ids,
                    excluded_ids: &mut excluded_ids,
                    references: &mut references,
                    access: access_references.as_mut(),
                };
                rewrite_registry_block_pubkeys_with_access_wire(
                    &mut block,
                    wire_profile,
                    &mut visitor,
                    &mut phase_timing,
                )?;
            }
            if let Some(started) = rewrite_started {
                phase_timing.message_metadata_rewrite = started.elapsed();
            }
            (block, outer_schema, access_references)
        }
    };

    let access_started = phase_timing_enabled.then(Instant::now);
    let access = match (access_context, access_references) {
        (Some(context), Some(references)) => {
            let signatures = input
                .signatures
                .as_deref()
                .context("access rebuild is missing block signature bytes")?;
            let blob = build_block_access_from_collected_references(
                &block,
                references,
                |id| target_registry.key(id),
                &context.blockhashes,
                &context.previous_tail,
                signatures,
                &context.vote_hashes,
            )?;
            let mut access_output = Vec::new();
            let written =
                serialize_bounded_into(&mut access_output, &blob, MAX_ACCESS_FRAME_BYTES_USIZE)
                    .with_context(|| format!("serialize block access {}", row.block_id))?;
            ensure!(
                written == access_output.len(),
                "rebuilt block-access {} serialized length changed",
                row.block_id
            );
            Some(access_output)
        }
        (None, None) => {
            ensure!(
                input.signatures.is_none(),
                "block signatures were attached without an access rebuild"
            );
            None
        }
        _ => bail!(
            "block-access rebuild state mismatch at block_id {}",
            row.block_id
        ),
    };
    if let Some(started) = access_started {
        phase_timing.access_build_serialize = started.elapsed();
    }

    let serialize_started = phase_timing_enabled.then(Instant::now);
    scratch.encoded.clear();
    reserve_byte_capacity(
        &mut scratch.encoded,
        input.row.uncompressed_len as usize,
        "registry rewrite whole block",
    )?;
    let encoded_len = serialize_bounded_into(
        &mut scratch.encoded,
        &block,
        MAX_HOT_BLOCK_FRAME_BYTES_USIZE,
    )
    .with_context(|| format!("serialize rewritten block_id {}", row.block_id))?;
    ensure!(
        encoded_len == scratch.encoded.len(),
        "rewritten hot block {} serialized length changed",
        row.block_id
    );
    let uncompressed_len =
        u32::try_from(scratch.encoded.len()).context("rewritten hot block exceeds u32::MAX")?;
    if let Some(started) = serialize_started {
        phase_timing.whole_block_serialize = started.elapsed();
    }

    let compress_started = phase_timing_enabled.then(Instant::now);
    compress_zstd_reused(
        &mut scratch.compressor,
        &scratch.encoded,
        &mut scratch.compressed,
    )
    .with_context(|| format!("zstd compress rewritten block_id {}", row.block_id))?;
    input.bytes.clear();
    reserve_byte_capacity(
        &mut input.bytes,
        scratch.compressed.len(),
        "rewritten compressed block result",
    )?;
    input.bytes.extend_from_slice(&scratch.compressed);
    if let Some(started) = compress_started {
        phase_timing.zstd_compress = started.elapsed();
    }

    if outer_schema == HotBlockOuterSchema::Current {
        scratch.rows = std::mem::take(&mut block.tx_rows);
        scratch.target_messages = std::mem::take(&mut block.message_bytes);
        scratch.target_metadata = std::mem::take(&mut block.metadata_bytes);
    }

    let stats = BlockRewriteStats {
        block_id: u64::from(row.block_id),
        slot: row.slot,
        transactions: row.tx_count,
        references,
    };
    let count_sort_started = phase_timing_enabled.then(Instant::now);
    let eligible = compress_id_counts(eligible_ids)?;
    let excluded = compress_id_counts(excluded_ids)?;
    if let Some(started) = count_sort_started {
        phase_timing.count_run_sort = started.elapsed();
    }
    Ok(RewrittenBlock {
        row,
        compressed: input.bytes,
        uncompressed_len,
        stats,
        eligible,
        excluded,
        access,
        phase_timing,
    })
}

#[inline]
fn remap_source_pubkey(
    key: &mut CompactPubkey,
    class: ReferenceClass,
    old_to_new: &[u32],
    mut resolve_raw: impl FnMut(u32) -> Result<[u8; 32]>,
    block_id: u32,
) -> Result<u32> {
    let CompactPubkey::Id(old_id) = *key else {
        bail!("strict first-seen block {block_id} contains a raw typed CompactPubkey");
    };
    ensure!(old_id != 0, "compact pubkey uses reserved ID 0");
    let index = usize::try_from(old_id - 1).context("old pubkey ID exceeds usize")?;
    let new_id = *old_to_new
        .get(index)
        .ok_or_else(|| anyhow!("old pubkey ID {old_id} is outside remap"))?;
    if class == ReferenceClass::Eligible {
        ensure!(
            new_id != 0,
            "eligible pubkey ID {old_id} was excluded from target registry"
        );
    }
    *key = if new_id == 0 {
        CompactPubkey::raw(resolve_raw(old_id)?)
    } else {
        // validate_registry_remap proves once per retained registry key that this target ID
        // resolves to the same raw key. Do not repeat the source-registry lookup for every use.
        CompactPubkey::id(new_id)
    };
    Ok(old_id)
}

impl RegistryWireVisitor<'_> {
    fn snapshot(&self) -> RegistryWireCheckpoint {
        RegistryWireCheckpoint {
            eligible_len: self.eligible_ids.len(),
            excluded_len: self.excluded_ids.len(),
            references: *self.references,
            access_lengths: self.access.as_deref().map(|access| {
                (
                    access.pubkey_ids.len(),
                    access.blockhash_ids.len(),
                    access.vote_hash_block_ids.len(),
                )
            }),
        }
    }

    fn rewrite_value(
        &mut self,
        source: CompactPubkey,
        class: ReferenceClass,
    ) -> Result<CompactPubkey> {
        ensure!(
            *self.references < MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK as u64,
            "block {} exceeds the {} typed-pubkey-reference safety limit",
            self.block_id,
            MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK
        );
        let next_references = (*self.references)
            .checked_add(1)
            .context("block rewrite pubkey reference overflow")?;
        let mut rewritten = source;
        let source_registry = self.source_registry;
        let old_id = remap_source_pubkey(
            &mut rewritten,
            class,
            self.old_to_new,
            |id| source_registry.key(id),
            self.block_id,
        )?;
        match class {
            ReferenceClass::Eligible => {
                push_bounded_reference_id(self.eligible_ids, old_id, self.block_id)?;
            }
            ReferenceClass::Excluded => {
                push_bounded_reference_id(self.excluded_ids, old_id, self.block_id)?;
            }
        }
        *self.references = next_references;
        if let Some(access) = self.access.as_deref_mut() {
            super::collect_access_pubkey_id(rewritten, &mut access.pubkey_ids);
        }
        Ok(rewritten)
    }

    fn rewrite_in_place(&mut self, key: &mut CompactPubkey, class: ReferenceClass) -> Result<()> {
        *key = self.rewrite_value(*key, class)?;
        Ok(())
    }

    fn remaining_reference_capacity(&self) -> Result<usize> {
        let used = usize::try_from(*self.references)
            .context("block pubkey reference count exceeds usize")?;
        MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK
            .checked_sub(used)
            .context("block pubkey reference count exceeds safety limit")
    }
}

impl ArchiveV2WireRewriteVisitor for RegistryWireVisitor<'_> {
    type Checkpoint = RegistryWireCheckpoint;

    fn checkpoint(&mut self) -> Self::Checkpoint {
        self.snapshot()
    }

    fn rewrite_pubkey(
        &mut self,
        pubkey: CompactPubkey,
        class: ArchiveV2WireReferenceClass,
    ) -> Result<CompactPubkey> {
        self.rewrite_value(
            pubkey,
            match class {
                ArchiveV2WireReferenceClass::Eligible => ReferenceClass::Eligible,
                ArchiveV2WireReferenceClass::Excluded => ReferenceClass::Excluded,
            },
        )
    }

    fn recent_blockhash_id(&mut self, id: i32) -> Result<()> {
        if let Some(access) = self.access.as_deref_mut() {
            super::collect_access_blockhash_id(id, &mut access.blockhash_ids);
        }
        Ok(())
    }

    fn vote_hash_block_id(&mut self, block_id: u32) -> Result<()> {
        if let Some(access) = self.access.as_deref_mut() {
            access.vote_hash_block_ids.push(block_id);
        }
        Ok(())
    }

    fn rollback(&mut self, checkpoint: Self::Checkpoint) {
        self.eligible_ids.truncate(checkpoint.eligible_len);
        self.excluded_ids.truncate(checkpoint.excluded_len);
        *self.references = checkpoint.references;
        match (self.access.as_deref_mut(), checkpoint.access_lengths) {
            (Some(access), Some((pubkeys, blockhashes, vote_hashes))) => {
                access.pubkey_ids.truncate(pubkeys);
                access.blockhash_ids.truncate(blockhashes);
                access.vote_hash_block_ids.truncate(vote_hashes);
            }
            (None, None) => {}
            _ => debug_assert!(false, "registry wire access checkpoint state changed"),
        }
    }
}

fn build_block_access_from_collected_references(
    block: &ArchiveV2HotBlockBlob,
    mut references: AccessReferenceSet,
    mut resolve_pubkey: impl FnMut(u32) -> Result<[u8; 32]>,
    blockhash_registry: &[[u8; 32]],
    previous_tail: &[super::PreviousBlockhash],
    block_signature_bytes: &[u8],
    vote_hash_rows: &[super::VoteHashRegistryRow],
) -> Result<ArchiveV2BlockAccessBlob> {
    let expected_signature_bytes = block.tx_rows.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.signature_count as usize * 64)
            .context("block signature bytes overflow")
    })?;
    ensure!(
        expected_signature_bytes == block_signature_bytes.len(),
        "block access signature length mismatch for slot {}: rows={} bytes={}",
        block.header.slot,
        expected_signature_bytes,
        block_signature_bytes.len()
    );

    let blockhash = super::resolve_access_blockhash_id(
        block.header.blockhash_id as i32,
        blockhash_registry,
        previous_tail,
    )
    .with_context(|| {
        format!(
            "resolve blockhash id {} for slot {}",
            block.header.blockhash_id, block.header.slot
        )
    })?;
    let previous_blockhash =
        super::resolve_access_previous_blockhash(block, blockhash_registry, previous_tail)
            .with_context(|| {
                format!(
                    "resolve previous blockhash id {} for slot {}",
                    block.header.previous_blockhash_id, block.header.slot
                )
            })?;

    references.pubkey_ids.sort_unstable();
    references.pubkey_ids.dedup();
    references.blockhash_ids.sort_unstable();
    references.blockhash_ids.dedup();
    references.vote_hash_block_ids.sort_unstable();
    references.vote_hash_block_ids.dedup();

    let pubkeys = references
        .pubkey_ids
        .into_iter()
        .map(|id| {
            let pubkey = resolve_pubkey(id)?;
            Ok(ArchiveV2BlockAccessPubkey { id, pubkey })
        })
        .collect::<Result<Vec<_>>>()?;
    let blockhashes = references
        .blockhash_ids
        .into_iter()
        .map(|id| {
            let blockhash =
                super::resolve_access_blockhash_id(id, blockhash_registry, previous_tail)
                    .with_context(|| format!("blockhash id {id} is outside loaded registry"))?;
            Ok(ArchiveV2BlockAccessBlockhash { id, blockhash })
        })
        .collect::<Result<Vec<_>>>()?;
    let vote_hashes = references
        .vote_hash_block_ids
        .into_iter()
        .map(|block_id| {
            let row = vote_hash_rows
                .get(block_id as usize)
                .copied()
                .with_context(|| {
                    format!("vote hash registry row {block_id} is outside loaded registry")
                })?;
            Ok(ArchiveV2BlockAccessVoteHash {
                block_id,
                bank_hash: row.bank_hash,
                block_id_hash: row.block_id_hash,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ArchiveV2BlockAccessBlob {
        version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
        flags: 0,
        blockhash,
        previous_blockhash,
        signature_counts: block
            .tx_rows
            .iter()
            .map(|row| row.signature_count)
            .collect(),
        signatures: block_signature_bytes.to_vec(),
        pubkeys,
        blockhashes,
        vote_hashes,
    })
}

fn resolve_compact_pubkey(key: CompactPubkey, registry: &MappedRegistry) -> Result<[u8; 32]> {
    match key {
        CompactPubkey::Id(id) => registry.key(id),
        CompactPubkey::Raw(raw) => Ok(raw),
    }
}

fn validate_registry_remap(
    source_registry: &MappedRegistry,
    old_to_new: &[u32],
    target_registry: &MappedRegistry,
) -> Result<()> {
    ensure!(
        old_to_new.len() == source_registry.len,
        "registry remap length does not match source registry"
    );
    for (index, (&source_raw, &new_id)) in source_registry.keys().iter().zip(old_to_new).enumerate()
    {
        if new_id == 0 {
            continue;
        }
        ensure!(
            target_registry.key(new_id)? == source_raw,
            "remapped pubkey ID {} does not preserve its raw key",
            index + 1
        );
    }
    Ok(())
}

fn normalize_block(
    block: &mut ArchiveV2HotBlockBlob,
    block_id: u64,
    slot: u64,
    wire_profile: ArchiveV2WireProfile,
    mut resolve: impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<[u8; 32]>,
) -> Result<BlockSemantic> {
    let mut reference = Sha256::new();
    reference.update(SEMANTIC_DOMAIN);
    reference.update(b".block-references");
    reference.update(block_id.to_le_bytes());
    reference.update(slot.to_le_bytes());
    let mut references = 0u64;
    rewrite_block_pubkeys(block, wire_profile, |key, class| {
        let raw = resolve(key, class)?;
        reference.update([match class {
            ReferenceClass::Eligible => 1,
            ReferenceClass::Excluded => 0,
        }]);
        reference.update(raw);
        references = references
            .checked_add(1)
            .context("block semantic pubkey reference overflow")?;
        // A one-byte registry ID is a representation-neutral placeholder. The ordered reference
        // digest above binds the actual resolved key and class, while this keeps normalization
        // from expanding each reference to a 33-byte raw sentinel.
        *key = CompactPubkey::id(1);
        Ok(())
    })?;
    let normalized_size = wincode::config::serialized_size(&*block, wincode_leb128_config())?;
    ensure!(
        normalized_size <= MAX_HOT_BLOCK_FRAME_BYTES,
        "normalized hot block {block_id} would encode to {normalized_size} bytes, exceeding {}",
        MAX_HOT_BLOCK_FRAME_BYTES
    );
    let normalized = wincode::config::serialize(block, wincode_leb128_config())?;
    Ok(BlockSemantic {
        block_id,
        slot,
        transactions: block.tx_count,
        references,
        reference_sha256: reference.finalize().into(),
        normalized_len: normalized.len() as u64,
        normalized_sha256: Sha256::digest(&normalized).into(),
    })
}

#[cfg(test)]
fn normalize_block_structure(
    block: &mut ArchiveV2HotBlockBlob,
    block_id: u32,
) -> Result<(u64, [u8; 32])> {
    rewrite_block_pubkeys(
        block,
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
        |key, _class| {
            *key = CompactPubkey::id(1);
            Ok(())
        },
    )?;
    hash_normalized_block_structure(block, block_id)
}

#[cfg(test)]
fn hash_normalized_block_structure(
    block: &ArchiveV2HotBlockBlob,
    block_id: u32,
) -> Result<(u64, [u8; 32])> {
    let normalized_size = wincode::config::serialized_size(&*block, wincode_leb128_config())?;
    ensure!(
        normalized_size <= MAX_HOT_BLOCK_FRAME_BYTES,
        "normalized hot block {block_id} would encode to {normalized_size} bytes, exceeding {}",
        MAX_HOT_BLOCK_FRAME_BYTES
    );
    let normalized = wincode::config::serialize(block, wincode_leb128_config())?;
    Ok((normalized.len() as u64, Sha256::digest(&normalized).into()))
}

fn compress_id_counts(mut ids: Vec<u32>) -> Result<Vec<(u32, u32)>> {
    ids.sort_unstable();
    let mut runs = Vec::<(u32, u32)>::new();
    runs.try_reserve_exact(ids.len())
        .context("allocate bounded block pubkey count runs")?;
    for id in ids {
        if let Some((last_id, count)) = runs.last_mut()
            && *last_id == id
        {
            *count = count
                .checked_add(1)
                .context("per-block pubkey reference count overflow")?;
        } else {
            runs.push((id, 1u32));
        }
    }
    Ok(runs)
}

fn merge_count_runs(counts: &mut [u32], runs: &[(u32, u32)], subtract: bool) -> Result<()> {
    for &(id, value) in runs {
        ensure!(id != 0, "compact pubkey ID 0 is reserved");
        let slot = counts
            .get_mut((id - 1) as usize)
            .ok_or_else(|| anyhow!("pubkey ID {id} is outside registry count vector"))?;
        if subtract {
            *slot = slot.checked_sub(value).ok_or_else(|| {
                anyhow!("typed references for pubkey ID {id} exceed registry_counts.bin")
            })?;
        } else {
            *slot = slot.saturating_add(value);
        }
    }
    Ok(())
}

fn validate_consumed_reference_counts(
    all_counts_remaining: &[u32],
    eligible_counts_remaining: &[u32],
) -> Result<()> {
    ensure!(
        all_counts_remaining.iter().all(|&remaining| remaining == 0),
        "registry_counts.bin does not exactly match typed CompactPubkey references"
    );
    ensure!(
        eligible_counts_remaining
            .iter()
            .all(|&remaining| remaining == 0),
        "count-only canonical registry counts do not match pass-2 eligible references"
    );
    Ok(())
}

/// Test-only reference model for the retired owned message rewrite.
///
/// Field order and enum variant order are part of the stored schema. Keep this mirror in exact
/// lockstep with the public format type. The account-key vectors stay owned because pass 2 mutates
/// them. Instruction byte regions and address-lookup index regions borrow the source message until
/// the rewritten value has been serialized.
#[cfg(test)]
#[derive(Debug, SchemaRead, SchemaWrite)]
enum BorrowedHotMessagePayload<'a> {
    Legacy(BorrowedHotLegacyMessage<'a>),
    V0(BorrowedHotV0Message<'a>),
}

#[cfg(test)]
#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedHotLegacyMessage<'a> {
    header: CompactMessageHeader,
    account_keys: Vec<CompactPubkey>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: Vec<BorrowedHotInstruction<'a>>,
}

#[cfg(test)]
#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedHotV0Message<'a> {
    header: CompactMessageHeader,
    account_keys: Vec<CompactPubkey>,
    recent_blockhash: OwnedCompactRecentBlockhash,
    instructions: Vec<BorrowedHotInstruction<'a>>,
    address_table_lookups: Vec<BorrowedHotAddressTableLookup<'a>>,
}

#[cfg(test)]
#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedHotInstruction<'a> {
    program_id_index: u8,
    accounts: &'a [u8],
    data: BorrowedHotInstructionData<'a>,
}

#[cfg(test)]
#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedHotAddressTableLookup<'a> {
    account_key: CompactPubkey,
    writable_indexes: &'a [u8],
    readonly_indexes: &'a [u8],
}

#[cfg(test)]
#[derive(Debug, SchemaRead, SchemaWrite)]
enum BorrowedHotInstructionData<'a> {
    Raw(&'a [u8]),
    UnknownSystem(&'a [u8]),
    UnknownVote(&'a [u8]),
    ComputeBudget(ArchiveV2ComputeBudgetInstructionData),
    System(ArchiveV2SystemInstructionData),
    VoteCompactUpdateVoteState(ArchiveV2VoteStateUpdate),
    VoteCompactUpdateVoteStateSwitch {
        update: ArchiveV2VoteStateUpdate,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
    VoteTowerSync(ArchiveV2VoteTowerSync),
    VoteTowerSyncSwitch {
        tower: ArchiveV2VoteTowerSync,
        switch_proof_hash: ArchiveV2VoteHashRef,
    },
}

/// Allocation-light wire mirror for the common successful metadata schema.
///
/// Only byte payloads borrow the source. Integer and length-bearing vectors remain owned, so
/// reserialization preserves the owned path's canonical LEB128 behavior. Every key-bearing value
/// and every log event also remains owned because pass 2 mutates registry references in place.
#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedCompactMetaV1<'a> {
    err: Option<CompactTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<BorrowedCompactInnerInstructions<'a>>>,
    logs: Option<BorrowedCompactLogStream<'a>>,
    pre_token_balances: Vec<CompactTokenBalance>,
    post_token_balances: Vec<CompactTokenBalance>,
    rewards: Vec<CompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
    return_data: Option<BorrowedCompactReturnData<'a>>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedCompactInnerInstructions<'a> {
    index: u32,
    instructions: Vec<BorrowedCompactInnerInstruction<'a>>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedCompactInnerInstruction<'a> {
    program_id_index: u32,
    accounts: &'a [u8],
    data: &'a [u8],
    stack_height: Option<u32>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedCompactLogStream<'a> {
    events: Vec<LogEvent>,
    strings: BorrowedStringTable<'a>,
    data: BorrowedDataTable<'a>,
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedStringTable<'a> {
    lengths: Vec<u32>,
    bytes: &'a [u8],
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedDataTable<'a> {
    arrays: Vec<blockzilla_format::DataArray>,
    chunk_lengths: Vec<u32>,
    bytes: &'a [u8],
}

#[derive(Debug, SchemaRead, SchemaWrite)]
struct BorrowedCompactReturnData<'a> {
    program_id: CompactPubkey,
    data: &'a [u8],
}

#[derive(Debug)]
enum RewritableCompactMetaV1<'a> {
    Borrowed(BorrowedCompactMetaV1<'a>),
    Owned(CompactMetaV1),
}

impl RewritableCompactMetaV1<'_> {
    fn visit_pubkeys(
        &mut self,
        visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Borrowed(metadata) => visit_borrowed_metadata_pubkeys(metadata, visit),
            Self::Owned(metadata) => visit_metadata_pubkeys(metadata, visit),
        }
    }

    fn serialized_size(&self) -> Result<usize> {
        let size = match self {
            Self::Borrowed(metadata) => {
                wincode::config::serialized_size(metadata, wincode_leb128_config())?
            }
            Self::Owned(metadata) => {
                wincode::config::serialized_size(metadata, wincode_leb128_config())?
            }
        };
        usize::try_from(size).context("rewritten metadata size exceeds usize")
    }

    fn serialize_into(&self, output: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Borrowed(metadata) => {
                wincode::config::serialize_into(output, metadata, wincode_leb128_config())?
            }
            Self::Owned(metadata) => {
                wincode::config::serialize_into(output, metadata, wincode_leb128_config())?
            }
        }
        Ok(())
    }
}

fn registry_wire_rewrite_limits(
    target_message_bytes: usize,
    target_metadata_bytes: usize,
    visitor: &RegistryWireVisitor<'_>,
) -> Result<ArchiveV2WireRewriteLimits> {
    let used = target_message_bytes
        .checked_add(target_metadata_bytes)
        .context("rewritten hot payload size overflow")?;
    let max_output_bytes = MAX_HOT_BLOCK_FRAME_BYTES_USIZE
        .checked_sub(used)
        .context("rewritten hot payload exceeds frame limit")?;
    Ok(ArchiveV2WireRewriteLimits {
        max_output_bytes,
        max_pubkey_references: visitor.remaining_reference_capacity()?,
        ..ArchiveV2WireRewriteLimits::default()
    })
}

fn ensure_hot_payload_append(
    target_message_bytes: usize,
    target_metadata_bytes: usize,
    additional_bytes: usize,
    tx_index: u32,
) -> Result<()> {
    let projected = target_message_bytes
        .checked_add(target_metadata_bytes)
        .and_then(|size| size.checked_add(additional_bytes))
        .context("rewritten hot payload size overflow")?;
    ensure!(
        projected <= MAX_HOT_BLOCK_FRAME_BYTES_USIZE,
        "rewritten hot payload exceeds {} byte limit at tx_index {}",
        MAX_HOT_BLOCK_FRAME_BYTES,
        tx_index
    );
    Ok(())
}

fn validate_registry_wire_stats(
    before: RegistryWireCheckpoint,
    stats: ArchiveV2WireRewriteStats,
    input_bytes: usize,
    output_bytes: usize,
    visitor: &RegistryWireVisitor<'_>,
    tx_index: u32,
    label: &str,
) -> Result<()> {
    let eligible = visitor
        .eligible_ids
        .len()
        .checked_sub(before.eligible_len)
        .context("wire rewrite eligible reference length regressed")?;
    let excluded = visitor
        .excluded_ids
        .len()
        .checked_sub(before.excluded_len)
        .context("wire rewrite excluded reference length regressed")?;
    let references = (*visitor.references)
        .checked_sub(before.references)
        .context("wire rewrite reference count regressed")?;
    ensure!(
        stats.input_bytes == input_bytes && stats.output_bytes == output_bytes,
        "wire {label} byte stats mismatch at tx_index {tx_index}"
    );
    ensure!(
        stats.eligible_pubkey_references == eligible
            && stats.excluded_pubkey_references == excluded
            && references == (eligible + excluded) as u64,
        "wire {label} reference stats mismatch at tx_index {tx_index}"
    );
    match (before.access_lengths, visitor.access.as_deref()) {
        (Some((_, blockhashes_before, vote_hashes_before)), Some(access)) => {
            let recent_blockhashes = access
                .blockhash_ids
                .len()
                .checked_sub(blockhashes_before)
                .context("wire rewrite blockhash access length regressed")?;
            let vote_hashes = access
                .vote_hash_block_ids
                .len()
                .checked_sub(vote_hashes_before)
                .context("wire rewrite vote-hash access length regressed")?;
            ensure!(
                stats.recent_blockhash_ids == recent_blockhashes
                    && stats.vote_hash_block_ids == vote_hashes,
                "wire {label} non-pubkey access stats mismatch at tx_index {tx_index}"
            );
        }
        (None, None) => {}
        _ => bail!("wire {label} access state changed at tx_index {tx_index}"),
    }
    Ok(())
}

fn append_raw_hot_payload(
    output: &mut Vec<u8>,
    other_output_bytes: usize,
    source: &[u8],
    tx_index: u32,
    label: &str,
) -> Result<()> {
    ensure_hot_payload_append(output.len(), other_output_bytes, source.len(), tx_index)?;
    output
        .try_reserve(source.len())
        .with_context(|| format!("reserve raw {label} payload"))?;
    output.extend_from_slice(source);
    Ok(())
}

fn rewrite_metadata_decoder_fallback(
    metadata: &[u8],
    target_metadata: &mut Vec<u8>,
    target_message_bytes: usize,
    visitor: &mut RegistryWireVisitor<'_>,
    tx_index: u32,
) -> Result<()> {
    let mut decoded = decode_rewritable_compact_metadata(metadata)
        .with_context(|| format!("decode fallback hot metadata tx_index={tx_index}"))?;
    decoded.visit_pubkeys(&mut |key, class| visitor.rewrite_in_place(key, class))?;
    let encoded_size = decoded.serialized_size()?;
    ensure_hot_payload_append(
        target_message_bytes,
        target_metadata.len(),
        encoded_size,
        tx_index,
    )?;
    target_metadata
        .try_reserve(encoded_size)
        .context("reserve rewritten fallback metadata payload")?;
    let output_start = target_metadata.len();
    decoded.serialize_into(target_metadata)?;
    ensure!(
        target_metadata.len() - output_start == encoded_size,
        "fallback metadata serialized size changed at tx_index {tx_index}"
    );
    Ok(())
}

fn rewrite_registry_block_pubkeys_with_access_wire(
    block: &mut ArchiveV2HotBlockBlob,
    wire_profile: ArchiveV2WireProfile,
    visitor: &mut RegistryWireVisitor<'_>,
    phase_timing: &mut Pass2PhaseTiming,
) -> Result<()> {
    let source_messages = std::mem::take(&mut block.message_bytes);
    let source_metadata = std::mem::take(&mut block.metadata_bytes);
    let mut target_messages = Vec::with_capacity(source_messages.len());
    let mut target_metadata = Vec::with_capacity(source_metadata.len());
    rewrite_registry_current_regions_with_access_wire(
        &mut block.header,
        &mut block.tx_rows,
        &source_messages,
        &source_metadata,
        wire_profile,
        visitor,
        phase_timing,
        &mut target_messages,
        &mut target_metadata,
    )?;
    block.message_bytes = target_messages;
    block.metadata_bytes = target_metadata;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn rewrite_registry_current_regions_with_access_wire(
    header: &mut ArchiveV2HotBlockHeader,
    rows: &mut [ArchiveV2HotTxRow],
    source_messages: &[u8],
    source_metadata: &[u8],
    wire_profile: ArchiveV2WireProfile,
    visitor: &mut RegistryWireVisitor<'_>,
    phase_timing: &mut Pass2PhaseTiming,
    target_messages: &mut Vec<u8>,
    target_metadata: &mut Vec<u8>,
) -> Result<()> {
    if let Some(rewards) = &mut header.rewards {
        for reward in &mut rewards.decoded {
            visitor.rewrite_in_place(&mut reward.pubkey, ReferenceClass::Excluded)?;
        }
    }

    target_messages.clear();
    target_metadata.clear();
    reserve_byte_capacity(
        target_messages,
        source_messages.len(),
        "registry rewrite message region",
    )?;
    reserve_byte_capacity(
        target_metadata,
        source_metadata.len(),
        "registry rewrite metadata region",
    )?;
    let mut source_message_cursor = 0usize;
    let mut source_metadata_cursor = 0usize;
    validate_tx_index_permutation(rows.iter().map(|row| row.tx_index), rows.len(), header.slot)?;
    for row in rows.iter_mut() {
        ensure!(
            row.reserved == [0; 3],
            "tx row {} has non-zero reserved bytes",
            row.tx_index
        );
        ensure!(
            row.message_offset as usize == source_message_cursor,
            "tx row {} message offset {} is not canonical cursor {}",
            row.tx_index,
            row.message_offset,
            source_message_cursor
        );
        source_message_cursor = source_message_cursor
            .checked_add(row.message_len as usize)
            .context("source message region cursor overflow")?;
        let message = checked_region(
            &source_messages,
            row.message_offset,
            row.message_len,
            header.slot,
            row.tx_index,
            "message",
        )?;
        row.message_offset = u32::try_from(target_messages.len())
            .context("rewritten message region exceeds u32::MAX")?;
        if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            append_raw_hot_payload(
                target_messages,
                target_metadata.len(),
                message,
                row.tx_index,
                "message",
            )?;
        } else {
            let before = visitor.snapshot();
            let output_start = target_messages.len();
            let limits = registry_wire_rewrite_limits(
                target_messages.len(),
                target_metadata.len(),
                visitor,
            )?;
            match ArchiveV2MessageProjector::new(wire_profile).rewrite_message_wire(
                message,
                target_messages,
                visitor,
                limits,
            ) {
                Ok(stats) => {
                    validate_registry_wire_stats(
                        before,
                        stats,
                        message.len(),
                        target_messages.len() - output_start,
                        visitor,
                        row.tx_index,
                        "message",
                    )?;
                    phase_timing.wire_message_fast_records += 1;
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error)
                        .context(format!("rewrite hot message tx_index={}", row.tx_index)));
                }
            }
        }
        row.message_len = u32::try_from(target_messages.len() - row.message_offset as usize)
            .context("rewritten message payload exceeds u32::MAX")?;

        let source_metadata_offset = row.metadata_offset;
        let source_metadata_len = row.metadata_len;
        ensure!(
            source_metadata_offset as usize == source_metadata_cursor,
            "tx row {} metadata offset {} is not canonical cursor {}",
            row.tx_index,
            source_metadata_offset,
            source_metadata_cursor
        );
        source_metadata_cursor = source_metadata_cursor
            .checked_add(source_metadata_len as usize)
            .context("source metadata region cursor overflow")?;
        row.metadata_offset = u32::try_from(target_metadata.len())
            .context("rewritten metadata region exceeds u32::MAX")?;
        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                source_metadata_len == 0,
                "tx row {} has metadata bytes without HAS_METADATA",
                row.tx_index
            );
        } else {
            ensure!(
                source_metadata_len > 0,
                "tx row {} declares empty metadata",
                row.tx_index
            );
            let metadata = checked_region(
                &source_metadata,
                source_metadata_offset,
                source_metadata_len,
                header.slot,
                row.tx_index,
                "metadata",
            )?;
            if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                append_raw_hot_payload(
                    target_metadata,
                    target_messages.len(),
                    metadata,
                    row.tx_index,
                    "metadata",
                )?;
            } else {
                let before = visitor.snapshot();
                let output_start = target_metadata.len();
                let limits = registry_wire_rewrite_limits(
                    target_messages.len(),
                    target_metadata.len(),
                    visitor,
                )?;
                match rewrite_archive_v2_metadata_wire(metadata, target_metadata, visitor, limits) {
                    Ok(stats) => {
                        validate_registry_wire_stats(
                            before,
                            stats,
                            metadata.len(),
                            target_metadata.len() - output_start,
                            visitor,
                            row.tx_index,
                            "metadata",
                        )?;
                        phase_timing.wire_metadata_fast_records += 1;
                        match stats.metadata_error_schema {
                            None => phase_timing.wire_metadata_success_fast_records += 1,
                            Some(ArchiveV2WireMetadataErrorSchema::Current) => {
                                phase_timing.wire_metadata_current_error_fast_records += 1;
                            }
                            Some(ArchiveV2WireMetadataErrorSchema::Legacy) => {
                                phase_timing.wire_metadata_legacy_error_fast_records += 1;
                            }
                        }
                    }
                    Err(error)
                        if matches!(error.kind(), ArchiveV2WireRewriteErrorKind::Fallback(_)) =>
                    {
                        ensure!(
                            target_metadata.len() == output_start && visitor.snapshot() == before,
                            "wire metadata fallback did not roll back tx_index {}",
                            row.tx_index
                        );
                        phase_timing.wire_metadata_fallback_records += 1;
                        match error.fallback_reason() {
                            Some(ArchiveV2WireFallbackReason::MetadataErrorSchemaAmbiguous) => {
                                phase_timing.wire_metadata_ambiguous_fallback_records += 1;
                            }
                            Some(
                                ArchiveV2WireFallbackReason::MetadataErrorPrefixRequiresOwnedFallback,
                            ) => {
                                phase_timing.wire_metadata_error_prefix_fallback_records += 1;
                            }
                            Some(ArchiveV2WireFallbackReason::MetadataErrorWireRollback) => {
                                phase_timing.wire_metadata_rollback_fallback_records += 1;
                            }
                            _ => {}
                        }
                        rewrite_metadata_decoder_fallback(
                            metadata,
                            target_metadata,
                            target_messages.len(),
                            visitor,
                            row.tx_index,
                        )?;
                    }
                    Err(error) => {
                        return Err(anyhow::Error::new(error)
                            .context(format!("rewrite hot metadata tx_index={}", row.tx_index)));
                    }
                }
            }
        }
        row.metadata_len = u32::try_from(target_metadata.len() - row.metadata_offset as usize)
            .context("rewritten metadata payload exceeds u32::MAX")?;
    }
    ensure!(
        source_message_cursor == source_messages.len(),
        "hot block message rows cover {source_message_cursor} of {} bytes",
        source_messages.len()
    );
    ensure!(
        source_metadata_cursor == source_metadata.len(),
        "hot block metadata rows cover {source_metadata_cursor} of {} bytes",
        source_metadata.len()
    );
    Ok(())
}

struct CallbackWireVisitor<'visit, 'access, F> {
    visit: &'visit mut F,
    access: Option<&'access mut AccessReferenceSet>,
}

impl<F> ArchiveV2WireRewriteVisitor for CallbackWireVisitor<'_, '_, F>
where
    F: FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
{
    type Checkpoint = Option<(usize, usize, usize)>;

    fn checkpoint(&mut self) -> Self::Checkpoint {
        self.access.as_ref().map(|access| {
            (
                access.pubkey_ids.len(),
                access.blockhash_ids.len(),
                access.vote_hash_block_ids.len(),
            )
        })
    }

    fn rewrite_pubkey(
        &mut self,
        mut pubkey: CompactPubkey,
        class: ArchiveV2WireReferenceClass,
    ) -> anyhow::Result<CompactPubkey> {
        let class = match class {
            ArchiveV2WireReferenceClass::Eligible => ReferenceClass::Eligible,
            ArchiveV2WireReferenceClass::Excluded => ReferenceClass::Excluded,
        };
        (self.visit)(&mut pubkey, class)?;
        if let Some(access) = self.access.as_deref_mut() {
            super::collect_access_pubkey_id(pubkey, &mut access.pubkey_ids);
        }
        Ok(pubkey)
    }

    fn recent_blockhash_id(&mut self, id: i32) -> anyhow::Result<()> {
        if let Some(access) = self.access.as_deref_mut() {
            super::collect_access_blockhash_id(id, &mut access.blockhash_ids);
        }
        Ok(())
    }

    fn vote_hash_block_id(&mut self, block_id: u32) -> anyhow::Result<()> {
        if let Some(access) = self.access.as_deref_mut() {
            access.vote_hash_block_ids.push(block_id);
        }
        Ok(())
    }

    fn rollback(&mut self, checkpoint: Self::Checkpoint) {
        if let (Some(access), Some((pubkeys, blockhashes, vote_hashes))) =
            (self.access.as_deref_mut(), checkpoint)
        {
            access.pubkey_ids.truncate(pubkeys);
            access.blockhash_ids.truncate(blockhashes);
            access.vote_hash_block_ids.truncate(vote_hashes);
        }
    }
}

fn rewrite_block_pubkeys(
    block: &mut ArchiveV2HotBlockBlob,
    wire_profile: ArchiveV2WireProfile,
    visit: impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    rewrite_block_pubkeys_with_access(block, wire_profile, None, visit)
}

fn rewrite_block_pubkeys_with_access(
    block: &mut ArchiveV2HotBlockBlob,
    wire_profile: ArchiveV2WireProfile,
    mut access: Option<&mut AccessReferenceSet>,
    mut visit: impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    if let Some(rewards) = &mut block.header.rewards {
        for reward in &mut rewards.decoded {
            // Match the canonical direct-CAR split_compact/PreHot registry pass: block rewards
            // remain semantically encoded but do not influence usage-sorted registry IDs.
            visit_mapped_pubkey_with_access(
                &mut reward.pubkey,
                ReferenceClass::Excluded,
                &mut access,
                &mut visit,
            )?;
        }
    }

    let source_messages = std::mem::take(&mut block.message_bytes);
    let source_metadata = std::mem::take(&mut block.metadata_bytes);
    let mut target_messages = Vec::with_capacity(source_messages.len());
    let mut target_metadata = Vec::with_capacity(source_metadata.len());
    let mut source_message_cursor = 0usize;
    let mut source_metadata_cursor = 0usize;
    validate_tx_index_permutation(
        block.tx_rows.iter().map(|row| row.tx_index),
        block.tx_rows.len(),
        block.header.slot,
    )?;
    for row in &mut block.tx_rows {
        ensure!(
            row.reserved == [0; 3],
            "tx row {} has non-zero reserved bytes",
            row.tx_index
        );
        ensure!(
            row.message_offset as usize == source_message_cursor,
            "tx row {} message offset {} is not canonical cursor {}",
            row.tx_index,
            row.message_offset,
            source_message_cursor
        );
        source_message_cursor = source_message_cursor
            .checked_add(row.message_len as usize)
            .context("source message region cursor overflow")?;
        let message = checked_region(
            &source_messages,
            row.message_offset,
            row.message_len,
            block.header.slot,
            row.tx_index,
            "message",
        )?;
        row.message_offset = u32::try_from(target_messages.len())
            .context("rewritten message region exceeds u32::MAX")?;
        if row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
            target_messages.extend_from_slice(message);
        } else {
            let used = target_messages
                .len()
                .checked_add(target_metadata.len())
                .context("rewritten hot payload size overflow")?;
            let max_output_bytes = MAX_HOT_BLOCK_FRAME_BYTES_USIZE
                .checked_sub(used)
                .context("rewritten hot payload exceeds frame limit")?;
            let mut visitor = CallbackWireVisitor {
                visit: &mut visit,
                access: access.as_deref_mut(),
            };
            ArchiveV2MessageProjector::new(wire_profile)
                .rewrite_message_wire(
                    message,
                    &mut target_messages,
                    &mut visitor,
                    ArchiveV2WireRewriteLimits {
                        max_output_bytes,
                        max_pubkey_references: MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK,
                        ..ArchiveV2WireRewriteLimits::default()
                    },
                )
                .map_err(anyhow::Error::new)
                .with_context(|| format!("rewrite hot message tx_index={}", row.tx_index))?;
        }
        row.message_len = u32::try_from(target_messages.len() - row.message_offset as usize)
            .context("rewritten message payload exceeds u32::MAX")?;

        let source_metadata_offset = row.metadata_offset;
        let source_metadata_len = row.metadata_len;
        ensure!(
            source_metadata_offset as usize == source_metadata_cursor,
            "tx row {} metadata offset {} is not canonical cursor {}",
            row.tx_index,
            source_metadata_offset,
            source_metadata_cursor
        );
        source_metadata_cursor = source_metadata_cursor
            .checked_add(source_metadata_len as usize)
            .context("source metadata region cursor overflow")?;
        row.metadata_offset = u32::try_from(target_metadata.len())
            .context("rewritten metadata region exceeds u32::MAX")?;
        if row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                source_metadata_len == 0,
                "tx row {} has metadata bytes without HAS_METADATA",
                row.tx_index
            );
        } else {
            ensure!(
                source_metadata_len > 0,
                "tx row {} declares empty metadata",
                row.tx_index
            );
            let metadata = checked_region(
                &source_metadata,
                source_metadata_offset,
                source_metadata_len,
                block.header.slot,
                row.tx_index,
                "metadata",
            )?;
            if row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
                target_metadata.extend_from_slice(metadata);
            } else {
                let mut decoded = decode_rewritable_compact_metadata(metadata)
                    .with_context(|| format!("decode hot metadata tx_index={}", row.tx_index))?;
                decoded.visit_pubkeys(&mut |key, class| {
                    visit_mapped_pubkey_with_access(key, class, &mut access, &mut visit)
                })?;
                let encoded_size = decoded.serialized_size()?;
                let projected = target_messages
                    .len()
                    .checked_add(target_metadata.len())
                    .and_then(|size| size.checked_add(encoded_size))
                    .context("rewritten hot payload size overflow")?;
                ensure!(
                    projected <= MAX_HOT_BLOCK_FRAME_BYTES_USIZE,
                    "rewritten hot payload exceeds {} byte limit at tx_index {}",
                    MAX_HOT_BLOCK_FRAME_BYTES,
                    row.tx_index
                );
                target_metadata
                    .try_reserve(encoded_size)
                    .context("reserve rewritten metadata payload")?;
                decoded.serialize_into(&mut target_metadata)?;
            }
        }
        row.metadata_len = u32::try_from(target_metadata.len() - row.metadata_offset as usize)
            .context("rewritten metadata payload exceeds u32::MAX")?;
    }
    ensure!(
        source_message_cursor == source_messages.len(),
        "hot block message rows cover {source_message_cursor} of {} bytes",
        source_messages.len()
    );
    ensure!(
        source_metadata_cursor == source_metadata.len(),
        "hot block metadata rows cover {source_metadata_cursor} of {} bytes",
        source_metadata.len()
    );
    block.message_bytes = target_messages;
    block.metadata_bytes = target_metadata;
    Ok(())
}

#[inline]
fn visit_mapped_pubkey_with_access(
    key: &mut CompactPubkey,
    class: ReferenceClass,
    access: &mut Option<&mut AccessReferenceSet>,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    visit(key, class)?;
    if let Some(access) = access.as_deref_mut() {
        super::collect_access_pubkey_id(*key, &mut access.pubkey_ids);
    }
    Ok(())
}

#[cfg(test)]
#[inline]
fn collect_access_message_non_pubkey_refs(
    message: &BorrowedHotMessagePayload<'_>,
    access: &mut AccessReferenceSet,
) {
    let (recent_blockhash, instructions) = match message {
        BorrowedHotMessagePayload::Legacy(message) => {
            (&message.recent_blockhash, message.instructions.as_slice())
        }
        BorrowedHotMessagePayload::V0(message) => {
            (&message.recent_blockhash, message.instructions.as_slice())
        }
    };
    super::collect_access_recent_blockhash_id(recent_blockhash, &mut access.blockhash_ids);
    for instruction in instructions {
        collect_access_borrowed_instruction_vote_hash_refs(
            &instruction.data,
            &mut access.vote_hash_block_ids,
        );
    }
}

#[cfg(test)]
#[inline]
fn collect_access_borrowed_instruction_vote_hash_refs(
    data: &BorrowedHotInstructionData<'_>,
    vote_hash_block_ids: &mut Vec<u32>,
) {
    match data {
        BorrowedHotInstructionData::VoteCompactUpdateVoteState(update)
        | BorrowedHotInstructionData::VoteCompactUpdateVoteStateSwitch { update, .. } => {
            super::collect_access_vote_hash_ref(update.hash, vote_hash_block_ids);
        }
        BorrowedHotInstructionData::VoteTowerSync(tower)
        | BorrowedHotInstructionData::VoteTowerSyncSwitch { tower, .. } => {
            super::collect_access_vote_hash_ref(tower.update.hash, vote_hash_block_ids);
            super::collect_access_vote_hash_ref(tower.block_id_hash, vote_hash_block_ids);
        }
        BorrowedHotInstructionData::Raw(_)
        | BorrowedHotInstructionData::UnknownSystem(_)
        | BorrowedHotInstructionData::UnknownVote(_)
        | BorrowedHotInstructionData::ComputeBudget(_)
        | BorrowedHotInstructionData::System(_) => {}
    }
}

fn checked_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    slot: u64,
    tx_index: u32,
    label: &str,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let end = start
        .checked_add(len as usize)
        .context("hot block region offset overflow")?;
    bytes.get(start..end).ok_or_else(|| {
        anyhow!(
            "slot {slot} tx_index={tx_index} {label} slice offset={offset} len={len} is outside {} bytes",
            bytes.len()
        )
    })
}

fn decode_rewritable_compact_metadata(bytes: &[u8]) -> Result<RewritableCompactMetaV1<'_>> {
    if bytes.first() == Some(&0) {
        decode_successful_borrowed_metadata_with_limit::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(bytes)
            .map(RewritableCompactMetaV1::Borrowed)
    } else {
        decode_compact_metadata(bytes).map(RewritableCompactMetaV1::Owned)
    }
}

fn decode_successful_borrowed_metadata_with_limit<const LIMIT: usize>(
    bytes: &[u8],
) -> Result<BorrowedCompactMetaV1<'_>> {
    ensure!(
        bytes.first() == Some(&0),
        "successful compact metadata wire gate requires err=None"
    );
    let metadata: BorrowedCompactMetaV1<'_> =
        wincode::config::deserialize_exact(bytes, bounded_wincode_config::<LIMIT>())?;
    ensure!(
        metadata.err.is_none(),
        "successful compact metadata wire gate decoded a present error"
    );
    Ok(metadata)
}

fn decode_compact_metadata(bytes: &[u8]) -> Result<CompactMetaV1> {
    decode_compact_metadata_with_limit::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(bytes)
}

fn decode_compact_metadata_with_limit<const LIMIT: usize>(bytes: &[u8]) -> Result<CompactMetaV1> {
    // Current and legacy metadata differ only in the payload type of a present transaction error.
    // None has the same one-byte wire form in both generations and is the common path. Exact
    // current-schema decoding therefore selects the only logical value without a second decode or
    // two canonical reserializations. Pass 1 applies the same fail-closed schema selection rule.
    if bytes.first() == Some(&0) {
        return wincode::config::deserialize_exact::<CompactMetaV1, _>(
            bytes,
            bounded_wincode_config::<LIMIT>(),
        )
        .map_err(anyhow::Error::from);
    }
    let current = wincode::config::deserialize_exact::<CompactMetaV1, _>(
        bytes,
        bounded_wincode_config::<LIMIT>(),
    );
    let legacy = wincode::config::deserialize_exact::<LegacyCompactMetaV1, _>(
        bytes,
        bounded_wincode_config::<LIMIT>(),
    )
    .map_err(anyhow::Error::from)
    .and_then(CompactMetaV1::try_from);
    let current_error = current.as_ref().err().map(ToString::to_string);
    let legacy_error = legacy.as_ref().err().map(ToString::to_string);
    match (current.ok(), legacy.ok()) {
        (Some(current), None) => Ok(current),
        (None, Some(legacy)) => Ok(legacy),
        (Some(current), Some(legacy)) => {
            let current_canonical = wincode::config::serialize(&current, wincode_leb128_config())?;
            let legacy_canonical = wincode::config::serialize(&legacy, wincode_leb128_config())?;
            ensure!(
                current_canonical == legacy_canonical,
                "ambiguous compact metadata decodes as different current and legacy values"
            );
            Ok(current)
        }
        (None, None) => bail!(
            "compact metadata is neither current nor legacy: current={}; legacy={}",
            current_error.as_deref().unwrap_or("unknown error"),
            legacy_error.as_deref().unwrap_or("unknown error")
        ),
    }
}

fn scan_metadata_excluded_pubkeys(
    bytes: &[u8],
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    // Option::None has the same one-byte wire form in both metadata generations. This is the
    // common successful-transaction path, so it can select the current tail schema without an
    // ambiguity fallback. A present error is the only current/legacy wire difference.
    if bytes.first() == Some(&0) {
        let mut selected: SelectiveCurrentMetaV1 = wincode::config::deserialize_exact(
            bytes,
            bounded_wincode_config::<PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES>(),
        )?;
        if let Some(logs) = &mut selected.logs {
            visit_log_events(&mut logs.events, visit)?;
        }
        return Ok(());
    }

    let current = wincode::config::deserialize_exact::<SelectiveCurrentMetaV1, _>(
        bytes,
        bounded_wincode_config::<PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES>(),
    );
    let legacy = wincode::config::deserialize_exact::<SelectiveLegacyMetaV1, _>(
        bytes,
        bounded_wincode_config::<PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES>(),
    )
    .map_err(anyhow::Error::from)
    .and_then(validate_selective_legacy_metadata);
    let current_error = current.as_ref().err().map(ToString::to_string);
    let legacy_error = legacy.as_ref().err().map(ToString::to_string);
    match (current.ok(), legacy.ok()) {
        (Some(mut current), None) => {
            if let Some(logs) = &mut current.logs {
                visit_log_events(&mut logs.events, visit)?;
            }
            Ok(())
        }
        (None, Some(mut legacy)) => {
            if let Some(logs) = &mut legacy.logs {
                visit_log_events(&mut logs.events, visit)?;
            }
            Ok(())
        }
        (Some(mut current), Some(mut legacy)) => {
            // Pass 1 needs only the excluded sequence. Require both wire interpretations to produce
            // that exact sequence and fail closed if they differ. Pass 2 still performs the full
            // canonical current/legacy ambiguity check before publication.
            let current_keys = collect_selective_log_pubkeys(&mut current.logs)?;
            let legacy_keys = collect_selective_log_pubkeys(&mut legacy.logs)?;
            ensure!(
                current_keys == legacy_keys,
                "ambiguous compact metadata decodes as different excluded pubkey sequences"
            );
            for mut key in current_keys {
                visit(&mut key, ReferenceClass::Excluded)?;
            }
            Ok(())
        }
        (None, None) => bail!(
            "compact metadata is neither current nor legacy: current={}; legacy={}",
            current_error.as_deref().unwrap_or("unknown error"),
            legacy_error.as_deref().unwrap_or("unknown error")
        ),
    }
}

fn validate_selective_legacy_metadata(
    metadata: SelectiveLegacyMetaV1,
) -> Result<SelectiveLegacyMetaV1> {
    if let Some(err) = metadata.err.as_deref() {
        // Legacy metadata stores the old fixed-width transaction-error bytes in a length-delimited
        // field. This conversion is part of the legacy schema: a structurally valid byte sequence
        // is not a valid legacy interpretation unless its stored error also decodes. Applying the
        // gate here keeps the selective pass consistent with decode_compact_metadata_with_limit.
        CompactTransactionError::from_stored_wincode_bytes(err)
            .context("decode legacy compact transaction error")?;
    }
    Ok(metadata)
}

fn collect_selective_log_pubkeys(
    logs: &mut Option<SelectiveLogStream>,
) -> Result<Vec<CompactPubkey>> {
    let mut keys = Vec::new();
    if let Some(logs) = logs {
        visit_log_events(&mut logs.events, &mut |key, class| {
            ensure!(class == ReferenceClass::Excluded);
            ensure!(
                keys.len() < MAX_TYPED_PUBKEY_REFERENCES_PER_BLOCK,
                "selective metadata pubkey references exceed per-block limit"
            );
            keys.push(*key);
            Ok(())
        })?;
    }
    Ok(keys)
}

#[cfg(test)]
fn visit_borrowed_message_pubkeys(
    message: &mut BorrowedHotMessagePayload<'_>,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    match message {
        BorrowedHotMessagePayload::Legacy(message) => {
            for key in &mut message.account_keys {
                visit(key, ReferenceClass::Eligible)?;
            }
        }
        BorrowedHotMessagePayload::V0(message) => {
            for key in &mut message.account_keys {
                visit(key, ReferenceClass::Eligible)?;
            }
            for lookup in &mut message.address_table_lookups {
                visit(&mut lookup.account_key, ReferenceClass::Eligible)?;
            }
        }
    }
    Ok(())
}

fn visit_borrowed_metadata_pubkeys(
    metadata: &mut BorrowedCompactMetaV1<'_>,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    for key in metadata
        .loaded_writable_addresses
        .iter_mut()
        .chain(metadata.loaded_readonly_addresses.iter_mut())
    {
        visit(key, ReferenceClass::Eligible)?;
    }
    for balance in metadata
        .pre_token_balances
        .iter_mut()
        .chain(metadata.post_token_balances.iter_mut())
    {
        if let Some(key) = &mut balance.mint {
            visit(key, ReferenceClass::Eligible)?;
        }
        if let Some(key) = &mut balance.owner {
            visit(key, ReferenceClass::Eligible)?;
        }
        if let Some(key) = &mut balance.program_id {
            visit(key, ReferenceClass::Eligible)?;
        }
    }
    for reward in &mut metadata.rewards {
        visit(&mut reward.pubkey, ReferenceClass::Eligible)?;
    }
    if let Some(return_data) = &mut metadata.return_data {
        visit(&mut return_data.program_id, ReferenceClass::Eligible)?;
    }
    if let Some(logs) = &mut metadata.logs {
        visit_log_events(&mut logs.events, visit)?;
    }
    Ok(())
}

fn visit_metadata_pubkeys(
    metadata: &mut CompactMetaV1,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    for key in metadata
        .loaded_writable_addresses
        .iter_mut()
        .chain(metadata.loaded_readonly_addresses.iter_mut())
    {
        visit(key, ReferenceClass::Eligible)?;
    }
    for balance in metadata
        .pre_token_balances
        .iter_mut()
        .chain(metadata.post_token_balances.iter_mut())
    {
        if let Some(key) = &mut balance.mint {
            visit(key, ReferenceClass::Eligible)?;
        }
        if let Some(key) = &mut balance.owner {
            visit(key, ReferenceClass::Eligible)?;
        }
        if let Some(key) = &mut balance.program_id {
            visit(key, ReferenceClass::Eligible)?;
        }
    }
    for reward in &mut metadata.rewards {
        visit(&mut reward.pubkey, ReferenceClass::Eligible)?;
    }
    if let Some(return_data) = &mut metadata.return_data {
        visit(&mut return_data.program_id, ReferenceClass::Eligible)?;
    }
    if let Some(logs) = &mut metadata.logs {
        visit_log_pubkeys(logs, visit)?;
    }
    Ok(())
}

fn visit_log_pubkeys(
    logs: &mut CompactLogStream,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    visit_log_events(&mut logs.events, visit)
}

fn visit_log_events(
    events: &mut [LogEvent],
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    for event in events {
        match event {
            LogEvent::LoaderUpgradedProgram { program }
            | LogEvent::Invoke { program, .. }
            | LogEvent::BpfInvoke { program }
            | LogEvent::Consumed { program, .. }
            | LogEvent::Success { program }
            | LogEvent::BpfSuccess { program }
            | LogEvent::Failure { program, .. }
            | LogEvent::BpfFailure { program, .. }
            | LogEvent::FailureCustomProgramError { program, .. }
            | LogEvent::BpfFailureCustomProgramError { program, .. }
            | LogEvent::FailureInvalidAccountData { program }
            | LogEvent::BpfFailureInvalidAccountData { program }
            | LogEvent::FailureInvalidProgramArgument { program }
            | LogEvent::BpfFailureInvalidProgramArgument { program }
            | LogEvent::Return { program, .. } => visit(program, ReferenceClass::Excluded)?,
            LogEvent::ProgramIdLog { program, log } => {
                visit(program, ReferenceClass::Excluded)?;
                visit_program_log_pubkeys(log, visit)?;
            }
            LogEvent::LoaderFinalizedAccount { account }
            | LogEvent::RuntimeWritablePrivilegeEscalated { account }
            | LogEvent::RuntimeSignerPrivilegeEscalated { account }
            | LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account } => {
                visit(account, ReferenceClass::Excluded)?;
            }
            LogEvent::ProgramNotDeployed { program } | LogEvent::ProgramNotCached { program } => {
                if let Some(program) = program {
                    visit(program, ReferenceClass::Excluded)?;
                }
            }
            LogEvent::System(log) => visit_system_log_pubkeys(log, visit)?,
            LogEvent::ProgramLog(log) | LogEvent::ProgramPlainLog(log) => {
                visit_program_log_pubkeys(log, visit)?;
            }
            LogEvent::ProgramLogError { .. }
            | LogEvent::ProgramAccountNotWritable
            | LogEvent::ProgramIdMismatch
            | LogEvent::ProgramNotUpgradeable
            | LogEvent::ProgramAndProgramDataAccountMismatch
            | LogEvent::ProgramWasExtendedInThisBlockAlready
            | LogEvent::BpfConsumed { .. }
            | LogEvent::FailedToComplete { .. }
            | LogEvent::CustomProgramError { .. }
            | LogEvent::Data { .. }
            | LogEvent::Consumption { .. }
            | LogEvent::CbRequestUnits { .. }
            | LogEvent::UnknownProgram { .. }
            | LogEvent::UnknownAccount { .. }
            | LogEvent::VerifyEd25519
            | LogEvent::VerifySecp256k1
            | LogEvent::LogTruncated
            | LogEvent::StakeMergingAccounts
            | LogEvent::CloseContextState
            | LogEvent::Plain { .. }
            | LogEvent::Unparsed { .. } => {}
        }
    }
    Ok(())
}

fn visit_program_log_pubkeys(
    log: &mut ProgramLog,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    if let ProgramLog::Token2022(
        Token2022Log::ErrorHarvestingFrom { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom2 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom3 { account_key, .. }
        | Token2022Log::ErrorHarvestingFrom4 { account_key, .. },
    ) = log
    {
        visit(account_key, ReferenceClass::Excluded)?;
    }
    Ok(())
}

fn visit_system_log_pubkeys(
    log: &mut SystemProgramLog,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    match log {
        SystemProgramLog::CreateAddressMismatch {
            provided_addr,
            derived_addr,
        }
        | SystemProgramLog::TransferFromAddressMismatch {
            provided_addr,
            derived_addr,
        } => {
            visit(provided_addr, ReferenceClass::Excluded)?;
            visit_pubkey_or_string(derived_addr, visit)?;
        }
        SystemProgramLog::CreateAccountAlreadyInUse { addr }
        | SystemProgramLog::AllocateAlreadyInUse { addr }
        | SystemProgramLog::AllocateToMustSign { addr }
        | SystemProgramLog::AllocateAccountAlreadyInUse { addr }
        | SystemProgramLog::AssignAccountMustSign { addr }
        | SystemProgramLog::CreateAccountAccountAlreadyInUse { addr } => {
            visit_system_address(addr, visit)?;
        }
        SystemProgramLog::TransferFromMustSign { from } => {
            visit(from, ReferenceClass::Excluded)?;
        }
        SystemProgramLog::NonceAccountMustBeWriteable { account, .. }
        | SystemProgramLog::NonceAccountMustBeSigner { account, .. }
        | SystemProgramLog::NonceAccountMustSign { account, .. }
        | SystemProgramLog::NonceAccountStateInvalid { account, .. } => {
            visit_pubkey_or_string(account, visit)?;
        }
        SystemProgramLog::Instruction(_)
        | SystemProgramLog::AllocateRequestedTooLarge { .. }
        | SystemProgramLog::CreateAccountDataSizeLimitedInInnerInstructions { .. }
        | SystemProgramLog::TransferFromMustNotCarryData
        | SystemProgramLog::TransferInsufficient { .. }
        | SystemProgramLog::AdvanceNonceRecentBlockhashesEmpty
        | SystemProgramLog::InitializeNonceRecentBlockhashesEmpty
        | SystemProgramLog::AuthorizeNonceAccount { .. }
        | SystemProgramLog::NonceInsufficientLamports { .. }
        | SystemProgramLog::NonceCanOnlyAdvanceOncePerSlot { .. } => {}
    }
    Ok(())
}

fn visit_system_address(
    address: &mut SystemAddress,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    match address {
        SystemAddress::Pubkey(value) => visit_pubkey_or_string(value, visit),
        SystemAddress::Debug { address, base } => {
            visit_pubkey_or_string(address, visit)?;
            if let Some(base) = base {
                visit_pubkey_or_string(base, visit)?;
            }
            Ok(())
        }
    }
}

fn visit_pubkey_or_string(
    value: &mut PubkeyOrString,
    visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
) -> Result<()> {
    if let PubkeyOrString::Pubkey(key) = value {
        visit(key, ReferenceClass::Excluded)?;
    }
    Ok(())
}

fn compute_budget_key() -> [u8; 32] {
    solana_pubkey::pubkey!("ComputeBudget111111111111111111111111111111").to_bytes()
}

fn build_usage_sorted_registry(
    source: &MappedRegistry,
    counts: &[u32],
    target: &Path,
    sort_memory_mib: usize,
    pool: &rayon::ThreadPool,
) -> Result<(Vec<u32>, u64)> {
    ensure!(
        source.len == counts.len(),
        "registry/count vector length mismatch"
    );
    let scratch_bytes = sort_memory_mib
        .checked_mul(1 << 20)
        .context("sort memory byte count overflow")?;
    let chunk_records = (scratch_bytes / std::mem::size_of::<SortRecord>())
        .max(1)
        .min(source.len.saturating_add(1));
    let run_dir = target.join(".registry-reprocess-sort-runs");
    fs::create_dir(&run_dir).with_context(|| format!("create {}", run_dir.display()))?;
    let mut chunk = Vec::<SortRecord>::new();
    chunk
        .try_reserve_exact(chunk_records)
        .context("allocate bounded registry sort chunk")?;
    let mut runs = Vec::new();
    let builtin = compute_budget_key();
    let mut builtin_old_id = None;
    for (index, (key, &count)) in source.keys().iter().zip(counts).enumerate() {
        let old_id = u32::try_from(index + 1).context("source registry ID exceeds u32")?;
        if *key == builtin {
            ensure!(
                builtin_old_id.replace(old_id).is_none(),
                "duplicate ComputeBudget registry key"
            );
        }
        if count == 0 {
            continue;
        }
        chunk.push(SortRecord {
            count,
            key: *key,
            old_id,
        });
        if chunk.len() == chunk_records {
            spill_sort_run(&run_dir, &mut runs, &mut chunk, pool)?;
        }
    }
    let builtin_is_synthetic_prefix = counts
        .get(
            builtin_old_id
                .map(|id| id as usize - 1)
                .unwrap_or(usize::MAX),
        )
        .copied()
        .unwrap_or(0)
        == 0;
    if !chunk.is_empty() {
        spill_sort_run(&run_dir, &mut runs, &mut chunk, pool)?;
    }

    let registry_path = target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE);
    let counts_path = target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE);
    let mut registry_writer = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        File::create(&registry_path)
            .with_context(|| format!("create {}", registry_path.display()))?,
    );
    let mut counts_writer = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        File::create(&counts_path).with_context(|| format!("create {}", counts_path.display()))?,
    );
    let mut cursors = runs
        .iter()
        .map(|path| SortRunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (run, cursor) in cursors.iter_mut().enumerate() {
        if let Some(record) = cursor.next()? {
            heap.push(HeapRecord { record, run });
        }
    }
    let mut old_to_new = vec![0u32; source.len];
    let mut previous = None::<SortRecord>;
    let mut target_keys = 0u64;
    let mut emitted_count_sum = 0u64;
    if builtin_is_synthetic_prefix {
        target_keys = 1;
        registry_writer.write_all(&builtin)?;
        write_u32_varint(&mut counts_writer, 0)?;
        if let Some(old_id) = builtin_old_id {
            old_to_new[(old_id - 1) as usize] = 1;
        }
    }
    while let Some(item) = heap.pop() {
        if let Some(previous) = previous {
            ensure!(
                previous.cmp_canonical(&item.record) != Ordering::Greater,
                "external registry merge violated canonical ordering"
            );
            ensure!(
                previous.key != item.record.key,
                "duplicate key in source registry"
            );
        }
        target_keys = target_keys
            .checked_add(1)
            .context("target registry key count overflow")?;
        let new_id = u32::try_from(target_keys).context("target registry exceeds u32 ID space")?;
        registry_writer.write_all(&item.record.key)?;
        write_u32_varint(&mut counts_writer, item.record.count)?;
        emitted_count_sum = emitted_count_sum
            .checked_add(u64::from(item.record.count))
            .context("target registry count sum overflow")?;
        if item.record.old_id != 0 {
            let slot = old_to_new
                .get_mut((item.record.old_id - 1) as usize)
                .context("sort record old ID is outside remap")?;
            ensure!(
                *slot == 0,
                "duplicate old ID {} in sort output",
                item.record.old_id
            );
            *slot = new_id;
        }
        previous = Some(item.record);
        let cursor = cursors
            .get_mut(item.run)
            .context("sort heap references missing run")?;
        if let Some(record) = cursor.next()? {
            heap.push(HeapRecord {
                record,
                run: item.run,
            });
        }
    }
    for cursor in &cursors {
        ensure!(
            cursor.remaining == 0,
            "sort run was not consumed completely"
        );
    }
    let expected_count_sum = counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("eligible count sum overflow")
    })?;
    ensure!(
        emitted_count_sum == expected_count_sum,
        "target registry count sum mismatch"
    );
    registry_writer
        .flush()
        .with_context(|| format!("flush {}", registry_path.display()))?;
    counts_writer
        .flush()
        .with_context(|| format!("flush {}", counts_path.display()))?;
    drop(registry_writer);
    drop(counts_writer);
    drop(cursors);
    for path in runs {
        fs::remove_file(&path).with_context(|| format!("remove sort run {}", path.display()))?;
    }
    fs::remove_dir(&run_dir).with_context(|| format!("remove {}", run_dir.display()))?;
    Ok((old_to_new, target_keys))
}

fn spill_sort_run(
    directory: &Path,
    runs: &mut Vec<PathBuf>,
    records: &mut Vec<SortRecord>,
    pool: &rayon::ThreadPool,
) -> Result<()> {
    pool.install(|| records.par_sort_unstable_by(SortRecord::cmp_canonical));
    let path = directory.join(format!("run-{:05}.bin", runs.len()));
    let mut writer = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        File::create(&path).with_context(|| format!("create {}", path.display()))?,
    );
    writer.write_all(SORT_RUN_MAGIC)?;
    writer.write_all(&(records.len() as u64).to_le_bytes())?;
    for record in records.drain(..) {
        writer.write_all(&record.count.to_le_bytes())?;
        writer.write_all(&record.key)?;
        writer.write_all(&record.old_id.to_le_bytes())?;
    }
    writer
        .flush()
        .with_context(|| format!("flush {}", path.display()))?;
    runs.push(path);
    Ok(())
}

impl SortRunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let bytes = file.metadata()?.len();
        // A merge can have many runs under a small sort budget. Keep per-run buffering bounded;
        // all records are fixed-width and each cursor is read sequentially.
        let mut reader = BufReader::with_capacity(64 << 10, file);
        let mut header = [0u8; 16];
        reader.read_exact(&mut header)?;
        ensure!(
            &header[..8] == SORT_RUN_MAGIC,
            "invalid registry sort run magic"
        );
        let remaining = u64::from_le_bytes(header[8..16].try_into().unwrap());
        let expected = 16u64
            .checked_add(
                remaining
                    .checked_mul(SORT_RECORD_BYTES as u64)
                    .context("sort run size overflow")?,
            )
            .context("sort run size overflow")?;
        ensure!(bytes == expected, "registry sort run length mismatch");
        Ok(Self { reader, remaining })
    }

    fn next(&mut self) -> Result<Option<SortRecord>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0u8; SORT_RECORD_BYTES];
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= 1;
        Ok(Some(SortRecord {
            count: u32::from_le_bytes(bytes[..4].try_into().unwrap()),
            key: bytes[4..36].try_into().unwrap(),
            old_id: u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
        }))
    }
}

fn build_registry_index(target: &Path) -> Result<()> {
    let registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let index = KeyIndex::build_from_slice_low_memory(registry.keys());
    let path = target.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    index.write(&path)?;
    ensure!(
        index.len() == registry.len,
        "built registry MPHF length mismatch"
    );
    Ok(())
}

fn validate_source_registry_index(source: &Path, registry: &MappedRegistry) -> Result<KeyIndex> {
    let path = source.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE);
    let index = KeyIndex::load(&path)
        .with_context(|| format!("load strict source registry index {}", path.display()))?;
    ensure!(
        index.len() == registry.len,
        "source registry MPHF has {} keys, registry.bin has {}",
        index.len(),
        registry.len
    );
    for (offset, key) in registry.keys().iter().enumerate() {
        let expected = u32::try_from(offset + 1).context("source registry ID exceeds u32")?;
        ensure!(
            index.lookup(key) == Some(expected),
            "source registry contains a duplicate key or MPHF mismatch at ID {expected}"
        );
    }
    Ok(index)
}

fn validate_canonical_registry(target: &Path, expected_keys: u64) -> Result<()> {
    let registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    ensure!(
        registry.len as u64 == expected_keys,
        "target registry key count mismatch"
    );
    let counts = read_registry_counts(
        &target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        registry.len,
    )?;
    let synthetic_builtin_prefix =
        registry.keys().first() == Some(&compute_budget_key()) && counts.first() == Some(&0);
    let ordered_start = if synthetic_builtin_prefix { 2 } else { 1 };
    for index in ordered_start..registry.len {
        let previous = SortRecord {
            count: counts[index - 1],
            key: registry.keys()[index - 1],
            old_id: 0,
        };
        let current = SortRecord {
            count: counts[index],
            key: registry.keys()[index],
            old_id: 0,
        };
        ensure!(
            previous.cmp_canonical(&current) != Ordering::Greater,
            "target registry is not canonical at IDs {} and {}",
            index,
            index + 1
        );
    }
    let index = KeyIndex::load(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))?;
    ensure!(
        index.len() == registry.len,
        "target registry MPHF key count mismatch"
    );
    for (offset, key) in registry.keys().iter().enumerate() {
        ensure!(
            index.lookup(key) == Some((offset + 1) as u32),
            "target registry MPHF mismatch at ID {}",
            offset + 1
        );
    }
    ensure!(
        index.lookup(&compute_budget_key()).is_some(),
        "target registry omits ComputeBudget"
    );
    Ok(())
}

impl From<LegacyBlockAccessBlobV1> for ArchiveV2BlockAccessBlob {
    fn from(value: LegacyBlockAccessBlobV1) -> Self {
        Self {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: 0,
            blockhash: value.blockhash,
            previous_blockhash: value.previous_blockhash,
            signature_counts: value.signature_counts,
            signatures: value.signatures,
            pubkeys: value.pubkeys,
            blockhashes: value.blockhashes,
            vote_hashes: Vec::new(),
        }
    }
}

impl From<LegacyBlockAccessBlobV2NoVotes> for ArchiveV2BlockAccessBlob {
    fn from(value: LegacyBlockAccessBlobV2NoVotes) -> Self {
        Self {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: value.flags,
            blockhash: value.blockhash,
            previous_blockhash: value.previous_blockhash,
            signature_counts: value.signature_counts,
            signatures: value.signatures,
            pubkeys: value.pubkeys,
            blockhashes: value.blockhashes,
            vote_hashes: Vec::new(),
        }
    }
}

fn decode_access_blob(bytes: &[u8], block_id: u32) -> Result<ArchiveV2BlockAccessBlob> {
    let current_error = match wincode::config::deserialize_exact(
        bytes,
        bounded_wincode_config::<MAX_ACCESS_FRAME_BYTES_USIZE>(),
    ) {
        Ok(blob) => return Ok(blob),
        Err(error) => error,
    };
    let no_votes = wincode::config::deserialize_exact::<LegacyBlockAccessBlobV2NoVotes, _>(
        bytes,
        bounded_wincode_config::<MAX_ACCESS_FRAME_BYTES_USIZE>(),
    )
    .map_err(anyhow::Error::from)
    .and_then(|blob| {
        ensure!(
            blob.version == WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            "decoded v2-no-votes payload with version {}",
            blob.version
        );
        Ok(blob)
    });
    let legacy_v1 = wincode::config::deserialize_exact::<LegacyBlockAccessBlobV1, _>(
        bytes,
        bounded_wincode_config::<MAX_ACCESS_FRAME_BYTES_USIZE>(),
    )
    .map_err(anyhow::Error::from)
    .and_then(|blob| {
        ensure!(
            blob.version == 1,
            "decoded legacy-v1 payload with version {}",
            blob.version
        );
        Ok(blob)
    });
    match (no_votes, legacy_v1) {
        (Ok(_), Ok(_)) => bail!("legacy block-access {block_id} has an ambiguous schema"),
        (Ok(blob), Err(_)) => Ok(blob.into()),
        (Err(_), Ok(blob)) => Ok(blob.into()),
        (Err(no_votes_error), Err(v1_error)) => bail!(
            "cannot decode block-access {block_id}: current={current_error}; v2-no-votes={no_votes_error:#}; v1={v1_error:#}"
        ),
    }
}

fn access_rows_equal(left: &ArchiveV2GetBlockIndexRow, right: &ArchiveV2GetBlockIndexRow) -> bool {
    left.block_offset == right.block_offset
        && left.block_len == right.block_len
        && left.access_offset == right.access_offset
        && left.access_len == right.access_len
}

fn validate_get_block_index_geometry(
    path: &Path,
    hot_rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    access_rows: &[ArchiveV2BlockAccessIndexRow],
) -> Result<()> {
    let actual = read_archive_v2_get_block_index(&path)?;
    let expected = build_get_block_rows(hot_rows, access_rows)?;
    ensure!(
        actual.rows.len() == expected.len(),
        "source get-block index has the wrong row count"
    );
    for (slot, (actual, expected)) in actual.rows.iter().zip(&expected).enumerate() {
        ensure!(
            access_rows_equal(actual, expected),
            "source get-block index mismatch at slot offset {slot}"
        );
    }
    Ok(())
}

fn ensure_strictly_sorted_access_ids(blob: &ArchiveV2BlockAccessBlob, block_id: u32) -> Result<()> {
    let mut previous_pubkey = None;
    for entry in &blob.pubkeys {
        ensure!(
            entry.id != 0 && previous_pubkey.is_none_or(|previous| previous < entry.id),
            "block-access pubkey IDs are not strictly increasing at block_id {block_id}"
        );
        previous_pubkey = Some(entry.id);
    }
    let mut previous_blockhash = None;
    for entry in &blob.blockhashes {
        ensure!(
            previous_blockhash.is_none_or(|previous| previous < entry.id),
            "block-access blockhash IDs are not strictly increasing at block_id {block_id}"
        );
        previous_blockhash = Some(entry.id);
    }
    let mut previous_vote = None;
    for entry in &blob.vote_hashes {
        ensure!(
            previous_vote.is_none_or(|previous| previous < entry.block_id),
            "block-access vote-hash IDs are not strictly increasing at block_id {block_id}"
        );
        previous_vote = Some(entry.block_id);
    }
    Ok(())
}

/// Correct only the exact epoch-301 legacy boundary defect proven in production.
///
/// The defect copied row 0's own blockhash into its `previous_blockhash`.  The trusted current
/// predecessor tail contains the correct epoch-300 terminal hash.  All values are pinned so a
/// different corrupt generation continues to fail closed.  Rows after row 0 still pass through
/// the ordinary strict stream check, which proves the row-1 link when row 1 is present.
fn validate_epoch_301_access_boundary_evidence(
    evidence: &Epoch301AccessBoundaryEvidence,
) -> Result<()> {
    for (digest, label) in [
        (
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_SHA256,
            "pinned epoch-301 predecessor-tail digest",
        ),
        (
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_MANIFEST_SHA256,
            "pinned epoch-301 manifest digest",
        ),
        (
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_SHA256,
            "pinned epoch-301 source-access digest",
        ),
        (
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_SHA256,
            "pinned epoch-301 access-index digest",
        ),
        (
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_0_SHA256,
            "pinned epoch-301 access row-0 digest",
        ),
        (
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_SHA256,
            "pinned epoch-301 access row-1 digest",
        ),
    ] {
        validate_hex_sha256(digest, label)?;
    }
    ensure!(
        evidence.tail_binding.bytes == EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_BYTES
            && evidence.tail_binding.sha256 == EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_SHA256
            && evidence.tail_rows == EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_ROWS
            && evidence.manifest_sha256 == EPOCH_301_ACCESS_BOUNDARY_REPAIR_MANIFEST_SHA256
            && evidence.source_blob_bytes == EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_BYTES
            && evidence.source_index_binding.bytes == EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_BYTES
            && evidence.source_index_binding.sha256
                == EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_SHA256
            && evidence.source_index_rows == EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_ROWS
            && evidence.source_index_blob_bytes
                == EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_BYTES
            && evidence.row_0_access_len == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_0_BYTES
            && evidence.row_0_frame_sha256 == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_0_SHA256
            && evidence.row_1_block_id == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCK_ID
            && evidence.row_1_slot == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_SLOT
            && evidence.row_1_previous_blockhash_hex
                == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX
            && evidence.row_1_blockhash_hex == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCKHASH_HEX
            && evidence.row_1_frame_sha256 == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_SHA256
            && evidence.first_hot_parent_slot == EPOCH_301_ACCESS_BOUNDARY_REPAIR_PREDECESSOR_SLOT,
        "epoch-301 access boundary evidence does not match the pinned production generation"
    );
    Ok(())
}

fn validate_epoch_301_source_access_blob_binding(binding: &FileBinding) -> Result<()> {
    ensure!(
        binding.bytes == EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_BYTES
            && binding.sha256 == EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_SHA256,
        "epoch-301 source access blob does not match the pinned full-file binding"
    );
    Ok(())
}

fn validate_and_repair_access_previous_blockhash(
    epoch: u64,
    position: usize,
    source_row: &ArchiveV2BlockAccessIndexRow,
    trusted_predecessor: &super::PreviousBlockhash,
    expected_previous_blockhash: [u8; 32],
    evidence: Option<&Epoch301AccessBoundaryEvidence>,
    blob: &mut ArchiveV2BlockAccessBlob,
) -> Result<Option<AccessBoundaryRepair>> {
    if blob.previous_blockhash == expected_previous_blockhash {
        return Ok(None);
    }

    ensure!(
        epoch == EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH
            && position == EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID as usize
            && source_row.block_id == EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID
            && source_row.slot == EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT
            && trusted_predecessor.slot == EPOCH_301_ACCESS_BOUNDARY_REPAIR_PREDECESSOR_SLOT
            && blob.previous_blockhash == blob.blockhash
            && hex_digest(blob.blockhash) == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX
            && hex_digest(blob.previous_blockhash) == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX
            && hex_digest(trusted_predecessor.hash)
                == EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX
            && expected_previous_blockhash == trusted_predecessor.hash
            && blob.previous_blockhash != expected_previous_blockhash,
        "block-access previous blockhash breaks the trusted stream at block_id {}",
        source_row.block_id
    );
    validate_epoch_301_access_boundary_evidence(
        evidence.context("epoch-301 access boundary repair has no production evidence")?,
    )?;

    let repair = AccessBoundaryRepair {
        mode: EPOCH_301_ACCESS_BOUNDARY_REPAIR_MODE.to_owned(),
        block_id: source_row.block_id,
        block_slot: source_row.slot,
        trusted_predecessor_slot: trusted_predecessor.slot,
        original_previous_blockhash_hex: EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX.to_owned(),
        corrected_previous_blockhash_hex: EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX.to_owned(),
    };
    blob.previous_blockhash = expected_previous_blockhash;
    Ok(Some(repair))
}

#[allow(clippy::too_many_arguments)]
fn collect_epoch_301_access_boundary_evidence(
    source: &Path,
    source_file: &mut File,
    source_metadata: &fs::Metadata,
    source_index_binding: &FileBinding,
    source_index: &blockzilla_format::ArchiveV2BlockAccessIndex,
    source_hot_rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    access_context: &AccessBuildContext,
    row_0_bytes: &[u8],
    row_0_blob: &ArchiveV2BlockAccessBlob,
) -> Result<Epoch301AccessBoundaryEvidence> {
    let row_1 = source_index
        .rows
        .get(EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCK_ID as usize)
        .context("epoch-301 repair source has no row 1")?;
    let row_1_hot = source_hot_rows
        .get(EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCK_ID as usize)
        .context("epoch-301 repair hot index has no row 1")?;
    ensure!(
        row_1.block_id == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCK_ID
            && row_1.block_id == row_1_hot.block_id
            && row_1.slot == row_1_hot.slot
            && row_1.tx_count == row_1_hot.tx_count
            && row_1.signature_count == row_1_hot.signature_count,
        "epoch-301 repair row-1 access/hot geometry mismatch"
    );
    source_file.seek(SeekFrom::Start(row_1.access_offset))?;
    let mut row_1_bytes = vec![0u8; row_1.access_len as usize];
    source_file.read_exact(&mut row_1_bytes)?;
    let row_1_blob = decode_access_blob(&row_1_bytes, row_1.block_id)?;
    ensure!(
        row_1_blob.version == WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION && row_1_blob.flags == 0,
        "unsupported epoch-301 source block-access row 1"
    );
    ensure_strictly_sorted_access_ids(&row_1_blob, row_1.block_id)?;
    ensure!(
        row_1_blob.previous_blockhash == row_0_blob.blockhash,
        "epoch-301 source row 1 does not continue from row 0"
    );
    ensure!(
        access_context
            .blockhashes
            .get(row_1.block_id as usize)
            .copied()
            == Some(row_1_blob.blockhash),
        "epoch-301 source row-1 blockhash disagrees with the registry"
    );

    let first_hot = *source_hot_rows
        .first()
        .context("epoch-301 repair source has no hot row 0")?;
    let mut blocks_file = File::open(source.join(ARCHIVE_V2_BLOCKS_FILE))?;
    let first_block = decode_hot_block(&read_compressed_block(&mut blocks_file, first_hot, None)?)?;
    ensure!(
        first_block.header.slot == EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT,
        "epoch-301 source hot row 0 has the wrong slot"
    );

    Ok(Epoch301AccessBoundaryEvidence {
        tail_binding: hash_file(&source.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE))?,
        tail_rows: access_context.previous_tail.len(),
        manifest_sha256: hash_file(&source.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE))?
            .sha256,
        source_blob_bytes: source_metadata.len(),
        source_index_binding: source_index_binding.clone(),
        source_index_rows: source_index.rows.len(),
        source_index_blob_bytes: source_index.blob_file_bytes,
        row_0_access_len: u32::try_from(row_0_bytes.len())?,
        row_0_frame_sha256: hex_digest(Sha256::digest(row_0_bytes)),
        row_1_block_id: row_1.block_id,
        row_1_slot: row_1.slot,
        row_1_previous_blockhash_hex: hex_digest(row_1_blob.previous_blockhash),
        row_1_blockhash_hex: hex_digest(row_1_blob.blockhash),
        row_1_frame_sha256: hex_digest(Sha256::digest(&row_1_bytes)),
        first_hot_parent_slot: first_block.header.parent_slot,
    })
}

fn publish_access_temp(staging: &Path, name: &str) -> Result<()> {
    let temp = staging.join(format!("{name}{ACCESS_TEMP_SUFFIX}"));
    let final_path = staging.join(name);
    File::open(&temp)
        .with_context(|| format!("open access temp {}", temp.display()))?
        .sync_all()
        .with_context(|| format!("sync access temp {}", temp.display()))?;
    publish_directory_no_replace(&temp, &final_path)
}

#[allow(clippy::too_many_arguments)]
fn remap_source_access(
    source: &Path,
    staging: &Path,
    epoch: u64,
    source_registry: &MappedRegistry,
    remap: &MappedRegistryRemap,
    source_hot_rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    target_hot_rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    target_registry: &MappedRegistry,
    access_context: &AccessBuildContext,
) -> Result<AccessRemapBindings> {
    let started = Instant::now();
    let source_blob_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let source_index_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    for path in [&source_blob_path, &source_index_path] {
        ensure!(
            fs::symlink_metadata(path)
                .with_context(|| format!("inspect required source access {}", path.display()))?
                .file_type()
                .is_file(),
            "source access input is not a regular file: {}",
            path.display()
        );
    }
    preflight_access_index(&source_index_path, source_hot_rows.len())?;
    let source_index_binding = hash_file(&source_index_path)?;
    let source_index = read_archive_v2_block_access_index(&source_index_path)?;
    ensure!(
        source_index.flags == 0 && source_index.rows.len() == source_hot_rows.len(),
        "source block-access index has unsupported flags or row count"
    );
    ensure!(
        hash_file(&source_index_path)? == source_index_binding,
        "source block-access index changed while loading"
    );
    let (mut source_file, source_metadata) = open_regular_read(&source_blob_path)?;
    ensure!(
        source_metadata.len() == source_index.blob_file_bytes,
        "source block-access blob length does not match its index"
    );

    let target_blob_temp = staging.join(format!(
        "{ARCHIVE_V2_BLOCK_ACCESS_FILE}{ACCESS_TEMP_SUFFIX}"
    ));
    let mut target_file = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target_blob_temp)?,
    );
    let mut target_rows = Vec::with_capacity(source_index.rows.len());
    let mut source_offset = 0u64;
    let mut target_offset = 0u64;
    let mut source_hasher = Sha256::new();
    let mut target_hasher = Sha256::new();
    let mut signature_hasher = Sha256::new();
    let mut signature_bytes = 0u64;
    let mut progress = ProgressTracker::new("registry access remap");
    progress.set_estimated_total_blocks(source_index.rows.len() as u64);
    let trusted_predecessor = access_context
        .previous_tail
        .last()
        .context("source access validation needs a previous blockhash tail")?;
    let mut previous_blockhash = trusted_predecessor.hash;
    let mut boundary_repair = None;
    for (position, ((source_row, source_hot), target_hot)) in source_index
        .rows
        .iter()
        .zip(source_hot_rows)
        .zip(target_hot_rows)
        .enumerate()
    {
        ensure!(
            source_row.block_id as usize == position
                && source_row.block_id == source_hot.block_id
                && source_row.slot == source_hot.slot
                && source_row.tx_count == source_hot.tx_count
                && source_row.signature_count == source_hot.signature_count
                && source_row.block_id == target_hot.block_id
                && source_row.slot == target_hot.slot
                && source_row.tx_count == target_hot.tx_count
                && source_row.signature_count == target_hot.signature_count,
            "block-access/hot-index geometry mismatch at block_id {}",
            source_row.block_id
        );
        ensure!(
            source_row.access_len > 0 && source_row.access_offset == source_offset,
            "source block-access offsets are not contiguous at block_id {}",
            source_row.block_id
        );
        source_offset = source_offset
            .checked_add(u64::from(source_row.access_len))
            .context("source block-access offset overflow")?;
        ensure!(
            source_offset <= source_index.blob_file_bytes,
            "source block-access row extends beyond the blob"
        );
        source_file.seek(SeekFrom::Start(source_row.access_offset))?;
        let mut bytes = vec![0u8; source_row.access_len as usize];
        source_file.read_exact(&mut bytes)?;
        source_hasher.update(&bytes);
        let mut blob = decode_access_blob(&bytes, source_row.block_id)?;
        ensure!(
            blob.version == WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION && blob.flags == 0,
            "unsupported source block-access header at block_id {}",
            source_row.block_id
        );
        ensure_strictly_sorted_access_ids(&blob, source_row.block_id)?;
        ensure!(
            access_context
                .blockhashes
                .get(source_row.block_id as usize)
                .copied()
                == Some(blob.blockhash),
            "block-access blockhash disagrees with registry at block_id {}",
            source_row.block_id
        );
        let repair_evidence = if blob.previous_blockhash != previous_blockhash
            && epoch == EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH
            && position == EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID as usize
        {
            Some(collect_epoch_301_access_boundary_evidence(
                source,
                &mut source_file,
                &source_metadata,
                &source_index_binding,
                &source_index,
                source_hot_rows,
                access_context,
                &bytes,
                &blob,
            )?)
        } else {
            None
        };
        let row_repair = validate_and_repair_access_previous_blockhash(
            epoch,
            position,
            source_row,
            trusted_predecessor,
            previous_blockhash,
            repair_evidence.as_ref(),
            &mut blob,
        )?;
        if let Some(row_repair) = row_repair {
            ensure!(
                boundary_repair.replace(row_repair).is_none(),
                "more than one access boundary repair was requested"
            );
        }
        ensure!(blob.previous_blockhash == previous_blockhash);
        previous_blockhash = blob.blockhash;
        for entry in &blob.blockhashes {
            ensure!(
                super::resolve_access_blockhash_id(
                    entry.id,
                    &access_context.blockhashes,
                    &access_context.previous_tail,
                )? == entry.blockhash,
                "block-access blockhash entry {} is corrupt at block_id {}",
                entry.id,
                source_row.block_id
            );
        }
        for entry in &blob.vote_hashes {
            let expected = access_context
                .vote_hashes
                .get(entry.block_id as usize)
                .context("block-access vote hash ID is outside the vote registry")?;
            ensure!(
                entry.bank_hash == expected.bank_hash
                    && entry.block_id_hash == expected.block_id_hash,
                "block-access vote hash entry {} is corrupt at block_id {}",
                entry.block_id,
                source_row.block_id
            );
        }
        ensure!(
            blob.signature_counts.len() == source_row.tx_count as usize,
            "block-access signature-count rows do not match tx_count at block_id {}",
            source_row.block_id
        );
        let declared_signatures = blob.signature_counts.iter().try_fold(0u32, |sum, &count| {
            sum.checked_add(u32::from(count))
                .context("block-access signature count overflow")
        })?;
        ensure!(
            declared_signatures == source_row.signature_count,
            "block-access signature counts disagree with index at block_id {}",
            source_row.block_id
        );
        let expected_signature_bytes = u64::from(source_row.signature_count)
            .checked_mul(64)
            .context("block-access signature byte length overflow")?;
        ensure!(
            blob.signatures.len() as u64 == expected_signature_bytes,
            "block-access signature bytes disagree with index at block_id {}",
            source_row.block_id
        );
        signature_hasher.update(&blob.signatures);
        signature_bytes = signature_bytes
            .checked_add(expected_signature_bytes)
            .context("signature stream byte count overflow")?;

        let mut mapped = Vec::with_capacity(blob.pubkeys.len());
        for entry in &blob.pubkeys {
            ensure!(
                source_registry.key(entry.id)? == entry.pubkey,
                "source access pubkey bytes disagree with registry ID {}",
                entry.id
            );
            let new_id = remap.get(entry.id)?;
            if new_id == 0 {
                continue;
            }
            ensure!(
                target_registry.key(new_id)? == entry.pubkey,
                "target access pubkey bytes disagree with remapped ID {new_id}"
            );
            mapped.push(ArchiveV2BlockAccessPubkey {
                id: new_id,
                pubkey: entry.pubkey,
            });
        }
        mapped.sort_unstable_by_key(|entry| entry.id);
        ensure!(
            mapped.windows(2).all(|pair| pair[0].id < pair[1].id),
            "remapped access contains duplicate IDs at block_id {}",
            source_row.block_id
        );
        blob.pubkeys = mapped;
        let mut encoded = Vec::new();
        serialize_bounded_into(&mut encoded, &blob, MAX_ACCESS_FRAME_BYTES_USIZE)
            .with_context(|| format!("serialize remapped access {}", source_row.block_id))?;
        let access_len =
            u32::try_from(encoded.len()).context("remapped access frame exceeds u32")?;
        target_file.write_all(&encoded)?;
        target_hasher.update(&encoded);
        target_rows.push(ArchiveV2BlockAccessIndexRow {
            block_id: target_hot.block_id,
            slot: target_hot.slot,
            access_offset: target_offset,
            access_len,
            tx_count: target_hot.tx_count,
            signature_count: target_hot.signature_count,
        });
        target_offset = target_offset
            .checked_add(u64::from(access_len))
            .context("target block-access offset overflow")?;
        progress.update_slot(source_row.slot);
        progress.update_input_bytes(source_offset);
        progress.update(1, u64::from(source_row.tx_count));
    }
    ensure!(
        source_offset == source_index.blob_file_bytes,
        "source block-access index does not cover the complete blob"
    );
    target_file.flush()?;
    target_file.get_ref().sync_all()?;
    drop(target_file);
    ensure_open_file_unchanged(&source_blob_path, &source_file, &source_metadata)?;
    let source_blob_binding = FileBinding {
        bytes: source_offset,
        sha256: hex_digest(source_hasher.finalize()),
    };
    if boundary_repair.is_some() {
        // This binding came from the same complete sequential read used for source validation.
        // Check it before any access temp is published into the staging generation.
        validate_epoch_301_source_access_blob_binding(&source_blob_binding)?;
    }
    let target_blob_binding = FileBinding {
        bytes: target_offset,
        sha256: hex_digest(target_hasher.finalize()),
    };
    let signatures = FileBinding {
        bytes: signature_bytes,
        sha256: hex_digest(signature_hasher.finalize()),
    };

    let target_index_temp = staging.join(format!(
        "{ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE}{ACCESS_TEMP_SUFFIX}"
    ));
    write_archive_v2_block_access_index(&target_index_temp, target_offset, 0, &target_rows)?;
    let target_get_block_temp = staging.join(format!(
        "{ARCHIVE_V2_GET_BLOCK_INDEX_FILE}{ACCESS_TEMP_SUFFIX}"
    ));
    let target_get_block = build_get_block_rows(target_hot_rows, &target_rows)?;
    write_archive_v2_get_block_index(&target_get_block_temp, &target_get_block)?;
    publish_access_temp(staging, ARCHIVE_V2_BLOCK_ACCESS_FILE)?;
    publish_access_temp(staging, ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE)?;
    publish_access_temp(staging, ARCHIVE_V2_GET_BLOCK_INDEX_FILE)?;
    sync_directory(staging)?;
    let target_index_binding = hash_file(&staging.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE))?;
    let target_get_block_binding = hash_file(&staging.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE))?;
    progress.final_report();
    info!(
        elapsed_secs = started.elapsed().as_secs_f64(),
        rows = source_index.rows.len(),
        source_bytes = source_offset,
        target_bytes = target_offset,
        signature_bytes,
        "completed registry access remap"
    );
    if let Some(repair) = &boundary_repair {
        info!(
            mode = %repair.mode,
            block_id = repair.block_id,
            block_slot = repair.block_slot,
            original_previous_blockhash = %repair.original_previous_blockhash_hex,
            corrected_previous_blockhash = %repair.corrected_previous_blockhash_hex,
            "applied pinned legacy block-access boundary repair"
        );
    }
    Ok(AccessRemapBindings {
        source_blob: source_blob_binding,
        source_index: source_index_binding,
        target_blob: target_blob_binding,
        target_index: target_index_binding,
        target_get_block: target_get_block_binding,
        signatures,
        boundary_repair,
    })
}

fn access_non_pubkey_fields_except_previous_equal(
    source: &ArchiveV2BlockAccessBlob,
    target: &ArchiveV2BlockAccessBlob,
) -> bool {
    source.version == target.version
        && source.flags == target.flags
        && source.blockhash == target.blockhash
        && source.signature_counts == target.signature_counts
        && source.signatures == target.signatures
        && source.blockhashes.len() == target.blockhashes.len()
        && source
            .blockhashes
            .iter()
            .zip(&target.blockhashes)
            .all(|(source, target)| source.id == target.id && source.blockhash == target.blockhash)
        && source.vote_hashes.len() == target.vote_hashes.len()
        && source
            .vote_hashes
            .iter()
            .zip(&target.vote_hashes)
            .all(|(source, target)| {
                source.block_id == target.block_id
                    && source.bank_hash == target.bank_hash
                    && source.block_id_hash == target.block_id_hash
            })
}

fn access_previous_blockhashes_match_receipt(
    source_row: &ArchiveV2BlockAccessIndexRow,
    source: &ArchiveV2BlockAccessBlob,
    target: &ArchiveV2BlockAccessBlob,
    repair: Option<&AccessBoundaryRepair>,
) -> bool {
    if let Some(repair) = repair {
        source_row.block_id == repair.block_id
            && source_row.slot == repair.block_slot
            && source.previous_blockhash == source.blockhash
            && hex_digest(source.previous_blockhash) == repair.original_previous_blockhash_hex
            && hex_digest(target.previous_blockhash) == repair.corrected_previous_blockhash_hex
    } else {
        source.previous_blockhash == target.previous_blockhash
    }
}

fn advance_trusted_target_access_chain(
    expected_previous: [u8; 32],
    target: &ArchiveV2BlockAccessBlob,
    block_id: u32,
) -> Result<[u8; 32]> {
    ensure!(
        target.previous_blockhash == expected_previous,
        "target access breaks the trusted previous-blockhash stream at block_id {block_id}"
    );
    Ok(target.blockhash)
}

fn validate_v3_access_remap(
    source: &Path,
    target: &Path,
    receipt: &RegistryReprocessReceipt,
) -> Result<()> {
    let source_hot = read_archive_v2_hot_block_index(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    let target_hot = read_archive_v2_hot_block_index(&target.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    validate_hot_index_geometry_for_access(&source_hot.rows, &target_hot.rows)?;
    let source_index_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let target_index_path = target.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    preflight_access_index(&source_index_path, source_hot.rows.len())?;
    preflight_access_index(&target_index_path, target_hot.rows.len())?;
    let source_index = read_archive_v2_block_access_index(&source_index_path)?;
    let target_index = read_archive_v2_block_access_index(&target_index_path)?;
    ensure!(
        source_index.flags == 0
            && target_index.flags == 0
            && source_index.rows.len() == target_index.rows.len()
    );
    validate_get_block_index_geometry(
        &target.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
        &target_hot.rows,
        &target_index.rows,
    )?;
    let source_registry = MappedRegistry::open(&source.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let target_registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let target_lookup = KeyIndex::load(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE))?;
    let mut source_file = File::open(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE))?;
    let mut target_file = File::open(target.join(ARCHIVE_V2_BLOCK_ACCESS_FILE))?;
    let mut source_offset = 0u64;
    let mut target_offset = 0u64;
    let mut repair_observed = false;
    let previous_tail = load_previous_blockhash_tail_bounded(source, receipt.epoch)?;
    let trusted_predecessor = previous_tail
        .last()
        .context("v3 access validation needs a trusted predecessor tail")?;
    if let Some(repair) = &receipt.access_boundary_repair {
        ensure!(
            trusted_predecessor.slot == repair.trusted_predecessor_slot
                && hex_digest(trusted_predecessor.hash) == repair.corrected_previous_blockhash_hex,
            "repaired access receipt disagrees with the trusted predecessor tail"
        );
    }
    let mut target_previous_blockhash = trusted_predecessor.hash;
    for ((source_row, target_row), hot_row) in source_index
        .rows
        .iter()
        .zip(&target_index.rows)
        .zip(&source_hot.rows)
    {
        ensure!(
            source_row.block_id == target_row.block_id
                && source_row.slot == target_row.slot
                && source_row.tx_count == target_row.tx_count
                && source_row.signature_count == target_row.signature_count
                && source_row.block_id == hot_row.block_id
                && source_row.access_offset == source_offset
                && target_row.access_offset == target_offset,
            "v3 source/target access geometry mismatch at block_id {}",
            source_row.block_id
        );
        source_offset = source_offset
            .checked_add(u64::from(source_row.access_len))
            .context("deep source access offset overflow")?;
        target_offset = target_offset
            .checked_add(u64::from(target_row.access_len))
            .context("deep target access offset overflow")?;
        let mut source_bytes = vec![0u8; source_row.access_len as usize];
        let mut target_bytes = vec![0u8; target_row.access_len as usize];
        source_file.seek(SeekFrom::Start(source_row.access_offset))?;
        source_file.read_exact(&mut source_bytes)?;
        target_file.seek(SeekFrom::Start(target_row.access_offset))?;
        target_file.read_exact(&mut target_bytes)?;
        let source_blob = decode_access_blob(&source_bytes, source_row.block_id)?;
        let target_blob: ArchiveV2BlockAccessBlob = wincode::config::deserialize_exact(
            &target_bytes,
            bounded_wincode_config::<MAX_ACCESS_FRAME_BYTES_USIZE>(),
        )?;
        ensure_strictly_sorted_access_ids(&source_blob, source_row.block_id)?;
        ensure_strictly_sorted_access_ids(&target_blob, target_row.block_id)?;
        let row_repair = receipt
            .access_boundary_repair
            .as_ref()
            .filter(|repair| repair.block_id == source_row.block_id);
        let previous_blockhash_equal = if let Some(repair) = row_repair {
            ensure!(
                !repair_observed,
                "access boundary repair appears more than once"
            );
            repair_observed = true;
            access_previous_blockhashes_match_receipt(
                source_row,
                &source_blob,
                &target_blob,
                Some(repair),
            )
        } else {
            access_previous_blockhashes_match_receipt(source_row, &source_blob, &target_blob, None)
        };
        target_previous_blockhash = advance_trusted_target_access_chain(
            target_previous_blockhash,
            &target_blob,
            target_row.block_id,
        )?;
        ensure!(
            previous_blockhash_equal
                && access_non_pubkey_fields_except_previous_equal(&source_blob, &target_blob),
            "v3 access non-pubkey fields changed at block_id {}",
            source_row.block_id
        );
        let mut expected = Vec::with_capacity(source_blob.pubkeys.len());
        for entry in &source_blob.pubkeys {
            ensure!(source_registry.key(entry.id)? == entry.pubkey);
            if let Some(new_id) = target_lookup.lookup(&entry.pubkey) {
                ensure!(target_registry.key(new_id)? == entry.pubkey);
                expected.push(ArchiveV2BlockAccessPubkey {
                    id: new_id,
                    pubkey: entry.pubkey,
                });
            }
        }
        expected.sort_unstable_by_key(|entry| entry.id);
        ensure!(
            expected.len() == target_blob.pubkeys.len()
                && expected
                    .iter()
                    .zip(&target_blob.pubkeys)
                    .all(|(expected, actual)| {
                        expected.id == actual.id && expected.pubkey == actual.pubkey
                    }),
            "v3 access pubkey remap differs at block_id {}",
            source_row.block_id
        );
    }
    ensure!(
        source_offset == source_index.blob_file_bytes
            && target_offset == target_index.blob_file_bytes,
        "v3 access indices do not cover their blobs"
    );
    ensure!(
        receipt.access_boundary_repair.is_some() == repair_observed,
        "access boundary repair receipt was not observed in the target"
    );
    Ok(())
}

#[cfg(unix)]
fn same_signature_snapshot(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    same_file_identity(left, right)
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
}

#[cfg(not(unix))]
fn same_signature_snapshot(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    same_file_identity(left, right)
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

fn signature_link_preflight(
    source: &Path,
    target: &Path,
    expected_bytes: u64,
) -> Result<fs::Metadata> {
    let source_path = source.join(ARCHIVE_V2_SIGNATURES_FILE);
    let target_path = target.join(ARCHIVE_V2_SIGNATURES_FILE);
    let source_metadata = fs::symlink_metadata(&source_path)
        .with_context(|| format!("inspect signature link source {}", source_path.display()))?;
    ensure!(
        source_metadata.file_type().is_file() && source_metadata.len() == expected_bytes,
        "signature link source is not the expected regular file"
    );
    ensure!(
        !target_path.try_exists()?,
        "signature link destination already exists"
    );
    let source_directory = fs::symlink_metadata(source)?;
    let target_directory = fs::symlink_metadata(target)?;
    ensure!(
        source_directory.file_type().is_dir() && target_directory.file_type().is_dir(),
        "signature link endpoint parent is not a directory"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            source_metadata.dev() == target_directory.dev(),
            "signature hard link requires the same filesystem"
        );
        ensure!(
            source_metadata.uid() == source_directory.uid()
                && source_metadata.gid() == source_directory.gid(),
            "signature sidecar ownership does not match its immutable source generation"
        );
    }
    Ok(source_metadata)
}

#[cfg(target_os = "linux")]
fn create_strict_signature_hard_link(
    source: &Path,
    target: &Path,
    expected_bytes: u64,
) -> Result<()> {
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    };
    let source_path = source.join(ARCHIVE_V2_SIGNATURES_FILE);
    let target_path = target.join(ARCHIVE_V2_SIGNATURES_FILE);
    let before = signature_link_preflight(source, target, expected_bytes)?;
    let source_c =
        CString::new(source_path.as_os_str().as_bytes()).context("signature path contains NUL")?;
    // SAFETY: source_c is a live NUL-terminated path. O_PATH obtains metadata/link identity only;
    // it cannot read file data.
    let source_fd = unsafe {
        libc::open(
            source_c.as_ptr(),
            libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if source_fd < 0 {
        return Err(io::Error::last_os_error())
            .with_context(|| format!("open signature identity {}", source_path.display()));
    }
    // SAFETY: source_fd was returned by open and is now owned by this File.
    let source_fd = unsafe { File::from_raw_fd(source_fd) };
    let opened_metadata = source_fd.metadata()?;
    ensure!(
        opened_metadata.file_type().is_file()
            && opened_metadata.len() == expected_bytes
            && same_signature_snapshot(&before, &opened_metadata),
        "signature source changed before link"
    );
    let target_c =
        CString::new(target.as_os_str().as_bytes()).context("staging path contains NUL")?;
    // SAFETY: target_c is a live NUL-terminated path. The descriptor is metadata-only.
    let target_fd = unsafe {
        libc::open(
            target_c.as_ptr(),
            libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if target_fd < 0 {
        return Err(io::Error::last_os_error())
            .with_context(|| format!("open staging identity {}", target.display()));
    }
    // SAFETY: target_fd was returned by open and is now owned by this File.
    let target_fd = unsafe { File::from_raw_fd(target_fd) };
    let name = CString::new(ARCHIVE_V2_SIGNATURES_FILE).unwrap();
    let proc_fd_path = CString::new(format!("/proc/self/fd/{}", source_fd.as_raw_fd())).unwrap();
    // SAFETY: the retained O_PATH descriptor pins the exact inode. AT_SYMLINK_FOLLOW dereferences
    // only the procfs descriptor link, not the original source path. This works without the
    // CAP_DAC_READ_SEARCH requirement of linkat(AT_EMPTY_PATH), and linkat still fails if the
    // destination already exists.
    let linked = unsafe {
        libc::linkat(
            libc::AT_FDCWD,
            proc_fd_path.as_ptr(),
            target_fd.as_raw_fd(),
            name.as_ptr(),
            libc::AT_SYMLINK_FOLLOW,
        )
    };
    if linked != 0 {
        return Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "hard-link exact signature inode into {}",
                target_path.display()
            )
        });
    }
    let source_after = fs::symlink_metadata(&source_path)?;
    let target_after = fs::symlink_metadata(&target_path)?;
    if !(source_after.file_type().is_file()
        && target_after.file_type().is_file()
        && same_signature_snapshot(&before, &source_after)
        && same_signature_snapshot(&source_after, &target_after)
        && target_after.len() == expected_bytes)
    {
        let _ = fs::remove_file(&target_path);
        bail!("signature source changed or the target is not the exact hard-linked inode");
    }
    sync_directory(target)
}

#[cfg(not(target_os = "linux"))]
fn create_strict_signature_hard_link(
    source: &Path,
    target: &Path,
    expected_bytes: u64,
) -> Result<()> {
    let source_path = source.join(ARCHIVE_V2_SIGNATURES_FILE);
    let target_path = target.join(ARCHIVE_V2_SIGNATURES_FILE);
    let before = signature_link_preflight(source, target, expected_bytes)?;
    fs::hard_link(&source_path, &target_path).with_context(|| {
        format!(
            "create no-replace signature hard link {} -> {}",
            source_path.display(),
            target_path.display()
        )
    })?;
    let source_after = fs::symlink_metadata(&source_path)?;
    let target_after = fs::symlink_metadata(&target_path)?;
    if !(source_after.file_type().is_file()
        && target_after.file_type().is_file()
        && same_signature_snapshot(&before, &source_after)
        && same_signature_snapshot(&source_after, &target_after)
        && target_after.len() == expected_bytes)
    {
        let _ = fs::remove_file(&target_path);
        bail!("signature source changed or the target is not the exact hard-linked inode");
    }
    sync_directory(target)
}

fn validate_signature_hard_link_metadata(
    source: &Path,
    target: &Path,
    expected_bytes: u64,
) -> Result<()> {
    let source_path = source.join(ARCHIVE_V2_SIGNATURES_FILE);
    let target_path = target.join(ARCHIVE_V2_SIGNATURES_FILE);
    let source_metadata = fs::symlink_metadata(&source_path)?;
    let target_metadata = fs::symlink_metadata(&target_path)?;
    ensure!(
        source_metadata.file_type().is_file()
            && target_metadata.file_type().is_file()
            && source_metadata.len() == expected_bytes
            && target_metadata.len() == expected_bytes
            && same_signature_snapshot(&source_metadata, &target_metadata),
        "signature target is not the exact expected hard link"
    );
    Ok(())
}

fn preflight_access_index(path: &Path, expected_rows: usize) -> Result<()> {
    let metadata = regular_file_metadata(path)?;
    ensure!(
        metadata.len() >= ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN as u64,
        "block-access index is shorter than its header"
    );
    let mut file = File::open(path)?;
    let mut header = [0u8; ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN];
    file.read_exact(&mut header)?;
    ensure!(
        &header[..8] == ARCHIVE_V2_BLOCK_ACCESS_INDEX_MAGIC,
        "invalid block-access index magic"
    );
    let row_count = u64::from_le_bytes(header[12..20].try_into().unwrap());
    ensure!(
        row_count == expected_rows as u64,
        "block-access index declares {row_count} rows; hot index has {expected_rows}"
    );
    let expected_len = u64::try_from(ARCHIVE_V2_BLOCK_ACCESS_INDEX_HEADER_LEN)?
        .checked_add(
            row_count
                .checked_mul(u64::try_from(ARCHIVE_V2_BLOCK_ACCESS_INDEX_ROW_LEN)?)
                .context("block-access index row byte count overflow")?,
        )
        .context("block-access index byte length overflow")?;
    ensure!(
        metadata.len() == expected_len,
        "block-access index has {} bytes; expected {expected_len}",
        metadata.len()
    );
    Ok(())
}

fn build_get_block_rows(
    hot_rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    access_rows: &[ArchiveV2BlockAccessIndexRow],
) -> Result<Vec<ArchiveV2GetBlockIndexRow>> {
    ensure!(hot_rows.len() == access_rows.len());
    let mut rows = vec![ArchiveV2GetBlockIndexRow::missing(); crate::SLOTS_PER_EPOCH as usize];
    for (hot, access) in hot_rows.iter().zip(access_rows) {
        ensure!(hot.block_id == access.block_id && hot.slot == access.slot);
        let offset = (hot.slot % crate::SLOTS_PER_EPOCH) as usize;
        ensure!(
            rows[offset].is_missing(),
            "duplicate get-block slot {}",
            hot.slot
        );
        rows[offset] = ArchiveV2GetBlockIndexRow {
            block_offset: hot.compressed_offset,
            block_len: hot.compressed_len,
            access_offset: access.access_offset,
            access_len: access.access_len,
        };
    }
    Ok(rows)
}

fn load_access_build_context(
    source: &Path,
    expected_blocks: usize,
    epoch: u64,
) -> Result<Option<AccessBuildContext>> {
    let blob_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
    let index_path = source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE);
    let blob_exists = fs::symlink_metadata(&blob_path)
        .map(|metadata| metadata.file_type().is_file())
        .or_else(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error)
            }
        })?;
    let index_exists = fs::symlink_metadata(&index_path)
        .map(|metadata| metadata.file_type().is_file())
        .or_else(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(error)
            }
        })?;
    ensure!(
        blob_exists == index_exists,
        "source has only one of block-access blob/index"
    );
    if !blob_exists {
        ensure!(
            !source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE).try_exists()?,
            "source get-block index exists without block-access sidecar"
        );
        return Ok(None);
    }

    // The access payload is a trusted phase-2 input. The caller streams and validates it before it
    // publishes the remapped target access. This context validates the independent hash fields.
    regular_file_metadata(&blob_path)?;
    regular_file_metadata(&index_path)?;
    let blockhash_path = source.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE);
    let blockhash_metadata = regular_file_metadata(&blockhash_path)?;
    let expected_blockhash_bytes = u64::try_from(expected_blocks)?
        .checked_mul(32)
        .context("blockhash registry length overflow")?;
    ensure!(
        blockhash_metadata.len() == expected_blockhash_bytes,
        "blockhash registry has invalid length {} for {expected_blocks} blocks (expected {expected_blockhash_bytes})",
        blockhash_metadata.len(),
    );
    let blockhashes = super::load_blockhash_registry_plain(&blockhash_path)?;
    let previous_tail = load_previous_blockhash_tail_bounded(source, epoch)?;
    ensure!(
        !previous_tail.is_empty(),
        "block-access rebuild for a non-genesis epoch requires a non-empty previous blockhash tail"
    );
    let vote_path = source.join(ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE);
    let vote_hashes = if vote_path.try_exists()? {
        let metadata = regular_file_metadata(&vote_path)?;
        let max_vote_bytes = u64::try_from(expected_blocks)?
            .checked_mul(65)
            .context("vote hash registry bound overflow")?;
        ensure!(
            metadata.len() == max_vote_bytes,
            "vote hash registry has invalid length {} for {expected_blocks} blocks",
            metadata.len()
        );
        super::load_vote_hash_registry(&vote_path)?
    } else {
        Vec::new()
    };
    Ok(Some(AccessBuildContext {
        blockhashes,
        previous_tail,
        vote_hashes,
    }))
}

fn load_previous_blockhash_tail_bounded(
    source: &Path,
    epoch: u64,
) -> Result<Vec<super::PreviousBlockhash>> {
    let path = source.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE);
    let (mut file, metadata) = match open_regular_read(&path) {
        Ok(opened) => opened,
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::NotFound) =>
        {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    let max_bytes = u64::try_from(super::ROLLING_BLOCKHASH_CAPACITY)?
        .checked_mul(40)
        .context("previous blockhash tail bound overflow")?;
    ensure!(
        metadata.len() <= max_bytes,
        "previous blockhash tail has {} bytes, exceeding {max_bytes}",
        metadata.len()
    );
    if metadata.len() == 0 {
        return Ok(Vec::new());
    }
    let mut bytes = vec![0u8; usize::try_from(metadata.len())?];
    file.read_exact(&mut bytes)?;
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    decode_previous_blockhash_tail_bytes(&bytes, epoch)
}

fn decode_previous_blockhash_tail_bytes(
    bytes: &[u8],
    epoch: u64,
) -> Result<Vec<super::PreviousBlockhash>> {
    ensure!(epoch != 0, "genesis has no previous blockhash tail");
    ensure!(!bytes.is_empty(), "previous blockhash tail is empty");

    // A legacy hash-only tail can have a byte length divisible by both 32 and 40 (including the
    // normal 300-row/9,600-byte file).  Length alone therefore cannot select the schema.  Current
    // rows are accepted only when their slots form a strictly increasing sequence in the previous
    // epoch; if both schemas remain possible, fail closed rather than reinterpret hash bytes.
    let previous_epoch = epoch.checked_sub(1).context("previous epoch underflow")?;
    let previous_epoch_start = previous_epoch
        .checked_mul(crate::SLOTS_PER_EPOCH)
        .context("previous epoch slot range overflow")?;
    let epoch_start = epoch
        .checked_mul(crate::SLOTS_PER_EPOCH)
        .context("epoch slot range overflow")?;

    let current = if bytes.len().is_multiple_of(40)
        && bytes.len() / 40 <= super::ROLLING_BLOCKHASH_CAPACITY
    {
        let mut rows = Vec::new();
        rows.try_reserve_exact(bytes.len() / 40)
            .context("allocate current previous blockhash tail")?;
        let mut previous_slot = None;
        let mut slots_are_canonical = true;
        for chunk in bytes.chunks_exact(40) {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&chunk[..32]);
            let slot = u64::from_le_bytes(chunk[32..40].try_into().unwrap());
            if !(previous_epoch_start..epoch_start).contains(&slot)
                || previous_slot.is_some_and(|previous| slot <= previous)
            {
                slots_are_canonical = false;
                break;
            }
            rows.push(super::PreviousBlockhash { hash, slot });
            previous_slot = Some(slot);
        }
        slots_are_canonical.then_some(rows)
    } else {
        None
    };

    let legacy = if bytes.len().is_multiple_of(32)
        && bytes.len() / 32 <= super::ROLLING_BLOCKHASH_CAPACITY
    {
        let mut rows = Vec::new();
        rows.try_reserve_exact(bytes.len() / 32)
            .context("allocate legacy previous blockhash tail")?;
        for chunk in bytes.chunks_exact(32) {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(chunk);
            rows.push(super::PreviousBlockhash { hash, slot: 0 });
        }
        Some(rows)
    } else {
        None
    };

    match (current, legacy) {
        (Some(rows), None) | (None, Some(rows)) => Ok(rows),
        (Some(_), Some(_)) => bail!(
            "previous blockhash tail byte length {} is ambiguous between current and legacy schemas",
            bytes.len()
        ),
        (None, None) => bail!(
            "previous blockhash tail has no valid bounded current or legacy schema (bytes={}, epoch={epoch})",
            bytes.len()
        ),
    }
}

const INDEPENDENT_SIDECARS: &[&str] = &[
    ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
    ARCHIVE_V2_BLOCKHASH_INDEX_V3_FILE,
    ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE,
    ARCHIVE_V2_VOTE_HASH_REGISTRY_FILE,
    ARCHIVE_V2_POH_FILE,
    ARCHIVE_V2_SHREDDING_FILE,
    BLOCK_TIME_GAP_FILE,
];

fn copy_independent_sidecars(
    source: &Path,
    target: &Path,
    prewritten: &BTreeMap<String, FileBinding>,
) -> Result<BTreeMap<String, FileBinding>> {
    let mut copied = BTreeMap::new();
    for &name in INDEPENDENT_SIDECARS {
        let source_path = source.join(name);
        match fs::symlink_metadata(&source_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("inspect {}", source_path.display()));
            }
            Ok(metadata) => ensure!(
                metadata.file_type().is_file(),
                "sidecar is not a regular file: {}",
                source_path.display()
            ),
        }
        let target_path = target.join(name);
        let binding = if let Some(expected) = prewritten.get(name) {
            ensure!(
                regular_file_metadata(&target_path)?.len() == expected.bytes,
                "prewritten sidecar length mismatch for {name}"
            );
            expected.clone()
        } else {
            match clone_or_copy_file(&source_path, &target_path)? {
                Some(actual) => actual,
                None => hash_file(&source_path)?,
            }
        };
        let target_metadata = regular_file_metadata(&target_path)?;
        ensure!(
            target_metadata.len() == binding.bytes,
            "copied sidecar length mismatch for {name}: target={} source={}",
            target_metadata.len(),
            binding.bytes
        );
        copied.insert(name.to_owned(), binding);
    }
    ensure!(
        prewritten
            .keys()
            .all(|name| copied.get(name) == prewritten.get(name)),
        "prewritten sidecar binding names are not independent sidecars"
    );
    for required in [
        ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
        ARCHIVE_V2_POH_FILE,
        ARCHIVE_V2_SHREDDING_FILE,
    ] {
        ensure!(
            copied.contains_key(required),
            "required independent sidecar is missing: {required}"
        );
    }
    Ok(copied)
}

#[cfg(target_os = "macos")]
fn clone_or_copy_file(source: &Path, target: &Path) -> Result<Option<FileBinding>> {
    use std::os::unix::ffi::OsStrExt;
    let source_c =
        CString::new(source.as_os_str().as_bytes()).context("source path contains NUL")?;
    let target_c =
        CString::new(target.as_os_str().as_bytes()).context("target path contains NUL")?;
    // SAFETY: both path strings remain valid, NUL-terminated C strings for the duration of the
    // call. `target` is inside a private fresh staging directory and does not exist.
    if unsafe { libc::clonefile(source_c.as_ptr(), target_c.as_ptr(), 0) } == 0 {
        return Ok(None);
    }
    if target.exists() {
        fs::remove_file(target)
            .with_context(|| format!("remove incomplete clone destination {}", target.display()))?;
    }
    copy_file_with_hash(source, target).map(Some)
}

fn analyze_target_block(
    input: CompressedBlockInput,
    registry: &MappedRegistry,
    wire_profile: ArchiveV2WireProfile,
) -> Result<BlockSemantic> {
    let row = input.row;
    let mut block = decode_hot_block(&input)?;
    normalize_block(
        &mut block,
        u64::from(row.block_id),
        row.slot,
        wire_profile,
        |key, _class| resolve_compact_pubkey(*key, registry),
    )
}

fn recompute_source_canonical_counts(
    source: &Path,
    target: &Path,
    receipt: &RegistryReprocessReceipt,
    epoch: u64,
) -> Result<(SemanticBinding, FileBinding)> {
    let manifest = read_source_manifest(source)?;
    let footer = validate_source_meta_for_deep(source)?;
    let registry = MappedRegistry::open(&source.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    ensure!(
        manifest.registry_keys == registry.len as u64
            && receipt.source_registry_keys == registry.len as u64,
        "source registry key count disagrees with manifest or receipt"
    );
    let source_index = validate_source_registry_index(source, &registry)?;
    let mut declared_counts = read_registry_counts(
        &source.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        registry.len,
    )?;
    let declared_sum = declared_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("source registry reference count overflow")
    })?;
    ensure!(
        declared_sum == manifest.references,
        "source registry count sum {declared_sum} != manifest references {}",
        manifest.references
    );

    let blocks_path = source.join(ARCHIVE_V2_BLOCKS_FILE);
    let hot_index = read_archive_v2_hot_block_index(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    validate_hot_index(&blocks_path, &hot_index, epoch)?;
    ensure!(
        footer.blocks == hot_index.rows.len() as u64,
        "source footer block count mismatch during deep validation"
    );
    let final_transactions = hot_index
        .rows
        .last()
        .map(|row| {
            row.first_tx_ordinal
                .checked_add(u64::from(row.tx_count))
                .context("deep source transaction ordinal overflow")
        })
        .transpose()?
        .unwrap_or(0);
    let final_signatures = hot_index
        .rows
        .last()
        .map(|row| {
            row.first_signature_ordinal
                .checked_add(u64::from(row.signature_count))
                .context("deep source signature ordinal overflow")
        })
        .transpose()?
        .unwrap_or(0);
    ensure!(
        footer.transactions == final_transactions,
        "source footer transaction count mismatch during deep validation"
    );
    ensure!(
        regular_file_metadata(&source.join(ARCHIVE_V2_SIGNATURES_FILE))?.len()
            == final_signatures
                .checked_mul(64)
                .context("deep source signature byte count overflow")?,
        "source signatures sidecar length mismatch during deep validation"
    );

    let mut eligible_counts = Vec::new();
    eligible_counts
        .try_reserve_exact(registry.len)
        .context("allocate deep eligible-count vector")?;
    eligible_counts.resize(registry.len, 0u32);
    let threads = receipt.threads.clamp(1, 64);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("registry-deep-validate-{index}"))
        .build()
        .context("build deep registry validation pool")?;
    let max_rows = parallel_batch_rows(threads);
    let (mut blocks_file, blocks_metadata) = open_regular_read(&blocks_path)?;
    ensure!(blocks_metadata.len() == hot_index.blob_file_bytes);
    let mut block_hasher = Sha256::new();
    let mut semantics = SemanticAccumulator::new();
    let mut start = 0usize;
    while start < hot_index.rows.len() {
        let end = hot_batch_end(&hot_index.rows, start, max_rows, false)?;
        let inputs = read_compressed_block_batch(
            &mut blocks_file,
            &hot_index.rows[start..end],
            Some(&mut block_hasher),
        )?;
        let analyses = pool.install(|| {
            inputs
                .into_par_iter()
                .map(|input| {
                    analyze_source_block(
                        input,
                        &registry,
                        receipt
                            .wire_profile
                            .context("registry receipt has no admitted source wire profile")?,
                    )
                })
                .collect::<Result<Vec<_>>>()
        })?;
        for analysis in analyses {
            merge_count_runs(&mut eligible_counts, &analysis.eligible, false)?;
            merge_count_runs(&mut declared_counts, &analysis.all, true)?;
            semantics.push(&analysis.semantic)?;
        }
        start = end;
    }
    ensure_open_file_unchanged(&blocks_path, &blocks_file, &blocks_metadata)?;
    ensure!(
        declared_counts.iter().all(|&remaining| remaining == 0),
        "source registry counts do not match the deep typed-reference traversal"
    );
    let semantics = semantics.finish();
    ensure!(
        semantics.pubkey_references == manifest.references,
        "deep source traversal reference count mismatch"
    );
    let eligible_references = eligible_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("deep eligible reference count overflow")
    })?;
    ensure!(
        eligible_references == receipt.eligible_references,
        "recomputed eligible references {eligible_references} != receipt {}",
        receipt.eligible_references
    );
    validate_target_registry_against_recomputed(
        &registry,
        &source_index,
        &eligible_counts,
        target,
        receipt.target_registry_keys,
        receipt.eligible_references,
    )?;
    Ok((
        semantics,
        FileBinding {
            bytes: hot_index.blob_file_bytes,
            sha256: hex_digest(block_hasher.finalize()),
        },
    ))
}

fn validate_target_registry_against_recomputed(
    source_registry: &MappedRegistry,
    source_index: &KeyIndex,
    eligible_counts: &[u32],
    target: &Path,
    receipt_target_keys: u64,
    receipt_eligible_references: u64,
) -> Result<()> {
    ensure!(source_registry.len == eligible_counts.len());
    let target_registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let target_counts = read_registry_counts(
        &target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
        target_registry.len,
    )?;
    let eligible_sum = eligible_counts.iter().try_fold(0u64, |sum, &count| {
        sum.checked_add(u64::from(count))
            .context("eligible count sum overflow")
    })?;
    ensure!(
        eligible_sum == receipt_eligible_references,
        "eligible count sum {eligible_sum} != receipt {receipt_eligible_references}"
    );
    let positive_keys = eligible_counts.iter().filter(|&&count| count != 0).count();
    let builtin = compute_budget_key();
    let builtin_source_id = source_index.lookup(&builtin).filter(|&id| {
        source_registry
            .key(id)
            .is_ok_and(|source_key| source_key == builtin)
    });
    let builtin_count = builtin_source_id
        .and_then(|id| eligible_counts.get((id - 1) as usize).copied())
        .unwrap_or(0);
    let synthetic_builtin = builtin_count == 0;
    let expected_target_keys = positive_keys
        .checked_add(usize::from(synthetic_builtin))
        .context("expected target key count overflow")?;
    ensure!(
        target_registry.len == expected_target_keys
            && target_registry.len as u64 == receipt_target_keys,
        "target registry key set does not match recomputed eligible source keys"
    );

    let mut matched_positive = 0usize;
    let mut target_sum = 0u64;
    let mut saw_synthetic_builtin = false;
    for (position, (&key, &target_count)) in target_registry
        .keys()
        .iter()
        .zip(&target_counts)
        .enumerate()
    {
        if synthetic_builtin && key == builtin {
            ensure!(
                position == 0 && target_count == 0 && !saw_synthetic_builtin,
                "synthetic ComputeBudget must be the unique zero-count ID-1 prefix"
            );
            saw_synthetic_builtin = true;
            continue;
        }
        let source_id = source_index
            .lookup(&key)
            .filter(|&id| {
                source_registry
                    .key(id)
                    .is_ok_and(|source_key| source_key == key)
            })
            .with_context(|| {
                format!(
                    "target registry key at ID {} is absent from the source registry",
                    position + 1
                )
            })?;
        let expected_count = eligible_counts[(source_id - 1) as usize];
        ensure!(
            expected_count != 0,
            "target registry retains source key ID {source_id} with zero canonical usage"
        );
        ensure!(
            target_count == expected_count,
            "target registry count mismatch at target ID {}: target={target_count} recomputed={expected_count}",
            position + 1
        );
        matched_positive += 1;
        target_sum = target_sum
            .checked_add(u64::from(target_count))
            .context("target canonical count sum overflow")?;
    }
    ensure!(
        saw_synthetic_builtin == synthetic_builtin,
        "target synthetic ComputeBudget presence mismatch"
    );
    ensure!(
        matched_positive == positive_keys,
        "target registry omits or duplicates recomputed eligible source keys"
    );
    ensure!(
        target_sum == receipt_eligible_references,
        "target registry count sum {target_sum} != receipt {receipt_eligible_references}"
    );
    Ok(())
}

fn validate_source_meta_for_deep(source: &Path) -> Result<WincodeArchiveV2Footer> {
    let path = source.join(ARCHIVE_V2_META_FILE);
    let (file, metadata) = open_regular_read(&path)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let first_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("source metadata is empty")?;
    let first: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &first_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let ArchiveV2HotMetaRecord::Header(header) = first else {
        bail!("source metadata does not start with a header");
    };
    let expected_flags = WINCODE_ARCHIVE_V2_FLAG_LEB128
        | WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY
        | WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS;
    ensure!(
        header.version == WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION && header.flags == expected_flags,
        "source metadata is not strict first-seen/all-reference Compact-V2"
    );
    let second_bytes = read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?
        .context("source metadata is missing footer")?;
    let second: ArchiveV2HotMetaRecord = wincode::config::deserialize_exact(
        &second_bytes,
        bounded_wincode_config::<MAX_META_FRAME_BYTES>(),
    )?;
    let ArchiveV2HotMetaRecord::Footer(footer) = second else {
        bail!("source metadata footer is missing or is genesis");
    };
    ensure!(
        read_bounded_frame(&mut reader, MAX_META_FRAME_BYTES)?.is_none(),
        "source metadata has trailing records"
    );
    ensure_open_file_unchanged(&path, reader.get_ref(), &metadata)?;
    Ok(footer)
}

fn scan_target_generation_semantics(
    target: &Path,
    epoch: u64,
    threads: usize,
    receipt_wire_profile: ArchiveV2WireProfile,
) -> Result<(SemanticBinding, FileBinding)> {
    let blocks_path = target.join(ARCHIVE_V2_BLOCKS_FILE);
    let index = read_archive_v2_hot_block_index(&target.join(ARCHIVE_V2_BLOCK_INDEX_FILE))?;
    validate_hot_index(&blocks_path, &index, epoch)?;
    let registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE))?;
    let (mut file, metadata) = open_regular_read(&blocks_path)?;
    ensure!(metadata.len() == index.blob_file_bytes);
    let mut hasher = Sha256::new();
    let mut semantics = SemanticAccumulator::new();
    let threads = threads.clamp(1, 64);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("registry-target-validate-{index}"))
        .build()
        .context("build target registry validation pool")?;
    let max_rows = parallel_batch_rows(threads);
    let mut start = 0usize;
    while start < index.rows.len() {
        let end = hot_batch_end(&index.rows, start, max_rows, false)?;
        let inputs =
            read_compressed_block_batch(&mut file, &index.rows[start..end], Some(&mut hasher))?;
        let analyses = pool.install(|| {
            inputs
                .into_par_iter()
                .map(|input| analyze_target_block(input, &registry, receipt_wire_profile))
                .collect::<Result<Vec<_>>>()
        })?;
        for analysis in analyses {
            semantics.push(&analysis)?;
        }
        start = end;
    }
    ensure_open_file_unchanged(&blocks_path, &file, &metadata)?;
    Ok((
        semantics.finish(),
        FileBinding {
            bytes: index.blob_file_bytes,
            sha256: hex_digest(hasher.finalize()),
        },
    ))
}

fn hash_file(path: &Path) -> Result<FileBinding> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options
        .open(path)
        .with_context(|| format!("open bound artifact {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("stat bound artifact {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "bound artifact is not a regular file: {}",
        path.display()
    );
    let mut file = BufReader::with_capacity(IO_BUFFER_BYTES, file);
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    let mut bytes = 0u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("artifact byte count overflow")?;
    }
    ensure!(
        bytes == metadata.len(),
        "artifact changed length while hashing: {}",
        path.display()
    );
    let after = file.get_ref().metadata()?;
    ensure!(
        same_file_snapshot(&metadata, &after),
        "artifact changed while hashing: {}",
        path.display()
    );
    let path_metadata = fs::symlink_metadata(path)
        .with_context(|| format!("reinspect bound artifact {}", path.display()))?;
    ensure!(
        path_metadata.file_type().is_file() && same_file_snapshot(&metadata, &path_metadata),
        "artifact path changed while hashing: {}",
        path.display()
    );
    Ok(FileBinding {
        bytes,
        sha256: hex_digest(hasher.finalize()),
    })
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
}

#[cfg(unix)]
fn same_file_snapshot(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    same_file_identity(left, right)
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[cfg(not(unix))]
fn same_file_snapshot(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    same_file_identity(left, right) && left.modified().ok() == right.modified().ok()
}

fn add_binding_if_file(
    bindings: &mut BTreeMap<String, FileBinding>,
    directory: &Path,
    name: &str,
) -> Result<()> {
    let path = directory.join(name);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("inspect {}", path.display())),
        Ok(_) => {
            ensure!(
                bindings
                    .insert(name.to_owned(), hash_file(&path)?)
                    .is_none(),
                "duplicate file binding {name}"
            );
            Ok(())
        }
    }
}

fn source_file_bindings(
    source: &Path,
    copied: &BTreeMap<String, FileBinding>,
    blocks: FileBinding,
    receipt_version: u32,
) -> Result<BTreeMap<String, FileBinding>> {
    ensure!(
        matches!(
            receipt_version,
            RECEIPT_VERSION_V1 | RECEIPT_VERSION_V2 | RECEIPT_VERSION
        ),
        "unsupported receipt version for source bindings"
    );
    let mut bindings = BTreeMap::new();
    bindings.insert(ARCHIVE_V2_BLOCKS_FILE.to_owned(), blocks);
    for name in [
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
    ] {
        add_binding_if_file(&mut bindings, source, name)?;
    }
    if receipt_version == RECEIPT_VERSION_V1 {
        for name in [
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
            ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
        ] {
            add_binding_if_file(&mut bindings, source, name)?;
        }
    }
    for (name, binding) in copied {
        ensure!(
            bindings.insert(name.clone(), binding.clone()).is_none(),
            "duplicate source binding {name}"
        );
    }
    Ok(bindings)
}

fn target_file_bindings(
    target: &Path,
    copied: &BTreeMap<String, FileBinding>,
    blocks: FileBinding,
    access: Option<FileBinding>,
) -> Result<BTreeMap<String, FileBinding>> {
    let mut bindings = BTreeMap::new();
    bindings.insert(ARCHIVE_V2_BLOCKS_FILE.to_owned(), blocks);
    if let Some(access) = access {
        bindings.insert(ARCHIVE_V2_BLOCK_ACCESS_FILE.to_owned(), access);
    }
    for name in [
        ARCHIVE_V2_BLOCK_INDEX_FILE,
        ARCHIVE_V2_META_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
        ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
        ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
    ] {
        add_binding_if_file(&mut bindings, target, name)?;
    }
    for (name, binding) in copied {
        ensure!(
            bindings.insert(name.clone(), binding.clone()).is_none(),
            "duplicate target binding {name}"
        );
    }
    Ok(bindings)
}

fn validate_bound_files_except_blocks(
    directory: &Path,
    expected: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    for (name, expected) in expected {
        if name == ARCHIVE_V2_BLOCKS_FILE {
            continue;
        }
        ensure!(
            !name.contains('/') && !name.contains('\\'),
            "receipt contains nested artifact name"
        );
        let actual = hash_file(&directory.join(name))?;
        ensure!(&actual == expected, "artifact binding mismatch for {name}");
    }
    Ok(())
}

fn validate_bound_files_for_deep(
    directory: &Path,
    expected: &BTreeMap<String, FileBinding>,
    receipt_version: u32,
) -> Result<()> {
    if receipt_version == RECEIPT_VERSION {
        validate_bound_files_except_blocks_and_signatures(directory, expected)
    } else {
        debug_assert!(matches!(
            receipt_version,
            RECEIPT_VERSION_V1 | RECEIPT_VERSION_V2
        ));
        validate_bound_files_except_blocks(directory, expected)
    }
}

fn generation_digest(files: &BTreeMap<String, FileBinding>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(GENERATION_DOMAIN);
    hasher.update((files.len() as u64).to_le_bytes());
    for (name, binding) in files {
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(binding.bytes.to_le_bytes());
        hasher.update(binding.sha256.as_bytes());
    }
    hex_digest(hasher.finalize())
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn validate_hex_sha256(value: &str, label: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label} is not a lowercase SHA-256 digest"
    );
    Ok(())
}

fn validate_access_boundary_repair_shape(repair: &AccessBoundaryRepair, epoch: u64) -> Result<()> {
    validate_hex_sha256(
        &repair.original_previous_blockhash_hex,
        "original access previous blockhash",
    )?;
    validate_hex_sha256(
        &repair.corrected_previous_blockhash_hex,
        "corrected access previous blockhash",
    )?;
    ensure!(
        epoch == EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH
            && repair.mode == EPOCH_301_ACCESS_BOUNDARY_REPAIR_MODE
            && repair.block_id == EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID
            && repair.block_slot == EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT
            && repair.trusted_predecessor_slot == EPOCH_301_ACCESS_BOUNDARY_REPAIR_PREDECESSOR_SLOT
            && repair.original_previous_blockhash_hex
                == EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX
            && repair.corrected_previous_blockhash_hex
                == EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX
            && repair.original_previous_blockhash_hex != repair.corrected_previous_blockhash_hex,
        "registry reprocess receipt has unsupported access boundary repair provenance"
    );
    Ok(())
}

fn validate_access_boundary_repair_source_bindings(
    source_files: &BTreeMap<String, FileBinding>,
) -> Result<()> {
    let tail = source_files
        .get(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE)
        .context("repaired receipt omits the trusted predecessor tail")?;
    let manifest = source_files
        .get(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE)
        .context("repaired receipt omits the first-seen manifest")?;
    let access_blob = source_files
        .get(ARCHIVE_V2_BLOCK_ACCESS_FILE)
        .context("repaired receipt omits the source access blob")?;
    let access_index = source_files
        .get(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE)
        .context("repaired receipt omits the source access index")?;
    validate_epoch_301_source_access_blob_binding(access_blob)?;
    ensure!(
        tail.bytes == EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_BYTES
            && tail.sha256 == EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_SHA256
            && manifest.sha256 == EPOCH_301_ACCESS_BOUNDARY_REPAIR_MANIFEST_SHA256
            && access_index.bytes == EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_BYTES
            && access_index.sha256 == EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_SHA256,
        "repaired receipt source bindings do not identify the pinned production generation"
    );
    Ok(())
}

fn validate_receipt_shape(receipt: &RegistryReprocessReceipt, epoch: u64) -> Result<()> {
    ensure!(
        matches!(
            receipt.version,
            RECEIPT_VERSION_V1 | RECEIPT_VERSION_V2 | RECEIPT_VERSION
        ),
        "unsupported registry reprocess receipt version"
    );
    let expected_algorithm = match receipt.version {
        RECEIPT_VERSION_V1 => RECEIPT_ALGORITHM_V1,
        RECEIPT_VERSION_V2 => RECEIPT_ALGORITHM_V2,
        RECEIPT_VERSION => RECEIPT_ALGORITHM,
        _ => unreachable!(),
    };
    ensure!(
        receipt.algorithm == expected_algorithm,
        "unsupported registry reprocess algorithm"
    );
    ensure!(
        receipt.epoch == epoch,
        "registry reprocess receipt epoch mismatch"
    );
    ensure!(
        receipt.source_registry_keys > 0 && receipt.source_registry_keys <= u64::from(u32::MAX)
    );
    ensure!(
        receipt.target_registry_keys > 0
            && receipt.target_registry_keys <= receipt.source_registry_keys + 1
    );
    match receipt.version {
        RECEIPT_VERSION_V1 => {
            let source = receipt
                .source_semantics
                .as_ref()
                .context("v1 receipt omits source semantics")?;
            let target = receipt
                .target_semantics
                .as_ref()
                .context("v1 receipt omits target semantics")?;
            ensure!(source == target, "receipt does not declare semantic parity");
            ensure!(
                receipt.rewrite_stats.is_none(),
                "v1 receipt contains v2 stats"
            );
            validate_hex_sha256(&source.reference_sha256, "source reference digest")?;
            validate_hex_sha256(
                &source.normalized_structure_sha256,
                "source normalized-structure digest",
            )?;
        }
        RECEIPT_VERSION_V2 | RECEIPT_VERSION => {
            ensure!(
                receipt.source_semantics.is_none() && receipt.target_semantics.is_none(),
                "v2/v3 receipt contains legacy semantic digests"
            );
            let stats = receipt
                .rewrite_stats
                .as_ref()
                .context("v2/v3 receipt omits rewrite stats")?;
            ensure!(stats.blocks > 0);
            ensure!(stats.pubkey_references >= receipt.eligible_references);
        }
        _ => unreachable!(),
    }
    ensure!(receipt.threads > 0 && receipt.sort_memory_mib > 0);
    validate_hex_sha256(&receipt.source_anchor_sha256, "source anchor digest")?;
    validate_hex_sha256(
        &receipt.source_generation_sha256,
        "source generation digest",
    )?;
    validate_hex_sha256(
        &receipt.target_generation_sha256,
        "target generation digest",
    )?;
    for (name, binding) in receipt.source_files.iter().chain(&receipt.target_files) {
        ensure!(!name.is_empty() && !name.contains('/') && !name.contains('\\'));
        validate_hex_sha256(&binding.sha256, "artifact digest")?;
    }
    match receipt.version {
        RECEIPT_VERSION_V1 | RECEIPT_VERSION_V2 => ensure!(
            receipt.attempt_id.is_none()
                && receipt.handoff_sha256.is_none()
                && receipt.assembly_mode.is_none()
                && receipt.signature_provenance.is_none()
                && receipt.wire_profile.is_none()
                && receipt.access_boundary_repair.is_none(),
            "legacy receipt contains staged-access provenance"
        ),
        RECEIPT_VERSION => {
            validate_receipt_wire_profile_binding(receipt)?;
            ensure!(
                receipt
                    .source_files
                    .keys()
                    .all(|name| allowed_v3_source_artifact(name)),
                "v3 source receipt declares an unexpected artifact"
            );
            ensure!(
                receipt
                    .target_files
                    .keys()
                    .all(|name| allowed_v3_target_artifact(name)),
                "v3 target receipt declares an unexpected artifact"
            );
            validate_attempt_id(
                receipt
                    .attempt_id
                    .as_deref()
                    .context("v3 receipt omits attempt ID")?,
            )?;
            validate_hex_sha256(
                receipt
                    .handoff_sha256
                    .as_deref()
                    .context("v3 receipt omits handoff digest")?,
                "v3 handoff digest",
            )?;
            ensure!(
                receipt.assembly_mode.as_deref() == Some(ACCESS_ASSEMBLY_MODE),
                "v3 receipt has an unsupported access assembly mode"
            );
            ensure!(
                receipt.signature_provenance.as_deref() == Some(SIGNATURE_PROVENANCE),
                "v3 receipt has unsupported signature provenance"
            );
            if let Some(repair) = &receipt.access_boundary_repair {
                validate_access_boundary_repair_shape(repair, receipt.epoch)?;
                validate_access_boundary_repair_source_bindings(&receipt.source_files)?;
            }
            for name in [
                ARCHIVE_V2_BLOCK_ACCESS_FILE,
                ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
                ARCHIVE_V2_SIGNATURES_FILE,
            ] {
                ensure!(
                    receipt.source_files.contains_key(name),
                    "v3 source receipt omits required artifact {name}"
                );
            }
            ensure!(
                !receipt
                    .source_files
                    .contains_key(ARCHIVE_V2_GET_BLOCK_INDEX_FILE),
                "v3 source receipt binds derived get-block input"
            );
            for name in [
                ARCHIVE_V2_BLOCK_ACCESS_FILE,
                ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
                ARCHIVE_V2_GET_BLOCK_INDEX_FILE,
                ARCHIVE_V2_SIGNATURES_FILE,
            ] {
                ensure!(
                    receipt.target_files.contains_key(name),
                    "v3 target receipt omits required artifact {name}"
                );
            }
            ensure!(
                receipt.source_files.get(ARCHIVE_V2_SIGNATURES_FILE)
                    == receipt.target_files.get(ARCHIVE_V2_SIGNATURES_FILE),
                "v3 source and target signature bindings differ"
            );
        }
        _ => unreachable!(),
    }
    Ok(())
}

#[cfg(test)]
fn write_receipt(target: &Path, receipt: &RegistryReprocessReceipt) -> Result<()> {
    let path = target.join(REGISTRY_REPROCESS_RECEIPT_FILE);
    ensure!(
        !path.exists(),
        "receipt already exists in staging generation"
    );
    let bytes = serde_json::to_vec_pretty(receipt)?;
    ensure!(
        bytes.len() as u64 <= RECEIPT_MAX_BYTES,
        "registry reprocess receipt exceeds size limit"
    );
    let mut file = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?,
    );
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.flush()?;
    file.get_ref().sync_all()?;
    Ok(())
}

fn write_receipt_temp(target: &Path, receipt: &RegistryReprocessReceipt) -> Result<()> {
    let path = target.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE);
    ensure!(!path.try_exists()?, "receipt temp already exists");
    let mut bytes = serde_json::to_vec_pretty(receipt)?;
    bytes.push(b'\n');
    ensure!(
        bytes.len() as u64 <= RECEIPT_MAX_BYTES,
        "registry reprocess receipt exceeds size limit"
    );
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_directory(target)
}

fn promote_receipt_temp(target: &Path) -> Result<()> {
    let temp = target.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE);
    let receipt = target.join(REGISTRY_REPROCESS_RECEIPT_FILE);
    publish_directory_no_replace(&temp, &receipt)?;
    sync_directory(target)
}

fn read_receipt(target: &Path) -> Result<RegistryReprocessReceipt> {
    let path = target.join(REGISTRY_REPROCESS_RECEIPT_FILE);
    let (mut file, metadata) = open_regular_read(&path)?;
    ensure!(
        metadata.file_type().is_file() && metadata.len() > 0 && metadata.len() <= RECEIPT_MAX_BYTES
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(RECEIPT_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= RECEIPT_MAX_BYTES,
        "receipt grew while reading"
    );
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn read_receipt_temp(target: &Path) -> Result<RegistryReprocessReceipt> {
    let path = target.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE);
    let (mut file, metadata) = open_regular_read(&path)?;
    ensure!(
        metadata.file_type().is_file() && metadata.len() > 0 && metadata.len() <= RECEIPT_MAX_BYTES
    );
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    (&mut file)
        .take(RECEIPT_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= RECEIPT_MAX_BYTES);
    ensure_open_file_unchanged(&path, &file, &metadata)?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn sync_generation(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read staging directory {}", directory.display()))?
    {
        let entry = entry?;
        let metadata = entry.metadata()?;
        ensure!(
            metadata.is_file(),
            "unexpected non-file in completed staging generation: {}",
            entry.path().display()
        );
        File::open(entry.path())
            .with_context(|| format!("open {} for sync", entry.path().display()))?
            .sync_all()
            .with_context(|| format!("sync {}", entry.path().display()))?;
    }
    sync_directory(directory)
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)
        .with_context(|| format!("open directory {} for sync", directory.display()))?
        .sync_all()
        .with_context(|| format!("sync directory {}", directory.display()))
}

#[cfg(target_os = "linux")]
fn publish_directory_no_replace(source: &Path, target: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source_c =
        CString::new(source.as_os_str().as_bytes()).context("staging path contains NUL")?;
    let target_c =
        CString::new(target.as_os_str().as_bytes()).context("target path contains NUL")?;
    // SAFETY: both path strings remain live and NUL-terminated for the syscall.  Staging and
    // target have the same parent, so success is an atomic same-filesystem directory rename.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD as libc::c_long,
            source_c.as_ptr(),
            libc::AT_FDCWD as libc::c_long,
            target_c.as_ptr(),
            libc::RENAME_NOREPLACE as libc::c_long,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "atomically publish {} as {} without replacement",
                source.display(),
                target.display()
            )
        })
    }
}

#[cfg(target_os = "macos")]
fn publish_directory_no_replace(source: &Path, target: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source_c =
        CString::new(source.as_os_str().as_bytes()).context("staging path contains NUL")?;
    let target_c =
        CString::new(target.as_os_str().as_bytes()).context("target path contains NUL")?;
    // SAFETY: both path strings remain live and NUL-terminated for the call. RENAME_EXCL gives
    // atomic no-replace semantics, including when an empty target directory appears concurrently.
    let result =
        unsafe { libc::renamex_np(source_c.as_ptr(), target_c.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).with_context(|| {
            format!(
                "atomically publish {} as {} without replacement",
                source.display(),
                target.display()
            )
        })
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn publish_directory_no_replace(source: &Path, target: &Path) -> Result<()> {
    let _ = (source, target);
    bail!("atomic no-replace directory publication is unsupported on this operating system")
}

#[cfg(target_os = "linux")]
fn clone_or_copy_file(source: &Path, target: &Path) -> Result<Option<FileBinding>> {
    use std::os::fd::AsRawFd;
    let mut source_file = File::open(source)?;
    let mut target_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;
    // SAFETY: both descriptors refer to open regular files and FICLONE does not outlive them.
    if unsafe {
        libc::ioctl(
            target_file.as_raw_fd(),
            libc::FICLONE,
            source_file.as_raw_fd(),
        )
    } == 0
    {
        return Ok(None);
    }
    target_file.set_len(0)?;
    source_file.seek(SeekFrom::Start(0))?;
    let metadata = source_file.metadata()?;
    ensure!(metadata.file_type().is_file());
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let read = source_file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        target_file.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("sidecar copy byte count overflow")?;
    }
    ensure!(
        bytes == metadata.len(),
        "source sidecar changed while copying"
    );
    Ok(Some(FileBinding {
        bytes,
        sha256: hex_digest(hasher.finalize()),
    }))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn clone_or_copy_file(source: &Path, target: &Path) -> Result<Option<FileBinding>> {
    copy_file_with_hash(source, target).map(Some)
}

fn copy_file_with_hash(source: &Path, target: &Path) -> Result<FileBinding> {
    let mut source_file = BufReader::with_capacity(
        IO_BUFFER_BYTES,
        File::open(source).with_context(|| format!("open {}", source.display()))?,
    );
    let metadata = source_file.get_ref().metadata()?;
    ensure!(metadata.file_type().is_file());
    let mut target_file = BufWriter::with_capacity(
        IO_BUFFER_BYTES,
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)
            .with_context(|| format!("create {}", target.display()))?,
    );
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let read = source_file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        target_file.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(read as u64)
            .context("sidecar copy byte count overflow")?;
    }
    target_file.flush()?;
    ensure!(
        bytes == metadata.len(),
        "source sidecar changed while copying"
    );
    Ok(FileBinding {
        bytes,
        sha256: hex_digest(hasher.finalize()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{
        ArchiveV2ComputeBudgetInstructionData, ArchiveV2HotInstruction,
        ArchiveV2HotInstructionData, ArchiveV2HotLegacyMessage, ArchiveV2HotMessagePayload,
        ArchiveV2HotV0Message, ArchiveV2SystemInstructionData, ArchiveV2VoteHashRef,
        ArchiveV2VoteLockoutOffset, ArchiveV2VoteStateUpdate, ArchiveV2VoteTowerSync,
        CompactInnerInstruction, CompactInstructionError, CompactMessageHeader, DataArray,
        DataTable, OwnedCompactAddressTableLookup, OwnedCompactRecentBlockhash, StringTable,
        program_logs::system_program::NonceAction, rewrite_archive_v2_hot_message_wire,
    };
    use blockzilla_read_sdk::manifest::{
        GENERATION_MANIFEST_SCHEMA_VERSION, GenerationFile, compute_generation_digest,
    };
    use of_car_reader::stored_transaction::StoredTransactionError;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};

    static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);
    const TEST_WIRE_PROFILE: ArchiveV2WireProfile =
        ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1;

    fn vote_update(hash: ArchiveV2VoteHashRef, seed: u64) -> ArchiveV2VoteStateUpdate {
        ArchiveV2VoteStateUpdate {
            root: Some(seed),
            lockout_offsets: vec![ArchiveV2VoteLockoutOffset {
                offset: seed + 1,
                confirmation_count: 2,
            }],
            hash,
            timestamp: Some(seed as i64),
        }
    }

    fn all_hot_instruction_variants() -> Vec<ArchiveV2HotInstruction> {
        let tower = ArchiveV2VoteTowerSync {
            update: vote_update(ArchiveV2VoteHashRef::Block(93), 30),
            block_id_hash: ArchiveV2VoteHashRef::Block(94),
        };
        let switch_tower = ArchiveV2VoteTowerSync {
            update: vote_update(ArchiveV2VoteHashRef::Zero, 40),
            block_id_hash: ArchiveV2VoteHashRef::Block(95),
        };
        vec![
            ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: Vec::new(),
                data: ArchiveV2HotInstructionData::Raw(vec![1, 2, 3, 4]),
            },
            ArchiveV2HotInstruction {
                program_id_index: 1,
                accounts: vec![0x31; 64 << 10],
                data: ArchiveV2HotInstructionData::UnknownSystem(vec![0x32; 32 << 10]),
            },
            ArchiveV2HotInstruction {
                program_id_index: 2,
                accounts: vec![2, 3],
                data: ArchiveV2HotInstructionData::UnknownVote(Vec::new()),
            },
            ArchiveV2HotInstruction {
                program_id_index: 3,
                accounts: vec![3, 4],
                data: ArchiveV2HotInstructionData::ComputeBudget(
                    ArchiveV2ComputeBudgetInstructionData::SetComputeUnitPrice(123_456),
                ),
            },
            ArchiveV2HotInstruction {
                program_id_index: 4,
                accounts: vec![4, 5],
                data: ArchiveV2HotInstructionData::System(
                    ArchiveV2SystemInstructionData::Transfer { lamports: 789 },
                ),
            },
            ArchiveV2HotInstruction {
                program_id_index: 5,
                accounts: vec![5, 6],
                data: ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(vote_update(
                    ArchiveV2VoteHashRef::Block(91),
                    10,
                )),
            },
            ArchiveV2HotInstruction {
                program_id_index: 6,
                accounts: vec![6, 7],
                data: ArchiveV2HotInstructionData::VoteCompactUpdateVoteStateSwitch {
                    update: vote_update(ArchiveV2VoteHashRef::Block(92), 20),
                    // The existing access format deliberately does not include switch proofs.
                    switch_proof_hash: ArchiveV2VoteHashRef::Block(900),
                },
            },
            ArchiveV2HotInstruction {
                program_id_index: 7,
                accounts: vec![7, 8],
                data: ArchiveV2HotInstructionData::VoteTowerSync(tower),
            },
            ArchiveV2HotInstruction {
                program_id_index: 8,
                accounts: vec![8, 9],
                data: ArchiveV2HotInstructionData::VoteTowerSyncSwitch {
                    tower: switch_tower,
                    // The existing access format deliberately does not include switch proofs.
                    switch_proof_hash: ArchiveV2VoteHashRef::Block(901),
                },
            },
        ]
    }

    fn hot_message_wire_fixtures() -> Vec<ArchiveV2HotMessagePayload> {
        let header = CompactMessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 2,
            num_readonly_unsigned_accounts: 3,
        };
        let boundary_keys = || {
            vec![
                CompactPubkey::id(127),
                CompactPubkey::id(128),
                CompactPubkey::id(16_383),
                CompactPubkey::id(16_384),
                CompactPubkey::raw([0x55; 32]),
            ]
        };
        vec![
            ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                header,
                account_keys: boundary_keys(),
                recent_blockhash: OwnedCompactRecentBlockhash::Id(-17),
                instructions: all_hot_instruction_variants(),
            }),
            ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
                header,
                account_keys: boundary_keys(),
                recent_blockhash: OwnedCompactRecentBlockhash::Nonce([0x66; 32]),
                instructions: all_hot_instruction_variants(),
                address_table_lookups: vec![
                    OwnedCompactAddressTableLookup {
                        account_key: CompactPubkey::id(127),
                        writable_indexes: Vec::new(),
                        readonly_indexes: Vec::new(),
                    },
                    OwnedCompactAddressTableLookup {
                        account_key: CompactPubkey::id(16_384),
                        writable_indexes: vec![0x71; 64 << 10],
                        readonly_indexes: vec![0x72; 64 << 10],
                    },
                ],
            }),
        ]
    }

    fn visit_owned_message_pubkeys_for_test(
        message: &mut ArchiveV2HotMessagePayload,
        visit: &mut impl FnMut(&mut CompactPubkey, ReferenceClass) -> Result<()>,
    ) -> Result<()> {
        match message {
            ArchiveV2HotMessagePayload::Legacy(message) => {
                for key in &mut message.account_keys {
                    visit(key, ReferenceClass::Eligible)?;
                }
            }
            ArchiveV2HotMessagePayload::V1(message) => {
                for key in &mut message.account_keys {
                    visit(key, ReferenceClass::Eligible)?;
                }
            }
            ArchiveV2HotMessagePayload::V0(message) => {
                for key in &mut message.account_keys {
                    visit(key, ReferenceClass::Eligible)?;
                }
                for lookup in &mut message.address_table_lookups {
                    visit(&mut lookup.account_key, ReferenceClass::Eligible)?;
                }
            }
        }
        Ok(())
    }

    type TestPubkeyRemap = fn(&mut CompactPubkey, ReferenceClass) -> Result<()>;

    fn rewrite_owned_message_for_test(bytes: &[u8], mut remap: TestPubkeyRemap) -> Vec<u8> {
        let mut message: ArchiveV2HotMessagePayload = wincode::config::deserialize_exact(
            bytes,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap();
        visit_owned_message_pubkeys_for_test(&mut message, &mut remap).unwrap();
        wincode::config::serialize(&message, wincode_leb128_config()).unwrap()
    }

    fn rewrite_borrowed_message_for_test(bytes: &[u8], mut remap: TestPubkeyRemap) -> Vec<u8> {
        let mut message: BorrowedHotMessagePayload<'_> = wincode::config::deserialize_exact(
            bytes,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap();
        visit_borrowed_message_pubkeys(&mut message, &mut remap).unwrap();
        wincode::config::serialize(&message, wincode_leb128_config()).unwrap()
    }

    fn boundary_id_remap(key: &mut CompactPubkey, class: ReferenceClass) -> Result<()> {
        ensure!(class == ReferenceClass::Eligible);
        if let CompactPubkey::Id(id) = key {
            *id = match *id {
                127 => 128,
                128 => 127,
                16_383 => 16_384,
                16_384 => 16_383,
                other => other,
            };
        }
        Ok(())
    }

    fn id_to_raw_remap(key: &mut CompactPubkey, class: ReferenceClass) -> Result<()> {
        ensure!(class == ReferenceClass::Eligible);
        if let CompactPubkey::Id(id) = *key {
            let mut raw = [0xa5; 32];
            raw[..4].copy_from_slice(&id.to_le_bytes());
            *key = CompactPubkey::raw(raw);
        }
        Ok(())
    }

    fn assert_slice_borrowed_from(source: &[u8], borrowed: &[u8]) {
        if borrowed.is_empty() {
            return;
        }
        let source_start = source.as_ptr() as usize;
        let source_end = source_start.checked_add(source.len()).unwrap();
        let borrowed_start = borrowed.as_ptr() as usize;
        let borrowed_end = borrowed_start.checked_add(borrowed.len()).unwrap();
        assert!(borrowed_start >= source_start);
        assert!(borrowed_end <= source_end);
    }

    fn assert_instruction_regions_are_borrowed(
        source: &[u8],
        instructions: &[BorrowedHotInstruction<'_>],
    ) {
        assert_eq!(instructions.len(), 9);
        for instruction in instructions {
            assert_slice_borrowed_from(source, instruction.accounts);
            match &instruction.data {
                BorrowedHotInstructionData::Raw(bytes)
                | BorrowedHotInstructionData::UnknownSystem(bytes)
                | BorrowedHotInstructionData::UnknownVote(bytes) => {
                    assert_slice_borrowed_from(source, bytes);
                }
                BorrowedHotInstructionData::ComputeBudget(_)
                | BorrowedHotInstructionData::System(_)
                | BorrowedHotInstructionData::VoteCompactUpdateVoteState(_)
                | BorrowedHotInstructionData::VoteCompactUpdateVoteStateSwitch { .. }
                | BorrowedHotInstructionData::VoteTowerSync(_)
                | BorrowedHotInstructionData::VoteTowerSyncSwitch { .. } => {}
            }
        }
    }

    #[test]
    fn borrowed_hot_messages_are_wire_exact_and_borrow_large_byte_regions() {
        for fixture in hot_message_wire_fixtures() {
            let source = wincode::config::serialize(&fixture, wincode_leb128_config()).unwrap();
            let borrowed: BorrowedHotMessagePayload<'_> = wincode::config::deserialize_exact(
                &source,
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            )
            .unwrap();
            match &borrowed {
                BorrowedHotMessagePayload::Legacy(message) => {
                    assert_instruction_regions_are_borrowed(&source, &message.instructions);
                }
                BorrowedHotMessagePayload::V0(message) => {
                    assert_instruction_regions_are_borrowed(&source, &message.instructions);
                    assert_eq!(message.address_table_lookups.len(), 2);
                    for lookup in &message.address_table_lookups {
                        assert_slice_borrowed_from(&source, lookup.writable_indexes);
                        assert_slice_borrowed_from(&source, lookup.readonly_indexes);
                    }
                }
            }
            let reencoded = wincode::config::serialize(&borrowed, wincode_leb128_config()).unwrap();
            assert_eq!(reencoded, source);
        }
    }

    #[test]
    fn borrowed_hot_message_remap_matches_owned_at_leb_boundaries_and_raw() {
        for fixture in hot_message_wire_fixtures() {
            let source = wincode::config::serialize(&fixture, wincode_leb128_config()).unwrap();
            let owned_boundary = rewrite_owned_message_for_test(&source, boundary_id_remap);
            let borrowed_boundary = rewrite_borrowed_message_for_test(&source, boundary_id_remap);
            assert_eq!(borrowed_boundary, owned_boundary);
            assert_ne!(borrowed_boundary, source);

            let owned_raw = rewrite_owned_message_for_test(&source, id_to_raw_remap);
            let borrowed_raw = rewrite_borrowed_message_for_test(&source, id_to_raw_remap);
            assert_eq!(borrowed_raw, owned_raw);
            assert_ne!(borrowed_raw, source);
        }
    }

    #[test]
    fn borrowed_hot_message_non_pubkey_access_matches_owned_path() {
        for fixture in hot_message_wire_fixtures() {
            let source = wincode::config::serialize(&fixture, wincode_leb128_config()).unwrap();
            let borrowed: BorrowedHotMessagePayload<'_> = wincode::config::deserialize_exact(
                &source,
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            )
            .unwrap();
            let owned: ArchiveV2HotMessagePayload = wincode::config::deserialize_exact(
                &source,
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            )
            .unwrap();

            let mut actual = AccessReferenceSet::default();
            collect_access_message_non_pubkey_refs(&borrowed, &mut actual);

            let mut expected_blockhashes = Vec::new();
            let recent_blockhash = match &owned {
                ArchiveV2HotMessagePayload::Legacy(message) => &message.recent_blockhash,
                ArchiveV2HotMessagePayload::V0(message) => &message.recent_blockhash,
                ArchiveV2HotMessagePayload::V1(message) => &message.recent_blockhash,
            };
            super::super::collect_access_recent_blockhash_id(
                recent_blockhash,
                &mut expected_blockhashes,
            );
            let mut expected_vote_hashes = Vec::new();
            super::super::collect_access_message_vote_hash_refs(&owned, &mut expected_vote_hashes);

            assert_eq!(actual.blockhash_ids, expected_blockhashes);
            assert_eq!(actual.vote_hash_block_ids, expected_vote_hashes);
            assert_eq!(actual.vote_hash_block_ids, vec![91, 92, 93, 94, 95]);
            assert!(actual.pubkey_ids.is_empty());
        }
    }

    #[test]
    fn borrowed_hot_message_decode_rejects_malformed_trailing_and_allocation_limit() {
        let source = wincode::config::serialize(
            hot_message_wire_fixtures().first().unwrap(),
            wincode_leb128_config(),
        )
        .unwrap();

        let mut invalid_tag = source.clone();
        invalid_tag[0] = 2;
        assert!(
            wincode::config::deserialize_exact::<BorrowedHotMessagePayload<'_>, _>(
                &invalid_tag,
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            )
            .is_err()
        );

        assert!(
            wincode::config::deserialize_exact::<BorrowedHotMessagePayload<'_>, _>(
                &source[..source.len() - 1],
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            )
            .is_err()
        );

        let mut trailing = source.clone();
        trailing.push(0xff);
        assert!(
            wincode::config::deserialize_exact::<BorrowedHotMessagePayload<'_>, _>(
                &trailing,
                bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
            )
            .is_err()
        );

        assert!(
            wincode::config::deserialize_exact::<BorrowedHotMessagePayload<'_>, _>(
                &source,
                bounded_wincode_config::<64>(),
            )
            .is_err()
        );
    }

    #[test]
    fn retained_remap_does_not_resolve_source_key_per_reference() {
        let calls = Cell::new(0usize);
        let mut key = CompactPubkey::id(1);
        let old_id = remap_source_pubkey(
            &mut key,
            ReferenceClass::Eligible,
            &[7],
            |_| {
                calls.set(calls.get() + 1);
                Ok([0xa5; 32])
            },
            0,
        )
        .unwrap();
        assert_eq!(old_id, 1);
        assert_eq!(key, CompactPubkey::id(7));
        assert_eq!(calls.get(), 0);
    }

    #[test]
    fn excluded_only_remap_resolves_exact_raw_key() {
        let raw = [0x5a; 32];
        let calls = Cell::new(0usize);
        let mut key = CompactPubkey::id(2);
        let old_id = remap_source_pubkey(
            &mut key,
            ReferenceClass::Excluded,
            &[9, 0],
            |id| {
                calls.set(calls.get() + 1);
                ensure!(id == 2);
                Ok(raw)
            },
            3,
        )
        .unwrap();
        assert_eq!(old_id, 2);
        assert_eq!(key, CompactPubkey::raw(raw));
        assert_eq!(calls.get(), 1);
    }

    #[test]
    fn remap_rejects_reserved_missing_and_eligible_zero_mappings() {
        let resolver = |_| -> Result<[u8; 32]> { panic!("invalid mappings must not resolve") };
        let mut reserved = CompactPubkey::Id(0);
        assert!(
            remap_source_pubkey(&mut reserved, ReferenceClass::Eligible, &[1], resolver, 0,)
                .is_err()
        );
        let mut missing = CompactPubkey::id(2);
        assert!(
            remap_source_pubkey(&mut missing, ReferenceClass::Eligible, &[1], resolver, 0,)
                .is_err()
        );
        let mut excluded = CompactPubkey::id(1);
        assert!(
            remap_source_pubkey(&mut excluded, ReferenceClass::Eligible, &[0], resolver, 0,)
                .is_err()
        );
    }

    #[test]
    fn registry_remap_rejects_wrong_target_key() {
        let root = TestDir::new();
        let source_path = root.0.join("source-registry.bin");
        let target_path = root.0.join("target-registry.bin");
        fs::write(&source_path, [[1; 32], [2; 32]].concat()).unwrap();
        fs::write(&target_path, [[2; 32], [1; 32]].concat()).unwrap();
        let source = MappedRegistry::open(&source_path).unwrap();
        let target = MappedRegistry::open(&target_path).unwrap();
        assert!(validate_registry_remap(&source, &[1, 2], &target).is_err());
    }

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let id = NEXT_TEST_DIR.fetch_add(1, AtomicOrdering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "blockzilla-registry-reprocess-{}-{id}",
                std::process::id()
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn empty_current_meta(err: Option<CompactTransactionError>) -> CompactMetaV1 {
        CompactMetaV1 {
            err,
            fee: 5_000,
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

    fn empty_legacy_meta(err: Option<Vec<u8>>) -> LegacyCompactMetaV1 {
        LegacyCompactMetaV1 {
            err,
            fee: 5_000,
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

    fn logs_with_excluded_ids(ids: &[u32]) -> CompactLogStream {
        CompactLogStream {
            events: ids
                .iter()
                .copied()
                .map(|id| LogEvent::Invoke {
                    program: CompactPubkey::id(id),
                    depth: 1,
                })
                .collect(),
            strings: Default::default(),
            data: Default::default(),
        }
    }

    fn key_for_log_fixture(index: u32) -> CompactPubkey {
        const IDS: [u32; 6] = [1, 127, 128, 16_383, 16_384, 70_000];
        CompactPubkey::id(IDS[index as usize % IDS.len()])
    }

    fn all_key_bearing_log_variants() -> Vec<LogEvent> {
        let mut next = 0u32;
        let mut key = || {
            let value = key_for_log_fixture(next);
            next += 1;
            value
        };
        let pubkey_value = |value| PubkeyOrString::Pubkey(value);
        vec![
            LogEvent::LoaderUpgradedProgram { program: key() },
            LogEvent::LoaderFinalizedAccount { account: key() },
            LogEvent::Invoke {
                program: key(),
                depth: 1,
            },
            LogEvent::BpfInvoke { program: key() },
            LogEvent::Consumed {
                program: key(),
                used: 10,
                limit: 20,
            },
            LogEvent::Success { program: key() },
            LogEvent::BpfSuccess { program: key() },
            LogEvent::Failure {
                program: key(),
                reason: 0,
            },
            LogEvent::BpfFailure {
                program: key(),
                reason: 0,
            },
            LogEvent::FailureCustomProgramError {
                program: key(),
                code: 1,
            },
            LogEvent::BpfFailureCustomProgramError {
                program: key(),
                code: 2,
            },
            LogEvent::FailureInvalidAccountData { program: key() },
            LogEvent::BpfFailureInvalidAccountData { program: key() },
            LogEvent::FailureInvalidProgramArgument { program: key() },
            LogEvent::BpfFailureInvalidProgramArgument { program: key() },
            LogEvent::Return {
                program: key(),
                data: 0,
            },
            LogEvent::ProgramNotDeployed {
                program: Some(key()),
            },
            LogEvent::ProgramNotCached {
                program: Some(key()),
            },
            LogEvent::RuntimeWritablePrivilegeEscalated { account: key() },
            LogEvent::RuntimeSignerPrivilegeEscalated { account: key() },
            LogEvent::RuntimeAccountOwnerBalanceVerificationFailed { account: key() },
            LogEvent::System(SystemProgramLog::CreateAddressMismatch {
                provided_addr: key(),
                derived_addr: pubkey_value(key()),
            }),
            LogEvent::System(SystemProgramLog::TransferFromAddressMismatch {
                provided_addr: key(),
                derived_addr: pubkey_value(key()),
            }),
            LogEvent::System(SystemProgramLog::CreateAccountAlreadyInUse {
                addr: SystemAddress::Pubkey(pubkey_value(key())),
            }),
            LogEvent::System(SystemProgramLog::AllocateAlreadyInUse {
                addr: SystemAddress::Debug {
                    address: pubkey_value(key()),
                    base: Some(pubkey_value(key())),
                },
            }),
            LogEvent::System(SystemProgramLog::AllocateToMustSign {
                addr: SystemAddress::Pubkey(PubkeyOrString::Text(0)),
            }),
            LogEvent::System(SystemProgramLog::AllocateAccountAlreadyInUse {
                addr: SystemAddress::Pubkey(pubkey_value(key())),
            }),
            LogEvent::System(SystemProgramLog::AssignAccountMustSign {
                addr: SystemAddress::Debug {
                    address: pubkey_value(key()),
                    base: None,
                },
            }),
            LogEvent::System(SystemProgramLog::CreateAccountAccountAlreadyInUse {
                addr: SystemAddress::Pubkey(pubkey_value(key())),
            }),
            LogEvent::System(SystemProgramLog::TransferFromMustSign { from: key() }),
            LogEvent::System(SystemProgramLog::NonceAccountMustBeWriteable {
                action: NonceAction::Advance,
                account: pubkey_value(key()),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountMustBeSigner {
                action: NonceAction::Withdraw,
                account: pubkey_value(key()),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountMustSign {
                action: NonceAction::Initialize,
                account: pubkey_value(key()),
            }),
            LogEvent::System(SystemProgramLog::NonceAccountStateInvalid {
                action: NonceAction::Authorize,
                account: pubkey_value(key()),
            }),
            LogEvent::ProgramLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom {
                account_key: key(),
                error: 0,
            })),
            LogEvent::ProgramPlainLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom2 {
                account_key: key(),
                error: 0,
            })),
            LogEvent::ProgramIdLog {
                program: key(),
                log: ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom3 {
                    account_key: key(),
                    error: 0,
                }),
            },
            LogEvent::ProgramLog(ProgramLog::Token2022(Token2022Log::ErrorHarvestingFrom4 {
                account_key: key(),
                error: 0,
            })),
        ]
    }

    fn fully_populated_success_metadata() -> CompactMetaV1 {
        let mut metadata = empty_current_meta(None);
        metadata.fee = 16_384;
        metadata.pre_balances = vec![0, 127, 128, 16_383, 16_384, u64::MAX];
        metadata.post_balances = vec![u64::MAX, 16_384, 16_383, 128, 127, 0];
        metadata.inner_instructions = Some(vec![CompactInnerInstructions {
            index: 128,
            instructions: vec![
                CompactInnerInstruction {
                    program_id_index: 127,
                    accounts: Vec::new(),
                    data: Vec::new(),
                    stack_height: None,
                },
                CompactInnerInstruction {
                    program_id_index: 16_384,
                    accounts: vec![0x81; 64 << 10],
                    data: vec![0x82; 64 << 10],
                    stack_height: Some(16_384),
                },
            ],
        }]);
        metadata.logs = Some(CompactLogStream {
            events: all_key_bearing_log_variants(),
            strings: StringTable {
                lengths: vec![64 << 10],
                bytes: vec![b'a'; 64 << 10],
            },
            data: DataTable {
                arrays: vec![DataArray { chunk_count: 1 }],
                chunk_lengths: vec![64 << 10],
                bytes: vec![0x83; 64 << 10],
            },
        });
        metadata.pre_token_balances = vec![CompactTokenBalance {
            account_index: 127,
            mint: Some(CompactPubkey::id(127)),
            owner: Some(CompactPubkey::id(128)),
            program_id: Some(CompactPubkey::id(16_383)),
            amount: u64::MAX,
            decimals: 9,
        }];
        metadata.post_token_balances = vec![CompactTokenBalance {
            account_index: 128,
            mint: Some(CompactPubkey::id(16_384)),
            owner: None,
            program_id: Some(CompactPubkey::raw([0x84; 32])),
            amount: 128,
            decimals: 6,
        }];
        metadata.rewards = vec![CompactReward {
            pubkey: CompactPubkey::id(70_000),
            lamports: -128,
            post_balance: 16_384,
            reward_type: -1,
            commission: Some(5),
        }];
        metadata.loaded_writable_addresses = vec![
            CompactPubkey::id(127),
            CompactPubkey::id(128),
            CompactPubkey::raw([0x85; 32]),
        ];
        metadata.loaded_readonly_addresses =
            vec![CompactPubkey::id(16_383), CompactPubkey::id(16_384)];
        metadata.return_data = Some(CompactReturnData {
            program_id: CompactPubkey::id(70_000),
            data: vec![0x86; 64 << 10],
        });
        metadata.compute_units_consumed = Some(16_384);
        metadata.cost_units = Some(u64::MAX);
        metadata
    }

    fn metadata_boundary_remap(key: &mut CompactPubkey, _class: ReferenceClass) -> Result<()> {
        if let CompactPubkey::Id(id) = key {
            *id = match *id {
                127 => 128,
                128 => 127,
                16_383 => 16_384,
                16_384 => 16_383,
                70_000 => 1,
                other => other,
            };
        }
        Ok(())
    }

    fn metadata_id_to_raw_remap(key: &mut CompactPubkey, _class: ReferenceClass) -> Result<()> {
        if let CompactPubkey::Id(id) = *key {
            let mut raw = [0x5a; 32];
            raw[..4].copy_from_slice(&id.to_le_bytes());
            *key = CompactPubkey::raw(raw);
        }
        Ok(())
    }

    fn rewrite_owned_metadata_for_test(bytes: &[u8], mut remap: TestPubkeyRemap) -> Vec<u8> {
        let mut metadata = decode_compact_metadata(bytes).unwrap();
        visit_metadata_pubkeys(&mut metadata, &mut remap).unwrap();
        wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap()
    }

    fn rewrite_rewritable_metadata_for_test(bytes: &[u8], mut remap: TestPubkeyRemap) -> Vec<u8> {
        let mut metadata = decode_rewritable_compact_metadata(bytes).unwrap();
        metadata.visit_pubkeys(&mut remap).unwrap();
        let expected_size = metadata.serialized_size().unwrap();
        let mut output = Vec::with_capacity(expected_size);
        metadata.serialize_into(&mut output).unwrap();
        assert_eq!(output.len(), expected_size);
        output
    }

    fn assert_success_metadata_regions_are_borrowed(
        source: &[u8],
        metadata: &BorrowedCompactMetaV1<'_>,
    ) {
        let groups = metadata.inner_instructions.as_ref().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].instructions.len(), 2);
        for instruction in &groups[0].instructions {
            assert_slice_borrowed_from(source, instruction.accounts);
            assert_slice_borrowed_from(source, instruction.data);
        }
        let logs = metadata.logs.as_ref().unwrap();
        assert_slice_borrowed_from(source, logs.strings.bytes);
        assert_slice_borrowed_from(source, logs.data.bytes);
        assert_slice_borrowed_from(source, metadata.return_data.as_ref().unwrap().data);
    }

    #[test]
    fn borrowed_success_metadata_is_wire_exact_and_borrows_only_byte_payloads() {
        let source = wincode::config::serialize(
            &fully_populated_success_metadata(),
            wincode_leb128_config(),
        )
        .unwrap();
        assert_eq!(source.first(), Some(&0));
        let metadata = decode_successful_borrowed_metadata_with_limit::<
            MAX_HOT_BLOCK_FRAME_BYTES_USIZE,
        >(&source)
        .unwrap();
        assert_success_metadata_regions_are_borrowed(&source, &metadata);
        let reencoded = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        assert_eq!(reencoded, source);
    }

    #[test]
    fn borrowed_success_metadata_remap_matches_owned_for_all_key_locations() {
        let source = wincode::config::serialize(
            &fully_populated_success_metadata(),
            wincode_leb128_config(),
        )
        .unwrap();
        let owned_boundary = rewrite_owned_metadata_for_test(&source, metadata_boundary_remap);
        let borrowed_boundary =
            rewrite_rewritable_metadata_for_test(&source, metadata_boundary_remap);
        assert_eq!(borrowed_boundary, owned_boundary);
        assert_ne!(borrowed_boundary, source);

        let owned_raw = rewrite_owned_metadata_for_test(&source, metadata_id_to_raw_remap);
        let borrowed_raw = rewrite_rewritable_metadata_for_test(&source, metadata_id_to_raw_remap);
        assert_eq!(borrowed_raw, owned_raw);
        assert_ne!(borrowed_raw, source);
    }

    #[test]
    fn borrowed_success_metadata_rejects_overlong_leb_like_owned_path() {
        let mut metadata = empty_current_meta(None);
        metadata.fee = 0;
        let canonical = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        assert_eq!(&canonical[..2], &[0, 0]);
        let mut overlong = canonical.clone();
        overlong[1] = 0x80;
        overlong.insert(2, 0);

        assert!(decode_compact_metadata(&overlong).is_err());
        assert!(decode_rewritable_compact_metadata(&overlong).is_err());

        let mut metadata = empty_current_meta(None);
        metadata.return_data = Some(CompactReturnData {
            program_id: CompactPubkey::raw([0x33; 32]),
            data: vec![0x42],
        });
        let canonical = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let decoded = decode_successful_borrowed_metadata_with_limit::<
            MAX_HOT_BLOCK_FRAME_BYTES_USIZE,
        >(&canonical)
        .unwrap();
        let data = decoded.return_data.as_ref().unwrap().data;
        assert_eq!(data, [0x42]);
        let data_offset = (data.as_ptr() as usize)
            .checked_sub(canonical.as_ptr() as usize)
            .unwrap();
        assert_eq!(canonical[data_offset - 1], 1);
        let mut overlong_length = canonical.clone();
        overlong_length[data_offset - 1] = 0x81;
        overlong_length.insert(data_offset, 0);

        assert!(decode_compact_metadata(&overlong_length).is_err());
        assert!(decode_rewritable_compact_metadata(&overlong_length).is_err());
    }

    #[test]
    fn borrowed_success_metadata_rejects_malformed_trailing_and_hostile_allocation_limit() {
        let source = wincode::config::serialize(
            &fully_populated_success_metadata(),
            wincode_leb128_config(),
        )
        .unwrap();
        assert!(
            decode_successful_borrowed_metadata_with_limit::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(
                &source[..source.len() - 1],
            )
            .is_err()
        );
        let mut trailing = source.clone();
        trailing.push(0xff);
        assert!(
            decode_successful_borrowed_metadata_with_limit::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(
                &trailing,
            )
            .is_err()
        );
        let mut present_error_tag = source.clone();
        present_error_tag[0] = 1;
        assert!(
            decode_successful_borrowed_metadata_with_limit::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(
                &present_error_tag,
            )
            .is_err()
        );
        assert!(decode_successful_borrowed_metadata_with_limit::<64>(&source).is_err());
    }

    #[test]
    fn rewritable_metadata_keeps_current_and_legacy_error_fallback_owned() {
        let mut current_metadata = empty_current_meta(Some(CompactTransactionError::AccountInUse));
        current_metadata.loaded_writable_addresses = vec![CompactPubkey::id(127)];
        let current =
            wincode::config::serialize(&current_metadata, wincode_leb128_config()).unwrap();
        assert!(matches!(
            decode_rewritable_compact_metadata(&current).unwrap(),
            RewritableCompactMetaV1::Owned(_)
        ));
        assert_eq!(
            rewrite_rewritable_metadata_for_test(&current, metadata_boundary_remap),
            rewrite_owned_metadata_for_test(&current, metadata_boundary_remap)
        );

        let stored = wincode::serialize(&StoredTransactionError::AccountInUse).unwrap();
        let mut legacy_metadata = empty_legacy_meta(Some(stored));
        legacy_metadata.loaded_readonly_addresses = vec![CompactPubkey::id(128)];
        let legacy = wincode::config::serialize(&legacy_metadata, wincode_leb128_config()).unwrap();
        assert!(matches!(
            decode_rewritable_compact_metadata(&legacy).unwrap(),
            RewritableCompactMetaV1::Owned(_)
        ));
        assert_eq!(
            rewrite_rewritable_metadata_for_test(&legacy, metadata_boundary_remap),
            rewrite_owned_metadata_for_test(&legacy, metadata_boundary_remap)
        );
    }

    fn full_metadata_excluded_ids(bytes: &[u8]) -> Result<Vec<u32>> {
        let mut metadata = decode_compact_metadata(bytes)?;
        let mut ids = Vec::new();
        if let Some(logs) = &mut metadata.logs {
            visit_log_pubkeys(logs, &mut |key, class| {
                ensure!(class == ReferenceClass::Excluded);
                let CompactPubkey::Id(id) = *key else {
                    bail!("test expected an ID reference")
                };
                ids.push(id);
                Ok(())
            })?;
        }
        Ok(ids)
    }

    fn selective_metadata_excluded_ids(bytes: &[u8]) -> Result<Vec<u32>> {
        let mut ids = Vec::new();
        scan_metadata_excluded_pubkeys(bytes, &mut |key, class| {
            ensure!(class == ReferenceClass::Excluded);
            let CompactPubkey::Id(id) = *key else {
                bail!("test expected an ID reference")
            };
            ids.push(id);
            Ok(())
        })?;
        Ok(ids)
    }

    #[test]
    fn no_error_metadata_fast_path_is_exact_and_rejects_trailing_bytes() {
        let mut metadata = empty_current_meta(None);
        metadata.loaded_writable_addresses = vec![CompactPubkey::id(3)];
        let mut encoded = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        assert_eq!(encoded.first(), Some(&0));
        let decoded = decode_compact_metadata(&encoded).unwrap();
        assert!(decoded.err.is_none());
        assert_eq!(decoded.fee, metadata.fee);
        assert_eq!(decoded.loaded_writable_addresses.len(), 1);
        encoded.push(0xa5);
        assert!(decode_compact_metadata(&encoded).is_err());
    }

    fn compressed_test_input(
        encoded: Vec<u8>,
        tx_count: u32,
        signature_count: u32,
    ) -> CompressedBlockInput {
        let compressed = zstd::bulk::compress(&encoded, 1).unwrap();
        CompressedBlockInput {
            row: blockzilla_format::ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot: crate::SLOTS_PER_EPOCH,
                compressed_offset: 0,
                compressed_len: compressed.len() as u32,
                uncompressed_len: encoded.len() as u32,
                tx_count,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count,
            },
            bytes: compressed,
            signatures: None,
        }
    }

    #[test]
    fn reused_zstd_contexts_match_fresh_bytes_and_decode() {
        let inputs = [
            Vec::new(),
            vec![0x5a; 4 << 10],
            (0..128 << 10)
                .map(|index| ((index * 31 + index / 97) & 0xff) as u8)
                .collect(),
        ];
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        let mut decompressor = zstd::bulk::Decompressor::new().unwrap();
        let mut compressed = Vec::new();
        let mut decoded = Vec::new();

        for source in inputs {
            let fresh = zstd::bulk::compress(&source, 3).unwrap();
            compress_zstd_reused(&mut compressor, &source, &mut compressed).unwrap();
            assert_eq!(compressed, fresh);

            decompress_zstd_reused(&mut decompressor, &compressed, source.len(), &mut decoded)
                .unwrap();
            assert_eq!(decoded, source);
        }
    }

    #[test]
    fn worker_scratch_reuses_normal_capacity_and_drops_outliers() {
        assert_eq!(
            registry_rewrite_pipeline_memory_budget(6).unwrap(),
            440 << 20
        );

        let mut scratch = RegistryRewriteWorkerScratch::new(1).unwrap();
        scratch.decoded.try_reserve_exact(4 << 10).unwrap();
        scratch.decoded.extend_from_slice(&[0xa5; 64]);
        let decoded_capacity = scratch.decoded.capacity();
        let decoded_pointer = scratch.decoded.as_ptr();
        assert!(!scratch.finish_block());
        assert!(scratch.decoded.is_empty());
        assert_eq!(scratch.decoded.capacity(), decoded_capacity);
        assert_eq!(scratch.decoded.as_ptr(), decoded_pointer);

        scratch
            .encoded
            .try_reserve_exact(REGISTRY_REWRITE_WORKER_RETAINED_VECTOR_BYTES + 1)
            .unwrap();
        assert!(scratch.finish_block());
        assert_eq!(scratch.retained_vector_bytes(), 0);
        assert_eq!(scratch.decoded.capacity(), 0);
        assert_eq!(scratch.encoded.capacity(), 0);
        assert_eq!(scratch.compressed.capacity(), 0);
        assert_eq!(scratch.rows.capacity(), 0);
        assert_eq!(scratch.target_messages.capacity(), 0);
        assert_eq!(scratch.target_metadata.capacity(), 0);
    }

    fn write_minimal_first_seen_source(source: &Path, epoch: u64) -> [u8; 32] {
        fs::create_dir(source).unwrap();
        fs::write(
            source.join(wire_profile_marker(TEST_WIRE_PROFILE).name),
            wire_profile_marker_bytes(TEST_WIRE_PROFILE),
        )
        .unwrap();
        let retained = compute_budget_key();
        let excluded = [0xe7; 32];
        fs::write(
            source.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            [retained, excluded].concat(),
        )
        .unwrap();
        let mut counts = Vec::new();
        write_u32_varint(&mut counts, 1).unwrap();
        write_u32_varint(&mut counts, 1).unwrap();
        fs::write(source.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE), counts).unwrap();
        build_registry_index(source).unwrap();
        fs::write(
            source.join(ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE),
            b"version=1\nregistry_order=first_seen_v1\ncount_semantics=all_compact_pubkey_refs_v1\nregistry_keys=2\nreferences=2\n",
        )
        .unwrap();

        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(0),
            instructions: Vec::new(),
        });
        let message_bytes = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let slot = epoch * crate::SLOTS_PER_EPOCH;
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot,
                parent_slot: slot - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: Some(ArchiveV2HotRewards {
                    num_partitions: None,
                    decoded: vec![CompactReward {
                        pubkey: CompactPubkey::id(2),
                        lamports: 1,
                        post_balance: 2,
                        reward_type: 0,
                        commission: None,
                    }],
                }),
            },
            tx_count: 1,
            tx_rows: vec![ArchiveV2HotTxRow {
                tx_index: 0,
                flags: 0,
                message_offset: 0,
                message_len: message_bytes.len() as u32,
                metadata_offset: 0,
                metadata_len: 0,
                signature_count: 1,
                reserved: [0; 3],
            }],
            message_bytes,
            metadata_bytes: Vec::new(),
        };
        let encoded = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let compressed = zstd::bulk::compress(&encoded, 1).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), &compressed).unwrap();
        write_archive_v2_hot_block_index(
            &source.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed.len() as u64,
            1,
            0,
            &[blockzilla_format::ArchiveV2HotBlockIndexRow {
                block_id: 0,
                slot,
                compressed_offset: 0,
                compressed_len: compressed.len() as u32,
                uncompressed_len: encoded.len() as u32,
                tx_count: 1,
                first_tx_ordinal: 0,
                first_signature_ordinal: 0,
                signature_count: 1,
            }],
        )
        .unwrap();

        let meta_file = File::create(source.join(ARCHIVE_V2_META_FILE)).unwrap();
        let mut meta = WincodeLeb128FramedWriter::new(BufWriter::new(meta_file));
        meta.write(&ArchiveV2HotMetaRecord::Header(WincodeArchiveV2Header {
            version: WINCODE_ARCHIVE_V2_HOT_BLOCK_VERSION,
            flags: WINCODE_ARCHIVE_V2_FLAG_LEB128
                | WINCODE_ARCHIVE_V2_FLAG_FIRST_SEEN_REGISTRY
                | WINCODE_ARCHIVE_V2_FLAG_ALL_PUBKEY_REF_COUNTS,
        }))
        .unwrap();
        meta.write(&ArchiveV2HotMetaRecord::Footer(WincodeArchiveV2Footer {
            blocks: 1,
            transactions: 1,
            entries: 0,
            rewards: 1,
            dataframes: 0,
            subset_nodes_ignored: 0,
            epoch_nodes_ignored: 0,
            car_entries: 0,
            car_payload_bytes: 0,
            decoded_node_payload_bytes: 0,
            tx_source_bytes: 0,
            metadata_source_bytes: 0,
            rewards_source_bytes: 0,
            tx_raw_fallbacks: 0,
            metadata_raw_fallbacks: 0,
            rewards_raw_fallbacks: 0,
            nonce_recent_blockhashes: 0,
            decode_errors: Vec::new(),
        }))
        .unwrap();
        meta.flush().unwrap();
        fs::write(source.join(ARCHIVE_V2_SIGNATURES_FILE), [0x33; 64]).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE), [0; 32]).unwrap();
        fs::write(source.join(ARCHIVE_V2_POH_FILE), [0]).unwrap();
        fs::write(source.join(ARCHIVE_V2_SHREDDING_FILE), [0]).unwrap();
        write_valid_source_access_artifacts(source, epoch, retained);
        publish_test_generation_manifest(source, epoch);
        excluded
    }

    fn publish_test_generation_manifest(source: &Path, epoch: u64) {
        publish_test_generation_manifest_for_profile(source, epoch, TEST_WIRE_PROFILE);
    }

    fn publish_test_generation_manifest_for_profile(
        source: &Path,
        epoch: u64,
        profile: ArchiveV2WireProfile,
    ) {
        let marker = wire_profile_marker(profile);
        let names = [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            marker.name.as_str(),
        ];
        let files = names
            .into_iter()
            .map(|name| {
                let binding = hash_file(&source.join(name)).unwrap();
                GenerationFile {
                    name: name.to_owned(),
                    size: binding.bytes,
                    sha256: binding.sha256,
                }
            })
            .collect();
        let mut manifest = GenerationManifest {
            schema_version: GENERATION_MANIFEST_SCHEMA_VERSION,
            cluster_id: "testnet-local".to_owned(),
            epoch,
            generation_id: format!("registry-reprocess-test-epoch-{epoch}"),
            generation_digest: "0".repeat(64),
            slots_per_epoch: crate::SLOTS_PER_EPOCH,
            complete: true,
            files,
        };
        manifest.generation_digest = compute_generation_digest(&manifest).unwrap();
        manifest.validate().unwrap();
        let path = source.join(GENERATION_MANIFEST_FILE);
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }
        let mut bytes = serde_json::to_vec_pretty(&manifest).unwrap();
        bytes.push(b'\n');
        fs::write(path, bytes).unwrap();
    }

    fn write_valid_source_access_artifacts(source: &Path, epoch: u64, retained: [u8; 32]) {
        let slot = epoch * crate::SLOTS_PER_EPOCH;
        let signature = [0x33; 64];
        let access = ArchiveV2BlockAccessBlob {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: 0,
            blockhash: [0; 32],
            previous_blockhash: [0x44; 32],
            signature_counts: vec![1],
            signatures: signature.to_vec(),
            pubkeys: vec![ArchiveV2BlockAccessPubkey {
                id: 1,
                pubkey: retained,
            }],
            blockhashes: vec![ArchiveV2BlockAccessBlockhash {
                id: 0,
                blockhash: [0; 32],
            }],
            vote_hashes: Vec::new(),
        };
        let access = wincode::config::serialize(&access, wincode_leb128_config()).unwrap();
        write_source_access_bytes(source, epoch, &access);

        let mut tail = Vec::with_capacity(40);
        tail.extend_from_slice(&[0x44; 32]);
        tail.extend_from_slice(&(slot - 1).to_le_bytes());
        fs::write(source.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE), tail).unwrap();
    }

    fn write_source_access_bytes(source: &Path, epoch: u64, access: &[u8]) {
        let slot = epoch * crate::SLOTS_PER_EPOCH;
        fs::write(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE), &access).unwrap();
        let access_rows = [ArchiveV2BlockAccessIndexRow {
            block_id: 0,
            slot,
            access_offset: 0,
            access_len: access.len() as u32,
            tx_count: 1,
            signature_count: 1,
        }];
        write_archive_v2_block_access_index(
            &source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
            access.len() as u64,
            0,
            &access_rows,
        )
        .unwrap();
        let hot =
            read_archive_v2_hot_block_index(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        let get_block = build_get_block_rows(&hot.rows, &access_rows).unwrap();
        write_archive_v2_get_block_index(&source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE), &get_block)
            .unwrap();
    }

    fn hash_from_hex(value: &str) -> [u8; 32] {
        assert_eq!(value.len(), 64);
        let mut hash = [0u8; 32];
        for (index, byte) in hash.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).unwrap();
        }
        hash
    }

    fn exact_epoch_301_boundary_evidence() -> Epoch301AccessBoundaryEvidence {
        Epoch301AccessBoundaryEvidence {
            tail_binding: FileBinding {
                bytes: EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_BYTES,
                sha256: EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_SHA256.to_owned(),
            },
            tail_rows: EPOCH_301_ACCESS_BOUNDARY_REPAIR_TAIL_ROWS,
            manifest_sha256: EPOCH_301_ACCESS_BOUNDARY_REPAIR_MANIFEST_SHA256.to_owned(),
            source_blob_bytes: EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_BYTES,
            source_index_binding: FileBinding {
                bytes: EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_BYTES,
                sha256: EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_SHA256.to_owned(),
            },
            source_index_rows: EPOCH_301_ACCESS_BOUNDARY_REPAIR_INDEX_ROWS,
            source_index_blob_bytes: EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_BYTES,
            row_0_access_len: EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_0_BYTES,
            row_0_frame_sha256: EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_0_SHA256.to_owned(),
            row_1_block_id: EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCK_ID,
            row_1_slot: EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_SLOT,
            row_1_previous_blockhash_hex: EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX.to_owned(),
            row_1_blockhash_hex: EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCKHASH_HEX.to_owned(),
            row_1_frame_sha256: EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_SHA256.to_owned(),
            first_hot_parent_slot: EPOCH_301_ACCESS_BOUNDARY_REPAIR_PREDECESSOR_SLOT,
        }
    }

    fn write_epoch_301_legacy_boundary_bug_source(source: &Path) {
        let epoch = EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH;
        write_minimal_first_seen_source(source, epoch);

        let original = hash_from_hex(EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX);
        let corrected = hash_from_hex(EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX);
        let mut hot =
            read_archive_v2_hot_block_index(&source.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        assert_eq!(hot.rows.len(), 1);
        let source_blocks = fs::read(source.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
        let mut block = decode_hot_block(&CompressedBlockInput {
            row: hot.rows[0],
            bytes: source_blocks,
            signatures: None,
        })
        .unwrap();
        block.header.slot = EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT;
        block.header.parent_slot = EPOCH_301_ACCESS_BOUNDARY_REPAIR_PREDECESSOR_SLOT;
        let encoded = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let compressed = zstd::bulk::compress(&encoded, 1).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), &compressed).unwrap();
        hot.rows[0].slot = EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT;
        hot.rows[0].compressed_len = compressed.len() as u32;
        hot.rows[0].uncompressed_len = encoded.len() as u32;
        write_archive_v2_hot_block_index(
            &source.join(ARCHIVE_V2_BLOCK_INDEX_FILE),
            compressed.len() as u64,
            1,
            0,
            &hot.rows,
        )
        .unwrap();

        fs::write(source.join(ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE), original).unwrap();
        let access_bytes = fs::read(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE)).unwrap();
        let mut access = decode_access_blob(&access_bytes, 0).unwrap();
        access.blockhash = original;
        access.previous_blockhash = original;
        access.blockhashes[0].blockhash = original;
        let access = wincode::config::serialize(&access, wincode_leb128_config()).unwrap();
        fs::write(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE), &access).unwrap();
        let access_rows = [ArchiveV2BlockAccessIndexRow {
            block_id: EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID,
            slot: EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT,
            access_offset: 0,
            access_len: access.len() as u32,
            tx_count: 1,
            signature_count: 1,
        }];
        write_archive_v2_block_access_index(
            &source.join(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE),
            access.len() as u64,
            0,
            &access_rows,
        )
        .unwrap();
        let get_block = build_get_block_rows(&hot.rows, &access_rows).unwrap();
        write_archive_v2_get_block_index(&source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE), &get_block)
            .unwrap();

        let mut tail = Vec::with_capacity(40);
        tail.extend_from_slice(&corrected);
        tail.extend_from_slice(&EPOCH_301_ACCESS_BOUNDARY_REPAIR_PREDECESSOR_SLOT.to_le_bytes());
        fs::write(source.join(ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE), tail).unwrap();
        publish_test_generation_manifest(source, epoch);
    }

    fn rewrite_source_access_as_legacy(source: &Path, epoch: u64, version: u16) {
        let bytes = fs::read(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE)).unwrap();
        let current = decode_access_blob(&bytes, 0).unwrap();
        let bytes = match version {
            1 => wincode::config::serialize(
                &LegacyBlockAccessBlobV1 {
                    version: 1,
                    blockhash: current.blockhash,
                    previous_blockhash: current.previous_blockhash,
                    signature_counts: current.signature_counts,
                    signatures: current.signatures,
                    pubkeys: current.pubkeys,
                    blockhashes: current.blockhashes,
                },
                wincode_leb128_config(),
            )
            .unwrap(),
            2 => wincode::config::serialize(
                &LegacyBlockAccessBlobV2NoVotes {
                    version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
                    flags: current.flags,
                    blockhash: current.blockhash,
                    previous_blockhash: current.previous_blockhash,
                    signature_counts: current.signature_counts,
                    signatures: current.signatures,
                    pubkeys: current.pubkeys,
                    blockhashes: current.blockhashes,
                },
                wincode_leb128_config(),
            )
            .unwrap(),
            _ => panic!("unsupported test access version"),
        };
        write_source_access_bytes(source, epoch, &bytes);
    }

    fn run_two_stage(
        source: &Path,
        target: &Path,
        epoch: u64,
        threads: usize,
    ) -> RegistryReprocessReceipt {
        publish_test_generation_manifest(source, epoch);
        let attempt_id = format!(
            "test-{}-{}",
            threads,
            NEXT_TEST_DIR.fetch_add(1, AtomicOrdering::Relaxed)
        );
        let staging = expected_staging_path(target, &attempt_id).unwrap();
        let core = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source.to_path_buf(),
            target_dir: target.to_path_buf(),
            epoch,
            threads,
            sort_memory_mib: 1,
            level: 1,
            attempt_id: attempt_id.clone(),
            staging_dir: staging.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(fs::metadata(&staging).unwrap().mode() & 0o777, 0o700);
        }
        assert_eq!(core.state, CORE_COMPLETE_STATE);
        assert!(staging.is_dir());
        assert!(!target.exists());
        assert!(!staging.join(ARCHIVE_V2_SIGNATURES_FILE).exists());
        assert!(!staging.join(ARCHIVE_V2_BLOCK_ACCESS_FILE).exists());
        assert!(!staging.join(REGISTRY_REPROCESS_RECEIPT_FILE).exists());
        complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: source.to_path_buf(),
            staging_dir: staging,
            target_dir: target.to_path_buf(),
            epoch,
            attempt_id,
            handoff_sha256: core.handoff_sha256,
            expected_continuation_state:
                RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap()
    }

    #[test]
    fn standalone_wire_profile_marker_is_not_source_authority() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 7;
        write_minimal_first_seen_source(&source, epoch);
        fs::remove_file(source.join(GENERATION_MANIFEST_FILE)).unwrap();
        let attempt_id = "standalone-marker".to_owned();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();

        let error = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source,
            target_dir: target.clone(),
            epoch,
            threads: 1,
            sort_memory_mib: 16,
            level: 1,
            attempt_id,
            staging_dir: staging.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap_err();

        assert!(
            format!("{error:#}").contains("marker is not authority by itself"),
            "unexpected authority error: {error:#}"
        );
        assert!(!target.exists());
        assert!(!staging.exists());
    }

    #[test]
    fn published_manifest_binds_the_exact_source_profile() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 7;
        write_minimal_first_seen_source(&source, epoch);

        let authority = validate_source_wire_profile_authority(
            &source,
            &target,
            epoch,
            TEST_WIRE_PROFILE,
            None,
        )
        .unwrap();
        assert!(matches!(
            authority.authority,
            SourceWireProfileAuthority::PublishedManifest { .. }
        ));

        fs::write(source.join(ARCHIVE_V2_BLOCKS_FILE), b"replaced").unwrap();
        let error = validate_source_wire_profile_authority(
            &source,
            &target,
            epoch,
            TEST_WIRE_PROFILE,
            None,
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(
            message.contains("authenticate and validate the published source generation")
                && message.contains("expected"),
            "unexpected binding error: {error:#}"
        );
    }

    #[test]
    fn published_manifest_rejects_a_dual_valid_noncanonical_profile() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 7;
        write_minimal_first_seen_source(&source, epoch);
        fs::remove_file(source.join(wire_profile_marker(TEST_WIRE_PROFILE).name)).unwrap();
        let noncanonical = ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1;
        fs::write(
            source.join(wire_profile_marker(noncanonical).name),
            wire_profile_marker_bytes(noncanonical),
        )
        .unwrap();
        publish_test_generation_manifest_for_profile(&source, epoch, noncanonical);

        let error =
            validate_source_wire_profile_authority(&source, &target, epoch, noncanonical, None)
                .unwrap_err();

        assert!(
            format!("{error:#}")
                .contains("all-equivalent generation must use the canonical post-fallback"),
            "unexpected profile audit error: {error:#}"
        );
    }

    fn synthetic_marker_free_authority(
        source: &Path,
        recovery: bool,
    ) -> ResolvedSourceWireProfileAuthority {
        let legacy_source_files = SOURCE_ANCHOR_FILES
            .into_iter()
            .map(|name| (name.to_owned(), hash_file(&source.join(name)).unwrap()))
            .collect::<BTreeMap<_, _>>();
        if recovery {
            let source_file_identities: BTreeMap<String, ProfileAuthorityFileIdentity> =
                SOURCE_ANCHOR_FILES
                    .into_iter()
                    .map(|name| {
                        (
                            name.to_owned(),
                            profile_authority_file_identity(
                                &regular_file_metadata(&source.join(name)).unwrap(),
                            ),
                        )
                    })
                    .collect();
            let receipt_identity = source_file_identities
                .get(ARCHIVE_V2_META_FILE)
                .unwrap()
                .clone();
            ResolvedSourceWireProfileAuthority {
                authority: SourceWireProfileAuthority::ProfileNeutralRecoveryReceipt {
                    path: source.join("synthetic-authority.json"),
                    receipt: FileBinding {
                        bytes: receipt_identity.size,
                        sha256: "00".repeat(32),
                    },
                    identity: receipt_identity,
                    source_generation_sha256: "11".repeat(32),
                    legacy_source_files,
                },
                marker_free_evidence: Some(
                    MarkerFreeSourceAuthorityEvidence::ProfileNeutralRecoveryReceipt {
                        source_file_identities,
                    },
                ),
            }
        } else {
            ResolvedSourceWireProfileAuthority {
                authority: SourceWireProfileAuthority::PinnedHistoricalIdentity {
                    epoch: 7,
                    blocks: legacy_source_files[ARCHIVE_V2_BLOCKS_FILE].clone(),
                    block_index: legacy_source_files[ARCHIVE_V2_BLOCK_INDEX_FILE].clone(),
                },
                marker_free_evidence: Some(
                    MarkerFreeSourceAuthorityEvidence::PinnedHistoricalIdentity,
                ),
            }
        }
    }

    #[test]
    fn marker_free_audit_snapshot_rejects_a_hostile_generation_replacement() {
        for recovery in [false, true] {
            let root = TestDir::new();
            let source = root.0.join("source");
            let replacement = root.0.join("replacement");
            let displaced = root.0.join("displaced");
            let target = root.0.join("target");
            let staging = root.0.join("staging");
            write_minimal_first_seen_source(&source, 7);
            fs::remove_file(source.join(GENERATION_MANIFEST_FILE)).unwrap();
            fs::remove_file(source.join(wire_profile_marker(TEST_WIRE_PROFILE).name)).unwrap();
            fs::create_dir(&replacement).unwrap();
            for entry in fs::read_dir(&source).unwrap() {
                let entry = entry.unwrap();
                assert!(entry.file_type().unwrap().is_file());
                fs::copy(entry.path(), replacement.join(entry.file_name())).unwrap();
            }
            let mut hostile_blocks = fs::read(replacement.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap();
            let last = hostile_blocks.last_mut().unwrap();
            *last ^= 1;
            fs::write(replacement.join(ARCHIVE_V2_BLOCKS_FILE), hostile_blocks).unwrap();

            let resolved = synthetic_marker_free_authority(&source, recovery);
            let audited =
                audit_admitted_source_wire_profile(&source, 7, TEST_WIRE_PROFILE, &resolved)
                    .unwrap()
                    .unwrap();
            let options = RegistryReprocessOptions {
                source_dir: source.clone(),
                target_dir: target.clone(),
                epoch: 7,
                threads: 1,
                sort_memory_mib: 16,
                level: 1,
                attempt_id: "hostile-replacement".to_owned(),
                staging_dir: staging.clone(),
                wire_profile: TEST_WIRE_PROFILE,
                wire_profile_authority_receipt: None,
            };
            build_checkpoint_with_authority(
                &source,
                &target,
                &staging,
                &options,
                resolved.authority.clone(),
                Some(&audited),
            )
            .unwrap();

            fs::rename(&source, &displaced).unwrap();
            fs::rename(&replacement, &source).unwrap();
            let error = build_checkpoint_with_authority(
                &source,
                &target,
                &staging,
                &options,
                resolved.authority,
                Some(&audited),
            )
            .unwrap_err();
            assert!(
                format!("{error:#}").contains(
                    "marker-free source changed between its audit and initial checkpoint"
                ),
                "unexpected descriptor replacement error: {error:#}"
            );
        }
    }

    #[test]
    fn metadata_decoder_accepts_current_account_in_use() {
        let bytes = wincode::config::serialize(
            &empty_current_meta(Some(CompactTransactionError::AccountInUse)),
            wincode_leb128_config(),
        )
        .unwrap();
        let decoded = decode_compact_metadata(&bytes).unwrap();
        assert!(matches!(
            decoded.err,
            Some(CompactTransactionError::AccountInUse)
        ));
    }

    #[test]
    fn metadata_decoder_accepts_legacy_stored_error() {
        let stored = wincode::serialize(&StoredTransactionError::AccountInUse).unwrap();
        let bytes =
            wincode::config::serialize(&empty_legacy_meta(Some(stored)), wincode_leb128_config())
                .unwrap();
        let decoded = decode_compact_metadata(&bytes).unwrap();
        assert!(matches!(
            decoded.err,
            Some(CompactTransactionError::AccountInUse)
        ));
    }

    #[test]
    fn selective_metadata_scan_matches_full_current_and_legacy_decoders() {
        let mut current_none = empty_current_meta(None);
        current_none.pre_balances = vec![1; 4_096];
        current_none.post_balances = vec![2; 4_096];
        current_none.logs = Some(logs_with_excluded_ids(&[7, 11, 7]));
        let current_none =
            wincode::config::serialize(&current_none, wincode_leb128_config()).unwrap();

        let mut current_error = empty_current_meta(Some(CompactTransactionError::AccountInUse));
        current_error.logs = Some(logs_with_excluded_ids(&[13, 17]));
        let current_error =
            wincode::config::serialize(&current_error, wincode_leb128_config()).unwrap();

        let stored = wincode::serialize(&StoredTransactionError::AccountInUse).unwrap();
        let mut legacy = empty_legacy_meta(Some(stored));
        legacy.logs = Some(logs_with_excluded_ids(&[19, 23, 19]));
        let legacy = wincode::config::serialize(&legacy, wincode_leb128_config()).unwrap();

        for bytes in [&current_none, &current_error, &legacy] {
            assert_eq!(
                selective_metadata_excluded_ids(bytes).unwrap(),
                full_metadata_excluded_ids(bytes).unwrap()
            );
        }
    }

    #[test]
    fn selective_metadata_scan_rejects_false_legacy_success_using_error_semantics() {
        // Epoch 997, block 152, transaction 607. Both selective wire readers consume these bytes,
        // but the legacy reader mistakes the current error tag for a byte-vector length. The full
        // legacy conversion rejects that byte vector as an invalid stored transaction error.
        const METADATA: [u8; 57] = [
            1, 31, 0, 184, 78, 4, 149, 223, 254, 5, 0, 1, 1, 4, 221, 144, 254, 5, 0, 1, 1, 1, 0, 1,
            6, 14, 1, 1, 18, 1, 14, 1, 1, 18, 1, 14, 6, 1, 18, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 194, 3, 1, 248, 13,
        ];

        let mut current = wincode::config::deserialize_exact::<SelectiveCurrentMetaV1, _>(
            &METADATA,
            bounded_wincode_config::<PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES>(),
        )
        .unwrap();
        let mut false_legacy = wincode::config::deserialize_exact::<SelectiveLegacyMetaV1, _>(
            &METADATA,
            bounded_wincode_config::<PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES>(),
        )
        .unwrap();
        let current_keys = collect_selective_log_pubkeys(&mut current.logs).unwrap();
        let false_legacy_keys = collect_selective_log_pubkeys(&mut false_legacy.logs).unwrap();
        assert_eq!(
            current_keys,
            vec![
                CompactPubkey::id(1),
                CompactPubkey::id(1),
                CompactPubkey::id(1),
                CompactPubkey::id(1),
                CompactPubkey::id(6),
                CompactPubkey::id(6),
            ]
        );
        assert!(false_legacy_keys.is_empty());

        let legacy_error = validate_selective_legacy_metadata(false_legacy).unwrap_err();
        assert!(
            format!("{legacy_error:#}").contains("Invalid tag encoding"),
            "unexpected legacy validation error: {legacy_error:#}"
        );

        let full_current = wincode::config::deserialize_exact::<CompactMetaV1, _>(
            &METADATA,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap();
        assert_eq!(
            wincode::config::serialize(&full_current, wincode_leb128_config()).unwrap(),
            METADATA
        );
        assert_eq!(
            selective_metadata_excluded_ids(&METADATA).unwrap(),
            full_metadata_excluded_ids(&METADATA).unwrap()
        );
        assert_eq!(
            selective_metadata_excluded_ids(&METADATA).unwrap(),
            [1, 1, 1, 1, 6, 6]
        );
    }

    #[test]
    fn selective_metadata_scan_rejects_hostile_lengths_like_full_decode() {
        let mut metadata = empty_current_meta(None);
        metadata.fee = 0;
        let bytes = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        assert_eq!(&bytes[..3], &[0, 0, 0]);

        let mut hostile = bytes;
        // Replace the empty pre-balance length with a canonical LEB128 length above the frame cap.
        hostile.splice(2..3, [0x81, 0x80, 0x80, 0x40]);
        assert!(full_metadata_excluded_ids(&hostile).is_err());
        assert!(selective_metadata_excluded_ids(&hostile).is_err());

        assert!(full_metadata_excluded_ids(&[1, 0xff]).is_err());
        assert!(selective_metadata_excluded_ids(&[1, 0xff]).is_err());
    }

    #[test]
    fn pass1_retained_event_and_reward_lengths_are_bounded_before_allocation() {
        fn declared_len_for_first_over_limit<T>(limit: usize) -> (Vec<u8>, usize) {
            let element_size = std::mem::size_of::<T>().max(1);
            let elements = limit / element_size + 1;
            let required = elements.checked_mul(element_size).unwrap();
            assert!(required > limit);
            (
                wincode::config::serialize(&(elements as u64), wincode_leb128_config()).unwrap(),
                required,
            )
        }

        #[derive(Debug, SchemaRead)]
        struct LimitedEvents {
            #[wincode(
                with = "wincode::containers::Vec<LogEvent, wincode::len::BincodeLen<PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES>>"
            )]
            events: Vec<LogEvent>,
        }
        let (event_length_only, event_bytes) = declared_len_for_first_over_limit::<LogEvent>(
            PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES,
        );
        let event_error = wincode::config::deserialize_exact::<LimitedEvents, _>(
            &event_length_only,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap_err();
        assert!(event_error.to_string().contains("preallocation"));
        assert!(event_bytes > PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES);

        #[derive(Debug, SchemaRead)]
        struct LimitedRewards {
            #[wincode(
                with = "wincode::containers::Vec<CompactReward, wincode::len::BincodeLen<PASS1_REWARDS_RETAINED_SEQUENCE_MAX_BYTES>>"
            )]
            rewards: Vec<CompactReward>,
        }
        let (reward_length_only, reward_bytes) = declared_len_for_first_over_limit::<CompactReward>(
            PASS1_REWARDS_RETAINED_SEQUENCE_MAX_BYTES,
        );
        let reward_error = wincode::config::deserialize_exact::<LimitedRewards, _>(
            &reward_length_only,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap_err();
        assert!(reward_error.to_string().contains("preallocation"));
        assert!(reward_bytes > PASS1_REWARDS_RETAINED_SEQUENCE_MAX_BYTES);
    }

    #[test]
    fn selective_metadata_scan_rejects_hostile_error_string_before_allocation() {
        let transaction_error = CompactTransactionError::InstructionError(
            0,
            CompactInstructionError::BorshIoError(String::new()),
        );
        let encoded_error =
            wincode::config::serialize(&transaction_error, wincode_leb128_config()).unwrap();
        assert_eq!(encoded_error.last(), Some(&0));

        let metadata = empty_current_meta(Some(transaction_error));
        let mut hostile = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        assert_eq!(hostile.first(), Some(&1));
        let string_len_offset = 1 + encoded_error.len() - 1;
        assert_eq!(hostile[string_len_offset], 0);
        let hostile_len = wincode::config::serialize(
            &((PASS1_METADATA_RETAINED_SEQUENCE_MAX_BYTES + 1) as u64),
            wincode_leb128_config(),
        )
        .unwrap();
        hostile.splice(string_len_offset..=string_len_offset, hostile_len);

        let error = selective_metadata_excluded_ids(&hostile).unwrap_err();
        assert!(error.to_string().contains("preallocation"));
    }

    #[test]
    fn count_only_scan_skips_raw_metadata_fallback() {
        let metadata_bytes = vec![0xff, 0xee, 0xdd, 0xcc];
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: Some(ArchiveV2HotRewards {
                    num_partitions: Some(1),
                    decoded: vec![CompactReward {
                        pubkey: CompactPubkey::id(5),
                        lamports: 1,
                        post_balance: 1,
                        reward_type: 0,
                        commission: None,
                    }],
                }),
            },
            tx_count: 1,
            tx_rows: vec![ArchiveV2HotTxRow {
                tx_index: 0,
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                message_offset: 0,
                message_len: 0,
                metadata_offset: 0,
                metadata_len: metadata_bytes.len() as u32,
                signature_count: 0,
                reserved: [0; 3],
            }],
            message_bytes: Vec::new(),
            metadata_bytes,
        };
        let encoded = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let analysis = analyze_source_exclusions(compressed_test_input(encoded, 1, 0)).unwrap();
        assert_eq!(analysis.excluded, vec![(5, 1)]);
        assert_eq!(analysis.transactions, 1);
    }

    #[test]
    fn count_only_scan_rejects_trailing_current_outer_bytes() {
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let mut encoded = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        encoded.push(0xa5);
        assert!(analyze_source_exclusions(compressed_test_input(encoded, 0, 0)).is_err());
    }

    #[test]
    fn count_only_scan_legacy_shredding_fallback_preserves_exclusions() {
        let mut metadata = empty_current_meta(None);
        metadata.logs = Some(logs_with_excluded_ids(&[7, 9, 7]));
        let metadata_bytes =
            wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let legacy = LegacyHotBlockWithShredding {
            header: LegacyHotBlockHeaderWithShredding {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                shredding: vec![CompactShredding {
                    entry_end_idx: 1,
                    shred_end_idx: 2,
                }],
                rewards: Some(ArchiveV2HotRewards {
                    num_partitions: None,
                    decoded: vec![CompactReward {
                        pubkey: CompactPubkey::id(7),
                        lamports: 1,
                        post_balance: 1,
                        reward_type: 0,
                        commission: None,
                    }],
                }),
            },
            tx_count: 1,
            tx_rows: vec![ArchiveV2HotTxRow {
                tx_index: 0,
                flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                message_offset: 0,
                message_len: 0,
                metadata_offset: 0,
                metadata_len: metadata_bytes.len() as u32,
                signature_count: 0,
                reserved: [0; 3],
            }],
            message_bytes: Vec::new(),
            metadata_bytes,
        };
        let encoded = wincode::config::serialize(&legacy, wincode_leb128_config()).unwrap();
        let row_len_offset = wincode::config::serialize(&legacy.header, wincode_leb128_config())
            .unwrap()
            .len()
            + wincode::config::serialize(&legacy.tx_count, wincode_leb128_config())
                .unwrap()
                .len();
        assert_eq!(encoded[row_len_offset], 1);
        let mut malformed_rows = encoded.clone();
        malformed_rows[row_len_offset] = 2;
        assert!(analyze_source_exclusions(compressed_test_input(malformed_rows, 1, 0)).is_err());

        let analysis = analyze_source_exclusions(compressed_test_input(encoded, 1, 0)).unwrap();
        assert_eq!(analysis.excluded, vec![(7, 3), (9, 1)]);
    }

    fn open_test_registry(root: &TestDir, name: &str, keys: &[[u8; 32]]) -> MappedRegistry {
        let path = root.0.join(name);
        fs::write(&path, keys.concat()).unwrap();
        MappedRegistry::open(&path).unwrap()
    }

    fn registry_wire_parity_block() -> ArchiveV2HotBlockBlob {
        let header = CompactMessageHeader {
            num_required_signatures: 0,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        };
        let first_message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header,
            account_keys: vec![CompactPubkey::id(1), CompactPubkey::id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(-1),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: vec![0],
                data: ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(vote_update(
                    ArchiveV2VoteHashRef::Block(0),
                    1,
                )),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::id(3),
                writable_indexes: vec![0],
                readonly_indexes: vec![1],
            }],
        });
        let second_message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header,
            account_keys: vec![CompactPubkey::id(2)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([0x41; 32]),
            instructions: Vec::new(),
        });
        let first_message =
            wincode::config::serialize(&first_message, wincode_leb128_config()).unwrap();
        let second_message =
            wincode::config::serialize(&second_message, wincode_leb128_config()).unwrap();
        let raw_message = vec![0xde, 0xad];

        let mut successful = empty_current_meta(None);
        successful.loaded_writable_addresses = vec![CompactPubkey::id(3)];
        successful.return_data = Some(CompactReturnData {
            program_id: CompactPubkey::id(1),
            data: vec![7, 8],
        });
        successful.logs = Some(logs_with_excluded_ids(&[4]));
        let successful = wincode::config::serialize(&successful, wincode_leb128_config()).unwrap();

        let mut failed = empty_current_meta(Some(CompactTransactionError::AccountInUse));
        failed.loaded_readonly_addresses = vec![CompactPubkey::id(2)];
        failed.rewards.push(CompactReward {
            pubkey: CompactPubkey::id(3),
            lamports: 1,
            post_balance: 2,
            reward_type: 0,
            commission: None,
        });
        failed.logs = Some(logs_with_excluded_ids(&[4]));
        let failed = wincode::config::serialize(&failed, wincode_leb128_config()).unwrap();
        let raw_metadata = vec![0xbe, 0xef, 0x33];

        ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: Some(ArchiveV2HotRewards {
                    num_partitions: None,
                    decoded: vec![CompactReward {
                        pubkey: CompactPubkey::id(5),
                        lamports: 1,
                        post_balance: 2,
                        reward_type: 0,
                        commission: None,
                    }],
                }),
            },
            tx_count: 3,
            tx_rows: vec![
                ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                    message_offset: 0,
                    message_len: first_message.len() as u32,
                    metadata_offset: 0,
                    metadata_len: successful.len() as u32,
                    signature_count: 0,
                    reserved: [0; 3],
                },
                ArchiveV2HotTxRow {
                    tx_index: 1,
                    flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                    message_offset: first_message.len() as u32,
                    message_len: second_message.len() as u32,
                    metadata_offset: successful.len() as u32,
                    metadata_len: failed.len() as u32,
                    signature_count: 0,
                    reserved: [0; 3],
                },
                ArchiveV2HotTxRow {
                    tx_index: 2,
                    flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                        | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                        | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                    message_offset: (first_message.len() + second_message.len()) as u32,
                    message_len: raw_message.len() as u32,
                    metadata_offset: (successful.len() + failed.len()) as u32,
                    metadata_len: raw_metadata.len() as u32,
                    signature_count: 0,
                    reserved: [0; 3],
                },
            ],
            message_bytes: [first_message, second_message, raw_message].concat(),
            metadata_bytes: [successful, failed, raw_metadata].concat(),
        }
    }

    fn canonical_access(mut access: AccessReferenceSet) -> (Vec<u32>, Vec<i32>, Vec<u32>) {
        access.pubkey_ids.sort_unstable();
        access.pubkey_ids.dedup();
        access.blockhash_ids.sort_unstable();
        access.blockhash_ids.dedup();
        access.vote_hash_block_ids.sort_unstable();
        access.vote_hash_block_ids.dedup();
        (
            access.pubkey_ids,
            access.blockhash_ids,
            access.vote_hash_block_ids,
        )
    }

    #[test]
    fn borrowed_current_outer_is_exact_and_rejects_malformed_input() {
        let block = registry_wire_parity_block();
        let mut encoded = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let (header, tx_count, row_bytes, message_bytes, metadata_bytes) =
            decode_current_rewrite_hot_block_borrowed(&encoded).unwrap();
        assert_eq!(header.slot, block.header.slot);
        assert_eq!(header.parent_slot, block.header.parent_slot);
        assert_eq!(tx_count, block.tx_count);
        assert_eq!(row_bytes.len(), block.tx_rows.len());
        assert_eq!(
            row_bytes.iter().map(decode_hot_tx_row).collect::<Vec<_>>(),
            block.tx_rows
        );
        assert_eq!(message_bytes, block.message_bytes);
        assert_eq!(metadata_bytes, block.metadata_bytes);

        assert!(decode_current_rewrite_hot_block_borrowed(&encoded[..encoded.len() - 1]).is_err());
        encoded.push(0xff);
        assert!(decode_current_rewrite_hot_block_borrowed(&encoded).is_err());
    }

    #[test]
    fn current_borrowed_source_rewrite_matches_owned_output_counts_and_access() {
        let root = TestDir::new();
        let source_registry = open_test_registry(
            &root,
            "borrowed-source-registry.bin",
            &[[1; 32], [2; 32], [3; 32], [4; 32], [5; 32]],
        );
        let target_registry = open_test_registry(
            &root,
            "borrowed-target-registry.bin",
            &[[2; 32], [3; 32], [1; 32]],
        );
        let old_to_new = [3, 1, 2, 0, 0];
        let source_encoded =
            wincode::config::serialize(&registry_wire_parity_block(), wincode_leb128_config())
                .unwrap();
        let mut expected_block: ArchiveV2HotBlockBlob = wincode::config::deserialize_exact(
            &source_encoded,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap();

        let mut expected_eligible = Vec::new();
        let mut expected_excluded = Vec::new();
        let mut expected_references = 0u64;
        let mut expected_access_references = AccessReferenceSet::default();
        super::super::collect_access_blockhash_id(
            expected_block.header.blockhash_id as i32,
            &mut expected_access_references.blockhash_ids,
        );
        super::super::collect_access_blockhash_id(
            expected_block.header.previous_blockhash_id as i32,
            &mut expected_access_references.blockhash_ids,
        );
        rewrite_block_pubkeys_with_access(
            &mut expected_block,
            TEST_WIRE_PROFILE,
            Some(&mut expected_access_references),
            |key, class| {
                let old_id =
                    remap_source_pubkey(key, class, &old_to_new, |id| source_registry.key(id), 0)?;
                match class {
                    ReferenceClass::Eligible => expected_eligible.push(old_id),
                    ReferenceClass::Excluded => expected_excluded.push(old_id),
                }
                expected_references += 1;
                Ok(())
            },
        )
        .unwrap();

        let access_context = AccessBuildContext {
            blockhashes: vec![[0xa1; 32]],
            previous_tail: vec![super::super::PreviousBlockhash {
                hash: [0xb2; 32],
                slot: crate::SLOTS_PER_EPOCH - 1,
            }],
            vote_hashes: vec![super::super::VoteHashRegistryRow {
                bank_hash: Some([0xc3; 32]),
                block_id_hash: Some([0xd4; 32]),
            }],
        };
        let expected_access = build_block_access_from_collected_references(
            &expected_block,
            expected_access_references,
            |id| target_registry.key(id),
            &access_context.blockhashes,
            &access_context.previous_tail,
            &[],
            &access_context.vote_hashes,
        )
        .unwrap();
        let expected_access =
            wincode::config::serialize(&expected_access, wincode_leb128_config()).unwrap();
        let expected_encoded =
            wincode::config::serialize(&expected_block, wincode_leb128_config()).unwrap();
        let expected_compressed = zstd::bulk::compress(&expected_encoded, 1).unwrap();
        let expected_eligible = compress_id_counts(expected_eligible).unwrap();
        let expected_excluded = compress_id_counts(expected_excluded).unwrap();

        let mut input = compressed_test_input(source_encoded.clone(), 3, 0);
        input.signatures = Some(Vec::new());
        let rewritten = rewrite_source_block(
            input,
            &source_registry,
            &old_to_new,
            &target_registry,
            1,
            TEST_WIRE_PROFILE,
            Some(&access_context),
            true,
        )
        .unwrap();

        assert_eq!(rewritten.compressed, expected_compressed);
        assert_eq!(rewritten.uncompressed_len as usize, expected_encoded.len());
        assert_eq!(
            zstd::bulk::decompress(&rewritten.compressed, expected_encoded.len()).unwrap(),
            expected_encoded
        );
        assert_eq!(rewritten.eligible, expected_eligible);
        assert_eq!(rewritten.excluded, expected_excluded);
        assert_eq!(rewritten.stats.references, expected_references);
        assert_eq!(
            rewritten.access.as_deref(),
            Some(expected_access.as_slice())
        );
        assert_eq!(rewritten.phase_timing.wire_message_fast_records, 2);
        assert_eq!(rewritten.phase_timing.wire_message_fallback_records, 0);
        assert_eq!(rewritten.phase_timing.wire_metadata_fast_records, 2);
        assert_eq!(rewritten.phase_timing.wire_metadata_fallback_records, 0);
        assert_eq!(rewritten.phase_timing.wire_metadata_success_fast_records, 1);
        assert_eq!(
            rewritten
                .phase_timing
                .wire_metadata_current_error_fast_records,
            1
        );

        let core_only = rewrite_source_block(
            compressed_test_input(source_encoded, 3, 0),
            &source_registry,
            &old_to_new,
            &target_registry,
            1,
            TEST_WIRE_PROFILE,
            None,
            true,
        )
        .unwrap();
        assert_eq!(core_only.compressed, expected_compressed);
        assert_eq!(core_only.eligible, expected_eligible);
        assert_eq!(core_only.excluded, expected_excluded);
        assert_eq!(core_only.stats.references, expected_references);
        assert!(core_only.access.is_none());
    }

    #[test]
    fn registry_wire_block_matches_owned_oracle_counts_access_and_error_fallback() {
        let root = TestDir::new();
        let source_registry = open_test_registry(
            &root,
            "wire-source-registry.bin",
            &[[1; 32], [2; 32], [3; 32], [4; 32], [5; 32]],
        );
        let old_to_new = [3, 1, 2, 0, 0];
        let source =
            wincode::config::serialize(&registry_wire_parity_block(), wincode_leb128_config())
                .unwrap();
        let mut owned: ArchiveV2HotBlockBlob = wincode::config::deserialize_exact(
            &source,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap();
        let mut wire: ArchiveV2HotBlockBlob = wincode::config::deserialize_exact(
            &source,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap();

        let mut owned_eligible = Vec::new();
        let mut owned_excluded = Vec::new();
        let mut owned_references = 0u64;
        let mut owned_access = AccessReferenceSet::default();
        super::super::collect_access_blockhash_id(
            owned.header.blockhash_id as i32,
            &mut owned_access.blockhash_ids,
        );
        super::super::collect_access_blockhash_id(
            owned.header.previous_blockhash_id as i32,
            &mut owned_access.blockhash_ids,
        );
        rewrite_block_pubkeys_with_access(
            &mut owned,
            TEST_WIRE_PROFILE,
            Some(&mut owned_access),
            |key, class| {
                let old_id =
                    remap_source_pubkey(key, class, &old_to_new, |id| source_registry.key(id), 0)?;
                match class {
                    ReferenceClass::Eligible => owned_eligible.push(old_id),
                    ReferenceClass::Excluded => owned_excluded.push(old_id),
                }
                owned_references += 1;
                Ok(())
            },
        )
        .unwrap();

        let mut wire_eligible = Vec::new();
        let mut wire_excluded = Vec::new();
        let mut wire_references = 0u64;
        let mut wire_access = AccessReferenceSet::default();
        super::super::collect_access_blockhash_id(
            wire.header.blockhash_id as i32,
            &mut wire_access.blockhash_ids,
        );
        super::super::collect_access_blockhash_id(
            wire.header.previous_blockhash_id as i32,
            &mut wire_access.blockhash_ids,
        );
        let mut timing = Pass2PhaseTiming::default();
        {
            let mut visitor = RegistryWireVisitor {
                source_registry: &source_registry,
                old_to_new: &old_to_new,
                block_id: 0,
                eligible_ids: &mut wire_eligible,
                excluded_ids: &mut wire_excluded,
                references: &mut wire_references,
                access: Some(&mut wire_access),
            };
            rewrite_registry_block_pubkeys_with_access_wire(
                &mut wire,
                TEST_WIRE_PROFILE,
                &mut visitor,
                &mut timing,
            )
            .unwrap();
        }

        assert_eq!(
            wincode::config::serialize(&wire, wincode_leb128_config()).unwrap(),
            wincode::config::serialize(&owned, wincode_leb128_config()).unwrap()
        );
        assert_eq!(wire_references, owned_references);
        assert_eq!(
            compress_id_counts(wire_eligible).unwrap(),
            compress_id_counts(owned_eligible).unwrap()
        );
        assert_eq!(
            compress_id_counts(wire_excluded).unwrap(),
            compress_id_counts(owned_excluded).unwrap()
        );
        assert_eq!(
            canonical_access(wire_access),
            canonical_access(owned_access)
        );
        assert_eq!(timing.wire_message_fast_records, 2);
        assert_eq!(timing.wire_message_fallback_records, 0);
        assert_eq!(timing.wire_metadata_fast_records, 2);
        assert_eq!(timing.wire_metadata_fallback_records, 0);
        assert_eq!(timing.wire_metadata_success_fast_records, 1);
        assert_eq!(timing.wire_metadata_current_error_fast_records, 1);
    }

    #[test]
    fn registry_wire_visitor_rolls_back_all_state_after_nested_fallback() {
        let root = TestDir::new();
        let source_registry = open_test_registry(&root, "wire-rollback-registry.bin", &[[1; 32]]);
        let mut eligible = vec![91];
        let mut excluded = vec![92];
        let mut references = 2u64;
        let mut access = AccessReferenceSet {
            pubkey_ids: vec![93],
            blockhash_ids: vec![94],
            vote_hash_block_ids: vec![95],
        };
        let mut visitor = RegistryWireVisitor {
            source_registry: &source_registry,
            old_to_new: &[1],
            block_id: 0,
            eligible_ids: &mut eligible,
            excluded_ids: &mut excluded,
            references: &mut references,
            access: Some(&mut access),
        };
        let before = visitor.snapshot();
        // Legacy message, one account key, recent blockhash ID, and one instruction whose data
        // tag 9 is deliberately outside the current 0..=8 schema. The callbacks run first.
        let message = [0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 9];
        let mut output = vec![0xaa];
        let error = rewrite_archive_v2_hot_message_wire(
            &message,
            &mut output,
            &mut visitor,
            ArchiveV2WireRewriteLimits::default(),
        )
        .unwrap_err();
        assert!(matches!(
            error.kind(),
            ArchiveV2WireRewriteErrorKind::Fallback(_)
        ));
        assert_eq!(output, vec![0xaa]);
        assert_eq!(visitor.snapshot(), before);
    }

    #[test]
    fn registry_wire_combined_payload_limit_is_exact_and_transactional() {
        let root = TestDir::new();
        let source_registry = open_test_registry(&root, "wire-limit-registry.bin", &[[1; 32]]);
        let mut eligible = Vec::new();
        let mut excluded = Vec::new();
        let mut references = 0u64;
        let mut visitor = RegistryWireVisitor {
            source_registry: &source_registry,
            old_to_new: &[1],
            block_id: 0,
            eligible_ids: &mut eligible,
            excluded_ids: &mut excluded,
            references: &mut references,
            access: None,
        };
        let limits =
            registry_wire_rewrite_limits(MAX_HOT_BLOCK_FRAME_BYTES_USIZE - 2, 1, &visitor).unwrap();
        assert_eq!(limits.max_output_bytes, 1);
        assert!(
            registry_wire_rewrite_limits(MAX_HOT_BLOCK_FRAME_BYTES_USIZE, 1, &visitor).is_err()
        );

        let message = ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
            header: CompactMessageHeader {
                num_required_signatures: 0,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![CompactPubkey::id(1)],
            recent_blockhash: OwnedCompactRecentBlockhash::Nonce([0; 32]),
            instructions: Vec::new(),
        });
        let message = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let before = visitor.snapshot();
        let mut output = vec![0xbb];
        let error =
            rewrite_archive_v2_hot_message_wire(&message, &mut output, &mut visitor, limits)
                .unwrap_err();
        assert_eq!(error.kind(), ArchiveV2WireRewriteErrorKind::LimitExceeded);
        assert_eq!(output, vec![0xbb]);
        assert_eq!(visitor.snapshot(), before);
    }

    #[test]
    fn legacy_outer_blocks_use_profiled_message_wire_with_exact_output() {
        let root = TestDir::new();
        let source_registry = open_test_registry(
            &root,
            "legacy-route-source-registry.bin",
            &[[1; 32], [2; 32], [3; 32], [4; 32], [5; 32]],
        );
        let target_registry = open_test_registry(
            &root,
            "legacy-route-target-registry.bin",
            &[[2; 32], [3; 32], [1; 32]],
        );
        let old_to_new = [3, 1, 2, 0, 0];

        let shredding_block = registry_wire_parity_block();
        let legacy_shredding = LegacyHotBlockWithShredding {
            header: LegacyHotBlockHeaderWithShredding {
                slot: shredding_block.header.slot,
                parent_slot: shredding_block.header.parent_slot,
                blockhash_id: shredding_block.header.blockhash_id,
                previous_blockhash_id: shredding_block.header.previous_blockhash_id,
                block_time: shredding_block.header.block_time,
                block_height: shredding_block.header.block_height,
                shredding: Vec::new(),
                rewards: shredding_block.header.rewards,
            },
            tx_count: shredding_block.tx_count,
            tx_rows: shredding_block.tx_rows,
            message_bytes: shredding_block.message_bytes,
            metadata_bytes: shredding_block.metadata_bytes,
        };
        let shredding_encoded =
            wincode::config::serialize(&legacy_shredding, wincode_leb128_config()).unwrap();
        let shredding_expected: ArchiveV2HotBlockBlob = legacy_shredding.into();

        let rewards_block = registry_wire_parity_block();
        let legacy_rewards = LegacyHotBlockWithRewardsVec {
            header: LegacyHotBlockHeaderWithRewardsVec {
                slot: rewards_block.header.slot,
                parent_slot: rewards_block.header.parent_slot,
                blockhash_id: rewards_block.header.blockhash_id,
                previous_blockhash_id: rewards_block.header.previous_blockhash_id,
                block_time: rewards_block.header.block_time,
                block_height: rewards_block.header.block_height,
                rewards: rewards_block
                    .header
                    .rewards
                    .map_or_else(Vec::new, |rewards| rewards.decoded),
            },
            tx_count: rewards_block.tx_count,
            tx_rows: rewards_block.tx_rows,
            message_bytes: rewards_block.message_bytes,
            metadata_bytes: rewards_block.metadata_bytes,
        };
        let rewards_encoded =
            wincode::config::serialize(&legacy_rewards, wincode_leb128_config()).unwrap();
        let rewards_expected: ArchiveV2HotBlockBlob = legacy_rewards.into();

        for (encoded, expected_schema, mut expected) in [
            (
                shredding_encoded,
                HotBlockOuterSchema::LegacyShredding,
                shredding_expected,
            ),
            (
                rewards_encoded,
                HotBlockOuterSchema::LegacyRewardsVec,
                rewards_expected,
            ),
        ] {
            let input = compressed_test_input(encoded.clone(), 3, 0);
            let current_error = decode_current_rewrite_hot_block_borrowed(&encoded).unwrap_err();
            assert_eq!(
                decode_legacy_hot_block_bytes(&encoded, input.row, &current_error.to_string())
                    .unwrap()
                    .outer_schema,
                expected_schema
            );
            assert_eq!(
                decode_hot_block_bytes_with_schema(&encoded, input.row)
                    .unwrap()
                    .outer_schema,
                expected_schema
            );
            rewrite_block_pubkeys_with_access(
                &mut expected,
                TEST_WIRE_PROFILE,
                None,
                |key, class| {
                    remap_source_pubkey(key, class, &old_to_new, |id| source_registry.key(id), 0)?;
                    Ok(())
                },
            )
            .unwrap();
            let expected = wincode::config::serialize(&expected, wincode_leb128_config()).unwrap();

            let rewritten = rewrite_source_block(
                input,
                &source_registry,
                &old_to_new,
                &target_registry,
                1,
                TEST_WIRE_PROFILE,
                None,
                true,
            )
            .unwrap();
            let actual =
                zstd::bulk::decompress(&rewritten.compressed, rewritten.uncompressed_len as usize)
                    .unwrap();
            assert_eq!(actual, expected);
            assert_eq!(rewritten.phase_timing.wire_message_fast_records, 2);
            assert_eq!(rewritten.phase_timing.wire_message_fallback_records, 0);
            assert_eq!(rewritten.phase_timing.wire_metadata_fast_records, 2);
            assert_eq!(rewritten.phase_timing.wire_metadata_fallback_records, 0);
        }
    }

    #[test]
    fn deep_normalization_is_registry_representation_neutral() {
        let mut metadata = empty_current_meta(None);
        metadata.logs = Some(logs_with_excluded_ids(&[7, 9, 7]));
        metadata.loaded_writable_addresses = vec![CompactPubkey::id(11)];
        let encoded_metadata =
            wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let raw_metadata = [0xde, 0xad, 0xbe, 0xef];
        let raw_messages = [0xa5, 0x5a, 0x33];
        let block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: Some(ArchiveV2HotRewards {
                    num_partitions: None,
                    decoded: vec![CompactReward {
                        pubkey: CompactPubkey::id(5),
                        lamports: 1,
                        post_balance: 2,
                        reward_type: 0,
                        commission: None,
                    }],
                }),
            },
            tx_count: 2,
            tx_rows: vec![
                ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK | ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                    message_offset: 0,
                    message_len: 1,
                    metadata_offset: 0,
                    metadata_len: encoded_metadata.len() as u32,
                    signature_count: 0,
                    reserved: [0; 3],
                },
                ArchiveV2HotTxRow {
                    tx_index: 1,
                    flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                        | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                        | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                    message_offset: 1,
                    message_len: 2,
                    metadata_offset: encoded_metadata.len() as u32,
                    metadata_len: raw_metadata.len() as u32,
                    signature_count: 0,
                    reserved: [0; 3],
                },
            ],
            message_bytes: raw_messages.to_vec(),
            metadata_bytes: [encoded_metadata, raw_metadata.to_vec()].concat(),
        };
        let encoded = wincode::config::serialize(&block, wincode_leb128_config()).unwrap();
        let mut full: ArchiveV2HotBlockBlob = wincode::config::deserialize_exact(
            &encoded,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap();
        let expected = normalize_block_structure(&mut full, 0).unwrap();

        let mut fused: ArchiveV2HotBlockBlob = wincode::config::deserialize_exact(
            &encoded,
            bounded_wincode_config::<MAX_HOT_BLOCK_FRAME_BYTES_USIZE>(),
        )
        .unwrap();
        rewrite_block_pubkeys_with_access(&mut fused, TEST_WIRE_PROFILE, None, |key, _class| {
            if let CompactPubkey::Id(id) = key {
                *id += 100;
            }
            Ok(())
        })
        .unwrap();
        let actual = normalize_block_structure(&mut fused, 0).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn collected_access_references_match_canonical_builder_bytes() {
        let message = ArchiveV2HotMessagePayload::V0(ArchiveV2HotV0Message {
            header: CompactMessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![
                CompactPubkey::id(1),
                CompactPubkey::id(1),
                CompactPubkey::id(2),
                CompactPubkey::raw([0x77; 32]),
            ],
            recent_blockhash: OwnedCompactRecentBlockhash::Id(-1),
            instructions: vec![ArchiveV2HotInstruction {
                program_id_index: 0,
                accounts: vec![0, 1],
                data: ArchiveV2HotInstructionData::VoteCompactUpdateVoteState(
                    ArchiveV2VoteStateUpdate {
                        root: None,
                        lockout_offsets: Vec::new(),
                        hash: ArchiveV2VoteHashRef::Block(0),
                        timestamp: None,
                    },
                ),
            }],
            address_table_lookups: vec![OwnedCompactAddressTableLookup {
                account_key: CompactPubkey::id(3),
                writable_indexes: vec![1],
                readonly_indexes: vec![2],
            }],
        });
        let message_bytes = wincode::config::serialize(&message, wincode_leb128_config()).unwrap();
        let mut metadata = empty_current_meta(None);
        metadata.loaded_writable_addresses = vec![CompactPubkey::id(2), CompactPubkey::id(2)];
        metadata.loaded_readonly_addresses = vec![CompactPubkey::id(3)];
        metadata.logs = Some(CompactLogStream {
            events: vec![
                LogEvent::Invoke {
                    program: CompactPubkey::id(4),
                    depth: 1,
                },
                LogEvent::Invoke {
                    program: CompactPubkey::raw([0x66; 32]),
                    depth: 2,
                },
            ],
            strings: Default::default(),
            data: Default::default(),
        });
        let metadata_bytes =
            wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        let raw_message = [0x55, 0x44];
        let raw_metadata = [0x33, 0x22, 0x11];
        let mut block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: Some(ArchiveV2HotRewards {
                    num_partitions: None,
                    decoded: vec![CompactReward {
                        pubkey: CompactPubkey::raw([0x88; 32]),
                        lamports: 1,
                        post_balance: 2,
                        reward_type: 0,
                        commission: None,
                    }],
                }),
            },
            tx_count: 2,
            tx_rows: vec![
                ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: ARCHIVE_V2_TX_FLAG_HAS_METADATA,
                    message_offset: 0,
                    message_len: message_bytes.len() as u32,
                    metadata_offset: 0,
                    metadata_len: metadata_bytes.len() as u32,
                    signature_count: 1,
                    reserved: [0; 3],
                },
                ArchiveV2HotTxRow {
                    tx_index: 1,
                    flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                        | ARCHIVE_V2_TX_FLAG_HAS_METADATA
                        | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
                    message_offset: message_bytes.len() as u32,
                    message_len: raw_message.len() as u32,
                    metadata_offset: metadata_bytes.len() as u32,
                    metadata_len: raw_metadata.len() as u32,
                    signature_count: 0,
                    reserved: [0; 3],
                },
            ],
            message_bytes: [message_bytes, raw_message.to_vec()].concat(),
            metadata_bytes: [metadata_bytes, raw_metadata.to_vec()].concat(),
        };
        let mut references = AccessReferenceSet::default();
        super::super::collect_access_blockhash_id(
            block.header.blockhash_id as i32,
            &mut references.blockhash_ids,
        );
        super::super::collect_access_blockhash_id(
            block.header.previous_blockhash_id as i32,
            &mut references.blockhash_ids,
        );
        rewrite_block_pubkeys_with_access(
            &mut block,
            TEST_WIRE_PROFILE,
            Some(&mut references),
            |key, _| {
                *key = match *key {
                    CompactPubkey::Id(1) => CompactPubkey::id(4),
                    CompactPubkey::Id(2) => CompactPubkey::raw([0x22; 32]),
                    CompactPubkey::Id(3) => CompactPubkey::id(3),
                    CompactPubkey::Id(4) => CompactPubkey::id(1),
                    CompactPubkey::Id(other) => panic!("unexpected pubkey ID {other}"),
                    CompactPubkey::Raw(raw) => CompactPubkey::raw(raw),
                };
                Ok(())
            },
        )
        .unwrap();

        let registry = vec![[0x01; 32], [0x02; 32], [0x03; 32], [0x04; 32]];
        let blockhashes = vec![[0xa1; 32]];
        let previous_tail = vec![super::super::PreviousBlockhash {
            hash: [0xb2; 32],
            slot: crate::SLOTS_PER_EPOCH - 1,
        }];
        let vote_hashes = vec![super::super::VoteHashRegistryRow {
            bank_hash: Some([0xc3; 32]),
            block_id_hash: Some([0xd4; 32]),
        }];
        let signatures = vec![0xe5; 64];
        let canonical = super::super::build_archive_v2_block_access_blob_with_pubkey_resolver(
            &block,
            |id| Ok(registry[(id - 1) as usize]),
            &blockhashes,
            &previous_tail,
            &signatures,
            &vote_hashes,
        )
        .unwrap();
        let collected = build_block_access_from_collected_references(
            &block,
            references,
            |id| Ok(registry[(id - 1) as usize]),
            &blockhashes,
            &previous_tail,
            &signatures,
            &vote_hashes,
        )
        .unwrap();
        let canonical = wincode::config::serialize(&canonical, wincode_leb128_config()).unwrap();
        let collected = wincode::config::serialize(&collected, wincode_leb128_config()).unwrap();
        assert_eq!(collected, canonical);
    }

    #[test]
    fn permuted_tx_rows_keep_storage_order_offsets_signatures_and_profile_decode() {
        let message = |account_id, recent_blockhash_id| {
            wincode::config::serialize(
                &ArchiveV2HotMessagePayload::Legacy(ArchiveV2HotLegacyMessage {
                    header: CompactMessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: vec![CompactPubkey::id(account_id)],
                    recent_blockhash: OwnedCompactRecentBlockhash::Id(recent_blockhash_id),
                    instructions: Vec::new(),
                }),
                wincode_leb128_config(),
            )
            .unwrap()
        };
        let first = message(1, -11);
        let second = message(2, -22);
        let mut block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: None,
            },
            tx_count: 2,
            tx_rows: vec![
                ArchiveV2HotTxRow {
                    tx_index: 1,
                    flags: 0,
                    message_offset: 0,
                    message_len: first.len() as u32,
                    metadata_offset: 0,
                    metadata_len: 0,
                    signature_count: 2,
                    reserved: [0; 3],
                },
                ArchiveV2HotTxRow {
                    tx_index: 0,
                    flags: 0,
                    message_offset: first.len() as u32,
                    message_len: second.len() as u32,
                    metadata_offset: 0,
                    metadata_len: 0,
                    signature_count: 1,
                    reserved: [0; 3],
                },
            ],
            message_bytes: [first, second].concat(),
            metadata_bytes: Vec::new(),
        };
        let mut access = AccessReferenceSet::default();

        rewrite_block_pubkeys_with_access(
            &mut block,
            TEST_WIRE_PROFILE,
            Some(&mut access),
            |key, _| {
                let CompactPubkey::Id(id) = key else {
                    bail!("test message unexpectedly contains a raw pubkey")
                };
                *id += 10;
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(
            block
                .tx_rows
                .iter()
                .map(|row| (row.tx_index, row.signature_count))
                .collect::<Vec<_>>(),
            [(1, 2), (0, 1)]
        );
        assert_eq!(block.tx_rows[0].message_offset, 0);
        assert_eq!(
            block.tx_rows[1].message_offset,
            block.tx_rows[0].message_len
        );
        let decoded_ids = block
            .tx_rows
            .iter()
            .map(|row| {
                let bytes = checked_region(
                    &block.message_bytes,
                    row.message_offset,
                    row.message_len,
                    block.header.slot,
                    row.tx_index,
                    "message",
                )
                .unwrap();
                let message = ArchiveV2MessageProjector::new(TEST_WIRE_PROFILE)
                    .decode_owned_message(bytes)
                    .unwrap();
                let ArchiveV2HotMessagePayload::Legacy(message) = message else {
                    panic!("test message changed variant")
                };
                message.account_keys[0]
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded_ids, [CompactPubkey::id(11), CompactPubkey::id(12)]);
        assert_eq!(access.blockhash_ids, [-11, -22]);
    }

    #[test]
    fn tx_index_permutation_rejects_duplicates_and_out_of_range_values() {
        assert!(validate_tx_index_permutation([1, 0].into_iter(), 2, 42).is_ok());
        assert!(validate_tx_index_permutation([0, 0].into_iter(), 2, 42).is_err());
        assert!(validate_tx_index_permutation([0, 2].into_iter(), 2, 42).is_err());
    }

    #[test]
    fn canonical_counts_exclude_block_rewards_but_include_metadata_rewards() {
        let reward = |byte| CompactReward {
            pubkey: CompactPubkey::raw([byte; 32]),
            lamports: 1,
            post_balance: 1,
            reward_type: 0,
            commission: None,
        };
        let mut block = ArchiveV2HotBlockBlob {
            header: ArchiveV2HotBlockHeader {
                slot: crate::SLOTS_PER_EPOCH,
                parent_slot: crate::SLOTS_PER_EPOCH - 1,
                blockhash_id: 0,
                previous_blockhash_id: 0,
                block_time: None,
                block_height: None,
                rewards: Some(ArchiveV2HotRewards {
                    num_partitions: None,
                    decoded: vec![reward(7)],
                }),
            },
            tx_count: 0,
            tx_rows: Vec::new(),
            message_bytes: Vec::new(),
            metadata_bytes: Vec::new(),
        };
        let mut classes = Vec::new();
        rewrite_block_pubkeys(&mut block, TEST_WIRE_PROFILE, |_key, class| {
            classes.push(class);
            Ok(())
        })
        .unwrap();

        let mut metadata = empty_current_meta(None);
        metadata.rewards.push(reward(9));
        visit_metadata_pubkeys(&mut metadata, &mut |_key, class| {
            classes.push(class);
            Ok(())
        })
        .unwrap();

        assert_eq!(
            classes,
            vec![ReferenceClass::Excluded, ReferenceClass::Eligible]
        );
    }

    #[test]
    fn hot_batch_rejects_single_row_over_memory_budget() {
        let rows = [blockzilla_format::ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: crate::SLOTS_PER_EPOCH,
            compressed_offset: 0,
            compressed_len: (HOT_BATCH_MEMORY_BUDGET_BYTES / 2 + 1) as u32,
            uncompressed_len: (HOT_BATCH_MEMORY_BUDGET_BYTES / 2) as u32,
            tx_count: 0,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 0,
        }];
        assert!(hot_batch_end(&rows, 0, 1, false).is_err());
    }

    #[test]
    fn bounded_ordered_pipeline_keeps_order_and_progresses_past_a_slow_first_item() {
        let gate = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let worker_gate = std::sync::Arc::clone(&gate);
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let handle = std::thread::spawn(move || {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(3)
                .build()
                .unwrap();
            let mut written = Vec::new();
            let result = run_bounded_ordered_pipeline(
                &pool,
                12,
                3,
                100,
                |_| Ok(30),
                Ok,
                move |sequence| {
                    started_tx.send(sequence).unwrap();
                    if sequence == 0 {
                        let (released, wake) = &*worker_gate;
                        let mut released = released.lock().unwrap();
                        while !*released {
                            released = wake.wait(released).unwrap();
                        }
                    }
                    Ok(sequence)
                },
                |_| Ok(10),
                |sequence, value| {
                    ensure!(sequence == value);
                    written.push(value);
                    Ok(())
                },
            );
            done_tx.send((result, written)).unwrap();
        });

        let mut started = Vec::new();
        let mut progress_error = None;
        while started.len() < 6 {
            match started_rx.recv_timeout(Duration::from_secs(2)) {
                Ok(sequence) => started.push(sequence),
                Err(error) => {
                    progress_error = Some(error);
                    break;
                }
            }
        }
        let over_budget_start = if progress_error.is_none() {
            Some(started_rx.recv_timeout(Duration::from_millis(100)))
        } else {
            None
        };

        let (released, wake) = &*gate;
        *released.lock().unwrap() = true;
        wake.notify_all();

        assert!(
            progress_error.is_none(),
            "pipeline did not replace a completed worker while item 0 was blocked: {progress_error:?}"
        );
        assert!(started.iter().any(|&sequence| sequence >= 3));
        assert!(
            matches!(
                over_budget_start,
                Some(Err(mpsc::RecvTimeoutError::Timeout))
            ),
            "memory backpressure admitted an item past the exact bound: {over_budget_start:?}"
        );

        let (result, written) = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("slow-first ordered pipeline deadlocked");
        handle.join().unwrap();
        let report = result.unwrap();
        assert_eq!(written, (0..12).collect::<Vec<_>>());
        assert_eq!(report.admitted, 12);
        assert_eq!(report.completed, 12);
        assert_eq!(report.max_active_workers, 3);
        assert!(report.max_pending_results >= 2);
        assert!(report.max_accounted_bytes <= 100);
    }

    #[test]
    fn bounded_ordered_pipeline_propagates_worker_error_and_drains_active_work() {
        let active = std::sync::Arc::new(AtomicUsize::new(0));
        let started = std::sync::Arc::new(AtomicUsize::new(0));
        let finished = std::sync::Arc::new(AtomicUsize::new(0));
        let worker_active = std::sync::Arc::clone(&active);
        let worker_started = std::sync::Arc::clone(&started);
        let worker_finished = std::sync::Arc::clone(&finished);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(3)
            .build()
            .unwrap();
        let mut written = Vec::new();
        let error = run_bounded_ordered_pipeline(
            &pool,
            10,
            3,
            75,
            |_| Ok(25),
            Ok,
            move |sequence| {
                worker_started.fetch_add(1, AtomicOrdering::SeqCst);
                worker_active.fetch_add(1, AtomicOrdering::SeqCst);
                if sequence != 1 {
                    std::thread::sleep(Duration::from_millis(20));
                }
                worker_active.fetch_sub(1, AtomicOrdering::SeqCst);
                worker_finished.fetch_add(1, AtomicOrdering::SeqCst);
                ensure!(sequence != 1, "synthetic worker failure");
                Ok(sequence)
            },
            |_| Ok(5),
            |_, value| {
                written.push(value);
                Ok(())
            },
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("synthetic worker failure"));
        assert_eq!(active.load(AtomicOrdering::SeqCst), 0);
        assert_eq!(started.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(finished.load(AtomicOrdering::SeqCst), 3);
        assert_eq!(written, vec![0]);
    }

    #[test]
    fn bounded_ordered_pipeline_fails_if_retained_bytes_exceed_the_reservation() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()
            .unwrap();
        let error = run_bounded_ordered_pipeline(
            &pool,
            1,
            1,
            4,
            |_| Ok(4),
            Ok,
            Ok,
            |_| Ok(5),
            |_, _: usize| Ok(()),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("retained 5 bytes after reserving only 4"));
    }

    #[test]
    fn count_only_subtraction_and_pass2_validation_fail_closed() {
        // ID 1 occurs once in an eligible field and once in an excluded field. ID 2 occurs once
        // in an eligible field. This is the exact two-phase count contract used by the converter.
        let declared_all = vec![2u32, 1];
        let mut canonical_eligible = declared_all.clone();
        merge_count_runs(&mut canonical_eligible, &[(1, 1)], true).unwrap();
        assert_eq!(canonical_eligible, vec![1, 1]);

        let mut all_remaining = declared_all.clone();
        let mut eligible_remaining = canonical_eligible.clone();
        merge_count_runs(&mut all_remaining, &[(1, 2), (2, 1)], true).unwrap();
        merge_count_runs(&mut eligible_remaining, &[(1, 1), (2, 1)], true).unwrap();
        validate_consumed_reference_counts(&all_remaining, &eligible_remaining).unwrap();

        // A declared count that is too large survives pass 2 and must stop publication.
        let mut wrong_all_remaining = vec![3u32, 1];
        merge_count_runs(&mut wrong_all_remaining, &[(1, 2), (2, 1)], true).unwrap();
        assert!(
            validate_consumed_reference_counts(&wrong_all_remaining, &eligible_remaining).is_err()
        );

        // A declared count that is too small fails during the count-only excluded subtraction.
        let mut underflow = vec![0u32];
        assert!(merge_count_runs(&mut underflow, &[(1, 1)], true).is_err());
    }

    #[test]
    fn previous_tail_decodes_full_legacy_length_even_when_divisible_by_40() {
        let bytes = vec![0xa5; crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY * 32];
        assert!(bytes.len().is_multiple_of(40));
        let tail = decode_previous_blockhash_tail_bytes(&bytes, 1).unwrap();
        assert_eq!(tail.len(), crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY);
        assert!(
            tail.iter()
                .all(|row| row.hash == [0xa5; 32] && row.slot == 0)
        );
    }

    #[test]
    fn previous_tail_rejects_a_genuinely_ambiguous_160_byte_payload() {
        let mut bytes = Vec::new();
        let start = crate::SLOTS_PER_EPOCH;
        for index in 0..4u64 {
            bytes.extend_from_slice(&[(index + 1) as u8; 32]);
            bytes.extend_from_slice(&(start + index).to_le_bytes());
        }
        assert_eq!(bytes.len(), 160);
        let error = decode_previous_blockhash_tail_bytes(&bytes, 2).unwrap_err();
        assert!(error.to_string().contains("ambiguous"));
    }

    #[test]
    fn previous_tail_accepts_unambiguous_current_capacity() {
        let mut bytes = Vec::new();
        for index in 0..crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY {
            bytes.extend_from_slice(&[(index % 251) as u8; 32]);
            bytes.extend_from_slice(&(index as u64).to_le_bytes());
        }
        assert_eq!(
            bytes.len(),
            crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY * 40
        );
        let tail = decode_previous_blockhash_tail_bytes(&bytes, 1).unwrap();
        assert_eq!(tail.len(), crate::archive_v2::ROLLING_BLOCKHASH_CAPACITY);
        assert_eq!(tail.first().unwrap().slot, 0);
        assert_eq!(tail.last().unwrap().slot, 299);
    }

    #[test]
    fn usage_sort_is_thread_deterministic_and_prefixes_missing_compute_budget() {
        let root = TestDir::new();
        let source_path = root.0.join("source-registry.bin");
        let key_a = [1u8; 32];
        let key_b = [2u8; 32];
        let excluded_only = [3u8; 32];
        let builtin = compute_budget_key();
        let source_bytes = [key_b, builtin, key_a, excluded_only].concat();
        fs::write(&source_path, source_bytes).unwrap();
        let source = MappedRegistry::open(&source_path).unwrap();
        let counts = [2u32, 0, 2, 0];
        let mut outputs = Vec::new();
        for threads in [1usize, 2] {
            let target = root.0.join(format!("target-{threads}"));
            fs::create_dir(&target).unwrap();
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .unwrap();
            let (mapping, keys) =
                build_usage_sorted_registry(&source, &counts, &target, 1, &pool).unwrap();
            assert_eq!(mapping, vec![3, 1, 2, 0]);
            assert_eq!(keys, 3);
            let mapped_target =
                MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)).unwrap();
            validate_registry_remap(&source, &mapping, &mapped_target).unwrap();
            let registry = fs::read(target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)).unwrap();
            let target_counts =
                read_registry_counts(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE), 3)
                    .unwrap();
            assert_eq!(
                registry,
                [builtin, key_a, key_b].concat(),
                "ComputeBudget must be the synthetic ID-1 prefix"
            );
            assert_eq!(target_counts, vec![0, 2, 2]);
            outputs.push((registry, target_counts));
        }
        assert_eq!(outputs[0], outputs[1]);
    }

    #[test]
    fn deep_counts_reject_internally_sorted_tampering_and_wrong_sum() {
        let root = TestDir::new();
        let source_path = root.0.join("source-registry.bin");
        let target = root.0.join("target");
        fs::create_dir(&target).unwrap();
        let key_a = [1u8; 32];
        let key_b = [2u8; 32];
        let builtin = compute_budget_key();
        fs::write(&source_path, [key_b, builtin, key_a].concat()).unwrap();
        fs::write(
            target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            [builtin, key_a, key_b].concat(),
        )
        .unwrap();
        fs::write(
            target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
            [0u8, 4, 3],
        )
        .unwrap();
        build_registry_index(&target).unwrap();
        assert!(validate_canonical_registry(&target, 3).is_ok());

        let source = MappedRegistry::open(&source_path).unwrap();
        let source_index = KeyIndex::build(source.keys().to_vec());
        let eligible = [4u32, 0, 5];
        assert!(
            validate_target_registry_against_recomputed(
                &source,
                &source_index,
                &eligible,
                &target,
                3,
                9,
            )
            .is_err(),
            "independently sorted but false target counts must be rejected"
        );

        fs::write(
            target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
            [0u8, 5, 4],
        )
        .unwrap();
        assert!(
            validate_target_registry_against_recomputed(
                &source,
                &source_index,
                &eligible,
                &target,
                3,
                8,
            )
            .is_err(),
            "receipt eligible-reference sum must be independently checked"
        );
        validate_target_registry_against_recomputed(
            &source,
            &source_index,
            &eligible,
            &target,
            3,
            9,
        )
        .unwrap();
    }

    #[test]
    fn target_probe_contract_rejects_first_seen_manifest() {
        let binding = FileBinding {
            bytes: 1,
            sha256: "00".repeat(32),
        };
        let mut files = BTreeMap::new();
        for name in [
            ARCHIVE_V2_BLOCKS_FILE,
            ARCHIVE_V2_BLOCK_INDEX_FILE,
            ARCHIVE_V2_META_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE,
            ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE,
            ARCHIVE_V2_SIGNATURES_FILE,
            ARCHIVE_V2_BLOCKHASH_REGISTRY_FILE,
            ARCHIVE_V2_POH_FILE,
            ARCHIVE_V2_SHREDDING_FILE,
            ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE,
        ] {
            files.insert(name.to_owned(), binding.clone());
        }
        assert!(validate_probe_core_files(&files, false, RECEIPT_VERSION_V2).is_err());
        assert!(validate_probe_core_files(&files, true, RECEIPT_VERSION_V2).is_ok());
        assert!(validate_probe_core_files(&files, true, RECEIPT_VERSION).is_err());
        for name in [
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ] {
            files.insert(name.to_owned(), binding.clone());
        }
        assert!(validate_probe_core_files(&files, true, RECEIPT_VERSION).is_ok());
        files.insert(ARCHIVE_V2_GET_BLOCK_INDEX_FILE.to_owned(), binding.clone());
        assert!(validate_probe_core_files(&files, true, RECEIPT_VERSION).is_err());
    }

    #[test]
    fn profile_neutral_v3_receipt_cannot_authorize_a_generation() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let mut receipt = run_two_stage(&source, &target, epoch, 1);
        receipt.wire_profile = None;
        receipt
            .target_files
            .remove(POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE);

        let error = validate_receipt_shape(&receipt, epoch).unwrap_err();
        assert!(format!("{error:#}").contains("omits Archive V2 wire profile"));
    }

    #[test]
    fn minimal_reprocess_publishes_v3_and_preserves_v1_probe_compatibility() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        let excluded = write_minimal_first_seen_source(&source, epoch);
        let receipt = run_two_stage(&source, &target, epoch, 1);
        assert_eq!(receipt.version, RECEIPT_VERSION);
        assert_eq!(receipt.algorithm, RECEIPT_ALGORITHM);
        assert!(receipt.source_semantics.is_none());
        assert!(receipt.target_semantics.is_none());
        assert_eq!(receipt.assembly_mode.as_deref(), Some(ACCESS_ASSEMBLY_MODE));
        assert_eq!(
            receipt.signature_provenance.as_deref(),
            Some(SIGNATURE_PROVENANCE)
        );
        assert!(receipt.access_boundary_repair.is_none());
        assert!(
            !serde_json::to_vec(&receipt)
                .unwrap()
                .windows(b"access_boundary_repair".len())
                .any(|window| window == b"access_boundary_repair"),
            "normal v3 receipts must stay byte-compatible and omit repair provenance"
        );
        assert_eq!(
            receipt.rewrite_stats,
            Some(RewriteStats {
                blocks: 1,
                transactions: 1,
                pubkey_references: 2,
            })
        );
        probe_published_reprocess(&target, epoch).unwrap();
        validate_published_reprocess(&source, &target, epoch).unwrap();

        let index =
            read_archive_v2_hot_block_index(&target.join(ARCHIVE_V2_BLOCK_INDEX_FILE)).unwrap();
        let input = read_compressed_block_batch(
            &mut File::open(target.join(ARCHIVE_V2_BLOCKS_FILE)).unwrap(),
            &index.rows,
            None,
        )
        .unwrap()
        .pop()
        .unwrap();
        let target_block = decode_hot_block(&input).unwrap();
        assert_eq!(
            target_block.header.rewards.unwrap().decoded[0].pubkey,
            CompactPubkey::raw(excluded)
        );

        let receipt_path = target.join(REGISTRY_REPROCESS_RECEIPT_FILE);
        let v2_bytes = fs::read(&receipt_path).unwrap();
        let mut v1 = receipt.clone();
        v1.version = RECEIPT_VERSION_V1;
        v1.algorithm = RECEIPT_ALGORITHM_V1.to_owned();
        let legacy_semantics = SemanticBinding {
            blocks: 1,
            transactions: 1,
            pubkey_references: 2,
            reference_sha256: "00".repeat(32),
            normalized_structure_sha256: "11".repeat(32),
        };
        v1.source_semantics = Some(legacy_semantics.clone());
        v1.target_semantics = Some(legacy_semantics);
        v1.rewrite_stats = None;
        v1.attempt_id = None;
        v1.handoff_sha256 = None;
        v1.assembly_mode = None;
        v1.signature_provenance = None;
        v1.wire_profile = None;
        v1.target_files
            .remove(POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE);
        v1.target_generation_sha256 = generation_digest(&v1.target_files);
        fs::remove_file(target.join(POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE)).unwrap();
        let mut v1_bytes = serde_json::to_vec_pretty(&v1).unwrap();
        v1_bytes.push(b'\n');
        fs::write(&receipt_path, v1_bytes).unwrap();
        probe_published_reprocess(&target, epoch).unwrap();
        fs::write(
            target.join(POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE),
            wire_profile_marker_bytes(TEST_WIRE_PROFILE),
        )
        .unwrap();
        fs::write(receipt_path, v2_bytes).unwrap();
    }

    #[test]
    fn minimal_reprocess_is_byte_exact_with_one_and_six_workers() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let mut receipts = Vec::new();
        for threads in [1usize, 6] {
            let target = root.0.join(format!("target-{threads}"));
            receipts.push(run_two_stage(&source, &target, epoch, threads));
        }
        assert_eq!(receipts[0].target_files, receipts[1].target_files);
        assert_eq!(
            receipts[0].target_generation_sha256,
            receipts[1].target_generation_sha256
        );
        assert_eq!(receipts[0].rewrite_stats, receipts[1].rewrite_stats);
    }

    #[test]
    fn two_stage_completion_accepts_legacy_v1_and_v2_no_votes_access() {
        let root = TestDir::new();
        let epoch = 1;
        for version in [1u16, 2] {
            let source = root.0.join(format!("source-v{version}"));
            let target = root.0.join(format!("target-v{version}"));
            write_minimal_first_seen_source(&source, epoch);
            rewrite_source_access_as_legacy(&source, epoch, version);

            let receipt = run_two_stage(&source, &target, epoch, 1);
            assert_eq!(receipt.version, RECEIPT_VERSION);
            probe_published_reprocess(&target, epoch).unwrap();
            validate_published_reprocess(&source, &target, epoch).unwrap();
        }
    }

    #[test]
    fn epoch_301_boundary_gate_is_exact_and_normal_rows_are_byte_unchanged() {
        let original = hash_from_hex(EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX);
        let corrected = hash_from_hex(EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX);
        let trusted = super::super::PreviousBlockhash {
            hash: corrected,
            slot: EPOCH_301_ACCESS_BOUNDARY_REPAIR_PREDECESSOR_SLOT,
        };
        let row = ArchiveV2BlockAccessIndexRow {
            block_id: EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID,
            slot: EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT,
            access_offset: 0,
            access_len: 1,
            tx_count: 0,
            signature_count: 0,
        };
        let make_blob = |previous_blockhash| ArchiveV2BlockAccessBlob {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: 0,
            blockhash: original,
            previous_blockhash,
            signature_counts: Vec::new(),
            signatures: Vec::new(),
            pubkeys: Vec::new(),
            blockhashes: Vec::new(),
            vote_hashes: Vec::new(),
        };

        let mut normal = make_blob(corrected);
        let normal_before = wincode::config::serialize(&normal, wincode_leb128_config()).unwrap();
        assert!(
            validate_and_repair_access_previous_blockhash(
                EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH,
                0,
                &row,
                &trusted,
                corrected,
                None,
                &mut normal,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            wincode::config::serialize(&normal, wincode_leb128_config()).unwrap(),
            normal_before,
            "the normal path must not change any access byte"
        );

        let evidence = exact_epoch_301_boundary_evidence();
        let mut receipt_source_files = BTreeMap::from([
            (
                ARCHIVE_V2_PREV_BLOCKHASH_TAIL_FILE.to_owned(),
                evidence.tail_binding.clone(),
            ),
            (
                ARCHIVE_V2_FIRST_SEEN_REGISTRY_MANIFEST_FILE.to_owned(),
                FileBinding {
                    bytes: 1,
                    sha256: evidence.manifest_sha256.clone(),
                },
            ),
            (
                ARCHIVE_V2_BLOCK_ACCESS_FILE.to_owned(),
                FileBinding {
                    bytes: evidence.source_blob_bytes,
                    sha256: EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_SHA256.to_owned(),
                },
            ),
            (
                ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE.to_owned(),
                evidence.source_index_binding.clone(),
            ),
        ]);
        validate_access_boundary_repair_source_bindings(&receipt_source_files).unwrap();
        receipt_source_files
            .get_mut(ARCHIVE_V2_BLOCK_ACCESS_FILE)
            .unwrap()
            .sha256 = "00".repeat(32);
        assert!(
            validate_access_boundary_repair_source_bindings(&receipt_source_files).is_err(),
            "repaired receipt must pin the complete source access blob digest"
        );
        receipt_source_files
            .get_mut(ARCHIVE_V2_BLOCK_ACCESS_FILE)
            .unwrap()
            .sha256 = EPOCH_301_ACCESS_BOUNDARY_REPAIR_SOURCE_BLOB_SHA256.to_owned();
        receipt_source_files
            .get_mut(ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE)
            .unwrap()
            .sha256 = "00".repeat(32);
        assert!(
            validate_access_boundary_repair_source_bindings(&receipt_source_files).is_err(),
            "repaired receipt must retain the pinned source identity"
        );
        let mut legacy = make_blob(original);
        let repair = validate_and_repair_access_previous_blockhash(
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH,
            0,
            &row,
            &trusted,
            corrected,
            Some(&evidence),
            &mut legacy,
        )
        .unwrap()
        .unwrap();
        assert_eq!(legacy.previous_blockhash, corrected);
        assert_eq!(repair.mode, EPOCH_301_ACCESS_BOUNDARY_REPAIR_MODE);
        assert_eq!(
            repair.original_previous_blockhash_hex,
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX
        );
        assert_eq!(
            repair.corrected_previous_blockhash_hex,
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX
        );
        validate_access_boundary_repair_shape(&repair, EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH)
            .unwrap();
        let repair_json = serde_json::to_value(&repair).unwrap();
        assert_eq!(
            repair_json["original_previous_blockhash_hex"],
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX
        );
        assert_eq!(
            repair_json["corrected_previous_blockhash_hex"],
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX
        );
        let mut tampered_repair = repair.clone();
        tampered_repair.corrected_previous_blockhash_hex =
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX.to_owned();
        assert!(
            validate_access_boundary_repair_shape(
                &tampered_repair,
                EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH,
            )
            .is_err(),
            "durable repair provenance must be exact"
        );
        let source_for_deep = make_blob(original);
        let target_for_deep = make_blob(corrected);
        assert!(access_non_pubkey_fields_except_previous_equal(
            &source_for_deep,
            &target_for_deep
        ));
        assert!(access_previous_blockhashes_match_receipt(
            &row,
            &source_for_deep,
            &target_for_deep,
            Some(&repair),
        ));
        let mut wrong_target_previous = make_blob(corrected);
        wrong_target_previous.previous_blockhash = [0x55; 32];
        assert!(!access_previous_blockhashes_match_receipt(
            &row,
            &source_for_deep,
            &wrong_target_previous,
            Some(&repair),
        ));
        assert!(
            !access_previous_blockhashes_match_receipt(
                &row,
                &source_for_deep,
                &target_for_deep,
                None,
            ),
            "deep validation must require receipt provenance for the exceptional difference"
        );
        let row_0_target_hash = advance_trusted_target_access_chain(
            corrected,
            &target_for_deep,
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID,
        )
        .unwrap();
        assert_eq!(row_0_target_hash, original);
        let reverted_row_0 = make_blob(original);
        assert!(
            advance_trusted_target_access_chain(
                corrected,
                &reverted_row_0,
                EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID,
            )
            .is_err(),
            "removing provenance and reverting row 0 must still break deep chain validation"
        );
        let valid_row_1 = ArchiveV2BlockAccessBlob {
            blockhash: hash_from_hex(EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCKHASH_HEX),
            previous_blockhash: original,
            ..make_blob(original)
        };
        assert!(advance_trusted_target_access_chain(row_0_target_hash, &valid_row_1, 1).is_ok());
        let mutated_later_row = ArchiveV2BlockAccessBlob {
            previous_blockhash: [0x66; 32],
            ..valid_row_1
        };
        assert!(
            advance_trusted_target_access_chain(row_0_target_hash, &mutated_later_row, 1).is_err(),
            "a later-row previous-blockhash mutation must fail deep validation"
        );

        let mut wrong_epoch = make_blob(original);
        assert!(
            validate_and_repair_access_previous_blockhash(
                EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH + 1,
                0,
                &row,
                &trusted,
                corrected,
                Some(&evidence),
                &mut wrong_epoch,
            )
            .is_err()
        );
        assert_eq!(wrong_epoch.previous_blockhash, original);

        let mut wrong_evidence = evidence.clone();
        wrong_evidence.row_0_frame_sha256 = "00".repeat(32);
        let mut wrong_generation = make_blob(original);
        assert!(
            validate_and_repair_access_previous_blockhash(
                EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH,
                0,
                &row,
                &trusted,
                corrected,
                Some(&wrong_evidence),
                &mut wrong_generation,
            )
            .is_err(),
            "a different epoch-301 generation must fail the pinned identity gate"
        );
        assert_eq!(wrong_generation.previous_blockhash, original);

        let row_1 = ArchiveV2BlockAccessIndexRow {
            block_id: 1,
            slot: row.slot + 1,
            ..row
        };
        let mut broken_row_1 = ArchiveV2BlockAccessBlob {
            blockhash: [0x77; 32],
            previous_blockhash: [0x99; 32],
            ..make_blob([0x99; 32])
        };
        assert!(
            validate_and_repair_access_previous_blockhash(
                EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH,
                1,
                &row_1,
                &trusted,
                original,
                None,
                &mut broken_row_1,
            )
            .is_err(),
            "a broken row-1 chain must not use the row-0 repair"
        );
    }

    #[test]
    fn v3_deep_chain_rejects_repair_downgrade_and_later_row_mutation() {
        let original = hash_from_hex(EPOCH_301_ACCESS_BOUNDARY_REPAIR_ORIGINAL_HEX);
        let corrected = hash_from_hex(EPOCH_301_ACCESS_BOUNDARY_REPAIR_CORRECTED_HEX);
        let blob = |blockhash, previous_blockhash| ArchiveV2BlockAccessBlob {
            version: WINCODE_ARCHIVE_V2_BLOCK_ACCESS_VERSION,
            flags: 0,
            blockhash,
            previous_blockhash,
            signature_counts: Vec::new(),
            signatures: Vec::new(),
            pubkeys: Vec::new(),
            blockhashes: Vec::new(),
            vote_hashes: Vec::new(),
        };

        let repaired_row_0 = blob(original, corrected);
        let source_row_0 = blob(original, original);
        let row_0_index = ArchiveV2BlockAccessIndexRow {
            block_id: EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID,
            slot: EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_SLOT,
            access_offset: 0,
            access_len: 1,
            tx_count: 0,
            signature_count: 0,
        };
        assert!(
            !access_previous_blockhashes_match_receipt(
                &row_0_index,
                &source_row_0,
                &repaired_row_0,
                None,
            ),
            "stripping repair provenance from a corrected target must fail source parity"
        );
        let row_0_hash = advance_trusted_target_access_chain(
            corrected,
            &repaired_row_0,
            EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID,
        )
        .unwrap();
        assert_eq!(row_0_hash, original);

        let downgraded_row_0 = blob(original, original);
        assert!(
            advance_trusted_target_access_chain(
                corrected,
                &downgraded_row_0,
                EPOCH_301_ACCESS_BOUNDARY_REPAIR_BLOCK_ID,
            )
            .is_err(),
            "removing repair provenance and reverting row 0 must fail the unconditional chain"
        );

        let row_1_hash = hash_from_hex(EPOCH_301_ACCESS_BOUNDARY_REPAIR_ROW_1_BLOCKHASH_HEX);
        let valid_row_1 = blob(row_1_hash, row_0_hash);
        assert_eq!(
            advance_trusted_target_access_chain(row_0_hash, &valid_row_1, 1).unwrap(),
            row_1_hash
        );
        let mutated_row_1 = blob(row_1_hash, [0x66; 32]);
        assert!(
            advance_trusted_target_access_chain(row_0_hash, &mutated_row_1, 1).is_err(),
            "a later-row previous-blockhash mutation must fail the unconditional chain"
        );
    }

    #[test]
    fn epoch_301_nonproduction_lookalike_fails_without_source_mutation() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        write_epoch_301_legacy_boundary_bug_source(&source);
        let source_access_before = fs::read(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE)).unwrap();
        let attempt_id = "epoch-301-lookalike".to_owned();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();
        let core = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source.clone(),
            target_dir: target.clone(),
            epoch: EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
            attempt_id: attempt_id.clone(),
            staging_dir: staging.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        let error = complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: source.clone(),
            staging_dir: staging.clone(),
            target_dir: target.clone(),
            epoch: EPOCH_301_ACCESS_BOUNDARY_REPAIR_EPOCH,
            attempt_id,
            handoff_sha256: core.handoff_sha256,
            expected_continuation_state:
                RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("epoch-301 repair source has no row 1"),
            "unexpected strict-gate error: {error:#}"
        );
        assert_eq!(
            fs::read(source.join(ARCHIVE_V2_BLOCK_ACCESS_FILE)).unwrap(),
            source_access_before,
            "the one-time repair must not mutate the legacy source"
        );
        assert!(!target.exists(), "a lookalike generation must not publish");
        assert!(!staging.join(REGISTRY_REPROCESS_RECEIPT_FILE).exists());
    }

    #[test]
    fn two_stage_completion_derives_target_get_block_when_source_index_is_absent() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        fs::remove_file(source.join(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)).unwrap();

        let receipt = run_two_stage(&source, &target, epoch, 1);
        assert!(
            !receipt
                .source_files
                .contains_key(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)
        );
        assert!(
            receipt
                .target_files
                .contains_key(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)
        );
        validate_published_reprocess(&source, &target, epoch).unwrap();
    }

    #[test]
    fn two_stage_completion_accepts_synthetic_compute_budget_prefix() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        let excluded = write_minimal_first_seen_source(&source, epoch);
        let retained = [0x55; 32];
        fs::write(
            source.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE),
            [retained, excluded].concat(),
        )
        .unwrap();
        fs::remove_file(source.join(ARCHIVE_V2_PUBKEY_REGISTRY_INDEX_FILE)).unwrap();
        build_registry_index(&source).unwrap();
        write_valid_source_access_artifacts(&source, epoch, retained);

        let receipt = run_two_stage(&source, &target, epoch, 1);
        let registry = MappedRegistry::open(&target.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE)).unwrap();
        let counts = read_registry_counts(
            &target.join(ARCHIVE_V2_PUBKEY_REGISTRY_COUNTS_FILE),
            registry.len,
        )
        .unwrap();
        assert_eq!(registry.keys(), &[compute_budget_key(), retained]);
        assert_eq!(counts, [0, 1]);
        assert_eq!(receipt.target_registry_keys, 2);
        validate_published_reprocess(&source, &target, epoch).unwrap();
    }

    #[test]
    fn registry_core_fails_fast_when_signature_link_metadata_is_invalid() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        fs::remove_file(source.join(ARCHIVE_V2_SIGNATURES_FILE)).unwrap();
        let attempt_id = "missing-signature-core".to_owned();
        let staging_dir = expected_staging_path(&target, &attempt_id).unwrap();

        let error = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source.clone(),
            target_dir: target.clone(),
            epoch,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
            attempt_id: attempt_id.clone(),
            staging_dir: staging_dir.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap_err();

        assert!(format!("{error:#}").contains("signature sidecar cannot be linked"));
        assert!(!target.exists());
        assert!(staging_dir.join(REPROCESS_CHECKPOINT_FILE).is_file());
        assert!(!staging_dir.join(REPROCESS_HANDOFF_FILE).exists());
        assert!(!staging_dir.join(ARCHIVE_V2_BLOCK_ACCESS_FILE).exists());
    }

    #[test]
    fn access_continuation_state_classifies_core_owned_partials_and_corrupt_temp_for_rebuild() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let attempt_id = "classified-core-partial".to_owned();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();
        let core = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source.clone(),
            target_dir: target.clone(),
            epoch,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
            attempt_id: attempt_id.clone(),
            staging_dir: staging.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();

        let classify = || {
            probe_registry_reprocess_access_continuation_state(
                &staging,
                &source,
                &target,
                epoch,
                &attempt_id,
                &core.handoff_sha256,
                TEST_WIRE_PROFILE,
            )
        };
        let initial = classify().unwrap();
        assert_eq!(
            initial.state,
            RegistryReprocessAccessContinuationState::CoreOrPartialRebuild
        );
        assert_eq!(initial.core_result, core);

        let owned_partial = staging.join(format!(
            "{ARCHIVE_V2_BLOCK_ACCESS_FILE}{ACCESS_TEMP_SUFFIX}"
        ));
        fs::write(&owned_partial, b"owned partial").unwrap();
        assert_eq!(
            classify().unwrap().state,
            RegistryReprocessAccessContinuationState::CoreOrPartialRebuild
        );

        fs::write(
            staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE),
            b"{not-json",
        )
        .unwrap();
        assert_eq!(
            classify().unwrap().state,
            RegistryReprocessAccessContinuationState::CoreOrPartialRebuild
        );

        fs::write(staging.join("unknown.partial"), b"not owned").unwrap();
        assert!(classify().is_err());
    }

    #[test]
    fn access_continuation_state_classifies_valid_final_and_temporary_receipts_as_ready() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let receipt = run_two_stage(&source, &target, epoch, 1);
        let attempt_id = receipt.attempt_id.clone().unwrap();
        let handoff_sha256 = receipt.handoff_sha256.clone().unwrap();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();
        fs::rename(&target, &staging).unwrap();

        let final_receipt = probe_registry_reprocess_access_continuation_state(
            &staging,
            &source,
            &target,
            epoch,
            &attempt_id,
            &handoff_sha256,
            TEST_WIRE_PROFILE,
        )
        .unwrap();
        assert_eq!(
            final_receipt.state,
            RegistryReprocessAccessContinuationState::ReceiptReady
        );
        assert_eq!(final_receipt.core_result.handoff_sha256, handoff_sha256);

        fs::rename(
            staging.join(REGISTRY_REPROCESS_RECEIPT_FILE),
            staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        }
        assert_eq!(
            probe_registry_reprocess_access_continuation_state(
                &staging,
                &source,
                &target,
                epoch,
                &attempt_id,
                &handoff_sha256,
                TEST_WIRE_PROFILE,
            )
            .unwrap()
            .state,
            RegistryReprocessAccessContinuationState::ReceiptReady
        );
    }

    #[test]
    fn access_continuation_state_routes_same_length_corrupt_temp_to_core_rebuild() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let backup = root.0.join("core-backup");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let attempt_id = "corrupt-bound-temp".to_owned();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();
        let core = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source.clone(),
            target_dir: target.clone(),
            epoch,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
            attempt_id: attempt_id.clone(),
            staging_dir: staging.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        fs::create_dir(&backup).unwrap();
        for name in [
            REPROCESS_CHECKPOINT_FILE,
            REPROCESS_HANDOFF_FILE,
            REPROCESS_REMAP_FILE,
            SOURCE_REGISTRY_SNAPSHOT_FILE,
        ] {
            fs::copy(staging.join(name), backup.join(name)).unwrap();
        }
        complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: source.clone(),
            staging_dir: staging.clone(),
            target_dir: target.clone(),
            epoch,
            attempt_id: attempt_id.clone(),
            handoff_sha256: core.handoff_sha256.clone(),
            expected_continuation_state:
                RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        fs::rename(&target, &staging).unwrap();
        for name in [
            REPROCESS_CHECKPOINT_FILE,
            REPROCESS_HANDOFF_FILE,
            REPROCESS_REMAP_FILE,
            SOURCE_REGISTRY_SNAPSHOT_FILE,
        ] {
            fs::copy(backup.join(name), staging.join(name)).unwrap();
        }
        fs::rename(
            staging.join(REGISTRY_REPROCESS_RECEIPT_FILE),
            staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let access_path = staging.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
        let mut access_bytes = fs::read(&access_path).unwrap();
        access_bytes[0] ^= 0x80;
        fs::write(&access_path, access_bytes).unwrap();

        let classified = probe_registry_reprocess_access_continuation_state(
            &staging,
            &source,
            &target,
            epoch,
            &attempt_id,
            &core.handoff_sha256,
            TEST_WIRE_PROFILE,
        )
        .unwrap();
        assert_eq!(
            classified.state,
            RegistryReprocessAccessContinuationState::CoreOrPartialRebuild
        );
        assert_eq!(classified.core_result, core);
    }

    #[test]
    fn access_continuation_state_rejects_same_length_corrupt_final_receipt_binding() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let receipt = run_two_stage(&source, &target, epoch, 1);
        let attempt_id = receipt.attempt_id.clone().unwrap();
        let handoff_sha256 = receipt.handoff_sha256.clone().unwrap();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();
        fs::rename(&target, &staging).unwrap();
        let access_path = staging.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
        let mut access_bytes = fs::read(&access_path).unwrap();
        access_bytes[0] ^= 0x80;
        fs::write(&access_path, access_bytes).unwrap();

        assert!(
            probe_registry_reprocess_access_continuation_state(
                &staging,
                &source,
                &target,
                epoch,
                &attempt_id,
                &handoff_sha256,
                TEST_WIRE_PROFILE,
            )
            .is_err()
        );
    }

    #[test]
    fn access_expected_continuation_state_rejects_both_mismatch_directions() {
        let root = TestDir::new();
        let epoch = 1;

        let core_source = root.0.join("core-source");
        let core_target = root.0.join("core-target");
        write_minimal_first_seen_source(&core_source, epoch);
        let core_attempt_id = "expected-receipt-on-core".to_owned();
        let core_staging = expected_staging_path(&core_target, &core_attempt_id).unwrap();
        let core = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: core_source.clone(),
            target_dir: core_target.clone(),
            epoch,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
            attempt_id: core_attempt_id.clone(),
            staging_dir: core_staging.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        let error = complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: core_source,
            staging_dir: core_staging.clone(),
            target_dir: core_target.clone(),
            epoch,
            attempt_id: core_attempt_id,
            handoff_sha256: core.handoff_sha256,
            expected_continuation_state: RegistryReprocessAccessContinuationState::ReceiptReady,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap_err();
        assert!(error.to_string().contains("expected receipt-ready"));
        assert!(core_staging.join(REPROCESS_HANDOFF_FILE).is_file());
        assert!(!core_target.exists());

        let receipt_source = root.0.join("receipt-source");
        let receipt_target = root.0.join("receipt-target");
        write_minimal_first_seen_source(&receipt_source, epoch);
        let receipt = run_two_stage(&receipt_source, &receipt_target, epoch, 1);
        let receipt_attempt_id = receipt.attempt_id.clone().unwrap();
        let receipt_handoff_sha256 = receipt.handoff_sha256.clone().unwrap();
        let receipt_staging = expected_staging_path(&receipt_target, &receipt_attempt_id).unwrap();

        // An exact published v3 attempt stays idempotent, independent of the staging charge.
        assert_eq!(
            complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
                source_dir: receipt_source.clone(),
                staging_dir: receipt_staging.clone(),
                target_dir: receipt_target.clone(),
                epoch,
                attempt_id: receipt_attempt_id.clone(),
                handoff_sha256: receipt_handoff_sha256.clone(),
                expected_continuation_state:
                    RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
                wire_profile: TEST_WIRE_PROFILE,
                wire_profile_authority_receipt: None,
            })
            .unwrap(),
            receipt
        );

        fs::rename(&receipt_target, &receipt_staging).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&receipt_staging, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let error = complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: receipt_source,
            staging_dir: receipt_staging.clone(),
            target_dir: receipt_target.clone(),
            epoch,
            attempt_id: receipt_attempt_id,
            handoff_sha256: receipt_handoff_sha256,
            expected_continuation_state:
                RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("expected core-or-partial-rebuild")
        );
        assert!(
            receipt_staging
                .join(REGISTRY_REPROCESS_RECEIPT_FILE)
                .is_file()
        );
        assert!(!receipt_target.exists());
    }

    #[test]
    fn access_expected_receipt_state_fails_closed_across_deletion_and_mutation() {
        let root = TestDir::new();
        let epoch = 1;

        for mutation in ["delete-final", "corrupt-temp-binding"] {
            let source = root.0.join(format!("source-{mutation}"));
            let target = root.0.join(format!("target-{mutation}"));
            write_minimal_first_seen_source(&source, epoch);
            let receipt = run_two_stage(&source, &target, epoch, 1);
            let attempt_id = receipt.attempt_id.clone().unwrap();
            let handoff_sha256 = receipt.handoff_sha256.clone().unwrap();
            let staging = expected_staging_path(&target, &attempt_id).unwrap();
            fs::rename(&target, &staging).unwrap();
            if mutation == "corrupt-temp-binding" {
                fs::rename(
                    staging.join(REGISTRY_REPROCESS_RECEIPT_FILE),
                    staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE),
                )
                .unwrap();
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
            }
            assert_eq!(
                probe_registry_reprocess_access_continuation_state(
                    &staging,
                    &source,
                    &target,
                    epoch,
                    &attempt_id,
                    &handoff_sha256,
                    TEST_WIRE_PROFILE,
                )
                .unwrap()
                .state,
                RegistryReprocessAccessContinuationState::ReceiptReady
            );

            if mutation == "delete-final" {
                fs::remove_file(staging.join(REGISTRY_REPROCESS_RECEIPT_FILE)).unwrap();
            } else {
                let access_path = staging.join(ARCHIVE_V2_BLOCK_ACCESS_FILE);
                let mut access = fs::read(&access_path).unwrap();
                access[0] ^= 0x80;
                fs::write(access_path, access).unwrap();
            }

            assert!(
                complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
                    source_dir: source,
                    staging_dir: staging.clone(),
                    target_dir: target.clone(),
                    epoch,
                    attempt_id,
                    handoff_sha256,
                    expected_continuation_state:
                        RegistryReprocessAccessContinuationState::ReceiptReady,
                    wire_profile: TEST_WIRE_PROFILE,
                    wire_profile_authority_receipt: None,
                })
                .is_err()
            );
            assert!(!target.exists());
            if mutation == "corrupt-temp-binding" {
                assert!(staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE).is_file());
                assert!(staging.join(ARCHIVE_V2_BLOCK_ACCESS_FILE).is_file());
            }
        }
    }

    #[test]
    fn access_continuation_accepts_only_owned_partials_and_core_probe_stays_strict() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let attempt_id = "partial-access".to_owned();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();
        let core = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source.clone(),
            target_dir: target.clone(),
            epoch,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
            attempt_id: attempt_id.clone(),
            staging_dir: staging.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            fs::set_permissions(
                &staging,
                fs::Permissions::from_mode(PUBLISHED_GENERATION_MODE),
            )
            .unwrap();
            assert!(
                probe_registry_reprocess_access_continuation(
                    &staging,
                    &source,
                    &target,
                    epoch,
                    &attempt_id,
                    &core.handoff_sha256,
                    TEST_WIRE_PROFILE,
                )
                .is_err()
            );
            assert!(
                complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
                    source_dir: source.clone(),
                    staging_dir: staging.clone(),
                    target_dir: target.clone(),
                    epoch,
                    attempt_id: attempt_id.clone(),
                    handoff_sha256: core.handoff_sha256.clone(),
                    expected_continuation_state:
                        RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
                    wire_profile: TEST_WIRE_PROFILE,
                    wire_profile_authority_receipt: None,
                })
                .is_err()
            );
            fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
            assert_eq!(fs::metadata(&staging).unwrap().mode() & 0o777, 0o700);
        }

        for name in phase2_output_names() {
            let path = staging.join(name);
            fs::write(&path, b"partial").unwrap();
            assert!(
                probe_registry_reprocess_core_handoff(
                    &staging,
                    &source,
                    &target,
                    epoch,
                    &attempt_id,
                    TEST_WIRE_PROFILE,
                )
                .is_err()
            );
            probe_registry_reprocess_access_continuation(
                &staging,
                &source,
                &target,
                epoch,
                &attempt_id,
                &core.handoff_sha256,
                TEST_WIRE_PROFILE,
            )
            .unwrap();
            fs::remove_file(path).unwrap();
        }

        let unknown = staging.join("unexpected.partial");
        fs::write(&unknown, b"partial").unwrap();
        assert!(
            probe_registry_reprocess_access_continuation(
                &staging,
                &source,
                &target,
                epoch,
                &attempt_id,
                &core.handoff_sha256,
                TEST_WIRE_PROFILE,
            )
            .is_err()
        );
        assert!(
            reprocess_first_seen_registry(&RegistryReprocessOptions {
                source_dir: source.clone(),
                target_dir: target.clone(),
                epoch,
                threads: 1,
                sort_memory_mib: 1,
                level: 1,
                attempt_id: attempt_id.clone(),
                staging_dir: staging.clone(),
                wire_profile: TEST_WIRE_PROFILE,
                wire_profile_authority_receipt: None,
            })
            .is_err()
        );
        assert!(
            complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
                source_dir: source.clone(),
                staging_dir: staging.clone(),
                target_dir: target.clone(),
                epoch,
                attempt_id: attempt_id.clone(),
                handoff_sha256: core.handoff_sha256.clone(),
                expected_continuation_state:
                    RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
                wire_profile: TEST_WIRE_PROFILE,
                wire_profile_authority_receipt: None,
            })
            .is_err()
        );
        assert!(unknown.is_file());
        assert!(!target.exists());
        fs::remove_file(unknown).unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(
                source.join(ARCHIVE_V2_SIGNATURES_FILE),
                staging.join(ARCHIVE_V2_SIGNATURES_FILE),
            )
            .unwrap();
            assert!(
                probe_registry_reprocess_access_continuation(
                    &staging,
                    &source,
                    &target,
                    epoch,
                    &attempt_id,
                    &core.handoff_sha256,
                    TEST_WIRE_PROFILE,
                )
                .is_err()
            );
            fs::remove_file(staging.join(ARCHIVE_V2_SIGNATURES_FILE)).unwrap();
        }

        fs::write(
            staging.join(format!(
                "{ARCHIVE_V2_BLOCK_ACCESS_FILE}{ACCESS_TEMP_SUFFIX}"
            )),
            b"partial",
        )
        .unwrap();
        complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: source.clone(),
            staging_dir: staging,
            target_dir: target.clone(),
            epoch,
            attempt_id,
            handoff_sha256: core.handoff_sha256,
            expected_continuation_state:
                RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        assert!(target.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                fs::metadata(&target).unwrap().mode() & 0o777,
                PUBLISHED_GENERATION_MODE
            );
        }
    }

    #[test]
    fn access_continuation_accepts_bound_final_and_temporary_receipts() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let receipt = run_two_stage(&source, &target, epoch, 1);
        let attempt_id = receipt.attempt_id.clone().unwrap();
        let handoff_sha256 = receipt.handoff_sha256.clone().unwrap();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();
        fs::rename(&target, &staging).unwrap();

        probe_registry_reprocess_access_continuation(
            &staging,
            &source,
            &target,
            epoch,
            &attempt_id,
            &handoff_sha256,
            TEST_WIRE_PROFILE,
        )
        .unwrap();
        let unknown = staging.join("unexpected-after-receipt");
        fs::write(&unknown, b"unexpected").unwrap();
        assert!(
            probe_registry_reprocess_access_continuation(
                &staging,
                &source,
                &target,
                epoch,
                &attempt_id,
                &handoff_sha256,
                TEST_WIRE_PROFILE,
            )
            .is_err()
        );
        assert!(
            complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
                source_dir: source.clone(),
                staging_dir: staging.clone(),
                target_dir: target.clone(),
                epoch,
                attempt_id: attempt_id.clone(),
                handoff_sha256: handoff_sha256.clone(),
                expected_continuation_state: RegistryReprocessAccessContinuationState::ReceiptReady,
                wire_profile: TEST_WIRE_PROFILE,
                wire_profile_authority_receipt: None,
            })
            .is_err()
        );
        assert!(unknown.is_file());
        fs::remove_file(unknown).unwrap();
        fs::rename(
            staging.join(REGISTRY_REPROCESS_RECEIPT_FILE),
            staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert!(
                probe_registry_reprocess_access_continuation(
                    &staging,
                    &source,
                    &target,
                    epoch,
                    &attempt_id,
                    &handoff_sha256,
                    TEST_WIRE_PROFILE,
                )
                .is_err()
            );
            assert!(
                complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
                    source_dir: source.clone(),
                    staging_dir: staging.clone(),
                    target_dir: target.clone(),
                    epoch,
                    attempt_id: attempt_id.clone(),
                    handoff_sha256: handoff_sha256.clone(),
                    expected_continuation_state:
                        RegistryReprocessAccessContinuationState::ReceiptReady,
                    wire_profile: TEST_WIRE_PROFILE,
                    wire_profile_authority_receipt: None,
                })
                .is_err()
            );
            fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        }
        probe_registry_reprocess_access_continuation(
            &staging,
            &source,
            &target,
            epoch,
            &attempt_id,
            &handoff_sha256,
            TEST_WIRE_PROFILE,
        )
        .unwrap();
        complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: source.clone(),
            staging_dir: staging,
            target_dir: target.clone(),
            epoch,
            attempt_id,
            handoff_sha256,
            expected_continuation_state: RegistryReprocessAccessContinuationState::ReceiptReady,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        assert!(target.join(REGISTRY_REPROCESS_RECEIPT_FILE).is_file());
        fs::write(target.join("undeclared"), b"unexpected").unwrap();
        assert!(probe_published_reprocess(&target, epoch).is_err());
        fs::remove_file(target.join("undeclared")).unwrap();
        probe_published_reprocess(&target, epoch).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
            assert!(probe_published_reprocess(&target, epoch).is_err());
            assert!(validate_published_reprocess(&source, &target, epoch).is_err());
            fs::set_permissions(
                &target,
                fs::Permissions::from_mode(PUBLISHED_GENERATION_MODE),
            )
            .unwrap();
            probe_published_reprocess(&target, epoch).unwrap();
        }
    }

    #[test]
    fn valid_receipt_temp_with_owned_partial_rebuilds_when_core_is_intact() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let backup = root.0.join("core-backup");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let attempt_id = "receipt-temp-partial".to_owned();
        let staging = expected_staging_path(&target, &attempt_id).unwrap();
        let core = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source.clone(),
            target_dir: target.clone(),
            epoch,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
            attempt_id: attempt_id.clone(),
            staging_dir: staging.clone(),
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        fs::create_dir(&backup).unwrap();
        for name in [
            REPROCESS_CHECKPOINT_FILE,
            REPROCESS_HANDOFF_FILE,
            REPROCESS_REMAP_FILE,
            SOURCE_REGISTRY_SNAPSHOT_FILE,
        ] {
            fs::copy(staging.join(name), backup.join(name)).unwrap();
        }
        complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: source.clone(),
            staging_dir: staging.clone(),
            target_dir: target.clone(),
            epoch,
            attempt_id: attempt_id.clone(),
            handoff_sha256: core.handoff_sha256.clone(),
            expected_continuation_state:
                RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        fs::rename(&target, &staging).unwrap();
        for name in [
            REPROCESS_CHECKPOINT_FILE,
            REPROCESS_HANDOFF_FILE,
            REPROCESS_REMAP_FILE,
            SOURCE_REGISTRY_SNAPSHOT_FILE,
        ] {
            fs::copy(backup.join(name), staging.join(name)).unwrap();
        }
        fs::rename(
            staging.join(REGISTRY_REPROCESS_RECEIPT_FILE),
            staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&staging, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::write(
            staging.join(format!(
                "{ARCHIVE_V2_BLOCK_ACCESS_FILE}{ACCESS_TEMP_SUFFIX}"
            )),
            b"partial",
        )
        .unwrap();

        probe_registry_reprocess_access_continuation(
            &staging,
            &source,
            &target,
            epoch,
            &attempt_id,
            &core.handoff_sha256,
            TEST_WIRE_PROFILE,
        )
        .unwrap();
        complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
            source_dir: source,
            staging_dir: staging,
            target_dir: target.clone(),
            epoch,
            attempt_id,
            handoff_sha256: core.handoff_sha256,
            expected_continuation_state:
                RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap();
        probe_published_reprocess(&target, epoch).unwrap();
    }

    #[test]
    fn corrupt_receipt_temp_recovers_only_with_an_exact_core_handoff() {
        let root = TestDir::new();
        let epoch = 1;
        for recoverable in [true, false] {
            let source = root.0.join(format!("source-{recoverable}"));
            let target = root.0.join(format!("target-{recoverable}"));
            write_minimal_first_seen_source(&source, epoch);
            let attempt_id = format!("corrupt-temp-{recoverable}");
            let staging = expected_staging_path(&target, &attempt_id).unwrap();
            let core = reprocess_first_seen_registry(&RegistryReprocessOptions {
                source_dir: source.clone(),
                target_dir: target.clone(),
                epoch,
                threads: 1,
                sort_memory_mib: 1,
                level: 1,
                attempt_id: attempt_id.clone(),
                staging_dir: staging.clone(),
                wire_profile: TEST_WIRE_PROFILE,
                wire_profile_authority_receipt: None,
            })
            .unwrap();
            let receipt_temp = staging.join(REGISTRY_REPROCESS_RECEIPT_TEMP_FILE);
            fs::write(&receipt_temp, b"{not-json").unwrap();

            if recoverable {
                probe_registry_reprocess_access_continuation(
                    &staging,
                    &source,
                    &target,
                    epoch,
                    &attempt_id,
                    &core.handoff_sha256,
                    TEST_WIRE_PROFILE,
                )
                .unwrap();
                complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
                    source_dir: source,
                    staging_dir: staging,
                    target_dir: target.clone(),
                    epoch,
                    attempt_id,
                    handoff_sha256: core.handoff_sha256,
                    expected_continuation_state:
                        RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
                    wire_profile: TEST_WIRE_PROFILE,
                    wire_profile_authority_receipt: None,
                })
                .unwrap();
                assert!(target.is_dir());
            } else {
                fs::remove_file(staging.join(REPROCESS_HANDOFF_FILE)).unwrap();
                assert!(
                    probe_registry_reprocess_access_continuation(
                        &staging,
                        &source,
                        &target,
                        epoch,
                        &attempt_id,
                        &core.handoff_sha256,
                        TEST_WIRE_PROFILE,
                    )
                    .is_err()
                );
                assert!(
                    complete_first_seen_registry_access(&RegistryReprocessAccessOptions {
                        source_dir: source,
                        staging_dir: staging,
                        target_dir: target.clone(),
                        epoch,
                        attempt_id,
                        handoff_sha256: core.handoff_sha256,
                        expected_continuation_state:
                            RegistryReprocessAccessContinuationState::CoreOrPartialRebuild,
                        wire_profile: TEST_WIRE_PROFILE,
                        wire_profile_authority_receipt: None,
                    })
                    .is_err()
                );
                assert!(receipt_temp.is_file());
                assert!(!target.exists());
            }
        }
    }

    #[test]
    fn v3_binds_access_and_hard_links_duplicate_signatures() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let receipt = run_two_stage(&source, &target, epoch, 1);

        for name in [
            ARCHIVE_V2_BLOCK_ACCESS_FILE,
            ARCHIVE_V2_BLOCK_ACCESS_INDEX_FILE,
        ] {
            assert!(
                receipt.source_files.contains_key(name),
                "v3 must bind trusted source artifact {name}"
            );
            assert!(
                receipt.target_files.contains_key(name),
                "v3 must bind remapped target artifact {name}"
            );
        }
        assert!(
            !receipt
                .source_files
                .contains_key(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)
        );
        assert!(
            receipt
                .target_files
                .contains_key(ARCHIVE_V2_GET_BLOCK_INDEX_FILE)
        );
        assert_eq!(
            receipt.source_files.get(ARCHIVE_V2_SIGNATURES_FILE),
            receipt.target_files.get(ARCHIVE_V2_SIGNATURES_FILE),
            "source and target must share the access-derived signature binding"
        );
        validate_signature_hard_link_metadata(&source, &target, 64).unwrap();
        probe_published_reprocess(&target, epoch).unwrap();
        validate_published_reprocess(&source, &target, epoch).unwrap();

        let source_poh = source.join(ARCHIVE_V2_POH_FILE);
        let original_source_poh = fs::read(&source_poh).unwrap();
        fs::write(&source_poh, [0xa5]).unwrap();
        assert!(
            validate_published_reprocess(&source, &target, epoch).is_err(),
            "tampering with a bound source input must fail deep validation"
        );
        fs::write(&source_poh, original_source_poh).unwrap();

        let target_shredding = target.join(ARCHIVE_V2_SHREDDING_FILE);
        let original_target_shredding = fs::read(&target_shredding).unwrap();
        fs::write(&target_shredding, [0x5a]).unwrap();
        assert!(
            validate_published_reprocess(&source, &target, epoch).is_err(),
            "tampering with a bound target artifact must fail deep validation"
        );
        fs::write(&target_shredding, original_target_shredding).unwrap();
        validate_published_reprocess(&source, &target, epoch).unwrap();
    }

    #[test]
    fn v3_receipt_rejects_incomplete_access_and_mismatched_signature_provenance() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let receipt = run_two_stage(&source, &target, epoch, 1);

        let mut omitted_get_block = receipt.clone();
        omitted_get_block
            .target_files
            .remove(ARCHIVE_V2_GET_BLOCK_INDEX_FILE);
        assert!(validate_receipt_shape(&omitted_get_block, epoch).is_err());

        let mut mismatched_signature = receipt;
        mismatched_signature
            .target_files
            .get_mut(ARCHIVE_V2_SIGNATURES_FILE)
            .unwrap()
            .sha256 = "ff".repeat(32);
        assert!(validate_receipt_shape(&mismatched_signature, epoch).is_err());
    }

    #[test]
    fn v3_receipt_rejects_bound_unsupported_target_artifact() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let mut receipt = run_two_stage(&source, &target, epoch, 1);
        let unsupported = "unsupported-target-artifact.bin";
        fs::write(target.join(unsupported), b"bound but unsupported").unwrap();
        receipt.target_files.insert(
            unsupported.to_owned(),
            hash_file(&target.join(unsupported)).unwrap(),
        );
        receipt.target_generation_sha256 = generation_digest(&receipt.target_files);
        let mut bytes = serde_json::to_vec_pretty(&receipt).unwrap();
        bytes.push(b'\n');
        fs::write(target.join(REGISTRY_REPROCESS_RECEIPT_FILE), bytes).unwrap();

        assert!(probe_published_reprocess(&target, epoch).is_err());
        assert!(validate_published_reprocess(&source, &target, epoch).is_err());
    }

    #[test]
    fn v3_receipt_rejects_bound_unsupported_source_artifact() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let mut receipt = run_two_stage(&source, &target, epoch, 1);
        let unsupported = "unsupported-source-artifact.bin";
        fs::write(source.join(unsupported), b"bound but unsupported").unwrap();
        receipt.source_files.insert(
            unsupported.to_owned(),
            hash_file(&source.join(unsupported)).unwrap(),
        );
        receipt.source_generation_sha256 = generation_digest(&receipt.source_files);
        let mut bytes = serde_json::to_vec_pretty(&receipt).unwrap();
        bytes.push(b'\n');
        fs::write(target.join(REGISTRY_REPROCESS_RECEIPT_FILE), bytes).unwrap();

        assert!(probe_published_reprocess(&target, epoch).is_err());
        assert!(validate_published_reprocess(&source, &target, epoch).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn normal_two_stage_path_never_reads_signature_sidecar_data() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let source_signatures = source.join(ARCHIVE_V2_SIGNATURES_FILE);
        fs::set_permissions(&source_signatures, fs::Permissions::from_mode(0)).unwrap();

        let receipt = run_two_stage(&source, &target, epoch, 1);
        probe_published_reprocess(&target, epoch).unwrap();
        validate_published_reprocess(&source, &target, epoch).unwrap();
        assert_eq!(
            receipt.signature_provenance.as_deref(),
            Some(SIGNATURE_PROVENANCE)
        );
        let target_signatures = target.join(ARCHIVE_V2_SIGNATURES_FILE);
        let source_metadata = fs::symlink_metadata(&source_signatures).unwrap();
        let target_metadata = fs::symlink_metadata(&target_signatures).unwrap();
        assert_eq!(source_metadata.len(), 64);
        assert_eq!(target_metadata.len(), 64);
        assert_eq!(source_metadata.dev(), target_metadata.dev());
        assert_eq!(source_metadata.ino(), target_metadata.ino());
        assert!(
            validate_bound_files_for_deep(&source, &receipt.source_files, RECEIPT_VERSION_V1,)
                .is_err(),
            "v1 deep validation must still read the signature sidecar"
        );
        assert!(
            validate_bound_files_for_deep(&source, &receipt.source_files, RECEIPT_VERSION_V2,)
                .is_err(),
            "v2 deep validation must still read the signature sidecar"
        );

        // The two paths share one inode, so this restores both names for fixture cleanup.
        fs::set_permissions(&source_signatures, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[test]
    fn profile_neutral_v1_receipt_is_visible_but_cannot_authorize_deep_decode() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        let epoch = 1;
        write_minimal_first_seen_source(&source, epoch);
        let mut receipt = run_two_stage(&source, &target, epoch, 1);

        let (source_semantics, _) =
            recompute_source_canonical_counts(&source, &target, &receipt, epoch).unwrap();
        let (target_semantics, _) =
            scan_target_generation_semantics(&target, epoch, receipt.threads, TEST_WIRE_PROFILE)
                .unwrap();
        assert_eq!(source_semantics, target_semantics);
        receipt.version = RECEIPT_VERSION_V1;
        receipt.algorithm = RECEIPT_ALGORITHM_V1.to_owned();
        receipt.source_generation_sha256 = generation_digest(&receipt.source_files);
        receipt.source_semantics = Some(source_semantics);
        receipt.target_semantics = Some(target_semantics);
        receipt.rewrite_stats = None;
        receipt.attempt_id = None;
        receipt.handoff_sha256 = None;
        receipt.assembly_mode = None;
        receipt.signature_provenance = None;
        receipt.wire_profile = None;
        receipt
            .target_files
            .remove(POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE);
        receipt.target_generation_sha256 = generation_digest(&receipt.target_files);
        fs::remove_file(target.join(POST_UNKNOWN_INSTRUCTION_FALLBACKS_MARKER_FILE)).unwrap();
        let receipt_path = target.join(REGISTRY_REPROCESS_RECEIPT_FILE);
        fs::remove_file(&receipt_path).unwrap();
        write_receipt(&target, &receipt).unwrap();

        probe_published_reprocess(&target, epoch).unwrap();
        assert!(
            validate_published_reprocess(&source, &target, epoch).is_err(),
            "a profile-neutral v1 receipt must not authorize semantic decoding"
        );
    }

    #[test]
    fn existing_target_is_immutable_and_reported_before_source_processing() {
        let root = TestDir::new();
        let source = root.0.join("source");
        let target = root.0.join("target");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&target).unwrap();
        fs::write(target.join("sentinel"), b"keep").unwrap();
        let attempt_id = "existing-target".to_owned();
        let staging_dir = expected_staging_path(&target, &attempt_id).unwrap();
        let error = reprocess_first_seen_registry(&RegistryReprocessOptions {
            source_dir: source,
            target_dir: target.clone(),
            epoch: 1,
            threads: 1,
            sort_memory_mib: 1,
            level: 1,
            attempt_id,
            staging_dir,
            wire_profile: TEST_WIRE_PROFILE,
            wire_profile_authority_receipt: None,
        })
        .unwrap_err();
        assert!(error.to_string().contains("target already exists"));
        assert_eq!(fs::read(target.join("sentinel")).unwrap(), b"keep");
    }
}
