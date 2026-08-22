//! On-disk layout for a per-epoch wallet -> program-id reverse index.
//!
//! Both wallets and programs are keyed by the archive's compact **registry
//! id** (`u32`, from `registry.bin`/`KeyIndex`), never by raw 32-byte
//! pubkeys. Registry ids are already what the archive decode path produces
//! (`CompactPubkey::Id`), so the archive scan never resolves a pubkey byte.
//! Query uses the epoch's prebuilt file-backed key index for pubkey -> id,
//! verifies that id against `registry.bin`, and resolves returned program ids
//! through the index's content-bound `programs.map`. This keeps every build
//! hot-path key 4 bytes instead of 32:
//! denser cache lines, trivial hashing, and a single-register comparison for
//! the on-disk binary search.
//!
//! ## Sharding
//!
//! A full epoch's registry can hold tens of millions of ids; an
//! `IndexBuilder` sized to all of them at once is a single large upfront
//! allocation (see `IndexBuilder` docs) that doesn't fit on constrained
//! hardware. So the *account* id space (never the program id space, which
//! stays global) is split into fixed-width chunks, and the archive is
//! scanned once per chunk — a full re-decode per chunk, trading CPU/wall
//! time for a bounded, predictable memory ceiling
//! (`chunk_width * bytes_per_wallet_slot`) that doesn't grow with registry
//! size. Each pass writes its own **shard**: a wallet's shard is always
//! `(wallet_id - 1) / chunk_width`, computable with no lookup, so shards
//! never need merging.
//!
//! A built index is a directory:
//! - `manifest.json`: epoch/provenance metadata plus `chunk_width` and
//!   `shard_count`, needed to find a wallet's shard.
//! - `programs.map`: sorted fixed-size `(registry id, pubkey)` records for the
//!   distinct programs referenced by the index.
//! - `shard-<N>/wallets.idx`: fixed-size records sorted by wallet registry
//!   id, each holding an (offset, count) slice into `shard-<N>/programs.rel`.
//!   Binary-searchable with positioned reads without loading the whole file.
//! - `shard-<N>/programs.rel`: concatenated fixed-size program-usage records,
//!   one contiguous slice per wallet.

use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
};

use rustix::fs::{Mode, OFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const WALLETS_FILE: &str = "wallets.idx";
const RELATIONS_FILE: &str = "programs.rel";
const PROGRAM_MAP_FILE: &str = "programs.map";
const MANIFEST_FILE: &str = "manifest.json";

const WALLETS_MAGIC: [u8; 4] = *b"FBIW";
const RELATIONS_MAGIC: [u8; 4] = *b"FBIR";
const PROGRAM_MAP_MAGIC: [u8; 4] = *b"FBIP";
pub const FORMAT_VERSION: u32 = 4;
pub const MANIFEST_SCHEMA_VERSION: u32 = 4;
pub const SEMANTICS_VERSION: u32 = 2;

const WALLETS_HEADER_LEN: usize = 4 + 4 + 8; // magic + version + count(u64)
const RELATIONS_HEADER_LEN: usize = 4 + 4 + 8; // magic + version + count(u64)
const WALLET_RECORD_LEN: usize = 4 + 8 + 4; // wallet_id(u32) + programs_offset(u64) + programs_count(u32)
pub const PROGRAM_USAGE_RECORD_LEN: usize = 4 * 5 + 8 * 4;
const PROGRAM_MAP_HEADER_LEN: usize = 4 + 4 + 8;
const PROGRAM_MAP_RECORD_LEN: usize = 4 + 32;
const REGISTRY_INDEX_MIN_LEN: u64 = 8 + 2 + 2 + 8;
const SHARD_READER_BUFFER_SIZE: usize = 8 << 20;
const MAX_INDEX_MANIFEST_BYTES: u64 = 4 << 20;

/// A standard vector keeps every untouched slot in the dense chunk array at
/// three machine words. Embedding even one aligned `ProgramUsage` in each slot
/// would make the fixed per-wallet cost much larger for sparse chunks.
type ProgramUsages = Vec<ProgramUsage>;

/// Sentinel stored in both block-time fields when no transaction contributing
/// to a wallet/program relation has an available block time.
pub const PROGRAM_USAGE_MISSING_BLOCK_TIME: i64 = i64::MIN;

/// Exact aggregate for one wallet/program relation within one epoch.
///
/// Instruction counts include every top-level or inner instruction occurrence.
/// `transaction_count` counts distinct successful transactions that reached the
/// program at least once. Block-time extrema include only transactions whose
/// block header supplied a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProgramUsage {
    pub program_id: u32,
    pub direct_instruction_count: u32,
    pub inner_instruction_count: u32,
    pub transaction_count: u32,
    pub first_seen_slot: u64,
    pub last_seen_slot: u64,
    pub min_block_time: i64,
    pub max_block_time: i64,
    pub timed_transaction_count: u32,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ProgramUsageError {
    #[error("program id must be nonzero")]
    InvalidProgramId,
    #[error("at least one direct or inner instruction must be present")]
    EmptyInstructionCounts,
    #[error("transaction count must be nonzero")]
    EmptyTransactionCount,
    #[error(
        "transaction count {transaction_count} exceeds total instruction count {instruction_count}"
    )]
    TransactionCountExceedsInstructions {
        transaction_count: u32,
        instruction_count: u64,
    },
    #[error(
        "timed transaction count {timed_transaction_count} exceeds transaction count {transaction_count}"
    )]
    TimedTransactionCountExceedsTransactions {
        timed_transaction_count: u32,
        transaction_count: u32,
    },
    #[error("first seen slot {first_seen_slot} is after last seen slot {last_seen_slot}")]
    InvalidSlotRange {
        first_seen_slot: u64,
        last_seen_slot: u64,
    },
    #[error("missing block-time sentinel is inconsistent with timed transaction count")]
    InconsistentMissingBlockTime,
    #[error("minimum block time {min_block_time} is after maximum block time {max_block_time}")]
    InvalidBlockTimeRange {
        min_block_time: i64,
        max_block_time: i64,
    },
    #[error("cannot merge program {left_program_id} with program {right_program_id}")]
    ProgramMismatch {
        left_program_id: u32,
        right_program_id: u32,
    },
    #[error("{field} overflow while merging program usage")]
    CountOverflow { field: &'static str },
}

impl ProgramUsage {
    #[inline]
    pub fn program_id(&self) -> u32 {
        self.program_id
    }

    /// Construct the contribution from one distinct successful transaction.
    pub fn new_transaction(
        program_id: u32,
        direct_instruction_count: u32,
        inner_instruction_count: u32,
        slot: u64,
        block_time: Option<i64>,
    ) -> Result<Self, ProgramUsageError> {
        let (min_block_time, max_block_time, timed_transaction_count) = match block_time {
            Some(block_time) => (block_time, block_time, 1),
            None => (
                PROGRAM_USAGE_MISSING_BLOCK_TIME,
                PROGRAM_USAGE_MISSING_BLOCK_TIME,
                0,
            ),
        };
        let usage = Self {
            program_id,
            direct_instruction_count,
            inner_instruction_count,
            transaction_count: 1,
            first_seen_slot: slot,
            last_seen_slot: slot,
            min_block_time,
            max_block_time,
            timed_transaction_count,
        };
        usage.validate()?;
        Ok(usage)
    }

    pub fn validate(&self) -> Result<(), ProgramUsageError> {
        if self.program_id == 0 {
            return Err(ProgramUsageError::InvalidProgramId);
        }
        let instruction_count =
            u64::from(self.direct_instruction_count) + u64::from(self.inner_instruction_count);
        if instruction_count == 0 {
            return Err(ProgramUsageError::EmptyInstructionCounts);
        }
        if self.transaction_count == 0 {
            return Err(ProgramUsageError::EmptyTransactionCount);
        }
        if u64::from(self.transaction_count) > instruction_count {
            return Err(ProgramUsageError::TransactionCountExceedsInstructions {
                transaction_count: self.transaction_count,
                instruction_count,
            });
        }
        if self.timed_transaction_count > self.transaction_count {
            return Err(
                ProgramUsageError::TimedTransactionCountExceedsTransactions {
                    timed_transaction_count: self.timed_transaction_count,
                    transaction_count: self.transaction_count,
                },
            );
        }
        if self.first_seen_slot > self.last_seen_slot {
            return Err(ProgramUsageError::InvalidSlotRange {
                first_seen_slot: self.first_seen_slot,
                last_seen_slot: self.last_seen_slot,
            });
        }
        if self.timed_transaction_count == 0 {
            if self.min_block_time != PROGRAM_USAGE_MISSING_BLOCK_TIME
                || self.max_block_time != PROGRAM_USAGE_MISSING_BLOCK_TIME
            {
                return Err(ProgramUsageError::InconsistentMissingBlockTime);
            }
        } else {
            if self.min_block_time == PROGRAM_USAGE_MISSING_BLOCK_TIME
                || self.max_block_time == PROGRAM_USAGE_MISSING_BLOCK_TIME
            {
                return Err(ProgramUsageError::InconsistentMissingBlockTime);
            }
            if self.min_block_time > self.max_block_time {
                return Err(ProgramUsageError::InvalidBlockTimeRange {
                    min_block_time: self.min_block_time,
                    max_block_time: self.max_block_time,
                });
            }
        }
        Ok(())
    }

    /// Merge two disjoint scan aggregates for the same wallet/program pair.
    /// The operation is commutative and independent of worker completion order.
    pub fn checked_merge(self, other: Self) -> Result<Self, ProgramUsageError> {
        self.validate()?;
        other.validate()?;
        if self.program_id != other.program_id {
            return Err(ProgramUsageError::ProgramMismatch {
                left_program_id: self.program_id,
                right_program_id: other.program_id,
            });
        }
        let add = |left: u32, right: u32, field: &'static str| {
            left.checked_add(right)
                .ok_or(ProgramUsageError::CountOverflow { field })
        };
        let timed_transaction_count = add(
            self.timed_transaction_count,
            other.timed_transaction_count,
            "timed_transaction_count",
        )?;
        let (min_block_time, max_block_time) = match (
            self.timed_transaction_count != 0,
            other.timed_transaction_count != 0,
        ) {
            (false, false) => (
                PROGRAM_USAGE_MISSING_BLOCK_TIME,
                PROGRAM_USAGE_MISSING_BLOCK_TIME,
            ),
            (true, false) => (self.min_block_time, self.max_block_time),
            (false, true) => (other.min_block_time, other.max_block_time),
            (true, true) => (
                self.min_block_time.min(other.min_block_time),
                self.max_block_time.max(other.max_block_time),
            ),
        };
        let merged = Self {
            program_id: self.program_id,
            direct_instruction_count: add(
                self.direct_instruction_count,
                other.direct_instruction_count,
                "direct_instruction_count",
            )?,
            inner_instruction_count: add(
                self.inner_instruction_count,
                other.inner_instruction_count,
                "inner_instruction_count",
            )?,
            transaction_count: add(
                self.transaction_count,
                other.transaction_count,
                "transaction_count",
            )?,
            first_seen_slot: self.first_seen_slot.min(other.first_seen_slot),
            last_seen_slot: self.last_seen_slot.max(other.last_seen_slot),
            min_block_time,
            max_block_time,
            timed_transaction_count,
        };
        merged.validate()?;
        Ok(merged)
    }

    /// Average spacing across available block-time observations, using the
    /// merge-order-independent extrema and observation count.
    pub fn average_timed_transaction_gap_seconds(&self) -> Option<f64> {
        if self.validate().is_err() || self.timed_transaction_count < 2 {
            return None;
        }
        let elapsed = i128::from(self.max_block_time) - i128::from(self.min_block_time);
        Some(elapsed as f64 / f64::from(self.timed_transaction_count - 1))
    }

    fn to_le_bytes(self) -> [u8; PROGRAM_USAGE_RECORD_LEN] {
        let mut bytes = [0u8; PROGRAM_USAGE_RECORD_LEN];
        bytes[0..4].copy_from_slice(&self.program_id.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.direct_instruction_count.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.inner_instruction_count.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.transaction_count.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.first_seen_slot.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.last_seen_slot.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.min_block_time.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.max_block_time.to_le_bytes());
        bytes[48..52].copy_from_slice(&self.timed_transaction_count.to_le_bytes());
        bytes
    }

    fn from_le_bytes(bytes: [u8; PROGRAM_USAGE_RECORD_LEN]) -> Self {
        Self {
            program_id: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            direct_instruction_count: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            inner_instruction_count: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            transaction_count: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            first_seen_slot: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            last_seen_slot: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            min_block_time: i64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            max_block_time: i64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            timed_transaction_count: u32::from_le_bytes(bytes[48..52].try_into().unwrap()),
        }
    }
}

#[derive(Debug, Error)]
pub enum FormatError {
    #[error("io error at {path}: {source}")]
    Io { path: String, source: io::Error },
    #[error("{file} at {path} is truncated: expected at least {expected} bytes, found {found}")]
    Truncated {
        file: &'static str,
        path: String,
        expected: usize,
        found: usize,
    },
    #[error(
        "{file} at {path} has invalid length: expected exactly {expected} bytes, found {found}"
    )]
    InvalidLength {
        file: &'static str,
        path: String,
        expected: usize,
        found: usize,
    },
    #[error("{file} at {path} has bad magic")]
    BadMagic { file: &'static str, path: String },
    #[error("{file} at {path} has unsupported version {version}")]
    UnsupportedVersion {
        file: &'static str,
        path: String,
        version: u32,
    },
    #[error("wallets.idx at {path} is not sorted by wallet id at record {index}")]
    Unsorted { path: String, index: usize },
    #[error(
        "programs.rel reference out of range: wallet record {index} points at {offset}..{end} but relations table has {len} entries"
    )]
    RelationRangeOutOfBounds {
        index: usize,
        offset: u64,
        end: u64,
        len: u64,
    },
    #[error(
        "programs.rel slice for wallet record {index} is not a sorted, unique list of valid program usages"
    )]
    InvalidProgramList { index: usize },
    #[error("invalid programs.rel usage at wallet record {index}: {source}")]
    InvalidProgramUsage {
        index: usize,
        #[source]
        source: ProgramUsageError,
    },
    #[error(
        "wallet record {index} contains id {wallet_id}, outside expected shard range {first_wallet_id}..={last_wallet_id}"
    )]
    WalletOutsideShard {
        index: usize,
        wallet_id: u32,
        first_wallet_id: u32,
        last_wallet_id: u32,
    },
    #[error(
        "non-canonical programs.rel layout at wallet record {index}: expected offset {expected_offset}, found {found_offset}"
    )]
    NonCanonicalRelationLayout {
        index: usize,
        expected_offset: u64,
        found_offset: u64,
    },
    #[error("content binding mismatch for {file} at {path}")]
    ContentBindingMismatch { file: &'static str, path: String },
    #[error("programs.map at {path} is not sorted by program id at record {index}")]
    UnsortedProgramMap { path: String, index: usize },
    #[error("program id {program_id} is absent from the bound programs.map")]
    MissingProgram { program_id: u32 },
    #[error("manifest error at {path}: {source}")]
    Manifest {
        path: String,
        source: serde_json::Error,
    },
    #[error("invalid index manifest at {path}: {message}")]
    InvalidManifest { path: String, message: String },
    #[error("invalid wallet id {wallet_id}: valid registry ids are 1..={registry_entries}")]
    InvalidWalletId {
        wallet_id: u32,
        registry_entries: u32,
    },
    #[error("integer overflow while decoding {file} at {path}")]
    IntegerOverflow { file: &'static str, path: String },
    #[error("invalid shard writer input at {path}: {message}")]
    InvalidWriterInput { path: String, message: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GenerationBindingKind {
    PublishedManifest,
    TrustedLocalAssertedImmutable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalletScope {
    AllTransactionSigners,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgramScope {
    ReachedDirectAndInnerUsageStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailedTransactionPolicy {
    ExcludeAll,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoteTransactionPolicy {
    Include,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexSemantics {
    pub version: u32,
    pub wallet_scope: WalletScope,
    pub program_scope: ProgramScope,
    pub failed_transactions: FailedTransactionPolicy,
    pub vote_transactions: VoteTransactionPolicy,
}

impl IndexSemantics {
    pub fn current() -> Self {
        Self {
            version: SEMANTICS_VERSION,
            wallet_scope: WalletScope::AllTransactionSigners,
            program_scope: ProgramScope::ReachedDirectAndInnerUsageStats,
            failed_transactions: FailedTransactionPolicy::ExcludeAll,
            // The compact-vote-instruction flag is not an exact whole-transaction vote
            // classifier, so the current semantics include votes rather than
            // silently guessing.
            vote_transactions: VoteTransactionPolicy::Include,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryFileIdentity {
    pub size: u64,
    pub device: u64,
    pub inode: u64,
    pub modified_seconds: i64,
    pub modified_nanoseconds: i64,
    pub changed_seconds: i64,
    pub changed_nanoseconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexFileBinding {
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardBinding {
    pub shard: u32,
    pub wallets: IndexFileBinding,
    pub relations: IndexFileBinding,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OmissionCounts {
    pub raw_transactions: u64,
    pub raw_metadata: u64,
    pub decode_errors: u64,
    pub unresolved_required_pubkeys: u64,
}

/// Top-level, whole-index metadata — one per `build` run, at
/// `<out_dir>/manifest.json`. Distinct from any single shard's contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexManifest {
    pub schema_version: u32,
    pub format_version: u32,
    pub semantics: IndexSemantics,
    pub complete: bool,
    pub omissions: OmissionCounts,
    pub binding_kind: GenerationBindingKind,
    pub cluster_id: String,
    pub epoch: u64,
    pub archive_root: String,
    pub generation_id: String,
    pub generation_digest: String,
    /// Exact Archive V2 hot-message grammar used to decode this generation.
    pub archive_wire_profile: blockzilla_read_sdk::ArchiveV2WireProfile,
    /// Content binding for the exact `registry.bin` used by this build.
    pub registry: IndexFileBinding,
    pub registry_file_identity: RegistryFileIdentity,
    /// Content binding and original filesystem identity for the exact
    /// `registry.mphf` accepted by this build. The identity enables the
    /// original-path query fast path; relocated queries verify the digest.
    pub registry_index: IndexFileBinding,
    pub registry_index_file_identity: RegistryFileIdentity,
    /// The epoch's registry size at build time — every valid program id is
    /// `1..=registry_entries`, regardless of chunking.
    pub registry_entries: u32,
    /// Account ids `[1 + i*chunk_width, 1 + (i+1)*chunk_width)` live in
    /// `shard-{i}`. A wallet's shard is always `(wallet_id - 1) / chunk_width`.
    pub chunk_width: u32,
    pub shard_count: u32,
    /// Content bindings for every immutable shard, ordered by shard number.
    pub shards: Vec<ShardBinding>,
    /// Compact, immutable registry-id -> pubkey map for every program that
    /// appears in any shard. This lets queries return exact program pubkeys
    /// without trusting mutable `registry.bin` bytes at lookup time.
    pub program_map: IndexFileBinding,
    pub wallet_count: u64,
    pub program_count: u64,
    pub transactions_scanned: u64,
    pub blocks_scanned: u64,
    /// Failed transactions are intentionally outside the semantic relation,
    /// not omissions: no program from them is indexed.
    pub failed_transactions_excluded: u64,
    pub built_unix_time: u64,
    pub tool_version: String,
}

impl IndexManifest {
    /// Verify every immutable data file in one published index generation.
    pub fn verify_generation(index_dir: &Path) -> Result<Self, FormatError> {
        let manifest = Self::read(index_dir)?;
        let program_map = ProgramMapReader::open_verified(
            index_dir,
            &manifest.program_map,
            manifest.program_count,
        )?;
        program_map.verify_unchanged()?;
        let mut wallets = 0u64;
        for binding in &manifest.shards {
            let shard = index_dir.join(format!("shard-{}", binding.shard));
            let reader = IndexReader::open_verified(&shard, binding)?;
            wallets = wallets.checked_add(reader.wallet_count()).ok_or_else(|| {
                FormatError::IntegerOverflow {
                    file: "wallets.idx",
                    path: index_dir.display().to_string(),
                }
            })?;
            reader.verify_unchanged()?;
        }
        if wallets != manifest.wallet_count {
            return Err(FormatError::ContentBindingMismatch {
                file: "wallets.idx",
                path: index_dir.display().to_string(),
            });
        }
        Ok(manifest)
    }

    pub fn write(&self, out_dir: &Path) -> Result<(), FormatError> {
        fs::create_dir_all(out_dir).map_err(|source| FormatError::Io {
            path: out_dir.display().to_string(),
            source,
        })?;
        let path = out_dir.join(MANIFEST_FILE);
        self.validate(&path)?;
        let json = serde_json::to_vec_pretty(self).map_err(|source| FormatError::Manifest {
            path: path.display().to_string(),
            source,
        })?;
        if json.len() as u64 > MAX_INDEX_MANIFEST_BYTES {
            return Err(FormatError::InvalidManifest {
                path: path.display().to_string(),
                message: format!(
                    "serialized manifest is {} bytes, above the {MAX_INDEX_MANIFEST_BYTES}-byte limit",
                    json.len()
                ),
            });
        }
        fs::write(&path, json).map_err(|source| FormatError::Io {
            path: path.display().to_string(),
            source,
        })
    }

    pub fn read(out_dir: &Path) -> Result<Self, FormatError> {
        let path = out_dir.join(MANIFEST_FILE);
        let file = open_file(&path)?;
        let initial_identity = open_file_identity(&file, &path)?;
        let manifest_size = initial_identity.size;
        if manifest_size > MAX_INDEX_MANIFEST_BYTES {
            return Err(FormatError::InvalidManifest {
                path: path.display().to_string(),
                message: format!(
                    "manifest is {manifest_size} bytes, above the {MAX_INDEX_MANIFEST_BYTES}-byte limit"
                ),
            });
        }
        let mut bytes = Vec::with_capacity(manifest_size as usize);
        (&file)
            .take(MAX_INDEX_MANIFEST_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|source| FormatError::Io {
                path: path.display().to_string(),
                source,
            })?;
        if bytes.len() as u64 > MAX_INDEX_MANIFEST_BYTES {
            return Err(FormatError::InvalidManifest {
                path: path.display().to_string(),
                message: format!(
                    "manifest exceeds the {MAX_INDEX_MANIFEST_BYTES}-byte limit while reading"
                ),
            });
        }
        if open_file_identity(&file, &path)? != initial_identity {
            return Err(FormatError::ContentBindingMismatch {
                file: "manifest.json",
                path: path.display().to_string(),
            });
        }
        let manifest: Self =
            serde_json::from_slice(&bytes).map_err(|source| FormatError::Manifest {
                path: path.display().to_string(),
                source,
            })?;
        manifest.validate(&path)?;
        Ok(manifest)
    }

    /// Which shard directory (relative to `out_dir`) a wallet id lives in.
    pub fn shard_dir_name(&self, wallet_id: u32) -> Result<String, FormatError> {
        if self.chunk_width == 0 {
            return Err(FormatError::InvalidManifest {
                path: "<in-memory manifest>".into(),
                message: "chunk_width must be greater than zero".into(),
            });
        }
        if wallet_id == 0 || wallet_id > self.registry_entries {
            return Err(FormatError::InvalidWalletId {
                wallet_id,
                registry_entries: self.registry_entries,
            });
        }
        Ok(format!(
            "shard-{}",
            shard_index(wallet_id, self.chunk_width)
        ))
    }

    pub fn shard_binding(&self, wallet_id: u32) -> Result<&ShardBinding, FormatError> {
        if self.chunk_width == 0 {
            return Err(FormatError::InvalidManifest {
                path: "<in-memory manifest>".into(),
                message: "chunk_width must be greater than zero".into(),
            });
        }
        if wallet_id == 0 || wallet_id > self.registry_entries {
            return Err(FormatError::InvalidWalletId {
                wallet_id,
                registry_entries: self.registry_entries,
            });
        }
        let shard = shard_index(wallet_id, self.chunk_width);
        self.shards
            .get(shard as usize)
            .filter(|binding| binding.shard == shard)
            .ok_or_else(|| FormatError::InvalidManifest {
                path: "<in-memory manifest>".into(),
                message: format!("missing content binding for shard {shard}"),
            })
    }

    fn validate(&self, path: &Path) -> Result<(), FormatError> {
        let invalid = |message: String| FormatError::InvalidManifest {
            path: path.display().to_string(),
            message,
        };
        if self.schema_version != MANIFEST_SCHEMA_VERSION {
            return Err(invalid(format!(
                "unsupported schema_version {}",
                self.schema_version
            )));
        }
        if self.format_version != FORMAT_VERSION {
            return Err(invalid(format!(
                "unsupported format_version {}",
                self.format_version
            )));
        }
        if self.semantics != IndexSemantics::current() {
            return Err(invalid("unsupported signer-to-program semantics".into()));
        }
        if !self.complete || self.omissions != OmissionCounts::default() {
            return Err(invalid(
                "strict indexes must be complete and contain no omissions".into(),
            ));
        }
        if self.cluster_id.is_empty()
            || self.generation_id.is_empty()
            || self.archive_root.is_empty()
            || self.tool_version.is_empty()
        {
            return Err(invalid(
                "cluster_id, generation_id, archive_root, and tool_version must be non-empty"
                    .into(),
            ));
        }
        for (field, value) in [("generation_digest", self.generation_digest.as_str())] {
            if !is_lower_sha256(value) {
                return Err(invalid(format!(
                    "{field} must be 64 lowercase hexadecimal characters"
                )));
            }
        }
        if self.registry_entries == 0 {
            return Err(invalid("registry_entries must be greater than zero".into()));
        }
        let expected_registry_size = u64::from(self.registry_entries) * 32;
        if self.registry.size != expected_registry_size
            || self.registry_file_identity.size != self.registry.size
            || !is_lower_sha256(&self.registry.sha256)
        {
            return Err(invalid(
                "registry.bin has an invalid size, identity, or SHA-256 binding".into(),
            ));
        }
        if self.registry_index.size < REGISTRY_INDEX_MIN_LEN
            || self.registry_index_file_identity.size != self.registry_index.size
            || !is_lower_sha256(&self.registry_index.sha256)
        {
            return Err(invalid(
                "registry.mphf has an invalid size, identity, or SHA-256 binding".into(),
            ));
        }
        if self.chunk_width == 0 {
            return Err(invalid("chunk_width must be greater than zero".into()));
        }
        let expected_shards = self.registry_entries.div_ceil(self.chunk_width);
        if self.shard_count != expected_shards {
            return Err(invalid(format!(
                "shard_count {} does not match registry/chunk geometry ({expected_shards})",
                self.shard_count
            )));
        }
        if self.shards.len() != self.shard_count as usize {
            return Err(invalid(format!(
                "shard binding count {} does not match shard_count {}",
                self.shards.len(),
                self.shard_count
            )));
        }
        for (expected, binding) in self.shards.iter().enumerate() {
            if binding.shard as usize != expected {
                return Err(invalid(format!(
                    "shard binding {} has shard number {}, expected {expected}",
                    expected, binding.shard
                )));
            }
            for (kind, file, minimum) in [
                ("wallets.idx", &binding.wallets, WALLETS_HEADER_LEN as u64),
                (
                    "programs.rel",
                    &binding.relations,
                    RELATIONS_HEADER_LEN as u64,
                ),
            ] {
                if file.size < minimum || !is_lower_sha256(&file.sha256) {
                    return Err(invalid(format!(
                        "shard {expected} {kind} has an invalid size or SHA-256 binding"
                    )));
                }
            }
        }
        if self.program_map.size < PROGRAM_MAP_HEADER_LEN as u64
            || !is_lower_sha256(&self.program_map.sha256)
        {
            return Err(invalid(
                "programs.map has an invalid size or SHA-256 binding".into(),
            ));
        }
        if self.wallet_count > u64::from(self.registry_entries)
            || self.program_count > u64::from(self.registry_entries)
        {
            return Err(invalid(
                "wallet_count/program_count cannot exceed registry_entries".into(),
            ));
        }
        if self.failed_transactions_excluded > self.transactions_scanned {
            return Err(invalid(
                "failed_transactions_excluded cannot exceed transactions_scanned".into(),
            ));
        }
        Ok(())
    }
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[inline]
fn shard_index(wallet_id: u32, chunk_width: u32) -> u32 {
    (wallet_id - 1) / chunk_width
}

/// Outcome of `IndexBuilder::record`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    Recorded,
    /// `usage.program_id` was 0 (the raw-pubkey sentinel) or beyond the registry's
    /// declared size — a malformed/corrupt archive, since a well-formed one
    /// only ever emits ids it declared in `registry.bin`.
    InvalidProgram,
    /// The usage aggregate violates the format's count, slot, or block-time
    /// invariants, or merging it with an existing aggregate overflowed.
    InvalidUsage(ProgramUsageError),
    /// `account` is outside this builder's chunk. Not an error — expected
    /// and routine in chunked/multi-pass builds; the account belongs to a
    /// different pass.
    OutOfChunk,
}

/// In-memory accumulation of the wallet -> program-id relation for one
/// **chunk** of the account id space, built while streaming the archive.
///
/// Every account and program id the archive can produce is already a dense,
/// 1-based integer bounded by the epoch's registry size (`CompactPubkey::Id`,
/// resolved with no hashing at all — see `blockzilla_format::KeyIndex`). So
/// rather than a hash map keyed by wallet id, this is a flat `Vec` indexed
/// directly by (chunk-relative) id: no hash computation, no probing, and
/// wallet ids come out of `write()` in ascending order for free (no sort
/// pass). The cost is a single upfront allocation sized to the whole chunk,
/// not just the wallets in it actually touched — see the module docs for why
/// this is chunked rather than sized to the whole registry.
#[derive(Debug)]
pub struct IndexBuilder {
    /// First registry id (1-based) this builder accepts as an account.
    chunk_start: u32,
    /// Upper bound for a valid program id (the full registry size — program
    /// ids are never chunked, since any program in the registry can be
    /// referenced by an account in this chunk).
    max_program_id: u32,
    /// Slot `i` holds usage aggregates sorted and unique by program id
    /// recorded for registry id `chunk_start + i`.
    wallet_programs: Vec<ProgramUsages>,
    distinct_wallets: u32,
}

impl IndexBuilder {
    /// `chunk_start..chunk_start + chunk_width` (registry ids) is the range
    /// of accounts this builder accepts. `max_program_id` bounds valid
    /// *program* ids, which are always the full registry range regardless
    /// of chunking.
    pub fn new(chunk_start: u32, chunk_width: u32, max_program_id: u32) -> Self {
        Self {
            chunk_start,
            max_program_id,
            wallet_programs: vec![ProgramUsages::new(); chunk_width as usize],
            distinct_wallets: 0,
        }
    }

    /// Add a usage aggregate for signer registry id `account`. Slots remain
    /// sorted and unique by program id. A repeated relation is checked and
    /// merged instead of creating a second on-disk entry.
    #[inline]
    pub fn record(&mut self, account: u32, usage: ProgramUsage) -> RecordOutcome {
        if usage.program_id == 0 || usage.program_id > self.max_program_id {
            return RecordOutcome::InvalidProgram;
        }
        if let Err(error) = usage.validate() {
            return RecordOutcome::InvalidUsage(error);
        }
        let Some(relative) = account.checked_sub(self.chunk_start) else {
            return RecordOutcome::OutOfChunk;
        };
        let Some(slot) = self.wallet_programs.get_mut(relative as usize) else {
            return RecordOutcome::OutOfChunk;
        };
        match slot.binary_search_by_key(&usage.program_id, ProgramUsage::program_id) {
            Ok(index) => match slot[index].checked_merge(usage) {
                Ok(merged) => slot[index] = merged,
                Err(error) => return RecordOutcome::InvalidUsage(error),
            },
            Err(index) => {
                if slot.is_empty() {
                    self.distinct_wallets += 1;
                }
                slot.insert(index, usage);
            }
        }
        RecordOutcome::Recorded
    }

    pub fn wallet_count(&self) -> usize {
        self.distinct_wallets as usize
    }

    /// Absorb another builder's recorded relations into this one — used to
    /// merge N threads' independent partial builders (one per disjoint
    /// block range, see `build::scan_into_builder_parallel`) back into a
    /// single builder for one chunk. Both builders must cover the exact same
    /// chunk (same `chunk_start`/width/`max_program_id`); panics otherwise,
    /// since merging mismatched chunks would silently corrupt wallet ids.
    /// Both inputs are sorted and unique by program id. Matching aggregates
    /// use checked count sums and extrema for slots and available block times.
    pub fn merge(&mut self, mut other: IndexBuilder) -> Result<(), ProgramUsageError> {
        assert_eq!(
            self.chunk_start, other.chunk_start,
            "merged builders must cover the same chunk"
        );
        assert_eq!(
            self.max_program_id, other.max_program_id,
            "merged builders must share max_program_id"
        );
        assert_eq!(
            self.wallet_programs.len(),
            other.wallet_programs.len(),
            "merged builders must cover the same chunk width"
        );
        for (mine, theirs) in self
            .wallet_programs
            .iter_mut()
            .zip(other.wallet_programs.iter_mut())
        {
            if theirs.is_empty() {
                continue;
            }
            if mine.is_empty() {
                self.distinct_wallets += 1;
                std::mem::swap(mine, theirs);
                continue;
            }
            let mut merged = ProgramUsages::with_capacity(mine.len() + theirs.len());
            let (mut left, mut right) = (0usize, 0usize);
            while left < mine.len() && right < theirs.len() {
                match mine[left].program_id.cmp(&theirs[right].program_id) {
                    std::cmp::Ordering::Less => {
                        merged.push(mine[left]);
                        left += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        merged.push(theirs[right]);
                        right += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        merged.push(mine[left].checked_merge(theirs[right])?);
                        left += 1;
                        right += 1;
                    }
                }
            }
            merged.extend_from_slice(&mine[left..]);
            merged.extend_from_slice(&theirs[right..]);
            *mine = merged;
        }
        Ok(())
    }

    /// Distinct program registry ids referenced across all wallets *in this
    /// chunk*. Not tracked incrementally (that would cost a branch on every
    /// `record` call for a count only needed once, at the end); computed
    /// with one pass over the already-accumulated data using a bitset sized
    /// to the full program-id space (no hashing here either).
    pub fn distinct_program_count(&self) -> usize {
        let mut seen = Bitset::new(self.max_program_id as usize + 1);
        let mut count = 0usize;
        for usages in &self.wallet_programs {
            for usage in usages {
                if seen.insert(usage.program_id as usize) {
                    count += 1;
                }
            }
        }
        count
    }

    /// Write this chunk's shard to `shard_dir` (e.g.
    /// `<out_dir>/shard-<N>`), creating it if needed. Returns this shard's
    /// wallet count (for the
    /// caller to aggregate into the top-level `IndexManifest`).
    pub fn write(&mut self, shard_dir: &Path) -> Result<u64, FormatError> {
        fs::create_dir_all(shard_dir).map_err(|source| FormatError::Io {
            path: shard_dir.display().to_string(),
            source,
        })?;
        let wallets_path = shard_dir.join(WALLETS_FILE);
        let relations_path = shard_dir.join(RELATIONS_FILE);
        write_wallets_and_relations(
            &wallets_path,
            &relations_path,
            self.chunk_start,
            &mut self.wallet_programs,
        )
    }
}

/// Tracks distinct program registry ids seen **across every chunk** of a
/// build. A per-chunk `IndexBuilder::distinct_program_count` can't simply be
/// summed across chunks — the same program (System, Token, ...) is
/// typically referenced by accounts in most/every chunk, so summing would
/// count it once per chunk. Program ids are never chunked (see
/// `IndexBuilder` docs), so one shared bitset sized to the full registry,
/// fed by every chunk's builder, gives the correct whole-epoch count.
pub struct ProgramTracker {
    seen: Bitset,
    count: usize,
    max_program_id: u32,
}

impl ProgramTracker {
    pub fn new(max_program_id: u32) -> Self {
        Self {
            seen: Bitset::new(max_program_id as usize + 1),
            count: 0,
            max_program_id,
        }
    }

    pub fn observe(&mut self, builder: &IndexBuilder) {
        for usages in &builder.wallet_programs {
            for usage in usages {
                self.observe_id(usage.program_id);
            }
        }
    }

    /// Record one already-validated program id. This is the streaming
    /// counterpart to [`Self::observe`] for builders that do not retain an
    /// [`IndexBuilder`] per shard.
    pub fn observe_id(&mut self, id: u32) {
        debug_assert!(id != 0 && id <= self.max_program_id);
        if self.seen.insert(id as usize) {
            self.count += 1;
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn ids(&self) -> impl Iterator<Item = u32> + '_ {
        (1..=self.max_program_id).filter(|id| self.seen.contains(*id as usize))
    }
}

/// Minimal fixed-size bitset. A `Vec<u64>` word array is 8x denser than
/// `Vec<bool>` for the tens-of-millions-of-ids scale a full epoch's registry
/// can reach — used for the one-time distinct-program scan at the end of a
/// build, and reusable (`pub(crate)`) for anything else that just needs a
/// cheap "have I seen this registry id" set (e.g. `build::discover_signers`).
pub(crate) struct Bitset {
    words: Vec<u64>,
}

impl Bitset {
    pub(crate) fn new(bits: usize) -> Self {
        Self {
            words: vec![0u64; bits.div_ceil(64)],
        }
    }

    /// Returns `true` the first time `bit` is inserted.
    #[inline]
    pub(crate) fn insert(&mut self, bit: usize) -> bool {
        let word = &mut self.words[bit / 64];
        let mask = 1u64 << (bit % 64);
        let was_set = *word & mask != 0;
        *word |= mask;
        !was_set
    }

    #[inline]
    pub(crate) fn contains(&self, bit: usize) -> bool {
        self.words
            .get(bit / 64)
            .is_some_and(|word| word & (1u64 << (bit % 64)) != 0)
    }
}

/// Returns this shard's wallet count.
fn write_wallets_and_relations(
    wallets_path: &Path,
    relations_path: &Path,
    chunk_start: u32,
    wallet_programs: &mut [ProgramUsages],
) -> Result<u64, FormatError> {
    let mut writer = ShardWriter::create_files(wallets_path, relations_path)?;
    for (relative_index, usages) in wallet_programs.iter_mut().enumerate() {
        if usages.is_empty() {
            continue;
        }
        debug_assert!(
            usages
                .windows(2)
                .all(|pair| pair[0].program_id < pair[1].program_id)
        );
        let relative_id =
            u32::try_from(relative_index).map_err(|_| FormatError::IntegerOverflow {
                file: "wallets.idx",
                path: wallets_path.display().to_string(),
            })?;
        let wallet_id =
            chunk_start
                .checked_add(relative_id)
                .ok_or_else(|| FormatError::IntegerOverflow {
                    file: "wallets.idx",
                    path: wallets_path.display().to_string(),
                })?;
        writer.push_sorted(wallet_id, usages)?;
    }
    writer.finish()
}

/// Streaming writer for one shard's canonical wallet/program relation.
///
/// Unlike [`IndexBuilder::write`], this does not require a dense
/// `chunk_width`-sized array or a second full copy of every relation. Callers
/// may feed non-empty wallets in ascending registry-id order and reuse one
/// small program scratch buffer between calls. Both table counts are patched
/// into their headers only after the stream finishes, producing byte-identical
/// files to the in-memory builder.
pub struct ShardWriter {
    wallets_path: PathBuf,
    relations_path: PathBuf,
    wallets_writer: BufWriter<File>,
    relations_writer: BufWriter<File>,
    wallet_count: u64,
    relation_count: u64,
    last_wallet_id: Option<u32>,
}

impl ShardWriter {
    pub fn create(shard_dir: &Path) -> Result<Self, FormatError> {
        fs::create_dir_all(shard_dir).map_err(|source| FormatError::Io {
            path: shard_dir.display().to_string(),
            source,
        })?;
        Self::create_files(
            &shard_dir.join(WALLETS_FILE),
            &shard_dir.join(RELATIONS_FILE),
        )
    }

    fn create_files(wallets_path: &Path, relations_path: &Path) -> Result<Self, FormatError> {
        let wallets_file = fs::File::create(wallets_path).map_err(|source| FormatError::Io {
            path: wallets_path.display().to_string(),
            source,
        })?;
        let relations_file =
            fs::File::create(relations_path).map_err(|source| FormatError::Io {
                path: relations_path.display().to_string(),
                source,
            })?;
        let mut wallets_writer = BufWriter::new(wallets_file);
        let mut relations_writer = BufWriter::new(relations_file);
        write_table_header(&mut wallets_writer, wallets_path, WALLETS_MAGIC, 0)?;
        write_table_header(&mut relations_writer, relations_path, RELATIONS_MAGIC, 0)?;
        Ok(Self {
            wallets_path: wallets_path.to_path_buf(),
            relations_path: relations_path.to_path_buf(),
            wallets_writer,
            relations_writer,
            wallet_count: 0,
            relation_count: 0,
            last_wallet_id: None,
        })
    }

    /// Append one non-empty list of valid usage records, strictly sorted and
    /// unique by program id.
    pub fn push_sorted(
        &mut self,
        wallet_id: u32,
        usages: &[ProgramUsage],
    ) -> Result<(), FormatError> {
        let invalid = |message: String| FormatError::InvalidWriterInput {
            path: self.wallets_path.display().to_string(),
            message,
        };
        if wallet_id == 0 || self.last_wallet_id.is_some_and(|last| wallet_id <= last) {
            return Err(invalid(format!(
                "wallet id {wallet_id} is zero or not greater than the previous id {:?}",
                self.last_wallet_id
            )));
        }
        if usages.is_empty()
            || usages
                .windows(2)
                .any(|pair| pair[0].program_id >= pair[1].program_id)
        {
            return Err(invalid(format!(
                "wallet id {wallet_id} has an empty, zero, duplicate, or unsorted program list"
            )));
        }
        let wallet_index =
            usize::try_from(self.wallet_count).map_err(|_| FormatError::IntegerOverflow {
                file: "wallets.idx",
                path: self.wallets_path.display().to_string(),
            })?;
        for usage in usages {
            usage
                .validate()
                .map_err(|source| FormatError::InvalidProgramUsage {
                    index: wallet_index,
                    source,
                })?;
        }
        let count = u32::try_from(usages.len()).map_err(|_| FormatError::IntegerOverflow {
            file: "programs.rel",
            path: self.relations_path.display().to_string(),
        })?;
        let next_wallet_count =
            self.wallet_count
                .checked_add(1)
                .ok_or_else(|| FormatError::IntegerOverflow {
                    file: "wallets.idx",
                    path: self.wallets_path.display().to_string(),
                })?;
        let next_relation_count = self
            .relation_count
            .checked_add(u64::from(count))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "programs.rel",
                path: self.relations_path.display().to_string(),
            })?;

        self.wallets_writer
            .write_all(&wallet_id.to_le_bytes())
            .and_then(|()| {
                self.wallets_writer
                    .write_all(&self.relation_count.to_le_bytes())
            })
            .and_then(|()| self.wallets_writer.write_all(&count.to_le_bytes()))
            .map_err(|source| FormatError::Io {
                path: self.wallets_path.display().to_string(),
                source,
            })?;
        for usage in usages {
            self.relations_writer
                .write_all(&usage.to_le_bytes())
                .map_err(|source| FormatError::Io {
                    path: self.relations_path.display().to_string(),
                    source,
                })?;
        }
        self.wallet_count = next_wallet_count;
        self.relation_count = next_relation_count;
        self.last_wallet_id = Some(wallet_id);
        Ok(())
    }

    pub fn finish(mut self) -> Result<u64, FormatError> {
        self.wallets_writer
            .flush()
            .map_err(|source| FormatError::Io {
                path: self.wallets_path.display().to_string(),
                source,
            })?;
        self.relations_writer
            .flush()
            .map_err(|source| FormatError::Io {
                path: self.relations_path.display().to_string(),
                source,
            })?;
        let mut wallets_file =
            self.wallets_writer
                .into_inner()
                .map_err(|error| FormatError::Io {
                    path: self.wallets_path.display().to_string(),
                    source: error.into_error(),
                })?;
        let mut relations_file =
            self.relations_writer
                .into_inner()
                .map_err(|error| FormatError::Io {
                    path: self.relations_path.display().to_string(),
                    source: error.into_error(),
                })?;
        rewrite_table_count(
            &mut wallets_file,
            &self.wallets_path,
            WALLETS_MAGIC,
            self.wallet_count,
        )?;
        rewrite_table_count(
            &mut relations_file,
            &self.relations_path,
            RELATIONS_MAGIC,
            self.relation_count,
        )?;
        Ok(self.wallet_count)
    }
}

fn write_table_header(
    writer: &mut impl Write,
    path: &Path,
    magic: [u8; 4],
    count: u64,
) -> Result<(), FormatError> {
    writer
        .write_all(&magic)
        .and_then(|()| writer.write_all(&FORMAT_VERSION.to_le_bytes()))
        .and_then(|()| writer.write_all(&count.to_le_bytes()))
        .map_err(|source| FormatError::Io {
            path: path.display().to_string(),
            source,
        })
}

fn rewrite_table_count(
    file: &mut File,
    path: &Path,
    magic: [u8; 4],
    count: u64,
) -> Result<(), FormatError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| FormatError::Io {
            path: path.display().to_string(),
            source,
        })?;
    write_table_header(file, path, magic, count)
}

/// Write the compact registry-id -> pubkey dictionary for every distinct
/// program referenced by this index. Entries must be strictly id-sorted.
pub fn write_program_map(
    index_dir: &Path,
    entries: &[(u32, [u8; 32])],
) -> Result<IndexFileBinding, FormatError> {
    if entries
        .windows(2)
        .any(|pair| pair[0].0 == 0 || pair[0].0 >= pair[1].0)
        || entries.last().is_some_and(|entry| entry.0 == 0)
    {
        return Err(FormatError::UnsortedProgramMap {
            path: index_dir.join(PROGRAM_MAP_FILE).display().to_string(),
            index: entries
                .windows(2)
                .position(|pair| pair[0].0 == 0 || pair[0].0 >= pair[1].0)
                .unwrap_or(entries.len().saturating_sub(1)),
        });
    }
    let path = index_dir.join(PROGRAM_MAP_FILE);
    let file = File::create(&path).map_err(|source| FormatError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let mut writer = BufWriter::new(file);
    let count = u64::try_from(entries.len()).map_err(|_| FormatError::IntegerOverflow {
        file: "programs.map",
        path: path.display().to_string(),
    })?;
    writer
        .write_all(&PROGRAM_MAP_MAGIC)
        .and_then(|()| writer.write_all(&FORMAT_VERSION.to_le_bytes()))
        .and_then(|()| writer.write_all(&count.to_le_bytes()))
        .map_err(|source| FormatError::Io {
            path: path.display().to_string(),
            source,
        })?;
    for (id, pubkey) in entries {
        writer
            .write_all(&id.to_le_bytes())
            .and_then(|()| writer.write_all(pubkey))
            .map_err(|source| FormatError::Io {
                path: path.display().to_string(),
                source,
            })?;
    }
    writer.flush().map_err(|source| FormatError::Io {
        path: path.display().to_string(),
        source,
    })?;
    drop(writer);
    let file = open_file(&path)?;
    binding_for_open_file(&file, &path, "programs.map")
}

pub struct ProgramMapReader {
    file: File,
    path: PathBuf,
    count: usize,
    identity: OpenFileIdentity,
}

impl ProgramMapReader {
    pub fn open_verified(
        index_dir: &Path,
        binding: &IndexFileBinding,
        expected_count: u64,
    ) -> Result<Self, FormatError> {
        let path = index_dir.join(PROGRAM_MAP_FILE);
        let file = open_file(&path)?;
        let len = file_len(&file, &path, "programs.map")?;
        let header = read_header(
            &file,
            &path,
            "programs.map",
            PROGRAM_MAP_MAGIC,
            PROGRAM_MAP_HEADER_LEN,
        )?;
        let count_u64 = u64::from_le_bytes(header[8..16].try_into().unwrap());
        if count_u64 != expected_count {
            return Err(FormatError::ContentBindingMismatch {
                file: "programs.map",
                path: path.display().to_string(),
            });
        }
        let count = usize::try_from(count_u64).map_err(|_| FormatError::IntegerOverflow {
            file: "programs.map",
            path: path.display().to_string(),
        })?;
        let expected_len = count
            .checked_mul(PROGRAM_MAP_RECORD_LEN)
            .and_then(|body| PROGRAM_MAP_HEADER_LEN.checked_add(body))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "programs.map",
                path: path.display().to_string(),
            })?;
        if len != expected_len {
            return Err(FormatError::InvalidLength {
                file: "programs.map",
                path: path.display().to_string(),
                expected: expected_len,
                found: len,
            });
        }

        let identity = open_file_identity(&file, &path)?;
        let actual = binding_for_open_file(&file, &path, "programs.map")?;
        if actual != *binding {
            return Err(FormatError::ContentBindingMismatch {
                file: "programs.map",
                path: path.display().to_string(),
            });
        }

        let mut scan = BufReader::with_capacity(
            SHARD_READER_BUFFER_SIZE,
            file.try_clone().map_err(|source| FormatError::Io {
                path: path.display().to_string(),
                source,
            })?,
        );
        scan.seek(SeekFrom::Start(PROGRAM_MAP_HEADER_LEN as u64))
            .map_err(|source| FormatError::Io {
                path: path.display().to_string(),
                source,
            })?;
        let mut previous = None;
        let mut record = [0u8; PROGRAM_MAP_RECORD_LEN];
        for index in 0..count {
            scan.read_exact(&mut record)
                .map_err(|source| FormatError::Io {
                    path: path.display().to_string(),
                    source,
                })?;
            let id = u32::from_le_bytes(record[..4].try_into().unwrap());
            if id == 0 || previous.is_some_and(|previous| previous >= id) {
                return Err(FormatError::UnsortedProgramMap {
                    path: path.display().to_string(),
                    index,
                });
            }
            previous = Some(id);
        }
        Ok(Self {
            file,
            path,
            count,
            identity,
        })
    }

    pub fn resolve(&self, program_id: u32) -> Result<[u8; 32], FormatError> {
        let mut low = 0usize;
        let mut high = self.count;
        while low < high {
            let mid = low + (high - low) / 2;
            let record = self.record_at(mid)?;
            let id = u32::from_le_bytes(record[..4].try_into().unwrap());
            match id.cmp(&program_id) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(record[4..].try_into().unwrap()),
            }
        }
        Err(FormatError::MissingProgram { program_id })
    }

    /// Confirm that the retained, content-verified file was not modified
    /// while one or more program ids were being resolved.
    pub fn verify_unchanged(&self) -> Result<(), FormatError> {
        verify_open_file_identity(&self.file, &self.path, "programs.map", self.identity)
    }

    fn record_at(&self, index: usize) -> Result<[u8; PROGRAM_MAP_RECORD_LEN], FormatError> {
        let offset = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_mul(PROGRAM_MAP_RECORD_LEN as u64))
            .and_then(|offset| offset.checked_add(PROGRAM_MAP_HEADER_LEN as u64))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "programs.map",
                path: self.path.display().to_string(),
            })?;
        let mut record = [0u8; PROGRAM_MAP_RECORD_LEN];
        self.file
            .read_exact_at(&mut record, offset)
            .map_err(|source| FormatError::Io {
                path: self.path.display().to_string(),
                source,
            })?;
        Ok(record)
    }
}

/// Read-only handle onto one shard. Files remain open and queries use
/// positioned reads, so the safe API remains sound even if another process
/// unexpectedly replaces or truncates a path after it is opened.
pub struct IndexReader {
    wallets: File,
    wallets_path: PathBuf,
    wallets_identity: OpenFileIdentity,
    wallet_count: usize,
    relations: File,
    relations_path: PathBuf,
    relations_identity: OpenFileIdentity,
    relation_count: u64,
}

impl IndexReader {
    /// Open one shard directory (e.g. `<out_dir>/shard-<N>`, as computed by
    /// `IndexManifest::shard_dir_name`).
    pub fn open(shard_dir: &Path) -> Result<Self, FormatError> {
        let wallets_path = shard_dir.join(WALLETS_FILE);
        let wallets = open_file(&wallets_path)?;
        let wallets_len = file_len(&wallets, &wallets_path, "wallets.idx")?;
        let wallets_header = read_header(
            &wallets,
            &wallets_path,
            "wallets.idx",
            WALLETS_MAGIC,
            WALLETS_HEADER_LEN,
        )?;
        let wallet_count = u64::from_le_bytes(wallets_header[8..16].try_into().unwrap());
        let wallet_count_usize =
            usize::try_from(wallet_count).map_err(|_| FormatError::IntegerOverflow {
                file: "wallets.idx",
                path: wallets_path.display().to_string(),
            })?;
        let expected = wallet_count_usize
            .checked_mul(WALLET_RECORD_LEN)
            .and_then(|body| WALLETS_HEADER_LEN.checked_add(body))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "wallets.idx",
                path: wallets_path.display().to_string(),
            })?;
        if wallets_len != expected {
            return Err(FormatError::InvalidLength {
                file: "wallets.idx",
                path: wallets_path.display().to_string(),
                expected,
                found: wallets_len,
            });
        }
        let mut wallets_scan = BufReader::with_capacity(
            SHARD_READER_BUFFER_SIZE,
            wallets.try_clone().map_err(|source| FormatError::Io {
                path: wallets_path.display().to_string(),
                source,
            })?,
        );
        wallets_scan
            .seek(SeekFrom::Start(WALLETS_HEADER_LEN as u64))
            .map_err(|source| FormatError::Io {
                path: wallets_path.display().to_string(),
                source,
            })?;
        let mut previous_wallet = None;
        let mut record = [0u8; WALLET_RECORD_LEN];
        for index in 0..wallet_count_usize {
            wallets_scan
                .read_exact(&mut record)
                .map_err(|source| FormatError::Io {
                    path: wallets_path.display().to_string(),
                    source,
                })?;
            let current = u32::from_le_bytes(record[..4].try_into().unwrap());
            if previous_wallet.is_some_and(|previous| previous >= current) {
                return Err(FormatError::Unsorted {
                    path: wallets_path.display().to_string(),
                    index,
                });
            }
            previous_wallet = Some(current);
        }

        let relations_path = shard_dir.join(RELATIONS_FILE);
        let relations = open_file(&relations_path)?;
        let relations_len = file_len(&relations, &relations_path, "programs.rel")?;
        let relations_header = read_header(
            &relations,
            &relations_path,
            "programs.rel",
            RELATIONS_MAGIC,
            RELATIONS_HEADER_LEN,
        )?;
        let relation_count = u64::from_le_bytes(relations_header[8..16].try_into().unwrap());
        let relation_count_usize =
            usize::try_from(relation_count).map_err(|_| FormatError::IntegerOverflow {
                file: "programs.rel",
                path: relations_path.display().to_string(),
            })?;
        let expected = relation_count_usize
            .checked_mul(PROGRAM_USAGE_RECORD_LEN)
            .and_then(|body| RELATIONS_HEADER_LEN.checked_add(body))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "programs.rel",
                path: relations_path.display().to_string(),
            })?;
        if relations_len != expected {
            return Err(FormatError::InvalidLength {
                file: "programs.rel",
                path: relations_path.display().to_string(),
                expected,
                found: relations_len,
            });
        }

        let wallets_identity = open_file_identity(&wallets, &wallets_path)?;
        let relations_identity = open_file_identity(&relations, &relations_path)?;
        Ok(Self {
            wallets,
            wallets_path,
            wallets_identity,
            wallet_count: wallet_count_usize,
            relations,
            relations_path,
            relations_identity,
            relation_count,
        })
    }

    /// Open and content-verify the exact file handles retained by this
    /// reader. Verification happens after opening, so a pathname replacement
    /// cannot switch the queried inode between hashing and lookup.
    pub fn open_verified(shard_dir: &Path, binding: &ShardBinding) -> Result<Self, FormatError> {
        let mut reader = Self::open(shard_dir)?;
        let wallets_identity = open_file_identity(&reader.wallets, &reader.wallets_path)?;
        let relations_identity = open_file_identity(&reader.relations, &reader.relations_path)?;
        reader.verify_content(binding)?;
        reader.wallets_identity = wallets_identity;
        reader.relations_identity = relations_identity;
        Ok(reader)
    }

    fn verify_content(&self, binding: &ShardBinding) -> Result<(), FormatError> {
        let wallets = binding_for_open_file(&self.wallets, &self.wallets_path, "wallets.idx")?;
        if wallets != binding.wallets {
            return Err(FormatError::ContentBindingMismatch {
                file: "wallets.idx",
                path: self.wallets_path.display().to_string(),
            });
        }
        let relations =
            binding_for_open_file(&self.relations, &self.relations_path, "programs.rel")?;
        if relations != binding.relations {
            return Err(FormatError::ContentBindingMismatch {
                file: "programs.rel",
                path: self.relations_path.display().to_string(),
            });
        }
        Ok(())
    }

    /// Look up the per-program usage aggregates from successful transactions
    /// signed by `wallet`, including direct and inner/CPI instructions. Empty
    /// if not found. `wallet` must be in this shard (see
    /// `IndexManifest::shard_dir_name`) — a wallet from a different shard
    /// always returns empty, since it can't be present here.
    pub fn query(&self, wallet: u32) -> Result<Vec<ProgramUsage>, FormatError> {
        let Some((index, record)) = self.binary_search(wallet)? else {
            return Ok(Vec::new());
        };
        let offset = u64::from_le_bytes(record[4..12].try_into().unwrap());
        let count = u32::from_le_bytes(record[12..16].try_into().unwrap());
        self.read_program_list(index, offset, count)
    }

    fn read_program_list(
        &self,
        index: usize,
        offset: u64,
        count: u32,
    ) -> Result<Vec<ProgramUsage>, FormatError> {
        let end = offset.checked_add(u64::from(count)).ok_or({
            FormatError::RelationRangeOutOfBounds {
                index,
                offset,
                end: u64::MAX,
                len: self.relation_count,
            }
        })?;

        if end > self.relation_count {
            return Err(FormatError::RelationRangeOutOfBounds {
                index,
                offset,
                end,
                len: self.relation_count,
            });
        }

        let byte_count = usize::try_from(count)
            .ok()
            .and_then(|count| count.checked_mul(PROGRAM_USAGE_RECORD_LEN))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "programs.rel",
                path: self.relations_path.display().to_string(),
            })?;
        let byte_offset = offset
            .checked_mul(PROGRAM_USAGE_RECORD_LEN as u64)
            .and_then(|offset| offset.checked_add(RELATIONS_HEADER_LEN as u64))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "programs.rel",
                path: self.relations_path.display().to_string(),
            })?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(byte_count)
            .map_err(|_| FormatError::IntegerOverflow {
                file: "programs.rel",
                path: self.relations_path.display().to_string(),
            })?;
        bytes.resize(byte_count, 0);
        self.relations
            .read_exact_at(&mut bytes, byte_offset)
            .map_err(|source| FormatError::Io {
                path: self.relations_path.display().to_string(),
                source,
            })?;

        let mut result = Vec::with_capacity(count as usize);
        for encoded in bytes.chunks_exact(PROGRAM_USAGE_RECORD_LEN) {
            let usage = ProgramUsage::from_le_bytes(encoded.try_into().unwrap());
            usage
                .validate()
                .map_err(|source| FormatError::InvalidProgramUsage { index, source })?;
            result.push(usage);
        }
        if result
            .windows(2)
            .any(|pair| pair[0].program_id >= pair[1].program_id)
        {
            return Err(FormatError::InvalidProgramList { index });
        }
        Ok(result)
    }

    fn validate_for_binding(
        &self,
        shard: u32,
        chunk_width: u32,
        registry_entries: u32,
    ) -> Result<(), FormatError> {
        if chunk_width == 0 || registry_entries == 0 {
            return Err(FormatError::InvalidManifest {
                path: self.wallets_path.display().to_string(),
                message: "chunk_width and registry_entries must be nonzero while binding a shard"
                    .into(),
            });
        }
        let first_u64 = u64::from(shard)
            .checked_mul(u64::from(chunk_width))
            .and_then(|offset| offset.checked_add(1))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "wallets.idx",
                path: self.wallets_path.display().to_string(),
            })?;
        if first_u64 > u64::from(registry_entries) {
            return Err(FormatError::InvalidManifest {
                path: self.wallets_path.display().to_string(),
                message: format!("shard {shard} starts beyond registry_entries"),
            });
        }
        let last_u64 = first_u64
            .saturating_add(u64::from(chunk_width) - 1)
            .min(u64::from(registry_entries));
        let first_wallet_id = first_u64 as u32;
        let last_wallet_id = last_u64 as u32;

        let mut expected_offset = 0u64;
        for index in 0..self.wallet_count {
            let record = self.wallet_record_at(index)?;
            let wallet_id = u32::from_le_bytes(record[..4].try_into().unwrap());
            if wallet_id < first_wallet_id || wallet_id > last_wallet_id {
                return Err(FormatError::WalletOutsideShard {
                    index,
                    wallet_id,
                    first_wallet_id,
                    last_wallet_id,
                });
            }
            let offset = u64::from_le_bytes(record[4..12].try_into().unwrap());
            let count = u32::from_le_bytes(record[12..16].try_into().unwrap());
            if offset != expected_offset {
                return Err(FormatError::NonCanonicalRelationLayout {
                    index,
                    expected_offset,
                    found_offset: offset,
                });
            }
            if count == 0 {
                return Err(FormatError::InvalidProgramList { index });
            }
            let usages = self.read_program_list(index, offset, count)?;
            if usages
                .last()
                .is_some_and(|usage| usage.program_id > registry_entries)
            {
                return Err(FormatError::InvalidProgramList { index });
            }
            expected_offset = offset.checked_add(u64::from(count)).ok_or({
                FormatError::RelationRangeOutOfBounds {
                    index,
                    offset,
                    end: u64::MAX,
                    len: self.relation_count,
                }
            })?;
        }
        if expected_offset != self.relation_count {
            return Err(FormatError::NonCanonicalRelationLayout {
                index: self.wallet_count,
                expected_offset,
                found_offset: self.relation_count,
            });
        }
        Ok(())
    }

    fn binary_search(
        &self,
        wallet: u32,
    ) -> Result<Option<(usize, [u8; WALLET_RECORD_LEN])>, FormatError> {
        let mut low = 0usize;
        let mut high = self.wallet_count;
        while low < high {
            let mid = low + (high - low) / 2;
            let record = self.wallet_record_at(mid)?;
            let current = u32::from_le_bytes(record[..4].try_into().unwrap());
            match current.cmp(&wallet) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return Ok(Some((mid, record))),
            }
        }
        Ok(None)
    }

    fn wallet_record_at(&self, index: usize) -> Result<[u8; WALLET_RECORD_LEN], FormatError> {
        let offset = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_mul(WALLET_RECORD_LEN as u64))
            .and_then(|offset| offset.checked_add(WALLETS_HEADER_LEN as u64))
            .ok_or_else(|| FormatError::IntegerOverflow {
                file: "wallets.idx",
                path: self.wallets_path.display().to_string(),
            })?;
        let mut record = [0u8; WALLET_RECORD_LEN];
        self.wallets
            .read_exact_at(&mut record, offset)
            .map_err(|source| FormatError::Io {
                path: self.wallets_path.display().to_string(),
                source,
            })?;
        Ok(record)
    }

    pub fn wallet_count(&self) -> u64 {
        self.wallet_count as u64
    }

    /// Confirm that both retained, content-verified shard files were not
    /// modified while the lookup was running. Positioned reads remain on the
    /// authenticated open generations; this check conservatively also rejects
    /// metadata changes such as renaming the retained inode.
    pub fn verify_unchanged(&self) -> Result<(), FormatError> {
        verify_open_file_identity(
            &self.wallets,
            &self.wallets_path,
            "wallets.idx",
            self.wallets_identity,
        )?;
        verify_open_file_identity(
            &self.relations,
            &self.relations_path,
            "programs.rel",
            self.relations_identity,
        )
    }
}

pub fn bind_shard(
    shard: u32,
    shard_dir: &Path,
    chunk_width: u32,
    registry_entries: u32,
) -> Result<ShardBinding, FormatError> {
    let reader = IndexReader::open(shard_dir)?;
    reader.validate_for_binding(shard, chunk_width, registry_entries)?;
    let binding = ShardBinding {
        shard,
        wallets: binding_for_open_file(&reader.wallets, &reader.wallets_path, "wallets.idx")?,
        relations: binding_for_open_file(
            &reader.relations,
            &reader.relations_path,
            "programs.rel",
        )?,
    };
    reader.verify_unchanged()?;
    Ok(binding)
}

fn binding_for_open_file(
    file: &File,
    path: &Path,
    kind: &'static str,
) -> Result<IndexFileBinding, FormatError> {
    let initial_identity = open_file_identity(file, path)?;
    let size = initial_identity.size;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; SHARD_READER_BUFFER_SIZE];
    let mut offset = 0u64;
    while offset < size {
        let remaining = usize::try_from((size - offset).min(buffer.len() as u64)).unwrap();
        let read = file
            .read_at(&mut buffer[..remaining], offset)
            .map_err(|source| FormatError::Io {
                path: path.display().to_string(),
                source,
            })?;
        if read == 0 {
            return Err(FormatError::Io {
                path: path.display().to_string(),
                source: io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("{kind} was truncated while hashing"),
                ),
            });
        }
        hasher.update(&buffer[..read]);
        offset += read as u64;
    }
    if open_file_identity(file, path)? != initial_identity {
        return Err(FormatError::ContentBindingMismatch {
            file: kind,
            path: path.display().to_string(),
        });
    }
    Ok(IndexFileBinding {
        size,
        sha256: hex_lower(&hasher.finalize()),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OpenFileIdentity {
    size: u64,
    device: u64,
    inode: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

fn open_file_identity(file: &File, path: &Path) -> Result<OpenFileIdentity, FormatError> {
    let metadata = file.metadata().map_err(|source| FormatError::Io {
        path: path.display().to_string(),
        source,
    })?;
    Ok(OpenFileIdentity {
        size: metadata.len(),
        device: metadata.dev(),
        inode: metadata.ino(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
        changed_seconds: metadata.ctime(),
        changed_nanoseconds: metadata.ctime_nsec(),
    })
}

fn verify_open_file_identity(
    file: &File,
    path: &Path,
    kind: &'static str,
    expected: OpenFileIdentity,
) -> Result<(), FormatError> {
    if open_file_identity(file, path)? != expected {
        return Err(FormatError::ContentBindingMismatch {
            file: kind,
            path: path.display().to_string(),
        });
    }
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

pub(crate) fn open_file(path: &Path) -> Result<File, FormatError> {
    let owned = rustix::fs::open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(io::Error::from)
    .map_err(|source| FormatError::Io {
        path: path.display().to_string(),
        source,
    })?;
    let file = File::from(owned);
    let metadata = file.metadata().map_err(|source| FormatError::Io {
        path: path.display().to_string(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(FormatError::Io {
            path: path.display().to_string(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "not a regular file"),
        });
    }
    Ok(file)
}

fn file_len(file: &File, path: &Path, kind: &'static str) -> Result<usize, FormatError> {
    let len = file
        .metadata()
        .map_err(|source| FormatError::Io {
            path: path.display().to_string(),
            source,
        })?
        .len();
    usize::try_from(len).map_err(|_| FormatError::IntegerOverflow {
        file: kind,
        path: path.display().to_string(),
    })
}

fn read_header(
    file_handle: &File,
    path: &Path,
    kind: &'static str,
    magic: [u8; 4],
    header_len: usize,
) -> Result<[u8; WALLETS_HEADER_LEN], FormatError> {
    let found = file_len(file_handle, path, kind)?;
    if found < header_len {
        return Err(FormatError::Truncated {
            file: kind,
            path: path.display().to_string(),
            expected: header_len,
            found,
        });
    }
    debug_assert_eq!(header_len, WALLETS_HEADER_LEN);
    let mut data = [0u8; WALLETS_HEADER_LEN];
    file_handle
        .read_exact_at(&mut data, 0)
        .map_err(|source| FormatError::Io {
            path: path.display().to_string(),
            source,
        })?;
    if data[0..4] != magic {
        return Err(FormatError::BadMagic {
            file: kind,
            path: path.display().to_string(),
        });
    }
    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version != FORMAT_VERSION {
        return Err(FormatError::UnsupportedVersion {
            file: kind,
            path: path.display().to_string(),
            version,
        });
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(program_id: u32) -> ProgramUsage {
        ProgramUsage::new_transaction(program_id, 1, 0, 100, Some(1_000)).unwrap()
    }

    fn program_ids(usages: &[ProgramUsage]) -> Vec<u32> {
        usages.iter().map(ProgramUsage::program_id).collect()
    }

    fn test_manifest(registry_entries: u32, chunk_width: u32) -> IndexManifest {
        let shard_count = registry_entries.div_ceil(chunk_width);
        IndexManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            format_version: FORMAT_VERSION,
            semantics: IndexSemantics::current(),
            complete: true,
            omissions: OmissionCounts::default(),
            binding_kind: GenerationBindingKind::PublishedManifest,
            cluster_id: "testnet".into(),
            epoch: 1,
            archive_root: "/archive".into(),
            generation_id: "gen".into(),
            generation_digest: "1".repeat(64),
            archive_wire_profile:
                blockzilla_read_sdk::ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1,
            registry: IndexFileBinding {
                size: u64::from(registry_entries) * 32,
                sha256: "2".repeat(64),
            },
            registry_file_identity: RegistryFileIdentity {
                size: u64::from(registry_entries) * 32,
                device: 1,
                inode: 2,
                modified_seconds: 3,
                modified_nanoseconds: 4,
                changed_seconds: 5,
                changed_nanoseconds: 6,
            },
            registry_index: IndexFileBinding {
                size: REGISTRY_INDEX_MIN_LEN,
                sha256: "6".repeat(64),
            },
            registry_index_file_identity: RegistryFileIdentity {
                size: REGISTRY_INDEX_MIN_LEN,
                device: 1,
                inode: 7,
                modified_seconds: 3,
                modified_nanoseconds: 4,
                changed_seconds: 5,
                changed_nanoseconds: 6,
            },
            registry_entries,
            chunk_width,
            shard_count,
            shards: (0..shard_count)
                .map(|shard| ShardBinding {
                    shard,
                    wallets: IndexFileBinding {
                        size: WALLETS_HEADER_LEN as u64,
                        sha256: "3".repeat(64),
                    },
                    relations: IndexFileBinding {
                        size: RELATIONS_HEADER_LEN as u64,
                        sha256: "4".repeat(64),
                    },
                })
                .collect(),
            program_map: IndexFileBinding {
                size: PROGRAM_MAP_HEADER_LEN as u64,
                sha256: "5".repeat(64),
            },
            wallet_count: 0,
            program_count: 0,
            transactions_scanned: 0,
            blocks_scanned: 0,
            failed_transactions_excluded: 0,
            built_unix_time: 0,
            tool_version: "test".into(),
        }
    }

    #[test]
    fn program_usage_record_is_fixed_52_byte_little_endian() {
        let usage = ProgramUsage {
            program_id: 0x0102_0304,
            direct_instruction_count: 5,
            inner_instruction_count: 7,
            transaction_count: 3,
            first_seen_slot: 9,
            last_seen_slot: 10,
            min_block_time: -11,
            max_block_time: 12,
            timed_transaction_count: 2,
        };
        usage.validate().unwrap();

        let bytes = usage.to_le_bytes();
        assert_eq!(PROGRAM_USAGE_RECORD_LEN, 52);
        assert_eq!(&bytes[0..4], &usage.program_id.to_le_bytes());
        assert_eq!(&bytes[4..8], &usage.direct_instruction_count.to_le_bytes());
        assert_eq!(&bytes[8..12], &usage.inner_instruction_count.to_le_bytes());
        assert_eq!(&bytes[12..16], &usage.transaction_count.to_le_bytes());
        assert_eq!(&bytes[16..24], &usage.first_seen_slot.to_le_bytes());
        assert_eq!(&bytes[24..32], &usage.last_seen_slot.to_le_bytes());
        assert_eq!(&bytes[32..40], &usage.min_block_time.to_le_bytes());
        assert_eq!(&bytes[40..48], &usage.max_block_time.to_le_bytes());
        assert_eq!(&bytes[48..52], &usage.timed_transaction_count.to_le_bytes());
        assert_eq!(ProgramUsage::from_le_bytes(bytes), usage);
    }

    #[test]
    fn dense_builder_empty_slot_is_three_machine_words() {
        assert_eq!(
            std::mem::size_of::<ProgramUsages>(),
            3 * std::mem::size_of::<usize>()
        );
    }

    #[test]
    fn program_usage_validates_counts_slots_and_missing_time_sentinel() {
        assert_eq!(
            ProgramUsage::new_transaction(0, 1, 0, 1, None),
            Err(ProgramUsageError::InvalidProgramId)
        );
        assert_eq!(
            ProgramUsage::new_transaction(1, 0, 0, 1, None),
            Err(ProgramUsageError::EmptyInstructionCounts)
        );

        let missing_time = ProgramUsage::new_transaction(1, 1, 0, 10, None).unwrap();
        assert_eq!(missing_time.timed_transaction_count, 0);
        assert_eq!(
            (missing_time.min_block_time, missing_time.max_block_time),
            (
                PROGRAM_USAGE_MISSING_BLOCK_TIME,
                PROGRAM_USAGE_MISSING_BLOCK_TIME
            )
        );
        assert_eq!(missing_time.average_timed_transaction_gap_seconds(), None);

        let mut invalid = missing_time;
        invalid.transaction_count = 2;
        assert!(matches!(
            invalid.validate(),
            Err(ProgramUsageError::TransactionCountExceedsInstructions { .. })
        ));

        let mut invalid = missing_time;
        invalid.timed_transaction_count = 2;
        assert!(matches!(
            invalid.validate(),
            Err(ProgramUsageError::TimedTransactionCountExceedsTransactions { .. })
        ));

        let mut invalid = missing_time;
        invalid.first_seen_slot = 11;
        assert!(matches!(
            invalid.validate(),
            Err(ProgramUsageError::InvalidSlotRange { .. })
        ));

        let mut invalid = missing_time;
        invalid.min_block_time = 0;
        assert_eq!(
            invalid.validate(),
            Err(ProgramUsageError::InconsistentMissingBlockTime)
        );

        let mut invalid = ProgramUsage::new_transaction(1, 1, 0, 10, Some(20)).unwrap();
        invalid.max_block_time = 19;
        assert!(matches!(
            invalid.validate(),
            Err(ProgramUsageError::InvalidBlockTimeRange { .. })
        ));
    }

    #[test]
    fn program_usage_merge_is_order_independent_and_uses_time_extrema() {
        let first = ProgramUsage::new_transaction(7, 2, 0, 20, Some(110)).unwrap();
        let untimed = ProgramUsage::new_transaction(7, 0, 3, 10, None).unwrap();
        let last = ProgramUsage::new_transaction(7, 1, 1, 30, Some(90)).unwrap();

        let forward = first
            .checked_merge(untimed)
            .unwrap()
            .checked_merge(last)
            .unwrap();
        let reverse = last
            .checked_merge(first)
            .unwrap()
            .checked_merge(untimed)
            .unwrap();
        assert_eq!(forward, reverse);
        assert_eq!(forward.direct_instruction_count, 3);
        assert_eq!(forward.inner_instruction_count, 4);
        assert_eq!(forward.transaction_count, 3);
        assert_eq!((forward.first_seen_slot, forward.last_seen_slot), (10, 30));
        assert_eq!((forward.min_block_time, forward.max_block_time), (90, 110));
        assert_eq!(forward.timed_transaction_count, 2);
        assert_eq!(forward.average_timed_transaction_gap_seconds(), Some(20.0));
    }

    #[test]
    fn program_usage_merge_reports_count_overflow() {
        let left = ProgramUsage {
            direct_instruction_count: u32::MAX,
            ..usage(8)
        };
        assert_eq!(
            left.checked_merge(usage(8)),
            Err(ProgramUsageError::CountOverflow {
                field: "direct_instruction_count"
            })
        );
    }

    #[test]
    fn round_trips_empty_shard() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = IndexBuilder::new(1, 1000, 1000);
        let shard_dir = dir.path().join("shard-0");
        let wallet_count = builder.write(&shard_dir).unwrap();
        assert_eq!(wallet_count, 0);
        let reader = IndexReader::open(&shard_dir).unwrap();
        assert_eq!(reader.wallet_count(), 0);
        assert_eq!(reader.query(1).unwrap(), Vec::<ProgramUsage>::new());
    }

    #[test]
    fn round_trips_populated_shard() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = IndexBuilder::new(1, 1000, 1000);
        assert_eq!(builder.record(1, usage(100)), RecordOutcome::Recorded);
        assert_eq!(builder.record(1, usage(101)), RecordOutcome::Recorded);
        assert_eq!(builder.record(2, usage(100)), RecordOutcome::Recorded);
        assert_eq!(builder.record(3, usage(102)), RecordOutcome::Recorded);
        // A repeated relation merges its aggregate into one relation entry.
        assert_eq!(builder.record(1, usage(100)), RecordOutcome::Recorded);

        assert_eq!(builder.wallet_count(), 3);
        assert_eq!(builder.distinct_program_count(), 3);

        let shard_dir = dir.path().join("shard-0");
        let wallet_count = builder.write(&shard_dir).unwrap();
        assert_eq!(wallet_count, 3);

        let reader = IndexReader::open(&shard_dir).unwrap();
        assert_eq!(reader.wallet_count(), 3);

        let wallet1 = reader.query(1).unwrap();
        assert_eq!(program_ids(&wallet1), vec![100, 101]);
        assert_eq!(wallet1[0].direct_instruction_count, 2);
        assert_eq!(wallet1[0].transaction_count, 2);

        assert_eq!(reader.query(2).unwrap(), vec![usage(100)]);
        assert_eq!(reader.query(3).unwrap(), vec![usage(102)]);
        assert_eq!(reader.query(4).unwrap(), Vec::<ProgramUsage>::new());
    }

    #[test]
    fn streaming_shard_writer_is_byte_identical_to_index_builder() {
        let dir = tempfile::tempdir().unwrap();
        let builder_dir = dir.path().join("builder");
        let streaming_dir = dir.path().join("streaming");

        let mut builder = IndexBuilder::new(1, 12, 1000);
        for (wallet, programs) in [(1, &[4, 9][..]), (3, &[2, 7, 20][..]), (12, &[999][..])] {
            for &program in programs {
                assert_eq!(
                    builder.record(wallet, usage(program)),
                    RecordOutcome::Recorded
                );
            }
        }
        assert_eq!(builder.write(&builder_dir).unwrap(), 3);

        let mut writer = ShardWriter::create(&streaming_dir).unwrap();
        writer.push_sorted(1, &[usage(4), usage(9)]).unwrap();
        writer
            .push_sorted(3, &[usage(2), usage(7), usage(20)])
            .unwrap();
        writer.push_sorted(12, &[usage(999)]).unwrap();
        assert_eq!(writer.finish().unwrap(), 3);

        assert_eq!(
            fs::read(builder_dir.join(WALLETS_FILE)).unwrap(),
            fs::read(streaming_dir.join(WALLETS_FILE)).unwrap()
        );
        assert_eq!(
            fs::read(builder_dir.join(RELATIONS_FILE)).unwrap(),
            fs::read(streaming_dir.join(RELATIONS_FILE)).unwrap()
        );
    }

    #[test]
    fn streaming_shard_writer_rejects_noncanonical_input() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = ShardWriter::create(dir.path()).unwrap();
        assert!(matches!(
            writer.push_sorted(0, &[usage(1)]),
            Err(FormatError::InvalidWriterInput { .. })
        ));
        assert!(matches!(
            writer.push_sorted(1, &[]),
            Err(FormatError::InvalidWriterInput { .. })
        ));
        let mut invalid_usage = usage(2);
        invalid_usage.transaction_count = 0;
        assert!(matches!(
            writer.push_sorted(1, &[invalid_usage]),
            Err(FormatError::InvalidProgramUsage {
                source: ProgramUsageError::EmptyTransactionCount,
                ..
            })
        ));
        assert!(matches!(
            writer.push_sorted(1, &[usage(2), usage(2)]),
            Err(FormatError::InvalidWriterInput { .. })
        ));
        writer.push_sorted(1, &[usage(2), usage(3)]).unwrap();
        assert!(matches!(
            writer.push_sorted(1, &[usage(4)]),
            Err(FormatError::InvalidWriterInput { .. })
        ));
    }

    #[test]
    fn handles_many_wallets_binary_search() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = IndexBuilder::new(1, 1000, 1000);
        for wallet in 1u32..=255 {
            builder.record(wallet, usage(200));
            if wallet % 3 == 0 {
                builder.record(wallet, usage(201));
            }
        }
        let shard_dir = dir.path().join("shard-0");
        builder.write(&shard_dir).unwrap();

        let reader = IndexReader::open(&shard_dir).unwrap();
        for wallet in 1u32..=255 {
            let programs = program_ids(&reader.query(wallet).unwrap());
            let expected = if wallet % 3 == 0 {
                vec![200, 201]
            } else {
                vec![200]
            };
            assert_eq!(programs, expected, "wallet {wallet}");
        }
    }

    #[test]
    fn wallet_with_many_programs_still_merges_by_program_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = IndexBuilder::new(1, 1000, 1000);
        const PROGRAMS: u32 = 24;
        for program in 1u32..=PROGRAMS {
            builder.record(7, usage(program));
            builder.record(7, usage(program));
        }
        let shard_dir = dir.path().join("shard-0");
        builder.write(&shard_dir).unwrap();

        let reader = IndexReader::open(&shard_dir).unwrap();
        let usages = reader.query(7).unwrap();
        let programs = program_ids(&usages);
        let expected: Vec<u32> = (1..=PROGRAMS).collect();
        assert_eq!(programs, expected);
        assert!(
            usages
                .iter()
                .all(|usage| usage.transaction_count == 2 && usage.direct_instruction_count == 2)
        );
    }

    #[test]
    fn record_dedupes_immediately_and_keeps_slots_sorted() {
        let mut builder = IndexBuilder::new(1, 2, 100);
        for program in [9, 3, 9, 5, 3, 1] {
            assert_eq!(builder.record(1, usage(program)), RecordOutcome::Recorded);
        }
        assert_eq!(
            program_ids(builder.wallet_programs[0].as_slice()),
            vec![1, 3, 5, 9]
        );
        assert_eq!(builder.wallet_programs[0][1].transaction_count, 2);
    }

    #[test]
    fn merge_combines_disjoint_and_overlapping_wallets_matching_a_single_pass() {
        let dir = tempfile::tempdir().unwrap();

        // Two builders simulate two threads scanning disjoint transaction
        // ranges of the same chunk: wallet 1 only appears in `a`, wallet 3
        // only in `b`, wallet 2 appears in both (as if it signed
        // transactions in both ranges) and must end up with the union of
        // programs, deduped.
        let mut a = IndexBuilder::new(1, 1000, 1000);
        a.record(1, usage(100));
        a.record(2, usage(100));
        a.record(2, usage(101));

        let mut b = IndexBuilder::new(1, 1000, 1000);
        b.record(2, usage(101)); // another aggregate for wallet 2/program 101
        b.record(2, usage(102));
        b.record(3, usage(200));

        a.merge(b).unwrap();

        assert_eq!(a.wallet_count(), 3);
        assert_eq!(a.distinct_program_count(), 4);

        let shard_dir = dir.path().join("shard-0-merged");
        a.write(&shard_dir).unwrap();
        let reader = IndexReader::open(&shard_dir).unwrap();

        assert_eq!(program_ids(&reader.query(1).unwrap()), vec![100]);
        let wallet2 = reader.query(2).unwrap();
        assert_eq!(program_ids(&wallet2), vec![100, 101, 102]);
        assert_eq!(wallet2[1].transaction_count, 2);
        assert_eq!(reader.query(3).unwrap(), vec![usage(200)]);

        // Cross-check against a single builder fed the same records
        // directly (as an unsplit scan would) — merge must be
        // indistinguishable from never having split the work at all.
        let mut single = IndexBuilder::new(1, 1000, 1000);
        single.record(1, usage(100));
        single.record(2, usage(100));
        single.record(2, usage(101));
        single.record(2, usage(101));
        single.record(2, usage(102));
        single.record(3, usage(200));
        let single_dir = dir.path().join("shard-0-single");
        single.write(&single_dir).unwrap();
        let single_reader = IndexReader::open(&single_dir).unwrap();
        for wallet in [1u32, 2, 3] {
            assert_eq!(
                reader.query(wallet).unwrap(),
                single_reader.query(wallet).unwrap(),
                "wallet {wallet}"
            );
        }
    }

    #[test]
    fn record_rejects_invalid_program_and_out_of_chunk_account_without_panicking() {
        let mut builder = IndexBuilder::new(1, 10, 10);
        assert_eq!(
            builder.record(5, usage(11)),
            RecordOutcome::InvalidProgram,
            "program id past registry_entries"
        );
        let mut zero_program = usage(1);
        zero_program.program_id = 0;
        assert_eq!(
            builder.record(5, zero_program),
            RecordOutcome::InvalidProgram,
            "program id 0 is the raw-pubkey sentinel, never valid"
        );
        assert_eq!(
            builder.record(11, usage(5)),
            RecordOutcome::OutOfChunk,
            "account id past this chunk's end"
        );
        assert_eq!(
            builder.record(10, usage(10)),
            RecordOutcome::Recorded,
            "ids at the exact bound are valid"
        );
        assert_eq!(builder.wallet_count(), 1);
        assert_eq!(builder.distinct_program_count(), 1);
    }

    #[test]
    fn chunking_partitions_accounts_and_wallet_ids_stay_absolute() {
        let dir = tempfile::tempdir().unwrap();
        // Chunk 0 covers accounts 1..=5, chunk 1 covers 6..=10. Programs are
        // global (not chunked) in both.
        let mut chunk0 = IndexBuilder::new(1, 5, 100);
        let mut chunk1 = IndexBuilder::new(6, 5, 100);
        for account in 1u32..=10 {
            let outcome0 = chunk0.record(account, usage(99));
            let outcome1 = chunk1.record(account, usage(99));
            if account <= 5 {
                assert_eq!(outcome0, RecordOutcome::Recorded);
                assert_eq!(outcome1, RecordOutcome::OutOfChunk);
            } else {
                assert_eq!(outcome0, RecordOutcome::OutOfChunk);
                assert_eq!(outcome1, RecordOutcome::Recorded);
            }
        }
        assert_eq!(chunk0.wallet_count(), 5);
        assert_eq!(chunk1.wallet_count(), 5);

        let shard0_dir = dir.path().join("shard-0");
        let shard1_dir = dir.path().join("shard-1");
        chunk0.write(&shard0_dir).unwrap();
        chunk1.write(&shard1_dir).unwrap();

        let reader0 = IndexReader::open(&shard0_dir).unwrap();
        let reader1 = IndexReader::open(&shard1_dir).unwrap();
        for account in 1u32..=5 {
            assert_eq!(
                program_ids(&reader0.query(account).unwrap()),
                vec![99],
                "account {account} in shard 0"
            );
        }
        for account in 6u32..=10 {
            assert_eq!(
                program_ids(&reader1.query(account).unwrap()),
                vec![99],
                "account {account} in shard 1"
            );
        }

        let manifest = test_manifest(10, 5);
        assert_eq!(manifest.shard_dir_name(1).unwrap(), "shard-0");
        assert_eq!(manifest.shard_dir_name(5).unwrap(), "shard-0");
        assert_eq!(manifest.shard_dir_name(6).unwrap(), "shard-1");
        assert_eq!(manifest.shard_dir_name(10).unwrap(), "shard-1");
        assert!(manifest.shard_dir_name(0).is_err());
        assert!(manifest.shard_dir_name(11).is_err());
    }

    #[test]
    fn manifest_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = test_manifest(5736, 5736);
        manifest.epoch = 822;
        manifest.wallet_count = 5358;
        manifest.program_count = 92;
        manifest.transactions_scanned = 2969;
        manifest.blocks_scanned = 1;
        manifest.built_unix_time = 123;
        manifest.tool_version = "0.1.0".into();
        manifest.write(dir.path()).unwrap();
        let read_back = IndexManifest::read(dir.path()).unwrap();
        assert_eq!(read_back.epoch, 822);
        assert_eq!(read_back.shard_count, 1);
    }

    #[test]
    fn manifest_read_rejects_fifo_without_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(MANIFEST_FILE);
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&path)
                .status()
                .unwrap()
                .success()
        );

        let (sender, receiver) = std::sync::mpsc::channel();
        let directory = dir.path().to_owned();
        let worker = std::thread::spawn(move || {
            sender.send(IndexManifest::read(&directory)).unwrap();
        });
        let result = match receiver.recv_timeout(std::time::Duration::from_secs(2)) {
            Ok(result) => result,
            Err(error) => {
                let _peer = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                let _ = receiver.recv_timeout(std::time::Duration::from_secs(2));
                worker.join().unwrap();
                panic!("manifest read blocked while opening FIFO: {error}");
            }
        };
        worker.join().unwrap();

        let error = result.unwrap_err().to_string();
        assert!(error.contains("not a regular file"), "{error}");
    }

    #[test]
    fn manifest_write_enforces_the_same_size_cap_as_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = test_manifest(1, 1);
        manifest.archive_root = "x".repeat(MAX_INDEX_MANIFEST_BYTES as usize);

        assert!(matches!(
            manifest.write(dir.path()),
            Err(FormatError::InvalidManifest { .. })
        ));
        assert!(!dir.path().join(MANIFEST_FILE).exists());
    }

    #[test]
    fn reader_rejects_trailing_relation_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard-0");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.write(&shard).unwrap();
        let path = shard.join(RELATIONS_FILE);
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&99u32.to_le_bytes()).unwrap();
        assert!(matches!(
            IndexReader::open(&shard),
            Err(FormatError::InvalidLength {
                file: "programs.rel",
                ..
            })
        ));
    }

    #[test]
    fn query_rejects_an_invalid_program_usage_record() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard-0");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.write(&shard).unwrap();

        let relations = shard.join(RELATIONS_FILE);
        let file = fs::OpenOptions::new().write(true).open(relations).unwrap();
        file.write_all_at(&0u32.to_le_bytes(), (RELATIONS_HEADER_LEN + 12) as u64)
            .unwrap();

        let reader = IndexReader::open(&shard).unwrap();
        assert!(matches!(
            reader.query(1),
            Err(FormatError::InvalidProgramUsage {
                source: ProgramUsageError::EmptyTransactionCount,
                ..
            })
        ));
    }

    #[test]
    fn query_validates_ranges_against_declared_relation_count() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard-0");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.write(&shard).unwrap();

        let wallets = shard.join(WALLETS_FILE);
        let mut file = fs::OpenOptions::new().write(true).open(wallets).unwrap();
        file.seek(SeekFrom::Start((WALLETS_HEADER_LEN + 4) as u64))
            .unwrap();
        file.write_all(&1u64.to_le_bytes()).unwrap();

        let reader = IndexReader::open(&shard).unwrap();
        assert!(matches!(
            reader.query(1),
            Err(FormatError::RelationRangeOutOfBounds { len: 1, .. })
        ));
        assert!(bind_shard(0, &shard, 1, 10).is_err());
    }

    #[test]
    fn shard_binding_rejects_wallets_outside_the_declared_shard() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard-0");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.write(&shard).unwrap();

        let wallets = shard.join(WALLETS_FILE);
        let file = fs::OpenOptions::new().write(true).open(wallets).unwrap();
        file.write_all_at(&2u32.to_le_bytes(), WALLETS_HEADER_LEN as u64)
            .unwrap();

        assert!(matches!(
            bind_shard(0, &shard, 1, 10),
            Err(FormatError::WalletOutsideShard {
                wallet_id: 2,
                first_wallet_id: 1,
                last_wallet_id: 1,
                ..
            })
        ));
    }

    #[test]
    fn shard_binding_rejects_noncontiguous_relation_slices() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard-0");
        let mut builder = IndexBuilder::new(1, 2, 10);
        builder.record(1, usage(3));
        builder.record(2, usage(4));
        builder.write(&shard).unwrap();

        let wallets = shard.join(WALLETS_FILE);
        let file = fs::OpenOptions::new().write(true).open(wallets).unwrap();
        let second_offset = (WALLETS_HEADER_LEN + WALLET_RECORD_LEN + 4) as u64;
        file.write_all_at(&0u64.to_le_bytes(), second_offset)
            .unwrap();

        assert!(matches!(
            bind_shard(0, &shard, 2, 10),
            Err(FormatError::NonCanonicalRelationLayout { index: 1, .. })
        ));
    }

    #[test]
    fn shard_binding_rejects_out_of_registry_and_duplicate_program_ids() {
        let dir = tempfile::tempdir().unwrap();

        let out_of_registry = dir.path().join("out-of-registry");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.write(&out_of_registry).unwrap();
        let relations = out_of_registry.join(RELATIONS_FILE);
        let file = fs::OpenOptions::new().write(true).open(relations).unwrap();
        file.write_all_at(&11u32.to_le_bytes(), RELATIONS_HEADER_LEN as u64)
            .unwrap();
        assert!(matches!(
            bind_shard(0, &out_of_registry, 1, 10),
            Err(FormatError::InvalidProgramList { index: 0 })
        ));

        let duplicate = dir.path().join("duplicate");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.record(1, usage(3));
        builder.write(&duplicate).unwrap();
        let relations = duplicate.join(RELATIONS_FILE);
        let file = fs::OpenOptions::new().write(true).open(relations).unwrap();
        file.write_all_at(
            &2u32.to_le_bytes(),
            (RELATIONS_HEADER_LEN + PROGRAM_USAGE_RECORD_LEN) as u64,
        )
        .unwrap();
        assert!(matches!(
            bind_shard(0, &duplicate, 1, 10),
            Err(FormatError::InvalidProgramList { index: 0 })
        ));
    }

    #[test]
    fn reader_keeps_the_open_generation_when_a_path_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard-0");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.write(&shard).unwrap();
        let reader = IndexReader::open(&shard).unwrap();

        let relations = shard.join(RELATIONS_FILE);
        let old_relations = shard.join("programs.rel.old");
        fs::rename(&relations, &old_relations).unwrap();
        fs::write(&relations, b"replacement").unwrap();

        assert_eq!(reader.query(1).unwrap(), vec![usage(2)]);
        assert!(matches!(
            reader.verify_unchanged(),
            Err(FormatError::ContentBindingMismatch {
                file: "programs.rel",
                ..
            })
        ));
    }

    #[test]
    fn retained_file_reader_reports_in_place_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard-0");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.write(&shard).unwrap();
        let reader = IndexReader::open(&shard).unwrap();

        fs::OpenOptions::new()
            .write(true)
            .open(shard.join(RELATIONS_FILE))
            .unwrap()
            .set_len(RELATIONS_HEADER_LEN as u64)
            .unwrap();

        assert!(matches!(reader.query(1), Err(FormatError::Io { .. })));
        assert!(matches!(
            reader.verify_unchanged(),
            Err(FormatError::ContentBindingMismatch {
                file: "programs.rel",
                ..
            })
        ));
    }

    #[test]
    fn verified_reader_rejects_structurally_valid_relation_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let shard = dir.path().join("shard-0");
        let mut builder = IndexBuilder::new(1, 1, 10);
        builder.record(1, usage(2));
        builder.write(&shard).unwrap();
        let binding = bind_shard(0, &shard, 1, 10).unwrap();

        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(shard.join(RELATIONS_FILE))
            .unwrap();
        file.seek(SeekFrom::Start(RELATIONS_HEADER_LEN as u64))
            .unwrap();
        file.write_all(&3u32.to_le_bytes()).unwrap();

        assert!(matches!(
            IndexReader::open_verified(&shard, &binding),
            Err(FormatError::ContentBindingMismatch {
                file: "programs.rel",
                ..
            })
        ));
    }

    #[test]
    fn bound_program_map_round_trips_and_detects_mutation() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![(2, [2u8; 32]), (9, [9u8; 32])];
        let binding = write_program_map(dir.path(), &entries).unwrap();
        let reader = ProgramMapReader::open_verified(dir.path(), &binding, 2).unwrap();
        assert_eq!(reader.resolve(2).unwrap(), [2u8; 32]);
        assert_eq!(reader.resolve(9).unwrap(), [9u8; 32]);
        assert!(matches!(
            reader.resolve(3),
            Err(FormatError::MissingProgram { program_id: 3 })
        ));
        let path = dir.path().join(PROGRAM_MAP_FILE);
        let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start((PROGRAM_MAP_HEADER_LEN + 4) as u64))
            .unwrap();
        file.write_all(&[7u8; 32]).unwrap();
        assert!(matches!(
            reader.verify_unchanged(),
            Err(FormatError::ContentBindingMismatch {
                file: "programs.map",
                ..
            })
        ));
        drop(reader);
        assert!(matches!(
            ProgramMapReader::open_verified(dir.path(), &binding, 2),
            Err(FormatError::ContentBindingMismatch {
                file: "programs.map",
                ..
            })
        ));
    }

    #[test]
    fn manifest_rejects_unknown_semantics_version() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = test_manifest(10, 10);
        manifest.semantics.version += 1;
        assert!(matches!(
            manifest.write(dir.path()),
            Err(FormatError::InvalidManifest { .. })
        ));
    }
}
