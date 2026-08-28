//! Source-preserving standalone ledger canary for the fast Archive V2 pass.
//!
//! The writer copies exact transaction-message and metadata field ranges out
//! of one decoded source block. It never assigns new public-key or blockhash
//! IDs. Output validation is a separate reader operation and is never called
//! by the conversion path.

use std::{
    array,
    fs::File,
    io::Write,
    mem,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_format::{
    ARCHIVE_V2_TX_FLAG_HAS_METADATA, ARCHIVE_V2_TX_FLAG_MESSAGE_V0,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ArchiveV2HotBlockIndexRow, ArchiveV2HotMessagePayload,
};
use blockzilla_read_sdk::{
    CompactV2MessageSchema, CompactV2MetadataSchema, PinnedLocalRangeSource, RangeSource,
    decode_compact_v2_message,
};
use serde::{Deserialize, Serialize};
use wincode::SchemaRead;

use blockzilla_user_program_index::decode;

#[path = "standalone_directory_v3.rs"]
#[allow(clippy::duplicate_mod)] // The storage-measure binary also exposes this codec at its root.
mod directory_v3;

use directory_v3::{
    CheckpointCodec, DecodedDirectory, ObjectSlice, RewardSlice, TransactionLayout,
    TransactionReward,
};

pub const INDEX_FILE: &str = "archive-v2-standalone-blocks.index";
pub const CANDIDATE_BINDING_FILE: &str = "archive-v2-retained-sidecars.candidate.json";

pub const FILE_HEADER_LEN: usize = 64;
pub const INDEX_ROW_LEN: usize = 248;
pub const DIRECTORY_ROW_LEN: usize = 40;
pub const LOCATOR_LEN: usize = 16;
pub const FORMAT_VERSION: u16 = 2;
pub const DATA_MAGIC: [u8; 8] = *b"BZV2LN02";
pub const INDEX_MAGIC: [u8; 8] = *b"BZV2LI02";
pub const FORMAT_VERSION_V3: u16 = 3;
pub const DATA_MAGIC_V3: [u8; 8] = *b"BZV2LN03";
pub const INDEX_MAGIC_V3: [u8; 8] = *b"BZV2LI03";

const ZSTD_CODEC_BIT: u32 = 1 << 31;
const LENGTH_MASK: u32 = ZSTD_CODEC_BIT - 1;
const BLOCK_TIME_PRESENT: u32 = 1;
const BLOCK_HEIGHT_PRESENT: u32 = 1 << 1;
const CORE_FLAGS_MASK: u32 = BLOCK_TIME_PRESENT | BLOCK_HEIGHT_PRESENT;
const SOURCE_TX_FLAG_MASK: u32 = (1 << 11) - 1;
const MAX_SCRATCH_BYTES: usize = 768 << 20;
const MAX_RETAINED_SCRATCH_BYTES: usize = 128 << 20;
const MAX_PACKED_BYTES: usize = 768 << 20;
/// Maximum heap bytes retained by the validated block-row table.
///
/// This cap is independent of epoch geometry and serialized file length. It
/// prevents a valid-size but impractical index from driving a large retained
/// allocation before any row body is read.
pub const MAX_RETAINED_INDEX_ROW_BYTES: usize = 512 << 20;
/// Maximum zstd window admitted by the standalone reader (512 MiB).
const ZSTD_WINDOW_LOG_MAX: u32 = 29;
const MAX_TOP_LEVEL_INSTRUCTIONS: usize = 65_536;
/// Maximum aggregate decoded storage retained by one semantic block visit.
///
/// The point reader keeps the existing per-object cap.  The batch reader needs
/// an additional aggregate cap because it retains several selected planes at
/// the same time so that each plane is read and decompressed only once.
pub const MAX_SEMANTIC_BLOCK_DECODED_BYTES: usize = 1 << 30;
pub const MAX_SEMANTIC_BLOCK_RETAINED_UPPER_BOUND: usize = 2 << 30;
const SIGNATURE_BYTES: u64 = 64;
/// Keep one remote index response well below the gateway's closed-range cap.
const MAX_REMOTE_INDEX_RANGE_BYTES: usize = 32 << 20;
/// Maximum body size of one remote semantic-plane range request.
pub const MAX_REMOTE_SEMANTIC_RANGE_BYTES: usize = 32 << 20;
/// Normal aggregate stored-byte budget for one contiguous semantic batch.
///
/// A single validated block that exceeds this budget is its own batch. Its
/// requests are still split at `MAX_REMOTE_SEMANTIC_RANGE_BYTES`, and its
/// existing stored-plus-decoded 2 GiB guard still applies.
pub const MAX_REMOTE_SEMANTIC_BATCH_STORED_BYTES: usize = 32 << 20;
/// Maximum controlled stored allocation while one isolated large block loads:
/// the 768 MiB validated packed block plus one 32 MiB response body.
pub const MAX_REMOTE_SEMANTIC_BATCH_LOAD_LIVE_BYTES_UPPER_BOUND: usize =
    MAX_PACKED_BYTES + MAX_REMOTE_SEMANTIC_RANGE_BYTES;
/// Maximum controlled stored-plus-decoded allocation during a batched scan.
pub const MAX_REMOTE_SEMANTIC_SCAN_LIVE_BYTES_UPPER_BOUND: usize =
    MAX_SEMANTIC_BLOCK_RETAINED_UPPER_BOUND;

const SEMANTIC_OBJECTS: [Object; 7] = [
    Object::TransactionDirectory,
    Object::Messages,
    Object::LoadedAddresses,
    Object::InnerInstructions,
    Object::TokenBalances,
    Object::Outcomes,
    Object::RawMetadataFallbacks,
];

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[repr(u8)]
pub enum StandaloneFormat {
    #[default]
    V2 = 2,
    V3 = 3,
}

impl StandaloneFormat {
    pub const fn name(self) -> &'static str {
        match self {
            Self::V2 => "v2-fixed-directory",
            Self::V3 => "v3-varint-directory",
        }
    }

    const fn data_magic(self) -> [u8; 8] {
        match self {
            Self::V2 => DATA_MAGIC,
            Self::V3 => DATA_MAGIC_V3,
        }
    }

    const fn index_magic(self) -> [u8; 8] {
        match self {
            Self::V2 => INDEX_MAGIC,
            Self::V3 => INDEX_MAGIC_V3,
        }
    }

    const fn version(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Object {
    TransactionDirectory = 0,
    Messages = 1,
    LoadedAddresses = 2,
    InnerInstructions = 3,
    Logs = 4,
    TokenBalances = 5,
    Balances = 6,
    Outcomes = 7,
    TransactionRewards = 8,
    RawMetadataFallbacks = 9,
    BlockRewards = 10,
}

impl Object {
    pub const ALL: [Self; 11] = [
        Self::TransactionDirectory,
        Self::Messages,
        Self::LoadedAddresses,
        Self::InnerInstructions,
        Self::Logs,
        Self::TokenBalances,
        Self::Balances,
        Self::Outcomes,
        Self::TransactionRewards,
        Self::RawMetadataFallbacks,
        Self::BlockRewards,
    ];

    pub const PER_TRANSACTION: [Self; 9] = [
        Self::Messages,
        Self::LoadedAddresses,
        Self::InnerInstructions,
        Self::Logs,
        Self::TokenBalances,
        Self::Balances,
        Self::Outcomes,
        Self::TransactionRewards,
        Self::RawMetadataFallbacks,
    ];

    pub const fn index(self) -> usize {
        self as usize
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::TransactionDirectory => "transaction-directory",
            Self::Messages => "messages",
            Self::LoadedAddresses => "loaded-addresses",
            Self::InnerInstructions => "inner-instructions",
            Self::Logs => "logs",
            Self::TokenBalances => "token-balances",
            Self::Balances => "balances",
            Self::Outcomes => "outcomes",
            Self::TransactionRewards => "transaction-rewards",
            Self::RawMetadataFallbacks => "raw-metadata-fallbacks",
            Self::BlockRewards => "block-rewards",
        }
    }

    pub const fn file_name(self) -> &'static str {
        match self {
            Self::TransactionDirectory => "archive-v2-standalone-transaction-directory.wincode",
            Self::Messages => "archive-v2-standalone-messages.wincode",
            Self::LoadedAddresses => "archive-v2-standalone-loaded-addresses.wincode",
            Self::InnerInstructions => "archive-v2-standalone-inner-instructions.wincode",
            Self::Logs => "archive-v2-standalone-logs.wincode",
            Self::TokenBalances => "archive-v2-standalone-token-balances.wincode",
            Self::Balances => "archive-v2-standalone-balances.wincode",
            Self::Outcomes => "archive-v2-standalone-outcomes.wincode",
            Self::TransactionRewards => "archive-v2-standalone-transaction-rewards.wincode",
            Self::RawMetadataFallbacks => "archive-v2-standalone-raw-metadata-fallbacks.wincode",
            Self::BlockRewards => "archive-v2-standalone-block-rewards.wincode",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|object| object.name() == value)
    }
}

pub const OBJECT_COUNT: usize = Object::ALL.len();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CodecPolicy {
    Raw = 0,
    Zstd = 1,
    Adaptive = 2,
}

impl CodecPolicy {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "raw" => Some(Self::Raw),
            "zstd" => Some(Self::Zstd),
            "adaptive" => Some(Self::Adaptive),
            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Zstd => "zstd",
            Self::Adaptive => "adaptive",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionPlan {
    policies: [CodecPolicy; OBJECT_COUNT],
    pub zstd_level: i32,
}

impl CompressionPlan {
    pub fn default_level_three() -> Self {
        let mut policies = [CodecPolicy::Zstd; OBJECT_COUNT];
        policies[Object::TransactionRewards.index()] = CodecPolicy::Adaptive;
        policies[Object::RawMetadataFallbacks.index()] = CodecPolicy::Adaptive;
        policies[Object::BlockRewards.index()] = CodecPolicy::Raw;
        Self {
            policies,
            zstd_level: 3,
        }
    }

    pub fn with_level(mut self, level: i32) -> Result<Self> {
        ensure!(
            matches!(level, 1 | 3 | 5 | 9),
            "unsupported zstd level {level}"
        );
        self.zstd_level = level;
        Ok(self)
    }

    pub fn apply_override(&mut self, value: &str) -> Result<()> {
        let (name, policy) = value
            .split_once('=')
            .context("file codec override must be NAME=raw|zstd|adaptive")?;
        let object =
            Object::parse(name).with_context(|| format!("unknown standalone file name {name}"))?;
        let policy = CodecPolicy::parse(policy)
            .with_context(|| format!("unknown standalone codec policy {policy}"))?;
        self.policies[object.index()] = policy;
        Ok(())
    }

    pub const fn policy(self, object: Object) -> CodecPolicy {
        self.policies[object.index()]
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Binding {
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub selected_blocks: u64,
    pub selected_transactions: u64,
    pub message_schema: CompactV2MessageSchema,
    pub metadata_schema: CompactV2MetadataSchema,
    pub prefix: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct DecodedMetadataParts<'a> {
    pub outcome_head: &'a [u8],
    pub pre_balances: &'a [u8],
    pub post_balances: &'a [u8],
    pub inner_instructions: &'a [u8],
    pub logs: &'a [u8],
    pub pre_token_balances: &'a [u8],
    pub post_token_balances: &'a [u8],
    pub transaction_rewards: &'a [u8],
    pub loaded_writable: &'a [u8],
    pub loaded_readonly: &'a [u8],
    pub outcome_tail: &'a [u8],
    pub effect_state: u8,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct ObjectStats {
    pub decoded_payload_bytes: u64,
    pub stored_payload_bytes: u64,
    pub zstd_chunks: u64,
    pub raw_chunks: u64,
}

impl ObjectStats {
    fn merge(&mut self, other: Self) {
        self.decoded_payload_bytes += other.decoded_payload_bytes;
        self.stored_payload_bytes += other.stored_payload_bytes;
        self.zstd_chunks += other.zstd_chunks;
        self.raw_chunks += other.raw_chunks;
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct DirectoryV3Stats {
    pub blocks: u64,
    pub source_projection_passes: u64,
    pub canonical_reward_fields_elided: u64,
    pub canonical_reward_bytes_elided: u64,
    pub stored_reward_records: u64,
    pub raw_fallback_records: u64,
    pub stride_32_blocks: u64,
    pub stride_64_blocks: u64,
    pub stride_128_blocks: u64,
    pub varint_delta_checkpoint_blocks: u64,
    pub selected_directory_bytes: u64,
    pub fixed56_baseline_bytes: u64,
    pub worst_transaction_scan: u16,
    pub encode_worker_ns: u64,
    pub checkpoint_varint_worker_ns: u64,
}

impl DirectoryV3Stats {
    fn merge(&mut self, other: Self) {
        self.blocks += other.blocks;
        self.source_projection_passes += other.source_projection_passes;
        self.canonical_reward_fields_elided += other.canonical_reward_fields_elided;
        self.canonical_reward_bytes_elided += other.canonical_reward_bytes_elided;
        self.stored_reward_records += other.stored_reward_records;
        self.raw_fallback_records += other.raw_fallback_records;
        self.stride_32_blocks += other.stride_32_blocks;
        self.stride_64_blocks += other.stride_64_blocks;
        self.stride_128_blocks += other.stride_128_blocks;
        self.varint_delta_checkpoint_blocks += other.varint_delta_checkpoint_blocks;
        self.selected_directory_bytes += other.selected_directory_bytes;
        self.fixed56_baseline_bytes += other.fixed56_baseline_bytes;
        self.worst_transaction_scan = self
            .worst_transaction_scan
            .max(other.worst_transaction_scan);
        self.encode_worker_ns = self.encode_worker_ns.saturating_add(other.encode_worker_ns);
        self.checkpoint_varint_worker_ns = self
            .checkpoint_varint_worker_ns
            .saturating_add(other.checkpoint_varint_worker_ns);
    }
}

#[derive(Debug, Default)]
pub struct WorkerScratch {
    raw: [Vec<u8>; OBJECT_COUNT],
    compression: Vec<u8>,
    ends: [u32; 9],
    prior_ends: [u32; 9],
    transaction_count: u32,
    pending: Option<(u16, u8)>,
    format: StandaloneFormat,
    v3_transactions: Vec<TransactionLayout>,
    v3_stats: DirectoryV3Stats,
    max_live_bytes: usize,
    max_capacity: usize,
    max_retained_capacity: usize,
}

impl WorkerScratch {
    pub fn begin_block(&mut self) {
        self.begin_block_with_format(StandaloneFormat::V2);
    }

    pub fn begin_block_v3(&mut self) {
        self.begin_block_with_format(StandaloneFormat::V3);
    }

    fn begin_block_with_format(&mut self, format: StandaloneFormat) {
        for raw in &mut self.raw {
            raw.clear();
        }
        self.ends.fill(0);
        self.prior_ends.fill(0);
        self.transaction_count = 0;
        self.pending = None;
        self.format = format;
        self.v3_transactions.clear();
        self.v3_stats = DirectoryV3Stats::default();
        self.max_live_bytes = 0;
        self.max_capacity = 0;
    }

    fn raw_len(&self) -> Result<usize> {
        let raw = self.raw.iter().try_fold(0usize, |total, bytes| {
            total
                .checked_add(bytes.len())
                .context("standalone raw length overflow")
        })?;
        raw.checked_add(
            self.v3_transactions
                .len()
                .checked_mul(std::mem::size_of::<TransactionLayout>())
                .context("v3 transaction live length overflow")?,
        )
        .context("standalone raw length overflow")
    }

    fn capacity(&self) -> Result<usize> {
        let raw = self
            .raw
            .iter()
            .try_fold(self.compression.capacity(), |total, bytes| {
                total
                    .checked_add(bytes.capacity())
                    .context("standalone scratch capacity overflow")
            })?;
        raw.checked_add(
            self.v3_transactions
                .capacity()
                .checked_mul(std::mem::size_of::<TransactionLayout>())
                .context("v3 transaction capacity overflow")?,
        )
        .context("standalone scratch capacity overflow")
    }

    fn note_bounds(&mut self) -> Result<()> {
        let live = self
            .raw_len()?
            .checked_add(self.compression.len())
            .context("standalone live scratch overflow")?;
        ensure!(
            live <= MAX_SCRATCH_BYTES,
            "standalone live scratch exceeds cap"
        );
        let capacity = self.capacity()?;
        ensure!(
            capacity <= MAX_SCRATCH_BYTES,
            "standalone scratch capacity exceeds cap"
        );
        self.max_live_bytes = self.max_live_bytes.max(live);
        self.max_capacity = self.max_capacity.max(capacity);
        Ok(())
    }

    fn append(&mut self, object: Object, parts: &[&[u8]]) -> Result<()> {
        let additional = parts.iter().try_fold(0usize, |total, part| {
            total
                .checked_add(part.len())
                .context("standalone append overflow")
        })?;
        let next = self
            .raw_len()?
            .checked_add(additional)
            .context("standalone aggregate append overflow")?;
        ensure!(
            next <= MAX_SCRATCH_BYTES,
            "standalone block exceeds scratch cap"
        );
        let index = object.index();
        let required = self.raw[index]
            .len()
            .checked_add(additional)
            .context("standalone object length overflow")?;
        if required > self.raw[index].capacity() {
            let old = self.raw[index].capacity();
            let others = self
                .capacity()?
                .checked_sub(old)
                .context("capacity underflow")?;
            let available = MAX_SCRATCH_BYTES
                .checked_sub(others)
                .context("standalone capacities exceed cap")?;
            ensure!(
                required <= available,
                "standalone object cannot fit scratch cap"
            );
            let desired = required.max(old.saturating_mul(2)).max(4096).min(available);
            self.raw[index]
                .try_reserve_exact(desired - self.raw[index].len())
                .with_context(|| format!("reserve standalone {}", object.name()))?;
        }
        for part in parts {
            self.raw[index].extend_from_slice(part);
        }
        self.note_bounds()
    }

    fn push_v3_layout(&mut self, layout: TransactionLayout) -> Result<()> {
        ensure!(
            self.format == StandaloneFormat::V3,
            "v3 layout added to a v2 block"
        );
        if self.v3_transactions.len() == self.v3_transactions.capacity() {
            let item_bytes = std::mem::size_of::<TransactionLayout>();
            let retained_bytes = self
                .v3_transactions
                .capacity()
                .checked_mul(item_bytes)
                .context("v3 retained layout bytes overflow")?;
            let other_capacity = self
                .capacity()?
                .checked_sub(retained_bytes)
                .context("v3 retained layout capacity underflow")?;
            let available_bytes = MAX_SCRATCH_BYTES
                .checked_sub(other_capacity)
                .context("standalone capacities exceed cap")?;
            let available_items = available_bytes / item_bytes;
            let required = self
                .v3_transactions
                .len()
                .checked_add(1)
                .context("v3 transaction count overflow")?;
            ensure!(
                required <= available_items,
                "v3 transaction layouts exceed scratch cap"
            );
            let desired = required
                .max(self.v3_transactions.capacity().saturating_mul(2))
                .max(128)
                .min(available_items);
            self.v3_transactions
                .try_reserve_exact(desired - self.v3_transactions.len())
                .context("reserve v3 transaction layouts")?;
        }
        self.v3_transactions.push(layout);
        self.note_bounds()
    }

    pub fn begin_transaction(
        &mut self,
        source_flags: u32,
        signature_count: u8,
        message: &[u8],
    ) -> Result<()> {
        ensure!(
            self.pending.is_none(),
            "previous standalone transaction is unfinished"
        );
        ensure!(
            source_flags & !SOURCE_TX_FLAG_MASK == 0,
            "unknown source transaction flags"
        );
        ensure!(
            !message.is_empty(),
            "standalone transaction message is empty"
        );
        self.append(Object::Messages, &[message])?;
        self.ends[0] = u32::try_from(self.raw[Object::Messages.index()].len())
            .context("standalone message arena exceeds u32")?;
        self.pending = Some((u16::try_from(source_flags)?, signature_count));
        Ok(())
    }

    pub fn record_missing_metadata(&mut self) -> Result<()> {
        self.finish_transaction(0, TransactionReward::Absent)
    }

    pub fn record_raw_metadata(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(!bytes.is_empty(), "raw metadata fallback is empty");
        self.append(Object::RawMetadataFallbacks, &[bytes])?;
        self.ends[8] = u32::try_from(self.raw[Object::RawMetadataFallbacks.index()].len())
            .context("raw metadata arena exceeds u32")?;
        self.finish_transaction(0, TransactionReward::Absent)
    }

    pub fn record_decoded_metadata(&mut self, parts: DecodedMetadataParts<'_>) -> Result<()> {
        self.append(
            Object::LoadedAddresses,
            &[parts.loaded_writable, parts.loaded_readonly],
        )?;
        self.ends[1] = u32::try_from(self.raw[Object::LoadedAddresses.index()].len())
            .context("loaded-address arena exceeds u32")?;
        self.append(Object::InnerInstructions, &[parts.inner_instructions])?;
        self.ends[2] = u32::try_from(self.raw[Object::InnerInstructions.index()].len())
            .context("inner-instruction arena exceeds u32")?;
        self.append(Object::Logs, &[parts.logs])?;
        self.ends[3] =
            u32::try_from(self.raw[Object::Logs.index()].len()).context("log arena exceeds u32")?;
        self.append(
            Object::TokenBalances,
            &[parts.pre_token_balances, parts.post_token_balances],
        )?;
        self.ends[4] = u32::try_from(self.raw[Object::TokenBalances.index()].len())
            .context("token-balance arena exceeds u32")?;
        self.append(Object::Balances, &[parts.pre_balances, parts.post_balances])?;
        self.ends[5] = u32::try_from(self.raw[Object::Balances.index()].len())
            .context("balance arena exceeds u32")?;
        self.append(Object::Outcomes, &[parts.outcome_head, parts.outcome_tail])?;
        self.ends[6] = u32::try_from(self.raw[Object::Outcomes.index()].len())
            .context("outcome arena exceeds u32")?;
        ensure!(
            !parts.transaction_rewards.is_empty(),
            "decoded transaction-reward Vec field is empty"
        );
        let semantic_rewards =
            parts.effect_state & directory_v3::EFFECT_STATE_SEMANTIC_REWARDS != 0;
        let reward = if self.format == StandaloneFormat::V3
            && !semantic_rewards
            && parts.transaction_rewards == [0]
        {
            self.v3_stats.canonical_reward_fields_elided += 1;
            self.v3_stats.canonical_reward_bytes_elided += 1;
            TransactionReward::CanonicalEmpty
        } else {
            self.append(Object::TransactionRewards, &[parts.transaction_rewards])?;
            self.v3_stats.stored_reward_records += u64::from(self.format == StandaloneFormat::V3);
            if semantic_rewards {
                TransactionReward::SemanticStored(parts.transaction_rewards.len() as u64)
            } else {
                TransactionReward::NoncanonicalEmptyStored(parts.transaction_rewards.len() as u64)
            }
        };
        self.ends[7] = u32::try_from(self.raw[Object::TransactionRewards.index()].len())
            .context("transaction-reward arena exceeds u32")?;
        self.finish_transaction(parts.effect_state, reward)
    }

    fn finish_transaction(&mut self, effect_state: u8, reward: TransactionReward) -> Result<()> {
        let (source_flags, signature_count) = self
            .pending
            .take()
            .context("standalone transaction message was not recorded")?;
        if self.format == StandaloneFormat::V2 {
            let mut row = [0_u8; DIRECTORY_ROW_LEN];
            row[0..2].copy_from_slice(&source_flags.to_le_bytes());
            row[2] = effect_state;
            row[3] = signature_count;
            for (index, end) in self.ends.into_iter().enumerate() {
                let start = 4 + index * 4;
                row[start..start + 4].copy_from_slice(&end.to_le_bytes());
            }
            self.append(Object::TransactionDirectory, &[&row])?;
        } else {
            let mut lengths = [0_u64; directory_v3::DENSE_FIELD_COUNT];
            for (index, length) in lengths.iter_mut().enumerate() {
                *length = u64::from(
                    self.ends[index]
                        .checked_sub(self.prior_ends[index])
                        .context("v3 dense object end decreases")?,
                );
            }
            let raw_length = self.ends[8]
                .checked_sub(self.prior_ends[8])
                .context("v3 raw-fallback object end decreases")?;
            let reward_length = self.ends[7]
                .checked_sub(self.prior_ends[7])
                .context("v3 reward object end decreases")?;
            ensure!(
                reward.stored_len() == Some(u64::from(reward_length))
                    || (reward.stored_len().is_none() && reward_length == 0),
                "v3 reward layout differs from stored bytes"
            );
            let raw_metadata_fallback_len = (raw_length != 0).then_some(u64::from(raw_length));
            self.push_v3_layout(TransactionLayout {
                source_flags,
                effect_state,
                signature_count,
                dense_lengths: lengths,
                reward,
                raw_metadata_fallback_len,
            })?;
            self.v3_stats.raw_fallback_records += u64::from(raw_length != 0);
        }
        self.prior_ends = self.ends;
        self.transaction_count = self
            .transaction_count
            .checked_add(1)
            .context("standalone transaction count overflow")?;
        self.note_bounds()
    }

    pub fn record_block_rewards(&mut self, bytes: &[u8]) -> Result<()> {
        ensure!(!bytes.is_empty(), "block reward Option bytes are empty");
        self.append(Object::BlockRewards, &[bytes])
    }

    pub fn finish_block(&mut self, expected_transactions: u32) -> Result<()> {
        ensure!(
            self.pending.is_none(),
            "standalone block has an unfinished transaction"
        );
        ensure!(
            self.transaction_count == expected_transactions,
            "standalone transaction count differs from source"
        );
        if self.format == StandaloneFormat::V2 {
            let expected = usize::try_from(expected_transactions)?
                .checked_mul(DIRECTORY_ROW_LEN)
                .context("standalone directory length overflow")?;
            ensure!(
                self.raw[Object::TransactionDirectory.index()].len() == expected,
                "standalone directory geometry differs from transaction count"
            );
            ensure!(
                self.v3_transactions.is_empty(),
                "v2 block retained v3 transactions"
            );
        } else {
            ensure!(
                self.raw[Object::TransactionDirectory.index()].is_empty(),
                "v3 directory was materialized before block finish"
            );
            ensure!(
                self.v3_transactions.len() == expected_transactions as usize,
                "v3 transaction layout count differs from source"
            );
            self.v3_stats.blocks = 1;
            self.v3_stats.source_projection_passes = 1;
        }
        self.note_bounds()
    }

    fn trim_retained(&mut self) -> Result<usize> {
        for raw in &mut self.raw {
            raw.clear();
        }
        self.compression.clear();
        self.v3_transactions.clear();
        loop {
            let capacity = self.capacity()?;
            if capacity <= MAX_RETAINED_SCRATCH_BYTES {
                self.max_retained_capacity = self.max_retained_capacity.max(capacity);
                return Ok(capacity);
            }
            let mut largest_object = None;
            let mut largest = self.compression.capacity();
            for (index, raw) in self.raw.iter().enumerate() {
                if raw.capacity() > largest {
                    largest_object = Some(index);
                    largest = raw.capacity();
                }
            }
            let v3_capacity = self
                .v3_transactions
                .capacity()
                .checked_mul(std::mem::size_of::<TransactionLayout>())
                .context("v3 retained capacity overflow")?;
            if v3_capacity > largest {
                largest_object = Some(OBJECT_COUNT);
                largest = v3_capacity;
            }
            ensure!(largest != 0, "standalone scratch trim made no progress");
            if largest_object == Some(OBJECT_COUNT) {
                self.v3_transactions = Vec::new();
            } else if let Some(index) = largest_object {
                self.raw[index] = Vec::new();
            } else {
                self.compression = Vec::new();
            }
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct Stats {
    pub decoded_bytes: u64,
    pub stored_bytes: u64,
    pub zstd_chunks: u64,
    pub raw_chunks: u64,
    pub compression_time_ms: u64,
    pub max_live_scratch_bytes: usize,
    pub max_scratch_capacity: usize,
    pub max_retained_scratch_capacity: usize,
    pub max_packed_bytes: usize,
    pub objects: [ObjectStats; OBJECT_COUNT],
    pub directory_v3: DirectoryV3Stats,
}

impl Stats {
    fn merge(&mut self, other: Self) {
        self.decoded_bytes += other.decoded_bytes;
        self.stored_bytes += other.stored_bytes;
        self.zstd_chunks += other.zstd_chunks;
        self.raw_chunks += other.raw_chunks;
        self.compression_time_ms += other.compression_time_ms;
        self.max_live_scratch_bytes = self
            .max_live_scratch_bytes
            .max(other.max_live_scratch_bytes);
        self.max_scratch_capacity = self.max_scratch_capacity.max(other.max_scratch_capacity);
        self.max_retained_scratch_capacity = self
            .max_retained_scratch_capacity
            .max(other.max_retained_scratch_capacity);
        self.max_packed_bytes = self.max_packed_bytes.max(other.max_packed_bytes);
        for (target, source) in self.objects.iter_mut().zip(other.objects) {
            target.merge(source);
        }
        self.directory_v3.merge(other.directory_v3);
    }
}

#[derive(Debug)]
pub struct ProjectedBlock {
    packed: Vec<u8>,
    ranges: [Range<usize>; OBJECT_COUNT],
    decoded_lengths: [u32; OBJECT_COUNT],
    compressed: [bool; OBJECT_COUNT],
    format: StandaloneFormat,
    v3_directory_timing: Option<V3DirectoryTiming>,
    pub stats: Stats,
}

#[derive(Debug, Clone, Copy)]
pub struct V3DirectoryTiming {
    pub started: std::time::Instant,
    pub elapsed: Duration,
    pub checkpoint_varint_elapsed: Duration,
}

impl ProjectedBlock {
    pub fn v3_directory_timing(&self) -> Option<V3DirectoryTiming> {
        self.v3_directory_timing
    }
}

pub fn encode_block(
    scratch: &mut WorkerScratch,
    compressor: &mut zstd::bulk::Compressor<'static>,
    plan: CompressionPlan,
) -> Result<ProjectedBlock> {
    ensure!(
        scratch.format == StandaloneFormat::V2,
        "v3 block requires encode_block_v3"
    );
    encode_block_inner(scratch, compressor, plan)
}

pub fn encode_block_v3(
    scratch: &mut WorkerScratch,
    compressor: &mut zstd::bulk::Compressor<'static>,
    plan: CompressionPlan,
) -> Result<ProjectedBlock> {
    ensure!(
        scratch.format == StandaloneFormat::V3,
        "v2 block cannot use encode_block_v3"
    );
    ensure!(
        scratch.raw[Object::TransactionDirectory.index()].is_empty(),
        "v3 directory was already materialized"
    );
    ensure!(
        scratch.v3_stats.blocks == 1 && scratch.v3_stats.source_projection_passes == 1,
        "v3 block was not finished before encoding"
    );
    ensure!(
        scratch.v3_transactions.len() == scratch.transaction_count as usize,
        "v3 layout count differs before encoding"
    );
    let directory_started = std::time::Instant::now();
    let checkpoint_varint_started = std::time::Instant::now();
    let best = directory_v3::encode_best_varint_delta(&scratch.v3_transactions)
        .context("encode direct standalone v3 directory")?;
    let checkpoint_varint_elapsed = checkpoint_varint_started.elapsed();
    ensure!(
        best.encoded.measurement.selected_checkpoint_codec == CheckpointCodec::VarintDelta,
        "production v3 directory selected a non-varint checkpoint codec"
    );
    let selected_stride = best.encoded.measurement.stride;
    match selected_stride {
        32 => scratch.v3_stats.stride_32_blocks = 1,
        64 => scratch.v3_stats.stride_64_blocks = 1,
        128 => scratch.v3_stats.stride_128_blocks = 1,
        _ => bail!("v3 directory selected unsupported stride"),
    }
    let selected_measurement = best
        .measurements
        .iter()
        .find(|measurement| measurement.stride == selected_stride)
        .context("selected v3 stride has no measurement")?;
    scratch.v3_stats.varint_delta_checkpoint_blocks = 1;
    scratch.v3_stats.selected_directory_bytes = best.encoded.bytes.len() as u64;
    scratch.v3_stats.fixed56_baseline_bytes = selected_measurement.fixed56.sizes.total_bytes;
    scratch.v3_stats.worst_transaction_scan = best.encoded.measurement.worst_transaction_scan;
    scratch.append(
        Object::TransactionDirectory,
        &[best.encoded.bytes.as_slice()],
    )?;
    let directory_elapsed = directory_started.elapsed();
    scratch.v3_stats.encode_worker_ns = duration_nanos_saturating(directory_elapsed);
    scratch.v3_stats.checkpoint_varint_worker_ns =
        duration_nanos_saturating(checkpoint_varint_elapsed);
    let mut block = encode_block_inner(scratch, compressor, plan)?;
    block.v3_directory_timing = Some(V3DirectoryTiming {
        started: directory_started,
        elapsed: directory_elapsed,
        checkpoint_varint_elapsed,
    });
    Ok(block)
}

fn encode_block_inner(
    scratch: &mut WorkerScratch,
    compressor: &mut zstd::bulk::Compressor<'static>,
    plan: CompressionPlan,
) -> Result<ProjectedBlock> {
    let mut packed = Vec::new();
    let mut ranges = array::from_fn(|_| 0..0);
    let mut decoded_lengths = [0_u32; OBJECT_COUNT];
    let mut compressed = [false; OBJECT_COUNT];
    let mut stats = Stats {
        max_live_scratch_bytes: scratch.max_live_bytes,
        max_scratch_capacity: scratch.max_capacity,
        ..Stats::default()
    };
    for object in Object::ALL {
        let index = object.index();
        let start = packed.len();
        let raw_len = scratch.raw[index].len();
        if raw_len == 0 {
            ranges[index] = start..start;
            continue;
        }
        ensure!(
            raw_len <= LENGTH_MASK as usize,
            "standalone decoded chunk exceeds locator"
        );
        decoded_lengths[index] = u32::try_from(raw_len)?;
        stats.decoded_bytes += u64::try_from(raw_len)?;
        stats.objects[index].decoded_payload_bytes += u64::try_from(raw_len)?;
        let policy = plan.policy(object);
        let mut elapsed = Duration::ZERO;
        if policy != CodecPolicy::Raw {
            scratch.compression.clear();
            let bound = zstd::zstd_safe::compress_bound(raw_len);
            let non_compression_capacity = scratch
                .capacity()?
                .checked_sub(scratch.compression.capacity())
                .context("compression capacity underflow")?;
            ensure!(
                non_compression_capacity
                    .checked_add(bound)
                    .is_some_and(|value| value <= MAX_SCRATCH_BYTES),
                "standalone zstd bound exceeds scratch cap"
            );
            if scratch.compression.capacity() < bound {
                let mut replacement = Vec::new();
                replacement.try_reserve_exact(bound)?;
                scratch.compression = replacement;
            }
            scratch.note_bounds()?;
            let started = std::time::Instant::now();
            compressor
                .compress_to_buffer(&scratch.raw[index], &mut scratch.compression)
                .with_context(|| format!("compress standalone {}", object.name()))?;
            elapsed = started.elapsed();
            scratch.note_bounds()?;
        }
        let use_zstd = match policy {
            CodecPolicy::Raw => false,
            CodecPolicy::Zstd => true,
            CodecPolicy::Adaptive => scratch.compression.len() < raw_len,
        };
        let stored = if use_zstd {
            scratch.compression.as_slice()
        } else {
            scratch.raw[index].as_slice()
        };
        ensure!(
            stored.len() <= LENGTH_MASK as usize,
            "standalone stored chunk exceeds locator"
        );
        ensure!(
            packed
                .len()
                .checked_add(stored.len())
                .is_some_and(|value| value <= MAX_PACKED_BYTES),
            "standalone packed block exceeds cap"
        );
        packed.extend_from_slice(stored);
        ranges[index] = start..packed.len();
        compressed[index] = use_zstd;
        stats.stored_bytes += u64::try_from(stored.len())?;
        stats.zstd_chunks += u64::from(use_zstd);
        stats.raw_chunks += u64::from(!use_zstd);
        stats.objects[index].stored_payload_bytes += u64::try_from(stored.len())?;
        stats.objects[index].zstd_chunks += u64::from(use_zstd);
        stats.objects[index].raw_chunks += u64::from(!use_zstd);
        stats.compression_time_ms += u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
    }
    stats.max_live_scratch_bytes = stats.max_live_scratch_bytes.max(scratch.max_live_bytes);
    stats.max_scratch_capacity = stats.max_scratch_capacity.max(scratch.max_capacity);
    stats.max_packed_bytes = packed.len();
    stats.directory_v3 = scratch.v3_stats;
    let format = scratch.format;
    stats.max_retained_scratch_capacity = scratch.trim_retained()?;
    Ok(ProjectedBlock {
        packed,
        ranges,
        decoded_lengths,
        compressed,
        format,
        v3_directory_timing: None,
        stats,
    })
}

fn duration_nanos_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Locator {
    pub offset: u64,
    pub stored_len: u32,
    pub decoded_len: u32,
    pub zstd: bool,
}

impl Locator {
    fn encode(self, output: &mut impl Write) -> Result<()> {
        ensure!(
            self.stored_len <= LENGTH_MASK,
            "stored length exceeds locator"
        );
        ensure!(
            !self.zstd || self.stored_len != 0,
            "empty locator cannot be zstd"
        );
        ensure!(
            self.zstd || self.stored_len == self.decoded_len,
            "raw locator lengths differ"
        );
        output.write_all(&self.offset.to_le_bytes())?;
        let stored = self.stored_len | (u32::from(self.zstd) * ZSTD_CODEC_BIT);
        output.write_all(&stored.to_le_bytes())?;
        output.write_all(&self.decoded_len.to_le_bytes())?;
        Ok(())
    }

    fn decode(input: &[u8]) -> Result<Self> {
        ensure!(
            input.len() == LOCATOR_LEN,
            "locator length is not {LOCATOR_LEN}"
        );
        let offset = u64::from_le_bytes(input[0..8].try_into().unwrap());
        let stored_and_codec = u32::from_le_bytes(input[8..12].try_into().unwrap());
        let decoded_len = u32::from_le_bytes(input[12..16].try_into().unwrap());
        let zstd = stored_and_codec & ZSTD_CODEC_BIT != 0;
        let stored_len = stored_and_codec & LENGTH_MASK;
        let locator = Self {
            offset,
            stored_len,
            decoded_len,
            zstd,
        };
        ensure!(!zstd || stored_len != 0, "empty locator is marked zstd");
        ensure!(
            zstd || stored_len == decoded_len,
            "raw locator lengths differ"
        );
        ensure!(
            (stored_len == 0) == (decoded_len == 0),
            "locator has one zero length"
        );
        offset
            .checked_add(u64::from(stored_len))
            .context("locator range overflows u64")?;
        Ok(locator)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRow {
    pub block_id: u32,
    pub tx_count: u32,
    pub slot: u64,
    pub parent_slot: u64,
    pub first_tx_ordinal: u64,
    pub first_signature_ordinal: u64,
    pub signature_count: u32,
    pub blockhash_id: u32,
    pub previous_blockhash_id: u32,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub locators: [Locator; OBJECT_COUNT],
}

impl BlockRow {
    fn encode(&self) -> Result<[u8; INDEX_ROW_LEN]> {
        ensure!(
            self.parent_slot < self.slot || self.slot == 0,
            "parent slot is not before slot"
        );
        let mut output = Vec::with_capacity(INDEX_ROW_LEN);
        output.extend_from_slice(&self.block_id.to_le_bytes());
        output.extend_from_slice(&self.tx_count.to_le_bytes());
        output.extend_from_slice(&self.slot.to_le_bytes());
        output.extend_from_slice(&self.parent_slot.to_le_bytes());
        output.extend_from_slice(&self.first_tx_ordinal.to_le_bytes());
        output.extend_from_slice(&self.first_signature_ordinal.to_le_bytes());
        output.extend_from_slice(&self.signature_count.to_le_bytes());
        output.extend_from_slice(&self.blockhash_id.to_le_bytes());
        output.extend_from_slice(&self.previous_blockhash_id.to_le_bytes());
        let flags = (u32::from(self.block_time.is_some()) * BLOCK_TIME_PRESENT)
            | (u32::from(self.block_height.is_some()) * BLOCK_HEIGHT_PRESENT);
        output.extend_from_slice(&flags.to_le_bytes());
        output.extend_from_slice(&self.block_time.unwrap_or(0).to_le_bytes());
        output.extend_from_slice(&self.block_height.unwrap_or(0).to_le_bytes());
        for locator in self.locators {
            locator.encode(&mut output)?;
        }
        ensure!(
            output.len() == INDEX_ROW_LEN,
            "encoded block row has wrong length"
        );
        Ok(output.try_into().expect("checked fixed row length"))
    }

    fn decode(input: &[u8]) -> Result<Self> {
        ensure!(input.len() == INDEX_ROW_LEN, "block row has wrong length");
        let flags = u32::from_le_bytes(input[52..56].try_into().unwrap());
        ensure!(
            flags & !CORE_FLAGS_MASK == 0,
            "block row has unknown core flags"
        );
        let raw_time = i64::from_le_bytes(input[56..64].try_into().unwrap());
        let raw_height = u64::from_le_bytes(input[64..72].try_into().unwrap());
        ensure!(
            flags & BLOCK_TIME_PRESENT != 0 || raw_time == 0,
            "absent block time has nonzero storage"
        );
        ensure!(
            flags & BLOCK_HEIGHT_PRESENT != 0 || raw_height == 0,
            "absent block height has nonzero storage"
        );
        let mut locators = [Locator::default(); OBJECT_COUNT];
        for (index, locator) in locators.iter_mut().enumerate() {
            let start = 72 + index * LOCATOR_LEN;
            *locator = Locator::decode(&input[start..start + LOCATOR_LEN])?;
        }
        let row = Self {
            block_id: u32::from_le_bytes(input[0..4].try_into().unwrap()),
            tx_count: u32::from_le_bytes(input[4..8].try_into().unwrap()),
            slot: u64::from_le_bytes(input[8..16].try_into().unwrap()),
            parent_slot: u64::from_le_bytes(input[16..24].try_into().unwrap()),
            first_tx_ordinal: u64::from_le_bytes(input[24..32].try_into().unwrap()),
            first_signature_ordinal: u64::from_le_bytes(input[32..40].try_into().unwrap()),
            signature_count: u32::from_le_bytes(input[40..44].try_into().unwrap()),
            blockhash_id: u32::from_le_bytes(input[44..48].try_into().unwrap()),
            previous_blockhash_id: u32::from_le_bytes(input[48..52].try_into().unwrap()),
            block_time: (flags & BLOCK_TIME_PRESENT != 0).then_some(raw_time),
            block_height: (flags & BLOCK_HEIGHT_PRESENT != 0).then_some(raw_height),
            locators,
        };
        ensure!(
            row.parent_slot < row.slot || row.slot == 0,
            "parent slot is not before slot"
        );
        Ok(row)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub format: StandaloneFormat,
    pub magic: [u8; 8],
    pub object: u16,
    pub policy: u8,
    pub message_schema: u8,
    pub metadata_schema: u8,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub selected_blocks: u64,
    pub selected_transactions: u64,
    pub prefix: bool,
    pub zstd_level: u8,
}

impl FileHeader {
    fn for_object(binding: Binding, plan: CompressionPlan, object: Object) -> Self {
        Self::for_object_format(binding, plan, object, StandaloneFormat::V2)
    }

    fn for_object_format(
        binding: Binding,
        plan: CompressionPlan,
        object: Object,
        format: StandaloneFormat,
    ) -> Self {
        Self {
            format,
            magic: format.data_magic(),
            object: object as u16,
            policy: plan.policy(object) as u8,
            message_schema: message_schema_code(binding.message_schema),
            metadata_schema: metadata_schema_code(binding.metadata_schema),
            epoch: binding.epoch,
            slots_per_epoch: binding.slots_per_epoch,
            selected_blocks: binding.selected_blocks,
            selected_transactions: binding.selected_transactions,
            prefix: binding.prefix,
            zstd_level: u8::try_from(plan.zstd_level).expect("supported zstd level fits u8"),
        }
    }

    fn for_index(binding: Binding, plan: CompressionPlan) -> Self {
        Self::for_index_format(binding, plan, StandaloneFormat::V2)
    }

    fn for_index_format(binding: Binding, plan: CompressionPlan, format: StandaloneFormat) -> Self {
        Self {
            format,
            magic: format.index_magic(),
            object: u16::MAX,
            policy: u8::MAX,
            message_schema: message_schema_code(binding.message_schema),
            metadata_schema: metadata_schema_code(binding.metadata_schema),
            epoch: binding.epoch,
            slots_per_epoch: binding.slots_per_epoch,
            selected_blocks: binding.selected_blocks,
            selected_transactions: binding.selected_transactions,
            prefix: binding.prefix,
            zstd_level: u8::try_from(plan.zstd_level).expect("supported zstd level fits u8"),
        }
    }

    fn encode(&self) -> [u8; FILE_HEADER_LEN] {
        let mut output = [0_u8; FILE_HEADER_LEN];
        output[0..8].copy_from_slice(&self.magic);
        output[8..10].copy_from_slice(&self.format.version().to_le_bytes());
        output[10..12].copy_from_slice(&self.object.to_le_bytes());
        output[12] = self.policy;
        output[13] = self.message_schema;
        output[14] = self.metadata_schema;
        output[15] = 1; // Current outer Archive V2 profile.
        output[16..24].copy_from_slice(&self.epoch.to_le_bytes());
        output[24..32].copy_from_slice(&self.slots_per_epoch.to_le_bytes());
        output[32..40].copy_from_slice(&self.selected_blocks.to_le_bytes());
        output[40..48].copy_from_slice(&self.selected_transactions.to_le_bytes());
        output[48] = u8::from(self.prefix);
        let directory_row_len = match self.format {
            StandaloneFormat::V2 => DIRECTORY_ROW_LEN as u16,
            StandaloneFormat::V3 => 0,
        };
        output[49..51].copy_from_slice(&directory_row_len.to_le_bytes());
        output[51] = Object::PER_TRANSACTION.len() as u8;
        output[52] = 1;
        output[53] = OBJECT_COUNT as u8;
        output[54] = self.zstd_level;
        output[55] = u8::from(self.format == StandaloneFormat::V3);
        output[56] = u8::from(self.format == StandaloneFormat::V3);
        output
    }

    fn decode(input: &[u8]) -> Result<Self> {
        ensure!(
            input.len() == FILE_HEADER_LEN,
            "file header has wrong length"
        );
        let mut magic = [0_u8; 8];
        magic.copy_from_slice(&input[0..8]);
        let version = u16::from_le_bytes(input[8..10].try_into().unwrap());
        let format = match (magic, version) {
            (DATA_MAGIC | INDEX_MAGIC, FORMAT_VERSION) => StandaloneFormat::V2,
            (DATA_MAGIC_V3 | INDEX_MAGIC_V3, FORMAT_VERSION_V3) => StandaloneFormat::V3,
            _ => bail!("unknown standalone file magic or version"),
        };
        let object = u16::from_le_bytes(input[10..12].try_into().unwrap());
        let policy = input[12];
        if magic == format.index_magic() {
            ensure!(
                object == u16::MAX && policy == u8::MAX,
                "invalid standalone index identity"
            );
        } else {
            ensure!(
                (object as usize) < OBJECT_COUNT,
                "unknown standalone object id"
            );
            ensure!(
                policy <= CodecPolicy::Adaptive as u8,
                "unknown codec policy"
            );
        }
        ensure!(input[15] == 1, "unsupported outer Archive V2 profile");
        ensure!(input[48] <= 1, "invalid prefix flag");
        let expected_directory_row_len = match format {
            StandaloneFormat::V2 => DIRECTORY_ROW_LEN as u16,
            StandaloneFormat::V3 => 0,
        };
        ensure!(
            u16::from_le_bytes(input[49..51].try_into().unwrap()) == expected_directory_row_len,
            "wrong directory row length"
        );
        ensure!(
            input[51] == 9 && input[52] == 1 && input[53] == OBJECT_COUNT as u8,
            "wrong object geometry"
        );
        ensure!(matches!(input[54], 1 | 3 | 5 | 9), "unsupported zstd level");
        let expected_v3_code = u8::from(format == StandaloneFormat::V3);
        ensure!(
            input[55] == expected_v3_code && input[56] == expected_v3_code,
            "directory or reward encoding code differs from format"
        );
        ensure!(
            input[57..64].iter().all(|byte| *byte == 0),
            "nonzero reserved header bytes"
        );
        ensure!(
            input[13] <= 1 && input[14] <= 1,
            "unknown source schema code"
        );
        let slots_per_epoch = u64::from_le_bytes(input[24..32].try_into().unwrap());
        ensure!(slots_per_epoch != 0, "slots per epoch is zero");
        Ok(Self {
            format,
            magic,
            object,
            policy,
            message_schema: input[13],
            metadata_schema: input[14],
            epoch: u64::from_le_bytes(input[16..24].try_into().unwrap()),
            slots_per_epoch,
            selected_blocks: u64::from_le_bytes(input[32..40].try_into().unwrap()),
            selected_transactions: u64::from_le_bytes(input[40..48].try_into().unwrap()),
            prefix: input[48] != 0,
            zstd_level: input[54],
        })
    }

    fn common_identity(&self) -> (StandaloneFormat, u8, u8, u64, u64, u64, u64, bool, u8) {
        (
            self.format,
            self.message_schema,
            self.metadata_schema,
            self.epoch,
            self.slots_per_epoch,
            self.selected_blocks,
            self.selected_transactions,
            self.prefix,
            self.zstd_level,
        )
    }
}

pub struct Writers {
    object_files: Vec<File>,
    index_file: File,
    object_offsets: [u64; OBJECT_COUNT],
    row_count: u64,
    transaction_count: u64,
    format: StandaloneFormat,
    plan: CompressionPlan,
    stats: Stats,
    object_write_time: [Duration; OBJECT_COUNT],
    object_write_bytes: [u64; OBJECT_COUNT],
    index_write_time: Duration,
    index_write_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct SourceBlockCore {
    pub parent_slot: u64,
    pub blockhash_id: u32,
    pub previous_blockhash_id: u32,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct OutputSummary {
    pub format: &'static str,
    pub object_file_bytes: [u64; OBJECT_COUNT],
    pub objects: [ObjectSummary; OBJECT_COUNT],
    pub index_file_bytes: u64,
    pub output_bytes: u64,
    pub zstd_level: i32,
    pub output_reopens: u64,
    pub io_phase: StandaloneIoPhase,
    pub stats: Stats,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct StandaloneIoPhase {
    pub timing_scope: &'static str,
    pub object_write_wall_ms: [u64; OBJECT_COUNT],
    pub object_write_bytes: [u64; OBJECT_COUNT],
    pub index_write_wall_ms: u64,
    pub index_write_bytes: u64,
    pub flush_wall_ms: u64,
    pub sync_wall_ms: u64,
    pub logical_write_bytes: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ObjectSummary {
    pub object: &'static str,
    pub file_name: &'static str,
    pub codec_policy: &'static str,
    pub header_bytes: u64,
    pub decoded_payload_bytes: u64,
    pub stored_payload_bytes: u64,
    pub zstd_chunks: u64,
    pub raw_chunks: u64,
    pub file_bytes: u64,
}

impl Writers {
    pub fn create(root: &Path, binding: Binding, plan: CompressionPlan) -> Result<Self> {
        Self::create_with_format(root, binding, plan, StandaloneFormat::V2)
    }

    pub fn create_v3(root: &Path, binding: Binding, plan: CompressionPlan) -> Result<Self> {
        Self::create_with_format(root, binding, plan, StandaloneFormat::V3)
    }

    fn create_with_format(
        root: &Path,
        binding: Binding,
        plan: CompressionPlan,
        format: StandaloneFormat,
    ) -> Result<Self> {
        let mut object_files = Vec::with_capacity(OBJECT_COUNT);
        let mut object_write_time = [Duration::ZERO; OBJECT_COUNT];
        let mut object_write_bytes = [0_u64; OBJECT_COUNT];
        for object in Object::ALL {
            let path = root.join(object.file_name());
            let mut file = File::create(&path)
                .with_context(|| format!("create standalone object {}", path.display()))?;
            let header = FileHeader::for_object_format(binding, plan, object, format).encode();
            let write_started = std::time::Instant::now();
            file.write_all(&header)?;
            object_write_time[object.index()] = write_started.elapsed();
            object_write_bytes[object.index()] = u64::try_from(header.len())?;
            object_files.push(file);
        }
        let index_path = root.join(INDEX_FILE);
        let mut index_file = File::create(&index_path)
            .with_context(|| format!("create standalone index {}", index_path.display()))?;
        let index_header = FileHeader::for_index_format(binding, plan, format).encode();
        let index_write_started = std::time::Instant::now();
        index_file.write_all(&index_header)?;
        let index_write_time = index_write_started.elapsed();
        Ok(Self {
            object_files,
            index_file,
            object_offsets: [FILE_HEADER_LEN as u64; OBJECT_COUNT],
            row_count: 0,
            transaction_count: 0,
            format,
            plan,
            stats: Stats::default(),
            object_write_time,
            object_write_bytes,
            index_write_time,
            index_write_bytes: u64::try_from(index_header.len())?,
        })
    }

    pub fn append(
        &mut self,
        source_row: ArchiveV2HotBlockIndexRow,
        core: SourceBlockCore,
        block: ProjectedBlock,
    ) -> Result<()> {
        ensure!(
            block.format == self.format,
            "standalone projected block format differs from writer"
        );
        ensure!(
            u64::from(source_row.block_id) == self.row_count,
            "standalone blocks are not ordered"
        );
        let mut locators = [Locator::default(); OBJECT_COUNT];
        let mut packed_position = 0usize;
        for object in Object::ALL {
            let index = object.index();
            let range = block.ranges[index].clone();
            ensure!(
                range.start == packed_position
                    && range.start <= range.end
                    && range.end <= block.packed.len(),
                "standalone packed ranges are not contiguous"
            );
            let bytes = &block.packed[range.clone()];
            let stored_len = u32::try_from(bytes.len())?;
            let decoded_len = block.decoded_lengths[index];
            let locator = Locator {
                offset: self.object_offsets[index],
                stored_len,
                decoded_len,
                zstd: block.compressed[index],
            };
            locator.encode(&mut std::io::sink())?;
            let write_started = std::time::Instant::now();
            self.object_files[index]
                .write_all(bytes)
                .with_context(|| format!("append standalone {} block", object.name()))?;
            self.object_write_time[index] =
                self.object_write_time[index].saturating_add(write_started.elapsed());
            self.object_write_bytes[index] = self.object_write_bytes[index]
                .checked_add(u64::try_from(bytes.len())?)
                .context("standalone object physical write-byte count overflow")?;
            self.object_offsets[index] = self.object_offsets[index]
                .checked_add(u64::from(stored_len))
                .context("standalone object offset overflow")?;
            locators[index] = locator;
            packed_position = range.end;
        }
        ensure!(
            packed_position == block.packed.len(),
            "standalone packed block has trailing bytes"
        );
        let row = BlockRow {
            block_id: source_row.block_id,
            tx_count: source_row.tx_count,
            slot: source_row.slot,
            parent_slot: core.parent_slot,
            first_tx_ordinal: source_row.first_tx_ordinal,
            first_signature_ordinal: source_row.first_signature_ordinal,
            signature_count: source_row.signature_count,
            blockhash_id: core.blockhash_id,
            previous_blockhash_id: core.previous_blockhash_id,
            block_time: core.block_time,
            block_height: core.block_height,
            locators,
        };
        let row_bytes = row.encode()?;
        let index_write_started = std::time::Instant::now();
        self.index_file.write_all(&row_bytes)?;
        self.index_write_time = self
            .index_write_time
            .saturating_add(index_write_started.elapsed());
        self.index_write_bytes = self
            .index_write_bytes
            .checked_add(u64::try_from(row_bytes.len())?)
            .context("standalone index physical write-byte count overflow")?;
        self.row_count += 1;
        self.transaction_count = self
            .transaction_count
            .checked_add(u64::from(source_row.tx_count))
            .context("standalone transaction count overflow")?;
        self.stats.merge(block.stats);
        Ok(())
    }

    pub fn finish(
        mut self,
        expected_rows: u64,
        expected_transactions: u64,
    ) -> Result<OutputSummary> {
        ensure!(
            self.row_count == expected_rows,
            "standalone row count differs from source"
        );
        ensure!(
            self.transaction_count == expected_transactions,
            "standalone transaction count differs from source"
        );
        let mut flush_time = Duration::ZERO;
        let mut sync_time = Duration::ZERO;
        for (index, file) in self.object_files.iter_mut().enumerate() {
            let flush_started = std::time::Instant::now();
            file.flush()
                .with_context(|| format!("flush standalone {}", Object::ALL[index].name()))?;
            flush_time = flush_time.saturating_add(flush_started.elapsed());
            let sync_started = std::time::Instant::now();
            file.sync_all()
                .with_context(|| format!("sync standalone {}", Object::ALL[index].name()))?;
            sync_time = sync_time.saturating_add(sync_started.elapsed());
        }
        let index_flush_started = std::time::Instant::now();
        self.index_file.flush().context("flush standalone index")?;
        flush_time = flush_time.saturating_add(index_flush_started.elapsed());
        let index_sync_started = std::time::Instant::now();
        self.index_file
            .sync_all()
            .context("sync standalone index")?;
        sync_time = sync_time.saturating_add(index_sync_started.elapsed());
        let index_file_bytes = (FILE_HEADER_LEN as u64)
            .checked_add(
                self.row_count
                    .checked_mul(INDEX_ROW_LEN as u64)
                    .context("index bytes overflow")?,
            )
            .context("index bytes overflow")?;
        let object_bytes = self.object_offsets.iter().try_fold(0_u64, |total, bytes| {
            total
                .checked_add(*bytes)
                .context("standalone output bytes overflow")
        })?;
        let output_bytes = object_bytes
            .checked_add(index_file_bytes)
            .context("standalone output bytes overflow")?;
        let logical_write_bytes = self
            .object_write_bytes
            .iter()
            .try_fold(self.index_write_bytes, |total, bytes| {
                total.checked_add(*bytes)
            })
            .context("standalone physical write-byte count overflow")?;
        ensure!(
            logical_write_bytes == output_bytes,
            "standalone logical write-byte accounting differs from output bytes"
        );
        for object in Object::ALL {
            let stats = self.stats.objects[object.index()];
            ensure!(
                stats
                    .stored_payload_bytes
                    .checked_add(FILE_HEADER_LEN as u64)
                    == Some(self.object_offsets[object.index()]),
                "standalone per-object stored-byte accounting differs"
            );
        }
        let objects = Object::ALL.map(|object| {
            let stats = self.stats.objects[object.index()];
            ObjectSummary {
                object: object.name(),
                file_name: object.file_name(),
                codec_policy: self.plan.policy(object).name(),
                header_bytes: FILE_HEADER_LEN as u64,
                decoded_payload_bytes: stats.decoded_payload_bytes,
                stored_payload_bytes: stats.stored_payload_bytes,
                zstd_chunks: stats.zstd_chunks,
                raw_chunks: stats.raw_chunks,
                file_bytes: self.object_offsets[object.index()],
            }
        });
        Ok(OutputSummary {
            format: self.format.name(),
            object_file_bytes: self.object_offsets,
            objects,
            index_file_bytes,
            output_bytes,
            zstd_level: self.plan.zstd_level,
            output_reopens: 0,
            io_phase: StandaloneIoPhase {
                timing_scope: "object and index write fields are serial write-call wall sums; flush and sync fields are serial finalization wall sums",
                object_write_wall_ms: self.object_write_time.map(duration_millis_saturating),
                object_write_bytes: self.object_write_bytes,
                index_write_wall_ms: duration_millis_saturating(self.index_write_time),
                index_write_bytes: self.index_write_bytes,
                flush_wall_ms: duration_millis_saturating(flush_time),
                sync_wall_ms: duration_millis_saturating(sync_time),
                logical_write_bytes,
            },
            stats: self.stats,
        })
    }
}

fn duration_millis_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn message_schema_code(schema: CompactV2MessageSchema) -> u8 {
    match schema {
        CompactV2MessageSchema::Current => 0,
        CompactV2MessageSchema::May24PreUnknownFallbacks => 1,
    }
}

fn metadata_schema_code(schema: CompactV2MetadataSchema) -> u8 {
    match schema {
        CompactV2MetadataSchema::CurrentTypedError => 0,
        CompactV2MetadataSchema::LegacyRawError => 1,
    }
}

fn message_schema_from_code(code: u8) -> Result<CompactV2MessageSchema> {
    match code {
        0 => Ok(CompactV2MessageSchema::Current),
        1 => Ok(CompactV2MessageSchema::May24PreUnknownFallbacks),
        _ => bail!("unknown message schema code {code}"),
    }
}

fn metadata_schema_from_code(code: u8) -> Result<CompactV2MetadataSchema> {
    match code {
        0 => Ok(CompactV2MetadataSchema::CurrentTypedError),
        1 => Ok(CompactV2MetadataSchema::LegacyRawError),
        _ => bail!("unknown metadata schema code {code}"),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateBinding {
    pub schema_version: u32,
    pub status: String,
    pub epoch: u64,
    pub slots_per_epoch: u64,
    pub selected_blocks: u64,
    pub selected_transactions: u64,
    pub complete_epoch: bool,
    pub outer_schema: String,
    pub message_schema: String,
    pub metadata_schema: String,
    pub source_generation_digest: Option<String>,
    pub objects: Vec<RetainedObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedObject {
    pub logical_name: String,
    pub role: String,
    pub admitted_source_size: u64,
}

impl CandidateBinding {
    pub fn encode_pretty(&self) -> Result<Vec<u8>> {
        ensure!(
            self.schema_version == 1,
            "candidate binding schema must be 1"
        );
        ensure!(
            self.status == "unverified-nonpublishable",
            "candidate binding status must remain unverified"
        );
        ensure!(
            self.slots_per_epoch != 0,
            "candidate slots per epoch is zero"
        );
        ensure!(
            self.outer_schema == "current",
            "candidate outer schema is not current"
        );
        ensure!(
            matches!(
                self.message_schema.as_str(),
                "current" | "may24-pre-unknown-fallbacks"
            ),
            "candidate message schema is unknown"
        );
        ensure!(
            matches!(
                self.metadata_schema.as_str(),
                "current-typed-error" | "legacy-raw-error"
            ),
            "candidate metadata schema is unknown"
        );
        if let Some(digest) = &self.source_generation_digest {
            ensure!(
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                "copied source generation digest is not lowercase SHA-256 text"
            );
        }
        let mut prior = None;
        for object in &self.objects {
            ensure!(
                !object.logical_name.is_empty(),
                "retained logical name is empty"
            );
            ensure!(
                object.logical_name != "."
                    && object.logical_name != ".."
                    && !object.logical_name.contains('/')
                    && !object.logical_name.contains('\\'),
                "retained logical name is not one safe component"
            );
            ensure!(!object.role.is_empty(), "retained object role is empty");
            if let Some(prior) = prior {
                ensure!(
                    prior < object.logical_name.as_str(),
                    "retained objects are not strictly sorted"
                );
            }
            prior = Some(object.logical_name.as_str());
        }
        serde_json::to_vec_pretty(self).context("encode retained-sidecar candidate binding")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectoryRow {
    pub source_flags: u16,
    pub effect_state: u8,
    pub signature_count: u8,
    pub ends: [u32; 9],
}

impl DirectoryRow {
    fn decode(input: &[u8]) -> Result<Self> {
        ensure!(
            input.len() == DIRECTORY_ROW_LEN,
            "directory row has wrong length"
        );
        let source_flags = u16::from_le_bytes(input[0..2].try_into().unwrap());
        ensure!(
            u32::from(source_flags) & !SOURCE_TX_FLAG_MASK == 0,
            "directory row has unknown source flags"
        );
        let mut ends = [0_u32; 9];
        for (index, end) in ends.iter_mut().enumerate() {
            let start = 4 + index * 4;
            *end = u32::from_le_bytes(input[start..start + 4].try_into().unwrap());
        }
        Ok(Self {
            source_flags,
            effect_state: input[2],
            signature_count: input[3],
            ends,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataBytes {
    Absent,
    Decoded(Vec<u8>),
    RawFallback(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadTransaction {
    pub block_id: u32,
    pub slot: u64,
    pub tx_index: u32,
    pub source_flags: u16,
    pub effect_state: u8,
    pub message: Vec<u8>,
    pub metadata: MetadataBytes,
    pub signature_ordinals: Range<u64>,
    pub signature_bytes: Range<u64>,
    pub directory_validation_records_scanned: u64,
    pub reward_validation_records_scanned: u64,
    pub raw_fallback_validation_records_scanned: u64,
    pub directory_lookup_records_scanned: u32,
    pub reward_lookup_records_scanned: u32,
    pub raw_fallback_lookup_records_scanned: u32,
}

/// Exact logical read work for one standalone object.
///
/// `stored_bytes` is the number of bytes requested from the candidate file.
/// `decoded_bytes` is the size retained after the optional zstd expansion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ObjectReadStats {
    pub read_calls: u64,
    pub stored_bytes: u64,
    pub decoded_bytes: u64,
}

impl ObjectReadStats {
    fn record(&mut self, locator: Locator) -> Result<()> {
        if locator.stored_len == 0 {
            return Ok(());
        }
        self.read_calls = self
            .read_calls
            .checked_add(1)
            .context("standalone read-call count overflow")?;
        self.stored_bytes = self
            .stored_bytes
            .checked_add(u64::from(locator.stored_len))
            .context("standalone stored-byte count overflow")?;
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(u64::from(locator.decoded_len))
            .context("standalone decoded-byte count overflow")?;
        Ok(())
    }
}

/// Exact I/O receipt for opening one validated standalone reader.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct OpenReadStats {
    pub read_calls: u64,
    pub stored_bytes: u64,
}

/// A borrowed transaction view from one block-batch semantic read.
///
/// Decoded metadata fields retain their exact Compact V2 field encoding.  A
/// decoded transaction has `Some` values for all four semantic planes.  A raw
/// fallback has only `raw_metadata`; an absent metadata row has neither.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticTransaction<'a> {
    pub block_id: u32,
    pub slot: u64,
    pub tx_index: u32,
    pub source_flags: u16,
    pub effect_state: u8,
    pub message: &'a [u8],
    pub loaded_addresses: Option<&'a [u8]>,
    pub inner_instructions: Option<&'a [u8]>,
    pub token_balances: Option<&'a [u8]>,
    pub outcome: Option<&'a [u8]>,
    pub raw_metadata: Option<&'a [u8]>,
    pub signature_ordinals: Range<u64>,
    pub signature_bytes: Range<u64>,
}

/// Exact work receipt for one semantic block visit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SemanticBlockReadStats {
    pub block_id: u32,
    pub block_transactions: u32,
    pub requested_transactions: u32,
    pub visited_transactions: u32,
    pub object_reads: [ObjectReadStats; OBJECT_COUNT],
    pub peak_decoded_bytes: u64,
    /// Conservative decoded-plus-stored upper bound. This double-counts raw
    /// planes, but it never understates the buffers controlled by this API.
    /// DecodedDirectory heap and allocator/RSS overhead remain outside it.
    pub peak_retained_bytes_upper_bound: u64,
    pub selected_semantic_bytes: u64,
}

impl SemanticBlockReadStats {
    pub fn total_read_calls(&self) -> u64 {
        self.object_reads.iter().map(|stats| stats.read_calls).sum()
    }

    pub fn total_stored_bytes(&self) -> u64 {
        self.object_reads
            .iter()
            .map(|stats| stats.stored_bytes)
            .sum()
    }

    pub fn total_decoded_bytes(&self) -> u64 {
        self.object_reads
            .iter()
            .map(|stats| stats.decoded_bytes)
            .sum()
    }
}

#[derive(Debug, Default)]
struct SemanticStoredPlane {
    offset: u64,
    bytes: Vec<u8>,
}

/// Exact zero-gap stored bytes for one contiguous block window.
///
/// The normal stored allocation is at most 32 MiB across all semantic planes.
/// A single larger block can use at most the already validated 768 MiB packed
/// block cap. During loading, one additional response body is at most 32 MiB.
/// During decoding, the existing 2 GiB stored-plus-decoded guard is the live
/// controlled-buffer upper bound. Directory heap and allocator/RSS overhead
/// remain outside these explicit bounds.
#[derive(Debug)]
struct SemanticStoredBatch {
    block_range: Range<usize>,
    planes: [SemanticStoredPlane; OBJECT_COUNT],
    stored_bytes: usize,
}

impl SemanticStoredBatch {
    fn contains(&self, block_ordinal: usize) -> bool {
        self.block_range.contains(&block_ordinal)
    }

    fn read_object_chunk(&self, block: &BlockRow, object: Object) -> Result<Vec<u8>> {
        ensure!(
            self.contains(block.block_id as usize),
            "semantic block is outside the loaded contiguous batch"
        );
        let locator = block.locators[object.index()];
        if locator.stored_len == 0 {
            return Ok(Vec::new());
        }
        let plane = &self.planes[object.index()];
        let relative_start = locator
            .offset
            .checked_sub(plane.offset)
            .context("semantic locator starts before its loaded batch plane")?;
        let relative_end = relative_start
            .checked_add(u64::from(locator.stored_len))
            .context("semantic batch slice end overflow")?;
        let start =
            usize::try_from(relative_start).context("semantic batch slice start exceeds usize")?;
        let end =
            usize::try_from(relative_end).context("semantic batch slice end exceeds usize")?;
        let stored = plane
            .bytes
            .get(start..end)
            .context("semantic locator is outside its loaded batch plane")?;
        decode_object_chunk(stored, locator, object)
    }
}

/// A strict sequential semantic scan over a bounded contiguous block range.
///
/// This opt-in session preserves the point-reader API and its exact per-block
/// logical receipts. It reduces remote requests by coalescing only adjacent
/// chunks in the same plane. The permitted gap is zero bytes, so it never
/// fetches unrelated planes or blocks outside the requested range.
pub struct ContiguousSemanticScan<'a> {
    reader: &'a Reader,
    requested_range: Range<usize>,
    next_block: usize,
    batch: Option<SemanticStoredBatch>,
}

impl ContiguousSemanticScan<'_> {
    pub fn visit_semantic_transactions(
        &mut self,
        block_ordinal: usize,
        transaction_indexes: Option<&[u32]>,
        visit: impl FnMut(SemanticTransaction<'_>) -> Result<()>,
    ) -> Result<SemanticBlockReadStats> {
        ensure!(
            block_ordinal == self.next_block,
            "contiguous semantic scan blocks must be visited once in increasing order"
        );
        ensure!(
            self.requested_range.contains(&block_ordinal),
            "semantic block is outside the requested contiguous scan"
        );
        if self
            .batch
            .as_ref()
            .is_none_or(|batch| !batch.contains(block_ordinal))
        {
            let end = self
                .reader
                .semantic_batch_end(block_ordinal, self.requested_range.end)?;
            // Drop the old allocation before the next remote batch starts.
            self.batch = None;
            let batch = self.reader.load_semantic_stored_batch(block_ordinal..end)?;
            self.batch = Some(batch);
        }
        let batch = self.batch.as_ref().context("semantic batch is missing")?;
        let stats = self.reader.visit_semantic_transactions_with_reader(
            block_ordinal,
            transaction_indexes,
            |block, object| batch.read_object_chunk(block, object),
            visit,
        )?;
        self.next_block = self
            .next_block
            .checked_add(1)
            .context("contiguous semantic scan ordinal overflow")?;
        Ok(stats)
    }

    /// Stored bytes held by the active zero-gap batch. This lets callers add
    /// the batch allocation to a decoded-block peak without changing logical
    /// I/O receipts.
    pub fn current_batch_stored_bytes(&self) -> u64 {
        self.batch
            .as_ref()
            .map_or(0, |batch| batch.stored_bytes as u64)
    }

    pub fn finish(self) -> Result<()> {
        ensure!(
            self.next_block == self.requested_range.end,
            "contiguous semantic scan ended before all requested blocks were visited"
        );
        Ok(())
    }
}

/// Owned candidate bytes needed by the read-only directory-v3 measurement.
/// No source archive object is opened by this API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDirectoryMeasurementBlock {
    pub block_id: u32,
    pub tx_count: u32,
    pub first_signature_ordinal: u64,
    pub signature_count: u32,
    pub final_object_decoded_lengths: [u32; 9],
    pub directory: Vec<u8>,
    pub transaction_rewards: Vec<u8>,
    pub raw_metadata_fallbacks: Vec<u8>,
}

pub struct Reader {
    root: PathBuf,
    source: Arc<dyn RangeSource>,
    pub header: FileHeader,
    rows: Vec<BlockRow>,
    metadata_schema: CompactV2MetadataSchema,
    message_schema: CompactV2MessageSchema,
    open_read_stats: OpenReadStats,
}

impl Reader {
    /// Open and strictly validate a standalone canary.
    ///
    /// This is a reader-only operation. The converter never calls it after
    /// writing output.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let allowed_objects = standalone_source_objects();
        let source = PinnedLocalRangeSource::new_anchored(
            &root,
            &allowed_objects
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        )
        .context("open pinned standalone source directory")?;
        Self::open_from_source(Arc::new(source), root, 1)
    }

    /// Open the same frozen reader over a strict immutable range source.
    ///
    /// The source is shared with adaptive indexes so all reads use one pinned
    /// local generation or one metered HTTP client. The label is diagnostic
    /// only and never participates in object addressing.
    pub fn open_with_source(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<PathBuf>,
    ) -> Result<Self> {
        let rows_per_read = (MAX_REMOTE_INDEX_RANGE_BYTES / INDEX_ROW_LEN).max(1);
        Self::open_from_source(source, source_label.into(), rows_per_read)
    }

    pub(super) fn open_with_local_source(
        source: Arc<dyn RangeSource>,
        source_label: impl Into<PathBuf>,
    ) -> Result<Self> {
        Self::open_from_source(source, source_label.into(), 1)
    }

    fn open_from_source(
        source: Arc<dyn RangeSource>,
        root: PathBuf,
        rows_per_read: usize,
    ) -> Result<Self> {
        let index_size = required_source_size(source.as_ref(), INDEX_FILE, "standalone index")?;
        let header_bytes = read_source_exact(
            source.as_ref(),
            INDEX_FILE,
            0,
            FILE_HEADER_LEN,
            "standalone index header",
        )?;
        let header = FileHeader::decode(&header_bytes)?;
        ensure!(
            header.magic == header.format.index_magic(),
            "standalone index has data magic"
        );
        // Validate the allocation-driving row count before converting it or
        // reserving the index vector. One epoch cannot contain more block rows
        // than slots, and the public block identity is u32.
        ensure!(
            header.selected_blocks <= header.slots_per_epoch,
            "standalone block count exceeds slots per epoch"
        );
        ensure!(
            header.selected_blocks <= u64::from(u32::MAX),
            "standalone block count exceeds u32"
        );
        let row_count =
            usize::try_from(header.selected_blocks).context("block count exceeds usize")?;
        let retained_row_bytes = row_count
            .checked_mul(mem::size_of::<BlockRow>())
            .context("standalone retained index-row bytes overflow")?;
        ensure!(
            retained_row_bytes <= MAX_RETAINED_INDEX_ROW_BYTES,
            "standalone retained index rows exceed {MAX_RETAINED_INDEX_ROW_BYTES} bytes"
        );
        let expected_index_bytes = (FILE_HEADER_LEN as u64)
            .checked_add(
                header
                    .selected_blocks
                    .checked_mul(INDEX_ROW_LEN as u64)
                    .context("standalone index length overflow")?,
            )
            .context("standalone index length overflow")?;
        ensure!(
            index_size == expected_index_bytes,
            "standalone index file length disagrees with header"
        );
        let mut rows = Vec::new();
        rows.try_reserve_exact(row_count)
            .context("reserve standalone index rows")?;
        let mut expected_offsets = [FILE_HEADER_LEN as u64; OBJECT_COUNT];
        let mut expected_first_tx = None;
        let mut expected_first_signature = None;
        let mut counted_transactions = 0_u64;
        let mut ordinal = 0_usize;
        let mut index_range_reads = 0_u64;
        while ordinal < row_count {
            let rows_in_read = (row_count - ordinal).min(rows_per_read);
            let offset = (FILE_HEADER_LEN as u64)
                .checked_add(
                    u64::try_from(ordinal)?
                        .checked_mul(INDEX_ROW_LEN as u64)
                        .context("standalone index range offset overflow")?,
                )
                .context("standalone index range offset overflow")?;
            let length = rows_in_read
                .checked_mul(INDEX_ROW_LEN)
                .context("standalone index range length overflow")?;
            let bytes = read_source_exact(
                source.as_ref(),
                INDEX_FILE,
                offset,
                length,
                "standalone index rows",
            )?;
            index_range_reads = index_range_reads
                .checked_add(1)
                .context("standalone index read count overflow")?;
            for row_bytes in bytes.chunks_exact(INDEX_ROW_LEN) {
                let row = BlockRow::decode(row_bytes)?;
                let packed_bytes = row.locators.iter().try_fold(0_u64, |total, locator| {
                    total
                        .checked_add(u64::from(locator.stored_len))
                        .context("standalone block packed-byte total overflow")
                })?;
                ensure!(
                    packed_bytes <= MAX_PACKED_BYTES as u64,
                    "standalone block packed bytes exceed {MAX_PACKED_BYTES}"
                );
                ensure!(
                    row.block_id as usize == ordinal,
                    "standalone block IDs are not dense"
                );
                if let Some(expected) = expected_first_tx {
                    ensure!(
                        row.first_tx_ordinal == expected,
                        "transaction ordinals are not contiguous"
                    );
                }
                if let Some(expected) = expected_first_signature {
                    ensure!(
                        row.first_signature_ordinal == expected,
                        "signature ordinals are not contiguous"
                    );
                }
                for object in Object::ALL {
                    let locator = row.locators[object.index()];
                    ensure!(
                        locator.offset == expected_offsets[object.index()],
                        "standalone object locators are not contiguous"
                    );
                    expected_offsets[object.index()] = expected_offsets[object.index()]
                        .checked_add(u64::from(locator.stored_len))
                        .context("standalone object length overflow")?;
                }
                expected_first_tx = Some(
                    row.first_tx_ordinal
                        .checked_add(u64::from(row.tx_count))
                        .context("transaction ordinal overflow")?,
                );
                expected_first_signature = Some(
                    row.first_signature_ordinal
                        .checked_add(u64::from(row.signature_count))
                        .context("signature ordinal overflow")?,
                );
                counted_transactions = counted_transactions
                    .checked_add(u64::from(row.tx_count))
                    .context("transaction count overflow")?;
                rows.push(row);
                ordinal += 1;
            }
        }
        ensure!(
            counted_transactions == header.selected_transactions,
            "header transaction count disagrees with rows"
        );
        for object in Object::ALL {
            let object_size = required_source_size(
                source.as_ref(),
                object.file_name(),
                &format!("standalone {}", object.name()),
            )?;
            let object_header_bytes = read_source_exact(
                source.as_ref(),
                object.file_name(),
                0,
                FILE_HEADER_LEN,
                &format!("standalone {} header", object.name()),
            )?;
            let object_header = FileHeader::decode(&object_header_bytes)?;
            ensure!(
                object_header.magic == object_header.format.data_magic(),
                "standalone object has index magic"
            );
            ensure!(
                object_header.object == object as u16,
                "standalone object ID disagrees with file name"
            );
            ensure!(
                object_header.common_identity() == header.common_identity(),
                "standalone object binding differs from index"
            );
            ensure!(
                object_size == expected_offsets[object.index()],
                "standalone object length disagrees with locators"
            );
            for row in &rows {
                let locator = row.locators[object.index()];
                match object_header.policy {
                    value if value == CodecPolicy::Raw as u8 => {
                        ensure!(!locator.zstd, "raw standalone object contains a zstd chunk")
                    }
                    value if value == CodecPolicy::Zstd as u8 => ensure!(
                        locator.stored_len == 0 || locator.zstd,
                        "zstd standalone object contains a raw nonempty chunk"
                    ),
                    value if value == CodecPolicy::Adaptive as u8 => {}
                    _ => unreachable!("header decode checked policy"),
                }
            }
        }
        let metadata_schema = metadata_schema_from_code(header.metadata_schema)?;
        let message_schema = message_schema_from_code(header.message_schema)?;
        let index_bytes = (FILE_HEADER_LEN as u64)
            .saturating_add(header.selected_blocks.saturating_mul(INDEX_ROW_LEN as u64));
        let open_read_stats = OpenReadStats {
            read_calls: 1_u64
                .saturating_add(index_range_reads)
                .saturating_add(OBJECT_COUNT as u64),
            stored_bytes: index_bytes
                .saturating_add((OBJECT_COUNT as u64).saturating_mul(FILE_HEADER_LEN as u64)),
        };
        Ok(Self {
            root,
            source,
            header,
            rows,
            metadata_schema,
            message_schema,
            open_read_stats,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn block(&self, ordinal: usize) -> Option<&BlockRow> {
        self.rows.get(ordinal)
    }

    pub fn message_schema(&self) -> CompactV2MessageSchema {
        self.message_schema
    }

    pub fn metadata_schema(&self) -> CompactV2MetadataSchema {
        self.metadata_schema
    }

    /// Return exact logical bytes consumed while `open` validated the index
    /// and every object header. File metadata calls do not read payload bytes.
    pub fn open_read_stats(&self) -> OpenReadStats {
        self.open_read_stats
    }

    /// Start an opt-in, read-only, zero-gap batch session for a contiguous V3
    /// semantic scan.
    ///
    /// The caller must visit every block in the supplied half-open range once
    /// and in increasing order, then call `finish`. Each upstream range body is
    /// at most 32 MiB. Normal batches retain at most 32 MiB of stored bytes.
    pub fn begin_contiguous_semantic_scan(
        &self,
        block_range: Range<usize>,
    ) -> Result<ContiguousSemanticScan<'_>> {
        ensure!(
            self.header.format == StandaloneFormat::V3,
            "contiguous semantic scan requires standalone V3"
        );
        ensure!(
            block_range.start <= block_range.end && block_range.end <= self.rows.len(),
            "contiguous semantic scan range is outside archive"
        );
        Ok(ContiguousSemanticScan {
            reader: self,
            next_block: block_range.start,
            requested_range: block_range,
            batch: None,
        })
    }

    fn semantic_block_memory_bounds(&self, block: &BlockRow) -> Result<(usize, usize)> {
        let decoded = SEMANTIC_OBJECTS.iter().try_fold(0_usize, |total, object| {
            total
                .checked_add(block.locators[object.index()].decoded_len as usize)
                .context("semantic block decoded-byte total overflow")
        })?;
        ensure!(
            decoded <= MAX_SEMANTIC_BLOCK_DECODED_BYTES,
            "semantic block decoded bytes exceed {MAX_SEMANTIC_BLOCK_DECODED_BYTES}"
        );
        let stored = SEMANTIC_OBJECTS.iter().try_fold(0_usize, |total, object| {
            total
                .checked_add(block.locators[object.index()].stored_len as usize)
                .context("semantic block stored-byte total overflow")
        })?;
        let retained = decoded
            .checked_add(stored)
            .context("semantic retained-byte upper bound overflow")?;
        ensure!(
            retained <= MAX_SEMANTIC_BLOCK_RETAINED_UPPER_BOUND,
            "semantic retained-byte upper bound exceeds {MAX_SEMANTIC_BLOCK_RETAINED_UPPER_BOUND}"
        );
        Ok((decoded, stored))
    }

    fn semantic_batch_end(&self, start: usize, requested_end: usize) -> Result<usize> {
        ensure!(start < requested_end, "semantic batch cannot be empty");
        let mut end = start;
        let mut stored = 0_usize;
        while end < requested_end {
            let block = self
                .rows
                .get(end)
                .context("semantic batch block is missing")?;
            let (_, block_stored) = self.semantic_block_memory_bounds(block)?;
            let candidate = stored
                .checked_add(block_stored)
                .context("semantic batch stored-byte total overflow")?;
            if end > start && candidate > MAX_REMOTE_SEMANTIC_BATCH_STORED_BYTES {
                break;
            }
            stored = candidate;
            end += 1;
            if stored > MAX_REMOTE_SEMANTIC_BATCH_STORED_BYTES {
                // A single validated large block is an isolated batch.
                break;
            }
        }
        ensure!(end > start, "semantic batch planner made no progress");
        Ok(end)
    }

    fn load_semantic_stored_batch(&self, block_range: Range<usize>) -> Result<SemanticStoredBatch> {
        ensure!(
            block_range.start < block_range.end && block_range.end <= self.rows.len(),
            "semantic stored batch range is outside archive"
        );
        let first = &self.rows[block_range.start];
        let last = &self.rows[block_range.end - 1];
        let mut planes: [SemanticStoredPlane; OBJECT_COUNT] =
            array::from_fn(|_| Default::default());
        let mut aggregate_stored = 0_usize;
        for object in SEMANTIC_OBJECTS {
            let offset = first.locators[object.index()].offset;
            let end = last.locators[object.index()]
                .offset
                .checked_add(u64::from(last.locators[object.index()].stored_len))
                .context("semantic batch plane end overflow")?;
            let span = end
                .checked_sub(offset)
                .context("semantic batch plane range decreases")?;
            let exact_stored =
                self.rows[block_range.clone()]
                    .iter()
                    .try_fold(0_u64, |total, block| {
                        total
                            .checked_add(u64::from(block.locators[object.index()].stored_len))
                            .context("semantic batch plane byte total overflow")
                    })?;
            ensure!(
                span == exact_stored,
                "semantic batch plane has a nonzero gap"
            );
            let length = usize::try_from(span).context("semantic batch plane exceeds usize")?;
            aggregate_stored = aggregate_stored
                .checked_add(length)
                .context("semantic batch aggregate stored-byte overflow")?;
            let bytes = read_source_exact_bounded(
                self.source.as_ref(),
                object.file_name(),
                offset,
                length,
                MAX_REMOTE_SEMANTIC_RANGE_BYTES,
                "contiguous semantic batch plane",
            )?;
            planes[object.index()] = SemanticStoredPlane { offset, bytes };
        }
        ensure!(
            block_range.len() == 1 || aggregate_stored <= MAX_REMOTE_SEMANTIC_BATCH_STORED_BYTES,
            "multi-block semantic batch exceeds stored-byte budget"
        );
        ensure!(
            aggregate_stored <= MAX_PACKED_BYTES,
            "semantic batch exceeds validated packed-byte cap"
        );
        Ok(SemanticStoredBatch {
            block_range,
            planes,
            stored_bytes: aggregate_stored,
        })
    }

    /// Visit selected transactions while reading each required V3 block plane
    /// at most once.
    ///
    /// `transaction_indexes=None` visits the complete block. A supplied slice
    /// must be strictly increasing and duplicate-free. The visitor receives
    /// exact borrowed Compact V2 field bytes and must consume them before this
    /// method returns. Source files are never written.
    pub fn visit_semantic_transactions(
        &self,
        block_ordinal: usize,
        transaction_indexes: Option<&[u32]>,
        visit: impl FnMut(SemanticTransaction<'_>) -> Result<()>,
    ) -> Result<SemanticBlockReadStats> {
        self.visit_semantic_transactions_with_reader(
            block_ordinal,
            transaction_indexes,
            |block, object| self.read_object_chunk(block, object),
            visit,
        )
    }

    fn visit_semantic_transactions_with_reader(
        &self,
        block_ordinal: usize,
        transaction_indexes: Option<&[u32]>,
        mut read_object: impl FnMut(&BlockRow, Object) -> Result<Vec<u8>>,
        mut visit: impl FnMut(SemanticTransaction<'_>) -> Result<()>,
    ) -> Result<SemanticBlockReadStats> {
        ensure!(
            self.header.format == StandaloneFormat::V3,
            "semantic block visitor requires standalone V3"
        );
        let block = self
            .rows
            .get(block_ordinal)
            .context("standalone block ordinal is outside archive")?;
        if let Some(indexes) = transaction_indexes {
            let mut previous = None;
            for &tx_index in indexes {
                ensure!(
                    tx_index < block.tx_count,
                    "semantic transaction index is outside block"
                );
                ensure!(
                    previous.is_none_or(|value| tx_index > value),
                    "semantic transaction indexes are not strictly increasing"
                );
                previous = Some(tx_index);
            }
        }

        let (peak_decoded_bytes, selected_stored_bytes) =
            self.semantic_block_memory_bounds(block)?;
        let peak_retained_bytes_upper_bound = peak_decoded_bytes
            .checked_add(selected_stored_bytes)
            .context("semantic retained-byte upper bound overflow")?;

        let mut stats = SemanticBlockReadStats {
            block_id: block.block_id,
            block_transactions: block.tx_count,
            requested_transactions: transaction_indexes
                .map_or(block.tx_count, |indexes| indexes.len() as u32),
            visited_transactions: 0,
            object_reads: [ObjectReadStats::default(); OBJECT_COUNT],
            peak_decoded_bytes: peak_decoded_bytes as u64,
            peak_retained_bytes_upper_bound: peak_retained_bytes_upper_bound as u64,
            selected_semantic_bytes: 0,
        };
        let mut read = |object: Object| -> Result<Vec<u8>> {
            let bytes = read_object(block, object)?;
            stats.object_reads[object.index()].record(block.locators[object.index()])?;
            Ok(bytes)
        };
        let directory_bytes = read(Object::TransactionDirectory)?;
        let messages = read(Object::Messages)?;
        let loaded_addresses = read(Object::LoadedAddresses)?;
        let inner_instructions = read(Object::InnerInstructions)?;
        let token_balances = read(Object::TokenBalances)?;
        let outcomes = read(Object::Outcomes)?;
        let raw_metadata = read(Object::RawMetadataFallbacks)?;

        let directory = DecodedDirectory::decode_production(&directory_bytes)
            .map_err(|error| anyhow::anyhow!("decode standalone v3 directory: {error}"))?;
        ensure!(
            directory.header.tx_count == block.tx_count,
            "v3 directory transaction count disagrees with block"
        );
        let object_lengths =
            Object::PER_TRANSACTION.map(|object| block.locators[object.index()].decoded_len);
        directory
            .verify_external_totals(object_lengths, u64::from(block.signature_count))
            .map_err(|error| anyhow::anyhow!("bind standalone v3 directory totals: {error}"))?;

        let visit_one = |tx_index: u32,
                         stats: &mut SemanticBlockReadStats,
                         visit: &mut dyn FnMut(SemanticTransaction<'_>) -> Result<()>|
         -> Result<()> {
            let selected = directory
                .lookup(tx_index, block.first_signature_ordinal)
                .map_err(|error| anyhow::anyhow!("lookup standalone v3 transaction: {error}"))?;
            let message_range = stored_object_range(&selected.objects[0], "message")?;
            let message = slice_ref(&messages, message_range.start, message_range.end, "message")?;
            ensure!(!message.is_empty(), "standalone message range is empty");
            validate_message(
                self.message_schema,
                message,
                selected.source_flags,
                selected.signature_count,
            )?;

            let flags = u32::from(selected.source_flags);
            let has_metadata = flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0;
            let raw_fallback = flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0;
            let (loaded, inner, token, outcome, raw) = if !has_metadata {
                ensure!(
                    selected.effect_state == 0
                        && selected.objects[1..]
                            .iter()
                            .all(|slice| matches!(slice, ObjectSlice::Absent)),
                    "missing metadata transaction has effect ranges"
                );
                (None, None, None, None, None)
            } else if raw_fallback {
                ensure!(
                    selected.effect_state == 0
                        && selected.objects[1..8]
                            .iter()
                            .all(|slice| matches!(slice, ObjectSlice::Absent)),
                    "raw metadata transaction has decoded effect ranges"
                );
                let range = stored_object_range(&selected.objects[8], "raw metadata")?;
                let raw = slice_ref(&raw_metadata, range.start, range.end, "raw metadata")?;
                ensure!(!raw.is_empty(), "raw metadata range is empty");
                (None, None, None, None, Some(raw))
            } else {
                ensure!(
                    matches!(selected.effect_state & 0b111, 1..=3)
                        && selected.effect_state & 0b0001_1000 == 0b0001_1000
                        && matches!(selected.objects[8], ObjectSlice::Absent),
                    "decoded metadata transaction has invalid effect state or raw bytes"
                );
                let ranges = [
                    stored_object_range(&selected.objects[1], "loaded addresses")?,
                    stored_object_range(&selected.objects[2], "inner instructions")?,
                    stored_object_range(&selected.objects[4], "token balances")?,
                    stored_object_range(&selected.objects[6], "outcome")?,
                ];
                let loaded = slice_ref(
                    &loaded_addresses,
                    ranges[0].start,
                    ranges[0].end,
                    "loaded addresses",
                )?;
                let inner = slice_ref(
                    &inner_instructions,
                    ranges[1].start,
                    ranges[1].end,
                    "inner instructions",
                )?;
                let token = slice_ref(
                    &token_balances,
                    ranges[2].start,
                    ranges[2].end,
                    "token balances",
                )?;
                let outcome = slice_ref(&outcomes, ranges[3].start, ranges[3].end, "outcome")?;
                let mut outcome_cursor = outcome;
                decode::decode_metadata_error_with_schema(
                    &mut outcome_cursor,
                    self.metadata_schema,
                )
                .context("decode standalone v3 semantic outcome")?;
                <u64 as SchemaRead<'_, decode::Cfg>>::get(&mut outcome_cursor)
                    .context("decode standalone v3 semantic fee")?;
                (Some(loaded), Some(inner), Some(token), Some(outcome), None)
            };

            let semantic_bytes = [Some(message), loaded, inner, token, outcome, raw]
                .into_iter()
                .flatten()
                .try_fold(0_u64, |total, bytes| {
                    total
                        .checked_add(bytes.len() as u64)
                        .context("selected semantic-byte count overflow")
                })?;
            stats.selected_semantic_bytes = stats
                .selected_semantic_bytes
                .checked_add(semantic_bytes)
                .context("selected semantic-byte total overflow")?;
            visit(SemanticTransaction {
                block_id: block.block_id,
                slot: block.slot,
                tx_index,
                source_flags: selected.source_flags,
                effect_state: selected.effect_state,
                message,
                loaded_addresses: loaded,
                inner_instructions: inner,
                token_balances: token,
                outcome,
                raw_metadata: raw,
                signature_ordinals: selected.absolute_signature_ordinals,
                signature_bytes: selected.absolute_signature_bytes,
            })?;
            stats.visited_transactions = stats
                .visited_transactions
                .checked_add(1)
                .context("visited semantic transaction count overflow")?;
            Ok(())
        };

        match transaction_indexes {
            Some(indexes) => {
                for &tx_index in indexes {
                    visit_one(tx_index, &mut stats, &mut visit)?;
                }
            }
            None => {
                for tx_index in 0..block.tx_count {
                    visit_one(tx_index, &mut stats, &mut visit)?;
                }
            }
        }
        ensure!(
            stats.visited_transactions == stats.requested_transactions,
            "semantic visitor completed a different transaction count"
        );
        Ok(stats)
    }

    /// Read only the three standalone-v2 chunks required by the directory-v3
    /// measurement adapter. Each locator is read once and strictly decoded.
    pub fn read_directory_measurement_block(
        &self,
        block_ordinal: usize,
    ) -> Result<ReadDirectoryMeasurementBlock> {
        ensure!(
            self.header.format == StandaloneFormat::V2,
            "v2 directory measurement API does not accept standalone v3"
        );
        let block = self
            .rows
            .get(block_ordinal)
            .context("standalone block ordinal is outside archive")?;
        let directory = self.read_object_chunk(block, Object::TransactionDirectory)?;
        let expected_directory_len = usize::try_from(block.tx_count)?
            .checked_mul(DIRECTORY_ROW_LEN)
            .context("directory length overflow")?;
        ensure!(
            directory.len() == expected_directory_len,
            "directory decoded length disagrees with block"
        );
        let transaction_rewards = self.read_object_chunk(block, Object::TransactionRewards)?;
        let raw_metadata_fallbacks = self.read_object_chunk(block, Object::RawMetadataFallbacks)?;
        let final_object_decoded_lengths =
            Object::PER_TRANSACTION.map(|object| block.locators[object.index()].decoded_len);
        ensure!(
            transaction_rewards.len()
                == final_object_decoded_lengths[Object::TransactionRewards.index() - 1] as usize,
            "transaction reward decoded length disagrees with locator"
        );
        ensure!(
            raw_metadata_fallbacks.len()
                == final_object_decoded_lengths[Object::RawMetadataFallbacks.index() - 1] as usize,
            "raw metadata decoded length disagrees with locator"
        );
        Ok(ReadDirectoryMeasurementBlock {
            block_id: block.block_id,
            tx_count: block.tx_count,
            first_signature_ordinal: block.first_signature_ordinal,
            signature_count: block.signature_count,
            final_object_decoded_lengths,
            directory,
            transaction_rewards,
            raw_metadata_fallbacks,
        })
    }

    pub fn read_transaction(
        &mut self,
        block_ordinal: usize,
        tx_index: u32,
    ) -> Result<ReadTransaction> {
        let block = self
            .rows
            .get(block_ordinal)
            .cloned()
            .context("standalone block ordinal is outside archive")?;
        ensure!(
            tx_index < block.tx_count,
            "transaction index is outside block"
        );
        if self.header.format == StandaloneFormat::V3 {
            return self.read_transaction_v3(block, tx_index);
        }
        let directory = self.read_object_chunk(&block, Object::TransactionDirectory)?;
        let expected_directory_len = usize::try_from(block.tx_count)?
            .checked_mul(DIRECTORY_ROW_LEN)
            .context("directory length overflow")?;
        ensure!(
            directory.len() == expected_directory_len,
            "directory decoded length disagrees with block"
        );
        let mut directory_rows = Vec::new();
        directory_rows
            .try_reserve_exact(block.tx_count as usize)
            .context("reserve standalone v2 directory rows")?;
        let mut prior_ends = [0_u32; 9];
        let mut signature_prefix = 0_u64;
        for index in 0..block.tx_count as usize {
            let start = index * DIRECTORY_ROW_LEN;
            let row = DirectoryRow::decode(&directory[start..start + DIRECTORY_ROW_LEN])?;
            for (plane, end) in row.ends.iter().copied().enumerate() {
                ensure!(end >= prior_ends[plane], "directory end offsets decrease");
            }
            if index < tx_index as usize {
                signature_prefix = signature_prefix
                    .checked_add(u64::from(row.signature_count))
                    .context("signature prefix overflow")?;
            }
            prior_ends = row.ends;
            directory_rows.push(row);
        }
        let total_signatures = directory_rows.iter().try_fold(0_u64, |total, row| {
            total
                .checked_add(u64::from(row.signature_count))
                .context("signature count overflow")
        })?;
        ensure!(
            total_signatures == u64::from(block.signature_count),
            "directory signature count disagrees with block"
        );
        for (position, object) in Object::PER_TRANSACTION.into_iter().enumerate() {
            ensure!(
                u64::from(prior_ends[position])
                    == u64::from(block.locators[object.index()].decoded_len),
                "directory final end disagrees with object chunk"
            );
        }
        let selected = directory_rows[tx_index as usize];
        let prior = if tx_index == 0 {
            [0_u32; 9]
        } else {
            directory_rows[tx_index as usize - 1].ends
        };
        let message_chunk = self.read_object_chunk(&block, Object::Messages)?;
        let message = slice_owned(&message_chunk, prior[0], selected.ends[0], "message")?;
        ensure!(!message.is_empty(), "standalone message range is empty");

        let flags = u32::from(selected.source_flags);
        if flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK == 0 {
            let decoded = decode_compact_v2_message(self.message_schema, &message)
                .context("decode reconstructed standalone message")?;
            let is_v0 = matches!(decoded, ArchiveV2HotMessagePayload::V0(_));
            ensure!(
                is_v0 == (flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
                "reconstructed message version disagrees with source flags"
            );
            let required_signatures = match decoded {
                ArchiveV2HotMessagePayload::Legacy(message) => {
                    message.header.num_required_signatures
                }
                ArchiveV2HotMessagePayload::V0(message) => message.header.num_required_signatures,
                ArchiveV2HotMessagePayload::V1(message) => message.header.num_required_signatures,
            };
            ensure!(
                required_signatures == selected.signature_count,
                "reconstructed message signature count disagrees with directory"
            );
        }
        let metadata = if flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                selected.effect_state == 0,
                "missing metadata transaction has decoded effect state"
            );
            ensure!(
                (1..=8).all(|plane| prior[plane] == selected.ends[plane]),
                "missing metadata transaction has effect bytes"
            );
            MetadataBytes::Absent
        } else if flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            ensure!(
                selected.effect_state == 0,
                "raw metadata transaction has decoded effect state"
            );
            ensure!(
                (1..=7).all(|plane| prior[plane] == selected.ends[plane]),
                "raw metadata transaction has decoded effect bytes"
            );
            let raw = self.read_object_chunk(&block, Object::RawMetadataFallbacks)?;
            let bytes = slice_owned(&raw, prior[8], selected.ends[8], "raw metadata")?;
            ensure!(!bytes.is_empty(), "raw metadata range is empty");
            MetadataBytes::RawFallback(bytes)
        } else {
            ensure!(
                matches!(selected.effect_state & 0b111, 1..=3)
                    && selected.effect_state & 0b0001_1000 == 0b0001_1000,
                "decoded metadata effect state is invalid"
            );
            ensure!(
                prior[8] == selected.ends[8],
                "decoded metadata transaction has raw fallback bytes"
            );
            let bytes = self.reconstruct_decoded_metadata(&block, prior, selected.ends)?;
            let mut cursor = bytes.as_slice();
            decode::stream_metadata_effects_structural_with_schema(
                &mut cursor,
                self.metadata_schema,
                |_event| Ok::<(), anyhow::Error>(()),
            )
            .context("decode reconstructed standalone metadata")?;
            ensure!(
                cursor.is_empty(),
                "reconstructed metadata has trailing bytes"
            );
            MetadataBytes::Decoded(bytes)
        };

        let signature_start = block
            .first_signature_ordinal
            .checked_add(signature_prefix)
            .context("signature ordinal start overflow")?;
        let signature_end = signature_start
            .checked_add(u64::from(selected.signature_count))
            .context("signature ordinal end overflow")?;
        let byte_start = signature_start
            .checked_mul(SIGNATURE_BYTES)
            .context("signature byte start overflow")?;
        let byte_end = signature_end
            .checked_mul(SIGNATURE_BYTES)
            .context("signature byte end overflow")?;
        Ok(ReadTransaction {
            block_id: block.block_id,
            slot: block.slot,
            tx_index,
            source_flags: selected.source_flags,
            effect_state: selected.effect_state,
            message,
            metadata,
            signature_ordinals: signature_start..signature_end,
            signature_bytes: byte_start..byte_end,
            directory_validation_records_scanned: u64::from(block.tx_count),
            reward_validation_records_scanned: 0,
            raw_fallback_validation_records_scanned: 0,
            directory_lookup_records_scanned: block.tx_count,
            reward_lookup_records_scanned: 0,
            raw_fallback_lookup_records_scanned: 0,
        })
    }

    fn read_transaction_v3(&mut self, block: BlockRow, tx_index: u32) -> Result<ReadTransaction> {
        let directory_bytes = self.read_object_chunk(&block, Object::TransactionDirectory)?;
        let directory = DecodedDirectory::decode_production(&directory_bytes)
            .map_err(|error| anyhow::anyhow!("decode standalone v3 directory: {error}"))?;
        ensure!(
            directory.header.tx_count == block.tx_count,
            "v3 directory transaction count disagrees with block"
        );
        let object_lengths =
            Object::PER_TRANSACTION.map(|object| block.locators[object.index()].decoded_len);
        directory
            .verify_external_totals(object_lengths, u64::from(block.signature_count))
            .map_err(|error| anyhow::anyhow!("bind standalone v3 directory totals: {error}"))?;
        let selected = directory
            .lookup(tx_index, block.first_signature_ordinal)
            .map_err(|error| anyhow::anyhow!("lookup standalone v3 transaction: {error}"))?;
        let validation_scans = directory.validation_scan_counters();

        let message_range = stored_object_range(&selected.objects[0], "message")?;
        let message_chunk = self.read_object_chunk(&block, Object::Messages)?;
        let message = slice_owned(
            &message_chunk,
            message_range.start,
            message_range.end,
            "message",
        )?;
        ensure!(!message.is_empty(), "standalone message range is empty");
        validate_message(
            self.message_schema,
            &message,
            selected.source_flags,
            selected.signature_count,
        )?;

        let flags = u32::from(selected.source_flags);
        let metadata = if flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
            ensure!(
                selected.effect_state == 0,
                "missing metadata transaction has decoded effect state"
            );
            ensure!(
                selected.objects[1..]
                    .iter()
                    .all(|slice| matches!(slice, ObjectSlice::Absent)),
                "missing metadata transaction has effect ranges"
            );
            MetadataBytes::Absent
        } else if flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
            ensure!(
                selected.effect_state == 0,
                "raw metadata transaction has decoded effect state"
            );
            ensure!(
                selected.objects[1..8]
                    .iter()
                    .all(|slice| matches!(slice, ObjectSlice::Absent)),
                "raw metadata transaction has decoded effect ranges"
            );
            let raw_range = stored_object_range(&selected.objects[8], "raw metadata")?;
            let raw = self.read_object_chunk(&block, Object::RawMetadataFallbacks)?;
            let bytes = slice_owned(&raw, raw_range.start, raw_range.end, "raw metadata")?;
            ensure!(!bytes.is_empty(), "raw metadata range is empty");
            MetadataBytes::RawFallback(bytes)
        } else {
            ensure!(
                matches!(selected.effect_state & 0b111, 1..=3)
                    && selected.effect_state & 0b0001_1000 == 0b0001_1000,
                "decoded metadata effect state is invalid"
            );
            ensure!(
                matches!(selected.objects[8], ObjectSlice::Absent),
                "decoded metadata transaction has raw fallback bytes"
            );
            let bytes =
                self.reconstruct_decoded_metadata_v3(&block, &selected.objects, &selected.reward)?;
            let mut cursor = bytes.as_slice();
            decode::stream_metadata_effects_structural_with_schema(
                &mut cursor,
                self.metadata_schema,
                |_event| Ok::<(), anyhow::Error>(()),
            )
            .context("decode reconstructed standalone v3 metadata")?;
            ensure!(
                cursor.is_empty(),
                "reconstructed v3 metadata has trailing bytes"
            );
            MetadataBytes::Decoded(bytes)
        };

        Ok(ReadTransaction {
            block_id: block.block_id,
            slot: block.slot,
            tx_index,
            source_flags: selected.source_flags,
            effect_state: selected.effect_state,
            message,
            metadata,
            signature_ordinals: selected.absolute_signature_ordinals,
            signature_bytes: selected.absolute_signature_bytes,
            directory_validation_records_scanned: validation_scans.dense_records,
            reward_validation_records_scanned: validation_scans.reward_records,
            raw_fallback_validation_records_scanned: validation_scans.raw_fallback_records,
            directory_lookup_records_scanned: u32::from(selected.dense_records_scanned),
            reward_lookup_records_scanned: u32::from(selected.reward_records_scanned),
            raw_fallback_lookup_records_scanned: u32::from(selected.raw_records_scanned),
        })
    }

    fn reconstruct_decoded_metadata(
        &mut self,
        block: &BlockRow,
        starts: [u32; 9],
        ends: [u32; 9],
    ) -> Result<Vec<u8>> {
        let loaded_chunk = self.read_object_chunk(block, Object::LoadedAddresses)?;
        let inner_chunk = self.read_object_chunk(block, Object::InnerInstructions)?;
        let logs_chunk = self.read_object_chunk(block, Object::Logs)?;
        let token_chunk = self.read_object_chunk(block, Object::TokenBalances)?;
        let balances_chunk = self.read_object_chunk(block, Object::Balances)?;
        let outcomes_chunk = self.read_object_chunk(block, Object::Outcomes)?;
        let rewards_chunk = self.read_object_chunk(block, Object::TransactionRewards)?;
        let loaded = slice_ref(&loaded_chunk, starts[1], ends[1], "loaded addresses")?;
        let inner = slice_ref(&inner_chunk, starts[2], ends[2], "inner instructions")?;
        let logs = slice_ref(&logs_chunk, starts[3], ends[3], "logs")?;
        let token = slice_ref(&token_chunk, starts[4], ends[4], "token balances")?;
        let balances = slice_ref(&balances_chunk, starts[5], ends[5], "balances")?;
        let outcome = slice_ref(&outcomes_chunk, starts[6], ends[6], "outcome")?;
        let rewards = slice_ref(&rewards_chunk, starts[7], ends[7], "transaction rewards")?;
        ensure!(
            !outcome.is_empty(),
            "decoded metadata outcome range is empty"
        );
        let mut outcome_tail = outcome;
        decode::decode_metadata_error_with_schema(&mut outcome_tail, self.metadata_schema)
            .context("decode standalone outcome error boundary")?;
        <u64 as SchemaRead<'_, decode::Cfg>>::get(&mut outcome_tail)
            .context("decode standalone outcome fee boundary")?;
        let head_len = outcome.len() - outcome_tail.len();
        let (outcome_head, outcome_tail) = outcome.split_at(head_len);
        let total = outcome_head
            .len()
            .checked_add(balances.len())
            .and_then(|value| value.checked_add(inner.len()))
            .and_then(|value| value.checked_add(logs.len()))
            .and_then(|value| value.checked_add(token.len()))
            .and_then(|value| value.checked_add(rewards.len()))
            .and_then(|value| value.checked_add(loaded.len()))
            .and_then(|value| value.checked_add(outcome_tail.len()))
            .context("reconstructed metadata length overflow")?;
        let mut metadata = Vec::new();
        metadata
            .try_reserve_exact(total)
            .context("reserve reconstructed standalone metadata")?;
        metadata.extend_from_slice(outcome_head);
        metadata.extend_from_slice(balances);
        metadata.extend_from_slice(inner);
        metadata.extend_from_slice(logs);
        metadata.extend_from_slice(token);
        metadata.extend_from_slice(rewards);
        metadata.extend_from_slice(loaded);
        metadata.extend_from_slice(outcome_tail);
        Ok(metadata)
    }

    fn reconstruct_decoded_metadata_v3(
        &mut self,
        block: &BlockRow,
        objects: &[ObjectSlice; 9],
        reward: &RewardSlice,
    ) -> Result<Vec<u8>> {
        let loaded_range = stored_object_range(&objects[1], "loaded addresses")?;
        let inner_range = stored_object_range(&objects[2], "inner instructions")?;
        let logs_range = stored_object_range(&objects[3], "logs")?;
        let token_range = stored_object_range(&objects[4], "token balances")?;
        let balances_range = stored_object_range(&objects[5], "balances")?;
        let outcome_range = stored_object_range(&objects[6], "outcome")?;
        let loaded_chunk = self.read_object_chunk(block, Object::LoadedAddresses)?;
        let inner_chunk = self.read_object_chunk(block, Object::InnerInstructions)?;
        let logs_chunk = self.read_object_chunk(block, Object::Logs)?;
        let token_chunk = self.read_object_chunk(block, Object::TokenBalances)?;
        let balances_chunk = self.read_object_chunk(block, Object::Balances)?;
        let outcomes_chunk = self.read_object_chunk(block, Object::Outcomes)?;
        let loaded = slice_ref(
            &loaded_chunk,
            loaded_range.start,
            loaded_range.end,
            "loaded addresses",
        )?;
        let inner = slice_ref(
            &inner_chunk,
            inner_range.start,
            inner_range.end,
            "inner instructions",
        )?;
        let logs = slice_ref(&logs_chunk, logs_range.start, logs_range.end, "logs")?;
        let token = slice_ref(
            &token_chunk,
            token_range.start,
            token_range.end,
            "token balances",
        )?;
        let balances = slice_ref(
            &balances_chunk,
            balances_range.start,
            balances_range.end,
            "balances",
        )?;
        let outcome = slice_ref(
            &outcomes_chunk,
            outcome_range.start,
            outcome_range.end,
            "outcome",
        )?;
        let rewards_chunk;
        let rewards = match reward {
            RewardSlice::ImplicitCanonicalEmpty => {
                ensure!(
                    matches!(objects[7], ObjectSlice::ImplicitCanonicalEmpty),
                    "implicit reward range marker differs"
                );
                &[0_u8][..]
            }
            RewardSlice::SemanticStored(range) | RewardSlice::NoncanonicalEmptyStored(range) => {
                ensure!(
                    matches!(&objects[7], ObjectSlice::Stored(object_range) if object_range == range),
                    "stored reward range marker differs"
                );
                rewards_chunk = self.read_object_chunk(block, Object::TransactionRewards)?;
                slice_ref(
                    &rewards_chunk,
                    range.start,
                    range.end,
                    "transaction rewards",
                )?
            }
            RewardSlice::Absent => bail!("decoded metadata has no reward field"),
        };
        ensure!(
            !outcome.is_empty(),
            "decoded metadata outcome range is empty"
        );
        let mut outcome_tail = outcome;
        decode::decode_metadata_error_with_schema(&mut outcome_tail, self.metadata_schema)
            .context("decode standalone v3 outcome error boundary")?;
        <u64 as SchemaRead<'_, decode::Cfg>>::get(&mut outcome_tail)
            .context("decode standalone v3 outcome fee boundary")?;
        let head_len = outcome.len() - outcome_tail.len();
        let (outcome_head, outcome_tail) = outcome.split_at(head_len);
        let total = outcome_head
            .len()
            .checked_add(balances.len())
            .and_then(|value| value.checked_add(inner.len()))
            .and_then(|value| value.checked_add(logs.len()))
            .and_then(|value| value.checked_add(token.len()))
            .and_then(|value| value.checked_add(rewards.len()))
            .and_then(|value| value.checked_add(loaded.len()))
            .and_then(|value| value.checked_add(outcome_tail.len()))
            .context("reconstructed v3 metadata length overflow")?;
        let mut metadata = Vec::new();
        metadata
            .try_reserve_exact(total)
            .context("reserve reconstructed standalone v3 metadata")?;
        metadata.extend_from_slice(outcome_head);
        metadata.extend_from_slice(balances);
        metadata.extend_from_slice(inner);
        metadata.extend_from_slice(logs);
        metadata.extend_from_slice(token);
        metadata.extend_from_slice(rewards);
        metadata.extend_from_slice(loaded);
        metadata.extend_from_slice(outcome_tail);
        Ok(metadata)
    }

    fn read_object_chunk(&self, block: &BlockRow, object: Object) -> Result<Vec<u8>> {
        let locator = block.locators[object.index()];
        if locator.stored_len == 0 {
            return Ok(Vec::new());
        }
        ensure!(
            locator.decoded_len as usize <= MAX_PACKED_BYTES,
            "decoded object chunk exceeds reader cap"
        );
        let stored = read_source_exact(
            self.source.as_ref(),
            object.file_name(),
            locator.offset,
            locator.stored_len as usize,
            "standalone object chunk",
        )?;
        decode_object_chunk(&stored, locator, object)
    }
}

fn decode_object_chunk(stored: &[u8], locator: Locator, object: Object) -> Result<Vec<u8>> {
    ensure!(
        locator.decoded_len as usize <= MAX_PACKED_BYTES,
        "decoded object chunk exceeds reader cap"
    );
    ensure!(
        stored.len() == locator.stored_len as usize,
        "stored object chunk length disagrees with locator"
    );
    if locator.zstd {
        let frame_len = zstd::zstd_safe::find_frame_compressed_size(stored).map_err(|code| {
            anyhow::anyhow!(
                "standalone {} has an invalid zstd frame: {}",
                object.name(),
                zstd::zstd_safe::get_error_name(code)
            )
        })?;
        ensure!(
            frame_len == stored.len(),
            "standalone {} zstd frame has trailing data",
            object.name()
        );
        let decoded_len = locator.decoded_len as usize;
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(decoded_len)
            .with_context(|| format!("reserve decoded standalone {}", object.name()))?;
        let mut decompressor = zstd::bulk::Decompressor::new()
            .with_context(|| format!("create standalone {} zstd decoder", object.name()))?;
        decompressor
            .set_parameter(zstd::zstd_safe::DParameter::WindowLogMax(
                ZSTD_WINDOW_LOG_MAX,
            ))
            .with_context(|| format!("set standalone {} zstd window limit", object.name()))?;
        let written = decompressor
            .decompress_to_buffer(stored, &mut decoded)
            .with_context(|| format!("decompress standalone {}", object.name()))?;
        ensure!(
            written == decoded_len && decoded.len() == decoded_len,
            "zstd decoded length disagrees with locator"
        );
        Ok(decoded)
    } else {
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(stored.len())
            .with_context(|| format!("reserve raw standalone {}", object.name()))?;
        decoded.extend_from_slice(stored);
        Ok(decoded)
    }
}

fn stored_object_range(slice: &ObjectSlice, label: &str) -> Result<Range<u32>> {
    match slice {
        ObjectSlice::Stored(range) => Ok(range.clone()),
        ObjectSlice::Absent => bail!("{label} range is absent"),
        ObjectSlice::ImplicitCanonicalEmpty => {
            bail!("{label} range is an implicit canonical empty value")
        }
    }
}

fn validate_message(
    schema: CompactV2MessageSchema,
    message: &[u8],
    source_flags: u16,
    signature_count: u8,
) -> Result<()> {
    let flags = u32::from(source_flags);
    if flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        return Ok(());
    }
    let mut cursor = message;
    let mut instruction_count = 0_usize;
    let shape = decode::stream_message_accounts_with_schema(&mut cursor, schema, |event| {
        if matches!(event, decode::MessageAccountEvent::Instruction(_)) {
            ensure!(
                instruction_count < MAX_TOP_LEVEL_INSTRUCTIONS,
                "reconstructed message top-level instruction count exceeds cap"
            );
            instruction_count += 1;
        }
        Ok::<(), anyhow::Error>(())
    })
    .context("decode bounded reconstructed standalone message")?;
    ensure!(
        shape.instruction_count == instruction_count,
        "reconstructed message instruction count differs from streamed count"
    );
    ensure!(
        cursor.is_empty(),
        "reconstructed message has trailing bytes"
    );
    let is_v0 = shape.is_v0;
    ensure!(
        is_v0 == (flags & ARCHIVE_V2_TX_FLAG_MESSAGE_V0 != 0),
        "reconstructed message version disagrees with source flags"
    );
    ensure!(
        shape.num_required_signatures == signature_count,
        "reconstructed message signature count disagrees with directory"
    );
    Ok(())
}

fn standalone_source_objects() -> Vec<String> {
    std::iter::once(INDEX_FILE.to_owned())
        .chain(Object::ALL.map(|object| object.file_name().to_owned()))
        .collect()
}

fn required_source_size(source: &dyn RangeSource, object: &str, label: &str) -> Result<u64> {
    source
        .size(object)
        .with_context(|| format!("read {label} size"))?
        .with_context(|| format!("{label} is missing"))
}

fn read_source_exact(
    source: &dyn RangeSource,
    object: &str,
    offset: u64,
    length: usize,
    label: &str,
) -> Result<Vec<u8>> {
    if length == 0 {
        return Ok(Vec::new());
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .with_context(|| format!("reserve {label}"))?;
    source
        .read_range_into(object, offset, length, &mut bytes)
        .with_context(|| format!("read {label}"))?;
    ensure!(
        bytes.len() == length,
        "short read for {label}: got {}, expected {length}",
        bytes.len()
    );
    Ok(bytes)
}

fn read_source_exact_bounded(
    source: &dyn RangeSource,
    object: &str,
    offset: u64,
    length: usize,
    max_request_bytes: usize,
    label: &str,
) -> Result<Vec<u8>> {
    ensure!(max_request_bytes != 0, "bounded source read cap is zero");
    if length == 0 {
        return Ok(Vec::new());
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .with_context(|| format!("reserve bounded {label}"))?;
    while bytes.len() < length {
        let request_length = (length - bytes.len()).min(max_request_bytes);
        let request_offset = offset
            .checked_add(bytes.len() as u64)
            .context("bounded source read offset overflow")?;
        let part = read_source_exact(source, object, request_offset, request_length, label)?;
        bytes.extend_from_slice(&part);
    }
    ensure!(
        bytes.len() == length,
        "bounded source read returned a different byte count"
    );
    Ok(bytes)
}

fn slice_ref<'a>(bytes: &'a [u8], start: u32, end: u32, label: &str) -> Result<&'a [u8]> {
    ensure!(start <= end, "{label} range decreases");
    bytes
        .get(start as usize..end as usize)
        .with_context(|| format!("{label} range is outside decoded chunk"))
}

fn slice_owned(bytes: &[u8], start: u32, end: u32, label: &str) -> Result<Vec<u8>> {
    let slice = slice_ref(bytes, start, end, label)?;
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(slice.len())
        .with_context(|| format!("reserve owned {label}"))?;
    owned.extend_from_slice(slice);
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockzilla_format::{CompactMetaV1, CompactPubkey, CompactReward, wincode_leb128_config};
    use blockzilla_read_sdk::{HttpRangeSource, HttpRangeSourceOptions, SourceError};
    use std::{
        collections::HashMap,
        io::Read as _,
        net::{TcpListener, TcpStream},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };
    use tempfile::tempdir;

    fn binding() -> Binding {
        binding_with_transactions(2)
    }

    fn binding_with_transactions(selected_transactions: u64) -> Binding {
        Binding {
            epoch: 7,
            slots_per_epoch: 432_000,
            selected_blocks: 1,
            selected_transactions,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
            prefix: false,
        }
    }

    fn raw_plan() -> CompressionPlan {
        let mut plan = CompressionPlan::default_level_three();
        for object in Object::ALL {
            plan.policies[object.index()] = CodecPolicy::Raw;
        }
        plan
    }

    fn decoded_metadata() -> (Vec<u8>, DecodedMetadataParts<'static>) {
        decoded_metadata_with_rewards(Vec::new())
    }

    fn decoded_metadata_with_rewards(
        rewards: Vec<CompactReward>,
    ) -> (Vec<u8>, DecodedMetadataParts<'static>) {
        let has_rewards = !rewards.is_empty();
        let metadata = CompactMetaV1 {
            err: None,
            fee: 5_000,
            pre_balances: vec![10, 20],
            post_balances: vec![9, 16],
            inner_instructions: Some(Vec::new()),
            logs: None,
            pre_token_balances: Vec::new(),
            post_token_balances: Vec::new(),
            rewards,
            loaded_writable_addresses: vec![CompactPubkey::raw([9; 32])],
            loaded_readonly_addresses: Vec::new(),
            return_data: None,
            compute_units_consumed: Some(77),
            cost_units: None,
        };
        let bytes = wincode::config::serialize(&metadata, wincode_leb128_config()).unwrap();
        // Leak only inside this focused unit test so borrowed field ranges can
        // be handed to the worker scratch after this helper returns.
        let leaked: &'static [u8] = Box::leak(bytes.clone().into_boxed_slice());
        let mut cursor = leaked;
        let effects = decode::stream_metadata_effects_with_schema(
            &mut cursor,
            CompactV2MetadataSchema::CurrentTypedError,
            decode::MetadataDecodeLimits {
                total_message_accounts: 2,
                top_level_instruction_count: 1,
            },
            |_event| Ok::<(), anyhow::Error>(()),
        )
        .unwrap();
        assert!(cursor.is_empty());
        let fields = effects.fields;
        (
            bytes,
            DecodedMetadataParts {
                outcome_head: fields.outcome_head,
                pre_balances: fields.pre_balances,
                post_balances: fields.post_balances,
                inner_instructions: fields.inner_instructions,
                logs: fields.logs,
                pre_token_balances: fields.pre_token_balances,
                post_token_balances: fields.post_token_balances,
                transaction_rewards: fields.transaction_rewards,
                loaded_writable: fields.loaded_writable,
                loaded_readonly: fields.loaded_readonly,
                outcome_tail: fields.outcome_tail,
                effect_state: 0b0011_1010 | (u8::from(has_rewards) << 7),
            },
        )
    }

    #[test]
    fn strict_headers_and_block_rows_round_trip() {
        let mut plan = CompressionPlan::default_level_three();
        assert_eq!(plan.zstd_level, 3);
        assert_eq!(plan.policy(Object::InnerInstructions), CodecPolicy::Zstd);
        assert_eq!(plan.policy(Object::Logs), CodecPolicy::Zstd);
        assert_eq!(plan.policy(Object::BlockRewards), CodecPolicy::Raw);
        plan.apply_override("logs=raw").unwrap();
        assert_eq!(plan.policy(Object::Logs), CodecPolicy::Raw);
        assert!(plan.apply_override("unknown=raw").is_err());
        let header = FileHeader::for_object(binding(), plan, Object::Messages);
        assert_eq!(FileHeader::decode(&header.encode()).unwrap(), header);
        let mut corrupt = header.encode();
        corrupt[63] = 1;
        assert!(FileHeader::decode(&corrupt).is_err());

        let row = BlockRow {
            block_id: 0,
            tx_count: 1,
            slot: 1,
            parent_slot: 0,
            first_tx_ordinal: 0,
            first_signature_ordinal: 0,
            signature_count: 1,
            blockhash_id: 2,
            previous_blockhash_id: 1,
            block_time: Some(-9),
            block_height: None,
            locators: [Locator {
                offset: FILE_HEADER_LEN as u64,
                stored_len: 3,
                decoded_len: 3,
                zstd: false,
            }; OBJECT_COUNT],
        };
        assert_eq!(BlockRow::decode(&row.encode().unwrap()).unwrap(), row);
        let mut corrupt = row.encode().unwrap();
        corrupt[52] = 0x80;
        assert!(BlockRow::decode(&corrupt).is_err());
    }

    #[test]
    fn reader_rejects_huge_header_row_counts_before_index_allocation() {
        let directory = tempdir().unwrap();
        let plan = raw_plan();
        for (name, slots_per_epoch, selected_blocks, expected) in [
            (
                "above-slots",
                10,
                11,
                "standalone block count exceeds slots per epoch",
            ),
            (
                "above-u32",
                u64::from(u32::MAX) + 1,
                u64::from(u32::MAX) + 1,
                "standalone block count exceeds u32",
            ),
        ] {
            let root = directory.path().join(name);
            std::fs::create_dir(&root).unwrap();
            let mut header = FileHeader::for_index_format(binding(), plan, StandaloneFormat::V3);
            header.slots_per_epoch = slots_per_epoch;
            header.selected_blocks = selected_blocks;
            std::fs::write(root.join(INDEX_FILE), header.encode()).unwrap();

            let error = Reader::open(&root).err().expect("huge row count must fail");
            assert!(error.to_string().contains(expected), "{error:#}");
        }
    }

    #[test]
    fn remote_reader_rejects_exact_huge_index_before_row_body_read() {
        struct HeaderOnlySource {
            header: [u8; FILE_HEADER_LEN],
            index_size: u64,
            body_reads: Arc<AtomicUsize>,
        }

        impl RangeSource for HeaderOnlySource {
            fn size(&self, object: &str) -> blockzilla_read_sdk::SourceResult<Option<u64>> {
                Ok((object == INDEX_FILE).then_some(self.index_size))
            }

            fn read_range(
                &self,
                object: &str,
                offset: u64,
                length: usize,
            ) -> blockzilla_read_sdk::SourceResult<Vec<u8>> {
                if object == INDEX_FILE && offset == 0 && length == FILE_HEADER_LEN {
                    return Ok(self.header.to_vec());
                }
                self.body_reads.fetch_add(1, Ordering::Relaxed);
                Err(SourceError::Protocol(
                    "test source rejects index body reads".into(),
                ))
            }
        }

        let row_count = MAX_RETAINED_INDEX_ROW_BYTES / mem::size_of::<BlockRow>() + 1;
        let mut header = FileHeader::for_index_format(binding(), raw_plan(), StandaloneFormat::V3);
        header.selected_blocks = row_count as u64;
        header.slots_per_epoch = row_count as u64;
        let index_size = (FILE_HEADER_LEN as u64)
            .checked_add(
                (row_count as u64)
                    .checked_mul(INDEX_ROW_LEN as u64)
                    .unwrap(),
            )
            .unwrap();
        let body_reads = Arc::new(AtomicUsize::new(0));
        let source = HeaderOnlySource {
            header: header.encode(),
            index_size,
            body_reads: body_reads.clone(),
        };
        let error = Reader::open_with_source(Arc::new(source), "huge-exact-index")
            .err()
            .expect("retained row cap must reject the exact-size index");
        assert!(
            error
                .to_string()
                .contains("standalone retained index rows exceed"),
            "{error:#}"
        );
        assert_eq!(body_reads.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn production_v3_reader_rejects_valid_fixed56_directory() {
        let root = tempdir().unwrap();
        let plan = raw_plan();
        let binding = binding_with_transactions(1);
        let mut writers = Writers::create_v3(root.path(), binding, plan).unwrap();
        let mut scratch = WorkerScratch::default();
        scratch.begin_block_v3();
        let raw_message = [0x80, 0x01, 0x02];
        scratch
            .begin_transaction(ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, 1, &raw_message)
            .unwrap();
        scratch.record_missing_metadata().unwrap();
        scratch.record_block_rewards(&[0]).unwrap();
        scratch.finish_block(1).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        let encoded = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
        writers
            .append(
                ArchiveV2HotBlockIndexRow {
                    block_id: 0,
                    slot: 10,
                    compressed_offset: 0,
                    compressed_len: 1,
                    uncompressed_len: 1,
                    tx_count: 1,
                    first_tx_ordinal: 0,
                    first_signature_ordinal: 0,
                    signature_count: 1,
                },
                SourceBlockCore {
                    parent_slot: 9,
                    blockhash_id: 1,
                    previous_blockhash_id: 0,
                    block_time: None,
                    block_height: None,
                },
                encoded,
            )
            .unwrap();
        writers.finish(1, 1).unwrap();

        let fixed = directory_v3::encode_with_stride_fixed56_for_test(
            &[TransactionLayout {
                source_flags: ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK as u16,
                effect_state: 0,
                signature_count: 1,
                dense_lengths: [raw_message.len() as u64, 0, 0, 0, 0, 0, 0],
                reward: TransactionReward::Absent,
                raw_metadata_fallback_len: None,
            }],
            32,
        )
        .unwrap();
        assert_eq!(
            fixed.measurement.selected_checkpoint_codec,
            CheckpointCodec::Fixed56
        );
        assert!(DecodedDirectory::decode(&fixed.bytes).is_ok());

        let directory_path = root.path().join(Object::TransactionDirectory.file_name());
        let mut directory_file = std::fs::read(&directory_path).unwrap();
        directory_file.truncate(FILE_HEADER_LEN);
        directory_file.extend_from_slice(&fixed.bytes);
        std::fs::write(&directory_path, directory_file).unwrap();

        let index_path = root.path().join(INDEX_FILE);
        let mut index = std::fs::read(&index_path).unwrap();
        let mut row = BlockRow::decode(&index[FILE_HEADER_LEN..]).unwrap();
        row.locators[Object::TransactionDirectory.index()] = Locator {
            offset: FILE_HEADER_LEN as u64,
            stored_len: fixed.bytes.len() as u32,
            decoded_len: fixed.bytes.len() as u32,
            zstd: false,
        };
        index[FILE_HEADER_LEN..].copy_from_slice(&row.encode().unwrap());
        std::fs::write(index_path, index).unwrap();

        let reader = Reader::open(root.path()).unwrap();
        let error = reader
            .visit_semantic_transactions(0, None, |_| Ok(()))
            .expect_err("production reader must reject fixed56 checkpoints");
        assert!(
            error.to_string().contains("requires varint-delta"),
            "{error:#}"
        );
    }

    #[test]
    fn writer_reader_and_read_only_measurement_chunks_are_exact() {
        let directory = tempdir().unwrap();
        let plan = raw_plan();
        let mut scratch = WorkerScratch::default();
        scratch.begin_block();
        let message0 = [1, 2, 3, 4, 5];
        let message1 = [9, 8, 7];
        let (metadata0, parts) = decoded_metadata();
        let raw_metadata1 = [0xaa, 0xbb, 0xcc];
        scratch
            .begin_transaction(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                2,
                &message0,
            )
            .unwrap();
        scratch.record_decoded_metadata(parts).unwrap();
        scratch
            .begin_transaction(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK
                    | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                1,
                &message1,
            )
            .unwrap();
        scratch.record_raw_metadata(&raw_metadata1).unwrap();
        scratch.record_block_rewards(&[0]).unwrap();
        scratch.finish_block(2).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        let block = encode_block(&mut scratch, &mut compressor, plan).unwrap();
        let source_row = ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: 10,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count: 2,
            first_tx_ordinal: 0,
            first_signature_ordinal: 5,
            signature_count: 3,
        };
        let mut writers = Writers::create(directory.path(), binding(), plan).unwrap();
        writers
            .append(
                source_row,
                SourceBlockCore {
                    parent_slot: 9,
                    blockhash_id: 100,
                    previous_blockhash_id: 99,
                    block_time: Some(123),
                    block_height: Some(456),
                },
                block,
            )
            .unwrap();
        writers.finish(1, 2).unwrap();

        let mut reader = Reader::open(directory.path()).unwrap();
        let snapshot =
            || {
                let mut files = vec![std::fs::read(directory.path().join(INDEX_FILE)).unwrap()];
                files.extend(Object::ALL.iter().map(|object| {
                    std::fs::read(directory.path().join(object.file_name())).unwrap()
                }));
                files
            };
        let before = snapshot();
        let measurement = reader.read_directory_measurement_block(0).unwrap();
        assert_eq!(measurement.block_id, 0);
        assert_eq!(measurement.tx_count, 2);
        assert_eq!(measurement.first_signature_ordinal, 5);
        assert_eq!(measurement.signature_count, 3);
        assert_eq!(measurement.directory.len(), 2 * DIRECTORY_ROW_LEN);
        assert_eq!(
            measurement.transaction_rewards.len(),
            measurement.final_object_decoded_lengths[7] as usize
        );
        assert_eq!(measurement.raw_metadata_fallbacks, raw_metadata1.to_vec());
        assert_eq!(
            snapshot(),
            before,
            "measurement API changed candidate bytes"
        );
        let first = reader.read_transaction(0, 0).unwrap();
        assert_eq!(first.message, message0);
        assert_eq!(first.metadata, MetadataBytes::Decoded(metadata0));
        assert_eq!(first.signature_ordinals, 5..7);
        assert_eq!(first.signature_bytes, 320..448);
        let second = reader.read_transaction(0, 1).unwrap();
        assert_eq!(second.message, message1);
        assert_eq!(
            second.metadata,
            MetadataBytes::RawFallback(raw_metadata1.to_vec())
        );
        assert_eq!(second.signature_ordinals, 7..8);
        assert_eq!(second.signature_bytes, 448..512);
    }

    fn write_semantic_candidate(
        root: &Path,
        format: StandaloneFormat,
    ) -> (OutputSummary, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        let plan = raw_plan();
        let mut scratch = WorkerScratch::default();
        if format == StandaloneFormat::V3 {
            scratch.begin_block_v3();
        } else {
            scratch.begin_block();
        }
        let message0 = vec![1, 2, 3, 4, 5];
        let message1 = vec![6, 7, 8, 9];
        let message2 = vec![10, 11, 12];
        let (metadata0, parts0) = decoded_metadata();
        let (metadata1, parts1) = decoded_metadata_with_rewards(vec![CompactReward {
            pubkey: CompactPubkey::raw([0x44; 32]),
            lamports: -7,
            post_balance: 91,
            reward_type: 2,
            commission: Some(3),
        }]);
        let semantic_reward_bytes = parts1.transaction_rewards.to_vec();
        let raw_metadata = vec![0xaa, 0xbb, 0xcc, 0xdd];
        scratch
            .begin_transaction(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                2,
                &message0,
            )
            .unwrap();
        scratch.record_decoded_metadata(parts0).unwrap();
        scratch
            .begin_transaction(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                1,
                &message1,
            )
            .unwrap();
        scratch.record_decoded_metadata(parts1).unwrap();
        scratch
            .begin_transaction(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA
                    | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK
                    | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                1,
                &message2,
            )
            .unwrap();
        scratch.record_raw_metadata(&raw_metadata).unwrap();
        scratch.record_block_rewards(&[0]).unwrap();
        scratch.finish_block(3).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        let block = if format == StandaloneFormat::V3 {
            encode_block_v3(&mut scratch, &mut compressor, plan).unwrap()
        } else {
            encode_block(&mut scratch, &mut compressor, plan).unwrap()
        };
        let source_row = ArchiveV2HotBlockIndexRow {
            block_id: 0,
            slot: 10,
            compressed_offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            tx_count: 3,
            first_tx_ordinal: 0,
            first_signature_ordinal: 5,
            signature_count: 4,
        };
        let binding = binding_with_transactions(3);
        let mut writers = if format == StandaloneFormat::V3 {
            Writers::create_v3(root, binding, plan).unwrap()
        } else {
            Writers::create(root, binding, plan).unwrap()
        };
        writers
            .append(
                source_row,
                SourceBlockCore {
                    parent_slot: 9,
                    blockhash_id: 100,
                    previous_blockhash_id: 99,
                    block_time: Some(123),
                    block_height: Some(456),
                },
                block,
            )
            .unwrap();
        let summary = writers.finish(1, 3).unwrap();
        (
            summary,
            metadata0,
            metadata1,
            semantic_reward_bytes,
            raw_metadata,
        )
    }

    #[test]
    fn direct_v3_matches_v2_semantics_and_binds_new_headers() {
        let v2_root = tempdir().unwrap();
        let v3_root = tempdir().unwrap();
        let (v2_summary, metadata0, metadata1, semantic_rewards, raw_metadata) =
            write_semantic_candidate(v2_root.path(), StandaloneFormat::V2);
        let (v3_summary, _, _, _, _) =
            write_semantic_candidate(v3_root.path(), StandaloneFormat::V3);
        assert_eq!(v2_summary.format, StandaloneFormat::V2.name());
        assert_eq!(v3_summary.format, StandaloneFormat::V3.name());
        assert_eq!(v2_summary.output_reopens, 0);
        assert_eq!(v3_summary.output_reopens, 0);
        assert_eq!(v3_summary.stats.directory_v3.blocks, 1);
        assert_eq!(v3_summary.stats.directory_v3.source_projection_passes, 1);
        assert_eq!(
            v3_summary.stats.directory_v3.canonical_reward_fields_elided,
            1
        );
        assert_eq!(
            v3_summary.stats.directory_v3.canonical_reward_bytes_elided,
            1
        );
        assert_eq!(v3_summary.stats.directory_v3.stored_reward_records, 1);
        assert_eq!(v3_summary.stats.directory_v3.raw_fallback_records, 1);
        assert_eq!(
            v3_summary.stats.directory_v3.varint_delta_checkpoint_blocks,
            1
        );
        assert_eq!(
            v3_summary.stats.directory_v3.stride_32_blocks
                + v3_summary.stats.directory_v3.stride_64_blocks
                + v3_summary.stats.directory_v3.stride_128_blocks,
            1
        );
        for summary in [&v2_summary, &v3_summary] {
            for object in &summary.objects {
                assert_eq!(
                    object.file_bytes,
                    object.header_bytes + object.stored_payload_bytes
                );
                assert_eq!(object.decoded_payload_bytes, object.stored_payload_bytes);
            }
        }

        let mut v2 = Reader::open(v2_root.path()).unwrap();
        let mut v3 = Reader::open(v3_root.path()).unwrap();
        assert_eq!(v2.header.format, StandaloneFormat::V2);
        assert_eq!(v3.header.format, StandaloneFormat::V3);
        for tx_index in 0..3 {
            let before = v2.read_transaction(0, tx_index).unwrap();
            let after = v3.read_transaction(0, tx_index).unwrap();
            assert_eq!(after.block_id, before.block_id);
            assert_eq!(after.slot, before.slot);
            assert_eq!(after.tx_index, before.tx_index);
            assert_eq!(after.source_flags, before.source_flags);
            assert_eq!(after.effect_state, before.effect_state);
            assert_eq!(after.message, before.message);
            assert_eq!(after.metadata, before.metadata);
            assert_eq!(after.signature_ordinals, before.signature_ordinals);
            assert_eq!(after.signature_bytes, before.signature_bytes);
            assert_eq!(after.directory_validation_records_scanned, 3);
            assert_eq!(after.reward_validation_records_scanned, 1);
            assert_eq!(after.raw_fallback_validation_records_scanned, 1);
            assert!(after.directory_lookup_records_scanned <= 32);
        }
        assert_eq!(
            v3.read_transaction(0, 0).unwrap().metadata,
            MetadataBytes::Decoded(metadata0)
        );
        assert_eq!(
            v3.read_transaction(0, 1).unwrap().metadata,
            MetadataBytes::Decoded(metadata1)
        );
        assert_eq!(
            v3.read_transaction(0, 2).unwrap().metadata,
            MetadataBytes::RawFallback(raw_metadata.clone())
        );

        let v2_row = v2.block(0).unwrap().clone();
        let v3_row = v3.block(0).unwrap().clone();
        for object in Object::ALL {
            if matches!(
                object,
                Object::TransactionDirectory | Object::TransactionRewards
            ) {
                continue;
            }
            assert_eq!(
                v2.read_object_chunk(&v2_row, object).unwrap(),
                v3.read_object_chunk(&v3_row, object).unwrap(),
                "{} plane differs",
                object.name()
            );
        }
        let v2_rewards = v2
            .read_object_chunk(&v2_row, Object::TransactionRewards)
            .unwrap();
        let v3_rewards = v3
            .read_object_chunk(&v3_row, Object::TransactionRewards)
            .unwrap();
        assert_eq!(
            v2_rewards,
            [&[0_u8][..], semantic_rewards.as_slice()].concat()
        );
        assert_eq!(v3_rewards, semantic_rewards);
        let v3_directory = v3
            .read_object_chunk(&v3_row, Object::TransactionDirectory)
            .unwrap();
        let decoded = DecodedDirectory::decode(&v3_directory).unwrap();
        assert_eq!(
            decoded.header.checkpoint_codec,
            CheckpointCodec::VarintDelta
        );

        let message_path = v3_root.path().join(Object::Messages.file_name());
        let mut corrupt = std::fs::read(&message_path).unwrap();
        corrupt[55] = 0;
        std::fs::write(&message_path, corrupt).unwrap();
        assert!(Reader::open(v3_root.path()).is_err());
    }

    #[test]
    fn v3_semantic_block_visit_reads_each_selected_plane_once() {
        let root = tempdir().unwrap();
        let (_, metadata0, metadata1, _, raw_metadata) =
            write_semantic_candidate(root.path(), StandaloneFormat::V3);
        let reader = Reader::open(root.path()).unwrap();
        assert_eq!(reader.message_schema(), CompactV2MessageSchema::Current);
        assert_eq!(
            reader.metadata_schema(),
            CompactV2MetadataSchema::CurrentTypedError
        );
        assert_eq!(
            reader.open_read_stats().stored_bytes,
            FILE_HEADER_LEN as u64 + INDEX_ROW_LEN as u64 + (OBJECT_COUNT * FILE_HEADER_LEN) as u64
        );

        #[derive(Debug, PartialEq, Eq)]
        struct Seen {
            tx_index: u32,
            message: Vec<u8>,
            loaded: Option<Vec<u8>>,
            inner: Option<Vec<u8>>,
            token: Option<Vec<u8>>,
            outcome: Option<Vec<u8>>,
            raw: Option<Vec<u8>>,
        }
        let mut seen = Vec::new();
        let stats = reader
            .visit_semantic_transactions(0, None, |transaction| {
                seen.push(Seen {
                    tx_index: transaction.tx_index,
                    message: transaction.message.to_vec(),
                    loaded: transaction.loaded_addresses.map(<[u8]>::to_vec),
                    inner: transaction.inner_instructions.map(<[u8]>::to_vec),
                    token: transaction.token_balances.map(<[u8]>::to_vec),
                    outcome: transaction.outcome.map(<[u8]>::to_vec),
                    raw: transaction.raw_metadata.map(<[u8]>::to_vec),
                });
                Ok(())
            })
            .unwrap();
        assert_eq!(stats.block_transactions, 3);
        assert_eq!(stats.requested_transactions, 3);
        assert_eq!(stats.visited_transactions, 3);
        assert_eq!(seen.len(), 3);
        assert_eq!(seen[2].raw.as_deref(), Some(raw_metadata.as_slice()));
        assert!(seen[2].loaded.is_none());
        for object in [
            Object::TransactionDirectory,
            Object::Messages,
            Object::LoadedAddresses,
            Object::InnerInstructions,
            Object::TokenBalances,
            Object::Outcomes,
            Object::RawMetadataFallbacks,
        ] {
            let expected =
                u64::from(reader.block(0).unwrap().locators[object.index()].stored_len != 0);
            assert_eq!(stats.object_reads[object.index()].read_calls, expected);
        }
        for object in [
            Object::Logs,
            Object::Balances,
            Object::TransactionRewards,
            Object::BlockRewards,
        ] {
            assert_eq!(stats.object_reads[object.index()].read_calls, 0);
        }

        for (seen, metadata) in [
            (&seen[0], metadata0.as_slice()),
            (&seen[1], metadata1.as_slice()),
        ] {
            let mut cursor = metadata;
            let effects = decode::stream_metadata_effects_structural_with_schema(
                &mut cursor,
                CompactV2MetadataSchema::CurrentTypedError,
                |_event| Ok::<(), anyhow::Error>(()),
            )
            .unwrap();
            assert!(cursor.is_empty());
            assert_eq!(
                seen.loaded.as_deref(),
                Some(
                    [
                        effects.fields.loaded_writable,
                        effects.fields.loaded_readonly,
                    ]
                    .concat()
                    .as_slice()
                )
            );
            assert_eq!(
                seen.inner.as_deref(),
                Some(effects.fields.inner_instructions)
            );
            assert_eq!(
                seen.token.as_deref(),
                Some(
                    [
                        effects.fields.pre_token_balances,
                        effects.fields.post_token_balances,
                    ]
                    .concat()
                    .as_slice()
                )
            );
            assert_eq!(
                seen.outcome.as_deref(),
                Some(
                    [effects.fields.outcome_head, effects.fields.outcome_tail]
                        .concat()
                        .as_slice()
                )
            );
        }

        let mut selected = Vec::new();
        let selected_stats = reader
            .visit_semantic_transactions(0, Some(&[1]), |transaction| {
                selected.push(transaction.tx_index);
                Ok(())
            })
            .unwrap();
        assert_eq!(selected, [1]);
        assert_eq!(selected_stats.visited_transactions, 1);
        assert!(
            reader
                .visit_semantic_transactions(0, Some(&[1, 1]), |_| Ok(()))
                .is_err()
        );
    }

    fn write_http_semantic_candidate(root: &Path, block_count: usize) {
        let plan = raw_plan();
        let binding = Binding {
            epoch: 7,
            slots_per_epoch: 432_000,
            selected_blocks: block_count as u64,
            selected_transactions: block_count as u64,
            message_schema: CompactV2MessageSchema::Current,
            metadata_schema: CompactV2MetadataSchema::CurrentTypedError,
            prefix: false,
        };
        let mut writers = Writers::create_v3(root, binding, plan).unwrap();
        let mut scratch = WorkerScratch::default();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        for block_ordinal in 0..block_count {
            scratch.begin_block_v3();
            scratch
                .begin_transaction(
                    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                    1,
                    &[0x80, block_ordinal as u8, 0x7f],
                )
                .unwrap();
            scratch.record_missing_metadata().unwrap();
            scratch.record_block_rewards(&[0]).unwrap();
            scratch.finish_block(1).unwrap();
            let encoded = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
            writers
                .append(
                    ArchiveV2HotBlockIndexRow {
                        block_id: block_ordinal as u32,
                        slot: 10 + block_ordinal as u64,
                        compressed_offset: block_ordinal as u64,
                        compressed_len: 1,
                        uncompressed_len: 1,
                        tx_count: 1,
                        first_tx_ordinal: block_ordinal as u64,
                        first_signature_ordinal: block_ordinal as u64,
                        signature_count: 1,
                    },
                    SourceBlockCore {
                        parent_slot: 9 + block_ordinal as u64,
                        blockhash_id: 100 + block_ordinal as u32,
                        previous_blockhash_id: 99 + block_ordinal as u32,
                        block_time: Some(123 + block_ordinal as i64),
                        block_height: Some(456 + block_ordinal as u64),
                    },
                    encoded,
                )
                .unwrap();
        }
        writers
            .finish(block_count as u64, block_count as u64)
            .unwrap();
    }

    struct TestRangeServer {
        base_url: String,
        stop: Arc<AtomicBool>,
        max_get_bytes: Arc<AtomicU64>,
        task: Option<thread::JoinHandle<()>>,
    }

    impl TestRangeServer {
        fn start(root: &Path) -> Self {
            let objects = std::iter::once(INDEX_FILE)
                .chain(Object::ALL.map(Object::file_name))
                .map(|name| (name.to_owned(), std::fs::read(root.join(name)).unwrap()))
                .collect::<HashMap<_, _>>();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let max_get_bytes = Arc::new(AtomicU64::new(0));
            let task_stop = stop.clone();
            let task_max_get_bytes = max_get_bytes.clone();
            let task = thread::spawn(move || {
                while !task_stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            stream.set_nonblocking(false).unwrap();
                            serve_range_request(stream, &objects, &task_max_get_bytes)
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("test range server accept failed: {error}"),
                    }
                }
            });
            Self {
                base_url: format!("http://{address}/indexer-v3"),
                stop,
                max_get_bytes,
                task: Some(task),
            }
        }

        fn source(&self) -> HttpRangeSource {
            HttpRangeSource::with_options(
                &self.base_url,
                7,
                None,
                HttpRangeSourceOptions {
                    allow_insecure_http: true,
                    ..HttpRangeSourceOptions::default()
                },
            )
            .unwrap()
        }

        fn reset_max_get_bytes(&self) {
            self.max_get_bytes.store(0, Ordering::Relaxed);
        }

        fn finish(mut self) {
            self.stop.store(true, Ordering::Relaxed);
            self.task.take().unwrap().join().unwrap();
        }
    }

    fn serve_range_request(
        mut stream: TcpStream,
        objects: &HashMap<String, Vec<u8>>,
        max_get_bytes: &AtomicU64,
    ) {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).unwrap();
            assert_ne!(read, 0, "HTTP client closed before request headers ended");
            request.extend_from_slice(&buffer[..read]);
            assert!(request.len() <= 64 << 10, "HTTP request headers exceed cap");
        }
        let request = String::from_utf8(request).unwrap();
        let request_line = request.lines().next().unwrap();
        let mut words = request_line.split_whitespace();
        let method = words.next().unwrap();
        let path = words.next().unwrap();
        let object = path.rsplit('/').next().unwrap();
        let bytes = objects
            .get(object)
            .unwrap_or_else(|| panic!("unknown object {object}"));
        if method == "HEAD" {
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                bytes.len()
            )
            .unwrap();
            return;
        }
        assert_eq!(method, "GET");
        let range = request
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("range: bytes=")
                    .map(str::to_owned)
            })
            .expect("range GET omitted Range header");
        let (start, end) = range.split_once('-').unwrap();
        let start = start.parse::<usize>().unwrap();
        let end = end.parse::<usize>().unwrap();
        assert!(start <= end && end < bytes.len());
        let body = &bytes[start..=end];
        max_get_bytes.fetch_max(body.len() as u64, Ordering::Relaxed);
        write!(
            stream,
            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{}\r\nConnection: close\r\n\r\n",
            body.len(),
            bytes.len()
        )
        .unwrap();
        stream.write_all(body).unwrap();
    }

    #[derive(Debug, PartialEq, Eq)]
    struct OwnedSemanticTransaction {
        block_id: u32,
        slot: u64,
        tx_index: u32,
        source_flags: u16,
        effect_state: u8,
        message: Vec<u8>,
        loaded_addresses: Option<Vec<u8>>,
        inner_instructions: Option<Vec<u8>>,
        token_balances: Option<Vec<u8>>,
        outcome: Option<Vec<u8>>,
        raw_metadata: Option<Vec<u8>>,
        signature_ordinals: Range<u64>,
        signature_bytes: Range<u64>,
    }

    impl From<SemanticTransaction<'_>> for OwnedSemanticTransaction {
        fn from(transaction: SemanticTransaction<'_>) -> Self {
            Self {
                block_id: transaction.block_id,
                slot: transaction.slot,
                tx_index: transaction.tx_index,
                source_flags: transaction.source_flags,
                effect_state: transaction.effect_state,
                message: transaction.message.to_vec(),
                loaded_addresses: transaction.loaded_addresses.map(<[u8]>::to_vec),
                inner_instructions: transaction.inner_instructions.map(<[u8]>::to_vec),
                token_balances: transaction.token_balances.map(<[u8]>::to_vec),
                outcome: transaction.outcome.map(<[u8]>::to_vec),
                raw_metadata: transaction.raw_metadata.map(<[u8]>::to_vec),
                signature_ordinals: transaction.signature_ordinals,
                signature_bytes: transaction.signature_bytes,
            }
        }
    }

    #[test]
    fn http_contiguous_semantic_scan_coalesces_requests_without_byte_or_receipt_changes() {
        const BLOCKS: usize = 8;
        let root = tempdir().unwrap();
        write_http_semantic_candidate(root.path(), BLOCKS);
        let server = TestRangeServer::start(root.path());

        let point_http = server.source();
        let point_reader = Reader::open_with_source(
            Arc::new(point_http.clone()),
            PathBuf::from("point-http-test"),
        )
        .unwrap();
        server.reset_max_get_bytes();
        let point_before = point_http.stats();
        let mut point_transactions: Vec<OwnedSemanticTransaction> = Vec::new();
        let mut point_receipts = Vec::new();
        for block_ordinal in 0..BLOCKS {
            point_receipts.push(
                point_reader
                    .visit_semantic_transactions(block_ordinal, None, |transaction| {
                        point_transactions.push(transaction.into());
                        Ok(())
                    })
                    .unwrap(),
            );
        }
        let point_io = point_http.stats().saturating_sub(point_before);
        assert_eq!(point_io.head_requests, 0);
        assert_eq!(point_io.get_requests, (BLOCKS * 2) as u64);
        assert!(
            server.max_get_bytes.load(Ordering::Relaxed) <= MAX_REMOTE_SEMANTIC_RANGE_BYTES as u64
        );

        let batch_http = server.source();
        let batch_reader = Reader::open_with_source(
            Arc::new(batch_http.clone()),
            PathBuf::from("batch-http-test"),
        )
        .unwrap();
        server.reset_max_get_bytes();
        let batch_before = batch_http.stats();
        let mut batch_transactions: Vec<OwnedSemanticTransaction> = Vec::new();
        let mut batch_receipts = Vec::new();
        let mut scan = batch_reader
            .begin_contiguous_semantic_scan(0..BLOCKS)
            .unwrap();
        for block_ordinal in 0..BLOCKS {
            batch_receipts.push(
                scan.visit_semantic_transactions(block_ordinal, None, |transaction| {
                    batch_transactions.push(transaction.into());
                    Ok(())
                })
                .unwrap(),
            );
        }
        scan.finish().unwrap();
        let batch_io = batch_http.stats().saturating_sub(batch_before);

        assert_eq!(batch_transactions, point_transactions);
        assert_eq!(batch_receipts, point_receipts);
        assert_eq!(batch_io.head_requests, 0);
        assert_eq!(batch_io.get_requests, 2);
        assert!(batch_io.get_requests < point_io.get_requests);
        assert_eq!(batch_io.returned_body_bytes, point_io.returned_body_bytes);
        assert_eq!(
            batch_io.returned_body_bytes,
            batch_receipts
                .iter()
                .map(SemanticBlockReadStats::total_stored_bytes)
                .sum::<u64>()
        );
        assert!(
            server.max_get_bytes.load(Ordering::Relaxed) <= MAX_REMOTE_SEMANTIC_RANGE_BYTES as u64
        );
        server.finish();
    }

    fn deterministic_v3_projection(marker: u8) -> Vec<u8> {
        let plan = raw_plan();
        let mut scratch = WorkerScratch::default();
        scratch.begin_block_v3();
        for tx_index in 0..65_u8 {
            scratch
                .begin_transaction(
                    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                    tx_index % 3,
                    &[marker, tx_index],
                )
                .unwrap();
            scratch.record_missing_metadata().unwrap();
        }
        scratch.record_block_rewards(&[0]).unwrap();
        scratch.finish_block(65).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        encode_block_v3(&mut scratch, &mut compressor, plan)
            .unwrap()
            .packed
    }

    fn deterministic_v3_batch(workers: usize) -> Vec<Vec<u8>> {
        let mut indexed = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for worker in 0..workers {
                handles.push(scope.spawn(move || {
                    (worker..24_usize)
                        .step_by(workers)
                        .map(|index| (index, deterministic_v3_projection(index as u8)))
                        .collect::<Vec<_>>()
                }));
            }
            handles
                .into_iter()
                .flat_map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        indexed.sort_by_key(|(index, _)| *index);
        indexed.into_iter().map(|(_, bytes)| bytes).collect()
    }

    #[test]
    fn direct_v3_is_byte_deterministic_with_one_and_twelve_workers() {
        assert_eq!(deterministic_v3_batch(1), deterministic_v3_batch(12));
    }

    #[test]
    fn direct_v3_empty_and_large_blocks_stay_bounded() {
        let plan = raw_plan();
        let mut empty = WorkerScratch::default();
        empty.begin_block_v3();
        empty.record_block_rewards(&[0]).unwrap();
        empty.finish_block(0).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        let empty = encode_block_v3(&mut empty, &mut compressor, plan).unwrap();
        let directory_range = empty.ranges[Object::TransactionDirectory.index()].clone();
        let decoded = DecodedDirectory::decode(&empty.packed[directory_range]).unwrap();
        assert_eq!(decoded.header.tx_count, 0);
        assert!(decoded.lookup(0, 0).is_err());
        assert_eq!(empty.stats.directory_v3.source_projection_passes, 1);

        let mut large = WorkerScratch::default();
        large.begin_block_v3();
        for tx_index in 0..4_097_u32 {
            large
                .begin_transaction(
                    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                    0,
                    &[0x80, tx_index as u8],
                )
                .unwrap();
            large.record_missing_metadata().unwrap();
        }
        large.record_block_rewards(&[0]).unwrap();
        large.finish_block(4_097).unwrap();
        let large = encode_block_v3(&mut large, &mut compressor, plan).unwrap();
        assert!(large.stats.max_live_scratch_bytes <= MAX_SCRATCH_BYTES);
        assert!(large.stats.max_scratch_capacity <= MAX_SCRATCH_BYTES);
        let directory_range = large.ranges[Object::TransactionDirectory.index()].clone();
        let decoded = DecodedDirectory::decode(&large.packed[directory_range]).unwrap();
        assert_eq!(decoded.header.tx_count, 4_097);
        assert_eq!(
            decoded.header.checkpoint_codec,
            CheckpointCodec::VarintDelta
        );
        let last = decoded.lookup(4_096, 0).unwrap();
        assert_eq!(last.tx_index, 4_096);
        assert!(last.dense_records_scanned <= 128);
        assert_eq!(large.stats.directory_v3.source_projection_passes, 1);
    }

    #[test]
    fn direct_v3_preserves_noncanonical_empty_reward_bytes() {
        let plan = raw_plan();
        let mut scratch = WorkerScratch::default();
        scratch.begin_block_v3();
        let (_, canonical_parts) = decoded_metadata();
        let noncanonical: &'static [u8] = Box::leak(vec![0x80, 0].into_boxed_slice());
        let parts = DecodedMetadataParts {
            transaction_rewards: noncanonical,
            ..canonical_parts
        };
        scratch
            .begin_transaction(
                ARCHIVE_V2_TX_FLAG_HAS_METADATA | ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
                1,
                &[1, 2, 3],
            )
            .unwrap();
        scratch.record_decoded_metadata(parts).unwrap();
        scratch.record_block_rewards(&[0]).unwrap();
        scratch.finish_block(1).unwrap();
        let mut compressor = zstd::bulk::Compressor::new(3).unwrap();
        let block = encode_block_v3(&mut scratch, &mut compressor, plan).unwrap();
        let reward_range = block.ranges[Object::TransactionRewards.index()].clone();
        assert_eq!(&block.packed[reward_range], noncanonical);
        let directory_range = block.ranges[Object::TransactionDirectory.index()].clone();
        let directory = DecodedDirectory::decode(&block.packed[directory_range]).unwrap();
        assert_eq!(
            directory.lookup(0, 0).unwrap().reward,
            RewardSlice::NoncanonicalEmptyStored(0..2)
        );
        assert_eq!(block.stats.directory_v3.canonical_reward_fields_elided, 0);
        assert_eq!(block.stats.directory_v3.stored_reward_records, 1);
    }

    #[test]
    fn candidate_binding_is_explicitly_unverified_sorted_and_digest_free() {
        let binding = CandidateBinding {
            schema_version: 1,
            status: "unverified-nonpublishable".into(),
            epoch: 7,
            slots_per_epoch: 432_000,
            selected_blocks: 1,
            selected_transactions: 2,
            complete_epoch: true,
            outer_schema: "current".into(),
            message_schema: "current".into(),
            metadata_schema: "current-typed-error".into(),
            source_generation_digest: None,
            objects: vec![
                RetainedObject {
                    logical_name: "registry.bin".into(),
                    role: "pubkey-registry".into(),
                    admitted_source_size: 32,
                },
                RetainedObject {
                    logical_name: "signatures.bin".into(),
                    role: "transaction-signatures".into(),
                    admitted_source_size: 64,
                },
            ],
        };
        let bytes = binding.encode_pretty().unwrap();
        let decoded: CandidateBinding = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded, binding);
        assert!(!String::from_utf8(bytes).unwrap().contains("sha256"));
    }
}
