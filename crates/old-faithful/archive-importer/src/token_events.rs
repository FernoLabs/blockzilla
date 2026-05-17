use anyhow::{Context, Result, anyhow};
use blockzilla_format::{
    ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY, ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS,
    ARCHIVE_V2_PUBKEY_REGISTRY_FILE, ARCHIVE_V2_RAW_BLOCKS_FILE, ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE,
    ARCHIVE_V2_SIGNATURES_FILE, ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_INNER_IX,
    ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK, ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK,
    ArchiveV2HotBlockBlob, ArchiveV2HotInstruction, ArchiveV2HotInstructionData,
    ArchiveV2HotMessagePayload, ArchiveV2HotTxRow, CompactInnerInstruction,
    CompactInnerInstructions, CompactLogStream, CompactMetaV1, CompactPubkey, KeyIndex, KeyStore,
    archive_v2_hot_index_path, read_archive_v2_hot_block_index, wincode_leb128_config,
};
use core::mem::MaybeUninit;
use of_car_reader::{
    CarBlockReader,
    confirmed_block::TransactionStatusMeta,
    metadata_decoder::{
        InnerInstructionVisit, TokenBalanceVisit, TransactionStatusMetaVisitor,
        ZstdReusableDecoder, decode_transaction_status_meta_into, slot_uses_protobuf_metadata,
        visit_protobuf_transaction_status_meta,
    },
    node::{Node, decode_node, peek_node_type},
    versioned_transaction::{CompiledInstruction, VersionedMessage, VersionedTransaction},
};
use prost::Message;
use solana_pubkey::Pubkey;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Instant,
};
use tracing::info;
use wincode::{
    ReadResult, SchemaRead,
    config::{Config, ConfigCore},
    io::Reader,
    len::SeqLen,
};

use crate::{BUFFER_SIZE, ProgressTracker, TokenEventInputFormat};

const EVENTS_MAGIC: &[u8; 8] = b"BZTEVT01";
const EVENT_RECORD_BYTES: usize = 48;
const TOKEN_IX_MAGIC: &[u8; 8] = b"BZTIX001";
const TOKEN_IX_RECORD_BYTES: usize = 64;
const NO_ID: u32 = u32::MAX;
const NO_INNER_IX: u16 = u16::MAX;

const EVENT_FLAG_INNER: u16 = 1 << 0;
const EVENT_FLAG_TX_ERROR: u16 = 1 << 1;
const TOKEN_IX_FLAG_DIRECT_MINT: u16 = 1 << 2;

const TOKEN_PROGRAM_ID_STR: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID_STR: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

#[derive(Debug)]
pub(crate) struct TokenEventDumpConfig {
    pub input: PathBuf,
    pub output_dir: PathBuf,
    pub format: TokenEventInputFormat,
    pub index: Option<PathBuf>,
    pub registry: Option<PathBuf>,
    pub signatures: Option<PathBuf>,
    pub blockzilla_account_ids: bool,
    pub workers: usize,
    pub chunk_size: usize,
    pub mint: String,
    pub max_blocks: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct TokenInstructionDumpConfig {
    pub input: PathBuf,
    pub output_dir: PathBuf,
    pub format: TokenEventInputFormat,
    pub index: Option<PathBuf>,
    pub registry: Option<PathBuf>,
    pub workers: usize,
    pub chunk_size: usize,
    pub mint_filter: Option<String>,
    pub no_output: bool,
    pub outer_only: bool,
    pub max_blocks: Option<u64>,
}

#[derive(Debug, Default)]
struct DumpStats {
    blocks: u64,
    txs: u64,
    txs_with_events: u64,
    events: u64,
    create_events: u64,
    transfer_events: u64,
    mint_events: u64,
    burn_events: u64,
    close_events: u64,
    skipped_raw_txs: u64,
    skipped_raw_metadata: u64,
    skipped_tx_decode: u64,
    skipped_metadata_decode: u64,
    skipped_unresolved_programs: u64,
    skipped_unresolved_accounts: u64,
    skipped_missing_registry_ids: u64,
    read_bytes: u64,
    decoded_bytes: u64,
}

#[derive(Debug)]
struct TokenInstructionStats {
    blocks: u64,
    txs: u64,
    outer_ixs: u64,
    inner_ixs: u64,
    token_program_ixs: u64,
    token_program_token_ixs: u64,
    token_program_2022_ixs: u64,
    token_ixs_without_data: u64,
    decoded_token_ixs: u64,
    unknown_token_ixs: u64,
    token_ixs: u64,
    direct_mint_ixs: u64,
    records_encoded: u64,
    create_ixs: u64,
    transfer_ixs: u64,
    mint_ixs: u64,
    burn_ixs: u64,
    close_ixs: u64,
    token_tag_counts: [u64; 256],
    metadata_needed: u64,
    metadata_inner_needed: u64,
    metadata_loaded_needed: u64,
    metadata_decoded: u64,
    metadata_skipped: u64,
    skipped_raw_txs: u64,
    skipped_raw_metadata: u64,
    skipped_unresolved_programs: u64,
    skipped_unresolved_accounts: u64,
    skipped_by_mint_filter: u64,
    read_bytes: u64,
    decoded_bytes: u64,
}

impl Default for TokenInstructionStats {
    fn default() -> Self {
        Self {
            blocks: 0,
            txs: 0,
            outer_ixs: 0,
            inner_ixs: 0,
            token_program_ixs: 0,
            token_program_token_ixs: 0,
            token_program_2022_ixs: 0,
            token_ixs_without_data: 0,
            decoded_token_ixs: 0,
            unknown_token_ixs: 0,
            token_ixs: 0,
            direct_mint_ixs: 0,
            records_encoded: 0,
            create_ixs: 0,
            transfer_ixs: 0,
            mint_ixs: 0,
            burn_ixs: 0,
            close_ixs: 0,
            token_tag_counts: [0; 256],
            metadata_needed: 0,
            metadata_inner_needed: 0,
            metadata_loaded_needed: 0,
            metadata_decoded: 0,
            metadata_skipped: 0,
            skipped_raw_txs: 0,
            skipped_raw_metadata: 0,
            skipped_unresolved_programs: 0,
            skipped_unresolved_accounts: 0,
            skipped_by_mint_filter: 0,
            read_bytes: 0,
            decoded_bytes: 0,
        }
    }
}

impl TokenInstructionStats {
    fn merge(&mut self, other: Self) {
        self.blocks += other.blocks;
        self.txs += other.txs;
        self.outer_ixs += other.outer_ixs;
        self.inner_ixs += other.inner_ixs;
        self.token_program_ixs += other.token_program_ixs;
        self.token_program_token_ixs += other.token_program_token_ixs;
        self.token_program_2022_ixs += other.token_program_2022_ixs;
        self.token_ixs_without_data += other.token_ixs_without_data;
        self.decoded_token_ixs += other.decoded_token_ixs;
        self.unknown_token_ixs += other.unknown_token_ixs;
        self.token_ixs += other.token_ixs;
        self.direct_mint_ixs += other.direct_mint_ixs;
        self.records_encoded += other.records_encoded;
        self.create_ixs += other.create_ixs;
        self.transfer_ixs += other.transfer_ixs;
        self.mint_ixs += other.mint_ixs;
        self.burn_ixs += other.burn_ixs;
        self.close_ixs += other.close_ixs;
        for (left, right) in self.token_tag_counts.iter_mut().zip(other.token_tag_counts) {
            *left += right;
        }
        self.metadata_needed += other.metadata_needed;
        self.metadata_inner_needed += other.metadata_inner_needed;
        self.metadata_loaded_needed += other.metadata_loaded_needed;
        self.metadata_decoded += other.metadata_decoded;
        self.metadata_skipped += other.metadata_skipped;
        self.skipped_raw_txs += other.skipped_raw_txs;
        self.skipped_raw_metadata += other.skipped_raw_metadata;
        self.skipped_unresolved_programs += other.skipped_unresolved_programs;
        self.skipped_unresolved_accounts += other.skipped_unresolved_accounts;
        self.skipped_by_mint_filter += other.skipped_by_mint_filter;
        self.read_bytes += other.read_bytes;
        self.decoded_bytes += other.decoded_bytes;
    }

    fn record_token_program_instruction(&mut self, token_program: TokenProgramKind, data: &[u8]) {
        self.token_program_ixs += 1;
        match token_program {
            TokenProgramKind::Token => self.token_program_token_ixs += 1,
            TokenProgramKind::Token2022 => self.token_program_2022_ixs += 1,
        }
        if let Some(tag) = data.first().copied() {
            self.token_tag_counts[tag as usize] += 1;
        } else {
            self.token_ixs_without_data += 1;
        }
    }

    fn record_decoded_token_instruction(&mut self, kind: TokenEventKind) {
        self.decoded_token_ixs += 1;
        match kind {
            TokenEventKind::CreateAccount => self.create_ixs += 1,
            TokenEventKind::Transfer => self.transfer_ixs += 1,
            TokenEventKind::Mint => self.mint_ixs += 1,
            TokenEventKind::Burn => self.burn_ixs += 1,
            TokenEventKind::Close => self.close_ixs += 1,
        }
    }

    fn token_event_kind_counts_summary(&self) -> String {
        format!(
            "create:{},transfer:{},mint:{},burn:{},close:{}",
            self.create_ixs, self.transfer_ixs, self.mint_ixs, self.burn_ixs, self.close_ixs
        )
    }

    fn token_instruction_tag_counts_summary(&self) -> String {
        let mut parts = Vec::new();
        for (tag, count) in self.token_tag_counts.iter().enumerate() {
            if *count == 0 {
                continue;
            }
            let name = token_instruction_tag_name(tag as u8);
            if name == "Unknown" {
                parts.push(format!("tag_{tag}:{count}"));
            } else {
                parts.push(format!("{name}:{count}"));
            }
        }
        if parts.is_empty() {
            "none".to_string()
        } else {
            parts.join(",")
        }
    }
}

impl DumpStats {
    fn merge(&mut self, other: Self) {
        self.blocks += other.blocks;
        self.txs += other.txs;
        self.txs_with_events += other.txs_with_events;
        self.events += other.events;
        self.create_events += other.create_events;
        self.transfer_events += other.transfer_events;
        self.mint_events += other.mint_events;
        self.burn_events += other.burn_events;
        self.close_events += other.close_events;
        self.skipped_raw_txs += other.skipped_raw_txs;
        self.skipped_raw_metadata += other.skipped_raw_metadata;
        self.skipped_tx_decode += other.skipped_tx_decode;
        self.skipped_metadata_decode += other.skipped_metadata_decode;
        self.skipped_unresolved_programs += other.skipped_unresolved_programs;
        self.skipped_unresolved_accounts += other.skipped_unresolved_accounts;
        self.skipped_missing_registry_ids += other.skipped_missing_registry_ids;
        self.read_bytes += other.read_bytes;
        self.decoded_bytes += other.decoded_bytes;
    }
}

#[derive(Debug)]
struct EventRecord {
    slot: u64,
    tx_index: u32,
    outer_ix: u16,
    inner_ix: u16,
    event_kind: TokenEventKind,
    token_program: TokenProgramKind,
    flags: u16,
    signature_id: u32,
    wallet_id: u32,
    token_account_id: u32,
    counterparty_token_account_id: u32,
    amount: u64,
    raw_tag: u8,
}

impl EventRecord {
    fn encode(&self) -> [u8; EVENT_RECORD_BYTES] {
        let mut out = [0u8; EVENT_RECORD_BYTES];
        out[0..8].copy_from_slice(&self.slot.to_le_bytes());
        out[8..12].copy_from_slice(&self.tx_index.to_le_bytes());
        out[12..14].copy_from_slice(&self.outer_ix.to_le_bytes());
        out[14..16].copy_from_slice(&self.inner_ix.to_le_bytes());
        out[16] = self.event_kind as u8;
        out[17] = self.token_program as u8;
        out[18..20].copy_from_slice(&self.flags.to_le_bytes());
        out[20..24].copy_from_slice(&self.signature_id.to_le_bytes());
        out[24..28].copy_from_slice(&self.wallet_id.to_le_bytes());
        out[28..32].copy_from_slice(&self.token_account_id.to_le_bytes());
        out[32..36].copy_from_slice(&self.counterparty_token_account_id.to_le_bytes());
        out[36..44].copy_from_slice(&self.amount.to_le_bytes());
        out[44] = self.raw_tag;
        out
    }
}

#[derive(Debug)]
struct TokenInstructionRecord {
    slot: u64,
    tx_index: u32,
    outer_ix: u16,
    inner_ix: u16,
    token_program: TokenProgramKind,
    event_kind: TokenEventKind,
    flags: u16,
    signature_id: u32,
    token_account_id: u32,
    counterparty_token_account_id: u32,
    wallet_id: u32,
    mint_id: u32,
    amount: u64,
    raw_tag: u8,
}

impl TokenInstructionRecord {
    fn encode(&self) -> [u8; TOKEN_IX_RECORD_BYTES] {
        let mut out = [0u8; TOKEN_IX_RECORD_BYTES];
        out[0..8].copy_from_slice(&self.slot.to_le_bytes());
        out[8..12].copy_from_slice(&self.tx_index.to_le_bytes());
        out[12..14].copy_from_slice(&self.outer_ix.to_le_bytes());
        out[14..16].copy_from_slice(&self.inner_ix.to_le_bytes());
        out[16] = self.token_program as u8;
        out[17] = self.event_kind as u8;
        out[18..20].copy_from_slice(&self.flags.to_le_bytes());
        out[20..24].copy_from_slice(&self.signature_id.to_le_bytes());
        out[24..28].copy_from_slice(&self.token_account_id.to_le_bytes());
        out[28..32].copy_from_slice(&self.counterparty_token_account_id.to_le_bytes());
        out[32..36].copy_from_slice(&self.wallet_id.to_le_bytes());
        out[36..40].copy_from_slice(&self.mint_id.to_le_bytes());
        out[40..48].copy_from_slice(&self.amount.to_le_bytes());
        out[48] = self.raw_tag;
        out
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum TokenEventKind {
    CreateAccount = 1,
    Transfer = 2,
    Mint = 3,
    Burn = 4,
    Close = 5,
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum TokenProgramKind {
    Token = 1,
    Token2022 = 2,
}

fn token_instruction_tag_name(tag: u8) -> &'static str {
    match tag {
        0 => "InitializeMint",
        1 => "InitializeAccount",
        2 => "InitializeMultisig",
        3 => "Transfer",
        4 => "Approve",
        5 => "Revoke",
        6 => "SetAuthority",
        7 => "MintTo",
        8 => "Burn",
        9 => "CloseAccount",
        10 => "FreezeAccount",
        11 => "ThawAccount",
        12 => "TransferChecked",
        13 => "ApproveChecked",
        14 => "MintToChecked",
        15 => "BurnChecked",
        16 => "InitializeAccount2",
        17 => "SyncNative",
        18 => "InitializeAccount3",
        19 => "InitializeMultisig2",
        20 => "InitializeMint2",
        21 => "GetAccountDataSize",
        22 => "InitializeImmutableOwner",
        23 => "AmountToUiAmount",
        24 => "UiAmountToAmount",
        25 => "InitializeMintCloseAuthority",
        26 => "TransferFeeExtension",
        27 => "ConfidentialTransferExtension",
        28 => "DefaultAccountStateExtension",
        29 => "Reallocate",
        30 => "MemoTransferExtension",
        31 => "CreateNativeMint",
        32 => "InitializeNonTransferableMint",
        33 => "InterestBearingMintExtension",
        34 => "CpiGuardExtension",
        35 => "InitializePermanentDelegate",
        36 => "TransferHookExtension",
        37 => "ConfidentialTransferFeeExtension",
        38 => "WithdrawExcessLamports",
        39 => "MetadataPointerExtension",
        40 => "GroupPointerExtension",
        41 => "GroupMemberPointerExtension",
        42 => "ConfidentialMintBurnExtension",
        43 => "ScaledUiAmountExtension",
        44 => "PausableExtension",
        _ => "Unknown",
    }
}

#[derive(Debug)]
struct DecodedTokenInstruction {
    kind: TokenEventKind,
    token_account_index: Option<u32>,
    counterparty_token_account_index: Option<u32>,
    wallet_index: Option<u32>,
    mint_index: Option<u32>,
    mint: Option<[u8; 32]>,
    owner: Option<[u8; 32]>,
    amount: u64,
    raw_tag: u8,
}

#[derive(Debug, Clone, Copy)]
struct InstructionRef<'a> {
    outer_ix: u16,
    inner_ix: u16,
    program_id_index: u32,
    accounts: &'a [u8],
    data: &'a [u8],
}

struct HotKeyLookup<'a> {
    static_keys: &'a [CompactPubkey],
    loaded_writable: &'a [CompactPubkey],
    loaded_readonly: &'a [CompactPubkey],
}

impl<'a> HotKeyLookup<'a> {
    fn new(
        message: &'a ArchiveV2HotMessagePayload,
        meta: Option<&'a CompactMetaInstructionView>,
    ) -> Self {
        Self::from_loaded(
            message,
            meta.map(|meta| meta.loaded_writable_addresses.as_slice())
                .unwrap_or_default(),
            meta.map(|meta| meta.loaded_readonly_addresses.as_slice())
                .unwrap_or_default(),
        )
    }

    fn from_loaded(
        message: &'a ArchiveV2HotMessagePayload,
        loaded_writable: &'a [CompactPubkey],
        loaded_readonly: &'a [CompactPubkey],
    ) -> Self {
        let static_keys = match message {
            ArchiveV2HotMessagePayload::Legacy(message) => message.account_keys.as_slice(),
            ArchiveV2HotMessagePayload::V0(message) => message.account_keys.as_slice(),
        };
        Self {
            static_keys,
            loaded_writable,
            loaded_readonly,
        }
    }

    #[inline]
    fn get(&self, index: u32) -> Option<HotKeyRef> {
        let index = index as usize;
        if let Some(key) = self.static_keys.get(index) {
            return Some(HotKeyRef::from_compact(*key));
        }
        let index = index.checked_sub(self.static_keys.len())?;
        if let Some(key) = self.loaded_writable.get(index) {
            return Some(HotKeyRef::from_compact(*key));
        }
        let index = index.checked_sub(self.loaded_writable.len())?;
        self.loaded_readonly
            .get(index)
            .map(|key| HotKeyRef::from_compact(*key))
    }
}

#[derive(Debug, SchemaRead)]
struct CompactMetaInstructionView {
    _err: Option<SkipBytes>,
    _fee: SkipU64,
    _pre_balances: Vec<SkipU64>,
    _post_balances: Vec<SkipU64>,
    inner_instructions: Option<Vec<CompactInnerInstructions>>,
    // Current metadata puts logs before loaded addresses. The next payload layout
    // should move loaded addresses and inner instructions to the prefix so this
    // reader can skip logs entirely.
    _logs: Option<CompactLogStream>,
    _pre_token_balances: Vec<SkipCompactTokenBalance>,
    _post_token_balances: Vec<SkipCompactTokenBalance>,
    _rewards: Vec<SkipCompactReward>,
    loaded_writable_addresses: Vec<CompactPubkey>,
    loaded_readonly_addresses: Vec<CompactPubkey>,
}

#[derive(Debug, SchemaRead)]
struct CompactMetaInnerInstructionView {
    _err: Option<SkipBytes>,
    _fee: SkipU64,
    _pre_balances: Vec<SkipU64>,
    _post_balances: Vec<SkipU64>,
    inner_instructions: Option<Vec<CompactInnerInstructions>>,
}

#[derive(Debug)]
struct SkipU8;

#[derive(Debug)]
struct SkipU32;

#[derive(Debug)]
struct SkipU64;

#[derive(Debug)]
struct SkipI32;

#[derive(Debug)]
struct SkipI64;

macro_rules! impl_skip_primitive {
    ($name:ty, $primitive:ty) => {
        unsafe impl<'de, C: ConfigCore> SchemaRead<'de, C> for $name {
            type Dst = Self;

            #[inline]
            fn read(reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
                let _ = <$primitive as SchemaRead<'de, C>>::get(reader)?;
                dst.write(Self);
                Ok(())
            }
        }
    };
}

impl_skip_primitive!(SkipU8, u8);
impl_skip_primitive!(SkipU32, u32);
impl_skip_primitive!(SkipU64, u64);
impl_skip_primitive!(SkipI32, i32);
impl_skip_primitive!(SkipI64, i64);

#[derive(Debug)]
struct SkipBytes;

unsafe impl<'de, C: Config> SchemaRead<'de, C> for SkipBytes {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let len = <C::LengthEncoding as SeqLen<C>>::read(reader.by_ref())?;
        let _ = reader.take_scoped(len)?;
        dst.write(Self);
        Ok(())
    }
}

#[derive(Debug)]
struct SkipCompactPubkey;

unsafe impl<'de, C: ConfigCore> SchemaRead<'de, C> for SkipCompactPubkey {
    type Dst = Self;

    #[inline]
    fn read(mut reader: impl Reader<'de>, dst: &mut MaybeUninit<Self::Dst>) -> ReadResult<()> {
        let id = <u32 as SchemaRead<'de, C>>::get(reader.by_ref())?;
        if id == CompactPubkey::RAW_SENTINEL {
            let _ = reader.take_scoped(32)?;
        }
        dst.write(Self);
        Ok(())
    }
}

#[derive(Debug, SchemaRead)]
struct SkipCompactTokenBalance {
    _account_index: SkipU32,
    _mint: Option<SkipCompactPubkey>,
    _owner: Option<SkipCompactPubkey>,
    _program_id: Option<SkipCompactPubkey>,
    _amount: SkipU64,
    _decimals: SkipU8,
}

#[derive(Debug, SchemaRead)]
struct SkipCompactReward {
    _pubkey: SkipCompactPubkey,
    _lamports: SkipI64,
    _post_balance: SkipU64,
    _reward_type: SkipI32,
    _commission: Option<SkipU8>,
}

#[derive(Debug)]
struct Registries {
    wallets: IdRegistry<32>,
    token_accounts: IdRegistry<32>,
    signatures: IdRegistry<64>,
}

impl Registries {
    fn new() -> Self {
        Self {
            wallets: IdRegistry::default(),
            token_accounts: IdRegistry::default(),
            signatures: IdRegistry::default(),
        }
    }
}

#[derive(Debug)]
struct IdRegistry<const N: usize> {
    rows: Vec<[u8; N]>,
    ids: HashMap<[u8; N], u32>,
}

enum AccountIdMode {
    Local,
    Blockzilla(KeyIndex),
}

impl AccountIdMode {
    fn account_id<const N: usize>(
        &self,
        key: [u8; 32],
        local: &mut IdRegistry<N>,
        stats: &mut DumpStats,
    ) -> Result<u32> {
        match self {
            Self::Local => {
                let key: [u8; N] = key[..]
                    .try_into()
                    .map_err(|_| anyhow!("invalid local registry key size"))?;
                local.id(key)
            }
            Self::Blockzilla(index) => {
                let Some(id) = index.lookup(&key) else {
                    stats.skipped_missing_registry_ids += 1;
                    return Ok(NO_ID);
                };
                Ok(id)
            }
        }
    }

    fn is_blockzilla(&self) -> bool {
        matches!(self, Self::Blockzilla(_))
    }

    fn known_key(&self, raw: [u8; 32]) -> KnownHotKey {
        KnownHotKey {
            raw,
            id: match self {
                Self::Local => None,
                Self::Blockzilla(index) => index.lookup(&raw),
            },
        }
    }

    fn hot_key_id(&self, key: HotKeyRef, stats: &mut DumpStats) -> Result<u32> {
        match key {
            HotKeyRef::Id(id) => Ok(id),
            HotKeyRef::Raw(key) => blockzilla_account_id(self, key, stats),
        }
    }
}

impl<const N: usize> Default for IdRegistry<N> {
    fn default() -> Self {
        Self {
            rows: Vec::new(),
            ids: HashMap::new(),
        }
    }
}

impl<const N: usize> IdRegistry<N> {
    fn id(&mut self, key: [u8; N]) -> Result<u32> {
        if let Some(id) = self.ids.get(&key).copied() {
            return Ok(id);
        }
        let id = u32::try_from(self.rows.len()).context("token event registry exceeds u32::MAX")?;
        self.rows.push(key);
        self.ids.insert(key, id);
        Ok(id)
    }

    fn write_raw(&self, path: &Path) -> Result<()> {
        let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
        for row in &self.rows {
            writer.write_all(row)?;
        }
        writer
            .flush()
            .with_context(|| format!("flush {}", path.display()))
    }
}

#[derive(Debug, Default)]
struct Discovery {
    token_accounts: HashSet<[u8; 32]>,
    token_account_owner: HashMap<[u8; 32], [u8; 32]>,
}

#[derive(Debug, Default)]
struct CarMeta<'a> {
    tx_error: bool,
    pre_token_balances: Vec<CarTokenBalance<'a>>,
    post_token_balances: Vec<CarTokenBalance<'a>>,
    inner_instructions: Vec<CarInnerInstruction<'a>>,
    loaded_writable_addresses: Vec<Cow<'a, [u8]>>,
    loaded_readonly_addresses: Vec<Cow<'a, [u8]>>,
}

impl<'a> TransactionStatusMetaVisitor<'a> for CarMeta<'a> {
    #[inline]
    fn wants_status_error(&self) -> bool {
        true
    }

    #[inline]
    fn wants_pre_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_post_token_balances(&self) -> bool {
        true
    }

    #[inline]
    fn wants_inner_instructions(&self) -> bool {
        true
    }

    #[inline]
    fn wants_loaded_addresses(&self) -> bool {
        true
    }

    #[inline]
    fn status_error(&mut self, _err: &'a [u8]) {
        self.tx_error = true;
    }

    #[inline]
    fn pre_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        self.pre_token_balances
            .push(CarTokenBalance::from_visit(balance));
    }

    #[inline]
    fn post_token_balance(&mut self, balance: TokenBalanceVisit<'a>) {
        self.post_token_balances
            .push(CarTokenBalance::from_visit(balance));
    }

    #[inline]
    fn inner_instruction(&mut self, instruction: InnerInstructionVisit<'a>) {
        self.inner_instructions
            .push(CarInnerInstruction::from_visit(instruction));
    }

    #[inline]
    fn loaded_writable_address(&mut self, address: &'a [u8]) {
        self.loaded_writable_addresses.push(Cow::Borrowed(address));
    }

    #[inline]
    fn loaded_readonly_address(&mut self, address: &'a [u8]) {
        self.loaded_readonly_addresses.push(Cow::Borrowed(address));
    }
}

impl CarMeta<'static> {
    fn from_owned(meta: &TransactionStatusMeta) -> Self {
        let mut out = Self {
            tx_error: meta.err.is_some(),
            ..Default::default()
        };
        out.pre_token_balances.extend(
            meta.pre_token_balances
                .iter()
                .map(CarTokenBalance::from_owned),
        );
        out.post_token_balances.extend(
            meta.post_token_balances
                .iter()
                .map(CarTokenBalance::from_owned),
        );
        out.loaded_writable_addresses.extend(
            meta.loaded_writable_addresses
                .iter()
                .map(|address| Cow::Owned(address.clone())),
        );
        out.loaded_readonly_addresses.extend(
            meta.loaded_readonly_addresses
                .iter()
                .map(|address| Cow::Owned(address.clone())),
        );
        for group in &meta.inner_instructions {
            for (inner_instruction_index, instruction) in group.instructions.iter().enumerate() {
                out.inner_instructions.push(CarInnerInstruction {
                    outer_instruction_index: group.index,
                    inner_instruction_index: inner_instruction_index as u32,
                    program_id_index: instruction.program_id_index,
                    accounts: Cow::Owned(instruction.accounts.clone()),
                    data: Cow::Owned(instruction.data.clone()),
                });
            }
        }
        out
    }
}

#[derive(Debug)]
struct CarTokenBalance<'a> {
    account_index: u32,
    mint: Cow<'a, str>,
    owner: Cow<'a, str>,
}

impl<'a> CarTokenBalance<'a> {
    fn from_visit(balance: TokenBalanceVisit<'a>) -> Self {
        Self {
            account_index: balance.account_index,
            mint: Cow::Borrowed(balance.mint),
            owner: Cow::Borrowed(balance.owner),
        }
    }

    fn from_owned(balance: &of_car_reader::confirmed_block::TokenBalance) -> Self {
        Self {
            account_index: balance.account_index,
            mint: Cow::Owned(balance.mint.clone()),
            owner: Cow::Owned(balance.owner.clone()),
        }
    }
}

#[derive(Debug)]
struct CarInnerInstruction<'a> {
    outer_instruction_index: u32,
    inner_instruction_index: u32,
    program_id_index: u32,
    accounts: Cow<'a, [u8]>,
    data: Cow<'a, [u8]>,
}

impl<'a> CarInnerInstruction<'a> {
    fn from_visit(instruction: InnerInstructionVisit<'a>) -> Self {
        Self {
            outer_instruction_index: instruction.outer_instruction_index,
            inner_instruction_index: instruction.inner_instruction_index,
            program_id_index: instruction.program_id_index,
            accounts: Cow::Borrowed(instruction.accounts),
            data: Cow::Borrowed(instruction.data),
        }
    }
}

pub(crate) fn dump_token_instructions(config: TokenInstructionDumpConfig) -> Result<()> {
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("create {}", config.output_dir.display()))?;

    let input_format = infer_format(&config.input, config.format);
    let raw_blocks = match input_format {
        TokenEventInputFormat::HotZstd => false,
        TokenEventInputFormat::HotRaw => true,
        other => anyhow::bail!(
            "token instruction dump currently supports hot-zstd and hot-raw only, got {other:?}"
        ),
    };
    let input_file_bytes = std::fs::metadata(&config.input)
        .with_context(|| format!("stat {}", config.input.display()))?
        .len();
    let input_dir = config.input.parent().unwrap_or_else(|| Path::new("."));
    let index_path = config
        .index
        .clone()
        .unwrap_or_else(|| archive_v2_hot_index_path(&config.input));
    let registry_path = config
        .registry
        .clone()
        .unwrap_or_else(|| input_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE));
    let account_id_mode = Arc::new(AccountIdMode::Blockzilla(load_key_index(&registry_path)?));
    let known = HotKnownKeys::new(
        account_id_mode.as_ref(),
        config
            .mint_filter
            .as_deref()
            .unwrap_or(TOKEN_PROGRAM_ID_STR)
            .parse::<Pubkey>()
            .map(|key| key.to_bytes())
            .unwrap_or_else(|_| token_program_id()),
    );
    let mint_filter = config
        .mint_filter
        .as_deref()
        .map(|mint| {
            Pubkey::from_str(mint)
                .with_context(|| format!("parse mint filter {mint}"))
                .map(|key| account_id_mode.known_key(key.to_bytes()))
        })
        .transpose()?;

    let instructions_path = config.output_dir.join("token_instructions.bin");
    let mut instructions = if config.no_output {
        None
    } else {
        let instructions_file = File::create(&instructions_path)
            .with_context(|| format!("create {}", instructions_path.display()))?;
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, instructions_file);
        writer.write_all(TOKEN_IX_MAGIC)?;
        Some(writer)
    };

    let mut stats = TokenInstructionStats::default();
    let started = Instant::now();
    scan_hot_token_instructions_parallel(
        &config.input,
        &index_path,
        raw_blocks,
        account_id_mode,
        known,
        mint_filter,
        instructions.as_mut(),
        config.max_blocks,
        config.workers,
        config.chunk_size,
        config.no_output,
        config.outer_only,
        &config.output_dir,
        &mut stats,
    )?;
    if let Some(instructions) = instructions.as_mut() {
        instructions
            .flush()
            .with_context(|| format!("flush {}", instructions_path.display()))?;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let output_bytes = if config.no_output {
        0
    } else {
        std::fs::metadata(&instructions_path)?.len()
    };
    let token_event_kind_counts = stats.token_event_kind_counts_summary();
    let token_instruction_tag_counts = stats.token_instruction_tag_counts_summary();
    let meta = format!(
        "format_version=1\n\
         record_bytes={TOKEN_IX_RECORD_BYTES}\n\
         input={}\n\
         input_file_bytes={input_file_bytes}\n\
         input_format={input_format:?}\n\
         index={}\n\
         registry={}\n\
         mint_filter={}\n\
         no_output={}\n\
         outer_only={}\n\
         workers={}\n\
         chunk_size={}\n\
         blocks={}\n\
         txs={}\n\
         outer_ixs={}\n\
         inner_ixs={}\n\
         token_program_ixs={}\n\
         token_program_token_ixs={}\n\
         token_program_2022_ixs={}\n\
         token_ixs_without_data={}\n\
         decoded_token_ixs={}\n\
         unknown_token_ixs={}\n\
         token_ixs={}\n\
         direct_mint_ixs={}\n\
         records_encoded={}\n\
         token_event_kind_counts={}\n\
         token_instruction_tag_counts={}\n\
         metadata_needed={}\n\
         metadata_inner_needed={}\n\
         metadata_loaded_needed={}\n\
         metadata_decoded={}\n\
         metadata_skipped={}\n\
         skipped_raw_txs={}\n\
         skipped_raw_metadata={}\n\
         skipped_unresolved_programs={}\n\
         skipped_unresolved_accounts={}\n\
         skipped_by_mint_filter={}\n\
         read_bytes={}\n\
         decoded_bytes={}\n\
         output_bytes={output_bytes}\n\
         elapsed_s={elapsed:.3}\n\
         blocks_s={:.2}\n\
         tx_s={:.2}\n\
         token_ix_s={:.2}\n\
         read_MiB_s={:.2}\n\
         decoded_MiB_s={:.2}\n",
        config.input.display(),
        index_path.display(),
        registry_path.display(),
        config.mint_filter.as_deref().unwrap_or("none"),
        config.no_output,
        config.outer_only,
        config.workers,
        config.chunk_size,
        stats.blocks,
        stats.txs,
        stats.outer_ixs,
        stats.inner_ixs,
        stats.token_program_ixs,
        stats.token_program_token_ixs,
        stats.token_program_2022_ixs,
        stats.token_ixs_without_data,
        stats.decoded_token_ixs,
        stats.unknown_token_ixs,
        stats.token_ixs,
        stats.direct_mint_ixs,
        stats.records_encoded,
        token_event_kind_counts,
        token_instruction_tag_counts,
        stats.metadata_needed,
        stats.metadata_inner_needed,
        stats.metadata_loaded_needed,
        stats.metadata_decoded,
        stats.metadata_skipped,
        stats.skipped_raw_txs,
        stats.skipped_raw_metadata,
        stats.skipped_unresolved_programs,
        stats.skipped_unresolved_accounts,
        stats.skipped_by_mint_filter,
        stats.read_bytes,
        stats.decoded_bytes,
        rate(stats.blocks, elapsed),
        rate(stats.txs, elapsed),
        rate(stats.token_ixs, elapsed),
        mib_rate(stats.read_bytes, elapsed),
        mib_rate(stats.decoded_bytes, elapsed),
    );
    std::fs::write(config.output_dir.join("meta.txt"), meta)?;
    let output_display = if config.no_output {
        "<disabled>".to_string()
    } else {
        instructions_path.display().to_string()
    };
    info!(
        "token instruction dump complete in {:.2}s: blocks={} txs={} token_ixs={} records_encoded={} metadata_decoded={} output={}",
        elapsed,
        stats.blocks,
        stats.txs,
        stats.token_ixs,
        stats.records_encoded,
        stats.metadata_decoded,
        output_display,
    );
    println!(
        "token_instructions format={input_format:?} workers={} chunk_size={} no_output={} outer_only={} blocks={} txs={} outer_ixs={} inner_ixs={} token_program_ixs={} decoded_token_ixs={} unknown_token_ixs={} token_ixs={} direct_mint_ixs={} records_encoded={} token_event_kind_counts={} token_instruction_tag_counts={} metadata_needed={} metadata_inner_needed={} metadata_loaded_needed={} metadata_decoded={} metadata_skipped={} skipped_raw_txs={} skipped_raw_metadata={} skipped_unresolved_programs={} skipped_unresolved_accounts={} skipped_by_mint_filter={} elapsed_s={elapsed:.3} blocks_s={:.2} tx_s={:.2} token_ix_s={:.2} input_file_bytes={input_file_bytes} read_MiB_s={:.2} decoded_MiB_s={:.2} output_bytes={output_bytes}",
        config.workers.max(1),
        config.chunk_size.max(1),
        config.no_output,
        config.outer_only,
        stats.blocks,
        stats.txs,
        stats.outer_ixs,
        stats.inner_ixs,
        stats.token_program_ixs,
        stats.decoded_token_ixs,
        stats.unknown_token_ixs,
        stats.token_ixs,
        stats.direct_mint_ixs,
        stats.records_encoded,
        stats.token_event_kind_counts_summary(),
        stats.token_instruction_tag_counts_summary(),
        stats.metadata_needed,
        stats.metadata_inner_needed,
        stats.metadata_loaded_needed,
        stats.metadata_decoded,
        stats.metadata_skipped,
        stats.skipped_raw_txs,
        stats.skipped_raw_metadata,
        stats.skipped_unresolved_programs,
        stats.skipped_unresolved_accounts,
        stats.skipped_by_mint_filter,
        rate(stats.blocks, elapsed),
        rate(stats.txs, elapsed),
        rate(stats.token_ixs, elapsed),
        mib_rate(stats.read_bytes, elapsed),
        mib_rate(stats.decoded_bytes, elapsed),
    );
    Ok(())
}

pub(crate) fn dump_usdc_token_events(config: TokenEventDumpConfig) -> Result<()> {
    let mint = Pubkey::from_str(&config.mint)
        .with_context(|| format!("parse mint {}", config.mint))?
        .to_bytes();
    std::fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("create {}", config.output_dir.display()))?;

    let input_format = infer_format(&config.input, config.format);
    let input_file_bytes = std::fs::metadata(&config.input)
        .with_context(|| format!("stat {}", config.input.display()))?
        .len();
    let input_dir = config.input.parent().unwrap_or_else(|| Path::new("."));
    let index_path = config
        .index
        .clone()
        .unwrap_or_else(|| archive_v2_hot_index_path(&config.input));
    let registry_path = config
        .registry
        .clone()
        .unwrap_or_else(|| input_dir.join(ARCHIVE_V2_PUBKEY_REGISTRY_FILE));
    let signatures_path = config
        .signatures
        .clone()
        .unwrap_or_else(|| input_dir.join(ARCHIVE_V2_SIGNATURES_FILE));
    let account_id_mode = Arc::new(if config.blockzilla_account_ids {
        AccountIdMode::Blockzilla(load_key_index(&registry_path)?)
    } else {
        AccountIdMode::Local
    });

    let events_path = config.output_dir.join("events.bin");
    let events_file =
        File::create(&events_path).with_context(|| format!("create {}", events_path.display()))?;
    let mut events = BufWriter::with_capacity(BUFFER_SIZE, events_file);
    events.write_all(EVENTS_MAGIC)?;

    let mut registries = Registries::new();
    let mut discovery = Discovery::default();
    let mut stats = DumpStats::default();
    let started = Instant::now();

    match input_format {
        TokenEventInputFormat::Car => scan_car(
            &config.input,
            false,
            &mut events,
            &mut registries,
            &mut discovery,
            account_id_mode.as_ref(),
            mint,
            config.max_blocks,
            &mut stats,
        )?,
        TokenEventInputFormat::CarZstd => scan_car(
            &config.input,
            true,
            &mut events,
            &mut registries,
            &mut discovery,
            account_id_mode.as_ref(),
            mint,
            config.max_blocks,
            &mut stats,
        )?,
        TokenEventInputFormat::HotZstd
        | TokenEventInputFormat::HotRaw
        | TokenEventInputFormat::HotRawZstd => {
            if account_id_mode.is_blockzilla()
                && config.workers > 1
                && matches!(
                    input_format,
                    TokenEventInputFormat::HotZstd | TokenEventInputFormat::HotRaw
                )
            {
                scan_hot_parallel(
                    &config.input,
                    &index_path,
                    input_format,
                    Arc::clone(&account_id_mode),
                    &mut events,
                    mint,
                    config.max_blocks,
                    config.workers,
                    config.chunk_size,
                    &config.output_dir,
                    &mut stats,
                )?;
            } else {
                let store = KeyStore::load(&registry_path)
                    .with_context(|| format!("load registry {}", registry_path.display()))?;
                let mut signature_file = File::open(&signatures_path)
                    .with_context(|| format!("open {}", signatures_path.display()))?;
                match input_format {
                    TokenEventInputFormat::HotZstd => scan_hot_zstd(
                        &config.input,
                        &index_path,
                        &store,
                        &mut signature_file,
                        &mut events,
                        &mut registries,
                        &mut discovery,
                        account_id_mode.as_ref(),
                        mint,
                        config.max_blocks,
                        &mut stats,
                    )?,
                    TokenEventInputFormat::HotRaw => scan_hot_raw(
                        &config.input,
                        &index_path,
                        false,
                        &store,
                        &mut signature_file,
                        &mut events,
                        &mut registries,
                        &mut discovery,
                        account_id_mode.as_ref(),
                        mint,
                        config.max_blocks,
                        &mut stats,
                    )?,
                    TokenEventInputFormat::HotRawZstd => scan_hot_raw(
                        &config.input,
                        &index_path,
                        true,
                        &store,
                        &mut signature_file,
                        &mut events,
                        &mut registries,
                        &mut discovery,
                        account_id_mode.as_ref(),
                        mint,
                        config.max_blocks,
                        &mut stats,
                    )?,
                    _ => unreachable!("hot format branch only handles hot formats"),
                }
            }
        }
        TokenEventInputFormat::Auto => unreachable!("auto format should be resolved"),
    }

    events
        .flush()
        .with_context(|| format!("flush {}", events_path.display()))?;
    registries
        .wallets
        .write_raw(&config.output_dir.join("wallets.bin"))?;
    registries
        .token_accounts
        .write_raw(&config.output_dir.join("token_accounts.bin"))?;
    registries
        .signatures
        .write_raw(&config.output_dir.join("signatures.bin"))?;

    let elapsed = started.elapsed().as_secs_f64();
    let events_file_bytes = std::fs::metadata(&events_path)?.len();
    let wallets_file_bytes = std::fs::metadata(config.output_dir.join("wallets.bin"))?.len();
    let token_accounts_file_bytes =
        std::fs::metadata(config.output_dir.join("token_accounts.bin"))?.len();
    let signatures_file_bytes = std::fs::metadata(config.output_dir.join("signatures.bin"))?.len();
    let total_output_bytes = events_file_bytes
        .saturating_add(wallets_file_bytes)
        .saturating_add(token_accounts_file_bytes)
        .saturating_add(signatures_file_bytes);
    let bytes_per_event = if stats.events == 0 {
        0.0
    } else {
        total_output_bytes as f64 / stats.events as f64
    };
    let meta = format!(
        concat!(
            "format_version=1\n",
            "event_record_bytes={}\n",
            "input={}\n",
            "input_file_bytes={}\n",
            "input_format={:?}\n",
            "account_id_mode={}\n",
            "workers={}\n",
            "mint={}\n",
            "blocks={}\n",
            "txs={}\n",
            "txs_with_events={}\n",
            "events={}\n",
            "create_events={}\n",
            "transfer_events={}\n",
            "mint_events={}\n",
            "burn_events={}\n",
            "close_events={}\n",
            "wallets={}\n",
            "token_accounts={}\n",
            "signatures={}\n",
            "skipped_raw_txs={}\n",
            "skipped_raw_metadata={}\n",
            "skipped_tx_decode={}\n",
            "skipped_metadata_decode={}\n",
            "skipped_unresolved_programs={}\n",
            "skipped_unresolved_accounts={}\n",
            "skipped_missing_registry_ids={}\n",
            "read_bytes={}\n",
            "decoded_bytes={}\n",
            "events_file_bytes={}\n",
            "wallets_file_bytes={}\n",
            "token_accounts_file_bytes={}\n",
            "signatures_file_bytes={}\n",
            "total_output_bytes={}\n",
            "bytes_per_event={:.2}\n",
            "elapsed_s={:.3}\n",
            "blocks_s={:.2}\n",
            "tx_s={:.2}\n",
            "event_s={:.2}\n",
            "read_MiB_s={:.2}\n",
            "decoded_MiB_s={:.2}\n"
        ),
        EVENT_RECORD_BYTES,
        config.input.display(),
        input_file_bytes,
        input_format,
        if config.blockzilla_account_ids {
            "blockzilla"
        } else {
            "local"
        },
        config.workers.max(1),
        config.mint,
        stats.blocks,
        stats.txs,
        stats.txs_with_events,
        stats.events,
        stats.create_events,
        stats.transfer_events,
        stats.mint_events,
        stats.burn_events,
        stats.close_events,
        registries.wallets.rows.len(),
        registries.token_accounts.rows.len(),
        registries.signatures.rows.len(),
        stats.skipped_raw_txs,
        stats.skipped_raw_metadata,
        stats.skipped_tx_decode,
        stats.skipped_metadata_decode,
        stats.skipped_unresolved_programs,
        stats.skipped_unresolved_accounts,
        stats.skipped_missing_registry_ids,
        stats.read_bytes,
        stats.decoded_bytes,
        events_file_bytes,
        wallets_file_bytes,
        token_accounts_file_bytes,
        signatures_file_bytes,
        total_output_bytes,
        bytes_per_event,
        elapsed,
        rate(stats.blocks, elapsed),
        rate(stats.txs, elapsed),
        rate(stats.events, elapsed),
        mib_rate(stats.read_bytes, elapsed),
        mib_rate(stats.decoded_bytes, elapsed),
    );
    std::fs::write(config.output_dir.join("meta.txt"), meta)?;
    info!(
        "USDC token event dump complete in {:.2}s: format={:?} blocks={} txs={} events={} wallets={} token_accounts={} signatures={} events_file={}",
        elapsed,
        input_format,
        stats.blocks,
        stats.txs,
        stats.events,
        registries.wallets.rows.len(),
        registries.token_accounts.rows.len(),
        registries.signatures.rows.len(),
        events_path.display(),
    );
    println!(
        "usdc_token_events format={input_format:?} account_id_mode={} workers={} blocks={} txs={} events={} wallets={} token_accounts={} signatures={} elapsed_s={elapsed:.3} blocks_s={:.2} tx_s={:.2} event_s={:.2} input_file_bytes={input_file_bytes} read_MiB_s={:.2} decoded_MiB_s={:.2} total_output_bytes={total_output_bytes} bytes_per_event={bytes_per_event:.2}",
        if config.blockzilla_account_ids {
            "blockzilla"
        } else {
            "local"
        },
        config.workers.max(1),
        stats.blocks,
        stats.txs,
        stats.events,
        registries.wallets.rows.len(),
        registries.token_accounts.rows.len(),
        registries.signatures.rows.len(),
        rate(stats.blocks, elapsed),
        rate(stats.txs, elapsed),
        rate(stats.events, elapsed),
        mib_rate(stats.read_bytes, elapsed),
        mib_rate(stats.decoded_bytes, elapsed),
    );
    Ok(())
}

fn infer_format(input: &Path, format: TokenEventInputFormat) -> TokenEventInputFormat {
    if format != TokenEventInputFormat::Auto {
        return format;
    }
    let name = input
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    if name.ends_with(".car.zst") {
        TokenEventInputFormat::CarZstd
    } else if name.ends_with(".car") {
        TokenEventInputFormat::Car
    } else if name == ARCHIVE_V2_RAW_BLOCKS_ZSTD_FILE || name.ends_with(".wincode.zst") {
        TokenEventInputFormat::HotRawZstd
    } else if name == ARCHIVE_V2_RAW_BLOCKS_FILE || name.ends_with(".wincode") {
        TokenEventInputFormat::HotRaw
    } else {
        TokenEventInputFormat::HotZstd
    }
}

fn load_key_index(path: &Path) -> Result<KeyIndex> {
    let store = KeyStore::load(path).with_context(|| {
        format!(
            "load Blockzilla account registry for event ids from {}",
            path.display()
        )
    })?;
    Ok(KeyIndex::build(store.keys))
}

fn token_program_id() -> [u8; 32] {
    static TOKEN_PROGRAM_ID: OnceLock<[u8; 32]> = OnceLock::new();
    *TOKEN_PROGRAM_ID.get_or_init(|| {
        Pubkey::from_str(TOKEN_PROGRAM_ID_STR)
            .expect("valid SPL Token program id")
            .to_bytes()
    })
}

fn token_2022_program_id() -> [u8; 32] {
    static TOKEN_2022_PROGRAM_ID: OnceLock<[u8; 32]> = OnceLock::new();
    *TOKEN_2022_PROGRAM_ID.get_or_init(|| {
        Pubkey::from_str(TOKEN_2022_PROGRAM_ID_STR)
            .expect("valid SPL Token-2022 program id")
            .to_bytes()
    })
}

#[allow(clippy::too_many_arguments)]
fn scan_car(
    input: &Path,
    compressed: bool,
    events: &mut BufWriter<File>,
    registries: &mut Registries,
    discovery: &mut Discovery,
    account_id_mode: &AccountIdMode,
    mint: [u8; 32],
    max_blocks: Option<u64>,
    stats: &mut DumpStats,
) -> Result<()> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    if compressed {
        let reader = BufReader::with_capacity(BUFFER_SIZE, file);
        let decoder = zstd::Decoder::with_buffer(reader)
            .with_context(|| format!("open zstd {}", input.display()))?;
        scan_car_reader(
            decoder,
            true,
            events,
            registries,
            discovery,
            account_id_mode,
            mint,
            max_blocks,
            stats,
        )
    } else {
        let reader = BufReader::with_capacity(BUFFER_SIZE, file);
        scan_car_reader(
            reader,
            false,
            events,
            registries,
            discovery,
            account_id_mode,
            mint,
            max_blocks,
            stats,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn scan_car_reader<R: Read>(
    reader: R,
    compressed: bool,
    events: &mut BufWriter<File>,
    registries: &mut Registries,
    discovery: &mut Discovery,
    account_id_mode: &AccountIdMode,
    mint: [u8; 32],
    max_blocks: Option<u64>,
    stats: &mut DumpStats,
) -> Result<()> {
    let mut reader = CarBlockReader::with_capacity(reader, BUFFER_SIZE);
    reader.skip_header().context("skip CAR header")?;

    let mut scratch = Vec::with_capacity(1 << 20);
    let mut zstd = ZstdReusableDecoder::new();
    let mut owned_meta = TransactionStatusMeta::default();
    let mut signature_ordinal = 0u64;
    let mut progress = ProgressTracker::new(if compressed {
        "USDC Token Events Car-Zstd"
    } else {
        "USDC Token Events Car"
    });
    let block_limit = max_blocks.unwrap_or(u64::MAX);

    while let Some(entry) = reader
        .read_entry_payload_with_scratch(&mut scratch)
        .context("read CAR entry")?
    {
        stats.read_bytes = reader.offset;
        stats.decoded_bytes = reader.offset;
        let node_kind = peek_node_type(entry.payload).context("peek node type")?;
        if node_kind == 2 {
            if let Node::Block(block) = decode_node(entry.payload).context("decode block node")? {
                stats.blocks += 1;
                progress.update_slot(block.slot);
                progress.update(1, 0);
                if stats.blocks >= block_limit {
                    break;
                }
            }
            continue;
        }
        if node_kind != 0 {
            continue;
        }

        let Node::Transaction(tx) =
            decode_node(entry.payload).context("decode transaction node")?
        else {
            continue;
        };
        stats.txs += 1;
        progress.update_slot(tx.slot);
        progress.update(0, 1);

        if tx.data.next.is_some() {
            stats.skipped_raw_txs += 1;
            continue;
        }
        let versioned_tx = match wincode::deserialize::<VersionedTransaction<'_>>(tx.data.data) {
            Ok(tx) => tx,
            Err(_) => {
                stats.skipped_tx_decode += 1;
                continue;
            }
        };
        let Some(signature) = versioned_tx.signatures.first().copied() else {
            stats.skipped_tx_decode += 1;
            continue;
        };
        let tx_signature_ordinal = signature_ordinal;
        signature_ordinal = signature_ordinal.saturating_add(versioned_tx.signatures.len() as u64);

        if tx.metadata.next.is_some() || tx.metadata.data.is_empty() {
            stats.skipped_raw_metadata += u64::from(tx.metadata.next.is_some());
            let meta = CarMeta::default();
            scan_car_tx(
                &versioned_tx,
                &meta,
                *signature,
                tx_signature_ordinal,
                tx.slot,
                tx.index,
                events,
                registries,
                discovery,
                account_id_mode,
                mint,
                stats,
            )?;
            continue;
        }

        let metadata_bytes = match zstd.decompress_if_zstd(tx.metadata.data) {
            Ok(true) => zstd.output(),
            Ok(false) => tx.metadata.data,
            Err(_) => {
                stats.skipped_metadata_decode += 1;
                continue;
            }
        };

        if slot_uses_protobuf_metadata(tx.slot) {
            let mut meta = CarMeta::default();
            if visit_protobuf_transaction_status_meta(metadata_bytes, &mut meta).is_err() {
                stats.skipped_metadata_decode += 1;
                continue;
            }
            scan_car_tx(
                &versioned_tx,
                &meta,
                *signature,
                tx_signature_ordinal,
                tx.slot,
                tx.index,
                events,
                registries,
                discovery,
                account_id_mode,
                mint,
                stats,
            )?;
        } else {
            owned_meta.clear();
            if decode_transaction_status_meta_into(tx.slot, metadata_bytes, &mut owned_meta)
                .is_err()
            {
                stats.skipped_metadata_decode += 1;
                continue;
            }
            let meta = CarMeta::from_owned(&owned_meta);
            scan_car_tx(
                &versioned_tx,
                &meta,
                *signature,
                tx_signature_ordinal,
                tx.slot,
                tx.index,
                events,
                registries,
                discovery,
                account_id_mode,
                mint,
                stats,
            )?;
        }
    }
    progress.final_report();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_car_tx(
    tx: &VersionedTransaction<'_>,
    meta: &CarMeta<'_>,
    signature: [u8; 64],
    signature_ordinal: u64,
    slot: u64,
    tx_index: Option<u64>,
    events: &mut BufWriter<File>,
    registries: &mut Registries,
    discovery: &mut Discovery,
    account_id_mode: &AccountIdMode,
    mint: [u8; 32],
    stats: &mut DumpStats,
) -> Result<()> {
    let full_keys = full_car_message_keys(tx, meta, stats);
    let account_mints = collect_car_account_mints(meta, &full_keys, mint, discovery, stats);
    let mut tx_had_event = false;
    let mut signature_id = None;

    for instruction in car_instruction_refs(tx, meta)? {
        let Some(program) = full_keys
            .get(instruction.program_id_index as usize)
            .copied()
            .flatten()
        else {
            stats.skipped_unresolved_programs += 1;
            continue;
        };
        let token_program = if program == token_program_id() {
            TokenProgramKind::Token
        } else if program == token_2022_program_id() {
            TokenProgramKind::Token2022
        } else {
            continue;
        };
        let Some(decoded) = decode_token_instruction(instruction.accounts, instruction.data) else {
            continue;
        };
        let Some(event) = materialize_token_event(
            decoded,
            instruction,
            token_program,
            &full_keys,
            &account_mints,
            mint,
            discovery,
            meta.tx_error,
            stats,
        )?
        else {
            continue;
        };
        let signature_id = match signature_id {
            Some(id) => id,
            None => {
                let id = event_signature_id(
                    account_id_mode,
                    registries,
                    Some(signature),
                    signature_ordinal,
                )?;
                signature_id = Some(id);
                id
            }
        };
        let wallet_id = event
            .wallet
            .map(|key| account_id_mode.account_id(key, &mut registries.wallets, stats))
            .transpose()?
            .unwrap_or(NO_ID);
        let token_account_id = account_id_mode.account_id(
            event.token_account,
            &mut registries.token_accounts,
            stats,
        )?;
        let counterparty_token_account_id = event
            .counterparty_token_account
            .map(|key| account_id_mode.account_id(key, &mut registries.token_accounts, stats))
            .transpose()?
            .unwrap_or(NO_ID);
        let record = EventRecord {
            slot,
            tx_index: tx_index
                .and_then(|index| u32::try_from(index).ok())
                .unwrap_or(NO_ID),
            outer_ix: instruction.outer_ix,
            inner_ix: instruction.inner_ix,
            event_kind: event.kind,
            token_program,
            flags: event.flags,
            signature_id,
            wallet_id,
            token_account_id,
            counterparty_token_account_id,
            amount: event.amount,
            raw_tag: event.raw_tag,
        };
        events.write_all(&record.encode())?;
        tx_had_event = true;
        stats.events += 1;
        match event.kind {
            TokenEventKind::CreateAccount => stats.create_events += 1,
            TokenEventKind::Transfer => stats.transfer_events += 1,
            TokenEventKind::Mint => stats.mint_events += 1,
            TokenEventKind::Burn => stats.burn_events += 1,
            TokenEventKind::Close => stats.close_events += 1,
        }
    }
    if tx_had_event {
        stats.txs_with_events += 1;
    }
    Ok(())
}

fn full_car_message_keys(
    tx: &VersionedTransaction<'_>,
    meta: &CarMeta<'_>,
    stats: &mut DumpStats,
) -> Vec<Option<[u8; 32]>> {
    let mut out = Vec::new();
    match &tx.message {
        VersionedMessage::Legacy(message) => {
            out.extend(message.account_keys.iter().map(|key| Some(**key)));
        }
        VersionedMessage::V0(message) => {
            out.extend(message.account_keys.iter().map(|key| Some(**key)));
            out.extend(
                meta.loaded_writable_addresses
                    .iter()
                    .map(|key| car_pubkey_bytes(key.as_ref(), stats)),
            );
            out.extend(
                meta.loaded_readonly_addresses
                    .iter()
                    .map(|key| car_pubkey_bytes(key.as_ref(), stats)),
            );
        }
    }
    out
}

fn car_pubkey_bytes(bytes: &[u8], stats: &mut DumpStats) -> Option<[u8; 32]> {
    match <[u8; 32]>::try_from(bytes) {
        Ok(key) => Some(key),
        Err(_) => {
            stats.skipped_unresolved_accounts += 1;
            None
        }
    }
}

fn collect_car_account_mints(
    meta: &CarMeta<'_>,
    full_keys: &[Option<[u8; 32]>],
    mint: [u8; 32],
    discovery: &mut Discovery,
    _stats: &mut DumpStats,
) -> HashMap<u32, [u8; 32]> {
    let mut out = HashMap::new();
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        let Some(balance_mint) = parse_pubkey_bytes(balance.mint.as_ref()) else {
            continue;
        };
        out.insert(balance.account_index, balance_mint);
        if balance_mint != mint {
            continue;
        }
        if let Some(Some(account)) = full_keys.get(balance.account_index as usize) {
            discovery.token_accounts.insert(*account);
            if let Some(owner) = parse_pubkey_bytes(balance.owner.as_ref()) {
                discovery.token_account_owner.insert(*account, owner);
            }
        }
    }
    out
}

fn parse_pubkey_bytes(value: &str) -> Option<[u8; 32]> {
    if value.is_empty() {
        return None;
    }
    Pubkey::from_str(value).ok().map(|key| key.to_bytes())
}

fn event_signature_id(
    account_id_mode: &AccountIdMode,
    registries: &mut Registries,
    signature: Option<[u8; 64]>,
    signature_ordinal: u64,
) -> Result<u32> {
    if account_id_mode.is_blockzilla() {
        return signature_ordinal_id(signature_ordinal);
    }
    let signature = signature.context("local signature registry requires signature bytes")?;
    registries.signatures.id(signature)
}

fn signature_ordinal_id(signature_ordinal: u64) -> Result<u32> {
    u32::try_from(signature_ordinal).context("signature ordinal exceeds u32::MAX")
}

fn car_instruction_refs<'a>(
    tx: &'a VersionedTransaction<'a>,
    meta: &'a CarMeta<'a>,
) -> Result<Vec<InstructionRef<'a>>> {
    let outer = match &tx.message {
        VersionedMessage::Legacy(message) => &message.instructions,
        VersionedMessage::V0(message) => &message.instructions,
    };
    let mut out = Vec::new();
    for (outer_ix, instruction) in outer.iter().enumerate() {
        out.push(car_outer_instruction_ref(
            u16::try_from(outer_ix).context("outer instruction index exceeds u16")?,
            instruction,
        ));
        for inner in meta
            .inner_instructions
            .iter()
            .filter(|inner| inner.outer_instruction_index as usize == outer_ix)
        {
            out.push(car_inner_instruction_ref(inner)?);
        }
    }
    for inner in meta
        .inner_instructions
        .iter()
        .filter(|inner| inner.outer_instruction_index as usize >= outer.len())
    {
        out.push(car_inner_instruction_ref(inner)?);
    }
    Ok(out)
}

fn car_outer_instruction_ref<'a>(
    outer_ix: u16,
    instruction: &'a CompiledInstruction,
) -> InstructionRef<'a> {
    InstructionRef {
        outer_ix,
        inner_ix: NO_INNER_IX,
        program_id_index: instruction.program_id_index as u32,
        accounts: &instruction.accounts,
        data: &instruction.data,
    }
}

fn car_inner_instruction_ref<'a>(
    instruction: &'a CarInnerInstruction<'a>,
) -> Result<InstructionRef<'a>> {
    Ok(InstructionRef {
        outer_ix: u16::try_from(instruction.outer_instruction_index)
            .context("outer instruction index exceeds u16")?,
        inner_ix: u16::try_from(instruction.inner_instruction_index)
            .context("inner instruction index exceeds u16")?,
        program_id_index: instruction.program_id_index,
        accounts: instruction.accounts.as_ref(),
        data: instruction.data.as_ref(),
    })
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_zstd(
    input: &Path,
    index_path: &Path,
    store: &KeyStore,
    signature_file: &mut File,
    events: &mut BufWriter<File>,
    registries: &mut Registries,
    discovery: &mut Discovery,
    account_id_mode: &AccountIdMode,
    mint: [u8; 32],
    max_blocks: Option<u64>,
    stats: &mut DumpStats,
) -> Result<()> {
    let index = read_archive_v2_hot_block_index(index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS == 0,
        "{} is a raw-block index, not independent zstd blocks",
        index_path.display()
    );
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY == 0,
        "dictionary-compressed hot blocks are not supported by this token dumper yet"
    );
    let file_bytes = std::fs::metadata(input)?.len();
    anyhow::ensure!(
        file_bytes == index.blob_file_bytes,
        "index was built for {} bytes, input has {} bytes",
        index.blob_file_bytes,
        file_bytes
    );
    let mut file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut compressed = Vec::with_capacity(2 << 20);
    let mut decompressor = zstd::bulk::Decompressor::new()?;
    let mut current_offset = 0u64;
    let mut progress = ProgressTracker::new("USDC Token Events Hot-Zstd");
    let limit = max_blocks
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(usize::MAX);

    for row in index.rows.iter().take(limit) {
        if current_offset != row.compressed_offset {
            file.seek(SeekFrom::Start(row.compressed_offset))?;
            current_offset = row.compressed_offset;
        }
        compressed.resize(row.compressed_len as usize, 0);
        file.read_exact(&mut compressed)?;
        current_offset += row.compressed_len as u64;
        let block_bytes = decompressor.decompress(&compressed, row.uncompressed_len as usize)?;
        stats.read_bytes += row.compressed_len as u64;
        stats.decoded_bytes += block_bytes.len() as u64;
        scan_hot_block_bytes(
            &block_bytes,
            row.block_id,
            row.slot,
            row.first_signature_ordinal,
            store,
            signature_file,
            events,
            registries,
            discovery,
            account_id_mode,
            mint,
            stats,
        )?;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }
    progress.final_report();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_raw(
    input: &Path,
    index_path: &Path,
    whole_zstd: bool,
    store: &KeyStore,
    signature_file: &mut File,
    events: &mut BufWriter<File>,
    registries: &mut Registries,
    discovery: &mut Discovery,
    account_id_mode: &AccountIdMode,
    mint: [u8; 32],
    max_blocks: Option<u64>,
    stats: &mut DumpStats,
) -> Result<()> {
    let index = read_archive_v2_hot_block_index(index_path)?;
    anyhow::ensure!(
        index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
        "{} is not a raw-block index",
        index_path.display()
    );
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let input_bytes = file.metadata()?.len();
    if !whole_zstd {
        anyhow::ensure!(
            input_bytes == index.blob_file_bytes,
            "index was built for {} raw bytes, input has {} bytes",
            index.blob_file_bytes,
            input_bytes
        );
    }
    let reader: Box<dyn Read> = if whole_zstd {
        Box::new(zstd::stream::read::Decoder::new(BufReader::with_capacity(
            BUFFER_SIZE,
            file,
        ))?)
    } else {
        Box::new(BufReader::with_capacity(BUFFER_SIZE, file))
    };
    let mut reader = reader;
    let mut block_bytes = Vec::with_capacity(2 << 20);
    let mut progress = ProgressTracker::new(if whole_zstd {
        "USDC Token Events Hot-Raw-Zstd"
    } else {
        "USDC Token Events Hot-Raw"
    });
    let limit = max_blocks
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(usize::MAX);

    for row in index.rows.iter().take(limit) {
        block_bytes.resize(row.uncompressed_len as usize, 0);
        reader.read_exact(&mut block_bytes).with_context(|| {
            format!(
                "read raw block_id {} at decompressed offset {}",
                row.block_id, row.compressed_offset
            )
        })?;
        stats.read_bytes += row.uncompressed_len as u64;
        stats.decoded_bytes += row.uncompressed_len as u64;
        scan_hot_block_bytes(
            &block_bytes,
            row.block_id,
            row.slot,
            row.first_signature_ordinal,
            store,
            signature_file,
            events,
            registries,
            discovery,
            account_id_mode,
            mint,
            stats,
        )?;
        progress.update_slot(row.slot);
        progress.update(1, row.tx_count as u64);
    }
    progress.final_report();
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum HotKeyRef {
    Id(u32),
    Raw([u8; 32]),
}

impl HotKeyRef {
    fn from_compact(key: CompactPubkey) -> Self {
        match key {
            CompactPubkey::Id(id) => Self::Id(id),
            CompactPubkey::Raw(key) => Self::Raw(key),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct KnownHotKey {
    raw: [u8; 32],
    id: Option<u32>,
}

impl KnownHotKey {
    fn matches(self, key: HotKeyRef) -> bool {
        match key {
            HotKeyRef::Id(id) => self.id == Some(id),
            HotKeyRef::Raw(raw) => self.raw == raw,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HotKnownKeys {
    token_program: KnownHotKey,
    token_2022_program: KnownHotKey,
    mint: KnownHotKey,
}

impl HotKnownKeys {
    fn new(account_id_mode: &AccountIdMode, mint: [u8; 32]) -> Self {
        Self {
            token_program: account_id_mode.known_key(token_program_id()),
            token_2022_program: account_id_mode.known_key(token_2022_program_id()),
            mint: account_id_mode.known_key(mint),
        }
    }
}

#[derive(Debug, Default)]
struct HotDiscovery {
    token_accounts: HashSet<u32>,
    token_account_owner: HashMap<u32, u32>,
}

#[derive(Clone)]
struct FrozenHotDiscovery {
    token_accounts: Arc<HashSet<u32>>,
    token_account_owner: Arc<HashMap<u32, u32>>,
}

impl From<HotDiscovery> for FrozenHotDiscovery {
    fn from(value: HotDiscovery) -> Self {
        Self {
            token_accounts: Arc::new(value.token_accounts),
            token_account_owner: Arc::new(value.token_account_owner),
        }
    }
}

#[derive(Debug)]
struct EventPart {
    start_row: usize,
    path: PathBuf,
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_parallel(
    input: &Path,
    index_path: &Path,
    input_format: TokenEventInputFormat,
    account_id_mode: Arc<AccountIdMode>,
    events: &mut BufWriter<File>,
    mint: [u8; 32],
    max_blocks: Option<u64>,
    workers: usize,
    chunk_size: usize,
    output_dir: &Path,
    stats: &mut DumpStats,
) -> Result<()> {
    anyhow::ensure!(
        account_id_mode.is_blockzilla(),
        "parallel hot token scan requires Blockzilla registry account ids"
    );
    let raw_blocks = match input_format {
        TokenEventInputFormat::HotZstd => false,
        TokenEventInputFormat::HotRaw => true,
        _ => anyhow::bail!("parallel hot token scan supports hot-zstd and hot-raw inputs only"),
    };
    let index = read_archive_v2_hot_block_index(index_path)?;
    if raw_blocks {
        anyhow::ensure!(
            index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
            "{} is not a raw-block index",
            index_path.display()
        );
    } else {
        anyhow::ensure!(
            index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS == 0,
            "{} is a raw-block index, not independent zstd blocks",
            index_path.display()
        );
        anyhow::ensure!(
            index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY == 0,
            "dictionary-compressed hot blocks are not supported by this token dumper yet"
        );
    }
    let file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        file_bytes == index.blob_file_bytes,
        "index was built for {} bytes, input has {} bytes",
        index.blob_file_bytes,
        file_bytes
    );
    let limit = max_blocks
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(index.rows.len())
        .min(index.rows.len());
    let rows = index.rows[..limit].to_vec();

    let started = Instant::now();
    let known = HotKnownKeys::new(&account_id_mode, mint);
    let mut discovery_stats = DumpStats::default();
    let discovery = discover_hot_accounts(
        input,
        raw_blocks,
        &rows,
        known,
        &account_id_mode,
        &mut discovery_stats,
    )?;
    stats.read_bytes += discovery_stats.read_bytes;
    stats.decoded_bytes += discovery_stats.decoded_bytes;
    stats.skipped_raw_txs += discovery_stats.skipped_raw_txs;
    stats.skipped_raw_metadata += discovery_stats.skipped_raw_metadata;
    stats.skipped_unresolved_programs += discovery_stats.skipped_unresolved_programs;
    stats.skipped_unresolved_accounts += discovery_stats.skipped_unresolved_accounts;
    let frozen = Arc::new(FrozenHotDiscovery::from(discovery));

    let rows = Arc::new(rows);
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let next = Arc::new(AtomicUsize::new(0));
    let parts_dir = output_dir.join("events.parts.tmp");
    std::fs::create_dir_all(&parts_dir)
        .with_context(|| format!("create {}", parts_dir.display()))?;
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let rows = Arc::clone(&rows);
        let file = Arc::clone(&file);
        let account_id_mode = Arc::clone(&account_id_mode);
        let frozen = Arc::clone(&frozen);
        let next = Arc::clone(&next);
        let parts_dir = parts_dir.clone();
        handles.push(thread::spawn(
            move || -> Result<(DumpStats, Vec<EventPart>)> {
                let mut worker_stats = DumpStats::default();
                let mut parts = Vec::new();
                let mut compressed = Vec::with_capacity(2 << 20);
                let mut block_bytes = Vec::with_capacity(2 << 20);
                let mut decompressor = zstd::bulk::Decompressor::new()?;
                loop {
                    let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if start >= rows.len() {
                        break;
                    }
                    let end = start.saturating_add(chunk_size).min(rows.len());
                    let mut chunk_events = Vec::new();
                    for row in &rows[start..end] {
                        read_hot_row_bytes(
                            &file,
                            row,
                            raw_blocks,
                            &mut compressed,
                            &mut block_bytes,
                            &mut decompressor,
                        )
                        .with_context(|| {
                            format!("worker {worker_id} read hot block_id {}", row.block_id)
                        })?;
                        worker_stats.read_bytes += if raw_blocks {
                            row.uncompressed_len as u64
                        } else {
                            row.compressed_len as u64
                        };
                        worker_stats.decoded_bytes += row.uncompressed_len as u64;
                        scan_hot_block_bytes_frozen(
                            &block_bytes,
                            row.block_id,
                            row.slot,
                            row.first_signature_ordinal,
                            &account_id_mode,
                            &frozen,
                            known,
                            &mut worker_stats,
                            &mut chunk_events,
                        )?;
                    }
                    if !chunk_events.is_empty() {
                        let path = parts_dir.join(format!("events.part.{start:010}.bin"));
                        std::fs::write(&path, &chunk_events)
                            .with_context(|| format!("write {}", path.display()))?;
                        parts.push(EventPart {
                            start_row: start,
                            path,
                        });
                    }
                }
                Ok((worker_stats, parts))
            },
        ));
    }

    let mut parts = Vec::new();
    for handle in handles {
        let (worker_stats, mut worker_parts) = handle
            .join()
            .map_err(|_| anyhow!("parallel hot token scan worker panicked"))??;
        stats.merge(worker_stats);
        parts.append(&mut worker_parts);
    }
    parts.sort_by_key(|part| part.start_row);
    for part in parts {
        let mut file =
            File::open(&part.path).with_context(|| format!("open {}", part.path.display()))?;
        std::io::copy(&mut file, events)
            .with_context(|| format!("merge {}", part.path.display()))?;
        let _ = std::fs::remove_file(&part.path);
    }
    let _ = std::fs::remove_dir(&parts_dir);
    let elapsed = started.elapsed().as_secs_f64();
    info!(
        "USDC hot parallel scan complete in {:.2}s: workers={} chunk_size={} blocks={} txs={} events={} read_MiB_s={:.2} decoded_MiB_s={:.2}",
        elapsed,
        workers,
        chunk_size,
        stats.blocks,
        stats.txs,
        stats.events,
        mib_rate(stats.read_bytes, elapsed),
        mib_rate(stats.decoded_bytes, elapsed),
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_token_instructions_parallel(
    input: &Path,
    index_path: &Path,
    raw_blocks: bool,
    account_id_mode: Arc<AccountIdMode>,
    known: HotKnownKeys,
    mint_filter: Option<KnownHotKey>,
    instructions: Option<&mut BufWriter<File>>,
    max_blocks: Option<u64>,
    workers: usize,
    chunk_size: usize,
    no_output: bool,
    outer_only: bool,
    output_dir: &Path,
    stats: &mut TokenInstructionStats,
) -> Result<()> {
    let index = read_archive_v2_hot_block_index(index_path)?;
    if raw_blocks {
        anyhow::ensure!(
            index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS != 0,
            "{} is not a raw-block index",
            index_path.display()
        );
    } else {
        anyhow::ensure!(
            index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_RAW_BLOCKS == 0,
            "{} is a raw-block index, not independent zstd blocks",
            index_path.display()
        );
        anyhow::ensure!(
            index.flags & ARCHIVE_V2_HOT_INDEX_FLAG_DICTIONARY == 0,
            "dictionary-compressed hot blocks are not supported by this token instruction dumper yet"
        );
    }
    let file_bytes = std::fs::metadata(input)
        .with_context(|| format!("stat {}", input.display()))?
        .len();
    anyhow::ensure!(
        file_bytes == index.blob_file_bytes,
        "index was built for {} bytes, input has {} bytes",
        index.blob_file_bytes,
        file_bytes
    );
    let limit = max_blocks
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(index.rows.len())
        .min(index.rows.len());
    let rows = Arc::new(index.rows[..limit].to_vec());
    let file = Arc::new(File::open(input).with_context(|| format!("open {}", input.display()))?);
    let workers = workers.max(1);
    let chunk_size = chunk_size.max(1);
    let next = Arc::new(AtomicUsize::new(0));
    let parts_dir = output_dir.join("token-instructions.parts.tmp");
    if !no_output {
        std::fs::create_dir_all(&parts_dir)
            .with_context(|| format!("create {}", parts_dir.display()))?;
    }
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers);

    for worker_id in 0..workers {
        let rows = Arc::clone(&rows);
        let file = Arc::clone(&file);
        let account_id_mode = Arc::clone(&account_id_mode);
        let next = Arc::clone(&next);
        let parts_dir = parts_dir.clone();
        handles.push(thread::spawn(
            move || -> Result<(TokenInstructionStats, Vec<EventPart>)> {
                let mut worker_stats = TokenInstructionStats::default();
                let mut parts = Vec::new();
                let mut compressed = Vec::with_capacity(2 << 20);
                let mut block_bytes = Vec::with_capacity(2 << 20);
                let mut decompressor = zstd::bulk::Decompressor::new()?;
                loop {
                    let start = next.fetch_add(chunk_size, Ordering::Relaxed);
                    if start >= rows.len() {
                        break;
                    }
                    let end = start.saturating_add(chunk_size).min(rows.len());
                    let mut chunk_out = Vec::new();
                    for row in &rows[start..end] {
                        read_hot_row_bytes(
                            &file,
                            row,
                            raw_blocks,
                            &mut compressed,
                            &mut block_bytes,
                            &mut decompressor,
                        )
                        .with_context(|| {
                            format!(
                                "worker {worker_id} read hot instruction block_id {}",
                                row.block_id
                            )
                        })?;
                        worker_stats.read_bytes += if raw_blocks {
                            row.uncompressed_len as u64
                        } else {
                            row.compressed_len as u64
                        };
                        worker_stats.decoded_bytes += row.uncompressed_len as u64;
                        scan_hot_token_instruction_block_bytes(
                            &block_bytes,
                            row.block_id,
                            row.slot,
                            row.first_signature_ordinal,
                            &account_id_mode,
                            known,
                            mint_filter,
                            no_output,
                            outer_only,
                            &mut worker_stats,
                            &mut chunk_out,
                        )?;
                    }
                    if !no_output && !chunk_out.is_empty() {
                        let path = parts_dir.join(format!("token-ix.part.{start:010}.bin"));
                        std::fs::write(&path, &chunk_out)
                            .with_context(|| format!("write {}", path.display()))?;
                        parts.push(EventPart {
                            start_row: start,
                            path,
                        });
                    }
                }
                Ok((worker_stats, parts))
            },
        ));
    }

    let mut parts = Vec::new();
    for handle in handles {
        let (worker_stats, mut worker_parts) = handle
            .join()
            .map_err(|_| anyhow!("parallel hot token instruction worker panicked"))??;
        stats.merge(worker_stats);
        parts.append(&mut worker_parts);
    }
    parts.sort_by_key(|part| part.start_row);
    if let Some(instructions) = instructions {
        for part in parts {
            let mut file =
                File::open(&part.path).with_context(|| format!("open {}", part.path.display()))?;
            std::io::copy(&mut file, instructions)
                .with_context(|| format!("merge {}", part.path.display()))?;
            let _ = std::fs::remove_file(&part.path);
        }
    }
    if !no_output {
        let _ = std::fs::remove_dir(&parts_dir);
    }

    let elapsed = started.elapsed().as_secs_f64();
    info!(
        "token instruction hot scan complete in {:.2}s: workers={} chunk_size={} no_output={} outer_only={} blocks={} txs={} outer_ixs={} inner_ixs={} token_program_ixs={} decoded_token_ixs={} unknown_token_ixs={} token_ixs={} records_encoded={} token_event_kind_counts={} token_instruction_tag_counts={} metadata_needed={} metadata_decoded={} read_MiB_s={:.2} decoded_MiB_s={:.2}",
        elapsed,
        workers,
        chunk_size,
        no_output,
        outer_only,
        stats.blocks,
        stats.txs,
        stats.outer_ixs,
        stats.inner_ixs,
        stats.token_program_ixs,
        stats.decoded_token_ixs,
        stats.unknown_token_ixs,
        stats.token_ixs,
        stats.records_encoded,
        stats.token_event_kind_counts_summary(),
        stats.token_instruction_tag_counts_summary(),
        stats.metadata_needed,
        stats.metadata_decoded,
        mib_rate(stats.read_bytes, elapsed),
        mib_rate(stats.decoded_bytes, elapsed),
    );
    Ok(())
}

fn read_hot_row_bytes(
    file: &File,
    row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
    raw_blocks: bool,
    compressed: &mut Vec<u8>,
    block_bytes: &mut Vec<u8>,
    decompressor: &mut zstd::bulk::Decompressor<'_>,
) -> Result<()> {
    if raw_blocks {
        block_bytes.resize(row.uncompressed_len as usize, 0);
        read_exact_at(file, block_bytes, row.compressed_offset)?;
        return Ok(());
    }
    compressed.resize(row.compressed_len as usize, 0);
    read_exact_at(file, compressed, row.compressed_offset)?;
    let decoded = decompressor
        .decompress(compressed, row.uncompressed_len as usize)
        .with_context(|| format!("zstd decompress hot block_id {}", row.block_id))?;
    anyhow::ensure!(
        decoded.len() == row.uncompressed_len as usize,
        "block_id {} decompressed length mismatch: decoded={} index={}",
        row.block_id,
        decoded.len(),
        row.uncompressed_len
    );
    block_bytes.clear();
    block_bytes.extend_from_slice(&decoded);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_token_instruction_block_bytes(
    block_bytes: &[u8],
    block_id: u32,
    slot: u64,
    first_signature_ordinal: u64,
    account_id_mode: &AccountIdMode,
    known: HotKnownKeys,
    mint_filter: Option<KnownHotKey>,
    no_output: bool,
    outer_only: bool,
    stats: &mut TokenInstructionStats,
    out: &mut Vec<u8>,
) -> Result<()> {
    let block: ArchiveV2HotBlockBlob =
        wincode::config::deserialize(block_bytes, wincode_leb128_config())
            .with_context(|| format!("decode hot block_id {block_id}"))?;
    anyhow::ensure!(
        block.header.slot == slot,
        "hot block_id {} slot mismatch: decoded={} index={}",
        block_id,
        block.header.slot,
        slot
    );
    stats.blocks += 1;
    stats.txs += block.tx_rows.len() as u64;
    let mut signature_ordinal = first_signature_ordinal;
    for tx_row in &block.tx_rows {
        let tx_signature_ordinal = signature_ordinal;
        signature_ordinal += tx_row.signature_count as u64;
        scan_hot_token_instruction_tx(
            &block,
            tx_row,
            block_id,
            slot,
            tx_signature_ordinal,
            account_id_mode,
            known,
            mint_filter,
            no_output,
            outer_only,
            stats,
            out,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_token_instruction_tx(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    slot: u64,
    signature_ordinal: u64,
    account_id_mode: &AccountIdMode,
    known: HotKnownKeys,
    mint_filter: Option<KnownHotKey>,
    no_output: bool,
    outer_only: bool,
    stats: &mut TokenInstructionStats,
    out: &mut Vec<u8>,
) -> Result<()> {
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        stats.skipped_raw_txs += 1;
        return Ok(());
    }
    let message_slice = hot_region(
        &block.message_bytes,
        tx_row.message_offset,
        tx_row.message_len,
        "message",
        block_id,
        tx_row.tx_index,
    )?;
    let message: ArchiveV2HotMessagePayload =
        wincode::config::deserialize(message_slice, wincode_leb128_config()).with_context(
            || {
                format!(
                    "decode hot token instruction message block_id={} slot={} tx_index={}",
                    block_id, slot, tx_row.tx_index
                )
            },
        )?;
    let need_loaded = !outer_only && tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_LOADED_ADDRESSES != 0;
    let need_inner = !outer_only && tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_INNER_IX != 0;
    let need_meta = need_loaded || need_inner;
    if need_meta {
        stats.metadata_needed += 1;
        if need_loaded {
            stats.metadata_loaded_needed += 1;
        }
        if need_inner {
            stats.metadata_inner_needed += 1;
        }
    }
    let full_meta;
    let inner_meta;
    if need_loaded {
        full_meta = decode_hot_instruction_meta(block, tx_row, block_id, slot, stats)?;
        inner_meta = None;
    } else if need_inner {
        full_meta = None;
        inner_meta = decode_hot_instruction_inner_meta(block, tx_row, block_id, slot, stats)?;
    } else {
        full_meta = None;
        inner_meta = None;
    };
    let inner_groups = full_meta
        .as_ref()
        .and_then(|meta| meta.inner_instructions.as_deref())
        .or_else(|| {
            inner_meta
                .as_ref()
                .and_then(|meta| meta.inner_instructions.as_deref())
        });
    let keys = HotKeyLookup::new(&message, full_meta.as_ref());
    let tx_error = tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
    let signature_id = signature_ordinal_id(signature_ordinal)?;
    scan_hot_token_instruction_refs(
        &message,
        inner_groups,
        &keys,
        account_id_mode,
        known,
        mint_filter,
        slot,
        tx_row.tx_index,
        signature_id,
        tx_error,
        no_output,
        outer_only,
        stats,
        out,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_token_instruction_refs(
    message: &ArchiveV2HotMessagePayload,
    inner_groups: Option<&[CompactInnerInstructions]>,
    keys: &HotKeyLookup<'_>,
    account_id_mode: &AccountIdMode,
    known: HotKnownKeys,
    mint_filter: Option<KnownHotKey>,
    slot: u64,
    tx_index: u32,
    signature_id: u32,
    tx_error: bool,
    no_output: bool,
    outer_only: bool,
    stats: &mut TokenInstructionStats,
    out: &mut Vec<u8>,
) -> Result<()> {
    let outer = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => message.instructions.as_slice(),
        ArchiveV2HotMessagePayload::V0(message) => message.instructions.as_slice(),
    };
    let inner_groups = if outer_only { None } else { inner_groups };
    for (outer_ix, instruction) in outer.iter().enumerate() {
        let outer_ix_u16 =
            u16::try_from(outer_ix).context("outer instruction index exceeds u16")?;
        let instruction = instruction_ref(outer_ix_u16, NO_INNER_IX, instruction);
        scan_one_hot_token_instruction_ref(
            instruction,
            keys,
            account_id_mode,
            known,
            mint_filter,
            slot,
            tx_index,
            signature_id,
            tx_error,
            no_output,
            stats,
            out,
        )?;
        if let Some(groups) = inner_groups {
            for group in groups
                .iter()
                .filter(|group| group.index as usize == outer_ix)
            {
                for (inner_ix, inner) in group.instructions.iter().enumerate() {
                    let instruction = inner_instruction_ref(
                        outer_ix_u16,
                        u16::try_from(inner_ix).context("inner instruction index exceeds u16")?,
                        inner,
                    );
                    scan_one_hot_token_instruction_ref(
                        instruction,
                        keys,
                        account_id_mode,
                        known,
                        mint_filter,
                        slot,
                        tx_index,
                        signature_id,
                        tx_error,
                        no_output,
                        stats,
                        out,
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_one_hot_token_instruction_ref(
    instruction: InstructionRef<'_>,
    keys: &HotKeyLookup<'_>,
    account_id_mode: &AccountIdMode,
    known: HotKnownKeys,
    mint_filter: Option<KnownHotKey>,
    slot: u64,
    tx_index: u32,
    signature_id: u32,
    tx_error: bool,
    no_output: bool,
    stats: &mut TokenInstructionStats,
    out: &mut Vec<u8>,
) -> Result<()> {
    if instruction.inner_ix == NO_INNER_IX {
        stats.outer_ixs += 1;
    } else {
        stats.inner_ixs += 1;
    }
    let Some(program) = keys.get(instruction.program_id_index) else {
        stats.skipped_unresolved_programs += 1;
        return Ok(());
    };
    let token_program = if known.token_program.matches(program) {
        TokenProgramKind::Token
    } else if known.token_2022_program.matches(program) {
        TokenProgramKind::Token2022
    } else {
        return Ok(());
    };
    stats.record_token_program_instruction(token_program, instruction.data);
    let Some(decoded) = decode_token_instruction(instruction.accounts, instruction.data) else {
        stats.unknown_token_ixs += 1;
        return Ok(());
    };
    stats.record_decoded_token_instruction(decoded.kind);
    let Some(record) = token_instruction_record(
        decoded,
        instruction,
        token_program,
        keys,
        account_id_mode,
        mint_filter,
        slot,
        tx_index,
        signature_id,
        tx_error,
        stats,
    )?
    else {
        return Ok(());
    };
    stats.token_ixs += 1;
    if !no_output {
        out.extend_from_slice(&record.encode());
        stats.records_encoded += 1;
    }
    Ok(())
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
    while !buf.is_empty() {
        let n = file.read_at(buf, offset)?;
        anyhow::ensure!(n != 0, "unexpected EOF reading at offset {offset}");
        offset += n as u64;
        buf = &mut buf[n..];
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_exact_at(_file: &File, _buf: &mut [u8], _offset: u64) -> Result<()> {
    anyhow::bail!("parallel hot token scan requires Unix pread support")
}

fn discover_hot_accounts(
    input: &Path,
    raw_blocks: bool,
    rows: &[blockzilla_format::ArchiveV2HotBlockIndexRow],
    known: HotKnownKeys,
    account_id_mode: &AccountIdMode,
    stats: &mut DumpStats,
) -> Result<HotDiscovery> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let mut compressed = Vec::with_capacity(2 << 20);
    let mut block_bytes = Vec::with_capacity(2 << 20);
    let mut decompressor = zstd::bulk::Decompressor::new()?;
    let mut discovery = HotDiscovery::default();
    for row in rows {
        read_hot_row_bytes(
            &file,
            row,
            raw_blocks,
            &mut compressed,
            &mut block_bytes,
            &mut decompressor,
        )?;
        stats.read_bytes += if raw_blocks {
            row.uncompressed_len as u64
        } else {
            row.compressed_len as u64
        };
        stats.decoded_bytes += row.uncompressed_len as u64;
        discover_hot_block_bytes(
            &block_bytes,
            row.block_id,
            row.slot,
            known,
            account_id_mode,
            &mut discovery,
            stats,
        )?;
    }
    Ok(discovery)
}

fn discover_hot_block_bytes(
    block_bytes: &[u8],
    block_id: u32,
    slot: u64,
    known: HotKnownKeys,
    account_id_mode: &AccountIdMode,
    discovery: &mut HotDiscovery,
    stats: &mut DumpStats,
) -> Result<()> {
    let block: ArchiveV2HotBlockBlob =
        wincode::config::deserialize(block_bytes, wincode_leb128_config())
            .with_context(|| format!("decode hot block_id {block_id}"))?;
    anyhow::ensure!(
        block.header.slot == slot,
        "hot block_id {} slot mismatch: decoded={} index={}",
        block_id,
        block.header.slot,
        slot
    );
    for tx_row in &block.tx_rows {
        discover_hot_tx(
            &block,
            tx_row,
            block_id,
            slot,
            known,
            account_id_mode,
            discovery,
            stats,
        )?;
    }
    Ok(())
}

fn discover_hot_tx(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    slot: u64,
    known: HotKnownKeys,
    account_id_mode: &AccountIdMode,
    discovery: &mut HotDiscovery,
    stats: &mut DumpStats,
) -> Result<()> {
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        stats.skipped_raw_txs += 1;
        return Ok(());
    }
    let message_slice = hot_region(
        &block.message_bytes,
        tx_row.message_offset,
        tx_row.message_len,
        "message",
        block_id,
        tx_row.tx_index,
    )?;
    let message: ArchiveV2HotMessagePayload =
        wincode::config::deserialize(message_slice, wincode_leb128_config()).with_context(
            || {
                format!(
                    "decode hot message block_id={} slot={} tx_index={}",
                    block_id, slot, tx_row.tx_index
                )
            },
        )?;
    let meta = decode_hot_meta(block, tx_row, block_id, slot, stats)?;
    let full_keys = full_message_key_refs(&message, meta.as_ref());
    let account_mints = collect_hot_account_mints(meta.as_ref());
    discover_hot_token_balances(
        meta.as_ref(),
        &full_keys,
        known,
        account_id_mode,
        discovery,
        stats,
    )?;
    let tx_error = tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
    for instruction in hot_instruction_refs(&message, meta.as_ref())? {
        let Some(program) = full_keys
            .get(instruction.program_id_index as usize)
            .copied()
            .flatten()
        else {
            stats.skipped_unresolved_programs += 1;
            continue;
        };
        if !known.token_program.matches(program) && !known.token_2022_program.matches(program) {
            continue;
        }
        let Some(decoded) = decode_token_instruction(instruction.accounts, instruction.data) else {
            continue;
        };
        let _ = materialize_hot_event_discovery(
            decoded,
            instruction,
            &full_keys,
            &account_mints,
            known,
            account_id_mode,
            discovery,
            tx_error,
            stats,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_block_bytes_frozen(
    block_bytes: &[u8],
    block_id: u32,
    slot: u64,
    first_signature_ordinal: u64,
    account_id_mode: &AccountIdMode,
    discovery: &FrozenHotDiscovery,
    known: HotKnownKeys,
    stats: &mut DumpStats,
    out: &mut Vec<u8>,
) -> Result<()> {
    let block: ArchiveV2HotBlockBlob =
        wincode::config::deserialize(block_bytes, wincode_leb128_config())
            .with_context(|| format!("decode hot block_id {block_id}"))?;
    anyhow::ensure!(
        block.header.slot == slot,
        "hot block_id {} slot mismatch: decoded={} index={}",
        block_id,
        block.header.slot,
        slot
    );
    stats.blocks += 1;
    stats.txs += block.tx_rows.len() as u64;
    let mut signature_ordinal = first_signature_ordinal;
    for tx_row in &block.tx_rows {
        let tx_signature_ordinal = signature_ordinal;
        signature_ordinal += tx_row.signature_count as u64;
        scan_hot_tx_frozen(
            &block,
            tx_row,
            block_id,
            slot,
            tx_signature_ordinal,
            account_id_mode,
            discovery,
            known,
            stats,
            out,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_tx_frozen(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    slot: u64,
    signature_ordinal: u64,
    account_id_mode: &AccountIdMode,
    discovery: &FrozenHotDiscovery,
    known: HotKnownKeys,
    stats: &mut DumpStats,
    out: &mut Vec<u8>,
) -> Result<()> {
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        stats.skipped_raw_txs += 1;
        return Ok(());
    }
    let message_slice = hot_region(
        &block.message_bytes,
        tx_row.message_offset,
        tx_row.message_len,
        "message",
        block_id,
        tx_row.tx_index,
    )?;
    let message: ArchiveV2HotMessagePayload =
        wincode::config::deserialize(message_slice, wincode_leb128_config()).with_context(
            || {
                format!(
                    "decode hot message block_id={} slot={} tx_index={}",
                    block_id, slot, tx_row.tx_index
                )
            },
        )?;
    let meta = decode_hot_meta(block, tx_row, block_id, slot, stats)?;
    let full_keys = full_message_key_refs(&message, meta.as_ref());
    let account_mints = collect_hot_account_mints(meta.as_ref());
    let tx_error = tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
    let mut tx_had_event = false;
    anyhow::ensure!(
        account_id_mode.is_blockzilla(),
        "parallel hot token scan requires Blockzilla registry account ids"
    );
    let signature_id = signature_ordinal_id(signature_ordinal)?;
    for instruction in hot_instruction_refs(&message, meta.as_ref())? {
        let Some(program) = full_keys
            .get(instruction.program_id_index as usize)
            .copied()
            .flatten()
        else {
            stats.skipped_unresolved_programs += 1;
            continue;
        };
        let token_program = if known.token_program.matches(program) {
            TokenProgramKind::Token
        } else if known.token_2022_program.matches(program) {
            TokenProgramKind::Token2022
        } else {
            continue;
        };
        let Some(decoded) = decode_token_instruction(instruction.accounts, instruction.data) else {
            continue;
        };
        let Some(event) = materialize_hot_event_frozen(
            decoded,
            instruction,
            &full_keys,
            &account_mints,
            known,
            account_id_mode,
            discovery,
            tx_error,
            stats,
        )?
        else {
            continue;
        };
        let record = EventRecord {
            slot,
            tx_index: tx_row.tx_index,
            outer_ix: instruction.outer_ix,
            inner_ix: instruction.inner_ix,
            event_kind: event.kind,
            token_program,
            flags: event.flags,
            signature_id,
            wallet_id: event.wallet_id,
            token_account_id: event.token_account_id,
            counterparty_token_account_id: event.counterparty_token_account_id,
            amount: event.amount,
            raw_tag: event.raw_tag,
        };
        out.extend_from_slice(&record.encode());
        tx_had_event = true;
        stats.events += 1;
        match event.kind {
            TokenEventKind::CreateAccount => stats.create_events += 1,
            TokenEventKind::Transfer => stats.transfer_events += 1,
            TokenEventKind::Mint => stats.mint_events += 1,
            TokenEventKind::Burn => stats.burn_events += 1,
            TokenEventKind::Close => stats.close_events += 1,
        }
    }
    if tx_had_event {
        stats.txs_with_events += 1;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_block_bytes(
    block_bytes: &[u8],
    block_id: u32,
    slot: u64,
    first_signature_ordinal: u64,
    store: &KeyStore,
    signature_file: &mut File,
    events: &mut BufWriter<File>,
    registries: &mut Registries,
    discovery: &mut Discovery,
    account_id_mode: &AccountIdMode,
    mint: [u8; 32],
    stats: &mut DumpStats,
) -> Result<()> {
    let block: ArchiveV2HotBlockBlob =
        wincode::config::deserialize(block_bytes, wincode_leb128_config())
            .with_context(|| format!("decode hot block_id {block_id}"))?;
    anyhow::ensure!(
        block.header.slot == slot,
        "hot block_id {} slot mismatch: decoded={} index={}",
        block_id,
        block.header.slot,
        slot
    );
    stats.blocks += 1;
    stats.txs += block.tx_rows.len() as u64;

    let mut signature_ordinal = first_signature_ordinal;
    for tx_row in &block.tx_rows {
        let tx_signature_ordinal = signature_ordinal;
        signature_ordinal += tx_row.signature_count as u64;
        scan_hot_tx(
            &block,
            tx_row,
            block_id,
            slot,
            tx_signature_ordinal,
            store,
            signature_file,
            events,
            registries,
            discovery,
            account_id_mode,
            mint,
            stats,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn scan_hot_tx(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    slot: u64,
    signature_ordinal: u64,
    store: &KeyStore,
    signature_file: &mut File,
    events: &mut BufWriter<File>,
    registries: &mut Registries,
    discovery: &mut Discovery,
    account_id_mode: &AccountIdMode,
    mint: [u8; 32],
    stats: &mut DumpStats,
) -> Result<()> {
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        stats.skipped_raw_txs += 1;
        return Ok(());
    }
    let message_slice = hot_region(
        &block.message_bytes,
        tx_row.message_offset,
        tx_row.message_len,
        "message",
        block_id,
        tx_row.tx_index,
    )?;
    let message: ArchiveV2HotMessagePayload =
        wincode::config::deserialize(message_slice, wincode_leb128_config()).with_context(
            || {
                format!(
                    "decode hot message block_id={} slot={} tx_index={}",
                    block_id, slot, tx_row.tx_index
                )
            },
        )?;

    let meta = decode_hot_meta(block, tx_row, block_id, slot, stats)?;
    let full_keys = full_message_keys(&message, meta.as_ref(), store, stats);
    let account_mints =
        collect_account_mints(meta.as_ref(), &full_keys, store, mint, discovery, stats);
    let mut tx_had_event = false;
    let mut signature_id = None;
    let tx_error = tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;

    for instruction in hot_instruction_refs(&message, meta.as_ref())? {
        let Some(program) = full_keys
            .get(instruction.program_id_index as usize)
            .copied()
            .flatten()
        else {
            stats.skipped_unresolved_programs += 1;
            continue;
        };
        let token_program = if program == token_program_id() {
            TokenProgramKind::Token
        } else if program == token_2022_program_id() {
            TokenProgramKind::Token2022
        } else {
            continue;
        };
        let Some(decoded) = decode_token_instruction(instruction.accounts, instruction.data) else {
            continue;
        };
        let Some(event) = materialize_token_event(
            decoded,
            instruction,
            token_program,
            &full_keys,
            &account_mints,
            mint,
            discovery,
            tx_error,
            stats,
        )?
        else {
            continue;
        };
        let signature_id = match signature_id {
            Some(id) => id,
            None => {
                let signature = if account_id_mode.is_blockzilla() {
                    None
                } else {
                    Some(read_signature(signature_file, signature_ordinal)?)
                };
                let id =
                    event_signature_id(account_id_mode, registries, signature, signature_ordinal)?;
                signature_id = Some(id);
                id
            }
        };
        let wallet_id = event
            .wallet
            .map(|key| account_id_mode.account_id(key, &mut registries.wallets, stats))
            .transpose()?
            .unwrap_or(NO_ID);
        let token_account_id = account_id_mode.account_id(
            event.token_account,
            &mut registries.token_accounts,
            stats,
        )?;
        let counterparty_token_account_id = event
            .counterparty_token_account
            .map(|key| account_id_mode.account_id(key, &mut registries.token_accounts, stats))
            .transpose()?
            .unwrap_or(NO_ID);
        let record = EventRecord {
            slot,
            tx_index: tx_row.tx_index,
            outer_ix: instruction.outer_ix,
            inner_ix: instruction.inner_ix,
            event_kind: event.kind,
            token_program,
            flags: event.flags,
            signature_id,
            wallet_id,
            token_account_id,
            counterparty_token_account_id,
            amount: event.amount,
            raw_tag: event.raw_tag,
        };
        events.write_all(&record.encode())?;
        tx_had_event = true;
        stats.events += 1;
        match event.kind {
            TokenEventKind::CreateAccount => stats.create_events += 1,
            TokenEventKind::Transfer => stats.transfer_events += 1,
            TokenEventKind::Mint => stats.mint_events += 1,
            TokenEventKind::Burn => stats.burn_events += 1,
            TokenEventKind::Close => stats.close_events += 1,
        }
    }
    if tx_had_event {
        stats.txs_with_events += 1;
    }
    Ok(())
}

fn decode_hot_meta(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    slot: u64,
    stats: &mut DumpStats,
) -> Result<Option<CompactMetaV1>> {
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
        return Ok(None);
    }
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        stats.skipped_raw_metadata += 1;
        return Ok(None);
    }
    let metadata_slice = hot_region(
        &block.metadata_bytes,
        tx_row.metadata_offset,
        tx_row.metadata_len,
        "metadata",
        block_id,
        tx_row.tx_index,
    )?;
    let meta = wincode::config::deserialize(metadata_slice, wincode_leb128_config()).with_context(
        || {
            format!(
                "decode hot metadata block_id={} slot={} tx_index={}",
                block_id, slot, tx_row.tx_index
            )
        },
    )?;
    Ok(Some(meta))
}

fn decode_hot_instruction_meta(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    slot: u64,
    stats: &mut TokenInstructionStats,
) -> Result<Option<CompactMetaInstructionView>> {
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
        stats.metadata_skipped += 1;
        return Ok(None);
    }
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        stats.skipped_raw_metadata += 1;
        return Ok(None);
    }
    let metadata_slice = hot_region(
        &block.metadata_bytes,
        tx_row.metadata_offset,
        tx_row.metadata_len,
        "metadata",
        block_id,
        tx_row.tx_index,
    )?;
    let meta = wincode::config::deserialize(metadata_slice, wincode_leb128_config()).with_context(
        || {
            format!(
                "decode hot instruction metadata view block_id={} slot={} tx_index={}",
                block_id, slot, tx_row.tx_index
            )
        },
    )?;
    stats.metadata_decoded += 1;
    Ok(Some(meta))
}

fn decode_hot_instruction_inner_meta(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    slot: u64,
    stats: &mut TokenInstructionStats,
) -> Result<Option<CompactMetaInnerInstructionView>> {
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA == 0 {
        stats.metadata_skipped += 1;
        return Ok(None);
    }
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK != 0 {
        stats.skipped_raw_metadata += 1;
        return Ok(None);
    }
    let metadata_slice = hot_region(
        &block.metadata_bytes,
        tx_row.metadata_offset,
        tx_row.metadata_len,
        "metadata",
        block_id,
        tx_row.tx_index,
    )?;
    let meta = wincode::config::deserialize(metadata_slice, wincode_leb128_config()).with_context(
        || {
            format!(
                "decode hot instruction metadata inner view block_id={} slot={} tx_index={}",
                block_id, slot, tx_row.tx_index
            )
        },
    )?;
    stats.metadata_decoded += 1;
    Ok(Some(meta))
}

fn full_message_keys(
    message: &ArchiveV2HotMessagePayload,
    meta: Option<&CompactMetaV1>,
    store: &KeyStore,
    stats: &mut DumpStats,
) -> Vec<Option<[u8; 32]>> {
    let mut out = Vec::new();
    let static_keys = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => &message.account_keys,
        ArchiveV2HotMessagePayload::V0(message) => &message.account_keys,
    };
    out.extend(
        static_keys
            .iter()
            .map(|key| resolve_key(*key, store, stats)),
    );
    if let Some(meta) = meta {
        out.extend(
            meta.loaded_writable_addresses
                .iter()
                .map(|key| resolve_key(*key, store, stats)),
        );
        out.extend(
            meta.loaded_readonly_addresses
                .iter()
                .map(|key| resolve_key(*key, store, stats)),
        );
    }
    out
}

fn full_message_key_refs(
    message: &ArchiveV2HotMessagePayload,
    meta: Option<&CompactMetaV1>,
) -> Vec<Option<HotKeyRef>> {
    let mut out = Vec::new();
    let static_keys = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => &message.account_keys,
        ArchiveV2HotMessagePayload::V0(message) => &message.account_keys,
    };
    out.extend(
        static_keys
            .iter()
            .map(|key| Some(HotKeyRef::from_compact(*key))),
    );
    if let Some(meta) = meta {
        out.extend(
            meta.loaded_writable_addresses
                .iter()
                .map(|key| Some(HotKeyRef::from_compact(*key))),
        );
        out.extend(
            meta.loaded_readonly_addresses
                .iter()
                .map(|key| Some(HotKeyRef::from_compact(*key))),
        );
    }
    out
}

fn resolve_key(key: CompactPubkey, store: &KeyStore, stats: &mut DumpStats) -> Option<[u8; 32]> {
    let resolved = key.resolve(store);
    if resolved.is_none() {
        stats.skipped_unresolved_accounts += 1;
    }
    resolved
}

fn collect_account_mints(
    meta: Option<&CompactMetaV1>,
    full_keys: &[Option<[u8; 32]>],
    store: &KeyStore,
    mint: [u8; 32],
    discovery: &mut Discovery,
    stats: &mut DumpStats,
) -> HashMap<u32, [u8; 32]> {
    let mut out = HashMap::new();
    let Some(meta) = meta else {
        return out;
    };
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        let Some(balance_mint) = balance.mint.and_then(|key| key.resolve(store)) else {
            continue;
        };
        out.insert(balance.account_index, balance_mint);
        if balance_mint != mint {
            continue;
        }
        if let Some(Some(account)) = full_keys.get(balance.account_index as usize) {
            discovery.token_accounts.insert(*account);
            if let Some(owner) = compact_owner(balance.owner, store, stats) {
                discovery.token_account_owner.insert(*account, owner);
            }
        }
    }
    out
}

fn collect_hot_account_mints(meta: Option<&CompactMetaV1>) -> HashMap<u32, HotKeyRef> {
    let mut out = HashMap::new();
    let Some(meta) = meta else {
        return out;
    };
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        let Some(balance_mint) = balance.mint.map(HotKeyRef::from_compact) else {
            continue;
        };
        out.insert(balance.account_index, balance_mint);
    }
    out
}

fn discover_hot_token_balances(
    meta: Option<&CompactMetaV1>,
    full_keys: &[Option<HotKeyRef>],
    known: HotKnownKeys,
    account_id_mode: &AccountIdMode,
    discovery: &mut HotDiscovery,
    stats: &mut DumpStats,
) -> Result<()> {
    let Some(meta) = meta else {
        return Ok(());
    };
    for balance in meta
        .pre_token_balances
        .iter()
        .chain(meta.post_token_balances.iter())
    {
        let Some(balance_mint) = balance.mint.map(HotKeyRef::from_compact) else {
            continue;
        };
        if !known.mint.matches(balance_mint) {
            continue;
        }
        let Some(account_id) =
            resolve_hot_key_index_id(full_keys, balance.account_index, account_id_mode, stats)?
        else {
            continue;
        };
        if account_id == NO_ID {
            continue;
        }
        discovery.token_accounts.insert(account_id);
        if let Some(owner_id) = balance
            .owner
            .map(HotKeyRef::from_compact)
            .map(|key| account_id_mode.hot_key_id(key, stats))
            .transpose()?
            .filter(|id| *id != NO_ID)
        {
            discovery.token_account_owner.insert(account_id, owner_id);
        }
    }
    Ok(())
}

fn blockzilla_account_id(
    account_id_mode: &AccountIdMode,
    key: [u8; 32],
    stats: &mut DumpStats,
) -> Result<u32> {
    let AccountIdMode::Blockzilla(index) = account_id_mode else {
        anyhow::bail!("Blockzilla account id lookup called in local id mode");
    };
    let Some(id) = index.lookup(&key) else {
        stats.skipped_missing_registry_ids += 1;
        return Ok(NO_ID);
    };
    Ok(id)
}

fn compact_owner(
    owner: Option<CompactPubkey>,
    store: &KeyStore,
    stats: &mut DumpStats,
) -> Option<[u8; 32]> {
    owner.and_then(|key| resolve_key(key, store, stats))
}

fn hot_instruction_refs<'a>(
    message: &'a ArchiveV2HotMessagePayload,
    meta: Option<&'a CompactMetaV1>,
) -> Result<Vec<InstructionRef<'a>>> {
    let outer = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => &message.instructions,
        ArchiveV2HotMessagePayload::V0(message) => &message.instructions,
    };
    let mut out = Vec::new();
    for (outer_ix, instruction) in outer.iter().enumerate() {
        out.push(instruction_ref(
            u16::try_from(outer_ix).context("outer instruction index exceeds u16")?,
            NO_INNER_IX,
            instruction,
        ));
        if let Some(meta) = meta
            && let Some(groups) = &meta.inner_instructions
        {
            for group in groups
                .iter()
                .filter(|group| group.index as usize == outer_ix)
            {
                for (inner_ix, inner) in group.instructions.iter().enumerate() {
                    out.push(inner_instruction_ref(
                        u16::try_from(outer_ix).context("outer instruction index exceeds u16")?,
                        u16::try_from(inner_ix).context("inner instruction index exceeds u16")?,
                        inner,
                    ));
                }
            }
        }
    }
    Ok(out)
}

fn instruction_ref<'a>(
    outer_ix: u16,
    inner_ix: u16,
    instruction: &'a ArchiveV2HotInstruction,
) -> InstructionRef<'a> {
    InstructionRef {
        outer_ix,
        inner_ix,
        program_id_index: instruction.program_id_index as u32,
        accounts: &instruction.accounts,
        data: match &instruction.data {
            ArchiveV2HotInstructionData::Raw(data) => data,
            _ => &[],
        },
    }
}

fn inner_instruction_ref<'a>(
    outer_ix: u16,
    inner_ix: u16,
    instruction: &'a CompactInnerInstruction,
) -> InstructionRef<'a> {
    InstructionRef {
        outer_ix,
        inner_ix,
        program_id_index: instruction.program_id_index,
        accounts: &instruction.accounts,
        data: &instruction.data,
    }
}

fn decode_token_instruction(accounts: &[u8], data: &[u8]) -> Option<DecodedTokenInstruction> {
    let tag = *data.first()?;
    let amount = || amount_le(data);
    match tag {
        1 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::CreateAccount,
            token_account_index: account_index(accounts, 0),
            counterparty_token_account_index: None,
            wallet_index: account_index(accounts, 2),
            mint_index: account_index(accounts, 1),
            mint: None,
            owner: None,
            amount: 0,
            raw_tag: tag,
        }),
        3 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::Transfer,
            token_account_index: account_index(accounts, 0),
            counterparty_token_account_index: account_index(accounts, 1),
            wallet_index: account_index(accounts, 2),
            mint_index: None,
            mint: None,
            owner: None,
            amount: amount()?,
            raw_tag: tag,
        }),
        7 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::Mint,
            token_account_index: account_index(accounts, 1),
            counterparty_token_account_index: None,
            wallet_index: account_index(accounts, 2),
            mint_index: account_index(accounts, 0),
            mint: None,
            owner: None,
            amount: amount()?,
            raw_tag: tag,
        }),
        8 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::Burn,
            token_account_index: account_index(accounts, 0),
            counterparty_token_account_index: None,
            wallet_index: account_index(accounts, 2),
            mint_index: account_index(accounts, 1),
            mint: None,
            owner: None,
            amount: amount()?,
            raw_tag: tag,
        }),
        9 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::Close,
            token_account_index: account_index(accounts, 0),
            counterparty_token_account_index: None,
            wallet_index: account_index(accounts, 2),
            mint_index: None,
            mint: None,
            owner: None,
            amount: 0,
            raw_tag: tag,
        }),
        12 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::Transfer,
            token_account_index: account_index(accounts, 0),
            counterparty_token_account_index: account_index(accounts, 2),
            wallet_index: account_index(accounts, 3),
            mint_index: account_index(accounts, 1),
            mint: None,
            owner: None,
            amount: amount()?,
            raw_tag: tag,
        }),
        14 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::Mint,
            token_account_index: account_index(accounts, 1),
            counterparty_token_account_index: None,
            wallet_index: account_index(accounts, 2),
            mint_index: account_index(accounts, 0),
            mint: None,
            owner: None,
            amount: amount()?,
            raw_tag: tag,
        }),
        15 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::Burn,
            token_account_index: account_index(accounts, 0),
            counterparty_token_account_index: None,
            wallet_index: account_index(accounts, 2),
            mint_index: account_index(accounts, 1),
            mint: None,
            owner: None,
            amount: amount()?,
            raw_tag: tag,
        }),
        16 | 18 => Some(DecodedTokenInstruction {
            kind: TokenEventKind::CreateAccount,
            token_account_index: account_index(accounts, 0),
            counterparty_token_account_index: None,
            wallet_index: None,
            mint_index: account_index(accounts, 1),
            mint: None,
            owner: owner_from_data(data),
            amount: 0,
            raw_tag: tag,
        }),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn token_instruction_record(
    decoded: DecodedTokenInstruction,
    instruction: InstructionRef<'_>,
    token_program: TokenProgramKind,
    keys: &HotKeyLookup<'_>,
    account_id_mode: &AccountIdMode,
    mint_filter: Option<KnownHotKey>,
    slot: u64,
    tx_index: u32,
    signature_id: u32,
    tx_error: bool,
    stats: &mut TokenInstructionStats,
) -> Result<Option<TokenInstructionRecord>> {
    let token_account_id = decoded
        .token_account_index
        .map(|index| resolve_hot_ix_lookup_id(keys, index, account_id_mode, stats))
        .transpose()?
        .flatten()
        .unwrap_or(NO_ID);
    let counterparty_token_account_id = decoded
        .counterparty_token_account_index
        .map(|index| resolve_hot_ix_lookup_id(keys, index, account_id_mode, stats))
        .transpose()?
        .flatten()
        .unwrap_or(NO_ID);
    let wallet_id = if let Some(owner) = decoded.owner {
        hot_ix_key_ref_id(HotKeyRef::Raw(owner), account_id_mode, stats)?
    } else {
        decoded
            .wallet_index
            .map(|index| resolve_hot_ix_lookup_id(keys, index, account_id_mode, stats))
            .transpose()?
            .flatten()
    }
    .unwrap_or(NO_ID);
    let mint_ref = decoded.mint.map(HotKeyRef::Raw).or_else(|| {
        decoded
            .mint_index
            .and_then(|index| resolve_hot_lookup_index(keys, index))
    });
    let direct_mint_match =
        mint_filter.is_some_and(|mint| mint_ref.is_some_and(|mint_ref| mint.matches(mint_ref)));
    if mint_filter.is_some() && !direct_mint_match {
        stats.skipped_by_mint_filter += 1;
        return Ok(None);
    }
    let mint_id = mint_ref
        .map(|key| hot_ix_key_ref_id(key, account_id_mode, stats))
        .transpose()?
        .flatten()
        .unwrap_or(NO_ID);
    let mut flags = if instruction.inner_ix == NO_INNER_IX {
        0
    } else {
        EVENT_FLAG_INNER
    };
    if tx_error {
        flags |= EVENT_FLAG_TX_ERROR;
    }
    if direct_mint_match {
        flags |= TOKEN_IX_FLAG_DIRECT_MINT;
        stats.direct_mint_ixs += 1;
    }
    Ok(Some(TokenInstructionRecord {
        slot,
        tx_index,
        outer_ix: instruction.outer_ix,
        inner_ix: instruction.inner_ix,
        token_program,
        event_kind: decoded.kind,
        flags,
        signature_id,
        token_account_id,
        counterparty_token_account_id,
        wallet_id,
        mint_id,
        amount: decoded.amount,
        raw_tag: decoded.raw_tag,
    }))
}

fn resolve_hot_lookup_index(keys: &HotKeyLookup<'_>, index: u32) -> Option<HotKeyRef> {
    keys.get(index)
}

fn resolve_hot_ix_lookup_id(
    keys: &HotKeyLookup<'_>,
    index: u32,
    account_id_mode: &AccountIdMode,
    stats: &mut TokenInstructionStats,
) -> Result<Option<u32>> {
    let Some(key) = resolve_hot_lookup_index(keys, index) else {
        stats.skipped_unresolved_accounts += 1;
        return Ok(None);
    };
    hot_ix_key_ref_id(key, account_id_mode, stats)
}

fn hot_ix_key_ref_id(
    key: HotKeyRef,
    account_id_mode: &AccountIdMode,
    stats: &mut TokenInstructionStats,
) -> Result<Option<u32>> {
    match key {
        HotKeyRef::Id(id) => Ok(Some(id)),
        HotKeyRef::Raw(key) => {
            let AccountIdMode::Blockzilla(index) = account_id_mode else {
                anyhow::bail!("token instruction dump requires Blockzilla registry ids")
            };
            if let Some(id) = index.lookup(&key) {
                Ok(Some(id))
            } else {
                stats.skipped_unresolved_accounts += 1;
                Ok(None)
            }
        }
    }
}

#[derive(Debug)]
struct MaterializedEvent {
    kind: TokenEventKind,
    token_account: [u8; 32],
    counterparty_token_account: Option<[u8; 32]>,
    wallet: Option<[u8; 32]>,
    amount: u64,
    raw_tag: u8,
    flags: u16,
}

#[derive(Debug)]
struct MaterializedHotEvent {
    kind: TokenEventKind,
    token_account_id: u32,
    counterparty_token_account_id: u32,
    wallet_id: u32,
    amount: u64,
    raw_tag: u8,
    flags: u16,
    direct_mint_match: bool,
}

#[allow(clippy::too_many_arguments)]
fn materialize_hot_event_discovery(
    decoded: DecodedTokenInstruction,
    instruction: InstructionRef<'_>,
    full_keys: &[Option<HotKeyRef>],
    account_mints: &HashMap<u32, HotKeyRef>,
    known: HotKnownKeys,
    account_id_mode: &AccountIdMode,
    discovery: &mut HotDiscovery,
    tx_error: bool,
    stats: &mut DumpStats,
) -> Result<Option<MaterializedHotEvent>> {
    let event = materialize_hot_event_ids(
        decoded,
        instruction,
        full_keys,
        account_mints,
        known,
        account_id_mode,
        &discovery.token_accounts,
        &discovery.token_account_owner,
        tx_error,
        stats,
    )?;
    let Some(event) = event else {
        return Ok(None);
    };
    if event.direct_mint_match {
        if event.token_account_id != NO_ID {
            discovery.token_accounts.insert(event.token_account_id);
            if event.wallet_id != NO_ID {
                discovery
                    .token_account_owner
                    .entry(event.token_account_id)
                    .or_insert(event.wallet_id);
            }
        }
        if event.counterparty_token_account_id != NO_ID {
            discovery
                .token_accounts
                .insert(event.counterparty_token_account_id);
        }
    }
    Ok(Some(event))
}

#[allow(clippy::too_many_arguments)]
fn materialize_hot_event_frozen(
    decoded: DecodedTokenInstruction,
    instruction: InstructionRef<'_>,
    full_keys: &[Option<HotKeyRef>],
    account_mints: &HashMap<u32, HotKeyRef>,
    known: HotKnownKeys,
    account_id_mode: &AccountIdMode,
    discovery: &FrozenHotDiscovery,
    tx_error: bool,
    stats: &mut DumpStats,
) -> Result<Option<MaterializedHotEvent>> {
    materialize_hot_event_ids(
        decoded,
        instruction,
        full_keys,
        account_mints,
        known,
        account_id_mode,
        &discovery.token_accounts,
        &discovery.token_account_owner,
        tx_error,
        stats,
    )
}

#[allow(clippy::too_many_arguments)]
fn materialize_hot_event_ids(
    decoded: DecodedTokenInstruction,
    instruction: InstructionRef<'_>,
    full_keys: &[Option<HotKeyRef>],
    account_mints: &HashMap<u32, HotKeyRef>,
    known: HotKnownKeys,
    account_id_mode: &AccountIdMode,
    token_accounts: &HashSet<u32>,
    token_account_owner: &HashMap<u32, u32>,
    tx_error: bool,
    stats: &mut DumpStats,
) -> Result<Option<MaterializedHotEvent>> {
    let token_account_id = decoded
        .token_account_index
        .map(|index| resolve_hot_key_index_id(full_keys, index, account_id_mode, stats))
        .transpose()?
        .flatten();
    let counterparty_token_account_id = decoded
        .counterparty_token_account_index
        .map(|index| resolve_hot_key_index_id(full_keys, index, account_id_mode, stats))
        .transpose()?
        .flatten();
    let wallet_id = if let Some(owner) = decoded.owner {
        Some(blockzilla_account_id(account_id_mode, owner, stats)?)
    } else if let Some(index) = decoded.wallet_index {
        resolve_hot_key_index_id(full_keys, index, account_id_mode, stats)?
    } else {
        None
    }
    .or_else(|| token_account_id.and_then(|account| token_account_owner.get(&account).copied()));
    let mint = decoded
        .mint
        .map(HotKeyRef::Raw)
        .or_else(|| {
            decoded
                .mint_index
                .and_then(|index| resolve_hot_key_index(full_keys, index))
        })
        .or_else(|| {
            decoded
                .token_account_index
                .and_then(|index| account_mints.get(&index).copied())
        })
        .or_else(|| {
            decoded
                .counterparty_token_account_index
                .and_then(|index| account_mints.get(&index).copied())
        });
    let direct_mint_match = mint.is_some_and(|mint| known.mint.matches(mint));
    let tracked = token_account_id
        .filter(|id| *id != NO_ID)
        .is_some_and(|account| token_accounts.contains(&account))
        || counterparty_token_account_id
            .filter(|id| *id != NO_ID)
            .is_some_and(|account| token_accounts.contains(&account));
    if !direct_mint_match && !tracked {
        return Ok(None);
    }
    let Some(token_account_id) = token_account_id else {
        stats.skipped_unresolved_accounts += 1;
        return Ok(None);
    };
    let mut flags = if instruction.inner_ix == NO_INNER_IX {
        0
    } else {
        EVENT_FLAG_INNER
    };
    if tx_error {
        flags |= EVENT_FLAG_TX_ERROR;
    }
    Ok(Some(MaterializedHotEvent {
        kind: decoded.kind,
        token_account_id,
        counterparty_token_account_id: counterparty_token_account_id.unwrap_or(NO_ID),
        wallet_id: wallet_id.unwrap_or(NO_ID),
        amount: decoded.amount,
        raw_tag: decoded.raw_tag,
        flags,
        direct_mint_match,
    }))
}

fn materialize_token_event(
    decoded: DecodedTokenInstruction,
    instruction: InstructionRef<'_>,
    _token_program: TokenProgramKind,
    full_keys: &[Option<[u8; 32]>],
    account_mints: &HashMap<u32, [u8; 32]>,
    target_mint: [u8; 32],
    discovery: &mut Discovery,
    tx_error: bool,
    stats: &mut DumpStats,
) -> Result<Option<MaterializedEvent>> {
    let token_account = decoded
        .token_account_index
        .and_then(|index| resolve_index(full_keys, index));
    let counterparty = decoded
        .counterparty_token_account_index
        .and_then(|index| resolve_index(full_keys, index));
    let wallet = decoded
        .owner
        .or_else(|| {
            decoded
                .wallet_index
                .and_then(|index| resolve_index(full_keys, index))
        })
        .or_else(|| {
            token_account.and_then(|account| discovery.token_account_owner.get(&account).copied())
        });
    let mint = decoded
        .mint
        .or_else(|| {
            decoded
                .mint_index
                .and_then(|index| resolve_index(full_keys, index))
        })
        .or_else(|| {
            decoded
                .token_account_index
                .and_then(|index| account_mints.get(&index).copied())
        })
        .or_else(|| {
            decoded
                .counterparty_token_account_index
                .and_then(|index| account_mints.get(&index).copied())
        });
    let direct_usdc = mint == Some(target_mint);
    let tracked = token_account.is_some_and(|account| discovery.token_accounts.contains(&account))
        || counterparty.is_some_and(|account| discovery.token_accounts.contains(&account));
    if !direct_usdc && !tracked {
        return Ok(None);
    }
    let Some(token_account) = token_account else {
        stats.skipped_unresolved_accounts += 1;
        return Ok(None);
    };
    if direct_usdc {
        discovery.token_accounts.insert(token_account);
        if let Some(counterparty) = counterparty {
            discovery.token_accounts.insert(counterparty);
        }
    }
    if let Some(wallet) = wallet {
        discovery
            .token_account_owner
            .entry(token_account)
            .or_insert(wallet);
    }
    let mut flags = if instruction.inner_ix == NO_INNER_IX {
        0
    } else {
        EVENT_FLAG_INNER
    };
    if tx_error {
        flags |= EVENT_FLAG_TX_ERROR;
    }
    Ok(Some(MaterializedEvent {
        kind: decoded.kind,
        token_account,
        counterparty_token_account: counterparty,
        wallet,
        amount: decoded.amount,
        raw_tag: decoded.raw_tag,
        flags,
    }))
}

fn resolve_index(full_keys: &[Option<[u8; 32]>], index: u32) -> Option<[u8; 32]> {
    full_keys.get(index as usize).copied().flatten()
}

fn resolve_hot_key_index(full_keys: &[Option<HotKeyRef>], index: u32) -> Option<HotKeyRef> {
    full_keys.get(index as usize).copied().flatten()
}

fn resolve_hot_key_index_id(
    full_keys: &[Option<HotKeyRef>],
    index: u32,
    account_id_mode: &AccountIdMode,
    stats: &mut DumpStats,
) -> Result<Option<u32>> {
    resolve_hot_key_index(full_keys, index)
        .map(|key| account_id_mode.hot_key_id(key, stats))
        .transpose()
}

fn account_index(accounts: &[u8], index: usize) -> Option<u32> {
    accounts.get(index).map(|value| *value as u32)
}

fn amount_le(data: &[u8]) -> Option<u64> {
    let bytes = data.get(1..9)?;
    Some(u64::from_le_bytes(bytes.try_into().ok()?))
}

fn owner_from_data(data: &[u8]) -> Option<[u8; 32]> {
    let bytes = data.get(1..33)?;
    let mut out = [0u8; 32];
    out.copy_from_slice(bytes);
    Some(out)
}

fn hot_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    region: &'static str,
    block_id: u32,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let start = offset as usize;
    let end = start
        .checked_add(len as usize)
        .ok_or_else(|| anyhow!("{region} slice overflow"))?;
    bytes.get(start..end).with_context(|| {
        format!(
            "{region} slice out of bounds block_id={block_id} tx_index={tx_index} offset={offset} len={len} region_len={}",
            bytes.len()
        )
    })
}

fn read_signature(file: &mut File, ordinal: u64) -> Result<[u8; 64]> {
    let mut signature = [0u8; 64];
    let offset = ordinal
        .checked_mul(64)
        .context("signature ordinal offset overflow")?;
    #[cfg(unix)]
    {
        file.read_exact_at(&mut signature, offset)
            .with_context(|| format!("read signature ordinal {ordinal}"))?;
    }
    #[cfg(not(unix))]
    {
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut signature)
            .with_context(|| format!("read signature ordinal {ordinal}"))?;
    }
    Ok(signature)
}

fn rate(value: u64, elapsed: f64) -> f64 {
    if elapsed > 0.0 {
        value as f64 / elapsed
    } else {
        0.0
    }
}

fn mib_rate(bytes: u64, elapsed: f64) -> f64 {
    rate(bytes, elapsed) / 1024.0 / 1024.0
}
