use crate::{
    archive::{ArchiveV2Reader, default_registry_path},
    dex::{DEFAULT_DEX_PROGRAMS, DexRegistry, DexTxContext, ResolvedDexProgram},
    format::{
        BALANCE_FLAG_CHANGED, BALANCE_FLAG_TX_ERROR, IndexMeta, NO_ID, PUBKEYS_FILE, PubkeyRecord,
        RecordFileWriter, SWAPS_FILE, SwapRecord, TOKEN_ACCOUNTS_FILE, TOKEN_FLAG_QUOTE,
        TOKENS_FILE, TokenAccountRecord, TokenBalanceChangeRecord, TokenInfoRecord, amount_to_ui,
        decode_pubkey, write_record_file,
    },
};
use anyhow::{Context, Result};
use blockzilla_format::{
    ARCHIVE_V2_TX_FLAG_HAS_ERROR, ARCHIVE_V2_TX_FLAG_HAS_METADATA,
    ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES, ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK,
    ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK, ArchiveV2HotBlockBlob, ArchiveV2HotInstruction,
    ArchiveV2HotMessagePayload, ArchiveV2HotTxRow, CompactMetaV1, CompactPubkey, KeyIndex,
    KeyStore,
};
use serde_json::to_vec_pretty;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{BufReader, Read, Seek, SeekFrom},
    path::PathBuf,
};
use tracing::info;

const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4SpDWGFB8ZQp8vAqW8zV";
const WINCODE_PREALLOCATION_LIMIT: usize = 64 << 20;

type SafeWincodeLeb128Config = wincode::config::Configuration<
    true,
    WINCODE_PREALLOCATION_LIMIT,
    wincode::len::BincodeLen,
    wincode::int_encoding::LittleEndian,
    blockzilla_format::Leb128,
>;

fn safe_wincode_leb128_config() -> SafeWincodeLeb128Config {
    wincode::config::Configuration::default()
        .with_preallocation_size_limit::<WINCODE_PREALLOCATION_LIMIT>()
        .with_int_encoding::<blockzilla_format::Leb128>()
}

#[derive(Debug)]
pub struct IndexArchiveV2Config {
    pub input: PathBuf,
    pub output_dir: PathBuf,
    pub index: Option<PathBuf>,
    pub registry: Option<PathBuf>,
    pub max_blocks: Option<u64>,
    pub quote_mints: Vec<String>,
    pub dex_programs: Vec<String>,
    pub profile: IndexProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexProfile {
    Full,
    PriceApi,
}

impl Default for IndexProfile {
    fn default() -> Self {
        Self::Full
    }
}

impl IndexProfile {
    fn write_balance_rows(self) -> bool {
        matches!(self, Self::Full)
    }

    fn write_token_accounts(self) -> bool {
        matches!(self, Self::Full)
    }

    fn record_all_message_pubkeys(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Debug, Default)]
struct IndexStats {
    blocks: u64,
    transactions: u64,
    metadata_decoded: u64,
    token_balance_rows: u64,
    swaps: u64,
    skipped_raw_txs: u64,
    skipped_raw_metadata: u64,
    metadata_decode_errors: u64,
    skipped_missing_keys: u64,
}

enum RegistryResolver {
    Full { store: KeyStore, index: KeyIndex },
    Lazy(LazyRegistryResolver),
}

impl RegistryResolver {
    fn load(path: &std::path::Path, lazy: bool, raw_lookup_keys: &[[u8; 32]]) -> Result<Self> {
        if lazy {
            return LazyRegistryResolver::load(path, raw_lookup_keys).map(Self::Lazy);
        }
        let store = KeyStore::load(path)?;
        let index = KeyIndex::build(store.keys.clone());
        Ok(Self::Full { store, index })
    }

    fn id_for_raw(&self, key: &[u8; 32]) -> Option<u32> {
        match self {
            Self::Full { index, .. } => index.lookup(key),
            Self::Lazy(resolver) => resolver.id_for_raw(key),
        }
    }

    fn pubkey_for_id(&self, id: u32) -> Option<[u8; 32]> {
        match self {
            Self::Full { store, .. } => store.get(id).copied(),
            Self::Lazy(resolver) => resolver.pubkey_for_id(id).ok().flatten(),
        }
    }
}

struct LazyRegistryResolver {
    file: RefCell<File>,
    len: u32,
    raw_lookup: BTreeMap<[u8; 32], u32>,
}

impl LazyRegistryResolver {
    fn load(path: &std::path::Path, raw_lookup_keys: &[[u8; 32]]) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open registry {}", path.display()))?;
        let len_bytes = file.metadata().context("stat registry")?.len();
        anyhow::ensure!(
            len_bytes.is_multiple_of(32),
            "invalid registry size {} (not multiple of 32)",
            len_bytes
        );
        let key_count = len_bytes / 32;
        let len = u32::try_from(key_count).context("registry has more than u32::MAX keys")?;
        let wanted = raw_lookup_keys.iter().copied().collect::<BTreeSet<_>>();
        let mut raw_lookup = BTreeMap::new();
        if !wanted.is_empty() {
            let mut reader = BufReader::with_capacity(
                64 << 20,
                File::open(path).with_context(|| format!("open registry {}", path.display()))?,
            );
            let mut key = [0u8; 32];
            for index in 0..key_count {
                reader
                    .read_exact(&mut key)
                    .context("read registry pubkey")?;
                if wanted.contains(&key) {
                    raw_lookup.insert(
                        key,
                        u32::try_from(index + 1).context("registry id exceeds u32")?,
                    );
                    if raw_lookup.len() == wanted.len() {
                        break;
                    }
                }
            }
        }
        Ok(Self {
            file: RefCell::new(file),
            len,
            raw_lookup,
        })
    }

    fn id_for_raw(&self, key: &[u8; 32]) -> Option<u32> {
        self.raw_lookup.get(key).copied()
    }

    fn pubkey_for_id(&self, id: u32) -> Result<Option<[u8; 32]>> {
        if id == 0 || id > self.len {
            return Ok(None);
        }
        let offset = (u64::from(id) - 1) * 32;
        let mut key = [0u8; 32];
        let mut file = self.file.borrow_mut();
        file.seek(SeekFrom::Start(offset))
            .context("seek registry pubkey")?;
        file.read_exact(&mut key).context("read registry pubkey")?;
        Ok(Some(key))
    }
}

#[derive(Debug)]
struct KnownIds {
    quote_mints: BTreeSet<u32>,
    dex_registry: DexRegistry,
    quote_mint_strings: Vec<String>,
    dex_program_strings: Vec<String>,
}

#[derive(Debug, Clone)]
struct TokenAgg {
    mint_id: u32,
    program_id: u32,
    decimals: u8,
    flags: u8,
    first_slot: u64,
    last_slot: u64,
    first_block_time: i64,
    last_block_time: i64,
    balance_change_count: u64,
    swap_count: u64,
}

#[derive(Debug, Clone)]
struct AccountAgg {
    account_id: u32,
    mint_id: u32,
    owner_id: u32,
    program_id: u32,
    first_slot: u64,
    last_slot: u64,
    first_block_time: i64,
    last_block_time: i64,
}

#[derive(Debug, Default)]
struct Accumulator {
    pubkeys: BTreeMap<u32, [u8; 32]>,
    tokens: BTreeMap<u32, TokenAgg>,
    accounts: BTreeMap<u32, AccountAgg>,
}

struct IndexWriters {
    balances: RecordFileWriter<TokenBalanceChangeRecord>,
    swaps: RecordFileWriter<SwapRecord>,
}

#[derive(Debug, Clone, Copy)]
struct BalanceView {
    account_index: u32,
    account_id: u32,
    mint_id: u32,
    owner_id: u32,
    program_id: u32,
    amount: u64,
    decimals: u8,
}

#[derive(Debug, Default)]
struct BalancePair {
    pre: Option<BalanceView>,
    post: Option<BalanceView>,
}

pub fn index_archive_v2(config: IndexArchiveV2Config) -> Result<IndexMeta> {
    fs::create_dir_all(&config.output_dir)
        .with_context(|| format!("create {}", config.output_dir.display()))?;
    let registry_path = config
        .registry
        .clone()
        .unwrap_or_else(|| default_registry_path(&config.input));
    let raw_lookup_keys = known_raw_lookup_keys(&config.quote_mints, &config.dex_programs);
    let resolver = RegistryResolver::load(
        &registry_path,
        matches!(config.profile, IndexProfile::PriceApi),
        &raw_lookup_keys,
    )
    .with_context(|| format!("load registry {}", registry_path.display()))?;
    let known = resolve_known_ids(
        &resolver,
        config.quote_mints.clone(),
        config.dex_programs.clone(),
    )?;

    let mut reader = ArchiveV2Reader::open(&config.input, config.index.as_deref())?;
    let index_path = config
        .index
        .clone()
        .unwrap_or_else(|| blockzilla_format::archive_v2_hot_index_path(&config.input));
    let rows: Vec<_> = reader
        .rows()
        .iter()
        .take(config.max_blocks.unwrap_or(u64::MAX) as usize)
        .copied()
        .collect();

    let mut acc = Accumulator::default();
    seed_known_pubkeys(&resolver, &known, &mut acc);
    let mut writers = IndexWriters {
        balances: RecordFileWriter::create(
            &config.output_dir.join(crate::format::BALANCE_CHANGES_FILE),
        )?,
        swaps: RecordFileWriter::create(&config.output_dir.join(SWAPS_FILE))?,
    };
    let mut stats = IndexStats::default();
    for row in rows {
        let block = reader.read_block(&row)?;
        stats.blocks += 1;
        process_block(
            &block,
            &row,
            &resolver,
            &known,
            &mut acc,
            &mut writers,
            &mut stats,
            config.profile,
        )
        .with_context(|| format!("process block_id={} slot={}", row.block_id, row.slot))?;
        if stats.blocks.is_multiple_of(10_000) {
            info!(
                "indexed blocks={} txs={} balance_rows={} swaps={}",
                stats.blocks, stats.transactions, stats.token_balance_rows, stats.swaps
            );
        }
    }

    let mut token_account_counts: BTreeMap<u32, u64> = BTreeMap::new();
    if config.profile.write_token_accounts() {
        for account in acc.accounts.values() {
            *token_account_counts.entry(account.mint_id).or_default() += 1;
        }
    }
    let pubkeys = acc
        .pubkeys
        .iter()
        .map(|(id, pubkey)| PubkeyRecord {
            id: *id,
            pubkey: *pubkey,
        })
        .collect::<Vec<_>>();
    let tokens = acc
        .tokens
        .values()
        .map(|token| TokenInfoRecord {
            mint_id: token.mint_id,
            program_id: token.program_id,
            decimals: token.decimals,
            flags: token.flags,
            first_slot: token.first_slot,
            last_slot: token.last_slot,
            first_block_time: token.first_block_time,
            last_block_time: token.last_block_time,
            balance_change_count: token.balance_change_count,
            swap_count: token.swap_count,
            token_account_count: token_account_counts
                .get(&token.mint_id)
                .copied()
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    let accounts = if config.profile.write_token_accounts() {
        acc.accounts
            .values()
            .map(|account| TokenAccountRecord {
                account_id: account.account_id,
                mint_id: account.mint_id,
                owner_id: account.owner_id,
                program_id: account.program_id,
                first_slot: account.first_slot,
                last_slot: account.last_slot,
                first_block_time: account.first_block_time,
                last_block_time: account.last_block_time,
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    write_record_file(&config.output_dir.join(PUBKEYS_FILE), &pubkeys)?;
    write_record_file(&config.output_dir.join(TOKENS_FILE), &tokens)?;
    write_record_file(&config.output_dir.join(TOKEN_ACCOUNTS_FILE), &accounts)?;
    let balance_count = writers.balances.finish()?;
    let swap_count = writers.swaps.finish()?;

    let meta = IndexMeta {
        format_version: crate::format::FORMAT_VERSION,
        archive_input: config.input.display().to_string(),
        archive_index: index_path.display().to_string(),
        archive_registry: registry_path.display().to_string(),
        compressed_input: reader.compressed_input(),
        raw_blocks: reader.raw_blocks(),
        quote_mints: known.quote_mint_strings.clone(),
        dex_programs: known.dex_program_strings.clone(),
        blocks: stats.blocks,
        transactions: stats.transactions,
        metadata_decoded: stats.metadata_decoded,
        token_balance_rows: balance_count,
        token_accounts: accounts.len() as u64,
        tokens: tokens.len() as u64,
        swaps: swap_count,
        pubkeys: pubkeys.len() as u64,
        skipped_raw_txs: stats.skipped_raw_txs,
        skipped_raw_metadata: stats.skipped_raw_metadata,
        metadata_decode_errors: stats.metadata_decode_errors,
        skipped_missing_keys: stats.skipped_missing_keys,
    };
    fs::write(
        config.output_dir.join(crate::format::META_FILE),
        to_vec_pretty(&meta)?,
    )
    .context("write meta.json")?;

    info!(
        "token api index complete: blocks={} txs={} tokens={} accounts={} balances={} swaps={} output={}",
        meta.blocks,
        meta.transactions,
        meta.tokens,
        meta.token_accounts,
        meta.token_balance_rows,
        meta.swaps,
        config.output_dir.display()
    );
    Ok(meta)
}

fn resolve_known_ids(
    resolver: &RegistryResolver,
    quote_mints: Vec<String>,
    dex_programs: Vec<String>,
) -> Result<KnownIds> {
    let quote_mint_strings = if quote_mints.is_empty() {
        vec![USDC_MINT.to_string(), USDT_MINT.to_string()]
    } else {
        quote_mints
    };
    let mut dex_program_strings = DEFAULT_DEX_PROGRAMS
        .iter()
        .map(|spec| spec.program_id.to_string())
        .collect::<Vec<_>>();
    dex_program_strings.extend(dex_programs.iter().cloned());

    let quote_mints = quote_mint_strings
        .iter()
        .filter_map(|value| decode_pubkey(value).ok())
        .filter_map(|key| resolver.id_for_raw(&key))
        .collect::<BTreeSet<_>>();

    let mut resolved_programs = Vec::new();
    for spec in DEFAULT_DEX_PROGRAMS {
        let Some(program_id) = decode_pubkey(spec.program_id)
            .ok()
            .and_then(|key| resolver.id_for_raw(&key))
        else {
            continue;
        };
        resolved_programs.push(ResolvedDexProgram {
            slug: spec.slug.to_string(),
            label: spec.label.to_string(),
            program_id,
            address: spec.program_id.to_string(),
            sources: spec.sources.to_vec(),
        });
    }
    for value in dex_programs {
        let Some(program_id) = decode_pubkey(&value)
            .ok()
            .and_then(|key| resolver.id_for_raw(&key))
        else {
            continue;
        };
        resolved_programs.push(ResolvedDexProgram::custom(program_id, value));
    }

    Ok(KnownIds {
        quote_mints,
        dex_registry: DexRegistry::new(resolved_programs),
        quote_mint_strings,
        dex_program_strings,
    })
}

fn known_raw_lookup_keys(quote_mints: &[String], dex_programs: &[String]) -> Vec<[u8; 32]> {
    let quote_mint_strings = if quote_mints.is_empty() {
        vec![USDC_MINT.to_string(), USDT_MINT.to_string()]
    } else {
        quote_mints.to_vec()
    };
    quote_mint_strings
        .iter()
        .map(String::as_str)
        .chain(DEFAULT_DEX_PROGRAMS.iter().map(|spec| spec.program_id))
        .chain(dex_programs.iter().map(String::as_str))
        .filter_map(|value| decode_pubkey(value).ok())
        .collect()
}

fn seed_known_pubkeys(resolver: &RegistryResolver, known: &KnownIds, acc: &mut Accumulator) {
    for id in known.quote_mints.iter().copied().chain(
        known
            .dex_registry
            .programs()
            .map(|program| program.program_id),
    ) {
        if let Some(pubkey) = resolver.pubkey_for_id(id) {
            acc.pubkeys.entry(id).or_insert(pubkey);
        }
    }
}

fn process_block(
    block: &ArchiveV2HotBlockBlob,
    row: &blockzilla_format::ArchiveV2HotBlockIndexRow,
    resolver: &RegistryResolver,
    known: &KnownIds,
    acc: &mut Accumulator,
    writers: &mut IndexWriters,
    stats: &mut IndexStats,
    profile: IndexProfile,
) -> Result<()> {
    let block_time = block.header.block_time.unwrap_or_default();
    let mut signature_ordinal = row.first_signature_ordinal;
    for tx_row in &block.tx_rows {
        let signature_id = u32::try_from(signature_ordinal + 1).unwrap_or(NO_ID);
        signature_ordinal += tx_row.signature_count as u64;
        stats.transactions += 1;
        process_tx(
            block,
            tx_row,
            row.block_id,
            row.slot,
            block_time,
            signature_id,
            resolver,
            known,
            acc,
            writers,
            stats,
            profile,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_tx(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    slot: u64,
    block_time: i64,
    signature_id: u32,
    resolver: &RegistryResolver,
    known: &KnownIds,
    acc: &mut Accumulator,
    writers: &mut IndexWriters,
    stats: &mut IndexStats,
    profile: IndexProfile,
) -> Result<()> {
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK != 0 {
        stats.skipped_raw_txs += 1;
        return Ok(());
    }
    if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_TOKEN_BALANCES == 0 {
        return Ok(());
    }

    let meta = match decode_meta(block, tx_row, block_id, slot, stats)? {
        Some(meta) => meta,
        None => return Ok(()),
    };
    let message_slice = hot_region(
        &block.message_bytes,
        tx_row.message_offset,
        tx_row.message_len,
        "message",
        block_id,
        tx_row.tx_index,
    )?;
    let message: ArchiveV2HotMessagePayload =
        wincode::config::deserialize(message_slice, safe_wincode_leb128_config()).with_context(
            || {
                format!(
                    "decode hot message block_id={} slot={} tx_index={}",
                    block_id, slot, tx_row.tx_index
                )
            },
        )?;
    let full_keys = collect_message_key_ids(
        &message,
        &meta,
        resolver,
        acc,
        stats,
        profile.record_all_message_pubkeys(),
    );
    let touched_program_ids =
        collect_touched_dex_programs(&message, &full_keys, &known.dex_registry);
    let records = collect_balance_records(
        &meta, &full_keys, block_id, slot, block_time, tx_row, resolver, known, acc, stats, profile,
    );
    let mut swaps = Vec::new();
    known.dex_registry.decode(
        DexTxContext {
            slot,
            block_time,
            tx_index: tx_row.tx_index,
            block_id,
            signature_id,
            tx_error: tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0,
            message: &message,
            account_key_ids: &full_keys,
            touched_program_ids: &touched_program_ids,
            quote_mint_ids: &known.quote_mints,
            balance_changes: &records,
        },
        &mut swaps,
    );
    observe_swaps(acc, &swaps);
    if profile.write_balance_rows() {
        writers.balances.write_all(&records)?;
    }
    writers.swaps.write_all(&swaps)?;
    stats.swaps += swaps.len() as u64;
    Ok(())
}

fn decode_meta(
    block: &ArchiveV2HotBlockBlob,
    tx_row: &ArchiveV2HotTxRow,
    block_id: u32,
    _slot: u64,
    stats: &mut IndexStats,
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
    let meta = match wincode::config::deserialize(metadata_slice, safe_wincode_leb128_config()) {
        Ok(meta) => meta,
        Err(_) => {
            stats.metadata_decode_errors += 1;
            return Ok(None);
        }
    };
    stats.metadata_decoded += 1;
    Ok(Some(meta))
}

#[allow(clippy::too_many_arguments)]
fn collect_balance_records(
    meta: &CompactMetaV1,
    full_keys: &[Option<u32>],
    block_id: u32,
    slot: u64,
    block_time: i64,
    tx_row: &ArchiveV2HotTxRow,
    resolver: &RegistryResolver,
    known: &KnownIds,
    acc: &mut Accumulator,
    stats: &mut IndexStats,
    profile: IndexProfile,
) -> Vec<TokenBalanceChangeRecord> {
    let mut pairs: BTreeMap<u32, BalancePair> = BTreeMap::new();
    for balance in &meta.pre_token_balances {
        if let Some(view) = balance_view(balance, full_keys, resolver, acc, stats) {
            pairs.entry(view.account_index).or_default().pre = Some(view);
        }
    }
    for balance in &meta.post_token_balances {
        if let Some(view) = balance_view(balance, full_keys, resolver, acc, stats) {
            pairs.entry(view.account_index).or_default().post = Some(view);
        }
    }

    let mut records = Vec::with_capacity(pairs.len());
    for pair in pairs.into_values() {
        let Some(observed) = pair.post.or(pair.pre) else {
            continue;
        };
        let pre_amount = pair.pre.map(|balance| balance.amount).unwrap_or_default();
        let post_amount = pair.post.map(|balance| balance.amount).unwrap_or_default();
        let mut flags = 0u8;
        if pre_amount != post_amount {
            flags |= BALANCE_FLAG_CHANGED;
        }
        if tx_row.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0 {
            flags |= BALANCE_FLAG_TX_ERROR;
        }
        observe_token(
            acc,
            known,
            observed.mint_id,
            observed.program_id,
            observed.decimals,
            slot,
            block_time,
        );
        if profile.write_token_accounts() {
            observe_account(
                acc,
                observed.account_id,
                observed.mint_id,
                observed.owner_id,
                observed.program_id,
                slot,
                block_time,
            );
        }
        stats.token_balance_rows += 1;
        records.push(TokenBalanceChangeRecord {
            slot,
            block_time,
            tx_index: tx_row.tx_index,
            block_id,
            account_id: observed.account_id,
            mint_id: observed.mint_id,
            owner_id: observed.owner_id,
            program_id: observed.program_id,
            pre_amount,
            post_amount,
            decimals: observed.decimals,
            flags,
        });
    }
    records
}

fn balance_view(
    balance: &blockzilla_format::CompactTokenBalance,
    full_keys: &[Option<u32>],
    resolver: &RegistryResolver,
    acc: &mut Accumulator,
    stats: &mut IndexStats,
) -> Option<BalanceView> {
    let account_id = full_keys
        .get(balance.account_index as usize)
        .copied()
        .flatten()?;
    let mint_id = resolve_optional_compact_id(balance.mint, resolver, acc, stats, true)?;
    let owner_id =
        resolve_optional_compact_id(balance.owner, resolver, acc, stats, true).unwrap_or(NO_ID);
    let program_id = resolve_optional_compact_id(balance.program_id, resolver, acc, stats, true)
        .unwrap_or(NO_ID);
    Some(BalanceView {
        account_index: balance.account_index,
        account_id,
        mint_id,
        owner_id,
        program_id,
        amount: balance.amount,
        decimals: balance.decimals,
    })
}

fn collect_message_key_ids(
    message: &ArchiveV2HotMessagePayload,
    meta: &CompactMetaV1,
    resolver: &RegistryResolver,
    acc: &mut Accumulator,
    stats: &mut IndexStats,
    record_pubkeys: bool,
) -> Vec<Option<u32>> {
    let mut out = Vec::new();
    let static_keys = match message {
        ArchiveV2HotMessagePayload::Legacy(message) => message.account_keys.as_slice(),
        ArchiveV2HotMessagePayload::V0(message) => message.account_keys.as_slice(),
    };
    for key in static_keys
        .iter()
        .chain(meta.loaded_writable_addresses.iter())
        .chain(meta.loaded_readonly_addresses.iter())
    {
        out.push(resolve_compact_id(
            *key,
            resolver,
            acc,
            stats,
            record_pubkeys,
        ));
    }
    out
}

fn collect_touched_dex_programs(
    message: &ArchiveV2HotMessagePayload,
    full_keys: &[Option<u32>],
    registry: &DexRegistry,
) -> BTreeSet<u32> {
    let mut out = BTreeSet::new();
    for instruction in message_instructions(message) {
        let Some(program_id) = full_keys
            .get(instruction.program_id_index as usize)
            .copied()
            .flatten()
        else {
            continue;
        };
        if registry.is_known_program(program_id) {
            out.insert(program_id);
        }
    }

    for program_id in full_keys.iter().flatten() {
        if registry.is_known_program(*program_id) {
            out.insert(*program_id);
        }
    }
    out
}

fn observe_token(
    acc: &mut Accumulator,
    known: &KnownIds,
    mint_id: u32,
    program_id: u32,
    decimals: u8,
    slot: u64,
    block_time: i64,
) {
    let entry = acc.tokens.entry(mint_id).or_insert(TokenAgg {
        mint_id,
        program_id,
        decimals,
        flags: if known.quote_mints.contains(&mint_id) {
            TOKEN_FLAG_QUOTE
        } else {
            0
        },
        first_slot: slot,
        last_slot: slot,
        first_block_time: block_time,
        last_block_time: block_time,
        balance_change_count: 0,
        swap_count: 0,
    });
    entry.program_id = prefer_id(entry.program_id, program_id);
    entry.decimals = decimals;
    entry.first_slot = entry.first_slot.min(slot);
    entry.last_slot = entry.last_slot.max(slot);
    if block_time != 0 {
        if entry.first_block_time == 0 || block_time < entry.first_block_time {
            entry.first_block_time = block_time;
        }
        if block_time > entry.last_block_time {
            entry.last_block_time = block_time;
        }
    }
    entry.balance_change_count += 1;
}

fn observe_swaps(acc: &mut Accumulator, swaps: &[SwapRecord]) {
    for swap in swaps {
        if let Some(token) = acc.tokens.get_mut(&swap.in_mint_id) {
            token.swap_count += 1;
        }
        if let Some(token) = acc.tokens.get_mut(&swap.out_mint_id) {
            token.swap_count += 1;
        }
    }
}

fn observe_account(
    acc: &mut Accumulator,
    account_id: u32,
    mint_id: u32,
    owner_id: u32,
    program_id: u32,
    slot: u64,
    block_time: i64,
) {
    let entry = acc.accounts.entry(account_id).or_insert(AccountAgg {
        account_id,
        mint_id,
        owner_id,
        program_id,
        first_slot: slot,
        last_slot: slot,
        first_block_time: block_time,
        last_block_time: block_time,
    });
    entry.mint_id = prefer_id(entry.mint_id, mint_id);
    entry.owner_id = prefer_id(entry.owner_id, owner_id);
    entry.program_id = prefer_id(entry.program_id, program_id);
    entry.first_slot = entry.first_slot.min(slot);
    entry.last_slot = entry.last_slot.max(slot);
    if block_time != 0 {
        if entry.first_block_time == 0 || block_time < entry.first_block_time {
            entry.first_block_time = block_time;
        }
        if block_time > entry.last_block_time {
            entry.last_block_time = block_time;
        }
    }
}

fn prefer_id(current: u32, candidate: u32) -> u32 {
    if current == NO_ID { candidate } else { current }
}

fn resolve_optional_compact_id(
    key: Option<CompactPubkey>,
    resolver: &RegistryResolver,
    acc: &mut Accumulator,
    stats: &mut IndexStats,
    record_pubkey: bool,
) -> Option<u32> {
    resolve_compact_id(key?, resolver, acc, stats, record_pubkey)
}

fn resolve_compact_id(
    key: CompactPubkey,
    resolver: &RegistryResolver,
    acc: &mut Accumulator,
    stats: &mut IndexStats,
    record_pubkey: bool,
) -> Option<u32> {
    let id = match key {
        CompactPubkey::Id(id) => id,
        CompactPubkey::Raw(pubkey) => {
            let Some(id) = resolver.id_for_raw(&pubkey) else {
                stats.skipped_missing_keys += 1;
                return None;
            };
            if record_pubkey {
                acc.pubkeys.entry(id).or_insert(pubkey);
            }
            return Some(id);
        }
    };
    if record_pubkey
        && !acc.pubkeys.contains_key(&id)
        && let Some(pubkey) = resolver.pubkey_for_id(id)
    {
        acc.pubkeys.insert(id, pubkey);
    }
    Some(id)
}

fn message_instructions(message: &ArchiveV2HotMessagePayload) -> &[ArchiveV2HotInstruction] {
    match message {
        ArchiveV2HotMessagePayload::Legacy(message) => &message.instructions,
        ArchiveV2HotMessagePayload::V0(message) => &message.instructions,
    }
}

fn hot_region<'a>(
    bytes: &'a [u8],
    offset: u32,
    len: u32,
    label: &str,
    block_id: u32,
    tx_index: u32,
) -> Result<&'a [u8]> {
    let offset = offset as usize;
    let len = len as usize;
    let end = offset
        .checked_add(len)
        .with_context(|| format!("{label} range overflow block_id={block_id} tx={tx_index}"))?;
    bytes.get(offset..end).with_context(|| {
        format!(
            "{label} range out of bounds block_id={block_id} tx={tx_index} offset={offset} len={len} bytes={}",
            bytes.len()
        )
    })
}

#[allow(dead_code)]
fn debug_amount(amount: u64, decimals: u8) -> f64 {
    amount_to_ui(amount, decimals)
}
