//! One-pass, fail-closed SPYx market-trade builder.
//!
//! The reducer deliberately publishes only transfers that are owned by a
//! decoded venue swap's execution subtree and that reconcile with the exact
//! pre/post token-balance rows. Parser amounts are constraints, not execution
//! amounts; the persisted amounts below always come from committed SPL Token
//! CPI transfers.

use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use blockzilla_archive_v2::{ARCHIVE_V2_TX_FLAG_HAS_ERROR, ArchiveV2WireMetadataErrorSchema};
use blockzilla_dex_parser::{
    AccountRoles, DecodeOutcome, DecodedInstruction, DispatchTable, Evidence, InstructionClass,
    MalformedReason, PARSER_IMPLEMENTATION_FINGERPRINT, PARSER_SEMANTIC_VERSION, PROGRAM_SPECS,
    ProgramRole,
};
use blockzilla_primitives::CompactPubkey;
use blockzilla_read_sdk::{
    ArchiveV2MessageProjector, ArchiveV2MetadataProjectionLimits, ArchiveV2WireProfile,
    BorrowedArchiveV2InnerTokenInstruction, BorrowedArchiveV2LogEventKind,
    BorrowedArchiveV2TokenBalance, LogPayloadValidation, MAX_MESSAGE_ACCOUNTS, TokenBalanceSide,
    visit_archive_v2_compact_logs_exact_with_selected_error_schema,
    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema,
};
use blockzilla_token_balance_audit::{
    CommitStatus, DecodeStatus as TokenDecodeStatus, InstructionEffect as TokenInstructionEffect,
    InvocationLogEvent, OrderedInvocation, TokenProgram, classify_committed_invocations,
    decode_token_instruction,
};
use blockzilla_token_transaction_dump::{
    DUMP_MANIFEST_FILE, DumpWireProfile,
    consolidated_posting_projection::{
        ConsolidatedPostingProjectionScratch, project_consolidated_transaction_postings,
    },
    consolidated_reader::{
        BorrowedDumpRecord, BorrowedTransactionRecord, ConsolidatedFrameReader,
        ExactMetadataSchemaSelection,
    },
};
use sha2::{Digest, Sha256};

use crate::{
    builder::{
        DigestFileWriter, HashingReader, IO_BUFFER_BYTES, ObservedBlock, create_new_file,
        prepare_output, sync_directory, validate_block_context, validate_footer,
        validate_stream_header, validate_transaction_record,
    },
    index_format::{TransactionCoordinate, hex_digest},
    market_format::{
        MARKET_HEADER_BYTES, MARKET_MANIFEST_FILE, MARKET_OUTER_INNER_INDEX, MARKET_SCHEMA_VERSION,
        MARKET_TRADE_FLAG_BALANCE_RECONCILED, MARKET_TRADE_FLAG_COMMIT_PROVEN,
        MARKET_TRADE_FLAG_DIRECT_USD_QUOTE, MARKET_TRADE_FLAG_FEE_KNOWN, MARKET_TRADE_FLAG_INNER,
        MARKET_TRADE_FLAG_INPUT_VAULT_MATCH, MARKET_TRADE_FLAG_OUTPUT_VAULT_MATCH,
        MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED, MARKET_TRADE_FLAG_STACK_PROVEN,
        MARKET_TRADE_FLAG_TARGET_INPUT, MARKET_TRADE_FLAG_TARGET_OUTPUT,
        MARKET_TRADE_FLAG_USER_DESTINATION_MATCH, MARKET_TRADE_FLAG_USER_SOURCE_MATCH,
        MARKET_TRADE_RECORD_BYTES, MARKET_TRADES_FILE, MarketCounters, MarketDefinitions,
        MarketFileBinding, MarketFileHeader, MarketInstructionKind, MarketManifest,
        MarketParserBinding, MarketScaledUiHistory, MarketSourceBinding, MarketTargetBinding,
        MarketTradeRecord,
    },
    scaled_ui_amount::{
        DEPLOYED_LEGACY_REPLAY_SEMANTICS, ParsedScaledUiAmountInstruction,
        ScaledUiAmountCoordinate, ScaledUiAmountEvent, ScaledUiAmountEventKind,
        canonical_pubkey_hex, parse_scaled_ui_amount_occurrences,
        validate_scaled_ui_amount_history,
    },
    source::{SourceDump, load_source_dump, require_hash},
};

const WORK_DIRECTORY: &str = ".market-build-v3";
const RAW_TRADES_FILE: &str = "market-trades-v3.raw.partial";
const LEGACY_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 64;
const PROGRESS_INTERVAL: u64 = 250_000;

pub const MARKET_REDUCER_SEMANTIC_VERSION: &str = "3.0.0";
pub const MAINNET_USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const MAINNET_USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

/// Canonical Solana-mainnet USD quote mints used by the CLI when the caller
/// does not provide an explicit quote set: native USDC, then native USDT.
pub const DEFAULT_USD_QUOTE_MINTS: &[&str] = &[MAINNET_USDC_MINT, MAINNET_USDT_MINT];

/// Composite identity for the DEX decoders and the market reducer semantics.
pub fn market_parser_implementation_fingerprint() -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"blockzilla-spyx-market-reducer\0");
    hasher.update(MARKET_REDUCER_SEMANTIC_VERSION.as_bytes());
    hasher.update(b"\0blockzilla-dex-parser\0");
    hasher.update(PARSER_SEMANTIC_VERSION.as_bytes());
    hasher.update(PARSER_IMPLEMENTATION_FINGERPRINT.as_bytes());
    hasher.update(b"\0market-builder-source\0");
    hasher.update(include_bytes!("market_builder.rs"));
    hasher.update(b"\0market-format-source\0");
    hasher.update(include_bytes!("market_format.rs"));
    hasher.update(b"\0scaled-ui-amount-source\0");
    hasher.update(include_bytes!("scaled_ui_amount.rs"));
    hasher.update(b"\0commit-classifier-source\0");
    hasher.update(include_bytes!(
        "../../blockzilla-token-balance-audit/src/commit.rs"
    ));
    hasher.update(b"\0token-decoder-source\0");
    hasher.update(include_bytes!(
        "../../blockzilla-token-balance-audit/src/instruction.rs"
    ));
    hasher.update(b"\0exact-message-projector-source\0");
    hasher.update(include_bytes!(
        "../../../crates/compat/blockzilla-read-sdk-legacy/src/message_projection.rs"
    ));
    hasher.update(b"\0exact-metadata-projector-source\0");
    hasher.update(include_bytes!(
        "../../../crates/compat/blockzilla-read-sdk-legacy/src/selective_metadata.rs"
    ));
    hasher.update(b"\0posting-projection-source\0");
    hasher.update(include_bytes!(
        "../../blockzilla-token-transaction-dump/src/consolidated_posting_projection.rs"
    ));
    hex_digest(hasher.finalize().into())
}

#[derive(Debug, Clone)]
pub struct MarketBuildConfig {
    pub dump: PathBuf,
    pub output: PathBuf,
    pub max_transactions: Option<u64>,
    pub usd_quote_mints: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MarketBuildSummary {
    pub output: PathBuf,
    pub complete: bool,
    pub transactions: u64,
    pub trades: u64,
    pub trade_bytes: u64,
}

#[derive(Debug)]
struct RegistryBindings {
    dispatch: DispatchTable,
    target_mint_id: u32,
    legacy_token_program_id: Option<u32>,
    token_2022_program_id: Option<u32>,
    usd_quote_mint_ids: Vec<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TokenBalance {
    mint_id: u32,
    amount: u64,
    decimals: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BalanceSlot {
    pre: Option<TokenBalance>,
    post: Option<TokenBalance>,
    invalid: bool,
}

impl BalanceSlot {
    const EMPTY: Self = Self {
        pre: None,
        post: None,
        invalid: false,
    };

    fn token_identity(self) -> Option<(u32, u8)> {
        if self.invalid {
            return None;
        }
        match (self.pre, self.post) {
            (Some(pre), Some(post))
                if pre.mint_id == post.mint_id && pre.decimals == post.decimals =>
            {
                Some((pre.mint_id, pre.decimals))
            }
            (Some(pre), None) => Some((pre.mint_id, pre.decimals)),
            (None, Some(post)) => Some((post.mint_id, post.decimals)),
            _ => None,
        }
    }

    fn observed_delta(self) -> Option<i128> {
        self.token_identity()?;
        Some(
            i128::from(self.post.map_or(0, |row| row.amount))
                - i128::from(self.pre.map_or(0, |row| row.amount)),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FrameOwner {
    Venue(usize),
    Router(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ActiveFrame {
    height: u32,
    owner: FrameOwner,
}

#[derive(Clone, Copy, Debug)]
struct Candidate {
    decoded: DecodedInstruction,
    dex_program_id: u32,
    router_program_id: u32,
    outer_index: u32,
    inner_index: u32,
    stack_height: u32,
    stack_proven: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenTransferKind {
    Transfer,
    TransferChecked,
    TransferCheckedWithFee,
}

#[derive(Clone, Copy, Debug)]
struct RawTransfer {
    owner: Option<usize>,
    outer_index: u32,
    inner_index: u32,
    source_index: usize,
    destination_index: usize,
    checked_mint_index: Option<usize>,
    amount: u64,
    expected_fee: u64,
    checked_decimals: Option<u8>,
    kind: TokenTransferKind,
}

#[derive(Clone, Copy, Debug)]
struct OpaqueTokenEffect {
    owner: Option<usize>,
    outer_index: u32,
    inner_index: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StagedInnerInvocation {
    outer_index: u32,
    inner_index: u32,
    program_id: u32,
    stack_height: Option<u32>,
}

#[derive(Clone, Copy, Debug)]
struct ResolvedTransfer {
    owner: Option<usize>,
    outer_index: u32,
    inner_index: u32,
    source_index: usize,
    destination_index: usize,
    source_id: u32,
    destination_id: u32,
    mint_id: u32,
    decimals: u8,
    debit_amount: u64,
    credit_amount: u64,
    fee_amount: u64,
    fee_known: bool,
    scaled_ui_config_id: u32,
}

#[derive(Clone, Debug)]
struct StagedScaledUiEvent {
    coordinate: ScaledUiAmountCoordinate,
    instruction: ParsedScaledUiAmountInstruction,
    authority_registry_id: Option<u32>,
}

#[derive(Debug)]
struct TransactionStage {
    outer_frames: [Option<FrameOwner>; MAX_MESSAGE_ACCOUNTS],
    outer_program_ids: [u32; MAX_MESSAGE_ACCOUNTS],
    outer_count: usize,
    inner_ordinals: [u32; MAX_MESSAGE_ACCOUNTS],
    group_seen: [bool; MAX_MESSAGE_ACCOUNTS],
    stack_invalid: [bool; MAX_MESSAGE_ACCOUNTS],
    active: [Option<ActiveFrame>; MAX_MESSAGE_ACCOUNTS],
    active_len: usize,
    current_outer: Option<usize>,
    previous_height: Option<u32>,
    candidates: Vec<Candidate>,
    raw_transfers: Vec<RawTransfer>,
    opaque_token_effects: Vec<OpaqueTokenEffect>,
    unowned_token_barrier: Option<CandidateRejection>,
    inner_invocations: Vec<StagedInnerInvocation>,
    ordered_invocations: Vec<OrderedInvocation>,
    log_events: Vec<InvocationLogEvent>,
    log_stack: Vec<[u8; 32]>,
    resolved_transfers: Vec<ResolvedTransfer>,
    scaled_ui_events: Vec<StagedScaledUiEvent>,
    scaled_ui_error: Option<String>,
    candidate_failures: Vec<Option<CandidateRejection>>,
    pending_failures: Vec<(usize, CandidateRejection)>,
    output_records: Vec<MarketTradeRecord>,
    balances: [BalanceSlot; MAX_MESSAGE_ACCOUNTS],
    debit: [u128; MAX_MESSAGE_ACCOUNTS],
    credit: [u128; MAX_MESSAGE_ACCOUNTS],
    callback_invalid: bool,
}

impl TransactionStage {
    fn new() -> Self {
        Self {
            outer_frames: [None; MAX_MESSAGE_ACCOUNTS],
            outer_program_ids: [0; MAX_MESSAGE_ACCOUNTS],
            outer_count: 0,
            inner_ordinals: [0; MAX_MESSAGE_ACCOUNTS],
            group_seen: [false; MAX_MESSAGE_ACCOUNTS],
            stack_invalid: [false; MAX_MESSAGE_ACCOUNTS],
            active: [None; MAX_MESSAGE_ACCOUNTS],
            active_len: 0,
            current_outer: None,
            previous_height: None,
            candidates: Vec::new(),
            raw_transfers: Vec::new(),
            opaque_token_effects: Vec::new(),
            unowned_token_barrier: None,
            inner_invocations: Vec::new(),
            ordered_invocations: Vec::new(),
            log_events: Vec::new(),
            log_stack: Vec::new(),
            resolved_transfers: Vec::new(),
            scaled_ui_events: Vec::new(),
            scaled_ui_error: None,
            candidate_failures: Vec::new(),
            pending_failures: Vec::new(),
            output_records: Vec::new(),
            balances: [BalanceSlot::EMPTY; MAX_MESSAGE_ACCOUNTS],
            debit: [0; MAX_MESSAGE_ACCOUNTS],
            credit: [0; MAX_MESSAGE_ACCOUNTS],
            callback_invalid: false,
        }
    }

    fn begin(&mut self) {
        self.outer_frames.fill(None);
        self.outer_program_ids.fill(0);
        self.outer_count = 0;
        self.inner_ordinals.fill(0);
        self.group_seen.fill(false);
        self.stack_invalid.fill(false);
        self.active.fill(None);
        self.active_len = 0;
        self.current_outer = None;
        self.previous_height = None;
        self.candidates.clear();
        self.raw_transfers.clear();
        self.opaque_token_effects.clear();
        self.unowned_token_barrier = None;
        self.inner_invocations.clear();
        self.ordered_invocations.clear();
        self.log_events.clear();
        self.log_stack.clear();
        self.resolved_transfers.clear();
        self.scaled_ui_events.clear();
        self.scaled_ui_error = None;
        self.candidate_failures.clear();
        self.pending_failures.clear();
        self.output_records.clear();
        self.balances.fill(BalanceSlot::EMPTY);
        self.debit.fill(0);
        self.credit.fill(0);
        self.callback_invalid = false;
    }

    fn begin_inner_group(&mut self, outer_index: usize) {
        if self.current_outer == Some(outer_index) {
            return;
        }
        if self
            .current_outer
            .is_some_and(|previous| outer_index <= previous)
        {
            self.callback_invalid = true;
            return;
        }
        if outer_index >= MAX_MESSAGE_ACCOUNTS || self.group_seen[outer_index] {
            self.callback_invalid = true;
            return;
        }
        self.group_seen[outer_index] = true;
        self.current_outer = Some(outer_index);
        self.previous_height = Some(1);
        self.active_len = 0;
        if let Some(owner) = self.outer_frames[outer_index] {
            self.active[0] = Some(ActiveFrame { height: 1, owner });
            self.active_len = 1;
        }
    }

    fn prepare_height(&mut self, outer_index: usize, height: Option<u32>) -> Option<u32> {
        self.begin_inner_group(outer_index);
        let Some(height) = height else {
            self.stack_invalid[outer_index] = true;
            return None;
        };
        if height < 2
            || self.previous_height.is_some_and(|previous| {
                previous
                    .checked_add(1)
                    .is_none_or(|maximum_next| height > maximum_next)
            })
        {
            self.stack_invalid[outer_index] = true;
        }
        self.previous_height = Some(height);
        while self.active_len != 0
            && self.active[self.active_len - 1].is_some_and(|frame| frame.height >= height)
        {
            self.active_len -= 1;
            self.active[self.active_len] = None;
        }
        Some(height)
    }

    fn deepest_venue(&self) -> Option<usize> {
        self.active[..self.active_len]
            .iter()
            .rev()
            .flatten()
            .find_map(|frame| match frame.owner {
                FrameOwner::Venue(index) => Some(index),
                FrameOwner::Router(_) => None,
            })
    }

    fn deepest_router(&self) -> u32 {
        self.active[..self.active_len]
            .iter()
            .rev()
            .flatten()
            .find_map(|frame| match frame.owner {
                FrameOwner::Router(program_id) => Some(program_id),
                FrameOwner::Venue(_) => None,
            })
            .unwrap_or(0)
    }

    fn push_frame(&mut self, height: u32, owner: FrameOwner) {
        if self.active_len == self.active.len() {
            self.callback_invalid = true;
            return;
        }
        self.active[self.active_len] = Some(ActiveFrame { height, owner });
        self.active_len += 1;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CandidateRejection {
    MissingBlockTime,
    MissingStack,
    UncommittedInvocation,
    MissingTokenBalance,
    UnsupportedTokenInstruction,
    TransferOutsideSubtree,
    UnresolvedFlow,
    AmbiguousFlow,
    TargetSides,
    ZeroAmount,
    DecimalMismatch,
    BalanceMismatch,
    ArithmeticOverflow,
}

#[derive(Clone, Copy, Debug, Default)]
struct DirectionalFlow {
    mint_id: u32,
    decimals: u8,
    amount: u64,
    transfer_count: u16,
    role_flags: u16,
    fee_amount: u64,
    fee_mint_id: u32,
    fee_known: bool,
    scaled_ui_config_id: u32,
    initialized: bool,
}

#[derive(Debug, Default)]
struct InstructionKindRegistry {
    entries: Vec<MarketInstructionKind>,
    keys: Vec<InstructionKindKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InstructionKindKey {
    program_index: usize,
    name: &'static str,
    discriminator: [u8; 8],
    discriminator_len: u8,
}

impl InstructionKindRegistry {
    fn intern(&mut self, decoded: DecodedInstruction) -> Result<u32> {
        let key = InstructionKindKey {
            program_index: decoded.program as usize,
            name: decoded.name,
            discriminator: decoded.discriminator.bytes,
            discriminator_len: decoded.discriminator.len,
        };
        if let Some(index) = self.keys.iter().position(|candidate| *candidate == key) {
            return u32::try_from(index + 1).context("instruction kind ID exceeds u32");
        }
        ensure!(
            matches!(key.discriminator_len, 1 | 5 | 8),
            "DEX parser returned a non-canonical discriminator length"
        );
        let spec = PROGRAM_SPECS
            .get(key.program_index)
            .context("decoded DEX program is outside program specifications")?;
        let discriminator = hex_bytes(&key.discriminator[..usize::from(key.discriminator_len)]);
        let id = u32::try_from(self.keys.len() + 1).context("instruction kind ID exceeds u32")?;
        self.keys.push(key);
        self.entries.push(MarketInstructionKind {
            id,
            program: spec.address.to_owned(),
            name: key.name.to_owned(),
            discriminator,
        });
        Ok(id)
    }
}

pub fn build_market(config: &MarketBuildConfig) -> Result<MarketBuildSummary> {
    if let Some(maximum) = config.max_transactions {
        ensure!(maximum != 0, "--max-transactions must be positive");
    }
    ensure!(
        !config.usd_quote_mints.is_empty(),
        "at least one --usd-quote-mint is required"
    );
    ensure!(
        config
            .usd_quote_mints
            .iter()
            .all(|mint| DEFAULT_USD_QUOTE_MINTS.contains(&mint.as_str())),
        "USD quote mints are restricted to canonical Solana-mainnet USDC and USDT"
    );
    let source = load_source_dump(&config.dump)?;
    require_hash(&source.registry_handle, source.registry_sha256, "registry")?;
    let registry_entries = u32::try_from(source.pubkeys).context("registry size exceeds u32")?;
    let bindings = bind_registry(&source, registry_entries, &config.usd_quote_mints)?;
    source.verify_file_identities()?;

    let output = prepare_output(&config.output, &source.root)?;
    let work = output.join(WORK_DIRECTORY);
    fs::create_dir(&work).with_context(|| format!("create {}", work.display()))?;
    let raw_path = work.join(RAW_TRADES_FILE);
    let mut raw_writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(&raw_path)?);

    let canary_max_transactions =
        effective_canary_limit(config.max_transactions, source.manifest.transactions);
    let complete = canary_max_transactions.is_none();
    let target_transactions = canary_max_transactions.unwrap_or(source.manifest.transactions);
    if !complete {
        // A prefix cannot observe the full manifest-bound digest while it is
        // decoded. Verify it once so a canary never publishes an unverified
        // full-stream identity in its header and manifest.
        require_hash(
            &source.transaction_handle,
            source.transaction_sha256,
            "transaction stream",
        )?;
    }
    let transaction_reader = HashingReader::new(source.transaction_handle.file());
    let mut transactions = ConsolidatedFrameReader::new(transaction_reader);
    validate_stream_header(&source, &mut transactions)?;

    let mut projection_scratch = ConsolidatedPostingProjectionScratch::new(registry_entries)?;
    let mut account_scratch = [0u32; MAX_MESSAGE_ACCOUNTS];
    let mut stage = TransactionStage::new();
    let mut kinds = InstructionKindRegistry::default();
    let mut counters = empty_counters();
    let mut transaction_count = 0u64;
    let mut signature_count = 0u64;
    let mut previous_coordinate = None;
    let mut previous_block = None::<ObservedBlock>;
    let mut target_decimals = None;
    let mut scaled_ui_events = Vec::<ScaledUiAmountEvent>::new();
    let started = Instant::now();

    while transaction_count < target_transactions {
        let frame = transactions
            .next_frame()?
            .context("consolidated stream ended before the requested transaction count")?;
        let BorrowedDumpRecord::Transaction(record) = frame.record else {
            bail!("consolidated stream has a non-transaction before the requested count")
        };
        let coordinate = TransactionCoordinate {
            epoch: record.source_epoch,
            slot: record.block.slot,
            source_block_id: record.source_block_id,
            tx_index: record.tx_index,
        };
        ensure!(
            previous_coordinate.is_none_or(|previous| previous < coordinate),
            "consolidated transactions are not in canonical coordinate order"
        );
        previous_coordinate = Some(coordinate);
        validate_transaction_record(&source, &record, signature_count)?;
        validate_block_context(&record.block, coordinate, &mut previous_block)?;
        signature_count = signature_count
            .checked_add(u64::from(record.signature_count))
            .context("signature count overflow")?;
        counters.source_transactions = counters
            .source_transactions
            .checked_add(1)
            .context("source transaction counter overflow")?;

        stage.begin();
        let projection = project_consolidated_transaction_postings(
            &record,
            registry_entries,
            &mut projection_scratch,
        )
        .with_context(|| {
            format!(
                "project market transaction at epoch {} slot {} transaction {}",
                record.source_epoch, record.block.slot, record.tx_index
            )
        })?;
        let resolved_accounts = projection.resolved_account_registry_ids;
        let metadata_schema = projection.metadata_schema;
        let failed = record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0;
        match transaction_eligibility(failed, metadata_schema) {
            TransactionEligibility::Failed => {
                checked_counter(&mut counters.failed_transactions, "failed transactions")?;
                checked_counter(
                    &mut counters.rejected_failed_transaction,
                    "rejected failed transactions",
                )?;
            }
            TransactionEligibility::MetadataAbsent => {
                ensure!(
                    !resolved_accounts.contains(&bindings.target_mint_id),
                    "successful metadata-absent target transaction could hide a Scaled UI Amount CPI"
                );
                checked_counter(
                    &mut counters.metadata_absent_transactions,
                    "metadata-absent transactions",
                )?;
                checked_counter(
                    &mut counters.rejected_missing_metadata,
                    "rejected metadata-absent transactions",
                )?;
            }
            TransactionEligibility::Eligible => {
                checked_counter(
                    &mut counters.successful_transactions,
                    "successful transactions",
                )?;
                process_transaction(
                    &source,
                    transaction_count,
                    &record,
                    metadata_schema,
                    resolved_accounts,
                    registry_entries,
                    &bindings,
                    &mut account_scratch,
                    &mut stage,
                    &mut kinds,
                    &mut counters,
                    &mut target_decimals,
                    &mut scaled_ui_events,
                    &mut raw_writer,
                )?;
            }
        }

        transaction_count = transaction_count
            .checked_add(1)
            .context("transaction count overflow")?;
        if transaction_count.is_multiple_of(PROGRESS_INTERVAL)
            || transaction_count == target_transactions
        {
            report_progress(
                transaction_count,
                target_transactions,
                counters.emitted_trades,
                transactions.logical_offset(),
                started,
            );
        }
    }

    if complete {
        let footer_frame = transactions
            .next_frame()?
            .context("consolidated transaction stream has no footer")?;
        let BorrowedDumpRecord::Footer(footer) = footer_frame.record else {
            bail!("consolidated stream does not end after the manifest transaction count")
        };
        ensure!(
            transactions.next_frame()?.is_none(),
            "consolidated stream has records after its footer"
        );
        validate_footer(&source, footer, transaction_count, signature_count)?;
        ensure!(
            transactions.logical_offset() == source.transaction_bytes,
            "transaction stream byte length changed while it was scanned"
        );
        ensure!(
            transactions.get_ref().digest() == source.transaction_sha256,
            "transaction stream digest differs from its manifest"
        );
    }
    ensure!(
        counters.source_transactions == transaction_count,
        "market counter transaction total differs"
    );
    validate_counter_partition(&counters)?;
    source.verify_file_identities()?;
    raw_writer.flush()?;
    raw_writer.get_ref().sync_all()?;
    drop(raw_writer);

    let target_decimals = target_decimals.context(
        "target mint decimals were not present in an exact successful token-balance row",
    )?;
    validate_scaled_ui_amount_history(&scaled_ui_events, bindings.target_mint_id)?;
    let trades_binding =
        finish_trade_file(&source, &work, complete, counters.emitted_trades, &raw_path)?;
    source.verify_file_identities()?;

    let manifest = build_manifest(
        &source,
        complete,
        canary_max_transactions,
        bindings.target_mint_id,
        target_decimals,
        bindings.usd_quote_mint_ids,
        kinds.entries,
        scaled_ui_events,
        counters,
        trades_binding,
    )?;
    publish_market(&output, &work, &manifest)?;

    Ok(MarketBuildSummary {
        output,
        complete,
        transactions: transaction_count,
        trades: manifest.counters.emitted_trades,
        trade_bytes: manifest.trades.bytes,
    })
}

fn bind_registry(
    source: &SourceDump,
    registry_entries: u32,
    quote_mints: &[String],
) -> Result<RegistryBindings> {
    ensure!(
        source.registry_bytes
            == u64::from(registry_entries)
                .checked_mul(KEY_BYTES as u64)
                .context("registry byte length overflow")?,
        "registry byte length differs from its row count"
    );
    for (index, spec) in PROGRAM_SPECS.iter().enumerate() {
        ensure!(
            spec.program as usize == index && spec.role == spec.program.role(),
            "DEX parser specifications are not in enum order"
        );
    }

    let target_key = source.mint;
    let legacy_key = parse_pubkey(LEGACY_TOKEN_PROGRAM, "legacy SPL Token program")?;
    let token_2022_key = parse_pubkey(TOKEN_2022_PROGRAM, "SPL Token-2022 program")?;
    let mut parser_keys = PROGRAM_SPECS
        .iter()
        .enumerate()
        .map(|(index, spec)| Ok((parse_pubkey(spec.address, "DEX parser program ID")?, index)))
        .collect::<Result<Vec<_>>>()?;
    parser_keys.sort_unstable_by_key(|entry| entry.0);
    ensure!(
        parser_keys.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "DEX parser program IDs are not unique"
    );
    let mut quote_keys = quote_mints
        .iter()
        .enumerate()
        .map(|(index, mint)| Ok((parse_pubkey(mint, "USD quote mint")?, index)))
        .collect::<Result<Vec<_>>>()?;
    quote_keys.sort_unstable_by_key(|entry| entry.0);
    ensure!(
        quote_keys.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "USD quote mint list contains a duplicate"
    );
    ensure!(
        quote_keys.iter().all(|entry| entry.0 != target_key),
        "target mint cannot also be a USD quote mint"
    );

    let mut parser_ids = vec![None; PROGRAM_SPECS.len()];
    let mut quote_ids = vec![None; quote_mints.len()];
    let mut target_id = None;
    let mut legacy_id = None;
    let mut token_2022_id = None;
    let mut previous = None;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, source.registry_handle.file());
    for ordinal in 0..registry_entries {
        let mut key = [0u8; KEY_BYTES];
        reader
            .read_exact(&mut key)
            .with_context(|| format!("read registry row {}", ordinal + 1))?;
        ensure!(
            previous.is_none_or(|value| value < key),
            "registry is not strictly sorted and unique"
        );
        previous = Some(key);
        let id = ordinal + 1;
        if key == target_key {
            target_id = Some(id);
        }
        if key == legacy_key {
            legacy_id = Some(id);
        }
        if key == token_2022_key {
            token_2022_id = Some(id);
        }
        if let Ok(position) = parser_keys.binary_search_by_key(&key, |entry| entry.0) {
            parser_ids[parser_keys[position].1] = Some(id);
        }
        if let Ok(position) = quote_keys.binary_search_by_key(&key, |entry| entry.0) {
            quote_ids[quote_keys[position].1] = Some(id);
        }
    }
    let mut extra = [0u8; 1];
    ensure!(reader.read(&mut extra)? == 0, "registry has trailing bytes");

    let target_mint_id = target_id.context("target mint is absent from registry")?;
    // A configured quote that is absent from the immutable registry cannot
    // occur in this dump. Keep only present quotes so the portable mainnet
    // default does not reject a dump that never touched one of them.
    let mut usd_quote_mint_ids = quote_ids.into_iter().flatten().collect::<Vec<_>>();
    ensure!(
        !usd_quote_mint_ids.is_empty(),
        "none of the configured USD quote mints is present in the source registry"
    );
    usd_quote_mint_ids.sort_unstable();
    ensure!(
        usd_quote_mint_ids.windows(2).all(|pair| pair[0] < pair[1]),
        "USD quote mint IDs are not unique"
    );
    let dense_len = usize::try_from(registry_entries)?
        .checked_add(1)
        .context("DEX dispatch table length overflow")?;
    let dispatch = DispatchTable::from_resolver(dense_len, |address| {
        PROGRAM_SPECS
            .iter()
            .position(|spec| spec.address == address)
            .and_then(|index| parser_ids[index])
    });
    ensure!(
        dispatch.len() == dense_len,
        "DEX dispatch table length differs"
    );
    Ok(RegistryBindings {
        dispatch,
        target_mint_id,
        legacy_token_program_id: legacy_id,
        token_2022_program_id: token_2022_id,
        usd_quote_mint_ids,
    })
}

#[allow(clippy::too_many_arguments)]
fn process_transaction(
    source: &SourceDump,
    transaction_id: u64,
    record: &BorrowedTransactionRecord<'_>,
    metadata_schema: ExactMetadataSchemaSelection,
    resolved_accounts: &[u32],
    registry_entries: u32,
    bindings: &RegistryBindings,
    account_scratch: &mut [u32; MAX_MESSAGE_ACCOUNTS],
    stage: &mut TransactionStage,
    kinds: &mut InstructionKindRegistry,
    counters: &mut MarketCounters,
    target_decimals: &mut Option<u8>,
    scaled_ui_history: &mut Vec<ScaledUiAmountEvent>,
    writer: &mut BufWriter<File>,
) -> Result<()> {
    let schema = metadata_schema
        .selected_schema()
        .context("eligible transaction has no selected metadata schema")?;
    let mut outer_index = 0usize;
    let message = projector(record.source_wire_profile)
        .visit_static_accounts_and_instructions_exact(
            record.message_bytes,
            registry_entries,
            |_, _| {},
            |instruction| {
                let index = outer_index;
                outer_index = outer_index.saturating_add(1);
                if index >= MAX_MESSAGE_ACCOUNTS {
                    stage.callback_invalid = true;
                    return;
                }
                let program_index = usize::from(instruction.program_id_index);
                let Some(&program_id) = resolved_accounts.get(program_index) else {
                    stage.callback_invalid = true;
                    return;
                };
                stage.outer_program_ids[index] = program_id;
                let token_program = if Some(program_id) == bindings.legacy_token_program_id {
                    Some(TokenProgram::Legacy)
                } else if Some(program_id) == bindings.token_2022_program_id {
                    Some(TokenProgram::Token2022)
                } else {
                    None
                };
                if let Some(token_program) = token_program {
                    if token_program == TokenProgram::Token2022 {
                        let Some(accounts) = resolve_accounts(
                            instruction.accounts,
                            resolved_accounts,
                            account_scratch,
                        ) else {
                            stage.callback_invalid = true;
                            return;
                        };
                        match instruction.raw_data {
                            Some(data) => {
                                if let Err(error) = stage_scaled_ui_events(
                                    transaction_id,
                                    record,
                                    u32::try_from(index).unwrap_or(u32::MAX),
                                    None,
                                    1,
                                    data,
                                    accounts,
                                    bindings.target_mint_id,
                                    stage,
                                ) {
                                    stage.scaled_ui_error = Some(error.to_string());
                                    return;
                                }
                            }
                            None if accounts.first().copied() == Some(bindings.target_mint_id) => {
                                stage.scaled_ui_error = Some(
                                    "target Token-2022 instruction has no exact data".to_owned(),
                                );
                                return;
                            }
                            None => {}
                        }
                    }
                    // A top-level token instruction cannot belong to a venue
                    // subtree. Fail closed when it can change a public token
                    // amount, because it can otherwise hide an offsetting
                    // effect inside an apparently reconciled swap account.
                    if instruction.raw_data.is_none_or(|data| {
                        token_instruction_may_change_balance(token_program, data)
                    }) {
                        stage
                            .unowned_token_barrier
                            .get_or_insert(CandidateRejection::TransferOutsideSubtree);
                    }
                    return;
                }
                checked_counter_callback(&mut counters.instructions_examined);
                let Some(data) = instruction.raw_data else {
                    if bindings.dispatch.program(program_id).is_some() {
                        checked_counter_callback(&mut counters.parser_program_hits);
                        checked_counter_callback(&mut counters.rejected_missing_instruction_data);
                    } else {
                        checked_counter_callback(&mut counters.rejected_unsupported_program);
                    }
                    return;
                };
                let Some(accounts) =
                    resolve_accounts(instruction.accounts, resolved_accounts, account_scratch)
                else {
                    stage.callback_invalid = true;
                    return;
                };
                stage.outer_frames[index] = classify_dex_instruction(
                    &bindings.dispatch,
                    program_id,
                    data,
                    accounts,
                    u32::try_from(index).unwrap_or(u32::MAX),
                    MARKET_OUTER_INNER_INDEX,
                    1,
                    true,
                    0,
                    &mut stage.candidates,
                    counters,
                );
            },
        )
        .context("decode exact market transaction message")?;
    ensure!(
        outer_index == message.instruction_count && !stage.callback_invalid,
        "market message callbacks differ from its exact projection"
    );
    if let Some(error) = stage.scaled_ui_error.take() {
        bail!("decode target Scaled UI Amount instruction: {error}");
    }
    stage.outer_count = outer_index;

    // Keep balance callbacks in a disjoint local stage. Rust then proves that
    // the inner-instruction callback has exclusive access to the execution
    // tracker for the full visitor call.
    let mut staged_balances = [BalanceSlot::EMPTY; MAX_MESSAGE_ACCOUNTS];
    let mut balance_callback_invalid = false;
    visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
        record.metadata_bytes,
        schema,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts: resolved_accounts.len(),
            top_level_instruction_count: message.instruction_count,
        },
        registry_entries,
        LogPayloadValidation::StructureOnly,
        |outer, instruction: BorrowedArchiveV2InnerTokenInstruction<'_>| {
            process_inner_instruction(
                transaction_id,
                record,
                usize::try_from(outer).unwrap_or(MAX_MESSAGE_ACCOUNTS),
                instruction,
                resolved_accounts,
                bindings,
                account_scratch,
                stage,
                counters,
            );
        },
        |side, balance| {
            stage_balance(
                side,
                balance,
                resolved_accounts.len(),
                &mut staged_balances,
                &mut balance_callback_invalid,
            );
        },
        |_, _, _| {},
    )
    .context("decode exact selected market transaction metadata")?;
    ensure!(
        !stage.callback_invalid && !balance_callback_invalid,
        "market metadata callbacks differ from exact resolved accounts"
    );
    if let Some(error) = stage.scaled_ui_error.take() {
        bail!("decode target Scaled UI Amount CPI: {error}");
    }
    stage.balances = staged_balances;
    prove_committed_invocations(
        record.metadata_bytes,
        schema,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts: resolved_accounts.len(),
            top_level_instruction_count: message.instruction_count,
        },
        registry_entries,
        stage,
        counters,
    )?;
    for candidate in &mut stage.candidates {
        let outer = usize::try_from(candidate.outer_index).unwrap_or(MAX_MESSAGE_ACCOUNTS);
        candidate.stack_proven &= outer < MAX_MESSAGE_ACCOUNTS && !stage.stack_invalid[outer];
    }
    let transaction_scaled_ui_events = materialize_scaled_ui_events(
        source,
        record,
        stage,
        scaled_ui_history.len(),
        bindings.target_mint_id,
    )?;
    observe_target_decimals(&stage.balances, bindings.target_mint_id, target_decimals)?;
    resolve_transfers(stage, resolved_accounts, counters)?;
    bind_scaled_ui_transfer_configs(
        stage,
        transaction_id,
        scaled_ui_history.len(),
        &transaction_scaled_ui_events,
        bindings.target_mint_id,
    )?;
    emit_candidates(
        transaction_id,
        record,
        bindings,
        stage,
        kinds,
        counters,
        writer,
    )?;
    scaled_ui_history.extend(transaction_scaled_ui_events);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_inner_instruction(
    transaction_id: u64,
    record: &BorrowedTransactionRecord<'_>,
    outer_index: usize,
    instruction: BorrowedArchiveV2InnerTokenInstruction<'_>,
    resolved_accounts: &[u32],
    bindings: &RegistryBindings,
    account_scratch: &mut [u32; MAX_MESSAGE_ACCOUNTS],
    stage: &mut TransactionStage,
    counters: &mut MarketCounters,
) {
    if outer_index >= MAX_MESSAGE_ACCOUNTS {
        stage.callback_invalid = true;
        return;
    }
    let inner_index = stage.inner_ordinals[outer_index];
    stage.inner_ordinals[outer_index] = inner_index.saturating_add(1);
    let height = stage.prepare_height(outer_index, instruction.stack_height);
    let program_index =
        usize::try_from(instruction.program_id_index).unwrap_or(MAX_MESSAGE_ACCOUNTS);
    let Some(&program_id) = resolved_accounts.get(program_index) else {
        stage.callback_invalid = true;
        return;
    };
    stage.inner_invocations.push(StagedInnerInvocation {
        outer_index: u32::try_from(outer_index).unwrap_or(u32::MAX),
        inner_index,
        program_id,
        stack_height: instruction.stack_height,
    });
    let token_program = if Some(program_id) == bindings.legacy_token_program_id {
        Some(TokenProgram::Legacy)
    } else if Some(program_id) == bindings.token_2022_program_id {
        Some(TokenProgram::Token2022)
    } else {
        None
    };
    if let Some(token_program) = token_program {
        if token_program == TokenProgram::Token2022 {
            let Some(accounts) =
                resolve_accounts(instruction.accounts, resolved_accounts, account_scratch)
            else {
                stage.callback_invalid = true;
                return;
            };
            if let Err(error) = stage_scaled_ui_events(
                transaction_id,
                record,
                u32::try_from(outer_index).unwrap_or(u32::MAX),
                Some(inner_index),
                instruction.stack_height.unwrap_or(0),
                instruction.data,
                accounts,
                bindings.target_mint_id,
                stage,
            ) {
                stage.scaled_ui_error = Some(error.to_string());
                return;
            }
        }
        process_token_transfer(
            token_program,
            u32::try_from(outer_index).unwrap_or(u32::MAX),
            inner_index,
            instruction,
            resolved_accounts,
            stage.deepest_venue(),
            &mut stage.raw_transfers,
            &mut stage.opaque_token_effects,
            counters,
        );
        return;
    }
    checked_counter_callback(&mut counters.instructions_examined);

    let Some(accounts) = resolve_accounts(instruction.accounts, resolved_accounts, account_scratch)
    else {
        stage.callback_invalid = true;
        return;
    };
    let router_program_id = stage.deepest_router();
    let frame = classify_dex_instruction(
        &bindings.dispatch,
        program_id,
        instruction.data,
        accounts,
        u32::try_from(outer_index).unwrap_or(u32::MAX),
        inner_index,
        height.unwrap_or(0),
        height.is_some(),
        router_program_id,
        &mut stage.candidates,
        counters,
    );
    if let (Some(height), Some(frame)) = (height, frame) {
        stage.push_frame(height, frame);
    }
}

#[allow(clippy::too_many_arguments)]
fn stage_scaled_ui_events(
    transaction_id: u64,
    record: &BorrowedTransactionRecord<'_>,
    outer_index: u32,
    inner_index: Option<u32>,
    stack_height: u32,
    data: &[u8],
    instruction_accounts: &[u32],
    target_mint_id: u32,
    stage: &mut TransactionStage,
) -> Result<()> {
    let occurrences = parse_scaled_ui_amount_occurrences(data)?;
    for occurrence in occurrences {
        let range = occurrence.account_range(instruction_accounts.len())?;
        let accounts = &instruction_accounts[range];
        if accounts.first().copied() != Some(target_mint_id) {
            continue;
        }
        let block_time = record
            .block
            .block_time
            .context("target Scaled UI Amount instruction has no block time")?;
        let authority_registry_id = match &occurrence.instruction {
            ParsedScaledUiAmountInstruction::Initialize { .. } => None,
            ParsedScaledUiAmountInstruction::UpdateMultiplier { .. } => Some(
                *accounts
                    .get(1)
                    .context("Scaled UI update has no authority account")?,
            ),
        };
        stage.scaled_ui_events.push(StagedScaledUiEvent {
            coordinate: ScaledUiAmountCoordinate {
                transaction_id,
                source_epoch: record.source_epoch,
                slot: record.block.slot,
                block_time,
                source_block_id: record.source_block_id,
                tx_index: record.tx_index,
                outer_index,
                inner_index,
                stack_height,
                batch_index: occurrence.batch_index,
            },
            instruction: occurrence.instruction,
            authority_registry_id,
        });
    }
    Ok(())
}

fn materialize_scaled_ui_events(
    source: &SourceDump,
    record: &BorrowedTransactionRecord<'_>,
    stage: &mut TransactionStage,
    existing_event_count: usize,
    target_mint_id: u32,
) -> Result<Vec<ScaledUiAmountEvent>> {
    if stage.scaled_ui_events.is_empty() {
        return Ok(Vec::new());
    }
    stage
        .scaled_ui_events
        .sort_unstable_by_key(|event| event.coordinate.canonical_order_key());
    ensure!(
        stage
            .scaled_ui_events
            .windows(2)
            .all(|pair| pair[0].coordinate < pair[1].coordinate),
        "target Scaled UI Amount events are not strictly ordered and unique"
    );
    let signature = read_transaction_signature(source, record)?;
    let mut events = Vec::with_capacity(stage.scaled_ui_events.len());
    for (index, staged) in stage.scaled_ui_events.iter().enumerate() {
        let config_id = existing_event_count
            .checked_add(index)
            .and_then(|value| value.checked_add(1))
            .and_then(|value| u32::try_from(value).ok())
            .context("Scaled UI config ID exceeds u32")?;
        let (kind, multiplier, effective_timestamp, configured_authority_hex) =
            match &staged.instruction {
                ParsedScaledUiAmountInstruction::Initialize {
                    authority,
                    multiplier,
                } => (
                    ScaledUiAmountEventKind::Initialize,
                    multiplier.clone(),
                    0,
                    authority.map(canonical_pubkey_hex),
                ),
                ParsedScaledUiAmountInstruction::UpdateMultiplier {
                    multiplier,
                    effective_timestamp,
                } => (
                    ScaledUiAmountEventKind::UpdateMultiplier,
                    multiplier.clone(),
                    *effective_timestamp,
                    None,
                ),
            };
        let event = ScaledUiAmountEvent {
            config_id,
            coordinate: staged.coordinate,
            signature: signature.clone(),
            target_mint_id,
            kind,
            multiplier,
            effective_timestamp,
            authority_registry_id: staged.authority_registry_id,
            configured_authority_hex,
            commit_proven: true,
        };
        event.validate()?;
        events.push(event);
    }
    Ok(events)
}

fn bind_scaled_ui_transfer_configs(
    stage: &mut TransactionStage,
    transaction_id: u64,
    existing_event_count: usize,
    transaction_events: &[ScaledUiAmountEvent],
    target_mint_id: u32,
) -> Result<()> {
    let previous_config_id =
        u32::try_from(existing_event_count).context("Scaled UI event count exceeds u32")?;
    for transfer in &mut stage.resolved_transfers {
        if transfer.mint_id != target_mint_id {
            continue;
        }
        let transfer_key = (
            transaction_id,
            transfer.outer_index,
            u64::from(transfer.inner_index) + 1,
            0u64,
        );
        let mut config_id = previous_config_id;
        for event in transaction_events {
            if event.coordinate.canonical_order_key() >= transfer_key {
                break;
            }
            config_id = event.config_id;
        }
        ensure!(
            config_id != 0,
            "target transfer occurs before Scaled UI Amount initialization"
        );
        transfer.scaled_ui_config_id = config_id;
    }
    Ok(())
}

fn read_transaction_signature(
    source: &SourceDump,
    record: &BorrowedTransactionRecord<'_>,
) -> Result<String> {
    ensure!(
        record.signature_count != 0,
        "Scaled UI transaction has no signature"
    );
    let dump_signature_ordinal = record
        .dump_signature_ordinal
        .context("Scaled UI transaction has no consolidated signature ordinal")?;
    let offset = dump_signature_ordinal
        .checked_mul(SIGNATURE_BYTES as u64)
        .context("Scaled UI signature offset overflow")?;
    ensure!(
        offset
            .checked_add(SIGNATURE_BYTES as u64)
            .is_some_and(|end| end <= source.signature_bytes),
        "Scaled UI signature range is outside the consolidated sidecar"
    );
    let mut bytes = [0u8; SIGNATURE_BYTES];
    positioned_read_exact(source.signature_handle.file(), &mut bytes, offset)?;
    Ok(bs58::encode(bytes).into_string())
}

#[cfg(unix)]
fn positioned_read_exact(file: &File, bytes: &mut [u8], offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(bytes, offset)?;
    Ok(())
}

#[cfg(windows)]
fn positioned_read_exact(file: &File, bytes: &mut [u8], offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;

    let mut read = 0usize;
    while read < bytes.len() {
        let current = offset
            .checked_add(u64::try_from(read)?)
            .context("positioned Scaled UI signature offset overflow")?;
        let count = file.seek_read(&mut bytes[read..], current)?;
        ensure!(
            count != 0,
            "positioned Scaled UI signature read reached EOF"
        );
        read += count;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn positioned_read_exact(_file: &File, _bytes: &mut [u8], _offset: u64) -> Result<()> {
    bail!("positioned file reads are not supported on this platform")
}

#[allow(clippy::too_many_arguments)]
fn classify_dex_instruction(
    dispatch: &DispatchTable,
    program_id: u32,
    data: &[u8],
    accounts: &[u32],
    outer_index: u32,
    inner_index: u32,
    stack_height: u32,
    stack_proven: bool,
    router_program_id: u32,
    candidates: &mut Vec<Candidate>,
    counters: &mut MarketCounters,
) -> Option<FrameOwner> {
    let Some(program) = dispatch.program(program_id) else {
        checked_counter_callback(&mut counters.rejected_unsupported_program);
        return None;
    };
    let router_frame =
        (program.role() == ProgramRole::Router).then_some(FrameOwner::Router(program_id));
    checked_counter_callback(&mut counters.parser_program_hits);
    match dispatch.decode(program_id, data, accounts) {
        DecodeOutcome::Decoded(decoded) => {
            checked_counter_callback(&mut counters.decoded_instructions);
            if decoded.role == ProgramRole::Router {
                checked_counter_callback(&mut counters.rejected_not_semantic_swap);
                return Some(FrameOwner::Router(program_id));
            }
            if !matches!(decoded.class, InstructionClass::Swap(_))
                || decoded.evidence.contains(Evidence::STRUCTURAL_ONLY)
            {
                checked_counter_callback(&mut counters.rejected_not_semantic_swap);
                return None;
            }
            checked_counter_callback(&mut counters.semantic_swap_instructions);
            checked_counter_callback(&mut counters.trade_candidates);
            let candidate_index = candidates.len();
            candidates.push(Candidate {
                decoded,
                dex_program_id: program_id,
                router_program_id,
                outer_index,
                inner_index,
                stack_height,
                stack_proven,
            });
            Some(FrameOwner::Venue(candidate_index))
        }
        DecodeOutcome::Unsupported { .. } => {
            checked_counter_callback(&mut counters.rejected_unsupported_discriminator);
            router_frame
        }
        DecodeOutcome::Malformed(reason) => {
            if matches!(reason, MalformedReason::InstructionAccountsTooShort { .. }) {
                checked_counter_callback(&mut counters.rejected_missing_accounts);
            } else {
                checked_counter_callback(&mut counters.rejected_malformed_instruction);
            }
            router_frame
        }
        DecodeOutcome::UnknownProgram => {
            checked_counter_callback(&mut counters.rejected_unsupported_program);
            None
        }
    }
}

fn resolve_accounts<'a>(
    indices: &[u8],
    resolved: &[u32],
    scratch: &'a mut [u32; MAX_MESSAGE_ACCOUNTS],
) -> Option<&'a [u32]> {
    if indices.len() > scratch.len() {
        return None;
    }
    for (destination, index) in scratch.iter_mut().zip(indices).take(indices.len()) {
        *destination = *resolved.get(usize::from(*index))?;
    }
    Some(&scratch[..indices.len()])
}

#[allow(clippy::too_many_arguments)]
fn process_token_transfer(
    token_program: TokenProgram,
    outer_index: u32,
    inner_index: u32,
    instruction: BorrowedArchiveV2InnerTokenInstruction<'_>,
    resolved_accounts: &[u32],
    owner: Option<usize>,
    transfers: &mut Vec<RawTransfer>,
    opaque_effects: &mut Vec<OpaqueTokenEffect>,
    counters: &mut MarketCounters,
) {
    let parsed = parse_token_transfer(instruction.data, instruction.accounts);
    let transfer = match parsed {
        Ok(Some(transfer)) => transfer,
        Ok(None) => {
            if !token_instruction_may_change_balance(token_program, instruction.data) {
                return;
            }
            opaque_effects.push(OpaqueTokenEffect {
                owner,
                outer_index,
                inner_index,
            });
            return;
        }
        Err(TokenTransferParseError::MissingAccounts | TokenTransferParseError::Malformed) => {
            opaque_effects.push(OpaqueTokenEffect {
                owner,
                outer_index,
                inner_index,
            });
            return;
        }
    };
    if transfer.source_index >= resolved_accounts.len()
        || transfer.destination_index >= resolved_accounts.len()
        || transfer
            .checked_mint_index
            .is_some_and(|index| index >= resolved_accounts.len())
    {
        opaque_effects.push(OpaqueTokenEffect {
            owner,
            outer_index,
            inner_index,
        });
        return;
    }
    checked_counter_callback(&mut counters.token_transfer_instructions);
    transfers.push(RawTransfer {
        owner,
        outer_index,
        inner_index,
        ..transfer
    });
}

fn token_instruction_may_change_balance(token_program: TokenProgram, data: &[u8]) -> bool {
    let decoded = decode_token_instruction(token_program, data);
    decoded.status != TokenDecodeStatus::Known
        || decoded.effect == TokenInstructionEffect::BalanceRelevant
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenTransferParseError {
    MissingAccounts,
    Malformed,
}

fn parse_token_transfer(
    data: &[u8],
    accounts: &[u8],
) -> std::result::Result<Option<RawTransfer>, TokenTransferParseError> {
    let read_amount = |offset: usize| -> Option<u64> {
        Some(u64::from_le_bytes(
            data.get(offset..offset + 8)?.try_into().ok()?,
        ))
    };
    let (kind, amount, checked_decimals, expected_fee, checked) = match data {
        [3, ..] if data.len() == 9 => (
            TokenTransferKind::Transfer,
            read_amount(1).ok_or(TokenTransferParseError::Malformed)?,
            None,
            0,
            false,
        ),
        [12, ..] if data.len() == 10 => (
            TokenTransferKind::TransferChecked,
            read_amount(1).ok_or(TokenTransferParseError::Malformed)?,
            data.get(9).copied(),
            0,
            true,
        ),
        [26, 1, ..] if data.len() == 19 => (
            TokenTransferKind::TransferCheckedWithFee,
            read_amount(2).ok_or(TokenTransferParseError::Malformed)?,
            data.get(10).copied(),
            read_amount(11).ok_or(TokenTransferParseError::Malformed)?,
            true,
        ),
        [3, ..] | [12, ..] | [26, 1, ..] => return Err(TokenTransferParseError::Malformed),
        _ => return Ok(None),
    };
    let source_index = usize::from(
        *accounts
            .first()
            .ok_or(TokenTransferParseError::MissingAccounts)?,
    );
    let (checked_mint_index, destination_position) = if checked {
        (
            Some(usize::from(
                *accounts
                    .get(1)
                    .ok_or(TokenTransferParseError::MissingAccounts)?,
            )),
            2,
        )
    } else {
        (None, 1)
    };
    let destination_index = usize::from(
        *accounts
            .get(destination_position)
            .ok_or(TokenTransferParseError::MissingAccounts)?,
    );
    if expected_fee > amount {
        return Err(TokenTransferParseError::Malformed);
    }
    Ok(Some(RawTransfer {
        owner: None,
        outer_index: 0,
        inner_index: 0,
        source_index,
        destination_index,
        checked_mint_index,
        amount,
        expected_fee,
        checked_decimals,
        kind,
    }))
}

fn stage_balance(
    side: TokenBalanceSide,
    row: BorrowedArchiveV2TokenBalance,
    account_count: usize,
    balances: &mut [BalanceSlot; MAX_MESSAGE_ACCOUNTS],
    callback_invalid: &mut bool,
) {
    let index = usize::try_from(row.account_index).unwrap_or(MAX_MESSAGE_ACCOUNTS);
    let Some(slot) = balances.get_mut(index).filter(|_| index < account_count) else {
        *callback_invalid = true;
        return;
    };
    let CompactPubkey::Id(mint_id) = row.mint.unwrap_or(CompactPubkey::Id(0)) else {
        slot.invalid = true;
        return;
    };
    if mint_id == 0 {
        slot.invalid = true;
        return;
    }
    let value = TokenBalance {
        mint_id,
        amount: row.amount,
        decimals: row.decimals,
    };
    let destination = match side {
        TokenBalanceSide::Pre => &mut slot.pre,
        TokenBalanceSide::Post => &mut slot.post,
    };
    if destination.replace(value).is_some() {
        slot.invalid = true;
    }
}

fn prove_committed_invocations(
    metadata: &[u8],
    schema: ArchiveV2WireMetadataErrorSchema,
    limits: ArchiveV2MetadataProjectionLimits,
    registry_entries: u32,
    stage: &mut TransactionStage,
    counters: &mut MarketCounters,
) -> Result<()> {
    build_ordered_invocations(stage)?;
    stage.log_events.clear();
    stage.log_stack.clear();
    let mut log_invalid = false;
    visit_archive_v2_compact_logs_exact_with_selected_error_schema(
        metadata,
        schema,
        limits,
        registry_entries,
        |event, _| {
            let resolve = |reference: CompactPubkey| match reference {
                CompactPubkey::Id(id) if id != 0 && id <= registry_entries => {
                    Some(invocation_program_key(id))
                }
                CompactPubkey::Id(_) | CompactPubkey::Raw(_) => None,
            };
            match event.kind {
                BorrowedArchiveV2LogEventKind::Invoke { program, depth } => {
                    let Some(program_id) = resolve(program) else {
                        log_invalid = true;
                        return Ok(());
                    };
                    let expected = u32::try_from(stage.log_stack.len())
                        .ok()
                        .and_then(|value| value.checked_add(1));
                    if expected != Some(u32::from(depth)) {
                        log_invalid = true;
                    }
                    stage.log_events.push(InvocationLogEvent::Invoke {
                        program_id,
                        depth: u32::from(depth),
                    });
                    stage.log_stack.push(program_id);
                }
                BorrowedArchiveV2LogEventKind::BpfInvoke { program } => {
                    let Some(program_id) = resolve(program) else {
                        log_invalid = true;
                        return Ok(());
                    };
                    let Some(depth) = u32::try_from(stage.log_stack.len())
                        .ok()
                        .and_then(|value| value.checked_add(1))
                    else {
                        log_invalid = true;
                        return Ok(());
                    };
                    stage
                        .log_events
                        .push(InvocationLogEvent::Invoke { program_id, depth });
                    stage.log_stack.push(program_id);
                }
                BorrowedArchiveV2LogEventKind::Success { program }
                | BorrowedArchiveV2LogEventKind::BpfSuccess { program } => {
                    let Some(program_id) = resolve(program) else {
                        log_invalid = true;
                        return Ok(());
                    };
                    if stage.log_stack.pop() != Some(program_id) {
                        log_invalid = true;
                    }
                    stage
                        .log_events
                        .push(InvocationLogEvent::Success { program_id });
                }
                BorrowedArchiveV2LogEventKind::Failure { program, .. }
                | BorrowedArchiveV2LogEventKind::BpfFailure { program, .. }
                | BorrowedArchiveV2LogEventKind::BpfFailureCustomProgramError { program, .. }
                | BorrowedArchiveV2LogEventKind::FailureInvalidAccountData { program }
                | BorrowedArchiveV2LogEventKind::BpfFailureInvalidAccountData { program }
                | BorrowedArchiveV2LogEventKind::FailureInvalidProgramArgument { program }
                | BorrowedArchiveV2LogEventKind::BpfFailureInvalidProgramArgument { program } => {
                    let Some(program_id) = resolve(program) else {
                        log_invalid = true;
                        return Ok(());
                    };
                    if stage.log_stack.pop() != Some(program_id) {
                        log_invalid = true;
                    }
                    stage
                        .log_events
                        .push(InvocationLogEvent::Failure { program_id });
                }
                BorrowedArchiveV2LogEventKind::FailureCustomProgramError { program, .. } => {
                    // Historical compact tag 22 is ambiguous between a plain
                    // program log and a terminal failure. Close the matching
                    // frame for structural continuity, but fail closed for all
                    // inner invocations in this transaction.
                    let Some(program_id) = resolve(program) else {
                        log_invalid = true;
                        return Ok(());
                    };
                    log_invalid = true;
                    if stage.log_stack.pop() == Some(program_id) {
                        stage
                            .log_events
                            .push(InvocationLogEvent::Failure { program_id });
                    }
                }
                BorrowedArchiveV2LogEventKind::LogTruncated => {
                    stage.log_events.push(InvocationLogEvent::LogTruncated);
                    log_invalid = true;
                }
                _ => {}
            }
            Ok(())
        },
    )
    .context("decode exact compact invocation logs for market commit proof")?;

    let classification =
        classify_committed_invocations(true, &stage.ordered_invocations, &stage.log_events);
    let strict_trace = !log_invalid
        && stage.log_stack.is_empty()
        && classification.diagnostics.is_empty()
        && classification.invocations.len() == stage.ordered_invocations.len()
        && classification.all_known();

    apply_commit_proof(stage, strict_trace, &classification.invocations, counters);
    retain_committed_scaled_ui_events(stage, strict_trace, &classification.invocations)?;
    Ok(())
}

fn retain_committed_scaled_ui_events(
    stage: &mut TransactionStage,
    strict_trace: bool,
    classified: &[blockzilla_token_balance_audit::ClassifiedInvocation],
) -> Result<()> {
    let mut write = 0usize;
    for read in 0..stage.scaled_ui_events.len() {
        let event = stage.scaled_ui_events[read].clone();
        let keep = match event.coordinate.inner_index {
            None => true,
            Some(inner_index) => {
                ensure!(
                    strict_trace,
                    "target Scaled UI Amount CPI has no complete commit trace"
                );
                match invocation_status(classified, event.coordinate.outer_index, inner_index) {
                    Some(CommitStatus::Committed) => true,
                    Some(CommitStatus::RolledBack(_)) => false,
                    Some(CommitStatus::Unknown(_)) | None => {
                        bail!("target Scaled UI Amount CPI commit status is unknown")
                    }
                }
            }
        };
        if keep {
            stage.scaled_ui_events[write] = event;
            write += 1;
        }
    }
    stage.scaled_ui_events.truncate(write);
    Ok(())
}

fn apply_commit_proof(
    stage: &mut TransactionStage,
    strict_trace: bool,
    classified: &[blockzilla_token_balance_audit::ClassifiedInvocation],
    counters: &mut MarketCounters,
) {
    for (candidate_index, candidate) in stage.candidates.iter().enumerate() {
        if candidate.inner_index == MARKET_OUTER_INNER_INDEX {
            continue;
        }
        if !strict_trace
            || invocation_status(classified, candidate.outer_index, candidate.inner_index)
                != Some(CommitStatus::Committed)
        {
            stage
                .pending_failures
                .push((candidate_index, CandidateRejection::UncommittedInvocation));
        }
    }

    let mut write = 0usize;
    for read in 0..stage.raw_transfers.len() {
        let transfer = stage.raw_transfers[read];
        let committed = strict_trace
            && invocation_status(classified, transfer.outer_index, transfer.inner_index)
                == Some(CommitStatus::Committed);
        if committed {
            stage.raw_transfers[write] = transfer;
            write += 1;
            if transfer.owner.is_some() {
                checked_counter_callback(&mut counters.attributed_token_transfers);
            }
        } else if let Some(owner) = transfer.owner {
            stage
                .pending_failures
                .push((owner, CandidateRejection::UncommittedInvocation));
        }
    }
    stage.raw_transfers.truncate(write);

    for index in 0..stage.opaque_token_effects.len() {
        let effect = stage.opaque_token_effects[index];
        let committed = strict_trace
            && invocation_status(classified, effect.outer_index, effect.inner_index)
                == Some(CommitStatus::Committed);
        if !committed {
            continue;
        }
        if let Some(owner) = effect.owner {
            stage
                .pending_failures
                .push((owner, CandidateRejection::UnsupportedTokenInstruction));
        } else {
            stage
                .unowned_token_barrier
                .get_or_insert(CandidateRejection::UnsupportedTokenInstruction);
        }
    }
}

fn build_ordered_invocations(stage: &mut TransactionStage) -> Result<()> {
    stage.ordered_invocations.clear();
    let mut inner_cursor = 0usize;
    for outer_index in 0..stage.outer_count {
        let program_id = stage.outer_program_ids[outer_index];
        ensure!(
            program_id != 0,
            "outer invocation has no resolved program ID"
        );
        stage.ordered_invocations.push(OrderedInvocation::outer(
            u32::try_from(outer_index).context("outer instruction index exceeds u32")?,
            invocation_program_key(program_id),
        ));
        while let Some(inner) = stage.inner_invocations.get(inner_cursor)
            && usize::try_from(inner.outer_index).ok() == Some(outer_index)
        {
            stage.ordered_invocations.push(OrderedInvocation::inner(
                inner.outer_index,
                inner.inner_index,
                invocation_program_key(inner.program_id),
                inner.stack_height,
            ));
            inner_cursor += 1;
        }
    }
    ensure!(
        inner_cursor == stage.inner_invocations.len(),
        "inner invocations are not in canonical outer-instruction groups"
    );
    Ok(())
}

fn invocation_program_key(registry_id: u32) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..4].copy_from_slice(&registry_id.to_le_bytes());
    key
}

fn invocation_status(
    classified: &[blockzilla_token_balance_audit::ClassifiedInvocation],
    outer_index: u32,
    inner_index: u32,
) -> Option<CommitStatus> {
    classified
        .iter()
        .find(|row| {
            row.invocation.coordinate.outer_index == outer_index
                && row.invocation.coordinate.inner_index == Some(inner_index)
        })
        .map(|row| row.status)
}

fn observe_target_decimals(
    balances: &[BalanceSlot; MAX_MESSAGE_ACCOUNTS],
    target_mint_id: u32,
    observed: &mut Option<u8>,
) -> Result<()> {
    for balance in balances {
        for row in [balance.pre, balance.post].into_iter().flatten() {
            if row.mint_id != target_mint_id {
                continue;
            }
            ensure!(
                observed.is_none_or(|decimals| decimals == row.decimals),
                "target mint has conflicting decimals in exact token-balance rows"
            );
            *observed = Some(row.decimals);
        }
    }
    Ok(())
}

fn resolve_transfers(
    stage: &mut TransactionStage,
    resolved_accounts: &[u32],
    counters: &mut MarketCounters,
) -> Result<()> {
    stage
        .candidate_failures
        .resize(stage.candidates.len(), None);
    for &(owner, rejection) in &stage.pending_failures {
        set_candidate_failure(&mut stage.candidate_failures, Some(owner), rejection);
    }
    for raw_index in 0..stage.raw_transfers.len() {
        let raw = stage.raw_transfers[raw_index];
        let resolved = match resolve_transfer(raw, &stage.balances, resolved_accounts) {
            Ok(transfer) => transfer,
            Err(rejection) => {
                set_transfer_failure(stage, raw.owner, rejection);
                continue;
            }
        };
        let Some(debit) =
            stage.debit[resolved.source_index].checked_add(u128::from(resolved.debit_amount))
        else {
            set_transfer_failure(
                stage,
                resolved.owner,
                CandidateRejection::ArithmeticOverflow,
            );
            continue;
        };
        let Some(credit) = stage.credit[resolved.destination_index]
            .checked_add(u128::from(resolved.credit_amount))
        else {
            set_transfer_failure(
                stage,
                resolved.owner,
                CandidateRejection::ArithmeticOverflow,
            );
            continue;
        };
        stage.debit[resolved.source_index] = debit;
        stage.credit[resolved.destination_index] = credit;
        stage.resolved_transfers.push(resolved);
    }
    ensure!(
        stage.resolved_transfers.len() <= stage.raw_transfers.len(),
        "resolved transfer count exceeds parsed transfers"
    );
    let _ = counters;
    Ok(())
}

fn set_transfer_failure(
    stage: &mut TransactionStage,
    owner: Option<usize>,
    rejection: CandidateRejection,
) {
    if owner.is_some() {
        set_candidate_failure(&mut stage.candidate_failures, owner, rejection);
    } else {
        stage
            .unowned_token_barrier
            .get_or_insert(CandidateRejection::TransferOutsideSubtree);
    }
}

fn resolve_transfer(
    raw: RawTransfer,
    balances: &[BalanceSlot; MAX_MESSAGE_ACCOUNTS],
    resolved_accounts: &[u32],
) -> std::result::Result<ResolvedTransfer, CandidateRejection> {
    if raw.amount == 0 {
        return Err(CandidateRejection::ZeroAmount);
    }
    let source_balance = balances
        .get(raw.source_index)
        .and_then(|slot| slot.token_identity())
        .ok_or(CandidateRejection::MissingTokenBalance)?;
    let destination_balance = balances
        .get(raw.destination_index)
        .and_then(|slot| slot.token_identity())
        .ok_or(CandidateRejection::MissingTokenBalance)?;
    if source_balance != destination_balance {
        return Err(CandidateRejection::DecimalMismatch);
    }
    let (mint_id, decimals) = source_balance;
    if let Some(index) = raw.checked_mint_index {
        let checked_mint_id = *resolved_accounts
            .get(index)
            .ok_or(CandidateRejection::MissingTokenBalance)?;
        if checked_mint_id != mint_id || raw.checked_decimals != Some(decimals) {
            return Err(CandidateRejection::DecimalMismatch);
        }
    }
    let credit_amount = raw
        .amount
        .checked_sub(raw.expected_fee)
        .ok_or(CandidateRejection::ArithmeticOverflow)?;
    if credit_amount == 0 {
        return Err(CandidateRejection::ZeroAmount);
    }
    Ok(ResolvedTransfer {
        owner: raw.owner,
        outer_index: raw.outer_index,
        inner_index: raw.inner_index,
        source_index: raw.source_index,
        destination_index: raw.destination_index,
        source_id: *resolved_accounts
            .get(raw.source_index)
            .ok_or(CandidateRejection::MissingTokenBalance)?,
        destination_id: *resolved_accounts
            .get(raw.destination_index)
            .ok_or(CandidateRejection::MissingTokenBalance)?,
        mint_id,
        decimals,
        debit_amount: raw.amount,
        credit_amount,
        fee_amount: raw.expected_fee,
        fee_known: raw.kind == TokenTransferKind::TransferCheckedWithFee,
        scaled_ui_config_id: 0,
    })
}

fn set_candidate_failure(
    failures: &mut [Option<CandidateRejection>],
    owner: Option<usize>,
    rejection: CandidateRejection,
) {
    if let Some(owner) = owner
        && let Some(destination) = failures.get_mut(owner)
        && destination.is_none()
    {
        *destination = Some(rejection);
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_candidates(
    transaction_id: u64,
    record: &BorrowedTransactionRecord<'_>,
    bindings: &RegistryBindings,
    stage: &mut TransactionStage,
    kinds: &mut InstructionKindRegistry,
    counters: &mut MarketCounters,
    writer: &mut BufWriter<File>,
) -> Result<()> {
    for (candidate_index, candidate) in stage.candidates.iter().copied().enumerate() {
        match build_candidate_record(
            transaction_id,
            record,
            bindings,
            stage,
            candidate_index,
            candidate,
            kinds,
            counters,
        ) {
            Ok(trade) => stage.output_records.push(trade),
            Err(rejection) => count_candidate_rejection(counters, rejection)?,
        }
    }
    stage
        .output_records
        .sort_unstable_by_key(|row| row.order_key());
    let mut previous_key = None;
    for record in &stage.output_records {
        let order_key = record.order_key();
        let encoded = record.encode()?;
        if previous_key == Some(order_key) {
            checked_counter(&mut counters.rejected_duplicate, "duplicate trades")?;
            continue;
        }
        ensure!(
            previous_key.is_none_or(|previous| previous < order_key),
            "market trades are not in strict canonical source order"
        );
        writer.write_all(&encoded)?;
        previous_key = Some(order_key);
        checked_counter(&mut counters.emitted_trades, "emitted trades")?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_candidate_record(
    transaction_id: u64,
    record: &BorrowedTransactionRecord<'_>,
    bindings: &RegistryBindings,
    stage: &TransactionStage,
    candidate_index: usize,
    candidate: Candidate,
    kinds: &mut InstructionKindRegistry,
    counters: &mut MarketCounters,
) -> std::result::Result<MarketTradeRecord, CandidateRejection> {
    let block_time = record
        .block
        .block_time
        .ok_or(CandidateRejection::MissingBlockTime)?;
    if !candidate.stack_proven {
        return Err(CandidateRejection::MissingStack);
    }
    if let Some(rejection) = stage
        .candidate_failures
        .get(candidate_index)
        .copied()
        .flatten()
    {
        return Err(rejection);
    }
    if let Some(rejection) = stage.unowned_token_barrier {
        return Err(rejection);
    }
    let roles = candidate.decoded.accounts;
    let pool_id = roles.pool.unwrap_or(0);
    let has_owned_transfer = stage
        .raw_transfers
        .iter()
        .any(|transfer| transfer.owner == Some(candidate_index));
    if !has_owned_transfer
        && stage
            .raw_transfers
            .iter()
            .any(|transfer| transfer.owner.is_none())
    {
        return Err(CandidateRejection::TransferOutsideSubtree);
    }
    let mut input = DirectionalFlow::default();
    let mut output = DirectionalFlow::default();
    let mut relevant_accounts = [false; MAX_MESSAGE_ACCOUNTS];
    for transfer in stage
        .resolved_transfers
        .iter()
        .filter(|transfer| transfer.owner == Some(candidate_index))
    {
        let (input_match, input_flags) = input_transfer_match(roles, *transfer);
        let (output_match, output_flags) = output_transfer_match(roles, *transfer);
        if input_match && output_match {
            return Err(CandidateRejection::AmbiguousFlow);
        }
        if input_match {
            add_directional_transfer(&mut input, *transfer, transfer.debit_amount, input_flags)?;
        } else if output_match {
            add_directional_transfer(&mut output, *transfer, transfer.credit_amount, output_flags)?;
        } else {
            continue;
        }
        relevant_accounts[transfer.source_index] = true;
        relevant_accounts[transfer.destination_index] = true;
    }
    if !input.initialized || !output.initialized {
        return Err(CandidateRejection::UnresolvedFlow);
    }
    if !unmatched_owned_transfers_are_disjoint(
        candidate_index,
        roles,
        &relevant_accounts,
        &stage.resolved_transfers,
    ) {
        return Err(CandidateRejection::AmbiguousFlow);
    }
    if !directional_mint_roles_match(roles, input.mint_id, output.mint_id) {
        return Err(CandidateRejection::AmbiguousFlow);
    }
    if input.mint_id == output.mint_id {
        return Err(CandidateRejection::AmbiguousFlow);
    }
    if input.amount == 0 || output.amount == 0 {
        return Err(CandidateRejection::ZeroAmount);
    }
    let target_input = input.mint_id == bindings.target_mint_id;
    let target_output = output.mint_id == bindings.target_mint_id;
    if target_input == target_output {
        return Err(CandidateRejection::TargetSides);
    }
    checked_counter_callback(&mut counters.semantic_target_swap_instructions);
    if !relevant_accounts_are_exclusive(
        candidate_index,
        &relevant_accounts,
        &stage.resolved_transfers,
    ) {
        return Err(CandidateRejection::AmbiguousFlow);
    }
    for (index, relevant) in relevant_accounts.into_iter().enumerate() {
        if relevant && !account_reconciles(index, stage) {
            return Err(CandidateRejection::BalanceMismatch);
        }
    }
    let (fee_amount, fee_mint_id, fee_known) = combine_fees(input, output)?;
    let instruction_kind_id = kinds
        .intern(candidate.decoded)
        .map_err(|_| CandidateRejection::ArithmeticOverflow)?;
    let mut flags = MARKET_TRADE_FLAG_STACK_PROVEN
        | MARKET_TRADE_FLAG_COMMIT_PROVEN
        | MARKET_TRADE_FLAG_BALANCE_RECONCILED
        | input.role_flags
        | output.role_flags;
    if candidate.inner_index != MARKET_OUTER_INNER_INDEX {
        flags |= MARKET_TRADE_FLAG_INNER;
    }
    if target_input {
        flags |= MARKET_TRADE_FLAG_TARGET_INPUT;
    } else {
        flags |= MARKET_TRADE_FLAG_TARGET_OUTPUT;
    }
    if candidate.router_program_id != 0 {
        flags |= MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED;
    }
    let other_mint = if target_input {
        output.mint_id
    } else {
        input.mint_id
    };
    if bindings
        .usd_quote_mint_ids
        .binary_search(&other_mint)
        .is_ok()
    {
        flags |= MARKET_TRADE_FLAG_DIRECT_USD_QUOTE;
    }
    if fee_known {
        flags |= MARKET_TRADE_FLAG_FEE_KNOWN;
    }
    let scaled_ui_config_id = if target_input {
        input.scaled_ui_config_id
    } else {
        output.scaled_ui_config_id
    };
    if scaled_ui_config_id == 0 {
        return Err(CandidateRejection::AmbiguousFlow);
    }
    let row = MarketTradeRecord {
        transaction_id,
        slot: record.block.slot,
        block_time,
        source_epoch: record.source_epoch,
        source_block_id: record.source_block_id,
        tx_index: record.tx_index,
        outer_index: candidate.outer_index,
        inner_index: candidate.inner_index,
        stack_height: candidate.stack_height,
        instruction_kind_id,
        dex_program_id: candidate.dex_program_id,
        router_program_id: candidate.router_program_id,
        pool_id,
        trader_id: roles.user_authority.unwrap_or(0),
        input_mint_id: input.mint_id,
        output_mint_id: output.mint_id,
        user_source_id: roles.user_source.unwrap_or(0),
        user_destination_id: roles.user_destination.unwrap_or(0),
        amount_in: input.amount,
        amount_out: output.amount,
        fee_amount,
        fee_mint_id,
        flags,
        input_decimals: input.decimals,
        output_decimals: output.decimals,
        input_transfer_count: input.transfer_count,
        output_transfer_count: output.transfer_count,
        scaled_ui_config_id,
    };
    row.validate()
        .map_err(|_| CandidateRejection::ArithmeticOverflow)?;
    Ok(row)
}

fn input_transfer_match(roles: AccountRoles, transfer: ResolvedTransfer) -> (bool, u16) {
    let source_match = roles.user_source == Some(transfer.source_id);
    let vault_match = roles.input_vault == Some(transfer.destination_id);
    let matched = strict_directional_match(
        roles.user_source,
        roles.input_vault,
        source_match,
        vault_match,
    );
    let mut flags = 0;
    if source_match {
        flags |= MARKET_TRADE_FLAG_USER_SOURCE_MATCH;
    }
    if vault_match {
        flags |= MARKET_TRADE_FLAG_INPUT_VAULT_MATCH;
    }
    (matched, flags)
}

fn output_transfer_match(roles: AccountRoles, transfer: ResolvedTransfer) -> (bool, u16) {
    let vault_match = roles.output_vault == Some(transfer.source_id);
    let destination_match = roles.user_destination == Some(transfer.destination_id);
    let matched = strict_directional_match(
        roles.output_vault,
        roles.user_destination,
        vault_match,
        destination_match,
    );
    let mut flags = 0;
    if vault_match {
        flags |= MARKET_TRADE_FLAG_OUTPUT_VAULT_MATCH;
    }
    if destination_match {
        flags |= MARKET_TRADE_FLAG_USER_DESTINATION_MATCH;
    }
    (matched, flags)
}

fn strict_directional_match(
    first_role: Option<u32>,
    second_role: Option<u32>,
    first_match: bool,
    second_match: bool,
) -> bool {
    match (first_role, second_role) {
        (Some(_), Some(_)) => first_match && second_match,
        (Some(_), None) => first_match,
        (None, Some(_)) => second_match,
        (None, None) => false,
    }
}

fn directional_mint_roles_match(
    roles: AccountRoles,
    input_mint_id: u32,
    output_mint_id: u32,
) -> bool {
    roles
        .input_mint
        .is_none_or(|mint_id| mint_id == input_mint_id)
        && roles
            .output_mint
            .is_none_or(|mint_id| mint_id == output_mint_id)
}

fn add_directional_transfer(
    flow: &mut DirectionalFlow,
    transfer: ResolvedTransfer,
    amount: u64,
    role_flags: u16,
) -> std::result::Result<(), CandidateRejection> {
    if amount == 0 {
        return Err(CandidateRejection::ZeroAmount);
    }
    if flow.initialized && (flow.mint_id != transfer.mint_id || flow.decimals != transfer.decimals)
    {
        return Err(CandidateRejection::AmbiguousFlow);
    }
    if !flow.initialized {
        flow.mint_id = transfer.mint_id;
        flow.decimals = transfer.decimals;
        flow.initialized = true;
    }
    flow.amount = flow
        .amount
        .checked_add(amount)
        .ok_or(CandidateRejection::ArithmeticOverflow)?;
    flow.transfer_count = flow
        .transfer_count
        .checked_add(1)
        .ok_or(CandidateRejection::ArithmeticOverflow)?;
    flow.role_flags |= role_flags;
    if transfer.scaled_ui_config_id != 0 {
        if flow.scaled_ui_config_id != 0 && flow.scaled_ui_config_id != transfer.scaled_ui_config_id
        {
            return Err(CandidateRejection::AmbiguousFlow);
        }
        flow.scaled_ui_config_id = transfer.scaled_ui_config_id;
    }
    if transfer.fee_known {
        if flow.fee_known && flow.fee_mint_id != transfer.mint_id {
            return Err(CandidateRejection::AmbiguousFlow);
        }
        flow.fee_known = true;
        flow.fee_mint_id = transfer.mint_id;
        flow.fee_amount = flow
            .fee_amount
            .checked_add(transfer.fee_amount)
            .ok_or(CandidateRejection::ArithmeticOverflow)?;
    }
    Ok(())
}

fn combine_fees(
    input: DirectionalFlow,
    output: DirectionalFlow,
) -> std::result::Result<(u64, u32, bool), CandidateRejection> {
    match (input.fee_known, output.fee_known) {
        (false, false) => Ok((0, 0, false)),
        (true, false) => Ok((input.fee_amount, input.fee_mint_id, true)),
        (false, true) => Ok((output.fee_amount, output.fee_mint_id, true)),
        (true, true) if input.fee_mint_id == output.fee_mint_id => Ok((
            input
                .fee_amount
                .checked_add(output.fee_amount)
                .ok_or(CandidateRejection::ArithmeticOverflow)?,
            input.fee_mint_id,
            true,
        )),
        (true, true) => Err(CandidateRejection::AmbiguousFlow),
    }
}

fn account_reconciles(index: usize, stage: &TransactionStage) -> bool {
    let Some(observed) = stage
        .balances
        .get(index)
        .and_then(|slot| slot.observed_delta())
    else {
        return false;
    };
    let Ok(credit) = i128::try_from(stage.credit[index]) else {
        return false;
    };
    let Ok(debit) = i128::try_from(stage.debit[index]) else {
        return false;
    };
    credit - debit == observed
}

fn relevant_accounts_are_exclusive(
    candidate_index: usize,
    relevant: &[bool; MAX_MESSAGE_ACCOUNTS],
    transfers: &[ResolvedTransfer],
) -> bool {
    transfers.iter().all(|transfer| {
        transfer.owner == Some(candidate_index)
            || (!relevant[transfer.source_index] && !relevant[transfer.destination_index])
    })
}

fn unmatched_owned_transfers_are_disjoint(
    candidate_index: usize,
    roles: AccountRoles,
    relevant: &[bool; MAX_MESSAGE_ACCOUNTS],
    transfers: &[ResolvedTransfer],
) -> bool {
    transfers
        .iter()
        .filter(|transfer| transfer.owner == Some(candidate_index))
        .all(|transfer| {
            let input_match = input_transfer_match(roles, *transfer).0;
            let output_match = output_transfer_match(roles, *transfer).0;
            input_match
                || output_match
                || (!relevant[transfer.source_index] && !relevant[transfer.destination_index])
        })
}

fn count_candidate_rejection(
    counters: &mut MarketCounters,
    rejection: CandidateRejection,
) -> Result<()> {
    let (counter, label) = match rejection {
        CandidateRejection::MissingBlockTime => (
            &mut counters.rejected_missing_block_time,
            "trades missing block time",
        ),
        CandidateRejection::MissingStack => (
            &mut counters.rejected_missing_stack_height,
            "trades missing stack proof",
        ),
        CandidateRejection::UncommittedInvocation => (
            &mut counters.rejected_uncommitted_invocation,
            "trades without committed-invocation proof",
        ),
        CandidateRejection::MissingTokenBalance => (
            &mut counters.rejected_missing_token_balance,
            "trades missing token balance",
        ),
        CandidateRejection::UnsupportedTokenInstruction => (
            &mut counters.rejected_unsupported_token_instruction,
            "trades with unsupported token instructions",
        ),
        CandidateRejection::TransferOutsideSubtree => (
            &mut counters.rejected_transfer_outside_subtree,
            "trades with transfers outside the venue subtree",
        ),
        CandidateRejection::UnresolvedFlow => (
            &mut counters.rejected_unresolved_token_flow,
            "trades with unresolved flow",
        ),
        CandidateRejection::AmbiguousFlow => (
            &mut counters.rejected_ambiguous_token_flow,
            "trades with ambiguous flow",
        ),
        CandidateRejection::TargetSides => (
            &mut counters.rejected_target_on_both_or_neither_sides,
            "trades with invalid target sides",
        ),
        CandidateRejection::ZeroAmount => {
            (&mut counters.rejected_zero_amount, "zero-amount trades")
        }
        CandidateRejection::DecimalMismatch => (
            &mut counters.rejected_decimal_mismatch,
            "trades with decimal mismatch",
        ),
        CandidateRejection::BalanceMismatch => (
            &mut counters.rejected_balance_mismatch,
            "trades with balance mismatch",
        ),
        CandidateRejection::ArithmeticOverflow => (
            &mut counters.rejected_arithmetic_overflow,
            "trades with arithmetic overflow",
        ),
    };
    checked_counter(counter, label)
}

fn finish_trade_file(
    source: &SourceDump,
    work: &Path,
    complete: bool,
    records: u64,
    raw_path: &Path,
) -> Result<MarketFileBinding> {
    let expected_raw_bytes = records
        .checked_mul(MARKET_TRADE_RECORD_BYTES as u64)
        .context("raw market trade byte length overflow")?;
    ensure!(
        fs::metadata(raw_path)?.len() == expected_raw_bytes,
        "raw market trade byte length differs from its record count"
    );
    let partial = work.join(format!("{MARKET_TRADES_FILE}.partial"));
    let mut writer = DigestFileWriter::create(&partial)?;
    writer.write_all(
        &MarketFileHeader::new(
            complete,
            records,
            source.manifest_sha256,
            source.transaction_sha256,
        )
        .encode(),
    )?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(raw_path)?);
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        writer.write_all(&buffer[..count])?;
    }
    let binding = writer.finish(
        MARKET_TRADES_FILE,
        records,
        MARKET_TRADE_RECORD_BYTES as u16,
    )?;
    let market_binding = MarketFileBinding {
        file: binding.file,
        bytes: binding.bytes,
        sha256: binding.sha256,
        records: binding.records,
        record_bytes: binding.record_bytes,
    };
    market_binding.validate()?;
    ensure!(
        market_binding.bytes
            == expected_raw_bytes
                .checked_add(MARKET_HEADER_BYTES as u64)
                .context("market trade file byte length overflow")?,
        "market trade file byte length differs"
    );
    Ok(market_binding)
}

#[allow(clippy::too_many_arguments)]
fn build_manifest(
    source: &SourceDump,
    complete: bool,
    canary_max_transactions: Option<u64>,
    target_mint_id: u32,
    target_decimals: u8,
    usd_quote_mint_ids: Vec<u32>,
    instruction_kinds: Vec<MarketInstructionKind>,
    scaled_ui_events: Vec<ScaledUiAmountEvent>,
    counters: MarketCounters,
    trades: MarketFileBinding,
) -> Result<MarketManifest> {
    let accounts = source
        .manifest
        .discovered_account_count
        .context("source manifest has no discovered-account count")?;
    let manifest = MarketManifest {
        schema_version: MARKET_SCHEMA_VERSION,
        artifact_kind: MarketManifest::ARTIFACT_KIND.to_owned(),
        complete,
        canary_max_transactions,
        created_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time is before Unix epoch")?
            .as_secs(),
        source: MarketSourceBinding {
            manifest_file: DUMP_MANIFEST_FILE.to_owned(),
            manifest_bytes: source.manifest_handle.len(),
            manifest_sha256: hex_digest(source.manifest_sha256),
            transaction_file: source.manifest.transaction_stream.clone(),
            transaction_bytes: source.transaction_bytes,
            transaction_sha256: hex_digest(source.transaction_sha256),
            signature_file: source
                .manifest
                .signature_stream
                .clone()
                .expect("validated signature file binding"),
            signature_bytes: source.signature_bytes,
            signature_sha256: hex_digest(source.signature_sha256),
            registry_file: source
                .manifest
                .pubkey_registry
                .clone()
                .expect("validated registry file binding"),
            registry_bytes: source.registry_bytes,
            registry_sha256: hex_digest(source.registry_sha256),
            accounts_file: source
                .manifest
                .discovered_accounts
                .clone()
                .expect("validated accounts file binding"),
            accounts_bytes: source.accounts_bytes,
            accounts_sha256: hex_digest(source.accounts_sha256),
            first_epoch: source.manifest.first_epoch,
            last_epoch: source.manifest.last_epoch,
            transactions: source.manifest.transactions,
            signatures: source.signatures,
            pubkeys: source.pubkeys,
            accounts,
        },
        parser: MarketParserBinding {
            semantic_version: MARKET_REDUCER_SEMANTIC_VERSION.to_owned(),
            implementation_fingerprint: market_parser_implementation_fingerprint(),
        },
        target: MarketTargetBinding {
            mint: source.manifest.mint.clone(),
            mint_id: target_mint_id,
            decimals: target_decimals,
        },
        scaled_ui: MarketScaledUiHistory {
            enabled: true,
            processor_semantics: DEPLOYED_LEGACY_REPLAY_SEMANTICS.to_owned(),
            mint_anchor_slot: source.manifest.mint_slot,
            mint_anchor_signature: source.manifest.mint_signature.clone(),
            events: scaled_ui_events,
        },
        usd_quote_mint_ids,
        instruction_kinds,
        counters,
        trades,
        definitions: MarketDefinitions::canonical(),
    };
    manifest.validate()?;
    Ok(manifest)
}

fn publish_market(output: &Path, work: &Path, manifest: &MarketManifest) -> Result<()> {
    let raw_path = work.join(RAW_TRADES_FILE);
    fs::remove_file(&raw_path).with_context(|| format!("remove {}", raw_path.display()))?;
    let trades_partial = work.join(format!("{MARKET_TRADES_FILE}.partial"));
    fs::rename(&trades_partial, output.join(MARKET_TRADES_FILE))?;
    fs::remove_dir(work)?;
    sync_directory(output)?;

    let manifest_partial = output.join(format!("{MARKET_MANIFEST_FILE}.partial"));
    let mut bytes = serde_json::to_vec_pretty(manifest)?;
    bytes.push(b'\n');
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, create_new_file(&manifest_partial)?);
    writer.write_all(&bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    fs::rename(&manifest_partial, output.join(MARKET_MANIFEST_FILE))?;
    sync_directory(output)?;
    Ok(())
}

fn projector(profile: DumpWireProfile) -> ArchiveV2MessageProjector {
    ArchiveV2MessageProjector::new(match profile {
        DumpWireProfile::PostUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PostUnknownInstructionFallbacksV1
        }
        DumpWireProfile::PreUnknownInstructionFallbacksV1 => {
            ArchiveV2WireProfile::PreUnknownInstructionFallbacksV1
        }
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionEligibility {
    Eligible,
    Failed,
    MetadataAbsent,
}

fn transaction_eligibility(
    failed: bool,
    metadata_schema: ExactMetadataSchemaSelection,
) -> TransactionEligibility {
    if failed {
        TransactionEligibility::Failed
    } else if metadata_schema == ExactMetadataSchemaSelection::NoMetadata {
        TransactionEligibility::MetadataAbsent
    } else {
        TransactionEligibility::Eligible
    }
}

fn effective_canary_limit(requested: Option<u64>, source_transactions: u64) -> Option<u64> {
    requested.filter(|maximum| *maximum < source_transactions)
}

fn validate_counter_partition(counters: &MarketCounters) -> Result<()> {
    counters.validate()
}

fn empty_counters() -> MarketCounters {
    MarketCounters::default()
}

fn checked_counter(counter: &mut u64, label: &str) -> Result<()> {
    *counter = counter
        .checked_add(1)
        .with_context(|| format!("{label} counter overflow"))?;
    Ok(())
}

fn checked_counter_callback(counter: &mut u64) {
    *counter = counter.saturating_add(1);
}

fn parse_pubkey(value: &str, label: &str) -> Result<[u8; 32]> {
    let mut output = [0u8; 32];
    let length = bs58::decode(value)
        .onto(&mut output)
        .with_context(|| format!("decode {label} as base58"))?;
    ensure!(length == output.len(), "{label} byte length differs");
    Ok(output)
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn report_progress(
    transactions: u64,
    target_transactions: u64,
    trades: u64,
    transaction_bytes: u64,
    started: Instant,
) {
    let elapsed = started.elapsed().as_secs_f64();
    let rate_mib = if elapsed > 0.0 {
        transaction_bytes as f64 / (1024.0 * 1024.0) / elapsed
    } else {
        0.0
    };
    let eta_seconds = if transactions == 0 {
        0.0
    } else {
        elapsed * (target_transactions.saturating_sub(transactions)) as f64 / transactions as f64
    };
    eprintln!(
        "market progress: tx {transactions}/{target_transactions}, trades {trades}, transaction bytes {transaction_bytes}, {rate_mib:.1} MiB/s, elapsed {elapsed:.0}s, ETA {eta_seconds:.0}s"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn balance(mint_id: u32, amount: u64, decimals: u8) -> TokenBalance {
        TokenBalance {
            mint_id,
            amount,
            decimals,
        }
    }

    #[test]
    fn stack_ownership_selects_deepest_venue_and_keeps_router() {
        let mut stage = TransactionStage::new();
        stage.begin();
        stage.outer_frames[0] = Some(FrameOwner::Router(90));
        assert_eq!(stage.prepare_height(0, Some(2)), Some(2));
        stage.push_frame(2, FrameOwner::Venue(3));
        assert_eq!(stage.deepest_router(), 90);
        assert_eq!(stage.deepest_venue(), Some(3));
        assert_eq!(stage.prepare_height(0, Some(3)), Some(3));
        assert_eq!(stage.deepest_venue(), Some(3));
        assert_eq!(stage.prepare_height(0, Some(2)), Some(2));
        assert_eq!(stage.deepest_venue(), None);
        assert_eq!(stage.deepest_router(), 90);

        stage.begin();
        assert_eq!(stage.prepare_height(0, Some(u32::MAX)), Some(u32::MAX));
        assert!(stage.stack_invalid[0]);
        assert_eq!(stage.prepare_height(0, Some(2)), Some(2));
        assert!(stage.stack_invalid[0]);
    }

    #[test]
    fn directional_flow_requires_every_declared_endpoint() {
        let roles = AccountRoles {
            user_source: Some(10),
            user_destination: Some(20),
            input_vault: Some(30),
            output_vault: Some(40),
            ..AccountRoles::default()
        };
        let transfer = ResolvedTransfer {
            owner: Some(0),
            outer_index: 0,
            inner_index: 0,
            source_index: 0,
            destination_index: 1,
            source_id: 10,
            destination_id: 30,
            mint_id: 5,
            decimals: 6,
            debit_amount: 100,
            credit_amount: 100,
            fee_amount: 0,
            fee_known: false,
            scaled_ui_config_id: 0,
        };
        assert!(input_transfer_match(roles, transfer).0);
        assert!(!output_transfer_match(roles, transfer).0);
        let wrong_vault = ResolvedTransfer {
            destination_id: 31,
            ..transfer
        };
        assert!(!input_transfer_match(roles, wrong_vault).0);
    }

    #[test]
    fn wrong_parser_mint_roles_reject_a_nested_venue_flow() {
        let mut stage = TransactionStage::new();
        stage.begin();
        stage.outer_frames[2] = Some(FrameOwner::Router(90));
        assert_eq!(stage.prepare_height(2, Some(2)), Some(2));
        stage.push_frame(2, FrameOwner::Venue(4));
        assert_eq!(stage.prepare_height(2, Some(3)), Some(3));
        assert_eq!(stage.deepest_venue(), Some(4));

        let correct = AccountRoles {
            input_mint: Some(7),
            output_mint: Some(8),
            ..AccountRoles::default()
        };
        assert!(directional_mint_roles_match(correct, 7, 8));
        let wrong = AccountRoles {
            output_mint: Some(9),
            ..correct
        };
        assert!(!directional_mint_roles_match(wrong, 7, 8));
    }

    #[test]
    fn reconciliation_uses_aggregate_committed_transfers() {
        let mut stage = TransactionStage::new();
        stage.begin();
        stage.balances[0] = BalanceSlot {
            pre: Some(balance(7, 1_000, 6)),
            post: Some(balance(7, 700, 6)),
            invalid: false,
        };
        stage.debit[0] = 400;
        stage.credit[0] = 100;
        assert!(account_reconciles(0, &stage));
        stage.credit[0] = 99;
        assert!(!account_reconciles(0, &stage));
    }

    #[test]
    fn unresolved_unowned_transfer_blocks_all_candidates() {
        let mut stage = TransactionStage::new();
        stage.begin();
        stage.raw_transfers.push(RawTransfer {
            owner: None,
            outer_index: 0,
            inner_index: 0,
            source_index: 0,
            destination_index: 1,
            checked_mint_index: None,
            amount: 100,
            expected_fee: 0,
            checked_decimals: None,
            kind: TokenTransferKind::Transfer,
        });
        let mut counters = MarketCounters::default();
        resolve_transfers(&mut stage, &[10, 20], &mut counters).unwrap();
        assert!(stage.resolved_transfers.is_empty());
        assert_eq!(
            stage.unowned_token_barrier,
            Some(CandidateRejection::TransferOutsideSubtree)
        );
    }

    #[test]
    fn sibling_transfer_makes_candidate_balance_ownership_ambiguous() {
        let owned = ResolvedTransfer {
            owner: Some(0),
            outer_index: 0,
            inner_index: 0,
            source_index: 1,
            destination_index: 2,
            source_id: 10,
            destination_id: 20,
            mint_id: 7,
            decimals: 6,
            debit_amount: 100,
            credit_amount: 100,
            fee_amount: 0,
            fee_known: false,
            scaled_ui_config_id: 0,
        };
        let sibling = ResolvedTransfer {
            owner: Some(1),
            source_index: 1,
            destination_index: 3,
            source_id: 10,
            destination_id: 30,
            ..owned
        };
        let unrelated = ResolvedTransfer {
            owner: None,
            source_index: 4,
            destination_index: 5,
            source_id: 40,
            destination_id: 50,
            ..owned
        };
        let mut relevant = [false; MAX_MESSAGE_ACCOUNTS];
        relevant[1] = true;
        relevant[2] = true;
        assert!(relevant_accounts_are_exclusive(
            0,
            &relevant,
            &[owned, unrelated]
        ));
        assert!(!relevant_accounts_are_exclusive(
            0,
            &relevant,
            &[owned, sibling, unrelated]
        ));

        let roles = AccountRoles {
            user_source: Some(10),
            input_vault: Some(20),
            ..AccountRoles::default()
        };
        let unmatched_owned = ResolvedTransfer {
            owner: Some(0),
            source_index: 1,
            destination_index: 3,
            source_id: 10,
            destination_id: 30,
            ..owned
        };
        assert!(!unmatched_owned_transfers_are_disjoint(
            0,
            roles,
            &relevant,
            &[owned, unmatched_owned]
        ));
        assert!(unmatched_owned_transfers_are_disjoint(
            0,
            roles,
            &relevant,
            &[owned, unrelated]
        ));
    }

    #[test]
    fn caught_failed_token_cpi_is_not_an_executed_transfer() {
        let venue = invocation_program_key(11);
        let token = invocation_program_key(12);
        let invocations = [
            OrderedInvocation::outer(0, venue),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let events = [
            InvocationLogEvent::Invoke {
                program_id: venue,
                depth: 1,
            },
            InvocationLogEvent::Invoke {
                program_id: token,
                depth: 2,
            },
            InvocationLogEvent::Failure { program_id: token },
            InvocationLogEvent::Success { program_id: venue },
        ];
        let classification = classify_committed_invocations(true, &invocations, &events);
        assert!(classification.diagnostics.is_empty());

        let mut stage = TransactionStage::new();
        stage.begin();
        stage.raw_transfers.push(RawTransfer {
            owner: Some(0),
            outer_index: 0,
            inner_index: 0,
            source_index: 1,
            destination_index: 2,
            checked_mint_index: None,
            amount: 100,
            expected_fee: 0,
            checked_decimals: None,
            kind: TokenTransferKind::Transfer,
        });
        stage.opaque_token_effects.push(OpaqueTokenEffect {
            owner: None,
            outer_index: 0,
            inner_index: 0,
        });
        let mut counters = MarketCounters::default();
        apply_commit_proof(&mut stage, true, &classification.invocations, &mut counters);
        assert!(stage.raw_transfers.is_empty());
        assert_eq!(stage.unowned_token_barrier, None);
        assert_eq!(counters.attributed_token_transfers, 0);
        assert_eq!(
            stage.pending_failures,
            vec![(0, CandidateRejection::UncommittedInvocation)]
        );

        let committed_events = [
            InvocationLogEvent::Invoke {
                program_id: venue,
                depth: 1,
            },
            InvocationLogEvent::Invoke {
                program_id: token,
                depth: 2,
            },
            InvocationLogEvent::Success { program_id: token },
            InvocationLogEvent::Success { program_id: venue },
        ];
        let committed = classify_committed_invocations(true, &invocations, &committed_events);
        assert!(committed.diagnostics.is_empty());
        let mut committed_stage = TransactionStage::new();
        committed_stage.begin();
        committed_stage
            .opaque_token_effects
            .push(OpaqueTokenEffect {
                owner: None,
                outer_index: 0,
                inner_index: 0,
            });
        apply_commit_proof(
            &mut committed_stage,
            true,
            &committed.invocations,
            &mut counters,
        );
        assert_eq!(
            committed_stage.unowned_token_barrier,
            Some(CandidateRejection::UnsupportedTokenInstruction)
        );
    }

    #[test]
    fn token_effect_classifier_fails_closed_for_public_amount_changes() {
        let mut transfer = vec![3];
        transfer.extend_from_slice(&42u64.to_le_bytes());
        assert!(token_instruction_may_change_balance(
            TokenProgram::Legacy,
            &transfer
        ));
        assert!(token_instruction_may_change_balance(
            TokenProgram::Token2022,
            &[254]
        ));
        assert!(!token_instruction_may_change_balance(
            TokenProgram::Legacy,
            &[5]
        ));
    }

    #[test]
    fn failed_and_metadata_absent_transactions_are_excluded() {
        assert_eq!(
            transaction_eligibility(true, ExactMetadataSchemaSelection::CurrentOnly),
            TransactionEligibility::Failed
        );
        assert_eq!(
            transaction_eligibility(false, ExactMetadataSchemaSelection::NoMetadata),
            TransactionEligibility::MetadataAbsent
        );
        assert_eq!(
            transaction_eligibility(false, ExactMetadataSchemaSelection::BothIdentical),
            TransactionEligibility::Eligible
        );
        assert_eq!(effective_canary_limit(Some(99), 100), Some(99));
        assert_eq!(effective_canary_limit(Some(100), 100), None);
        assert_eq!(effective_canary_limit(Some(101), 100), None);
    }

    #[test]
    fn parses_all_supported_token_transfer_forms() {
        let mut transfer = vec![3];
        transfer.extend_from_slice(&42u64.to_le_bytes());
        let decoded = parse_token_transfer(&transfer, &[1, 2]).unwrap().unwrap();
        assert_eq!(decoded.kind, TokenTransferKind::Transfer);
        assert_eq!(decoded.amount, 42);

        let mut checked = vec![12];
        checked.extend_from_slice(&43u64.to_le_bytes());
        checked.push(6);
        let decoded = parse_token_transfer(&checked, &[1, 3, 2]).unwrap().unwrap();
        assert_eq!(decoded.kind, TokenTransferKind::TransferChecked);
        assert_eq!(decoded.checked_decimals, Some(6));

        let mut fee = vec![26, 1];
        fee.extend_from_slice(&44u64.to_le_bytes());
        fee.push(6);
        fee.extend_from_slice(&4u64.to_le_bytes());
        let decoded = parse_token_transfer(&fee, &[1, 3, 2]).unwrap().unwrap();
        assert_eq!(decoded.kind, TokenTransferKind::TransferCheckedWithFee);
        assert_eq!(decoded.expected_fee, 4);
    }
}
