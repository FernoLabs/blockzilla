//! Read-only, source-bound access to one immutable SPYx Market DB V3 artifact.
//!
//! The store loads validated trade rows and builds in-memory indexes. Raw token
//! integer amounts remain the database truth. Response prices apply the exact
//! manifest-bound Token-2022 Scaled UI Amount state for each trade.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    path::Path,
};

use anyhow::{Context, Result, ensure};
use blockzilla_dex_parser::{PROGRAM_SPECS, ProgramRole};
use blockzilla_token_transaction_dump::DumpSourceBinding;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    market_builder::{MARKET_REDUCER_SEMANTIC_VERSION, market_parser_implementation_fingerprint},
    market_format::{
        MARKET_HEADER_BYTES, MARKET_MANIFEST_FILE, MARKET_OUTER_INNER_INDEX, MARKET_SCHEMA_VERSION,
        MARKET_TRADE_FLAG_BALANCE_RECONCILED, MARKET_TRADE_FLAG_COMMIT_PROVEN,
        MARKET_TRADE_FLAG_DIRECT_USD_QUOTE, MARKET_TRADE_FLAG_FEE_KNOWN, MARKET_TRADE_FLAG_INNER,
        MARKET_TRADE_FLAG_INPUT_VAULT_MATCH, MARKET_TRADE_FLAG_OUTPUT_VAULT_MATCH,
        MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED, MARKET_TRADE_FLAG_STACK_PROVEN,
        MARKET_TRADE_FLAG_TARGET_INPUT, MARKET_TRADE_FLAG_TARGET_OUTPUT,
        MARKET_TRADE_FLAG_USER_DESTINATION_MATCH, MARKET_TRADE_FLAG_USER_SOURCE_MATCH,
        MARKET_TRADE_RECORD_BYTES, MARKET_TRADES_FILE, MarketCounters, MarketFileHeader,
        MarketManifest, MarketScaledUiHistory, MarketSourceBinding, MarketTradeRecord,
        market_hex_digest, parse_market_hex_digest,
    },
    scaled_ui_amount::{
        LegacyScaledUiAmountState, ScaledUiAmountMultiplier, build_legacy_state_snapshots,
        checked_scaled_raw_amount,
    },
    source::{PinnedSourceFile, SourceDump, load_source_dump},
};

const MAX_MARKET_MANIFEST_BYTES: u64 = 16 << 20;
const MARKET_SCAN_BUFFER_BYTES: usize = 8 << 20;
const REGISTRY_KEY_BYTES: u64 = 32;
const PRICE_DECIMAL_DIGITS: u8 = 18;
const SECONDS_PER_DAY: i64 = 86_400;

pub const MAX_MARKET_TRADE_PAGE_ROWS: usize = 200;
pub const MAX_MARKET_CANDLES: usize = 100_000;
pub const MAX_MARKET_SLOT_CANDLES: usize = 100_000;
pub const MAX_MARKET_PROGRAM_VOLUME_POINTS: usize = 100_000;
pub const MAX_MARKET_TRADER_ACTIVITY_POINTS: usize = 100_000;

/// This value is deliberately narrow. A row belongs to an account only when
/// the DEX parser put that exact registry ID in `MarketTradeRecord::trader_id`.
/// It does not prove that the account signed, is a person, or owns a protocol
/// position.
pub const MARKET_TRADER_ATTRIBUTION: &str = "parser_proven_exact_trader";

#[derive(Debug, Clone, Copy, Default)]
pub struct MarketOpenOptions {
    /// Permit an explicitly bounded canary artifact.
    pub allow_incomplete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketSide {
    /// SPYx is the executed output.
    Buy,
    /// SPYx is the executed input.
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryKeyView {
    pub registry_id: u32,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ExactPrice {
    /// Exact quote-token units per SPYx token, as a rational numerator.
    pub numerator: String,
    /// Exact quote-token units per SPYx token, as a rational denominator.
    pub denominator: String,
    /// A base-10 display string truncated to a fixed number of digits.
    pub decimal: String,
    /// An explicitly non-authoritative chart value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chart_display: Option<f64>,
    /// The Token-2022 UI multiplier used for the target amount.
    pub target_multiplier: String,
    /// Exact IEEE-754 bits for `target_multiplier`.
    pub target_multiplier_bits: String,
    /// The one-based Scaled UI configuration event bound to this trade.
    pub scaled_ui_config_id: u32,
    /// Exact price before the Token-2022 display multiplier is applied.
    pub unscaled_decimal: String,
    /// Non-authoritative chart form of `unscaled_decimal`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unscaled_chart_display: Option<f64>,
    #[serde(skip)]
    target_amount_raw: u64,
    #[serde(skip)]
    quote_amount_raw: u64,
    #[serde(skip)]
    target_decimals: u8,
    #[serde(skip)]
    quote_decimals: u8,
}

impl ExactPrice {
    pub fn decimal_string(&self, fractional_digits: u8) -> Result<String> {
        normalized_price_decimal_string(
            self.target_amount_raw,
            self.quote_amount_raw,
            self.target_decimals,
            self.quote_decimals,
            fractional_digits,
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketHealth {
    pub available: bool,
    pub schema_version: u16,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary_max_transactions: Option<u64>,
    pub source_transactions_scanned: u64,
    pub source_transaction_sha256: String,
    pub market_manifest_sha256: String,
    pub parser_semantic_version: String,
    pub parser_implementation_fingerprint: String,
    pub target_mint: String,
    pub target_mint_id: u32,
    pub target_decimals: u8,
    pub proven_trades: u64,
    pub pairs: u64,
    pub programs: u64,
    /// Deprecated compatibility field. Use `programs` for new clients.
    pub venues: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_latest_block_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketProvenance {
    pub schema_version: u16,
    pub artifact_kind: String,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary_max_transactions: Option<u64>,
    pub created_unix_seconds: u64,
    pub market_manifest_sha256: String,
    pub market_trade_file_sha256: String,
    pub parser_semantic_version: String,
    pub parser_implementation_fingerprint: String,
    pub source: MarketSourceBinding,
    pub counters: MarketCounters,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketSummary {
    pub target_mint: RegistryKeyView,
    pub target_decimals: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_quote_mint: Option<RegistryKeyView>,
    pub pair_count: u64,
    pub program_count: u64,
    /// Deprecated compatibility field. Use `program_count` for new clients.
    pub venue_count: u64,
    pub trade_count: u64,
    pub trade_count_24h: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_block_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_latest_block_time: Option<i64>,
    pub target_volume_raw: String,
    pub target_volume_24h_raw: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_volume_raw: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote_volume_24h_raw: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_price: Option<ExactPrice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_change_24h_chart_percent: Option<f64>,
    pub direct_usd: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_24h_usd_decimal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_24h_usd_chart_display: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketPair {
    pub target_mint: RegistryKeyView,
    pub target_decimals: u8,
    pub quote_mint: RegistryKeyView,
    pub quote_decimals: u8,
    pub trade_count: u64,
    pub trade_count_24h: u64,
    pub program_count: u64,
    /// Deprecated compatibility field. Use `program_count` for new clients.
    pub venue_count: u64,
    pub first_block_time: i64,
    pub last_block_time: i64,
    pub target_volume_raw: String,
    pub quote_volume_raw: String,
    pub target_volume_24h_raw: String,
    pub quote_volume_24h_raw: String,
    pub latest_price: ExactPrice,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_change_24h_chart_percent: Option<f64>,
    pub direct_usd: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_24h_usd_decimal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_24h_usd_chart_display: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketMint {
    pub mint: RegistryKeyView,
    pub decimals: u8,
    pub is_target: bool,
    pub direct_usd_quote: bool,
    pub trade_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_block_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_block_time: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketProgramView {
    pub registry_id: u32,
    pub address: String,
    pub name: String,
    pub role: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketProgramSummary {
    pub program: MarketProgramView,
    pub trade_count: u64,
    pub trade_count_24h: u64,
    pub first_block_time: i64,
    pub last_block_time: i64,
    pub target_volume_raw: String,
    pub target_volume_24h_raw: String,
    pub pair_count: u64,
    pub primary_pool_count: u64,
    /// Deprecated compatibility field. This counts primary stored pools only.
    pub pool_count: u64,
    pub routed_trade_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MarketTransactionCoordinate {
    pub transaction_id: u64,
    pub source_epoch: u64,
    pub slot: u64,
    pub source_block_id: u32,
    pub tx_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MarketInstructionPath {
    pub outer_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inner_index: Option<u32>,
    pub stack_height: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketTradeView {
    /// Stable zero-based ordinal in `market-trades-v2.bin`.
    pub trade_id: u64,
    pub transaction: MarketTransactionCoordinate,
    pub block_time: i64,
    pub instruction: MarketInstructionPath,
    pub instruction_kind_id: u32,
    pub instruction_kind: String,
    pub instruction_discriminator: String,
    /// Executed DEX program. This is the authoritative volume attribution.
    pub program: MarketProgramView,
    /// Deprecated compatibility field. Use `program` for new clients.
    pub venue: RegistryKeyView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub router: Option<MarketProgramView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<RegistryKeyView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trader: Option<RegistryKeyView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_source: Option<RegistryKeyView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_destination: Option<RegistryKeyView>,
    pub target_mint: RegistryKeyView,
    pub quote_mint: RegistryKeyView,
    pub side: MarketSide,
    /// Unchanged on-chain Token-2022 amount.
    pub target_amount_raw: String,
    /// Token-2022 displayed amount after the active multiplier and truncation.
    pub target_amount_scaled_ui_raw: String,
    pub quote_amount_raw: String,
    pub target_decimals: u8,
    pub quote_decimals: u8,
    pub price: ExactPrice,
    pub fee_amount_raw: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fee_mint: Option<RegistryKeyView>,
    pub input_transfer_count: u16,
    pub output_transfer_count: u16,
    pub evidence_flags: u16,
    pub evidence: Vec<&'static str>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct MarketTradeQuery {
    pub quote_mint_id: Option<u32>,
    pub venue_program_id: Option<u32>,
    pub time_from: Option<i64>,
    pub time_to: Option<i64>,
    pub offset: u64,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketTradePage {
    pub total: u64,
    pub offset: u64,
    pub limit: usize,
    pub trades: Vec<MarketTradeView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u64>,
}

/// Exact quote-side amounts for proven trades attributed to one trader.
///
/// Quote amounts from different mints are never added together. A buy spends
/// quote units and receives target units. A sell spends target units and
/// receives quote units.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketTraderQuoteActivity {
    pub quote_mint: RegistryKeyView,
    pub quote_decimals: u8,
    pub trade_count: u64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub target_bought_raw: String,
    pub target_sold_raw: String,
    pub quote_spent_on_buys_raw: String,
    pub quote_received_from_sells_raw: String,
}

/// Exact additive totals for parser-proven trades attributed to one trader.
/// This is trade activity, not a wallet balance or a protocol position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketTraderActivityTotals {
    pub trade_count: u64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub target_bought_raw: String,
    pub target_sold_raw: String,
    pub quote_totals: Vec<MarketTraderQuoteActivity>,
}

/// Full-artifact proven trade totals for one registry account.
///
/// `supported` is true even when `has_proven_trades` is false. This makes a
/// known account with no exact parser attribution an explicit empty result.
/// It must not be converted into an inferred trade or protocol position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketTraderActivitySummary {
    pub supported: bool,
    pub artifact_complete: bool,
    pub has_proven_trades: bool,
    pub attribution: &'static str,
    pub includes_inferred_trades: bool,
    pub includes_protocol_positions: bool,
    pub trader: RegistryKeyView,
    pub target_mint: RegistryKeyView,
    pub target_decimals: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_block_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_block_time: Option<i64>,
    pub totals: MarketTraderActivityTotals,
}

#[derive(Debug, Clone, Copy)]
pub struct MarketTraderActivityQuery {
    pub trader_id: u32,
    pub quote_mint_id: Option<u32>,
    pub dex_program_id: Option<u32>,
    pub interval_seconds: u64,
    pub time_from: i64,
    pub time_to: i64,
    pub max_points: usize,
}

/// One non-empty time bucket of exact parser-proven trader activity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketTraderActivityPoint {
    pub interval_seconds: u64,
    pub start_time: i64,
    pub end_time: i64,
    pub totals: MarketTraderActivityTotals,
}

/// A filtered time series for one exact parser-identified trader.
/// Empty points and zero totals are a supported result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketTraderActivitySeries {
    pub supported: bool,
    pub artifact_complete: bool,
    pub has_matching_proven_trades: bool,
    pub attribution: &'static str,
    pub includes_inferred_trades: bool,
    pub includes_protocol_positions: bool,
    pub trader: RegistryKeyView,
    pub target_mint: RegistryKeyView,
    pub target_decimals: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_quote_mint: Option<RegistryKeyView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_program: Option<MarketProgramView>,
    pub interval_seconds: u64,
    pub time_from: i64,
    pub time_to: i64,
    pub totals: MarketTraderActivityTotals,
    pub points: Vec<MarketTraderActivityPoint>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct MarketOhlcvQuery {
    pub quote_mint_id: u32,
    pub interval_seconds: u64,
    pub time_from: Option<i64>,
    pub time_to: Option<i64>,
    pub venue_program_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Candle {
    pub quote_mint: RegistryKeyView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program: Option<MarketProgramView>,
    /// Deprecated compatibility field. Use `program` for new clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub venue: Option<RegistryKeyView>,
    pub interval_seconds: u64,
    pub start_time: i64,
    pub end_time: i64,
    pub open: ExactPrice,
    pub high: ExactPrice,
    pub low: ExactPrice,
    pub close: ExactPrice,
    pub trade_count: u64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub target_volume_raw: String,
    pub quote_volume_raw: String,
    pub direct_usd: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_usd_decimal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_usd_chart_display: Option<f64>,
}

/// One exact, non-empty Solana-slot OHLCV point. Trades inside the slot use
/// canonical source order: transaction ID, outer instruction, then inner
/// instruction. `program`, when present, is always the executed DEX program;
/// a router is never treated as the execution venue.
#[derive(Debug, Clone, Serialize)]
pub struct MarketSlotCandle {
    pub quote_mint: RegistryKeyView,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub program: Option<MarketProgramView>,
    pub slot: u64,
    pub block_time: i64,
    pub open: ExactPrice,
    pub high: ExactPrice,
    pub low: ExactPrice,
    pub close: ExactPrice,
    pub trade_count: u64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub target_volume_raw: String,
    pub quote_volume_raw: String,
    pub direct_usd: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_usd_decimal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_usd_chart_display: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub struct MarketSlotOhlcvQuery {
    pub quote_mint_id: u32,
    pub dex_program_id: Option<u32>,
    pub slot_from: Option<u64>,
    pub slot_to: Option<u64>,
    pub max_points: usize,
}

/// Executed DEX attribution inside one time bucket. Routed fields are a
/// subset of the DEX totals and do not add a second copy of routed volume.
#[derive(Debug, Clone, Serialize)]
pub struct MarketDexProgramVolume {
    pub program: MarketProgramView,
    pub trade_count: u64,
    pub buy_count: u64,
    pub sell_count: u64,
    pub target_volume_raw: String,
    pub routed_trade_count: u64,
    pub routed_target_volume_raw: String,
    pub router_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketProgramVolumePoint {
    pub interval_seconds: u64,
    pub start_time: i64,
    pub end_time: i64,
    pub trade_count: u64,
    pub target_volume_raw: String,
    /// Program entries are keyed only by `dex_program_id`. Router IDs are
    /// evidence attached to those executed DEX trades, not volume venues.
    pub programs: Vec<MarketDexProgramVolume>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketProgramVolumeSeries {
    pub target_mint: RegistryKeyView,
    pub target_decimals: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_quote_mint: Option<RegistryKeyView>,
    pub interval_seconds: u64,
    pub time_from: i64,
    pub time_to: i64,
    /// Volume is attributed to the executed DEX program. Router evidence is
    /// reported only as a subset of each DEX program total.
    pub attribution: &'static str,
    pub points: Vec<MarketProgramVolumePoint>,
}

#[derive(Debug, Clone, Copy)]
pub struct MarketProgramVolumeQuery {
    pub quote_mint_id: Option<u32>,
    pub interval_seconds: u64,
    pub time_from: i64,
    pub time_to: i64,
    pub max_points: usize,
}

#[derive(Debug, Clone, Copy)]
struct StoredTrade {
    record: MarketTradeRecord,
    quote_mint_id: u32,
    target_amount_raw: u64,
    target_amount_scaled_ui_raw: u64,
    target_multiplier_bits: u64,
    quote_amount_raw: u64,
    target_decimals: u8,
    quote_decimals: u8,
    side: MarketSide,
}

impl StoredTrade {
    fn from_record(record: MarketTradeRecord, target_mint_id: u32) -> Result<Self> {
        let multiplier = ScaledUiAmountMultiplier::from_f64(1.0)?;
        Self::from_record_with_multiplier(record, target_mint_id, &multiplier)
    }

    fn from_record_with_multiplier(
        record: MarketTradeRecord,
        target_mint_id: u32,
        target_multiplier: &ScaledUiAmountMultiplier,
    ) -> Result<Self> {
        let target_multiplier_bits = target_multiplier.to_bits()?;
        let target_input = record.flags & MARKET_TRADE_FLAG_TARGET_INPUT != 0;
        let target_output = record.flags & MARKET_TRADE_FLAG_TARGET_OUTPUT != 0;
        ensure!(
            target_input ^ target_output,
            "market trade does not select one target side"
        );
        if target_input {
            ensure!(
                record.input_mint_id == target_mint_id && record.output_mint_id != target_mint_id,
                "market sell does not bind the target input"
            );
            let target_amount_scaled_ui_raw =
                checked_scaled_raw_amount(record.amount_in, target_multiplier)?;
            Ok(Self {
                record,
                quote_mint_id: record.output_mint_id,
                target_amount_raw: record.amount_in,
                target_amount_scaled_ui_raw,
                target_multiplier_bits,
                quote_amount_raw: record.amount_out,
                target_decimals: record.input_decimals,
                quote_decimals: record.output_decimals,
                side: MarketSide::Sell,
            })
        } else {
            ensure!(
                record.output_mint_id == target_mint_id && record.input_mint_id != target_mint_id,
                "market buy does not bind the target output"
            );
            let target_amount_scaled_ui_raw =
                checked_scaled_raw_amount(record.amount_out, target_multiplier)?;
            Ok(Self {
                record,
                quote_mint_id: record.input_mint_id,
                target_amount_raw: record.amount_out,
                target_amount_scaled_ui_raw,
                target_multiplier_bits,
                quote_amount_raw: record.amount_in,
                target_decimals: record.output_decimals,
                quote_decimals: record.input_decimals,
                side: MarketSide::Buy,
            })
        }
    }

    fn order_key(self) -> (u64, u32, u64) {
        self.record.order_key()
    }

    fn time_key(self) -> (i64, u64, u32, u64) {
        let (_, outer_index, execution_index) = self.record.order_key();
        (
            self.record.block_time,
            self.record.transaction_id,
            outer_index,
            execution_index,
        )
    }

    fn slot_key(self) -> (u64, u64, u32, u64) {
        let (transaction_id, outer_index, execution_index) = self.record.order_key();
        (
            self.record.slot,
            transaction_id,
            outer_index,
            execution_index,
        )
    }
}

pub struct MarketStore {
    manifest_handle: PinnedSourceFile,
    manifest_sha256: [u8; 32],
    manifest: MarketManifest,
    trades_handle: PinnedSourceFile,
    trades: Vec<StoredTrade>,
    all_by_time: Vec<usize>,
    pair_indexes: BTreeMap<u32, Vec<usize>>,
    program_indexes: BTreeMap<u32, Vec<usize>>,
    /// Time-sorted rows with an exact, non-zero parser-provided trader ID.
    trader_indexes: BTreeMap<u32, Vec<usize>>,
    pair_venue_indexes: BTreeMap<(u32, u32), Vec<usize>>,
    pair_slot_indexes: BTreeMap<u32, Vec<usize>>,
    pair_program_slot_indexes: BTreeMap<(u32, u32), Vec<usize>>,
    decimals_by_mint: BTreeMap<u32, u8>,
    venues: BTreeSet<u32>,
    program_views: BTreeMap<u32, MarketProgramView>,
    latest_time: Option<i64>,
    source: SourceDump,
}

impl MarketStore {
    pub fn open(dump: &Path, market: &Path) -> Result<Self> {
        Self::open_with_options(dump, market, MarketOpenOptions::default())
    }

    pub fn open_with_options(
        dump: &Path,
        market: &Path,
        options: MarketOpenOptions,
    ) -> Result<Self> {
        let source = load_source_dump(dump)?;
        let root = fs::canonicalize(market)
            .with_context(|| format!("resolve market artifact {}", market.display()))?;
        ensure!(root.is_dir(), "market artifact is not a directory");

        let manifest_handle =
            PinnedSourceFile::open(&root.join(MARKET_MANIFEST_FILE), "market manifest")?;
        let manifest_bytes = manifest_handle.read_bounded(MAX_MARKET_MANIFEST_BYTES)?;
        let manifest_sha256: [u8; 32] = Sha256::digest(&manifest_bytes).into();
        let manifest: MarketManifest =
            serde_json::from_slice(&manifest_bytes).context("parse market manifest")?;
        manifest.validate()?;
        ensure!(
            manifest.complete || options.allow_incomplete,
            "market artifact is an incomplete canary; allow it explicitly"
        );
        validate_parser_binding(&manifest)?;
        validate_source_binding(&manifest.source, &SourceSnapshot::from_source(&source)?)?;
        validate_target_binding(&manifest, &source)?;

        let trades_handle =
            PinnedSourceFile::open(&root.join(MARKET_TRADES_FILE), "market trade file")?;
        ensure!(
            trades_handle.len() == manifest.trades.bytes,
            "market trade file size differs from its manifest"
        );
        let mut header_bytes = [0u8; MARKET_HEADER_BYTES];
        positioned_read_exact(trades_handle.file(), &mut header_bytes, 0)?;
        let header = MarketFileHeader::decode(&header_bytes)?;
        manifest.validate_header(header)?;

        let kind_programs = validate_instruction_kind_programs(&manifest, &source)?;
        let scaled_ui_configs =
            replay_scaled_ui_configs(&manifest.scaled_ui, manifest.target.mint_id)?;
        let loaded = load_and_validate_trades(
            &trades_handle,
            &manifest,
            &source,
            &kind_programs,
            &header_bytes,
            &scaled_ui_configs,
        )?;
        ensure!(
            loaded.sha256 == parse_market_hex_digest(&manifest.trades.sha256, "trade file digest")?,
            "market trade file digest differs from its manifest"
        );
        let trades = loaded.trades;
        let decimals_by_mint = loaded.decimals_by_mint;
        ensure!(
            u64::try_from(trades.len()).context("market trade count exceeds u64")?
                == manifest.counters.emitted_trades,
            "loaded market trade count differs from the counter total"
        );

        let mut all_by_time = (0..trades.len()).collect::<Vec<_>>();
        all_by_time.sort_unstable_by_key(|index| trades[*index].time_key());
        let mut pair_indexes: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        let mut program_indexes: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        let mut trader_indexes: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        let mut pair_venue_indexes: BTreeMap<(u32, u32), Vec<usize>> = BTreeMap::new();
        let mut pair_slot_indexes: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        let mut pair_program_slot_indexes: BTreeMap<(u32, u32), Vec<usize>> = BTreeMap::new();
        let mut venues = BTreeSet::new();
        let mut latest_time = None;
        for (index, trade) in trades.iter().enumerate() {
            pair_indexes
                .entry(trade.quote_mint_id)
                .or_default()
                .push(index);
            program_indexes
                .entry(trade.record.dex_program_id)
                .or_default()
                .push(index);
            index_exact_trader(&mut trader_indexes, index, *trade);
            pair_venue_indexes
                .entry((trade.quote_mint_id, trade.record.dex_program_id))
                .or_default()
                .push(index);
            pair_slot_indexes
                .entry(trade.quote_mint_id)
                .or_default()
                .push(index);
            pair_program_slot_indexes
                .entry((trade.quote_mint_id, trade.record.dex_program_id))
                .or_default()
                .push(index);
            venues.insert(trade.record.dex_program_id);
            latest_time = Some(latest_time.map_or(trade.record.block_time, |current: i64| {
                current.max(trade.record.block_time)
            }));
        }
        for indexes in pair_indexes
            .values_mut()
            .chain(program_indexes.values_mut())
            .chain(trader_indexes.values_mut())
            .chain(pair_venue_indexes.values_mut())
        {
            indexes.sort_unstable_by_key(|index| trades[*index].time_key());
        }
        for indexes in pair_slot_indexes
            .values_mut()
            .chain(pair_program_slot_indexes.values_mut())
        {
            indexes.sort_unstable_by_key(|index| trades[*index].slot_key());
        }
        let mut program_ids = venues.clone();
        program_ids.extend(trades.iter().filter_map(|trade| {
            (trade.record.router_program_id != 0).then_some(trade.record.router_program_id)
        }));
        let mut program_views = BTreeMap::new();
        for id in program_ids {
            let address = bs58::encode(registry_key(&source, id)?).into_string();
            let spec = PROGRAM_SPECS
                .iter()
                .find(|spec| spec.address == address)
                .with_context(|| format!("market program {address} is absent from parser specs"))?;
            program_views.insert(
                id,
                MarketProgramView {
                    registry_id: id,
                    address,
                    name: spec.label.to_owned(),
                    role: match spec.role {
                        ProgramRole::Venue => "dex",
                        ProgramRole::Router => "router",
                    },
                },
            );
        }

        manifest_handle.verify_identity("market manifest")?;
        trades_handle.verify_identity("market trade file")?;
        source.verify_file_identities()?;
        Ok(Self {
            manifest_handle,
            manifest_sha256,
            manifest,
            trades_handle,
            trades,
            all_by_time,
            pair_indexes,
            program_indexes,
            trader_indexes,
            pair_venue_indexes,
            pair_slot_indexes,
            pair_program_slot_indexes,
            decimals_by_mint,
            venues,
            program_views,
            latest_time,
            source,
        })
    }

    pub fn health(&self) -> MarketHealth {
        MarketHealth {
            available: true,
            schema_version: MARKET_SCHEMA_VERSION,
            complete: self.manifest.complete,
            canary_max_transactions: self.manifest.canary_max_transactions,
            source_transactions_scanned: self.manifest.counters.source_transactions,
            source_transaction_sha256: self.manifest.source.transaction_sha256.clone(),
            market_manifest_sha256: market_hex_digest(self.manifest_sha256),
            parser_semantic_version: self.manifest.parser.semantic_version.clone(),
            parser_implementation_fingerprint: self
                .manifest
                .parser
                .implementation_fingerprint
                .clone(),
            target_mint: self.manifest.target.mint.clone(),
            target_mint_id: self.manifest.target.mint_id,
            target_decimals: self.manifest.target.decimals,
            proven_trades: self.manifest.trades.records,
            pairs: self.pair_indexes.len() as u64,
            programs: self.venues.len() as u64,
            venues: self.venues.len() as u64,
            dataset_latest_block_time: self.latest_time,
        }
    }

    pub fn provenance(&self) -> MarketProvenance {
        MarketProvenance {
            schema_version: self.manifest.schema_version,
            artifact_kind: self.manifest.artifact_kind.clone(),
            complete: self.manifest.complete,
            canary_max_transactions: self.manifest.canary_max_transactions,
            created_unix_seconds: self.manifest.created_unix_seconds,
            market_manifest_sha256: market_hex_digest(self.manifest_sha256),
            market_trade_file_sha256: self.manifest.trades.sha256.clone(),
            parser_semantic_version: self.manifest.parser.semantic_version.clone(),
            parser_implementation_fingerprint: self
                .manifest
                .parser
                .implementation_fingerprint
                .clone(),
            source: self.manifest.source.clone(),
            counters: self.manifest.counters.clone(),
        }
    }

    pub fn scaled_ui_history(&self) -> MarketScaledUiHistory {
        self.manifest.scaled_ui.clone()
    }

    pub fn market_overview(&self, quote_mint_id: Option<u32>) -> Result<MarketSummary> {
        let target_mint = self.registry_view(self.manifest.target.mint_id)?;
        let cutoff = self
            .latest_time
            .map(|time| time.saturating_sub(SECONDS_PER_DAY));
        if let Some(quote_mint_id) = quote_mint_id {
            let pair = self
                .pair_summary(quote_mint_id)?
                .with_context(|| format!("quote mint ID {quote_mint_id} has no proven trades"))?;
            return Ok(MarketSummary {
                target_mint,
                target_decimals: self.manifest.target.decimals,
                selected_quote_mint: Some(pair.quote_mint.clone()),
                pair_count: 1,
                program_count: pair.program_count,
                venue_count: pair.venue_count,
                trade_count: pair.trade_count,
                trade_count_24h: pair.trade_count_24h,
                first_block_time: Some(pair.first_block_time),
                dataset_latest_block_time: self.latest_time,
                target_volume_raw: pair.target_volume_raw,
                target_volume_24h_raw: pair.target_volume_24h_raw,
                quote_volume_raw: Some(pair.quote_volume_raw),
                quote_volume_24h_raw: Some(pair.quote_volume_24h_raw),
                latest_price: Some(pair.latest_price),
                price_change_24h_chart_percent: pair.price_change_24h_chart_percent,
                direct_usd: pair.direct_usd,
                volume_24h_usd_decimal: pair.volume_24h_usd_decimal,
                volume_24h_usd_chart_display: pair.volume_24h_usd_chart_display,
            });
        }

        let totals = volume_totals(self.all_by_time.iter().map(|index| &self.trades[*index]))?;
        let recent = volume_totals(self.all_by_time.iter().filter_map(|index| {
            let trade = &self.trades[*index];
            cutoff
                .is_none_or(|cutoff| trade.record.block_time >= cutoff)
                .then_some(trade)
        }))?;
        Ok(MarketSummary {
            target_mint,
            target_decimals: self.manifest.target.decimals,
            selected_quote_mint: None,
            pair_count: self.pair_indexes.len() as u64,
            program_count: self.venues.len() as u64,
            venue_count: self.venues.len() as u64,
            trade_count: totals.trade_count,
            trade_count_24h: recent.trade_count,
            first_block_time: self
                .all_by_time
                .first()
                .map(|index| self.trades[*index].record.block_time),
            dataset_latest_block_time: self.latest_time,
            target_volume_raw: totals.target_raw.to_string(),
            target_volume_24h_raw: recent.target_raw.to_string(),
            quote_volume_raw: None,
            quote_volume_24h_raw: None,
            latest_price: None,
            price_change_24h_chart_percent: None,
            direct_usd: false,
            volume_24h_usd_decimal: None,
            volume_24h_usd_chart_display: None,
        })
    }

    pub fn pair_summaries(&self) -> Result<Vec<MarketPair>> {
        self.pair_indexes
            .keys()
            .map(|quote| {
                self.pair_summary(*quote)?
                    .context("indexed market pair has no trades")
            })
            .collect()
    }

    /// Return the exact mint set proven by the loaded market rows.
    pub fn mint_summaries(&self) -> Result<Vec<MarketMint>> {
        self.decimals_by_mint
            .iter()
            .map(|(&mint_id, &decimals)| {
                let is_target = mint_id == self.manifest.target.mint_id;
                let indexes = if is_target {
                    self.all_by_time.as_slice()
                } else {
                    self.pair_indexes
                        .get(&mint_id)
                        .map(Vec::as_slice)
                        .context("non-target market mint has no pair index")?
                };
                Ok(MarketMint {
                    mint: self.registry_view(mint_id)?,
                    decimals,
                    is_target,
                    direct_usd_quote: !is_target && self.is_usd_quote(mint_id),
                    trade_count: u64::try_from(indexes.len())
                        .context("market mint trade count exceeds u64")?,
                    first_block_time: indexes
                        .first()
                        .map(|index| self.trades[*index].record.block_time),
                    last_block_time: indexes
                        .last()
                        .map(|index| self.trades[*index].record.block_time),
                })
            })
            .collect()
    }

    pub fn program_summaries(&self) -> Result<Vec<MarketProgramSummary>> {
        let cutoff = self
            .latest_time
            .map(|time| time.saturating_sub(SECONDS_PER_DAY));
        let mut summaries = self
            .program_indexes
            .iter()
            .map(|(&program_id, indexes)| {
                let first = indexes
                    .first()
                    .map(|index| &self.trades[*index])
                    .context("market program index is empty")?;
                let last = indexes
                    .last()
                    .map(|index| &self.trades[*index])
                    .context("market program index is empty")?;
                let totals = volume_totals(indexes.iter().map(|index| &self.trades[*index]))?;
                let recent = volume_totals(indexes.iter().filter_map(|index| {
                    let trade = &self.trades[*index];
                    cutoff
                        .is_none_or(|cutoff| trade.record.block_time >= cutoff)
                        .then_some(trade)
                }))?;
                let pair_count = indexes
                    .iter()
                    .map(|index| self.trades[*index].quote_mint_id)
                    .collect::<BTreeSet<_>>()
                    .len();
                let pool_count = indexes
                    .iter()
                    .filter_map(|index| {
                        let id = self.trades[*index].record.pool_id;
                        (id != 0).then_some(id)
                    })
                    .collect::<BTreeSet<_>>()
                    .len();
                let routed_trade_count = indexes
                    .iter()
                    .filter(|index| self.trades[**index].record.router_program_id != 0)
                    .count();
                Ok(MarketProgramSummary {
                    program: self.program_view(program_id, "dex")?,
                    trade_count: totals.trade_count,
                    trade_count_24h: recent.trade_count,
                    first_block_time: first.record.block_time,
                    last_block_time: last.record.block_time,
                    target_volume_raw: totals.target_raw.to_string(),
                    target_volume_24h_raw: recent.target_raw.to_string(),
                    pair_count: u64::try_from(pair_count)
                        .context("market program pair count exceeds u64")?,
                    primary_pool_count: u64::try_from(pool_count)
                        .context("market program primary pool count exceeds u64")?,
                    pool_count: u64::try_from(pool_count)
                        .context("market program pool count exceeds u64")?,
                    routed_trade_count: u64::try_from(routed_trade_count)
                        .context("market routed trade count exceeds u64")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        summaries.sort_unstable_by(|left, right| {
            right
                .trade_count
                .cmp(&left.trade_count)
                .then_with(|| left.program.registry_id.cmp(&right.program.registry_id))
        });
        Ok(summaries)
    }

    pub fn pair_summary(&self, quote_mint_id: u32) -> Result<Option<MarketPair>> {
        let Some(indexes) = self.pair_indexes.get(&quote_mint_id) else {
            return Ok(None);
        };
        let first = indexes
            .first()
            .map(|index| &self.trades[*index])
            .context("market pair index is empty")?;
        let latest = indexes
            .last()
            .map(|index| &self.trades[*index])
            .context("market pair index is empty")?;
        let totals = volume_totals(indexes.iter().map(|index| &self.trades[*index]))?;
        let cutoff = self
            .latest_time
            .context("non-empty market pair has no dataset time")?
            .saturating_sub(SECONDS_PER_DAY);
        let recent = volume_totals(indexes.iter().filter_map(|index| {
            let trade = &self.trades[*index];
            (trade.record.block_time >= cutoff).then_some(trade)
        }))?;
        let baseline = indexes
            .iter()
            .rev()
            .map(|index| &self.trades[*index])
            .find(|trade| trade.record.block_time <= cutoff);
        let direct_usd = self.is_usd_quote(quote_mint_id);
        let quote_decimals = *self
            .decimals_by_mint
            .get(&quote_mint_id)
            .context("market quote has no validated decimal value")?;
        let venue_count = indexes
            .iter()
            .map(|index| self.trades[*index].record.dex_program_id)
            .collect::<BTreeSet<_>>()
            .len() as u64;
        let volume_24h_usd_decimal =
            direct_usd.then(|| raw_amount_decimal_string(recent.quote_raw, quote_decimals));
        let volume_24h_usd_chart_display = volume_24h_usd_decimal
            .as_deref()
            .and_then(decimal_string_to_f64);
        Ok(Some(MarketPair {
            target_mint: self.registry_view(self.manifest.target.mint_id)?,
            target_decimals: self.manifest.target.decimals,
            quote_mint: self.registry_view(quote_mint_id)?,
            quote_decimals,
            trade_count: totals.trade_count,
            trade_count_24h: recent.trade_count,
            program_count: venue_count,
            venue_count,
            first_block_time: first.record.block_time,
            last_block_time: latest.record.block_time,
            target_volume_raw: totals.target_raw.to_string(),
            quote_volume_raw: totals.quote_raw.to_string(),
            target_volume_24h_raw: recent.target_raw.to_string(),
            quote_volume_24h_raw: recent.quote_raw.to_string(),
            latest_price: exact_price(*latest)?,
            price_change_24h_chart_percent: baseline.and_then(|baseline| {
                chart_price(*baseline).and_then(|old| {
                    chart_price(*latest)
                        .filter(|_| old > 0.0)
                        .map(|new| ((new - old) / old) * 100.0)
                        .filter(|value| value.is_finite())
                })
            }),
            direct_usd,
            volume_24h_usd_decimal,
            volume_24h_usd_chart_display,
        }))
    }

    pub fn ohlcv(&self, query: MarketOhlcvQuery) -> Result<Vec<Candle>> {
        validate_time_range(query.time_from, query.time_to)?;
        ensure!(query.quote_mint_id != 0, "OHLCV quote mint ID is zero");
        ensure!(query.interval_seconds != 0, "OHLCV interval is zero");
        let interval = i64::try_from(query.interval_seconds)
            .context("OHLCV interval exceeds the signed time domain")?;
        let indexes = match query.venue_program_id {
            Some(venue) => self
                .pair_venue_indexes
                .get(&(query.quote_mint_id, venue))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            None => self
                .pair_indexes
                .get(&query.quote_mint_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
        };
        let cores = build_candle_cores(
            &self.trades,
            indexes,
            interval,
            query.time_from,
            query.time_to,
            MAX_MARKET_CANDLES,
        )?;
        let quote_mint = self.registry_view(query.quote_mint_id)?;
        let venue = query
            .venue_program_id
            .map(|id| self.registry_view(id))
            .transpose()?;
        let program = query
            .venue_program_id
            .map(|id| self.program_view(id, "dex"))
            .transpose()?;
        let direct_usd = self.is_usd_quote(query.quote_mint_id);
        let quote_decimals = self
            .decimals_by_mint
            .get(&query.quote_mint_id)
            .copied()
            .unwrap_or_default();
        cores
            .into_iter()
            .map(|core| {
                let usd_decimal = direct_usd
                    .then(|| raw_amount_decimal_string(core.quote_volume_raw, quote_decimals));
                Ok(Candle {
                    quote_mint: quote_mint.clone(),
                    program: program.clone(),
                    venue: venue.clone(),
                    interval_seconds: query.interval_seconds,
                    start_time: core.start_time,
                    end_time: core
                        .start_time
                        .checked_add(interval)
                        .context("OHLCV bucket end overflow")?,
                    open: exact_price(self.trades[core.open])?,
                    high: exact_price(self.trades[core.high])?,
                    low: exact_price(self.trades[core.low])?,
                    close: exact_price(self.trades[core.close])?,
                    trade_count: core.trade_count,
                    buy_count: core.buy_count,
                    sell_count: core.sell_count,
                    target_volume_raw: core.target_volume_raw.to_string(),
                    quote_volume_raw: core.quote_volume_raw.to_string(),
                    direct_usd,
                    volume_usd_chart_display: usd_decimal
                        .as_deref()
                        .and_then(decimal_string_to_f64),
                    volume_usd_decimal: usd_decimal,
                })
            })
            .collect()
    }

    /// Return one candle for each non-empty slot in ascending slot order.
    /// When `slot_from` is absent, the result is the newest `max_points`
    /// non-empty slots at or before `slot_to`. An explicit lower bound is an
    /// exact range and fails if it contains more than `max_points` slots.
    pub fn slot_ohlcv(&self, query: MarketSlotOhlcvQuery) -> Result<Vec<MarketSlotCandle>> {
        validate_slot_range(query.slot_from, query.slot_to)?;
        ensure!(query.quote_mint_id != 0, "slot OHLCV quote mint ID is zero");
        ensure!(
            query.max_points != 0 && query.max_points <= MAX_MARKET_SLOT_CANDLES,
            "slot OHLCV max_points must be between 1 and {MAX_MARKET_SLOT_CANDLES}"
        );
        if let Some(id) = query.dex_program_id {
            ensure!(id != 0, "slot OHLCV DEX program ID is zero");
            ensure!(
                self.program_indexes.contains_key(&id),
                "slot OHLCV program ID is not an executed DEX program"
            );
        }
        let indexes = match query.dex_program_id {
            Some(program) => self
                .pair_program_slot_indexes
                .get(&(query.quote_mint_id, program))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            None => self
                .pair_slot_indexes
                .get(&query.quote_mint_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
        };
        let cores = build_slot_candle_cores(
            &self.trades,
            indexes,
            query.slot_from,
            query.slot_to,
            query.max_points,
        )?;
        let quote_mint = self.registry_view(query.quote_mint_id)?;
        let program = query
            .dex_program_id
            .map(|id| self.program_view(id, "dex"))
            .transpose()?;
        let direct_usd = self.is_usd_quote(query.quote_mint_id);
        let quote_decimals = *self
            .decimals_by_mint
            .get(&query.quote_mint_id)
            .with_context(|| {
                format!(
                    "slot OHLCV quote mint ID {} has no proven pair",
                    query.quote_mint_id
                )
            })?;
        cores
            .into_iter()
            .map(|core| {
                let usd_decimal = direct_usd
                    .then(|| raw_amount_decimal_string(core.quote_volume_raw, quote_decimals));
                Ok(MarketSlotCandle {
                    quote_mint: quote_mint.clone(),
                    program: program.clone(),
                    slot: core.slot,
                    block_time: self.trades[core.open].record.block_time,
                    open: exact_price(self.trades[core.open])?,
                    high: exact_price(self.trades[core.high])?,
                    low: exact_price(self.trades[core.low])?,
                    close: exact_price(self.trades[core.close])?,
                    trade_count: core.trade_count,
                    buy_count: core.buy_count,
                    sell_count: core.sell_count,
                    target_volume_raw: core.target_volume_raw.to_string(),
                    quote_volume_raw: core.quote_volume_raw.to_string(),
                    direct_usd,
                    volume_usd_chart_display: usd_decimal
                        .as_deref()
                        .and_then(decimal_string_to_f64),
                    volume_usd_decimal: usd_decimal,
                })
            })
            .collect()
    }

    /// Aggregate additive SPYx target volume by executed DEX program and time
    /// bucket. Quote amounts are intentionally absent because raw amounts from
    /// different quote mints are not additive. Router IDs remain evidence on
    /// the DEX rows and are never emitted as DEX programs.
    pub fn program_volume_series(
        &self,
        query: MarketProgramVolumeQuery,
    ) -> Result<MarketProgramVolumeSeries> {
        validate_time_range(Some(query.time_from), Some(query.time_to))?;
        ensure!(
            query.interval_seconds != 0,
            "program volume interval is zero"
        );
        ensure!(
            query.max_points != 0 && query.max_points <= MAX_MARKET_PROGRAM_VOLUME_POINTS,
            "program volume max_points must be between 1 and {MAX_MARKET_PROGRAM_VOLUME_POINTS}"
        );
        if let Some(id) = query.quote_mint_id {
            ensure!(id != 0, "program volume quote mint ID is zero");
        }
        let interval = i64::try_from(query.interval_seconds)
            .context("program volume interval exceeds the signed time domain")?;
        let bucket_count = inclusive_time_bucket_count(query.time_from, query.time_to, interval)?;
        ensure!(
            bucket_count <= query.max_points as i128,
            "program volume time window exceeds max_points"
        );
        let indexes = query
            .quote_mint_id
            .map_or(self.all_by_time.as_slice(), |quote| {
                self.pair_indexes
                    .get(&quote)
                    .map(Vec::as_slice)
                    .unwrap_or(&[])
            });
        let cores = build_program_volume_cores(
            &self.trades,
            indexes,
            interval,
            query.time_from,
            query.time_to,
            query.max_points,
        )?;
        let points = cores
            .into_iter()
            .map(|core| {
                let programs = core
                    .programs
                    .into_iter()
                    .map(|(program_id, program)| {
                        Ok(MarketDexProgramVolume {
                            program: self.program_view(program_id, "dex")?,
                            trade_count: program.trade_count,
                            buy_count: program.buy_count,
                            sell_count: program.sell_count,
                            target_volume_raw: program.target_volume_raw.to_string(),
                            routed_trade_count: program.routed_trade_count,
                            routed_target_volume_raw: program.routed_target_volume_raw.to_string(),
                            router_count: u64::try_from(program.router_ids.len())
                                .context("program volume router count exceeds u64")?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(MarketProgramVolumePoint {
                    interval_seconds: query.interval_seconds,
                    start_time: core.start_time,
                    end_time: core
                        .start_time
                        .checked_add(interval)
                        .context("program volume bucket end overflow")?,
                    trade_count: core.trade_count,
                    target_volume_raw: core.target_volume_raw.to_string(),
                    programs,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MarketProgramVolumeSeries {
            target_mint: self.registry_view(self.manifest.target.mint_id)?,
            target_decimals: self.manifest.target.decimals,
            selected_quote_mint: query
                .quote_mint_id
                .map(|id| self.registry_view(id))
                .transpose()?,
            interval_seconds: query.interval_seconds,
            time_from: query.time_from,
            time_to: query.time_to,
            attribution: "executed_dex_program",
            points,
        })
    }

    pub fn is_executed_dex_program(&self, id: u32) -> bool {
        self.program_indexes.contains_key(&id)
    }

    /// Return exact full-artifact DEX trade totals for one parser-identified
    /// trader. A valid registry ID with no indexed trades returns a supported
    /// summary with zero totals.
    pub fn trader_activity_summary(&self, trader_id: u32) -> Result<MarketTraderActivitySummary> {
        ensure!(trader_id != 0, "market trader ID is zero");
        let trader = self.registry_view(trader_id)?;
        let indexes = self
            .trader_indexes
            .get(&trader_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let totals = trader_activity_totals(indexes.iter().map(|index| self.trades[*index]))?;
        Ok(MarketTraderActivitySummary {
            supported: true,
            artifact_complete: self.manifest.complete,
            has_proven_trades: totals.trade_count != 0,
            attribution: MARKET_TRADER_ATTRIBUTION,
            includes_inferred_trades: false,
            includes_protocol_positions: false,
            trader,
            target_mint: self.registry_view(self.manifest.target.mint_id)?,
            target_decimals: self.manifest.target.decimals,
            first_block_time: indexes
                .first()
                .map(|index| self.trades[*index].record.block_time),
            last_block_time: indexes
                .last()
                .map(|index| self.trades[*index].record.block_time),
            totals: self.trader_activity_totals_view(totals)?,
        })
    }

    /// Resolve an address and return its exact trader summary. `None` means
    /// the address is absent from the pinned source registry. A registry-known
    /// address with no proven trades returns `Some` with zero totals.
    pub fn trader_activity_summary_by_address(
        &self,
        address: &str,
    ) -> Result<Option<MarketTraderActivitySummary>> {
        self.registry_id_for_address(address)?
            .map(|id| self.trader_activity_summary(id))
            .transpose()
    }

    /// Return exact non-empty time buckets for one parser-identified trader.
    /// Filters only remove proven rows. They never create inferred activity.
    pub fn trader_activity_series(
        &self,
        query: MarketTraderActivityQuery,
    ) -> Result<MarketTraderActivitySeries> {
        ensure!(query.trader_id != 0, "market trader ID is zero");
        validate_time_range(Some(query.time_from), Some(query.time_to))?;
        ensure!(
            query.interval_seconds != 0,
            "market trader activity interval is zero"
        );
        ensure!(
            query.max_points != 0 && query.max_points <= MAX_MARKET_TRADER_ACTIVITY_POINTS,
            "market trader activity max_points must be between 1 and {MAX_MARKET_TRADER_ACTIVITY_POINTS}"
        );
        if let Some(id) = query.quote_mint_id {
            ensure!(id != 0, "market trader activity quote mint ID is zero");
            self.registry_view(id)
                .context("resolve market trader activity quote mint")?;
        }
        if let Some(id) = query.dex_program_id {
            ensure!(id != 0, "market trader activity DEX program ID is zero");
            ensure!(
                self.is_executed_dex_program(id),
                "market trader activity program ID is not an executed DEX program"
            );
        }
        let trader = self.registry_view(query.trader_id)?;
        let interval = i64::try_from(query.interval_seconds)
            .context("market trader activity interval exceeds the signed time domain")?;
        ensure!(
            inclusive_time_bucket_count(query.time_from, query.time_to, interval)?
                <= query.max_points as i128,
            "market trader activity time window exceeds max_points"
        );
        let indexes = self
            .trader_indexes
            .get(&query.trader_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let core = build_trader_activity_series_core(
            &self.trades,
            indexes,
            query.trader_id,
            interval,
            query.time_from,
            query.time_to,
            query.quote_mint_id,
            query.dex_program_id,
            query.max_points,
        )?;
        let points = core
            .points
            .into_iter()
            .map(|point| {
                Ok(MarketTraderActivityPoint {
                    interval_seconds: query.interval_seconds,
                    start_time: point.start_time,
                    end_time: point
                        .start_time
                        .checked_add(interval)
                        .context("market trader activity bucket end overflow")?,
                    totals: self.trader_activity_totals_view(point.totals)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MarketTraderActivitySeries {
            supported: true,
            artifact_complete: self.manifest.complete,
            has_matching_proven_trades: core.totals.trade_count != 0,
            attribution: MARKET_TRADER_ATTRIBUTION,
            includes_inferred_trades: false,
            includes_protocol_positions: false,
            trader,
            target_mint: self.registry_view(self.manifest.target.mint_id)?,
            target_decimals: self.manifest.target.decimals,
            selected_quote_mint: query
                .quote_mint_id
                .map(|id| self.registry_view(id))
                .transpose()?,
            selected_program: query
                .dex_program_id
                .map(|id| self.program_view(id, "dex"))
                .transpose()?,
            interval_seconds: query.interval_seconds,
            time_from: query.time_from,
            time_to: query.time_to,
            totals: self.trader_activity_totals_view(core.totals)?,
            points,
        })
    }

    /// Return proven trades newest first. The stable `trade_id` is independent
    /// of filtering and paging.
    pub fn paged_trades(&self, query: MarketTradeQuery) -> Result<MarketTradePage> {
        validate_trade_query(query)?;
        let indexes = match (query.quote_mint_id, query.venue_program_id) {
            (Some(quote), Some(venue)) => self
                .pair_venue_indexes
                .get(&(quote, venue))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            (Some(quote), None) => self
                .pair_indexes
                .get(&quote)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            _ => self.all_by_time.as_slice(),
        };
        self.paged_trades_from_indexes(indexes, query)
    }

    /// Return proven trades for one exact parser-identified trader, newest
    /// first. A registry-known account with no proven trades returns an empty
    /// page with `total == 0`.
    pub fn paged_trader_trades(
        &self,
        trader_id: u32,
        query: MarketTradeQuery,
    ) -> Result<MarketTradePage> {
        ensure!(trader_id != 0, "market trader ID is zero");
        self.registry_view(trader_id)?;
        validate_trade_query(query)?;
        let indexes = self
            .trader_indexes
            .get(&trader_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        self.paged_trades_from_indexes(indexes, query)
    }

    pub fn trade_by_ordinal(&self, trade_id: u64) -> Result<Option<MarketTradeView>> {
        let index = usize::try_from(trade_id).context("market trade ID exceeds usize")?;
        if index >= self.trades.len() {
            return Ok(None);
        }
        self.trade_view(index, &mut HashMap::new()).map(Some)
    }

    /// Resolve one base58 public key against the pinned, one-based registry.
    pub fn registry_id_for_address(&self, address: &str) -> Result<Option<u32>> {
        let decoded = bs58::decode(address)
            .into_vec()
            .with_context(|| format!("decode registry address {address}"))?;
        ensure!(decoded.len() == 32, "registry address is not 32 bytes");
        let key: [u8; 32] = decoded.try_into().expect("validated registry key length");
        self.registry_id_for_key(key)
    }

    pub fn pair_summary_by_quote_address(&self, address: &str) -> Result<Option<MarketPair>> {
        let Some(id) = self.registry_id_for_address(address)? else {
            return Ok(None);
        };
        self.pair_summary(id)
    }

    pub fn verify_identities(&self) -> Result<()> {
        self.manifest_handle.verify_identity("market manifest")?;
        self.trades_handle.verify_identity("market trade file")?;
        self.source.verify_file_identities()
    }

    pub const fn complete(&self) -> bool {
        self.manifest.complete
    }

    pub fn source_transaction_sha256(&self) -> &str {
        &self.manifest.source.transaction_sha256
    }

    pub fn source_registry_sha256(&self) -> &str {
        &self.manifest.source.registry_sha256
    }

    /// Minimum finalized RPC context that is not older than this source dump.
    pub fn minimum_metadata_context_slot(&self) -> Result<u64> {
        let DumpSourceBinding::TrustedLocalSizesOnly {
            slots_per_epoch, ..
        } = &self.source.manifest.source_binding;
        self.manifest
            .source
            .last_epoch
            .checked_add(1)
            .and_then(|epoch| epoch.checked_mul(*slots_per_epoch))
            .and_then(|slot| slot.checked_sub(1))
            .context("source final slot overflow")
    }

    fn is_usd_quote(&self, mint_id: u32) -> bool {
        self.manifest
            .usd_quote_mint_ids
            .binary_search(&mint_id)
            .is_ok()
    }

    fn trader_activity_totals_view(
        &self,
        totals: TraderActivityTotalsCore,
    ) -> Result<MarketTraderActivityTotals> {
        let quote_totals = totals
            .quote_totals
            .into_iter()
            .map(|(quote_mint_id, quote)| {
                let quote_decimals = *self
                    .decimals_by_mint
                    .get(&quote_mint_id)
                    .context("market trader quote has no validated decimal value")?;
                Ok(MarketTraderQuoteActivity {
                    quote_mint: self.registry_view(quote_mint_id)?,
                    quote_decimals,
                    trade_count: quote.trade_count,
                    buy_count: quote.buy_count,
                    sell_count: quote.sell_count,
                    target_bought_raw: quote.target_bought_raw.to_string(),
                    target_sold_raw: quote.target_sold_raw.to_string(),
                    quote_spent_on_buys_raw: quote.quote_spent_on_buys_raw.to_string(),
                    quote_received_from_sells_raw: quote.quote_received_from_sells_raw.to_string(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(MarketTraderActivityTotals {
            trade_count: totals.trade_count,
            buy_count: totals.buy_count,
            sell_count: totals.sell_count,
            target_bought_raw: totals.target_bought_raw.to_string(),
            target_sold_raw: totals.target_sold_raw.to_string(),
            quote_totals,
        })
    }

    fn paged_trades_from_indexes(
        &self,
        indexes: &[usize],
        query: MarketTradeQuery,
    ) -> Result<MarketTradePage> {
        let mut total = 0u64;
        let mut selected = Vec::with_capacity(query.limit);
        for index in indexes.iter().rev().copied() {
            let trade = self.trades[index];
            if !trade_matches(&trade, &query) {
                continue;
            }
            if total >= query.offset && selected.len() < query.limit {
                selected.push(index);
            }
            total = total.checked_add(1).context("market page total overflow")?;
        }
        ensure!(
            query.offset <= total,
            "market trade page offset exceeds the filtered result"
        );
        let mut registry_cache = HashMap::new();
        let trades = selected
            .into_iter()
            .map(|index| self.trade_view(index, &mut registry_cache))
            .collect::<Result<Vec<_>>>()?;
        let consumed = u64::try_from(trades.len()).context("market page size exceeds u64")?;
        let end = query
            .offset
            .checked_add(consumed)
            .context("market page end overflow")?;
        Ok(MarketTradePage {
            total,
            offset: query.offset,
            limit: query.limit,
            trades,
            next_offset: (end < total).then_some(end),
        })
    }

    fn trade_view(
        &self,
        index: usize,
        registry_cache: &mut HashMap<u32, RegistryKeyView>,
    ) -> Result<MarketTradeView> {
        let trade = *self
            .trades
            .get(index)
            .context("market trade index exceeds loaded rows")?;
        let record = trade.record;
        let kind_index = usize::try_from(record.instruction_kind_id - 1)
            .context("market instruction kind ID exceeds usize")?;
        let kind = self
            .manifest
            .instruction_kinds
            .get(kind_index)
            .context("market instruction kind ID is absent")?;
        let target_mint =
            self.registry_view_cached(self.manifest.target.mint_id, registry_cache)?;
        let quote_mint = self.registry_view_cached(trade.quote_mint_id, registry_cache)?;
        Ok(MarketTradeView {
            trade_id: u64::try_from(index).context("market trade ordinal exceeds u64")?,
            transaction: MarketTransactionCoordinate {
                transaction_id: record.transaction_id,
                source_epoch: record.source_epoch,
                slot: record.slot,
                source_block_id: record.source_block_id,
                tx_index: record.tx_index,
            },
            block_time: record.block_time,
            instruction: MarketInstructionPath {
                outer_index: record.outer_index,
                inner_index: (record.inner_index != MARKET_OUTER_INNER_INDEX)
                    .then_some(record.inner_index),
                stack_height: record.stack_height,
            },
            instruction_kind_id: record.instruction_kind_id,
            instruction_kind: kind.name.clone(),
            instruction_discriminator: kind.discriminator.clone(),
            program: self.program_view(record.dex_program_id, "dex")?,
            venue: self.registry_view_cached(record.dex_program_id, registry_cache)?,
            router: (record.router_program_id != 0)
                .then(|| self.program_view(record.router_program_id, "router"))
                .transpose()?,
            pool: self.optional_registry_view(record.pool_id, registry_cache)?,
            trader: self.optional_registry_view(record.trader_id, registry_cache)?,
            user_source: self.optional_registry_view(record.user_source_id, registry_cache)?,
            user_destination: self
                .optional_registry_view(record.user_destination_id, registry_cache)?,
            target_mint,
            quote_mint,
            side: trade.side,
            target_amount_raw: trade.target_amount_raw.to_string(),
            target_amount_scaled_ui_raw: trade.target_amount_scaled_ui_raw.to_string(),
            quote_amount_raw: trade.quote_amount_raw.to_string(),
            target_decimals: trade.target_decimals,
            quote_decimals: trade.quote_decimals,
            price: exact_price(trade)?,
            fee_amount_raw: record.fee_amount.to_string(),
            fee_mint: self.optional_registry_view(record.fee_mint_id, registry_cache)?,
            input_transfer_count: record.input_transfer_count,
            output_transfer_count: record.output_transfer_count,
            evidence_flags: record.flags,
            evidence: evidence_names(record.flags),
        })
    }

    fn optional_registry_view(
        &self,
        id: u32,
        cache: &mut HashMap<u32, RegistryKeyView>,
    ) -> Result<Option<RegistryKeyView>> {
        (id != 0)
            .then(|| self.registry_view_cached(id, cache))
            .transpose()
    }

    fn registry_view_cached(
        &self,
        id: u32,
        cache: &mut HashMap<u32, RegistryKeyView>,
    ) -> Result<RegistryKeyView> {
        if let Some(view) = cache.get(&id) {
            return Ok(view.clone());
        }
        let view = self.registry_view(id)?;
        cache.insert(id, view.clone());
        Ok(view)
    }

    fn registry_view(&self, id: u32) -> Result<RegistryKeyView> {
        let key = registry_key(&self.source, id)?;
        Ok(RegistryKeyView {
            registry_id: id,
            address: bs58::encode(key).into_string(),
        })
    }

    fn program_view(&self, id: u32, role: &'static str) -> Result<MarketProgramView> {
        let mut view = self
            .program_views
            .get(&id)
            .with_context(|| format!("market program ID {id} is absent from the program cache"))?
            .clone();
        view.role = role;
        Ok(view)
    }

    fn registry_id_for_key(&self, key: [u8; 32]) -> Result<Option<u32>> {
        let mut left = 0u64;
        let mut right = self.source.pubkeys;
        let mut row = [0u8; REGISTRY_KEY_BYTES as usize];
        while left < right {
            let middle = left + (right - left) / 2;
            positioned_read_exact(
                self.source.registry_handle.file(),
                &mut row,
                middle
                    .checked_mul(REGISTRY_KEY_BYTES)
                    .context("registry row offset overflow")?,
            )?;
            match row.cmp(&key) {
                Ordering::Less => left = middle + 1,
                Ordering::Greater => right = middle,
                Ordering::Equal => {
                    return Ok(Some(
                        u32::try_from(middle + 1).context("registry ID exceeds u32")?,
                    ));
                }
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Clone)]
struct SourceSnapshot {
    manifest_bytes: u64,
    manifest_sha256: String,
    transaction_bytes: u64,
    transaction_sha256: String,
    signature_bytes: u64,
    signature_sha256: String,
    registry_bytes: u64,
    registry_sha256: String,
    accounts_bytes: u64,
    accounts_sha256: String,
    first_epoch: u64,
    last_epoch: u64,
    transactions: u64,
    signatures: u64,
    pubkeys: u64,
    accounts: u64,
}

impl SourceSnapshot {
    fn from_source(source: &SourceDump) -> Result<Self> {
        Ok(Self {
            manifest_bytes: source.manifest_handle.len(),
            manifest_sha256: market_hex_digest(source.manifest_sha256),
            transaction_bytes: source.transaction_bytes,
            transaction_sha256: market_hex_digest(source.transaction_sha256),
            signature_bytes: source.signature_bytes,
            signature_sha256: market_hex_digest(source.signature_sha256),
            registry_bytes: source.registry_bytes,
            registry_sha256: market_hex_digest(source.registry_sha256),
            accounts_bytes: source.accounts_bytes,
            accounts_sha256: market_hex_digest(source.accounts_sha256),
            first_epoch: source.manifest.first_epoch,
            last_epoch: source.manifest.last_epoch,
            transactions: source.manifest.transactions,
            signatures: source.signatures,
            pubkeys: source.pubkeys,
            accounts: source
                .manifest
                .discovered_account_count
                .context("source manifest has no discovered-account count")?,
        })
    }
}

fn validate_source_binding(binding: &MarketSourceBinding, source: &SourceSnapshot) -> Result<()> {
    ensure!(
        binding.manifest_bytes == source.manifest_bytes
            && binding.manifest_sha256 == source.manifest_sha256
            && binding.transaction_bytes == source.transaction_bytes
            && binding.transaction_sha256 == source.transaction_sha256
            && binding.signature_bytes == source.signature_bytes
            && binding.signature_sha256 == source.signature_sha256
            && binding.registry_bytes == source.registry_bytes
            && binding.registry_sha256 == source.registry_sha256
            && binding.accounts_bytes == source.accounts_bytes
            && binding.accounts_sha256 == source.accounts_sha256
            && binding.first_epoch == source.first_epoch
            && binding.last_epoch == source.last_epoch
            && binding.transactions == source.transactions
            && binding.signatures == source.signatures
            && binding.pubkeys == source.pubkeys
            && binding.accounts == source.accounts,
        "market source hashes, sizes, ranges, or counts differ from the consolidated dump"
    );
    Ok(())
}

fn validate_parser_binding(manifest: &MarketManifest) -> Result<()> {
    ensure!(
        manifest.parser.semantic_version == MARKET_REDUCER_SEMANTIC_VERSION
            && manifest.parser.implementation_fingerprint
                == market_parser_implementation_fingerprint(),
        "market parser version or implementation fingerprint differs from this reader"
    );
    Ok(())
}

fn validate_target_binding(manifest: &MarketManifest, source: &SourceDump) -> Result<()> {
    ensure!(
        manifest.target.mint == source.manifest.mint
            && manifest.target.mint == bs58::encode(source.mint).into_string(),
        "market target mint differs from the consolidated dump"
    );
    ensure!(
        registry_key(source, manifest.target.mint_id)? == source.mint,
        "market target mint ID does not resolve to the consolidated mint"
    );
    for id in &manifest.usd_quote_mint_ids {
        registry_key(source, *id).context("resolve direct USD quote mint ID")?;
    }
    Ok(())
}

fn validate_instruction_kind_programs(
    manifest: &MarketManifest,
    source: &SourceDump,
) -> Result<Vec<[u8; 32]>> {
    let mut programs = Vec::new();
    programs
        .try_reserve_exact(manifest.instruction_kinds.len())
        .context("reserve instruction-kind program table")?;
    for kind in &manifest.instruction_kinds {
        let bytes = bs58::decode(&kind.program)
            .into_vec()
            .with_context(|| format!("decode instruction-kind program {}", kind.program))?;
        ensure!(
            bytes.len() == 32,
            "instruction-kind program is not 32 bytes"
        );
        let program: [u8; 32] = bytes.try_into().expect("validated program length");
        ensure!(
            registry_id_for_source_key(source, program)?.is_some(),
            "instruction-kind program is absent from the pinned source registry"
        );
        // The exact registry ID is row data. The row scan binds that ID to this
        // validated raw program key.
        programs.push(program);
    }
    ensure!(
        u64::try_from(programs.len()).context("instruction-kind count exceeds u64")?
            <= source.pubkeys,
        "instruction-kind table is larger than the source registry"
    );
    Ok(programs)
}

fn replay_scaled_ui_configs(
    history: &MarketScaledUiHistory,
    target_mint_id: u32,
) -> Result<Vec<LegacyScaledUiAmountState>> {
    if !history.enabled {
        ensure!(
            history.events.is_empty(),
            "disabled Scaled UI history contains events"
        );
        return Ok(Vec::new());
    }
    build_legacy_state_snapshots(&history.events, target_mint_id)
}

fn load_and_validate_trades(
    file: &PinnedSourceFile,
    manifest: &MarketManifest,
    source: &SourceDump,
    kind_programs: &[[u8; 32]],
    header: &[u8; MARKET_HEADER_BYTES],
    scaled_ui_configs: &[LegacyScaledUiAmountState],
) -> Result<LoadedTradeRows> {
    let record_count = manifest.trades.records;
    let capacity = usize::try_from(record_count).context("market trade count exceeds usize")?;
    let mut trades = Vec::new();
    trades
        .try_reserve_exact(capacity)
        .context("reserve validated market trades")?;
    let mut decimals_by_mint = BTreeMap::new();
    decimals_by_mint.insert(manifest.target.mint_id, manifest.target.decimals);
    let mut program_key_cache = HashMap::new();
    let mut previous_order = None;
    let mut hasher = Sha256::new();
    hasher.update(header);

    let rows_per_chunk = MARKET_SCAN_BUFFER_BYTES / MARKET_TRADE_RECORD_BYTES;
    ensure!(
        rows_per_chunk != 0,
        "market scan buffer cannot hold one row"
    );
    let mut buffer = vec![0u8; rows_per_chunk * MARKET_TRADE_RECORD_BYTES];
    let mut ordinal = 0u64;
    while ordinal < record_count {
        let rows = usize::try_from((record_count - ordinal).min(rows_per_chunk as u64))
            .context("market scan chunk row count exceeds usize")?;
        let byte_count = rows
            .checked_mul(MARKET_TRADE_RECORD_BYTES)
            .context("market scan chunk byte count overflow")?;
        let offset = u64::try_from(MARKET_HEADER_BYTES)
            .expect("market header size fits u64")
            .checked_add(
                ordinal
                    .checked_mul(MARKET_TRADE_RECORD_BYTES as u64)
                    .context("market trade row offset overflow")?,
            )
            .context("market trade file offset overflow")?;
        positioned_read_exact(file.file(), &mut buffer[..byte_count], offset)?;
        hasher.update(&buffer[..byte_count]);

        for encoded in buffer[..byte_count].chunks_exact(MARKET_TRADE_RECORD_BYTES) {
            let record = MarketTradeRecord::decode(encoded)
                .with_context(|| format!("decode market trade row {ordinal}"))?;
            validate_trade_against_manifest(
                record,
                manifest,
                source,
                kind_programs,
                &mut program_key_cache,
            )
            .with_context(|| format!("validate market trade row {ordinal}"))?;
            let trade = if record.scaled_ui_config_id == 0 {
                StoredTrade::from_record(record, manifest.target.mint_id)?
            } else {
                let config_index = usize::try_from(record.scaled_ui_config_id - 1)
                    .context("Scaled UI config ID exceeds usize")?;
                let state = scaled_ui_configs
                    .get(config_index)
                    .context("market trade Scaled UI config ID is absent")?;
                let active = state.active_at(record.block_time);
                StoredTrade::from_record_with_multiplier(
                    record,
                    manifest.target.mint_id,
                    &active.multiplier,
                )?
            };
            ensure!(
                previous_order.is_none_or(|previous| previous < trade.order_key()),
                "market trade rows are not strictly ordered and unique"
            );
            bind_decimals(
                &mut decimals_by_mint,
                record.input_mint_id,
                record.input_decimals,
            )?;
            bind_decimals(
                &mut decimals_by_mint,
                record.output_mint_id,
                record.output_decimals,
            )?;
            previous_order = Some(trade.order_key());
            trades.push(trade);
            ordinal = ordinal
                .checked_add(1)
                .context("market trade scan ordinal overflow")?;
        }
    }
    ensure!(
        ordinal == record_count && trades.len() == capacity,
        "market trade scan did not consume the exact bound row count"
    );
    file.verify_identity("market trade file")?;
    Ok(LoadedTradeRows {
        trades,
        sha256: hasher.finalize().into(),
        decimals_by_mint,
    })
}

struct LoadedTradeRows {
    trades: Vec<StoredTrade>,
    sha256: [u8; 32],
    decimals_by_mint: BTreeMap<u32, u8>,
}

fn validate_trade_against_manifest(
    record: MarketTradeRecord,
    manifest: &MarketManifest,
    source: &SourceDump,
    kind_programs: &[[u8; 32]],
    program_key_cache: &mut HashMap<u32, [u8; 32]>,
) -> Result<()> {
    record.validate()?;
    ensure!(
        record.transaction_id < manifest.counters.source_transactions,
        "market trade transaction ID is outside the scanned source prefix"
    );
    ensure!(
        (manifest.source.first_epoch..=manifest.source.last_epoch).contains(&record.source_epoch),
        "market trade epoch is outside the source range"
    );
    ensure!(
        record.block_time > 0,
        "market trade has no positive block time"
    );
    for (id, label) in [
        (record.dex_program_id, "DEX program"),
        (record.router_program_id, "router program"),
        (record.pool_id, "pool"),
        (record.trader_id, "trader"),
        (record.input_mint_id, "input mint"),
        (record.output_mint_id, "output mint"),
        (record.user_source_id, "user source"),
        (record.user_destination_id, "user destination"),
        (record.fee_mint_id, "fee mint"),
    ] {
        ensure!(
            id == 0 || u64::from(id) <= source.pubkeys,
            "market trade {label} ID exceeds the pinned registry"
        );
    }
    ensure!(
        record.flags & MARKET_TRADE_FLAG_STACK_PROVEN != 0
            && record.flags & MARKET_TRADE_FLAG_BALANCE_RECONCILED != 0
            && record.flags & MARKET_TRADE_FLAG_COMMIT_PROVEN != 0,
        "market trade lacks required commit, stack, or balance proof"
    );
    if manifest.scaled_ui.enabled {
        ensure!(
            record.scaled_ui_config_id != 0
                && usize::try_from(record.scaled_ui_config_id)
                    .is_ok_and(|id| id <= manifest.scaled_ui.events.len()),
            "market trade Scaled UI config ID is absent from the manifest"
        );
    } else {
        ensure!(
            record.scaled_ui_config_id == 0,
            "unscaled market trade has a Scaled UI config ID"
        );
    }
    let trade = StoredTrade::from_record(record, manifest.target.mint_id)?;
    ensure!(
        trade.target_decimals == manifest.target.decimals,
        "market trade target decimals differ from the manifest"
    );
    ensure!(
        (record.flags & MARKET_TRADE_FLAG_DIRECT_USD_QUOTE != 0)
            == manifest
                .usd_quote_mint_ids
                .binary_search(&trade.quote_mint_id)
                .is_ok(),
        "market trade direct-USD flag differs from the manifest quote set"
    );
    let kind_index = usize::try_from(record.instruction_kind_id)
        .ok()
        .and_then(|id| id.checked_sub(1))
        .context("market trade instruction kind ID is zero or exceeds usize")?;
    let expected_program = kind_programs
        .get(kind_index)
        .context("market trade instruction kind is absent from the manifest")?;
    let actual_program = if let Some(key) = program_key_cache.get(&record.dex_program_id) {
        *key
    } else {
        let key = registry_key(source, record.dex_program_id)?;
        program_key_cache.insert(record.dex_program_id, key);
        key
    };
    ensure!(
        &actual_program == expected_program,
        "market trade DEX program differs from its instruction-kind definition"
    );
    Ok(())
}

fn bind_decimals(values: &mut BTreeMap<u32, u8>, mint_id: u32, decimals: u8) -> Result<()> {
    if let Some(previous) = values.insert(mint_id, decimals) {
        ensure!(
            previous == decimals,
            "one market mint has inconsistent decimal values"
        );
    }
    Ok(())
}

fn registry_key(source: &SourceDump, id: u32) -> Result<[u8; 32]> {
    ensure!(
        id != 0 && u64::from(id) <= source.pubkeys,
        "registry ID is zero or exceeds the source registry"
    );
    let offset = u64::from(id - 1)
        .checked_mul(REGISTRY_KEY_BYTES)
        .context("registry key offset overflow")?;
    let mut key = [0u8; REGISTRY_KEY_BYTES as usize];
    positioned_read_exact(source.registry_handle.file(), &mut key, offset)?;
    Ok(key)
}

fn registry_id_for_source_key(source: &SourceDump, key: [u8; 32]) -> Result<Option<u32>> {
    let mut left = 0u64;
    let mut right = source.pubkeys;
    let mut row = [0u8; REGISTRY_KEY_BYTES as usize];
    while left < right {
        let middle = left + (right - left) / 2;
        positioned_read_exact(
            source.registry_handle.file(),
            &mut row,
            middle
                .checked_mul(REGISTRY_KEY_BYTES)
                .context("registry row offset overflow")?,
        )?;
        match row.cmp(&key) {
            Ordering::Less => left = middle + 1,
            Ordering::Greater => right = middle,
            Ordering::Equal => {
                return Ok(Some(
                    u32::try_from(middle + 1).context("registry ID exceeds u32")?,
                ));
            }
        }
    }
    Ok(None)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct VolumeTotals {
    trade_count: u64,
    target_raw: u128,
    quote_raw: u128,
}

fn index_exact_trader(indexes: &mut BTreeMap<u32, Vec<usize>>, index: usize, trade: StoredTrade) {
    if trade.record.trader_id != 0 {
        indexes
            .entry(trade.record.trader_id)
            .or_default()
            .push(index);
    }
}

fn volume_totals<'a>(trades: impl IntoIterator<Item = &'a StoredTrade>) -> Result<VolumeTotals> {
    let mut totals = VolumeTotals::default();
    for trade in trades {
        totals.trade_count = totals
            .trade_count
            .checked_add(1)
            .context("market trade count overflow")?;
        totals.target_raw = totals
            .target_raw
            .checked_add(u128::from(trade.target_amount_raw))
            .context("market target volume overflow")?;
        totals.quote_raw = totals
            .quote_raw
            .checked_add(u128::from(trade.quote_amount_raw))
            .context("market quote volume overflow")?;
    }
    Ok(totals)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TraderQuoteActivityCore {
    trade_count: u64,
    buy_count: u64,
    sell_count: u64,
    target_bought_raw: u128,
    target_sold_raw: u128,
    quote_spent_on_buys_raw: u128,
    quote_received_from_sells_raw: u128,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TraderActivityTotalsCore {
    trade_count: u64,
    buy_count: u64,
    sell_count: u64,
    target_bought_raw: u128,
    target_sold_raw: u128,
    quote_totals: BTreeMap<u32, TraderQuoteActivityCore>,
}

impl TraderActivityTotalsCore {
    fn push(&mut self, trade: StoredTrade) -> Result<()> {
        ensure!(
            trade.record.trader_id != 0,
            "market trader activity row has no exact trader"
        );
        self.trade_count = self
            .trade_count
            .checked_add(1)
            .context("market trader activity trade count overflow")?;
        let target_amount = u128::from(trade.target_amount_raw);
        let quote_amount = u128::from(trade.quote_amount_raw);
        let quote = self.quote_totals.entry(trade.quote_mint_id).or_default();
        quote.trade_count = quote
            .trade_count
            .checked_add(1)
            .context("market trader quote trade count overflow")?;
        match trade.side {
            MarketSide::Buy => {
                self.buy_count = self
                    .buy_count
                    .checked_add(1)
                    .context("market trader buy count overflow")?;
                self.target_bought_raw = self
                    .target_bought_raw
                    .checked_add(target_amount)
                    .context("market trader target bought amount overflow")?;
                quote.buy_count = quote
                    .buy_count
                    .checked_add(1)
                    .context("market trader quote buy count overflow")?;
                quote.target_bought_raw = quote
                    .target_bought_raw
                    .checked_add(target_amount)
                    .context("market trader quote target bought amount overflow")?;
                quote.quote_spent_on_buys_raw = quote
                    .quote_spent_on_buys_raw
                    .checked_add(quote_amount)
                    .context("market trader quote spent amount overflow")?;
            }
            MarketSide::Sell => {
                self.sell_count = self
                    .sell_count
                    .checked_add(1)
                    .context("market trader sell count overflow")?;
                self.target_sold_raw = self
                    .target_sold_raw
                    .checked_add(target_amount)
                    .context("market trader target sold amount overflow")?;
                quote.sell_count = quote
                    .sell_count
                    .checked_add(1)
                    .context("market trader quote sell count overflow")?;
                quote.target_sold_raw = quote
                    .target_sold_raw
                    .checked_add(target_amount)
                    .context("market trader quote target sold amount overflow")?;
                quote.quote_received_from_sells_raw = quote
                    .quote_received_from_sells_raw
                    .checked_add(quote_amount)
                    .context("market trader quote received amount overflow")?;
            }
        }
        Ok(())
    }
}

fn trader_activity_totals(
    trades: impl IntoIterator<Item = StoredTrade>,
) -> Result<TraderActivityTotalsCore> {
    let mut totals = TraderActivityTotalsCore::default();
    for trade in trades {
        totals.push(trade)?;
    }
    Ok(totals)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraderActivityPointCore {
    start_time: i64,
    totals: TraderActivityTotalsCore,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TraderActivitySeriesCore {
    totals: TraderActivityTotalsCore,
    points: Vec<TraderActivityPointCore>,
}

#[allow(clippy::too_many_arguments)]
fn build_trader_activity_series_core(
    trades: &[StoredTrade],
    indexes: &[usize],
    trader_id: u32,
    interval: i64,
    time_from: i64,
    time_to: i64,
    quote_mint_id: Option<u32>,
    dex_program_id: Option<u32>,
    max_points: usize,
) -> Result<TraderActivitySeriesCore> {
    ensure!(
        interval > 0 && max_points != 0,
        "market trader activity interval or max_points is not positive"
    );
    ensure!(trader_id != 0, "market trader activity ID is zero");
    validate_time_range(Some(time_from), Some(time_to))?;
    ensure!(
        inclusive_time_bucket_count(time_from, time_to, interval)? <= max_points as i128,
        "market trader activity time window exceeds max_points"
    );
    let mut output = TraderActivitySeriesCore::default();
    let start = indexes.partition_point(|index| trades[*index].record.block_time < time_from);
    let end = indexes.partition_point(|index| trades[*index].record.block_time <= time_to);
    for index in indexes[start..end].iter().copied() {
        let trade = *trades
            .get(index)
            .context("market trader activity index exceeds the loaded trade rows")?;
        ensure!(
            trade.record.trader_id == trader_id,
            "market trader activity index contains a different trader"
        );
        if quote_mint_id.is_some_and(|id| trade.quote_mint_id != id)
            || dex_program_id.is_some_and(|id| trade.record.dex_program_id != id)
        {
            continue;
        }
        let start_time = trade
            .record
            .block_time
            .div_euclid(interval)
            .checked_mul(interval)
            .context("market trader activity bucket start overflow")?;
        if output
            .points
            .last()
            .is_none_or(|point| point.start_time != start_time)
        {
            ensure!(
                output.points.len() < max_points,
                "market trader activity response exceeds max_points"
            );
            output.points.push(TraderActivityPointCore {
                start_time,
                totals: TraderActivityTotalsCore::default(),
            });
        }
        output
            .points
            .last_mut()
            .expect("trader activity point was inserted")
            .totals
            .push(trade)?;
        output.totals.push(trade)?;
    }
    Ok(output)
}

#[derive(Debug, Clone, Copy)]
struct CandleCore {
    start_time: i64,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    trade_count: u64,
    buy_count: u64,
    sell_count: u64,
    target_volume_raw: u128,
    quote_volume_raw: u128,
}

impl CandleCore {
    fn new(start_time: i64, index: usize, trade: StoredTrade) -> Self {
        Self {
            start_time,
            open: index,
            high: index,
            low: index,
            close: index,
            trade_count: 1,
            buy_count: u64::from(trade.side == MarketSide::Buy),
            sell_count: u64::from(trade.side == MarketSide::Sell),
            target_volume_raw: u128::from(trade.target_amount_raw),
            quote_volume_raw: u128::from(trade.quote_amount_raw),
        }
    }

    fn push(&mut self, index: usize, trades: &[StoredTrade]) -> Result<()> {
        let trade = trades[index];
        ensure!(
            trade.quote_mint_id == trades[self.open].quote_mint_id
                && trade.quote_decimals == trades[self.open].quote_decimals
                && trade.target_decimals == trades[self.open].target_decimals,
            "OHLCV bucket mixes different pairs or decimal domains"
        );
        if compare_prices(trade, trades[self.high])? == Ordering::Greater {
            self.high = index;
        }
        if compare_prices(trade, trades[self.low])? == Ordering::Less {
            self.low = index;
        }
        self.close = index;
        self.trade_count = self
            .trade_count
            .checked_add(1)
            .context("OHLCV trade count overflow")?;
        match trade.side {
            MarketSide::Buy => {
                self.buy_count = self
                    .buy_count
                    .checked_add(1)
                    .context("buy count overflow")?
            }
            MarketSide::Sell => {
                self.sell_count = self
                    .sell_count
                    .checked_add(1)
                    .context("sell count overflow")?
            }
        }
        self.target_volume_raw = self
            .target_volume_raw
            .checked_add(u128::from(trade.target_amount_raw))
            .context("OHLCV target volume overflow")?;
        self.quote_volume_raw = self
            .quote_volume_raw
            .checked_add(u128::from(trade.quote_amount_raw))
            .context("OHLCV quote volume overflow")?;
        Ok(())
    }
}

fn build_candle_cores(
    trades: &[StoredTrade],
    indexes: &[usize],
    interval: i64,
    time_from: Option<i64>,
    time_to: Option<i64>,
    maximum_candles: usize,
) -> Result<Vec<CandleCore>> {
    ensure!(
        interval > 0 && maximum_candles != 0,
        "OHLCV interval or candle limit is not positive"
    );
    let mut output = Vec::new();
    let mut current: Option<CandleCore> = None;
    for index in indexes.iter().copied() {
        let trade = *trades
            .get(index)
            .context("OHLCV index exceeds the loaded trade rows")?;
        if !time_in_range(trade.record.block_time, time_from, time_to) {
            continue;
        }
        let start_time = trade
            .record
            .block_time
            .div_euclid(interval)
            .checked_mul(interval)
            .context("OHLCV bucket start overflow")?;
        match current.as_mut() {
            Some(candle) if candle.start_time == start_time => candle.push(index, trades)?,
            Some(_) => {
                ensure!(
                    output.len() < maximum_candles,
                    "OHLCV response exceeds the candle limit; use a larger interval or a smaller range"
                );
                output.push(current.take().expect("present candle"));
                current = Some(CandleCore::new(start_time, index, trade));
            }
            None => current = Some(CandleCore::new(start_time, index, trade)),
        }
    }
    if let Some(candle) = current {
        ensure!(
            output.len() < maximum_candles,
            "OHLCV response exceeds the candle limit; use a larger interval or a smaller range"
        );
        output.push(candle);
    }
    Ok(output)
}

#[derive(Debug, Clone, Copy)]
struct SlotCandleCore {
    slot: u64,
    open: usize,
    high: usize,
    low: usize,
    close: usize,
    trade_count: u64,
    buy_count: u64,
    sell_count: u64,
    target_volume_raw: u128,
    quote_volume_raw: u128,
}

impl SlotCandleCore {
    fn new(index: usize, trade: StoredTrade) -> Self {
        Self {
            slot: trade.record.slot,
            open: index,
            high: index,
            low: index,
            close: index,
            trade_count: 1,
            buy_count: u64::from(trade.side == MarketSide::Buy),
            sell_count: u64::from(trade.side == MarketSide::Sell),
            target_volume_raw: u128::from(trade.target_amount_raw),
            quote_volume_raw: u128::from(trade.quote_amount_raw),
        }
    }

    fn push(&mut self, index: usize, trades: &[StoredTrade]) -> Result<()> {
        let trade = *trades
            .get(index)
            .context("slot OHLCV index exceeds the loaded trade rows")?;
        let first = trades[self.open];
        ensure!(
            trade.record.slot == self.slot
                && trade.record.block_time == first.record.block_time
                && trade.quote_mint_id == first.quote_mint_id
                && trade.quote_decimals == first.quote_decimals
                && trade.target_decimals == first.target_decimals,
            "slot OHLCV bucket mixes slots, block times, pairs, or decimal domains"
        );
        if compare_prices(trade, trades[self.high])? == Ordering::Greater {
            self.high = index;
        }
        if compare_prices(trade, trades[self.low])? == Ordering::Less {
            self.low = index;
        }
        self.close = index;
        self.trade_count = self
            .trade_count
            .checked_add(1)
            .context("slot OHLCV trade count overflow")?;
        match trade.side {
            MarketSide::Buy => {
                self.buy_count = self
                    .buy_count
                    .checked_add(1)
                    .context("slot OHLCV buy count overflow")?;
            }
            MarketSide::Sell => {
                self.sell_count = self
                    .sell_count
                    .checked_add(1)
                    .context("slot OHLCV sell count overflow")?;
            }
        }
        self.target_volume_raw = self
            .target_volume_raw
            .checked_add(u128::from(trade.target_amount_raw))
            .context("slot OHLCV target volume overflow")?;
        self.quote_volume_raw = self
            .quote_volume_raw
            .checked_add(u128::from(trade.quote_amount_raw))
            .context("slot OHLCV quote volume overflow")?;
        Ok(())
    }
}

fn build_slot_candle_cores(
    trades: &[StoredTrade],
    indexes: &[usize],
    slot_from: Option<u64>,
    slot_to: Option<u64>,
    max_points: usize,
) -> Result<Vec<SlotCandleCore>> {
    validate_slot_range(slot_from, slot_to)?;
    ensure!(max_points != 0, "slot OHLCV max_points is zero");

    let end = slot_to.map_or(indexes.len(), |upper| {
        indexes.partition_point(|index| trades[*index].record.slot <= upper)
    });
    let start = slot_from.map_or_else(
        || newest_non_empty_slot_start(trades, indexes, end, max_points),
        |lower| indexes[..end].partition_point(|index| trades[*index].record.slot < lower),
    );

    let mut output = Vec::with_capacity(max_points.min(end.saturating_sub(start)));
    let mut current: Option<SlotCandleCore> = None;
    for index in indexes[start..end].iter().copied() {
        let trade = *trades
            .get(index)
            .context("slot OHLCV index exceeds the loaded trade rows")?;
        match current.as_mut() {
            Some(candle) if candle.slot == trade.record.slot => candle.push(index, trades)?,
            Some(_) => {
                ensure!(
                    output.len() < max_points,
                    "slot OHLCV response exceeds max_points; use a smaller explicit slot range"
                );
                output.push(current.take().expect("present slot candle"));
                current = Some(SlotCandleCore::new(index, trade));
            }
            None => current = Some(SlotCandleCore::new(index, trade)),
        }
    }
    if let Some(candle) = current {
        ensure!(
            output.len() < max_points,
            "slot OHLCV response exceeds max_points; use a smaller explicit slot range"
        );
        output.push(candle);
    }
    Ok(output)
}

fn newest_non_empty_slot_start(
    trades: &[StoredTrade],
    indexes: &[usize],
    end: usize,
    max_points: usize,
) -> usize {
    let mut start = end;
    let mut newest_seen = None;
    let mut slots = 0usize;
    while start != 0 {
        let candidate = start - 1;
        let slot = trades[indexes[candidate]].record.slot;
        if newest_seen != Some(slot) {
            if slots == max_points {
                break;
            }
            newest_seen = Some(slot);
            slots += 1;
        }
        start = candidate;
    }
    start
}

#[derive(Debug, Clone, Default)]
struct ProgramVolumeCore {
    trade_count: u64,
    buy_count: u64,
    sell_count: u64,
    target_volume_raw: u128,
    routed_trade_count: u64,
    routed_target_volume_raw: u128,
    router_ids: BTreeSet<u32>,
}

impl ProgramVolumeCore {
    fn push(&mut self, trade: StoredTrade) -> Result<()> {
        self.trade_count = self
            .trade_count
            .checked_add(1)
            .context("program volume trade count overflow")?;
        match trade.side {
            MarketSide::Buy => {
                self.buy_count = self
                    .buy_count
                    .checked_add(1)
                    .context("program volume buy count overflow")?;
            }
            MarketSide::Sell => {
                self.sell_count = self
                    .sell_count
                    .checked_add(1)
                    .context("program volume sell count overflow")?;
            }
        }
        let target_amount = u128::from(trade.target_amount_raw);
        self.target_volume_raw = self
            .target_volume_raw
            .checked_add(target_amount)
            .context("program target volume overflow")?;
        if trade.record.router_program_id != 0 {
            self.routed_trade_count = self
                .routed_trade_count
                .checked_add(1)
                .context("routed program volume trade count overflow")?;
            self.routed_target_volume_raw = self
                .routed_target_volume_raw
                .checked_add(target_amount)
                .context("routed program target volume overflow")?;
            self.router_ids.insert(trade.record.router_program_id);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ProgramVolumePointCore {
    start_time: i64,
    trade_count: u64,
    target_volume_raw: u128,
    programs: BTreeMap<u32, ProgramVolumeCore>,
}

impl ProgramVolumePointCore {
    fn new(start_time: i64) -> Self {
        Self {
            start_time,
            trade_count: 0,
            target_volume_raw: 0,
            programs: BTreeMap::new(),
        }
    }

    fn push(&mut self, trade: StoredTrade) -> Result<()> {
        self.trade_count = self
            .trade_count
            .checked_add(1)
            .context("program volume bucket trade count overflow")?;
        self.target_volume_raw = self
            .target_volume_raw
            .checked_add(u128::from(trade.target_amount_raw))
            .context("program volume bucket target volume overflow")?;
        self.programs
            .entry(trade.record.dex_program_id)
            .or_default()
            .push(trade)
    }
}

fn build_program_volume_cores(
    trades: &[StoredTrade],
    indexes: &[usize],
    interval: i64,
    time_from: i64,
    time_to: i64,
    max_points: usize,
) -> Result<Vec<ProgramVolumePointCore>> {
    ensure!(
        interval > 0 && max_points != 0,
        "program volume interval or max_points is not positive"
    );
    validate_time_range(Some(time_from), Some(time_to))?;
    ensure!(
        inclusive_time_bucket_count(time_from, time_to, interval)? <= max_points as i128,
        "program volume time window exceeds max_points"
    );
    let mut output = Vec::new();
    let mut current: Option<ProgramVolumePointCore> = None;
    let start = indexes.partition_point(|index| trades[*index].record.block_time < time_from);
    let end = indexes.partition_point(|index| trades[*index].record.block_time <= time_to);
    for index in indexes[start..end].iter().copied() {
        let trade = *trades
            .get(index)
            .context("program volume index exceeds the loaded trade rows")?;
        let start_time = trade
            .record
            .block_time
            .div_euclid(interval)
            .checked_mul(interval)
            .context("program volume bucket start overflow")?;
        match current.as_mut() {
            Some(point) if point.start_time == start_time => point.push(trade)?,
            Some(_) => {
                output.push(current.take().expect("present program volume point"));
                let mut point = ProgramVolumePointCore::new(start_time);
                point.push(trade)?;
                current = Some(point);
            }
            None => {
                let mut point = ProgramVolumePointCore::new(start_time);
                point.push(trade)?;
                current = Some(point);
            }
        }
    }
    if let Some(point) = current {
        output.push(point);
    }
    Ok(output)
}

fn compare_prices(left: StoredTrade, right: StoredTrade) -> Result<Ordering> {
    ensure!(
        left.quote_mint_id == right.quote_mint_id
            && left.quote_decimals == right.quote_decimals
            && left.target_decimals == right.target_decimals,
        "cannot compare prices from different pairs or decimal domains"
    );
    let left_cross = u128::from(left.quote_amount_raw)
        .checked_mul(u128::from(right.target_amount_scaled_ui_raw))
        .context("left price cross product overflow")?;
    let right_cross = u128::from(right.quote_amount_raw)
        .checked_mul(u128::from(left.target_amount_scaled_ui_raw))
        .context("right price cross product overflow")?;
    Ok(left_cross.cmp(&right_cross))
}

fn exact_price(trade: StoredTrade) -> Result<ExactPrice> {
    ensure!(
        trade.target_amount_scaled_ui_raw != 0 && trade.quote_amount_raw != 0,
        "cannot derive a price from a zero amount"
    );
    let common = gcd_u64(trade.quote_amount_raw, trade.target_amount_scaled_ui_raw);
    let mut quote = trade.quote_amount_raw / common;
    let mut target = trade.target_amount_scaled_ui_raw / common;
    let decimal_difference = i16::from(trade.target_decimals) - i16::from(trade.quote_decimals);
    let (numerator, denominator) = if decimal_difference >= 0 {
        let mut powers_of_two = decimal_difference as usize;
        let mut powers_of_five = decimal_difference as usize;
        cancel_small_factor(&mut target, 2, &mut powers_of_two);
        cancel_small_factor(&mut target, 5, &mut powers_of_five);
        let mut numerator = quote.to_string();
        multiply_decimal_string_small(&mut numerator, 2, powers_of_two)?;
        multiply_decimal_string_small(&mut numerator, 5, powers_of_five)?;
        (numerator, target.to_string())
    } else {
        let exponent = usize::from(decimal_difference.unsigned_abs());
        let mut powers_of_two = exponent;
        let mut powers_of_five = exponent;
        cancel_small_factor(&mut quote, 2, &mut powers_of_two);
        cancel_small_factor(&mut quote, 5, &mut powers_of_five);
        let mut denominator = target.to_string();
        multiply_decimal_string_small(&mut denominator, 2, powers_of_two)?;
        multiply_decimal_string_small(&mut denominator, 5, powers_of_five)?;
        (quote.to_string(), denominator)
    };
    Ok(ExactPrice {
        numerator,
        denominator,
        decimal: normalized_price_decimal_string(
            trade.target_amount_scaled_ui_raw,
            trade.quote_amount_raw,
            trade.target_decimals,
            trade.quote_decimals,
            PRICE_DECIMAL_DIGITS,
        )?,
        chart_display: chart_price(trade),
        target_multiplier: f64::from_bits(trade.target_multiplier_bits).to_string(),
        target_multiplier_bits: format!("{:016x}", trade.target_multiplier_bits),
        scaled_ui_config_id: trade.record.scaled_ui_config_id,
        unscaled_decimal: normalized_price_decimal_string(
            trade.target_amount_raw,
            trade.quote_amount_raw,
            trade.target_decimals,
            trade.quote_decimals,
            PRICE_DECIMAL_DIGITS,
        )?,
        unscaled_chart_display: unscaled_chart_price(trade),
        target_amount_raw: trade.target_amount_scaled_ui_raw,
        quote_amount_raw: trade.quote_amount_raw,
        target_decimals: trade.target_decimals,
        quote_decimals: trade.quote_decimals,
    })
}

/// Produce a base-10 display for quote-token units per target token.
///
/// The result is truncated, not rounded. The exact rational values remain the
/// authoritative representation.
pub fn normalized_price_decimal_string(
    target_amount_raw: u64,
    quote_amount_raw: u64,
    target_decimals: u8,
    quote_decimals: u8,
    fractional_digits: u8,
) -> Result<String> {
    ensure!(
        target_amount_raw != 0 && quote_amount_raw != 0,
        "normalized price amount is zero"
    );
    shifted_ratio_decimal_string(
        quote_amount_raw,
        target_amount_raw,
        i16::from(target_decimals) - i16::from(quote_decimals),
        fractional_digits,
    )
}

fn chart_price(trade: StoredTrade) -> Option<f64> {
    let exponent = i32::from(trade.target_decimals) - i32::from(trade.quote_decimals);
    let value = (trade.quote_amount_raw as f64 / trade.target_amount_scaled_ui_raw as f64)
        * 10f64.powi(exponent);
    value.is_finite().then_some(value)
}

fn unscaled_chart_price(trade: StoredTrade) -> Option<f64> {
    let exponent = i32::from(trade.target_decimals) - i32::from(trade.quote_decimals);
    let value =
        (trade.quote_amount_raw as f64 / trade.target_amount_raw as f64) * 10f64.powi(exponent);
    value.is_finite().then_some(value)
}

fn shifted_ratio_decimal_string(
    numerator: u64,
    denominator: u64,
    decimal_shift: i16,
    fractional_digits: u8,
) -> Result<String> {
    ensure!(denominator != 0, "decimal ratio denominator is zero");
    let integer = numerator / denominator;
    let mut remainder = numerator % denominator;
    let extra = usize::from(decimal_shift.unsigned_abs());
    let desired_fraction = usize::from(fractional_digits);
    let generated = desired_fraction
        .checked_add(extra)
        .and_then(|value| value.checked_add(1))
        .context("decimal display digit count overflow")?;
    let mut digits = integer.to_string();
    let original_decimal = i32::try_from(digits.len()).context("decimal position exceeds i32")?;
    digits
        .try_reserve(generated)
        .context("reserve decimal display digits")?;
    for _ in 0..generated {
        let expanded = u128::from(remainder)
            .checked_mul(10)
            .context("decimal remainder overflow")?;
        let digit = expanded / u128::from(denominator);
        remainder = u64::try_from(expanded % u128::from(denominator))
            .expect("remainder remains below u64 denominator");
        digits.push(char::from(
            b'0' + u8::try_from(digit).expect("decimal digit is at most 9"),
        ));
    }
    let shifted_decimal = original_decimal + i32::from(decimal_shift);
    let (mut whole, mut fraction) = if shifted_decimal <= 0 {
        let zeros = usize::try_from(-shifted_decimal).context("decimal prefix exceeds usize")?;
        let mut fraction = String::new();
        fraction
            .try_reserve(zeros.saturating_add(digits.len()))
            .context("reserve shifted decimal fraction")?;
        fraction.extend(std::iter::repeat_n('0', zeros));
        fraction.push_str(&digits);
        ("0".to_owned(), fraction)
    } else {
        let position =
            usize::try_from(shifted_decimal).context("decimal position exceeds usize")?;
        if position >= digits.len() {
            let mut whole = digits;
            whole.extend(std::iter::repeat_n('0', position - whole.len()));
            (whole, String::new())
        } else {
            (digits[..position].to_owned(), digits[position..].to_owned())
        }
    };
    whole = whole.trim_start_matches('0').to_owned();
    if whole.is_empty() {
        whole.push('0');
    }
    fraction.truncate(desired_fraction);
    while fraction.ends_with('0') {
        fraction.pop();
    }
    if fraction.is_empty() {
        Ok(whole)
    } else {
        Ok(format!("{whole}.{fraction}"))
    }
}

fn cancel_small_factor(value: &mut u64, factor: u64, exponent: &mut usize) {
    while *exponent != 0 && (*value).is_multiple_of(factor) {
        *value /= factor;
        *exponent -= 1;
    }
}

fn multiply_decimal_string_small(value: &mut String, factor: u8, exponent: usize) -> Result<()> {
    debug_assert!(matches!(factor, 2 | 5));
    let mut digits = std::mem::take(value).into_bytes();
    for _ in 0..exponent {
        let mut carry = 0u8;
        for digit in digits.iter_mut().rev() {
            ensure!(
                digit.is_ascii_digit(),
                "decimal integer contains a non-digit"
            );
            let product = (*digit - b'0')
                .checked_mul(factor)
                .and_then(|product| product.checked_add(carry))
                .context("small decimal multiplication overflow")?;
            *digit = b'0' + product % 10;
            carry = product / 10;
        }
        if carry != 0 {
            digits.insert(0, b'0' + carry);
        }
    }
    *value = String::from_utf8(digits).expect("decimal multiplication preserves ASCII digits");
    Ok(())
}

fn gcd_u64(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn raw_amount_decimal_string(amount: u128, decimals: u8) -> String {
    let mut digits = amount.to_string();
    let decimals = usize::from(decimals);
    if decimals == 0 {
        return digits;
    }
    if digits.len() <= decimals {
        let mut prefixed = String::with_capacity(decimals + 2);
        prefixed.push_str("0.");
        prefixed.extend(std::iter::repeat_n('0', decimals - digits.len()));
        prefixed.push_str(&digits);
        digits = prefixed;
    } else {
        digits.insert(digits.len() - decimals, '.');
    }
    while digits.ends_with('0') {
        digits.pop();
    }
    if digits.ends_with('.') {
        digits.pop();
    }
    digits
}

fn decimal_string_to_f64(value: &str) -> Option<f64> {
    value.parse::<f64>().ok().filter(|value| value.is_finite())
}

fn trade_matches(trade: &StoredTrade, query: &MarketTradeQuery) -> bool {
    query
        .quote_mint_id
        .is_none_or(|quote| trade.quote_mint_id == quote)
        && query
            .venue_program_id
            .is_none_or(|venue| trade.record.dex_program_id == venue)
        && time_in_range(trade.record.block_time, query.time_from, query.time_to)
}

fn validate_trade_query(query: MarketTradeQuery) -> Result<()> {
    validate_time_range(query.time_from, query.time_to)?;
    ensure!(
        query.limit != 0 && query.limit <= MAX_MARKET_TRADE_PAGE_ROWS,
        "market trade page limit must be between 1 and {MAX_MARKET_TRADE_PAGE_ROWS}"
    );
    if let Some(id) = query.quote_mint_id {
        ensure!(id != 0, "market trade quote mint ID is zero");
    }
    if let Some(id) = query.venue_program_id {
        ensure!(id != 0, "market trade venue program ID is zero");
    }
    Ok(())
}

fn validate_time_range(time_from: Option<i64>, time_to: Option<i64>) -> Result<()> {
    ensure!(
        !matches!((time_from, time_to), (Some(from), Some(to)) if from > to),
        "market time range starts after it ends"
    );
    Ok(())
}

fn inclusive_time_bucket_count(time_from: i64, time_to: i64, interval: i64) -> Result<i128> {
    ensure!(interval > 0, "market interval is not positive");
    validate_time_range(Some(time_from), Some(time_to))?;
    Ok(i128::from(time_to.div_euclid(interval)) - i128::from(time_from.div_euclid(interval)) + 1)
}

fn validate_slot_range(slot_from: Option<u64>, slot_to: Option<u64>) -> Result<()> {
    ensure!(
        !matches!((slot_from, slot_to), (Some(from), Some(to)) if from > to),
        "market slot range starts after it ends"
    );
    Ok(())
}

fn time_in_range(value: i64, from: Option<i64>, to: Option<i64>) -> bool {
    from.is_none_or(|from| value >= from) && to.is_none_or(|to| value <= to)
}

fn evidence_names(flags: u16) -> Vec<&'static str> {
    let mut values = Vec::new();
    for (flag, name) in [
        (MARKET_TRADE_FLAG_TARGET_INPUT, "target_input"),
        (MARKET_TRADE_FLAG_TARGET_OUTPUT, "target_output"),
        (MARKET_TRADE_FLAG_INNER, "inner"),
        (MARKET_TRADE_FLAG_STACK_PROVEN, "stack_proven"),
        (MARKET_TRADE_FLAG_USER_SOURCE_MATCH, "user_source_match"),
        (
            MARKET_TRADE_FLAG_USER_DESTINATION_MATCH,
            "user_destination_match",
        ),
        (MARKET_TRADE_FLAG_INPUT_VAULT_MATCH, "input_vault_match"),
        (MARKET_TRADE_FLAG_OUTPUT_VAULT_MATCH, "output_vault_match"),
        (MARKET_TRADE_FLAG_BALANCE_RECONCILED, "balance_reconciled"),
        (MARKET_TRADE_FLAG_COMMIT_PROVEN, "commit_proven"),
        (MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED, "router_attributed"),
        (MARKET_TRADE_FLAG_DIRECT_USD_QUOTE, "direct_usd_quote"),
        (MARKET_TRADE_FLAG_FEE_KNOWN, "fee_known"),
    ] {
        if flags & flag != 0 {
            values.push(name);
        }
    }
    values
}

#[cfg(unix)]
fn positioned_read_exact(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> Result<()> {
    use std::os::unix::fs::FileExt;

    file.read_exact_at(bytes, offset)?;
    Ok(())
}

#[cfg(windows)]
fn positioned_read_exact(file: &std::fs::File, bytes: &mut [u8], offset: u64) -> Result<()> {
    use std::os::windows::fs::FileExt;

    let mut read = 0usize;
    while read < bytes.len() {
        let current = offset
            .checked_add(u64::try_from(read)?)
            .context("positioned market read offset overflow")?;
        let count = file.seek_read(&mut bytes[read..], current)?;
        ensure!(count != 0, "positioned market read reached end of file");
        read += count;
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn positioned_read_exact(_file: &std::fs::File, _bytes: &mut [u8], _offset: u64) -> Result<()> {
    anyhow::bail!("positioned file reads are not supported on this platform")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_format::{
        MARKET_TRADE_FLAG_BALANCE_RECONCILED, MARKET_TRADE_FLAG_COMMIT_PROVEN,
        MARKET_TRADE_FLAG_INNER, MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED,
        MARKET_TRADE_FLAG_STACK_PROVEN, MARKET_TRADE_FLAG_TARGET_INPUT,
        MARKET_TRADE_FLAG_TARGET_OUTPUT,
    };

    fn sell(price_quote_raw: u64, time: i64) -> StoredTrade {
        StoredTrade::from_record(
            MarketTradeRecord {
                transaction_id: u64::try_from(time).unwrap(),
                slot: u64::try_from(time).unwrap(),
                block_time: time,
                source_epoch: 1,
                source_block_id: 1,
                tx_index: 1,
                outer_index: 1,
                inner_index: MARKET_OUTER_INNER_INDEX,
                stack_height: 1,
                instruction_kind_id: 1,
                dex_program_id: 2,
                router_program_id: 0,
                pool_id: 3,
                trader_id: 4,
                input_mint_id: 20,
                output_mint_id: 30,
                user_source_id: 5,
                user_destination_id: 6,
                amount_in: 1_000_000_000,
                amount_out: price_quote_raw,
                fee_amount: 0,
                fee_mint_id: 0,
                flags: MARKET_TRADE_FLAG_TARGET_INPUT
                    | MARKET_TRADE_FLAG_STACK_PROVEN
                    | MARKET_TRADE_FLAG_COMMIT_PROVEN
                    | MARKET_TRADE_FLAG_BALANCE_RECONCILED,
                input_decimals: 9,
                output_decimals: 6,
                input_transfer_count: 1,
                output_transfer_count: 1,
                scaled_ui_config_id: 0,
            },
            20,
        )
        .unwrap()
    }

    fn buy(price_quote_raw: u64, time: i64) -> StoredTrade {
        let mut record = sell(price_quote_raw, time).record;
        record.input_mint_id = 30;
        record.output_mint_id = 20;
        record.amount_in = price_quote_raw;
        record.amount_out = 1_000_000_000;
        record.input_decimals = 6;
        record.output_decimals = 9;
        record.flags &= !MARKET_TRADE_FLAG_TARGET_INPUT;
        record.flags |= MARKET_TRADE_FLAG_TARGET_OUTPUT;
        StoredTrade::from_record(record, 20).unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn configured_sell(
        target_amount_raw: u64,
        quote_amount_raw: u64,
        time: i64,
        slot: u64,
        transaction_id: u64,
        outer_index: u32,
        inner_index: u32,
        quote_mint_id: u32,
        dex_program_id: u32,
        router_program_id: u32,
    ) -> StoredTrade {
        let mut record = sell(quote_amount_raw, time).record;
        record.transaction_id = transaction_id;
        record.slot = slot;
        record.outer_index = outer_index;
        record.inner_index = inner_index;
        record.output_mint_id = quote_mint_id;
        record.amount_in = target_amount_raw;
        record.amount_out = quote_amount_raw;
        record.dex_program_id = dex_program_id;
        record.router_program_id = router_program_id;
        if inner_index == MARKET_OUTER_INNER_INDEX {
            record.flags &= !MARKET_TRADE_FLAG_INNER;
        } else {
            record.flags |= MARKET_TRADE_FLAG_INNER;
        }
        if router_program_id == 0 {
            record.flags &= !MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED;
        } else {
            record.flags |= MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED;
        }
        StoredTrade::from_record(record, 20).unwrap()
    }

    fn with_trader(trade: StoredTrade, trader_id: u32) -> StoredTrade {
        let mut record = trade.record;
        record.trader_id = trader_id;
        StoredTrade::from_record(record, 20).unwrap()
    }

    fn configured_buy(
        target_amount_raw: u64,
        quote_amount_raw: u64,
        time: i64,
        slot: u64,
        transaction_id: u64,
        quote_mint_id: u32,
        dex_program_id: u32,
        trader_id: u32,
    ) -> StoredTrade {
        let sell = configured_sell(
            target_amount_raw,
            quote_amount_raw,
            time,
            slot,
            transaction_id,
            0,
            MARKET_OUTER_INNER_INDEX,
            quote_mint_id,
            dex_program_id,
            0,
        );
        let mut record = sell.record;
        record.input_mint_id = quote_mint_id;
        record.output_mint_id = 20;
        record.amount_in = quote_amount_raw;
        record.amount_out = target_amount_raw;
        record.input_decimals = 6;
        record.output_decimals = 9;
        record.trader_id = trader_id;
        record.flags &= !MARKET_TRADE_FLAG_TARGET_INPUT;
        record.flags |= MARKET_TRADE_FLAG_TARGET_OUTPUT;
        StoredTrade::from_record(record, 20).unwrap()
    }

    #[test]
    fn rational_price_normalizes_buy_and_sell_to_spyx_base() {
        let sell = sell(2_000_000, 1);
        let buy = buy(2_000_000, 2);
        assert_eq!(sell.side, MarketSide::Sell);
        assert_eq!(buy.side, MarketSide::Buy);
        assert_eq!(sell.target_amount_raw, buy.target_amount_raw);
        assert_eq!(sell.quote_amount_raw, buy.quote_amount_raw);
        assert_eq!(compare_prices(sell, buy).unwrap(), Ordering::Equal);
        let price = exact_price(sell).unwrap();
        assert_eq!(price.numerator, "2");
        assert_eq!(price.denominator, "1");
        assert_eq!(price.decimal, "2");
    }

    #[test]
    fn exact_price_uses_the_bound_scaled_ui_multiplier() {
        let raw = sell(2_000_000, 1);
        let mut record = raw.record;
        record.scaled_ui_config_id = 1;
        let multiplier = ScaledUiAmountMultiplier::from_f64(2.0).unwrap();
        let scaled = StoredTrade::from_record_with_multiplier(record, 20, &multiplier).unwrap();
        assert_eq!(scaled.target_amount_raw, 1_000_000_000);
        assert_eq!(scaled.target_amount_scaled_ui_raw, 2_000_000_000);
        let price = exact_price(scaled).unwrap();
        assert_eq!(price.decimal, "1");
        assert_eq!(price.unscaled_decimal, "2");
        assert_eq!(price.target_multiplier, "2");
        assert_eq!(price.target_multiplier_bits, "4000000000000000");
        assert_eq!(price.scaled_ui_config_id, 1);
    }

    #[test]
    fn public_evidence_names_include_commit_proof() {
        assert_eq!(
            evidence_names(MARKET_TRADE_FLAG_COMMIT_PROVEN),
            vec!["commit_proven"]
        );
    }

    #[test]
    fn candle_uses_exact_price_order_and_sums_raw_volumes() {
        let trades = vec![
            sell(2_000_000, 60),
            buy(4_000_000, 61),
            sell(1_000_000, 62),
            buy(3_000_000, 63),
        ];
        let cores = build_candle_cores(&trades, &[0, 1, 2, 3], 60, None, None, 10).unwrap();
        assert_eq!(cores.len(), 1);
        let candle = cores[0];
        assert_eq!(
            (candle.open, candle.high, candle.low, candle.close),
            (0, 1, 2, 3)
        );
        assert_eq!(candle.trade_count, 4);
        assert_eq!(candle.buy_count, 2);
        assert_eq!(candle.sell_count, 2);
        assert_eq!(candle.target_volume_raw, 4_000_000_000);
        assert_eq!(candle.quote_volume_raw, 10_000_000);
    }

    #[test]
    fn slot_candles_use_canonical_instruction_order_and_newest_non_empty_slots() {
        let trades = vec![
            configured_sell(1_000, 4, 50, 100, 10, 3, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
            configured_sell(1_000, 3, 50, 100, 11, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
            configured_sell(1_000, 2, 50, 100, 10, 2, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
            configured_sell(1_000, 1, 50, 100, 10, 2, 0, 30, 2, 0),
            configured_sell(1_000, 5, 51, 101, 12, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
            configured_sell(1_000, 6, 52, 103, 13, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
        ];
        let mut indexes = (0..trades.len()).collect::<Vec<_>>();
        indexes.sort_unstable_by_key(|index| trades[*index].slot_key());

        let exact = build_slot_candle_cores(&trades, &indexes, Some(100), Some(100), 1).unwrap();
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].slot, 100);
        assert_eq!(exact[0].open, 2);
        assert_eq!(exact[0].close, 1);
        assert_eq!(exact[0].high, 0);
        assert_eq!(exact[0].low, 3);
        assert_eq!(exact[0].trade_count, 4);

        let newest = build_slot_candle_cores(&trades, &indexes, None, None, 2).unwrap();
        assert_eq!(
            newest.iter().map(|point| point.slot).collect::<Vec<_>>(),
            [101, 103]
        );
        let capped = build_slot_candle_cores(&trades, &indexes, None, Some(101), 2).unwrap();
        assert_eq!(
            capped.iter().map(|point| point.slot).collect::<Vec<_>>(),
            [100, 101]
        );
        assert!(
            build_slot_candle_cores(&trades, &indexes, Some(100), None, 2).is_err(),
            "an explicit lower bound must not silently truncate"
        );
    }

    #[test]
    fn program_volume_adds_target_units_across_pairs_without_promoting_routers() {
        let trades = vec![
            configured_sell(100, 10, 60, 1_000, 1, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
            configured_sell(200, 20, 61, 1_001, 2, 0, MARKET_OUTER_INNER_INDEX, 31, 2, 0),
            configured_sell(
                300,
                30,
                62,
                1_002,
                3,
                0,
                MARKET_OUTER_INNER_INDEX,
                30,
                3,
                90,
            ),
            configured_sell(
                400,
                40,
                120,
                1_003,
                4,
                0,
                MARKET_OUTER_INNER_INDEX,
                30,
                2,
                0,
            ),
        ];
        let indexes = [0, 1, 2, 3];
        let points = build_program_volume_cores(&trades, &indexes, 60, 60, 179, 2).unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].start_time, 60);
        assert_eq!(points[0].trade_count, 3);
        assert_eq!(points[0].target_volume_raw, 600);
        assert_eq!(points[0].programs[&2].target_volume_raw, 300);
        assert_eq!(points[0].programs[&3].target_volume_raw, 300);
        assert_eq!(points[0].programs[&3].routed_trade_count, 1);
        assert_eq!(points[0].programs[&3].routed_target_volume_raw, 300);
        assert_eq!(points[0].programs[&3].router_ids, BTreeSet::from([90]));
        assert!(!points[0].programs.contains_key(&90));

        let quote_30 = [0, 2, 3];
        let selected = build_program_volume_cores(&trades, &quote_30, 60, 60, 179, 2).unwrap();
        assert_eq!(selected[0].target_volume_raw, 400);
        assert_eq!(selected[0].programs[&2].target_volume_raw, 100);
    }

    #[test]
    fn trade_filters_apply_quote_venue_and_inclusive_time_range() {
        let trade = sell(2_000_000, 100);
        assert!(trade_matches(
            &trade,
            &MarketTradeQuery {
                quote_mint_id: Some(30),
                venue_program_id: Some(2),
                time_from: Some(100),
                time_to: Some(100),
                offset: 0,
                limit: 10,
            }
        ));
        assert!(!trade_matches(
            &trade,
            &MarketTradeQuery {
                quote_mint_id: Some(31),
                ..Default::default()
            }
        ));
    }

    #[test]
    fn exact_trader_index_excludes_unknown_and_sorts_by_time_key() {
        let trades = vec![
            with_trader(
                configured_sell(10, 1, 120, 3, 3, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
                7,
            ),
            with_trader(
                configured_sell(20, 2, 60, 1, 1, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
                7,
            ),
            with_trader(
                configured_sell(30, 3, 90, 2, 2, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
                0,
            ),
            with_trader(
                configured_sell(40, 4, 60, 1, 4, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
                8,
            ),
        ];
        let mut indexes = BTreeMap::new();
        for (index, trade) in trades.iter().copied().enumerate() {
            index_exact_trader(&mut indexes, index, trade);
        }
        for rows in indexes.values_mut() {
            rows.sort_unstable_by_key(|index| trades[*index].time_key());
        }

        assert_eq!(indexes.keys().copied().collect::<Vec<_>>(), [7, 8]);
        assert_eq!(indexes[&7], [1, 0]);
        assert_eq!(indexes[&8], [3]);
        assert!(!indexes.contains_key(&0));
    }

    #[test]
    fn trader_totals_keep_buy_sell_and_each_quote_exact() {
        let trades = vec![
            configured_buy(100, 11, 60, 1, 1, 30, 2, 7),
            with_trader(
                configured_sell(40, 5, 61, 2, 2, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
                7,
            ),
            configured_buy(200, 17, 62, 3, 3, 31, 3, 7),
        ];
        let totals = trader_activity_totals(trades).unwrap();

        assert_eq!(totals.trade_count, 3);
        assert_eq!(totals.buy_count, 2);
        assert_eq!(totals.sell_count, 1);
        assert_eq!(totals.target_bought_raw, 300);
        assert_eq!(totals.target_sold_raw, 40);
        assert_eq!(
            totals.quote_totals.keys().copied().collect::<Vec<_>>(),
            [30, 31]
        );
        assert_eq!(
            totals.quote_totals[&30],
            TraderQuoteActivityCore {
                trade_count: 2,
                buy_count: 1,
                sell_count: 1,
                target_bought_raw: 100,
                target_sold_raw: 40,
                quote_spent_on_buys_raw: 11,
                quote_received_from_sells_raw: 5,
            }
        );
        assert_eq!(totals.quote_totals[&31].quote_spent_on_buys_raw, 17);
        assert_eq!(totals.quote_totals[&31].quote_received_from_sells_raw, 0);
    }

    #[test]
    fn trader_series_filters_only_proven_rows_and_empty_is_supported_core() {
        let trades = vec![
            configured_buy(100, 11, 60, 1, 1, 30, 2, 7),
            with_trader(
                configured_sell(40, 5, 61, 2, 2, 0, MARKET_OUTER_INNER_INDEX, 30, 2, 0),
                7,
            ),
            configured_buy(200, 17, 121, 3, 3, 31, 3, 7),
            configured_buy(999, 99, 122, 4, 4, 30, 2, 8),
        ];
        let indexes = [0, 1, 2];
        let series = build_trader_activity_series_core(
            &trades,
            &indexes,
            7,
            60,
            60,
            179,
            Some(30),
            Some(2),
            2,
        )
        .unwrap();
        assert_eq!(series.points.len(), 1);
        assert_eq!(series.points[0].start_time, 60);
        assert_eq!(series.totals.trade_count, 2);
        assert_eq!(series.totals.target_bought_raw, 100);
        assert_eq!(series.totals.target_sold_raw, 40);

        let empty =
            build_trader_activity_series_core(&trades, &[], 7, 60, 60, 179, None, None, 2).unwrap();
        assert!(empty.points.is_empty());
        assert_eq!(empty.totals, TraderActivityTotalsCore::default());

        assert!(
            build_trader_activity_series_core(&trades, &[3], 7, 60, 60, 179, None, None, 2,)
                .is_err(),
            "an index row for another trader must be rejected"
        );
    }

    #[test]
    fn corrupted_header_and_trade_row_are_rejected() {
        let header = MarketFileHeader::new(true, 1, [1; 32], [2; 32]).encode();
        let mut corrupt_header = header;
        corrupt_header[0] ^= 1;
        assert!(MarketFileHeader::decode(&corrupt_header).is_err());

        let mut record = sell(2_000_000, 1).record;
        record.amount_out = 0;
        assert!(record.validate().is_err());
    }

    #[test]
    fn source_binding_rejects_one_changed_digest_or_count() {
        let digest = "01".repeat(32);
        let snapshot = SourceSnapshot {
            manifest_bytes: 1,
            manifest_sha256: digest.clone(),
            transaction_bytes: 2,
            transaction_sha256: digest.clone(),
            signature_bytes: 3,
            signature_sha256: digest.clone(),
            registry_bytes: 4,
            registry_sha256: digest.clone(),
            accounts_bytes: 5,
            accounts_sha256: digest.clone(),
            first_epoch: 1,
            last_epoch: 2,
            transactions: 10,
            signatures: 11,
            pubkeys: 12,
            accounts: 6,
        };
        let mut binding = MarketSourceBinding {
            manifest_file: "manifest.json".into(),
            manifest_bytes: 1,
            manifest_sha256: digest.clone(),
            transaction_file: "transactions.wincode".into(),
            transaction_bytes: 2,
            transaction_sha256: digest.clone(),
            signature_file: "signatures.bin".into(),
            signature_bytes: 3,
            signature_sha256: digest.clone(),
            registry_file: "registry.bin".into(),
            registry_bytes: 4,
            registry_sha256: digest.clone(),
            accounts_file: "accounts.bin".into(),
            accounts_bytes: 5,
            accounts_sha256: digest,
            first_epoch: 1,
            last_epoch: 2,
            transactions: 10,
            signatures: 11,
            pubkeys: 12,
            accounts: 6,
        };
        validate_source_binding(&binding, &snapshot).unwrap();
        binding.transaction_sha256 = "02".repeat(32);
        assert!(validate_source_binding(&binding, &snapshot).is_err());
        binding.transaction_sha256 = snapshot.transaction_sha256.clone();
        binding.transactions += 1;
        assert!(validate_source_binding(&binding, &snapshot).is_err());
    }

    #[test]
    fn decimal_helpers_cover_large_decimal_differences() {
        assert_eq!(
            normalized_price_decimal_string(1_000_000_000, 2_000_000, 9, 6, 18).unwrap(),
            "2"
        );
        assert_eq!(
            normalized_price_decimal_string(1, 1, 0, 30, 18).unwrap(),
            "0"
        );
        assert_eq!(raw_amount_decimal_string(12_340_000, 6), "12.34");
    }
}
