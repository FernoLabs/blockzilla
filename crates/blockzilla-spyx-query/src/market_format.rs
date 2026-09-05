//! Immutable, provenance-bound SPYx Market DB V3 wire format.
//!
//! The trade file contains one 128-byte header followed by fixed 128-byte
//! records. Token amounts are raw integer amounts. The format does not persist
//! floating-point prices or a derived price representation. V3 binds the exact
//! Token-2022 Scaled UI Amount event history used to select the display
//! multiplier for each trade.

use anyhow::{Context, Result, bail, ensure};
use blockzilla_token_transaction_dump::{
    ACCOUNTS_FILE, DUMP_MANIFEST_FILE, PUBKEY_REGISTRY_FILE, SIGNATURES_FILE, TRANSACTIONS_FILE,
};
use serde::{Deserialize, Serialize};

use crate::scaled_ui_amount::{
    DEPLOYED_LEGACY_REPLAY_SEMANTICS, ScaledUiAmountEvent, ScaledUiAmountEventKind,
    parse_canonical_signature, validate_scaled_ui_amount_history,
};

pub const MARKET_SCHEMA_VERSION: u16 = 3;
pub const MARKET_MANIFEST_FILE: &str = "market-manifest-v3.json";
pub const MARKET_TRADES_FILE: &str = "market-trades-v3.bin";

pub const MARKET_HEADER_BYTES: usize = 128;
pub const MARKET_TRADE_RECORD_BYTES: usize = 128;
pub const MARKET_TRADES_MAGIC: [u8; 8] = *b"BZSMKT03";
pub const MARKET_HEADER_FLAG_COMPLETE: u16 = 1;

/// Exact historical processor behavior used for Scaled UI Amount replay.
pub const MARKET_SCALED_UI_PROCESSOR_SEMANTICS: &str = DEPLOYED_LEGACY_REPLAY_SEMANTICS;

pub const MARKET_TRADE_FLAG_TARGET_INPUT: u16 = 1 << 0;
pub const MARKET_TRADE_FLAG_TARGET_OUTPUT: u16 = 1 << 1;
pub const MARKET_TRADE_FLAG_INNER: u16 = 1 << 2;
pub const MARKET_TRADE_FLAG_STACK_PROVEN: u16 = 1 << 3;
pub const MARKET_TRADE_FLAG_USER_SOURCE_MATCH: u16 = 1 << 4;
pub const MARKET_TRADE_FLAG_USER_DESTINATION_MATCH: u16 = 1 << 5;
pub const MARKET_TRADE_FLAG_INPUT_VAULT_MATCH: u16 = 1 << 6;
pub const MARKET_TRADE_FLAG_OUTPUT_VAULT_MATCH: u16 = 1 << 7;
pub const MARKET_TRADE_FLAG_BALANCE_RECONCILED: u16 = 1 << 8;
pub const MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED: u16 = 1 << 9;
pub const MARKET_TRADE_FLAG_DIRECT_USD_QUOTE: u16 = 1 << 10;
pub const MARKET_TRADE_FLAG_FEE_KNOWN: u16 = 1 << 11;
pub const MARKET_TRADE_FLAG_COMMIT_PROVEN: u16 = 1 << 12;

pub const MARKET_TRADE_KNOWN_FLAGS: u16 = MARKET_TRADE_FLAG_TARGET_INPUT
    | MARKET_TRADE_FLAG_TARGET_OUTPUT
    | MARKET_TRADE_FLAG_INNER
    | MARKET_TRADE_FLAG_STACK_PROVEN
    | MARKET_TRADE_FLAG_USER_SOURCE_MATCH
    | MARKET_TRADE_FLAG_USER_DESTINATION_MATCH
    | MARKET_TRADE_FLAG_INPUT_VAULT_MATCH
    | MARKET_TRADE_FLAG_OUTPUT_VAULT_MATCH
    | MARKET_TRADE_FLAG_BALANCE_RECONCILED
    | MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED
    | MARKET_TRADE_FLAG_DIRECT_USD_QUOTE
    | MARKET_TRADE_FLAG_FEE_KNOWN
    | MARKET_TRADE_FLAG_COMMIT_PROVEN;

/// The `inner_index` value used for an outer instruction.
pub const MARKET_OUTER_INNER_INDEX: u32 = u32::MAX;

/// A provenance-bound header for `market-trades-v3.bin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketFileHeader {
    pub complete: bool,
    pub record_count: u64,
    pub source_manifest_sha256: [u8; 32],
    pub source_transaction_sha256: [u8; 32],
}

impl MarketFileHeader {
    pub const fn new(
        complete: bool,
        record_count: u64,
        source_manifest_sha256: [u8; 32],
        source_transaction_sha256: [u8; 32],
    ) -> Self {
        Self {
            complete,
            record_count,
            source_manifest_sha256,
            source_transaction_sha256,
        }
    }

    pub fn encode(self) -> [u8; MARKET_HEADER_BYTES] {
        let mut bytes = [0u8; MARKET_HEADER_BYTES];
        bytes[0..8].copy_from_slice(&MARKET_TRADES_MAGIC);
        bytes[8..10].copy_from_slice(&MARKET_SCHEMA_VERSION.to_le_bytes());
        bytes[10..12].copy_from_slice(&(MARKET_HEADER_BYTES as u16).to_le_bytes());
        bytes[12..14].copy_from_slice(&(MARKET_TRADE_RECORD_BYTES as u16).to_le_bytes());
        let flags = if self.complete {
            MARKET_HEADER_FLAG_COMPLETE
        } else {
            0
        };
        bytes[14..16].copy_from_slice(&flags.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.record_count.to_le_bytes());
        bytes[24..56].copy_from_slice(&self.source_manifest_sha256);
        bytes[56..88].copy_from_slice(&self.source_transaction_sha256);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= MARKET_HEADER_BYTES,
            "market trade file is shorter than its header"
        );
        let header = &bytes[..MARKET_HEADER_BYTES];
        ensure!(
            header[0..8] == MARKET_TRADES_MAGIC,
            "market trade file magic differs"
        );
        ensure!(
            read_u16(header, 8) == MARKET_SCHEMA_VERSION,
            "market schema version differs"
        );
        ensure!(
            usize::from(read_u16(header, 10)) == MARKET_HEADER_BYTES,
            "market header byte length differs"
        );
        ensure!(
            usize::from(read_u16(header, 12)) == MARKET_TRADE_RECORD_BYTES,
            "market trade record byte length differs"
        );
        let flags = read_u16(header, 14);
        ensure!(
            flags & !MARKET_HEADER_FLAG_COMPLETE == 0,
            "market header has unknown flags"
        );
        ensure!(
            header[88..MARKET_HEADER_BYTES]
                .iter()
                .all(|byte| *byte == 0),
            "market header has non-zero reserved bytes"
        );
        Ok(Self {
            complete: flags & MARKET_HEADER_FLAG_COMPLETE != 0,
            record_count: read_u64(header, 16),
            source_manifest_sha256: header[24..56]
                .try_into()
                .expect("fixed source manifest digest range"),
            source_transaction_sha256: header[56..88]
                .try_into()
                .expect("fixed source transaction digest range"),
        })
    }
}

/// One committed, flow-reconciled market trade.
///
/// All registry IDs are one-based. Zero is allowed only for an optional ID.
/// `amount_in`, `amount_out`, and `fee_amount` are raw token integer amounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketTradeRecord {
    pub transaction_id: u64,
    pub slot: u64,
    pub block_time: i64,
    pub source_epoch: u64,
    pub source_block_id: u32,
    pub tx_index: u32,
    pub outer_index: u32,
    pub inner_index: u32,
    pub stack_height: u32,
    pub instruction_kind_id: u32,
    pub dex_program_id: u32,
    pub router_program_id: u32,
    pub pool_id: u32,
    pub trader_id: u32,
    pub input_mint_id: u32,
    pub output_mint_id: u32,
    pub user_source_id: u32,
    pub user_destination_id: u32,
    pub amount_in: u64,
    pub amount_out: u64,
    pub fee_amount: u64,
    pub fee_mint_id: u32,
    pub flags: u16,
    pub input_decimals: u8,
    pub output_decimals: u8,
    pub input_transfer_count: u16,
    pub output_transfer_count: u16,
    /// One-based Scaled UI history configuration ID, or zero when disabled.
    pub scaled_ui_config_id: u32,
}

impl MarketTradeRecord {
    pub fn encode(self) -> Result<[u8; MARKET_TRADE_RECORD_BYTES]> {
        self.validate()?;
        let mut bytes = [0u8; MARKET_TRADE_RECORD_BYTES];
        bytes[0..8].copy_from_slice(&self.transaction_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.slot.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.block_time.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.source_epoch.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.source_block_id.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.tx_index.to_le_bytes());
        bytes[40..44].copy_from_slice(&self.outer_index.to_le_bytes());
        bytes[44..48].copy_from_slice(&self.inner_index.to_le_bytes());
        bytes[48..52].copy_from_slice(&self.stack_height.to_le_bytes());
        bytes[52..56].copy_from_slice(&self.instruction_kind_id.to_le_bytes());
        bytes[56..60].copy_from_slice(&self.dex_program_id.to_le_bytes());
        bytes[60..64].copy_from_slice(&self.router_program_id.to_le_bytes());
        bytes[64..68].copy_from_slice(&self.pool_id.to_le_bytes());
        bytes[68..72].copy_from_slice(&self.trader_id.to_le_bytes());
        bytes[72..76].copy_from_slice(&self.input_mint_id.to_le_bytes());
        bytes[76..80].copy_from_slice(&self.output_mint_id.to_le_bytes());
        bytes[80..84].copy_from_slice(&self.user_source_id.to_le_bytes());
        bytes[84..88].copy_from_slice(&self.user_destination_id.to_le_bytes());
        bytes[88..96].copy_from_slice(&self.amount_in.to_le_bytes());
        bytes[96..104].copy_from_slice(&self.amount_out.to_le_bytes());
        bytes[104..112].copy_from_slice(&self.fee_amount.to_le_bytes());
        bytes[112..116].copy_from_slice(&self.fee_mint_id.to_le_bytes());
        bytes[116..118].copy_from_slice(&self.flags.to_le_bytes());
        bytes[118] = self.input_decimals;
        bytes[119] = self.output_decimals;
        bytes[120..122].copy_from_slice(&self.input_transfer_count.to_le_bytes());
        bytes[122..124].copy_from_slice(&self.output_transfer_count.to_le_bytes());
        bytes[124..128].copy_from_slice(&self.scaled_ui_config_id.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() == MARKET_TRADE_RECORD_BYTES,
            "market trade row byte length differs"
        );
        let record = Self {
            transaction_id: read_u64(bytes, 0),
            slot: read_u64(bytes, 8),
            block_time: read_i64(bytes, 16),
            source_epoch: read_u64(bytes, 24),
            source_block_id: read_u32(bytes, 32),
            tx_index: read_u32(bytes, 36),
            outer_index: read_u32(bytes, 40),
            inner_index: read_u32(bytes, 44),
            stack_height: read_u32(bytes, 48),
            instruction_kind_id: read_u32(bytes, 52),
            dex_program_id: read_u32(bytes, 56),
            router_program_id: read_u32(bytes, 60),
            pool_id: read_u32(bytes, 64),
            trader_id: read_u32(bytes, 68),
            input_mint_id: read_u32(bytes, 72),
            output_mint_id: read_u32(bytes, 76),
            user_source_id: read_u32(bytes, 80),
            user_destination_id: read_u32(bytes, 84),
            amount_in: read_u64(bytes, 88),
            amount_out: read_u64(bytes, 96),
            fee_amount: read_u64(bytes, 104),
            fee_mint_id: read_u32(bytes, 112),
            flags: read_u16(bytes, 116),
            input_decimals: bytes[118],
            output_decimals: bytes[119],
            input_transfer_count: read_u16(bytes, 120),
            output_transfer_count: read_u16(bytes, 122),
            scaled_ui_config_id: read_u32(bytes, 124),
        };
        record.validate()?;
        Ok(record)
    }

    pub fn validate(self) -> Result<()> {
        ensure!(
            self.flags & !MARKET_TRADE_KNOWN_FLAGS == 0,
            "market trade row has unknown flags"
        );
        ensure!(
            self.instruction_kind_id != 0
                && self.dex_program_id != 0
                && self.input_mint_id != 0
                && self.output_mint_id != 0,
            "market trade row has a zero required registry or kind ID"
        );
        ensure!(
            self.input_mint_id != self.output_mint_id,
            "market trade input and output mints are equal"
        );
        ensure!(
            self.amount_in != 0 && self.amount_out != 0,
            "market trade row has a zero executed amount"
        );
        ensure!(
            self.input_transfer_count != 0 && self.output_transfer_count != 0,
            "market trade row has no attributed input or output transfer"
        );
        ensure!(
            self.has_flag(MARKET_TRADE_FLAG_COMMIT_PROVEN),
            "market trade row lacks committed-invocation proof"
        );

        let target_input = self.has_flag(MARKET_TRADE_FLAG_TARGET_INPUT);
        let target_output = self.has_flag(MARKET_TRADE_FLAG_TARGET_OUTPUT);
        ensure!(
            target_input ^ target_output,
            "market trade row must select exactly one target side"
        );

        ensure!(
            self.has_flag(MARKET_TRADE_FLAG_INNER) == !self.is_outer(),
            "market trade inner flag differs from the inner-index sentinel"
        );
        ensure!(
            self.has_flag(MARKET_TRADE_FLAG_STACK_PROVEN) == (self.stack_height != 0),
            "market trade stack proof flag differs from its stack height"
        );
        ensure!(
            !self.has_flag(MARKET_TRADE_FLAG_USER_SOURCE_MATCH) || self.user_source_id != 0,
            "market trade source-match flag has no user source"
        );
        ensure!(
            !self.has_flag(MARKET_TRADE_FLAG_USER_DESTINATION_MATCH)
                || self.user_destination_id != 0,
            "market trade destination-match flag has no user destination"
        );
        ensure!(
            self.has_flag(MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED) == (self.router_program_id != 0),
            "market trade router flag differs from its router program ID"
        );
        if self.has_flag(MARKET_TRADE_FLAG_FEE_KNOWN) {
            ensure!(
                self.fee_mint_id != 0,
                "market trade known fee has no fee mint"
            );
        } else {
            ensure!(
                self.fee_amount == 0 && self.fee_mint_id == 0,
                "market trade unknown fee has stored fee values"
            );
        }
        Ok(())
    }

    pub const fn is_outer(self) -> bool {
        self.inner_index == MARKET_OUTER_INNER_INDEX
    }

    pub const fn has_flag(self, flag: u16) -> bool {
        self.flags & flag != 0
    }

    /// Canonical source order with an outer instruction before its inner calls.
    pub fn order_key(self) -> (u64, u32, u64) {
        let instruction_order = if self.is_outer() {
            0
        } else {
            u64::from(self.inner_index) + 1
        };
        (self.transaction_id, self.outer_index, instruction_order)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketManifest {
    pub schema_version: u16,
    pub artifact_kind: String,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canary_max_transactions: Option<u64>,
    pub created_unix_seconds: u64,
    pub source: MarketSourceBinding,
    pub parser: MarketParserBinding,
    pub target: MarketTargetBinding,
    pub scaled_ui: MarketScaledUiHistory,
    pub usd_quote_mint_ids: Vec<u32>,
    pub instruction_kinds: Vec<MarketInstructionKind>,
    pub counters: MarketCounters,
    pub trades: MarketFileBinding,
    pub definitions: MarketDefinitions,
}

impl MarketManifest {
    pub const ARTIFACT_KIND: &'static str = "blockzilla_spyx_market_db_v3";

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == MARKET_SCHEMA_VERSION
                && self.artifact_kind == Self::ARTIFACT_KIND
                && self.created_unix_seconds != 0,
            "invalid market manifest header"
        );
        self.source.validate()?;
        self.parser.validate()?;
        self.target.validate()?;
        self.scaled_ui.validate(&self.source, &self.target)?;
        self.counters.validate()?;
        self.trades.validate()?;
        ensure!(
            self.definitions == MarketDefinitions::canonical(),
            "market manifest definitions differ from the V3 contract"
        );

        ensure!(
            u64::from(self.target.mint_id) <= self.source.pubkeys,
            "market target mint ID exceeds the source registry"
        );
        validate_registry_ids(
            &self.usd_quote_mint_ids,
            self.source.pubkeys,
            "USD quote mint",
        )?;
        ensure!(
            !self.usd_quote_mint_ids.contains(&self.target.mint_id),
            "market target mint is also listed as a USD quote mint"
        );
        validate_instruction_kinds(&self.instruction_kinds)?;

        ensure!(
            self.counters.source_transactions <= self.source.transactions,
            "market scan transaction count exceeds its source"
        );
        match (self.complete, self.canary_max_transactions) {
            (true, None) => ensure!(
                self.counters.source_transactions == self.source.transactions,
                "complete market artifact does not cover the exact source transaction count"
            ),
            (false, Some(maximum)) => ensure!(
                maximum != 0
                    && self.counters.source_transactions == maximum.min(self.source.transactions),
                "incomplete market artifact has an invalid canary transaction limit"
            ),
            _ => bail!("market artifact completion markers are inconsistent"),
        }
        ensure!(
            self.trades.records == self.counters.emitted_trades,
            "market trade binding count differs from the emitted-trade counter"
        );
        Ok(())
    }

    pub fn validate_header(&self, header: MarketFileHeader) -> Result<()> {
        self.validate()?;
        ensure!(
            header.complete == self.complete
                && header.record_count == self.trades.records
                && header.source_manifest_sha256
                    == parse_market_hex_digest(
                        &self.source.manifest_sha256,
                        "source manifest digest",
                    )?
                && header.source_transaction_sha256
                    == parse_market_hex_digest(
                        &self.source.transaction_sha256,
                        "source transaction digest",
                    )?,
            "market trade header differs from its manifest binding"
        );
        Ok(())
    }

    /// Applies manifest-level registry, target, quote, decimal, and source bounds.
    pub fn validate_trade(&self, trade: MarketTradeRecord) -> Result<()> {
        self.validate()?;
        trade.validate()?;
        ensure!(
            trade.transaction_id < self.source.transactions,
            "market trade transaction ID exceeds the source"
        );
        ensure!(
            (self.source.first_epoch..=self.source.last_epoch).contains(&trade.source_epoch),
            "market trade source epoch exceeds the source range"
        );
        ensure!(
            usize::try_from(trade.instruction_kind_id)
                .ok()
                .and_then(|id| id.checked_sub(1))
                .is_some_and(|index| index < self.instruction_kinds.len()),
            "market trade instruction kind ID is absent from its manifest"
        );
        for (id, label) in [
            (trade.dex_program_id, "DEX program"),
            (trade.router_program_id, "router program"),
            (trade.pool_id, "pool"),
            (trade.trader_id, "trader"),
            (trade.input_mint_id, "input mint"),
            (trade.output_mint_id, "output mint"),
            (trade.user_source_id, "user source"),
            (trade.user_destination_id, "user destination"),
            (trade.fee_mint_id, "fee mint"),
        ] {
            ensure!(
                id == 0 || u64::from(id) <= self.source.pubkeys,
                "market trade {label} ID exceeds the source registry"
            );
        }

        let target_input = trade.has_flag(MARKET_TRADE_FLAG_TARGET_INPUT);
        if target_input {
            ensure!(
                trade.input_mint_id == self.target.mint_id
                    && trade.input_decimals == self.target.decimals
                    && trade.output_mint_id != self.target.mint_id,
                "market trade target-input binding differs from its manifest"
            );
        } else {
            ensure!(
                trade.output_mint_id == self.target.mint_id
                    && trade.output_decimals == self.target.decimals
                    && trade.input_mint_id != self.target.mint_id,
                "market trade target-output binding differs from its manifest"
            );
        }
        let quote_mint_id = if target_input {
            trade.output_mint_id
        } else {
            trade.input_mint_id
        };
        let direct_usd_quote = self
            .usd_quote_mint_ids
            .binary_search(&quote_mint_id)
            .is_ok();
        ensure!(
            trade.has_flag(MARKET_TRADE_FLAG_DIRECT_USD_QUOTE) == direct_usd_quote,
            "market trade direct-USD flag differs from its quote mint"
        );
        if self.scaled_ui.enabled {
            ensure!(
                trade.scaled_ui_config_id != 0
                    && usize::try_from(trade.scaled_ui_config_id)
                        .is_ok_and(|id| id <= self.scaled_ui.events.len()),
                "market trade Scaled UI configuration ID is absent from its manifest"
            );
        } else {
            ensure!(
                trade.scaled_ui_config_id == 0,
                "market trade has a Scaled UI configuration ID while the extension is disabled"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSourceBinding {
    pub manifest_file: String,
    pub manifest_bytes: u64,
    pub manifest_sha256: String,
    pub transaction_file: String,
    pub transaction_bytes: u64,
    pub transaction_sha256: String,
    pub signature_file: String,
    pub signature_bytes: u64,
    pub signature_sha256: String,
    pub registry_file: String,
    pub registry_bytes: u64,
    pub registry_sha256: String,
    pub accounts_file: String,
    pub accounts_bytes: u64,
    pub accounts_sha256: String,
    pub first_epoch: u64,
    pub last_epoch: u64,
    pub transactions: u64,
    pub signatures: u64,
    pub pubkeys: u64,
    pub accounts: u64,
}

impl MarketSourceBinding {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.manifest_file == DUMP_MANIFEST_FILE
                && self.transaction_file == TRANSACTIONS_FILE
                && self.signature_file == SIGNATURES_FILE
                && self.registry_file == PUBKEY_REGISTRY_FILE
                && self.accounts_file == ACCOUNTS_FILE,
            "market source file names differ from the schema-3 dump"
        );
        ensure!(
            self.manifest_bytes != 0
                && self.transaction_bytes != 0
                && self.signature_bytes != 0
                && self.registry_bytes != 0
                && self.accounts_bytes != 0,
            "market source has an empty bound file"
        );
        ensure!(
            self.first_epoch <= self.last_epoch
                && self.transactions != 0
                && self.signatures >= self.transactions
                && self.pubkeys != 0
                && self.accounts != 0
                && self.accounts <= self.pubkeys,
            "market source counts or epoch range are invalid"
        );
        for (digest, label) in [
            (&self.manifest_sha256, "source manifest digest"),
            (&self.transaction_sha256, "source transaction digest"),
            (&self.signature_sha256, "source signature digest"),
            (&self.registry_sha256, "source registry digest"),
            (&self.accounts_sha256, "source accounts digest"),
        ] {
            parse_market_hex_digest(digest, label)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketParserBinding {
    pub semantic_version: String,
    pub implementation_fingerprint: String,
}

impl MarketParserBinding {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.semantic_version.is_empty()
                && self.semantic_version.is_ascii()
                && !self
                    .semantic_version
                    .bytes()
                    .any(|byte| byte.is_ascii_control()),
            "market parser semantic version is invalid"
        );
        parse_market_hex_digest(
            &self.implementation_fingerprint,
            "parser implementation fingerprint",
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketTargetBinding {
    pub mint: String,
    pub mint_id: u32,
    pub decimals: u8,
}

impl MarketTargetBinding {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.mint_id != 0, "market target mint ID is zero");
        validate_pubkey(&self.mint, "market target mint")
    }
}

/// Manifest-bound Token-2022 Scaled UI Amount configuration history.
///
/// When `enabled` is true, `events` starts with the mint-anchor initialize
/// instruction and contains every committed update in canonical source order.
/// A trade's `scaled_ui_config_id` selects the event state directly after the
/// referenced configuration instruction. When `enabled` is false, `events` is
/// empty and every trade uses configuration ID zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketScaledUiHistory {
    pub enabled: bool,
    pub processor_semantics: String,
    pub mint_anchor_slot: u64,
    pub mint_anchor_signature: String,
    pub events: Vec<ScaledUiAmountEvent>,
}

impl MarketScaledUiHistory {
    pub fn validate(
        &self,
        source: &MarketSourceBinding,
        target: &MarketTargetBinding,
    ) -> Result<()> {
        ensure!(
            self.processor_semantics == MARKET_SCALED_UI_PROCESSOR_SEMANTICS,
            "market Scaled UI processor semantics differ from the V3 contract"
        );
        ensure!(
            self.mint_anchor_slot != 0,
            "market Scaled UI mint-anchor slot is zero"
        );
        parse_canonical_signature(&self.mint_anchor_signature)
            .context("market Scaled UI mint-anchor signature is invalid")?;

        if !self.enabled {
            ensure!(
                self.events.is_empty(),
                "disabled market Scaled UI history contains events"
            );
            return Ok(());
        }

        validate_scaled_ui_amount_history(&self.events, target.mint_id)?;
        let initialize = self
            .events
            .first()
            .expect("validated Scaled UI history is not empty");
        ensure!(
            initialize.kind == ScaledUiAmountEventKind::Initialize
                && initialize.coordinate.slot == self.mint_anchor_slot
                && initialize.signature == self.mint_anchor_signature,
            "market Scaled UI initialize event is not at the source mint anchor"
        );

        for event in &self.events {
            ensure!(
                event.coordinate.transaction_id < source.transactions,
                "market Scaled UI event transaction ID exceeds the source"
            );
            ensure!(
                (source.first_epoch..=source.last_epoch).contains(&event.coordinate.source_epoch),
                "market Scaled UI event source epoch exceeds the source range"
            );
            ensure!(
                u64::from(event.target_mint_id) <= source.pubkeys,
                "market Scaled UI event target mint ID exceeds the source registry"
            );
            if let Some(authority_id) = event.authority_registry_id {
                ensure!(
                    u64::from(authority_id) <= source.pubkeys,
                    "market Scaled UI event authority ID exceeds the source registry"
                );
            }
        }
        for pair in self.events.windows(2) {
            let [previous, event] = pair else {
                unreachable!("a two-event window has two elements")
            };
            ensure!(
                scaled_ui_source_order_key(previous) < scaled_ui_source_order_key(event),
                "market Scaled UI events are not in strict source order"
            );
            if previous.coordinate.transaction_id == event.coordinate.transaction_id {
                ensure!(
                    previous.signature == event.signature,
                    "market Scaled UI events disagree about one transaction signature"
                );
            }
        }
        Ok(())
    }
}

fn scaled_ui_source_order_key(
    event: &ScaledUiAmountEvent,
) -> (u64, u64, u32, u32, u32, Option<u32>, Option<u32>) {
    let coordinate = event.coordinate;
    (
        coordinate.source_epoch,
        coordinate.slot,
        coordinate.source_block_id,
        coordinate.tx_index,
        coordinate.outer_index,
        coordinate.inner_index,
        coordinate.batch_index,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketInstructionKind {
    pub id: u32,
    pub program: String,
    pub name: String,
    /// Canonical lowercase hex for the exact discriminator bytes.
    pub discriminator: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketCounters {
    pub source_transactions: u64,
    pub successful_transactions: u64,
    pub failed_transactions: u64,
    pub metadata_absent_transactions: u64,
    pub instructions_examined: u64,
    pub parser_program_hits: u64,
    pub decoded_instructions: u64,
    pub semantic_swap_instructions: u64,
    pub semantic_target_swap_instructions: u64,
    pub token_transfer_instructions: u64,
    pub attributed_token_transfers: u64,
    pub trade_candidates: u64,
    pub emitted_trades: u64,
    pub rejected_failed_transaction: u64,
    pub rejected_missing_metadata: u64,
    pub rejected_missing_block_time: u64,
    pub rejected_missing_stack_height: u64,
    pub rejected_uncommitted_invocation: u64,
    pub rejected_missing_token_balance: u64,
    pub rejected_unsupported_program: u64,
    pub rejected_unsupported_discriminator: u64,
    pub rejected_malformed_instruction: u64,
    pub rejected_missing_instruction_data: u64,
    pub rejected_missing_accounts: u64,
    pub rejected_not_semantic_swap: u64,
    pub rejected_target_not_in_swap: u64,
    pub rejected_unsupported_token_instruction: u64,
    pub rejected_transfer_outside_subtree: u64,
    pub rejected_unresolved_token_flow: u64,
    pub rejected_ambiguous_token_flow: u64,
    pub rejected_target_on_both_or_neither_sides: u64,
    pub rejected_zero_amount: u64,
    pub rejected_decimal_mismatch: u64,
    pub rejected_balance_mismatch: u64,
    pub rejected_arithmetic_overflow: u64,
    pub rejected_duplicate: u64,
}

impl MarketCounters {
    pub fn trade_level_rejections(&self) -> Result<u64> {
        let mut total = 0u64;
        for value in [
            self.rejected_missing_block_time,
            self.rejected_missing_stack_height,
            self.rejected_uncommitted_invocation,
            self.rejected_missing_token_balance,
            self.rejected_unsupported_token_instruction,
            self.rejected_transfer_outside_subtree,
            self.rejected_unresolved_token_flow,
            self.rejected_ambiguous_token_flow,
            self.rejected_target_on_both_or_neither_sides,
            self.rejected_zero_amount,
            self.rejected_decimal_mismatch,
            self.rejected_balance_mismatch,
            self.rejected_arithmetic_overflow,
            self.rejected_duplicate,
        ] {
            total = total
                .checked_add(value)
                .context("market trade-level rejection counter overflow")?;
        }
        Ok(total)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(self.source_transactions != 0, "market scan is empty");
        ensure!(
            self.successful_transactions
                .checked_add(self.failed_transactions)
                .and_then(|value| value.checked_add(self.metadata_absent_transactions))
                == Some(self.source_transactions),
            "market transaction disposition counters do not partition the source scan"
        );
        ensure!(
            self.parser_program_hits <= self.instructions_examined
                && self.decoded_instructions <= self.parser_program_hits
                && self.semantic_swap_instructions <= self.decoded_instructions
                && self.semantic_target_swap_instructions <= self.semantic_swap_instructions,
            "market parser counters are not monotonic"
        );
        ensure!(
            self.parser_program_hits
                .checked_add(self.rejected_unsupported_program)
                == Some(self.instructions_examined),
            "market program counters do not partition examined instructions"
        );
        ensure!(
            self.decoded_instructions
                .checked_add(self.rejected_unsupported_discriminator)
                .and_then(|value| value.checked_add(self.rejected_malformed_instruction))
                .and_then(|value| value.checked_add(self.rejected_missing_instruction_data))
                .and_then(|value| value.checked_add(self.rejected_missing_accounts))
                == Some(self.parser_program_hits),
            "market parser outcomes do not partition parser-program hits"
        );
        ensure!(
            self.semantic_swap_instructions
                .checked_add(self.rejected_not_semantic_swap)
                == Some(self.decoded_instructions),
            "market decoded instruction classes do not partition decoded instructions"
        );
        ensure!(
            self.attributed_token_transfers <= self.token_transfer_instructions,
            "market attributed token transfers exceed token transfer instructions"
        );
        ensure!(
            self.emitted_trades
                .checked_add(self.trade_level_rejections()?)
                == Some(self.trade_candidates),
            "market candidate outcomes do not partition trade candidates"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketFileBinding {
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
    pub records: u64,
    pub record_bytes: u16,
}

impl MarketFileBinding {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.file == MARKET_TRADES_FILE
                && usize::from(self.record_bytes) == MARKET_TRADE_RECORD_BYTES
                && self.bytes == market_file_bytes(self.records)?,
            "market trade file binding differs from its fixed format"
        );
        parse_market_hex_digest(&self.sha256, "market trade file digest")?;
        Ok(())
    }
}

/// Human-readable definitions are fixed data in V3, not producer comments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDefinitions {
    pub transaction_id: String,
    pub registry_ids: String,
    pub instruction_coordinates: String,
    pub amounts: String,
    pub fees: String,
    pub prices: String,
    pub scaled_ui: String,
    pub flags: String,
    pub ordering: String,
    pub trade_candidates: String,
    pub rejection_counters: String,
}

impl MarketDefinitions {
    pub fn canonical() -> Self {
        Self {
            transaction_id: "Zero-based ordinal of the source transaction record in transactions.wincode.".into(),
            registry_ids: "All non-zero account, mint, and program IDs are one-based IDs in the bound source registry.bin; zero means absent only for optional IDs.".into(),
            instruction_coordinates: "outer_index is the zero-based outer instruction index; inner_index is the zero-based inner instruction index or u32::MAX for an outer instruction; stack_height is zero only when the stack is not proven.".into(),
            amounts: "amount_in and amount_out are executed raw token integer amounts proven from committed token transfers; decimals are stored separately; declared instruction limits are not execution truth.".into(),
            fees: "fee_amount is a raw token integer amount and fee_mint_id identifies its mint; both are zero unless FEE_KNOWN is set.".into(),
            prices: "No derived price is stored; clients first reproduce Token-2022 Scaled UI multiplication and truncation for the target amount selected by scaled_ui_config_id, then derive quote units per displayed target unit from that integer amount and the stored decimals.".into(),
            scaled_ui: "scaled_ui_config_id is zero only when Scaled UI Amount is disabled; otherwise it is the one-based config_id of the last committed Scaled UI configuration event before the trade; the manifest stores exact multiplier bits and the deployed legacy no-pending-promotion replay semantics.".into(),
            flags: "Flags record target direction and proven attribution evidence; every row requires COMMIT_PROVEN; unknown flag bits are invalid and INNER, STACK_PROVEN, ROUTER_ATTRIBUTED, and FEE_KNOWN must agree with their fields.".into(),
            ordering: "Rows use deterministic source order: transaction_id, outer_index, then zero for the outer instruction or inner_index plus one for an inner instruction; equal coordinates are invalid producer output.".into(),
            trade_candidates: "trade_candidates counts instructions admitted to trade materialization; each candidate has exactly one disposition: one emitted trade or one mutually exclusive trade-level rejection.".into(),
            rejection_counters: "Parser and pre-candidate rejection counters are informational and can describe different scan domains; only the documented trade-level rejection counters partition trade_candidates.".into(),
        }
    }
}

pub fn market_file_bytes(record_count: u64) -> Result<u64> {
    record_count
        .checked_mul(MARKET_TRADE_RECORD_BYTES as u64)
        .and_then(|body| body.checked_add(MARKET_HEADER_BYTES as u64))
        .context("market trade file byte length overflow")
}

pub fn parse_market_hex_digest(value: &str, label: &str) -> Result<[u8; 32]> {
    ensure!(value.len() == 64, "{label} is not a 32-byte hex digest");
    let mut output = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0], label)? << 4) | hex_nibble(pair[1], label)?;
    }
    Ok(output)
}

pub fn market_hex_digest(value: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn validate_registry_ids(ids: &[u32], maximum: u64, label: &str) -> Result<()> {
    ensure!(!ids.is_empty(), "market {label} list is empty");
    ensure!(
        ids.iter().all(|id| *id != 0 && u64::from(*id) <= maximum),
        "market {label} ID is zero or exceeds the source registry"
    );
    ensure!(
        ids.windows(2).all(|pair| pair[0] < pair[1]),
        "market {label} IDs are not strictly sorted and unique"
    );
    Ok(())
}

fn validate_instruction_kinds(kinds: &[MarketInstructionKind]) -> Result<()> {
    ensure!(!kinds.is_empty(), "market instruction-kind table is empty");
    for (index, kind) in kinds.iter().enumerate() {
        let expected_id = u32::try_from(index)
            .context("market instruction-kind table exceeds u32")?
            .checked_add(1)
            .context("market instruction-kind ID overflow")?;
        ensure!(
            kind.id == expected_id,
            "market instruction-kind IDs are not contiguous and one-based"
        );
        validate_pubkey(&kind.program, "market instruction-kind program")?;
        ensure!(
            !kind.name.is_empty()
                && kind.name.is_ascii()
                && !kind.name.bytes().any(|byte| byte.is_ascii_control()),
            "market instruction-kind name is invalid"
        );
        ensure!(
            matches!(kind.discriminator.len(), 2 | 10 | 16),
            "market instruction-kind discriminator has an unsupported byte length"
        );
        parse_canonical_hex(&kind.discriminator, "market instruction-kind discriminator")?;
        for previous in &kinds[..index] {
            ensure!(
                (
                    previous.program.as_str(),
                    previous.name.as_str(),
                    previous.discriminator.as_str(),
                ) != (
                    kind.program.as_str(),
                    kind.name.as_str(),
                    kind.discriminator.as_str(),
                ),
                "market instruction-kind table has duplicate definitions"
            );
        }
    }
    Ok(())
}

fn validate_pubkey(value: &str, label: &str) -> Result<()> {
    let bytes = bs58::decode(value)
        .into_vec()
        .with_context(|| format!("{label} is not base58"))?;
    ensure!(bytes.len() == 32, "{label} is not a 32-byte public key");
    Ok(())
}

fn parse_canonical_hex(value: &str, label: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.len().is_multiple_of(2),
        "{label} is not whole bytes"
    );
    for byte in value.bytes() {
        hex_nibble(byte, label)?;
    }
    Ok(())
}

fn hex_nibble(value: u8, label: &str) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => bail!("{label} is not canonical lowercase hex"),
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed u16 byte range"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed u32 byte range"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 byte range"),
    )
}

fn read_i64(bytes: &[u8], offset: usize) -> i64 {
    i64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed i64 byte range"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST_DIGEST: [u8; 32] = [3; 32];
    const TRANSACTION_DIGEST: [u8; 32] = [5; 32];
    const OTHER_DIGEST: [u8; 32] = [7; 32];
    const SPYX_MINT: &str = "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W";
    const SPYX_MINT_SIGNATURE: &str =
        "51QCqbftjH2JdVScV8MUPEEGTTCBBwRdFLcJnhR3e7gVr5PGcJaL6HTh4hpxpJC6sjXGNafCW8eZEZxRuScDs49R";
    const SPYX_MINT_SLOT: u64 = 346_066_298;
    const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    const RAYDIUM_CLMM: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";

    fn trade() -> MarketTradeRecord {
        MarketTradeRecord {
            transaction_id: 7,
            slot: 346_066_300,
            block_time: 1_750_000_000,
            source_epoch: 801,
            source_block_id: 22,
            tx_index: 4,
            outer_index: 3,
            inner_index: 2,
            stack_height: 3,
            instruction_kind_id: 1,
            dex_program_id: 11,
            router_program_id: 12,
            pool_id: 13,
            trader_id: 14,
            input_mint_id: 20,
            output_mint_id: 30,
            user_source_id: 15,
            user_destination_id: 16,
            amount_in: 1_234_567,
            amount_out: 9_876_543,
            fee_amount: 99,
            fee_mint_id: 20,
            flags: MARKET_TRADE_FLAG_TARGET_INPUT
                | MARKET_TRADE_FLAG_INNER
                | MARKET_TRADE_FLAG_STACK_PROVEN
                | MARKET_TRADE_FLAG_USER_SOURCE_MATCH
                | MARKET_TRADE_FLAG_USER_DESTINATION_MATCH
                | MARKET_TRADE_FLAG_INPUT_VAULT_MATCH
                | MARKET_TRADE_FLAG_OUTPUT_VAULT_MATCH
                | MARKET_TRADE_FLAG_BALANCE_RECONCILED
                | MARKET_TRADE_FLAG_ROUTER_ATTRIBUTED
                | MARKET_TRADE_FLAG_DIRECT_USD_QUOTE
                | MARKET_TRADE_FLAG_FEE_KNOWN
                | MARKET_TRADE_FLAG_COMMIT_PROVEN,
            input_decimals: 9,
            output_decimals: 6,
            input_transfer_count: 1,
            output_transfer_count: 2,
            scaled_ui_config_id: 1,
        }
    }

    fn scaled_ui_initialize() -> ScaledUiAmountEvent {
        ScaledUiAmountEvent {
            config_id: 1,
            coordinate: crate::scaled_ui_amount::ScaledUiAmountCoordinate {
                transaction_id: 0,
                source_epoch: 801,
                slot: SPYX_MINT_SLOT,
                block_time: 1_750_000_000,
                source_block_id: 1,
                tx_index: 0,
                outer_index: 0,
                inner_index: None,
                stack_height: 1,
                batch_index: None,
            },
            signature: SPYX_MINT_SIGNATURE.into(),
            target_mint_id: 20,
            kind: ScaledUiAmountEventKind::Initialize,
            multiplier: crate::scaled_ui_amount::ScaledUiAmountMultiplier::from_f64(1.0).unwrap(),
            effective_timestamp: 0,
            authority_registry_id: None,
            configured_authority_hex: Some(crate::scaled_ui_amount::canonical_pubkey_hex([9; 32])),
            commit_proven: true,
        }
    }

    fn scaled_ui_update(config_id: u32) -> ScaledUiAmountEvent {
        ScaledUiAmountEvent {
            config_id,
            coordinate: crate::scaled_ui_amount::ScaledUiAmountCoordinate {
                transaction_id: u64::from(config_id - 1),
                source_epoch: 801,
                slot: SPYX_MINT_SLOT + u64::from(config_id - 1),
                block_time: 1_750_000_000 + i64::from(config_id - 1),
                source_block_id: config_id,
                tx_index: 0,
                outer_index: 0,
                inner_index: None,
                stack_height: 1,
                batch_index: None,
            },
            signature: SPYX_MINT_SIGNATURE.into(),
            target_mint_id: 20,
            kind: ScaledUiAmountEventKind::UpdateMultiplier,
            multiplier: crate::scaled_ui_amount::ScaledUiAmountMultiplier::from_f64(1.5).unwrap(),
            effective_timestamp: 1_750_000_100,
            authority_registry_id: Some(7),
            configured_authority_hex: None,
            commit_proven: true,
        }
    }

    fn scaled_ui_history() -> MarketScaledUiHistory {
        MarketScaledUiHistory {
            enabled: true,
            processor_semantics: MARKET_SCALED_UI_PROCESSOR_SEMANTICS.into(),
            mint_anchor_slot: SPYX_MINT_SLOT,
            mint_anchor_signature: SPYX_MINT_SIGNATURE.into(),
            events: vec![scaled_ui_initialize()],
        }
    }

    fn counters() -> MarketCounters {
        MarketCounters {
            source_transactions: 10,
            successful_transactions: 7,
            failed_transactions: 2,
            metadata_absent_transactions: 1,
            instructions_examined: 100,
            parser_program_hits: 60,
            decoded_instructions: 50,
            semantic_swap_instructions: 40,
            semantic_target_swap_instructions: 30,
            token_transfer_instructions: 80,
            attributed_token_transfers: 60,
            trade_candidates: 2,
            emitted_trades: 1,
            rejected_failed_transaction: 2,
            rejected_missing_metadata: 1,
            rejected_missing_block_time: 0,
            rejected_missing_stack_height: 0,
            rejected_uncommitted_invocation: 0,
            rejected_missing_token_balance: 0,
            rejected_unsupported_program: 40,
            rejected_unsupported_discriminator: 7,
            rejected_malformed_instruction: 1,
            rejected_missing_instruction_data: 1,
            rejected_missing_accounts: 1,
            rejected_not_semantic_swap: 10,
            rejected_target_not_in_swap: 10,
            rejected_unsupported_token_instruction: 0,
            rejected_transfer_outside_subtree: 0,
            rejected_unresolved_token_flow: 1,
            rejected_ambiguous_token_flow: 0,
            rejected_target_on_both_or_neither_sides: 0,
            rejected_zero_amount: 0,
            rejected_decimal_mismatch: 0,
            rejected_balance_mismatch: 0,
            rejected_arithmetic_overflow: 0,
            rejected_duplicate: 0,
        }
    }

    fn manifest() -> MarketManifest {
        MarketManifest {
            schema_version: MARKET_SCHEMA_VERSION,
            artifact_kind: MarketManifest::ARTIFACT_KIND.into(),
            complete: true,
            canary_max_transactions: None,
            created_unix_seconds: 1_750_000_100,
            source: MarketSourceBinding {
                manifest_file: DUMP_MANIFEST_FILE.into(),
                manifest_bytes: 100,
                manifest_sha256: market_hex_digest(MANIFEST_DIGEST),
                transaction_file: TRANSACTIONS_FILE.into(),
                transaction_bytes: 10_000,
                transaction_sha256: market_hex_digest(TRANSACTION_DIGEST),
                signature_file: SIGNATURES_FILE.into(),
                signature_bytes: 640,
                signature_sha256: market_hex_digest(OTHER_DIGEST),
                registry_file: PUBKEY_REGISTRY_FILE.into(),
                registry_bytes: 3_200,
                registry_sha256: market_hex_digest(OTHER_DIGEST),
                accounts_file: ACCOUNTS_FILE.into(),
                accounts_bytes: 1_000,
                accounts_sha256: market_hex_digest(OTHER_DIGEST),
                first_epoch: 801,
                last_epoch: 1_018,
                transactions: 10,
                signatures: 10,
                pubkeys: 100,
                accounts: 50,
            },
            parser: MarketParserBinding {
                semantic_version: "1.0.0".into(),
                implementation_fingerprint: market_hex_digest(OTHER_DIGEST),
            },
            target: MarketTargetBinding {
                mint: SPYX_MINT.into(),
                mint_id: 20,
                decimals: 9,
            },
            scaled_ui: scaled_ui_history(),
            usd_quote_mint_ids: vec![30],
            instruction_kinds: vec![MarketInstructionKind {
                id: 1,
                program: RAYDIUM_CLMM.into(),
                name: "swap_v2".into(),
                discriminator: "2b04ed0b1ac91e62".into(),
            }],
            counters: counters(),
            trades: MarketFileBinding {
                file: MARKET_TRADES_FILE.into(),
                bytes: market_file_bytes(1).unwrap(),
                sha256: market_hex_digest(OTHER_DIGEST),
                records: 1,
                record_bytes: MARKET_TRADE_RECORD_BYTES as u16,
            },
            definitions: MarketDefinitions::canonical(),
        }
    }

    #[test]
    fn trade_row_is_exactly_128_bytes_and_round_trips() {
        let record = trade();
        let bytes = record.encode().unwrap();
        assert_eq!(bytes.len(), 128);
        assert_eq!(read_u64(&bytes, 0), record.transaction_id);
        assert_eq!(read_u64(&bytes, 88), record.amount_in);
        assert_eq!(read_u64(&bytes, 96), record.amount_out);
        assert_eq!(read_u64(&bytes, 104), record.fee_amount);
        assert_eq!(MarketTradeRecord::decode(&bytes).unwrap(), record);
    }

    #[test]
    fn trade_row_preserves_scaled_ui_config_id_and_rejects_unknown_flags() {
        let mut bytes = trade().encode().unwrap();
        bytes[116..118].copy_from_slice(&(1u16 << 15).to_le_bytes());
        assert!(MarketTradeRecord::decode(&bytes).is_err());

        let mut record = trade();
        record.scaled_ui_config_id = 23;
        let bytes = record.encode().unwrap();
        assert_eq!(read_u32(&bytes, 124), 23);
        assert_eq!(MarketTradeRecord::decode(&bytes).unwrap(), record);
    }

    #[test]
    fn outer_sentinel_and_inner_flag_are_one_invariant() {
        let inner_key = trade().order_key();
        let mut record = trade();
        record.inner_index = MARKET_OUTER_INNER_INDEX;
        assert!(record.encode().is_err());
        record.flags &= !MARKET_TRADE_FLAG_INNER;
        let bytes = record.encode().unwrap();
        let decoded = MarketTradeRecord::decode(&bytes).unwrap();
        assert!(decoded.is_outer());
        assert!(decoded.order_key() < inner_key);

        let mut inner = trade();
        inner.flags &= !MARKET_TRADE_FLAG_INNER;
        assert!(inner.encode().is_err());
    }

    #[test]
    fn target_flags_must_select_exactly_one_side() {
        let mut record = trade();
        record.flags |= MARKET_TRADE_FLAG_TARGET_OUTPUT;
        assert!(record.encode().is_err());
        record.flags &= !(MARKET_TRADE_FLAG_TARGET_INPUT | MARKET_TRADE_FLAG_TARGET_OUTPUT);
        assert!(record.encode().is_err());
    }

    #[test]
    fn header_round_trips_and_rejects_reserved_bytes_and_flags() {
        let header = MarketFileHeader::new(true, 42, MANIFEST_DIGEST, TRANSACTION_DIGEST);
        let bytes = header.encode();
        assert_eq!(bytes.len(), 128);
        assert_eq!(&bytes[..8], b"BZSMKT03");
        assert_eq!(read_u16(&bytes, 8), 3);
        assert!(bytes[88..].iter().all(|byte| *byte == 0));
        assert_eq!(MarketFileHeader::decode(&bytes).unwrap(), header);

        let mut reserved = bytes;
        reserved[127] = 1;
        assert!(MarketFileHeader::decode(&reserved).is_err());

        let mut flags = bytes;
        flags[14..16].copy_from_slice(&2u16.to_le_bytes());
        assert!(MarketFileHeader::decode(&flags).is_err());
    }

    #[test]
    fn v3_file_and_manifest_identifiers_are_fixed() {
        assert_eq!(MARKET_SCHEMA_VERSION, 3);
        assert_eq!(MARKET_MANIFEST_FILE, "market-manifest-v3.json");
        assert_eq!(MARKET_TRADES_FILE, "market-trades-v3.bin");
        assert_eq!(
            MarketManifest::ARTIFACT_KIND,
            "blockzilla_spyx_market_db_v3"
        );
    }

    #[test]
    fn manifest_strictly_binds_the_header_and_trade() {
        let manifest = manifest();
        let header = MarketFileHeader::new(true, 1, MANIFEST_DIGEST, TRANSACTION_DIGEST);
        manifest.validate().unwrap();
        manifest.validate_header(header).unwrap();
        manifest.validate_trade(trade()).unwrap();

        let wrong_manifest = MarketFileHeader::new(true, 1, OTHER_DIGEST, TRANSACTION_DIGEST);
        assert!(manifest.validate_header(wrong_manifest).is_err());
        let wrong_transactions = MarketFileHeader::new(true, 1, MANIFEST_DIGEST, OTHER_DIGEST);
        assert!(manifest.validate_header(wrong_transactions).is_err());
        let wrong_count = MarketFileHeader::new(true, 2, MANIFEST_DIGEST, TRANSACTION_DIGEST);
        assert!(manifest.validate_header(wrong_count).is_err());
    }

    #[test]
    fn scaled_ui_history_is_strictly_bound_to_anchor_source_and_target() {
        let mut manifest = manifest();
        manifest.scaled_ui.events.push(scaled_ui_update(2));
        manifest.validate().unwrap();

        let mut wrong_semantics = manifest.clone();
        wrong_semantics.scaled_ui.processor_semantics = "current".into();
        assert!(wrong_semantics.validate().is_err());

        let mut wrong_anchor = manifest.clone();
        wrong_anchor.scaled_ui.mint_anchor_slot += 1;
        assert!(wrong_anchor.validate().is_err());

        let mut wrong_signature = manifest.clone();
        wrong_signature.scaled_ui.mint_anchor_signature = SPYX_MINT.into();
        assert!(wrong_signature.validate().is_err());

        let mut wrong_initialize_signature = manifest.clone();
        wrong_initialize_signature.scaled_ui.events[0].signature =
            bs58::encode([1u8; 64]).into_string();
        assert!(wrong_initialize_signature.validate().is_err());

        let mut wrong_target = manifest.clone();
        wrong_target.scaled_ui.events[1].target_mint_id = 30;
        assert!(wrong_target.validate().is_err());

        let mut non_sequential = manifest.clone();
        non_sequential.scaled_ui.events[1].config_id = 3;
        assert!(non_sequential.validate().is_err());

        let mut reversed_source = manifest.clone();
        reversed_source.scaled_ui.events[1].coordinate.slot = SPYX_MINT_SLOT - 1;
        assert!(reversed_source.validate().is_err());

        let mut duplicate_initialize = manifest.clone();
        duplicate_initialize.scaled_ui.events[1].kind = ScaledUiAmountEventKind::Initialize;
        duplicate_initialize.scaled_ui.events[1].effective_timestamp = 0;
        duplicate_initialize.scaled_ui.events[1].authority_registry_id = None;
        duplicate_initialize.scaled_ui.events[1].configured_authority_hex =
            Some(crate::scaled_ui_amount::canonical_pubkey_hex([8; 32]));
        assert!(duplicate_initialize.validate().is_err());

        let mut uncommitted = manifest;
        uncommitted.scaled_ui.events[1].commit_proven = false;
        assert!(uncommitted.validate().is_err());
    }

    #[test]
    fn trade_scaled_ui_config_id_matches_manifest_mode_and_event_range() {
        let enabled = manifest();
        enabled.validate_trade(trade()).unwrap();

        let mut missing = trade();
        missing.scaled_ui_config_id = 0;
        assert!(enabled.validate_trade(missing).is_err());

        let mut out_of_range = trade();
        out_of_range.scaled_ui_config_id = 2;
        assert!(enabled.validate_trade(out_of_range).is_err());

        let mut disabled = manifest();
        disabled.scaled_ui.enabled = false;
        disabled.scaled_ui.events.clear();
        let mut unscaled_trade = trade();
        unscaled_trade.scaled_ui_config_id = 0;
        disabled.validate_trade(unscaled_trade).unwrap();
        assert!(disabled.validate_trade(trade()).is_err());

        disabled.scaled_ui.events.push(scaled_ui_initialize());
        assert!(disabled.validate().is_err());
    }

    #[test]
    fn candidate_and_rejection_counters_must_partition() {
        let mut values = counters();
        values.validate().unwrap();
        values.rejected_ambiguous_token_flow = 1;
        assert!(values.validate().is_err());
    }

    #[test]
    fn manifest_serde_rejects_unknown_fields() {
        let value = serde_json::to_value(manifest()).unwrap();
        let mut object = value.as_object().unwrap().clone();
        object.insert("price_micros".into(), serde_json::json!(1));
        assert!(serde_json::from_value::<MarketManifest>(object.into()).is_err());
    }

    #[test]
    fn digest_hex_is_canonical_lowercase() {
        assert_eq!(
            parse_market_hex_digest(&market_hex_digest(OTHER_DIGEST), "test").unwrap(),
            OTHER_DIGEST
        );
        assert!(parse_market_hex_digest(&"AA".repeat(32), "test").is_err());
    }

    #[test]
    fn public_key_fixture_is_valid() {
        validate_pubkey(USDC_MINT, "USDC").unwrap();
    }
}
