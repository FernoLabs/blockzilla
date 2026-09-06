//! Token-2022 Scaled UI Amount decoding and historical replay.
//!
//! Raw token amounts never change. This module preserves each multiplier's
//! exact IEEE-754 bits and applies the deployed Token-2022 program semantics
//! that were active for the indexed SPYx history. In those semantics, an
//! update does not promote an older pending multiplier before replacing it.

use std::{cmp::Ordering, ops::Range};

use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

/// `TokenInstruction::ScaledUiAmountExtension`.
pub const SCALED_UI_AMOUNT_OUTER_DISCRIMINATOR: u8 = 43;
/// `TokenInstruction::Batch`.
pub const TOKEN_2022_BATCH_DISCRIMINATOR: u8 = 255;
/// `ScaledUiAmountMintInstruction::Initialize`.
pub const SCALED_UI_AMOUNT_INITIALIZE_DISCRIMINATOR: u8 = 0;
/// `ScaledUiAmountMintInstruction::UpdateMultiplier`.
pub const SCALED_UI_AMOUNT_UPDATE_DISCRIMINATOR: u8 = 1;

pub const SCALED_UI_AMOUNT_INITIALIZE_DATA_BYTES: usize = 42;
pub const SCALED_UI_AMOUNT_UPDATE_DATA_BYTES: usize = 18;

/// Stable name for the historical processor behavior implemented here.
pub const DEPLOYED_LEGACY_REPLAY_SEMANTICS: &str = "deployed_legacy_no_pending_promotion_v1";

const U64_EXCLUSIVE_UPPER_BOUND_AS_F64: f64 = 18_446_744_073_709_551_616.0;

/// An exact, manifest-safe representation of a valid Token-2022 multiplier.
///
/// `bits` is always 16 lowercase hexadecimal digits. `decimal` is Rust's
/// shortest round-tripping decimal representation for those exact bits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScaledUiAmountMultiplier {
    pub bits: String,
    pub decimal: String,
}

impl ScaledUiAmountMultiplier {
    pub fn from_f64(value: f64) -> Result<Self> {
        validate_multiplier(value)?;
        Ok(Self {
            bits: format!("{:016x}", value.to_bits()),
            decimal: value.to_string(),
        })
    }

    pub fn from_bits(bits: u64) -> Result<Self> {
        Self::from_f64(f64::from_bits(bits))
    }

    pub fn validate(&self) -> Result<()> {
        let value = self.value_from_bits()?;
        validate_multiplier(value)?;
        ensure!(
            self.decimal == value.to_string(),
            "scaled UI multiplier decimal is not canonical for its exact bits"
        );
        let decimal_value = self
            .decimal
            .parse::<f64>()
            .map_err(|_| anyhow::anyhow!("scaled UI multiplier decimal is not an f64"))?;
        ensure!(
            decimal_value.to_bits() == value.to_bits(),
            "scaled UI multiplier decimal does not round-trip to its exact bits"
        );
        Ok(())
    }

    pub fn to_f64(&self) -> Result<f64> {
        self.validate()?;
        self.value_from_bits()
    }

    pub fn to_bits(&self) -> Result<u64> {
        self.validate()?;
        parse_canonical_u64_hex(&self.bits, "scaled UI multiplier bits")
    }

    fn value_from_bits(&self) -> Result<f64> {
        Ok(f64::from_bits(parse_canonical_u64_hex(
            &self.bits,
            "scaled UI multiplier bits",
        )?))
    }
}

/// The full position of an instruction in the canonical transaction stream.
///
/// `inner_index` is absent for an outer instruction. `batch_index` is the
/// zero-based index of the embedded instruction inside Token-2022 `Batch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScaledUiAmountCoordinate {
    pub transaction_id: u64,
    pub source_epoch: u64,
    pub slot: u64,
    pub block_time: i64,
    pub source_block_id: u32,
    pub tx_index: u32,
    pub outer_index: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inner_index: Option<u32>,
    pub stack_height: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_index: Option<u32>,
}

impl ScaledUiAmountCoordinate {
    /// Outer instructions sort before inner instructions for the same outer
    /// instruction. Batch items then sort by their embedded item index.
    pub fn canonical_order_key(self) -> (u64, u32, u64, u64) {
        (
            self.transaction_id,
            self.outer_index,
            self.inner_index.map_or(0, |index| u64::from(index) + 1),
            self.batch_index.map_or(0, |index| u64::from(index) + 1),
        )
    }

    pub fn validate(self) -> Result<()> {
        if self.inner_index.is_none() {
            ensure!(
                self.stack_height <= 1,
                "outer scaled UI instruction has an inner stack height"
            );
        } else {
            ensure!(
                self.stack_height > 1,
                "inner scaled UI instruction has no inner stack height"
            );
        }
        Ok(())
    }

    fn validate_same_transaction_facts(self, other: Self) -> Result<()> {
        if self.transaction_id == other.transaction_id {
            ensure!(
                self.source_epoch == other.source_epoch
                    && self.slot == other.slot
                    && self.block_time == other.block_time
                    && self.source_block_id == other.source_block_id
                    && self.tx_index == other.tx_index,
                "scaled UI coordinates disagree about one transaction"
            );
        }
        Ok(())
    }
}

impl Ord for ScaledUiAmountCoordinate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.canonical_order_key().cmp(&other.canonical_order_key())
    }
}

impl PartialOrd for ScaledUiAmountCoordinate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScaledUiAmountEventKind {
    Initialize,
    UpdateMultiplier,
}

/// One committed Scaled UI Amount configuration instruction.
///
/// Configuration IDs are one-based and contiguous in canonical instruction
/// order. The raw `effective_timestamp` is preserved. Replay normalizes it to
/// zero when it is negative, as the deployed processor does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScaledUiAmountEvent {
    pub config_id: u32,
    pub coordinate: ScaledUiAmountCoordinate,
    /// Canonical Base58 encoding of the 64-byte transaction signature.
    pub signature: String,
    pub target_mint_id: u32,
    pub kind: ScaledUiAmountEventKind,
    pub multiplier: ScaledUiAmountMultiplier,
    pub effective_timestamp: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_registry_id: Option<u32>,
    /// The initialize instruction's configured authority, as exact lowercase
    /// hexadecimal bytes. A null authority is represented by absence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_authority_hex: Option<String>,
    pub commit_proven: bool,
}

impl ScaledUiAmountEvent {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.config_id != 0, "scaled UI config ID is zero");
        ensure!(self.target_mint_id != 0, "scaled UI target mint ID is zero");
        ensure!(
            self.commit_proven,
            "scaled UI event does not have committed-invocation proof"
        );
        parse_canonical_signature(&self.signature)?;
        self.coordinate.validate()?;
        self.multiplier.validate()?;
        if let Some(authority_id) = self.authority_registry_id {
            ensure!(authority_id != 0, "scaled UI authority registry ID is zero");
        }
        if let Some(authority) = &self.configured_authority_hex {
            let bytes = parse_canonical_pubkey_hex(authority)?;
            ensure!(
                bytes.iter().any(|byte| *byte != 0),
                "scaled UI configured authority must omit a null pubkey"
            );
        }
        match self.kind {
            ScaledUiAmountEventKind::Initialize => {
                ensure!(
                    self.effective_timestamp == 0,
                    "scaled UI initialize event has a non-zero effective timestamp"
                );
            }
            ScaledUiAmountEventKind::UpdateMultiplier => {
                ensure!(
                    self.authority_registry_id.is_some(),
                    "scaled UI update event has no authority registry ID"
                );
                ensure!(
                    self.configured_authority_hex.is_none(),
                    "scaled UI update event contains an initialize authority"
                );
            }
        }
        Ok(())
    }

    pub fn normalized_effective_timestamp(&self) -> i64 {
        self.effective_timestamp.max(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedScaledUiAmountInstruction {
    Initialize {
        authority: Option<[u8; 32]>,
        multiplier: ScaledUiAmountMultiplier,
    },
    UpdateMultiplier {
        multiplier: ScaledUiAmountMultiplier,
        effective_timestamp: i64,
    },
}

impl ParsedScaledUiAmountInstruction {
    pub const fn minimum_account_count(&self) -> usize {
        match self {
            Self::Initialize { .. } => 1,
            Self::UpdateMultiplier { .. } => 2,
        }
    }

    pub const fn event_kind(&self) -> ScaledUiAmountEventKind {
        match self {
            Self::Initialize { .. } => ScaledUiAmountEventKind::Initialize,
            Self::UpdateMultiplier { .. } => ScaledUiAmountEventKind::UpdateMultiplier,
        }
    }

    pub fn multiplier(&self) -> &ScaledUiAmountMultiplier {
        match self {
            Self::Initialize { multiplier, .. } | Self::UpdateMultiplier { multiplier, .. } => {
                multiplier
            }
        }
    }

    pub const fn effective_timestamp(&self) -> i64 {
        match self {
            Self::Initialize { .. } => 0,
            Self::UpdateMultiplier {
                effective_timestamp,
                ..
            } => *effective_timestamp,
        }
    }
}

/// A direct or Batch-embedded Scaled UI Amount instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedScaledUiAmountOccurrence {
    /// `None` for a direct Token-2022 instruction.
    pub batch_index: Option<u32>,
    /// First account in the Batch instruction's concatenated account list.
    pub account_offset: usize,
    /// Present for Batch items because the count is part of the wire data.
    pub account_count: Option<u8>,
    pub instruction: ParsedScaledUiAmountInstruction,
}

impl ParsedScaledUiAmountOccurrence {
    /// Resolve and validate this instruction's account slice without copying.
    pub fn account_range(&self, outer_account_count: usize) -> Result<Range<usize>> {
        let count = self.account_count.map_or(outer_account_count, usize::from);
        let end = self
            .account_offset
            .checked_add(count)
            .ok_or_else(|| anyhow::anyhow!("scaled UI account range overflows"))?;
        ensure!(
            end <= outer_account_count,
            "scaled UI instruction account range exceeds the outer account list"
        );
        ensure!(
            count >= self.instruction.minimum_account_count(),
            "scaled UI instruction has too few accounts"
        );
        Ok(self.account_offset..end)
    }
}

/// Parse one direct Token-2022 instruction.
///
/// A non-Scaled-UI discriminator returns `None`. Once discriminator 43 is
/// present, every byte and the extension sub-discriminator are strict.
pub fn parse_scaled_ui_amount_instruction(
    data: &[u8],
) -> Result<Option<ParsedScaledUiAmountInstruction>> {
    ensure!(!data.is_empty(), "Token-2022 instruction data is empty");
    if data[0] != SCALED_UI_AMOUNT_OUTER_DISCRIMINATOR {
        return Ok(None);
    }
    ensure!(
        data.len() >= 2,
        "scaled UI instruction has no extension discriminator"
    );
    match data[1] {
        SCALED_UI_AMOUNT_INITIALIZE_DISCRIMINATOR => {
            ensure!(
                data.len() == SCALED_UI_AMOUNT_INITIALIZE_DATA_BYTES,
                "scaled UI initialize instruction byte length differs"
            );
            let authority_bytes: [u8; 32] = data[2..34]
                .try_into()
                .expect("fixed scaled UI authority range");
            let multiplier = parse_multiplier_le(&data[34..42])?;
            Ok(Some(ParsedScaledUiAmountInstruction::Initialize {
                authority: authority_bytes
                    .iter()
                    .any(|byte| *byte != 0)
                    .then_some(authority_bytes),
                multiplier,
            }))
        }
        SCALED_UI_AMOUNT_UPDATE_DISCRIMINATOR => {
            ensure!(
                data.len() == SCALED_UI_AMOUNT_UPDATE_DATA_BYTES,
                "scaled UI update instruction byte length differs"
            );
            let multiplier = parse_multiplier_le(&data[2..10])?;
            let effective_timestamp = i64::from_le_bytes(
                data[10..18]
                    .try_into()
                    .expect("fixed scaled UI timestamp range"),
            );
            Ok(Some(ParsedScaledUiAmountInstruction::UpdateMultiplier {
                multiplier,
                effective_timestamp,
            }))
        }
        discriminator => bail!("unknown scaled UI instruction discriminator {discriminator}"),
    }
}

/// Parse all Scaled UI Amount instructions in one Token-2022 instruction.
///
/// Token-2022 `Batch` items are scanned without copying their data. Malformed
/// item headers, empty embedded instructions, nested batches, and malformed
/// discriminator-43 items are rejected.
pub fn parse_scaled_ui_amount_occurrences(
    data: &[u8],
) -> Result<Vec<ParsedScaledUiAmountOccurrence>> {
    ensure!(!data.is_empty(), "Token-2022 instruction data is empty");
    if data[0] != TOKEN_2022_BATCH_DISCRIMINATOR {
        return Ok(parse_scaled_ui_amount_instruction(data)?
            .into_iter()
            .map(|instruction| ParsedScaledUiAmountOccurrence {
                batch_index: None,
                account_offset: 0,
                account_count: None,
                instruction,
            })
            .collect());
    }

    ensure!(data.len() > 1, "Token-2022 Batch has no items");
    let mut cursor = 1usize;
    let mut batch_index = 0u32;
    let mut account_offset = 0usize;
    let mut occurrences = Vec::new();
    while cursor < data.len() {
        ensure!(
            data.len() - cursor >= 2,
            "Token-2022 Batch item header is truncated"
        );
        let account_count = data[cursor];
        let data_length = usize::from(data[cursor + 1]);
        cursor += 2;
        ensure!(data_length != 0, "Token-2022 Batch item data is empty");
        let end = cursor
            .checked_add(data_length)
            .ok_or_else(|| anyhow::anyhow!("Token-2022 Batch data range overflows"))?;
        ensure!(end <= data.len(), "Token-2022 Batch item data is truncated");
        let embedded = &data[cursor..end];
        ensure!(
            embedded[0] != TOKEN_2022_BATCH_DISCRIMINATOR,
            "nested Token-2022 Batch instruction is invalid"
        );
        if let Some(instruction) = parse_scaled_ui_amount_instruction(embedded)? {
            let occurrence = ParsedScaledUiAmountOccurrence {
                batch_index: Some(batch_index),
                account_offset,
                account_count: Some(account_count),
                instruction,
            };
            ensure!(
                usize::from(account_count) >= occurrence.instruction.minimum_account_count(),
                "Batch-embedded scaled UI instruction has too few accounts"
            );
            occurrences.push(occurrence);
        }
        account_offset = account_offset
            .checked_add(usize::from(account_count))
            .ok_or_else(|| anyhow::anyhow!("Token-2022 Batch account count overflows"))?;
        batch_index = batch_index
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Token-2022 Batch item index overflows"))?;
        cursor = end;
    }
    Ok(occurrences)
}

/// One multiplier configuration after legacy processor replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScaledUiAmountConfiguration {
    pub config_id: u32,
    pub multiplier: ScaledUiAmountMultiplier,
    pub effective_timestamp: i64,
    pub source: ScaledUiAmountCoordinate,
}

/// The exact legacy Token-2022 configuration state after one event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyScaledUiAmountState {
    target_mint_id: u32,
    last_config_id: u32,
    last_coordinate: ScaledUiAmountCoordinate,
    baseline: ScaledUiAmountConfiguration,
    pending: ScaledUiAmountConfiguration,
}

impl LegacyScaledUiAmountState {
    pub fn from_initialize(event: &ScaledUiAmountEvent) -> Result<Self> {
        event.validate()?;
        ensure!(
            event.kind == ScaledUiAmountEventKind::Initialize,
            "first scaled UI event is not initialize"
        );
        ensure!(event.config_id == 1, "first scaled UI config ID is not one");
        let initial = configuration_from_event(event);
        Ok(Self {
            target_mint_id: event.target_mint_id,
            last_config_id: event.config_id,
            last_coordinate: event.coordinate,
            baseline: initial.clone(),
            pending: initial,
        })
    }

    /// Apply one update with the behavior of the deployed legacy processor.
    ///
    /// A future update replaces `pending` without promoting an older pending
    /// value that has already become active. This can cause the active value
    /// to return to `baseline` until the new update's timestamp is reached.
    pub fn apply_update(&mut self, event: &ScaledUiAmountEvent) -> Result<()> {
        event.validate()?;
        ensure!(
            event.kind == ScaledUiAmountEventKind::UpdateMultiplier,
            "scaled UI replay received a second initialize event"
        );
        ensure!(
            event.target_mint_id == self.target_mint_id,
            "scaled UI update targets a different mint"
        );
        let expected_config_id = self
            .last_config_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("scaled UI config ID overflows"))?;
        ensure!(
            event.config_id == expected_config_id,
            "scaled UI config IDs are not contiguous"
        );
        self.last_coordinate
            .validate_same_transaction_facts(event.coordinate)?;
        ensure!(
            event.coordinate > self.last_coordinate,
            "scaled UI events are not in strict canonical order"
        );

        let new_configuration = configuration_from_event(event);
        if event.coordinate.block_time >= new_configuration.effective_timestamp {
            self.baseline = new_configuration.clone();
        }
        self.pending = new_configuration;
        self.last_config_id = event.config_id;
        self.last_coordinate = event.coordinate;
        Ok(())
    }

    pub const fn target_mint_id(&self) -> u32 {
        self.target_mint_id
    }

    pub const fn last_config_id(&self) -> u32 {
        self.last_config_id
    }

    pub fn baseline(&self) -> &ScaledUiAmountConfiguration {
        &self.baseline
    }

    pub fn pending(&self) -> &ScaledUiAmountConfiguration {
        &self.pending
    }

    pub fn active_at(&self, unix_timestamp: i64) -> &ScaledUiAmountConfiguration {
        if unix_timestamp >= self.pending.effective_timestamp {
            &self.pending
        } else {
            &self.baseline
        }
    }
}

/// Validate a complete, canonical event history for one target mint.
pub fn validate_scaled_ui_amount_history(
    events: &[ScaledUiAmountEvent],
    target_mint_id: u32,
) -> Result<()> {
    ensure!(target_mint_id != 0, "scaled UI target mint ID is zero");
    let (first, remaining) = events
        .split_first()
        .ok_or_else(|| anyhow::anyhow!("scaled UI event history is empty"))?;
    ensure!(
        first.target_mint_id == target_mint_id,
        "scaled UI initialize event targets a different mint"
    );
    let mut state = LegacyScaledUiAmountState::from_initialize(first)?;
    for event in remaining {
        state.apply_update(event)?;
    }
    Ok(())
}

/// Build one immutable legacy state snapshot per configuration ID.
///
/// Snapshot `config_id - 1` is the state directly after that event. A trade
/// record can therefore bind to the last configuration instruction that ran
/// before the trade and resolve the active multiplier using its block time.
pub fn build_legacy_state_snapshots(
    events: &[ScaledUiAmountEvent],
    target_mint_id: u32,
) -> Result<Vec<LegacyScaledUiAmountState>> {
    validate_scaled_ui_amount_history(events, target_mint_id)?;
    let mut states = Vec::with_capacity(events.len());
    let mut state = LegacyScaledUiAmountState::from_initialize(&events[0])?;
    states.push(state.clone());
    for event in &events[1..] {
        state.apply_update(event)?;
        states.push(state.clone());
    }
    Ok(states)
}

/// Apply the official floating-point multiplication and truncation.
pub fn scaled_raw_amount_f64(
    raw_amount: u64,
    multiplier: &ScaledUiAmountMultiplier,
) -> Result<f64> {
    let value = multiplier.to_f64()?;
    Ok(((raw_amount as f64) * value).trunc())
}

/// Apply official truncation and return an integer when it fits in `u64`.
///
/// The official display function can produce values larger than `u64` or
/// infinity. Those values do not have a safe fixed-width trade denominator,
/// so this helper fails instead of saturating.
pub fn checked_scaled_raw_amount(
    raw_amount: u64,
    multiplier: &ScaledUiAmountMultiplier,
) -> Result<u64> {
    let scaled = scaled_raw_amount_f64(raw_amount, multiplier)?;
    ensure!(scaled.is_finite(), "scaled UI raw amount is not finite");
    ensure!(scaled >= 0.0, "scaled UI raw amount is negative");
    ensure!(
        scaled < U64_EXCLUSIVE_UPPER_BOUND_AS_F64,
        "scaled UI raw amount does not fit in u64"
    );
    Ok(scaled as u64)
}

/// Match Token-2022's displayed amount conversion and zero trimming.
pub fn scaled_ui_amount_string(
    raw_amount: u64,
    decimals: u8,
    multiplier: &ScaledUiAmountMultiplier,
) -> Result<String> {
    let truncated = scaled_raw_amount_f64(raw_amount, multiplier)?;
    let ui_amount = truncated / 10_f64.powi(i32::from(decimals));
    let mut rendered = format!("{ui_amount:.*}", usize::from(decimals));
    if decimals > 0 {
        let without_zeroes = rendered.trim_end_matches('0');
        rendered = without_zeroes.trim_end_matches('.').to_owned();
    }
    Ok(rendered)
}

pub fn canonical_pubkey_hex(pubkey: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in pubkey {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

pub fn parse_canonical_pubkey_hex(value: &str) -> Result<[u8; 32]> {
    ensure!(
        value.len() == 64,
        "scaled UI configured authority is not 32-byte hex"
    );
    let mut output = [0u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0], "scaled UI configured authority")? << 4)
            | hex_nibble(pair[1], "scaled UI configured authority")?;
    }
    Ok(output)
}

/// Decode a canonical Solana transaction signature without heap allocation.
pub fn parse_canonical_signature(value: &str) -> Result<[u8; 64]> {
    let mut output = [0u8; 64];
    let length = bs58::decode(value)
        .onto(&mut output)
        .map_err(|_| anyhow::anyhow!("scaled UI event signature is not Base58"))?;
    ensure!(
        length == output.len(),
        "scaled UI event signature is not 64 bytes"
    );
    ensure!(
        bs58::encode(output).into_string() == value,
        "scaled UI event signature is not canonical Base58"
    );
    Ok(output)
}

fn configuration_from_event(event: &ScaledUiAmountEvent) -> ScaledUiAmountConfiguration {
    ScaledUiAmountConfiguration {
        config_id: event.config_id,
        multiplier: event.multiplier.clone(),
        effective_timestamp: event.normalized_effective_timestamp(),
        source: event.coordinate,
    }
}

fn parse_multiplier_le(bytes: &[u8]) -> Result<ScaledUiAmountMultiplier> {
    ensure!(bytes.len() == 8, "scaled UI multiplier byte length differs");
    ScaledUiAmountMultiplier::from_bits(u64::from_le_bytes(
        bytes.try_into().expect("fixed scaled UI multiplier range"),
    ))
}

fn validate_multiplier(value: f64) -> Result<()> {
    ensure!(
        value.is_normal() && value.is_sign_positive(),
        "scaled UI multiplier is not a positive normal f64"
    );
    Ok(())
}

fn parse_canonical_u64_hex(value: &str, label: &str) -> Result<u64> {
    ensure!(value.len() == 16, "{label} is not 8-byte hex");
    let mut output = 0u64;
    for byte in value.bytes() {
        output = output
            .checked_mul(16)
            .expect("16 hexadecimal digits fit in u64");
        output |= u64::from(hex_nibble(byte, label)?);
    }
    Ok(output)
}

fn hex_nibble(value: u8, label: &str) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => bail!("{label} is not canonical lowercase hex"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET_MINT_ID: u32 = 42;
    const SIGNATURE: &str =
        "51QCqbftjH2JdVScV8MUPEEGTTCBBwRdFLcJnhR3e7gVr5PGcJaL6HTh4hpxpJC6sjXGNafCW8eZEZxRuScDs49R";

    fn coordinate(transaction_id: u64, block_time: i64) -> ScaledUiAmountCoordinate {
        ScaledUiAmountCoordinate {
            transaction_id,
            source_epoch: 801,
            slot: 346_066_298 + transaction_id,
            block_time,
            source_block_id: transaction_id as u32,
            tx_index: 0,
            outer_index: 4,
            inner_index: None,
            stack_height: 1,
            batch_index: None,
        }
    }

    fn event(
        config_id: u32,
        kind: ScaledUiAmountEventKind,
        multiplier: f64,
        block_time: i64,
        effective_timestamp: i64,
    ) -> ScaledUiAmountEvent {
        ScaledUiAmountEvent {
            config_id,
            coordinate: coordinate(u64::from(config_id), block_time),
            signature: SIGNATURE.to_owned(),
            target_mint_id: TARGET_MINT_ID,
            kind,
            multiplier: ScaledUiAmountMultiplier::from_f64(multiplier).unwrap(),
            effective_timestamp,
            authority_registry_id: (kind == ScaledUiAmountEventKind::UpdateMultiplier).then_some(7),
            configured_authority_hex: (kind == ScaledUiAmountEventKind::Initialize)
                .then(|| canonical_pubkey_hex([9; 32])),
            commit_proven: true,
        }
    }

    fn update_bytes(multiplier: f64, timestamp: i64) -> Vec<u8> {
        let mut bytes = vec![
            SCALED_UI_AMOUNT_OUTER_DISCRIMINATOR,
            SCALED_UI_AMOUNT_UPDATE_DISCRIMINATOR,
        ];
        bytes.extend_from_slice(&multiplier.to_le_bytes());
        bytes.extend_from_slice(&timestamp.to_le_bytes());
        bytes
    }

    #[test]
    fn parses_real_spyx_initialize_layout() {
        let data = decode_hex(
            "2b00066f592251cc47747825a59ad1422ea43573f528da5dee2af7812b314f9945e3\
             000000000000f03f",
        );
        let parsed = parse_scaled_ui_amount_instruction(&data).unwrap().unwrap();
        match parsed {
            ParsedScaledUiAmountInstruction::Initialize {
                authority,
                multiplier,
            } => {
                assert_eq!(
                    canonical_pubkey_hex(authority.unwrap()),
                    "066f592251cc47747825a59ad1422ea43573f528da5dee2af7812b314f9945e3"
                );
                assert_eq!(multiplier.to_f64().unwrap(), 1.0);
                assert_eq!(multiplier.bits, "3ff0000000000000");
                assert_eq!(multiplier.decimal, "1");
            }
            _ => panic!("expected initialize"),
        }
    }

    #[test]
    fn parses_update_in_wire_field_order() {
        let bytes = update_bytes(1.005_714_560_286_254, 1_781_755_200);
        let parsed = parse_scaled_ui_amount_instruction(&bytes).unwrap().unwrap();
        match parsed {
            ParsedScaledUiAmountInstruction::UpdateMultiplier {
                multiplier,
                effective_timestamp,
            } => {
                assert_eq!(multiplier.to_f64().unwrap(), 1.005_714_560_286_254);
                assert_eq!(effective_timestamp, 1_781_755_200);
            }
            _ => panic!("expected update"),
        }
    }

    #[test]
    fn strict_scaled_parser_rejects_bad_layout_and_values() {
        assert!(parse_scaled_ui_amount_instruction(&[43]).is_err());
        assert!(parse_scaled_ui_amount_instruction(&[43, 2]).is_err());
        let mut trailing = update_bytes(2.0, 10);
        trailing.push(0);
        assert!(parse_scaled_ui_amount_instruction(&trailing).is_err());
        assert!(parse_scaled_ui_amount_instruction(&update_bytes(0.0, 10)).is_err());
        assert!(parse_scaled_ui_amount_instruction(&update_bytes(f64::NAN, 10)).is_err());
        assert!(
            parse_scaled_ui_amount_instruction(&update_bytes(f64::MIN_POSITIVE / 2.0, 10)).is_err()
        );
    }

    #[test]
    fn parses_batch_account_offsets_without_copying_accounts() {
        let update = update_bytes(2.0, 200);
        let mut batch = vec![TOKEN_2022_BATCH_DISCRIMINATOR];
        batch.extend_from_slice(&[3, 1, 3]);
        batch.push(2);
        batch.push(update.len() as u8);
        batch.extend_from_slice(&update);

        let occurrences = parse_scaled_ui_amount_occurrences(&batch).unwrap();
        assert_eq!(occurrences.len(), 1);
        assert_eq!(occurrences[0].batch_index, Some(1));
        assert_eq!(occurrences[0].account_offset, 3);
        assert_eq!(occurrences[0].account_count, Some(2));
        assert_eq!(occurrences[0].account_range(5).unwrap(), 3..5);
    }

    #[test]
    fn strict_batch_parser_rejects_malformed_items_and_nesting() {
        assert!(parse_scaled_ui_amount_occurrences(&[255]).is_err());
        assert!(parse_scaled_ui_amount_occurrences(&[255, 1]).is_err());
        assert!(parse_scaled_ui_amount_occurrences(&[255, 1, 2, 3]).is_err());
        assert!(parse_scaled_ui_amount_occurrences(&[255, 0, 0]).is_err());
        assert!(parse_scaled_ui_amount_occurrences(&[255, 0, 1, 255]).is_err());

        let update = update_bytes(2.0, 1);
        let mut too_few_accounts = vec![255, 1, update.len() as u8];
        too_few_accounts.extend_from_slice(&update);
        assert!(parse_scaled_ui_amount_occurrences(&too_few_accounts).is_err());
    }

    #[test]
    fn multiplier_json_preserves_bits_and_rejects_noncanonical_forms() {
        let multiplier = ScaledUiAmountMultiplier::from_f64(1.005_714_560_286_254).unwrap();
        let json = serde_json::to_string(&multiplier).unwrap();
        let decoded: ScaledUiAmountMultiplier = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, multiplier);
        decoded.validate().unwrap();

        let mut uppercase = decoded.clone();
        uppercase.bits = uppercase.bits.to_uppercase();
        assert!(uppercase.validate().is_err());
        let mut verbose_decimal = decoded.clone();
        verbose_decimal.decimal.push('0');
        assert!(verbose_decimal.validate().is_err());
    }

    #[test]
    fn legacy_replay_reproduces_pending_overwrite_reversion() {
        let events = vec![
            event(1, ScaledUiAmountEventKind::Initialize, 1.0, 100, 0),
            event(2, ScaledUiAmountEventKind::UpdateMultiplier, 2.0, 150, 200),
            event(3, ScaledUiAmountEventKind::UpdateMultiplier, 3.0, 220, 300),
        ];
        let states = build_legacy_state_snapshots(&events, TARGET_MINT_ID).unwrap();
        assert_eq!(states[1].active_at(199).multiplier.to_f64().unwrap(), 1.0);
        assert_eq!(states[1].active_at(200).multiplier.to_f64().unwrap(), 2.0);

        // At t=220, 2x was active before update 3. The deployed legacy update
        // overwrites pending without promoting 2x, so state returns to 1x.
        assert_eq!(states[2].active_at(220).multiplier.to_f64().unwrap(), 1.0);
        assert_eq!(states[2].active_at(300).multiplier.to_f64().unwrap(), 3.0);
    }

    #[test]
    fn legacy_two_update_workaround_keeps_the_active_value() {
        let events = vec![
            event(1, ScaledUiAmountEventKind::Initialize, 1.0, 100, 0),
            event(2, ScaledUiAmountEventKind::UpdateMultiplier, 2.0, 150, 200),
            event(3, ScaledUiAmountEventKind::UpdateMultiplier, 2.0, 220, 200),
            event(4, ScaledUiAmountEventKind::UpdateMultiplier, 3.0, 220, 300),
        ];
        let states = build_legacy_state_snapshots(&events, TARGET_MINT_ID).unwrap();
        assert_eq!(states[3].active_at(220).multiplier.to_f64().unwrap(), 2.0);
        assert_eq!(states[3].active_at(300).multiplier.to_f64().unwrap(), 3.0);
    }

    #[test]
    fn legacy_replay_normalizes_negative_timestamps() {
        let events = vec![
            event(1, ScaledUiAmountEventKind::Initialize, 1.0, 100, 0),
            event(2, ScaledUiAmountEventKind::UpdateMultiplier, 4.0, 110, -5),
        ];
        let states = build_legacy_state_snapshots(&events, TARGET_MINT_ID).unwrap();
        assert_eq!(states[1].pending().effective_timestamp, 0);
        assert_eq!(states[1].active_at(110).multiplier.to_f64().unwrap(), 4.0);
    }

    #[test]
    fn official_conversion_multiplies_then_truncates() {
        let multiplier = ScaledUiAmountMultiplier::from_f64(0.99).unwrap();
        assert_eq!(checked_scaled_raw_amount(101, &multiplier).unwrap(), 99);
        assert_eq!(
            scaled_ui_amount_string(101, 2, &multiplier).unwrap(),
            "0.99"
        );

        let one = ScaledUiAmountMultiplier::from_f64(1.0).unwrap();
        assert!(checked_scaled_raw_amount(u64::MAX, &one).is_err());
        assert_eq!(
            scaled_ui_amount_string(
                10_000_000_000,
                10,
                &ScaledUiAmountMultiplier::from_f64(5.0).unwrap()
            )
            .unwrap(),
            "5"
        );
    }

    #[test]
    fn history_validation_rejects_gaps_and_noncanonical_order() {
        let mut events = vec![
            event(1, ScaledUiAmountEventKind::Initialize, 1.0, 100, 0),
            event(3, ScaledUiAmountEventKind::UpdateMultiplier, 2.0, 110, 110),
        ];
        assert!(validate_scaled_ui_amount_history(&events, TARGET_MINT_ID).is_err());

        events[1].config_id = 2;
        events[1].coordinate.transaction_id = 0;
        assert!(validate_scaled_ui_amount_history(&events, TARGET_MINT_ID).is_err());
    }

    #[test]
    fn event_serde_rejects_unknown_fields() {
        let original = event(1, ScaledUiAmountEventKind::Initialize, 1.0, 100, 0);
        let mut value = serde_json::to_value(original).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unknown".to_owned(), serde_json::json!(true));
        assert!(serde_json::from_value::<ScaledUiAmountEvent>(value).is_err());
    }

    #[test]
    fn event_signature_is_exact_and_canonical() {
        let decoded = parse_canonical_signature(SIGNATURE).unwrap();
        assert_eq!(bs58::encode(decoded).into_string(), SIGNATURE);

        let mut invalid = event(1, ScaledUiAmountEventKind::Initialize, 1.0, 100, 0);
        invalid.signature = "1111".to_owned();
        assert!(invalid.validate().is_err());
        invalid.signature = format!("1{SIGNATURE}");
        assert!(invalid.validate().is_err());
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        let compact: String = value
            .chars()
            .filter(|value| !value.is_whitespace())
            .collect();
        compact
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                (hex_nibble(pair[0], "test hex").unwrap() << 4)
                    | hex_nibble(pair[1], "test hex").unwrap()
            })
            .collect()
    }
}
