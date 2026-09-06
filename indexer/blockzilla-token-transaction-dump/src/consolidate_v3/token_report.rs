//! Exact public token-balance history for one completed schema-3 dump.
//!
//! This intentionally reports the public raw token-account amount from
//! transaction metadata. For Token-2022 mints with ScaledUiAmount or
//! ConfidentialTransfer, that value is not the complete economic balance.

use std::{
    cell::Cell,
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

use blockzilla_read_sdk::{BorrowedArchiveV2TokenBalance, TokenBalanceSide};

use super::*;

const TOKEN_HISTORY_PROGRESS_TRANSACTIONS: u64 = 250_000;
const TOKEN_HISTORY_REPORT_SCHEMA_VERSION: u16 = 1;
const RPC_SIGNATURE_PAGE_SIZE: u64 = 1_000;
const RPC_SIGNATURE_CREDIT_PAGE_SIZE: u64 = 100;
const TOP_HOLDER_LIMIT: usize = 25;
const FINAL_TOP_HOLDER_HISTORY_LIMIT: usize = 100;
const TOP_VOLUME_LIMIT: usize = 25;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PublicBalanceRow {
    account_index: u32,
    amount: u128,
    owner_id: Option<u32>,
    decimals: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TokenMetadataStage {
    pre: Vec<PublicBalanceRow>,
    post: Vec<PublicBalanceRow>,
    loaded_ids: [u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    total_pre_rows: usize,
    total_post_rows: usize,
}

impl TokenMetadataStage {
    fn new() -> Self {
        Self {
            pre: Vec::with_capacity(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS),
            post: Vec::with_capacity(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS),
            loaded_ids: [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
            total_pre_rows: 0,
            total_post_rows: 0,
        }
    }

    fn clear(&mut self) {
        self.pre.clear();
        self.post.clear();
        self.loaded_ids.fill(0);
        self.total_pre_rows = 0;
        self.total_post_rows = 0;
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TokenAccountState {
    amount: u128,
    owner_id: Option<u32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AmountDelta {
    increase: u128,
    decrease: u128,
}

impl AmountDelta {
    fn add_increase(&mut self, amount: u128, label: &str) -> Result<()> {
        self.increase = self
            .increase
            .checked_add(amount)
            .with_context(|| format!("{label} increase overflow"))?;
        Ok(())
    }

    fn add_decrease(&mut self, amount: u128, label: &str) -> Result<()> {
        self.decrease = self
            .decrease
            .checked_add(amount)
            .with_context(|| format!("{label} decrease overflow"))?;
        Ok(())
    }

    fn apply(self, value: u128, label: &str) -> Result<u128> {
        if self.increase >= self.decrease {
            value
                .checked_add(self.increase - self.decrease)
                .with_context(|| format!("{label} overflow"))
        } else {
            value
                .checked_sub(self.decrease - self.increase)
                .with_context(|| format!("{label} underflow"))
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CountDelta {
    increase: u64,
    decrease: u64,
}

impl CountDelta {
    fn apply(self, value: u64, label: &str) -> Result<u64> {
        if self.increase >= self.decrease {
            value
                .checked_add(self.increase - self.decrease)
                .with_context(|| format!("{label} overflow"))
        } else {
            value
                .checked_sub(self.decrease - self.increase)
                .with_context(|| format!("{label} underflow"))
        }
    }
}

#[derive(Debug, Default)]
struct DailyAccumulator {
    selected_transactions: u64,
    public_balance_changing_transactions: u64,
    public_owner_reassignment_transactions: u64,
    public_movement: u128,
    inferred_public_mint: u128,
    inferred_public_burn: u128,
    owner_deltas: HashMap<u32, AmountDelta>,
    active_token_account_delta: CountDelta,
    supply_delta: AmountDelta,
}

#[derive(Debug, Clone, Copy, Default)]
struct TransactionPublicChanges {
    positive: u128,
    negative: u128,
    amount_changed: bool,
    owner_changed: bool,
}

impl TransactionPublicChanges {
    fn record_amounts(&mut self, pre: u128, post: u128) -> Result<()> {
        match post.cmp(&pre) {
            Ordering::Greater => {
                self.positive = self
                    .positive
                    .checked_add(post - pre)
                    .context("transaction positive public delta overflow")?;
                self.amount_changed = true;
            }
            Ordering::Less => {
                self.negative = self
                    .negative
                    .checked_add(pre - post)
                    .context("transaction negative public delta overflow")?;
                self.amount_changed = true;
            }
            Ordering::Equal => {}
        }
        Ok(())
    }

    fn movement(self) -> u128 {
        self.positive.min(self.negative)
    }

    fn inferred_mint(self) -> u128 {
        self.positive.saturating_sub(self.negative)
    }

    fn inferred_burn(self) -> u128 {
        self.negative.saturating_sub(self.positive)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VolumeCandidate {
    movement: u128,
    inferred_mint: u128,
    inferred_burn: u128,
    source_epoch: u64,
    slot: u64,
    source_block_id: u32,
    tx_index: u32,
    signature_ordinal: u64,
    block_time: i64,
}

impl Ord for VolumeCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.movement
            .cmp(&other.movement)
            // For equal volume, the earlier canonical transaction ranks first.
            .then_with(|| other.source_epoch.cmp(&self.source_epoch))
            .then_with(|| other.slot.cmp(&self.slot))
            .then_with(|| other.source_block_id.cmp(&self.source_block_id))
            .then_with(|| other.tx_index.cmp(&self.tx_index))
    }
}

impl PartialOrd for VolumeCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
struct AddressCounter {
    raw_key: [u8; KEY_BYTES],
    registry_id: u32,
    kind: &'static str,
    transactions: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct HolderValue {
    amount: u128,
    account_count: u64,
}

#[derive(Debug, Default, serde::Serialize)]
struct TokenHistoryAuditCounters {
    transactions: u64,
    signatures: u64,
    transactions_with_target_balance_rows: u64,
    public_balance_changing_transactions: u64,
    public_owner_reassignment_transactions: u64,
    target_pre_balance_rows: u64,
    target_post_balance_rows: u64,
    implicit_zero_pre_rows: u64,
    implicit_zero_post_rows: u64,
    target_balance_rows_without_owner: u64,
    target_positive_states_without_owner: u64,
    transactions_without_block_time: u64,
    public_state_changes_without_block_time: u64,
    metadata_absent: u64,
    metadata_without_error: u64,
    metadata_current_only: u64,
    metadata_legacy_only: u64,
    metadata_both_same_target_balance_resolution: u64,
    address_signature_rows: u64,
    selected_transactions_without_target_address: u64,
}

#[derive(Debug, serde::Serialize)]
struct SourceArtifactReport {
    file: &'static str,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, serde::Serialize)]
struct TokenHistorySourceReport {
    mint: String,
    mint_slot: u64,
    first_epoch: u64,
    last_epoch: u64,
    transactions: u64,
    signatures: u64,
    registry_entries: u32,
    discovered_token_accounts: u64,
    total_dump_bytes: u64,
    manifest: SourceArtifactReport,
    transactions_file: SourceArtifactReport,
    signatures_file: SourceArtifactReport,
    registry_file: SourceArtifactReport,
    accounts_file: SourceArtifactReport,
}

#[derive(Debug, Clone, serde::Serialize)]
struct PublicAmountReport {
    raw_amount: String,
    base_units: String,
}

#[derive(Debug, serde::Serialize)]
struct ConcentrationReport {
    amount: PublicAmountReport,
    supply_fraction_numerator_raw: String,
    supply_fraction_denominator_raw: String,
    supply_share_parts_per_million_floor: u64,
}

#[derive(Debug, serde::Serialize)]
struct DailyPublicBalanceReport {
    utc_date: String,
    selected_transactions: u64,
    public_balance_changing_transactions: u64,
    public_owner_reassignment_transactions: u64,
    positive_public_balance_holders: u64,
    active_public_token_accounts: u64,
    public_raw_balance_sum: PublicAmountReport,
    public_bilateral_movement: PublicAmountReport,
    inferred_public_mint: PublicAmountReport,
    inferred_public_burn: PublicAmountReport,
    top_1_concentration: ConcentrationReport,
    top_10_concentration: ConcentrationReport,
    top_100_concentration: ConcentrationReport,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
struct FinalTopHolderHistorySourceBindingReport {
    mint: String,
    mint_slot: u64,
    first_epoch: u64,
    last_epoch: u64,
    manifest_sha256: String,
    transactions_sha256: String,
    signatures_sha256: String,
    registry_sha256: String,
    accounts_sha256: String,
}

impl FinalTopHolderHistorySourceBindingReport {
    fn from_report_source(source: &TokenHistorySourceReport) -> Self {
        Self {
            mint: source.mint.clone(),
            mint_slot: source.mint_slot,
            first_epoch: source.first_epoch,
            last_epoch: source.last_epoch,
            manifest_sha256: source.manifest.sha256.clone(),
            transactions_sha256: source.transactions_file.sha256.clone(),
            signatures_sha256: source.signatures_file.sha256.clone(),
            registry_sha256: source.registry_file.sha256.clone(),
            accounts_sha256: source.accounts_file.sha256.clone(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct FinalTopHolderHistoryCohortReport {
    selection_boundary: &'static str,
    maximum_holders: usize,
    selected_holders: usize,
    ranking: &'static str,
    tie_break: &'static str,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct FinalTopHolderHistoryDefinitionsReport {
    cohort: &'static str,
    daily_boundary: &'static str,
    calendar_dates: &'static str,
    source_boundary: &'static str,
    complete_utc_day: &'static str,
    balance_state_carried_forward: &'static str,
    raw_balance: &'static str,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct FinalTopHolderHistoryDayReport {
    utc_date: String,
    complete_utc_day: bool,
    source_boundary_start: bool,
    source_boundary_end: bool,
    observed_selected_transaction_day: bool,
    balance_state_carried_forward: bool,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct FinalTopHolderDailyBalanceSeriesReport {
    final_rank: usize,
    owner: String,
    final_raw_balance: String,
    daily_raw_balances: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
struct FinalTopHolderHistoryReport {
    source_binding: FinalTopHolderHistorySourceBindingReport,
    cohort: FinalTopHolderHistoryCohortReport,
    definitions: FinalTopHolderHistoryDefinitionsReport,
    days: Vec<FinalTopHolderHistoryDayReport>,
    series: Vec<FinalTopHolderDailyBalanceSeriesReport>,
}

#[derive(Debug, serde::Serialize)]
struct HolderReport {
    owner: String,
    token_account_count: u64,
    public_balance: PublicAmountReport,
}

#[derive(Debug, serde::Serialize)]
struct DistributionBandReport {
    base_unit_range: &'static str,
    holder_count: u64,
    public_balance: PublicAmountReport,
}

#[derive(Debug, serde::Serialize)]
struct FinalPublicBalanceReport {
    decimals: u8,
    positive_public_balance_holders: u64,
    active_public_token_accounts: u64,
    public_raw_balance_sum: PublicAmountReport,
    top_1_concentration: ConcentrationReport,
    top_10_concentration: ConcentrationReport,
    top_100_concentration: ConcentrationReport,
    largest_25_holders: Vec<HolderReport>,
    smallest_25_positive_holders: Vec<HolderReport>,
    balance_distribution: Vec<DistributionBandReport>,
}

#[derive(Debug, serde::Serialize)]
struct VolumeDayReport {
    utc_date: String,
    public_bilateral_movement: PublicAmountReport,
    inferred_public_mint: PublicAmountReport,
    inferred_public_burn: PublicAmountReport,
    selected_transactions: u64,
    public_balance_changing_transactions: u64,
}

#[derive(Debug, serde::Serialize)]
struct PublicVolumeTotalsReport {
    public_balance_changing_transactions: u64,
    public_owner_reassignment_transactions: u64,
    public_bilateral_movement: PublicAmountReport,
    inferred_public_mint: PublicAmountReport,
    inferred_public_burn: PublicAmountReport,
}

#[derive(Debug, serde::Serialize)]
struct VolumeTransactionReport {
    first_signature: String,
    source_epoch: u64,
    slot: u64,
    source_block_id: u32,
    tx_index: u32,
    block_time_unix_seconds: i64,
    utc_date: String,
    public_bilateral_movement: PublicAmountReport,
    inferred_public_mint: PublicAmountReport,
    inferred_public_burn: PublicAmountReport,
}

#[derive(Debug, serde::Serialize)]
struct AddressRequestReport {
    address: String,
    kind: &'static str,
    returned_address_signature_rows: u64,
    get_signatures_for_address_requests_at_limit_1000: u64,
    get_signatures_for_address_credit_pages_at_100: u64,
}

#[derive(Debug, serde::Serialize)]
struct RpcRequestModelReport {
    scope: &'static str,
    address_count: u64,
    mint_addresses: u64,
    token_account_addresses: u64,
    get_signatures_for_address_page_limit: u64,
    get_signatures_for_address_requests: u64,
    get_signatures_for_address_credit_page_size: u64,
    get_signatures_for_address_credit_pages: u64,
    returned_address_signature_rows: u64,
    duplicate_address_signature_rows_removed: u64,
    unique_get_transaction_calls: u64,
    total_rpc_requests: u64,
    per_address: Vec<AddressRequestReport>,
}

#[derive(Debug, serde::Serialize)]
struct TokenHistoryDefinitions {
    holder: &'static str,
    active_token_account: &'static str,
    public_bilateral_movement: &'static str,
    inferred_public_mint: &'static str,
    inferred_public_burn: &'static str,
    base_units: &'static str,
    daily_boundary: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct TokenHistoryLimitations {
    token_program: &'static str,
    scaled_ui_amount: &'static str,
    confidential_transfer: &'static str,
    holder_scope: &'static str,
    volume_scope: &'static str,
    rpc_model_scope: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct TokenHistoryReport {
    schema_version: u16,
    artifact_kind: &'static str,
    bounded_selected_dump_scan_complete: bool,
    metadata_balance_chain_continuous_from_spyx_mint_creation: bool,
    instruction_replay_performed: bool,
    daily_public_balance_series_complete: bool,
    daily_selected_transaction_counts_complete: bool,
    definitions: TokenHistoryDefinitions,
    limitations: TokenHistoryLimitations,
    source: TokenHistorySourceReport,
    audit: TokenHistoryAuditCounters,
    final_public_balance: FinalPublicBalanceReport,
    public_volume_totals: PublicVolumeTotalsReport,
    daily: Vec<DailyPublicBalanceReport>,
    final_top_100_holder_history: FinalTopHolderHistoryReport,
    top_25_volume_days: Vec<VolumeDayReport>,
    top_25_volume_transactions: Vec<VolumeTransactionReport>,
    rpc_request_model: RpcRequestModelReport,
}

fn format_base_units(raw: u128, decimals: u8) -> Result<String> {
    let scale = 10u128
        .checked_pow(u32::from(decimals))
        .context("token decimals exceed exact u128 formatting capacity")?;
    let whole = raw / scale;
    if decimals == 0 {
        return Ok(whole.to_string());
    }
    let fractional = raw % scale;
    Ok(format!(
        "{whole}.{fractional:0width$}",
        width = usize::from(decimals)
    ))
}

fn amount_report(raw: u128, decimals: u8) -> Result<PublicAmountReport> {
    Ok(PublicAmountReport {
        raw_amount: raw.to_string(),
        base_units: format_base_units(raw, decimals)?,
    })
}

fn positive_balance_crossing(before: u128, after: u128) -> i8 {
    match (before != 0, after != 0) {
        (false, true) => 1,
        (true, false) => -1,
        (false, false) | (true, true) => 0,
    }
}

fn concentration_amounts<I>(balances: I) -> Result<[u128; 3]>
where
    I: IntoIterator<Item = u128>,
{
    let mut top = BinaryHeap::<Reverse<u128>>::with_capacity(101);
    for balance in balances {
        if balance == 0 {
            continue;
        }
        if top.len() < 100 {
            top.push(Reverse(balance));
        } else if top.peek().is_some_and(|smallest| balance > smallest.0) {
            top.pop();
            top.push(Reverse(balance));
        }
    }
    let mut values = top.into_iter().map(|value| value.0).collect::<Vec<_>>();
    values.sort_unstable_by(|left, right| right.cmp(left));
    let top_1 = values.first().copied().unwrap_or(0);
    let top_10 = values.iter().take(10).try_fold(0u128, |sum, value| {
        sum.checked_add(*value).context("top-10 balance overflow")
    })?;
    let top_100 = values.iter().try_fold(0u128, |sum, value| {
        sum.checked_add(*value).context("top-100 balance overflow")
    })?;
    Ok([top_1, top_10, top_100])
}

fn concentration_report(amount: u128, supply: u128, decimals: u8) -> Result<ConcentrationReport> {
    ensure!(amount <= supply, "concentrated balance exceeds supply");
    let parts_per_million = if supply == 0 {
        0
    } else {
        u64::try_from(
            amount
                .checked_mul(1_000_000)
                .context("concentration ratio overflow")?
                .checked_div(supply)
                .context("concentration has a zero supply denominator")?,
        )?
    };
    Ok(ConcentrationReport {
        amount: amount_report(amount, decimals)?,
        supply_fraction_numerator_raw: amount.to_string(),
        supply_fraction_denominator_raw: supply.to_string(),
        supply_share_parts_per_million_floor: parts_per_million,
    })
}

fn utc_day_number(timestamp: i64) -> i64 {
    timestamp.div_euclid(86_400)
}

fn utc_date_from_day(day: i64) -> Result<String> {
    // Howard Hinnant's civil-from-days algorithm, with Unix day zero at
    // 1970-01-01. All arithmetic remains inside i64 for every i64 timestamp.
    let z = day
        .checked_add(719_468)
        .context("UTC civil-date offset overflow")?;
    let era = if z >= 0 { z } else { z - 146_096 }.div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day_of_month = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    ensure!(
        (0..=9_999).contains(&year),
        "UTC date is outside report year range"
    );
    Ok(format!("{year:04}-{month:02}-{day_of_month:02}"))
}

fn compact_optional_id(
    value: Option<CompactPubkey>,
    registry_entries: u32,
    invalid: &Cell<bool>,
) -> Option<u32> {
    match value {
        None => None,
        Some(CompactPubkey::Id(id)) if id != 0 && id <= registry_entries => Some(id),
        Some(CompactPubkey::Id(_)) | Some(CompactPubkey::Raw(_)) => {
            invalid.set(true);
            None
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_token_metadata_stage(
    stage: &mut TokenMetadataStage,
    bytes: &[u8],
    error_schema: ArchiveV2WireMetadataErrorSchema,
    message: &ProjectedArchiveV2MessageAccountSummary,
    registry_entries: u32,
    target_mint_id: u32,
    flags: u32,
) -> Result<ProjectedArchiveV2TokenMetadataSummary> {
    stage.clear();
    let total_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("resolved message-account count overflow")?;
    ensure!(
        total_accounts <= blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS,
        "resolved message-account count exceeds its format cap"
    );

    let invalid_balance_reference = Cell::new(false);
    let invalid_loaded_reference = Cell::new(false);
    let pre = &mut stage.pre;
    let post = &mut stage.post;
    let total_pre_rows = &mut stage.total_pre_rows;
    let total_post_rows = &mut stage.total_post_rows;
    let loaded_ids = &mut stage.loaded_ids;
    let summary = visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
        bytes,
        error_schema,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts: total_accounts,
            top_level_instruction_count: message.instruction_count,
        },
        registry_entries,
        LogPayloadValidation::StructureOnly,
        |_, _| {},
        |side, balance: BorrowedArchiveV2TokenBalance| {
            match side {
                TokenBalanceSide::Pre => *total_pre_rows += 1,
                TokenBalanceSide::Post => *total_post_rows += 1,
            }
            let mint_id =
                compact_optional_id(balance.mint, registry_entries, &invalid_balance_reference);
            let owner_id =
                compact_optional_id(balance.owner, registry_entries, &invalid_balance_reference);
            let _program_id = compact_optional_id(
                balance.program_id,
                registry_entries,
                &invalid_balance_reference,
            );
            if mint_id == Some(target_mint_id) {
                let row = PublicBalanceRow {
                    account_index: balance.account_index,
                    amount: u128::from(balance.amount),
                    owner_id,
                    decimals: balance.decimals,
                };
                match side {
                    TokenBalanceSide::Pre => pre.push(row),
                    TokenBalanceSide::Post => post.push(row),
                }
            }
        },
        |side, ordinal, reference| {
            let absolute = match side {
                ArchiveV2LoadedAddressSide::Writable => {
                    message.static_account_count.checked_add(ordinal)
                }
                ArchiveV2LoadedAddressSide::Readonly => message
                    .static_account_count
                    .checked_add(message.expected_loaded_writable)
                    .and_then(|start| start.checked_add(ordinal)),
            };
            let Some(absolute) = absolute.filter(|index| *index < loaded_ids.len()) else {
                invalid_loaded_reference.set(true);
                return;
            };
            match reference {
                CompactPubkey::Id(id) if id != 0 && id <= registry_entries => {
                    loaded_ids[absolute] = id;
                }
                CompactPubkey::Id(_) | CompactPubkey::Raw(_) => {
                    invalid_loaded_reference.set(true);
                }
            }
        },
    )?;
    ensure!(
        !invalid_balance_reference.get() && !invalid_loaded_reference.get(),
        "metadata contains an unresolved public-key reference"
    );
    ensure!(
        summary.pre_token_balance_count == *total_pre_rows
            && summary.post_token_balance_count == *total_post_rows,
        "token-balance callback count differs from metadata summary"
    );
    validate_inventory_metadata_summary(&summary, message, flags)?;
    let loaded_start = message.static_account_count;
    let loaded_end = loaded_start
        .checked_add(message.expected_loaded_writable)
        .and_then(|value| value.checked_add(message.expected_loaded_readonly))
        .context("loaded account boundary overflow")?;
    ensure!(
        loaded_ids[loaded_start..loaded_end]
            .iter()
            .all(|id| *id != 0),
        "metadata did not resolve every loaded message account"
    );
    pre.sort_unstable_by_key(|row| row.account_index);
    post.sort_unstable_by_key(|row| row.account_index);
    ensure!(
        pre.windows(2)
            .all(|pair| pair[0].account_index < pair[1].account_index)
            && post
                .windows(2)
                .all(|pair| pair[0].account_index < pair[1].account_index),
        "target mint has duplicate token-balance rows on one side"
    );
    Ok(summary)
}

fn resolve_message_account_id(
    account_index: u32,
    message: &ProjectedArchiveV2MessageAccountSummary,
    static_ids: &[u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    stage: &TokenMetadataStage,
) -> Result<u32> {
    let index = usize::try_from(account_index).context("message account index exceeds usize")?;
    let total = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|count| count.checked_add(message.expected_loaded_readonly))
        .context("resolved message-account count overflow")?;
    ensure!(
        index < total,
        "token-balance account index is outside message"
    );
    let id = if index < message.static_account_count {
        static_ids[index]
    } else {
        stage.loaded_ids[index]
    };
    ensure!(id != 0, "token-balance account index was not resolved");
    Ok(id)
}

fn record_owner_delta(
    deltas: &mut HashMap<u32, AmountDelta>,
    owner_id: u32,
    amount: u128,
    increase: bool,
) -> Result<()> {
    if amount == 0 {
        return Ok(());
    }
    let delta = deltas.entry(owner_id).or_default();
    if increase {
        delta.add_increase(amount, "daily owner balance")
    } else {
        delta.add_decrease(amount, "daily owner balance")
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_public_balance_pair(
    account_ordinal: usize,
    pre: Option<PublicBalanceRow>,
    post: Option<PublicBalanceRow>,
    states: &mut [TokenAccountState],
    day: &mut DailyAccumulator,
    changes: &mut TransactionPublicChanges,
    audit: &mut TokenHistoryAuditCounters,
    decimals: &mut Option<u8>,
) -> Result<()> {
    let state = states
        .get_mut(account_ordinal)
        .context("target token-account ordinal is outside state table")?;
    let pre_amount = pre.map_or(0, |row| row.amount);
    let post_amount = post.map_or(0, |row| row.amount);
    if pre.is_none() {
        checked_increment(&mut audit.implicit_zero_pre_rows, "implicit pre-zero count")?;
    }
    if post.is_none() {
        checked_increment(
            &mut audit.implicit_zero_post_rows,
            "implicit post-zero count",
        )?;
    }
    for row in pre.into_iter().chain(post) {
        if row.owner_id.is_none() {
            checked_increment(
                &mut audit.target_balance_rows_without_owner,
                "target balance row without owner count",
            )?;
        }
        match *decimals {
            None => *decimals = Some(row.decimals),
            Some(value) => ensure!(
                value == row.decimals,
                "target mint token-balance decimals changed"
            ),
        }
    }

    ensure!(
        state.amount == pre_amount,
        "public token-account state differs from transaction pre-balance"
    );
    let explicit_pre_owner = pre.and_then(|row| row.owner_id);
    let explicit_post_owner = post.and_then(|row| row.owner_id);
    let pre_owner = explicit_pre_owner.or(state.owner_id);
    if pre_amount != 0 {
        let resolved = pre_owner.context("positive pre-balance has no resolvable owner")?;
        ensure!(
            state.owner_id.is_none_or(|owner| owner == resolved),
            "public token-account owner differs from stored pre-state"
        );
    }
    let post_owner = explicit_post_owner.or(explicit_pre_owner).or({
        if pre_amount != 0 {
            state.owner_id
        } else {
            None
        }
    });
    if post_amount != 0 && post_owner.is_none() {
        checked_increment(
            &mut audit.target_positive_states_without_owner,
            "positive target state without owner count",
        )?;
        bail!("positive post-balance has no resolvable owner");
    }

    if let Some(owner) = pre_owner {
        record_owner_delta(&mut day.owner_deltas, owner, pre_amount, false)?;
    }
    if let Some(owner) = post_owner {
        record_owner_delta(&mut day.owner_deltas, owner, post_amount, true)?;
    }
    match positive_balance_crossing(pre_amount, post_amount) {
        1 => checked_increment(
            &mut day.active_token_account_delta.increase,
            "daily active token-account increase",
        )?,
        -1 => checked_increment(
            &mut day.active_token_account_delta.decrease,
            "daily active token-account decrease",
        )?,
        0 => {}
        _ => unreachable!("positive-balance crossing has a fixed range"),
    }
    day.supply_delta
        .add_decrease(pre_amount, "daily public supply")?;
    day.supply_delta
        .add_increase(post_amount, "daily public supply")?;
    changes.record_amounts(pre_amount, post_amount)?;
    if pre_owner != post_owner && (pre_amount != 0 || post_amount != 0) {
        changes.owner_changed = true;
    }
    state.amount = post_amount;
    state.owner_id = post_owner.or(pre_owner);
    Ok(())
}

fn retain_volume_candidate(
    heap: &mut BinaryHeap<Reverse<VolumeCandidate>>,
    candidate: VolumeCandidate,
) {
    if candidate.movement == 0 {
        return;
    }
    if heap.len() < TOP_VOLUME_LIMIT {
        heap.push(Reverse(candidate));
    } else if heap.peek().is_some_and(|worst| candidate > worst.0) {
        heap.pop();
        heap.push(Reverse(candidate));
    }
}

fn serialize_holders(
    holders: &[(u32, HolderValue)],
    registry: &[u8],
    decimals: u8,
) -> Result<Vec<HolderReport>> {
    holders
        .iter()
        .map(|(owner_id, value)| {
            Ok(HolderReport {
                owner: bs58::encode(registry_key_at(registry, *owner_id)?).into_string(),
                token_account_count: value.account_count,
                public_balance: amount_report(value.amount, decimals)?,
            })
        })
        .collect()
}

fn build_final_top_100_holder_history(
    daily: &BTreeMap<i64, DailyAccumulator>,
    final_holders: &[(u32, HolderValue)],
    registry: &[u8],
    expected_final_supply: u128,
    expected_final_top_100_amount: u128,
    source_binding: FinalTopHolderHistorySourceBindingReport,
) -> Result<FinalTopHolderHistoryReport> {
    for pair in final_holders.windows(2) {
        let (left_id, left) = pair[0];
        let (right_id, right) = pair[1];
        let order = right.amount.cmp(&left.amount).then_with(|| {
            registry_key_at(registry, left_id)
                .expect("validated owner registry ID")
                .cmp(&registry_key_at(registry, right_id).expect("validated owner registry ID"))
        });
        ensure!(
            order != Ordering::Greater,
            "final holder history input is not ordered by raw balance and owner bytes"
        );
    }

    let selected_holders = final_holders.len().min(FINAL_TOP_HOLDER_HISTORY_LIMIT);
    let mut selected_index_by_owner = HashMap::<u32, usize>::with_capacity(selected_holders);
    let mut selected_running_balances = vec![0u128; selected_holders];
    let mut series = Vec::new();
    series
        .try_reserve_exact(selected_holders)
        .context("reserve final top-holder history series")?;
    for (index, &(owner_id, holder)) in final_holders.iter().take(selected_holders).enumerate() {
        ensure!(
            holder.amount != 0,
            "final top-holder history cohort contains a zero balance"
        );
        ensure!(
            selected_index_by_owner.insert(owner_id, index).is_none(),
            "final top-holder history cohort contains a duplicate owner"
        );
        series.push(FinalTopHolderDailyBalanceSeriesReport {
            final_rank: index + 1,
            owner: bs58::encode(registry_key_at(registry, owner_id)?).into_string(),
            final_raw_balance: holder.amount.to_string(),
            daily_raw_balances: Vec::new(),
        });
    }

    let mut days = Vec::new();
    let mut running_owner_balances = HashMap::<u32, u128>::new();
    let mut running_owner_supply = 0u128;
    let mut running_supply = 0u128;
    if let (Some((&first_day, _)), Some((&last_day, _))) =
        (daily.first_key_value(), daily.last_key_value())
    {
        let calendar_day_count = usize::try_from(
            i128::from(last_day)
                .checked_sub(i128::from(first_day))
                .and_then(|span| span.checked_add(1))
                .context("final top-holder calendar span overflow")?,
        )
        .context("final top-holder calendar span exceeds addressable memory")?;
        days.try_reserve_exact(calendar_day_count)
            .context("reserve final top-holder history calendar")?;
        for holder in &mut series {
            holder
                .daily_raw_balances
                .try_reserve_exact(calendar_day_count)
                .context("reserve final top-holder daily balances")?;
        }

        for day_number in first_day..=last_day {
            let observed = daily.get(&day_number);
            if let Some(value) = observed {
                for (&owner_id, &delta) in &value.owner_deltas {
                    let previous = running_owner_balances.get(&owner_id).copied().unwrap_or(0);
                    let next =
                        delta.apply(previous, "final top-holder full daily owner balance")?;
                    if next >= previous {
                        running_owner_supply = running_owner_supply
                            .checked_add(next - previous)
                            .context("final top-holder reconstructed daily supply overflow")?;
                    } else {
                        running_owner_supply = running_owner_supply
                            .checked_sub(previous - next)
                            .context("final top-holder reconstructed daily supply underflow")?;
                    }
                    if next == 0 {
                        running_owner_balances.remove(&owner_id);
                    } else {
                        running_owner_balances.insert(owner_id, next);
                    }
                    if let Some(&selected_index) = selected_index_by_owner.get(&owner_id) {
                        selected_running_balances[selected_index] = delta.apply(
                            selected_running_balances[selected_index],
                            "final top-holder selected daily owner balance",
                        )?;
                    }
                }
                running_supply = value
                    .supply_delta
                    .apply(running_supply, "final top-holder daily public supply")?;
            }

            ensure!(
                running_owner_supply == running_supply,
                "final top-holder daily owner balances do not match the existing daily supply"
            );
            for (index, holder) in series.iter_mut().enumerate() {
                let owner_id = final_holders[index].0;
                let reconstructed_balance =
                    running_owner_balances.get(&owner_id).copied().unwrap_or(0);
                ensure!(
                    selected_running_balances[index] == reconstructed_balance,
                    "final top-holder selected daily balance differs from full owner reconstruction"
                );
                holder
                    .daily_raw_balances
                    .push(reconstructed_balance.to_string());
            }

            let source_boundary_start = day_number == first_day;
            let source_boundary_end = day_number == last_day;
            days.push(FinalTopHolderHistoryDayReport {
                utc_date: utc_date_from_day(day_number)?,
                complete_utc_day: !source_boundary_start && !source_boundary_end,
                source_boundary_start,
                source_boundary_end,
                observed_selected_transaction_day: observed.is_some(),
                balance_state_carried_forward: observed.is_none(),
            });
        }
    }

    let mut expected_final_owner_balances = HashMap::<u32, u128>::new();
    expected_final_owner_balances
        .try_reserve(final_holders.len())
        .context("reserve expected final holder balances")?;
    let mut final_holder_supply = 0u128;
    for &(owner_id, holder) in final_holders {
        ensure!(
            holder.amount != 0,
            "final holder history input contains a zero balance"
        );
        ensure!(
            expected_final_owner_balances
                .insert(owner_id, holder.amount)
                .is_none(),
            "final holder history input contains a duplicate owner"
        );
        final_holder_supply = final_holder_supply
            .checked_add(holder.amount)
            .context("final holder history supply overflow")?;
    }
    ensure!(
        final_holder_supply == expected_final_supply
            && running_supply == expected_final_supply
            && running_owner_balances == expected_final_owner_balances,
        "final top-holder history reconstruction differs from existing final holder totals"
    );

    let cohort_final_amount = final_holders
        .iter()
        .take(FINAL_TOP_HOLDER_HISTORY_LIMIT)
        .try_fold(0u128, |sum, (_, holder)| {
            sum.checked_add(holder.amount)
                .context("final top-holder cohort amount overflow")
        })?;
    ensure!(
        cohort_final_amount == expected_final_top_100_amount,
        "final top-holder cohort amount differs from existing top-100 concentration"
    );
    let series_final_amount = series.iter().try_fold(0u128, |sum, holder| {
        let final_point = holder
            .daily_raw_balances
            .last()
            .map(String::as_str)
            .unwrap_or("0");
        ensure!(
            final_point == holder.final_raw_balance,
            "final top-holder series point differs from final holder balance"
        );
        let final_point = final_point
            .parse::<u128>()
            .context("final top-holder raw balance is not an integer")?;
        sum.checked_add(final_point)
            .context("final top-holder series amount overflow")
    })?;
    ensure!(
        series_final_amount == expected_final_top_100_amount,
        "final top-holder series amount differs from existing top-100 concentration"
    );
    ensure!(
        series
            .iter()
            .all(|holder| holder.daily_raw_balances.len() == days.len()),
        "final top-holder series does not align with its calendar"
    );

    Ok(FinalTopHolderHistoryReport {
        source_binding,
        cohort: FinalTopHolderHistoryCohortReport {
            selection_boundary: "final_public_balance_at_dump_boundary",
            maximum_holders: FINAL_TOP_HOLDER_HISTORY_LIMIT,
            selected_holders,
            ranking: "positive_public_raw_balance_descending",
            tie_break: "raw_32_byte_owner_pubkey_ascending",
        },
        definitions: FinalTopHolderHistoryDefinitionsReport {
            cohort: "one fixed cohort selected at the final dump boundary; it is not recalculated for each day",
            daily_boundary: "balance after all selected transactions assigned to the UTC calendar day",
            calendar_dates: "every UTC calendar date from the first through the last dated selected transaction, inclusive",
            source_boundary: "source_boundary_start and source_boundary_end mark the first and last dated days in the selected dump",
            complete_utc_day: "false on source-boundary days because coverage outside the observed source interval on those UTC dates is not proven; true on dates strictly between them",
            balance_state_carried_forward: "true only when no selected transaction is assigned to that date and the prior end-of-day balance is repeated",
            raw_balance: "exact public raw token-account amount; no decimal or Token-2022 display multiplier is applied",
        },
        days,
        series,
    })
}

fn build_distribution(
    holders: &[(u32, HolderValue)],
    decimals: u8,
) -> Result<Vec<DistributionBandReport>> {
    const LABELS: [&str; 8] = [
        "greater_than_0_and_less_than_1",
        "1_to_less_than_10",
        "10_to_less_than_100",
        "100_to_less_than_1000",
        "1000_to_less_than_10000",
        "10000_to_less_than_100000",
        "100000_to_less_than_1000000",
        "1000000_or_more",
    ];
    let scale = 10u128
        .checked_pow(u32::from(decimals))
        .context("token decimals exceed distribution capacity")?;
    let boundaries =
        [1u128, 10, 100, 1_000, 10_000, 100_000, 1_000_000].map(|value| value.checked_mul(scale));
    ensure!(
        boundaries.iter().all(Option::is_some),
        "balance distribution boundary overflow"
    );
    let boundaries = boundaries.map(Option::unwrap);
    let mut counts = [0u64; 8];
    let mut amounts = [0u128; 8];
    for (_, holder) in holders {
        let index = boundaries.partition_point(|boundary| holder.amount >= *boundary);
        checked_increment(&mut counts[index], "distribution holder count")?;
        amounts[index] = amounts[index]
            .checked_add(holder.amount)
            .context("distribution balance overflow")?;
    }
    LABELS
        .into_iter()
        .enumerate()
        .map(|(index, label)| {
            Ok(DistributionBandReport {
                base_unit_range: label,
                holder_count: counts[index],
                public_balance: amount_report(amounts[index], decimals)?,
            })
        })
        .collect()
}

fn read_signature_at(file: &File, ordinal: u64) -> Result<[u8; SIGNATURE_BYTES]> {
    let mut signature = [0u8; SIGNATURE_BYTES];
    read_exact_at(
        file,
        &mut signature,
        ordinal
            .checked_mul(SIGNATURE_BYTES as u64)
            .context("signature byte offset overflow")?,
    )?;
    Ok(signature)
}

fn report_progress(
    started: Instant,
    transactions: u64,
    total_transactions: u64,
    logical_bytes: u64,
) {
    inventory_progress(
        "token history report",
        started,
        transactions,
        total_transactions,
        logical_bytes,
    );
}

/// Build an exact public raw-balance history from one completed schema-3 dump.
///
/// The large transaction stream is read and SHA-256 hashed exactly once. The
/// frame payload and all message/metadata staging buffers are reused.
pub(super) fn build_consolidated_token_history_report_v3(dump: &Path, report: &Path) -> Result<()> {
    let started = Instant::now();
    let dump = fs::canonicalize(dump)
        .with_context(|| format!("resolve consolidated dump {}", dump.display()))?;
    ensure!(dump.is_dir(), "consolidated dump is not a directory");
    validate_exact_final_files(&dump)?;

    let report_parent = report.parent().unwrap_or_else(|| Path::new("."));
    let report_parent = if report_parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        report_parent
    };
    let canonical_report_parent = fs::canonicalize(report_parent)
        .with_context(|| format!("resolve report directory {}", report_parent.display()))?;
    ensure!(
        canonical_report_parent != dump,
        "token history report must not modify the immutable dump directory"
    );
    let report_name = report
        .file_name()
        .context("token history report path has no file name")?;
    ensure!(
        !canonical_report_parent.join(report_name).exists(),
        "refusing to replace an existing token history report"
    );

    let manifest_path = dump.join(DUMP_MANIFEST_FILE);
    let manifest_bytes = read_bounded_regular(&manifest_path, MAX_ROOT_MANIFEST_BYTES)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);
    let manifest: DumpManifest = serde_json::from_slice(&manifest_bytes)?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::Consolidated
            && manifest.complete
            && manifest.workers != 0
            && manifest.first_epoch <= manifest.last_epoch
            && manifest.transactions != 0,
        "invalid consolidated manifest header"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE
            && manifest.signature_stream.as_deref() == Some(DUMP_SIGNATURES_FILE)
            && manifest.pubkey_registry.as_deref() == Some(PUBKEY_REGISTRY_FILE)
            && manifest.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE)
            && manifest.account_id_log.is_none()
            && manifest.account_id_log_sha256.is_none()
            && manifest.registry_maps.is_none(),
        "consolidated manifest file bindings differ"
    );
    validate_source_binding(&manifest.source_binding)?;
    let expected_transaction_sha256 = parse_hex_digest(
        manifest
            .transaction_stream_sha256
            .as_deref()
            .context("missing transaction digest")?,
        "transaction digest",
    )?;
    let expected_signature_sha256 = parse_hex_digest(
        manifest
            .signature_stream_sha256
            .as_deref()
            .context("missing signature digest")?,
        "signature digest",
    )?;
    let expected_registry_sha256 = parse_hex_digest(
        manifest
            .pubkey_registry_sha256
            .as_deref()
            .context("missing registry digest")?,
        "registry digest",
    )?;
    let expected_account_sha256 = parse_hex_digest(
        manifest
            .discovered_accounts_sha256
            .as_deref()
            .context("missing account digest")?,
        "account digest",
    )?;
    let expected_signatures = manifest.signatures.context("missing signature count")?;
    let expected_registry_rows = manifest.pubkeys.context("missing public-key count")?;
    ensure!(
        expected_registry_rows != 0 && expected_registry_rows < u64::from(u32::MAX),
        "invalid registry row count"
    );
    let registry_entries = u32::try_from(expected_registry_rows)?;

    let expected_registry_bytes = expected_registry_rows
        .checked_mul(KEY_BYTES as u64)
        .context("registry byte length overflow")?;
    let registry_path = dump.join(PUBKEY_REGISTRY_FILE);
    let registry = read_bounded_regular(&registry_path, expected_registry_bytes)?;
    ensure!(
        u64::try_from(registry.len())? == expected_registry_bytes
            && sha256_bytes(&registry) == expected_registry_sha256
            && registry
                .chunks_exact(KEY_BYTES)
                .zip(registry.chunks_exact(KEY_BYTES).skip(1))
                .all(|(left, right)| left < right),
        "registry differs from its manifest or is not strictly sorted"
    );

    let target = TargetBinding {
        mint: parse_pubkey(&manifest.mint, "mint")?,
        mint_slot: manifest.mint_slot,
        mint_signature: parse_signature(&manifest.mint_signature)?,
    };
    let target_mint_id = registry_id_for_key(&registry, &target.mint)
        .context("target mint is absent from the consolidated registry")?;

    let accounts_path = dump.join(ACCOUNTS_FILE);
    let account_bytes = read_bounded_regular(
        &accounts_path,
        ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES as u64,
    )?;
    ensure!(
        sha256_bytes(&account_bytes) == expected_account_sha256,
        "account digest differs from its manifest"
    );
    let accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
        &account_bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )?;
    ensure!(
        accounts.schema_version == DUMP_SCHEMA_VERSION
            && accounts.mint == target.mint
            && accounts.anchor_position.slot == target.mint_slot
            && accounts.anchor_position.signature_count != 0
            && (manifest.first_epoch..=manifest.last_epoch)
                .contains(&accounts.anchor_position.epoch)
            && accounts
                .accounts
                .windows(2)
                .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey)
            && accounts.accounts.iter().all(|account| {
                account.raw_pubkey != target.mint
                    && account.first_creation.slot >= target.mint_slot
                    && (manifest.first_epoch..=manifest.last_epoch)
                        .contains(&account.first_creation.epoch)
            })
            && manifest.discovered_account_count == Some(accounts.accounts.len() as u64),
        "frozen account artifact is invalid"
    );

    let mut account_ordinal_by_registry_id = vec![u32::MAX; usize::try_from(registry_entries)? + 1];
    let mut addresses = Vec::new();
    addresses
        .try_reserve_exact(
            accounts
                .accounts
                .len()
                .checked_add(1)
                .context("address count overflow")?,
        )
        .context("reserve RPC address counters")?;
    addresses.push(AddressCounter {
        raw_key: target.mint,
        registry_id: target_mint_id,
        kind: "mint",
        transactions: 0,
    });
    for account in &accounts.accounts {
        let registry_id = registry_id_for_key(&registry, &account.raw_pubkey)
            .context("discovered token account is absent from the dump registry")?;
        addresses.push(AddressCounter {
            raw_key: account.raw_pubkey,
            registry_id,
            kind: "token_account",
            transactions: 0,
        });
    }
    addresses.sort_unstable_by_key(|address| address.raw_key);
    for (index, address) in addresses.iter().enumerate() {
        let dense = account_ordinal_by_registry_id
            .get_mut(usize::try_from(address.registry_id)?)
            .context("address registry ID is outside dense address table")?;
        ensure!(*dense == u32::MAX, "duplicate target RPC address");
        *dense = u32::try_from(index).context("target address count exceeds u32")?;
    }
    let token_account_ordinal_by_registry_id = {
        let mut dense = vec![u32::MAX; usize::try_from(registry_entries)? + 1];
        for (index, account) in accounts.accounts.iter().enumerate() {
            let registry_id = registry_id_for_key(&registry, &account.raw_pubkey)
                .context("discovered token account is absent from registry")?;
            dense[usize::try_from(registry_id)?] =
                u32::try_from(index).context("token-account count exceeds u32")?;
        }
        dense
    };
    let mut account_states = vec![TokenAccountState::default(); accounts.accounts.len()];

    let signature_path = dump.join(DUMP_SIGNATURES_FILE);
    let expected_signature_bytes = expected_signatures
        .checked_mul(SIGNATURE_BYTES as u64)
        .context("signature byte length overflow")?;
    let signature_file = File::open(&signature_path)?;
    let signature_stamp = FileStamp::read(&signature_file)?;
    ensure!(
        signature_stamp.bytes == expected_signature_bytes,
        "signature sidecar size differs from its manifest"
    );
    ensure!(
        hash_regular_file(&signature_path, expected_signature_bytes)? == expected_signature_sha256,
        "signature digest differs from its manifest"
    );
    signature_stamp.verify(&signature_file, "consolidated signature sidecar")?;
    verify_path_binding(
        &signature_path,
        &signature_stamp,
        "consolidated signature sidecar",
    )?;

    let transaction_path = dump.join(TRANSACTIONS_FILE);
    let transaction_file = File::open(&transaction_path)?;
    let transaction_stamp = FileStamp::read(&transaction_file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, transaction_file);
    let mut transaction_hasher = Sha256::new();
    let mut logical_offset = 0u64;
    let mut payload = Vec::new();
    read_frame_hashed(
        &mut reader,
        &mut logical_offset,
        &mut transaction_hasher,
        &mut payload,
    )?
    .context("consolidated transaction stream is empty")?;
    let BorrowedDumpRecord::Header(header) = decode_borrowed_frame(&payload)? else {
        bail!("consolidated transaction stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::Consolidated
            && header.mint == target.mint
            && header.mint_slot == target.mint_slot
            && header.mint_signature == target.mint_signature
            && header.source_epoch.is_none()
            && header.source_generation_digest.is_none()
            && header.source_wire_profile.is_none()
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "consolidated stream header differs from its manifest"
    );

    let mut audit = TokenHistoryAuditCounters::default();
    let mut current_stage = TokenMetadataStage::new();
    let mut legacy_stage = TokenMetadataStage::new();
    let mut static_ids = [0u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
    let mut touched_addresses =
        Vec::<u32>::with_capacity(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS);
    let mut daily = BTreeMap::<i64, DailyAccumulator>::new();
    let mut top_volume_transactions = BinaryHeap::<Reverse<VolumeCandidate>>::new();
    let mut decimals = None;
    let mut previous_coordinate = None;
    let mut previous_slot = None::<(u64, u64, u32, BlockIdentity)>;
    let mut anchor_count = 0u64;

    let footer = loop {
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut transaction_hasher,
            &mut payload,
        )?
        .context("consolidated transaction stream has no footer")?;
        match decode_borrowed_frame(&payload)? {
            BorrowedDumpRecord::Header(_) => {
                bail!("consolidated transaction stream repeats its header")
            }
            BorrowedDumpRecord::Footer(footer) => break footer,
            BorrowedDumpRecord::Transaction(record) => {
                let coordinate = ProgramInventoryCoordinate::from_record(&record);
                ensure!(
                    previous_coordinate
                        .is_none_or(|previous| previous < coordinate.canonical_key()),
                    "consolidated transactions are not in canonical order"
                );
                previous_coordinate = Some(coordinate.canonical_key());
                ensure!(
                    (manifest.first_epoch..=manifest.last_epoch).contains(&record.source_epoch)
                        && record.block.slot >= target.mint_slot
                        && record.block.parent_slot < record.block.slot
                        && record.block.transaction_count != 0
                        && record.tx_index < record.block.transaction_count
                        && record.signature_count != 0
                        && !record.message_bytes.is_empty()
                        && record.flags & !ARCHIVE_V2_TX_KNOWN_FLAGS == 0
                        && record.flags
                            & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                                | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                            == 0,
                    "consolidated transaction has invalid source fields"
                );
                let DumpSourceBinding::TrustedLocalSizesOnly {
                    slots_per_epoch,
                    wire_profile,
                    ..
                } = &manifest.source_binding;
                let first_slot = record
                    .source_epoch
                    .checked_mul(*slots_per_epoch)
                    .context("source epoch first slot overflow")?;
                ensure!(
                    record.source_wire_profile == *wire_profile
                        && record.block.slot >= first_slot
                        && record.block.slot - first_slot < *slots_per_epoch
                        && u64::from(record.source_block_id) < *slots_per_epoch,
                    "consolidated transaction differs from its trusted source binding"
                );
                ensure!(
                    (record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0)
                        == !record.metadata_bytes.is_empty()
                        && (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                            == (record.metadata_bytes.first() == Some(&1)),
                    "consolidated transaction flags differ from metadata bytes"
                );
                ensure!(
                    record.dump_signature_ordinal == Some(audit.signatures),
                    "consolidated signature ordinals are not contiguous"
                );
                record
                    .source_first_signature_ordinal
                    .checked_add(u64::from(record.signature_count))
                    .context("source signature range overflow")?;
                let identity = BlockIdentity::from(&record.block);
                if let Some((epoch, slot, block_id, previous_identity)) = previous_slot
                    && epoch == record.source_epoch
                    && slot == record.block.slot
                {
                    ensure!(
                        block_id == record.source_block_id && previous_identity == identity,
                        "one source slot has conflicting block context"
                    );
                }
                previous_slot = Some((
                    record.source_epoch,
                    record.block.slot,
                    record.source_block_id,
                    identity,
                ));

                let is_anchor = coordinate.canonical_key()
                    == (
                        accounts.anchor_position.epoch,
                        accounts.anchor_position.slot,
                        accounts.anchor_position.source_block_id,
                        accounts.anchor_position.tx_index,
                    );
                if is_anchor {
                    ensure!(
                        record.source_first_signature_ordinal
                            == accounts.anchor_position.source_first_signature_ordinal
                            && record.signature_count == accounts.anchor_position.signature_count
                            && read_signature_at(&signature_file, audit.signatures)?
                                == target.mint_signature,
                        "mint anchor signature binding differs"
                    );
                }
                anchor_count = anchor_count
                    .checked_add(u64::from(is_anchor))
                    .context("anchor count overflow")?;

                static_ids.fill(0);
                let invalid_static_key = Cell::new(false);
                let mut static_count = 0usize;
                let message = projector(record.source_wire_profile)
                    .visit_static_accounts_exact(
                        record.message_bytes,
                        registry_entries,
                        |ordinal, reference| {
                            static_count = ordinal + 1;
                            match reference {
                                CompactPubkey::Id(id) if id != 0 && id <= registry_entries => {
                                    static_ids[ordinal] = id;
                                }
                                CompactPubkey::Id(_) | CompactPubkey::Raw(_) => {
                                    invalid_static_key.set(true);
                                }
                            }
                        },
                    )
                    .with_context(|| {
                        format!(
                            "decode message at epoch {} slot {} transaction {}",
                            record.source_epoch, record.block.slot, record.tx_index
                        )
                    })?;
                ensure!(
                    !invalid_static_key.get() && static_count == message.static_account_count,
                    "message contains an unresolved static key"
                );
                validate_inventory_message_summary(&message, record.flags, record.signature_count)?;

                let selected_stage = if record.metadata_bytes.is_empty() {
                    checked_increment(&mut audit.metadata_absent, "metadata-absent count")?;
                    validate_inventory_absent_metadata(&message, record.flags)?;
                    current_stage.clear();
                    &current_stage
                } else if record.metadata_bytes.first() == Some(&0) {
                    parse_token_metadata_stage(
                        &mut current_stage,
                        record.metadata_bytes,
                        ArchiveV2WireMetadataErrorSchema::Current,
                        &message,
                        registry_entries,
                        target_mint_id,
                        record.flags,
                    )?;
                    checked_increment(
                        &mut audit.metadata_without_error,
                        "metadata-without-error count",
                    )?;
                    &current_stage
                } else {
                    let current_summary =
                        validate_archive_v2_metadata_error_prefix_for_selected_schema(
                            record.metadata_bytes,
                            ArchiveV2WireMetadataErrorSchema::Current,
                            record.metadata_bytes.len(),
                        )
                        .is_ok()
                        .then(|| {
                            parse_token_metadata_stage(
                                &mut current_stage,
                                record.metadata_bytes,
                                ArchiveV2WireMetadataErrorSchema::Current,
                                &message,
                                registry_entries,
                                target_mint_id,
                                record.flags,
                            )
                        })
                        .transpose()
                        .ok()
                        .flatten();
                    let legacy_summary =
                        validate_archive_v2_metadata_error_prefix_for_selected_schema(
                            record.metadata_bytes,
                            ArchiveV2WireMetadataErrorSchema::Legacy,
                            record.metadata_bytes.len(),
                        )
                        .is_ok()
                        .then(|| {
                            parse_token_metadata_stage(
                                &mut legacy_stage,
                                record.metadata_bytes,
                                ArchiveV2WireMetadataErrorSchema::Legacy,
                                &message,
                                registry_entries,
                                target_mint_id,
                                record.flags,
                            )
                        })
                        .transpose()
                        .ok()
                        .flatten();
                    ensure!(
                        current_summary.is_some() || legacy_summary.is_some(),
                        "metadata is invalid under both selected error schemas at epoch {} slot {} transaction {}",
                        record.source_epoch,
                        record.block.slot,
                        record.tx_index
                    );
                    match (current_summary, legacy_summary) {
                        (Some(_), None) => {
                            checked_increment(
                                &mut audit.metadata_current_only,
                                "current-only metadata count",
                            )?;
                            &current_stage
                        }
                        (None, Some(_)) => {
                            checked_increment(
                                &mut audit.metadata_legacy_only,
                                "legacy-only metadata count",
                            )?;
                            &legacy_stage
                        }
                        (Some(current), Some(legacy)) => {
                            let current_tail =
                                validate_archive_v2_metadata_error_prefix_for_selected_schema(
                                    record.metadata_bytes,
                                    ArchiveV2WireMetadataErrorSchema::Current,
                                    record.metadata_bytes.len(),
                                )
                                .context("revalidate dual-valid current metadata prefix")?;
                            let legacy_tail =
                                validate_archive_v2_metadata_error_prefix_for_selected_schema(
                                    record.metadata_bytes,
                                    ArchiveV2WireMetadataErrorSchema::Legacy,
                                    record.metadata_bytes.len(),
                                )
                                .context("revalidate dual-valid legacy metadata prefix")?;
                            ensure!(
                                current_tail.bytes == legacy_tail.bytes
                                    && current_tail.error_index == legacy_tail.error_index
                                    && current == legacy
                                    && current_stage == legacy_stage,
                                "dual-valid metadata resolves to divergent target balances at epoch {} slot {} transaction {}",
                                record.source_epoch,
                                record.block.slot,
                                record.tx_index
                            );
                            checked_increment(
                                &mut audit.metadata_both_same_target_balance_resolution,
                                "dual-valid same-target-balance metadata count",
                            )?;
                            &current_stage
                        }
                        (None, None) => unreachable!("both-invalid metadata was rejected"),
                    }
                };

                touched_addresses.clear();
                let total_message_accounts = message
                    .static_account_count
                    .checked_add(message.expected_loaded_writable)
                    .and_then(|count| count.checked_add(message.expected_loaded_readonly))
                    .context("message account count overflow")?;
                for &registry_id in static_ids[..message.static_account_count].iter().chain(
                    selected_stage.loaded_ids[message.static_account_count..total_message_accounts]
                        .iter(),
                ) {
                    ensure!(registry_id != 0, "message account was not resolved");
                    let address_index =
                        account_ordinal_by_registry_id[usize::try_from(registry_id)?];
                    if address_index != u32::MAX {
                        touched_addresses.push(address_index);
                    }
                }
                touched_addresses.sort_unstable();
                touched_addresses.dedup();
                if touched_addresses.is_empty() {
                    checked_increment(
                        &mut audit.selected_transactions_without_target_address,
                        "selected transaction without target address count",
                    )?;
                    bail!(
                        "selected transaction does not reference the mint or a discovered token account"
                    );
                }
                for &address_index in &touched_addresses {
                    checked_increment(
                        &mut addresses[usize::try_from(address_index)?].transactions,
                        "per-address transaction count",
                    )?;
                    checked_increment(
                        &mut audit.address_signature_rows,
                        "address-signature row count",
                    )?;
                }

                if record.block.block_time.is_none() {
                    checked_increment(
                        &mut audit.transactions_without_block_time,
                        "transaction without block time count",
                    )?;
                }
                let day_number = record.block.block_time.map(utc_day_number);
                let mut undated = DailyAccumulator::default();
                let day = match day_number {
                    Some(value) => daily.entry(value).or_default(),
                    None => &mut undated,
                };
                checked_increment(
                    &mut day.selected_transactions,
                    "daily selected transaction count",
                )?;

                let mut changes = TransactionPublicChanges::default();
                let mut pre_index = 0usize;
                let mut post_index = 0usize;
                if !selected_stage.pre.is_empty() || !selected_stage.post.is_empty() {
                    checked_increment(
                        &mut audit.transactions_with_target_balance_rows,
                        "transaction with target balance rows count",
                    )?;
                }
                audit.target_pre_balance_rows = audit
                    .target_pre_balance_rows
                    .checked_add(u64::try_from(selected_stage.pre.len())?)
                    .context("target pre-balance row count overflow")?;
                audit.target_post_balance_rows = audit
                    .target_post_balance_rows
                    .checked_add(u64::try_from(selected_stage.post.len())?)
                    .context("target post-balance row count overflow")?;
                while pre_index < selected_stage.pre.len() || post_index < selected_stage.post.len()
                {
                    let pre = selected_stage.pre.get(pre_index).copied();
                    let post = selected_stage.post.get(post_index).copied();
                    let account_index = match (pre, post) {
                        (Some(left), Some(right)) => left.account_index.min(right.account_index),
                        (Some(left), None) => left.account_index,
                        (None, Some(right)) => right.account_index,
                        (None, None) => unreachable!("balance merge has one live side"),
                    };
                    let paired_pre = pre.filter(|row| row.account_index == account_index);
                    let paired_post = post.filter(|row| row.account_index == account_index);
                    pre_index += usize::from(paired_pre.is_some());
                    post_index += usize::from(paired_post.is_some());
                    let registry_id = resolve_message_account_id(
                        account_index,
                        &message,
                        &static_ids,
                        selected_stage,
                    )?;
                    let account_ordinal = *token_account_ordinal_by_registry_id
                        .get(usize::try_from(registry_id)?)
                        .context("target balance account registry ID is outside dense table")?;
                    ensure!(
                        account_ordinal != u32::MAX,
                        "target mint balance refers to an account absent from the frozen account list"
                    );
                    apply_public_balance_pair(
                        usize::try_from(account_ordinal)?,
                        paired_pre,
                        paired_post,
                        &mut account_states,
                        day,
                        &mut changes,
                        &mut audit,
                        &mut decimals,
                    )?;
                }
                if changes.amount_changed {
                    checked_increment(
                        &mut audit.public_balance_changing_transactions,
                        "public balance-changing transaction count",
                    )?;
                    checked_increment(
                        &mut day.public_balance_changing_transactions,
                        "daily public balance-changing transaction count",
                    )?;
                }
                if changes.owner_changed {
                    checked_increment(
                        &mut audit.public_owner_reassignment_transactions,
                        "public owner-reassignment transaction count",
                    )?;
                    checked_increment(
                        &mut day.public_owner_reassignment_transactions,
                        "daily public owner-reassignment transaction count",
                    )?;
                }
                day.public_movement = day
                    .public_movement
                    .checked_add(changes.movement())
                    .context("daily public movement overflow")?;
                day.inferred_public_mint = day
                    .inferred_public_mint
                    .checked_add(changes.inferred_mint())
                    .context("daily inferred public mint overflow")?;
                day.inferred_public_burn = day
                    .inferred_public_burn
                    .checked_add(changes.inferred_burn())
                    .context("daily inferred public burn overflow")?;
                if day_number.is_none() && (changes.amount_changed || changes.owner_changed) {
                    checked_increment(
                        &mut audit.public_state_changes_without_block_time,
                        "public state change without block time count",
                    )?;
                    bail!("a public balance or owner state change has no block time");
                }
                if let Some(block_time) = record.block.block_time {
                    retain_volume_candidate(
                        &mut top_volume_transactions,
                        VolumeCandidate {
                            movement: changes.movement(),
                            inferred_mint: changes.inferred_mint(),
                            inferred_burn: changes.inferred_burn(),
                            source_epoch: record.source_epoch,
                            slot: record.block.slot,
                            source_block_id: record.source_block_id,
                            tx_index: record.tx_index,
                            signature_ordinal: audit.signatures,
                            block_time,
                        },
                    );
                }

                checked_increment(&mut audit.transactions, "transaction count")?;
                audit.signatures = audit
                    .signatures
                    .checked_add(u64::from(record.signature_count))
                    .context("signature count overflow")?;
                if audit
                    .transactions
                    .is_multiple_of(TOKEN_HISTORY_PROGRESS_TRANSACTIONS)
                {
                    report_progress(
                        started,
                        audit.transactions,
                        manifest.transactions,
                        logical_offset,
                    );
                }
            }
        }
    };

    ensure!(
        read_frame_hashed(
            &mut reader,
            &mut logical_offset,
            &mut transaction_hasher,
            &mut payload,
        )?
        .is_none(),
        "consolidated transaction stream has records after its footer"
    );
    let transaction_file = reader.into_inner();
    transaction_stamp.verify(&transaction_file, "consolidated transaction stream")?;
    verify_path_binding(
        &transaction_path,
        &transaction_stamp,
        "consolidated transaction stream",
    )?;
    ensure!(
        logical_offset == transaction_stamp.bytes,
        "transaction stream size changed while it was read"
    );
    let actual_transaction_sha256: [u8; 32] = transaction_hasher.finalize().into();
    ensure!(
        actual_transaction_sha256 == expected_transaction_sha256,
        "transaction digest differs from its manifest"
    );
    let epoch_count = manifest
        .last_epoch
        .checked_sub(manifest.first_epoch)
        .and_then(|span| span.checked_add(1))
        .context("manifest epoch count overflow")?;
    ensure!(
        audit.transactions == manifest.transactions
            && audit.signatures == expected_signatures
            && anchor_count == 1
            && footer.epochs == epoch_count
            && footer.transactions_written == audit.transactions
            && footer.transactions_scanned >= audit.transactions
            && footer.pubkeys == expected_registry_rows
            && footer.signatures == audit.signatures
            && footer.owned_block_fallbacks <= footer.blocks_scanned
            && footer.raw_transaction_fallbacks == 0
            && footer.raw_metadata_fallbacks == 0,
        "consolidated stream counters differ from its manifest"
    );
    let metadata_records = audit
        .metadata_absent
        .checked_add(audit.metadata_without_error)
        .and_then(|count| count.checked_add(audit.metadata_current_only))
        .and_then(|count| count.checked_add(audit.metadata_legacy_only))
        .and_then(|count| count.checked_add(audit.metadata_both_same_target_balance_resolution))
        .context("metadata classification count overflow")?;
    ensure!(
        metadata_records == audit.transactions
            && audit.selected_transactions_without_target_address == 0
            && audit.target_positive_states_without_owner == 0
            && audit.public_state_changes_without_block_time == 0,
        "token history classifications are incomplete"
    );
    let decimals = decimals.context("target mint has no public token-balance metadata rows")?;

    let mut running_owner_balances = HashMap::<u32, u128>::new();
    let mut running_active_accounts = 0u64;
    let mut running_supply = 0u128;
    let mut daily_reports = Vec::new();
    daily_reports
        .try_reserve_exact(daily.len())
        .context("reserve daily token history")?;
    for (day_number, value) in &daily {
        for (&owner, &delta) in &value.owner_deltas {
            let next = delta.apply(
                running_owner_balances.get(&owner).copied().unwrap_or(0),
                "daily owner balance",
            )?;
            if next == 0 {
                running_owner_balances.remove(&owner);
            } else {
                running_owner_balances.insert(owner, next);
            }
        }
        running_active_accounts = value
            .active_token_account_delta
            .apply(running_active_accounts, "daily active token-account count")?;
        running_supply = value
            .supply_delta
            .apply(running_supply, "daily public supply")?;
        let owner_supply = running_owner_balances
            .values()
            .try_fold(0u128, |sum, amount| {
                sum.checked_add(*amount)
                    .context("daily owner public-balance sum overflow")
            })?;
        ensure!(
            owner_supply == running_supply,
            "daily owner balances do not sum to public supply"
        );
        let concentrations = concentration_amounts(running_owner_balances.values().copied())?;
        daily_reports.push(DailyPublicBalanceReport {
            utc_date: utc_date_from_day(*day_number)?,
            selected_transactions: value.selected_transactions,
            public_balance_changing_transactions: value.public_balance_changing_transactions,
            public_owner_reassignment_transactions: value.public_owner_reassignment_transactions,
            positive_public_balance_holders: u64::try_from(running_owner_balances.len())?,
            active_public_token_accounts: running_active_accounts,
            public_raw_balance_sum: amount_report(running_supply, decimals)?,
            public_bilateral_movement: amount_report(value.public_movement, decimals)?,
            inferred_public_mint: amount_report(value.inferred_public_mint, decimals)?,
            inferred_public_burn: amount_report(value.inferred_public_burn, decimals)?,
            top_1_concentration: concentration_report(concentrations[0], running_supply, decimals)?,
            top_10_concentration: concentration_report(
                concentrations[1],
                running_supply,
                decimals,
            )?,
            top_100_concentration: concentration_report(
                concentrations[2],
                running_supply,
                decimals,
            )?,
        });
    }

    let mut final_holders_by_owner = HashMap::<u32, HolderValue>::new();
    let mut final_active_accounts = 0u64;
    let mut final_supply = 0u128;
    for state in &account_states {
        if state.amount == 0 {
            continue;
        }
        let owner = state
            .owner_id
            .context("positive final token-account balance has no owner")?;
        checked_increment(
            &mut final_active_accounts,
            "final active token-account count",
        )?;
        final_supply = final_supply
            .checked_add(state.amount)
            .context("final public supply overflow")?;
        let holder = final_holders_by_owner.entry(owner).or_default();
        holder.amount = holder
            .amount
            .checked_add(state.amount)
            .context("final owner public balance overflow")?;
        checked_increment(&mut holder.account_count, "final owner account count")?;
    }
    ensure!(
        final_supply == running_supply
            && final_active_accounts == running_active_accounts
            && final_holders_by_owner.len() == running_owner_balances.len()
            && final_holders_by_owner
                .iter()
                .all(|(owner, value)| { running_owner_balances.get(owner) == Some(&value.amount) }),
        "final account states differ from the daily reconstructed state"
    );
    let mut holder_rows = final_holders_by_owner.into_iter().collect::<Vec<_>>();
    holder_rows.sort_unstable_by(|(left_id, left), (right_id, right)| {
        right.amount.cmp(&left.amount).then_with(|| {
            registry_key_at(&registry, *left_id)
                .expect("validated owner registry ID")
                .cmp(&registry_key_at(&registry, *right_id).expect("validated owner registry ID"))
        })
    });
    let largest_rows = holder_rows
        .iter()
        .take(TOP_HOLDER_LIMIT)
        .copied()
        .collect::<Vec<_>>();
    let mut smallest_rows = holder_rows.clone();
    smallest_rows.sort_unstable_by(|(left_id, left), (right_id, right)| {
        left.amount.cmp(&right.amount).then_with(|| {
            registry_key_at(&registry, *left_id)
                .expect("validated owner registry ID")
                .cmp(&registry_key_at(&registry, *right_id).expect("validated owner registry ID"))
        })
    });
    smallest_rows.truncate(TOP_HOLDER_LIMIT);
    let final_concentrations = concentration_amounts(holder_rows.iter().map(|row| row.1.amount))?;
    let final_public_balance = FinalPublicBalanceReport {
        decimals,
        positive_public_balance_holders: u64::try_from(holder_rows.len())?,
        active_public_token_accounts: final_active_accounts,
        public_raw_balance_sum: amount_report(final_supply, decimals)?,
        top_1_concentration: concentration_report(final_concentrations[0], final_supply, decimals)?,
        top_10_concentration: concentration_report(
            final_concentrations[1],
            final_supply,
            decimals,
        )?,
        top_100_concentration: concentration_report(
            final_concentrations[2],
            final_supply,
            decimals,
        )?,
        largest_25_holders: serialize_holders(&largest_rows, &registry, decimals)?,
        smallest_25_positive_holders: serialize_holders(&smallest_rows, &registry, decimals)?,
        balance_distribution: build_distribution(&holder_rows, decimals)?,
    };

    let mut volume_days = daily.iter().collect::<Vec<_>>();
    volume_days.sort_unstable_by(|(left_day, left), (right_day, right)| {
        right
            .public_movement
            .cmp(&left.public_movement)
            .then_with(|| left_day.cmp(right_day))
    });
    let top_25_volume_days = volume_days
        .into_iter()
        .filter(|(_, value)| value.public_movement != 0)
        .take(TOP_VOLUME_LIMIT)
        .map(|(day_number, value)| {
            Ok(VolumeDayReport {
                utc_date: utc_date_from_day(*day_number)?,
                public_bilateral_movement: amount_report(value.public_movement, decimals)?,
                inferred_public_mint: amount_report(value.inferred_public_mint, decimals)?,
                inferred_public_burn: amount_report(value.inferred_public_burn, decimals)?,
                selected_transactions: value.selected_transactions,
                public_balance_changing_transactions: value.public_balance_changing_transactions,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (total_movement, total_inferred_mint, total_inferred_burn) = daily.values().try_fold(
        (0u128, 0u128, 0u128),
        |(movement, mint, burn), value| -> Result<_> {
            Ok((
                movement
                    .checked_add(value.public_movement)
                    .context("total public movement overflow")?,
                mint.checked_add(value.inferred_public_mint)
                    .context("total inferred public mint overflow")?,
                burn.checked_add(value.inferred_public_burn)
                    .context("total inferred public burn overflow")?,
            ))
        },
    )?;
    let public_volume_totals = PublicVolumeTotalsReport {
        public_balance_changing_transactions: audit.public_balance_changing_transactions,
        public_owner_reassignment_transactions: audit.public_owner_reassignment_transactions,
        public_bilateral_movement: amount_report(total_movement, decimals)?,
        inferred_public_mint: amount_report(total_inferred_mint, decimals)?,
        inferred_public_burn: amount_report(total_inferred_burn, decimals)?,
    };

    let mut volume_candidates = top_volume_transactions
        .into_iter()
        .map(|value| value.0)
        .collect::<Vec<_>>();
    volume_candidates.sort_unstable_by(|left, right| right.cmp(left));
    let top_25_volume_transactions = volume_candidates
        .into_iter()
        .map(|candidate| {
            Ok(VolumeTransactionReport {
                first_signature: bs58::encode(read_signature_at(
                    &signature_file,
                    candidate.signature_ordinal,
                )?)
                .into_string(),
                source_epoch: candidate.source_epoch,
                slot: candidate.slot,
                source_block_id: candidate.source_block_id,
                tx_index: candidate.tx_index,
                block_time_unix_seconds: candidate.block_time,
                utc_date: utc_date_from_day(utc_day_number(candidate.block_time))?,
                public_bilateral_movement: amount_report(candidate.movement, decimals)?,
                inferred_public_mint: amount_report(candidate.inferred_mint, decimals)?,
                inferred_public_burn: amount_report(candidate.inferred_burn, decimals)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    signature_stamp.verify(&signature_file, "consolidated signature sidecar")?;
    verify_path_binding(
        &signature_path,
        &signature_stamp,
        "consolidated signature sidecar",
    )?;

    let mut get_signatures_requests = 0u64;
    let mut get_signatures_credit_pages = 0u64;
    let mut returned_rows = 0u64;
    let mut per_address = Vec::new();
    per_address
        .try_reserve_exact(addresses.len())
        .context("reserve serialized RPC address model")?;
    for address in &addresses {
        let requests = address.transactions.div_ceil(RPC_SIGNATURE_PAGE_SIZE);
        let credit_pages = address
            .transactions
            .div_ceil(RPC_SIGNATURE_CREDIT_PAGE_SIZE);
        get_signatures_requests = get_signatures_requests
            .checked_add(requests)
            .context("getSignaturesForAddress request count overflow")?;
        get_signatures_credit_pages = get_signatures_credit_pages
            .checked_add(credit_pages)
            .context("getSignaturesForAddress credit-page count overflow")?;
        returned_rows = returned_rows
            .checked_add(address.transactions)
            .context("returned address-signature row count overflow")?;
        per_address.push(AddressRequestReport {
            address: bs58::encode(address.raw_key).into_string(),
            kind: address.kind,
            returned_address_signature_rows: address.transactions,
            get_signatures_for_address_requests_at_limit_1000: requests,
            get_signatures_for_address_credit_pages_at_100: credit_pages,
        });
    }
    ensure!(
        returned_rows == audit.address_signature_rows && returned_rows >= audit.transactions,
        "RPC request model counters are inconsistent"
    );
    let duplicate_rows = returned_rows
        .checked_sub(audit.transactions)
        .context("address-signature rows do not cover each selected transaction")?;
    let total_rpc_requests = get_signatures_requests
        .checked_add(audit.transactions)
        .context("total RPC request count overflow")?;
    let rpc_request_model = RpcRequestModelReport {
        scope: "exact_requests_for_the_selected_dump_transaction_set; one_getTransaction_per_unique_first_signature_after_cross_address_deduplication",
        address_count: u64::try_from(addresses.len())?,
        mint_addresses: 1,
        token_account_addresses: u64::try_from(accounts.accounts.len())?,
        get_signatures_for_address_page_limit: RPC_SIGNATURE_PAGE_SIZE,
        get_signatures_for_address_requests: get_signatures_requests,
        get_signatures_for_address_credit_page_size: RPC_SIGNATURE_CREDIT_PAGE_SIZE,
        get_signatures_for_address_credit_pages: get_signatures_credit_pages,
        returned_address_signature_rows: returned_rows,
        duplicate_address_signature_rows_removed: duplicate_rows,
        unique_get_transaction_calls: audit.transactions,
        total_rpc_requests,
        per_address,
    };

    let total_dump_bytes = u64::try_from(manifest_bytes.len())?
        .checked_add(transaction_stamp.bytes)
        .and_then(|value| value.checked_add(signature_stamp.bytes))
        .and_then(|value| value.checked_add(u64::try_from(registry.len()).ok()?))
        .and_then(|value| value.checked_add(u64::try_from(account_bytes.len()).ok()?))
        .context("total dump byte count overflow")?;
    let source = TokenHistorySourceReport {
        mint: manifest.mint,
        mint_slot: manifest.mint_slot,
        first_epoch: manifest.first_epoch,
        last_epoch: manifest.last_epoch,
        transactions: audit.transactions,
        signatures: audit.signatures,
        registry_entries,
        discovered_token_accounts: u64::try_from(accounts.accounts.len())?,
        total_dump_bytes,
        manifest: SourceArtifactReport {
            file: DUMP_MANIFEST_FILE,
            bytes: u64::try_from(manifest_bytes.len())?,
            sha256: hex_digest(manifest_sha256),
        },
        transactions_file: SourceArtifactReport {
            file: TRANSACTIONS_FILE,
            bytes: transaction_stamp.bytes,
            sha256: hex_digest(actual_transaction_sha256),
        },
        signatures_file: SourceArtifactReport {
            file: DUMP_SIGNATURES_FILE,
            bytes: signature_stamp.bytes,
            sha256: hex_digest(expected_signature_sha256),
        },
        registry_file: SourceArtifactReport {
            file: PUBKEY_REGISTRY_FILE,
            bytes: u64::try_from(registry.len())?,
            sha256: hex_digest(expected_registry_sha256),
        },
        accounts_file: SourceArtifactReport {
            file: ACCOUNTS_FILE,
            bytes: u64::try_from(account_bytes.len())?,
            sha256: hex_digest(expected_account_sha256),
        },
    };
    let final_top_100_holder_history = build_final_top_100_holder_history(
        &daily,
        &holder_rows,
        &registry,
        final_supply,
        final_concentrations[2],
        FinalTopHolderHistorySourceBindingReport::from_report_source(&source),
    )?;
    let daily_selected_transaction_counts_complete = audit.transactions_without_block_time == 0;
    let report_value = TokenHistoryReport {
        schema_version: TOKEN_HISTORY_REPORT_SCHEMA_VERSION,
        artifact_kind: "token_public_balance_history",
        bounded_selected_dump_scan_complete: true,
        metadata_balance_chain_continuous_from_spyx_mint_creation: true,
        instruction_replay_performed: false,
        daily_public_balance_series_complete: true,
        daily_selected_transaction_counts_complete,
        definitions: TokenHistoryDefinitions {
            holder: "one resolved owner with a positive summed public raw balance across its discovered target-mint token accounts",
            active_token_account: "one discovered target-mint token account with a positive public raw balance",
            public_bilateral_movement: "min(sum_positive_public_raw_account_deltas,sum_negative_public_raw_account_deltas) per transaction",
            inferred_public_mint: "max(sum_positive_public_raw_account_deltas-sum_negative_public_raw_account_deltas,0)",
            inferred_public_burn: "max(sum_negative_public_raw_account_deltas-sum_positive_public_raw_account_deltas,0)",
            base_units: "exact raw_amount divided by 10^metadata_decimals; this is not ScaledUiAmount output",
            daily_boundary: "UTC calendar day from block_time; state is measured after all selected transactions assigned to that day",
        },
        limitations: TokenHistoryLimitations {
            token_program: "SPYX uses Token-2022",
            scaled_ui_amount: "ScaledUiAmount can change the displayed economic amount; this report does not apply its multiplier history",
            confidential_transfer: "ConfidentialTransfer balances and movement are not visible in public token-balance metadata and are excluded",
            holder_scope: "holder counts include positive public raw balances only; confidential holdings can make the economic holder set larger",
            volume_scope: "movement, mint, and burn fields are public raw-balance inferences, not complete Token-2022 economic flow",
            rpc_model_scope: "request counts model the exact selected dump transaction set; a live provider can return extra rows outside this bounded time and selection scope",
        },
        source,
        audit,
        final_public_balance,
        public_volume_totals,
        daily: daily_reports,
        final_top_100_holder_history,
        top_25_volume_days,
        top_25_volume_transactions,
        rpc_request_model,
    };
    let mut report_bytes = serde_json::to_vec_pretty(&report_value)?;
    report_bytes.push(b'\n');
    let report_sha256 = sha256_bytes(&report_bytes);
    let report_path = publish_program_inventory_report(report, &report_bytes)?;
    report_progress(
        started,
        report_value.audit.transactions,
        report_value.audit.transactions,
        logical_offset,
    );
    eprintln!(
        "token history report complete: {} transactions, {} discovered token accounts, {} positive public holders, {} active public token accounts, {:.1}s elapsed, report_sha256={}, report={}",
        report_value.audit.transactions,
        report_value.source.discovered_token_accounts,
        report_value
            .final_public_balance
            .positive_public_balance_holders,
        report_value
            .final_public_balance
            .active_public_token_accounts,
        started.elapsed().as_secs_f64(),
        hex_digest(report_sha256),
        report_path.display(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registry(owner_count: u32) -> Vec<u8> {
        let mut registry = Vec::with_capacity(usize::try_from(owner_count).unwrap() * KEY_BYTES);
        for owner_id in 1..=owner_count {
            let mut key = [0u8; KEY_BYTES];
            key[KEY_BYTES - std::mem::size_of::<u32>()..].copy_from_slice(&owner_id.to_be_bytes());
            registry.extend_from_slice(&key);
        }
        registry
    }

    fn test_source_binding() -> FinalTopHolderHistorySourceBindingReport {
        FinalTopHolderHistorySourceBindingReport {
            mint: "test-mint".to_owned(),
            mint_slot: 42,
            first_epoch: 10,
            last_epoch: 11,
            manifest_sha256: "11".repeat(32),
            transactions_sha256: "22".repeat(32),
            signatures_sha256: "33".repeat(32),
            registry_sha256: "44".repeat(32),
            accounts_sha256: "55".repeat(32),
        }
    }

    fn final_test_holder_amount(owner_id: u32) -> u128 {
        match owner_id {
            1..=99 => 1_000 - u128::from(owner_id),
            100 | 101 => 900,
            102 => 899,
            _ => unreachable!("test owner ID is outside its fixed range"),
        }
    }

    fn existing_daily_totals(
        daily: &BTreeMap<i64, DailyAccumulator>,
    ) -> (u64, u64, u64, u128, u128, u128, u128, u128) {
        daily.values().fold(
            (0, 0, 0, 0, 0, 0, 0, 0),
            |(
                selected,
                changing,
                reassigned,
                movement,
                minted,
                burned,
                supply_increase,
                supply_decrease,
            ),
             value| {
                (
                    selected + value.selected_transactions,
                    changing + value.public_balance_changing_transactions,
                    reassigned + value.public_owner_reassignment_transactions,
                    movement + value.public_movement,
                    minted + value.inferred_public_mint,
                    burned + value.inferred_public_burn,
                    supply_increase + value.supply_delta.increase,
                    supply_decrease + value.supply_delta.decrease,
                )
            },
        )
    }

    #[test]
    fn amount_formatting_is_exact_and_keeps_all_decimal_places() {
        assert_eq!(format_base_units(0, 8).unwrap(), "0.00000000");
        assert_eq!(format_base_units(1, 8).unwrap(), "0.00000001");
        assert_eq!(
            format_base_units(12_345_678_901, 8).unwrap(),
            "123.45678901"
        );
        assert_eq!(format_base_units(42, 0).unwrap(), "42");
    }

    #[test]
    fn concentration_keeps_the_largest_one_ten_and_hundred() {
        let values = (1u128..=150).collect::<Vec<_>>();
        let result = concentration_amounts(values).unwrap();
        assert_eq!(result[0], 150);
        assert_eq!(result[1], (141u128..=150).sum());
        assert_eq!(result[2], (51u128..=150).sum());
    }

    #[test]
    fn positive_balance_state_crossing_is_exact() {
        assert_eq!(positive_balance_crossing(0, 0), 0);
        assert_eq!(positive_balance_crossing(0, 1), 1);
        assert_eq!(positive_balance_crossing(1, 2), 0);
        assert_eq!(positive_balance_crossing(2, 0), -1);
    }

    #[test]
    fn utc_date_conversion_handles_epoch_boundaries() {
        assert_eq!(utc_date_from_day(utc_day_number(0)).unwrap(), "1970-01-01");
        assert_eq!(utc_date_from_day(utc_day_number(-1)).unwrap(), "1969-12-31");
        assert_eq!(
            utc_date_from_day(utc_day_number(1_704_067_200)).unwrap(),
            "2024-01-01"
        );
    }

    #[test]
    fn final_top_holder_history_uses_final_cohort_and_all_calendar_dates() {
        let registry = test_registry(102);
        let mut final_holders = (1u32..=102)
            .map(|owner_id| {
                (
                    owner_id,
                    HolderValue {
                        amount: final_test_holder_amount(owner_id),
                        account_count: 1,
                    },
                )
            })
            .collect::<Vec<_>>();
        final_holders.sort_unstable_by(|(left_id, left), (right_id, right)| {
            right.amount.cmp(&left.amount).then_with(|| {
                registry_key_at(&registry, *left_id)
                    .unwrap()
                    .cmp(&registry_key_at(&registry, *right_id).unwrap())
            })
        });

        let mut first_day = DailyAccumulator {
            selected_transactions: 7,
            public_balance_changing_transactions: 6,
            public_owner_reassignment_transactions: 5,
            public_movement: 4,
            inferred_public_mint: 3,
            inferred_public_burn: 2,
            ..DailyAccumulator::default()
        };
        let mut last_day = DailyAccumulator {
            selected_transactions: 13,
            public_balance_changing_transactions: 12,
            public_owner_reassignment_transactions: 11,
            public_movement: 10,
            inferred_public_mint: 9,
            inferred_public_burn: 8,
            ..DailyAccumulator::default()
        };
        let mut first_supply = 0u128;
        let mut final_supply = 0u128;
        for owner_id in 1u32..=102 {
            let final_amount = final_test_holder_amount(owner_id);
            // Owner 102 was much larger on the first day, but it is not in the
            // fixed cohort because its final balance ranks below the cutoff.
            let first_amount = if owner_id == 102 {
                50_000
            } else {
                final_amount - 1
            };
            first_day.owner_deltas.insert(
                owner_id,
                AmountDelta {
                    increase: first_amount,
                    decrease: 0,
                },
            );
            last_day.owner_deltas.insert(
                owner_id,
                if final_amount >= first_amount {
                    AmountDelta {
                        increase: final_amount - first_amount,
                        decrease: 0,
                    }
                } else {
                    AmountDelta {
                        increase: 0,
                        decrease: first_amount - final_amount,
                    }
                },
            );
            first_supply += first_amount;
            final_supply += final_amount;
        }
        first_day.supply_delta.increase = first_supply;
        last_day.supply_delta = AmountDelta {
            increase: 0,
            decrease: first_supply - final_supply,
        };
        let mut daily = BTreeMap::new();
        daily.insert(10, first_day);
        daily.insert(12, last_day);

        let expected_top_100 = final_holders
            .iter()
            .take(100)
            .map(|(_, holder)| holder.amount)
            .sum::<u128>();
        let source_binding = test_source_binding();
        let daily_totals_before = existing_daily_totals(&daily);
        let final_holders_before = final_holders
            .iter()
            .map(|(owner_id, holder)| (*owner_id, holder.amount, holder.account_count))
            .collect::<Vec<_>>();
        let report = build_final_top_100_holder_history(
            &daily,
            &final_holders,
            &registry,
            final_supply,
            expected_top_100,
            source_binding.clone(),
        )
        .unwrap();

        assert_eq!(report.source_binding, source_binding);
        assert_eq!(report.cohort.maximum_holders, 100);
        assert_eq!(report.cohort.selected_holders, 100);
        assert_eq!(report.days.len(), 3);
        assert_eq!(report.days[0].utc_date, "1970-01-11");
        assert!(report.days[0].source_boundary_start);
        assert!(!report.days[0].source_boundary_end);
        assert!(!report.days[0].complete_utc_day);
        assert!(report.days[0].observed_selected_transaction_day);
        assert!(!report.days[0].balance_state_carried_forward);
        assert_eq!(report.days[1].utc_date, "1970-01-12");
        assert!(report.days[1].complete_utc_day);
        assert!(!report.days[1].observed_selected_transaction_day);
        assert!(report.days[1].balance_state_carried_forward);
        assert_eq!(report.days[2].utc_date, "1970-01-13");
        assert!(!report.days[2].source_boundary_start);
        assert!(report.days[2].source_boundary_end);
        assert!(!report.days[2].complete_utc_day);

        assert_eq!(report.series.len(), 100);
        assert_eq!(report.series[99].final_rank, 100);
        assert_eq!(
            report.series[99].owner,
            bs58::encode(registry_key_at(&registry, 100).unwrap()).into_string()
        );
        assert!(report.series.iter().all(|holder| {
            holder.owner != bs58::encode(registry_key_at(&registry, 101).unwrap()).into_string()
                && holder.owner
                    != bs58::encode(registry_key_at(&registry, 102).unwrap()).into_string()
        }));
        assert_eq!(report.series[0].daily_raw_balances, ["998", "998", "999"]);
        assert_eq!(report.series[99].daily_raw_balances, ["899", "899", "900"]);
        for (holder_series, (_, holder)) in report.series.iter().zip(&final_holders) {
            assert_eq!(holder_series.daily_raw_balances.len(), report.days.len());
            assert_eq!(
                holder_series.daily_raw_balances.last().unwrap(),
                &holder.amount.to_string()
            );
        }
        assert_eq!(
            report
                .series
                .iter()
                .map(|holder| holder.final_raw_balance.parse::<u128>().unwrap())
                .sum::<u128>(),
            expected_top_100
        );

        assert_eq!(existing_daily_totals(&daily), daily_totals_before);
        assert_eq!(
            final_holders
                .iter()
                .map(|(owner_id, holder)| (*owner_id, holder.amount, holder.account_count))
                .collect::<Vec<_>>(),
            final_holders_before
        );
    }

    #[test]
    fn final_top_holder_history_rejects_changed_daily_or_concentration_totals() {
        let registry = test_registry(1);
        let final_holders = vec![(
            1,
            HolderValue {
                amount: 7,
                account_count: 1,
            },
        )];
        let mut bad_daily_total = DailyAccumulator::default();
        bad_daily_total.owner_deltas.insert(
            1,
            AmountDelta {
                increase: 7,
                decrease: 0,
            },
        );
        bad_daily_total.supply_delta.increase = 8;
        let bad_daily = BTreeMap::from([(0, bad_daily_total)]);
        let error = build_final_top_100_holder_history(
            &bad_daily,
            &final_holders,
            &registry,
            7,
            7,
            test_source_binding(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("daily owner balances do not match the existing daily supply")
        );

        let mut valid_day = DailyAccumulator::default();
        valid_day.owner_deltas.insert(
            1,
            AmountDelta {
                increase: 7,
                decrease: 0,
            },
        );
        valid_day.supply_delta.increase = 7;
        let valid_daily = BTreeMap::from([(0, valid_day)]);
        let error = build_final_top_100_holder_history(
            &valid_daily,
            &final_holders,
            &registry,
            7,
            6,
            test_source_binding(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cohort amount differs from existing top-100 concentration")
        );
    }

    #[test]
    fn final_top_holder_history_wire_shape_keeps_schema_one_and_one_day_boundaries() {
        assert_eq!(TOKEN_HISTORY_REPORT_SCHEMA_VERSION, 1);
        let registry = test_registry(1);
        let final_holders = vec![(
            1,
            HolderValue {
                amount: 7,
                account_count: 1,
            },
        )];
        let mut day = DailyAccumulator::default();
        day.owner_deltas.insert(
            1,
            AmountDelta {
                increase: 7,
                decrease: 0,
            },
        );
        day.supply_delta.increase = 7;
        let report = build_final_top_100_holder_history(
            &BTreeMap::from([(0, day)]),
            &final_holders,
            &registry,
            7,
            7,
            test_source_binding(),
        )
        .unwrap();

        assert!(report.days[0].source_boundary_start);
        assert!(report.days[0].source_boundary_end);
        assert!(!report.days[0].complete_utc_day);
        assert_eq!(report.series[0].daily_raw_balances, ["7"]);

        let wire = serde_json::to_value(&report).unwrap();
        assert_eq!(wire["cohort"]["maximum_holders"], 100);
        assert_eq!(wire["days"][0]["utc_date"], "1970-01-01");
        assert_eq!(wire["series"][0]["final_rank"], 1);
        assert_eq!(wire["series"][0]["final_raw_balance"], "7");
        assert_eq!(wire["series"][0]["daily_raw_balances"][0], "7");
        for field in [
            "manifest_sha256",
            "transactions_sha256",
            "signatures_sha256",
            "registry_sha256",
            "accounts_sha256",
        ] {
            assert!(wire["source_binding"][field].is_string());
        }
    }
}
