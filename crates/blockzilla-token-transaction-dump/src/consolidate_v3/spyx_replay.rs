//! Fail-closed SPYx public-balance replay over one consolidated dump.
//!
//! This scanner reads the canonical transaction stream once. It never uses a
//! metadata post-balance as replay input. Metadata is an oracle that is
//! compared with instruction-derived state only after a transaction is
//! staged.

use std::{
    cell::Cell,
    collections::{BTreeMap, HashMap},
};

use blockzilla_dex_parser::{
    DecodeOutcome as DexDecodeOutcome, DispatchTable, InstructionClass as DexInstructionClass,
    PROGRAM_SPECS,
};
use blockzilla_format::ArchiveV2WireMetadataErrorSchema;
use blockzilla_read_sdk::{
    BorrowedArchiveV2TokenBalance, ProjectedArchiveV2CompactLogsSummary, TokenBalanceSide,
};
use blockzilla_token_balance_audit::{
    commit::{
        CommitStatus, InstructionCoordinate, InvocationLogEvent, OrderedInvocation, RollbackReason,
        UnknownReason, classify_committed_invocations,
    },
    effect::{ResolvedInstructionAccount, ResolvedTokenInstruction},
    instruction::{
        DecodeStatus, ExtensionFamily, InstructionEffect, TokenProgram, TopLevelInstruction,
        decode_token_instruction,
    },
    replay::{
        AccountLifecycle, ReplayErrorReason, TargetAccountChange, TargetBalanceReducer,
        TargetMintConfig, TransferFeeKnowledge,
    },
};
use solana_pubkey::Pubkey as SolanaPubkey;

use crate::consolidated_reader::{ExactMetadataSchemaSelection, select_exact_metadata_schema};

use super::spyx_portfolio_history::{
    AuthorityPortfolioHistoryCollector, AuthorityPortfolioHistoryReport,
    AuthorityPortfolioHistorySourceBinding, CandidatePrincipal as HistoryCandidatePrincipal,
    HISTORY_SLOT_WINDOW_WIDTH, HistoryLocation, PortfolioClaimComponentState, PortfolioState,
    allocate_candidate_claims,
};
use super::*;

const REPLAY_PROGRESS_TRANSACTIONS: u64 = 250_000;
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const LEGACY_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const SPYX_DECIMALS: u8 = 8;
const TOP_CLASSIFIED_HOLDER_LIMIT: usize = 25;
const SPYX_REPLAY_REPORT_SCHEMA_VERSION: u16 = 5;

type OwnerPostingVisitor<'a> = dyn FnMut(u64, &[u32]) -> Result<()> + 'a;
type OwnerBalanceHistoryVisitor<'a> =
    dyn for<'b> FnMut(crate::consolidate::SpyxOwnerBalanceTransaction<'b>) -> Result<()> + 'a;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SliceRange {
    start: u32,
    len: u32,
}

impl SliceRange {
    fn capture(base: &[u8], value: &[u8], label: &'static str) -> Result<Self> {
        let start = value
            .as_ptr()
            .addr()
            .checked_sub(base.as_ptr().addr())
            .with_context(|| format!("{label} does not borrow its source record"))?;
        ensure!(
            start
                .checked_add(value.len())
                .is_some_and(|end| end <= base.len()),
            "{label} is outside its source record"
        );
        Ok(Self {
            start: u32::try_from(start).with_context(|| format!("{label} offset exceeds u32"))?,
            len: u32::try_from(value.len())
                .with_context(|| format!("{label} length exceeds u32"))?,
        })
    }

    fn get<'a>(self, base: &'a [u8], label: &'static str) -> Result<&'a [u8]> {
        let start = usize::try_from(self.start)?;
        let end = start
            .checked_add(usize::try_from(self.len)?)
            .with_context(|| format!("{label} range overflow"))?;
        base.get(start..end)
            .with_context(|| format!("{label} range exceeds its source record"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedOuterInstruction {
    program_id_index: u32,
    accounts: SliceRange,
    data: Option<SliceRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedInnerInstruction {
    outer_index: u32,
    inner_index: u32,
    program_id_index: u32,
    accounts: SliceRange,
    data: SliceRange,
    stack_height: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OracleRow {
    account_index: u32,
    amount: u64,
    owner_id: Option<u32>,
    program_id: Option<u32>,
    decimals: u8,
}

#[derive(Debug, Clone, Copy)]
enum InstructionDataRange {
    Message(SliceRange),
    Metadata(SliceRange),
    Missing,
}

#[derive(Debug, Clone, Copy)]
struct StagedReducerInstruction {
    coordinate: InstructionCoordinate,
    program: TokenProgram,
    data: InstructionDataRange,
    accounts_start: u32,
    accounts_len: u16,
    commit_status: CommitStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct InstructionNameKey {
    family: Option<&'static str>,
    name: &'static str,
}

impl InstructionNameKey {
    fn from_decoded(
        decoded: blockzilla_token_balance_audit::instruction::DecodedInstruction<'_>,
    ) -> Self {
        if let Some(interface) = decoded.interface {
            Self {
                family: Some(interface.family.name()),
                name: interface.name,
            }
        } else if let Some(extension) = decoded.extension {
            Self {
                family: Some(extension.family.name()),
                name: extension.name,
            }
        } else {
            Self {
                family: None,
                name: decoded.name(),
            }
        }
    }

    fn display_name(self) -> String {
        self.family.map_or_else(
            || self.name.to_owned(),
            |family| format!("{family}::{}", self.name),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataStage {
    inner: Vec<StagedInnerInstruction>,
    pre: Vec<OracleRow>,
    post: Vec<OracleRow>,
    loaded_ids: [u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    summary: Option<ProjectedArchiveV2TokenMetadataSummary>,
}

impl MetadataStage {
    fn new() -> Self {
        Self {
            inner: Vec::with_capacity(32),
            pre: Vec::with_capacity(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS),
            post: Vec::with_capacity(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS),
            loaded_ids: [0; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
            summary: None,
        }
    }

    fn clear(&mut self) {
        self.inner.clear();
        self.pre.clear();
        self.post.clear();
        self.loaded_ids.fill(0);
        self.summary = None;
    }
}

#[derive(Debug, Default, serde::Serialize)]
struct ReplayCounters {
    transactions_scanned: u64,
    successful_transactions: u64,
    failed_transactions: u64,
    transactions_with_target_oracle_rows: u64,
    pre_target_oracle_rows: u64,
    post_target_oracle_rows: u64,
    outer_token_invocations: u64,
    inner_token_invocations: u64,
    target_relevant_token_invocations: u64,
    committed_target_token_invocations: u64,
    rolled_back_target_token_invocations: u64,
    unknown_commit_target_token_invocations: u64,
    known_decoded_target_token_invocations: u64,
    malformed_target_token_invocations: u64,
    unknown_top_level_target_token_invocations: u64,
    unknown_extension_target_token_invocations: u64,
    balance_relevant_target_token_invocations: u64,
    state_relevant_target_token_invocations: u64,
    no_public_balance_effect_target_token_invocations: u64,
    ambiguous_custom_failure_log_transactions: u64,
    ambiguous_custom_failure_log_transactions_resolved: u64,
    ambiguous_custom_failure_log_transactions_unresolved: u64,
    truncated_log_transactions: u64,
    commit_trace_diagnostics: u64,
    metadata_absent: u64,
    successful_transactions_without_inner_instruction_recording: u64,
    metadata_without_error: u64,
    metadata_current_only: u64,
    metadata_legacy_only: u64,
    metadata_both_identical: u64,
    replay_transactions_attempted: u64,
    replay_transactions_applied: u64,
    replay_clean_prefix_transactions: u64,
    replay_errors: u64,
    oracle_pre_rows_compared: u64,
    oracle_post_rows_compared: u64,
    oracle_pre_mismatches: u64,
    oracle_post_mismatches: u64,
}

#[derive(Debug, serde::Serialize)]
struct FirstReplayFailure {
    source_epoch: u64,
    slot: u64,
    source_block_id: u32,
    tx_index: u32,
    phase: &'static str,
    code: String,
    detail: String,
    outer_index: Option<u32>,
    inner_index: Option<u32>,
}

#[derive(Debug, serde::Serialize)]
struct ReplaySourceReport {
    dump: String,
    mint: String,
    mint_slot: u64,
    first_epoch: u64,
    last_epoch: u64,
    manifest_sha256: String,
    expected_transaction_sha256: String,
    observed_transaction_sha256: Option<String>,
    registry_sha256: String,
    accounts_sha256: String,
    manifest_transactions: u64,
    discovered_token_accounts: u64,
}

#[derive(Debug, serde::Serialize)]
struct ReplayStateReport {
    history_complete: bool,
    tracked_accounts: u64,
    open_accounts: u64,
    closed_accounts: u64,
    positive_public_balance_accounts: u64,
    public_raw_balance: String,
    state_sha256: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ProgramAttributionEvidence {
    program_registry_id: u32,
    parser_authority_observations: u64,
    direct_cpi_authorizations: u64,
    conflicting_program_observations: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
struct RuntimeOwnerObservationLocation {
    transaction_id: u64,
    outer_index: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    inner_index: Option<u32>,
    source_epoch: u64,
    slot: u64,
    source_block_id: u32,
    tx_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RuntimeAccountOwnerEvidence {
    program_id: [u8; 32],
    observation_count: u64,
    owner_change_count: u64,
    conflict_count: u64,
    final_observation: RuntimeOwnerObservationLocation,
}

impl RuntimeAccountOwnerEvidence {
    fn observe(
        &mut self,
        program_id: [u8; 32],
        location: RuntimeOwnerObservationLocation,
    ) -> Result<()> {
        ensure!(
            location >= self.final_observation,
            "runtime account owner observations are not in canonical order"
        );
        if location == self.final_observation {
            if program_id != self.program_id {
                checked_add(
                    &mut self.conflict_count,
                    1,
                    "runtime account owner conflict count overflow",
                )?;
                bail!("conflicting final runtime account owner assignments");
            }
            return Ok(());
        }
        if program_id != self.program_id {
            checked_add(
                &mut self.owner_change_count,
                1,
                "runtime account owner change count overflow",
            )?;
        }
        self.program_id = program_id;
        self.observation_count = self
            .observation_count
            .checked_add(1)
            .context("runtime account owner observation count overflow")?;
        self.final_observation = location;
        Ok(())
    }
}

impl ProgramAttributionEvidence {
    fn observe_parser(&mut self, program_registry_id: u32) -> Result<()> {
        self.observe(program_registry_id, true)
    }

    fn observe_direct_cpi(&mut self, program_registry_id: u32) -> Result<()> {
        self.observe(program_registry_id, false)
    }

    fn observe(&mut self, program_registry_id: u32, parser: bool) -> Result<()> {
        ensure!(
            program_registry_id != 0,
            "PDA attribution program ID is zero"
        );
        if self.program_registry_id == 0 {
            self.program_registry_id = program_registry_id;
        }
        if self.program_registry_id != program_registry_id {
            checked_add(
                &mut self.conflicting_program_observations,
                1,
                "conflicting PDA program observation count overflow",
            )?;
            return Ok(());
        }
        let counter = if parser {
            &mut self.parser_authority_observations
        } else {
            &mut self.direct_cpi_authorizations
        };
        checked_add(counter, 1, "PDA attribution observation count overflow")
    }

    const fn attributed_program(self) -> Option<u32> {
        if self.program_registry_id != 0 && self.conflicting_program_observations == 0 {
            Some(self.program_registry_id)
        } else {
            None
        }
    }

    const fn evidence_name(self) -> &'static str {
        match (
            self.direct_cpi_authorizations != 0,
            self.parser_authority_observations != 0,
        ) {
            (true, true) => "direct_cpi_and_parser_authority",
            (true, false) => "direct_depth_2_cpi_authorization",
            (false, true) => "dex_parser_program_authority_role",
            (false, false) => "none",
        }
    }

    const fn observation_count(self) -> u64 {
        self.parser_authority_observations
            .saturating_add(self.direct_cpi_authorizations)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HolderAuthorityKind {
    ObservedTransactionSigner,
    AttributedProgramDerivedAddress,
    OffCurveUnattributed,
    UnclassifiedOnCurve,
}

impl HolderAuthorityKind {
    const ALL: [Self; 4] = [
        Self::ObservedTransactionSigner,
        Self::AttributedProgramDerivedAddress,
        Self::OffCurveUnattributed,
        Self::UnclassifiedOnCurve,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::ObservedTransactionSigner => "observed_transaction_signer",
            Self::AttributedProgramDerivedAddress => "attributed_program_derived_address",
            Self::OffCurveUnattributed => "off_curve_unattributed",
            Self::UnclassifiedOnCurve => "unclassified_on_curve",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FinalHolderValue {
    amount: u128,
    token_account_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClassifiedHolderValue {
    owner: [u8; 32],
    owner_registry_id: u32,
    value: FinalHolderValue,
    activity: HolderActivityValue,
    authority_kind: HolderAuthorityKind,
    signer_transaction_count: u64,
    program_registry_id: Option<u32>,
    attribution: ProgramAttributionEvidence,
    runtime_account_owner: Option<RuntimeAccountOwnerEvidence>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct HolderActivityValue {
    public_balance_increase: u128,
    public_balance_decrease: u128,
    transaction_count: u64,
}

impl HolderActivityValue {
    fn volume(self) -> Result<u128> {
        self.public_balance_increase
            .checked_add(self.public_balance_decrease)
            .context("holder public activity volume overflow")
    }
}

#[derive(Debug, Clone, serde::Serialize)]
struct HolderAmountReport {
    raw_amount: String,
    base_units: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ClassifiedHolderReport {
    owner: String,
    authority_kind: &'static str,
    classification_evidence: &'static str,
    signer_transaction_count: u64,
    pda_program_id: Option<String>,
    pda_program_evidence_count: u64,
    token_account_count: u64,
    public_balance: HolderAmountReport,
    activity_transaction_count: u64,
    public_balance_increase: HolderAmountReport,
    public_balance_decrease: HolderAmountReport,
    public_activity_volume: HolderAmountReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime_account_owner: Option<RuntimeAccountOwnerReport>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct RuntimeAccountOwnerReport {
    source: &'static str,
    program_id: String,
    observation_count: u64,
    owner_change_count: u64,
    conflict_count: u64,
    proves_pda_derivation: bool,
    last_observation: RuntimeOwnerObservationLocation,
}

#[derive(Debug, serde::Serialize)]
struct HolderAuthorityClassReport {
    authority_kind: &'static str,
    holder_count: u64,
    token_account_count: u64,
    public_balance: HolderAmountReport,
}

#[derive(Debug, serde::Serialize)]
struct ProgramHoldingReport {
    program_id: String,
    pda_holder_count: u64,
    token_account_count: u64,
    public_balance: HolderAmountReport,
    owner_activity_transaction_links: u64,
    public_balance_increase: HolderAmountReport,
    public_balance_decrease: HolderAmountReport,
    public_activity_volume: HolderAmountReport,
}

#[derive(Debug, serde::Serialize)]
struct LargestHoldersByClassReport {
    observed_transaction_signer: Vec<ClassifiedHolderReport>,
    attributed_program_derived_address: Vec<ClassifiedHolderReport>,
    off_curve_unattributed: Vec<ClassifiedHolderReport>,
    unclassified_on_curve: Vec<ClassifiedHolderReport>,
}

#[derive(Debug, serde::Serialize)]
struct HolderAuthorityDefinitions {
    observed_transaction_signer: &'static str,
    attributed_program_derived_address: &'static str,
    off_curve_unattributed: &'static str,
    unclassified_on_curve: &'static str,
    program_attribution: &'static str,
    runtime_account_owner: &'static str,
    nested_cpi_attribution_limit: &'static str,
    public_activity_volume: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct HolderAuthorityReport {
    complete: bool,
    definitions: HolderAuthorityDefinitions,
    class_totals: Vec<HolderAuthorityClassReport>,
    largest_25_all: Vec<ClassifiedHolderReport>,
    largest_25_by_class: LargestHoldersByClassReport,
    largest_25_by_activity_all: Vec<ClassifiedHolderReport>,
    largest_25_by_activity_by_class: LargestHoldersByClassReport,
    attributed_program_holders: Vec<ClassifiedHolderReport>,
    off_curve_unattributed_holders: Vec<ClassifiedHolderReport>,
    holdings_by_program: Vec<ProgramHoldingReport>,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioSourceBinding {
    mint: String,
    first_epoch: u64,
    last_epoch: u64,
    manifest_sha256: String,
    transactions_sha256: String,
    registry_sha256: String,
    replay_state_sha256: String,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioDefinitions {
    estimated_defi_claim: &'static str,
    creation_provenance: &'static str,
    unallocated_custody: &'static str,
    candidate_flow_evidence: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioCoverage {
    complete: bool,
    method: &'static str,
    candidate_flow_evidence_complete: bool,
    transactions_scanned: u64,
    parsed_dex_swap_transactions_excluded: u64,
    candidate_deposit_transactions: u64,
    candidate_return_transactions: u64,
    ambiguous_owner_delta_transactions_excluded: u64,
    current_positive_off_curve_custody_owners: u64,
    definitions: AuthorityPortfolioDefinitions,
}

#[derive(Debug, serde::Serialize)]
struct CandidateFlowEvidenceReport {
    transaction_id: u64,
    slot: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_time: Option<i64>,
    direction: &'static str,
    raw_amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    matched_principal_raw_amount: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityClaimComponentReport {
    custody_owner: String,
    program_id: Option<String>,
    observed_deposited_principal: HolderAmountReport,
    observed_returned_principal: HolderAmountReport,
    candidate_net_principal: HolderAmountReport,
    attributed_claim: HolderAmountReport,
    deposit_transaction_count: u64,
    return_transaction_count: u64,
    candidate_flow_evidence: Vec<CandidateFlowEvidenceReport>,
    confidence: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioRowReport {
    authority: String,
    authority_kind: &'static str,
    direct_public_balance: HolderAmountReport,
    estimated_defi_claim: HolderAmountReport,
    estimated_total_exposure: HolderAmountReport,
    programs_used: Vec<String>,
    claim_components: Vec<AuthorityClaimComponentReport>,
}

#[derive(Debug, serde::Serialize)]
struct ProtocolCustodyRowReport {
    custody_owner: String,
    program_id: Option<String>,
    direct_custody_balance: HolderAmountReport,
    candidate_net_principal: HolderAmountReport,
    attributed_claim: HolderAmountReport,
    unallocated_custody: HolderAmountReport,
    claim_excess: HolderAmountReport,
    candidate_authority_count: u64,
    confidence: &'static str,
}

#[derive(Debug, serde::Serialize)]
struct PdaCreationProvenanceReport {
    subject_pda: String,
    event_kind: &'static str,
    system_instruction: &'static str,
    runtime_owner_program_id: String,
    direct_caller_program_id: Option<String>,
    create_with_seed_base: Option<String>,
    signer_candidates: Vec<String>,
    confidence: &'static str,
    proves_beneficial_ownership: bool,
    location: RuntimeOwnerObservationLocation,
}

#[derive(Debug, serde::Serialize)]
struct AuthorityPortfolioReport {
    schema_version: u16,
    artifact_kind: &'static str,
    source_binding: AuthorityPortfolioSourceBinding,
    coverage: AuthorityPortfolioCoverage,
    portfolios: Vec<AuthorityPortfolioRowReport>,
    protocol_custody: Vec<ProtocolCustodyRowReport>,
    pda_creation_provenance: Vec<PdaCreationProvenanceReport>,
}

#[derive(Debug, serde::Serialize)]
struct ReplayReport {
    schema_version: u16,
    artifact_kind: &'static str,
    bounded_selected_dump_scan_complete: bool,
    instruction_replay_implemented: bool,
    instruction_replay_matches_metadata_for_complete_spyx_selected_history: bool,
    proof_scope: &'static str,
    status: &'static str,
    source: ReplaySourceReport,
    replayed_state: ReplayStateReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    holder_authority: Option<HolderAuthorityReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    authority_portfolios: Option<AuthorityPortfolioReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    authority_portfolio_history: Option<AuthorityPortfolioHistoryReport>,
    counters: ReplayCounters,
    instruction_names: BTreeMap<String, u64>,
    census_findings: BTreeMap<String, u64>,
    blockers: BTreeMap<String, u64>,
    first_failure: Option<FirstReplayFailure>,
    elapsed_seconds: f64,
}

#[derive(Debug)]
struct HolderAuthorityStage {
    signer_transaction_counts: Vec<u32>,
    holder_activity: Vec<HolderActivityValue>,
    last_activity_transaction: Vec<u64>,
    program_attribution: HashMap<u32, ProgramAttributionEvidence>,
    runtime_account_owners: HashMap<u32, RuntimeAccountOwnerEvidence>,
    candidate_principals: HashMap<(u32, u32), CandidatePrincipalValue>,
    candidate_flow_evidence: HashMap<(u32, u32), Vec<CandidateFlowEvidenceValue>>,
    pda_creation_provenance: Vec<PdaCreationProvenanceValue>,
    parsed_dex_swap_transactions_excluded: u64,
    candidate_deposit_transactions: u64,
    candidate_return_transactions: u64,
    ambiguous_owner_delta_transactions_excluded: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CandidateFlowLocation {
    transaction_id: u64,
    slot: u64,
    block_time: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateFlowDirection {
    Deposit,
    Return,
}

impl CandidateFlowDirection {
    const fn name(self) -> &'static str {
        match self {
            Self::Deposit => "deposit",
            Self::Return => "return",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CandidateFlowEvidenceValue {
    location: CandidateFlowLocation,
    direction: CandidateFlowDirection,
    raw_amount: u128,
    matched_principal_raw_amount: u128,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CandidatePrincipalValue {
    observed_deposited_principal: u128,
    observed_returned_principal: u128,
    deposit_transaction_count: u64,
    return_transaction_count: u64,
}

impl CandidatePrincipalValue {
    fn net_principal(self) -> Result<u128> {
        Ok(self
            .observed_deposited_principal
            .saturating_sub(self.observed_returned_principal))
    }
}

impl From<CandidatePrincipalValue> for HistoryCandidatePrincipal {
    fn from(value: CandidatePrincipalValue) -> Self {
        Self {
            observed_deposited_principal: value.observed_deposited_principal,
            observed_returned_principal: value.observed_returned_principal,
            deposit_transaction_count: value.deposit_transaction_count,
            return_transaction_count: value.return_transaction_count,
        }
    }
}

impl From<HistoryCandidatePrincipal> for CandidatePrincipalValue {
    fn from(value: HistoryCandidatePrincipal) -> Self {
        Self {
            observed_deposited_principal: value.observed_deposited_principal,
            observed_returned_principal: value.observed_returned_principal,
            deposit_transaction_count: value.deposit_transaction_count,
            return_transaction_count: value.return_transaction_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PdaCreationProvenanceValue {
    subject_registry_id: u32,
    instruction: SystemOwnerInstructionKind,
    runtime_owner_program_id: [u8; 32],
    direct_caller_program_registry_id: Option<u32>,
    create_with_seed_base: Option<[u8; 32]>,
    signer_candidate_registry_ids: Vec<u32>,
    location: RuntimeOwnerObservationLocation,
}

impl HolderAuthorityStage {
    fn new(registry_entries: u32) -> Result<Self> {
        let length = usize::try_from(registry_entries)?
            .checked_add(1)
            .context("holder signer table length overflow")?;
        Ok(Self {
            signer_transaction_counts: vec![0; length],
            holder_activity: vec![HolderActivityValue::default(); length],
            last_activity_transaction: vec![0; length],
            program_attribution: HashMap::new(),
            runtime_account_owners: HashMap::new(),
            candidate_principals: HashMap::new(),
            candidate_flow_evidence: HashMap::new(),
            pda_creation_provenance: Vec::new(),
            parsed_dex_swap_transactions_excluded: 0,
            candidate_deposit_transactions: 0,
            candidate_return_transactions: 0,
            ambiguous_owner_delta_transactions_excluded: 0,
        })
    }

    fn observe_signers(&mut self, signer_registry_ids: &[u32]) -> Result<()> {
        for &registry_id in signer_registry_ids {
            let count = self
                .signer_transaction_counts
                .get_mut(usize::try_from(registry_id)?)
                .context("signer registry ID exceeds the dense table")?;
            *count = count
                .checked_add(1)
                .context("signer transaction count exceeds u32")?;
        }
        Ok(())
    }

    fn observe_parser_authority(
        &mut self,
        authority_registry_id: u32,
        program_registry_id: u32,
    ) -> Result<()> {
        self.program_attribution
            .entry(authority_registry_id)
            .or_default()
            .observe_parser(program_registry_id)
    }

    fn observe_direct_cpi_authority(
        &mut self,
        authority_registry_id: u32,
        program_registry_id: u32,
    ) -> Result<()> {
        self.program_attribution
            .entry(authority_registry_id)
            .or_default()
            .observe_direct_cpi(program_registry_id)
    }

    fn observe_runtime_account_owner(
        &mut self,
        holder_registry_id: u32,
        program_id: [u8; 32],
        location: RuntimeOwnerObservationLocation,
    ) -> Result<()> {
        match self.runtime_account_owners.entry(holder_registry_id) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().observe(program_id, location)
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(RuntimeAccountOwnerEvidence {
                    program_id,
                    observation_count: 1,
                    owner_change_count: 0,
                    conflict_count: 0,
                    final_observation: location,
                });
                Ok(())
            }
        }
    }

    fn observe_candidate_deposit(
        &mut self,
        authority_registry_id: u32,
        custody_owner_registry_id: u32,
        amount: u128,
        location: CandidateFlowLocation,
    ) -> Result<()> {
        ensure!(amount != 0, "candidate deposit amount is zero");
        let value = self
            .candidate_principals
            .entry((authority_registry_id, custody_owner_registry_id))
            .or_default();
        value.observed_deposited_principal = value
            .observed_deposited_principal
            .checked_add(amount)
            .context("candidate deposited principal overflow")?;
        checked_add(
            &mut value.deposit_transaction_count,
            1,
            "candidate deposit transaction count overflow",
        )?;
        self.candidate_flow_evidence
            .entry((authority_registry_id, custody_owner_registry_id))
            .or_default()
            .push(CandidateFlowEvidenceValue {
                location,
                direction: CandidateFlowDirection::Deposit,
                raw_amount: amount,
                matched_principal_raw_amount: amount,
            });
        checked_add(
            &mut self.candidate_deposit_transactions,
            1,
            "candidate deposit transaction count overflow",
        )
    }

    fn observe_candidate_return(
        &mut self,
        authority_registry_id: u32,
        custody_owner_registry_id: u32,
        amount: u128,
        location: CandidateFlowLocation,
    ) -> Result<()> {
        ensure!(amount != 0, "candidate return amount is zero");
        let Some(value) = self
            .candidate_principals
            .get_mut(&(authority_registry_id, custody_owner_registry_id))
        else {
            return Ok(());
        };
        let unmatched_principal = value.net_principal()?;
        let matched = amount.min(unmatched_principal);
        if matched == 0 {
            return Ok(());
        }
        value.observed_returned_principal = value
            .observed_returned_principal
            .checked_add(matched)
            .context("candidate returned principal overflow")?;
        checked_add(
            &mut value.return_transaction_count,
            1,
            "candidate return transaction count overflow",
        )?;
        self.candidate_flow_evidence
            .entry((authority_registry_id, custody_owner_registry_id))
            .or_default()
            .push(CandidateFlowEvidenceValue {
                location,
                direction: CandidateFlowDirection::Return,
                raw_amount: amount,
                matched_principal_raw_amount: matched,
            });
        checked_add(
            &mut self.candidate_return_transactions,
            1,
            "candidate return transaction count overflow",
        )
    }

    fn signer_transaction_count(&self, registry_id: u32) -> Result<u64> {
        self.signer_transaction_counts
            .get(usize::try_from(registry_id)?)
            .copied()
            .map(u64::from)
            .context("holder registry ID exceeds the signer table")
    }

    fn activity(&self, registry_id: u32) -> Result<HolderActivityValue> {
        self.holder_activity
            .get(usize::try_from(registry_id)?)
            .copied()
            .context("holder registry ID exceeds the activity table")
    }

    fn observe_owner_delta(
        &mut self,
        registry_id: u32,
        delta: i128,
        transaction_ordinal: u64,
    ) -> Result<()> {
        ensure!(
            transaction_ordinal != 0,
            "holder activity transaction ordinal is zero"
        );
        if delta == 0 {
            return Ok(());
        }
        let index = usize::try_from(registry_id)?;
        let activity = self
            .holder_activity
            .get_mut(index)
            .context("holder registry ID exceeds the activity table")?;
        if delta > 0 {
            activity.public_balance_increase = activity
                .public_balance_increase
                .checked_add(delta.unsigned_abs())
                .context("holder public balance increase overflow")?;
        } else {
            activity.public_balance_decrease = activity
                .public_balance_decrease
                .checked_add(delta.unsigned_abs())
                .context("holder public balance decrease overflow")?;
        }
        let last_transaction = self
            .last_activity_transaction
            .get_mut(index)
            .context("holder registry ID exceeds the activity transaction table")?;
        if *last_transaction != transaction_ordinal {
            activity.transaction_count = activity
                .transaction_count
                .checked_add(1)
                .context("holder activity transaction count overflow")?;
            *last_transaction = transaction_ordinal;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ProgramHoldingValue {
    holder_count: u64,
    token_account_count: u64,
    amount: u128,
    owner_activity_transaction_links: u64,
    public_balance_increase: u128,
    public_balance_decrease: u128,
}

fn holder_amount_report(raw: u128) -> HolderAmountReport {
    let scale = 10u128.pow(u32::from(SPYX_DECIMALS));
    let whole = raw / scale;
    let fractional = raw % scale;
    HolderAmountReport {
        raw_amount: raw.to_string(),
        base_units: format!(
            "{whole}.{fractional:0width$}",
            width = usize::from(SPYX_DECIMALS)
        ),
    }
}

fn holder_sort(left: &ClassifiedHolderValue, right: &ClassifiedHolderValue) -> std::cmp::Ordering {
    right
        .value
        .amount
        .cmp(&left.value.amount)
        .then_with(|| left.owner.cmp(&right.owner))
}

fn holder_activity_sort(
    left: &ClassifiedHolderValue,
    right: &ClassifiedHolderValue,
) -> std::cmp::Ordering {
    right
        .activity
        .volume()
        .expect("validated holder activity volume")
        .cmp(
            &left
                .activity
                .volume()
                .expect("validated holder activity volume"),
        )
        .then_with(|| right.value.amount.cmp(&left.value.amount))
        .then_with(|| left.owner.cmp(&right.owner))
}

fn serialize_classified_holder(
    holder: &ClassifiedHolderValue,
    registry: &[u8],
) -> Result<ClassifiedHolderReport> {
    let program_id = holder
        .program_registry_id
        .map(|registry_id| {
            raw_registry_key(registry, registry_id)
                .context("PDA program registry ID cannot be resolved")
                .map(|key| bs58::encode(key).into_string())
        })
        .transpose()?;
    let classification_evidence = match holder.authority_kind {
        HolderAuthorityKind::ObservedTransactionSigner => "required_transaction_signer_prefix",
        HolderAuthorityKind::AttributedProgramDerivedAddress => holder.attribution.evidence_name(),
        HolderAuthorityKind::OffCurveUnattributed
            if holder.attribution.conflicting_program_observations != 0 =>
        {
            "off_curve_conflicting_program_evidence"
        }
        HolderAuthorityKind::OffCurveUnattributed => "ed25519_off_curve_program_unknown",
        HolderAuthorityKind::UnclassifiedOnCurve => "on_curve_not_observed_as_signer",
    };
    Ok(ClassifiedHolderReport {
        owner: bs58::encode(holder.owner).into_string(),
        authority_kind: holder.authority_kind.name(),
        classification_evidence,
        signer_transaction_count: holder.signer_transaction_count,
        pda_program_id: program_id,
        pda_program_evidence_count: holder.attribution.observation_count(),
        token_account_count: holder.value.token_account_count,
        public_balance: holder_amount_report(holder.value.amount),
        activity_transaction_count: holder.activity.transaction_count,
        public_balance_increase: holder_amount_report(holder.activity.public_balance_increase),
        public_balance_decrease: holder_amount_report(holder.activity.public_balance_decrease),
        public_activity_volume: holder_amount_report(holder.activity.volume()?),
        runtime_account_owner: holder.runtime_account_owner.map(|evidence| {
            RuntimeAccountOwnerReport {
                source: "committed_system_owner_instruction",
                program_id: bs58::encode(evidence.program_id).into_string(),
                observation_count: evidence.observation_count,
                owner_change_count: evidence.owner_change_count,
                conflict_count: evidence.conflict_count,
                proves_pda_derivation: false,
                last_observation: evidence.final_observation,
            }
        }),
    })
}

fn serialize_top_holders<'a>(
    holders: impl Iterator<Item = &'a ClassifiedHolderValue>,
    registry: &[u8],
) -> Result<Vec<ClassifiedHolderReport>> {
    holders
        .take(TOP_CLASSIFIED_HOLDER_LIMIT)
        .map(|holder| serialize_classified_holder(holder, registry))
        .collect()
}

fn serialize_holders<'a>(
    holders: impl Iterator<Item = &'a ClassifiedHolderValue>,
    registry: &[u8],
) -> Result<Vec<ClassifiedHolderReport>> {
    holders
        .map(|holder| serialize_classified_holder(holder, registry))
        .collect()
}

fn validate_complete_off_curve_unattributed_holders(
    holders: &[ClassifiedHolderReport],
    class_total: &HolderAuthorityClassReport,
) -> Result<()> {
    ensure!(
        class_total.authority_kind == HolderAuthorityKind::OffCurveUnattributed.name(),
        "off-curve unattributed holder list has the wrong class total"
    );
    ensure!(
        holders.iter().all(|holder| {
            holder.authority_kind == HolderAuthorityKind::OffCurveUnattributed.name()
                && holder.pda_program_id.is_none()
        }),
        "off-curve unattributed holder list contains another class or an attributed program"
    );
    let token_account_count = holders.iter().try_fold(0u64, |sum, holder| {
        sum.checked_add(holder.token_account_count)
            .context("complete off-curve holder token-account count overflow")
    })?;
    let public_balance = holders.iter().try_fold(0u128, |sum, holder| {
        let amount = holder
            .public_balance
            .raw_amount
            .parse::<u128>()
            .context("complete off-curve holder balance is not a raw integer")?;
        sum.checked_add(amount)
            .context("complete off-curve holder balance overflow")
    })?;
    ensure!(
        u64::try_from(holders.len())? == class_total.holder_count
            && token_account_count == class_total.token_account_count
            && public_balance
                == class_total
                    .public_balance
                    .raw_amount
                    .parse::<u128>()
                    .context("off-curve class total balance is not a raw integer")?,
        "complete off-curve unattributed holder list does not reconcile with its class total"
    );
    Ok(())
}

fn build_holder_authority_report(
    reducer: &TargetBalanceReducer,
    registry: &[u8],
    stage: &HolderAuthorityStage,
    complete: bool,
) -> Result<HolderAuthorityReport> {
    let mut by_owner = HashMap::<u32, ([u8; 32], FinalHolderValue)>::new();
    for account in reducer.accounts() {
        let AccountLifecycle::Open { owner, amount } = account.lifecycle else {
            continue;
        };
        if amount == 0 {
            continue;
        }
        let owner_registry_id = registry_id_for_key(registry, &owner)
            .context("positive holder owner is absent from the source registry")?;
        let (_, value) = by_owner
            .entry(owner_registry_id)
            .or_insert((owner, FinalHolderValue::default()));
        value.amount = value
            .amount
            .checked_add(u128::from(amount))
            .context("classified holder balance overflow")?;
        checked_add(
            &mut value.token_account_count,
            1,
            "classified holder token-account count overflow",
        )?;
    }

    let mut holders = Vec::new();
    holders
        .try_reserve_exact(by_owner.len())
        .context("reserve classified holders")?;
    for (owner_registry_id, (owner, value)) in by_owner {
        let signer_transaction_count = stage.signer_transaction_count(owner_registry_id)?;
        let on_curve = SolanaPubkey::new_from_array(owner).is_on_curve();
        ensure!(
            signer_transaction_count == 0 || on_curve,
            "an observed transaction signer is not on the Ed25519 curve"
        );
        let attribution = stage
            .program_attribution
            .get(&owner_registry_id)
            .copied()
            .unwrap_or_default();
        let (authority_kind, program_registry_id) = if signer_transaction_count != 0 {
            (HolderAuthorityKind::ObservedTransactionSigner, None)
        } else if !on_curve {
            match attribution.attributed_program() {
                Some(program_registry_id) => (
                    HolderAuthorityKind::AttributedProgramDerivedAddress,
                    Some(program_registry_id),
                ),
                None => (HolderAuthorityKind::OffCurveUnattributed, None),
            }
        } else {
            (HolderAuthorityKind::UnclassifiedOnCurve, None)
        };
        holders.push(ClassifiedHolderValue {
            owner,
            owner_registry_id,
            value,
            activity: stage.activity(owner_registry_id)?,
            authority_kind,
            signer_transaction_count,
            program_registry_id,
            attribution,
            runtime_account_owner: stage
                .runtime_account_owners
                .get(&owner_registry_id)
                .copied(),
        });
    }
    holders.sort_unstable_by(holder_sort);

    let total_public_balance =
        reducer
            .accounts()
            .iter()
            .try_fold(0u128, |sum, account| match account.lifecycle {
                AccountLifecycle::Open { amount, .. } => sum
                    .checked_add(u128::from(amount))
                    .context("classified final public balance overflow"),
                AccountLifecycle::Closed => Ok(sum),
            })?;
    let total_token_accounts = reducer
        .accounts()
        .iter()
        .filter(|account| {
            matches!(
                account.lifecycle,
                AccountLifecycle::Open { amount, .. } if amount != 0
            )
        })
        .count();

    let mut class_totals = Vec::with_capacity(HolderAuthorityKind::ALL.len());
    for authority_kind in HolderAuthorityKind::ALL {
        let mut holder_count = 0u64;
        let mut token_account_count = 0u64;
        let mut amount = 0u128;
        for holder in holders
            .iter()
            .filter(|holder| holder.authority_kind == authority_kind)
        {
            checked_add(
                &mut holder_count,
                1,
                "holder authority class count overflow",
            )?;
            token_account_count = token_account_count
                .checked_add(holder.value.token_account_count)
                .context("holder authority class token-account count overflow")?;
            amount = amount
                .checked_add(holder.value.amount)
                .context("holder authority class balance overflow")?;
        }
        class_totals.push(HolderAuthorityClassReport {
            authority_kind: authority_kind.name(),
            holder_count,
            token_account_count,
            public_balance: holder_amount_report(amount),
        });
    }
    ensure!(
        class_totals.iter().map(|row| row.holder_count).sum::<u64>()
            == u64::try_from(holders.len())?
            && class_totals
                .iter()
                .map(|row| row.token_account_count)
                .sum::<u64>()
                == u64::try_from(total_token_accounts)?
            && class_totals
                .iter()
                .map(|row| row.public_balance.raw_amount.parse::<u128>())
                .collect::<std::result::Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<u128>()
                == total_public_balance,
        "holder authority class totals do not reconcile"
    );

    let mut by_program = HashMap::<u32, ProgramHoldingValue>::new();
    for holder in holders.iter().filter(|holder| {
        holder.authority_kind == HolderAuthorityKind::AttributedProgramDerivedAddress
    }) {
        let program_registry_id = holder
            .program_registry_id
            .context("attributed PDA holder has no program")?;
        let value = by_program.entry(program_registry_id).or_default();
        checked_add(
            &mut value.holder_count,
            1,
            "program PDA holder count overflow",
        )?;
        value.token_account_count = value
            .token_account_count
            .checked_add(holder.value.token_account_count)
            .context("program PDA token-account count overflow")?;
        value.amount = value
            .amount
            .checked_add(holder.value.amount)
            .context("program PDA balance overflow")?;
        value.owner_activity_transaction_links = value
            .owner_activity_transaction_links
            .checked_add(holder.activity.transaction_count)
            .context("program owner activity transaction link overflow")?;
        value.public_balance_increase = value
            .public_balance_increase
            .checked_add(holder.activity.public_balance_increase)
            .context("program public balance increase overflow")?;
        value.public_balance_decrease = value
            .public_balance_decrease
            .checked_add(holder.activity.public_balance_decrease)
            .context("program public balance decrease overflow")?;
    }
    let mut program_values = by_program.into_iter().collect::<Vec<_>>();
    program_values.sort_unstable_by(|(left_id, left), (right_id, right)| {
        right.amount.cmp(&left.amount).then_with(|| {
            raw_registry_key(registry, *left_id)
                .expect("validated program registry ID")
                .cmp(&raw_registry_key(registry, *right_id).expect("validated program registry ID"))
        })
    });
    let holdings_by_program = program_values
        .into_iter()
        .map(|(program_registry_id, value)| {
            let program = raw_registry_key(registry, program_registry_id)
                .context("program holdings registry ID cannot be resolved")?;
            Ok(ProgramHoldingReport {
                program_id: bs58::encode(program).into_string(),
                pda_holder_count: value.holder_count,
                token_account_count: value.token_account_count,
                public_balance: holder_amount_report(value.amount),
                owner_activity_transaction_links: value.owner_activity_transaction_links,
                public_balance_increase: holder_amount_report(value.public_balance_increase),
                public_balance_decrease: holder_amount_report(value.public_balance_decrease),
                public_activity_volume: holder_amount_report(
                    value
                        .public_balance_increase
                        .checked_add(value.public_balance_decrease)
                        .context("program public activity volume overflow")?,
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let largest_25_all = serialize_top_holders(holders.iter(), registry)?;
    let largest_25_by_class = LargestHoldersByClassReport {
        observed_transaction_signer: serialize_top_holders(
            holders.iter().filter(|holder| {
                holder.authority_kind == HolderAuthorityKind::ObservedTransactionSigner
            }),
            registry,
        )?,
        attributed_program_derived_address: serialize_top_holders(
            holders.iter().filter(|holder| {
                holder.authority_kind == HolderAuthorityKind::AttributedProgramDerivedAddress
            }),
            registry,
        )?,
        off_curve_unattributed: serialize_top_holders(
            holders.iter().filter(|holder| {
                holder.authority_kind == HolderAuthorityKind::OffCurveUnattributed
            }),
            registry,
        )?,
        unclassified_on_curve: serialize_top_holders(
            holders
                .iter()
                .filter(|holder| holder.authority_kind == HolderAuthorityKind::UnclassifiedOnCurve),
            registry,
        )?,
    };
    let attributed_program_holders = serialize_holders(
        holders.iter().filter(|holder| {
            holder.authority_kind == HolderAuthorityKind::AttributedProgramDerivedAddress
        }),
        registry,
    )?;
    let off_curve_unattributed_holders = serialize_holders(
        holders
            .iter()
            .filter(|holder| holder.authority_kind == HolderAuthorityKind::OffCurveUnattributed),
        registry,
    )?;
    let off_curve_unattributed_total = class_totals
        .iter()
        .find(|row| row.authority_kind == HolderAuthorityKind::OffCurveUnattributed.name())
        .context("off-curve unattributed class total is missing")?;
    validate_complete_off_curve_unattributed_holders(
        &off_curve_unattributed_holders,
        off_curve_unattributed_total,
    )?;
    let mut holders_by_activity = holders.clone();
    holders_by_activity.sort_unstable_by(holder_activity_sort);
    let largest_25_by_activity_all = serialize_top_holders(holders_by_activity.iter(), registry)?;
    let largest_25_by_activity_by_class = LargestHoldersByClassReport {
        observed_transaction_signer: serialize_top_holders(
            holders_by_activity.iter().filter(|holder| {
                holder.authority_kind == HolderAuthorityKind::ObservedTransactionSigner
            }),
            registry,
        )?,
        attributed_program_derived_address: serialize_top_holders(
            holders_by_activity.iter().filter(|holder| {
                holder.authority_kind == HolderAuthorityKind::AttributedProgramDerivedAddress
            }),
            registry,
        )?,
        off_curve_unattributed: serialize_top_holders(
            holders_by_activity.iter().filter(|holder| {
                holder.authority_kind == HolderAuthorityKind::OffCurveUnattributed
            }),
            registry,
        )?,
        unclassified_on_curve: serialize_top_holders(
            holders_by_activity
                .iter()
                .filter(|holder| holder.authority_kind == HolderAuthorityKind::UnclassifiedOnCurve),
            registry,
        )?,
    };

    Ok(HolderAuthorityReport {
        complete,
        definitions: HolderAuthorityDefinitions {
            observed_transaction_signer: "the owner address is on the Ed25519 curve and signed at least one indexed top-level transaction; this is a wallet candidate, not proof of a human",
            attributed_program_derived_address: "the owner address is off the Ed25519 curve and one program is attributed by a decoded DEX program-authority role, a direct depth-2 CPI authorization, or both",
            off_curve_unattributed: "the owner address is off the Ed25519 curve, but this data does not identify one program",
            unclassified_on_curve: "the owner address is on the Ed25519 curve but did not sign an indexed SPYx transaction",
            program_attribution: "the program is derivation attribution evidence, not the live Solana account owner field; the archive has no holder-authority account snapshot",
            runtime_account_owner: "the last committed raw System Program CreateAccount, Assign, CreateAccountWithSeed, AllocateWithSeed, AssignWithSeed, or CreateAccountAllowPrefund owner-setting instruction with a runtime-valid minimum account layout and decodable owner prefix observed in the bounded SPYx transaction set; transaction_id is the zero-based consolidated transaction ordinal; later non-SPYx transactions can exist, so this is not a guaranteed live-final owner, is not PDA derivation evidence, and does not change the authority classes or holdings-by-program totals",
            nested_cpi_attribution_limit: "only a committed depth-2 Token CPI can identify its outer program as the source of an off-curve authority signature; deeper CPI signer privilege can be inherited through intermediate programs, and stack heights and program logs do not record where that signer privilege first appeared",
            public_activity_volume: "the sum of the absolute transaction-final owner balance delta per indexed transaction; token-account movements within one owner cancel, bilateral transfers appear once for each side, and ownership reassignment moves the public balance from the previous owner to the next owner",
        },
        class_totals,
        largest_25_all,
        largest_25_by_class,
        largest_25_by_activity_all,
        largest_25_by_activity_by_class,
        attributed_program_holders,
        off_curve_unattributed_holders,
        holdings_by_program,
    })
}

#[derive(Debug, Clone, Copy)]
struct CandidateClaimAllocation {
    custody_owner_registry_id: u32,
    program_id: Option<[u8; 32]>,
    evidence: CandidatePrincipalValue,
    attributed_claim: u128,
}

fn custody_program_id(
    stage: &HolderAuthorityStage,
    custody_owner_registry_id: u32,
    registry: &[u8],
) -> Result<Option<[u8; 32]>> {
    if let Some(program_registry_id) = stage
        .program_attribution
        .get(&custody_owner_registry_id)
        .copied()
        .and_then(ProgramAttributionEvidence::attributed_program)
    {
        return raw_registry_key(registry, program_registry_id)
            .context("portfolio custody program registry ID cannot be resolved")
            .map(Some);
    }
    let Some(evidence) = stage.runtime_account_owners.get(&custody_owner_registry_id) else {
        return Ok(None);
    };
    let system_program = parse_pubkey(SYSTEM_PROGRAM, "System Program")?;
    let legacy_token_program = parse_pubkey(LEGACY_TOKEN_PROGRAM, "legacy token program")?;
    let token_2022_program = parse_pubkey(TOKEN_2022_PROGRAM, "Token-2022 program")?;
    Ok((evidence.program_id != system_program
        && evidence.program_id != legacy_token_program
        && evidence.program_id != token_2022_program)
        .then_some(evidence.program_id))
}

fn build_authority_portfolio_history_state(
    owner_balances: &[u128],
    positive_owner_registry_ids: &[u32],
    owner_on_curve: &[bool],
    registry: &[u8],
    stage: &HolderAuthorityStage,
) -> Result<BTreeMap<u32, PortfolioState>> {
    let registry_entries = registry.len() / KEY_BYTES;
    ensure!(
        registry.len().is_multiple_of(KEY_BYTES)
            && owner_balances.len() == registry_entries.saturating_add(1)
            && owner_on_curve.len() == owner_balances.len(),
        "authority portfolio history owner table differs from the registry"
    );

    let mut state = BTreeMap::<u32, PortfolioState>::new();
    for &registry_id in positive_owner_registry_ids {
        let index = usize::try_from(registry_id)?;
        let direct_public_balance = *owner_balances
            .get(index)
            .context("positive history owner registry ID exceeds the owner table")?;
        ensure!(
            direct_public_balance != 0,
            "positive history owner index contains a zero balance"
        );
        if owner_on_curve[index] {
            state.insert(
                registry_id,
                PortfolioState {
                    direct_public_balance,
                    claim_components: Vec::new(),
                },
            );
        }
    }

    let mut candidates_by_custody = BTreeMap::<u32, Vec<(u32, HistoryCandidatePrincipal)>>::new();
    for (&(authority_registry_id, custody_owner_registry_id), &evidence) in
        &stage.candidate_principals
    {
        if evidence.net_principal()? == 0 {
            continue;
        }
        candidates_by_custody
            .entry(custody_owner_registry_id)
            .or_default()
            .push((authority_registry_id, evidence.into()));
    }

    for (custody_owner_registry_id, candidates) in candidates_by_custody {
        let direct_custody_balance = owner_balances
            .get(usize::try_from(custody_owner_registry_id)?)
            .copied()
            .context("history custody owner registry ID exceeds the owner table")?;
        if direct_custody_balance == 0 {
            continue;
        }
        if owner_on_curve[usize::try_from(custody_owner_registry_id)?] {
            continue;
        }

        let program_id = custody_program_id(stage, custody_owner_registry_id, registry)?;
        let custody_allocation = allocate_candidate_claims(direct_custody_balance, candidates)?;
        for allocation in custody_allocation.allocations {
            ensure!(
                owner_on_curve[usize::try_from(allocation.authority_registry_id)?],
                "history candidate authority is off the Ed25519 curve"
            );
            state
                .entry(allocation.authority_registry_id)
                .or_default()
                .claim_components
                .push(PortfolioClaimComponentState {
                    custody_owner_registry_id,
                    program_id,
                    evidence: allocation.evidence,
                    attributed_claim: allocation.attributed_claim,
                });
        }
    }

    for value in state.values_mut() {
        value.normalize_and_validate()?;
    }
    Ok(state)
}

fn report_raw_amount(amount: &HolderAmountReport, label: &'static str) -> Result<u128> {
    amount
        .raw_amount
        .parse::<u128>()
        .with_context(|| format!("{label} is not an unsigned integer"))
}

fn validate_authority_portfolio_history_final(
    final_state: &BTreeMap<u32, PortfolioState>,
    final_report: &AuthorityPortfolioReport,
    registry: &[u8],
) -> Result<()> {
    ensure!(
        final_state.len() == final_report.portfolios.len(),
        "authority portfolio history final authority count differs from the final report"
    );
    for portfolio in &final_report.portfolios {
        let authority = parse_pubkey(&portfolio.authority, "history final authority")?;
        let authority_registry_id = registry_id_for_key(registry, &authority)
            .context("history final authority is absent from the registry")?;
        let state = final_state
            .get(&authority_registry_id)
            .context("final authority report row is absent from history state")?;
        let direct = report_raw_amount(
            &portfolio.direct_public_balance,
            "final direct public balance",
        )?;
        let claim = report_raw_amount(
            &portfolio.estimated_defi_claim,
            "final estimated DeFi claim",
        )?;
        let total = report_raw_amount(
            &portfolio.estimated_total_exposure,
            "final estimated total exposure",
        )?;
        ensure!(
            state.direct_public_balance == direct
                && state.estimated_defi_claim()? == claim
                && state.estimated_total_exposure()? == total,
            "authority portfolio history final totals differ from the final report"
        );
        ensure!(
            state.claim_components.len() == portfolio.claim_components.len(),
            "authority portfolio history final component count differs from the final report"
        );
        for (history_component, report_component) in state
            .claim_components
            .iter()
            .zip(&portfolio.claim_components)
        {
            let custody_owner =
                raw_registry_key(registry, history_component.custody_owner_registry_id)
                    .context("history final custody owner cannot be resolved")?;
            let program_id = history_component
                .program_id
                .map(|program| bs58::encode(program).into_string());
            ensure!(
                report_component.custody_owner == bs58::encode(custody_owner).into_string()
                    && report_component.program_id == program_id
                    && report_raw_amount(
                        &report_component.observed_deposited_principal,
                        "final deposited principal",
                    )? == history_component.evidence.observed_deposited_principal
                    && report_raw_amount(
                        &report_component.observed_returned_principal,
                        "final returned principal",
                    )? == history_component.evidence.observed_returned_principal
                    && report_raw_amount(
                        &report_component.candidate_net_principal,
                        "final candidate net principal",
                    )? == history_component.evidence.net_principal()
                    && report_raw_amount(
                        &report_component.attributed_claim,
                        "final attributed claim",
                    )? == history_component.attributed_claim
                    && report_component.deposit_transaction_count
                        == history_component.evidence.deposit_transaction_count
                    && report_component.return_transaction_count
                        == history_component.evidence.return_transaction_count,
                "authority portfolio history final component differs from the final report"
            );
        }
    }
    Ok(())
}

fn candidate_flow_evidence_report(
    stage: &HolderAuthorityStage,
    authority_registry_id: u32,
    custody_owner_registry_id: u32,
    aggregate: CandidatePrincipalValue,
) -> Result<Vec<CandidateFlowEvidenceReport>> {
    let flows = stage
        .candidate_flow_evidence
        .get(&(authority_registry_id, custody_owner_registry_id))
        .map(Vec::as_slice)
        .unwrap_or_default();
    ensure!(
        flows
            .windows(2)
            .all(|pair| { pair[0].location.transaction_id < pair[1].location.transaction_id }),
        "candidate flow evidence is not in canonical transaction order"
    );

    let mut deposited = 0u128;
    let mut returned = 0u128;
    let mut deposit_count = 0u64;
    let mut return_count = 0u64;
    for flow in flows {
        ensure!(
            flow.raw_amount != 0
                && flow.matched_principal_raw_amount != 0
                && flow.matched_principal_raw_amount <= flow.raw_amount,
            "candidate flow evidence has an invalid amount"
        );
        match flow.direction {
            CandidateFlowDirection::Deposit => {
                ensure!(
                    flow.raw_amount == flow.matched_principal_raw_amount,
                    "candidate deposit evidence is partially matched"
                );
                deposited = deposited
                    .checked_add(flow.matched_principal_raw_amount)
                    .context("candidate deposit evidence amount overflow")?;
                checked_add(
                    &mut deposit_count,
                    1,
                    "candidate deposit evidence count overflow",
                )?;
            }
            CandidateFlowDirection::Return => {
                returned = returned
                    .checked_add(flow.matched_principal_raw_amount)
                    .context("candidate return evidence amount overflow")?;
                checked_add(
                    &mut return_count,
                    1,
                    "candidate return evidence count overflow",
                )?;
            }
        }
    }
    ensure!(
        deposited == aggregate.observed_deposited_principal
            && returned == aggregate.observed_returned_principal
            && deposit_count == aggregate.deposit_transaction_count
            && return_count == aggregate.return_transaction_count,
        "candidate flow evidence does not reconcile with its aggregate"
    );

    Ok(flows
        .iter()
        .map(|flow| CandidateFlowEvidenceReport {
            transaction_id: flow.location.transaction_id,
            slot: flow.location.slot,
            block_time: flow.location.block_time,
            direction: flow.direction.name(),
            raw_amount: flow.raw_amount.to_string(),
            matched_principal_raw_amount: (flow.matched_principal_raw_amount != flow.raw_amount)
                .then(|| flow.matched_principal_raw_amount.to_string()),
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn build_authority_portfolio_report(
    reducer: &TargetBalanceReducer,
    registry: &[u8],
    stage: &HolderAuthorityStage,
    source_binding: AuthorityPortfolioSourceBinding,
    complete: bool,
    transactions_scanned: u64,
) -> Result<AuthorityPortfolioReport> {
    let mut final_balances = HashMap::<u32, ([u8; 32], u128)>::new();
    for account in reducer.accounts() {
        let AccountLifecycle::Open { owner, amount } = account.lifecycle else {
            continue;
        };
        if amount == 0 {
            continue;
        }
        let owner_registry_id = registry_id_for_key(registry, &owner)
            .context("portfolio holder owner is absent from the source registry")?;
        let (_, balance) = final_balances
            .entry(owner_registry_id)
            .or_insert((owner, 0));
        *balance = balance
            .checked_add(u128::from(amount))
            .context("portfolio final owner balance overflow")?;
    }

    let mut positive_off_curve_custody = HashMap::<u32, ([u8; 32], u128)>::new();
    for (&registry_id, &(owner, balance)) in &final_balances {
        if !SolanaPubkey::new_from_array(owner).is_on_curve() {
            positive_off_curve_custody.insert(registry_id, (owner, balance));
        }
    }

    let mut candidates_by_custody = HashMap::<u32, Vec<(u32, CandidatePrincipalValue)>>::new();
    for (&(authority_registry_id, custody_owner_registry_id), &evidence) in
        &stage.candidate_principals
    {
        if evidence.net_principal()? == 0 {
            continue;
        }
        candidates_by_custody
            .entry(custody_owner_registry_id)
            .or_default()
            .push((authority_registry_id, evidence));
    }
    for candidates in candidates_by_custody.values_mut() {
        candidates.sort_unstable_by_key(|(authority_registry_id, _)| *authority_registry_id);
    }

    let mut custody_owner_registry_ids = positive_off_curve_custody
        .keys()
        .copied()
        .collect::<Vec<_>>();
    custody_owner_registry_ids.sort_unstable();
    custody_owner_registry_ids.dedup();

    let mut protocol_custody = Vec::with_capacity(custody_owner_registry_ids.len());
    let mut allocations_by_authority = HashMap::<u32, Vec<CandidateClaimAllocation>>::new();
    for custody_owner_registry_id in custody_owner_registry_ids {
        let custody_owner = raw_registry_key(registry, custody_owner_registry_id)
            .context("portfolio custody owner registry ID cannot be resolved")?;
        ensure!(
            !SolanaPubkey::new_from_array(custody_owner).is_on_curve(),
            "candidate custody owner is on the Ed25519 curve"
        );
        let direct_custody_balance = positive_off_curve_custody
            .get(&custody_owner_registry_id)
            .map_or(0, |(_, balance)| *balance);
        let candidates = candidates_by_custody
            .remove(&custody_owner_registry_id)
            .unwrap_or_default();
        let candidate_authority_count = u64::try_from(candidates.len())?;
        let custody_allocation = allocate_candidate_claims(
            direct_custody_balance,
            candidates
                .into_iter()
                .map(|(authority_registry_id, evidence)| {
                    (
                        authority_registry_id,
                        HistoryCandidatePrincipal::from(evidence),
                    )
                })
                .collect(),
        )?;
        let candidate_net_principal = custody_allocation.candidate_net_principal;
        let attributed_claim = custody_allocation.attributed_claim;
        let program_id = custody_program_id(stage, custody_owner_registry_id, registry)?;
        for allocation in custody_allocation.allocations {
            allocations_by_authority
                .entry(allocation.authority_registry_id)
                .or_default()
                .push(CandidateClaimAllocation {
                    custody_owner_registry_id,
                    program_id,
                    evidence: CandidatePrincipalValue::from(allocation.evidence),
                    attributed_claim: allocation.attributed_claim,
                });
        }

        let unallocated_custody = direct_custody_balance
            .checked_sub(attributed_claim)
            .context("attributed claim exceeds direct custody")?;
        let claim_excess = candidate_net_principal
            .checked_sub(attributed_claim)
            .context("attributed claim exceeds candidate principal")?;
        protocol_custody.push(ProtocolCustodyRowReport {
            custody_owner: bs58::encode(custody_owner).into_string(),
            program_id: program_id.map(|program| bs58::encode(program).into_string()),
            direct_custody_balance: holder_amount_report(direct_custody_balance),
            candidate_net_principal: holder_amount_report(candidate_net_principal),
            attributed_claim: holder_amount_report(attributed_claim),
            unallocated_custody: holder_amount_report(unallocated_custody),
            claim_excess: holder_amount_report(claim_excess),
            candidate_authority_count,
            confidence: "heuristic_owner_net_flow_capped_by_current_custody",
        });
    }

    let mut portfolio_authority_registry_ids = final_balances
        .iter()
        .filter_map(|(&registry_id, &(owner, _))| {
            SolanaPubkey::new_from_array(owner)
                .is_on_curve()
                .then_some(registry_id)
        })
        .chain(allocations_by_authority.keys().copied())
        .collect::<Vec<_>>();
    portfolio_authority_registry_ids.sort_unstable();
    portfolio_authority_registry_ids.dedup();
    let mut portfolio_values = Vec::<(u32, u128, AuthorityPortfolioRowReport)>::with_capacity(
        portfolio_authority_registry_ids.len(),
    );
    for authority_registry_id in portfolio_authority_registry_ids {
        let mut allocations = allocations_by_authority
            .remove(&authority_registry_id)
            .unwrap_or_default();
        allocations.sort_unstable_by_key(|allocation| allocation.custody_owner_registry_id);
        let authority = raw_registry_key(registry, authority_registry_id)
            .context("portfolio authority registry ID cannot be resolved")?;
        ensure!(
            SolanaPubkey::new_from_array(authority).is_on_curve(),
            "candidate portfolio authority is off the Ed25519 curve"
        );
        let direct_public_balance = final_balances
            .get(&authority_registry_id)
            .map_or(0, |(_, balance)| *balance);
        let estimated_defi_claim =
            allocations
                .iter()
                .try_fold(0u128, |sum, allocation| -> Result<u128> {
                    sum.checked_add(allocation.attributed_claim)
                        .context("portfolio estimated claim overflow")
                })?;
        let estimated_total_exposure = direct_public_balance
            .checked_add(estimated_defi_claim)
            .context("portfolio estimated total exposure overflow")?;
        let authority_kind = if stage.signer_transaction_count(authority_registry_id)? != 0 {
            "observed_transaction_signer"
        } else {
            "other_on_curve_account"
        };
        let mut programs_used = allocations
            .iter()
            .filter_map(|allocation| allocation.program_id)
            .collect::<Vec<_>>();
        programs_used.sort_unstable();
        programs_used.dedup();
        let claim_components = allocations
            .into_iter()
            .map(|allocation| {
                let custody_owner =
                    raw_registry_key(registry, allocation.custody_owner_registry_id)
                        .context("portfolio component custody owner cannot be resolved")?;
                Ok(AuthorityClaimComponentReport {
                    custody_owner: bs58::encode(custody_owner).into_string(),
                    program_id: allocation
                        .program_id
                        .map(|program| bs58::encode(program).into_string()),
                    observed_deposited_principal: holder_amount_report(
                        allocation.evidence.observed_deposited_principal,
                    ),
                    observed_returned_principal: holder_amount_report(
                        allocation.evidence.observed_returned_principal,
                    ),
                    candidate_net_principal: holder_amount_report(
                        allocation.evidence.net_principal()?,
                    ),
                    attributed_claim: holder_amount_report(allocation.attributed_claim),
                    deposit_transaction_count: allocation.evidence.deposit_transaction_count,
                    return_transaction_count: allocation.evidence.return_transaction_count,
                    candidate_flow_evidence: candidate_flow_evidence_report(
                        stage,
                        authority_registry_id,
                        allocation.custody_owner_registry_id,
                        allocation.evidence,
                    )?,
                    confidence: "heuristic_owner_net_flow_capped_by_current_custody",
                })
            })
            .collect::<Result<Vec<_>>>()?;
        portfolio_values.push((
            authority_registry_id,
            estimated_total_exposure,
            AuthorityPortfolioRowReport {
                authority: bs58::encode(authority).into_string(),
                authority_kind,
                direct_public_balance: holder_amount_report(direct_public_balance),
                estimated_defi_claim: holder_amount_report(estimated_defi_claim),
                estimated_total_exposure: holder_amount_report(estimated_total_exposure),
                programs_used: programs_used
                    .into_iter()
                    .map(|program| bs58::encode(program).into_string())
                    .collect(),
                claim_components,
            },
        ));
    }
    portfolio_values
        .sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let portfolios = portfolio_values
        .into_iter()
        .map(|(_, _, report)| report)
        .collect();

    let mut pda_creation_provenance = stage
        .pda_creation_provenance
        .iter()
        .filter(|value| positive_off_curve_custody.contains_key(&value.subject_registry_id))
        .cloned()
        .collect::<Vec<_>>();
    pda_creation_provenance.sort_unstable_by(|left, right| {
        left.subject_registry_id
            .cmp(&right.subject_registry_id)
            .then_with(|| left.location.cmp(&right.location))
    });
    let pda_creation_provenance = pda_creation_provenance
        .into_iter()
        .map(|value| {
            let subject = raw_registry_key(registry, value.subject_registry_id)
                .context("creation provenance subject cannot be resolved")?;
            let direct_caller_program_id = value
                .direct_caller_program_registry_id
                .map(|registry_id| {
                    raw_registry_key(registry, registry_id)
                        .context("creation provenance caller cannot be resolved")
                        .map(|program| bs58::encode(program).into_string())
                })
                .transpose()?;
            let signer_candidates = value
                .signer_candidate_registry_ids
                .into_iter()
                .map(|registry_id| {
                    raw_registry_key(registry, registry_id)
                        .context("creation signer candidate cannot be resolved")
                        .map(|signer| bs58::encode(signer).into_string())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(PdaCreationProvenanceReport {
                subject_pda: bs58::encode(subject).into_string(),
                event_kind: value.instruction.event_kind(),
                system_instruction: value.instruction.name(),
                runtime_owner_program_id: bs58::encode(value.runtime_owner_program_id)
                    .into_string(),
                direct_caller_program_id,
                create_with_seed_base: value
                    .create_with_seed_base
                    .map(|base| bs58::encode(base).into_string()),
                signer_candidates,
                confidence: "provenance_only_no_amount_assigned",
                proves_beneficial_ownership: false,
                location: value.location,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(AuthorityPortfolioReport {
        schema_version: 1,
        artifact_kind: "spyx_authority_portfolio_heuristic",
        source_binding,
        coverage: AuthorityPortfolioCoverage {
            complete,
            method: "committed_non_dex_owner_net_flow_v1",
            candidate_flow_evidence_complete: true,
            transactions_scanned,
            parsed_dex_swap_transactions_excluded: stage.parsed_dex_swap_transactions_excluded,
            candidate_deposit_transactions: stage.candidate_deposit_transactions,
            candidate_return_transactions: stage.candidate_return_transactions,
            ambiguous_owner_delta_transactions_excluded: stage
                .ambiguous_owner_delta_transactions_excluded,
            current_positive_off_curve_custody_owners: u64::try_from(
                positive_off_curve_custody.len(),
            )?,
            definitions: AuthorityPortfolioDefinitions {
                estimated_defi_claim: "a conservative heuristic: committed non-DEX transaction-final SPYx owner-net deposits from the same on-curve signer, less matched returns, proportionally capped by the custody owner's current physical SPYx balance; it is not a decoded protocol position, yield balance, debt-adjusted balance, or proof of beneficial ownership",
                creation_provenance: "a committed System Program owner-setting event and signer candidates referenced by its enclosing top-level instruction; a payer, signer, base, runtime owner program, or direct caller is provenance only and receives no amount",
                unallocated_custody: "current physical SPYx custody not assigned by this heuristic; unknown programs and unsupported or ambiguous flows remain visible here",
                candidate_flow_evidence: "the canonical transaction ordinal, slot, optional block time, direction, exact two-owner public-balance delta, and matched principal contribution for every candidate flow used by an emitted claim component; the component identifies the authority and custody owner, and transaction_id can resolve the full signature from the bound transaction stream",
            },
        },
        portfolios,
        protocol_custody,
        pda_creation_provenance,
    })
}

fn replay_state_report(
    reducer: &TargetBalanceReducer,
    history_complete: bool,
) -> Result<ReplayStateReport> {
    let mut open_accounts = 0_u64;
    let mut positive_public_balance_accounts = 0_u64;
    let mut state_hasher = Sha256::new();
    state_hasher.update(b"blockzilla-spyx-public-account-state-v1\0");

    for account in reducer.accounts() {
        state_hasher.update(account.address);
        state_hasher.update(account.generation.to_le_bytes());
        match account.lifecycle {
            AccountLifecycle::Closed => state_hasher.update([0]),
            AccountLifecycle::Open { owner, amount } => {
                checked_add(&mut open_accounts, 1, "open replay account count overflow")?;
                if amount != 0 {
                    checked_add(
                        &mut positive_public_balance_accounts,
                        1,
                        "positive replay account count overflow",
                    )?;
                }
                state_hasher.update([1]);
                state_hasher.update(owner);
                state_hasher.update(amount.to_le_bytes());
            }
        }
    }

    let tracked_accounts = u64::try_from(reducer.accounts().len())?;
    let closed_accounts = tracked_accounts
        .checked_sub(open_accounts)
        .context("open replay account count exceeds tracked accounts")?;
    let public_raw_balance = reducer
        .checked_total_public_amount()
        .context("replayed public raw balance exceeds u64")?;

    Ok(ReplayStateReport {
        history_complete,
        tracked_accounts,
        open_accounts,
        closed_accounts,
        positive_public_balance_accounts,
        public_raw_balance: public_raw_balance.to_string(),
        state_sha256: hex_digest(state_hasher.finalize().into()),
    })
}

fn cached_owner_registry_id(
    owner: [u8; 32],
    registry: &[u8],
    owner_registry_cache: &mut HashMap<[u8; 32], u32>,
) -> Result<u32> {
    if let Some(registry_id) = owner_registry_cache.get(&owner) {
        return Ok(*registry_id);
    }
    let registry_id = registry_id_for_key(registry, &owner)
        .context("replayed target-account owner is absent from the source registry")?;
    owner_registry_cache.insert(owner, registry_id);
    Ok(registry_id)
}

fn append_open_owner_registry_ids(
    reducer: &TargetBalanceReducer,
    mentioned_target_indices: &[u32],
    registry: &[u8],
    owner_registry_cache: &mut HashMap<[u8; 32], u32>,
    owner_registry_ids: &mut Vec<u32>,
) -> Result<()> {
    for &target_index in mentioned_target_indices {
        let state = reducer
            .account(target_index)
            .context("mentioned target index is outside the replay account table")?;
        if let AccountLifecycle::Open { owner, .. } = state.lifecycle {
            let owner_registry_id =
                cached_owner_registry_id(owner, registry, owner_registry_cache)?;
            owner_registry_ids.push(owner_registry_id);
        }
    }
    Ok(())
}

fn collect_public_owner_deltas(
    changes: &[TargetAccountChange],
    reducer: &TargetBalanceReducer,
    registry: &[u8],
    owner_registry_cache: &mut HashMap<[u8; 32], u32>,
    deltas: &mut Vec<(u32, i128)>,
) -> Result<()> {
    deltas.clear();
    for change in changes {
        if let AccountLifecycle::Open { owner, amount } = change.previous.lifecycle {
            deltas.push((
                cached_owner_registry_id(owner, registry, owner_registry_cache)?,
                -i128::from(amount),
            ));
        }
        let current = reducer
            .account(change.index)
            .context("changed target account is outside the replay account table")?;
        if let AccountLifecycle::Open { owner, amount } = current.lifecycle {
            deltas.push((
                cached_owner_registry_id(owner, registry, owner_registry_cache)?,
                i128::from(amount),
            ));
        }
    }
    deltas.sort_unstable_by_key(|(registry_id, _)| *registry_id);
    let mut cursor = 0usize;
    let mut write = 0usize;
    while cursor < deltas.len() {
        let registry_id = deltas[cursor].0;
        let mut delta = 0i128;
        while cursor < deltas.len() && deltas[cursor].0 == registry_id {
            delta = delta
                .checked_add(deltas[cursor].1)
                .context("transaction owner public balance delta overflow")?;
            cursor += 1;
        }
        if delta != 0 {
            deltas[write] = (registry_id, delta);
            write += 1;
        }
    }
    deltas.truncate(write);
    Ok(())
}

fn observe_public_owner_activity(
    stage: &mut HolderAuthorityStage,
    deltas: &[(u32, i128)],
    transaction_ordinal: u64,
    candidate_flow_location: CandidateFlowLocation,
    transaction_signer_registry_ids: &[u32],
    transaction_has_parsed_dex_swap: bool,
    registry: &[u8],
) -> Result<()> {
    for &(registry_id, delta) in deltas {
        stage.observe_owner_delta(registry_id, delta, transaction_ordinal)?;
    }
    observe_candidate_owner_net_flow(
        stage,
        deltas,
        candidate_flow_location,
        transaction_signer_registry_ids,
        transaction_has_parsed_dex_swap,
        registry,
    )
}

fn observe_candidate_owner_net_flow(
    stage: &mut HolderAuthorityStage,
    deltas: &[(u32, i128)],
    location: CandidateFlowLocation,
    transaction_signer_registry_ids: &[u32],
    transaction_has_parsed_dex_swap: bool,
    registry: &[u8],
) -> Result<()> {
    if transaction_has_parsed_dex_swap {
        checked_add(
            &mut stage.parsed_dex_swap_transactions_excluded,
            1,
            "excluded parsed DEX transaction count overflow",
        )?;
        return Ok(());
    }

    let is_transaction_signer =
        |registry_id: u32| transaction_signer_registry_ids.contains(&registry_id);
    let is_on_curve = |registry_id: u32| -> Result<bool> {
        let key = raw_registry_key(registry, registry_id)
            .context("owner-delta registry ID cannot be resolved")?;
        Ok(SolanaPubkey::new_from_array(key).is_on_curve())
    };

    if deltas.len() == 2 {
        let negative = deltas.iter().find(|(_, delta)| *delta < 0).copied();
        let positive = deltas.iter().find(|(_, delta)| *delta > 0).copied();
        if let (Some((negative_owner, negative_delta)), Some((positive_owner, positive_delta))) =
            (negative, positive)
            && negative_delta.unsigned_abs() == positive_delta.unsigned_abs()
        {
            let amount = positive_delta.unsigned_abs();
            let negative_on_curve = is_on_curve(negative_owner)?;
            let positive_on_curve = is_on_curve(positive_owner)?;
            if negative_on_curve && !positive_on_curve && is_transaction_signer(negative_owner) {
                return stage.observe_candidate_deposit(
                    negative_owner,
                    positive_owner,
                    amount,
                    location,
                );
            }
            if !negative_on_curve && positive_on_curve && is_transaction_signer(positive_owner) {
                return stage.observe_candidate_return(
                    positive_owner,
                    negative_owner,
                    amount,
                    location,
                );
            }
        }
    }

    let mut eligible_on_curve_negative = false;
    let mut eligible_on_curve_positive = false;
    let mut off_curve_negative = false;
    let mut off_curve_positive = false;
    for &(registry_id, delta) in deltas {
        let on_curve = is_on_curve(registry_id)?;
        if delta < 0 {
            eligible_on_curve_negative |= on_curve && is_transaction_signer(registry_id);
            off_curve_negative |= !on_curve;
        } else if delta > 0 {
            eligible_on_curve_positive |= on_curve && is_transaction_signer(registry_id);
            off_curve_positive |= !on_curve;
        }
    }
    if (eligible_on_curve_negative && off_curve_positive)
        || (off_curve_negative && eligible_on_curve_positive)
    {
        checked_add(
            &mut stage.ambiguous_owner_delta_transactions_excluded,
            1,
            "ambiguous candidate owner-delta transaction count overflow",
        )?;
    }
    Ok(())
}

fn begin_owner_projection(
    reducer: &TargetBalanceReducer,
    mentioned_target_indices: &[u32],
    registry: &[u8],
    owner_registry_cache: &mut HashMap<[u8; 32], u32>,
    owner_registry_ids: &mut Vec<u32>,
) -> Result<()> {
    owner_registry_ids.clear();
    append_open_owner_registry_ids(
        reducer,
        mentioned_target_indices,
        registry,
        owner_registry_cache,
        owner_registry_ids,
    )
}

fn finish_owner_projection(
    reducer: &TargetBalanceReducer,
    mentioned_target_indices: &[u32],
    registry: &[u8],
    owner_registry_cache: &mut HashMap<[u8; 32], u32>,
    owner_registry_ids: &mut Vec<u32>,
) -> Result<()> {
    append_open_owner_registry_ids(
        reducer,
        mentioned_target_indices,
        registry,
        owner_registry_cache,
        owner_registry_ids,
    )?;
    owner_registry_ids.sort_unstable();
    owner_registry_ids.dedup();
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ScanEnd {
    Footer(TokenTransactionDumpFooter),
    Prefix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayInputBlocker {
    MetadataMissing,
    InnerInstructionRecordingMissing,
}

impl ReplayInputBlocker {
    const fn code(self) -> &'static str {
        match self {
            Self::MetadataMissing => "metadata_missing",
            Self::InnerInstructionRecordingMissing => "inner_instruction_recording_missing",
        }
    }

    const fn detail(self) -> &'static str {
        match self {
            Self::MetadataMissing => {
                "transaction metadata is absent, so outcome and token-balance oracle data are unknown"
            }
            Self::InnerInstructionRecordingMissing => {
                "successful transaction does not prove a complete recorded inner-instruction list"
            }
        }
    }
}

fn replay_input_blocker(
    summary: Option<ProjectedArchiveV2TokenMetadataSummary>,
) -> Option<ReplayInputBlocker> {
    match summary {
        None => Some(ReplayInputBlocker::MetadataMissing),
        Some(summary) if !summary.has_error && !summary.inner_instructions_present => {
            Some(ReplayInputBlocker::InnerInstructionRecordingMissing)
        }
        Some(_) => None,
    }
}

fn replay_error_report_code(reason: ReplayErrorReason) -> String {
    match reason {
        ReplayErrorReason::UnsupportedEffect(cause) => {
            format!("unsupported_effect:{}", cause.code())
        }
        _ => reason.code().to_owned(),
    }
}

fn checked_add(counter: &mut u64, value: u64, label: &'static str) -> Result<()> {
    *counter = counter.checked_add(value).context(label)?;
    Ok(())
}

fn bump(map: &mut BTreeMap<String, u64>, key: &str) -> Result<()> {
    if let Some(value) = map.get_mut(key) {
        return checked_add(value, 1, "replay map counter overflow");
    }
    map.insert(key.to_owned(), 1);
    Ok(())
}

fn materialize_instruction_names(
    counts: BTreeMap<InstructionNameKey, u64>,
) -> Result<BTreeMap<String, u64>> {
    let mut names = BTreeMap::<String, u64>::new();
    for (key, count) in counts {
        let name = key.display_name();
        if let Some(existing) = names.get_mut(&name) {
            checked_add(
                existing,
                count,
                "materialized instruction name count overflow",
            )?;
        } else {
            names.insert(name, count);
        }
    }
    Ok(names)
}

fn compact_id(reference: CompactPubkey, registry_entries: u32) -> Option<u32> {
    match reference {
        CompactPubkey::Id(id) if id != 0 && id <= registry_entries => Some(id),
        CompactPubkey::Id(_) | CompactPubkey::Raw(_) => None,
    }
}

fn raw_registry_key(registry: &[u8], id: u32) -> Option<[u8; 32]> {
    let index = usize::try_from(id.checked_sub(1)?).ok()?;
    let start = index.checked_mul(KEY_BYTES)?;
    let bytes: [u8; 32] = registry.get(start..start + KEY_BYTES)?.try_into().ok()?;
    Some(bytes)
}

fn build_dex_dispatch(registry: &[u8], registry_entries: u32) -> Result<DispatchTable> {
    let mut program_ids = HashMap::<&'static str, u32>::with_capacity(PROGRAM_SPECS.len());
    for spec in PROGRAM_SPECS {
        let key = parse_pubkey(spec.address, "DEX parser program")?;
        if let Some(registry_id) = registry_id_for_key(registry, &key) {
            program_ids.insert(spec.address, registry_id);
        }
    }
    let dense_len = usize::try_from(registry_entries)?
        .checked_add(1)
        .context("DEX dispatch table length overflow")?;
    Ok(DispatchTable::from_resolver(dense_len, |address| {
        program_ids.get(address).copied()
    }))
}

fn observe_dex_parser_authority(
    stage: &mut HolderAuthorityStage,
    dispatch: &DispatchTable,
    program_registry_id: u32,
    data: &[u8],
    account_indices: &[u8],
    resolved_accounts: &[u32],
    account_scratch: &mut [u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
) -> Result<bool> {
    ensure!(
        account_indices.len() <= account_scratch.len(),
        "DEX instruction account list exceeds the message account limit"
    );
    for (destination, &message_index) in account_scratch.iter_mut().zip(account_indices) {
        *destination = *resolved_accounts
            .get(usize::from(message_index))
            .context("DEX instruction account index exceeds resolved accounts")?;
    }
    if let DexDecodeOutcome::Decoded(decoded) = dispatch.decode(
        program_registry_id,
        data,
        &account_scratch[..account_indices.len()],
    ) {
        if let Some(authority_registry_id) = decoded.accounts.authority {
            stage.observe_parser_authority(authority_registry_id, program_registry_id)?;
        }
        return Ok(matches!(
            decoded.class,
            DexInstructionClass::Swap(_) | DexInstructionClass::Route
        ));
    }
    Ok(false)
}

fn token_owner_authority_layout(
    program: TokenProgram,
    data: &[u8],
    account_count: usize,
) -> Option<(usize, usize)> {
    let decoded = decode_token_instruction(program, data);
    if decoded.status != DecodeStatus::Known {
        return None;
    }
    if let Some(extension) = decoded.extension {
        return (extension.family == ExtensionFamily::TransferFee
            && extension.subtype == 1
            && account_count == 4)
            .then_some((0, 3));
    }
    let (source, authority, exact_accounts) = match decoded.top_level? {
        TopLevelInstruction::Transfer => (0, 2, 3),
        TopLevelInstruction::TransferChecked => (0, 3, 4),
        TopLevelInstruction::Approve => (0, 2, 3),
        TopLevelInstruction::ApproveChecked => (0, 3, 4),
        TopLevelInstruction::Revoke => (0, 1, 2),
        TopLevelInstruction::SetAuthority => (0, 1, 2),
        TopLevelInstruction::Burn | TopLevelInstruction::BurnChecked => (0, 2, 3),
        TopLevelInstruction::CloseAccount => (0, 2, 3),
        _ => return None,
    };
    (account_count == exact_accounts).then_some((source, authority))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemOwnerInstructionKind {
    CreateAccount,
    Assign,
    CreateAccountWithSeed,
    AllocateWithSeed,
    AssignWithSeed,
    CreateAccountAllowPrefund,
}

impl SystemOwnerInstructionKind {
    const fn name(self) -> &'static str {
        match self {
            Self::CreateAccount => "create_account",
            Self::Assign => "assign",
            Self::CreateAccountWithSeed => "create_account_with_seed",
            Self::AllocateWithSeed => "allocate_with_seed",
            Self::AssignWithSeed => "assign_with_seed",
            Self::CreateAccountAllowPrefund => "create_account_allow_prefund",
        }
    }

    const fn event_kind(self) -> &'static str {
        match self {
            Self::CreateAccount | Self::CreateAccountWithSeed | Self::CreateAccountAllowPrefund => {
                "account_creation"
            }
            Self::Assign | Self::AllocateWithSeed | Self::AssignWithSeed => "owner_assignment",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SystemRuntimeOwnerAssignment {
    account_position: usize,
    program_id: [u8; 32],
    instruction: SystemOwnerInstructionKind,
    create_with_seed_base: Option<[u8; 32]>,
}

fn system_instruction_tag(data: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(data.get(..4)?.try_into().ok()?))
}

fn is_system_runtime_owner_assignment(data: &[u8]) -> bool {
    matches!(system_instruction_tag(data), Some(0 | 1 | 3 | 9 | 10 | 13))
}

fn system_seed_end(data: &[u8]) -> Option<usize> {
    let seed_length =
        usize::try_from(u64::from_le_bytes(data.get(36..44)?.try_into().ok()?)).ok()?;
    if seed_length > 32 {
        return None;
    }
    let seed_end = 44usize.checked_add(seed_length)?;
    std::str::from_utf8(data.get(44..seed_end)?).ok()?;
    Some(seed_end)
}

/// Decode optional runtime-owner evidence without adding a replay failure path.
///
/// The System Program accepts extra account metas and trailing instruction
/// bytes. We therefore require only the runtime's minimum positional account
/// layout and a complete owner-field prefix. Truncated, unsupported, or
/// otherwise malformed candidates return `None` and do not affect replay.
fn decode_system_runtime_owner_assignment(
    data: &[u8],
    account_count: usize,
) -> Option<SystemRuntimeOwnerAssignment> {
    let tag = system_instruction_tag(data)?;
    let (account_position, program_id, instruction, create_with_seed_base) = match tag {
        // SystemInstruction::CreateAccount { lamports, space, owner }
        0 => {
            if data.len() < 4 + 8 + 8 + 32 || account_count < 2 {
                return None;
            }
            (
                1,
                data.get(20..52)?.try_into().ok()?,
                SystemOwnerInstructionKind::CreateAccount,
                None,
            )
        }
        // SystemInstruction::Assign { owner }
        1 => {
            if data.len() < 4 + 32 || account_count < 1 {
                return None;
            }
            (
                0,
                data.get(4..36)?.try_into().ok()?,
                SystemOwnerInstructionKind::Assign,
                None,
            )
        }
        // SystemInstruction::CreateAccountWithSeed {
        //     base, seed, lamports, space, owner
        // }
        3 => {
            if data.len() < 4 + 32 + 8 + 8 + 8 + 32 || account_count < 2 {
                return None;
            }
            let owner_start = system_seed_end(data)?.checked_add(16)?;
            let owner_end = owner_start.checked_add(32)?;
            (
                1,
                data.get(owner_start..owner_end)?.try_into().ok()?,
                SystemOwnerInstructionKind::CreateAccountWithSeed,
                Some(data.get(4..36)?.try_into().ok()?),
            )
        }
        // SystemInstruction::AllocateWithSeed { base, seed, space, owner }
        9 => {
            if data.len() < 4 + 32 + 8 + 8 + 32 || account_count < 1 {
                return None;
            }
            let owner_start = system_seed_end(data)?.checked_add(8)?;
            let owner_end = owner_start.checked_add(32)?;
            (
                0,
                data.get(owner_start..owner_end)?.try_into().ok()?,
                SystemOwnerInstructionKind::AllocateWithSeed,
                Some(data.get(4..36)?.try_into().ok()?),
            )
        }
        // SystemInstruction::AssignWithSeed { base, seed, owner }
        10 => {
            if data.len() < 4 + 32 + 8 + 32 || account_count < 1 {
                return None;
            }
            let owner_start = system_seed_end(data)?;
            let owner_end = owner_start.checked_add(32)?;
            (
                0,
                data.get(owner_start..owner_end)?.try_into().ok()?,
                SystemOwnerInstructionKind::AssignWithSeed,
                Some(data.get(4..36)?.try_into().ok()?),
            )
        }
        // SystemInstruction::CreateAccountAllowPrefund { lamports, space, owner }
        13 => {
            if data.len() < 4 + 8 + 8 + 32 {
                return None;
            }
            let lamports = u64::from_le_bytes(data.get(4..12)?.try_into().ok()?);
            let required_accounts = if lamports == 0 { 1 } else { 2 };
            if account_count < required_accounts {
                return None;
            }
            (
                0,
                data.get(20..52)?.try_into().ok()?,
                SystemOwnerInstructionKind::CreateAccountAllowPrefund,
                None,
            )
        }
        _ => return None,
    };
    Some(SystemRuntimeOwnerAssignment {
        account_position,
        program_id,
        instruction,
        create_with_seed_base,
    })
}

/// Return the outer program that directly invoked a depth-2 Token instruction.
///
/// This deliberately returns no program for deeper CPI. Signer privilege can
/// pass through intermediate programs, while compiled inner account lists and
/// invocation logs do not record which ancestor first added that privilege.
fn direct_depth_2_derivation_program_position(
    instructions: &[OrderedInvocation],
    position: usize,
) -> Option<usize> {
    let instruction = instructions.get(position)?;
    if instruction.coordinate.is_outer() || instruction.stack_height != Some(2) {
        return None;
    }
    instructions[..position].iter().rposition(|candidate| {
        candidate.coordinate.is_outer()
            && candidate.coordinate.outer_index == instruction.coordinate.outer_index
            && candidate.stack_height == Some(1)
    })
}

fn outer_instruction_signer_candidates(
    outer: &[StagedOuterInstruction],
    outer_index: u32,
    message_bytes: &[u8],
    resolved_accounts: &[u32],
    transaction_signer_registry_ids: &[u32],
    registry: &[u8],
) -> Result<Vec<u32>> {
    let instruction = outer
        .get(usize::try_from(outer_index)?)
        .context("creation provenance outer instruction is missing")?;
    let account_indices = instruction
        .accounts
        .get(message_bytes, "creation provenance outer accounts")?;
    let mut candidates = Vec::new();
    for &message_index in account_indices {
        let registry_id = *resolved_accounts
            .get(usize::from(message_index))
            .context("creation provenance account index exceeds resolved accounts")?;
        if transaction_signer_registry_ids.contains(&registry_id)
            && !candidates.contains(&registry_id)
        {
            candidates.push(registry_id);
        }
    }
    candidates.sort_unstable_by(|left, right| {
        raw_registry_key(registry, *left)
            .expect("validated creation signer registry ID")
            .cmp(
                &raw_registry_key(registry, *right).expect("validated creation signer registry ID"),
            )
    });
    Ok(candidates)
}

#[allow(clippy::too_many_arguments)]
fn observe_direct_depth_2_owner_authority(
    stage: &mut HolderAuthorityStage,
    program: TokenProgram,
    data: &[u8],
    account_indices: &[u8],
    resolved_accounts: &[u32],
    target_account_by_registry_id: &[u32],
    reducer: &TargetBalanceReducer,
    registry: &[u8],
    parent_program_registry_id: u32,
) -> Result<()> {
    let Some((source_position, authority_position)) =
        token_owner_authority_layout(program, data, account_indices.len())
    else {
        return Ok(());
    };
    let source_message_index = usize::from(account_indices[source_position]);
    let authority_message_index = usize::from(account_indices[authority_position]);
    let source_registry_id = *resolved_accounts
        .get(source_message_index)
        .context("token owner source index exceeds resolved accounts")?;
    let authority_registry_id = *resolved_accounts
        .get(authority_message_index)
        .context("token owner authority index exceeds resolved accounts")?;
    let Some(target_index) = target_account_by_registry_id
        .get(usize::try_from(source_registry_id)?)
        .copied()
        .filter(|index| *index != u32::MAX)
    else {
        return Ok(());
    };
    let AccountLifecycle::Open { owner, .. } = reducer
        .account(target_index)
        .context("token owner source target index is outside replay state")?
        .lifecycle
    else {
        return Ok(());
    };
    let authority = raw_registry_key(registry, authority_registry_id)
        .context("token owner authority registry ID cannot be resolved")?;
    if authority == owner {
        stage.observe_direct_cpi_authority(authority_registry_id, parent_program_registry_id)?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
struct LogNormalization {
    ambiguous_custom_failure: bool,
    truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StagedLogBoundary {
    Invoke {
        program_id: [u8; 32],
        depth: Option<u32>,
    },
    Success {
        program_id: [u8; 32],
    },
    Failure {
        program_id: [u8; 32],
    },
    AmbiguousCustomFailure {
        program_id: [u8; 32],
    },
    Truncated,
}

fn collect_log_boundaries(
    output: &mut Vec<StagedLogBoundary>,
    metadata: &[u8],
    schema: ArchiveV2WireMetadataErrorSchema,
    message: &ProjectedArchiveV2MessageAccountSummary,
    registry_entries: u32,
    registry: &[u8],
) -> Result<(ProjectedArchiveV2CompactLogsSummary, LogNormalization)> {
    output.clear();
    let total_message_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|value| value.checked_add(message.expected_loaded_readonly))
        .context("log message account count overflow")?;
    let invalid_program = Cell::new(false);
    let normalization = Cell::new(LogNormalization::default());
    let summary = visit_archive_v2_compact_logs_exact_with_selected_error_schema(
        metadata,
        schema,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts,
            top_level_instruction_count: message.instruction_count,
        },
        registry_entries,
        |event, _| {
            let resolve = |reference| {
                compact_id(reference, registry_entries)
                    .and_then(|id| raw_registry_key(registry, id))
            };
            match event.kind {
                BorrowedArchiveV2LogEventKind::Invoke { program, depth } => {
                    let Some(program_id) = resolve(program) else {
                        invalid_program.set(true);
                        return Ok(());
                    };
                    output.push(StagedLogBoundary::Invoke {
                        program_id,
                        depth: Some(u32::from(depth)),
                    });
                }
                BorrowedArchiveV2LogEventKind::BpfInvoke { program } => {
                    let Some(program_id) = resolve(program) else {
                        invalid_program.set(true);
                        return Ok(());
                    };
                    output.push(StagedLogBoundary::Invoke {
                        program_id,
                        depth: None,
                    });
                }
                BorrowedArchiveV2LogEventKind::Success { program }
                | BorrowedArchiveV2LogEventKind::BpfSuccess { program } => {
                    let Some(program_id) = resolve(program) else {
                        invalid_program.set(true);
                        return Ok(());
                    };
                    output.push(StagedLogBoundary::Success { program_id });
                }
                BorrowedArchiveV2LogEventKind::Failure { program, .. }
                | BorrowedArchiveV2LogEventKind::BpfFailure { program, .. }
                | BorrowedArchiveV2LogEventKind::BpfFailureCustomProgramError { program, .. }
                | BorrowedArchiveV2LogEventKind::FailureInvalidAccountData { program }
                | BorrowedArchiveV2LogEventKind::BpfFailureInvalidAccountData { program }
                | BorrowedArchiveV2LogEventKind::FailureInvalidProgramArgument { program }
                | BorrowedArchiveV2LogEventKind::BpfFailureInvalidProgramArgument { program } => {
                    let Some(program_id) = resolve(program) else {
                        invalid_program.set(true);
                        return Ok(());
                    };
                    output.push(StagedLogBoundary::Failure { program_id });
                }
                BorrowedArchiveV2LogEventKind::FailureCustomProgramError { program, .. } => {
                    // Compact tag 22 is ambiguous: historical input can use it
                    // for either a terminal boundary or an explicit program
                    // log. Do not guess which meaning committed state.
                    let Some(program_id) = resolve(program) else {
                        invalid_program.set(true);
                        return Ok(());
                    };
                    output.push(StagedLogBoundary::AmbiguousCustomFailure { program_id });
                    let mut state = normalization.get();
                    state.ambiguous_custom_failure = true;
                    normalization.set(state);
                }
                BorrowedArchiveV2LogEventKind::LogTruncated => {
                    let mut state = normalization.get();
                    state.truncated = true;
                    normalization.set(state);
                    output.push(StagedLogBoundary::Truncated);
                }
                _ => {}
            }
            Ok(())
        },
    )?;
    ensure!(
        !invalid_program.get(),
        "compact logs contain an unresolved program ID"
    );
    Ok((summary, normalization.get()))
}

const MAX_LOG_INTERPRETATIONS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LogCandidate {
    events: Vec<InvocationLogEvent>,
    stack: Vec<[u8; 32]>,
    truncated: bool,
}

#[derive(Debug)]
enum LogInterpretation {
    Unique(blockzilla_token_balance_audit::commit::CommitClassification),
    NoValidTrace,
    Divergent,
    TooMany,
    StructuralDiagnostics(usize),
}

fn push_unique_log_candidate(candidates: &mut Vec<LogCandidate>, candidate: LogCandidate) -> bool {
    if !candidates.contains(&candidate) {
        candidates.push(candidate);
    }
    candidates.len() <= MAX_LOG_INTERPRETATIONS
}

fn classification_is_strictly_usable(
    classification: &blockzilla_token_balance_audit::commit::CommitClassification,
) -> bool {
    classification.diagnostics.is_empty()
        || (classification.all_known()
            && classification.diagnostics.iter().all(|diagnostic| {
                matches!(
                    diagnostic,
                    blockzilla_token_balance_audit::commit::TraceDiagnostic::LogTruncated { .. }
                )
            }))
}

fn classify_log_interpretations(
    transaction_succeeded: bool,
    instructions: &[OrderedInvocation],
    boundaries: &[StagedLogBoundary],
) -> LogInterpretation {
    let mut candidates = vec![LogCandidate {
        events: Vec::with_capacity(boundaries.len()),
        stack: Vec::with_capacity(8),
        truncated: false,
    }];
    for boundary in boundaries {
        let mut next = Vec::with_capacity(candidates.len().saturating_mul(2));
        for mut candidate in candidates {
            match *boundary {
                StagedLogBoundary::Invoke { program_id, depth } => {
                    let expected = match u32::try_from(candidate.stack.len())
                        .ok()
                        .and_then(|value| value.checked_add(1))
                    {
                        Some(value) => value,
                        None => continue,
                    };
                    if depth.is_some_and(|value| value != expected) {
                        continue;
                    }
                    candidate.events.push(InvocationLogEvent::Invoke {
                        program_id,
                        depth: expected,
                    });
                    candidate.stack.push(program_id);
                    if !push_unique_log_candidate(&mut next, candidate) {
                        return LogInterpretation::TooMany;
                    }
                }
                StagedLogBoundary::Success { program_id } => {
                    if candidate.stack.pop() != Some(program_id) {
                        continue;
                    }
                    candidate
                        .events
                        .push(InvocationLogEvent::Success { program_id });
                    if !push_unique_log_candidate(&mut next, candidate) {
                        return LogInterpretation::TooMany;
                    }
                }
                StagedLogBoundary::Failure { program_id } => {
                    let depth = candidate.stack.len();
                    if candidate.stack.pop() != Some(program_id) {
                        continue;
                    }
                    if transaction_succeeded && depth == 1 {
                        continue;
                    }
                    candidate
                        .events
                        .push(InvocationLogEvent::Failure { program_id });
                    if !push_unique_log_candidate(&mut next, candidate) {
                        return LogInterpretation::TooMany;
                    }
                }
                StagedLogBoundary::AmbiguousCustomFailure { program_id } => {
                    // Interpretation one is an explicit program log. It does
                    // not change the runtime invocation stack. A program can
                    // only emit it while it is the active invocation.
                    if candidate.stack.last().copied() != Some(program_id) {
                        continue;
                    }
                    if !push_unique_log_candidate(&mut next, candidate.clone()) {
                        return LogInterpretation::TooMany;
                    }
                    // Interpretation two is a terminal failure. It is valid
                    // only when it closes the active matching invocation.
                    let depth = candidate.stack.len();
                    if candidate.stack.pop() == Some(program_id)
                        && !(transaction_succeeded && depth == 1)
                    {
                        candidate
                            .events
                            .push(InvocationLogEvent::Failure { program_id });
                        if !push_unique_log_candidate(&mut next, candidate) {
                            return LogInterpretation::TooMany;
                        }
                    }
                }
                StagedLogBoundary::Truncated => {
                    candidate.truncated = true;
                    candidate.events.push(InvocationLogEvent::LogTruncated);
                    if !push_unique_log_candidate(&mut next, candidate) {
                        return LogInterpretation::TooMany;
                    }
                }
            }
        }
        if next.is_empty() {
            return LogInterpretation::NoValidTrace;
        }
        candidates = next;
    }
    candidates.retain(|candidate| candidate.truncated || candidate.stack.is_empty());
    if candidates.is_empty() {
        return LogInterpretation::NoValidTrace;
    }

    let mut first = None::<blockzilla_token_balance_audit::commit::CommitClassification>;
    let mut structural_diagnostics = None::<usize>;
    for candidate in candidates {
        let classification =
            classify_committed_invocations(transaction_succeeded, instructions, &candidate.events);
        if !classification_is_strictly_usable(&classification) {
            structural_diagnostics = Some(
                structural_diagnostics
                    .unwrap_or_default()
                    .max(classification.diagnostics.len()),
            );
        }
        if let Some(expected) = &first {
            if classification
                .invocations
                .iter()
                .map(|value| value.status)
                .ne(expected.invocations.iter().map(|value| value.status))
            {
                return LogInterpretation::Divergent;
            }
        } else {
            first = Some(classification);
        }
    }
    if let Some(count) = structural_diagnostics {
        return LogInterpretation::StructuralDiagnostics(count);
    }
    LogInterpretation::Unique(first.expect("candidate set is non-empty"))
}

fn classify_unambiguous_log_trace(
    transaction_succeeded: bool,
    instructions: &[OrderedInvocation],
    boundaries: &[StagedLogBoundary],
    events: &mut Vec<InvocationLogEvent>,
    stack: &mut Vec<[u8; 32]>,
) -> LogInterpretation {
    events.clear();
    stack.clear();
    let mut truncated = false;
    for boundary in boundaries {
        match *boundary {
            StagedLogBoundary::Invoke { program_id, depth } => {
                let Some(expected) = u32::try_from(stack.len())
                    .ok()
                    .and_then(|value| value.checked_add(1))
                else {
                    return LogInterpretation::NoValidTrace;
                };
                if depth.is_some_and(|value| value != expected) {
                    return LogInterpretation::NoValidTrace;
                }
                events.push(InvocationLogEvent::Invoke {
                    program_id,
                    depth: expected,
                });
                stack.push(program_id);
            }
            StagedLogBoundary::Success { program_id } => {
                if stack.pop() != Some(program_id) {
                    return LogInterpretation::NoValidTrace;
                }
                events.push(InvocationLogEvent::Success { program_id });
            }
            StagedLogBoundary::Failure { program_id } => {
                let depth = stack.len();
                if stack.pop() != Some(program_id) || (transaction_succeeded && depth == 1) {
                    return LogInterpretation::NoValidTrace;
                }
                events.push(InvocationLogEvent::Failure { program_id });
            }
            StagedLogBoundary::AmbiguousCustomFailure { .. } => {
                return classify_log_interpretations(
                    transaction_succeeded,
                    instructions,
                    boundaries,
                );
            }
            StagedLogBoundary::Truncated => {
                truncated = true;
                events.push(InvocationLogEvent::LogTruncated);
            }
        }
    }
    if !truncated && !stack.is_empty() {
        return LogInterpretation::NoValidTrace;
    }

    let classification =
        classify_committed_invocations(transaction_succeeded, instructions, events);
    if classification_is_strictly_usable(&classification) {
        LogInterpretation::Unique(classification)
    } else {
        LogInterpretation::StructuralDiagnostics(classification.diagnostics.len())
    }
}

fn parse_metadata_stage(
    stage: &mut MetadataStage,
    bytes: &[u8],
    schema: ArchiveV2WireMetadataErrorSchema,
    message: &ProjectedArchiveV2MessageAccountSummary,
    registry_entries: u32,
    target_mint_id: u32,
    flags: u32,
) -> Result<ProjectedArchiveV2TokenMetadataSummary> {
    stage.clear();
    let total_message_accounts = message
        .static_account_count
        .checked_add(message.expected_loaded_writable)
        .and_then(|value| value.checked_add(message.expected_loaded_readonly))
        .context("resolved message account count overflow")?;
    ensure!(
        total_message_accounts <= blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS,
        "resolved message account count exceeds format cap"
    );

    let callback_error = Cell::new(None::<&'static str>);
    let mut inner_ordinal_by_outer = [0u32; 256];
    let summary = visit_archive_v2_token_metadata_exact_ordered_with_selected_error_schema(
        bytes,
        schema,
        ArchiveV2MetadataProjectionLimits {
            total_message_accounts,
            top_level_instruction_count: message.instruction_count,
        },
        registry_entries,
        LogPayloadValidation::StructureOnly,
        |outer_index, instruction| {
            if callback_error.get().is_some() {
                return;
            }
            let result = (|| -> Result<()> {
                let outer = usize::try_from(outer_index)?;
                let inner_index = *inner_ordinal_by_outer
                    .get(outer)
                    .context("inner instruction outer index exceeds transaction limit")?;
                inner_ordinal_by_outer[outer] = inner_index
                    .checked_add(1)
                    .context("inner instruction ordinal overflow")?;
                stage.inner.push(StagedInnerInstruction {
                    outer_index,
                    inner_index,
                    program_id_index: instruction.program_id_index,
                    accounts: SliceRange::capture(bytes, instruction.accounts, "inner accounts")?,
                    data: SliceRange::capture(bytes, instruction.data, "inner data")?,
                    stack_height: instruction.stack_height,
                });
                Ok(())
            })();
            if result.is_err() {
                callback_error.set(Some("cannot stage inner instruction"));
            }
        },
        |side, balance: BorrowedArchiveV2TokenBalance| {
            if callback_error.get().is_some() {
                return;
            }
            let Some(mint_id) = balance
                .mint
                .and_then(|value| compact_id(value, registry_entries))
            else {
                if balance.mint.is_some() {
                    callback_error.set(Some("token balance has an unresolved mint"));
                }
                return;
            };
            if mint_id != target_mint_id {
                return;
            }
            if usize::try_from(balance.account_index)
                .ok()
                .is_none_or(|index| index >= total_message_accounts)
            {
                callback_error.set(Some("target balance account index is outside the message"));
                return;
            }
            let owner_id = match balance.owner {
                Some(value) => match compact_id(value, registry_entries) {
                    Some(id) => Some(id),
                    None => {
                        callback_error.set(Some("target balance has an unresolved owner"));
                        return;
                    }
                },
                None => None,
            };
            let program_id = match balance.program_id {
                Some(value) => match compact_id(value, registry_entries) {
                    Some(id) => Some(id),
                    None => {
                        callback_error.set(Some("target balance has an unresolved program"));
                        return;
                    }
                },
                None => None,
            };
            let row = OracleRow {
                account_index: balance.account_index,
                amount: balance.amount,
                owner_id,
                program_id,
                decimals: balance.decimals,
            };
            match side {
                TokenBalanceSide::Pre => stage.pre.push(row),
                TokenBalanceSide::Post => stage.post.push(row),
            }
        },
        |side, ordinal, reference| {
            if callback_error.get().is_some() {
                return;
            }
            let absolute = match side {
                ArchiveV2LoadedAddressSide::Writable => {
                    message.static_account_count.checked_add(ordinal)
                }
                ArchiveV2LoadedAddressSide::Readonly => message
                    .static_account_count
                    .checked_add(message.expected_loaded_writable)
                    .and_then(|value| value.checked_add(ordinal)),
            };
            let Some(absolute) = absolute.filter(|value| *value < stage.loaded_ids.len()) else {
                callback_error.set(Some("loaded address ordinal exceeds message limit"));
                return;
            };
            let Some(id) = compact_id(reference, registry_entries) else {
                callback_error.set(Some("metadata has an unresolved loaded address"));
                return;
            };
            stage.loaded_ids[absolute] = id;
        },
    )?;
    ensure!(
        callback_error.get().is_none(),
        "{}",
        callback_error.get().unwrap_or("metadata callback failed")
    );
    ensure!(
        stage.inner.len() == summary.inner_instruction_count,
        "inner instruction callback count differs from metadata summary"
    );
    validate_inventory_metadata_summary(&summary, message, flags)?;
    stage.pre.sort_unstable_by_key(|row| row.account_index);
    stage.post.sort_unstable_by_key(|row| row.account_index);
    ensure!(
        stage
            .pre
            .windows(2)
            .all(|pair| pair[0].account_index < pair[1].account_index)
            && stage
                .post
                .windows(2)
                .all(|pair| pair[0].account_index < pair[1].account_index),
        "target token balance rows contain a duplicate account index"
    );
    stage.summary = Some(summary);
    Ok(summary)
}

fn select_metadata_stage<'a>(
    current: &'a mut MetadataStage,
    legacy: &'a mut MetadataStage,
    record: &BorrowedTransactionRecord<'_>,
    message: &ProjectedArchiveV2MessageAccountSummary,
    registry_entries: u32,
    target_mint_id: u32,
    counters: &mut ReplayCounters,
) -> Result<(Option<ArchiveV2WireMetadataErrorSchema>, &'a MetadataStage)> {
    if record.metadata_bytes.is_empty() {
        validate_inventory_absent_metadata(message, record.flags)?;
        current.clear();
        checked_add(&mut counters.metadata_absent, 1, "metadata absent overflow")?;
        return Ok((None, current));
    }
    let selection =
        select_exact_metadata_schema(record.metadata_bytes, current, legacy, |stage, schema| {
            parse_metadata_stage(
                stage,
                record.metadata_bytes,
                schema,
                message,
                registry_entries,
                target_mint_id,
                record.flags,
            )?;
            Ok(())
        })
        .with_context(|| {
            format!(
                "select exact metadata schema at epoch {} slot {} transaction {}",
                record.source_epoch, record.block.slot, record.tx_index
            )
        })?;
    match selection {
        ExactMetadataSchemaSelection::NoMetadata => {
            unreachable!("non-empty metadata selected as absent")
        }
        ExactMetadataSchemaSelection::NoError => {
            checked_add(
                &mut counters.metadata_without_error,
                1,
                "metadata success overflow",
            )?;
        }
        ExactMetadataSchemaSelection::CurrentOnly => {
            checked_add(
                &mut counters.metadata_current_only,
                1,
                "current-only metadata overflow",
            )?;
        }
        ExactMetadataSchemaSelection::LegacyOnly => {
            checked_add(
                &mut counters.metadata_legacy_only,
                1,
                "legacy-only metadata overflow",
            )?;
        }
        ExactMetadataSchemaSelection::BothIdentical => {
            checked_add(
                &mut counters.metadata_both_identical,
                1,
                "dual-valid metadata overflow",
            )?;
        }
    }
    let selected = match selection {
        ExactMetadataSchemaSelection::LegacyOnly => legacy,
        ExactMetadataSchemaSelection::NoError
        | ExactMetadataSchemaSelection::CurrentOnly
        | ExactMetadataSchemaSelection::BothIdentical => current,
        ExactMetadataSchemaSelection::NoMetadata => {
            unreachable!("non-empty metadata selected as absent")
        }
    };
    Ok((selection.selected_schema(), selected))
}

fn account_slice_is_target(
    accounts: &[u8],
    resolved_accounts: &[u32],
    target_mint_id: u32,
    target_account_by_registry_id: &[u32],
) -> Result<bool> {
    let mut is_target = false;
    for &account_index in accounts {
        let id = *resolved_accounts
            .get(usize::from(account_index))
            .context("instruction account index is outside resolved message accounts")?;
        let is_target_account = target_account_by_registry_id
            .get(usize::try_from(id)?)
            .is_some_and(|ordinal| *ordinal != u32::MAX);
        if id == target_mint_id || is_target_account {
            is_target = true;
        }
    }
    Ok(is_target)
}

fn record_decoded(
    counters: &mut ReplayCounters,
    instruction_names: &mut BTreeMap<InstructionNameKey, u64>,
    decoded: blockzilla_token_balance_audit::instruction::DecodedInstruction<'_>,
) -> Result<()> {
    let value = instruction_names
        .entry(InstructionNameKey::from_decoded(decoded))
        .or_default();
    checked_add(value, 1, "instruction name count overflow")?;
    match decoded.status {
        DecodeStatus::Known => checked_add(
            &mut counters.known_decoded_target_token_invocations,
            1,
            "known decode count overflow",
        )?,
        DecodeStatus::Malformed => checked_add(
            &mut counters.malformed_target_token_invocations,
            1,
            "malformed decode count overflow",
        )?,
        DecodeStatus::UnknownTopLevel | DecodeStatus::BatchContainsUnknown => checked_add(
            &mut counters.unknown_top_level_target_token_invocations,
            1,
            "unknown top-level count overflow",
        )?,
        DecodeStatus::UnknownExtensionSubtype => checked_add(
            &mut counters.unknown_extension_target_token_invocations,
            1,
            "unknown extension count overflow",
        )?,
    }
    match decoded.effect {
        InstructionEffect::BalanceRelevant => checked_add(
            &mut counters.balance_relevant_target_token_invocations,
            1,
            "balance-relevant count overflow",
        )?,
        InstructionEffect::StateRelevant => checked_add(
            &mut counters.state_relevant_target_token_invocations,
            1,
            "state-relevant count overflow",
        )?,
        InstructionEffect::NoPublicBalanceEffect => checked_add(
            &mut counters.no_public_balance_effect_target_token_invocations,
            1,
            "no-effect count overflow",
        )?,
    }
    Ok(())
}

fn program_kind(
    program_id: [u8; 32],
    legacy: [u8; 32],
    token_2022: [u8; 32],
) -> Option<TokenProgram> {
    if program_id == legacy {
        Some(TokenProgram::Legacy)
    } else if program_id == token_2022 {
        Some(TokenProgram::Token2022)
    } else {
        None
    }
}

#[derive(Debug)]
struct OracleMismatch {
    code: &'static str,
    detail: String,
}

fn target_message_index(
    target_index: u32,
    resolved_accounts: &[u32],
    target_account_by_registry_id: &[u32],
) -> Result<usize> {
    resolved_accounts
        .iter()
        .enumerate()
        .find_map(|(message_index, &registry_id)| {
            target_account_by_registry_id
                .get(usize::try_from(registry_id).ok()?)
                .is_some_and(|index| *index == target_index)
                .then_some(message_index)
        })
        .context("changed target account is absent from resolved message accounts")
}

fn require_changed_pre_oracle_rows(
    changes: &[TargetAccountChange],
    row_by_message_account: &[Option<OracleRow>; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    resolved_accounts: &[u32],
    target_account_by_registry_id: &[u32],
) -> Result<Result<(), OracleMismatch>> {
    for change in changes {
        if !matches!(change.previous.lifecycle, AccountLifecycle::Open { .. }) {
            continue;
        }
        let message_index = target_message_index(
            change.index,
            resolved_accounts,
            target_account_by_registry_id,
        )?;
        if row_by_message_account[message_index].is_none() {
            return Ok(Err(OracleMismatch {
                code: "oracle_pre_row_missing_for_changed_open_account",
                detail: format!(
                    "pre metadata omits changed open SPYx account {} at message index {message_index}",
                    bs58::encode(change.previous.address).into_string()
                ),
            }));
        }
    }
    Ok(Ok(()))
}

#[allow(clippy::too_many_arguments)]
fn compare_oracle_side(
    side: &'static str,
    rows: &[OracleRow],
    required_account_changes: &[TargetAccountChange],
    row_by_message_account: &mut [Option<OracleRow>; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS],
    resolved_accounts: &[u32],
    target_account_by_registry_id: &[u32],
    reducer: &TargetBalanceReducer,
    registry: &[u8],
    token_2022_program: [u8; 32],
    decimals: u8,
) -> Result<Result<u64, OracleMismatch>> {
    row_by_message_account.fill(None);
    for &row in rows {
        let index = usize::try_from(row.account_index)?;
        let slot = row_by_message_account
            .get_mut(index)
            .context("oracle row account index exceeds message account limit")?;
        ensure!(
            slot.is_none(),
            "oracle rows repeat one message account index"
        );
        *slot = Some(row);
    }

    for &row in rows {
        let message_index = usize::try_from(row.account_index)?;
        let registry_id = *resolved_accounts
            .get(message_index)
            .context("oracle row account index exceeds resolved message accounts")?;
        let target_index = target_account_by_registry_id
            .get(usize::try_from(registry_id)?)
            .copied()
            .filter(|index| *index != u32::MAX)
            .context("target oracle row resolves outside the frozen target account list")?;
        let state = reducer
            .account(target_index)
            .context("target reducer index is outside its account table")?;
        match state.lifecycle {
            AccountLifecycle::Closed => {
                return Ok(Err(OracleMismatch {
                    code: "oracle_row_for_closed_account",
                    detail: format!(
                        "{side} metadata has a SPYx row for closed target account {}",
                        bs58::encode(state.address).into_string()
                    ),
                }));
            }
            AccountLifecycle::Open { owner, amount } => {
                if row.amount != amount {
                    return Ok(Err(OracleMismatch {
                        code: "oracle_amount_mismatch",
                        detail: format!(
                            "{side} amount for {} is {}; replay expected {amount}",
                            bs58::encode(state.address).into_string(),
                            row.amount
                        ),
                    }));
                }
                if row.decimals != decimals {
                    return Ok(Err(OracleMismatch {
                        code: "oracle_decimals_mismatch",
                        detail: format!(
                            "{side} decimals for {} are {}; replay expected {decimals}",
                            bs58::encode(state.address).into_string(),
                            row.decimals
                        ),
                    }));
                }
                if let Some(owner_id) = row.owner_id {
                    let actual = raw_registry_key(registry, owner_id)
                        .context("oracle owner ID cannot be resolved")?;
                    if actual != owner {
                        return Ok(Err(OracleMismatch {
                            code: "oracle_owner_mismatch",
                            detail: format!(
                                "{side} owner for {} differs from instruction replay",
                                bs58::encode(state.address).into_string()
                            ),
                        }));
                    }
                }
                if let Some(program_id) = row.program_id {
                    let actual = raw_registry_key(registry, program_id)
                        .context("oracle program ID cannot be resolved")?;
                    if actual != token_2022_program {
                        return Ok(Err(OracleMismatch {
                            code: "oracle_program_mismatch",
                            detail: format!(
                                "{side} program for {} is not Token-2022",
                                bs58::encode(state.address).into_string()
                            ),
                        }));
                    }
                }
            }
        }
    }

    // Metadata can omit an unchanged token account that is only mentioned in
    // the message. A modeled state mutation is different: an open post-state
    // must have an oracle row, while a closed post-state must not have one.
    for change in required_account_changes {
        let required_target_index = change.index;
        let state = reducer
            .account(required_target_index)
            .context("required target reducer index is outside its account table")?;
        let message_index = target_message_index(
            required_target_index,
            resolved_accounts,
            target_account_by_registry_id,
        )?;
        if matches!(state.lifecycle, AccountLifecycle::Open { .. })
            && row_by_message_account[message_index].is_none()
        {
            return Ok(Err(OracleMismatch {
                code: "oracle_row_missing_for_mutated_open_account",
                detail: format!(
                    "{side} metadata omits mutated open SPYx account {} at message index {message_index}",
                    bs58::encode(state.address).into_string()
                ),
            }));
        }
    }
    Ok(Ok(u64::try_from(rows.len())?))
}

/// Build a direct census and replay report from a completed schema-3 dump.
pub(super) fn replay_consolidated_spyx_balances_v3(
    dump: &Path,
    report: &Path,
    max_transactions: Option<u64>,
) -> Result<()> {
    scan_consolidated_spyx_replay(dump, Some(report), max_transactions, None, None)?;
    Ok(())
}

pub(super) fn visit_consolidated_spyx_owner_postings_v3<F>(
    dump: &Path,
    max_transactions: Option<u64>,
    mut visit: F,
) -> Result<crate::consolidate::SpyxOwnerReplaySummary>
where
    F: FnMut(u64, &[u32]) -> Result<()>,
{
    scan_consolidated_spyx_replay(dump, None, max_transactions, Some(&mut visit), None)
}

pub(super) fn visit_consolidated_spyx_owner_balance_history_v3<F>(
    dump: &Path,
    max_transactions: Option<u64>,
    mut visit: F,
) -> Result<crate::consolidate::SpyxOwnerReplaySummary>
where
    F: for<'a> FnMut(crate::consolidate::SpyxOwnerBalanceTransaction<'a>) -> Result<()>,
{
    scan_consolidated_spyx_replay(dump, None, max_transactions, None, Some(&mut visit))
}

fn scan_consolidated_spyx_replay(
    dump: &Path,
    report: Option<&Path>,
    max_transactions: Option<u64>,
    mut visit_owners: Option<&mut OwnerPostingVisitor<'_>>,
    mut visit_owner_balance_history: Option<&mut OwnerBalanceHistoryVisitor<'_>>,
) -> Result<crate::consolidate::SpyxOwnerReplaySummary> {
    ensure!(
        max_transactions != Some(0),
        "--max-transactions must be positive"
    );
    let started = Instant::now();
    let dump = fs::canonicalize(dump)
        .with_context(|| format!("resolve consolidated dump {}", dump.display()))?;
    ensure!(dump.is_dir(), "consolidated dump is not a directory");
    validate_exact_final_files(&dump)?;

    if let Some(report) = report {
        let report_parent = report.parent().unwrap_or_else(|| Path::new("."));
        let report_parent = if report_parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            report_parent
        };
        let canonical_report_parent = fs::canonicalize(report_parent).with_context(|| {
            format!(
                "resolve replay report directory {}",
                report_parent.display()
            )
        })?;
        ensure!(
            !canonical_report_parent.starts_with(&dump),
            "replay report must not modify the immutable dump directory"
        );
        let report_name = report
            .file_name()
            .context("replay report path has no file name")?;
        ensure!(
            !canonical_report_parent.join(report_name).exists(),
            "refusing to replace an existing replay report"
        );
    }

    let manifest_bytes =
        read_bounded_regular(&dump.join(DUMP_MANIFEST_FILE), MAX_ROOT_MANIFEST_BYTES)?;
    let manifest_sha256 = sha256_bytes(&manifest_bytes);
    let manifest: DumpManifest = serde_json::from_slice(&manifest_bytes)?;
    ensure!(
        manifest.schema_version == DUMP_SCHEMA_VERSION
            && manifest.artifact_kind == DumpArtifactKind::Consolidated
            && manifest.complete
            && manifest.first_epoch <= manifest.last_epoch
            && manifest.transactions != 0,
        "invalid consolidated manifest header"
    );
    ensure!(
        manifest.transaction_stream == TRANSACTIONS_FILE
            && manifest.pubkey_registry.as_deref() == Some(PUBKEY_REGISTRY_FILE)
            && manifest.discovered_accounts.as_deref() == Some(ACCOUNTS_FILE),
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
    let expected_registry_sha256 = parse_hex_digest(
        manifest
            .pubkey_registry_sha256
            .as_deref()
            .context("missing registry digest")?,
        "registry digest",
    )?;
    let expected_accounts_sha256 = parse_hex_digest(
        manifest
            .discovered_accounts_sha256
            .as_deref()
            .context("missing account digest")?,
        "account digest",
    )?;
    let registry_rows = manifest.pubkeys.context("missing public-key count")?;
    let registry_entries = u32::try_from(registry_rows)?;
    ensure!(registry_entries != 0, "consolidated registry is empty");

    let registry_bytes = registry_rows
        .checked_mul(KEY_BYTES as u64)
        .context("registry byte length overflow")?;
    let registry = read_bounded_regular(&dump.join(PUBKEY_REGISTRY_FILE), registry_bytes)?;
    ensure!(
        u64::try_from(registry.len())? == registry_bytes
            && sha256_bytes(&registry) == expected_registry_sha256
            && registry
                .chunks_exact(KEY_BYTES)
                .zip(registry.chunks_exact(KEY_BYTES).skip(1))
                .all(|(left, right)| left < right),
        "registry differs from its manifest or is not sorted and unique"
    );
    let mint = parse_pubkey(&manifest.mint, "mint")?;
    let target_mint_id = registry_id_for_key(&registry, &mint)
        .context("target mint is absent from the consolidated registry")?;
    let system_program = parse_pubkey(SYSTEM_PROGRAM, "System Program")?;
    let legacy_program = parse_pubkey(LEGACY_TOKEN_PROGRAM, "legacy token program")?;
    let token_2022_program = parse_pubkey(TOKEN_2022_PROGRAM, "Token-2022 program")?;
    let history_owner_on_curve = if report.is_some() {
        let mut values = Vec::with_capacity(usize::try_from(registry_entries)? + 1);
        values.push(false);
        values.extend(registry.chunks_exact(KEY_BYTES).map(|bytes| {
            SolanaPubkey::new_from_array(
                bytes
                    .try_into()
                    .expect("registry validation guarantees 32-byte keys"),
            )
            .is_on_curve()
        }));
        values
    } else {
        Vec::new()
    };

    let account_bytes = read_bounded_regular(
        &dump.join(ACCOUNTS_FILE),
        ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES as u64,
    )?;
    ensure!(
        sha256_bytes(&account_bytes) == expected_accounts_sha256,
        "account artifact digest differs from its manifest"
    );
    let accounts: DiscoveredAccountList = wincode::config::deserialize_exact(
        &account_bytes,
        bounded_wincode_leb128_config::<ARCHIVE_V2_DECODE_PREALLOCATION_LIMIT_BYTES>(),
    )?;
    ensure!(
        accounts.schema_version == DUMP_SCHEMA_VERSION
            && accounts.mint == mint
            && accounts.anchor_position.slot == manifest.mint_slot
            && accounts
                .accounts
                .windows(2)
                .all(|pair| pair[0].raw_pubkey < pair[1].raw_pubkey)
            && manifest.discovered_account_count == Some(accounts.accounts.len() as u64),
        "frozen account artifact is invalid"
    );
    let mut target_account_by_registry_id = vec![u32::MAX; usize::try_from(registry_entries)? + 1];
    let mut target_account_keys = Vec::with_capacity(accounts.accounts.len());
    for (ordinal, account) in accounts.accounts.iter().enumerate() {
        let id = registry_id_for_key(&registry, &account.raw_pubkey)
            .context("discovered token account is absent from registry")?;
        let dense = &mut target_account_by_registry_id[usize::try_from(id)?];
        ensure!(*dense == u32::MAX, "duplicate discovered token account");
        *dense = u32::try_from(ordinal)?;
        target_account_keys.push(account.raw_pubkey);
    }
    let mut reducer = TargetBalanceReducer::new(
        TargetMintConfig {
            mint,
            program: TokenProgram::Token2022,
            decimals: SPYX_DECIMALS,
            native: false,
            initialized: false,
            transfer_fee_knowledge: TransferFeeKnowledge::Unknown,
        },
        target_account_keys,
    )?;

    let transaction_path = dump.join(TRANSACTIONS_FILE);
    let transaction_file = File::open(&transaction_path)?;
    let transaction_stamp = FileStamp::read(&transaction_file)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, transaction_file);
    let mut hasher = Sha256::new();
    let mut logical_offset = 0u64;
    let mut payload = Vec::new();
    read_frame_hashed(&mut reader, &mut logical_offset, &mut hasher, &mut payload)?
        .context("consolidated transaction stream is empty")?;
    let BorrowedDumpRecord::Header(header) = decode_borrowed_frame(&payload)? else {
        bail!("consolidated transaction stream does not start with a header")
    };
    ensure!(
        header.schema_version == DUMP_SCHEMA_VERSION
            && header.stream_kind == DumpStreamKind::Consolidated
            && header.mint == mint
            && header.mint_slot == manifest.mint_slot
            && header.pubkey_registry_id_base == PUBKEY_REGISTRY_ID_BASE,
        "consolidated stream header differs from its manifest"
    );

    let mut counters = ReplayCounters::default();
    let mut instruction_name_counts = BTreeMap::new();
    let mut census_findings = BTreeMap::new();
    let mut blockers = BTreeMap::new();
    let mut outer = Vec::<StagedOuterInstruction>::with_capacity(16);
    let mut ordered = Vec::<OrderedInvocation>::with_capacity(48);
    let mut ordered_registry_ids = Vec::<u32>::with_capacity(48);
    let mut invocation_sources = Vec::<(bool, usize)>::with_capacity(48);
    let mut invocation_commit_statuses = Vec::<CommitStatus>::with_capacity(48);
    let mut log_boundaries = Vec::<StagedLogBoundary>::with_capacity(96);
    let mut log_events = Vec::<InvocationLogEvent>::with_capacity(96);
    let mut log_stack = Vec::<[u8; 32]>::with_capacity(8);
    let mut reducer_account_storage = Vec::<ResolvedInstructionAccount>::with_capacity(192);
    let mut staged_reducer_instructions = Vec::<StagedReducerInstruction>::with_capacity(32);
    let mut pre_rows_by_message = [None; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
    let mut post_rows_by_message = [None; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
    let mut replay_active = true;
    let mut first_failure = None;
    let mut current = MetadataStage::new();
    let mut legacy = MetadataStage::new();
    let mut static_ids = [0u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
    let mut resolved_accounts = [0u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
    let mut dex_account_scratch = [0u32; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
    let mut mentioned_target_indices = Vec::<u32>::new();
    let mut owner_registry_ids = Vec::<u32>::new();
    let mut owner_activity_deltas =
        Vec::<(u32, i128)>::with_capacity(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS * 2);
    let owner_projection_requested =
        visit_owners.is_some() || visit_owner_balance_history.is_some();
    let owner_balance_state_requested = report.is_some() || visit_owner_balance_history.is_some();
    if owner_projection_requested {
        mentioned_target_indices
            .try_reserve_exact(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS)
            .context("reserve owner projection target scratch")?;
        owner_registry_ids
            .try_reserve_exact(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS * 2)
            .context("reserve owner projection ID scratch")?;
    }
    let mut owner_registry_cache = if owner_projection_requested || report.is_some() {
        HashMap::<[u8; 32], u32>::with_capacity(accounts.accounts.len())
    } else {
        HashMap::new()
    };
    let mut owner_post_balances = Vec::<u128>::new();
    let mut owner_balance_changes = Vec::<crate::consolidate::SpyxOwnerBalanceChange>::new();
    let mut positive_owner_registry_ids = Vec::<u32>::new();
    let mut positive_owner_positions = Vec::<u32>::new();
    if owner_balance_state_requested {
        let balance_slots = usize::try_from(registry_entries)
            .context("owner balance table length exceeds usize")?
            .checked_add(1)
            .context("owner balance table length overflow")?;
        owner_post_balances
            .try_reserve_exact(balance_slots)
            .context("reserve bounded owner balance table")?;
        owner_post_balances.resize(balance_slots, 0);
        if report.is_some() {
            positive_owner_registry_ids
                .try_reserve_exact(accounts.accounts.len())
                .context("reserve positive history owner IDs")?;
            positive_owner_positions
                .try_reserve_exact(balance_slots)
                .context("reserve positive history owner positions")?;
            positive_owner_positions.resize(balance_slots, u32::MAX);
        }
    }
    if visit_owner_balance_history.is_some() {
        owner_balance_changes
            .try_reserve_exact(blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS * 2)
            .context("reserve owner balance-change scratch")?;
    }
    let dex_dispatch = report
        .is_some()
        .then(|| build_dex_dispatch(&registry, registry_entries))
        .transpose()?;
    let mut holder_authority_stage = report
        .is_some()
        .then(|| HolderAuthorityStage::new(registry_entries))
        .transpose()?;
    let mut authority_portfolio_history = report
        .is_some()
        .then(AuthorityPortfolioHistoryCollector::default);
    let mut last_history_location = None::<HistoryLocation>;
    let mut previous_coordinate = None;

    let scan_end = loop {
        if max_transactions.is_some_and(|limit| counters.transactions_scanned >= limit) {
            break ScanEnd::Prefix;
        }
        read_frame_hashed(&mut reader, &mut logical_offset, &mut hasher, &mut payload)?
            .context("consolidated transaction stream has no footer")?;
        match decode_borrowed_frame(&payload)? {
            BorrowedDumpRecord::Header(_) => bail!("consolidated stream repeats its header"),
            BorrowedDumpRecord::Footer(footer) => break ScanEnd::Footer(footer),
            BorrowedDumpRecord::Transaction(record) => {
                let coordinate = ProgramInventoryCoordinate::from_record(&record).canonical_key();
                ensure!(
                    previous_coordinate.is_none_or(|previous| previous < coordinate),
                    "consolidated transactions are not in canonical order"
                );
                previous_coordinate = Some(coordinate);
                ensure!(
                    (manifest.first_epoch..=manifest.last_epoch).contains(&record.source_epoch)
                        && record.block.slot >= manifest.mint_slot
                        && record.block.parent_slot < record.block.slot
                        && record.tx_index < record.block.transaction_count
                        && record.flags & !ARCHIVE_V2_TX_KNOWN_FLAGS == 0
                        && record.flags
                            & (ARCHIVE_V2_TX_FLAG_TX_RAW_FALLBACK
                                | ARCHIVE_V2_TX_FLAG_METADATA_RAW_FALLBACK)
                            == 0,
                    "consolidated transaction has invalid source fields"
                );
                ensure!(
                    (record.flags & ARCHIVE_V2_TX_FLAG_HAS_METADATA != 0)
                        == !record.metadata_bytes.is_empty()
                        && (record.flags & ARCHIVE_V2_TX_FLAG_HAS_ERROR != 0)
                            == (record.metadata_bytes.first() == Some(&1)),
                    "consolidated transaction flags differ from metadata bytes"
                );

                if replay_active
                    && let Some(last_location) = last_history_location
                    && last_location.slot_window() != record.block.slot / HISTORY_SLOT_WINDOW_WIDTH
                    && let (Some(history), Some(stage)) = (
                        authority_portfolio_history.as_mut(),
                        holder_authority_stage.as_ref(),
                    )
                {
                    history.capture(
                        last_location,
                        build_authority_portfolio_history_state(
                            &owner_post_balances,
                            &positive_owner_registry_ids,
                            &history_owner_on_curve,
                            &registry,
                            stage,
                        )?,
                        false,
                    )?;
                }

                outer.clear();
                static_ids.fill(0);
                let callback_error = Cell::new(None::<&'static str>);
                let mut static_count = 0usize;
                let message = projector(record.source_wire_profile)
                    .visit_static_accounts_and_instructions_exact(
                        record.message_bytes,
                        registry_entries,
                        |ordinal, reference| {
                            static_count = ordinal + 1;
                            let Some(id) = compact_id(reference, registry_entries) else {
                                callback_error.set(Some("message has an unresolved static key"));
                                return;
                            };
                            static_ids[ordinal] = id;
                        },
                        |instruction| {
                            if callback_error.get().is_some() {
                                return;
                            }
                            let result = (|| -> Result<()> {
                                outer.push(StagedOuterInstruction {
                                    program_id_index: u32::from(instruction.program_id_index),
                                    accounts: SliceRange::capture(
                                        record.message_bytes,
                                        instruction.accounts,
                                        "outer accounts",
                                    )?,
                                    data: instruction
                                        .raw_data
                                        .map(|data| {
                                            SliceRange::capture(
                                                record.message_bytes,
                                                data,
                                                "outer data",
                                            )
                                        })
                                        .transpose()?,
                                });
                                Ok(())
                            })();
                            if result.is_err() {
                                callback_error.set(Some("cannot stage outer instruction"));
                            }
                        },
                    )?;
                ensure!(
                    callback_error.get().is_none()
                        && static_count == message.static_account_count
                        && outer.len() == message.instruction_count,
                    "message callbacks differ from its summary"
                );
                validate_inventory_message_summary(&message, record.flags, record.signature_count)?;
                if let Some(stage) = holder_authority_stage.as_mut() {
                    stage.observe_signers(
                        &static_ids[..usize::from(message.num_required_signatures)],
                    )?;
                }

                let (selected_schema, metadata) = select_metadata_stage(
                    &mut current,
                    &mut legacy,
                    &record,
                    &message,
                    registry_entries,
                    target_mint_id,
                    &mut counters,
                )?;
                if metadata.summary.is_some_and(|summary| {
                    !summary.has_error && !summary.inner_instructions_present
                }) {
                    checked_add(
                        &mut counters.successful_transactions_without_inner_instruction_recording,
                        1,
                        "successful transaction without inner recording count overflow",
                    )?;
                }
                if replay_active && let Some(blocker) = replay_input_blocker(metadata.summary) {
                    bump(&mut blockers, blocker.code())?;
                    first_failure = Some(FirstReplayFailure {
                        source_epoch: record.source_epoch,
                        slot: record.block.slot,
                        source_block_id: record.source_block_id,
                        tx_index: record.tx_index,
                        phase: "replay_input",
                        code: blocker.code().to_owned(),
                        detail: blocker.detail().to_owned(),
                        outer_index: None,
                        inner_index: None,
                    });
                    replay_active = false;
                }
                let total_accounts = message
                    .static_account_count
                    .checked_add(message.expected_loaded_writable)
                    .and_then(|value| value.checked_add(message.expected_loaded_readonly))
                    .context("resolved account count overflow")?;
                resolved_accounts[..message.static_account_count]
                    .copy_from_slice(&static_ids[..message.static_account_count]);
                resolved_accounts[message.static_account_count..total_accounts].copy_from_slice(
                    &metadata.loaded_ids[message.static_account_count..total_accounts],
                );
                ensure!(
                    resolved_accounts[..total_accounts]
                        .iter()
                        .all(|id| *id != 0),
                    "message account resolution is incomplete"
                );
                if owner_projection_requested {
                    mentioned_target_indices.clear();
                    for &registry_id in &resolved_accounts[..total_accounts] {
                        let target_index = target_account_by_registry_id
                            .get(usize::try_from(registry_id)?)
                            .copied()
                            .filter(|value| *value != u32::MAX);
                        if let Some(target_index) = target_index {
                            mentioned_target_indices.push(target_index);
                        }
                    }
                    mentioned_target_indices.sort_unstable();
                    mentioned_target_indices.dedup();
                }

                checked_add(
                    &mut counters.pre_target_oracle_rows,
                    u64::try_from(metadata.pre.len())?,
                    "pre oracle row count overflow",
                )?;
                checked_add(
                    &mut counters.post_target_oracle_rows,
                    u64::try_from(metadata.post.len())?,
                    "post oracle row count overflow",
                )?;
                if !metadata.pre.is_empty() || !metadata.post.is_empty() {
                    checked_add(
                        &mut counters.transactions_with_target_oracle_rows,
                        1,
                        "transactions with oracle rows overflow",
                    )?;
                }

                ordered.clear();
                ordered_registry_ids.clear();
                invocation_sources.clear();
                let mut inner_cursor = 0usize;
                for (outer_index, outer_instruction) in outer.iter().enumerate() {
                    let program_account_index =
                        usize::try_from(outer_instruction.program_id_index)?;
                    let program_id = raw_registry_key(
                        &registry,
                        *resolved_accounts
                            .get(program_account_index)
                            .context("outer program index exceeds resolved accounts")?,
                    )
                    .context("outer program ID cannot be resolved")?;
                    ordered.push(OrderedInvocation::outer(
                        u32::try_from(outer_index)?,
                        program_id,
                    ));
                    ordered_registry_ids.push(
                        *resolved_accounts
                            .get(program_account_index)
                            .context("outer program index exceeds resolved accounts")?,
                    );
                    invocation_sources.push((false, outer_index));
                    while let Some(inner_instruction) = metadata.inner.get(inner_cursor)
                        && usize::try_from(inner_instruction.outer_index)? == outer_index
                    {
                        let inner_program_index =
                            usize::try_from(inner_instruction.program_id_index)?;
                        let inner_program_id = raw_registry_key(
                            &registry,
                            *resolved_accounts
                                .get(inner_program_index)
                                .context("inner program index exceeds resolved accounts")?,
                        )
                        .context("inner program ID cannot be resolved")?;
                        ordered.push(OrderedInvocation::inner(
                            inner_instruction.outer_index,
                            inner_instruction.inner_index,
                            inner_program_id,
                            inner_instruction.stack_height,
                        ));
                        ordered_registry_ids.push(
                            *resolved_accounts
                                .get(inner_program_index)
                                .context("inner program index exceeds resolved accounts")?,
                        );
                        invocation_sources.push((true, inner_cursor));
                        inner_cursor += 1;
                    }
                }
                ensure!(
                    inner_cursor == metadata.inner.len()
                        && ordered_registry_ids.len() == ordered.len(),
                    "inner instructions are not grouped under valid outer instructions"
                );

                let transaction_succeeded = metadata.summary.map(|value| !value.has_error);
                if transaction_succeeded == Some(true) {
                    checked_add(
                        &mut counters.successful_transactions,
                        1,
                        "successful transaction count overflow",
                    )?;
                } else if transaction_succeeded == Some(false) {
                    checked_add(
                        &mut counters.failed_transactions,
                        1,
                        "failed transaction count overflow",
                    )?;
                } else {
                    bump(&mut census_findings, "transaction_outcome_missing")?;
                }

                let mut needs_inner_commit_trace = false;
                for (position, (is_inner, source_index)) in
                    invocation_sources.iter().copied().enumerate()
                {
                    if !is_inner {
                        continue;
                    }
                    let source = &metadata.inner[source_index];
                    let accounts_slice = source
                        .accounts
                        .get(record.metadata_bytes, "inner accounts")?;
                    let is_relevant_token = program_kind(
                        ordered[position].program_id,
                        legacy_program,
                        token_2022_program,
                    )
                    .is_some()
                        && account_slice_is_target(
                            accounts_slice,
                            &resolved_accounts[..total_accounts],
                            target_mint_id,
                            &target_account_by_registry_id,
                        )?;
                    let is_runtime_owner_setting = ordered[position].program_id == system_program
                        && source
                            .data
                            .get(record.metadata_bytes, "inner System data")
                            .ok()
                            .is_some_and(is_system_runtime_owner_assignment);
                    if is_relevant_token || is_runtime_owner_setting {
                        needs_inner_commit_trace = true;
                        break;
                    }
                }

                let mut force_unknown_inner = false;
                log_boundaries.clear();
                let mut selected_classification = None;
                if transaction_succeeded == Some(true) && needs_inner_commit_trace {
                    let schema = selected_schema
                        .context("inner instructions have no selected metadata schema")?;
                    let (log_summary, normalization) = collect_log_boundaries(
                        &mut log_boundaries,
                        record.metadata_bytes,
                        schema,
                        &message,
                        registry_entries,
                        &registry,
                    )?;
                    ensure!(
                        metadata.summary.is_some_and(|summary| {
                            summary.has_error == log_summary.has_error
                                && summary.logs_present == log_summary.logs_present
                                && summary.inner_instruction_count
                                    == log_summary.metadata.inner_instruction_count
                        }),
                        "log and token metadata projections differ"
                    );
                    if normalization.ambiguous_custom_failure {
                        checked_add(
                            &mut counters.ambiguous_custom_failure_log_transactions,
                            1,
                            "ambiguous custom failure transaction count overflow",
                        )?;
                    }
                    if normalization.truncated {
                        checked_add(
                            &mut counters.truncated_log_transactions,
                            1,
                            "truncated log transaction count overflow",
                        )?;
                    }
                    if !log_summary.logs_present {
                        bump(&mut census_findings, "inner_instructions_without_logs")?;
                        force_unknown_inner = true;
                    } else {
                        let interpretation = if normalization.ambiguous_custom_failure {
                            classify_log_interpretations(true, &ordered, &log_boundaries)
                        } else {
                            classify_unambiguous_log_trace(
                                true,
                                &ordered,
                                &log_boundaries,
                                &mut log_events,
                                &mut log_stack,
                            )
                        };
                        match interpretation {
                            LogInterpretation::Unique(classification) => {
                                if normalization.ambiguous_custom_failure {
                                    checked_add(
                                        &mut counters
                                            .ambiguous_custom_failure_log_transactions_resolved,
                                        1,
                                        "resolved ambiguous custom failure count overflow",
                                    )?;
                                }
                                selected_classification = Some(classification);
                            }
                            LogInterpretation::NoValidTrace => {
                                bump(&mut census_findings, "no_valid_log_boundary_interpretation")?;
                                force_unknown_inner = true;
                            }
                            LogInterpretation::Divergent => {
                                bump(&mut census_findings, "divergent_custom_failure_log_meaning")?;
                                force_unknown_inner = true;
                            }
                            LogInterpretation::TooMany => {
                                bump(
                                    &mut census_findings,
                                    "too_many_custom_failure_log_interpretations",
                                )?;
                                force_unknown_inner = true;
                            }
                            LogInterpretation::StructuralDiagnostics(count) => {
                                checked_add(
                                    &mut counters.commit_trace_diagnostics,
                                    u64::try_from(count)?,
                                    "commit diagnostic count overflow",
                                )?;
                                bump(&mut census_findings, "structural_log_diagnostics")?;
                                force_unknown_inner = true;
                            }
                        }
                        if normalization.ambiguous_custom_failure && force_unknown_inner {
                            checked_add(
                                &mut counters.ambiguous_custom_failure_log_transactions_unresolved,
                                1,
                                "unresolved ambiguous custom failure count overflow",
                            )?;
                        }
                    }
                }

                if let Some(classification) = &selected_classification {
                    checked_add(
                        &mut counters.commit_trace_diagnostics,
                        u64::try_from(classification.diagnostics.len())?,
                        "commit diagnostic count overflow",
                    )?;
                }

                invocation_commit_statuses.clear();
                for (position, (is_inner, _)) in invocation_sources.iter().copied().enumerate() {
                    let status = match transaction_succeeded {
                        None => CommitStatus::Unknown(UnknownReason::MalformedLogTrace),
                        Some(false) => CommitStatus::RolledBack(RollbackReason::TransactionFailed),
                        Some(true) if !is_inner => CommitStatus::Committed,
                        Some(true) if force_unknown_inner => {
                            CommitStatus::Unknown(UnknownReason::MalformedLogTrace)
                        }
                        Some(true) => selected_classification
                            .as_ref()
                            .and_then(|classification| classification.invocations.get(position))
                            .map_or(
                                CommitStatus::Unknown(UnknownReason::MissingInvocationLog),
                                |classified| classified.status,
                            ),
                    };
                    invocation_commit_statuses.push(status);
                }
                ensure!(
                    invocation_commit_statuses.len() == invocation_sources.len(),
                    "invocation commit status count differs from staged instructions"
                );

                if let Some(stage) = holder_authority_stage.as_mut() {
                    for (position, (is_inner, source_index)) in
                        invocation_sources.iter().copied().enumerate()
                    {
                        if invocation_commit_statuses[position] != CommitStatus::Committed
                            || ordered[position].program_id != system_program
                        {
                            continue;
                        }
                        let (accounts_slice, data) = if is_inner {
                            let Some(source) = metadata.inner.get(source_index) else {
                                continue;
                            };
                            let Ok(accounts_slice) = source
                                .accounts
                                .get(record.metadata_bytes, "inner System accounts")
                            else {
                                continue;
                            };
                            (
                                accounts_slice,
                                source
                                    .data
                                    .get(record.metadata_bytes, "inner System data")
                                    .ok(),
                            )
                        } else {
                            let Some(source) = outer.get(source_index) else {
                                continue;
                            };
                            let Ok(accounts_slice) = source
                                .accounts
                                .get(record.message_bytes, "outer System accounts")
                            else {
                                continue;
                            };
                            (
                                accounts_slice,
                                source.data.and_then(|range| {
                                    range.get(record.message_bytes, "outer System data").ok()
                                }),
                            )
                        };
                        let Some(data) = data else {
                            continue;
                        };
                        let Some(assignment) =
                            decode_system_runtime_owner_assignment(data, accounts_slice.len())
                        else {
                            continue;
                        };
                        let Some(&account_message_index) =
                            accounts_slice.get(assignment.account_position)
                        else {
                            continue;
                        };
                        let account_message_index = usize::from(account_message_index);
                        let Some(&holder_registry_id) =
                            resolved_accounts.get(account_message_index)
                        else {
                            continue;
                        };
                        let Some(holder) = raw_registry_key(&registry, holder_registry_id) else {
                            continue;
                        };
                        if SolanaPubkey::new_from_array(holder).is_on_curve() {
                            continue;
                        }
                        let location = RuntimeOwnerObservationLocation {
                            transaction_id: counters.transactions_scanned,
                            outer_index: ordered[position].coordinate.outer_index,
                            inner_index: ordered[position].coordinate.inner_index,
                            source_epoch: record.source_epoch,
                            slot: record.block.slot,
                            source_block_id: record.source_block_id,
                            tx_index: record.tx_index,
                        };
                        stage.observe_runtime_account_owner(
                            holder_registry_id,
                            assignment.program_id,
                            location,
                        )?;
                        let direct_caller_program_registry_id =
                            direct_depth_2_derivation_program_position(&ordered, position)
                                .map(|parent_position| ordered_registry_ids[parent_position]);
                        let signer_candidate_registry_ids = outer_instruction_signer_candidates(
                            &outer,
                            ordered[position].coordinate.outer_index,
                            record.message_bytes,
                            &resolved_accounts[..total_accounts],
                            &static_ids[..usize::from(message.num_required_signatures)],
                            &registry,
                        )?;
                        stage
                            .pda_creation_provenance
                            .push(PdaCreationProvenanceValue {
                                subject_registry_id: holder_registry_id,
                                instruction: assignment.instruction,
                                runtime_owner_program_id: assignment.program_id,
                                direct_caller_program_registry_id,
                                create_with_seed_base: assignment.create_with_seed_base,
                                signer_candidate_registry_ids,
                                location,
                            });
                    }
                }

                let mut transaction_has_parsed_dex_swap = false;
                if let (Some(stage), Some(dispatch)) =
                    (holder_authority_stage.as_mut(), dex_dispatch.as_ref())
                {
                    for (position, (is_inner, source_index)) in
                        invocation_sources.iter().copied().enumerate()
                    {
                        if invocation_commit_statuses[position] != CommitStatus::Committed {
                            continue;
                        }
                        let (accounts_slice, data) = if is_inner {
                            let source = metadata
                                .inner
                                .get(source_index)
                                .context("inner DEX invocation source is missing")?;
                            (
                                source
                                    .accounts
                                    .get(record.metadata_bytes, "inner DEX accounts")?,
                                Some(source.data.get(record.metadata_bytes, "inner DEX data")?),
                            )
                        } else {
                            let source = outer
                                .get(source_index)
                                .context("outer DEX invocation source is missing")?;
                            (
                                source
                                    .accounts
                                    .get(record.message_bytes, "outer DEX accounts")?,
                                source
                                    .data
                                    .map(|range| range.get(record.message_bytes, "outer DEX data"))
                                    .transpose()?,
                            )
                        };
                        if let Some(data) = data {
                            transaction_has_parsed_dex_swap |= observe_dex_parser_authority(
                                stage,
                                dispatch,
                                ordered_registry_ids[position],
                                data,
                                accounts_slice,
                                &resolved_accounts[..total_accounts],
                                &mut dex_account_scratch,
                            )?;
                        }
                    }
                }

                staged_reducer_instructions.clear();
                reducer_account_storage.clear();
                for (position, (is_inner, source_index)) in
                    invocation_sources.iter().copied().enumerate()
                {
                    let program = match program_kind(
                        ordered[position].program_id,
                        legacy_program,
                        token_2022_program,
                    ) {
                        Some(value) => value,
                        None => continue,
                    };
                    let (accounts_slice, data, data_range) = if is_inner {
                        let source = metadata
                            .inner
                            .get(source_index)
                            .context("inner invocation source is missing")?;
                        checked_add(
                            &mut counters.inner_token_invocations,
                            1,
                            "inner token invocation count overflow",
                        )?;
                        (
                            source
                                .accounts
                                .get(record.metadata_bytes, "inner accounts")?,
                            Some(source.data.get(record.metadata_bytes, "inner data")?),
                            InstructionDataRange::Metadata(source.data),
                        )
                    } else {
                        let source = outer
                            .get(source_index)
                            .context("outer invocation source is missing")?;
                        checked_add(
                            &mut counters.outer_token_invocations,
                            1,
                            "outer token invocation count overflow",
                        )?;
                        (
                            source
                                .accounts
                                .get(record.message_bytes, "outer accounts")?,
                            source
                                .data
                                .map(|range| range.get(record.message_bytes, "outer data"))
                                .transpose()?,
                            source.data.map_or(
                                InstructionDataRange::Missing,
                                InstructionDataRange::Message,
                            ),
                        )
                    };
                    let commit_status = invocation_commit_statuses[position];

                    if is_inner
                        && commit_status == CommitStatus::Committed
                        && let Some(parent_position) =
                            direct_depth_2_derivation_program_position(&ordered, position)
                        && let Some(data) = data
                        && let Some(stage) = holder_authority_stage.as_mut()
                    {
                        observe_direct_depth_2_owner_authority(
                            stage,
                            program,
                            data,
                            accounts_slice,
                            &resolved_accounts[..total_accounts],
                            &target_account_by_registry_id,
                            &reducer,
                            &registry,
                            ordered_registry_ids[parent_position],
                        )?;
                    }

                    let is_target = account_slice_is_target(
                        accounts_slice,
                        &resolved_accounts[..total_accounts],
                        target_mint_id,
                        &target_account_by_registry_id,
                    )?;
                    let accounts_start = reducer_account_storage.len();
                    let accounts_len = if is_target {
                        for &account_index in accounts_slice {
                            let registry_id = resolved_accounts
                                .get(usize::from(account_index))
                                .copied()
                                .context("instruction account index exceeds resolved accounts")?;
                            let pubkey = raw_registry_key(&registry, registry_id)
                                .context("instruction account public key cannot be resolved")?;
                            let target_index = target_account_by_registry_id
                                .get(usize::try_from(registry_id)?)
                                .copied()
                                .filter(|value| *value != u32::MAX);
                            reducer_account_storage.push(ResolvedInstructionAccount {
                                pubkey,
                                target_index,
                            });
                        }
                        u16::try_from(accounts_slice.len())?
                    } else {
                        // Keep the complete token invocation sequence for the
                        // reducer. The ID-only scan above proved that this
                        // instruction is irrelevant, so an empty resolved
                        // account slice has the same reducer meaning without
                        // resolving or storing every public key.
                        0
                    };
                    staged_reducer_instructions.push(StagedReducerInstruction {
                        coordinate: ordered[position].coordinate,
                        program,
                        data: data_range,
                        accounts_start: u32::try_from(accounts_start)?,
                        accounts_len,
                        commit_status,
                    });
                    if !is_target {
                        continue;
                    }
                    checked_add(
                        &mut counters.target_relevant_token_invocations,
                        1,
                        "target-relevant invocation count overflow",
                    )?;
                    match commit_status {
                        CommitStatus::Committed => checked_add(
                            &mut counters.committed_target_token_invocations,
                            1,
                            "committed target invocation count overflow",
                        )?,
                        CommitStatus::RolledBack(_) => checked_add(
                            &mut counters.rolled_back_target_token_invocations,
                            1,
                            "rolled-back target invocation count overflow",
                        )?,
                        CommitStatus::Unknown(_) => checked_add(
                            &mut counters.unknown_commit_target_token_invocations,
                            1,
                            "unknown target commit count overflow",
                        )?,
                    }
                    let Some(data) = data else {
                        checked_add(
                            &mut counters.malformed_target_token_invocations,
                            1,
                            "missing raw token data count overflow",
                        )?;
                        bump(&mut census_findings, "token_instruction_without_raw_data")?;
                        continue;
                    };
                    record_decoded(
                        &mut counters,
                        &mut instruction_name_counts,
                        decode_token_instruction(program, data),
                    )?;
                }

                if replay_active {
                    checked_add(
                        &mut counters.replay_transactions_attempted,
                        1,
                        "replay attempted transaction count overflow",
                    )?;
                    match compare_oracle_side(
                        "pre",
                        &metadata.pre,
                        &[],
                        &mut pre_rows_by_message,
                        &resolved_accounts[..total_accounts],
                        &target_account_by_registry_id,
                        &reducer,
                        &registry,
                        token_2022_program,
                        SPYX_DECIMALS,
                    )? {
                        Ok(compared) => checked_add(
                            &mut counters.oracle_pre_rows_compared,
                            compared,
                            "pre oracle comparison count overflow",
                        )?,
                        Err(mismatch) => {
                            checked_add(
                                &mut counters.oracle_pre_mismatches,
                                1,
                                "pre oracle mismatch count overflow",
                            )?;
                            bump(&mut blockers, mismatch.code)?;
                            first_failure = Some(FirstReplayFailure {
                                source_epoch: record.source_epoch,
                                slot: record.block.slot,
                                source_block_id: record.source_block_id,
                                tx_index: record.tx_index,
                                phase: "pre_oracle",
                                code: mismatch.code.to_owned(),
                                detail: mismatch.detail,
                                outer_index: None,
                                inner_index: None,
                            });
                            replay_active = false;
                        }
                    }
                }

                if replay_active && owner_projection_requested {
                    begin_owner_projection(
                        &reducer,
                        &mentioned_target_indices,
                        &registry,
                        &mut owner_registry_cache,
                        &mut owner_registry_ids,
                    )?;
                }
                if replay_active {
                    for staged in &staged_reducer_instructions {
                        let accounts_start = usize::try_from(staged.accounts_start)?;
                        let accounts_end = accounts_start
                            .checked_add(usize::from(staged.accounts_len))
                            .context("staged reducer account range overflow")?;
                        ensure!(
                            reducer_account_storage
                                .get(accounts_start..accounts_end)
                                .is_some(),
                            "staged reducer account range exceeds storage"
                        );
                        match staged.data {
                            InstructionDataRange::Message(range) => {
                                range.get(record.message_bytes, "reducer outer data")?;
                            }
                            InstructionDataRange::Metadata(range) => {
                                range.get(record.metadata_bytes, "reducer inner data")?;
                            }
                            InstructionDataRange::Missing => {}
                        }
                    }
                    let instruction_views = staged_reducer_instructions.iter().map(|staged| {
                        let accounts_start = staged.accounts_start as usize;
                        let accounts_end = accounts_start + usize::from(staged.accounts_len);
                        let data = match staged.data {
                            InstructionDataRange::Message(range) => {
                                let start = range.start as usize;
                                &record.message_bytes[start..start + range.len as usize]
                            }
                            InstructionDataRange::Metadata(range) => {
                                let start = range.start as usize;
                                &record.metadata_bytes[start..start + range.len as usize]
                            }
                            InstructionDataRange::Missing => &[],
                        };
                        ResolvedTokenInstruction {
                            coordinate: staged.coordinate,
                            program: staged.program,
                            data,
                            accounts: &reducer_account_storage[accounts_start..accounts_end],
                            commit_status: staged.commit_status,
                        }
                    });
                    match reducer.apply_transaction_iter(instruction_views) {
                        Ok(_) => {
                            checked_add(
                                &mut counters.replay_transactions_applied,
                                1,
                                "applied replay transaction count overflow",
                            )?;
                        }
                        Err(error) => {
                            checked_add(
                                &mut counters.replay_errors,
                                1,
                                "replay error count overflow",
                            )?;
                            let error_code = replay_error_report_code(error.reason);
                            bump(&mut blockers, &error_code)?;
                            first_failure = Some(FirstReplayFailure {
                                source_epoch: record.source_epoch,
                                slot: record.block.slot,
                                source_block_id: record.source_block_id,
                                tx_index: record.tx_index,
                                phase: "instruction_effect",
                                code: error_code,
                                detail: error.to_string(),
                                outer_index: Some(error.coordinate.outer_index),
                                inner_index: error.coordinate.inner_index,
                            });
                            replay_active = false;
                        }
                    }
                }

                if replay_active {
                    match require_changed_pre_oracle_rows(
                        reducer.last_account_changes(),
                        &pre_rows_by_message,
                        &resolved_accounts[..total_accounts],
                        &target_account_by_registry_id,
                    )? {
                        Ok(()) => {}
                        Err(mismatch) => {
                            checked_add(
                                &mut counters.oracle_pre_mismatches,
                                1,
                                "pre oracle mismatch count overflow",
                            )?;
                            bump(&mut blockers, mismatch.code)?;
                            first_failure = Some(FirstReplayFailure {
                                source_epoch: record.source_epoch,
                                slot: record.block.slot,
                                source_block_id: record.source_block_id,
                                tx_index: record.tx_index,
                                phase: "pre_oracle",
                                code: mismatch.code.to_owned(),
                                detail: mismatch.detail,
                                outer_index: None,
                                inner_index: None,
                            });
                            replay_active = false;
                        }
                    }
                }

                if replay_active {
                    match compare_oracle_side(
                        "post",
                        &metadata.post,
                        reducer.last_account_changes(),
                        &mut post_rows_by_message,
                        &resolved_accounts[..total_accounts],
                        &target_account_by_registry_id,
                        &reducer,
                        &registry,
                        token_2022_program,
                        SPYX_DECIMALS,
                    )? {
                        Ok(compared) => {
                            checked_add(
                                &mut counters.oracle_post_rows_compared,
                                compared,
                                "post oracle comparison count overflow",
                            )?;
                            counters.replay_clean_prefix_transactions = counters
                                .transactions_scanned
                                .checked_add(1)
                                .context("clean replay prefix count overflow")?;
                        }
                        Err(mismatch) => {
                            checked_add(
                                &mut counters.oracle_post_mismatches,
                                1,
                                "post oracle mismatch count overflow",
                            )?;
                            bump(&mut blockers, mismatch.code)?;
                            first_failure = Some(FirstReplayFailure {
                                source_epoch: record.source_epoch,
                                slot: record.block.slot,
                                source_block_id: record.source_block_id,
                                tx_index: record.tx_index,
                                phase: "post_oracle",
                                code: mismatch.code.to_owned(),
                                detail: mismatch.detail,
                                outer_index: None,
                                inner_index: None,
                            });
                            replay_active = false;
                        }
                    }
                }
                if replay_active
                    && (holder_authority_stage.is_some() || visit_owner_balance_history.is_some())
                {
                    collect_public_owner_deltas(
                        reducer.last_account_changes(),
                        &reducer,
                        &registry,
                        &mut owner_registry_cache,
                        &mut owner_activity_deltas,
                    )?;
                }
                if replay_active && let Some(stage) = holder_authority_stage.as_mut() {
                    observe_public_owner_activity(
                        stage,
                        &owner_activity_deltas,
                        counters
                            .transactions_scanned
                            .checked_add(1)
                            .context("holder activity transaction ordinal overflow")?,
                        CandidateFlowLocation {
                            transaction_id: counters.transactions_scanned,
                            slot: record.block.slot,
                            block_time: record.block.block_time,
                        },
                        &static_ids[..usize::from(message.num_required_signatures)],
                        transaction_has_parsed_dex_swap,
                        &registry,
                    )?;
                }
                if replay_active && owner_projection_requested {
                    finish_owner_projection(
                        &reducer,
                        &mentioned_target_indices,
                        &registry,
                        &mut owner_registry_cache,
                        &mut owner_registry_ids,
                    )?;
                }
                if replay_active && let Some(visitor) = visit_owners.as_deref_mut() {
                    visitor(counters.transactions_scanned, &owner_registry_ids)?;
                }
                if replay_active && owner_balance_state_requested {
                    owner_balance_changes.clear();
                    for &(owner_registry_id, raw_delta) in &owner_activity_deltas {
                        let index = usize::try_from(owner_registry_id)
                            .context("owner balance registry ID exceeds usize")?;
                        let post_raw_balance = owner_post_balances
                            .get_mut(index)
                            .context("owner balance registry ID exceeds its bounded table")?;
                        let was_positive = *post_raw_balance != 0;
                        if raw_delta > 0 {
                            *post_raw_balance = post_raw_balance
                                .checked_add(raw_delta.unsigned_abs())
                                .context("owner post-transaction balance overflow")?;
                        } else {
                            *post_raw_balance = post_raw_balance
                                .checked_sub(raw_delta.unsigned_abs())
                                .context("owner post-transaction balance underflow")?;
                        }
                        let is_positive = *post_raw_balance != 0;
                        if authority_portfolio_history.is_some() && was_positive != is_positive {
                            if is_positive {
                                ensure!(
                                    positive_owner_positions[index] == u32::MAX,
                                    "positive history owner is already indexed"
                                );
                                positive_owner_positions[index] =
                                    u32::try_from(positive_owner_registry_ids.len())?;
                                positive_owner_registry_ids.push(owner_registry_id);
                            } else {
                                let position = positive_owner_positions[index];
                                ensure!(
                                    position != u32::MAX,
                                    "zeroed history owner is absent from the positive index"
                                );
                                let position = usize::try_from(position)?;
                                let removed = positive_owner_registry_ids.swap_remove(position);
                                ensure!(
                                    removed == owner_registry_id,
                                    "positive history owner index is inconsistent"
                                );
                                positive_owner_positions[index] = u32::MAX;
                                if let Some(&moved_registry_id) =
                                    positive_owner_registry_ids.get(position)
                                {
                                    positive_owner_positions[usize::try_from(moved_registry_id)?] =
                                        u32::try_from(position)?;
                                }
                            }
                        }
                        if visit_owner_balance_history.is_some() {
                            owner_balance_changes.push(
                                crate::consolidate::SpyxOwnerBalanceChange {
                                    owner_registry_id,
                                    raw_delta,
                                    post_raw_balance: *post_raw_balance,
                                },
                            );
                        }
                    }
                }
                if replay_active && authority_portfolio_history.is_some() {
                    last_history_location = Some(HistoryLocation {
                        transaction_id: counters.transactions_scanned,
                        slot: record.block.slot,
                        block_time: record.block.block_time,
                    });
                }
                if replay_active && let Some(visitor) = visit_owner_balance_history.as_deref_mut() {
                    visitor(crate::consolidate::SpyxOwnerBalanceTransaction {
                        transaction_id: counters.transactions_scanned,
                        slot: record.block.slot,
                        block_time: record.block.block_time,
                        linked_owner_registry_ids: &owner_registry_ids,
                        balance_changes: &owner_balance_changes,
                    })?;
                }
                if selected_schema.is_none() && !metadata.inner.is_empty() {
                    bump(
                        &mut census_findings,
                        "inner_instructions_without_metadata_schema",
                    )?;
                }
                checked_add(
                    &mut counters.transactions_scanned,
                    1,
                    "transaction count overflow",
                )?;
                if counters
                    .transactions_scanned
                    .is_multiple_of(REPLAY_PROGRESS_TRANSACTIONS)
                {
                    inventory_progress(
                        "SPYx instruction replay",
                        started,
                        counters.transactions_scanned,
                        manifest.transactions,
                        logical_offset,
                    );
                }
            }
        }
    };

    let mut observed_transaction_sha256 = None;
    let full_scan = match scan_end {
        ScanEnd::Prefix => false,
        ScanEnd::Footer(footer) => {
            ensure!(
                read_frame_hashed(&mut reader, &mut logical_offset, &mut hasher, &mut payload,)?
                    .is_none(),
                "consolidated transaction stream has records after its footer"
            );
            ensure!(
                counters.transactions_scanned == manifest.transactions
                    && footer.transactions_written == counters.transactions_scanned
                    && footer.pubkeys == registry_rows
                    && footer.raw_transaction_fallbacks == 0
                    && footer.raw_metadata_fallbacks == 0,
                "consolidated stream counters differ from its manifest"
            );
            let actual: [u8; 32] = hasher.finalize().into();
            ensure!(
                actual == expected_transaction_sha256,
                "transaction digest differs from its manifest"
            );
            observed_transaction_sha256 = Some(hex_digest(actual));
            true
        }
    };
    let transaction_file = reader.into_inner();
    transaction_stamp.verify(&transaction_file, "consolidated transaction stream")?;
    verify_path_binding(
        &transaction_path,
        &transaction_stamp,
        "consolidated transaction stream",
    )?;
    if full_scan {
        ensure!(
            logical_offset == transaction_stamp.bytes,
            "transaction stream size changed while it was read"
        );
    }

    if counters.unknown_commit_target_token_invocations != 0 {
        census_findings.insert(
            "unknown_inner_commit_status".to_owned(),
            counters.unknown_commit_target_token_invocations,
        );
    }
    let unknown_decode = counters
        .malformed_target_token_invocations
        .checked_add(counters.unknown_top_level_target_token_invocations)
        .and_then(|value| value.checked_add(counters.unknown_extension_target_token_invocations))
        .context("unknown decode blocker count overflow")?;
    if unknown_decode != 0 {
        census_findings.insert(
            "unknown_or_malformed_token_instruction".to_owned(),
            unknown_decode,
        );
    }

    let replay_matches = full_scan
        && replay_active
        && counters.metadata_absent == 0
        && counters.successful_transactions_without_inner_instruction_recording == 0
        && counters.replay_clean_prefix_transactions == manifest.transactions;

    let expected_owner_transactions = max_transactions.map_or(manifest.transactions, |limit| {
        limit.min(manifest.transactions)
    });
    let clean_owner_prefix =
        replay_active && counters.replay_clean_prefix_transactions == counters.transactions_scanned;
    let replayed_state = replay_state_report(&reducer, replay_matches)?;
    let holder_authority = holder_authority_stage
        .as_ref()
        .map(|stage| build_holder_authority_report(&reducer, &registry, stage, replay_matches))
        .transpose()?;
    let authority_portfolios = holder_authority_stage
        .as_ref()
        .map(|stage| {
            build_authority_portfolio_report(
                &reducer,
                &registry,
                stage,
                AuthorityPortfolioSourceBinding {
                    mint: manifest.mint.clone(),
                    first_epoch: manifest.first_epoch,
                    last_epoch: manifest.last_epoch,
                    manifest_sha256: hex_digest(manifest_sha256),
                    transactions_sha256: hex_digest(expected_transaction_sha256),
                    registry_sha256: hex_digest(expected_registry_sha256),
                    replay_state_sha256: replayed_state.state_sha256.clone(),
                },
                replay_matches,
                counters.transactions_scanned,
            )
        })
        .transpose()?;
    let authority_portfolio_history_report = if replay_matches {
        let stage = holder_authority_stage
            .as_ref()
            .context("complete authority portfolio history has no authority stage")?;
        let final_report = authority_portfolios
            .as_ref()
            .context("complete authority portfolio history has no final portfolio report")?;
        let final_location = last_history_location
            .context("complete authority portfolio history has no final transaction")?;
        let final_state = build_authority_portfolio_history_state(
            &owner_post_balances,
            &positive_owner_registry_ids,
            &history_owner_on_curve,
            &registry,
            stage,
        )?;
        validate_authority_portfolio_history_final(&final_state, final_report, &registry)?;
        let mut history = authority_portfolio_history
            .take()
            .context("complete replay has no authority portfolio history collector")?;
        history.capture(final_location, final_state.clone(), true)?;
        history.validate_final(&final_state, final_location)?;
        Some(history.into_report(
            &registry,
            AuthorityPortfolioHistorySourceBinding {
                mint: manifest.mint.clone(),
                first_epoch: manifest.first_epoch,
                last_epoch: manifest.last_epoch,
                manifest_sha256: hex_digest(manifest_sha256),
                transactions_sha256: hex_digest(expected_transaction_sha256),
                registry_sha256: hex_digest(expected_registry_sha256),
                replay_state_sha256: replayed_state.state_sha256.clone(),
            },
            counters.transactions_scanned,
        )?)
    } else {
        None
    };
    let owner_summary = crate::consolidate::SpyxOwnerReplaySummary {
        complete: replay_matches,
        transactions: counters.transactions_scanned,
        transaction_bytes_scanned: logical_offset,
        manifest_sha256: hex_digest(manifest_sha256),
        transaction_sha256: hex_digest(expected_transaction_sha256),
        registry_sha256: hex_digest(expected_registry_sha256),
        accounts_sha256: hex_digest(expected_accounts_sha256),
        replay_state_sha256: replayed_state.state_sha256.clone(),
    };
    let instruction_names = materialize_instruction_names(instruction_name_counts)?;
    let report_value = ReplayReport {
        schema_version: SPYX_REPLAY_REPORT_SCHEMA_VERSION,
        artifact_kind: "spyx_public_balance_instruction_replay",
        bounded_selected_dump_scan_complete: full_scan,
        instruction_replay_implemented: true,
        instruction_replay_matches_metadata_for_complete_spyx_selected_history: replay_matches,
        proof_scope: "public raw SPYx token-account balances in the bounded selected dump; confidential balances and live-chain snapshots are outside scope",
        status: if replay_matches {
            "complete_match"
        } else if full_scan {
            "complete_scan_replay_blocked"
        } else {
            "canary_prefix_only"
        },
        source: ReplaySourceReport {
            dump: dump.display().to_string(),
            mint: manifest.mint,
            mint_slot: manifest.mint_slot,
            first_epoch: manifest.first_epoch,
            last_epoch: manifest.last_epoch,
            manifest_sha256: hex_digest(manifest_sha256),
            expected_transaction_sha256: hex_digest(expected_transaction_sha256),
            observed_transaction_sha256,
            registry_sha256: hex_digest(expected_registry_sha256),
            accounts_sha256: hex_digest(expected_accounts_sha256),
            manifest_transactions: manifest.transactions,
            discovered_token_accounts: u64::try_from(accounts.accounts.len())?,
        },
        replayed_state,
        holder_authority,
        authority_portfolios,
        authority_portfolio_history: authority_portfolio_history_report,
        counters,
        instruction_names,
        census_findings,
        blockers,
        first_failure,
        elapsed_seconds: started.elapsed().as_secs_f64(),
    };
    if let Some(report) = report {
        let bytes = serde_json::to_vec_pretty(&report_value)?;
        publish_program_inventory_report(report, &bytes)?;
        eprintln!(
            "SPYx instruction replay: wrote {} after {:.1}s",
            report.display(),
            started.elapsed().as_secs_f64()
        );
    }
    if report.is_none() {
        ensure!(
            clean_owner_prefix,
            "owner projection requires a clean matching strict replay prefix"
        );
    }
    ensure!(
        owner_summary.transactions == expected_owner_transactions,
        "owner projection transaction count differs from its requested prefix"
    );
    Ok(owner_summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn key(value: u8) -> [u8; 32] {
        [value; 32]
    }

    fn curve_key(mut seed: u64, on_curve: bool, used: &[[u8; 32]]) -> [u8; 32] {
        loop {
            let mut candidate = [0_u8; 32];
            candidate[..8].copy_from_slice(&seed.to_le_bytes());
            candidate[8..16].copy_from_slice(&seed.rotate_left(17).to_le_bytes());
            if SolanaPubkey::new_from_array(candidate).is_on_curve() == on_curve
                && !used.contains(&candidate)
            {
                return candidate;
            }
            seed = seed.checked_add(1).unwrap();
        }
    }

    fn registry_curve_table(registry: &[u8]) -> Vec<bool> {
        std::iter::once(false)
            .chain(
                registry.chunks_exact(KEY_BYTES).map(|bytes| {
                    SolanaPubkey::new_from_array(bytes.try_into().unwrap()).is_on_curve()
                }),
            )
            .collect()
    }

    fn positive_owner_ids(owner_balances: &[u128]) -> Vec<u32> {
        owner_balances
            .iter()
            .enumerate()
            .skip(1)
            .filter(|(_, balance)| **balance != 0)
            .map(|(index, _)| u32::try_from(index).unwrap())
            .collect()
    }

    #[test]
    fn candidate_principal_floors_returns_above_deposits_at_zero() {
        let value = CandidatePrincipalValue {
            observed_deposited_principal: 10,
            observed_returned_principal: 15,
            deposit_transaction_count: 1,
            return_transaction_count: 1,
        };
        assert_eq!(value.net_principal().unwrap(), 0);
    }

    #[test]
    fn candidate_owner_net_flow_is_exact_unambiguous_signed_and_non_dex() {
        let authority = curve_key(1, true, &[]);
        let custody = curve_key(2, false, &[authority]);
        let second_custody = curve_key(3, false, &[authority, custody]);
        let mut keys = [authority, custody, second_custody];
        keys.sort_unstable();
        let registry = keys.concat();
        let id = |key| registry_id_for_key(&registry, &key).unwrap();
        let authority_id = id(authority);
        let custody_id = id(custody);
        let second_custody_id = id(second_custody);
        let mut stage = HolderAuthorityStage::new(3).unwrap();
        let location = |transaction_id, slot, block_time| CandidateFlowLocation {
            transaction_id,
            slot,
            block_time,
        };

        observe_candidate_owner_net_flow(
            &mut stage,
            &[(authority_id, -100), (custody_id, 100)],
            location(10, 1_000, Some(1_700_000_000)),
            &[authority_id],
            false,
            &registry,
        )
        .unwrap();
        let value = stage
            .candidate_principals
            .get(&(authority_id, custody_id))
            .copied()
            .unwrap();
        assert_eq!(value.observed_deposited_principal, 100);
        assert_eq!(value.net_principal().unwrap(), 100);

        observe_candidate_owner_net_flow(
            &mut stage,
            &[(custody_id, -30), (authority_id, 30)],
            location(11, 1_001, None),
            &[authority_id],
            false,
            &registry,
        )
        .unwrap();
        let value = stage
            .candidate_principals
            .get(&(authority_id, custody_id))
            .copied()
            .unwrap();
        assert_eq!(value.observed_returned_principal, 30);
        assert_eq!(value.net_principal().unwrap(), 70);
        assert_eq!(
            stage.candidate_flow_evidence[&(authority_id, custody_id)],
            [
                CandidateFlowEvidenceValue {
                    location: location(10, 1_000, Some(1_700_000_000)),
                    direction: CandidateFlowDirection::Deposit,
                    raw_amount: 100,
                    matched_principal_raw_amount: 100,
                },
                CandidateFlowEvidenceValue {
                    location: location(11, 1_001, None),
                    direction: CandidateFlowDirection::Return,
                    raw_amount: 30,
                    matched_principal_raw_amount: 30,
                },
            ]
        );

        observe_candidate_owner_net_flow(
            &mut stage,
            &[(authority_id, -40), (custody_id, 40)],
            location(12, 1_002, Some(1_700_000_002)),
            &[authority_id],
            true,
            &registry,
        )
        .unwrap();
        assert_eq!(
            stage
                .candidate_principals
                .get(&(authority_id, custody_id))
                .unwrap()
                .net_principal()
                .unwrap(),
            70
        );
        assert_eq!(stage.parsed_dex_swap_transactions_excluded, 1);

        observe_candidate_owner_net_flow(
            &mut stage,
            &[
                (authority_id, -20),
                (custody_id, 10),
                (second_custody_id, 10),
            ],
            location(13, 1_003, Some(1_700_000_003)),
            &[authority_id],
            false,
            &registry,
        )
        .unwrap();
        assert_eq!(stage.ambiguous_owner_delta_transactions_excluded, 1);
        assert!(
            !stage
                .candidate_principals
                .contains_key(&(authority_id, second_custody_id))
        );

        observe_candidate_owner_net_flow(
            &mut stage,
            &[(authority_id, -5), (second_custody_id, 5)],
            location(14, 1_004, Some(1_700_000_004)),
            &[],
            false,
            &registry,
        )
        .unwrap();
        assert!(
            !stage
                .candidate_principals
                .contains_key(&(authority_id, second_custody_id))
        );

        observe_candidate_owner_net_flow(
            &mut stage,
            &[(custody_id, -100), (authority_id, 100)],
            location(15, 1_005, Some(1_700_000_005)),
            &[authority_id],
            false,
            &registry,
        )
        .unwrap();
        let value = stage
            .candidate_principals
            .get(&(authority_id, custody_id))
            .copied()
            .unwrap();
        assert_eq!(value.observed_returned_principal, 100);
        assert_eq!(value.net_principal().unwrap(), 0);
        let partial_return = stage
            .candidate_flow_evidence
            .get(&(authority_id, custody_id))
            .unwrap()
            .last()
            .unwrap();
        assert_eq!(partial_return.direction, CandidateFlowDirection::Return);
        assert_eq!(partial_return.raw_amount, 100);
        assert_eq!(partial_return.matched_principal_raw_amount, 70);
    }

    #[test]
    fn portfolio_estimate_caps_shared_custody_and_keeps_unallocated_rows() {
        let authority_a = curve_key(10, true, &[]);
        let authority_b = curve_key(20, true, &[authority_a]);
        let authority_c = curve_key(25, true, &[authority_a, authority_b]);
        let custody = curve_key(30, false, &[authority_a, authority_b, authority_c]);
        let unknown_custody =
            curve_key(40, false, &[authority_a, authority_b, authority_c, custody]);
        let program = key(240);
        let mut registry_keys = vec![
            authority_a,
            authority_b,
            authority_c,
            custody,
            unknown_custody,
            program,
        ];
        registry_keys.sort_unstable();
        registry_keys.dedup();
        let registry = registry_keys.concat();
        let id = |key| registry_id_for_key(&registry, &key).unwrap();

        let state =
            |address, owner, amount| blockzilla_token_balance_audit::replay::TargetAccountState {
                address,
                generation: 1,
                lifecycle: AccountLifecycle::Open { owner, amount },
            };
        let reducer = TargetBalanceReducer::from_states(
            TargetMintConfig {
                mint: key(250),
                program: TokenProgram::Token2022,
                decimals: SPYX_DECIMALS,
                native: false,
                initialized: true,
                transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
            },
            vec![
                state(key(101), authority_a, 10),
                state(key(102), authority_b, 5),
                state(key(103), authority_c, 7),
                state(key(104), custody, 50),
                state(key(105), unknown_custody, 25),
            ],
        )
        .unwrap();
        let mut stage =
            HolderAuthorityStage::new(u32::try_from(registry_keys.len()).unwrap()).unwrap();
        stage
            .observe_candidate_deposit(
                id(authority_a),
                id(custody),
                60,
                CandidateFlowLocation {
                    transaction_id: 0,
                    slot: 900,
                    block_time: Some(1_700_000_000),
                },
            )
            .unwrap();
        stage
            .observe_candidate_deposit(
                id(authority_b),
                id(custody),
                40,
                CandidateFlowLocation {
                    transaction_id: 1,
                    slot: 901,
                    block_time: None,
                },
            )
            .unwrap();
        stage
            .observe_signers(&[id(authority_a), id(authority_b)])
            .unwrap();
        stage
            .observe_parser_authority(id(custody), id(program))
            .unwrap();
        stage
            .pda_creation_provenance
            .push(PdaCreationProvenanceValue {
                subject_registry_id: id(custody),
                instruction: SystemOwnerInstructionKind::CreateAccount,
                runtime_owner_program_id: program,
                direct_caller_program_registry_id: Some(id(program)),
                create_with_seed_base: None,
                signer_candidate_registry_ids: vec![id(authority_a)],
                location: RuntimeOwnerObservationLocation {
                    transaction_id: 1,
                    outer_index: 0,
                    inner_index: Some(0),
                    source_epoch: 900,
                    slot: 1_000,
                    source_block_id: 2,
                    tx_index: 3,
                },
            });

        let report = build_authority_portfolio_report(
            &reducer,
            &registry,
            &stage,
            AuthorityPortfolioSourceBinding {
                mint: bs58::encode(key(250)).into_string(),
                first_epoch: 900,
                last_epoch: 901,
                manifest_sha256: "11".repeat(32),
                transactions_sha256: "22".repeat(32),
                registry_sha256: "33".repeat(32),
                replay_state_sha256: "44".repeat(32),
            },
            true,
            2,
        )
        .unwrap();

        assert_eq!(report.artifact_kind, "spyx_authority_portfolio_heuristic");
        assert_eq!(report.schema_version, 1);
        assert!(report.coverage.candidate_flow_evidence_complete);
        assert_eq!(report.portfolios.len(), 3);
        let portfolio_a = report
            .portfolios
            .iter()
            .find(|row| row.authority == bs58::encode(authority_a).into_string())
            .unwrap();
        assert_eq!(portfolio_a.direct_public_balance.raw_amount, "10");
        assert_eq!(portfolio_a.authority_kind, "observed_transaction_signer");
        assert_eq!(portfolio_a.estimated_defi_claim.raw_amount, "30");
        assert_eq!(portfolio_a.estimated_total_exposure.raw_amount, "40");
        assert_eq!(
            portfolio_a.programs_used,
            [bs58::encode(program).into_string()]
        );
        assert_eq!(
            portfolio_a.claim_components[0]
                .candidate_net_principal
                .raw_amount,
            "60"
        );
        assert_eq!(
            portfolio_a.claim_components[0].attributed_claim.raw_amount,
            "30"
        );
        assert_eq!(
            portfolio_a.claim_components[0]
                .candidate_flow_evidence
                .len(),
            1
        );
        let flow = &portfolio_a.claim_components[0].candidate_flow_evidence[0];
        assert_eq!(flow.transaction_id, 0);
        assert_eq!(flow.slot, 900);
        assert_eq!(flow.block_time, Some(1_700_000_000));
        assert_eq!(flow.direction, "deposit");
        assert_eq!(flow.raw_amount, "60");
        assert_eq!(flow.matched_principal_raw_amount, None);
        let portfolio_c = report
            .portfolios
            .iter()
            .find(|row| row.authority == bs58::encode(authority_c).into_string())
            .unwrap();
        assert_eq!(portfolio_c.authority_kind, "other_on_curve_account");
        assert_eq!(portfolio_c.direct_public_balance.raw_amount, "7");
        assert_eq!(portfolio_c.estimated_defi_claim.raw_amount, "0");
        assert!(portfolio_c.claim_components.is_empty());

        let custody_row = report
            .protocol_custody
            .iter()
            .find(|row| row.custody_owner == bs58::encode(custody).into_string())
            .unwrap();
        assert_eq!(custody_row.direct_custody_balance.raw_amount, "50");
        assert_eq!(custody_row.candidate_net_principal.raw_amount, "100");
        assert_eq!(custody_row.attributed_claim.raw_amount, "50");
        assert_eq!(custody_row.unallocated_custody.raw_amount, "0");
        assert_eq!(custody_row.claim_excess.raw_amount, "50");
        assert_eq!(custody_row.candidate_authority_count, 2);

        let unknown_row = report
            .protocol_custody
            .iter()
            .find(|row| row.custody_owner == bs58::encode(unknown_custody).into_string())
            .unwrap();
        assert_eq!(unknown_row.program_id, None);
        assert_eq!(unknown_row.direct_custody_balance.raw_amount, "25");
        assert_eq!(unknown_row.attributed_claim.raw_amount, "0");
        assert_eq!(unknown_row.unallocated_custody.raw_amount, "25");
        assert_eq!(unknown_row.claim_excess.raw_amount, "0");

        let portfolio_total = report
            .portfolios
            .iter()
            .map(|row| {
                row.estimated_total_exposure
                    .raw_amount
                    .parse::<u128>()
                    .unwrap()
            })
            .sum::<u128>();
        let unallocated_total = report
            .protocol_custody
            .iter()
            .map(|row| row.unallocated_custody.raw_amount.parse::<u128>().unwrap())
            .sum::<u128>();
        assert_eq!(portfolio_total + unallocated_total, 97);
        assert_eq!(
            report
                .portfolios
                .iter()
                .map(|row| row.estimated_defi_claim.raw_amount.parse::<u128>().unwrap())
                .sum::<u128>(),
            report
                .protocol_custody
                .iter()
                .map(|row| row.attributed_claim.raw_amount.parse::<u128>().unwrap())
                .sum::<u128>()
        );

        assert_eq!(report.pda_creation_provenance.len(), 1);
        let provenance = &report.pda_creation_provenance[0];
        assert_eq!(provenance.subject_pda, bs58::encode(custody).into_string());
        assert_eq!(provenance.confidence, "provenance_only_no_amount_assigned");
        assert!(!provenance.proves_beneficial_ownership);
        let serialized = serde_json::to_value(provenance).unwrap();
        assert!(serialized.get("assigned_amount").is_none());
        assert!(serialized.get("attributed_claim").is_none());

        let mut owner_balances = vec![0u128; registry_keys.len() + 1];
        owner_balances[usize::try_from(id(authority_a)).unwrap()] = 10;
        owner_balances[usize::try_from(id(authority_b)).unwrap()] = 5;
        owner_balances[usize::try_from(id(authority_c)).unwrap()] = 7;
        owner_balances[usize::try_from(id(custody)).unwrap()] = 50;
        owner_balances[usize::try_from(id(unknown_custody)).unwrap()] = 25;
        let final_history_state = build_authority_portfolio_history_state(
            &owner_balances,
            &positive_owner_ids(&owner_balances),
            &registry_curve_table(&registry),
            &registry,
            &stage,
        )
        .unwrap();
        validate_authority_portfolio_history_final(&final_history_state, &report, &registry)
            .unwrap();
        let final_location = HistoryLocation {
            transaction_id: 1,
            slot: 1_001,
            block_time: Some(1_700_000_001),
        };
        let mut history = AuthorityPortfolioHistoryCollector::default();
        history
            .capture(final_location, final_history_state.clone(), true)
            .unwrap();
        history
            .validate_final(&final_history_state, final_location)
            .unwrap();
        let history = history
            .into_report(
                &registry,
                AuthorityPortfolioHistorySourceBinding {
                    mint: bs58::encode(key(250)).into_string(),
                    first_epoch: 900,
                    last_epoch: 901,
                    manifest_sha256: "11".repeat(32),
                    transactions_sha256: "22".repeat(32),
                    registry_sha256: "33".repeat(32),
                    replay_state_sha256: "44".repeat(32),
                },
                2,
            )
            .unwrap();
        let history = serde_json::to_value(history).unwrap();
        assert_eq!(history["schema_version"], 2);
        assert_eq!(
            history["point_fields"],
            serde_json::json!([
                "transaction_id",
                "slot",
                "block_time",
                "direct_public_balance_raw",
                "estimated_defi_claim_raw"
            ])
        );
        assert_eq!(
            history["coverage"]["final_sample_matches_current_portfolio"],
            true
        );
        assert_eq!(history["coverage"]["authority_series"], 3);
    }

    #[test]
    fn portfolio_history_uses_only_forward_candidate_and_custody_state() {
        let authority_a = curve_key(100, true, &[]);
        let authority_b = curve_key(200, true, &[authority_a]);
        let custody = curve_key(300, false, &[authority_a, authority_b]);
        let mut registry_keys = [authority_a, authority_b, custody];
        registry_keys.sort_unstable();
        let registry = registry_keys.concat();
        let id = |key| registry_id_for_key(&registry, &key).unwrap();
        let authority_a_id = id(authority_a);
        let authority_b_id = id(authority_b);
        let custody_id = id(custody);

        let mut stage = HolderAuthorityStage::new(3).unwrap();
        stage
            .observe_candidate_deposit(
                authority_a_id,
                custody_id,
                40,
                CandidateFlowLocation {
                    transaction_id: 0,
                    slot: 100,
                    block_time: Some(1_700_000_000),
                },
            )
            .unwrap();
        let mut owner_balances = vec![0u128; 4];
        owner_balances[usize::try_from(authority_a_id).unwrap()] = 5;
        owner_balances[usize::try_from(custody_id).unwrap()] = 20;
        let owner_on_curve = registry_curve_table(&registry);
        let early_state = build_authority_portfolio_history_state(
            &owner_balances,
            &positive_owner_ids(&owner_balances),
            &owner_on_curve,
            &registry,
            &stage,
        )
        .unwrap();
        assert_eq!(early_state[&authority_a_id].direct_public_balance, 5);
        assert_eq!(
            early_state[&authority_a_id].estimated_defi_claim().unwrap(),
            20
        );
        assert!(!early_state.contains_key(&authority_b_id));

        let early_location = HistoryLocation {
            transaction_id: 0,
            slot: 100,
            block_time: Some(1_700_000_000),
        };
        let mut history = AuthorityPortfolioHistoryCollector::default();
        history.capture(early_location, early_state, false).unwrap();

        stage
            .observe_candidate_deposit(
                authority_b_id,
                custody_id,
                60,
                CandidateFlowLocation {
                    transaction_id: 1,
                    slot: HISTORY_SLOT_WINDOW_WIDTH + 1,
                    block_time: Some(1_700_086_400),
                },
            )
            .unwrap();
        owner_balances[usize::try_from(authority_a_id).unwrap()] = 3;
        owner_balances[usize::try_from(custody_id).unwrap()] = 50;
        let final_state = build_authority_portfolio_history_state(
            &owner_balances,
            &positive_owner_ids(&owner_balances),
            &owner_on_curve,
            &registry,
            &stage,
        )
        .unwrap();
        assert_eq!(
            final_state[&authority_a_id].estimated_defi_claim().unwrap(),
            20
        );
        assert_eq!(
            final_state[&authority_b_id].estimated_defi_claim().unwrap(),
            30
        );

        let account_state =
            |address, owner, amount| blockzilla_token_balance_audit::replay::TargetAccountState {
                address,
                generation: 1,
                lifecycle: AccountLifecycle::Open { owner, amount },
            };
        let reducer = TargetBalanceReducer::from_states(
            TargetMintConfig {
                mint: key(250),
                program: TokenProgram::Token2022,
                decimals: SPYX_DECIMALS,
                native: false,
                initialized: true,
                transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
            },
            vec![
                account_state(key(101), authority_a, 3),
                account_state(key(102), custody, 50),
            ],
        )
        .unwrap();
        let final_report = build_authority_portfolio_report(
            &reducer,
            &registry,
            &stage,
            AuthorityPortfolioSourceBinding {
                mint: bs58::encode(key(250)).into_string(),
                first_epoch: 900,
                last_epoch: 901,
                manifest_sha256: "11".repeat(32),
                transactions_sha256: "22".repeat(32),
                registry_sha256: "33".repeat(32),
                replay_state_sha256: "44".repeat(32),
            },
            true,
            2,
        )
        .unwrap();
        validate_authority_portfolio_history_final(&final_state, &final_report, &registry).unwrap();

        let final_location = HistoryLocation {
            transaction_id: 1,
            slot: HISTORY_SLOT_WINDOW_WIDTH + 1,
            block_time: Some(1_700_086_400),
        };
        history
            .capture(final_location, final_state.clone(), true)
            .unwrap();
        history
            .validate_final(&final_state, final_location)
            .unwrap();
        let report = history
            .into_report(
                &registry,
                AuthorityPortfolioHistorySourceBinding {
                    mint: bs58::encode(key(250)).into_string(),
                    first_epoch: 900,
                    last_epoch: 901,
                    manifest_sha256: "11".repeat(32),
                    transactions_sha256: "22".repeat(32),
                    registry_sha256: "33".repeat(32),
                    replay_state_sha256: "44".repeat(32),
                },
                2,
            )
            .unwrap();
        let report = serde_json::to_value(report).unwrap();
        let series = report["series"].as_array().unwrap();
        let authority_a_series = series
            .iter()
            .find(|row| row["authority"] == bs58::encode(authority_a).into_string())
            .unwrap();
        let authority_b_series = series
            .iter()
            .find(|row| row["authority"] == bs58::encode(authority_b).into_string())
            .unwrap();
        assert_eq!(authority_a_series["points"].as_array().unwrap().len(), 2);
        assert_eq!(authority_a_series["points"][0][4], "20");
        assert_eq!(authority_b_series["points"].as_array().unwrap().len(), 1);
        assert_eq!(authority_b_series["points"][0][0], 1);
        assert_eq!(authority_b_series["points"][0][3], "0");
        assert_eq!(authority_b_series["points"][0][4], "30");
    }

    const fn metadata_summary(
        has_error: bool,
        inner_instructions_present: bool,
    ) -> ProjectedArchiveV2TokenMetadataSummary {
        ProjectedArchiveV2TokenMetadataSummary {
            has_error,
            pre_balance_count: 0,
            post_balance_count: 0,
            inner_instructions_present,
            inner_instruction_count: 0,
            logs_present: false,
            pre_token_balance_count: 0,
            post_token_balance_count: 0,
            loaded_writable_count: 0,
            loaded_readonly_count: 0,
            return_data_present: false,
        }
    }

    #[test]
    fn replay_requires_metadata_and_successful_inner_instruction_recording() {
        assert_eq!(
            replay_input_blocker(None),
            Some(ReplayInputBlocker::MetadataMissing)
        );
        assert_eq!(
            replay_input_blocker(Some(metadata_summary(false, false))),
            Some(ReplayInputBlocker::InnerInstructionRecordingMissing)
        );
        assert_eq!(
            replay_input_blocker(Some(metadata_summary(false, true))),
            None
        );
        assert_eq!(
            replay_input_blocker(Some(metadata_summary(true, false))),
            None
        );
        assert_eq!(
            replay_error_report_code(ReplayErrorReason::UnsupportedEffect(
                blockzilla_token_balance_audit::effect::UnsupportedEffectReason::TransferFeeEffect,
            )),
            "unsupported_effect:transfer_fee_effect"
        );
    }

    #[test]
    fn replay_state_summary_counts_and_hashes_the_exact_public_state() {
        let reducer = TargetBalanceReducer::from_states(
            TargetMintConfig {
                mint: key(9),
                program: TokenProgram::Token2022,
                decimals: 8,
                native: false,
                initialized: true,
                transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
            },
            vec![
                blockzilla_token_balance_audit::replay::TargetAccountState {
                    address: key(1),
                    generation: 1,
                    lifecycle: AccountLifecycle::Open {
                        owner: key(2),
                        amount: 5,
                    },
                },
                blockzilla_token_balance_audit::replay::TargetAccountState {
                    address: key(3),
                    generation: 2,
                    lifecycle: AccountLifecycle::Open {
                        owner: key(4),
                        amount: 0,
                    },
                },
                blockzilla_token_balance_audit::replay::TargetAccountState::closed(key(5)),
            ],
        )
        .unwrap();

        let summary = replay_state_report(&reducer, true).unwrap();
        assert!(summary.history_complete);
        assert_eq!(summary.tracked_accounts, 3);
        assert_eq!(summary.open_accounts, 2);
        assert_eq!(summary.closed_accounts, 1);
        assert_eq!(summary.positive_public_balance_accounts, 1);
        assert_eq!(summary.public_raw_balance, "5");
        assert_eq!(summary.state_sha256.len(), 64);
        assert_eq!(
            summary.state_sha256,
            replay_state_report(&reducer, false).unwrap().state_sha256
        );
    }

    #[test]
    fn owner_projection_unions_validated_pre_and_post_lifecycles() {
        let config = TargetMintConfig {
            mint: key(99),
            program: TokenProgram::Token2022,
            decimals: 8,
            native: false,
            initialized: true,
            transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
        };
        let state =
            |address, lifecycle| blockzilla_token_balance_audit::replay::TargetAccountState {
                address,
                generation: 1,
                lifecycle,
            };
        let pre = TargetBalanceReducer::from_states(
            config,
            vec![
                state(
                    key(1),
                    AccountLifecycle::Open {
                        owner: key(10),
                        amount: 0,
                    },
                ),
                state(
                    key(2),
                    AccountLifecycle::Open {
                        owner: key(12),
                        amount: 0,
                    },
                ),
                state(key(3), AccountLifecycle::Closed),
                state(key(4), AccountLifecycle::Closed),
                state(
                    key(5),
                    AccountLifecycle::Open {
                        owner: key(10),
                        amount: 0,
                    },
                ),
            ],
        )
        .unwrap();
        let post = TargetBalanceReducer::from_states(
            config,
            vec![
                state(
                    key(1),
                    AccountLifecycle::Open {
                        owner: key(11),
                        amount: 0,
                    },
                ),
                state(key(2), AccountLifecycle::Closed),
                state(
                    key(3),
                    AccountLifecycle::Open {
                        owner: key(13),
                        amount: 0,
                    },
                ),
                state(key(4), AccountLifecycle::Closed),
                state(
                    key(5),
                    AccountLifecycle::Open {
                        owner: key(10),
                        amount: 0,
                    },
                ),
            ],
        )
        .unwrap();
        let registry = [key(10), key(11), key(12), key(13)].concat();
        let mentioned = [0, 0, 1, 2, 3, 4];
        let mut cache = HashMap::new();
        let mut owner_ids = vec![u32::MAX];

        begin_owner_projection(&pre, &mentioned, &registry, &mut cache, &mut owner_ids).unwrap();
        finish_owner_projection(&post, &mentioned, &registry, &mut cache, &mut owner_ids).unwrap();

        assert_eq!(owner_ids, [1, 2, 3, 4]);
        assert_eq!(cache.len(), 4);

        begin_owner_projection(&pre, &[], &registry, &mut cache, &mut owner_ids).unwrap();
        finish_owner_projection(&post, &[], &registry, &mut cache, &mut owner_ids).unwrap();
        assert!(owner_ids.is_empty(), "mint-only mentions have no owner");

        begin_owner_projection(&pre, &[3], &registry, &mut cache, &mut owner_ids).unwrap();
        finish_owner_projection(&post, &[3], &registry, &mut cache, &mut owner_ids).unwrap();
        assert!(owner_ids.is_empty(), "closed on both sides has no owner");
    }

    #[test]
    fn oracle_allows_unchanged_omission_but_requires_changed_open_rows() {
        let state = blockzilla_token_balance_audit::replay::TargetAccountState {
            address: key(1),
            generation: 1,
            lifecycle: AccountLifecycle::Open {
                owner: key(2),
                amount: 5,
            },
        };
        let reducer = TargetBalanceReducer::from_states(
            TargetMintConfig {
                mint: key(9),
                program: TokenProgram::Token2022,
                decimals: 8,
                native: false,
                initialized: true,
                transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
            },
            vec![state],
        )
        .unwrap();
        let registry = [key(1), key(2), key(3)].concat();
        let target_by_registry_id = [u32::MAX, 0, u32::MAX, u32::MAX];
        let resolved_accounts = [1_u32];
        let mut row_by_message = [None; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];
        let change = TargetAccountChange {
            index: 0,
            previous: state,
        };
        let row = OracleRow {
            account_index: 0,
            amount: 5,
            owner_id: Some(2),
            program_id: Some(3),
            decimals: 8,
        };

        assert_eq!(
            compare_oracle_side(
                "pre",
                &[],
                &[],
                &mut row_by_message,
                &resolved_accounts,
                &target_by_registry_id,
                &reducer,
                &registry,
                key(3),
                8,
            )
            .unwrap()
            .unwrap(),
            0
        );
        let missing_pre = require_changed_pre_oracle_rows(
            &[change],
            &row_by_message,
            &resolved_accounts,
            &target_by_registry_id,
        )
        .unwrap()
        .unwrap_err();
        assert_eq!(
            missing_pre.code,
            "oracle_pre_row_missing_for_changed_open_account"
        );
        assert_eq!(
            compare_oracle_side(
                "pre",
                &[row],
                &[],
                &mut row_by_message,
                &resolved_accounts,
                &target_by_registry_id,
                &reducer,
                &registry,
                key(3),
                8,
            )
            .unwrap()
            .unwrap(),
            1
        );
        require_changed_pre_oracle_rows(
            &[change],
            &row_by_message,
            &resolved_accounts,
            &target_by_registry_id,
        )
        .unwrap()
        .unwrap();

        let missing = compare_oracle_side(
            "post",
            &[],
            &[change],
            &mut row_by_message,
            &resolved_accounts,
            &target_by_registry_id,
            &reducer,
            &registry,
            key(3),
            8,
        )
        .unwrap()
        .unwrap_err();
        assert_eq!(missing.code, "oracle_row_missing_for_mutated_open_account");

        assert_eq!(
            compare_oracle_side(
                "post",
                &[row],
                &[change],
                &mut row_by_message,
                &resolved_accounts,
                &target_by_registry_id,
                &reducer,
                &registry,
                key(3),
                8,
            )
            .unwrap()
            .unwrap(),
            1
        );
    }

    #[test]
    fn oracle_lifecycle_gate_requires_rows_only_on_open_sides() {
        let config = TargetMintConfig {
            mint: key(9),
            program: TokenProgram::Token2022,
            decimals: 8,
            native: false,
            initialized: true,
            transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
        };
        let open_state = blockzilla_token_balance_audit::replay::TargetAccountState {
            address: key(1),
            generation: 1,
            lifecycle: AccountLifecycle::Open {
                owner: key(2),
                amount: 5,
            },
        };
        let open_reducer = TargetBalanceReducer::from_states(config, vec![open_state]).unwrap();
        let closed_reducer = TargetBalanceReducer::new(config, vec![key(1)]).unwrap();
        let registry = [key(1), key(2), key(3)].concat();
        let target_by_registry_id = [u32::MAX, 0, u32::MAX, u32::MAX];
        let resolved_accounts = [1_u32];
        let row = OracleRow {
            account_index: 0,
            amount: 5,
            owner_id: Some(2),
            program_id: Some(3),
            decimals: 8,
        };
        let closed_change = TargetAccountChange {
            index: 0,
            previous: open_state,
        };
        let mut row_by_message = [None; blockzilla_read_sdk::MAX_MESSAGE_ACCOUNTS];

        compare_oracle_side(
            "pre",
            &[row],
            &[],
            &mut row_by_message,
            &resolved_accounts,
            &target_by_registry_id,
            &open_reducer,
            &registry,
            key(3),
            8,
        )
        .unwrap()
        .unwrap();
        require_changed_pre_oracle_rows(
            &[closed_change],
            &row_by_message,
            &resolved_accounts,
            &target_by_registry_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            compare_oracle_side(
                "post",
                &[],
                &[closed_change],
                &mut row_by_message,
                &resolved_accounts,
                &target_by_registry_id,
                &closed_reducer,
                &registry,
                key(3),
                8,
            )
            .unwrap()
            .unwrap(),
            0
        );
        assert_eq!(
            compare_oracle_side(
                "post",
                &[row],
                &[closed_change],
                &mut row_by_message,
                &resolved_accounts,
                &target_by_registry_id,
                &closed_reducer,
                &registry,
                key(3),
                8,
            )
            .unwrap()
            .unwrap_err()
            .code,
            "oracle_row_for_closed_account"
        );

        let opened_change = TargetAccountChange {
            index: 0,
            previous: blockzilla_token_balance_audit::replay::TargetAccountState::closed(key(1)),
        };
        row_by_message.fill(None);
        require_changed_pre_oracle_rows(
            &[opened_change],
            &row_by_message,
            &resolved_accounts,
            &target_by_registry_id,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            compare_oracle_side(
                "post",
                &[row],
                &[opened_change],
                &mut row_by_message,
                &resolved_accounts,
                &target_by_registry_id,
                &open_reducer,
                &registry,
                key(3),
                8,
            )
            .unwrap()
            .unwrap(),
            1
        );
    }

    #[test]
    fn slice_range_keeps_a_borrowed_subslice_without_copying() {
        let bytes = [10u8, 20, 30, 40, 50];
        let range = SliceRange::capture(&bytes, &bytes[1..4], "test slice").unwrap();
        let resolved = range.get(&bytes, "test slice").unwrap();
        assert_eq!(resolved, &[20, 30, 40]);
        assert_eq!(resolved.as_ptr(), bytes[1..].as_ptr());
    }

    #[test]
    fn instruction_names_allocate_only_when_the_report_is_materialized() {
        let mut counts = BTreeMap::new();
        counts.insert(
            InstructionNameKey {
                family: None,
                name: "TransferChecked",
            },
            11,
        );
        counts.insert(
            InstructionNameKey {
                family: Some("TransferFee"),
                name: "WithdrawWithheldTokensFromAccounts",
            },
            7,
        );

        let names = materialize_instruction_names(counts).unwrap();
        assert_eq!(names.get("TransferChecked"), Some(&11));
        assert_eq!(
            names.get("TransferFee::WithdrawWithheldTokensFromAccounts"),
            Some(&7)
        );
    }

    #[test]
    fn target_scan_validates_all_account_indices_after_a_match() {
        let resolved_accounts = [3u32];
        let mut targets = vec![u32::MAX; 4];
        targets[3] = 0;
        assert!(
            account_slice_is_target(&[0, 1], &resolved_accounts, 2, &targets).is_err(),
            "an invalid trailing account index must not be hidden by an early target match"
        );
    }

    #[test]
    fn direct_unambiguous_log_path_matches_the_general_classifier() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let boundaries = [
            StagedLogBoundary::Invoke {
                program_id: outer,
                depth: Some(1),
            },
            StagedLogBoundary::Invoke {
                program_id: token,
                depth: Some(2),
            },
            StagedLogBoundary::Success { program_id: token },
            StagedLogBoundary::Success { program_id: outer },
        ];
        let LogInterpretation::Unique(general) =
            classify_log_interpretations(true, &instructions, &boundaries)
        else {
            panic!("general classifier must accept the trace")
        };
        let mut events = Vec::with_capacity(boundaries.len());
        let mut stack = Vec::with_capacity(2);
        let LogInterpretation::Unique(direct) = classify_unambiguous_log_trace(
            true,
            &instructions,
            &boundaries,
            &mut events,
            &mut stack,
        ) else {
            panic!("direct classifier must accept the trace")
        };
        assert_eq!(direct.invocations, general.invocations);
        assert_eq!(direct.diagnostics, general.diagnostics);
        assert!(stack.is_empty());
    }

    #[test]
    fn ambiguous_custom_failure_resolves_when_only_terminal_is_balanced() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let boundaries = [
            StagedLogBoundary::Invoke {
                program_id: outer,
                depth: Some(1),
            },
            StagedLogBoundary::Invoke {
                program_id: token,
                depth: Some(2),
            },
            StagedLogBoundary::AmbiguousCustomFailure { program_id: token },
            StagedLogBoundary::Success { program_id: outer },
        ];
        let LogInterpretation::Unique(result) =
            classify_log_interpretations(true, &instructions, &boundaries)
        else {
            panic!("one balanced interpretation must remain")
        };
        assert!(matches!(
            result.invocations[1].status,
            CommitStatus::RolledBack(_)
        ));
    }

    #[test]
    fn ambiguous_custom_failure_stays_unknown_when_truncation_preserves_both_meanings() {
        let outer = key(1);
        let token = key(2);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, Some(2)),
        ];
        let boundaries = [
            StagedLogBoundary::Invoke {
                program_id: outer,
                depth: Some(1),
            },
            StagedLogBoundary::Invoke {
                program_id: token,
                depth: Some(2),
            },
            StagedLogBoundary::AmbiguousCustomFailure { program_id: token },
            StagedLogBoundary::Truncated,
        ];
        assert!(matches!(
            classify_log_interpretations(true, &instructions, &boundaries),
            LogInterpretation::Divergent
        ));
    }

    #[test]
    fn ambiguous_explicit_log_must_come_from_the_active_program() {
        let outer = key(1);
        let other = key(2);
        let instructions = [OrderedInvocation::outer(0, outer)];
        let boundaries = [
            StagedLogBoundary::Invoke {
                program_id: outer,
                depth: Some(1),
            },
            StagedLogBoundary::AmbiguousCustomFailure { program_id: other },
            StagedLogBoundary::Success { program_id: outer },
        ];
        assert!(matches!(
            classify_log_interpretations(true, &instructions, &boundaries),
            LogInterpretation::NoValidTrace
        ));
    }

    #[test]
    fn successful_transaction_rejects_a_root_failure_trace() {
        let outer = key(1);
        let instructions = [OrderedInvocation::outer(0, outer)];
        let boundaries = [
            StagedLogBoundary::Invoke {
                program_id: outer,
                depth: Some(1),
            },
            StagedLogBoundary::Failure { program_id: outer },
        ];
        assert!(matches!(
            classify_log_interpretations(true, &instructions, &boundaries),
            LogInterpretation::NoValidTrace
        ));
    }

    #[test]
    fn structural_alignment_diagnostics_block_a_strict_interpretation() {
        let extra = key(1);
        let outer = key(2);
        let instructions = [OrderedInvocation::outer(0, outer)];
        let boundaries = [
            StagedLogBoundary::Invoke {
                program_id: extra,
                depth: Some(1),
            },
            StagedLogBoundary::Success { program_id: extra },
            StagedLogBoundary::Invoke {
                program_id: outer,
                depth: Some(1),
            },
            StagedLogBoundary::Success { program_id: outer },
        ];
        assert!(matches!(
            classify_log_interpretations(true, &instructions, &boundaries),
            LogInterpretation::StructuralDiagnostics(_)
        ));
    }

    #[test]
    fn holder_signers_use_only_the_required_static_key_prefix() {
        let mut stage = HolderAuthorityStage::new(6).unwrap();
        let static_ids = [2_u32, 4, 5];

        stage.observe_signers(&static_ids[..2]).unwrap();
        stage.observe_signers(&static_ids[..1]).unwrap();

        assert_eq!(stage.signer_transaction_count(2).unwrap(), 2);
        assert_eq!(stage.signer_transaction_count(4).unwrap(), 1);
        assert_eq!(stage.signer_transaction_count(5).unwrap(), 0);
        assert_eq!(stage.signer_transaction_count(6).unwrap(), 0);
    }

    #[test]
    fn holder_activity_nets_internal_moves_and_tracks_owner_reassignment() {
        let owner_a = key(1);
        let owner_b = key(2);
        let mut registry_keys = [owner_a, owner_b];
        registry_keys.sort_unstable();
        let registry = registry_keys.concat();
        let owner_a_id = registry_id_for_key(&registry, &owner_a).unwrap();
        let owner_b_id = registry_id_for_key(&registry, &owner_b).unwrap();
        let state =
            |address, owner, amount| blockzilla_token_balance_audit::replay::TargetAccountState {
                address,
                generation: 1,
                lifecycle: AccountLifecycle::Open { owner, amount },
            };
        let reducer = TargetBalanceReducer::from_states(
            TargetMintConfig {
                mint: key(250),
                program: TokenProgram::Token2022,
                decimals: SPYX_DECIMALS,
                native: false,
                initialized: true,
                transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
            },
            vec![
                state(key(101), owner_a, 60),
                state(key(102), owner_a, 50),
                state(key(103), owner_b, 5),
            ],
        )
        .unwrap();
        let changes = [
            TargetAccountChange {
                index: 0,
                previous: state(key(101), owner_a, 100),
            },
            TargetAccountChange {
                index: 1,
                previous: state(key(102), owner_a, 10),
            },
            TargetAccountChange {
                index: 2,
                previous: state(key(103), owner_a, 5),
            },
        ];
        let mut stage = HolderAuthorityStage::new(2).unwrap();
        let mut cache = HashMap::new();
        let mut deltas = Vec::new();

        collect_public_owner_deltas(&changes, &reducer, &registry, &mut cache, &mut deltas)
            .unwrap();
        assert_eq!(deltas, [(owner_a_id, -5), (owner_b_id, 5)]);
        observe_public_owner_activity(
            &mut stage,
            &deltas,
            1,
            CandidateFlowLocation {
                transaction_id: 0,
                slot: 1,
                block_time: None,
            },
            &[],
            false,
            &registry,
        )
        .unwrap();

        let owner_a_activity = stage.activity(owner_a_id).unwrap();
        assert_eq!(owner_a_activity.public_balance_increase, 0);
        assert_eq!(owner_a_activity.public_balance_decrease, 5);
        assert_eq!(owner_a_activity.transaction_count, 1);
        assert_eq!(owner_a_activity.volume().unwrap(), 5);
        let owner_b_activity = stage.activity(owner_b_id).unwrap();
        assert_eq!(owner_b_activity.public_balance_increase, 5);
        assert_eq!(owner_b_activity.public_balance_decrease, 0);
        assert_eq!(owner_b_activity.transaction_count, 1);
        assert_eq!(owner_b_activity.volume().unwrap(), 5);
    }

    #[test]
    fn program_attribution_deduplicates_one_program_and_rejects_conflicts() {
        let mut evidence = ProgramAttributionEvidence::default();

        evidence.observe_parser(7).unwrap();
        evidence.observe_parser(7).unwrap();
        evidence.observe_direct_cpi(7).unwrap();

        assert_eq!(evidence.attributed_program(), Some(7));
        assert_eq!(evidence.parser_authority_observations, 2);
        assert_eq!(evidence.direct_cpi_authorizations, 1);
        assert_eq!(evidence.conflicting_program_observations, 0);
        assert_eq!(evidence.evidence_name(), "direct_cpi_and_parser_authority");
        assert_eq!(evidence.observation_count(), 3);

        evidence.observe_direct_cpi(9).unwrap();
        assert_eq!(evidence.attributed_program(), None);
        assert_eq!(evidence.conflicting_program_observations, 1);
    }

    #[test]
    fn nested_cpi_does_not_claim_the_immediate_caller_as_the_derivation_program() {
        let outer = key(1);
        let intermediate = key(2);
        let token = key(3);
        let instructions = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, intermediate, Some(2)),
            OrderedInvocation::inner(0, 1, token, Some(3)),
        ];

        assert_eq!(
            direct_depth_2_derivation_program_position(&instructions, 1),
            Some(0)
        );
        assert_eq!(
            direct_depth_2_derivation_program_position(&instructions, 2),
            None
        );

        let missing_depth = [
            OrderedInvocation::outer(0, outer),
            OrderedInvocation::inner(0, 0, token, None),
        ];
        assert_eq!(
            direct_depth_2_derivation_program_position(&missing_depth, 1),
            None
        );
    }

    #[test]
    fn system_runtime_owner_decoder_matches_runtime_layouts_and_fails_closed() {
        fn seeded_data(tag: u32, seed: &[u8], suffix: &[[u8; 8]], owner: [u8; 32]) -> Vec<u8> {
            let mut data = Vec::new();
            data.extend_from_slice(&tag.to_le_bytes());
            data.extend_from_slice(&key(4));
            data.extend_from_slice(&u64::try_from(seed.len()).unwrap().to_le_bytes());
            data.extend_from_slice(seed);
            for value in suffix {
                data.extend_from_slice(value);
            }
            data.extend_from_slice(&owner);
            data
        }

        fn assignment(
            data: &[u8],
            account_count: usize,
            account_position: usize,
            program_id: [u8; 32],
        ) {
            let decoded = decode_system_runtime_owner_assignment(data, account_count).unwrap();
            assert_eq!(decoded.account_position, account_position);
            assert_eq!(decoded.program_id, program_id);
        }

        let owner = key(9);
        let mut create = Vec::new();
        create.extend_from_slice(&0u32.to_le_bytes());
        create.extend_from_slice(&7u64.to_le_bytes());
        create.extend_from_slice(&8u64.to_le_bytes());
        create.extend_from_slice(&owner);
        assignment(&create, 2, 1, owner);
        let decoded_create = decode_system_runtime_owner_assignment(&create, 2).unwrap();
        assert_eq!(
            decoded_create.instruction,
            SystemOwnerInstructionKind::CreateAccount
        );
        assert_eq!(decoded_create.instruction.event_kind(), "account_creation");
        assert_eq!(decoded_create.create_with_seed_base, None);
        assignment(&create, 5, 1, owner);
        assert_eq!(decode_system_runtime_owner_assignment(&create, 1), None);
        assert_eq!(
            decode_system_runtime_owner_assignment(&create[..create.len() - 1], 2),
            None
        );
        let mut create_with_trailing_bytes = create.clone();
        create_with_trailing_bytes.extend_from_slice(&[0xaa, 0xbb]);
        assignment(&create_with_trailing_bytes, 2, 1, owner);

        let mut assign = Vec::new();
        assign.extend_from_slice(&1u32.to_le_bytes());
        assign.extend_from_slice(&owner);
        assignment(&assign, 1, 0, owner);
        assignment(&assign, 3, 0, owner);
        assert_eq!(decode_system_runtime_owner_assignment(&assign, 0), None);
        assert_eq!(
            decode_system_runtime_owner_assignment(&assign[..assign.len() - 1], 1),
            None
        );
        let mut assign_with_trailing_bytes = assign.clone();
        assign_with_trailing_bytes.push(0xcc);
        assignment(&assign_with_trailing_bytes, 1, 0, owner);

        let seed = b"seed";
        let create_with_seed =
            seeded_data(3, seed, &[7u64.to_le_bytes(), 8u64.to_le_bytes()], owner);
        assignment(&create_with_seed, 2, 1, owner);
        let decoded_seeded = decode_system_runtime_owner_assignment(&create_with_seed, 2).unwrap();
        assert_eq!(
            decoded_seeded.instruction,
            SystemOwnerInstructionKind::CreateAccountWithSeed
        );
        assert_eq!(decoded_seeded.create_with_seed_base, Some(key(4)));
        assignment(&create_with_seed, 5, 1, owner);
        assert_eq!(
            decode_system_runtime_owner_assignment(&create_with_seed, 1),
            None
        );
        assert_eq!(
            decode_system_runtime_owner_assignment(
                &create_with_seed[..create_with_seed.len() - 1],
                2,
            ),
            None
        );
        let mut create_with_seed_and_trailing_bytes = create_with_seed.clone();
        create_with_seed_and_trailing_bytes.push(0xdd);
        assignment(&create_with_seed_and_trailing_bytes, 2, 1, owner);

        let allocate_with_seed = seeded_data(9, seed, &[8u64.to_le_bytes()], owner);
        assignment(&allocate_with_seed, 1, 0, owner);
        assignment(&allocate_with_seed, 4, 0, owner);
        assert_eq!(
            decode_system_runtime_owner_assignment(&allocate_with_seed, 0),
            None
        );
        assert_eq!(
            decode_system_runtime_owner_assignment(
                &allocate_with_seed[..allocate_with_seed.len() - 1],
                1,
            ),
            None
        );
        let mut allocate_with_seed_and_trailing_bytes = allocate_with_seed.clone();
        allocate_with_seed_and_trailing_bytes.push(0xee);
        assignment(&allocate_with_seed_and_trailing_bytes, 1, 0, owner);

        let assign_with_seed = seeded_data(10, seed, &[], owner);
        assignment(&assign_with_seed, 1, 0, owner);
        assignment(&assign_with_seed, 4, 0, owner);
        assert_eq!(
            decode_system_runtime_owner_assignment(&assign_with_seed, 0),
            None
        );
        assert_eq!(
            decode_system_runtime_owner_assignment(
                &assign_with_seed[..assign_with_seed.len() - 1],
                1,
            ),
            None
        );
        let mut assign_with_seed_and_trailing_bytes = assign_with_seed.clone();
        assign_with_seed_and_trailing_bytes.push(0xff);
        assignment(&assign_with_seed_and_trailing_bytes, 1, 0, owner);

        let mut allow_prefund = Vec::new();
        allow_prefund.extend_from_slice(&13u32.to_le_bytes());
        allow_prefund.extend_from_slice(&0u64.to_le_bytes());
        allow_prefund.extend_from_slice(&8u64.to_le_bytes());
        allow_prefund.extend_from_slice(&owner);
        assignment(&allow_prefund, 1, 0, owner);
        assignment(&allow_prefund, 3, 0, owner);
        assert_eq!(
            decode_system_runtime_owner_assignment(&allow_prefund, 0),
            None
        );
        allow_prefund[4..12].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(
            decode_system_runtime_owner_assignment(&allow_prefund, 1),
            None
        );
        assignment(&allow_prefund, 2, 0, owner);
        assignment(&allow_prefund, 4, 0, owner);
        assert_eq!(
            decode_system_runtime_owner_assignment(&allow_prefund[..allow_prefund.len() - 1], 2),
            None
        );
        let mut allow_prefund_with_trailing_bytes = allow_prefund.clone();
        allow_prefund_with_trailing_bytes.push(0x11);
        assignment(&allow_prefund_with_trailing_bytes, 2, 0, owner);

        let oversized_seed = seeded_data(10, &[0; 33], &[], owner);
        assert_eq!(
            decode_system_runtime_owner_assignment(&oversized_seed, 1),
            None
        );
        for boundary_seed in [Vec::new(), vec![b'a'; 32]] {
            assignment(
                &seeded_data(
                    3,
                    &boundary_seed,
                    &[7u64.to_le_bytes(), 8u64.to_le_bytes()],
                    owner,
                ),
                2,
                1,
                owner,
            );
            assignment(
                &seeded_data(9, &boundary_seed, &[8u64.to_le_bytes()], owner),
                1,
                0,
                owner,
            );
            assignment(&seeded_data(10, &boundary_seed, &[], owner), 1, 0, owner);
        }
        for (invalid_utf8_seed, account_count) in [
            (
                seeded_data(3, &[0xff], &[7u64.to_le_bytes(), 8u64.to_le_bytes()], owner),
                2,
            ),
            (seeded_data(9, &[0xff], &[8u64.to_le_bytes()], owner), 1),
            (seeded_data(10, &[0xff], &[], owner), 1),
        ] {
            assert_eq!(
                decode_system_runtime_owner_assignment(&invalid_utf8_seed, account_count),
                None
            );
        }
        let mut overflowing_seed = assign_with_seed;
        overflowing_seed[36..44].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(
            decode_system_runtime_owner_assignment(&overflowing_seed, 1),
            None
        );
        let mut declared_seed_exceeds_buffer = seeded_data(10, b"x", &[], owner);
        declared_seed_exceeds_buffer[36..44].copy_from_slice(&32u64.to_le_bytes());
        assert_eq!(
            decode_system_runtime_owner_assignment(&declared_seed_exceeds_buffer, 1),
            None
        );
        assert_eq!(
            decode_system_runtime_owner_assignment(&2u32.to_le_bytes(), 0),
            None
        );
        assert_eq!(decode_system_runtime_owner_assignment(&[0, 0, 0], 2), None);
    }

    #[test]
    fn malformed_runtime_owner_candidate_does_not_stop_later_evidence() {
        fn owner_data(tag: u32, owner: [u8; 32]) -> Vec<u8> {
            let mut data = Vec::new();
            data.extend_from_slice(&tag.to_le_bytes());
            if tag == 0 {
                data.extend_from_slice(&7u64.to_le_bytes());
                data.extend_from_slice(&8u64.to_le_bytes());
            }
            data.extend_from_slice(&owner);
            data
        }

        let location = |transaction_id| RuntimeOwnerObservationLocation {
            transaction_id,
            outer_index: 0,
            inner_index: None,
            source_epoch: 900,
            slot: 1_000 + transaction_id,
            source_block_id: u32::try_from(transaction_id).unwrap(),
            tx_index: 0,
        };
        let first_owner = key(7);
        let final_owner = key(8);
        let candidates = [
            (owner_data(0, first_owner), 2, 1),
            (0u32.to_le_bytes().to_vec(), 2, 2),
            (owner_data(1, final_owner), 1, 3),
        ];
        let mut stage = HolderAuthorityStage::new(2).unwrap();

        for (data, account_count, transaction_id) in candidates {
            if let Some(assignment) = decode_system_runtime_owner_assignment(&data, account_count) {
                stage
                    .observe_runtime_account_owner(
                        1,
                        assignment.program_id,
                        location(transaction_id),
                    )
                    .unwrap();
            }
        }

        let evidence = stage.runtime_account_owners.get(&1).unwrap();
        assert_eq!(evidence.program_id, final_owner);
        assert_eq!(evidence.observation_count, 2);
        assert_eq!(evidence.owner_change_count, 1);
        assert_eq!(evidence.final_observation.transaction_id, 3);
    }

    #[test]
    fn runtime_owner_evidence_keeps_the_last_ordered_assignment() {
        let location = |transaction_id, outer_index| RuntimeOwnerObservationLocation {
            transaction_id,
            outer_index,
            inner_index: None,
            source_epoch: 900,
            slot: 1_000 + transaction_id,
            source_block_id: u32::try_from(transaction_id).unwrap(),
            tx_index: outer_index,
        };
        let mut stage = HolderAuthorityStage::new(3).unwrap();

        stage
            .observe_runtime_account_owner(1, key(2), location(4, 0))
            .unwrap();
        stage
            .observe_runtime_account_owner(1, key(3), location(7, 1))
            .unwrap();

        let evidence = stage.runtime_account_owners.get(&1).copied().unwrap();
        assert_eq!(evidence.program_id, key(3));
        assert_eq!(evidence.observation_count, 2);
        assert_eq!(evidence.owner_change_count, 1);
        assert_eq!(evidence.conflict_count, 0);
        assert_eq!(evidence.final_observation.transaction_id, 7);
        assert_eq!(evidence.final_observation.outer_index, 1);

        assert!(
            stage
                .observe_runtime_account_owner(1, key(2), location(7, 1))
                .is_err()
        );
        assert_eq!(
            stage.runtime_account_owners.get(&1).unwrap().conflict_count,
            1
        );
    }

    #[test]
    fn direct_depth_2_helper_requires_an_exact_single_authority_layout() {
        let reducer = TargetBalanceReducer::from_states(
            TargetMintConfig {
                mint: key(9),
                program: TokenProgram::Token2022,
                decimals: SPYX_DECIMALS,
                native: false,
                initialized: true,
                transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
            },
            vec![blockzilla_token_balance_audit::replay::TargetAccountState {
                address: key(1),
                generation: 1,
                lifecycle: AccountLifecycle::Open {
                    owner: key(2),
                    amount: 5,
                },
            }],
        )
        .unwrap();
        let registry = [key(1), key(2), key(3), key(4)].concat();
        let target_by_registry_id = [u32::MAX, 0, u32::MAX, u32::MAX, u32::MAX];
        // Message accounts are source, destination, authority, then a possible
        // multisig signer. The compact instruction list indexes this slice.
        let resolved_accounts = [1_u32, 3, 2, 4];
        let transfer = [3_u8, 1, 0, 0, 0, 0, 0, 0, 0];

        let mut stage = HolderAuthorityStage::new(4).unwrap();
        observe_direct_depth_2_owner_authority(
            &mut stage,
            TokenProgram::Token2022,
            &transfer,
            &[0, 1, 2],
            &resolved_accounts,
            &target_by_registry_id,
            &reducer,
            &registry,
            4,
        )
        .unwrap();
        let evidence = stage.program_attribution.get(&2).copied().unwrap();
        assert_eq!(evidence.attributed_program(), Some(4));
        assert_eq!(evidence.direct_cpi_authorizations, 1);

        let mut multisig_stage = HolderAuthorityStage::new(4).unwrap();
        observe_direct_depth_2_owner_authority(
            &mut multisig_stage,
            TokenProgram::Token2022,
            &transfer,
            &[0, 1, 2, 3],
            &resolved_accounts,
            &target_by_registry_id,
            &reducer,
            &registry,
            4,
        )
        .unwrap();
        assert!(multisig_stage.program_attribution.is_empty());
        assert_eq!(
            token_owner_authority_layout(TokenProgram::Token2022, &transfer, 4),
            None
        );
    }

    #[test]
    fn complete_off_curve_unattributed_holders_are_not_limited_to_the_top_25() {
        let mut owners = Vec::<[u8; 32]>::new();
        let mut seed = 1u64;
        while owners.len() < 30 {
            let mut candidate = [0u8; 32];
            candidate[..8].copy_from_slice(&seed.to_le_bytes());
            candidate[8..16].copy_from_slice(&seed.rotate_left(17).to_le_bytes());
            if !SolanaPubkey::new_from_array(candidate).is_on_curve()
                && !owners.contains(&candidate)
            {
                owners.push(candidate);
            }
            seed = seed.checked_add(1).unwrap();
        }
        let mut registry_keys = owners.clone();
        registry_keys.sort_unstable();
        let registry = registry_keys.concat();
        let state =
            |address, owner, amount| blockzilla_token_balance_audit::replay::TargetAccountState {
                address,
                generation: 1,
                lifecycle: AccountLifecycle::Open { owner, amount },
            };
        let mut states = owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                state(
                    key(u8::try_from(100 + index).unwrap()),
                    *owner,
                    u64::try_from(index + 1).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        states.push(state(key(200), owners[0], 31));
        let reducer = TargetBalanceReducer::from_states(
            TargetMintConfig {
                mint: key(250),
                program: TokenProgram::Token2022,
                decimals: SPYX_DECIMALS,
                native: false,
                initialized: true,
                transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
            },
            states,
        )
        .unwrap();
        let stage = HolderAuthorityStage::new(u32::try_from(owners.len()).unwrap()).unwrap();

        let report = build_holder_authority_report(&reducer, &registry, &stage, true).unwrap();
        let class_total = report
            .class_totals
            .iter()
            .find(|row| row.authority_kind == "off_curve_unattributed")
            .unwrap();

        assert_eq!(report.largest_25_by_class.off_curve_unattributed.len(), 25);
        assert_eq!(report.off_curve_unattributed_holders.len(), 30);
        assert_eq!(
            u64::try_from(report.off_curve_unattributed_holders.len()).unwrap(),
            class_total.holder_count
        );
        assert_eq!(
            report
                .off_curve_unattributed_holders
                .iter()
                .map(|holder| holder.token_account_count)
                .sum::<u64>(),
            class_total.token_account_count
        );
        assert_eq!(
            report
                .off_curve_unattributed_holders
                .iter()
                .map(|holder| holder.public_balance.raw_amount.parse::<u128>().unwrap())
                .sum::<u128>(),
            class_total
                .public_balance
                .raw_amount
                .parse::<u128>()
                .unwrap()
        );
        assert!(report.off_curve_unattributed_holders.iter().all(|holder| {
            holder.authority_kind == "off_curve_unattributed"
                && holder.pda_program_id.is_none()
                && holder.runtime_account_owner.is_none()
        }));
    }

    #[test]
    fn holder_authority_classes_partition_the_exact_final_state() {
        fn find_curve_key(mut seed: u64, on_curve: bool, used: &[[u8; 32]]) -> [u8; 32] {
            loop {
                let mut candidate = [0_u8; 32];
                candidate[..8].copy_from_slice(&seed.to_le_bytes());
                candidate[8..16].copy_from_slice(&seed.rotate_left(17).to_le_bytes());
                if SolanaPubkey::new_from_array(candidate).is_on_curve() == on_curve
                    && !used.contains(&candidate)
                {
                    return candidate;
                }
                seed = seed.checked_add(1).unwrap();
            }
        }

        let signer = find_curve_key(1, true, &[]);
        let on_curve_unobserved = find_curve_key(2, true, &[signer]);
        let attributed_pda = find_curve_key(3, false, &[signer, on_curve_unobserved]);
        let off_curve_unattributed =
            find_curve_key(4, false, &[signer, on_curve_unobserved, attributed_pda]);
        let program = key(240);
        let mut registry_keys = vec![
            signer,
            on_curve_unobserved,
            attributed_pda,
            off_curve_unattributed,
            program,
        ];
        registry_keys.sort_unstable();
        registry_keys.dedup();
        assert_eq!(registry_keys.len(), 5);
        let registry = registry_keys.concat();
        let registry_entries = u32::try_from(registry_keys.len()).unwrap();
        let id = |value: [u8; 32]| registry_id_for_key(&registry, &value).unwrap();

        let state =
            |address, owner, amount| blockzilla_token_balance_audit::replay::TargetAccountState {
                address,
                generation: 1,
                lifecycle: AccountLifecycle::Open { owner, amount },
            };
        let reducer = TargetBalanceReducer::from_states(
            TargetMintConfig {
                mint: key(250),
                program: TokenProgram::Token2022,
                decimals: SPYX_DECIMALS,
                native: false,
                initialized: true,
                transfer_fee_knowledge: TransferFeeKnowledge::KnownAbsent,
            },
            vec![
                state(key(101), signer, 100),
                state(key(102), attributed_pda, 200),
                state(key(103), off_curve_unattributed, 300),
                state(key(104), on_curve_unobserved, 400),
            ],
        )
        .unwrap();
        let mut stage = HolderAuthorityStage::new(registry_entries).unwrap();
        stage.observe_signers(&[id(signer)]).unwrap();
        stage
            .observe_parser_authority(id(attributed_pda), id(program))
            .unwrap();
        stage
            .observe_runtime_account_owner(
                id(off_curve_unattributed),
                program,
                RuntimeOwnerObservationLocation {
                    transaction_id: 12,
                    outer_index: 3,
                    inner_index: Some(1),
                    source_epoch: 900,
                    slot: 123,
                    source_block_id: 5,
                    tx_index: 7,
                },
            )
            .unwrap();

        let report = build_holder_authority_report(&reducer, &registry, &stage, true).unwrap();
        assert_eq!(SPYX_REPLAY_REPORT_SCHEMA_VERSION, 5);
        assert!(report.complete);
        assert_eq!(report.class_totals.len(), 4);
        assert_eq!(report.largest_25_all.len(), 4);
        assert_eq!(
            report
                .class_totals
                .iter()
                .map(|row| row.holder_count)
                .sum::<u64>(),
            4
        );
        assert_eq!(
            report
                .class_totals
                .iter()
                .map(|row| row.token_account_count)
                .sum::<u64>(),
            4
        );
        assert_eq!(
            report
                .class_totals
                .iter()
                .map(|row| row.public_balance.raw_amount.parse::<u128>().unwrap())
                .sum::<u128>(),
            1_000
        );
        let expected = [
            ("observed_transaction_signer", "100"),
            ("attributed_program_derived_address", "200"),
            ("off_curve_unattributed", "300"),
            ("unclassified_on_curve", "400"),
        ];
        for (row, (kind, amount)) in report.class_totals.iter().zip(expected) {
            assert_eq!(row.authority_kind, kind);
            assert_eq!(row.holder_count, 1);
            assert_eq!(row.token_account_count, 1);
            assert_eq!(row.public_balance.raw_amount, amount);
        }
        assert_eq!(report.holdings_by_program.len(), 1);
        assert_eq!(report.holdings_by_program[0].pda_holder_count, 1);
        assert_eq!(report.holdings_by_program[0].token_account_count, 1);
        assert_eq!(
            report.holdings_by_program[0].public_balance.raw_amount,
            "200"
        );
        assert_eq!(
            report.largest_25_by_class.observed_transaction_signer.len(),
            1
        );
        assert_eq!(
            report
                .largest_25_by_class
                .attributed_program_derived_address
                .len(),
            1
        );
        assert_eq!(report.largest_25_by_class.off_curve_unattributed.len(), 1);
        assert_eq!(report.largest_25_by_class.unclassified_on_curve.len(), 1);
        assert_eq!(report.off_curve_unattributed_holders.len(), 1);
        let runtime_owner = report.off_curve_unattributed_holders[0]
            .runtime_account_owner
            .as_ref()
            .unwrap();
        assert_eq!(runtime_owner.source, "committed_system_owner_instruction");
        assert_eq!(
            runtime_owner.program_id,
            bs58::encode(program).into_string()
        );
        assert_eq!(runtime_owner.observation_count, 1);
        assert_eq!(runtime_owner.owner_change_count, 0);
        assert_eq!(runtime_owner.conflict_count, 0);
        assert!(!runtime_owner.proves_pda_derivation);
        assert_eq!(runtime_owner.last_observation.transaction_id, 12);
        assert_eq!(runtime_owner.last_observation.inner_index, Some(1));
    }
}
