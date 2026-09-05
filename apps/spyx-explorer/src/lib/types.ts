export interface Amount {
  raw_amount: string;
  base_units: string;
}

export interface Concentration {
  amount: Amount;
  supply_fraction_numerator_raw: string;
  supply_fraction_denominator_raw: string;
  supply_share_parts_per_million_floor: number;
}

export interface PublicBalanceOwner {
  owner: string;
  token_account_count: number;
  public_balance: Amount;
}

export type HolderAuthorityKind =
  | 'observed_transaction_signer'
  | 'attributed_program_derived_address'
  | 'off_curve_unattributed'
  | 'unclassified_on_curve';

export type RuntimeOwnerAttributionStatus =
  | 'attributed_custom_program_runtime_owner'
  | 'not_attributed_account_missing'
  | 'not_attributed_system_program'
  | 'not_attributed_token_program'
  | 'not_attributed_executable_account';

export interface PublicAccountLabelEvidence {
  kind: 'public_explorer_label';
  source_name: string;
  source_url: string;
}

export interface SupplementalProgramAttribution {
  evidence_kind: 'solana_runtime_account_owner';
  snapshot_slot: number;
  account_exists: boolean;
  runtime_owner_program_id: string | null;
  runtime_owner_program_name: string | null;
  data_bytes: number | null;
  executable: boolean | null;
  attribution_status: RuntimeOwnerAttributionStatus;
  proves_pda_derivation: false;
  account_label?: string;
  account_label_evidence?: PublicAccountLabelEvidence;
}

export interface CommittedRuntimeAccountOwnerEvidence {
  source: 'committed_system_owner_instruction';
  program_id: string;
  program_name?: string | null;
  observation_count: number;
  owner_change_count: number;
  conflict_count: number;
  proves_pda_derivation: false;
  last_observation: {
    transaction_id: number;
    outer_index: number;
    inner_index?: number;
    source_epoch: number;
    slot: number;
    source_block_id: number;
    tx_index: number;
  };
}

export interface ClassifiedPublicBalanceOwner extends PublicBalanceOwner {
  authority_kind: HolderAuthorityKind;
  classification_evidence: string;
  signer_transaction_count: number;
  pda_program_id: string | null;
  pda_program_name?: string | null;
  pda_program_evidence_count: number;
  public_activity_volume?: Amount;
  activity_transaction_count?: number;
  public_balance_increase?: Amount;
  public_balance_decrease?: Amount;
  runtime_account_owner?: CommittedRuntimeAccountOwnerEvidence;
  supplemental_program_attribution?: SupplementalProgramAttribution;
}

export interface HolderAuthorityClassTotal {
  authority_kind: HolderAuthorityKind;
  holder_count: number;
  token_account_count: number;
  public_balance: Amount;
}

export interface ProgramHolding {
  program_id: string;
  program_name?: string | null;
  pda_holder_count: number;
  token_account_count: number;
  public_balance: Amount;
  public_activity_volume?: Amount;
  public_balance_increase?: Amount;
  public_balance_decrease?: Amount;
  owner_activity_transaction_links?: number;
}

export interface SupplementalAttributionTotals {
  holder_count: number;
  token_account_count: number;
  public_balance: Amount;
  public_activity_volume?: Amount;
  public_balance_increase?: Amount;
  public_balance_decrease?: Amount;
  owner_activity_transaction_links?: number;
}

export interface RuntimeOwnerProgramHolding extends SupplementalAttributionTotals {
  program_id: string;
  program_name: string | null;
}

export interface HolderAuthorityAttributionSupplement {
  schema_version: 1;
  artifact_kind: 'spyx_holder_authority_runtime_owner_snapshot';
  source_report_sha256: string;
  source_replay_sha256: string;
  program_source_report_sha256: string;
  evidence_kind: 'solana_runtime_account_owner';
  cluster: 'mainnet-beta';
  rpc_method: 'getMultipleAccounts';
  rpc_endpoint: string;
  snapshot_slot: number;
  snapshot_slot_min: number;
  snapshot_slot_max: number;
  selection_scope:
    | 'exposed_off_curve_unattributed_holder_rows'
    | 'all_off_curve_unattributed_holders';
  selection: string;
  coverage: {
    complete_for_all_off_curve_unattributed_holders: boolean;
    replay_off_curve_unattributed_holder_count: number;
    queried_holder_count: number;
    unqueried_holder_count: number;
    observed_holder_count: number;
    unobserved_holder_count: number;
  };
  counts: {
    accounts: number;
    present_accounts: number;
    absent_accounts: number;
    runtime_owner_programs: number;
    attributed_custom_program_runtime_owner: number;
    not_attributed_account_missing: number;
    not_attributed_system_program: number;
    not_attributed_token_program: number;
    not_attributed_executable_account: number;
  };
  definitions: Record<string, string>;
  totals: {
    observed: SupplementalAttributionTotals;
    attributed_custom_program: SupplementalAttributionTotals;
    not_attributed: SupplementalAttributionTotals;
  };
  holders: ClassifiedPublicBalanceOwner[];
  holdings_by_program: RuntimeOwnerProgramHolding[];
}

export interface HolderAuthorityReport {
  complete: boolean;
  definitions: Record<string, string>;
  class_totals: HolderAuthorityClassTotal[];
  largest_25_all: ClassifiedPublicBalanceOwner[];
  largest_25_by_class: Record<HolderAuthorityKind, ClassifiedPublicBalanceOwner[]>;
  largest_25_by_activity_all?: ClassifiedPublicBalanceOwner[];
  largest_25_by_activity_by_class?: Record<HolderAuthorityKind, ClassifiedPublicBalanceOwner[]>;
  attributed_program_holders?: ClassifiedPublicBalanceOwner[];
  holdings_by_program: ProgramHolding[];
  off_curve_unattributed_holders?: ClassifiedPublicBalanceOwner[];
  attribution_supplements?: HolderAuthorityAttributionSupplement[];
}

export interface BalanceDistributionRow {
  base_unit_range: string;
  holder_count: number;
  public_balance: Amount;
}

export interface DailyRow {
  utc_date: string;
  selected_transactions: number;
  public_balance_changing_transactions: number;
  public_owner_reassignment_transactions: number;
  positive_public_balance_holders: number;
  active_public_token_accounts: number;
  public_raw_balance_sum: Amount;
  public_bilateral_movement: Amount;
  inferred_public_mint: Amount;
  inferred_public_burn: Amount;
  top_1_concentration: Concentration;
  top_10_concentration: Concentration;
  top_100_concentration: Concentration;
}

export interface FinalTopHolderHistoryDay {
  utc_date: string;
  complete_utc_day: boolean;
  source_boundary_start: boolean;
  source_boundary_end: boolean;
  observed_selected_transaction_day: boolean;
  balance_state_carried_forward: boolean;
}

export interface FinalTopHolderHistorySeries {
  final_rank: number;
  owner: string;
  final_raw_balance: string;
  daily_raw_balances: string[];
}

export interface FinalTopHolderHistory {
  source_binding: {
    mint: string;
    mint_slot: number;
    first_epoch: number;
    last_epoch: number;
    manifest_sha256: string;
    transactions_sha256: string;
    signatures_sha256: string;
    registry_sha256: string;
    accounts_sha256: string;
  };
  cohort: {
    selection_boundary: 'final_public_balance_at_dump_boundary';
    maximum_holders: 100;
    selected_holders: number;
    ranking: 'positive_public_raw_balance_descending';
    tie_break: 'raw_32_byte_owner_pubkey_ascending';
  };
  definitions: {
    cohort: string;
    daily_boundary: string;
    calendar_dates: string;
    source_boundary: string;
    complete_utc_day: string;
    balance_state_carried_forward: string;
    raw_balance: string;
  };
  days: FinalTopHolderHistoryDay[];
  series: FinalTopHolderHistorySeries[];
}

export type PortfolioAccountKind =
  | 'observed_transaction_signer'
  | 'other_on_curve_account';

export interface AuthorityPortfolioProgram {
  program_id: string;
  program_name?: string | null;
  program_id_evidence?: 'replay_program_id' | 'supplemental_runtime_account_owner';
}

export interface AuthorityCandidateFlowEvidence {
  transaction_id: number;
  slot: number;
  block_time?: number;
  direction: 'deposit' | 'return';
  raw_amount: string;
  matched_principal_raw_amount?: string;
}

export interface AuthorityPortfolioClaimComponent {
  custody_owner: string;
  program_id: string | null;
  program_name?: string | null;
  program_id_evidence?: 'replay_program_id' | 'supplemental_runtime_account_owner';
  observed_deposited_principal: Amount;
  observed_returned_principal: Amount;
  candidate_net_principal: Amount;
  attributed_claim: Amount;
  deposit_transaction_count: number;
  return_transaction_count: number;
  candidate_flow_evidence: AuthorityCandidateFlowEvidence[];
  confidence: 'heuristic_owner_net_flow_capped_by_current_custody';
}

export interface AuthorityPortfolio {
  authority: string;
  authority_kind: PortfolioAccountKind;
  direct_public_balance: Amount;
  estimated_defi_claim: Amount;
  estimated_total_exposure: Amount;
  programs_used: AuthorityPortfolioProgram[];
  claim_components: AuthorityPortfolioClaimComponent[];
}

export interface AuthorityPortfolioTableRow {
  authority: string;
  authority_kind: PortfolioAccountKind;
  direct_public_balance: Amount;
  estimated_defi_claim: Amount;
  estimated_total_exposure: Amount;
  programs_used: AuthorityPortfolioProgram[];
  custody_owners: string[];
}

export interface AuthorityPortfolioHistoryPoint {
  transaction_id: number;
  slot: number;
  block_time: number | null;
  direct_public_balance: Amount;
  estimated_defi_claim: Amount;
  estimated_total_exposure: Amount;
}

export interface AuthorityPortfolioHistorySeries {
  authority: string;
  points: AuthorityPortfolioHistoryPoint[];
}

export interface AuthorityPortfolioHistoryReport {
  schema_version: 2;
  artifact_kind: 'spyx_authority_portfolio_history';
  source_binding: AuthorityPortfolioReport['source_binding'];
  coverage: {
    complete: true;
    method: 'forward_replay_216000_slot_window_end_sparse_aggregate_tuple_v2';
    slot_window_width: 216000;
    transactions_scanned: number;
    state_samples: number;
    authority_series: number;
    history_points: number;
    final_sample_matches_current_portfolio: true;
    definitions: {
      sampling: string;
      estimated_defi_claim: string;
      direct_public_balance: string;
    };
  };
  point_fields: [
    'transaction_id',
    'slot',
    'block_time',
    'direct_public_balance_raw',
    'estimated_defi_claim_raw'
  ];
  series: Array<{
    authority: string;
    points: Array<[number, number, number | null, string, string]>;
  }>;
}

export interface ProtocolCustodyAllocation {
  custody_owner: string;
  program_id: string | null;
  program_name?: string | null;
  program_id_evidence?: 'replay_program_id' | 'supplemental_runtime_account_owner';
  direct_custody_balance: Amount;
  candidate_net_principal: Amount;
  attributed_claim: Amount;
  unallocated_custody: Amount;
  claim_excess: Amount;
  candidate_authority_count: number;
  confidence: 'heuristic_owner_net_flow_capped_by_current_custody';
}

export interface PdaCreationProvenance {
  subject_pda: string;
  event_kind: 'account_creation' | 'owner_assignment';
  system_instruction: string;
  runtime_owner_program_id: string;
  direct_caller_program_id: string | null;
  create_with_seed_base: string | null;
  signer_candidates: string[];
  confidence: 'provenance_only_no_amount_assigned';
  proves_beneficial_ownership: false;
  location: {
    transaction_id: number;
    source_epoch: number;
    slot: number;
    source_block_id: number;
    tx_index: number;
    outer_index: number;
    inner_index?: number;
  };
}

export type PdaAuthorityEstimateResolution =
  | 'single_unique_creation_signer_candidate'
  | 'ambiguous_creation_signer_candidates'
  | 'shared_creation_signer_candidate'
  | 'candidate_portfolio_unavailable'
  | 'no_creation_signer_candidate';

export interface PdaAuthorityProgramPosition {
  program_id: string | null;
  program_name: string | null;
  program_id_evidence?: 'replay_program_id' | 'supplemental_runtime_account_owner';
  custody_owners: string[];
  custody_owner_count: number;
  observed_deposited_principal: Amount;
  observed_returned_principal: Amount;
  candidate_net_principal: Amount;
  estimated_claim: Amount;
  deposit_transaction_count: number;
  return_transaction_count: number;
}

export interface PdaAuthorityCandidate {
  authority: string;
  authority_kind: PortfolioAccountKind | null;
  portfolio_available: boolean;
  linked_subject_pda_count: number;
  estimated_external_defi_claim: Amount;
  programs_used: AuthorityPortfolioProgram[];
  program_positions: PdaAuthorityProgramPosition[];
}

export interface PdaAuthorityEstimate {
  subject_pda: string;
  runtime_owner_program_id: string;
  runtime_owner_program_name: string | null;
  direct_caller_program_id: string | null;
  direct_caller_program_name: string | null;
  system_instruction: string;
  create_with_seed_base: string | null;
  creation_event_count: number;
  creation_location: PdaCreationProvenance['location'];
  signer_candidates: string[];
  candidates: PdaAuthorityCandidate[];
  direct_public_balance: Amount;
  selected_candidate_authority: string | null;
  estimated_external_defi_claim: Amount | null;
  estimated_total_exposure: Amount | null;
  resolution: PdaAuthorityEstimateResolution;
  confidence: 'heuristic_pda_creation_signer_external_claims';
  proves_beneficial_ownership: false;
  additive_to_authority_totals: false;
}

export interface PdaAuthorityEstimateSummary {
  schema_version: 1;
  method: 'committed_pda_creation_signer_external_claims_v1';
  subject_count: number;
  selected_subject_count: number;
  proves_beneficial_ownership: false;
  additive_to_authority_totals: false;
}

export interface PdaAuthorityEstimateReport {
  schema_version: 1;
  artifact_kind: 'spyx_pda_authority_estimates';
  source_binding: AuthorityPortfolioReport['source_binding'];
  summary: PdaAuthorityEstimateSummary;
  estimates: PdaAuthorityEstimate[];
}

export interface PdaFlowProofAccount {
  address: string;
  role: string;
  label: string;
}

export interface PdaFlowProofTransfer {
  transaction_id: number;
  signature: string;
  slot: number;
  block_time_unix_seconds: number;
  direction: string;
  amount: Amount;
  source_token_account: string;
  destination_token_account: string;
  authority: string;
  invoked_program_id: string;
  note: string;
}

export interface PdaFlowProofPositionObservation {
  observed_at_utc: string;
  source: 'Jupiter Portfolio API';
  position_url: string;
  subject_pda_position_found: false;
  position_owner: string;
  supplied_spyx: string;
  borrowed_usdc: string;
  position_state: string;
  position_nft_mint: string;
  vault: string;
}

export interface PdaFlowProof {
  subject_pda: string;
  owner_program_id: string;
  creation_signer: string;
  evidence_status: 'verified_two_way_fund_flow';
  accounts: PdaFlowProofAccount[];
  transfers: PdaFlowProofTransfer[];
  position_observation: PdaFlowProofPositionObservation;
  conclusion: string;
  proves_direct_pda_position: false;
}

export interface PdaFlowProofReport {
  schema_version: 1;
  artifact_kind: 'spyx_pda_flow_proofs';
  source_binding: {
    transactions_sha256: string;
  };
  proofs: PdaFlowProof[];
}

export interface AuthorityPortfolioReport {
  schema_version: 1;
  artifact_kind: 'spyx_authority_portfolio_heuristic';
  source_binding: {
    mint: string;
    first_epoch: number;
    last_epoch: number;
    manifest_sha256: string;
    transactions_sha256: string;
    registry_sha256: string;
    replay_state_sha256: string;
  };
  coverage: {
    complete: boolean;
    method: 'committed_non_dex_owner_net_flow_v1';
    candidate_flow_evidence_complete: boolean;
    transactions_scanned: number;
    parsed_dex_swap_transactions_excluded: number;
    candidate_deposit_transactions: number;
    candidate_return_transactions: number;
    ambiguous_owner_delta_transactions_excluded: number;
    current_positive_off_curve_custody_owners: number;
    definitions: {
      estimated_defi_claim: string;
      creation_provenance: string;
      unallocated_custody: string;
      candidate_flow_evidence?: string;
    };
  };
  portfolios: AuthorityPortfolio[];
  protocol_custody: ProtocolCustodyAllocation[];
  pda_creation_provenance: PdaCreationProvenance[];
  pda_authority_estimate_summary?: PdaAuthorityEstimateSummary;
  pda_authority_estimates?: PdaAuthorityEstimate[];
}

export interface AuthorityPortfolioTableReport {
  schema_version: 1;
  artifact_kind: 'spyx_authority_portfolio_table';
  source_binding: AuthorityPortfolioReport['source_binding'];
  coverage: {
    complete: boolean;
    candidate_flow_evidence_complete: boolean;
    transactions_scanned: number;
  };
  portfolios: AuthorityPortfolioTableRow[];
  protocol_custody: ProtocolCustodyAllocation[];
}

export interface MovementDay {
  utc_date: string;
  public_bilateral_movement: Amount;
  inferred_public_mint: Amount;
  inferred_public_burn: Amount;
  selected_transactions: number;
  public_balance_changing_transactions: number;
}

export interface MovementTransaction {
  first_signature: string;
  source_epoch: number;
  slot: number;
  source_block_id: number;
  tx_index: number;
  block_time_unix_seconds: number;
  utc_date: string;
  public_bilateral_movement: Amount;
  inferred_public_mint: Amount;
  inferred_public_burn: Amount;
}

export interface SourceFile {
  file: string;
  bytes: number;
  sha256: string;
}

export interface SpyxSource {
  mint: string;
  mint_slot: number;
  first_epoch: number;
  last_epoch: number;
  transactions: number;
  signatures: number;
  registry_entries: number;
  discovered_token_accounts: number;
  total_dump_bytes: number;
  manifest: SourceFile;
  transactions_file: SourceFile;
  signatures_file: SourceFile;
  registry_file: SourceFile;
  accounts_file: SourceFile;
}

export interface AuditCounters {
  transactions: number;
  signatures: number;
  transactions_with_target_balance_rows: number;
  public_balance_changing_transactions: number;
  public_owner_reassignment_transactions: number;
  target_pre_balance_rows: number;
  target_post_balance_rows: number;
  implicit_zero_pre_rows: number;
  implicit_zero_post_rows: number;
  target_balance_rows_without_owner: number;
  target_positive_states_without_owner: number;
  transactions_without_block_time: number;
  public_state_changes_without_block_time: number;
  metadata_absent: number;
  metadata_without_error: number;
  metadata_current_only: number;
  metadata_legacy_only: number;
  metadata_both_same_target_balance_resolution: number;
  address_signature_rows: number;
  selected_transactions_without_target_address: number;
}

export interface FinalPublicBalance {
  decimals: number;
  positive_public_balance_holders: number;
  active_public_token_accounts: number;
  public_raw_balance_sum: Amount;
  top_1_concentration: Concentration;
  top_10_concentration: Concentration;
  top_100_concentration: Concentration;
  largest_25_holders: PublicBalanceOwner[];
  smallest_25_positive_holders: PublicBalanceOwner[];
  balance_distribution: BalanceDistributionRow[];
  holder_authority?: HolderAuthorityReport;
}

export interface PublicMovementTotals {
  public_balance_changing_transactions: number;
  public_owner_reassignment_transactions: number;
  public_bilateral_movement: Amount;
  inferred_public_mint: Amount;
  inferred_public_burn: Amount;
}

export interface RpcRequestModel {
  scope: string;
  address_count: number;
  mint_addresses: number;
  token_account_addresses: number;
  get_signatures_for_address_page_limit: number;
  get_signatures_for_address_requests: number;
  get_signatures_for_address_credit_page_size: number;
  get_signatures_for_address_credit_pages: number;
  returned_address_signature_rows: number;
  duplicate_address_signature_rows_removed: number;
  unique_get_transaction_calls: number;
  total_rpc_requests: number;
}

export interface RpcProviderAccessPath {
  addresses_queried: number;
  selected_transactions_covered: number;
  selected_transactions_missed: number;
  complete_selected_dump_coverage: boolean;
  get_signatures_for_address_requests: number;
  get_transaction_requests: number;
  modeled_request_total: number;
}

export interface AllTargetCoveragePrerequisite {
  required: true;
  historical_token_account_list_must_preexist: true;
  includes_closed_accounts: true;
  discoverable_from_mint_only_rpc: false;
}

export interface AllTargetRpcProviderAccessPath extends RpcProviderAccessPath {
  coverage_prerequisite: AllTargetCoveragePrerequisite;
}

export interface VerifiedDumpAccessPath {
  selected_transactions_covered: number;
  selected_transactions_missed: number;
  complete_selected_dump_coverage: boolean;
  provider_rpc_requests: number;
  verified_source_files: number;
}

export interface ProviderAccessComparison {
  basis: string;
  mint_only: RpcProviderAccessPath;
  all_target_addresses: AllTargetRpcProviderAccessPath;
  existing_verified_dump_scan: VerifiedDumpAccessPath;
}

export interface ReplayFailure {
  source_epoch: number;
  slot: number;
  source_block_id: number;
  tx_index: number;
  phase: string;
  code: string;
  detail: string;
  outer_index: number | null;
  inner_index: number | null;
}

export interface ReplayedState {
  history_complete: boolean;
  tracked_accounts: number;
  open_accounts: number;
  closed_accounts: number;
  positive_public_balance_accounts: number;
  public_raw_balance: string;
  state_sha256: string;
}

export interface StrictReplaySummary {
  present: boolean;
  source_report_sha256?: string;
  schema_version?: number;
  artifact_kind?: string;
  bounded_selected_dump_scan_complete?: boolean;
  instruction_replay_implemented: boolean;
  instruction_replay_matches_metadata_for_complete_spyx_selected_history: boolean;
  proof_scope?: string;
  status: string;
  source?: Record<string, unknown>;
  replayed_state: ReplayedState | null;
  counters: Record<string, number>;
  instruction_names?: Record<string, number>;
  census_findings?: Record<string, number>;
  blockers: Record<string, number>;
  first_failure: ReplayFailure | null;
  elapsed_seconds?: number;
}

export interface SpyxReport {
  schema_version: number;
  artifact_kind: string;
  status: {
    bounded_selected_dump_scan_complete: boolean;
    metadata_balance_chain_continuous_from_spyx_mint_creation: boolean;
    instruction_replay_performed: boolean;
  };
  definitions: Record<string, string>;
  limitations: Record<string, string>;
  source: SpyxSource;
  audit: AuditCounters;
  final_public_balance: FinalPublicBalance;
  public_volume_totals: PublicMovementTotals;
  daily: DailyRow[];
  final_top_100_holder_history?: FinalTopHolderHistory;
  top_25_volume_days: MovementDay[];
  top_25_volume_transactions: MovementTransaction[];
  rpc_request_model: RpcRequestModel;
  provider_access_comparison: ProviderAccessComparison;
  strict_instruction_replay: StrictReplaySummary;
  compact_build: {
    source_report_sha256: string;
    holder_authority_supplement_sha256?: string;
    authority_portfolios_available?: boolean;
    authority_portfolio_table_available?: boolean;
    authority_portfolio_history_available?: boolean;
    omitted_rpc_per_address_rows: number;
    inserted_zero_activity_calendar_days: number;
  };
}

export interface ProgramRow {
  rank: number;
  registry_id: number;
  program_id: string;
  identity_status: string;
  selected_name: string | null;
  selected_source: string | null;
  selected_confidence: string | null;
  usable_onchain_idl: boolean;
  address_clean_onchain_idl: boolean;
  decoder_source_found: boolean;
  total_occurrences: number;
  outer_occurrences: number;
  inner_occurrences: number;
  transactions: number;
  target_account_inner_occurrences: number;
  target_account_inner_transactions: number;
  target_mint_inner_occurrences: number;
  target_token_account_inner_occurrences: number;
  target_account_inner_references: number;
  target_mint_inner_references: number;
  target_token_account_inner_references: number;
}

export interface ProgramTargetAccountCpiCounters {
  target_account_inner_occurrences: number;
  target_account_inner_transactions: number;
  target_mint_inner_occurrences: number;
  target_token_account_inner_occurrences: number;
  target_account_inner_references: number;
  target_mint_inner_references: number;
  target_token_account_inner_references: number;
  transactions_with_target_account_inner_instructions: number;
  programs_with_target_account_inner_instructions: number;
}

export interface ProgramTargetAccountCpiSummary {
  complete: true;
  source_report_sha256: string;
  target_accounts: {
    file: string;
    sha256: string;
    discovered_token_accounts: number;
    target_addresses: number;
    membership_definition: string;
  };
  counters: ProgramTargetAccountCpiCounters;
}

export interface ProgramCounters {
  transactions: number;
  programs_total: number;
  programs_identified: number;
  programs_unidentified: number;
  identified_program_ratio: number;
  programs_named_onchain: number;
  programs_added_by_public_sources: number;
  usable_onchain_idls: number;
  address_clean_onchain_idls: number;
  programs_with_any_decoder_source: number;
  decoder_source_program_ratio: number;
  instruction_occurrences_total: number;
  identified_instruction_occurrences: number;
  unidentified_instruction_occurrences: number;
  identified_instruction_occurrence_ratio: number;
  decoder_source_instruction_occurrences: number;
  decoder_source_instruction_occurrence_ratio: number;
  identified_outer_occurrences: number;
  identified_inner_occurrences: number;
  ignored_generic_or_empty_evidence: number;
  programs_explicitly_excluded_as_class_only: number;
}

export interface ProgramReport {
  schema_version: number;
  artifact_kind: string;
  complete: boolean;
  generated_at: string;
  definitions: Record<string, string>;
  source: {
    first_epoch: number;
    last_epoch: number;
    inventory_sha256: string;
    dump_manifest_sha256: string;
    dump_transaction_stream_sha256: string;
    dump_pubkey_registry_sha256: string;
  };
  counters: ProgramCounters;
  target_account_cpi: ProgramTargetAccountCpiSummary;
  source_match_counts: Record<string, number>;
  programs: ProgramRow[];
  compact_build: {
    source_report_sha256: string;
    target_account_cpi_source_report_sha256: string;
    evidence_arrays_omitted: boolean;
  };
}
