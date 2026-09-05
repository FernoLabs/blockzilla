import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import test, { after } from 'node:test';
import { fileURLToPath } from 'node:url';
import { deriveProviderAccessComparison } from './provider-access-model.mjs';

const appRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const repositoryRoot = resolve(appRoot, '../..');
const builder = resolve(appRoot, 'scripts/build-data.mjs');
const temporary = await mkdtemp(join(tmpdir(), 'spyx-explorer-contract-'));
const fixtureSha256 = new Map();
let outputSequence = 0;
let cpiFixtureSequence = 0;
after(() => rm(temporary, { recursive: true, force: true }));

const mint = 'XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W';
const digests = {
  manifest: '11'.repeat(32),
  transactions: '22'.repeat(32),
  signatures: '33'.repeat(32),
  registry: '44'.repeat(32),
  accounts: '55'.repeat(32)
};
const sourceFile = (file, sha256) => ({ file, bytes: 1, sha256 });
const amount = (rawAmount, baseUnits) => ({ raw_amount: rawAmount, base_units: baseUnits });
const concentration = (
  rawAmount,
  baseUnits,
  numerator = rawAmount,
  denominator = rawAmount,
  partsPerMillion = rawAmount === '0' ? 0 : 1_000_000
) => ({
  amount: amount(rawAmount, baseUnits),
  supply_fraction_numerator_raw: numerator,
  supply_fraction_denominator_raw: denominator,
  supply_share_parts_per_million_floor: partsPerMillion
});
const zeroAmount = amount('0', '0.00000000');
const zeroConcentration = concentration('0', '0.00000000');
const finalAmount = amount('123', '0.00000123');
const finalConcentration = concentration('123', '0.00000123');
const movementAmount = amount('5', '0.00000005');
const mintAmount = amount('2', '0.00000002');
const burnAmount = amount('1', '0.00000001');
const perAddress = [
  ...Array.from({ length: 15 }, (_, index) => ({
    address: `token-account-${index}`,
    kind: 'token_account',
    returned_address_signature_rows: 1,
    get_signatures_for_address_requests_at_limit_1000: 1,
    get_signatures_for_address_credit_pages_at_100: 1
  })),
  {
    address: mint,
    kind: 'mint',
    returned_address_signature_rows: 10,
    get_signatures_for_address_requests_at_limit_1000: 1,
    get_signatures_for_address_credit_pages_at_100: 1
  }
];
const history = {
  schema_version: 1,
  artifact_kind: 'token_public_balance_history',
  bounded_selected_dump_scan_complete: true,
  metadata_balance_chain_continuous_from_spyx_mint_creation: true,
  daily_public_balance_series_complete: true,
  daily_selected_transaction_counts_complete: true,
  instruction_replay_performed: false,
  definitions: { holder: 'test holder definition' },
  limitations: { public_scope: 'test public metadata limit' },
  source: {
    mint,
    mint_slot: 346_066_298,
    first_epoch: 801,
    last_epoch: 1018,
    transactions: 12,
    signatures: 13,
    registry_entries: 14,
    discovered_token_accounts: 15,
    total_dump_bytes: 5,
    manifest: sourceFile('manifest.json', digests.manifest),
    transactions_file: sourceFile('transactions.wincode', digests.transactions),
    signatures_file: sourceFile('signatures.bin', digests.signatures),
    registry_file: sourceFile('registry.bin', digests.registry),
    accounts_file: sourceFile('accounts.wincode', digests.accounts)
  },
  audit: {
    transactions: 12,
    signatures: 13,
    transactions_with_target_balance_rows: 11,
    public_balance_changing_transactions: 7,
    public_owner_reassignment_transactions: 1,
    target_pre_balance_rows: 20,
    target_post_balance_rows: 21,
    implicit_zero_pre_rows: 1,
    implicit_zero_post_rows: 2,
    target_balance_rows_without_owner: 0,
    target_positive_states_without_owner: 0,
    transactions_without_block_time: 0,
    public_state_changes_without_block_time: 0,
    metadata_absent: 0,
    metadata_without_error: 8,
    metadata_current_only: 4,
    metadata_legacy_only: 0,
    metadata_both_same_target_balance_resolution: 0,
    address_signature_rows: 25,
    selected_transactions_without_target_address: 0
  },
  final_public_balance: {
    decimals: 8,
    positive_public_balance_holders: 3,
    active_public_token_accounts: 4,
    public_raw_balance_sum: finalAmount,
    top_1_concentration: finalConcentration,
    top_10_concentration: finalConcentration,
    top_100_concentration: finalConcentration,
    largest_25_holders: [
      { owner: 'owner-a', token_account_count: 2, public_balance: amount('100', '0.00000100') },
      { owner: 'owner-b', token_account_count: 1, public_balance: amount('20', '0.00000020') },
      { owner: 'owner-c', token_account_count: 1, public_balance: amount('3', '0.00000003') }
    ],
    smallest_25_positive_holders: [
      { owner: 'owner-c', token_account_count: 1, public_balance: amount('3', '0.00000003') },
      { owner: 'owner-b', token_account_count: 1, public_balance: amount('20', '0.00000020') },
      { owner: 'owner-a', token_account_count: 2, public_balance: amount('100', '0.00000100') }
    ],
    balance_distribution: [
      { base_unit_range: 'test_range', holder_count: 3, public_balance: finalAmount }
    ]
  },
  public_volume_totals: {
    public_balance_changing_transactions: 7,
    public_owner_reassignment_transactions: 1,
    public_bilateral_movement: movementAmount,
    inferred_public_mint: mintAmount,
    inferred_public_burn: burnAmount
  },
  daily: [
    {
      utc_date: '2026-08-18',
      selected_transactions: 5,
      public_balance_changing_transactions: 2,
      public_owner_reassignment_transactions: 0,
      positive_public_balance_holders: 0,
      active_public_token_accounts: 0,
      public_raw_balance_sum: zeroAmount,
      public_bilateral_movement: zeroAmount,
      inferred_public_mint: zeroAmount,
      inferred_public_burn: zeroAmount,
      top_1_concentration: zeroConcentration,
      top_10_concentration: zeroConcentration,
      top_100_concentration: zeroConcentration
    },
    {
      utc_date: '2026-08-19',
      selected_transactions: 7,
      public_balance_changing_transactions: 5,
      public_owner_reassignment_transactions: 1,
      positive_public_balance_holders: 3,
      active_public_token_accounts: 4,
      public_raw_balance_sum: finalAmount,
      public_bilateral_movement: movementAmount,
      inferred_public_mint: mintAmount,
      inferred_public_burn: burnAmount,
      top_1_concentration: finalConcentration,
      top_10_concentration: finalConcentration,
      top_100_concentration: finalConcentration
    }
  ],
  top_25_volume_days: [
    {
      utc_date: '2026-08-19',
      public_bilateral_movement: movementAmount,
      inferred_public_mint: mintAmount,
      inferred_public_burn: burnAmount,
      selected_transactions: 7,
      public_balance_changing_transactions: 5
    }
  ],
  top_25_volume_transactions: [
    {
      first_signature: '1'.repeat(88),
      source_epoch: 801,
      slot: 346_066_299,
      source_block_id: 1,
      tx_index: 0,
      block_time_unix_seconds: Date.parse('2026-08-19T12:00:00Z') / 1_000,
      utc_date: '2026-08-19',
      public_bilateral_movement: movementAmount,
      inferred_public_mint: mintAmount,
      inferred_public_burn: burnAmount
    }
  ],
  rpc_request_model: {
    scope: 'test selected dump',
    address_count: 16,
    mint_addresses: 1,
    token_account_addresses: 15,
    get_signatures_for_address_page_limit: 1_000,
    get_signatures_for_address_requests: 16,
    get_signatures_for_address_credit_page_size: 100,
    get_signatures_for_address_credit_pages: 16,
    returned_address_signature_rows: 25,
    duplicate_address_signature_rows_removed: 13,
    unique_get_transaction_calls: 12,
    total_rpc_requests: 28,
    per_address: perAddress
  }
};

const finalTopHistoryOwners = [
  mint,
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
  '11111111111111111111111111111111'
];

function historyWithFinalTopHolderHistory() {
  const value = structuredClone(history);
  value.final_public_balance.largest_25_holders.forEach((holder, index) => {
    holder.owner = finalTopHistoryOwners[index];
  });
  value.final_public_balance.smallest_25_positive_holders.forEach((holder, index) => {
    holder.owner = finalTopHistoryOwners.at(-index - 1);
  });
  value.final_top_100_holder_history = {
    source_binding: {
      mint,
      mint_slot: value.source.mint_slot,
      first_epoch: value.source.first_epoch,
      last_epoch: value.source.last_epoch,
      manifest_sha256: digests.manifest,
      transactions_sha256: digests.transactions,
      signatures_sha256: digests.signatures,
      registry_sha256: digests.registry,
      accounts_sha256: digests.accounts
    },
    cohort: {
      selection_boundary: 'final_public_balance_at_dump_boundary',
      maximum_holders: 100,
      selected_holders: 3,
      ranking: 'positive_public_raw_balance_descending',
      tie_break: 'raw_32_byte_owner_pubkey_ascending'
    },
    definitions: {
      cohort: 'One fixed final cohort.',
      daily_boundary: 'End of selected UTC day.',
      calendar_dates: 'All UTC dates in the source range.',
      source_boundary: 'First and last dated source days.',
      complete_utc_day: 'True only between source boundary days.',
      balance_state_carried_forward: 'True on dates without a selected transaction.',
      raw_balance: 'Exact public raw token amount.'
    },
    days: [
      {
        utc_date: '2026-08-18',
        complete_utc_day: false,
        source_boundary_start: true,
        source_boundary_end: false,
        observed_selected_transaction_day: true,
        balance_state_carried_forward: false
      },
      {
        utc_date: '2026-08-19',
        complete_utc_day: false,
        source_boundary_start: false,
        source_boundary_end: true,
        observed_selected_transaction_day: true,
        balance_state_carried_forward: false
      }
    ],
    series: [
      {
        final_rank: 1,
        owner: finalTopHistoryOwners[0],
        final_raw_balance: '100',
        daily_raw_balances: ['0', '100']
      },
      {
        final_rank: 2,
        owner: finalTopHistoryOwners[1],
        final_raw_balance: '20',
        daily_raw_balances: ['0', '20']
      },
      {
        final_rank: 3,
        owner: finalTopHistoryOwners[2],
        final_raw_balance: '3',
        daily_raw_balances: ['0', '3']
      }
    ]
  };
  return value;
}

const replay = {
  schema_version: 1,
  artifact_kind: 'spyx_public_balance_instruction_replay',
  bounded_selected_dump_scan_complete: false,
  instruction_replay_implemented: true,
  instruction_replay_matches_metadata_for_complete_spyx_selected_history: false,
  proof_scope: 'test',
  status: 'canary_prefix_only',
  source: {
    mint,
    mint_slot: history.source.mint_slot,
    first_epoch: history.source.first_epoch,
    last_epoch: history.source.last_epoch,
    manifest_sha256: digests.manifest,
    expected_transaction_sha256: digests.transactions,
    observed_transaction_sha256: null,
    registry_sha256: digests.registry,
    accounts_sha256: digests.accounts,
    manifest_transactions: history.source.transactions,
    discovered_token_accounts: history.source.discovered_token_accounts
  },
  replayed_state: {
    history_complete: false,
    tracked_accounts: 15,
    open_accounts: 5,
    closed_accounts: 10,
    positive_public_balance_accounts: 4,
    public_raw_balance: '123',
    state_sha256: 'aa'.repeat(32)
  },
  counters: {},
  blockers: {},
  first_failure: null,
  elapsed_seconds: 1
};
const programs = {
  schema_version: 1,
  artifact_kind: 'spyx_program_identification',
  complete: true,
  generated_at: '2026-08-29T00:00:00Z',
  definitions: {},
  source: {
    first_epoch: history.source.first_epoch,
    last_epoch: history.source.last_epoch,
    inventory_sha256: '66'.repeat(32),
    dump_manifest_sha256: digests.manifest,
    dump_transaction_stream_sha256: digests.transactions,
    dump_pubkey_registry_sha256: digests.registry
  },
  counters: {
    transactions: history.source.transactions,
    programs_total: 0,
    programs_identified: 0,
    programs_unidentified: 0,
    identified_program_ratio: 0,
    programs_named_onchain: 0,
    programs_added_by_public_sources: 0,
    usable_onchain_idls: 0,
    address_clean_onchain_idls: 0,
    programs_with_any_decoder_source: 0,
    decoder_source_program_ratio: 0,
    instruction_occurrences_total: 0,
    identified_instruction_occurrences: 0,
    unidentified_instruction_occurrences: 0,
    identified_instruction_occurrence_ratio: 0,
    decoder_source_instruction_occurrences: 0,
    decoder_source_instruction_occurrence_ratio: 0,
    identified_outer_occurrences: 0,
    identified_inner_occurrences: 0,
    ignored_generic_or_empty_evidence: 0,
    programs_explicitly_excluded_as_class_only: 0
  },
  source_match_counts: {},
  programs: []
};
const identifiedProgram = {
  rank: 1,
  registry_id: 42,
  program_id: 'fixture-program',
  identity_status: 'identified',
  selected_name: 'Fixture program',
  selected_source: 'test_public_source',
  selected_confidence: 'high',
  usable_onchain_idl: false,
  address_clean_onchain_idl: false,
  decoder_source_found: false,
  total_occurrences: 3,
  outer_occurrences: 1,
  inner_occurrences: 2,
  transactions: 2,
  evidence: [
    {
      source: 'test_public_source',
      name: 'Fixture program',
      confidence: 'high',
      decoder_source: false
    }
  ]
};
const populatedPrograms = {
  ...programs,
  counters: {
    ...programs.counters,
    programs_total: 1,
    programs_identified: 1,
    identified_program_ratio: 1,
    programs_added_by_public_sources: 1,
    instruction_occurrences_total: 3,
    identified_instruction_occurrences: 3,
    identified_instruction_occurrence_ratio: 1,
    identified_outer_occurrences: 1,
    identified_inner_occurrences: 2
  },
  source_match_counts: { test_public_source: 1 },
  programs: [identifiedProgram]
};

const historyPath = await fixture('history.json', history);
const replayPath = await fixture('replay.json', replay);
const programsPath = await fixture('programs.json', programs);
const populatedProgramsPath = await fixture('programs-populated.json', populatedPrograms);
const completeReplay = {
  ...replay,
  schema_version: 5,
  bounded_selected_dump_scan_complete: true,
  instruction_replay_matches_metadata_for_complete_spyx_selected_history: true,
  status: 'complete_match',
  source: {
    ...replay.source,
    observed_transaction_sha256: digests.transactions
  },
  replayed_state: {
    ...replay.replayed_state,
    history_complete: true
  },
  holder_authority: {
    complete: true,
    definitions: {
      observed_transaction_signer: 'Signed at least one indexed top-level transaction.',
      attributed_program_derived_address: 'Off-curve owner with one unambiguous program attribution.'
    },
    class_totals: [
      {
        authority_kind: 'observed_transaction_signer',
        holder_count: 1,
        token_account_count: 2,
        public_balance: amount('100', '0.00000100')
      },
      {
        authority_kind: 'attributed_program_derived_address',
        holder_count: 1,
        token_account_count: 1,
        public_balance: amount('20', '0.00000020')
      },
      {
        authority_kind: 'off_curve_unattributed',
        holder_count: 1,
        token_account_count: 1,
        public_balance: amount('3', '0.00000003')
      },
      {
        authority_kind: 'unclassified_on_curve',
        holder_count: 0,
        token_account_count: 0,
        public_balance: amount('0', '0.00000000')
      }
    ],
    largest_25_all: [
      {
        ...history.final_public_balance.largest_25_holders[0],
        authority_kind: 'observed_transaction_signer',
        classification_evidence: 'observed_top_level_signer',
        signer_transaction_count: 7,
        pda_program_id: null,
        pda_program_evidence_count: 0,
        activity_transaction_count: 4,
        public_balance_increase: amount('30', '0.00000030'),
        public_balance_decrease: amount('10', '0.00000010'),
        public_activity_volume: amount('40', '0.00000040')
      },
      {
        ...history.final_public_balance.largest_25_holders[1],
        authority_kind: 'attributed_program_derived_address',
        classification_evidence: 'direct_depth_2_cpi_authorization',
        signer_transaction_count: 0,
        pda_program_id: 'program-a',
        pda_program_evidence_count: 2,
        activity_transaction_count: 6,
        public_balance_increase: amount('50', '0.00000050'),
        public_balance_decrease: amount('10', '0.00000010'),
        public_activity_volume: amount('60', '0.00000060')
      },
      {
        ...history.final_public_balance.largest_25_holders[2],
        authority_kind: 'off_curve_unattributed',
        classification_evidence: 'off_curve_without_unambiguous_program_evidence',
        signer_transaction_count: 0,
        pda_program_id: null,
        pda_program_evidence_count: 0,
        activity_transaction_count: 2,
        public_balance_increase: amount('2', '0.00000002'),
        public_balance_decrease: amount('1', '0.00000001'),
        public_activity_volume: amount('3', '0.00000003')
      }
    ],
    largest_25_by_class: {},
    holdings_by_program: [
      {
        program_id: 'program-a',
        pda_holder_count: 1,
        token_account_count: 1,
        public_balance: amount('20', '0.00000020'),
        owner_activity_transaction_links: 6,
        public_balance_increase: amount('50', '0.00000050'),
        public_balance_decrease: amount('10', '0.00000010'),
        public_activity_volume: amount('60', '0.00000060')
      }
    ]
  },
  counters: {
    transactions_scanned: 12,
    successful_transactions: 8,
    failed_transactions: 4,
    transactions_with_target_oracle_rows: 11,
    pre_target_oracle_rows: 20,
    post_target_oracle_rows: 21,
    metadata_without_error: 8,
    metadata_current_only: 4,
    metadata_legacy_only: 0,
    metadata_both_identical: 0,
    replay_transactions_attempted: 12,
    replay_transactions_applied: 12,
    replay_clean_prefix_transactions: 12,
    replay_errors: 0,
    oracle_pre_rows_compared: 20,
    oracle_post_rows_compared: 21,
    oracle_pre_mismatches: 0,
    oracle_post_mismatches: 0
  }
};
completeReplay.holder_authority.largest_25_by_class = {
  observed_transaction_signer: [completeReplay.holder_authority.largest_25_all[0]],
  attributed_program_derived_address: [completeReplay.holder_authority.largest_25_all[1]],
  off_curve_unattributed: [completeReplay.holder_authority.largest_25_all[2]],
  unclassified_on_curve: []
};
completeReplay.holder_authority.largest_25_by_activity_all = [
  completeReplay.holder_authority.largest_25_all[1],
  completeReplay.holder_authority.largest_25_all[0],
  completeReplay.holder_authority.largest_25_all[2]
];
completeReplay.holder_authority.largest_25_by_activity_by_class = {
  observed_transaction_signer: [completeReplay.holder_authority.largest_25_all[0]],
  attributed_program_derived_address: [completeReplay.holder_authority.largest_25_all[1]],
  off_curve_unattributed: [completeReplay.holder_authority.largest_25_all[2]],
  unclassified_on_curve: []
};
completeReplay.holder_authority.attributed_program_holders = [
  completeReplay.holder_authority.largest_25_all[1]
];

function authorityPortfolioFixture() {
  return {
    schema_version: 1,
    artifact_kind: 'spyx_authority_portfolio_heuristic',
    source_binding: {
      mint,
      first_epoch: history.source.first_epoch,
      last_epoch: history.source.last_epoch,
      manifest_sha256: digests.manifest,
      transactions_sha256: digests.transactions,
      registry_sha256: digests.registry,
      replay_state_sha256: completeReplay.replayed_state.state_sha256
    },
    coverage: {
      complete: true,
      method: 'committed_non_dex_owner_net_flow_v1',
      candidate_flow_evidence_complete: true,
      transactions_scanned: 12,
      parsed_dex_swap_transactions_excluded: 2,
      candidate_deposit_transactions: 2,
      candidate_return_transactions: 1,
      ambiguous_owner_delta_transactions_excluded: 3,
      current_positive_off_curve_custody_owners: 2,
      definitions: {
        estimated_defi_claim: 'A capped estimate from committed owner-net SPYx flows.',
        creation_provenance: 'A creation signer is provenance only.',
        unallocated_custody: 'Custody not assigned by this heuristic.',
        candidate_flow_evidence: 'Exact source transactions for each emitted claim component.'
      }
    },
    portfolios: [
      {
        authority: 'owner-a',
        authority_kind: 'observed_transaction_signer',
        direct_public_balance: amount('100', '0.00000100'),
        estimated_defi_claim: amount('16', '0.00000016'),
        estimated_total_exposure: amount('116', '0.00000116'),
        programs_used: ['fixture-program'],
        claim_components: [
          {
            custody_owner: 'owner-b',
            program_id: 'fixture-program',
            observed_deposited_principal: amount('20', '0.00000020'),
            observed_returned_principal: amount('5', '0.00000005'),
            candidate_net_principal: amount('15', '0.00000015'),
            attributed_claim: amount('15', '0.00000015'),
            deposit_transaction_count: 1,
            return_transaction_count: 1,
            candidate_flow_evidence: [
              {
                transaction_id: 3,
                slot: 346_066_301,
                block_time: 1_700_000_000,
                direction: 'deposit',
                raw_amount: '20'
              },
              {
                transaction_id: 4,
                slot: 346_066_302,
                direction: 'return',
                raw_amount: '7',
                matched_principal_raw_amount: '5'
              }
            ],
            confidence: 'heuristic_owner_net_flow_capped_by_current_custody'
          },
          {
            custody_owner: 'owner-c',
            program_id: null,
            observed_deposited_principal: amount('1', '0.00000001'),
            observed_returned_principal: amount('0', '0.00000000'),
            candidate_net_principal: amount('1', '0.00000001'),
            attributed_claim: amount('1', '0.00000001'),
            deposit_transaction_count: 1,
            return_transaction_count: 0,
            candidate_flow_evidence: [
              {
                transaction_id: 5,
                slot: 346_066_303,
                block_time: 1_700_000_002,
                direction: 'deposit',
                raw_amount: '1'
              }
            ],
            confidence: 'heuristic_owner_net_flow_capped_by_current_custody'
          }
        ]
      }
    ],
    protocol_custody: [
      {
        custody_owner: 'owner-b',
        program_id: 'fixture-program',
        direct_custody_balance: amount('20', '0.00000020'),
        candidate_net_principal: amount('15', '0.00000015'),
        attributed_claim: amount('15', '0.00000015'),
        unallocated_custody: amount('5', '0.00000005'),
        claim_excess: amount('0', '0.00000000'),
        candidate_authority_count: 1,
        confidence: 'heuristic_owner_net_flow_capped_by_current_custody'
      },
      {
        custody_owner: 'owner-c',
        program_id: null,
        direct_custody_balance: amount('3', '0.00000003'),
        candidate_net_principal: amount('1', '0.00000001'),
        attributed_claim: amount('1', '0.00000001'),
        unallocated_custody: amount('2', '0.00000002'),
        claim_excess: amount('0', '0.00000000'),
        candidate_authority_count: 1,
        confidence: 'heuristic_owner_net_flow_capped_by_current_custody'
      }
    ],
    pda_creation_provenance: [
      {
        subject_pda: 'owner-b',
        event_kind: 'account_creation',
        system_instruction: 'create_account',
        runtime_owner_program_id: 'fixture-program',
        direct_caller_program_id: null,
        create_with_seed_base: null,
        signer_candidates: ['owner-a'],
        confidence: 'provenance_only_no_amount_assigned',
        proves_beneficial_ownership: false,
        location: {
          transaction_id: 4,
          outer_index: 1,
          source_epoch: 801,
          slot: 346_066_299,
          source_block_id: 1,
          tx_index: 0
        }
      }
    ]
  };
}

function authorityPortfolioHistoryFixture() {
  return {
    schema_version: 2,
    artifact_kind: 'spyx_authority_portfolio_history',
    source_binding: structuredClone(authorityPortfolioFixture().source_binding),
    coverage: {
      complete: true,
      method: 'forward_replay_216000_slot_window_end_sparse_aggregate_tuple_v2',
      slot_window_width: 216_000,
      transactions_scanned: 12,
      state_samples: 2,
      authority_series: 1,
      history_points: 2,
      final_sample_matches_current_portfolio: true,
      definitions: {
        sampling: 'Forward replay samples with unchanged authority rows omitted.',
        estimated_defi_claim: 'Only state known at each sample is used.',
        direct_public_balance: 'Exact transaction-final public balance at each sample.'
      }
    },
    point_fields: [
      'transaction_id',
      'slot',
      'block_time',
      'direct_public_balance_raw',
      'estimated_defi_claim_raw'
    ],
    series: [
      {
        authority: 'owner-a',
        points: [
          [3, 346_066_301, 1_700_000_000, '80', '10'],
          [11, 346_066_309, null, '100', '16']
        ]
      }
    ]
  };
}

completeReplay.authority_portfolios = authorityPortfolioFixture();
completeReplay.authority_portfolio_history = authorityPortfolioHistoryFixture();

const completeReplayPath = await fixture('replay-complete.json', completeReplay);
const runtimeOwnerSupplement = {
  schema_version: 1,
  artifact_kind: 'spyx_holder_authority_runtime_owner_snapshot',
  evidence_kind: 'solana_runtime_account_owner',
  cluster: 'mainnet-beta',
  rpc_method: 'getMultipleAccounts',
  rpc_endpoint: 'https://api.mainnet-beta.solana.com',
  observed_slot: 442_934_356,
  selection_scope: 'exposed_off_curve_unattributed_holder_rows',
  selection: 'Distinct off-curve rows exposed by the replay fixture.',
  source_replay_sha256: fixtureSha256.get(completeReplayPath),
  accounts: [
    {
      address: 'owner-c',
      exists: true,
      runtime_owner_program_id: 'unknown-program',
      data_bytes: 10,
      executable: false,
      account_label: 'Fixture vault owner',
      account_label_evidence: {
        kind: 'public_explorer_label',
        source_name: 'Fixture explorer',
        source_url: 'https://example.invalid/account/owner-c'
      }
    }
  ]
};
const runtimeOwnerSupplementPath = await fixture(
  'holder-runtime-owner-supplement.json',
  runtimeOwnerSupplement
);

for (const field of [
  'bounded_selected_dump_scan_complete',
  'metadata_balance_chain_continuous_from_spyx_mint_creation',
  'daily_public_balance_series_complete',
  'daily_selected_transaction_counts_complete'
]) {
  test(`rejects history when ${field} is not complete`, async () => {
    const mismatch = structuredClone(history);
    mismatch[field] = false;
    const mismatchPath = await fixture(`history-incomplete-${field}.json`, mismatch);
    const result = runBuilder([], false, programsPath, mismatchPath);
    assert.notEqual(result.status, 0);
    assert.match(output(result), new RegExp(`requires ${field} to be true`));
  });
}

test('rejects a history report without daily rows', async () => {
  const mismatch = structuredClone(history);
  mismatch.daily = [];
  const mismatchPath = await fixture('history-empty-daily.json', mismatch);
  const result = runBuilder([], false, programsPath, mismatchPath);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /daily rows must not be empty/);
});

test('rejects daily rows that are not in strict date order', async () => {
  const mismatch = structuredClone(history);
  mismatch.daily[1].utc_date = mismatch.daily[0].utc_date;
  const mismatchPath = await fixture('history-duplicate-daily-date.json', mismatch);
  const result = runBuilder([], false, programsPath, mismatchPath);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /daily rows are not in strict UTC date order/);
});

test('rejects a daily row without an exact required counter', async () => {
  const mismatch = structuredClone(history);
  delete mismatch.daily[0].selected_transactions;
  const mismatchPath = await fixture('history-daily-missing-counter.json', mismatch);
  const result = runBuilder([], false, programsPath, mismatchPath);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /daily\[0\]\.selected_transactions is not a non-negative safe integer/);
});

test('accepts and copies a valid final top-100 holder history', async () => {
  const withHistory = historyWithFinalTopHolderHistory();
  const withHistoryPath = await fixture('history-with-final-top-holders.json', withHistory);
  const { result, summary } = await runHistoryBuilderWithOutput(withHistoryPath);
  assert.equal(result.status, 0, output(result));
  assert.deepEqual(
    summary.final_top_100_holder_history,
    withHistory.final_top_100_holder_history
  );
});

test('accepts an old schema-1 history without final top-100 holder history', async () => {
  const { result, summary } = await runHistoryBuilderWithOutput(historyPath);
  assert.equal(result.status, 0, output(result));
  assert.equal(Object.hasOwn(summary, 'final_top_100_holder_history'), false);
});

for (const [name, mutate, expectedError] of [
  [
    'source binding mismatch',
    (value) => {
      value.final_top_100_holder_history.source_binding.transactions_sha256 = '99'.repeat(32);
    },
    /source_binding\.transactions_sha256 does not match/
  ],
  [
    'calendar date mismatch',
    (value) => {
      value.final_top_100_holder_history.days[1].utc_date = '2026-08-20';
    },
    /days\[1\]\.utc_date does not match/
  ],
  [
    'fixed cohort size mismatch',
    (value) => {
      value.final_top_100_holder_history.cohort.selected_holders = 2;
    },
    /cohort\.selected_holders does not match/
  ],
  [
    'fixed cohort owner mismatch',
    (value) => {
      const first = value.final_top_100_holder_history.series[0].owner;
      value.final_top_100_holder_history.series[0].owner =
        value.final_top_100_holder_history.series[1].owner;
      value.final_top_100_holder_history.series[1].owner = first;
    },
    /series\[0\] final cohort owner does not match/
  ],
  [
    'invalid raw amount',
    (value) => {
      value.final_top_100_holder_history.series[0].daily_raw_balances[0] = '01';
    },
    /daily_raw_balances\[0\] is not an unsigned decimal string/
  ],
  [
    'final point mismatch',
    (value) => {
      value.final_top_100_holder_history.series[0].daily_raw_balances[1] = '99';
    },
    /series\[0\] final point does not match/
  ],
  [
    'series ordering mismatch',
    (value) => {
      const first = value.final_top_100_holder_history.series[0];
      const second = value.final_top_100_holder_history.series[1];
      value.final_top_100_holder_history.series[0] = { ...second, final_rank: 1 };
      value.final_top_100_holder_history.series[1] = { ...first, final_rank: 2 };
    },
    /series is not in final rank order/
  ],
  [
    'final top-100 total mismatch',
    (value) => {
      const top100 = concentration('122', '0.00000122', '122', '123', 991_869);
      value.final_public_balance.top_100_concentration = top100;
      value.daily.at(-1).top_100_concentration = structuredClone(top100);
    },
    /final top-100 total does not match/
  ]
]) {
  test(`rejects malformed final top-100 history with ${name}`, async () => {
    const mismatch = historyWithFinalTopHolderHistory();
    mutate(mismatch);
    const mismatchPath = await fixture(
      `history-final-top-100-${name.replaceAll(' ', '-')}.json`,
      mismatch
    );
    const result = runBuilder([], false, programsPath, mismatchPath);
    assert.notEqual(result.status, 0);
    assert.match(output(result), expectedError);
  });
}

test('rejects an audit transaction count that differs from the source', async () => {
  const mismatch = structuredClone(history);
  mismatch.audit.transactions = 11;
  const mismatchPath = await fixture('history-audit-transaction-mismatch.json', mismatch);
  const result = runBuilder([], false, programsPath, mismatchPath);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /audit transaction count does not match/);
});

test('rejects daily transaction totals that differ from the source', async () => {
  const mismatch = structuredClone(history);
  mismatch.daily[0].selected_transactions = 4;
  const mismatchPath = await fixture('history-daily-transaction-mismatch.json', mismatch);
  const result = runBuilder([], false, programsPath, mismatchPath);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /daily selected transaction count does not match/);
});

for (const [name, mutate, expectedError] of [
  [
    'holder count',
    (value) => {
      value.daily.at(-1).positive_public_balance_holders = 2;
    },
    /final daily positive_public_balance_holders does not match/
  ],
  [
    'raw balance',
    (value) => {
      value.daily.at(-1).public_raw_balance_sum = amount('124', '0.00000124');
      for (const field of [
        'top_1_concentration',
        'top_10_concentration',
        'top_100_concentration'
      ]) {
        value.daily.at(-1)[field] = concentration('124', '0.00000124');
      }
    },
    /final daily public_raw_balance_sum does not match/
  ],
  [
    'concentration',
    (value) => {
      value.daily.at(-1).top_1_concentration = concentration(
        '122',
        '0.00000122',
        '122',
        '123',
        991_869
      );
    },
    /final daily top_1_concentration does not match/
  ]
]) {
  test(`rejects a final daily ${name} mismatch`, async () => {
    const mismatch = structuredClone(history);
    mutate(mismatch);
    const mismatchPath = await fixture(`history-final-${name.replaceAll(' ', '-')}.json`, mismatch);
    const result = runBuilder([], false, programsPath, mismatchPath);
    assert.notEqual(result.status, 0);
    assert.match(output(result), expectedError);
  });
}

for (const [name, mutate, expectedError] of [
  [
    'non-string limitation',
    (value) => {
      value.limitations.public_scope = 7;
    },
    /limitations\.public_scope is not a non-empty string/
  ],
  [
    'missing holder array',
    (value) => {
      delete value.final_public_balance.largest_25_holders;
    },
    /Expected final_public_balance\.largest_25_holders to be an array/
  ],
  [
    'invalid holder counter',
    (value) => {
      value.final_public_balance.largest_25_holders[0].token_account_count = '2';
    },
    /largest_25_holders\[0\]\.token_account_count is not a non-negative safe integer/
  ],
  [
    'unordered holder rows',
    (value) => {
      value.final_public_balance.largest_25_holders.reverse();
    },
    /largest_25_holders is not in descending public-balance order/
  ],
  [
    'distribution total mismatch',
    (value) => {
      value.final_public_balance.balance_distribution[0].holder_count = 2;
    },
    /balance distribution holder count does not match/
  ],
  [
    'invalid public movement amount',
    (value) => {
      value.public_volume_totals.public_bilateral_movement.base_units = 'not-a-number';
    },
    /public_volume_totals\.public_bilateral_movement\.base_units does not match/
  ],
  [
    'daily movement total mismatch',
    (value) => {
      value.public_volume_totals.public_bilateral_movement = amount('6', '0.00000006');
    },
    /daily public_bilateral_movement does not match/
  ],
  [
    'top day mismatch',
    (value) => {
      value.top_25_volume_days[0].selected_transactions = 6;
    },
    /top_25_volume_days\[0\]\.selected_transactions does not match its daily row/
  ],
  [
    'missing movement transaction signature',
    (value) => {
      value.top_25_volume_transactions[0].first_signature = '';
    },
    /top_25_volume_transactions\[0\]\.first_signature is not a non-empty string/
  ],
  [
    'movement transaction date mismatch',
    (value) => {
      value.top_25_volume_transactions[0].utc_date = '2026-08-18';
    },
    /top_25_volume_transactions\[0\]\.utc_date does not match block_time_unix_seconds/
  ],
  [
    'missing RPC scope',
    (value) => {
      delete value.rpc_request_model.scope;
    },
    /rpc_request_model\.scope is not a non-empty string/
  ]
]) {
  test(`rejects UI-facing history data with ${name}`, async () => {
    const mismatch = structuredClone(history);
    mutate(mismatch);
    const mismatchPath = await fixture(`history-ui-${name.replaceAll(' ', '-')}.json`, mismatch);
    const result = runBuilder([], false, programsPath, mismatchPath);
    assert.notEqual(result.status, 0);
    assert.match(output(result), expectedError);
  });
}

test('derives exact mint-only and complete request coverage before rows are omitted', () => {
  const comparison = deriveProviderAccessComparison(history);
  assert.deepEqual(comparison.mint_only, {
    addresses_queried: 1,
    selected_transactions_covered: 10,
    selected_transactions_missed: 2,
    complete_selected_dump_coverage: false,
    get_signatures_for_address_requests: 1,
    get_transaction_requests: 10,
    modeled_request_total: 11
  });
  assert.equal(comparison.all_target_addresses.modeled_request_total, 28);
  assert.equal(comparison.all_target_addresses.complete_selected_dump_coverage, true);
  assert.deepEqual(comparison.all_target_addresses.coverage_prerequisite, {
    required: true,
    historical_token_account_list_must_preexist: true,
    includes_closed_accounts: true,
    discoverable_from_mint_only_rpc: false
  });
  assert.equal(comparison.existing_verified_dump_scan.provider_rpc_requests, 0);
  assert.equal(comparison.basis.includes('price'), true);
});

test('rejects a provider model without the exact SPYx mint row', () => {
  const mismatch = structuredClone(history);
  mismatch.rpc_request_model.per_address.at(-1).address = 'different-mint';
  assert.throws(
    () => deriveProviderAccessComparison(mismatch),
    /does not have one mint row for the SPYx mint/
  );
});

test('rejects inconsistent per-address request totals', () => {
  const mismatch = structuredClone(history);
  mismatch.rpc_request_model.per_address.at(-1).returned_address_signature_rows = 11;
  assert.throws(
    () => deriveProviderAccessComparison(mismatch),
    /RPC returned address rows does not match/
  );
});

test('accepts an attached canary replay for a development validation', () => {
  const result = runBuilder(['--strict-replay', replayPath], false);
  assert.equal(result.status, 0, output(result));
});

test('rejects an incomplete strict replay for a release validation', () => {
  const result = runBuilder(['--strict-replay', replayPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /requires a complete_match strict replay report/);
});

test('rejects a UI-facing replay counter with the wrong type', async () => {
  const mismatchPath = await fixture('replay-invalid-ui-counter.json', {
    ...replay,
    counters: { transactions_scanned: '12' }
  });
  const result = runBuilder(['--strict-replay', mismatchPath], false);
  assert.notEqual(result.status, 0);
  assert.match(
    output(result),
    /strict replay counters\.transactions_scanned is not a non-negative safe integer/
  );
});

test('rejects a malformed UI-facing replay failure', async () => {
  const mismatchPath = await fixture('replay-invalid-ui-failure.json', {
    ...replay,
    first_failure: { code: 'failure' }
  });
  const result = runBuilder(['--strict-replay', mismatchPath], false);
  assert.notEqual(result.status, 0);
  assert.match(
    output(result),
    /strict replay first_failure\.source_epoch is not a non-negative safe integer/
  );
});

test('accepts a complete replay state bound to the history totals', () => {
  const result = runBuilder(['--strict-replay', completeReplayPath]);
  assert.equal(result.status, 0, output(result));
});

test('rejects a schema-4 strict replay for a release validation', async () => {
  const mismatch = structuredClone(completeReplay);
  mismatch.schema_version = 4;
  const mismatchPath = await fixture('replay-release-schema-4.json', mismatch);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(
    output(result),
    /requires a complete_match strict replay report with complete holder authority classification/
  );
});

test('rejects a release replay without authority portfolios', async () => {
  const mismatch = structuredClone(completeReplay);
  delete mismatch.authority_portfolios;
  delete mismatch.authority_portfolio_history;
  const mismatchPath = await fixture('replay-release-without-authority-portfolios.json', mismatch);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(
    output(result),
    /requires a complete authority portfolio scan with transaction evidence/
  );
});

test(
  'rejects incomplete authority portfolio transaction evidence for a release validation',
  async () => {
    const mismatch = structuredClone(completeReplay);
    mismatch.authority_portfolios.coverage.candidate_flow_evidence_complete = false;
    const mismatchPath = await fixture(
      'replay-release-incomplete-flow-evidence.json',
      mismatch
    );
    const result = runBuilder(['--strict-replay', mismatchPath]);
    assert.notEqual(result.status, 0);
    assert.match(
      output(result),
      /requires a complete authority portfolio scan with transaction evidence/
    );
  }
);

test('rejects a release replay without authority portfolio history', async () => {
  const mismatch = structuredClone(completeReplay);
  delete mismatch.authority_portfolio_history;
  const mismatchPath = await fixture(
    'replay-release-without-authority-portfolio-history.json',
    mismatch
  );
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /requires complete forward authority portfolio history/);
});

test('copies a reconciled authority portfolio heuristic and adds display-only program names', async () => {
  const withPortfolio = structuredClone(completeReplay);
  withPortfolio.schema_version = 5;
  withPortfolio.authority_portfolios = authorityPortfolioFixture();
  const withPortfolioPath = await fixture('replay-authority-portfolio.json', withPortfolio);
  const {
    result,
    summary,
    authorityPortfolios,
    authorityPortfolioTable,
    authorityPortfolioPath
  } = await runReplayBuilderWithOutput(
    withPortfolioPath,
    populatedProgramsPath
  );
  assert.equal(result.status, 0, output(result));
  assert.equal(Object.hasOwn(summary, 'authority_portfolios'), false);
  assert.equal(summary.compact_build.authority_portfolios_available, true);
  assert.equal(authorityPortfolios.artifact_kind, 'spyx_authority_portfolio_heuristic');
  assert.equal(authorityPortfolios.coverage.candidate_flow_evidence_complete, true);
  assert.equal(authorityPortfolioTable.artifact_kind, 'spyx_authority_portfolio_table');
  assert.equal(authorityPortfolioTable.coverage.complete, true);
  assert.deepEqual(authorityPortfolioTable.portfolios[0].custody_owners, ['owner-b', 'owner-c']);
  assert.equal(
    Object.hasOwn(authorityPortfolioTable.portfolios[0], 'claim_components'),
    false
  );
  assert.deepEqual(authorityPortfolios.portfolios[0].programs_used, [
    {
      program_id: 'fixture-program',
      program_name: 'Fixture program',
      program_id_evidence: 'replay_program_id'
    }
  ]);
  assert.equal(
    authorityPortfolios.protocol_custody[0].program_name,
    'Fixture program'
  );
  assert.equal(authorityPortfolios.protocol_custody[1].program_id, null);
  assert.equal(authorityPortfolios.portfolios[0].claim_components[1].program_id, null);
  assert.deepEqual(
    authorityPortfolios.portfolios[0].claim_components[0].candidate_flow_evidence,
    [
      {
        transaction_id: 3,
        slot: 346_066_301,
        block_time: 1_700_000_000,
        direction: 'deposit',
        raw_amount: '20'
      },
      {
        transaction_id: 4,
        slot: 346_066_302,
        direction: 'return',
        raw_amount: '7',
        matched_principal_raw_amount: '5'
      }
    ]
  );
  assert.equal(
    Object.hasOwn(authorityPortfolios.protocol_custody[1], 'program_id_evidence'),
    false
  );
  assert.equal(
    authorityPortfolios.pda_creation_provenance[0].proves_beneficial_ownership,
    false
  );
  assert.deepEqual(authorityPortfolios.pda_authority_estimate_summary, {
    schema_version: 1,
    method: 'committed_pda_creation_signer_external_claims_v1',
    subject_count: 1,
    selected_subject_count: 1,
    proves_beneficial_ownership: false,
    additive_to_authority_totals: false
  });
  const pdaEstimate = authorityPortfolios.pda_authority_estimates[0];
  assert.equal(pdaEstimate.subject_pda, 'owner-b');
  assert.equal(pdaEstimate.runtime_owner_program_name, 'Fixture program');
  assert.equal(pdaEstimate.resolution, 'single_unique_creation_signer_candidate');
  assert.equal(pdaEstimate.selected_candidate_authority, 'owner-a');
  assert.deepEqual(pdaEstimate.direct_public_balance, amount('20', '0.00000020'));
  assert.deepEqual(
    pdaEstimate.estimated_external_defi_claim,
    amount('1', '0.00000001')
  );
  const shardDirectory = join(
    dirname(authorityPortfolioPath),
    'spyx-authority-portfolios-by-prefix'
  );
  const shard = JSON.parse(await readFile(join(shardDirectory, '6f.json'), 'utf8'));
  assert.equal(shard.artifact_kind, 'spyx_authority_portfolio_shard');
  assert.equal(shard.prefix, 'o');
  assert.deepEqual(shard.source_binding, authorityPortfolios.source_binding);
  assert.deepEqual(shard.portfolios, authorityPortfolios.portfolios);
  const shardIndex = JSON.parse(await readFile(join(shardDirectory, 'index.json'), 'utf8'));
  assert.deepEqual(shardIndex.prefixes, ['o']);
  assert.equal(shardIndex.portfolios, 1);
  assert.deepEqual(pdaEstimate.estimated_total_exposure, amount('21', '0.00000021'));
  assert.equal(pdaEstimate.proves_beneficial_ownership, false);
  assert.equal(pdaEstimate.additive_to_authority_totals, false);
  assert.deepEqual(pdaEstimate.candidates[0].program_positions, [
    {
      program_id: null,
      program_name: null,
      custody_owners: ['owner-c'],
      custody_owner_count: 1,
      observed_deposited_principal: amount('1', '0.00000001'),
      observed_returned_principal: amount('0', '0.00000000'),
      candidate_net_principal: amount('1', '0.00000001'),
      estimated_claim: amount('1', '0.00000001'),
      deposit_transaction_count: 1,
      return_transaction_count: 0
    }
  ]);
  assert.deepEqual(
    summary.final_public_balance.holder_authority.class_totals,
    completeReplay.holder_authority.class_totals
  );
});

test('validates and shards forward-only authority portfolio history', async () => {
  const withHistory = structuredClone(completeReplay);
  withHistory.schema_version = 5;
  withHistory.authority_portfolios = authorityPortfolioFixture();
  withHistory.authority_portfolio_history = authorityPortfolioHistoryFixture();
  const withHistoryPath = await fixture('replay-authority-portfolio-history.json', withHistory);
  const {
    result,
    summary,
    authorityPortfolioHistory,
    authorityPortfolioHistoryPath
  } = await runReplayBuilderWithOutput(withHistoryPath, populatedProgramsPath);
  assert.equal(result.status, 0, output(result));
  assert.equal(summary.compact_build.authority_portfolio_history_available, true);
  assert.equal(
    authorityPortfolioHistory.artifact_kind,
    'spyx_authority_portfolio_history_shard_index'
  );
  assert.equal(authorityPortfolioHistory.history_points, 2);
  const shardDirectory = join(
    dirname(authorityPortfolioHistoryPath),
    'spyx-authority-portfolio-history-by-prefix'
  );
  const shard = JSON.parse(await readFile(join(shardDirectory, '6f77.json'), 'utf8'));
  assert.equal(shard.artifact_kind, 'spyx_authority_portfolio_history_shard');
  assert.equal(shard.source_schema_version, 2);
  assert.equal(shard.prefix_length, 2);
  assert.equal(shard.prefix, 'ow');
  assert.deepEqual(shard.series, withHistory.authority_portfolio_history.series);
  assert.deepEqual(shard.point_fields, withHistory.authority_portfolio_history.point_fields);
  const shardIndex = JSON.parse(await readFile(join(shardDirectory, 'index.json'), 'utf8'));
  assert.deepEqual(shardIndex.prefixes, ['ow']);
  assert.equal(shardIndex.authority_series, 1);
  assert.equal(shardIndex.history_points, 2);
});

test('rejects authority history whose final estimate differs from the current portfolio', async () => {
  const withHistory = structuredClone(completeReplay);
  withHistory.schema_version = 5;
  withHistory.authority_portfolios = authorityPortfolioFixture();
  withHistory.authority_portfolio_history = authorityPortfolioHistoryFixture();
  withHistory.authority_portfolio_history.series[0].points.at(-1)[4] = '15';
  const mismatchPath = await fixture('replay-authority-portfolio-history-mismatch.json', withHistory);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /final owner-a estimated_defi_claim does not match/);
});

test('does not combine a creation signer that is shared by more than one PDA', async () => {
  const withPortfolio = structuredClone(completeReplay);
  withPortfolio.schema_version = 5;
  withPortfolio.authority_portfolios = authorityPortfolioFixture();
  withPortfolio.authority_portfolios.pda_creation_provenance.push({
    ...structuredClone(withPortfolio.authority_portfolios.pda_creation_provenance[0]),
    subject_pda: 'owner-c',
    runtime_owner_program_id: 'unknown-program',
    location: {
      transaction_id: 5,
      outer_index: 0,
      source_epoch: 801,
      slot: 346_066_300,
      source_block_id: 2,
      tx_index: 0
    }
  });
  const withPortfolioPath = await fixture(
    'replay-authority-portfolio-shared-creation-signer.json',
    withPortfolio
  );
  const { result, authorityPortfolios } = await runReplayBuilderWithOutput(
    withPortfolioPath,
    populatedProgramsPath
  );
  assert.equal(result.status, 0, output(result));
  assert.equal(authorityPortfolios.pda_authority_estimate_summary.subject_count, 2);
  assert.equal(authorityPortfolios.pda_authority_estimate_summary.selected_subject_count, 0);
  for (const estimate of authorityPortfolios.pda_authority_estimates) {
    assert.equal(estimate.resolution, 'shared_creation_signer_candidate');
    assert.equal(estimate.selected_candidate_authority, null);
    assert.equal(estimate.estimated_external_defi_claim, null);
    assert.equal(estimate.estimated_total_exposure, null);
    assert.equal(estimate.candidates[0].linked_subject_pda_count, 2);
  }
});

test('fills missing portfolio program IDs only from attributed runtime-owner evidence', async () => {
  const withPortfolio = structuredClone(completeReplay);
  withPortfolio.schema_version = 5;
  withPortfolio.authority_portfolios = authorityPortfolioFixture();
  const withPortfolioPath = await fixture(
    'replay-authority-portfolio-runtime-owner.json',
    withPortfolio
  );
  const supplement = structuredClone(runtimeOwnerSupplement);
  supplement.source_replay_sha256 = fixtureSha256.get(withPortfolioPath);
  const supplementPath = await fixture(
    'holder-runtime-owner-authority-portfolio.json',
    supplement
  );
  const { result, authorityPortfolios } = await runBuilderWithOutput(
    supplementPath,
    withPortfolioPath,
    populatedProgramsPath
  );
  assert.equal(result.status, 0, output(result));
  assert.deepEqual(authorityPortfolios.portfolios[0].programs_used, [
    {
      program_id: 'fixture-program',
      program_name: 'Fixture program',
      program_id_evidence: 'replay_program_id'
    },
    {
      program_id: 'unknown-program',
      program_name: null,
      program_id_evidence: 'supplemental_runtime_account_owner'
    }
  ]);
  const resolvedComponent = authorityPortfolios.portfolios[0].claim_components[1];
  assert.equal(resolvedComponent.custody_owner, 'owner-c');
  assert.equal(resolvedComponent.program_id, 'unknown-program');
  assert.equal(resolvedComponent.program_name, null);
  assert.equal(resolvedComponent.program_id_evidence, 'supplemental_runtime_account_owner');
  const resolvedCustody = authorityPortfolios.protocol_custody[1];
  assert.equal(resolvedCustody.custody_owner, 'owner-c');
  assert.equal(resolvedCustody.program_id, 'unknown-program');
  assert.equal(resolvedCustody.program_name, null);
  assert.equal(resolvedCustody.program_id_evidence, 'supplemental_runtime_account_owner');
  assert.equal(authorityPortfolios.protocol_custody[0].program_id, 'fixture-program');
  assert.equal(
    authorityPortfolios.protocol_custody[0].program_id_evidence,
    'replay_program_id'
  );
});

test('keeps older authority portfolios usable while marking transaction evidence incomplete', async () => {
  const withPortfolio = structuredClone(completeReplay);
  withPortfolio.schema_version = 5;
  withPortfolio.authority_portfolios = authorityPortfolioFixture();
  delete withPortfolio.authority_portfolios.coverage.candidate_flow_evidence_complete;
  delete withPortfolio.authority_portfolios.coverage.definitions.candidate_flow_evidence;
  for (const portfolio of withPortfolio.authority_portfolios.portfolios) {
    for (const component of portfolio.claim_components) {
      delete component.candidate_flow_evidence;
    }
  }
  const withPortfolioPath = await fixture(
    'replay-authority-portfolio-without-flow-evidence.json',
    withPortfolio
  );
  const { result, authorityPortfolios } = await runReplayBuilderWithOutput(
    withPortfolioPath,
    populatedProgramsPath,
    false
  );
  assert.equal(result.status, 0, output(result));
  assert.equal(authorityPortfolios.coverage.candidate_flow_evidence_complete, false);
  assert.deepEqual(
    authorityPortfolios.portfolios[0].claim_components[0].candidate_flow_evidence,
    []
  );
});

for (const [name, mutate, expectedError] of [
  [
    'portfolio total mismatch',
    (value) => {
      value.portfolios[0].estimated_total_exposure = amount('114', '0.00000114');
    },
    /estimated_total_exposure does not equal direct plus claim/
  ],
  [
    'custody reconciliation mismatch',
    (value) => {
      value.protocol_custody[0].unallocated_custody = amount('4', '0.00000004');
    },
    /does not reconcile attributed and unallocated custody/
  ],
  [
    'creation provenance ownership claim',
    (value) => {
      value.pda_creation_provenance[0].proves_beneficial_ownership = true;
    },
    /must not claim beneficial ownership/
  ],
  [
    'PDA beneficiary authority kind',
    (value) => {
      value.portfolios[0].authority_kind = 'pda_or_program_account';
    },
    /authority_kind is not supported/
  ],
  [
    'source binding mismatch',
    (value) => {
      value.source_binding.transactions_sha256 = '99'.repeat(32);
    },
    /source_binding\.transactions_sha256 does not match/
  ],
  [
    'candidate flow aggregate mismatch',
    (value) => {
      value.portfolios[0].claim_components[0].candidate_flow_evidence[1]
        .matched_principal_raw_amount = '4';
    },
    /candidate_flow_evidence does not match its aggregate/
  ],
  [
    'candidate flow transaction order mismatch',
    (value) => {
      value.portfolios[0].claim_components[0].candidate_flow_evidence[1].transaction_id = 2;
    },
    /candidate_flow_evidence is not in transaction order/
  ],
  [
    'direct balance partition mismatch',
    (value) => {
      value.portfolios[0].direct_public_balance = amount('99', '0.00000099');
      value.portfolios[0].estimated_total_exposure = amount('115', '0.00000115');
    },
    /direct portfolio and custody balances do not equal the final public balance/
  ],
  [
    'direct holder partition mismatch',
    (value) => {
      value.portfolios[0].direct_public_balance = amount('0', '0.00000000');
      value.portfolios[0].estimated_total_exposure = amount('16', '0.00000016');
      value.protocol_custody[0].direct_custody_balance = amount('120', '0.00000120');
      value.protocol_custody[0].unallocated_custody = amount('105', '0.00000105');
    },
    /direct portfolio and custody holder counts do not equal the final holder count/
  ]
]) {
  test(`rejects an authority portfolio heuristic with ${name}`, async () => {
    const mismatch = structuredClone(completeReplay);
    mismatch.schema_version = 5;
    mismatch.authority_portfolios = authorityPortfolioFixture();
    mutate(mismatch.authority_portfolios);
    const mismatchPath = await fixture(
      `replay-authority-portfolio-${name.replaceAll(' ', '-')}.json`,
      mismatch
    );
    const result = runBuilder(['--strict-replay', mismatchPath]);
    assert.notEqual(result.status, 0);
    assert.match(output(result), expectedError);
  });
}

test('accepts bounded committed runtime-owner evidence without treating it as PDA proof', async () => {
  const withEvidence = structuredClone(completeReplay);
  withEvidence.schema_version = 5;
  withEvidence.holder_authority.largest_25_all[2].runtime_account_owner = {
    source: 'committed_system_owner_instruction',
    program_id: 'unknown-program',
    observation_count: 2,
    owner_change_count: 1,
    conflict_count: 0,
    proves_pda_derivation: false,
    last_observation: {
      transaction_id: 11,
      outer_index: 2,
      inner_index: 0,
      source_epoch: 801,
      slot: 346_066_299,
      source_block_id: 1,
      tx_index: 0
    }
  };
  const withEvidencePath = await fixture('replay-runtime-owner-evidence.json', withEvidence);
  const result = runBuilder(['--strict-replay', withEvidencePath]);
  assert.equal(result.status, 0, output(result));
});

test('rejects committed runtime-owner evidence that claims PDA derivation', async () => {
  const mismatch = structuredClone(completeReplay);
  mismatch.holder_authority.largest_25_all[2].runtime_account_owner = {
    source: 'committed_system_owner_instruction',
    program_id: 'unknown-program',
    observation_count: 1,
    owner_change_count: 0,
    conflict_count: 0,
    proves_pda_derivation: true,
    last_observation: {
      transaction_id: 11,
      outer_index: 2,
      source_epoch: 801,
      slot: 346_066_299,
      source_block_id: 1,
      tx_index: 0
    }
  };
  const mismatchPath = await fixture('replay-runtime-owner-false-pda.json', mismatch);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /runtime_account_owner must not claim PDA derivation/);
});

test('enriches committed runtime-owner labels in the complete and supplemental holder rows', async () => {
  const replayWithCompleteRows = structuredClone(completeReplay);
  replayWithCompleteRows.schema_version = 5;
  const offCurveRow = structuredClone(
    replayWithCompleteRows.holder_authority.largest_25_all[2]
  );
  offCurveRow.runtime_account_owner = {
    source: 'committed_system_owner_instruction',
    program_id: 'runtime-program',
    observation_count: 1,
    owner_change_count: 0,
    conflict_count: 0,
    proves_pda_derivation: false,
    last_observation: {
      transaction_id: 11,
      outer_index: 2,
      source_epoch: 801,
      slot: 346_066_299,
      source_block_id: 1,
      tx_index: 0
    }
  };
  replayWithCompleteRows.holder_authority.off_curve_unattributed_holders = [offCurveRow];
  const replayPath = await fixture('replay-complete-runtime-owner.json', replayWithCompleteRows);

  const fullSupplement = structuredClone(runtimeOwnerSupplement);
  fullSupplement.selection_scope = 'all_off_curve_unattributed_holders';
  fullSupplement.source_replay_sha256 = fixtureSha256.get(replayPath);
  const supplementPath = await fixture('holder-runtime-owner-full.json', fullSupplement);
  const labeledPrograms = structuredClone(programs);
  labeledPrograms.programs = [
    {
      rank: 1,
      registry_id: 1,
      program_id: 'runtime-program',
      identity_status: 'identified',
      selected_name: 'Known runtime program',
      selected_source: 'test_public_source',
      selected_confidence: 'high',
      usable_onchain_idl: false,
      address_clean_onchain_idl: false,
      decoder_source_found: false,
      total_occurrences: 1,
      outer_occurrences: 1,
      inner_occurrences: 0,
      transactions: 1,
      evidence: [
        {
          source: 'test_public_source',
          name: 'Known runtime program',
          confidence: 'high',
          decoder_source: false
        }
      ]
    }
  ];
  labeledPrograms.counters = {
    ...labeledPrograms.counters,
    programs_total: 1,
    programs_identified: 1,
    identified_program_ratio: 1,
    programs_added_by_public_sources: 1,
    instruction_occurrences_total: 1,
    identified_instruction_occurrences: 1,
    identified_instruction_occurrence_ratio: 1,
    identified_outer_occurrences: 1
  };
  labeledPrograms.source_match_counts = { test_public_source: 1 };
  const labeledProgramsPath = await fixture('programs-runtime-owner.json', labeledPrograms);

  const { result, summary } = await runBuilderWithOutput(
    supplementPath,
    replayPath,
    labeledProgramsPath
  );
  assert.equal(result.status, 0, output(result));
  const authority = summary.final_public_balance.holder_authority;
  assert.equal(
    authority.off_curve_unattributed_holders[0].runtime_account_owner.program_name,
    'Known runtime program'
  );
  assert.equal(
    authority.attribution_supplements[0].holders[0].runtime_account_owner.program_name,
    'Known runtime program'
  );
});

test('adds runtime-owner evidence without changing canonical holder totals or PDA rows', async () => {
  const { result, summary } = await runBuilderWithOutput(runtimeOwnerSupplementPath);
  assert.equal(result.status, 0, output(result));

  const authority = summary.final_public_balance.holder_authority;
  assert.deepEqual(authority.class_totals, completeReplay.holder_authority.class_totals);
  assert.deepEqual(
    authority.attributed_program_holders.map(withoutExplorerLabels),
    completeReplay.holder_authority.attributed_program_holders
  );
  assert.deepEqual(
    authority.holdings_by_program.map(withoutExplorerLabels),
    completeReplay.holder_authority.holdings_by_program
  );
  assert.equal(summary.final_public_balance.positive_public_balance_holders, 3);
  assert.equal(summary.final_public_balance.active_public_token_accounts, 4);
  assert.equal(summary.final_public_balance.public_raw_balance_sum.raw_amount, '123');

  const target = authority.largest_25_all.find((row) => row.owner === 'owner-c');
  assert.equal(target.authority_kind, 'off_curve_unattributed');
  assert.deepEqual(target.supplemental_program_attribution, {
    evidence_kind: 'solana_runtime_account_owner',
    snapshot_slot: 442_934_356,
    account_exists: true,
    runtime_owner_program_id: 'unknown-program',
    runtime_owner_program_name: null,
    data_bytes: 10,
    executable: false,
    attribution_status: 'attributed_custom_program_runtime_owner',
    proves_pda_derivation: false,
    account_label: 'Fixture vault owner',
    account_label_evidence: runtimeOwnerSupplement.accounts[0].account_label_evidence
  });

  const overlay = authority.attribution_supplements[0];
  assert.equal(
    summary.compact_build.holder_authority_supplement_sha256,
    fixtureSha256.get(runtimeOwnerSupplementPath)
  );
  assert.equal(overlay.coverage.complete_for_all_off_curve_unattributed_holders, false);
  assert.equal(overlay.coverage.replay_off_curve_unattributed_holder_count, 1);
  assert.equal(overlay.coverage.queried_holder_count, 1);
  assert.equal(overlay.coverage.unqueried_holder_count, 0);
  assert.equal(overlay.coverage.observed_holder_count, 1);
  assert.equal(overlay.coverage.unobserved_holder_count, 0);
  assert.equal(overlay.counts.attributed_custom_program_runtime_owner, 1);
  assert.equal(overlay.counts.not_attributed_system_program, 0);
  assert.equal(overlay.totals.observed.public_balance.raw_amount, '3');
  assert.equal(overlay.totals.attributed_custom_program.public_balance.raw_amount, '3');
  assert.deepEqual(overlay.holdings_by_program.map((row) => row.program_id), [
    'unknown-program'
  ]);
  assert.equal(overlay.holdings_by_program[0].program_name, null);
});

test('keeps System Program runtime-owner evidence explicit and unattributed', async () => {
  const systemSupplement = structuredClone(runtimeOwnerSupplement);
  systemSupplement.accounts[0] = {
    address: 'owner-c',
    exists: true,
    runtime_owner_program_id: '11111111111111111111111111111111',
    data_bytes: 0,
    executable: false
  };
  const systemPath = await fixture('holder-runtime-owner-system.json', systemSupplement);
  const { result, summary } = await runBuilderWithOutput(systemPath);
  assert.equal(result.status, 0, output(result));
  const overlay = summary.final_public_balance.holder_authority.attribution_supplements[0];
  assert.equal(overlay.holders.length, 1);
  assert.equal(
    overlay.holders[0].supplemental_program_attribution.attribution_status,
    'not_attributed_system_program'
  );
  assert.equal(overlay.counts.not_attributed_system_program, 1);
  assert.equal(overlay.counts.runtime_owner_programs, 1);
  assert.equal(overlay.totals.attributed_custom_program.holder_count, 0);
  assert.deepEqual(
    overlay.holdings_by_program.map((row) => row.program_id),
    ['11111111111111111111111111111111']
  );
});

test('separates queried holders from existing Account.owner observations', async () => {
  const missingSupplement = structuredClone(runtimeOwnerSupplement);
  missingSupplement.accounts[0] = {
    address: 'owner-c',
    exists: false,
    runtime_owner_program_id: null,
    data_bytes: null,
    executable: null
  };
  const missingPath = await fixture('holder-runtime-owner-missing.json', missingSupplement);
  const { result, summary } = await runBuilderWithOutput(missingPath);
  assert.equal(result.status, 0, output(result));
  const overlay = summary.final_public_balance.holder_authority.attribution_supplements[0];
  assert.equal(overlay.coverage.queried_holder_count, 1);
  assert.equal(overlay.coverage.unqueried_holder_count, 0);
  assert.equal(overlay.coverage.observed_holder_count, 0);
  assert.equal(overlay.coverage.unobserved_holder_count, 1);
  assert.equal(overlay.counts.present_accounts, 0);
  assert.equal(overlay.counts.absent_accounts, 1);
  assert.deepEqual(overlay.holdings_by_program, []);
});

test('rejects a runtime-owner supplement bound to another replay', async () => {
  const mismatch = structuredClone(runtimeOwnerSupplement);
  mismatch.source_replay_sha256 = '99'.repeat(32);
  const mismatchPath = await fixture('holder-runtime-owner-wrong-replay.json', mismatch);
  const result = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--holder-authority-supplement',
    mismatchPath,
    '--holder-authority-supplement-sha256',
    fixtureSha256.get(mismatchPath)
  ]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /supplement source replay SHA-256 does not match/);
});

test('rejects a runtime-owner supplement that omits a selected holder', async () => {
  const mismatch = structuredClone(runtimeOwnerSupplement);
  mismatch.accounts = [];
  const mismatchPath = await fixture('holder-runtime-owner-empty.json', mismatch);
  const result = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--holder-authority-supplement',
    mismatchPath,
    '--holder-authority-supplement-sha256',
    fixtureSha256.get(mismatchPath)
  ]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /supplement\.accounts must not be empty/);
});

test('rejects full off-curve coverage when the replay has no complete holder array', async () => {
  const mismatch = structuredClone(runtimeOwnerSupplement);
  mismatch.selection_scope = 'all_off_curve_unattributed_holders';
  const mismatchPath = await fixture('holder-runtime-owner-false-full.json', mismatch);
  const result = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--holder-authority-supplement',
    mismatchPath,
    '--holder-authority-supplement-sha256',
    fixtureSha256.get(mismatchPath)
  ]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /replay has no complete holder array/);
});

test('requires and checks a runtime-owner supplement pin for release data', async () => {
  const missingPin = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--holder-authority-supplement',
    runtimeOwnerSupplementPath
  ]);
  assert.notEqual(missingPin.status, 0);
  assert.match(output(missingPin), /supplement requires its SHA-256 pin/);

  const wrongPin = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--holder-authority-supplement',
    runtimeOwnerSupplementPath,
    '--holder-authority-supplement-sha256',
    '99'.repeat(32)
  ]);
  assert.notEqual(wrongPin.status, 0);
  assert.match(output(wrongPin), /supplement SHA-256 pin does not match/);
});

test('merges the pinned 38-row SPYx runtime-owner snapshot with exact totals', async () => {
  outputSequence += 1;
  const summaryPath = join(temporary, `real-summary-${outputSequence}.json`);
  const programSummaryPath = join(temporary, `real-programs-${outputSequence}.json`);
  const authorityPortfolioPath = join(temporary, `real-authority-portfolios-${outputSequence}.json`);
  const tokenReportRoot = resolve(repositoryRoot, 'benchmark-results/spyx-token-report-v1');
  const realHistoryPath = resolve(tokenReportRoot, 'token-history-report.json');
  const realProgramPath = resolve(
    repositoryRoot,
    'benchmark-results/spyx-program-identification-v1/program-identification-report.json'
  );
  const realCpiPath = programCpiFixture(realProgramPath, realHistoryPath);
  const result = spawnSync(process.execPath, [
    builder,
    '--history',
    realHistoryPath,
    '--history-sha256',
    '06af65b65e3cd77b65dd8ec7d0ede0b0c0ac60d727ed6b3ff1a9c7bad8b8d2db',
    '--strict-replay',
    resolve(tokenReportRoot, 'spyx-replay-holder-volume-full-20dea675.json'),
    '--strict-replay-sha256',
    '2933b837ccfc5cb4551f13f089790552c4a409113b6509ff1e654376360ff841',
    '--programs',
    realProgramPath,
    '--programs-sha256',
    '066397944a0bc8596ad20056320d1a900d1aeb4a9893caeea03a010ac3536d3c',
    '--program-cpi-inventory',
    realCpiPath,
    '--program-cpi-inventory-sha256',
    fixtureSha256.get(realCpiPath),
    '--holder-authority-supplement',
    resolve(tokenReportRoot, 'holder-authority-runtime-owner-snapshot-442934356.json'),
    '--holder-authority-supplement-sha256',
    '4e518c66542ab71dbc0339788ff7fe2454462af872b4fd3441161e63d38b135b',
    '--output',
    summaryPath,
    '--program-output',
    programSummaryPath,
    '--authority-portfolio-output',
    authorityPortfolioPath
  ], { cwd: appRoot, encoding: 'utf8' });
  assert.equal(result.status, 0, output(result));
  const summary = JSON.parse(await readFile(summaryPath, 'utf8'));
  const authority = summary.final_public_balance.holder_authority;
  const overlay = authority.attribution_supplements[0];
  assert.deepEqual(
    overlay.counts,
    {
      accounts: 38,
      present_accounts: 35,
      absent_accounts: 3,
      runtime_owner_programs: 14,
      attributed_custom_program_runtime_owner: 24,
      not_attributed_account_missing: 3,
      not_attributed_system_program: 10,
      not_attributed_token_program: 1,
      not_attributed_executable_account: 0
    }
  );
  assert.deepEqual(overlay.coverage, {
    complete_for_all_off_curve_unattributed_holders: false,
    replay_off_curve_unattributed_holder_count: 388,
    queried_holder_count: 38,
    unqueried_holder_count: 350,
    observed_holder_count: 35,
    unobserved_holder_count: 353
  });
  assert.equal(overlay.totals.observed.public_balance.raw_amount, '1658694592676');
  assert.equal(overlay.totals.attributed_custom_program.public_balance.raw_amount, '1637876262971');
  assert.equal(overlay.totals.not_attributed.public_balance.raw_amount, '20818329705');
  assert.equal(
    overlay.holdings_by_program.some(
      (row) => row.program_id === '11111111111111111111111111111111'
    ),
    true
  );
  assert.equal(
    overlay.holdings_by_program.some(
      (row) => row.program_id === 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
    ),
    true
  );
  assert.equal(overlay.holdings_by_program.some((row) => row.program_name === null), true);
  const target = authority.largest_25_all.find(
    (row) => row.owner === '7s1da8DduuBFqGra5bJBjpnvL5E9mGzCuMk1Qkh4or2Z'
  );
  assert.equal(
    target.supplemental_program_attribution.runtime_owner_program_name,
    'Jupiter Lend (Liquidity)'
  );
  assert.equal(
    target.supplemental_program_attribution.account_label,
    'Jupiter Lend: Supply Vault Owner'
  );
  const offCurveTotal = authority.class_totals.find(
    (row) => row.authority_kind === 'off_curve_unattributed'
  );
  assert.deepEqual(offCurveTotal, {
    authority_kind: 'off_curve_unattributed',
    holder_count: 388,
    token_account_count: 393,
    public_balance: amount('1658991318094', '16589.91318094')
  });
  assert.equal(
    authority.class_totals.reduce((sum, row) => sum + row.holder_count, 0),
    summary.final_public_balance.positive_public_balance_holders
  );
  assert.equal(
    authority.class_totals.reduce((sum, row) => sum + row.token_account_count, 0),
    summary.final_public_balance.active_public_token_accounts
  );
  assert.equal(
    authority.class_totals.reduce(
      (sum, row) => sum + BigInt(row.public_balance.raw_amount),
      0n
    ),
    BigInt(summary.final_public_balance.public_raw_balance_sum.raw_amount)
  );
});

test('accepts an attributed PDA program without label metadata', () => {
  assert.equal(programs.programs.length, 0);
  assert.equal(
    completeReplay.holder_authority.holdings_by_program[0].program_id,
    'program-a'
  );
  const result = runBuilder(['--strict-replay', completeReplayPath]);
  assert.equal(result.status, 0, output(result));
});

test('rejects holder authority class totals that do not reconcile with final balances', async () => {
  const mismatch = structuredClone(completeReplay);
  mismatch.holder_authority.class_totals[0].public_balance = amount('99', '0.00000099');
  const mismatchPath = await fixture('replay-holder-total-mismatch.json', mismatch);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /holder_authority final public balance does not match/);
});

for (const [name, mutate, expectedError] of [
  [
    'token-account total',
    (row) => {
      row.token_account_count = 2;
    },
    /complete off-curve token-account total does not match/
  ],
  [
    'public-balance total',
    (row) => {
      row.public_balance = amount('4', '0.00000004');
    },
    /complete off-curve public-balance total does not match/
  ]
]) {
  test(`rejects a complete off-curve holder array with a mismatched ${name}`, async () => {
    const mismatch = structuredClone(completeReplay);
    const row = structuredClone(mismatch.holder_authority.largest_25_all[2]);
    mutate(row);
    mismatch.holder_authority.off_curve_unattributed_holders = [row];
    const mismatchPath = await fixture(
      `replay-complete-off-curve-${name.replaceAll(' ', '-')}.json`,
      mismatch
    );
    const result = runBuilder(['--strict-replay', mismatchPath]);
    assert.notEqual(result.status, 0);
    assert.match(output(result), expectedError);
  });
}

test('rejects an attributed PDA row without program evidence', async () => {
  const mismatch = structuredClone(completeReplay);
  mismatch.holder_authority.largest_25_all[1].pda_program_evidence_count = 0;
  mismatch.holder_authority.largest_25_by_class.attributed_program_derived_address[0]
    .pda_program_evidence_count = 0;
  const mismatchPath = await fixture('replay-pda-without-evidence.json', mismatch);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /has invalid attributed-PDA evidence/);
});

test('rejects an incomplete holder activity extension', async () => {
  const mismatch = structuredClone(completeReplay);
  delete mismatch.holder_authority.attributed_program_holders;
  const mismatchPath = await fixture('replay-incomplete-holder-activity.json', mismatch);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /incomplete public activity extension/);
});

test('rejects a program activity volume that differs from increase plus decrease', async () => {
  const mismatch = structuredClone(completeReplay);
  mismatch.holder_authority.holdings_by_program[0].public_activity_volume = amount(
    '61',
    '0.00000061'
  );
  const mismatchPath = await fixture('replay-program-activity-mismatch.json', mismatch);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /public activity volume does not match/);
});

test('rejects a holder assigned to the wrong program aggregate', async () => {
  const mismatch = structuredClone(completeReplay);
  mismatch.holder_authority.attributed_program_holders[0].pda_program_id = 'program-b';
  const mismatchPath = await fixture('replay-holder-program-aggregate-mismatch.json', mismatch);
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /holdings_by_program is missing program program-b/);
});

test('rejects a complete replay that was not implemented', async () => {
  const mismatchPath = await fixture('replay-not-implemented.json', {
    ...completeReplay,
    instruction_replay_implemented: false
  });
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /must have instruction replay implemented/);
});

for (const field of ['replay_errors', 'oracle_pre_mismatches', 'oracle_post_mismatches']) {
  test(`rejects a complete replay with non-zero ${field}`, async () => {
    const mismatchPath = await fixture(`replay-nonzero-${field}.json`, {
      ...completeReplay,
      counters: { ...completeReplay.counters, [field]: 1 }
    });
    const result = runBuilder(['--strict-replay', mismatchPath]);
    assert.notEqual(result.status, 0);
    assert.match(output(result), new RegExp(`non-zero ${field}`));
  });
}

test('rejects a complete replay with a blocker', async () => {
  const mismatchPath = await fixture('replay-with-blocker.json', {
    ...completeReplay,
    blockers: { test_blocker: 1 }
  });
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /must have no blockers/);
});

test('rejects a complete replay with a first failure', async () => {
  const mismatchPath = await fixture('replay-with-first-failure.json', {
    ...completeReplay,
    first_failure: {
      source_epoch: 801,
      slot: 346_066_299,
      source_block_id: 1,
      tx_index: 0,
      phase: 'test',
      code: 'test_failure',
      detail: 'test failure detail',
      outer_index: null,
      inner_index: null
    }
  });
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /must have no first failure/);
});

for (const [field, value] of [
  ['transactions_scanned', 11],
  ['replay_transactions_applied', 11],
  ['pre_target_oracle_rows', 19],
  ['oracle_post_rows_compared', 20]
]) {
  test(`rejects a complete replay with a mismatched ${field}`, async () => {
    const mismatchPath = await fixture(`replay-mismatch-${field}.json`, {
      ...completeReplay,
      counters: { ...completeReplay.counters, [field]: value }
    });
    const result = runBuilder(['--strict-replay', mismatchPath]);
    assert.notEqual(result.status, 0);
    assert.match(output(result), new RegExp(`strict replay ${field} does not match`));
  });
}

for (const [name, statePatch, expectedError] of [
  [
    'incomplete replay state',
    { history_complete: false },
    /complete_match strict replay report has an incomplete replayed state/
  ],
  [
    'tracked-account mismatch',
    { tracked_accounts: 16, closed_accounts: 11 },
    /strict replay tracked account count does not match/
  ],
  [
    'positive-account mismatch',
    { positive_public_balance_accounts: 3 },
    /strict replay positive-balance account count does not match/
  ],
  [
    'raw-balance mismatch',
    { public_raw_balance: '124' },
    /strict replay public raw balance does not match/
  ]
]) {
  test(`rejects a complete replay with ${name}`, async () => {
    const mismatchPath = await fixture(`replay-${name.replaceAll(' ', '-')}.json`, {
      ...completeReplay,
      replayed_state: { ...completeReplay.replayed_state, ...statePatch }
    });
    const result = runBuilder(['--strict-replay', mismatchPath]);
    assert.notEqual(result.status, 0);
    assert.match(output(result), expectedError);
  });
}

test('requires the replayed-state data contract for attached reports', async () => {
  const { replayed_state: omitted, ...withoutState } = replay;
  assert.ok(omitted);
  const missingPath = await fixture('replay-without-state.json', withoutState);
  const result = runBuilder(['--strict-replay', missingPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /Expected strict replay replayed_state to be an object/);
});

test('requires replay for a release validation', () => {
  const result = runBuilder([]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /release data build requires/);
});

test('requires an exact history SHA-256 pin for a release validation', () => {
  const result = runBuilder(
    ['--strict-replay', completeReplayPath],
    true,
    programsPath,
    historyPath,
    { history: false, replay: true }
  );
  assert.notEqual(result.status, 0);
  assert.match(output(result), /requires --history-sha256 or SPYX_HISTORY_REPORT_SHA256/);
});

test('requires an exact replay SHA-256 pin for a release validation', () => {
  const result = runBuilder(
    ['--strict-replay', completeReplayPath],
    true,
    programsPath,
    historyPath,
    { history: true, replay: false }
  );
  assert.notEqual(result.status, 0);
  assert.match(
    output(result),
    /requires --strict-replay-sha256 or SPYX_STRICT_REPLAY_REPORT_SHA256/
  );
});

test('requires an exact program report SHA-256 pin for a release validation', () => {
  const result = runBuilder(
    ['--strict-replay', completeReplayPath],
    true,
    programsPath,
    historyPath,
    { programs: false }
  );
  assert.notEqual(result.status, 0);
  assert.match(output(result), /requires --programs-sha256 or SPYX_PROGRAM_REPORT_SHA256/);
});

test('rejects a same-size middle change to the pinned history bytes', async () => {
  const changed = structuredClone(history);
  changed.limitations.public_scope = 'test qublic metadata limit';
  assert.equal(changed.limitations.public_scope.length, history.limitations.public_scope.length);
  const changedPath = await fixture('history-middle-byte-changed.json', changed);
  const result = runBuilder(
    [
      '--strict-replay',
      completeReplayPath,
      '--history-sha256',
      fixtureSha256.get(historyPath)
    ],
    true,
    programsPath,
    changedPath
  );
  assert.notEqual(result.status, 0);
  assert.match(output(result), /history report SHA-256 pin does not match/);
});

test('allows an intentional development validation without replay', () => {
  const result = runBuilder([], false);
  assert.equal(result.status, 0, output(result));
});

test('clears stale authority outputs when a development build has no strict replay', async () => {
  outputSequence += 1;
  const outputRoot = join(temporary, `stale-authority-output-${outputSequence}`);
  const summaryPath = join(outputRoot, 'spyx-summary.json');
  const programSummaryPath = join(outputRoot, 'spyx-programs.json');
  const authorityPortfolioPath = join(outputRoot, 'spyx-authority-portfolios.json');
  const authorityPortfolioTablePath = join(outputRoot, 'spyx-authority-portfolios-table.json');
  const authorityPortfolioHistoryPath = join(
    outputRoot,
    'spyx-authority-portfolio-history-index.json'
  );
  const pdaAuthorityEstimatePath = join(outputRoot, 'spyx-pda-authority-estimates.json');
  const authorityPortfolioShardDirectory = join(
    outputRoot,
    'spyx-authority-portfolios-by-prefix'
  );
  const authorityPortfolioHistoryShardDirectory = join(
    outputRoot,
    'spyx-authority-portfolio-history-by-prefix'
  );
  await Promise.all([
    mkdir(authorityPortfolioShardDirectory, { recursive: true }),
    mkdir(authorityPortfolioHistoryShardDirectory, { recursive: true })
  ]);
  await Promise.all([
    writeFile(authorityPortfolioPath, '{"stale":true}\n'),
    writeFile(authorityPortfolioTablePath, '{"stale":true}\n'),
    writeFile(authorityPortfolioHistoryPath, '{"stale":true}\n'),
    writeFile(pdaAuthorityEstimatePath, '{"stale":true}\n'),
    writeFile(join(authorityPortfolioShardDirectory, 'stale.json'), '{}\n'),
    writeFile(join(authorityPortfolioHistoryShardDirectory, 'stale.json'), '{}\n')
  ]);

  const selectedCpi = programCpiFixture(programsPath, historyPath);
  const result = spawnSync(
    process.execPath,
    [
      builder,
      '--history',
      historyPath,
      '--history-sha256',
      fixtureSha256.get(historyPath),
      '--programs',
      programsPath,
      '--programs-sha256',
      fixtureSha256.get(programsPath),
      '--program-cpi-inventory',
      selectedCpi,
      '--program-cpi-inventory-sha256',
      fixtureSha256.get(selectedCpi),
      '--output',
      summaryPath,
      '--program-output',
      programSummaryPath,
      '--authority-portfolio-output',
      authorityPortfolioPath,
      '--authority-portfolio-table-output',
      authorityPortfolioTablePath,
      '--authority-portfolio-history-output',
      authorityPortfolioHistoryPath,
      '--pda-authority-estimate-output',
      pdaAuthorityEstimatePath
    ],
    {
      cwd: appRoot,
      encoding: 'utf8',
      env: {
        ...process.env,
        SPYX_HISTORY_REPORT_SHA256: '',
        SPYX_STRICT_REPLAY_REPORT: '',
        SPYX_STRICT_REPLAY_REPORT_SHA256: '',
        SPYX_PROGRAM_REPORT_SHA256: '',
        SPYX_PROGRAM_CPI_INVENTORY: '',
        SPYX_PROGRAM_CPI_INVENTORY_SHA256: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256: '',
        SPYX_AUTHORITY_PORTFOLIO_OUTPUT: '',
        SPYX_AUTHORITY_PORTFOLIO_HISTORY_OUTPUT: ''
      }
    }
  );
  assert.equal(result.status, 0, output(result));

  const summary = JSON.parse(await readFile(summaryPath, 'utf8'));
  assert.equal(JSON.parse(await readFile(authorityPortfolioPath, 'utf8')), null);
  assert.equal(JSON.parse(await readFile(authorityPortfolioTablePath, 'utf8')), null);
  assert.equal(JSON.parse(await readFile(authorityPortfolioHistoryPath, 'utf8')), null);
  assert.equal(JSON.parse(await readFile(pdaAuthorityEstimatePath, 'utf8')), null);
  await assert.rejects(() => stat(authorityPortfolioShardDirectory), { code: 'ENOENT' });
  await assert.rejects(() => stat(authorityPortfolioHistoryShardDirectory), { code: 'ENOENT' });
  assert.equal(summary.compact_build.authority_portfolios_available, false);
  assert.equal(summary.compact_build.authority_portfolio_table_available, false);
  assert.equal(summary.compact_build.authority_portfolio_history_available, false);
});

test('rejects a strict replay from a different account artifact', async () => {
  const mismatchPath = await fixture('replay-account-mismatch.json', {
    ...replay,
    source: { ...replay.source, accounts_sha256: '77'.repeat(32) }
  });
  const result = runBuilder(['--strict-replay', mismatchPath]);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /strict replay accounts SHA-256 does not match/);
});

test('rejects a program report from a different transaction stream', async () => {
  const mismatchPath = await fixture('program-transaction-mismatch.json', {
    ...programs,
    source: { ...programs.source, dump_transaction_stream_sha256: '88'.repeat(32) }
  });
  const result = runBuilder(['--strict-replay', completeReplayPath], true, mismatchPath);
  assert.notEqual(result.status, 0);
  assert.match(output(result), /program report transaction SHA-256 does not match/);
});

test('requires a complete program report for a release validation', async () => {
  const incompletePath = await fixture('program-incomplete.json', {
    ...programs,
    complete: false
  });
  const result = runBuilder(
    ['--strict-replay', completeReplayPath],
    true,
    incompletePath
  );
  assert.notEqual(result.status, 0);
  assert.match(output(result), /release data build requires a complete program report/);
});

test('rejects duplicate program IDs', async () => {
  const duplicate = structuredClone(populatedPrograms);
  duplicate.programs.push({
    ...structuredClone(identifiedProgram),
    rank: 2,
    registry_id: 43
  });
  const duplicatePath = await fixture('program-duplicate-id.json', duplicate);
  const result = runBuilder(
    ['--strict-replay', completeReplayPath],
    true,
    duplicatePath
  );
  assert.notEqual(result.status, 0);
  assert.match(output(result), /duplicate program ID fixture-program/);
});

test('rejects an incomplete program row', async () => {
  const incomplete = structuredClone(populatedPrograms);
  delete incomplete.programs[0].transactions;
  const incompletePath = await fixture('program-row-incomplete.json', incomplete);
  const result = runBuilder(
    ['--strict-replay', completeReplayPath],
    true,
    incompletePath
  );
  assert.notEqual(result.status, 0);
  assert.match(output(result), /programs\[0\]\.transactions is not a non-negative safe integer/);
});

test('rejects inconsistent direct, inner, and total program occurrences', async () => {
  const mismatch = structuredClone(populatedPrograms);
  mismatch.programs[0].total_occurrences = 4;
  const mismatchPath = await fixture('program-scope-occurrence-mismatch.json', mismatch);
  const result = runBuilder(
    ['--strict-replay', completeReplayPath],
    true,
    mismatchPath
  );
  assert.notEqual(result.status, 0);
  assert.match(output(result), /total_occurrences does not equal outer plus inner occurrences/);
});

test('rejects program aggregate counters that differ from the rows', async () => {
  const mismatch = structuredClone(populatedPrograms);
  mismatch.counters.identified_inner_occurrences = 3;
  const mismatchPath = await fixture('program-counter-mismatch.json', mismatch);
  const result = runBuilder(
    ['--strict-replay', completeReplayPath],
    true,
    mismatchPath
  );
  assert.notEqual(result.status, 0);
  assert.match(
    output(result),
    /counters\.identified_inner_occurrences does not match its program rows/
  );
});

test('checks an optional exact program report pin', () => {
  const accepted = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--programs-sha256',
    fixtureSha256.get(programsPath)
  ]);
  assert.equal(accepted.status, 0, output(accepted));

  const rejected = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--programs-sha256',
    '99'.repeat(32)
  ]);
  assert.notEqual(rejected.status, 0);
  assert.match(output(rejected), /program report SHA-256 pin does not match/);
});

test('accepts the release program report pin from the environment', () => {
  const selectedCpi = programCpiFixture(programsPath, historyPath);
  const result = spawnSync(
    process.execPath,
    [
      builder,
      '--history',
      historyPath,
      '--history-sha256',
      fixtureSha256.get(historyPath),
      '--strict-replay',
      completeReplayPath,
      '--strict-replay-sha256',
      fixtureSha256.get(completeReplayPath),
      '--programs',
      programsPath,
      '--program-cpi-inventory',
      selectedCpi,
      '--validate-only',
      '--require-strict-replay'
    ],
    {
      cwd: appRoot,
      encoding: 'utf8',
      env: {
        ...process.env,
        SPYX_PROGRAM_REPORT_SHA256: fixtureSha256.get(programsPath),
        SPYX_PROGRAM_CPI_INVENTORY_SHA256: fixtureSha256.get(selectedCpi),
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256: '',
        SPYX_AUTHORITY_PORTFOLIO_OUTPUT: '',
        SPYX_AUTHORITY_PORTFOLIO_HISTORY_OUTPUT: ''
      }
    }
  );
  assert.equal(result.status, 0, output(result));
});

test('checks an optional exact replay report pin', () => {
  const bytes = JSON.stringify(completeReplay);
  const digest = createHash('sha256').update(`${bytes}\n`).digest('hex');
  const accepted = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--strict-replay-sha256',
    digest
  ]);
  assert.equal(accepted.status, 0, output(accepted));

  const rejected = runBuilder([
    '--strict-replay',
    completeReplayPath,
    '--strict-replay-sha256',
    '99'.repeat(32)
  ]);
  assert.notEqual(rejected.status, 0);
  assert.match(output(rejected), /strict replay report SHA-256 pin does not match/);
});

test('valid CPI merge exposes target counters and retains an unlabeled program', async () => {
  const unlabeledPrograms = structuredClone(populatedPrograms);
  unlabeledPrograms.programs.push({
    rank: 2,
    registry_id: 43,
    program_id: 'unlabeled-program',
    identity_status: 'unidentified',
    selected_name: null,
    selected_source: null,
    selected_confidence: null,
    usable_onchain_idl: false,
    address_clean_onchain_idl: false,
    decoder_source_found: false,
    total_occurrences: 2,
    outer_occurrences: 0,
    inner_occurrences: 2,
    transactions: 1,
    evidence: []
  });
  unlabeledPrograms.counters = {
    ...unlabeledPrograms.counters,
    programs_total: 2,
    programs_identified: 1,
    programs_unidentified: 1,
    identified_program_ratio: 0.5,
    instruction_occurrences_total: 5,
    identified_instruction_occurrences: 3,
    unidentified_instruction_occurrences: 2,
    identified_instruction_occurrence_ratio: 0.6
  };
  const unlabeledProgramsPath = await fixture(
    'programs-with-unlabeled-cpi.json',
    unlabeledPrograms
  );
  const selectedCpi = programCpiFixture(unlabeledProgramsPath, historyPath);
  const { result, programSummary } = await runProgramBuilderWithOutput(
    unlabeledProgramsPath,
    selectedCpi
  );
  assert.equal(result.status, 0, output(result));
  assert.deepEqual(programSummary.target_account_cpi.counters, {
    target_account_inner_occurrences: 2,
    target_account_inner_transactions: 2,
    target_mint_inner_occurrences: 0,
    target_token_account_inner_occurrences: 2,
    target_account_inner_references: 2,
    target_mint_inner_references: 0,
    target_token_account_inner_references: 2,
    transactions_with_target_account_inner_instructions: 1,
    programs_with_target_account_inner_instructions: 2
  });
  assert.deepEqual(
    programSummary.programs.map((program) => program.program_id),
    ['fixture-program', 'unlabeled-program']
  );
  const unlabeled = programSummary.programs[1];
  assert.equal(unlabeled.selected_name, null);
  assert.equal(unlabeled.identity_status, 'unidentified');
  assert.equal(unlabeled.target_account_inner_occurrences, 1);
});

test('program label changes do not affect CPI inclusion order or counts', async () => {
  const selectedCpi = programCpiFixture(populatedProgramsPath, historyPath);
  const renamedPrograms = structuredClone(populatedPrograms);
  renamedPrograms.programs[0].selected_name = 'Renamed fixture program';
  renamedPrograms.programs[0].evidence[0].name = 'Renamed fixture program';
  const renamedProgramsPath = await fixture('programs-renamed-cpi.json', renamedPrograms);

  const baseline = await runProgramBuilderWithOutput(populatedProgramsPath, selectedCpi);
  const renamed = await runProgramBuilderWithOutput(renamedProgramsPath, selectedCpi);

  assert.equal(baseline.result.status, 0, output(baseline.result));
  assert.equal(renamed.result.status, 0, output(renamed.result));
  assert.deepEqual(
    baseline.programSummary.target_account_cpi,
    renamed.programSummary.target_account_cpi
  );
  assert.deepEqual(
    baseline.programSummary.programs.map((program) => ({
      program_id: program.program_id,
      rank: program.rank,
      target_account_inner_occurrences: program.target_account_inner_occurrences,
      target_account_inner_transactions: program.target_account_inner_transactions,
      target_mint_inner_occurrences: program.target_mint_inner_occurrences,
      target_token_account_inner_occurrences: program.target_token_account_inner_occurrences,
      target_account_inner_references: program.target_account_inner_references,
      target_mint_inner_references: program.target_mint_inner_references,
      target_token_account_inner_references: program.target_token_account_inner_references
    })),
    renamed.programSummary.programs.map((program) => ({
      program_id: program.program_id,
      rank: program.rank,
      target_account_inner_occurrences: program.target_account_inner_occurrences,
      target_account_inner_transactions: program.target_account_inner_transactions,
      target_mint_inner_occurrences: program.target_mint_inner_occurrences,
      target_token_account_inner_occurrences: program.target_token_account_inner_occurrences,
      target_account_inner_references: program.target_account_inner_references,
      target_mint_inner_references: program.target_mint_inner_references,
      target_token_account_inner_references: program.target_token_account_inner_references
    }))
  );
});

test('rejects a wrong exact CPI inventory SHA-256 pin', () => {
  const selectedCpi = programCpiFixture(populatedProgramsPath, historyPath);
  const rejected = runBuilder([
    '--programs',
    populatedProgramsPath,
    '--program-cpi-inventory',
    selectedCpi,
    '--program-cpi-inventory-sha256',
    '00'.repeat(32)
  ], false, populatedProgramsPath);
  assert.notEqual(rejected.status, 0);
  assert.match(output(rejected), /program CPI inventory SHA-256 pin does not match/);
});

test('rejects a CPI row with a target counter above inner occurrences', async () => {
  const selectedCpi = programCpiFixture(populatedProgramsPath, historyPath);
  const mismatch = JSON.parse(readFileSync(selectedCpi, 'utf8'));
  mismatch.programs[0].target_account_inner_occurrences =
    mismatch.programs[0].inner_occurrences + 1;
  mismatch.programs[0].target_token_account_inner_occurrences =
    mismatch.programs[0].target_account_inner_occurrences;
  mismatch.programs[0].target_account_inner_references =
    mismatch.programs[0].target_account_inner_occurrences;
  mismatch.programs[0].target_token_account_inner_references =
    mismatch.programs[0].target_account_inner_occurrences;
  mismatch.counters.target_account_inner_occurrences =
    mismatch.programs[0].target_account_inner_occurrences;
  mismatch.counters.target_token_account_inner_occurrences =
    mismatch.programs[0].target_token_account_inner_occurrences;
  mismatch.counters.target_account_inner_references =
    mismatch.programs[0].target_account_inner_references;
  mismatch.counters.target_token_account_inner_references =
    mismatch.programs[0].target_token_account_inner_references;
  const mismatchPath = await fixture('program-cpi-row-occurrence-mismatch.json', mismatch);
  const rejected = runBuilder([
    '--programs',
    populatedProgramsPath,
    '--program-cpi-inventory',
    mismatchPath
  ], false, populatedProgramsPath, historyPath, { cpi: true });
  assert.notEqual(rejected.status, 0);
  assert.match(
    output(rejected),
    /target_account_inner_occurrences exceeds all inner occurrences/
  );
});

test('rejects a CPI row with a base program mismatch', async () => {
  const selectedCpi = programCpiFixture(populatedProgramsPath, historyPath);
  const mismatch = JSON.parse(readFileSync(selectedCpi, 'utf8'));
  mismatch.programs[0].program_id = 'different-program';
  const mismatchPath = await fixture('program-cpi-base-program-mismatch.json', mismatch);
  const rejected = runBuilder([
    '--programs',
    populatedProgramsPath,
    '--program-cpi-inventory',
    mismatchPath
  ], false, populatedProgramsPath, historyPath, { cpi: true });
  assert.notEqual(rejected.status, 0);
  assert.match(output(rejected), /programs\[0\]\.program_id does not match the identification report/);
});

test('rejects a CPI inventory with a wrong global target counter total', async () => {
  const selectedCpi = programCpiFixture(populatedProgramsPath, historyPath);
  const mismatch = JSON.parse(readFileSync(selectedCpi, 'utf8'));
  mismatch.counters.target_account_inner_occurrences += 1;
  const mismatchPath = await fixture('program-cpi-global-counter-mismatch.json', mismatch);
  const rejected = runBuilder([
    '--programs',
    populatedProgramsPath,
    '--program-cpi-inventory',
    mismatchPath
  ], false, populatedProgramsPath, historyPath, { cpi: true });
  assert.notEqual(rejected.status, 0);
  assert.match(
    output(rejected),
    /counters\.target_account_inner_occurrences does not match its program rows/
  );
});

async function fixture(name, value) {
  const path = join(temporary, name);
  const bytes = `${JSON.stringify(value)}\n`;
  await writeFile(path, bytes);
  fixtureSha256.set(path, createHash('sha256').update(bytes).digest('hex'));
  return path;
}

function programCpiFixture(selectedPrograms, selectedHistory) {
  cpiFixtureSequence += 1;
  const programReport = JSON.parse(readFileSync(selectedPrograms, 'utf8'));
  const historyReport = JSON.parse(readFileSync(selectedHistory, 'utf8'));
  const rows = programReport.programs.map((program) => {
    const targetOccurrences = program.inner_occurrences > 0 ? 1 : 0;
    return {
      registry_id: program.registry_id,
      program_id: program.program_id,
      total_occurrences: program.total_occurrences,
      outer_occurrences: program.outer_occurrences,
      inner_occurrences: program.inner_occurrences,
      transactions: program.transactions,
      target_account_inner_occurrences: targetOccurrences,
      target_account_inner_transactions: targetOccurrences,
      target_mint_inner_occurrences: 0,
      target_token_account_inner_occurrences: targetOccurrences,
      target_account_inner_references: targetOccurrences,
      target_mint_inner_references: 0,
      target_token_account_inner_references: targetOccurrences
    };
  });
  const sum = (field) => rows.reduce((total, row) => total + row[field], 0);
  const targetFields = [
    'target_account_inner_occurrences',
    'target_account_inner_transactions',
    'target_mint_inner_occurrences',
    'target_token_account_inner_occurrences',
    'target_account_inner_references',
    'target_mint_inner_references',
    'target_token_account_inner_references'
  ];
  const inventory = {
    schema_version: 2,
    artifact_kind: 'program_inventory',
    complete: true,
    instruction_program_resolution_complete: true,
    program_order: 'total_occurrences_desc_then_raw_pubkey_asc',
    source: {
      mint: historyReport.source.mint,
      manifest_sha256: historyReport.source.manifest.sha256,
      transaction_stream_sha256: historyReport.source.transactions_file.sha256,
      pubkey_registry_sha256: historyReport.source.registry_file.sha256,
      transactions: historyReport.source.transactions,
      signatures: historyReport.source.signatures,
      registry_entries: historyReport.source.registry_entries,
      first_epoch: historyReport.source.first_epoch,
      last_epoch: historyReport.source.last_epoch,
      target_accounts: {
        file: historyReport.source.accounts_file.file,
        sha256: historyReport.source.accounts_file.sha256,
        discovered_token_accounts: historyReport.source.discovered_token_accounts,
        target_addresses: historyReport.source.discovered_token_accounts + 1,
        membership_definition: 'SPYx mint plus every discovered SPYx token account'
      }
    },
    counters: {
      ...Object.fromEntries(targetFields.map((field) => [field, sum(field)])),
      transactions_with_target_account_inner_instructions:
        rows.some((row) => row.target_account_inner_occurrences > 0) ? 1 : 0
    },
    programs: rows
  };
  const path = join(temporary, `program-cpi-${cpiFixtureSequence}.json`);
  const bytes = `${JSON.stringify(inventory)}\n`;
  writeFileSync(path, bytes);
  fixtureSha256.set(path, createHash('sha256').update(bytes).digest('hex'));
  return path;
}

function runBuilder(
  extra,
  requireReplay = true,
  selectedPrograms = programsPath,
  selectedHistory = historyPath,
  pins = {}
) {
  const selectedPins = { history: true, replay: true, programs: true, cpi: true, ...pins };
  const selectedCpi = optionValue(extra, '--program-cpi-inventory') ??
    programCpiFixture(selectedPrograms, selectedHistory);
  const arguments_ = [
    builder,
    '--history',
    selectedHistory,
    '--programs',
    selectedPrograms,
    '--program-cpi-inventory',
    selectedCpi,
    '--validate-only'
  ];
  if (requireReplay) {
    arguments_.push('--require-strict-replay');
    if (selectedPins.history && !hasOption(extra, '--history-sha256')) {
      arguments_.push('--history-sha256', fixtureSha256.get(selectedHistory));
    }
    const selectedReplay = optionValue(extra, '--strict-replay');
    if (
      selectedPins.replay &&
      selectedReplay &&
      !hasOption(extra, '--strict-replay-sha256')
    ) {
      arguments_.push('--strict-replay-sha256', fixtureSha256.get(selectedReplay));
    }
    if (selectedPins.programs && !hasOption(extra, '--programs-sha256')) {
      arguments_.push('--programs-sha256', fixtureSha256.get(selectedPrograms));
    }
    if (selectedPins.cpi && !hasOption(extra, '--program-cpi-inventory-sha256')) {
      arguments_.push('--program-cpi-inventory-sha256', fixtureSha256.get(selectedCpi));
    }
  }
  arguments_.push(...extra);
  return spawnSync(process.execPath, arguments_, {
    cwd: appRoot,
    encoding: 'utf8',
    env: {
      ...process.env,
      SPYX_HISTORY_REPORT_SHA256: '',
      SPYX_STRICT_REPLAY_REPORT: '',
      SPYX_STRICT_REPLAY_REPORT_SHA256: '',
      SPYX_PROGRAM_REPORT_SHA256: '',
      SPYX_PROGRAM_CPI_INVENTORY: '',
      SPYX_PROGRAM_CPI_INVENTORY_SHA256: '',
      SPYX_HOLDER_AUTHORITY_SUPPLEMENT: '',
      SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256: '',
      SPYX_AUTHORITY_PORTFOLIO_OUTPUT: '',
      SPYX_AUTHORITY_PORTFOLIO_HISTORY_OUTPUT: ''
    }
  });
}

async function runHistoryBuilderWithOutput(selectedHistory) {
  outputSequence += 1;
  const selectedCpi = programCpiFixture(programsPath, selectedHistory);
  const summaryPath = join(temporary, `history-summary-${outputSequence}.json`);
  const programSummaryPath = join(
    temporary,
    `history-program-summary-${outputSequence}.json`
  );
  const authorityPortfolioPath = join(
    temporary,
    `history-authority-portfolio-${outputSequence}.json`
  );
  const result = spawnSync(
    process.execPath,
    [
      builder,
      '--history',
      selectedHistory,
      '--history-sha256',
      fixtureSha256.get(selectedHistory),
      '--programs',
      programsPath,
      '--programs-sha256',
      fixtureSha256.get(programsPath),
      '--program-cpi-inventory',
      selectedCpi,
      '--program-cpi-inventory-sha256',
      fixtureSha256.get(selectedCpi),
      '--output',
      summaryPath,
      '--program-output',
      programSummaryPath,
      '--authority-portfolio-output',
      authorityPortfolioPath
    ],
    {
      cwd: appRoot,
      encoding: 'utf8',
      env: {
        ...process.env,
        SPYX_HISTORY_REPORT_SHA256: '',
        SPYX_STRICT_REPLAY_REPORT: '',
        SPYX_STRICT_REPLAY_REPORT_SHA256: '',
        SPYX_PROGRAM_REPORT_SHA256: '',
        SPYX_PROGRAM_CPI_INVENTORY: '',
        SPYX_PROGRAM_CPI_INVENTORY_SHA256: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256: '',
        SPYX_AUTHORITY_PORTFOLIO_OUTPUT: '',
        SPYX_AUTHORITY_PORTFOLIO_HISTORY_OUTPUT: ''
      }
    }
  );
  const summary = result.status === 0
    ? JSON.parse(await readFile(summaryPath, 'utf8'))
    : null;
  return { result, summary };
}

async function runProgramBuilderWithOutput(
  selectedPrograms = populatedProgramsPath,
  selectedCpi = programCpiFixture(selectedPrograms, historyPath)
) {
  outputSequence += 1;
  const summaryPath = join(temporary, `program-only-summary-${outputSequence}.json`);
  const programSummaryPath = join(
    temporary,
    `program-only-program-summary-${outputSequence}.json`
  );
  const authorityPortfolioPath = join(
    temporary,
    `program-only-authority-portfolio-${outputSequence}.json`
  );
  const result = spawnSync(
    process.execPath,
    [
      builder,
      '--history',
      historyPath,
      '--history-sha256',
      fixtureSha256.get(historyPath),
      '--programs',
      selectedPrograms,
      '--programs-sha256',
      fixtureSha256.get(selectedPrograms),
      '--program-cpi-inventory',
      selectedCpi,
      '--program-cpi-inventory-sha256',
      fixtureSha256.get(selectedCpi),
      '--output',
      summaryPath,
      '--program-output',
      programSummaryPath,
      '--authority-portfolio-output',
      authorityPortfolioPath
    ],
    {
      cwd: appRoot,
      encoding: 'utf8',
      env: {
        ...process.env,
        SPYX_HISTORY_REPORT_SHA256: '',
        SPYX_STRICT_REPLAY_REPORT: '',
        SPYX_STRICT_REPLAY_REPORT_SHA256: '',
        SPYX_PROGRAM_REPORT_SHA256: '',
        SPYX_PROGRAM_CPI_INVENTORY: '',
        SPYX_PROGRAM_CPI_INVENTORY_SHA256: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256: '',
        SPYX_AUTHORITY_PORTFOLIO_OUTPUT: '',
        SPYX_AUTHORITY_PORTFOLIO_HISTORY_OUTPUT: ''
      }
    }
  );
  const programSummary = result.status === 0
    ? JSON.parse(await readFile(programSummaryPath, 'utf8'))
    : null;
  return { result, programSummary };
}

async function runReplayBuilderWithOutput(
  selectedReplay,
  selectedPrograms = programsPath,
  requireReplay = true
) {
  outputSequence += 1;
  const selectedCpi = programCpiFixture(selectedPrograms, historyPath);
  const summaryPath = join(temporary, `replay-summary-${outputSequence}.json`);
  const programSummaryPath = join(temporary, `replay-program-summary-${outputSequence}.json`);
  const authorityPortfolioPath = join(
    temporary,
    `replay-authority-portfolio-${outputSequence}.json`
  );
  const authorityPortfolioHistoryPath = join(
    temporary,
    `replay-authority-portfolio-history-${outputSequence}.json`
  );
  const authorityPortfolioTablePath = authorityPortfolioPath.replace(/\.json$/i, '-table.json');
  const result = spawnSync(
    process.execPath,
    [
      builder,
      '--history',
      historyPath,
      '--history-sha256',
      fixtureSha256.get(historyPath),
      '--strict-replay',
      selectedReplay,
      '--strict-replay-sha256',
      fixtureSha256.get(selectedReplay),
      '--programs',
      selectedPrograms,
      '--programs-sha256',
      fixtureSha256.get(selectedPrograms),
      '--program-cpi-inventory',
      selectedCpi,
      '--program-cpi-inventory-sha256',
      fixtureSha256.get(selectedCpi),
      '--output',
      summaryPath,
      '--program-output',
      programSummaryPath,
      '--authority-portfolio-output',
      authorityPortfolioPath,
      '--authority-portfolio-history-output',
      authorityPortfolioHistoryPath,
      ...(requireReplay ? ['--require-strict-replay'] : [])
    ],
    {
      cwd: appRoot,
      encoding: 'utf8',
      env: {
        ...process.env,
        SPYX_HISTORY_REPORT_SHA256: '',
        SPYX_STRICT_REPLAY_REPORT: '',
        SPYX_STRICT_REPLAY_REPORT_SHA256: '',
        SPYX_PROGRAM_REPORT_SHA256: '',
        SPYX_PROGRAM_CPI_INVENTORY: '',
        SPYX_PROGRAM_CPI_INVENTORY_SHA256: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT: '',
        SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256: '',
        SPYX_AUTHORITY_PORTFOLIO_OUTPUT: '',
        SPYX_AUTHORITY_PORTFOLIO_HISTORY_OUTPUT: ''
      }
    }
  );
  const summary = result.status === 0
    ? JSON.parse(await readFile(summaryPath, 'utf8'))
    : null;
  const authorityPortfolios = result.status === 0
    ? JSON.parse(await readFile(authorityPortfolioPath, 'utf8'))
    : null;
  const authorityPortfolioHistory = result.status === 0
    ? JSON.parse(await readFile(authorityPortfolioHistoryPath, 'utf8'))
    : null;
  const authorityPortfolioTable = result.status === 0
    ? JSON.parse(await readFile(authorityPortfolioTablePath, 'utf8'))
    : null;
  return {
    result,
    summary,
    authorityPortfolios,
    authorityPortfolioTable,
    authorityPortfolioPath,
    authorityPortfolioHistory,
    authorityPortfolioHistoryPath
  };
}

async function runBuilderWithOutput(
  supplementPath,
  replayPath = completeReplayPath,
  selectedPrograms = programsPath
) {
  outputSequence += 1;
  const selectedCpi = programCpiFixture(selectedPrograms, historyPath);
  const summaryPath = join(temporary, `summary-${outputSequence}.json`);
  const programSummaryPath = join(temporary, `program-summary-${outputSequence}.json`);
  const authorityPortfolioPath = join(
    temporary,
    `authority-portfolio-${outputSequence}.json`
  );
  const arguments_ = [
    builder,
    '--history',
    historyPath,
    '--history-sha256',
    fixtureSha256.get(historyPath),
    '--strict-replay',
    replayPath,
    '--strict-replay-sha256',
    fixtureSha256.get(replayPath),
    '--programs',
    selectedPrograms,
    '--programs-sha256',
    fixtureSha256.get(selectedPrograms),
    '--program-cpi-inventory',
    selectedCpi,
    '--program-cpi-inventory-sha256',
    fixtureSha256.get(selectedCpi),
    '--holder-authority-supplement',
    supplementPath,
    '--holder-authority-supplement-sha256',
    fixtureSha256.get(supplementPath),
    '--output',
    summaryPath,
    '--program-output',
    programSummaryPath,
    '--authority-portfolio-output',
    authorityPortfolioPath,
    '--require-strict-replay'
  ];
  const result = spawnSync(process.execPath, arguments_, {
    cwd: appRoot,
    encoding: 'utf8',
    env: {
      ...process.env,
      SPYX_HISTORY_REPORT_SHA256: '',
      SPYX_STRICT_REPLAY_REPORT: '',
      SPYX_STRICT_REPLAY_REPORT_SHA256: '',
      SPYX_PROGRAM_REPORT_SHA256: '',
      SPYX_PROGRAM_CPI_INVENTORY: '',
      SPYX_PROGRAM_CPI_INVENTORY_SHA256: '',
      SPYX_HOLDER_AUTHORITY_SUPPLEMENT: '',
      SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256: '',
      SPYX_AUTHORITY_PORTFOLIO_OUTPUT: '',
      SPYX_AUTHORITY_PORTFOLIO_HISTORY_OUTPUT: ''
    }
  });
  const summary = result.status === 0
    ? JSON.parse(await readFile(summaryPath, 'utf8'))
    : null;
  const authorityPortfolios = result.status === 0
    ? JSON.parse(await readFile(authorityPortfolioPath, 'utf8'))
    : null;
  return { result, summary, authorityPortfolios };
}

function withoutExplorerLabels(row) {
  const {
    pda_program_name: omittedPdaProgramName,
    program_name: omittedProgramName,
    supplemental_program_attribution: omittedSupplement,
    ...sourceRow
  } = row;
  void omittedPdaProgramName;
  void omittedProgramName;
  void omittedSupplement;
  return sourceRow;
}

function hasOption(arguments_, name) {
  return arguments_.includes(name);
}

function optionValue(arguments_, name) {
  const index = arguments_.indexOf(name);
  return index === -1 ? undefined : arguments_[index + 1];
}

function output(result) {
  return `${result.stdout ?? ''}${result.stderr ?? ''}`;
}
