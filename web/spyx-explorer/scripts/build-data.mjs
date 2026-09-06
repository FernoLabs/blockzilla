import { createHash } from 'node:crypto';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import { deriveProviderAccessComparison } from './provider-access-model.mjs';

const HOLDER_AUTHORITY_KINDS = [
  'observed_transaction_signer',
  'attributed_program_derived_address',
  'off_curve_unattributed',
  'unclassified_on_curve'
];
const SYSTEM_PROGRAM_ID = '11111111111111111111111111111111';
const TOKEN_PROGRAM_IDS = new Set([
  'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
  'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
]);
const appRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const defaultSource = resolve(
  appRoot,
  '../../benchmark-results/spyx-token-report-v1/token-history-report-top100-20260831.json'
);
const defaultProgramSource = resolve(
  appRoot,
  '../../benchmark-results/spyx-program-identification-v1/program-identification-report.json'
);
const defaultProgramCpiSource = resolve(
  appRoot,
  '../../benchmark-results/spyx-program-identification-v1/program-inventory-target-cpi-v2.json'
);
const options = parseArguments(process.argv.slice(2));
const sourcePath = resolve(options.history ?? process.env.SPYX_HISTORY_REPORT ?? defaultSource);
const historySha256 = options.historySha256 ?? process.env.SPYX_HISTORY_REPORT_SHA256;
const strictReplayPath = optionalPath(
  options.strictReplay ?? process.env.SPYX_STRICT_REPLAY_REPORT
);
const strictReplaySha256 =
  options.strictReplaySha256 ?? process.env.SPYX_STRICT_REPLAY_REPORT_SHA256;
const holderAuthoritySupplementPath = optionalPath(
  options.holderAuthoritySupplement ?? process.env.SPYX_HOLDER_AUTHORITY_SUPPLEMENT
);
const holderAuthoritySupplementSha256 =
  options.holderAuthoritySupplementSha256 ??
  process.env.SPYX_HOLDER_AUTHORITY_SUPPLEMENT_SHA256;
const programSourcePath = resolve(
  options.programs ?? process.env.SPYX_PROGRAM_REPORT ?? defaultProgramSource
);
const programSourceSha256 =
  options.programsSha256 ?? process.env.SPYX_PROGRAM_REPORT_SHA256;
const programCpiSourcePath = resolve(
  options.programCpiInventory ??
    process.env.SPYX_PROGRAM_CPI_INVENTORY ??
    defaultProgramCpiSource
);
const programCpiSourceSha256 =
  options.programCpiInventorySha256 ?? process.env.SPYX_PROGRAM_CPI_INVENTORY_SHA256;
const outputPath = resolve(options.output ?? resolve(appRoot, 'static/data/spyx-summary.json'));
const programOutputPath = resolve(
  options.programOutput ?? resolve(appRoot, 'static/data/spyx-programs.json')
);
const authorityPortfolioOutputPath = resolve(
  options.authorityPortfolioOutput ??
    (process.env.SPYX_AUTHORITY_PORTFOLIO_OUTPUT ||
      resolve(appRoot, 'static/data/spyx-authority-portfolios.json'))
);
const pdaAuthorityEstimateOutputPath = resolve(
  options.pdaAuthorityEstimateOutput ??
    (process.env.SPYX_PDA_AUTHORITY_ESTIMATE_OUTPUT ||
      resolve(dirname(authorityPortfolioOutputPath), 'spyx-pda-authority-estimates.json'))
);
const authorityPortfolioHistoryOutputPath = resolve(
  options.authorityPortfolioHistoryOutput ??
    (process.env.SPYX_AUTHORITY_PORTFOLIO_HISTORY_OUTPUT ||
      resolve(dirname(authorityPortfolioOutputPath), 'spyx-authority-portfolio-history-index.json'))
);
const authorityPortfolioTableOutputPath = resolve(
  options.authorityPortfolioTableOutput ??
    (process.env.SPYX_AUTHORITY_PORTFOLIO_TABLE_OUTPUT ||
      authorityPortfolioOutputPath.replace(/\.json$/i, '-table.json'))
);

if (options.requireStrictReplay && !strictReplayPath) {
  throw new Error(
    'A release data build requires --strict-replay or SPYX_STRICT_REPLAY_REPORT'
  );
}
if (options.requireStrictReplay && !historySha256) {
  throw new Error(
    'A release data build requires --history-sha256 or SPYX_HISTORY_REPORT_SHA256'
  );
}
if (options.requireStrictReplay && !strictReplaySha256) {
  throw new Error(
    'A release data build requires --strict-replay-sha256 or SPYX_STRICT_REPLAY_REPORT_SHA256'
  );
}
if (options.requireStrictReplay && !programSourceSha256) {
  throw new Error(
    'A release data build requires --programs-sha256 or SPYX_PROGRAM_REPORT_SHA256'
  );
}
if (options.requireStrictReplay && !programCpiSourceSha256) {
  throw new Error(
    'A release data build requires --program-cpi-inventory-sha256 or SPYX_PROGRAM_CPI_INVENTORY_SHA256'
  );
}
if (strictReplaySha256 && !strictReplayPath) {
  throw new Error('A strict replay SHA-256 pin requires a strict replay report path');
}
if (holderAuthoritySupplementPath && !strictReplayPath) {
  throw new Error('A holder-authority supplement requires a strict replay report path');
}
if (holderAuthoritySupplementSha256 && !holderAuthoritySupplementPath) {
  throw new Error('A holder-authority supplement SHA-256 pin requires a supplement path');
}
if (
  options.requireStrictReplay &&
  holderAuthoritySupplementPath &&
  !holderAuthoritySupplementSha256
) {
  throw new Error('A release data build with a holder-authority supplement requires its SHA-256 pin');
}

const sourceBytes = await readFile(sourcePath);
const sourceReportSha256 = createHash('sha256').update(sourceBytes).digest('hex');
if (historySha256) {
  assertSha256(historySha256, 'history report SHA-256 pin');
  assertSame(
    sourceReportSha256,
    historySha256.toLowerCase(),
    'history report SHA-256 pin'
  );
}
const source = JSON.parse(sourceBytes.toString('utf8'));

assertObject(source.source, 'source');
assertObject(source.audit, 'audit');
assertObject(source.final_public_balance, 'final_public_balance');
assertObject(source.public_volume_totals, 'public_volume_totals');
assertObject(source.rpc_request_model, 'rpc_request_model');
assertArray(source.daily, 'daily');

if (source.artifact_kind !== 'token_public_balance_history') {
  throw new Error('The history report has an unexpected artifact kind');
}
if (source.source.mint !== 'XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W') {
  throw new Error('The input report is not the expected SPYx dataset');
}
validateCompleteHistoryReport(source);

const providerAccessComparison = deriveProviderAccessComparison(source);
const omittedPerAddress = source.rpc_request_model.per_address;
const compactRpcModel = compactRpcRequestModel(source.rpc_request_model);
const { rows: daily, insertedDays } = fillCalendarDays(source.daily, source.final_public_balance.decimals);
const strictReplay = strictReplayPath
  ? await buildStrictReplaySummary(
      strictReplayPath,
      source,
      strictReplaySha256
    )
  : emptyStrictReplaySummary();
if (options.requireStrictReplay) validateReleaseStrictReplay(strictReplay);
const {
  holder_authority: strictHolderAuthority,
  authority_portfolios: strictAuthorityPortfolios,
  authority_portfolio_history: strictAuthorityPortfolioHistory,
  ...compactStrictReplay
} = strictReplay;

const compact = {
  schema_version: 2,
  artifact_kind: 'spyx_public_metadata_explorer_summary',
  status: {
    bounded_selected_dump_scan_complete: source.bounded_selected_dump_scan_complete === true,
    metadata_balance_chain_continuous_from_spyx_mint_creation:
      source.metadata_balance_chain_continuous_from_spyx_mint_creation === true,
    instruction_replay_performed:
      strictReplay.present && strictReplay.instruction_replay_implemented
  },
  definitions: source.definitions,
  limitations: source.limitations,
  source: source.source,
  audit: source.audit,
  final_public_balance: source.final_public_balance,
  public_volume_totals: source.public_volume_totals,
  daily,
  ...(source.final_top_100_holder_history
    ? { final_top_100_holder_history: source.final_top_100_holder_history }
    : {}),
  top_25_volume_days: source.top_25_volume_days,
  top_25_volume_transactions: source.top_25_volume_transactions,
  rpc_request_model: compactRpcModel,
  provider_access_comparison: providerAccessComparison,
  strict_instruction_replay: compactStrictReplay,
  compact_build: {
    source_report_sha256: sourceReportSha256,
    omitted_rpc_per_address_rows: Array.isArray(omittedPerAddress) ? omittedPerAddress.length : 0,
    inserted_zero_activity_calendar_days: insertedDays
  }
};

const programSourceBytes = await readFile(programSourcePath);
const programSourceReportSha256 = createHash('sha256').update(programSourceBytes).digest('hex');
if (programSourceSha256) {
  assertSha256(programSourceSha256, 'program report SHA-256 pin');
  assertSame(
    programSourceReportSha256,
    programSourceSha256.toLowerCase(),
    'program report SHA-256 pin'
  );
}
const programSource = JSON.parse(programSourceBytes.toString('utf8'));
assertObject(programSource.source, 'program source');
assertObject(programSource.counters, 'program counters');
assertArray(programSource.programs, 'programs');
if (programSource.artifact_kind !== 'spyx_program_identification') {
  throw new Error('The program report has an unexpected artifact kind');
}
validateProgramReport(programSource, options.requireStrictReplay === true);
validateProgramSourceIdentity(programSource, source.source);
const programCpiInventory = await loadProgramCpiInventory(
  programCpiSourcePath,
  programCpiSourceSha256,
  programSource,
  source.source
);

const programNames = new Map(
  programSource.programs.map((program) => [program.program_id, program.selected_name ?? null])
);
const authoritySupplement =
  strictHolderAuthority?.complete === true && holderAuthoritySupplementPath
    ? await loadHolderAuthoritySupplement(
        holderAuthoritySupplementPath,
        holderAuthoritySupplementSha256,
        strictReplay.source_report_sha256,
        strictHolderAuthority,
        compact.final_public_balance.decimals,
        programNames,
        programSourceReportSha256
      )
    : null;
const authorityPortfolioOutput = strictAuthorityPortfolios
  ? enrichAuthorityPortfolioProgramNames(
      strictAuthorityPortfolios,
      programNames,
      authoritySupplement,
      compact.final_public_balance.decimals
    )
  : null;
const authorityPortfolioTableOutput = authorityPortfolioOutput
  ? buildAuthorityPortfolioTable(authorityPortfolioOutput)
  : null;
const pdaAuthorityEstimateOutput = authorityPortfolioOutput
  ? {
      schema_version: 1,
      artifact_kind: 'spyx_pda_authority_estimates',
      source_binding: authorityPortfolioOutput.source_binding,
      summary: authorityPortfolioOutput.pda_authority_estimate_summary,
      estimates: authorityPortfolioOutput.pda_authority_estimates
    }
  : null;
compact.compact_build.authority_portfolios_available = authorityPortfolioOutput !== null;
compact.compact_build.authority_portfolio_table_available =
  authorityPortfolioTableOutput !== null;
compact.compact_build.authority_portfolio_history_available =
  strictAuthorityPortfolioHistory !== null;

if (strictHolderAuthority?.complete === true) {
  const enrichHolder = (holder) => ({
    ...holder,
    pda_program_name: holder.pda_program_id
      ? (programNames.get(holder.pda_program_id) ?? null)
      : null,
    ...(holder.runtime_account_owner
      ? {
          runtime_account_owner: {
            ...holder.runtime_account_owner,
            program_name:
              programNames.get(holder.runtime_account_owner.program_id) ?? null
          }
        }
      : {}),
    ...(authoritySupplement?.holdersByAddress.has(holder.owner)
      ? {
          supplemental_program_attribution:
            authoritySupplement.holdersByAddress.get(holder.owner)
              .supplemental_program_attribution
        }
      : {})
  });
  const labeledProgramHoldings = strictHolderAuthority.holdings_by_program.map((row) => ({
    ...row,
    program_name: programNames.get(row.program_id) ?? null
  }));
  validateAdditiveProgramLabels(
    strictHolderAuthority.holdings_by_program,
    labeledProgramHoldings
  );
  const holderAuthority = {
    ...strictHolderAuthority,
    largest_25_all: strictHolderAuthority.largest_25_all.map(enrichHolder),
    largest_25_by_class: Object.fromEntries(
      Object.entries(strictHolderAuthority.largest_25_by_class).map(([kind, rows]) => [
        kind,
        rows.map(enrichHolder)
      ])
    ),
    ...(strictHolderAuthority.largest_25_by_activity_all
      ? {
          largest_25_by_activity_all:
            strictHolderAuthority.largest_25_by_activity_all.map(enrichHolder)
        }
      : {}),
    ...(strictHolderAuthority.largest_25_by_activity_by_class
      ? {
          largest_25_by_activity_by_class: Object.fromEntries(
            Object.entries(strictHolderAuthority.largest_25_by_activity_by_class).map(
              ([kind, rows]) => [kind, rows.map(enrichHolder)]
            )
          )
        }
      : {}),
    ...(strictHolderAuthority.attributed_program_holders
      ? {
          attributed_program_holders:
            strictHolderAuthority.attributed_program_holders.map(enrichHolder)
        }
      : {}),
    ...(strictHolderAuthority.off_curve_unattributed_holders
      ? {
          off_curve_unattributed_holders:
            strictHolderAuthority.off_curve_unattributed_holders.map(enrichHolder)
        }
      : {}),
    holdings_by_program: labeledProgramHoldings,
    ...(authoritySupplement
      ? { attribution_supplements: [authoritySupplement.publicReport] }
      : {})
  };
  compact.final_public_balance = {
    ...compact.final_public_balance,
    largest_25_holders: holderAuthority.largest_25_all,
    holder_authority: holderAuthority
  };
  if (authoritySupplement) {
    compact.compact_build.holder_authority_supplement_sha256 =
      authoritySupplement.publicReport.source_report_sha256;
  }
}

function enrichAuthorityPortfolioProgramNames(
  report,
  programNames,
  authoritySupplement,
  decimals
) {
  const supplementalProgramId = (custodyOwner) => {
    const attribution = authoritySupplement?.holdersByAddress.get(custodyOwner)
      ?.supplemental_program_attribution;
    return attribution?.attribution_status === 'attributed_custom_program_runtime_owner'
      ? attribution.runtime_owner_program_id
      : null;
  };
  const resolveProgram = (programId, custodyOwner) =>
    programId
      ? { programId, evidence: 'replay_program_id' }
      : supplementalProgramId(custodyOwner)
        ? {
            programId: supplementalProgramId(custodyOwner),
            evidence: 'supplemental_runtime_account_owner'
          }
        : { programId: null, evidence: null };
  const programReference = (programId, evidence = 'replay_program_id') => ({
    program_id: programId,
    program_name: programNames.get(programId) ?? null,
    program_id_evidence: evidence
  });
  const enriched = {
    ...report,
    portfolios: report.portfolios.map((portfolio) => {
      const programs = new Map(
        portfolio.programs_used.map((programId) => [programId, 'replay_program_id'])
      );
      const claimComponents = portfolio.claim_components.map((component) => {
        const resolved = resolveProgram(component.program_id, component.custody_owner);
        if (resolved.programId && !programs.has(resolved.programId)) {
          programs.set(resolved.programId, resolved.evidence);
        }
        return {
          ...component,
          program_id: resolved.programId,
          program_name: resolved.programId
            ? (programNames.get(resolved.programId) ?? null)
            : null,
          ...(resolved.evidence ? { program_id_evidence: resolved.evidence } : {})
        };
      });
      return {
        ...portfolio,
        programs_used: [...programs].map(([programId, evidence]) =>
          programReference(programId, evidence)
        ),
        claim_components: claimComponents
      };
    }),
    protocol_custody: report.protocol_custody.map((custody) => {
      const resolved = resolveProgram(custody.program_id, custody.custody_owner);
      return {
        ...custody,
        program_id: resolved.programId,
        program_name: resolved.programId
          ? (programNames.get(resolved.programId) ?? null)
          : null,
        ...(resolved.evidence ? { program_id_evidence: resolved.evidence } : {})
      };
    })
  };
  const pdaAuthorityEstimates = derivePdaAuthorityEstimates(
    enriched,
    programNames,
    decimals
  );
  return {
    ...enriched,
    pda_authority_estimate_summary: {
      schema_version: 1,
      method: 'committed_pda_creation_signer_external_claims_v1',
      subject_count: pdaAuthorityEstimates.length,
      selected_subject_count: pdaAuthorityEstimates.filter(
        (row) => row.selected_candidate_authority !== null
      ).length,
      proves_beneficial_ownership: false,
      additive_to_authority_totals: false
    },
    pda_authority_estimates: pdaAuthorityEstimates
  };
}

function derivePdaAuthorityEstimates(report, programNames, decimals) {
  const portfoliosByAuthority = new Map(
    report.portfolios.map((portfolio) => [portfolio.authority, portfolio])
  );
  const custodyByOwner = new Map(
    report.protocol_custody.map((custody) => [custody.custody_owner, custody])
  );
  const creationEventsBySubject = new Map();
  const subjectsBySigner = new Map();

  for (const event of report.pda_creation_provenance) {
    if (event.event_kind !== 'account_creation') continue;
    const events = creationEventsBySubject.get(event.subject_pda) ?? [];
    events.push(event);
    creationEventsBySubject.set(event.subject_pda, events);
    for (const signer of event.signer_candidates) {
      const subjects = subjectsBySigner.get(signer) ?? new Set();
      subjects.add(event.subject_pda);
      subjectsBySigner.set(signer, subjects);
    }
  }

  const compareCreationLocation = (left, right) =>
    left.location.slot - right.location.slot ||
    left.location.source_block_id - right.location.source_block_id ||
    left.location.tx_index - right.location.tx_index ||
    left.location.outer_index - right.location.outer_index ||
    (left.location.inner_index ?? -1) - (right.location.inner_index ?? -1);
  const compareAddress = (left, right) => (left < right ? -1 : left > right ? 1 : 0);

  return [...creationEventsBySubject.entries()]
    .map(([subjectPda, unsortedEvents]) => {
      const events = [...unsortedEvents].sort(compareCreationLocation);
      const creation = events[0];
      const signerCandidates = [...new Set(events.flatMap((event) => event.signer_candidates))]
        .sort(compareAddress);
      const custody = custodyByOwner.get(subjectPda) ?? null;
      const directPublicBalance = custody?.direct_custody_balance ?? amountFromRaw(0n, decimals);
      const candidates = signerCandidates.map((authority) => {
        const portfolio = portfoliosByAuthority.get(authority) ?? null;
        const linkedSubjectPdaCount = subjectsBySigner.get(authority)?.size ?? 0;
        const externalClaimComponents = portfolio
          ? portfolio.claim_components.filter(
              (component) => component.custody_owner !== subjectPda
            )
          : [];
        const externalRawClaim = externalClaimComponents.reduce(
          (sum, component) => sum + BigInt(component.attributed_claim.raw_amount),
          0n
        );
        const programsUsed = new Map();
        const positionsByProgram = new Map();
        for (const component of externalClaimComponents) {
          if (component.program_id && !programsUsed.has(component.program_id)) {
            programsUsed.set(component.program_id, {
              program_id: component.program_id,
              program_name:
                component.program_name ?? programNames.get(component.program_id) ?? null,
              ...(component.program_id_evidence
                ? { program_id_evidence: component.program_id_evidence }
                : {})
            });
          }
          const positionKey = component.program_id ?? '';
          const position = positionsByProgram.get(positionKey) ?? {
            program_id: component.program_id,
            program_name: component.program_name ?? null,
            program_id_evidence: component.program_id_evidence ?? null,
            custody_owners: new Set(),
            deposited: 0n,
            returned: 0n,
            candidate: 0n,
            attributed: 0n,
            deposit_transactions: 0,
            return_transactions: 0
          };
          position.custody_owners.add(component.custody_owner);
          position.deposited += BigInt(component.observed_deposited_principal.raw_amount);
          position.returned += BigInt(component.observed_returned_principal.raw_amount);
          position.candidate += BigInt(component.candidate_net_principal.raw_amount);
          position.attributed += BigInt(component.attributed_claim.raw_amount);
          position.deposit_transactions += component.deposit_transaction_count;
          position.return_transactions += component.return_transaction_count;
          positionsByProgram.set(positionKey, position);
        }
        const programPositions = [...positionsByProgram.values()]
          .map((position) => {
            const custodyOwners = [...position.custody_owners].sort(compareAddress);
            return {
              program_id: position.program_id,
              program_name: position.program_name,
              ...(position.program_id_evidence
                ? { program_id_evidence: position.program_id_evidence }
                : {}),
              custody_owners: custodyOwners,
              custody_owner_count: custodyOwners.length,
              observed_deposited_principal: amountFromRaw(position.deposited, decimals),
              observed_returned_principal: amountFromRaw(position.returned, decimals),
              candidate_net_principal: amountFromRaw(position.candidate, decimals),
              estimated_claim: amountFromRaw(position.attributed, decimals),
              deposit_transaction_count: position.deposit_transactions,
              return_transaction_count: position.return_transactions
            };
          })
          .sort((left, right) => {
            if (left.program_id === null) return right.program_id === null ? 0 : 1;
            if (right.program_id === null) return -1;
            return compareAddress(left.program_id, right.program_id);
          });
        return {
          authority,
          authority_kind: portfolio?.authority_kind ?? null,
          portfolio_available: portfolio !== null,
          linked_subject_pda_count: linkedSubjectPdaCount,
          estimated_external_defi_claim: amountFromRaw(externalRawClaim, decimals),
          programs_used: [...programsUsed.values()],
          program_positions: programPositions
        };
      });

      let resolution = 'no_creation_signer_candidate';
      let selectedCandidateAuthority = null;
      if (candidates.length > 1) {
        resolution = 'ambiguous_creation_signer_candidates';
      } else if (candidates.length === 1 && candidates[0].linked_subject_pda_count > 1) {
        resolution = 'shared_creation_signer_candidate';
      } else if (candidates.length === 1 && !candidates[0].portfolio_available) {
        resolution = 'candidate_portfolio_unavailable';
      } else if (candidates.length === 1) {
        resolution = 'single_unique_creation_signer_candidate';
        selectedCandidateAuthority = candidates[0].authority;
      }

      const selectedCandidate = selectedCandidateAuthority
        ? candidates.find((candidate) => candidate.authority === selectedCandidateAuthority)
        : null;
      const publicCandidates = selectedCandidateAuthority
        ? candidates
        : candidates.map((candidate) => ({
            ...candidate,
            programs_used: [],
            program_positions: []
          }));
      const selectedExternalClaim = selectedCandidate
        ? BigInt(selectedCandidate.estimated_external_defi_claim.raw_amount)
        : null;
      const directRawBalance = BigInt(directPublicBalance.raw_amount);

      return {
        subject_pda: subjectPda,
        runtime_owner_program_id: creation.runtime_owner_program_id,
        runtime_owner_program_name:
          programNames.get(creation.runtime_owner_program_id) ?? null,
        direct_caller_program_id: creation.direct_caller_program_id,
        direct_caller_program_name: creation.direct_caller_program_id
          ? (programNames.get(creation.direct_caller_program_id) ?? null)
          : null,
        system_instruction: creation.system_instruction,
        create_with_seed_base: creation.create_with_seed_base,
        creation_event_count: events.length,
        creation_location: creation.location,
        signer_candidates: signerCandidates,
        candidates: publicCandidates,
        direct_public_balance: directPublicBalance,
        selected_candidate_authority: selectedCandidateAuthority,
        estimated_external_defi_claim:
          selectedExternalClaim === null
            ? null
            : amountFromRaw(selectedExternalClaim, decimals),
        estimated_total_exposure:
          selectedExternalClaim === null
            ? null
            : amountFromRaw(directRawBalance + selectedExternalClaim, decimals),
        resolution,
        confidence: 'heuristic_pda_creation_signer_external_claims',
        proves_beneficial_ownership: false,
        additive_to_authority_totals: false
      };
    })
    .sort((left, right) => compareAddress(left.subject_pda, right.subject_pda));
}

function validateAdditiveProgramLabels(sourceRows, labeledRows) {
  if (sourceRows.length !== labeledRows.length) {
    throw new Error('program labels must not change holder-authority program row inclusion');
  }
  for (const [index, sourceRow] of sourceRows.entries()) {
    if (labeledRows[index]?.program_id !== sourceRow.program_id) {
      throw new Error('program labels must not change holder-authority program row order');
    }
  }
}

async function loadHolderAuthoritySupplement(
  path,
  expectedSha256,
  strictReplayReportSha256,
  holderAuthority,
  decimals,
  programNames,
  programSourceReportSha256
) {
  const bytes = await readFile(path);
  const sourceReportSha256 = createHash('sha256').update(bytes).digest('hex');
  if (expectedSha256) {
    assertSha256(expectedSha256, 'holder-authority supplement SHA-256 pin');
    assertSame(
      sourceReportSha256,
      expectedSha256.toLowerCase(),
      'holder-authority supplement SHA-256 pin'
    );
  }
  const report = JSON.parse(bytes.toString('utf8'));
  const label = 'holder-authority supplement';

  assertObject(report, label);
  assertSame(report.schema_version, 1, `${label} schema version`);
  if (report.artifact_kind !== 'spyx_holder_authority_runtime_owner_snapshot') {
    throw new Error(`${label} has an unexpected artifact kind`);
  }
  if (report.evidence_kind !== 'solana_runtime_account_owner') {
    throw new Error(`${label}.evidence_kind is not solana_runtime_account_owner`);
  }
  if (report.cluster !== 'mainnet-beta') {
    throw new Error(`${label}.cluster is not mainnet-beta`);
  }
  if (report.rpc_method !== 'getMultipleAccounts') {
    throw new Error(`${label}.rpc_method is not getMultipleAccounts`);
  }
  assertNonEmptyString(report.rpc_endpoint, `${label}.rpc_endpoint`);
  assertNonNegativeSafeInteger(report.observed_slot, `${label}.observed_slot`);
  if (report.observed_slot === 0) {
    throw new Error(`${label}.observed_slot must be greater than zero`);
  }
  const hasObservedSlotMin = report.observed_slot_min !== undefined;
  const hasObservedSlotMax = report.observed_slot_max !== undefined;
  if (hasObservedSlotMin !== hasObservedSlotMax) {
    throw new Error(`${label} has an incomplete observed slot range`);
  }
  const observedSlotMin = hasObservedSlotMin
    ? report.observed_slot_min
    : report.observed_slot;
  const observedSlotMax = hasObservedSlotMax
    ? report.observed_slot_max
    : report.observed_slot;
  assertNonNegativeSafeInteger(observedSlotMin, `${label}.observed_slot_min`);
  assertNonNegativeSafeInteger(observedSlotMax, `${label}.observed_slot_max`);
  if (
    observedSlotMin === 0 ||
    observedSlotMin > observedSlotMax ||
    report.observed_slot !== observedSlotMax
  ) {
    throw new Error(`${label} has an invalid observed slot range`);
  }
  assertNonEmptyString(report.selection, `${label}.selection`);
  if (
    report.selection_scope !== 'exposed_off_curve_unattributed_holder_rows' &&
    report.selection_scope !== 'all_off_curve_unattributed_holders'
  ) {
    throw new Error(`${label}.selection_scope is not supported`);
  }
  assertSha256(report.source_replay_sha256, `${label}.source_replay_sha256`);
  assertSame(
    report.source_replay_sha256.toLowerCase(),
    strictReplayReportSha256,
    `${label} source replay SHA-256`
  );
  assertArray(report.accounts, `${label}.accounts`);
  if (report.accounts.length === 0) {
    throw new Error(`${label}.accounts must not be empty`);
  }

  const sourceRows = selectSupplementSourceRows(holderAuthority, report.selection_scope);
  const sourceRowsByAddress = new Map(sourceRows.map((row) => [row.owner, row]));
  if (sourceRowsByAddress.size !== sourceRows.length) {
    throw new Error(`${label} source holder rows contain a duplicate address`);
  }
  if (report.accounts.length !== sourceRows.length) {
    throw new Error(`${label} account count does not match its selected replay holder rows`);
  }

  const accountsByAddress = new Map();
  let previousAddress = null;
  for (const [index, account] of report.accounts.entries()) {
    const rowLabel = `${label}.accounts[${index}]`;
    assertObject(account, rowLabel);
    assertNonEmptyString(account.address, `${rowLabel}.address`);
    if (previousAddress !== null && account.address <= previousAddress) {
      throw new Error(`${label}.accounts must be in strict address order`);
    }
    previousAddress = account.address;
    if (accountsByAddress.has(account.address)) {
      throw new Error(`${label}.accounts has a duplicate address`);
    }
    if (!sourceRowsByAddress.has(account.address)) {
      throw new Error(`${rowLabel}.address is not in the selected replay holder rows`);
    }
    if (typeof account.exists !== 'boolean') {
      throw new Error(`${rowLabel}.exists is not a boolean`);
    }
    if (account.exists) {
      assertNonEmptyString(
        account.runtime_owner_program_id,
        `${rowLabel}.runtime_owner_program_id`
      );
      assertNonNegativeSafeInteger(account.data_bytes, `${rowLabel}.data_bytes`);
      if (typeof account.executable !== 'boolean') {
        throw new Error(`${rowLabel}.executable is not a boolean`);
      }
    } else if (
      account.runtime_owner_program_id !== null ||
      account.data_bytes !== null ||
      account.executable !== null
    ) {
      throw new Error(`${rowLabel} has account fields for a missing account`);
    }
    const accountObservedSlot = account.observed_slot ?? report.observed_slot;
    assertNonNegativeSafeInteger(accountObservedSlot, `${rowLabel}.observed_slot`);
    if (
      accountObservedSlot < observedSlotMin ||
      accountObservedSlot > observedSlotMax
    ) {
      throw new Error(`${rowLabel}.observed_slot is outside the snapshot slot range`);
    }
    validateOptionalAccountLabel(account, rowLabel);
    accountsByAddress.set(account.address, account);
  }
  for (const address of sourceRowsByAddress.keys()) {
    if (!accountsByAddress.has(address)) {
      throw new Error(`${label}.accounts is missing selected replay holder ${address}`);
    }
  }

  const publicHolderRows = sourceRows
    .map((holder) => {
      const account = accountsByAddress.get(holder.owner);
      const runtimeOwnerProgramId = account.runtime_owner_program_id;
      const attributionStatus = runtimeOwnerAttributionStatus(account);
      const runtimeOwnerProgramName = runtimeOwnerProgramId
        ? (programNames.get(runtimeOwnerProgramId) ?? null)
        : null;
      return {
        ...holder,
        ...(holder.runtime_account_owner
          ? {
              runtime_account_owner: {
                ...holder.runtime_account_owner,
                program_name:
                  programNames.get(holder.runtime_account_owner.program_id) ?? null
              }
            }
          : {}),
        supplemental_program_attribution: {
          evidence_kind: report.evidence_kind,
          snapshot_slot: account.observed_slot ?? report.observed_slot,
          account_exists: account.exists,
          runtime_owner_program_id: runtimeOwnerProgramId,
          runtime_owner_program_name: runtimeOwnerProgramName,
          data_bytes: account.data_bytes,
          executable: account.executable,
          attribution_status: attributionStatus,
          proves_pda_derivation: false,
          ...(account.account_label ? { account_label: account.account_label } : {}),
          ...(account.account_label_evidence
            ? { account_label_evidence: account.account_label_evidence }
            : {})
        }
      };
    })
    .sort(compareHolderBalance);
  const attributedRows = publicHolderRows.filter(
    (row) =>
      row.supplemental_program_attribution.attribution_status ===
      'attributed_custom_program_runtime_owner'
  );
  const ownerObservedRows = publicHolderRows.filter(
    (row) => row.supplemental_program_attribution.runtime_owner_program_id !== null
  );
  const unattributedRows = publicHolderRows.filter(
    (row) =>
      row.supplemental_program_attribution.attribution_status !==
      'attributed_custom_program_runtime_owner'
  );
  const totals = {
    observed: summarizeSupplementRows(publicHolderRows, decimals),
    attributed_custom_program: summarizeSupplementRows(attributedRows, decimals),
    not_attributed: summarizeSupplementRows(unattributedRows, decimals)
  };
  validateSupplementPartitionTotals(totals, label);
  const holdingsByProgram = summarizeSupplementPrograms(ownerObservedRows, decimals);
  const attributionStatusCounts = Object.fromEntries(
    [
      'attributed_custom_program_runtime_owner',
      'not_attributed_account_missing',
      'not_attributed_system_program',
      'not_attributed_token_program',
      'not_attributed_executable_account'
    ].map((status) => [
      status,
      publicHolderRows.filter(
        (row) => row.supplemental_program_attribution.attribution_status === status
      ).length
    ])
  );
  const holdersByAddress = new Map(publicHolderRows.map((row) => [row.owner, row]));
  if (holdersByAddress.size !== sourceRowsByAddress.size) {
    throw new Error(`${label} output changed holder inclusion`);
  }

  const replayOffCurveHolderCount = holderAuthority.class_totals.find(
    (row) => row.authority_kind === 'off_curve_unattributed'
  ).holder_count;
  const queriedHolderCount = publicHolderRows.length;
  const accountOwnerObservedCount = ownerObservedRows.length;
  if (queriedHolderCount > replayOffCurveHolderCount) {
    throw new Error(`${label} queried holder count exceeds the replay class total`);
  }
  if (accountOwnerObservedCount > queriedHolderCount) {
    throw new Error(`${label} account-owner observation count exceeds queried holders`);
  }
  return {
    holdersByAddress,
    publicReport: {
      schema_version: report.schema_version,
      artifact_kind: report.artifact_kind,
      source_report_sha256: sourceReportSha256,
      source_replay_sha256: report.source_replay_sha256.toLowerCase(),
      program_source_report_sha256: programSourceReportSha256,
      evidence_kind: report.evidence_kind,
      cluster: report.cluster,
      rpc_method: report.rpc_method,
      rpc_endpoint: report.rpc_endpoint,
      snapshot_slot: report.observed_slot,
      snapshot_slot_min: observedSlotMin,
      snapshot_slot_max: observedSlotMax,
      selection_scope: report.selection_scope,
      selection: report.selection,
      coverage: {
        complete_for_all_off_curve_unattributed_holders:
          report.selection_scope === 'all_off_curve_unattributed_holders',
        replay_off_curve_unattributed_holder_count: replayOffCurveHolderCount,
        queried_holder_count: queriedHolderCount,
        unqueried_holder_count: replayOffCurveHolderCount - queriedHolderCount,
        observed_holder_count: accountOwnerObservedCount,
        unobserved_holder_count: replayOffCurveHolderCount - accountOwnerObservedCount
      },
      counts: {
        accounts: publicHolderRows.length,
        present_accounts: publicHolderRows.filter(
          (row) => row.supplemental_program_attribution.account_exists
        ).length,
        absent_accounts: attributionStatusCounts.not_attributed_account_missing,
        runtime_owner_programs: holdingsByProgram.length,
        ...attributionStatusCounts
      },
      definitions: {
        runtime_owner:
          'The Solana Account.owner program observed for this holder address at the snapshot slot.',
        attribution:
          'A custom runtime owner links an off-curve holder address to a program. It does not prove PDA seeds, custody, or protocol TVL.',
        excluded_runtime_owners:
          'Missing accounts have no runtime Account.owner observation. System Program, SPL Token, Token-2022, and executable-account owners remain visible but do not count as custom protocol attribution.'
      },
      totals,
      holders: publicHolderRows,
      holdings_by_program: holdingsByProgram
    }
  };
}

function selectSupplementSourceRows(holderAuthority, selectionScope) {
  if (selectionScope === 'all_off_curve_unattributed_holders') {
    if (!Array.isArray(holderAuthority.off_curve_unattributed_holders)) {
      throw new Error(
        'holder-authority supplement requests all off-curve holders, but the replay has no complete holder array'
      );
    }
    return holderAuthority.off_curve_unattributed_holders;
  }

  const rows = [
    ...holderAuthority.largest_25_by_class.off_curve_unattributed,
    ...(holderAuthority.largest_25_by_activity_by_class?.off_curve_unattributed ?? [])
  ];
  const byAddress = new Map();
  for (const row of rows) {
    const existing = byAddress.get(row.owner);
    if (existing && !isDeepStrictEqual(existing, row)) {
      throw new Error(
        `holder-authority supplement source rows differ for holder ${row.owner}`
      );
    }
    byAddress.set(row.owner, row);
  }
  return [...byAddress.values()];
}

function validateOptionalAccountLabel(account, label) {
  const hasLabel = account.account_label !== undefined;
  const hasEvidence = account.account_label_evidence !== undefined;
  if (hasLabel !== hasEvidence) {
    throw new Error(`${label} has incomplete account-label evidence`);
  }
  if (!hasLabel) return;
  assertNonEmptyString(account.account_label, `${label}.account_label`);
  assertObject(account.account_label_evidence, `${label}.account_label_evidence`);
  if (account.account_label_evidence.kind !== 'public_explorer_label') {
    throw new Error(`${label}.account_label_evidence.kind is not supported`);
  }
  assertNonEmptyString(
    account.account_label_evidence.source_name,
    `${label}.account_label_evidence.source_name`
  );
  assertNonEmptyString(
    account.account_label_evidence.source_url,
    `${label}.account_label_evidence.source_url`
  );
}

function runtimeOwnerAttributionStatus(account) {
  if (!account.exists) return 'not_attributed_account_missing';
  if (account.runtime_owner_program_id === SYSTEM_PROGRAM_ID) {
    return 'not_attributed_system_program';
  }
  if (TOKEN_PROGRAM_IDS.has(account.runtime_owner_program_id)) {
    return 'not_attributed_token_program';
  }
  if (account.executable) return 'not_attributed_executable_account';
  return 'attributed_custom_program_runtime_owner';
}

function summarizeSupplementRows(rows, decimals) {
  let tokenAccountCount = 0;
  let rawBalance = 0n;
  let activityTransactionLinks = 0;
  let rawIncrease = 0n;
  let rawDecrease = 0n;
  let hasActivity = true;
  for (const row of rows) {
    tokenAccountCount = safeIntegerAdd(
      tokenAccountCount,
      row.token_account_count,
      'holder-authority supplement token-account count'
    );
    rawBalance += BigInt(row.public_balance.raw_amount);
    if (
      row.activity_transaction_count === undefined ||
      row.public_balance_increase === undefined ||
      row.public_balance_decrease === undefined ||
      row.public_activity_volume === undefined
    ) {
      hasActivity = false;
      continue;
    }
    activityTransactionLinks = safeIntegerAdd(
      activityTransactionLinks,
      row.activity_transaction_count,
      'holder-authority supplement activity transaction links'
    );
    rawIncrease += BigInt(row.public_balance_increase.raw_amount);
    rawDecrease += BigInt(row.public_balance_decrease.raw_amount);
  }
  const summary = {
    holder_count: rows.length,
    token_account_count: tokenAccountCount,
    public_balance: amountFromRaw(rawBalance, decimals)
  };
  if (hasActivity) {
    summary.owner_activity_transaction_links = activityTransactionLinks;
    summary.public_balance_increase = amountFromRaw(rawIncrease, decimals);
    summary.public_balance_decrease = amountFromRaw(rawDecrease, decimals);
    summary.public_activity_volume = amountFromRaw(rawIncrease + rawDecrease, decimals);
  }
  return summary;
}

function validateSupplementPartitionTotals(totals, label) {
  assertSame(
    totals.attributed_custom_program.holder_count + totals.not_attributed.holder_count,
    totals.observed.holder_count,
    `${label} holder partition`
  );
  assertSame(
    totals.attributed_custom_program.token_account_count +
      totals.not_attributed.token_account_count,
    totals.observed.token_account_count,
    `${label} token-account partition`
  );
  assertSame(
    BigInt(totals.attributed_custom_program.public_balance.raw_amount) +
      BigInt(totals.not_attributed.public_balance.raw_amount),
    BigInt(totals.observed.public_balance.raw_amount),
    `${label} public-balance partition`
  );
  if (totals.observed.owner_activity_transaction_links !== undefined) {
    assertSame(
      totals.attributed_custom_program.owner_activity_transaction_links +
        totals.not_attributed.owner_activity_transaction_links,
      totals.observed.owner_activity_transaction_links,
      `${label} activity-link partition`
    );
    for (const field of [
      'public_balance_increase',
      'public_balance_decrease',
      'public_activity_volume'
    ]) {
      assertSame(
        BigInt(totals.attributed_custom_program[field].raw_amount) +
          BigInt(totals.not_attributed[field].raw_amount),
        BigInt(totals.observed[field].raw_amount),
        `${label} ${field} partition`
      );
    }
  }
}

function summarizeSupplementPrograms(rows, decimals) {
  const byProgram = new Map();
  for (const row of rows) {
    const attribution = row.supplemental_program_attribution;
    const programId = attribution.runtime_owner_program_id;
    const aggregate = byProgram.get(programId) ?? {
      program_id: programId,
      program_name: attribution.runtime_owner_program_name,
      rows: []
    };
    aggregate.rows.push(row);
    byProgram.set(programId, aggregate);
  }
  return [...byProgram.values()]
    .map((aggregate) => ({
      program_id: aggregate.program_id,
      program_name: aggregate.program_name,
      ...summarizeSupplementRows(aggregate.rows, decimals)
    }))
    .sort((left, right) => {
      const balanceOrder = compareBigIntDescending(
        BigInt(left.public_balance.raw_amount),
        BigInt(right.public_balance.raw_amount)
      );
      return balanceOrder === 0 ? left.program_id.localeCompare(right.program_id) : balanceOrder;
    });
}

function compareHolderBalance(left, right) {
  const balanceOrder = compareBigIntDescending(
    BigInt(left.public_balance.raw_amount),
    BigInt(right.public_balance.raw_amount)
  );
  return balanceOrder === 0 ? left.owner.localeCompare(right.owner) : balanceOrder;
}

function compareBigIntDescending(left, right) {
  return left === right ? 0 : left > right ? -1 : 1;
}

function amountFromRaw(rawAmount, decimals) {
  const raw = rawAmount.toString();
  return { raw_amount: raw, base_units: formatBaseUnits(raw, decimals) };
}

const compactPrograms = {
  schema_version: 2,
  artifact_kind: 'spyx_program_identification_explorer_summary',
  complete: programSource.complete === true,
  generated_at: programSource.generated_at,
  definitions: programSource.definitions,
  source: {
    first_epoch: programSource.source.first_epoch,
    last_epoch: programSource.source.last_epoch,
    inventory_sha256: programSource.source.inventory_sha256,
    dump_manifest_sha256: programSource.source.dump_manifest_sha256,
    dump_transaction_stream_sha256: programSource.source.dump_transaction_stream_sha256,
    dump_pubkey_registry_sha256: programSource.source.dump_pubkey_registry_sha256
  },
  counters: programSource.counters,
  target_account_cpi: programCpiInventory.publicSummary,
  source_match_counts: programSource.source_match_counts,
  programs: programSource.programs.map((program) => {
    const cpi = programCpiInventory.rowsByProgramId.get(program.program_id);
    if (!cpi) throw new Error(`program CPI inventory is missing ${program.program_id}`);
    return {
      rank: program.rank,
      registry_id: program.registry_id,
      program_id: program.program_id,
      identity_status: program.identity_status,
      selected_name: program.selected_name,
      selected_source: program.selected_source,
      selected_confidence: program.selected_confidence,
      usable_onchain_idl: program.usable_onchain_idl,
      address_clean_onchain_idl: program.address_clean_onchain_idl,
      decoder_source_found: program.decoder_source_found,
      total_occurrences: program.total_occurrences,
      outer_occurrences: program.outer_occurrences,
      inner_occurrences: program.inner_occurrences,
      transactions: program.transactions,
      target_account_inner_occurrences: cpi.target_account_inner_occurrences,
      target_account_inner_transactions: cpi.target_account_inner_transactions,
      target_mint_inner_occurrences: cpi.target_mint_inner_occurrences,
      target_token_account_inner_occurrences: cpi.target_token_account_inner_occurrences,
      target_account_inner_references: cpi.target_account_inner_references,
      target_mint_inner_references: cpi.target_mint_inner_references,
      target_token_account_inner_references: cpi.target_token_account_inner_references
    };
  }),
  compact_build: {
    source_report_sha256: programSourceReportSha256,
    target_account_cpi_source_report_sha256: programCpiInventory.sourceReportSha256,
    evidence_arrays_omitted: true
  }
};

if (!options.validateOnly) {
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, `${JSON.stringify(compact)}\n`);
  await writeFile(programOutputPath, `${JSON.stringify(compactPrograms)}\n`);
  if (authorityPortfolioOutput) {
    await mkdir(dirname(authorityPortfolioOutputPath), { recursive: true });
    await writeFile(
      authorityPortfolioOutputPath,
      `${JSON.stringify(authorityPortfolioOutput)}\n`
    );
    await writeAuthorityPortfolioShards(
      authorityPortfolioOutput,
      resolve(dirname(authorityPortfolioOutputPath), 'spyx-authority-portfolios-by-prefix')
    );
  } else {
    await mkdir(dirname(authorityPortfolioOutputPath), { recursive: true });
    await writeFile(authorityPortfolioOutputPath, 'null\n');
    await rm(
      resolve(dirname(authorityPortfolioOutputPath), 'spyx-authority-portfolios-by-prefix'),
      { recursive: true, force: true }
    );
  }
  await mkdir(dirname(authorityPortfolioTableOutputPath), { recursive: true });
  await writeFile(
    authorityPortfolioTableOutputPath,
    authorityPortfolioTableOutput
      ? `${JSON.stringify(authorityPortfolioTableOutput)}\n`
      : 'null\n'
  );
  if (pdaAuthorityEstimateOutput) {
    await mkdir(dirname(pdaAuthorityEstimateOutputPath), { recursive: true });
    await writeFile(
      pdaAuthorityEstimateOutputPath,
      `${JSON.stringify(pdaAuthorityEstimateOutput)}\n`
    );
  } else {
    await mkdir(dirname(pdaAuthorityEstimateOutputPath), { recursive: true });
    await writeFile(pdaAuthorityEstimateOutputPath, 'null\n');
  }
  if (strictAuthorityPortfolioHistory) {
    await mkdir(dirname(authorityPortfolioHistoryOutputPath), { recursive: true });
    const historyIndex = await writeAuthorityPortfolioHistoryShards(
      strictAuthorityPortfolioHistory,
      resolve(
        dirname(authorityPortfolioHistoryOutputPath),
        'spyx-authority-portfolio-history-by-prefix'
      )
    );
    await writeFile(authorityPortfolioHistoryOutputPath, `${JSON.stringify(historyIndex)}\n`);
  } else {
    await mkdir(dirname(authorityPortfolioHistoryOutputPath), { recursive: true });
    await writeFile(authorityPortfolioHistoryOutputPath, 'null\n');
    await rm(
      resolve(
        dirname(authorityPortfolioHistoryOutputPath),
        'spyx-authority-portfolio-history-by-prefix'
      ),
      { recursive: true, force: true }
    );
  }
}

async function writeAuthorityPortfolioShards(report, directory) {
  const portfoliosByPrefix = new Map();
  for (const portfolio of report.portfolios) {
    const prefix = portfolio.authority[0];
    if (!prefix) throw new Error('authority portfolio has an empty address');
    const rows = portfoliosByPrefix.get(prefix) ?? [];
    rows.push(portfolio);
    portfoliosByPrefix.set(prefix, rows);
  }

  await rm(directory, { recursive: true, force: true });
  await mkdir(directory, { recursive: true });
  const prefixes = [...portfoliosByPrefix.keys()].sort((left, right) =>
    left.localeCompare(right)
  );
  for (const prefix of prefixes) {
    const shard = {
      schema_version: 1,
      artifact_kind: 'spyx_authority_portfolio_shard',
      source_binding: report.source_binding,
      prefix,
      portfolios: portfoliosByPrefix.get(prefix)
    };
    await writeFile(resolve(directory, shardFileName(prefix)), `${JSON.stringify(shard)}\n`);
  }
  await writeFile(
    resolve(directory, 'index.json'),
    `${JSON.stringify({
      schema_version: 1,
      artifact_kind: 'spyx_authority_portfolio_shard_index',
      source_binding: report.source_binding,
      prefixes,
      portfolios: report.portfolios.length
    })}\n`
  );
}

async function writeAuthorityPortfolioHistoryShards(report, directory) {
  const prefixLength = 2;
  const seriesByPrefix = new Map();
  for (const series of report.series) {
    const prefix = series.authority.slice(0, prefixLength);
    if (prefix.length !== prefixLength) {
      throw new Error('authority portfolio history has an invalid authority address');
    }
    const rows = seriesByPrefix.get(prefix) ?? [];
    rows.push(series);
    seriesByPrefix.set(prefix, rows);
  }

  await rm(directory, { recursive: true, force: true });
  await mkdir(directory, { recursive: true });
  const prefixes = [...seriesByPrefix.keys()].sort((left, right) =>
    left.localeCompare(right)
  );
  for (const prefix of prefixes) {
    const shard = {
      schema_version: 1,
      artifact_kind: 'spyx_authority_portfolio_history_shard',
      source_schema_version: report.schema_version,
      source_binding: report.source_binding,
      coverage: report.coverage,
      point_fields: report.point_fields,
      prefix_length: prefixLength,
      prefix,
      series: seriesByPrefix.get(prefix)
    };
    await writeFile(resolve(directory, shardFileName(prefix)), `${JSON.stringify(shard)}\n`);
  }
  const index = {
    schema_version: 1,
    artifact_kind: 'spyx_authority_portfolio_history_shard_index',
    source_schema_version: report.schema_version,
    source_binding: report.source_binding,
    coverage: report.coverage,
    point_fields: report.point_fields,
    prefix_length: prefixLength,
    prefixes,
    authority_series: report.series.length,
    history_points: report.series.reduce((sum, series) => sum + series.points.length, 0)
  };
  await writeFile(resolve(directory, 'index.json'), `${JSON.stringify(index)}\n`);
  return index;
}

function shardFileName(prefix) {
  return `${Buffer.from(prefix, 'utf8').toString('hex')}.json`;
}

const outputBytes = Buffer.byteLength(JSON.stringify(compact));
const programOutputBytes = Buffer.byteLength(JSON.stringify(compactPrograms));
const authorityPortfolioOutputBytes = Buffer.byteLength(
  JSON.stringify(authorityPortfolioOutput)
);
const authorityPortfolioTableOutputBytes = Buffer.byteLength(
  JSON.stringify(authorityPortfolioTableOutput)
);
const pdaAuthorityEstimateOutputBytes = Buffer.byteLength(
  JSON.stringify(pdaAuthorityEstimateOutput)
);
console.log(
  `SPYx summary${options.validateOnly ? ' validation' : ''}: ${daily.length} calendar days, ${compact.compact_build.omitted_rpc_per_address_rows} RPC address rows omitted, ${outputBytes} bytes; ${compactPrograms.programs.length} programs, ${programOutputBytes} bytes; authority portfolios ${authorityPortfolioOutputBytes} bytes, table ${authorityPortfolioTableOutputBytes} bytes; authority history ${strictAuthorityPortfolioHistory?.coverage.history_points ?? 0} points; PDA authority estimates ${pdaAuthorityEstimateOutputBytes} bytes`
);

function buildAuthorityPortfolioTable(report) {
  return {
    schema_version: 1,
    artifact_kind: 'spyx_authority_portfolio_table',
    source_binding: report.source_binding,
    coverage: {
      complete: report.coverage.complete,
      candidate_flow_evidence_complete:
        report.coverage.candidate_flow_evidence_complete,
      transactions_scanned: report.coverage.transactions_scanned
    },
    portfolios: report.portfolios.map((portfolio) => ({
      authority: portfolio.authority,
      authority_kind: portfolio.authority_kind,
      direct_public_balance: portfolio.direct_public_balance,
      estimated_defi_claim: portfolio.estimated_defi_claim,
      estimated_total_exposure: portfolio.estimated_total_exposure,
      programs_used: portfolio.programs_used,
      custody_owners: [
        ...new Set(portfolio.claim_components.map((component) => component.custody_owner))
      ]
    })),
    protocol_custody: report.protocol_custody
  };
}

async function buildStrictReplaySummary(path, historyReport, expectedSha256) {
  const bytes = await readFile(path);
  const sourceReportSha256 = createHash('sha256').update(bytes).digest('hex');
  if (expectedSha256) {
    assertSha256(expectedSha256, 'strict replay SHA-256 pin');
    assertSame(
      sourceReportSha256,
      expectedSha256.toLowerCase(),
      'strict replay report SHA-256 pin'
    );
  }
  const report = JSON.parse(bytes.toString('utf8'));
  assertObject(report.source, 'strict replay source');
  assertObject(report.replayed_state, 'strict replay replayed_state');
  assertObject(report.counters, 'strict replay counters');
  assertObject(report.blockers, 'strict replay blockers');
  if (report.artifact_kind !== 'spyx_public_balance_instruction_replay') {
    throw new Error('The strict replay report has an unexpected artifact kind');
  }
  const replayedState = normalizeReplayedState(report.replayed_state);
  validateStrictReplayUiFields(report);
  validateStrictReplaySourceIdentity(report, historyReport.source);
  validateCompleteReplayState(report, replayedState, historyReport);
  const authorityPortfolios = normalizeAuthorityPortfolioReport(
    report.authority_portfolios,
    report,
    historyReport
  );
  const authorityPortfolioHistory = normalizeAuthorityPortfolioHistoryReport(
    report.authority_portfolio_history,
    report,
    authorityPortfolios
  );

  return {
    present: true,
    source_report_sha256: sourceReportSha256,
    schema_version: report.schema_version,
    artifact_kind: report.artifact_kind,
    bounded_selected_dump_scan_complete: report.bounded_selected_dump_scan_complete === true,
    instruction_replay_implemented: report.instruction_replay_implemented === true,
    instruction_replay_matches_metadata_for_complete_spyx_selected_history:
      report.instruction_replay_matches_metadata_for_complete_spyx_selected_history === true,
    proof_scope: report.proof_scope,
    status: report.status,
    source: report.source,
    replayed_state: replayedState,
    holder_authority: normalizeHolderAuthority(
      report.holder_authority,
      historyReport.final_public_balance
    ),
    authority_portfolios: authorityPortfolios,
    authority_portfolio_history: authorityPortfolioHistory,
    counters: report.counters,
    instruction_names: report.instruction_names ?? {},
    census_findings: report.census_findings ?? {},
    blockers: report.blockers,
    first_failure: report.first_failure ?? null,
    elapsed_seconds: report.elapsed_seconds
  };
}

function emptyStrictReplaySummary() {
  return {
    present: false,
    instruction_replay_implemented: false,
    instruction_replay_matches_metadata_for_complete_spyx_selected_history: false,
    status: 'not_performed',
    replayed_state: null,
    holder_authority: null,
    authority_portfolios: null,
    authority_portfolio_history: null,
    counters: {},
    blockers: {},
    first_failure: null
  };
}

function normalizeAuthorityPortfolioHistoryReport(
  value,
  replayReport,
  currentPortfolioReport
) {
  if (value === undefined || value === null) return null;
  const label = 'strict replay authority_portfolio_history';
  if (!currentPortfolioReport) {
    throw new Error(`${label} requires a current authority portfolio report`);
  }
  assertObject(value, label);
  assertSame(value.schema_version, 2, `${label}.schema_version`);
  assertSame(
    value.artifact_kind,
    'spyx_authority_portfolio_history',
    `${label}.artifact_kind`
  );

  assertObject(value.source_binding, `${label}.source_binding`);
  for (const field of ['mint', 'first_epoch', 'last_epoch']) {
    assertSame(
      value.source_binding[field],
      currentPortfolioReport.source_binding[field],
      `${label}.source_binding.${field}`
    );
  }
  for (const field of [
    'manifest_sha256',
    'transactions_sha256',
    'registry_sha256',
    'replay_state_sha256'
  ]) {
    assertSha256(value.source_binding[field], `${label}.source_binding.${field}`);
    assertSame(
      value.source_binding[field],
      currentPortfolioReport.source_binding[field],
      `${label}.source_binding.${field}`
    );
  }
  assertSame(
    value.source_binding.transactions_sha256,
    replayReport.source.expected_transaction_sha256,
    `${label}.source_binding.transactions_sha256`
  );

  assertObject(value.coverage, `${label}.coverage`);
  assertSame(value.coverage.complete, true, `${label}.coverage.complete`);
  assertSame(
    value.coverage.method,
    'forward_replay_216000_slot_window_end_sparse_aggregate_tuple_v2',
    `${label}.coverage.method`
  );
  assertSame(value.coverage.slot_window_width, 216_000, `${label}.coverage.slot_window_width`);
  assertSame(
    value.coverage.final_sample_matches_current_portfolio,
    true,
    `${label}.coverage.final_sample_matches_current_portfolio`
  );
  for (const field of [
    'transactions_scanned',
    'state_samples',
    'authority_series',
    'history_points'
  ]) {
    assertNonNegativeSafeInteger(value.coverage[field], `${label}.coverage.${field}`);
  }
  assertSame(
    value.coverage.transactions_scanned,
    replayReport.counters.transactions_scanned,
    `${label}.coverage.transactions_scanned`
  );
  validateStringRecord(value.coverage.definitions, `${label}.coverage.definitions`);
  const pointFields = [
    'transaction_id',
    'slot',
    'block_time',
    'direct_public_balance_raw',
    'estimated_defi_claim_raw'
  ];
  assertArray(value.point_fields, `${label}.point_fields`);
  if (!isDeepStrictEqual(value.point_fields, pointFields)) {
    throw new Error(`${label}.point_fields is not supported`);
  }

  assertArray(value.series, `${label}.series`);
  assertSame(value.coverage.authority_series, value.series.length, `${label}.coverage.authority_series`);
  const authorities = new Set();
  const seriesByAuthority = new Map();
  let historyPointCount = 0;
  const normalizedSeries = value.series.map((series, seriesIndex) => {
    const seriesLabel = `${label}.series[${seriesIndex}]`;
    assertObject(series, seriesLabel);
    assertNonEmptyString(series.authority, `${seriesLabel}.authority`);
    if (authorities.has(series.authority)) {
      throw new Error(`${seriesLabel}.authority is duplicated`);
    }
    authorities.add(series.authority);
    assertArray(series.points, `${seriesLabel}.points`);
    if (series.points.length === 0) throw new Error(`${seriesLabel}.points is empty`);
    let previousTransactionId = null;
    const points = series.points.map((point, pointIndex) => {
      const pointLabel = `${seriesLabel}.points[${pointIndex}]`;
      assertArray(point, pointLabel);
      if (point.length !== pointFields.length) {
        throw new Error(`${pointLabel} is not a five-field tuple`);
      }
      const [transactionId, slot, blockTime, directRaw, claimRaw] = point;
      assertNonNegativeSafeInteger(transactionId, `${pointLabel}.transaction_id`);
      assertNonNegativeSafeInteger(slot, `${pointLabel}.slot`);
      if (blockTime !== null && !Number.isSafeInteger(blockTime)) {
        throw new Error(`${pointLabel}.block_time is not a safe integer or null`);
      }
      for (const [rawAmount, field] of [
        [directRaw, 'direct_public_balance_raw'],
        [claimRaw, 'estimated_defi_claim_raw']
      ]) {
        if (typeof rawAmount !== 'string' || !/^(0|[1-9][0-9]*)$/.test(rawAmount)) {
          throw new Error(`${pointLabel}.${field} is not an unsigned decimal string`);
        }
      }
      if (transactionId >= value.coverage.transactions_scanned) {
        throw new Error(`${pointLabel}.transaction_id is outside the scanned transaction set`);
      }
      if (previousTransactionId !== null && transactionId <= previousTransactionId) {
        throw new Error(`${seriesLabel}.points is not in transaction order`);
      }
      previousTransactionId = transactionId;
      historyPointCount += 1;
      return point;
    });
    const normalized = { ...series, points };
    seriesByAuthority.set(series.authority, normalized);
    return normalized;
  });
  assertSame(value.coverage.history_points, historyPointCount, `${label}.coverage.history_points`);

  for (const portfolio of currentPortfolioReport.portfolios) {
    const series = seriesByAuthority.get(portfolio.authority);
    if (!series) {
      throw new Error(`${label} has no final series for ${portfolio.authority}`);
    }
    const finalPoint = series.points.at(-1);
    const directRaw = finalPoint[3];
    const claimRaw = finalPoint[4];
    assertSame(
      directRaw,
      portfolio.direct_public_balance.raw_amount,
      `${label} final ${portfolio.authority} direct_public_balance`
    );
    assertSame(
      claimRaw,
      portfolio.estimated_defi_claim.raw_amount,
      `${label} final ${portfolio.authority} estimated_defi_claim`
    );
    assertSame(
      (BigInt(directRaw) + BigInt(claimRaw)).toString(),
      portfolio.estimated_total_exposure.raw_amount,
      `${label} final ${portfolio.authority} estimated_total_exposure`
    );
  }

  return { ...value, series: normalizedSeries };
}

function normalizeAuthorityPortfolioReport(value, replayReport, historyReport) {
  if (value === undefined || value === null) return null;
  const label = 'strict replay authority_portfolios';
  const decimals = historyReport.final_public_balance.decimals;
  assertObject(value, label);
  assertSame(value.schema_version, 1, `${label}.schema_version`);
  assertSame(
    value.artifact_kind,
    'spyx_authority_portfolio_heuristic',
    `${label}.artifact_kind`
  );

  assertObject(value.source_binding, `${label}.source_binding`);
  const sourceBinding = value.source_binding;
  assertSame(sourceBinding.mint, replayReport.source.mint, `${label}.source_binding.mint`);
  assertSame(
    sourceBinding.first_epoch,
    replayReport.source.first_epoch,
    `${label}.source_binding.first_epoch`
  );
  assertSame(
    sourceBinding.last_epoch,
    replayReport.source.last_epoch,
    `${label}.source_binding.last_epoch`
  );
  for (const field of [
    'manifest_sha256',
    'transactions_sha256',
    'registry_sha256',
    'replay_state_sha256'
  ]) {
    assertSha256(sourceBinding[field], `${label}.source_binding.${field}`);
  }
  assertSame(
    sourceBinding.manifest_sha256,
    replayReport.source.manifest_sha256,
    `${label}.source_binding.manifest_sha256`
  );
  assertSame(
    sourceBinding.transactions_sha256,
    replayReport.source.expected_transaction_sha256,
    `${label}.source_binding.transactions_sha256`
  );
  assertSame(
    sourceBinding.registry_sha256,
    replayReport.source.registry_sha256,
    `${label}.source_binding.registry_sha256`
  );
  assertSame(
    sourceBinding.replay_state_sha256,
    replayReport.replayed_state.state_sha256,
    `${label}.source_binding.replay_state_sha256`
  );

  assertObject(value.coverage, `${label}.coverage`);
  if (typeof value.coverage.complete !== 'boolean') {
    throw new Error(`${label}.coverage.complete is not a boolean`);
  }
  assertSame(
    value.coverage.method,
    'committed_non_dex_owner_net_flow_v1',
    `${label}.coverage.method`
  );
  const candidateFlowEvidenceComplete =
    value.coverage.candidate_flow_evidence_complete ?? false;
  if (typeof candidateFlowEvidenceComplete !== 'boolean') {
    throw new Error(`${label}.coverage.candidate_flow_evidence_complete is not a boolean`);
  }
  for (const field of [
    'transactions_scanned',
    'parsed_dex_swap_transactions_excluded',
    'candidate_deposit_transactions',
    'candidate_return_transactions',
    'ambiguous_owner_delta_transactions_excluded',
    'current_positive_off_curve_custody_owners'
  ]) {
    assertNonNegativeSafeInteger(value.coverage[field], `${label}.coverage.${field}`);
  }
  assertSame(
    value.coverage.transactions_scanned,
    replayReport.counters.transactions_scanned,
    `${label}.coverage.transactions_scanned`
  );
  validateStringRecord(value.coverage.definitions, `${label}.coverage.definitions`);

  assertArray(value.portfolios, `${label}.portfolios`);
  assertArray(value.protocol_custody, `${label}.protocol_custody`);
  assertArray(value.pda_creation_provenance, `${label}.pda_creation_provenance`);

  const portfolios = [];
  const authorities = new Set();
  const claimsByCustody = new Map();
  const candidateAuthoritiesByCustody = new Map();
  let directPortfolioBalance = 0n;
  let positiveDirectPortfolioCount = 0;
  let previousPortfolioKey = null;
  for (const [index, portfolio] of value.portfolios.entries()) {
    const rowLabel = `${label}.portfolios[${index}]`;
    assertObject(portfolio, rowLabel);
    assertNonEmptyString(portfolio.authority, `${rowLabel}.authority`);
    if (authorities.has(portfolio.authority)) {
      throw new Error(`${rowLabel}.authority is duplicated`);
    }
    authorities.add(portfolio.authority);
    if (
      portfolio.authority_kind !== 'observed_transaction_signer' &&
      portfolio.authority_kind !== 'other_on_curve_account'
    ) {
      throw new Error(`${rowLabel}.authority_kind is not supported`);
    }
    validateAmount(portfolio.direct_public_balance, `${rowLabel}.direct_public_balance`, decimals);
    validateAmount(portfolio.estimated_defi_claim, `${rowLabel}.estimated_defi_claim`, decimals);
    validateAmount(
      portfolio.estimated_total_exposure,
      `${rowLabel}.estimated_total_exposure`,
      decimals
    );
    const direct = BigInt(portfolio.direct_public_balance.raw_amount);
    const claim = BigInt(portfolio.estimated_defi_claim.raw_amount);
    const total = BigInt(portfolio.estimated_total_exposure.raw_amount);
    if (direct + claim !== total) {
      throw new Error(`${rowLabel}.estimated_total_exposure does not equal direct plus claim`);
    }
    directPortfolioBalance += direct;
    if (direct > 0n) positiveDirectPortfolioCount += 1;
    const portfolioKey = [total, portfolio.authority];
    if (
      previousPortfolioKey &&
      (total > previousPortfolioKey[0] ||
        (total === previousPortfolioKey[0] &&
          compareByteArrays(
            decodeBase58Pubkey(portfolio.authority, `${rowLabel}.authority`),
            decodeBase58Pubkey(
              previousPortfolioKey[1],
              `${label}.previous portfolio authority`
            )
          ) < 0))
    ) {
      throw new Error(`${label}.portfolios is not in deterministic exposure order`);
    }
    previousPortfolioKey = portfolioKey;

    assertArray(portfolio.programs_used, `${rowLabel}.programs_used`);
    const programs = new Set();
    for (const [programIndex, programId] of portfolio.programs_used.entries()) {
      assertNonEmptyString(programId, `${rowLabel}.programs_used[${programIndex}]`);
      if (programs.has(programId)) {
        throw new Error(`${rowLabel}.programs_used contains a duplicate program ID`);
      }
      programs.add(programId);
    }

    assertArray(portfolio.claim_components, `${rowLabel}.claim_components`);
    let componentClaim = 0n;
    const componentKeys = new Set();
    const normalizedComponents = [];
    for (const [componentIndex, component] of portfolio.claim_components.entries()) {
      const componentLabel = `${rowLabel}.claim_components[${componentIndex}]`;
      assertObject(component, componentLabel);
      assertNonEmptyString(component.custody_owner, `${componentLabel}.custody_owner`);
      if (component.program_id !== null) {
        assertNonEmptyString(component.program_id, `${componentLabel}.program_id`);
      }
      const componentKey = `${component.custody_owner}\0${component.program_id ?? ''}`;
      if (componentKeys.has(componentKey)) {
        throw new Error(`${componentLabel} duplicates a custody/program component`);
      }
      componentKeys.add(componentKey);
      for (const field of [
        'observed_deposited_principal',
        'observed_returned_principal',
        'candidate_net_principal',
        'attributed_claim'
      ]) {
        validateAmount(component[field], `${componentLabel}.${field}`, decimals);
      }
      for (const field of ['deposit_transaction_count', 'return_transaction_count']) {
        assertNonNegativeSafeInteger(component[field], `${componentLabel}.${field}`);
      }
      assertSame(
        component.confidence,
        'heuristic_owner_net_flow_capped_by_current_custody',
        `${componentLabel}.confidence`
      );
      const deposited = BigInt(component.observed_deposited_principal.raw_amount);
      const returned = BigInt(component.observed_returned_principal.raw_amount);
      const net = BigInt(component.candidate_net_principal.raw_amount);
      const attributed = BigInt(component.attributed_claim.raw_amount);
      const expectedNet = deposited > returned ? deposited - returned : 0n;
      if (net !== expectedNet) {
        throw new Error(`${componentLabel}.candidate_net_principal is not positive net principal`);
      }
      if (attributed > net) {
        throw new Error(`${componentLabel}.attributed_claim exceeds candidate net principal`);
      }
      const candidateFlowEvidence = component.candidate_flow_evidence ?? [];
      assertArray(candidateFlowEvidence, `${componentLabel}.candidate_flow_evidence`);
      let previousTransactionId = null;
      let evidenceDeposited = 0n;
      let evidenceReturned = 0n;
      let evidenceDepositCount = 0;
      let evidenceReturnCount = 0;
      for (const [flowIndex, flow] of candidateFlowEvidence.entries()) {
        const flowLabel = `${componentLabel}.candidate_flow_evidence[${flowIndex}]`;
        assertObject(flow, flowLabel);
        assertNonNegativeSafeInteger(flow.transaction_id, `${flowLabel}.transaction_id`);
        if (flow.transaction_id >= value.coverage.transactions_scanned) {
          throw new Error(`${flowLabel}.transaction_id is outside the scanned transaction set`);
        }
        if (previousTransactionId !== null && flow.transaction_id <= previousTransactionId) {
          throw new Error(`${componentLabel}.candidate_flow_evidence is not in transaction order`);
        }
        previousTransactionId = flow.transaction_id;
        assertNonNegativeSafeInteger(flow.slot, `${flowLabel}.slot`);
        if (flow.block_time !== undefined && !Number.isSafeInteger(flow.block_time)) {
          throw new Error(`${flowLabel}.block_time is not a safe integer`);
        }
        if (flow.direction !== 'deposit' && flow.direction !== 'return') {
          throw new Error(`${flowLabel}.direction is not supported`);
        }
        if (typeof flow.raw_amount !== 'string' || !/^[1-9][0-9]*$/.test(flow.raw_amount)) {
          throw new Error(`${flowLabel}.raw_amount is not a positive raw amount`);
        }
        if (
          flow.matched_principal_raw_amount !== undefined &&
          (typeof flow.matched_principal_raw_amount !== 'string' ||
            !/^[1-9][0-9]*$/.test(flow.matched_principal_raw_amount))
        ) {
          throw new Error(
            `${flowLabel}.matched_principal_raw_amount is not a positive raw amount`
          );
        }
        const rawAmount = BigInt(flow.raw_amount);
        const matchedPrincipal = BigInt(flow.matched_principal_raw_amount ?? flow.raw_amount);
        if (matchedPrincipal > rawAmount) {
          throw new Error(`${flowLabel}.matched_principal_raw_amount exceeds raw_amount`);
        }
        if (flow.direction === 'deposit') {
          if (matchedPrincipal !== rawAmount) {
            throw new Error(`${flowLabel} partially matches a candidate deposit`);
          }
          evidenceDeposited += matchedPrincipal;
          evidenceDepositCount += 1;
        } else {
          evidenceReturned += matchedPrincipal;
          evidenceReturnCount += 1;
        }
      }
      if (
        candidateFlowEvidenceComplete &&
        (evidenceDeposited !== deposited ||
          evidenceReturned !== returned ||
          evidenceDepositCount !== component.deposit_transaction_count ||
          evidenceReturnCount !== component.return_transaction_count)
      ) {
        throw new Error(`${componentLabel}.candidate_flow_evidence does not match its aggregate`);
      }
      componentClaim += attributed;
      const custodyClaims = claimsByCustody.get(component.custody_owner) ?? {
        attributed: 0n,
        candidate: 0n
      };
      custodyClaims.attributed += attributed;
      custodyClaims.candidate += net;
      claimsByCustody.set(component.custody_owner, custodyClaims);
      if (net > 0n) {
        const custodyAuthorities = candidateAuthoritiesByCustody.get(component.custody_owner) ?? new Set();
        custodyAuthorities.add(portfolio.authority);
        candidateAuthoritiesByCustody.set(component.custody_owner, custodyAuthorities);
      }
      if (component.program_id && !programs.has(component.program_id)) {
        throw new Error(`${componentLabel}.program_id is absent from programs_used`);
      }
      normalizedComponents.push({
        ...component,
        candidate_flow_evidence: candidateFlowEvidence
      });
    }
    if (componentClaim !== claim) {
      throw new Error(`${rowLabel}.estimated_defi_claim does not equal its claim components`);
    }
    portfolios.push({ ...portfolio, claim_components: normalizedComponents });
  }

  const protocolCustody = [];
  const custodyOwners = new Set();
  let directCustodyBalance = 0n;
  for (const [index, custody] of value.protocol_custody.entries()) {
    const rowLabel = `${label}.protocol_custody[${index}]`;
    assertObject(custody, rowLabel);
    assertNonEmptyString(custody.custody_owner, `${rowLabel}.custody_owner`);
    if (custodyOwners.has(custody.custody_owner)) {
      throw new Error(`${rowLabel}.custody_owner is duplicated`);
    }
    custodyOwners.add(custody.custody_owner);
    if (custody.program_id !== null) {
      assertNonEmptyString(custody.program_id, `${rowLabel}.program_id`);
    }
    for (const field of [
      'direct_custody_balance',
      'candidate_net_principal',
      'attributed_claim',
      'unallocated_custody',
      'claim_excess'
    ]) {
      validateAmount(custody[field], `${rowLabel}.${field}`, decimals);
    }
    assertNonNegativeSafeInteger(
      custody.candidate_authority_count,
      `${rowLabel}.candidate_authority_count`
    );
    assertSame(
      custody.confidence,
      'heuristic_owner_net_flow_capped_by_current_custody',
      `${rowLabel}.confidence`
    );
    const direct = BigInt(custody.direct_custody_balance.raw_amount);
    const candidate = BigInt(custody.candidate_net_principal.raw_amount);
    const attributed = BigInt(custody.attributed_claim.raw_amount);
    const unallocated = BigInt(custody.unallocated_custody.raw_amount);
    const excess = BigInt(custody.claim_excess.raw_amount);
    if (direct === 0n) {
      throw new Error(`${rowLabel}.direct_custody_balance is not positive`);
    }
    directCustodyBalance += direct;
    if (attributed + unallocated !== direct) {
      throw new Error(`${rowLabel} does not reconcile attributed and unallocated custody`);
    }
    if (attributed + excess !== candidate) {
      throw new Error(`${rowLabel} does not reconcile candidate principal and claim excess`);
    }
    const linkedClaims = claimsByCustody.get(custody.custody_owner) ?? {
      attributed: 0n,
      candidate: 0n
    };
    if (linkedClaims.attributed !== attributed || linkedClaims.candidate !== candidate) {
      throw new Error(`${rowLabel} does not match portfolio claim components`);
    }
    const linkedAuthorities = candidateAuthoritiesByCustody.get(custody.custody_owner)?.size ?? 0;
    if (custody.candidate_authority_count !== linkedAuthorities) {
      throw new Error(`${rowLabel}.candidate_authority_count does not match portfolio components`);
    }
    protocolCustody.push(custody);
  }
  if (protocolCustody.length !== value.coverage.current_positive_off_curve_custody_owners) {
    throw new Error(`${label}.protocol_custody does not match its coverage count`);
  }
  for (const custodyOwner of claimsByCustody.keys()) {
    if (!custodyOwners.has(custodyOwner)) {
      throw new Error(`${label} has a claim component without a protocol custody row`);
    }
  }
  for (const portfolio of portfolios) {
    if (
      custodyOwners.has(portfolio.authority) &&
      BigInt(portfolio.direct_public_balance.raw_amount) !== 0n
    ) {
      throw new Error(
        `${label} counts ${portfolio.authority} as both direct portfolio balance and custody`
      );
    }
  }
  const finalPublicBalance = BigInt(
    historyReport.final_public_balance.public_raw_balance_sum.raw_amount
  );
  if (directPortfolioBalance + directCustodyBalance !== finalPublicBalance) {
    throw new Error(
      `${label} direct portfolio and custody balances do not equal the final public balance`
    );
  }
  if (
    positiveDirectPortfolioCount + protocolCustody.length !==
    historyReport.final_public_balance.positive_public_balance_holders
  ) {
    throw new Error(
      `${label} direct portfolio and custody holder counts do not equal the final holder count`
    );
  }

  const pdaCreationProvenance = [];
  const provenanceKeys = new Set();
  for (const [index, provenance] of value.pda_creation_provenance.entries()) {
    const rowLabel = `${label}.pda_creation_provenance[${index}]`;
    assertObject(provenance, rowLabel);
    assertNonEmptyString(provenance.subject_pda, `${rowLabel}.subject_pda`);
    if (provenance.event_kind !== 'account_creation' && provenance.event_kind !== 'owner_assignment') {
      throw new Error(`${rowLabel}.event_kind is not supported`);
    }
    assertNonEmptyString(provenance.system_instruction, `${rowLabel}.system_instruction`);
    assertNonEmptyString(
      provenance.runtime_owner_program_id,
      `${rowLabel}.runtime_owner_program_id`
    );
    if (provenance.direct_caller_program_id !== null) {
      assertNonEmptyString(
        provenance.direct_caller_program_id,
        `${rowLabel}.direct_caller_program_id`
      );
    }
    if (provenance.create_with_seed_base !== null) {
      assertNonEmptyString(
        provenance.create_with_seed_base,
        `${rowLabel}.create_with_seed_base`
      );
    }
    assertArray(provenance.signer_candidates, `${rowLabel}.signer_candidates`);
    const signers = new Set();
    for (const [signerIndex, signer] of provenance.signer_candidates.entries()) {
      assertNonEmptyString(signer, `${rowLabel}.signer_candidates[${signerIndex}]`);
      if (signers.has(signer)) {
        throw new Error(`${rowLabel}.signer_candidates contains a duplicate address`);
      }
      signers.add(signer);
    }
    assertSame(
      provenance.confidence,
      'provenance_only_no_amount_assigned',
      `${rowLabel}.confidence`
    );
    if (provenance.proves_beneficial_ownership !== false) {
      throw new Error(`${rowLabel} must not claim beneficial ownership`);
    }
    assertObject(provenance.location, `${rowLabel}.location`);
    for (const field of [
      'transaction_id',
      'outer_index',
      'source_epoch',
      'slot',
      'source_block_id',
      'tx_index'
    ]) {
      assertNonNegativeSafeInteger(provenance.location[field], `${rowLabel}.location.${field}`);
    }
    if (provenance.location.inner_index !== undefined) {
      assertNonNegativeSafeInteger(
        provenance.location.inner_index,
        `${rowLabel}.location.inner_index`
      );
    }
    const provenanceKey = [
      provenance.subject_pda,
      provenance.location.transaction_id,
      provenance.location.outer_index,
      provenance.location.inner_index ?? -1
    ].join('\0');
    if (provenanceKeys.has(provenanceKey)) {
      throw new Error(`${rowLabel} duplicates a creation-provenance event`);
    }
    provenanceKeys.add(provenanceKey);
    pdaCreationProvenance.push(provenance);
  }

  return {
    ...value,
    coverage: {
      ...value.coverage,
      candidate_flow_evidence_complete: candidateFlowEvidenceComplete
    },
    source_binding: Object.fromEntries(
      Object.entries(sourceBinding).map(([key, fieldValue]) => [
        key,
        typeof fieldValue === 'string' && key.endsWith('_sha256')
          ? fieldValue.toLowerCase()
          : fieldValue
      ])
    ),
    portfolios,
    protocol_custody: protocolCustody,
    pda_creation_provenance: pdaCreationProvenance
  };
}

function normalizeReplayedState(value) {
  if (typeof value.history_complete !== 'boolean') {
    throw new Error('strict replay replayed_state.history_complete is not a boolean');
  }

  const countFields = [
    'tracked_accounts',
    'open_accounts',
    'closed_accounts',
    'positive_public_balance_accounts'
  ];
  for (const field of countFields) {
    assertNonNegativeSafeInteger(value[field], `strict replay replayed_state.${field}`);
  }
  if (value.open_accounts + value.closed_accounts !== value.tracked_accounts) {
    throw new Error('strict replay replayed_state open and closed counts do not equal tracked accounts');
  }
  if (value.positive_public_balance_accounts > value.open_accounts) {
    throw new Error('strict replay replayed_state positive account count exceeds open accounts');
  }
  if (typeof value.public_raw_balance !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value.public_raw_balance)) {
    throw new Error('strict replay replayed_state.public_raw_balance is not an unsigned decimal string');
  }
  assertSha256(value.state_sha256, 'strict replay replayed_state.state_sha256');

  return {
    history_complete: value.history_complete,
    tracked_accounts: value.tracked_accounts,
    open_accounts: value.open_accounts,
    closed_accounts: value.closed_accounts,
    positive_public_balance_accounts: value.positive_public_balance_accounts,
    public_raw_balance: value.public_raw_balance,
    state_sha256: value.state_sha256.toLowerCase()
  };
}

function normalizeHolderAuthority(value, finalBalance) {
  if (value === undefined || value === null) return null;
  const label = 'strict replay holder_authority';
  assertObject(value, label);
  if (typeof value.complete !== 'boolean') {
    throw new Error(`${label}.complete is not a boolean`);
  }
  validateStringRecord(value.definitions, `${label}.definitions`);

  assertArray(value.class_totals, `${label}.class_totals`);
  if (value.class_totals.length !== HOLDER_AUTHORITY_KINDS.length) {
    throw new Error(`${label}.class_totals must contain exactly four authority classes`);
  }
  const totalsByKind = new Map();
  let totalHolders = 0;
  let totalTokenAccounts = 0;
  let totalRawBalance = 0n;
  for (const [index, row] of value.class_totals.entries()) {
    const rowLabel = `${label}.class_totals[${index}]`;
    assertObject(row, rowLabel);
    if (!HOLDER_AUTHORITY_KINDS.includes(row.authority_kind)) {
      throw new Error(`${rowLabel}.authority_kind is not supported`);
    }
    if (totalsByKind.has(row.authority_kind)) {
      throw new Error(`${label}.class_totals has a duplicate authority class`);
    }
    assertNonNegativeSafeInteger(row.holder_count, `${rowLabel}.holder_count`);
    assertNonNegativeSafeInteger(row.token_account_count, `${rowLabel}.token_account_count`);
    validateAmount(row.public_balance, `${rowLabel}.public_balance`, finalBalance.decimals);
    totalsByKind.set(row.authority_kind, row);
    totalHolders = safeIntegerAdd(totalHolders, row.holder_count, `${label} holder count`);
    totalTokenAccounts = safeIntegerAdd(
      totalTokenAccounts,
      row.token_account_count,
      `${label} token-account count`
    );
    totalRawBalance += BigInt(row.public_balance.raw_amount);
  }
  for (const kind of HOLDER_AUTHORITY_KINDS) {
    if (!totalsByKind.has(kind)) throw new Error(`${label}.class_totals is missing ${kind}`);
  }

  const allRows = validateClassifiedHolderRows(
    value.largest_25_all,
    `${label}.largest_25_all`,
    finalBalance.decimals,
    null
  );
  assertObject(value.largest_25_by_class, `${label}.largest_25_by_class`);
  const rowsByKind = new Map();
  for (const kind of HOLDER_AUTHORITY_KINDS) {
    rowsByKind.set(
      kind,
      validateClassifiedHolderRows(
        value.largest_25_by_class[kind],
        `${label}.largest_25_by_class.${kind}`,
        finalBalance.decimals,
        kind
      )
    );
  }
  for (const row of allRows.values()) {
    const classRow = rowsByKind.get(row.authority_kind)?.get(row.owner);
    if (!classRow || !sameHolderValue(row, classRow)) {
      throw new Error(`${label}.largest_25_all row is absent or different in its class list`);
    }
  }

  const activityExtensionFields = [
    'largest_25_by_activity_all',
    'largest_25_by_activity_by_class',
    'attributed_program_holders'
  ];
  const activityExtensionCount = activityExtensionFields.filter(
    (field) => value[field] !== undefined
  ).length;
  if (activityExtensionCount !== 0 && activityExtensionCount !== activityExtensionFields.length) {
    throw new Error(`${label} has an incomplete public activity extension`);
  }
  let attributedProgramRows = null;
  if (activityExtensionCount !== 0) {
    const activityAllRows = validateClassifiedHolderRows(
      value.largest_25_by_activity_all,
      `${label}.largest_25_by_activity_all`,
      finalBalance.decimals,
      null,
      { sortBy: 'activity', requireActivity: true }
    );
    assertObject(
      value.largest_25_by_activity_by_class,
      `${label}.largest_25_by_activity_by_class`
    );
    const activityRowsByKind = new Map();
    for (const kind of HOLDER_AUTHORITY_KINDS) {
      activityRowsByKind.set(
        kind,
        validateClassifiedHolderRows(
          value.largest_25_by_activity_by_class[kind],
          `${label}.largest_25_by_activity_by_class.${kind}`,
          finalBalance.decimals,
          kind,
          { sortBy: 'activity', requireActivity: true }
        )
      );
    }
    for (const row of activityAllRows.values()) {
      const classRow = activityRowsByKind.get(row.authority_kind)?.get(row.owner);
      if (!classRow || !sameHolderActivityValue(row, classRow)) {
        throw new Error(
          `${label}.largest_25_by_activity_all row is absent or different in its class list`
        );
      }
    }
    const attributedHolderCount =
      totalsByKind.get('attributed_program_derived_address').holder_count;
    attributedProgramRows = validateClassifiedHolderRows(
      value.attributed_program_holders,
      `${label}.attributed_program_holders`,
      finalBalance.decimals,
      'attributed_program_derived_address',
      {
        maxRows: attributedHolderCount,
        sortBy: 'balance',
        requireActivity: true
      }
    );
    assertSame(
      attributedProgramRows.size,
      attributedHolderCount,
      `${label} attributed program holder rows`
    );
  }

  if (value.off_curve_unattributed_holders !== undefined) {
    const offCurveHolderCount = totalsByKind.get('off_curve_unattributed').holder_count;
    const offCurveRows = validateClassifiedHolderRows(
      value.off_curve_unattributed_holders,
      `${label}.off_curve_unattributed_holders`,
      finalBalance.decimals,
      'off_curve_unattributed',
      {
        maxRows: offCurveHolderCount,
        sortBy: 'balance',
        requireActivity: activityExtensionCount !== 0
      }
    );
    assertSame(
      offCurveRows.size,
      offCurveHolderCount,
      `${label} complete off-curve unattributed holder rows`
    );
    let offCurveTokenAccounts = 0;
    let offCurveRawBalance = 0n;
    for (const row of offCurveRows.values()) {
      offCurveTokenAccounts = safeIntegerAdd(
        offCurveTokenAccounts,
        row.token_account_count,
        `${label} complete off-curve token-account count`
      );
      offCurveRawBalance += BigInt(row.public_balance.raw_amount);
    }
    const offCurveTotal = totalsByKind.get('off_curve_unattributed');
    assertSame(
      offCurveTokenAccounts,
      offCurveTotal.token_account_count,
      `${label} complete off-curve token-account total`
    );
    assertSame(
      offCurveRawBalance,
      BigInt(offCurveTotal.public_balance.raw_amount),
      `${label} complete off-curve public-balance total`
    );
  }

  assertArray(value.holdings_by_program, `${label}.holdings_by_program`);
  const programs = new Set();
  const programReports = new Map();
  let programHolderCount = 0;
  let programTokenAccountCount = 0;
  let programRawBalance = 0n;
  let programActivityTransactionLinks = 0;
  let programRawIncrease = 0n;
  let programRawDecrease = 0n;
  let previousProgramBalance = null;
  for (const [index, row] of value.holdings_by_program.entries()) {
    const rowLabel = `${label}.holdings_by_program[${index}]`;
    assertObject(row, rowLabel);
    assertNonEmptyString(row.program_id, `${rowLabel}.program_id`);
    if (programs.has(row.program_id)) {
      throw new Error(`${label}.holdings_by_program has a duplicate program`);
    }
    programs.add(row.program_id);
    programReports.set(row.program_id, row);
    assertNonNegativeSafeInteger(row.pda_holder_count, `${rowLabel}.pda_holder_count`);
    assertNonNegativeSafeInteger(row.token_account_count, `${rowLabel}.token_account_count`);
    if (row.pda_holder_count === 0 || row.token_account_count === 0) {
      throw new Error(`${rowLabel} counts must be greater than zero`);
    }
    validateAmount(row.public_balance, `${rowLabel}.public_balance`, finalBalance.decimals);
    const rawBalance = BigInt(row.public_balance.raw_amount);
    if (rawBalance === 0n) throw new Error(`${rowLabel}.public_balance must be positive`);
    if (previousProgramBalance !== null && rawBalance > previousProgramBalance) {
      throw new Error(`${label}.holdings_by_program is not in descending balance order`);
    }
    previousProgramBalance = rawBalance;
    const programActivityFields = [
      'owner_activity_transaction_links',
      'public_balance_increase',
      'public_balance_decrease',
      'public_activity_volume'
    ];
    const programActivityFieldCount = programActivityFields.filter(
      (field) => row[field] !== undefined
    ).length;
    if (
      (activityExtensionCount !== 0 && programActivityFieldCount !== programActivityFields.length) ||
      (programActivityFieldCount !== 0 &&
        programActivityFieldCount !== programActivityFields.length)
    ) {
      throw new Error(`${rowLabel} has incomplete public activity fields`);
    }
    if (programActivityFieldCount !== 0) {
      assertNonNegativeSafeInteger(
        row.owner_activity_transaction_links,
        `${rowLabel}.owner_activity_transaction_links`
      );
      validateAmount(
        row.public_balance_increase,
        `${rowLabel}.public_balance_increase`,
        finalBalance.decimals
      );
      validateAmount(
        row.public_balance_decrease,
        `${rowLabel}.public_balance_decrease`,
        finalBalance.decimals
      );
      validateAmount(
        row.public_activity_volume,
        `${rowLabel}.public_activity_volume`,
        finalBalance.decimals
      );
      const rawIncrease = BigInt(row.public_balance_increase.raw_amount);
      const rawDecrease = BigInt(row.public_balance_decrease.raw_amount);
      assertSame(
        rawIncrease + rawDecrease,
        BigInt(row.public_activity_volume.raw_amount),
        `${rowLabel} public activity volume`
      );
      programActivityTransactionLinks = safeIntegerAdd(
        programActivityTransactionLinks,
        row.owner_activity_transaction_links,
        `${label} program owner activity transaction links`
      );
      programRawIncrease += rawIncrease;
      programRawDecrease += rawDecrease;
    }
    programHolderCount = safeIntegerAdd(
      programHolderCount,
      row.pda_holder_count,
      `${label} program holder count`
    );
    programTokenAccountCount = safeIntegerAdd(
      programTokenAccountCount,
      row.token_account_count,
      `${label} program token-account count`
    );
    programRawBalance += rawBalance;
  }
  const attributed = totalsByKind.get('attributed_program_derived_address');
  assertSame(programHolderCount, attributed.holder_count, `${label} attributed program holders`);
  assertSame(
    programTokenAccountCount,
    attributed.token_account_count,
    `${label} attributed program token accounts`
  );
  assertSame(
    programRawBalance,
    BigInt(attributed.public_balance.raw_amount),
    `${label} attributed program balance`
  );
  if (attributedProgramRows !== null) {
    let holderActivityTransactionCount = 0;
    let holderRawIncrease = 0n;
    let holderRawDecrease = 0n;
    const expectedByProgram = new Map();
    for (const row of attributedProgramRows.values()) {
      const programId = row.pda_program_id;
      const expected = expectedByProgram.get(programId) ?? {
        holderCount: 0,
        tokenAccountCount: 0,
        rawBalance: 0n,
        activityTransactionLinks: 0,
        rawIncrease: 0n,
        rawDecrease: 0n
      };
      expected.holderCount = safeIntegerAdd(
        expected.holderCount,
        1,
        `${label} per-program holder count`
      );
      expected.tokenAccountCount = safeIntegerAdd(
        expected.tokenAccountCount,
        row.token_account_count,
        `${label} per-program token-account count`
      );
      expected.rawBalance += BigInt(row.public_balance.raw_amount);
      holderActivityTransactionCount = safeIntegerAdd(
        holderActivityTransactionCount,
        row.activity_transaction_count,
        `${label} attributed holder activity transaction count`
      );
      expected.activityTransactionLinks = safeIntegerAdd(
        expected.activityTransactionLinks,
        row.activity_transaction_count,
        `${label} per-program activity transaction links`
      );
      holderRawIncrease += BigInt(row.public_balance_increase.raw_amount);
      holderRawDecrease += BigInt(row.public_balance_decrease.raw_amount);
      expected.rawIncrease += BigInt(row.public_balance_increase.raw_amount);
      expected.rawDecrease += BigInt(row.public_balance_decrease.raw_amount);
      expectedByProgram.set(programId, expected);
    }
    assertSame(
      programActivityTransactionLinks,
      holderActivityTransactionCount,
      `${label} attributed program owner activity transaction links`
    );
    assertSame(
      programRawIncrease,
      holderRawIncrease,
      `${label} attributed program public balance increase`
    );
    assertSame(
      programRawDecrease,
      holderRawDecrease,
      `${label} attributed program public balance decrease`
    );
    assertSame(
      programReports.size,
      expectedByProgram.size,
      `${label} attributed per-program row count`
    );
    for (const [programId, expected] of expectedByProgram) {
      const actual = programReports.get(programId);
      if (!actual) {
        throw new Error(`${label}.holdings_by_program is missing program ${programId}`);
      }
      assertSame(
        actual.pda_holder_count,
        expected.holderCount,
        `${label} ${programId} holder count`
      );
      assertSame(
        actual.token_account_count,
        expected.tokenAccountCount,
        `${label} ${programId} token-account count`
      );
      assertSame(
        BigInt(actual.public_balance.raw_amount),
        expected.rawBalance,
        `${label} ${programId} public balance`
      );
      assertSame(
        actual.owner_activity_transaction_links,
        expected.activityTransactionLinks,
        `${label} ${programId} owner activity transaction links`
      );
      assertSame(
        BigInt(actual.public_balance_increase.raw_amount),
        expected.rawIncrease,
        `${label} ${programId} public balance increase`
      );
      assertSame(
        BigInt(actual.public_balance_decrease.raw_amount),
        expected.rawDecrease,
        `${label} ${programId} public balance decrease`
      );
    }
  }

  if (value.complete) {
    assertSame(
      totalHolders,
      finalBalance.positive_public_balance_holders,
      `${label} final holder count`
    );
    assertSame(
      totalTokenAccounts,
      finalBalance.active_public_token_accounts,
      `${label} final token-account count`
    );
    assertSame(
      totalRawBalance,
      BigInt(finalBalance.public_raw_balance_sum.raw_amount),
      `${label} final public balance`
    );
    assertSame(
      allRows.size,
      finalBalance.largest_25_holders.length,
      `${label} largest-holder row count`
    );
    for (const [index, expected] of finalBalance.largest_25_holders.entries()) {
      const actual = value.largest_25_all[index];
      if (!actual || !sameHolderValue(actual, expected)) {
        throw new Error(`${label}.largest_25_all[${index}] differs from token history`);
      }
    }
  }
  return value;
}

function validateClassifiedHolderRows(
  rows,
  label,
  decimals,
  expectedKind,
  { maxRows = 25, sortBy = 'balance', requireActivity = false } = {}
) {
  assertArray(rows, label);
  if (rows.length > maxRows) throw new Error(`${label} must contain at most ${maxRows} rows`);
  const owners = new Map();
  let previousSortAmount = null;
  for (const [index, row] of rows.entries()) {
    const rowLabel = `${label}[${index}]`;
    assertObject(row, rowLabel);
    assertNonEmptyString(row.owner, `${rowLabel}.owner`);
    if (owners.has(row.owner)) throw new Error(`${label} has a duplicate owner`);
    if (!HOLDER_AUTHORITY_KINDS.includes(row.authority_kind)) {
      throw new Error(`${rowLabel}.authority_kind is not supported`);
    }
    if (expectedKind !== null && row.authority_kind !== expectedKind) {
      throw new Error(`${rowLabel}.authority_kind differs from its class list`);
    }
    assertNonEmptyString(row.classification_evidence, `${rowLabel}.classification_evidence`);
    assertNonNegativeSafeInteger(
      row.signer_transaction_count,
      `${rowLabel}.signer_transaction_count`
    );
    assertNonNegativeSafeInteger(
      row.pda_program_evidence_count,
      `${rowLabel}.pda_program_evidence_count`
    );
    assertNonNegativeSafeInteger(row.token_account_count, `${rowLabel}.token_account_count`);
    if (row.token_account_count === 0) {
      throw new Error(`${rowLabel}.token_account_count must be greater than zero`);
    }
    if (row.authority_kind === 'observed_transaction_signer') {
      if (row.signer_transaction_count === 0 || row.pda_program_id !== null) {
        throw new Error(`${rowLabel} has invalid observed-signer evidence`);
      }
    } else if (row.authority_kind === 'attributed_program_derived_address') {
      if (
        row.signer_transaction_count !== 0 ||
        typeof row.pda_program_id !== 'string' ||
        row.pda_program_id.length === 0 ||
        row.pda_program_evidence_count === 0
      ) {
        throw new Error(`${rowLabel} has invalid attributed-PDA evidence`);
      }
    } else if (row.signer_transaction_count !== 0 || row.pda_program_id !== null) {
      throw new Error(`${rowLabel} has invalid unclassified authority evidence`);
    }
    if (row.runtime_account_owner !== undefined) {
      assertObject(row.runtime_account_owner, `${rowLabel}.runtime_account_owner`);
      if (
        row.runtime_account_owner.source !== 'committed_system_owner_instruction'
      ) {
        throw new Error(`${rowLabel}.runtime_account_owner.source is not supported`);
      }
      assertNonEmptyString(
        row.runtime_account_owner.program_id,
        `${rowLabel}.runtime_account_owner.program_id`
      );
      assertNonNegativeSafeInteger(
        row.runtime_account_owner.observation_count,
        `${rowLabel}.runtime_account_owner.observation_count`
      );
      assertNonNegativeSafeInteger(
        row.runtime_account_owner.owner_change_count,
        `${rowLabel}.runtime_account_owner.owner_change_count`
      );
      assertNonNegativeSafeInteger(
        row.runtime_account_owner.conflict_count,
        `${rowLabel}.runtime_account_owner.conflict_count`
      );
      if (row.runtime_account_owner.observation_count === 0) {
        throw new Error(`${rowLabel}.runtime_account_owner has no observations`);
      }
      if (
        row.runtime_account_owner.owner_change_count >
        row.runtime_account_owner.observation_count
      ) {
        throw new Error(`${rowLabel}.runtime_account_owner has too many owner changes`);
      }
      if (
        row.runtime_account_owner.conflict_count >
        row.runtime_account_owner.observation_count
      ) {
        throw new Error(`${rowLabel}.runtime_account_owner has too many conflicts`);
      }
      if (row.runtime_account_owner.proves_pda_derivation !== false) {
        throw new Error(`${rowLabel}.runtime_account_owner must not claim PDA derivation`);
      }
      assertObject(
        row.runtime_account_owner.last_observation,
        `${rowLabel}.runtime_account_owner.last_observation`
      );
      for (const field of [
        'transaction_id',
        'outer_index',
        'source_epoch',
        'slot',
        'source_block_id',
        'tx_index'
      ]) {
        assertNonNegativeSafeInteger(
          row.runtime_account_owner.last_observation[field],
          `${rowLabel}.runtime_account_owner.last_observation.${field}`
        );
      }
      if (row.runtime_account_owner.last_observation.inner_index !== undefined) {
        assertNonNegativeSafeInteger(
          row.runtime_account_owner.last_observation.inner_index,
          `${rowLabel}.runtime_account_owner.last_observation.inner_index`
        );
      }
    }
    validateAmount(row.public_balance, `${rowLabel}.public_balance`, decimals);
    const balance = BigInt(row.public_balance.raw_amount);
    if (balance === 0n) throw new Error(`${rowLabel}.public_balance must be positive`);
    const activityFields = [
      'activity_transaction_count',
      'public_balance_increase',
      'public_balance_decrease',
      'public_activity_volume'
    ];
    const activityFieldCount = activityFields.filter((field) => row[field] !== undefined).length;
    if ((requireActivity && activityFieldCount !== activityFields.length) ||
        (activityFieldCount !== 0 && activityFieldCount !== activityFields.length)) {
      throw new Error(`${rowLabel} has incomplete public activity fields`);
    }
    let activityVolume = null;
    if (activityFieldCount !== 0) {
      assertNonNegativeSafeInteger(
        row.activity_transaction_count,
        `${rowLabel}.activity_transaction_count`
      );
      validateAmount(
        row.public_balance_increase,
        `${rowLabel}.public_balance_increase`,
        decimals
      );
      validateAmount(
        row.public_balance_decrease,
        `${rowLabel}.public_balance_decrease`,
        decimals
      );
      validateAmount(
        row.public_activity_volume,
        `${rowLabel}.public_activity_volume`,
        decimals
      );
      activityVolume = BigInt(row.public_activity_volume.raw_amount);
      assertSame(
        BigInt(row.public_balance_increase.raw_amount) +
          BigInt(row.public_balance_decrease.raw_amount),
        activityVolume,
        `${rowLabel} public activity volume`
      );
    }
    const sortAmount = sortBy === 'activity' ? activityVolume : balance;
    if (sortAmount === null) {
      throw new Error(`${rowLabel} has no public activity value for activity sorting`);
    }
    if (previousSortAmount !== null && sortAmount > previousSortAmount) {
      throw new Error(`${label} is not in descending ${sortBy} order`);
    }
    previousSortAmount = sortAmount;
    owners.set(row.owner, row);
  }
  return owners;
}

function sameHolderActivityValue(left, right) {
  return (
    sameHolderValue(left, right) &&
    left.activity_transaction_count === right.activity_transaction_count &&
    left.public_balance_increase?.raw_amount === right.public_balance_increase?.raw_amount &&
    left.public_balance_decrease?.raw_amount === right.public_balance_decrease?.raw_amount &&
    left.public_activity_volume?.raw_amount === right.public_activity_volume?.raw_amount
  );
}

function sameHolderValue(left, right) {
  return (
    left.owner === right.owner &&
    left.token_account_count === right.token_account_count &&
    left.public_balance.raw_amount === right.public_balance.raw_amount &&
    left.public_balance.base_units === right.public_balance.base_units
  );
}

function validateCompleteReplayState(report, replayedState, historyReport) {
  const claimsCompleteMatch =
    report.status === 'complete_match' ||
    report.instruction_replay_matches_metadata_for_complete_spyx_selected_history === true;
  if (!claimsCompleteMatch) return;

  if (report.status !== 'complete_match') {
    throw new Error('A strict replay metadata-match claim must use complete_match status');
  }
  if (report.bounded_selected_dump_scan_complete !== true) {
    throw new Error('A complete_match strict replay report must cover the full bounded scan');
  }
  if (report.instruction_replay_matches_metadata_for_complete_spyx_selected_history !== true) {
    throw new Error('A complete_match strict replay report must claim a full-history metadata match');
  }
  if (report.instruction_replay_implemented !== true) {
    throw new Error('A complete_match strict replay report must have instruction replay implemented');
  }
  if (replayedState.history_complete !== true) {
    throw new Error('A complete_match strict replay report has an incomplete replayed state');
  }
  if (Object.keys(report.blockers).length !== 0) {
    throw new Error('A complete_match strict replay report must have no blockers');
  }
  if (report.first_failure !== null) {
    throw new Error('A complete_match strict replay report must have no first failure');
  }

  for (const field of [
    'transactions_scanned',
    'successful_transactions',
    'failed_transactions',
    'transactions_with_target_oracle_rows',
    'pre_target_oracle_rows',
    'post_target_oracle_rows',
    'metadata_without_error',
    'metadata_current_only',
    'metadata_legacy_only',
    'metadata_both_identical',
    'replay_transactions_attempted',
    'replay_transactions_applied',
    'replay_clean_prefix_transactions',
    'replay_errors',
    'oracle_pre_rows_compared',
    'oracle_post_rows_compared',
    'oracle_pre_mismatches',
    'oracle_post_mismatches'
  ]) {
    assertNonNegativeSafeInteger(report.counters[field], `strict replay counters.${field}`);
  }

  for (const [field, expected] of [
    ['transactions_scanned', historyReport.audit.transactions],
    ['successful_transactions', historyReport.audit.metadata_without_error],
    [
      'failed_transactions',
      historyReport.audit.metadata_current_only +
        historyReport.audit.metadata_legacy_only +
        historyReport.audit.metadata_both_same_target_balance_resolution
    ],
    ['transactions_with_target_oracle_rows', historyReport.audit.transactions_with_target_balance_rows],
    ['pre_target_oracle_rows', historyReport.audit.target_pre_balance_rows],
    ['post_target_oracle_rows', historyReport.audit.target_post_balance_rows],
    ['metadata_without_error', historyReport.audit.metadata_without_error],
    ['metadata_current_only', historyReport.audit.metadata_current_only],
    ['metadata_legacy_only', historyReport.audit.metadata_legacy_only],
    ['metadata_both_identical', historyReport.audit.metadata_both_same_target_balance_resolution],
    ['replay_transactions_attempted', historyReport.audit.transactions],
    ['replay_transactions_applied', historyReport.audit.transactions],
    ['replay_clean_prefix_transactions', historyReport.audit.transactions],
    ['oracle_pre_rows_compared', historyReport.audit.target_pre_balance_rows],
    ['oracle_post_rows_compared', historyReport.audit.target_post_balance_rows]
  ]) {
    assertSame(report.counters[field], expected, `strict replay ${field}`);
  }

  for (const field of ['replay_errors', 'oracle_pre_mismatches', 'oracle_post_mismatches']) {
    if (report.counters[field] !== 0) {
      throw new Error(`A complete_match strict replay report has non-zero ${field}`);
    }
  }

  assertSame(
    replayedState.tracked_accounts,
    historyReport.source.discovered_token_accounts,
    'strict replay tracked account count'
  );
  assertSame(
    replayedState.positive_public_balance_accounts,
    historyReport.final_public_balance.active_public_token_accounts,
    'strict replay positive-balance account count'
  );
  assertSame(
    replayedState.public_raw_balance,
    historyReport.final_public_balance.public_raw_balance_sum.raw_amount,
    'strict replay public raw balance'
  );
}

function validateStrictReplayUiFields(report) {
  assertNonNegativeSafeInteger(report.schema_version, 'strict replay schema_version');
  for (const field of [
    'bounded_selected_dump_scan_complete',
    'instruction_replay_implemented',
    'instruction_replay_matches_metadata_for_complete_spyx_selected_history'
  ]) {
    if (typeof report[field] !== 'boolean') {
      throw new Error(`strict replay ${field} is not a boolean`);
    }
  }
  assertNonEmptyString(report.status, 'strict replay status');
  if (report.proof_scope !== undefined) {
    assertNonEmptyString(report.proof_scope, 'strict replay proof_scope');
  }
  if (
    report.elapsed_seconds !== undefined &&
    (!Number.isFinite(report.elapsed_seconds) || report.elapsed_seconds < 0)
  ) {
    throw new Error('strict replay elapsed_seconds is not a non-negative finite number');
  }
  validateNumberRecord(report.counters, 'strict replay counters');
  validateNumberRecord(report.blockers, 'strict replay blockers');
  validateNumberRecord(report.instruction_names ?? {}, 'strict replay instruction_names');
  validateNumberRecord(report.census_findings ?? {}, 'strict replay census_findings');
  if (report.first_failure !== null && report.first_failure !== undefined) {
    validateReplayFailure(report.first_failure);
  }
}

function validateReplayFailure(value) {
  const label = 'strict replay first_failure';
  assertObject(value, label);
  for (const field of ['source_epoch', 'slot', 'source_block_id', 'tx_index']) {
    assertNonNegativeSafeInteger(value[field], `${label}.${field}`);
  }
  for (const field of ['phase', 'code', 'detail']) {
    assertNonEmptyString(value[field], `${label}.${field}`);
  }
  for (const field of ['outer_index', 'inner_index']) {
    if (value[field] !== null) {
      assertNonNegativeSafeInteger(value[field], `${label}.${field}`);
    }
  }
}

function validateReleaseStrictReplay(summary) {
  if (
    summary.schema_version !== 5 ||
    summary.status !== 'complete_match' ||
    summary.bounded_selected_dump_scan_complete !== true ||
    summary.instruction_replay_implemented !== true ||
    summary.instruction_replay_matches_metadata_for_complete_spyx_selected_history !== true ||
    summary.replayed_state?.history_complete !== true ||
    summary.holder_authority?.complete !== true
  ) {
    throw new Error(
      'A release data build requires a complete_match strict replay report with complete holder authority classification'
    );
  }
  if (
    summary.authority_portfolios === null ||
    summary.authority_portfolios.coverage.complete !== true ||
    summary.authority_portfolios.coverage.candidate_flow_evidence_complete !== true
  ) {
    throw new Error(
      'A release data build requires a complete authority portfolio scan with transaction evidence'
    );
  }
  if (
    summary.authority_portfolio_history === null ||
    summary.authority_portfolio_history.coverage.complete !== true ||
    summary.authority_portfolio_history.coverage.final_sample_matches_current_portfolio !== true
  ) {
    throw new Error(
      'A release data build requires complete forward authority portfolio history'
    );
  }
}

function validateCompleteHistoryReport(report) {
  for (const field of [
    'bounded_selected_dump_scan_complete',
    'metadata_balance_chain_continuous_from_spyx_mint_creation',
    'daily_public_balance_series_complete',
    'daily_selected_transaction_counts_complete'
  ]) {
    if (report[field] !== true) {
      throw new Error(`The history report requires ${field} to be true`);
    }
  }

  validateStringRecord(report.definitions, 'definitions');
  validateStringRecord(report.limitations, 'limitations');

  for (const field of [
    'mint_slot',
    'first_epoch',
    'last_epoch',
    'transactions',
    'signatures',
    'registry_entries',
    'discovered_token_accounts',
    'total_dump_bytes'
  ]) {
    assertNonNegativeSafeInteger(report.source[field], `source.${field}`);
  }
  if (report.source.transactions === 0) {
    throw new Error('source.transactions must be greater than zero');
  }
  if (report.source.first_epoch > report.source.last_epoch) {
    throw new Error('source.first_epoch must not be after source.last_epoch');
  }
  for (const [field, label] of [
    ['manifest', 'history manifest'],
    ['transactions_file', 'history transaction stream'],
    ['signatures_file', 'history signature stream'],
    ['registry_file', 'history registry'],
    ['accounts_file', 'history account artifact']
  ]) {
    validateSourceFile(report.source[field], label);
  }

  const auditFields = [
    'transactions',
    'signatures',
    'transactions_with_target_balance_rows',
    'public_balance_changing_transactions',
    'public_owner_reassignment_transactions',
    'target_pre_balance_rows',
    'target_post_balance_rows',
    'implicit_zero_pre_rows',
    'implicit_zero_post_rows',
    'target_balance_rows_without_owner',
    'target_positive_states_without_owner',
    'transactions_without_block_time',
    'public_state_changes_without_block_time',
    'metadata_absent',
    'metadata_without_error',
    'metadata_current_only',
    'metadata_legacy_only',
    'metadata_both_same_target_balance_resolution',
    'address_signature_rows',
    'selected_transactions_without_target_address'
  ];
  for (const field of auditFields) {
    assertNonNegativeSafeInteger(report.audit[field], `audit.${field}`);
  }
  assertSame(report.audit.transactions, report.source.transactions, 'audit transaction count');
  assertSame(report.audit.signatures, report.source.signatures, 'audit signature count');

  validateFinalPublicBalance(report.final_public_balance);
  validatePublicVolumeTotals(
    report.public_volume_totals,
    report.audit,
    report.final_public_balance.decimals
  );
  validateDailyRows(report.daily, report.final_public_balance.decimals);
  if (report.final_top_100_holder_history !== undefined) {
    validateFinalTopHolderHistory(
      report.final_top_100_holder_history,
      report.source,
      report.daily,
      report.final_public_balance
    );
  }
  validateMovementDays(
    report.top_25_volume_days,
    report.daily,
    report.final_public_balance.decimals
  );
  validateMovementTransactions(
    report.top_25_volume_transactions,
    report.source,
    report.final_public_balance.decimals
  );
  assertNonEmptyString(report.rpc_request_model.scope, 'rpc_request_model.scope');

  const selectedTransactions = sumSafeIntegerField(
    report.daily,
    'selected_transactions',
    'daily selected transaction count'
  );
  assertSame(selectedTransactions, report.source.transactions, 'daily selected transaction count');
  assertSame(
    sumSafeIntegerField(
      report.daily,
      'public_balance_changing_transactions',
      'daily public balance-changing transaction count'
    ),
    report.public_volume_totals.public_balance_changing_transactions,
    'daily public balance-changing transaction count'
  );
  assertSame(
    sumSafeIntegerField(
      report.daily,
      'public_owner_reassignment_transactions',
      'daily public owner-reassignment transaction count'
    ),
    report.public_volume_totals.public_owner_reassignment_transactions,
    'daily public owner-reassignment transaction count'
  );
  for (const field of [
    'public_bilateral_movement',
    'inferred_public_mint',
    'inferred_public_burn'
  ]) {
    assertSame(
      sumRawAmountField(report.daily, field),
      BigInt(report.public_volume_totals[field].raw_amount),
      `daily ${field}`
    );
  }
  validateFinalDailyState(report.daily.at(-1), report.final_public_balance);
}

function validateFinalTopHolderHistory(value, source, daily, finalBalance) {
  const label = 'final_top_100_holder_history';
  assertObject(value, label);
  assertObject(value.source_binding, `${label}.source_binding`);
  assertObject(value.cohort, `${label}.cohort`);
  assertObject(value.definitions, `${label}.definitions`);
  assertArray(value.days, `${label}.days`);
  assertArray(value.series, `${label}.series`);

  for (const [field, expected] of [
    ['mint', source.mint],
    ['mint_slot', source.mint_slot],
    ['first_epoch', source.first_epoch],
    ['last_epoch', source.last_epoch],
    ['manifest_sha256', source.manifest.sha256],
    ['transactions_sha256', source.transactions_file.sha256],
    ['signatures_sha256', source.signatures_file.sha256],
    ['registry_sha256', source.registry_file.sha256],
    ['accounts_sha256', source.accounts_file.sha256]
  ]) {
    assertSame(
      value.source_binding[field],
      expected,
      `${label}.source_binding.${field}`
    );
  }

  assertSame(
    value.cohort.selection_boundary,
    'final_public_balance_at_dump_boundary',
    `${label}.cohort.selection_boundary`
  );
  assertSame(value.cohort.maximum_holders, 100, `${label}.cohort.maximum_holders`);
  const expectedSelectedHolders = Math.min(
    value.cohort.maximum_holders,
    finalBalance.positive_public_balance_holders
  );
  assertSame(
    value.cohort.selected_holders,
    expectedSelectedHolders,
    `${label}.cohort.selected_holders`
  );
  assertSame(
    value.cohort.ranking,
    'positive_public_raw_balance_descending',
    `${label}.cohort.ranking`
  );
  assertSame(
    value.cohort.tie_break,
    'raw_32_byte_owner_pubkey_ascending',
    `${label}.cohort.tie_break`
  );
  assertSame(
    value.series.length,
    expectedSelectedHolders,
    `${label}.series holder count`
  );

  for (const field of [
    'cohort',
    'daily_boundary',
    'calendar_dates',
    'source_boundary',
    'complete_utc_day',
    'balance_state_carried_forward',
    'raw_balance'
  ]) {
    assertNonEmptyString(value.definitions[field], `${label}.definitions.${field}`);
  }

  const firstDate = parseUtcDate(daily[0].utc_date);
  const lastDate = parseUtcDate(daily.at(-1).utc_date);
  const expectedDayCount = Math.floor((lastDate - firstDate) / 86_400_000) + 1;
  assertSame(value.days.length, expectedDayCount, `${label}.days calendar length`);
  const observedDates = new Set(daily.map((row) => row.utc_date));
  for (const [index, day] of value.days.entries()) {
    const rowLabel = `${label}.days[${index}]`;
    assertObject(day, rowLabel);
    const expectedDate = new Date(firstDate + index * 86_400_000)
      .toISOString()
      .slice(0, 10);
    assertSame(day.utc_date, expectedDate, `${rowLabel}.utc_date`);
    for (const field of [
      'complete_utc_day',
      'source_boundary_start',
      'source_boundary_end',
      'observed_selected_transaction_day',
      'balance_state_carried_forward'
    ]) {
      if (typeof day[field] !== 'boolean') {
        throw new Error(`${rowLabel}.${field} is not a boolean`);
      }
    }
    const sourceBoundaryStart = index === 0;
    const sourceBoundaryEnd = index === value.days.length - 1;
    const observedSelectedTransactionDay = observedDates.has(day.utc_date);
    assertSame(
      day.source_boundary_start,
      sourceBoundaryStart,
      `${rowLabel}.source_boundary_start`
    );
    assertSame(
      day.source_boundary_end,
      sourceBoundaryEnd,
      `${rowLabel}.source_boundary_end`
    );
    assertSame(
      day.complete_utc_day,
      !sourceBoundaryStart && !sourceBoundaryEnd,
      `${rowLabel}.complete_utc_day`
    );
    assertSame(
      day.observed_selected_transaction_day,
      observedSelectedTransactionDay,
      `${rowLabel}.observed_selected_transaction_day`
    );
    assertSame(
      day.balance_state_carried_forward,
      !observedSelectedTransactionDay,
      `${rowLabel}.balance_state_carried_forward`
    );
  }

  const expectedLargestHolderRows = Math.min(
    25,
    finalBalance.positive_public_balance_holders
  );
  assertSame(
    finalBalance.largest_25_holders.length,
    expectedLargestHolderRows,
    `${label} final largest-holder row count`
  );
  const owners = new Set();
  let previousBalance = null;
  let previousOwnerBytes = null;
  let finalTopAmount = 0n;
  for (const [index, series] of value.series.entries()) {
    const rowLabel = `${label}.series[${index}]`;
    assertObject(series, rowLabel);
    assertSame(series.final_rank, index + 1, `${rowLabel}.final_rank`);
    const ownerBytes = decodeBase58Pubkey(series.owner, `${rowLabel}.owner`);
    if (owners.has(series.owner)) throw new Error(`${label}.series has a duplicate owner`);
    owners.add(series.owner);
    assertUnsignedDecimalString(series.final_raw_balance, `${rowLabel}.final_raw_balance`);
    const finalRawBalance = BigInt(series.final_raw_balance);
    if (finalRawBalance === 0n) {
      throw new Error(`${rowLabel}.final_raw_balance must be positive`);
    }
    if (
      previousBalance !== null &&
      (finalRawBalance > previousBalance ||
        (finalRawBalance === previousBalance &&
          compareByteArrays(ownerBytes, previousOwnerBytes) < 0))
    ) {
      throw new Error(`${label}.series is not in final rank order`);
    }
    previousBalance = finalRawBalance;
    previousOwnerBytes = ownerBytes;

    assertArray(series.daily_raw_balances, `${rowLabel}.daily_raw_balances`);
    assertSame(
      series.daily_raw_balances.length,
      value.days.length,
      `${rowLabel}.daily_raw_balances length`
    );
    for (const [dayIndex, rawBalance] of series.daily_raw_balances.entries()) {
      assertUnsignedDecimalString(
        rawBalance,
        `${rowLabel}.daily_raw_balances[${dayIndex}]`
      );
    }
    assertSame(
      series.daily_raw_balances.at(-1),
      series.final_raw_balance,
      `${rowLabel} final point`
    );

    finalTopAmount += finalRawBalance;
  }
  for (const [index, finalHolder] of finalBalance.largest_25_holders.entries()) {
    const series = value.series[index];
    const rowLabel = `${label}.series[${index}]`;
    assertSame(series.owner, finalHolder.owner, `${rowLabel} final cohort owner`);
    assertSame(
      series.final_raw_balance,
      finalHolder.public_balance.raw_amount,
      `${rowLabel} final cohort balance`
    );
  }
  assertSame(
    finalTopAmount,
    BigInt(finalBalance.top_100_concentration.amount.raw_amount),
    `${label} final top-100 total`
  );
}

function assertUnsignedDecimalString(value, label) {
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new Error(`${label} is not an unsigned decimal string`);
  }
}

function decodeBase58Pubkey(value, label) {
  assertNonEmptyString(value, label);
  const alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  const bytesLittleEndian = [];
  let leadingZeroBytes = 0;
  while (leadingZeroBytes < value.length && value[leadingZeroBytes] === '1') {
    leadingZeroBytes += 1;
  }
  for (const character of value) {
    const digit = alphabet.indexOf(character);
    if (digit === -1) throw new Error(`${label} is not valid base58`);
    let carry = digit;
    for (let index = 0; index < bytesLittleEndian.length; index += 1) {
      carry += bytesLittleEndian[index] * 58;
      bytesLittleEndian[index] = carry & 0xff;
      carry >>= 8;
    }
    while (carry > 0) {
      bytesLittleEndian.push(carry & 0xff);
      carry >>= 8;
    }
  }
  const bytes = new Uint8Array(leadingZeroBytes + bytesLittleEndian.length);
  for (let index = 0; index < bytesLittleEndian.length; index += 1) {
    bytes[bytes.length - index - 1] = bytesLittleEndian[index];
  }
  if (bytes.length !== 32) throw new Error(`${label} is not a 32-byte Solana pubkey`);
  return bytes;
}

function compareByteArrays(left, right) {
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) return left[index] < right[index] ? -1 : 1;
  }
  return 0;
}

function validateSourceFile(value, label) {
  assertObject(value, label);
  if (typeof value.file !== 'string' || value.file.length === 0) {
    throw new Error(`${label}.file is not a non-empty string`);
  }
  assertNonNegativeSafeInteger(value.bytes, `${label}.bytes`);
  assertSha256(value.sha256, `${label} SHA-256`);
}

function validateFinalPublicBalance(value) {
  assertNonNegativeSafeInteger(value.decimals, 'final_public_balance.decimals');
  if (value.decimals > 255) {
    throw new Error('final_public_balance.decimals exceeds the supported range');
  }
  assertNonNegativeSafeInteger(
    value.positive_public_balance_holders,
    'final_public_balance.positive_public_balance_holders'
  );
  assertNonNegativeSafeInteger(
    value.active_public_token_accounts,
    'final_public_balance.active_public_token_accounts'
  );
  validateAmount(
    value.public_raw_balance_sum,
    'final_public_balance.public_raw_balance_sum',
    value.decimals
  );
  for (const field of ['top_1_concentration', 'top_10_concentration', 'top_100_concentration']) {
    validateConcentration(value[field], `final_public_balance.${field}`, value.decimals);
    assertSame(
      value[field].supply_fraction_denominator_raw,
      value.public_raw_balance_sum.raw_amount,
      `final_public_balance.${field} denominator`
    );
  }
  validateHolderRows(
    value.largest_25_holders,
    'final_public_balance.largest_25_holders',
    value.decimals,
    'descending'
  );
  validateHolderRows(
    value.smallest_25_positive_holders,
    'final_public_balance.smallest_25_positive_holders',
    value.decimals,
    'ascending'
  );
  validateBalanceDistribution(value.balance_distribution, value, value.decimals);
}

function validateHolderRows(rows, label, decimals, order) {
  assertArray(rows, label);
  if (rows.length === 0 || rows.length > 25) {
    throw new Error(`${label} must contain between 1 and 25 rows`);
  }

  const owners = new Set();
  let previousBalance = null;
  for (const [index, row] of rows.entries()) {
    const rowLabel = `${label}[${index}]`;
    assertObject(row, rowLabel);
    assertNonEmptyString(row.owner, `${rowLabel}.owner`);
    if (owners.has(row.owner)) throw new Error(`${label} has a duplicate owner`);
    owners.add(row.owner);
    assertNonNegativeSafeInteger(row.token_account_count, `${rowLabel}.token_account_count`);
    if (row.token_account_count === 0) {
      throw new Error(`${rowLabel}.token_account_count must be greater than zero`);
    }
    validateAmount(row.public_balance, `${rowLabel}.public_balance`, decimals);
    const balance = BigInt(row.public_balance.raw_amount);
    if (balance === 0n) throw new Error(`${rowLabel}.public_balance must be positive`);
    if (
      previousBalance !== null &&
      ((order === 'descending' && balance > previousBalance) ||
        (order === 'ascending' && balance < previousBalance))
    ) {
      throw new Error(`${label} is not in ${order} public-balance order`);
    }
    previousBalance = balance;
  }
}

function validateBalanceDistribution(rows, finalBalance, decimals) {
  const label = 'final_public_balance.balance_distribution';
  assertArray(rows, label);
  if (rows.length === 0) throw new Error(`${label} must not be empty`);

  const ranges = new Set();
  let holders = 0;
  let rawBalance = 0n;
  for (const [index, row] of rows.entries()) {
    const rowLabel = `${label}[${index}]`;
    assertObject(row, rowLabel);
    assertNonEmptyString(row.base_unit_range, `${rowLabel}.base_unit_range`);
    if (ranges.has(row.base_unit_range)) throw new Error(`${label} has a duplicate range`);
    ranges.add(row.base_unit_range);
    assertNonNegativeSafeInteger(row.holder_count, `${rowLabel}.holder_count`);
    holders = safeIntegerAdd(holders, row.holder_count, `${label} holder count`);
    validateAmount(row.public_balance, `${rowLabel}.public_balance`, decimals);
    rawBalance += BigInt(row.public_balance.raw_amount);
  }
  assertSame(
    holders,
    finalBalance.positive_public_balance_holders,
    'balance distribution holder count'
  );
  assertSame(
    rawBalance,
    BigInt(finalBalance.public_raw_balance_sum.raw_amount),
    'balance distribution public raw balance'
  );
}

function validatePublicVolumeTotals(value, audit, decimals) {
  for (const field of [
    'public_balance_changing_transactions',
    'public_owner_reassignment_transactions'
  ]) {
    assertNonNegativeSafeInteger(value[field], `public_volume_totals.${field}`);
    assertSame(value[field], audit[field], `public_volume_totals.${field}`);
  }
  for (const field of [
    'public_bilateral_movement',
    'inferred_public_mint',
    'inferred_public_burn'
  ]) {
    validateAmount(value[field], `public_volume_totals.${field}`, decimals);
  }
}

function validateMovementDays(rows, dailyRows, decimals) {
  const label = 'top_25_volume_days';
  assertArray(rows, label);
  if (rows.length === 0 || rows.length > 25) {
    throw new Error(`${label} must contain between 1 and 25 rows`);
  }
  const dailyByDate = new Map(dailyRows.map((row) => [row.utc_date, row]));
  const dates = new Set();
  let previousMovement = null;
  for (const [index, row] of rows.entries()) {
    const rowLabel = `${label}[${index}]`;
    assertObject(row, rowLabel);
    parseUtcDate(row.utc_date);
    if (dates.has(row.utc_date)) throw new Error(`${label} has a duplicate UTC date`);
    dates.add(row.utc_date);
    for (const field of ['selected_transactions', 'public_balance_changing_transactions']) {
      assertNonNegativeSafeInteger(row[field], `${rowLabel}.${field}`);
    }
    for (const field of [
      'public_bilateral_movement',
      'inferred_public_mint',
      'inferred_public_burn'
    ]) {
      validateAmount(row[field], `${rowLabel}.${field}`, decimals);
    }
    const movement = BigInt(row.public_bilateral_movement.raw_amount);
    if (previousMovement !== null && movement > previousMovement) {
      throw new Error(`${label} is not in descending public-movement order`);
    }
    previousMovement = movement;

    const dailyRow = dailyByDate.get(row.utc_date);
    if (!dailyRow) throw new Error(`${rowLabel} does not match a daily row`);
    for (const field of [
      'selected_transactions',
      'public_balance_changing_transactions',
      'public_bilateral_movement',
      'inferred_public_mint',
      'inferred_public_burn'
    ]) {
      if (!isDeepStrictEqual(row[field], dailyRow[field])) {
        throw new Error(`${rowLabel}.${field} does not match its daily row`);
      }
    }
  }
}

function validateMovementTransactions(rows, source, decimals) {
  const label = 'top_25_volume_transactions';
  assertArray(rows, label);
  if (rows.length === 0 || rows.length > 25) {
    throw new Error(`${label} must contain between 1 and 25 rows`);
  }

  const signatures = new Set();
  const coordinates = new Set();
  let previousMovement = null;
  for (const [index, row] of rows.entries()) {
    const rowLabel = `${label}[${index}]`;
    assertObject(row, rowLabel);
    assertNonEmptyString(row.first_signature, `${rowLabel}.first_signature`);
    if (signatures.has(row.first_signature)) throw new Error(`${label} has a duplicate signature`);
    signatures.add(row.first_signature);
    for (const field of [
      'source_epoch',
      'slot',
      'source_block_id',
      'tx_index',
      'block_time_unix_seconds'
    ]) {
      assertNonNegativeSafeInteger(row[field], `${rowLabel}.${field}`);
    }
    if (row.source_epoch < source.first_epoch || row.source_epoch > source.last_epoch) {
      throw new Error(`${rowLabel}.source_epoch is outside the source epoch range`);
    }
    parseUtcDate(row.utc_date);
    const blockTime = new Date(row.block_time_unix_seconds * 1_000);
    if (!Number.isFinite(blockTime.getTime())) {
      throw new Error(`${rowLabel}.block_time_unix_seconds is outside the supported date range`);
    }
    const blockDate = blockTime.toISOString().slice(0, 10);
    if (row.utc_date !== blockDate) {
      throw new Error(`${rowLabel}.utc_date does not match block_time_unix_seconds`);
    }
    const coordinate = `${row.source_epoch}:${row.source_block_id}:${row.tx_index}`;
    if (coordinates.has(coordinate)) throw new Error(`${label} has a duplicate transaction coordinate`);
    coordinates.add(coordinate);
    for (const field of [
      'public_bilateral_movement',
      'inferred_public_mint',
      'inferred_public_burn'
    ]) {
      validateAmount(row[field], `${rowLabel}.${field}`, decimals);
    }
    const movement = BigInt(row.public_bilateral_movement.raw_amount);
    if (previousMovement !== null && movement > previousMovement) {
      throw new Error(`${label} is not in descending public-movement order`);
    }
    previousMovement = movement;
  }
}

function validateDailyRows(rows, decimals) {
  if (rows.length === 0) throw new Error('The history report daily rows must not be empty');

  let previousDate = null;
  for (const [index, row] of rows.entries()) {
    const label = `daily[${index}]`;
    assertObject(row, label);
    const date = parseUtcDate(row.utc_date);
    if (previousDate !== null && date <= previousDate) {
      throw new Error('The history report daily rows are not in strict UTC date order');
    }
    previousDate = date;

    for (const field of [
      'selected_transactions',
      'public_balance_changing_transactions',
      'public_owner_reassignment_transactions',
      'positive_public_balance_holders',
      'active_public_token_accounts'
    ]) {
      assertNonNegativeSafeInteger(row[field], `${label}.${field}`);
    }
    for (const field of [
      'public_raw_balance_sum',
      'public_bilateral_movement',
      'inferred_public_mint',
      'inferred_public_burn'
    ]) {
      validateAmount(row[field], `${label}.${field}`, decimals);
    }
    for (const field of ['top_1_concentration', 'top_10_concentration', 'top_100_concentration']) {
      validateConcentration(row[field], `${label}.${field}`, decimals);
      assertSame(
        row[field].supply_fraction_denominator_raw,
        row.public_raw_balance_sum.raw_amount,
        `${label}.${field} denominator`
      );
    }
  }
}

function validateFinalDailyState(last, finalBalance) {
  for (const field of ['positive_public_balance_holders', 'active_public_token_accounts']) {
    assertSame(last[field], finalBalance[field], `final daily ${field}`);
  }
  assertJsonSame(
    last.public_raw_balance_sum,
    finalBalance.public_raw_balance_sum,
    'final daily public_raw_balance_sum'
  );
  for (const field of ['top_1_concentration', 'top_10_concentration', 'top_100_concentration']) {
    assertJsonSame(last[field], finalBalance[field], `final daily ${field}`);
  }
}

function validateAmount(value, label, decimals) {
  assertObject(value, label);
  if (typeof value.raw_amount !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value.raw_amount)) {
    throw new Error(`${label}.raw_amount is not an unsigned decimal string`);
  }
  if (typeof value.base_units !== 'string' || value.base_units.length === 0) {
    throw new Error(`${label}.base_units is not a non-empty string`);
  }
  if (decimals !== undefined && value.base_units !== formatBaseUnits(value.raw_amount, decimals)) {
    throw new Error(`${label}.base_units does not match raw_amount and decimals`);
  }
  if (!Number.isFinite(Number(value.base_units))) {
    throw new Error(`${label}.base_units cannot be displayed as a finite number`);
  }
}

function validateConcentration(value, label, decimals) {
  assertObject(value, label);
  validateAmount(value.amount, `${label}.amount`, decimals);
  for (const field of ['supply_fraction_numerator_raw', 'supply_fraction_denominator_raw']) {
    if (typeof value[field] !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value[field])) {
      throw new Error(`${label}.${field} is not an unsigned decimal string`);
    }
  }
  assertNonNegativeSafeInteger(
    value.supply_share_parts_per_million_floor,
    `${label}.supply_share_parts_per_million_floor`
  );
  const numerator = BigInt(value.supply_fraction_numerator_raw);
  const denominator = BigInt(value.supply_fraction_denominator_raw);
  if (numerator !== BigInt(value.amount.raw_amount)) {
    throw new Error(`${label}.amount does not match its supply numerator`);
  }
  if (numerator > denominator) {
    throw new Error(`${label} supply numerator exceeds its denominator`);
  }
  const expectedPartsPerMillion =
    denominator === 0n ? 0n : (numerator * 1_000_000n) / denominator;
  if (BigInt(value.supply_share_parts_per_million_floor) !== expectedPartsPerMillion) {
    throw new Error(`${label}.supply_share_parts_per_million_floor does not match its fraction`);
  }
}

function formatBaseUnits(rawAmount, decimals) {
  if (decimals === 0) return rawAmount;
  const padded = rawAmount.padStart(decimals + 1, '0');
  return `${padded.slice(0, -decimals)}.${padded.slice(-decimals)}`;
}

function assertJsonSame(actual, expected, label) {
  if (!isDeepStrictEqual(actual, expected)) {
    throw new Error(`${label} does not match final_public_balance`);
  }
}

function validateStringRecord(value, label) {
  assertObject(value, label);
  if (Object.keys(value).length === 0) throw new Error(`${label} must not be empty`);
  for (const [key, text] of Object.entries(value)) {
    assertNonEmptyString(key, `${label} key`);
    assertNonEmptyString(text, `${label}.${key}`);
  }
}

function validateNumberRecord(value, label) {
  assertObject(value, label);
  for (const [key, count] of Object.entries(value)) {
    assertNonEmptyString(key, `${label} key`);
    assertNonNegativeSafeInteger(count, `${label}.${key}`);
  }
}

function assertNonEmptyString(value, label) {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`${label} is not a non-empty string`);
  }
}

function safeIntegerAdd(left, right, label) {
  const value = left + right;
  if (!Number.isSafeInteger(value)) throw new Error(`${label} exceeds the safe integer range`);
  return value;
}

function sumSafeIntegerField(rows, field, label) {
  return rows.reduce((sum, row) => safeIntegerAdd(sum, row[field], label), 0);
}

function sumRawAmountField(rows, field) {
  return rows.reduce((sum, row) => sum + BigInt(row[field].raw_amount), 0n);
}

function compactRpcRequestModel(value) {
  return {
    scope: value.scope,
    address_count: value.address_count,
    mint_addresses: value.mint_addresses,
    token_account_addresses: value.token_account_addresses,
    get_signatures_for_address_page_limit: value.get_signatures_for_address_page_limit,
    get_signatures_for_address_requests: value.get_signatures_for_address_requests,
    get_signatures_for_address_credit_page_size: value.get_signatures_for_address_credit_page_size,
    get_signatures_for_address_credit_pages: value.get_signatures_for_address_credit_pages,
    returned_address_signature_rows: value.returned_address_signature_rows,
    duplicate_address_signature_rows_removed: value.duplicate_address_signature_rows_removed,
    unique_get_transaction_calls: value.unique_get_transaction_calls,
    total_rpc_requests: value.total_rpc_requests
  };
}

function parseArguments(args) {
  const options = {};
  const positional = [];
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === '--require-strict-replay') {
      options.requireStrictReplay = true;
      continue;
    }
    if (argument === '--validate-only') {
      options.validateOnly = true;
      continue;
    }
    if (
      argument === '--history' ||
      argument === '--history-sha256' ||
      argument === '--strict-replay' ||
      argument === '--strict-replay-sha256' ||
      argument === '--programs' ||
      argument === '--programs-sha256' ||
      argument === '--program-cpi-inventory' ||
      argument === '--program-cpi-inventory-sha256' ||
      argument === '--holder-authority-supplement' ||
      argument === '--holder-authority-supplement-sha256' ||
      argument === '--output' ||
      argument === '--program-output' ||
      argument === '--authority-portfolio-output' ||
      argument === '--authority-portfolio-table-output' ||
      argument === '--authority-portfolio-history-output' ||
      argument === '--pda-authority-estimate-output'
    ) {
      const value = args[index + 1];
      if (!value) throw new Error(`Missing value for ${argument}`);
      if (argument === '--history') options.history = value;
      if (argument === '--history-sha256') options.historySha256 = value;
      if (argument === '--strict-replay') options.strictReplay = value;
      if (argument === '--strict-replay-sha256') options.strictReplaySha256 = value;
      if (argument === '--programs') options.programs = value;
      if (argument === '--programs-sha256') options.programsSha256 = value;
      if (argument === '--program-cpi-inventory') options.programCpiInventory = value;
      if (argument === '--program-cpi-inventory-sha256') {
        options.programCpiInventorySha256 = value;
      }
      if (argument === '--holder-authority-supplement') {
        options.holderAuthoritySupplement = value;
      }
      if (argument === '--holder-authority-supplement-sha256') {
        options.holderAuthoritySupplementSha256 = value;
      }
      if (argument === '--output') options.output = value;
      if (argument === '--program-output') options.programOutput = value;
      if (argument === '--authority-portfolio-output') {
        options.authorityPortfolioOutput = value;
      }
      if (argument === '--authority-portfolio-table-output') {
        options.authorityPortfolioTableOutput = value;
      }
      if (argument === '--authority-portfolio-history-output') {
        options.authorityPortfolioHistoryOutput = value;
      }
      if (argument === '--pda-authority-estimate-output') {
        options.pdaAuthorityEstimateOutput = value;
      }
      index += 1;
      continue;
    }
    if (argument.startsWith('--')) throw new Error(`Unknown option: ${argument}`);
    positional.push(argument);
  }
  options.history ??= positional[0];
  options.strictReplay ??= positional[1];
  options.programs ??= positional[2];
  return options;
}

function validateStrictReplaySourceIdentity(report, historySource) {
  const replaySource = report.source;
  assertSame(replaySource.mint, historySource.mint, 'strict replay mint');
  assertSame(replaySource.mint_slot, historySource.mint_slot, 'strict replay mint slot');
  assertSame(replaySource.first_epoch, historySource.first_epoch, 'strict replay first epoch');
  assertSame(replaySource.last_epoch, historySource.last_epoch, 'strict replay last epoch');
  assertSame(
    replaySource.manifest_transactions,
    historySource.transactions,
    'strict replay manifest transaction count'
  );
  assertSame(
    replaySource.discovered_token_accounts,
    historySource.discovered_token_accounts,
    'strict replay discovered account count'
  );
  assertSame(
    replaySource.manifest_sha256,
    sourceFileSha256(historySource.manifest, 'history manifest'),
    'strict replay manifest SHA-256'
  );
  const transactionSha256 = sourceFileSha256(
    historySource.transactions_file,
    'history transaction stream'
  );
  assertSame(
    replaySource.expected_transaction_sha256,
    transactionSha256,
    'strict replay expected transaction SHA-256'
  );
  if (replaySource.observed_transaction_sha256 !== null && replaySource.observed_transaction_sha256 !== undefined) {
    assertSame(
      replaySource.observed_transaction_sha256,
      transactionSha256,
      'strict replay observed transaction SHA-256'
    );
  } else if (report.bounded_selected_dump_scan_complete === true) {
    throw new Error('A complete strict replay report has no observed transaction SHA-256');
  }
  assertSame(
    replaySource.registry_sha256,
    sourceFileSha256(historySource.registry_file, 'history registry'),
    'strict replay registry SHA-256'
  );
  assertSame(
    replaySource.accounts_sha256,
    sourceFileSha256(historySource.accounts_file, 'history account artifact'),
    'strict replay accounts SHA-256'
  );
}

async function loadProgramCpiInventory(
  path,
  expectedSha256,
  programReport,
  historySource
) {
  const bytes = await readFile(path);
  const sourceReportSha256 = createHash('sha256').update(bytes).digest('hex');
  if (expectedSha256) {
    assertSha256(expectedSha256, 'program CPI inventory SHA-256 pin');
    if (sourceReportSha256 !== expectedSha256.toLowerCase()) {
      throw new Error('program CPI inventory SHA-256 pin does not match');
    }
  }
  const report = JSON.parse(bytes.toString('utf8'));
  const label = 'program CPI inventory';
  assertObject(report, label);
  if (report.schema_version !== 2 || report.artifact_kind !== 'program_inventory') {
    throw new Error(`${label} has an unsupported schema or artifact kind`);
  }
  if (report.complete !== true || report.instruction_program_resolution_complete !== true) {
    throw new Error(`${label} is not complete`);
  }
  assertObject(report.source, `${label}.source`);
  assertObject(report.source.target_accounts, `${label}.source.target_accounts`);
  assertObject(report.counters, `${label}.counters`);
  assertArray(report.programs, `${label}.programs`);

  const source = report.source;
  assertSame(source.mint, historySource.mint, `${label} mint`);
  assertSame(source.first_epoch, historySource.first_epoch, `${label} first epoch`);
  assertSame(source.last_epoch, historySource.last_epoch, `${label} last epoch`);
  assertSame(source.transactions, historySource.transactions, `${label} transaction count`);
  assertSame(
    source.manifest_sha256,
    sourceFileSha256(historySource.manifest, 'history manifest'),
    `${label} manifest SHA-256`
  );
  assertSame(
    source.transaction_stream_sha256,
    sourceFileSha256(historySource.transactions_file, 'history transaction stream'),
    `${label} transaction SHA-256`
  );
  assertSame(
    source.pubkey_registry_sha256,
    sourceFileSha256(historySource.registry_file, 'history registry'),
    `${label} registry SHA-256`
  );
  assertSame(
    source.registry_entries,
    historySource.registry_entries,
    `${label} registry entries`
  );

  const targetAccounts = source.target_accounts;
  assertNonEmptyString(targetAccounts.file, `${label}.source.target_accounts.file`);
  assertSha256(targetAccounts.sha256, `${label}.source.target_accounts.sha256`);
  assertSame(
    targetAccounts.sha256,
    sourceFileSha256(historySource.accounts_file, 'history account artifact'),
    `${label} account SHA-256`
  );
  assertNonNegativeSafeInteger(
    targetAccounts.discovered_token_accounts,
    `${label}.source.target_accounts.discovered_token_accounts`
  );
  assertSame(
    targetAccounts.discovered_token_accounts,
    historySource.discovered_token_accounts,
    `${label} discovered token accounts`
  );
  assertNonNegativeSafeInteger(
    targetAccounts.target_addresses,
    `${label}.source.target_accounts.target_addresses`
  );
  assertSame(
    targetAccounts.target_addresses,
    safeIntegerAdd(
      targetAccounts.discovered_token_accounts,
      1,
      `${label} target address count`
    ),
    `${label} target addresses`
  );
  assertNonEmptyString(
    targetAccounts.membership_definition,
    `${label}.source.target_accounts.membership_definition`
  );

  if (report.programs.length !== programReport.programs.length) {
    throw new Error(`${label} program count does not match the identification report`);
  }
  const targetFields = [
    'target_account_inner_occurrences',
    'target_account_inner_transactions',
    'target_mint_inner_occurrences',
    'target_token_account_inner_occurrences',
    'target_account_inner_references',
    'target_mint_inner_references',
    'target_token_account_inner_references'
  ];
  const sums = Object.fromEntries(targetFields.map((field) => [field, 0]));
  const rowsByProgramId = new Map();
  let programsWithTargetCpi = 0;

  for (const [index, row] of report.programs.entries()) {
    const rowLabel = `${label}.programs[${index}]`;
    assertObject(row, rowLabel);
    const identified = programReport.programs[index];
    if (!identified) throw new Error(`${rowLabel} has no identification row`);
    for (const field of [
      'registry_id',
      'program_id',
      'total_occurrences',
      'outer_occurrences',
      'inner_occurrences',
      'transactions'
    ]) {
      if (row[field] !== identified[field]) {
        throw new Error(`${rowLabel}.${field} does not match the identification report`);
      }
    }
    if (rowsByProgramId.has(row.program_id)) {
      throw new Error(`${label} has duplicate program ID ${row.program_id}`);
    }
    rowsByProgramId.set(row.program_id, row);

    for (const field of targetFields) {
      assertNonNegativeSafeInteger(row[field], `${rowLabel}.${field}`);
      sums[field] = safeIntegerAdd(sums[field], row[field], `${label} ${field} total`);
    }
    if (row.target_account_inner_occurrences > row.inner_occurrences) {
      throw new Error(`${rowLabel}.target_account_inner_occurrences exceeds all inner occurrences`);
    }
    if (
      row.target_account_inner_transactions > row.transactions ||
      row.target_account_inner_transactions > row.target_account_inner_occurrences
    ) {
      throw new Error(`${rowLabel}.target_account_inner_transactions is inconsistent`);
    }
    if (
      row.target_mint_inner_occurrences > row.target_account_inner_occurrences ||
      row.target_token_account_inner_occurrences > row.target_account_inner_occurrences ||
      row.target_account_inner_occurrences >
        row.target_mint_inner_occurrences + row.target_token_account_inner_occurrences
    ) {
      throw new Error(`${rowLabel} target CPI occurrence categories are inconsistent`);
    }
    if (
      row.target_account_inner_references !==
      row.target_mint_inner_references + row.target_token_account_inner_references
    ) {
      throw new Error(`${rowLabel} target CPI reference categories do not reconcile`);
    }
    if (row.target_account_inner_references < row.target_account_inner_occurrences) {
      throw new Error(`${rowLabel} has fewer target references than target CPI occurrences`);
    }
    const hasTargetCpi = row.target_account_inner_occurrences !== 0;
    if (
      hasTargetCpi !== (row.target_account_inner_transactions !== 0) ||
      hasTargetCpi !== (row.target_account_inner_references !== 0)
    ) {
      throw new Error(`${rowLabel} target CPI zero state is inconsistent`);
    }
    if (hasTargetCpi) programsWithTargetCpi += 1;
  }

  for (const field of targetFields) {
    assertNonNegativeSafeInteger(report.counters[field], `${label}.counters.${field}`);
    if (report.counters[field] !== sums[field]) {
      throw new Error(`${label}.counters.${field} does not match its program rows`);
    }
  }
  assertNonNegativeSafeInteger(
    report.counters.transactions_with_target_account_inner_instructions,
    `${label}.counters.transactions_with_target_account_inner_instructions`
  );
  if (
    report.counters.transactions_with_target_account_inner_instructions >
    historySource.transactions
  ) {
    throw new Error(`${label} target CPI transaction count exceeds the source`);
  }
  if (
    (report.counters.target_account_inner_occurrences === 0) !==
    (report.counters.transactions_with_target_account_inner_instructions === 0)
  ) {
    throw new Error(`${label} target CPI transaction zero state is inconsistent`);
  }

  return {
    sourceReportSha256,
    rowsByProgramId,
    publicSummary: {
      complete: true,
      source_report_sha256: sourceReportSha256,
      target_accounts: targetAccounts,
      counters: {
        ...Object.fromEntries(
          targetFields.map((field) => [field, report.counters[field]])
        ),
        transactions_with_target_account_inner_instructions:
          report.counters.transactions_with_target_account_inner_instructions,
        programs_with_target_account_inner_instructions: programsWithTargetCpi
      }
    }
  };
}

function validateProgramSourceIdentity(programReport, historySource) {
  const programSource = programReport.source;
  assertSame(programSource.first_epoch, historySource.first_epoch, 'program report first epoch');
  assertSame(programSource.last_epoch, historySource.last_epoch, 'program report last epoch');
  assertSame(
    programReport.counters.transactions,
    historySource.transactions,
    'program report transaction count'
  );
  assertSame(
    programSource.dump_manifest_sha256,
    sourceFileSha256(historySource.manifest, 'history manifest'),
    'program report manifest SHA-256'
  );
  assertSame(
    programSource.dump_transaction_stream_sha256,
    sourceFileSha256(historySource.transactions_file, 'history transaction stream'),
    'program report transaction SHA-256'
  );
  assertSame(
    programSource.dump_pubkey_registry_sha256,
    sourceFileSha256(historySource.registry_file, 'history registry'),
    'program report registry SHA-256'
  );
}

function validateProgramReport(programReport, requireComplete) {
  const label = 'program report';
  if (programReport.schema_version !== 1) {
    throw new Error(`${label} has an unsupported schema version`);
  }
  if (typeof programReport.complete !== 'boolean') {
    throw new Error(`${label}.complete is not a boolean`);
  }
  if (requireComplete && programReport.complete !== true) {
    throw new Error('A release data build requires a complete program report');
  }
  assertObject(programReport.definitions, `${label} definitions`);
  validateNumberRecord(programReport.source_match_counts, `${label} source_match_counts`);

  const integerCounterFields = [
    'transactions',
    'programs_total',
    'programs_identified',
    'programs_unidentified',
    'programs_named_onchain',
    'programs_added_by_public_sources',
    'usable_onchain_idls',
    'address_clean_onchain_idls',
    'programs_with_any_decoder_source',
    'instruction_occurrences_total',
    'identified_instruction_occurrences',
    'unidentified_instruction_occurrences',
    'decoder_source_instruction_occurrences',
    'identified_outer_occurrences',
    'identified_inner_occurrences',
    'ignored_generic_or_empty_evidence',
    'programs_explicitly_excluded_as_class_only'
  ];
  for (const field of integerCounterFields) {
    assertNonNegativeSafeInteger(programReport.counters[field], `${label} counters.${field}`);
  }
  for (const field of [
    'identified_program_ratio',
    'decoder_source_program_ratio',
    'identified_instruction_occurrence_ratio',
    'decoder_source_instruction_occurrence_ratio'
  ]) {
    const value = programReport.counters[field];
    if (!Number.isFinite(value) || value < 0 || value > 1) {
      throw new Error(`${label} counters.${field} is not a ratio from zero to one`);
    }
  }

  const programIds = new Set();
  const registryIds = new Set();
  const sourceMatches = new Map();
  const totals = {
    identifiedPrograms: 0,
    unidentifiedPrograms: 0,
    publicSourceOnlyPrograms: 0,
    usableOnchainIdls: 0,
    addressCleanOnchainIdls: 0,
    decoderPrograms: 0,
    instructionOccurrences: 0,
    identifiedOccurrences: 0,
    unidentifiedOccurrences: 0,
    decoderOccurrences: 0,
    identifiedOuterOccurrences: 0,
    identifiedInnerOccurrences: 0
  };

  for (const [index, program] of programReport.programs.entries()) {
    const rowLabel = `${label}.programs[${index}]`;
    assertObject(program, rowLabel);
    assertNonNegativeSafeInteger(program.rank, `${rowLabel}.rank`);
    if (program.rank !== index + 1) {
      throw new Error(`${label} program ranks are not complete and sequential`);
    }
    assertNonNegativeSafeInteger(program.registry_id, `${rowLabel}.registry_id`);
    assertNonEmptyString(program.program_id, `${rowLabel}.program_id`);
    if (registryIds.has(program.registry_id)) {
      throw new Error(`${label} has duplicate registry ID ${program.registry_id}`);
    }
    if (programIds.has(program.program_id)) {
      throw new Error(`${label} has duplicate program ID ${program.program_id}`);
    }
    registryIds.add(program.registry_id);
    programIds.add(program.program_id);

    for (const field of [
      'usable_onchain_idl',
      'address_clean_onchain_idl',
      'decoder_source_found'
    ]) {
      if (typeof program[field] !== 'boolean') {
        throw new Error(`${rowLabel}.${field} is not a boolean`);
      }
    }
    if (program.address_clean_onchain_idl && !program.usable_onchain_idl) {
      throw new Error(`${rowLabel} has an address-clean IDL that is not usable`);
    }
    if (program.usable_onchain_idl && !program.decoder_source_found) {
      throw new Error(`${rowLabel} has a usable IDL but no decoder source`);
    }

    for (const field of [
      'total_occurrences',
      'outer_occurrences',
      'inner_occurrences',
      'transactions'
    ]) {
      assertNonNegativeSafeInteger(program[field], `${rowLabel}.${field}`);
    }
    const scopedOccurrences = safeIntegerAdd(
      program.outer_occurrences,
      program.inner_occurrences,
      `${rowLabel} direct and inner occurrence total`
    );
    if (program.total_occurrences !== scopedOccurrences) {
      throw new Error(`${rowLabel}.total_occurrences does not equal outer plus inner occurrences`);
    }
    if (program.total_occurrences === 0 || program.transactions === 0) {
      throw new Error(`${rowLabel} is not an observed program row`);
    }
    if (program.transactions > program.total_occurrences) {
      throw new Error(`${rowLabel}.transactions exceeds its instruction occurrences`);
    }

    assertArray(program.evidence, `${rowLabel}.evidence`);
    const rowEvidenceSources = new Set();
    for (const [evidenceIndex, evidence] of program.evidence.entries()) {
      const evidenceLabel = `${rowLabel}.evidence[${evidenceIndex}]`;
      assertObject(evidence, evidenceLabel);
      assertNonEmptyString(evidence.source, `${evidenceLabel}.source`);
      assertNonEmptyString(evidence.name, `${evidenceLabel}.name`);
      assertNonEmptyString(evidence.confidence, `${evidenceLabel}.confidence`);
      if (typeof evidence.decoder_source !== 'boolean') {
        throw new Error(`${evidenceLabel}.decoder_source is not a boolean`);
      }
      rowEvidenceSources.add(evidence.source);
    }
    for (const source of rowEvidenceSources) {
      sourceMatches.set(source, (sourceMatches.get(source) ?? 0) + 1);
    }

    if (program.identity_status === 'identified') {
      totals.identifiedPrograms += 1;
      assertNonEmptyString(program.selected_name, `${rowLabel}.selected_name`);
      assertNonEmptyString(program.selected_source, `${rowLabel}.selected_source`);
      assertNonEmptyString(program.selected_confidence, `${rowLabel}.selected_confidence`);
      if (
        !program.evidence.some(
          (evidence) =>
            evidence.name === program.selected_name &&
            evidence.source === program.selected_source &&
            evidence.confidence === program.selected_confidence
        )
      ) {
        throw new Error(`${rowLabel} selected identity is missing from its evidence`);
      }
      if (!program.evidence.some((evidence) => evidence.source.startsWith('onchain_'))) {
        totals.publicSourceOnlyPrograms += 1;
      }
      totals.identifiedOccurrences = safeIntegerAdd(
        totals.identifiedOccurrences,
        program.total_occurrences,
        `${label} identified occurrence total`
      );
      totals.identifiedOuterOccurrences = safeIntegerAdd(
        totals.identifiedOuterOccurrences,
        program.outer_occurrences,
        `${label} identified outer occurrence total`
      );
      totals.identifiedInnerOccurrences = safeIntegerAdd(
        totals.identifiedInnerOccurrences,
        program.inner_occurrences,
        `${label} identified inner occurrence total`
      );
    } else if (program.identity_status === 'unidentified') {
      totals.unidentifiedPrograms += 1;
      if (
        program.selected_name !== null ||
        program.selected_source !== null ||
        program.selected_confidence !== null ||
        program.evidence.length !== 0
      ) {
        throw new Error(`${rowLabel} has identity evidence but is marked unidentified`);
      }
      totals.unidentifiedOccurrences = safeIntegerAdd(
        totals.unidentifiedOccurrences,
        program.total_occurrences,
        `${label} unidentified occurrence total`
      );
    } else {
      throw new Error(`${rowLabel}.identity_status is not supported`);
    }

    if (program.usable_onchain_idl) totals.usableOnchainIdls += 1;
    if (program.address_clean_onchain_idl) totals.addressCleanOnchainIdls += 1;
    if (program.decoder_source_found) {
      totals.decoderPrograms += 1;
      totals.decoderOccurrences = safeIntegerAdd(
        totals.decoderOccurrences,
        program.total_occurrences,
        `${label} decoder occurrence total`
      );
    }
    totals.instructionOccurrences = safeIntegerAdd(
      totals.instructionOccurrences,
      program.total_occurrences,
      `${label} instruction occurrence total`
    );
  }

  const expectedCounters = {
    programs_total: programReport.programs.length,
    programs_identified: totals.identifiedPrograms,
    programs_unidentified: totals.unidentifiedPrograms,
    programs_added_by_public_sources: totals.publicSourceOnlyPrograms,
    usable_onchain_idls: totals.usableOnchainIdls,
    address_clean_onchain_idls: totals.addressCleanOnchainIdls,
    programs_with_any_decoder_source: totals.decoderPrograms,
    instruction_occurrences_total: totals.instructionOccurrences,
    identified_instruction_occurrences: totals.identifiedOccurrences,
    unidentified_instruction_occurrences: totals.unidentifiedOccurrences,
    decoder_source_instruction_occurrences: totals.decoderOccurrences,
    identified_outer_occurrences: totals.identifiedOuterOccurrences,
    identified_inner_occurrences: totals.identifiedInnerOccurrences,
    identified_program_ratio: roundedRatio(
      totals.identifiedPrograms,
      programReport.programs.length
    ),
    decoder_source_program_ratio: roundedRatio(
      totals.decoderPrograms,
      programReport.programs.length
    ),
    identified_instruction_occurrence_ratio: roundedRatio(
      totals.identifiedOccurrences,
      totals.instructionOccurrences
    ),
    decoder_source_instruction_occurrence_ratio: roundedRatio(
      totals.decoderOccurrences,
      totals.instructionOccurrences
    )
  };
  for (const [field, expected] of Object.entries(expectedCounters)) {
    if (programReport.counters[field] !== expected) {
      throw new Error(`${label} counters.${field} does not match its program rows`);
    }
  }
  if (programReport.counters.programs_named_onchain > programReport.programs.length) {
    throw new Error(`${label} counters.programs_named_onchain exceeds its program rows`);
  }

  const reportedSources = Object.entries(programReport.source_match_counts);
  if (reportedSources.length !== sourceMatches.size) {
    throw new Error(`${label} source_match_counts does not match its program evidence`);
  }
  for (const [source, expected] of sourceMatches) {
    if (programReport.source_match_counts[source] !== expected) {
      throw new Error(`${label} source_match_counts.${source} does not match its program evidence`);
    }
  }
}

function roundedRatio(numerator, denominator) {
  if (denominator === 0) return 0;
  return Number((numerator / denominator).toFixed(12));
}

function sourceFileSha256(value, label) {
  assertObject(value, label);
  assertSha256(value.sha256, `${label} SHA-256`);
  return value.sha256;
}

function assertSha256(value, label) {
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/i.test(value)) {
    throw new Error(`${label} is not a hexadecimal SHA-256 digest`);
  }
}

function assertNonNegativeSafeInteger(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} is not a non-negative safe integer`);
  }
}

function assertSame(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(`${label} does not match the SPYx history report`);
  }
}

function optionalPath(value) {
  return value ? resolve(value) : null;
}

function fillCalendarDays(inputRows, decimals) {
  if (inputRows.length === 0) return { rows: [], insertedDays: 0 };

  const byDate = new Map(inputRows.map((row) => [row.utc_date, row]));
  const firstDate = parseUtcDate(inputRows[0].utc_date);
  const lastDate = parseUtcDate(inputRows.at(-1).utc_date);
  const rows = [];
  let previous = null;
  let insertedDays = 0;

  for (let cursor = firstDate; cursor <= lastDate; cursor += 86_400_000) {
    const utcDate = new Date(cursor).toISOString().slice(0, 10);
    const sourceRow = byDate.get(utcDate);
    if (sourceRow) {
      rows.push(sourceRow);
      previous = sourceRow;
      continue;
    }

    insertedDays += 1;
    rows.push({
      utc_date: utcDate,
      selected_transactions: 0,
      public_balance_changing_transactions: 0,
      public_owner_reassignment_transactions: 0,
      positive_public_balance_holders: previous?.positive_public_balance_holders ?? 0,
      active_public_token_accounts: previous?.active_public_token_accounts ?? 0,
      public_raw_balance_sum: previous?.public_raw_balance_sum ?? zeroAmount(decimals),
      public_bilateral_movement: zeroAmount(decimals),
      inferred_public_mint: zeroAmount(decimals),
      inferred_public_burn: zeroAmount(decimals),
      top_1_concentration: previous?.top_1_concentration ?? zeroConcentration(decimals),
      top_10_concentration: previous?.top_10_concentration ?? zeroConcentration(decimals),
      top_100_concentration: previous?.top_100_concentration ?? zeroConcentration(decimals)
    });
  }

  return { rows, insertedDays };
}

function zeroAmount(decimals) {
  return { raw_amount: '0', base_units: `0.${'0'.repeat(decimals)}` };
}

function zeroConcentration(decimals) {
  return {
    amount: zeroAmount(decimals),
    supply_fraction_numerator_raw: '0',
    supply_fraction_denominator_raw: '0',
    supply_share_parts_per_million_floor: 0
  };
}

function parseUtcDate(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`Invalid UTC date: ${value}`);
  }
  const parsed = Date.parse(`${value}T00:00:00Z`);
  if (
    !Number.isFinite(parsed) ||
    new Date(parsed).toISOString().slice(0, 10) !== value
  ) {
    throw new Error(`Invalid UTC date: ${value}`);
  }
  return parsed;
}

function assertObject(value, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`Expected ${name} to be an object`);
  }
}

function assertArray(value, name) {
  if (!Array.isArray(value)) throw new Error(`Expected ${name} to be an array`);
}
