import { base } from '$app/paths';
import type {
  PdaAuthorityEstimate,
  PdaAuthorityEstimateReport
} from '$lib/types';

let reportPromise: Promise<PdaAuthorityEstimateReport> | null = null;

export async function getPdaAuthorityEstimate(
  address: string,
  expectedTransactionSha256: string
): Promise<PdaAuthorityEstimate | null> {
  const report = await loadPdaAuthorityEstimateReport();
  validateDatasetBinding(report, expectedTransactionSha256);
  return report.estimates.find((estimate) => estimate.subject_pda === address) ?? null;
}

export async function getPdaAuthorityEstimatesByProgram(
  programId: string,
  expectedTransactionSha256: string
): Promise<PdaAuthorityEstimate[]> {
  const report = await loadPdaAuthorityEstimateReport();
  validateDatasetBinding(report, expectedTransactionSha256);
  return report.estimates.filter(
    (estimate) =>
      estimate.runtime_owner_program_id === programId ||
      estimate.direct_caller_program_id === programId
  );
}

function validateDatasetBinding(
  report: PdaAuthorityEstimateReport,
  expectedTransactionSha256: string
): void {
  if (
    report.source_binding.transactions_sha256.toLowerCase() !==
    expectedTransactionSha256.toLowerCase()
  ) {
    throw new Error('The PDA authority data does not match the transaction dataset.');
  }
}

function loadPdaAuthorityEstimateReport(): Promise<PdaAuthorityEstimateReport> {
  reportPromise ??= fetch(`${base}/data/spyx-pda-authority-estimates.json`, {
    headers: { accept: 'application/json' }
  })
    .then(async (response) => {
      if (!response.ok) {
        throw new Error(`The PDA authority data returned HTTP ${response.status}.`);
      }
      return response.json() as Promise<unknown>;
    })
    .then(validateReport);
  return reportPromise.catch((error: unknown) => {
    reportPromise = null;
    throw error;
  });
}

function validateReport(value: unknown): PdaAuthorityEstimateReport {
  if (!isRecord(value)) throw new Error('The PDA authority data is not an object.');
  if (value.schema_version !== 1 || value.artifact_kind !== 'spyx_pda_authority_estimates') {
    throw new Error('The PDA authority data has an unsupported schema.');
  }
  if (!isRecord(value.source_binding)) {
    throw new Error('The PDA authority data has no source binding.');
  }
  if (
    typeof value.source_binding.transactions_sha256 !== 'string' ||
    !/^[0-9a-f]{64}$/i.test(value.source_binding.transactions_sha256)
  ) {
    throw new Error('The PDA authority data has an invalid transaction digest.');
  }
  if (!Array.isArray(value.estimates)) {
    throw new Error('The PDA authority data has no estimate array.');
  }
  return value as unknown as PdaAuthorityEstimateReport;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
