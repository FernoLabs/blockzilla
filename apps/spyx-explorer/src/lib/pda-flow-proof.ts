import { base } from '$app/paths';
import type { PdaFlowProof, PdaFlowProofReport } from '$lib/types';

let reportPromise: Promise<PdaFlowProofReport> | null = null;

export async function getPdaFlowProof(
  address: string,
  expectedTransactionSha256: string
): Promise<PdaFlowProof | null> {
  const report = await loadPdaFlowProofReport();
  if (
    report.source_binding.transactions_sha256.toLowerCase() !==
    expectedTransactionSha256.toLowerCase()
  ) {
    throw new Error('The PDA flow proof does not match the transaction dataset.');
  }
  return report.proofs.find((proof) => proof.subject_pda === address) ?? null;
}

function loadPdaFlowProofReport(): Promise<PdaFlowProofReport> {
  reportPromise ??= fetch(`${base}/data/spyx-pda-flow-proofs.json`, {
    headers: { accept: 'application/json' }
  })
    .then(async (response) => {
      if (!response.ok) {
        throw new Error(`The PDA flow proof returned HTTP ${response.status}.`);
      }
      return response.json() as Promise<unknown>;
    })
    .then(validateReport);
  return reportPromise.catch((error: unknown) => {
    reportPromise = null;
    throw error;
  });
}

function validateReport(value: unknown): PdaFlowProofReport {
  if (!isRecord(value)) throw new Error('The PDA flow proof is not an object.');
  if (value.schema_version !== 1 || value.artifact_kind !== 'spyx_pda_flow_proofs') {
    throw new Error('The PDA flow proof has an unsupported schema.');
  }
  if (!isRecord(value.source_binding)) {
    throw new Error('The PDA flow proof has no source binding.');
  }
  if (
    typeof value.source_binding.transactions_sha256 !== 'string' ||
    !/^[0-9a-f]{64}$/i.test(value.source_binding.transactions_sha256)
  ) {
    throw new Error('The PDA flow proof has an invalid transaction digest.');
  }
  if (!Array.isArray(value.proofs)) {
    throw new Error('The PDA flow proof has no proof array.');
  }
  return value as unknown as PdaFlowProofReport;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
