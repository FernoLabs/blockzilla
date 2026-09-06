import { asset } from '$app/paths';
import type {
  Amount,
  AuthorityPortfolioHistoryPoint,
  AuthorityPortfolioHistoryReport,
  AuthorityPortfolioHistorySeries
} from '$lib/types';

interface AuthorityPortfolioHistoryShard {
  schema_version: 1;
  artifact_kind: 'spyx_authority_portfolio_history_shard';
  source_schema_version: 2;
  source_binding: AuthorityPortfolioHistoryReport['source_binding'];
  coverage: AuthorityPortfolioHistoryReport['coverage'];
  point_fields: AuthorityPortfolioHistoryReport['point_fields'];
  prefix_length: 2;
  prefix: string;
  series: AuthorityPortfolioHistorySeries[];
}

const shardRequests = new Map<string, Promise<AuthorityPortfolioHistoryShard | null>>();
const expectedPointFields: AuthorityPortfolioHistoryReport['point_fields'] = [
  'transaction_id',
  'slot',
  'block_time',
  'direct_public_balance_raw',
  'estimated_defi_claim_raw'
];

export async function getAuthorityPortfolioHistory(
  address: string,
  expectedTransactionsSha256: string,
  signal?: AbortSignal
): Promise<AuthorityPortfolioHistorySeries | null> {
  const canonicalAddress = address.trim();
  if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(canonicalAddress)) {
    throw new TypeError('authority address must be a base58 public key');
  }
  const prefix = canonicalAddress.slice(0, 2);
  const shard = await loadShard(prefix, signal);
  if (shard === null) return null;
  if (
    shard.source_binding.transactions_sha256.toLowerCase() !==
    expectedTransactionsSha256.toLowerCase()
  ) {
    throw new Error('The authority portfolio history does not match the transaction index.');
  }
  return shard.series.find((series) => series.authority === canonicalAddress) ?? null;
}

async function loadShard(
  prefix: string,
  signal?: AbortSignal
): Promise<AuthorityPortfolioHistoryShard | null> {
  const cached = shardRequests.get(prefix);
  if (cached) return cached;

  const request = fetch(
    asset(`/data/spyx-authority-portfolio-history-by-prefix/${shardFileStem(prefix)}.json`),
    { headers: { accept: 'application/json' }, signal }
  )
    .then(async (response) => {
      if (response.status === 404) return null;
      if (!response.ok) {
        throw new Error(`Authority portfolio history request failed with HTTP ${response.status}.`);
      }
      return validateShard(await response.json(), prefix);
    })
    .catch((error: unknown) => {
      shardRequests.delete(prefix);
      throw error;
    });
  shardRequests.set(prefix, request);
  return request;
}

function shardFileStem(prefix: string): string {
  return Array.from(prefix, (character) =>
    character.codePointAt(0)?.toString(16).padStart(2, '0') ?? ''
  ).join('');
}

function validateShard(value: unknown, expectedPrefix: string): AuthorityPortfolioHistoryShard {
  const shard = requireRecord(value, 'authority portfolio history shard');
  if (
    shard.schema_version !== 1 ||
    shard.artifact_kind !== 'spyx_authority_portfolio_history_shard' ||
    shard.source_schema_version !== 2
  ) {
    throw new Error('Authority portfolio history shard format is not supported.');
  }
  if (shard.prefix_length !== 2 || shard.prefix !== expectedPrefix) {
    throw new Error('Authority portfolio history shard prefix is invalid.');
  }
  const sourceBinding = requireRecord(shard.source_binding, 'authority portfolio history source');
  for (const field of [
    'manifest_sha256',
    'transactions_sha256',
    'registry_sha256',
    'replay_state_sha256'
  ]) {
    const digest = sourceBinding[field];
    if (typeof digest !== 'string' || !/^[0-9a-f]{64}$/i.test(digest)) {
      throw new Error(`Authority portfolio history ${field} is invalid.`);
    }
  }
  const coverage = requireRecord(shard.coverage, 'authority portfolio history coverage');
  if (
    coverage.complete !== true ||
    coverage.method !== 'forward_replay_216000_slot_window_end_sparse_aggregate_tuple_v2' ||
    coverage.slot_window_width !== 216000 ||
    coverage.final_sample_matches_current_portfolio !== true
  ) {
    throw new Error('Authority portfolio history coverage is incomplete or unsupported.');
  }
  for (const field of [
    'transactions_scanned',
    'state_samples',
    'authority_series',
    'history_points'
  ]) {
    requireNonNegativeInteger(coverage[field], `authority portfolio history coverage.${field}`);
  }
  if (
    !Array.isArray(shard.point_fields) ||
    shard.point_fields.length !== expectedPointFields.length ||
    shard.point_fields.some((field, index) => field !== expectedPointFields[index])
  ) {
    throw new Error('Authority portfolio history point fields are unsupported.');
  }
  if (!Array.isArray(shard.series)) {
    throw new Error('Authority portfolio history series are invalid.');
  }
  const authorities = new Set<string>();
  const series = shard.series.map((candidate, index) => {
    const validated = validateSeries(candidate, `${index}`, expectedPrefix);
    if (authorities.has(validated.authority)) {
      throw new Error('Authority portfolio history has a duplicate authority.');
    }
    authorities.add(validated.authority);
    return validated;
  });
  return { ...shard, series } as unknown as AuthorityPortfolioHistoryShard;
}

function validateSeries(
  value: unknown,
  label: string,
  expectedPrefix: string
): AuthorityPortfolioHistorySeries {
  const series = requireRecord(value, `authority portfolio history series ${label}`);
  if (
    typeof series.authority !== 'string' ||
    !series.authority.startsWith(expectedPrefix) ||
    !Array.isArray(series.points)
  ) {
    throw new Error(`Authority portfolio history series ${label} is invalid.`);
  }
  let previousTransactionId = -1;
  const points = series.points.map((point, index) => {
    const validated = validatePoint(point, `${label}.${index}`);
    if (validated.transaction_id <= previousTransactionId) {
      throw new Error(`Authority portfolio history series ${label} is not transaction ordered.`);
    }
    previousTransactionId = validated.transaction_id;
    return validated;
  });
  return { authority: series.authority, points };
}

function validatePoint(value: unknown, label: string): AuthorityPortfolioHistoryPoint {
  if (!Array.isArray(value) || value.length !== expectedPointFields.length) {
    throw new Error(`Authority portfolio history point ${label} is not a five-field tuple.`);
  }
  const [transactionId, slot, blockTime, directRawValue, claimRawValue] = value;
  requireNonNegativeInteger(transactionId, `authority portfolio history point ${label}.transaction_id`);
  requireNonNegativeInteger(slot, `authority portfolio history point ${label}.slot`);
  if (blockTime !== null && !Number.isSafeInteger(blockTime)) {
    throw new Error(`Authority portfolio history point ${label} has an invalid block time.`);
  }
  const directRaw = validateRawAmount(directRawValue, `${label}.direct_public_balance_raw`);
  const claimRaw = validateRawAmount(claimRawValue, `${label}.estimated_defi_claim_raw`);
  const totalRaw = (BigInt(directRaw) + BigInt(claimRaw)).toString();
  return {
    transaction_id: Number(transactionId),
    slot: Number(slot),
    block_time: blockTime === null ? null : Number(blockTime),
    direct_public_balance: amountFromRaw(directRaw),
    estimated_defi_claim: amountFromRaw(claimRaw),
    estimated_total_exposure: amountFromRaw(totalRaw)
  };
}

function validateRawAmount(value: unknown, label: string): string {
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new Error(`${label} is not an unsigned decimal string.`);
  }
  return value;
}

function amountFromRaw(rawAmount: string): Amount {
  const divisor = 100_000_000n;
  const raw = BigInt(rawAmount);
  return {
    raw_amount: rawAmount,
    base_units: `${raw / divisor}.${(raw % divisor).toString().padStart(8, '0')}`
  };
}

function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`${label} is not an object.`);
  }
  return value as Record<string, unknown>;
}

function requireNonNegativeInteger(value: unknown, label: string): void {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new Error(`${label} is not a non-negative safe integer.`);
  }
}
