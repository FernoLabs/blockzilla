import { asset } from '$app/paths';
import type { AuthorityPortfolio, AuthorityPortfolioReport } from '$lib/types';

interface AuthorityPortfolioShard {
  schema_version: 1;
  artifact_kind: 'spyx_authority_portfolio_shard';
  source_binding: AuthorityPortfolioReport['source_binding'];
  prefix: string;
  portfolios: AuthorityPortfolio[];
}

const shardRequests = new Map<string, Promise<AuthorityPortfolioShard>>();

export async function getAuthorityPortfolio(
  address: string,
  expectedTransactionsSha256: string,
  signal?: AbortSignal
): Promise<AuthorityPortfolio | null> {
  const canonicalAddress = address.trim();
  if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(canonicalAddress)) {
    throw new TypeError('authority address must be a base58 public key');
  }
  const prefix = canonicalAddress[0];
  const shard = await loadShard(prefix, signal);
  if (
    shard.source_binding.transactions_sha256.toLowerCase() !==
    expectedTransactionsSha256.toLowerCase()
  ) {
    throw new Error('The authority portfolio dataset does not match the transaction index.');
  }
  return shard.portfolios.find((portfolio) => portfolio.authority === canonicalAddress) ?? null;
}

async function loadShard(prefix: string, signal?: AbortSignal): Promise<AuthorityPortfolioShard> {
  const cached = shardRequests.get(prefix);
  if (cached) return cached;

  const request = fetch(
    asset(`/data/spyx-authority-portfolios-by-prefix/${shardFileStem(prefix)}.json`),
    { headers: { accept: 'application/json' }, signal }
  )
    .then(async (response) => {
      if (!response.ok) {
        throw new Error(`Authority portfolio request failed with HTTP ${response.status}.`);
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

function validateShard(value: unknown, expectedPrefix: string): AuthorityPortfolioShard {
  if (!isRecord(value)) throw new Error('Authority portfolio shard is not an object.');
  if (value.schema_version !== 1 || value.artifact_kind !== 'spyx_authority_portfolio_shard') {
    throw new Error('Authority portfolio shard format is not supported.');
  }
  if (value.prefix !== expectedPrefix || !Array.isArray(value.portfolios)) {
    throw new Error('Authority portfolio shard prefix or rows are invalid.');
  }
  if (!isRecord(value.source_binding)) {
    throw new Error('Authority portfolio source binding is missing.');
  }
  const transactionsSha256 = value.source_binding.transactions_sha256;
  if (typeof transactionsSha256 !== 'string' || !/^[0-9a-f]{64}$/i.test(transactionsSha256)) {
    throw new Error('Authority portfolio transaction digest is invalid.');
  }
  for (const portfolio of value.portfolios) {
    if (!isRecord(portfolio) || typeof portfolio.authority !== 'string') {
      throw new Error('Authority portfolio row is invalid.');
    }
    if (!portfolio.authority.startsWith(expectedPrefix)) {
      throw new Error('Authority portfolio row is in the wrong shard.');
    }
  }
  return value as unknown as AuthorityPortfolioShard;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
