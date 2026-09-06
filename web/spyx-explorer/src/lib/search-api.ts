import { env } from '$env/dynamic/public';

export type PostingKind = 'target-address' | 'token-account' | 'program' | 'owner';
export type ProgramInstructionScope = 'all' | 'direct' | 'inner';

export interface TransactionCoordinate {
  epoch: number;
  slot: number;
  source_block_id: number;
  tx_index: number;
}

export interface TransactionAccountDetail {
  account_index: number;
  registry_id: number;
  address: string;
}

export interface DumpTransaction {
  id: number;
  coordinate: TransactionCoordinate;
  block: {
    parent_slot: number;
    block_time: number | null;
    block_height: number | null;
    transaction_count: number;
  };
  signatures: string[];
  accounts: TransactionAccountDetail[];
  flags: number;
  source_wire_profile: string;
  message_bytes_base64: string;
  metadata_bytes_base64: string;
}

export interface TransactionLookupResponse {
  transaction: DumpTransaction;
  [key: string]: unknown;
}

export interface PostingItem {
  transaction_id: number;
  coordinate: TransactionCoordinate;
  first_signature: string | null;
}

interface BasePostingsResponse {
  key: string;
  items: PostingItem[];
  next_cursor: string | null;
  limit: number;
  registry_id: number;
  flags: number;
  total: number;
  offset: number;
  [key: string]: unknown;
}

export type PostingsResponse =
  | (BasePostingsResponse & {
      kind: 'program';
      instruction_scope: ProgramInstructionScope;
    })
  | (BasePostingsResponse & {
      kind: Exclude<PostingKind, 'program'>;
      instruction_scope?: never;
    });

export interface SearchHealthResponse {
  status: 'ok';
  index: {
    complete: boolean;
    transactions: number;
    source_transaction_sha256: string;
  };
  postings: {
    available: boolean;
    complete: boolean;
    target_address: boolean;
    token_account: boolean;
    program: boolean;
    owner: boolean;
    target_address_keys: number;
    target_address_postings: number;
    program_keys: number;
    program_postings: number;
    owner_keys: number;
    owner_postings: number;
    owner_balance_history?: boolean;
    owner_balance_history_keys?: number;
    owner_balance_history_events?: number;
  };
  market?: {
    available: boolean;
    schema_version: number;
    complete: boolean;
    source_transactions_scanned: number;
    pairs: number;
    programs?: number;
    venues: number;
    source_transaction_sha256: string;
    parser_semantic_version: string;
    parser_implementation_fingerprint: string;
    market_manifest_sha256: string;
    target_mint: string;
    target_mint_id: number;
    target_decimals: number;
    proven_trades: number;
    dataset_latest_block_time?: number;
  };
  [key: string]: unknown;
}

export interface SearchDatasetIdentity {
  transactions: number;
  source_transaction_sha256: string;
}

export type ProvenTraderAttribution = 'parser_proven_exact_trader';
export type AccountTradeSide = 'buy' | 'sell';
export type AccountTradingInterval = '1h' | '4h' | '1d' | '1w' | `${bigint}`;

export interface RegistryKeyView {
  registry_id: number;
  address: string;
}

export interface ProvenDexProgramView extends RegistryKeyView {
  name: string;
  role: 'dex' | 'router';
}

export interface ExactTradePrice {
  numerator: string;
  denominator: string;
  decimal: string;
  chart_display?: number;
  target_multiplier: string;
  target_multiplier_bits: string;
  scaled_ui_config_id: number;
  unscaled_decimal: string;
  unscaled_chart_display?: number;
}

export interface AccountProvenDexTrade {
  trade_id: number;
  transaction: {
    transaction_id: number;
    source_epoch: number;
    slot: number;
    source_block_id: number;
    tx_index: number;
  };
  block_time: number;
  instruction: {
    outer_index: number;
    inner_index?: number;
    stack_height: number;
  };
  instruction_kind_id: number;
  instruction_kind: string;
  instruction_discriminator: string;
  program: ProvenDexProgramView;
  venue: RegistryKeyView;
  router?: ProvenDexProgramView;
  pool?: RegistryKeyView;
  trader: RegistryKeyView;
  user_source?: RegistryKeyView;
  user_destination?: RegistryKeyView;
  target_mint: RegistryKeyView;
  quote_mint: RegistryKeyView;
  side: AccountTradeSide;
  target_amount_raw: string;
  target_amount_scaled_ui_raw: string;
  quote_amount_raw: string;
  target_decimals: number;
  quote_decimals: number;
  price: ExactTradePrice;
  fee_amount_raw: string;
  fee_mint?: RegistryKeyView;
  input_transfer_count: number;
  output_transfer_count: number;
  evidence_flags: number;
  evidence: string[];
}

export interface AccountTraderQuoteActivity {
  quote_mint: RegistryKeyView;
  quote_decimals: number;
  trade_count: number;
  buy_count: number;
  sell_count: number;
  target_bought_raw: string;
  target_sold_raw: string;
  quote_spent_on_buys_raw: string;
  quote_received_from_sells_raw: string;
}

export interface AccountTraderActivityTotals {
  trade_count: number;
  buy_count: number;
  sell_count: number;
  target_bought_raw: string;
  target_sold_raw: string;
  quote_totals: AccountTraderQuoteActivity[];
}

interface ProvenTraderContract {
  supported: true;
  artifact_complete: boolean;
  attribution: ProvenTraderAttribution;
  includes_inferred_trades: false;
  includes_protocol_positions: false;
  trader: RegistryKeyView;
}

export interface AccountTradingSummaryResponse extends ProvenTraderContract {
  has_proven_trades: boolean;
  target_mint: RegistryKeyView;
  target_decimals: number;
  first_block_time?: number;
  last_block_time?: number;
  totals: AccountTraderActivityTotals;
}

export interface AccountProvenTradesResponse extends ProvenTraderContract {
  has_matching_proven_trades: boolean;
  total: number;
  offset: number;
  limit: number;
  trades: AccountProvenDexTrade[];
  next_offset?: number;
}

export interface AccountTradingActivityPoint {
  interval_seconds: number;
  start_time: number;
  end_time: number;
  totals: AccountTraderActivityTotals;
}

export interface AccountTradingActivityResponse extends ProvenTraderContract {
  has_matching_proven_trades: boolean;
  target_mint: RegistryKeyView;
  target_decimals: number;
  selected_quote_mint?: RegistryKeyView;
  selected_program?: ProvenDexProgramView;
  interval_seconds: number;
  time_from: number;
  time_to: number;
  totals: AccountTraderActivityTotals;
  points: AccountTradingActivityPoint[];
}

export interface AccountProvenTradesQuery {
  quote_mint?: string;
  program?: string;
  time_from?: number;
  time_to?: number;
  offset?: number;
  limit?: number;
}

export interface AccountTradingActivityQuery {
  quote_mint?: string;
  program?: string;
  interval?: AccountTradingInterval;
  time_from?: number;
  time_to?: number;
  max_points?: number;
}

export interface AccountBalanceHistoryEvent {
  transaction_id: number;
  slot: number;
  block_time: number | null;
  raw_delta: string;
  post_raw_balance: string;
}

export interface AccountBalanceHistoryResponse {
  supported: true;
  artifact_complete: boolean;
  address: string;
  attribution: string;
  registry_id: number;
  matching_events: number;
  sampled: boolean;
  items: AccountBalanceHistoryEvent[];
}

export type SearchDatasetBinding =
  | {
      status: 'match';
      transaction_count_matches: true;
      source_transaction_sha256_matches: true;
    }
  | {
      status: 'incomplete';
      transaction_count_matches: true;
      source_transaction_sha256_matches: true;
    }
  | {
      status: 'mismatch';
      transaction_count_matches: boolean;
      source_transaction_sha256_matches: boolean;
    };

export class SearchApiError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
    readonly unavailable: boolean
  ) {
    super(message);
    this.name = 'SearchApiError';
  }
}

const configuredApiBase = (env.PUBLIC_SPYX_API_BASE_URL ?? '').trim().replace(/\/+$/, '');

export function searchApiBaseLabel(): string {
  return configuredApiBase || 'Same origin';
}

export function bindSearchHealthToDataset(
  health: SearchHealthResponse,
  expected: SearchDatasetIdentity
): SearchDatasetBinding {
  const transactionCountMatches = health.index.transactions === expected.transactions;
  const sourceTransactionSha256Matches =
    health.index.source_transaction_sha256.toLowerCase() === expected.source_transaction_sha256.toLowerCase();

  if (!transactionCountMatches || !sourceTransactionSha256Matches) {
    return {
      status: 'mismatch',
      transaction_count_matches: transactionCountMatches,
      source_transaction_sha256_matches: sourceTransactionSha256Matches
    };
  }

  return {
    status: health.index.complete ? 'match' : 'incomplete',
    transaction_count_matches: true,
    source_transaction_sha256_matches: true
  };
}

export async function getSearchHealth(signal?: AbortSignal): Promise<SearchHealthResponse> {
  const payload = await requestJson('/healthz', signal, false);
  const response = requireRecord(payload, 'health response');
  if (response.status !== 'ok') {
    throw invalidResponse('health response status is not ok');
  }
  const index = requireRecord(response.index, 'health response index');
  requireBoolean(index.complete, 'health index completion state');
  requireInteger(index.transactions, 'health index transaction count');
  requireSha256(index.source_transaction_sha256, 'health index source transaction SHA-256');
  const postings = requireRecord(response.postings, 'health response postings');
  requireBoolean(postings.available, 'postings availability');
  requireBoolean(postings.complete, 'postings completion state');
  requireBoolean(postings.target_address, 'target-address posting capability');
  requireBoolean(postings.token_account, 'token-account posting capability');
  requireBoolean(postings.program, 'program posting capability');
  requireBoolean(postings.owner, 'owner posting capability');
  requireInteger(postings.target_address_keys, 'target-address key count');
  requireInteger(postings.target_address_postings, 'target-address posting count');
  requireInteger(postings.program_keys, 'program key count');
  requireInteger(postings.program_postings, 'program posting count');
  requireInteger(postings.owner_keys, 'owner key count');
  requireInteger(postings.owner_postings, 'owner posting count');
  if (postings.owner_balance_history !== undefined) {
    requireBoolean(postings.owner_balance_history, 'owner balance-history capability');
  }
  if (postings.owner_balance_history_keys !== undefined) {
    requireInteger(postings.owner_balance_history_keys, 'owner balance-history key count');
  }
  if (postings.owner_balance_history_events !== undefined) {
    requireInteger(postings.owner_balance_history_events, 'owner balance-history event count');
  }
  if (response.market !== undefined) {
    const market = requireRecord(response.market, 'health response market');
    requireBoolean(market.available, 'market availability');
    requireInteger(market.schema_version, 'market schema version');
    requireBoolean(market.complete, 'market completion state');
    requireInteger(market.source_transactions_scanned, 'market source transaction count');
    requireInteger(market.pairs, 'market pair count');
    requireInteger(market.programs ?? market.venues, 'market program count');
    requireSha256(market.source_transaction_sha256, 'market source transaction SHA-256');
    requireString(market.parser_semantic_version, 'market parser semantic version');
    requireSha256(market.parser_implementation_fingerprint, 'market parser implementation fingerprint');
    requireSha256(market.market_manifest_sha256, 'market manifest SHA-256');
    requireString(market.target_mint, 'market target mint');
    requireInteger(market.target_mint_id, 'market target mint ID');
    requireInteger(market.target_decimals, 'market target decimals');
    requireInteger(market.proven_trades, 'market proven trade count');
    if (market.dataset_latest_block_time !== undefined) {
      requireInteger(market.dataset_latest_block_time, 'market latest block time');
    }
  }
  return response as unknown as SearchHealthResponse;
}

export async function getTransactionBySignature(
  signature: string,
  signal?: AbortSignal
): Promise<TransactionLookupResponse | null> {
  const payload = await requestJson(
    `/api/v1/transactions/by-signature/${encodeURIComponent(signature)}`,
    signal
  );
  return payload === null ? null : validateTransactionResponse(payload);
}

export async function getTransactionById(
  transactionId: number,
  signal?: AbortSignal
): Promise<TransactionLookupResponse | null> {
  if (!Number.isSafeInteger(transactionId) || transactionId < 0) {
    throw new TypeError('transaction ID must be a non-negative safe integer');
  }
  const payload = await requestJson(`/api/v1/transactions/${transactionId}`, signal);
  return payload === null ? null : validateTransactionResponse(payload);
}

export async function getTransactionByCoordinate(
  coordinate: Record<keyof TransactionCoordinate, string>,
  signal?: AbortSignal
): Promise<TransactionLookupResponse | null> {
  const parameters = new URLSearchParams(coordinate);
  const payload = await requestJson(
    `/api/v1/transactions/by-coordinate?${parameters.toString()}`,
    signal
  );
  return payload === null ? null : validateTransactionResponse(payload);
}

export async function getPostings(
  kind: PostingKind,
  key: string,
  cursor: string,
  limit: string,
  programInstructionScope: ProgramInstructionScope = 'all',
  signal?: AbortSignal
): Promise<PostingsResponse | null> {
  const parameters = new URLSearchParams({ cursor, limit });
  if (kind === 'program') parameters.set('instruction_scope', programInstructionScope);
  const payload = await requestJson(
    `/api/v1/postings/${kind}/${encodeURIComponent(key)}?${parameters.toString()}`,
    signal
  );
  return payload === null
    ? null
    : validatePostingsResponse(payload, kind, programInstructionScope);
}

export async function getAccountTradingSummary(
  address: string,
  signal?: AbortSignal
): Promise<AccountTradingSummaryResponse | null> {
  const account = requireRequestAddress(address, 'account address');
  const payload = await requestJson(
    `/api/v1/accounts/${encodeURIComponent(account)}/trading-summary`,
    signal
  );
  return payload === null ? null : validateAccountTradingSummary(payload);
}

export async function getAccountProvenTrades(
  address: string,
  query: AccountProvenTradesQuery = {},
  signal?: AbortSignal
): Promise<AccountProvenTradesResponse | null> {
  const account = requireRequestAddress(address, 'account address');
  const parameters = new URLSearchParams();
  appendOptionalAddress(parameters, 'quote_mint', query.quote_mint);
  appendOptionalAddress(parameters, 'program', query.program);
  appendOptionalSafeInteger(parameters, 'time_from', query.time_from);
  appendOptionalSafeInteger(parameters, 'time_to', query.time_to);
  appendOptionalNonNegativeInteger(parameters, 'offset', query.offset);
  appendOptionalPositiveInteger(parameters, 'limit', query.limit);
  if (query.limit !== undefined && query.limit > 200) {
    throw new TypeError('account proven trade limit must not exceed 200');
  }
  validateRequestTimeRange(query.time_from, query.time_to);
  const suffix = parameters.size === 0 ? '' : `?${parameters.toString()}`;
  const payload = await requestJson(
    `/api/v1/accounts/${encodeURIComponent(account)}/trades${suffix}`,
    signal
  );
  return payload === null ? null : validateAccountProvenTrades(payload);
}

export async function getAccountTradingActivity(
  address: string,
  query: AccountTradingActivityQuery = {},
  signal?: AbortSignal
): Promise<AccountTradingActivityResponse | null> {
  const account = requireRequestAddress(address, 'account address');
  const parameters = new URLSearchParams();
  appendOptionalAddress(parameters, 'quote_mint', query.quote_mint);
  appendOptionalAddress(parameters, 'program', query.program);
  if (query.interval !== undefined) {
    const interval = String(query.interval);
    if (!isAccountTradingInterval(interval)) {
      throw new TypeError('account trading interval must be 1h, 4h, 1d, 1w, or positive seconds');
    }
    parameters.set('interval', interval);
  }
  appendOptionalSafeInteger(parameters, 'time_from', query.time_from);
  appendOptionalSafeInteger(parameters, 'time_to', query.time_to);
  appendOptionalPositiveInteger(parameters, 'max_points', query.max_points);
  if (query.max_points !== undefined && query.max_points > 100_000) {
    throw new TypeError('account trading max_points must not exceed 100000');
  }
  validateRequestTimeRange(query.time_from, query.time_to);
  const suffix = parameters.size === 0 ? '' : `?${parameters.toString()}`;
  const payload = await requestJson(
    `/api/v1/accounts/${encodeURIComponent(account)}/trading-activity${suffix}`,
    signal
  );
  return payload === null ? null : validateAccountTradingActivity(payload);
}

export async function getAccountBalanceHistory(
  address: string,
  maxPoints = 1_000,
  signal?: AbortSignal
): Promise<AccountBalanceHistoryResponse | null> {
  const account = requireRequestAddress(address, 'account address');
  if (!Number.isSafeInteger(maxPoints) || maxPoints < 1 || maxPoints > 4_096) {
    throw new TypeError('account balance-history max_points must be from 1 through 4096');
  }
  const payload = await requestJson(
    `/api/v1/accounts/${encodeURIComponent(account)}/balance-history?max_points=${maxPoints}`,
    signal
  );
  return payload === null ? null : validateAccountBalanceHistory(payload, account);
}

function apiUrl(path: string): string {
  return configuredApiBase ? `${configuredApiBase}${path}` : path;
}

async function requestJson(
  path: string,
  signal: AbortSignal | undefined,
  allowNotFound = true
): Promise<unknown | null> {
  let response: Response;
  try {
    response = await fetch(apiUrl(path), {
      method: 'GET',
      headers: { accept: 'application/json' },
      signal
    });
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') throw error;
    throw new SearchApiError('The transaction index service is not reachable.', null, true);
  }

  if (allowNotFound && (response.status === 204 || response.status === 404)) return null;
  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new SearchApiError(
      detail || `The transaction index service returned HTTP ${response.status}.`,
      response.status,
      response.status >= 500
    );
  }

  try {
    return await response.json();
  } catch {
    throw invalidResponse('response body is not valid JSON');
  }
}

async function readErrorDetail(response: Response): Promise<string> {
  try {
    const payload = (await response.json()) as unknown;
    if (isRecord(payload)) {
      if (typeof payload.error === 'string') return payload.error;
      if (typeof payload.message === 'string') return payload.message;
    }
  } catch {
    return '';
  }
  return '';
}

function validateTransactionResponse(value: unknown): TransactionLookupResponse {
  const response = requireRecord(value, 'transaction lookup response');
  const transaction = requireRecord(response.transaction, 'transaction');
  requireInteger(transaction.id, 'transaction ID');
  validateCoordinate(transaction.coordinate);

  const block = requireRecord(transaction.block, 'transaction block');
  requireInteger(block.parent_slot, 'block parent slot');
  requireNullableInteger(block.block_time, 'block time');
  requireNullableInteger(block.block_height, 'block height');
  requireInteger(block.transaction_count, 'block transaction count');

  if (!Array.isArray(transaction.signatures) || !transaction.signatures.every((item) => typeof item === 'string')) {
    throw invalidResponse('transaction signatures are not a string array');
  }
  if (!Array.isArray(transaction.accounts)) {
    throw invalidResponse('transaction accounts are not an array');
  }
  for (const [index, accountValue] of transaction.accounts.entries()) {
    const account = requireRecord(accountValue, `transaction account ${index}`);
    const accountIndex = readInteger(account.account_index, `transaction account ${index} index`);
    if (accountIndex !== index) {
      throw invalidResponse('transaction accounts are not in canonical message order');
    }
    readPositiveInteger(account.registry_id, `transaction account ${index} registry ID`);
    const address = readString(account.address, `transaction account ${index} address`);
    if (!/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(address)) {
      throw invalidResponse(`transaction account ${index} address is not a Solana public key`);
    }
  }
  requireInteger(transaction.flags, 'transaction flags');
  requireString(transaction.source_wire_profile, 'transaction source wire profile');
  requireString(transaction.message_bytes_base64, 'transaction message bytes');
  requireString(transaction.metadata_bytes_base64, 'transaction metadata bytes');
  return response as unknown as TransactionLookupResponse;
}

function validatePostingsResponse(
  value: unknown,
  requestedKind: PostingKind,
  requestedProgramScope: ProgramInstructionScope
): PostingsResponse {
  const response = requireRecord(value, 'postings response');
  if (response.kind !== requestedKind) {
    throw invalidResponse('postings kind does not match the request');
  }
  if (requestedKind === 'program') {
    if (response.instruction_scope !== requestedProgramScope) {
      throw invalidResponse('program instruction scope does not match the request');
    }
  } else if (response.instruction_scope !== undefined) {
    throw invalidResponse('non-program postings include an instruction scope');
  }
  requireString(response.key, 'postings key');
  requireInteger(response.limit, 'postings limit');
  requireInteger(response.registry_id, 'postings registry ID');
  requireInteger(response.flags, 'postings flags');
  requireInteger(response.total, 'postings total');
  requireInteger(response.offset, 'postings offset');
  if (response.next_cursor !== null) requireString(response.next_cursor, 'postings next cursor');
  if (!Array.isArray(response.items)) throw invalidResponse('postings items are not an array');

  for (const itemValue of response.items) {
    const item = requireRecord(itemValue, 'posting item');
    requireInteger(item.transaction_id, 'posting transaction ID');
    validateCoordinate(item.coordinate);
    if (item.first_signature !== null) requireString(item.first_signature, 'posting first signature');
  }
  return response as unknown as PostingsResponse;
}

function validateAccountBalanceHistory(
  value: unknown,
  requestedAddress: string
): AccountBalanceHistoryResponse {
  const response = requireRecord(value, 'account balance history');
  if (response.supported !== true) {
    throw invalidResponse('account balance history is not supported');
  }
  const address = readString(response.address, 'account balance-history address');
  if (address !== requestedAddress) {
    throw invalidResponse('account balance-history address differs from the request');
  }
  const matchingEvents = readInteger(
    response.matching_events,
    'account balance-history event count'
  );
  const sampled = readBoolean(response.sampled, 'account balance-history sampled state');
  if (!Array.isArray(response.items)) {
    throw invalidResponse('account balance-history items are not an array');
  }
  let previousTransactionId: number | null = null;
  const items = response.items.map((value, index) => {
    const item = requireRecord(value, `account balance-history item ${index}`);
    const transactionId = readInteger(
      item.transaction_id,
      `account balance-history item ${index} transaction ID`
    );
    const rawDelta = readSignedRawAmount(
      item.raw_delta,
      `account balance-history item ${index} delta`
    );
    const postRawBalance = readRawAmount(
      item.post_raw_balance,
      `account balance-history item ${index} balance`
    );
    if (previousTransactionId !== null && previousTransactionId >= transactionId) {
      throw invalidResponse('account balance-history items are not strictly transaction ordered');
    }
    previousTransactionId = transactionId;
    return {
      transaction_id: transactionId,
      slot: readInteger(item.slot, `account balance-history item ${index} slot`),
      block_time:
        item.block_time === null
          ? null
          : readInteger(item.block_time, `account balance-history item ${index} block time`),
      raw_delta: rawDelta,
      post_raw_balance: postRawBalance
    };
  });
  if (items.length > matchingEvents || sampled !== (items.length < matchingEvents)) {
    throw invalidResponse('account balance-history sampling state is invalid');
  }
  return {
    supported: true,
    artifact_complete: readBoolean(
      response.artifact_complete,
      'account balance-history artifact completion'
    ),
    address,
    attribution: readString(response.attribution, 'account balance-history attribution'),
    registry_id: readInteger(response.registry_id, 'account balance-history registry ID'),
    matching_events: matchingEvents,
    sampled,
    items
  };
}

function validateAccountTradingSummary(value: unknown): AccountTradingSummaryResponse {
  const response = requireRecord(value, 'account trading summary');
  const contract = validateProvenTraderContract(response, 'account trading summary');
  const totals = validateAccountTraderActivityTotals(
    response.totals,
    'account trading summary totals'
  );
  const hasProvenTrades = readBoolean(
    response.has_proven_trades,
    'account trading summary proven-trade state'
  );
  if (hasProvenTrades !== (totals.trade_count !== 0)) {
    throw invalidResponse('account trading summary proven-trade state differs from its total');
  }
  const firstBlockTime = readOptionalInteger(
    response.first_block_time,
    'account trading summary first block time'
  );
  const lastBlockTime = readOptionalInteger(
    response.last_block_time,
    'account trading summary last block time'
  );
  if (totals.trade_count === 0) {
    if (firstBlockTime !== undefined || lastBlockTime !== undefined) {
      throw invalidResponse('empty account trading summary includes a trade time');
    }
  } else if (
    firstBlockTime === undefined ||
    lastBlockTime === undefined ||
    firstBlockTime > lastBlockTime
  ) {
    throw invalidResponse('account trading summary time range is invalid');
  }
  return {
    ...contract,
    has_proven_trades: hasProvenTrades,
    target_mint: validateRegistryKey(response.target_mint, 'account trading target mint'),
    target_decimals: readTokenDecimals(
      response.target_decimals,
      'account trading target decimals'
    ),
    ...(firstBlockTime === undefined ? {} : { first_block_time: firstBlockTime }),
    ...(lastBlockTime === undefined ? {} : { last_block_time: lastBlockTime }),
    totals
  };
}

function validateAccountProvenTrades(value: unknown): AccountProvenTradesResponse {
  const response = requireRecord(value, 'account proven trades');
  const contract = validateProvenTraderContract(response, 'account proven trades');
  const total = readInteger(response.total, 'account proven trade total');
  const offset = readInteger(response.offset, 'account proven trade offset');
  const limit = readPositiveInteger(response.limit, 'account proven trade limit');
  const hasMatchingProvenTrades = readBoolean(
    response.has_matching_proven_trades,
    'account matching proven-trade state'
  );
  if (hasMatchingProvenTrades !== (total !== 0)) {
    throw invalidResponse('account matching proven-trade state differs from its total');
  }
  if (offset > total) throw invalidResponse('account proven trade offset exceeds its total');
  if (!Array.isArray(response.trades)) {
    throw invalidResponse('account proven trades are not an array');
  }
  const trades = response.trades.map(validateAccountProvenDexTrade);
  const consumed = offset + trades.length;
  if (!Number.isSafeInteger(consumed) || trades.length > limit || consumed > total) {
    throw invalidResponse('account proven trade page bounds are invalid');
  }
  for (const trade of trades) {
    if (
      trade.trader.registry_id !== contract.trader.registry_id ||
      trade.trader.address !== contract.trader.address
    ) {
      throw invalidResponse('account proven trade has a different exact trader');
    }
  }
  const nextOffset = readOptionalInteger(response.next_offset, 'account proven trade next offset');
  if (nextOffset === undefined && consumed < total) {
    throw invalidResponse('account proven trade next offset is missing');
  }
  if (nextOffset !== undefined && (nextOffset !== consumed || nextOffset >= total)) {
    throw invalidResponse('account proven trade next offset is invalid');
  }
  return {
    ...contract,
    has_matching_proven_trades: hasMatchingProvenTrades,
    total,
    offset,
    limit,
    trades,
    ...(nextOffset === undefined ? {} : { next_offset: nextOffset })
  };
}

function validateAccountTradingActivity(value: unknown): AccountTradingActivityResponse {
  const response = requireRecord(value, 'account trading activity');
  const contract = validateProvenTraderContract(response, 'account trading activity');
  const totals = validateAccountTraderActivityTotals(
    response.totals,
    'account trading activity totals'
  );
  const hasMatchingProvenTrades = readBoolean(
    response.has_matching_proven_trades,
    'account trading activity proven-trade state'
  );
  if (hasMatchingProvenTrades !== (totals.trade_count !== 0)) {
    throw invalidResponse('account trading activity state differs from its total');
  }
  const intervalSeconds = readPositiveInteger(
    response.interval_seconds,
    'account trading activity interval'
  );
  const timeFrom = readInteger(response.time_from, 'account trading activity start time');
  const timeTo = readInteger(response.time_to, 'account trading activity end time');
  if (timeFrom > timeTo) throw invalidResponse('account trading activity time range is invalid');
  if (!Array.isArray(response.points)) {
    throw invalidResponse('account trading activity points are not an array');
  }
  const points = response.points.map((point, index) =>
    validateAccountTradingActivityPoint(point, intervalSeconds, index)
  );
  for (let index = 0; index < points.length; index += 1) {
    const point = points[index];
    if (point.end_time <= timeFrom || point.start_time > timeTo) {
      throw invalidResponse('account trading activity point is outside the selected time range');
    }
    if (index !== 0 && points[index - 1].start_time >= point.start_time) {
      throw invalidResponse('account trading activity points are not strictly time ordered');
    }
  }
  validatePointTotalsEqualSeries(points, totals);
  return {
    ...contract,
    has_matching_proven_trades: hasMatchingProvenTrades,
    target_mint: validateRegistryKey(response.target_mint, 'account trading target mint'),
    target_decimals: readTokenDecimals(
      response.target_decimals,
      'account trading target decimals'
    ),
    ...optionalRegistryKeyField(
      'selected_quote_mint',
      response.selected_quote_mint,
      'account trading selected quote mint'
    ),
    ...optionalProgramField(
      'selected_program',
      response.selected_program,
      'account trading selected program',
      'dex'
    ),
    interval_seconds: intervalSeconds,
    time_from: timeFrom,
    time_to: timeTo,
    totals,
    points
  };
}

function validateProvenTraderContract(
  response: Record<string, unknown>,
  label: string
): ProvenTraderContract {
  if (response.supported !== true) throw invalidResponse(`${label} is not supported`);
  if (response.attribution !== 'parser_proven_exact_trader') {
    throw invalidResponse(`${label} attribution is not parser-proven exact trader`);
  }
  if (response.includes_inferred_trades !== false) {
    throw invalidResponse(`${label} includes inferred trades`);
  }
  if (response.includes_protocol_positions !== false) {
    throw invalidResponse(`${label} includes protocol positions`);
  }
  return {
    supported: true,
    artifact_complete: readBoolean(response.artifact_complete, `${label} artifact completion`),
    attribution: 'parser_proven_exact_trader',
    includes_inferred_trades: false,
    includes_protocol_positions: false,
    trader: validateRegistryKey(response.trader, `${label} trader`)
  };
}

function validateAccountTraderActivityTotals(
  value: unknown,
  label: string
): AccountTraderActivityTotals {
  const totals = requireRecord(value, label);
  const tradeCount = readInteger(totals.trade_count, `${label} trade count`);
  const buyCount = readInteger(totals.buy_count, `${label} buy count`);
  const sellCount = readInteger(totals.sell_count, `${label} sell count`);
  if (buyCount + sellCount !== tradeCount) {
    throw invalidResponse(`${label} side counts do not equal the trade count`);
  }
  const targetBoughtRaw = readRawAmount(totals.target_bought_raw, `${label} target bought`);
  const targetSoldRaw = readRawAmount(totals.target_sold_raw, `${label} target sold`);
  if (!Array.isArray(totals.quote_totals)) {
    throw invalidResponse(`${label} quote totals are not an array`);
  }
  const quoteTotals = totals.quote_totals.map((quote, index) =>
    validateAccountTraderQuoteActivity(quote, `${label} quote ${index}`)
  );
  let quoteTradeCount = 0n;
  let quoteBuyCount = 0n;
  let quoteSellCount = 0n;
  let quoteTargetBoughtRaw = 0n;
  let quoteTargetSoldRaw = 0n;
  let previousQuoteId = 0;
  for (const quote of quoteTotals) {
    if (quote.quote_mint.registry_id <= previousQuoteId) {
      throw invalidResponse(`${label} quote totals are not strictly registry ordered`);
    }
    previousQuoteId = quote.quote_mint.registry_id;
    quoteTradeCount += BigInt(quote.trade_count);
    quoteBuyCount += BigInt(quote.buy_count);
    quoteSellCount += BigInt(quote.sell_count);
    quoteTargetBoughtRaw += BigInt(quote.target_bought_raw);
    quoteTargetSoldRaw += BigInt(quote.target_sold_raw);
  }
  if (
    quoteTradeCount !== BigInt(tradeCount) ||
    quoteBuyCount !== BigInt(buyCount) ||
    quoteSellCount !== BigInt(sellCount) ||
    quoteTargetBoughtRaw !== BigInt(targetBoughtRaw) ||
    quoteTargetSoldRaw !== BigInt(targetSoldRaw)
  ) {
    throw invalidResponse(`${label} differs from the exact sum of its quote totals`);
  }
  return {
    trade_count: tradeCount,
    buy_count: buyCount,
    sell_count: sellCount,
    target_bought_raw: targetBoughtRaw,
    target_sold_raw: targetSoldRaw,
    quote_totals: quoteTotals
  };
}

function validateAccountTraderQuoteActivity(
  value: unknown,
  label: string
): AccountTraderQuoteActivity {
  const quote = requireRecord(value, label);
  const tradeCount = readInteger(quote.trade_count, `${label} trade count`);
  const buyCount = readInteger(quote.buy_count, `${label} buy count`);
  const sellCount = readInteger(quote.sell_count, `${label} sell count`);
  if (tradeCount === 0 || buyCount + sellCount !== tradeCount) {
    throw invalidResponse(`${label} trade counts are invalid`);
  }
  return {
    quote_mint: validateRegistryKey(quote.quote_mint, `${label} mint`),
    quote_decimals: readTokenDecimals(quote.quote_decimals, `${label} decimals`),
    trade_count: tradeCount,
    buy_count: buyCount,
    sell_count: sellCount,
    target_bought_raw: readRawAmount(quote.target_bought_raw, `${label} target bought`),
    target_sold_raw: readRawAmount(quote.target_sold_raw, `${label} target sold`),
    quote_spent_on_buys_raw: readRawAmount(
      quote.quote_spent_on_buys_raw,
      `${label} quote spent on buys`
    ),
    quote_received_from_sells_raw: readRawAmount(
      quote.quote_received_from_sells_raw,
      `${label} quote received from sells`
    )
  };
}

function validateAccountTradingActivityPoint(
  value: unknown,
  expectedInterval: number,
  index: number
): AccountTradingActivityPoint {
  const point = requireRecord(value, `account trading activity point ${index}`);
  const intervalSeconds = readPositiveInteger(
    point.interval_seconds,
    `account trading activity point ${index} interval`
  );
  if (intervalSeconds !== expectedInterval) {
    throw invalidResponse(`account trading activity point ${index} interval differs`);
  }
  const startTime = readInteger(
    point.start_time,
    `account trading activity point ${index} start time`
  );
  const endTime = readInteger(point.end_time, `account trading activity point ${index} end time`);
  const expectedEndTime = startTime + intervalSeconds;
  if (!Number.isSafeInteger(expectedEndTime) || expectedEndTime !== endTime) {
    throw invalidResponse(`account trading activity point ${index} end time is invalid`);
  }
  const totals = validateAccountTraderActivityTotals(
    point.totals,
    `account trading activity point ${index} totals`
  );
  if (totals.trade_count === 0) {
    throw invalidResponse(`account trading activity point ${index} is empty`);
  }
  return {
    interval_seconds: intervalSeconds,
    start_time: startTime,
    end_time: endTime,
    totals
  };
}

function validatePointTotalsEqualSeries(
  points: AccountTradingActivityPoint[],
  series: AccountTraderActivityTotals
): void {
  let tradeCount = 0n;
  let buyCount = 0n;
  let sellCount = 0n;
  let targetBoughtRaw = 0n;
  let targetSoldRaw = 0n;
  const quoteTotals = new Map<
    number,
    {
      address: string;
      decimals: number;
      tradeCount: bigint;
      buyCount: bigint;
      sellCount: bigint;
      targetBoughtRaw: bigint;
      targetSoldRaw: bigint;
      quoteSpentOnBuysRaw: bigint;
      quoteReceivedFromSellsRaw: bigint;
    }
  >();
  for (const point of points) {
    tradeCount += BigInt(point.totals.trade_count);
    buyCount += BigInt(point.totals.buy_count);
    sellCount += BigInt(point.totals.sell_count);
    targetBoughtRaw += BigInt(point.totals.target_bought_raw);
    targetSoldRaw += BigInt(point.totals.target_sold_raw);
    for (const quote of point.totals.quote_totals) {
      const existing = quoteTotals.get(quote.quote_mint.registry_id);
      if (
        existing !== undefined &&
        (existing.address !== quote.quote_mint.address || existing.decimals !== quote.quote_decimals)
      ) {
        throw invalidResponse('account trading activity quote identity changes between points');
      }
      const total = existing ?? {
        address: quote.quote_mint.address,
        decimals: quote.quote_decimals,
        tradeCount: 0n,
        buyCount: 0n,
        sellCount: 0n,
        targetBoughtRaw: 0n,
        targetSoldRaw: 0n,
        quoteSpentOnBuysRaw: 0n,
        quoteReceivedFromSellsRaw: 0n
      };
      total.tradeCount += BigInt(quote.trade_count);
      total.buyCount += BigInt(quote.buy_count);
      total.sellCount += BigInt(quote.sell_count);
      total.targetBoughtRaw += BigInt(quote.target_bought_raw);
      total.targetSoldRaw += BigInt(quote.target_sold_raw);
      total.quoteSpentOnBuysRaw += BigInt(quote.quote_spent_on_buys_raw);
      total.quoteReceivedFromSellsRaw += BigInt(quote.quote_received_from_sells_raw);
      quoteTotals.set(quote.quote_mint.registry_id, total);
    }
  }
  if (
    tradeCount !== BigInt(series.trade_count) ||
    buyCount !== BigInt(series.buy_count) ||
    sellCount !== BigInt(series.sell_count) ||
    targetBoughtRaw !== BigInt(series.target_bought_raw) ||
    targetSoldRaw !== BigInt(series.target_sold_raw) ||
    quoteTotals.size !== series.quote_totals.length
  ) {
    throw invalidResponse('account trading activity points differ from the exact series totals');
  }
  for (const quote of series.quote_totals) {
    const total = quoteTotals.get(quote.quote_mint.registry_id);
    if (
      total === undefined ||
      total.address !== quote.quote_mint.address ||
      total.decimals !== quote.quote_decimals ||
      total.tradeCount !== BigInt(quote.trade_count) ||
      total.buyCount !== BigInt(quote.buy_count) ||
      total.sellCount !== BigInt(quote.sell_count) ||
      total.targetBoughtRaw !== BigInt(quote.target_bought_raw) ||
      total.targetSoldRaw !== BigInt(quote.target_sold_raw) ||
      total.quoteSpentOnBuysRaw !== BigInt(quote.quote_spent_on_buys_raw) ||
      total.quoteReceivedFromSellsRaw !== BigInt(quote.quote_received_from_sells_raw)
    ) {
      throw invalidResponse('account trading activity quote points differ from the exact totals');
    }
  }
}

function validateAccountProvenDexTrade(value: unknown): AccountProvenDexTrade {
  const trade = requireRecord(value, 'account proven DEX trade');
  const transaction = requireRecord(trade.transaction, 'account proven trade transaction');
  const instruction = requireRecord(trade.instruction, 'account proven trade instruction');
  const side = readString(trade.side, 'account proven trade side');
  if (side !== 'buy' && side !== 'sell') {
    throw invalidResponse('account proven trade side is invalid');
  }
  const program = validateProgramView(trade.program, 'account proven trade program');
  if (program.role !== 'dex') throw invalidResponse('account proven trade program is not a DEX');
  const venue = validateRegistryKey(trade.venue, 'account proven trade venue');
  if (venue.registry_id !== program.registry_id || venue.address !== program.address) {
    throw invalidResponse('account proven trade venue differs from its DEX program');
  }
  const evidence = trade.evidence;
  if (!Array.isArray(evidence) || evidence.some((item) => typeof item !== 'string')) {
    throw invalidResponse('account proven trade evidence is not a string array');
  }
  const price = validateExactTradePrice(trade.price);
  const targetAmountRaw = readRawAmount(trade.target_amount_raw, 'account proven target amount');
  const targetAmountScaledUiRaw = readRawAmount(
    trade.target_amount_scaled_ui_raw,
    'account proven Scaled UI target amount'
  );
  const quoteAmountRaw = readRawAmount(trade.quote_amount_raw, 'account proven quote amount');
  if (targetAmountRaw === '0' || targetAmountScaledUiRaw === '0' || quoteAmountRaw === '0') {
    throw invalidResponse('account proven trade has a zero executed amount');
  }
  return {
    trade_id: readInteger(trade.trade_id, 'account proven trade ID'),
    transaction: {
      transaction_id: readInteger(transaction.transaction_id, 'account trade transaction ID'),
      source_epoch: readInteger(transaction.source_epoch, 'account trade source epoch'),
      slot: readInteger(transaction.slot, 'account trade slot'),
      source_block_id: readInteger(transaction.source_block_id, 'account trade source block ID'),
      tx_index: readInteger(transaction.tx_index, 'account trade transaction index')
    },
    block_time: readInteger(trade.block_time, 'account proven trade block time'),
    instruction: {
      outer_index: readInteger(instruction.outer_index, 'account trade outer index'),
      ...optionalIntegerField(
        'inner_index',
        instruction.inner_index,
        'account trade inner index'
      ),
      stack_height: readInteger(instruction.stack_height, 'account trade stack height')
    },
    instruction_kind_id: readPositiveInteger(
      trade.instruction_kind_id,
      'account trade instruction kind ID'
    ),
    instruction_kind: readString(trade.instruction_kind, 'account trade instruction kind'),
    instruction_discriminator: readString(
      trade.instruction_discriminator,
      'account trade instruction discriminator'
    ),
    program,
    venue,
    ...optionalProgramField('router', trade.router, 'account proven trade router', 'router'),
    ...optionalRegistryKeyField('pool', trade.pool, 'account proven trade pool'),
    trader: validateRegistryKey(trade.trader, 'account proven trade trader'),
    ...optionalRegistryKeyField(
      'user_source',
      trade.user_source,
      'account proven trade user source'
    ),
    ...optionalRegistryKeyField(
      'user_destination',
      trade.user_destination,
      'account proven trade user destination'
    ),
    target_mint: validateRegistryKey(trade.target_mint, 'account proven trade target mint'),
    quote_mint: validateRegistryKey(trade.quote_mint, 'account proven trade quote mint'),
    side,
    target_amount_raw: targetAmountRaw,
    target_amount_scaled_ui_raw: targetAmountScaledUiRaw,
    quote_amount_raw: quoteAmountRaw,
    target_decimals: readTokenDecimals(
      trade.target_decimals,
      'account proven trade target decimals'
    ),
    quote_decimals: readTokenDecimals(
      trade.quote_decimals,
      'account proven trade quote decimals'
    ),
    price,
    fee_amount_raw: readRawAmount(trade.fee_amount_raw, 'account proven trade fee amount'),
    ...optionalRegistryKeyField('fee_mint', trade.fee_mint, 'account proven trade fee mint'),
    input_transfer_count: readInteger(
      trade.input_transfer_count,
      'account proven trade input transfer count'
    ),
    output_transfer_count: readInteger(
      trade.output_transfer_count,
      'account proven trade output transfer count'
    ),
    evidence_flags: readInteger(trade.evidence_flags, 'account proven trade evidence flags'),
    evidence: [...evidence] as string[]
  };
}

function validateExactTradePrice(value: unknown): ExactTradePrice {
  const price = requireRecord(value, 'account proven trade price');
  const numerator = readRawAmount(price.numerator, 'account proven price numerator');
  const denominator = readRawAmount(price.denominator, 'account proven price denominator');
  if (numerator === '0' || denominator === '0') {
    throw invalidResponse('account proven price ratio has a zero term');
  }
  const chartDisplay = readOptionalFiniteNumber(
    price.chart_display,
    'account proven price chart display'
  );
  if (chartDisplay !== undefined && chartDisplay < 0) {
    throw invalidResponse('account proven price chart display is negative');
  }
  const unscaledChartDisplay = readOptionalFiniteNumber(
    price.unscaled_chart_display,
    'account proven unscaled price chart display'
  );
  if (unscaledChartDisplay !== undefined && unscaledChartDisplay < 0) {
    throw invalidResponse('account proven unscaled price chart display is negative');
  }
  const targetMultiplier = readString(
    price.target_multiplier,
    'account proven price target multiplier'
  );
  const targetMultiplierBits = requireCanonicalHex(
    price.target_multiplier_bits,
    16,
    'account proven price target multiplier bits'
  );
  validateMultiplierBits(
    targetMultiplier,
    targetMultiplierBits,
    'account proven price target multiplier'
  );
  return {
    numerator,
    denominator,
    decimal: requireDecimalString(price.decimal, 'account proven price decimal'),
    ...(chartDisplay === undefined ? {} : { chart_display: chartDisplay }),
    target_multiplier: targetMultiplier,
    target_multiplier_bits: targetMultiplierBits,
    scaled_ui_config_id: readInteger(
      price.scaled_ui_config_id,
      'account proven price Scaled UI configuration ID'
    ),
    unscaled_decimal: requireDecimalString(
      price.unscaled_decimal,
      'account proven unscaled price decimal'
    ),
    ...(unscaledChartDisplay === undefined
      ? {}
      : { unscaled_chart_display: unscaledChartDisplay })
  };
}

function validateRegistryKey(value: unknown, label: string): RegistryKeyView {
  const key = requireRecord(value, label);
  const registryId = readPositiveInteger(key.registry_id, `${label} registry ID`);
  const address = readString(key.address, `${label} address`);
  if (address.length === 0) throw invalidResponse(`${label} address is empty`);
  return { registry_id: registryId, address };
}

function validateProgramView(value: unknown, label: string): ProvenDexProgramView {
  const program = requireRecord(value, label);
  const key = validateRegistryKey(program, label);
  const role = readString(program.role, `${label} role`);
  if (role !== 'dex' && role !== 'router') throw invalidResponse(`${label} role is invalid`);
  return {
    ...key,
    name: readString(program.name, `${label} name`),
    role
  };
}

function optionalRegistryKeyField<K extends string>(
  key: K,
  value: unknown,
  label: string
): Partial<Record<K, RegistryKeyView>> {
  return value === undefined
    ? {}
    : ({ [key]: validateRegistryKey(value, label) } as Record<K, RegistryKeyView>);
}

function optionalProgramField<K extends string>(
  key: K,
  value: unknown,
  label: string,
  expectedRole: 'dex' | 'router'
): Partial<Record<K, ProvenDexProgramView>> {
  if (value === undefined) return {};
  const program = validateProgramView(value, label);
  if (program.role !== expectedRole) throw invalidResponse(`${label} role is invalid`);
  return { [key]: program } as Record<K, ProvenDexProgramView>;
}

function optionalIntegerField<K extends string>(
  key: K,
  value: unknown,
  label: string
): Partial<Record<K, number>> {
  const integer = readOptionalInteger(value, label);
  return integer === undefined ? {} : ({ [key]: integer } as Record<K, number>);
}

function requireRequestAddress(value: string, label: string): string {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new TypeError(`${label} must not be empty`);
  }
  return value.trim();
}

function appendOptionalAddress(
  parameters: URLSearchParams,
  name: string,
  value: string | undefined
): void {
  if (value !== undefined) parameters.set(name, requireRequestAddress(value, name));
}

function appendOptionalSafeInteger(
  parameters: URLSearchParams,
  name: string,
  value: number | undefined
): void {
  if (value === undefined) return;
  if (!Number.isSafeInteger(value)) throw new TypeError(`${name} must be a safe integer`);
  parameters.set(name, String(value));
}

function appendOptionalNonNegativeInteger(
  parameters: URLSearchParams,
  name: string,
  value: number | undefined
): void {
  if (value !== undefined && value < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
  appendOptionalSafeInteger(parameters, name, value);
}

function appendOptionalPositiveInteger(
  parameters: URLSearchParams,
  name: string,
  value: number | undefined
): void {
  if (value !== undefined && value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  appendOptionalSafeInteger(parameters, name, value);
}

function validateRequestTimeRange(timeFrom: number | undefined, timeTo: number | undefined): void {
  if (timeFrom !== undefined && timeTo !== undefined && timeFrom > timeTo) {
    throw new TypeError('time_from must not be after time_to');
  }
}

function isAccountTradingInterval(value: string): value is AccountTradingInterval {
  if (value === '1h' || value === '4h' || value === '1d' || value === '1w') return true;
  if (!/^[1-9]\d*$/.test(value)) return false;
  return BigInt(value) <= BigInt(Number.MAX_SAFE_INTEGER);
}

function readInteger(value: unknown, label: string): number {
  requireInteger(value, label);
  return value;
}

function readPositiveInteger(value: unknown, label: string): number {
  const integer = readInteger(value, label);
  if (integer === 0) throw invalidResponse(`${label} is not a positive safe integer`);
  return integer;
}

function readOptionalInteger(value: unknown, label: string): number | undefined {
  return value === undefined ? undefined : readInteger(value, label);
}

function readBoolean(value: unknown, label: string): boolean {
  requireBoolean(value, label);
  return value;
}

function readString(value: unknown, label: string): string {
  requireString(value, label);
  return value;
}

function readTokenDecimals(value: unknown, label: string): number {
  const decimals = readInteger(value, label);
  if (decimals > 255) throw invalidResponse(`${label} exceeds an unsigned byte`);
  return decimals;
}

function readRawAmount(value: unknown, label: string): string {
  requireString(value, label);
  if (!/^(?:0|[1-9]\d*)$/.test(value)) {
    throw invalidResponse(`${label} is not a canonical unsigned decimal integer`);
  }
  if (BigInt(value) > (1n << 128n) - 1n) {
    throw invalidResponse(`${label} exceeds an unsigned 128-bit integer`);
  }
  return value;
}

function readSignedRawAmount(value: unknown, label: string): string {
  requireString(value, label);
  if (!/^(?:0|-[1-9]\d*|[1-9]\d*)$/.test(value)) {
    throw invalidResponse(`${label} is not a canonical signed decimal integer`);
  }
  const parsed = BigInt(value);
  if (parsed < -(1n << 127n) || parsed > (1n << 127n) - 1n) {
    throw invalidResponse(`${label} exceeds a signed 128-bit integer`);
  }
  return value;
}

function requireDecimalString(value: unknown, label: string): string {
  requireString(value, label);
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(value)) {
    throw invalidResponse(`${label} is not a canonical unsigned decimal number`);
  }
  return value;
}

function requireCanonicalHex(value: unknown, length: number, label: string): string {
  requireString(value, label);
  if (value.length !== length || !/^[0-9a-f]+$/.test(value)) {
    throw invalidResponse(`${label} is not canonical lowercase hexadecimal`);
  }
  return value;
}

function validateMultiplierBits(decimal: string, bits: string, label: string): void {
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?(?:e[+-]?\d+)?$/.test(decimal)) {
    throw invalidResponse(`${label} decimal is not canonical`);
  }
  const multiplier = Number(decimal);
  if (!Number.isFinite(multiplier) || multiplier < 2.2250738585072014e-308) {
    throw invalidResponse(`${label} is not a positive normal number`);
  }
  const buffer = new ArrayBuffer(8);
  const view = new DataView(buffer);
  view.setFloat64(0, multiplier, false);
  const parsedBits = view.getBigUint64(0, false).toString(16).padStart(16, '0');
  if (parsedBits !== bits) throw invalidResponse(`${label} decimal differs from its exact bits`);
}

function readOptionalFiniteNumber(value: unknown, label: string): number | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw invalidResponse(`${label} is not a finite number`);
  }
  return value;
}

function validateCoordinate(value: unknown): TransactionCoordinate {
  const coordinate = requireRecord(value, 'transaction coordinate');
  requireInteger(coordinate.epoch, 'coordinate epoch');
  requireInteger(coordinate.slot, 'coordinate slot');
  requireInteger(coordinate.source_block_id, 'coordinate source block ID');
  requireInteger(coordinate.tx_index, 'coordinate transaction index');
  return coordinate as unknown as TransactionCoordinate;
}

function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) throw invalidResponse(`${label} is not an object`);
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requireInteger(value: unknown, label: string): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw invalidResponse(`${label} is not a non-negative safe integer`);
  }
}

function requireNullableInteger(value: unknown, label: string): void {
  if (value !== null) requireInteger(value, label);
}

function requireString(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string') throw invalidResponse(`${label} is not a string`);
}

function requireBoolean(value: unknown, label: string): asserts value is boolean {
  if (typeof value !== 'boolean') throw invalidResponse(`${label} is not a boolean`);
}

function requireSha256(value: unknown, label: string): void {
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/i.test(value)) {
    throw invalidResponse(`${label} is not a hexadecimal SHA-256 digest`);
  }
}

function invalidResponse(detail: string): SearchApiError {
  return new SearchApiError(`The transaction index service returned an invalid response: ${detail}.`, null, false);
}
