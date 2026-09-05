import { env } from '$env/dynamic/public';
import { SearchApiError } from '$lib/search-api';

export type MarketSide = 'buy' | 'sell';
export type MarketInterval = '60' | '1h' | '4h' | '1d' | '1w';
export type MarketPriceResolution = 'slot' | MarketInterval;

export interface MarketProgram {
  registry_id: number;
  address: string;
  name: string;
  role: 'dex' | 'router';
}

export interface MarketMint {
  mint: string;
  mint_id: number;
  decimals: number;
  is_target: boolean;
  direct_usd_quote: boolean;
  trade_count: number;
  metadata_available: boolean;
  decimals_verified_onchain: boolean;
  token_program: 'legacy' | 'token2022' | null;
  metadata_source: 'token2022' | 'metaplex' | 'official_project_site' | null;
  metadata_source_uri: string | null;
  name: string | null;
  symbol: string | null;
  uri: string | null;
  warnings: string[];
}

export interface MarketProgramSummary {
  program: MarketProgram;
  trade_count: number;
  trade_count_24h: number;
  first_block_time: number;
  last_block_time: number;
  target_volume_raw: string;
  target_volume_24h_raw: string;
  pair_count: number;
  primary_pool_count: number;
  routed_trade_count: number;
}

export interface ExactPrice {
  numerator: string;
  denominator: string;
  display: number;
  target_multiplier: string;
  target_multiplier_bits: string;
  scaled_ui_config_id: number;
  unscaled_decimal: string;
  unscaled_display: number;
}

export interface MarketScaledUiAmountMultiplier {
  bits: string;
  decimal: string;
}

export interface MarketScaledUiAmountCoordinate {
  transaction_id: number;
  source_epoch: number;
  slot: number;
  block_time: number;
  source_block_id: number;
  tx_index: number;
  outer_index: number;
  inner_index?: number;
  stack_height: number;
  batch_index?: number;
}

export type MarketScaledUiAmountEventKind = 'initialize' | 'update_multiplier';

export interface MarketScaledUiAmountEvent {
  config_id: number;
  coordinate: MarketScaledUiAmountCoordinate;
  signature: string;
  target_mint_id: number;
  kind: MarketScaledUiAmountEventKind;
  multiplier: MarketScaledUiAmountMultiplier;
  effective_timestamp: number;
  authority_registry_id?: number;
  configured_authority_hex?: string;
  commit_proven: boolean;
}

export interface MarketScaledUiHistory {
  enabled: boolean;
  processor_semantics: 'deployed_legacy_no_pending_promotion_v1';
  mint_anchor_slot: number;
  mint_anchor_signature: string;
  events: MarketScaledUiAmountEvent[];
}

export function resolveMarketScaledUiEventAt(
  history: MarketScaledUiHistory,
  unixTimestamp: number
): MarketScaledUiAmountEvent | null {
  if (!Number.isSafeInteger(unixTimestamp) || unixTimestamp < 0) {
    throw new RangeError('Scaled UI resolution timestamp must be a non-negative safe integer');
  }
  if (!history.enabled || history.events.length === 0) return null;

  const initialize = history.events[0];
  if (initialize.coordinate.block_time > unixTimestamp) return null;

  let baseline = initialize;
  let pending = initialize;
  for (const event of history.events.slice(1)) {
    if (event.coordinate.block_time > unixTimestamp) break;
    const effectiveTimestamp = Math.max(0, event.effective_timestamp);
    if (event.coordinate.block_time >= effectiveTimestamp) baseline = event;
    pending = event;
  }

  return unixTimestamp >= Math.max(0, pending.effective_timestamp) ? pending : baseline;
}

export interface MarketSummary {
  target_mint: string;
  target_decimals: number;
  trade_count: number;
  pair_count: number;
  program_count: number;
  first_block_time: number | null;
  last_block_time: number | null;
  trades_24h: number;
  target_volume_raw: string;
  target_volume_24h_raw: string;
}

export interface MarketPair {
  quote_mint: string;
  quote_mint_id: number;
  quote_decimals: number;
  trade_count: number;
  program_count: number;
  first_block_time: number | null;
  last_block_time: number | null;
  target_volume_raw: string;
  quote_volume_raw: string;
  trades_24h: number;
  target_volume_24h_raw: string;
  quote_volume_24h_raw: string;
  latest_price: ExactPrice | null;
  direct_usd: boolean;
}

export interface MarketCandle {
  start_time: number;
  end_time: number;
  open: ExactPrice;
  high: ExactPrice;
  low: ExactPrice;
  close: ExactPrice;
  trade_count: number;
  target_volume_raw: string;
  quote_volume_raw: string;
}

export interface MarketSlotCandle {
  slot: number;
  block_time: number;
  open: ExactPrice;
  high: ExactPrice;
  low: ExactPrice;
  close: ExactPrice;
  trade_count: number;
  buy_count: number;
  sell_count: number;
  target_volume_raw: string;
  quote_volume_raw: string;
}

export interface MarketDexProgramVolume {
  program: MarketProgram;
  trade_count: number;
  buy_count: number;
  sell_count: number;
  target_volume_raw: string;
  routed_trade_count: number;
  routed_target_volume_raw: string;
  router_count: number;
}

export interface MarketProgramVolumePoint {
  interval_seconds: number;
  start_time: number;
  end_time: number;
  trade_count: number;
  target_volume_raw: string;
  programs: MarketDexProgramVolume[];
}

export interface MarketProgramVolumeSeries {
  target_mint: string;
  target_decimals: number;
  selected_quote_mint: string | null;
  interval_seconds: number;
  time_from: number;
  time_to: number;
  attribution: 'executed_dex_program';
  points: MarketProgramVolumePoint[];
}

export interface MarketTrade {
  trade_id: number;
  transaction_id: number;
  signature: string | null;
  coordinate: {
    epoch: number;
    slot: number;
    source_block_id: number;
    tx_index: number;
  };
  block_time: number | null;
  outer_index: number;
  inner_index: number | null;
  stack_height: number;
  program: MarketProgram;
  /** Compatibility address for older views. */
  venue: string;
  router: MarketProgram | null;
  pool: string | null;
  trader: string | null;
  side: MarketSide;
  target_amount_raw: string;
  target_amount_scaled_ui_raw: string;
  quote_amount_raw: string;
  target_decimals: number;
  quote_decimals: number;
  quote_mint: string;
  price: ExactPrice;
  evidence_flags: number;
}

export interface MarketTradesPage {
  items: MarketTrade[];
  offset: number;
  limit: number;
  total: number;
}

const configuredApiBase = (env.PUBLIC_SPYX_API_BASE_URL ?? '').trim().replace(/\/+$/, '');

export async function getMarketSummary(signal?: AbortSignal): Promise<MarketSummary> {
  return validateSummary(await requestMarketJson('/api/v1/market/summary', signal));
}

export async function getMarketScaledUiHistory(
  signal?: AbortSignal
): Promise<MarketScaledUiHistory> {
  return validateScaledUiHistory(
    await requestMarketJson('/api/v1/market/scaled-ui-amount', signal)
  );
}

export async function getMarketPairs(signal?: AbortSignal): Promise<MarketPair[]> {
  const value = requireRecord(await requestMarketJson('/api/v1/market/pairs', signal), 'market pairs');
  if (!Array.isArray(value.items)) throw invalidResponse('market pair items are not an array');
  return value.items.map(validatePair);
}

export async function getMarketMints(signal?: AbortSignal): Promise<MarketMint[]> {
  const value = requireRecord(await requestMarketJson('/api/v1/market/mints', signal), 'market mints');
  if (!Array.isArray(value.items)) throw invalidResponse('market mint items are not an array');
  return value.items.map(validateMint);
}

export async function getMarketPrograms(signal?: AbortSignal): Promise<MarketProgramSummary[]> {
  const value = requireRecord(
    await requestMarketJson('/api/v1/market/programs', signal),
    'market programs'
  );
  if (!Array.isArray(value.items)) throw invalidResponse('market program items are not an array');
  return value.items.map(validateProgramSummary);
}

export async function getMarketCandles(
  quoteMint: string,
  interval: MarketInterval,
  signal?: AbortSignal,
  program?: string
): Promise<MarketCandle[]> {
  const query = new URLSearchParams({
    quote_mint: quoteMint,
    interval,
    max_points: interval === '60' ? '1440' : '720'
  });
  if (program) query.set('program', program);
  const value = requireRecord(
    await requestMarketJson(`/api/v1/market/candles?${query.toString()}`, signal),
    'market candles'
  );
  if (!Array.isArray(value.items)) throw invalidResponse('market candle items are not an array');
  return value.items.map(validateCandle);
}

export async function getMarketSlotCandles(
  quoteMint: string,
  signal?: AbortSignal,
  program?: string
): Promise<MarketSlotCandle[]> {
  const query = new URLSearchParams({ quote_mint: quoteMint, max_points: '1000' });
  if (program) query.set('program', program);
  const value = requireRecord(
    await requestMarketJson(`/api/v1/market/slot-candles?${query.toString()}`, signal),
    'market slot candles'
  );
  if (!Array.isArray(value.items)) throw invalidResponse('market slot candle items are not an array');
  return value.items.map(validateSlotCandle);
}

export async function getMarketProgramVolume(
  interval: MarketInterval,
  timeFrom: number,
  timeTo: number,
  signal?: AbortSignal,
  quoteMint?: string
): Promise<MarketProgramVolumeSeries> {
  const query = new URLSearchParams({
    interval,
    time_from: String(timeFrom),
    time_to: String(timeTo),
    max_points: '720'
  });
  if (quoteMint) query.set('quote_mint', quoteMint);
  return validateProgramVolumeSeries(
    await requestMarketJson(`/api/v1/market/program-volume?${query.toString()}`, signal)
  );
}

export async function getMarketTrades(
  quoteMint: string,
  limit = 50,
  signal?: AbortSignal
): Promise<MarketTradesPage> {
  const query = new URLSearchParams({
    quote_mint: quoteMint,
    offset: '0',
    limit: String(limit)
  });
  const value = requireRecord(
    await requestMarketJson(`/api/v1/market/trades?${query.toString()}`, signal),
    'market trades'
  );
  if (!Array.isArray(value.trades)) throw invalidResponse('market trade items are not an array');
  return {
    items: value.trades.map(validateTrade),
    offset: requireInteger(value.offset, 'market trade offset'),
    limit: requireInteger(value.limit, 'market trade limit'),
    total: requireInteger(value.total, 'market trade total')
  };
}

async function requestMarketJson(path: string, signal?: AbortSignal): Promise<unknown> {
  let response: Response;
  try {
    response = await fetch(configuredApiBase ? `${configuredApiBase}${path}` : path, {
      method: 'GET',
      headers: { accept: 'application/json' },
      signal
    });
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') throw error;
    throw new SearchApiError('The SPYx market API is not reachable.', null, true);
  }
  if (!response.ok) {
    throw new SearchApiError(
      await readError(response, `The SPYx market API returned HTTP ${response.status}.`),
      response.status,
      response.status >= 500
    );
  }
  try {
    return await response.json();
  } catch {
    throw invalidResponse('market response body is not valid JSON');
  }
}

function validateSummary(value: unknown): MarketSummary {
  const row = requireRecord(value, 'market summary');
  const targetMint = requireRegistryKey(row.target_mint, 'market target mint');
  return {
    target_mint: targetMint.address,
    target_decimals: requireInteger(row.target_decimals, 'market target decimals'),
    trade_count: requireInteger(row.trade_count, 'market trade count'),
    pair_count: requireInteger(row.pair_count, 'market pair count'),
    program_count: requireInteger(row.program_count ?? row.venue_count, 'market program count'),
    first_block_time: requireNullableInteger(row.first_block_time, 'market first block time'),
    last_block_time: requireNullableInteger(
      row.dataset_latest_block_time,
      'market latest block time'
    ),
    trades_24h: requireInteger(row.trade_count_24h, 'market 24-hour trade count'),
    target_volume_raw: requireUnsignedString(row.target_volume_raw, 'market target volume'),
    target_volume_24h_raw: requireUnsignedString(
      row.target_volume_24h_raw,
      'market 24-hour target volume'
    )
  };
}

function validateScaledUiHistory(value: unknown): MarketScaledUiHistory {
  const row = requireRecord(value, 'market Scaled UI history');
  const enabled = requireBoolean(row.enabled, 'Scaled UI enabled state');
  const processorSemantics = requireString(
    row.processor_semantics,
    'Scaled UI processor semantics'
  );
  if (processorSemantics !== 'deployed_legacy_no_pending_promotion_v1') {
    throw invalidResponse('Scaled UI processor semantics are unsupported');
  }
  const mintAnchorSlot = requirePositiveInteger(row.mint_anchor_slot, 'Scaled UI mint anchor slot');
  const mintAnchorSignature = requireBase58Bytes(
    row.mint_anchor_signature,
    64,
    'Scaled UI mint anchor signature'
  );
  if (!Array.isArray(row.events)) throw invalidResponse('Scaled UI events are not an array');
  const events = row.events.map(validateScaledUiEvent);
  if (enabled !== (events.length !== 0)) {
    throw invalidResponse('Scaled UI enabled state differs from its event history');
  }
  if (events.length !== 0) {
    const initialize = events[0];
    if (
      initialize.kind !== 'initialize' ||
      initialize.coordinate.slot !== mintAnchorSlot ||
      initialize.signature !== mintAnchorSignature
    ) {
      throw invalidResponse('Scaled UI initialize event differs from the mint anchor');
    }
    const targetMintId = initialize.target_mint_id;
    for (let index = 0; index < events.length; index += 1) {
      const event = events[index];
      if (event.config_id !== index + 1) {
        throw invalidResponse('Scaled UI configuration IDs are not sequential and one-based');
      }
      if (event.target_mint_id !== targetMintId) {
        throw invalidResponse('Scaled UI events target different mint IDs');
      }
      if (index !== 0 && event.kind !== 'update_multiplier') {
        throw invalidResponse('Scaled UI history contains more than one initialize event');
      }
      if (index === 0) continue;
      const previous = events[index - 1];
      if (compareNumberTuples(canonicalScaledUiKey(previous), canonicalScaledUiKey(event)) >= 0) {
        throw invalidResponse('Scaled UI events are not in canonical transaction order');
      }
      if (compareNumberTuples(sourceScaledUiKey(previous), sourceScaledUiKey(event)) >= 0) {
        throw invalidResponse('Scaled UI events are not in canonical source order');
      }
      if (previous.coordinate.transaction_id === event.coordinate.transaction_id) {
        if (
          previous.coordinate.source_epoch !== event.coordinate.source_epoch ||
          previous.coordinate.slot !== event.coordinate.slot ||
          previous.coordinate.block_time !== event.coordinate.block_time ||
          previous.coordinate.source_block_id !== event.coordinate.source_block_id ||
          previous.coordinate.tx_index !== event.coordinate.tx_index ||
          previous.signature !== event.signature
        ) {
          throw invalidResponse('Scaled UI events disagree about one transaction');
        }
      }
    }
  }
  return {
    enabled,
    processor_semantics: processorSemantics,
    mint_anchor_slot: mintAnchorSlot,
    mint_anchor_signature: mintAnchorSignature,
    events
  };
}

function validateScaledUiEvent(value: unknown, index: number): MarketScaledUiAmountEvent {
  const label = `Scaled UI event ${index}`;
  const row = requireRecord(value, label);
  const coordinate = validateScaledUiCoordinate(row.coordinate, label);
  const kind = requireString(row.kind, `${label} kind`);
  if (kind !== 'initialize' && kind !== 'update_multiplier') {
    throw invalidResponse(`${label} kind is invalid`);
  }
  const effectiveTimestamp = requireSignedInteger(
    row.effective_timestamp,
    `${label} effective timestamp`
  );
  const authorityRegistryId =
    row.authority_registry_id === undefined
      ? undefined
      : requirePositiveInteger(row.authority_registry_id, `${label} authority registry ID`);
  const configuredAuthorityHex =
    row.configured_authority_hex === undefined
      ? undefined
      : requireCanonicalHex(row.configured_authority_hex, 64, `${label} configured authority`);
  if (configuredAuthorityHex !== undefined && /^0+$/.test(configuredAuthorityHex)) {
    throw invalidResponse(`${label} configured authority is null`);
  }
  if (kind === 'initialize' && effectiveTimestamp !== 0) {
    throw invalidResponse(`${label} initialize timestamp is not zero`);
  }
  if (kind === 'update_multiplier') {
    if (authorityRegistryId === undefined) {
      throw invalidResponse(`${label} update authority is missing`);
    }
    if (configuredAuthorityHex !== undefined) {
      throw invalidResponse(`${label} update contains an initialize authority`);
    }
  }
  const commitProven = requireBoolean(row.commit_proven, `${label} commit proof`);
  if (!commitProven) throw invalidResponse(`${label} is not commit proven`);
  return {
    config_id: requirePositiveInteger(row.config_id, `${label} configuration ID`),
    coordinate,
    signature: requireBase58Bytes(row.signature, 64, `${label} signature`),
    target_mint_id: requirePositiveInteger(row.target_mint_id, `${label} target mint ID`),
    kind,
    multiplier: validateScaledUiMultiplier(row.multiplier, label),
    effective_timestamp: effectiveTimestamp,
    ...(authorityRegistryId === undefined
      ? {}
      : { authority_registry_id: authorityRegistryId }),
    ...(configuredAuthorityHex === undefined
      ? {}
      : { configured_authority_hex: configuredAuthorityHex }),
    commit_proven: true
  };
}

function validateScaledUiCoordinate(
  value: unknown,
  eventLabel: string
): MarketScaledUiAmountCoordinate {
  const row = requireRecord(value, `${eventLabel} coordinate`);
  const innerIndex =
    row.inner_index === undefined
      ? undefined
      : requireInteger(row.inner_index, `${eventLabel} inner index`);
  const batchIndex =
    row.batch_index === undefined
      ? undefined
      : requireInteger(row.batch_index, `${eventLabel} batch index`);
  const stackHeight = requireInteger(row.stack_height, `${eventLabel} stack height`);
  if ((innerIndex === undefined && stackHeight > 1) || (innerIndex !== undefined && stackHeight <= 1)) {
    throw invalidResponse(`${eventLabel} stack height differs from its instruction level`);
  }
  return {
    transaction_id: requireInteger(row.transaction_id, `${eventLabel} transaction ID`),
    source_epoch: requireInteger(row.source_epoch, `${eventLabel} source epoch`),
    slot: requireInteger(row.slot, `${eventLabel} slot`),
    block_time: requireSignedInteger(row.block_time, `${eventLabel} block time`),
    source_block_id: requireInteger(row.source_block_id, `${eventLabel} source block ID`),
    tx_index: requireInteger(row.tx_index, `${eventLabel} transaction index`),
    outer_index: requireInteger(row.outer_index, `${eventLabel} outer index`),
    ...(innerIndex === undefined ? {} : { inner_index: innerIndex }),
    stack_height: stackHeight,
    ...(batchIndex === undefined ? {} : { batch_index: batchIndex })
  };
}

function validateScaledUiMultiplier(
  value: unknown,
  eventLabel: string
): MarketScaledUiAmountMultiplier {
  const row = requireRecord(value, `${eventLabel} multiplier`);
  const bits = requireCanonicalHex(row.bits, 16, `${eventLabel} multiplier bits`);
  const decimal = requireString(row.decimal, `${eventLabel} multiplier decimal`);
  validateMultiplierBits(decimal, bits, `${eventLabel} multiplier`);
  return { bits, decimal };
}

function canonicalScaledUiKey(event: MarketScaledUiAmountEvent): number[] {
  return [
    event.coordinate.transaction_id,
    event.coordinate.outer_index,
    event.coordinate.inner_index === undefined ? 0 : event.coordinate.inner_index + 1,
    event.coordinate.batch_index === undefined ? 0 : event.coordinate.batch_index + 1
  ];
}

function sourceScaledUiKey(event: MarketScaledUiAmountEvent): number[] {
  return [
    event.coordinate.source_epoch,
    event.coordinate.slot,
    event.coordinate.source_block_id,
    event.coordinate.tx_index,
    event.coordinate.outer_index,
    event.coordinate.inner_index === undefined ? 0 : event.coordinate.inner_index + 1,
    event.coordinate.batch_index === undefined ? 0 : event.coordinate.batch_index + 1
  ];
}

function compareNumberTuples(left: number[], right: number[]): number {
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) return left[index] < right[index] ? -1 : 1;
  }
  return 0;
}

function validatePair(value: unknown): MarketPair {
  const row = requireRecord(value, 'market pair');
  const quoteMint = requireRegistryKey(row.quote_mint, 'pair quote mint');
  return {
    quote_mint: quoteMint.address,
    quote_mint_id: quoteMint.registry_id,
    quote_decimals: requireInteger(row.quote_decimals, 'pair quote decimals'),
    trade_count: requireInteger(row.trade_count, 'pair trade count'),
    program_count: requireInteger(row.program_count ?? row.venue_count, 'pair program count'),
    first_block_time: requireNullableInteger(row.first_block_time, 'pair first block time'),
    last_block_time: requireNullableInteger(row.last_block_time, 'pair last block time'),
    target_volume_raw: requireUnsignedString(row.target_volume_raw, 'pair target volume'),
    quote_volume_raw: requireUnsignedString(row.quote_volume_raw, 'pair quote volume'),
    trades_24h: requireInteger(row.trade_count_24h, 'pair 24-hour trade count'),
    target_volume_24h_raw: requireUnsignedString(
      row.target_volume_24h_raw,
      'pair 24-hour target volume'
    ),
    quote_volume_24h_raw: requireUnsignedString(
      row.quote_volume_24h_raw,
      'pair 24-hour quote volume'
    ),
    latest_price: row.latest_price === null ? null : validatePrice(row.latest_price),
    direct_usd: requireBoolean(row.direct_usd, 'pair direct USD status')
  };
}

function validateCandle(value: unknown): MarketCandle {
  const row = requireRecord(value, 'market candle');
  return {
    start_time: requireInteger(row.start_time, 'candle start time'),
    end_time: requireInteger(row.end_time, 'candle end time'),
    open: validatePrice(row.open),
    high: validatePrice(row.high),
    low: validatePrice(row.low),
    close: validatePrice(row.close),
    trade_count: requireInteger(row.trade_count, 'candle trade count'),
    target_volume_raw: requireUnsignedString(row.target_volume_raw, 'candle target volume'),
    quote_volume_raw: requireUnsignedString(row.quote_volume_raw, 'candle quote volume')
  };
}

function validateSlotCandle(value: unknown): MarketSlotCandle {
  const row = requireRecord(value, 'market slot candle');
  return {
    slot: requireInteger(row.slot, 'slot candle slot'),
    block_time: requireInteger(row.block_time, 'slot candle block time'),
    open: validatePrice(row.open),
    high: validatePrice(row.high),
    low: validatePrice(row.low),
    close: validatePrice(row.close),
    trade_count: requireInteger(row.trade_count, 'slot candle trade count'),
    buy_count: requireInteger(row.buy_count, 'slot candle buy count'),
    sell_count: requireInteger(row.sell_count, 'slot candle sell count'),
    target_volume_raw: requireUnsignedString(row.target_volume_raw, 'slot candle target volume'),
    quote_volume_raw: requireUnsignedString(row.quote_volume_raw, 'slot candle quote volume')
  };
}

function validateProgramVolumeSeries(value: unknown): MarketProgramVolumeSeries {
  const row = requireRecord(value, 'market program volume series');
  const targetMint = requireRegistryKey(row.target_mint, 'program volume target mint');
  const attribution = requireString(row.attribution, 'program volume attribution');
  if (attribution !== 'executed_dex_program') {
    throw invalidResponse('program volume attribution is invalid');
  }
  if (!Array.isArray(row.points)) throw invalidResponse('program volume points are not an array');
  return {
    target_mint: targetMint.address,
    target_decimals: requireInteger(row.target_decimals, 'program volume target decimals'),
    selected_quote_mint:
      row.selected_quote_mint === null || row.selected_quote_mint === undefined
        ? null
        : requireRegistryKey(row.selected_quote_mint, 'program volume quote mint').address,
    interval_seconds: requireInteger(row.interval_seconds, 'program volume interval'),
    time_from: requireInteger(row.time_from, 'program volume start time'),
    time_to: requireInteger(row.time_to, 'program volume end time'),
    attribution,
    points: row.points.map(validateProgramVolumePoint)
  };
}

function validateProgramVolumePoint(value: unknown): MarketProgramVolumePoint {
  const row = requireRecord(value, 'market program volume point');
  if (!Array.isArray(row.programs)) {
    throw invalidResponse('market program volume entries are not an array');
  }
  return {
    interval_seconds: requireInteger(row.interval_seconds, 'program volume point interval'),
    start_time: requireInteger(row.start_time, 'program volume point start time'),
    end_time: requireInteger(row.end_time, 'program volume point end time'),
    trade_count: requireInteger(row.trade_count, 'program volume point trade count'),
    target_volume_raw: requireUnsignedString(
      row.target_volume_raw,
      'program volume point target volume'
    ),
    programs: row.programs.map(validateDexProgramVolume)
  };
}

function validateDexProgramVolume(value: unknown): MarketDexProgramVolume {
  const row = requireRecord(value, 'market DEX program volume');
  return {
    program: requireDexProgram(row.program, 'market DEX program volume program'),
    trade_count: requireInteger(row.trade_count, 'market DEX program trade count'),
    buy_count: requireInteger(row.buy_count, 'market DEX program buy count'),
    sell_count: requireInteger(row.sell_count, 'market DEX program sell count'),
    target_volume_raw: requireUnsignedString(
      row.target_volume_raw,
      'market DEX program target volume'
    ),
    routed_trade_count: requireInteger(
      row.routed_trade_count,
      'market DEX program routed trade count'
    ),
    routed_target_volume_raw: requireUnsignedString(
      row.routed_target_volume_raw,
      'market DEX program routed target volume'
    ),
    router_count: requireInteger(row.router_count, 'market DEX program router count')
  };
}

function validateTrade(value: unknown): MarketTrade {
  const row = requireRecord(value, 'market trade');
  const coordinate = requireRecord(row.transaction, 'market trade coordinate');
  const instruction = requireRecord(row.instruction, 'market trade instruction path');
  const program = requireProgram(row.program, 'market trade program');
  const quoteMint = requireRegistryKey(row.quote_mint, 'market trade quote mint');
  const side = requireString(row.side, 'market trade side');
  if (side !== 'buy' && side !== 'sell') throw invalidResponse('market trade side is invalid');
  return {
    trade_id: requireInteger(row.trade_id, 'market trade ID'),
    transaction_id: requireInteger(coordinate.transaction_id, 'market transaction ID'),
    signature: null,
    coordinate: {
      epoch: requireInteger(coordinate.source_epoch, 'market trade epoch'),
      slot: requireInteger(coordinate.slot, 'market trade slot'),
      source_block_id: requireInteger(coordinate.source_block_id, 'market trade source block ID'),
      tx_index: requireInteger(coordinate.tx_index, 'market trade transaction index')
    },
    block_time: requireNullableInteger(row.block_time, 'market trade block time'),
    outer_index: requireInteger(instruction.outer_index, 'market trade outer index'),
    inner_index: requireOptionalInteger(instruction.inner_index, 'market trade inner index'),
    stack_height: requireInteger(instruction.stack_height, 'market trade stack height'),
    program,
    venue: program.address,
    router: requireOptionalProgram(row.router, 'market trade router'),
    pool: requireOptionalRegistryAddress(row.pool, 'market trade pool'),
    trader: requireOptionalRegistryAddress(row.trader, 'market trade trader'),
    side,
    target_amount_raw: requireUnsignedString(row.target_amount_raw, 'market target amount'),
    target_amount_scaled_ui_raw: requirePositiveUnsignedString(
      row.target_amount_scaled_ui_raw,
      'market Scaled UI target amount'
    ),
    quote_amount_raw: requireUnsignedString(row.quote_amount_raw, 'market quote amount'),
    target_decimals: requireInteger(row.target_decimals, 'market target decimals'),
    quote_decimals: requireInteger(row.quote_decimals, 'market quote decimals'),
    quote_mint: quoteMint.address,
    price: validatePrice(row.price),
    evidence_flags: requireInteger(row.evidence_flags, 'market trade evidence flags')
  };
}

function validateMint(value: unknown): MarketMint {
  const row = requireRecord(value, 'market mint');
  const mint = requireRegistryKey(row.mint, 'market mint key');
  const tokenProgram = row.token_program === undefined ? null : requireString(row.token_program, 'token program');
  if (tokenProgram !== null && tokenProgram !== 'legacy' && tokenProgram !== 'token2022') {
    throw invalidResponse('token program is invalid');
  }
  const rpcDecimals = row.rpc_decimals === undefined
    ? null
    : requireInteger(row.rpc_decimals, 'RPC mint decimals');
  const metadataSource = row.metadata_source === undefined
    ? null
    : requireString(row.metadata_source, 'mint metadata source');
  if (
    metadataSource !== null &&
    metadataSource !== 'token2022' &&
    metadataSource !== 'metaplex' &&
    metadataSource !== 'official_project_site'
  ) {
    throw invalidResponse('mint metadata source is invalid');
  }
  const warnings = row.warnings;
  if (!Array.isArray(warnings) || warnings.some((warning) => typeof warning !== 'string')) {
    throw invalidResponse('mint metadata warnings are invalid');
  }
  return {
    mint: mint.address,
    mint_id: mint.registry_id,
    decimals: requireInteger(row.decimals, 'market mint decimals'),
    is_target: requireBoolean(row.is_target, 'market target mint status'),
    direct_usd_quote: requireBoolean(row.direct_usd_quote, 'market USD quote status'),
    trade_count: requireInteger(row.trade_count, 'market mint trade count'),
    metadata_available: requireBoolean(row.metadata_available, 'mint metadata availability'),
    decimals_verified_onchain: rpcDecimals !== null,
    token_program: tokenProgram,
    metadata_source: metadataSource,
    metadata_source_uri: requireOptionalString(
      row.metadata_source_uri,
      'mint metadata source URI'
    ),
    name: requireOptionalString(row.name, 'mint name'),
    symbol: requireOptionalString(row.symbol, 'mint symbol'),
    uri: requireOptionalString(row.uri, 'mint URI'),
    warnings: warnings as string[]
  };
}

function validateProgramSummary(value: unknown): MarketProgramSummary {
  const row = requireRecord(value, 'market program summary');
  return {
    program: requireProgram(row.program, 'market program'),
    trade_count: requireInteger(row.trade_count, 'market program trade count'),
    trade_count_24h: requireInteger(row.trade_count_24h, 'market program 24-hour trade count'),
    first_block_time: requireInteger(row.first_block_time, 'market program first block time'),
    last_block_time: requireInteger(row.last_block_time, 'market program last block time'),
    target_volume_raw: requireUnsignedString(row.target_volume_raw, 'market program target volume'),
    target_volume_24h_raw: requireUnsignedString(
      row.target_volume_24h_raw,
      'market program 24-hour target volume'
    ),
    pair_count: requireInteger(row.pair_count, 'market program pair count'),
    primary_pool_count: requireInteger(
      row.primary_pool_count ?? row.pool_count,
      'market program primary pool count'
    ),
    routed_trade_count: requireInteger(row.routed_trade_count, 'market program routed trade count')
  };
}

function validatePrice(value: unknown): ExactPrice {
  const row = requireRecord(value, 'exact price');
  const decimal = requireDecimalString(row.decimal, 'price decimal value');
  const unscaledDecimal = requireDecimalString(
    row.unscaled_decimal,
    'unscaled price decimal value'
  );
  const targetMultiplier = requireString(row.target_multiplier, 'price target multiplier');
  const targetMultiplierBits = requireCanonicalHex(
    row.target_multiplier_bits,
    16,
    'price target multiplier bits'
  );
  validateMultiplierBits(targetMultiplier, targetMultiplierBits, 'price target multiplier');
  return {
    numerator: requireUnsignedString(row.numerator, 'price numerator'),
    denominator: requirePositiveUnsignedString(row.denominator, 'price denominator'),
    display: requireDisplayNumber(row.chart_display, decimal, 'price chart value'),
    target_multiplier: targetMultiplier,
    target_multiplier_bits: targetMultiplierBits,
    scaled_ui_config_id: requireInteger(
      row.scaled_ui_config_id,
      'price Scaled UI configuration ID'
    ),
    unscaled_decimal: unscaledDecimal,
    unscaled_display: requireDisplayNumber(
      row.unscaled_chart_display,
      unscaledDecimal,
      'unscaled price chart value'
    )
  };
}

function requireRegistryKey(value: unknown, name: string): { registry_id: number; address: string } {
  const row = requireRecord(value, name);
  return {
    registry_id: requireInteger(row.registry_id, `${name} registry ID`),
    address: requireString(row.address, `${name} address`)
  };
}

function requireProgram(value: unknown, name: string): MarketProgram {
  const row = requireRecord(value, name);
  const role = requireString(row.role, `${name} role`);
  if (role !== 'dex' && role !== 'router') throw invalidResponse(`${name} role is invalid`);
  return {
    ...requireRegistryKey(row, name),
    name: requireString(row.name, `${name} name`),
    role
  };
}

function requireDexProgram(value: unknown, name: string): MarketProgram {
  const program = requireProgram(value, name);
  if (program.role !== 'dex') throw invalidResponse(`${name} is not a DEX program`);
  return program;
}

function requireOptionalProgram(value: unknown, name: string): MarketProgram | null {
  if (value === null || value === undefined) return null;
  return requireProgram(value, name);
}

function requireOptionalRegistryAddress(value: unknown, name: string): string | null {
  if (value === null || value === undefined) return null;
  return requireRegistryKey(value, name).address;
}

function requireOptionalInteger(value: unknown, name: string): number | null {
  if (value === undefined || value === null) return null;
  return requireInteger(value, name);
}

function requirePositiveInteger(value: unknown, name: string): number {
  const integer = requireInteger(value, name);
  if (integer === 0) throw invalidResponse(`${name} is zero`);
  return integer;
}

function requireSignedInteger(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
    throw invalidResponse(`${name} is invalid`);
  }
  return value;
}

function requireRecord(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw invalidResponse(`${name} is not an object`);
  }
  return value as Record<string, unknown>;
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length === 0) throw invalidResponse(`${name} is invalid`);
  return value;
}

function requireOptionalString(value: unknown, name: string): string | null {
  if (value === undefined || value === null) return null;
  return requireString(value, name);
}

function requireInteger(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw invalidResponse(`${name} is invalid`);
  }
  return value;
}

function requireNullableInteger(value: unknown, name: string): number | null {
  if (value === null) return null;
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
    throw invalidResponse(`${name} is invalid`);
  }
  return value;
}

function requireNumber(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) throw invalidResponse(`${name} is invalid`);
  return value;
}

function requireDisplayNumber(value: unknown, fallback: string, name: string): number {
  const display = value === undefined ? Number(fallback) : requireNumber(value, name);
  if (!Number.isFinite(display) || display < 0) throw invalidResponse(`${name} is invalid`);
  return display;
}

function requireBoolean(value: unknown, name: string): boolean {
  if (typeof value !== 'boolean') throw invalidResponse(`${name} is invalid`);
  return value;
}

function requireUnsignedString(value: unknown, name: string): string {
  if (typeof value !== 'string' || !/^\d+$/.test(value)) throw invalidResponse(`${name} is invalid`);
  return value;
}

function requirePositiveUnsignedString(value: unknown, name: string): string {
  const result = requireUnsignedString(value, name);
  if (/^0+$/.test(result)) throw invalidResponse(`${name} is zero`);
  return result;
}

function requireDecimalString(value: unknown, name: string): string {
  const decimal = requireString(value, name);
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?$/.test(decimal)) {
    throw invalidResponse(`${name} is not a canonical unsigned decimal`);
  }
  return decimal;
}

function requireCanonicalHex(value: unknown, length: number, name: string): string {
  const hex = requireString(value, name);
  if (hex.length !== length || !/^[0-9a-f]+$/.test(hex)) {
    throw invalidResponse(`${name} is not canonical lowercase hexadecimal`);
  }
  return hex;
}

function validateMultiplierBits(decimal: string, bits: string, name: string): void {
  if (!/^(?:0|[1-9]\d*)(?:\.\d+)?(?:e[+-]?\d+)?$/.test(decimal)) {
    throw invalidResponse(`${name} decimal is not canonical`);
  }
  const multiplier = Number(decimal);
  if (!Number.isFinite(multiplier) || multiplier < 2.2250738585072014e-308) {
    throw invalidResponse(`${name} is not a positive normal number`);
  }
  const buffer = new ArrayBuffer(8);
  const view = new DataView(buffer);
  view.setFloat64(0, multiplier, false);
  const parsedBits = view.getBigUint64(0, false).toString(16).padStart(16, '0');
  if (parsedBits !== bits) throw invalidResponse(`${name} decimal differs from its exact bits`);
}

function requireBase58Bytes(value: unknown, byteLength: number, name: string): string {
  const encoded = requireString(value, name);
  const alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  if (!/^[1-9A-HJ-NP-Za-km-z]+$/.test(encoded)) {
    throw invalidResponse(`${name} is not Base58`);
  }
  let leadingZeroes = 0;
  while (leadingZeroes < encoded.length && encoded[leadingZeroes] === '1') leadingZeroes += 1;
  let magnitude = 0n;
  for (const character of encoded) {
    const digit = alphabet.indexOf(character);
    if (digit < 0) throw invalidResponse(`${name} is not Base58`);
    magnitude = magnitude * 58n + BigInt(digit);
  }
  let magnitudeBytes = 0;
  while (magnitude !== 0n) {
    magnitude >>= 8n;
    magnitudeBytes += 1;
  }
  if (leadingZeroes + magnitudeBytes !== byteLength) {
    throw invalidResponse(`${name} does not encode ${byteLength} bytes`);
  }
  return encoded;
}

async function readError(response: Response, fallback: string): Promise<string> {
  try {
    const value = requireRecord(await response.json(), 'error response');
    if (typeof value.message === 'string') return value.message;
    if (typeof value.error === 'string') return value.error;
  } catch {
    return fallback;
  }
  return fallback;
}

function invalidResponse(message: string): SearchApiError {
  return new SearchApiError(`Invalid market API response: ${message}.`, null, false);
}
