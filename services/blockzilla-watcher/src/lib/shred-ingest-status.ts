export type ShredGossipState = 'observed' | 'waiting' | 'unavailable';
export type ShredTvuState = 'receiving' | 'waiting' | 'idle' | 'unavailable';
export type ShredForwardingState =
  | 'disabled'
  | 'waiting'
  | 'sending'
  | 'errors'
  | 'unavailable';
export type HivezillaAvailability = 'available' | 'unavailable';
export type HivezillaShredState =
  | 'waiting'
  | 'receiving'
  | 'stalled'
  | 'stopped'
  | 'unavailable';

type NullableMetric = number | null;

export type ShredIngestStatus = {
  schema_version: 1;
  updated_unix_secs: number;
  gossip: {
    state: ShredGossipState;
    recent_peer_count: NullableMetric;
    known_peer_count: NullableMetric;
    tvu_peer_count: NullableMetric;
    shred_version: NullableMetric;
    receiver_uptime_secs: NullableMetric;
    updated_unix_secs: NullableMetric;
  };
  tvu: {
    state: ShredTvuState;
    packets_total: NullableMetric;
    bytes_total: NullableMetric;
    parsed_total: NullableMetric;
    unique_total: NullableMetric;
    duplicates_total: NullableMetric;
    data_total: NullableMetric;
    code_total: NullableMetric;
    invalid_total: NullableMetric;
    version_mismatch_total: NullableMetric;
    latest_slot: NullableMetric;
    seconds_since_last_packet: NullableMetric;
    updated_unix_secs: NullableMetric;
  };
  forwarding: {
    state: ShredForwardingState;
    target_count: NullableMetric;
    attempts_total: NullableMetric;
    successful_datagrams_total: NullableMetric;
    errors_total: NullableMetric;
    updated_unix_secs: NullableMetric;
  };
  hivezilla: {
    availability: HivezillaAvailability;
    status_fresh: boolean;
    state: HivezillaShredState;
    updated_unix_secs: NullableMetric;
    started_unix_secs: NullableMetric;
    accepted_total: NullableMetric;
    invalid_total: NullableMetric;
    bytes_total: NullableMetric;
    durable_through_sequence: NullableMetric;
    latest_slot: NullableMetric;
    shred_version: NullableMetric;
    last_durable_unix_secs: NullableMetric;
    spool_bytes: NullableMetric;
    spool_max_bytes: NullableMetric;
    filesystem_free_bytes: NullableMetric;
    filesystem_total_bytes: NullableMetric;
    reserve_free_bytes: NullableMetric;
  };
};

const GOSSIP_STATES = new Set<ShredGossipState>(['observed', 'waiting', 'unavailable']);
const TVU_STATES = new Set<ShredTvuState>(['receiving', 'waiting', 'idle', 'unavailable']);
const FORWARDING_STATES = new Set<ShredForwardingState>([
  'disabled',
  'waiting',
  'sending',
  'errors',
  'unavailable'
]);
const HIVEZILLA_AVAILABILITY = new Set<HivezillaAvailability>(['available', 'unavailable']);
const HIVEZILLA_STATES = new Set<HivezillaShredState>([
  'waiting',
  'receiving',
  'stalled',
  'stopped',
  'unavailable'
]);

const ROOT_KEYS = [
  'schema_version',
  'updated_unix_secs',
  'gossip',
  'tvu',
  'forwarding',
  'hivezilla'
] as const;
const GOSSIP_KEYS = [
  'state',
  'recent_peer_count',
  'known_peer_count',
  'tvu_peer_count',
  'shred_version',
  'receiver_uptime_secs',
  'updated_unix_secs'
] as const;
const TVU_KEYS = [
  'state',
  'packets_total',
  'bytes_total',
  'parsed_total',
  'unique_total',
  'duplicates_total',
  'data_total',
  'code_total',
  'invalid_total',
  'version_mismatch_total',
  'latest_slot',
  'seconds_since_last_packet',
  'updated_unix_secs'
] as const;
const FORWARDING_KEYS = [
  'state',
  'target_count',
  'attempts_total',
  'successful_datagrams_total',
  'errors_total',
  'updated_unix_secs'
] as const;
const HIVEZILLA_KEYS = [
  'availability',
  'status_fresh',
  'state',
  'updated_unix_secs',
  'started_unix_secs',
  'accepted_total',
  'invalid_total',
  'bytes_total',
  'durable_through_sequence',
  'latest_slot',
  'shred_version',
  'last_durable_unix_secs',
  'spool_bytes',
  'spool_max_bytes',
  'filesystem_free_bytes',
  'filesystem_total_bytes',
  'reserve_free_bytes'
] as const;

export function parseShredIngestStatus(value: unknown): ShredIngestStatus | null {
  const root = exactRecord(value, ROOT_KEYS);
  if (!root || root.schema_version !== 1) return null;

  const updatedUnixSecs = positiveInteger(root.updated_unix_secs);
  const gossip = parseGossip(root.gossip);
  const tvu = parseTvu(root.tvu);
  const forwarding = parseForwarding(root.forwarding);
  const hivezilla = parseHivezilla(root.hivezilla);
  if (
    updatedUnixSecs === null ||
    !gossip ||
    !tvu ||
    !forwarding ||
    !hivezilla
  ) return null;

  if (
    gossip.updated_unix_secs !== null &&
    gossip.updated_unix_secs > updatedUnixSecs + 5
  ) return null;
  if (tvu.updated_unix_secs !== null && tvu.updated_unix_secs > updatedUnixSecs + 5) return null;
  if (
    forwarding.updated_unix_secs !== null &&
    forwarding.updated_unix_secs > updatedUnixSecs + 5
  ) return null;
  if (
    hivezilla.updated_unix_secs !== null &&
    hivezilla.updated_unix_secs > updatedUnixSecs + 5
  ) return null;

  return value as ShredIngestStatus;
}

export function shredIngestStatusIsFresh(
  value: ShredIngestStatus,
  nowUnixSecs: number,
  maxAgeSecs = 30,
  maxFutureSkewSecs = 5
) {
  return value.updated_unix_secs <= nowUnixSecs + maxFutureSkewSecs &&
    nowUnixSecs <= value.updated_unix_secs + maxAgeSecs;
}

function parseGossip(value: unknown): ShredIngestStatus['gossip'] | null {
  const stage = exactRecord(value, GOSSIP_KEYS);
  if (!stage || !enumValue(stage.state, GOSSIP_STATES)) return null;

  const metrics = GOSSIP_KEYS.slice(1).map((key) => nullableNonNegativeInteger(stage[key]));
  if (metrics.some((metric) => metric === undefined)) return null;
  const typed = value as ShredIngestStatus['gossip'];
  if (typed.state === 'unavailable') {
    return metrics.every((metric) => metric === null) ? typed : null;
  }
  if (metrics.some((metric) => metric === null)) return null;
  if (
    typed.shred_version === null ||
    typed.shred_version === 0 ||
    typed.shred_version > 65_535 ||
    typed.updated_unix_secs === null ||
    typed.updated_unix_secs === 0 ||
    typed.recent_peer_count === null ||
    typed.known_peer_count === null ||
    typed.recent_peer_count > typed.known_peer_count
  ) return null;
  if (typed.state === 'observed' && typed.recent_peer_count === 0) return null;
  if (typed.state === 'waiting' && typed.recent_peer_count !== 0) return null;
  return typed;
}

function parseTvu(value: unknown): ShredIngestStatus['tvu'] | null {
  const stage = exactRecord(value, TVU_KEYS);
  if (!stage || !enumValue(stage.state, TVU_STATES)) return null;

  const metrics = TVU_KEYS.slice(1).map((key) => nullableNonNegativeInteger(stage[key]));
  if (metrics.some((metric) => metric === undefined)) return null;
  const typed = value as ShredIngestStatus['tvu'];
  if (typed.state === 'unavailable') {
    return metrics.every((metric) => metric === null) ? typed : null;
  }
  if (
    typed.packets_total === null ||
    typed.bytes_total === null ||
    typed.parsed_total === null ||
    typed.unique_total === null ||
    typed.duplicates_total === null ||
    typed.data_total === null ||
    typed.code_total === null ||
    typed.invalid_total === null ||
    typed.version_mismatch_total === null ||
    typed.updated_unix_secs === null ||
    typed.updated_unix_secs === 0
  ) return null;
  if ((typed.parsed_total === 0) !== (typed.latest_slot === null)) return null;
  return typed;
}

function parseForwarding(value: unknown): ShredIngestStatus['forwarding'] | null {
  const stage = exactRecord(value, FORWARDING_KEYS);
  if (!stage || !enumValue(stage.state, FORWARDING_STATES)) return null;

  const metrics = FORWARDING_KEYS.slice(1).map((key) => nullableNonNegativeInteger(stage[key]));
  if (metrics.some((metric) => metric === undefined)) return null;
  const typed = value as ShredIngestStatus['forwarding'];
  if (typed.state === 'unavailable') {
    return metrics.every((metric) => metric === null) ? typed : null;
  }
  if (
    typed.target_count === null ||
    typed.attempts_total === null ||
    typed.successful_datagrams_total === null ||
    typed.errors_total === null ||
    typed.updated_unix_secs === null ||
    typed.updated_unix_secs === 0
  ) return null;
  if (
    addSafe(typed.successful_datagrams_total, typed.errors_total) !== typed.attempts_total
  ) return null;
  if (
    typed.state === 'disabled' &&
    (typed.target_count !== 0 || typed.attempts_total !== 0)
  ) return null;
  if (typed.state !== 'disabled' && typed.target_count === 0) return null;
  if (
    typed.state === 'waiting' &&
    typed.attempts_total !== 0
  ) return null;
  if (typed.state === 'sending' && typed.successful_datagrams_total === 0) return null;
  if (typed.state === 'errors' && typed.errors_total === 0) return null;
  return typed;
}

function parseHivezilla(value: unknown): ShredIngestStatus['hivezilla'] | null {
  const stage = exactRecord(value, HIVEZILLA_KEYS);
  if (
    !stage ||
    !enumValue(stage.availability, HIVEZILLA_AVAILABILITY) ||
    typeof stage.status_fresh !== 'boolean' ||
    !enumValue(stage.state, HIVEZILLA_STATES)
  ) return null;

  const numericKeys = HIVEZILLA_KEYS.slice(3);
  const metrics = numericKeys.map((key) => nullableNonNegativeInteger(stage[key]));
  if (metrics.some((metric) => metric === undefined)) return null;
  const typed = value as ShredIngestStatus['hivezilla'];
  if (typed.availability === 'unavailable') {
    return typed.state === 'unavailable' &&
      !typed.status_fresh &&
      metrics.every((metric) => metric === null)
      ? typed
      : null;
  }
  if (typed.state === 'unavailable') return null;

  const required = [
    typed.updated_unix_secs,
    typed.started_unix_secs,
    typed.accepted_total,
    typed.invalid_total,
    typed.bytes_total,
    typed.spool_bytes,
    typed.spool_max_bytes,
    typed.filesystem_free_bytes,
    typed.filesystem_total_bytes,
    typed.reserve_free_bytes
  ];
  if (required.some((metric) => metric === null)) return null;
  if (
    typed.updated_unix_secs === null ||
    typed.updated_unix_secs === 0 ||
    typed.started_unix_secs === null ||
    typed.started_unix_secs === 0 ||
    typed.updated_unix_secs < typed.started_unix_secs ||
    typed.spool_bytes === null ||
    typed.spool_max_bytes === null ||
    typed.spool_bytes > typed.spool_max_bytes ||
    typed.filesystem_free_bytes === null ||
    typed.filesystem_total_bytes === null ||
    typed.filesystem_free_bytes > typed.filesystem_total_bytes ||
    typed.reserve_free_bytes === null ||
    typed.reserve_free_bytes > typed.filesystem_total_bytes
  ) return null;
  if (
    typed.last_durable_unix_secs !== null &&
    (typed.last_durable_unix_secs === 0 || typed.last_durable_unix_secs > typed.updated_unix_secs)
  ) return null;
  const durableTail = [
    typed.durable_through_sequence,
    typed.latest_slot,
    typed.shred_version
  ];
  if (
    durableTail.some((metric) => metric === null) &&
    durableTail.some((metric) => metric !== null)
  ) return null;
  if (
    typed.last_durable_unix_secs !== null &&
    durableTail.some((metric) => metric === null)
  ) return null;
  if (
    typed.shred_version !== null &&
    (typed.shred_version === 0 || typed.shred_version > 65_535)
  ) return null;
  if (
    typed.accepted_total !== null &&
    typed.bytes_total !== null &&
    ((typed.accepted_total === 0 && typed.bytes_total !== 0) ||
      (typed.accepted_total > 0 &&
        (typed.bytes_total === 0 ||
          typed.last_durable_unix_secs === null ||
          durableTail.some((metric) => metric === null))))
  ) return null;
  if (
    ['receiving', 'stalled'].includes(typed.state) &&
    typed.last_durable_unix_secs === null
  ) return null;
  return typed;
}

function exactRecord<const Keys extends readonly string[]>(
  value: unknown,
  keys: Keys
): Record<Keys[number], unknown> | null {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return null;
  const record = value as Record<string, unknown>;
  const actual = Object.keys(record);
  if (actual.length !== keys.length || !keys.every((key) => actual.includes(key))) return null;
  return record as Record<Keys[number], unknown>;
}

function enumValue<T extends string>(value: unknown, allowed: Set<T>): value is T {
  return typeof value === 'string' && allowed.has(value as T);
}

function nonNegativeInteger(value: unknown) {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function positiveInteger(value: unknown) {
  const parsed = nonNegativeInteger(value);
  return parsed !== null && parsed > 0 ? parsed : null;
}

function nullableNonNegativeInteger(value: unknown): number | null | undefined {
  if (value === null) return null;
  const parsed = nonNegativeInteger(value);
  return parsed === null ? undefined : parsed;
}

function addSafe(...values: Array<number | null>) {
  if (values.some((value) => value === null)) return null;
  const sum = (values as number[]).reduce((total, value) => total + value, 0);
  return Number.isSafeInteger(sum) ? sum : null;
}
