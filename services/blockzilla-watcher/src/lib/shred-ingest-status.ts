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
export type ShredRepairAvailability = 'available' | 'unavailable';
export type ShredRepairState =
  | 'disabled'
  | 'inactive'
  | 'starting'
  | 'active'
  | 'backoff'
  | 'stopping'
  | 'unavailable';

type NullableMetric = number | null;

export type HivezillaDurableSource = {
  committed_datagrams_total: number;
  last_durable_unix_secs: NullableMetric;
  last_durable_sequence: NullableMetric;
  last_durable_slot: NullableMetric;
};

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
    socket_rxq_overflow_supported: boolean | null;
    socket_rxq_overflow_total: NullableMetric;
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
  repair: {
    availability: ShredRepairAvailability;
    enabled: boolean | null;
    active: boolean | null;
    state: ShredRepairState;
    restart_count: NullableMetric;
    seconds_since_last_success: NullableMetric;
    peers: NullableMetric;
    tracked_slots: NullableMetric;
    outstanding: NullableMetric;
    observation_queue_dropped_total: NullableMetric;
    requests_sent_total: NullableMetric;
    requests_exhausted_total: NullableMetric;
    shreds_accepted_total: NullableMetric;
    socket_datagrams_received_total: NullableMetric;
    response_datagrams_processed_total: NullableMetric;
    socket_requested_recv_buffer_bytes: NullableMetric;
    socket_effective_recv_buffer_bytes: NullableMetric;
    socket_rxq_overflow_supported: boolean | null;
    socket_rxq_overflow_total: NullableMetric;
    response_queue_capacity: NullableMetric;
    response_queue_depth: NullableMetric;
    response_queue_dropped_total: NullableMetric;
    wal_retained_bytes: NullableMetric;
    wal_segment_count: NullableMetric;
    wal_rollovers_total: NullableMetric;
    wal_durable_through_sequence: NullableMetric;
    wal_warning_bytes: NullableMetric;
    wal_critical_bytes: NullableMetric;
    wal_hard_bytes: NullableMetric;
    wal_filesystem_reserve_bytes: NullableMetric;
    wal_filesystem_available_bytes: NullableMetric;
    wal_warning: boolean | null;
    wal_critical: boolean | null;
    wal_hard: boolean | null;
    wal_filesystem_reserve_breached: boolean | null;
    wal_admission_blocked: boolean | null;
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
    udp_received_total: NullableMetric;
    udp_received_bytes_total: NullableMetric;
    ingest_queue_depth_events: NullableMetric;
    ingest_queue_depth_bytes: NullableMetric;
    ingest_queue_high_water_events: NullableMetric;
    ingest_queue_high_water_bytes: NullableMetric;
    ingest_queue_capacity_events: NullableMetric;
    ingest_queue_capacity_bytes: NullableMetric;
    ingest_queue_backpressure_events_total: NullableMetric;
    ingest_queue_backpressure_micros_total: NullableMetric;
    ingest_queue_backpressured: boolean | null;
    socket_rxq_overflow_supported: boolean | null;
    socket_rxq_overflow_total: NullableMetric;
    durable_sources: Record<string, HivezillaDurableSource>;
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
const REPAIR_AVAILABILITY = new Set<ShredRepairAvailability>(['available', 'unavailable']);
const REPAIR_STATES = new Set<ShredRepairState>([
  'disabled',
  'inactive',
  'starting',
  'active',
  'backoff',
  'stopping',
  'unavailable'
]);

const ROOT_KEYS = [
  'schema_version',
  'updated_unix_secs',
  'gossip',
  'tvu',
  'forwarding',
  'repair',
  'hivezilla'
] as const;
const LEGACY_ROOT_KEYS = [
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
  'socket_rxq_overflow_supported',
  'socket_rxq_overflow_total',
  'latest_slot',
  'seconds_since_last_packet',
  'updated_unix_secs'
] as const;
const LEGACY_TVU_KEYS = TVU_KEYS.filter(
  (key) => !['socket_rxq_overflow_supported', 'socket_rxq_overflow_total'].includes(key)
);
const FORWARDING_KEYS = [
  'state',
  'target_count',
  'attempts_total',
  'successful_datagrams_total',
  'errors_total',
  'updated_unix_secs'
] as const;
const REPAIR_KEYS = [
  'availability',
  'enabled',
  'active',
  'state',
  'restart_count',
  'seconds_since_last_success',
  'peers',
  'tracked_slots',
  'outstanding',
  'observation_queue_dropped_total',
  'requests_sent_total',
  'requests_exhausted_total',
  'shreds_accepted_total',
  'socket_datagrams_received_total',
  'response_datagrams_processed_total',
  'socket_requested_recv_buffer_bytes',
  'socket_effective_recv_buffer_bytes',
  'socket_rxq_overflow_supported',
  'socket_rxq_overflow_total',
  'response_queue_capacity',
  'response_queue_depth',
  'response_queue_dropped_total',
  'wal_retained_bytes',
  'wal_segment_count',
  'wal_rollovers_total',
  'wal_durable_through_sequence',
  'wal_warning_bytes',
  'wal_critical_bytes',
  'wal_hard_bytes',
  'wal_filesystem_reserve_bytes',
  'wal_filesystem_available_bytes',
  'wal_warning',
  'wal_critical',
  'wal_hard',
  'wal_filesystem_reserve_breached',
  'wal_admission_blocked',
  'updated_unix_secs'
] as const;
const CURRENT_HIVEZILLA_KEYS = [
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
  'reserve_free_bytes',
  'udp_received_total',
  'udp_received_bytes_total',
  'ingest_queue_depth_events',
  'ingest_queue_depth_bytes',
  'ingest_queue_high_water_events',
  'ingest_queue_high_water_bytes',
  'ingest_queue_capacity_events',
  'ingest_queue_capacity_bytes',
  'ingest_queue_backpressure_events_total',
  'ingest_queue_backpressure_micros_total',
  'ingest_queue_backpressured',
  'socket_rxq_overflow_supported',
  'socket_rxq_overflow_total'
] as const;
const HIVEZILLA_KEYS = [...CURRENT_HIVEZILLA_KEYS, 'durable_sources'] as const;
const LEGACY_HIVEZILLA_KEYS = CURRENT_HIVEZILLA_KEYS.filter(
  (key) => ![
    'udp_received_total',
    'udp_received_bytes_total',
    'ingest_queue_depth_events',
    'ingest_queue_depth_bytes',
    'ingest_queue_high_water_events',
    'ingest_queue_high_water_bytes',
    'ingest_queue_capacity_events',
    'ingest_queue_capacity_bytes',
    'ingest_queue_backpressure_events_total',
    'ingest_queue_backpressure_micros_total',
    'ingest_queue_backpressured',
    'socket_rxq_overflow_supported',
    'socket_rxq_overflow_total'
  ].includes(key)
);

export function parseShredIngestStatus(value: unknown): ShredIngestStatus | null {
  const currentRoot = exactRecord(value, ROOT_KEYS);
  const legacyRoot = currentRoot ? null : exactRecord(value, LEGACY_ROOT_KEYS);
  const root = currentRoot ?? legacyRoot;
  if (!root || root.schema_version !== 1) return null;
  const legacy = legacyRoot !== null;

  const updatedUnixSecs = positiveInteger(root.updated_unix_secs);
  const gossip = parseGossip(root.gossip);
  const tvu = parseTvu(root.tvu, legacy);
  const forwarding = parseForwarding(root.forwarding);
  const repair = legacy ? unavailableRepair() : parseRepair(currentRoot?.repair);
  const hivezilla = parseHivezilla(root.hivezilla, legacy);
  if (
    updatedUnixSecs === null ||
    !gossip ||
    !tvu ||
    !forwarding ||
    !repair ||
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
    repair.updated_unix_secs !== null &&
    repair.updated_unix_secs > updatedUnixSecs + 5
  ) return null;
  if (
    hivezilla.updated_unix_secs !== null &&
    hivezilla.updated_unix_secs > updatedUnixSecs + 5
  ) return null;

  return {
    schema_version: 1,
    updated_unix_secs: updatedUnixSecs,
    gossip,
    tvu,
    forwarding,
    repair,
    hivezilla
  };
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

function parseTvu(value: unknown, legacy = false): ShredIngestStatus['tvu'] | null {
  const stage = legacy
    ? exactRecord(value, LEGACY_TVU_KEYS)
    : exactRecord(value, TVU_KEYS);
  if (!stage || !enumValue(stage.state, TVU_STATES)) return null;

  const numericKeys = (legacy ? LEGACY_TVU_KEYS : TVU_KEYS)
    .slice(1)
    .filter((key) => key !== 'socket_rxq_overflow_supported');
  const metrics = numericKeys.map((key) => nullableNonNegativeInteger(stage[key]));
  if (metrics.some((metric) => metric === undefined)) return null;
  const typed = (legacy
    ? {
        ...(value as Record<string, unknown>),
        socket_rxq_overflow_supported: stage.state === 'unavailable' ? null : false,
        socket_rxq_overflow_total: null
      }
    : value) as ShredIngestStatus['tvu'];
  if (typed.state === 'unavailable') {
    return typed.socket_rxq_overflow_supported === null &&
      metrics.every((metric) => metric === null)
      ? typed
      : null;
  }
  if (typeof typed.socket_rxq_overflow_supported !== 'boolean') return null;
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
  if (
    typed.socket_rxq_overflow_supported !== (typed.socket_rxq_overflow_total !== null)
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

function parseRepair(value: unknown): ShredIngestStatus['repair'] | null {
  const stage = exactRecord(value, REPAIR_KEYS);
  if (
    !stage ||
    !enumValue(stage.availability, REPAIR_AVAILABILITY) ||
    !enumValue(stage.state, REPAIR_STATES)
  ) return null;

  const booleanKeys = [
    'enabled',
    'active',
    'socket_rxq_overflow_supported',
    'wal_warning',
    'wal_critical',
    'wal_hard',
    'wal_filesystem_reserve_breached',
    'wal_admission_blocked'
  ] as const;
  const numericKeys = REPAIR_KEYS.slice(4).filter(
    (key) => !booleanKeys.includes(key as (typeof booleanKeys)[number])
  );
  const metrics = numericKeys.map((key) => nullableNonNegativeInteger(stage[key]));
  if (metrics.some((metric) => metric === undefined)) return null;

  const typed = value as ShredIngestStatus['repair'];
  if (typed.availability === 'unavailable') {
    return typed.state === 'unavailable' &&
      booleanKeys.every((key) => typed[key] === null) &&
      metrics.every((metric) => metric === null)
      ? typed
      : null;
  }
  if (typed.state === 'unavailable') return null;
  if (booleanKeys.some((key) => typeof typed[key] !== 'boolean')) return null;
  const required = [
    typed.restart_count,
    typed.peers,
    typed.tracked_slots,
    typed.outstanding,
    typed.observation_queue_dropped_total,
    typed.requests_sent_total,
    typed.requests_exhausted_total,
    typed.shreds_accepted_total,
    typed.socket_datagrams_received_total,
    typed.response_datagrams_processed_total,
    typed.socket_requested_recv_buffer_bytes,
    typed.socket_effective_recv_buffer_bytes,
    typed.response_queue_capacity,
    typed.response_queue_depth,
    typed.response_queue_dropped_total,
    typed.wal_retained_bytes,
    typed.wal_segment_count,
    typed.wal_rollovers_total,
    typed.wal_warning_bytes,
    typed.wal_critical_bytes,
    typed.wal_hard_bytes,
    typed.wal_filesystem_reserve_bytes,
    typed.updated_unix_secs
  ];
  if (required.some((metric) => metric === null)) return null;
  if (
    typed.updated_unix_secs === null ||
    typed.updated_unix_secs === 0 ||
    typed.response_queue_capacity === null ||
    typed.response_queue_depth === null ||
    typed.response_queue_depth > typed.response_queue_capacity ||
    typed.socket_rxq_overflow_supported !== (typed.socket_rxq_overflow_total !== null)
  ) return null;
  if (
    typed.enabled === false &&
    (typed.active !== false || typed.state !== 'disabled')
  ) return null;
  if (typed.enabled === true && typed.state === 'disabled') return null;
  if (
    typed.enabled === true &&
    (typed.wal_warning_bytes === null ||
      typed.wal_critical_bytes === null ||
      typed.wal_hard_bytes === null ||
      typed.wal_warning_bytes >= typed.wal_critical_bytes ||
      typed.wal_critical_bytes >= typed.wal_hard_bytes)
  ) return null;
  return typed;
}

function parseHivezilla(value: unknown, legacy = false): ShredIngestStatus['hivezilla'] | null {
  const newStage = legacy ? null : exactRecord(value, HIVEZILLA_KEYS);
  const currentStage = legacy || newStage ? null : exactRecord(value, CURRENT_HIVEZILLA_KEYS);
  const stage = legacy ? exactRecord(value, LEGACY_HIVEZILLA_KEYS) : newStage ?? currentStage;
  if (
    !stage ||
    !enumValue(stage.availability, HIVEZILLA_AVAILABILITY) ||
    typeof stage.status_fresh !== 'boolean' ||
    !enumValue(stage.state, HIVEZILLA_STATES)
  ) return null;

  const booleanKeys = [
    'ingest_queue_backpressured',
    'socket_rxq_overflow_supported'
  ] as const;
  const numericKeys = (legacy ? LEGACY_HIVEZILLA_KEYS : CURRENT_HIVEZILLA_KEYS).slice(3).filter(
    (key) => !booleanKeys.includes(key as (typeof booleanKeys)[number])
  );
  const metrics = numericKeys.map((key) => nullableNonNegativeInteger(stage[key]));
  if (metrics.some((metric) => metric === undefined)) return null;
  const typed = (legacy
    ? {
        ...(value as Record<string, unknown>),
        udp_received_total: null,
        udp_received_bytes_total: null,
        ingest_queue_depth_events: null,
        ingest_queue_depth_bytes: null,
        ingest_queue_high_water_events: null,
        ingest_queue_high_water_bytes: null,
        ingest_queue_capacity_events: null,
        ingest_queue_capacity_bytes: null,
        ingest_queue_backpressure_events_total: null,
        ingest_queue_backpressure_micros_total: null,
        ingest_queue_backpressured: null,
        socket_rxq_overflow_supported: stage.availability === 'unavailable' ? null : false,
        socket_rxq_overflow_total: null,
        durable_sources: {}
      }
    : currentStage
      ? { ...(value as Record<string, unknown>), durable_sources: {} }
      : value) as ShredIngestStatus['hivezilla'];
  const durableSources = parseHivezillaDurableSources(typed.durable_sources, typed);
  if (!durableSources) return null;
  typed.durable_sources = durableSources;
  if (typed.availability === 'unavailable') {
    return typed.state === 'unavailable' &&
      !typed.status_fresh &&
      booleanKeys.every((key) => typed[key] === null) &&
      metrics.every((metric) => metric === null)
      ? typed
      : null;
  }
  if (typed.state === 'unavailable') return null;
  if (typeof typed.socket_rxq_overflow_supported !== 'boolean') return null;
  const queueMetrics = [
    typed.udp_received_total,
    typed.udp_received_bytes_total,
    typed.ingest_queue_depth_events,
    typed.ingest_queue_depth_bytes,
    typed.ingest_queue_high_water_events,
    typed.ingest_queue_high_water_bytes,
    typed.ingest_queue_capacity_events,
    typed.ingest_queue_capacity_bytes,
    typed.ingest_queue_backpressure_events_total,
    typed.ingest_queue_backpressure_micros_total
  ];
  const queueTelemetryUnavailable =
    queueMetrics.every((metric) => metric === null) &&
    typed.ingest_queue_backpressured === null;
  const queueTelemetryAvailable =
    queueMetrics.every((metric) => metric !== null) &&
    typeof typed.ingest_queue_backpressured === 'boolean';
  if (!queueTelemetryUnavailable && !queueTelemetryAvailable) return null;
  if (
    typed.socket_rxq_overflow_supported !== (typed.socket_rxq_overflow_total !== null)
  ) return null;

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
  const acceptedOrInvalid = addSafe(typed.accepted_total, typed.invalid_total);
  if (queueTelemetryAvailable && (
    typed.udp_received_total === null ||
    typed.udp_received_bytes_total === null ||
    typed.accepted_total === null ||
    typed.invalid_total === null ||
    acceptedOrInvalid === null ||
    acceptedOrInvalid > typed.udp_received_total ||
    typed.bytes_total === null ||
    typed.bytes_total > typed.udp_received_bytes_total
  )) return null;
  if (queueTelemetryAvailable && (
    typed.ingest_queue_depth_events === null ||
    typed.ingest_queue_high_water_events === null ||
    typed.ingest_queue_capacity_events === null ||
    typed.ingest_queue_depth_bytes === null ||
    typed.ingest_queue_high_water_bytes === null ||
    typed.ingest_queue_capacity_bytes === null ||
    typed.ingest_queue_capacity_events === 0 ||
    typed.ingest_queue_capacity_bytes === 0 ||
    typed.ingest_queue_depth_events > typed.ingest_queue_high_water_events ||
    typed.ingest_queue_high_water_events > typed.ingest_queue_capacity_events ||
    typed.ingest_queue_depth_bytes > typed.ingest_queue_high_water_bytes ||
    typed.ingest_queue_high_water_bytes > typed.ingest_queue_capacity_bytes
  )) return null;
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

function parseHivezillaDurableSources(
  value: unknown,
  hivezilla: Omit<ShredIngestStatus['hivezilla'], 'durable_sources'> & {
    durable_sources?: unknown;
  }
): Record<string, HivezillaDurableSource> | null {
  if (!isRecord(value)) return null;
  const entries = Object.entries(value);
  if (entries.length > 4) return null;

  const parsed: Record<string, HivezillaDurableSource> = {};
  let committedTotal = 0;
  for (const [name, evidence] of entries) {
    if (!/^[A-Za-z][A-Za-z0-9_-]{0,31}$/.test(name)) return null;
    const record = exactRecord(evidence, [
      'committed_datagrams_total',
      'last_durable_unix_secs',
      'last_durable_sequence',
      'last_durable_slot'
    ] as const);
    if (!record) return null;
    const committed = nonNegativeInteger(record.committed_datagrams_total);
    const timestamp = nullableNonNegativeInteger(record.last_durable_unix_secs);
    const sequence = nullableNonNegativeInteger(record.last_durable_sequence);
    const slot = nullableNonNegativeInteger(record.last_durable_slot);
    if (
      committed === null ||
      timestamp === undefined ||
      sequence === undefined ||
      slot === undefined
    ) return null;
    const tail = [timestamp, sequence, slot];
    if (
      (committed === 0 && tail.some((metric) => metric !== null)) ||
      (committed > 0 && tail.some((metric) => metric === null))
    ) return null;
    if (
      timestamp !== null &&
      (timestamp === 0 ||
        hivezilla.started_unix_secs === null ||
        timestamp < hivezilla.started_unix_secs ||
        hivezilla.updated_unix_secs === null ||
        timestamp > hivezilla.updated_unix_secs)
    ) return null;
    if (
      sequence !== null &&
      (hivezilla.durable_through_sequence === null ||
        sequence > hivezilla.durable_through_sequence)
    ) return null;
    if (
      slot !== null &&
      (hivezilla.latest_slot === null || slot > hivezilla.latest_slot)
    ) return null;
    const nextCommittedTotal = addSafe(committedTotal, committed);
    if (nextCommittedTotal === null) return null;
    committedTotal = nextCommittedTotal;
    parsed[name] = {
      committed_datagrams_total: committed,
      last_durable_unix_secs: timestamp,
      last_durable_sequence: sequence,
      last_durable_slot: slot
    };
  }
  if (
    (hivezilla.accepted_total === null && entries.length !== 0) ||
    (hivezilla.accepted_total !== null && committedTotal > hivezilla.accepted_total)
  ) return null;
  return parsed;
}

function unavailableRepair(): ShredIngestStatus['repair'] {
  return {
    availability: 'unavailable',
    enabled: null,
    active: null,
    state: 'unavailable',
    restart_count: null,
    seconds_since_last_success: null,
    peers: null,
    tracked_slots: null,
    outstanding: null,
    observation_queue_dropped_total: null,
    requests_sent_total: null,
    requests_exhausted_total: null,
    shreds_accepted_total: null,
    socket_datagrams_received_total: null,
    response_datagrams_processed_total: null,
    socket_requested_recv_buffer_bytes: null,
    socket_effective_recv_buffer_bytes: null,
    socket_rxq_overflow_supported: null,
    socket_rxq_overflow_total: null,
    response_queue_capacity: null,
    response_queue_depth: null,
    response_queue_dropped_total: null,
    wal_retained_bytes: null,
    wal_segment_count: null,
    wal_rollovers_total: null,
    wal_durable_through_sequence: null,
    wal_warning_bytes: null,
    wal_critical_bytes: null,
    wal_hard_bytes: null,
    wal_filesystem_reserve_bytes: null,
    wal_filesystem_available_bytes: null,
    wal_warning: null,
    wal_critical: null,
    wal_hard: null,
    wal_filesystem_reserve_breached: null,
    wal_admission_blocked: null,
    updated_unix_secs: null
  };
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
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
