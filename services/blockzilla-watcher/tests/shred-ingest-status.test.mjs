import assert from 'node:assert/strict';
import test from 'node:test';

import {
  parseShredIngestStatus,
  shredIngestStatusIsFresh
} from '../src/lib/shred-ingest-status.ts';

function status(overrides = {}) {
  return {
    schema_version: 1,
    updated_unix_secs: 2_000,
    gossip: {
      state: 'observed',
      recent_peer_count: 37,
      known_peer_count: 2_000,
      tvu_peer_count: 1_500,
      shred_version: 50_093,
      receiver_uptime_secs: 900,
      updated_unix_secs: 2_000
    },
    tvu: {
      state: 'receiving',
      packets_total: 1_000,
      bytes_total: 1_228_800,
      parsed_total: 990,
      invalid_total: 3,
      version_mismatch_total: 7,
      unique_total: 800,
      duplicates_total: 190,
      data_total: 700,
      code_total: 290,
      socket_rxq_overflow_supported: true,
      socket_rxq_overflow_total: 0,
      latest_slot: 433_735_944,
      seconds_since_last_packet: 2,
      updated_unix_secs: 2_000
    },
    forwarding: {
      state: 'errors',
      target_count: 1,
      attempts_total: 990,
      successful_datagrams_total: 985,
      errors_total: 5,
      updated_unix_secs: 2_000
    },
    repair: {
      availability: 'available',
      enabled: true,
      active: true,
      state: 'active',
      restart_count: 1,
      seconds_since_last_success: 1,
      peers: 8,
      tracked_slots: 4,
      outstanding: 12,
      observation_queue_dropped_total: 3,
      requests_sent_total: 400,
      requests_exhausted_total: 7,
      shreds_accepted_total: 125,
      socket_datagrams_received_total: 180,
      response_datagrams_processed_total: 125,
      socket_requested_recv_buffer_bytes: 67_108_864,
      socket_effective_recv_buffer_bytes: 134_217_728,
      socket_rxq_overflow_supported: true,
      socket_rxq_overflow_total: 0,
      response_queue_capacity: 2_048,
      response_queue_depth: 3,
      response_queue_dropped_total: 0,
      wal_retained_bytes: 300_000_000,
      wal_segment_count: 2,
      wal_rollovers_total: 1,
      wal_durable_through_sequence: 199_999,
      wal_warning_bytes: 1_073_741_824,
      wal_critical_bytes: 2_147_483_648,
      wal_hard_bytes: 4_294_967_296,
      wal_filesystem_reserve_bytes: 8_589_934_592,
      wal_filesystem_available_bytes: 12_884_901_888,
      wal_warning: false,
      wal_critical: false,
      wal_hard: false,
      wal_filesystem_reserve_breached: false,
      wal_admission_blocked: false,
      updated_unix_secs: 2_000
    },
    hivezilla: {
      availability: 'available',
      status_fresh: true,
      state: 'receiving',
      updated_unix_secs: 2_000,
      started_unix_secs: 1_900,
      accepted_total: 500,
      invalid_total: 2,
      bytes_total: 614_400,
      durable_through_sequence: 7_499,
      latest_slot: 433_735_944,
      shred_version: 50_093,
      last_durable_unix_secs: 2_000,
      spool_bytes: 1_048_576,
      spool_max_bytes: 21_474_836_480,
      filesystem_free_bytes: 42_949_672_960,
      filesystem_total_bytes: 64_424_509_440,
      reserve_free_bytes: 2_147_483_648,
      udp_received_total: 503,
      udp_received_bytes_total: 618_096,
      ingest_queue_depth_events: 3,
      ingest_queue_depth_bytes: 3_696,
      ingest_queue_high_water_events: 64,
      ingest_queue_high_water_bytes: 78_848,
      ingest_queue_capacity_events: 16_384,
      ingest_queue_capacity_bytes: 67_108_864,
      ingest_queue_backpressure_events_total: 0,
      ingest_queue_backpressure_micros_total: 0,
      ingest_queue_backpressured: false,
      socket_rxq_overflow_supported: true,
      socket_rxq_overflow_total: 0,
      durable_sources: {
        green: {
          committed_datagrams_total: 200,
          last_durable_unix_secs: 2_000,
          last_durable_sequence: 7_499,
          last_durable_slot: 433_735_944
        }
      }
    },
    ...overrides
  };
}

test('parses the exact public shred evidence contract', () => {
  const sample = status();
  assert.deepEqual(parseShredIngestStatus(sample), sample);
});

test('normalizes the exact incumbent schema during a no-gap rollout', () => {
  const currentCollector = status();
  delete currentCollector.hivezilla.durable_sources;
  const currentParsed = parseShredIngestStatus(currentCollector);
  assert.ok(currentParsed);
  assert.deepEqual(currentParsed.hivezilla.durable_sources, {});

  const incumbent = status();
  delete incumbent.repair;
  delete incumbent.tvu.socket_rxq_overflow_supported;
  delete incumbent.tvu.socket_rxq_overflow_total;
  for (const key of [
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
    'socket_rxq_overflow_total',
    'durable_sources'
  ]) {
    delete incumbent.hivezilla[key];
  }

  const parsed = parseShredIngestStatus(incumbent);
  assert.ok(parsed);
  assert.equal(parsed.tvu.socket_rxq_overflow_supported, false);
  assert.equal(parsed.tvu.socket_rxq_overflow_total, null);
  assert.equal(parsed.repair.availability, 'unavailable');
  assert.equal(parsed.hivezilla.socket_rxq_overflow_supported, false);
  assert.equal(parsed.hivezilla.socket_rxq_overflow_total, null);
  assert.equal(parsed.hivezilla.ingest_queue_capacity_events, null);

  const collectorOverlap = parseShredIngestStatus(parsed);
  assert.deepEqual(collectorOverlap, parsed);

  const partialExtendedTelemetry = structuredClone(parsed);
  partialExtendedTelemetry.hivezilla.ingest_queue_depth_events = 0;
  assert.equal(parseShredIngestStatus(partialExtendedTelemetry), null);

  const partialUpgrade = structuredClone(incumbent);
  partialUpgrade.repair = status().repair;
  assert.equal(parseShredIngestStatus(partialUpgrade), null);
});

test('rejects extra fields, unsupported states, and contradictory counters', () => {
  assert.equal(parseShredIngestStatus({ ...status(), private_endpoint: '127.0.0.1' }), null);

  const badGossip = status();
  badGossip.gossip = { ...badGossip.gossip, state: 'connected' };
  assert.equal(parseShredIngestStatus(badGossip), null);

  const badTvu = status();
  badTvu.tvu = { ...badTvu.tvu, parsed_total: 0 };
  assert.equal(parseShredIngestStatus(badTvu), null);

  const badForwarding = status();
  badForwarding.forwarding = { ...badForwarding.forwarding, attempts_total: 989 };
  assert.equal(parseShredIngestStatus(badForwarding), null);
});

test('accepts independently unavailable receiver and Hivezilla sources', () => {
  const receiverUnavailable = status({
    gossip: unavailableObject(status().gossip, { state: 'unavailable' }),
    tvu: unavailableObject(status().tvu, { state: 'unavailable' }),
    forwarding: unavailableObject(status().forwarding, { state: 'unavailable' })
  });
  assert.ok(parseShredIngestStatus(receiverUnavailable));

  const hivezillaUnavailable = status({
    hivezilla: unavailableObject(status().hivezilla, {
      availability: 'unavailable',
      status_fresh: false,
      state: 'unavailable',
      durable_sources: {}
    })
  });
  assert.ok(parseShredIngestStatus(hivezillaUnavailable));
});

test('accepts active repair and keeps unavailable repair independent from raw capture', () => {
  const active = status();
  assert.equal(parseShredIngestStatus(active)?.repair.state, 'active');

  const repairUnavailable = status({
    repair: unavailableObject(status().repair, {
      availability: 'unavailable',
      state: 'unavailable'
    })
  });
  const parsed = parseShredIngestStatus(repairUnavailable);
  assert.ok(parsed);
  assert.equal(parsed.tvu.state, 'receiving');
  assert.equal(parsed.hivezilla.state, 'receiving');
  assert.equal(parsed.repair.availability, 'unavailable');
});

test('accepts repair backoff and explicit storage pressure', () => {
  const backoff = status();
  backoff.repair = {
    ...backoff.repair,
    active: false,
    state: 'backoff',
    restart_count: 2,
    seconds_since_last_success: null
  };
  assert.equal(parseShredIngestStatus(backoff)?.repair.state, 'backoff');

  const pressure = status();
  pressure.repair = {
    ...pressure.repair,
    active: false,
    state: 'backoff',
    wal_retained_bytes: pressure.repair.wal_hard_bytes,
    wal_warning: true,
    wal_critical: true,
    wal_hard: true,
    wal_filesystem_reserve_breached: true,
    wal_admission_blocked: true
  };
  assert.equal(parseShredIngestStatus(pressure)?.repair.wal_admission_blocked, true);
});

test('keeps each socket-loss counter distinct and rejects contradictory support flags', () => {
  const sample = status();
  sample.tvu.socket_rxq_overflow_total = 7;
  sample.repair.socket_rxq_overflow_total = 2;
  sample.hivezilla.socket_rxq_overflow_total = 3;
  const parsed = parseShredIngestStatus(sample);
  assert.ok(parsed);
  assert.equal(parsed.tvu.socket_rxq_overflow_total, 7);
  assert.equal(parsed.repair.socket_rxq_overflow_total, 2);
  assert.equal(parsed.hivezilla.socket_rxq_overflow_total, 3);

  const invalid = status();
  invalid.repair = {
    ...invalid.repair,
    socket_rxq_overflow_supported: false,
    socket_rxq_overflow_total: 1
  };
  assert.equal(parseShredIngestStatus(invalid), null);
});

test('rejects private repair fields and arbitrary error text', () => {
  const leaked = status();
  leaked.repair = {
    ...leaked.repair,
    last_error: 'private peer 198.51.100.2 /data/accepted.repair.wal'
  };
  assert.equal(parseShredIngestStatus(leaked), null);
});

test('requires complete durable evidence and valid capacity bounds', () => {
  const incomplete = status();
  incomplete.hivezilla = { ...incomplete.hivezilla, last_durable_unix_secs: null };
  assert.equal(parseShredIngestStatus(incomplete), null);

  const overCapacity = status();
  overCapacity.hivezilla = {
    ...overCapacity.hivezilla,
    spool_bytes: overCapacity.hivezilla.spool_max_bytes + 1
  };
  assert.equal(parseShredIngestStatus(overCapacity), null);
});

test('validates named post-fsync Hivezilla source evidence', () => {
  const parsed = parseShredIngestStatus(status());
  assert.ok(parsed);
  assert.equal(parsed.hivezilla.durable_sources.green.committed_datagrams_total, 200);
  assert.equal(parsed.hivezilla.durable_sources.green.last_durable_slot, 433_735_944);

  const invalidName = status();
  invalidName.hivezilla.durable_sources = {
    '127.0.0.1:18104': invalidName.hivezilla.durable_sources.green
  };
  assert.equal(parseShredIngestStatus(invalidName), null);

  const incompleteTail = status();
  incompleteTail.hivezilla.durable_sources.green.last_durable_slot = null;
  assert.equal(parseShredIngestStatus(incompleteTail), null);

  const beyondTail = status();
  beyondTail.hivezilla.durable_sources.green.last_durable_sequence = 7_500;
  assert.equal(parseShredIngestStatus(beyondTail), null);

  const tooMany = status();
  tooMany.hivezilla.durable_sources = Object.fromEntries(
    ['one', 'two', 'three', 'four', 'five'].map((name) => [
      name,
      {
        committed_datagrams_total: 0,
        last_durable_unix_secs: null,
        last_durable_sequence: null,
        last_durable_slot: null
      }
    ])
  );
  assert.equal(parseShredIngestStatus(tooMany), null);

  const overAccepted = status();
  overAccepted.hivezilla.durable_sources.green.committed_datagrams_total = 501;
  assert.equal(parseShredIngestStatus(overAccepted), null);
});

test('accepts a recovered durable tail before this process writes a new shred', () => {
  const recovered = status();
  recovered.hivezilla = {
    ...recovered.hivezilla,
    state: 'waiting',
    accepted_total: 0,
    bytes_total: 0,
    last_durable_unix_secs: null,
    durable_sources: {
      green: {
        committed_datagrams_total: 0,
        last_durable_unix_secs: null,
        last_durable_sequence: null,
        last_durable_slot: null
      }
    }
  };
  assert.deepEqual(parseShredIngestStatus(recovered), recovered);
});

test('freshness depends on the publication timestamp only', () => {
  const sample = parseShredIngestStatus(status());
  assert.ok(sample);
  assert.equal(shredIngestStatusIsFresh(sample, 2_030), true);
  assert.equal(shredIngestStatusIsFresh(sample, 2_031), false);
  assert.equal(shredIngestStatusIsFresh(sample, 1_995), true);
  assert.equal(shredIngestStatusIsFresh(sample, 1_994), false);
});

function unavailableObject(value, preserved) {
  return Object.fromEntries(
    Object.keys(value).map((key) => [key, key in preserved ? preserved[key] : null])
  );
}
