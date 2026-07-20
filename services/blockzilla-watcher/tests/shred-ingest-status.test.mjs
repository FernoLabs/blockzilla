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
      reserve_free_bytes: 2_147_483_648
    },
    ...overrides
  };
}

test('parses the exact public shred evidence contract', () => {
  const sample = status();
  assert.deepEqual(parseShredIngestStatus(sample), sample);
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
      state: 'unavailable'
    })
  });
  assert.ok(parseShredIngestStatus(hivezillaUnavailable));
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

test('accepts a recovered durable tail before this process writes a new shred', () => {
  const recovered = status();
  recovered.hivezilla = {
    ...recovered.hivezilla,
    state: 'waiting',
    accepted_total: 0,
    bytes_total: 0,
    last_durable_unix_secs: null
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
