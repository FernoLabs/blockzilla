import assert from 'node:assert/strict';
import test from 'node:test';

import { parseRawShredStatus } from '../src/lib/raw-shred-status.ts';

function status(overrides = {}) {
  return {
    schema_version: 1,
    updated_unix_secs: 2_000,
    hivezilla: {
      availability: 'available',
      status_fresh: true,
      state: 'receiving',
      latest_slot: 434_143_453,
      spool_bytes: 12_000_000_000,
      spool_max_bytes: 32_000_000_000,
      filesystem_free_bytes: 4_600_000_000,
      filesystem_total_bytes: 38_000_000_000,
      reserve_free_bytes: 2_147_483_648,
      accepted_total: 54_601,
      invalid_total: 0
    },
    ...overrides
  };
}

test('parses bounded raw-shred recorder telemetry', () => {
  assert.deepEqual(parseRawShredStatus(status()), status());
});

test('rejects contradictory storage and partial unavailable telemetry', () => {
  const noSpace = status();
  noSpace.hivezilla = { ...noSpace.hivezilla, filesystem_free_bytes: 40_000_000_000 };
  assert.equal(parseRawShredStatus(noSpace), null);

  const unavailable = status({ hivezilla: { ...status().hivezilla, availability: 'unavailable', state: 'unavailable', latest_slot: null } });
  assert.equal(parseRawShredStatus(unavailable), null);
});
