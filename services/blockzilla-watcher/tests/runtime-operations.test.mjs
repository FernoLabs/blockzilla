import assert from 'node:assert/strict';
import test from 'node:test';

import {
  parseRuntimeOperations,
  runtimeOperationsIsFresh
} from '../src/lib/runtime-operations.ts';

function status(overrides = {}) {
  return {
    schema_version: 1,
    updated_unix_secs: 2_000,
    live_capture: {
      state: 'capturing',
      mode: 'raw_wal',
      source: 'grpc',
      pid: 42,
      epoch: 1003,
      last_slot: 433_719_121,
      blocks_written: 7_000,
      bytes_written: 8_000,
      write_mib_per_sec: 12.5,
      started_unix_secs: 1_000,
      updated_unix_secs: 2_000
    },
    jobs: [{
      id: 'car-verify-epoch-1001',
      kind: 'car_verify',
      epoch: 1001,
      phase: 'checksum',
      state: 'running',
      pid: 43,
      bytes_done: 500,
      bytes_total: 1_000,
      progress_pct: 50,
      read_mib_per_sec: 200,
      write_mib_per_sec: 0,
      eta_secs: 3,
      rss_bytes: 4096,
      started_unix_secs: 1_500,
      updated_unix_secs: 2_000
    }],
    process_io: {
      state: 'ready',
      sampled_unix_secs: 2_000,
      sample_window_secs: 5,
      active_count: 1,
      inaccessible_count: 0,
      truncated: false,
      processes: [{
        id: '43-100',
        pid: 43,
        name: 'sha256sum',
        read_mib_per_sec: 200,
        write_mib_per_sec: 0,
        cpu_percent: 30,
        rss_bytes: 4096,
        blockzilla_owned: false
      }]
    },
    ...overrides
  };
}

test('parses raw capture, maintenance jobs, and process telemetry', () => {
  assert.deepEqual(parseRuntimeOperations(status()), status());
});

test('rejects mismatched capture epochs, duplicate jobs, and invalid progress', () => {
  const mismatched = status();
  mismatched.live_capture = { ...mismatched.live_capture, epoch: 1002 };
  assert.equal(parseRuntimeOperations(mismatched), null);

  const duplicate = status();
  duplicate.jobs = [duplicate.jobs[0], { ...duplicate.jobs[0] }];
  assert.equal(parseRuntimeOperations(duplicate), null);

  const invalidProgress = status();
  invalidProgress.jobs = [{ ...invalidProgress.jobs[0], progress_pct: 101 }];
  assert.equal(parseRuntimeOperations(invalidProgress), null);
});

test('uses a bounded freshness window', () => {
  const parsed = parseRuntimeOperations(status());
  assert.ok(parsed);
  assert.equal(runtimeOperationsIsFresh(parsed, 2_020), true);
  assert.equal(runtimeOperationsIsFresh(parsed, 2_021), false);
});
