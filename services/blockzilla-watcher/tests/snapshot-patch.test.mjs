import assert from 'node:assert/strict';
import test from 'node:test';

import {
  applySnapshotPatch,
  snapshotPatchSequenceAction
} from '../src/lib/snapshot-patch.ts';

function snapshot() {
  return {
    schema_version: 3,
    sequence: 7,
    now_unix_secs: 100,
    current_epoch: 1000,
    observer_mode: false,
    scheduler: { paused: false },
    inventory: { complete: true },
    scan_sweep: { complete: false },
    summary: { queued: 3 },
    machine: { load_1m: 1 },
    epochs: [
      { epoch: 3, state: 'queued' },
      { epoch: 1, state: 'complete' },
      { epoch: 2, state: 'scanning' }
    ],
    lanes: [{ id: 'old-lane' }],
    live: [{ id: 'old-live' }],
    finalizer_queue: [],
    errors: []
  };
}

function patch(overrides = {}) {
  return {
    schema_version: 3,
    sequence: 8,
    now_unix_secs: 105,
    current_epoch: 1001,
    observer_mode: true,
    scheduler: { paused: true },
    inventory: { complete: true },
    scan_sweep: { complete: true },
    summary: { queued: 1 },
    machine: { load_1m: 2 },
    epochs_changed: [
      { epoch: 2, state: 'complete' },
      { epoch: 4, state: 'queued' }
    ],
    epochs_removed: [3],
    lanes: [{ id: 'new-lane' }],
    live: [{ id: 'new-live' }],
    finalizer_queue: [{ id: 'finalizer' }],
    errors: [{ message: 'new' }],
    ...overrides
  };
}

test('queued patches at or behind the initial snapshot are ignored without a resync', () => {
  assert.equal(snapshotPatchSequenceAction(10, 9), 'ignore');
  assert.equal(snapshotPatchSequenceAction(10, 10), 'ignore');
  assert.equal(snapshotPatchSequenceAction(10, 11), 'apply');
  assert.equal(snapshotPatchSequenceAction(10, 12), 'resync');
  assert.equal(snapshotPatchSequenceAction(-1, 0), 'resync');
});

test('snapshot patches upsert and remove epochs by key in sorted order', () => {
  const base = snapshot();
  const baseEpochs = base.epochs;
  const result = applySnapshotPatch(base, patch());

  assert.notEqual(result, base);
  assert.notEqual(result.epochs, baseEpochs);
  assert.deepEqual(result.epochs, [
    { epoch: 1, state: 'complete' },
    { epoch: 2, state: 'complete' },
    { epoch: 4, state: 'queued' }
  ]);
  assert.deepEqual(base.epochs, [
    { epoch: 3, state: 'queued' },
    { epoch: 1, state: 'complete' },
    { epoch: 2, state: 'scanning' }
  ]);
});

test('snapshot patches replace every non-epoch top-level field', () => {
  const result = applySnapshotPatch(snapshot(), patch());

  assert.equal(result.sequence, 8);
  assert.equal(result.now_unix_secs, 105);
  assert.equal(result.current_epoch, 1001);
  assert.equal(result.observer_mode, true);
  assert.deepEqual(result.scheduler, { paused: true });
  assert.deepEqual(result.inventory, { complete: true });
  assert.deepEqual(result.scan_sweep, { complete: true });
  assert.deepEqual(result.summary, { queued: 1 });
  assert.deepEqual(result.machine, { load_1m: 2 });
  assert.deepEqual(result.lanes, [{ id: 'new-lane' }]);
  assert.deepEqual(result.live, [{ id: 'new-live' }]);
  assert.deepEqual(result.finalizer_queue, [{ id: 'finalizer' }]);
  assert.deepEqual(result.errors, [{ message: 'new' }]);
});

test('optional process telemetry survives omission and is replaced when supplied', () => {
  const processIo = {
    state: 'ready',
    sampled_unix_secs: 99,
    sample_window_secs: 2,
    active_count: 1,
    inaccessible_count: 0,
    truncated: false,
    processes: [{ id: '12:4', pid: 12, name: 'restic', read_mib_per_sec: 3 }]
  };
  const base = { ...snapshot(), process_io: processIo };

  assert.equal(applySnapshotPatch(base, patch()).process_io, processIo);

  const replacement = { ...processIo, sampled_unix_secs: 104, processes: [] };
  assert.equal(applySnapshotPatch(base, patch({ process_io: replacement })).process_io, replacement);
});

test('optional compaction history survives omission and is replaced when supplied', () => {
  const history = [{
    id: 'historical-831-1',
    epoch: 831,
    workflow: 'historical',
    completed_unix_secs: 98,
    duration_secs: 3600
  }];
  const base = { ...snapshot(), recent_compactions: history };

  assert.equal(applySnapshotPatch(base, patch()).recent_compactions, history);

  const replacement = [{ ...history[0], id: 'historical-832-1', epoch: 832 }];
  assert.equal(
    applySnapshotPatch(base, patch({ recent_compactions: replacement })).recent_compactions,
    replacement
  );
});
