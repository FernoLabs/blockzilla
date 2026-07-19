import assert from 'node:assert/strict';
import test from 'node:test';

import {
  groupLiveCaptures,
  isBenignLiveDiagnostic,
  isLiveWorkflowCapture,
  selectVisibleLiveCaptures
} from '../src/lib/live-capture-groups.ts';

function capture(id, epoch, state, overrides = {}) {
  return {
    id,
    epoch,
    state,
    updated_unix_secs: 1,
    ...overrides
  };
}

test('blocked retained folders stay out of the live workflow while failed jobs remain actionable', () => {
  const retained = capture('debug-folder', 1000, 'blocked', {
    message: 'capture is neither active nor marked ready for packaging'
  });
  const failed = capture('failed-job', 1000, 'failed');

  assert.equal(isBenignLiveDiagnostic(retained), true);
  assert.deepEqual([retained, failed].filter(isLiveWorkflowCapture).map((item) => item.id), ['failed-job']);
});

test('unsuperseded blocked workflow errors remain visible and actionable', () => {
  const collision = capture('owner-collision', 1001, 'blocked', {
    message: 'target epoch is owned by a different pipeline item'
  });

  assert.equal(isBenignLiveDiagnostic(collision), false);
  assert.equal(isLiveWorkflowCapture(collision), true);
  assert.deepEqual(
    selectVisibleLiveCaptures([collision], [collision], null).map((item) => item.id),
    ['owner-collision']
  );
});

test('nonempty epoch capture without a ready marker remains a recovery input', () => {
  const stranded = capture('epoch-1001-capture', 1001, 'blocked', {
    blocks_written: 166_463,
    message: 'capture is neither active nor marked ready for packaging'
  });

  assert.equal(isBenignLiveDiagnostic(stranded), false);
  assert.equal(isLiveWorkflowCapture(stranded), true);
});

test('a published repair bundle groups and hides its superseded same-epoch source', () => {
  const early = capture('early', 1000, 'blocked', { superseded_by: 'repair' });
  const repair = capture('repair', 1000, 'repair_required', {
    source_capture_ids: ['early', 'late']
  });
  const late = capture('late', 1001, 'repair_gate');
  const grouped = groupLiveCaptures([early, repair, late]);

  assert.deepEqual(grouped.visible.map((item) => item.id), ['repair', 'late']);
  assert.deepEqual(grouped.sourcesByBundle.get('repair')?.map((item) => item.id), ['early', 'late']);
});

for (const state of ['repair_required', 'ready_to_package', 'packaging', 'packaged']) {
  test(`repair bundle relationship persists while the bundle is ${state}`, () => {
    const source = capture('source', 1000, 'blocked', { superseded_by: 'repair' });
    const repair = capture('repair', 1000, state, {
      source_capture_ids: ['source']
    });
    const grouped = groupLiveCaptures([source, repair]);

    assert.deepEqual(grouped.visible.map((item) => item.id), ['repair']);
    assert.deepEqual(grouped.sourcesByBundle.get('repair')?.map((item) => item.id), ['source']);
  });
}

test('active, repair-gated, and cross-epoch sources fail open even with stale supersession data', () => {
  const repair = capture('repair', 1000, 'repair_required', {
    source_capture_ids: ['active', 'gate', 'tail']
  });
  const active = capture('active', 1000, 'capturing', { superseded_by: 'repair' });
  const gate = capture('gate', 1000, 'repair_gate', { superseded_by: 'repair' });
  const tail = capture('tail', 1001, 'blocked', { superseded_by: 'repair' });

  assert.deepEqual(
    groupLiveCaptures([repair, active, gate, tail]).visible.map((item) => item.id),
    ['repair', 'active', 'gate', 'tail']
  );
});

test('an unrecognized or inconsistent bundle relationship hides nothing', () => {
  const source = capture('source', 1000, 'blocked', { superseded_by: 'missing' });
  const wrong = capture('wrong', 1000, 'repair_required', { source_capture_ids: [] });
  assert.deepEqual(
    groupLiveCaptures([source, wrong]).visible.map((item) => item.id),
    ['source', 'wrong']
  );
});

test('active and repair-required workflows for one epoch are both visible', () => {
  const active = capture('active', 1001, 'capturing', { is_current: true });
  const gate = capture('gate', 1001, 'repair_gate');
  const primary = [active];

  assert.deepEqual(
    selectVisibleLiveCaptures(primary, [active, gate], active).map((item) => item.id),
    ['active', 'gate']
  );
});

test('superseded blocked sources cannot return through panel selection', () => {
  const repair = capture('repair', 1000, 'repair_required', {
    source_capture_ids: ['source']
  });
  const source = capture('source', 1000, 'blocked', { superseded_by: 'repair' });
  const grouped = groupLiveCaptures([source, repair]);

  assert.deepEqual(
    selectVisibleLiveCaptures([repair], grouped.visible, null).map((item) => item.id),
    ['repair']
  );
});
