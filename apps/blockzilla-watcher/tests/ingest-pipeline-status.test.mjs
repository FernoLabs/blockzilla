import assert from 'node:assert/strict';
import test from 'node:test';

import {
  ingestPipelineStatusIsFresh,
  parseIngestPipelineStatus
} from '../src/lib/ingest-pipeline-status.ts';

function status(overrides = {}) {
  return {
    schema_version: 1,
    updated_unix_secs: 2_000,
    overall_state: 'degraded',
    upstream: {
      state: 'connected',
      updated_unix_secs: 2_000,
      reconnects_1h: 0
    },
    recorder: {
      state: 'recording',
      durable_slot: 433_735_944,
      updated_unix_secs: 2_000,
      active_bytes: 181_372_434,
      sealed_generations: 0,
      unacknowledged_bytes: 181_372_434,
      disk_free_bytes: 2_895_732_736,
      disk_total_bytes: 5_368_709_120
    },
    replication: {
      state: 'caught_up',
      ack_through_sequence: 561_885,
      ack_slot: 433_735_944,
      updated_unix_secs: 2_000,
      lag_records: 0
    },
    indexer: {
      state: 'unavailable',
      last_slot: null,
      updated_unix_secs: null,
      lag_slots: null
    },
    object_store: {
      state: 'standby',
      provider: 'r2',
      committed_bytes: 0,
      pending_bytes: 0,
      updated_unix_secs: 1_990
    },
    fallback: {
      state: 'capturing',
      last_slot: 433_735_939,
      updated_unix_secs: 2_000,
      lag_slots: 5
    },
    gaps: [{
      from_slot: 433_728_271,
      to_slot: 433_731_796,
      produced_blocks: 3_526,
      coverage: 'rpc_recoverable'
    }],
    gaps_truncated: false,
    incidents: [],
    ...overrides
  };
}

test('parses a complete secret-free ingest status document', () => {
  assert.deepEqual(parseIngestPipelineStatus(status()), status());
  const withoutReconnectCounter = status();
  withoutReconnectCounter.upstream = { ...withoutReconnectCounter.upstream, reconnects_1h: null };
  assert.deepEqual(parseIngestPipelineStatus(withoutReconnectCounter), withoutReconnectCounter);
});

test('rejects invalid enums, impossible storage, and unbounded arrays', () => {
  assert.equal(parseIngestPipelineStatus(status({ overall_state: 'excellent' })), null);

  const impossibleStorage = status();
  impossibleStorage.recorder = {
    ...impossibleStorage.recorder,
    disk_free_bytes: impossibleStorage.recorder.disk_total_bytes + 1
  };
  assert.equal(parseIngestPipelineStatus(impossibleStorage), null);

  assert.equal(parseIngestPipelineStatus(status({ gaps: Array.from({ length: 33 }, () => status().gaps[0]) })), null);
});

test('rejects duplicate incidents and backwards resolution times', () => {
  const incident = {
    id: 'grpc_stale',
    severity: 'error',
    started_unix_secs: 1_900,
    resolved_unix_secs: null
  };
  assert.equal(parseIngestPipelineStatus(status({ incidents: [incident, incident] })), null);
  assert.equal(parseIngestPipelineStatus(status({
    incidents: [{ ...incident, resolved_unix_secs: 1_899 }]
  })), null);
  assert.equal(parseIngestPipelineStatus(status({
    incidents: [{ ...incident, id: 'secret-from-upstream' }]
  })), null);
});

test('rejects contradictory stage evidence and duplicate gap ranges', () => {
  const contradictoryAck = status();
  contradictoryAck.replication = { ...contradictoryAck.replication, lag_records: 2 };
  assert.equal(parseIngestPipelineStatus(contradictoryAck), null);

  const contradictoryIndexer = status();
  contradictoryIndexer.indexer = {
    ...contradictoryIndexer.indexer,
    state: 'indexing',
    last_slot: null
  };
  assert.equal(parseIngestPipelineStatus(contradictoryIndexer), null);

  const duplicateGap = status().gaps[0];
  assert.equal(parseIngestPipelineStatus(status({ gaps: [duplicateGap, duplicateGap] })), null);
});

test('requires overall state to reflect continuity and active incidents', () => {
  assert.equal(parseIngestPipelineStatus(status({ overall_state: 'healthy' })), null);
  assert.ok(parseIngestPipelineStatus(status()));

  const unproven = status({
    overall_state: 'failed',
    gaps: [{ ...status().gaps[0], coverage: 'unproven' }]
  });
  assert.ok(parseIngestPipelineStatus(unproven));
  assert.equal(parseIngestPipelineStatus({ ...unproven, overall_state: 'degraded' }), null);
});

test('uses a bounded freshness window', () => {
  const parsed = parseIngestPipelineStatus(status());
  assert.ok(parsed);
  assert.equal(ingestPipelineStatusIsFresh(parsed, 2_030), true);
  assert.equal(ingestPipelineStatusIsFresh(parsed, 2_031), false);
  assert.equal(ingestPipelineStatusIsFresh(parsed, 1_994), false);
  assert.equal(ingestPipelineStatusIsFresh(parsed, 1_995), true);
});
