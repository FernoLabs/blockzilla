import test from "node:test";
import assert from "node:assert/strict";
import { GIB, heartbeat, issues, progress, transition, REQUIRED_SERVICES } from "../src/model.ts";
import type { Heartbeat, Incident } from "../src/model.ts";

const now = 1_791_800_000_000;
function healthy(at = now): Heartbeat {
  return {
    schema: "blockzilla-foundation-heartbeat-v1", machine_id: "foundation", sampled_unix_ms: at, monotonic_ms: at,
    boot_id: "11111111-1111-1111-1111-111111111111", free_disk_bytes: 750 * GIB,
    capture_reserve_bytes: 400 * GIB, mem_available_bytes: 200 * GIB,
    capture: { session: "0".repeat(32) + "-1", state: "CAPTURING", durable_sequence: 1000, latest_rooted_slot: 200, updated_unix_ms: null },
    compact: { state: "RUNNING", last_compact_slot: 200, event_sequence: 500, source_sequence: 1000, updated_unix_ms: at },
    publisher: { state: "RUNNING", sequence: 450, updated_unix_ms: at },
    cold_storage: { state: "RUNNING", retired_banks: 100, updated_unix_ms: at },
    services: Object.fromEntries(REQUIRED_SERVICES.map(s => [s, "active"])),
    rpc: { healthy: true, finalized_slot: 200 }, errors: [],
  };
}
const initial = (): Incident => ({ active: [], candidate: [], candidateSinceMs: now });

test("healthy sample validates and remains healthy", () => {
  const sample = heartbeat(healthy(), now);
  assert.deepEqual(issues(sample, now, now, progress(null, sample, now), null, now), []);
});
test("machine silence alerts at the external deadline and deduplicates", () => {
  const sample = healthy();
  assert.deepEqual(issues(sample, now, now, null, null, now + 89_999), []);
  const faults = issues(sample, now, now, null, null, now + 90_000);
  assert.deepEqual(faults, ["machine_heartbeat_missing"]);
  const first = transition(initial(), faults, now + 90_000);
  assert.equal(first.event?.kind, "failure");
  assert.equal(transition(first.incident, faults, now + 120_000).event, null);
});
test("short pipeline failure does not notify, persistent failure does", () => {
  const first = transition(initial(), ["capture_not_running"], now);
  assert.equal(first.event, null);
  assert.equal(transition(first.incident, ["capture_not_running"], now + 59_999).event, null);
  const persistent = transition(first.incident, ["capture_not_running"], now + 60_000);
  assert.equal(persistent.event?.kind, "failure");
  const recovery = transition(persistent.incident, [], now + 61_000);
  assert.equal(recovery.event, null);
  assert.equal(transition(recovery.incident, [], now + 121_000).event?.kind, "recovery");
});
test("an alternating stream error cannot hide a persistent capture failure", () => {
  let incident = initial();
  const events = [];
  for (const elapsed of [0, 30_000, 60_000, 90_000, 120_000, 150_000]) {
    const faults = ["capture_not_running"];
    if ((elapsed / 30_000) % 2) faults.push("public_stream_unreachable");
    const result = transition(incident, faults, now + elapsed);
    incident = result.incident;
    if (result.event) events.push(result.event);
  }
  assert.deepEqual(events, [{ kind: "failure", issues: ["capture_not_running"], createdMs: now + 60_000 }]);
});
test("new issues have their own confirmation period and recovery needs stable all-clear", () => {
  let result = transition(initial(), ["capture_not_running"], now);
  result = transition(result.incident, ["capture_not_running"], now + 60_000);
  assert.equal(result.event?.kind, "failure");
  result = transition(result.incident, ["capture_not_running", "public_stream_unreachable"], now + 70_000);
  assert.equal(result.event, null);
  result = transition(result.incident, ["capture_not_running", "public_stream_unreachable"], now + 129_999);
  assert.equal(result.event, null);
  result = transition(result.incident, ["capture_not_running", "public_stream_unreachable"], now + 130_000);
  assert.deepEqual(result.event?.issues, ["capture_not_running", "public_stream_unreachable"]);
  result = transition(result.incident, ["capture_not_running"], now + 140_000);
  assert.equal(result.event, null); // Removing one fault is not another failure.
  result = transition(result.incident, [], now + 150_000);
  assert.equal(result.event, null);
  result = transition(result.incident, ["public_stream_unreachable"], now + 180_000);
  assert.equal(result.event, null);
  result = transition(result.incident, [], now + 190_000);
  assert.equal(result.event, null);
  result = transition(result.incident, [], now + 249_999);
  assert.equal(result.event, null);
  result = transition(result.incident, [], now + 250_000);
  assert.equal(result.event?.kind, "recovery");
  assert.equal(transition(result.incident, [], now + 280_000).event, null);
});
test("saved incidents without per-issue timers keep their pending confirmation", () => {
  const saved = { active: [], candidate: ["capture_not_running"], candidateSinceMs: now };
  const result = transition(saved, ["capture_not_running", "public_stream_unreachable"], now + 60_000);
  assert.deepEqual(result.event?.issues, ["capture_not_running"]);
  assert.equal(result.incident.issueSinceMs?.public_stream_unreachable, now + 60_000);
});
test("worker heartbeat cannot hide frozen capture counters", () => {
  const sample = healthy();
  const counters = progress(null, sample, now);
  const refreshed = healthy(now + 91_000);
  const result = issues(refreshed, now + 91_000, now, progress(counters, refreshed, now + 91_000), null, now + 91_000);
  assert.deepEqual(result, ["capture_stalled"]);
});
test("counter regression does not refresh progress", () => {
  const prior = progress(null, healthy(), now);
  const sample = healthy(now + 70_000);
  sample.capture.durable_sequence = 900;
  const next = progress(prior, sample, now + 70_000);
  assert.equal(next.captureSequence, 1000);
  assert.equal(next.captureSequenceChangedMs, now);
  assert.ok(issues(sample, now + 70_000, now, next, null, now + 70_000).includes("capture_counter_regressed"));
});
test("upload stalls, disk headroom and source mismatches have separate alerts", () => {
  const prior = progress(null, healthy(), now);
  const sample = healthy(now + 301_000);
  sample.capture.durable_sequence = 1001;
  sample.capture.latest_rooted_slot = 201;
  sample.free_disk_bytes = 499 * GIB;
  sample.errors.push("source_active_capture_mismatch");
  const result = issues(sample, now + 301_000, now, progress(prior, sample, now + 301_000), null, now + 301_000);
  assert.deepEqual(result, ["cloud_upload_stalled", "disk_headroom_low", "telemetry:source_active_capture_mismatch"]);
});
test("partial telemetry still validates as a machine heartbeat", () => {
  const sample = healthy();
  sample.capture.state = null;
  sample.capture.durable_sequence = null;
  sample.errors.push("capture_unavailable");
  assert.equal(heartbeat(sample, now).machine_id, "foundation");
  assert.ok(issues(sample, now, now, null, null, now).includes("capture_not_running"));
});
test("wrong machine, bad clocks and unsafe error text are rejected", () => {
  assert.throws(() => heartbeat({ ...healthy(), machine_id: "other" }, now));
  assert.throws(() => heartbeat({ ...healthy(), monotonic_ms: -1 }, now));
  assert.throws(() => heartbeat({ ...healthy(), free_disk_bytes: Number.POSITIVE_INFINITY }, now));
  assert.throws(() => heartbeat({ ...healthy(), errors: ["https://example.com?token=private"] }, now));
});
