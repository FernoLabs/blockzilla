import test from "node:test";
import assert from "node:assert/strict";
import { heartbeat, REQUIRED_SERVICES } from "../src/model.ts";
import { telegramStatus, telegramCommand } from "../src/telegram.ts";
import type { Heartbeat, ArchiveStatus } from "../src/model.ts";

const now = 1_791_860_000_000;
function sample(): Heartbeat {
  return { schema: "blockzilla-foundation-heartbeat-v1", machine_id: "foundation", sampled_unix_ms: now, monotonic_ms: 1000,
    boot_id: "11111111-1111-1111-1111-111111111111", free_disk_bytes: 800e9, capture_reserve_bytes: 400e9, mem_available_bytes: 200e9,
    capture: { session: "0".repeat(32) + "-1", state: "CAPTURING", durable_sequence: 1000, latest_rooted_slot: 200, updated_unix_ms: null },
    compact: { state: "RUNNING", last_compact_slot: 200, event_sequence: 500, source_sequence: 1000, updated_unix_ms: now },
    publisher: { state: "RUNNING", sequence: 450, updated_unix_ms: now },
    cold_storage: { state: "RUNNING", retired_banks: 100, updated_unix_ms: now },
    services: Object.fromEntries(REQUIRED_SERVICES.map(name => [name, "active"])), rpc: { healthy: true, finalized_slot: 200 }, errors: [] };
}
function archive(): ArchiveStatus {
  return { state: "REGISTRIES", epoch: 1051, updated_unix_ms: now, stage_updated_unix_ms: now - 5e5,
    progress_updated_unix_ms: now,
    progress_percent: 82, bytes_per_second: null, eta_seconds: 205, eta_scope: "stage",
    paused: false, epoch_eta_min_seconds: 3600, epoch_eta_max_seconds: 4500, queue_eta_min_seconds: 21600, queue_eta_max_seconds: 28800,
    last_completed_epoch: 1050, last_compact_bytes: 112e9, completed_epochs: [1048, 1049, 1050], upload_queue_epochs: [1051, 1052, 1053, 1054] };
}
function snapshot(report: Heartbeat | null = sample()) {
  return { armed: report !== null, sample: report, startedMs: now, lastReceivedMs: now, lastCheckMs: now, counters: null, streamIssue: null };
}

test("archive telemetry is optional and validated without accepting unbounded fields", () => {
  assert.equal(heartbeat(sample(), now).archive, undefined);
  const report = { ...sample(), archive: archive() };
  assert.deepEqual(heartbeat(report, now).archive, report.archive);
  for (const changes of [
    { epoch: 100_001 }, { completed_epochs: Array(65).fill(1050) }, { upload_queue_epochs: [null] },
    { progress_percent: 101 }, { bytes_per_second: Infinity }, { eta_seconds: -1 }, { eta_scope: "whole_plan" },
    { state: "token=private" }, { updated_unix_ms: 1.5 }, { paused: "yes" },
    { epoch_eta_min_seconds: 4501 }, { queue_eta_max_seconds: -1 }, { progress_updated_unix_ms: -1 },
  ]) assert.throws(() => heartbeat({ ...report, archive: { ...archive(), ...changes } }, now));
});

test("fresh archive samples cannot make old or unknown progress current", () => {
  for (const progressTime of [now - 120_001, null, undefined, now + 60_001]) {
    const report = { ...sample(), archive: { ...archive(), progress_updated_unix_ms: progressTime, bytes_per_second: 250e6 } };
    const parsed = heartbeat(report, now);
    if (progressTime === undefined) assert.equal(parsed.archive?.progress_updated_unix_ms, null);
    const text = telegramStatus(snapshot(parsed), "Megaloadon", now);
    assert.ok(text.includes("Archive: epoch 1051, REGISTRIES; report 0 seconds ago"));
    assert.ok(text.includes("Archive progress: unavailable (no recent matching stage report)."));
    assert.ok(text.includes("Archive ETA: unavailable for the current stage."));
    assert.equal(text.includes("(82%)"), false);
    assert.equal(text.includes("Archive transfer rate"), false);
    assert.ok(text.includes("Last complete archive epoch: 1050; compact size 112.0 GB"));
    assert.ok(text.includes("Archive epochs left: 1051, 1052, 1053, 1054"));
  }
});

test("archive ETA identifies only its current stage and preserves later work", () => {
  const text = telegramStatus(snapshot({ ...sample(), archive: archive() }), "Megaloadon", now);
  assert.ok(text.includes("Archive: epoch 1051, REGISTRIES (82%)"));
  assert.ok(text.includes("Archive ETA: 3 minutes 25 seconds (this stage only). Later stages are not included."));
  assert.ok(text.includes("Last complete archive epoch: 1050; compact size 112.0 GB"));
  assert.ok(text.includes("Archive epochs left: 1051, 1052, 1053, 1054"));
  assert.ok(text.includes("Epoch upload estimate: 1 hour to 1 hour 15 minutes (recent runs; can pause for live work)."));
  assert.ok(text.includes("Archive plan estimate: 6 hours to 8 hours (recent runs; can pause for live work)."));
  assert.equal(text.includes("STALE report"), false); // A long stage is allowed; fresh sample collection matters.
});

test("command dates use Paris time with the applicable daylight offset", () => {
  for (const [utc, local] of [["2026-10-13T04:05:06Z", "13 Oct 2026 06:05:06 (Paris)"],
    ["2026-12-13T04:05:06Z", "13 Dec 2026 05:05:06 (Paris)"]]) {
    const at = Date.parse(utc), report = sample(); report.sampled_unix_ms = at;
    const text = telegramStatus({ ...snapshot(report), lastReceivedMs: at }, "Megaloadon", at);
    assert.ok(text.startsWith("Megaloadon status at " + local + "\n"));
    assert.ok(text.includes("Machine sample: " + local + "\n"));
    assert.equal(report.sampled_unix_ms, at);
  }
});

test("history estimates round outward while stage ETA keeps its reported seconds", () => {
  const report = { ...sample(), archive: { ...archive(), epoch_eta_min_seconds: 32 * 60 + 38,
    epoch_eta_max_seconds: 47 * 60 + 45, queue_eta_min_seconds: 5 * 3600 + 9 * 60,
    queue_eta_max_seconds: 7 * 3600 + 35 * 60 } };
  const text = telegramStatus(snapshot(report), "Megaloadon", now);
  assert.ok(text.includes("Epoch upload estimate: 30 minutes to 50 minutes (recent runs; can pause for live work)."));
  assert.ok(text.includes("Archive plan estimate: 5 hours to 7 hours 45 minutes (recent runs; can pause for live work)."));
  assert.ok(text.includes("Archive ETA: 3 minutes 25 seconds (this stage only)."));
});

test("paused archive jobs do not present a running ETA", () => {
  const text = telegramStatus(snapshot({ ...sample(), archive: { ...archive(), paused: true } }), "Megaloadon", now);
  assert.ok(text.includes("PAUSED for live work. ETA is unavailable."));
  assert.ok(text.includes("Archive ETA: unavailable for the current stage."));
  assert.equal(text.includes("Epoch upload estimate"), false);
  assert.equal(text.includes("Archive plan estimate"), false);
});

test("missing heartbeat and stale archive reports never advertise a current ETA", () => {
  const report = { ...sample(), archive: archive() };
  for (const state of [snapshot(report), snapshot({ ...report, archive: { ...archive(), updated_unix_ms: now - 120_001 } })]) {
    const at = state.sample?.archive?.updated_unix_ms === now ? now + 90_000 : now;
    const text = telegramStatus(state, "Megaloadon", at);
    assert.ok(text.includes("Archive ETA: unavailable for the current stage."));
    assert.equal(text.includes("(82%)"), false);
    if (at !== now) {
      assert.ok(text.includes("Heartbeat: MISSING"));
      assert.ok(text.includes("the last received values"));
      assert.ok(text.includes("Checks: machine_heartbeat_missing"));
    } else assert.ok(text.includes("STALE report"));
  }
  assert.ok(telegramStatus(snapshot(null), "Megaloadon", now).includes("Status and ETA are unavailable"));
});

test("fresh heartbeats cannot conceal old compaction reports in command replies", () => {
  const report = sample(); report.compact.updated_unix_ms = now - 120_001;
  const text = telegramStatus(snapshot(report), "Megaloadon", now);
  assert.ok(text.includes("Live compaction: RUNNING; slot 200; lag 0 slots; STALE report"));
  assert.ok(text.includes("compaction_status_stale"));
});

test("commands require a safe numeric sender and destination, with exact bot addressing", () => {
  const update = { update_id: 1, message: { message_id: 2, text: "/status@Megaloadon_alerts_bot",
    chat: { id: 123456789, type: "private" }, from: { id: 123456789, is_bot: false } } };
  assert.equal(telegramCommand(update, "123456789", "megaloadon_alerts_bot")?.command, "status");
  for (const recipient of [undefined, "@username", "-123456789", "9007199254740992"]) assert.equal(telegramCommand(update, recipient, "megaloadon_alerts_bot"), null);
  assert.equal(telegramCommand({ ...update, message: { ...update.message, text: "/status@another_bot" } }, "123456789", "megaloadon_alerts_bot"), null);
});
