var __defProp = Object.defineProperty;
var __name = (target, value2) => __defProp(target, "name", { value: value2, configurable: true });

// src/index.ts
import { DurableObject } from "cloudflare:workers";
import { timingSafeEqual } from "node:crypto";

// src/model.ts
var HEARTBEAT_TIMEOUT_MS = 9e4;
var ISSUE_CONFIRM_MS = 6e4;
var GIB = 1024 ** 3;
var REQUIRED_SERVICES = [
  "hivezilla-validator.service",
  "hivezilla-direct-worker.service",
  "hivezilla-bank-v2-r2-publisher.service",
  "hivezilla-native-cold-storage-20261012-live.service",
  "hivezilla-live-priority-20261012.service",
  "hivezilla-bank-v2-producer.service"
];
function object(value2) {
  if (!value2 || typeof value2 !== "object" || Array.isArray(value2)) throw new Error("invalid_object");
  return value2;
}
__name(object, "object");
function number(value2) {
  if (value2 === null) return null;
  if (typeof value2 !== "number" || !Number.isSafeInteger(value2) || value2 < 0) throw new Error("invalid_number");
  return value2;
}
__name(number, "number");
function text(value2) {
  if (value2 === null) return null;
  if (typeof value2 !== "string" || value2.length > 120 || !/^[a-zA-Z0-9_.:-]+$/.test(value2)) throw new Error("invalid_text");
  return value2;
}
__name(text, "text");
function decimal(value2, maximum) {
  if (value2 === null) return null;
  if (typeof value2 !== "number" || !Number.isFinite(value2) || value2 < 0 || value2 > maximum) throw new Error("invalid_decimal");
  return value2;
}
__name(decimal, "decimal");
function epoch(value2) {
  const result = number(value2);
  if (result !== null && result > 1e5) throw new Error("invalid_epoch");
  return result;
}
__name(epoch, "epoch");
function epochList(value2) {
  if (!Array.isArray(value2) || value2.length > 64) throw new Error("invalid_epoch_list");
  return [...new Set(value2.map((item) => {
    const parsed = epoch(item);
    if (parsed === null) throw new Error("invalid_epoch_list");
    return parsed;
  }))].sort((a, b) => a - b);
}
__name(epochList, "epochList");
function archiveStatus(value2) {
  const v = object(value2);
  if (v.eta_scope !== null && !["download", "upload", "stage"].includes(String(v.eta_scope))) throw new Error("invalid_eta_scope");
  if (v.paused !== void 0 && v.paused !== null && typeof v.paused !== "boolean") throw new Error("invalid_archive_paused");
  const range = /* @__PURE__ */ __name((minimum, maximum) => {
    const min = decimal(minimum ?? null, 365 * 24 * 60 * 60), max = decimal(maximum ?? null, 365 * 24 * 60 * 60);
    if (min !== null && max !== null && min > max) throw new Error("invalid_eta_range");
    return [min, max];
  }, "range");
  const [epochMin, epochMax] = range(v.epoch_eta_min_seconds, v.epoch_eta_max_seconds);
  const [queueMin, queueMax] = range(v.queue_eta_min_seconds, v.queue_eta_max_seconds);
  return {
    state: text(v.state),
    epoch: epoch(v.epoch),
    updated_unix_ms: number(v.updated_unix_ms),
    stage_updated_unix_ms: number(v.stage_updated_unix_ms),
    progress_updated_unix_ms: number(v.progress_updated_unix_ms ?? null),
    progress_percent: decimal(v.progress_percent, 100),
    bytes_per_second: decimal(v.bytes_per_second, 1e12),
    eta_seconds: decimal(v.eta_seconds, 365 * 24 * 60 * 60),
    eta_scope: v.eta_scope,
    paused: v.paused === void 0 ? null : v.paused,
    epoch_eta_min_seconds: epochMin,
    epoch_eta_max_seconds: epochMax,
    queue_eta_min_seconds: queueMin,
    queue_eta_max_seconds: queueMax,
    last_completed_epoch: epoch(v.last_completed_epoch),
    last_compact_bytes: number(v.last_compact_bytes),
    completed_epochs: epochList(v.completed_epochs),
    upload_queue_epochs: epochList(v.upload_queue_epochs)
  };
}
__name(archiveStatus, "archiveStatus");
function heartbeat(value2, now) {
  const v = object(value2);
  if (v.schema !== "blockzilla-foundation-heartbeat-v1" || v.machine_id !== "foundation") throw new Error("invalid_schema");
  const sampled = number(v.sampled_unix_ms);
  const monotonic = number(v.monotonic_ms);
  if (sampled === null || monotonic === null) throw new Error("invalid_sample_clock");
  const c = object(v.capture), w = object(v.compact), p = object(v.publisher), s = object(v.cold_storage), rpc = object(v.rpc);
  const services = {};
  const incoming = object(v.services);
  for (const name of REQUIRED_SERVICES) services[name] = text(incoming[name]);
  if (!Array.isArray(v.errors) || v.errors.length > 40 || v.errors.some((x) => typeof x !== "string" || !/^[a-z0-9_]{1,80}$/.test(x))) throw new Error("invalid_errors");
  const session = text(c.session);
  if (session !== null && !/^[0-9a-f]{32}-[0-9a-f]+$/.test(session)) throw new Error("invalid_session");
  if (rpc.healthy !== null && typeof rpc.healthy !== "boolean") throw new Error("invalid_rpc_health");
  return {
    schema: "blockzilla-foundation-heartbeat-v1",
    machine_id: "foundation",
    sampled_unix_ms: sampled,
    monotonic_ms: monotonic,
    boot_id: text(v.boot_id),
    free_disk_bytes: number(v.free_disk_bytes),
    capture_reserve_bytes: number(v.capture_reserve_bytes),
    mem_available_bytes: number(v.mem_available_bytes),
    capture: {
      session,
      state: text(c.state),
      durable_sequence: number(c.durable_sequence),
      latest_rooted_slot: number(c.latest_rooted_slot),
      updated_unix_ms: number(c.updated_unix_ms)
    },
    compact: {
      state: text(w.state),
      last_compact_slot: number(w.last_compact_slot),
      event_sequence: number(w.event_sequence),
      source_sequence: number(w.source_sequence),
      updated_unix_ms: number(w.updated_unix_ms)
    },
    publisher: { state: text(p.state), sequence: number(p.sequence), updated_unix_ms: number(p.updated_unix_ms) },
    cold_storage: { state: text(s.state), retired_banks: number(s.retired_banks), updated_unix_ms: number(s.updated_unix_ms) },
    services,
    rpc: { healthy: rpc.healthy, finalized_slot: number(rpc.finalized_slot) },
    errors: [...new Set(v.errors)].sort(),
    ...v.archive === void 0 ? {} : { archive: archiveStatus(v.archive) }
  };
}
__name(heartbeat, "heartbeat");
function progress(previous, sample, now) {
  const values = [sample.capture.durable_sequence, sample.capture.latest_rooted_slot, sample.publisher.sequence];
  if (!previous || previous.session !== sample.capture.session) return {
    session: sample.capture.session,
    captureSequence: values[0],
    captureRoot: values[1],
    publisherSequence: values[2],
    captureSequenceChangedMs: now,
    captureRootChangedMs: now,
    publisherChangedMs: now
  };
  function changed(value2, prior, at) {
    return value2 !== null && (prior === null || value2 > prior) ? now : at;
  }
  __name(changed, "changed");
  const maximum = /* @__PURE__ */ __name((value2, prior) => value2 === null ? prior : prior === null ? value2 : Math.max(value2, prior), "maximum");
  return {
    session: sample.capture.session,
    captureSequence: maximum(values[0], previous.captureSequence),
    captureRoot: maximum(values[1], previous.captureRoot),
    publisherSequence: maximum(values[2], previous.publisherSequence),
    captureSequenceChangedMs: changed(values[0], previous.captureSequence, previous.captureSequenceChangedMs),
    captureRootChangedMs: changed(values[1], previous.captureRoot, previous.captureRootChangedMs),
    publisherChangedMs: changed(values[2], previous.publisherSequence, previous.publisherChangedMs)
  };
}
__name(progress, "progress");
function issues(sample, lastReceivedMs, startedMs, counters, streamIssue2, now) {
  if (now - (lastReceivedMs || startedMs) >= HEARTBEAT_TIMEOUT_MS) return ["machine_heartbeat_missing"];
  if (!sample) return [];
  const problems = /* @__PURE__ */ new Set();
  for (const code of sample.errors) problems.add("telemetry:" + code);
  for (const name of REQUIRED_SERVICES) if (sample.services[name] !== "active") problems.add("service:" + name);
  if (sample.rpc.healthy !== true) problems.add("validator_rpc_unhealthy");
  if (sample.capture.state !== "CAPTURING") problems.add("capture_not_running");
  if (sample.compact.state !== "RUNNING") problems.add("compaction_not_running");
  if (sample.publisher.state !== "RUNNING") problems.add("publisher_not_running");
  if (sample.cold_storage.state !== "RUNNING") problems.add("cloud_storage_not_running");
  if (sample.compact.updated_unix_ms === null || now - sample.compact.updated_unix_ms > 12e4) problems.add("compaction_status_stale");
  if (sample.publisher.updated_unix_ms === null || now - sample.publisher.updated_unix_ms > 12e4) problems.add("publisher_status_stale");
  if (sample.cold_storage.updated_unix_ms === null || now - sample.cold_storage.updated_unix_ms > 3e5) problems.add("cloud_storage_status_stale");
  if (sample.free_disk_bytes !== null && sample.free_disk_bytes < (sample.capture_reserve_bytes ?? 400 * GIB) + 100 * GIB) problems.add("disk_headroom_low");
  if (sample.mem_available_bytes !== null && sample.mem_available_bytes < 64 * GIB) problems.add("memory_headroom_low");
  if (sample.capture.latest_rooted_slot !== null && sample.compact.last_compact_slot !== null && sample.capture.latest_rooted_slot - sample.compact.last_compact_slot > 64) problems.add("compaction_behind");
  if (sample.compact.event_sequence !== null && sample.publisher.sequence !== null && sample.compact.event_sequence - sample.publisher.sequence > 8192) problems.add("cloud_upload_behind");
  if (counters) {
    if (sample.capture.durable_sequence !== null && counters.captureSequence !== null && sample.capture.durable_sequence < counters.captureSequence) problems.add("capture_counter_regressed");
    if (sample.capture.latest_rooted_slot !== null && counters.captureRoot !== null && sample.capture.latest_rooted_slot < counters.captureRoot) problems.add("capture_counter_regressed");
    if (sample.publisher.sequence !== null && counters.publisherSequence !== null && sample.publisher.sequence < counters.publisherSequence) problems.add("cloud_upload_counter_regressed");
    if (now - counters.captureSequenceChangedMs > 9e4 || now - counters.captureRootChangedMs > 9e4) problems.add("capture_stalled");
    if (sample.compact.event_sequence !== null && sample.publisher.sequence !== null && sample.compact.event_sequence > sample.publisher.sequence && now - counters.publisherChangedMs > 3e5) problems.add("cloud_upload_stalled");
  }
  if (streamIssue2) problems.add(streamIssue2);
  return [...problems].sort();
}
__name(issues, "issues");
function transition(prior, current, now) {
  const currentKey = JSON.stringify(current);
  const previousTimes = prior.issueSinceMs ?? Object.fromEntries(prior.candidate.map((code) => [code, prior.candidateSinceMs]));
  const issueSinceMs = Object.fromEntries(current.map((code) => [code, previousTimes[code] ?? now]));
  const incident = {
    ...prior,
    candidate: current,
    issueSinceMs,
    candidateSinceMs: currentKey === JSON.stringify(prior.candidate) ? prior.candidateSinceMs : now
  };
  if (currentKey === JSON.stringify(prior.active)) return { incident: { ...incident, candidateSinceMs: now }, event: null };
  if (current[0] === "machine_heartbeat_missing") return {
    incident: { ...incident, active: current, candidateSinceMs: now },
    event: { kind: "failure", issues: current, createdMs: now }
  };
  if (!current.length) {
    if (now - incident.candidateSinceMs < ISSUE_CONFIRM_MS) return { incident, event: null };
    return {
      incident: { ...incident, active: [], candidateSinceMs: now },
      event: { kind: "recovery", issues: [], createdMs: now }
    };
  }
  const confirmed = current.filter((code) => now - issueSinceMs[code] >= ISSUE_CONFIRM_MS);
  if (!confirmed.length) return { incident, event: null };
  const newFault = confirmed.some((code) => !prior.active.includes(code));
  return {
    incident: { ...incident, active: confirmed },
    event: newFault ? { kind: "failure", issues: confirmed, createdMs: now } : null
  };
}
__name(transition, "transition");

// src/telegram.ts
function record(value2) {
  return value2 !== null && typeof value2 === "object" && !Array.isArray(value2) ? value2 : null;
}
__name(record, "record");
function positive(value2) {
  return typeof value2 === "number" && Number.isSafeInteger(value2) && value2 > 0;
}
__name(positive, "positive");
function telegramCommand(value2, chatId, botUsername) {
  if (!chatId || !/^[1-9]\d{0,15}$/.test(chatId) || !Number.isSafeInteger(Number(chatId))) return null;
  const update = record(value2);
  if (!update || typeof update.update_id !== "number" || !Number.isSafeInteger(update.update_id) || update.update_id < 0) throw new Error("invalid_telegram_update");
  const message = record(update.message), chat = record(message?.chat), sender = record(message?.from);
  if (!message || !chat || !sender || !positive(chat.id) || !positive(sender.id) || String(chat.id) !== chatId || String(sender.id) !== chatId || chat.type !== "private" || sender.is_bot !== false) return null;
  if (!positive(message.message_id) || typeof message.text !== "string" || message.text.length > 256) return null;
  const match = /^\/(status|help)(?:@([a-zA-Z0-9_]{5,32}))?\s*$/.exec(message.text);
  if (!match || match[2] && match[2].toLowerCase() !== botUsername.toLowerCase()) return null;
  return { updateId: update.update_id, messageId: message.message_id, recipient: chatId, command: match[1] };
}
__name(telegramCommand, "telegramCommand");
function age(at, now) {
  return at === null || at === 0 ? "unavailable" : Math.max(0, Math.floor((now - at) / 1e3)) + " seconds ago";
}
__name(age, "age");
function value(number2) {
  return number2 === null ? "unavailable" : String(number2);
}
__name(value, "value");
function gb(bytes) {
  return bytes === null ? "unavailable" : (bytes / 1e9).toFixed(1) + " GB";
}
__name(gb, "gb");
function freshness(at, now, limit) {
  return at === null ? "report time unavailable" : now - at > limit ? "STALE report, " + age(at, now) : "report " + age(at, now);
}
__name(freshness, "freshness");
function duration(seconds) {
  const total = Math.ceil(seconds), hours = Math.floor(total / 3600), minutes = Math.floor(total % 3600 / 60), remainder = total % 60;
  const unit = /* @__PURE__ */ __name((count, name) => count + " " + name + (count === 1 ? "" : "s"), "unit");
  return hours > 0 ? unit(hours, "hour") + " " + unit(minutes, "minute") : minutes > 0 ? unit(minutes, "minute") + " " + unit(remainder, "second") : unit(total, "second");
}
__name(duration, "duration");
function estimateRange(minimum, maximum, stepMinutes) {
  const stepSeconds = stepMinutes * 60;
  const brief = /* @__PURE__ */ __name((seconds) => {
    const minutes = Math.round(seconds / 60), hours = Math.floor(minutes / 60), remainder = minutes % 60;
    const unit = /* @__PURE__ */ __name((count, name) => count + " " + name + (count === 1 ? "" : "s"), "unit");
    return hours > 0 ? unit(hours, "hour") + (remainder ? " " + unit(remainder, "minute") : "") : unit(minutes, "minute");
  }, "brief");
  return brief(Math.floor(minimum / stepSeconds) * stepSeconds) + " to " + brief(Math.ceil(maximum / stepSeconds) * stepSeconds);
}
__name(estimateRange, "estimateRange");
function parisDate(unixMs) {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Paris",
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23"
  }).formatToParts(new Date(unixMs));
  const part = /* @__PURE__ */ __name((kind) => parts.find((item) => item.type === kind)?.value, "part");
  return `${part("day")} ${part("month")} ${part("year")} ${part("hour")}:${part("minute")}:${part("second")} (Paris)`;
}
__name(parisDate, "parisDate");
function archiveLines(sample, now, missing) {
  const archive = sample.archive;
  if (!archive) return ["Archive upload: status and ETA are not in this machine sample."];
  const current = !missing && archive.updated_unix_ms !== null && now - archive.updated_unix_ms <= 12e4 && now - archive.updated_unix_ms >= -6e4;
  const estimatesCurrent = current && archive.paused !== true;
  const progressCurrent = estimatesCurrent && archive.progress_updated_unix_ms !== null && archive.progress_updated_unix_ms !== void 0 && now - archive.progress_updated_unix_ms <= 12e4 && now - archive.progress_updated_unix_ms >= -6e4;
  const percent = progressCurrent && archive.progress_percent !== null ? " (" + Number(archive.progress_percent.toFixed(1)) + "%)" : "";
  const stage = archive.state ?? "unavailable";
  const lines = ["Archive: epoch " + value(archive.epoch) + ", " + stage + percent + "; " + freshness(archive.updated_unix_ms, now, 12e4)];
  const scope = archive.eta_scope === "download" ? "download only" : archive.eta_scope === "upload" ? "upload only" : "this stage only";
  if (archive.paused === true) lines.push("Background archive work: PAUSED for live work. ETA is unavailable.");
  else if (!progressCurrent) lines.push("Archive progress: unavailable (no recent matching stage report).");
  lines.push("Archive ETA: " + (progressCurrent && archive.eta_seconds !== null && archive.eta_scope !== null ? duration(archive.eta_seconds) + " (" + scope + "). Later stages are not included." : "unavailable for the current stage."));
  if (estimatesCurrent && archive.epoch_eta_min_seconds !== null && archive.epoch_eta_max_seconds !== null)
    lines.push("Epoch upload estimate: " + estimateRange(archive.epoch_eta_min_seconds, archive.epoch_eta_max_seconds, 5) + " (recent runs; can pause for live work).");
  if (estimatesCurrent && archive.queue_eta_min_seconds !== null && archive.queue_eta_max_seconds !== null)
    lines.push("Archive plan estimate: " + estimateRange(archive.queue_eta_min_seconds, archive.queue_eta_max_seconds, 15) + " (recent runs; can pause for live work).");
  if (progressCurrent && archive.bytes_per_second !== null) lines.push("Archive transfer rate: " + (archive.bytes_per_second / 1e6).toFixed(1) + " MB/second");
  if (archive.last_completed_epoch !== null) lines.push("Last complete archive epoch: " + archive.last_completed_epoch + "; compact size " + gb(archive.last_compact_bytes));
  lines.push("Archive epochs left: " + (archive.upload_queue_epochs.length ? archive.upload_queue_epochs.join(", ") : archive.state === "COMPLETE" ? "none in this plan" : "unavailable"));
  return lines;
}
__name(archiveLines, "archiveLines");
function telegramStatus(state, label, now) {
  const lines = [label + " status at " + parisDate(now)];
  if (!state.armed || !state.sample) return lines.concat("No machine heartbeat has arrived. Status and ETA are unavailable.").join("\n");
  const sample = state.sample, missing = now - state.lastReceivedMs >= HEARTBEAT_TIMEOUT_MS;
  lines.push(missing ? "Heartbeat: MISSING. The values below are the last received values." : "Heartbeat: received " + age(state.lastReceivedMs, now));
  lines.push("Machine sample: " + parisDate(sample.sampled_unix_ms));
  const root = sample.capture.latest_rooted_slot, compact = sample.compact.last_compact_slot;
  const slotLag = root === null || compact === null ? "unavailable" : String(Math.max(0, root - compact));
  const events = sample.compact.event_sequence, published = sample.publisher.sequence;
  const eventLag = events === null || published === null ? "unavailable" : String(Math.max(0, events - published));
  lines.push("Capture: " + (sample.capture.state ?? "unavailable") + "; rooted slot " + value(root));
  lines.push("Live compaction: " + (sample.compact.state ?? "unavailable") + "; slot " + value(compact) + "; lag " + slotLag + " slots; " + freshness(sample.compact.updated_unix_ms, now, 12e4));
  lines.push("Live upload: " + (sample.publisher.state ?? "unavailable") + "; event " + value(published) + "; lag " + eventLag + " events; " + freshness(sample.publisher.updated_unix_ms, now, 12e4));
  lines.push("Native storage: " + (sample.cold_storage.state ?? "unavailable") + "; " + value(sample.cold_storage.retired_banks) + " banks stored; " + freshness(sample.cold_storage.updated_unix_ms, now, 3e5));
  lines.push(...archiveLines(sample, now, missing));
  lines.push("Disk free: " + gb(sample.free_disk_bytes) + "; memory available: " + gb(sample.mem_available_bytes));
  const current = issues(sample, state.lastReceivedMs, state.startedMs, state.counters, state.streamIssue, now);
  lines.push("Checks: " + (current.length ? current.join(", ") : "no current fault"));
  lines.push("External stream check: " + (state.lastCheckMs === 0 ? "not yet completed" : freshness(state.lastCheckMs, now, 12e4) + "; " + (state.streamIssue ?? "passed")));
  return lines.join("\n");
}
__name(telegramStatus, "telegramStatus");
function telegramHelp(label) {
  return label + " commands\n/status - Show the last machine report, live compaction, cloud upload, and available ETA.\n/help - Show these commands.\nCommands are read-only and available only in your private alert chat.";
}
__name(telegramHelp, "telegramHelp");

// src/index.ts
var LIMIT = 64 * 1024;
var ALARM_INTERVAL_MS = 3e4;
async function boundedJSON(response) {
  const length = response.headers.get("content-length");
  if (length && (!/^\d+$/.test(length) || Number(length) > LIMIT)) throw new Error("body_too_large");
  const reader = response.body?.getReader();
  if (!reader) throw new Error("body_missing");
  const parts = [];
  let bytes = 0;
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) break;
      bytes += item.value.byteLength;
      if (bytes > LIMIT) throw new Error("body_too_large");
      parts.push(item.value);
    }
    const result = new Uint8Array(bytes);
    let offset = 0;
    for (const part of parts) {
      result.set(part, offset);
      offset += part.byteLength;
    }
    return JSON.parse(new TextDecoder().decode(result));
  } finally {
    await reader.cancel().catch(() => {
    });
    reader.releaseLock();
  }
}
__name(boundedJSON, "boundedJSON");
function alertDestination(env) {
  if (env.TELEGRAM_BOT_TOKEN || env.TELEGRAM_CHAT_ID) {
    if (!/^[1-9]\d{4,19}:[A-Za-z0-9_-]{20,128}$/.test(env.TELEGRAM_BOT_TOKEN ?? "") || !/^(?:-?[1-9]\d{0,18}|@[A-Za-z][A-Za-z0-9_]{4,31})$/.test(env.TELEGRAM_CHAT_ID ?? "")) {
      return { destination: null, error: "alert_telegram_configuration_invalid" };
    }
    return { destination: {
      channel: "telegram",
      url: `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`,
      chat: env.TELEGRAM_CHAT_ID
    }, error: null };
  }
  if (!env.ALERT_WEBHOOK_URL) return { destination: null, error: "alert_destination_not_configured" };
  try {
    const url = new URL(env.ALERT_WEBHOOK_URL);
    if (url.protocol !== "https:" || url.username || url.password || url.hash) throw new Error("invalid_url");
    return { destination: { channel: "webhook", url: url.href }, error: null };
  } catch {
    return { destination: null, error: "alert_destination_invalid" };
  }
}
__name(alertDestination, "alertDestination");
function telegramReplyValid(value2, chat) {
  if (!value2 || typeof value2 !== "object" || Array.isArray(value2)) return false;
  const reply = value2;
  if (reply.ok !== true || !reply.result || typeof reply.result !== "object" || Array.isArray(reply.result)) return false;
  const result = reply.result;
  if (typeof result.message_id !== "number" || !Number.isSafeInteger(result.message_id) || result.message_id <= 0 || !result.chat || typeof result.chat !== "object" || Array.isArray(result.chat)) return false;
  const deliveredChat = result.chat;
  return chat.startsWith("@") ? typeof deliveredChat.username === "string" && deliveredChat.username.toLowerCase() === chat.slice(1).toLowerCase() : typeof deliveredChat.id === "number" && Number.isSafeInteger(deliveredChat.id) && String(deliveredChat.id) === chat;
}
__name(telegramReplyValid, "telegramReplyValid");
async function authorized(request, expected) {
  const provided = request.headers.get("authorization") ?? "";
  if (!provided.startsWith("Bearer ") || provided.length > 512) return false;
  return secretMatches(provided.slice(7), expected);
}
__name(authorized, "authorized");
async function secretMatches(provided, expected) {
  if (!expected || !provided || provided.length > 512) return false;
  const encode = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", encode.encode(provided)),
    crypto.subtle.digest("SHA-256", encode.encode(expected))
  ]);
  return timingSafeEqual(new Uint8Array(a), new Uint8Array(b));
}
__name(secretMatches, "secretMatches");
async function streamIssue(env) {
  try {
    const url = new URL(env.STREAM_URL);
    if (url.href !== "https://stream.blockzilla.dev/v1/blockhashes") return "stream_check_url_invalid";
    const response = await fetch(url.href, {
      redirect: "manual",
      signal: AbortSignal.timeout(1e4),
      headers: { Accept: "text/event-stream", "Cache-Control": "no-cache", "User-Agent": "Blockzilla-watch/1.0" }
    });
    if (!response.ok) {
      await response.body?.cancel();
      return "public_stream_unreachable";
    }
    if (!response.headers.get("content-type")?.startsWith("text/event-stream")) {
      await response.body?.cancel();
      return "public_stream_invalid";
    }
    const reader = response.body?.getReader();
    if (!reader) return "public_stream_invalid";
    const decoder = new TextDecoder();
    let pending = "", bytes = 0, firstSlot = null;
    try {
      while (true) {
        const part = await reader.read();
        if (part.done) return "public_stream_stale";
        bytes += part.value.byteLength;
        if (bytes > LIMIT) return "public_stream_invalid";
        pending = (pending + decoder.decode(part.value, { stream: true })).replaceAll("\r\n", "\n");
        let end;
        while ((end = pending.indexOf("\n\n")) >= 0) {
          const record2 = pending.slice(0, end);
          pending = pending.slice(end + 2);
          const lines = record2.split("\n");
          const kind = lines.find((line) => line.startsWith("event:"))?.slice(6).trim();
          if (kind === "gap") return "public_stream_stale";
          if (kind !== "block") continue;
          const data = JSON.parse(lines.filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trim()).join("\n"));
          if (typeof data.slot !== "number" || !Number.isSafeInteger(data.slot) || data.slot < 0) return "public_stream_invalid";
          if (firstSlot !== null && data.slot > firstSlot) return null;
          firstSlot = data.slot;
        }
      }
    } finally {
      await reader.cancel().catch(() => {
      });
      reader.releaseLock();
    }
  } catch {
    return "public_stream_unreachable";
  }
}
__name(streamIssue, "streamIssue");
var FoundationWatch = class extends DurableObject {
  static {
    __name(this, "FoundationWatch");
  }
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS watch(id INTEGER PRIMARY KEY, state TEXT NOT NULL)");
    this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS alerts(
      id TEXT PRIMARY KEY, event TEXT NOT NULL, status TEXT NOT NULL,
      lease_until_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0)`);
    this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS telegram_commands(
      update_id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL, recipient TEXT NOT NULL,
      command TEXT NOT NULL, queued_ms INTEGER NOT NULL, reply TEXT,
      status TEXT NOT NULL, lease_until_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0)`);
    if (!this.ctx.storage.sql.exec("SELECT id FROM watch WHERE id=1").toArray().length) this.save({
      startedMs: Date.now(),
      armed: false,
      sample: null,
      lastReceivedMs: 0,
      lastCheckMs: 0,
      counters: null,
      incident: { active: [], candidate: [], candidateSinceMs: 0 },
      currentIssues: [],
      streamIssue: null,
      lastDeliveryMs: null,
      deliveryError: null
    });
  }
  read() {
    const row = this.ctx.storage.sql.exec("SELECT state FROM watch WHERE id=1").one();
    return JSON.parse(row.state);
  }
  save(state) {
    this.ctx.storage.sql.exec("INSERT INTO watch(id,state) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state", JSON.stringify(state));
  }
  async ensureAlarm() {
    if (await this.ctx.storage.getAlarm() === null) await this.ctx.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
  }
  queue(event) {
    const alert = { ...event, id: crypto.randomUUID() };
    this.ctx.storage.sql.exec("INSERT INTO alerts(id,event,status) VALUES(?,?,'pending')", alert.id, JSON.stringify(alert));
    return alert;
  }
  async ingest(sample) {
    const state = this.read();
    const now = Date.now();
    const previous = state.sample;
    const changedBoot = previous !== null && sample.boot_id !== null && sample.boot_id !== previous.boot_id;
    const retired = state.retiredBoots ?? [];
    if (sample.boot_id !== null && retired.includes(sample.boot_id)) throw new Error("retired_boot_heartbeat");
    if (!previous && (sample.sampled_unix_ms < now - 12e4 || sample.sampled_unix_ms > now + 6e4)) throw new Error("initial_sample_clock_outside_window");
    if (!previous || changedBoot || sample.monotonic_ms > previous.monotonic_ms) {
      if (changedBoot && previous?.boot_id) state.retiredBoots = [...retired, previous.boot_id].slice(-64);
      if (sample.sampled_unix_ms < now - 12e4 || sample.sampled_unix_ms > now + 6e4) sample.errors.push("sender_clock_skew");
      if (!changedBoot && previous && sample.sampled_unix_ms < previous.sampled_unix_ms) sample.errors.push("sender_clock_regressed");
      sample.errors = [...new Set(sample.errors)].sort();
      state.sample = sample;
      state.lastReceivedMs = now;
      state.counters = progress(changedBoot ? null : state.counters, sample, now);
      state.armed = true;
      this.save(state);
    } else if (sample.monotonic_ms < previous.monotonic_ms) throw new Error("out_of_order_heartbeat");
    await this.ensureAlarm();
    return { accepted: true, schema: "blockzilla-foundation-heartbeat-accepted-v1", sampled_unix_ms: sample.sampled_unix_ms };
  }
  async check() {
    await this.ensureAlarm();
    const publicIssue = await streamIssue(this.env);
    const now = Date.now(), state = this.read();
    const current = state.armed ? issues(state.sample, state.lastReceivedMs, state.startedMs, state.counters, publicIssue, now) : [];
    const result = transition(state.incident, current, now);
    state.lastCheckMs = now;
    state.streamIssue = publicIssue;
    state.currentIssues = current;
    state.incident = result.incident;
    this.ctx.storage.transactionSync(() => {
      this.save(state);
      if (result.event) this.queue(result.event);
    });
    await this.deliver();
    await this.deliverCommands();
  }
  async acceptCommand(command) {
    if (command.recipient !== this.env.TELEGRAM_CHAT_ID || !/^[1-9]\d{0,15}$/.test(command.recipient) || !Number.isSafeInteger(Number(command.recipient)) || !Number.isSafeInteger(command.updateId) || command.updateId < 0 || !Number.isSafeInteger(command.messageId) || command.messageId <= 0 || !["status", "help"].includes(command.command)) throw new Error("invalid_command");
    const duplicate = this.ctx.storage.sql.exec("SELECT update_id FROM telegram_commands WHERE update_id=?", command.updateId).toArray().length > 0;
    if (!duplicate) {
      const pending = this.ctx.storage.sql.exec("SELECT COUNT(*) AS n FROM telegram_commands WHERE status!='delivered'").one().n;
      if (pending >= 100) throw new Error("command_outbox_full");
      this.ctx.storage.sql.exec(
        "INSERT INTO telegram_commands(update_id,message_id,recipient,command,queued_ms,status) VALUES(?,?,?,?,?,'pending')",
        command.updateId,
        command.messageId,
        command.recipient,
        command.command,
        Date.now()
      );
    }
    await this.ensureAlarm();
  }
  async deliverCommands() {
    const row = this.ctx.storage.sql.exec(
      "SELECT update_id,message_id,recipient,command,queued_ms,reply,status,lease_until_ms FROM telegram_commands WHERE status!='delivered' ORDER BY queued_ms,update_id LIMIT 1"
    ).toArray()[0];
    if (!row) return;
    const now = Date.now();
    if (row.status === "sending" && row.lease_until_ms > now) return;
    const { destination } = alertDestination(this.env);
    if (destination?.channel !== "telegram" || destination.chat !== row.recipient) {
      const state = this.read();
      state.commandDeliveryError = "command_telegram_configuration_changed";
      this.save(state);
      return;
    }
    const lease = now + 3e4;
    const suffix = "\nRequest: " + row.update_id;
    const content = row.reply ?? (row.command === "status" ? telegramStatus(this.read(), this.env.MACHINE_LABEL, now) : telegramHelp(this.env.MACHINE_LABEL));
    const text2 = content.length + suffix.length <= 4096 ? content + suffix : content.slice(0, 4096 - suffix.length - 4) + " ..." + suffix;
    this.ctx.storage.sql.exec("UPDATE telegram_commands SET status='sending',lease_until_ms=?,attempts=attempts+1,reply=? WHERE update_id=?", lease, content, row.update_id);
    let error = null;
    try {
      const response = await fetch(destination.url, {
        method: "POST",
        redirect: "manual",
        signal: AbortSignal.timeout(1e4),
        headers: { "Content-Type": "application/json", "Idempotency-Key": "telegram-command-" + row.update_id },
        body: JSON.stringify({
          chat_id: row.recipient,
          text: text2,
          disable_notification: true,
          link_preview_options: { is_disabled: true },
          reply_parameters: { message_id: row.message_id, allow_sending_without_reply: true }
        })
      });
      if (!response.ok) {
        await response.body?.cancel();
        error = "command_delivery_http_" + response.status;
      } else {
        try {
          if (!telegramReplyValid(await boundedJSON(response), row.recipient)) error = "command_delivery_telegram_response_invalid";
        } catch {
          error = "command_delivery_telegram_response_invalid";
        }
      }
    } catch {
      error = "command_delivery_transport_failed";
    }
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec("UPDATE telegram_commands SET status=?,lease_until_ms=0 WHERE update_id=? AND lease_until_ms=?", error ? "pending" : "delivered", row.update_id, lease);
      const latest = this.read();
      latest.commandDeliveryError = error;
      if (!error) latest.lastCommandDeliveryMs = Date.now();
      this.save(latest);
    });
    console.log(JSON.stringify({ event: "telegram_command_delivery", update_id: row.update_id, command: row.command, result: error ?? "delivered" }));
    this.ctx.storage.sql.exec("DELETE FROM telegram_commands WHERE status='delivered' AND queued_ms<? AND update_id NOT IN (SELECT update_id FROM telegram_commands WHERE status='delivered' ORDER BY queued_ms DESC,update_id DESC LIMIT 1000)", Date.now() - 7 * 24 * 60 * 60 * 1e3);
  }
  async deliver() {
    const state = this.read();
    const { destination, error: configurationError } = alertDestination(this.env);
    if (!destination) {
      state.deliveryError = configurationError;
      this.save(state);
      return;
    }
    const now = Date.now();
    const row = this.ctx.storage.sql.exec(
      "SELECT id,event,status,lease_until_ms FROM alerts WHERE status!='delivered' ORDER BY rowid LIMIT 1"
    ).toArray()[0];
    if (!row) {
      if (["alert_destination_not_configured", "alert_destination_invalid", "alert_telegram_configuration_invalid"].includes(state.deliveryError ?? "")) {
        state.deliveryError = null;
        this.save(state);
      }
      return;
    }
    if (row.status === "sending" && row.lease_until_ms > now) return;
    const lease = now + 3e4;
    this.ctx.storage.sql.exec("UPDATE alerts SET status='sending',lease_until_ms=?,attempts=attempts+1 WHERE id=?", lease, row.id);
    const alert = JSON.parse(row.event);
    const label = this.env.MACHINE_LABEL;
    const at = new Date(alert.createdMs).toISOString();
    const text2 = alert.kind === "test" ? `TEST: alert delivery check for ${label} at ${at}. Production capture has not been stopped.` : alert.kind === "recovery" ? `${label}: the monitored machine and pipeline checks recovered at ${at}.` : `${label}: a monitored check failed at ${at}. ${alert.issues.join(", ")}. A missing heartbeat can mean a machine, sender or network failure.`;
    let error = null;
    try {
      const suffix = `
Event: ${alert.id}`;
      const telegramText = text2.length + suffix.length <= 4096 ? text2 + suffix : text2.slice(0, 4096 - suffix.length - 4) + " ..." + suffix;
      const body = destination.channel === "telegram" ? {
        chat_id: destination.chat,
        text: telegramText,
        link_preview_options: { is_disabled: true },
        disable_notification: false
      } : {
        text: text2,
        content: text2,
        event_id: alert.id,
        machine: label,
        kind: alert.kind,
        issues: alert.issues,
        occurred_at: at
      };
      const response = await fetch(destination.url, {
        method: "POST",
        redirect: "manual",
        signal: AbortSignal.timeout(1e4),
        headers: { "Content-Type": "application/json", "Idempotency-Key": alert.id },
        body: JSON.stringify(body)
      });
      if (!response.ok) {
        await response.body?.cancel();
        error = "alert_delivery_http_" + response.status;
      } else if (destination.channel === "telegram") {
        try {
          const reply = await boundedJSON(response);
          if (!telegramReplyValid(reply, destination.chat)) error = "alert_delivery_telegram_response_invalid";
        } catch {
          error = "alert_delivery_telegram_response_invalid";
        }
      } else await response.body?.cancel();
    } catch {
      error = "alert_delivery_transport_failed";
    }
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec("UPDATE alerts SET status=?,lease_until_ms=0 WHERE id=? AND lease_until_ms=?", error ? "pending" : "delivered", alert.id, lease);
      const latest = this.read();
      latest.deliveryError = error;
      if (!error) latest.lastDeliveryMs = Date.now();
      this.save(latest);
    });
    console.log(JSON.stringify({ event: "alert_delivery", event_id: alert.id, kind: alert.kind, result: error ?? "delivered" }));
    this.ctx.storage.sql.exec("DELETE FROM alerts WHERE status='delivered' AND rowid NOT IN (SELECT rowid FROM alerts WHERE status='delivered' ORDER BY rowid DESC LIMIT 100)");
  }
  async testAlert() {
    const event = this.queue({ kind: "test", issues: [], createdMs: Date.now() });
    await this.ensureAlarm();
    await this.deliver();
    return { event_id: event.id };
  }
  async status() {
    const { destination } = alertDestination(this.env);
    return {
      ...this.read(),
      alert_delivery_configured: destination !== null,
      alert_delivery_channel: destination?.channel ?? null,
      telegram_commands_configured: destination?.channel === "telegram" && /^[1-9]\d{0,15}$/.test(destination.chat) && Number.isSafeInteger(Number(destination.chat)) && /^[A-Za-z0-9_-]{32,256}$/.test(this.env.TELEGRAM_WEBHOOK_SECRET ?? ""),
      pending_commands: this.ctx.storage.sql.exec("SELECT COUNT(*) AS n FROM telegram_commands WHERE status!='delivered'").one().n,
      next_alarm_ms: await this.ctx.storage.getAlarm(),
      pending_alerts: this.ctx.storage.sql.exec("SELECT COUNT(*) AS n FROM alerts WHERE status!='delivered'").one().n
    };
  }
  async alarm() {
    await this.ctx.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
    await this.check();
  }
};
var index_default = {
  async fetch(request, env, ctx) {
    const path = new URL(request.url).pathname;
    const stub = env.WATCH.getByName("foundation");
    if (path === "/telegram" && request.method === "POST") {
      if (!/^[A-Za-z0-9_-]{32,256}$/.test(env.TELEGRAM_WEBHOOK_SECRET ?? "") || !await secretMatches(request.headers.get("X-Telegram-Bot-Api-Secret-Token") ?? "", env.TELEGRAM_WEBHOOK_SECRET)) return new Response("Unauthorized", { status: 401 });
      let command;
      try {
        command = telegramCommand(await boundedJSON(request), env.TELEGRAM_CHAT_ID, env.TELEGRAM_BOT_USERNAME);
      } catch {
        return new Response("Invalid update", { status: 400 });
      }
      if (command) {
        try {
          await stub.acceptCommand(command);
        } catch {
          return new Response("Try later", { status: 503 });
        }
        ctx.waitUntil(stub.deliverCommands());
      }
      return Response.json({ ok: true }, { headers: { "Cache-Control": "no-store" } });
    }
    if (path === "/heartbeat" && request.method === "POST") {
      if (!await authorized(request, env.HEARTBEAT_TOKEN)) return new Response("Unauthorized", { status: 401 });
      try {
        const sample = heartbeat(await boundedJSON(request), Date.now());
        return Response.json(await stub.ingest(sample), { status: 202, headers: { "Cache-Control": "no-store" } });
      } catch {
        return new Response("Invalid heartbeat", { status: 400 });
      }
    }
    if (!["/status", "/check", "/test-alert"].includes(path)) return new Response("Not found", { status: 404 });
    if (!await authorized(request, env.ADMIN_TOKEN)) return new Response("Unauthorized", { status: 401 });
    if (path === "/status" && request.method === "GET") return Response.json(await stub.status(), { headers: { "Cache-Control": "no-store" } });
    if (path === "/check" && request.method === "POST") {
      await stub.check();
      return Response.json(await stub.status());
    }
    if (path === "/test-alert" && request.method === "POST") return Response.json(await stub.testAlert(), { status: 202 });
    return new Response("Method not allowed", { status: 405 });
  },
  async scheduled(_controller, env) {
    await env.WATCH.getByName("foundation").check();
  }
};
export {
  FoundationWatch,
  index_default as default
};
//# sourceMappingURL=index.js.map
