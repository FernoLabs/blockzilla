import { HEARTBEAT_TIMEOUT_MS, issues } from "./model.ts";
import type { Heartbeat, Progress } from "./model.ts";

export interface TelegramCommand {
  updateId: number;
  messageId: number;
  recipient: string;
  command: "status" | "help";
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}
function positive(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

export function telegramCommand(value: unknown, chatId: string | undefined, botUsername: string): TelegramCommand | null {
  // Commands require the owner's numeric private-chat identity. Alert channels
  // and username destinations do not grant access to private machine metrics.
  if (!chatId || !/^[1-9]\d{0,15}$/.test(chatId) || !Number.isSafeInteger(Number(chatId))) return null;
  const update = record(value);
  if (!update || typeof update.update_id !== "number" || !Number.isSafeInteger(update.update_id) || update.update_id < 0) throw new Error("invalid_telegram_update");
  const message = record(update.message), chat = record(message?.chat), sender = record(message?.from);
  if (!message || !chat || !sender || !positive(chat.id) || !positive(sender.id) ||
      String(chat.id) !== chatId || String(sender.id) !== chatId || chat.type !== "private" || sender.is_bot !== false) return null;
  if (!positive(message.message_id) || typeof message.text !== "string" || message.text.length > 256) return null;
  const match = /^\/(status|help)(?:@([a-zA-Z0-9_]{5,32}))?\s*$/.exec(message.text);
  if (!match || (match[2] && match[2].toLowerCase() !== botUsername.toLowerCase())) return null;
  return { updateId: update.update_id, messageId: message.message_id, recipient: chatId, command: match[1] as "status" | "help" };
}

interface Snapshot {
  armed: boolean;
  startedMs: number;
  sample: Heartbeat | null;
  lastReceivedMs: number;
  lastCheckMs: number;
  counters: Progress | null;
  streamIssue: string | null;
}
function age(at: number | null, now: number): string {
  return at === null || at === 0 ? "unavailable" : Math.max(0, Math.floor((now - at) / 1000)) + " seconds ago";
}
function value(number: number | null): string { return number === null ? "unavailable" : String(number); }
function gb(bytes: number | null): string { return bytes === null ? "unavailable" : (bytes / 1e9).toFixed(1) + " GB"; }
function freshness(at: number | null, now: number, limit: number): string {
  return at === null ? "report time unavailable" : now - at > limit ? "STALE report, " + age(at, now) : "report " + age(at, now);
}
function duration(seconds: number): string {
  const total = Math.ceil(seconds), hours = Math.floor(total / 3600), minutes = Math.floor(total % 3600 / 60), remainder = total % 60;
  const unit = (count: number, name: string) => count + " " + name + (count === 1 ? "" : "s");
  return hours > 0 ? unit(hours, "hour") + " " + unit(minutes, "minute") : minutes > 0 ? unit(minutes, "minute") + " " + unit(remainder, "second") : unit(total, "second");
}
function estimateRange(minimum: number, maximum: number, stepMinutes: number): string {
  const stepSeconds = stepMinutes * 60;
  const brief = (seconds: number) => {
    const minutes = Math.round(seconds / 60), hours = Math.floor(minutes / 60), remainder = minutes % 60;
    const unit = (count: number, name: string) => count + " " + name + (count === 1 ? "" : "s");
    return hours > 0 ? unit(hours, "hour") + (remainder ? " " + unit(remainder, "minute") : "") : unit(minutes, "minute");
  };
  return brief(Math.floor(minimum / stepSeconds) * stepSeconds) + " to " + brief(Math.ceil(maximum / stepSeconds) * stepSeconds);
}
function parisDate(unixMs: number): string {
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Paris", day: "2-digit", month: "short", year: "numeric",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" }).formatToParts(new Date(unixMs));
  const part = (kind: string) => parts.find(item => item.type === kind)?.value;
  return `${part("day")} ${part("month")} ${part("year")} ${part("hour")}:${part("minute")}:${part("second")} (Paris)`;
}
function archiveLines(sample: Heartbeat, now: number, missing: boolean): string[] {
  const archive = sample.archive;
  if (!archive) return ["Archive upload: status and ETA are not in this machine sample."];
  const current = !missing && archive.updated_unix_ms !== null && now - archive.updated_unix_ms <= 120_000 && now - archive.updated_unix_ms >= -60_000;
  const estimatesCurrent = current && archive.paused !== true;
  const progressCurrent = estimatesCurrent && archive.progress_updated_unix_ms !== null && archive.progress_updated_unix_ms !== undefined &&
    now - archive.progress_updated_unix_ms <= 120_000 && now - archive.progress_updated_unix_ms >= -60_000;
  const percent = progressCurrent && archive.progress_percent !== null ? " (" + Number(archive.progress_percent.toFixed(1)) + "%)" : "";
  const stage = archive.state ?? "unavailable";
  const lines = ["Archive: epoch " + value(archive.epoch) + ", " + stage + percent + "; " + freshness(archive.updated_unix_ms, now, 120_000)];
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

export function telegramStatus(state: Snapshot, label: string, now: number): string {
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
  lines.push("Live compaction: " + (sample.compact.state ?? "unavailable") + "; slot " + value(compact) + "; lag " + slotLag + " slots; " + freshness(sample.compact.updated_unix_ms, now, 120_000));
  lines.push("Live upload: " + (sample.publisher.state ?? "unavailable") + "; event " + value(published) + "; lag " + eventLag + " events; " + freshness(sample.publisher.updated_unix_ms, now, 120_000));
  lines.push("Native storage: " + (sample.cold_storage.state ?? "unavailable") + "; " + value(sample.cold_storage.retired_banks) + " banks stored; " + freshness(sample.cold_storage.updated_unix_ms, now, 300_000));
  lines.push(...archiveLines(sample, now, missing));
  lines.push("Disk free: " + gb(sample.free_disk_bytes) + "; memory available: " + gb(sample.mem_available_bytes));
  const current = issues(sample, state.lastReceivedMs, state.startedMs, state.counters, state.streamIssue, now);
  lines.push("Checks: " + (current.length ? current.join(", ") : "no current fault"));
  lines.push("External stream check: " + (state.lastCheckMs === 0 ? "not yet completed" : freshness(state.lastCheckMs, now, 120_000) + "; " + (state.streamIssue ?? "passed")));
  return lines.join("\n");
}

export function telegramHelp(label: string): string {
  return label + " commands\n/status - Show the last machine report, live compaction, cloud upload, and available ETA.\n/help - Show these commands.\nCommands are read-only and available only in your private alert chat.";
}
