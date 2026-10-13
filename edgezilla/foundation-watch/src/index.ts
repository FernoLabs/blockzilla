import { DurableObject } from "cloudflare:workers";
import { timingSafeEqual } from "node:crypto";
import { heartbeat, issues, progress, transition } from "./model";
import type { Alert, Heartbeat, Incident, Progress } from "./model";
import { telegramCommand, telegramHelp, telegramStatus } from "./telegram";
import type { TelegramCommand } from "./telegram";

const LIMIT = 64 * 1024;
const ALARM_INTERVAL_MS = 30_000;
interface WatchState {
  startedMs: number;
  armed: boolean;
  sample: Heartbeat | null;
  lastReceivedMs: number;
  lastCheckMs: number;
  counters: Progress | null;
  incident: Incident;
  currentIssues: string[];
  streamIssue: string | null;
  lastDeliveryMs: number | null;
  deliveryError: string | null;
  retiredBoots?: string[];
  commandDeliveryError?: string | null;
  lastCommandDeliveryMs?: number | null;
}

async function boundedJSON(response: Response | Request): Promise<unknown> {
  const length = response.headers.get("content-length");
  if (length && (!/^\d+$/.test(length) || Number(length) > LIMIT)) throw new Error("body_too_large");
  const reader = response.body?.getReader();
  if (!reader) throw new Error("body_missing");
  const parts: Uint8Array[] = [];
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
    for (const part of parts) { result.set(part, offset); offset += part.byteLength; }
    return JSON.parse(new TextDecoder().decode(result));
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

type AlertDestination = { channel: "telegram"; url: string; chat: string } | { channel: "webhook"; url: string };
function alertDestination(env: Env): { destination: AlertDestination | null; error: string | null } {
  // Explicit Telegram settings must be complete. Do not fall back to another
  // recipient when only one secret is present or either setting is malformed.
  if (env.TELEGRAM_BOT_TOKEN || env.TELEGRAM_CHAT_ID) {
    if (!/^[1-9]\d{4,19}:[A-Za-z0-9_-]{20,128}$/.test(env.TELEGRAM_BOT_TOKEN ?? "") ||
        !/^(?:-?[1-9]\d{0,18}|@[A-Za-z][A-Za-z0-9_]{4,31})$/.test(env.TELEGRAM_CHAT_ID ?? "")) {
      return { destination: null, error: "alert_telegram_configuration_invalid" };
    }
    return { destination: { channel: "telegram", url: `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`,
      chat: env.TELEGRAM_CHAT_ID }, error: null };
  }
  if (!env.ALERT_WEBHOOK_URL) return { destination: null, error: "alert_destination_not_configured" };
  try {
    const url = new URL(env.ALERT_WEBHOOK_URL);
    if (url.protocol !== "https:" || url.username || url.password || url.hash) throw new Error("invalid_url");
    return { destination: { channel: "webhook", url: url.href }, error: null };
  } catch { return { destination: null, error: "alert_destination_invalid" }; }
}

function telegramReplyValid(value: unknown, chat: string): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const reply = value as Record<string, unknown>;
  if (reply.ok !== true || !reply.result || typeof reply.result !== "object" || Array.isArray(reply.result)) return false;
  const result = reply.result as Record<string, unknown>;
  if (typeof result.message_id !== "number" || !Number.isSafeInteger(result.message_id) || result.message_id <= 0 ||
      !result.chat || typeof result.chat !== "object" || Array.isArray(result.chat)) return false;
  const deliveredChat = result.chat as Record<string, unknown>;
  return chat.startsWith("@") ? typeof deliveredChat.username === "string" &&
    deliveredChat.username.toLowerCase() === chat.slice(1).toLowerCase() :
    typeof deliveredChat.id === "number" && Number.isSafeInteger(deliveredChat.id) && String(deliveredChat.id) === chat;
}

async function authorized(request: Request, expected: string | undefined): Promise<boolean> {
  const provided = request.headers.get("authorization") ?? "";
  if (!provided.startsWith("Bearer ") || provided.length > 512) return false;
  return secretMatches(provided.slice(7), expected);
}
async function secretMatches(provided: string, expected: string | undefined): Promise<boolean> {
  if (!expected || !provided || provided.length > 512) return false;
  const encode = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", encode.encode(provided)),
    crypto.subtle.digest("SHA-256", encode.encode(expected)),
  ]);
  return timingSafeEqual(new Uint8Array(a), new Uint8Array(b));
}

async function streamIssue(env: Env): Promise<string | null> {
  try {
    const url = new URL(env.STREAM_URL);
    if (url.href !== "https://stream.blockzilla.dev/v1/blockhashes") return "stream_check_url_invalid";
    const response = await fetch(url.href, { redirect: "manual", signal: AbortSignal.timeout(10_000),
      headers: { Accept: "text/event-stream", "Cache-Control": "no-cache", "User-Agent": "Blockzilla-watch/1.0" } });
    if (!response.ok) { await response.body?.cancel(); return "public_stream_unreachable"; }
    if (!response.headers.get("content-type")?.startsWith("text/event-stream")) { await response.body?.cancel(); return "public_stream_invalid"; }
    const reader = response.body?.getReader();
    if (!reader) return "public_stream_invalid";
    const decoder = new TextDecoder();
    let pending = "", bytes = 0, firstSlot: number | null = null;
    try {
      while (true) {
        const part = await reader.read();
        if (part.done) return "public_stream_stale";
        bytes += part.value.byteLength;
        if (bytes > LIMIT) return "public_stream_invalid";
        pending = (pending + decoder.decode(part.value, { stream: true })).replaceAll("\r\n", "\n");
        let end: number;
        while ((end = pending.indexOf("\n\n")) >= 0) {
          const record = pending.slice(0, end); pending = pending.slice(end + 2);
          const lines = record.split("\n");
          const kind = lines.find(line => line.startsWith("event:"))?.slice(6).trim();
          if (kind === "gap") return "public_stream_stale";
          if (kind !== "block") continue;
          const data = JSON.parse(lines.filter(line => line.startsWith("data:")).map(line => line.slice(5).trim()).join("\n")) as Record<string, unknown>;
          if (typeof data.slot !== "number" || !Number.isSafeInteger(data.slot) || data.slot < 0) return "public_stream_invalid";
          if (firstSlot !== null && data.slot > firstSlot) return null;
          firstSlot = data.slot;
        }
      }
    } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
  } catch { return "public_stream_unreachable"; }
}

export class FoundationWatch extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // SQL is synchronous; no network operation is held inside an input gate.
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS watch(id INTEGER PRIMARY KEY, state TEXT NOT NULL)");
    this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS alerts(
      id TEXT PRIMARY KEY, event TEXT NOT NULL, status TEXT NOT NULL,
      lease_until_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0)`);
    this.ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS telegram_commands(
      update_id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL, recipient TEXT NOT NULL,
      command TEXT NOT NULL, queued_ms INTEGER NOT NULL, reply TEXT,
      status TEXT NOT NULL, lease_until_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0)`);
    if (!this.ctx.storage.sql.exec("SELECT id FROM watch WHERE id=1").toArray().length) this.save({
      startedMs: Date.now(), armed: false, sample: null, lastReceivedMs: 0, lastCheckMs: 0,
      counters: null, incident: { active: [], candidate: [], candidateSinceMs: 0 }, currentIssues: [],
      streamIssue: null, lastDeliveryMs: null, deliveryError: null,
    });
  }

  private read(): WatchState {
    const row = this.ctx.storage.sql.exec<{ state: string }>("SELECT state FROM watch WHERE id=1").one();
    return JSON.parse(row.state) as WatchState;
  }
  private save(state: WatchState): void {
    this.ctx.storage.sql.exec("INSERT INTO watch(id,state) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state", JSON.stringify(state));
  }
  private async ensureAlarm(): Promise<void> {
    if (await this.ctx.storage.getAlarm() === null) await this.ctx.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
  }
  private queue(event: Omit<Alert, "id">): Alert {
    const alert = { ...event, id: crypto.randomUUID() };
    this.ctx.storage.sql.exec("INSERT INTO alerts(id,event,status) VALUES(?,?,'pending')", alert.id, JSON.stringify(alert));
    return alert;
  }

  async ingest(sample: Heartbeat): Promise<{ accepted: true; schema: string; sampled_unix_ms: number }> {
    const state = this.read();
    const now = Date.now();
    const previous = state.sample;
    const changedBoot = previous !== null && sample.boot_id !== null && sample.boot_id !== previous.boot_id;
    const retired = state.retiredBoots ?? [];
    if (sample.boot_id !== null && retired.includes(sample.boot_id)) throw new Error("retired_boot_heartbeat");
    if (!previous && (sample.sampled_unix_ms < now - 120_000 || sample.sampled_unix_ms > now + 60_000)) throw new Error("initial_sample_clock_outside_window");
    if (!previous || changedBoot || sample.monotonic_ms > previous.monotonic_ms) {
      if (changedBoot && previous?.boot_id) state.retiredBoots = [...retired, previous.boot_id].slice(-64);
      if (sample.sampled_unix_ms < now - 120_000 || sample.sampled_unix_ms > now + 60_000) sample.errors.push("sender_clock_skew");
      if (!changedBoot && previous && sample.sampled_unix_ms < previous.sampled_unix_ms) sample.errors.push("sender_clock_regressed");
      sample.errors = [...new Set(sample.errors)].sort();
      state.sample = sample;
      state.lastReceivedMs = now;
      state.counters = progress(changedBoot ? null : state.counters, sample, now);
      state.armed = true;
      this.save(state);
    } else if (sample.monotonic_ms < previous.monotonic_ms) throw new Error("out_of_order_heartbeat");
    // Duplicate retries are accepted, but do not refresh the heartbeat deadline.
    await this.ensureAlarm();
    return { accepted: true, schema: "blockzilla-foundation-heartbeat-accepted-v1", sampled_unix_ms: sample.sampled_unix_ms };
  }

  async check(): Promise<void> {
    await this.ensureAlarm();
    const publicIssue = await streamIssue(this.env);
    // Read after network I/O so a concurrent heartbeat remains authoritative.
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

  async acceptCommand(command: TelegramCommand): Promise<void> {
    // The outer route authenticates the webhook. Check the private recipient
    // again at the storage boundary, before an update can enter the outbox.
    if (command.recipient !== this.env.TELEGRAM_CHAT_ID || !/^[1-9]\d{0,15}$/.test(command.recipient) || !Number.isSafeInteger(Number(command.recipient)) ||
        !Number.isSafeInteger(command.updateId) || command.updateId < 0 || !Number.isSafeInteger(command.messageId) || command.messageId <= 0 ||
        !["status", "help"].includes(command.command)) throw new Error("invalid_command");
    const duplicate = this.ctx.storage.sql.exec("SELECT update_id FROM telegram_commands WHERE update_id=?", command.updateId).toArray().length > 0;
    if (!duplicate) {
      const pending = this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM telegram_commands WHERE status!='delivered'").one().n;
      if (pending >= 100) throw new Error("command_outbox_full");
      this.ctx.storage.sql.exec("INSERT INTO telegram_commands(update_id,message_id,recipient,command,queued_ms,status) VALUES(?,?,?,?,?,'pending')",
        command.updateId, command.messageId, command.recipient, command.command, Date.now());
    }
    await this.ensureAlarm();
  }

  async deliverCommands(): Promise<void> {
    const row = this.ctx.storage.sql.exec<{ update_id: number; message_id: number; recipient: string; command: string; queued_ms: number;
        reply: string | null; status: string; lease_until_ms: number }>(
      "SELECT update_id,message_id,recipient,command,queued_ms,reply,status,lease_until_ms FROM telegram_commands WHERE status!='delivered' ORDER BY queued_ms,update_id LIMIT 1").toArray()[0];
    if (!row) return;
    const now = Date.now();
    if (row.status === "sending" && row.lease_until_ms > now) return;
    const { destination } = alertDestination(this.env);
    if (destination?.channel !== "telegram" || destination.chat !== row.recipient) {
      const state = this.read(); state.commandDeliveryError = "command_telegram_configuration_changed"; this.save(state); return;
    }
    const lease = now + 30_000;
    const suffix = "\nRequest: " + row.update_id;
    const content = row.reply ?? (row.command === "status" ? telegramStatus(this.read(), this.env.MACHINE_LABEL, now) : telegramHelp(this.env.MACHINE_LABEL));
    const text = content.length + suffix.length <= 4096 ? content + suffix : content.slice(0, 4096 - suffix.length - 4) + " ..." + suffix;
    this.ctx.storage.sql.exec("UPDATE telegram_commands SET status='sending',lease_until_ms=?,attempts=attempts+1,reply=? WHERE update_id=?", lease, content, row.update_id);
    let error: string | null = null;
    try {
      const response = await fetch(destination.url, { method: "POST", redirect: "manual", signal: AbortSignal.timeout(10_000),
        headers: { "Content-Type": "application/json", "Idempotency-Key": "telegram-command-" + row.update_id },
        body: JSON.stringify({ chat_id: row.recipient, text, disable_notification: true,
          link_preview_options: { is_disabled: true }, reply_parameters: { message_id: row.message_id, allow_sending_without_reply: true } }) });
      if (!response.ok) { await response.body?.cancel(); error = "command_delivery_http_" + response.status; }
      else {
        try { if (!telegramReplyValid(await boundedJSON(response), row.recipient)) error = "command_delivery_telegram_response_invalid"; }
        catch { error = "command_delivery_telegram_response_invalid"; }
      }
    } catch { error = "command_delivery_transport_failed"; }
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec("UPDATE telegram_commands SET status=?,lease_until_ms=0 WHERE update_id=? AND lease_until_ms=?", error ? "pending" : "delivered", row.update_id, lease);
      const latest = this.read(); latest.commandDeliveryError = error;
      if (!error) latest.lastCommandDeliveryMs = Date.now();
      this.save(latest);
    });
    console.log(JSON.stringify({ event: "telegram_command_delivery", update_id: row.update_id, command: row.command, result: error ?? "delivered" }));
    // Keep a week of update IDs plus the newest 1,000 delivered updates. Pending
    // rows survive restart and are never removed by this retention step.
    this.ctx.storage.sql.exec("DELETE FROM telegram_commands WHERE status='delivered' AND queued_ms<? AND update_id NOT IN (SELECT update_id FROM telegram_commands WHERE status='delivered' ORDER BY queued_ms DESC,update_id DESC LIMIT 1000)", Date.now() - 7 * 24 * 60 * 60 * 1000);
  }

  private async deliver(): Promise<void> {
    const state = this.read();
    const { destination, error: configurationError } = alertDestination(this.env);
    if (!destination) {
      state.deliveryError = configurationError;
      this.save(state);
      return;
    }
    const now = Date.now();
    const row = this.ctx.storage.sql.exec<{ id: string; event: string; status: string; lease_until_ms: number }>(
      "SELECT id,event,status,lease_until_ms FROM alerts WHERE status!='delivered' ORDER BY rowid LIMIT 1").toArray()[0];
    if (!row) {
      if (["alert_destination_not_configured", "alert_destination_invalid", "alert_telegram_configuration_invalid"].includes(state.deliveryError ?? "")) {
        state.deliveryError = null; this.save(state);
      }
      return;
    }
    if (row.status === "sending" && row.lease_until_ms > now) return;
    const lease = now + 30_000;
    this.ctx.storage.sql.exec("UPDATE alerts SET status='sending',lease_until_ms=?,attempts=attempts+1 WHERE id=?", lease, row.id);
    const alert = JSON.parse(row.event) as Alert;
    const label = this.env.MACHINE_LABEL;
    const at = new Date(alert.createdMs).toISOString();
    const text = alert.kind === "test" ? `TEST: alert delivery check for ${label} at ${at}. Production capture has not been stopped.` :
      alert.kind === "recovery" ? `${label}: the monitored machine and pipeline checks recovered at ${at}.` :
      `${label}: a monitored check failed at ${at}. ${alert.issues.join(", ")}. A missing heartbeat can mean a machine, sender or network failure.`;
    let error: string | null = null;
    try {
      // Telegram does not provide a documented idempotency key for sendMessage.
      // Keep the stable ID in the message so a retried delivery is identifiable.
      const suffix = `\nEvent: ${alert.id}`;
      const telegramText = text.length + suffix.length <= 4096 ? text + suffix : text.slice(0, 4096 - suffix.length - 4) + " ..." + suffix;
      const body = destination.channel === "telegram" ? { chat_id: destination.chat, text: telegramText,
        link_preview_options: { is_disabled: true }, disable_notification: false } :
        { text, content: text, event_id: alert.id, machine: label, kind: alert.kind,
          issues: alert.issues, occurred_at: at };
      const response = await fetch(destination.url, { method: "POST", redirect: "manual", signal: AbortSignal.timeout(10_000),
        headers: { "Content-Type": "application/json", "Idempotency-Key": alert.id },
        body: JSON.stringify(body) });
      if (!response.ok) {
        await response.body?.cancel(); error = "alert_delivery_http_" + response.status;
      } else if (destination.channel === "telegram") {
        try {
          const reply = await boundedJSON(response);
          if (!telegramReplyValid(reply, destination.chat)) error = "alert_delivery_telegram_response_invalid";
        } catch { error = "alert_delivery_telegram_response_invalid"; }
      } else await response.body?.cancel();
    } catch { error = "alert_delivery_transport_failed"; }
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec("UPDATE alerts SET status=?,lease_until_ms=0 WHERE id=? AND lease_until_ms=?", error ? "pending" : "delivered", alert.id, lease);
      const latest = this.read();
      latest.deliveryError = error;
      if (!error) latest.lastDeliveryMs = Date.now();
      this.save(latest);
    });
    console.log(JSON.stringify({ event: "alert_delivery", event_id: alert.id, kind: alert.kind, result: error ?? "delivered" }));
    // Bounded private history. Pending or leased alerts are never pruned.
    this.ctx.storage.sql.exec("DELETE FROM alerts WHERE status='delivered' AND rowid NOT IN (SELECT rowid FROM alerts WHERE status='delivered' ORDER BY rowid DESC LIMIT 100)");
  }

  async testAlert(): Promise<{ event_id: string }> {
    const event = this.queue({ kind: "test", issues: [], createdMs: Date.now() });
    await this.ensureAlarm();
    await this.deliver();
    return { event_id: event.id };
  }
  async status(): Promise<WatchState & { alert_delivery_configured: boolean; alert_delivery_channel: "telegram" | "webhook" | null;
      telegram_commands_configured: boolean; pending_commands: number; pending_alerts: number; next_alarm_ms: number | null }> {
    const { destination } = alertDestination(this.env);
    return { ...this.read(), alert_delivery_configured: destination !== null, alert_delivery_channel: destination?.channel ?? null,
      telegram_commands_configured: destination?.channel === "telegram" && /^[1-9]\d{0,15}$/.test(destination.chat) && Number.isSafeInteger(Number(destination.chat)) && /^[A-Za-z0-9_-]{32,256}$/.test(this.env.TELEGRAM_WEBHOOK_SECRET ?? ""),
      pending_commands: this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM telegram_commands WHERE status!='delivered'").one().n,
      next_alarm_ms: await this.ctx.storage.getAlarm(),
      pending_alerts: this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM alerts WHERE status!='delivered'").one().n };
  }
  async alarm(): Promise<void> {
    // Schedule first: an exception cannot leave the watcher without its next check.
    await this.ctx.storage.setAlarm(Date.now() + ALARM_INTERVAL_MS);
    await this.check();
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const path = new URL(request.url).pathname;
    const stub = env.WATCH.getByName("foundation");
    if (path === "/telegram" && request.method === "POST") {
      if (!/^[A-Za-z0-9_-]{32,256}$/.test(env.TELEGRAM_WEBHOOK_SECRET ?? "") ||
          !await secretMatches(request.headers.get("X-Telegram-Bot-Api-Secret-Token") ?? "", env.TELEGRAM_WEBHOOK_SECRET)) return new Response("Unauthorized", { status: 401 });
      let command: TelegramCommand | null;
      try { command = telegramCommand(await boundedJSON(request), env.TELEGRAM_CHAT_ID, env.TELEGRAM_BOT_USERNAME); }
      catch { return new Response("Invalid update", { status: 400 }); }
      if (command) {
        try { await stub.acceptCommand(command); }
        catch { return new Response("Try later", { status: 503 }); }
        ctx.waitUntil(stub.deliverCommands());
      }
      // Unknown chats, senders and commands get the same empty receipt. They
      // cause no outbound message and reveal no machine data.
      return Response.json({ ok: true }, { headers: { "Cache-Control": "no-store" } });
    }
    if (path === "/heartbeat" && request.method === "POST") {
      if (!await authorized(request, env.HEARTBEAT_TOKEN)) return new Response("Unauthorized", { status: 401 });
      try {
        const sample = heartbeat(await boundedJSON(request), Date.now());
        return Response.json(await stub.ingest(sample), { status: 202, headers: { "Cache-Control": "no-store" } });
      } catch { return new Response("Invalid heartbeat", { status: 400 }); }
    }
    if (!["/status", "/check", "/test-alert"].includes(path)) return new Response("Not found", { status: 404 });
    if (!await authorized(request, env.ADMIN_TOKEN)) return new Response("Unauthorized", { status: 401 });
    if (path === "/status" && request.method === "GET") return Response.json(await stub.status(), { headers: { "Cache-Control": "no-store" } });
    if (path === "/check" && request.method === "POST") { await stub.check(); return Response.json(await stub.status()); }
    if (path === "/test-alert" && request.method === "POST") return Response.json(await stub.testAlert(), { status: 202 });
    return new Response("Method not allowed", { status: 405 });
  },
  async scheduled(_controller: ScheduledController, env: Env): Promise<void> {
    // The independent cron repairs a missing alarm and checks persisted state.
    await env.WATCH.getByName("foundation").check();
  },
} satisfies ExportedHandler<Env>;
