import test from 'node:test';
import assert from 'node:assert/strict';
import { Miniflare, Response as MFResponse, convertV4MiniflareOptions } from 'miniflare';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const admin = { Authorization: 'Bearer local-admin-token-only' };
const hb = { Authorization: 'Bearer local-heartbeat-token-only', 'Content-Type': 'application/json' };
const TEST_TIMEOUT_MS = 5000;
const telegramToken = '1234567890:LOCAL_FAKE_TOKEN_NEVER_USE_123456789';
const telegramChat = '123456789';
const telegramSecrets = { TELEGRAM_BOT_TOKEN: telegramToken, TELEGRAM_CHAT_ID: telegramChat };
const commandSecret = 'LOCAL_FAKE_WEBHOOK_SECRET_NEVER_USE_123456789';
const commandSecrets = { ...telegramSecrets, TELEGRAM_WEBHOOK_SECRET: commandSecret };
const tg = { 'X-Telegram-Bot-Api-Secret-Token': commandSecret, 'Content-Type': 'application/json' };
const telegramURL = `https://api.telegram.org/bot${telegramToken}/sendMessage`;
const telegramAccepted = () => MFResponse.json({ ok: true, result: { message_id: 1, chat: { id: Number(telegramChat), type: 'private' } } });
function sample() {
  const at = Date.now();
  return { schema: 'blockzilla-foundation-heartbeat-v1', machine_id: 'foundation', sampled_unix_ms: at, monotonic_ms: at,
    boot_id: '11111111-1111-1111-1111-111111111111', free_disk_bytes: 800 * 1024 ** 3,
    capture_reserve_bytes: 400 * 1024 ** 3, mem_available_bytes: 200 * 1024 ** 3,
    capture: { session: '0'.repeat(32) + '-1', state: 'CAPTURING', durable_sequence: 1000, latest_rooted_slot: 200, updated_unix_ms: null },
    compact: { state: 'RUNNING', last_compact_slot: 200, event_sequence: 500, source_sequence: 1000, updated_unix_ms: at },
    publisher: { state: 'RUNNING', sequence: 450, updated_unix_ms: at },
    cold_storage: { state: 'RUNNING', retired_banks: 100, updated_unix_ms: at },
    services: Object.fromEntries(['hivezilla-validator', 'hivezilla-direct-worker', 'hivezilla-bank-v2-producer',
      'hivezilla-bank-v2-r2-publisher', 'hivezilla-native-cold-storage-20261012-live', 'hivezilla-live-priority-20261012'].map(x => [x + '.service', 'active'])),
    rpc: { healthy: true, finalized_slot: 200 }, errors: [] };
}
async function setup(t, webhook, extra = {}, streamReply = null, telegramReply = null, inspectStorage = false) {
  const dir = await mkdtemp(join(tmpdir(), 'foundation-watch-test-'));
  const calls = [];
  const options = {
    ...(inspectStorage ? { name: 'foundation-watch-history-fixture', unsafeInspectDurableObjects: true } : {}),
    modules: true, scriptPath: '.wrangler/dry-run/index.js', compatibilityDate: '2026-10-12', compatibilityFlags: ['nodejs_compat'],
    durableObjects: { WATCH: { className: 'FoundationWatch', useSQLite: true } }, durableObjectsPersist: dir,
    bindings: { MACHINE_LABEL: 'Megaloadon', TELEGRAM_BOT_USERNAME: 'megaloadon_alerts_bot', STREAM_URL: 'https://stream.blockzilla.dev/v1/blockhashes',
      HEARTBEAT_TOKEN: 'local-heartbeat-token-only', ADMIN_TOKEN: 'local-admin-token-only',
      ...(webhook ? { ALERT_WEBHOOK_URL: 'https://alerts.example.invalid/incoming' } : {}), ...extra },
    outboundService: async request => {
      calls.push({ url: request.url, authorization: request.headers.get('authorization') });
      if (request.url === 'https://stream.blockzilla.dev/v1/blockhashes') return streamReply ? streamReply(request) : new MFResponse(
        'event: block\ndata: {"slot":200}\n\nevent: block\ndata: {"slot":201}\n\n', { headers: { 'Content-Type': 'text/event-stream' } });
      if (request.url === 'https://alerts.example.invalid/incoming' && webhook) return webhook(request);
      if (request.url === telegramURL && telegramReply) return telegramReply(request);
      throw new Error('Unexpected outbound request');
    },
  };
  const converted = convertV4MiniflareOptions(options);
  // Miniflare 5 stores the outbound fetcher on each worker's dev options.
  // Keep the fail-closed handler on the worker that owns FoundationWatch.
  assert.equal(converted.workers[0].dev.outboundService.type, 'fetcher');
  assert.equal(converted.workers[0].dev.outboundService.handler, options.outboundService);
  const mf = new Miniflare(converted);
  t.after(async () => { await mf.dispose(); await rm(dir, { recursive: true, force: true }); });
  async function request(path, method = 'GET', headers = admin, body) {
    return mf.dispatchFetch('https://watch.example' + path, { method, headers, body });
  }
  return { mf, options, calls, request, status: async () => (await request('/status')).json() };
}

async function historySetup(t, webhook = null) {
  const app = await setup(t, webhook, {}, null, null, true);
  // Use Miniflare's local storage inspector; production needs no test method.
  await app.status();
  const storage = await app.mf.unsafeGetDurableObjectStorage(
    'foundation-watch-history-fixture', 'FoundationWatch', { name: 'foundation' });
  return { ...app, storage };
}
function historyRow(number, changes = {}) {
  const id = '00000000-0000-4000-8000-' + String(number).padStart(12, '0');
  return { id, event: JSON.stringify({ id, kind: 'failure', issues: ['capture_not_running'], createdMs: number }),
    status: 'pending', attempts: 0, ...changes };
}
async function insertHistory(storage, rows) {
  for (let start = 0; start < rows.length; start += 20) {
    const group = rows.slice(start, start + 20);
    await storage.exec('INSERT INTO alerts(id,event,status,attempts) VALUES ' + group.map(() => '(?,?,?,?)').join(','),
      ...group.flatMap(row => [row.id, row.event, row.status, row.attempts]));
  }
}

test('alert history requires the admin token, accepts only GET, and leaves an empty watcher unchanged', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await historySetup(t);
  const before = await app.status();
  for (const headers of [{}, hb, { Authorization: 'Bearer wrong' }]) {
    for (const method of ['GET', 'POST']) {
      assert.equal((await app.request('/history', method, headers)).status, 401);
    }
  }
  for (const method of ['HEAD', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS']) {
    assert.equal((await app.request('/history', method)).status, 405);
  }
  const response = await app.request('/history');
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.deepEqual(await response.json(), {
    schema: 'blockzilla-foundation-alert-history-v1', limit: 100, truncated: false, alerts: [],
  });
  assert.deepEqual(await app.status(), before);
  assert.equal(before.next_alarm_ms, null);
  assert.equal(app.calls.length, 0);
});

test('alert history returns stored failure and recovery events newest first after restart', { timeout: TEST_TIMEOUT_MS }, async t => {
  const delivered = [];
  const app = await historySetup(t, async request => {
    delivered.push(await request.json()); return new MFResponse(null, { status: 204 });
  });
  const payload = sample(); payload.errors = ['bank_v2_not_live'];
  await app.request('/heartbeat', 'POST', hb, JSON.stringify(payload));
  const failure = JSON.parse((await app.storage.exec('SELECT state FROM watch WHERE id=1'))[0].state);
  const issue = 'telemetry:bank_v2_not_live';
  failure.incident = { active: [], candidate: [issue], candidateSinceMs: Date.now() - 61_000,
    issueSinceMs: { [issue]: Date.now() - 61_000 } };
  await app.storage.exec('UPDATE watch SET state=? WHERE id=1', JSON.stringify(failure));
  await app.request('/check', 'POST');
  assert.equal(delivered[0].kind, 'failure');
  await app.request('/heartbeat', 'POST', hb, JSON.stringify({ ...sample(), monotonic_ms: payload.monotonic_ms + 1 }));
  const recovery = JSON.parse((await app.storage.exec('SELECT state FROM watch WHERE id=1'))[0].state);
  recovery.incident = { ...recovery.incident, candidate: [], candidateSinceMs: Date.now() - 61_000, issueSinceMs: {} };
  await app.storage.exec('UPDATE watch SET state=? WHERE id=1', JSON.stringify(recovery));
  await app.request('/check', 'POST');
  assert.deepEqual(delivered.map(event => event.kind), ['failure', 'recovery']);
  const stored = await app.storage.exec('SELECT id,event,status,attempts FROM alerts ORDER BY rowid DESC');
  assert.equal(stored.length, 2);
  await app.mf.setOptions(convertV4MiniflareOptions(app.options));
  const before = await app.status(), calls = app.calls.length;
  const response = await app.request('/history');
  assert.equal(response.status, 200);
  const history = await response.json();
  assert.equal(history.truncated, false);
  assert.deepEqual(history.alerts, stored.map(row => ({ ...JSON.parse(row.event), status: row.status, attempts: row.attempts })));
  assert.deepEqual(history.alerts.map(event => event.kind), ['recovery', 'failure']);
  assert.deepEqual(history.alerts.map(event => event.id), delivered.map(event => event.event_id).reverse());
  assert.deepEqual(history.alerts.map(event => event.status), ['delivered', 'delivered']);
  assert.deepEqual(history.alerts.map(event => event.attempts), [1, 1]);
  assert.deepEqual(await app.status(), before);
  assert.equal(app.calls.length, calls);
});

test('alert history projects safe fields without delivering pending alerts or changing state and alarms', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await historySetup(t, () => { throw new Error('History must not deliver'); });
  await app.request('/heartbeat', 'POST', hb, JSON.stringify(sample()));
  const row = historyRow(1, { status: 'sending', attempts: 2 });
  const event = { ...JSON.parse(row.event), kind: 'test', createdMs: 0,
    issues: ['a'.repeat(80), 'telemetry:' + 'b'.repeat(80), 'service:hivezilla-validator.service'],
    secret: 'NEVER_RETURN_SECRET', recipient: 'NEVER_RETURN_RECIPIENT', url: 'https://secret.invalid',
    headers: { Authorization: 'Bearer NEVER_RETURN_HEADER' }, raw: 'NEVER_RETURN_RAW' };
  row.event = JSON.stringify(event);
  await insertHistory(app.storage, [row]);
  const before = await app.status();
  const stored = await app.storage.exec('SELECT * FROM alerts ORDER BY rowid');
  const tables = await app.storage.exec("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name");
  const response = await app.request('/history');
  assert.equal(response.status, 200);
  const text = await response.text(), history = JSON.parse(text);
  assert.deepEqual(history.alerts, [{ id: row.id, kind: event.kind, issues: event.issues,
    createdMs: 0, status: 'sending', attempts: 2 }]);
  for (const privateValue of ['NEVER_RETURN', 'secret.invalid', 'Authorization', 'recipient', 'headers', 'raw']) {
    assert.equal(text.includes(privateValue), false);
  }
  assert.deepEqual(await app.status(), before);
  assert.ok(before.next_alarm_ms > 0);
  assert.deepEqual(await app.storage.exec('SELECT * FROM alerts ORDER BY rowid'), stored);
  assert.deepEqual(await app.storage.exec("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name"), tables);
  assert.equal(app.calls.length, 0);
});

test('alert history has a fixed 100-row limit and uses insertion order, not event time', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await historySetup(t);
  const rows = Array.from({ length: 100 }, (_, i) => {
    const row = historyRow(i + 1);
    row.event = JSON.stringify({ ...JSON.parse(row.event), createdMs: 1000 - i });
    return row;
  });
  await insertHistory(app.storage, rows);
  let history = await (await app.request('/history?limit=1&order=oldest')).json();
  assert.equal(history.limit, 100);
  assert.equal(history.truncated, false);
  assert.deepEqual(history.alerts.map(row => row.id), rows.map(row => row.id).reverse());
  await insertHistory(app.storage, [historyRow(101)]);
  history = await (await app.request('/history?limit=1000000')).json();
  assert.equal(history.limit, 100);
  assert.equal(history.truncated, true);
  assert.equal(history.alerts.length, 100);
  assert.deepEqual(history.alerts.map(row => row.id), [historyRow(101).id, ...rows.slice(1).map(row => row.id).reverse()]);
  assert.equal((await app.storage.exec('SELECT count(*) AS count FROM alerts'))[0].count, 101);
  assert.equal(app.calls.length, 0);
});

test('alert history rejects corrupt stored records with one generic response and no partial output', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await historySetup(t);
  const valid = historyRow(1);
  await insertHistory(app.storage, [valid, historyRow(2)]);
  const event = JSON.parse(valid.event);
  const invalidEvents = [
    '{broken NEVER_RETURN_SECRET', 'null', '[]',
    JSON.stringify({ ...event, id: historyRow(99).id }),
    JSON.stringify({ ...event, kind: 'unknown' }),
    ...[-1, 0.5, Number.MAX_SAFE_INTEGER + 1, '1'].map(createdMs => JSON.stringify({ ...event, createdMs })),
    JSON.stringify({ ...event, issues: null }),
    JSON.stringify({ ...event, issues: Array(81).fill('capture_stalled') }),
    ...[null, 1, '', 'a'.repeat(81), 'telemetry:' + 'b'.repeat(81), 'x'.repeat(121),
      'service:hivezilla-stream-relay.service', 'service:hivezilla-validator.service/private', 'bad:code', 'UPPER', 'contains space']
      .map(issue => JSON.stringify({ ...event, issues: [issue] })),
    JSON.stringify({ ...event, private: 'é'.repeat(8192) }),
    JSON.stringify({ ...event, private: 'x'.repeat(100_000) }),
    valid.event + '\0' + 'x'.repeat(20_000),
  ];
  const cases = [
    ...invalidEvents.map(value => ({ event: value, status: 'pending', attempts: 0 })),
    ...['unknown', '', 'delivered/private', 'pending\0', 'x'.repeat(100_000)].map(status => ({ event: valid.event, status, attempts: 0 })),
    ...[-1, 0.5, Number.MAX_SAFE_INTEGER + 1, 'invalid'].map(attempts => ({ event: valid.event, status: 'pending', attempts })),
  ];
  const before = await app.status();
  for (const row of cases) {
    await app.storage.exec('UPDATE alerts SET event=?,status=?,attempts=? WHERE id=?', row.event, row.status, row.attempts, valid.id);
    const response = await app.request('/history');
    assert.equal(response.status, 500, JSON.stringify(row));
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.deepEqual(await response.json(), { error: 'alert_history_unavailable' });
  }
  await app.storage.exec('UPDATE alerts SET event=?,status=?,attempts=zeroblob(100000) WHERE id=?', valid.event, valid.status, valid.id);
  assert.equal((await app.request('/history')).status, 500);
  await app.storage.exec('UPDATE alerts SET event=?,status=?,attempts=? WHERE id=?', valid.event, valid.status, valid.attempts, valid.id);
  await app.storage.exec('UPDATE alerts SET id=?,event=? WHERE id=?', 'x'.repeat(100_000),
    JSON.stringify({ ...event, id: 'x'.repeat(100_000) }), valid.id);
  assert.equal((await app.request('/history')).status, 500);
  await app.storage.exec('UPDATE alerts SET id=?,event=? WHERE id=?', valid.id, valid.event, 'x'.repeat(100_000));
  await app.storage.exec('UPDATE alerts SET id=? WHERE id=?', valid.id + '\0extra', valid.id);
  assert.equal((await app.request('/history')).status, 500);
  await app.storage.exec('UPDATE alerts SET id=? WHERE id=?', valid.id, valid.id + '\0extra');
  // Equal non-UUID values in the SQL key and event must also fail validation.
  await app.storage.exec('UPDATE alerts SET id=?,event=? WHERE id=?', 'not-a-uuid', JSON.stringify({ ...event, id: 'not-a-uuid' }), valid.id);
  const response = await app.request('/history');
  assert.equal(response.status, 500);
  assert.deepEqual(await response.json(), { error: 'alert_history_unavailable' });
  assert.deepEqual(await app.status(), before);
  assert.equal(app.calls.length, 0);
});

test('private routes reject missing or wrong credentials', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null);
  assert.equal((await app.request('/heartbeat', 'POST', {}, JSON.stringify(sample()))).status, 401);
  assert.equal((await app.request('/status', 'GET', hb)).status, 401);
  assert.equal((await app.request('/test-alert', 'POST', hb)).status, 401);
  assert.equal(app.calls.length, 0);
});
test('accepted heartbeat survives restart; duplicate does not extend its deadline', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null);
  const payload = sample();
  const accepted = await (await app.request('/heartbeat', 'POST', hb, JSON.stringify(payload))).json();
  assert.deepEqual(accepted, { accepted: true, schema: 'blockzilla-foundation-heartbeat-accepted-v1', sampled_unix_ms: payload.sampled_unix_ms });
  const before = await app.status();
  assert.equal(before.armed, true);
  await app.mf.setOptions(convertV4MiniflareOptions(app.options));
  await app.request('/heartbeat', 'POST', hb, JSON.stringify(payload));
  assert.equal((await app.status()).lastReceivedMs, before.lastReceivedMs);
});
test('oversized, stale and wrong-machine heartbeats cannot arm the watcher', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null);
  for (const payload of [{ ...sample(), machine_id: 'other' }, { ...sample(), sampled_unix_ms: Date.now() - 180000 }]) {
    assert.equal((await app.request('/heartbeat', 'POST', hb, JSON.stringify(payload))).status, 400);
  }
  assert.equal((await app.request('/heartbeat', 'POST', hb, 'x'.repeat(65537))).status, 400);
  assert.equal((await app.status()).armed, false);
});
test('failed alert stays durable and retries with the same event ID', { timeout: TEST_TIMEOUT_MS }, async t => {
  const events = [];
  const app = await setup(t, async request => {
    events.push(await request.json());
    return new MFResponse(null, { status: events.length === 1 ? 500 : 204 });
  });
  const accepted = await (await app.request('/test-alert', 'POST')).json();
  assert.equal((await app.status()).pending_alerts, 1);
  await app.mf.setOptions(convertV4MiniflareOptions(app.options));
  await app.request('/check', 'POST');
  assert.equal((await app.status()).pending_alerts, 0);
  assert.deepEqual(events.map(x => x.event_id), [accepted.event_id, accepted.event_id]);
  assert.equal(app.calls.filter(x => x.url.startsWith('https://alerts.')).some(x => x.authorization !== null), false);
  assert.equal(app.calls.filter(x => x.url.startsWith('https://stream.'))[0].authorization, null);
});
test('redirects do not call unexpected hosts or mark an alert delivered', { timeout: TEST_TIMEOUT_MS }, async t => {
  const redirect = () => new MFResponse(null, { status: 302, headers: { Location: 'https://unexpected.example.invalid/leak' } });
  const app = await setup(t, redirect, {}, redirect);
  await app.request('/test-alert', 'POST');
  const status = await (await app.request('/check', 'POST')).json();
  assert.equal(status.streamIssue, 'public_stream_unreachable');
  assert.equal(status.deliveryError, 'alert_delivery_http_302');
  assert.equal(status.lastDeliveryMs, null);
  assert.equal(status.pending_alerts, 1);
  assert.deepEqual(app.calls.map(x => x.url), [
    'https://alerts.example.invalid/incoming',
    'https://stream.blockzilla.dev/v1/blockhashes',
    'https://alerts.example.invalid/incoming',
  ]);
  assert.equal(app.calls[1].authorization, null);
  assert.equal(app.calls[0].authorization, null);
  assert.equal(app.calls[2].authorization, null);
});
test('an active earlier delivery lease prevents a later alert overtaking it', { timeout: TEST_TIMEOUT_MS }, async t => {
  let release, entered;
  const gate = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { entered = resolve; });
  // Release the mock before disposal if an assertion or test timeout stops us.
  t.after(() => release());
  t.signal.addEventListener('abort', () => release(), { once: true });
  const events = [];
  const app = await setup(t, async request => {
    events.push(await request.json());
    if (events.length === 1) { entered(); await gate; }
    return new MFResponse(null, { status: 204 });
  });
  const first = app.request('/test-alert', 'POST');
  await started;
  const second = await (await app.request('/test-alert', 'POST')).json();
  assert.equal(events.length, 1);
  release();
  const firstReply = await (await first).json();
  await app.request('/check', 'POST');
  assert.deepEqual(events.map(x => x.event_id), [firstReply.event_id, second.event_id]);
});
test('without a destination, status shows delivery is not configured', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null);
  await app.request('/test-alert', 'POST');
  const status = await app.status();
  assert.equal(status.alert_delivery_configured, false);
  assert.equal(status.deliveryError, 'alert_destination_not_configured');
  assert.equal(status.lastDeliveryMs, null);
  assert.equal(status.pending_alerts, 1);
});

test('Telegram delivery requires its JSON receipt and takes priority over a webhook', { timeout: TEST_TIMEOUT_MS }, async t => {
  const sent = [];
  const app = await setup(t, () => { throw new Error('Must not send to webhook'); }, telegramSecrets, null, async request => {
    assert.equal(request.method, 'POST');
    assert.equal(request.headers.get('authorization'), null);
    sent.push(await request.json());
    return telegramAccepted();
  });
  const accepted = await (await app.request('/test-alert', 'POST')).json();
  const status = await app.status();
  assert.equal(status.alert_delivery_channel, 'telegram');
  assert.equal(status.alert_delivery_configured, true);
  assert.equal(status.pending_alerts, 0);
  assert.equal(status.deliveryError, null);
  assert.ok(status.lastDeliveryMs);
  assert.equal(sent[0].chat_id, telegramChat);
  assert.equal(sent[0].disable_notification, false);
  assert.deepEqual(sent[0].link_preview_options, { is_disabled: true });
  assert.ok(sent[0].text.startsWith('TEST: alert delivery check for Megaloadon'));
  assert.ok(sent[0].text.endsWith(`Event: ${accepted.event_id}`));
  assert.equal(sent[0].parse_mode, undefined);
  assert.deepEqual(app.calls.map(x => x.url), [telegramURL]);
  assert.equal(JSON.stringify(status).includes(telegramToken), false);
  assert.equal(JSON.stringify(status).includes(telegramChat), false);
});

test('Telegram HTTP 200 with ok false stays durable, preserves order, and retries the same ID', { timeout: TEST_TIMEOUT_MS }, async t => {
  const sent = [];
  let accept = false;
  const app = await setup(t, null, telegramSecrets, null, async request => {
    sent.push({ body: await request.json(), event_id: request.headers.get('Idempotency-Key') });
    return accept ? telegramAccepted() : MFResponse.json({ ok: false, description: 'Private token must never be retained in errors' });
  });
  const first = await (await app.request('/test-alert', 'POST')).json();
  const second = await (await app.request('/test-alert', 'POST')).json();
  let status = await app.status();
  assert.equal(status.pending_alerts, 2);
  assert.equal(status.lastDeliveryMs, null);
  assert.equal(status.deliveryError, 'alert_delivery_telegram_response_invalid');
  assert.deepEqual(sent.map(x => x.event_id), [first.event_id, first.event_id]);
  await app.mf.setOptions(convertV4MiniflareOptions(app.options));
  accept = true;
  await app.request('/check', 'POST');
  await app.request('/check', 'POST');
  status = await app.status();
  assert.equal(status.pending_alerts, 0);
  assert.deepEqual(sent.map(x => x.event_id), [first.event_id, first.event_id, first.event_id, second.event_id]);
  assert.ok(sent.slice(0, 3).every(x => x.body.text.endsWith(`Event: ${first.event_id}`)));
});

test('Telegram redirects keep the token on the fixed API host and leave the alert pending', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null, telegramSecrets, null, () => new MFResponse(null,
    { status: 302, headers: { Location: 'https://unexpected.example.invalid/leak' } }));
  await app.request('/test-alert', 'POST');
  const status = await app.status();
  assert.equal(status.pending_alerts, 1);
  assert.equal(status.lastDeliveryMs, null);
  assert.equal(status.deliveryError, 'alert_delivery_http_302');
  assert.deepEqual(app.calls.map(x => x.url), [telegramURL]);
});

test('invalid Telegram settings do not fall back to a webhook or issue a request', { timeout: TEST_TIMEOUT_MS }, async t => {
  for (const bindings of [
    { TELEGRAM_BOT_TOKEN: telegramToken }, { TELEGRAM_CHAT_ID: telegramChat },
    { ...telegramSecrets, TELEGRAM_BOT_TOKEN: telegramToken + '/leak' },
    { ...telegramSecrets, TELEGRAM_CHAT_ID: '123456789/other' },
  ]) {
    const app = await setup(t, () => { throw new Error('Must not fall back to webhook'); }, bindings);
    await app.request('/test-alert', 'POST');
    const status = await app.status();
    assert.equal(status.alert_delivery_configured, false);
    assert.equal(status.alert_delivery_channel, null);
    assert.equal(status.deliveryError, 'alert_telegram_configuration_invalid');
    assert.equal(status.pending_alerts, 1);
    assert.equal(app.calls.length, 0);
  }
});

test('Telegram rejects malformed, oversized, and wrong-chat receipts', { timeout: TEST_TIMEOUT_MS }, async t => {
  for (const reply of [
    () => new MFResponse('{broken'),
    () => new MFResponse('x'.repeat(65537)),
    () => MFResponse.json({ ok: true }),
    () => MFResponse.json({ ok: true, result: { message_id: 1, chat: { id: 999 } } }),
  ]) {
    const app = await setup(t, null, telegramSecrets, null, reply);
    await app.request('/test-alert', 'POST');
    const status = await app.status();
    assert.equal(status.pending_alerts, 1);
    assert.equal(status.lastDeliveryMs, null);
    assert.equal(status.deliveryError, 'alert_delivery_telegram_response_invalid');
  }
});

test('Telegram messages remain within the API limit and keep the event ID', { timeout: TEST_TIMEOUT_MS }, async t => {
  const sent = [];
  const app = await setup(t, null, { ...telegramSecrets, MACHINE_LABEL: 'M'.repeat(5000) }, null, async request => {
    sent.push(await request.json()); return telegramAccepted();
  });
  const accepted = await (await app.request('/test-alert', 'POST')).json();
  assert.equal(sent[0].text.length, 4096);
  assert.ok(sent[0].text.endsWith(`Event: ${accepted.event_id}`));
  assert.equal((await app.status()).pending_alerts, 0);
});

test('public stream must produce two advancing blocks, not comments or repeated slots', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null, {}, () => new MFResponse(
    ': heartbeat\n\nevent: block\ndata: {"slot":200}\n\nevent: block\ndata: {"slot":200}\n\n',
    { headers: { 'Content-Type': 'text/event-stream' } }));
  const status = await (await app.request('/check', 'POST')).json();
  assert.equal(status.streamIssue, 'public_stream_stale');
  assert.equal(app.calls[0].authorization, null);
});

test('invalid public block data cannot pass the stream check', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null, {}, () => new MFResponse('event: block\ndata: {"slot":"wrong"}\n\n',
    { headers: { 'Content-Type': 'text/event-stream' } }));
  const status = await (await app.request('/check', 'POST')).json();
  assert.equal(status.streamIssue, 'public_stream_invalid');
});

test('reboot and a backward wall clock preserve machine liveness; retired boots cannot replay', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null);
  const first = sample();
  await app.request('/heartbeat', 'POST', hb, JSON.stringify(first));
  const reboot = { ...sample(), boot_id: '22222222-2222-2222-2222-222222222222', monotonic_ms: 1,
    sampled_unix_ms: first.sampled_unix_ms - 110000 };
  assert.equal((await app.request('/heartbeat', 'POST', hb, JSON.stringify(reboot))).status, 202);
  const backward = { ...reboot, monotonic_ms: 2, sampled_unix_ms: reboot.sampled_unix_ms - 200000 };
  assert.equal((await app.request('/heartbeat', 'POST', hb, JSON.stringify(backward))).status, 202);
  const status = await app.status();
  assert.equal(status.sample.boot_id, reboot.boot_id);
  assert.ok(status.sample.errors.includes('sender_clock_regressed'));
  assert.ok(status.sample.errors.includes('sender_clock_skew'));
  assert.ok(Date.now() - status.lastReceivedMs < 5000);
  assert.equal((await app.request('/heartbeat', 'POST', hb, JSON.stringify({ ...first, monotonic_ms: first.monotonic_ms + 1 }))).status, 400);
});

function command(updateId = 700, text = '/status') {
  return { update_id: updateId, message: { message_id: updateId + 1, text,
    chat: { id: Number(telegramChat), type: 'private' }, from: { id: Number(telegramChat), is_bot: false } } };
}
async function eventually(read, accepts) {
  const end = Date.now() + 1500;
  let value;
  do {
    value = await read();
    if (accepts(value)) return value;
    await new Promise(resolve => setTimeout(resolve, 10));
  } while (Date.now() < end);
  assert.fail('Timed out waiting for the durable command receipt: ' + JSON.stringify(value));
}

test('Telegram webhook requires its own secret and rejects bounded invalid input', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null, commandSecrets, null, telegramAccepted);
  for (const headers of [admin, hb, {}, { ...tg, 'X-Telegram-Bot-Api-Secret-Token': 'wrong' }]) {
    assert.equal((await app.request('/telegram', 'POST', headers, JSON.stringify(command()))).status, 401);
  }
  for (const payload of ['{broken', 'x'.repeat(65537), JSON.stringify({ update_id: -1 })]) {
    assert.equal((await app.request('/telegram', 'POST', tg, payload)).status, 400);
  }
  assert.equal(app.calls.length, 0);
});

test('unknown private identities, groups, bots, and commands receive no information', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null, commandSecrets, null, telegramAccepted);
  for (const change of [
    { chat: { id: 999, type: 'private' } }, { from: { id: 999, is_bot: false } },
    { chat: { id: Number(telegramChat), type: 'group' } }, { from: { id: Number(telegramChat), is_bot: true } },
    { text: '/status@another_bot' }, { text: '/restart' }, { text: '/status token=private' }, { text: 'a'.repeat(257) },
  ]) {
    const payload = command(); payload.message = { ...payload.message, ...change };
    assert.equal((await app.request('/telegram', 'POST', tg, JSON.stringify(payload))).status, 200);
  }
  assert.equal(app.calls.length, 0);
  assert.equal((await app.status()).pending_commands, 0);
  assert.equal(JSON.stringify(await app.status()).includes(telegramChat), false);
});

test('private status reply is silent and reports live compaction and upload', { timeout: TEST_TIMEOUT_MS }, async t => {
  const sent = [];
  const app = await setup(t, null, commandSecrets, null, async request => { sent.push(await request.json()); return telegramAccepted(); });
  await app.request('/heartbeat', 'POST', hb, JSON.stringify(sample()));
  const response = await app.request('/telegram', 'POST', tg, JSON.stringify(command()));
  assert.equal(response.status, 200);
  const status = await eventually(app.status, value => value.lastCommandDeliveryMs > 0 && value.pending_commands === 0);
  assert.equal(status.telegram_commands_configured, true);
  assert.equal(status.commandDeliveryError, null);
  assert.equal(sent.length, 1);
  assert.equal(sent[0].chat_id, telegramChat);
  assert.equal(sent[0].disable_notification, true);
  assert.deepEqual(sent[0].reply_parameters, { message_id: 701, allow_sending_without_reply: true });
  assert.ok(sent[0].text.includes('Live compaction: RUNNING; slot 200; lag 0 slots'));
  assert.ok(sent[0].text.includes('Live upload: RUNNING; event 450; lag 50 events'));
  assert.ok(sent[0].text.endsWith('Request: 700'));
  assert.equal(sent[0].text.includes('local-admin-token-only'), false);
  assert.equal(sent[0].text.includes(commandSecret), false);
  assert.equal(app.calls[0].authorization, null);
});

test('duplicate status updates remain deduplicated after restart', { timeout: TEST_TIMEOUT_MS }, async t => {
  const sent = [];
  const app = await setup(t, null, commandSecrets, null, async request => { sent.push(await request.json()); return telegramAccepted(); });
  await app.request('/telegram', 'POST', tg, JSON.stringify(command()));
  await eventually(app.status, value => value.lastCommandDeliveryMs > 0 && value.pending_commands === 0);
  await app.mf.setOptions(convertV4MiniflareOptions(app.options));
  await app.request('/telegram', 'POST', tg, JSON.stringify(command()));
  await app.request('/check', 'POST');
  assert.equal(sent.length, 1);
  assert.equal((await app.status()).pending_commands, 0);
});

test('failed command delivery keeps its reply and request ID across restart', { timeout: TEST_TIMEOUT_MS }, async t => {
  const sent = []; let accept = false;
  const app = await setup(t, null, commandSecrets, null, async request => {
    sent.push(await request.json()); return accept ? telegramAccepted() : MFResponse.json({ ok: false });
  });
  await app.request('/telegram', 'POST', tg, JSON.stringify(command(702, '/help')));
  await eventually(app.status, value => value.commandDeliveryError === 'command_delivery_telegram_response_invalid');
  const firstText = sent[0].text;
  await app.mf.setOptions(convertV4MiniflareOptions(app.options));
  accept = true;
  await app.request('/check', 'POST');
  assert.equal((await app.status()).pending_commands, 0);
  assert.deepEqual(sent.map(value => value.text), [firstText, firstText]);
  assert.ok(firstText.includes('/status - Show the last machine report'));
});

test('command redirects and wrong-chat receipts cannot pass delivery checks', { timeout: TEST_TIMEOUT_MS }, async t => {
  for (const [reply, error] of [
    [() => new MFResponse(null, { status: 302, headers: { Location: 'https://unexpected.example.invalid/leak' } }), 'command_delivery_http_302'],
    [() => MFResponse.json({ ok: true, result: { message_id: 1, chat: { id: 999 } } }), 'command_delivery_telegram_response_invalid'],
  ]) {
    const app = await setup(t, null, commandSecrets, null, reply);
    await app.request('/telegram', 'POST', tg, JSON.stringify(command()));
    const status = await eventually(app.status, value => value.commandDeliveryError === error);
    assert.equal(status.pending_commands, 1);
    assert.equal(status.lastCommandDeliveryMs ?? null, null);
    assert.deepEqual(app.calls.map(value => value.url), [telegramURL]);
  }
});

test('changing the configured recipient cannot redirect a queued command reply', { timeout: TEST_TIMEOUT_MS }, async t => {
  const app = await setup(t, null, commandSecrets, null, () => MFResponse.json({ ok: false }));
  await app.request('/telegram', 'POST', tg, JSON.stringify(command()));
  await eventually(app.status, value => value.commandDeliveryError === 'command_delivery_telegram_response_invalid');
  app.options.bindings.TELEGRAM_CHAT_ID = '987654321';
  await app.mf.setOptions(convertV4MiniflareOptions(app.options));
  await app.request('/check', 'POST');
  const status = await app.status();
  assert.equal(status.commandDeliveryError, 'command_telegram_configuration_changed');
  assert.equal(status.pending_commands, 1);
  assert.deepEqual(app.calls.map(value => value.url), [telegramURL, 'https://stream.blockzilla.dev/v1/blockhashes']);
});

test('a command delivery lease prevents duplicate sends and preserves queue order', { timeout: TEST_TIMEOUT_MS }, async t => {
  let release, entered;
  const gate = new Promise(resolve => { release = resolve; });
  const started = new Promise(resolve => { entered = resolve; });
  t.after(() => release()); t.signal.addEventListener('abort', () => release(), { once: true });
  const sent = [];
  const app = await setup(t, null, commandSecrets, null, async request => {
    sent.push(await request.json());
    if (sent.length === 1) { entered(); await gate; }
    return telegramAccepted();
  });
  await app.request('/telegram', 'POST', tg, JSON.stringify(command(800)));
  await started;
  await app.request('/telegram', 'POST', tg, JSON.stringify(command(800)));
  await app.request('/telegram', 'POST', tg, JSON.stringify(command(799, '/help')));
  assert.equal(sent.length, 1);
  assert.equal((await app.status()).pending_commands, 2);
  release();
  await eventually(app.status, value => value.lastCommandDeliveryMs > 0 && value.pending_commands === 1);
  await app.request('/check', 'POST');
  assert.equal((await app.status()).pending_commands, 0);
  assert.deepEqual(sent.map(value => value.text.match(/Request: (\d+)$/)[1]), ['800', '799']);
});
