// Opt-in backend bank stream. This module does not change the v1 SDK.
import { StreamError, MAX_FRAME, MAX_META } from './index.mjs';
const encoder = new TextEncoder(), text = new TextDecoder('utf-8', { fatal: true });
const PROFILE = 'native-bank-compact-v2';
const check = (ok, code, detail) => { if (!ok) throw new StreamError(code, detail); };
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
const sourceFields = ['genesis_hash', 'node_identity', 'agave_revision', 'session_id'];
const sameSource = (a, b) => sourceFields.every(k => a[k] === b[k]);
const sha = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))].map(n => n.toString(16).padStart(2, '0')).join('');
export function bankCursor(value) {
  const m = typeof value === 'string' && /^([a-zA-Z0-9-]{1,128}):p(0|[1-9][0-9]*):(-|[a-f0-9]{64})$/.exec(value);
  check(m && Number.isSafeInteger(+m[2]) && (m[3] !== '-' || +m[2] === 0), 'invalid_cursor');
  return { session: m[1], sequence: +m[2], digest: m[3] };
}
/** The shared Rust parser validates required nulls, native expiry, and objects. */
export async function loadBankDecoder(wasm = new URL('./decoder-banks-v2.wasm', import.meta.url)) {
  if (wasm instanceof URL || typeof wasm === 'string') {
    const url = new URL(wasm, import.meta.url);
    if (url.protocol === 'file:' && typeof process !== 'undefined' && process.versions?.node) wasm = await (await import('node:fs/promises')).readFile(url);
    else { const r = await fetch(url, { redirect: 'error' }); check(r.ok, 'decoder_download_failed'); wasm = await r.arrayBuffer(); }
  }
  const result = await WebAssembly.instantiate(wasm, {}), api = (result instanceof WebAssembly.Instance ? result : result.instance).exports;
  check(api.memory instanceof WebAssembly.Memory && ['bz_alloc','bz_free','bz_decode_banks','bz_validate_bank_head'].every(k => typeof api[k] === 'function'), 'invalid_bank_decoder_abi');
  function call(name, bytes, config, limit) {
    check(bytes instanceof Uint8Array && bytes.length <= limit, 'decoder_input_limit_exceeded');
    const options = encoder.encode(JSON.stringify(config)); check(options.length <= MAX_META, 'options_too_large');
    let input = 0, opts = 0, output = 0, n = 0;
    try {
      input = api.bz_alloc(bytes.length); new Uint8Array(api.memory.buffer, input, bytes.length).set(bytes);
      opts = api.bz_alloc(options.length); new Uint8Array(api.memory.buffer, opts, options.length).set(options);
      const packed = api[name](input, bytes.length, opts, options.length);
      output = Number(packed & 0xffffffffn); n = Number(packed >> 32n);
      check(n > 0 && n <= 32 * 1024 * 1024, 'decoded_output_limit_exceeded');
      const value = JSON.parse(text.decode(new Uint8Array(api.memory.buffer, output, n)));
      check(!value.error, 'bank_decode_failed', { reason: value.error }); return value;
    } finally {
      if (output) api.bz_free(output, n);
      if (opts) api.bz_free(opts, options.length);
      if (input) api.bz_free(input, bytes.length);
    }
  }
  return {
    decode(wire, { includeDetails = true } = {}) {
      check(typeof includeDetails === 'boolean', 'invalid_filter_options');
      // Signature watches must see failed executions and every transaction.
      return call('bz_decode_banks', wire, { programs: [], accounts: [], successful_only: false, include_details: includeDetails }, MAX_FRAME);
    },
    validateHead(head, source, now = Date.now()) { return call('bz_validate_bank_head', encoder.encode(JSON.stringify(head)), { source, now }, MAX_META); },
    get memoryBytes() { return api.memory.buffer.byteLength; },
  };
}
export async function decodeBankFrame(wire, expectedDigest, source, codec) {
  check(wire instanceof Uint8Array && wire.length >= 16 && wire.length <= MAX_FRAME, 'invalid_frame_size');
  const digest = await sha(wire); check(digest === expectedDigest, 'chunk_digest_mismatch');
  // Rust validates lengths, all envelope fields, identities, object SHA and semantics.
  const decoded = codec.decode(wire), n = new DataView(wire.buffer, wire.byteOffset, wire.length).getUint32(8, true);
  const envelope = JSON.parse(text.decode(wire.subarray(16, 16 + n)));
  check(decoded.profile === PROFILE && sameSource(envelope.marker, source), 'source_changed');
  const cursor = `${source.session_id}:p${envelope.event.sequence}:${digest}`; bankCursor(cursor);
  return { wire, digest, cursor, event: envelope.event, decoded };
}
export function noticePreflight(notice, id, next, source) {
  const keys = ['cursor','sha256','bytes','start_slot','end_slot','url'];
  check(notice && Object.keys(notice).length === keys.length && keys.every(k => Object.hasOwn(notice, k)), 'invalid_notice');
  const cursor = bankCursor(notice.cursor);
  check(id === notice.cursor && cursor.session === source.session_id && cursor.sequence === next && cursor.digest === notice.sha256
    && Number.isSafeInteger(notice.bytes) && notice.bytes >= 16 && notice.bytes <= MAX_FRAME
    && Number.isSafeInteger(notice.start_slot) && notice.start_slot >= 0 && notice.end_slot === notice.start_slot
    && notice.url === `/v2/banks/chunks/${notice.sha256}`, 'invalid_notice');
}
const observation = f => ({ bank: f.event.bank, block_height: f.event.block_height, payload_sha256: f.event.payload_sha256,
  payload_frame_sha256: f.event.payload_frame_sha256, recent_blockhash: f.event.recent_blockhash,
  observed_at_unix_ms: f.event.observed_at_unix_ms, commitment: f.event.commitment, event_sequence: f.event.sequence, event_cursor: f.cursor });
const parentMatches = (a, b) => a.parent_slot === b.slot && same(a.parent_blockhash, b.blockhash);
/** Keep bounded metadata proofs, not full decoded transaction payloads. */
export class BankState {
  constructor(source, cursor, codec, { wallClock = Date.now, monotonicClock = () => performance.now() } = {}) {
    check(bankCursor(cursor).session === source.session_id, 'source_changed');
    this.source = structuredClone(source); this.cursor = cursor; this.codec = codec; this.wall = wallClock; this.mono = monotonicClock;
    this.proofs = new Map(); this.selected = null; this.head = null; this.failed = false;
  }
  hasPayload(digest) { return this.proofs.has(digest); }
  addPayload(f) {
    check(f.event.kind === 'block', 'invalid_dependency');
    const old = this.proofs.get(f.digest);
    if (old) { check(same(old.event, f.event), 'payload_changed'); return; }
    check(this.proofs.size < 16384, 'client_bank_queue_overflow');
    this.proofs.set(f.digest, { event: structuredClone(f.event), rooted: false, invalidated: false });
  }
  clock(at) { check(Number.isSafeInteger(at) && at <= this.wall() + 5000, 'future_source_clock'); }
  match(event, proof) {
    check(proof && same(event.bank, proof.event.bank) && event.payload_sha256 === proof.event.payload_sha256
      && (event.block_height === null || event.block_height === proof.event.block_height), 'payload_mismatch');
  }
  select(c) {
    this.clock(c.observed_at_unix_ms);
    if (!this.selected || c.block_height > this.selected.block_height) {
      this.selected = structuredClone(c); this.selectedAt = this.mono(); this.selectedAge = Math.max(0, this.wall() - c.observed_at_unix_ms);
    }
  }
  resolveHead(head, control, payload) {
    head = this.codec.validateHead(head, this.source, this.wall());
    check(['confirmed','root'].includes(control.event.kind) && same(head.latest_confirmed, observation(control)), 'head_control_mismatch');
    check(control.event.payload_frame_sha256 === payload.digest, 'payload_mismatch'); this.match(control.event, { event: payload.event });
    this.addPayload(payload); const p = this.proofs.get(payload.digest); check(!p.invalidated, 'invalidated_bank'); p.rooted ||= control.event.kind === 'root';
    this.select(observation(control));
  }
  health(head) {
    this.head = this.codec.validateHead(head, this.source, this.wall()); this.healthAt = this.mono();
    if (head.status === 'failed') { this.failed = true; this.selected = null; }
  }
  apply(f, dependency) {
    check(!this.failed, 'source_failed'); this.clock(f.event.observed_at_unix_ms);
    if (f.cursor === this.cursor) return false;
    check(bankCursor(f.cursor).session === this.source.session_id && f.event.sequence === bankCursor(this.cursor).sequence + 1, 'event_sequence_gap');
    const e = f.event;
    if (e.kind === 'block') this.addPayload(f);
    else {
      if (dependency) { check(e.payload_frame_sha256 === dependency.digest, 'payload_mismatch'); this.match(e, { event: dependency.event }); this.addPayload(dependency); }
      const p = this.proofs.get(e.payload_frame_sha256); this.match(e, p);
      if (e.kind === 'invalidate') {
        const removed = new Set([e.payload_frame_sha256]); let changed = true;
        while (changed) {
          changed = false;
          for (const [sha, proof] of this.proofs) if (!removed.has(sha) && [...removed].some(key => parentMatches(proof.event.bank, this.proofs.get(key).event.bank))) { removed.add(sha); changed = true; }
        }
        if ([...removed].some(key => this.proofs.get(key).rooted)) { this.failed = true; this.selected = null; throw new StreamError('rooted_bank_invalidated'); }
        for (const key of removed) this.proofs.get(key).invalidated = true;
        if (this.selected && removed.has(this.selected.payload_frame_sha256)) { this.failed = true; this.selected = null; }
      } else {
        check(!p.invalidated, 'invalidated_bank'); p.rooted ||= e.kind === 'root'; this.select(observation(f));
      }
    }
    this.cursor = f.cursor;
    if (this.selected && this.proofs.size > 8192) for (const [key, p] of this.proofs) if (p.event.block_height < this.selected.block_height - 4096) this.proofs.delete(key);
    return true;
  }
  readyHash() {
    check(!this.failed, 'source_failed'); check(this.head, 'health_unavailable');
    this.codec.validateHead(this.head, this.source, this.wall());
    check(this.head.status === 'live' && this.wall() - this.head.sampled_at_unix_ms <= 5000 && this.mono() - this.healthAt <= 5000, 'source_stale');
    const c = this.selected, a = this.head.latest_confirmed;
    check(c && a, 'confirmed_hash_unavailable');
    check((same(c, a) || (c.event_sequence > a.event_sequence && c.block_height > a.block_height))
      && bankCursor(this.cursor).sequence >= bankCursor(this.head.cursor).sequence, 'client_not_caught_up');
    check(this.selectedAge + this.mono() - this.selectedAt <= 15000 && this.wall() - c.observed_at_unix_ms <= 5000, 'confirmed_hash_expired_by_age');
    return structuredClone({ ...c.recent_blockhash, block_height: c.block_height, commitment: c.commitment });
  }
}
async function readBounded(response, max) {
  check(response.body, 'missing_response_body'); const reader = response.body.getReader(); let count = 0; const parts = [];
  try { for (;;) { const { done, value } = await reader.read(); if (done) break; count += value.length; check(count <= max, 'response_too_large'); parts.push(value); } }
  finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
  const out = new Uint8Array(count); let at = 0; for (const p of parts) { out.set(p, at); at += p.length; } return out;
}
async function* bankSse(body) {
  const reader = body.getReader(), decoder = new TextDecoder('utf-8', { fatal: true });
  let pending = '', event = null, id = null, data = [], size = 0;
  try {
    for (;;) {
      let timer;
      const read = await Promise.race([reader.read(), new Promise((_, reject) => { timer = setTimeout(() => reject(new TypeError('stream_idle_timeout')), 15000); })]).finally(() => clearTimeout(timer));
      if (read.done) return; pending += decoder.decode(read.value, { stream: true });
      check(pending.length <= 2 * MAX_META, 'sse_read_too_large'); let end;
      while ((end = pending.indexOf('\n')) >= 0) {
        let line = pending.slice(0, end).replace(/\r$/, ''); pending = pending.slice(end + 1); size += encoder.encode(line).length;
        check(size <= MAX_META, 'sse_event_too_large');
        if (!line) { if (data.length) yield { event, id, data: data.join('\n') }; event = null; id = null; data = []; size = 0; continue; }
        if (line.startsWith(':')) continue;
        const colon = line.indexOf(':'), key = colon < 0 ? line : line.slice(0, colon), value = (colon < 0 ? '' : line.slice(colon + 1)).replace(/^ /, '');
        if (key === 'event') { check(event === null, 'duplicate_sse_event'); event = value; }
        if (key === 'id') { check(id === null && !value.includes('\0'), 'invalid_sse_id'); id = value; }
        if (key === 'data') data.push(value);
      }
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
// At most four bounded frame reads. Keep results in notice order and stop all
// pending reads when the application closes or the connection retries.
async function* orderedPrefetch(input, map, stop) {
  const iterator = input[Symbol.asyncIterator](), queue = [], waiters = new Set();
  let ended = false, stopped = false, error;
  const notify = () => { for (const wake of waiters) wake(); waiters.clear(); };
  const wait = () => new Promise(resolve => waiters.add(resolve));
  const pump = (async () => {
    try {
      for (;;) {
        while (queue.length >= 4 && !stopped) await wait();
        if (stopped) return;
        const item = await iterator.next();
        if (item.done) break;
        const value = Promise.resolve().then(() => map(item.value)); value.catch(() => {});
        queue.push(value); notify();
      }
    } catch (failure) { error = failure; }
    finally { ended = true; notify(); }
  })();
  try {
    for (;;) {
      while (!queue.length && !ended) await wait();
      if (!queue.length) { if (error) throw error; return; }
      const value = await queue[0]; queue.shift(); notify(); yield value;
    }
  } finally { stopped = true; stop(); notify(); await iterator.return?.().catch(() => {}); await pump; }
}
/** Keep the API key in the server process. Commit application data + cursor before ack. */
export function createBankClient({ url, apiKey, source, decoder, wasm, fetchImpl = fetch, reconnect = true, wallClock, monotonicClock }) {
  const base = new URL(url);
  check(base.protocol === 'https:' && base.pathname === '/' && !base.search && !base.hash && !base.username && !base.password, 'invalid_https_base');
  check(typeof apiKey === 'string' && apiKey.length >= 32 && apiKey.length <= 256 && !/[\r\n]/.test(apiKey), 'invalid_api_key');
  const codecReady = decoder ? Promise.resolve(decoder) : loadBankDecoder(wasm); codecReady.catch(() => {});
  let state = null;
  async function request(path, signal, streaming = false) {
    const timeout = AbortSignal.timeout(streaming ? 24 * 60 * 60 * 1000 : 15000);
    const combined = signal ? AbortSignal.any([signal, timeout]) : timeout;
    const r = await fetchImpl(new URL(path, base), { headers: { Authorization: `Bearer ${apiKey}`, 'User-Agent': 'blockzilla-bank-client/2' }, signal: combined, redirect: 'error', cache: 'no-store' });
    check(!r.redirected, 'redirect_rejected');
    if ([400,401,403,410].includes(r.status)) {
      let detail; if (r.status === 400 || r.status === 410) detail = JSON.parse(text.decode(await readBounded(r, MAX_META)));
      if (detail?.head) (await codecReady).validateHead(detail.head, source, (wallClock || Date.now)());
      throw new StreamError(`http_${r.status}`, detail);
    }
    if (!r.ok) throw new TypeError(`stream_http_${r.status}`); return r;
  }
  async function getFrame(digest, signal) {
    check(/^[a-f0-9]{64}$/.test(digest), 'invalid_frame_digest');
    const r = await request(`/v2/banks/chunks/${digest}`, signal);
    return decodeBankFrame(await readBounded(r, MAX_FRAME), digest, source, await codecReady);
  }
  return {
    readyHash() { check(state, 'health_unavailable'); return state.readyHash(); },
    async *stream({ after, signal, onBootstrap, onCheckpoint, onStatus } = {}) {
      const codec = await codecReady;
      const head = codec.validateHead(JSON.parse(text.decode(await readBounded(await request('/v2/banks/head', signal), MAX_META))), source, (wallClock || Date.now)());
      after ??= head.cursor; check(after, 'source_not_started'); state = new BankState(source, after, codec, { wallClock, monotonicClock });
      if (head.latest_confirmed) {
        const c = head.latest_confirmed, control = await getFrame(bankCursor(c.event_cursor).digest, signal), payload = await getFrame(c.payload_frame_sha256, signal);
        state.resolveHead(head, control, payload); await onBootstrap?.({ after, head, control, payload });
      }
      state.health(head); let delay = 250;
      for (;;) {
        const connection = new AbortController(), connectionSignal = signal ? AbortSignal.any([signal, connection.signal]) : connection.signal;
        try {
          const r = await request(`/v2/banks/events?after=${encodeURIComponent(after)}`, connectionSignal, true);
          check(r.headers.get('x-blockzilla-start-cursor') === after, 'start_cursor_mismatch');
          check(r.headers.get('content-type')?.startsWith('text/event-stream'), 'wrong_sse_content_type');
          let initial = true, received = bankCursor(after).sequence;
          const pending = orderedPrefetch(bankSse(r.body), async notice => {
            if (initial) { check(notice.event === 'status' && notice.id === null, 'missing_initial_status'); initial = false; }
            const value = JSON.parse(notice.data); let frame = null;
            if (notice.event === 'chunk') {
              noticePreflight(value, notice.id, ++received, source);
              frame = await getFrame(value.sha256, connectionSignal);
              check(frame.cursor === value.cursor && frame.wire.length === value.bytes && frame.event.bank.slot === value.start_slot, 'notice_frame_mismatch');
            } else check(['status','gap'].includes(notice.event) && notice.id === null, 'invalid_sse_event');
            return { notice, value, frame };
          }, () => connection.abort());
          for await (const { notice, value, frame } of pending) {
            if (notice.event === 'status') { check(notice.id === null, 'status_has_id'); state.health(value); await onStatus?.(structuredClone(state.head)); continue; }
            if (notice.event === 'gap') {
              check(notice.id === null && Object.keys(value).length === 2 && ['source_changed','replay_unavailable','source_failed','dependency_unavailable'].includes(value.error), 'invalid_gap');
              state.health(value.head); throw new StreamError(value.error, { head: value.head, reconciliationRequired: true });
            }
            check(notice.event === 'chunk', 'unsupported_sse_event'); const next = bankCursor(after).sequence + 1;
            noticePreflight(value, notice.id, next, source);
            check(frame.cursor === value.cursor && frame.wire.length === value.bytes && frame.event.bank.slot === value.start_slot, 'notice_frame_mismatch');
            const ref = frame.event.payload_frame_sha256, dependency = ref && !state.hasPayload(ref) ? await getFrame(ref, connectionSignal) : null;
            state.apply(frame, dependency); let acknowledged = false;
            yield { ...frame, dependency, ack: async () => { if (!acknowledged) { await onCheckpoint?.(frame.cursor); after = frame.cursor; acknowledged = true; } } };
            check(acknowledged, 'event_not_acknowledged'); delay = 250;
          }
          if (!reconnect || signal?.aborted) return;
        } catch (error) {
          if (signal?.aborted) return;
          if (!reconnect || error instanceof StreamError || !(error instanceof TypeError || error.name === 'TimeoutError')) throw error;
        } finally { connection.abort(); }
        await new Promise(resolve => { const timer = setTimeout(resolve, delay); signal?.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true }); }); delay = Math.min(delay * 2, 10000);
      }
    },
  };
}
