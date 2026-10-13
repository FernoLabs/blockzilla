// Browser/Node transport. Compact transaction decoding is a separate binding.
export const MAX_META = 64 * 1024;
export const MAX_PAYLOAD = 16 * 1024 * 1024;
export const MAX_FRAME = 16 + MAX_META + MAX_PAYLOAD;
const MAGIC = new TextEncoder().encode('BZSTRM01');
const required = ['DIRECT-AGAVE-NORMALIZED.v1.json', 'archive-v2-blocks.index',
  'archive-v2-blocks.zstd', 'archive-v2-meta.wincode', 'available-poh.wincode',
  'blockhash_registry.bin', 'registry.bin', 'signatures.bin'];
const allowed = new Set([...required, 'prev_blockhash_tail.bin', 'registry.mphf',
  'registry_counts.bin', 'vote_hash_registry.bin']);
export class StreamError extends Error {
  constructor(code, detail) { super(code); this.name = 'StreamError'; this.code = code; this.detail = detail; }
}
const check = (condition, code) => { if (!condition) throw new StreamError(code); };
const uint = n => Number.isSafeInteger(n) && n >= 0;
export function parseCursor(s) {
  check(typeof s === 'string' && s.length <= 240, 'invalid_cursor');
  const m = /^([a-zA-Z0-9-]{1,128}):(p?(?:0|[1-9][0-9]*)):(-|[a-f0-9]{64})$/.exec(s);
  check(m && uint(Number(m[2].replace(/^p/, ''))), 'invalid_cursor');
  return { session: m[1], slot: Number(m[2].replace(/^p/, '')), processed: m[2].startsWith('p'), digest: m[3] };
}
const hash = async bytes => [...new Uint8Array(await crypto.subtle.digest('SHA-256', bytes))]
  .map(n => n.toString(16).padStart(2, '0')).join('');
function lengths(h) {
  check(h.length === 16 && MAGIC.every((b, i) => h[i] === b), 'invalid_frame_magic');
  const v = new DataView(h.buffer, h.byteOffset, h.byteLength);
  const meta = v.getUint32(8, true), payload = v.getUint32(12, true);
  check(meta > 0 && meta <= MAX_META && payload <= MAX_PAYLOAD, 'frame_too_large');
  return { meta, payload, total: 16 + meta + payload };
}
export async function decodeFrame(wire, expectedDigest) {
  check(wire instanceof Uint8Array && wire.length >= 16, 'truncated_frame');
  const size = lengths(wire.subarray(0, 16)); check(wire.length === size.total, 'truncated_or_trailing_frame');
  let envelope;
  try { envelope = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(wire.subarray(16, 16 + size.meta))); }
  catch { throw new StreamError('invalid_envelope_json'); }
  check(envelope.schema === 'blockzilla-stream-envelope-v1' && ['native-compact-projection-v1', 'native-processed-compact-v1'].includes(envelope.profile)
    && envelope.evidence_included === false, 'unsupported_profile');
  const m = envelope.marker;
  const processed = envelope.profile === 'native-processed-compact-v1';
  check(m?.state === 'COMPLETE' && m.source_profile === 'agave-bank-callbacks-v1'
    && m.finalized === !processed && m.canonical === false && m.publication_ready === false
    && (processed || (envelope.event == null && m.schema === 'hivezilla-direct-agave-compact-v1'
      && m.finality_basis === 'operator-trusted-agave-rooted-bank-callback')), 'invalid_provenance');
  for (const k of ['genesis_hash', 'node_identity', 'agave_revision']) check(typeof m[k] === 'string' && m[k].length > 0 && m[k].length <= 128, 'missing_source_identity');
  const event = envelope.event;
  if (processed) {
    check(event && uint(event.sequence) && event.sequence > 0 && uint(event.source_sequence) && event.source_sequence > 0
      && ['block', 'invalidate', 'root'].includes(event.kind) && /^[a-f0-9]{64}$/.test(event.payload_sha256), 'invalid_bank_event');
    const bank = event.bank;
    check(bank && uint(bank.slot) && uint(bank.bank_id) && uint(bank.parent_slot) && bank.parent_slot < bank.slot
      && ['blockhash', 'parent_blockhash'].every(k => Array.isArray(bank[k]) && bank[k].length === 32 && bank[k].every(n => uint(n) && n < 256)), 'invalid_bank_identity');
    if (event.kind !== 'block') {
      check(m.schema === 'hivezilla-processed-control-v1' && Array.isArray(envelope.objects) && envelope.objects.length === 0 && size.payload === 0 && m.produced_blocks == null && m.files == null, 'invalid_control_payload');
      const digest = await hash(wire); check(!expectedDigest || digest === expectedDigest, 'chunk_digest_mismatch');
      const cursor = `${m.session_id}:p${event.sequence}:${digest}`; parseCursor(cursor);
      return { cursor, digest, wire, envelope, objects: new Map(), blocks: [] };
    }
    check(m.schema === 'hivezilla-direct-agave-processed-compact-v1'
      && m.finality_basis === 'operator-trusted-agave-processed-bank-callback'
      && m.produced_blocks?.length === 1
      && ['slot','bank_id','parent_slot','blockhash','parent_blockhash'].every(k => JSON.stringify(m.produced_blocks[0][k]) === JSON.stringify(bank[k])), 'event_bank_mismatch');
  }
  check(Array.isArray(m.produced_blocks) && m.produced_blocks.length > 0 && m.produced_blocks.length <= 32
    && m.blocks === m.produced_blocks.length, 'invalid_block_count');
  const first = m.produced_blocks[0], last = m.produced_blocks.at(-1);
  check(m.start_slot === first.slot && m.end_slot_exclusive === last.slot + 1
    && m.epoch === Math.floor(first.slot / 432000) && Math.floor(last.slot / 432000) === m.epoch, 'invalid_range');
  for (const [i, b] of m.produced_blocks.entries()) {
    check(uint(b.slot) && uint(b.parent_slot) && b.parent_slot < b.slot && b.block_id === i, 'invalid_block_identity');
    for (const k of ['blockhash', 'parent_blockhash']) check(Array.isArray(b[k]) && b[k].length === 32 && b[k].every(n => uint(n) && n < 256), 'invalid_block_hash');
    if (i) check(b.parent_slot === m.produced_blocks[i - 1].slot
      && JSON.stringify(b.parent_blockhash) === JSON.stringify(m.produced_blocks[i - 1].blockhash), 'parent_chain_gap');
  }
  check(Array.isArray(envelope.objects) && envelope.objects.length <= allowed.size && Array.isArray(m.files) && m.files.length <= 32, 'invalid_object_count');
  const payload = wire.subarray(16 + size.meta), objects = new Map(); let offset = 0;
  for (const o of envelope.objects) {
    check(allowed.has(o.path) && !objects.has(o.path) && o.offset === offset && uint(o.bytes), 'invalid_object_layout');
    offset += o.bytes; check(uint(offset) && offset <= payload.length, 'invalid_object_layout');
    const bytes = payload.subarray(o.offset, offset);
    check(await hash(bytes) === o.sha256, 'object_digest_mismatch');
    const matches = m.files.filter(f => f.path === o.path);
    check(matches.length === 1 && matches[0].bytes === o.bytes && matches[0].sha256 === o.sha256, 'marker_object_mismatch');
    objects.set(o.path, bytes);
  }
  check(offset === payload.length && required.every(p => objects.has(p))
    && m.files.filter(f => allowed.has(f.path)).every(f => objects.has(f.path)), 'missing_decode_dependency');
  const digest = await hash(wire);
  check(!expectedDigest || digest === expectedDigest, 'chunk_digest_mismatch');
  const cursor = `${m.session_id}:${processed ? 'p' + event.sequence : last.slot}:${digest}`; parseCursor(cursor);
  return { cursor, digest, wire, envelope, objects, blocks: m.produced_blocks };
}
async function readWithDeadline(reader, milliseconds) {
  let timer;
  try {
    return await Promise.race([reader.read(), new Promise((_, reject) => {
      timer = setTimeout(() => reject(new TypeError('stream_idle_timeout')), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
}
async function smallJson(response) {
  const reader = response.body.getReader(); let bytes = 0; const parts = [];
  try {
    for (;;) {
      const { value, done } = await readWithDeadline(reader, 30000); if (done) break;
      bytes += value.length; check(bytes <= MAX_META, 'head_too_large'); parts.push(value);
    }
    const all = new Uint8Array(bytes); let offset = 0;
    for (const p of parts) { all.set(p, offset); offset += p.length; }
    return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(all));
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
export async function* binaryFrames(body, idleTimeoutMs = 30000) {
  const reader = body.getReader(); let pending = new Uint8Array(0), pos = 0;
  async function exact(n, eofAllowed = false) {
    const out = new Uint8Array(n); let written = 0;
    while (written < n) {
      if (pos === pending.length) {
        const { value, done } = await readWithDeadline(reader, idleTimeoutMs);
        if (done) { if (eofAllowed && written === 0) return null; throw new StreamError('truncated_frame'); }
        pending = value; pos = 0;
        check(pending instanceof Uint8Array && pending.length <= 2 * MAX_FRAME, 'network_read_too_large');
      }
      const take = Math.min(n - written, pending.length - pos);
      out.set(pending.subarray(pos, pos + take), written); written += take; pos += take;
    }
    return out;
  }
  try {
    for (;;) {
      const h = await exact(16, true); if (!h) return;
      const size = lengths(h), wire = new Uint8Array(size.total); wire.set(h);
      wire.set(await exact(size.total - 16), 16); yield await decodeFrame(wire);
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
export async function* sseEvents(body) {
  const reader = body.getReader(), decoder = new TextDecoder('utf-8', { fatal: true });
  let pending = '', event = '', id = '', data = [], eventBytes = 0;
  try {
    for (;;) {
      const { value, done } = await readWithDeadline(reader, 30000); if (done) return;
      pending += decoder.decode(value, { stream: true });
      let i;
      while ((i = pending.indexOf('\n')) >= 0) {
        let line = pending.slice(0, i); pending = pending.slice(i + 1);
        if (line.endsWith('\r')) line = line.slice(0, -1);
        eventBytes += line.length; check(eventBytes <= MAX_META, 'sse_event_too_large');
        if (!line) {
          if (data.length) yield { event, id, data: data.join('\n') };
          event = ''; id = ''; data = []; eventBytes = 0; continue;
        }
        if (line[0] === ':') continue;
        const colon = line.indexOf(':'), key = colon < 0 ? line : line.slice(0, colon);
        let val = colon < 0 ? '' : line.slice(colon + 1); if (val[0] === ' ') val = val.slice(1);
        if (key === 'event') event = val;
        if (key === 'id') { check(!val.includes('\0'), 'invalid_sse_id'); id = val; }
        if (key === 'data') data.push(val);
      }
      check(pending.length <= MAX_META, 'sse_event_too_large');
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
}
async function checkedResponse(fetchImpl, url, init) {
  const r = await fetchImpl(url, { ...init, redirect: 'error', cache: 'no-store' });
  if (!r.ok) {
    if ([400, 401, 403, 410].includes(r.status)) throw new StreamError(`http_${r.status}`);
    throw new TypeError(`stream_http_${r.status}`);
  }
  check(r.body, 'missing_response_body'); return r;
}
async function* sseChunks(body, base, init, fetchImpl, onStatus) {
  for await (const e of sseEvents(body)) {
    const data = JSON.parse(e.data);
    if (e.event === 'gap') throw new StreamError(data.error ?? 'replay_unavailable', data.head);
    if (e.event === 'status') { await onStatus?.(data); if (data.state === 'FAILED') throw new StreamError('source_failed', data); continue; }
    if (e.event !== 'chunk') continue;
    check(e.id === data.cursor && typeof data.url === 'string' && /^\/v1\/chunks\/[a-f0-9]{64}$/.test(data.url)
      && data.url.endsWith(data.sha256) && uint(data.bytes) && data.bytes <= MAX_FRAME, 'invalid_chunk_notice');
    const r = await checkedResponse(fetchImpl, new URL(data.url, base), init);
    let chunk, count = 0;
    for await (const c of binaryFrames(r.body)) { count++; check(count === 1, 'multiple_chunk_frames'); chunk = c; }
    check(chunk && chunk.digest === data.sha256 && chunk.cursor === data.cursor && chunk.wire.length === data.bytes, 'chunk_notice_mismatch');
    yield chunk;
  }
}
// Acknowledge only after the application's work is durable. Reconnect uses this
// position. An unacknowledged chunk can be delivered again after interruption.
export async function* streamChunks({ url, apiKey, after, mode = 'binary', signal,
  onCheckpoint, onStatus, fetchImpl = fetch, reconnect = true }) {
  const base = new URL(url); check(['http:', 'https:'].includes(base.protocol) && !base.username && !base.password && !base.search && !base.hash, 'invalid_base_url');
  check(base.protocol === 'https:' || ['127.0.0.1', '[::1]', 'localhost'].includes(base.hostname), 'https_required');
  check(['binary', 'sse'].includes(mode), 'invalid_mode');
  const controller = new AbortController(), abort = () => controller.abort(signal?.reason);
  signal?.addEventListener('abort', abort, { once: true }); if (signal?.aborted) abort();
  const init = { headers: { Authorization: `Bearer ${apiKey}` }, signal: controller.signal };
  let checkpoint = after, delay = 1000, previous;
  try {
    if (!checkpoint) {
      const r = await checkedResponse(fetchImpl, new URL('/v1/head', base), init);
      const head = await smallJson(r); checkpoint = head.latest_cursor; await onStatus?.(head);
      if (head.state === 'FAILED') throw new StreamError('source_failed', head);
    }
    parseCursor(checkpoint);
    while (!controller.signal.aborted) {
      try {
        const endpoint = new URL(mode === 'binary' ? '/v1/blocks' : '/v1/events', base);
        endpoint.searchParams.set('after', checkpoint);
        const r = await checkedResponse(fetchImpl, endpoint, init);
        if (mode === 'binary') check(r.headers.get('x-blockzilla-start-cursor') === checkpoint, 'start_cursor_mismatch');
        const input = mode === 'binary' ? binaryFrames(r.body) : sseChunks(r.body, base, init, fetchImpl, onStatus);
        for await (const chunk of input) {
          const a = parseCursor(checkpoint), b = parseCursor(chunk.cursor);
          check(a.session === b.session && a.processed === b.processed && (b.processed ? b.slot === a.slot + 1 : b.slot > a.slot), 'stream_cursor_conflict');
          if (previous) {
            check(chunk.envelope.profile === previous.envelope.profile
              && (b.processed || (chunk.blocks[0].parent_slot === previous.blocks.at(-1).slot
              && JSON.stringify(chunk.blocks[0].parent_blockhash) === JSON.stringify(previous.blocks.at(-1).blockhash)))
              && ['genesis_hash', 'node_identity', 'agave_revision'].every(k => chunk.envelope.marker[k] === previous.envelope.marker[k]), 'parent_chain_gap');
          }
          let acked = false;
          chunk.ack = async () => {
            if (acked) return;
            await onCheckpoint?.(chunk.cursor); checkpoint = chunk.cursor; acked = true;
          };
          yield chunk;
          check(acked, 'checkpoint_required');
          previous = { blocks: [chunk.blocks.at(-1)], envelope: { profile: chunk.envelope.profile, marker:
            Object.fromEntries(['genesis_hash', 'node_identity', 'agave_revision'].map(k => [k, chunk.envelope.marker[k]])) } };
          delay = 1000;
        }
        if (!reconnect) return;
      } catch (e) {
        if (controller.signal.aborted) return;
        if (!reconnect || (e instanceof StreamError && e.code !== 'truncated_frame') || e instanceof SyntaxError) throw e;
        try {
          const r = await checkedResponse(fetchImpl, new URL('/v1/head', base), init);
          const h = await smallJson(r);
          if (h.state === 'FAILED') throw new StreamError('source_failed', h);
        } catch (headError) { if (headError instanceof StreamError) throw headError; }
        await onStatus?.({ state: 'RECONNECTING', error: e.message, checkpoint });
      }
      if (!reconnect) return;
      await new Promise((resolve, reject) => {
        const cancel = () => { clearTimeout(t); reject(controller.signal.reason); };
        const t = setTimeout(() => { controller.signal.removeEventListener('abort', cancel); resolve(); }, delay);
        controller.signal.addEventListener('abort', cancel, { once: true });
      });
      delay = Math.min(delay * 2, 30000);
    }
  } finally { controller.abort(); signal?.removeEventListener('abort', abort); }
}
