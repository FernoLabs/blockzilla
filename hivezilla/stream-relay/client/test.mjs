import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { binaryFrames, decodeFrame, sseEvents, streamChunks, parseCursor } from './index.mjs';

const paths = ['DIRECT-AGAVE-NORMALIZED.v1.json', 'archive-v2-blocks.index',
  'archive-v2-blocks.zstd', 'archive-v2-meta.wincode', 'available-poh.wincode',
  'blockhash_registry.bin', 'registry.bin', 'signatures.bin'];
const digest = b => createHash('sha256').update(b).digest('hex');
export function fixture(slot = 100, parent = 99) {
  const objects = paths.map((path, offset) => ({ path, offset, bytes: 1, sha256: digest(Buffer.from([42])) }));
  const marker = { schema: 'hivezilla-direct-agave-compact-v1', state: 'COMPLETE',
    source_profile: 'agave-bank-callbacks-v1', finalized: true, canonical: false,
    publication_ready: false, finality_basis: 'operator-trusted-agave-rooted-bank-callback',
    session_id: 'session1', genesis_hash: 'genesis', node_identity: 'operator', agave_revision: 'rev',
    epoch: 0, start_slot: slot, end_slot_exclusive: slot + 1, blocks: 1,
    produced_blocks: [{ slot, parent_slot: parent, bank_id: slot, block_id: 0,
      blockhash: Array(32).fill(1), parent_blockhash: Array(32).fill(1) }],
    files: objects.map(({ path, bytes, sha256 }) => ({ path, bytes, sha256 })) };
  const meta = Buffer.from(JSON.stringify({ schema: 'blockzilla-stream-envelope-v1',
    profile: 'native-compact-projection-v1', evidence_included: false, marker, objects }));
  const header = Buffer.alloc(16); header.write('BZSTRM01'); header.writeUInt32LE(meta.length, 8); header.writeUInt32LE(paths.length, 12);
  return Buffer.concat([header, meta, Buffer.alloc(paths.length, 42)]);
}
const body = arrays => new ReadableStream({ start(c) { for (const a of arrays) c.enqueue(new Uint8Array(a)); c.close(); } });
test('single-byte fragmentation and coalesced frames give identical chunks', async () => {
  const a = fixture(), b = fixture(103, 100), combined = Buffer.concat([a, b]);
  const split = [];
  for await (const c of binaryFrames(body([...combined].map(n => [n])))) split.push(c.cursor);
  const together = [];
  for await (const c of binaryFrames(body([combined]))) together.push(c.cursor);
  assert.deepEqual(split, together); assert.equal(split.length, 2);
});
test('corrupt, truncated and oversized frames fail', async () => {
  const bad = fixture(); bad[bad.length - 1] ^= 1;
  await assert.rejects(decodeFrame(bad), /object_digest_mismatch/);
  const h = fixture().subarray(0, 16); h.writeUInt32LE(0xffffffff, 12);
  await assert.rejects(decodeFrame(h), /frame_too_large/);
  await assert.rejects(async () => { for await (const c of binaryFrames(body([fixture().subarray(0, 25)]))) void c; }, /truncated_frame/);
});
test('SSE handles CRLF split across network reads', async () => {
  const text = Buffer.from('id: example\r\nevent: chunk\r\ndata: {"ok":true}\r\n\r\n');
  const events = [];
  for await (const e of sseEvents(body([...text].map(n => [n])))) events.push(e);
  assert.deepEqual(events, [{ id: 'example', event: 'chunk', data: '{"ok":true}' }]);
});
test('binary and SSE paths return exactly the same frame', async () => {
  const wire = fixture(), c = await decodeFrame(wire), base = 'session1:99:-';
  const mock = async (url, init) => {
    assert.equal(init.headers.Authorization, 'Bearer secret');
    const u = new URL(url);
    if (u.pathname === '/v1/blocks') return new Response(wire, { headers: { 'x-blockzilla-start-cursor': base } });
    if (u.pathname === '/v1/events') return new Response(`id: ${c.cursor}\nevent: chunk\ndata: ${JSON.stringify({ cursor: c.cursor, sha256: c.digest, bytes: wire.length, url: `/v1/chunks/${c.digest}` })}\n\n`);
    if (u.pathname === `/v1/chunks/${c.digest}`) return new Response(wire);
    throw Error('wrong URL');
  };
  for (const mode of ['binary', 'sse']) {
    let checkpoint;
    const chunks = [];
    for await (const c of streamChunks({ url: 'http://127.0.0.1/', apiKey: 'secret', after: base, mode,
      fetchImpl: mock, reconnect: false, onCheckpoint: p => { checkpoint = p; } })) { chunks.push(c.wire); await c.ack(); }
    assert.equal(chunks.length, 1); assert.deepEqual(Buffer.from(chunks[0]), wire); assert.equal(checkpoint, c.cursor);
  }
});
test('checkpoint must be saved before the next chunk', async () => {
  const saved = [], base = 'session1:99:-';
  const mock = async () => new Response(fixture(), { headers: { 'x-blockzilla-start-cursor': base } });
  const iterator = streamChunks({ url: 'http://127.0.0.1/', apiKey: 'key', after: base, fetchImpl: mock, reconnect: false, onCheckpoint: c => saved.push(c) });
  await iterator.next(); assert.equal(saved.length, 0);
  await assert.rejects(iterator.next(), /checkpoint_required/);
});
test('expired cursor fails instead of switching to latest', async () => {
  const iterator = streamChunks({ url: 'http://127.0.0.1/', apiKey: 'key', after: 'session1:99:-', fetchImpl: async () => new Response('', { status: 410 }) });
  await assert.rejects(iterator.next(), /http_410/);
});
test('SSE cannot send credentials to an external chunk URL', async () => {
  let requests = 0;
  const mock = async () => { requests++; return new Response('id: bad\nevent: chunk\ndata: {"cursor":"bad","sha256":"abc","bytes":1,"url":"https://other.invalid/"}\n\n'); };
  await assert.rejects(async () => { for await (const c of streamChunks({ url: 'http://127.0.0.1/', apiKey: 'key', after: 'session1:99:-', mode: 'sse', fetchImpl: mock })) void c; }, /invalid_chunk_notice/);
  assert.equal(requests, 1);
});
test('ambiguous cursor syntax is rejected', () => {
  for (const s of ['session1:01:-', '../:1:-', 'session1:2:abc']) assert.throws(() => parseCursor(s));
});
test('silent binary connection has a bounded read deadline', async () => {
  let cancelled = false;
  const b = new ReadableStream({ cancel() { cancelled = true; } });
  await assert.rejects(async () => { for await (const c of binaryFrames(b, 10)) void c; }, /stream_idle_timeout/);
  assert.equal(cancelled, true);
});

function processedFixture(sequence, bankId, kind = 'block') {
  const old = fixture(), n = old.readUInt32LE(8);
  const envelope = JSON.parse(old.subarray(16,16+n));
  envelope.profile = 'native-processed-compact-v1';
  const m = envelope.marker;
  m.schema = kind === 'block' ? 'hivezilla-direct-agave-processed-compact-v1' : 'hivezilla-processed-control-v1';
  m.finalized = false; m.finality_basis = 'operator-trusted-agave-processed-bank-callback';
  m.produced_blocks[0].bank_id = bankId;
  const { slot, bank_id, blockhash, parent_slot, parent_blockhash } = m.produced_blocks[0];
  envelope.event = { sequence, source_sequence: sequence, kind, bank: { slot, bank_id, blockhash, parent_slot, parent_blockhash }, payload_sha256: digest(Buffer.from('marker')), reason: null };
  if (kind !== 'block') { envelope.objects = []; delete m.produced_blocks; delete m.files; }
  const meta = Buffer.from(JSON.stringify(envelope)), payload = kind === 'block' ? old.subarray(16+n) : Buffer.alloc(0);
  const header = Buffer.alloc(16);header.write('BZSTRM01');header.writeUInt32LE(meta.length,8);header.writeUInt32LE(payload.length,12);
  return Buffer.concat([header,meta,payload]);
}
test('processed sequence admits competing banks and payload-free controls', async () => {
  const frames = [processedFixture(1,7),processedFixture(2,8),processedFixture(3,7,'invalidate'),processedFixture(4,8,'root')];
  const base = 'session1:p0:-', saved = [], seen = [];
  const mock = async () => new Response(Buffer.concat(frames),{headers:{'x-blockzilla-start-cursor':base}});
  for await (const c of streamChunks({url:'http://127.0.0.1/',apiKey:'key',after:base,fetchImpl:mock,reconnect:false,onCheckpoint:p=>saved.push(p)})) {
    seen.push(c.envelope.event.kind);if(c.envelope.event.kind !== 'block') assert.equal(c.objects.size,0);await c.ack();
  }
  assert.deepEqual(seen,['block','block','invalidate','root']);assert.equal(parseCursor(saved.at(-1)).slot,4);
});
test('processed sequence gaps fail before acknowledgment', async () => {
  const base='session1:p0:-';
  const mock=async()=>new Response(processedFixture(2,7),{headers:{'x-blockzilla-start-cursor':base}});
  await assert.rejects(async()=>{for await(const c of streamChunks({url:'http://127.0.0.1/',apiKey:'key',after:base,fetchImpl:mock,reconnect:false}))await c.ack();},/stream_cursor_conflict/);
});
