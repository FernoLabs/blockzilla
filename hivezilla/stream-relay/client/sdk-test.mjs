import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { loadDecoder, resolvePubkey, createClient } from './sdk.mjs';

test('portable decoder has no host imports and handles errors without a trap', async () => {
  const bytes = await readFile(new URL('./decoder.wasm', import.meta.url));
  const module = await WebAssembly.compile(bytes);
  assert.deepEqual(WebAssembly.Module.imports(module), []);
  const codec = await loadDecoder(module);
  assert.throws(() => codec.decode({ wire: new Uint8Array(16), cursor: 'test:1:-' }),
    e => e.code === 'compact_decode_failed' && e.detail.reason.includes('invalid_transport_frame'));
});
test('owned decoder buffers do not grow memory during repeated failed reads', async () => {
  const codec = await loadDecoder();
  const invalid = { wire: new Uint8Array(32768), cursor: 'test:1:-' };
  for (let i = 0; i < 10; i++) assert.throws(() => codec.decode(invalid));
  const warm = codec.memoryBytes;
  for (let i = 0; i < 1000; i++) assert.throws(() => codec.decode(invalid));
  assert.equal(codec.memoryBytes, warm);
});
test('filter option bounds and types fail before invoking the decoder', async () => {
  const codec = await loadDecoder();
  const c = { wire: new Uint8Array(16) };
  for (const options of [{ programs: Array(257).fill('1') }, { accounts: [12] }, { successfulOnly: 'yes' }, { includeDetails: 1 }]) {
    assert.throws(() => codec.decode(c, options), e => e.code === 'invalid_filter_options');
  }
});
test('chunk key resolution preserves leading zeros and uses the current dictionary', () => {
  const dictionary = new Uint8Array(64); dictionary[63] = 1;
  assert.equal(resolvePubkey({ Id: 1 }, dictionary), '1'.repeat(32));
  assert.equal(resolvePubkey(2, dictionary), '1'.repeat(31) + '2');
  assert.equal(resolvePubkey({ Raw: Array(32).fill(0) }), '1'.repeat(32));
  for (const key of [0, 3, -1, { Raw: [0] }, { Id: 1.5 }]) assert.throws(() => resolvePubkey(key, dictionary));
});
test('client requires a key and keeps transport initialization out of filtering', () => {
  assert.throws(() => createClient({ url: 'http://127.0.0.1/', apiKey: '' }), e => e.code === 'missing_api_key');
});

test('processed control is returned without invoking the transaction decoder', async () => {
  const envelope = { schema: 'blockzilla-stream-envelope-v1', profile: 'native-processed-compact-v1', evidence_included: false,
    marker: { schema: 'hivezilla-processed-control-v1', state: 'COMPLETE', source_profile: 'agave-bank-callbacks-v1',
      finalized: false, canonical: false, publication_ready: false, session_id: 'session1', genesis_hash: 'genesis', node_identity: 'operator', agave_revision: 'rev' },
    event: { sequence: 1, source_sequence: 1, kind: 'invalidate', bank: { slot: 100, bank_id: 7, parent_slot: 99, blockhash: Array(32).fill(1), parent_blockhash: Array(32).fill(1) }, payload_sha256: 'a'.repeat(64), reason: 'clear' }, objects: [] };
  const meta = Buffer.from(JSON.stringify(envelope)), wire = Buffer.alloc(16 + meta.length);wire.write('BZSTRM01');wire.writeUInt32LE(meta.length,8);meta.copy(wire,16);
  const decoder = { decode() { throw new Error('control must not decode transactions'); } };
  const base = 'session1:p0:-';
  const client = createClient({url:'http://127.0.0.1/',apiKey:'key',decoder,reconnect:false,fetchImpl:async()=>new Response(wire,{headers:{'x-blockzilla-start-cursor':base}})});
  for await(const batch of client.stream({after:base})) {assert.equal(batch.kind,'invalidate');assert.equal(batch.event.bank.bank_id,7);assert.equal(batch.blocks.length,0);await batch.ack();}
});
