import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { BankState, bankCursor, loadBankDecoder, decodeBankFrame, noticePreflight, createBankClient } from './banks.mjs';
const root = new URL('./fixtures/bank-v2/', import.meta.url);
const read = name => readFile(new URL(name, root));
async function setup() {
  const codec = await loadBankDecoder(), head = JSON.parse(await read('head.json'));
  const source = head.source, frames = {};
  for (const name of ['block','confirmed','root']) {
    const wire = new Uint8Array(await read(`${name}.bin`));
    const digest = [...new Uint8Array(await crypto.subtle.digest('SHA-256', wire))].map(n => n.toString(16).padStart(2,'0')).join('');
    frames[name] = await decodeBankFrame(wire, digest, source, codec);
    assert.deepEqual(codec.decode(wire, { includeDetails: true }), JSON.parse(await read(`${name}.decoded.json`)));
  }
  return { codec, head, source, frames };
}
test('real Rust compact objects decode with non-null failed error and native height', async () => {
  const { frames, codec } = await setup(), tx = codec.decode(frames.block.wire, { includeDetails: true }).blocks[0].transactions[0];
  assert.equal(tx.success, false); assert.notEqual(tx.metadata.err, null);
  assert.equal(tx.accountKeys.length, 3); assert.equal(frames.block.decoded.blocks[0].blockHeight, 400000000);
});
test('strict required nulls, unknown fields, source and native queue expiry', async () => {
  const { codec, head, source, frames } = await setup();
  codec.validateHead(head, source, 1000);
  for (const bad of [{ ...head, alien: 1 }, { ...head, cursor: undefined }, { ...head, source_lag_ms: 5001 }]) assert.throws(() => codec.validateHead(bad, source, 1000));
  for (const field of ['genesis_hash','node_identity','agave_revision','session_id']) assert.throws(() => codec.validateHead(head, { ...source, [field]: 'wrong' }, 1000));
  await assert.rejects(decodeBankFrame(frames.block.wire, '0'.repeat(64), source, codec));
  for (const value of ['test:p1:-','test:p01:'+'a'.repeat(64),'test:1:'+'a'.repeat(64)]) assert.throws(() => bankCursor(value));
});
test('processed cannot confirm; exact control, duplicates, old root and monotonic age', async () => {
  const { codec, head, source, frames } = await setup(); let wall = 1000, mono = 0;
  const s = new BankState(source, `${source.session_id}:p0:-`, codec, { wallClock: () => wall, monotonicClock: () => mono });
  s.apply(frames.block); assert.equal(s.selected, null); assert.throws(() => s.readyHash()); assert.equal(s.apply(frames.block), false);
  s.apply(frames.confirmed); s.health(head); assert.equal(s.readyHash().last_valid_block_height, 400000150);
  const at = s.selectedAt; s.apply(frames.root); assert.equal(s.selectedAt, at); assert.equal(s.selected.commitment, 'confirmed');
  mono = 16000; wall = 900; s.health({ ...head, sampled_at_unix_ms: 1000 }); assert.throws(() => s.readyHash());
});
test('notice URL and exact sequence are checked before any credentialed request', async () => {
  const { source, frames } = await setup(), f = frames.block;
  const n = { cursor: f.cursor, sha256: f.digest, bytes: f.wire.length, start_slot: f.event.bank.slot, end_slot: f.event.bank.slot, url: `/v2/banks/chunks/${f.digest}` };
  noticePreflight(n, f.cursor, 1, source);
  for (const bad of [{ ...n, url: 'https://evil.test/' }, { ...n, bytes: 1 }, { ...n, extra: true }]) assert.throws(() => noticePreflight(bad, f.cursor, 1, source));
  assert.throws(() => noticePreflight(n, 'wrong', 1, source)); assert.throws(() => noticePreflight(n, f.cursor, 2, source));
});
test('client checks SSE headers, explicit gaps, and persists before resume', async () => {
  const { codec, head, source, frames } = await setup(); const begin = `${source.session_id}:p0:-`, calls = [], checkpoints = [];
  const event = f => `event: chunk\nid: ${f.cursor}\ndata: ${JSON.stringify({ cursor: f.cursor, sha256: f.digest, bytes: f.wire.length, start_slot: f.event.bank.slot, end_slot: f.event.bank.slot, url: `/v2/banks/chunks/${f.digest}` })}\n\n`;
  const client = createBankClient({ url: 'https://provider.test/', apiKey: 'k'.repeat(32), source, decoder: codec, reconnect: false, wallClock: () => 1000,
    fetchImpl: async (url, init) => {
      calls.push(url.pathname); assert.equal(init.redirect, 'error'); assert.equal(init.headers.Authorization, 'Bearer '+'k'.repeat(32));
      if (url.pathname.endsWith('/head')) return Response.json(head);
      if (url.pathname.endsWith('/events')) return new Response(`event: status\ndata: ${JSON.stringify(head)}\n\n${event(frames.block)}${event(frames.confirmed)}`, { headers: { 'content-type': 'text/event-stream', 'x-blockzilla-start-cursor': begin } });
      const f = Object.values(frames).find(f => url.pathname.endsWith(f.digest)); assert.ok(f); return new Response(f.wire);
    } });
  let count = 0;
  for await (const f of client.stream({ after: begin, onCheckpoint: cursor => checkpoints.push(cursor) })) { await f.ack(); count++; }
  assert.equal(count, 2); assert.deepEqual(checkpoints, [frames.block.cursor,frames.confirmed.cursor]);
  assert.equal(calls.filter(p => p.endsWith(frames.block.digest)).length, 2); // bootstrap + one block, no second dependency GET
  assert.equal(client.readyHash().context_slot, frames.block.event.bank.slot);
});
test('v2 does not accept URL credentials or unsupported ABI', async () => {
  assert.throws(() => createBankClient({ url: 'https://provider.test/?api_key=key', apiKey: 'k'.repeat(32), source: {} }));
  const old = await readFile(new URL('./decoder.wasm', import.meta.url)); await assert.rejects(loadBankDecoder(old), e => e.code === 'invalid_bank_decoder_abi');
});

test('delayed factual live health permits replay and never restores readiness', async () => {
  const { codec, source, frames, head } = await setup(); let wall = 1000;
  const s = new BankState(source, frames.confirmed.cursor, codec, { wallClock: () => wall });
  s.resolveHead(head, frames.confirmed, frames.block); s.health(head); wall = 8000;
  s.health(head); assert.throws(() => s.readyHash()); s.apply(frames.root); assert.throws(() => s.readyHash());
});

test('concurrent GETs apply in order and stop at the durable cursor', async () => {
  const { codec, head, source, frames } = await setup();
  const begin = `${source.session_id}:p0:-`, checkpoints = [];
  let streaming = false, active = 0, peak = 0;
  const event = f => `event: chunk\nid: ${f.cursor}\ndata: ${JSON.stringify({ cursor: f.cursor, sha256: f.digest, bytes: f.wire.length, start_slot: f.event.bank.slot, end_slot: f.event.bank.slot, url: `/v2/banks/chunks/${f.digest}` })}\n\n`;
  const client = createBankClient({ url: 'https://provider.test/', apiKey: 'k'.repeat(32), source, decoder: codec, reconnect: false, wallClock: () => 1000,
    fetchImpl: async url => {
      if (url.pathname.endsWith('/head')) return Response.json(head);
      if (url.pathname.endsWith('/events')) {
        streaming = true;
        return new Response(`event: status\ndata: ${JSON.stringify(head)}\n\n${event(frames.block)}${event(frames.confirmed)}${event(frames.root)}`, { headers: { 'content-type': 'text/event-stream', 'x-blockzilla-start-cursor': begin } });
      }
      const f = Object.values(frames).find(f => url.pathname.endsWith(f.digest)); assert.ok(f);
      if (streaming) { active++; peak = Math.max(peak, active); await new Promise(resolve => setTimeout(resolve, f.event.kind === 'block' ? 35 : 1)); active--; }
      return new Response(f.wire);
    } });
  for await (const f of client.stream({ after: begin, onCheckpoint: c => checkpoints.push(c) })) await f.ack();
  assert.deepEqual(checkpoints, [frames.block.cursor, frames.confirmed.cursor, frames.root.cursor]);
  assert.ok(peak > 1 && peak <= 4); assert.equal(active, 0);
});
