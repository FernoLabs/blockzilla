import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { loadConsensusDecoder, decodeConsensusFact, MAX_CONSENSUS_FACT_BYTES } from './consensus.mjs';

const fixtureBase = new URL('../../../crates/blockzilla-bank-stream-v2/tests/fixtures/', import.meta.url);
const footer = await readFile(new URL('captured-footer-3384311167.wincode', fixtureBase));
const provenance = JSON.parse(await readFile(new URL('captured-footer-3384311167.json', fixtureBase)));
const decoder = await loadConsensusDecoder();
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
// This reader test binds retained native bytes to a synthetic execution identity.
// Only the native exporter tests check a matching execution frame.
const binding = {
  source: provenance.source,
  bank: { slot: provenance.carrier_slot, bank_id: provenance.carrier_bank_id,
    blockhash: Array(32).fill(8), parent_slot: provenance.carrier_slot - 1, parent_blockhash: Array(32).fill(7) },
  execution_payload_sha256: 'ab'.repeat(32),
  source_event_sequence: provenance.source_event_sequence,
  source_event_sha256: provenance.source_event_sha256,
};
function frame(native = footer) {
  const json = Buffer.from(JSON.stringify(binding)), header = Buffer.alloc(20);
  header.write('BZCNF001'); header.writeUInt16LE(1, 8); header.writeUInt16LE(1, 10);
  header.writeUInt32LE(json.length, 12); header.writeUInt32LE(native.length, 16);
  return Buffer.concat([header, json, native]);
}

test('shared Wasm reader keeps native nanoseconds, bytes and older certificate target', async () => {
  assert.equal(sha(footer), provenance.native_footer_sha256);
  const bytes = frame();
  const value = (await decodeConsensusFact(bytes, sha(bytes), binding, decoder)).decoded;
  assert.equal(value.verification, 'captured_not_verified');
  assert.equal(value.carrier_consensus_block_id, null);
  assert.equal(value.footer_sha256, provenance.native_footer_sha256);
  assert.equal(value.footer.block_producer_time_nanos, provenance.producer_time_nanos);
  assert.equal(BigInt(value.footer.block_producer_time_nanos), 1792700777706079707n);
  assert.equal(value.footer.block_final_cert.slot, '460170403');
  assert.notEqual(BigInt(value.footer.block_final_cert.slot), BigInt(binding.bank.slot));
  assert.equal(Buffer.from(value.footer.bank_hash).toString('hex'), provenance.bank_hash_hex);
  assert.equal(Buffer.from(value.footer.block_user_agent).toString('hex'), provenance.user_agent_hex);
  assert.equal(value.footer.block_final_cert.final_aggregate.signature.length, 96);
  assert.equal(value.footer.block_final_cert.notar_aggregate, null); // Captured fast-final path.
  assert.equal(value.footer.skip_reward_cert, null);
  assert.equal(value.footer.notar_reward_cert.aggregate.signature.length, 96);
});

test('SDK rejects altered bytes and a different source, bank, payload or event binding', async () => {
  const wire = frame();
  await assert.rejects(decodeConsensusFact(wire, '00'.repeat(32), binding, decoder), /consensus_digest_mismatch/);
  const alternatives = [
    b => { b.source.session_id = 'different-session'; },
    b => { b.bank.bank_id++; },
    b => { b.bank.parent_blockhash[0] ^= 1; },
    b => { b.execution_payload_sha256 = '12'.repeat(32); },
    b => { b.source_event_sequence++; },
    b => { b.source_event_sha256 = '34'.repeat(32); },
  ];
  for (const alter of alternatives) {
    const other = structuredClone(binding); alter(other);
    await assert.rejects(decodeConsensusFact(wire, sha(wire), other, decoder), /consensus_decode_failed/);
  }
});

test('shared parser rejects an unknown grammar, invalid option and every truncated footer', async () => {
  const good = frame();
  for (const at of [0, 8, 10]) {
    const bad = Buffer.from(good); bad[at] ^= 2;
    await assert.rejects(decodeConsensusFact(bad, sha(bad), binding, decoder), /consensus_decode_failed/);
  }
  const native = Buffer.from(footer), optionAt = 42 + native[41];
  native[optionAt] = 2;
  const bad = frame(native);
  await assert.rejects(decodeConsensusFact(bad, sha(bad), binding, decoder), /consensus_decode_failed/);
  for (let n = 0; n < footer.length; n++) {
    assert.throws(() => decoder.decode(frame(footer.subarray(0, n)), binding), /consensus_decode_failed/);
  }
  assert.throws(() => decoder.decode(new Uint8Array(MAX_CONSENSUS_FACT_BYTES + 1), binding), /invalid_consensus_fact_size/);
});

test('full u64 time and absent certificates stay exact without a Tower fallback', () => {
  const native = Buffer.alloc(45); native[0] = 1; native.writeBigUInt64LE(0xffffffffffffffffn, 33);
  const value = decoder.decode(frame(native), binding);
  assert.equal(value.footer.block_producer_time_nanos, '18446744073709551615');
  assert.equal(value.footer.block_final_cert, null);
  assert.equal(value.footer.skip_reward_cert, null);
  assert.equal(value.footer.notar_reward_cert, null);
  assert.equal(value.verification, 'captured_not_verified');
  const before = decoder.memoryBytes;
  for (let n = 0; n < 200; n++) decoder.decode(frame(), binding);
  assert.ok(decoder.memoryBytes <= before + 4 * 65536, 'owned Wasm buffers must be released');
});
