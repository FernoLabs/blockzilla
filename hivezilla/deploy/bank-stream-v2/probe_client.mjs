// Finite SDK integration probe. Private frames and checkpoints stay on Foundation.
import { readFile, writeFile, mkdir, rename, open } from 'node:fs/promises';
import { createBankClient, bankCursor } from '../../stream-relay/client/banks.mjs';
const root = process.argv[2];
if (!root) throw new Error('root_required');
const report = `${root}/reports/bank-stream-v2-20261010`;
const source = JSON.parse(await readFile(`${report}/expected-source.json`, 'utf8'));
const key = (await readFile(`${root}/config/stream-20261004/client-api.key`, 'utf8')).trim();
const out = `${report}/javascript-probe`;
await mkdir(out, { recursive: true });
async function atomic(path, bytes) {
  const f = await open(`${path}.tmp`, 'w', 0o600);
  try { await f.writeFile(bytes); await f.sync(); } finally { await f.close(); }
  await rename(`${path}.tmp`, path);
  const directory = await open(out, 'r');
  try { await directory.sync(); } finally { await directory.close(); }
}
let events = 0, blocks = 0, transactions = 0, failed = 0, missingErrors = 0, ready = 0;
let cursor, firstEnd, resumeSequence;
const kinds = {};
async function savePayload(frame) { if (frame) await atomic(`${out}/${frame.digest}.bin`, frame.wire); }
for (const limit of [64, 16]) {
  const client = createBankClient({ url: 'https://stream.blockzilla.dev/', apiKey: key, source });
  let count = 0;
  const signal = AbortSignal.timeout(120000);
  for await (const frame of client.stream({
    after: cursor, signal,
    onBootstrap: async ({ payload }) => savePayload(payload),
    onCheckpoint: async after => atomic(`${out}/checkpoint.json`, JSON.stringify({ source, cursor: after })),
  })) {
    if (cursor && frame.event.sequence !== bankCursor(cursor).sequence + 1) throw new Error('event_sequence_gap');
    if (firstEnd && resumeSequence === undefined) resumeSequence = frame.event.sequence;
    await savePayload(frame.dependency);
    await atomic(`${out}/${frame.digest}.bin`, frame.wire);
    for (const block of frame.decoded.blocks || []) {
      blocks++;
      for (const tx of block.transactions) {
        transactions++;
        if (tx.success === false) { failed++; missingErrors += tx.err == null ? 1 : 0; }
      }
    }
    kinds[frame.event.kind] = (kinds[frame.event.kind] || 0) + 1;
    await frame.ack(); cursor = frame.cursor; events++; count++;
    try { client.readyHash(); ready++; } catch {}
    if (count === limit) break;
  }
  if (count !== limit) throw new Error('probe_event_limit_not_reached');
  const saved = JSON.parse(await readFile(`${out}/checkpoint.json`, 'utf8'));
  if (saved.cursor !== cursor) throw new Error('checkpoint_not_saved');
  firstEnd ??= bankCursor(cursor).sequence;
  cursor = saved.cursor;
}
if (!failed || missingErrors || resumeSequence !== firstEnd + 1) throw new Error('probe_validation_failed');
const result = { state: 'PASSED', events, blocks, transactions, failed_transactions: failed, missing_errors: missingErrors,
  kinds, confirmed_hash_ready_observations: ready, exact_resume: true, private_data_kept_on_server: true };
await writeFile(`${out}/SUMMARY.json`, JSON.stringify(result, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify(result));
