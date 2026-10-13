import { readFile, open, rename } from 'node:fs/promises';
import { dirname } from 'node:path';
import { createClient } from './sdk.mjs';

// node example.mjs URL KEY_FILE CHECKPOINT_FILE [PROGRAM_ADDRESS]
const [, , url, keyFile, checkpointFile, program] = process.argv;
if (!url || !keyFile || !checkpointFile) throw new Error('Usage: node example.mjs URL KEY_FILE CHECKPOINT_FILE [PROGRAM_ADDRESS]');
const apiKey = (await readFile(keyFile, 'utf8')).trim();
let after;
try { after = (await readFile(checkpointFile, 'utf8')).trim(); }
catch (e) { if (e.code !== 'ENOENT') throw e; }
const signal = new AbortController();
process.once('SIGINT', () => signal.abort());
const client = createClient({ url, apiKey });
for await (const batch of client.stream({ after, signal: signal.signal, mode: process.env.BLOCKZILLA_STREAM_MODE ?? 'binary', programs: program ? [program] : [], includeDetails: Boolean(program),
  onCheckpoint: async cursor => {
    const file = await open(`${checkpointFile}.tmp`, 'w', 0o600);
    try { await file.writeFile(`${cursor}\n`); await file.sync(); }
    finally { await file.close(); }
    await rename(`${checkpointFile}.tmp`, checkpointFile);
    const directory = await open(dirname(checkpointFile), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  } })) {
  if (batch.kind !== 'block') console.log(JSON.stringify({ kind: batch.kind, event: batch.event, cursor: batch.cursor }));
  for (const block of batch.blocks) for (const tx of block.transactions) {
    console.log(JSON.stringify({ kind: 'transaction', bankId: block.bankId, commitment: block.finality, slot: block.slot, blockhash: block.blockhash, signature: tx.signature,
      success: tx.success, topLevelPrograms: tx.topLevelPrograms, innerPrograms: tx.innerPrograms }));
  }
  // Replace logging with durable application processing before acknowledging.
  await batch.ack();
}
