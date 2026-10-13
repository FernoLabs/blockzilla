import { readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import assert from 'node:assert/strict';
import { createClient, loadDecoder } from './sdk.mjs';
import { streamChunks } from './index.mjs';

// node sdk-live-test.mjs KEY_FILE REPORT ORACLE_INPUT PLAYWRIGHT_MODULE CHROME
const [, , keyFile, reportFile, oracleFile, playwrightModule, chrome] = process.argv;
const apiKey = (await readFile(keyFile, 'utf8')).trim();
const url = 'http://127.0.0.1:8871/';
const program = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const head = await (await fetch(new URL('/v1/head', url), { headers: { Authorization: `Bearer ${apiKey}` } })).json();
assert.equal(head.state, 'LIVE'); assert.equal(head.stale, false);
const after = head.latest_cursor;
const codec = await loadDecoder();
const client = createClient({ url, apiKey, decoder: codec });
const summaries = {};
const batches = {};
const summarize = b => ({ cursor: b.cursor, digest: b.digest, slots: b.blocks.map(x => x.slot),
  transactions: b.transactionCount, matches: b.matchedTransactions, wireBytes: b.wireBytes, decodeMilliseconds: b.decodeMilliseconds });
async function run(name, options) {
  const rows = [], values = [], checkpoints = [];
  for await (const b of client.stream({ after, signal: AbortSignal.timeout(45000), ...options,
    onCheckpoint: cursor => checkpoints.push(cursor) })) {
    rows.push(summarize(b)); values.push(b.blocks);
    await b.ack(); if (rows.length === 3) break;
  }
  assert.equal(rows.length, 3); assert.deepEqual(checkpoints, rows.map(b => b.cursor));
  summaries[name] = { rows, checkpoints }; batches[name] = values.flat();
}
await run('node_all', {});
await run('node_program_binary', { programs: [program], includeDetails: true });
await run('node_program_sse', { programs: [program] });
await run('node_account_binary', { accounts: [program] });
for (const name of Object.keys(summaries)) assert.deepEqual(summaries[name].rows.map(b => b.cursor), summaries.node_all.rows.map(b => b.cursor));
const identities = b => b.map(x => [x.slot, x.transactions.map(t => [t.index, t.signature, t.success, t.topLevelPrograms, t.innerPrograms])]);
assert.deepEqual(identities(batches.node_program_binary), identities(batches.node_program_sse));
assert.ok(batches.node_program_binary.some(b => b.transactions.length > 0));
// Replay is bound to the acknowledged position, including a new SDK instance.
const saved = summaries.node_all.checkpoints[0];
for await (const b of createClient({ url, apiKey, decoder: codec }).stream({ after: saved, signal: AbortSignal.timeout(15000) })) {
  assert.equal(b.cursor, summaries.node_all.checkpoints[1]); await b.ack(); break;
}
// The same frozen bytes exercise decoder ownership without changing validator state.
let sample;
for await (const c of streamChunks({ url, apiKey, after, signal: AbortSignal.timeout(15000) })) { sample = c; await c.ack(); break; }
for (let i = 0; i < 4; i++) codec.decode(sample, { programs: [program] });
const warmMemory = codec.memoryBytes;
for (let i = 0; i < 30; i++) codec.decode(sample, { programs: [program] });
assert.equal(codec.memoryBytes, warmMemory);
await writeFile(oracleFile, JSON.stringify({ program, all: batches.node_all,
  programMatches: batches.node_program_sse, accountMatches: batches.node_account_binary }));

const files = new Map();
for (const name of ['sdk.mjs', 'index.mjs', 'decoder.wasm']) files.set(`/${name}`, await readFile(new URL(`./${name}`, import.meta.url)));
const server = createServer((req, res) => {
  if (files.has(req.url)) { res.setHeader('Content-Type', req.url.endsWith('.wasm') ? 'application/wasm' : 'text/javascript'); res.end(files.get(req.url)); }
  else { res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><title>SDK test</title>'); }
});
await new Promise((resolve, reject) => { server.once('error', reject); server.listen(8891, '127.0.0.1', resolve); });
let browser;
try {
  const { chromium } = await import(pathToFileURL(playwrightModule));
  browser = await chromium.launch({ executablePath: chrome, headless: true, args: ['--disable-background-networking', '--disable-component-update'] });
  const page = await browser.newPage(); await page.goto('http://127.0.0.1:8891/');
  for (const mode of ['binary', 'sse']) {
    const result = await page.evaluate(async ({ url, apiKey, after, mode, program }) => {
      const { createClient } = await import('/sdk.mjs');
      const rows = [], blocks = [], checkpoints = [];
      for await (const b of createClient({ url, apiKey }).stream({ after, mode, programs: [program],
        signal: AbortSignal.timeout(45000), onCheckpoint: c => checkpoints.push(c) })) {
        rows.push({ cursor: b.cursor, digest: b.digest, slots: b.blocks.map(x => x.slot), transactions: b.transactionCount,
          matches: b.matchedTransactions, wireBytes: b.wireBytes, decodeMilliseconds: b.decodeMilliseconds });
        blocks.push(...b.blocks); await b.ack(); if (rows.length === 3) break;
      }
      return { rows, blocks, checkpoints };
    }, { url, apiKey, after, mode, program });
    assert.deepEqual(identities(result.blocks), identities(batches.node_program_sse));
    assert.deepEqual(result.checkpoints, summaries.node_all.checkpoints);
    summaries[`browser_program_${mode}`] = { rows: result.rows, checkpoints: result.checkpoints };
  }
  const report = { schema: 'hivezilla-stream-sdk-live-v1', state: 'PASS', node: process.version, browser: browser.version(),
    wasm_bytes: files.get('/decoder.wasm').length, wasm_sha256: createHash('sha256').update(files.get('/decoder.wasm')).digest('hex'),
    program, batches: 3, blocks: batches.node_all.length, transactions: batches.node_all.reduce((n,b) => n+b.transactionCount,0),
    program_matches: batches.node_program_sse.reduce((n,b) => n+b.transactions.length,0),
    account_matches: batches.node_account_binary.reduce((n,b) => n+b.transactions.length,0),
    memory_bytes_after_warmup: warmMemory, repeated_decode_memory_stable: true, reconnect_checkpoint_test: true,
    source_finality: 'operator-trusted finalized', canonical: false, publication_ready: false,
    public_route: false, rpc_oracle_verified: false, results: summaries };
  await writeFile(reportFile, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ state: report.state, blocks: report.blocks, transactions: report.transactions,
    program_matches: report.program_matches, browser: report.browser, wasm_bytes: report.wasm_bytes, memory_bytes: warmMemory }));
} finally { await browser?.close(); await new Promise(resolve => server.close(resolve)); }
