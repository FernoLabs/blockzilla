import { readFile, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import assert from 'node:assert/strict';
import { streamChunks } from './index.mjs';

// Usage: node live-test.mjs KEY_FILE REPORT PLAYWRIGHT_MODULE CHROME_EXECUTABLE
const [, , keyFile, reportFile, playwrightModule, chromeExecutable] = process.argv;
const apiKey = (await readFile(keyFile, 'utf8')).trim();
const url = 'http://127.0.0.1:8871/';
const head = await (await fetch(new URL('/v1/head', url), { headers: { Authorization: `Bearer ${apiKey}` } })).json();
assert.equal(head.state, 'LIVE');
const after = head.latest_cursor;
const results = {};
for (const mode of ['binary', 'sse']) {
  const rows = [], checkpoints = [], started = performance.now();
  for await (const c of streamChunks({ url, apiKey, after, mode, onCheckpoint: p => checkpoints.push(p) })) {
    rows.push({ cursor: c.cursor, bytes: c.wire.length, sha256: c.digest, blocks: c.blocks.length });
    await c.ack(); if (rows.length === 5) break;
  }
  results[`node_${mode}`] = { rows, checkpoints, seconds: (performance.now() - started) / 1000 };
}
assert.deepEqual(results.node_binary.rows, results.node_sse.rows);
const module = await readFile(new URL('./index.mjs', import.meta.url));
const server = createServer((req, res) => {
  if (req.url === '/index.mjs') { res.setHeader('Content-Type', 'text/javascript'); res.end(module); }
  else { res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><title>Stream transport test</title>'); }
});
await new Promise((resolve, reject) => { server.once('error', reject); server.listen(8891, '127.0.0.1', resolve); });
let browser;
try {
  const { chromium } = await import(pathToFileURL(playwrightModule));
  browser = await chromium.launch({ executablePath: chromeExecutable, headless: true,
    args: ['--disable-background-networking', '--disable-component-update'] });
  const page = await browser.newPage();
  await page.goto('http://127.0.0.1:8891/');
  const run = async mode => page.evaluate(async ({ apiKey, after, mode }) => {
    const { streamChunks } = await import('/index.mjs');
    const rows = [], checkpoints = [], started = performance.now();
    for await (const c of streamChunks({ url: 'http://127.0.0.1:8871/', apiKey, after, mode,
      onCheckpoint: p => checkpoints.push(p) })) {
      rows.push({ cursor: c.cursor, bytes: c.wire.length, sha256: c.digest, blocks: c.blocks.length });
      await c.ack(); if (rows.length === 5) break;
    }
    return { rows, checkpoints, seconds: (performance.now() - started) / 1000 };
  }, { apiKey, after, mode });
  results.browser_binary = await run('binary');
  results.browser_sse = await run('sse');
  assert.deepEqual(results.browser_binary.rows, results.node_binary.rows);
  assert.deepEqual(results.browser_sse.rows, results.node_binary.rows);
  const report = { schema: 'hivezilla-stream-javascript-live-test-v1', state: 'PASS',
    browser: browser.version(), node: process.version, module_sha256: createHash('sha256').update(module).digest('hex'),
    real_live_output: true, results, shared_decoder: false, compact_transaction_decode: false,
    origin: 'current machine loopback pilot via SSH', source_commitment: 'finalized operator-trusted projection' };
  await writeFile(reportFile, JSON.stringify(report, null, 2) + '\n');
  console.log(JSON.stringify({ state: 'PASS', browser: report.browser, node: report.node,
    identical_live_chunks: 5, modes: ['node binary', 'node SSE', 'browser binary', 'browser SSE'] }));
} finally { await browser?.close(); await new Promise(resolve => server.close(resolve)); }
