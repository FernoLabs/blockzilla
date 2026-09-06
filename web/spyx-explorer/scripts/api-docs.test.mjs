import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { API_ROUTE_GROUPS, buildApiExamples } from '../src/lib/api-docs.js';

const testOrigin = 'https://example.test:18787';

test('builds every copyable example from the supplied browser origin', () => {
  const examples = buildApiExamples(testOrigin);

  assert.equal(examples.length, 8);
  assert.equal(new Set(examples.map((row) => row.id)).size, examples.length);
  for (const example of examples) {
    assert.ok(example.url.startsWith(`${testOrigin}/`));
    assert.equal(example.command, `curl -sS '${example.url}'`);
    assert.doesNotMatch(example.url, /192\.168\.|localhost|spyx\.blockzilla\.dev/);
  }
  assert.match(examples.find((row) => row.id === 'owner-history').url, /\/postings\/owner\//);
  assert.match(
    examples.find((row) => row.id === 'account-balance-history').url,
    /\/accounts\/[^/]+\/balance-history\?max_points=1000$/
  );
  assert.match(examples.find((row) => row.id === 'market-candles').url, /max_points=24$/);
});

test('rejects a non-HTTP example origin', () => {
  assert.throws(() => buildApiExamples('file:///tmp/spyx'), /must use HTTP or HTTPS/);
});

test('documents the complete public GET route surface', () => {
  const paths = API_ROUTE_GROUPS.flatMap((group) => group.routes.map((route) => route.path));
  const requiredRoutes = [
    '/healthz',
    '/data/spyx-summary.json',
    '/data/spyx-authority-portfolios.json',
    '/api/v1/transactions/{id}',
    '/api/v1/transactions/by-signature/{signature}',
    '/api/v1/transactions/by-coordinate',
    '/api/v1/postings/target-address/{key}',
    '/api/v1/postings/token-account/{key}',
    '/api/v1/postings/owner/{key}',
    '/api/v1/postings/program/{key}',
    '/api/v1/accounts/{address}/balance-history',
    '/api/v1/accounts/{address}/trading-summary',
    '/api/v1/accounts/{address}/trades',
    '/api/v1/accounts/{address}/trading-activity',
    '/api/v1/market/provenance',
    '/api/v1/market/summary',
    '/api/v1/market/pairs',
    '/api/v1/market/mints',
    '/api/v1/market/mints/{address}',
    '/api/v1/market/programs',
    '/api/v1/market/trades',
    '/api/v1/market/trades/{trade_id}',
    '/api/v1/market/candles',
    '/api/v1/market/slot-candles',
    '/api/v1/market/program-volume'
  ];

  for (const route of requiredRoutes) {
    assert.ok(paths.some((path) => path.startsWith(route)), `missing documented route ${route}`);
  }
  assert.ok(
    paths.some((path) =>
      path.startsWith('/api/v1/postings/program/{key}?instruction_scope={all|direct|inner}')
    ),
    'program history must document direct and inner instruction scopes'
  );
  assert.ok(API_ROUTE_GROUPS.every((group) => group.routes.every((route) => route.method === 'GET')));
});

test('the API page reads the current window origin and has no fixed host', async () => {
  const source = await readFile(new URL('../src/routes/api-docs/+page.svelte', import.meta.url), 'utf8');
  assert.match(source, /window\.location\.origin/);
  assert.doesNotMatch(source, /192\.168\.|localhost|spyx\.blockzilla\.dev/);
});
