import assert from 'node:assert/strict';
import test from 'node:test';
import {
  defaultHolderNavigationState,
  holderNavigationUrl,
  parseHolderNavigationState
} from '../src/lib/holder-navigation.ts';

const programA = '11111111111111111111111111111111';
const programB = 'Pig2ienhM3ukiTec3x8aCdnLASpU4z8yRPLgH9QxDvm';
const options = {
  estimateAvailable: true,
  estimateProgramIds: new Set([programA, programB])
};

test('parses every estimate table control from compact validated parameters', () => {
  const state = parseHolderNavigationState(
    new URLSearchParams({
      view: 'chain',
      estimate_q: '49mm',
      estimate_type: 'signer',
      estimate_program: programB,
      estimate_sort: 'claim',
      estimate_dir: 'asc',
      estimate_page: '7',
      estimate_rows: '50',
      estimate_custody_sort: 'owner',
      estimate_custody_dir: 'desc',
      estimate_custody_page: '3',
      estimate_custody_rows: '100'
    }),
    options
  );

  assert.deepEqual(state, {
    view: 'chain',
    estimateQuery: '49mm',
    estimateAccountFilter: 'observed_transaction_signer',
    estimateProgramId: programB,
    estimatePortfolioSort: 'claim',
    estimatePortfolioDirection: 'asc',
    estimatePortfolioPageIndex: 6,
    estimatePortfolioPageSize: 50,
    estimateCustodySort: 'owner',
    estimateCustodyDirection: 'desc',
    estimateCustodyPageIndex: 2,
    estimateCustodyPageSize: 100
  });
});

test('rejects unknown programs and invalid enum, page, and row values', () => {
  const state = parseHolderNavigationState(
    new URLSearchParams({
      view: 'bad',
      estimate_type: 'pda',
      estimate_program: 'UnknownProgram1111111111111111111111111111',
      estimate_sort: 'label',
      estimate_dir: 'sideways',
      estimate_page: '-2',
      estimate_rows: '500',
      estimate_custody_sort: 'label',
      estimate_custody_dir: 'sideways',
      estimate_custody_page: '1000001',
      estimate_custody_rows: '1'
    }),
    options
  );

  assert.deepEqual(state, defaultHolderNavigationState(options));
});

test('serializes only non-default estimate state and preserves chain parameters', () => {
  const state = {
    ...defaultHolderNavigationState(options),
    estimateQuery: 'Piggy',
    estimateAccountFilter: 'other_on_curve_account',
    estimateProgramId: programA,
    estimatePortfolioSort: 'authority',
    estimatePortfolioDirection: 'desc',
    estimatePortfolioPageIndex: 1,
    estimatePortfolioPageSize: 100,
    estimateCustodySort: 'program',
    estimateCustodyDirection: 'desc',
    estimateCustodyPageIndex: 4,
    estimateCustodyPageSize: 50
  };
  const url = holderNavigationUrl(
    new URL('https://spyx.blockzilla.dev/holders?chain_q=whale&unrelated=keep'),
    state,
    options
  );

  assert.equal(url.searchParams.get('chain_q'), 'whale');
  assert.equal(url.searchParams.get('unrelated'), 'keep');
  assert.equal(url.searchParams.get('estimate_q'), 'Piggy');
  assert.equal(url.searchParams.get('estimate_type'), 'other');
  assert.equal(url.searchParams.get('estimate_program'), programA);
  assert.equal(url.searchParams.get('estimate_sort'), 'authority');
  assert.equal(url.searchParams.get('estimate_dir'), 'desc');
  assert.equal(url.searchParams.get('estimate_page'), '2');
  assert.equal(url.searchParams.get('estimate_rows'), '100');
  assert.equal(url.searchParams.get('estimate_custody_sort'), 'program');
  assert.equal(url.searchParams.get('estimate_custody_dir'), 'desc');
  assert.equal(url.searchParams.get('estimate_custody_page'), '5');
  assert.equal(url.searchParams.get('estimate_custody_rows'), '50');
  assert.equal(url.searchParams.has('view'), false);
});

test('uses the sort-specific direction when the direction parameter is absent', () => {
  const state = parseHolderNavigationState(
    new URLSearchParams({
      estimate_sort: 'program',
      estimate_custody_sort: 'custody'
    }),
    options
  );

  assert.equal(state.estimatePortfolioDirection, 'asc');
  assert.equal(state.estimateCustodyDirection, 'desc');
});

test('defaults to chain but preserves an explicit estimate view when data is unavailable', () => {
  const unavailableOptions = { estimateAvailable: false, estimateProgramIds: new Set() };
  const explicitState = parseHolderNavigationState(
    new URLSearchParams({ view: 'estimate', estimate_program: programA }),
    unavailableOptions
  );
  const defaultState = parseHolderNavigationState(new URLSearchParams(), unavailableOptions);

  assert.equal(defaultState.view, 'chain');
  assert.equal(explicitState.view, 'estimate');
  assert.equal(explicitState.estimateProgramId, 'all');
});
