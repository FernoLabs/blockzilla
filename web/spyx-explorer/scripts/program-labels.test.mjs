import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildProgramOptions,
  programDisplayName,
  programOptionLabel
} from '../src/lib/program-labels.js';
import {
  buildCustodyProgramHoldings,
  buildProgramHoldings
} from '../src/lib/program-holdings.js';

test('keeps every PDA program ID when labels are absent', () => {
  const options = buildProgramOptions(
    [
      { program_id: 'Z-program', program_name: 'Zulu' },
      { program_id: 'A-unlabeled', program_name: null },
      { program_id: 'B-empty-label', program_name: '' }
    ],
    [
      { pda_program_id: 'C-holder-only', pda_program_name: null },
      { pda_program_id: 'A-unlabeled', pda_program_name: null }
    ]
  );

  assert.deepEqual(
    options.map((option) => option.id),
    ['A-unlabeled', 'B-empty-label', 'C-holder-only', 'Z-program']
  );
  assert.equal(options[0].label, 'Unlabeled program — A-unlabeled');
  assert.equal(options[1].label, 'Unlabeled program — B-empty-label');
  assert.equal(options[2].label, 'Unlabeled program — C-holder-only');
  assert.equal(options[3].label, 'Zulu — Z-program');
});

test('program labels do not change option inclusion or order', () => {
  const idsWithoutLabels = buildProgramOptions([
    { program_id: 'B-program', program_name: null },
    { program_id: 'A-program', program_name: null }
  ]).map((option) => option.id);
  const idsWithLabels = buildProgramOptions([
    { program_id: 'B-program', program_name: 'Alpha label' },
    { program_id: 'A-program', program_name: 'Zulu label' }
  ]).map((option) => option.id);

  assert.deepEqual(idsWithLabels, idsWithoutLabels);
  assert.deepEqual(idsWithLabels, ['A-program', 'B-program']);
});

test('merges direct PDA and runtime owner IDs without dropping unlabeled programs', () => {
  const options = buildProgramOptions(
    [
      { program_id: 'Pda-program', program_name: 'Known PDA program' },
      { program_id: 'Runtime-unknown', program_name: null },
      { program_id: 'Runtime-known', program_name: 'Known runtime program' }
    ],
    [{ pda_program_id: 'Holder-only', pda_program_name: null }]
  );

  assert.deepEqual(
    options.map((option) => option.id),
    ['Holder-only', 'Pda-program', 'Runtime-known', 'Runtime-unknown']
  );
  assert.equal(options[0].label, 'Unlabeled program — Holder-only');
  assert.equal(options[3].label, 'Unlabeled program — Runtime-unknown');
});

test('shows an explicit unlabeled marker with the full program ID', () => {
  assert.equal(programDisplayName(null), 'Unlabeled program');
  assert.equal(
    programOptionLabel('FullProgramAddress1111111111111111111111111', null),
    'Unlabeled program — FullProgramAddress1111111111111111111111111'
  );
});

test('merges program evidence by ID and counts an overlapping holder once', () => {
  const parserHolder = {
    owner: 'shared-holder',
    token_account_count: 2,
    public_balance: { raw_amount: '100' },
    public_activity_volume: { raw_amount: '300' },
    pda_program_id: 'program-a',
    pda_program_name: 'Known program'
  };
  const runtimeHolders = [
    {
      ...parserHolder,
      supplemental_program_attribution: {
        account_exists: true,
        runtime_owner_program_id: 'program-a',
        runtime_owner_program_name: 'Different display label'
      }
    },
    {
      owner: 'runtime-only',
      token_account_count: 1,
      public_balance: { raw_amount: '25' },
      public_activity_volume: { raw_amount: '50' },
      supplemental_program_attribution: {
        account_exists: true,
        runtime_owner_program_id: 'program-a',
        runtime_owner_program_name: 'Different display label'
      }
    },
    {
      owner: 'unknown-program-holder',
      token_account_count: 1,
      public_balance: { raw_amount: '7' },
      public_activity_volume: { raw_amount: '9' },
      supplemental_program_attribution: {
        account_exists: true,
        runtime_owner_program_id: 'unknown-program',
        runtime_owner_program_name: null
      }
    }
  ];

  const rows = buildProgramHoldings([parserHolder], runtimeHolders);
  const program = rows.find((row) => row.program_id === 'program-a');
  assert.deepEqual(program, {
    program_id: 'program-a',
    program_name: 'Known program',
    holder_count: 2,
    parser_holder_count: 1,
    runtime_holder_count: 2,
    overlap_holder_count: 1,
    token_account_count: 3,
    public_balance_raw_amount: '125',
    public_activity_raw_amount: '350'
  });
  assert.equal(rows.find((row) => row.program_id === 'unknown-program').program_name, null);
});

test('groups all custody owners by program ID and keeps unlinked owners visible', () => {
  const holders = [
    {
      owner: 'pda-a',
      token_account_count: 2,
      public_balance: { raw_amount: '100' },
      public_activity_volume: { raw_amount: '300' },
      pda_program_id: 'program-a',
      pda_program_name: null
    },
    {
      owner: 'pda-b',
      token_account_count: 1,
      public_balance: { raw_amount: '25' },
      public_activity_volume: { raw_amount: '50' },
      supplemental_program_attribution: {
        account_exists: true,
        runtime_owner_program_id: 'program-a',
        runtime_owner_program_name: null
      }
    },
    {
      owner: 'pda-c',
      token_account_count: 1,
      public_balance: { raw_amount: '7' },
      public_activity_volume: { raw_amount: '9' },
      pda_program_id: 'program-b',
      pda_program_name: null
    },
    {
      owner: 'unlinked-owner',
      token_account_count: 3,
      public_balance: { raw_amount: '5' },
      public_activity_volume: { raw_amount: '11' }
    }
  ];

  const rows = buildCustodyProgramHoldings(holders);
  assert.deepEqual(rows.map((row) => row.program_id), ['program-a', 'program-b', null]);
  assert.deepEqual(rows[0], {
    program_id: 'program-a',
    program_name: null,
    owner_ids: ['pda-a', 'pda-b'],
    holder_count: 2,
    token_account_count: 3,
    public_balance_raw_amount: '125',
    public_activity_raw_amount: '350'
  });
  assert.deepEqual(rows[2], {
    program_id: null,
    program_name: null,
    owner_ids: ['unlinked-owner'],
    holder_count: 1,
    token_account_count: 3,
    public_balance_raw_amount: '5',
    public_activity_raw_amount: '11'
  });
});

test('custody labels do not change program rows, order, or totals', () => {
  const base = [
    {
      owner: 'pda-a',
      token_account_count: 1,
      public_balance: { raw_amount: '10' },
      public_activity_volume: { raw_amount: '20' },
      pda_program_id: 'program-b',
      pda_program_name: null
    },
    {
      owner: 'pda-b',
      token_account_count: 1,
      public_balance: { raw_amount: '30' },
      public_activity_volume: { raw_amount: '40' },
      pda_program_id: 'program-a',
      pda_program_name: null
    }
  ];
  const relabeled = structuredClone(base);
  relabeled[0].pda_program_name = 'A label that sorts first';
  relabeled[1].pda_program_name = 'A label that sorts last';

  const withoutNames = (holders) =>
    buildCustodyProgramHoldings(holders).map(({ program_name: _programName, ...row }) => row);

  assert.deepEqual(withoutNames(relabeled), withoutNames(base));
});
