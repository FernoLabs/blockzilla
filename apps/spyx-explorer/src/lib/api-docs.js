const MAINNET_USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const EXAMPLE_OWNER = '49mmYSJPUMgu2AcV7u3U3vQ3vean6PbDzV9pw1RmQeQh';
const EXAMPLE_TRANSACTION_ID = 7_311_109;

/**
 * @typedef {{
 *   id: string;
 *   label: string;
 *   description: string;
 *   url: string;
 *   command: string;
 * }} ApiExample
 */

/**
 * @typedef {{
 *   method: 'GET';
 *   path: string;
 *   description: string;
 * }} ApiRoute
 */

/**
 * @typedef {{
 *   id: string;
 *   title: string;
 *   routes: ApiRoute[];
 * }} ApiRouteGroup
 */

/** @type {ApiRouteGroup[]} */
export const API_ROUTE_GROUPS = [
  {
    id: 'service',
    title: 'Service and dataset',
    routes: [
      { method: 'GET', path: '/healthz', description: 'Service health, dataset identity, and index capabilities.' },
      {
        method: 'GET',
        path: '/data/spyx-summary.json',
        description:
          'Static balance history, holder-authority classes, precomputed holder tables, and data-integrity summary used by this explorer.'
      },
      {
        method: 'GET',
        path: '/data/spyx-authority-portfolios.json',
        description:
          'Large true-owner estimate dataset. The holders page loads this file only when it is opened.'
      }
    ]
  },
  {
    id: 'transactions',
    title: 'Transactions',
    routes: [
      { method: 'GET', path: '/api/v1/transactions/{id}', description: 'One indexed transaction by stable ID.' },
      {
        method: 'GET',
        path: '/api/v1/transactions/by-signature/{signature}',
        description: 'One indexed transaction by its exact base58 signature.'
      },
      {
        method: 'GET',
        path: '/api/v1/transactions/by-coordinate?epoch={epoch}&slot={slot}&source_block_id={block}&tx_index={tx}',
        description: 'One transaction by slot and its exact position in the indexed source data.'
      }
    ]
  },
  {
    id: 'postings',
    title: 'Mints, token accounts, accounts, and programs',
    routes: [
      {
        method: 'GET',
        path: '/api/v1/postings/target-address/{key}?limit={1..200}&cursor={next_cursor}',
        description: 'Transactions that mention the SPYx mint or one discovered SPYx token account.'
      },
      {
        method: 'GET',
        path: '/api/v1/postings/token-account/{key}?limit={1..200}&cursor={next_cursor}',
        description: 'Transactions for one discovered SPYx token account.'
      },
      {
        method: 'GET',
        path: '/api/v1/postings/owner/{key}?limit={1..200}&cursor={next_cursor}',
        description: 'SPYx token account activity linked to one account owner.'
      },
      {
        method: 'GET',
        path: '/api/v1/postings/program/{key}?instruction_scope={all|direct|inner}&limit={1..200}&cursor={next_cursor}',
        description:
          'Transactions with a direct top-level instruction, an inner CPI, or either invocation of one program.'
      }
    ]
  },
  {
    id: 'accounts',
    title: 'Account details',
    routes: [
      {
        method: 'GET',
        path: '/api/v1/accounts/{address}/balance-history?transaction_id_from={id}&transaction_id_to={id}&max_points={1..4096}',
        description: 'Exact transaction-final SPYx balance changes across all token accounts owned by one account.'
      },
      {
        method: 'GET',
        path: '/api/v1/accounts/{address}/trading-summary',
        description: 'Parser-proven DEX totals for one exact trader account.'
      },
      {
        method: 'GET',
        path: '/api/v1/accounts/{address}/trades?quote_mint={mint}&program={program}&offset={offset}&limit={1..200}',
        description: 'Parser-proven swaps attributed to one exact trader account.'
      },
      {
        method: 'GET',
        path: '/api/v1/accounts/{address}/trading-activity?interval={seconds|1h|4h|1d|1w}&max_points={1..100000}',
        description: 'Parser-proven buy and sell activity over time for one exact trader account.'
      }
    ]
  },
  {
    id: 'market',
    title: 'Market data',
    routes: [
      { method: 'GET', path: '/api/v1/market/provenance', description: 'Market parser and source evidence.' },
      {
        method: 'GET',
        path: '/api/v1/market/scaled-ui-amount',
        description: 'Exact Token-2022 Scaled UI multiplier history and source proof.'
      },
      {
        method: 'GET',
        path: '/api/v1/market/summary?quote_mint={mint}',
        description: 'SPYx market summary, optionally for one quote mint.'
      },
      { method: 'GET', path: '/api/v1/market/pairs', description: 'All proven SPYx quote pairs.' },
      { method: 'GET', path: '/api/v1/market/mints', description: 'Mint metadata used by proven swaps.' },
      { method: 'GET', path: '/api/v1/market/mints/{address}', description: 'Metadata for one market mint.' },
      { method: 'GET', path: '/api/v1/market/programs', description: 'DEX program summaries.' },
      {
        method: 'GET',
        path: '/api/v1/market/trades?quote_mint={mint}&program={program}&time_from={unix}&time_to={unix}&offset={offset}&limit={1..200}',
        description: 'Proven instruction-level swaps, newest first.'
      },
      { method: 'GET', path: '/api/v1/market/trades/{trade_id}', description: 'One proven swap by stable trade ID.' },
      {
        method: 'GET',
        path: '/api/v1/market/candles?quote_mint={mint}&interval={seconds|1h|4h|1d|1w}&time_from={unix}&time_to={unix}&program={program}&max_points={1..100000}',
        description: 'Chronological, non-empty OHLCV candles.'
      },
      {
        method: 'GET',
        path: '/api/v1/market/slot-candles?quote_mint={mint}&program={program}&slot_from={slot}&slot_to={slot}&max_points={1..100000}',
        description: 'Exact OHLCV for non-empty Solana slots, in canonical instruction order.'
      },
      {
        method: 'GET',
        path: '/api/v1/market/program-volume?interval={seconds|1h|4h|1d|1w}&time_from={unix}&time_to={unix}&quote_mint={mint}&max_points={1..100000}',
        description: 'SPYx volume over time, grouped by the executed DEX program.'
      }
    ]
  }
];

/**
 * Build working examples from the origin that currently serves the explorer.
 *
 * @param {string} origin
 * @returns {ApiExample[]}
 */
export function buildApiExamples(origin) {
  const base = normalizeHttpOrigin(origin);
  const rows = [
    {
      id: 'health',
      label: 'Health and capabilities',
      description: 'Confirm the dataset identity and available indexes.',
      path: '/healthz'
    },
    {
      id: 'holder-summary',
      label: 'Holder-authority snapshot',
      description:
        'Read the four class totals, precomputed top 25 per class, direct PDA custody grouped by attributed program, and token balance history used by the UI.',
      path: '/data/spyx-summary.json'
    },
    {
      id: 'owner-history',
      label: 'Account token activity',
      description: 'Read the first 20 SPYx token-account transactions linked to one account owner.',
      path: `/api/v1/postings/owner/${EXAMPLE_OWNER}?limit=20`
    },
    {
      id: 'account-balance-history',
      label: 'Account holding history',
      description: 'Read exact SPYx balance changes for the example account.',
      path: `/api/v1/accounts/${EXAMPLE_OWNER}/balance-history?max_points=1000`
    },
    {
      id: 'transaction',
      label: 'Transaction record',
      description: 'Read one original indexed transaction.',
      path: `/api/v1/transactions/${EXAMPLE_TRANSACTION_ID}`
    },
    {
      id: 'market-summary',
      label: 'USDC market summary',
      description: 'Read proven SPYx market totals for the USDC quote.',
      path: `/api/v1/market/summary?quote_mint=${MAINNET_USDC_MINT}`
    },
    {
      id: 'market-trades',
      label: 'Recent USDC swaps',
      description: 'Read the newest ten proven instruction-level swaps.',
      path: `/api/v1/market/trades?quote_mint=${MAINNET_USDC_MINT}&limit=10`
    },
    {
      id: 'market-candles',
      label: 'Hourly USDC candles',
      description: 'Read up to 24 chronological OHLCV candles.',
      path: `/api/v1/market/candles?quote_mint=${MAINNET_USDC_MINT}&interval=1h&max_points=24`
    }
  ];

  return rows.map((row) => {
    const url = `${base}${row.path}`;
    return { ...row, url, command: `curl -sS '${url}'` };
  });
}

/**
 * @param {string} value
 * @returns {string}
 */
function normalizeHttpOrigin(value) {
  const parsed = new URL(value);
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new TypeError('API example origin must use HTTP or HTTPS');
  }
  return parsed.origin;
}
