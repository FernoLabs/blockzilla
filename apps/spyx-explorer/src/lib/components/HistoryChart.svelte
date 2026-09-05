<script lang="ts">
  import { afterNavigate, goto } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { page } from '$app/state';
  import ScaledUiMultiplierChart from '$lib/components/ScaledUiMultiplierChart.svelte';
  import type { MarketScaledUiHistory } from '$lib/market-api';
  import type { DailyRow } from '$lib/types';
  import { formatBaseUnits, formatCompact, formatDate, formatInteger } from '$lib/format';

  type ChartMode =
    | 'balance'
    | 'accounts'
    | 'concentration'
    | 'movement'
    | 'transactions'
    | 'multiplier';
  type SummaryMode = 'latest' | 'total';

  interface SeriesDefinition {
    id: string;
    label: string;
    color: string;
    dash?: string;
    value: (row: DailyRow) => number;
  }

  interface ModeDefinition {
    id: ChartMode;
    label: string;
    description: string;
    summary: SummaryMode;
    series: SeriesDefinition[];
  }

  const chartWidth = 960;
  const chartHeight = 286;
  const plot = { left: 68, top: 18, right: 16, bottom: 34 };
  const modes: ModeDefinition[] = [
    {
      id: 'balance',
      label: 'Public balance',
      description: 'Daily public balance from stored public Token-2022 balance metadata.',
      summary: 'latest',
      series: [
        {
          id: 'balance',
          label: 'Public balance',
          color: '#0f766e',
          value: (row) => Number(row.public_raw_balance_sum.base_units)
        }
      ]
    },
    {
      id: 'accounts',
      label: 'Owners and accounts',
      description: 'Owners with a positive balance and open token accounts at each day end.',
      summary: 'latest',
      series: [
        {
          id: 'owners',
          label: 'Owners with a balance',
          color: '#0f766e',
          value: (row) => row.positive_public_balance_holders
        },
        {
          id: 'accounts',
          label: 'Open token accounts',
          color: '#3568a6',
          dash: '7 4',
          value: (row) => row.active_public_token_accounts
        }
      ]
    },
    {
      id: 'concentration',
      label: 'Concentration',
      description: 'Share of the daily public balance held by the largest 1, 10, and 100 owners.',
      summary: 'latest',
      series: [
        {
          id: 'top-1',
          label: 'Largest owner',
          color: '#9a3412',
          value: (row) => row.top_1_concentration.supply_share_parts_per_million_floor / 10_000
        },
        {
          id: 'top-10',
          label: 'Largest 10 owners',
          color: '#8a5a00',
          dash: '7 4',
          value: (row) => row.top_10_concentration.supply_share_parts_per_million_floor / 10_000
        },
        {
          id: 'top-100',
          label: 'Largest 100 owners',
          color: '#6554a4',
          dash: '2 4',
          value: (row) => row.top_100_concentration.supply_share_parts_per_million_floor / 10_000
        }
      ]
    },
    {
      id: 'movement',
      label: 'Movement, mint, burn',
      description:
        'Daily account-to-account movement, inferred mint, and inferred burn. These are not trade or USD values.',
      summary: 'total',
      series: [
        {
          id: 'movement',
          label: 'Account-to-account movement',
          color: '#0f766e',
          value: (row) => Number(row.public_bilateral_movement.base_units)
        },
        {
          id: 'mint',
          label: 'Inferred public mint',
          color: '#3568a6',
          dash: '7 4',
          value: (row) => Number(row.inferred_public_mint.base_units)
        },
        {
          id: 'burn',
          label: 'Inferred public burn',
          color: '#9a3412',
          dash: '2 4',
          value: (row) => Number(row.inferred_public_burn.base_units)
        }
      ]
    },
    {
      id: 'transactions',
      label: 'Transactions',
      description:
        'Daily indexed transactions, balance-changing transactions, and token-account owner changes.',
      summary: 'total',
      series: [
        {
          id: 'selected',
          label: 'Indexed transactions',
          color: '#3568a6',
          value: (row) => row.selected_transactions
        },
        {
          id: 'balance-changing',
          label: 'Balance-changing transactions',
          color: '#0f766e',
          dash: '7 4',
          value: (row) => row.public_balance_changing_transactions
        },
        {
          id: 'owner-reassignment',
          label: 'Token-account owner changes',
          color: '#9a3412',
          dash: '2 4',
          value: (row) => row.public_owner_reassignment_transactions
        }
      ]
    },
    {
      id: 'multiplier',
      label: 'UI multiplier',
      description: 'Token-2022 multiplier used to convert raw amounts into displayed SPYx amounts.',
      summary: 'latest',
      series: []
    }
  ];

  let {
    rows,
    scaledUiHistory = null,
    scaledUiDatasetTip = null,
    scaledUiStatus
  }: {
    rows: DailyRow[];
    scaledUiHistory?: MarketScaledUiHistory | null;
    scaledUiDatasetTip?: number | null;
    scaledUiStatus?: 'loading' | 'ready' | 'unavailable';
  } = $props();
  const routePath = page.url.pathname;
  const availableModes = $derived(
    scaledUiStatus === undefined ? modes.filter((item) => item.id !== 'multiplier') : modes
  );
  let mode = $state<ChartMode>('accounts');

  const activeMode = $derived(availableModes.find((item) => item.id === mode) ?? availableModes[0]);
  const maximum = $derived(
    Math.max(1, ...rows.flatMap((row) => activeMode.series.map((series) => series.value(row))))
  );
  const plottedSeries = $derived.by(() =>
    activeMode.series.map((series) => {
      const values = rows.map((row) => series.value(row));
      return {
        ...series,
        path: values
          .map((value, index) => `${pointX(index, values.length)},${pointY(value, maximum)}`)
          .join(' L '),
        summaryValue:
          activeMode.summary === 'total'
            ? values.reduce((total, value) => total + value, 0)
            : (values.at(-1) ?? 0)
      };
    })
  );
  const yTicks = $derived(
    [0, 0.25, 0.5, 0.75, 1].map((fraction) => ({
      fraction,
      value: maximum * fraction,
      y: pointY(maximum * fraction, maximum)
    }))
  );
  const xTicks = $derived.by(() => {
    if (!rows.length) return [];
    return [0, 0.25, 0.5, 0.75, 1].map((fraction) => {
      const index = Math.round((rows.length - 1) * fraction);
      return {
        index,
        x: pointX(index, rows.length),
        date: rows[index].utc_date
      };
    });
  });
  const latestDate = $derived(rows.at(-1)?.utc_date);

  afterNavigate((navigation) => {
    const destination = navigation.to?.url;
    if (!destination || destination.pathname !== routePath) return;
    mode = readChartMode(destination, scaledUiStatus !== undefined);
  });

  function readChartMode(url: URL, multiplierEnabled: boolean): ChartMode {
    const value = url.searchParams.get('chart');
    return modes.some((item) => item.id === value) && (value !== 'multiplier' || multiplierEnabled)
      ? (value as ChartMode)
      : 'accounts';
  }

  function selectMode(nextMode: ChartMode): void {
    if (mode === nextMode) return;
    mode = nextMode;
    const url = new URL(page.url);
    if (nextMode === 'accounts') url.searchParams.delete('chart');
    else url.searchParams.set('chart', nextMode);
    if (routePath === resolve('/holders')) {
      void goto(resolve(`/holders${url.search}${url.hash}` as '/holders'), {
        keepFocus: true,
        noScroll: true
      });
      return;
    }
    void goto(resolve(`/${url.search}${url.hash}` as '/'), {
      keepFocus: true,
      noScroll: true
    });
  }

  function pointX(index: number, length: number): number {
    const width = chartWidth - plot.left - plot.right;
    return plot.left + (length <= 1 ? 0 : (index / (length - 1)) * width);
  }

  function pointY(value: number, max: number): number {
    const height = chartHeight - plot.top - plot.bottom;
    return plot.top + height - (value / max) * height;
  }

  function formatValue(value: number): string {
    if (mode === 'concentration') {
      return `${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 4 })}%`;
    }
    if (mode === 'accounts' || mode === 'transactions') return formatInteger(value);
    return `${formatBaseUnits(String(value), 2)} SPYx`;
  }

  function formatAxis(value: number): string {
    if (mode === 'concentration') return `${value.toLocaleString('en-US', { maximumFractionDigits: 1 })}%`;
    return formatCompact(value);
  }
</script>

<div class="chart-tabs" aria-label="Public metadata history measure">
  {#each availableModes as option (option.id)}
    <button
      type="button"
      aria-pressed={mode === option.id}
      class:active={mode === option.id}
      onclick={() => selectMode(option.id)}
    >
      {option.label}
    </button>
  {/each}
</div>

<div class="chart-context">
  <p>{activeMode.description}</p>
  <span>
    {mode === 'multiplier'
      ? 'Effective time in UTC'
      : activeMode.summary === 'total'
      ? 'Totals for the full date range'
      : latestDate
        ? `State at ${formatDate(latestDate)}`
        : 'No daily rows'}
  </span>
</div>

{#if mode === 'multiplier'}
  <ScaledUiMultiplierChart
    history={scaledUiHistory}
    datasetTip={scaledUiDatasetTip}
    status={scaledUiStatus}
  />
{:else}
  <div class="chart-legend" aria-label="Chart series">
    {#each plottedSeries as series (series.id)}
      <div class="legend-item">
        <svg width="26" height="8" aria-hidden="true">
          <line
            x1="1"
            x2="25"
            y1="4"
            y2="4"
            stroke={series.color}
            stroke-width="2"
            stroke-dasharray={series.dash}
          />
        </svg>
        <span>{series.label}</span>
        <strong>{formatValue(series.summaryValue)}</strong>
      </div>
    {/each}
  </div>

  <div class="chart-wrap">
    <svg
      class="history-chart"
      viewBox={`0 0 ${chartWidth} ${chartHeight}`}
      role="img"
      aria-labelledby="history-chart-title history-chart-description"
    >
      <title id="history-chart-title">SPYx {activeMode.label} history</title>
      <desc id="history-chart-description">{activeMode.description}</desc>

    {#each yTicks as tick (tick.fraction)}
      <line
        class="grid-line"
        x1={plot.left}
        x2={chartWidth - plot.right}
        y1={tick.y}
        y2={tick.y}
      />
      <text class="axis-label y-label" x={plot.left - 9} y={tick.y + 4}>{formatAxis(tick.value)}</text>
    {/each}

    {#each plottedSeries as series (series.id)}
      {#if series.path}
        <path
          class="series-line"
          d={`M ${series.path}`}
          stroke={series.color}
          stroke-dasharray={series.dash}
        />
      {/if}
    {/each}

    {#each xTicks as tick (tick.index)}
      <text
        class="axis-label x-label"
        x={tick.x}
        y={chartHeight - 9}
        text-anchor={tick.index === 0 ? 'start' : tick.index === rows.length - 1 ? 'end' : 'middle'}
      >
        {tick.date.slice(0, 7)}
      </text>
    {/each}
    </svg>
  </div>
{/if}

<style>
  .chart-tabs {
    display: flex;
    overflow-x: auto;
    border-bottom: 1px solid var(--border);
    background: #fbfbfc;
  }

  .chart-tabs button {
    min-height: 38px;
    padding: 7px 12px;
    border: 0;
    border-right: 1px solid var(--border);
    border-bottom: 2px solid transparent;
    color: var(--muted);
    background: transparent;
    white-space: nowrap;
  }

  .chart-tabs button:hover,
  .chart-tabs button.active {
    color: var(--text);
    background: var(--surface);
  }

  .chart-tabs button.active {
    border-bottom-color: var(--accent);
  }

  .chart-tabs button:focus-visible {
    position: relative;
    z-index: 1;
    outline: 2px solid var(--accent);
    outline-offset: -3px;
  }

  .chart-context {
    min-height: 46px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding: 8px 12px;
    border-bottom: 1px solid var(--border);
  }

  .chart-context p {
    margin: 0;
    color: var(--muted);
    font-size: 12px;
  }

  .chart-context span {
    flex: none;
    color: var(--faint);
    font-size: 11px;
  }

  .chart-legend {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 8px 16px;
    padding: 9px 12px 2px;
  }

  .legend-item {
    min-width: 0;
    display: grid;
    grid-template-columns: 26px minmax(0, 1fr) auto;
    align-items: center;
    gap: 7px;
    color: var(--muted);
    font-size: 11px;
  }

  .legend-item span {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .legend-item strong {
    color: var(--text);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .chart-wrap {
    min-height: 300px;
    padding: 5px 12px 12px;
  }

  .history-chart {
    display: block;
    width: 100%;
    height: auto;
    min-height: 260px;
  }

  .grid-line {
    stroke: var(--grid);
    stroke-width: 1;
    vector-effect: non-scaling-stroke;
  }

  .series-line {
    fill: none;
    stroke-width: 1.8;
    stroke-linecap: round;
    stroke-linejoin: round;
    vector-effect: non-scaling-stroke;
  }

  .axis-label {
    fill: var(--muted);
    font-size: 10px;
    font-variant-numeric: tabular-nums;
  }

  .y-label {
    text-anchor: end;
  }

  @media (max-width: 760px) {
    .chart-context {
      align-items: flex-start;
      flex-direction: column;
      gap: 3px;
    }

    .chart-legend {
      grid-template-columns: 1fr;
    }

    .chart-wrap {
      min-height: 0;
      padding-inline: 4px;
      overflow-x: auto;
    }

    .history-chart {
      min-width: 500px;
      min-height: 0;
    }
  }
</style>
