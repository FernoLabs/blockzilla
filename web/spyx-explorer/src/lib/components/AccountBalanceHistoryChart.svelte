<script lang="ts">
  import { SvelteMap } from 'svelte/reactivity';
  import { formatBaseUnits, formatInteger } from '$lib/format';
  import type { AccountBalanceHistoryResponse } from '$lib/search-api';
  import type { AuthorityPortfolio, AuthorityPortfolioHistorySeries } from '$lib/types';

  interface Props {
    direct: AccountBalanceHistoryResponse | null;
    history?: AuthorityPortfolioHistorySeries | null;
    portfolio?: AuthorityPortfolio | null;
    decimals?: number;
  }

  interface PlotPoint {
    transactionId: number;
    slot: number;
    blockTime: number | null;
    raw: string;
  }

  const width = 920;
  const height = 280;
  const margin = { top: 18, right: 20, bottom: 38, left: 70 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;

  let { direct, history = null, portfolio = null, decimals = 8 }: Props = $props();

  const exactDirectPoints = $derived<PlotPoint[]>(
    (direct?.items ?? []).map((item) => ({
      transactionId: item.transaction_id,
      slot: item.slot,
      blockTime: item.block_time,
      raw: item.post_raw_balance
    }))
  );
  const sampledDirectPoints = $derived<PlotPoint[]>(
    (history?.points ?? []).map((point) => ({
      transactionId: point.transaction_id,
      slot: point.slot,
      blockTime: point.block_time ?? null,
      raw: point.direct_public_balance.raw_amount
    }))
  );
  const sourceDirectPoints = $derived.by(() =>
    exactDirectPoints.length > 0
      ? carryForwardToHistoryEnd(exactDirectPoints, sampledDirectPoints)
      : sampledDirectPoints
  );
  const exactDefiPoints = $derived(buildDefiPrincipalPoints(portfolio));
  const directPoints = $derived.by(() =>
    carryForwardToHistoryEnd(sourceDirectPoints, exactDefiPoints)
  );
  const defiPoints = $derived.by(() =>
    carryForwardToHistoryEnd(exactDefiPoints, sourceDirectPoints)
  );
  const domain = $derived.by(() => {
    const points = [...directPoints, ...defiPoints];
    if (points.length === 0) return null;
    const first = Math.min(...points.map((point) => point.slot));
    const last = Math.max(...points.map((point) => point.slot));
    const maximum = points.reduce(
      (current, point) => (BigInt(point.raw) > current ? BigInt(point.raw) : current),
      0n
    );
    return { first, last, maximum: maximum === 0n ? 1n : maximum };
  });
  const directPath = $derived(domain ? stepPath(directPoints, domain) : '');
  const defiPath = $derived(domain ? stepPath(defiPoints, domain, true) : '');
  const yTicks = $derived(domain ? tickValues(domain.maximum, 4) : []);
  const firstPoint = $derived(firstByTransaction(directPoints, defiPoints));
  const lastPoint = $derived(lastByTransaction(directPoints, defiPoints));
  const finalDirectPoint = $derived(lastByTransaction(directPoints));
  const finalDefiPoint = $derived(lastByTransaction(defiPoints));

  function stepPath(
    points: PlotPoint[],
    selectedDomain: { first: number; last: number; maximum: bigint },
    startsAtZero = false
  ): string {
    if (points.length === 0) return '';
    const sorted = points.slice().sort((left, right) => left.transactionId - right.transactionId);
    const first = sorted[0];
    const firstX = xPosition(first.slot, selectedDomain);
    let path = startsAtZero
      ? `M ${xPosition(selectedDomain.first, selectedDomain)} ${yPosition(0n, selectedDomain)} H ${firstX}`
      : `M ${firstX} ${yPosition(BigInt(first.raw), selectedDomain)}`;
    if (startsAtZero && first.raw !== '0') {
      path += ` V ${yPosition(BigInt(first.raw), selectedDomain)}`;
    }
    for (const point of sorted.slice(1)) {
      const x = xPosition(point.slot, selectedDomain);
      path += ` H ${x} V ${yPosition(BigInt(point.raw), selectedDomain)}`;
    }
    return path;
  }

  function buildDefiPrincipalPoints(selectedPortfolio: AuthorityPortfolio | null): PlotPoint[] {
    const changes = new SvelteMap<
      number,
      { transactionId: number; slot: number; blockTime: number | null; delta: bigint }
    >();
    for (const component of selectedPortfolio?.claim_components ?? []) {
      for (const flow of component.candidate_flow_evidence ?? []) {
        const matchedRaw = flow.matched_principal_raw_amount ?? flow.raw_amount;
        const amount = BigInt(flow.direction === 'deposit' ? flow.raw_amount : matchedRaw);
        const delta = flow.direction === 'deposit' ? amount : -amount;
        const existing = changes.get(flow.transaction_id);
        if (existing) {
          existing.delta += delta;
        } else {
          changes.set(flow.transaction_id, {
            transactionId: flow.transaction_id,
            slot: flow.slot,
            blockTime: flow.block_time ?? null,
            delta
          });
        }
      }
    }

    let principal = 0n;
    return [...changes.values()]
      .sort((left, right) => left.transactionId - right.transactionId)
      .map((change) => {
        principal += change.delta;
        if (principal < 0n) principal = 0n;
        return {
          transactionId: change.transactionId,
          slot: change.slot,
          blockTime: change.blockTime,
          raw: principal.toString()
        };
      });
  }

  function carryForwardToHistoryEnd(
    exactPoints: PlotPoint[],
    historyPoints: PlotPoint[]
  ): PlotPoint[] {
    const points = exactPoints.slice();
    const finalExact = lastByTransaction(points);
    const finalHistory = lastByTransaction(historyPoints);
    if (!finalExact || !finalHistory || finalHistory.slot <= finalExact.slot) return points;
    points.push({
      transactionId: finalHistory.transactionId,
      slot: finalHistory.slot,
      blockTime: finalHistory.blockTime,
      raw: finalExact.raw
    });
    return points;
  }

  function xPosition(
    slot: number,
    selectedDomain: { first: number; last: number }
  ): number {
    if (selectedDomain.first === selectedDomain.last) return margin.left + plotWidth / 2;
    return (
      margin.left +
      ((slot - selectedDomain.first) / (selectedDomain.last - selectedDomain.first)) *
        plotWidth
    );
  }

  function yPosition(
    raw: bigint,
    selectedDomain: { maximum: bigint }
  ): number {
    return margin.top + plotHeight * (1 - ratio(raw, selectedDomain.maximum));
  }

  function ratio(value: bigint, maximum: bigint): number {
    const scale = 1_000_000n;
    return Number((value * scale) / maximum) / Number(scale);
  }

  function tickValues(maximum: bigint, count: number): bigint[] {
    return Array.from({ length: count + 1 }, (_, index) =>
      (maximum * BigInt(index)) / BigInt(count)
    ).reverse();
  }

  function baseUnits(raw: bigint): string {
    const divisor = 10n ** BigInt(decimals);
    const whole = raw / divisor;
    const fraction = (raw % divisor).toString().padStart(decimals, '0').replace(/0+$/, '');
    return fraction ? `${whole}.${fraction}` : whole.toString();
  }

  function axisAmount(raw: bigint): string {
    const value = Number(baseUnits(raw));
    if (value >= 1_000_000) return `${formatBaseUnits((value / 1_000_000).toString(), 2)}M`;
    if (value >= 1_000) return `${formatBaseUnits((value / 1_000).toString(), 2)}k`;
    return formatBaseUnits(baseUnits(raw), 2);
  }

  function firstByTransaction(...series: PlotPoint[][]): PlotPoint | null {
    return series.flat().reduce<PlotPoint | null>(
      (current, point) =>
        current === null || point.transactionId < current.transactionId ? point : current,
      null
    );
  }

  function lastByTransaction(...series: PlotPoint[][]): PlotPoint | null {
    return series.flat().reduce<PlotPoint | null>(
      (current, point) =>
        current === null || point.transactionId > current.transactionId ? point : current,
      null
    );
  }

  function pointLabel(point: PlotPoint | null): string {
    if (!point) return '—';
    if (point.blockTime !== null) {
      return new Date(point.blockTime * 1000).toLocaleDateString('en-GB', {
        day: '2-digit',
        month: 'short',
        year: 'numeric',
        timeZone: 'UTC'
      });
    }
    return `Slot ${formatInteger(point.slot)}`;
  }
</script>

{#if domain && (directPoints.length > 0 || defiPoints.length > 0)}
  <div class="chart-legend" aria-label="Holding history series">
    {#if finalDirectPoint}
      <span class="legend-item">
        <span class="legend-swatch direct-swatch" aria-hidden="true"></span>
        <span>Wallet holding</span>
        <strong>{formatBaseUnits(baseUnits(BigInt(finalDirectPoint.raw)), 6)} SPYx</strong>
      </span>
    {/if}
    {#if finalDefiPoint}
      <span class="legend-item">
        <span class="legend-swatch defi-swatch" aria-hidden="true"></span>
        <span>Estimated DeFi holding</span>
        <strong>{formatBaseUnits(baseUnits(BigInt(finalDefiPoint.raw)), 6)} SPYx</strong>
      </span>
    {/if}
  </div>
  <div class="chart-scroll">
    <svg
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label="Wallet SPYx holding and estimated DeFi holding from observed net deposits over indexed slots"
    >
      {#each yTicks as tick, index (`${tick}-${index}`)}
        {@const y = yPosition(tick, domain)}
        <line class="grid-line" x1={margin.left} x2={width - margin.right} y1={y} y2={y} />
        <text class="axis-label y-label" x={margin.left - 10} y={y + 4} text-anchor="end">
          {axisAmount(tick)}
        </text>
      {/each}
      <line
        class="axis-line"
        x1={margin.left}
        x2={width - margin.right}
        y1={height - margin.bottom}
        y2={height - margin.bottom}
      />
      {#if directPath}
        <path class="series direct-series" d={directPath} />
      {/if}
      {#if defiPath}
        <path class="series defi-series" d={defiPath} />
      {/if}
      {#each exactDefiPoints as point, index (`${point.transactionId}-${point.slot}-${index}`)}
        <circle
          class="series-point defi-point"
          cx={xPosition(point.slot, domain)}
          cy={yPosition(BigInt(point.raw), domain)}
          r="2.4"
        />
      {/each}
      <text class="axis-label" x={margin.left} y={height - 12}>{pointLabel(firstPoint)}</text>
      <text class="axis-label" x={width - margin.right} y={height - 12} text-anchor="end">
        {pointLabel(lastPoint)}
      </text>
    </svg>
  </div>
  <div class="chart-meta">
    {#if exactDirectPoints.length > 0}
      <span>{formatInteger(direct?.matching_events ?? 0)} exact balance changes</span>
      {#if direct?.sampled}<span>Sampled to {formatInteger(direct.items.length)} points</span>{/if}
    {:else if sampledDirectPoints.length > 0}
      <span>{formatInteger(sampledDirectPoints.length)} exact replay samples</span>
    {/if}
    {#if exactDefiPoints.length > 0}
      <span>{formatInteger(exactDefiPoints.length)} observed DeFi flow changes</span>
    {/if}
  </div>
{/if}

<style>
  .chart-legend,
  .chart-meta {
    display: flex;
    flex-wrap: wrap;
    gap: 8px 18px;
    color: var(--muted);
    font-size: 0.8rem;
  }

  .legend-item {
    display: inline-flex;
    align-items: center;
    gap: 7px;
  }

  .legend-item strong {
    color: var(--text);
    font-variant-numeric: tabular-nums;
  }

  .legend-swatch {
    width: 18px;
    height: 2px;
    background: var(--accent);
  }

  .defi-swatch {
    height: 0;
    border-top: 2px dashed #8b5a2b;
    background: transparent;
  }

  .defi-series {
    stroke: #8b5a2b;
    stroke-dasharray: 6 4;
  }

  .defi-point {
    fill: #8b5a2b;
  }

  .chart-scroll {
    overflow-x: auto;
  }

  svg {
    display: block;
    width: 100%;
    min-width: 620px;
    margin-top: 8px;
  }

  .grid-line,
  .axis-line {
    stroke: var(--border);
    stroke-width: 1;
    vector-effect: non-scaling-stroke;
  }

  .grid-line {
    opacity: 0.7;
  }

  .axis-label {
    fill: var(--muted);
    font-size: 12px;
  }

  .series {
    fill: none;
    stroke-width: 2;
    vector-effect: non-scaling-stroke;
  }

  .direct-series {
    stroke: var(--accent);
  }

  .series-point {
    vector-effect: non-scaling-stroke;
  }

  .chart-meta {
    justify-content: space-between;
    margin-top: 4px;
  }

  @media (max-width: 720px) {
    .chart-scroll {
      overflow-x: visible;
    }

    svg {
      min-width: 0;
    }

    .axis-label {
      font-size: 22px;
    }
  }
</style>
