<script lang="ts">
  import {
    resolveMarketScaledUiEventAt,
    type MarketScaledUiAmountEvent,
    type MarketScaledUiHistory
  } from '$lib/market-api';

  type LoadStatus = 'loading' | 'ready' | 'unavailable';

  interface TimelinePoint {
    timestamp: number;
    event: MarketScaledUiAmountEvent;
    value: number;
  }

  const chartWidth = 960;
  const chartHeight = 286;
  const plot = { left: 68, top: 34, right: 18, bottom: 36 };
  const chartId = $props.id();

  let {
    history = null,
    datasetTip = null,
    status = 'unavailable'
  }: {
    history?: MarketScaledUiHistory | null;
    datasetTip?: number | null;
    status?: LoadStatus;
  } = $props();

  const timeline = $derived.by((): TimelinePoint[] => {
    if (!history?.enabled || history.events.length === 0 || datasetTip === null) return [];

    const firstTimestamp = history.events[0].coordinate.block_time;
    if (datasetTip < firstTimestamp) return [];

    const boundaries = [firstTimestamp, datasetTip];
    for (const event of history.events) {
      if (event.coordinate.block_time >= firstTimestamp && event.coordinate.block_time <= datasetTip) {
        boundaries.push(event.coordinate.block_time);
      }
      const effectiveTimestamp = Math.max(0, event.effective_timestamp);
      if (effectiveTimestamp >= firstTimestamp && effectiveTimestamp <= datasetTip) {
        boundaries.push(effectiveTimestamp);
      }
    }

    const points: TimelinePoint[] = [];
    for (const timestamp of boundaries.sort((left, right) => left - right)) {
      if (points.at(-1)?.timestamp === timestamp) continue;
      const event = resolveMarketScaledUiEventAt(history, timestamp);
      if (!event || points.at(-1)?.event.multiplier.bits === event.multiplier.bits) continue;
      points.push({ timestamp, event, value: Number(event.multiplier.decimal) });
    }
    return points;
  });

  const currentPoint = $derived(timeline.at(-1) ?? null);
  const startTimestamp = $derived(timeline[0]?.timestamp ?? 0);
  const endTimestamp = $derived(datasetTip ?? startTimestamp);
  const yDomain = $derived.by(() => {
    const values = timeline.map((point) => point.value);
    const minimum = Math.min(...values);
    const maximum = Math.max(...values);
    const padding = Math.max((maximum - minimum) * 0.14, 0.0001);
    return { minimum: minimum - padding, maximum: maximum + padding };
  });
  const path = $derived(buildStepPath(timeline, startTimestamp, endTimestamp, yDomain));
  const yTicks = $derived(
    [0, 0.25, 0.5, 0.75, 1].map((fraction) => {
      const value = yDomain.minimum + (yDomain.maximum - yDomain.minimum) * fraction;
      return { fraction, value, y: pointY(value, yDomain) };
    })
  );
  const xTicks = $derived(
    [0, 0.25, 0.5, 0.75, 1].map((fraction) => {
      const timestamp = Math.round(startTimestamp + (endTimestamp - startTimestamp) * fraction);
      return { fraction, timestamp, x: pointX(timestamp, startTimestamp, endTimestamp) };
    })
  );

  function buildStepPath(
    points: TimelinePoint[],
    start: number,
    end: number,
    domain: { minimum: number; maximum: number }
  ): string {
    if (points.length === 0) return '';
    const commands = [`M ${pointX(points[0].timestamp, start, end)},${pointY(points[0].value, domain)}`];
    for (let index = 1; index < points.length; index += 1) {
      const previous = points[index - 1];
      const current = points[index];
      const x = pointX(current.timestamp, start, end);
      commands.push(`L ${x},${pointY(previous.value, domain)}`);
      commands.push(`L ${x},${pointY(current.value, domain)}`);
    }
    const last = points.at(-1);
    if (last) commands.push(`L ${pointX(end, start, end)},${pointY(last.value, domain)}`);
    return commands.join(' ');
  }

  function pointX(timestamp: number, start: number, end: number): number {
    const width = chartWidth - plot.left - plot.right;
    return plot.left + (end <= start ? 0 : ((timestamp - start) / (end - start)) * width);
  }

  function pointY(value: number, domain: { minimum: number; maximum: number }): number {
    const height = chartHeight - plot.top - plot.bottom;
    const range = domain.maximum - domain.minimum;
    return plot.top + height - ((value - domain.minimum) / range) * height;
  }

  function formatAxis(value: number): string {
    return `×${value.toLocaleString('en-US', {
      minimumFractionDigits: 4,
      maximumFractionDigits: 4
    })}`;
  }

  function formatPoint(value: number): string {
    return `×${value.toLocaleString('en-US', {
      minimumFractionDigits: 4,
      maximumFractionDigits: 6
    })}`;
  }

  function formatMonth(timestamp: number): string {
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      year: '2-digit',
      timeZone: 'UTC'
    }).format(new Date(timestamp * 1_000));
  }

  function formatTimestamp(timestamp: number): string {
    return new Intl.DateTimeFormat('en-US', {
      dateStyle: 'medium',
      timeStyle: 'short',
      timeZone: 'UTC'
    }).format(new Date(timestamp * 1_000));
  }
</script>

{#if status === 'loading'}
  <div class="chart-state" aria-live="polite">Loading multiplier history…</div>
{:else if status !== 'ready' || timeline.length === 0 || !currentPoint}
  <div class="chart-state">Multiplier history is not available for this dataset.</div>
{:else}
  <div class="multiplier-summary" aria-label="Current Token-2022 UI multiplier">
    <span>Current at indexed tip</span>
    <strong class="mono">×{currentPoint.event.multiplier.decimal}</strong>
    <span>{Math.max(0, timeline.length - 1)} changes</span>
  </div>

  <div class="chart-wrap">
    <svg
      class="multiplier-chart"
      viewBox={`0 0 ${chartWidth} ${chartHeight}`}
      role="img"
      aria-labelledby={`${chartId}-title ${chartId}-description`}
    >
      <title id={`${chartId}-title`}>SPYx Token-2022 UI multiplier history</title>
      <desc id={`${chartId}-description`}>
        Step timeline with {timeline.length} multiplier levels and {Math.max(0, timeline.length - 1)} changes.
      </desc>

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

      {#if path}
        <path class="step-line" d={path} />
      {/if}

      {#each timeline as point, index (point.event.config_id)}
        <line
          class="change-line"
          x1={pointX(point.timestamp, startTimestamp, endTimestamp)}
          x2={pointX(point.timestamp, startTimestamp, endTimestamp)}
          y1={pointY(point.value, yDomain)}
          y2={chartHeight - plot.bottom}
        />
        <circle
          class="change-point"
          cx={pointX(point.timestamp, startTimestamp, endTimestamp)}
          cy={pointY(point.value, yDomain)}
          r="4"
        >
          <title>{formatTimestamp(point.timestamp)} UTC · ×{point.event.multiplier.decimal}</title>
        </circle>
        <text
          class="point-label"
          x={pointX(point.timestamp, startTimestamp, endTimestamp)}
          y={pointY(point.value, yDomain) - 10}
          text-anchor={index === 0 ? 'start' : index === timeline.length - 1 ? 'end' : 'middle'}
        >{formatPoint(point.value)}</text>
      {/each}

      {#each xTicks as tick (tick.fraction)}
        <text
          class="axis-label x-label"
          x={tick.x}
          y={chartHeight - 9}
          text-anchor={tick.fraction === 0 ? 'start' : tick.fraction === 1 ? 'end' : 'middle'}
        >{formatMonth(tick.timestamp)}</text>
      {/each}
    </svg>
  </div>
{/if}

<style>
  .multiplier-summary {
    display: flex;
    align-items: baseline;
    gap: 10px;
    padding: 9px 12px 2px;
    color: var(--muted);
    font-size: 11px;
  }

  .multiplier-summary strong {
    color: var(--text);
    font-size: 13px;
    font-variant-numeric: tabular-nums;
  }

  .multiplier-summary span:last-child {
    margin-left: auto;
    color: var(--faint);
  }

  .chart-state {
    min-height: 300px;
    display: grid;
    place-items: center;
    padding: 24px;
    color: var(--muted);
    font-size: 12px;
  }

  .chart-wrap {
    min-height: 300px;
    padding: 5px 12px 12px;
  }

  .multiplier-chart {
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

  .step-line {
    fill: none;
    stroke: var(--accent);
    stroke-width: 2;
    stroke-linecap: square;
    stroke-linejoin: miter;
    vector-effect: non-scaling-stroke;
  }

  .change-line {
    stroke: color-mix(in srgb, var(--accent) 22%, transparent);
    stroke-width: 1;
    stroke-dasharray: 2 4;
    vector-effect: non-scaling-stroke;
  }

  .change-point {
    fill: var(--surface);
    stroke: var(--accent);
    stroke-width: 2;
    vector-effect: non-scaling-stroke;
  }

  .axis-label,
  .point-label {
    fill: var(--muted);
    font-size: 10px;
    font-variant-numeric: tabular-nums;
  }

  .point-label {
    fill: var(--text);
    font-size: 9px;
    font-weight: 650;
  }

  .y-label {
    text-anchor: end;
  }

  @media (max-width: 760px) {
    .chart-wrap {
      min-height: 0;
      padding-inline: 4px;
      overflow-x: auto;
    }

    .multiplier-chart {
      min-width: 600px;
      min-height: 0;
    }
  }
</style>
