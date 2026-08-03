<script lang="ts">
  export type TrendPoint = { at: number; value: number };

  let {
    title,
    points,
    valueLabel,
    rateLabel,
    emptyLabel = 'Collecting samples'
  }: {
    title: string;
    points: TrendPoint[];
    valueLabel: string;
    rateLabel: string;
    emptyLabel?: string;
  } = $props();

  const width = 320;
  const height = 54;
  const inset = 2;
  const path = $derived.by(() => {
    if (points.length < 2) return '';
    const firstAt = points[0].at;
    const duration = Math.max(1, points.at(-1)!.at - firstAt);
    const values = points.map((point) => point.value);
    const minimum = Math.min(...values);
    const maximum = Math.max(...values);
    const span = Math.max(1, maximum - minimum);
    return points.map((point) => {
      const x = inset + ((point.at - firstAt) / duration) * (width - inset * 2);
      const y = height - inset - ((point.value - minimum) / span) * (height - inset * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
  });
</script>

<div class="trend">
  <div class="trend-copy">
    <strong>{title}</strong>
    <span>{valueLabel}</span>
    <small>{points.length > 1 ? rateLabel : emptyLabel}</small>
  </div>
  <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`${title}: ${valueLabel}; ${rateLabel}`}>
    <line x1={inset} y1={height - inset} x2={width - inset} y2={height - inset}></line>
    {#if path}
      <polyline points={path}></polyline>
    {/if}
  </svg>
</div>

<style>
  .trend {
    display: grid;
    grid-template-columns: minmax(170px, 0.7fr) minmax(220px, 1.3fr);
    align-items: center;
    gap: 18px;
    min-height: 82px;
    padding: 12px 14px;
    border-bottom: 1px solid var(--border);
  }

  .trend:last-child {
    border-bottom: 0;
  }

  .trend-copy {
    display: grid;
    gap: 3px;
    min-width: 0;
  }

  strong,
  span,
  small {
    font-variant-numeric: tabular-nums;
  }

  strong {
    font-size: 12px;
    font-weight: 640;
  }

  span {
    font-size: 13px;
  }

  small {
    color: var(--muted);
    font-size: 11px;
  }

  svg {
    width: 100%;
    height: 54px;
    overflow: visible;
  }

  line {
    stroke: var(--border);
    stroke-width: 1;
  }

  polyline {
    fill: none;
    stroke: var(--green);
    stroke-linecap: square;
    stroke-linejoin: round;
    stroke-width: 2;
    vector-effect: non-scaling-stroke;
  }

  @media (max-width: 680px) {
    .trend {
      grid-template-columns: 1fr;
      gap: 8px;
    }
  }
</style>
