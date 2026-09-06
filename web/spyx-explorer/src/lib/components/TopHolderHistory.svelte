<script lang="ts">
  import { afterNavigate, goto, replaceState } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { page } from '$app/state';
  import { Search } from '@lucide/svelte';
  import { untrack } from 'svelte';
  import { formatCompact, formatDate, formatRawAmount, shortAddress } from '$lib/format';
  import { programDisplayName, programOptionLabel } from '$lib/program-labels.js';
  import type {
    ClassifiedPublicBalanceOwner,
    FinalTopHolderHistory,
    FinalTopHolderHistorySeries,
    HolderAuthorityKind,
    HolderAuthorityReport,
    SupplementalProgramAttribution
  } from '$lib/types';

  type AccountFilter = 'all' | 'observed_signer' | 'pda_or_program' | 'other_on_curve' | 'not_loaded';

  interface ProgramEvidence {
    id: string;
    name: string | null;
    source: 'Parser PDA' | 'Runtime owner';
  }

  interface ChartLine extends FinalTopHolderHistorySeries {
    path: string;
    evidence: ClassifiedPublicBalanceOwner | null;
    program: ProgramEvidence | null;
  }

  interface HistoryUrlState {
    query: string;
    rankLimit: number;
    accountFilter: AccountFilter;
    programId: string;
    selectedOwner: string;
    dayIndex: number;
  }

  const chartWidth = 1_000;
  const chartHeight = 340;
  const plot = { left: 72, top: 18, right: 18, bottom: 32 };
  const rankOptions = [10, 25, 50, 100];
  const colors = [
    '#0f766e', '#3568a6', '#9a3412', '#6554a4', '#8a5a00', '#147d64',
    '#a13b63', '#4774a8', '#6d7432', '#a34a2c', '#3d7d91', '#795da8'
  ];

  let {
    history,
    authority,
    decimals
  }: {
    history: FinalTopHolderHistory;
    authority: HolderAuthorityReport | null;
    decimals: number;
  } = $props();

  const routePath = page.url.pathname;
  const initialUrlState = untrack(() => readHistoryUrlState(page.url));
  let query = $state(initialUrlState.query);
  let rankLimit = $state(initialUrlState.rankLimit);
  let accountFilter = $state<AccountFilter>(initialUrlState.accountFilter);
  let programId = $state(initialUrlState.programId);
  let selectedOwner = $state(initialUrlState.selectedOwner);
  let dayIndex = $state(initialUrlState.dayIndex);

  const evidenceByOwner = $derived.by(() => {
    const rows = authority
      ? [
          ...(authority.attributed_program_holders ?? []),
          ...(authority.off_curve_unattributed_holders ?? []),
          ...authority.largest_25_all,
          ...Object.values(authority.largest_25_by_class).flat(),
          ...(authority.largest_25_by_activity_all ?? []),
          ...Object.values(authority.largest_25_by_activity_by_class ?? {}).flat()
        ]
      : [];
    const result = new Map<string, ClassifiedPublicBalanceOwner>();
    for (const row of rows) {
      const previous = result.get(row.owner);
      result.set(row.owner, previous ? mergeEvidence(previous, row) : row);
    }
    return result;
  });
  const programOptions = $derived.by(() => {
    const names = new Map<string, string | null>();
    for (const series of history.series) {
      const program = programEvidence(evidenceByOwner.get(series.owner));
      if (!program) continue;
      const previous = names.get(program.id);
      if (!names.has(program.id) || (previous === null && program.name !== null)) {
        names.set(program.id, program.name);
      }
    }
    return [...names.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([id, name]) => ({ id, label: programOptionLabel(id, name) }));
  });
  const filteredSeries = $derived.by(() => {
    const term = query.trim().toLowerCase();
    return history.series.filter((series) => {
      if (series.final_rank > rankLimit) return false;
      const evidence = evidenceByOwner.get(series.owner) ?? null;
      const program = programEvidence(evidence ?? undefined);
      if (!matchesAccountFilter(evidence, accountFilter)) return false;
      if (programId !== 'all' && program?.id !== programId) return false;
      if (!term) return true;
      return [series.owner, program?.id]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
        .includes(term);
    });
  });
  const maximumRawBalance = $derived.by(() => {
    let maximum = 1;
    for (const series of filteredSeries) {
      for (const rawBalance of series.daily_raw_balances) {
        maximum = Math.max(maximum, Number(rawBalance));
      }
    }
    return maximum;
  });
  const chartLines = $derived.by(() => {
    const lines: ChartLine[] = filteredSeries.map((series) => ({
      ...series,
      evidence: evidenceByOwner.get(series.owner) ?? null,
      program: programEvidence(evidenceByOwner.get(series.owner)),
      path: series.daily_raw_balances
        .map(
          (value, index) =>
            `${index === 0 ? 'M' : 'L'} ${pointX(index, history.days.length)} ${pointY(Number(value), maximumRawBalance)}`
        )
        .join(' ')
    }));
    const focused = focusedOwner(lines);
    return focused
      ? [...lines.filter((line) => line.owner !== focused), lines.find((line) => line.owner === focused)!]
      : lines;
  });
  const focusedSeries = $derived.by(() => {
    const owner = focusedOwner(chartLines);
    return chartLines.find((series) => series.owner === owner) ?? null;
  });
  const selectedDay = $derived(history.days[Math.min(dayIndex, history.days.length - 1)] ?? null);
  const yTicks = $derived(
    [0, 0.25, 0.5, 0.75, 1].map((fraction) => ({
      value: maximumRawBalance * fraction,
      y: pointY(maximumRawBalance * fraction, maximumRawBalance)
    }))
  );
  const xTicks = $derived.by(() => {
    if (history.days.length === 0) return [];
    return [0, 0.25, 0.5, 0.75, 1].map((fraction) => {
      const index = Math.round((history.days.length - 1) * fraction);
      return { index, x: pointX(index, history.days.length), date: history.days[index].utc_date };
    });
  });

  afterNavigate((navigation) => {
    const destination = navigation.to?.url;
    if (!destination || destination.pathname !== routePath) return;
    applyHistoryUrlState(readHistoryUrlState(destination));
  });

  function readHistoryUrlState(url: URL): HistoryUrlState {
    const rank = Number(url.searchParams.get('history_rank'));
    const type = url.searchParams.get('history_type');
    const requestedOwner = url.searchParams.get('history_owner') ?? '';
    const requestedDate = url.searchParams.get('history_date');
    const dateIndex = requestedDate
      ? history.days.findIndex((day) => day.utc_date === requestedDate)
      : -1;
    return {
      query: url.searchParams.get('history_q') ?? '',
      rankLimit: rankOptions.includes(rank) ? rank : 10,
      accountFilter:
        type === 'observed_signer' ||
        type === 'pda_or_program' ||
        type === 'other_on_curve' ||
        type === 'not_loaded'
          ? type
          : 'all',
      programId: url.searchParams.get('history_program') || 'all',
      selectedOwner: history.series.some((series) => series.owner === requestedOwner)
        ? requestedOwner
        : (history.series[0]?.owner ?? ''),
      dayIndex: dateIndex >= 0 ? dateIndex : Math.max(0, history.days.length - 1)
    };
  }

  function applyHistoryUrlState(state: HistoryUrlState): void {
    query = state.query;
    rankLimit = state.rankLimit;
    accountFilter = state.accountFilter;
    programId = state.programId;
    selectedOwner = state.selectedOwner;
    dayIndex = state.dayIndex;
  }

  function historyStateUrl(): URL {
    const url = new URL(page.url);
    for (const name of [
      'history_q',
      'history_rank',
      'history_type',
      'history_program',
      'history_owner',
      'history_date'
    ]) {
      url.searchParams.delete(name);
    }
    const trimmedQuery = query.trim();
    if (trimmedQuery) url.searchParams.set('history_q', trimmedQuery);
    if (rankLimit !== 10) url.searchParams.set('history_rank', String(rankLimit));
    if (accountFilter !== 'all') url.searchParams.set('history_type', accountFilter);
    if (programId !== 'all') url.searchParams.set('history_program', programId);
    if (selectedOwner && selectedOwner !== history.series[0]?.owner) {
      url.searchParams.set('history_owner', selectedOwner);
    }
    const selectedDate = history.days[dayIndex]?.utc_date;
    if (selectedDate && dayIndex !== history.days.length - 1) {
      url.searchParams.set('history_date', selectedDate);
    }
    return url;
  }

  function commitHistoryState(replace = false): void {
    const url = historyStateUrl();
    if (url.href === page.url.href) return;
    const destination = `/holders${url.search}${url.hash}` as '/holders';
    if (replace) {
      replaceState(resolve(destination), page.state);
      return;
    }
    void goto(resolve(destination), { keepFocus: true, noScroll: true });
  }

  function changeHistoryQuery(value: string): void {
    query = value;
    commitHistoryState(true);
  }

  function changeRankLimit(value: number): void {
    rankLimit = value;
    commitHistoryState();
  }

  function changeAccountFilter(value: AccountFilter): void {
    accountFilter = value;
    commitHistoryState();
  }

  function changeHistoryProgram(value: string): void {
    programId = value;
    commitHistoryState();
  }

  function selectHistoryOwner(owner: string): void {
    selectedOwner = owner;
    commitHistoryState();
  }

  function changeHistoryDay(index: number): void {
    dayIndex = index;
    commitHistoryState(true);
  }

  function mergeEvidence(
    previous: ClassifiedPublicBalanceOwner,
    next: ClassifiedPublicBalanceOwner
  ): ClassifiedPublicBalanceOwner {
    return {
      ...previous,
      ...next,
      pda_program_name: next.pda_program_name ?? previous.pda_program_name,
      supplemental_program_attribution:
        next.supplemental_program_attribution ?? previous.supplemental_program_attribution
    };
  }

  function runtimeProgram(
    attribution: SupplementalProgramAttribution | undefined
  ): ProgramEvidence | null {
    if (attribution?.account_exists !== true || !attribution.runtime_owner_program_id) return null;
    return {
      id: attribution.runtime_owner_program_id,
      name: attribution.runtime_owner_program_name,
      source: 'Runtime owner'
    };
  }

  function programEvidence(
    holder: ClassifiedPublicBalanceOwner | undefined
  ): ProgramEvidence | null {
    if (
      holder?.authority_kind === 'attributed_program_derived_address' &&
      holder.pda_program_id
    ) {
      return {
        id: holder.pda_program_id,
        name: holder.pda_program_name ?? null,
        source: 'Parser PDA'
      };
    }
    return runtimeProgram(holder?.supplemental_program_attribution);
  }

  function matchesAccountFilter(
    evidence: ClassifiedPublicBalanceOwner | null,
    filter: AccountFilter
  ): boolean {
    if (filter === 'all') return true;
    if (filter === 'not_loaded') return evidence === null;
    if (!evidence) return false;
    if (filter === 'observed_signer') {
      return evidence.authority_kind === 'observed_transaction_signer';
    }
    if (filter === 'pda_or_program') {
      return (
        evidence.authority_kind === 'attributed_program_derived_address' ||
        evidence.authority_kind === 'off_curve_unattributed'
      );
    }
    return evidence.authority_kind === 'unclassified_on_curve';
  }

  function authorityLabel(kind: HolderAuthorityKind): string {
    if (kind === 'observed_transaction_signer') return 'Observed signer';
    if (kind === 'attributed_program_derived_address') return 'PDA';
    if (kind === 'off_curve_unattributed') return 'Off-curve';
    return 'Other on-curve';
  }

  function focusedOwner(lines: Array<{ owner: string }>): string {
    return lines.some((line) => line.owner === selectedOwner)
      ? selectedOwner
      : (lines[0]?.owner ?? '');
  }

  function pointX(index: number, count: number): number {
    if (count <= 1) return plot.left;
    return plot.left + (index / (count - 1)) * (chartWidth - plot.left - plot.right);
  }

  function pointY(value: number, maximum: number): number {
    const height = chartHeight - plot.top - plot.bottom;
    return plot.top + height - (value / maximum) * height;
  }

  function lineColor(rank: number): string {
    return colors[(rank - 1) % colors.length];
  }
</script>

<section class="panel holder-history-panel">
  <div class="panel-toolbar history-title">
    <div>
      <h2>Top holder balances over time</h2>
      <span class="panel-toolbar-detail">Fixed cohort ranked at the final dump boundary</span>
    </div>
    <span>{filteredSeries.length} lines</span>
  </div>

  <div class="history-controls">
    <label class="history-search">
      <Search size={16} strokeWidth={1.8} aria-hidden="true" />
      <input
        type="search"
        aria-label="Search holder history"
        placeholder="Find owner or program ID"
        value={query}
        oninput={(event) => changeHistoryQuery(event.currentTarget.value)}
      />
    </label>
    <label>
      <span>Final cohort</span>
      <select
        value={rankLimit}
        onchange={(event) => changeRankLimit(Number(event.currentTarget.value))}
      >
        {#each rankOptions as limit (limit)}
          <option value={limit}>Top {limit}</option>
        {/each}
      </select>
    </label>
    <label>
      <span>Account type</span>
      <select
        value={accountFilter}
        onchange={(event) =>
          changeAccountFilter(event.currentTarget.value as AccountFilter)}
      >
        <option value="all">All holders</option>
        <option value="observed_signer">Signer wallets (observed)</option>
        <option value="pda_or_program">PDA or program account</option>
        <option value="other_on_curve">Other on-curve accounts</option>
        <option value="not_loaded">Type evidence not loaded</option>
      </select>
    </label>
    <label>
      <span>Program</span>
      <select
        value={programId}
        onchange={(event) => changeHistoryProgram(event.currentTarget.value)}
      >
        <option value="all">All programs and unlinked holders</option>
        {#each programOptions as program (program.id)}
          <option value={program.id}>{program.label}</option>
        {/each}
      </select>
    </label>
  </div>

  {#if chartLines.length > 0}
    <div class="history-layout">
      <div class="chart-column">
        <div class="chart-wrap">
          <svg
            viewBox={`0 0 ${chartWidth} ${chartHeight}`}
            role="img"
            aria-label={`Daily SPYx balances for ${chartLines.length} final top holders`}
          >
            {#each yTicks as tick (tick.value)}
              <line class="grid-line" x1={plot.left} x2={chartWidth - plot.right} y1={tick.y} y2={tick.y} />
              <text class="axis-label y-label" x={plot.left - 10} y={tick.y + 4}>
                {formatCompact(Number(formatRawAmount(String(Math.round(tick.value)), decimals, 3).replaceAll(',', '')))}
              </text>
            {/each}
            {#each xTicks as tick (tick.index)}
              <text class="axis-label x-label" x={tick.x} y={chartHeight - 8}>{tick.date.slice(0, 7)}</text>
            {/each}
            {#each chartLines as series (series.owner)}
              <path
                d={series.path}
                fill="none"
                stroke={lineColor(series.final_rank)}
                stroke-width={focusedSeries?.owner === series.owner ? 2.8 : 1.15}
                stroke-opacity={focusedSeries?.owner === series.owner ? 1 : 0.22}
                vector-effect="non-scaling-stroke"
              />
            {/each}
            <line
              class="cursor-line"
              x1={pointX(dayIndex, history.days.length)}
              x2={pointX(dayIndex, history.days.length)}
              y1={plot.top}
              y2={chartHeight - plot.bottom}
            />
            {#if focusedSeries}
              <circle
                cx={pointX(dayIndex, history.days.length)}
                cy={pointY(Number(focusedSeries.daily_raw_balances[dayIndex] ?? '0'), maximumRawBalance)}
                r="4"
                fill={lineColor(focusedSeries.final_rank)}
                stroke="white"
                stroke-width="2"
                vector-effect="non-scaling-stroke"
              />
            {/if}
          </svg>
        </div>

        <div class="date-scrubber">
          <input
            type="range"
            min="0"
            max={Math.max(0, history.days.length - 1)}
            value={dayIndex}
            aria-label="History date"
            oninput={(event) => changeHistoryDay(Number(event.currentTarget.value))}
          />
          <div>
            <strong>{selectedDay ? formatDate(selectedDay.utc_date) : '—'}</strong>
            <span>{selectedDay?.complete_utc_day ? 'Complete UTC day' : 'Source-boundary day'}</span>
          </div>
        </div>
      </div>

      <aside class="history-legend" aria-label="Visible holder histories">
        {#each chartLines.slice().sort((left, right) => left.final_rank - right.final_rank) as series (series.owner)}
          <button
            type="button"
            class:active={focusedSeries?.owner === series.owner}
            aria-pressed={focusedSeries?.owner === series.owner}
            onclick={() => selectHistoryOwner(series.owner)}
          >
            <i style:background={lineColor(series.final_rank)}></i>
            <span class="legend-owner">
              <strong>#{series.final_rank} {shortAddress(series.owner)}</strong>
              <small>
                {series.evidence ? authorityLabel(series.evidence.authority_kind) : 'Type evidence not loaded'}
                {series.program ? ` · ${programDisplayName(series.program.name)}` : ''}
              </small>
            </span>
            <span class="legend-balance">
              {formatRawAmount(series.daily_raw_balances[dayIndex] ?? '0', decimals, 4)}
            </span>
          </button>
        {/each}
      </aside>
    </div>

    {#if focusedSeries}
      <footer class="focused-holder">
        <div>
          <span>Selected owner</span>
          <a
            class="mono"
            href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(focusedSeries.owner)}`)}
          >{focusedSeries.owner}</a>
        </div>
        <div>
          <span>Balance on selected day</span>
          <strong>{formatRawAmount(focusedSeries.daily_raw_balances[dayIndex] ?? '0', decimals, 8)} SPYx</strong>
        </div>
        <div>
          <span>Final balance</span>
          <strong>{formatRawAmount(focusedSeries.final_raw_balance, decimals, 8)} SPYx</strong>
        </div>
      </footer>
    {/if}
  {:else}
    <p class="empty-history">No holder history matches these filters.</p>
  {/if}
</section>

<style>
  .history-title > div:first-child {
    display: flex;
    align-items: baseline;
    gap: 10px;
  }

  .history-title > span {
    color: var(--muted);
    font-size: 12px;
  }

  .history-controls {
    display: grid;
    grid-template-columns: minmax(220px, 1.4fr) 130px minmax(190px, 0.8fr) minmax(260px, 1.1fr);
    gap: 8px;
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
    background: #fbfbfc;
  }

  .history-controls label {
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 3px;
    color: var(--muted);
    font-size: 11px;
  }

  .history-controls select,
  .history-search {
    width: 100%;
    height: 36px;
    border: 1px solid var(--border);
    border-radius: 7px;
    color: var(--text);
    background: var(--surface);
  }

  .history-controls select {
    padding: 0 28px 0 8px;
  }

  .history-search {
    align-self: end;
    flex-direction: row !important;
    align-items: center;
    gap: 8px !important;
    padding: 0 10px;
  }

  .history-search:focus-within {
    border-color: var(--accent);
    outline: 2px solid color-mix(in srgb, var(--accent) 18%, transparent);
  }

  .history-search input {
    min-width: 0;
    width: 100%;
    border: 0;
    outline: 0;
    color: var(--text);
    background: transparent;
  }

  .history-layout {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 300px;
    min-height: 390px;
  }

  .chart-column {
    min-width: 0;
    padding: 12px;
    border-right: 1px solid var(--border);
  }

  .chart-wrap {
    width: 100%;
    overflow-x: auto;
  }

  svg {
    display: block;
    width: 100%;
    min-width: 620px;
    height: auto;
  }

  .grid-line {
    stroke: #e8eaed;
    stroke-width: 1;
    vector-effect: non-scaling-stroke;
  }

  .axis-label {
    fill: var(--muted);
    font-size: 10px;
  }

  .y-label {
    text-anchor: end;
  }

  .x-label {
    text-anchor: middle;
  }

  .cursor-line {
    stroke: #7d858e;
    stroke-dasharray: 3 4;
    stroke-width: 1;
    vector-effect: non-scaling-stroke;
  }

  .date-scrubber {
    display: grid;
    grid-template-columns: minmax(180px, 1fr) auto;
    align-items: center;
    gap: 14px;
    margin-top: 6px;
  }

  .date-scrubber input {
    width: 100%;
  }

  .date-scrubber div {
    display: flex;
    flex-direction: column;
    text-align: right;
  }

  .date-scrubber strong {
    font-size: 12px;
  }

  .date-scrubber span {
    color: var(--muted);
    font-size: 10px;
  }

  .history-legend {
    max-height: 390px;
    overflow-y: auto;
    background: #fbfbfc;
  }

  .history-legend button {
    width: 100%;
    display: grid;
    grid-template-columns: 4px minmax(0, 1fr) auto;
    align-items: center;
    gap: 8px;
    padding: 8px 10px;
    border: 0;
    border-bottom: 1px solid var(--border);
    color: var(--text);
    text-align: left;
    background: transparent;
  }

  .history-legend button:hover,
  .history-legend button.active {
    background: var(--surface);
  }

  .history-legend button.active {
    box-shadow: inset 2px 0 var(--accent);
  }

  .history-legend i {
    width: 4px;
    height: 26px;
    border-radius: 2px;
  }

  .legend-owner {
    min-width: 0;
    display: flex;
    flex-direction: column;
  }

  .legend-owner strong,
  .legend-owner small {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .legend-owner strong {
    font-size: 11px;
  }

  .legend-owner small {
    margin-top: 2px;
    color: var(--muted);
    font-size: 9px;
  }

  .legend-balance {
    font-size: 10px;
    font-variant-numeric: tabular-nums;
  }

  .focused-holder {
    display: grid;
    grid-template-columns: minmax(260px, 1.4fr) minmax(180px, 0.8fr) minmax(180px, 0.8fr);
    gap: 12px;
    padding: 10px 12px;
    border-top: 1px solid var(--border);
  }

  .focused-holder > div {
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 3px;
  }

  .focused-holder span {
    color: var(--muted);
    font-size: 10px;
  }

  .focused-holder a,
  .focused-holder strong {
    overflow: hidden;
    color: var(--text);
    font-size: 11px;
    text-decoration: none;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .empty-history {
    margin: 0;
    padding: 28px 12px;
    color: var(--muted);
    text-align: center;
  }

  @media (max-width: 1050px) {
    .history-controls {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .history-layout {
      grid-template-columns: 1fr;
    }

    .chart-column {
      border-right: 0;
      border-bottom: 1px solid var(--border);
    }

    .history-legend {
      max-height: 300px;
    }
  }

  @media (max-width: 700px) {
    .history-title > div:first-child {
      display: block;
    }

    .history-controls {
      grid-template-columns: 1fr;
      padding: 9px;
    }

    .history-controls select,
    .history-search {
      height: 44px;
    }

    .chart-column {
      padding: 9px;
    }

    svg {
      min-width: 560px;
    }

    .date-scrubber {
      grid-template-columns: 1fr;
    }

    .date-scrubber div {
      flex-direction: row;
      justify-content: space-between;
      text-align: left;
    }

    .focused-holder {
      grid-template-columns: 1fr;
    }
  }
</style>
