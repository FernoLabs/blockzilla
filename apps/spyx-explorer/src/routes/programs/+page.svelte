<script lang="ts">
  import { afterNavigate, goto, replaceState } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { page } from '$app/state';
  import { ArrowLeft, Search } from '@lucide/svelte';
  import { formatInteger } from '$lib/format';
  import { programDisplayName } from '$lib/program-labels.js';
  import type { ProgramRow } from '$lib/types';
  import type { PageProps } from './$types';

  type InstructionScope = 'all' | 'direct' | 'inner';
  type ProgramView = 'all_instructions' | 'target_cpi';

  interface ProgramUrlState {
    query: string;
    programView: ProgramView;
    instructionScope: InstructionScope;
    rowLimit: number;
  }

  let { data }: PageProps = $props();
  const report = $derived(data.programReport);
  const rowStep = 25;
  const routePath = page.url.pathname;
  const initialUrlState = readProgramUrlState(page.url);
  let query = $state(initialUrlState.query);
  let programView = $state<ProgramView>(initialUrlState.programView);
  let instructionScope = $state<InstructionScope>(initialUrlState.instructionScope);
  let rowLimit = $state(initialUrlState.rowLimit);
  const scopedPrograms = $derived.by(() => {
    const rows = report.programs.filter((program) => selectedOccurrences(program) > 0);
    return [...rows].sort(
      (left, right) =>
        selectedOccurrences(right) - selectedOccurrences(left) ||
        left.program_id.localeCompare(right.program_id)
    );
  });
  const filteredPrograms = $derived.by(() => {
    const term = query.trim().toLowerCase();
    return scopedPrograms.filter((program) => {
      if (!term) return true;
      return program.program_id.toLowerCase().includes(term);
    });
  });
  const rankByProgramId = $derived(
    new Map(scopedPrograms.map((program, index) => [program.program_id, index + 1]))
  );
  const visiblePrograms = $derived(filteredPrograms.slice(0, rowLimit));
  const scopeMetrics = $derived.by(() => {
    let occurrences = 0;
    let identifiedOccurrences = 0;
    let decoderOccurrences = 0;
    let identifiedPrograms = 0;
    let decoderPrograms = 0;
    for (const program of scopedPrograms) {
      const count = selectedOccurrences(program);
      occurrences += count;
      if (program.identity_status === 'identified') {
        identifiedPrograms += 1;
        identifiedOccurrences += count;
      }
      if (program.decoder_source_found) {
        decoderPrograms += 1;
        decoderOccurrences += count;
      }
    }
    return {
      programs: scopedPrograms.length,
      identifiedPrograms,
      decoderPrograms,
      occurrences,
      identifiedOccurrenceRatio: occurrences === 0 ? 0 : identifiedOccurrences / occurrences,
      decoderOccurrenceRatio: occurrences === 0 ? 0 : decoderOccurrences / occurrences
    };
  });
  const sourceRows = $derived(
    Object.entries(report.source_match_counts).sort((left, right) => right[1] - left[1])
  );
  const searchInstructionScope = $derived(
    programView === 'target_cpi' ? 'inner' : instructionScope
  );

  afterNavigate((navigation) => {
    const destination = navigation.to?.url;
    if (!destination || destination.pathname !== routePath) return;
    applyProgramUrlState(readProgramUrlState(destination));
  });

  function readProgramUrlState(url: URL): ProgramUrlState {
    const view = url.searchParams.get('view');
    const scope = url.searchParams.get('scope');
    const rows = Number(url.searchParams.get('rows'));
    return {
      query: url.searchParams.get('q') ?? '',
      programView: view === 'cpi' ? 'target_cpi' : 'all_instructions',
      instructionScope: scope === 'direct' || scope === 'inner' ? scope : 'all',
      rowLimit:
        Number.isSafeInteger(rows) && rows >= rowStep && rows <= 5_000
          ? Math.ceil(rows / rowStep) * rowStep
          : rowStep
    };
  }

  function applyProgramUrlState(state: ProgramUrlState): void {
    query = state.query;
    programView = state.programView;
    instructionScope = state.instructionScope;
    rowLimit = state.rowLimit;
  }

  function programStateUrl(): URL {
    const url = new URL(page.url);
    for (const name of ['q', 'view', 'scope', 'rows']) url.searchParams.delete(name);
    const trimmedQuery = query.trim();
    if (trimmedQuery) url.searchParams.set('q', trimmedQuery);
    if (programView === 'target_cpi') {
      url.searchParams.set('view', 'cpi');
    } else if (instructionScope !== 'all') {
      url.searchParams.set('scope', instructionScope);
    }
    if (rowLimit !== rowStep) url.searchParams.set('rows', String(rowLimit));
    return url;
  }

  function commitProgramState(replace = false): void {
    const url = programStateUrl();
    if (url.href === page.url.href) return;
    const destination = `/programs${url.search}${url.hash}` as '/programs';
    if (replace) {
      replaceState(resolve(destination), page.state);
      return;
    }
    void goto(resolve(destination), { keepFocus: true, noScroll: true });
  }

  function selectedOccurrences(program: ProgramRow): number {
    if (programView === 'target_cpi') return program.target_account_inner_occurrences;
    if (instructionScope === 'direct') return program.outer_occurrences;
    if (instructionScope === 'inner') return program.inner_occurrences;
    return program.total_occurrences;
  }

  function instructionScopeLabel(scope: InstructionScope): string {
    if (scope === 'direct') return 'Direct (top-level)';
    if (scope === 'inner') return 'Inner (CPI)';
    return 'Direct or inner';
  }

  function selectedScopeLabel(): string {
    return programView === 'target_cpi'
      ? 'SPYx account CPI'
      : instructionScopeLabel(instructionScope);
  }

  function changeProgramView(view: ProgramView): void {
    if (programView === view) return;
    programView = view;
    instructionScope = 'all';
    query = '';
    rowLimit = rowStep;
    commitProgramState();
  }

  function changeInstructionScope(scope: InstructionScope): void {
    if (instructionScope === scope) return;
    instructionScope = scope;
    rowLimit = rowStep;
    commitProgramState();
  }

  function changeQuery(value: string): void {
    query = value;
    rowLimit = rowStep;
    commitProgramState(true);
  }

  function showMorePrograms(): void {
    rowLimit += rowStep;
    commitProgramState();
  }

  function formatRatio(value: number): string {
    return `${(value * 100).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 3 })}%`;
  }

  function sourceLabel(value: string | null): string {
    return value ? value.replaceAll('_', ' ') : '—';
  }

  function programLinkTitle(programId: string): string {
    if (programView === 'target_cpi') {
      return `View all inner transactions for program ${programId}`;
    }
    return `View ${instructionScopeLabel(instructionScope).toLowerCase()} transactions for program ${programId}`;
  }

</script>

<svelte:head>
  <title>SPYx programs</title>
  <meta
    name="description"
    content="Exact SPYx program identity and decoder-source coverage for all outer and inner instruction occurrences."
  />
</svelte:head>

<header class="topbar">
  <div>
    <h1>Programs</h1>
    <div class="address">{formatInteger(scopeMetrics.programs)} programs in {selectedScopeLabel().toLowerCase()} · epochs {report.source.first_epoch}–{report.source.last_epoch}</div>
  </div>
  <div class="controls">
    <a class="toolbar-button" href={resolve('/')}>
      <ArrowLeft size={16} strokeWidth={1.8} />
      <span>Overview</span>
    </a>
  </div>
</header>

<section class="summary" aria-label="Program identification summary">
  <div class="summary-cell">
    <div class="label">Programs in scope</div>
    <div class="value">{formatInteger(scopeMetrics.programs)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Identified programs</div>
    <div class="value">{formatInteger(scopeMetrics.identifiedPrograms)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Programs with a decoder source</div>
    <div class="value">{formatInteger(scopeMetrics.decoderPrograms)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Identified instruction occurrences</div>
    <div class="value">{formatRatio(scopeMetrics.identifiedOccurrenceRatio)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Decoder-source occurrences</div>
    <div class="value">{formatRatio(scopeMetrics.decoderOccurrenceRatio)}</div>
  </div>
</section>

<div class="two-column programs-grid">
  <section class="panel">
    <div class="program-tabs" role="tablist" aria-label="Program sets">
      <button
        type="button"
        role="tab"
        aria-selected={programView === 'all_instructions'}
        class={['program-tab', programView === 'all_instructions' && 'active']}
        onclick={() => changeProgramView('all_instructions')}
      >All instructions</button>
      <button
        type="button"
        role="tab"
        aria-selected={programView === 'target_cpi'}
        class={['program-tab', programView === 'target_cpi' && 'active']}
        onclick={() => changeProgramView('target_cpi')}
      >SPYx account CPI</button>
    </div>
    <p class="scope-note">
      {programView === 'target_cpi'
        ? 'Each CPI row names the SPYx mint or a discovered SPYx token account in that inner instruction.'
        : 'This view includes every direct and inner instruction in the selected SPYx transactions.'}
      Names add display text only.
      {#if programView === 'target_cpi'} Program links open the broader list of all inner transactions.{/if}
    </p>
    <div class="program-toolbar">
      <div class="program-controls">
        {#if programView === 'all_instructions'}
          <label class="program-scope">
            <span>Instruction scope</span>
            <select
              value={instructionScope}
              onchange={(event) =>
                changeInstructionScope(event.currentTarget.value as InstructionScope)}
            >
              <option value="all">Direct or inner</option>
              <option value="direct">Direct (top-level)</option>
              <option value="inner">Inner (CPI)</option>
            </select>
          </label>
        {/if}
        <div class="program-search">
          <Search size={16} strokeWidth={1.8} />
          <input
            aria-label="Search programs"
            placeholder="Search program ID"
            value={query}
            oninput={(event) => changeQuery(event.currentTarget.value)}
          />
        </div>
      </div>
      <span class="muted">
        Showing {formatInteger(visiblePrograms.length)} of {formatInteger(filteredPrograms.length)} matches
      </span>
    </div>
    <div class="table-wrap desktop-programs">
      <table>
        <thead>
          {#if programView === 'target_cpi'}
            <tr>
              <th>#</th>
              <th>Program</th>
              <th>Decoder source</th>
              <th class="numeric">Linked CPI calls</th>
              <th class="numeric">Transactions</th>
              <th class="numeric" title="CPI calls that name the SPYx mint">Mint calls</th>
              <th class="numeric" title="CPI calls that name a discovered SPYx token account">Token-account calls</th>
              <th class="numeric">All CPI calls</th>
            </tr>
          {:else}
            <tr>
              <th>#</th>
              <th>Program</th>
              <th>Identity</th>
              <th>Decoder source</th>
              <th class="numeric">Selected occurrences</th>
              <th class="numeric">Direct</th>
              <th class="numeric">Inner</th>
              <th class="numeric">All transactions</th>
            </tr>
          {/if}
        </thead>
        <tbody>
          {#each visiblePrograms as program (program.program_id)}
            <tr>
              <td class="muted">{rankByProgramId.get(program.program_id)}</td>
              <td class="program-cell">
                <strong>{programDisplayName(program.selected_name)}</strong>
                <a
                  class="mono"
                  href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(program.program_id)}&instruction_scope=${searchInstructionScope}`)}
                  title={programLinkTitle(program.program_id)}
                >
                  {program.program_id}
                </a>
              </td>
              {#if programView === 'all_instructions'}
                <td>
                  <span class={['status-value', program.identity_status === 'identified' ? 'pass' : 'neutral']}>
                    {program.identity_status === 'identified' ? 'Identified' : 'Unidentified'}
                  </span>
                  {#if program.selected_source}
                    <span class="cell-note">{sourceLabel(program.selected_source)}</span>
                  {/if}
                </td>
              {/if}
              <td>
                <span class={['status-value', program.decoder_source_found ? 'pass' : 'neutral']}>
                  {program.decoder_source_found ? 'Source found' : 'No source'}
                </span>
              </td>
              {#if programView === 'target_cpi'}
                <td class="numeric">{formatInteger(program.target_account_inner_occurrences)}</td>
                <td class="numeric">{formatInteger(program.target_account_inner_transactions)}</td>
                <td class="numeric">{formatInteger(program.target_mint_inner_occurrences)}</td>
                <td class="numeric">{formatInteger(program.target_token_account_inner_occurrences)}</td>
                <td class="numeric">{formatInteger(program.inner_occurrences)}</td>
              {:else}
                <td class="numeric">{formatInteger(selectedOccurrences(program))}</td>
                <td class="numeric">{formatInteger(program.outer_occurrences)}</td>
                <td class="numeric">{formatInteger(program.inner_occurrences)}</td>
                <td class="numeric">{formatInteger(program.transactions)}</td>
              {/if}
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    <div class="mobile-programs" aria-label="Programs">
      {#each visiblePrograms as program (program.program_id)}
        <article class="program-card">
          <header>
            <span class="muted">#{rankByProgramId.get(program.program_id)}</span>
            <strong>{programDisplayName(program.selected_name)}</strong>
          </header>
          <a
            class="mono program-address"
            href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(program.program_id)}&instruction_scope=${searchInstructionScope}`)}
            title={programLinkTitle(program.program_id)}
          >{program.program_id}</a>
          <dl>
            <div><dt>Label</dt><dd>{program.identity_status === 'identified' ? 'Identified' : 'Unidentified'}</dd></div>
            <div><dt>Decoder</dt><dd>{program.decoder_source_found ? 'Source found' : 'No source'}</dd></div>
            {#if programView === 'target_cpi'}
              <div><dt>Linked CPI</dt><dd>{formatInteger(program.target_account_inner_occurrences)}</dd></div>
              <div><dt>Transactions</dt><dd>{formatInteger(program.target_account_inner_transactions)}</dd></div>
              <div><dt>Mint calls</dt><dd>{formatInteger(program.target_mint_inner_occurrences)}</dd></div>
              <div><dt>Token-account calls</dt><dd>{formatInteger(program.target_token_account_inner_occurrences)}</dd></div>
              <div><dt>All CPI</dt><dd>{formatInteger(program.inner_occurrences)}</dd></div>
            {:else}
              <div><dt>Selected</dt><dd>{formatInteger(selectedOccurrences(program))}</dd></div>
              <div><dt>Direct</dt><dd>{formatInteger(program.outer_occurrences)}</dd></div>
              <div><dt>Inner</dt><dd>{formatInteger(program.inner_occurrences)}</dd></div>
              <div><dt>All transactions</dt><dd>{formatInteger(program.transactions)}</dd></div>
            {/if}
          </dl>
          {#if program.selected_source}
            <p>Source: {sourceLabel(program.selected_source)}</p>
          {/if}
        </article>
      {/each}
    </div>
    {#if visiblePrograms.length < filteredPrograms.length}
      <div class="load-more">
        <button type="button" onclick={showMorePrograms}>Show {rowStep} more</button>
      </div>
    {/if}
  </section>

  <section class="panel source-panel">
    <div class="panel-toolbar">
      <h2>Label and decoder sources</h2>
      <span class="panel-toolbar-detail">A program can match more than one source</span>
    </div>
    <div class="table-wrap">
      <table>
        <tbody>
          {#each sourceRows as row (row[0])}
            <tr>
              <th>{sourceLabel(row[0])}</th>
              <td class="numeric">{formatInteger(row[1])}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  </section>
</div>

<style>
  .programs-grid {
    grid-template-columns: minmax(0, 1fr) 330px;
  }

  .program-toolbar {
    min-height: 50px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 8px 12px;
    border-bottom: 1px solid var(--border);
  }

  .program-tabs {
    display: flex;
    gap: 18px;
    min-height: 40px;
    padding: 0 12px;
    border-bottom: 1px solid var(--border);
  }

  .program-tab {
    position: relative;
    padding: 0 2px;
    border: 0;
    color: var(--muted);
    font-weight: 600;
    background: transparent;
  }

  .program-tab:hover,
  .program-tab.active {
    color: var(--text);
  }

  .program-tab.active::after {
    position: absolute;
    right: 0;
    bottom: -1px;
    left: 0;
    height: 2px;
    background: var(--accent);
    content: '';
  }

  .scope-note {
    margin: 0;
    padding: 9px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--muted);
    font-size: 12px;
  }

  .program-controls {
    min-width: 0;
    display: flex;
    align-items: end;
    gap: 10px;
    flex: 1;
  }

  .program-scope {
    display: grid;
    gap: 4px;
    color: var(--muted);
    font-size: 11px;
  }

  .program-scope select {
    height: 34px;
    padding: 0 28px 0 9px;
    border: 1px solid var(--border);
    border-radius: 7px;
    color: var(--text);
    background: var(--surface);
  }

  .program-search {
    position: relative;
    width: min(420px, 100%);
  }

  .program-search :global(svg) {
    position: absolute;
    top: 9px;
    left: 9px;
    color: var(--faint);
    pointer-events: none;
  }

  .program-search input {
    width: 100%;
    height: 34px;
    padding: 0 10px 0 32px;
    border: 1px solid var(--border);
    border-radius: 7px;
    color: var(--text);
    background: var(--surface);
  }

  .program-search input:focus {
    outline: 2px solid #b7ded8;
    outline-offset: 1px;
    border-color: var(--accent);
  }

  .program-cell {
    min-width: 190px;
  }

  .program-cell strong,
  .program-cell a,
  .cell-note {
    display: block;
  }

  .program-cell a {
    max-width: 390px;
    white-space: normal;
    overflow-wrap: anywhere;
  }

  .program-cell a,
  .cell-note {
    margin-top: 2px;
    color: var(--muted);
    font-size: 11px;
  }

  .program-cell a:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }

  .status-value.neutral {
    color: var(--muted);
    background: var(--surface-muted);
  }

  .mobile-programs {
    display: none;
  }

  .load-more {
    padding: 10px 12px;
    border-top: 1px solid var(--border);
    text-align: center;
  }

  .load-more button {
    height: 32px;
    padding: 0 10px;
    border: 1px solid var(--border);
    border-radius: 7px;
    color: var(--text);
    background: var(--surface);
  }

  .load-more button:hover {
    border-color: var(--border-strong);
    background: var(--surface-muted);
  }

  @media (max-width: 1480px) {
    .programs-grid {
      grid-template-columns: 1fr;
    }
  }

  @media (max-width: 640px) {
    .program-toolbar {
      align-items: stretch;
      flex-direction: column;
    }

    .program-search input,
    .program-scope select,
    .load-more button {
      min-height: 44px;
    }

    .program-controls {
      align-items: stretch;
      flex-direction: column;
    }

    .program-scope select {
      width: 100%;
    }

    .desktop-programs {
      display: none;
    }

    .mobile-programs {
      display: block;
    }

    .program-card {
      padding: 11px 12px;
      border-bottom: 1px solid var(--border);
    }

    .program-card:last-child {
      border-bottom: 0;
    }

    .program-card header {
      display: grid;
      grid-template-columns: auto minmax(0, 1fr);
      gap: 8px;
    }

    .program-address {
      display: block;
      margin: 4px 0 8px;
      color: var(--muted);
      font-size: 11px;
      overflow-wrap: anywhere;
      text-decoration: none;
    }

    .program-card dl {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 5px 12px;
      margin: 0;
    }

    .program-card dl > div {
      display: flex;
      justify-content: space-between;
      gap: 8px;
    }

    .program-card dd {
      margin: 0;
      font-variant-numeric: tabular-nums;
    }

    .program-card p {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 11px;
    }
  }
</style>
