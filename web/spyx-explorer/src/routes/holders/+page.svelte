<script lang="ts">
  import { afterNavigate, pushState, replaceState } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { page } from '$app/state';
  import { ArrowLeft, FileCheck2 } from '@lucide/svelte';
  import { SvelteSet } from 'svelte/reactivity';
  import AuthorityPortfolioExplorer from '$lib/components/AuthorityPortfolioExplorer.svelte';
  import BalanceDistribution from '$lib/components/BalanceDistribution.svelte';
  import HistoryChart from '$lib/components/HistoryChart.svelte';
  import HolderExplorer from '$lib/components/HolderExplorer.svelte';
  import MovementTransactions from '$lib/components/MovementTransactions.svelte';
  import TopHolderHistory from '$lib/components/TopHolderHistory.svelte';
  import { formatBaseUnits, formatInteger, formatPercentFromPpm } from '$lib/format';
  import {
    holderNavigationUrl,
    normalizeHolderNavigationState,
    parseHolderNavigationState,
    type HolderHistoryMode,
    type HolderNavigationOptions,
    type HolderNavigationState,
    type HolderView
  } from '$lib/holder-navigation';
  import type { AuthorityPortfolioTableReport } from '$lib/types';
  import type { PageProps } from './$types';

  let { data }: PageProps = $props();
  const report = $derived(data.report);
  const authorityPortfolios = $derived(data.authorityPortfolios);
  const navigationOptions = $derived(holderNavigationOptions(authorityPortfolios));
  let holderNavigation = $state.raw<HolderNavigationState>(initialHolderNavigation());
  const holderView = $derived(holderNavigation.view);
  const holderAuthority = $derived(report.final_public_balance.holder_authority ?? null);
  const concentration = $derived([
    { group: 'Largest owner', value: report.final_public_balance.top_1_concentration },
    { group: 'Largest 10 owners', value: report.final_public_balance.top_10_concentration },
    { group: 'Largest 100 owners', value: report.final_public_balance.top_100_concentration }
  ]);

  function initialHolderNavigation(): HolderNavigationState {
    return parseHolderNavigationState(page.url.searchParams, navigationOptions);
  }

  function holderNavigationOptions(
    portfolioReport: AuthorityPortfolioTableReport | null
  ): HolderNavigationOptions {
    const programIds = new SvelteSet<string>();
    for (const portfolio of portfolioReport?.portfolios ?? []) {
      for (const program of portfolio.programs_used) programIds.add(program.program_id);
    }
    for (const custody of portfolioReport?.protocol_custody ?? []) {
      if (custody.program_id) programIds.add(custody.program_id);
    }
    return {
      estimateAvailable: portfolioReport !== null,
      estimateProgramIds: programIds
    };
  }

  function updateHolderNavigation(
    requestedState: HolderNavigationState,
    historyMode: HolderHistoryMode
  ): void {
    const nextState = normalizeHolderNavigationState(requestedState, navigationOptions);
    const currentUrl = new URL(window.location.href);
    const nextUrl = holderNavigationUrl(currentUrl, nextState, navigationOptions);
    holderNavigation = nextState;
    if (nextUrl.href === currentUrl.href) return;
    const destination = resolve(`/holders${nextUrl.search}${nextUrl.hash}` as '/holders');
    if (historyMode === 'replace') {
      replaceState(destination, page.state);
    } else {
      pushState(destination, page.state);
    }
  }

  function selectHolderView(view: HolderView): void {
    updateHolderNavigation({ ...holderNavigation, view }, 'push');
  }

  function restoreHolderNavigation(): void {
    holderNavigation = parseHolderNavigationState(
      new URL(window.location.href).searchParams,
      navigationOptions
    );
  }

  afterNavigate((navigation) => {
    const destination = navigation.to?.url;
    if (!destination || destination.pathname !== resolve('/holders')) return;
    holderNavigation = parseHolderNavigationState(destination.searchParams, navigationOptions);
  });
</script>

<svelte:window onpopstate={restoreHolderNavigation} />

<svelte:head>
  <title>SPYx holders</title>
  <meta
    name="description"
    content="SPYx holder ranking, program links, balance distribution, and verified history."
  />
</svelte:head>

<header class="topbar">
  <div>
    <h1>Holders</h1>
    <div class="address">SPYx owners, token accounts, program links, and public balance history</div>
  </div>
  <div class="controls">
    <a class="toolbar-button" href={resolve('/')}>
      <ArrowLeft size={16} strokeWidth={1.8} />
      <span>Overview</span>
    </a>
    <a class="toolbar-button" href={resolve('/audit')}>
      <FileCheck2 size={16} strokeWidth={1.8} />
      <span>Integrity</span>
    </a>
  </div>
</header>

<section class="summary" aria-label="SPYx holder summary">
  <div class="summary-cell">
    <div class="label">On-chain owners with a balance</div>
    <div class="value">{formatInteger(report.final_public_balance.positive_public_balance_holders)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Open token accounts</div>
    <div class="value">{formatInteger(report.final_public_balance.active_public_token_accounts)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Verified token balance</div>
    <div class="value">{formatBaseUnits(report.final_public_balance.public_raw_balance_sum.base_units, 4)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Largest custody owner share</div>
    <div class="value">{formatPercentFromPpm(report.final_public_balance.top_1_concentration.supply_share_parts_per_million_floor)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Largest 100 custody share</div>
    <div class="value">{formatPercentFromPpm(report.final_public_balance.top_100_concentration.supply_share_parts_per_million_floor)}</div>
  </div>
</section>

<div class="holder-tabs" role="tablist" aria-label="Holder views">
  <button
    type="button"
    role="tab"
    aria-selected={holderView === 'estimate'}
    aria-controls="portfolio-view"
    class={['holder-tab', holderView === 'estimate' && 'active']}
    onclick={() => selectHolderView('estimate')}
  >True owner estimate</button>
  <button
    type="button"
    role="tab"
    aria-selected={holderView === 'chain'}
    aria-controls="account-view"
    class={['holder-tab', holderView === 'chain' && 'active']}
    onclick={() => selectHolderView('chain')}
  >On-chain custody</button>
</div>

{#if holderView === 'estimate'}
  <div id="portfolio-view" role="tabpanel">
    {#if authorityPortfolios}
      <AuthorityPortfolioExplorer
        report={authorityPortfolios}
        navigation={holderNavigation}
        onnavigationchange={updateHolderNavigation}
      />
    {:else}
      <section class="panel unavailable portfolio-unavailable">
        <h2>True owner estimates are not available</h2>
        <p>This report does not include the estimate data.</p>
      </section>
    {/if}
  </div>
{:else}
  <div id="account-view" role="tabpanel">
    {#if holderAuthority?.complete}
      <HolderExplorer authority={holderAuthority} />
    {:else}
      <section class="panel unavailable">
        <h2>Holder authority data is not available</h2>
        <p>The verified balance totals remain available, but account classification was not loaded.</p>
      </section>
    {/if}

    {#if report.final_top_100_holder_history}
      <TopHolderHistory
        history={report.final_top_100_holder_history}
        authority={holderAuthority}
        decimals={report.final_public_balance.decimals}
      />
    {/if}

    <div class="holder-grid">
      <section class="panel">
        <div class="panel-toolbar">
          <h2>Balance concentration</h2>
          <span class="panel-toolbar-detail">Share of the verified token balance</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Group</th><th class="numeric">Balance</th><th class="numeric">Share</th></tr></thead>
            <tbody>
              {#each concentration as row (row.group)}
                <tr>
                  <td>{row.group}</td>
                  <td class="numeric">{formatBaseUnits(row.value.amount.base_units, 2)}</td>
                  <td class="numeric">{formatPercentFromPpm(row.value.supply_share_parts_per_million_floor)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      </section>

      <BalanceDistribution rows={report.final_public_balance.balance_distribution} />
    </div>

    <section class="panel">
      <div class="panel-toolbar">
        <h2>Aggregate holder history</h2>
        <span class="panel-toolbar-detail">Daily owner count, concentration, total balance, and movement</span>
      </div>
      <HistoryChart rows={report.daily} />
    </section>

    <MovementTransactions rows={report.top_25_volume_transactions} />
  </div>
{/if}

<style>
  .holder-tabs {
    display: flex;
    gap: 18px;
    min-height: 42px;
    padding: 0 2px;
    border-bottom: 1px solid var(--border);
  }

  .holder-tab {
    position: relative;
    padding: 0 2px;
    border: 0;
    color: var(--muted);
    font-weight: 600;
    background: transparent;
  }

  .holder-tab:hover,
  .holder-tab.active {
    color: var(--text);
  }

  .holder-tab.active::after {
    position: absolute;
    right: 0;
    bottom: -1px;
    left: 0;
    height: 2px;
    background: var(--accent);
    content: '';
  }

  .holder-grid {
    display: grid;
    grid-template-columns: minmax(300px, 0.75fr) minmax(520px, 1.25fr);
    gap: 14px;
  }

  .unavailable {
    padding: 14px;
  }

  .unavailable p {
    margin: 6px 0 0;
    color: var(--muted);
  }

  .portfolio-unavailable {
    min-height: 92px;
  }

  @media (max-width: 1050px) {
    .holder-grid {
      grid-template-columns: 1fr;
    }
  }

  @media (max-width: 700px) {
    .holder-tabs {
      gap: 14px;
      overflow-x: auto;
    }

    .holder-tab {
      min-height: 44px;
      white-space: nowrap;
    }
  }
</style>
