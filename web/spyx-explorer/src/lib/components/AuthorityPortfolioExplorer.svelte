<script lang="ts">
  import { resolve } from '$app/paths';
  import { ArrowDown, ArrowUp, Search, SlidersHorizontal } from '@lucide/svelte';
  import SortableHeader from '$lib/components/SortableHeader.svelte';
  import { formatBaseUnits, formatInteger, shortAddress } from '$lib/format';
  import {
    defaultCustodyDirection,
    defaultPortfolioDirection,
    type EstimateAccountFilter,
    type EstimateCustodySort,
    type EstimatePortfolioSort,
    type HolderHistoryMode,
    type HolderNavigationState,
    type SortDirection
  } from '$lib/holder-navigation';
  import { programDisplayName, programOptionLabel } from '$lib/program-labels.js';
  import type {
    AuthorityPortfolioTableReport,
    AuthorityPortfolioTableRow,
    PortfolioAccountKind,
    ProtocolCustodyAllocation
  } from '$lib/types';

  interface Props {
    report: AuthorityPortfolioTableReport;
    navigation: HolderNavigationState;
    onnavigationchange: (
      state: HolderNavigationState,
      historyMode: HolderHistoryMode
    ) => void;
  }

  const accountOptions: Array<{ value: EstimateAccountFilter; label: string }> = [
    { value: 'all', label: 'All portfolio authorities' },
    { value: 'observed_transaction_signer', label: 'Signer wallets (observed)' },
    { value: 'other_on_curve_account', label: 'Other on-curve accounts' }
  ];
  const portfolioSortOptions: Array<{
    value: EstimatePortfolioSort;
    label: string;
    direction: SortDirection;
  }> = [
    { value: 'total', label: 'Total exposure', direction: 'desc' },
    { value: 'direct', label: 'Direct SPYx', direction: 'desc' },
    { value: 'claim', label: 'DeFi claim estimate', direction: 'desc' },
    { value: 'authority', label: 'Authority address', direction: 'asc' },
    { value: 'type', label: 'Account type', direction: 'asc' },
    { value: 'program', label: 'Program ID', direction: 'asc' }
  ];
  const pageSizes = [25, 50, 100];

  let { report, navigation, onnavigationchange }: Props = $props();

  const query = $derived(navigation.estimateQuery);
  const accountFilter = $derived(navigation.estimateAccountFilter);
  const programId = $derived(navigation.estimateProgramId);
  const portfolioSort = $derived(navigation.estimatePortfolioSort);
  const portfolioDirection = $derived(navigation.estimatePortfolioDirection);
  const portfolioPageSize = $derived(navigation.estimatePortfolioPageSize);
  const portfolioPageIndex = $derived(navigation.estimatePortfolioPageIndex);
  const custodySort = $derived(navigation.estimateCustodySort);
  const custodyDirection = $derived(navigation.estimateCustodyDirection);
  const custodyPageSize = $derived(navigation.estimateCustodyPageSize);
  const custodyPageIndex = $derived(navigation.estimateCustodyPageIndex);
  let filtersOpen = $state(false);

  const programOptions = $derived.by(() => {
    const namesById: Record<string, string | null> = Object.create(null);
    for (const portfolio of report.portfolios) {
      for (const program of portfolio.programs_used) {
        addProgramName(namesById, program.program_id, program.program_name);
      }
    }
    for (const custody of report.protocol_custody) {
      addProgramName(namesById, custody.program_id, custody.program_name);
    }
    return Object.keys(namesById)
      .sort((left, right) => left.localeCompare(right))
      .map((id) => ({ id, label: programOptionLabel(id, namesById[id]) }));
  });
  const activeFilterCount = $derived(
    Number(accountFilter !== 'all') + Number(programId !== 'all')
  );
  const rankedPortfolios = $derived(
    report.portfolios
      .slice()
      .sort((left, right) => comparePortfolios(left, right, portfolioSort, portfolioDirection))
  );
  const portfolioRankByAuthority = $derived(
    new Map(rankedPortfolios.map((portfolio, index) => [portfolio.authority, index + 1]))
  );
  const filteredPortfolios = $derived.by(() => {
    const term = query.trim().toLowerCase();
    return rankedPortfolios.filter((portfolio) => {
      if (accountFilter !== 'all' && portfolio.authority_kind !== accountFilter) return false;
      const programIds = portfolio.programs_used.map((program) => program.program_id);
      if (programId !== 'all' && !programIds.includes(programId)) return false;
      if (!term) return true;
      return [
        portfolio.authority,
        ...programIds,
        ...portfolio.custody_owners
      ]
        .join(' ')
        .toLowerCase()
        .includes(term);
    });
  });
  const portfolioPageCount = $derived(
    Math.max(1, Math.ceil(filteredPortfolios.length / portfolioPageSize))
  );
  const portfolioPage = $derived(Math.min(portfolioPageIndex, portfolioPageCount - 1));
  const visiblePortfolios = $derived(
    filteredPortfolios.slice(
      portfolioPage * portfolioPageSize,
      portfolioPage * portfolioPageSize + portfolioPageSize
    )
  );
  const portfolioFirst = $derived(
    filteredPortfolios.length === 0 ? 0 : portfolioPage * portfolioPageSize + 1
  );
  const portfolioLast = $derived(
    Math.min(filteredPortfolios.length, portfolioPage * portfolioPageSize + portfolioPageSize)
  );

  const filteredCustody = $derived.by(() => {
    const term = query.trim().toLowerCase();
    return report.protocol_custody.filter((row) => {
      if (programId !== 'all' && row.program_id !== programId) return false;
      if (!term) return true;
      return [row.custody_owner, row.program_id]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
        .includes(term);
    });
  });
  const sortedCustody = $derived(
    filteredCustody
      .slice()
      .sort((left, right) => compareCustody(left, right, custodySort, custodyDirection))
  );
  const custodyPageCount = $derived(
    Math.max(1, Math.ceil(sortedCustody.length / custodyPageSize))
  );
  const custodyPage = $derived(Math.min(custodyPageIndex, custodyPageCount - 1));
  const visibleCustody = $derived(
    sortedCustody.slice(
      custodyPage * custodyPageSize,
      custodyPage * custodyPageSize + custodyPageSize
    )
  );
  const custodyFirst = $derived(
    sortedCustody.length === 0 ? 0 : custodyPage * custodyPageSize + 1
  );
  const custodyLast = $derived(
    Math.min(sortedCustody.length, custodyPage * custodyPageSize + custodyPageSize)
  );

  function addProgramName(
    namesById: Record<string, string | null>,
    id: string | null,
    name: string | null | undefined
  ): void {
    if (!id) return;
    const normalized = typeof name === 'string' && name.trim() ? name.trim() : null;
    if (!(id in namesById) || (namesById[id] === null && normalized !== null)) {
      namesById[id] = normalized;
    }
  }

  function accountKindLabel(kind: PortfolioAccountKind): string {
    if (kind === 'observed_transaction_signer') return 'Observed signer';
    return 'Other on-curve';
  }

  function compareBigInt(left: string, right: string): number {
    const leftValue = BigInt(left);
    const rightValue = BigInt(right);
    return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
  }

  function primaryProgramId(portfolio: AuthorityPortfolioTableRow): string | null {
    let first: string | null = null;
    for (const program of portfolio.programs_used) {
      if (first === null || program.program_id.localeCompare(first) < 0) first = program.program_id;
    }
    return first;
  }

  function compareProgramIds(left: string | null, right: string | null): number {
    if (left === null && right !== null) return 1;
    if (left !== null && right === null) return -1;
    return (left ?? '').localeCompare(right ?? '');
  }

  function comparePortfolios(
    left: AuthorityPortfolioTableRow,
    right: AuthorityPortfolioTableRow,
    column: EstimatePortfolioSort,
    direction: SortDirection
  ): number {
    let result = 0;
    if (column === 'authority') result = left.authority.localeCompare(right.authority);
    if (column === 'type') result = left.authority_kind.localeCompare(right.authority_kind);
    if (column === 'program') {
      result = compareProgramIds(primaryProgramId(left), primaryProgramId(right));
      if (result !== 0) return direction === 'asc' ? result : -result;
    }
    if (column === 'direct') {
      result = compareBigInt(
        left.direct_public_balance.raw_amount,
        right.direct_public_balance.raw_amount
      );
    }
    if (column === 'claim') {
      result = compareBigInt(
        left.estimated_defi_claim.raw_amount,
        right.estimated_defi_claim.raw_amount
      );
    }
    if (column === 'total') {
      result = compareBigInt(
        left.estimated_total_exposure.raw_amount,
        right.estimated_total_exposure.raw_amount
      );
    }
    if (result === 0) result = left.authority.localeCompare(right.authority);
    return direction === 'asc' ? result : -result;
  }

  function compareCustody(
    left: ProtocolCustodyAllocation,
    right: ProtocolCustodyAllocation,
    column: EstimateCustodySort,
    direction: SortDirection
  ): number {
    let result = 0;
    if (column === 'owner') result = left.custody_owner.localeCompare(right.custody_owner);
    if (column === 'program') {
      result = compareProgramIds(left.program_id, right.program_id);
      if (result !== 0) return direction === 'asc' ? result : -result;
    }
    if (column === 'custody') {
      result = compareBigInt(
        left.direct_custody_balance.raw_amount,
        right.direct_custody_balance.raw_amount
      );
    }
    if (column === 'attributed') {
      result = compareBigInt(left.attributed_claim.raw_amount, right.attributed_claim.raw_amount);
    }
    if (column === 'unallocated') {
      result = compareBigInt(
        left.unallocated_custody.raw_amount,
        right.unallocated_custody.raw_amount
      );
    }
    if (column === 'excess') {
      result = compareBigInt(left.claim_excess.raw_amount, right.claim_excess.raw_amount);
    }
    if (column === 'authorities') {
      result = left.candidate_authority_count - right.candidate_authority_count;
    }
    if (result === 0) result = left.custody_owner.localeCompare(right.custody_owner);
    return direction === 'asc' ? result : -result;
  }

  function updateNavigation(
    changes: Partial<HolderNavigationState>,
    historyMode: HolderHistoryMode = 'push'
  ): void {
    onnavigationchange({ ...navigation, ...changes }, historyMode);
  }

  function changePortfolioSort(column: EstimatePortfolioSort): void {
    const direction =
      portfolioSort === column
        ? portfolioDirection === 'asc'
          ? 'desc'
          : 'asc'
        : defaultPortfolioDirection(column);
    updateNavigation({
      estimatePortfolioSort: column,
      estimatePortfolioDirection: direction,
      estimatePortfolioPageIndex: 0
    });
  }

  function changeCustodySort(column: EstimateCustodySort): void {
    const direction =
      custodySort === column
        ? custodyDirection === 'asc'
          ? 'desc'
          : 'asc'
        : defaultCustodyDirection(column);
    updateNavigation({
      estimateCustodySort: column,
      estimateCustodyDirection: direction,
      estimateCustodyPageIndex: 0
    });
  }

  function resetFilters(): void {
    updateNavigation({
      estimateQuery: '',
      estimateAccountFilter: 'all',
      estimateProgramId: 'all',
      estimatePortfolioPageIndex: 0,
      estimateCustodyPageIndex: 0
    });
  }
</script>

<section class="panel portfolio-panel">
  <div class="panel-toolbar portfolio-title-row">
    <div>
      <h2>True owner estimate</h2>
      <span class="panel-toolbar-detail">On-chain holdings and capped DeFi claims</span>
    </div>
    <span class="row-count">{formatInteger(filteredPortfolios.length)} authorities</span>
  </div>

  <div class="portfolio-search-toolbar">
    <label class="search-field">
      <Search size={17} strokeWidth={1.8} aria-hidden="true" />
      <input
        type="search"
        aria-label="Search portfolios and custody"
        placeholder="Search authority, custody, or program ID"
        maxlength="128"
        value={query}
        oninput={(event) =>
          updateNavigation(
            {
              estimateQuery: event.currentTarget.value,
              estimatePortfolioPageIndex: 0,
              estimateCustodyPageIndex: 0
            },
            'replace'
          )}
      />
    </label>
    <button
      type="button"
      class={['filter-button', (filtersOpen || activeFilterCount > 0) && 'active']}
      aria-expanded={filtersOpen}
      aria-label={`Filters${activeFilterCount > 0 ? `, ${activeFilterCount} active` : ''}`}
      onclick={() => (filtersOpen = !filtersOpen)}
    >
      <SlidersHorizontal size={16} strokeWidth={1.8} />
      <span>Filters{activeFilterCount > 0 ? ` (${activeFilterCount})` : ''}</span>
    </button>
  </div>

  {#if filtersOpen}
    <div class="advanced-filters">
      <label>
        <span>Authority type</span>
        <select
          value={accountFilter}
          onchange={(event) =>
            updateNavigation({
              estimateAccountFilter: event.currentTarget.value as EstimateAccountFilter,
              estimatePortfolioPageIndex: 0
            })}
        >
          {#each accountOptions as option (option.value)}
            <option value={option.value}>{option.label}</option>
          {/each}
        </select>
      </label>
      <label class="program-filter">
        <span>Program</span>
        <select
          value={programId}
          onchange={(event) =>
            updateNavigation({
              estimateProgramId: event.currentTarget.value,
              estimatePortfolioPageIndex: 0,
              estimateCustodyPageIndex: 0
            })}
        >
          <option value="all">All programs and unlinked authorities</option>
          {#each programOptions as program (program.id)}
            <option value={program.id}>{program.label}</option>
          {/each}
        </select>
      </label>
      <label>
        <span>Sort by</span>
        <select
          value={portfolioSort}
          onchange={(event) => {
            const sort = event.currentTarget.value as EstimatePortfolioSort;
            updateNavigation({
              estimatePortfolioSort: sort,
              estimatePortfolioDirection: defaultPortfolioDirection(sort),
              estimatePortfolioPageIndex: 0
            });
          }}
        >
          {#each portfolioSortOptions as option (option.value)}
            <option value={option.value}>{option.label}</option>
          {/each}
        </select>
      </label>
      <button
        type="button"
        class="direction-button"
        onclick={() =>
          updateNavigation({
            estimatePortfolioDirection: portfolioDirection === 'asc' ? 'desc' : 'asc',
            estimatePortfolioPageIndex: 0
          })}
        aria-label={`Sort ${portfolioDirection === 'asc' ? 'descending' : 'ascending'}`}
      >
        {#if portfolioDirection === 'asc'}
          <ArrowUp size={16} strokeWidth={2} />
          <span>Ascending</span>
        {:else}
          <ArrowDown size={16} strokeWidth={2} />
          <span>Descending</span>
        {/if}
      </button>
      {#if activeFilterCount > 0 || query}
        <button type="button" class="clear-button" onclick={resetFilters}>Clear</button>
      {/if}
    </div>
  {/if}

  <div class="method-line">
    Claims use committed net SPYx flows and are capped by current custody. Yield and debt are not
    included. PDA creation signers do not receive a balance.
  </div>

  <div class="table-wrap desktop-portfolio-table">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <SortableHeader
            label="Authority"
            active={portfolioSort === 'authority'}
            direction={portfolioDirection}
            onclick={() => changePortfolioSort('authority')}
          />
          <SortableHeader
            label="Type"
            active={portfolioSort === 'type'}
            direction={portfolioDirection}
            onclick={() => changePortfolioSort('type')}
          />
          <SortableHeader
            label="Programs"
            active={portfolioSort === 'program'}
            direction={portfolioDirection}
            onclick={() => changePortfolioSort('program')}
          />
          <SortableHeader
            label="Direct SPYx"
            numeric
            active={portfolioSort === 'direct'}
            direction={portfolioDirection}
            onclick={() => changePortfolioSort('direct')}
          />
          <SortableHeader
            label="DeFi claim estimate"
            numeric
            active={portfolioSort === 'claim'}
            direction={portfolioDirection}
            onclick={() => changePortfolioSort('claim')}
          />
          <SortableHeader
            label="Total exposure (est.)"
            numeric
            active={portfolioSort === 'total'}
            direction={portfolioDirection}
            onclick={() => changePortfolioSort('total')}
          />
        </tr>
      </thead>
      <tbody>
        {#each visiblePortfolios as portfolio (portfolio.authority)}
          <tr>
            <td class="muted">{portfolioRankByAuthority.get(portfolio.authority)}</td>
            <td>
              <a
                class="owner-link mono"
                href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(portfolio.authority)}`)}
                title={portfolio.authority}
              >{shortAddress(portfolio.authority)}</a>
            </td>
            <td>{accountKindLabel(portfolio.authority_kind)}</td>
            <td class="program-list-cell">
              {#each portfolio.programs_used.slice(0, 2) as program (program.program_id)}
                <a
                  href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(program.program_id)}`)}
                  title={program.program_id}
                >
                  <strong>{programDisplayName(program.program_name)}</strong>
                  <span>{shortAddress(program.program_id)}</span>
                </a>
              {:else}
                <span class="muted">—</span>
              {/each}
              {#if portfolio.programs_used.length > 2}
                <span class="more-programs">+{portfolio.programs_used.length - 2}</span>
              {/if}
            </td>
            <td class="numeric amount-cell">
              {formatBaseUnits(portfolio.direct_public_balance.base_units, 6)}
            </td>
            <td class="numeric amount-cell">
              {formatBaseUnits(portfolio.estimated_defi_claim.base_units, 6)}
            </td>
            <td class="numeric amount-cell total-amount">
              {formatBaseUnits(portfolio.estimated_total_exposure.base_units, 6)}
            </td>
          </tr>
        {:else}
          <tr><td class="empty-row" colspan="7">No portfolio matches this search.</td></tr>
        {/each}
      </tbody>
    </table>
  </div>

  <div class="mobile-portfolio-list" aria-label="True owner estimates">
    {#each visiblePortfolios as portfolio (portfolio.authority)}
      <article>
        <header>
          <span class="rank">#{portfolioRankByAuthority.get(portfolio.authority)}</span>
          <a
            class="mono"
            href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(portfolio.authority)}`)}
            title={portfolio.authority}
          >{shortAddress(portfolio.authority)}</a>
          <span>{accountKindLabel(portfolio.authority_kind)}</span>
        </header>
        <div class="mobile-total">
          <span>Total exposure (est.)</span>
          <strong>{formatBaseUnits(portfolio.estimated_total_exposure.base_units, 6)} SPYx</strong>
        </div>
        <dl>
          <div>
            <dt>Direct SPYx</dt>
            <dd>{formatBaseUnits(portfolio.direct_public_balance.base_units, 6)}</dd>
          </div>
          <div>
            <dt>DeFi claim estimate</dt>
            <dd>{formatBaseUnits(portfolio.estimated_defi_claim.base_units, 6)}</dd>
          </div>
        </dl>
        {#if portfolio.programs_used.length > 0}
          <div class="mobile-programs">
            {#each portfolio.programs_used.slice(0, 2) as program (program.program_id)}
              <a
                href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(program.program_id)}`)}
                title={program.program_id}
              >{programDisplayName(program.program_name)} · {shortAddress(program.program_id)}</a>
            {/each}
            {#if portfolio.programs_used.length > 2}
              <span>+{portfolio.programs_used.length - 2} programs</span>
            {/if}
          </div>
        {/if}
      </article>
    {:else}
      <p class="empty-row">No portfolio matches this search.</p>
    {/each}
  </div>

  <footer class="table-footer">
    <span>Rows {formatInteger(portfolioFirst)}–{formatInteger(portfolioLast)} of {formatInteger(filteredPortfolios.length)}</span>
    <div class="pagination">
      <label>
        <span>Rows</span>
        <select
          value={portfolioPageSize}
          onchange={(event) =>
            updateNavigation({
              estimatePortfolioPageSize: Number(event.currentTarget.value),
              estimatePortfolioPageIndex: 0
            })}
        >
          {#each pageSizes as size (size)}<option value={size}>{size}</option>{/each}
        </select>
      </label>
      <button
        type="button"
        disabled={portfolioPage === 0}
        onclick={() => updateNavigation({ estimatePortfolioPageIndex: portfolioPage - 1 })}
      >Previous</button>
      <span>{portfolioPage + 1} / {portfolioPageCount}</span>
      <button
        type="button"
        disabled={portfolioPage >= portfolioPageCount - 1}
        onclick={() => updateNavigation({ estimatePortfolioPageIndex: portfolioPage + 1 })}
      >Next</button>
    </div>
  </footer>
</section>

<section class="panel custody-panel">
  <div class="panel-toolbar custody-title-row">
    <div>
      <h2>PDA and program custody</h2>
      <span class="panel-toolbar-detail">Assigned estimate and amount left unallocated</span>
    </div>
    <span class="row-count">{formatInteger(filteredCustody.length)} custody rows</span>
  </div>

  <div class="table-wrap desktop-custody-table">
    <table>
      <thead>
        <tr>
          <SortableHeader
            label="Custody owner"
            active={custodySort === 'owner'}
            direction={custodyDirection}
            onclick={() => changeCustodySort('owner')}
          />
          <SortableHeader
            label="Program"
            active={custodySort === 'program'}
            direction={custodyDirection}
            onclick={() => changeCustodySort('program')}
          />
          <SortableHeader
            label="Custody balance"
            numeric
            active={custodySort === 'custody'}
            direction={custodyDirection}
            onclick={() => changeCustodySort('custody')}
          />
          <SortableHeader
            label="Assigned estimate"
            numeric
            active={custodySort === 'attributed'}
            direction={custodyDirection}
            onclick={() => changeCustodySort('attributed')}
          />
          <SortableHeader
            label="Unallocated"
            numeric
            active={custodySort === 'unallocated'}
            direction={custodyDirection}
            onclick={() => changeCustodySort('unallocated')}
          />
          <SortableHeader
            label="Estimate over custody"
            numeric
            active={custodySort === 'excess'}
            direction={custodyDirection}
            onclick={() => changeCustodySort('excess')}
          />
          <SortableHeader
            label="Authorities"
            numeric
            active={custodySort === 'authorities'}
            direction={custodyDirection}
            onclick={() => changeCustodySort('authorities')}
          />
        </tr>
      </thead>
      <tbody>
        {#each visibleCustody as row (row.custody_owner)}
          <tr>
            <td>
              <a
                class="owner-link mono"
                href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(row.custody_owner)}`)}
                title={row.custody_owner}
              >{shortAddress(row.custody_owner)}</a>
            </td>
            <td class="program-list-cell">
              {#if row.program_id}
                <a
                  href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(row.program_id)}`)}
                  title={row.program_id}
                >
                  <strong>{programDisplayName(row.program_name)}</strong>
                  <span>{shortAddress(row.program_id)}</span>
                </a>
              {:else}
                <span class="muted">Program not linked</span>
              {/if}
            </td>
            <td class="numeric amount-cell">{formatBaseUnits(row.direct_custody_balance.base_units, 6)}</td>
            <td class="numeric amount-cell">{formatBaseUnits(row.attributed_claim.base_units, 6)}</td>
            <td class="numeric amount-cell">{formatBaseUnits(row.unallocated_custody.base_units, 6)}</td>
            <td class="numeric amount-cell">{formatBaseUnits(row.claim_excess.base_units, 6)}</td>
            <td class="numeric">{formatInteger(row.candidate_authority_count)}</td>
          </tr>
        {:else}
          <tr><td class="empty-row" colspan="7">No custody row matches this search.</td></tr>
        {/each}
      </tbody>
    </table>
  </div>

  <div class="mobile-custody-list" aria-label="PDA and program custody">
    {#each visibleCustody as row (row.custody_owner)}
      <article>
        <header>
          <a
            class="mono"
            href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(row.custody_owner)}`)}
            title={row.custody_owner}
          >{shortAddress(row.custody_owner)}</a>
          <strong>{formatBaseUnits(row.unallocated_custody.base_units, 6)} SPYx</strong>
        </header>
        {#if row.program_id}
          <a
            class="mobile-custody-program"
            href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(row.program_id)}`)}
            title={row.program_id}
          >{programDisplayName(row.program_name)} · {shortAddress(row.program_id)}</a>
        {:else}
          <span class="mobile-custody-program muted">Program not linked</span>
        {/if}
        <dl>
          <div><dt>Custody balance</dt><dd>{formatBaseUnits(row.direct_custody_balance.base_units, 6)}</dd></div>
          <div><dt>Assigned estimate</dt><dd>{formatBaseUnits(row.attributed_claim.base_units, 6)}</dd></div>
          <div><dt>Estimate over custody</dt><dd>{formatBaseUnits(row.claim_excess.base_units, 6)}</dd></div>
          <div><dt>Authorities</dt><dd>{formatInteger(row.candidate_authority_count)}</dd></div>
        </dl>
      </article>
    {:else}
      <p class="empty-row">No custody row matches this search.</p>
    {/each}
  </div>

  <footer class="table-footer">
    <span>Rows {formatInteger(custodyFirst)}–{formatInteger(custodyLast)} of {formatInteger(sortedCustody.length)}</span>
    <div class="pagination">
      <label>
        <span>Rows</span>
        <select
          value={custodyPageSize}
          onchange={(event) =>
            updateNavigation({
              estimateCustodyPageSize: Number(event.currentTarget.value),
              estimateCustodyPageIndex: 0
            })}
        >
          {#each pageSizes as size (size)}<option value={size}>{size}</option>{/each}
        </select>
      </label>
      <button
        type="button"
        disabled={custodyPage === 0}
        onclick={() => updateNavigation({ estimateCustodyPageIndex: custodyPage - 1 })}
      >Previous</button>
      <span>{custodyPage + 1} / {custodyPageCount}</span>
      <button
        type="button"
        disabled={custodyPage >= custodyPageCount - 1}
        onclick={() => updateNavigation({ estimateCustodyPageIndex: custodyPage + 1 })}
      >Next</button>
    </div>
  </footer>
</section>

<style>
  .portfolio-title-row > div:first-child,
  .custody-title-row > div:first-child {
    display: flex;
    align-items: baseline;
    gap: 10px;
  }

  .row-count {
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .portfolio-search-toolbar {
    display: grid;
    grid-template-columns: minmax(240px, 620px) auto;
    gap: 8px;
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
  }

  .search-field {
    min-width: 0;
    height: 36px;
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 0 10px;
    border: 1px solid var(--border);
    border-radius: 7px;
    color: var(--muted);
    background: var(--surface);
  }

  .search-field:focus-within {
    border-color: var(--accent);
    outline: 2px solid color-mix(in srgb, var(--accent) 18%, transparent);
  }

  .search-field input {
    min-width: 0;
    width: 100%;
    border: 0;
    outline: 0;
    color: var(--text);
    background: transparent;
  }

  .filter-button,
  .direction-button,
  .clear-button,
  .pagination button {
    min-height: 34px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 6px;
    padding: 0 10px;
    border: 1px solid var(--border);
    border-radius: 7px;
    color: var(--text);
    background: var(--surface);
  }

  .filter-button.active {
    border-color: #91c9c1;
    background: var(--accent-weak);
  }

  .filter-button:hover,
  .direction-button:hover,
  .clear-button:hover,
  .pagination button:not(:disabled):hover {
    border-color: var(--border-strong);
    background: var(--surface-muted);
  }

  .advanced-filters {
    display: grid;
    grid-template-columns: minmax(190px, 0.9fr) minmax(280px, 1.5fr) minmax(160px, 0.8fr) auto auto;
    align-items: end;
    gap: 8px;
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
    background: #fbfbfc;
  }

  .advanced-filters label,
  .pagination label {
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 3px;
    color: var(--muted);
    font-size: 11px;
  }

  .advanced-filters select,
  .pagination select {
    width: 100%;
    height: 34px;
    padding: 0 28px 0 8px;
    border: 1px solid var(--border);
    border-radius: 7px;
    color: var(--text);
    background: var(--surface);
  }

  .method-line {
    padding: 7px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--muted);
    font-size: 11px;
  }

  .program-list-cell {
    min-width: 175px;
    max-width: 250px;
  }

  .program-list-cell a {
    display: block;
    color: var(--text);
    text-decoration: none;
  }

  .program-list-cell a + a {
    margin-top: 5px;
  }

  .program-list-cell strong,
  .program-list-cell span {
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .program-list-cell strong {
    font-weight: 600;
  }

  .program-list-cell a span,
  .more-programs {
    margin-top: 1px;
    color: var(--muted);
    font-size: 10px;
  }

  .amount-cell {
    font-variant-numeric: tabular-nums;
  }

  .total-amount {
    font-weight: 650;
  }

  .empty-row {
    padding: 24px 12px;
    color: var(--muted);
    text-align: center;
  }

  .mobile-portfolio-list,
  .mobile-custody-list {
    display: none;
  }

  .table-footer {
    min-height: 48px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 7px 12px;
    border-top: 1px solid var(--border);
    color: var(--muted);
    font-size: 12px;
  }

  .pagination {
    display: flex;
    align-items: center;
    gap: 7px;
  }

  .pagination label {
    flex-direction: row;
    align-items: center;
  }

  .pagination select {
    width: 68px;
  }

  .pagination button:disabled {
    cursor: default;
    opacity: 0.45;
  }

  @media (max-width: 980px) {
    .advanced-filters {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .program-filter {
      grid-column: 1 / -1;
    }

    .direction-button,
    .clear-button {
      min-height: 40px;
    }
  }

  @media (max-width: 700px) {
    .portfolio-title-row > div:first-child,
    .custody-title-row > div:first-child {
      display: block;
    }

    .portfolio-search-toolbar {
      grid-template-columns: minmax(0, 1fr) auto;
      padding: 9px;
    }

    .filter-button {
      min-width: 44px;
      min-height: 44px;
    }

    .filter-button span {
      display: none;
    }

    .search-field {
      height: 44px;
    }

    .advanced-filters {
      grid-template-columns: 1fr;
      padding: 9px;
    }

    .advanced-filters label,
    .program-filter {
      grid-column: auto;
    }

    .advanced-filters select,
    .direction-button,
    .clear-button {
      min-height: 44px;
    }

    .desktop-portfolio-table,
    .desktop-custody-table {
      display: none;
    }

    .mobile-portfolio-list,
    .mobile-custody-list {
      display: grid;
      gap: 8px;
      padding: 9px;
      background: var(--surface-muted);
    }

    .mobile-portfolio-list article,
    .mobile-custody-list article {
      padding: 10px;
      border: 1px solid var(--border);
      border-radius: 7px;
      background: var(--surface);
    }

    .mobile-portfolio-list article header {
      display: grid;
      grid-template-columns: auto minmax(0, 1fr) auto;
      align-items: center;
      gap: 8px;
    }

    .mobile-portfolio-list article header a,
    .mobile-custody-list article header a {
      min-width: 0;
      overflow: hidden;
      color: var(--text);
      text-decoration: none;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .mobile-portfolio-list article header > span:last-child,
    .rank {
      color: var(--muted);
      font-size: 10px;
    }

    .mobile-total {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px solid var(--border);
    }

    .mobile-total span {
      color: var(--muted);
      font-size: 10px;
    }

    .mobile-total strong {
      font-size: 13px;
      font-variant-numeric: tabular-nums;
    }

    .mobile-portfolio-list dl,
    .mobile-custody-list dl {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 12px;
      margin: 9px 0 0;
    }

    .mobile-portfolio-list dt,
    .mobile-custody-list dt {
      font-size: 10px;
    }

    .mobile-portfolio-list dd,
    .mobile-custody-list dd {
      margin: 2px 0 0;
      font-size: 12px;
      font-variant-numeric: tabular-nums;
      font-weight: 600;
      overflow-wrap: anywhere;
    }

    .mobile-programs,
    .mobile-custody-program {
      display: grid;
      gap: 4px;
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px solid var(--border);
      font-size: 11px;
    }

    .mobile-programs a,
    .mobile-custody-program {
      color: var(--text);
      text-decoration: none;
      overflow-wrap: anywhere;
    }

    .mobile-programs span {
      color: var(--muted);
    }

    .mobile-custody-list article header {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: center;
      gap: 10px;
    }

    .mobile-custody-list article header strong {
      font-size: 12px;
      font-variant-numeric: tabular-nums;
    }

    .table-footer {
      align-items: flex-start;
      flex-direction: column;
    }

    .pagination {
      width: 100%;
      justify-content: space-between;
    }

    .pagination button {
      min-height: 44px;
    }
  }
</style>
