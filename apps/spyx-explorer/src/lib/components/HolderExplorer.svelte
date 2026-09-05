<script lang="ts">
  import { afterNavigate, goto, replaceState } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { page } from '$app/state';
  import { ArrowDown, ArrowUp, Search, SlidersHorizontal } from '@lucide/svelte';
  import SortableHeader from '$lib/components/SortableHeader.svelte';
  import { formatBaseUnits, formatInteger, formatRawAmount, shortAddress } from '$lib/format';
  import { buildProgramOptions, programDisplayName } from '$lib/program-labels.js';
  import { buildCustodyProgramHoldings } from '$lib/program-holdings.js';
  import type {
    ClassifiedPublicBalanceOwner,
    HolderAuthorityKind,
    HolderAuthorityReport,
    SupplementalProgramAttribution
  } from '$lib/types';

  type HolderTypeFilter = 'all' | 'observed_signer' | 'pda_or_program' | 'other_on_curve';
  type CustodyView = 'owners' | 'programs';
  type SortColumn =
    | 'owner'
    | 'type'
    | 'program'
    | 'accounts'
    | 'balance'
    | 'activity'
    | 'transactions';
  type SortDirection = 'asc' | 'desc';
  type ProgramSort = 'program' | 'holders' | 'accounts' | 'balance' | 'activity';
  type ProgramHoldingRow = ReturnType<typeof buildCustodyProgramHoldings>[number];

  interface ProgramEvidence {
    id: string;
    name: string | null;
    source: 'Parser PDA' | 'Runtime owner';
  }

  interface ChainUrlState {
    query: string;
    custodyView: CustodyView;
    holderType: HolderTypeFilter;
    programId: string;
    sortColumn: SortColumn;
    sortDirection: SortDirection;
    pageSize: number;
    pageIndex: number;
    programSort: ProgramSort;
    programSortDirection: SortDirection;
    programPageSize: number;
    programPageIndex: number;
  }

  const typeOptions: Array<{ value: HolderTypeFilter; label: string }> = [
    { value: 'all', label: 'All loaded holders' },
    { value: 'observed_signer', label: 'Signer wallets (observed)' },
    { value: 'pda_or_program', label: 'PDA or program account' },
    { value: 'other_on_curve', label: 'Other on-curve accounts' }
  ];
  const sortOptions: Array<{ value: SortColumn; label: string; direction: SortDirection }> = [
    { value: 'balance', label: 'SPYx balance', direction: 'desc' },
    { value: 'activity', label: 'Public activity', direction: 'desc' },
    { value: 'transactions', label: 'Activity transactions', direction: 'desc' },
    { value: 'accounts', label: 'Token accounts', direction: 'desc' },
    { value: 'owner', label: 'Owner address', direction: 'asc' },
    { value: 'type', label: 'Account type', direction: 'asc' },
    { value: 'program', label: 'Program', direction: 'asc' }
  ];
  const pageSizes = [25, 50, 100];
  const programSortOptions: Array<{
    value: ProgramSort;
    label: string;
    direction: SortDirection;
  }> = [
    { value: 'balance', label: 'SPYx balance', direction: 'desc' },
    { value: 'activity', label: 'Public activity', direction: 'desc' },
    { value: 'holders', label: 'Custody owners', direction: 'desc' },
    { value: 'accounts', label: 'Token accounts', direction: 'desc' },
    { value: 'program', label: 'Program ID', direction: 'asc' }
  ];

  let { authority }: { authority: HolderAuthorityReport } = $props();

  const routePath = page.url.pathname;
  const initialUrlState = readChainUrlState(page.url);
  let query = $state(initialUrlState.query);
  let custodyView = $state<CustodyView>(initialUrlState.custodyView);
  let holderType = $state<HolderTypeFilter>(initialUrlState.holderType);
  let programId = $state(initialUrlState.programId);
  let sortColumn = $state<SortColumn>(initialUrlState.sortColumn);
  let sortDirection = $state<SortDirection>(initialUrlState.sortDirection);
  let filtersOpen = $state(false);
  let pageSize = $state(initialUrlState.pageSize);
  let pageIndex = $state(initialUrlState.pageIndex);
  let programSort = $state<ProgramSort>(initialUrlState.programSort);
  let programSortDirection = $state<SortDirection>(initialUrlState.programSortDirection);
  let programPageSize = $state(initialUrlState.programPageSize);
  let programPageIndex = $state(initialUrlState.programPageIndex);

  const runtimeSupplement = $derived(
    authority.attribution_supplements?.find(
      (supplement) => supplement.evidence_kind === 'solana_runtime_account_owner'
    ) ?? null
  );
  const attributedPdas = $derived(
    authority.attributed_program_holders ??
      authority.largest_25_by_class.attributed_program_derived_address
  );
  const offCurveRows = $derived(
    authority.off_curve_unattributed_holders ?? authority.largest_25_by_class.off_curve_unattributed
  );
  const custodyRows = $derived.by(() => mergeHolderRows([...attributedPdas, ...offCurveRows]));
  const sourceRows = $derived.by(() => mergeHolderRows([
    ...custodyRows,
    ...authority.largest_25_all,
    ...Object.values(authority.largest_25_by_class).flat(),
    ...(authority.largest_25_by_activity_all ?? []),
    ...Object.values(authority.largest_25_by_activity_by_class ?? {}).flat()
  ]));
  const programOptions = $derived(
    buildProgramOptions(
      [...authority.holdings_by_program, ...(runtimeSupplement?.holdings_by_program ?? [])],
      attributedPdas
    )
  );
  const activeFilterCount = $derived(
    Number(custodyView === 'owners' && holderType !== 'all') + Number(programId !== 'all')
  );
  const filteredRows = $derived.by(() => {
    const term = query.trim().toLowerCase();
    return sourceRows.filter((holder) => {
      if (!matchesHolderType(holder, holderType)) return false;
      const evidence = programEvidence(holder);
      if (programId !== 'all' && evidence?.id !== programId) return false;
      if (!term) return true;
      return [holder.owner, evidence?.id]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
        .includes(term);
    });
  });
  const sortedRows = $derived(
    filteredRows.slice().sort((left, right) => compareHolders(left, right, sortColumn, sortDirection))
  );
  const pageCount = $derived(Math.max(1, Math.ceil(sortedRows.length / pageSize)));
  const currentPage = $derived(Math.min(pageIndex, pageCount - 1));
  const visibleRows = $derived(
    sortedRows.slice(currentPage * pageSize, currentPage * pageSize + pageSize)
  );
  const firstVisible = $derived(sortedRows.length === 0 ? 0 : currentPage * pageSize + 1);
  const lastVisible = $derived(Math.min(sortedRows.length, currentPage * pageSize + pageSize));
  const programRows = $derived(buildCustodyProgramHoldings(custodyRows));
  const filteredProgramRows = $derived.by(() => {
    const term = query.trim().toLowerCase();
    return programRows.filter((row) => {
      if (programId !== 'all' && row.program_id !== programId) return false;
      if (!term) return true;
      return [row.program_id, ...row.owner_ids]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
        .includes(term);
    });
  });
  const sortedProgramRows = $derived(
    filteredProgramRows
      .slice()
      .sort((left, right) =>
        compareProgramHoldings(left, right, programSort, programSortDirection)
      )
  );
  const programPageCount = $derived(
    Math.max(1, Math.ceil(sortedProgramRows.length / programPageSize))
  );
  const currentProgramPage = $derived(
    Math.min(programPageIndex, programPageCount - 1)
  );
  const visibleProgramRows = $derived(
    sortedProgramRows.slice(
      currentProgramPage * programPageSize,
      currentProgramPage * programPageSize + programPageSize
    )
  );
  const firstVisibleProgram = $derived(
    sortedProgramRows.length === 0 ? 0 : currentProgramPage * programPageSize + 1
  );
  const lastVisibleProgram = $derived(
    Math.min(
      sortedProgramRows.length,
      currentProgramPage * programPageSize + programPageSize
    )
  );
  const activeSortDirection = $derived(
    custodyView === 'programs' ? programSortDirection : sortDirection
  );

  afterNavigate((navigation) => {
    const destination = navigation.to?.url;
    if (!destination || destination.pathname !== routePath) return;
    applyChainUrlState(readChainUrlState(destination));
  });

  function validPageSize(value: string | null): number {
    const parsed = Number(value);
    return pageSizes.includes(parsed) ? parsed : 50;
  }

  function validPageIndex(value: string | null): number {
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) && parsed >= 1 && parsed <= 100_000 ? parsed - 1 : 0;
  }

  function readChainUrlState(url: URL): ChainUrlState {
    const view = url.searchParams.get('chain_view');
    const type = url.searchParams.get('chain_type');
    const sort = url.searchParams.get('chain_sort');
    const direction = url.searchParams.get('chain_dir');
    const programSortValue = url.searchParams.get('chain_program_sort');
    const programDirection = url.searchParams.get('chain_program_dir');
    const selectedSort = sortOptions.some((option) => option.value === sort)
      ? (sort as SortColumn)
      : 'balance';
    const selectedProgramSort = programSortOptions.some(
      (option) => option.value === programSortValue
    )
      ? (programSortValue as ProgramSort)
      : 'balance';
    return {
      query: url.searchParams.get('chain_q') ?? '',
      custodyView: view === 'programs' ? 'programs' : 'owners',
      holderType: typeOptions.some((option) => option.value === type)
        ? (type as HolderTypeFilter)
        : 'all',
      programId: url.searchParams.get('chain_program') || 'all',
      sortColumn: selectedSort,
      sortDirection:
        direction === 'asc' || direction === 'desc'
          ? direction
          : defaultDirection(selectedSort),
      pageSize: validPageSize(url.searchParams.get('chain_rows')),
      pageIndex: validPageIndex(url.searchParams.get('chain_page')),
      programSort: selectedProgramSort,
      programSortDirection:
        programDirection === 'asc' || programDirection === 'desc'
          ? programDirection
          : defaultProgramDirection(selectedProgramSort),
      programPageSize: validPageSize(url.searchParams.get('chain_program_rows')),
      programPageIndex: validPageIndex(url.searchParams.get('chain_program_page'))
    };
  }

  function applyChainUrlState(state: ChainUrlState): void {
    query = state.query;
    custodyView = state.custodyView;
    holderType = state.holderType;
    programId = state.programId;
    sortColumn = state.sortColumn;
    sortDirection = state.sortDirection;
    pageSize = state.pageSize;
    pageIndex = state.pageIndex;
    programSort = state.programSort;
    programSortDirection = state.programSortDirection;
    programPageSize = state.programPageSize;
    programPageIndex = state.programPageIndex;
  }

  function chainStateUrl(): URL {
    const url = new URL(page.url);
    const names = [
      'chain_view',
      'chain_q',
      'chain_type',
      'chain_program',
      'chain_sort',
      'chain_dir',
      'chain_rows',
      'chain_page',
      'chain_program_sort',
      'chain_program_dir',
      'chain_program_rows',
      'chain_program_page'
    ];
    for (const name of names) url.searchParams.delete(name);
    const trimmedQuery = query.trim();
    if (custodyView === 'programs') url.searchParams.set('chain_view', 'programs');
    if (trimmedQuery) url.searchParams.set('chain_q', trimmedQuery);
    if (holderType !== 'all') url.searchParams.set('chain_type', holderType);
    if (programId !== 'all') url.searchParams.set('chain_program', programId);
    if (sortColumn !== 'balance') url.searchParams.set('chain_sort', sortColumn);
    if (sortDirection !== defaultDirection(sortColumn)) {
      url.searchParams.set('chain_dir', sortDirection);
    }
    if (pageSize !== 50) url.searchParams.set('chain_rows', String(pageSize));
    if (pageIndex > 0) url.searchParams.set('chain_page', String(pageIndex + 1));
    if (programSort !== 'balance') url.searchParams.set('chain_program_sort', programSort);
    if (programSortDirection !== defaultProgramDirection(programSort)) {
      url.searchParams.set('chain_program_dir', programSortDirection);
    }
    if (programPageSize !== 50) {
      url.searchParams.set('chain_program_rows', String(programPageSize));
    }
    if (programPageIndex > 0) {
      url.searchParams.set('chain_program_page', String(programPageIndex + 1));
    }
    return url;
  }

  function commitChainState(replace = false): void {
    const url = chainStateUrl();
    if (url.href === page.url.href) return;
    const destination = `/holders${url.search}${url.hash}` as '/holders';
    if (replace) {
      replaceState(resolve(destination), page.state);
      return;
    }
    void goto(resolve(destination), { keepFocus: true, noScroll: true });
  }

  function mergeHolderRows(rows: ClassifiedPublicBalanceOwner[]): ClassifiedPublicBalanceOwner[] {
    const byOwner = new Map<string, ClassifiedPublicBalanceOwner>();
    for (const row of rows) {
      const previous = byOwner.get(row.owner);
      if (!previous) {
        byOwner.set(row.owner, row);
        continue;
      }
      byOwner.set(row.owner, {
        ...previous,
        ...row,
        pda_program_name: row.pda_program_name ?? previous.pda_program_name,
        public_activity_volume: row.public_activity_volume ?? previous.public_activity_volume,
        public_balance_increase: row.public_balance_increase ?? previous.public_balance_increase,
        public_balance_decrease: row.public_balance_decrease ?? previous.public_balance_decrease,
        activity_transaction_count:
          row.activity_transaction_count ?? previous.activity_transaction_count,
        runtime_account_owner: row.runtime_account_owner ?? previous.runtime_account_owner,
        supplemental_program_attribution:
          row.supplemental_program_attribution ?? previous.supplemental_program_attribution
      });
    }
    return [...byOwner.values()];
  }

  function holderTypeLabel(kind: HolderAuthorityKind): string {
    if (kind === 'observed_transaction_signer') return 'Observed signer';
    if (kind === 'attributed_program_derived_address') return 'PDA';
    if (kind === 'off_curve_unattributed') return 'Off-curve';
    return 'Other on-curve';
  }

  function matchesHolderType(
    holder: ClassifiedPublicBalanceOwner,
    filter: HolderTypeFilter
  ): boolean {
    if (filter === 'all') return true;
    if (filter === 'observed_signer') {
      return holder.authority_kind === 'observed_transaction_signer';
    }
    if (filter === 'pda_or_program') {
      return (
        holder.authority_kind === 'attributed_program_derived_address' ||
        holder.authority_kind === 'off_curve_unattributed'
      );
    }
    return holder.authority_kind === 'unclassified_on_curve';
  }

  function runtimeProgram(
    attribution: SupplementalProgramAttribution | undefined
  ): ProgramEvidence | null {
    if (
      attribution?.account_exists !== true ||
      !attribution.runtime_owner_program_id
    ) {
      return null;
    }
    return {
      id: attribution.runtime_owner_program_id,
      name: attribution.runtime_owner_program_name,
      source: 'Runtime owner'
    };
  }

  function programEvidence(holder: ClassifiedPublicBalanceOwner): ProgramEvidence | null {
    if (
      holder.authority_kind === 'attributed_program_derived_address' &&
      holder.pda_program_id
    ) {
      return {
        id: holder.pda_program_id,
        name: holder.pda_program_name ?? null,
        source: 'Parser PDA'
      };
    }
    return runtimeProgram(holder.supplemental_program_attribution);
  }

  function compareBigInt(left: string | undefined, right: string | undefined): number {
    const leftValue = BigInt(left ?? '-1');
    const rightValue = BigInt(right ?? '-1');
    return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
  }

  function compareText(left: string | undefined, right: string | undefined): number {
    return (left ?? '').localeCompare(right ?? '');
  }

  function compareHolders(
    left: ClassifiedPublicBalanceOwner,
    right: ClassifiedPublicBalanceOwner,
    column: SortColumn,
    direction: SortDirection
  ): number {
    let result = 0;
    if (column === 'owner') result = compareText(left.owner, right.owner);
    if (column === 'type') {
      result = compareText(left.authority_kind, right.authority_kind);
    }
    if (column === 'program') {
      const leftProgram = programEvidence(left);
      const rightProgram = programEvidence(right);
      if (leftProgram === null && rightProgram !== null) return 1;
      if (leftProgram !== null && rightProgram === null) return -1;
      result = compareText(leftProgram?.id, rightProgram?.id);
    }
    if (column === 'accounts') result = left.token_account_count - right.token_account_count;
    if (column === 'balance') {
      result = compareBigInt(left.public_balance.raw_amount, right.public_balance.raw_amount);
    }
    if (column === 'activity') {
      result = compareBigInt(
        left.public_activity_volume?.raw_amount,
        right.public_activity_volume?.raw_amount
      );
    }
    if (column === 'transactions') {
      result = (left.activity_transaction_count ?? -1) - (right.activity_transaction_count ?? -1);
    }
    if (result === 0) result = left.owner.localeCompare(right.owner);
    return direction === 'asc' ? result : -result;
  }

  function compareProgramIds(left: string | null, right: string | null): number {
    if (left === null && right !== null) return 1;
    if (left !== null && right === null) return -1;
    return (left ?? '').localeCompare(right ?? '');
  }

  function compareProgramHoldings(
    left: ProgramHoldingRow,
    right: ProgramHoldingRow,
    column: ProgramSort,
    direction: SortDirection
  ): number {
    let result = 0;
    if (column === 'program') result = compareProgramIds(left.program_id, right.program_id);
    if (column === 'holders') result = left.holder_count - right.holder_count;
    if (column === 'accounts') result = left.token_account_count - right.token_account_count;
    if (column === 'balance') {
      result = compareBigInt(left.public_balance_raw_amount, right.public_balance_raw_amount);
    }
    if (column === 'activity') {
      result = compareBigInt(
        left.public_activity_raw_amount ?? undefined,
        right.public_activity_raw_amount ?? undefined
      );
    }
    if (result === 0) result = compareProgramIds(left.program_id, right.program_id);
    return direction === 'asc' ? result : -result;
  }

  function defaultDirection(column: SortColumn): SortDirection {
    return sortOptions.find((option) => option.value === column)?.direction ?? 'asc';
  }

  function changeSort(column: SortColumn): void {
    if (sortColumn === column) {
      sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
      sortColumn = column;
      sortDirection = defaultDirection(column);
    }
    pageIndex = 0;
    commitChainState();
  }

  function selectSort(column: SortColumn): void {
    sortColumn = column;
    sortDirection = defaultDirection(column);
    pageIndex = 0;
    commitChainState();
  }

  function defaultProgramDirection(column: ProgramSort): SortDirection {
    return programSortOptions.find((option) => option.value === column)?.direction ?? 'asc';
  }

  function changeProgramSort(column: ProgramSort): void {
    if (programSort === column) {
      programSortDirection = programSortDirection === 'asc' ? 'desc' : 'asc';
    } else {
      programSort = column;
      programSortDirection = defaultProgramDirection(column);
    }
    programPageIndex = 0;
    commitChainState();
  }

  function selectProgramSort(column: ProgramSort): void {
    programSort = column;
    programSortDirection = defaultProgramDirection(column);
    programPageIndex = 0;
    commitChainState();
  }

  function toggleSortDirection(): void {
    if (custodyView === 'programs') {
      programSortDirection = programSortDirection === 'asc' ? 'desc' : 'asc';
      programPageIndex = 0;
    } else {
      sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
      pageIndex = 0;
    }
    commitChainState();
  }

  function changeCustodyView(view: CustodyView): void {
    if (custodyView === view) return;
    custodyView = view;
    commitChainState();
  }

  function changeHolderQuery(value: string): void {
    query = value;
    pageIndex = 0;
    programPageIndex = 0;
    commitChainState(true);
  }

  function changeHolderType(value: HolderTypeFilter): void {
    holderType = value;
    pageIndex = 0;
    commitChainState();
  }

  function changeProgramFilter(value: string): void {
    programId = value;
    pageIndex = 0;
    programPageIndex = 0;
    commitChainState();
  }

  function changePageSize(value: number): void {
    pageSize = value;
    pageIndex = 0;
    commitChainState();
  }

  function changeProgramPageSize(value: number): void {
    programPageSize = value;
    programPageIndex = 0;
    commitChainState();
  }

  function goToPage(index: number): void {
    pageIndex = Math.max(0, index);
    commitChainState();
  }

  function goToProgramPage(index: number): void {
    programPageIndex = Math.max(0, index);
    commitChainState();
  }

  function resetFilters(): void {
    query = '';
    holderType = 'all';
    programId = 'all';
    pageIndex = 0;
    programPageIndex = 0;
    commitChainState();
  }
</script>

<section class="panel holder-panel">
  <div class="panel-toolbar holder-title-row">
    <div>
      <h2>{custodyView === 'programs' ? 'Program custody' : 'Holder ranking'}</h2>
      <span class="panel-toolbar-detail">
        {custodyView === 'programs' ? 'Grouped by program ID' : 'Individual custody owners'}
      </span>
    </div>
    <span class="row-count">
      {custodyView === 'programs'
        ? `${formatInteger(filteredProgramRows.length)} program groups`
        : `${formatInteger(filteredRows.length)} loaded matches`}
    </span>
  </div>

  <div class="custody-tabs" role="tablist" aria-label="On-chain custody grouping">
    <button
      type="button"
      role="tab"
      aria-selected={custodyView === 'owners'}
      class={['custody-tab', custodyView === 'owners' && 'active']}
      onclick={() => changeCustodyView('owners')}
    >Custody owners</button>
    <button
      type="button"
      role="tab"
      aria-selected={custodyView === 'programs'}
      class={['custody-tab', custodyView === 'programs' && 'active']}
      onclick={() => changeCustodyView('programs')}
    >Programs</button>
  </div>

  <div class="holder-search-toolbar">
    <label class="search-field">
      <Search size={17} strokeWidth={1.8} aria-hidden="true" />
      <input
        type="search"
        aria-label={custodyView === 'programs' ? 'Search program custody' : 'Search holders'}
        placeholder={custodyView === 'programs'
          ? 'Search program ID or custody owner'
          : 'Search owner or program ID'}
        value={query}
        oninput={(event) => changeHolderQuery(event.currentTarget.value)}
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
      {#if custodyView === 'owners'}
        <label>
          <span>Account type</span>
          <select
            value={holderType}
            onchange={(event) =>
              changeHolderType(event.currentTarget.value as HolderTypeFilter)}
          >
            {#each typeOptions as option (option.value)}
              <option value={option.value}>{option.label}</option>
            {/each}
          </select>
        </label>
      {/if}
      <label class="program-filter">
        <span>Program</span>
        <select
          value={programId}
          onchange={(event) => changeProgramFilter(event.currentTarget.value)}
        >
          <option value="all">All programs and unlinked holders</option>
          {#each programOptions as program (program.id)}
            <option value={program.id}>{program.label}</option>
          {/each}
        </select>
      </label>
      <label>
        <span>Sort by</span>
        {#if custodyView === 'programs'}
          <select
            value={programSort}
            onchange={(event) => selectProgramSort(event.currentTarget.value as ProgramSort)}
          >
            {#each programSortOptions as option (option.value)}
              <option value={option.value}>{option.label}</option>
            {/each}
          </select>
        {:else}
          <select
            value={sortColumn}
            onchange={(event) => selectSort(event.currentTarget.value as SortColumn)}
          >
            {#each sortOptions as option (option.value)}
              <option value={option.value}>{option.label}</option>
            {/each}
          </select>
        {/if}
      </label>
      <button
        type="button"
        class="direction-button"
        onclick={toggleSortDirection}
        aria-label={`Sort ${activeSortDirection === 'asc' ? 'descending' : 'ascending'}`}
      >
        {#if activeSortDirection === 'asc'}
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

  <div class="coverage-line">
    {custodyView === 'programs'
      ? 'Every program ID is included. Names only add display text.'
      : 'PDA or program account includes every PDA and off-curve row. Labels only add names.'}
  </div>

  {#if custodyView === 'programs'}
    <div class="table-wrap desktop-program-table">
      <table>
        <thead>
          <tr>
            <th>#</th>
            <SortableHeader
              label="Program"
              active={programSort === 'program'}
              direction={programSortDirection}
              onclick={() => changeProgramSort('program')}
            />
            <SortableHeader
              label="Custody owners"
              numeric
              active={programSort === 'holders'}
              direction={programSortDirection}
              onclick={() => changeProgramSort('holders')}
            />
            <SortableHeader
              label="Token accounts"
              numeric
              active={programSort === 'accounts'}
              direction={programSortDirection}
              onclick={() => changeProgramSort('accounts')}
            />
            <SortableHeader
              label="SPYx balance"
              numeric
              active={programSort === 'balance'}
              direction={programSortDirection}
              onclick={() => changeProgramSort('balance')}
            />
            <SortableHeader
              label="Public activity"
              numeric
              active={programSort === 'activity'}
              direction={programSortDirection}
              onclick={() => changeProgramSort('activity')}
            />
          </tr>
        </thead>
        <tbody>
          {#each visibleProgramRows as row, index (row.program_id ?? '__program_not_linked__')}
            <tr>
              <td class="muted">{currentProgramPage * programPageSize + index + 1}</td>
              <td class="program-cell">
                {#if row.program_id}
                  <a
                    href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(row.program_id)}`)}
                    title={row.program_id}
                  >
                    <strong>{programDisplayName(row.program_name)}</strong>
                    <span>{shortAddress(row.program_id)}</span>
                  </a>
                {:else}
                  <strong>Program not linked</strong>
                {/if}
              </td>
              <td class="numeric">{formatInteger(row.holder_count)}</td>
              <td class="numeric">{formatInteger(row.token_account_count)}</td>
              <td class="numeric amount-cell">
                {formatRawAmount(row.public_balance_raw_amount, 8, 8)}
              </td>
              <td class="numeric amount-cell">
                {row.public_activity_raw_amount === null
                  ? '—'
                  : formatRawAmount(row.public_activity_raw_amount, 8, 8)}
              </td>
            </tr>
          {:else}
            <tr><td class="empty-row" colspan="6">No program group matches this search.</td></tr>
          {/each}
        </tbody>
      </table>
    </div>

    <div class="mobile-program-list" aria-label="Program custody">
      {#each visibleProgramRows as row, index (row.program_id ?? '__program_not_linked__')}
        <article>
          <header>
            <span class="rank">#{currentProgramPage * programPageSize + index + 1}</span>
            {#if row.program_id}
              <a
                href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(row.program_id)}`)}
                title={row.program_id}
              >{programDisplayName(row.program_name)} · {shortAddress(row.program_id)}</a>
            {:else}
              <strong>Program not linked</strong>
            {/if}
          </header>
          <div class="mobile-program-balance">
            <span>SPYx balance</span>
            <strong>{formatRawAmount(row.public_balance_raw_amount, 8, 6)}</strong>
          </div>
          <dl>
            <div><dt>Custody owners</dt><dd>{formatInteger(row.holder_count)}</dd></div>
            <div><dt>Token accounts</dt><dd>{formatInteger(row.token_account_count)}</dd></div>
            <div>
              <dt>Public activity</dt>
              <dd>{row.public_activity_raw_amount === null ? '—' : formatRawAmount(row.public_activity_raw_amount, 8, 6)}</dd>
            </div>
          </dl>
        </article>
      {:else}
        <p class="empty-row">No program group matches this search.</p>
      {/each}
    </div>

    <footer class="table-footer">
      <span>Rows {formatInteger(firstVisibleProgram)}–{formatInteger(lastVisibleProgram)} of {formatInteger(sortedProgramRows.length)}</span>
      <div class="pagination">
        <label>
          <span>Rows</span>
          <select
            value={programPageSize}
            onchange={(event) => changeProgramPageSize(Number(event.currentTarget.value))}
          >
            {#each pageSizes as size (size)}
              <option value={size}>{size}</option>
            {/each}
          </select>
        </label>
        <button
          type="button"
          disabled={currentProgramPage === 0}
          onclick={() => goToProgramPage(currentProgramPage - 1)}
        >Previous</button>
        <span>{currentProgramPage + 1} / {programPageCount}</span>
        <button
          type="button"
          disabled={currentProgramPage >= programPageCount - 1}
          onclick={() => goToProgramPage(currentProgramPage + 1)}
        >Next</button>
      </div>
    </footer>
  {:else}
  <div class="table-wrap desktop-holder-table">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <SortableHeader
            label="Owner"
            active={sortColumn === 'owner'}
            direction={sortDirection}
            onclick={() => changeSort('owner')}
          />
          <SortableHeader
            label="Type"
            active={sortColumn === 'type'}
            direction={sortDirection}
            onclick={() => changeSort('type')}
          />
          <SortableHeader
            label="Program"
            active={sortColumn === 'program'}
            direction={sortDirection}
            onclick={() => changeSort('program')}
          />
          <SortableHeader
            label="Token accounts"
            numeric
            active={sortColumn === 'accounts'}
            direction={sortDirection}
            onclick={() => changeSort('accounts')}
          />
          <SortableHeader
            label="SPYx balance"
            numeric
            active={sortColumn === 'balance'}
            direction={sortDirection}
            onclick={() => changeSort('balance')}
          />
          <SortableHeader
            label="Public activity"
            numeric
            active={sortColumn === 'activity'}
            direction={sortDirection}
            onclick={() => changeSort('activity')}
          />
          <SortableHeader
            label="Transactions"
            numeric
            active={sortColumn === 'transactions'}
            direction={sortDirection}
            onclick={() => changeSort('transactions')}
          />
        </tr>
      </thead>
      <tbody>
        {#each visibleRows as holder, index (holder.owner)}
          {@const program = programEvidence(holder)}
          <tr>
            <td class="muted">{currentPage * pageSize + index + 1}</td>
            <td>
              <a
                class="owner-link mono"
                href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(holder.owner)}`)}
                title={holder.owner}
              >
                {shortAddress(holder.owner)}
              </a>
            </td>
            <td>{holderTypeLabel(holder.authority_kind)}</td>
            <td class="program-cell">
              {#if program}
                <a
                  href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(program.id)}`)}
                  title={program.id}
                >
                  <strong>{programDisplayName(program.name)}</strong>
                  <span>{shortAddress(program.id)} · {program.source}</span>
                </a>
              {:else}
                <span class="muted">—</span>
              {/if}
            </td>
            <td class="numeric">{formatInteger(holder.token_account_count)}</td>
            <td class="numeric amount-cell">
              {formatBaseUnits(holder.public_balance.base_units, 8)}
            </td>
            <td class="numeric amount-cell">
              {holder.public_activity_volume
                ? formatBaseUnits(holder.public_activity_volume.base_units, 8)
                : '—'}
            </td>
            <td class="numeric">
              {holder.activity_transaction_count === undefined
                ? '—'
                : formatInteger(holder.activity_transaction_count)}
            </td>
          </tr>
        {:else}
          <tr><td class="empty-row" colspan="8">No loaded holder matches this search.</td></tr>
        {/each}
      </tbody>
    </table>
  </div>

  <div class="mobile-holder-list" aria-label="Holder ranking">
    {#each visibleRows as holder, index (holder.owner)}
      {@const program = programEvidence(holder)}
      <article>
        <header>
          <span class="rank">#{currentPage * pageSize + index + 1}</span>
          <a
            class="mono"
            href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(holder.owner)}`)}
            title={holder.owner}
          >{shortAddress(holder.owner)}</a>
          <span>{holderTypeLabel(holder.authority_kind)}</span>
        </header>
        {#if program}
          <a
            class="mobile-program"
            href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(program.id)}`)}
            title={program.id}
          >
            {programDisplayName(program.name)} · {shortAddress(program.id)}
          </a>
        {/if}
        <dl>
          <div><dt>SPYx balance</dt><dd>{formatBaseUnits(holder.public_balance.base_units, 6)}</dd></div>
          <div><dt>Token accounts</dt><dd>{formatInteger(holder.token_account_count)}</dd></div>
          <div>
            <dt>Public activity</dt>
            <dd>{holder.public_activity_volume ? formatBaseUnits(holder.public_activity_volume.base_units, 6) : '—'}</dd>
          </div>
          <div>
            <dt>Transactions</dt>
            <dd>{holder.activity_transaction_count === undefined ? '—' : formatInteger(holder.activity_transaction_count)}</dd>
          </div>
        </dl>
      </article>
    {:else}
      <p class="empty-row">No loaded holder matches this search.</p>
    {/each}
  </div>

  <footer class="table-footer">
    <span>Rows {formatInteger(firstVisible)}–{formatInteger(lastVisible)} of {formatInteger(sortedRows.length)}</span>
    <div class="pagination">
      <label>
        <span>Rows</span>
        <select
        value={pageSize}
        onchange={(event) => changePageSize(Number(event.currentTarget.value))}
        >
          {#each pageSizes as size (size)}
            <option value={size}>{size}</option>
          {/each}
        </select>
      </label>
      <button type="button" disabled={currentPage === 0} onclick={() => goToPage(currentPage - 1)}>
        Previous
      </button>
      <span>{currentPage + 1} / {pageCount}</span>
      <button
        type="button"
        disabled={currentPage >= pageCount - 1}
        onclick={() => goToPage(currentPage + 1)}
      >Next</button>
    </div>
  </footer>
  {/if}
</section>

<style>
  .holder-title-row > div:first-child {
    display: flex;
    align-items: baseline;
    gap: 10px;
  }

  .row-count {
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .custody-tabs {
    display: flex;
    gap: 18px;
    min-height: 38px;
    padding: 0 12px;
    border-bottom: 1px solid var(--border);
  }

  .custody-tab {
    position: relative;
    padding: 0 2px;
    border: 0;
    color: var(--muted);
    font-weight: 600;
    background: transparent;
  }

  .custody-tab:hover,
  .custody-tab.active {
    color: var(--text);
  }

  .custody-tab.active::after {
    position: absolute;
    right: 0;
    bottom: -1px;
    left: 0;
    height: 2px;
    background: var(--accent);
    content: '';
  }

  .holder-search-toolbar {
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
    grid-template-columns: minmax(170px, 0.8fr) minmax(260px, 1.5fr) minmax(160px, 0.8fr) auto auto;
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

  .coverage-line {
    padding: 7px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--muted);
    font-size: 11px;
  }

  .program-cell {
    max-width: 250px;
  }

  .program-cell a,
  .mobile-program {
    color: var(--text);
    text-decoration: none;
  }

  .program-cell strong,
  .program-cell span {
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .program-cell strong {
    font-weight: 600;
  }

  .program-cell span {
    margin-top: 2px;
    color: var(--muted);
    font-size: 10px;
  }

  .amount-cell {
    font-variant-numeric: tabular-nums;
  }

  .empty-row {
    padding: 24px 12px;
    color: var(--muted);
    text-align: center;
  }

  .mobile-holder-list,
  .mobile-program-list {
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
    .holder-title-row > div:first-child {
      display: block;
    }

    .holder-search-toolbar {
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

    .desktop-holder-table,
    .desktop-program-table {
      display: none;
    }

    .mobile-holder-list,
    .mobile-program-list {
      display: grid;
      gap: 8px;
      padding: 9px;
      background: var(--surface-muted);
    }

    .mobile-holder-list article,
    .mobile-program-list article {
      padding: 10px;
      border: 1px solid var(--border);
      border-radius: 7px;
      background: var(--surface);
    }

    .mobile-holder-list article header {
      display: grid;
      grid-template-columns: auto minmax(0, 1fr) auto;
      align-items: center;
      gap: 8px;
    }

    .mobile-holder-list article header a {
      min-width: 0;
      color: var(--text);
      overflow: hidden;
      text-overflow: ellipsis;
      text-decoration: none;
      white-space: nowrap;
    }

    .mobile-program-list article header {
      display: grid;
      grid-template-columns: auto minmax(0, 1fr);
      align-items: center;
      gap: 8px;
    }

    .mobile-program-list article header a {
      min-width: 0;
      color: var(--text);
      overflow: hidden;
      text-overflow: ellipsis;
      text-decoration: none;
      white-space: nowrap;
    }

    .mobile-program-list article header strong {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .mobile-holder-list article header > span:last-child,
    .rank {
      color: var(--muted);
      font-size: 11px;
    }

    .mobile-program {
      display: block;
      margin-top: 7px;
      padding-top: 7px;
      border-top: 1px solid var(--border);
      overflow-wrap: anywhere;
      font-size: 11px;
    }

    .mobile-program-balance {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 10px;
      margin-top: 8px;
      padding-top: 8px;
      border-top: 1px solid var(--border);
    }

    .mobile-program-balance span {
      color: var(--muted);
      font-size: 10px;
    }

    .mobile-program-balance strong {
      font-size: 13px;
      font-variant-numeric: tabular-nums;
    }

    .mobile-holder-list dl,
    .mobile-program-list dl {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 12px;
      margin: 9px 0 0;
    }

    .mobile-holder-list dt,
    .mobile-program-list dt {
      font-size: 10px;
    }

    .mobile-holder-list dd,
    .mobile-program-list dd {
      margin: 2px 0 0;
      font-size: 12px;
      font-variant-numeric: tabular-nums;
      font-weight: 600;
      overflow-wrap: anywhere;
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
