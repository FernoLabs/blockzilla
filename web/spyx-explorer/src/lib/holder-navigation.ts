export type HolderView = 'estimate' | 'chain';
export type EstimateAccountFilter =
  | 'all'
  | 'observed_transaction_signer'
  | 'other_on_curve_account';
export type SortDirection = 'asc' | 'desc';
export type EstimatePortfolioSort =
  | 'authority'
  | 'type'
  | 'program'
  | 'direct'
  | 'claim'
  | 'total';
export type EstimateCustodySort =
  | 'owner'
  | 'program'
  | 'custody'
  | 'attributed'
  | 'unallocated'
  | 'excess'
  | 'authorities';
export type HolderHistoryMode = 'push' | 'replace';

export interface HolderNavigationState {
  view: HolderView;
  estimateQuery: string;
  estimateAccountFilter: EstimateAccountFilter;
  estimateProgramId: string;
  estimatePortfolioSort: EstimatePortfolioSort;
  estimatePortfolioDirection: SortDirection;
  estimatePortfolioPageIndex: number;
  estimatePortfolioPageSize: number;
  estimateCustodySort: EstimateCustodySort;
  estimateCustodyDirection: SortDirection;
  estimateCustodyPageIndex: number;
  estimateCustodyPageSize: number;
}

export interface HolderNavigationOptions {
  estimateAvailable: boolean;
  estimateProgramIds?: ReadonlySet<string>;
}

const QUERY_LIMIT = 128;
const MAX_PAGE = 1_000_000;
const PAGE_SIZES = new Set([25, 50, 100]);
const ESTIMATE_PARAMS = [
  'estimate_q',
  'estimate_type',
  'estimate_program',
  'estimate_sort',
  'estimate_dir',
  'estimate_page',
  'estimate_rows',
  'estimate_custody_sort',
  'estimate_custody_dir',
  'estimate_custody_page',
  'estimate_custody_rows'
] as const;

const portfolioSorts = new Set<EstimatePortfolioSort>([
  'authority',
  'type',
  'program',
  'direct',
  'claim',
  'total'
]);
const custodySorts = new Set<EstimateCustodySort>([
  'owner',
  'program',
  'custody',
  'attributed',
  'unallocated',
  'excess',
  'authorities'
]);

export function defaultHolderNavigationState(
  options: HolderNavigationOptions
): HolderNavigationState {
  return {
    view: options.estimateAvailable ? 'estimate' : 'chain',
    estimateQuery: '',
    estimateAccountFilter: 'all',
    estimateProgramId: 'all',
    estimatePortfolioSort: 'total',
    estimatePortfolioDirection: 'desc',
    estimatePortfolioPageIndex: 0,
    estimatePortfolioPageSize: 25,
    estimateCustodySort: 'unallocated',
    estimateCustodyDirection: 'desc',
    estimateCustodyPageIndex: 0,
    estimateCustodyPageSize: 25
  };
}

export function parseHolderNavigationState(
  parameters: URLSearchParams,
  options: HolderNavigationOptions
): HolderNavigationState {
  const portfolioSort = readPortfolioSort(parameters.get('estimate_sort'));
  const custodySort = readCustodySort(parameters.get('estimate_custody_sort'));
  return {
    view: readView(parameters.get('view'), options),
    estimateQuery: readQuery(parameters.get('estimate_q')),
    estimateAccountFilter: readAccountFilter(parameters.get('estimate_type')),
    estimateProgramId: readProgramId(parameters.get('estimate_program'), options),
    estimatePortfolioSort: portfolioSort,
    estimatePortfolioDirection: readDirection(
      parameters.get('estimate_dir'),
      defaultPortfolioDirection(portfolioSort)
    ),
    estimatePortfolioPageIndex: readPageIndex(parameters.get('estimate_page')),
    estimatePortfolioPageSize: readPageSize(parameters.get('estimate_rows')),
    estimateCustodySort: custodySort,
    estimateCustodyDirection: readDirection(
      parameters.get('estimate_custody_dir'),
      defaultCustodyDirection(custodySort)
    ),
    estimateCustodyPageIndex: readPageIndex(parameters.get('estimate_custody_page')),
    estimateCustodyPageSize: readPageSize(parameters.get('estimate_custody_rows'))
  } satisfies HolderNavigationState;
}

export function holderNavigationUrl(
  currentUrl: URL,
  requestedState: HolderNavigationState,
  options: HolderNavigationOptions
): URL {
  const state = normalizeHolderNavigationState(requestedState, options);
  const defaults = defaultHolderNavigationState(options);
  const url = new URL(currentUrl);
  url.searchParams.delete('view');
  for (const parameter of ESTIMATE_PARAMS) url.searchParams.delete(parameter);

  if (state.view !== defaults.view) url.searchParams.set('view', state.view);
  if (state.estimateQuery) url.searchParams.set('estimate_q', state.estimateQuery);
  if (state.estimateAccountFilter !== 'all') {
    url.searchParams.set(
      'estimate_type',
      state.estimateAccountFilter === 'observed_transaction_signer' ? 'signer' : 'other'
    );
  }
  if (state.estimateProgramId !== 'all') {
    url.searchParams.set('estimate_program', state.estimateProgramId);
  }
  if (state.estimatePortfolioSort !== defaults.estimatePortfolioSort) {
    url.searchParams.set('estimate_sort', state.estimatePortfolioSort);
  }
  if (
    state.estimatePortfolioDirection !==
    defaultPortfolioDirection(state.estimatePortfolioSort)
  ) {
    url.searchParams.set('estimate_dir', state.estimatePortfolioDirection);
  }
  writePage(url.searchParams, 'estimate_page', state.estimatePortfolioPageIndex);
  writePageSize(url.searchParams, 'estimate_rows', state.estimatePortfolioPageSize);

  if (state.estimateCustodySort !== defaults.estimateCustodySort) {
    url.searchParams.set('estimate_custody_sort', state.estimateCustodySort);
  }
  if (
    state.estimateCustodyDirection !== defaultCustodyDirection(state.estimateCustodySort)
  ) {
    url.searchParams.set('estimate_custody_dir', state.estimateCustodyDirection);
  }
  writePage(url.searchParams, 'estimate_custody_page', state.estimateCustodyPageIndex);
  writePageSize(
    url.searchParams,
    'estimate_custody_rows',
    state.estimateCustodyPageSize
  );
  return url;
}

export function normalizeHolderNavigationState(
  state: HolderNavigationState,
  options: HolderNavigationOptions
): HolderNavigationState {
  const parameters = new URLSearchParams();
  parameters.set('view', state.view);
  parameters.set('estimate_q', state.estimateQuery);
  parameters.set(
    'estimate_type',
    state.estimateAccountFilter === 'observed_transaction_signer'
      ? 'signer'
      : state.estimateAccountFilter === 'other_on_curve_account'
        ? 'other'
        : 'all'
  );
  parameters.set('estimate_program', state.estimateProgramId);
  parameters.set('estimate_sort', state.estimatePortfolioSort);
  parameters.set('estimate_dir', state.estimatePortfolioDirection);
  parameters.set('estimate_page', String(state.estimatePortfolioPageIndex + 1));
  parameters.set('estimate_rows', String(state.estimatePortfolioPageSize));
  parameters.set('estimate_custody_sort', state.estimateCustodySort);
  parameters.set('estimate_custody_dir', state.estimateCustodyDirection);
  parameters.set('estimate_custody_page', String(state.estimateCustodyPageIndex + 1));
  parameters.set('estimate_custody_rows', String(state.estimateCustodyPageSize));
  return parseHolderNavigationState(parameters, options);
}

export function defaultPortfolioDirection(sort: EstimatePortfolioSort): SortDirection {
  return sort === 'authority' || sort === 'type' || sort === 'program' ? 'asc' : 'desc';
}

export function defaultCustodyDirection(sort: EstimateCustodySort): SortDirection {
  return sort === 'owner' || sort === 'program' ? 'asc' : 'desc';
}

function readView(value: string | null, options: HolderNavigationOptions): HolderView {
  if (value === 'chain') return 'chain';
  if (value === 'estimate') return 'estimate';
  return options.estimateAvailable ? 'estimate' : 'chain';
}

function readQuery(value: string | null): string {
  return (value ?? '').slice(0, QUERY_LIMIT);
}

function readAccountFilter(value: string | null): EstimateAccountFilter {
  if (value === 'signer') return 'observed_transaction_signer';
  if (value === 'other') return 'other_on_curve_account';
  return 'all';
}

function readProgramId(value: string | null, options: HolderNavigationOptions): string {
  if (!value || !/^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(value)) return 'all';
  if (options.estimateProgramIds && !options.estimateProgramIds.has(value)) return 'all';
  return value;
}

function readPortfolioSort(value: string | null): EstimatePortfolioSort {
  return portfolioSorts.has(value as EstimatePortfolioSort)
    ? (value as EstimatePortfolioSort)
    : 'total';
}

function readCustodySort(value: string | null): EstimateCustodySort {
  return custodySorts.has(value as EstimateCustodySort)
    ? (value as EstimateCustodySort)
    : 'unallocated';
}

function readDirection(value: string | null, fallback: SortDirection): SortDirection {
  return value === 'asc' || value === 'desc' ? value : fallback;
}

function readPageIndex(value: string | null): number {
  if (!value || !/^[1-9][0-9]*$/.test(value)) return 0;
  const page = Number(value);
  return Number.isSafeInteger(page) && page <= MAX_PAGE ? page - 1 : 0;
}

function readPageSize(value: string | null): number {
  const size = Number(value);
  return PAGE_SIZES.has(size) ? size : 25;
}

function writePage(parameters: URLSearchParams, name: string, pageIndex: number): void {
  if (pageIndex > 0) parameters.set(name, String(pageIndex + 1));
}

function writePageSize(parameters: URLSearchParams, name: string, pageSize: number): void {
  if (pageSize !== 25) parameters.set(name, String(pageSize));
}
