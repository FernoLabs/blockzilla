import { asset } from '$app/paths';
import type { PageLoad } from './$types';
import type { AuthorityPortfolioTableReport } from '$lib/types';

export const load: PageLoad = async ({ fetch, parent }) => {
  const { report } = await parent();
  if (report.compact_build.authority_portfolio_table_available !== true) {
    return { authorityPortfolios: null };
  }

  const response = await fetch(asset('/data/spyx-authority-portfolios-table.json'));
  if (!response.ok) {
    throw new Error(`SPYx authority portfolio table failed with HTTP ${response.status}`);
  }
  const table = (await response.json()) as AuthorityPortfolioTableReport;
  if (
    table?.schema_version !== 1 ||
    table.artifact_kind !== 'spyx_authority_portfolio_table' ||
    table.coverage?.complete !== true ||
    table.source_binding?.transactions_sha256 !== report.source.transactions_file.sha256 ||
    table.coverage.transactions_scanned !== report.source.transactions
  ) {
    throw new Error('SPYx authority portfolio table does not match the loaded report');
  }

  return { authorityPortfolios: table };
};
