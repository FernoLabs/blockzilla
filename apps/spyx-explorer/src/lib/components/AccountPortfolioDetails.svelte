<script lang="ts">
  import { resolve } from '$app/paths';
  import { ExternalLink } from '@lucide/svelte';
  import AccountBalanceHistoryChart from '$lib/components/AccountBalanceHistoryChart.svelte';
  import { formatBaseUnits, formatInteger, shortAddress } from '$lib/format';
  import { programDisplayName } from '$lib/program-labels.js';
  import type {
    AccountBalanceHistoryResponse,
    AccountProvenTradesResponse,
    AccountTradingSummaryResponse
  } from '$lib/search-api';
  import type {
    AuthorityCandidateFlowEvidence,
    AuthorityPortfolio,
    AuthorityPortfolioHistorySeries,
    PortfolioAccountKind
  } from '$lib/types';

  interface Props {
    address: string;
    portfolio: AuthorityPortfolio | null;
    portfolioHistory: AuthorityPortfolioHistorySeries | null;
    balanceHistory: AccountBalanceHistoryResponse | null;
    tradingSummary: AccountTradingSummaryResponse | null;
    provenTrades: AccountProvenTradesResponse | null;
    balanceHistoryMessage?: string | null;
    dataIntegrityMessage?: string | null;
  }

  let {
    address,
    portfolio,
    portfolioHistory,
    balanceHistory,
    tradingSummary,
    provenTrades,
    balanceHistoryMessage = null,
    dataIntegrityMessage = null
  }: Props = $props();

  const hasDefiFlowHistory = $derived(
    portfolio?.claim_components.some((component) => component.candidate_flow_evidence.length > 0) ??
      false
  );

  function accountKindLabel(kind: PortfolioAccountKind): string {
    if (kind === 'observed_transaction_signer') return 'Observed signer';
    return 'Other on-curve account';
  }

  function rawBaseUnits(raw: string, decimals: number): string {
    const value = BigInt(raw);
    const divisor = 10n ** BigInt(decimals);
    const whole = value / divisor;
    const fraction = (value % divisor).toString().padStart(decimals, '0').replace(/0+$/, '');
    return fraction ? `${whole}.${fraction}` : whole.toString();
  }

  function formatUtc(value: number): string {
    return `${new Date(value * 1000).toLocaleString('en-GB', {
      dateStyle: 'medium',
      timeStyle: 'short',
      timeZone: 'UTC'
    })} UTC`;
  }

  function flowLocation(flow: AuthorityCandidateFlowEvidence): string {
    return flow.block_time === undefined
      ? `Slot ${formatInteger(flow.slot)}`
      : formatUtc(flow.block_time);
  }
</script>

<section class="panel account-panel">
  <div class="account-heading">
    <div>
      <h2>Account details</h2>
      <a
        class="account-address mono"
        href={`https://solscan.io/account/${address}`}
        target="_blank"
        rel="noreferrer"
      >
        {address}
        <ExternalLink size={14} strokeWidth={1.8} aria-label="Open in Solscan" />
      </a>
    </div>
    {#if portfolio}<span class="account-kind">{accountKindLabel(portfolio.authority_kind)}</span>{/if}
  </div>

  {#if portfolio}
    <dl class="account-metrics">
      <div>
        <dt>On-chain holding</dt>
        <dd>{formatBaseUnits(portfolio.direct_public_balance.base_units, 8)} SPYx</dd>
      </div>
      <div>
        <dt>DeFi claim estimate</dt>
        <dd>{formatBaseUnits(portfolio.estimated_defi_claim.base_units, 8)} SPYx</dd>
      </div>
      <div>
        <dt>Total exposure estimate</dt>
        <dd>{formatBaseUnits(portfolio.estimated_total_exposure.base_units, 8)} SPYx</dd>
      </div>
    </dl>

    <p class="portfolio-method-note">
      Estimated claims use observed non-DEX SPYx net flows, capped by current custody. They are not
      decoded protocol positions; yield and debt are excluded.
    </p>

    {#if portfolio.claim_components.length > 0}
      <div class="section-heading">
        <h3>Estimated DeFi claims</h3>
        <span>{formatInteger(portfolio.claim_components.length)} custody links</span>
      </div>
      <div class="position-list">
        {#each portfolio.claim_components as component (`${component.custody_owner}-${component.program_id ?? 'unknown'}`)}
          <article class="position-row">
            <div class="position-program">
              <strong>{programDisplayName(component.program_name)}</strong>
              {#if component.program_id}
                <a
                  class="mono detail-link"
                  href={resolve(`/search?posting_kind=program&posting_key=${encodeURIComponent(component.program_id)}`)}
                  title={component.program_id}
                >{shortAddress(component.program_id)}</a>
              {:else}
                <span class="muted">Program not resolved</span>
              {/if}
            </div>
            <div class="position-custody">
              <span>Custody account</span>
              <a
                class="mono detail-link"
                href={resolve(`/search?posting_kind=owner&posting_key=${encodeURIComponent(component.custody_owner)}`)}
                title={component.custody_owner}
              >{shortAddress(component.custody_owner)}</a>
            </div>
            <dl>
              <div>
                <dt>Claim estimate</dt>
                <dd>{formatBaseUnits(component.attributed_claim.base_units, 8)} SPYx</dd>
              </div>
              <div>
                <dt>Net deposits observed</dt>
                <dd>{formatBaseUnits(component.candidate_net_principal.base_units, 8)} SPYx</dd>
              </div>
              <div>
                <dt>Flows</dt>
                <dd>
                  {formatInteger(component.deposit_transaction_count)} in ·
                  {formatInteger(component.return_transaction_count)} out
                </dd>
              </div>
            </dl>
            {#if (component.candidate_flow_evidence ?? []).length > 0}
              <details class="flow-evidence">
                <summary>
                  Observed flows ({formatInteger(component.candidate_flow_evidence.length)})
                </summary>
                <div class="flow-list">
                  {#each component.candidate_flow_evidence as flow (flow.transaction_id)}
                    <div class="flow-row">
                      <span class={['flow-direction', flow.direction]}>
                        {flow.direction === 'deposit' ? 'Deposit' : 'Return'}
                      </span>
                      <strong>{formatBaseUnits(rawBaseUnits(flow.raw_amount, 8), 8)} SPYx</strong>
                      <span>{flowLocation(flow)}</span>
                      {#if flow.matched_principal_raw_amount && flow.matched_principal_raw_amount !== flow.raw_amount}
                        <span>
                          {formatBaseUnits(rawBaseUnits(flow.matched_principal_raw_amount, 8), 8)} SPYx
                          matched
                        </span>
                      {/if}
                      <a
                        class="detail-link"
                        href={resolve(`/search?transaction_id=${flow.transaction_id}`)}
                      >Transaction {formatInteger(flow.transaction_id)}</a>
                    </div>
                  {/each}
                </div>
              </details>
            {:else if component.deposit_transaction_count + component.return_transaction_count > 0}
              <p class="flow-unavailable">Flow transactions are not stored in this report.</p>
            {/if}
          </article>
        {/each}
      </div>
    {:else}
      <p class="empty-note">No protocol position was estimated for this account.</p>
    {/if}
  {:else}
    <p class="empty-note">No authority portfolio row was found at the dump boundary.</p>
  {/if}

  <div class="history-section">
    <div class="section-heading">
      <h3>Holding over time</h3>
      <span>Wallet and DeFi estimate</span>
    </div>
    {#if dataIntegrityMessage}
      <p class="integrity-note" role="alert">{dataIntegrityMessage}</p>
    {/if}
    {#if (balanceHistory && balanceHistory.items.length > 0) || (portfolioHistory && portfolioHistory.points.length > 0) || hasDefiFlowHistory}
      <AccountBalanceHistoryChart
        direct={balanceHistory}
        history={portfolioHistory}
        {portfolio}
      />
      {#if hasDefiFlowHistory}
        <p class="method-note">
          Wallet holdings use exact balance changes. Estimated DeFi holdings follow observed net
          deposits minus matched returns for open positions. Yield and debt are not included.
        </p>
      {:else}
        <p class="method-note">
          Wallet holdings use exact balance changes. No open DeFi flow history was found.
        </p>
      {/if}
    {:else}
      <p class="empty-note">{balanceHistoryMessage ?? 'No direct balance changes were found.'}</p>
    {/if}
  </div>

  {#if tradingSummary?.has_proven_trades}
    <div class="trading-section">
      <div class="section-heading">
        <h3>Proven DEX activity</h3>
        <span>{formatInteger(tradingSummary.totals.trade_count)} parsed trades</span>
      </div>
      <dl class="trade-metrics">
        <div>
          <dt>SPYx bought</dt>
          <dd>
            {formatBaseUnits(
              rawBaseUnits(tradingSummary.totals.target_bought_raw, tradingSummary.target_decimals),
              8
            )}
          </dd>
        </div>
        <div>
          <dt>SPYx sold</dt>
          <dd>
            {formatBaseUnits(
              rawBaseUnits(tradingSummary.totals.target_sold_raw, tradingSummary.target_decimals),
              8
            )}
          </dd>
        </div>
        <div>
          <dt>Observed range</dt>
          <dd>
            {tradingSummary.first_block_time === undefined
              ? '—'
              : formatUtc(tradingSummary.first_block_time)}
            {#if tradingSummary.last_block_time !== undefined}
              <span class="range-end">to {formatUtc(tradingSummary.last_block_time)}</span>
            {/if}
          </dd>
        </div>
      </dl>

      {#if provenTrades && provenTrades.trades.length > 0}
        <div class="trade-list">
          {#each provenTrades.trades.slice(0, 10) as trade (trade.trade_id)}
            <div class="trade-row">
              <span class={['trade-side', trade.side]}>{trade.side === 'buy' ? 'Buy' : 'Sell'}</span>
              <strong title={`On-chain amount before the UI multiplier: ${formatBaseUnits(rawBaseUnits(trade.target_amount_raw, trade.target_decimals), 8)} SPYx`}>
                {formatBaseUnits(rawBaseUnits(trade.target_amount_scaled_ui_raw, trade.target_decimals), 8)} SPYx
              </strong>
              <span>{trade.program.name}</span>
              <a
                class="mono detail-link"
                href={resolve(`/search?transaction_id=${trade.transaction.transaction_id}`)}
              >ID {formatInteger(trade.transaction.transaction_id)}</a>
            </div>
          {/each}
        </div>
      {/if}
    </div>
  {/if}
</section>

<style>
  .account-panel {
    overflow: hidden;
  }

  .account-heading,
  .section-heading {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
  }

  .account-heading h2,
  .section-heading h3 {
    margin: 0;
  }

  .account-address {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    margin-top: 6px;
    overflow-wrap: anywhere;
  }

  .account-kind {
    flex: 0 0 auto;
    color: var(--muted);
    font-size: 0.86rem;
  }

  .account-metrics,
  .trade-metrics {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    margin: 22px 0 0;
    border: 1px solid var(--border);
    border-radius: 8px;
  }

  .account-metrics > div,
  .trade-metrics > div {
    min-width: 0;
    padding: 14px 16px;
  }

  .account-metrics > div + div,
  .trade-metrics > div + div {
    border-left: 1px solid var(--border);
  }

  dt,
  .position-custody > span {
    color: var(--muted);
    font-size: 0.78rem;
  }

  dd {
    margin: 5px 0 0;
    font-variant-numeric: tabular-nums;
  }

  .account-metrics dd {
    font-size: 1.12rem;
    font-weight: 650;
  }

  .section-heading {
    align-items: baseline;
    margin-top: 26px;
    padding-bottom: 10px;
    border-bottom: 1px solid var(--border);
  }

  .section-heading span,
  .muted,
  .method-note,
  .portfolio-method-note,
  .empty-note {
    color: var(--muted);
  }

  .section-heading span {
    font-size: 0.82rem;
  }

  .position-list {
    border-bottom: 1px solid var(--border);
  }

  .position-row {
    display: grid;
    grid-template-columns: minmax(180px, 0.9fr) minmax(180px, 1fr) minmax(320px, 1.7fr);
    gap: 18px;
    align-items: center;
    padding: 14px 0;
  }

  .position-row + .position-row {
    border-top: 1px solid var(--border);
  }

  .position-program,
  .position-custody {
    display: grid;
    gap: 4px;
    min-width: 0;
  }

  .position-row dl {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 14px;
    margin: 0;
  }

  .flow-evidence,
  .flow-unavailable {
    grid-column: 1 / -1;
  }

  .flow-evidence summary {
    width: fit-content;
    color: var(--muted);
    cursor: pointer;
    font-size: 0.82rem;
  }

  .flow-list {
    margin-top: 8px;
    border-top: 1px solid var(--border);
  }

  .flow-row {
    display: grid;
    grid-template-columns: 62px minmax(112px, 0.6fr) minmax(180px, 1fr) minmax(120px, 0.7fr) auto;
    gap: 10px;
    align-items: center;
    padding: 8px 0;
    border-bottom: 1px solid var(--border);
    font-size: 0.82rem;
  }

  .flow-direction {
    font-weight: 650;
  }

  .flow-direction.deposit {
    color: #216e50;
  }

  .flow-direction.return {
    color: #9c3d2c;
  }

  .flow-unavailable,
  .integrity-note {
    margin: 0;
    color: var(--muted);
    font-size: 0.82rem;
  }

  .integrity-note {
    margin-top: 12px;
    color: #9c3d2c;
  }

  .detail-link {
    width: fit-content;
    max-width: 100%;
  }

  .history-section,
  .trading-section {
    margin-top: 8px;
  }

  .method-note,
  .portfolio-method-note,
  .empty-note {
    margin: 12px 0 0;
    font-size: 0.84rem;
  }

  .trade-list {
    margin-top: 12px;
    border-top: 1px solid var(--border);
  }

  .trade-row {
    display: grid;
    grid-template-columns: 48px minmax(120px, 0.7fr) minmax(150px, 1fr) auto;
    gap: 12px;
    align-items: center;
    padding: 10px 0;
    border-bottom: 1px solid var(--border);
  }

  .trade-side {
    font-weight: 650;
  }

  .trade-side.buy {
    color: #216e50;
  }

  .trade-side.sell {
    color: #9c3d2c;
  }

  .range-end {
    display: block;
    margin-top: 2px;
  }

  @media (max-width: 900px) {
    .position-row {
      grid-template-columns: 1fr 1fr;
    }

    .position-row dl {
      grid-column: 1 / -1;
    }
  }

  @media (max-width: 680px) {
    .account-heading {
      display: grid;
    }

    .account-kind {
      justify-self: start;
    }

    .account-metrics,
    .trade-metrics {
      grid-template-columns: 1fr;
    }

    .account-metrics > div + div,
    .trade-metrics > div + div {
      border-top: 1px solid var(--border);
      border-left: 0;
    }

    .position-row {
      grid-template-columns: 1fr;
      gap: 12px;
    }

    .position-row dl {
      grid-column: auto;
      grid-template-columns: 1fr 1fr;
    }

    .position-row dl > div:last-child {
      grid-column: 1 / -1;
    }

    .trade-row {
      grid-template-columns: 42px 1fr;
    }

    .flow-row {
      grid-template-columns: 58px 1fr;
    }

    .flow-row > :nth-child(n + 3) {
      grid-column: 2;
    }

    .trade-row > :nth-child(3),
    .trade-row > :nth-child(4) {
      grid-column: 2;
    }
  }
</style>
