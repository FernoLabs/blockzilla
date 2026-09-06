<script lang="ts">
  import { resolve } from '$app/paths';
  import { ArrowLeft } from '@lucide/svelte';
  import { formatBaseUnits, formatBytes, formatInteger } from '$lib/format';
  import type { PageProps } from './$types';

  let { data }: PageProps = $props();
  const report = $derived(data.report);
  const replay = $derived(report.strict_instruction_replay);
  const blockerCount = $derived(
    Object.values(replay.blockers).reduce((total, value) => total + value, 0)
  );
  const statusRows = $derived([
    {
      name: 'Indexed transaction coverage',
      detail: `${formatInteger(report.source.transactions)} Solana transactions were checked across epochs ${report.source.first_epoch}–${report.source.last_epoch}.`,
      complete: report.status.bounded_selected_dump_scan_complete,
      pending: 'Incomplete'
    },
    {
      name: 'Token account balance continuity',
      detail: 'Each stored pre-balance matches the prior stored post-balance from the SPYx mint transaction onward.',
      complete: report.status.metadata_balance_chain_continuous_from_spyx_mint_creation,
      pending: 'Mismatch'
    },
    {
      name: 'Token-2022 instruction verification',
      detail: replay.present
        ? (replay.proof_scope ?? 'A Token-2022 instruction verification report is attached.')
        : 'No instruction verification report is attached.',
      complete: replay.instruction_replay_matches_metadata_for_complete_spyx_selected_history,
      pending: replayStatusLabel(replay.status)
    }
  ]);
  const missingDataChecks = $derived([
    ['Transactions without metadata', report.audit.metadata_absent],
    ['Balance rows without a wallet owner', report.audit.target_balance_rows_without_owner],
    ['Positive balances without a wallet owner', report.audit.target_positive_states_without_owner],
    ['Transactions without block time', report.audit.transactions_without_block_time],
    ['Balance changes without block time', report.audit.public_state_changes_without_block_time],
    ['Transactions without the mint or a token account', report.audit.selected_transactions_without_target_address]
  ]);
  const replayChecks = $derived([
    ['Verification errors', replay.counters.replay_errors ?? 0],
    ['Pre-balance mismatches', replay.counters.oracle_pre_mismatches ?? 0],
    ['Post-balance mismatches', replay.counters.oracle_post_mismatches ?? 0],
    ['Unknown commit decisions', replay.counters.unknown_commit_target_token_invocations ?? 0],
    ['Missing inner-instruction records', replay.counters.successful_transactions_without_inner_instruction_recording ?? 0],
    ['Recorded malformed token instructions', replay.counters.malformed_target_token_invocations ?? 0]
  ]);
  const sourceFiles = $derived([
    report.source.transactions_file,
    report.source.signatures_file,
    report.source.registry_file,
    report.source.accounts_file,
    report.source.manifest
  ]);

  function replayStatusLabel(value: string): string {
    const labels: Record<string, string> = {
      complete_match: 'Complete match',
      complete_scan_replay_blocked: 'Verification blocked',
      canary_prefix_only: 'Partial check',
      not_performed: 'Not performed'
    };
    return labels[value] ?? value.replaceAll('_', ' ');
  }
</script>

<svelte:head>
  <title>SPYx data integrity</title>
  <meta
    name="description"
    content="Verification status, coverage limits, and source hashes for the indexed SPYx Solana history."
  />
</svelte:head>

<header class="topbar">
  <div>
    <h1>Data integrity</h1>
    <div class="address">{report.source.mint}</div>
  </div>
  <div class="controls">
    <a class="toolbar-button" href={resolve('/')}>
      <ArrowLeft size={16} strokeWidth={1.8} />
      <span>Overview</span>
    </a>
  </div>
</header>

<section class="panel">
  <div class="panel-toolbar">
    <h2>Verification result</h2>
    <span class="panel-toolbar-detail">SPYx Token-2022 account history</span>
  </div>
  <div class="integrity-list">
    {#each statusRows as row (row.name)}
      <article class="integrity-row">
        <h3>{row.name}</h3>
        <p>{row.detail}</p>
        <span class={['status-value', row.complete ? 'pass' : 'pending']}>
          {row.complete ? 'Complete' : row.pending}
        </span>
      </article>
    {/each}
  </div>
  {#if replay.first_failure}
    <div class="notice replay-failure">
      <p>
        <strong>First verification issue:</strong> {replay.first_failure.code} at slot
        {formatInteger(replay.first_failure.slot)}, transaction index {formatInteger(replay.first_failure.tx_index)}.
      </p>
      <p>{replay.first_failure.detail}</p>
    </div>
  {/if}
</section>

<section class="summary" aria-label="Indexed SPYx history">
  <div class="summary-cell">
    <div class="label">Epoch range</div>
    <div class="value">{report.source.first_epoch}–{report.source.last_epoch}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Mint creation slot</div>
    <div class="value">{formatInteger(report.source.mint_slot)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Indexed transactions</div>
    <div class="value">{formatInteger(report.source.transactions)}</div>
  </div>
  <div class="summary-cell">
    <div class="label">Historical token accounts</div>
    <div class="value">{formatInteger(report.source.discovered_token_accounts)}</div>
  </div>
</section>

<section class="panel">
  <div class="panel-toolbar">
    <h2>Missing data checks</h2>
    <span class="panel-toolbar-detail">All values must be zero</span>
  </div>
  <dl class="check-grid">
    {#each missingDataChecks as row (row[0])}
      <div>
        <dt>{row[0]}</dt>
        <dd class:pass-number={row[1] === 0}>{formatInteger(row[1])}</dd>
      </div>
    {/each}
  </dl>
</section>

{#if replay.present}
  <section class="panel">
    <div class="panel-toolbar">
      <h2>Instruction verification evidence</h2>
      <span class="panel-toolbar-detail">{replayStatusLabel(replay.status)}</span>
    </div>
    <dl class="replay-state">
      <div>
        <dt>Final token balance</dt>
        <dd>{formatBaseUnits(report.final_public_balance.public_raw_balance_sum.base_units, 4)} SPYx</dd>
        <small class="mono">{replay.replayed_state?.public_raw_balance ?? '—'} raw units</small>
      </div>
      <div>
        <dt>Tracked token accounts</dt>
        <dd>{formatInteger(replay.replayed_state?.tracked_accounts ?? 0)}</dd>
      </div>
      <div>
        <dt>Open / closed token accounts</dt>
        <dd>{formatInteger(replay.replayed_state?.open_accounts ?? 0)} / {formatInteger(replay.replayed_state?.closed_accounts ?? 0)}</dd>
      </div>
      <div>
        <dt>Blocking issues</dt>
        <dd>{formatInteger(blockerCount)}</dd>
      </div>
    </dl>
    <dl class="check-grid replay-checks">
      {#each replayChecks as row (row[0])}
        <div>
          <dt>{row[0]}</dt>
          <dd class:pass-number={row[1] === 0}>{formatInteger(row[1])}</dd>
        </div>
      {/each}
    </dl>
    {#if (replay.counters.malformed_target_token_invocations ?? 0) > 0}
      <div class="notice neutral-note">
        The verifier recorded {formatInteger(replay.counters.malformed_target_token_invocations)} malformed token
        instructions. They did not block the accepted result because the verified token balance still matched the
        stored pre- and post-balances.
      </div>
    {/if}
  </section>
{/if}

<section class="panel">
  <div class="panel-toolbar">
    <h2>Coverage limits</h2>
    <span class="panel-toolbar-detail">Read this before you use the totals</span>
  </div>
  <div class="notice limits-note">
    <p>
      This page verifies public raw SPYx token account balances in the indexed history. It does not verify all
      activity for each wallet owner.
    </p>
    <p>
      Confidential Transfer balances are not visible in public token balance metadata. Scaled UI Amount can
      change displayed amounts. The explorer uses raw balances with {report.final_public_balance.decimals} mint decimals.
    </p>
    <p>
      Account-to-account movement is not DEX trading volume or USD volume. Market panels use separately verified
      instruction-level swaps.
    </p>
  </div>
</section>

<section class="panel provenance-panel">
  <div class="panel-toolbar">
    <h2>Source proof</h2>
    <span class="panel-toolbar-detail">SHA-256</span>
  </div>
  <dl class="hash-list">
    <div>
      <dt>Transaction history</dt>
      <dd class="mono">{report.source.transactions_file.sha256}</dd>
    </div>
    {#if replay.source_report_sha256}
      <div>
        <dt>Instruction verification report</dt>
        <dd class="mono">{replay.source_report_sha256}</dd>
      </div>
    {/if}
    {#if replay.replayed_state}
      <div>
        <dt>Verified final state</dt>
        <dd class="mono">{replay.replayed_state.state_sha256}</dd>
      </div>
    {/if}
  </dl>
  <details class="source-files">
    <summary>All indexed source files</summary>
    <dl>
      {#each sourceFiles as file (file.file)}
        <div>
          <dt class="mono">{file.file}</dt>
          <dd>
            <span>{formatBytes(file.bytes)}</span>
            <code>{file.sha256}</code>
          </dd>
        </div>
      {/each}
    </dl>
  </details>
</section>

<style>
  .integrity-row {
    min-height: 70px;
    display: grid;
    grid-template-columns: minmax(190px, 0.28fr) minmax(0, 1fr) auto;
    align-items: center;
    gap: 16px;
    padding: 12px;
    border-bottom: 1px solid var(--border);
  }

  .integrity-row:last-child { border-bottom: 0; }
  .integrity-row h3,
  .integrity-row p { margin: 0; }
  .integrity-row h3 { font-size: 13px; }
  .integrity-row p { color: var(--muted); font-size: 12px; }

  .summary { grid-template-columns: repeat(4, minmax(130px, 1fr)); }
  .summary-cell:nth-child(4) { border-right: 0; }

  .check-grid,
  .replay-state,
  .hash-list,
  .source-files dl { margin: 0; }

  .check-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .check-grid > div,
  .replay-state > div {
    min-width: 0;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 10px 12px;
    border-right: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
  }

  .check-grid > div:nth-child(3n) { border-right: 0; }
  .check-grid > div:nth-last-child(-n + 3) { border-bottom: 0; }
  .check-grid dd,
  .replay-state dd { margin: 0; font-weight: 650; }
  .pass-number { color: var(--accent); }

  .replay-state {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
  }

  .replay-state > div { display: grid; gap: 3px; align-content: start; }
  .replay-state > div:last-child { border-right: 0; }
  .replay-state small { color: var(--muted); overflow-wrap: anywhere; }
  .replay-checks { border-top: 1px solid var(--border); }
  .neutral-note,
  .limits-note { border-bottom: 0; }

  .hash-list > div,
  .source-files dl > div {
    display: grid;
    grid-template-columns: 210px minmax(0, 1fr);
    gap: 12px;
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
  }

  .hash-list dd,
  .source-files dd { margin: 0; overflow-wrap: anywhere; }

  .source-files summary {
    min-height: 44px;
    display: flex;
    align-items: center;
    padding: 9px 12px;
    cursor: pointer;
    font-weight: 600;
  }

  .source-files dl { border-top: 1px solid var(--border); }
  .source-files dl > div:last-child { border-bottom: 0; }
  .source-files dd { display: grid; gap: 4px; }
  .source-files code { font-size: 11px; overflow-wrap: anywhere; }

  @media (max-width: 760px) {
    .integrity-row { grid-template-columns: minmax(0, 1fr) auto; gap: 6px 10px; }
    .integrity-row p { grid-column: 1 / -1; }
    .summary { grid-template-columns: 1fr 1fr; }
    .check-grid { grid-template-columns: 1fr 1fr; }
    .check-grid > div:nth-child(3n) { border-right: 1px solid var(--border); }
    .check-grid > div:nth-child(even) { border-right: 0; }
    .check-grid > div:nth-last-child(-n + 3) { border-bottom: 1px solid var(--border); }
    .check-grid > div:nth-last-child(-n + 2) { border-bottom: 0; }
    .replay-state { grid-template-columns: 1fr 1fr; }
    .replay-state > div:nth-child(even) { border-right: 0; }
    .replay-state > div:nth-child(-n + 2) { border-bottom: 1px solid var(--border); }
    .hash-list > div,
    .source-files dl > div { grid-template-columns: 1fr; gap: 4px; }
  }

  @media (max-width: 420px) {
    .check-grid,
    .replay-state { grid-template-columns: 1fr; }
    .check-grid > div,
    .check-grid > div:nth-child(3n),
    .replay-state > div { border-right: 0; border-bottom: 1px solid var(--border); }
    .check-grid > div:last-child,
    .replay-state > div:last-child { border-bottom: 0; }
  }
</style>
