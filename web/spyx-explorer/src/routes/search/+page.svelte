<script lang="ts">
  import { afterNavigate, beforeNavigate, goto } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { page } from '$app/state';
  import { ArrowLeft, RefreshCw } from '@lucide/svelte';
  import { onDestroy } from 'svelte';
  import AccountPortfolioDetails from '$lib/components/AccountPortfolioDetails.svelte';
  import { getAuthorityPortfolio } from '$lib/authority-portfolio';
  import { getAuthorityPortfolioHistory } from '$lib/authority-portfolio-history';
  import { formatBaseUnits, formatInteger, shortAddress } from '$lib/format';
  import { createLatestRequestObserver } from '$lib/latest-request.js';
  import {
    getPdaAuthorityEstimate,
    getPdaAuthorityEstimatesByProgram
  } from '$lib/pda-authority';
  import { getPdaFlowProof } from '$lib/pda-flow-proof';
  import {
    SearchApiError,
    bindSearchHealthToDataset,
    getAccountBalanceHistory,
    getAccountProvenTrades,
    getAccountTradingSummary,
    getPostings,
    getSearchHealth,
    getTransactionByCoordinate,
    getTransactionById,
    getTransactionBySignature,
    searchApiBaseLabel,
    type PostingKind,
    type AccountBalanceHistoryResponse,
    type AccountProvenTradesResponse,
    type AccountTradingSummaryResponse,
    type PostingsResponse,
    type ProgramInstructionScope,
    type SearchHealthResponse,
    type TransactionLookupResponse
  } from '$lib/search-api';
  import type {
    Amount,
    AuthorityPortfolio,
    AuthorityPortfolioHistorySeries,
    PdaAuthorityEstimate,
    PdaFlowProof
  } from '$lib/types';
  import type { PageProps } from './$types';

  type SearchMode = 'signature' | 'id' | 'coordinate' | 'postings';
  type SearchHref = `/search?${string}`;
  type SearchState =
    | { status: 'idle' }
    | { status: 'loading' }
    | { status: 'empty'; detail: string }
    | { status: 'error' | 'unavailable'; message: string }
    | { status: 'transaction'; response: TransactionLookupResponse }
    | {
        status: 'postings';
        response: PostingsResponse;
        pdaAuthorityEstimate: PdaAuthorityEstimate | null;
        pdaCreationTransaction: TransactionLookupResponse | null;
        pdaFlowProof: PdaFlowProof | null;
        programPdaEstimates: PdaAuthorityEstimate[];
        pdaAuthorityError: string | null;
        accountDetails: AccountDetailsResult | null;
      };

  interface AccountDetailsResult {
    portfolio: AuthorityPortfolio | null;
    portfolioHistory: AuthorityPortfolioHistorySeries | null;
    balanceHistory: AccountBalanceHistoryResponse | null;
    tradingSummary: AccountTradingSummaryResponse | null;
    provenTrades: AccountProvenTradesResponse | null;
    balanceHistoryMessage: string | null;
    dataIntegrityMessage: string | null;
  }

  interface OwnerPdaAuthorityResult {
    estimate: PdaAuthorityEstimate | null;
    creationTransaction: TransactionLookupResponse | null;
    flowProof: PdaFlowProof | null;
    error: string | null;
  }

  interface ProgramPdaAuthorityResult {
    estimates: PdaAuthorityEstimate[];
    error: string | null;
  }

  interface ProgramLinkedPda {
    address: string;
    ownerProgramName: string | null;
    exactPublicBalance: Amount;
    linkEvidence: string;
    estimate: PdaAuthorityEstimate | null;
  }

  interface SearchRouteState {
    mode: SearchMode;
    signature: string;
    transactionId: string;
    epoch: string;
    slot: string;
    sourceBlockId: string;
    transactionIndex: string;
    postingKind: PostingKind;
    postingKey: string;
    programInstructionScope: ProgramInstructionScope;
    postingCursor: string;
    postingLimit: string;
    hasSearch: boolean;
  }

  const modes: Array<{ id: SearchMode; label: string }> = [
    { id: 'signature', label: 'Signature' },
    { id: 'id', label: 'Index record ID' },
    { id: 'coordinate', label: 'Slot + index' },
    { id: 'postings', label: 'Accounts & programs' }
  ];
  const postingPageLimit = 200;
  const apiBase = searchApiBaseLabel();
  let { data }: PageProps = $props();
  const report = $derived(data.report);
  const expectedDataset = $derived({
    transactions: report.source.transactions,
    source_transaction_sha256: report.source.transactions_file.sha256
  });
  const initialRouteState = readSearchRouteState(page.url);

  let mode = $state<SearchMode>(initialRouteState.mode);
  let signature = $state(initialRouteState.signature);
  let transactionId = $state(initialRouteState.transactionId);
  let epoch = $state(initialRouteState.epoch);
  let slot = $state(initialRouteState.slot);
  let sourceBlockId = $state(initialRouteState.sourceBlockId);
  let transactionIndex = $state(initialRouteState.transactionIndex);
  let postingKind = $state<PostingKind>(initialRouteState.postingKind);
  let postingKey = $state(initialRouteState.postingKey);
  let programInstructionScope = $state<ProgramInstructionScope>(
    initialRouteState.programInstructionScope
  );
  let postingCursor = $state(initialRouteState.postingCursor);
  let postingLimit = $state(initialRouteState.postingLimit);
  let searchState = $state.raw<SearchState>({ status: 'idle' });
  let health = $state.raw<SearchHealthResponse | null>(null);
  const observeLatestHealthRequest = createLatestRequestObserver();
  const firstHealthRequest = loadHealth();
  let healthRequest = $state.raw(firstHealthRequest);
  let activeController: AbortController | undefined;
  let queryNavigationSequence = 0;
  const backendBinding = $derived(
    health === null ? null : bindSearchHealthToDataset(health, expectedDataset)
  );
  const backendReady = $derived(backendBinding?.status === 'match');
  const postingAvailable = $derived.by(() => {
    if (!backendReady) return false;
    const capabilities = health?.postings;
    if (!capabilities?.available || !capabilities.complete) return false;
    if (postingKind === 'target-address') return capabilities.target_address;
    if (postingKind === 'token-account') return capabilities.token_account;
    if (postingKind === 'program') return capabilities.program;
    return capabilities.owner;
  });
  const ownerPostingAvailable = $derived(
    backendReady &&
      health?.postings.available === true &&
      health.postings.complete &&
      health.postings.owner
  );

  beforeNavigate((navigation) => {
    const destination = navigation.to?.url;
    if (!destination || destination.pathname !== page.url.pathname) return;
    if (destination.search === page.url.search) return;
    queryNavigationSequence += 1;
    activeController?.abort();
    activeController = undefined;
    searchState = { status: 'idle' };
  });

  afterNavigate((navigation) => {
    syncSearchFromUrl(navigation.to?.url ?? page.url);
  });

  onDestroy(() => {
    activeController?.abort();
  });

  function syncSearchFromUrl(url: URL): void {
    const navigationSequence = ++queryNavigationSequence;
    const routeState = readSearchRouteState(url);

    activeController?.abort();
    activeController = undefined;
    mode = routeState.mode;
    signature = routeState.signature;
    transactionId = routeState.transactionId;
    epoch = routeState.epoch;
    slot = routeState.slot;
    sourceBlockId = routeState.sourceBlockId;
    transactionIndex = routeState.transactionIndex;
    postingKind = routeState.postingKind;
    postingKey = routeState.postingKey;
    programInstructionScope = routeState.programInstructionScope;
    postingCursor = routeState.postingCursor;
    postingLimit = routeState.postingLimit;
    searchState = { status: 'idle' };

    if (!routeState.hasSearch) return;
    void healthRequest.then(
      () => {
        if (navigationSequence === queryNavigationSequence) void executeSearch();
      },
      () => {
        // The backend status panel reports the failed health request.
      }
    );
  }

  function changePostingKind(nextKind: PostingKind): void {
    if (postingKind === nextKind) return;
    postingKind = nextKind;
    resetPostingPage();
  }

  function changePostingKey(nextKey: string): void {
    if (postingKey === nextKey) return;
    postingKey = nextKey;
    resetPostingPage();
  }

  function changeProgramInstructionScope(nextScope: ProgramInstructionScope): void {
    if (programInstructionScope === nextScope) return;
    programInstructionScope = nextScope;
    resetPostingPage();
  }

  function resetPostingPage(): void {
    postingCursor = '';
    clearDraftSearchResult();
  }

  function clearDraftSearchResult(): void {
    queryNavigationSequence += 1;
    activeController?.abort();
    activeController = undefined;
    searchState = { status: 'idle' };
  }

  function refreshHealth(): void {
    activeController?.abort();
    activeController = undefined;
    searchState = { status: 'idle' };
    healthRequest = loadHealth();
  }

  function loadHealth(): Promise<SearchHealthResponse> {
    health = null;
    const request = getSearchHealth();
    observeLatestHealthRequest(
      request,
      (response) => {
        health = response;
      },
      () => {
        health = null;
      }
    );
    return request;
  }

  function submitSearch(): void {
    const validationError = validateInput();
    if (validationError) {
      searchState = { status: 'error', message: validationError };
      return;
    }

    const destinationPath = currentSearchHref();
    const destination = resolve(destinationPath);
    const current = `${page.url.pathname}${page.url.search}`;
    if (destination === current) {
      void executeSearch();
      return;
    }
    void goto(resolve(destinationPath), { keepFocus: true, noScroll: true }).catch(() => {
      searchState = {
        status: 'error',
        message: 'The search URL could not be opened.'
      };
    });
  }

  async function executeSearch(): Promise<void> {
    activeController?.abort();
    const controller = new AbortController();
    activeController = controller;
    searchState = { status: 'loading' };

    try {
      if (mode === 'signature') {
        const response = await getTransactionBySignature(signature.trim(), controller.signal);
        searchState = response
          ? { status: 'transaction', response }
          : { status: 'empty', detail: 'No transaction matched this signature.' };
        return;
      }

      if (mode === 'id') {
        const response = await getTransactionById(Number(transactionId.trim()), controller.signal);
        searchState = response
          ? { status: 'transaction', response }
          : { status: 'empty', detail: 'No transaction matched this immutable ID.' };
        return;
      }

      if (mode === 'coordinate') {
        const response = await getTransactionByCoordinate(
          {
            epoch: epoch.trim(),
            slot: slot.trim(),
            source_block_id: sourceBlockId.trim(),
            tx_index: transactionIndex.trim()
          },
          controller.signal
        );
        searchState = response
          ? { status: 'transaction', response }
          : { status: 'empty', detail: 'No transaction matched this slot and archive position.' };
        return;
      }

      const pdaAuthorityRequest =
        postingKind === 'owner'
          ? loadOwnerPdaAuthority(postingKey.trim(), controller.signal)
          : Promise.resolve<OwnerPdaAuthorityResult>({
              estimate: null,
              creationTransaction: null,
              flowProof: null,
              error: null
            });
      const programPdaRequest =
        postingKind === 'program'
          ? loadProgramPdaAuthorities(postingKey.trim())
          : Promise.resolve<ProgramPdaAuthorityResult>({ estimates: [], error: null });
      const accountDetailsRequest =
        postingKind === 'owner'
          ? loadAccountDetails(postingKey.trim(), controller.signal)
          : Promise.resolve<AccountDetailsResult | null>(null);
      const [response, pdaAuthority, programPdaAuthority, accountDetails] = await Promise.all([
        getPostings(
          postingKind,
          postingKey.trim(),
          postingCursor.trim(),
          postingLimit.trim(),
          programInstructionScope,
          controller.signal
        ),
        pdaAuthorityRequest,
        programPdaRequest,
        accountDetailsRequest
      ]);
      if (controller.signal.aborted) return;
      searchState =
        response &&
        (response.items.length > 0 ||
          pdaAuthority.estimate !== null ||
          accountDetails?.portfolio != null ||
          accountDetails?.portfolioHistory != null ||
          programPdaAuthority.estimates.length > 0 ||
          hasProgramLinkedHolder(postingKey.trim()))
          ? {
              status: 'postings',
              response,
              pdaAuthorityEstimate: pdaAuthority.estimate,
              pdaCreationTransaction: pdaAuthority.creationTransaction,
              pdaFlowProof: pdaAuthority.flowProof,
              programPdaEstimates: programPdaAuthority.estimates,
              pdaAuthorityError: pdaAuthority.error ?? programPdaAuthority.error,
              accountDetails
            }
          : { status: 'empty', detail: 'No transactions were found for this address.' };
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') return;
      if (error instanceof SearchApiError) {
        searchState = {
          status: error.unavailable ? 'unavailable' : 'error',
          message: error.message
        };
      } else {
        searchState = { status: 'error', message: 'The search failed with an unexpected client error.' };
      }
    } finally {
      if (activeController === controller) activeController = undefined;
    }
  }

  async function loadAccountDetails(
    address: string,
    signal: AbortSignal
  ): Promise<AccountDetailsResult> {
    const historyAvailable = health?.postings.owner_balance_history === true;
    const portfolioAvailable = report.compact_build.authority_portfolios_available === true;
    const portfolioHistoryAvailable =
      report.compact_build.authority_portfolio_history_available === true;
    const [portfolioResult, portfolioHistoryResult, balanceHistoryResult, tradingSummary, provenTrades] = await Promise.all([
      portfolioAvailable
        ? getAuthorityPortfolio(address, expectedDataset.source_transaction_sha256, signal)
            .then((value) => ({ value, error: null }))
            .catch((error: unknown) => {
              if (error instanceof Error && error.name === 'AbortError') throw error;
              return { value: null, error: 'The current portfolio data could not be verified.' };
            })
        : Promise.resolve({ value: null, error: null }),
      portfolioHistoryAvailable
        ? getAuthorityPortfolioHistory(address, expectedDataset.source_transaction_sha256, signal)
            .then((value) => ({ value, error: null }))
            .catch((error: unknown) => {
              if (error instanceof Error && error.name === 'AbortError') throw error;
              return { value: null, error: 'The historical portfolio data could not be verified.' };
            })
        : Promise.resolve({ value: null, error: null }),
      historyAvailable
        ? getAccountBalanceHistory(address, 4_096, signal)
            .then((value) => ({ value, error: null }))
            .catch((error: unknown) => {
              if (error instanceof Error && error.name === 'AbortError') throw error;
              return { value: null, error: 'Exact holding history could not be loaded.' };
            })
        : Promise.resolve({ value: null, error: null }),
      getAccountTradingSummary(address, signal).catch((error: unknown) => {
        if (error instanceof Error && error.name === 'AbortError') throw error;
        return null;
      }),
      getAccountProvenTrades(address, { limit: 20 }, signal).catch((error: unknown) => {
        if (error instanceof Error && error.name === 'AbortError') throw error;
        return null;
      })
    ]);
    const portfolio = portfolioResult.value;
    let portfolioHistory = portfolioHistoryResult.value;
    let balanceHistory = balanceHistoryResult.value;
    const integrityMessages = [portfolioResult.error, portfolioHistoryResult.error].filter(
      (message): message is string => message !== null
    );
    if (portfolio && portfolioHistory && !historyFinalMatchesPortfolio(portfolioHistory, portfolio)) {
      portfolioHistory = null;
      integrityMessages.push(
        'Historical estimates do not match the current portfolio. The historical lines are hidden.'
      );
    }
    if (portfolio && balanceHistory?.items.length) {
      const finalDirectBalance = balanceHistory.items.at(-1)?.post_raw_balance;
      if (finalDirectBalance !== portfolio.direct_public_balance.raw_amount) {
        balanceHistory = null;
        integrityMessages.push(
          'Exact balance history does not match the current holding. The direct line is hidden.'
        );
      }
    }
    return {
      portfolio,
      portfolioHistory,
      balanceHistory,
      tradingSummary,
      provenTrades,
      balanceHistoryMessage:
        balanceHistoryResult.error ??
        (historyAvailable ? null : 'Exact holding history is being prepared for this dataset.'),
      dataIntegrityMessage: integrityMessages.length > 0 ? integrityMessages.join(' ') : null
    };
  }

  function historyFinalMatchesPortfolio(
    history: AuthorityPortfolioHistorySeries,
    portfolio: AuthorityPortfolio
  ): boolean {
    const finalPoint = history.points.at(-1);
    return (
      finalPoint !== undefined &&
      finalPoint.direct_public_balance.raw_amount === portfolio.direct_public_balance.raw_amount &&
      finalPoint.estimated_defi_claim.raw_amount === portfolio.estimated_defi_claim.raw_amount &&
      finalPoint.estimated_total_exposure.raw_amount ===
        portfolio.estimated_total_exposure.raw_amount
    );
  }

  async function loadOwnerPdaAuthority(
    address: string,
    signal: AbortSignal
  ): Promise<OwnerPdaAuthorityResult> {
    try {
      const [estimate, flowProofResult] = await Promise.all([
        getPdaAuthorityEstimate(address, expectedDataset.source_transaction_sha256),
        getPdaFlowProof(address, expectedDataset.source_transaction_sha256)
          .then((flowProof) => ({ flowProof, error: null }))
          .catch((error: unknown) => ({
            flowProof: null,
            error:
              error instanceof Error ? error.message : 'The verified flow proof could not be loaded.'
          }))
      ]);
      if (!estimate) {
        return {
          estimate: null,
          creationTransaction: null,
          flowProof: flowProofResult.flowProof,
          error: flowProofResult.error
        };
      }
      try {
        const creationTransaction = await getTransactionById(
          estimate.creation_location.transaction_id,
          signal
        );
        if (!creationTransaction || !matchesCreationLocation(estimate, creationTransaction)) {
          return {
            estimate,
            creationTransaction: null,
            flowProof: flowProofResult.flowProof,
            error:
              flowProofResult.error ??
              'The indexed creation transaction did not match the PDA proof record.'
          };
        }
        return {
          estimate,
          creationTransaction,
          flowProof: flowProofResult.flowProof,
          error: flowProofResult.error
        };
      } catch (error) {
        if (error instanceof Error && error.name === 'AbortError') throw error;
        return {
          estimate,
          creationTransaction: null,
          flowProof: flowProofResult.flowProof,
          error:
            error instanceof Error
              ? error.message
              : 'The indexed creation transaction could not be loaded.'
        };
      }
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') throw error;
      return {
        estimate: null,
        creationTransaction: null,
        flowProof: null,
        error:
          error instanceof Error
            ? error.message
            : 'The PDA authority estimate could not be loaded.'
      };
    }
  }

  async function loadProgramPdaAuthorities(
    programId: string
  ): Promise<ProgramPdaAuthorityResult> {
    try {
      return {
        estimates: await getPdaAuthorityEstimatesByProgram(
          programId,
          expectedDataset.source_transaction_sha256
        ),
        error: null
      };
    } catch (error) {
      return {
        estimates: [],
        error:
          error instanceof Error
            ? error.message
            : 'Program-linked PDA estimates could not be loaded.'
      };
    }
  }

  function matchesCreationLocation(
    estimate: PdaAuthorityEstimate,
    response: TransactionLookupResponse
  ): boolean {
    const transaction = response.transaction;
    const location = estimate.creation_location;
    return (
      transaction.id === location.transaction_id &&
      transaction.coordinate.epoch === location.source_epoch &&
      transaction.coordinate.slot === location.slot &&
      transaction.coordinate.source_block_id === location.source_block_id &&
      transaction.coordinate.tx_index === location.tx_index
    );
  }

  function hasProgramLinkedHolder(programId: string): boolean {
    return (
      report.final_public_balance.holder_authority?.attributed_program_holders?.some(
        (holder) => holder.pda_program_id === programId
      ) ?? false
    );
  }

  function programLinkedPdas(
    programId: string,
    estimates: PdaAuthorityEstimate[]
  ): ProgramLinkedPda[] {
    const linked: ProgramLinkedPda[] = [];
    for (const holder of
      report.final_public_balance.holder_authority?.attributed_program_holders ?? []) {
      if (holder.pda_program_id !== programId) continue;
      const estimate = estimates.find((candidate) => candidate.subject_pda === holder.owner) ?? null;
      linked.push({
        address: holder.owner,
        ownerProgramName: holder.pda_program_name ?? null,
        exactPublicBalance: holder.public_balance,
        linkEvidence: holder.classification_evidence,
        estimate
      });
    }
    for (const estimate of estimates) {
      if (linked.some((candidate) => candidate.address === estimate.subject_pda)) continue;
      linked.push({
        address: estimate.subject_pda,
        ownerProgramName: estimate.runtime_owner_program_name,
        exactPublicBalance: estimate.direct_public_balance,
        linkEvidence:
          estimate.runtime_owner_program_id === programId
            ? 'committed_creation_owner_program'
            : 'direct_creation_caller',
        estimate
      });
    }
    return linked.sort((left, right) => left.address.localeCompare(right.address));
  }

  function readSearchRouteState(url: URL): SearchRouteState {
    const searchParams = url.searchParams;
    const explicitMode = prefilledSearchMode(searchParams.get('mode'));
    const signatureValue = prefilledSignature(searchParams.get('signature'));
    const transactionIdValue = prefilledTransactionId(searchParams.get('transaction_id'));
    const epochValue = prefilledUnsignedInteger(searchParams.get('epoch'));
    const slotValue = prefilledUnsignedInteger(searchParams.get('slot'));
    const sourceBlockIdValue = prefilledUnsignedInteger(searchParams.get('source_block_id'));
    const transactionIndexValue = prefilledUnsignedInteger(searchParams.get('tx_index'));
    const postingKindValue = prefilledPostingKind(searchParams.get('posting_kind'));
    const postingKeyValue = prefilledPostingKey(searchParams.get('posting_key'));
    const hasCoordinate =
      epochValue !== '' &&
      slotValue !== '' &&
      sourceBlockIdValue !== '' &&
      transactionIndexValue !== '';
    const hasLegacyPosting = postingKindValue !== null && postingKeyValue !== '';
    const inferredMode: SearchMode = hasLegacyPosting
      ? 'postings'
      : transactionIdValue !== ''
        ? 'id'
        : signatureValue !== ''
          ? 'signature'
          : hasCoordinate
            ? 'coordinate'
            : 'signature';
    const routeMode = explicitMode ?? inferredMode;
    const resolvedPostingKind = postingKindValue ?? 'target-address';
    const hasSearch =
      (routeMode === 'signature' && signatureValue !== '') ||
      (routeMode === 'id' && transactionIdValue !== '') ||
      (routeMode === 'coordinate' && hasCoordinate) ||
      (routeMode === 'postings' && postingKeyValue !== '');

    return {
      mode: routeMode,
      signature: signatureValue,
      transactionId: transactionIdValue,
      epoch: epochValue,
      slot: slotValue,
      sourceBlockId: sourceBlockIdValue,
      transactionIndex: transactionIndexValue,
      postingKind: resolvedPostingKind,
      postingKey: postingKeyValue,
      programInstructionScope: prefilledProgramInstructionScope(
        searchParams.get('instruction_scope')
      ),
      postingCursor: prefilledPostingCursor(searchParams.get('cursor')),
      postingLimit: prefilledPostingLimit(searchParams.get('limit')),
      hasSearch
    };
  }

  function currentSearchHref(): SearchHref {
    if (mode === 'signature') {
      return searchHref({ mode, signature: signature.trim() });
    }
    if (mode === 'id') {
      return searchHref({ mode, transaction_id: transactionId.trim() });
    }
    if (mode === 'coordinate') {
      return searchHref({
        mode,
        epoch: epoch.trim(),
        slot: slot.trim(),
        source_block_id: sourceBlockId.trim(),
        tx_index: transactionIndex.trim()
      });
    }
    return postingsSearchHref(
      postingKind,
      postingKey.trim(),
      postingCursor.trim(),
      postingLimit.trim(),
      programInstructionScope
    );
  }

  function searchModeHref(nextMode: SearchMode): SearchHref {
    return searchHref({ mode: nextMode });
  }

  function transactionSearchHref(id: number): SearchHref {
    return searchHref({ mode: 'id', transaction_id: String(id) });
  }

  function signatureSearchHref(signatureValue: string): SearchHref {
    return searchHref({ mode: 'signature', signature: signatureValue });
  }

  function ownerSearchHref(address: string): SearchHref {
    return postingsSearchHref('owner', address, '', '100', 'all');
  }

  function postingsSearchHref(
    kind: PostingKind,
    key: string,
    cursor: string,
    limit: string,
    instructionScope: ProgramInstructionScope
  ): SearchHref {
    return searchHref({
      mode: 'postings',
      posting_kind: kind,
      posting_key: key,
      ...(kind === 'program' ? { instruction_scope: instructionScope } : {}),
      ...(cursor ? { cursor } : {}),
      limit
    });
  }

  function searchHref(parameters: Record<string, string>): SearchHref {
    return `/search?${new URLSearchParams(parameters).toString()}`;
  }

  function validateInput(): string | null {
    if (!backendReady) return backendGateMessage();
    if (mode === 'signature') {
      return signature.trim() ? null : 'Enter a transaction signature.';
    }
    if (mode === 'id') {
      return isNonNegativeSafeInteger(transactionId)
        ? null
        : 'Enter an index record ID from 0 to 9,007,199,254,740,991.';
    }
    if (mode === 'coordinate') {
      const values = [epoch, slot, sourceBlockId, transactionIndex];
      return values.every(isUnsignedInteger)
        ? null
        : 'Enter epoch, slot, archive block record, and transaction index as non-negative integers.';
    }
    if (!postingAvailable) return 'Transaction history is not ready for this address type.';
    if (!postingKey.trim()) return 'Enter a mint, token account, program ID, or wallet owner address.';
    if (!isPostingPageLimit(postingLimit)) {
      return `Enter a result limit from 1 to ${postingPageLimit}.`;
    }
    return null;
  }

  function backendGateMessage(): string {
    if (backendBinding?.status === 'mismatch') {
      return 'The backend dataset does not match this report. All searches are disabled.';
    }
    if (backendBinding?.status === 'incomplete') {
      return 'The matching backend index is incomplete. All searches are disabled.';
    }
    return 'The backend dataset identity is not verified. All searches are disabled.';
  }

  function isUnsignedInteger(value: string): boolean {
    return /^[0-9]+$/.test(value.trim());
  }

  function isNonNegativeSafeInteger(value: string): boolean {
    const trimmed = value.trim();
    return /^(0|[1-9][0-9]*)$/.test(trimmed) && Number.isSafeInteger(Number(trimmed));
  }

  function isPostingPageLimit(value: string): boolean {
    const trimmed = value.trim();
    return (
      /^[1-9][0-9]*$/.test(trimmed) &&
      Number.isSafeInteger(Number(trimmed)) &&
      Number(trimmed) <= postingPageLimit
    );
  }

  function prefilledSignature(value: string | null): string {
    const signatureValue = value?.trim() ?? '';
    return /^[1-9A-HJ-NP-Za-km-z]{80,100}$/.test(signatureValue) ? signatureValue : '';
  }

  function prefilledTransactionId(value: string | null): string {
    const candidate = value?.trim() ?? '';
    if (!/^\d+$/.test(candidate)) return '';
    const parsed = Number(candidate);
    return Number.isSafeInteger(parsed) && parsed >= 0 ? candidate : '';
  }

  function prefilledUnsignedInteger(value: string | null): string {
    return prefilledTransactionId(value);
  }

  function prefilledSearchMode(value: string | null): SearchMode | null {
    return value === 'signature' || value === 'id' || value === 'coordinate' || value === 'postings'
      ? value
      : null;
  }

  function prefilledPostingKind(value: string | null): PostingKind | null {
    return value === 'target-address' ||
      value === 'token-account' ||
      value === 'program' ||
      value === 'owner'
      ? value
      : null;
  }

  function prefilledPostingKey(value: string | null): string {
    const key = value?.trim() ?? '';
    return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(key) ? key : '';
  }

  function prefilledProgramInstructionScope(value: string | null): ProgramInstructionScope {
    return value === 'direct' || value === 'inner' || value === 'all' ? value : 'all';
  }

  function prefilledPostingCursor(value: string | null): string {
    const cursor = value?.trim() ?? '';
    return cursor.length <= 512 ? cursor : '';
  }

  function prefilledPostingLimit(value: string | null): string {
    const limit = value?.trim() ?? '';
    return isPostingPageLimit(limit) ? limit : '100';
  }

  function formatBlockTime(value: number | null): string {
    if (value === null) return '—';
    return `${new Date(value * 1000).toLocaleString('en-GB', {
      dateStyle: 'medium',
      timeStyle: 'medium',
      timeZone: 'UTC'
    })} UTC`;
  }

  function decodedBase64Bytes(value: string): number {
    if (!value) return 0;
    const padding = value.endsWith('==') ? 2 : value.endsWith('=') ? 1 : 0;
    return Math.max(0, Math.floor((value.length * 3) / 4) - padding);
  }

  function postingKeyLabel(kind: PostingKind): string {
    const labels: Record<PostingKind, string> = {
      'target-address': 'Mint or SPYx token account',
      'token-account': 'Token account',
      program: 'Program ID',
      owner: 'Wallet or PDA owner'
    };
    return labels[kind];
  }

  function postingResultLabel(kind: PostingKind): string {
    const labels: Record<PostingKind, string> = {
      'target-address': 'Transactions for this mint or token account',
      'token-account': 'Transactions for this token account',
      program: 'Transactions for this program',
      owner: 'SPYx token account activity for this owner'
    };
    return labels[kind];
  }

  function programInstructionScopeLabel(scope: ProgramInstructionScope): string {
    const labels: Record<ProgramInstructionScope, string> = {
      all: 'Direct or inner',
      direct: 'Direct (top-level)',
      inner: 'Inner (CPI)'
    };
    return labels[scope];
  }
</script>

<svelte:head>
  <title>Search SPYx transactions</title>
  <meta
    name="description"
    content="Search indexed SPYx transactions by signature, record ID, slot and transaction index, mint, token account, wallet owner, or program."
  />
</svelte:head>

<header class="topbar">
  <div>
    <h1>Search transactions</h1>
    <div class="address">API: {apiBase}</div>
  </div>
  <div class="controls">
    <a class="toolbar-button" href={resolve('/')}>
      <ArrowLeft size={16} strokeWidth={1.8} />
      <span>Overview</span>
    </a>
  </div>
</header>

<section class="panel backend-panel" aria-label="Search backend status">
  {#await healthRequest}
    <div class="backend-row" aria-live="polite">
      <strong>Backend</strong>
      <span class="muted">Checking availability…</span>
    </div>
  {:then backendHealth}
    {@const binding = bindSearchHealthToDataset(backendHealth, expectedDataset)}
    {#if binding.status === 'match'}
      <div class="backend-row" aria-live="polite">
        <strong>Backend available</strong>
        <span class="muted">
          {formatInteger(backendHealth.index.transactions)} indexed transactions · source
          <span class="mono" title={backendHealth.index.source_transaction_sha256}>
            {shortAddress(backendHealth.index.source_transaction_sha256)}
          </span>
          · exact report match · address history {backendHealth.postings.complete ? 'ready' : 'not available'}
        </span>
        <button class="quiet-button" type="button" onclick={refreshHealth} aria-label="Refresh backend status">
          <RefreshCw size={15} strokeWidth={1.8} />
          Refresh
        </button>
      </div>
    {:else if binding.status === 'mismatch'}
      <div class="backend-row unavailable" aria-live="assertive">
        <strong>Backend dataset mismatch</strong>
        <span>
          Searches are disabled. Expected {formatInteger(expectedDataset.transactions)} transactions and source
          <span class="mono" title={expectedDataset.source_transaction_sha256}>
            {shortAddress(expectedDataset.source_transaction_sha256)}
          </span>;
          backend has {formatInteger(backendHealth.index.transactions)} and
          <span class="mono" title={backendHealth.index.source_transaction_sha256}>
            {shortAddress(backendHealth.index.source_transaction_sha256)}
          </span>.
        </span>
        <button class="quiet-button" type="button" onclick={refreshHealth} aria-label="Refresh backend status">
          <RefreshCw size={15} strokeWidth={1.8} />
          Refresh
        </button>
      </div>
    {:else}
      <div class="backend-row unavailable" aria-live="assertive">
        <strong>Backend index incomplete</strong>
        <span>The dataset identity matches, but all searches stay disabled until the index is complete.</span>
        <button class="quiet-button" type="button" onclick={refreshHealth} aria-label="Refresh backend status">
          <RefreshCw size={15} strokeWidth={1.8} />
          Refresh
        </button>
      </div>
    {/if}
  {:catch}
    <div class="backend-row unavailable" aria-live="polite">
      <strong>Backend unavailable</strong>
      <span>The health check did not complete. Searches stay disabled until the dataset identity is verified.</span>
      <button class="quiet-button" type="button" onclick={refreshHealth}>
        <RefreshCw size={15} strokeWidth={1.8} />
        Retry
      </button>
    </div>
  {/await}
</section>

<section class="panel">
  <div class="search-tabs" role="tablist" aria-label="Search method">
    {#each modes as item (item.id)}
      <a
        href={resolve(searchModeHref(item.id))}
        role="tab"
        aria-selected={mode === item.id}
        class={mode === item.id ? 'active' : undefined}
      >
        {item.label}
      </a>
    {/each}
  </div>

  <form
    class="search-form"
    method="GET"
    action={resolve('/search')}
    onsubmit={(event) => {
      event.preventDefault();
      submitSearch();
    }}
  >
    <input type="hidden" name="mode" value={mode} />
    {#if mode === 'signature'}
      <label class="form-field wide-field">
        <span>Transaction signature</span>
        <input
          name="signature"
          autocomplete="off"
          spellcheck="false"
          placeholder="Base58 transaction signature"
          value={signature}
          oninput={(event) => {
            signature = event.currentTarget.value;
            clearDraftSearchResult();
          }}
        />
      </label>
    {:else if mode === 'id'}
      <label class="form-field wide-field">
        <span>Index record ID</span>
        <input
          name="transaction_id"
          inputmode="numeric"
          autocomplete="off"
          placeholder="0"
          value={transactionId}
          oninput={(event) => {
            transactionId = event.currentTarget.value;
            clearDraftSearchResult();
          }}
        />
      </label>
    {:else if mode === 'coordinate'}
      <label class="form-field">
        <span>Epoch</span>
        <input
          name="epoch"
          inputmode="numeric"
          autocomplete="off"
          placeholder="801"
          value={epoch}
          oninput={(event) => {
            epoch = event.currentTarget.value;
            clearDraftSearchResult();
          }}
        />
      </label>
      <label class="form-field">
        <span>Slot</span>
        <input
          name="slot"
          inputmode="numeric"
          autocomplete="off"
          placeholder="346066298"
          value={slot}
          oninput={(event) => {
            slot = event.currentTarget.value;
            clearDraftSearchResult();
          }}
        />
      </label>
      <label class="form-field">
        <span>Archive block record</span>
        <input
          name="source_block_id"
          inputmode="numeric"
          autocomplete="off"
          placeholder="0"
          value={sourceBlockId}
          oninput={(event) => {
            sourceBlockId = event.currentTarget.value;
            clearDraftSearchResult();
          }}
        />
      </label>
      <label class="form-field">
        <span>Transaction index</span>
        <input
          name="tx_index"
          inputmode="numeric"
          autocomplete="off"
          placeholder="0"
          value={transactionIndex}
          oninput={(event) => {
            transactionIndex = event.currentTarget.value;
            clearDraftSearchResult();
          }}
        />
      </label>
    {:else}
      <label class="form-field">
        <span>Search by</span>
        <select
          name="posting_kind"
          value={postingKind}
          onchange={(event) => changePostingKind(event.currentTarget.value as PostingKind)}
        >
          <option value="target-address">Mint or SPYx token account</option>
          <option value="token-account">Token account</option>
          <option value="program">Program ID</option>
          <option value="owner" disabled={!ownerPostingAvailable}>Wallet or PDA owner</option>
        </select>
      </label>
      {#if postingKind === 'program'}
        <label class="form-field">
          <span>Instruction scope</span>
          <select
            name="instruction_scope"
            value={programInstructionScope}
            onchange={(event) =>
              changeProgramInstructionScope(event.currentTarget.value as ProgramInstructionScope)}
          >
            <option value="all">Direct or inner</option>
            <option value="direct">Direct (top-level)</option>
            <option value="inner">Inner (CPI)</option>
          </select>
        </label>
      {/if}
      <label class="form-field wide-field postings-key">
        <span>{postingKeyLabel(postingKind)}</span>
        <input
          name="posting_key"
          autocomplete="off"
          spellcheck="false"
          placeholder="Base58 public key"
          value={postingKey}
          oninput={(event) => changePostingKey(event.currentTarget.value)}
        />
      </label>
      <label class="form-field">
        <span>Page cursor</span>
        <input
          name="cursor"
          autocomplete="off"
          placeholder="First page"
          value={postingCursor}
          oninput={(event) => {
            postingCursor = event.currentTarget.value;
            clearDraftSearchResult();
          }}
        />
      </label>
      <label class="form-field limit-field">
        <span>Results per page</span>
        <input
          name="limit"
          type="number"
          min="1"
          max={postingPageLimit}
          step="1"
          autocomplete="off"
          value={postingLimit}
          oninput={(event) => {
            postingLimit = event.currentTarget.value;
            clearDraftSearchResult();
          }}
        />
      </label>
    {/if}

    <div class="submit-row">
      <button
        class="submit-button"
        type="submit"
        disabled={searchState.status === 'loading' || !backendReady || (mode === 'postings' && !postingAvailable)}
      >
        {searchState.status === 'loading' ? 'Searching…' : 'Search'}
      </button>
    </div>
    {#if !backendReady}
      <p class="posting-capability-note" aria-live="polite">
        {backendGateMessage()}
      </p>
    {:else if mode === 'postings' && !postingAvailable}
      <p class="posting-capability-note" aria-live="polite">
        This address type stays disabled until the backend reports a complete matching index.
      </p>
    {/if}
  </form>
</section>

{#if searchState.status === 'loading'}
  <section class="panel result-state" aria-live="polite">
    <strong>Searching indexed transactions…</strong>
    <p>The result will use only data returned by the configured backend.</p>
  </section>
{:else if searchState.status === 'empty'}
  <section class="panel result-state" aria-live="polite">
    <strong>No result</strong>
    <p>{searchState.detail}</p>
  </section>
{:else if searchState.status === 'unavailable'}
  <section class="panel result-state error-state" aria-live="assertive">
    <strong>Backend unavailable</strong>
    <p>{searchState.message}</p>
  </section>
{:else if searchState.status === 'error'}
  <section class="panel result-state error-state" aria-live="assertive">
    <strong>Search error</strong>
    <p>{searchState.message}</p>
  </section>
{:else if searchState.status === 'transaction'}
  {@const transaction = searchState.response.transaction}
  <section class="panel">
    <div class="panel-toolbar">
      <h2>Transaction</h2>
      <span class="panel-toolbar-detail">ID {formatInteger(transaction.id)}</span>
    </div>
    <dl class="metadata-grid transaction-grid">
      <div>
        <dt>Epoch</dt>
        <dd>{formatInteger(transaction.coordinate.epoch)}</dd>
      </div>
      <div>
        <dt>Slot</dt>
        <dd>{formatInteger(transaction.coordinate.slot)}</dd>
      </div>
      <div>
        <dt>Archive block record</dt>
        <dd>{formatInteger(transaction.coordinate.source_block_id)}</dd>
      </div>
      <div>
        <dt>Transaction index</dt>
        <dd>{formatInteger(transaction.coordinate.tx_index)}</dd>
      </div>
      <div>
        <dt>Block time</dt>
        <dd>{formatBlockTime(transaction.block.block_time)}</dd>
      </div>
      <div>
        <dt>Block height</dt>
        <dd>{transaction.block.block_height === null ? '—' : formatInteger(transaction.block.block_height)}</dd>
      </div>
      <div>
        <dt>Parent slot</dt>
        <dd>{formatInteger(transaction.block.parent_slot)}</dd>
      </div>
      <div>
        <dt>Transactions in block</dt>
        <dd>{formatInteger(transaction.block.transaction_count)}</dd>
      </div>
      <div>
        <dt>Wire profile</dt>
        <dd class="mono">{transaction.source_wire_profile}</dd>
      </div>
      <div>
        <dt>Flags</dt>
        <dd>{formatInteger(transaction.flags)}</dd>
      </div>
      <div>
        <dt>Message bytes</dt>
        <dd>{formatInteger(decodedBase64Bytes(transaction.message_bytes_base64))}</dd>
      </div>
      <div>
        <dt>Metadata bytes</dt>
        <dd>{formatInteger(decodedBase64Bytes(transaction.metadata_bytes_base64))}</dd>
      </div>
    </dl>
  </section>

  <section class="panel">
    <div class="panel-toolbar">
      <h2>Signatures</h2>
      <span class="panel-toolbar-detail">{formatInteger(transaction.signatures.length)} stored</span>
    </div>
    <div class="signature-list">
      {#each transaction.signatures as item (item)}
        <div class="mono">{item}</div>
      {/each}
    </div>
  </section>

  <section class="panel">
    <div class="panel-toolbar">
      <h2>Accounts</h2>
      <span class="panel-toolbar-detail">
        {formatInteger(transaction.accounts.length)} resolved · canonical message order
      </span>
    </div>
    <div class="transaction-account-list" role="list" aria-label="Resolved transaction accounts">
      {#each transaction.accounts as account (account.account_index)}
        <div class="transaction-account-row" role="listitem">
          <span class="transaction-account-index">Account {formatInteger(account.account_index)}</span>
          <span class="mono transaction-account-address">{account.address}</span>
          <span class="transaction-account-registry">
            Registry {formatInteger(account.registry_id)}
          </span>
        </div>
      {/each}
    </div>
  </section>

  <section class="panel raw-panel">
    <details>
      <summary>Raw backend response</summary>
      <pre>{JSON.stringify(searchState.response, null, 2)}</pre>
    </details>
  </section>
{:else if searchState.status === 'postings'}
  {@const nextCursor = searchState.response.next_cursor}
  {@const linkedProgramPdas =
    searchState.response.kind === 'program'
      ? programLinkedPdas(searchState.response.key, searchState.programPdaEstimates)
      : []}
  {#if searchState.response.kind === 'program' && linkedProgramPdas.length > 0}
    <section class="panel">
      <div class="panel-toolbar">
        <div>
          <h2>Program-linked PDAs</h2>
          <span class="panel-toolbar-detail">
            {formatInteger(linkedProgramPdas.length)} found in the replay
          </span>
        </div>
      </div>
      <p class="history-scope">
        Replay attribution or creation evidence links these PDAs to this program. Select one to see
        the exact evidence and SPYx estimate.
      </p>
      <div class="linked-pda-list">
        {#each linkedProgramPdas as linkedPda (linkedPda.address)}
          <a
            class="linked-pda-row"
            href={resolve(ownerSearchHref(linkedPda.address))}
          >
            <span class="linked-pda-identity">
              <strong class="mono">{linkedPda.address}</strong>
              <span>
                {linkedPda.ownerProgramName ?? 'Unknown owner program'} ·
                {linkedPda.linkEvidence.replaceAll('_', ' ')}
                {#if linkedPda.estimate}
                  · creation transaction {formatInteger(linkedPda.estimate.creation_location.transaction_id)}
                {/if}
              </span>
            </span>
            <span class="linked-pda-value">
              <small>PDA custody at dump boundary</small>
              <strong>{formatBaseUnits(linkedPda.exactPublicBalance.base_units)} SPYx</strong>
            </span>
            <span class="linked-pda-value">
              <small>Signer-linked replay estimate</small>
              <strong>
                {linkedPda.estimate?.estimated_external_defi_claim
                  ? `${formatBaseUnits(linkedPda.estimate.estimated_external_defi_claim.base_units)} SPYx`
                  : 'Not combined'}
              </strong>
            </span>
          </a>
        {/each}
      </div>
    </section>
  {:else if searchState.response.kind === 'program' && searchState.pdaAuthorityError}
    <section class="panel result-state error-state" aria-live="polite">
      <strong>Program-linked PDAs unavailable</strong>
      <p>{searchState.pdaAuthorityError}</p>
    </section>
  {/if}
  {#if searchState.response.kind === 'owner' && searchState.accountDetails}
    <AccountPortfolioDetails
      address={searchState.response.key}
      portfolio={searchState.accountDetails.portfolio}
      portfolioHistory={searchState.accountDetails.portfolioHistory}
      balanceHistory={searchState.accountDetails.balanceHistory}
      tradingSummary={searchState.accountDetails.tradingSummary}
      provenTrades={searchState.accountDetails.provenTrades}
      balanceHistoryMessage={searchState.accountDetails.balanceHistoryMessage}
      dataIntegrityMessage={searchState.accountDetails.dataIntegrityMessage}
    />
  {/if}
  {#if searchState.response.kind === 'owner' && searchState.pdaAuthorityEstimate}
    {@const estimate = searchState.pdaAuthorityEstimate}
    {@const selectedCandidate = estimate.candidates.find(
      (candidate) => candidate.authority === estimate.selected_candidate_authority
    )}
    {@const creationTransaction = searchState.pdaCreationTransaction?.transaction ?? null}
    {@const creationSignature = creationTransaction?.signatures[0] ?? null}
    {@const flowProof = searchState.pdaFlowProof}
    <section class="panel portfolio-panel">
      <div class="panel-toolbar portfolio-toolbar">
        <div>
          <h2>Authority-linked PDA estimate</h2>
          <span class="panel-toolbar-detail">
            {estimate.runtime_owner_program_name ?? 'Unknown program'}
          </span>
        </div>
        <span class="evidence-badge">Heuristic, not a PDA position</span>
      </div>

      <dl class="portfolio-metrics">
        <div>
          <dt>PDA custody at dump boundary</dt>
          <dd>{formatBaseUnits(estimate.direct_public_balance.base_units)} SPYx</dd>
        </div>
        <div>
          <dt>Signer-linked replay estimate</dt>
          <dd>
            {estimate.estimated_external_defi_claim
              ? `${formatBaseUnits(estimate.estimated_external_defi_claim.base_units)} SPYx`
              : 'Not combined'}
          </dd>
        </div>
        <div>
          <dt>Combined replay estimate</dt>
          <dd>
            {estimate.estimated_total_exposure
              ? `${formatBaseUnits(estimate.estimated_total_exposure.base_units)} SPYx`
              : 'Not available'}
          </dd>
        </div>
      </dl>

      {#if selectedCandidate}
        <div class="authority-lead">
          <div>
            <span>Creation signer</span>
            <strong class="mono">{selectedCandidate.authority}</strong>
          </div>
          <p>
            The external amount belongs to this signer's flow estimate. It is not a Jupiter position
            owned by the PDA.
          </p>
        </div>

        {#if selectedCandidate.program_positions.length > 0}
          <div class="table-wrap portfolio-positions">
            <table>
              <thead>
                <tr>
                  <th>Program</th>
                  <th>Replay estimate</th>
                  <th>Net principal</th>
                  <th>Observed flows</th>
                </tr>
              </thead>
              <tbody>
                {#each selectedCandidate.program_positions as position (`${position.program_id ?? 'unknown'}-${position.estimated_claim.raw_amount}`)}
                  <tr>
                    <td data-label="Program">
                      <span class="program-name">
                        <strong>{position.program_name ?? 'Unknown program'}</strong>
                        <span class="program-id mono">
                          {position.program_id ?? 'Program ID not resolved'}
                        </span>
                        {#if position.custody_owners.length > 0}
                          <span class="custody-owner-label">
                            Custody owner{position.custody_owners.length === 1 ? '' : 's'}
                          </span>
                          {#each position.custody_owners as custodyOwner (custodyOwner)}
                            <a
                              class="custody-owner mono"
                              href="https://solscan.io/account/{custodyOwner}"
                              target="_blank"
                              rel="noreferrer"
                            >{custodyOwner}</a>
                          {/each}
                        {/if}
                      </span>
                    </td>
                    <td class="numeric" data-label="Replay estimate">
                      {formatBaseUnits(position.estimated_claim.base_units)} SPYx
                    </td>
                    <td class="numeric" data-label="Net principal">
                      {formatBaseUnits(position.candidate_net_principal.base_units)} SPYx
                    </td>
                    <td class="numeric" data-label="Observed flows">
                      {formatInteger(position.deposit_transaction_count)} in ·
                      {formatInteger(position.return_transaction_count)} out
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {:else}
          <div class="portfolio-empty">No external SPYx position was inferred for this signer.</div>
        {/if}
      {:else}
        <div class="authority-lead unresolved">
          <div>
            <span>Creation signer candidates</span>
            <strong>{formatInteger(estimate.candidates.length)}</strong>
          </div>
          <p>The candidates are ambiguous or shared with other PDAs, so no balance is combined.</p>
        </div>
      {/if}

      <details class="portfolio-evidence" open>
        <summary>Authority link proof</summary>
        <dl>
          <div>
            <dt>PDA</dt>
            <dd>
              <a
                class="mono"
                href="https://solscan.io/account/{estimate.subject_pda}"
                target="_blank"
                rel="noreferrer"
              >{estimate.subject_pda}</a>
            </dd>
          </div>
          <div>
            <dt>Owner program</dt>
            <dd>
              <span>{estimate.runtime_owner_program_name ?? 'Unknown program'}</span>
              <a
                class="mono proof-address"
                href="https://solscan.io/account/{estimate.runtime_owner_program_id}"
                target="_blank"
                rel="noreferrer"
              >{estimate.runtime_owner_program_id}</a>
            </dd>
          </div>
          {#if selectedCandidate}
            <div>
              <dt>Creation signer</dt>
              <dd>
                <a
                  class="mono"
                  href="https://solscan.io/account/{selectedCandidate.authority}"
                  target="_blank"
                  rel="noreferrer"
                >{selectedCandidate.authority}</a>
              </dd>
            </div>
          {/if}
          <div>
            <dt>Creation transaction</dt>
            <dd>
              {#if creationSignature}
                <a
                  class="mono"
                  href="https://solscan.io/tx/{creationSignature}"
                  target="_blank"
                  rel="noreferrer"
                >{creationSignature}</a>
              {:else}
                Signature unavailable
              {/if}
              <a
                class="inline-action"
                href={resolve(transactionSearchHref(estimate.creation_location.transaction_id))}
              >
                Open indexed transaction {formatInteger(estimate.creation_location.transaction_id)}
              </a>
            </dd>
          </div>
          <div>
            <dt>Creation slot</dt>
            <dd>{formatInteger(estimate.creation_location.slot)}</dd>
          </div>
          <div>
            <dt>Instruction</dt>
            <dd class="mono">{estimate.system_instruction}</dd>
          </div>
        </dl>
        {#if searchState.pdaAuthorityError}
          <p class="proof-warning">{searchState.pdaAuthorityError}</p>
        {/if}
        <p class="proof-caveat">
          This transaction proves the creation signer and owner program. It does not prove beneficial
          ownership. The external claim is a separate non-DEX flow estimate for that signer.
        </p>
      </details>

      {#if flowProof}
        <section class="flow-proof" aria-label="Verified fund-flow proof">
          <div class="flow-proof-header">
            <div>
              <h3>Verified two-way fund flow</h3>
              <p>Indexed transfers link the Piggy vault, its creation signer, and Jupiter.</p>
            </div>
            <span class="evidence-badge">Exact transfers</span>
          </div>

          <div class="position-observation">
            <div>
              <span>Jupiter position owner — not the PDA</span>
              <a
                class="mono proof-address"
                href="https://solscan.io/account/{flowProof.position_observation.position_owner}"
                target="_blank"
                rel="noreferrer"
              >{flowProof.position_observation.position_owner}</a>
            </div>
            <div class="position-values">
              <span>{flowProof.position_observation.supplied_spyx} SPYx supplied</span>
              <span>{flowProof.position_observation.borrowed_usdc} USDC borrowed</span>
              <small>Observed {flowProof.position_observation.observed_at_utc.replace('T', ' ').replace('Z', ' UTC')}</small>
            </div>
            <a
              class="inline-action external-action"
              href="https://jup.ag/lend/borrow/78/nfts/28"
              target="_blank"
              rel="noreferrer"
            >Open Jupiter position</a>
          </div>

          <details class="flow-proof-details" open>
            <summary>Full accounts</summary>
            <dl class="proof-account-list">
              {#each flowProof.accounts as account (account.address)}
                <div>
                  <dt>{account.role}</dt>
                  <dd>
                    <a
                      class="mono proof-address"
                      href="https://solscan.io/account/{account.address}"
                      target="_blank"
                      rel="noreferrer"
                    >{account.address}</a>
                    <span>{account.label}</span>
                  </dd>
                </div>
              {/each}
              <div>
                <dt>Jupiter position state</dt>
                <dd>
                  <a
                    class="mono proof-address"
                    href="https://solscan.io/account/{flowProof.position_observation.position_state}"
                    target="_blank"
                    rel="noreferrer"
                  >{flowProof.position_observation.position_state}</a>
                </dd>
              </div>
              <div>
                <dt>Jupiter position NFT</dt>
                <dd>
                  <a
                    class="mono proof-address"
                    href="https://solscan.io/token/{flowProof.position_observation.position_nft_mint}"
                    target="_blank"
                    rel="noreferrer"
                  >{flowProof.position_observation.position_nft_mint}</a>
                </dd>
              </div>
              <div>
                <dt>Jupiter vault</dt>
                <dd>
                  <a
                    class="mono proof-address"
                    href="https://solscan.io/account/{flowProof.position_observation.vault}"
                    target="_blank"
                    rel="noreferrer"
                  >{flowProof.position_observation.vault}</a>
                </dd>
              </div>
            </dl>
          </details>

          <details class="flow-proof-details" open>
            <summary>Indexed transaction signatures</summary>
            <div class="flow-transfer-list">
              {#each flowProof.transfers as transfer (transfer.transaction_id)}
                <article class="flow-transfer">
                  <div class="flow-transfer-title">
                    <strong>{transfer.direction}</strong>
                    <span>{formatBaseUnits(transfer.amount.base_units)} SPYx</span>
                  </div>
                  <a
                    class="mono proof-address"
                    href="https://solscan.io/tx/{transfer.signature}"
                    target="_blank"
                    rel="noreferrer"
                  >{transfer.signature}</a>
                  <dl>
                    <div>
                      <dt>From</dt>
                      <dd class="mono">{transfer.source_token_account}</dd>
                    </div>
                    <div>
                      <dt>To</dt>
                      <dd class="mono">{transfer.destination_token_account}</dd>
                    </div>
                    <div>
                      <dt>Authority</dt>
                      <dd class="mono">{transfer.authority}</dd>
                    </div>
                    <div>
                      <dt>Program</dt>
                      <dd class="mono">{transfer.invoked_program_id}</dd>
                    </div>
                  </dl>
                  <div class="flow-transfer-footer">
                    <span>Slot {formatInteger(transfer.slot)} · {formatBlockTime(transfer.block_time_unix_seconds)}</span>
                    <a
                      class="inline-action"
                      href={resolve(transactionSearchHref(transfer.transaction_id))}
                    >Open indexed transaction {formatInteger(transfer.transaction_id)}</a>
                  </div>
                </article>
              {/each}
            </div>
          </details>

          <p class="proof-caveat">{flowProof.conclusion}</p>
        </section>
      {/if}
    </section>
  {:else if searchState.response.kind === 'owner' && searchState.pdaAuthorityError}
    <section class="panel result-state error-state" aria-live="polite">
      <strong>PDA estimate unavailable</strong>
      <p>{searchState.pdaAuthorityError}</p>
    </section>
  {/if}
  <section class="panel">
    <div class="panel-toolbar">
      <h2>{postingResultLabel(searchState.response.kind)}</h2>
      <span class="panel-toolbar-detail">
        {#if searchState.response.kind === 'program'}
          {programInstructionScopeLabel(searchState.response.instruction_scope)} ·
        {/if}
        {formatInteger(searchState.response.items.length)} returned · {formatInteger(searchState.response.total)} total
        · offset {formatInteger(searchState.response.offset)}
      </span>
    </div>
    <div class="posting-key-value mono">{searchState.response.key}</div>
    {#if searchState.response.kind === 'owner'}
      <p class="history-scope">
        These transactions are linked through SPYx token accounts owned by this address. This is not full
        wallet activity or signer history.
      </p>
    {:else if searchState.response.kind === 'program'}
      <p class="history-scope">
        Direct means the transaction message invokes this program at the top level. Inner means another
        program invokes it through CPI. A transaction that has both is returned once in the combined scope.
      </p>
    {/if}
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Index record ID</th>
            <th>Epoch</th>
            <th>Slot</th>
            <th>Archive block record</th>
            <th>Transaction index</th>
            <th>First signature</th>
          </tr>
        </thead>
        <tbody>
          {#each searchState.response.items as item (`${item.transaction_id}-${item.coordinate.epoch}-${item.coordinate.source_block_id}-${item.coordinate.tx_index}`)}
            <tr>
              <td class="numeric">
                <a class="transaction-link" href={resolve(transactionSearchHref(item.transaction_id))}>
                  {formatInteger(item.transaction_id)}
                </a>
              </td>
              <td class="numeric">{formatInteger(item.coordinate.epoch)}</td>
              <td class="numeric">{formatInteger(item.coordinate.slot)}</td>
              <td class="numeric">{formatInteger(item.coordinate.source_block_id)}</td>
              <td class="numeric">{formatInteger(item.coordinate.tx_index)}</td>
              <td class="mono" title={item.first_signature ?? undefined}>
                {#if item.first_signature}
                  <a
                    class="transaction-link"
                    href={resolve(signatureSearchHref(item.first_signature))}
                  >{shortAddress(item.first_signature)}</a>
                {:else}
                  —
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
    {#if nextCursor}
      <div class="next-page-row">
        <span class="muted">More transactions are available.</span>
        <a
          class="quiet-button"
          href={resolve(
            postingsSearchHref(
              searchState.response.kind,
              searchState.response.key,
              nextCursor,
              String(searchState.response.limit),
              searchState.response.kind === 'program'
                ? searchState.response.instruction_scope
                : 'all'
            )
          )}
        >
          Load next page
        </a>
      </div>
    {/if}
  </section>

  <section class="panel raw-panel">
    <details>
      <summary>Raw backend response</summary>
      <pre>{JSON.stringify(searchState.response, null, 2)}</pre>
    </details>
  </section>
{/if}

<style>
  .backend-panel {
    min-height: 46px;
  }

  .backend-row {
    min-height: 44px;
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 8px 12px;
  }

  .backend-row > span {
    min-width: 0;
    flex: 1;
  }

  .backend-row.unavailable {
    color: var(--warn);
    background: var(--warn-weak);
  }

  .search-tabs {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    border-bottom: 1px solid var(--border);
    background: #fbfbfc;
  }

  .search-tabs a {
    min-width: 0;
    padding: 10px 14px;
    border: 0;
    border-right: 1px solid var(--border);
    border-bottom: 2px solid transparent;
    color: var(--muted);
    background: transparent;
    text-align: center;
    text-decoration: none;
  }

  .search-tabs a:hover {
    color: var(--text);
    background: var(--surface-muted);
  }

  .search-tabs a.active {
    border-bottom-color: var(--accent);
    color: var(--text);
    background: var(--surface);
  }

  .search-form {
    display: grid;
    grid-template-columns: repeat(4, minmax(130px, 1fr));
    gap: 12px;
    padding: 14px;
  }

  .form-field {
    min-width: 0;
    display: grid;
    gap: 5px;
    color: var(--muted);
    font-size: 12px;
  }

  .form-field input,
  .form-field select {
    width: 100%;
    min-width: 0;
    height: 36px;
    padding: 0 9px;
    border: 1px solid var(--border-strong);
    border-radius: 6px;
    color: var(--text);
    background: var(--surface);
  }

  .form-field input:focus,
  .form-field select:focus {
    border-color: var(--accent);
    outline: 2px solid var(--accent-weak);
    outline-offset: 1px;
  }

  .wide-field {
    grid-column: span 3;
  }

  .postings-key {
    grid-column: span 2;
  }

  .limit-field {
    max-width: 130px;
  }

  .submit-row {
    grid-column: 1 / -1;
    display: flex;
    justify-content: flex-end;
  }

  .posting-capability-note {
    grid-column: 1 / -1;
    margin: -4px 0 0;
    color: var(--muted);
    font-size: 12px;
  }

  .submit-button,
  .quiet-button {
    min-height: 34px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 7px;
    padding: 0 12px;
    border: 1px solid var(--border-strong);
    border-radius: 6px;
    font: inherit;
  }

  .submit-button {
    min-width: 96px;
    border-color: #0a655e;
    color: #fff;
    background: var(--accent);
  }

  .submit-button:disabled {
    cursor: wait;
    opacity: 0.65;
  }

  .quiet-button {
    color: var(--text);
    background: var(--surface);
    text-decoration: none;
  }

  .quiet-button:hover {
    background: var(--surface-muted);
  }

  .transaction-link {
    padding: 0;
    border: 0;
    color: var(--accent);
    background: transparent;
    font: inherit;
    font-variant-numeric: tabular-nums;
    cursor: pointer;
    text-decoration: none;
  }

  .transaction-link:hover {
    text-decoration: underline;
  }

  .transaction-link:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }

  .result-state {
    padding: 14px;
  }

  .result-state p {
    margin: 4px 0 0;
    color: var(--muted);
  }

  .error-state {
    border-color: #edc9b5;
    color: var(--warn);
    background: var(--warn-weak);
  }

  .transaction-grid {
    grid-template-columns: repeat(4, minmax(130px, 1fr));
  }

  .transaction-grid > div:nth-child(4n) {
    border-right: 0;
  }

  .transaction-grid > div:nth-child(n + 5) {
    border-top: 1px solid var(--border);
  }

  .signature-list,
  .posting-key-value {
    display: grid;
    gap: 8px;
    padding: 12px;
    overflow-wrap: anywhere;
  }

  .signature-list > div + div {
    padding-top: 8px;
    border-top: 1px solid var(--border);
  }

  .transaction-account-list {
    display: grid;
  }

  .transaction-account-row {
    min-width: 0;
    display: grid;
    grid-template-columns: 110px minmax(280px, 1fr) 150px;
    align-items: baseline;
    gap: 12px;
    padding: 9px 12px;
    border-bottom: 1px solid var(--border);
  }

  .transaction-account-row:last-child {
    border-bottom: 0;
  }

  .transaction-account-index {
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
  }

  .transaction-account-address {
    min-width: 0;
    overflow-wrap: anywhere;
  }

  .transaction-account-registry {
    color: var(--muted);
    font-size: 12px;
    font-variant-numeric: tabular-nums;
    text-align: right;
  }

  .posting-key-value {
    border-bottom: 1px solid var(--border);
  }

  .history-scope {
    margin: 0;
    padding: 9px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--muted);
    font-size: 12px;
  }

  .linked-pda-list {
    display: grid;
  }

  .linked-pda-row {
    width: 100%;
    display: grid;
    grid-template-columns: minmax(300px, 1fr) minmax(150px, auto) minmax(170px, auto);
    align-items: center;
    gap: 18px;
    padding: 12px;
    border: 0;
    border-bottom: 1px solid var(--border);
    color: var(--text);
    background: var(--surface);
    text-align: left;
    text-decoration: none;
    cursor: pointer;
  }

  .linked-pda-row:last-child {
    border-bottom: 0;
  }

  .linked-pda-row:hover {
    background: var(--surface-muted);
  }

  .linked-pda-row:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: -2px;
  }

  .linked-pda-identity,
  .linked-pda-value {
    min-width: 0;
    display: grid;
    gap: 3px;
  }

  .linked-pda-identity strong {
    overflow-wrap: anywhere;
  }

  .linked-pda-identity span,
  .linked-pda-value small {
    color: var(--muted);
    font-size: 11px;
  }

  .linked-pda-value {
    text-align: right;
  }

  .portfolio-toolbar > div {
    min-width: 0;
    display: flex;
    align-items: baseline;
    gap: 10px;
  }

  .evidence-badge {
    flex: none;
    padding: 3px 7px;
    border: 1px solid #b8d8d4;
    border-radius: 999px;
    color: #0a655e;
    background: #eff8f6;
    font-size: 11px;
    font-weight: 650;
  }

  .portfolio-metrics {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    margin: 0;
    border-bottom: 1px solid var(--border);
  }

  .portfolio-metrics > div {
    min-width: 0;
    padding: 14px 16px;
    border-right: 1px solid var(--border);
  }

  .portfolio-metrics > div:last-child {
    border-right: 0;
  }

  .portfolio-metrics dt,
  .portfolio-evidence dt,
  .authority-lead span {
    color: var(--muted);
    font-size: 11px;
  }

  .portfolio-metrics dd {
    margin: 4px 0 0;
    font-size: clamp(16px, 2vw, 21px);
    font-weight: 650;
    font-variant-numeric: tabular-nums;
  }

  .authority-lead {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 18px;
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
    background: #fbfbfc;
  }

  .authority-lead > div {
    min-width: 0;
    display: grid;
    gap: 2px;
  }

  .authority-lead strong {
    overflow-wrap: anywhere;
  }

  .authority-lead p {
    max-width: 620px;
    margin: 0;
    color: var(--muted);
    font-size: 12px;
  }

  .authority-lead.unresolved {
    color: var(--warn);
    background: var(--warn-weak);
  }

  .portfolio-positions td:first-child {
    min-width: 220px;
  }

  .program-id {
    display: block;
    margin-top: 2px;
    color: var(--muted);
    font-size: 11px;
    overflow-wrap: anywhere;
  }

  .program-name {
    display: block;
  }

  .custody-owner-label {
    display: block;
    margin-top: 7px;
    color: var(--muted);
    font-size: 11px;
  }

  .custody-owner {
    display: block;
    margin-top: 2px;
    color: var(--accent);
    font-size: 11px;
    overflow-wrap: anywhere;
  }

  .portfolio-empty {
    padding: 14px 12px;
    border-bottom: 1px solid var(--border);
    color: var(--muted);
  }

  .portfolio-evidence {
    padding: 9px 12px;
  }

  .portfolio-evidence summary {
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
  }

  .portfolio-evidence dl {
    display: grid;
    gap: 8px;
    margin: 10px 0 2px;
  }

  .portfolio-evidence dl > div {
    display: grid;
    grid-template-columns: 110px minmax(0, 1fr);
    gap: 10px;
  }

  .portfolio-evidence dd {
    min-width: 0;
    margin: 0;
    overflow-wrap: anywhere;
  }

  .portfolio-evidence a {
    color: var(--accent);
  }

  .proof-address {
    display: block;
    margin-top: 2px;
  }

  .inline-action {
    display: block;
    margin-top: 5px;
    padding: 0;
    border: 0;
    color: var(--accent);
    background: transparent;
    font: inherit;
    cursor: pointer;
    text-decoration: none;
  }

  .inline-action:hover {
    text-decoration: underline;
  }

  .proof-warning,
  .proof-caveat {
    margin: 9px 0 0;
    color: var(--muted);
    font-size: 11px;
  }

  .proof-warning {
    color: var(--warn);
  }

  .flow-proof {
    border-top: 1px solid var(--border);
  }

  .flow-proof-header,
  .position-observation {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 18px;
    padding: 12px;
  }

  .flow-proof-header {
    border-bottom: 1px solid var(--border);
  }

  .flow-proof-header h3,
  .flow-proof-header p {
    margin: 0;
  }

  .flow-proof-header p {
    margin-top: 2px;
    color: var(--muted);
    font-size: 12px;
  }

  .position-observation {
    align-items: flex-start;
    border-bottom: 1px solid var(--border);
    background: #fbfbfc;
  }

  .position-observation > div {
    min-width: 0;
  }

  .position-observation > div:first-child {
    flex: 1;
  }

  .position-observation > div:first-child > span,
  .position-values small {
    display: block;
    color: var(--muted);
    font-size: 11px;
  }

  .position-observation a {
    color: var(--accent);
    overflow-wrap: anywhere;
  }

  .position-values {
    display: grid;
    gap: 2px;
    font-variant-numeric: tabular-nums;
    text-align: right;
  }

  .external-action {
    flex: none;
    margin-top: 0;
  }

  .flow-proof-details {
    padding: 10px 12px;
    border-bottom: 1px solid var(--border);
  }

  .flow-proof-details summary {
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
  }

  .proof-account-list {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0 22px;
    margin: 8px 0 0;
  }

  .proof-account-list > div {
    min-width: 0;
    padding: 8px 0;
    border-bottom: 1px solid var(--border);
  }

  .proof-account-list dt,
  .flow-transfer dt {
    color: var(--muted);
    font-size: 11px;
  }

  .proof-account-list dd,
  .flow-transfer dd {
    min-width: 0;
    margin: 2px 0 0;
    overflow-wrap: anywhere;
  }

  .proof-account-list dd span {
    display: block;
    margin-top: 2px;
    color: var(--muted);
    font-size: 11px;
  }

  .proof-account-list a,
  .flow-transfer > a {
    color: var(--accent);
  }

  .flow-transfer-list {
    display: grid;
    gap: 10px;
    margin-top: 10px;
  }

  .flow-transfer {
    min-width: 0;
    padding: 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: #fbfbfc;
  }

  .flow-transfer-title,
  .flow-transfer-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .flow-transfer-title span {
    font-variant-numeric: tabular-nums;
    font-weight: 650;
  }

  .flow-transfer > a {
    margin: 6px 0 8px;
    overflow-wrap: anywhere;
  }

  .flow-transfer dl {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 8px 18px;
    margin: 0;
  }

  .flow-transfer dl > div {
    min-width: 0;
  }

  .flow-transfer-footer {
    align-items: flex-end;
    margin-top: 9px;
    color: var(--muted);
    font-size: 11px;
  }

  .flow-transfer-footer .inline-action {
    flex: none;
    margin-top: 0;
    font-size: 11px;
  }

  .flow-proof > .proof-caveat {
    margin: 0;
    padding: 9px 12px;
  }

  .next-page-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 10px 12px;
    border-top: 1px solid var(--border);
  }

  .raw-panel details {
    padding: 10px 12px;
  }

  .raw-panel summary {
    cursor: pointer;
    font-weight: 600;
  }

  .raw-panel pre {
    max-height: 520px;
    margin: 12px -12px -10px;
    padding: 12px;
    overflow: auto;
    border-top: 1px solid var(--border);
    color: var(--text);
    background: #fbfbfc;
    font-size: 12px;
  }

  @media (max-width: 900px) {
    .search-form,
    .transaction-grid {
      grid-template-columns: 1fr 1fr;
    }

    .wide-field,
    .postings-key {
      grid-column: span 2;
    }

    .transaction-grid > div:nth-child(4n) {
      border-right: 1px solid var(--border);
    }

    .transaction-grid > div:nth-child(even) {
      border-right: 0;
    }

    .transaction-grid > div:nth-child(n + 3) {
      border-top: 1px solid var(--border);
    }

    .linked-pda-row {
      grid-template-columns: 1fr 1fr;
    }

    .linked-pda-identity {
      grid-column: 1 / -1;
    }

    .position-observation {
      flex-wrap: wrap;
    }
  }

  @media (max-width: 560px) {
    .backend-row {
      align-items: flex-start;
      flex-wrap: wrap;
    }

    .backend-row > span {
      flex-basis: 100%;
      order: 3;
    }

    .search-tabs {
      grid-template-columns: 1fr 1fr;
    }

    .search-tabs a {
      min-height: 44px;
      display: grid;
      place-items: center;
      padding-inline: 8px;
    }

    .search-form,
    .transaction-grid {
      grid-template-columns: 1fr;
    }

    .wide-field,
    .postings-key {
      grid-column: auto;
    }

    .transaction-grid > div,
    .transaction-grid > div:nth-child(4n),
    .transaction-grid > div:nth-child(even) {
      border-right: 0;
    }

    .transaction-grid > div:nth-child(n + 2) {
      border-top: 1px solid var(--border);
    }

    .transaction-account-row {
      grid-template-columns: 1fr auto;
      gap: 5px 12px;
      padding-block: 10px;
    }

    .transaction-account-address {
      grid-column: 1 / -1;
      grid-row: 2;
      font-size: 12px;
    }

    .limit-field {
      max-width: none;
    }

    .form-field input,
    .form-field select,
    .submit-button,
    .quiet-button {
      min-height: 44px;
    }

    .submit-button {
      width: 100%;
    }

    .portfolio-toolbar,
    .portfolio-toolbar > div,
    .authority-lead {
      align-items: flex-start;
    }

    .portfolio-toolbar > div,
    .authority-lead {
      flex-direction: column;
    }

    .portfolio-metrics {
      grid-template-columns: 1fr;
    }

    .portfolio-metrics > div {
      border-right: 0;
      border-bottom: 1px solid var(--border);
    }

    .portfolio-metrics > div:last-child {
      border-bottom: 0;
    }

    .portfolio-evidence dl > div {
      grid-template-columns: 1fr;
      gap: 2px;
    }

    .portfolio-positions table,
    .portfolio-positions tbody,
    .portfolio-positions tr,
    .portfolio-positions td {
      display: block;
      width: 100%;
      min-width: 0;
    }

    .portfolio-positions thead {
      display: none;
    }

    .portfolio-positions tr {
      padding: 8px 12px;
    }

    .portfolio-positions td,
    .portfolio-positions td:first-child {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 14px;
      padding: 5px 0;
      border: 0;
      text-align: right;
      white-space: normal;
    }

    .portfolio-positions td::before {
      content: attr(data-label);
      flex: none;
      color: var(--muted);
      font-size: 11px;
      font-weight: 400;
      text-align: left;
    }

    .portfolio-positions .program-name {
      min-width: 0;
      text-align: right;
    }

    .linked-pda-row {
      grid-template-columns: 1fr;
      gap: 10px;
    }

    .linked-pda-identity {
      grid-column: auto;
    }

    .linked-pda-value {
      text-align: left;
    }

    .flow-proof-header,
    .position-observation,
    .flow-transfer-title,
    .flow-transfer-footer {
      align-items: flex-start;
      flex-direction: column;
    }

    .position-values {
      text-align: left;
    }

    .proof-account-list,
    .flow-transfer dl {
      grid-template-columns: 1fr;
    }

    .proof-account-list > div:last-child {
      border-bottom: 0;
    }
  }
</style>
