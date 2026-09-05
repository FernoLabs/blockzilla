export function deriveProviderAccessComparison(report) {
  assertObject(report, 'history report');
  assertObject(report.source, 'history source');
  assertObject(report.audit, 'history audit');
  assertObject(report.rpc_request_model, 'RPC request model');
  assertArray(report.rpc_request_model.per_address, 'RPC request model per_address');

  const source = report.source;
  const audit = report.audit;
  const model = report.rpc_request_model;
  const rows = model.per_address;
  const integerFields = [
    'address_count',
    'mint_addresses',
    'token_account_addresses',
    'get_signatures_for_address_page_limit',
    'get_signatures_for_address_requests',
    'get_signatures_for_address_credit_page_size',
    'get_signatures_for_address_credit_pages',
    'returned_address_signature_rows',
    'duplicate_address_signature_rows_removed',
    'unique_get_transaction_calls',
    'total_rpc_requests'
  ];
  for (const field of integerFields) {
    assertNonNegativeSafeInteger(model[field], `RPC request model ${field}`);
  }
  assertNonNegativeSafeInteger(source.transactions, 'history source transactions');
  assertNonNegativeSafeInteger(
    source.discovered_token_accounts,
    'history source discovered token accounts'
  );
  assertNonNegativeSafeInteger(
    audit.address_signature_rows,
    'history audit address signature rows'
  );
  assertNonNegativeSafeInteger(
    audit.selected_transactions_without_target_address,
    'history audit selected transactions without a target address'
  );

  if (model.get_signatures_for_address_page_limit !== 1_000) {
    throw new Error('RPC request model page limit does not match its per-address field name');
  }
  if (model.get_signatures_for_address_credit_page_size !== 100) {
    throw new Error('RPC request model credit page size does not match its per-address field name');
  }
  if (rows.length !== model.address_count) {
    throw new Error('RPC request model address count does not match per-address rows');
  }
  if (model.mint_addresses !== 1) {
    throw new Error('RPC request model must contain exactly one mint address');
  }
  if (model.token_account_addresses !== source.discovered_token_accounts) {
    throw new Error('RPC request model token-account count does not match the history source');
  }
  if (model.address_count !== model.mint_addresses + model.token_account_addresses) {
    throw new Error('RPC request model address classes do not add up to the address count');
  }

  const addresses = new Set();
  const mintRows = [];
  let tokenAccountRows = 0;
  let returnedRows = 0;
  let signatureRequests = 0;
  let creditPages = 0;

  for (const [index, row] of rows.entries()) {
    assertObject(row, `RPC request model per_address row ${index}`);
    if (typeof row.address !== 'string' || row.address.length === 0) {
      throw new Error(`RPC request model per_address row ${index} has no address`);
    }
    if (addresses.has(row.address)) {
      throw new Error(`RPC request model has a duplicate per-address row for ${row.address}`);
    }
    addresses.add(row.address);

    if (row.kind === 'mint') mintRows.push(row);
    else if (row.kind === 'token_account') tokenAccountRows += 1;
    else throw new Error(`RPC request model per_address row ${index} has an unknown kind`);

    for (const field of [
      'returned_address_signature_rows',
      'get_signatures_for_address_requests_at_limit_1000',
      'get_signatures_for_address_credit_pages_at_100'
    ]) {
      assertNonNegativeSafeInteger(row[field], `RPC request model per_address row ${index} ${field}`);
    }

    const expectedSignatureRequests = ceilingDivision(
      row.returned_address_signature_rows,
      model.get_signatures_for_address_page_limit
    );
    if (row.get_signatures_for_address_requests_at_limit_1000 !== expectedSignatureRequests) {
      throw new Error(`RPC request model per_address row ${index} has an invalid request count`);
    }
    const expectedCreditPages = ceilingDivision(
      row.returned_address_signature_rows,
      model.get_signatures_for_address_credit_page_size
    );
    if (row.get_signatures_for_address_credit_pages_at_100 !== expectedCreditPages) {
      throw new Error(`RPC request model per_address row ${index} has an invalid credit-page count`);
    }

    returnedRows = safeAdd(returnedRows, row.returned_address_signature_rows, 'returned rows');
    signatureRequests = safeAdd(
      signatureRequests,
      row.get_signatures_for_address_requests_at_limit_1000,
      'signature requests'
    );
    creditPages = safeAdd(
      creditPages,
      row.get_signatures_for_address_credit_pages_at_100,
      'credit pages'
    );
  }

  if (mintRows.length !== 1 || mintRows[0].address !== source.mint) {
    throw new Error('RPC request model does not have one mint row for the SPYx mint');
  }
  if (tokenAccountRows !== model.token_account_addresses) {
    throw new Error('RPC request model token-account rows do not match the declared count');
  }
  assertSame(returnedRows, model.returned_address_signature_rows, 'RPC returned address rows');
  assertSame(signatureRequests, model.get_signatures_for_address_requests, 'RPC signature requests');
  assertSame(creditPages, model.get_signatures_for_address_credit_pages, 'RPC credit pages');
  assertSame(returnedRows, audit.address_signature_rows, 'RPC rows and history audit rows');

  const uniqueRows = model.returned_address_signature_rows - model.duplicate_address_signature_rows_removed;
  assertSame(uniqueRows, model.unique_get_transaction_calls, 'RPC unique transaction calls');
  assertSame(
    model.unique_get_transaction_calls,
    source.transactions,
    'RPC unique calls and selected transactions'
  );
  assertSame(
    safeAdd(
      model.get_signatures_for_address_requests,
      model.unique_get_transaction_calls,
      'complete RPC request total'
    ),
    model.total_rpc_requests,
    'RPC total requests'
  );
  if (report.bounded_selected_dump_scan_complete !== true) {
    throw new Error('Provider access comparison requires a complete selected dump scan');
  }
  if (audit.selected_transactions_without_target_address !== 0) {
    throw new Error('Provider access comparison requires target-address coverage for every transaction');
  }

  const mintRow = mintRows[0];
  if (mintRow.returned_address_signature_rows > source.transactions) {
    throw new Error('RPC mint row covers more transactions than the selected dump contains');
  }
  const mintMissed = source.transactions - mintRow.returned_address_signature_rows;
  const mintRequestTotal = safeAdd(
    mintRow.get_signatures_for_address_requests_at_limit_1000,
    mintRow.returned_address_signature_rows,
    'mint-only RPC request total'
  );

  return {
    basis: 'exact_selected_dump_address_rows; no provider price is modeled',
    mint_only: {
      addresses_queried: 1,
      selected_transactions_covered: mintRow.returned_address_signature_rows,
      selected_transactions_missed: mintMissed,
      complete_selected_dump_coverage: mintMissed === 0,
      get_signatures_for_address_requests:
        mintRow.get_signatures_for_address_requests_at_limit_1000,
      get_transaction_requests: mintRow.returned_address_signature_rows,
      modeled_request_total: mintRequestTotal
    },
    all_target_addresses: {
      addresses_queried: model.address_count,
      selected_transactions_covered: source.transactions,
      selected_transactions_missed: 0,
      complete_selected_dump_coverage: true,
      coverage_prerequisite: {
        required: true,
        historical_token_account_list_must_preexist: true,
        includes_closed_accounts: true,
        discoverable_from_mint_only_rpc: false
      },
      get_signatures_for_address_requests: model.get_signatures_for_address_requests,
      get_transaction_requests: model.unique_get_transaction_calls,
      modeled_request_total: model.total_rpc_requests
    },
    existing_verified_dump_scan: {
      selected_transactions_covered: source.transactions,
      selected_transactions_missed: 0,
      complete_selected_dump_coverage: true,
      provider_rpc_requests: 0,
      verified_source_files: 5
    }
  };
}

function ceilingDivision(value, divisor) {
  return value === 0 ? 0 : Math.floor((value - 1) / divisor) + 1;
}

function safeAdd(left, right, label) {
  const result = left + right;
  if (!Number.isSafeInteger(result)) throw new Error(`RPC request model ${label} exceeds safe integer range`);
  return result;
}

function assertSame(actual, expected, label) {
  if (actual !== expected) throw new Error(`${label} does not match`);
}

function assertNonNegativeSafeInteger(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${label} is not a non-negative safe integer`);
  }
}

function assertObject(value, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`Expected ${label} to be an object`);
  }
}

function assertArray(value, label) {
  if (!Array.isArray(value)) throw new Error(`Expected ${label} to be an array`);
}
