import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { isDeepStrictEqual } from 'node:util';

const DEFAULT_RPC_URL = 'https://api.mainnet-beta.solana.com';
const options = parseArguments(process.argv.slice(2));
if (!options.replay || !options.output) {
  throw new Error(
    'Usage: node scripts/build-holder-runtime-owner-snapshot.mjs --replay PATH --output PATH [--rpc-url URL] [--labels-from PATH]'
  );
}

const replayPath = resolve(options.replay);
const outputPath = resolve(options.output);
const rpcUrl = options.rpcUrl ?? process.env.SOLANA_RPC_URL ?? DEFAULT_RPC_URL;
const replayBytes = await readFile(replayPath);
const replaySha256 = createHash('sha256').update(replayBytes).digest('hex');
const replay = JSON.parse(replayBytes.toString('utf8'));

if (replay.artifact_kind !== 'spyx_public_balance_instruction_replay') {
  throw new Error('The replay has an unexpected artifact kind');
}
if (replay.holder_authority?.complete !== true) {
  throw new Error('The replay does not contain a complete holder-authority report');
}

const selection = selectHolderAddresses(replay.holder_authority);
const preservedLabels = options.labelsFrom
  ? await loadLabels(resolve(options.labelsFrom))
  : new Map();
const batches = chunk(selection.addresses, 100);
const accounts = [];
let observedSlot = null;
let observedSlotMin = null;

for (const [batchIndex, addresses] of batches.entries()) {
  const response = await fetch(rpcUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: batchIndex + 1,
      method: 'getMultipleAccounts',
      params: [addresses, { commitment: 'finalized', encoding: 'base64' }]
    })
  });
  if (!response.ok) {
    throw new Error(`getMultipleAccounts batch ${batchIndex + 1} failed with HTTP ${response.status}`);
  }
  const body = await response.json();
  if (body.error) {
    throw new Error(
      `getMultipleAccounts batch ${batchIndex + 1} failed: ${JSON.stringify(body.error)}`
    );
  }
  const slot = body.result?.context?.slot;
  if (!Number.isSafeInteger(slot) || slot <= 0) {
    throw new Error(`getMultipleAccounts batch ${batchIndex + 1} has no valid context slot`);
  }
  observedSlot = observedSlot === null ? slot : Math.max(observedSlot, slot);
  observedSlotMin = observedSlotMin === null ? slot : Math.min(observedSlotMin, slot);
  const values = body.result?.value;
  if (!Array.isArray(values) || values.length !== addresses.length) {
    throw new Error(`getMultipleAccounts batch ${batchIndex + 1} returned the wrong row count`);
  }
  for (const [index, address] of addresses.entries()) {
    const value = values[index];
    const label = preservedLabels.get(address);
    if (value === null) {
      accounts.push({
        address,
        exists: false,
        runtime_owner_program_id: null,
        data_bytes: null,
        executable: null,
        observed_slot: slot,
        ...(label ?? {})
      });
      continue;
    }
    if (
      typeof value.owner !== 'string' ||
      typeof value.executable !== 'boolean' ||
      !Array.isArray(value.data) ||
      typeof value.data[0] !== 'string' ||
      value.data[1] !== 'base64'
    ) {
      throw new Error(`getMultipleAccounts returned an invalid account row for ${address}`);
    }
    accounts.push({
      address,
      exists: true,
      runtime_owner_program_id: value.owner,
      data_bytes: Buffer.from(value.data[0], 'base64').byteLength,
      executable: value.executable,
      observed_slot: slot,
      ...(label ?? {})
    });
  }
}

accounts.sort((left, right) =>
  left.address < right.address ? -1 : left.address > right.address ? 1 : 0
);
if (accounts.length !== selection.addresses.length) {
  throw new Error('The snapshot output changed holder inclusion');
}
const report = {
  schema_version: 1,
  artifact_kind: 'spyx_holder_authority_runtime_owner_snapshot',
  evidence_kind: 'solana_runtime_account_owner',
  cluster: 'mainnet-beta',
  rpc_method: 'getMultipleAccounts',
  rpc_endpoint: publicRpcEndpointLabel(rpcUrl, options.rpcEndpointLabel),
  observed_slot: observedSlot,
  observed_slot_min: observedSlotMin,
  observed_slot_max: observedSlot,
  selection_scope: selection.scope,
  selection: selection.description,
  source_replay_sha256: replaySha256,
  accounts
};

await writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`);
console.log(
  `Wrote ${accounts.length} runtime-owner observations at finalized slot ${observedSlot} to ${outputPath} (${selection.scope})`
);

function selectHolderAddresses(holderAuthority) {
  if (Array.isArray(holderAuthority.off_curve_unattributed_holders)) {
    const classTotal = holderAuthority.class_totals.find(
      (row) => row.authority_kind === 'off_curve_unattributed'
    );
    if (holderAuthority.off_curve_unattributed_holders.length !== classTotal?.holder_count) {
      throw new Error('The complete off-curve holder array does not match its class total');
    }
    assertFullOffCurveTotals(holderAuthority.off_curve_unattributed_holders, classTotal);
    return {
      scope: 'all_off_curve_unattributed_holders',
      description: 'All off_curve_unattributed holders in the source replay.',
      addresses: exactAddressSet(holderAuthority.off_curve_unattributed_holders)
    };
  }

  const balanceRows = holderAuthority.largest_25_by_class?.off_curve_unattributed;
  const activityRows = holderAuthority.largest_25_by_activity_by_class?.off_curve_unattributed;
  if (!Array.isArray(balanceRows) || !Array.isArray(activityRows)) {
    throw new Error('The replay has neither a complete nor an exposed off-curve holder selection');
  }
  const rowsByAddress = new Map();
  for (const row of [...balanceRows, ...activityRows]) {
    const previous = rowsByAddress.get(row.owner);
    if (previous && !isDeepStrictEqual(previous, row)) {
      throw new Error(`The replay has different exposed rows for holder ${row.owner}`);
    }
    rowsByAddress.set(row.owner, row);
  }
  return {
    scope: 'exposed_off_curve_unattributed_holder_rows',
    description:
      'Distinct off_curve_unattributed owner rows exposed by the pinned holder balance and activity lists.',
    addresses: [...rowsByAddress.keys()].sort()
  };
}

function assertFullOffCurveTotals(rows, classTotal) {
  let tokenAccountCount = 0;
  let rawBalance = 0n;
  for (const [index, row] of rows.entries()) {
    if (!Number.isSafeInteger(row.token_account_count) || row.token_account_count <= 0) {
      throw new Error(`Complete off-curve holder row ${index} has an invalid token-account count`);
    }
    if (!/^(0|[1-9][0-9]*)$/.test(row.public_balance?.raw_amount ?? '')) {
      throw new Error(`Complete off-curve holder row ${index} has an invalid public balance`);
    }
    tokenAccountCount += row.token_account_count;
    rawBalance += BigInt(row.public_balance.raw_amount);
  }
  if (tokenAccountCount !== classTotal.token_account_count) {
    throw new Error('The complete off-curve holder array has a mismatched token-account total');
  }
  if (rawBalance !== BigInt(classTotal.public_balance.raw_amount)) {
    throw new Error('The complete off-curve holder array has a mismatched public-balance total');
  }
}

function exactAddressSet(rows) {
  const addresses = [];
  const seen = new Set();
  for (const [index, row] of rows.entries()) {
    if (row?.authority_kind !== 'off_curve_unattributed') {
      throw new Error(`Complete off-curve holder row ${index} has the wrong authority kind`);
    }
    if (typeof row.owner !== 'string' || row.owner.length === 0 || seen.has(row.owner)) {
      throw new Error(`Complete off-curve holder row ${index} has an invalid or duplicate owner`);
    }
    seen.add(row.owner);
    addresses.push(row.owner);
  }
  return addresses.sort();
}

async function loadLabels(path) {
  const source = JSON.parse(await readFile(path, 'utf8'));
  if (!Array.isArray(source.accounts)) {
    throw new Error('The label source has no accounts array');
  }
  const labels = new Map();
  for (const account of source.accounts) {
    if (account.account_label === undefined && account.account_label_evidence === undefined) continue;
    if (
      typeof account.address !== 'string' ||
      typeof account.account_label !== 'string' ||
      account.account_label.length === 0 ||
      !account.account_label_evidence ||
      labels.has(account.address)
    ) {
      throw new Error('The label source contains invalid or duplicate label evidence');
    }
    labels.set(account.address, {
      account_label: account.account_label,
      account_label_evidence: account.account_label_evidence
    });
  }
  return labels;
}

function publicRpcEndpointLabel(rpcUrl, explicitLabel) {
  if (explicitLabel) return explicitLabel;
  if (rpcUrl === DEFAULT_RPC_URL) return DEFAULT_RPC_URL;
  const parsed = new URL(rpcUrl);
  return `${parsed.protocol}//${parsed.host}/[configured-endpoint]`;
}

function chunk(values, size) {
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function parseArguments(args) {
  const options = {};
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (
      argument !== '--replay' &&
      argument !== '--output' &&
      argument !== '--rpc-url' &&
      argument !== '--rpc-endpoint-label' &&
      argument !== '--labels-from'
    ) {
      throw new Error(`Unknown option: ${argument}`);
    }
    const value = args[index + 1];
    if (!value) throw new Error(`Missing value for ${argument}`);
    if (argument === '--replay') options.replay = value;
    if (argument === '--output') options.output = value;
    if (argument === '--rpc-url') options.rpcUrl = value;
    if (argument === '--rpc-endpoint-label') options.rpcEndpointLabel = value;
    if (argument === '--labels-from') options.labelsFrom = value;
    index += 1;
  }
  return options;
}
