#!/usr/bin/env node

import { createHash, randomBytes } from 'node:crypto';

const API_KEY_PREFIX = 'bz_live_';
const ID_PATTERN = /^[A-Za-z0-9_-]{1,64}$/;

function usage() {
  console.log(`Usage:
  npm run rpc-key:generate -- --key-id <stable-key-id> --customer-id <stable-customer-id> --label <label>

Example:
  npm run rpc-key:generate -- --key-id key_acme_main --customer-id customer_acme --label "Acme production"`);
}

function parseArgs(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--help' || arg === '-h') {
      usage();
      process.exit(0);
    }
    if (!arg.startsWith('--') || index + 1 >= argv.length) {
      throw new Error(`Unexpected argument: ${arg}`);
    }
    values.set(arg.slice(2), argv[index + 1]);
    index += 1;
  }
  return values;
}

function requireValue(values, name) {
  const value = values.get(name);
  if (!value) {
    throw new Error(`Missing --${name}`);
  }
  return value;
}

try {
  const values = parseArgs(process.argv.slice(2));
  const keyId = requireValue(values, 'key-id');
  const customerId = requireValue(values, 'customer-id');
  const label = requireValue(values, 'label');
  const unknown = [...values.keys()].filter(
    (name) => !['key-id', 'customer-id', 'label'].includes(name),
  );
  if (unknown.length > 0) {
    throw new Error(`Unknown option: --${unknown[0]}`);
  }
  if (!ID_PATTERN.test(keyId)) {
    throw new Error('--key-id must be 1-64 ASCII letters, digits, underscores, or hyphens');
  }
  if (!ID_PATTERN.test(customerId)) {
    throw new Error('--customer-id must be 1-64 ASCII letters, digits, underscores, or hyphens');
  }
  if (label.length > 128 || /[\u0000-\u001f\u007f]/u.test(label)) {
    throw new Error('--label must be 1-128 characters without control characters');
  }

  const apiKey = `${API_KEY_PREFIX}${randomBytes(32).toString('base64url')}`;
  const digest = createHash('sha256').update(apiKey, 'utf8').digest('hex');
  const record = JSON.stringify({
    keyId,
    customerId,
    label,
    status: 'enabled',
  });

  console.log('Generated a Blockzilla RPC API key. This helper made no remote changes.');
  console.log('');
  console.log('Raw key — shown once; give this to the customer and do not store it in KV:');
  console.log(apiKey);
  console.log('');
  console.log('KV namespace binding:');
  console.log('BZ_RPC_API_KEYS');
  console.log('KV key — SHA-256 digest of the raw key:');
  console.log(digest);
  console.log('KV JSON value:');
  console.log(record);
  console.log('');
  console.log(
    'Store the digest and JSON value through your reviewed Cloudflare admin or IaC workflow. Never paste the raw key into Wrangler config, source control, logs, or the KV value.',
  );
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  console.error('');
  usage();
  process.exitCode = 1;
}
