import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';

const appRoot = resolve(import.meta.dirname, '..');
const [proofReport, summary] = await Promise.all([
  readJson('static/data/spyx-pda-flow-proofs.json'),
  readJson('static/data/spyx-summary.json')
]);

test('PDA flow proofs are bound to the transaction dataset', () => {
  assert.equal(proofReport.schema_version, 1);
  assert.equal(proofReport.artifact_kind, 'spyx_pda_flow_proofs');
  assert.equal(
    proofReport.source_binding.transactions_sha256,
    summary.source.transactions_file.sha256
  );
});

test('PDA flow proofs keep full accounts and indexed transaction signatures', () => {
  assert.ok(proofReport.proofs.length > 0);
  for (const proof of proofReport.proofs) {
    assertPublicKey(proof.subject_pda);
    assertPublicKey(proof.owner_program_id);
    assertPublicKey(proof.creation_signer);
    assert.equal(proof.proves_direct_pda_position, false);
    assert.equal(proof.position_observation.subject_pda_position_found, false);
    assert.equal(proof.position_observation.position_owner, proof.creation_signer);

    const accounts = new Set();
    for (const account of proof.accounts) {
      assertPublicKey(account.address);
      assert.equal(accounts.has(account.address), false);
      accounts.add(account.address);
      assert.ok(account.role.length > 0);
    }

    let previousSlot = -1;
    const transactionIds = new Set();
    const signatures = new Set();
    for (const transfer of proof.transfers) {
      assert.ok(Number.isSafeInteger(transfer.transaction_id));
      assert.ok(transfer.transaction_id >= 0);
      assert.equal(transactionIds.has(transfer.transaction_id), false);
      transactionIds.add(transfer.transaction_id);
      assert.match(transfer.signature, /^[1-9A-HJ-NP-Za-km-z]{80,100}$/);
      assert.equal(signatures.has(transfer.signature), false);
      signatures.add(transfer.signature);
      assert.ok(transfer.slot >= previousSlot);
      previousSlot = transfer.slot;
      assert.ok(BigInt(transfer.amount.raw_amount) > 0n);
      assertPublicKey(transfer.source_token_account);
      assertPublicKey(transfer.destination_token_account);
      assertPublicKey(transfer.authority);
      assertPublicKey(transfer.invoked_program_id);
    }
  }
});

async function readJson(path) {
  return JSON.parse(await readFile(resolve(appRoot, path), 'utf8'));
}

function assertPublicKey(value) {
  assert.match(value, /^[1-9A-HJ-NP-Za-km-z]{32,44}$/);
}
