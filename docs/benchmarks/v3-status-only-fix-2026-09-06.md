# V3 status-only reader fix

## Cause

The 6 September local run stopped on V3 epoch 100 USDC after five successful
jobs. The error was `decoded V3 metadata has no loaded-addresses plane`.

The shared USDC request now asks for execution status to exclude known failed
transactions. It does not ask for instructions. V3 correctly selected outcomes
and token balances, but its transaction projector treated a status request as
an instruction request. It then required the unselected loaded-address and
inner-instruction planes. This is a request/decoder mismatch, not evidence of
missing archive data. Epoch 0 did not expose it because its metadata is absent.

## Change

- Add `CompactV2MetadataProjector::project_split_outcome`. It reads status from
  the borrowed outcome bytes and validates the complete outcome without
  creating loaded-address or CPI vectors.
- Add an explicit V3 status-only metadata state. Preserve unknown status for
  absent and raw metadata. Check typed outcome status against the row flags.
- Use count-only message projection when instructions and signers are not
  requested, even when execution status is requested.
- When the request excludes failed transaction details, preserve the failed
  header but skip public-key resolution and balance conversion for that row.
- Keep full instruction projection and count behavior unchanged.

No format change, archive repair, upload, or index rebuild is required.

## Account index scope

The current account postings are derived from messages and loaded addresses;
the candidate policies select signer wallets or reached programs. They are not
a token-balance-mint index. A mint in balance metadata need not be in the
transaction account list. Using only mint account postings for this balance
dump could omit valid records. USDC therefore scans the requested balance and
outcome planes; it does not load the loaded-address or inner-instruction planes.
Instruction examples can still need loaded-address IDs after index selection.

## Checks

- 47 V3 query tests pass, including status-only balance scans with one and three
  workers, known success/failure, and absent/raw metadata.
- 19 metadata projection tests pass, including outcome-only status equivalence,
  truncated outcomes, and trailing-byte rejection.
- NAS epoch 100 USDC passes in 7.753477 seconds (11,089,991.714 total TPS).
- NAS epoch 300 USDC passes in 69.805288 seconds (10,382,165.180 total TPS),
  with 30,224,439 output rows.
- Both NAS checks match the completed patched V2 runs in block/transaction
  totals, output schema, rows, byte length, completeness, coverage, and exact
  output bytes. Epoch 100 has zero USDC rows; epoch 300 checks real records.
  Both outputs are complete. V2 took 8.643509 and 76.445991 seconds respectively;
  these are single-run observations with uncontrolled OS cache state.

## Run locations

The failed run remains unchanged:
`/volume2/blockzilla-bench/results/all-v3-car-local-patched-20260906/`.

Focused checks:
`/volume2/blockzilla-bench/results/v3-statusfix-check-20260906/`.

Fixed binaries and source patches:
`/volume2/blockzilla-bench/control/v3-car-statusfix-20260906/`.

The replacement full run uses
`/volume2/blockzilla-bench/results/all-v3-car-local-statusfix-20260906/`.
Its order remains all 44 V3 jobs, then all 44 CAR jobs. V3 input and all outputs
are on SSD. CAR inputs for epochs 100, 200, 400, 500, 600, and 700 are on HDD;
the other five CAR sample inputs are on SSD. Each source is recorded in
`input-storage.json`. Stop on reader errors or V3/CAR comparison mismatches.
