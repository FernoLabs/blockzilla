# Archive reader integration — 6 September 2026

This is the historical sample-branch report at `c086be87`. For the later
refactor integration and current validation results, see the
[refactor merge review](../design/refactor-merge-review.md).

## Included work

The integration contains the three reader and benchmark commits after
`ffffcbe1`, the pending fixes in `1d949be4`, and the existing local-main
example cleanup in `545dfa0b`. The separate `refactor` worktree is not part of
this integration.

- V2 and V3 retain the shared projection buffers and ordered worker pipeline.
- The V2 USDC example permits one shared registry up to 3 GiB. The SDK default
  is unchanged.
- USDC and Pump.fun exclude known failures. Unknown status remains explicit.
  Their output versions are `BZUSDC02` and `BZPUMP02`. Count still includes all
  transactions and recorded inner instructions.
- V3 can read execution status and token balances without loading instruction
  metadata planes. The outcome parser checks its complete input.
- CAR omits unrequested failure details before key resolution and CPI
  projection. Strict full-detail requests still validate instruction order.
- CAR rejects unsupported DataFrame array shapes before borrowed iteration.
  The ordered reader does not gain a transaction CID lookup table.
- Benchmark scripts, source audits, local results, and allocation profiles are
  saved with the code. Historical reports describe their original run state;
  they are not claims that every later test has completed.

## Verification

- Shared workloads: 19 tests passed.
- Query SDK: 74 tests passed.
- V3 query adapter: 47 tests passed.
- CAR library with `query-sdk`: 102 tests passed.
- Benchmark runner: 11 tests passed.
- V2 query adapter: 32 tests passed; metadata projection: 19 tests passed.
- All binaries in the CAR, V2, and V3 example packages passed `cargo check --locked`.
- V2 read SDK full library suite: 151 passed, 13 failed.

The 13 failures are in `archive_integrity` and `archive_signatures`. They call
the published-manifest accessor on a local reader and panic with
`operator-trusted local readers have no published manifest`. The three files
that contain these paths (`reader.rs`, `archive_integrity.rs`, and
`archive_signatures.rs`) are byte-identical to `origin/main` at `ffffcbe1`.
The failure is not fixed by this integration. No manifest requirement was
added back. A later repair must use the common reader descriptor accessors
and retain manifest-free local reads.

Two CAR test fixtures were updated for the existing ordered-stream contract:
out-of-order transaction frames must fail, and repeated references must fail
on the collected-node count mismatch. No production reader behavior was
changed for these fixture updates.

## Remaining live checks

The latest epoch-100 Pump.fun failure-filter build has not completed a full
epoch parity check. Do not call that full-epoch validation complete.

The [epoch-300 raw versus Zstd level-3 SSD comparison](epoch-300-ssd-zstd-2026-09-06.md)
continues separately. It checks the real count example, not every workload.
It is not a gate for this source commit and push.
