# Advanced Compact V2 examples

The landing page has four beginner examples. This page keeps the tools used
for tests and performance reports separate from that first-use path.

## Other programs

- `read-compact-v2` is the ordered reader benchmark.
- `read-compact-v2-transactions` exports one common identity record for every
  transaction.
- `read-compact-v2-usdc-instructions` builds a bounded SQLite instruction
  ledger with token-account lifetime tracking.
- `read-compact-v2-usdc-indexed` writes optional compact recorded balances and
  a source-scoped account dictionary; `expand-usdc-indexed` checks and expands
  those files. See the [output guide](README.md#optional-compact-usdc-output).

The transaction exporter keeps its old positional command form because the
cross-format matrix runner uses it. The instruction-ledger tool has its own
bounded correctness command. These controls do not exist in the four primary
examples on the landing page.

## Result fields

The workload programs print setup, scan, and total time. They also print
transaction rate, logical source bytes, network bytes, cache bytes, local read
bytes, output size, and coverage SHA-256. Use these values only after the output
and coverage values match between formats. Compare output files byte-for-byte
outside the timed run.

Compact V2 uses one ordered source reader and parallel decode and projection.
The default worker count is the number of logical CPUs available to the
process. Use `--threads N` for a repeatable test.

Small scans use bounded worker-local registry caches. A dense or complete scan
can share one complete registry when it fits the SDK memory limit. The result
reports the selected registry mode and measured projected-memory high-water
values.

The pipeline reports `pipeline_max_in_flight_blocks`,
`pipeline_max_in_flight_transactions`, and
`pipeline_max_in_flight_declared_uncompressed_bytes`. These measure admitted
work through sink completion. They do not measure total process memory.

With the rolling pipeline, `pipeline_decode_project_wall_s` spans first
admission through last projection completion. `pipeline_projection_buffer_wait_s`
measures admission waits, and `pipeline_result_send_wait_s` is zero because
workers own reserved result slots. These fields have different meanings from
the older group-based implementation. Use total/scan time, worker sums, and
verified outputs for comparisons across that change. See the
[pipeline contract](../../docs/design/reader-pipeline-rolling-window.md).

See the [transaction exporter source](src/bin/read-compact-v2-transactions.rs)
for its command arguments and the
[sample reader matrix guide](../../docs/benchmarks/sample-reader-matrix.md)
for output comparison and coverage checks.
