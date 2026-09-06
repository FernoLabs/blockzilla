# Advanced Compact V2 examples

The landing page has four beginner examples. This page keeps the tools used
for tests and performance reports separate from that first-use path.

## Other programs

- `read-compact-v2` is the ordered reader benchmark.
- `read-compact-v2-transactions` exports one common identity record for every
  transaction.
- `read-compact-v2-usdc-instructions` builds a bounded SQLite instruction
  ledger with token-account lifetime tracking.

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

See the [transaction exporter source](src/bin/read-compact-v2-transactions.rs)
for its command arguments and the
[sample reader matrix guide](../../docs/benchmarks/sample-reader-matrix.md)
for output comparison and coverage checks.
