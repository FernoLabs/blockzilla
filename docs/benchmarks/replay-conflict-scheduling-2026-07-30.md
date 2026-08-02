# Replay conflict scheduling — epoch 73

This experiment measures the transaction-account dependency graph that a
trusted-history replay executor can use inside each slot. It reads Blockzilla
Compact Archive V2 directly. It does not use CAR input, execute programs,
mutate Bank state, or predict SBF wall time.

## Implementation

`conflict_schedule` keeps a dense frontier per slot-local account and emits
only the dependencies required to preserve canonical conflict order:

- RAW: a reader follows the preceding writer;
- WAR: a writer follows every reader since the preceding writer;
- WAW: a writer follows the preceding writer when no intervening reader
  frontier already preserves that order; and
- RAR: no edge.

Duplicate account metas are folded with writable access taking precedence.
The finalized graph uses flat predecessor/successor arrays. A deterministic
critical-path-first list scheduler simulates bounded worker pools. Slots remain
strict barriers, and legacy loader transactions conservatively add a global
barrier within their slot.

The Compact benchmark retains one slot graph at a time, rejects V0 messages
whose loaded addresses are unavailable, includes archived failed transactions,
and fails if decoded pubkeys unexpectedly escape the sealed registry. Static
unit and top-level-instruction weights are deliberately labelled structural.

Correctness coverage includes directed RAW/WAR/WAW examples, duplicate-meta
and frontier-clearing cases, fork/join metrics, simultaneous completions, and
512 deterministic randomized comparisons of the minimal graph's transitive
closure against the complete canonical conflict graph. Four worker counts are
also checked for deterministic scheduling invariants in every randomized case.

## NAS run

- Host: Intel Core i5-1235U, 12 logical CPUs, 7.5 GiB RAM
- Build: Rust 1.96 release, `-C target-cpu=native`
- Benchmark pinned to logical CPU 2
- Binary SHA-256:
  `6dea443634642b8da79f6ff0366b271498c82de2040eaf58849f17fdb6453fda`
- Run identifier: `conflict-epoch73-v2-20260730T214228Z`
- Cache state: warm after the required input hash and 30,000-slot gate
- Before/after Compact input-tree manifest SHA-256:
  `54589416704b679c3d234bd6280f354f3313e52ea5392f74b52d5d74c59d9e2d`

The 30,000-slot gate covered 4,307,414 transactions and passed with zero raw
registry fallbacks. The complete run then covered exactly:

| Metric | Result |
|---|---:|
| Present slots | 357,671 |
| Transactions | 50,654,495 |
| Top-level instructions | 61,213,911 |
| Compressed Compact bytes | 1,676,685,399 |
| Uncompressed bytes | 8,011,549,341 |
| Scan wall time | 85.438 s |
| Present slots/s | 4,186.3 |
| Transactions/s | 592,877 |
| Compressed GB/s | 0.019624 |
| Sampled average CPU | 92.824% of one pinned CPU |
| Sampled average I/O wait | 0.235% |
| Sampled maximum RSS | 133,472 KiB (130.3 MiB) |
| Missing registry keys | 0 |

The online access projection plus graph construction took 27.254 s, or 538.0
ns/transaction. Projection dominated at 23.530 s; graph construction itself
took 3.724 s. Simulating all five worker counts was offline analysis and added
19.051 s. It is not production scheduler overhead.

The graph contained 30,072,850 unique transaction dependencies over
142,009,071 readonly and 146,021,391 writable accesses. The largest finalized
slot graph was only 31,864 logical bytes. There were 23,886 loader-barrier
transactions (0.0472% of transactions), 43,580,982 successful transactions,
and 7,073,513 archived failures.

## Structural concurrency

Unit-weight results are:

| Workers | Structural speedup | Utilization | Makespan / lower-bound gap |
|---:|---:|---:|---:|
| 1 | 1.000x | 100.0% | 0% |
| 2 | 1.975x | 98.8% | 0.095% |
| 4 | 3.539x | 88.5% | 0.168% |
| 8 | 4.818x | 60.2% | 0.100% |
| 12 | 5.154x | 42.9% | 0.051% |

Aggregate work divided by the per-slot critical paths gives a conservative
unlimited-worker ceiling of 5.540x. The list schedule is already within 0.17%
of its worker-count lower bound, so a more elaborate static scheduling
algorithm has little room to improve this unit-weight graph. More workers help
through eight, but utilization drops sharply after four.

Median slots have 135 transactions, 94 initially ready transactions, a
33-transaction longest dependency chain, and 5.324x unit-weight parallelism.
The p90 longest chain is 40; the maximum is 260. WAW constraints dominate the
frontier counts, consistent with the epoch's highly repeated writable accounts
and 40,452,602 Vote-program instructions.

## Conclusion and executor layout

Keep archive and diff order canonical. Do not physically reorder the Compact
stream. Instead, build the conflict DAG while decoding each slot, dispatch
ready transactions to workers, and retain a slot barrier. Publish transaction
and instruction/PDA diffs through a canonical-index reorder buffer so parallel
completion never changes observable order.

Start the real executor A/B at 4, 6, and 8 workers. Four workers capture most
of the efficient structural gain; six is a likely NAS balance once one core is
reserved for decode/planning and another for ordered commit/diff emission.
Use versioned program-cache entries before removing the conservative loader
barrier.

The decisive next measurement is not another static cost proxy. Sample actual
sequential replay cycles or elapsed time per transaction/program, feed those
weights into the same graph, then implement a parallel transaction-overlay
executor. Acceptance requires exact final account-state hash and canonically
ordered diff-transcript parity against sequential replay, including failed
transactions and a loader deployment. Only that A/B can revise full-chain
replay ETA.
