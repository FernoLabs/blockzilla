# V2 extraction coverage investigation

## Read-rate audit resolved, 6 September

The high USDC logical-byte rates are explained in [the read-rate audit](usdc-read-rate-audit-2026-09-06.md). Epochs 700, 800 and 1000 exceed the 1 GiB full-registry limit and use small per-worker caches. Repeated registry reads are largely served by the operating system's page cache. Independent process read-call counters confirm the high logical volume, while sampled storage reads are about 427–444 MB/s. These rates must not be published as physical SSD throughput. No performance change or rerun was made.

## Follow-up: independent raw CAR and Helius check, 6 September

See [CAR inner-instruction audit](car-cpi-rpc-audit-2026-09-06.md). Nine affected transactions in eight blocks were checked independently of the Blockzilla decoders. Their raw metadata explicitly marks inner instructions as not recorded; no transaction or metadata continuation links were present. All 13,608 sampled node CID hashes match. Helius confirms the transaction identities and errors, with zero compute units in every selected transaction. For three epoch 1000 samples Helius returns empty lists where CAR and public RPC report absence. No provider supplies instruction content missing from CAR in these samples.

This narrows the likely issue to coverage classification for pre-execution failures, not lost DataFrames in the sampled records. It does not classify every affected transaction. The earlier automated label `SOURCE_GAPS_CONFIRMED` was too strong as a claim of missing extraction results: it established equal absence flags, not that instructions had executed and were lost. Whole-file hash checks for CAR epochs 0, 800, 900 and 1000 are separate and in progress; epoch 0 has matched. No recompaction or performance benchmark was started.

## Result at 02:54 CEST, 2026-09-06

The affected CAR copies and all five CAR/V2 comparison jobs finished at 02:51:06 CEST. Every job returned matching block/transaction totals, byte-for-byte equal workload output, and the same ordered coverage digest and incomplete-transaction count as V2. All five outputs remain explicitly incomplete; parity is not a claim of complete extraction.

| CAR check | Indeterminate transactions | CAR/V2 output and coverage |
| --- | ---: | --- |
| Epoch 0 USDC | 1,724,876 | Match |
| Epoch 0 Pump.fun | 1,724,876 | Match |
| Epoch 800 Pump.fun | 2,216 | Match |
| Epoch 900 Pump.fun | 2,901 | Match |
| Epoch 1000 Pump.fun | 15,663 | Match |

The CAR reader reports the same missing evidence as V2. These checks found no converter data loss for the selected workloads. Recompaction is not justified by these coverage warnings: it cannot recover evidence absent from CAR. No converter, V2 or V3 archive was changed, and no further benchmark started.

The remaining issue is how the examples and benchmark report source coverage, independently from reader execution and output parity. A raw-record audit can still test whether a shared interpretation of absent CPI data is unnecessarily conservative; the comparison alone does not settle that semantic question. The unusually high USDC logical-byte rate also remains unresolved.

Saved evidence: [CAR/V2 comparison summary](car-v2-coverage-check-2026-09-06-summary.tsv). NAS status: `SOURCE_GAPS_CONFIRMED`, 5/5 completed. The earlier investigation sections below describe the evidence available before this comparison.

## State

Benchmarks are stopped on user request. The full V2 SSD-input/SSD-output run completed 43 examples. Epoch 1000 FireWatch was interrupted; its partial output and logs are retained. No new reader benchmark, archive repair, or archive deletion was started.

## Confirmed evidence

| Epoch | Workload | Indeterminate transactions | Saved reason |
| --- | --- | ---: | --- |
| 0 | USDC | 1,724,876 | Token balances unavailable; no missing-mint cases |
| 0 | Pump.fun | 1,724,876 | Incomplete CPI coverage |
| 800 | Pump.fun | 2,216 | Incomplete CPI coverage |
| 900 | Pump.fun | 2,901 | Incomplete CPI coverage |
| 1000 | Pump.fun | 15,663 | Incomplete CPI coverage |

The Pump.fun detail counters report zero incomplete outer-instruction transactions and zero matching transactions without a primary signature. These are coverage gaps, not reader crashes or signature failures.

The existing V3 sparse coverage files were inspected without scanning archive payloads. Their headers, record lengths, reserved bytes and footer counts were checked. They contain:

| Epoch | Sparse records | Account state | CPI state |
| --- | ---: | --- | --- |
| 0 | 1,724,876 | Complete | MissingMetadata (2) |
| 800 | 2,216 | Complete | NotRecorded (1) |
| 900 | 2,901 | Complete | NotRecorded (1) |
| 1000 | 15,663 | Complete | NotRecorded (1) |

These counts agree with the V2 example reports. V3 already records the same classes of missing evidence, so this is not evidence of a new SSD-copy-only failure. This check does not prove equality of all transaction coordinates, or that the original CAR data contains the same gaps.

## Code path

- `crates/blockzilla-read-sdk/src/compact_query.rs`: metadata without an inner-instruction list maps to `CpiCoverage::NotRecorded`; absent metadata maps to `Unknown(MetadataAbsent)`. An empty recorded list is distinct from an absent list.
- `crates/blockzilla-example-workloads/src/pump.rs`: every transaction with non-complete CPI coverage contributes to the coverage count, even if no Pump.fun invocation is confirmed. Confirmed matches are still written.
- `crates/blockzilla-example-workloads/src/usdc.rs`: unavailable token balances contribute to incomplete coverage.
- `crates/blockzilla-example-workloads/src/output.rs`: the coverage tracker stores a count and a digest of coordinates/reasons, but does not keep diagnostic coordinate samples or a general reason histogram.
- `scripts/archive_sample_matrix.py`: output completeness is parsed, but `run_one` marks a non-crashing result PASS unless peer parity mismatches. A single-format run has no independent peer. PASS therefore did not establish complete extraction or cross-format correctness.

## What is not established

The original CAR metadata has not yet been compared at selected affected transaction coordinates. Thus it is not yet known whether later-epoch NotRecorded values are faithful source gaps, conversion errors, or an SDK interpretation that could safely be more precise. Do not turn absent CPI data into a complete empty list without source evidence.

## Required next checks before performance work

1. Resolve a few affected transaction ordinals from each later epoch to slot and transaction index using the existing block index.
2. Compare those exact original CAR metadata records with V2 and V3: metadata presence, raw fallback, inner-instruction presence, execution error and recorded inner instructions. Use bounded reads, not another full benchmark.
3. If the CAR has inner instructions that V2/V3 lost, fix the conversion and repair affected archives. If the CAR also lacks them, document the source limitation and determine whether a narrow, proven execution-status rule can establish that CPI was impossible.
4. Keep execution success, extraction completeness and independent parity as separate result fields. Incomplete extraction must not be presented as a correctness PASS or publication-ready result.
5. Add bounded diagnostic reason counts and transaction samples in SDK/diagnostic support, not bulky logic in the beginner examples. Verify the selected affected records before considering another benchmark.

Acceptance: every listed gap has an evidence-backed classification; no unknown CPI data is silently treated as empty; affected CAR/V2/V3 records agree or the conversion issue is repaired; benchmark reporting cannot hide incomplete output behind PASS.

The unusually high USDC logical-byte rates on epochs 700, 800 and 1000 remain a separate metrics issue. They are not physical disk throughput and must not be published as such.

## Evidence location

NAS results: `/volume2/blockzilla-bench/results/all-v2-ssd-in-out-20260905/`.

## Follow-up authorized and started at 21:33 CEST

The user authorized CAR copies and source checks before a conditional recompaction. Controller PID 530622 runs `/volume2/blockzilla-bench/control/all-samples-20260905/copy-and-check-affected-car.sh` on NAS. It copies raw CAR epochs 0, 800, 900 and 1000 plus their slot indexes (2,123,059,631,349 bytes total) to SSD. There was 5,562,273,931,264 bytes of free SSD space at preflight.

Log: `/volume2/blockzilla-bench/control/all-samples-20260905/car-coverage-copy-check.log`.

After the copies, it runs five correctness checks: USDC and Pump.fun on epoch 0; Pump.fun on epochs 800, 900 and 1000. It uses existing CAR binaries and compares the outputs byte-for-byte with V2, plus output coverage digests, counts and block/transaction totals. Results stay on SSD at `/volume2/blockzilla-bench/results/car-coverage-check-20260905/`. It separates incomplete-but-matching source evidence from complete extraction. No performance matrix is resumed.

The controller does not rebuild or overwrite an archive. If CAR/V2 mismatch, inspect the exact affected records and diagnose the converter before rebuilding to a separate destination. If both agree on absent source evidence, do not recompact merely to hide the coverage warning. No active heartbeat was created for this sequence; the previous automation update reported that the app automation had been deleted.

Saved workload details: `jobs/compact-v2/local/epoch-N/WORKLOAD/attempt-001/stdout.log`.

Existing V3 coverage: `/volume2/blockzilla-bench/archive/indexer-v3/N/archive-v2-standalone-account-postings-adaptive-v3.coverage`.
