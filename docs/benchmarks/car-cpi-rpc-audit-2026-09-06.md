# CAR inner-instruction audit — 6 September 2026

## Result

The nine selected transactions do not show lost inner instructions in the CAR reader. Their raw CAR metadata explicitly says that inner instructions were not recorded. All nine have transaction errors and zero compute units. Helius confirms their signatures, error types, and zero compute units.

This is a sample check, not a classification of every coverage warning. The separate whole-file SHA-256 check has now completed: all four SSD CAR files match their published Old Faithful digests.

## Independent raw-byte check

The diagnostic imports no Blockzilla decoder. It reads eight block ranges from the SSD CAR files, decodes CAR framing and CBOR arrays, decompresses the selected metadata with the system zstd tool, and reads protobuf fields directly.

- Read 10,005,070 bytes across eight blocks containing 9,551 transactions.
- Recomputed SHA-256 for all 13,608 CAR nodes in those ranges. Every digest matches its stored CID.
- Found no transaction-data or transaction-metadata `next` links in these blocks. There are no standalone DataFrame nodes in these ranges.
- Each selected transaction and metadata DataFrame is a five-field CBOR array. There is no sixth `next` field to overlook.
- Each selected metadata record has protobuf field 10 equal to 1 (`inner_instructions_none`), no field 5 inner-instruction records, field 11 equal to 1 (`log_messages_none`), and field 16 equal to 0 (compute units).
- CAR transaction indexes and signatures agree with the selected RPC transactions. All eight RPC block transaction counts match CAR.

## Helius results

The eight `getBlock` requests used the local Helius secret. Requests were sequential, with a minimum one-second delay after each request. No key or authenticated URL is included in the evidence.

| Epoch | Slot | Transaction index | Error | CAR inner list | Helius inner list |
| --- | --- | --- | --- | --- | --- |
| 800 | 345601886 | 1349 | ProgramAccountNotFound | Not recorded | null |
| 800 | 345602206 | 291 | InvalidProgramForExecution | Not recorded | null |
| 800 | 345602213 | 107 | InvalidProgramForExecution | Not recorded | null |
| 900 | 388800328 | 1070 | InvalidProgramForExecution | Not recorded | null |
| 900 | 388800379 | 370 | InvalidProgramForExecution | Not recorded | null |
| 900 | 388800449 | 837 | InvalidProgramForExecution | Not recorded | null |
| 1000 | 432000032 | 714 | MaxLoadedAccountsDataSizeExceeded | Not recorded | [] |
| 1000 | 432000036 | 751 | MaxLoadedAccountsDataSizeExceeded | Not recorded | [] |
| 1000 | 432000036 | 758 | MaxLoadedAccountsDataSizeExceeded | Not recorded | [] |

Helius returns empty log lists for the three epoch 1000 samples, and null logs for the other six. The public mainnet RPC, checked before the user selected Helius, returned null inner-instruction and log lists for all nine. Thus the providers differ on absence representation; neither returns instruction content missing from these CAR records. Do not call Helius and CAR metadata identical.

Saved evidence: [raw CAR and Helius comparison](car-cpi-helius-evidence-2026-09-06.jsonl).

The diagnostic source is `/private/tmp/audit-car-cpi-20260906.py`, also deployed at `/volume2/blockzilla-bench/control/all-samples-20260905/audit-car-cpi-20260906.py` on NAS. Raw evidence is saved beside the NAS script as `car-cpi-raw-evidence-20260906.jsonl`.

## Reader review

The CAR query adapter uses `OrderedLosslessCarBlock`. In `crates/old-faithful/car-reader/src/ordered_lossless.rs`, lines 169–185 reject non-empty transaction-data and metadata continuation lists. Rewards continuations use a separate, ordered join path.

There is a separate defensive gap worth fixing: `CborArrayView::len()` and `iter()` in `node.rs` use `array().ok().flatten().unwrap_or(0)`. An indefinite-length array, or a wrong CBOR type accepted by the borrowed view, can therefore appear empty. `RawDataFrame::from_borrowed_with_data_buffer` builds `next` through this iterator. Such input can lose its links before the non-empty-list guard. This is not the cause observed in the real samples: the independent parser rejects indefinite arrays, and the selected frames contain no `next` field at all. A narrow fix should reject unsupported array forms when creating the borrowed view, without introducing a CID lookup table or new per-transaction allocations. No production fix was made in this diagnostic turn.

### Reproduction of the array gap

Compiled the current worktree's `node.rs` directly into an isolated diagnostic, using the locally cached minicbor and serde libraries. No production source was edited. `/private/tmp/reproduce-car-next-array-20260906.rs` supplies three DataFrames to the actual current `DataFrame::decode` implementation:

| Encoded next field | Decoder accepts it | Links produced by iterator |
| --- | --- | --- |
| Definite array, one valid CID | Yes | 1 |
| Indefinite array, the same CID | Yes | 0 |
| Byte string, not an array | Yes | 0 |

All three cases consumed their complete input. The diagnostic asserts these observed results. This confirms silent loss at the borrowed-array decoder boundary. It is not a full-pipeline test of malformed CAR acceptance. The existing ordered-reader unit test covers ordinary definite-array transaction-data and metadata continuations, not these malformed/unsupported cases.

### Successful-activity filtering

The user proposed skipping failed transactions in indexing workloads. This is suitable for an explicitly successful-activity index, but not for a full-ledger or fee/balance analysis. Failed transactions can still charge fees and advance a durable nonce; see [Agave rollback accounts](https://github.com/anza-xyz/agave/blob/master/svm/src/rollback_accounts.rs).

Current code: FireWatch checks execution status in its sink and excludes failed transactions from its successful reached-program list. Pump.fun and USDC explicitly request `without_execution_status()`. A sink-only filter is too late to avoid SDK instruction projection. A shared SDK filter should inspect status first, skip instruction/token projection for known failures, preserve unknown status as unknown, and report skipped-failure counts. The count example must continue to count every transaction. Source archives must remain unchanged. This filter was reviewed but not implemented in this diagnostic turn.

## Interpretation and next action

The sample errors and zero compute units are consistent with failure before instruction execution. The current [Agave account loader](https://github.com/anza-xyz/agave/blob/master/svm/src/account_loader.rs) separates account-loading failures from loaded transactions. Before changing coverage rules, verify the applicable historical error paths and classify the remaining affected transactions. Do not use zero compute units alone, and do not treat every transaction error as proof that no CPI occurred.

Keep the stored metadata lossless. A derived SDK result can distinguish a proven pre-execution failure from an unknown inner-instruction list. The workload report should then distinguish source-field absence from missing extraction results. A transaction message can still contain a Pump.fun instruction even when the transaction never executed; the dump must keep its intended semantics explicit.

Epoch 0 USDC is not an archive-failure case merely because metadata is absent: the user identified that the USDC mint did not yet exist. Its workload result should distinguish historical non-applicability from generic metadata coverage.

Do not recompact these archives on the basis of the sampled warnings. No archive, SDK, or example binary was modified. Benchmarks remain stopped.

## Whole-file SHA-256 verification

Expected digests were obtained from the links in the [Old Faithful CAR report](https://github.com/rpcpool/yellowstone-faithful/blob/gha-report/docs/CAR-REPORT.md). This is a one-time diagnostic verification, not a manifest or an archive-format requirement.

| Epoch | Expected SHA-256 | Final status |
| --- | --- | --- |
| 0 | 3c6347f0c51d9cbdb64e2cd17cf1c0d378b7152743fc45ef61226638daca417f | MATCH: all 4,286,945,461 bytes |
| 800 | 5680b25080394175cf2780becd4a36281a3000bbe9e24bcb4e194a1e8e75f48e | MATCH: all 824,317,017,506 bytes |
| 900 | 6d0ca5d4374d9f860e6a3d2733a58a78104720c62ff46206f9ae6a4ef8aecb26 | MATCH: all 527,045,598,158 bytes |
| 1000 | 3c9727378e617f5cba8b5e206bce8fc6ae5df34d3a4408eeb69aea4d62ac7218 | MATCH: all 767,389,334,224 bytes |

The checker reads the SSD copies at `/volume2/blockzilla-bench/archive/car/{epoch}/epoch-{epoch}.car`. It does not independently hash the retained HDD copies. Total scope: 2,123,038,895,349 CAR bytes; no slot indexes included. Two file readers run concurrently, with a reusable 16 MiB buffer each. They check file identity, size, and modification time before accepting a hash result.

NAS process at launch: 1059677. Script: `/volume2/blockzilla-bench/control/all-samples-20260905/verify-car-sha256-20260906.py`. Progress and results: `/volume2/blockzilla-bench/control/all-samples-20260905/car-sha256-audit-20260906.log`. The job exits after the four files. No recurring monitor was created.

Final log: `COMPLETE`, `all_match=true`. All four files remained stable during verification. Per-file elapsed times were 3.854 s (0), 708.559 s (800), 454.792 s (900), and 635.632 s (1000); two files ran concurrently, so these times must not be added as wall-clock duration.
