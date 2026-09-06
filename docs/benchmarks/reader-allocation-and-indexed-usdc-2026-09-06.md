# Reader allocation and indexed USDC review — 6 September 2026

Status: implementation and independent source review complete. All 3,549 workspace tests passed, with one ignored and no failures.
The Linux release build passed. The authorized epoch 300 NAS retest is complete:
all output checks pass, indexed output is 47.97% smaller, and the historical USDC
speed gap remains. See [the measured results](epoch-300-indexed-usdc-and-allocation-retest-2026-09-06.md).

## Findings and changes

V2 USDC already used borrowed block/row views, scalar message validation, reused
pre/post token vectors, and mint filtering by bound registry ID. Its old and
refactored output sinks are unchanged after crate-name normalization. The sink
uses a fixed stack record and borrowed iteration. Empty `Vec::new()` values do
not allocate. The owned canonical model still requires public keys for selected
balance rows; it is not an entirely zero-copy output API.

The first USDC patch removes the second metadata traversal. Its full epoch 300
measurements are recorded separately: 109.547s before that patch, 89.014s with
one traversal, and 76.192s for a fresh old-reader pair. Those are historical
measurements of the one-pass snapshot, not results for the additions below.
See [the one-pass report](epoch-300-usdc-single-pass-fix-2026-09-06.md).

The allocation follow-up changes are:

- V2 and V3 take a retained balance buffer only when a row is selected. Empty or
  nonmatching transactions no longer keep useful pool capacity until block end.
- V2 can use scalar validation for exact failed records when instruction data is
  not requested and failed details will be omitted. `HAS_ERROR` selects a parser
  path only; decoded status and all row flags must agree before omission. This
  removes unused message/CPI/loaded descriptor lists from Pump.fun failures.
  Selected instruction-data requests keep their reconstruction checks.
- The profiler records allocation-size buckets and existing worker/batch/cache
  measurements. Buckets now use immediate atomic counters in allocation-only
  runs. Review found that the original thread-exit flush could miss worker counts.
  It has an `--indexed-usdc` diagnostic mode; public examples still scan complete
  epochs.

Earlier allocation totals used the thread-exit counter and remain provisional.
The corrected retest over 3,274,503 epoch 300 transactions measures 3,150–3,314
canonical USDC allocation calls and 1,563–1,648 indexed calls at twelve workers.
Requested bytes include scan setup and the 432,762,560-byte registry load;
they are not live memory and exclude native C zstd allocations. No verified
old/new allocation percentage is derived from the earlier counter.

The existing full USDC receipt reports twelve distinct workers and twelve active
at once. Its decode/projection pool shares immutable registry data and recycles
worker buffers. It still uses groups of at most 48 blocks with twelve workers.
Peak concurrency is not continuous utilization. A new scheduling design was not
introduced without evidence that it is needed.

## Optional compact output

The new V2 indexed token scan reuses the complete validated transaction projection
and preserves compact source references before canonical public-key conversion.
It collects selected rows in flat reusable block buffers. Each worker has fixed
storage for at most 256 message account references. It derives the actual token
account from static, writable-loaded, and readonly-loaded accounts; the owner and
transaction-local account position are distinct fields.

The ordered consumer resolves a reference only when it writes its first mapping
entry. Numeric registry IDs remain numeric; raw inline keys have a separate
namespace. A full registry cache is still permitted by the caller's memory cap,
so discovery does not restore the earlier sparse-cache reread problem. Sparse
mode uses one consumer cache, not twelve key-resolution caches.

The new `read-compact-v2-usdc-indexed` example writes 70-byte balance records and
60-byte first-observation dictionary records. Existing USDC records are 136 bytes.
The full epoch 300 retest measures 47.97% less space including the dictionary,
with no scan-speed gain. No SQLite database is created. Dictionary memory
grows with distinct references. Requested output across threads owns its data
until consumption; the callback borrows that reusable storage.

Each dictionary is bound to the admitted registry metadata. The source sidecar
records local pinned-file identity or the actual remote resource URL, length,
and strong ETag. These bindings are not described as registry content hashes.
Completion requires both file hashes, totals, coverage, and final source checks.
The expansion CLI verifies completion, source scope, sizes, hashes, and row
counts. It reconstructs the existing BZUSDC02 bytes. Unknown status and missing
mint retain coverage; known failures add no discoveries.

This is first observation in selected balances, not proof of account creation.
The SPYX dumper already uses source-scoped discovery logs and compact-ID matching,
but its raw-copy pass can retain failed transactions. Its discovery shortcuts do
not replace canonical reader validation. See [the SPYX review](reader-indexed-discovery-review-2026-09-06.md)
and [the indexed output specification](../reference/usdc-indexed-balances-v1.md).

## Validation and benchmark

Independent review found no correctness blocker after the allocation growth and
completion checks were addressed. Tests cover buffer ownership, failed-record
validation, selected-data reconstruction, static and loaded token accounts,
missing/inline/indexed references, one/many workers, full/sparse caches, ordered
errors, output reconstruction, dictionary reuse, damaged files, and coverage.
The full workspace run passed 3,549 tests across 131 test harnesses, with one
ignored and no failures. Workspace formatting and diff checks pass. The reader
HTTP feature is enabled by workspace dependencies. Two new fixture mistakes
were corrected before this successful run; no production validation was relaxed.
Test log: `/tmp/blockzilla-indexed-workspace-tests.log`. The Linux x86-64 musl release build passed with the same `+aes,+sse2` flags as
the earlier NAS binaries. Its build record, source patch, per-file hashes, binary
hashes and test log are included in the local package.

The local package is prepared under
`target/nas-validation/epoch300-allocation-review-20260906T161500Z/`.
The completed NAS sequence compares bounded USDC/Pump profiles, then full epoch
300 canonical/indexed output and exact expansion parity. All 18 output checks
pass. The allocation-counter correction passes three targeted tests and six
NAS cases with two measured iterations each. Prior results stay intact.
The full 88-job matrix and its automation remain stopped.
