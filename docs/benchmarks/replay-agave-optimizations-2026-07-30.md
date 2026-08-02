# Agave-inspired replay optimization tranche — 2026-07-30

This tranche applies account and VM-lifecycle techniques from Agave to the
Blockzilla state-only replay runtime. All end-to-end input is Blockzilla
Compact Archive V2; no CAR reader is involved.

Final measurements used native Apple Silicon release binaries:

- `blockzilla-replay-poc`: `83d78e5e74e74ccb12ed9b7e4b1cd9922a93ad09fadf78b99a4855000a0ba1c6`
- `replay-hotpath-bench`: `f2dc5b58530945fa0e6bcd9d778540e4b681cac557da524f0bc9429ffac09c93`
- `bpf-execution-bench`: `c5d73e621a75f4718d30f6c96fd6bf0f3f7b5c09dda8dc30f5fe8bac709d72e0`

## Implemented changes

- Account payloads are reference-counted and copy-on-write. Transaction and
  instruction snapshots share immutable bytes; the first real mutation
  detaches them.
- Empty accounts share one process-wide immutable payload.
- Instruction before/after capture includes writable accounts only, while the
  absent-account reconciliation path remains complete.
- Diff key union is an allocation-free ordered merge of the two `BTreeMap`
  iterators. Shared unchanged payloads compare in constant time.
- The legacy-BPF parameter layout is collected once and reused for sizing,
  serialization, verifier preparation, and copyback. The launch-era 256-account
  limit is enforced before its one-byte duplicate indexes are emitted.
- BPF verifier baselines retain copy-on-write payload references instead of
  cloning protected account bytes. No-op copyback does not request mutable
  access. Changed shared data is replaced in one pass instead of cloning the
  old bytes and then overwriting them.
- Every replay worker has a recursion-safe thread-local VM scratch pool.
  Stack, heap, and call-frame buffers no longer contend on one mutex.
- The immutable syscall/runtime environment is constructed once and shared by
  compiler clones. Compiler clones also share a per-program single-flight
  nested-program cache and the CPI activation state.
- Serialized BPF parameter buffers are leased from a recursion-safe
  thread-local pool. Successful guest outputs are recycled even when status,
  copyback, or verifier checks fail; buffers above 16 MiB are not retained.

Checkpoint encoding is unchanged: it still writes the raw account byte length
and bytes, never the in-memory reference-counted representation.

## Compact epoch-0 hot-path A/B

The preserved pre-change and new release binaries replayed the first 5,000
epoch-0 Compact rows. Each run committed 19,996 transactions/instructions and
produced the same 442-account state hash:

`ba07cfc5b7c2a3d77c64f4f9b38852d2df9078035659236ec8fc081c3799f09a`

Apple Silicon timing varied substantially with host temperature and frequency.
In the final two paired run orders, the optimized binary was 4–15% faster with
every instruction diff enabled. Diff-disabled timing was flat within noise:
one order was 1.4% faster and the reverse order was 3.8% slower. The allocation
measurements are deterministic:

| Capture mode | Binary | Allocation calls/instruction | Requested bytes/instruction |
|---|---|---:|---:|
| Every instruction | Before | 90.1187 | 31,801.9 |
| Every instruction | Optimized | 83.1398 | 22,558.7 |
| None | Before | 0.1955 | 4,707.8 |
| None | Optimized | 0.2166 | 4,702.6 |

For full instruction diffs this is 7.7% fewer allocation calls and 29.1% fewer
requested bytes. Diff materialization remains much more expensive than state-
only replay; copy-on-write is an intermediate step, not a substitute for a
first-write dirty journal.

## Legacy-BPF pipeline

The final embedded SBPFv0 fixture executes four instructions and increments the
first byte of a writable program-owned account. It starts every operation from
shared payloads, so each call measures real first-write COW. Native Cranelift
AArch64 and the interpreter produced identical output hashes. Medians use seven
release-mode rounds; allocation samples are separate from timing samples.

| Shape | Path | Full ABI ns/call | Allocations/call | Requested bytes/call |
|---|---|---:|---:|---:|
| 4 accounts × 200 B | Before, one-instruction no-op | 5,163 | 8 | 2,306 |
| 4 accounts × 200 B | Final, four instructions + changed data | 2,367 | 8 | 1,936 |
| 32 accounts × 4 KiB | Before, one-instruction no-op | 44,039 | 25 | 207,010 |
| 32 accounts × 4 KiB | Final, four instructions + changed data | 20,199 | 14 | 17,040 |

The final path does more guest and state work, so this is deliberately not
presented as identical-operation semantic parity. Even so, both shapes are
about 2.18× faster than the old no-op production path. At 32 × 4 KiB,
allocation calls fell 44% and requested bytes fell 91.8%; retaining the 134 KiB
guest buffer is the largest memory win. Direct mapped account memory remains a
larger future step, but it is no longer required merely to avoid allocating the
serialization buffer on every call.

For the earlier one-instruction fixture, building the runtime environment once
reduced cold compilation from 444 to 434 allocation calls and from 91,563 to
90,874 requested bytes. Timing was too frequency-sensitive to claim a latency
improvement. Cache hits remain the important steady-state path.

## Complete epoch 0 → 1 replay

The optimized `v16` runtime replayed both Compact generations continuously
with `--sample-diffs 0` (state mutation enabled, diagnostic diff materializing
disabled):

The final warm-cache instrumented pass measured:

| Generation | Blocks | Transactions | Wall | Blocks/s | Transactions/s | Compressed payload GB/s | Decode/visit | Replay/state |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Epoch 0 | 431,548 | 1,724,876 | 2.098 s | 205,733 | 822,306 | 0.035299 | 1.203 s | 0.895 s |
| Epoch 1 | 430,517 | 12,548,674 | 12.758 s | 33,745 | 983,602 | 0.015726 | 7.534 s | 5.223 s |

The continuous replay committed 14,272,644 transactions, preserved 906
historical transaction failures, rebuilt 637 accounts, and produced:

`0ab5cb669c7f189cf4b84ec39945c90d0a06a166647c33f835253d8e2a202580`

A separate warm-cache run without per-generation telemetry completed in 13.86
seconds: about 62.2k present blocks/s, 1.03 million transactions/s, and 0.0198
GB/s of compressed block payload end to end. The first colder instrumented pass
took 31.28 seconds inside the two generation visitors. Filesystem cache and
host-frequency state therefore need to be reported with any absolute number.

A much older preserved release binary completed in 23.43 seconds, but it uses
runtime profile `v7` rather than current profile `v16` and omits later
historical fee/balance and native-program work. It is not a valid end-to-end
optimization baseline and is deliberately not used to claim a speedup.

## Validation and next bottleneck

- 336 all-target tests passed; one manual benchmark test was ignored.
- All targets passed `cargo check` and formatting checks.
- Hot-path, BPF ABI, checkpoint, and full epoch state hashes remained stable.

The final warm-cache instrumented epoch run spent 8.74 seconds in Compact
decode/visit and 6.12 seconds in execution/state mutation. The in-memory
account table is still not the bottleneck. The highest-value next change is a transaction-
local dirty journal that records first-write before state and final after state,
then emits sorted instruction diffs without constructing two `BTreeMap`s or
hashing untouched buffers.
