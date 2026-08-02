# Replay aligned-memory-mapping experiment — 2026-07-30

This experiment follows the corrected epoch-73 replay-only flamegraph. All
replay inputs and planned validation runs use Blockzilla Compact Archive V2;
no CAR input is involved.

## Why this is the next optimization

The 1,307-sample x86 replay profile attributed 42.31% of CPU to explicit SBPF
load/store helper roots. `MappingCache::find` alone was 171 samples, or
13.08%. The replay process was approximately 99.5% user CPU with under 0.6%
I/O wait, so archive layout, zstd, and account-registry lookup are not the
current limiting factors.

`ReplayCompiler` previously inherited
`Config::default().aligned_memory_mapping=false`. SBPFv0 therefore searched a
four-entry mapping cache on every guest memory operation even though replay
always exposes exactly one canonical region in each 4-GiB slot:

1. program/rodata;
2. gapped stack;
3. heap;
4. packed instruction/account input.

The aligned mapper indexes this table with the high 32 address bits. It still
uses the same `MemoryRegion::vm_to_host` checks for bounds, writability, range
overflow, and SBPFv0 stack gaps.

## Implemented profile

- Enable checked aligned mapping for SBPFv0.
- Keep address translation and stack-frame gaps enabled.
- Set `allow_memory_region_zero=false`, preserving the old mapper's rejection
  of address zero even for a zero-length translation.
- Keep `optimize_rodata=true` unchanged; changing historical rodata visibility
  would be a separate semantic change.
- Reserve five region-vector entries so adding the private region-zero
  sentinel cannot reallocate on every VM invocation.
- Reject any execution region that escapes its canonical 4-GiB slot.
- Change the compiler profile/artifact identity to
  `aligned-map-no-region-zero-v7`.

## Apple ARM microbenchmark

`bpf-execution-bench` now compares aligned and unaligned mappings in one
process. Each sample alternates implementation order by round and performs one
million batches; every batch contains eight checked 64-bit memory operations.
Two independent nine-round release runs produced:

| Pattern | Run | Unaligned ns/batch | Aligned ns/batch | Aligned reduction |
|---|---:|---:|---:|---:|
| Hot input region | 1 | 51.6 | 46.7 | 9.5% |
| Hot input region | 2 | 73.4 | 64.6 | 12.0% |
| Program/stack/heap/input rotation | 1 | 62.9 | 50.1 | 20.3% |
| Program/stack/heap/input rotation | 2 | 121.9 | 87.6 | 28.1% |

Absolute host speed varied substantially, but aligned mapping won in all four
long paired comparisons. The semantic checksum matched for both modes and the
operations allocated nothing.

The complete embedded SBPF fixture also retained exactly the same interpreter,
native, and automatic semantic SHA-256:

`4c3122a08db994a4ea6f5b47ed594b302e83154e49d03464a49c9c78eca90841`

VM construction changed from four allocations/208 requested bytes per call to
two allocations/176 bytes. The full ABI mutation path changed from seven
allocations/1,520 bytes to five allocations/1,488 bytes. Timing from separate
pre/post ARM processes is deliberately not used as an end-to-end speed claim:
unrelated reset and serialization controls varied by up to roughly 3x.

## Validation

- Aligned and unaligned interpreter success and access-fault results match.
- Native-required success and access-fault results match on Apple ARM; the same
  test selects upstream x86 JIT on Linux x86-64.
- Canonical region containment and hidden region-zero behavior are locked by
  tests.
- 348 all-target tests pass; one manual encoder benchmark is ignored.
- Formatting and all-target compile checks pass.

The change is benchmark-ready, not yet accepted for the marathon runtime. The
decisive x86 test must wait until the active replay releases its CPU/lock.

## NAS acceptance run

Use the immutable epoch-72 checkpoint
`15172e16aefbe670799e8224c6ab742796603e6129ecea5c8c7d2614f631c2a0`
and the epoch-73 Compact V2 successor. Run the preserved unaligned binary and
an isolated aligned build in balanced `A B B A` order on CPU 2, without a
profiler and without writing a checkpoint.

Every 30,000-block run must end at slot 31,572,826 and produce:

`08d0b7e10f1b8fbfe8261ec8744d94093bc0a660354d4c460cbad865248693df`

The adoption gates are exact stdout/state/counter parity and at least 5% median
replay-throughput improvement. A delayed follow-up flamegraph should show
`MappingCache::find` disappearing.

## What to optimize next

Aligned lookup removes only the 13.08% cache-search leaf. The remaining larger
target is a small `solana-sbpf` fork that emits an inline checked fast path for
canonical regions and calls the current Rust memory helper only on a failed
guard. That can remove the helper call, caller-saved-register spill/restore,
mapping dispatch, and generic unaligned copy from successful guest memory
operations. It must be differentially tested against stock `MemoryMapping`
before replay use.

On ARM, the analogous next target is the Cranelift memory helper's per-access
`catch_unwind`/fault-status path. Disabling the instruction meter is a separate
trusted-history experiment: it also removes the infinite-loop watchdog and
must not be mixed into this mapping A/B.
