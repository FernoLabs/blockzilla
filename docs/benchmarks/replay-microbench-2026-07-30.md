# Replay microbenchmarks — 2026-07-30

> Follow-up: the copy-on-write, VM/compiler reuse, BPF buffer-pooling, and
> epoch-0-to-1 results are recorded in
> [Agave-inspired replay optimization tranche](replay-agave-optimizations-2026-07-30.md).
> The “current” paths below are the pre-optimization baseline.

This report isolates the native SBPF compiler, Compact Archive V2 decode,
transaction account state, and instruction-diff paths. No CAR input is used.

## Hosts and method

- NAS: Intel Core i5-1235U, Linux 6.12.30+, one run pinned to logical CPU 2.
- NAS binaries: Rust 1.96 release builds with `RUSTFLAGS=-Ctarget-cpu=native`.
- Apple test: native arm64 macOS release build.
- Primary values are medians of five timing rounds unless noted otherwise.
- Allocation samples are separate, untimed runs. Bytes are requested allocation
  sizes, not peak or retained memory.
- The NAS remained a production host, so these results rank costs more reliably
  than they predict an isolated machine's absolute ceiling.

## SBPF to native code

The x86-64 replay runtime is executing real machine code emitted by
`solana-sbpf 0.21.0`. The compiled artifact is executable memory in the replay
process, not a persistent `.so` file.

One BPFLoader program extracted from historical replay was used as the real
compile fixture:

- program: `breakbUwq5541KXXmMEgaDBEwgWYiVe23P3u3n7qod3`
- ELF: 15,464 bytes
- SBPF text: 11,792 bytes
- emitted x86-64 machine code: 38,293 bytes
- backend selected: `NativeJitX86_64`

| Real-program phase | Median cost | Allocations/op | Requested bytes/op |
|---|---:|---:|---:|
| Extract canonical ELF | 9.24 us | 1 | 15,464 |
| Load SBPF ELF | 7.13 us | 14 | 32,570 |
| Requisite verifier | 3.58 us | 0 | 0 |
| JIT from loaded executable | 37.80 us | 21 | 8,309 |
| Complete production cold compile | 67.93 us | 50 | 41,661 |
| Compiled-program cache hit | 4.6 ns | 0 | 0 |

This makes cold compilation negligible during uninterrupted replay once the
program cache is warm. Checkpoint restore currently creates an empty compiler
cache, so the first later invocation recompiles each program.

The small execution fixture produced the same semantic SHA-256 with the
interpreter, explicitly required native backend, and automatic selection. On
the NAS it took 2.172 us in the interpreter and 2.104 us through native JIT.
That fixture executes only one watched instruction, so fixed VM/ownership cost
dominates and the ratio is not representative of a real program.

### ABI/account-size scaling on x86-64

| Accounts x data bytes | Prepare | Serialize | Copy back | Full ABI + native call | Allocations/call | Requested bytes/call |
|---|---:|---:|---:|---:|---:|---:|
| 4 x 200 | 150 ns | 119 ns | 78 ns | 2.36 us | 7 | 1,088 |
| 8 x 1,024 | 206 ns | 282 ns | 229 ns | 3.09 us | 10 | 5,744 |
| 32 x 4,096 | 2.10 us | 3.62 us | 3.28 us | 14.40 us | 24 | 72,944 |

Large account sets make ABI preparation and copying visible. Reusing buffers
and borrowing read-only account data are therefore more valuable than reducing
the already tiny program-cache lookup.

### Native Apple arm64 status

The embedded SBPFv0 fixture selected the custom
`NativeCraneliftAarch64Subset` backend in a native arm64 macOS build. It emitted
80 bytes of AArch64 code and matched interpreter output exactly. Complete cold
compilation took 53.68 us; native execution took 2.280 us versus 2.354 us for
the interpreter.

This is a proof of native AArch64 execution, not full SBPF coverage. The
Cranelift backend currently accepts only a narrow syscall-free SBPFv0 subset;
unsupported real programs fall back to the interpreter. An x86-64 macOS build
can use the upstream x86 JIT under Rosetta, but that is translated x86 code,
not a native Apple Silicon backend.

Persisting raw JIT pages as `.so` is unsafe because they are process- and
backend-specific. A future durable artifact must be relocatable and keyed by
at least ELF hash, target triple, CPU features, SBPF version, VM configuration,
syscall ABI, and compiler version. At the measured 68 us real-program cold
compile time, this is a restart-latency optimization rather than a steady-state
throughput priority.

## Account state and registry size

The existing canonical `HashMap` account store remains the right design; Redis
or another external database would add serialization, copies, and IPC. At
54,339 accounts it measured 19.52 million random lookups/s and 18.49 million
random in-place updates/s. Even at one million accounts it retained 10.86
million lookups/s and 8.38 million updates/s.

The new transaction-state benchmark uses 32 reads, eight writable accounts,
and 2,000 iterations per sample. At the current 54,339-account scale:

| Account data | Current BTree overlay read | Reserved Hash overlay read | Current writable clone | Reserved Hash writable clone | Stage eight writes | Publish staged writes |
|---|---:|---:|---:|---:|---:|---:|
| 0 B | 30.9 ns/item | 20.5 ns/item | 28.4 ns/item | 22.7 ns/item | 20.5 ns/item | 16.6 ns/item |
| 128 B | 68.4 ns/item | 57.3 ns/item | 43.0 ns/item | 40.2 ns/item | 33.3 ns/item | 39.7 ns/item |
| 1,024 B | 75.6 ns/item | 77.6 ns/item | 49.8 ns/item | 45.7 ns/item | 31.9 ns/item | 73.9 ns/item |

Changing only the transaction overlay from BTreeMap to HashMap is a small and
inconsistent win. Avoiding eager clones is the larger opportunity. At 1 KiB
per account, the current 32-read overlay performs 37 allocations and requests
39.1 KiB per instruction-like iteration; the eight-write staging path performs
9 allocations and requests 9.44 KiB. A borrowed-read, copy-on-write overlay
could eliminate roughly 28 allocations and 76% of those requested bytes for
this shape.

## Instruction diff cost

Current `InstructionDiff` construction unions before/after BTreeMap keys,
hashes account data, and materializes changed ranges. With eight accounts per
instruction at a 54,339-account registry:

| Account data | Diff cost/instruction | Cost/account | Allocations/instruction | Requested bytes/instruction |
|---|---:|---:|---:|---:|
| 0 B | 0.507 us | 63 ns | 61 | 6,276 |
| 128 B | 194.6 us | 24.33 us | 61 | 6,276 |
| 1,024 B | 1.109 ms | 138.61 us | 61 | 6,276 |

An end-to-end replay A/B over the first 5,000 epoch-0 Compact rows (three timed
rounds) confirms the synthetic signal. Both modes produced the same final state
hash over 442 accounts and committed the same 19,996
transactions/instructions.

| Capture mode | Median replay | ns/instruction | Allocations/instruction | Requested bytes/instruction |
|---|---:|---:|---:|---:|
| Every instruction diff | 10.062 s | 503,201 | 89.119 | 31,434 |
| No instruction diffs | 59.679 ms | 2,985 | 0.196 | 4,708 |

Full diff capture was **168.6x slower** on this vote-heavy sample. Diff
materialization is the dominant measured cost when every instruction diff is
requested. The result does not mean diffs should be removed; it means they
should be generated from a per-instruction dirty journal instead of cloned
before/after account maps.

The changed-account set itself is minor. Reserved HashSet membership was about
5.6x faster than BTreeSet at 54,339 accounts (7.4-7.9 versus 42-44 ns/item), but
the benchmark mostly measures duplicate membership checks after the first
iteration, and the absolute cost is tiny.

## Compact V2 decode and reads

The epoch-75 sample contained 20,000 blocks, 2,619,084 transactions, 2,804,550
instructions, 77.28 MB compressed, and 398.56 MB uncompressed.

| Phase | Median | Blocks/s | Transactions/s | Compressed GB/s | Allocation calls | Requested bytes |
|---|---:|---:|---:|---:|---:|---:|
| Coalesced compressed reads | 31.1 ms | 643,817 | 84.31 M | 2.488 | 7 | 67.11 MB |
| Borrowed read + zstd + outer schema | 312.4 ms | 64,021 | 8.38 M | 0.247 | 20 | 67.78 MB |
| Full public visitor stream | 1.400 s | 14,281 | 1.87 M | 0.055 | 569,408 | 246.24 MB |

With a 64 MiB prefetch bound, the borrowed decoder fetched all 20,000 block
frames using two range-read calls. The nested read median was 31.4 ms; zstd plus
outer-schema decode was approximately 281.0 ms. Transaction schema,
materialization, program histogram, and the minimal visitor accounted for an
estimated 1.088 s and about 28.5 allocations per block.

The full decode time is still under 1% of the roughly 185.9 s later-epoch replay
time previously measured for 20,000 blocks, so Compact decode and block-read
syscall count are not the present end-to-end bottleneck. Linux syscall trace
counters were unavailable to the unprivileged NAS user; the reader-level call
counter verifies the important batching behavior without requiring elevated
access.

## Optimization order

1. Add replay-only in-place System, Config, Stake, and BPF-loader processors,
   following the existing Vote fast path. The transaction overlay already
   provides rollback on failure, so nested full overlay clones are redundant.
2. Replace eager cloning of all instruction accounts with borrowed reads and a
   copy-on-write writable set. Keep deterministic sorting at commit/checkpoint
   boundaries rather than on every lookup.
3. Build complete instruction diffs directly from a per-instruction dirty
   journal. Preserve lexicographic output order, creation/deletion semantics,
   SHA-256 values, CPI boundaries, and transaction rollback.
4. Add a host-local content-addressed compiled-program cache or checkpoint
   prewarm only if restart profiles show cold compilation matters. Do not store
   raw process-local JIT pages as `.so`.
5. Defer deeper Compact materialization work until execution/state costs fall;
   its current maximum end-to-end contribution is below about 1%.

For maximum-throughput replay, use diff capture `None` and prioritize steps 1
and 2. For the analytical product that requires every instruction diff,
prioritize all three state changes, especially the dirty-journal diff path.

## Benchmark binaries

- `runtime/blockzilla-replay/src/bin/bpf-execution-bench.rs`
- `runtime/blockzilla-replay/src/bin/compact-decode-bench.rs`
- `runtime/blockzilla-replay/src/bin/replay-state-bench.rs`
- `runtime/blockzilla-replay/src/bin/replay-hotpath-bench.rs`

Focused validation passed: 3 BPF tests, 5 Compact tests, 6 state tests, and
`cargo check` for all three new/expanded benchmark binaries.
