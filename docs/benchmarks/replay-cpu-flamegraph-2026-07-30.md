# Replay CPU flamegraph, epoch 73 prefix

Status: measured on the NAS, 2026-07-30

## Conclusion

Combined flamegraph and `pidstat` evidence shows that the current replay is
CPU-bound inside native SBF execution and its guest-memory boundary. It is not
blocked on archive I/O, Zstandard decompression, or the in-memory account
registry at the measured state size.

The replay-only profile attributes 960 of 1,307 accepted samples (73.45%) to
anonymous native code, `solana-sbpf` memory loads/stores, and builtin/syscall
callbacks. Only 347 samples (26.55%) unwind through the ordinary Rust replay
loop. The anonymous 362-sample branch is inferred to be generated JIT code
because the process does not yet publish a perf map or jitdump symbol table;
memory and callback branches return from that generated code with an
interrupted unwind.

The optimization order is therefore:

1. Attribute generated code by program id and native artifact, then measure
   guest instructions, translated bytes, cache hits, and syscall counts per
   program.
2. Reduce guest-address translation and copies in the SBF load/store path:
   cache validated regions, specialize common bounded translations, and avoid
   redundant parameter serialization/copy-back while preserving the loader ABI.
3. Keep the existing in-memory account store. Its directly visible hot-path
   operations account for about 66 samples (5.05%) and are not the first-order
   bottleneck at roughly 40,000 resident accounts.
4. Consider exact read/write conflict-DAG execution after sequential parity.
   Parallelism can use more cores, but it should not hide per-transaction VM
   overhead or weaken deterministic rollback and instruction-diff ordering.
5. Do not build an uncompressed transaction-only archive yet. Compact block
   decompression is 27 samples (2.07%); removing it cannot materially solve the
   present bottleneck.

## Replay-only measurement

The run resumed from the completed epoch-72 checkpoint and visited 30,000
present blocks from epoch 73 using Blockzilla Compact Archive V2. No CAR input
or conversion was used. The optimized, symbolized binary was built with native
CPU tuning and frame pointers, pinned to CPU 0 at nice level 10, and sampled at
a requested 49 Hz. Sampling began after 40 seconds so checkpoint loading and
verification were excluded.

| Property | Value |
|---|---:|
| Input | epoch-73 Compact V2 prefix |
| Present blocks | 30,000 |
| End-to-end wall time | 146.193 s |
| End-to-end throughput | 205.208 blocks/s |
| Sampling start delay | 40 s; exact sampled duration was not recorded |
| Retained samples | 1,307 |
| Starting checkpoint SHA-256 | `15172e16aefbe670799e8224c6ab742796603e6129ecea5c8c7d2614f631c2a0` |
| Profiler binary SHA-256 | `d046ee01d84c4ec23c2e029b15c7d92417eae38bc4f2ed012b05d52b9f4fc79d` |
| Resulting bounded-state SHA-256 | `08d0b7e10f1b8fbfe8261ec8744d94093bc0a660354d4c460cbad865248693df` |

This throughput includes checkpoint startup and profiling overhead, so it is a
diagnostic result rather than a replacement for the uninstrumented generation
metrics. The simultaneous marathon was pinned to CPU 2. Its epoch-73 timing is
marked as perturbed because profiler builds and bounded profiles used other
cores and shared memory bandwidth.

Independent `pidstat` samples on the marathon process show 99.43–99.53% user
CPU, 0.42–0.53% I/O wait, 7.67–20.07 KiB/s reads, and approximately 40–42 MiB
RSS. That observation, rather than flamegraph percentages alone, establishes
that the run is CPU-bound.

### Root attribution

| Root branch | Samples | Total |
|---|---:|---:|
| Anonymous/generated native code | 362 | 27.70% |
| `MemoryMapping::load` | 320 | 24.48% |
| `MemoryMapping::store` | 233 | 17.83% |
| SBF builtin/syscall callback entry | 45 | 3.44% |
| Normally unwound Rust replay path | 347 | 26.55% |
| **Total** | **1,307** | **100.00%** |

The load branch includes 147 samples (11.25%) in unaligned copy and 150
(11.48%) under mapping with access-violation handling. The store branch has
228 samples (17.44%) under the mapping handler. `MappingCache::find` appears in
171 leaf samples (13.08%). These are the clearest concrete targets for the next
microbenchmarks.

The first resulting optimization and its semantic/performance gates are
recorded in
[Replay aligned-memory-mapping experiment](replay-aligned-memory-mapping-2026-07-30.md).

### Normally unwound host work

The following nodes are inclusive. Child rows overlap their parent and must not
be summed as an exclusive CPU budget.

| Host path | Samples | Total | Host subtree |
|---|---:|---:|---:|
| Compact transaction decode/materialization | 68 | 5.20% | 19.60% |
| Zstandard block decompression | 27 | 2.07% | 7.78% |
| Slot/transaction processing | 245 | 18.75% | 70.61% |
| Allocation-minimal vote path | 107 | 8.19% | 30.84% |
| BPF host wrapper | 33 | 2.52% | 9.51% |
| BPF parameter/baseline serialization | 18 | 1.38% | 5.19% |
| Compact post-balance reconciliation | 17 | 1.30% | 4.90% |
| Directly visible hot account-store operations | 66 | 5.05% | 19.02% |

The BPF wrapper's 33 samples understate program execution: generated code and
the VM memory callbacks form separate root branches when stack unwinding crosses
JIT code. The root attribution is the correct view of the dominant execution
cost.

## Checkpoint-startup control profile

An earlier 10,000-block profile started sampling before checkpoint restore. Of
1,332 samples, 786 (59.01%) were under
`read_trusted_frozen_checkpoint`: 394 samples (29.58%) were in the trusted
whole-file SHA-256 pass and 389 (29.20%) in the checkpoint's internal checksum
pass. This is a one-time resume cost, not steady replay CPU.

We should eventually compute both digests in one streaming traversal or skip
the embedded-checksum pass only after the trusted whole-file digest matches.
The embedded checksum cannot independently authenticate the file unless that
exact digest is itself stored in trusted metadata. This is worth doing for
short resumes, but it is lower priority than SBF execution for multi-epoch
runs.

## Artifacts and interpretation

- `artifacts/epoch73-replay-only-30000-blocks.svg` is the corrected interactive
  replay-only flamegraph.
- `artifacts/epoch73-replay-only-30000-blocks.top.tsv` ranks leaf/self samples;
  it is not an inclusive call-tree table.
- `artifacts/epoch73-first-10000-blocks.svg` is the checkpoint-startup control.
- Matching `.metadata`, `.stdout`, and `.stderr` files preserve the command
  inputs, hashes, replay report, and profiler output.

Percentages are shares of the 1,307 retained samples, not exact instruction
counters or shares of every process CPU cycle. A follow-up profile should add
generated-code symbols and per-program counters before changing the VM memory
ABI, so improvements can be tied to actual programs and checked against
deterministic state hashes.

This run selected the allocation-minimal `--sample-diffs 0` path. It therefore
does not price full per-instruction/PDA diff construction. A separate
diff-enabled profile is required before choosing the long-run trace retention
policy. The profiler also blocklists libc, libgcc, pthread, and vdso frames;
all percentages above are percentages of accepted samples.
