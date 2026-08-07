# Compact V2 PoH verifier — macOS ARM64 baseline

Date: 2026-08-04

This is a synthetic hash-core baseline, not a claim about whole-archive
throughput. No complete Compact V2 epoch was present on the Mac, so the real
streaming command still needs a representative archive parity/performance run
before deployment.

## Implementation

`blockzilla verify-archive-v2-poh` reads Compact V2 directly. It:

- mmaps immutable block and signature payloads;
- streams bounded `poh.wincode` frames;
- reuses the zstd output buffer, transaction signature-prefix table, entry-job
  table, and per-worker Merkle scratch;
- recomputes independent entries in a dedicated Rayon pool;
- checks every entry hash and the final blockhash registry value;
- distinguishes blocks with externally supplied blockhashes from recomputed
  blocks instead of reporting false verification.

PoH semantics match Agave: apply `num_hashes - 1` plain SHA-256 rounds, then
hash either once for a tick or with the Merkle root of ordered transaction
signatures for a record.

## Synthetic benchmark

Workload per iteration: 32,768 independent entries, 4,096 PoH hashes per
entry, and two signatures per entry. Five measured iterations follow one
warmup. Build profile is the workspace release profile (optimization level 3,
fat LTO, one codegen unit).

| Workers | Median time | Median hashes/s | Best hashes/s | Worst hashes/s |
|---:|---:|---:|---:|---:|
| 1 | 4.372 s | 30.70 M | 31.02 M | 29.46 M |
| 4 | 1.214 s | 110.58 M | 114.11 M | 107.34 M |
| 8 | 0.809 s | 165.97 M | 184.83 M | 161.40 M |

All runs produced checksum
`3eb7c7f8af09d47c58b417df63be41af60c8c0b207298fc5024a9babf294aa35`.
Eight workers are 5.41 times faster than one worker; scaling tapers after the
Mac's four performance cores, as expected.

The debug-symbol profiling build itself measured 136.95 M hashes/s at eight
workers. Do not compare that number directly with the stripped baseline.

## CPU and memory profile

Apple `sample` captured each thread at 1 ms for five seconds. Of runnable
worker samples, 27,469 landed directly in `recompute_entry_hash`; allocator
functions did not appear among top-of-stack samples. This confirms the
synthetic workload is SHA-256 compute-bound, rather than scheduler- or
allocation-bound. The generated flamegraph is a local profiling artifact at
`/tmp/blockzilla-poh-core-macos-arm64.svg`.

`/usr/bin/time -l` measured approximately 11.7–12.0 MB maximum RSS across the
one-, four-, and eight-worker runs. Apple reported a 5.8–6.1 MB peak physical
footprint. The small increase with worker count is consistent with bounded
worker state.

## Reproduction

```sh
cargo build --release -p blockzilla --bin blockzilla --features benchmark-tools

/usr/bin/time -l target/release/blockzilla \
  bench-archive-v2-poh-core \
  --entries 32768 \
  --hashes-per-entry 4096 \
  --signatures-per-entry 2 \
  --iterations 5 \
  --threads 8

target/release/blockzilla verify-archive-v2-poh \
  /path/to/epoch-N \
  --threads 8
```

## Next measurement gate

Run the streaming verifier on at least one small early epoch and one large
late epoch. Record exact archive generation hashes, verifier stdout, wall/CPU
time, maximum RSS, compressed MiB/s, blocks/s, entries/s, and hashes/s. A CAR
file is neither required nor read. Only after exact parity should the command
be admitted as an archive integrity task.
