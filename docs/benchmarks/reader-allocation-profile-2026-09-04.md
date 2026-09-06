# V2/V3 allocation pass — 2026-09-04

## Measured result

Real epoch 700, block ordinals 0–2047: 3,560,931 transactions, 354,726
recorded USDC balance rows. The diagnostic calls the format SDK and the same
USDC sink as the public example. It discards output bytes after serialization.
The NAS full benchmark continued during these small runs.

| USDC, 12 workers | Before, median seconds | After, median seconds | Gain |
| --- | ---: | ---: | ---: |
| Compact V2 | 2.312 | 1.105 | 2.09× |
| Indexer V3 | 1.871 | 0.906 | 2.06× |

Each timing is the median of three measured passes after one warm-up pass.
Allocation counting and CPU profiling were disabled for these timings. This is
a warm bounded-range comparison, **not a full-epoch or WAN speed claim**.

In separate one-worker V2 allocation runs, about 2.38 million Rust allocation
requests became about 71,000: a 97% reduction. Requested allocation space fell
from about 4.81 GB to 110 MB. These are cumulative allocation requests, not RSS
or live memory. Native C zstd allocations are not included. Worker-local counters
avoid a shared atomic increment on every measured allocation.

The before/after V2 and V3 runs agree on transaction totals, pre/post row totals,
matched transactions, output length, and coverage. The harness does not compare
every output byte. The full sample matrix remains the complete archive run.

## SDK changes

- USDC uses the existing scalar message parser for account/instruction limits.
  It no longer builds unused key, instruction, and lookup descriptor lists.
- Both metadata projectors can fill bounded, reusable pre/post balance vectors.
  Existing owned-return methods remain available. The vectors contain at most
  256 rows per side. This removes repeated allocation; it is not yet a fully
  streaming token-balance visitor.
- V2 recycles evicted registry chunk buffers and reads directly into them.
  The old path allocated raw bytes, allocated key rows, then copied every row.
- V3 preserves initialized plane and registry-buffer lengths between reads.
  It grows only when needed instead of zeroing each complete reused buffer.
- V2 now reports output-buffer wait, result-send wait, signature-read time,
  signature-assignment time, and publish time separately. These are coarse
  per-block/group clocks, not per-transaction clocks.

These changes are shared by the SDK entry points, including local and network
sources. The measured 2× gain is for **USDC**. Pump.fun and FireWatch retain their
instruction projections; their improvement has not been established. Native
counting already avoids the object graph. CAR code was not changed in this pass.

## Other findings

The full V2 epoch 700 USDC run used its sparse registry cache because the 2.17 GB
registry exceeded the 1 GiB full-cache limit. It reported 4.83 million source
reads and 391 GB requested, compared with a 75 GB main block file. These are
logical file reads, not necessarily physical disk bytes.

A 3 GiB full-registry override was tested only in the diagnostic. It cut the
small run to 126 source calls, but loading the whole registry for a 2,048-block
range made that run much slower. **The default memory limit was not raised.**
Future work must distinguish full-epoch scans from small ranges and account for
the cost of loading the registry.

On the sampled warm Pump.fun range, the new stage counters separated about
0.09 s of signature reads, 0.086 s of assignment, and 0.67 s of publication from
2.48 s total. The measured result-send and output-buffer waits were small on
this range. This does not rule out larger signature I/O waits on a cold full
epoch. The earlier full epoch 700 run spent 534 s in the combined consumer stage.

The named CPU profiles still show compact integer/message/metadata parsing,
zstd, instruction projection, and registry access. Next: remove temporary CPI
descriptor lists, measure instruction-output validation cost, and improve V3
read coalescing beyond four-block jobs. Do not remove ordering or coverage checks
based only on a CPU profile.

## Reproduction and evidence

Tool: `crates/blockzilla-reader-profile`. This is a diagnostic tool, separate
from the 12 public examples. Its block-range options do not alter example CLIs.
It supports all four workloads with V2/V3; V3 Pump.fun and FireWatch use reverse
candidates by default, with `--dense` available to isolate projection costs.

Example normal timing:

```sh
cargo run --release -p blockzilla-reader-profile -- \
  --archive-root /path/to/archive --epoch 700 --format v2 --workload usdc \
  --blocks 2048 --workers 12 --warmups 1 --iterations 3
```

Use `--allocations` in a separate run. For a Linux CPU flamegraph, build all
dependencies with frame pointers and unwind information:

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none \
RUSTFLAGS='-C target-feature=+aes,+sse2 -C force-frame-pointers=yes -C force-unwind-tables=yes' \
cargo build --release --target x86_64-unknown-linux-musl \
  -p blockzilla-reader-profile --features frame-profiler
```

Then add `--flamegraph /path/to/profile.svg`. Do not use that instrumented build
for the before/after timing table. The first profile from the normal static build
had no usable stack names; it is not evidence of CPU cost. The tool now rejects
profiles without resolved frames. Some frames in the valid profiles remain
unresolved; sampling is not an exact function-time measurement.

NAS evidence: `/volume1/blockzilla/benchmark-results/reader-profile-20260904-r1`.
The preserved `reader-profile-before` and `reader-profile-after` binaries have
the same release flags. Their initial V3 diagnostic path error is preserved in
the first logs. The successful `*-r2` V3 runs used a private symlink path adapter;
the current tool fixes the root path directly. No archive was modified.

Local raw results and named flamegraphs are in
`docs/benchmarks/reader-profiles-2026-09-04/`. These are diagnostic artifacts,
not files needed to build or run the SDKs.

The new production-run bundle is
`/private/tmp/sample-reader-package-20260904-allocation-reuse-final`.
The previous full-run results remain in the separate `stream-reuse-final`
package. Do not combine both builds into an unlabeled speed result.

## Deployment

The optimized bundle was deployed to the NAS on 2026-09-04. The old matrix
runner (PID 1429090) was stopped with its normal termination handler. Its
results were kept. The new runner is PID 1766612, with 12 workers, all 11 sample
epochs, all four workloads, local and network sources, in V2 → V3 → CAR order
(264 jobs). CAR has no new optimization in this pass.

NAS package and live log:

```text
/volume1/blockzilla/benchmark-results/sample-reader-package-20260904-allocation-reuse-final/
  runner.log
  results/status.json
```

The archive root is unchanged: `sample-reader-package-20260904-final/archive`.
No compactor, registry builder, download, or archive file was changed by this
deployment. The known V2 epoch 100 FireWatch execution-boundary failure is not
fixed by this allocation pass.

Initial check: five jobs passed (all four V2 epoch 0 workloads and V2 epoch 100
slot-hours). The runner then started V2 epoch 100 USDC. Cross-format parity is
pending until the other formats run. All 18 metadata projection SDK unit tests
passed on the NAS, including the new buffer-reuse and stale-row test.

The user then deferred network tests. The original runner was no longer active
when checked; the reason was not established. The same binaries and completed
local results were resumed with `--mode local`, PID 1795756. The active plan is
now 132 disk-only jobs, still V2 → V3 → CAR. The previous configuration and
summary are saved in `results/before-local-only-20260904/`, and the mode change
is recorded in `results/scope-change.json`. No network reader had started.
The unfinished epoch 200 Pump.fun job restarted in a new attempt directory;
successful jobs were not repeated.
