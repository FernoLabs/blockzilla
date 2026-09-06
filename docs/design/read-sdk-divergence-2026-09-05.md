# Read SDK divergence: `wip/wire-profile-subsystem` vs `codex/sample-archive-benchmark`

Status: analysis note, 2026-09-05. Input to a merge decision, not a decision itself.

Two worktrees of the same repository, diverged at `f5ad4758`:

| | branch | HEAD | ahead of base |
| --- | --- | --- | --- |
| OURS | `wip/wire-profile-subsystem` | `b109fa40` | 4 |
| SAMPLE | `codex/sample-archive-benchmark` | `b5499e69` | 20 |

`crates/compact-v2/blockzilla-compact-v2-reader` diverged by **+24,586 / −5,950 across 30 files**. The
crate grew from 10,587 to 29,166 source lines. This is a different implementation
under the same crate name, not a delta that can be cherry-picked.

## Confidence

Six of nine planned subsystem analyses completed. The adversarial verification
pass did **not** run — every verifier hit a session limit. Findings below are
therefore **single-analyst and unverified**, though each cites `file:line`
evidence that can be checked directly. Three dimensions were not analysed:
CAR/V3 reader internals, and part of the migration surface. Treat perf claims as
hypotheses to benchmark, which is what they are.

## Verdict by subsystem

| Subsystem | Stronger | Note |
| --- | --- | --- |
| SDK layering | SAMPLE | read-sdk is the engine; facades are thin |
| Message projection | SAMPLE | borrowed + stack-inline vs owned Vecs |
| HTTP and cache | SAMPLE | but far less decisive than it looks |
| Reader core | MIXED | decode primitives byte-identical |
| Validation posture | MIXED | stronger crypto, weaker fail-closed |
| Metadata projection | **OURS** | SAMPLE is not a superset |

## The measurement trap: committed HEAD is not your working tree

Two analysts independently found that comparisons against `b109fa40` **understate
OURS**, because significant work is uncommitted:

- `reader.rs` — `b109fa40` has *no rayon dependency and is single-threaded end to
  end*. The working tree already contains `process_borrowed_blocks_parallel_ordered`
  plus two entry points SAMPLE has no equivalent for
  (`process_borrowed_blocks_parallel_batch_barrier{,_with_transaction_state}`),
  which decompress each frame once and lend the same retained bytes to two
  ordered stages.
- `selective_metadata.rs` — 1,245 lines at `b109fa40`, **3,367 in the working
  tree**. Its consumer crates `blockzilla-spyx-query` and
  `blockzilla-token-transaction-dump` are **untracked entirely**.

**Commit before benchmarking.** A committed-vs-committed run measures a
single-threaded reader you no longer have and would produce a badly misleading
result.

## Merge blockers

These are independent of speed. None is resolved by "SAMPLE is faster".

### 1. Marker assets are mutually rejecting — on both axes

Message grammar:

| | marker asset |
| --- | --- |
| OURS | `archive-v2-message-schema-post-unknown-fallbacks-v1.marker` |
| SAMPLE | `archive-v2-message-schema-current-v1.marker` |

The metadata-schema marker convention is *also* mutually incompatible and
mutually rejecting.

An archive published with OURS' markers is not recognised by SAMPLE's selector
and falls through to the Current default. On mainnet-beta epochs 0/1/2 SAMPLE
hard-fails with `HistoricalMainnetMarkerMissing`. The NAS archive carries OURS'
markers.

**Verify which markers the benchmark corpus carries before running anything**, or
a crash and a fallback path will be misread as reader performance.

### 2. `project_signers` has no SAMPLE equivalent

OURS has a signer-prefix-only decode that stops inside `account_keys`
(`message_projection.rs:95-104, :602-624`). Consumer:
`indexer/blockzilla-firebase-indexer/src/build.rs:1561` — the signer→program index.

> For a signer-discovery pass, OURS reads a few dozen bytes per message and
> allocates nothing; SAMPLE must parse the entire message body. The gap is
> proportional to message size, not to signer count.

Predicted **>1.5×** in OURS' favour on that workload. Port it forward or accept a
measured regression on an index that is actively being built.

### 3. Metadata capability loss

OURS is a family of zero-allocation visitor functions with three selectivity axes
(outcome-only, prefix-with-early-exit, full-record) plus a complete borrowed
compact-log event reader (~1,000 lines: `BorrowedArchiveV2LogEvent`,
`LogTables`, `ProgramLog`, `LogDataChunks`). **SAMPLE has no equivalent — only
`skip_logs`.**

SAMPLE's `CompactV2MetadataProjector` always walks the whole record to a
trailing-bytes check and materialises owned `Vec<Vec<InnerInstruction>>`;
`count()` and `*_reusing` were bolted on later (`10e1f310`) to claw back
allocations OURS never makes.

SAMPLE does add two things OURS lacks: Indexer-V3 split-plane projection
(outcome / loaded / inner / token-balance planes read separately), and several
per-record semantic checks.

### 4. Producer-side proof apparatus deleted

SAMPLE removed the full-generation dual-grammar audit, per-record metadata
classification counts, the `registry.mphf` ↔ `registry.bin` mapping proof, the
shared publication lock with its TOCTOU-checked no-clobber marker publication,
and the gateway's `AllFiles` re-hash and re-audit at startup.

## What SAMPLE does better

**Layering.** `blockzilla-query-sdk` is a true leaf contract crate with zero
workspace dependencies. `blockzilla-compact-v2-read-sdk` is 821 non-test lines
with **zero decode logic** — `scan_ordered` is a two-arm enum match forwarding to
`read_sdk::CompactV2InstructionSource`. Format adapters live inside the format
engines. read-sdk is the shared engine, not a superseded layer.

**Message projection allocation behaviour.**

| | OURS | SAMPLE |
| --- | --- | --- |
| Legacy vote transaction | 2 heap allocs | 0 |
| Raw instruction data | validated then discarded | `Cow::Borrowed` |
| Output graph | fresh per block | `ProjectionPool`, 8 MiB/worker cap |
| Count-only path | none | zero-allocation |

Vote transactions are the mainnet majority, so the 2→0 change applies to most
rows. Evidence: OURS `message_projection.rs:641` (`Vec::with_capacity`), `:665`
(`vote_hash_references`); SAMPLE `:315-325` (`SmallVec`, 8 keys / 4 instructions),
`:846-858` (`Cow::Borrowed`).

**Ordered-parallel pipeline** (`reader.rs:1482-1856`) — producer thread, rayon
pool, wave-granular ordered sink. Absent from OURS' *committed* HEAD; present in
its working tree.

**`read_range_into_slice`** — a zero-allocation innermost range read
(`source.rs:54`) with no OURS equivalent. Called once per registry chunk, block
frame, and signature window.

**Grammar decoupling.** `ArchiveV2WireProfile`: **452 references across 26 files
in OURS → 0 in SAMPLE**, replaced by `CompactV2MessageSchema` passed as a
parameter. This is the coupling problem solved, and it is the strongest
architectural argument for adopting SAMPLE's design.

**Cryptographic verification** OURS has none of: Ed25519 verification of
reconstructed signed messages, PoH entry-hash recomputation, blockhash-chain
continuity. SAMPLE's per-marker check is also *stronger* — it verifies the
manifest binding, independently reads the object, recomputes SHA-256 and
byte-compares, in every `HashVerification` mode.

## What the HTTP cache actually does — less than it appears

My initial assumption that `http_cache.rs` would dominate was wrong.

- Range coalescing is **byte-for-byte identical** on both sides: sum adjacent
  frames until `> prefetch_bytes`, default and hard cap 64 MiB.
- The cache is a **whole-object on-disk mirror, not a range cache**. It
  pre-downloads up to 8 named sidecar objects in serial 32 MiB GETs, then serves
  them by `pread` from a retained fd.
- **`archive-v2-blocks.zstd` is never cached in any facade.** Cold-vs-warm
  therefore affects index/meta/registry objects and random access only — *not*
  full-scan byte volume.
- No eviction of any kind. Two avoidable warm-path costs: a missing
  `read_range_into_slice` override, and a zero-fill before every `pread`.

Also: OURS' `HttpRangeSource` has **no in-repo consumer at all** — it is
constructed nowhere outside its own unit tests and the README.

SAMPLE's real network advantage is *overlap* (producer thread feeding a decode
pool) and per-worker concurrency in the V3 path, not caching.

## Reader core: no per-block CPU difference

`decode_compressed_block{,_reusing,_borrowed_reusing}`,
`decode_uncompressed_block{,_borrowed}`, `validate_exact_zstd_frame`,
`BlockIterator`, `BorrowedBlockStream::{next_block,refill}` **diff to zero** (or
to counter increments only). Any throughput difference lives above that layer, in
pipelining and projection — not in decode.

OURS also carries two per-record regressions relative to the shared base
`f5ad4758`: a dual-decode-and-re-serialize metadata canonicalization, and a
per-block `sort_unstable_by_key` over `ScannedTransaction`. The working tree has
fixed one; the other remains.

## Benchmark plan

**The harness already exists — do not build one.**

`bench/reader-profile` — *"Diagnostic harness, separate from the
small public examples. Calls the same format SDKs and workload sinks; discards
output bytes, not workload work."*

```
--archive-root --epoch --format --workload --first-block --blocks
--workers --iterations --warmups --allocations --flamegraph
--dense --registry-mib --wallet
```

`examples/workloads` — *"Small, format-neutral application
workloads... only the application rules and canonical output... all formats prove
parity with the same record bytes."* Four sinks: `firewatch`, `pump`, `usdc`,
`transaction_identity`. Consumed by all three read examples plus reader-profile,
so parity is checked across formats on identical bytes.

### Runs, ranked by decision value

1. **Allocations per transaction, full-epoch mix.** `--allocations`, Compact V2.
   Hypothesis: SAMPLE ≈ 0 per legacy vote transaction, OURS ≈ 2N. This is the
   cleanest signal and is allocator-counted, not timing-noisy.
2. **Signer-discovery scan.** The `project_signers` regression. Hypothesis: OURS
   >1.5× faster. If confirmed, porting `project_signers` becomes mandatory.
3. **Count / slot-hours workload.** Hypothesis: SAMPLE's largest margin of the
   four workloads, scaling with transactions/sec rather than bytes/sec.
4. **Raw-instruction-heavy selected workload** (USDC, Pump.fun). Hypothesis:
   bytes-allocated-per-selected-instruction roughly halves between `ffffcbe1` and
   `70f04c2d`; elapsed-time gain smaller than the allocation gain.
5. **Local NAS vs network transport, separately.** The cache only affects
   sidecars and random access. Run local to isolate decode/projection from
   transport.
6. **Facade overhead.** `CompactV2Archive::open_local` vs
   `CompactV2InstructionSource` directly. Expect <0.5%; more than ~1% means the
   cost is elsewhere.

### Runs to exclude, and why

- **CAR results prove nothing about this decision.**
  `blockzilla-car-read-sdk` has no dependency on `blockzilla-read-sdk`; a CAR
  speedup attributes entirely to `of-car-reader/src/query_sdk.rs` (+701 lines in
  `70f04c2d`).
- **`examples/archive-token-events` is not a Compact V2 measurement.** It is the
  only consumer of `blockzilla-archive-sdk`, a stale first-generation facade
  superseded three days after it landed (`4a0fa59b` → `b3c5765b`), never deleted.
  It exposes no parallel scan path and opens with `HashVerification::ControlFiles`
  instead of `SizesOnly`, inflating setup time.
- **Indexer V3 numbers are not transport-independent.** The V3 SDK imports
  `CachedHttpRangeSource` from read-sdk, so transport changes move V2 and V3
  together. If they move in *opposite* directions, the cause is in the format
  adapters.

## Suggested merge sequence

1. **Commit the working tree first.** Otherwise the parallel reader path and
   3,367-line `selective_metadata.rs` are invisible to the comparison, and the
   untracked consumer crates cannot be evaluated at all.
2. **Confirm the marker assets** on the benchmark corpus and on the NAS archive.
3. Run benchmarks 1, 2 and 5 — enough to settle the decision.
4. Adopt SAMPLE's read-sdk as the engine **if** benchmarks hold, porting forward:
   `project_signers`, the borrowed compact-log event reader, and the
   selectivity axes.
5. Decide separately on the deleted producer-side proof apparatus — it is
   publication-time tooling, not read-path, and can live in a `compat` crate.
6. Delete `blockzilla-archive-sdk` as part of the merge.

## Open questions

- How does SAMPLE read archives carrying OURS' markers — is a conversion step
  required, and does one exist?
- Was the dual-grammar audit deleted because the archive was frozen, or because
  the requirement was dropped?
- `blockzilla-car-read-sdk` and `blockzilla-indexer-v3-read-sdk` internals were
  not analysed.
- No adversarial verification ran. Every perf claim above should be treated as a
  hypothesis until benchmark 1 or 2 confirms it.
