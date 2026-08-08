# Firewatch signer-to-program index redesign

Goal: given a wallet pubkey, return the program ids reached by transactions it
signed, per epoch, with a tractable build and a low-latency query path.

Status (2026-08-08): the production/correctness batch, safe Stage 1 work, and
the Stage 3 signer-dense builder are implemented. Stage 2 remains partial.
The legacy chunked builder is retained as a byte-level oracle while
`build-dense` is promoted through fixture and full-epoch gates.

## 1. Current semantic and publication contract

The shard format is version 3, the manifest schema is version 3, and semantic
version 1 means exactly:

- include successful transactions only;
- map every required transaction signer (fee payer and co-signers) to every
  distinct top-level and recorded inner/CPI program in that transaction;
- include vote transactions (the compact-vote flag is not an exact
  whole-transaction classifier);
- fail the build on raw transactions, successful raw/absent metadata, decode
  errors, invalid registry references, or unresolved signer/program pubkeys.

Failed transactions are a declared semantic exclusion, not an omission. They
are excluded before metadata availability/decoding is considered. A raw
transaction remains unclassifiable and fails the build even if its row says it
failed.

There is no partial-build mode. A publishable manifest records
`complete=true`, zero omission counts, the exact schema/format/semantic
versions, and the archive generation + registry binding.

Published builds hash every archive object, retain the opened file handles for
all later scan passes, and verify their file identities again before publish.
The Firewatch manifest separately binds both `registry.bin` and
`registry.mphf` by exact size and SHA-256, and records the original filesystem
identity of both. A trusted-local build does the same artifact hashing but
marks the generation identity as `trusted_local_asserted_immutable`; query
requires an explicit `--trust-local` acknowledgement and the exact original
archive path and file identities. A published query at that exact path takes
the same identity fast path; a relocated published archive must fully hash
both retained artifacts before lookup. All paths recheck their retained
artifact identities after lookup. Synthetic zero hashes from `open_trusted`
are never treated as content identity.

Every shard file has an exact size/SHA-256 binding in the manifest. A compact
content-bound `programs.map` captures the registry bytes for every distinct
program in the index, so a query never turns a valid relation id into a program
name using later-mutated registry bytes.

Builds write a same-filesystem staging directory, sync its files/directories,
and publish it with an atomic no-replace rename. Existing or racing output
paths are never overwritten.

## 2. Measured baseline (2026-08-08, NAS `Blockzilla-00`)

| | epoch-920 | epoch-900 (cold) |
|---|---:|---:|
| `archive-v2-blocks.zstd` | 104,003,932,861 B | 59,912,307,540 B |
| registry entries | 50,909,144 | 27,798,494 |
| legacy-discovered signer candidates | 10,629,622 (20.9%) | 5,045,706 (18.2%) |
| transactions | 620,318,150 | 476,026,811 |
| blocks | 431,291 | 431,858 |
| full pass @ 6 threads | 369.6 s | 230.9 s |

Measured `discover-signers` read throughput was 41 MB/s with one reader,
about 64 MB/s across two processes, 281 MB/s with six warm readers, and 259
MB/s with six cold readers. Large scans mostly bypass the bcache, so this is
real parallelism across the backing disks rather than a warm-cache artifact.
Those signer counts predate the strict successful-only discovery alignment;
the production rerun must record the smaller exact V1 counts before sizing the
final rollout.

The box has 12 cores and 7.5 GB RAM, with roughly 4.4 GB free while sharing
production work. The epoch-920 baseline projected about 47.8M distinct
relations, roughly 191 MB in `programs.rel` plus 170 MB in `wallets.idx`.

## 3. Root causes and current disposition

### R1. Duplicate relation amplification — landed

The original `record()` unconditionally appended and deferred sort/dedup until
write. About 1.67 billion pushes per pass could produce only about 47.8 million
distinct relations. Hot vote authorities and bots therefore consumed gigabytes
for answers that were only a few integers.

`record()` now keeps every wallet slot sorted and unique at insertion time,
with a consecutive-duplicate fast path and binary-search insertion. Parallel
builder merge is a true sorted-set union, so duplicates cannot return during
merge. The writer no longer sorts/deduplicates.

### R2. Account-id chunking rereads the archive — landed in `build-dense`

At 50.9M ids and a 5M chunk width, the legacy sharded builder performs eleven
full archive passes (about 1.14 TB read). `build-dense` instead performs one
signer-discovery pass and one relation-decode pass, independent of output shard
width. A generation-bound signer artifact can eliminate the discovery pass on
later builds of the same published generation.

### R3. Query loaded 2.32 GB — partly landed

The original query eagerly loaded `registry.mphf` and all 1.63 GB of
`registry.bin`, and could rebuild the index as a fallback, to resolve only a
few ids.

Current queries require an existing `registry.mphf`, never build it during a
lookup, and verify the returned wallet id against the exact retained registry
bytes. Returned programs are resolved from the small, content-bound
`programs.map` emitted by the build rather than from mutable `registry.bin`.

The MPHF values and membership-tag tables now stay file-backed instead of
being eagerly deserialized (about 596 MB at epoch-920). The loader structurally
preflights and then decodes only the compact MPHF function into owned memory;
each member lookup uses one positioned tag read and one positioned value read.
`IndexReader::open` still validates sortedness with an O(n) scan, so
the query path is substantially smaller than the baseline but is not yet a
few-KB/sub-millisecond operation.

### R4. Owned block decode — landed

Build and signer discovery now use
`borrowed_blocks_without_rewards_range`. Current-schema message/metadata bytes
are borrowed while rewards are validated and discarded, avoiding a full owned
block payload copy.

### R5. Read/decode pipeline stalls — landed in `build-dense`

Each individual reader remains synchronous, but Stage 3 splits disjoint block
ranges across decoder workers and sends bounded, recycled relation batches to
one accumulator owner. Decoder count no longer multiplies accumulator memory;
the queue has explicit batch-size and capacity limits.

### R6. Smaller correctness and efficiency issues

Landed:

- `ProgramTracker` walks insert-deduped lists.
- Legacy metadata decoding retains the no-CPI outcome fast path; V0 lookups
  and loaded writable/readonly classes are always decoded and matched exactly.
- Inner-instruction groups must reference a real top-level instruction, and
  message/account/count fields are bounded before allocation or iteration.
- Wallet id 0/out-of-registry and malformed shard geometry are rejected.
- Exact file lengths, trailing data, integer/range overflow, and relation
  slices beyond the declared relation count are rejected.
- Required signer counts are checked against static message keys before V0
  loaded addresses are appended.
- Manifest schema, index format, semantic policy, completeness, generation,
  both registry-artifact bindings, requested wallet identity, shard content
  digests, and the program-map digest are checked at query time using retained
  file handles. Retained shard/program-map identities are checked again after
  the lookup.

Remaining:

- Move the O(n) `wallets.idx` sortedness scan out of the lookup hot path
  without simply dropping corruption validation (for example, an explicit
  verify/preparation operation plus immutable verified-generation metadata).
- `programs_offset` remains `u64`; `u32` would save about 42 MB epoch-wide at
  the measured size, but requires another format change.

## 4. Landed implementation stages

### Stage 1 — build correctness/efficiency: landed

1. Insert-time sorted dedup and sorted-union parallel merge.
2. `ProgramTracker` observes already-deduped builders.
3. Borrowed/no-reward archive scans for build and signer discovery.
4. Bounded metadata/message streaming with direct + CPI completeness and exact
   V0 loaded-address class validation.
5. Wallet/shard/relation bounds hardening and strict malformed-data errors.
6. Successful-only outcome policy, all-signer cross-product semantics, and
   explicit vote inclusion encoded in the manifest.
7. Strict all-or-nothing publication with exact omission counts (always zero
   for a valid version-3 index).

### Stage 2 — query and binding: partial

Landed:

1. One positioned registry read for the requested wallet; returned programs
   resolve through the compact content-bound `programs.map`.
2. No query-time full-registry index build fallback.
3. Requested wallet id is checked against its exact 32-byte registry entry.
4. Exact size/SHA-256 bindings for both `registry.bin` and `registry.mphf`,
   with full retained-handle verification for relocated published archives or
   explicit asserted-immutable original-path identity checks for trusted-local.
5. Atomic immutable staged publication and strict manifest validation.
6. Safely file-backed `registry.mphf` value/tag tables, with only the
   structurally preflighted compact hash function decoded and the requested
   lookup slots read by position.
7. Per-shard size/SHA-256 bindings verified on the exact retained handles used
   for lookup, plus a bound program-id/pubkey map. Before a shard is blessed,
   a build-only full pass validates its wallet range, contiguous/complete
   relation coverage, relation bounds, and sorted nonzero in-registry program
   ids.
8. Full published archive hashing and pinned source handles across every
   multi-pass scan, followed by final content and identity checks of both
   registry artifacts.

Remaining:

1. Cache successful shard-digest and sortedness verification in a long-lived
   service so each lookup does not touch every wallet/relation page.
2. Benchmark a long-lived query service, where loading the compact MPHF
   function once and retaining file-backed shard handles may already give
   acceptable amortized latency.

## 5. Stage 3 implementation

### Pass 1: persisted signer discovery

`discover-signers --out signers.bits` persists a bitset over registry ids plus
a 128-bit-stride rank directory, so `registry_id -> dense_signer_rank` is O(1).
At epoch-920 the raw bitset is about 6.07 MiB and the rank directory about
1.52 MiB. The artifact binds the exact generation digest, registry size and
SHA-256, semantic policy, and its own payload digest. Loads are regular-file,
bounded, no-follow, FIFO-safe, and fully revalidate rank prefixes. The V1
format caps registries at 268,435,456 entries (about 40 MiB payload maximum).

Discovery and relation decode share the exact successful-only/raw-transaction
policy. Persisted artifacts are accepted only for published generations;
trusted-local builds run both passes in one process against retained handles.

### Pass 2: one signer-dense accumulator

Decode threads scan disjoint block ranges and send bounded batches of
`(dense_signer_rank, program_id)` pairs to one accumulator owner. Empty batch
buffers are recycled to their originating worker. The accumulator uses one
four-byte linked-list head per discovered signer, one eight-byte node per
distinct relation, and one registry-sized program bitset. Lists stay sorted
and unique during insertion, so repeated vote/bot traffic never allocates
duplicate nodes. Output walks those lists directly without a second CSR copy.

For the epoch-920 projection, signer heads are about **40.55 MiB**, the program
bitset about **6.07 MiB**, and 47.8M distinct relation nodes about **364.7 MiB**:
roughly **411 MiB** of logical accumulator storage before `Vec` capacity slack.
The resident signer-rank payload adds about 7.59 MiB. The default full-batch
queue holds at most 4 MiB of full pair payload; retained queue, worker, and
recycled `Vec` capacities are roughly 5 MiB at six workers. Six default
64 MiB reader-prefetch buffers can still add about 384 MiB, so the initial
operational budget remains **about 0.9–1.1 GiB** until full-epoch RSS is
measured; the reason is now reader/allocator overhead, not empty signer slots.
Non-default batch/thread/queue combinations are rejected if their combined
retained pair-buffer estimate exceeds 256 MiB.

### Output layout

Stage 3 deliberately preserves the version-3 shard layout byte-for-byte. Shard
width affects only output geometry and no longer causes another archive pass.
This keeps the existing query/binding contract and enables exact old/new
oracles. It does **not** enable the inverse `program -> wallets` query; that
still requires a separate postings index (or a full wallet scan).

### Full-epoch projection (fixture measurements are recorded below)

| | pre-batch baseline | landed batch | Stage 3 target |
|---|---|---|---|
| archive decode passes | 11 | 11, plus one required hash pass | 2, plus one required hash pass; 1 with a reused signer artifact |
| build wall clock | ~10 h (1 thread) | not remeasured | unmeasured at full epoch; do not extrapolate from discovery alone |
| peak memory | 3.8 GB | not remeasured; duplicate growth removed | logical accumulator ~411 MiB; budget ≥0.9–1.1 GiB initially |
| query | 2.32 GB, ~72 s | file-backed MPHF, bound shard/program map; verification scan remains | unchanged format; service caching remains Stage 2 work |
| output | 11 shards | 11 shards, format v3 | identical format-v3 shard geometry |

Stage 3 status:

1. Landed: `discover-signers --out signers.bits` with magic, version, registry
   count, semantic policy, generation/registry binding, and atomic no-replace
   publication.
2. Landed: `SignerRank::rank(registry_id) -> Option<dense_rank>` and ascending
   zero-allocation signer-id iteration.
3. Landed: compact dense builder with insertion-time dedup.
4. Landed: bounded decoder-to-accumulator pipeline and recycled buffers.
5. Landed: deterministic streaming writer for unchanged version-3 shards.
6. Optional/future: program-to-wallet postings if inverse queries become a
   real product requirement.

## 6. Verification status

Repeatable synthetic oracles now cover:

- multi-signer direct+CPI cross-products;
- V0 loaded CPI program resolution;
- exact V0 writable/readonly loaded-address cardinality and invalid inner-group rejection;
- failed/raw policy ordering;
- unresolved required-program rejection;
- static-signer-count enforcement before loaded-address append;
- insert dedup and overlapping/disjoint sorted-union merge;
- malformed/trailing index lengths and relation ranges;
- wrong-shard wallet ids, noncontiguous relation slices, duplicate/zero, and
  out-of-registry program ids before shard binding;
- shard/program-map digest mutation detection, post-lookup identity checks,
  and safe post-open truncation;
- relocated same-size `registry.bin` replacement with a matching MPHF, and
  independent same-size `registry.mphf` mutation rejection;
- symmetric manifest read/write size caps;
- semantic-version and wallet/shard bounds;
- positioned registry reads and exact 32-byte wallet decoding;
- staging-parent selection, including a relative `index` output path;
- atomic no-replace behavior when a destination path races publication.

The 3,000-block epoch-1000 production fixture now provides an end-to-end gate:

- 3,688,626 transactions, 3,000 blocks, and 1,975,310 registry entries;
- strict version-3 legacy builder, `build-dense --threads 1`, and
  `build-dense --threads 4` all produce exactly 158,492 wallets, 678,408
  relations, and canonical SHA-256
  `3abdaf0276492ca5f0c120e9399791ee00b7c806ae5dd1fe0e64f5b0854fd373`;
- the three physical `wallets.idx`, `programs.rel`, and `programs.map` outputs
  are byte-identical; manifests differ only by `built_unix_time`;
- signer discovery is deterministic at one/four threads: 158,492 successful
  signers, 443,615 failed transactions excluded, 3,688,626 transactions and
  3,000 blocks scanned;
- release timings/max RSS were: strict one-thread 6.13 s / 168.6 MiB, dense
  one-thread 7.46 s / 86.4 MiB; strict four-thread 3.26 s / 770.5 MiB, dense
  four-thread 3.64 s / 417.2 MiB. Dense pass 2 plus output was effectively at
  parity with strict accumulation (3.301 vs 3.266 s at one thread; 1.036 vs
  1.099 s at four); end-to-end overhead was the required discovery pass. The
  fixture therefore does not justify another recent-pair cache yet.

The older version-2 semantic oracle intentionally differs: 159,977 wallets and
484,364 relations. A streaming set diff found 1,681 old-only wallets, 196
strict-V1-only wallets, 7,661 old-only relations, and 201,705 strict-V1-only
relations, matching the successful-only exclusion and all-signer cross-product
changes rather than an output-order artifact.

Still required before production rollout:

1. Rebuild epoch-900, compare counts, and spot-check known wallets/programs.
2. Measure full-epoch release RSS/wall time and cold/warm query latency after
   the landed changes before selecting production chunk/thread settings.

## 7. Rollout and remaining risks

- Version-3 indexes must be rebuilt; older semantic/format manifests are
  deliberately rejected.
- The fixture gate passed; validate epoch-900 before rebuilding epoch-920.
- `build-dense` keeps the legacy builder available until epoch-900 parity and
  full-epoch RSS/performance gates complete.
- Linked-list insertion is linear in one wallet's distinct program count;
  measured lists are expected to be small, but pathological wallets must be
  included in the production profile before considering another structure.
- Channel backpressure is intentional and memory-bounded. If one accumulator
  cannot sustain measured pair rate, profile batch dedup/insertion before
  considering striped ownership.
- The message/metadata share of the compressed archive is not known from the
  repository and should be measured before proposing further column
  projection.
