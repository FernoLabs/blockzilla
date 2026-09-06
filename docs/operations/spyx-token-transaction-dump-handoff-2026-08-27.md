# SPYX token transaction dump handoff

Date: 2026-08-27
Last updated: 2026-08-28 19:31 CEST
Workspace: `/Users/augustin/Developement/ferno/blockzilla-v1`
Status: the schema-3 raw dump is complete and verified for all 218 epochs from 801 through 1018.
The extractor exited normally, and no dump tmux session is active. Section 14 contains the final
counts, hashes, and audit result. Consolidation into a portable global registry is not yet
implemented.

## 1. Objective

Make a small, lossless raw dump of all transactions that contain the SPYX mint or a known SPYX
token account.

The fixed anchor is:

- Mint: `XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W`
- InitializeMint transaction first signature:
  `51QCqbftjH2JdVScV8MUPEEGTTCBBwRdFLcJnhR3e7gVr5PGcJaL6HTh4hpxpJC6sjXGNafCW8eZEZxRuScDs49R`
- InitializeMint slot: `346066298`
- InitializeMint transaction index: `1509`
- Slots per epoch: `432000`
- Start epoch: `801`
- Slot offset in epoch 801: `34298`
- First archive index row: resolve it from the admitted index at run time
- Last admitted epoch: `1018`
- Decode workers: `12`

The raw dump must keep the exact Compact V2 message and metadata bytes. It must not remap pubkeys,
copy signatures, or reconstruct blockhash and vote-hash references during extraction.

## 2. Final design decisions

These decisions replace the earlier stateful token-account tracker design.

1. Extraction has discovery and raw-copy stages. The new single-read mode runs both stages for one
   retained block batch before it reads the next batch.
2. Pass A finds token-account creation only.
3. Pass A work is parallel. It does not update a tracker in transaction order.
4. Default two-pass mode freezes one complete token-account list after Pass A. Single-read mode
   merges new accounts in ledger order at each batch barrier and freezes the final list after the
   last epoch.
5. Pass B matches the complete transaction account list against the frozen list.
6. A match does not require instruction parsing in Pass B.
7. Pass B writes exact raw records directly from worker scratch buffers.
8. Raw record order does not have to be canonical.
9. Every record keeps its source epoch, slot, block ID, and transaction index. A later phase can sort
   the records.
10. Pubkey consolidation and signature collection occur only after the raw dump is complete.
11. Blockhash and vote-hash IDs stay opaque source IDs. `prev_blockhash_tail.bin` is not required or
    read.
12. The source wire profile is an explicit `post` assertion in trusted-local mode. The extractor
    does not run another profile audit.
13. Optional match hints move exact matching of already tracked accounts into Stage A. Clean-batch
    negative hints skip Stage B account decoding. A dirty-batch negative hint gets one exact
    post-merge check, so same-batch account creation stays correct.

## 3. Active extraction architecture

The active entry point is `extract_epoch_shards` in
`indexer/blockzilla-token-transaction-dump/src/extract.rs`.

The old stateful implementation remains in the same file as
`extract_epoch_shards_stateful_removed`, but it has `#[cfg(any())]` and is not compiled. Do not use
it as the current design reference.

### 3.1 Pass A: account creation discovery

In default mode, Pass A scans all admitted epochs before Pass B starts. In epoch-barrier mode, Pass
A completes for one epoch before a separate Pass B read of that epoch. In single-read batch mode,
Pass A completes for all blocks in one retained batch before Pass B uses the same decompressed
bytes.

It accepts only successful classic SPL Token or Token-2022 account initialization instructions:

- `InitializeAccount`, tag 1
- `InitializeAccount2`, tag 16
- `InitializeAccount3`, tag 18

It scans outer and inner instructions. It rejects creations from failed transactions. It records
the first successful source coordinate for each new account. The coordinate is used as a creation
floor, so a transaction before account creation does not match the future token account.

The supplied mint signature must occur exactly once as the first signature of a transaction at the
mint slot. The anchor lookup reads only the first signature of each transaction in that one slot.
It emits no signature bytes.

Pass A writes one immutable discovery artifact per epoch. It then writes one fixed, sorted global
account artifact.

### 3.2 Epoch-local ID tables

The extractor maps the applicable raw pubkeys through each epoch's `registry.mphf`. It does not
load the full registry into memory. Each epoch account-ID sidecar is a prefix: it contains the mint
and only accounts first created in that epoch or an earlier epoch.

For each positive MPHF lookup, it reads and compares the exact 32-byte `registry.bin` row. This
check protects against an incorrect positive result. It writes `account-ids.wincode` for the epoch.

The Pass B hot table uses:

- a dense `Vec<u64>` bitset for normal epoch-local IDs;
- a small sorted list for same-epoch IDs and their creation floors;
- a sorted raw-pubkey list;
- the target mint and its verified anchor floor.

There is no hash-table lookup in the Pass B transaction hot path.

### 3.3 Pass B: account-list presence and raw copy

Pass B checks these transaction accounts:

- static message accounts;
- loaded writable accounts;
- loaded read-only accounts.

It does not treat address lookup table descriptor keys as transaction accounts. It does not parse
instructions after it finds a static-account match. It skips metadata when the message has no
loaded addresses.

The result is an intentional safe superset. A transaction is selected when a tracked account is in
the transaction account list, even if no instruction uses that account. Failed transactions remain
eligible.

Each selected record contains:

- source epoch and synthetic or published generation digest;
- source wire profile;
- source block ID;
- slot and parent slot;
- raw `blockhash_id` and `previous_blockhash_id` bit patterns;
- block time and block height;
- total transaction count in the source block;
- source transaction index and row flags;
- source first-signature ordinal and signature count;
- exact source message bytes;
- exact source metadata bytes.

It does not contain signature bytes or block rewards. It does not change any CompactPubkey,
recent-blockhash, or vote-hash reference.

Workers serialize a borrowed Wincode record into a reusable worker buffer. A worker locks the
shared buffered output only while it writes one complete frame. Selected transactions are rare, so
this short lock is not part of the full scan hot path.

### 3.4 Single-read batch schedule

Use `--single-read-batches` for a new fused extraction. The reader does this work for each source
batch:

1. Read the compressed source range once.
2. Decompress each block once into retained worker buffers.
3. Run account discovery in parallel.
4. Merge discovery results in source row order.
5. Add new account IDs to the epoch match table.
6. Run raw matching and copy in parallel against the same decompressed bytes.
7. Recycle the buffers for the next batch.

The creation coordinate remains the eligibility floor. Thus, a precreation transaction in the
same batch is not selected. A later transaction in the creation batch and a transaction in the
next batch are selected. Progress JSON includes physical read, decompression, stage, time, and
buffer-reuse counters for each epoch. The mode rejects an owned legacy-block fallback.

These counters cover the extraction scan of `blocks.bin`. Trusted-local and normal published
control-file verification do not add a full block-file read. Published `--verification all`
intentionally reads the block file again to hash it.

The older `--epoch-barrier` mode is not this schedule. It makes one discovery read and one raw-copy
read for each epoch.

## 4. Zero-copy and allocation rules

"Zero-copy" starts after block decompression. Each worker decompresses one block into its retained
buffer. The borrowed decoder then reads the block, transaction rows, messages, and metadata from
that byte slice.

The optimized implementation now has these properties:

- `BorrowedDecodedBlock::storage_transaction_rows()` streams source storage rows without a row
  vector or sort.
- Pass A uses one `DiscoveryScratch` per worker.
- Pass A uses fixed 256-entry account arrays and reused small candidate buffers.
- Pass A does not create a `Vec`, `BTreeSet`, or `HashMap` for each transaction.
- Pass A skips failed rows before instruction discovery.
- Pass A reads metadata only when inner instructions or needed loaded references can change the
  result.
- Pass B uses one `MatchScratch` per worker.
- Pass B reuses the Wincode frame buffer.
- Pass B does not collect selected records in a vector.
- Pass B writes the borrowed message and metadata slices directly into the Wincode frame.
- The binary uses mimalloc. The allocator change is limited to this command.
- Normal compressed and decompressed buffers are retained and reused.
- Abnormally large frames can use a temporary buffer so one large frame does not permanently grow
  every worker buffer.

### 4.1 P1 registry and target-table optimization

The final P1 change reduces coordinator work at each single-read batch barrier:

- Reusable `Vec` lanes collect ID and raw-pubkey creation candidates.
- Epoch-local raw/ID mapping caches answer repeated candidates without more registry work.
- Unknown candidates are sorted and deduplicated. Each unknown account gets one bulk verified
  mapping.
- Nearby `registry.bin` rows are combined into positioned reads. Every MPHF result is still checked
  against the exact 32-byte registry row.
- The current-epoch ID table uses a full bitset. Sorted raw and ID deltas merge into retained
  vectors instead of rebuilding the table.
- The extractor reuses the same in-memory mappings for logs and validation.
- Telemetry reports candidate counts, unique ID and raw-reference counts, new accounts, MPHF
  lookups, registry rows, registry read calls and bytes, registry time, and target-table time.

P1 does not change schema 3 or the selected transaction set.

### 4.2 Single-read match hints

`--single-read-match-hints` is optional and requires `--single-read-batches`.

The reader keeps one reused byte for each transaction in the retained batch. Stage A sets that byte
when the transaction matches the exact pre-merge target table. After the account-creation barrier:

- a positive hint writes the exact transaction without another account decode;
- a negative hint in a clean batch skips the transaction without another account decode;
- a negative hint in a dirty batch gets one exact check against the post-merge table;
- creation-coordinate floors still reject transactions that occur before account creation;
- failed transactions can match, but they cannot create a tracked account;
- loaded writable and read-only accounts can match;
- address lookup-table descriptor keys do not match.

The transaction-state buffers are per retained block. They use safe reusable `Vec<u8>` storage.
The reader checks the aggregate 64 MiB state budget, resets active bytes before Stage A, and reports
active and retained capacity. There is no raw-pointer slicing.

This option is a performance choice, not an artifact identity choice. New baseline and match-hint
checkpoints write the same identity and hash. A baseline checkpoint can resume with hints enabled.
The reader also accepts the short-lived checkpoint form that stored `true`, but changed mint,
epoch range, source binding, and extraction mode remain strict errors.

The SDK keeps an owned fallback for old block schemas. Normal admitted Post blocks use the borrowed
path. Reader statistics now report borrowed storage blocks and owned schema fallback blocks.

Important streaming APIs:

- `BorrowedDecodedBlock::storage_transaction_rows()`
- `ArchiveV2MessageProjector::visit_static_accounts_and_instructions_exact(...)`
- `ArchiveV2MessageProjector::visit_static_accounts_exact(...)`
- `visit_archive_v2_token_metadata_exact_ordered(...)`

### 4.3 Historical transaction-error metadata

A Post outer block can contain transaction-error metadata from either historical metadata schema.
This condition is independent of the outer block wire profile. It does not permit an owned outer
block fallback.

Both Stage A match hints and Stage B now make this decision before they invoke a metadata callback:

- if the metadata starts with zero, the exact current-schema streaming visitor reads the source
  bytes directly;
- if the first byte is nonzero, the bounded and ambiguity-safe owned decoder selects the historical
  error schema and makes canonical current-schema bytes in a reused scratch buffer;
- the same exact streaming visitor reads those canonical scratch bytes;
- malformed or ambiguous metadata stops the run;
- a selected raw record still copies the original source metadata bytes without a change.

The program reports metadata owned fallbacks separately from outer block fallbacks. A nonzero
metadata fallback count is expected when historical error metadata occurs. The
`owned_schema_fallback_blocks` value must remain zero for this run.

## 5. Raw output and resume format

Schema version: 3.

```text
manifest.json
resume-checkpoint.json
accounts.wincode
discoveries/
  epoch-N/
    creations.wincode
epochs/
  epoch-N/
    account-ids.wincode
    transactions.wincode
    manifest.json
```

`transactions.wincode` has one framed header, zero or more framed transaction records, and one
framed footer.

Transaction frames can be in worker completion order. The resume validator does not require sorted
frames. It does require all of these conditions:

- every coordinate is unique;
- each slot is in the source epoch;
- each source block and transaction index is in range;
- repeated block context is consistent;
- the anchor occurs exactly as required;
- the header, footer, manifest, and SHA-256 values agree.

Default two-pass resume checkpoints bind these stages:

1. immutable discovery shards;
2. the frozen account list and anchor;
3. epoch account-ID logs;
4. immutable raw epoch shards and cumulative counters.

A partial file or directory is renamed to an `.abandoned-*` quarantine name. The program does not
delete it. A complete artifact is trusted only after validation. Use an explicit
`--last-epoch 1018`; otherwise a new source epoch can change the resume identity.

The worker count is also in the run identity. Resume the current dump with `--workers 12`.

Single-read checkpoints bind the extraction mode and the true anchor. Discovery and raw epoch
shards advance as one validated pair. A crash can leave a partial or one-sided pair. Resume moves
that artifact to quarantine and repeats the epoch. It never accepts a two-pass checkpoint as a
single-read checkpoint.

## 6. Source admission

NAS host: `ach@192.168.1.46`
Archive root: `/volume1/blockzilla/archive`

Epochs 801 through 1018 form the intended source range. There is no
`archive-v2-generation.json` in these directories. Use explicit trusted-local mode:

```text
--trusted-local
--cluster-id mainnet-beta
--slots-per-epoch 432000
--wire-profile post
--last-epoch 1018
```

Trusted-local mode requires the core archive files, `signatures.bin`, `registry.bin`, and a
non-empty `registry.mphf`. Its synthetic source identity binds admitted file names, sizes, and the
asserted wire profile. It does not authenticate file contents. It size-binds blockhash and vote
registries when present, but it does not read them.

All epochs were converted to the Post profile. Do not add a new full profile-verification pass to
this extraction. Fifty epochs have conversion-chain authority instead of a later direct Post scan:

```text
900
930-934
937-942
944-946
948
950-983
```

Epochs 1013 through 1018 do not have `prev_blockhash_tail.bin`. This is valid for this raw dump
because the dump keeps opaque blockhash IDs.

Published-manifest mode remains available. It rejects a sizes-only verification policy. Trusted-
local mode is the explicit exception.

## 7. Old performance result and diagnosis

The old binary scanned the epoch-801 suffix from the incorrect later `MintTo` slot `346330505`
with this result. Keep these numbers only as historical performance evidence; this range is not a
correct SPYX dump:

- blocks: `133281`
- transactions: `216024100`
- compressed bytes: `27112488598` (about 25.25 GiB)
- elapsed time: `1435.95 s`
- rate: `18.0065 MiB/s`
- aggregate worker CPU: about 1.66 cores
- host idle CPU: about 85 percent
- storage busy: about 4 percent
- average tasks per compressed batch: about 79 blocks

The 12-worker pool was active. I/O and batch task count were not the main limits. The old Pass A
allocated at least three new vectors for almost every transaction. Epoch 801 therefore caused at
least about 648 million allocations and a similar number of frees. A static musl allocator then
caused severe allocator-lock contention.

The changes in section 4 remove this per-transaction allocation pattern and add mimalloc.

A local synthetic full-epoch probe after the allocator change measured:

- 12 workers: `516.5 MiB/s`
- 1 worker: `176.4 MiB/s`

The synthetic fixture has very small compressed blocks and no selected transactions. It proves
worker scaling and allocator improvement. It is not a NAS performance prediction. A new NAS probe
was completed after this note was written; see the next section.

### 7.1 Obsolete optimized NAS two-pass result

This static binary predates the corrected `InitializeMint` anchor and must not be used for a new
dump. Its SHA-256 was:

```text
c671a166ef92d99d439df0a7334723834b335f77c13349558901c655b2cd87bf
```

Its NAS path is:

```text
/volume1/blockzilla/bin/blockzilla-token-transaction-dump-c671a166ef92d99d439df0a7334723834b335f77c13349558901c655b2cd87bf
```

The isolated test ran both passes over the epoch-801 suffix from the incorrect later `MintTo`
anchor. The zero-account result is evidence of the bad floor, not a valid SPYX result. Results:

- Pass A: 54.4505 seconds, 474.8622 MiB/s.
- Pass B: 52.9053 seconds, 488.7313 MiB/s.
- Total: 107.3923 seconds.
- Effective two-pass rate: 481.533 MiB/s.
- Blocks per pass: 133281.
- Transactions per pass: 216024100.
- Compressed bytes per pass: 27112488598.
- New token accounts found: 0.
- Raw transactions written: 1, the mint-anchor transaction.
- Pass A speed increase over the old measured rate: 26.372 times.
- All 12 workers were active. A live sample used about 5.7 CPU cores in aggregate.

The schema-3 root manifest is complete. Running the exact command again with `--resume` validated
all artifacts in 0.102 seconds and did not rescan source blocks.

Benchmark output:

```text
/volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-only-schema3-20260827T140516Z
```

Benchmark logs and lock:

```text
/volume1/blockzilla/token-transaction-dump-state/spyx-mainnet-e801-only-schema3-20260827T140516Z
```

## 8. Test and build state

The latest epoch-900 metadata compatibility gates passed:

- `cargo fmt --all -- --check`;
- default all-target tests: 42 library tests and 10 command-line tests;
- all-feature all-target tests: 42 library tests and 17 command-line tests;
- strict all-target Clippy with `-D warnings`, in default and all-feature modes;
- `git diff --check`;
- the x86_64-musl production build.

The read SDK gates also passed: 92 library tests, 5 `read-bench` tests, 6 scanner tests, formatting,
and strict all-feature Clippy.

The semantic and pipeline tests include:

- one worker versus 12 workers with equal normalized output;
- single-read versus two-pass with equal normalized output;
- one physical read per batch and one decompression per block;
- same-batch and cross-batch creation floors;
- raw/ID alias deduplication and earliest-coordinate selection;
- epoch-local cache reuse for a repeated creation in a later batch;
- combined and split registry row reads with exact counters;
- current/prior ID bitsets and sorted-delta merges across word boundaries;
- reusable transaction-state buffers, reset and reuse, budget rejection, and storage-order
  alignment;
- baseline and match-hint output equivalence;
- clean-batch direct matches and no-decode skips;
- dirty-batch exact checks and same-batch creation floors;
- failed loaded-account matching and address-table descriptor exclusion;
- current and historical error metadata in both Stage A and Stage B;
- fail-closed rejection of malformed or ambiguous historical error metadata;
- paired-epoch resume and interrupted-checkpoint recovery;
- borrowed versus owned Wincode record compatibility;
- unordered frame validation and duplicate-coordinate rejection.

Run these gates again if the code changes:

```sh
cargo fmt --all -- --check
cargo test -p blockzilla-token-transaction-dump --all-targets
cargo test -p blockzilla-token-transaction-dump --all-targets --all-features
cargo clippy -p blockzilla-token-transaction-dump --all-targets -- -D warnings
cargo clippy -p blockzilla-token-transaction-dump --all-targets --all-features -- -D warnings
git diff --check
```

The current production binary is an x86-64 static PIE. It is stripped and contains mimalloc.

```text
size: 2837440 bytes
SHA-256: 5c2f5c89da0a87a208543286018ea490515bd27d91ebf4d396a37b64f29fd94e
NAS path: /volume1/blockzilla/bin/blockzilla-token-transaction-dump-5c2f5c89da0a87a208543286018ea490515bd27d91ebf4d396a37b64f29fd94e
```

Build this static Linux binary from macOS with:

```sh
env -u CPATH \
  RUSTFLAGS='-C target-feature=+crt-static,+aes,+sse2' \
  cargo build --release \
  --target x86_64-unknown-linux-musl \
  -p blockzilla-token-transaction-dump
```

The local path is:

```text
target/x86_64-unknown-linux-musl/release/blockzilla-token-transaction-dump
```

### 8.1 Opt-in CPU flamegraph build

The default production binary does not contain the profiler dependency or its command-line
options. Do not replace or attach to the active full-dump process for profiling. The active binary
is stripped, and the NAS denies unprivileged `perf` attachment with
`kernel.perf_event_paranoid=3`.

For a separate benchmark, build the opt-in in-process profiler with symbols and frame pointers:

```sh
env -u CPATH \
  RUSTFLAGS='-C target-feature=+crt-static,+aes,+sse2 -C force-frame-pointers=yes' \
  cargo build --profile release-debug \
  --features cpu-profile \
  --target x86_64-unknown-linux-musl \
  -p blockzilla-token-transaction-dump
```

The corrected baseline profiler binary keeps debug symbols and frame pointers. It is not stripped.
It predates the match-hint option and must not be used to measure the new option.

```text
size: 52872544 bytes
SHA-256: 2d4d13c27abcf61aa8e9b096886b99f40f361abb79d290b0f739d1e473704901
local path: target/x86_64-unknown-linux-musl/release-debug/blockzilla-token-transaction-dump
NAS path: /volume1/blockzilla/bin/blockzilla-token-transaction-dump-profile-2d4d13c27abcf61aa8e9b096886b99f40f361abb79d290b0f739d1e473704901
```

This path uses the same optimized release settings, with symbols retained. The in-process `pprof`
sampler does not require root access or a change to `perf_event_paranoid`.

The profiler requires two explicit absolute output paths. The flamegraph path must end in `.svg`,
the leaf-sample table must end in `.tsv`, both parent directories must already exist, and neither
output can already exist. It refuses overwrite and ambiguous output. Sampling defaults to 49 Hz
and 60 seconds. `--profile-skip-seconds` delays the sampling window, and
`--profile-duration-seconds` bounds it. If extraction ends first, the shorter profile is flushed
without waiting for the unused duration. If the duration ends first, the profile is written while
extraction continues. Sampling starts after command-line parsing and configuration validation.
Archive admission and resume checks still run inside the command, so use the delay when the target
is steady-state scanning.

The `.top.tsv` file ranks leaf or self samples. It is not an inclusive call-tree table. Use the SVG
for inclusive path attribution, and keep allocation or unsafe-code changes behind a measured hot
path plus exact-output tests. Profiled elapsed time is diagnostic because sampling and SVG writing
perturb the run. Use non-profiled release binaries for the formal old/new timing comparison.

### 8.2 Baseline flamegraph result

The corrected profiler completed a full epoch-801 run. It accepted 33129 samples over 281.218
seconds. The graph contains Rust, Wincode, Blockzilla, and Zstandard symbols. It is not the earlier
invalid graph that contained only thread IDs.

```text
SVG: /volume1/blockzilla/token-transaction-dump-state/profiles/spyx-e801-p1-2d4d13c2-20260827T2240/cpu.svg
SVG SHA-256: dcaaab9f0dbc6746f263aecbdff93d098cd00751bbada98b536af0c3996f7f15
TSV: /volume1/blockzilla/token-transaction-dump-state/profiles/spyx-e801-p1-2d4d13c2-20260827T2240/cpu.top.tsv
TSV SHA-256: 43fd5872caa3b400baa5b0ffeabab704612f2023702126accce03f0b35718273
```

The inclusive graph assigns about 34.3% to Stage A and 36.7% to Stage B. Integer Wincode reads are
the largest shared decode family. Allocator leaves are about 0.04%, so allocation work is not the
next large target. The frame-pointer profiler omits the interrupted instruction pointer. Use
inclusive paths, not exact leaf ownership, for decisions.

### 8.3 Completed epoch-801 match-hint check

The match-hint production binary completed epoch 801 while the full dump read the same NAS volume.
The output passed resume validation and matched the baseline byte for byte.

```text
output: /volume1/blockzilla/token-transaction-benchmarks/spyx-e801-hints-748faedb-20260827T2350
state: /volume1/blockzilla/token-transaction-benchmark-state/spyx-e801-hints-748faedb-20260827T2350
wall time: 557.998356 seconds
producer read time: 556.996086 seconds
Stage A: 120.631346 seconds
Stage B: 3.417655 seconds
merge: 0.114044 seconds
```

The baseline epoch-801 Stage B time was 74.045322 seconds. Match hints reduced it by 95.4%.
Stage A plus Stage B fell from 177.245277 seconds to 124.049001 seconds, a 30.0% reduction.

The hint decisions were:

- 4802 clean batches and 2 dirty batches;
- 2 direct positive hints;
- 642063252 negative transactions skipped without Stage B account decoding;
- 264899 dirty-batch negative transactions checked exactly;
- 3 selected transactions, 1 tracked account, and 0 owned block fallbacks.

The 557.998-second wall time is not a fair baseline comparison. The active full dump controlled the
shared NAS read rate. Use the internal stage times as the current CPU evidence.

### 8.4 Historical queued one-epoch A/B benchmark

The replacement tmux waiter was configured to run one baseline epoch and one match-hint epoch after
the full dump ended. It used the same `748faedb...` production binary for both runs and fresh output
paths. It refused to start if the full dump had no complete root manifest. The waiter exited safely
at 02:36 CEST because the root manifest was absent after the epoch-900 stop. It did not run either
benchmark, and the tmux session no longer exists.

```text
tmux session: spyx-hint-ab-after-full
runner: /volume1/blockzilla/bin/spyx-hint-ab-after-full-20260828T0002-76ad754e.sh
runner SHA-256: 76ad754ee61e6307533e89c60c3ed52ea3148ee795c77c832791d9ae2ad252da
state: /volume1/blockzilla/token-transaction-benchmark-state/spyx-e801-ab-748faedb-20260828T0002
baseline output: /volume1/blockzilla/token-transaction-benchmarks/spyx-e801-ab-baseline-748faedb-20260828T0002
hint output: /volume1/blockzilla/token-transaction-benchmarks/spyx-e801-ab-hints-748faedb-20260828T0002
```

## 9. Current NAS run state

All output that used the later `2xCK...` transaction at slot `346330505` is evidence only. It is not
a valid SPYX dump. Do not resume it, add files to it, or use its output root for a true-anchor run.
The correct identity uses the `51QC...` transaction at slot `346066298` and requires a fresh output
root.

A true-anchor two-pass spare run kept the NAS busy during development:

```text
tmux session: spyx-two-pass-spare
source range: epochs 801-1018
workers: 12
mode: epoch-barrier, two physical reads per epoch
```

Epoch 801 completed in this spare run:

- discovery: 156.121 seconds at 489.266 MiB/s;
- raw copy: 156.477 seconds at 488.155 MiB/s;
- blocks per stage: 397107;
- transactions per stage: 642328154;
- discovered token accounts: 1;
- raw transactions: 3.

The spare run completed epochs 801-805 and was stopped with Ctrl-C during epoch 806 discovery. Its
completed and partial files remain at:

```text
output: /volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-epoch-barrier-spare-20260827
logs: /volume1/blockzilla/token-transaction-dump-state/spyx-mainnet-e801-e1018-epoch-barrier-spare-20260827
```

This run has no epoch-barrier resume support. Treat it as benchmark evidence only. Do not use its
root for single-read mode because the extraction-mode identity and checkpoint sequence differ.

The fresh epoch-801 single-read NAS check completed in 186.4335 seconds. It found one token account
and wrote three transactions. It proved these physical-reader invariants:

```text
batches = reads = 4804
blocks = decompressions = stage-A visits = stage-B visits = borrowed blocks = 397107
owned block fallbacks = 0
compressed bytes = 80095300097
```

The creation log, frozen account list, epoch account-ID log, and transaction stream were
byte-for-byte equal to the corrected two-pass epoch-801 artifacts. A resume-only validation then
completed successfully in about 0.5 seconds.

The full single-read run resumed the same output root with the earlier match-hint binary at epoch
857:

```text
tmux session: spyx-single-read-full
output: /volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-single-read-20260827T201409
logs: /volume1/blockzilla/token-transaction-dump-state/spyx-mainnet-e801-e1018-single-read-20260827T201409
binary SHA-256: 37c7128a8825e3adc2102373db31823740a6c1c67abd7de734054495600dd024
binary path: /volume1/blockzilla/bin/blockzilla-token-transaction-dump-37c7128a8825e3adc2102373db31823740a6c1c67abd7de734054495600dd024
launcher: /volume1/blockzilla/bin/spyx-full-resume-hints-37c7128a-e148ea77.sh
```

The old process stopped after epoch 856 was durable. Recovery preserved both partial epoch-857
lanes under `.abandoned-*` names, validated 56 completed epoch pairs, and resumed epoch 857 with
49762 tracked accounts.

The first live hinted epoch, epoch 857, completed with:

- 139.681 seconds at 453.024 MiB/s;
- Stage A 82.477 seconds, merge 6.586 seconds, and Stage B 3.042 seconds;
- 3866 clean batches and 109 dirty batches;
- 16820 direct hints, 544721239 no-decode skips, and 15035505 exact dirty checks;
- 16821 selected transactions, 49897 tracked accounts, and zero owned fallbacks;
- an ETA of 22490 seconds, about 6 hours 15 minutes.

Compared with the preceding non-hinted epoch 856, Stage B fell by 92.5%, total epoch time fell by
13.6%, and compressed throughput increased by 18.4%. That process then completed epochs 858-899.

### 9.1 Epoch-900 stop and proof

The earlier binary stopped at epoch 900, slot `388800002`, transaction index `1151`:

```text
Error: invalid Archive V2 block at slot 388800002: transaction 1151: Invalid tag encoding: 2
```

The source index identifies this as row 2, block ID 2. The compressed block starts at byte 105683,
has a compressed length of 347683 bytes, an uncompressed length of 1038762 bytes, and 1597
transactions. Its signature range starts at ordinal 0 and contains 1787 signatures.

The source block is valid. The proof is:

- the outer block and all messages use the Post profile;
- an exact Post probe parses the messages in this block;
- a Pre probe fails on transaction 0 with `Invalid tag encoding: 7`;
- the epoch-900 conversion descriptor says that legacy Pre messages were rewritten to Post and
  metadata bytes were unchanged;
- the full generation dual-profile scan found 476026811 typed messages, 471302797 Post-only
  messages, 4724014 equivalent messages, zero Pre-only messages, and zero invalid messages;
- the read SDK bounded metadata canonicalizer accepts transaction 1151 as historical error
  metadata.

The defect was in the token-dump metadata hot path. That path sent all error metadata directly to
the exact current-schema streaming visitor. A Post outer block can still contain the historical
transaction-error schema. The current-schema error decoder then read the historical bytes at the
wrong offset and reported tag 2. The source data is not corrupt, and this is not a message-profile
error.

The safe fix is in section 4.3. It selects historical error metadata before any streaming callback,
uses the bounded and ambiguity-safe canonicalizer, and then uses the exact visitor on canonical
scratch bytes. It does not retry a general visitor error. Thus, it does not hide unrelated data
damage and it cannot keep partial callback state. Raw output still contains the exact source
metadata bytes.

### 9.2 Durable prefix and restart

Before the corrected restart, the authenticated checkpoint contained exactly 99 paired epochs,
801-899. Its cumulative counters were:

- selected transactions: 1222262;
- blocks: 42667109;
- scanned transactions: 58119387206;
- owned outer block fallbacks: 0.

The failed attempt left only these incomplete epoch-900 artifacts:

- an empty `discoveries/epoch-900.partial` lane;
- a 145-byte header in `epochs/epoch-900.partial/transactions.wincode`;
- no pending or staging checkpoint.

The missing root manifest was expected because the full dump was not complete. Resume renamed the
two partial lanes to these exact quarantine paths:

```text
/volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-single-read-20260827T201409/discoveries/.abandoned-epoch-900-partial-1787899183898-0
/volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-single-read-20260827T201409/epochs/.abandoned-epoch-900-partial-1787899183930-0
```

Do not delete these artifacts manually.

The corrected restart uses the same output root and identity:

```text
tmux session: spyx-single-read-full
process ID at start: 252572
start time: 2026-08-28 08:38:50 CEST
output: /volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-single-read-20260827T201409
logs: /volume1/blockzilla/token-transaction-dump-state/spyx-mainnet-e801-e1018-single-read-20260827T201409
binary SHA-256: 5c2f5c89da0a87a208543286018ea490515bd27d91ebf4d396a37b64f29fd94e
binary path: /volume1/blockzilla/bin/blockzilla-token-transaction-dump-5c2f5c89da0a87a208543286018ea490515bd27d91ebf4d396a37b64f29fd94e
launcher SHA-256: 34f13e73593183c60faea3c827de2902a18eed864092e6439c878edf40fcb2b1
launcher path: /volume1/blockzilla/bin/spyx-full-resume-hints-5c2f5c89-34f13e73.sh
```

The process emitted `run_start` at 09:20:07.565 CEST with status `resumed`. It reported 99 completed
epochs, 99 resumed epochs, and 218 total epochs. It started epoch 900 with 56182 tracked accounts.

Epoch 900 completed successfully at 09:22:19.603 CEST. Its exact result is:

```text
elapsed_seconds=132.023955509
blocks=431858
transactions=476026811
selected_transactions=3402
tracked_accounts=56292
compressed_bytes=59928994141
compressed_mib_per_second=432.89675953116154
output_transactions=3402
```

The exact reader counters are:

```text
block_count=431858
borrowed_storage_blocks=431858
owned_schema_fallback_blocks=0
batch_count=3588
read_call_count=3588
compressed_bytes=59928994141
decompression_count=431858
decompressed_bytes=206649738297
stage_a_block_count=431858
stage_b_block_count=431858
producer_read_seconds=106.081865139
stage_a_seconds=111.301657524
merge_seconds=4.984897251
stage_b_seconds=3.5614156279999998
producer_wait_for_free_buffer_seconds=17.761077387
coordinator_wait_for_ready_batch_seconds=3.89211537
max_compressed_batch_bytes=16777155
max_declared_uncompressed_batch_bytes=67228244
max_live_decompressed_batch_bytes=67228244
max_retained_decompressed_capacity_bytes=169714006
decompressed_buffer_reuse_count=430620
decompressed_buffer_growth_count=1238
transaction_state_buffer_reuse_count=430467
transaction_state_buffer_growth_count=1371
max_live_transaction_state_bytes=163242
max_retained_transaction_state_capacity_bytes=327634
```

The exact extractor counters are:

```text
creation_candidates=150
unique_candidate_ids=110
unique_candidate_raw_refs=0
new_accounts=110
registry_rows_read=455
registry_coalesced_read_calls=454
registry_read_bytes=14624
mphf_lookups=56295
registry_resolution_seconds=12.323128852
target_build_seconds=0.128205325
target_finalize_seconds=0.032623259
discovery_validation_seconds=0.140534759
raw_validation_seconds=0.034430962
clean_hint_batches=3486
dirty_hint_batches=102
hint_direct_matches=3396
hint_skips_without_decode=462437583
hint_exact_reparses=13585832
metadata_owned_fallbacks=12154115
```

The epoch-complete event reported:

```text
elapsed_seconds=132.023992228
completed_epochs=100
resumed_epochs=99
eta_seconds=15580.51941357
compressed_mib_per_second=432.896639132317
```

All physical read, decompression, borrowed block, and stage visit counts agree. The metadata
fallback count proves that the corrected path handled the historical error schema. The zero outer
fallback count proves that the run stayed on the borrowed Post block path. The process then started
epoch 901. Its one-epoch live ETA was 15580.519 seconds, or 4 hours 19 minutes 41 seconds, with an
estimated completion time near 13:42 CEST.

## 10. Historical monitoring and recovery record

The corrected build and resumed full launch are complete. The commands below are kept only as an
operator record. There is no active archive reader to monitor. Do not restart extraction unless a
new run is separately authorized.

### 10.1 Inspect the saved full-run state

```sh
tmux list-sessions
tmux capture-pane -p -t spyx-single-read-full -S -80
tail -n 40 /volume1/blockzilla/token-transaction-dump-state/spyx-mainnet-e801-e1018-single-read-20260827T201409/stderr.log
```

The tmux session is expected not to exist because the run completed. Inspect the saved log and
root controls instead. Do not modify or restart the completed artifact.

### 10.2 Reader proof for every completed epoch

Check each `single_read_reader_stats` JSON event. It must show:

- `read_call_count == batch_count`;
- `decompression_count == block_count`;
- `stage_a_block_count == block_count`;
- `stage_b_block_count == block_count`;
- `borrowed_storage_blocks == block_count`;
- `owned_schema_fallback_blocks == 0`;
- `metadata_owned_fallbacks` is present and can be nonzero;
- nonzero compressed and decompressed byte totals;
- reasonable producer, stage A, merge, and stage B times;
- retained/live buffer maxima and reuse/growth counts.

### 10.3 Historical exact recovery command

Set `NEWBIN` to the exact corrected binary used by the resumed run:

```sh
NEWBIN=/volume1/blockzilla/bin/blockzilla-token-transaction-dump-5c2f5c89da0a87a208543286018ea490515bd27d91ebf4d396a37b64f29fd94e
```

Keep this command as provenance only. Use it only if a new recovery run is separately authorized:

```sh
"$NEWBIN" extract \
  /volume1/blockzilla/archive \
  /volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-single-read-20260827T201409 \
  --mint XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W \
  --mint-slot 346066298 \
  --mint-signature 51QCqbftjH2JdVScV8MUPEEGTTCBBwRdFLcJnhR3e7gVr5PGcJaL6HTh4hpxpJC6sjXGNafCW8eZEZxRuScDs49R \
  --workers 12 \
  --last-epoch 1018 \
  --trusted-local \
  --cluster-id mainnet-beta \
  --slots-per-epoch 432000 \
  --wire-profile post \
  --single-read-batches \
  --single-read-match-hints \
  --resume
```

The verified launcher contains this exact command, checks the binary SHA-256, and appends both log
streams. Start it in the same tmux name only after the old session is absent. Keep the same output
root and identity because this is a resume, not a new run.

```sh
tmux new-session -d -s spyx-single-read-full \
  /volume1/blockzilla/bin/spyx-full-resume-hints-5c2f5c89-34f13e73.sh
```

Attach with:

```sh
tmux attach -t spyx-single-read-full
```

The phase name is `single_read_batches`. Confirm the physical-read invariants in each
`single_read_reader_stats` event. After a controlled stop, start the same command again with
`--resume`. Confirm that the extractor validates each paired epoch, quarantines an incomplete pair,
and continues at the next epoch.

## 11. Later consolidation phase

Schema-3 consolidation is intentionally disabled. The current `consolidate` command returns an
explicit error. This prevents the old remapper from accepting the new raw format.

The later phase must use the much smaller raw dump and do this work:

1. Read all raw transaction records.
2. Sort records by `(epoch, slot, source_block_id, tx_index)` when canonical order is required.
3. Reopen the bound source epoch `registry.bin` files.
4. Resolve all typed CompactPubkey references in selected message and metadata bytes.
5. Build one deterministic, one-based global pubkey registry.
6. Build one old-ID-to-new-ID map per source epoch.
7. Rewrite only CompactPubkey references with the existing wire-rewrite visitors.
8. Reopen each source epoch `signatures.bin`.
9. Copy the selected signature ranges to one global `signatures.bin`.
10. Set each final record's dump signature ordinal.
11. Validate that every pubkey resolves to the same 32-byte key before and after rewriting.

Keep raw shards unchanged. Blockhash and vote-hash references remain source-bound opaque IDs unless
an explicit optional resolver is added later.

## 12. Main code map

- `indexer/blockzilla-token-transaction-dump/src/main.rs`: command-line interface.
- `indexer/blockzilla-token-transaction-dump/src/pipeline.rs`: public configuration and entry points.
- `indexer/blockzilla-token-transaction-dump/src/extract.rs`: two-pass and single-read extractors,
  probe, fast tables, worker scratch, and raw writer.
- `indexer/blockzilla-token-transaction-dump/src/format.rs`: schema-3 Wincode records and manifests.
- `indexer/blockzilla-token-transaction-dump/src/resume.rs`: authenticated staged resume and
  quarantine logic.
- `indexer/blockzilla-token-transaction-dump/src/progress.rs`: JSONL phase, epoch, rate, and ETA
  reports.
- `indexer/blockzilla-token-transaction-dump/src/consolidate.rs`: raw validation; schema-3
  consolidation is disabled.
- `indexer/blockzilla-token-transaction-dump/src/registry.rs`: registry and rewrite support for the
  future consolidation phase.
- `indexer/blockzilla-token-transaction-dump/src/allocator.rs`: binary-only mimalloc selection.
- `indexer/blockzilla-token-transaction-dump/src/profiling.rs`: feature-gated, bounded in-process CPU
  profiling.
- `crates/compact-v2/blockzilla-compact-v2-reader/src/reader.rs`: borrowed block pipeline, reusable buffers, storage-row
  iterator, and reader statistics.
- `crates/compact-v2/blockzilla-compact-v2-reader/src/message_projection.rs`: exact borrowed message visitors.
- `crates/compact-v2/blockzilla-compact-v2-reader/src/selective_metadata.rs`: exact streamed inner-instruction and
  loaded-address visitor.
- `crates/blockzilla-format/src/v2/wire_rewrite.rs`: existing pubkey wire-rewrite support for the
  later phase.

The crate README is `indexer/blockzilla-token-transaction-dump/README.md`.

## 13. Worktree safety

The worktree has many existing modified and untracked files from Compact V2 conversion work. These
files belong to the user. Do not reset, clean, or replace the worktree.

The token-dump crate is currently untracked as a directory. The root workspace and lock file also
have changes. Inspect exact diffs before a commit. Keep unrelated archive conversion changes intact.

## 14. Final full-run result

The full single-read run completed on 2026-08-28 at 19:31 CEST. PID `252572` exited normally, the
`spyx-single-read-full` tmux session ended, `resume-checkpoint.json` reached stage `complete`, and
the root `manifest.json` has `complete: true`. Do not use the recovery command in section 10 unless
a new, separately authorized run is required.

The final controls are:

- output root:
  `/volume1/blockzilla/token-transaction-dumps/spyx-mainnet-e801-e1018-single-read-20260827T201409`;
- root manifest: 779 bytes, SHA-256
  `841a8511cf1ad80060641bf0b81fa7feafe35fa71bc619312e39d71cd1d36783`;
- resume checkpoint: 183,478 bytes, SHA-256
  `3b520de5e5df86d2e9ff1fcac65a98389e43dd8313c4280f90585637e7b0ab9c`;
- domain-separated checkpoint payload SHA-256
  `235707838ca21648b7996059790c48403c64712838a796b950886ba494a67bf9`;
- frozen accounts: 134,942 records, file SHA-256
  `e77aa22f96283ede6b8732bd48fdc34567582d09c10a65f024c70cda45f07060`;
- epochs: 218, with exact continuous discovery and raw-shard ranges 801 through 1018;
- selected transactions: 7,311,137;
- blocks scanned: 93,982,801;
- source transactions scanned: 123,831,042,775;
- outer owned-block fallbacks: 0;
- mint-anchor transactions: 1.

A separate read-only verification hashed 14,158,430,473 bytes. It checked all 218 discovery logs,
all 218 transaction streams, all 218 account-ID logs, all 218 shard manifests, the frozen account
file, both root controls, and both files in the preserved quarantine directories. Every physical
hash matched the authenticated checkpoint and every shard manifest. The summed counters matched
the root manifest and checkpoint. Epoch 900 remained bound to source-generation digest
`2477cfc2e93bf8ee85c6ea9092de810e947e8fbe8cc092d4094b48da1a9a752e`.

The root inventory is exactly `accounts.wincode`, `discoveries`, `epochs`, `manifest.json`, and
`resume-checkpoint.json`. There are no live partial or pending entries. The only allowed quarantine
directories are the four preserved recovery records:

- `discoveries/.abandoned-epoch-857-partial-1787869104464-0`;
- `discoveries/.abandoned-epoch-900-partial-1787899183898-0`;
- `epochs/.abandoned-epoch-857-partial-1787869104465-0`;
- `epochs/.abandoned-epoch-900-partial-1787899183930-0`.

The archive publisher must use the exact manifest and checkpoint bindings above. It must also use
the separately captured live-process authority receipt. Publication remains blocked until the
publisher has passed its full tests and two independent audits. Root-only source freeze and atomic
cutover also require separate operator authorization.
