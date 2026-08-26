# Blockzilla replay POC

This crate is the first POC for Blockzilla's replay-first runtime. It has now
completed a manifest-bound Compact Archive V2 mutation replay across mainnet
epochs 0 through 33 on a native x86-64 Linux NAS. The first epoch-34 attempt
stopped at the first legacy BPF-loader `Write`; support is now implemented but
the full epoch-34 retry has not completed. This POC does not claim to be an SVM
or a parity implementation of the historical Bank.

The complete epochs 0 and 1 corpus scan found no BPF-loader, user-SBF, or token
program instruction. The measured epoch 0–1 path is therefore the launch-era
Bank plus System, Config, Stake, and Vote native builtins. Epoch 34 is the first
observed loader-activation boundary; the compiler experiment is not presented
as part of the epoch 0–1 critical path.

Implemented now:

- canonical ELF extraction from legacy BPF-loader, upgradeable Buffer, and
  upgradeable ProgramData account layouts;
- allocation-padding trimming and content-addressed artifact keys;
- SBPFv0 load plus `RequisiteVerifier` safety validation;
- execution without protocol CU accounting, plus a fixed host-safety watchdog;
- target-aware in-memory native compilation: `solana-sbpf` on x86-64 and a
  deliberately small Cranelift lowering on AArch64, with an explicit
  interpreter fallback when an otherwise valid program is outside the native
  subset;
- mainnet genesis fingerprint/epoch-window inspection;
- owner/executable/rent/data byte-range account diffs at nested instruction
  boundaries, with lamport output disabled by default;
- manifest-bound Blockzilla Compact Archive V2 decoding with exact
  `genesis.bin`, registry, blockhash, slot, transaction, and instruction
  ordering, including a lending, borrowed current-schema hot-block path that
  reuses decompression storage and exposes checked message and metadata slices;
- a transaction-only borrowed replay view that discards unused archived reward
  rows during current-schema decode, while preserving the owned compatibility
  path;
- a launch-era Solana v1.0.7 native System path for the non-nonce
  `CreateAccount`, `Assign`, `Transfer`, `CreateAccountWithSeed`, `Allocate`,
  `AllocateWithSeed`, and `AssignWithSeed` variants;
- launch-era v1.0.7 System `InitializeNonceAccount` and
  `AuthorizeNonceAccount`, including the exact 80-byte versioned nonce state,
  plus `AdvanceNonceAccount` with launch-era recent-blockhash handling;
- launch-era v1.0.7 Stake `Initialize`, `DelegateStake`, `Split`, `Authorize`,
  `Withdraw`, `Deactivate`, and `SetLockup`, with historical
  Rent/Clock/StakeHistory/StakeConfig inputs and the historical account
  verifier;
- launch-era v1.0.7 Vote, `InitializeAccount`, `Authorize`, and `Withdraw`,
  including the exact 3,731-byte vote-state layout and historical signer
  semantics;
- the complete launch-era v1.0.7 generic Config byte-store ABI, including its
  positional signer/cardinality quirks and opaque payload semantics;
- the v1.1.14 Stable epoch-34 BPF-loader activation and legacy `Write`/`Finalize`
  deployment profile, with compile-on-`Finalize` derivative artifacts published
  to the native cache only after their transaction commits;
- exact Bank-created executable NativeLoader accounts for every builtin
  declared by genesis (Config, Stake, System, and Vote);
- exact genesis-Bank accounts for Fees, StakeHistory, Clock, Rent,
  EpochSchedule, and RecentBlockhashes;
- child-Bank Clock, Fees, and RecentBlockhashes; per-freeze SlotHistory; and
  epoch-boundary zero-inflation Rewards/StakeHistory lifecycle, with SlotHashes
  kept explicitly unavailable until historical Bank hashes are computed;
- launch-era Config/System/Stake/Vote mutations through transaction overlays,
  including committed and rolled-back per-instruction account diffs and
  rollback-and-continue for implemented native instruction errors;
- an in-process `hashbrown` canonical account store with validated atomic
  transaction batches and deterministic pubkey-sorted state hashing;
- byte-range account writes for the 131,097-byte SlotHistory sysvar, avoiding
  a full-value rewrite for each Bank freeze; and
- portable frozen-Bank checkpoint V2, with strict migration of exhausted
  pre-activation V1 checkpoints and deliberate exclusion of the host-native
  derivative program cache.

Run the bundled self-contained program:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  demo --input-byte 1 --engine native-required
```

The program returns `2 * input + 1`, so the expected return value is `3`. The
bundled fixture is inside the AArch64 subset and is the first native Apple
Silicon acceptance case. See the dated evidence document for recorded output;
do not infer full SBPF support from this one fixture.

The AArch64 POC currently lowers only the instructions needed by the bundled
fixture: byte and double-word loads, double-word stores, 64-bit moves and adds,
resolved internal calls, and exit. Calls are expanded with bounded depth.
Branches, recursion, syscalls, and every unlisted opcode are rejected by this
native backend and run through the already verified interpreter instead. Guest
memory access still goes through the SBPF memory mapping, including stack guard
gaps. The verifier always runs before either native backend is considered.

This split is intentional: a Mac builds native code for its own architecture
without pretending that AArch64 and x86-64 machine-code artifacts are portable.
Artifact keys include the target and compiler profile, so an artifact produced
for one architecture cannot be selected for the other.

Inspect the mainnet genesis archive:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  genesis /path/to/genesis.tar.bz2
```

Inspect a Blockzilla compact generation without reading signature bytes:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  probe-compact /path/to/compact-generation --max-slots 10
```

Compact generations encoded before the `UnknownSystem` and `UnknownVote`
instruction fallbacks were inserted can opt into that historical message schema
by copying
`../blockzilla-read-sdk/assets/archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker` into
the generation and listing this exact tuple in `archive-v2-generation.json`:

```text
name   archive-v2-message-schema-may24-pre-unknown-fallbacks-v1.marker
size   87
sha256 2a3aa5808085bc7b869c7536508227f19e6b9d9e3f5fb34b65ebda9936bf0206
```

The generation digest must bind that file entry. The reader checks the exact
marker bytes, not only the declared file entry. It rejects unmarked mainnet
epochs 0 and 1 instead of guessing a message schema.

Run the bounded launch Config/System/Stake/Vote mutation path:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  replay-compact-prefix /path/to/compact-generation --max-slots 10
```

Run an ordered chain of complete Compact generations while retaining one Bank
and account store across the generation boundary:

```bash
cargo run --release -p blockzilla-replay --bin blockzilla-replay-poc -- \
  replay-compact-chain /path/to/epoch-0 /path/to/epoch-1 \
  --sample-diffs 0 --sample-accounts 0 \
  --checkpoint-out /state/launch-replay.chk
```

The checkpoint path is atomically refreshed only after a sealed generation's
final bound index row completes. Preserve the printed
`checkpoint_file_sha256` in trusted job metadata. Resume with the checkpointed
generation as the explicit anchor and only successor generations as positional
inputs:

```bash
cargo run --release -p blockzilla-replay --bin blockzilla-replay-poc -- \
  resume-compact-chain \
  --checkpoint /state/launch-replay.chk \
  --expected-checkpoint-sha256 <trusted-whole-file-sha256> \
  --completed-generation /path/to/epoch-1 \
  /path/to/epoch-2 /path/to/epoch-3 \
  --checkpoint-out /state/launch-replay.chk \
  --sample-diffs 0 --sample-accounts 0
```

For an attributed CPU sample, use a bounded replay with a symbolized optimized
binary. The profiler writes both an interactive flamegraph SVG and a sibling
leaf-sample `*.top.tsv`; profiling output is diagnostic and its run must not be
used as an unperturbed throughput result.

```bash
RUSTFLAGS="-C target-cpu=native -C force-frame-pointers=yes" \
  cargo build --profile release-debug -p blockzilla-replay \
  --bin blockzilla-replay-poc

target/release-debug/blockzilla-replay-poc resume-compact-chain \
  --checkpoint /state/launch-replay.chk \
  --expected-checkpoint-sha256 <trusted-whole-file-sha256> \
  --completed-generation /path/to/epoch-72 \
  /path/to/epoch-73 --max-slots 10000 \
  --sample-diffs 0 --sample-accounts 0 \
  --flamegraph-out /profiles/epoch-73.svg --profile-frequency 49 \
  --profile-skip-seconds 40
```

`--sample-diffs 0` selects the allocation-minimal execution policy: it does
not construct visitor diffs or diagnostic-only rollback diffs. A positive
sample count preserves exact hard-failure rollback evidence after the visitor
budget is exhausted, and the analytical library entry points still default to
all instruction diffs.

This command runs in diagnostic mode. An error returned by an implemented
native instruction is a derived failed-transaction outcome: the transaction
overlay is discarded, earlier instruction diffs are emitted as `RolledBack`,
and replay continues. On missing runtime support, it prints the exact
slot/transaction/instruction boundary, the fully committed prefix, and a
canonical POC replay-state hash, then exits nonzero.

`InitializeNonceAccount`, `AuthorizeNonceAccount`, and `AdvanceNonceAccount`
are implemented with their launch-era Bank inputs and state layout.
`WithdrawNonceAccount` remains fail-closed.

On the measured Compact prefix, the Split transactions at slots 105,368 and
105,532 pass the wrong authority to the Stake instruction. Both fail with
`MissingRequiredSignature`, roll their preceding `AllocateWithSeed` back, and
replay continues. The retry at 105,800 passes the correct authority and
commits. One Authorize and five Withdraws then commit; replay reaches slot
131,071 with no unsupported instruction in the available generation. That
generation contains no Config call, so Config behavior is covered by exact ABI
fixtures and transaction-rollback tests rather than ledger execution evidence.

Manifest-bound Compact Archive V2 is the POC's only executable ledger input.
Replay never opens or converts another ledger container and has no shred or RPC
fallback. Replay Projection V1 is a non-operational design draft, not an
accepted input. The Config/System/Stake/Vote POC is not Bank parity: it does
not yet apply fees or rent or compute account and Bank hashes.
Genesis-declared NativeLoader program accounts are materialized exactly. Its
commit model derives errors for implemented native instructions; archived
outcomes are observed but not consumed.
The six genesis sysvars and every child-Bank sysvar that does not require a
Bank hash are materialized. SlotHashes is not faked from Compact's PoH hashes;
Bank hashing remains required for that account. Every transaction row in the
measured bounded launch prefix has flags
`0x00000000` and no decoded metadata, so the archived outcome is `Unknown`; an
unset `HAS_ERROR` bit is not success evidence, and the complete Bank must derive
the outcome.

Fees and RecentBlockhashes advance with the implemented subset's executed
signature count. Exact general v1.0.7 classification still depends on the
historical account-load and fee pipeline, so this field is not yet Bank parity.

The exact two-generation release run completed every one of the 862,065 present
Compact block rows spanning slots 0 through 863,999. It processed 14,273,550
transactions: 14,051,214 committed under the implemented POC semantics and
222,336 were classified as derived failures. The committed path applied
14,051,343 instructions (14,050,966 Vote, 12 Config, 189 System, and 176 Stake),
with two earlier speculative instruction mutations rolled back. The final
run recorded 200 instruction-changed accounts and 3,448,259 Bank-sysvar writes.
The final 637-account POC state has canonical SHA-256
`9e0cf0dde2432719682de7b44cf4314e042c19f95ffd969bd58559439553ec32`.
The epoch-0 and epoch-1 generation digests are respectively
`fe71d3f13216bc94251da2fd4bda16264292cea72c0a39eca0a7cbd584ce9473`
and
`85dd5cb7efd28eb82eab23a5a81908ea8f7473cf59293b90edeaecdf461ac479`.
The first observed release run took 733.31 seconds wall time (664.66 user,
45.95 system) on the recorded Apple Silicon host. A second complete run took
721.79 seconds (644.57 user, 50.76 system) and reproduced every counter and the
same final state hash. These runs are execution and determinism evidence, not a
comparative throughput benchmark.
The run consumed only the two complete, manifest-bound Compact Archive V2
generations; no CAR file was opened, converted, or used as a fallback.

After the 2026-07-29 hot-path pass, two exact release replays of epoch 0 alone
completed in 21.674 and 22.031 seconds on the same Apple Silicon host. Both
runs processed all 431,548 present rows and reproduced state SHA-256
`7d07380fd242b4c4e701d9f4d85a1d0f809dfcef7a85c37210eeba6a71ceca36`.
The mean 21.853-second result is 4.63x faster than the 101.09-second
pre-optimization baseline. See the dated benchmark evidence for the exact
allocation microbenchmark and correctness counters.

The follow-up streaming decode pass inlined the common launch transaction
shape (eight account keys, one instruction, eight instruction accounts, and 64
raw bytes), removed the duplicated instruction pubkey collection, and retained
byte-exact historical Compact encoding. On the same 50,000-row epoch-0 prefix,
no-diff replay improved from 2,163.972 to 1,500.977 ms; allocation calls fell
from 5.7878 to 0.7878 per instruction and requested allocator traffic fell
62.6%. The exact state fingerprint was unchanged.

With those changes, a complete epoch-0/1 Compact chain replay finished in
99.27 seconds wall time (96.80 user, 1.03 system), and a second direct run
exited zero with identical counters. It completed all 862,065 present rows and
14,273,550 transactions through slot 863,999, ending with 637 accounts and
state SHA-256
`9e0cf0dde2432719682de7b44cf4314e042c19f95ffd969bd58559439553ec32`.
That is 7.27x faster than the previously documented 721.79-second full-chain
run. Only manifest-bound Blockzilla Compact V2 input was used.

## Earlier native x86-64 NAS replay milestone — 2026-07-29

The release replay binary was built and run directly on an x86-64 Linux NAS
with `RUSTFLAGS="-C target-cpu=native"`. It completed the 11 ordered Compact
Archive V2 generations from epoch 0 through epoch 10. All inputs were
manifest-bound; no CAR file was opened, converted, copied into the replay set,
or used as a fallback.

The run first exposed two bounded launch-runtime gaps: Stake `Deactivate` at
slot 882,928 and System `AdvanceNonceAccount` at slot 4,185,036. After adding
their v1.0.7 semantics, the complete retry produced:

```text
generation_count=11
completed_slot_range=Some(0)..=Some(4751999)
completed_slots=4677315
committed_transactions=241709502
failed_transactions=2819618
committed_instructions=241710659
rolled_back_instructions=8
vote_mutations=241706555
config_mutations=28
system_mutations=3657
stake_mutations=419
instruction_changed_accounts=1533
bank_sysvar_writes=18709277
bank_sysvar_accounts=6
replay_state accounts=1937 sha256=4c93b3498465af074b4adcbaca696206d7589fdd1dc2e554046a3ef2b7502a3f
wall_seconds=1009.411 user_seconds=991.233 system_seconds=3.072
```

That is approximately 239,456 committed transactions/s, 242,249 attempted
transactions/s, and 4,633.7 present slots/s. Archived outcomes were not
consumed; the failed-transaction count was derived by the implemented runtime.
This remains POC mutation replay, not historical Bank parity: signatures, CU,
fees, and rent are disabled, while SlotHashes and historical AccountsDB/Bank
hashes remain unavailable.

The replay hot path validates Compact control files and bounds/decompresses
each indexed frame, but deliberately does not SHA-256 the entire blocks object
on every open. Full payload authentication belongs at generation admission;
the local generation must remain immutable afterwards.

`replay-compact-chain` keeps one Bank across ordered generation directories.
A deterministic portable V2 frozen-Bank checkpoint codec round-trips accounts,
Bank sysvar caches, counters, and an exact generation row/slot cursor sealed from
the validated Compact index. Checkpoints may be captured beyond epoch 0 because
the codec still requires cumulative replay from slot 0 and validates the frozen
slot's epoch. Publication uses a temporary file in the destination directory,
flushes it, atomically renames it, and flushes the parent directory on Unix.

The native program cache is deliberately absent from the portable checkpoint:
account bytes are authoritative and host-native code is only a rebuildable
derivative. Legacy V1 restore is a strict pre-activation migration path. It
accepts an exhausted source generation such as epoch 33 only when the frozen
Bank predates epoch 34 and has no BPF-loader builtin, then emits V2 on the next
capture; in-progress, loader-containing, and epoch-34-or-later V1 states fail
closed.

Resume is boundary-only and fail-closed. It requires a trusted standard SHA-256
over the complete checkpoint file, reopens the completed source generation,
and matches its generation digest, registry digest, row count, and final index
slot before clearing the restored cursor guard. The successor must then begin
at physical row zero and pass the normal parent-slot and previous-PoH-hash Bank
checks. The embedded codec checksum remains corruption detection, not an
authenticity boundary; a digest sidecar stored beside a mutable checkpoint is
not trusted metadata. These guarantees preserve this runtime's deterministic
mutation state, not Solana Bank parity: signatures, CU, fees, rent, SlotHashes,
and historical Bank hashes remain outside the current POC.

The recorded epoch-33 V1 checkpoint has trusted whole-file SHA-256
`e02520615763e3e16dc3815a75fd903cdc90a2f6116c264903ad18983c8e9f25`.
An authenticated migration/resume from that exact file and completed epoch-33
Compact anchor passed a one-slot epoch-34 probe. The digest remains trusted
external job metadata; neither its successful probe nor the migration claims
that the full epoch 34 has replayed.

## Current borrowed-stream NAS result — 2026-07-29

The current-schema Compact V2 hot path now decodes each frame into reusable
storage and lends checked transaction, message, and metadata slices directly
to replay. Legacy Compact schemas retain their owned fallback. This removes
the former whole-slot transaction collection from the measured current-schema
path without weakening its structural validation. Compact V2 remains the only
executable ledger input; no CAR file was opened, converted, or used as a
fallback.

On one pinned NAS CPU, the comparison baseline immediately before this pass
had a 7.50-second median for a complete epoch-0 replay. The current five-round
median is 1.605 seconds (range 1.559–1.652 seconds), about 4.67x faster, with
the exact same epoch-0 state SHA-256
`7d07380fd242b4c4e701d9f4d85a1d0f809dfcef7a85c37210eeba6a71ceca36`.
On the same real 50,000-row prefix, the median moved from 902.016 to 211.401
milliseconds; successful allocation/reallocation requests fell from 157,580
to 7,584 and requested allocator traffic fell from 246,146,828 to 103,550,361
bytes. Requested bytes are cumulative allocator traffic, not peak RSS.

The final Vote change writes canonical state directly into the account buffer
after an exact two-phase capacity/conversion proof. It is retained for lower
byte traffic and stronger atomic semantics, not claimed as a timing win: the
immediately preceding borrowed-decoder build measured 212.657 milliseconds on
the 50,000-row prefix and 1.592 seconds for epoch 0, within run-to-run noise of
the current 211.401-millisecond and 1.605-second medians.

The later reward-discard decode variant also preserved the exact epoch-0 and
50,000-row state hashes, respectively
`7d07380fd242b4c4e701d9f4d85a1d0f809dfcef7a85c37210eeba6a71ceca36`
and
`0f4b7a0352af8ef3b87b265b2843c36bb6117d81bccc935bc2432f89cc365395`.
It did not improve this corpus: full epoch 0 measured 1.614479 seconds, 40,280
allocation/reallocation requests, and 180,280,671 requested bytes; the prefix
measured 213.409 milliseconds, 7,584 requests, and 103,550,361 requested bytes.
The allocation figures are identical to the preceding path and the timing
difference is noise. Reward discard is retained as protection against future
reward-heavy decode spikes, not claimed as an epoch-0 speedup.

The preceding borrowed-runtime revision completed all 31 manifest-bound Compact
V2 generations from epoch 0 through epoch 30:

```text
generation_count=31
completed_slot_range=Some(0)..=Some(13391999)
completed_slots=12905623
committed_transactions=794616477
failed_transactions=9855129
committed_instructions=794618095
rolled_back_instructions=314
vote_mutations=794606262
config_mutations=86
system_mutations=10109
stake_mutations=1638
instruction_changed_accounts=4054
bank_sysvar_writes=51622549
bank_sysvar_accounts=6
replay_state accounts=4066 sha256=a57a6255dc366cb150a2f748b2feb226542f168aa825b5010bf2dce1cefd2496
```

This is deterministic POC mutation-state evidence through epoch 30, not
historical Bank parity or a claim about later epochs. Signatures and compute
units remain unchecked, fees and rent are not applied, archived outcomes are
not consumed, and SlotHashes plus historical AccountsDB/Bank hashes remain
unavailable.

## Epochs 31–34 continuation and loader boundary — 2026-07-29

The Compact-only NAS continuation completed epochs 31, 32, and 33, extending
the sealed chain through slot 14,687,999. The first epoch-34 attempt then
stopped before freezing slot 15,105,072 on a legacy
`BPFLoader1111111111111111111111111111111111` `Write` instruction. At that
stop, the cumulative committed prefix reported:

```text
completed_slot_range=Some(0)..=Some(15105071)
completed_slots=14541644
committed_transactions=917182461
failed_transactions=11212103
committed_instructions=917184186
rolled_back_instructions=317
vote_mutations=917171811
config_mutations=110
system_mutations=10493
stake_mutations=1772
instruction_changed_accounts=4227
bank_sysvar_writes=58166644
replay_state accounts=4187 sha256=36d24c97595dd0ff64f07df97e56914820cb020800214f115ff93a66a16d0372
```

The completed-slot boundary is slot 15,105,071. The counters and hash can also
reflect transactions committed earlier in the not-yet-frozen failing block,
according to the diagnostic runner's prefix semantics; they are not the
epoch-33 checkpoint state hash.

The runtime now activates the legacy loader on entry to epoch 34 according to
the Solana v1.1.14 Stable schedule and implements the historical deployment
profile for `Write` and `Finalize`: instruction decoding, signer and account
checks, writes, the epoch-34 Bank-rent rule, executable transition, and
post-instruction verification. `Finalize` also builds a host-native derivative
when the modern compiler accepts the ELF, but publishes it to the cache only
after transaction commit. Compiler rejection cannot decide whether historical
Bank state finalizes the account.

The next Compact resume is still the acceptance test. Exact
`solana_rbpf 0.1.28` verifier parity is not yet implemented, and executable
program invocation, the historical serialized account ABI, syscalls, and CPI
remain unsupported. Therefore neither the one-slot migration probe nor the
deployment implementation is evidence that epoch 34 completes.

The account-store decision and crash-safe persistent roadmap are specified in
[Replay Account Storage V0](../../docs/design/replay-account-storage-v0.md).
Redis is not in the execution path: the target is a RAM hash index over sealed
read-only mmap segments, a hot delta, and a slot-coalesced recovery log.

Run the synthetic B-tree/hash-index baseline with an optimized build:

```bash
cargo run --release -p blockzilla-replay --no-default-features \
  --bin account-store-bench -- \
  --accounts 100000 --lookups 1000000 --data-bytes 128
```

Extract and compile a loader account image:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  compile /path/to/program-account.data \
  --loader legacy \
  --extract-to /tmp/program.so
```

The verifier is intentionally retained. Skipping ledger/signature verification
does not make attacker-authored program bytes safe compiler input.

For a compiler-only cross-target check, the historical genesis parser can be
disabled:

```bash
cargo check -p blockzilla-replay --lib --no-default-features \
  --target x86_64-unknown-linux-gnu
```

This compiles the x86-64 JIT branch but does not execute it. Native execution
still needs an x86-64 host or runner.

The current native acceptance matrix is:

| Host | Native compiler | Current acceptance boundary |
| --- | --- | --- |
| Apple Silicon (`aarch64-apple-darwin`) | Cranelift | Bundled verified SBPFv0 fixture and explicit parity/error cases |
| Intel/Rosetta (`x86_64-apple-darwin`) | `solana-sbpf` JIT | Bundled verified SBPFv0 fixture |
| x86-64 Linux | `solana-sbpf` JIT | Native replay completed through epoch 33; epoch 34 first stopped on loader deployment and its full retry is pending |

Neither native path emits a persistent `.so` yet; compiled code is owned by the
process-local artifact.

Use `--engine interpreter` for an explicit oracle run, or omit the flag for
target-adaptive `auto` selection. `native-required` returns an error instead of
silently falling back when a program or host is outside the native backend.
