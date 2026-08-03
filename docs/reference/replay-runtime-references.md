# Replay runtime reference ledger

Status: research snapshot, 2026-07-30

This document pins the upstream material used to design Blockzilla's trusted-history replay runtime. It is a reference ledger, not a claim that current validator behavior applies to old slots. Mainnet epochs 0 and 1 must be implemented from launch-era semantics and checked against archived outcomes; current runtimes are useful mainly for execution architecture, program loading, and state-commit patterns.

## Executive findings

- Mainnet's downloaded `genesis.bin` decodes as the launch-era `GenesisConfig` with `OperatingMode::Stable` (discriminant `1`), four native builtins, and no executable account. It is a configuration object, not a normal transaction-bearing block.
- The Solana v1.0.7 and v1.0.8 Stable schedules contain those four epoch-0 builtins and no epoch-1 program activation. Their BPF loader activation is still a placeholder at `u64::MAX - 1`. Solana v1.1.14 later names Stable epoch `34` for BPF loader activation.
- The evidence therefore strongly predicts that epochs 0 and 1 contain no successfully deployable user BPF program. This is an inference, not yet an acceptance-test result. We still must scan the epoch-0/1 corpus for BPF loader IDs and reconcile the exact validator-release/hard-fork timeline.
- Agave gives the best current source of truth for account loading, transaction rollback, loader versions, verifier-before-JIT behavior, and deployment visibility. It is not a historical epoch-0 runtime profile.
- LiteSVM and QuasarSVM are in-process testing/execution libraries. They are useful API examples, but their defaults describe a current, synthetic environment rather than a historical bank.
- At the pinned Firedancer revision, Frankendancer is available on mainnet but execution and consensus still use Agave; the fully independent runtime is explicitly not ready for test or production use. Firedancer's replay scheduler and staged commit/cancel design are useful, while its current SBPF execution path is an interpreter rather than a native-code compiler reference.
- Mithril demonstrates a lower-hardware, snapshot-bootstrapped verifying node and contributes useful account batching, copy-on-write, touched-only publication, and conflict-DAG patterns. It does not demonstrate genesis-to-tip archival replay: its README lists historical replay compatibility as future work, reports rare bankhash mismatches, and its SBF engine is an interpreter rather than a native compiler.
- `solana-sbpf` 0.21.0 verifies an ELF before JIT compilation and exposes JIT only on non-Windows `x86_64`. Its JIT output is process-local machine code, not a durable host `.so`. A portable/persistent AOT artifact format remains Blockzilla work.

## Pinned inventory

| Reference | Pin used | Role and status at this pin | License |
|---|---|---|---|
| [Agave](https://github.com/anza-xyz/agave/tree/e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3) | `e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3` (`4.3.0-alpha.2` workspace) | Production-validator lineage and present-day runtime reference; this alpha commit is not asserted to be the exact mainnet release | Apache-2.0 |
| [LiteSVM](https://github.com/LiteSVM/litesvm/tree/8dea7cde73923cf2a60b1c934e40442df9cf20c2) | `8dea7cde73923cf2a60b1c934e40442df9cf20c2` (`0.15.1`) | In-process program-testing library, not a validator or historical block replayer | Apache-2.0 |
| [Firedancer](https://github.com/firedancer-io/firedancer/tree/decca0535765f25e1dbe94258db1408d1213c17f) | `decca0535765f25e1dbe94258db1408d1213c17f` | Frankendancer is released; fully independent Firedancer is unreleased and not production-ready at this pin | Apache-2.0, with `NOTICE` obligations |
| [Mithril](https://github.com/Overclock-Validator/mithril/tree/2325aa5802c176bd97768c46188370ee47706d2b) | `2325aa5802c176bd97768c46188370ee47706d2b` (`alpenglow-dev`, replay performance improvements) | Go validator/full node; snapshot-bootstrapped live/catch-up replay reference, not a current historical-archive replayer | Apache-2.0 |
| [QuasarSVM](https://github.com/blueshift-gg/quasar-svm/tree/b5a9363de13e0f1e5e4559f4251c77563c3c9986) | `b5a9363de13e0f1e5e4559f4251c77563c3c9986` (Rust crate `0.1.0`) | In-process Rust/Node/Python execution library, not a validator or bank-history implementation | MIT |
| [Solana v1.0.7](https://github.com/solana-labs/solana/tree/57abc370fa39e42e8fb84145a30395ddcf891692) | tag `v1.0.7`, commit `57abc370fa39e42e8fb84145a30395ddcf891692` | Launch-era candidate; schema and genesis-program schedule match the downloaded mainnet genesis | Apache-2.0 |
| [Solana v1.0.8](https://github.com/solana-labs/solana/tree/2a617f2d07f714918891f2b479d1cb1c324f0365) | tag `v1.0.8`, commit `2a617f2d07f714918891f2b479d1cb1c324f0365` | Launch-era candidate and required comparison point | Apache-2.0 |
| [Solana v1.1.14](https://github.com/solana-labs/solana/tree/fd5222ad21673494fa1a1850ec131ecda5362ba2) | tag `v1.1.14`, commit `fd5222ad21673494fa1a1850ec131ecda5362ba2` | Later historical comparison that explicitly schedules Stable BPF loader activation at epoch 34 | Apache-2.0 |
| [`solana-sbpf` 0.21.0](https://github.com/anza-xyz/sbpf/tree/f95941e1f8ffed43d8722543f350b09e389f332f) | crate `0.21.0`, VCS commit `f95941e1f8ffed43d8722543f350b09e389f332f`; tracking HEAD observed as `5e501a5952b12a80b14f1e2af55a573e1ed3a485` | Blockzilla POC compiler/VM dependency; not a complete Solana runtime | Crate metadata: Apache-2.0; repository README also offers MIT/Apache-2.0 |
| [Cranelift](https://docs.rs/cranelift-codegen/0.134.2/) | crates pinned exactly to `0.134.2` | AArch64 machine-code backend for Blockzilla's strict, syscall-free SBPFv0 subset; not an SVM or persistent artifact format | Apache-2.0 WITH LLVM-exception |
| [Yellowstone Old Faithful](https://github.com/rpcpool/yellowstone-faithful/tree/ec48e6c12e7c1cb9e8b03fb1d045057d7bba7ba9) | HEAD observed as `ec48e6c12e7c1cb9e8b03fb1d045057d7bba7ba9` | Content-addressed historical data and index tooling; not an execution oracle | Top-level AGPL-3.0; inspect `LICENSING.md` for exceptions |

Pins are immutable inputs to this design. Updating one requires a documented semantic diff, especially around loader state layouts, SBPF versions, syscalls, feature activation, transaction rollback, and rent/fee behavior.

The source trees can be recreated without moving tags by running
`scripts/personal/reference/fetch-replay-runtime-references.sh`; it fetches and
detaches each exact revision into a caller-selected reference directory.

## Agave: current semantic reference

Primary sources:

- [SVM transaction processor](https://github.com/anza-xyz/agave/blob/e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3/svm/src/transaction_processor.rs)
- [program account loader](https://github.com/anza-xyz/agave/blob/e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3/svm/src/program_loader.rs)
- [program cache entry and visibility states](https://github.com/anza-xyz/agave/blob/e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3/program-runtime/src/program_cache_entry.rs)
- [deployment path](https://github.com/anza-xyz/agave/blob/e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3/program-runtime/src/deploy.rs)
- [program cache and runtime environments](https://github.com/anza-xyz/agave/blob/e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3/program-runtime/src/loaded_programs.rs)
- [transaction rollback accounts](https://github.com/anza-xyz/agave/blob/e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3/svm/src/rollback_accounts.rs)
- [bank and slot lifecycle](https://github.com/anza-xyz/agave/blob/e1566c2ec46ab4ba8f6f12ebb5399bfff62c4dc3/runtime/src/bank.rs)

What to carry into Blockzilla:

- Keep transaction state in an overlay and make commit versus rollback explicit. A recorded failed transaction is not equivalent to "do nothing": fee-payer and durable-nonce rollback accounts can have consensus-visible handling.
- Model loader ownership separately for deprecated loader v1, loader v2, upgradeable loader v3, and loader v4. Do not infer the byte offset from the first ELF magic sequence alone.
- Compile only a canonical program image that has passed the loader's state transition. For the legacy loader this is successful `Finalize`; for modern loaders it is successful deploy/upgrade/extend processing. Writes to a buffer are not deployments.
- Retain ELF/SBPF verification even with a trusted ledger. Agave's `ProgramCacheEntry` invokes `RequisiteVerifier` before JIT. The verifier is a host-safety boundary, not merely block validation.
- Preserve `deployment_slot`, `effective_slot`, runtime environment, feature set, and fork lineage in the cache identity. Agave represents the one-slot deployment delay with `DelayVisibility`; publishing a newly compiled image too early changes execution.
- Publish a compiled entry only when its containing transaction commits. Compilation can happen speculatively inside the transaction, but a failed deployment must not poison the global cache.

What not to copy as epoch-0 truth:

- Current feature activation and current builtin sets.
- Current loader account layouts, syscall registry, serialization ABI, rent collection, or compute-budget defaults without a historical profile resolver.
- Full validator admission, consensus, networking, signature verification, cost scheduling, and PoH verification when the input contract is an already selected canonical history.

## LiteSVM: fast in-process harness patterns

Primary sources:

- [project scope and capabilities](https://github.com/LiteSVM/litesvm/blob/8dea7cde73923cf2a60b1c934e40442df9cf20c2/README.md)
- [`LiteSVM` configuration and transaction path](https://github.com/LiteSVM/litesvm/blob/8dea7cde73923cf2a60b1c934e40442df9cf20c2/crates/litesvm/src/lib.rs)
- [transaction history](https://github.com/LiteSVM/litesvm/blob/8dea7cde73923cf2a60b1c934e40442df9cf20c2/crates/litesvm/src/history.rs)
- [message/instruction processing](https://github.com/LiteSVM/litesvm/blob/8dea7cde73923cf2a60b1c934e40442df9cf20c2/crates/litesvm/src/message_processor.rs)

Useful lessons:

- The bare `LiteSVM::default()` state starts with `sigverify: false`, while the normal `LiteSVM::new()` constructor's `into_basic()` path enables it. An explicit `.with_sigverify(false)` selects the no-crypto path while transaction sanitization and signer/writable metadata remain. That separation is close to Blockzilla's trusted-history admission contract.
- Transaction-history capacity can be set to zero to bypass duplicate-history
  storage/checking. That is useful for isolated microbenchmarks, but it is not
  faithful epoch replay: the launch-era duplicate/status cache can change
  whether a transaction executes and must remain enabled there.
- Simulation and invocation-inspection callbacks are good patterns for exposing pre/post state and nested invocation data without coupling output code to the executor.

Limits:

- LiteSVM deliberately creates a convenient current testing environment. It does not reconstruct genesis, banks, fork choice, rewards, or historical feature activation.
- It still processes compute budgets and current runtime constraints. Turning off signature verification is not evidence that all other admission or execution checks can be removed safely.

## QuasarSVM: compact result and trace API

Primary sources:

- [project scope](https://github.com/blueshift-gg/quasar-svm/blob/b5a9363de13e0f1e5e4559f4251c77563c3c9986/README.md)
- [core executor](https://github.com/blueshift-gg/quasar-svm/blob/b5a9363de13e0f1e5e4559f4251c77563c3c9986/svm/src/svm.rs)
- [program cache wrapper](https://github.com/blueshift-gg/quasar-svm/blob/b5a9363de13e0f1e5e4559f4251c77563c3c9986/svm/src/program_cache.rs)
- [public account/result types](https://github.com/blueshift-gg/quasar-svm/blob/b5a9363de13e0f1e5e4559f4251c77563c3c9986/svm/src/lib.rs)

Useful lessons:

- Execution builds a transaction context from merged accounts, runs instructions, and writes resulting accounts to the store only on success when `commit` is requested. That is a clean small-scale example of execute-then-commit.
- Results expose returned accounts, logs, balances, token balances, and a nested execution trace. The trace preserves stack depth and full instruction data.
- Its program cache delegates to Agave `ProgramCacheEntry`; it is not an independent native compiler.

Limits and discrepancies at this pin:

- `QuasarSvm::new` uses `FeatureSet::all_enabled()` and current compute-budget defaults. This is unsuitable for historical replay without a profile layer.
- The trace annotates per-instruction result/CU partly by parsing logs. Blockzilla should capture instruction boundaries directly in the invocation engine so disabled logging cannot corrupt the trace.
- `AccountDiff` is declared as a public type, but `ExecutionResult` returns resulting accounts rather than a wired sequence of per-instruction byte diffs. The README's "byte-level account diffs" claim should not be treated as an implemented historical-diff oracle at this revision.

## Firedancer: scheduler and staged state

Primary sources:

- [release/readiness distinction](https://github.com/firedancer-io/firedancer/blob/decca0535765f25e1dbe94258db1408d1213c17f/README.md)
- [replay scheduler contract](https://github.com/firedancer-io/firedancer/blob/decca0535765f25e1dbe94258db1408d1213c17f/src/discof/replay/fd_sched.h)
- [replay scheduler implementation](https://github.com/firedancer-io/firedancer/blob/decca0535765f25e1dbe94258db1408d1213c17f/src/discof/replay/fd_sched.c)
- [runtime prepare/execute/commit/cancel contract](https://github.com/firedancer-io/firedancer/blob/decca0535765f25e1dbe94258db1408d1213c17f/src/flamenco/runtime/fd_runtime.h)
- [instruction executor boundary](https://github.com/firedancer-io/firedancer/blob/decca0535765f25e1dbe94258db1408d1213c17f/src/flamenco/runtime/fd_executor.h)
- [BPF loader program](https://github.com/firedancer-io/firedancer/blob/decca0535765f25e1dbe94258db1408d1213c17f/src/flamenco/runtime/program/fd_bpf_loader_program.c)
- [program cache](https://github.com/firedancer-io/firedancer/blob/decca0535765f25e1dbe94258db1408d1213c17f/src/flamenco/progcache/README.md)
- [computed-goto SBPF interpreter core](https://github.com/firedancer-io/firedancer/blob/decca0535765f25e1dbe94258db1408d1213c17f/src/flamenco/vm/fd_vm_interp_core.c)

Production-status boundary:

- The pinned README says Frankendancer combines Firedancer networking/block production with Agave execution and consensus and is available on testnet and mainnet-beta.
- The same README says the fully from-scratch Firedancer validator has no releases and is not ready for test or production use. Flamenco runtime code is therefore valuable conformance engineering, not yet an independent production oracle at this pin.

Useful lessons:

- Replay separates transaction execution, signature verification, and PoH hashing into task classes. The scheduler contains build-time `FD_SCHED_SKIP_SIGVERIFY` and `FD_SCHED_SKIP_POH` paths, confirming that these tasks can be removed structurally rather than hidden inside execution.
- `fd_runtime_prepare_and_execute_txn` produces staged output that must be followed by `fd_runtime_commit_txn` or `fd_runtime_cancel_txn`. This is the right ownership model for parallel replay and precise rollback.
- Its conflict-aware replay DAG is a later optimization target after sequential parity. Transaction execution remains the highest-priority task and account dependencies determine readiness.
- Loader validation, program-cache lineage, deployment-slot behavior, and VM tracing are useful independent comparisons against Agave.
- The current VM path is a highly optimized computed-goto interpreter. No active ELF-to-host-native JIT/AOT pipeline was found in this pin, so Firedancer is not the reference for Blockzilla's native artifact format.

For instruction-level diffs, the natural instrumentation seam is immediately around `fd_execute_instr`: snapshot only the instruction's writable account set before invocation, capture state after invocation, and retain the frame even when the enclosing transaction later rolls back.

## Mithril: consumer-hardware account and scheduling patterns

Primary sources:

- [project scope, hardware guidance, bootstrap path, status and limitations](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/README.md)
- [Alpenglow branch engine and durable-state design](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/docs/alpenglow_branch_engine.md)
- [account database](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/accountsdb/accountsdb.go), [batched loads](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/accountsdb/batch.go), and [folding](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/accountsdb/fold.go)
- [mutable working set](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/accounts/working_set.go), [sharded overlay](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/accounts/overlay.go), and [transaction first-write copy-on-write](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/sealevel/transaction_ctx.go)
- [account-conflict topological planner](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/replay/topsort_planner.go) and [block execution loop](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/replay/block.go)
- [SBF interpreter](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/sbpf/interpreter.go) and [BPF loader](https://github.com/Overclock-Validator/mithril/blob/2325aa5802c176bd97768c46188370ee47706d2b/pkg/sealevel/bpf_loader.go)
- [merged replay-performance work](https://github.com/Overclock-Validator/mithril/pull/256)

Scope and evidence boundary:

- Mithril starts from full and incremental Solana snapshots, catches up with parallel RPC `getBlock`, and then follows live blocks. Its claim that a six-core Ryzen 5 7640HS performs well applies to this near-tip full-node workflow. It is not a published measurement of replaying every historical epoch from genesis.
- The pinned README explicitly says the implementation is not production-ready, reports rare bankhash mismatches, and lists historical replay compatibility under future archival-node work. Use it as an implementation and conformance reference, not as a historical state oracle.
- Pull request 256 reports roughly 50% lower block-execution time in many cases after a collection of replay optimizations, but provides no controlled host/slot/raw benchmark table. Treat the number as directional until reproduced against Blockzilla Compact input.
- Mithril caches verified/parsed Go SBF programs and pools its 256 KiB interpreter heap, but executes them in a pure-Go interpreter. It supplies no JIT, AOT, or persistent native-code artifact design; Blockzilla's verified x86-64 JIT and AArch64 Cranelift work remain separate.

What to carry into Blockzilla:

- Preserve a strict load, execute, and commit boundary. Deduplicate each block's account keys and issue batched loads; if state later spills to disk, group cold reads by append-vector/segment location rather than performing independent random gets.
- Keep immutable account values shared until first mutation, clone account data only on first write, and publish only touched accounts. This matches the direction of the current dirty first-preimage journal and reusable per-transaction state.
- Build an exact read/write conflict DAG from Compact message keys and writable flags. RAW, WAR, and WAW edges retain sequential semantics while read/read transactions may run concurrently. Gate this behind sequential parity and benchmark scheduler overhead on vote-heavy early epochs before enabling it globally.
- Retain the current single-thread hash-table fast path. Shard locks only when parallel execution shows contention; adding Pebble while the selected epoch registry fits in RAM would add work without solving the measured bottleneck.
- If checkpoint state outgrows RAM, borrow the shape—not the fork machinery—of a RAM working set folded periodically into append-only newest-wins segments with an atomic manifest/index publication.
- Add phase and cache counters around unique-account load, DAG construction/wait, VM setup/run, bytes cloned, touched-account publication, program cache hits, and checkpoint folding. This makes each transferred optimization independently falsifiable.

What not to copy for canonical historical replay:

- Certificate/fork rewind machinery, live-network services, current fee/rent/CU policy, or a current snapshot as launch-era truth.
- Pebble and append-vector persistence before measurements show RAM residency is no longer viable.
- Interpreter execution as the main performance path; retain it only as a semantic oracle and fallback.

## `solana-sbpf`: verifier and native-code POC base

Primary sources for crate 0.21.0:

- [crate module architecture and JIT target gate](https://github.com/anza-xyz/sbpf/blob/f95941e1f8ffed43d8722543f350b09e389f332f/src/lib.rs)
- [ELF loading, verification, and `jit_compile`](https://github.com/anza-xyz/sbpf/blob/f95941e1f8ffed43d8722543f350b09e389f332f/src/elf.rs)
- [JIT compiler](https://github.com/anza-xyz/sbpf/blob/f95941e1f8ffed43d8722543f350b09e389f332f/src/jit.rs)
- [VM configuration, including instruction metering](https://github.com/anza-xyz/sbpf/blob/f95941e1f8ffed43d8722543f350b09e389f332f/src/vm.rs)
- [license statement](https://github.com/anza-xyz/sbpf/blob/f95941e1f8ffed43d8722543f350b09e389f332f/README.md)

POC implications:

- Always run `RequisiteVerifier` before native compilation, even when program bytes came from canonical history.
- Treat "native" precisely: the upstream JIT produces executable memory for the running process. It does not emit a relocatable, durable `.so` artifact.
- At 0.21.0 the upstream JIT modules and execution branches are compiled only
  for non-Windows `x86_64`. Its unused `aarch64.rs` is an instruction encoder,
  not an SBPF compiler or invocation path, so enabling it with a cfg change is
  insufficient.
- An artifact cache key must include at least canonical ELF digest, loader/ABI profile, SBPF version, syscall environment, feature profile, compiler version/config, host target, and deployment/effective-slot identity. Hashing only the ELF is incorrect.
- Disabling the instruction meter removes accounting overhead, but compute limits and "remaining compute" can affect control flow and recorded failures. Meter-free execution therefore needs a recorded-outcome contract plus a strict fallback for CU-observable programs; it is not a universally semantics-free toggle.

Blockzilla therefore preserves the upstream x86-64 JIT and uses Cranelift
`0.134.2` for the first AArch64 proof. The current lowering accepts only the
eight verified, acyclic and syscall-free SBPFv0 operations exercised by the
minor fixture. Calls are inlined with bounded depth; memory operations call the
same checked `MemoryMapping`; every expanded guest instruction ticks a
non-consensus watchdog. Any unsupported opcode, syscall/unresolved call,
recursion, or expansion overflow produces no native artifact and selects the
interpreter. This is native execution, but it remains process-local JIT code,
not a `.so` or AOT cache.

Cranelift's ordinary JIT allocator supplied working RW-to-RX protection and
instruction-cache coherence in the unsigned Apple Silicon CLI test. That does
not establish compatibility with a signed Hardened Runtime application;
`MAP_JIT`, entitlements, and per-thread JIT write protection remain a separate
packaging gate.

## Launch-era Solana sources

### v1.0.7

Primary sources:

- [`GenesisConfig` and `OperatingMode` enum](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/sdk/src/genesis_config.rs)
- [genesis program and inflation schedule](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/genesis-programs/src/lib.rs)
- [bank creation, epoch transitions, account execution, rent, rewards, and sysvars](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/runtime/src/bank.rs)
- [native System instruction processor](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/runtime/src/system_instruction_processor.rs)
- [native Stake instruction dispatcher](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/programs/stake/src/stake_instruction.rs)
- [launch Stake state and Split/Authorize/Withdraw semantics](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/programs/stake/src/stake_state.rs)
- [runtime Stake cache and history lifecycle](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/runtime/src/stakes.rs)
- [post-instruction account invariant verifier](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/runtime/src/message_processor.rs)
- [ledger replay with optional PoH verification](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/ledger/src/blockstore_processor.rs)
- [legacy BPF loader](https://github.com/solana-labs/solana/blob/57abc370fa39e42e8fb84145a30395ddcf891692/programs/bpf_loader/src/lib.rs)

Facts relevant to epoch 0/1:

- Enum order is `Preview = 0`, `Stable = 1`, `Development = 2`; the final genesis field is `operating_mode`, not a modern cluster-type field.
- Stable epoch 0 registers Config, Stake, System, and Vote. Stable epoch 1 returns no new program list. Inflation is disabled at epoch 0 and unchanged at epoch 1.
- Stable BPF loader activation is a future-hard-fork placeholder at `u64::MAX - 1` in this revision.
- The legacy loader uses chunked `Write` instructions followed by `Finalize`. `Finalize` verifies the full program account ELF, verifies rent exemption, then marks the account executable. Runtime execution uses `solana_rbpf = 0.1.21` with a 100,000-instruction maximum and calls the interpreter's `execute_program`; the loader does not JIT at deployment.
- The blockstore processor already has a `poh_verify` option. This supports a trusted-history replay mode, but the Bank still performs structural transaction/account checks required for safe and correct mutation.

### v1.0.8

Primary sources:

- [`GenesisConfig`](https://github.com/solana-labs/solana/blob/2a617f2d07f714918891f2b479d1cb1c324f0365/sdk/src/genesis_config.rs)
- [genesis program schedule](https://github.com/solana-labs/solana/blob/2a617f2d07f714918891f2b479d1cb1c324f0365/genesis-programs/src/lib.rs)
- [BPF loader](https://github.com/solana-labs/solana/blob/2a617f2d07f714918891f2b479d1cb1c324f0365/programs/bpf_loader/src/lib.rs)

The inspected Stable schedule still has only the four launch builtins at epoch 0, no epoch-1 addition, and the BPF loader placeholder at `u64::MAX - 1`. This tag must remain a separate parity target because a matching schedule does not prove all runtime behavior is byte-for-byte identical to v1.0.7.

### v1.1.14

Primary sources:

- [genesis program and inflation schedule](https://github.com/solana-labs/solana/blob/fd5222ad21673494fa1a1850ec131ecda5362ba2/genesis-programs/src/lib.rs)
- [bank](https://github.com/solana-labs/solana/blob/fd5222ad21673494fa1a1850ec131ecda5362ba2/runtime/src/bank.rs)
- [BPF loader](https://github.com/solana-labs/solana/blob/fd5222ad21673494fa1a1850ec131ecda5362ba2/programs/bpf_loader/src/lib.rs)

This revision explicitly registers the BPF loader for Stable mode at epoch `34`. That is strong evidence that the placeholder was resolved later and that no BPF deployment belongs in epochs 0 or 1. It does not by itself prove which binary activated the loader on mainnet or whether an intervening release/hard-fork changed the schedule.

## Mainnet genesis audit

Source downloaded on 2026-07-28: [mainnet-beta `genesis.tar.bz2`](https://api.mainnet-beta.solana.com/genesis.tar.bz2).

Identity:

| Field | Observed value |
|---|---|
| Compressed tarball bytes | `20,144` |
| Compressed tarball SHA-256 | `133f7eaefcd59466f3b291aadd1b0d3522432072cf5b539445218c6c125ea945` |
| `genesis.bin` bytes | `132,347` |
| `genesis.bin` SHA-256 / genesis hash, hex | `45296998a6f8e2a784db5d9f95e18fc23f70441a1039446801089879b08c7ef0` |
| Genesis hash, base58 | `5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d` |
| Creation time | Unix `1584368940` (`2020-03-16T14:29:00Z`) |
| Operating mode | discriminant `1`, `Stable` in the launch-era schema |

The `genesis.bin` hash is the durable identity. The compressed tarball hash can change if an endpoint recompresses the same payload, so replay manifests must pin both the uncompressed genesis hash and the archive hash actually consumed.

State and builtins:

| Field | Observed value |
|---|---|
| Ordinary accounts | `431` |
| Account data bytes | `93,534` |
| Executable ordinary accounts | `0` |
| Capitalization | `500,000,000,000,000,000` lamports (500 million SOL) |
| Reward-pool accounts | `0` |
| Native builtins | `solana_config_program`, `solana_stake_program`, `solana_system_program`, `solana_vote_program` |

Builtin addresses:

- Config: `Config1111111111111111111111111111111111111`
- Stake: `Stake11111111111111111111111111111111111111`
- System: `11111111111111111111111111111111`
- Vote: `Vote111111111111111111111111111111111111111`

Solana v1.0.7 `Bank::process_genesis_config()` subsequently materializes one
runtime account at each builtin address. Each account has one lamport, owner
`NativeLoader1111111111111111111111111111111`, `executable=true`,
`rent_epoch=0`, and ASCII data equal to its builtin name. These four accounts
are not serialized in `genesis.bin` and do not contribute to its hash. The sole
serialized Config-owned state account is
`StakeConfig11111111111111111111111111111111` (960,480 lamports, 10 data
bytes, empty ConfigKeys plus the launch stake configuration).

Timing, fees, rent, and epochs:

| Field | Observed value |
|---|---|
| Ticks per slot | `64` |
| Hashes per tick | `12,500` |
| Target lamports/signature | `10,000` |
| Target signatures/slot | `20,000` |
| Minimum / maximum lamports/signature | `5,000` / `100,000` |
| Fee burn | `100%` |
| Rent lamports/byte-year | `3,480` |
| Rent exemption threshold | `2.0` years |
| Rent burn | `100%` |
| Inflation fields | all zero/disabled at genesis |
| Slots per epoch | `432,000` |
| Leader schedule offset | `432,000` |
| Warmup | `false` |
| First normal epoch / slot | `0` / `0` |
| Epoch 0 slots | `[0, 432000)` |
| Epoch 1 slots | `[432000, 864000)` |

Implementation consequence: even though token/SOL balance changes are not the first output product, lamports, fees, rent, capitalization, and reward state must remain in the internal state transition. Hiding lamport deltas from the diff stream is safe; omitting lamports from execution state is not.

## Old Faithful epoch inputs

Primary sources:

- [pinned project README](https://github.com/rpcpool/yellowstone-faithful/blob/ec48e6c12e7c1cb9e8b03fb1d045057d7bba7ba9/README.md)
- [pinned IPLD ledger schema](https://github.com/rpcpool/yellowstone-faithful/blob/ec48e6c12e7c1cb9e8b03fb1d045057d7bba7ba9/ledger.ipldsch)
- [Old Faithful documentation](https://docs.old-faithful.net/)
- [epoch 0 CAR](https://files.old-faithful.net/0/epoch-0.car)
- [epoch 1 CAR](https://files.old-faithful.net/1/epoch-1.car)

HTTP metadata observed on 2026-07-28:

| Object | Content length | Range requests |
|---|---:|---|
| epoch 0 CAR | `4,286,945,461` bytes | advertised |
| epoch 1 CAR | `9,021,859,488` bytes | advertised |

The IPLD hierarchy is epoch to subset to block to entry to transaction, with raw transaction and transaction-metadata frames. This is an efficient authoritative ordering/input format for the POC, but not an execution oracle: recorded metadata must be treated as comparison and outcome input, while account state must be rebuilt by the runtime.

Before full replay, create a local immutable manifest containing each CAR root CID, byte length, complete-file digest, index digests, schema revision, genesis hash, and retrieval URL. HTTP `Content-Length` is not content identity. Because the upstream README labels the project/format as RFC-stage, the decoder must be selected by a pinned schema/version rather than by whatever `main` serves later.

## Historical activation ambiguity and closure test

What is established:

1. The actual genesis payload is Stable and has only four native builtins.
2. v1.0.7 and v1.0.8 add no Stable program at epoch 1 and leave the BPF loader at a placeholder epoch.
3. v1.1.14 schedules Stable BPF loader activation at epoch 34.
4. The actual genesis contains no executable account.

What is not yet established:

- The exact validator binary/release mix that produced every epoch-0/1 slot.
- Whether any mainnet-specific hard-fork patch outside the inspected tags altered program registration or other runtime semantics during those epochs.
- A corpus-level proof that the loader IDs never appear in epoch-0/1 instructions, account ownership, or successful transaction effects.

Closure gate before claiming epoch-0/1 support:

- Scan every transaction message and inner instruction in both CARs for the legacy/deprecated/current BPF loader IDs and record the earliest occurrence.
- Scan all reconstructed account owners/executable transitions for those IDs.
- Compare epoch-boundary builtins/sysvars and selected bank/account checkpoints against an independent historical snapshot or a launch-era validator replay.
- Build and differential-test v1.0.7 and v1.0.8 runtime profiles on the same early-slot fixtures. If they diverge, resolve by slot-level recorded outcomes rather than choosing the newer tag by default.
- Keep the BPF compiler milestone independent from the epoch-0/1 acceptance path unless the scan disproves the no-loader inference.

## Adopt/reject matrix

| Concern | Adopt | Reject or defer |
|---|---|---|
| Trusted archive admission | LiteSVM-style disabled signature crypto; Firedancer-style removal of sigverify/PoH tasks | Removing signer/writable metadata, message sanitization, ownership checks, borrow rules, CPI privilege checks, or rollback |
| Historical runtime | Versioned launch-era profiles selected by slot/epoch and evidence | `FeatureSet::all_enabled()` or a present-day Agave bank as epoch-0 truth |
| Program deployment | Loader-aware canonical extraction; verifier; compile after successful finalize/deploy/upgrade/extend; publish on commit | Compiling every buffer write, scanning arbitrary ELF magic, or making failed/speculative deployment globally visible |
| Native execution | `solana-sbpf` verified x86_64 JIT; Cranelift AArch64 strict-subset lowering; forced interpreter oracle | Calling process-local JIT output a portable `.so`; treating the orphan upstream ARM encoder as a complete backend; claiming full SBPF from the subset fixture |
| Program cache | Content plus complete semantic/runtime/deployment identity | Cache key of program ID or ELF digest alone |
| State mutation | Explicit transaction overlay and commit/cancel; Mithril-style first-write copy-on-write and touched-only publication; preserve fee/nonce failure rules | Applying successful instruction writes from a failed transaction to canonical state |
| Diff capture | Direct pre/post snapshots at every invocation frame; changed byte ranges for every changed account | Token-only diffs, log-parsed boundaries, or labeling every program-owned account as a PDA |
| PDA semantics | Generic account diff first; attach PDA derivation only when seeds/bump/program derivation are known | Treating PDA as an on-chain account type |
| Compute units | Suppress accounting output where proven safe; use recorded result and strict fallback | Globally removing limits when a program can observe remaining CU or when historical failure depends on exhaustion |
| Parallel replay | Firedancer/Mithril-style account-conflict DAG after sequential parity; measure DAG overhead before enabling it for vote-heavy epochs | Parallel execution before deterministic state and rollback parity |

## Licensing boundary

The reference repositories are for study and differential testing; source reuse must be intentional and tracked.

- Agave, LiteSVM, historical Solana, Firedancer, and Mithril are Apache-2.0. Preserve copyright/license notices and, for Firedancer, the repository `NOTICE` material when copying covered code.
- QuasarSVM is MIT; preserve its copyright and permission notice when copying covered code.
- `solana-sbpf` 0.21.0 crate metadata declares Apache-2.0, while its repository README describes an MIT/Apache-2.0 choice. Record the chosen license for any copied or redistributed source and retain its notice.
- Yellowstone Old Faithful's top-level code license is AGPL-3.0 and it also ships `LICENSING.md`. Prefer consuming the archive format/data through Blockzilla's independent reader; do not copy server/tool code without a deliberate license review. The repository code license alone should not be assumed to define redistribution terms for every hosted dataset.

This section is an engineering compliance note, not legal advice.
