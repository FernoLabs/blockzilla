# Replay Runtime POC Evidence — 2026-07-28

Status: **complete manifest-bound Compact Archive V2 epochs 0–33 POC mutation
replay on a native x86-64 Linux NAS; the first epoch-34 attempt stopped on a
legacy BPF-loader `Write`, and the implemented loader support has not yet
completed the full retry. This remains bounded compiler, genesis, and native
runtime evidence, not historical Bank or epoch parity**.

The POC's only executable ledger input is a manifest-bound **Blockzilla Compact
Archive V2** generation. The compiler and genesis subcommands below are
isolated utilities, not ledger input paths. Replay never opens or converts
another ledger container and has no shred or RPC fallback. In particular, no
CAR file is opened, converted, or used as a fallback. Replay Projection V1 is a
non-operational design draft, not an accepted input.

## Environment

- Host: Apple Silicon, macOS, `aarch64-apple-darwin`
- Rust: 1.97.1
- SBPF library: `solana-sbpf 0.21.0`
- AArch64 compiler: `cranelift 0.134.2` (strict-subset POC)
- Pre-Cranelift compiler profile:
  `blockzilla-poc-sbpfv0-no-cu-accounting-watchdog-v2`
- AArch64 compiler profile:
  `blockzilla-poc-sbpfv0-no-cu-accounting-static-watchdog-native-dispatch-v3`
- Fixture: upstream dual-licensed `relative_call_sbpfv0.so`

The fixture is deliberately self-contained because the POC does not yet expose
Solana syscalls or the serialized account ABI. It returns `2 * input[0] + 1`.

The AArch64 backend is intentionally not a general SBPF compiler. Its current
accepted surface is limited to the fixture's byte/double-word loads,
double-word stores, 64-bit moves/adds, resolved internal calls, and exits.
Every program is loaded and passed through `RequisiteVerifier` first. A valid
program containing another opcode, a syscall, recursion, or an expansion that
exceeds the backend's bound is routed to the interpreter rather than partially
compiled.

## Program pipeline result

Command:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  demo --input-byte 1 --engine native-required
```

Previously observed AArch64 interpreter manifest, before the Cranelift
milestone:

```text
loader=BareElf
account_data_len=1656 elf_offset=0 canonical_elf_len=1656
elf_sha256=3122910a10488425d6ab3b4a79aae67e85c0c9d792866913028886374bae00af
artifact_key=de499c72860ae8e88d2754bd3d1d1aebc3375d96265759c5b4f918f153e656b5
sbpf_version=V0 text_vaddr=0x100000120 text_len=128 entrypoint_instruction=4
profile=blockzilla-poc-sbpfv0-no-cu-accounting-watchdog-v2
verifier=solana_sbpf::verifier::RequisiteVerifier
protocol_compute_accounting=false watchdog_instruction_limit=1000000
backend=interpreter-only
execution_engine=Interpreter input=1 return=3 watchdog_instructions=16
```

Native AArch64 result after the Cranelift milestone:

```text
host_arch=arm64
binary=Mach-O 64-bit executable arm64
artifact_key=f0d1f18457c077bb5022aa7e8f9a64f80935d33ab6c693d200d6977f7b5e0d34
profile=blockzilla-poc-sbpfv0-no-cu-accounting-static-watchdog-native-dispatch-v3
native_backend_id=cranelift-0.134.2-aarch64-sbpfv0-subset-v1
native_entry_abi_id=blockzilla-native-entry-v1-checked-memory-helpers
backend=native-cranelift-aarch64-subset machine_code_len=796 lowered_instructions=16
execution_engine=NativeCraneliftAarch64Subset input=1 return=3 watchdog_instructions=16
```

This output came from the native `aarch64-apple-darwin` process, not the x86-64
cross-build. It identifies the Cranelift backend and forced native engine,
returns `3`, and reports the same 16 expanded guest instructions as the forced
interpreter.

The crate's x86-64 Linux JIT branch was also compile-checked with:

```bash
cargo check -p blockzilla-replay --lib --no-default-features \
  --target x86_64-unknown-linux-gnu
```

That command passed. More importantly, the macOS x86-64 build was executed
under Rosetta on the same machine:

```bash
cargo build -p blockzilla-replay --bin blockzilla-replay-poc \
  --target x86_64-apple-darwin
arch -x86_64 target/x86_64-apple-darwin/debug/blockzilla-replay-poc \
  demo --input-byte 1 --engine native-required
```

Observed native result:

```text
artifact_key=846cbb44494af7f59f853aa9c3fa1af49a9726287b0544ce552fa9ef55cacb51
profile=blockzilla-poc-sbpfv0-no-cu-accounting-static-watchdog-native-dispatch-v3
native_backend_id=solana-sbpf-0.21.0-x86_64-jit
backend=native-jit-x86_64 machine_code_len=2321
execution_engine=NativeJitX86_64 input=1 return=3 watchdog_instructions=16
```

The target-specific artifact key differs intentionally. This closes the first
minor-example native execution gate, but an x86-64 Linux CI job is still needed
for the intended production target.

## AArch64 strict-subset acceptance matrix

The following matrix separates required behavior from recorded evidence. A row
is complete only after its named assertion has passed on native Apple Silicon.

| Case | Required AArch64 selection | Required assertion | Evidence |
| --- | --- | --- | --- |
| Bundled fixture, every input byte `0..=255` | Cranelift native | Return value, complete post-input bytes, watchdog count, and success class exactly match forced interpreter execution | PASS: `forced_native_matches_forced_interpreter_for_every_input_byte`; interpreter-entry counter unchanged during each forced-native call |
| Bundled fixture, input `1` | Cranelift native | Return `3`; machine-code length and executed-instruction count are nonzero | PASS: 796 native bytes, return `3`, 16 instructions |
| Verified backward branch/loop | Interpreter fallback | Native subset declines the opcode; `NativeRequired` errors; interpreter watchdog returns `WatchdogExceeded` | PASS: `watchdog_stops_a_verified_backward_loop` |
| Invalid SBPF bytecode | No backend | `RequisiteVerifier` rejects it before AArch64 lowering | PASS: `verifier_rejects_invalid_register_before_native_lowering` |
| Empty input with fixture load | Cranelift native | Execution fails through checked SBPF memory mapping; no unchecked host dereference | PASS: native and interpreter return the same access-violation error |
| Access into an SBPFv0 stack guard gap using an otherwise supported store | Cranelift native | Execution reports an access violation and cannot cross the guard gap | PASS: `native_store_cannot_cross_an_sbpfv0_stack_guard_gap`; exact error parity |
| Recursive or unresolved internal call | Interpreter fallback | The native artifact is absent and no partial lowering is executed | PASS: direct backend tests classify both as unsupported; compiler maps unsupported classifications to `InterpreterOnly` |
| Same ELF on AArch64 and x86-64 | Architecture-specific native backend | Target-specific artifact keys differ; both engines return `3` with 16 instructions | PASS: ARM key `f0d1…0d34`, x86 key `846c…b51`; engines `NativeCraneliftAarch64Subset` / `NativeJitX86_64` |

This matrix does not accept “compiled successfully” as parity evidence. For
success cases it compares the return value and post-execution bytes; for
failure cases it compares the safety/error class. Later account-runtime tests
must extend that comparison to all touched accounts and instruction-boundary
diffs.

## Genesis result

Input: the exact `genesis.bin` member from the mainnet-beta source archive at
`asset/epochs/genesis.tar.bz2`.
The source archive is 20,144 bytes with SHA-256
`133f7eaefcd59466f3b291aadd1b0d3522432072cf5b539445218c6c125ea945`.
The extracted member is 132,347 bytes with SHA-256
`45296998a6f8e2a784db5d9f95e18fc23f70441a1039446801089879b08c7ef0`.

Command:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  genesis asset/epochs/genesis.tar.bz2
```

Observed facts:

```text
genesis_hash_base58=5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d
genesis_hash_hex=45296998a6f8e2a784db5d9f95e18fc23f70441a1039446801089879b08c7ef0
mainnet_beta=true
creation_time_unix=1584368940 operating_mode_discriminant=1 genesis_bin_len=132347
accounts=431 account_data_bytes=93534 executable_accounts=0
capitalization_lamports=500000000000000000
builtin=solana_config_program Config1111111111111111111111111111111111111
builtin=solana_stake_program Stake11111111111111111111111111111111111111
builtin=solana_system_program 11111111111111111111111111111111
builtin=solana_vote_program Vote111111111111111111111111111111111111111
poh ticks_per_slot=64 tick_duration=0s+6250000ns hashes_per_tick=Some(12500)
fees target_lamports_per_signature=10000 target_signatures_per_slot=20000
fees min=5000 max=100000 burn_percent=100
rent lamports_per_byte_year=3480 exemption_threshold=2 burn_percent=100
inflation initial=0 terminal=0 taper=0 foundation=0 foundation_term=0
epoch_0 slots=[0, 432000) count=432000
epoch_1 slots=[432000, 864000) count=432000
```

This command parses and fingerprints genesis. The replay path documented below
also constructs the exact Bank-created NativeLoader accounts and genesis
sysvars before processing the first Compact row.

## Exact Compact Archive V2 epoch-0/1 mutation replay

The release build replayed the two complete mainnet-beta generations in one
ordered process, retaining one account store and Bank lifecycle across the
epoch boundary:

```bash
/usr/bin/time -p target/release/blockzilla-replay-poc \
  replay-compact-chain \
  /private/tmp/blockzilla-mainnet-replay-compact/epoch-0 \
  /private/tmp/blockzilla-mainnet-replay-compact/epoch-1 \
  --sample-diffs 0 --sample-accounts 0
```

Both inputs are `complete=true`, manifest-bound Compact Archive V2
generations:

| Generation | Numeric slot window | Present block rows | Generation digest |
| --- | ---: | ---: | --- |
| `epoch-0-replay-compact` | `[0, 432000)` | 431,548 | `fe71d3f13216bc94251da2fd4bda16264292cea72c0a39eca0a7cbd584ce9473` |
| `epoch-1-replay-compact` | `[432000, 864000)` | 430,517 | `85dd5cb7efd28eb82eab23a5a81908ea8f7473cf59293b90edeaecdf461ac479` |

The replay-minimal set is 347,348,691 bytes. Epoch 0 embeds the exact
132,347-byte `genesis.bin` with SHA-256
`45296998a6f8e2a784db5d9f95e18fc23f70441a1039446801089879b08c7ef0`.
The reader validated both manifests and generation digests. This run used only
those Compact files: there was no CAR input or conversion.

Observed completion result:

```text
input_format=blockzilla-compact-archive-v2
input_mode=streaming-ordered-compact-generation-chain
runtime_profile=launch-v1.0.7-bank-sysvars-native-config-system-stake-and-trusted-vote-poc
replay_status=complete
generation_count=2
generation index=0 epoch=0 generation_id=epoch-0-replay-compact generation_digest=fe71d3f13216bc94251da2fd4bda16264292cea72c0a39eca0a7cbd584ce9473
generation index=1 epoch=1 generation_id=epoch-1-replay-compact generation_digest=85dd5cb7efd28eb82eab23a5a81908ea8f7473cf59293b90edeaecdf461ac479
epoch=1
completed_slot_range=Some(0)..=Some(863999)
completed_slots=862065
committed_transactions=14051214
failed_transactions=222336
committed_instructions=14051343
rolled_back_instructions=2
vote_mutations=14050966
config_mutations=12
system_mutations=189
stake_mutations=176
instruction_changed_accounts=200
bank_sysvar_writes=3448259
replay_state accounts=637 sha256=9e0cf0dde2432719682de7b44cf4314e042c19f95ffd969bd58559439553ec32
real 733.31
user 664.66
sys 45.95
```

An immediate second complete run processed the same two Compact generations
with identical counters and reproduced
`9e0cf0dde2432719682de7b44cf4314e042c19f95ffd969bd58559439553ec32`.
It took 721.79 seconds wall time (644.57 user, 50.76 system). The second run is
determinism evidence; because validation commands ran concurrently, its timing
is not used as a comparative benchmark.

The 862,065 completed slots are the present Compact block rows; the numeric
range also contains 1,935 skipped slots. The transaction counts account for all
14,273,550 archived transaction rows. `failed_transactions=222336` is derived
by executing the implemented native semantics, not read from archived outcome
metadata. Archived outcomes are observed but not consumed. The two rolled-back
instructions are speculative mutations from earlier instructions in
transactions that subsequently failed.

The complete epoch-0/1 corpus exercises these launch-era native paths:

- System's seven non-nonce instructions plus `InitializeNonceAccount` and
  `AuthorizeNonceAccount`; `AdvanceNonceAccount` is now also implemented and
  is exercised by the later NAS run, while `WithdrawNonceAccount` remains
  unsupported;
- Vote, `InitializeAccount`, `Authorize`, and `Withdraw`;
- Stake `Initialize`, `DelegateStake`, `Split`, `Authorize`, and `Withdraw`;
  `Deactivate` is now also implemented and is exercised by the later NAS run;
  and
- the generic Config byte-store ABI.

The audited epoch-0/1 instruction corpus contains no BPF-loader, user-SBF, or
token-program instruction, so this run does not exercise the ELF/JIT path.

This is a complete **POC mutation replay of the Compact rows**, not historical
Bank or epoch parity. Signature bytes are not cryptographically verified,
compute units are not metered, and fees and rent are not applied. SlotHashes,
the historical AccountsDB/Bank hash, the status cache, and the complete account
load/transaction pipeline are absent. The final SHA-256 is the POC's
deterministic pubkey-sorted account-state hash, not a historical Solana account
or Bank hash. General account and per-instruction diffs are tracked; token
balance reporting is deliberately not the replay objective.

## Earlier Compact Archive V2 input probes

The measured generation at
`/private/tmp/blockzilla-epoch0-replay-compact` is a deliberately bounded local
fixture for slots `[0, 10)`. Its `genesis.bin` is the exact extracted member
identified above, included in `archive-v2-generation.json` before the
generation digest was computed.

Command:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  probe-compact /private/tmp/blockzilla-epoch0-replay-compact \
  --start-slot 0 --end-slot-exclusive 10 \
  --max-slots 10 --sample-transactions 0
```

Observed summary:

```text
input_format=blockzilla-compact-archive-v2
cluster=mainnet-beta epoch=0 generation_id=epoch-0-slots-0-9-replay-poc slots_per_epoch=432000
generation_digest=bb8ecc3271770df50ad3ddcaec0e70b9fdc3444b14da59c94eec869449849a65
registry_sha256=927287dbce9105c9d116041f85c9ffaff264716f511eb18c450ba7e5a75c2c25
genesis source=ExactGenesisBin hash=5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d bytes=132347 accounts=431 reward_pools=0 builtins=4 ticks_per_slot=64 slots_per_segment=Some(1024)
scanned slots=10 transactions=34 retained_transactions=0 instructions=34
program=Vote111111111111111111111111111111111111111 instructions=34
```

The ten decoded index rows were slots 0 through 9 in order. Slot 0 contained no
transactions; slots 1 through 9 contained the 34 transactions and 34 top-level
Vote instructions. The probe opened the generation through its manifest,
validated its generation/registry binding and control files, recovered the
exact embedded genesis, resolved compact pubkey and blockhash references, and
decoded all selected messages without reading signature bytes or transaction
metadata payloads.

### All-Vote slots-0–9 System/Vote replay run

The same Compact fixture was then passed through the current bounded
launch-era native-System/trusted-Vote path. This fixture happens to contain
only Vote instructions, so no System mutation commits in this run:

```bash
cargo run -p blockzilla-replay --bin blockzilla-replay-poc -- \
  replay-compact-prefix /private/tmp/blockzilla-epoch0-replay-compact \
  --max-slots 10
```

Observed summary:

```text
input_format=blockzilla-compact-archive-v2
input_mode=streaming-one-compact-block-at-a-time
runtime_profile=launch-v1.0.7-bank-sysvars-native-config-system-stake-and-trusted-vote-poc
replay_status=complete
generation_id=epoch-0-slots-0-9-replay-poc
generation_digest=bb8ecc3271770df50ad3ddcaec0e70b9fdc3444b14da59c94eec869449849a65
epoch=0 completed_slot_range=Some(0)..=Some(9) completed_slots=10 committed_transactions=34 failed_transactions=0 committed_instructions=34 rolled_back_instructions=0 vote_mutations=34 config_mutations=0 system_mutations=0 stake_mutations=0 instruction_changed_accounts=4 bank_sysvar_writes=37 bank_sysvar_accounts=4 slot_hashes_unavailable=true
state_scope=serialized-genesis-plus-native-builtins-plus-bank-sysvars-plus-config-system-stake-vote-mutations commit_model=implemented-native-errors-derived archived_outcomes=observed-not-consumed bank_parity=false signatures_verified=false cu_metered=false fees_applied=false fee_sysvar_advanced=true fee_signature_classification=implemented-subset rent_applied=false genesis_sysvars_materialized=true child_bank_sysvars_materialized=clock-fees-recent-blockhashes-rewards-stake-history freeze_sysvars_materialized=slot-history slot_hashes_materialized=false bank_hash_computed=false
replay_state accounts=442 instruction_changed_accounts=4 bank_sysvar_accounts=4 sha256=e6932abcc1341b859a8700e7ff891183b477301120297bf576612ea240d19eb8
```

All 34 transactions contained one launch-era Vote instruction and produced one
Vote-account mutation. State was threaded across instructions for four distinct
vote accounts initialized from the exact compact `genesis.bin`. Each
instruction produced one account-data diff with before/after byte lengths,
before/after SHA-256 hashes, bounded changed-byte ranges, and an explicit
non-truncation flag. One representative first-instruction record was:

```text
mutation slot=1 tx=0 instruction=0 kind=vote vote_account=sCtiJieP8B3SwYnXemiLpRFRR8KJLMtsMVN25fAFWjW voted_slots=[0] root=None credits=0 disposition=Committed account_diffs=1
data_diff account=sCtiJieP8B3SwYnXemiLpRFRR8KJLMtsMVN25fAFWjW before_len=Some(3731) after_len=Some(3731) before_sha256=54526132bf2ddf8065c7ed937270ff5974f5cfc3270d28c8434f78e023d3ab41 after_sha256=b5ed0b973d85fee29c97e8822e8ca94243b142199d5d7312ba915fe0fd1421ad ranges=10 truncated=false
```

The streaming path retained only the current compact block and produced these
deterministic final data hashes for the four touched accounts:

```text
sCtiJieP8B3SwYnXemiLpRFRR8KJLMtsMVN25fAFWjW 3ef333c722560c66e6a63cfd861bf9f993899e02fb6d060d3f2f65166ddf775f
4785anyR2rYSas6cQGHtykgzwYEtChvFYhcEgdDw3gGL 84519c1957ff6d1ae2fe6d545da06240ff764e65e9c283cb16145c3331a72b26
8XgHUtBRY6qePVYERxosyX3MUq8NQkjtmFDSzQ2WpHTJ 38f7d7cd4f7b440efa7820f81e7b18bbcfa5a28f997e971c29d42f3c8d2b0719
9bRDrYShoQ77MZKYTMoAsoCkU7dAR24mxYCBjXLpfEJx 56646a1a7291a5ae796bdcf00e90bb3253f75793454bb6a67d347f3369fcc83e
```

The POC commit model marks a transaction committed when it succeeds under the
implemented native subset. Archived outcomes are observed but not consumed:
these transaction rows have no decoded status metadata and therefore have
outcome `Unknown`. The POC advances non-Bank-hash sysvars but still skips
SlotHashes, transaction recent-blockhash/status checks, fee debit, and rent, so
`Committed` here is not independent historical-success or parity evidence.

This fixture is **partial POC evidence, not an epoch-0 artifact or parity
result**. Its structurally sealed local manifest exercises the same reader
validation path, but it is not complete-generation production input and covers
only ten of epoch 0's 432,000 slots. It must not be published, registered, or
described as a complete epoch generation. The native path did mutate Vote
account data, advance all non-Bank-hash sysvars, and emit per-instruction diffs
from this all-Vote fixture, but it did not run the complete historical
transaction or Bank pipeline: fee/rent effects and SlotHashes were not applied,
Bank hashes were not computed, signatures were not cryptographically verified,
and compute units were not metered. No checkpoint was compared with a
historical runtime, and transaction outcomes were not independently derived.
This is therefore neither Bank parity nor epoch parity.

### Completed larger epoch-0 prefix

The same procedure was expanded from genesis through requested slots
`[0, 131072)` using the manifest-bound Compact Archive V2 generation. The
requested range contains 130,621 present block records and 451 missing slots.
The local generation is deliberately partial and must not be published as a
complete epoch.

```bash
target/debug/blockzilla-replay-poc replay-compact-prefix \
  /private/tmp/blockzilla-epoch0-failure-search-131072/compact \
  --max-slots 131072 --sample-diffs 2
```

The following historical state scope completed the entire available generation
after materializing all four genesis-declared NativeLoader builtin accounts. It
is retained as milestone evidence and superseded by the Bank-sysvar rerun below:

```text
runtime_profile=launch-v1.0.7-native-config-system-stake-and-trusted-vote-poc
replay_status=complete
generation_id=epoch-0-slots-0-131071-failure-search
generation_digest=1543fd33f34ca7b68b5cdd7e4914aeece69acdee9ab83b2e48a97de3c59d0ba8
epoch=0 completed_slot_range=Some(0)..=Some(131071) completed_slots=130621 committed_transactions=521176 failed_transactions=2 committed_instructions=521177 rolled_back_instructions=2 vote_mutations=521169 config_mutations=0 system_mutations=1 stake_mutations=7 changed_accounts=12
replay_state accounts=441 changed_accounts=12 sha256=f0c4987d548feb1688394e9e0f507bd2f022df192385d00e6bd44c65be16a8c4
first_derived_transaction_failure slot=105368 transaction=2 instruction=1 rolled_back_instructions=1 reason=missing_required_signature authority=Eo1iDtrZZiAkQFA8u431hedChaSUnPbU8MWg849MFvEZ
```

The two derived failures occur at slots 105,368 and 105,532. Each transaction
first executes this System instruction speculatively:

```text
instruction 0: System AllocateWithSeed
  target=oBR5GGynSXtzEBgLoV9vyACqgxGX2amXbe1U4HLBPEL
  base=Eo1iDtrZZiAkQFA8u431hedChaSUnPbU8MWg849MFvEZ
  seed="1" space=200 owner=Stake11111111111111111111111111111111111111
instruction 1: Stake raw=030000000080c6a47e8d0300
  accounts=[C7C8odR8oashR5Feyrq2tJKaXL18id1dSj2zbkDGL2C2,
            oBR5GGynSXtzEBgLoV9vyACqgxGX2amXbe1U4HLBPEL,
            C6erjt6KN8iAHpBaR4foLRS8HmbquANc99r7HMrwvRa6]
```

Under the launch-era Stake ABI, discriminant `3` plus the little-endian value
is `Split(1_000_000_000_000_000)`. The source's authorized staker is
`Eo1iDtrZZiAkQFA8u431hedChaSUnPbU8MWg849MFvEZ`, but the Stake instruction
passes only `C6erjt6KN8iAHpBaR4foLRS8HmbquANc99r7HMrwvRa6` as its authority
signer. The Split therefore returns `MissingRequiredSignature`; the allocation
diff is marked `RolledBack`, the transaction overlay is discarded, and replay
continues.

The native System path follows Solana v1.0.7 for these non-nonce variants:
`CreateAccount`, `Assign`, `Transfer`, `CreateAccountWithSeed`, `Allocate`,
`AllocateWithSeed`, and `AssignWithSeed`. `InitializeNonceAccount` and
`AuthorizeNonceAccount` are also implemented with the exact versioned state and
Bank inputs. `AdvanceNonceAccount` is implemented with the launch-era
recent-blockhash and nonce-state behavior. `WithdrawNonceAccount` remains
fail-closed.

Slot 105,800 passes the correct authority and commits the allocation and Split.
Slot 106,440 commits `Authorize(Staker)`, and slots 108,931, 109,104, 109,212,
109,312, and 109,688 each commit `Withdraw(1_000_000_000)`. These are the only
nine Stake instructions in the generation; every later instruction through
slot 131,071 is Vote. The state hash above is a deterministic POC hash, not a
historical accounts hash or Bank hash. Its baseline includes the exact
executable NativeLoader accounts for Config, Stake, System, and Vote. The
generation contains zero Config instructions; direct fixtures cover the
complete v1.0.7 Config byte-store ABI and its historical signer quirks.

All 521,178 transaction rows in this bounded Compact generation have flags
`0x00000000` and carry no decoded transaction metadata. Their archived outcome
is therefore `Unknown`; `HAS_ERROR=false` cannot be used as evidence that a
transaction committed. Replay must derive success or failure by executing the
historical runtime semantics.

### Bank-sysvar lifecycle rerun

The same Compact generation was replayed again after adding the exact six
genesis-Bank sysvar accounts and the v1.0.7 lifecycle that can be derived
without a Bank hash. Child-Bank Clock, Fees, and RecentBlockhashes;
epoch-boundary StakeHistory/zero-inflation Rewards; and per-freeze SlotHistory
are now materialized in historical order. SlotHashes remains explicitly
unavailable because Compact's PoH hashes are not Bank hashes.

```text
runtime_profile=launch-v1.0.7-bank-sysvars-native-config-system-stake-and-trusted-vote-poc
replay_status=complete
generation_id=epoch-0-slots-0-131071-failure-search
generation_digest=1543fd33f34ca7b68b5cdd7e4914aeece69acdee9ab83b2e48a97de3c59d0ba8
epoch=0 completed_slot_range=Some(0)..=Some(131071) completed_slots=130621 committed_transactions=521176 failed_transactions=2 committed_instructions=521177 rolled_back_instructions=2 vote_mutations=521169 config_mutations=0 system_mutations=1 stake_mutations=7 instruction_changed_accounts=12 bank_sysvar_writes=522481 bank_sysvar_accounts=4 slot_hashes_unavailable=true
replay_state accounts=448 instruction_changed_accounts=12 bank_sysvar_accounts=4 sha256=d425b2088adf01a0fdcbddceb287df4565b42c3358487d942d9b160ba52c65fd
```

The 448 accounts are 431 serialized genesis accounts, four NativeLoader
builtins, six genesis sysvars, SlotHistory, and six transaction-created
accounts. Bank writes include each completed Bank's SlotHistory freeze store
plus Clock, Fees, and RecentBlockhashes at every child-Bank boundary.

This hash supersedes the earlier POC state-scope hash above. It remains a
canonical POC replay-state hash, not a historical AccountsDB hash or Bank hash:
fee debit, rent collection, SlotHashes, status-cache checks, and the complete
transaction outcome pipeline are still absent.

Fees and RecentBlockhashes use the signature count of transactions reaching
the implemented execution subset. Exact v1.0.7 classification excludes
historical account-load failures, which this POC does not yet derive; the fee
rate remains at the launch minimum in this measured prefix, masking that gap.

## Automated checks

The following baseline checks passed on the host before the Cranelift milestone:

```text
cargo fmt -p blockzilla-replay -- --check
cargo check -p blockzilla-replay --all-targets
cargo clippy -p blockzilla-replay --all-targets -- -D warnings
cargo test -p blockzilla-replay
cargo test -p blockzilla-replay --target x86_64-apple-darwin
```

Post-Cranelift checks recorded on the host:

```text
cargo fmt -p blockzilla-replay -- --check                         PASS
cargo clippy -p blockzilla-replay --all-targets -- -D warnings
                                                                  PASS
cargo test -p blockzilla-replay                                   23 passed
cargo test -p blockzilla-replay --target x86_64-apple-darwin      15 passed
```

Post-Compact-V2/Vote-mutation checks recorded after the run above:

```text
cargo fmt -p blockzilla-replay -p blockzilla-read-sdk \
  -p blockzilla-format -p of-car-reader \
  -p blockzilla-archive-gateway -- --check                        PASS
cargo test -p blockzilla-replay                                  34 passed
cargo clippy -p blockzilla-replay --all-targets --no-deps \
  -- -D warnings                                                  PASS
cargo check -p blockzilla-replay --lib --no-default-features     PASS
cargo test -p blockzilla-read-sdk --all-features                 13 passed
cargo test -p of-car-reader genesis --lib                         2 passed
cargo test -p blockzilla-archive-gateway                         10 passed
cargo check -p blockzilla-archive-gateway                        PASS
cargo check -p blockzilla                                        PASS
```

Post-native-System checks recorded after the current runs:

```text
cargo fmt -p blockzilla-replay -- --check                         PASS
cargo test -p blockzilla-replay                                  55 passed
cargo clippy -p blockzilla-replay --all-targets --no-deps \
  -- -D warnings                                                  PASS
target/debug/blockzilla-replay-poc replay-compact-prefix \
  /private/tmp/blockzilla-epoch0-failure-search-131072/compact \
  --max-slots 131072 --sample-diffs 2                            EXPECTED STOP
first_failure slot=105368 transaction=2 instruction=1 \
  program=Stake11111111111111111111111111111111111111             PASS
replay_state sha256=a585575d718d67f199c07e31c45bc231da7636d1524437bf122e924968ed6d45 PASS
```

Those expected-stop results are retained as earlier milestone evidence and are
superseded by the native-Stake run. Post-native-Stake checks:

```text
cargo test -p blockzilla-replay --all-targets                   64 passed
cargo check -p blockzilla-replay --all-targets                  PASS
target/debug/blockzilla-replay-poc replay-compact-prefix \
  /private/tmp/blockzilla-epoch0-failure-search-131072/compact \
  --max-slots 200000 --sample-diffs 0                           COMPLETE
failed_transactions=2 rolled_back_instructions=2                PASS
stake_mutations=7                                               PASS
replay_state sha256=4a314c84b7890db15ae26e8a645ed0fa522f6f9ffbbd0de2159ee6f82ad7146b PASS
```

Post-native-Config/builtin-account checks supersede that state-scope hash:

```text
cargo test -p blockzilla-replay --all-targets                 79 passed
cargo clippy -p blockzilla-replay --all-targets --no-deps \
  -- -D warnings                                                PASS
target/debug/blockzilla-replay-poc replay-compact-prefix \
  /private/tmp/blockzilla-epoch0-failure-search-131072/compact \
  --max-slots 200000 --sample-diffs 0                           COMPLETE
config_mutations=0 system_mutations=1 stake_mutations=7         PASS
replay_state accounts=441                                       PASS
replay_state sha256=f0c4987d548feb1688394e9e0f507bd2f022df192385d00e6bd44c65be16a8c4 PASS
```

Post-Bank-sysvar/nonce checks supersede the previous account count and hash:

```text
cargo test -p blockzilla-replay --all-targets                 86 passed
cargo clippy -p blockzilla-replay --all-targets --no-deps \
  -- -D warnings                                                PASS
target/debug/blockzilla-replay-poc replay-compact-prefix \
  /private/tmp/blockzilla-epoch0-failure-search-131072/compact \
  --max-slots 200000 --sample-diffs 0                           COMPLETE
bank_sysvar_writes=522481 bank_sysvar_accounts=4               PASS
slot_hashes_unavailable=true                                   PASS
replay_state accounts=448                                       PASS
replay_state sha256=d425b2088adf01a0fdcbddceb287df4565b42c3358487d942d9b160ba52c65fd PASS
```

Post-account-store checks preserve that exact state while replacing the
canonical `BTreeMap` with an in-process `hashbrown` index, publishing
transaction overlays as validated batches, and writing SlotHistory through
small byte patches:

```text
cargo test -p blockzilla-replay                              92 passed
cargo check -p blockzilla-replay --all-targets               PASS
10-slot replay_state accounts=442                            PASS
10-slot replay_state sha256=e6932abcc1341b859a8700e7ff891183b477301120297bf576612ea240d19eb8 PASS
full-prefix replay_status=complete                            PASS
full-prefix completed_slots=130621                            PASS
full-prefix committed_transactions=521176 failed_transactions=2 PASS
full-prefix replay_state accounts=448                         PASS
full-prefix replay_state sha256=d425b2088adf01a0fdcbddceb287df4565b42c3358487d942d9b160ba52c65fd PASS
```

The synthetic optimized lookup baseline uses deterministic unique pubkeys,
identical account values, the same randomized lookup sequence, a warm-up pass,
and an equality-checked result checksum:

```bash
target/release/account-store-bench \
  --accounts 1000000 --lookups 5000000 --data-bytes 200 --rounds 5
```

```text
BTreeMap:           build=215.189 ms lookup_median=2323.812 ms  2151637 lookup/s
MemoryAccountStore: build=223.865 ms lookup_median= 400.174 ms 12494558 lookup/s
ratios: build=0.96x lookup=5.81x
checksum (both)=e03af3f544dd1466
```

This microbenchmark supports using `hashbrown` for the hot index; it does not
measure checkpoint I/O, WAL durability, mmap page faults, transaction overlay
copying, or end-to-end replay speed. Those remain acceptance gates for the
persistent backend.

Running Clippy without `--no-deps` still reports two existing warnings in
unrelated `blockzilla-format` compact-log/meta code; the replay crate itself is
warning-clean under `-D warnings`.

Post-Compact-chain and frozen-checkpoint checks preserve the exact full-prefix
state through the new multi-generation entry point:

```text
cargo test -p blockzilla-replay --all-targets                 106 passed
cargo check -p blockzilla-replay --all-targets                PASS
cargo clippy -p blockzilla-replay --all-targets --no-deps -- -D warnings PASS
scripts/test-sync-replay-compact.sh                            PASS
target/release/blockzilla-replay-poc replay-compact-chain \
  /private/tmp/blockzilla-epoch0-failure-search-131072/compact \
  --sample-diffs 0                                             COMPLETE
input_mode=streaming-ordered-compact-generation-chain          PASS
completed_slots=130621                                         PASS
replay_state accounts=448                                      PASS
replay_state sha256=d425b2088adf01a0fdcbddceb287df4565b42c3358487d942d9b160ba52c65fd PASS
```

Checkpoint coverage includes deterministic uninterrupted-versus-restored bytes
and state, frozen-phase enforcement, corruption/truncation/descriptor/cursor
rejection, and rejection of a caller-forged Compact cursor. The codec now
emits portable V2 boundary checkpoints beyond epoch 0 while retaining ancestry
from slot 0. Publication writes a new same-directory file, flushes it,
atomically renames it, and flushes the parent directory on Unix. Resume
requires a caller-supplied SHA-256 over the whole checkpoint, then reopens the
completed source generation and binds its digest, registry, row count, final
slot, epoch/schedule, and genesis before accepting successor row zero. The
embedded checksum detects corruption, but is not an authenticity boundary; the
expected whole-file digest must be retained in trusted job metadata, not only
in a sidecar beside the mutable checkpoint.

The host-native program cache is excluded from V2 because compiled artifacts
are rebuildable derivatives of authoritative account bytes. Legacy V1 restore
is limited to exhausted pre-activation state with no BPF-loader builtin; it
rejects in-progress, loader-containing, and epoch-34-or-later state, and the
next capture emits V2. The real epoch-33 V1 checkpoint has trusted whole-file
SHA-256
`e02520615763e3e16dc3815a75fd903cdc90a2f6116c264903ad18983c8e9f25`.
Authenticated restore against that digest and the completed epoch-33 Compact
anchor passed a one-slot epoch-34 resume probe. No separate epoch-33 account
state hash was recorded, and this probe is not full epoch-34 completion.
The Compact sync self-test covers exact allowlisting, pinned sizes/row counts,
mainnet genesis identity, interruption/resume, immutable manifest refusal,
unsafe source syntax, decoy alternate-source files, and forbidden alternate
ledger options.

The full epoch-0 and epoch-1 Compact directories were copied into the local
replay-minimal set and validated before the completed run above. They contain
431,548 and 430,517 block rows respectively and occupy 347,348,691 bytes
(331.26 MiB), including exact epoch-0 genesis. No alternate ledger container
was copied into or accepted by the replay path.

The test suite covers canonical ELF extraction and padding removal, upgradeable
state-tag validation, SBPF load/verify/execute, stack guard gaps, a verified
backward-loop watchdog, artifact-digest recomputation, epoch windows, account
creation and deletion, changed byte ranges, truncation hashes, and nested
instruction identity. The native-System additions cover all seven launch-era
non-nonce variants, historical post-instruction account-verifier behavior and
native error ordering, instruction/transaction atomic rollback, exact
`InitializeNonceAccount`, `AuthorizeNonceAccount`, and `AdvanceNonceAccount`,
fail-closed rejection of `WithdrawNonceAccount`, and the exact slot-105,368
derived-address seed fixture. Native-Stake coverage adds the launch 200-byte
state layout and Initialize/DelegateStake/Split/Authorize/Withdraw/Deactivate/
SetLockup,
wrong-authority rollback-and-continue, repeated-deactivation error handling,
trailing-byte instruction decoding, self-withdraw aliasing, read-only
atomicity, and zero-lamport commit purge.
Native-Vote coverage adds the exact 3,731-byte current-state layout,
InitializeAccount/Authorize/Withdraw, historical signer semantics, and
rollback-and-continue for derived errors. Native-Config coverage adds exact
short-vector prefix decoding, positional multisigner and duplicate/cardinality
quirks, opaque data/tail preservation, writable/owner post-verification, no-op
writes, and transaction-level rollback of an earlier System mutation. Genesis
tests also assert the exact one-lamport executable NativeLoader account shape
for all four declared builtins.

## Replay hot-path optimization — 2026-07-29

This pass measured and optimized the exact epoch-0 Blockzilla Compact Archive
V2 replay path. No CAR file was opened, converted, copied, or accepted.

The pre-change exact release baseline was:

```text
wall=101.09 s  user=81.02 s  system=16.05 s
```

The first coalesced-I/O and no-diff pass completed in 41.68 seconds. After the
Vote-state cache, in-place Bank-sysvar updates, allocation cleanup, and
benchmark corrections, two complete runs produced:

| Run | Wall | User | System | State SHA-256 |
| --- | ---: | ---: | ---: | --- |
| final 1 | 21.674 s | 18.71 s | 0.42 s | `7d07380fd242b4c4e701d9f4d85a1d0f809dfcef7a85c37210eeba6a71ceca36` |
| final 2 | 22.031 s | 18.70 s | 0.43 s | `7d07380fd242b4c4e701d9f4d85a1d0f809dfcef7a85c37210eeba6a71ceca36` |

The 21.853-second mean is **4.63x faster** than the 101.09-second
baseline, a 78.4% wall-time reduction. System time fell by about 97.4%.
Both final runs retained these exact counters:

```text
generation_digest=fe71d3f13216bc94251da2fd4bda16264292cea72c0a39eca0a7cbd584ce9473
completed_slot_range=0..=431999
completed_slots=431548
committed_transactions=1724872
failed_transactions=4
committed_instructions=1724873
rolled_back_instructions=2
vote_mutations=1724864
config_mutations=0
system_mutations=1
stake_mutations=8
instruction_changed_accounts=13
bank_sysvar_writes=1726189
replay_state_accounts=449
```

The maintained microbenchmark uses a real 10,000-row epoch-0 Compact prefix.
Timing runs keep allocation counters disabled; separate allocation runs count
successful allocation/reallocation requests and requested bytes. Fingerprints
compare the full account-state hash, exact changed-account and Bank-sysvar
account sets, counters, and first derived failure evidence.

```bash
target/release/replay-hotpath-bench \
  /private/tmp/blockzilla-mainnet-replay-compact/epoch-0 \
  --max-slots 10000 --warmups 1 --rounds 5
```

```text
timing all:   median=1661.075 ms  41531.0 ns/instruction  diffs=39996
timing none:  median= 564.002 ms  14101.5 ns/instruction  diffs=0
speedup:      2.945x

alloc all:    3912392 calls  1317250216 requested bytes
alloc none:    594244 calls   500607072 requested bytes

equivalence=PASS
state_hash=4709229ecc9b590712d977591237aa490e77b517b80ed5886aab14af8c2a85a0
```

Relative to analytical `all` capture, the execution-only `none` policy removes
84.8% of allocation calls and 62.0% of requested allocation traffic on this
prefix. Requested bytes are cumulative allocator traffic, not peak RSS.

The changes responsible for the end-to-end result are:

- coalesced sequential block-frame range reads instead of one file open/read
  sequence per row, with reusable input, decompressor, and output buffers;
- no repeated zero-fill of a reused range buffer before `pread` overwrites it;
- an explicit `None` diff policy that also skips hidden multi-instruction
  rollback-diff construction in execution-only mode;
- a replay-private decoded Vote-state cache that always writes canonical bytes
  after success and invalidates on errors or non-Vote variants;
- direct fixed-buffer Bank-sysvar writes, an ordered recent-blockhash queue,
  fixed written-account sets, and in-place SlotHistory updates; and
- inline common-case metadata, account-key, pre-account, and per-slot program
  count collections.

The replay path still verifies Compact control files and validates indexed
frame bounds/decompression. It intentionally no longer hashes the entire
historical blocks object on every open. Full payload SHA-256 verification must
occur once at generation admission, and the admitted local generation must be
immutable. This is a trusted replay optimization, not a claim that mutable
untrusted payload storage is authenticated during execution.

Validation after the optimization:

```text
cargo fmt --all -- --check                                      PASS
cargo test -p blockzilla-read-sdk --all-targets                 14 passed
cargo test -p blockzilla-replay --all-targets                   141 + 5 passed
cargo clippy -p blockzilla-read-sdk --all-targets --no-deps -- -D warnings PASS
cargo clippy -p blockzilla-replay --all-targets --no-deps -- -D warnings   PASS
```

### Streaming decode follow-up — 2026-07-29

The next pass removed the remaining common-path ownership conversions without
changing the Compact V2 input contract. Final replay probes now keep up to
eight static account keys, one instruction, eight instruction account indexes,
and 64 raw instruction bytes inline. That covers the measured launch-era Vote
shape; the historical May-24 wire type uses the same inline raw buffer, and the
duplicated per-instruction pubkey collection was removed. Consumers resolve
instruction account indexes against the transaction's account-key slice.

The before/after comparison uses the same real 50,000-row epoch-0 prefix,
199,996 transactions/instructions, one warmup, three alternating measured
rounds, and separate timing/allocation runs:

```bash
target/release/replay-hotpath-bench \
  /private/tmp/blockzilla-mainnet-replay-compact/epoch-0 \
  --max-slots 50000 --warmups 1 --rounds 3
```

| Metric | Before inline probes | After inline probes | Change |
| --- | ---: | ---: | ---: |
| no-diff median | 2,163.972 ms | 1,500.977 ms | -30.6% |
| no-diff ns/instruction | 10,820.1 | 7,505.0 | -30.6% |
| no-diff allocation calls | 1,157,543 | 157,565 | -86.4% |
| no-diff calls/instruction | 5.7878 | 0.7878 | -86.4% |
| no-diff requested bytes | 648,975,952 | 242,948,022 | -62.6% |
| all-diff median | 6,605.005 ms | 5,333.291 ms | -19.3% |
| all-diff allocation calls | 19,155,806 | 18,155,828 | -5.2% |

Both sides produced exact state SHA-256
`0f4b7a0352af8ef3b87b265b2843c36bb6117d81bccc935bc2432f89cc365395`
and identical replay counters. The resulting no-diff throughput is 1.44x the
pre-inline version on this prefix.

The optimized exact two-generation replay then completed all 862,065 present
rows spanning slots 0 through 863,999. A timed run reported 99.27 seconds wall,
96.80 seconds user, and 1.03 seconds system. A second direct invocation exited
zero and reproduced every counter and the final state hash:

```text
committed_transactions=14051214
failed_transactions=222336
committed_instructions=14051343
rolled_back_instructions=2
vote_mutations=14050966
config_mutations=12
system_mutations=189
stake_mutations=176
instruction_changed_accounts=200
bank_sysvar_writes=3448259
accounts=637
sha256=9e0cf0dde2432719682de7b44cf4314e042c19f95ffd969bd58559439553ec32
```

This is 7.27x faster than the previously documented 721.79-second complete
epoch-0/1 run, while retaining the exact terminal fingerprint. Both runs read
only the two manifest-bound Blockzilla Compact Archive V2 generations; no CAR
file was opened or converted.

That result still used one owned whole-slot transaction collection per present
block. The subsequent borrowed hot-block decoder removes it from the measured
current-schema path; a later section records that result.

## Earlier native x86-64 NAS Compact replay milestone — 2026-07-29

The optimized release replay binary was built and executed directly on an
x86-64 Linux NAS with `RUSTFLAGS="-C target-cpu=native"`. The input was the 11
ordered, complete epoch generations from epoch 0 through epoch 10. Every input
was admitted through its Blockzilla Compact Archive V2 manifest and generation
digest. No CAR file was opened, converted, copied into the replay set, or used
as a fallback.

The first attempt stopped at slot 882,928 on Stake discriminant 5. That
instruction is launch-era `Deactivate`; the v1.0.7 authority, state, epoch, and
already-deactivated semantics were implemented before retrying. A later run
stopped at slot 4,185,036 on a System `AdvanceNonceAccount` followed by a
`Transfer` in the same transaction. The launch-era nonce-state and
recent-blockhash behavior was implemented, and the complete chain was then
restarted and replayed successfully.

Observed completion result:

```text
input_format=blockzilla-compact-archive-v2
input_mode=streaming-ordered-compact-generation-chain
runtime_profile=launch-v1.0.7-bank-sysvars-native-config-system-stake-and-trusted-vote-poc
replay_status=complete
generation_count=11
epoch=10
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
wall_seconds=1009.411
user_seconds=991.233
system_seconds=3.072
```

The run covered 4,677,315 present Compact block rows across the numeric slot
range 0 through 4,751,999. Its measured wall-clock rates were approximately
239,456 committed transactions/s, 242,249 attempted transactions/s, and
4,633.7 present slots/s. The 2,819,618 failed transactions are outcomes derived
by the implemented POC semantics; archived outcomes were not consumed.

This result extends the executable Compact evidence through epoch 10, but it is
still not historical Bank parity. Signatures were not cryptographically
verified, compute units were not metered, fees and rent were not applied,
SlotHashes and historical AccountsDB/Bank hashes remain unavailable, and the
complete historical account-load/status-cache pipeline is not implemented. The
terminal SHA-256 is the POC's deterministic pubkey-sorted account-state hash,
not a Solana Bank or AccountsDB hash.

## Borrowed current-schema decoder and epochs 0–30 — 2026-07-29

The current-schema Compact V2 reader now uses a lending hot-block API. It
reuses decompression storage and exposes checked borrowed transaction-row,
message, and metadata slices to replay; legacy schemas keep the owned fallback.
The replay loop also reuses its transaction workspace. The format contract and
structural validation are unchanged, and the executable input remains
manifest-bound Compact V2 only—no CAR file was opened, converted, copied, or
accepted as a fallback.

### Pinned epoch-0 benchmark

The following comparison used one pinned NAS CPU. The baseline is the exact
runtime immediately before the borrowed-decoder/custom-encoder pass, not the
much older 101.09-second Apple Silicon milestone above:

| Workload/metric | Before this pass | Current | Change |
| --- | ---: | ---: | ---: |
| complete epoch-0 median | 7.50 s | 1.605 s | about 4.67x faster |
| 50,000-row median | 902.016 ms | 211.401 ms | about 4.27x faster |
| 50,000-row allocation calls | 157,580 | 7,584 | -95.2% |
| 50,000-row requested bytes | 246,146,828 | 103,550,361 | -57.9% |

The current epoch-0 value is a five-round median with a 1.559–1.652-second
range. The current 50,000-row value is a nine-round median with a
209.284–246.049-millisecond range. Both preserve exact replay counters and
state fingerprints; epoch 0 ends at SHA-256
`7d07380fd242b4c4e701d9f4d85a1d0f809dfcef7a85c37210eeba6a71ceca36`,
while the 50,000-row prefix ends at
`0f4b7a0352af8ef3b87b265b2843c36bb6117d81bccc935bc2432f89cc365395`.
Allocation calls count successful allocation/reallocation requests, and
requested bytes are cumulative allocator traffic rather than peak RSS.

The final Vote encoder writes canonical state directly into the destination
account buffer only after an exact two-phase capacity/conversion proof. That
change is useful for lower byte traffic and atomic semantics, but it is
performance-neutral in these measurements: the immediately preceding build
measured 212.657 milliseconds for 50,000 rows and 1.592 seconds for epoch 0,
versus the current 211.401 milliseconds and 1.605 seconds. No speedup is
attributed to the direct-to-account encoder by itself.

### Reward-discard A/B

The transaction-only borrowed view can discard archived reward rows that the
current mutation runtime does not consume. The epoch-0 A/B preserved the exact
full and 50,000-row state hashes:

```text
full_epoch_0_sha256=7d07380fd242b4c4e701d9f4d85a1d0f809dfcef7a85c37210eeba6a71ceca36
prefix_50000_sha256=0f4b7a0352af8ef3b87b265b2843c36bb6117d81bccc935bc2432f89cc365395
```

| Workload/metric | Before reward discard | Reward discard |
| --- | ---: | ---: |
| complete epoch-0 median | 1.604879 s | 1.614479 s |
| complete epoch-0 allocation calls | 40,280 | 40,280 |
| complete epoch-0 requested bytes | 180,280,671 | 180,280,671 |
| 50,000-row median | 211.401 ms | 213.409 ms |
| 50,000-row allocation calls | 7,584 | 7,584 |
| 50,000-row requested bytes | 103,550,361 | 103,550,361 |

The allocation figures are unchanged and the timing movement is noise, so no
epoch-0 speedup is attributed to this change. It is retained as protection
against decode and allocation spikes in later reward-heavy blocks. Requested
bytes remain cumulative allocator traffic, not peak RSS.

### Ordered epochs 0–30 completion

That borrowed-runtime revision, including launch-era Stake `SetLockup`,
completed 31 ordered manifest-bound generations from epoch 0 through epoch 30
without an unsupported-runtime stop:

```text
input_format=blockzilla-compact-archive-v2
input_mode=streaming-ordered-compact-generation-chain
replay_status=complete
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

This terminal hash is the deterministic pubkey-sorted POC mutation-state hash,
not a historical Solana Bank or AccountsDB hash. The run proves completion of
the available Compact rows through epoch 30 under the implemented semantics;
it does not prove later epochs or Bank parity. Signatures were not
cryptographically verified, compute units were not metered, fees and rent were
not applied, archived outcomes were observed but not consumed, and SlotHashes
plus historical AccountsDB/Bank hashes remain unavailable.

The cross-epoch checkpoint/resume path uses the deterministic frozen-state
codec described above. A completed-generation checkpoint is published by
same-directory write/flush/atomic-rename/parent-flush and is accepted for
resume only with a caller-supplied whole-file SHA-256 plus a reopened source
generation whose digest, registry, row count, final slot, epoch/schedule, and
genesis match. This is crash-safe publication and fail-closed provenance
binding for the POC; it does not make a digest stored beside a mutable
checkpoint trusted, and it does not add Solana Bank parity.

## Epochs 31–34 continuation and legacy loader boundary — 2026-07-29

The next Compact-only NAS continuation completed epochs 31, 32, and 33,
sealing the ordered chain through slot 14,687,999. No CAR input, conversion, or
fallback was used. The first epoch-34 attempt stopped before freezing slot
15,105,072 on a legacy
`BPFLoader1111111111111111111111111111111111` `Write` instruction. Its
cumulative committed-prefix report was:

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

The completed-slot boundary is 15,105,071. Under the diagnostic runner's
prefix semantics, these counters and the state hash can also include
transactions committed earlier in the not-yet-frozen failing block. They are
the epoch-34 failure-prefix result, not the epoch-33 checkpoint state hash.

The runtime implementation now follows the Solana v1.1.14 Stable epoch-34
activation schedule and materializes the legacy loader builtin when entering
that epoch. Its historical deployment profile covers exact instruction
decoding and error order, `Write` bounds and signer semantics, the epoch-34
Bank-rent rule for `Finalize`, executable transition, and the historical
post-instruction account verifier. A `Finalize` attempts to compile a
host-native derivative, but only publishes a successful artifact after the
entire transaction commits; compiler rejection is recorded and cannot decide
whether the historical account becomes executable.

Portable checkpoint V2 adds the loader mutation counter and intentionally
omits that derivative native cache. Strict V1 migration allows the exhausted
pre-activation epoch-33 checkpoint identified above, rejects unsafe legacy
states, and re-encodes as V2 on capture. The authenticated one-slot resume
proves the real V1 checkpoint can cross the activation boundary under the new
profile, not that epoch 34 completes.

Exact `solana_rbpf 0.1.28` verifier parity is still absent: the canonical ELF
extractor is currently the fail-closed structural gate. Executable program
invocation, the historical serialized account ABI, syscalls, and CPI are also
unimplemented. Those gaps require the next full Compact run to establish the
next real failure boundary; epoch 34 must not yet be described as complete.

## What is and is not proven

Proven:

- loader-account code can be reduced to a stable canonical ELF digest;
- the ELF is parsed and verified before execution;
- execution without protocol CU accounting works for the minor fixture, with a
  fixed watchdog preventing an unbounded guest loop;
- the supported upstream native JIT path compiles and executes under macOS
  x86-64/Rosetta;
- the strict-subset Cranelift backend emits and executes native AArch64 code in
  an ARM64 process, with 256-input interpreter parity and checked memory-fault
  parity;
- unsupported hosts report interpreter use instead of claiming native code;
- the launch genesis fingerprint and first two epoch windows are decoded;
- the manifest-bound, partial Compact Archive V2 slots 0–9 fixture opens with
  exact `genesis.bin` and decodes 10 slots, 34 transactions, and 34 top-level
  instructions;
- those 34 instructions produce 34 launch-era Vote-account mutations across
  four accounts with per-instruction before/after data diffs;
- the two complete manifest-bound Compact generations replay in one ordered
  process across 862,065 present block rows and all 14,273,550 transaction
  rows, ending in the recorded deterministic POC state hash;
- the 31 complete manifest-bound Compact generations from epoch 0 through
  epoch 30 replay on an x86-64 Linux NAS across 12,905,623 present block rows
  and all 804,471,606 transaction rows, ending in the recorded deterministic
  POC state hash;
- epochs 31, 32, and 33 subsequently complete from the authenticated chain,
  extending its sealed completed-generation boundary through slot 14,687,999;
- reward-discard decoding preserves the exact epoch-0 and 50,000-row hashes,
  although it provides no timing or allocation improvement on epoch 0;
- the launch-era native System path implements all seven v1.0.7 non-nonce
  variants plus nonce initialization, authorization, and advancement;
- the launch-era native Vote path implements Vote, InitializeAccount,
  Authorize, and Withdraw;
- the launch-era native Stake path derives both failed Split retries, commits
  the authorized Split/Authorize/five-Withdraw sequence, implements Initialize,
  DelegateStake, Deactivate, and SetLockup, and reaches the end of epoch 33;
- the v1.1.14 Stable schedule activates the legacy BPF loader at epoch 34, and
  its exact `Write`/`Finalize` deployment profile plus commit-gated derivative
  cache are implemented;
- portable checkpoint V2 excludes native derivatives, strictly migrates the
  exhausted epoch-33 V1 checkpoint, and passed an authenticated one-slot resume;
- the six genesis sysvars and non-Bank-hash child lifecycle reproduce their
  exact historical account bytes, with SlotHashes explicitly unavailable; and
- the initial general-account diff representation behaves deterministically.

Not yet proven:

- execution of a real loader-deployed SBPF program through the JIT on the
  intended x86-64 Linux production target; the completed NAS corpus exercises
  native builtins, not the ELF/JIT path;
- exact `solana_rbpf 0.1.28` verifier parity for historical `Finalize`;
- full SBPF instruction coverage in the AArch64 backend;
- signed/notarized macOS Hardened Runtime compatibility (`MAP_JIT` and the JIT
  entitlement remain a separate packaging test);
- a portable or persistent `.so` artifact;
- executable-program invocation, Solana's serialized account ABI, syscalls,
  CPI, or VM memory mapping for real programs;
- historical Bank-wide, Config, rent, fee, nonce, reward, or rollback parity;
- the complete historical transaction and Bank pipeline from Compact Archive
  V2, including fee/rent effects, remaining nonce/status-cache behavior,
  SlotHashes and remaining sysvar parity, AccountsDB/Bank hashes, signatures,
  and compute-unit semantics;
- completion of the full epoch-34 Compact generation;
- historical epoch parity; or
- repeatable production replay throughput across multiple timed runs on the
  target x86-64 Linux host.
