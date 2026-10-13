# Hivezilla validator test: completion plan

The user wants a lower-cost model to finish this task. Do not start more parallel agents. Use short updates and ASD-STE100 simple technical English. No further design round is needed.

## Authorized work and limits

Prepare and run a non-voting Agave node on `ubuntu@213.239.141.61`, with local Yellowstone gRPC. Capture a small finalized sample, normalize it, build a verified Compact V2 execution test archive, record CPU/memory/time/size metrics, and upload the test archive to Foundation R2 only after the exact bucket, test prefix, and credential-file path are known.

An async question for the R2 bucket, prefix, and credential-file path is still pending. No answer was received. Do not guess a bucket or read secret keys into logs. All local work can continue.

This is a finite execution test, not a canonical epoch archive or a continuous production service. The source retains raw current-schema data. The hot projection does not contain every raw field, including reward commission_bps. No independent consensus, cryptographic PoH, signature, shred-boundary, or footer-completeness proof is claimed. The native validator plugin is a later step, after the local Yellowstone baseline.

Do not commit or push. Keep all earlier dirty benchmark and raw R2 work. Do not remove benchmark data. Do not restart the running validator.

## Running remote node

- Root: `/home/ubuntu/live/hivezilla-validator-20260930`.
- Source: `<root>/source`.
- Service: `hivezilla-validator-canary.service`, active PID `313135` at last check.
- Public identity: `J7qpvCEWhdBgPdYRV3s3btWPrRVXa2qnAzgvxWTvtbrQ`.
- Genesis: `5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d`.
- Agave v4.3.0, source commit `825efd18292aff6ffcf9daa0f7612f21b3531a72`, built with Rust 1.97.1 and native CPU options.
- Validator: `<root>/releases/agave-source-v4.3.0/target/release/agave-validator`.
- Yellowstone v16.0.0+solana.4.3.0, exact matching plugin. Config check passed.
- RPC `127.0.0.1:8899`; gRPC `127.0.0.1:10000`; metrics `127.0.0.1:8999`.
- Unit began about 19:52:51 UTC on 2026-09-30, maximum run time 6 hours. No restart, no boot enablement. MemoryHigh 800G, MemoryMax 850G, NOFILE 1,000,000, MEMLOCK 2,000,000,000.
- `--no-voting`, `--no-xdp`, `--private-rpc`, `--full-rpc-api`, transaction history and extended metadata enabled, blockstore limit 100,000,000 shreds.
- Genesis download and gossip succeeded. Snapshot is downloading from a known Anza validator. At 20:11:22 UTC, 33,000,070,216 bytes had arrived out of 113,190,255,518 bytes. RPC was not ready. Source ignores HTTP Range; keep the transfer running.
- Snapshot file: `<root>/snapshots/tmp-snapshot-452060793-4Gz9cdMZtdrP8bcyxq4PahkQ57ERxnsaMWpT7GLu4xtU.tar.zst`.
- Kernel UDP max values are 134,217,728. `<root>/reports/kernel-before.json` keeps original values 4,194,304 for both rmem_max and wmem_max. Never replace its contents.
- Identity key exists with mode 0600. Do not read it into output. Config, install, and report JSON files were set to 0600; validator binary is 0700.
- Hardware: EPYC 9555P, 64 physical cores / 128 logical, 1.1 TiB RAM, one 7.68 TB NVMe. About 3.48 TB disk free at last check. No claim that NIC link speed is internet speed.

## Code already built and tested on Linux

Current capture is isolated from legacy format 2. Two new Git aliases use Yellowstone client/proto 14.0.0 from exact revision `bfd1d7ee548081b6e36d4238d5ce8f34c94c4fb6`. Crates.io 14.0.0 is yanked; keep the Git pins. Existing client 13.5.0/proto 12.7.0 remain unchanged. Root Cargo.lock includes the new aliases.

- `hivezilla/service/src/grpc_raw/current.rs` and tests: sealed format-3 finite capture, full pinned-schema events, V1 config, rewards, bank IDs, parent invalidations, slot events, footer events, before/after local RPC identity evidence. 12 tests.
- `hivezilla/service/src/execution_canary.rs`: trusted finalized normalization with exact getBlocks coverage, parent chain and selected-bank checks, dense transaction/entry indices, bounded output, retained raw evidence. 9 tests.
- `crates/blockzilla-live-format/src/execution_canary_v1.rs`: receipt types.
- `blockzilla/cli/src/archive_v2/execution_canary.rs`: verified Compact V2 writer wrapper, explicit noncanonical marker, evidence/provenance checks. 5 tests.
- `hivezilla/object-store/src/uploader/compact_archive.rs`: immutable R2 uploads, full remote SHA-256 read-back, manifest last, durable retry state, no catalog/deletion authority. 12 focused tests.
- Main CLI already has `record-grpc-current`, `inspect-grpc-current`, `normalize-current-execution`; Blockzilla has `build-live-execution-canary`; uploader has `upload-compact-archive`.

Completed remote checks before the new Rust control modules:

1. Hivezilla library: 490 passed. Run serial with umask 0077. Parallel tests have an existing transient fork/flock problem. Do not change legacy lock policy here.
2. Blockzilla execution_canary tests: 5 passed.
3. Shared live-format tests: 10 passed.
4. Object-store tests: 91 library + 12 compact integration + 9 R2 transport passed.
5. Release build of hivezilla, blockzilla, blockzilla-s3-upload: passed in 201.55 seconds.

Logs: `<root>/reports/check-{0..4}.log`; gate report `<root>/reports/pipeline-check-results.json` (JSON array of command/exit_code/elapsed_seconds/log). Driver `<root>/run-checks.py` is an initial one-off build experiment; do not turn it into a maintained service.

Build env: PATH starts `/home/ubuntu/.cargo/bin`; RUSTFLAGS=`-C target-cpu=native`; CARGO_BUILD_JOBS=16; CARGO_TARGET_DIR=`/home/ubuntu/benchmarks/zilla-triton-20260930/source/target`. Use default Cargo/Rust 1.98.0 for workspace. No local Cargo builds: Mac disk is low.

Private installed binaries in `<root>/bin` currently reflect the tested capture pipeline BEFORE the new control modules. Rebuild Hivezilla and replace it after native control tests pass. Existing Blockzilla and uploader binaries can remain if unchanged. Installation report `<root>/reports/pipeline-binaries.json` records their hashes.

## Native control modules

Repository policy `docs/architecture/rust-runtime-boundary.md` requires all new operational controls in Rust. Temporary Python drafts were used as test oracles and are now deleted. The three native ports are compiled and tested:

- `validator_canary_prepare.rs` and `validator_canary_prepare/tests.rs`: `PrepareValidatorCanaryArgs`, async `run -> Result<Value>`. Exact pinned release downloads and hashes, exact-member extraction of solana and solana-keygen via maintained tar, matching config/argv and install report, no key generation/start/kernel changes. Focused tests passed.
- `validator_canary_run.rs` and `validator_canary_run/tests.rs`: `RunLocalExecutionCanaryArgs`, async `run -> Result<Value>`. Same finite controller, exact RPC evidence, 32 blocks/512 MiB capture/500 GiB free floor, time/CPU/RSS/threads, hot compression separate from raw evidence, owned child cleanup on timeout and SIGTERM. Focused tests passed.
- `validator_canary_ops.rs`: `ValidatorCanaryArgs`, async `run -> Result<Value>`. Actions start/stop/status/execute, fixed unit controls, exact gate checks, active PID/start-time/boot/invocation pinning, at most 4 hours wait for the correct healthy node and an advancing finalized slot. Its focused tests passed.

`lib.rs` exports all three. `main.rs` already wires `prepare-validator-canary`, `validator-canary`, and `run-local-execution-canary` with the agreed Args types and async run APIs. No dependency change was requested.

## Current state

- All five test/build gates passed with the native controls. Hivezilla: 514 library tests; compact builder: 5; shared format: 10; object store: 91 library + 12 compact integration + 9 R2 transport tests. The release build passed.
- The native Hivezilla binary is installed at `<root>/bin/hivezilla`, SHA-256 `f383c02217240c87de7218d0ddcc145c77613292a801502c2bc2d30734cfd9c3`. Its help and status commands passed.
- Native readiness process PID 342347 is waiting at `<root>/reports/canary-readiness-e45e7f2810e8cab09c99f82ce6243bf5.json`. At 20:28 UTC it had one sample and no capture. Validator RPC was not ready; about 64 GB of the 113 GB snapshot had downloaded. The node remains PID 313135.
- The gate report `<root>/reports/pipeline-check-results.json` points to private logs `check-10.log` to `check-14.log`; its hash is in `<root>/reports/native-execution-dispatch.json`.
- Docs use native commands. R2 details remain pending; no upload has been attempted.

## Remaining work

1. Monitor the native readiness report at intervals. It waits at most four hours for the existing node, then runs one 32-block finalized capture.
2. Read the capture and compact metrics. Report the real block count, time, hot compression, CPU/RSS, and node delay.
3. Upload only after the user supplies the R2 bucket, test prefix, and credential-file path. The prefix must start `execution-canary/`. Report success only after remote read-back and the final manifest.

If the node is still bootstrapping when the runner is started, state clearly that execution has started and results are pending. Do not spend tokens on frequent unchanged polls. Do not create a scheduled automation unless the user requests it. No full epoch, canonical promotion, or native plugin rollout is authorized by a small test result.
