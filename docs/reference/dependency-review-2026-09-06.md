# Dependency review — 6 September 2026

The update uses the latest stable releases that fit the supported wire formats. The workspace minimum Rust version is 1.98. The review covered 46 Cargo manifests, including the workspace manifest, and five JavaScript package manifests. The initial manifest inventory contained 113 distinct direct registry dependency names, including the separately published `of-car-reader` that is also a workspace member. No Python dependency manifest was found.

The [JavaScript dependency review](js-dependency-review-2026-09-06.md) covers the web application and Workers tooling.

The registry inventory and release notes were checked on 6 September 2026. A newer version number alone is not proof of compatibility. Solana repair packets, historical vote instructions, signatures, and stored archive bytes have explicit compatibility requirements.

## Rust release review

The table lists updates with material API, dependency, or behavior changes. Compatible patch updates are recorded in `Cargo.lock`.

| Dependency | Update | Relevant upstream change and local check |
| --- | --- | --- |
| `zstd` | 0.13.3 → 0.14.0 | Prepared-dictionary constructors now retain a borrow of the dictionary. `Decoder::finish()` reads the rest of the current frame; `finish_frame()` also reports a read error. No repository call uses decoder `finish()` or a prepared-dictionary constructor. CAR range scans stop by dropping the decoder. Bulk dictionary calls use the copied-dictionary constructors. Preserve compressed input, frame boundary, and truncation tests. [Release notes](https://github.com/gyscos/zstd-rs/releases/tag/v0.14.0). |
| `ruzstd` | 0.8.3 → 0.9.0 | The decoder can limit the window size and applies the limit to the first frame. The default limit remains 100 MB. The release also fixes a malformed-dictionary panic. Check reusable CAR metadata decoding and malformed frames. [Release notes](https://github.com/KillingSpark/zstd-rs/releases/tag/v0.9.0). |
| `bzip2` | Runtime 0.5.2 → 0.6.1 | The default backend changes from C to `libbz2-rs-sys`. CAR already used 0.6.1. The update shares the version and keeps genesis/snapshot decompression checks. [0.6 release notes](https://github.com/trifectatechfoundation/bzip2-rs/releases/tag/v0.6.0). |
| `base64` | 0.22.1 → 0.23.1 | Adds runtime-selected SIMD engines and changes an error payload. Existing `GeneralPurpose` engines retain their API. Check native and WebAssembly decoding paths. [Release notes](https://github.com/marshallpierce/rust-base64/blob/master/RELEASE-NOTES.md). |
| `base58-turbo` | 0.1.0 → 0.3.0 | Used by the Old Faithful edge reader. The published package did not include a changelog, and the upstream release list had no release text at review time. Check the published API and existing address/hash encoding tests. [Published package](https://docs.rs/crate/base58-turbo/0.3.0). |
| `ed25519-dalek` | Remaining 2.2.0 uses → 3.0.0 | Updates `signature`, `sha2`, and random-source traits; removes the `std` feature. The reader/indexer test fixtures use fixed signing keys. Preserve strict signature verification and fixed packet tests. [Changelog](https://github.com/dalek-cryptography/curve25519-dalek/blob/main/ed25519-dalek/CHANGELOG.md). |
| `sha2` | Remaining 0.10.9 uses → 0.11.0 | Uses `digest` 0.11; low-level compression functions move to `block_api`, and `std`/`asm` features are removed. Existing `Digest`/`Sha256` users need fixed hash and identity tests. [Changelog](https://github.com/RustCrypto/hashes/blob/master/sha2/CHANGELOG.md). |
| `hashbrown` | Remaining 0.15.5 uses → 0.17.1 | Updates the default hash builder and fixes raw-table failure handling. Entry and ownership APIs changed. Hash iteration order must not define stored row order. [Changelog](https://github.com/rust-lang/hashbrown/blob/master/CHANGELOG.md). |
| `lru` | 0.16.4 → 0.18.4 | Updates `hashbrown`; fixes mutable-reference lifetime and panic-safety defects. Check cache eviction and reuse behavior. [Changelog](https://github.com/jeromefroe/lru-rs/blob/master/CHANGELOG.md). |
| `quick-xml` | 0.26.0 → 0.42.0 | The latest API uses UTF-8 strings in events and attributes. Reader decoding/error APIs changed; entity references have a separate event. The uploader error-code parser needs text, numeric/predefined entity, and malformed XML checks; CDATA remains rejected. [Changelog](https://github.com/tafia/quick-xml/blob/v0.42.0/Changelog.md). |
| `rand` | 0.8.5 → 0.10.2 | Renames random traits and removes deprecated constructors. The monitor uses random jitter; the runtime's cryptographic compatibility code is separate. [Migration guide](https://rust-random.github.io/book/update.html). |
| `tower-http` | 0.6.8 → 0.7.1 | Changes compression negotiation, range handling, redirects, and some CORS/static-file behavior. Unsupported compression can return 406. Preserve gateway and HTTP behavior tests. [Changelog](https://github.com/tower-rs/tower-http/blob/master/tower-http/CHANGELOG.md). |
| `topcoat` | 0.5.0 → 0.7.0 | 0.6 removes `CxBuilder` and changes context/layer APIs. 0.7 changes streaming server rendering and requires Rust 1.98. The monitor is migrated; 113 focused tests pass, including five server-rendered routes, vendored assets, and stream lifecycle checks. [Changelog](https://github.com/tokio-rs/topcoat/blob/main/CHANGELOG.md). |
| `object` | 0.37.3 → 0.40.0 | ELF fields/constants use newtypes, and ELF section/segment flags change. The replay loader uses the read API; validate ELF loading and reject invalid files. [Changelog](https://github.com/gimli-rs/object/blob/master/CHANGELOG.md). |
| Cranelift | 0.134.2 → 0.135.1 | The five direct Cranelift crates move together. The associated Wasmtime 48 notes include new optimization and instruction selection work. Preserve interpreter/JIT result comparisons on supported native targets. [Release notes](https://github.com/bytecodealliance/wasmtime/blob/v48.0.1/RELEASES.md). |
| `solana-sbpf` | 0.21.0 → 0.24.0 | The intervening releases make memory-region fields private and introduce `HostBuffer`; memory mapping no longer returns a raw integer address. They also remove `allow_memory_region_zero` and fix memory/JIT handling. The public release list ended at 0.23 when reviewed. The published 0.24 source confirms `vm_addr_range()` and `HostBuffer::ptr()` for the local adapter. Native artifact identities and the replay profile version are updated to invalidate older cached code. The syscall adapter retains rejection of region-zero addresses, including zero-length reads and writes; the new upstream mapping permits an empty range at zero. Replay and memory-boundary tests pass. [Release notes](https://github.com/anza-xyz/sbpf/releases), [0.24 API](https://docs.rs/solana-sbpf/0.24.0/solana_sbpf/). |
| Agave entry/network/RPC crates | 4.1.2 → 4.2.2 | 4.2 changes some parsed RPC fields and removes old blockstore reward fallback decoding. These are not a change to the archive SDK's own format. The local repair compatibility enum is aligned with 4.2.2, including its unused final request variant. The block-marker test uses the renamed `from_block_header` constructor. Fixed request, ping, and pong bytes remain the compatibility check. [4.2 changelog](https://github.com/anza-xyz/agave/blob/v4.2.2/CHANGELOG.md), [repair protocol source](https://github.com/anza-xyz/agave/blob/v4.2.2/core/src/repair/serve_repair.rs). |
| Yellowstone client/proto | Client 13.5.0 / proto 12.7.0 | Adds gossip subscription and retains the V1 transaction config in protobuf. The block relay authenticates the new unsupported gossip endpoint before returning `Unimplemented`. The existing raw stream rejects a present V1 config at admission and decode boundaries, preserving payload format 2 and identity schema 1. The Legacy/V0 ledger adapter also rejects it instead of dropping signed fields. It still proves config-absent versioned messages with the fee-payer signature. [Protocol source](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-proto/proto/geyser.proto), [transaction schema](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-proto/proto/solana-storage.proto). |

## Required compatibility versions

### Agave and the split Solana SDK

Agave 4.2.2 is the latest stable release checked. It still uses `wincode` 0.5 traits and places explicit upper bounds on several split SDK crates. Later split SDK releases use `wincode` 0.6. A matching byte layout does not make traits from different crate versions interchangeable.

The current compatible direct versions are:

| Crate | Selected version | Latest stable checked | Reason |
| --- | --- | --- | --- |
| `solana-address` | 2.6.1 | 2.7.0 | Agave requires `<2.7.0`. The local gossip wire structs need its 0.5 schema traits. |
| `solana-hash` | 4.5.0 | 4.6.0 | Agave requires `<4.6.0`. 4.5.0 is the latest compatible release. |
| `solana-pubkey` | 4.2.1 | 4.3.0 | 4.2.1 bounds its address dependency below 2.7; 4.3.0 requires address 2.7. |
| `solana-signature` | 3.4.1 | 3.5.2 | Agave requires `<3.5.0`. The local gossip wire structs use its 0.5 schema traits. |
| `solana-short-vec` | 3.2.2 | 3.3.0 | The CLI uses this directly. 3.3.0 changes to the 0.6 schema traits; Agave transaction/message serialization still uses 0.5. |
| `solana-vote-interface` | 6.0.3 | 6.1.0 | 6.0.3 shares the compatible SDK family and replaces the CLI's 5.1.1 dependency. 6.1.0 requires the later split SDK. Historical instruction bytes remain encoded with bincode 1. |
| `wincode` for Agave wire types | 0.5.5 | 0.6.1 | Required by Agave and the local repair/gossip compatibility surface. |
| `wincode` for Blockzilla formats | 0.6.1 | 0.6.1 | Already current. Stored archive configuration and schema stay unchanged. |

Primary dependency evidence: [entry 4.2.2](https://crates.io/api/v1/crates/solana-entry/4.2.2/dependencies), [RPC client 4.2.2](https://crates.io/api/v1/crates/solana-rpc-client/4.2.2/dependencies), [transaction status types 4.2.2](https://crates.io/api/v1/crates/solana-transaction-status-client-types/4.2.2/dependencies), [pubkey 4.2.1](https://crates.io/api/v1/crates/solana-pubkey/4.2.1/dependencies), [vote interface 6.1.0](https://crates.io/api/v1/crates/solana-vote-interface/6.1.0/dependencies).

`wincode` 0.6 makes its reader trait unsafe, changes required methods, and changes derive validation and collection allocation handling. These changes do not justify replacing Agave's selected trait family. [0.6 release notes](https://github.com/anza-xyz/wincode/releases/tag/wincode%40v0.6.0), [0.6.1 release notes](https://github.com/anza-xyz/wincode/releases/tag/wincode%40v0.6.1).

### Historical vote bytes and replay curve checks

`bincode` remains at 1.3.3 for historical `VoteInstruction` bytes. Version 3.0.0 is a non-buildable tombstone release. A blanket update would break the build and would not preserve the legacy encoding. Four fixed compact/tower instruction fixtures were generated with the pre-update vote SDK 5.1.1 and bincode 1.3.3. The tests compare new serialization to those bytes and require the archive parser to keep compacting each variant. The historical fallback parser tests remain in place. [bincode 3 package notice](https://docs.rs/crate/bincode/3.0.0).

The runtime's original exact `curve25519-dalek` 2.1.0 pin was introduced in
commit `636351be8af86181af41b9375b7d91abd42a23a8` on 2 August 2026, without a
separate version rationale. This update moves the direct dependency to 5.0.0
after a source and behavior comparison of the PDA point-decompression check.

The old and new source use the same low-255-bit field decoder, including its
noncanonical-input behavior, and the same square-root validity and sign-bit
logic. The standalone comparison checked 101,866 inputs with **zero differences**
in acceptance or canonical recompression bytes: 296 field-boundary/sign cases,
510 single-bit/sign cases, 32 small-order cases, four official SDK PDA goldens,
1,024 PDA bump candidates, and 100,000 deterministic inputs. The 5.0 release
changes the Rust edition, random/digest dependencies, and unrelated scalar/group
APIs; none changes the `CompressedEdwardsY::decompress` API used here.
[5.0 changelog](https://github.com/dalek-cryptography/curve25519-dalek/blob/main/curve25519-dalek/CHANGELOG.md),
[comparison source and instructions](fixtures/curve-upgrade-2026-09-06/README.md),
[recorded evidence](fixtures/curve-upgrade-2026-09-06/evidence.json).

The measured run used Rust 1.98.1 on aarch64-apple-darwin. It is not an exhaustive
proof over all inputs or a separate check of every 32-bit/fiat backend. Source
selection on the supported default 64-bit targets uses the reviewed field
implementation. Existing replay/PDA and interpreter/JIT tests verify the
application integration. The standalone audit is outside the workspace; its
old curve dependency is not an application dependency.


## Removed unused and duplicate dependencies

`cargo machete --skip-target-dir` initially reported four unused direct dependencies in the Hivezilla service: `solana-address`, `solana-reward-info`, `solana-signature`, and `solana-short-vec`. These had served as resolver anchors. Agave 4.2.2 now supplies explicit bounds, the local gossip crate retains its required value-type pins, and the CLI has a real short-vector use. The four service entries and their feature links are removed. The transitive reward type remains at Agave's compatible 6.2.0; removing its unused direct entry does not upgrade the wire type to 6.3 or 7.

`tonic-prost` is retained. Generated protobuf code in `OUT_DIR` uses it, which a source-only unused-dependency scan cannot see.

The resolved registry graph changes from 794 to 793 package/version entries. This is a small net reduction: newer dependencies also add packages.

`of-slot-ranges` and the Old Faithful edge reader now use the workspace `of-car-reader` path with the existing publish version. Their registry-only dependency compiled a second reader and its older dependency graph. The local path selects the reviewed SDK and does not introduce a dependency cycle.

## Completed validation

The final checks used Rust 1.98.1 on `aarch64-apple-darwin`, the locked dependency graph, and four test threads. WebAssembly checks used `wasm32-unknown-unknown`. Native checks cleared the local C include/library overrides and disabled incremental compilation. They did not use the musl build flags.

| Check | Result |
| --- | --- |
| `cargo test --offline --workspace --all-targets --locked` | 3,568 passed across 128 test targets; zero failed, one ignored. |
| Optional CLI and user-program-index features | Both full-target build checks passed. Developer-tool tests: 247 passed. Metadata-repair tests: three passed. |
| Optional shred reconstruction and repair packets | Eight shred compact, 12 FEC trial, 22 epoch audit, and six fixed repair-wire tests passed. |
| Monitor migration | 113 focused tests passed, including HTTP routes, assets, and stream lifecycle. These overlap the workspace total. |
| Pure Rust CAR decoder | 102 tests passed with only `query-sdk,compact-index,zstd-wasm`. The compressed protobuf fixture needs no native compressor dependency. |
| Worker native features | 15 tests passed. |
| Workers | Both complete release WebAssembly builds and Wrangler dry runs passed. No deployment was made. |
| CI CAR-to-V2 fixture | Conversion and read commands both exited successfully: one block, 4,208 transactions, 771,452 uncompressed bytes and 127,578 compressed bytes. The zstd, index, and metadata outputs were nonempty. |
| JavaScript | 198 tests passed. Explorer type check and static build passed. All five npm dependency trees and vulnerability audits passed. |
| Repository scripts | 18 sample-comparison tests, six workspace-target tests, and the archive wire-boundary check passed. |
| Dependency and format checks | `cargo machete --skip-target-dir`, workspace formatting, and whitespace checks passed. |
| Documentation | 183 Markdown files, 434 local links, and 13 local fragments checked with no broken destinations or anchors. |

These rows describe validation runs, not separate sets of tests. The monitor, decoder, Worker, and optional-feature runs can repeat workspace tests; do not sum the table into a total test count.

The optional Rust sequence passed 298 tests in 16 test targets. Some repeat tests from the default workspace, so these counts must not be added to the workspace total. The [validation record](dependency-validation-2026-09-06.json) stores commands, exit codes, and log hashes. Fixed repair/ping/pong packets, historical vote bytes, signatures, malformed compressed input, CAR range scans, and replay memory/JIT comparisons remain covered.

The final cached workspace build reported 2 minutes 11 seconds; the sum of test-target execution times was 68.15 seconds. Earlier compilation during this upgrade populated that cache. These times do not establish a build-speed improvement over the earlier run. See the [test suite review](test-suite-review-2026-09-06.md) for the three fewer integration link targets and four removed redundant tests.

The explorer's full release command still requires the external strict replay report. The static build passes, but this release-data gate was not bypassed. The [JavaScript review](js-dependency-review-2026-09-06.md) records this limit and the temporary valid placeholder configuration used for the Worker dry runs.

The [epoch 300 dependency retest](../benchmarks/epoch-300-dependency-review-2026-09-06.md) compares the updated reader with the frozen rolling-pipeline build on 2,048-block V2 prefixes. All 24 cases and 60 measured iterations passed counter, coverage, source, and resource checks. No median elapsed comparison reached the 10% investigation threshold. Pump.fun allocation calls increased at both worker counts, with up to 4.26% more requested Rust bytes at twelve workers. This bounded test does not repeat full-epoch output checks. Earlier full-epoch NAS reports remain evidence for their original source and binary hashes.
