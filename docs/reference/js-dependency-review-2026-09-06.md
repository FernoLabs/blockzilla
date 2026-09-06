# JavaScript and build tool dependency review — 2026-09-06

This review covers all five JavaScript packages and the CI tools that build or check them. Package versions were checked against the npm registry, crates.io, and the publishers’ GitHub releases on 2026-09-06. No service was deployed.

## Package changes

All four Edgezilla packages now use Wrangler 4.129.0. The archive sample and R2 gateway lockfiles moved from 4.127.1 to 4.129.0. The two Rust worker manifests moved from 4.111.0 to 4.129.0 and now have committed npm lockfiles. Their existing Cloudflare compatibility dates and bindings are unchanged. The [Wrangler release notes](https://github.com/cloudflare/workers-sdk/releases) did not require a configuration change for these workers.

The explorer manifest minimum versions now match the current supported releases. Several packages were already at these versions in the old lockfile; raising those minimums does not change their installed code.

| Explorer package | Previous lock | Current lock |
| --- | --- | --- |
| `@lucide/svelte` | 1.37.0 | 1.41.0 |
| `@sveltejs/kit` | 2.70.3 | 2.70.3 |
| `@sveltejs/adapter-static` | 3.0.10 | 3.0.10 |
| `svelte` | 5.57.0 | 5.57.0 |
| `vite` | 8.2.2 | 8.2.2 |
| `svelte-check` | 4.7.6 | 4.7.6 |
| `@types/node` | 24.13.3 | 26.4.1 |
| `typescript` | 5.9.3 | 6.0.3 |
| `lightweight-charts` | 5.2.1 | 5.2.1 |
| `cookie` (Kit dependency) | 0.6.0 | 0.7.2 |

The explorer needed no application source changes. The [TypeScript 6 release notes](https://devblogs.microsoft.com/typescript/announcing-typescript-6-0/) and [Vite 8 migration guide](https://vite.dev/guide/migration) were reviewed. The installed packages passed the existing type check and static application build.

## Version exceptions

TypeScript 7.0.2 is the newest stable release, but the current [SvelteKit 2.70.3 package](https://registry.npmjs.org/@sveltejs/kit/2.70.3) and [svelte-check 4.7.6 package](https://registry.npmjs.org/svelte-check/4.7.6) support TypeScript 5 or 6 in their peer dependency declarations. The explorer uses 6.0.3, the newest supported 6.x release. No peer dependency override was used.

SvelteKit still selects `cookie` 0.6.0, which is affected by [GHSA-pxg6-pf52-xh8x](https://github.com/advisories/GHSA-pxg6-pf52-xh8x). A narrow npm override selects 0.7.2 for SvelteKit. It includes the fix and preserves the API and external type names used by Kit. The newest `cookie` release is 2.0.1, which changes that API. This override can be removed when Kit selects a fixed compatible version itself. All five final npm installs reported zero known vulnerabilities.

## CI tools

- `worker-build` moves from 0.8.3 to 0.8.5, the newest stable [published crate](https://crates.io/crates/worker-build/0.8.5). This matches the upgraded Rust `worker` crate. The [0.8.5 release notes](https://github.com/cloudflare/workers-rs/releases/tag/v0.8.5) cover the corresponding build and runtime changes.
- `actions/checkout` moves from v7 to [v7.0.1](https://github.com/actions/checkout/releases/tag/v7.0.1), pinned to the verified tag commit `3d3c42e5aac5ba805825da76410c181273ba90b1`.
- [Lychee action v2.9.0](https://github.com/lycheeverse/lychee-action/releases/tag/v2.9.0) is already current. Its existing immutable pin `e7477775783ea5526144ba13e8db5eec57747ce8` matches the published tag.
- [Gitleaks v8.30.1](https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1) is already current. Its published Linux x64 asset digest matches the CI checksum `551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb`.

## Validation

Checks used Node 26.8.1, npm 11.19.0, and Rust 1.98.1. Native feature checks used the shared Cargo target with incremental compilation disabled. The two worker builds used the actual `wasm32-unknown-unknown` target.

| Check | Result |
| --- | --- |
| Explorer data tests | 137 passed |
| Explorer `npm run check`, after the cookie override | 0 errors, 0 warnings |
| Explorer `npm exec vite build` | Passed; static site built in 28.92 seconds |
| Explorer `npm run build` release data gate | Stopped as required: no `SPYX_STRICT_REPLAY_REPORT` or `--strict-replay` input was provided |
| Archive sample worker tests | 31 passed |
| Archive sample Wrangler dry run | Passed |
| R2 gateway JavaScript check and tests | Passed; 30 tests |
| R2 gateway Wrangler dry run | Passed |
| All five `npm ls --depth=0` checks | Passed; manifests and lockfile root versions match |
| Both Rust worker helper scripts | JavaScript syntax checks passed |
| `worker-build 0.8.5 --locked` tool installation | Passed; installed in a temporary directory |
| Archive V2 full `worker-build` | Passed; 32.46 seconds after the fixes |
| Old Faithful full `worker-build` | Passed; 14.04 seconds after the profile fix |
| Archive V2 Wrangler dry run | Passed with a temporary validation config; 1,253.36 KiB, 430.84 KiB gzip |
| Old Faithful Wrangler dry run | Passed with a temporary validation config; 1,187.71 KiB, 433.42 KiB gzip |
| Native Archive V2 Worker feature tests | 15 passed |
| Pure Rust Old Faithful codec tests | 102 passed with only `query-sdk,compact-index,zstd-wasm`; 13.15 seconds |

The separate native checks were `cargo test --locked -p blockzilla-get-block --no-default-features --features worker --lib` and `cargo test --locked -p of-car-reader --no-default-features --features query-sdk,compact-index,zstd-wasm --lib`.

The feature-separated codec check also found a test fixture that called the optional native compressor. It now uses a fixed zstd 1.5.7 level-1 frame and checks its decoded bytes against the generated protobuf value. This adds no dependency and leaves the decoder on `ruzstd` when only `zstd-wasm` is enabled.

The first Archive V2 WebAssembly build found a misplaced native platform gate on `FileBackedKeyIndex`. The native gate was restored on its implementation before the build was retried. This correction keeps file-backed registry operations out of WebAssembly.

Both Rust workers then compiled for WebAssembly, but the binding step found the upstream [worker-build symbol stripping issue](https://github.com/cloudflare/workers-rs/issues/1014). The workspace release profile uses `strip = true`, which removes names needed to build the catch wrappers. Both worker packages now override the release setting with `strip = "debuginfo"`. Both complete builds passed with this change, including binding generation, WebAssembly optimization, and JavaScript bundling. Other release packages keep their original strip setting. Panic recovery and current binding versions are retained.

The Rust worker example configurations contain uppercase bucket placeholders, which Wrangler rejects before bundling. Their dry runs use temporary copies with valid placeholder bucket names, a zero KV ID where needed, and absolute paths to the same built entry and build directory. The committed example bindings are unchanged. These checks do not verify live resource names or credentials.

The direct Vite build checks application compatibility with the local data. It does not replace the release data gate or prove that a new benchmark release has the required strict replay evidence. The release gate was not weakened, and no benchmark data was invented.

The official `worker-build 0.8.5 --locked` installation reports yanked entries for `der 0.8.0`, `time 0.3.48`, and `time-macros 0.2.28` in the tool’s upstream lockfile. These are separate from the workspace lockfile. The published locked installation was retained.

The npm install script policy left optional package install scripts unapproved. The installed platform binaries were sufficient for the completed Wrangler dry runs and Vite build. No general install script permission was added.

See the [Rust dependency review](dependency-review-2026-09-06.md) for Rust package changes and their compatibility limits.
