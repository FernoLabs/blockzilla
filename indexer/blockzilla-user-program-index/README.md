# blockzilla-user-program-index

This crate builds and queries a per-epoch user-program index. A signer user is
each required signer of a transaction. This includes the fee payer and all
co-signers. For each successful transaction, the index relates every signer
user to every distinct program that the transaction reached. Reached programs
include top-level instructions and recorded inner/CPI instructions. Failed
transactions are not in the index. Vote transactions are in the index.

The builder streams an immutable Archive V2 generation. It does not read logs,
token balances, rewards, or return data. The dense builder uses two bounded
passes. Pass 1 finds signer users. Pass 2 builds user-program relations. The
builder then publishes the complete index with an atomic no-replace rename.

The library and standalone command are in this one package. The former
`blockzilla-firebase-indexer` package is removed. Install the command with:

```sh
cargo install --path indexer/blockzilla-user-program-index --features cli --bin blockzilla-user-program-index
```

The command supports `build`, `build-dense`, `discover-signers`, and `query`.
Existing command options and index files are unchanged. For example:

```sh
cargo run -p blockzilla-user-program-index --features cli -- --help
```

Use these public operations from the dump command:

- `blockzilla-dump user-program-index build` builds one immutable epoch index.
- `blockzilla-dump user-program-index query` returns the reached programs for
  one signer user.

For a local archive, call `build_dense_index`:

```rust,no_run
use std::path::Path;

use blockzilla_user_program_index::build::{
    DEFAULT_MAX_ACCOUNTS_PER_CHUNK, DEFAULT_QUEUED_RELATION_BATCHES,
    DEFAULT_RELATION_BATCH_PAIRS, build_dense_index, default_scan_threads,
};

build_dense_index(
    Path::new("archive/epoch-900"),
    900,
    Path::new("indexes/epoch-900"),
    None,
    None,
    DEFAULT_MAX_ACCOUNTS_PER_CHUNK,
    default_scan_threads(),
    DEFAULT_RELATION_BATCH_PAIRS,
    DEFAULT_QUEUED_RELATION_BATCHES,
)?;
# Ok::<(), anyhow::Error>(())
```

For an archive reader that is already open, call
`build_dense_index_from_reader`. This also works when the reader uses an HTTP
range source. Keep `registry.bin` and `registry.mphf` in one local cache
directory.

```rust,no_run
use std::path::Path;

use blockzilla_compact_v2_reader::{ArchiveReader, RangeSource};
use blockzilla_user_program_index::build::{
    DenseIndexBuildOptions, build_dense_index_from_reader,
};

fn build_from_reader<S: RangeSource>(reader: &ArchiveReader<S>) -> anyhow::Result<()> {
    build_dense_index_from_reader(
        reader,
        Path::new("cache/epoch-900/registry.bin"),
        Path::new("cache/epoch-900/registry.mphf"),
        Path::new("indexes/epoch-900"),
        DenseIndexBuildOptions::default(),
    )
}
```

The output keeps the established versioned layout:

- `manifest.json`
- `programs.map`
- `shard-N/wallets.idx`
- `shard-N/programs.rel`

Use `format::IndexManifest::verify_generation` to verify all immutable index
files. Use `query::query_user_program_index` for new tools. Its JSON uses
`user` and `index_user_count`. The older `query::query_index` result keeps its
Rust `wallet` names for source compatibility. On-disk field and file names that
contain `wallet` also remain unchanged for format compatibility.

## Optional operations and benchmarks

Build retained operational tools with `--features developer-tools`. This feature
also enables `cli`. The default library does not add the legacy read SDK,
HTTP operations client, profiler, or command dependencies.

The retained binaries are:

- `archive-v2-account-projection` and `archive-v2-account-projection-verify`
- `archive-v2-lean-read-bench`
- `user-program-index-wire-profile-audit` and `user-program-index-wire-profile-audit-batch`
- `user-program-index-wire-profile-marker-transition`
- `index-bench` and `index-parity`

```sh
cargo test -p blockzilla-user-program-index --features developer-tools --all-targets
cargo run -p blockzilla-user-program-index --features developer-tools --bin index-parity -- --help
```

The batch auditor accepts the canonical indexer executable path for new
immutable batch manifests. It also accepts the old executable path in existing
manifests. Each selected path still requires the manifest's exact binary hash.

The controller remains parked. See [PARKED-BINS.md](PARKED-BINS.md) for its API
blocker. [REDESIGN.md](REDESIGN.md) retains the earlier design and measurements.

## Naming and compatibility

The current project name is `user-program-index`. Operational binaries use
`user-program-index-wire-profile-*`. The parked controller source is
`user-program-index-controller.rs`. Optional Rust modules use
`user_program_index_*`.

The controller configuration uses `BLOCKZILLA_USER_PROGRAM_INDEX_*`; its output
root variable is `BLOCKZILLA_USER_PROGRAM_INDEX_ROOT`. Old
`BLOCKZILLA_FIREWATCH_*` variables remain fallback inputs for existing service
configurations. A current variable takes precedence. New child processes use
current attempt tags. Recovery also accepts an exact old attempt tag.

These stored identifiers keep the legacy `firewatch` spelling:

- Hash domains for canonical parity, generation identity, effective inputs,
  profile authority and control bundles. Changing them would change identities
  for unchanged data.
- Audit and retry JSON `kind` values, batch receipt kinds, and generation IDs.
  Existing manifests and receipts must still validate.
- The scheduler `firewatch-index` state directory, accepted marker names, audit
  scratch prefixes, and the `.firewatch-all-epochs-post.lock` name. Existing
  state must resume under the same lock and cleanup ownership rules.

The renamed batch scripts check both the current and old controller service
names. They also detect an old `blockzilla-firebase-indexer` process. Existing
immutable audit manifests may refer to that old executable path; their exact
binary hash is still required. These names are compatibility identifiers, not
separate projects or tools to install.
