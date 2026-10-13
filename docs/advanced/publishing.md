# Publish the reader and CLI

The intended user install is `cargo install blockzilla --locked`. A Rust
application uses the reader crate with its `http` feature. These commands need
the CLI and all its internal dependencies on crates.io.

## Prepared release set

The CLI release set checked on 2026-10-03 contained 23 packages, including
optional archive tools. `scripts/check-crate-release.py` derives the current
dependency order from Cargo metadata.
It checks publication settings, descriptions, and registry versions on local
dependencies. It never uploads packages.

That set used `0.2.0`, except the retained decoder dependency
`of-car-reader`, which uses `0.2.1`. Its `0.2.0` already exists on crates.io and
cannot be replaced. The customer setup does not require that file format.

The public CLI's default install builds only the `blockzilla` binary. Contributor
diagnostic and benchmark binaries require explicit features. This first release
still includes archive production dependencies; a smaller client package can
reduce later install cost.

## Check the packages

From the repository root:

```bash
python3 scripts/check-crate-release.py --offline
python3 scripts/check-crate-release.py --package --offline --allow-dirty
python3 scripts/check-crate-release.py --dry-run --allow-dirty
```

The package step creates `.crate` files but does not compile them. The dry run
also compiles the packaged source and uses the registry to resolve dependencies.
It does not upload. Use `--allow-dirty` only for review of the current checkout;
publish a reviewed, committed release source.

See [Cargo's publish reference](https://doc.rust-lang.org/cargo/commands/cargo-publish.html)
for dry-run, authentication, and package selection behavior.

## Publish after the release check

Print the exact command for the current dependency set:

```bash
python3 scripts/check-crate-release.py --print-publish-command
```

Review the listed package names and versions. Run that command with an authorized
crates.io account. Cargo publishes the selected packages in dependency order.
Do not publish only `blockzilla` while its dependencies are missing from the
registry. If an upload returns an uncertain result, check the registry before
retrying the same version.

## Check the user install

After the registry shows the complete release:

```bash
cargo install blockzilla --version 0.2.0 --locked
blockzilla stream --epoch 0 --max-blocks 10
blockzilla dump-tokens --epoch 900 --max-blocks 3
```

Test this in a clean install environment. A working `--path` install does not
prove that installation from crates.io works. After that check, update the quick
start to remove the release-pending note and source-install fallback.

## Release status

The current source checkout adds standalone stream workspaces to the CLI's
dependency graph. The stream decoder, relay, bank v2 contract, and object-store
packages still disable crates.io publication. Some of their local dependencies
also have no registry version. The release checker now inspects these separate
workspaces and reports those limits. A complete source snapshot can be saved in
Git, but the current CLI dependency set is not ready for a crates.io release.

All 23 packages passed the registry dry run on 2026-10-03. The check compiled
the packaged source and aborted each upload. A separate local CLI install passed.
The public check read ten epoch 0 blocks in 3.6 seconds and exported 4,718 token
balance rows from three epoch 900 blocks in 36.1 seconds.

The compatibility reader now declares its required known-program log features
explicitly. This fixes an isolated package build that previously depended on
feature settings elsewhere in the workspace.

Package manifests, local CLI installation, public historical reads, and token
output checks are prepared. Registry publication has not been performed. The
continuous live stream and a full Superbank ingestion adapter remain later work.
