# Blockzilla

This repo contains the tooling used to build and maintain a “ferno” Solana archive
node.

## CLI quick start

### Optimize a single epoch

Run the end-to-end pipeline for one epoch. The command downloads the CAR file if
it is missing, builds the registry artifacts, writes the optimized archive, and
then removes the CAR again when it was fetched during this run.

```bash
cargo run --release --bin blockzilla optimize epoch 839
```

By default the workflow stores CAR files in `./cache`, registries in
`./registry`, and optimized blocks in `./optimized`. Use `--cache-dir`,
`--registry-dir`, or `--optimized-dir` to override the directories. Pass
`--force` to rebuild an epoch even when all four outputs already exist.

### Optimize multiple epochs

Sequentially process a range of epochs with the same pipeline. Already completed
epochs are skipped unless `--force` is supplied.

```bash
cargo run --release --bin blockzilla optimize range 800 805
```

### Registry helpers

Build a registry and ordered pubkey file for a single epoch:

```bash
cargo run --release --bin blockzilla registry build 839
```

Inspect summary information about an existing registry:

```bash
cargo run --release --bin blockzilla registry info 839
```

Merge all per-epoch registries into a single directory (defaults to `./registry/merged`):

```bash
cargo run --release --bin blockzilla registry merge --registry-dir registry
```

## Legacy archive builder flow

The older, more manual steps are still available if you want to drive each stage
individually:

```bash
# download epoch car file from old-faithful archive or IPFS
curl -fSL https://files.old-faithful.net/0/epoch-0.car -o cache/epoch-0.car # /!\ epoch 0 is 4Gb

# convert the cached CAR without running the downloader/cleanup workflow
cargo run --release --bin blockzilla optimize car --epoch 0 --cache-dir cache --results-dir optimized
# parse and embed compact transaction metadata (omit the flag to store raw protobuf bytes)
cargo run --release --bin blockzilla optimize car --epoch 0 --cache-dir cache --results-dir optimized --include-metadata
# drop metadata entirely from the output
cargo run --release --bin blockzilla optimize car --epoch 0 --cache-dir cache --results-dir optimized --drop-metadata

# read the optimized archive (sequential by default, or parallel with --jobs)
cargo run --release --bin blockzilla optimize read 0 --input-dir optimized --jobs 4
```

## Registry outputs

The `blockzilla registry` workflows now produce two artifacts per epoch: the existing
`registry-<epoch>.bin` postcard map and a companion `registry-pubkeys-<epoch>.bin`
file that lists every pubkey sorted by the usage-derived `order_id`. The legacy
`registry-splitter` helper binary has been removed. The optimizer now consumes this
ordered pubkey file: point `blockzilla optimize` at the directory containing
`registry-pubkeys-<epoch>.bin` via `--registry-dir` (defaults to the optimize output
directory) and every pubkey in the compressed blocks will be replaced with its
usage-based ID.

## Deploy to local blockzilla

```bash
tar cz --no-xattrs --exclude target --exclude .git --exclude epoch-0.car --exclude epoch-1.car --exclude optimized . | ssh ach@blockzilla.local 'mkdir -p ~/dev/blockzilla && tar xz -C ~/dev/blockzilla'


```

## mac m1 build

gcc is required to build protobuff related package

```bash
brew install gcc
export CC=/opt/homebrew/bin/gcc-15                                                                                       
export CXX=/opt/homebrew/bin/g++-15
export CXXFLAGS="-std=c++11"
```

tar cz --no-xattrs --exclude target --exclude .git --exclude epoch-0.car --exclude epoch-1.car --exclude optimized . | ssh root@static.127.147.245.188.clients.your-server.de 'mkdir -p ~/dev/blockzilla && tar xz -C ~/dev/blockzilla'

aria2c -x 16 -s 16 -j 8 https://files.old-faithful.net/800/epoch-800.car -o /dev/null --file-allocation=none 