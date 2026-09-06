# blockzilla-dump

For format choice, source trust, and the common read API, start with
[`Archive formats and the read SDK`](../../docs/reference/archive-formats-and-read-sdk.md).

`blockzilla-dump` reads immutable Blockzilla Archive V2 generations. It can:

- create a resumable SQLite dump for one program;
- create a resumable SQLite dump for one recorded token mint;
- build and query a signer user-to-program index; and
- verify archive history.

The CLI reads a complete Archive V2 bundle. The current lean conversion sample
is not a standalone input. It does not contain all source transaction messages
and signatures.

## Install

Install the current source version:

```console
git clone https://github.com/FernoLabs/blockzilla.git
cd blockzilla
cargo install --locked --path indexer/blockzilla-dump
```

After the crate is published, the install command will be:

```console
cargo install --locked blockzilla-dump
```

The crate is not published at this time.

## Select a source

Use one complete local generation, or a root that contains `epoch-N`
directories:

```console
blockzilla-dump program \
  --archive /data/blockzilla/archive-v2 \
  --epoch 900 \
  --output pump-fun.sqlite
```

Use the Cloudflare gateway with a local cache:

```console
export BLOCKZILLA_GATEWAY_URL='https://archive.example.invalid'
blockzilla-dump program \
  --gateway "$BLOCKZILLA_GATEWAY_URL" \
  --cache "$PWD/blockzilla-cache" \
  --epoch 900 \
  --output pump-fun.sqlite
```

The example URL is a placeholder. The public gateway is not deployed. If the
gateway needs a bearer token, set `BLOCKZILLA_ARCHIVE_TOKEN`.

The gateway mode first downloads and verifies the manifest and the control
files. It then reads block and signature ranges as needed. A user-program index
build also downloads and verifies `registry.mphf`. Processing uses the host's
logical CPU count by default. Use `--threads N` to set a different value.

## Common operations

The `program` command uses the Pump.fun program by default. It selects an
actual top-level or recorded CPI invocation. A program address that is only an
account does not match.

```console
blockzilla-dump program \
  --archive /data/blockzilla/archive-v2 \
  --epoch 900 \
  --output pump-fun.sqlite
```

The `token` command uses the mainnet USDC mint by default. It selects
transactions whose metadata records the mint in a pre-token or post-token
balance. It does not select every SPL Token instruction.

```console
blockzilla-dump token \
  --archive /data/blockzilla/archive-v2 \
  --epoch 900 \
  --output usdc.sqlite
```

Build and query a signer user-to-program index:

```console
blockzilla-dump user-program-index build \
  --archive /data/blockzilla/archive-v2 \
  --epoch 900 \
  --output user-program-index-900

blockzilla-dump user-program-index query \
  --archive /data/blockzilla/archive-v2 \
  --epoch 900 \
  --index user-program-index-900 \
  --user '<SIGNER_PUBKEY>' \
  --json
```

The default verification check is blockhash continuity. The other checks are
off until you request them.

```console
blockzilla-dump verify \
  --archive /data/blockzilla/archive-v2 \
  --epoch 900

blockzilla-dump verify \
  --archive /data/blockzilla/archive-v2 \
  --epoch 900 \
  --signatures

blockzilla-dump verify \
  --archive /data/blockzilla/archive-v2 \
  --epoch 900 \
  --poh \
  --poh-hashes-per-tick '<TRUSTED_VALUE>'

blockzilla-dump verify \
  --archive /data/blockzilla/archive-v2 \
  --epoch-range 900..=1000 \
  --all-checks \
  --poh-hashes-per-tick '<TRUSTED_VALUE>'
```

PoH recomputation needs a local archive at this time. A nonzero start epoch
also needs the preceding epoch for the boundary check. Each report marks a
check as `passed`, `not-requested`, or `failed`. A range needs every epoch
between its endpoints. The selected Foundation samples are not one continuous
range.

## Coverage and exit status

The default `--on-indeterminate fail` policy stops if the archive cannot prove
a match or a non-match. This condition is not treated as a non-match.

- Exit `0` means that the requested operation completed without coverage gaps.
- Exit `1` means that a requested verification check failed or another error
  stopped the command.
- Exit `2` means that a dump completed with coverage gaps.

Use `--on-indeterminate record` to save each gap in the SQLite
`coverage_issues` table. Use `--on-indeterminate skip` only when you accept a
partial result. The skip policy does not save the gap rows, but the command
still reports a partial result and exits with status `2`.

Use `blockzilla-dump status --output FILE` to inspect a resumable dump.

## Foundation sample epochs

The selected sample epochs are `0`, `100`, `200`, `300`, `400`, `500`, `600`,
`700`, `800`, `900`, and `1000`. Public complete bundles and a public gateway
URL are not available until publication is approved.

When all complete bundles are available under one source root, scan the exact
sample set into one SQLite file:

```console
blockzilla-dump program \
  --archive /data/blockzilla/archive-v2 \
  --epoch 0,100,200,300,400,500,600,700,800,900,1000 \
  --output pump-fun-foundation-samples.sqlite
```

See [Archive formats and the read SDK](../../docs/reference/archive-formats-and-read-sdk.md)
for local and network source setup and trust rules.
