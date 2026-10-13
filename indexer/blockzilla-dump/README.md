# blockzilla-dump

For format choice, source trust, and the common read API, start with
[`Archive formats and the read SDK`](../../docs/reference/archive-formats-and-read-sdk.md).

`blockzilla-dump` reads fixed Compact V2 file sets. It can:

- create a resumable SQLite dump for one program;
- create a resumable SQLite dump for one recorded token mint;
- build and query a signer user-to-program index; and
- verify archive history.

Start with the [program dumper guide](../../docs/start/program-dumper.md) for
installation, source options, SQL queries, and restart rules. For balance JSON
exports, use the [token dumper](../../docs/start/token-dumper.md). For a live
SQLite consumer with history recovery, use the
[token indexer](../../docs/start/token-indexer.md). The
[live stream guide](../../docs/start/live-stream.md) covers browser/Node filters.

This executable reads historical files, not the live stream. A self-contained
dump needs source messages, metadata, signatures, and the required registries
from the same immutable build. A lean input that omits those fields is insufficient.

## Install

Install the current source version:

```console
git clone https://github.com/FernoLabs/blockzilla.git
cd blockzilla
cargo install --locked --path indexer/blockzilla-dump --bin blockzilla-dump --debug
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
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --output pump-fun.sqlite
```

Use the Cloudflare gateway with a local cache:

```console
export BLOCKZILLA_GATEWAY_URL='https://archive.example.invalid'
blockzilla-dump program \
  --gateway "$BLOCKZILLA_GATEWAY_URL" \
  --cache "$PWD/blockzilla-cache" \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --output pump-fun.sqlite
```

The example URL is a placeholder. Supply an operator-provided gateway with
`/v1/epochs/N/files/NAME` routes. The public sample origin uses a different
`/compact-v2/N/NAME` route and cannot be passed directly to this command. If the
gateway needs a bearer token, set `BLOCKZILLA_ARCHIVE_TOKEN`.

The source schema options are required. The values above apply to current
normalized Compact V2 samples. Select the recorded schema for another build;
do not infer it from an epoch number. Local sources also require
`--source-generation-prefix`. Keep that label and the files stable when resuming.
It is an operator label, not a content hash.

Gateway mode binds object names, sizes, and strong ETags. It caches control and
registry files, then reads block and signature ranges as needed. It does not
require a generation manifest in this path. Processing uses the host's logical
CPU count by default. Use `--threads 4` for a bounded first run.

## Common operations

The `program` command uses the Pump.fun program by default. Add
`--program ADDRESS` to select another program. It matches top-level instruction
references and recorded CPI references. A program address that is only an account
does not match. A top-level reference does not prove a failed execution reached
that instruction. Failed matching transactions remain visible in compact metadata.

```console
blockzilla-dump program \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --output pump-fun.sqlite
```

The `token` command uses the mainnet USDC mint by default. It selects
transactions whose metadata records the mint in a pre-token or post-token
balance. It does not select every SPL Token instruction.

```console
blockzilla-dump token \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --output usdc.sqlite
```

Build and query a signer user-to-program index:

```console
blockzilla-dump user-program-index build \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --output user-program-index-900

blockzilla-dump user-program-index query \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --index user-program-index-900 \
  --user '<SIGNER_PUBKEY>' \
  --json
```

The separate `verify` command defaults to blockhash continuity. Signature and
PoH checks are off until you request them. The dump examples do not run this
verification command.

```console
blockzilla-dump verify \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900

blockzilla-dump verify \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --signatures

blockzilla-dump verify \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 900 \
  --poh \
  --poh-hashes-per-tick '<TRUSTED_VALUE>'

blockzilla-dump verify \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
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

Use `blockzilla-dump status --output FILE --json` to inspect a resumable dump.
Repeat the same dump command after an interruption. Matching rows and the next
block-row position commit together. A changed source or target is rejected.
This historical checkpoint is separate from a live chunk cursor. See the
[program guide](../../docs/start/program-dumper.md#query-the-result) for SQL.
The dump schema is not the live token-index schema; `blockzilla token-db` does
not query it.

## Foundation sample epochs

The selected sample epochs are `0`, `100`, `200`, `300`, `400`, `500`, `600`,
`700`, `800`, `900`, and `1000`. The public sample origin serves selected Compact
V2 epochs. It is not the
`--gateway` interface above. Obtain a compatible local file set or gateway
from the operator before running this tool. A sample list does not establish
continuous historical coverage.

When all complete bundles are available under one source root, scan the exact
sample set into one SQLite file:

```console
blockzilla-dump program \
  --archive /data/blockzilla/archive-v2 \
  --source-generation-prefix my-archive-v1 \
  --message-schema current \
  --metadata-schema current-typed-error \
  --epoch 0,100,200,300,400,500,600,700,800,900,1000 \
  --output pump-fun-foundation-samples.sqlite
```

See [Archive formats and the read SDK](../../docs/reference/archive-formats-and-read-sdk.md)
for local and network source setup and trust rules.
