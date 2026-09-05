# Network benchmark staged R2 publisher

Use `scripts/publish-network-format-benchmark-r2.sh` to copy one reviewed,
immutable release to R2. The script has two separate operations: private stage
and serving-bucket promotion.

It does not delete or replace an object. It does not calculate a payload hash
or read a payload body back from R2. It accepts an object only when its key and
byte size match the fixed TSV inventory.

The buckets are fixed:

- private staging: `blockzilla-network-format-benchmark-staging-v1`;
- serving: `blockzilla-network-format-benchmark-v1`.

## Inventory contract

The TSV has this exact header:

```text
role	source_kind	source	target_key	bytes
```

| Field | Allowed value | Meaning |
| --- | --- | --- |
| `role` | `payload`, `control`, or optional `completion` | Copy order. Payload rows come first. Control rows follow. Optional completion rows come last. |
| `source_kind` | `local` or `staged-copy` | Upload an absolute local file, or copy an exact private-staging key. |
| `source` | Absolute file or safe R2 key | Exact source path or key. |
| `target_key` | Safe R2 key | Exact key to create in both buckets. |
| `bytes` | Canonical decimal integer | Required source, staging, and serving size. |

At least one payload row is required. Completion rows are not required. A
zero-completion inventory is complete when all payload and control objects
exist with their exact sizes.

The publisher rejects `archive-v2-generation.json`,
`benchmark-manifest.json`, and `*.sha256` target names. The fixed epoch 900
inventory has 35 payload rows, two control rows, and zero completion rows.

A `staged-copy` row avoids a second NAS upload. The epoch 900 inventory uses
this operation for nine same-name V3 keys, including `signatures.bin`.

## Required scopes

Pass every target root with `--scope`. A scope must contain the release ID as
one full path segment. Scopes must be safe, unique, and non-overlapping.

For corrected epoch 900, use only these scopes:

```text
compact-v2/releases/e900-current-typed-errors-v1
indexer-v3/releases/e900-current-typed-errors-v1
```

## Private stage

Start with one new local state directory:

```bash
scripts/publish-network-format-benchmark-r2.sh \
  --inventory /absolute/path/epoch-900-r2-inventory.tsv \
  --state-dir /absolute/path/epoch-900-publisher-state \
  --release-id e900-current-typed-errors-v1 \
  --scope compact-v2/releases/e900-current-typed-errors-v1 \
  --scope indexer-v3/releases/e900-current-typed-errors-v1 \
  --mode stage \
  --rclone-remote r2
```

The publisher first requires both declared scopes to be empty in both buckets.
It creates a release-specific owner lock in the private bucket. It then uploads
payloads, performs staged copies, uploads controls, and validates the complete
private prefix.

Review the private result before promotion. Check all 37 keys and byte sizes.

## Promotion

Use the same inventory, scopes, release ID, and state directory:

```bash
scripts/publish-network-format-benchmark-r2.sh \
  --inventory /absolute/path/epoch-900-r2-inventory.tsv \
  --state-dir /absolute/path/epoch-900-publisher-state \
  --release-id e900-current-typed-errors-v1 \
  --scope compact-v2/releases/e900-current-typed-errors-v1 \
  --scope indexer-v3/releases/e900-current-typed-errors-v1 \
  --mode promote \
  --resume \
  --rclone-remote r2
```

Promotion uses R2 server-side copies. It checks the private source size and the
serving destination size for each object. The script has no combined stage and
promote mode.

Promotion does not make epoch 900 public through the Worker. The Worker route
map stays unchanged until a separate review and deployment activates the two
new prefixes.

## Resume and safety rules

Repeat an interrupted command with `--resume`. The stored inventory and scope
files must be byte-for-byte equal to the admitted files. A complete object with
the correct size is reused. An unexpected or wrong-size object stops the run.

If a control exists while a payload is absent, resume stops. If an optional
completion object exists, all earlier payload and control objects must also
exist. The remote owner lock remains after success. The publisher does not
remove it.

Local uploads use immutable, size-only multipart settings. Staged copies and
promotion use immutable R2 server-side copy settings. Use a new release ID and
new scopes for any different archive generation.

## Local test

```bash
scripts/test-publish-network-format-benchmark-r2.sh
```

The test uses a local file-backed `rclone` substitute. It covers a
zero-completion release, stage and promotion order, resume, staged copies,
wrong sizes, unexpected objects, no replacement, and rejected manifest or hash
names. It does not contact R2.
