# hivezilla-object-store

Provider-neutral immutable object operations shared by Hivezilla custody and
recovery components. The crate supplies deterministic memory and filesystem
implementations for conformance and crash-boundary tests plus bounded
production S3, Cloudflare R2, and Backblaze B2 adapters.

The native `blockzilla-s3-upload` binary owns generation upload and immutable
readback verification, Backblaze account-usage reporting, and crash-safe R2
retention:

```bash
cargo run --locked -p hivezilla-object-store \
  --bin blockzilla-s3-upload -- --help
```

Credentials are read from a literal file or the documented provider
environment variables. They are never sourced as shell code or included in
command output. R2 deletion remains opt-in and requires both `--apply` and an
independently confirmed maximum generation slot.

Provider and credential selection is fail-closed:

- `--provider` wins, followed by `STORAGE_PROVIDER`/`S3_PROVIDER`. Automatic
  selection rejects credentials containing more than one of the `R2_*`,
  `B2_*`, and `S3_*` families.
- R2 reads only `R2_*`, B2 reads only `B2_*`, and generic S3 reads only `S3_*`.
  The provider-neutral `AWS_*` compatibility names are considered only after
  one provider has been selected; another provider's namespace is never used
  as a fallback.
- Native Backblaze verification is enabled only for provider B2 with
  `B2_BUCKET_ID`. S3 and R2 never construct or contact the native B2 client.
- Signed requests retry only HTTP 429/500/502/503/504 and classified transient
  transport failures. Local validation, request construction, and source-file
  errors fail immediately.

Generic versioned-S3 custody additionally requires bucket Object Lock/retention
or credentials and operations that guarantee no concurrent
`DeleteObjectVersion`. The uploader performs exact-version payload/manifest
checks immediately before commit and payload/manifest/commit checks immediately
after it, but sequential API requests cannot make deletion atomic with commit.
R2 retention likewise requires its reported exclusive immutable-prefix
precondition because R2 has no conditional delete.

Backblaze account-usage output sets `scope_complete` only after all bounded
pagination finishes. It is an exhaustive scan, not an atomic snapshot of an
account that is changing concurrently.

Creating or verifying an object through this API does not make data terminally
protected, advance an ACK, authorize source retirement, or commit an archive
catalog entry. Those decisions require their respective protocol records.
