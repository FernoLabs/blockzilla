# Blockzilla network format benchmark gateway

This Worker gives public, range-based access to the benchmark R2 bucket. It
serves CAR, Compact V2, and Indexer V3 objects. It does not build an archive,
upload data, or validate archive contents.

This manifest-free Worker is deployed. Worker version
`73ae3918-cb8a-4e01-b39f-025f37507061` activated the corrected epoch-900
Compact V2 and Indexer V3 release on 2026-08-31.

## Indexed archive routes

Compact V2 and Indexer V3 use these public paths:

```text
/compact-v2/v1/epochs/<epoch>/files/<fixed-name>
/indexer-v3/v1/epochs/<epoch>/files/<fixed-name>
```

The Worker maps each `(format, epoch)` pair to one exact R2 prefix through
`BENCHMARK_RELEASE_MAP`. The checked-in setting keeps the existing epoch-0
routes and activates the immutable epoch-900 routes:

```text
compact-v2:0=compact-v2/epoch-0,indexer-v3:0=indexer-v3/epoch-0,compact-v2:900=compact-v2/releases/e900-current-typed-errors-v1,indexer-v3:900=indexer-v3/releases/e900-current-typed-errors-v1
```

An indexed epoch that is not in this map returns
`benchmark_release_not_published` without an R2 request. The map parser rejects
duplicate routes, duplicate prefixes, noncanonical epochs, cross-format
prefixes, unsafe path segments, and values above unsigned 64-bit range.

There is no indexed `/manifest` route. The Worker does not use an archive
manifest, object hash, or R2 existence check as a publication gate. Publication
is one explicit configuration change after all objects have been uploaded and
checked.

The active epoch-900 release prefixes are:

```text
compact-v2/releases/e900-current-typed-errors-v1
indexer-v3/releases/e900-current-typed-errors-v1
```

Before activation, both serving prefixes passed an independent exact check:
37 objects and 210,399,350,834 bytes, with no missing, extra, or wrong-size
object. See the publication runbook for the recorded checks.

## File policy

The Worker uses fixed file-name lists. Compact V2 has 12 names, including
`registry_counts.bin`. Indexer V3 has 25 names. Old schema markers and old
manifest names are not in either list.

Large objects require one closed byte range. A request can read at most 64
MiB. Small control files can use a bounded full GET. Responses are streamed
from R2, keep the R2 strong ETag, and use `Cache-Control: no-store`.

CAR routing is unchanged and does not use `BENCHMARK_RELEASE_MAP`:

```text
/car/<epoch>/epoch-<epoch>.car
/car/<epoch>/epoch-<epoch>-slot-ranges.raw
```

## Access control

`BENCHMARK_PUBLIC_READ=true` enables public reads. If public reads are off, the
Worker requires the configured source-IP allowlist. Invalid or absent access
configuration fails closed.

## Local checks

Run these commands in this directory:

```bash
npm test
npm run check
```

The tests cover legacy epoch 0, unmapped epochs, the immutable epoch-900
prefixes, all 37 fixed indexed names, `registry_counts.bin`, release-map
validation, range limits, R2 errors, CAR independence, and access control.

See these runbooks for the exact object inventory and activation order:

- `docs/operations/epoch-900-network-format-r2-inventory.md`
- `docs/operations/network-format-benchmark-staged-r2-publisher.md`
- `docs/operations/epoch-900-corrected-network-publication.md`
