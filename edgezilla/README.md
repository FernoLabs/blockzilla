# Edgezilla

## Purpose

Edgezilla is the cloud-replica and read-serving boundary in the target
architecture. It is intended to keep a second copy of compacted Archive V2 data
in Cloudflare R2 and/or S3-compatible storage such as Backblaze B2, then expose
read-only Solana `getBlock` access close to users.

## Status

Edgezilla is experimental. The repository currently contains two Cloudflare
Workers: one reads Archive V2 ranges from R2 or an S3-compatible bucket; the
other range-reads official Old Faithful CAR data using a separately populated
read-only index store. Both render JSON-RPC responses. The Archive V2 package
also provides a local file benchmark companion.

The public Edgezilla boundary is read-only. Archive upload, replication,
deletion, lifecycle policy, and multi-replica reconciliation are not public
Worker APIs and are not implemented as a complete product here. Deployment
configuration in this repository is a development example, not a production
environment.

## Current implementations

[`blockzilla-get-block/`](blockzilla-get-block/README.md) contains:

- the `blockzilla-get-block` Rust crate and native companion;
- Worker handlers for `getBlock`, `getBlockTime`, and `getVersion`;
- R2 and signed S3-compatible range readers;
- direct block and probe routes for diagnostics; and
- a native `local-bench` command for an existing local Archive V2 tree.

Edgezilla expects serving sidecars produced by Blockzilla. It does not build or
repair archives.

[`old-faithful-get-block/`](old-faithful-get-block/README.md) contains the
independent CAR compatibility and reference path. It reads official Old
Faithful CAR objects plus pre-populated compact indexes. It does not warm or
modify the index store from request handlers.

## Planned work

- a documented and verified Blockzilla-to-cloud replication process;
- replica manifests, integrity checks, and safe retention rules;
- a stable public serving contract and compatibility test corpus; and
- deployment templates that require operators to supply their own bindings and
  secrets explicitly.

## Validation

Native code and tests:

```bash
cargo check --locked -p blockzilla-get-block --all-targets
cargo test --locked -p blockzilla-get-block --all-targets
```

The Worker/Wasm builds have separate prerequisites and are documented in the
[Archive V2 Worker README](blockzilla-get-block/README.md) and the
[Old Faithful Worker README](old-faithful-get-block/README.md).

## Safety and security

Use object-read-only credentials for S3-compatible sources. Direct R2 bindings
are read-only only because these Workers do not call mutation APIs, so isolate
them in dedicated serving buckets and review that boundary before deployment.
Never commit account IDs, bucket names, access keys, Worker tokens, or
production deployment files. Do not deploy the example configuration
unchanged. Report vulnerabilities through the repository
[security policy](../SECURITY.md).
