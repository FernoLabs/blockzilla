# gRPC ledger-projection shadow

Status: **read-only migration canary; never catalog eligible**.

This canary proves that every record in one immutable Hivezilla raw-gRPC
generation can be projected into the structural ledger-candidate shape. It does
not promote a source, select finality, build an epoch, upload an object, advance
an ACK, or authorize deletion.

## Input boundary

Use only one of:

- a stopped raw-gRPC recorder directory; or
- a read-only filesystem snapshot taken at a durable generation boundary.

Do not point the command at a live mutable directory. Run the existing raw-spool
audit first if the generation was not closed cleanly.

## Run

```bash
hivezilla verify-grpc-raw-ledger-shadow \
  --output-dir /immutable/grpc-generation \
  --max-record-bytes 134217728 \
  --min-records 1
```

The JSON report is successful only when `records_scanned` equals
`candidates_projected`. Retain the exact binary digest, source-generation
identity, command arguments, stdout JSON, and exit status as non-canonical
diagnostic evidence.

## Isolation policy

When run by a container scheduler, require all of the following:

- `network_mode: none`;
- source mount read-only;
- read-only root filesystem and a small temporary filesystem only if the
  runtime requires it;
- no cloud-write, catalog, scheduler, source-control, or ACK credentials;
- no Docker socket and all Linux capabilities dropped;
- `restart: "no"` so a deterministic failure remains visible; and
- fixed CPU, memory, and wall-time limits sized from a measured snapshot.

The report may be captured by the scheduler's log collector. The command itself
creates no result or cursor file in the source tree.

## Fail-closed alerts

Any non-zero exit is an alert and blocks cutover. Distinguish at least:

- raw WAL/handoff corruption or incomplete committed evidence;
- incomplete or inconsistent PoH/transaction indexes;
- invalid parent/final hash or signature/header structure;
- a versioned transaction whose fee-payer signature does not prove the pinned
  V0 encoding; and
- resource exhaustion or timeout.

The version failure may be an Agave V1 transaction. Yellowstone schema 12.4
drops its V1-only config field during known-schema decode/re-encode, so the
shadow can detect but cannot reconstruct it. Preserve the rejected raw
generation and upgrade capture to a new version/config-preserving stream schema.
Never skip the row or reinterpret it as V0.

## Promotion gate

A clean shadow is necessary but insufficient. Canonical Archive or Replay input
still requires an immutable evidence/policy receipt, complete era-pinned
sanitation and signature/PoH verification, an authoritative finality manifest,
and an opaque finality-selected candidate API. Until those exist, this command
must remain isolated from every product builder and catalog writer.
