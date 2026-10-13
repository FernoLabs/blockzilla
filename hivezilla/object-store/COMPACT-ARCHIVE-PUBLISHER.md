# Compact execution-canary publisher

`blockzilla-s3-upload upload-compact-archive` stores a sealed, verified Compact V2
execution canary in R2. It is separate from raw overflow and the stopped raw-WAL
generation uploader. It does not publish an official canonical archive catalog,
create a `READY` marker, issue an ACK, or authorize source deletion.

The accepted source profile is `validator-finalized-execution-v1`. Finality is an
assertion from the trusted self-operated validator. It is not an independent
consensus proof. Missing shred evidence remains missing; this command cannot
add it or infer canonical status.

## Input contract

The caller supplies an existing private-owner archive directory and a separate
existing private-owner state directory. State must be outside the archive. Use
one state directory for one publication. The archive directory must contain
`LIVE-EXECUTION-CANARY.json` and its listed files. The marker has these fields:

```json
{
  "schema": "blockzilla-live-execution-canary-v1",
  "source_profile": "validator-finalized-execution-v1",
  "state": "COMPLETE",
  "canonical": false,
  "publication_ready": false,
  "finalized": true,
  "archive_verified": true,
  "source_receipt_sha256": "<64 lowercase hex digits>",
  "files": [
    {"path": "blocks.bin", "bytes": 123, "sha256": "<64 lowercase hex digits>"}
  ]
}
```

This is a contract example, not a valid archive or verification receipt. The
trusted materializer must write the marker only after it seals, syncs, and
verifies the complete Compact V2 file set. The marker must include the source
authority, range, coverage, counts, and missing capabilities supplied by that
materializer. The publisher preserves all marker evidence in the remote
manifest. It checks the fixed contract above and every file length and SHA-256;
it does not itself parse Compact V2 or prove the source receipt's meaning.
Do not create a COMPLETE marker for raw format-3 input or a blocked audit.

Files use unique flat ASCII names: letters, digits, `.`, `_`, and `-`; a name
cannot start with `.`. Symlink files are rejected. At most 64 files are accepted,
with a 512 MiB limit per file. The source marker is limited to 1 MiB. These are
application limits for the first canary, not R2 service limits. Payloads stream
from held file descriptors with fixed-size hash buffers. Multipart upload is
not implemented; an oversized input stops before remote access.

## Run and recovery

```sh
blockzilla-s3-upload upload-compact-archive \
  /path/to/verified-sealed-canary \
  execution-canary/node-name/capture-name \
  /path/to/private-publication-state \
  --credentials-file /path/to/private-storage-settings \
  --provider r2
```

The prefix must start with `execution-canary/`. The selected R2 credentials and
prefix must belong to the approved bucket and canary namespace. The command
does not discover or select a foundation bucket.

1. Validate the source marker and hash every listed file before any remote
   request. Pin each source file and check its identity at upload boundaries.
2. Lock the state directory and durably publish `intent.json`. It binds the
   exact source marker and remote manifest to a hash of endpoint, bucket, and
   prefix. Changing credentials does not change this binding.
3. Upload files with conditional creation. Existing objects and racing writes
   must pass a full size and SHA-256 read-back. User metadata and ETags do not
   replace read-back verification.
4. Publish `EXECUTION-CANARY-MANIFEST.json` as the final remote write, then read
   it back and verify it. Keys include the source marker SHA-256; the manifest
   retains `canonical=false` and `publication_ready=false`.
5. Durably publish `complete.json` locally and print the same report. Neither
   that report nor the remote manifest permits source retirement.

After failure, rerun the same command with the same source, state, endpoint,
bucket, and prefix. A lost upload reply leaves durable intent. A retry verifies
existing objects and does not replace them. A prior local completion report
does not skip remote verification. Failed or partial uploads can leave orphan
objects; this command never deletes them. Retain source data and state until a
separate approved recovery and retention policy permits cleanup.

The namespace must remain create-only for this workflow. Another principal or
bucket lifecycle rule can delete or overwrite an object after verification;
the publisher cannot turn these separate operations into an R2 transaction.
Canonical catalog authority remains outside this command.

The signed transport uses the documented [R2 S3 API](https://developers.cloudflare.com/r2/api/s3/api/).
Transport tests use small synthetic files and a local HTTP fixture. They test
publication order, retries, collisions, corrupt read-back, input rejection, and
state isolation. They do not establish Compact V2 semantic validity or a live
upload to the production bucket.
